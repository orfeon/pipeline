package com.mercari.solution.util.pipeline.lookup;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.mercari.solution.module.Schema;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * An external source that answers <em>key-driven lookups</em>: given a batch of
 * key values (point) or key-prefix + range requests, it returns the matching
 * rows. This is what powers the input-to-source lookup-join of
 * {@link com.mercari.solution.util.pipeline.Query2} across all adapters (JDBC /
 * Spanner primary keys, Bigtable row keys, REST request parameters, ...).
 *
 * <p>The join condition is restricted to a contiguous prefix of the chosen
 * candidate key's columns — point equality on the full key, or equality on the
 * leading columns plus a bounded range on the next. This guarantees the lookup
 * is index-backed and never degrades into a full scan; a standalone scan of a
 * lookup table is rejected at execution.
 *
 * <p>Lifecycle: implementations are {@link Serializable} configuration holders
 * (shipped inside a {@code DoFn}) with transient clients. {@link #setup()} is
 * called once per worker (and once at pipeline construction, for planning): it
 * registers the source in {@link LookupSourceRegistry}, derives table metadata
 * if not already present, and opens clients. {@link #close()} deregisters and
 * releases clients; the instance must remain re-{@code setup()}-able. Derived
 * metadata (schemas, key candidates) is kept in serializable fields so workers
 * do not repeat the derivation.
 *
 * <p><b>On-memory cache</b>: with a {@link CacheSpec} set (before {@link #setup()}),
 * {@link #lookupWithCache} serves repeated point / prefix-equality requests from a
 * bounded per-instance in-memory cache instead of re-fetching from the backend —
 * the win is large for the {@code query} transform, which evaluates its SQL per
 * element and therefore issues single-request batches. Range requests always
 * bypass the cache. Cached results are stale up to the configured TTL (in
 * addition to any backend-side staleness such as Spanner {@code maxStalenessSeconds}),
 * so caching is opt-in and intended for read-mostly tables.
 */
public abstract class LookupSource implements Serializable, AutoCloseable {

    /**
     * On-memory lookup cache settings. {@code maxSize} bounds the number of
     * cached request keys (per table+projection entry, per instance);
     * {@code expireAfterSeconds} is a TTL from write ({@code 0} = no expiry);
     * {@code cacheEmptyResults} also caches "no matching row" results
     * (recommended for LEFT joins against sparse tables).
     */
    public record CacheSpec(long maxSize, long expireAfterSeconds, boolean cacheEmptyResults)
            implements Serializable {

        public static final long DEFAULT_MAX_SIZE = 10_000L;

        public CacheSpec {
            if (maxSize <= 0) {
                throw new IllegalArgumentException(
                        "lookup cache maxSize must be positive but was: " + maxSize);
            }
            if (expireAfterSeconds < 0) {
                throw new IllegalArgumentException(
                        "lookup cache expireAfterSeconds must not be negative but was: "
                                + expireAfterSeconds);
            }
        }

        public static CacheSpec withDefaults() {
            return new CacheSpec(DEFAULT_MAX_SIZE, 0L, true);
        }
    }

    /** Cache key: one lookup request against one table/index/projection. */
    private record CacheKey(String table, String indexName, LookupRequest request,
            List<Integer> projects) {
    }

    private final String name;
    private CacheSpec cacheSpec;

    private transient long lookupSourceId;
    private transient boolean registered;
    private transient Cache<CacheKey, List<Object[]>> cache;
    // Test hook: injected fake clock for TTL tests (null = system ticker).
    transient Ticker cacheTicker;

    protected LookupSource(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("lookup source name must not be null or empty");
        }
        this.name = name;
    }

    /** The schema name this source's tables are referenced by in SQL. */
    public String getName() {
        return name;
    }

    /** Enables the on-memory lookup cache. Call before {@link #setup()}. */
    public final void setCacheSpec(CacheSpec cacheSpec) {
        this.cacheSpec = cacheSpec;
    }

    /** Stable (per live instance) id used by generated lookup-join code to resolve this source. */
    public long lookupSourceId() {
        if (!registered) {
            throw new IllegalStateException("lookup source '" + name + "' is not set up");
        }
        return lookupSourceId;
    }

    /** Registers the source, derives metadata if needed and opens clients. Idempotent. */
    public final void setup() {
        if (!registered) {
            this.lookupSourceId = LookupSourceRegistry.register(this);
            this.registered = true;
        }
        setupInternal();
        if (cacheSpec != null && cache == null) {
            this.cache = buildCache();
        }
    }

    /** Deregisters the source and closes clients; the instance can be set up again. */
    @Override
    public final void close() {
        if (registered) {
            LookupSourceRegistry.deregister(lookupSourceId);
            this.registered = false;
        }
        if (cache != null) {
            cache.invalidateAll();
            this.cache = null;
        }
        closeInternal();
    }

    /** Derives table metadata if absent and opens transient clients. */
    protected abstract void setupInternal();

    /** Closes transient clients (must tolerate repeated calls). */
    protected abstract void closeInternal();

    /**
     * The tables this source exposes (SQL table name → row schema), available
     * after {@link #setup()}. Iteration order is the configuration order.
     */
    public abstract Map<String, Schema> tableSchemas();

    /**
     * Candidate keys of {@code table} a lookup may use — the primary key first,
     * then any unique secondary indexes. Empty if the table cannot be looked up
     * by key. The planner rule picks the first candidate the join condition
     * matches.
     */
    public abstract List<LookupKey> keyCandidates(String table);

    /**
     * Whether this source can answer a lookup that constrains only a
     * <em>leading prefix</em> of a candidate key by equality (no range on the
     * next column) — an index-backed prefix scan returning all rows under the
     * prefix. True for ordered-key stores (JDBC, Spanner); false for sources
     * whose key columns are all required request parameters (REST).
     */
    public boolean supportsKeyPrefixLookup() {
        return false;
    }

    /**
     * Planner hook: called by the lookup-join rules when a join/LATERAL block
     * over {@code table} has matched the key contract, before the rewrite.
     * {@code boundFieldNames} holds, for each matched equality-prefix key
     * column (in key order), the name of the left-input field its bound
     * expression directly references — or {@code null} when the bound
     * expression is not a simple field reference. Sources whose data is
     * partitioned by the processing element's own key (e.g. {@code buffer})
     * override this to reject bindings they cannot answer, with an
     * explanatory error at planning time instead of wrong results at runtime.
     */
    public void validateLookupBinding(
            String table, String indexName, int prefixLength, List<String> boundFieldNames) {
    }

    /**
     * Planner hook: called by the lookup-join rules with the {@code table}
     * columns a matched lookup actually uses (projected / referenced in a
     * LATERAL block / constrained key columns). Sources may accumulate these
     * to let the host narrow what it materializes (e.g. the {@code buffer}
     * source's stored-fields auto-derivation). Called during planning at
     * pipeline construction and again on worker setup; implementations must
     * be idempotent.
     */
    public void markLookupUsage(String table, java.util.Collection<String> columns) {
    }

    /**
     * Fetches rows of {@code table} matching the batch via the chosen key.
     * {@code indexName} is {@code null} for the primary key, or a unique index
     * name. May return a superset; the lookup-join operator filters to exact
     * matches. Returned rows are in the {@code projects} column order (null =
     * all columns) using Calcite-internal value types (see {@link CalciteValues}),
     * and must include the key columns referenced by the request.
     */
    public abstract Iterable<Object[]> lookup(
            String table, String indexName, LookupBatch batch, int[] projects);

    /**
     * {@link #lookup} through the on-memory cache. Point and prefix-equality
     * requests are answered per distinct request: hits come from the cache and
     * only the missing requests are fetched from the backend (whose per-request
     * results — including empty ones, when configured — are cached). Range
     * batches, and lookups whose projection does not include the key columns,
     * bypass the cache. With no {@link CacheSpec} set this is a plain delegate.
     *
     * <p>Rows returned from (or via) the cache are shared across calls and must
     * not be mutated by callers.
     */
    public final Iterable<Object[]> lookupWithCache(
            String table, String indexName, LookupBatch batch, int[] projects) {
        if (cache == null || batch.isEmpty() || batch.isRange()) {
            return lookup(table, indexName, batch, projects);
        }
        // Positions of the request's key-prefix columns in the returned rows;
        // null when they cannot be located (then per-request attribution — and
        // therefore caching — is impossible).
        final int[] prefixPositions =
                prefixPositions(table, indexName, batch.prefixLength(), projects);
        if (prefixPositions == null) {
            return lookup(table, indexName, batch, projects);
        }
        final List<Integer> projectsKey = toProjectsKey(projects);
        final Counter hitCounter = Metrics.counter("lookup_cache", name + "_hit");
        final Counter missCounter = Metrics.counter("lookup_cache", name + "_miss");

        final LinkedHashSet<LookupRequest> distinct = new LinkedHashSet<>(batch.requests());
        final List<Object[]> out = new ArrayList<>();
        final List<LookupRequest> missed = new ArrayList<>();
        for (final LookupRequest request : distinct) {
            final List<Object[]> cached =
                    cache.getIfPresent(new CacheKey(table, indexName, request, projectsKey));
            if (cached != null) {
                out.addAll(cached);
                hitCounter.inc();
            } else {
                missed.add(request);
            }
        }
        if (!missed.isEmpty()) {
            missCounter.inc(missed.size());
            // Fetch only the misses, then attribute the returned rows back to
            // their requests by key prefix. The backend may return a superset;
            // rows under a prefix nobody requested are passed through to the
            // caller (which filters exactly) but never cached — the fetch is
            // only guaranteed complete for the requested prefixes.
            final Map<List<Object>, List<Object[]>> byPrefix = new HashMap<>();
            for (final Object[] row : lookup(table, indexName, LookupBatch.of(missed), projects)) {
                out.add(row);
                final List<Object> prefix = new ArrayList<>(prefixPositions.length);
                for (final int pos : prefixPositions) {
                    prefix.add(row[pos]);
                }
                byPrefix.computeIfAbsent(prefix, k -> new ArrayList<>()).add(row);
            }
            for (final LookupRequest request : missed) {
                final List<Object[]> rows = byPrefix.getOrDefault(request.prefix(), List.of());
                if (!rows.isEmpty() || cacheSpec.cacheEmptyResults()) {
                    cache.put(new CacheKey(table, indexName, request, projectsKey),
                            List.copyOf(rows));
                }
            }
        }
        return out;
    }

    private Cache<CacheKey, List<Object[]>> buildCache() {
        CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder()
                .maximumSize(cacheSpec.maxSize());
        if (cacheSpec.expireAfterSeconds() > 0) {
            builder = builder.expireAfterWrite(cacheSpec.expireAfterSeconds(), TimeUnit.SECONDS);
        }
        if (cacheTicker != null) {
            builder = builder.ticker(cacheTicker);
        }
        return builder.build();
    }

    /**
     * Positions of the leading {@code prefixLength} key columns of the chosen
     * candidate key within the projected row, or {@code null} when a key column
     * is not part of the projection.
     */
    private int[] prefixPositions(String table, String indexName, int prefixLength,
            int[] projects) {
        List<String> keyColumns = null;
        for (final LookupKey candidate : keyCandidates(table)) {
            if (Objects.equals(candidate.indexName(), indexName)) {
                keyColumns = candidate.columns();
                break;
            }
        }
        if (keyColumns == null || prefixLength > keyColumns.size()) {
            return null;
        }
        final Schema schema = tableSchemas().get(table);
        if (schema == null) {
            return null;
        }
        final int[] positions = new int[prefixLength];
        for (int i = 0; i < prefixLength; i++) {
            final int ordinal = fieldOrdinal(schema, keyColumns.get(i));
            if (ordinal < 0) {
                return null;
            }
            int position = ordinal;
            if (projects != null) {
                position = -1;
                for (int j = 0; j < projects.length; j++) {
                    if (projects[j] == ordinal) {
                        position = j;
                        break;
                    }
                }
                if (position < 0) {
                    return null;
                }
            }
            positions[i] = position;
        }
        return positions;
    }

    private static int fieldOrdinal(Schema schema, String fieldName) {
        for (int i = 0; i < schema.countFields(); i++) {
            if (schema.getField(i).getName().equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    private static List<Integer> toProjectsKey(int[] projects) {
        if (projects == null) {
            return null;
        }
        final List<Integer> key = new ArrayList<>(projects.length);
        for (final int project : projects) {
            key.add(project);
        }
        return key;
    }
}
