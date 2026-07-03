package com.mercari.solution.util.pipeline.lookup;

import com.mercari.solution.module.Schema;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

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
 */
public abstract class LookupSource implements Serializable, AutoCloseable {

    private final String name;

    private transient long lookupSourceId;
    private transient boolean registered;

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
    }

    /** Deregisters the source and closes clients; the instance can be set up again. */
    @Override
    public final void close() {
        if (registered) {
            LookupSourceRegistry.deregister(lookupSourceId);
            this.registered = false;
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
     * Fetches rows of {@code table} matching the batch via the chosen key.
     * {@code indexName} is {@code null} for the primary key, or a unique index
     * name. May return a superset; the lookup-join operator filters to exact
     * matches. Returned rows are in the {@code projects} column order (null =
     * all columns) using Calcite-internal value types (see {@link CalciteValues}),
     * and must include the key columns referenced by the request.
     */
    public abstract Iterable<Object[]> lookup(
            String table, String indexName, LookupBatch batch, int[] projects);
}
