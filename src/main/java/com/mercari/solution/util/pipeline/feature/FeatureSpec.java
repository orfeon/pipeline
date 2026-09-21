package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.pipeline.feature.SourceContract.Json;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The "intent" side of the DSL (docs/design/feature-dsl.md §3–§5): the parsed {@code parameters} block of a
 * {@code feature} transform. Parsing is syntactic only; cross-references and semantics are resolved by
 * {@link FeaturePlanCompiler}.
 */
public class FeatureSpec implements Serializable {

    public enum Scope { row, context, sequence, population }
    public enum FitMode {
        expanding, fold, forward, statik;

        /** static / fold / forward: statistics fitted over the input and applied by lookup (a fit stage, not a keyed stage). */
        public boolean isLookup() { return this != expanding; }

        /** The value written to the {@code fit} column coordinate. */
        public String token() { return this == statik ? "static" : name(); }

        public static boolean isLookupToken(final String token) { return "static".equals(token) || "fold".equals(token) || "forward".equals(token); }
    }
    public enum NullPolicy { keep, fillZero, indicator }
    public enum Combine { product, zip }
    /** The values of a sequence block's {@code direction} (null = past). */
    public static final List<String> DIRECTIONS = List.of("past", "future");

    public record LineageEntry(List<String> fields, String from, String eventTime) implements Serializable {}
    public record EntityDef(String name, List<String> keys, Duration minInterval) implements Serializable {}
    public record ContextDef(String name, List<String> keys) implements Serializable {}
    /** {@code emit}: output column name of the baseline value (baselines are intermediate unless emitted). */
    public record BaselineDef(String name, String context, String expr, String emit) implements Serializable {}

    public static class Window implements Serializable {
        public Integer maxEvents;
        public Duration maxAge;
        public String filter;
        /** The clock {@code maxAge} is measured on: null / {@code time} (wall time) or a calendar declared in the sources. */
        public String clock;
        /** {@code maxAge} on a calendar clock: a number of ticks. */
        public Long maxAgeTicks;
        /**
         * The window's name in generated column names, replacing the derived token. A filter has no token of its own, so
         * a filter-only window is {@code all} — the name of the unconditional window: naming it is what lets the two
         * stand side by side in one block (a statistic over everything next to the one per {@code $self} pool).
         */
        public String as;

        /** A window measured on a calendar clock. */
        public boolean onCalendar() {
            return maxAgeTicks != null;
        }

        /**
         * Whether two windows select the same rows: the far edge, the count, the clock and the condition. {@code as}
         * is a display name, not part of the window — two windows are the same window whether or not they are named,
         * and two windows that select different rows are different however they are named.
         */
        public boolean sameBounds(final Window other) {
            return other != null && Objects.equals(maxAge, other.maxAge) && Objects.equals(maxEvents, other.maxEvents)
                    && Objects.equals(maxAgeTicks, other.maxAgeTicks) && Objects.equals(clock, other.clock)
                    && Objects.equals(filter, other.filter);
        }

        /** Short token for generated names (§4.3): 365d, n20, 365d_n20, 20trading, all — or the declared {@code as}. */
        public String token() {
            if (as != null) return as;
            final List<String> parts = new ArrayList<>();
            if (maxAge != null) parts.add(Durations.shortName(maxAge));
            if (maxAgeTicks != null) parts.add(maxAgeTicks + clock);
            if (maxEvents != null) parts.add("n" + maxEvents);
            return parts.isEmpty() ? "all" : String.join("_", parts);
        }
    }

    public static class Op implements Serializable {
        public String type;
        public List<String> fields = new ArrayList<>();
        public String expr;
        public String predicate;
        public Integer k;
        public List<Double> halflife = new ArrayList<>();
        public List<String> funcs = new ArrayList<>();
        public String value;
        public List<String> unit = new ArrayList<>();
        public String decayBy;
        /** regression: the explanatory series ({@code field} is regressed against it; not {@code on}, a YAML 1.1 boolean). */
        public String against;
        /** regression: pair {@code field} with {@code against} this many events earlier (lead-lag; null / 0 = same event). */
        public Integer lag;
        /** fracdiff: the differencing order (0 < d < 1 keeps memory; 1 = the first difference). */
        public Double d;
        /** barrier (future windows): the relative move from the current row's value that counts as touching the upper / lower barrier. */
        public Double up;
        public Double down;
        /** Output name override (replaces the field / anonymous-expression segment, or the op suffix). */
        public String as;
        /** countByValue / ratioByValue: emit one column per listed value instead of a map. */
        public List<String> values = new ArrayList<>();
        /** softmax: the offset (a baselines[].name or a column) whose value weights each row in probability space. */
        public String offset;
        /** softmax: temperature (default 1); a {@code temperatureFrom} URI is resolved into it before compile. */
        public Double temperature;
        /** softmax: where {@code temperature} was read from (URI) and the content hash of that document. */
        public String temperatureSource;
        public String temperatureHash;
        /** softmax: {@code probability} (default: the offset is a probability / weight) | {@code log} (exp is applied first). */
        public String offsetScale;
        /** softmax: {@code zero} (default: a null score falls back to the offset) | {@code null} (the row's output is null). */
        public String scoreNull;
        /** shuffle: the seed of the deterministic permutation. */
        public Long seed;
        /** residualize (context): the fields the op's field is regressed against within the group ({@code against: [a, b]}). */
        public List<String> regressors = new ArrayList<>();
        /** harville (context): the places {@code k} whose "finishes within the first k" probability is emitted (1..3). */
        public List<Integer> top = new ArrayList<>();
        /** harville: the exponents applied to the probabilities when the 2nd / 3rd place is drawn (1 = plain Harville). */
        public List<Double> discount = new ArrayList<>();
        /** harville: groups with more valid rows than this read null (the op is cubic in the group size). */
        public Integer maxGroupSize;
        /**
         * sequence aggregate: a numeric expression giving every past event its weight; it reads the event's fields by
         * name and the current row's through {@code $self.<field>} (a similarity kernel). Null / NaN / non-positive
         * weights contribute nothing.
         */
        public String weightBy;
        /**
         * rating: the update rule ({@code elo | bradleyTerry | plackettLuce}), the contexts[].name whose groups are the
         * contests, and whether a smaller outcome ({@code ascending}, the default: a rank) or a larger one
         * ({@code descending}: a score) is the better.
         */
        public String method;
        public String context;
        public String order;
        /** rating: the prior ({@code mu}, {@code sigma}), the performance noise {@code beta}, the per-contest drift {@code tau}; elo's {@code kFactor} / {@code scale}. */
        public Double mu;
        public Double sigma;
        public Double beta;
        public Double tau;
        public Double kFactor;
        public Double scale;
        /**
         * rating: the time {@code tau} is the drift of — a player's variance grows by {@code tau² · Δt / tauPer} over the
         * time {@code Δt} since its previous contest instead of by {@code tau²} per contest (null), so a long absence
         * reopens the uncertainty.
         */
        public Duration tauPer;
        /** rating (bradleyTerry): which opponents a player is paired with — {@code all} (default) | {@code adjacent} | {@code mean}. */
        public String pairs;
        /**
         * rating: the other entities of the row that are rated with the block's entity as one team (the row's strength is
         * the sum of its members', a contest's change is shared among them by their part of the team's variance).
         */
        public List<TeamMember> with = new ArrayList<>();
        /** rating with a team: the readouts of the whole team ({@code mu} = the members' sum, {@code sigma} = its uncertainty). */
        public List<String> team = new ArrayList<>();
        /** rating: {@code with} / {@code team} were declared but are not a list of members / of readout names. */
        public String withInvalid;
    }

    /** The keys a {@link TeamMember} accepts; anything else is reported (see {@link TeamMember#unknown}). */
    static final List<String> TEAM_MEMBER_KEYS = List.of("entity", "mu", "sigma", "tau");

    /** A member of a rating team: an {@code entities[].name} and its own prior / drift (null = the op's). */
    public static class TeamMember implements Serializable {
        public String entity;
        public Double mu;
        public Double sigma;
        public Double tau;
        /** Keys other than entity / mu / sigma / tau (reported, so a misspelled parameter does not silently default). */
        public List<String> unknown = new ArrayList<>();
    }

    /**
     * The general form of a sequence feature (§1.4, §4.3): {@code lift} maps an event to channels, {@code summarize}
     * folds the channels of a window into a fixed-length state. Only {@code dynamics} summaries exist.
     */
    public static class Lift implements Serializable {
        /** The channels: fields (or block columns) read from each event. */
        public List<String> fields = new ArrayList<>();
        /** Expression channels (each desugared into an anonymous row column, like an op's {@code expr}). */
        public List<LiftExpr> exprs = new ArrayList<>();
        /** Adds the constant channel 1 (named {@code time}): the measure's own components describe when events happened. */
        public boolean timeAugment;
    }

    /**
     * An expression channel: {@code "expr"} or {@code {expr: "...", as: name}} — {@code as} names the channel segment
     * of its columns (otherwise the anonymous {@code {block}__e{n}}, numbered across the whole spec).
     */
    public record LiftExpr(String expr, String as) implements Serializable {}

    /** {@code summarize.dynamics}: the recurrence summarising the lifted channels. */
    public static class DynamicsSpec implements Serializable {
        /** {@code lti} (implemented) | {@code bilinear} | {@code probabilistic}. */
        public String family;
        /** lti: {@code exponential} | {@code legendre} | {@code fourier}. */
        public String measure;
        public Integer order;
        /** exponential (required) / fourier (optional damping): one state per halflife, in clock units. */
        public List<Double> halflife = new ArrayList<>();
        /** fourier: the period of the first harmonic, in clock units. */
        public Double period;
        /** The clock: {@code events} (default) | {@code time} (days) | a calendar (ticks); for bilinear, the clock of the time channel. */
        public String decayBy;
        /** bilinear: {@code logsignature}. */
        public String type;
        /** bilinear: the truncation depth of the signature. */
        public Integer depth;
        /** Keys other than the known ones (reported, so a misspelled parameter does not silently default). */
        public List<String> unknown = new ArrayList<>();
    }

    public static class KeySet implements Serializable {
        public List<String> keys = new ArrayList<>();
        public List<Window> windows = new ArrayList<>();
        public String structure;
        public String hierarchyJson;
        public String shrinkageJson;
        public String parentRef;
        public Integer maxDepth;
        /**
         * The keySet's name in generated column names, replacing the {@code {keys}} segment (the keys joined by
         * {@code _}) — a path of lag columns is otherwise a name of a hundred characters. The hidden level statistics
         * keep their key-derived names: they are shared by every keySet of the block whose lattice contains the level.
         */
        public String as;
    }

    /** One factorization output: {@code pair: [a, b]}, {@code embedding: field (dims)} or {@code sum: true}. */
    public static class FmOutput implements Serializable {
        public String kind;
        public List<String> pair = new ArrayList<>();
        public String embedding;
        public Integer dims;
        public String as;
    }

    public static class Target implements Serializable {
        public String field;
        public String expr;
        public boolean ref;
        public List<String> stats = new ArrayList<>();
        /** Target name override (replaces the field name or the anonymous e{n}). */
        public String as;
        /** stat distribution: emit one FLOAT64 column per listed category instead of the map column. */
        public List<String> values = new ArrayList<>();
    }

    public static class FeatureDef implements Serializable {
        public String name;
        public Scope scope;
        public String type;
        public String computeAtExpression;
        public Duration validFor;

        // row
        public String expr;
        public String input;
        public List<String> inputs = new ArrayList<>();
        public List<String> derive = new ArrayList<>();
        public boolean cyclical;
        public List<Double> edges = new ArrayList<>();
        public List<String> values = new ArrayList<>();
        public String baseline;
        public String on;
        /** noise: {@code normal} (default) | {@code uniform}; the draw's seed is the shared {@code seed} field. */
        public String distribution;
        /** vector: the readouts to emit (one column each; {@code polyfit} one per coefficient up to {@code degree}). */
        public List<String> funcs = new ArrayList<>();
        /** vector: {@code slice: {from, to}} — elements [from, to), negative = from the end, null = open. */
        public Integer sliceFrom;
        public Integer sliceTo;
        /** vector: {@code slice} was declared in a form other than an object (a compile error). */
        public boolean sliceMalformed;
        /** vector: differencing order applied after the slice (null = none). */
        public Integer diff;
        /** vector: rescaling applied after the differencing ({@code sum | mean | l2 | zscore}; null = none). */
        public String normalize;
        /** vector: element positions for {@code slope} / {@code polyfit}: {@code index} (default) | {@code unit}. */
        public String position;
        /** vector: polynomial degree of the {@code polyfit} readout (null = 2). */
        public Integer degree;

        // context / sequence
        public String context;
        public boolean excludeSelf;
        public List<Op> ops = new ArrayList<>();
        public String entity;
        /**
         * sequence: {@code past} (default — strictly-past windows, features) or {@code future} — strictly-future
         * windows {@code (t, t + maxAge]}: label columns, post-event by construction (§4.3 labels).
         */
        public String direction;
        public List<Window> windows = new ArrayList<>();
        /** sequence general form: null when absent. */
        public Lift lift;
        /** sequence general form: the {@code summarize.dynamics} block, null when absent. */
        public DynamicsSpec dynamics;
        /** sequence general form: {@code summarize} was declared (with or without a usable {@code dynamics}). */
        public boolean summarize;
        /** sequence general form: {@code compress} was declared. */
        public boolean compress;
        /** sequence general form: the {@code compress} object ({@code {svd: {rank, center, standardize, fit}, keep}}), as JSON. */
        public String compressJson;

        // population
        public List<KeySet> keySets = new ArrayList<>();
        public List<Target> targets = new ArrayList<>();
        public Combine combine = Combine.product;
        public String naming;
        public Integer maxFeatures;
        public String offset;
        public boolean emitConfidence;
        public String shrinkageJson;
        public String smoothingJson;
        public String fitJson;

        // factorization
        public String variant;
        public List<String> fields = new ArrayList<>();
        public Integer latentDim;
        public String taskTarget;
        public String taskTargetExpr;
        public String taskOffset;
        public List<FmOutput> fmOutputs = new ArrayList<>();
        public Integer epochs;
        public Double reg;
        public Long seed;
        // discretize / quantileTransform ({@code bins}, {@code distribution} shared with noise)
        public String method;
        public Integer bins;
        public Integer minSamplesPerBin;
        /** quantileTransform: probability clamp of the normal score (null = the default 1e-6). */
        public Double clip;
        public String target;
        // svd
        public Integer rank;
        /** null = the type's default (svd: centre). */
        public Boolean center;
        public boolean standardize;
        /** svd: what to emit — {@code scores} (default) | {@code residual} (per input, in input units) | {@code residualNorm}. */
        public List<String> outputs = new ArrayList<>();
        // smooth ({@code input}, {@code target}, {@code method}, {@code degree}, {@code outputs} shared with the types above)
        /** smooth: equal intervals of the range the B-splines span (null = the default). */
        public Integer segments;
        /** smooth: {@code [lo, hi]} of the key; empty = not declared. */
        public List<Double> range = new ArrayList<>();
        /** smooth: order of the difference penalty (null = the default). */
        public Integer penaltyOrder;
        /** smooth: {@code penalty.lambda} as written — {@code reml} or a number (null = reml). */
        public String penaltyLambda;
        /** smooth: keys of the {@code penalty} block other than {@code order} / {@code lambda}. */
        public List<String> penaltyUnknown = new ArrayList<>();
        // spectralEmbedding / transitionStats: the values an entity takes one after another ({@code rank} shared with svd)
        /** {@code sequenceOf.entity} / {@code sequenceOf.field}. */
        public String sequenceEntity;
        public String sequenceField;
        /** spectralEmbedding: {@code cooccur.window} (steps back that count as co-occurring) / {@code cooccur.weighting}. */
        public Integer cooccurWindow;
        public String cooccurWeighting;
        /** spectralEmbedding: which value of the row is embedded — {@code current} (default) | {@code previous}. */
        public String embedOf;
        /** spectralEmbedding: vocabulary cap (null = the default). */
        public Integer maxValues;
        /** transitionStats: how many previous values make the state (null = 1). */
        public Integer order;
        /** transitionStats {@code emit}: the {@code toValueProb} values, and whether the whole {@code distribution} map is emitted. */
        public List<String> emitValues = new ArrayList<>();
        public boolean emitDistribution;
        /** transitionStats {@code blend}: null when the block is absent. */
        public Boolean blendPerEntity;
        public Double blendPriorWeight;
        /** Entries / keys of {@code sequenceOf}, {@code cooccur}, {@code emit}, {@code blend} that were not understood. */
        public List<String> sequenceUnknown = new ArrayList<>();

        public String location() {
            return "features." + name;
        }
    }

    public static class FitSpec implements Serializable {
        public String orderBy;
        public FitMode mode = FitMode.expanding;
        public Duration minHistory;
        public String groupBy;
        /** Number of folds for {@code fit.mode: fold} (out-of-fold statistics). */
        public Integer folds = 5;
        /**
         * {@code fit.fold.by}: {@code row} (default — {@code folds} hash folds of the row identity or the groupBy entity)
         * or {@code time} — every time block ({@code fit.blocks}) is a fold, and a row reads the totals minus its own
         * block, the {@code purge} on both sides of it and the {@code embargo} beyond the purge after it.
         */
        public String foldBy;
        /** {@code fit.fold.purge}: the span left out on both sides of the row's block (default: the target label's horizon). */
        public Duration purge;
        /** {@code fit.fold.embargo}: the extra span left out after the purge that follows the row's block (default none). */
        public Duration embargo;

        public boolean isTimeFold() {
            return "time".equals(foldBy);
        }

        /** Parses {@code fold: {by: row | time, purge, embargo}} of a fit block (top level or per feature). */
        static void parseFold(final JsonObject fit, final FitSpec spec, final Diagnostics diagnostics, final String loc) {
            if (fit == null || !fit.has("fold") || fit.get("fold").isJsonNull()) return;
            if (!fit.get("fold").isJsonObject()) {
                diagnostics.error("fit.fold", loc, "fit.fold must be an object: {by: row | time, purge: <ISO-8601 duration>, embargo: <ISO-8601 duration>}");
                return;
            }
            final JsonObject fold = fit.getAsJsonObject("fold");
            final String by = Json.string(fold, "by");
            if (by != null && !List.of("row", "time").contains(by)) {
                diagnostics.error("fit.fold.by", loc, "fit.fold.by must be row | time: " + by);
            } else if (by != null) {
                spec.foldBy = by;
            }
            final Duration purge = Json.duration(fold, "purge", null, diagnostics, loc);
            final Duration embargo = Json.duration(fold, "embargo", null, diagnostics, loc);
            if (purge != null && purge.isNegative() || embargo != null && embargo.isNegative()) {
                diagnostics.error("fit.fold.negative", loc, "fit.fold.purge / embargo must not be negative: purge=" + purge + " embargo=" + embargo);
            }
            if (purge != null && !purge.isNegative()) spec.purge = purge;
            if (embargo != null && !embargo.isNegative()) spec.embargo = embargo;
            for (final String key : fold.keySet()) {
                if (!List.of("by", "purge", "embargo").contains(key)) {
                    diagnostics.error("fit.fold", loc, "unknown fit.fold key '" + key + "' (accepted: by, purge, embargo)");
                }
            }
        }
        /** Root URI of fit artifacts ({@code <uri>/<planHash>/<block>.avro}); null = fit in-pipeline only. */
        public String artifactUri;
        /** Re-fit and overwrite even when an artifact for the plan hash exists. */
        public boolean refit;
        /** Explicit artifact version replacing the plan hash in artifact paths (pin a fitted version). */
        public String artifactId;
        /** fit.mode forward: the time blocks ({@code blocks.bucket} calendar bucket, else {@code blocks.size}; default P90D). */
        public String blockBucket;
        public Duration blockSize;
        /** fit.blocks on a calendar clock: {@code blocks: {size: <ticks>, clock: <name>}} (resolved to {@link #blockCalendar} by the compiler). */
        public String blockClock;
        public Integer blockTicks;
        public Clock blockCalendar;
        /** fit.mode forward: rows with fewer usable preceding blocks (with data for the key) read null. */
        public Integer minBlocks;
        /**
         * The fewest rows a lookup fit (smooth / svd / quantileTransform / spectralEmbedding) is solved from; a fit —
         * the whole input's, or one time-block window's under forward — with fewer contributes null. Null = the type's
         * default (smooth: its number of coefficients; the others: no floor), 0 = no floor.
         */
        public Integer minRows;
        /**
         * fit.mode forward: the range of blocks a row reads, {@code (usable − window, usable]}, rounded up to whole
         * blocks — the block-level default of a keySet {@code window.maxAge}, and the window of a static-fit block
         * (svd) that has no keySet. Null = every usable block.
         */
        public Duration window;

        /** fit.mode forward: the minimum blocks a row must be able to read — {@code minBlocks}, else {@code minHistory} rounded up to blocks, else 1. */
        public int minBlocksOf(final ForwardBlocks blocks) {
            if (minBlocks != null) return minBlocks;
            if (minHistory != null) return blocks.windowBlocks(minHistory);
            return 1;
        }

        /** The forward blocks of this spec (defaults applied). */
        public ForwardBlocks forwardBlocks() {
            if (blockCalendar != null && blockTicks != null) return ForwardBlocks.ofClock(blockCalendar, blockTicks);
            return blockBucket != null ? ForwardBlocks.ofBucket(blockBucket) : ForwardBlocks.ofSize(blockSize == null ? ForwardBlocks.DEFAULT_SIZE : blockSize);
        }

        /** Parses {@code blocks: {field, bucket, size}} and {@code minBlocks} of a fit block (top level or per feature). */
        static void parseForward(final JsonObject fit, final FitSpec spec, final Diagnostics diagnostics, final String loc, final String timeField) {
            if (fit == null) return;
            if (fit.has("blocks") && !fit.get("blocks").isJsonNull()) {
                if (!fit.get("blocks").isJsonObject()) {
                    diagnostics.error("fit.blocks", loc, "fit.blocks must be an object: {bucket: year | quarter | month | week | day}, {size: <ISO-8601 duration>} or {size: <ticks>, clock: <calendar>}");
                } else {
                    final JsonObject blocks = fit.getAsJsonObject("blocks");
                    final String field = Json.string(blocks, "field");
                    if (field != null && timeField != null && !field.equals(timeField)) {
                        diagnostics.error("fit.blocks.field", loc, "fit.blocks.field must be time.field (" + timeField + "): " + field);
                    }
                    final String bucket = Json.string(blocks, "bucket");
                    final String clock = Json.string(blocks, "clock");
                    final boolean onCalendar = clock != null && !"time".equals(clock);
                    final Duration size = onCalendar ? null : Json.duration(blocks, "size", null, diagnostics, loc);
                    if (onCalendar) {
                        // blocks of n ticks of a calendar clock
                        final JsonElement ticks = blocks.get("size");
                        if (bucket != null || ticks == null || !ticks.isJsonPrimitive() || !ticks.getAsJsonPrimitive().isNumber()
                                || ticks.getAsDouble() != Math.floor(ticks.getAsDouble()) || ticks.getAsInt() < 1) {
                            diagnostics.error("fit.blocks.clock", loc, "fit.blocks on the clock '" + clock + "' takes size: <whole number of ticks >= 1> (no bucket): " + blocks);
                        } else {
                            spec.blockClock = clock;
                            spec.blockTicks = ticks.getAsInt();
                            spec.blockBucket = null;
                            spec.blockSize = null;
                        }
                    } else if (bucket != null && size != null) {
                        diagnostics.error("fit.blocks", loc, "fit.blocks takes either bucket or size, not both");
                    } else if (bucket != null) {
                        if (!ForwardBlocks.BUCKETS.contains(bucket)) {
                            diagnostics.error("fit.blocks.bucket", loc, "fit.blocks.bucket must be one of " + ForwardBlocks.BUCKETS + ": " + bucket);
                        } else {
                            spec.blockBucket = bucket;
                            spec.blockSize = null;
                            spec.blockClock = null;
                            spec.blockTicks = null;
                        }
                    } else if (size != null) {
                        if (size.isZero() || size.isNegative()) {
                            diagnostics.error("fit.blocks.size", loc, "fit.blocks.size must be positive: " + size);
                        } else {
                            spec.blockSize = size;
                            spec.blockBucket = null;
                            spec.blockClock = null;
                            spec.blockTicks = null;
                        }
                    } else {
                        diagnostics.error("fit.blocks", loc, "fit.blocks requires bucket or size");
                    }
                }
            }
            final Integer minBlocks = Json.integer(fit, "minBlocks");
            if (minBlocks != null) {
                if (minBlocks < 1) diagnostics.error("fit.minBlocks", loc, "fit.minBlocks must be >= 1: " + minBlocks);
                else spec.minBlocks = minBlocks;
            }
            final Integer minRows = Json.integer(fit, "minRows");
            if (minRows != null) {
                if (minRows < 0) diagnostics.error("fit.minRows", loc, "fit.minRows must be >= 0 (0 = no floor): " + minRows);
                else spec.minRows = minRows;
            }
            // the fit window (a duration on the time axis; ISO-8601 like blocks.size) and the minimum history
            if (fit.has("window") && !fit.get("window").isJsonNull()) {
                if (fit.get("window").isJsonPrimitive()) {
                    final Duration window = Json.duration(fit, "window", null, diagnostics, loc);
                    if (window != null) {
                        if (window.isZero() || window.isNegative()) diagnostics.error("fit.window", loc, "fit.window must be a positive duration: " + window);
                        else spec.window = window;
                    }
                } else {
                    diagnostics.error("fit.window", loc, "fit.window must be an ISO-8601 duration (the range of blocks a row reads, rounded up to whole blocks)");
                }
            }
            final Duration minHistory = Json.duration(fit, "minHistory", null, diagnostics, loc);
            if (minHistory != null) {
                if (minHistory.isZero() || minHistory.isNegative()) diagnostics.error("fit.minHistory", loc, "fit.minHistory must be a positive duration: " + minHistory);
                else spec.minHistory = minHistory;
            }
        }

        static void parseArtifact(final JsonObject fit, final FitSpec spec) {
            if (fit == null || !fit.has("artifact")) return;
            final JsonElement a = fit.get("artifact");
            if (a.isJsonPrimitive()) {
                spec.artifactUri = a.getAsString();
            } else if (a.isJsonObject()) {
                spec.artifactUri = Json.string(a.getAsJsonObject(), "uri");
                spec.refit = Json.bool(a.getAsJsonObject(), "refit", false);
                if (Json.string(a.getAsJsonObject(), "id") != null) spec.artifactId = Json.string(a.getAsJsonObject(), "id");
            }
        }
    }

    public static class OutputSpec implements Serializable {
        public String prefix = "";
        public NullPolicy nullPolicy = NullPolicy.keep;
        public List<String> exclude = new ArrayList<>();
        public String groupBy;
        public List<String> parentFields = new ArrayList<>();
        /** Field name of the child array in grouped output (default "rows"; rename to dodge reserved words). */
        public String childName = "rows";
        /** Which input fields pass through to the output: all (default) | keys (time.field, entity / context keys, parentFields) | none. */
        public String passThrough = "all";
        /** Data-contract roles (group / time / entity / label / baseline / weight → input field, context, entity or baseline name); a role column is never a feature. */
        public Map<String, String> roles = new LinkedHashMap<>();
        /** Output projection: the columns to emit (canonical or output names); null = not declared (exclude applies). */
        public List<String> include;
        /** Where {@code include} came from when it was a URI (resolved by FeaturePlanService before compile). */
        public String includeSource;
        /** SHA-256 (16 hex) of the resolved include list content, recorded in the manifest / output hash. */
        public String includeHash;
        /** URI of the assembly-time manifest ({@code manifest.json}); the run manifest is written next to it. */
        public String manifest;

        public static final List<String> ROLE_NAMES = List.of("group", "time", "entity", "label", "baseline", "weight");
    }

    /** The observedAt audit (sources with {@code observedAtField}): what to do with a row observed after its declared availability. */
    public static class AuditSpec implements Serializable {
        /** count (default: counters + run manifest) | fail (route the row to the failure output) | off. */
        public String observedAt = "count";
    }

    /** Engine (runtime) knobs that do not change the plan: {@code engine.spill} of the keyed stages' sorter, the wave fan-out. */
    public static class EngineSpec implements Serializable {
        /** Evaluate the independent stages of a wave in parallel and merge them by row id (engine doc §9.4); false = linear chain. */
        public boolean parallelWaves = true;
        /** Input fields identifying a row for the fan-out merge; empty = a random id pinned by a Reshuffle. */
        public List<String> rowId = new ArrayList<>();
        /** In-memory sort buffer per key (MB); null = derived from the worker heap. */
        public Integer spillMemoryMB;
        /** Spill directory on the worker; null = java.io.tmpdir. */
        public String spillDirectory;
        /** Deflate the spilled chunk files. */
        public boolean spillCompress = false;
    }

    public List<LineageEntry> lineage = new ArrayList<>();
    public EngineSpec engine = new EngineSpec();
    public AuditSpec audit = new AuditSpec();
    /**
     * Values resolved from external documents at assembly ({@code temperatureFrom}): {@code location=source:hash:value}.
     * Outside the plan hash (no fit depends on them), inside the output hash and the manifest.
     */
    public List<String> resolvedExternals = new ArrayList<>();
    public String timeField;
    public List<String> orderTieBreak = new ArrayList<>();
    public String predictAtExpression;
    public AvailableAt predictAt;
    public List<EntityDef> entities = new ArrayList<>();
    public List<ContextDef> contexts = new ArrayList<>();
    public List<BaselineDef> baselines = new ArrayList<>();
    public List<FeatureDef> features = new ArrayList<>();
    public FitSpec fit = new FitSpec();
    public OutputSpec output = new OutputSpec();

    public static FeatureSpec parse(final JsonObject parameters, final Diagnostics diagnostics) {
        final FeatureSpec spec = new FeatureSpec();

        if (parameters.has("lineage") && parameters.get("lineage").isJsonArray()) {
            for (final JsonElement e : parameters.getAsJsonArray("lineage")) {
                if (!e.isJsonObject()) continue;
                final JsonObject o = e.getAsJsonObject();
                final List<String> fields = Json.strings(o, "fields");
                final String from = Json.string(o, "from");
                if (fields.isEmpty() || from == null) {
                    diagnostics.error("lineage.invalid", "lineage", "each lineage entry requires 'fields' and 'from'");
                    continue;
                }
                spec.lineage.add(new LineageEntry(fields, from, Json.string(o, "eventTime")));
            }
        } else {
            diagnostics.error("lineage.missing", "lineage", "lineage is required (fields → source mapping)");
        }

        if (parameters.has("time") && parameters.get("time").isJsonObject()) {
            final JsonObject time = parameters.getAsJsonObject("time");
            spec.timeField = Json.string(time, "field");
            spec.orderTieBreak = Json.strings(time, "orderTieBreak");
        }
        if (spec.timeField == null) {
            diagnostics.error("time.field", "time", "time.field is required");
        }

        spec.predictAtExpression = Json.string(parameters, "predictAt");
        if (spec.predictAtExpression == null) {
            diagnostics.error("predictAt.missing", "predictAt", "predictAt is required");
            spec.predictAt = AvailableAt.atEventTime();
        } else {
            try {
                spec.predictAt = AvailableAt.parseTimeExpression(spec.predictAtExpression);
                if (!spec.predictAt.isStatic()) {
                    diagnostics.error("predictAt.invalid", "predictAt", "predictAt must be event_time ± duration");
                }
            } catch (final IllegalArgumentException e) {
                diagnostics.error("predictAt.invalid", "predictAt", e.getMessage());
                spec.predictAt = AvailableAt.atEventTime();
            }
        }

        for (final JsonObject o : objects(parameters, "entities")) {
            final String name = Json.string(o, "name");
            final List<String> keys = Json.strings(o, "keys");
            if (name == null || keys.isEmpty()) {
                diagnostics.error("entities.invalid", "entities", "each entity requires 'name' and 'keys'");
                continue;
            }
            spec.entities.add(new EntityDef(name, keys, Json.duration(o, "minInterval", null, diagnostics, "entities." + name)));
        }
        for (final JsonObject o : objects(parameters, "contexts")) {
            final String name = Json.string(o, "name");
            final List<String> keys = Json.strings(o, "keys");
            if (name == null || keys.isEmpty()) {
                diagnostics.error("contexts.invalid", "contexts", "each context requires 'name' and 'keys'");
                continue;
            }
            spec.contexts.add(new ContextDef(name, keys));
        }
        for (final JsonObject o : objects(parameters, "baselines")) {
            final String name = Json.string(o, "name");
            final String expr = Json.string(o, "expr");
            if (name == null || expr == null) {
                diagnostics.error("baselines.invalid", "baselines", "each baseline requires 'name' and 'expr'");
                continue;
            }
            spec.baselines.add(new BaselineDef(name, Json.string(o, "context"), expr, Json.string(o, "emit")));
        }

        for (final JsonObject o : objects(parameters, "features")) {
            final FeatureDef def = parseFeature(o, diagnostics);
            if (def != null) spec.features.add(def);
        }
        if (spec.features.isEmpty()) {
            diagnostics.error("features.missing", "features", "features must not be empty");
        }

        if (parameters.has("fit") && parameters.get("fit").isJsonObject()) {
            final JsonObject fit = parameters.getAsJsonObject("fit");
            spec.fit.orderBy = Json.string(fit, "orderBy");
            spec.fit.mode = parseFitMode(Json.string(fit, "mode"), diagnostics, "fit");
            // minHistory / window / blocks / minBlocks are parsed (and validated) by parseForward below
            spec.fit.groupBy = Json.string(fit, "groupBy");
            if (Json.integer(fit, "folds") != null) spec.fit.folds = Json.integer(fit, "folds");
            FitSpec.parseFold(fit, spec.fit, diagnostics, "fit");
            FitSpec.parseArtifact(fit, spec.fit);
            FitSpec.parseForward(fit, spec.fit, diagnostics, "fit", spec.timeField);
        }

        if (parameters.has("output") && parameters.get("output").isJsonObject()) {
            final JsonObject out = parameters.getAsJsonObject("output");
            spec.output.prefix = Json.string(out, "prefix") == null ? "" : Json.string(out, "prefix");
            final String nullPolicy = Json.string(out, "nullPolicy");
            if (nullPolicy != null) {
                try {
                    spec.output.nullPolicy = NullPolicy.valueOf(nullPolicy);
                } catch (final IllegalArgumentException e) {
                    diagnostics.error("output.nullPolicy", "output", "nullPolicy must be keep | fillZero | indicator");
                }
            }
            spec.output.exclude = Json.strings(out, "exclude");
            spec.output.groupBy = Json.string(out, "groupBy");
            spec.output.parentFields = Json.strings(out, "parentFields");
            if (Json.string(out, "childName") != null) spec.output.childName = Json.string(out, "childName");
            if (Json.string(out, "passThrough") != null) {
                final String passThrough = Json.string(out, "passThrough");
                if (!List.of("all", "keys", "none").contains(passThrough)) {
                    diagnostics.error("output.passThrough", "output", "output.passThrough must be all | keys | none: " + passThrough);
                } else {
                    spec.output.passThrough = passThrough;
                }
            }
            if (out.has("roles") && out.get("roles").isJsonObject()) {
                for (final Map.Entry<String, JsonElement> e : out.getAsJsonObject("roles").entrySet()) {
                    if (!OutputSpec.ROLE_NAMES.contains(e.getKey())) {
                        diagnostics.error("output.roles.unknown", "output.roles", "unknown role '" + e.getKey() + "' (roles: " + OutputSpec.ROLE_NAMES + ")");
                        continue;
                    }
                    if (e.getValue() == null || e.getValue().isJsonNull()) continue;
                    if (!e.getValue().isJsonPrimitive()) {
                        diagnostics.error("output.roles.value", "output.roles", "role '" + e.getKey() + "' must name one field / context / entity / baseline");
                        continue;
                    }
                    spec.output.roles.put(e.getKey(), e.getValue().getAsString());
                }
            } else if (out.has("roles") && !out.get("roles").isJsonNull()) {
                diagnostics.error("output.roles", "output", "output.roles must be an object (group / time / entity / label / baseline / weight)");
            }
            if (out.has("include") && !out.get("include").isJsonNull()) {
                if (out.get("include").isJsonArray()) {
                    spec.output.include = Json.strings(out, "include");
                } else {
                    // a URI is resolved by FeaturePlanService.resolve before the compiler sees the parameters
                    diagnostics.error("output.include.unresolved", "output", "output.include must be a list of column names (a URI is resolved before compile): " + out.get("include"));
                }
            }
            spec.output.includeSource = Json.string(out, "includeSource");
            spec.output.includeHash = Json.string(out, "includeHash");
            spec.output.manifest = Json.string(out, "manifest");
        }
        if (parameters.has("audit") && parameters.get("audit").isJsonObject()) {
            final JsonObject audit = parameters.getAsJsonObject("audit");
            final String observedAt = Json.string(audit, "observedAt");
            if (observedAt != null) {
                if (!List.of("count", "fail", "off").contains(observedAt)) {
                    diagnostics.error("audit.observedAt", "audit", "audit.observedAt must be count | fail | off: " + observedAt);
                } else {
                    spec.audit.observedAt = observedAt;
                }
            }
        }
        if (parameters.has("engine") && parameters.get("engine").isJsonObject()) {
            final JsonObject engine = parameters.getAsJsonObject("engine");
            spec.engine.parallelWaves = Json.bool(engine, "parallelWaves", true);
            spec.engine.rowId = Json.strings(engine, "rowId");
            if (engine.has("spill") && engine.get("spill").isJsonObject()) {
                final JsonObject spill = engine.getAsJsonObject("spill");
                if (spill.has("memoryMB") && !spill.get("memoryMB").isJsonNull()) {
                    final JsonElement memoryMB = spill.get("memoryMB");
                    if (!memoryMB.isJsonPrimitive() || !memoryMB.getAsJsonPrimitive().isNumber() || memoryMB.getAsInt() < 1) {
                        diagnostics.error("engine.spill.memoryMB", "engine.spill", "engine.spill.memoryMB must be an integer >= 1: " + memoryMB);
                    } else {
                        spec.engine.spillMemoryMB = memoryMB.getAsInt();
                    }
                }
                spec.engine.spillDirectory = Json.string(spill, "directory");
                spec.engine.spillCompress = Json.bool(spill, "compress", false);
            }
        }
        for (final FeatureDef def : spec.features) {
            for (int i = 0; i < def.ops.size(); i++) {
                final Op op = def.ops.get(i);
                if (op.temperatureSource != null) {
                    spec.resolvedExternals.add(def.location() + ".ops[" + i + "].temperatureFrom=" + op.temperatureSource + ":" + op.temperatureHash + ":" + op.temperature);
                }
            }
        }
        return spec;
    }

    static FitMode parseFitMode(final String text, final Diagnostics diagnostics, final String location) {
        if (text == null) return FitMode.expanding;
        return switch (text) {
            case "expanding" -> FitMode.expanding;
            case "fold" -> FitMode.fold;
            case "forward" -> FitMode.forward;
            case "static" -> FitMode.statik;
            default -> {
                diagnostics.error("fit.mode", location, "fit.mode must be expanding | fold | forward | static: " + text);
                yield FitMode.expanding;
            }
        };
    }

    private static FeatureDef parseFeature(final JsonObject o, final Diagnostics diagnostics) {
        final FeatureDef def = new FeatureDef();
        def.name = Json.string(o, "name");
        if (def.name == null) {
            diagnostics.error("features.name", "features", "each feature requires 'name'");
            return null;
        }
        final String loc = def.location();
        final String scope = Json.string(o, "scope");
        if (scope == null) {
            diagnostics.error("features.scope", loc, "scope is required (row | context | sequence | population)");
            return null;
        }
        try {
            def.scope = Scope.valueOf(scope);
        } catch (final IllegalArgumentException e) {
            diagnostics.error("features.scope", loc, "scope must be row | context | sequence | population: " + scope);
            return null;
        }
        def.type = Json.string(o, "type");
        def.computeAtExpression = Json.string(o, "computeAt");
        def.validFor = Json.duration(o, "validFor", null, diagnostics, loc);

        def.expr = Json.string(o, "expr");
        def.input = Json.string(o, "input");
        def.inputs = Json.strings(o, "inputs");
        def.derive = Json.strings(o, "derive");
        def.cyclical = Json.bool(o, "cyclical", false);
        def.edges = doubles(o, "edges", diagnostics, loc);
        def.values = Json.strings(o, "values");
        def.baseline = Json.string(o, "baseline");
        def.on = Json.string(o, "on");
        def.distribution = Json.string(o, "distribution");
        def.seed = longOf(o, "seed", diagnostics, loc);
        def.funcs = Json.strings(o, "funcs");
        if (o.has("slice") && !o.get("slice").isJsonNull()) {
            if (o.get("slice").isJsonObject()) {
                def.sliceFrom = Json.integer(o.getAsJsonObject("slice"), "from");
                def.sliceTo = Json.integer(o.getAsJsonObject("slice"), "to");
            } else {
                def.sliceMalformed = true;
            }
        }
        def.diff = Json.integer(o, "diff");
        def.normalize = Json.string(o, "normalize");
        def.position = Json.string(o, "position");
        def.degree = Json.integer(o, "degree");

        def.context =Json.string(o, "context");
        def.excludeSelf = Json.bool(o, "excludeSelf", false);
        def.entity = Json.string(o, "entity");
        def.direction = Json.string(o, "direction");
        if (def.direction != null && def.scope != Scope.sequence) {
            diagnostics.error("features.direction", loc, "direction is a sequence parameter (scope " + def.scope + ")");
        } else if (def.direction != null && !DIRECTIONS.contains(def.direction)) {
            diagnostics.error("sequence.direction", loc, "direction must be past | future: " + def.direction);
        }
        def.windows = parseWindows(o, diagnostics, loc);
        if (o.has("ops")) {
            for (final JsonElement e : arrayOf(o.get("ops"))) {
                final Op op = parseOp(e, diagnostics, loc);
                if (op != null) def.ops.add(op);
            }
        }
        if (o.has("lift") && o.get("lift").isJsonObject()) {
            final JsonObject lift = o.getAsJsonObject("lift");
            def.lift = new Lift();
            def.lift.fields = Json.strings(lift, "fields");
            def.lift.exprs = parseLiftExprs(lift, diagnostics, loc);
            def.lift.timeAugment = Json.bool(lift, "timeAugment", false);
            // a misspelled key (field, timeaugment ...) would otherwise drop a channel silently
            final List<String> unknown = lift.keySet().stream().filter(k -> !List.of("fields", "exprs", "timeAugment").contains(k)).toList();
            if (!unknown.isEmpty()) diagnostics.error("sequence.lift", loc, "unknown lift key(s) " + unknown + " (accepted: fields, exprs, timeAugment)");
        } else if (o.has("lift") && !o.get("lift").isJsonNull()) {
            diagnostics.error("sequence.lift", loc, "lift must be an object with fields / exprs / timeAugment");
        }
        def.summarize = o.has("summarize") && !o.get("summarize").isJsonNull();
        if (def.summarize && o.get("summarize").isJsonObject()
                && o.getAsJsonObject("summarize").has("dynamics") && o.getAsJsonObject("summarize").get("dynamics").isJsonObject()) {
            final JsonObject d = o.getAsJsonObject("summarize").getAsJsonObject("dynamics");
            def.dynamics = new DynamicsSpec();
            def.dynamics.family = Json.string(d, "family");
            def.dynamics.measure = Json.string(d, "measure");
            def.dynamics.order = Json.integer(d, "order");
            def.dynamics.halflife = doubles(d, "halflife", diagnostics, loc);
            def.dynamics.period = doubleOf(d, "period", diagnostics, loc);
            def.dynamics.decayBy = Json.string(d, "decayBy");
            def.dynamics.type = Json.string(d, "type");
            def.dynamics.depth = Json.integer(d, "depth");
            for (final String key : d.keySet()) {
                if (!List.of("family", "measure", "order", "halflife", "period", "decayBy", "type", "depth").contains(key)) def.dynamics.unknown.add(key);
            }
        }
        def.compress = o.has("compress") && !o.get("compress").isJsonNull();
        def.compressJson = def.compress && o.get("compress").isJsonObject() ? o.get("compress").toString() : null;

        for (final JsonObject ks : objects(o, "keySets")) {
            final KeySet keySet = new KeySet();
            keySet.keys = Json.strings(ks, "keys");
            keySet.windows = parseWindows(ks, diagnostics, loc + ".keySets");
            keySet.structure = Json.string(ks, "structure");
            keySet.hierarchyJson = ks.has("hierarchy") ? ks.get("hierarchy").toString() : null;
            keySet.shrinkageJson = ks.has("shrinkage") && ks.get("shrinkage").isJsonObject() ? ks.get("shrinkage").toString() : null;
            keySet.parentRef = Json.string(ks, "parentRef");
            keySet.maxDepth = Json.integer(ks, "maxDepth");
            keySet.as = nameSegment(ks, "encoding.keySet.as", "keySet", diagnostics, loc + ".keySets");
            def.keySets.add(keySet);
        }
        checkWindowNames(def, diagnostics, loc);
        for (final JsonObject t : objects(o, "targets")) {
            final Target target = new Target();
            if (t.has("field") && t.get("field").isJsonObject()) {
                final JsonObject f = t.getAsJsonObject("field");
                target.ref = true;
            } else {
                target.field = Json.string(t, "field");
            }
            target.expr = Json.string(t, "expr");
            target.stats = Json.strings(t, "stats");
            target.as = Json.string(t, "as");
            target.values = Json.strings(t, "values");
            def.targets.add(target);
        }
        final String combine = Json.string(o, "combine");
        if (combine != null) {
            try {
                def.combine = Combine.valueOf(combine);
            } catch (final IllegalArgumentException e) {
                diagnostics.error("features.combine", loc, "combine must be product | zip: " + combine);
            }
        }
        def.naming = Json.string(o, "naming");
        def.maxFeatures = Json.integer(o, "maxFeatures");
        def.offset = Json.string(o, "offset");
        def.emitConfidence = Json.bool(o, "emitConfidence", false);
        def.shrinkageJson = o.has("shrinkage") && o.get("shrinkage").isJsonObject() ? o.get("shrinkage").toString() : null;
        def.smoothingJson = o.has("smoothing") && o.get("smoothing").isJsonObject() ? o.get("smoothing").toString() : null;
        def.variant = Json.string(o, "variant");
        def.fields = Json.strings(o, "fields");
        def.latentDim = Json.integer(o, "latentDim");
        def.method = Json.string(o, "method");
        def.bins = Json.integer(o, "bins");
        def.clip = doubleOf(o, "clip", diagnostics, loc);
        def.minSamplesPerBin = Json.integer(o, "minSamplesPerBin");
        def.target = Json.string(o, "target");
        def.rank = Json.integer(o, "rank");
        def.center = o.has("center") && !o.get("center").isJsonNull() ? Json.bool(o, "center", true) : null;
        def.standardize = Json.bool(o, "standardize", false);
        def.outputs = Json.strings(o, "outputs");
        def.segments = Json.integer(o, "segments");
        def.range = doubles(o, "range", diagnostics, loc);
        if (o.has("penalty") && !o.get("penalty").isJsonNull()) {
            if (o.get("penalty").isJsonObject()) {
                final JsonObject penalty = o.getAsJsonObject("penalty");
                def.penaltyOrder = Json.integer(penalty, "order");
                def.penaltyLambda = Json.string(penalty, "lambda");
                for (final String key : penalty.keySet()) if (!List.of("order", "lambda").contains(key)) def.penaltyUnknown.add(key);
            } else {
                // a bare `penalty: 2` would otherwise be dropped and the defaults used silently
                diagnostics.error("smooth.penalty", loc, "penalty must be an object {order, lambda}: " + o.get("penalty"));
            }
        }
        // a block declared in another shape than an object would otherwise be dropped without a word (the
        // defaults would then decide the state, the window or the shrinkage): it is reported like an unknown key
        if (o.has("sequenceOf") && !o.get("sequenceOf").isJsonNull()) {
            if (o.get("sequenceOf").isJsonObject()) {
                final JsonObject sequenceOf = o.getAsJsonObject("sequenceOf");
                def.sequenceEntity = Json.string(sequenceOf, "entity");
                def.sequenceField = Json.string(sequenceOf, "field");
                for (final String key : sequenceOf.keySet()) if (!List.of("entity", "field").contains(key)) def.sequenceUnknown.add("sequenceOf." + key);
            } else {
                def.sequenceUnknown.add("sequenceOf " + o.get("sequenceOf"));
            }
        }
        if (o.has("cooccur") && !o.get("cooccur").isJsonNull()) {
            if (o.get("cooccur").isJsonObject()) {
                final JsonObject cooccur = o.getAsJsonObject("cooccur");
                def.cooccurWindow = Json.integer(cooccur, "window");
                def.cooccurWeighting = Json.string(cooccur, "weighting");
                for (final String key : cooccur.keySet()) if (!List.of("window", "weighting").contains(key)) def.sequenceUnknown.add("cooccur." + key);
            } else {
                def.sequenceUnknown.add("cooccur " + o.get("cooccur"));
            }
        }
        def.embedOf = Json.string(o, "of");
        def.maxValues = Json.integer(o, "maxValues");
        def.order = Json.integer(o, "order");
        if (o.has("emit") && !o.get("emit").isJsonNull()) {
            // emit: [distribution, {toValueProb: good}, ...] — a bare map is one entry
            for (final JsonElement e : arrayOf(o.get("emit"))) {
                if (e.isJsonPrimitive() && "distribution".equals(e.getAsString())) {
                    def.emitDistribution = true;
                } else if (e.isJsonObject() && e.getAsJsonObject().size() == 1 && e.getAsJsonObject().has("toValueProb")) {
                    for (final JsonElement value : arrayOf(e.getAsJsonObject().get("toValueProb"))) {
                        if (value.isJsonPrimitive()) def.emitValues.add(value.getAsString());
                        else def.sequenceUnknown.add("emit " + value);
                    }
                } else {
                    def.sequenceUnknown.add("emit " + e);
                }
            }
        }
        if (o.has("blend") && !o.get("blend").isJsonNull()) {
            if (o.get("blend").isJsonObject()) {
                final JsonObject blend = o.getAsJsonObject("blend");
                def.blendPerEntity = Json.bool(blend, "perEntity", true);
                def.blendPriorWeight = doubleOf(blend, "priorWeight", diagnostics, loc);
                for (final String key : blend.keySet()) if (!List.of("perEntity", "priorWeight").contains(key)) def.sequenceUnknown.add("blend." + key);
            } else {
                def.sequenceUnknown.add("blend " + o.get("blend"));
            }
        }
        if (o.has("task") && o.get("task").isJsonObject()) {
            final JsonObject task = o.getAsJsonObject("task");
            def.taskTarget = Json.string(task, "target") != null ? Json.string(task, "target") : Json.string(task, "field");
            def.taskTargetExpr = Json.string(task, "expr");
            def.taskOffset = Json.string(task, "offset");
        }
        for (final JsonObject out : objects(o, "outputs")) {
            final FmOutput fm = new FmOutput();
            fm.as = Json.string(out, "as");
            if (out.has("pair")) {
                fm.kind = "pair";
                fm.pair = Json.strings(out, "pair");
            } else if (out.has("embedding")) {
                fm.kind = "embedding";
                fm.embedding = Json.string(out, "embedding");
                fm.dims = Json.integer(out, "dims");
            } else if (Json.bool(out, "sum", false)) {
                fm.kind = "sum";
            } else {
                diagnostics.error("factorization.outputs", loc, "each output requires pair: [a, b], embedding: <field> or sum: true");
                continue;
            }
            def.fmOutputs.add(fm);
        }
        if (o.has("als") && o.get("als").isJsonObject()) {
            final JsonObject als = o.getAsJsonObject("als");
            try {
                def.epochs = Json.integer(als, "epochs");
                def.reg = Json.string(als, "reg") == null ? null : Double.parseDouble(Json.string(als, "reg"));
                def.seed = Json.string(als, "seed") == null ? null : Long.parseLong(Json.string(als, "seed"));
            } catch (final RuntimeException e) {
                diagnostics.error("factorization.als", loc, "als.epochs / reg / seed must be numeric: " + als);
            }
        }
        def.fitJson = o.has("fit") && o.get("fit").isJsonObject() ? o.get("fit").toString() : null;
        return def;
    }

    /**
     * An {@code as} that names a segment of generated column names: letters, digits and {@code _}, starting with a
     * letter (a leading {@code _} marks an intermediate column), so the column stays a legal field name in every sink
     * — the rule a clock's name follows for the same reason ({@link Clock#NAME}).
     */
    private static String nameSegment(final JsonObject o, final String code, final String what, final Diagnostics diagnostics, final String loc) {
        final String as = Json.string(o, "as");
        if (as == null) return null;
        if (!Clock.NAME.matcher(as).matches()) {
            diagnostics.error(code, loc, what + " as must be a name of letters, digits and '_' starting with a letter (it becomes a segment of the column names): " + as);
            return null;
        }
        return as;
    }

    /**
     * A window name is the block's: every window of the block (its own and its keySets') writes it into the same
     * {@code {window}} segment, and the hidden level statistics of an encoding are shared by that name. Two windows
     * of one block that select different rows must therefore not carry the same name — unnamed they could not
     * (the token is derived from the bounds), and sharing one would silently make one window read the other's
     * statistics wherever the emitted names happen not to collide.
     */
    private static void checkWindowNames(final FeatureDef def, final Diagnostics diagnostics, final String loc) {
        final Map<String, Window> named = new LinkedHashMap<>();
        final List<Window> windows = new ArrayList<>(def.windows);
        for (final KeySet ks : def.keySets) windows.addAll(ks.windows);
        for (final Window w : windows) {
            if (w.as == null) continue;
            final Window previous = named.putIfAbsent(w.as, w);
            if (previous != null && !previous.sameBounds(w)) {
                diagnostics.error("window.as", loc, "two windows of this block are named '" + w.as + "' but select different rows"
                        + " (maxAge / maxEvents / clock / filter): one name is one window — the statistics behind it are shared, so name them apart");
            }
        }
    }

    private static List<Window> parseWindows(final JsonObject o, final Diagnostics diagnostics, final String loc) {
        final List<Window> windows = new ArrayList<>();
        final List<JsonElement> elements = new ArrayList<>();
        if (o.has("windows")) {
            elements.addAll(arrayOf(o.get("windows")));
            if (o.has("window")) {
                diagnostics.error("window.both", loc, "use either 'window' or 'windows', not both");
            }
        } else if (o.has("window")) {
            elements.add(o.get("window"));
        }
        for (final JsonElement e : elements) {
            if (!e.isJsonObject()) {
                diagnostics.error("window.invalid", loc, "window must be an object with maxEvents / maxAge / filter");
                continue;
            }
            final JsonObject w = e.getAsJsonObject();
            final Window window = new Window();
            window.maxEvents = Json.integer(w, "maxEvents");
            window.clock = Json.string(w, "clock");
            if ("events".equals(window.clock)) {
                diagnostics.error("window.clock", loc, "a window counts events with maxEvents (clock: events has no maxAge)");
            } else if (window.clock != null && !"time".equals(window.clock)) {
                // a calendar clock: maxAge counts its ticks (the clock itself is resolved against the sources' clocks by the compiler)
                final JsonElement ticks = w.get("maxAge");
                if (ticks == null || !ticks.isJsonPrimitive() || !ticks.getAsJsonPrimitive().isNumber() || ticks.getAsDouble() != Math.floor(ticks.getAsDouble()) || ticks.getAsLong() < 0) {
                    diagnostics.error("window.clock", loc, "on the calendar clock '" + window.clock + "' maxAge is a whole number of ticks: " + ticks);
                } else {
                    window.maxAgeTicks = ticks.getAsLong();
                }
            } else {
                window.maxAge = Json.duration(w, "maxAge", null, diagnostics, loc);
            }
            window.filter = Json.string(w, "filter");
            window.as = nameSegment(w, "window.as", "window", diagnostics, loc);
            for (final String key : w.keySet()) {
                if (!List.of("maxEvents", "maxAge", "filter", "clock", "as").contains(key)) {
                    diagnostics.error("window.nearEdge", loc,
                            "window." + key + " is not allowed: the near edge is derived from sources.ingestionLag (§4.3)");
                }
            }
            windows.add(window);
        }
        return windows;
    }

    /** {@code lift.exprs}: strings, or {@code {expr, as}} objects naming their channel. */
    private static List<LiftExpr> parseLiftExprs(final JsonObject lift, final Diagnostics diagnostics, final String loc) {
        final List<LiftExpr> exprs = new ArrayList<>();
        if (!lift.has("exprs") || lift.get("exprs").isJsonNull()) return exprs;
        for (final JsonElement e : arrayOf(lift.get("exprs"))) {
            if (e.isJsonPrimitive()) {
                exprs.add(new LiftExpr(e.getAsString(), null));
                continue;
            }
            final JsonObject o = e.isJsonObject() ? e.getAsJsonObject() : null;
            final String expr = o == null ? null : Json.string(o, "expr");
            if (expr == null) {
                diagnostics.error("sequence.lift", loc, "each lift.exprs entry must be an expression string or {expr: \"...\", as: name}");
                continue;
            }
            final List<String> unknown = o.keySet().stream().filter(k -> !List.of("expr", "as").contains(k)).toList();
            if (!unknown.isEmpty()) diagnostics.error("sequence.lift", loc, "unknown lift.exprs key(s) " + unknown + " (accepted: expr, as)");
            exprs.add(new LiftExpr(expr, Json.string(o, "as")));
        }
        return exprs;
    }

    private static Op parseOp(final JsonElement e, final Diagnostics diagnostics, final String loc) {
        final Op op = new Op();
        if (e.isJsonPrimitive()) {
            op.type = e.getAsString();
            return op;
        }
        if (!e.isJsonObject()) {
            diagnostics.error("ops.invalid", loc, "each op must be a string or an object");
            return null;
        }
        final JsonObject o = e.getAsJsonObject();
        op.type = Json.string(o, "type");
        if (op.type == null) {
            diagnostics.error("ops.type", loc, "op.type is required");
            return null;
        }
        op.fields = Json.strings(o, "fields");
        final String field = Json.string(o, "field");
        if (field != null) op.fields.add(field);
        op.expr = Json.string(o, "expr");
        op.predicate = Json.string(o, "predicate");
        op.k = Json.integer(o, "k");
        op.against = Json.string(o, "against");
        op.lag = Json.integer(o, "lag");
        op.d = doubleOf(o, "d", diagnostics, loc);
        op.up = doubleOf(o, "up", diagnostics, loc);
        op.down = doubleOf(o, "down", diagnostics, loc);
        op.halflife = doubles(o, "halflife", diagnostics, loc);
        op.funcs = Json.strings(o, "funcs");
        op.value = Json.string(o, "value");
        op.unit = Json.strings(o, "unit");
        op.decayBy = Json.string(o, "decayBy");
        op.weightBy = Json.string(o, "weightBy");
        op.method = Json.string(o, "method");
        op.context = Json.string(o, "context");
        op.order = Json.string(o, "order");
        op.mu = doubleOf(o, "mu", diagnostics, loc);
        op.sigma = doubleOf(o, "sigma", diagnostics, loc);
        op.beta = doubleOf(o, "beta", diagnostics, loc);
        op.tau = doubleOf(o, "tau", diagnostics, loc);
        op.kFactor = doubleOf(o, "kFactor", diagnostics, loc);
        op.scale = doubleOf(o, "scale", diagnostics, loc);
        op.tauPer = Json.duration(o, "tauPer", null, diagnostics, loc);
        op.pairs = Json.string(o, "pairs");
        if (o.has("with") && !o.get("with").isJsonNull()) {
            for (final JsonElement m : arrayOf(o.get("with"))) {
                final TeamMember member = new TeamMember();
                if (m.isJsonPrimitive()) {
                    member.entity = m.getAsString();   // `with: [agent]`: the op's prior and drift
                } else if (m.isJsonObject()) {
                    final JsonObject mo = m.getAsJsonObject();
                    member.entity = Json.string(mo, "entity");
                    member.mu = doubleOf(mo, "mu", diagnostics, loc);
                    member.sigma = doubleOf(mo, "sigma", diagnostics, loc);
                    member.tau = doubleOf(mo, "tau", diagnostics, loc);
                    for (final String key : mo.keySet()) if (!TEAM_MEMBER_KEYS.contains(key)) member.unknown.add(key);
                } else {
                    op.withInvalid = "with must list entity names or {entity, mu, sigma, tau} members: " + m;
                    continue;
                }
                op.with.add(member);
            }
        }
        if (o.has("team") && !o.get("team").isJsonNull()) {
            // not Json.strings: a team that is not a list of readout names must be reported, not silently dropped
            for (final JsonElement f : arrayOf(o.get("team"))) {
                if (f.isJsonPrimitive()) op.team.add(f.getAsString());
                else op.withInvalid = "team must list the readouts of the whole team (mu / sigma): " + f;
            }
        }
        op.as = Json.string(o, "as");
        op.values = Json.strings(o, "values");
        op.offset = Json.string(o, "offset");
        op.offsetScale = Json.string(o, "offsetScale");
        op.scoreNull = Json.string(o, "scoreNull");
        op.seed = longOf(o, "seed", diagnostics, loc);
        if (o.has("against") && o.get("against").isJsonArray()) {
            // a list of explanatory fields (context residualize); the single series of a sequence regression stays a string
            op.regressors = Json.strings(o, "against");
            op.against = null;
        } else if (op.against != null) {
            op.regressors.add(op.against);
        }
        for (final JsonElement k : arrayOf(o.get("top"))) {
            if (k.isJsonPrimitive() && k.getAsJsonPrimitive().isNumber() && k.getAsDouble() == Math.rint(k.getAsDouble())) op.top.add(k.getAsInt());
            else diagnostics.error("context.harville.top", loc, "top must list integer places: " + k);
        }
        op.discount = doubles(o, "discount", diagnostics, loc);
        op.maxGroupSize = Json.integer(o, "maxGroupSize");
        if (o.has("temperature") && !o.get("temperature").isJsonNull()) {
            final JsonElement t = o.get("temperature");
            if (t.isJsonPrimitive() && t.getAsJsonPrimitive().isNumber()) {
                op.temperature = t.getAsDouble();
            } else {
                diagnostics.error("context.softmax.temperature", loc, "temperature must be a number: " + t);
            }
        }
        if (o.has("temperatureFrom") && !o.get("temperatureFrom").isJsonNull()) {
            final JsonElement t = o.get("temperatureFrom");
            if (t.isJsonObject() && t.getAsJsonObject().has("value")) {
                // resolved by FeaturePlanService.resolve: {source, hash, value}
                final JsonObject resolved = t.getAsJsonObject();
                op.temperature = resolved.get("value").getAsDouble();
                op.temperatureSource = Json.string(resolved, "source");
                op.temperatureHash = Json.string(resolved, "hash");
            } else {
                diagnostics.error("context.softmax.temperatureFrom.unresolved", loc,
                        "temperatureFrom must be a URI resolved before compile (got " + t + ")");
            }
        }
        return op;
    }

    /** A numeric parameter as a Double: null when absent or not a number (reported as {@code <key>.invalid}, like {@link #longOf}). */
    private static Double doubleOf(final JsonObject o, final String key, final Diagnostics diagnostics, final String loc) {
        if (!o.has(key) || o.get(key).isJsonNull()) return null;
        final JsonElement e = o.get(key);
        if (e.isJsonPrimitive() && e.getAsJsonPrimitive().isNumber()) return e.getAsDouble();
        if (e.isJsonPrimitive()) {
            try {
                return Double.parseDouble(e.getAsString().trim());
            } catch (final NumberFormatException ignored) {
                // reported below
            }
        }
        diagnostics.error(key + ".invalid", loc, key + " must be a number: " + e);
        return null;
    }

    private static Long longOf(final JsonObject o, final String key, final Diagnostics diagnostics, final String loc) {
        if (!o.has(key) || o.get(key).isJsonNull()) return null;
        final JsonElement e = o.get(key);
        if (e.isJsonPrimitive() && e.getAsJsonPrimitive().isNumber()) return e.getAsLong();
        if (e.isJsonPrimitive()) {
            try {
                return Long.parseLong(e.getAsString().trim());
            } catch (final NumberFormatException ignored) {
                // reported below
            }
        }
        diagnostics.error(key + ".invalid", loc, key + " must be an integer: " + e);
        return null;
    }

    /** A list of numeric parameters; a value that is not a number is reported as {@code <key>.invalid}, like {@link #doubleOf}. */
    private static List<Double> doubles(final JsonObject o, final String key, final Diagnostics diagnostics, final String loc) {
        final List<Double> list = new ArrayList<>();
        if (!o.has(key)) return list;
        for (final JsonElement e : arrayOf(o.get(key))) {
            if (e.isJsonPrimitive() && e.getAsJsonPrimitive().isNumber()) {
                list.add(e.getAsDouble());
                continue;
            }
            if (e.isJsonPrimitive()) {
                try {
                    list.add(Double.parseDouble(e.getAsString().trim()));
                    continue;
                } catch (final NumberFormatException ignored) {
                    // reported below
                }
            }
            diagnostics.error(key + ".invalid", loc, key + " must list numbers: " + e);
        }
        return list;
    }

    private static List<JsonElement> arrayOf(final JsonElement e) {
        final List<JsonElement> list = new ArrayList<>();
        if (e == null || e.isJsonNull()) return list;
        if (e.isJsonArray()) {
            for (final JsonElement v : e.getAsJsonArray()) list.add(v);
        } else {
            list.add(e);
        }
        return list;
    }

    private static List<JsonObject> objects(final JsonObject parent, final String key) {
        final List<JsonObject> list = new ArrayList<>();
        if (parent == null || !parent.has(key) || !parent.get(key).isJsonArray()) return list;
        final JsonArray array = parent.getAsJsonArray(key);
        for (final JsonElement e : array) {
            if (e.isJsonObject()) list.add(e.getAsJsonObject());
        }
        return list;
    }

}
