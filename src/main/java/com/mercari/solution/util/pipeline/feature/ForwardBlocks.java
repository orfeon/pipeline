package com.mercari.solution.util.pipeline.feature;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

/**
 * The time blocks of {@code fit.mode: forward} (docs/design/feature-dsl.md §5.6): either fixed-size blocks
 * counted from the epoch ({@code size: P90D}) or calendar buckets ({@code bucket: year | quarter | month | week |
 * day}, UTC). A row at event time {@code t} reads the statistics of every block that is <em>complete and
 * known</em> at {@code predictAt(t)}: block {@code b} qualifies when its end is at or before
 * {@code predictAt(t) − lag}, {@code lag} being the target's availability delay after its own event (settlement +
 * ingestion). {@link #usableBlock} is the last such block; the row's own block is never included.
 */
public final class ForwardBlocks implements Serializable {

    public static final List<String> BUCKETS = List.of("year", "quarter", "month", "week", "day");
    public static final Duration DEFAULT_SIZE = Duration.ofDays(90);

    private final String bucket;
    private final long sizeMillis;
    /** Blocks of {@link #ticks} ticks of a calendar clock (null: wall-time blocks). */
    private final Clock clock;
    private final int ticks;

    private ForwardBlocks(final String bucket, final long sizeMillis) {
        this(bucket, sizeMillis, null, 0);
    }

    private ForwardBlocks(final String bucket, final long sizeMillis, final Clock clock, final int ticks) {
        this.bucket = bucket;
        this.sizeMillis = sizeMillis;
        this.clock = clock;
        this.ticks = ticks;
    }

    /** Blocks of {@code ticks} consecutive ticks of a calendar clock (block {@code k} = ticks {@code [k·ticks, (k+1)·ticks)}). */
    public static ForwardBlocks ofClock(final Clock clock, final int ticks) {
        if (clock == null || ticks < 1) throw new IllegalArgumentException("calendar blocks need a clock and a size >= 1 tick");
        return new ForwardBlocks(null, 0L, clock, ticks);
    }

    public static ForwardBlocks ofSize(final Duration size) {
        if (size == null || size.isZero() || size.isNegative()) throw new IllegalArgumentException("blocks.size must be a positive duration");
        return new ForwardBlocks(null, size.toMillis());
    }

    public static ForwardBlocks ofBucket(final String bucket) {
        if (!BUCKETS.contains(bucket)) throw new IllegalArgumentException("blocks.bucket must be one of " + BUCKETS + ": " + bucket);
        return new ForwardBlocks(bucket, 0L);
    }

    /** Rebuilds the blocks from the column coordinates ({@code blockBucket} or {@code blockSizeMillis}). */
    public static ForwardBlocks fromCoordinates(final String bucket, final String sizeMillis) {
        return bucket != null ? ofBucket(bucket) : new ForwardBlocks(null, Long.parseLong(sizeMillis));
    }

    /**
     * Rebuilds the blocks from the column coordinates: {@code blockClock} + {@code blockTicks} (the calendar comes
     * with the column, {@link OutputColumn#getClocks}), else {@code blockBucket} or {@code blockSizeMillis}.
     */
    public static ForwardBlocks fromCoordinates(final java.util.Map<String, String> coordinates, final java.util.Map<String, Clock> clocks) {
        final String clock = coordinates.get("blockClock");
        if (clock != null) {
            final Clock calendar = clocks.get(clock);
            if (calendar == null) throw new IllegalStateException("the calendar clock '" + clock + "' of the blocks is not attached to the column");
            return ofClock(calendar, Integer.parseInt(coordinates.get("blockTicks")));
        }
        return fromCoordinates(coordinates.get("blockBucket"), coordinates.get("blockSizeMillis"));
    }

    public String bucket() { return bucket; }
    public long sizeMillis() { return sizeMillis; }
    public Clock clock() { return clock; }
    public int ticks() { return ticks; }

    /** The block containing an instant. */
    public long indexOf(final long millis) {
        // a date before the calendar's first tick has ordinal −1: block −1
        if (clock != null) return Math.floorDiv(clock.ordinal(millis), ticks);
        if (bucket == null) return Math.floorDiv(millis, sizeMillis);
        final LocalDate date = Instant.ofEpochMilli(millis).atZone(ZoneOffset.UTC).toLocalDate();
        return switch (bucket) {
            case "year" -> date.getYear();
            case "quarter" -> date.getYear() * 4L + (date.getMonthValue() - 1) / 3;
            case "month" -> date.getYear() * 12L + (date.getMonthValue() - 1);
            case "week" -> Math.floorDiv(millis, 7 * 86_400_000L);
            default -> Math.floorDiv(millis, 86_400_000L);
        };
    }

    /**
     * The last block whose statistics a row may read: every block that ends at or before
     * {@code eventMillis + predictOffset − lag} (the latest event time whose target is known at predictAt).
     */
    public long usableBlock(final long eventMillis, final long predictOffsetMillis, final long lagMillis) {
        return indexOf(eventMillis + predictOffsetMillis - lagMillis) - 1;
    }

    /** Nominal block length, for rounding a {@code maxAge} window to whole blocks. */
    public Duration nominalLength() {
        if (clock != null) return Duration.ofMillis(clock.meanSpacingMillis() * ticks);
        if (bucket == null) return Duration.ofMillis(sizeMillis);
        return switch (bucket) {
            case "year" -> Duration.ofDays(365);
            case "quarter" -> Duration.ofDays(91);
            case "month" -> Duration.ofDays(30);
            case "week" -> Duration.ofDays(7);
            default -> Duration.ofDays(1);
        };
    }

    /** Whole blocks covering a window: {@code ceil(maxAge / nominal length)}, at least 1. */
    public int windowBlocks(final Duration maxAge) {
        final long nominal = nominalLength().toMillis();
        return (int) Math.max(1, (maxAge.toMillis() + nominal - 1) / nominal);
    }

    /** Shortest length a block can have (a calendar bucket varies: a 28-day February, a 90-day quarter). */
    public Duration shortestLength() {
        if (clock != null) return Duration.ofMillis(clock.minSpacingMillis() * ticks);
        if (bucket == null) return Duration.ofMillis(sizeMillis);
        return switch (bucket) {
            case "year" -> Duration.ofDays(365);
            case "quarter" -> Duration.ofDays(90);
            case "month" -> Duration.ofDays(28);
            case "week" -> Duration.ofDays(7);
            default -> Duration.ofDays(1);
        };
    }

    /**
     * Whole blocks that always span {@code span}: {@code ceil(span / shortest length)}, at least 1 — any that many
     * consecutive blocks are at least {@code span} long, whatever the calendar. The rounding of a range that must
     * never under-cover (a time fold's purge / embargo); {@link #windowBlocks} rounds by the nominal length.
     */
    public int coveringBlocks(final Duration span) {
        final long shortest = shortestLength().toMillis();
        return (int) Math.max(1, (span.toMillis() + shortest - 1) / shortest);
    }

    public String describe() {
        if (clock != null) return "size " + ticks + " ticks of " + clock.name();
        return bucket != null ? "bucket " + bucket : "size " + Durations.shortName(Duration.ofMillis(sizeMillis));
    }

    @Override
    public String toString() {
        return describe();
    }

    /**
     * The cumulative sufficient statistics of one (level, key) over its blocks, in block order: entry {@code i}
     * holds the totals of every block up to and including {@code blocks[i]}. Immutable once built.
     */
    public static final class Series implements Serializable {
        final long[] blocks;
        final double[] n;
        final double[] sum;
        final double[] sumSq;
        /** Cumulative Σ baseline of the offset rows (zeros without an offset). */
        final double[] sumOff;
        /** Cumulative Σ b(1 − b) of the offset rows (zeros without an offset): the logit-scale information. */
        final double[] sumInfo;

        public Series(final long[] blocks, final double[] n, final double[] sum, final double[] sumSq) {
            this(blocks, n, sum, sumSq, new double[blocks.length], new double[blocks.length]);
        }

        public Series(final long[] blocks, final double[] n, final double[] sum, final double[] sumSq, final double[] sumOff, final double[] sumInfo) {
            this.blocks = blocks;
            this.n = n;
            this.sum = sum;
            this.sumSq = sumSq;
            this.sumOff = sumOff;
            this.sumInfo = sumInfo;
        }

        public int size() { return blocks.length; }
        public long blockAt(final int position) { return blocks[position]; }

        /** Position of the last block ≤ {@code block}, or −1 when none. */
        public int floor(final long block) {
            int lo = 0, hi = blocks.length - 1, found = -1;
            while (lo <= hi) {
                final int mid = (lo + hi) >>> 1;
                if (blocks[mid] <= block) { found = mid; lo = mid + 1; } else { hi = mid - 1; }
            }
            return found;
        }

        /** Totals up to {@code position} inclusive, minus the totals up to {@code from} inclusive ({@code from < 0}: nothing subtracted). */
        public VarianceComponents.KeyStats statsBetween(final int from, final int position) {
            if (position < 0) return null;
            final VarianceComponents.KeyStats stats = new VarianceComponents.KeyStats();
            stats.n = n[position] - (from < 0 ? 0 : n[from]);
            stats.sum = sum[position] - (from < 0 ? 0 : sum[from]);
            stats.sumSq = sumSq[position] - (from < 0 ? 0 : sumSq[from]);
            stats.sumOff = sumOff[position] - (from < 0 ? 0 : sumOff[from]);
            stats.sumInfo = sumInfo[position] - (from < 0 ? 0 : sumInfo[from]);
            return stats.n <= 0 ? null : stats;
        }

        /** The totals over every block (what a static artifact holds). */
        public VarianceComponents.KeyStats totals() {
            return statsBetween(-1, blocks.length - 1);
        }
    }

}
