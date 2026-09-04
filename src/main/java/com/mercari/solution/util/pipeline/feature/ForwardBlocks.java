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

    private ForwardBlocks(final String bucket, final long sizeMillis) {
        this.bucket = bucket;
        this.sizeMillis = sizeMillis;
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

    public String bucket() { return bucket; }
    public long sizeMillis() { return sizeMillis; }

    /** The block containing an instant. */
    public long indexOf(final long millis) {
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

    public String describe() {
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

        public Series(final long[] blocks, final double[] n, final double[] sum, final double[] sumSq) {
            this.blocks = blocks;
            this.n = n;
            this.sum = sum;
            this.sumSq = sumSq;
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
            return stats.n <= 0 ? null : stats;
        }

        /** The totals over every block (what a static artifact holds). */
        public VarianceComponents.KeyStats totals() {
            return statsBetween(-1, blocks.length - 1);
        }
    }

}
