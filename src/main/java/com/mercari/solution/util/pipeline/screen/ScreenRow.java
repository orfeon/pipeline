package com.mercari.solution.util.pipeline.screen;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;

/**
 * One prepared sample: the unit key, a deterministic identity (noise seed and sort tie-break), event time,
 * period bucket (null = no periods), label, baseline (NaN = absent), weight and the numeric columns in the
 * spec's column order (candidates, then the shuffle reference column when configured). NaN marks a missing value.
 */
public final class ScreenRow implements Serializable {

    final String group;
    final String identity;
    /** Row time when no time field is set and the element carries no usable timestamp (kept out of the summary range). */
    public static final long NO_TIME = Long.MIN_VALUE;

    final long time;
    final String period;
    final double label;
    final double baseline;
    final double weight;
    final double[] x;

    public ScreenRow(final String group, final String identity, final long time, final String period,
                     final double label, final double baseline, final double weight, final double[] x) {
        this.group = group;
        this.identity = identity;
        this.time = time;
        this.period = period;
        this.label = label;
        this.baseline = baseline;
        this.weight = weight;
        this.x = x;
    }

    /**
     * The row as the conditioning fit reads it: the conditioning columns alone, at offset 0 (a
     * {@link ConditioningScorer} built with offset 0). The identity stays: it is the sort tie-break of the
     * unit's rows, and the evaluation sums them in that order (a floating-point sum is order-dependent, and
     * the GroupByKey iteration order is not stable across runs). The period is not read by the fit.
     */
    ScreenRow conditioningOnly(final ScreenSpec spec) {
        final int off = spec.conditioningOffset();
        return new ScreenRow(group, identity, time, null, label, baseline, weight,
                Arrays.copyOfRange(x, off, off + spec.conditioningFields.size()));
    }

    public String getGroup() {
        return group;
    }

    public String getIdentity() {
        return identity;
    }

    public long getTime() {
        return time;
    }

    public static final Coder<ScreenRow> CODER = new RowCoder();

    private static class RowCoder extends AtomicCoder<ScreenRow> {
        private static final Coder<String> NULLABLE_STRING = NullableCoder.of(StringUtf8Coder.of());
        private static final StringUtf8Coder STRING = StringUtf8Coder.of();
        private static final VarLongCoder LONG = VarLongCoder.of();
        private static final DoubleCoder DOUBLE = DoubleCoder.of();
        private static final VarIntCoder INT = VarIntCoder.of();

        @Override
        public void encode(final ScreenRow value, final OutputStream out) throws CoderException, IOException {
            NULLABLE_STRING.encode(value.group, out);
            STRING.encode(value.identity, out);
            LONG.encode(value.time, out);
            NULLABLE_STRING.encode(value.period, out);
            DOUBLE.encode(value.label, out);
            DOUBLE.encode(value.baseline, out);
            DOUBLE.encode(value.weight, out);
            INT.encode(value.x.length, out);
            for (final double v : value.x) DOUBLE.encode(v, out);
        }

        @Override
        public ScreenRow decode(final InputStream in) throws CoderException, IOException {
            final String group = NULLABLE_STRING.decode(in);
            final String identity = STRING.decode(in);
            final long time = LONG.decode(in);
            final String period = NULLABLE_STRING.decode(in);
            final double label = DOUBLE.decode(in);
            final double baseline = DOUBLE.decode(in);
            final double weight = DOUBLE.decode(in);
            final int n = INT.decode(in);
            final double[] x = new double[n];
            for (int i = 0; i < n; i++) x[i] = DOUBLE.decode(in);
            return new ScreenRow(group, identity, time, period, label, baseline, weight, x);
        }
    }
}
