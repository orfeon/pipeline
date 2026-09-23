package com.mercari.solution.util.pipeline.evaluation;

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

/**
 * One prepared sample of the evaluation transform: its split, group (null for independent rows), a
 * deterministic identity (the sort tie-break, the unit key of independent rows), event time, the bootstrap
 * resampling key (null = the unit's own key), label, baseline (NaN = absent), weight, the slice values (null =
 * missing), the categorical discovery dimensions (null = missing) and the numeric columns in
 * {@link EvaluationSpec#rowColumns} order (NaN = missing).
 */
public final class EvaluationRow implements Serializable {

    /** Row time when no time field is set and the element carries no usable timestamp. */
    public static final long NO_TIME = Long.MIN_VALUE;

    final String split;
    final String group;
    final String identity;
    final long time;
    final String bootKey;
    final double label;
    final double baseline;
    final double weight;
    final String[] slices;
    final String[] dims;
    final double[] x;
    /** the rowId fields' values as text (the rows output; empty unless a rows output is declared) */
    final String[] ids;

    public EvaluationRow(final String split, final String group, final String identity, final long time, final String bootKey,
                         final double label, final double baseline, final double weight, final String[] slices, final double[] x) {
        this(split, group, identity, time, bootKey, label, baseline, weight, slices, new String[0], x);
    }

    public EvaluationRow(final String split, final String group, final String identity, final long time, final String bootKey,
                         final double label, final double baseline, final double weight, final String[] slices, final String[] dims, final double[] x) {
        this(split, group, identity, time, bootKey, label, baseline, weight, slices, dims, x, new String[0]);
    }

    public EvaluationRow(final String split, final String group, final String identity, final long time, final String bootKey,
                         final double label, final double baseline, final double weight, final String[] slices, final String[] dims, final double[] x, final String[] ids) {
        this.ids = ids;
        this.split = split;
        this.group = group;
        this.identity = identity;
        this.time = time;
        this.bootKey = bootKey;
        this.label = label;
        this.baseline = baseline;
        this.weight = weight;
        this.slices = slices;
        this.dims = dims;
        this.x = x;
    }

    public String getIdentity() {
        return identity;
    }

    public long getTime() {
        return time;
    }

    public static final Coder<EvaluationRow> CODER = new RowCoder();

    private static class RowCoder extends AtomicCoder<EvaluationRow> {
        private static final Coder<String> NULLABLE_STRING = NullableCoder.of(StringUtf8Coder.of());
        private static final StringUtf8Coder STRING = StringUtf8Coder.of();
        private static final VarLongCoder LONG = VarLongCoder.of();
        private static final DoubleCoder DOUBLE = DoubleCoder.of();
        private static final VarIntCoder INT = VarIntCoder.of();

        @Override
        public void encode(final EvaluationRow value, final OutputStream out) throws CoderException, IOException {
            STRING.encode(value.split, out);
            NULLABLE_STRING.encode(value.group, out);
            STRING.encode(value.identity, out);
            LONG.encode(value.time, out);
            NULLABLE_STRING.encode(value.bootKey, out);
            DOUBLE.encode(value.label, out);
            DOUBLE.encode(value.baseline, out);
            DOUBLE.encode(value.weight, out);
            INT.encode(value.slices.length, out);
            for (final String s : value.slices) NULLABLE_STRING.encode(s, out);
            INT.encode(value.dims.length, out);
            for (final String s : value.dims) NULLABLE_STRING.encode(s, out);
            INT.encode(value.x.length, out);
            for (final double v : value.x) DOUBLE.encode(v, out);
            INT.encode(value.ids.length, out);
            for (final String s : value.ids) NULLABLE_STRING.encode(s, out);
        }

        @Override
        public EvaluationRow decode(final InputStream in) throws CoderException, IOException {
            final String split = STRING.decode(in);
            final String group = NULLABLE_STRING.decode(in);
            final String identity = STRING.decode(in);
            final long time = LONG.decode(in);
            final String bootKey = NULLABLE_STRING.decode(in);
            final double label = DOUBLE.decode(in);
            final double baseline = DOUBLE.decode(in);
            final double weight = DOUBLE.decode(in);
            final int ns = INT.decode(in);
            final String[] slices = new String[ns];
            for (int i = 0; i < ns; i++) slices[i] = NULLABLE_STRING.decode(in);
            final int nd = INT.decode(in);
            final String[] dims = new String[nd];
            for (int i = 0; i < nd; i++) dims[i] = NULLABLE_STRING.decode(in);
            final int n = INT.decode(in);
            final double[] x = new double[n];
            for (int i = 0; i < n; i++) x[i] = DOUBLE.decode(in);
            final int ni = INT.decode(in);
            final String[] ids = new String[ni];
            for (int i = 0; i < ni; i++) ids[i] = NULLABLE_STRING.decode(in);
            return new EvaluationRow(split, group, identity, time, bootKey, label, baseline, weight, slices, dims, x, ids);
        }
    }
}
