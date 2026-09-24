package com.mercari.solution.util.pipeline.evaluation;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * One row of a scored unit as the calibration tables read it: the split, the label y as declared (the
 * realised outcome: positives, rates and utility count it), its normalised share ỹ (the likelihood's label,
 * Σ ỹ = 1 within a grouped unit; equal to y for binomial), the baseline mean p, every prediction set's mean q
 * (baseline form: a share or a probability), the calibration fields' values and the utility (NaN = none).
 */
public final class AlignedRow implements Serializable {

    final String split;
    final double label;
    final double share;
    final double baseline;
    final double[] predictions;
    final double[] fields;
    final double utility;

    public AlignedRow(final String split, final double label, final double share, final double baseline, final double[] predictions, final double[] fields, final double utility) {
        this.split = split;
        this.label = label;
        this.share = share;
        this.baseline = baseline;
        this.predictions = predictions;
        this.fields = fields;
        this.utility = utility;
    }

    public static final Coder<AlignedRow> CODER = new RowCoder();

    private static class RowCoder extends AtomicCoder<AlignedRow> {
        private static final StringUtf8Coder STRING = StringUtf8Coder.of();
        private static final DoubleCoder DOUBLE = DoubleCoder.of();
        private static final VarIntCoder INT = VarIntCoder.of();

        @Override
        public void encode(final AlignedRow value, final OutputStream out) throws CoderException, IOException {
            STRING.encode(value.split, out);
            DOUBLE.encode(value.label, out);
            DOUBLE.encode(value.share, out);
            DOUBLE.encode(value.baseline, out);
            INT.encode(value.predictions.length, out);
            for (final double v : value.predictions) DOUBLE.encode(v, out);
            INT.encode(value.fields.length, out);
            for (final double v : value.fields) DOUBLE.encode(v, out);
            DOUBLE.encode(value.utility, out);
        }

        @Override
        public AlignedRow decode(final InputStream in) throws CoderException, IOException {
            final String split = STRING.decode(in);
            final double label = DOUBLE.decode(in);
            final double share = DOUBLE.decode(in);
            final double baseline = DOUBLE.decode(in);
            final int k = INT.decode(in);
            final double[] predictions = new double[k];
            for (int i = 0; i < k; i++) predictions[i] = DOUBLE.decode(in);
            final int m = INT.decode(in);
            final double[] fields = new double[m];
            for (int i = 0; i < m; i++) fields[i] = DOUBLE.decode(in);
            final double utility = DOUBLE.decode(in);
            return new AlignedRow(split, label, share, baseline, predictions, fields, utility);
        }
    }
}
