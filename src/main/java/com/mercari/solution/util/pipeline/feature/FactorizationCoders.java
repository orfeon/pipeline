package com.mercari.solution.util.pipeline.feature;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.util.VarInt;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Coders of what a factorization fit moves between workers — the training examples (gathered for one fit, or grouped
 * per fold), the models (per forward change point, joined with the rows that read them) and the fits a side input
 * serves — in place of Java serialization, which writes every class descriptor and boxes every number.
 */
final class FactorizationCoders {

    private FactorizationCoders() {}

    /** {@link Factorization.Example}: the values (null-able), the numeric values, the target, offset, tag and contest. */
    static final class ExampleCoder extends AtomicCoder<Factorization.Example> {
        private static final ExampleCoder INSTANCE = new ExampleCoder();

        static ExampleCoder of() {
            return INSTANCE;
        }

        @Override
        public void encode(final Factorization.Example e, final OutputStream out) throws IOException {
            final DataOutputStream data = new DataOutputStream(out);
            VarInt.encode(e.values().length, out);
            for (final String value : e.values()) writeString(value, out);
            writeDoubles(e.x(), out);
            data.writeDouble(e.y());
            data.writeDouble(e.offset());
            data.writeLong(e.tag());
            writeString(e.contest(), out);
            data.flush();
        }

        @Override
        public Factorization.Example decode(final InputStream in) throws IOException {
            final DataInputStream data = new DataInputStream(in);
            final String[] values = new String[VarInt.decodeInt(in)];
            for (int f = 0; f < values.length; f++) values[f] = readString(in);
            final double[] x = readDoubles(in);
            final double y = data.readDouble();
            final double offset = data.readDouble();
            final long tag = data.readLong();
            return new Factorization.Example(values, x, y, offset, tag, readString(in));
        }
    }

    /** {@link Factorization.Model}: the field layout, then every field's value → parameter vector map. */
    static final class ModelCoder extends AtomicCoder<Factorization.Model> {
        private static final ModelCoder INSTANCE = new ModelCoder();

        static ModelCoder of() {
            return INSTANCE;
        }

        @Override
        public void encode(final Factorization.Model m, final OutputStream out) throws IOException {
            final DataOutputStream data = new DataOutputStream(out);
            final int fields = m.fields.size();
            VarInt.encode(fields, out);
            for (final String field : m.fields) writeString(field, out);
            data.writeBoolean(m.fieldWeighted);
            VarInt.encode(m.k, out);
            data.writeDouble(m.w0);
            writeString(m.loss, out);
            for (int f = 0; f < fields; f++) {
                data.writeBoolean(m.numeric[f]);
                data.writeDouble(m.mean[f]);
                data.writeDouble(m.scale[f]);
                for (int g = 0; g < fields; g++) data.writeDouble(m.r[f][g]);
            }
            for (int f = 0; f < fields; f++) {
                final Map<String, double[]> params = m.params.get(f);
                VarInt.encode(params.size(), out);
                for (final Map.Entry<String, double[]> e : params.entrySet()) {
                    writeString(e.getKey(), out);
                    writeDoubles(e.getValue(), out);
                }
            }
            data.flush();
        }

        @Override
        public Factorization.Model decode(final InputStream in) throws IOException {
            final DataInputStream data = new DataInputStream(in);
            final int fields = VarInt.decodeInt(in);
            final List<String> names = new ArrayList<>(fields);
            for (int f = 0; f < fields; f++) names.add(readString(in));
            final boolean fieldWeighted = data.readBoolean();
            final int k = VarInt.decodeInt(in);
            final double w0 = data.readDouble();
            final String loss = readString(in);
            final boolean[] numeric = new boolean[fields];
            final double[] mean = new double[fields], scale = new double[fields];
            final double[][] r = new double[fields][fields];
            for (int f = 0; f < fields; f++) {
                numeric[f] = data.readBoolean();
                mean[f] = data.readDouble();
                scale[f] = data.readDouble();
                for (int g = 0; g < fields; g++) r[f][g] = data.readDouble();
            }
            final Factorization.Model m = new Factorization.Model(List.copyOf(names), numeric, fieldWeighted, k);
            m.w0 = w0;
            m.loss = loss;
            System.arraycopy(mean, 0, m.mean, 0, fields);
            System.arraycopy(scale, 0, m.scale, 0, fields);
            for (int f = 0; f < fields; f++) System.arraycopy(r[f], 0, m.r[f], 0, fields);
            for (int f = 0; f < fields; f++) {
                final int size = VarInt.decodeInt(in);
                final Map<String, double[]> params = m.params.get(f);
                for (int i = 0; i < size; i++) params.put(readString(in), readDoubles(in));
            }
            return m;
        }
    }

    /** {@link Factorization.Fits}: the whole-input model, the fold models and the forward models, each null-able. */
    static final class FitsCoder extends AtomicCoder<Factorization.Fits> {
        private static final FitsCoder INSTANCE = new FitsCoder();

        static FitsCoder of() {
            return INSTANCE;
        }

        @Override
        public void encode(final Factorization.Fits fits, final OutputStream out) throws IOException {
            final DataOutputStream data = new DataOutputStream(out);
            writeModel(fits.total, out);
            data.writeBoolean(fits.folds != null);
            if (fits.folds != null) {
                VarInt.encode(fits.folds.length, out);
                for (final Factorization.Model fold : fits.folds) writeModel(fold, out);
            }
            data.writeBoolean(fits.blocks != null);
            if (fits.blocks != null) {
                VarInt.encode(fits.blocks.size(), out);
                for (final Map.Entry<Long, Factorization.Model> e : fits.blocks.entrySet()) {
                    data.writeLong(e.getKey());
                    writeModel(e.getValue(), out);
                }
                VarInt.encode(fits.observedBlocks.size(), out);
                for (final long block : fits.observedBlocks) data.writeLong(block);
            }
            data.flush();
        }

        @Override
        public Factorization.Fits decode(final InputStream in) throws IOException {
            final DataInputStream data = new DataInputStream(in);
            final Factorization.Model total = readModel(in);
            Factorization.Model[] folds = null;
            if (data.readBoolean()) {
                folds = new Factorization.Model[VarInt.decodeInt(in)];
                for (int f = 0; f < folds.length; f++) folds[f] = readModel(in);
            }
            TreeMap<Long, Factorization.Model> blocks = null;
            TreeSet<Long> observed = null;
            if (data.readBoolean()) {
                blocks = new TreeMap<>();
                final int size = VarInt.decodeInt(in);
                for (int i = 0; i < size; i++) {
                    final long at = data.readLong();
                    blocks.put(at, readModel(in));
                }
                observed = new TreeSet<>();
                final int observedSize = VarInt.decodeInt(in);
                for (int i = 0; i < observedSize; i++) observed.add(data.readLong());
            }
            return new Factorization.Fits(total, folds, blocks, observed);
        }

        private static void writeModel(final Factorization.Model model, final OutputStream out) throws IOException {
            out.write(model == null ? 0 : 1);
            if (model != null) ModelCoder.INSTANCE.encode(model, out);
        }

        private static Factorization.Model readModel(final InputStream in) throws IOException {
            return in.read() == 1 ? ModelCoder.INSTANCE.decode(in) : null;
        }
    }

    /** A null-able string: its UTF-8 length + 1 (0 = null), then the bytes. */
    private static void writeString(final String value, final OutputStream out) throws IOException {
        if (value == null) {
            VarInt.encode(0, out);
            return;
        }
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        VarInt.encode(bytes.length + 1, out);
        out.write(bytes);
    }

    private static String readString(final InputStream in) throws IOException {
        final int length = VarInt.decodeInt(in);
        if (length == 0) return null;
        final byte[] bytes = new byte[length - 1];
        new DataInputStream(in).readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** A null-able double array: its length + 1 (0 = null), then the values. */
    private static void writeDoubles(final double[] values, final OutputStream out) throws IOException {
        if (values == null) {
            VarInt.encode(0, out);
            return;
        }
        VarInt.encode(values.length + 1, out);
        final DataOutputStream data = new DataOutputStream(out);
        for (final double v : values) data.writeDouble(v);
        data.flush();
    }

    private static double[] readDoubles(final InputStream in) throws IOException {
        final int length = VarInt.decodeInt(in);
        if (length == 0) return null;
        final double[] values = new double[length - 1];
        final DataInputStream data = new DataInputStream(in);
        for (int i = 0; i < values.length; i++) values[i] = data.readDouble();
        return values;
    }
}
