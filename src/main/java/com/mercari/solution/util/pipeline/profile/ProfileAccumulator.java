package com.mercari.solution.util.pipeline.profile;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.sampling.VarOptItemsSketch;
import org.apache.datasketches.sampling.VarOptItemsUnion;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

/**
 * Mergeable profile state for one dataset: per-field sketch sets, the numeric co-moment
 * (correlation) matrix and a VarOpt row sample.
 *
 * <p>Sketches are not {@link Serializable}; this class implements custom serialization on top of
 * their compact binary representations, so the whole accumulator can be carried by Beam's
 * {@code SerializableCoder}.
 *
 * <p>Sketch types whose merged form cannot be updated afterwards (CPC, Theta, VarOpt) are held as a
 * pair of {@code live} (update target) and {@code merged} (union result) instances, because Beam may
 * keep adding inputs to an accumulator after {@code mergeAccumulators}.
 */
public class ProfileAccumulator implements Serializable {

    private static final ArrayOfStringsSerDe STRINGS_SERDE = new ArrayOfStringsSerDe();

    private ProfileSpec spec;

    private long rowCount;
    private long errorCount;

    private FieldAccumulator[] fields;

    // correlation co-moments over numeric field pairs (flattened upper triangle)
    private int numericCount;
    private double[] pairN;
    private double[] pairMeanX;
    private double[] pairMeanY;
    private double[] pairM2X;
    private double[] pairM2Y;
    private double[] pairComoment;

    private transient VarOptItemsSketch<String> sampleLive;
    private transient VarOptItemsSketch<String> sampleMerged;

    private ProfileAccumulator() {
    }

    public static ProfileAccumulator of(final ProfileSpec spec) {
        final ProfileAccumulator acc = new ProfileAccumulator();
        acc.spec = spec;
        final List<ProfileSpec.FieldSpec> fieldSpecs = spec.getFields();
        acc.fields = new FieldAccumulator[fieldSpecs.size()];
        for(int i = 0; i < fieldSpecs.size(); i++) {
            acc.fields[i] = FieldAccumulator.of(fieldSpecs.get(i), spec.getSketchParameters());
        }
        acc.numericCount = spec.getNumericFieldIndices().size();
        final int pairs = acc.numericCount * (acc.numericCount - 1) / 2;
        if(spec.isCorrelationEnabled() && pairs > 0) {
            acc.pairN = new double[pairs];
            acc.pairMeanX = new double[pairs];
            acc.pairMeanY = new double[pairs];
            acc.pairM2X = new double[pairs];
            acc.pairM2Y = new double[pairs];
            acc.pairComoment = new double[pairs];
        }
        if(spec.isSampleEnabled()) {
            acc.sampleLive = VarOptItemsSketch.newInstance(spec.getSketchParameters().sampleK);
        }
        return acc;
    }

    public ProfileSpec getSpec() {
        return spec;
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getErrorCount() {
        return errorCount;
    }

    public FieldAccumulator getField(final int index) {
        return fields[index];
    }

    public int getFieldCount() {
        return fields.length;
    }

    public void countError() {
        errorCount += 1;
    }

    /**
     * Adds one row. {@code values[i]} is the raw primitive value for field spec {@code i}
     * (already navigated through nested maps), {@code sampleJson} is the row rendered as a JSON
     * string (null unless sampling is enabled).
     */
    public void add(final Object[] values, final String sampleJson) {
        rowCount += 1;
        final List<ProfileSpec.FieldSpec> fieldSpecs = spec.getFields();
        final Double[] numericValues = pairN != null ? new Double[numericCount] : null;
        int numericIndex = 0;
        for(int i = 0; i < fields.length; i++) {
            final ProfileSpec.FieldSpec fieldSpec = fieldSpecs.get(i);
            final Double numericValue = fields[i].add(fieldSpec, values[i]);
            if(ProfileSpec.ProfileType.NUMERIC.equals(fieldSpec.profileType)) {
                if(numericValues != null) {
                    numericValues[numericIndex] = numericValue;
                }
                numericIndex += 1;
            }
        }
        if(numericValues != null) {
            updatePairs(numericValues);
        }
        if(sampleJson != null && spec.isSampleEnabled()) {
            if(sampleLive == null) {
                sampleLive = VarOptItemsSketch.newInstance(spec.getSketchParameters().sampleK);
            }
            sampleLive.update(sampleJson, 1.0);
        }
    }

    private void updatePairs(final Double[] numericValues) {
        int pair = 0;
        for(int i = 0; i < numericCount; i++) {
            for(int j = i + 1; j < numericCount; j++) {
                final Double x = numericValues[i];
                final Double y = numericValues[j];
                if(x != null && y != null && !x.isNaN() && !y.isNaN() && !x.isInfinite() && !y.isInfinite()) {
                    final double n = pairN[pair] + 1;
                    final double dx = x - pairMeanX[pair];
                    final double dy = y - pairMeanY[pair];
                    pairMeanX[pair] += dx / n;
                    pairMeanY[pair] += dy / n;
                    pairM2X[pair] += dx * (x - pairMeanX[pair]);
                    pairM2Y[pair] += dy * (y - pairMeanY[pair]);
                    pairComoment[pair] += dx * (y - pairMeanY[pair]);
                    pairN[pair] = n;
                }
                pair += 1;
            }
        }
    }

    public ProfileAccumulator merge(final ProfileAccumulator other) {
        rowCount += other.rowCount;
        errorCount += other.errorCount;
        for(int i = 0; i < fields.length; i++) {
            fields[i].merge(other.fields[i], spec.getSketchParameters());
        }
        if(pairN != null && other.pairN != null) {
            for(int p = 0; p < pairN.length; p++) {
                final double na = pairN[p];
                final double nb = other.pairN[p];
                final double n = na + nb;
                if(nb == 0) {
                    continue;
                }
                if(na == 0) {
                    pairN[p] = other.pairN[p];
                    pairMeanX[p] = other.pairMeanX[p];
                    pairMeanY[p] = other.pairMeanY[p];
                    pairM2X[p] = other.pairM2X[p];
                    pairM2Y[p] = other.pairM2Y[p];
                    pairComoment[p] = other.pairComoment[p];
                    continue;
                }
                final double dx = other.pairMeanX[p] - pairMeanX[p];
                final double dy = other.pairMeanY[p] - pairMeanY[p];
                pairM2X[p] += other.pairM2X[p] + dx * dx * na * nb / n;
                pairM2Y[p] += other.pairM2Y[p] + dy * dy * na * nb / n;
                pairComoment[p] += other.pairComoment[p] + dx * dy * na * nb / n;
                pairMeanX[p] += dx * nb / n;
                pairMeanY[p] += dy * nb / n;
                pairN[p] = n;
            }
        }
        final VarOptItemsSketch<String> otherSample = other.sampleResult();
        if(otherSample != null) {
            sampleMerged = unionSamples(sampleMerged, otherSample, spec.getSketchParameters().sampleK);
        }
        return this;
    }

    // ---- results ----

    /** Pearson correlation for the (i, j) numeric pair (indices into getNumericFieldIndices order), or null. */
    public Double correlation(final int i, final int j) {
        if(pairN == null || i == j) {
            return i == j ? 1.0 : null;
        }
        final int a = Math.min(i, j);
        final int b = Math.max(i, j);
        final int pair = a * numericCount - a * (a + 1) / 2 + (b - a - 1);
        if(pairN[pair] < 2) {
            return null;
        }
        final double denominator = Math.sqrt(pairM2X[pair] * pairM2Y[pair]);
        if(denominator == 0d || !Double.isFinite(denominator)) {
            return null;
        }
        return pairComoment[pair] / denominator;
    }

    public Double pairCount(final int i, final int j) {
        if(pairN == null || i == j) {
            return null;
        }
        final int a = Math.min(i, j);
        final int b = Math.max(i, j);
        final int pair = a * numericCount - a * (a + 1) / 2 + (b - a - 1);
        return pairN[pair];
    }

    public VarOptItemsSketch<String> sampleResult() {
        if(sampleLive == null && sampleMerged == null) {
            return null;
        }
        if(sampleMerged == null) {
            return sampleLive;
        }
        if(sampleLive == null || sampleLive.getN() == 0) {
            return sampleMerged;
        }
        return unionSamples(sampleMerged, sampleLive, spec.getSketchParameters().sampleK);
    }

    private static VarOptItemsSketch<String> unionSamples(
            final VarOptItemsSketch<String> a,
            final VarOptItemsSketch<String> b,
            final int k) {

        final VarOptItemsUnion<String> union = VarOptItemsUnion.newInstance(k);
        if(a != null) {
            union.update(a);
        }
        if(b != null) {
            union.update(b);
        }
        return union.getResult();
    }

    /**
     * Deep copy via serialization round trip. Querying sketches mutates their lazily-sorted
     * internal state (e.g. KLL sorts its base buffer), so consumers that must not mutate their
     * input (DoFns under DirectRunner's enforceImmutability) should render from a copy.
     */
    public ProfileAccumulator copy() {
        try {
            final java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
            try(final ObjectOutputStream out = new ObjectOutputStream(bytes)) {
                out.writeObject(this);
            }
            try(final ObjectInputStream in = new ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray()))) {
                return (ProfileAccumulator) in.readObject();
            }
        } catch (final IOException | ClassNotFoundException e) {
            throw new IllegalStateException("failed to copy profile accumulator", e);
        }
    }

    // ---- serialization ----

    private void writeObject(final ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        final VarOptItemsSketch<String> sample = sampleResult();
        writeBytes(out, sample == null ? null : sample.toByteArray(STRINGS_SERDE));
    }

    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        final byte[] sampleBytes = readBytes(in);
        if(sampleBytes != null) {
            // heapified union results are not update targets: keep as merged, updates go to a fresh live
            sampleMerged = VarOptItemsSketch.heapify(Memory.wrap(sampleBytes), STRINGS_SERDE);
        }
    }

    static void writeBytes(final ObjectOutputStream out, final byte[] bytes) throws IOException {
        if(bytes == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    static byte[] readBytes(final ObjectInputStream in) throws IOException {
        final int length = in.readInt();
        if(length < 0) {
            return null;
        }
        final byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    /**
     * Per-field sketch set. Component slots are nullable depending on the profile type.
     */
    public static class FieldAccumulator implements Serializable {

        private static final ArrayOfStringsSerDe SERDE = new ArrayOfStringsSerDe();

        public long count;      // non-null observations
        public long nullCount;
        public long errorCount; // values that failed to coerce
        public long emptyCount; // empty strings
        public long zeroCount;
        public long nanCount;
        public long infCount;
        public long trueCount;
        public long falseCount;

        public double min = Double.POSITIVE_INFINITY;
        public double max = Double.NEGATIVE_INFINITY;
        public double mean;
        public double m2;
        public double m3;

        private int cpcLgK = 12;
        private int thetaLgK = 12;

        private transient KllDoublesSketch kll;         // numeric / timestamp values, string lengths, array lengths
        private transient CpcSketch cpcLive;
        private transient CpcSketch cpcMerged;
        private transient ItemsSketch<String> frequentItems;
        private transient UpdateSketch thetaLive;
        private transient CompactSketch thetaMerged;

        private FieldAccumulator() {
        }

        static FieldAccumulator of(final ProfileSpec.FieldSpec fieldSpec, final ProfileSpec.SketchParameters params) {
            final FieldAccumulator acc = new FieldAccumulator();
            acc.cpcLgK = params.cpcLgK;
            acc.thetaLgK = params.thetaLgK;
            switch (fieldSpec.profileType) {
                case NUMERIC, TIMESTAMP, ARRAY_LENGTH -> acc.kll = KllDoublesSketch.newHeapInstance(params.kllK);
                case STRING -> {
                    acc.kll = KllDoublesSketch.newHeapInstance(params.kllK);
                    acc.frequentItems = new ItemsSketch<>(params.fiMaxMapSize);
                }
                case BOOL -> { }
            }
            switch (fieldSpec.profileType) {
                case NUMERIC, STRING -> acc.cpcLive = new CpcSketch(params.cpcLgK);
                default -> { }
            }
            if(fieldSpec.isKey) {
                acc.thetaLive = UpdateSketch.builder().setNominalEntries(1 << params.thetaLgK).build();
            }
            return acc;
        }

        /** Adds one raw value; returns the numeric interpretation for correlation (NUMERIC only). */
        Double add(final ProfileSpec.FieldSpec fieldSpec, final Object value) {
            if(value == null) {
                nullCount += 1;
                return null;
            }
            if(value == ProfileRow.Marker.ERROR) {
                errorCount += 1;
                return null;
            }
            try {
                switch (fieldSpec.profileType) {
                    case NUMERIC -> {
                        final Double v = ProfileSpec.toDouble(value, fieldSpec.scale);
                        if(v == null) {
                            errorCount += 1;
                            return null;
                        }
                        if(v.isNaN()) {
                            nanCount += 1;
                            return null;
                        }
                        if(v.isInfinite()) {
                            infCount += 1;
                            return null;
                        }
                        count += 1;
                        if(v == 0d) {
                            zeroCount += 1;
                        }
                        updateMoments(v);
                        kll.update(v);
                        cpcLive.update(v);
                        updateTheta(canonicalNumeric(v));
                        return v;
                    }
                    case STRING -> {
                        final String s = ProfileSpec.toStringValue(value, fieldSpec.symbols);
                        if(s == null) {
                            errorCount += 1;
                            return null;
                        }
                        count += 1;
                        if(s.isEmpty()) {
                            emptyCount += 1;
                        }
                        kll.update(s.length());
                        cpcLive.update(s);
                        frequentItems.update(s);
                        updateTheta(s);
                        return null;
                    }
                    case BOOL -> {
                        final Boolean b = ProfileSpec.toBoolean(value);
                        if(b == null) {
                            errorCount += 1;
                            return null;
                        }
                        count += 1;
                        if(b) {
                            trueCount += 1;
                        } else {
                            falseCount += 1;
                        }
                        return null;
                    }
                    case TIMESTAMP -> {
                        final Double ms = ProfileSpec.toEpochMillis(value, fieldSpec.sourceType);
                        if(ms == null) {
                            errorCount += 1;
                            return null;
                        }
                        count += 1;
                        updateMoments(ms);
                        kll.update(ms);
                        updateTheta(canonicalNumeric(ms));
                        return null;
                    }
                    case ARRAY_LENGTH -> {
                        final Integer length = arrayLength(value);
                        if(length == null) {
                            errorCount += 1;
                            return null;
                        }
                        count += 1;
                        updateMoments(length);
                        kll.update(length);
                        return null;
                    }
                }
            } catch (final Throwable e) {
                errorCount += 1;
            }
            return null;
        }

        private static Integer arrayLength(final Object value) {
            return ProfileSpec.arrayLength(value);
        }

        private static String canonicalNumeric(final double v) {
            if(v == Math.rint(v) && Double.isFinite(v) && Math.abs(v) < 1e15) {
                return String.valueOf((long) v);
            }
            return String.valueOf(v);
        }

        private void updateMoments(final double v) {
            if(v < min) {
                min = v;
            }
            if(v > max) {
                max = v;
            }
            final double n1 = momentsCount();
            final double n = n1 + 1;
            final double delta = v - mean;
            final double deltaN = delta / n;
            final double term1 = delta * deltaN * n1;
            m3 += term1 * deltaN * (n - 2) - 3 * deltaN * m2;
            m2 += term1;
            mean += deltaN;
        }

        private double momentsCount() {
            // moments are updated exactly once per counted (finite) observation, before count increments
            return count - 1 < 0 ? 0 : count - 1;
        }

        private void updateTheta(final String canonical) {
            if(thetaLive != null) {
                thetaLive.update(canonical);
            }
        }

        void merge(final FieldAccumulator other, final ProfileSpec.SketchParameters params) {
            final double na = count;
            final double nb = other.count;
            if(nb > 0) {
                final double n = na + nb;
                final double delta = other.mean - mean;
                if(na == 0) {
                    mean = other.mean;
                    m2 = other.m2;
                    m3 = other.m3;
                } else {
                    m3 = m3 + other.m3
                            + delta * delta * delta * na * nb * (na - nb) / (n * n)
                            + 3 * delta * (na * other.m2 - nb * m2) / n;
                    m2 = m2 + other.m2 + delta * delta * na * nb / n;
                    mean = mean + delta * nb / n;
                }
            }
            count += other.count;
            nullCount += other.nullCount;
            errorCount += other.errorCount;
            emptyCount += other.emptyCount;
            zeroCount += other.zeroCount;
            nanCount += other.nanCount;
            infCount += other.infCount;
            trueCount += other.trueCount;
            falseCount += other.falseCount;
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);

            if(kll != null && other.kll != null) {
                kll.merge(other.kll);
            } else if(kll == null) {
                kll = other.kll;
            }
            if(frequentItems != null && other.frequentItems != null) {
                frequentItems.merge(other.frequentItems);
            } else if(frequentItems == null) {
                frequentItems = other.frequentItems;
            }

            final CpcSketch otherCpc = cpcResultOf(other.cpcLive, other.cpcMerged, params.cpcLgK);
            if(otherCpc != null) {
                cpcMerged = cpcResultOf(cpcResultOf(cpcLive, cpcMerged, params.cpcLgK), otherCpc, params.cpcLgK);
                cpcLive = new CpcSketch(params.cpcLgK);
            }

            final CompactSketch otherTheta = thetaResultOf(other.thetaLive, other.thetaMerged, params.thetaLgK);
            if(otherTheta != null) {
                thetaMerged = thetaResultOf(thetaResultOf(thetaLive, thetaMerged, params.thetaLgK), otherTheta, params.thetaLgK);
                thetaLive = thetaLive != null
                        ? UpdateSketch.builder().setNominalEntries(1 << params.thetaLgK).build()
                        : null;
            }
        }

        public KllDoublesSketch getKll() {
            return kll;
        }

        public ItemsSketch<String> getFrequentItems() {
            return frequentItems;
        }

        public CpcSketch cpcResult(final ProfileSpec.SketchParameters params) {
            return cpcResultOf(cpcLive, cpcMerged, params.cpcLgK);
        }

        public CompactSketch thetaResult(final ProfileSpec.SketchParameters params) {
            return thetaResultOf(thetaLive, thetaMerged, params.thetaLgK);
        }

        private static CpcSketch cpcResultOf(final CpcSketch live, final CpcSketch merged, final int lgK) {
            if(merged == null) {
                return live;
            }
            if(live == null || live.getEstimate() == 0d) {
                return merged;
            }
            final CpcUnion union = new CpcUnion(lgK);
            union.update(live);
            union.update(merged);
            return union.getResult();
        }

        private static CompactSketch thetaResultOf(final Sketch live, final CompactSketch merged, final int lgK) {
            if(live == null && merged == null) {
                return null;
            }
            if(merged == null) {
                return live.compact();
            }
            if(live == null || live.getRetainedEntries() == 0) {
                return merged;
            }
            final Union union = SetOperation.builder().setNominalEntries(1 << lgK).buildUnion();
            union.union(live);
            union.union(merged);
            return union.getResult();
        }

        private void writeObject(final ObjectOutputStream out) throws IOException {
            out.defaultWriteObject();
            writeBytes(out, kll == null ? null : kll.toByteArray());
            writeBytes(out, frequentItems == null ? null : frequentItems.toByteArray(SERDE));
            final CpcSketch cpc = cpcResultOf(cpcLive, cpcMerged, cpcLgK);
            writeBytes(out, cpc == null ? null : cpc.toByteArray());
            final CompactSketch theta = thetaResultOf(thetaLive, thetaMerged, thetaLgK);
            writeBytes(out, theta == null ? null : theta.toByteArray());
        }

        private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            final byte[] kllBytes = readBytes(in);
            if(kllBytes != null) {
                kll = KllDoublesSketch.heapify(Memory.wrap(kllBytes));
            }
            final byte[] fiBytes = readBytes(in);
            if(fiBytes != null) {
                frequentItems = ItemsSketch.getInstance(Memory.wrap(fiBytes), SERDE);
            }
            final byte[] cpcBytes = readBytes(in);
            if(cpcBytes != null) {
                cpcMerged = CpcSketch.heapify(cpcBytes);
                cpcLive = new CpcSketch(cpcLgK);
            }
            final byte[] thetaBytes = readBytes(in);
            if(thetaBytes != null) {
                thetaMerged = CompactSketch.heapify(Memory.wrap(thetaBytes));
                thetaLive = UpdateSketch.builder().setNominalEntries(1 << thetaLgK).build();
            }
        }
    }
}
