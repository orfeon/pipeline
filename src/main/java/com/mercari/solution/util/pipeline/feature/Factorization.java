package com.mercari.solution.util.pipeline.feature;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.util.domain.file.ResourceUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * Factorization machine over categorical and numeric fields (docs/design/feature-dsl.md §4.4):
 *
 * <pre>
 *   ŷ = w0 + Σ_f w_f[x_f] · u_f + Σ_{f&lt;g} r_fg · ⟨v_f[x_f], v_g[x_g]⟩ · u_f · u_g
 * </pre>
 *
 * A categorical field has a parameter set per value and {@code u = 1}; a numeric field has one parameter set and
 * {@code u} = its (standardised) value, so a pair with it is the value's slope times the affinity — a categorical ×
 * continuous interaction on the same latent space.
 *
 * {@code fm}: r ≡ 1; {@code fwfm}: one scalar r per field pair (its fitted values are a field-pair
 * importance ranking, exported with the artifact manifest). Training is coordinate descent: the model is
 * linear in every single parameter, so each parameter takes a closed-form step — the ridge minimiser under
 * squared loss (alternating least squares), one Newton step under the logistic and Plackett–Luce losses
 * ({@link Loss}). The scores are cached (and, for fm, the per-example sums {@code Σ_g v_g[x_g]}, so a factor's
 * slope is read in O(1)), so one sweep over all parameters costs O(N · F · k) for fm and O(N · F² · k) for fwfm.
 *
 * <p>The fit is a function of the example SET and the seed: the examples are put in a canonical order first
 * ({@link #ORDER}), so the order a runner delivers them in never reaches the model.
 *
 * <p>{@link Fits} holds what a fit stage serves: the whole-input model and, under {@code fit.mode: fold}, one
 * out-of-fold model per fold, or under {@code fit.mode: forward} one model per window change point
 * ({@link #fitAll}). Refits are aligned ({@link Model#alignTo}): a model's latent space is determined only up to
 * an orthogonal rotation (every inner product, hence every pair score and prediction, is invariant), so the
 * embeddings of two fits are comparable only once one is rotated into the other.
 */
public final class Factorization {

    private static final Logger LOG = LoggerFactory.getLogger(Factorization.class);
    private static final char SEPARATOR = (char) 1;

    private Factorization() {}

    /**
     * One training example: the field values (null = missing; a numeric field's is {@link #NUMERIC} when it has a value),
     * the raw values of the numeric fields ({@code x}, by field index; null without numeric fields), the target, the offset its score starts from (0 without
     * one; on the score scale: the target's own units under squared loss, log-odds under logistic, log-strength under
     * Plackett–Luce), the tag the fit plan partitions by — the row's fold under {@code fit.mode: fold}, its time block
     * under {@code forward}, 0 otherwise — and the contest it is ranked in (Plackett–Luce; null otherwise).
     */
    public record Example(String[] values, double[] x, double y, double offset, long tag, String contest) implements Serializable {
        public Example(final String[] values, final double y) {
            this(values, null, y, 0d, 0L, null);
        }

        public Example(final String[] values, final double y, final long tag) {
            this(values, null, y, 0d, tag, null);
        }

        public Example(final String[] values, final double y, final double offset, final long tag, final String contest) {
            this(values, null, y, offset, tag, contest);
        }
    }

    /** The value a numeric field carries in {@link Example#values} (and its one key in {@link Model#params}). */
    public static final String NUMERIC = "";

    /** The canonical example order: tag, contest, then the values (null first), the numeric values, the target and the offset. */
    static final Comparator<Example> ORDER = (a, b) -> {
        int c = Long.compare(a.tag(), b.tag());
        if (c != 0) return c;
        if (a.contest() == null || b.contest() == null) {
            if (a.contest() != b.contest()) return a.contest() == null ? -1 : 1;
        } else {
            c = a.contest().compareTo(b.contest());
            if (c != 0) return c;
        }
        for (int f = 0; f < a.values().length; f++) {
            final String x = a.values()[f], y = b.values()[f];
            if (x == null || y == null) {
                if (x != y) return x == null ? -1 : 1;
                continue;
            }
            c = x.compareTo(y);
            if (c != 0) return c;
        }
        if (a.x() != null && b.x() != null) {
            for (int f = 0; f < a.x().length; f++) {
                c = Double.compare(a.x()[f], b.x()[f]);
                if (c != 0) return c;
            }
        }
        c = Double.compare(a.y(), b.y());
        return c != 0 ? c : Double.compare(a.offset(), b.offset());
    };

    public static final class Model implements Serializable {
        public final List<String> fields;
        public final boolean fieldWeighted;
        public final int k;
        public double w0;
        /** field index → value → [w, v_1 .. v_k, n] (n = the training examples with the value) */
        public final List<Map<String, double[]>> params;
        /** pair weights r[f][g] (symmetric; 1 for fm) */
        public final double[][] r;
        /** The loss the model was fitted under ({@link Loss}; recorded in the artifact manifest). */
        public String loss = Loss.squared.name();
        /** Per field: numeric (one parameter set, scaled by the standardised value) or categorical. */
        public final boolean[] numeric;
        /** Per numeric field: the centre and the scale its value is standardised by (0 and 1 when not standardised). */
        public final double[] mean, scale;
        /** What the fit that produced the model did (null for a model read from an artifact). */
        public FitReport report;
        /** The training rows of the fit (the rows a contest loss learns from; 0 = unknown, for an artifact written without them). */
        public long rows;
        /**
         * fit.mode forward: the change point (time block) the model was fitted at — it read blocks up to this one; null
         * otherwise. It repeats the key the model is kept under because the side-input lookup ({@link Fits#modelFor})
         * hands out the model alone. Without a window the whole-input model is the last change point's (one object), so
         * it carries that change point too; nothing reads it there, and an artifact does not keep it.
         */
        public Long changePoint;

        Model(final List<String> fields, final boolean[] numeric, final boolean fieldWeighted, final int k) {
            this.fields = fields;
            this.numeric = numeric.clone();
            this.mean = new double[fields.size()];
            this.scale = new double[fields.size()];
            Arrays.fill(scale, 1d);
            this.fieldWeighted = fieldWeighted;
            this.k = k;
            this.params = new ArrayList<>();
            for (int f = 0; f < fields.size(); f++) params.add(new HashMap<>());
            this.r = new double[fields.size()][fields.size()];
            for (final double[] row : r) Arrays.fill(row, 1d);
        }

        double[] param(final int f, final String value) {
            return value == null ? null : params.get(f).get(value);
        }

        /** The multiplier of field {@code f}'s parameters: 1 for a categorical field, the standardised value of a numeric one (NaN when missing). */
        public double unit(final int f, final double[] x) {
            if (!numeric[f]) return 1d;
            if (x == null || !Double.isFinite(x[f])) return Double.NaN;
            return (x[f] - mean[f]) / scale[f];
        }

        /**
         * ⟨v_f[x_f], v_g[x_g]⟩ scaled by r_fg, or null when either value is unknown (categorical fields only: a model with
         * a numeric field needs the row's values, {@link #pair(String[], double[], int, int, boolean)}).
         */
        public Double pair(final String[] values, final int f, final int g) {
            requireCategorical();
            return pair(values, null, f, g, false);
        }

        /** The categorical-only readouts would drop a numeric field's terms without a word: refuse them instead. */
        private void requireCategorical() {
            for (final boolean b : numeric) {
                if (b) throw new IllegalStateException("factorization model with numeric fields " + fields + ": pass the row's numeric values");
            }
        }

        /**
         * {@code r_fg ⟨v_f[x_f], v_g[x_g]⟩ u_f u_g}: the pair's term of the prediction, or — {@code perUnit} — its value per
         * unit of the numeric fields' standardised values (the slope; {@code u = 1}), which does not depend on the row's
         * numeric value (a missing one included). Null when a categorical value is unknown or missing, or (not per unit)
         * a numeric value is missing.
         */
        public Double pair(final String[] values, final double[] x, final int f, final int g, final boolean perUnit) {
            final double[] a = perUnit && numeric[f] ? params.get(f).get(NUMERIC) : param(f, values[f]);
            final double[] b = perUnit && numeric[g] ? params.get(g).get(NUMERIC) : param(g, values[g]);
            if (a == null || b == null) return null;
            final double units = perUnit ? 1d : unit(f, x) * unit(g, x);
            if (Double.isNaN(units)) return null;
            double dot = 0;
            for (int d = 1; d <= k; d++) dot += a[d] * b[d];
            return r[f][g] * dot * units;
        }

        /**
         * The affinity between two sides of a row: {@code Σ_{f ∈ left, g ∈ right} r_fg ⟨v_f, v_g⟩ u_f u_g} over the pairs
         * whose both values are known — a side whose own entity is new still reads through its other fields (its
         * attributes). Null when no pair is known. {@code x} may be null only without numeric fields (their terms would
         * drop out without a word).
         */
        public Double between(final String[] values, final double[] x, final int[] left, final int[] right) {
            if (x == null) requireCategorical();
            double sum = 0;
            boolean any = false;
            for (final int f : left) {
                for (final int g : right) {
                    if (f == g) continue;
                    final Double pair = pair(values, x, f, g, false);
                    if (pair == null) continue;
                    sum += pair;
                    any = true;
                }
            }
            return any ? sum : null;
        }

        public double[] embedding(final String[] values, final int f, final int dims) {
            final double[] p = param(f, values[f]);
            if (p == null) return null;
            return Arrays.copyOfRange(p, 1, 1 + Math.min(dims, k));
        }

        /** The training examples that carried the row's value of field {@code f}: 0 for a value the fit never saw, null for a missing one. */
        public Long count(final String[] values, final int f) {
            if (values[f] == null) return null;
            final double[] p = params.get(f).get(values[f]);
            return p == null ? 0L : (long) p[k + 1];
        }

        /** Full linear predictor (without the offset) over categorical fields ({@link #pair(String[], int, int)}). */
        public double predict(final String[] values) {
            requireCategorical();
            return predict(values, null);
        }

        /** Full linear predictor (without the offset): an unknown or missing value contributes nothing. */
        public double predict(final String[] values, final double[] x) {
            double y = w0;
            for (int f = 0; f < fields.size(); f++) {
                final double[] p = param(f, values[f]);
                final double u = unit(f, x);
                if (p != null && !Double.isNaN(u)) y += p[0] * u;
            }
            for (int f = 0; f < fields.size(); f++) {
                for (int g = f + 1; g < fields.size(); g++) {
                    final Double pair = pair(values, x, f, g, false);
                    if (pair != null) y += pair;
                }
            }
            return y;
        }

        public int size() {
            int n = 0;
            for (final Map<String, double[]> m : params) n += m.size();
            return n;
        }

        /**
         * This model rotated into the latent coordinates of {@code previous} ({@link Alignment}, {@code mode}
         * procrustes | sign | none): the factors of the values both models know are paired, and every factor of this
         * model is multiplied by the orthogonal map that brings them closest. Inner products are unchanged, so the
         * pair scores and the predictions are those of the unrotated model (to rounding); only the embeddings move.
         * Returns a rotated copy (this model itself when there is nothing to align): a model may be a runner's input.
         */
        public Model alignTo(final Model previous, final String mode) {
            if (previous == null || mode == null || Alignment.NONE.equals(mode) || previous.k != k) return this;
            final List<double[]> a = new ArrayList<>();
            final List<double[]> b = new ArrayList<>();
            for (int f = 0; f < fields.size(); f++) {
                final int pf = previous.fields.indexOf(fields.get(f));
                if (pf < 0) continue;
                final Map<String, double[]> own = params.get(f);
                final Map<String, double[]> before = previous.params.get(pf);
                for (final String value : new TreeSet<>(own.keySet())) {
                    final double[] q = before.get(value);
                    if (q == null) continue;
                    a.add(Arrays.copyOfRange(own.get(value), 1, k + 1));
                    b.add(Arrays.copyOfRange(q, 1, k + 1));
                }
            }
            final Alignment.Map map = Alignment.map(mode, Alignment.cross(a, b, k), a);
            if (map == null) return this;
            final Model rotated = new Model(fields, numeric, fieldWeighted, k);
            rotated.w0 = w0;
            rotated.loss = loss;
            rotated.report = report;
            rotated.rows = rows;
            rotated.changePoint = changePoint;
            System.arraycopy(mean, 0, rotated.mean, 0, mean.length);
            System.arraycopy(scale, 0, rotated.scale, 0, scale.length);
            for (int f = 0; f < fields.size(); f++) {
                System.arraycopy(r[f], 0, rotated.r[f], 0, fields.size());
                for (final Map.Entry<String, double[]> e : params.get(f).entrySet()) {
                    final double[] p = e.getValue().clone();
                    System.arraycopy(Alignment.apply(Arrays.copyOfRange(p, 1, k + 1), map.r()), 0, p, 1, k);
                    rotated.params.get(f).put(e.getKey(), p);
                }
            }
            return rotated;
        }
    }

    /** {@code task.loss}: the target as a value (squared), a probability (logistic) or a rank within a contest (Plackett–Luce). */
    public enum Loss { squared, logistic, plackettLuce }

    /**
     * @param top        Plackett–Luce: the places that are choices (0 = every place)
     * @param descending Plackett–Luce: a larger target is a better place
     * @param linear     whether the per-value weights w are fitted (false: interactions only, e.g. over an offset
     *                   that already carries the main effects)
     * @param scales     per field: null for a categorical field, {@code zscore} / {@code none} for a numeric one (how its
     *                   value is standardised; the mean and standard deviation are the fit's own); null = all categorical
     * @param reg        the ridge per kind of parameter and field (a number: {@link Regularization#uniform})
     * @param tol        stop a fit once an epoch moves the penalised objective by less than this fraction ({@code als.tol};
     *                   0 = run every epoch); {@code epochs} is then the cap
     * @param label      the block, for the fit log (null: none)
     */
    public record Options(List<String> fields, boolean fieldWeighted, int latentDim, int epochs, Regularization reg, long seed,
                          Loss loss, int top, boolean descending, boolean linear, List<String> scales,
                          double tol, String label) implements Serializable {
        public Options {
            Objects.requireNonNull(reg, "reg");
        }

        public Options(final List<String> fields, final boolean fieldWeighted, final int latentDim, final int epochs, final double reg, final long seed) {
            this(fields, fieldWeighted, latentDim, epochs, reg, seed, Loss.squared, 0, false, true, null);
        }

        public Options(final List<String> fields, final boolean fieldWeighted, final int latentDim, final int epochs, final double reg, final long seed,
                       final Loss loss, final int top, final boolean descending, final boolean linear) {
            this(fields, fieldWeighted, latentDim, epochs, reg, seed, loss, top, descending, linear, null);
        }

        /** {@code reg} on every parameter. */
        public Options(final List<String> fields, final boolean fieldWeighted, final int latentDim, final int epochs, final double reg, final long seed,
                       final Loss loss, final int top, final boolean descending, final boolean linear, final List<String> scales) {
            this(fields, fieldWeighted, latentDim, epochs, Regularization.uniform(reg, fields.size()), seed, loss, top, descending, linear, scales);
        }

        /** Every epoch runs, no label. */
        public Options(final List<String> fields, final boolean fieldWeighted, final int latentDim, final int epochs, final Regularization reg, final long seed,
                       final Loss loss, final int top, final boolean descending, final boolean linear, final List<String> scales) {
            this(fields, fieldWeighted, latentDim, epochs, reg, seed, loss, top, descending, linear, scales, 0, null);
        }

        /** Per field: numeric or categorical. */
        public boolean[] numeric() {
            final boolean[] numeric = new boolean[fields.size()];
            for (int f = 0; f < numeric.length; f++) numeric[f] = scales != null && scales.get(f) != null;
            return numeric;
        }
    }

    /**
     * What one fit did (logged per fit, counted per block, kept with the model and in the artifact manifest): its
     * training rows and epochs, whether it converged ({@code als.tol}), the penalised objective (negative
     * log-likelihood + ½ Σ λ θ²) before the first and after the last epoch and the relative move of that last epoch,
     * the per-unit loss ({@code unit}: rmse | logloss | nll/choice) against the offset alone (ŷ = 0), the size of the
     * parameters (largest |w|, the per-value factor norms ‖v‖) and the spread of the scores ŷ (the offset excluded: the
     * mean within-contest standard deviation under a contest loss, the standard deviation over the rows otherwise).
     * {@code flags} are the warnings it raised: {@link #SPREAD}, {@link #MEMORIZING}, {@link #UNCONVERGED}.
     */
    public record FitReport(int rows, int epochs, boolean converged, double objectiveStart, double objectiveEnd, double lastChange,
                            String unit, double loss, double baselineLoss, double maxLinear, double factorNormP50, double factorNormP99,
                            double maxFactorNorm, double scoreSd, double maxScore, boolean contest, long millis,
                            List<String> flags) implements Serializable {
        /** A contest's scores spread by more than {@link #SPREAD_LIMIT} (sd within a contest): the probability piles onto one entry. */
        public static final String SPREAD = "spread";
        /** The training loss fell below a tenth of the offset's (logistic / contest): the parameters learn the rows by heart. */
        public static final String MEMORIZING = "memorizing";
        /**
         * The objective was still moving at the last epoch: above {@code als.tol} at the cap, or (no tol) by more than
         * {@link #MOVING}; or it is not finite (the fit diverged).
         */
        public static final String UNCONVERGED = "unconverged";
        static final double SPREAD_LIMIT = 5, MEMORIZING_RATIO = 0.1, MOVING = 1e-3;

        /** The log text of a flag. */
        public String describe(final String flag) {
            return switch (flag) {
                case SPREAD -> String.format("the scores spread by sd %.3g within a contest (> %.0f): the probabilities pile onto one entry"
                        + " - the factors are under-regularised (als.reg factors)", scoreSd, SPREAD_LIMIT);
                case MEMORIZING -> String.format("the training %s %.4g is below a tenth of the offset's %.4g: the model learns the training"
                        + " rows by heart (a pair close to unique per row, a weak factor ridge)", unit, loss, baselineLoss);
                case UNCONVERGED -> !Double.isFinite(objectiveEnd) ? "the objective is " + objectiveEnd + " after the fit: it diverged"
                        + " (a non-finite target or score, or a ridge too weak to hold the parameters)"
                        : converged || Double.isNaN(lastChange) ? flag : String.format("the objective still moved %.3g%% in the last epoch:"
                        + " the result depends on the epoch count (and, warm-started, on how many fits came before) - als.tol to stop at"
                        + " convergence, or more epochs when it is set (they are its cap)", 100 * lastChange);
                default -> flag;
            };
        }

        /** The per-fit log line. */
        public String summary() {
            return String.format("%d example(s), %d epoch(s)%s, %d ms; objective %.6g -> %.6g (last epoch %.3g%%); %s %.4g (offset only %.4g);"
                            + " |w| max %.3g, |v| p50 %.3g p99 %.3g max %.3g; score sd %.3g%s, max |score| %.3g",
                    rows, epochs, converged ? ", converged" : "", millis, objectiveStart, objectiveEnd, 100 * lastChange, unit, loss, baselineLoss,
                    maxLinear, factorNormP50, factorNormP99, maxFactorNorm, scoreSd, contest ? " within a contest" : "", maxScore);
        }

        /**
         * The manifest entry (no timing: the manifest describes the model, not the run). A non-finite number (the
         * lastChange of a fit that ran no epoch, the values of a diverged fit) is written as its string form
         * ({@link FitArtifact#lambdaJson}): JSON has no NaN / ±Infinity.
         */
        public JsonObject toJson() {
            final JsonObject json = new JsonObject();
            json.addProperty("rows", rows);
            json.addProperty("epochs", epochs);
            json.addProperty("converged", converged);
            json.add("objectiveStart", FitArtifact.lambdaJson(objectiveStart));
            json.add("objectiveEnd", FitArtifact.lambdaJson(objectiveEnd));
            json.add("lastChange", FitArtifact.lambdaJson(lastChange));
            json.addProperty("unit", unit);
            json.add("loss", FitArtifact.lambdaJson(loss));
            json.add("baselineLoss", FitArtifact.lambdaJson(baselineLoss));
            json.add("maxLinear", FitArtifact.lambdaJson(maxLinear));
            json.add("factorNormP50", FitArtifact.lambdaJson(factorNormP50));
            json.add("factorNormP99", FitArtifact.lambdaJson(factorNormP99));
            json.add("maxFactorNorm", FitArtifact.lambdaJson(maxFactorNorm));
            json.add("scoreSd", FitArtifact.lambdaJson(scoreSd));
            json.add("maxScore", FitArtifact.lambdaJson(maxScore));
            final JsonArray array = new JsonArray();
            flags.forEach(array::add);
            json.add("flags", array);
            return json;
        }
    }

    /**
     * The ridge {@code (g − λ θ) / (H + λ)} of every parameter ({@code als.reg}): an absolute amount added to the
     * parameter's curvature H, which is summed over the training rows that carry it — not a per-row penalty. Under
     * {@code perRow} ({@code als.regScale: perRow}) the λ of a value's weight and factors is multiplied by the value's
     * training rows. The bias and the field-pair weights are not per value: one parameter shared by (nearly) every row,
     * so a per-row λ would shrink it towards 0 by λ / (1 + λ) of its size (a constant target of 100 read as 75 at λ = 1)
     * — they keep their λ as it is.
     *
     * @param bias        the global bias w0 (a contest loss has none)
     * @param linear      per field: its per-value weights w
     * @param factors     per field: its latent factors v
     * @param pairWeights fwfm: the field-pair weights r_fg
     */
    public record Regularization(double bias, double[] linear, double[] factors, double pairWeights, boolean perRow) implements Serializable {
        public static Regularization uniform(final double reg, final int fields) {
            final double[] all = new double[fields];
            Arrays.fill(all, reg);
            return new Regularization(reg, all, all.clone(), reg, false);
        }

        /** The ridge of a parameter carried by {@code rows} training rows. */
        double of(final double reg, final int rows) {
            return perRow ? reg * rows : reg;
        }

        // by value: a record compares and prints its array components by identity
        @Override
        public boolean equals(final Object o) {
            return o instanceof Regularization other && Double.compare(bias, other.bias) == 0 && Arrays.equals(linear, other.linear)
                    && Arrays.equals(factors, other.factors) && Double.compare(pairWeights, other.pairWeights) == 0 && perRow == other.perRow;
        }

        @Override
        public int hashCode() {
            return Objects.hash(bias, Arrays.hashCode(linear), Arrays.hashCode(factors), pairWeights, perRow);
        }

        @Override
        public String toString() {
            return "Regularization[bias=" + bias + ", linear=" + Arrays.toString(linear) + ", factors=" + Arrays.toString(factors)
                    + ", pairWeights=" + pairWeights + ", perRow=" + perRow + "]";
        }
    }

    /**
     * How a block is refitted: {@code folds} out-of-fold models (≤ 1 = none), or one model per {@code forward} change
     * point over the blocks of a usable block's window ({@code windowBlocks}, 0 = every earlier block); a fit over fewer
     * than {@code minRows} examples is not solved; {@code align} the gauge mode of the refits; {@code warmStart} starts a
     * change point's fit from the one before it, for {@code warmEpochs} epochs.
     */
    public record Plan(int folds, boolean forward, int windowBlocks, int minRows, String align, boolean warmStart, int warmEpochs) implements Serializable {}

    /** The models a fit stage serves (see {@link #fitAll}). */
    public static final class Fits implements Serializable {
        public final Model total;
        /** fit.mode fold: the out-of-fold model per fold (null where a fold had too few examples), or null. */
        public final Model[] folds;
        /** fit.mode forward: the model per window change point (null where the window had too few examples), or null. */
        public final TreeMap<Long, Model> blocks;
        /** fit.mode forward: the block indices that carried examples (minBlocks counts these, not the change points). */
        public final TreeSet<Long> observedBlocks;

        Fits(final Model total, final Model[] folds, final TreeMap<Long, Model> blocks, final TreeSet<Long> observedBlocks) {
            this.total = total;
            this.folds = folds;
            this.blocks = blocks;
            this.observedBlocks = observedBlocks;
        }

        public static Fits ofTotal(final Model total) {
            return new Fits(total, null, null, null);
        }

        /**
         * What rows read: under fold / forward the whole-input model is never read by a row (it is the artifact's), so
         * it is left out of what is served; a static fit is returned as it is.
         */
        public Fits served() {
            return folds == null && blocks == null ? this : new Fits(null, folds, blocks, observedBlocks);
        }

        /**
         * The fold models, each rotated into the coordinates of the whole-input model so an embedding column means one
         * thing across folds. The rotation is a k × k gauge fitted over every value: it carries no row's outcome into
         * another fold's values.
         */
        public static Fits ofFolds(final Model total, final Model[] folds, final String align) {
            final Model[] aligned = new Model[folds.length];
            for (int f = 0; f < folds.length; f++) aligned[f] = folds[f] == null ? null : folds[f].alignTo(total, align);
            return new Fits(total, aligned, null, null);
        }

        /**
         * The model a row reads: its fold's out-of-fold model (null without a fold), the change point at or before its
         * usable block (null before {@code minBlocks} observed blocks), else the whole-input model.
         */
        public Model modelFor(final Integer fold, final Long usableBlock, final int minBlocks) {
            if (folds != null) return fold == null || fold < 0 || fold >= folds.length ? null : folds[fold];
            if (blocks != null) return usableBlock == null ? null : BlockSeries.lookup(blocks, observedBlocks, usableBlock, minBlocks);
            return total;
        }
    }

    /** Fits one model from scratch over the examples (any order). */
    public static Model fit(final Options options, final List<Example> examples) {
        final List<Example> sorted = new ArrayList<>(examples);
        sorted.sort(ORDER);
        final Model model = train(options, informative(options, sorted), null, options.epochs());
        log(options, "fit", model);
        return model;
    }

    /** {@link #fit}, or null over fewer than {@code minRows} examples (none at all included). */
    public static Model fit(final Options options, final List<Example> examples, final int minRows) {
        return fit(options, examples, minRows, "fit");
    }

    /** {@link #fit(Options, List, int)}, logged as {@code what} (the fold it is for). */
    public static Model fit(final Options options, final List<Example> examples, final int minRows, final String what) {
        final List<Example> sorted = new ArrayList<>(examples);
        sorted.sort(ORDER);
        return trainOrNull(options, sorted, null, options.epochs(), minRows, what);
    }

    /**
     * Fits every model of a plan: the whole-input model, and the fold or the forward models. The examples are tagged
     * with their fold / time block ({@link Example#tag}); a fold model is fitted from scratch on every other fold (a
     * warm start from the whole-input model would carry the fold's own outcomes into it), a forward model from the
     * change point before it under {@code warmStart}.
     */
    public static Fits fitAll(final Options options, final List<Example> examples, final Plan plan) {
        final List<Example> sorted = new ArrayList<>(examples);
        sorted.sort(ORDER);
        if (plan.folds() > 1) {
            final Model total = trainOrNull(options, sorted, null, options.epochs(), plan.minRows(), "whole input");
            final Model[] folds = new Model[plan.folds()];
            for (int f = 0; f < folds.length; f++) folds[f] = trainOrNull(options, outOfFold(sorted, f), null, options.epochs(), plan.minRows(), "fold " + f);
            return Fits.ofFolds(total, folds, plan.align());
        }
        if (!plan.forward()) return Fits.ofTotal(trainOrNull(options, sorted, null, options.epochs(), plan.minRows(), "fit"));
        final TreeMap<Long, Model> models = new TreeMap<>();
        final Model total = forward(options, sorted, plan, true, true, models::put);
        return new Fits(total, null, models, observedBlocks(sorted));
    }

    /**
     * The forward fit change point by change point (every observed block, and every block at which one leaves the
     * window, in time order): each model is handed to {@code sink} as soon as it is fitted — null when its window had
     * too few examples — and only the one before it is kept, so the models never sit in memory together. Returns the
     * whole-input model.
     */
    public static Model fitForward(final Options options, final List<Example> examples, final Plan plan, final BiConsumer<Long, Model> sink) {
        return fitForward(options, examples, plan, true, false, sink);
    }

    /**
     * {@link #fitForward}, fitting the whole-input model only when {@code total} (no row reads it: only the artifact
     * does, so a run that writes none skips its fit — under a window, one more fit over every example); null otherwise.
     * Under {@code parallel} the change points are fitted {@link #FIT_THREADS} at a time on a pool shared by every fit
     * of the JVM — unless warm-started, when each starts from the one before it — and still handed to {@code sink} one
     * by one in time order: every fit is from scratch, so the models are the sequential ones bit for bit, and only the
     * alignment, which reads the model before, runs in order. At most {@code 2 × FIT_THREADS} change points are fitted
     * ahead of the one handed out.
     */
    public static Model fitForward(final Options options, final List<Example> examples, final Plan plan, final boolean total,
                                   final boolean parallel, final BiConsumer<Long, Model> sink) {
        final List<Example> sorted = new ArrayList<>(examples);
        sorted.sort(ORDER);
        return forward(options, sorted, plan, total, parallel, sink);
    }

    /** The change points of a forward fit over the {@code observed} blocks: each of them, and each block at which one leaves a window of {@code windowBlocks}. */
    public static TreeSet<Long> changePoints(final TreeSet<Long> observed, final int windowBlocks) {
        final TreeSet<Long> changePoints = new TreeSet<>(observed);
        if (windowBlocks > 0) for (final long block : observed) changePoints.add(block + windowBlocks);
        return changePoints;
    }

    /** How many fits a parallel forward fit runs at once: the cores of the JVM (a worker's). */
    static final int FIT_THREADS = Math.max(1, Runtime.getRuntime().availableProcessors());

    /** The pool of the parallel forward fits, created on first use: daemon threads shared by every fit of the JVM. */
    private static final class FitPool {
        private static final AtomicInteger COUNT = new AtomicInteger();
        static final ExecutorService POOL = Executors.newFixedThreadPool(FIT_THREADS, runnable -> {
            final Thread thread = new Thread(runnable, "factorization-fit-" + COUNT.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }

    /** The model of a fit run on the pool, its failure rethrown as it was raised. */
    private static Model await(final CompletableFuture<Model> fit) {
        try {
            return fit.join();
        } catch (final CompletionException e) {
            if (e.getCause() instanceof RuntimeException runtime) throw runtime;
            if (e.getCause() instanceof Error error) throw error;
            throw e;
        }
    }

    /** The time blocks that carry examples (the tags of a forward fit's examples): what {@code minBlocks} counts. */
    public static TreeSet<Long> observedBlocks(final List<Example> examples) {
        final TreeSet<Long> observed = new TreeSet<>();
        for (final Example e : examples) observed.add(e.tag());
        return observed;
    }

    /** The examples of every fold but {@code fold} (the tag is the fold; an untagged row, tag −1, is in every fold's fit). */
    public static List<Example> outOfFold(final List<Example> examples, final int fold) {
        final List<Example> out = new ArrayList<>();
        for (final Example e : examples) if (e.tag() != fold) out.add(e);
        return out;
    }

    private static Model forward(final Options options, final List<Example> sorted, final Plan plan, final boolean withTotal,
                                 final boolean parallel, final BiConsumer<Long, Model> sink) {
        // sorted by tag first, so a block is a contiguous range [start, end)
        final TreeMap<Long, int[]> ranges = new TreeMap<>();
        for (int i = 0; i < sorted.size(); i++) {
            final long block = sorted.get(i).tag();
            final int index = i;
            ranges.computeIfAbsent(block, b -> new int[]{index, index})[1] = index + 1;
        }
        final TreeSet<Long> observed = new TreeSet<>(ranges.keySet());
        final List<Long> points = new ArrayList<>(changePoints(observed, plan.windowBlocks()));
        final List<List<Example>> windows = new ArrayList<>(points.size());
        for (final long at : points) {
            // the blocks a usable block `at` reads: (at − W, at], every block ≤ at without a window
            final Long first = plan.windowBlocks() > 0 ? observed.ceiling(at - plan.windowBlocks() + 1) : observed.isEmpty() ? null : observed.first();
            final Long last = observed.floor(at);
            windows.add(first == null || last == null || first > last ? List.of() : sorted.subList(ranges.get(first)[0], ranges.get(last)[1]));
        }
        // concurrent: every fit from scratch, run ahead on the pool and taken in time order (the whole-input fit too,
        // when it is one of its own: under a window)
        final boolean concurrent = parallel && !plan.warmStart() && FIT_THREADS > 1;
        final ArrayDeque<CompletableFuture<Model>> ahead = new ArrayDeque<>();
        CompletableFuture<Model> totalFit = null;
        try {
            if (concurrent && withTotal && plan.windowBlocks() > 0) {
                totalFit = CompletableFuture.supplyAsync(() -> trainOrNull(options, sorted, null, options.epochs(), plan.minRows(), "whole input"), FitPool.POOL);
            }
            Model previous = null;
            Model latest = null;
            int submitted = 0;
            for (int i = 0; i < points.size(); i++) {
                final long at = points.get(i);
                final Model fitted;
                if (concurrent) {
                    for (; submitted < points.size() && submitted < i + 2 * FIT_THREADS; submitted++) {
                        final List<Example> window = windows.get(submitted);
                        final String what = "forward change point " + points.get(submitted);
                        ahead.add(CompletableFuture.supplyAsync(() -> trainOrNull(options, window, null, options.epochs(), plan.minRows(), what), FitPool.POOL));
                    }
                    fitted = await(ahead.poll());
                } else {
                    final boolean warm = plan.warmStart() && previous != null;
                    fitted = trainOrNull(options, windows.get(i), warm ? previous : null, warm ? plan.warmEpochs() : options.epochs(), plan.minRows(),
                            "forward change point " + at + (warm ? " (warm start)" : ""));
                }
                Model model = fitted;
                if (model != null) {
                    model = model.alignTo(previous, plan.align());
                    model.changePoint = at;
                    previous = model;
                }
                sink.accept(at, model);
                latest = model;
            }
            if (!withTotal) return null;
            // the whole-input model (a static serving run loads it): the last change point when it reads every block
            if (plan.windowBlocks() == 0 && latest != null) return latest;
            final Model total;
            if (totalFit != null) {
                total = await(totalFit);
            } else {
                final boolean warm = plan.warmStart() && previous != null;
                total = trainOrNull(options, sorted, warm ? previous : null, warm ? plan.warmEpochs() : options.epochs(), plan.minRows(),
                        "whole input" + (warm ? " (warm start)" : ""));
            }
            return total == null ? null : total.alignTo(previous, plan.align());
        } finally {
            // after a failure the fits run ahead finish on their own: none is handed out
            for (final CompletableFuture<Model> fit : ahead) fit.cancel(false);
            if (totalFit != null) totalFit.cancel(false);
        }
    }

    private static Model trainOrNull(final Options options, final List<Example> sorted, final Model warm, final int epochs, final int minRows,
                                     final String what) {
        final List<Example> used = informative(options, sorted);
        if (used.isEmpty() || used.size() < minRows) {
            LOG.info("{}: {} example(s), not solved ({})", name(options, what), used.size(),
                    used.isEmpty() ? "no training example" : "fewer than fit.minRows " + minRows);
            return null;
        }
        final Model model = train(options, used, warm, epochs);
        log(options, what, model);
        return model;
    }

    /** The fit's log line, and a warning per flag it raised. */
    private static void log(final Options options, final String what, final Model model) {
        final String name = name(options, what);
        LOG.info("{}: {}", name, model.report.summary());
        for (final String flag : model.report.flags()) LOG.warn("{}: {}", name, model.report.describe(flag));
    }

    /** How the log names a fit: the block ({@link Options#label}) and which of its fits. */
    private static String name(final Options options, final String what) {
        return "factorization " + (options.label() == null ? "" : options.label() + " ") + what;
    }

    /**
     * The examples a fit learns from, in their order: under Plackett–Luce the rows of the contests with at least two
     * places only — a contest whose entries all share one place (a lone entry, or equal targets throughout) chooses
     * nothing, and a row without a contest is in none, so neither counts towards {@code minRows} nor gives a value
     * parameters or a {@code count}; every example otherwise.
     */
    static List<Example> informative(final Options options, final List<Example> examples) {
        if (options.loss() != Loss.plackettLuce) return examples;
        // per contest: its smallest and largest target (equal = one place)
        final Map<String, double[]> ranges = new HashMap<>();
        for (final Example e : examples) {
            if (e.contest() == null) continue;
            final double[] range = ranges.computeIfAbsent(e.contest(), c -> new double[]{e.y(), e.y()});
            range[0] = Math.min(range[0], e.y());
            range[1] = Math.max(range[1], e.y());
        }
        final List<Example> kept = new ArrayList<>(examples.size());
        for (final Example e : examples) {
            if (e.contest() == null) continue;
            final double[] range = ranges.get(e.contest());
            if (range[0] < range[1]) kept.add(e);
        }
        return kept.size() == examples.size() ? examples : kept;
    }

    /**
     * Coordinate Newton over examples in canonical order. Values get dense ids in order of first appearance; a value the
     * warm model knows starts from its parameters, any other from small random factors (drawn in that same order).
     *
     * <p>The score of an example is {@code s = offset + ŷ}, and ŷ is linear in every single parameter θ with a slope
     * {@code h} per example, so each parameter takes one Newton step {@code θ += (g − reg θ) / (H + reg)} on the
     * loss restricted to it ({@link Objective}). Under squared loss that step is the exact ridge minimiser — the
     * alternating least squares of the plain fit; under the logistic and Plackett–Luce losses, which are convex in
     * every single parameter, it is one step towards it (capped at {@link #MAX_STEP}).
     */
    private static Model train(final Options options, final List<Example> examples, final Model warm, final int epochs) {
        final long started = System.nanoTime();
        final int F = options.fields().size();
        final int k = options.latentDim();
        final int n = examples.size();
        final boolean contest = options.loss() == Loss.plackettLuce;
        final Random random = new Random(options.seed());
        final List<Map<String, Integer>> ids = new ArrayList<>();
        final List<List<String>> names = new ArrayList<>();
        for (int f = 0; f < F; f++) {
            ids.add(new HashMap<>());
            names.add(new ArrayList<>());
        }
        final boolean[] numeric = options.numeric();
        final int[] idx = new int[n * F];
        final List<List<double[]>> initial = new ArrayList<>();
        for (int f = 0; f < F; f++) initial.add(new ArrayList<>());
        for (int i = 0; i < n; i++) {
            final String[] x = examples.get(i).values();
            final double[] raw = examples.get(i).x();
            for (int f = 0; f < F; f++) {
                // a numeric field is present only with a finite value (whatever its text says)
                if (x[f] == null || numeric[f] && (raw == null || !Double.isFinite(raw[f]))) {
                    idx[i * F + f] = -1;
                    continue;
                }
                Integer id = ids.get(f).get(x[f]);
                if (id == null) {
                    id = names.get(f).size();
                    ids.get(f).put(x[f], id);
                    names.get(f).add(x[f]);
                    final int warmField = warm == null ? -1 : warm.fields.indexOf(options.fields().get(f));
                    final double[] known = warmField < 0 ? null : warm.param(warmField, x[f]);
                    final double[] p = new double[k + 1];
                    if (known != null) {
                        System.arraycopy(known, 0, p, 0, k + 1);
                    } else {
                        for (int d = 1; d <= k; d++) p[d] = random.nextGaussian() * 0.1;
                    }
                    if (!options.linear()) p[0] = 0;
                    initial.get(f).add(p);
                }
                idx[i * F + f] = id;
            }
        }
        // the multiplier of every field of every example: 1 for a categorical field, the standardised value of a numeric
        // one (null without numeric fields: every multiplier is 1, see multiplier)
        final double[] mean = new double[F], scale = new double[F];
        Arrays.fill(scale, 1d);
        boolean anyNumeric = false;
        for (final boolean b : numeric) anyNumeric |= b;
        final double[] u = anyNumeric ? new double[n * F] : null;
        if (u != null) Arrays.fill(u, 1d);
        for (int f = 0; f < F; f++) {
            if (!numeric[f]) continue;
            if ("zscore".equals(options.scales().get(f))) {
                double count = 0, sum = 0;
                for (int i = 0; i < n; i++) {
                    if (idx[i * F + f] < 0) continue;
                    count++;
                    sum += examples.get(i).x()[f];
                }
                if (count > 0) {
                    mean[f] = sum / count;
                    // the squared deviations from the mean (two passes): Σx² / n − mean² cancels away for a value far
                    // from 0 with a small spread
                    double squares = 0;
                    for (int i = 0; i < n; i++) {
                        if (idx[i * F + f] < 0) continue;
                        final double deviation = examples.get(i).x()[f] - mean[f];
                        squares += deviation * deviation;
                    }
                    final double sd = Math.sqrt(squares / count);
                    scale[f] = sd > 0 ? sd : 1d;
                }
            }
            for (int i = 0; i < n; i++) if (idx[i * F + f] >= 0) u[i * F + f] = (examples.get(i).x()[f] - mean[f]) / scale[f];
        }
        // a warm start keeps a numeric field's slope per raw unit: its parameters were fitted against the previous fit's
        // scale, so they move by the ratio of the scales (the centre's move is left to the epochs)
        if (warm != null) {
            for (int f = 0; f < F; f++) {
                if (!numeric[f]) continue;
                final int wf = warm.fields.indexOf(options.fields().get(f));
                final Integer id = ids.get(f).get(NUMERIC);
                if (wf < 0 || !warm.numeric[wf] || id == null || warm.param(wf, NUMERIC) == null) continue;
                final double ratio = scale[f] / warm.scale[wf];
                final double[] p = initial.get(f).get(id);
                for (int d = 0; d <= k; d++) p[d] *= ratio;
            }
        }
        // parameters as flat arrays: w[f][id], v[f][id · k + d]
        final double[][] w = new double[F][];
        final double[][] v = new double[F][];
        final int[][] start = new int[F][];
        final int[][] members = new int[F][];
        for (int f = 0; f < F; f++) {
            final int values = names.get(f).size();
            w[f] = new double[values];
            v[f] = new double[values * k];
            for (int id = 0; id < values; id++) {
                final double[] p = initial.get(f).get(id);
                w[f][id] = p[0];
                System.arraycopy(p, 1, v[f], id * k, k);
            }
            // CSR index: the examples of every value, in example order
            start[f] = new int[values + 1];
            for (int i = 0; i < n; i++) if (idx[i * F + f] >= 0) start[f][idx[i * F + f] + 1]++;
            for (int id = 0; id < values; id++) start[f][id + 1] += start[f][id];
            members[f] = new int[start[f][values]];
            final int[] fill = Arrays.copyOf(start[f], values);
            for (int i = 0; i < n; i++) if (idx[i * F + f] >= 0) members[f][fill[idx[i * F + f]]++] = i;
        }
        final double[][] r = new double[F][F];
        for (final double[] row : r) Arrays.fill(row, 1d);
        if (options.fieldWeighted() && warm != null && warm.fieldWeighted) {
            for (int f = 0; f < F; f++) {
                for (int g = 0; g < F; g++) {
                    final int wf = warm.fields.indexOf(options.fields().get(f)), wg = warm.fields.indexOf(options.fields().get(g));
                    if (f != g && wf >= 0 && wg >= 0) r[f][g] = warm.r[wf][wg];
                }
            }
        }
        // a contest loss sees score differences within a contest only: a global bias is not identified
        double w0 = warm == null || contest ? 0 : warm.w0;
        final boolean fm = !options.fieldWeighted();
        // fm: q[i · k + d] = Σ_g v_g[x_g][d], so the slope of v_f[x_f][d] is q − v_f[x_f][d]
        final double[] q = fm ? new double[n * k] : null;
        final double[] s = new double[n];
        for (int i = 0; i < n; i++) {
            double y = w0;
            for (int f = 0; f < F; f++) {
                final int id = idx[i * F + f];
                if (id >= 0) y += w[f][id] * multiplier(u, i * F + f);
            }
            for (int f = 0; f < F; f++) {
                final int a = idx[i * F + f];
                if (a < 0) continue;
                for (int g = f + 1; g < F; g++) {
                    final int b = idx[i * F + g];
                    if (b < 0) continue;
                    double dot = 0;
                    for (int d = 0; d < k; d++) dot += v[f][a * k + d] * v[g][b * k + d];
                    y += r[f][g] * dot * multiplier(u, i * F + f) * multiplier(u, i * F + g);
                }
            }
            s[i] = examples.get(i).offset() + y;
            if (fm) {
                for (int f = 0; f < F; f++) {
                    final int id = idx[i * F + f];
                    if (id >= 0) for (int d = 0; d < k; d++) q[i * k + d] += v[f][id * k + d] * multiplier(u, i * F + f);
                }
            }
        }
        final Objective objective = Objective.of(options, examples, s);
        final Regularization reg = options.reg();
        int longest = 0;
        for (int f = 0; f < F; f++) for (int id = 0; id + 1 < start[f].length; id++) longest = Math.max(longest, start[f][id + 1] - start[f][id]);
        final double[] h = new double[Math.max(longest, n)];
        final int[] all = new int[n];
        for (int i = 0; i < n; i++) all[i] = i;

        // refreshed here and after every epoch (what the loss caches, so rounding does not accumulate): each epoch
        // starts from fresh caches, and the objective is read from them
        objective.refresh();
        final double objectiveStart = -objective.logLikelihood() + penalty(reg, w0, w, v, r, start, k, contest, options.linear(), fm);
        double previous = objectiveStart, lastChange = Double.NaN;
        int ran = 0;
        boolean converged = false;
        for (int epoch = 0; epoch < epochs; epoch++) {
            // global bias
            if (!contest) {
                Arrays.fill(h, 0, n, 1d);
                final double delta = objective.delta(all, 0, n, h, w0, reg.bias());
                objective.shift(all, 0, n, h, delta);
                w0 += delta;
            }
            // linear weights
            if (options.linear()) {
                for (int f = 0; f < F; f++) {
                    for (int id = 0; id + 1 < start[f].length; id++) {
                        final int from = start[f][id], to = start[f][id + 1];
                        for (int j = from; j < to; j++) h[j - from] = multiplier(u, members[f][j] * F + f);
                        final double delta = objective.delta(members[f], from, to, h, w[f][id], reg.of(reg.linear()[f], to - from));
                        objective.shift(members[f], from, to, h, delta);
                        w[f][id] += delta;
                    }
                }
            }
            // factors: ŷ is linear in v_f[x][d] with slope h = u_f Σ_{g≠f} r_fg v_g[x_g][d] u_g
            for (int f = 0; f < F; f++) {
                for (int id = 0; id + 1 < start[f].length; id++) {
                    final int from = start[f][id], to = start[f][id + 1];
                    for (int d = 0; d < k; d++) {
                        final double p = v[f][id * k + d];
                        for (int j = from; j < to; j++) {
                            final int i = members[f][j];
                            final double own = multiplier(u, i * F + f);
                            h[j - from] = own * (fm ? q[i * k + d] - p * own : slope(r, v, u, idx, i, F, f, d, k));
                        }
                        final double delta = objective.delta(members[f], from, to, h, p, reg.of(reg.factors()[f], to - from));
                        objective.shift(members[f], from, to, h, delta);
                        if (fm) for (int j = from; j < to; j++) q[members[f][j] * k + d] += delta * multiplier(u, members[f][j] * F + f);
                        v[f][id * k + d] = p + delta;
                    }
                }
            }
            // field-pair weights (fwfm): ŷ is linear in r_fg with slope ⟨v_f, v_g⟩
            if (!fm) {
                for (int f = 0; f < F; f++) {
                    for (int g = f + 1; g < F; g++) {
                        for (int i = 0; i < n; i++) {
                            final int a = idx[i * F + f], b = idx[i * F + g];
                            double dot = 0;
                            if (a >= 0 && b >= 0) for (int d = 0; d < k; d++) dot += v[f][a * k + d] * v[g][b * k + d];
                            h[i] = a >= 0 && b >= 0 ? dot * multiplier(u, i * F + f) * multiplier(u, i * F + g) : 0;
                        }
                        final double delta = objective.delta(all, 0, n, h, r[f][g], reg.pairWeights());
                        objective.shift(all, 0, n, h, delta);
                        r[f][g] += delta;
                        r[g][f] = r[f][g];
                    }
                }
            }
            ran++;
            // the penalised objective the coordinate steps descend: its relative move decides convergence
            objective.refresh();
            final double current = -objective.logLikelihood() + penalty(reg, w0, w, v, r, start, k, contest, options.linear(), fm);
            lastChange = Math.abs(previous - current) / Math.max(Math.abs(previous), 1e-12);
            previous = current;
            if (LOG.isDebugEnabled()) LOG.debug("factorization epoch {}: {} objective={}", epoch + 1, objective.describe(), current);
            if (options.tol() > 0 && lastChange <= options.tol()) {
                converged = true;
                break;
            }
        }

        final Model model = new Model(options.fields(), numeric, options.fieldWeighted(), k);
        model.w0 = w0;
        model.loss = options.loss().name();
        System.arraycopy(mean, 0, model.mean, 0, F);
        System.arraycopy(scale, 0, model.scale, 0, F);
        for (int f = 0; f < F; f++) {
            for (int id = 0; id < names.get(f).size(); id++) {
                final double[] p = new double[k + 2];
                p[0] = w[f][id];
                System.arraycopy(v[f], id * k, p, 1, k);
                p[k + 1] = start[f][id + 1] - start[f][id];
                model.params.get(f).put(names.get(f).get(id), p);
            }
            if (!fm) System.arraycopy(r[f], 0, model.r[f], 0, F);
        }
        model.rows = n;
        model.report = report(options, examples, objective, objectiveStart, previous, lastChange, ran, converged, w, v, k, contest,
                (System.nanoTime() - started) / 1_000_000L);
        return model;
    }

    /** ½ Σ λ θ² over every fitted parameter (the ridge part of the objective). */
    private static double penalty(final Regularization reg, final double w0, final double[][] w, final double[][] v, final double[][] r,
                                  final int[][] start, final int k, final boolean contest,
                                  final boolean linear, final boolean fm) {
        double sum = contest ? 0 : reg.bias() * w0 * w0;
        for (int f = 0; f < w.length; f++) {
            for (int id = 0; id + 1 < start[f].length; id++) {
                final int rows = start[f][id + 1] - start[f][id];
                if (linear) sum += reg.of(reg.linear()[f], rows) * w[f][id] * w[f][id];
                double norm = 0;
                for (int d = 0; d < k; d++) norm += v[f][id * k + d] * v[f][id * k + d];
                sum += reg.of(reg.factors()[f], rows) * norm;
            }
            if (!fm) for (int g = f + 1; g < w.length; g++) sum += reg.pairWeights() * r[f][g] * r[f][g];
        }
        return sum / 2;
    }

    /**
     * The report of a fit whose {@code objective} holds the final scores (refreshed). The objective is spent here: its
     * scores are then set to the offsets alone for the baseline loss (the same examples, so the same loss structure,
     * without building a second one).
     */
    private static FitReport report(final Options options, final List<Example> examples, final Objective objective, final double objectiveStart,
                                    final double objectiveEnd, final double lastChange, final int epochs, final boolean converged,
                                    final double[][] w, final double[][] v, final int k, final boolean contest, final long millis) {
        final int n = examples.size();
        double maxLinear = 0;
        int values = 0;
        for (final double[] factors : v) values += factors.length / k;
        final double[] norms = new double[values];
        int at = 0;
        for (int f = 0; f < w.length; f++) {
            for (final double value : w[f]) maxLinear = Math.max(maxLinear, Math.abs(value));
            for (int id = 0; id * k < v[f].length; id++) {
                double norm = 0;
                for (int d = 0; d < k; d++) norm += v[f][id * k + d] * v[f][id * k + d];
                norms[at++] = Math.sqrt(norm);
            }
        }
        Arrays.sort(norms);
        // the scores without the offset: per contest under a contest loss, over the rows otherwise
        final Map<String, double[]> groups = new HashMap<>();
        double maxScore = 0;
        for (int i = 0; i < n; i++) {
            final double score = objective.s[i] - examples.get(i).offset();
            maxScore = Math.max(maxScore, Math.abs(score));
            final double[] moments = groups.computeIfAbsent(contest ? examples.get(i).contest() : "", c -> new double[3]);
            moments[0]++;
            moments[1] += score;
            moments[2] += score * score;
        }
        double sd = 0;
        int counted = 0;
        for (final double[] moments : groups.values()) {
            if (moments[0] < 2) continue;
            final double mean = moments[1] / moments[0];
            sd += Math.sqrt(Math.max(0, moments[2] / moments[0] - mean * mean));
            counted++;
        }
        sd = counted == 0 ? 0 : sd / counted;
        final double loss = objective.unitLoss();
        // the loss of the offset alone (ŷ = 0)
        for (int i = 0; i < n; i++) objective.s[i] = examples.get(i).offset();
        objective.refresh();
        final double baselineLoss = objective.unitLoss();
        final List<String> flags = new ArrayList<>();
        if (contest && sd > FitReport.SPREAD_LIMIT) flags.add(FitReport.SPREAD);
        if (options.loss() != Loss.squared && baselineLoss > 0 && loss < FitReport.MEMORIZING_RATIO * baselineLoss) flags.add(FitReport.MEMORIZING);
        // a diverged fit (a non-finite objective) passes every comparison above: it is unconverged whatever the tol
        if (!Double.isFinite(objectiveEnd) || (options.tol() > 0 ? !converged : lastChange > FitReport.MOVING)) flags.add(FitReport.UNCONVERGED);
        return new FitReport(n, epochs, converged, objectiveStart, objectiveEnd, lastChange, objective.unit(), loss, baselineLoss, maxLinear,
                quantile(norms, 0.5), quantile(norms, 0.99), norms.length == 0 ? 0 : norms[norms.length - 1], sd, maxScore, contest, millis,
                List.copyOf(flags));
    }

    private static double quantile(final double[] sorted, final double p) {
        return sorted.length == 0 ? 0 : sorted[(int) Math.min(sorted.length - 1, Math.floor(p * sorted.length))];
    }

    /** The largest single coordinate step under a loss whose Newton step is not exact (logistic, Plackett–Luce). */
    static final double MAX_STEP = 1d;

    /**
     * The loss of a fit over the example scores {@code s} (kept current by {@link #shift}): the coordinate Newton step
     * of one parameter given its slopes over the examples that carry it.
     */
    abstract static class Objective {
        final double[] s;

        Objective(final double[] s) {
            this.s = s;
        }

        static Objective of(final Options options, final List<Example> examples, final double[] s) {
            return switch (options.loss()) {
                case squared -> new Squared(examples, s);
                case logistic -> new Logistic(examples, s);
                case plackettLuce -> new PlackettLuce(examples, s, options.top(), options.descending());
            };
        }

        /**
         * The step of a parameter at {@code theta} whose slope on example {@code rows[j]} is {@code h[j − from]} for
         * j in {@code [from, to)}: {@code (g − reg θ) / (H + reg)} ({@link #derivatives}), capped at {@link #MAX_STEP}
         * unless the step is exact.
         */
        final double delta(final int[] rows, final int from, final int to, final double[] h, final double theta, final double reg) {
            final double[] gh = derivatives(rows, from, to, h);
            final double curvature = gh[1] + reg;
            if (!(curvature > 0)) return 0;
            final double step = (gh[0] - reg * theta) / curvature;
            return exact() ? step : capped(step);
        }

        /**
         * {g, H}: the derivative of the log-likelihood (the negative half squared error) along the parameter, and its
         * curvature (the negative second derivative, ≥ 0).
         */
        abstract double[] derivatives(int[] rows, int from, int to, double[] h);

        /** Whether the Newton step is the exact minimiser along the parameter (a quadratic loss). */
        boolean exact() {
            return false;
        }

        /** Moves the scores of the examples by {@code delta · h}. */
        void shift(final int[] rows, final int from, final int to, final double[] h, final double delta) {
            if (delta == 0) return;
            for (int j = from; j < to; j++) s[rows[j]] += delta * h[j - from];
        }

        /** Recomputes what the loss caches from the scores (once per epoch, so rounding does not accumulate). */
        void refresh() {
        }

        /** The loss of the current scores, for the log. */
        String describe() {
            return unit() + "=" + unitLoss();
        }

        /** The name of {@link #unitLoss}. */
        abstract String unit();

        /** The loss per unit (row / choice) of the current scores (after a {@link #refresh}). */
        abstract double unitLoss();

        /** The log-likelihood of the current scores (the negative half squared error under squared loss). */
        abstract double logLikelihood();

        static double capped(final double step) {
            return Math.max(-MAX_STEP, Math.min(MAX_STEP, step));
        }
    }

    /** Squared error: the Newton step is the exact ridge minimiser of the parameter. */
    static final class Squared extends Objective {
        private final double[] y;

        Squared(final List<Example> examples, final double[] s) {
            super(s);
            this.y = new double[examples.size()];
            for (int i = 0; i < y.length; i++) y[i] = examples.get(i).y();
        }

        @Override
        double[] derivatives(final int[] rows, final int from, final int to, final double[] h) {
            double g = 0, curvature = 0;
            for (int j = from; j < to; j++) {
                final int i = rows[j];
                final double slope = h[j - from];
                g += slope * (y[i] - s[i]);
                curvature += slope * slope;
            }
            return new double[]{g, curvature};
        }

        @Override
        boolean exact() {
            return true;
        }

        @Override
        String unit() {
            return "rmse";
        }

        @Override
        double unitLoss() {
            return Math.sqrt(-2 * logLikelihood() / Math.max(1, y.length));
        }

        @Override
        double logLikelihood() {
            double sse = 0;
            for (int i = 0; i < y.length; i++) sse += (y[i] - s[i]) * (y[i] - s[i]);
            return -sse / 2;
        }
    }

    /** Binary log loss on the log-odds score: y in [0, 1] (a rate or a 0 / 1 outcome). */
    static final class Logistic extends Objective {
        private final double[] y;

        Logistic(final List<Example> examples, final double[] s) {
            super(s);
            this.y = new double[examples.size()];
            for (int i = 0; i < y.length; i++) y[i] = examples.get(i).y();
        }

        static double sigmoid(final double x) {
            return x >= 0 ? 1 / (1 + Math.exp(-x)) : Math.exp(x) / (1 + Math.exp(x));
        }

        @Override
        double[] derivatives(final int[] rows, final int from, final int to, final double[] h) {
            double g = 0, curvature = 0;
            for (int j = from; j < to; j++) {
                final int i = rows[j];
                final double slope = h[j - from];
                final double p = sigmoid(s[i]);
                g += slope * (y[i] - p);
                curvature += slope * slope * p * (1 - p);
            }
            return new double[]{g, curvature};
        }

        @Override
        String unit() {
            return "logloss";
        }

        @Override
        double unitLoss() {
            return -logLikelihood() / Math.max(1, y.length);
        }

        @Override
        double logLikelihood() {
            double ll = 0;
            for (int i = 0; i < y.length; i++) {
                final double p = Math.min(1 - 1e-15, Math.max(1e-15, sigmoid(s[i])));
                ll += y[i] * Math.log(p) + (1 - y[i]) * Math.log(1 - p);
            }
            return ll;
        }
    }

    /**
     * Plackett–Luce over the examples of a contest (the same {@link Example#contest}): the entries are chosen best first,
     * each with probability {@code exp(s_j) / Σ_{l still in} exp(s_l)}. Places come from the target ({@code descending}:
     * larger is better) as 1 + the entries strictly better, so ties share a place; a place deeper than {@code top}
     * (0 = every place) adds no choice, its entries stay in the earlier pools. Tied entries are chosen from one pool
     * (Breslow: a tie group of size m is m choices; the rating op's Weng–Lin tie form weighs it as one, 1 / m each),
     * which keeps the likelihood free of the entry order. The last place is never a choice: its pool is the entries
     * at it, so they finish there in some order with probability 1 — one entry or a whole tie group alike (Breslow
     * would read a tied last group as pulling its entries together). A contest whose entries all share one place (a
     * lone entry, equal targets throughout) chooses nothing and is left out, as is an example without a contest.
     *
     * <p>Stage q of a contest is its q-th distinct place within {@code top}; an entry is in the pools of the stages
     * up to its own place ({@code last}), and is chosen at its own stage when its place is within {@code top}. The
     * pool sums {@code Z_q = Σ exp(s − m_c)} are kept per stage, shifted by the contest's largest score at the last
     * {@link #refresh} so they do not overflow.
     */
    static final class PlackettLuce extends Objective {
        private final int[] contestOf;
        private final int[] last;
        private final boolean[] chosen;
        /** Per contest: the first stage index into the flat stage arrays, and its stage count. */
        private final int[] base, stages;
        /** Per stage: the entries chosen at it (tied at its place), and the pool sum. */
        private final int[] size;
        private final double[] z, a, b;
        private final double[] shift;
        private final int[][] rowsOf;
        private final int[] touched;
        private final boolean[] isTouched;

        PlackettLuce(final List<Example> examples, final double[] s, final int top, final boolean descending) {
            super(s);
            final int n = examples.size();
            final Map<String, List<Integer>> byContest = new LinkedHashMap<>();
            for (int i = 0; i < n; i++) {
                final String c = examples.get(i).contest();
                if (c != null) byContest.computeIfAbsent(c, x -> new ArrayList<>()).add(i);
            }
            contestOf = new int[n];
            Arrays.fill(contestOf, -1);
            last = new int[n];
            chosen = new boolean[n];
            final List<int[]> kept = new ArrayList<>();
            final List<Integer> stageSizes = new ArrayList<>();
            final List<Integer> bases = new ArrayList<>(), counts = new ArrayList<>();
            for (final List<Integer> rows : byContest.values()) {
                if (rows.size() < 2) continue;
                final double[] ys = new double[rows.size()];
                for (int j = 0; j < ys.length; j++) ys[j] = examples.get(rows.get(j)).y();
                final int[] place = new int[ys.length];
                int lastPlace = 1;
                for (int j = 0; j < ys.length; j++) {
                    int better = 0;
                    for (final double other : ys) if (descending ? other > ys[j] : other < ys[j]) better++;
                    place[j] = 1 + better;
                    lastPlace = Math.max(lastPlace, place[j]);
                }
                final TreeSet<Integer> places = new TreeSet<>();
                for (final int p : place) if ((top <= 0 || p <= top) && p != lastPlace) places.add(p);
                if (places.isEmpty()) continue;
                final int c = kept.size();
                final List<Integer> ordered = new ArrayList<>(places);
                bases.add(stageSizes.size());
                counts.add(ordered.size());
                for (final int p : ordered) {
                    int tied = 0;
                    for (final int other : place) if (other == p) tied++;
                    stageSizes.add(tied);
                }
                for (int j = 0; j < ys.length; j++) {
                    final int i = rows.get(j);
                    contestOf[i] = c;
                    // the stages whose place is at or before the entry's own: it is in their pools
                    last[i] = places.headSet(place[j], true).size() - 1;
                    chosen[i] = places.contains(place[j]);
                }
                kept.add(rows.stream().mapToInt(Integer::intValue).toArray());
            }
            rowsOf = kept.toArray(new int[0][]);
            base = bases.stream().mapToInt(Integer::intValue).toArray();
            stages = counts.stream().mapToInt(Integer::intValue).toArray();
            size = stageSizes.stream().mapToInt(Integer::intValue).toArray();
            z = new double[size.length];
            a = new double[size.length];
            b = new double[size.length];
            shift = new double[rowsOf.length];
            touched = new int[rowsOf.length];
            isTouched = new boolean[rowsOf.length];
        }

        @Override
        void refresh() {
            Arrays.fill(z, 0d);
            for (int c = 0; c < rowsOf.length; c++) {
                double max = Double.NEGATIVE_INFINITY;
                for (final int i : rowsOf[c]) max = Math.max(max, s[i]);
                shift[c] = max;
                for (final int i : rowsOf[c]) {
                    final double e = Math.exp(s[i] - max);
                    for (int q = 0; q <= last[i]; q++) z[base[c] + q] += e;
                }
            }
        }

        @Override
        double[] derivatives(final int[] rows, final int from, final int to, final double[] h) {
            double g = 0, curvature = 0;
            int count = 0;
            for (int j = from; j < to; j++) {
                final int i = rows[j];
                final int c = contestOf[i];
                final double slope = h[j - from];
                if (c < 0 || slope == 0) continue;
                if (!isTouched[c]) {
                    isTouched[c] = true;
                    touched[count++] = c;
                }
                final double e = Math.exp(s[i] - shift[c]);
                for (int q = 0; q <= last[i]; q++) {
                    a[base[c] + q] += e * slope;
                    b[base[c] + q] += e * slope * slope;
                }
                if (chosen[i]) g += slope;
            }
            for (int t = 0; t < count; t++) {
                final int c = touched[t];
                isTouched[c] = false;
                for (int q = base[c]; q < base[c] + stages[c]; q++) {
                    if (a[q] != 0 || b[q] != 0) {
                        final double mean = a[q] / z[q];
                        g -= size[q] * mean;
                        curvature += size[q] * (b[q] / z[q] - mean * mean);
                        a[q] = 0;
                        b[q] = 0;
                    }
                }
            }
            return new double[]{g, curvature};
        }

        @Override
        void shift(final int[] rows, final int from, final int to, final double[] h, final double delta) {
            if (delta == 0) return;
            for (int j = from; j < to; j++) {
                final int i = rows[j];
                final double slope = h[j - from];
                if (slope == 0) continue;
                final int c = contestOf[i];
                if (c >= 0) {
                    final double before = Math.exp(s[i] - shift[c]);
                    final double after = Math.exp(s[i] + delta * slope - shift[c]);
                    for (int q = 0; q <= last[i]; q++) z[base[c] + q] += after - before;
                }
                s[i] += delta * slope;
            }
        }

        @Override
        String describe() {
            return "contests=" + rowsOf.length + " nll/choice=" + unitLoss();
        }

        @Override
        String unit() {
            return "nll/choice";
        }

        @Override
        double unitLoss() {
            int choices = 0;
            for (final int[] rows : rowsOf) for (final int i : rows) if (chosen[i]) choices++;
            return choices == 0 ? 0 : -logLikelihood() / choices;
        }

        /** From the cached pool sums: call {@link #refresh} after the scores moved outside {@link #shift}. */
        @Override
        double logLikelihood() {
            double ll = 0;
            for (int c = 0; c < rowsOf.length; c++) {
                for (final int i : rowsOf[c]) if (chosen[i]) ll += s[i] - shift[c] - Math.log(z[base[c] + last[i]]);
            }
            return ll;
        }
    }

    private static double slope(final double[][] r, final double[][] v, final double[] u, final int[] idx, final int i, final int F, final int f, final int d, final int k) {
        double h = 0;
        for (int g = 0; g < F; g++) {
            if (g == f) continue;
            final int id = idx[i * F + g];
            if (id >= 0) h += r[f][g] * v[g][id * k + d] * multiplier(u, i * F + g);
        }
        return h;
    }

    /** The per-example multiplier at {@code at}: {@code u[at]}, or 1 without numeric fields ({@code u} null). */
    private static double multiplier(final double[] u, final int at) {
        return u == null ? 1d : u[at];
    }

    // ------------------------------------------------------------------------------------------
    // artifact
    // ------------------------------------------------------------------------------------------

    public static final Schema SCHEMA = new Schema.Parser().parse("""
            {"type": "record", "name": "FeatureFmParam", "namespace": "com.mercari.solution.feature",
             "fields": [
               {"name": "field", "type": "string"},
               {"name": "value", "type": "string"},
               {"name": "w", "type": "double"},
               {"name": "v", "type": "bytes"},
               {"name": "n", "type": "long", "default": 0},
               {"name": "mean", "type": "double", "default": 0},
               {"name": "scale", "type": "double", "default": 1}
             ]}
            """);

    public static String artifactPath(final String artifactUri, final String planHash, final String block) {
        return FitArtifact.directory(artifactUri, planHash) + "/" + block + ".fm.avro";
    }

    public static boolean exists(final String artifactUri, final String planHash, final String block) {
        return ResourceUtil.exists(artifactPath(artifactUri, planHash, block));
    }

    public static void write(final String artifactUri, final String planHash, final String block, final Model model) {
        final String path = artifactPath(artifactUri, planHash, block);
        try {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (final DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(SCHEMA))) {
                writer.create(SCHEMA, bytes);
                // n of the global record: the training rows of the fit
                writer.append(record("__global", "", model.w0, new double[0], model.rows, 0, 1));
                for (int f = 0; f < model.fields.size(); f++) {
                    for (int g = f + 1; g < model.fields.size(); g++) {
                        writer.append(record("__r", model.fields.get(f) + SEPARATOR + model.fields.get(g), model.r[f][g], new double[0], 0L, 0, 1));
                    }
                }
                for (int f = 0; f < model.fields.size(); f++) {
                    final List<String> values = new ArrayList<>(model.params.get(f).keySet());
                    Collections.sort(values);
                    for (final String value : values) {
                        final double[] p = model.params.get(f).get(value);
                        writer.append(record(model.fields.get(f), value, p[0], Arrays.copyOfRange(p, 1, model.k + 1), (long) p[model.k + 1],
                                model.mean[f], model.scale[f]));
                    }
                }
            }
            ResourceUtil.writeBytes(path, bytes.toByteArray());
            final JsonObject manifest = FitArtifact.manifest(planHash, block);
            manifest.addProperty("variant", model.fieldWeighted ? "fwfm" : "fm");
            manifest.addProperty("latentDim", model.k);
            manifest.addProperty("loss", model.loss);
            // the standardisation of every numeric field: what a serving run's values are scaled by
            final JsonObject numericFields = new JsonObject();
            for (int f = 0; f < model.fields.size(); f++) {
                if (!model.numeric[f]) continue;
                final JsonObject standardisation = new JsonObject();
                standardisation.addProperty("mean", model.mean[f]);
                standardisation.addProperty("scale", model.scale[f]);
                numericFields.add(model.fields.get(f), standardisation);
            }
            if (!numericFields.isEmpty()) manifest.add("numeric", numericFields);
            manifest.addProperty("parameters", model.size());
            final JsonArray fields = new JsonArray();
            model.fields.forEach(fields::add);
            manifest.add("fields", fields);
            // fwfm: the field-pair weights rank the interactions (§4.4, lineage feedback for the generative DSL)
            final JsonArray pairs = new JsonArray();
            for (int f = 0; f < model.fields.size(); f++) {
                for (int g = f + 1; g < model.fields.size(); g++) {
                    final JsonObject pair = new JsonObject();
                    pair.addProperty("fields", model.fields.get(f) + "," + model.fields.get(g));
                    pair.addProperty("r", model.r[f][g]);
                    pairs.add(pair);
                }
            }
            manifest.add("pairWeights", pairs);
            if (model.report != null) manifest.add("fit", model.report.toJson());
            ResourceUtil.writeString(FitArtifact.directory(artifactUri, planHash) + "/" + block + ".fm.manifest.json", manifest.toString());
            LOG.info("wrote factorization artifact {} ({} parameters)", path, model.size());
        } catch (final IOException e) {
            throw new RuntimeException("Failed to write factorization artifact: " + path, e);
        }
    }

    private static GenericRecord record(final String field, final String value, final double w, final double[] v, final long n,
                                        final double mean, final double scale) {
        final GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("field", field);
        record.put("value", value);
        record.put("w", w);
        // latent vector as big-endian doubles: array<double> round-trips with float precision on this Avro version
        final java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(8 * v.length);
        for (final double d : v) buffer.putDouble(d);
        buffer.flip();
        record.put("v", buffer);
        record.put("n", n);
        record.put("mean", mean);
        record.put("scale", scale);
        return record;
    }

    /** Reads an artifact of categorical fields. */
    public static Model read(final String artifactUri, final String planHash, final String block,
                             final List<String> fields, final boolean fieldWeighted, final int k) {
        return read(artifactUri, planHash, block, fields, new boolean[fields.size()], fieldWeighted, k);
    }

    /** Reads an artifact; one written before the per-value counts were kept reads count 0 for every value. */
    public static Model read(final String artifactUri, final String planHash, final String block,
                             final List<String> fields, final boolean[] numeric, final boolean fieldWeighted, final int k) {
        final String path = artifactPath(artifactUri, planHash, block);
        final Model model = new Model(fields, numeric, fieldWeighted, k);
        try (final DataFileReader<GenericRecord> reader = new DataFileReader<>(
                new SeekableByteArrayInput(ResourceUtil.readBytes(path)), new GenericDatumReader<>(null, SCHEMA))) {
            while (reader.hasNext()) {
                final GenericRecord record = reader.next();
                final String field = record.get("field").toString();
                final String value = record.get("value").toString();
                final double w = (Double) record.get("w");
                if ("__global".equals(field)) {
                    model.w0 = w;
                    model.rows = (Long) record.get("n");
                } else if ("__r".equals(field)) {
                    final int sep = value.indexOf(SEPARATOR);
                    final int f = fields.indexOf(value.substring(0, sep));
                    final int g = fields.indexOf(value.substring(sep + 1));
                    if (f >= 0 && g >= 0) {
                        model.r[f][g] = w;
                        model.r[g][f] = w;
                    }
                } else {
                    final int f = fields.indexOf(field);
                    if (f < 0) continue;
                    final java.nio.ByteBuffer v = (java.nio.ByteBuffer) record.get("v");
                    final double[] p = new double[k + 2];
                    p[0] = w;
                    for (int d = 0; d < k && v.remaining() >= 8; d++) p[d + 1] = v.getDouble();
                    p[k + 1] = (Long) record.get("n");
                    model.params.get(f).put(value, p);
                    if (numeric[f]) {
                        model.mean[f] = (Double) record.get("mean");
                        model.scale[f] = (Double) record.get("scale");
                    }
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException("Failed to read factorization artifact: " + path, e);
        }
        LOG.info("loaded factorization artifact {} ({} parameters)", path, model.size());
        return model;
    }

}
