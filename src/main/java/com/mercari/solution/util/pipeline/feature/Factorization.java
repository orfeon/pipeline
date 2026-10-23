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

/**
 * Factorization machine over categorical fields (docs/design/feature-dsl.md §4.4):
 *
 * <pre>
 *   ŷ = w0 + Σ_f w_f[x_f] + Σ_{f&lt;g} r_fg · ⟨v_f[x_f], v_g[x_g]⟩
 * </pre>
 *
 * {@code fm}: r ≡ 1; {@code fwfm}: one scalar r per field pair (its fitted values are a field-pair
 * importance ranking, exported with the artifact manifest). Training is alternating least squares: the
 * model is linear in every single parameter, so each parameter has a closed-form ridge update; residuals
 * are cached (and, for fm, the per-example sums {@code Σ_g v_g[x_g]}, so a factor's slope is read in O(1))
 * so one sweep over all parameters costs O(N · F · k) for fm and O(N · F² · k) for fwfm.
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
     * One training example: the field values (null = missing), the (offset-adjusted) target, and the tag the fit
     * plan partitions by — the row's fold under {@code fit.mode: fold}, its time block under {@code forward}, 0
     * otherwise.
     */
    public record Example(String[] values, double y, long tag) implements Serializable {
        public Example(final String[] values, final double y) {
            this(values, y, 0L);
        }
    }

    /** The canonical example order: tag, then the values (null first), then the target. */
    static final Comparator<Example> ORDER = (a, b) -> {
        int c = Long.compare(a.tag(), b.tag());
        if (c != 0) return c;
        for (int f = 0; f < a.values().length; f++) {
            final String x = a.values()[f], y = b.values()[f];
            if (x == null || y == null) {
                if (x != y) return x == null ? -1 : 1;
                continue;
            }
            c = x.compareTo(y);
            if (c != 0) return c;
        }
        return Double.compare(a.y(), b.y());
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

        Model(final List<String> fields, final boolean fieldWeighted, final int k) {
            this.fields = fields;
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

        /** ⟨v_f[x_f], v_g[x_g]⟩ scaled by r_fg, or null when either value is unknown. */
        public Double pair(final String[] values, final int f, final int g) {
            final double[] a = param(f, values[f]);
            final double[] b = param(g, values[g]);
            if (a == null || b == null) return null;
            double dot = 0;
            for (int d = 1; d <= k; d++) dot += a[d] * b[d];
            return r[f][g] * dot;
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

        /** Full linear predictor (without the offset). */
        public double predict(final String[] values) {
            double y = w0;
            for (int f = 0; f < fields.size(); f++) {
                final double[] p = param(f, values[f]);
                if (p != null) y += p[0];
            }
            for (int f = 0; f < fields.size(); f++) {
                for (int g = f + 1; g < fields.size(); g++) {
                    final Double pair = pair(values, f, g);
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
            final Model rotated = new Model(fields, fieldWeighted, k);
            rotated.w0 = w0;
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

    public record Options(List<String> fields, boolean fieldWeighted, int latentDim, int epochs, double reg, long seed) implements Serializable {}

    /**
     * How a block is refitted: {@code folds} out-of-fold models (≤ 1 = none), or one model per {@code forward} change
     * point over the blocks of a usable block's window ({@code windowBlocks}, 0 = every earlier block); a fit over fewer
     * than {@code minRows} examples is not solved; {@code align} the gauge mode of the refits; {@code warmStart} starts a
     * change point's fit from the one before it, for {@code warmEpochs} epochs.
     */
    public record Plan(int folds, boolean forward, int windowBlocks, int minRows, String align, boolean warmStart, int warmEpochs) implements Serializable {
        public static final Plan STATIC = new Plan(0, false, 0, 0, null, false, 0);
    }

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
            if (blocks != null) {
                if (usableBlock == null) return null;
                final Map.Entry<Long, Model> floor = blocks.floorEntry(usableBlock);
                if (floor == null || observedBlocks.headSet(usableBlock, true).size() < minBlocks) return null;
                return floor.getValue();
            }
            return total;
        }
    }

    /** Fits one model from scratch over the examples (any order). */
    public static Model fit(final Options options, final List<Example> examples) {
        final List<Example> sorted = new ArrayList<>(examples);
        sorted.sort(ORDER);
        return train(options, sorted, null, options.epochs());
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
            final Model total = trainOrNull(options, sorted, null, options.epochs(), plan.minRows());
            final Model[] folds = new Model[plan.folds()];
            for (int f = 0; f < folds.length; f++) folds[f] = trainOrNull(options, outOfFold(sorted, f), null, options.epochs(), plan.minRows());
            return Fits.ofFolds(total, folds, plan.align());
        }
        if (!plan.forward()) return Fits.ofTotal(trainOrNull(options, sorted, null, options.epochs(), plan.minRows()));
        return fitForward(options, sorted, plan);
    }

    /** The examples of every fold but {@code fold} (the tag is the fold; an untagged row, tag −1, is in every fold's fit). */
    public static List<Example> outOfFold(final List<Example> examples, final int fold) {
        final List<Example> out = new ArrayList<>();
        for (final Example e : examples) if (e.tag() != fold) out.add(e);
        return out;
    }

    /** One model per change point (every observed block, and every block at which one leaves the window), in time order. */
    private static Fits fitForward(final Options options, final List<Example> sorted, final Plan plan) {
        // sorted by tag first, so a block is a contiguous range [start, end)
        final TreeMap<Long, int[]> ranges = new TreeMap<>();
        for (int i = 0; i < sorted.size(); i++) {
            final long block = sorted.get(i).tag();
            final int index = i;
            ranges.computeIfAbsent(block, b -> new int[]{index, index})[1] = index + 1;
        }
        final TreeSet<Long> observed = new TreeSet<>(ranges.keySet());
        final TreeSet<Long> changePoints = new TreeSet<>(observed);
        if (plan.windowBlocks() > 0) for (final long block : observed) changePoints.add(block + plan.windowBlocks());
        final TreeMap<Long, Model> models = new TreeMap<>();
        Model previous = null;
        for (final long at : changePoints) {
            // the blocks a usable block `at` reads: (at − W, at], every block ≤ at without a window
            final Long first = plan.windowBlocks() > 0 ? observed.ceiling(at - plan.windowBlocks() + 1) : observed.isEmpty() ? null : observed.first();
            final Long last = observed.floor(at);
            final List<Example> window = first == null || last == null || first > last ? List.of()
                    : sorted.subList(ranges.get(first)[0], ranges.get(last)[1]);
            final boolean warm = plan.warmStart() && previous != null;
            Model model = trainOrNull(options, window, warm ? previous : null, warm ? plan.warmEpochs() : options.epochs(), plan.minRows());
            if (model != null) {
                model = model.alignTo(previous, plan.align());
                previous = model;
            }
            models.put(at, model);
            LOG.info("factorization forward: change point {} - {} example(s){}", at, window.size(), model == null ? ", not solved (fit.minRows)" : "");
        }
        // the whole-input model (a static serving run loads it): the last change point when it reads every block
        Model total;
        if (plan.windowBlocks() == 0 && !models.isEmpty() && models.lastEntry().getValue() != null) {
            total = models.lastEntry().getValue();
        } else {
            final boolean warm = plan.warmStart() && previous != null;
            total = trainOrNull(options, sorted, warm ? previous : null, warm ? plan.warmEpochs() : options.epochs(), plan.minRows());
            if (total != null) total = total.alignTo(previous, plan.align());
        }
        return new Fits(total, null, models, observed);
    }

    private static Model trainOrNull(final Options options, final List<Example> sorted, final Model warm, final int epochs, final int minRows) {
        if (sorted.isEmpty() || sorted.size() < minRows) return null;
        return train(options, sorted, warm, epochs);
    }

    /**
     * ALS over examples in canonical order. Values get dense ids in order of first appearance; a value the warm model
     * knows starts from its parameters, any other from small random factors (drawn in that same order).
     */
    private static Model train(final Options options, final List<Example> examples, final Model warm, final int epochs) {
        final int F = options.fields().size();
        final int k = options.latentDim();
        final int n = examples.size();
        final Random random = new Random(options.seed());
        final List<Map<String, Integer>> ids = new ArrayList<>();
        final List<List<String>> names = new ArrayList<>();
        for (int f = 0; f < F; f++) {
            ids.add(new HashMap<>());
            names.add(new ArrayList<>());
        }
        final int[] idx = new int[n * F];
        final List<List<double[]>> initial = new ArrayList<>();
        for (int f = 0; f < F; f++) initial.add(new ArrayList<>());
        for (int i = 0; i < n; i++) {
            final String[] x = examples.get(i).values();
            for (int f = 0; f < F; f++) {
                if (x[f] == null) {
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
                    initial.get(f).add(p);
                }
                idx[i * F + f] = id;
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
        double w0 = warm == null ? 0 : warm.w0;
        final boolean fm = !options.fieldWeighted();
        // fm: q[i · k + d] = Σ_g v_g[x_g][d], so the slope of v_f[x_f][d] is q − v_f[x_f][d]
        final double[] q = fm ? new double[n * k] : null;
        final double[] e = new double[n];
        for (int i = 0; i < n; i++) {
            double y = w0;
            for (int f = 0; f < F; f++) {
                final int id = idx[i * F + f];
                if (id >= 0) y += w[f][id];
            }
            for (int f = 0; f < F; f++) {
                final int a = idx[i * F + f];
                if (a < 0) continue;
                for (int g = f + 1; g < F; g++) {
                    final int b = idx[i * F + g];
                    if (b < 0) continue;
                    double dot = 0;
                    for (int d = 0; d < k; d++) dot += v[f][a * k + d] * v[g][b * k + d];
                    y += r[f][g] * dot;
                }
            }
            e[i] = examples.get(i).y() - y;
            if (fm) {
                for (int f = 0; f < F; f++) {
                    final int id = idx[i * F + f];
                    if (id >= 0) for (int d = 0; d < k; d++) q[i * k + d] += v[f][id * k + d];
                }
            }
        }
        final double reg = options.reg();
        int longest = 0;
        for (int f = 0; f < F; f++) for (int id = 0; id + 1 < start[f].length; id++) longest = Math.max(longest, start[f][id + 1] - start[f][id]);
        final double[] h = new double[longest];

        for (int epoch = 0; epoch < epochs; epoch++) {
            // global bias
            {
                double num = 0;
                for (int i = 0; i < n; i++) num += e[i] + w0;
                final double updated = num / (n + reg);
                final double delta = updated - w0;
                for (int i = 0; i < n; i++) e[i] -= delta;
                w0 = updated;
            }
            // linear weights
            for (int f = 0; f < F; f++) {
                for (int id = 0; id + 1 < start[f].length; id++) {
                    final int from = start[f][id], to = start[f][id + 1];
                    double num = 0;
                    for (int j = from; j < to; j++) num += e[members[f][j]] + w[f][id];
                    final double updated = num / ((to - from) + reg);
                    final double delta = updated - w[f][id];
                    for (int j = from; j < to; j++) e[members[f][j]] -= delta;
                    w[f][id] = updated;
                }
            }
            // factors: ŷ is linear in v_f[x][d] with slope h = Σ_{g≠f} r_fg v_g[x_g][d]
            for (int f = 0; f < F; f++) {
                for (int id = 0; id + 1 < start[f].length; id++) {
                    final int from = start[f][id], to = start[f][id + 1];
                    for (int d = 0; d < k; d++) {
                        final double p = v[f][id * k + d];
                        double num = 0, den = reg;
                        for (int j = from; j < to; j++) {
                            final int i = members[f][j];
                            final double slope = fm ? q[i * k + d] - p : slope(r, v, idx, i, F, f, d, k);
                            h[j - from] = slope;
                            num += slope * (e[i] + p * slope);
                            den += slope * slope;
                        }
                        final double updated = den == 0 ? 0 : num / den;
                        final double delta = updated - p;
                        for (int j = from; j < to; j++) {
                            final int i = members[f][j];
                            e[i] -= delta * h[j - from];
                            if (fm) q[i * k + d] += delta;
                        }
                        v[f][id * k + d] = updated;
                    }
                }
            }
            // field-pair weights (fwfm): ŷ is linear in r_fg with slope ⟨v_f, v_g⟩
            if (!fm) {
                final double[] dots = new double[n];
                for (int f = 0; f < F; f++) {
                    for (int g = f + 1; g < F; g++) {
                        double num = 0, den = reg;
                        for (int i = 0; i < n; i++) {
                            final int a = idx[i * F + f], b = idx[i * F + g];
                            double dot = 0;
                            if (a >= 0 && b >= 0) for (int d = 0; d < k; d++) dot += v[f][a * k + d] * v[g][b * k + d];
                            dots[i] = dot;
                            num += dot * (e[i] + r[f][g] * dot);
                            den += dot * dot;
                        }
                        final double updated = den == 0 ? r[f][g] : num / den;
                        final double delta = updated - r[f][g];
                        for (int i = 0; i < n; i++) e[i] -= delta * dots[i];
                        r[f][g] = updated;
                        r[g][f] = updated;
                    }
                }
            }
            if (LOG.isDebugEnabled()) {
                double sse = 0;
                for (final double x : e) sse += x * x;
                LOG.debug("factorization epoch {}: rmse={}", epoch + 1, Math.sqrt(sse / Math.max(1, n)));
            }
        }
        double sse = 0;
        for (final double x : e) sse += x * x;
        LOG.info("factorization fit: {} example(s), {} epoch(s){}, rmse={}", n, epochs, warm == null ? "" : " (warm start)", Math.sqrt(sse / Math.max(1, n)));

        final Model model = new Model(options.fields(), options.fieldWeighted(), k);
        model.w0 = w0;
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
        return model;
    }

    private static double slope(final double[][] r, final double[][] v, final int[] idx, final int i, final int F, final int f, final int d, final int k) {
        double h = 0;
        for (int g = 0; g < F; g++) {
            if (g == f) continue;
            final int id = idx[i * F + g];
            if (id >= 0) h += r[f][g] * v[g][id * k + d];
        }
        return h;
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
               {"name": "n", "type": "long", "default": 0}
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
                writer.append(record("__global", "", model.w0, new double[0], 0L));
                for (int f = 0; f < model.fields.size(); f++) {
                    for (int g = f + 1; g < model.fields.size(); g++) {
                        writer.append(record("__r", model.fields.get(f) + SEPARATOR + model.fields.get(g), model.r[f][g], new double[0], 0L));
                    }
                }
                for (int f = 0; f < model.fields.size(); f++) {
                    final List<String> values = new ArrayList<>(model.params.get(f).keySet());
                    Collections.sort(values);
                    for (final String value : values) {
                        final double[] p = model.params.get(f).get(value);
                        writer.append(record(model.fields.get(f), value, p[0], Arrays.copyOfRange(p, 1, model.k + 1), (long) p[model.k + 1]));
                    }
                }
            }
            ResourceUtil.writeBytes(path, bytes.toByteArray());
            final JsonObject manifest = FitArtifact.manifest(planHash, block);
            manifest.addProperty("variant", model.fieldWeighted ? "fwfm" : "fm");
            manifest.addProperty("latentDim", model.k);
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
            ResourceUtil.writeString(FitArtifact.directory(artifactUri, planHash) + "/" + block + ".fm.manifest.json", manifest.toString());
            LOG.info("wrote factorization artifact {} ({} parameters)", path, model.size());
        } catch (final IOException e) {
            throw new RuntimeException("Failed to write factorization artifact: " + path, e);
        }
    }

    private static GenericRecord record(final String field, final String value, final double w, final double[] v, final long n) {
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
        return record;
    }

    /** Reads an artifact; one written before the per-value counts were kept reads count 0 for every value. */
    public static Model read(final String artifactUri, final String planHash, final String block,
                             final List<String> fields, final boolean fieldWeighted, final int k) {
        final String path = artifactPath(artifactUri, planHash, block);
        final Model model = new Model(fields, fieldWeighted, k);
        try (final DataFileReader<GenericRecord> reader = new DataFileReader<>(
                new SeekableByteArrayInput(ResourceUtil.readBytes(path)), new GenericDatumReader<>(null, SCHEMA))) {
            while (reader.hasNext()) {
                final GenericRecord record = reader.next();
                final String field = record.get("field").toString();
                final String value = record.get("value").toString();
                final double w = (Double) record.get("w");
                if ("__global".equals(field)) {
                    model.w0 = w;
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
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException("Failed to read factorization artifact: " + path, e);
        }
        LOG.info("loaded factorization artifact {} ({} parameters)", path, model.size());
        return model;
    }

}
