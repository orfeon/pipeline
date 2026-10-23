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
     * One training example: the field values (null = missing), the target, the offset its score starts from (0 without
     * one; on the score scale: the target's own units under squared loss, log-odds under logistic, log-strength under
     * Plackett–Luce), the tag the fit plan partitions by — the row's fold under {@code fit.mode: fold}, its time block
     * under {@code forward}, 0 otherwise — and the contest it is ranked in (Plackett–Luce; null otherwise).
     */
    public record Example(String[] values, double y, double offset, long tag, String contest) implements Serializable {
        public Example(final String[] values, final double y) {
            this(values, y, 0d, 0L, null);
        }

        public Example(final String[] values, final double y, final long tag) {
            this(values, y, 0d, tag, null);
        }
    }

    /** The canonical example order: tag, contest, then the values (null first), the target and the offset. */
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
            rotated.loss = loss;
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
     */
    public record Options(List<String> fields, boolean fieldWeighted, int latentDim, int epochs, double reg, long seed,
                          Loss loss, int top, boolean descending, boolean linear) implements Serializable {
        public Options(final List<String> fields, final boolean fieldWeighted, final int latentDim, final int epochs, final double reg, final long seed) {
            this(fields, fieldWeighted, latentDim, epochs, reg, seed, Loss.squared, 0, false, true);
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
        return train(options, sorted, null, options.epochs());
    }

    /** {@link #fit}, or null over fewer than {@code minRows} examples (none at all included). */
    public static Model fit(final Options options, final List<Example> examples, final int minRows) {
        final List<Example> sorted = new ArrayList<>(examples);
        sorted.sort(ORDER);
        return trainOrNull(options, sorted, null, options.epochs(), minRows);
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
                    if (!options.linear()) p[0] = 0;
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
            s[i] = examples.get(i).offset() + y;
            if (fm) {
                for (int f = 0; f < F; f++) {
                    final int id = idx[i * F + f];
                    if (id >= 0) for (int d = 0; d < k; d++) q[i * k + d] += v[f][id * k + d];
                }
            }
        }
        final Objective objective = Objective.of(options, examples, s);
        final double reg = options.reg();
        int longest = 0;
        for (int f = 0; f < F; f++) for (int id = 0; id + 1 < start[f].length; id++) longest = Math.max(longest, start[f][id + 1] - start[f][id]);
        final double[] h = new double[Math.max(longest, n)];
        final int[] all = new int[n];
        for (int i = 0; i < n; i++) all[i] = i;

        for (int epoch = 0; epoch < epochs; epoch++) {
            objective.refresh();
            // global bias
            if (!contest) {
                Arrays.fill(h, 0, n, 1d);
                final double delta = objective.delta(all, 0, n, h, w0, reg);
                objective.shift(all, 0, n, h, delta);
                w0 += delta;
            }
            // linear weights
            if (options.linear()) {
                for (int f = 0; f < F; f++) {
                    for (int id = 0; id + 1 < start[f].length; id++) {
                        final int from = start[f][id], to = start[f][id + 1];
                        Arrays.fill(h, 0, to - from, 1d);
                        final double delta = objective.delta(members[f], from, to, h, w[f][id], reg);
                        objective.shift(members[f], from, to, h, delta);
                        w[f][id] += delta;
                    }
                }
            }
            // factors: ŷ is linear in v_f[x][d] with slope h = Σ_{g≠f} r_fg v_g[x_g][d]
            for (int f = 0; f < F; f++) {
                for (int id = 0; id + 1 < start[f].length; id++) {
                    final int from = start[f][id], to = start[f][id + 1];
                    for (int d = 0; d < k; d++) {
                        final double p = v[f][id * k + d];
                        for (int j = from; j < to; j++) {
                            final int i = members[f][j];
                            h[j - from] = fm ? q[i * k + d] - p : slope(r, v, idx, i, F, f, d, k);
                        }
                        final double delta = objective.delta(members[f], from, to, h, p, reg);
                        objective.shift(members[f], from, to, h, delta);
                        if (fm) for (int j = from; j < to; j++) q[members[f][j] * k + d] += delta;
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
                            h[i] = dot;
                        }
                        final double delta = objective.delta(all, 0, n, h, r[f][g], reg);
                        objective.shift(all, 0, n, h, delta);
                        r[f][g] += delta;
                        r[g][f] = r[f][g];
                    }
                }
            }
            if (LOG.isDebugEnabled()) LOG.debug("factorization epoch {}: {}", epoch + 1, objective.describe());
        }
        objective.refresh();
        LOG.info("factorization fit: {} example(s), {} epoch(s){}, {}", n, epochs, warm == null ? "" : " (warm start)", objective.describe());

        final Model model = new Model(options.fields(), options.fieldWeighted(), k);
        model.w0 = w0;
        model.loss = options.loss().name();
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
        abstract String describe();

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
        String describe() {
            return "rmse=" + Math.sqrt(-2 * logLikelihood() / Math.max(1, y.length));
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
        String describe() {
            return "logloss=" + -logLikelihood() / Math.max(1, y.length);
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
     * (Breslow), which keeps the likelihood free of the entry order. A contest of fewer than two entries carries no
     * information and is left out, as is an example without a contest.
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
                final int c = kept.size();
                final double[] ys = new double[rows.size()];
                for (int j = 0; j < ys.length; j++) ys[j] = examples.get(rows.get(j)).y();
                final int[] place = new int[ys.length];
                for (int j = 0; j < ys.length; j++) {
                    int better = 0;
                    for (final double other : ys) if (descending ? other > ys[j] : other < ys[j]) better++;
                    place[j] = 1 + better;
                }
                final TreeSet<Integer> places = new TreeSet<>();
                for (final int p : place) if (top <= 0 || p <= top) places.add(p);
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
            int choices = 0;
            for (final int[] rows : rowsOf) for (final int i : rows) if (chosen[i]) choices++;
            return "contests=" + rowsOf.length + " nll/choice=" + (choices == 0 ? 0 : -logLikelihood() / choices);
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
            manifest.addProperty("loss", model.loss);
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
