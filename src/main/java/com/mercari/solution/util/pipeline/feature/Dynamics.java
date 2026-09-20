package com.mercari.solution.util.pipeline.feature;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * The {@code lti} family of the Summarize stage (docs/design/feature-dsl.md §1.4, §4.3 general form): a linear state
 * driven by the events of a path, read out as a fixed-length vector of components. Each event is an impulse
 * {@code x} at a clock position; component {@code j} of the state is {@code Σ x_i · K_j(age_i)}, the projection of
 * the path onto a basis under a measure, and a readout divides it by the measure's mass over the present values
 * (so every component is a weighted mean and order 0 of the exponential measure is exactly {@code ewma}):
 *
 * <ul>
 *   <li>{@code exponential} — measure {@code e^(−θ·age)}, θ = ln 2 / halflife; basis the Laguerre polynomials
 *       {@code L_j(θ·age)} (HiPPO-LagT). Time-invariant: the state at a later position is {@code Φ(Δ) s} with
 *       {@code Φ(Δ) = e^(−θΔ) T(θΔ)}, {@code T} the lower-triangular Toeplitz matrix of
 *       {@code φ_m = L_m − L_{m−1}} ({@code e^(−uN)} for N the strictly-lower all-ones matrix), exact for any
 *       spacing. A group: an evicted event is subtracted as {@code x · K(age)}.</li>
 *   <li>{@code fourier} — basis {@code cos(kω·age)}, {@code sin(kω·age)} for k = 1..order plus the constant, ω = 2π /
 *       period, under an optional exponential measure (a halflife damps it; without one the measure is uniform
 *       over the window). Propagation is a rotation per harmonic (and the decay): a group.</li>
 *   <li>{@code legendre} — the shifted Legendre polynomials {@code P_j(2u − 1)} over the window's own span, u =
 *       (position − first position) / (now − first position): the scaled measure of HiPPO-LegS, uniform over the
 *       events the window holds. The state is the power sums {@code Σ x · s^k} about the first event, read out by
 *       the polynomial coefficients; the span grows with the window, so an eviction would re-scale every
 *       contribution — the family is a monoid, and a {@code maxAge} window re-reads (the scan path).</li>
 * </ul>
 *
 * <p>The clock is {@code time} (positions in days from the event millis) or {@code events} (positions are event
 * ordinals, the newest event at age 0 — as {@code ewma} counts). On the time clock the fourier and legendre readouts
 * are taken at the current row's time (a phase / a span measured from now, both bounded); the exponential readout is
 * taken at the newest event, like the events clock: its weighted mean of {@code L_j(θ·age)} would grow as
 * {@code (θ·gap)^j / j!} with the gap since the entity's last event, so the gap is left to its own feature
 * ({@code sinceEvent}) and order 0 — {@code ewma} — is unchanged (it does not depend on the readout position). A row
 * with a missing value is still an event: on the events clock it advances the position; it contributes no mass.
 *
 * <p>No periodic re-fold: {@code Φ} is contractive (exponential) or a rotation (fourier), so the rounding an
 * eviction leaves decays with the state or stays at the scale of the values folded in, and a state whose window
 * empties resets exactly. {@link #project} is the direct projection of a window — the scan path, and the reference
 * the incremental state is tested against.
 */
public final class Dynamics implements Summary<Dynamics.State> {

    public enum Measure { exponential, legendre, fourier }

    /** Largest order per measure: the Legendre readout from power sums loses digits like its coefficients grow. */
    public static int maxOrder(final Measure measure) {
        return switch (measure) {
            case exponential, fourier -> 16;
            case legendre -> 8;
        };
    }

    static final double DAY_MILLIS = 86_400_000d;

    /** One event of the path: its time and its value ({@code NaN} = missing — still an event, no mass). */
    public record Event(long millis, double value) implements Serializable {}

    public static final class State implements Serializable {
        /** Events summarised, missing values included (the events clock). */
        long rows;
        /** Time of the newest event (the time clock's state position). */
        long newest;
        /** legendre: time of the first event (the origin of the power sums). */
        long origin;
        /** Present values summarised. */
        double n;
        /** exponential / fourier: the measure's mass over the present values at the newest event. */
        double den;
        final double[] s;

        State(final int dimension) {
            this.s = new double[dimension];
        }
    }

    private final Measure measure;
    private final int order;
    /** The clock is positional (time or a calendar), not the event ordinal. */
    private final boolean byTime;
    /** A calendar clock ({@code decayBy: <calendar>}): ages in ticks instead of days; null on wall time. */
    private final Clock calendar;
    /** Decay per clock unit (ln 2 / halflife), 0 without a halflife. */
    private final double theta;
    /** fourier: the base angular frequency 2π / period. */
    private final double omega;
    /** legendre: {@code coefficients[j][k]} of u^k in P_j(2u − 1). */
    private final double[][] coefficients;
    /**
     * The propagator of one clock unit (every fold on the events clock), computed once: its decay, and the Laguerre
     * values {@code L_0..L_order(θ)} (exponential) or {@code cos / sin(hω)} per harmonic h (fourier, index h).
     */
    private final double unitDecay;
    private final double[] unitLaguerre;
    private final double[] unitCos;
    private final double[] unitSin;

    public Dynamics(final Measure measure, final int order, final Double halflife, final Double period, final boolean byTime) {
        this(measure, order, halflife, period, byTime, null);
    }

    /** @param calendar the calendar clock ages are counted on (ticks), or null for days of wall time / the event ordinal */
    public Dynamics(final Measure measure, final int order, final Double halflife, final Double period, final boolean byTime, final Clock calendar) {
        this.calendar = calendar;
        this.measure = measure;
        this.order = order;
        this.byTime = byTime;
        this.theta = halflife == null ? 0d : Math.log(2) / halflife;
        this.omega = period == null ? 0d : 2 * Math.PI / period;
        this.coefficients = measure == Measure.legendre ? shiftedLegendre(order) : null;
        this.unitDecay = Math.exp(-theta);
        this.unitLaguerre = measure == Measure.exponential ? laguerre(theta, order) : null;
        this.unitCos = measure == Measure.fourier ? new double[order + 1] : null;
        this.unitSin = measure == Measure.fourier ? new double[order + 1] : null;
        if (measure == Measure.fourier) {
            for (int h = 1; h <= order; h++) {
                unitCos[h] = Math.cos(h * omega);
                unitSin[h] = Math.sin(h * omega);
            }
        }
    }

    /**
     * The family and readout of a column from its coordinates: {@code measure} (default exponential — the
     * {@code ewma} sugar carries only halflife / decayBy), {@code order}, {@code halflife}, {@code period},
     * {@code decayBy} (events | time | a calendar clock attached to the column, default events) and {@code component}.
     */
    public static Summary.Spec spec(final Map<String, String> coordinates, final Map<String, Clock> clocks) {
        final String decayBy = coordinates.get("decayBy");
        final Clock calendar = decayBy == null || Clock.BUILT_IN.contains(decayBy) ? null : clocks.get(decayBy);
        if (decayBy != null && !Clock.BUILT_IN.contains(decayBy) && calendar == null) {
            throw new IllegalStateException("the calendar clock '" + decayBy + "' of decayBy is not attached to the column");
        }
        final Measure measure = Measure.valueOf(coordinates.getOrDefault("measure", Measure.exponential.name()));
        final int order = Integer.parseInt(coordinates.getOrDefault("order", "0"));
        final String halflife = coordinates.get("halflife"), period = coordinates.get("period");
        final Dynamics family = new Dynamics(measure, order,
                halflife == null ? null : Double.valueOf(halflife), period == null ? null : Double.valueOf(period),
                "time".equals(decayBy) || calendar != null, calendar);
        return new Summary.Spec(family, Summary.Readout.of("component", Integer.parseInt(coordinates.getOrDefault("component", "0"))));
    }

    /** Clock units between two instants: days of wall time, or ticks of the calendar. */
    private double distance(final long later, final long earlier) {
        return calendar != null ? calendar.distance(later, earlier) : (later - earlier) / DAY_MILLIS;
    }

    /** Number of components of a channel: order + 1, or for fourier the constant plus a cosine and a sine per harmonic. */
    public static int dimension(final Measure measure, final int order) {
        return measure == Measure.fourier ? 2 * order + 1 : order + 1;
    }

    /** The name suffix of a component: {@code 0..order}, or for fourier {@code c0}, {@code c<k>} / {@code s<k>}. */
    public static String componentName(final Measure measure, final int component) {
        if (measure != Measure.fourier) return Integer.toString(component);
        if (component == 0) return "c0";
        final int k = (component + 1) / 2;
        return (component % 2 == 1 ? "c" : "s") + k;
    }

    public Event event(final long millis, final Double value) {
        return new Event(millis, value == null ? Double.NaN : value);
    }

    /** The event a past row contributes to a channel ({@code field} null = the constant channel 1, time augmentation). */
    public Event event(final SequenceEvaluator.Past p, final String field) {
        return event(p.millis(), value(p, field));
    }

    @Override
    public State create() {
        return new State(dimension(measure, order));
    }

    @Override
    public boolean invertible() {
        return measure != Measure.legendre;
    }

    @Override
    public void update(final State st, final Object contribution, final int sign) {
        final Event e = (Event) contribution;
        if (sign > 0) add(st, e);
        else remove(st, e);
    }

    private void add(final State st, final Event e) {
        final boolean present = !Double.isNaN(e.value());
        if (measure == Measure.legendre) {
            if (st.rows == 0) st.origin = e.millis();
            if (present) {
                final double position = byTime ? distance(e.millis(), st.origin) : st.rows;
                double power = 1;
                for (int k = 0; k <= order; k++) {
                    st.s[k] += e.value() * power;
                    power *= position;
                }
                st.n++;
            }
        } else {
            if (st.rows > 0) propagate(st, byTime ? distance(e.millis(), st.newest) : 1);
            if (present) {
                // K(0): every Laguerre polynomial and every cosine is 1 at age 0, every sine 0
                if (measure == Measure.exponential) {
                    for (int j = 0; j <= order; j++) st.s[j] += e.value();
                } else {
                    st.s[0] += e.value();
                    for (int k = 1; k <= order; k++) st.s[2 * k - 1] += e.value();
                }
                st.den += 1;
                st.n++;
            }
        }
        st.rows++;
        st.newest = e.millis();
    }

    /** Evicts the oldest event of the state (the keyed replay evicts in time order). */
    private void remove(final State st, final Event e) {
        if (measure == Measure.legendre) throw new UnsupportedOperationException("a legendre state is not invertible");
        if (!Double.isNaN(e.value())) {
            final double age = byTime ? distance(st.newest, e.millis()) : st.rows - 1;
            final double w = Math.exp(-theta * age);
            if (w > 0) {
                final double[] k = kernel(age, w);
                for (int j = 0; j < k.length; j++) st.s[j] -= e.value() * k[j];
                st.den -= w;
            }
            st.n--;
        }
        st.rows--;
        // an emptied window starts over: no rounding residue left by the evictions
        if (st.n == 0) {
            java.util.Arrays.fill(st.s, 0);
            st.den = 0;
        }
    }

    /** {@code K(age)} with its weight {@code w = e^(−θ·age)}: the state an event of value 1 leaves at that age. */
    private double[] kernel(final double age, final double w) {
        final double[] k = new double[dimension(measure, order)];
        if (measure == Measure.exponential) {
            final double[] l = laguerre(theta * age, order);
            for (int j = 0; j <= order; j++) k[j] = w * l[j];
        } else {
            k[0] = w;
            for (int h = 1; h <= order; h++) {
                k[2 * h - 1] = w * Math.cos(h * omega * age);
                k[2 * h] = w * Math.sin(h * omega * age);
            }
        }
        return k;
    }

    /**
     * Moves an exponential / fourier state {@code delta} clock units forward in place: {@code s ← Φ(Δ) s}, the mass
     * decays alike. One clock unit (every fold on the events clock) reuses the precomputed propagator.
     */
    private void propagate(final State st, final double delta) {
        if (delta == 0) return;
        final boolean unit = delta == 1;
        final double decay = unit ? unitDecay : Math.exp(-theta * delta);
        if (decay == 0) {
            java.util.Arrays.fill(st.s, 0);
            st.den = 0;
            return;
        }
        if (measure == Measure.exponential) {
            final double[] l = unit ? unitLaguerre : laguerre(theta * delta, order);
            // T(u) is lower-triangular: from the top down, component j only reads the not yet moved s_{j−m}
            for (int j = order; j >= 0; j--) st.s[j] = decay * shifted(st.s, l, j);
        } else {
            for (int h = 1; h <= order; h++) {
                final double angle = h * omega * delta;
                final double cos = unit ? unitCos[h] : Math.cos(angle), sin = unit ? unitSin[h] : Math.sin(angle);
                final double c = st.s[2 * h - 1], d = st.s[2 * h];
                st.s[2 * h - 1] = c * cos - d * sin;
                st.s[2 * h] = d * cos + c * sin;
            }
            for (int j = 0; j < st.s.length; j++) st.s[j] *= decay;
        }
        st.den *= decay;
    }

    /** Component j of {@code T(u) s} = Σ_{m ≤ j} φ_m(u) s_{j−m}, φ_m = L_m − L_{m−1} (φ_0 = 1), from {@code l} = L_0..L_j(u). */
    private static double shifted(final double[] s, final double[] l, final int j) {
        double v = s[j];
        for (int m = 1; m <= j; m++) v += (l[m] - l[m - 1]) * s[j - m];
        return v;
    }

    /**
     * {@code into ← into ⊕ other}, {@code other} holding the events after {@code into}'s (the time order of blocks
     * and partitions; on the time clock any order is accepted — the older state is moved to the newer's position).
     */
    @Override
    public void merge(final State into, final State other) {
        if (other.rows == 0) return;
        if (into.rows == 0) {
            copy(other, into);
            return;
        }
        if (measure == Measure.legendre) {
            if (byTime) {
                // re-express both sets of power sums about the earlier origin
                if (other.origin >= into.origin) {
                    addShifted(into.s, other.s, distance(other.origin, into.origin));
                } else {
                    final double[] mine = into.s.clone();
                    java.util.Arrays.fill(into.s, 0);
                    addShifted(into.s, mine, distance(into.origin, other.origin));
                    for (int k = 0; k <= order; k++) into.s[k] += other.s[k];
                    into.origin = other.origin;
                }
            } else {
                addShifted(into.s, other.s, into.rows);
            }
        } else if (!byTime) {
            propagate(into, other.rows);
            for (int j = 0; j < into.s.length; j++) into.s[j] += other.s[j];
            into.den += other.den;
        } else if (other.newest >= into.newest) {
            propagate(into, distance(other.newest, into.newest));
            for (int j = 0; j < into.s.length; j++) into.s[j] += other.s[j];
            into.den += other.den;
        } else {
            final State moved = create();
            copy(other, moved);
            propagate(moved, distance(into.newest, other.newest));
            for (int j = 0; j < into.s.length; j++) into.s[j] += moved.s[j];
            into.den += moved.den;
        }
        into.rows += other.rows;
        into.n += other.n;
        into.newest = Math.max(into.newest, other.newest);
    }

    /** {@code into_k += Σ_i C(k, i) d^(k−i) other_i}: power sums about an origin {@code d} later, moved onto this origin. */
    private void addShifted(final double[] into, final double[] other, final double d) {
        for (int k = 0; k <= order; k++) {
            double sum = 0, binomial = 1, power = 1;
            // i runs down from k: C(k, i) d^(k − i)
            for (int i = k; i >= 0; i--) {
                sum += binomial * power * other[i];
                binomial = binomial * i / (k - i + 1);
                power *= d;
            }
            into[k] += sum;
        }
    }

    private static void copy(final State from, final State to) {
        to.rows = from.rows;
        to.newest = from.newest;
        to.origin = from.origin;
        to.n = from.n;
        to.den = from.den;
        System.arraycopy(from.s, 0, to.s, 0, from.s.length);
    }

    @Override
    public double count(final State st) {
        return st.n;
    }

    /** The component at the state's own position (the newest event). */
    @Override
    public Object read(final State st, final Readout readout) {
        return readAt(st, readout, st.newest);
    }

    /**
     * Component {@code readout.parameter()} at clock time {@code nowMillis} (the time clock reads the state moved to
     * that time; the events clock at the newest event), divided by the measure's mass; null without a present value.
     */
    @Override
    public Object readAt(final State st, final Readout readout, final long nowMillis) {
        if (st.n <= 0) return null;
        final int j = readout.parameter().intValue();
        final double value;
        switch (measure) {
            case legendre -> {
                final double span = byTime ? distance(nowMillis, st.origin) : st.rows - 1;
                if (!(span > 0)) {
                    // one position only: every event sits at u = 1, where every P_j is 1
                    value = st.s[0] / st.n;
                } else {
                    double sum = 0, scale = 1;
                    for (int k = 0; k <= j; k++) {
                        sum += coefficients[j][k] * st.s[k] * scale;
                        scale /= span;
                    }
                    value = sum / st.n;
                }
            }
            // read at the newest event on either clock (the class comment: bounded in the gap since it)
            case exponential -> {
                if (!(st.den > 0)) return null;
                value = st.s[j] / st.den;
            }
            default -> {
                if (!(st.den > 0)) return null;
                final double delta = byTime ? Math.max(0, distance(nowMillis, st.newest)) : 0;
                if (j == 0 || delta == 0) {
                    value = st.s[j] / st.den;
                } else {
                    final int h = (j + 1) / 2;
                    final double angle = h * omega * delta, cos = Math.cos(angle), sin = Math.sin(angle);
                    final double c = st.s[2 * h - 1], d = st.s[2 * h];
                    value = (j % 2 == 1 ? c * cos - d * sin : d * cos + c * sin) / st.den;
                }
            }
        }
        return Double.isFinite(value) ? value : null;
    }

    /**
     * The direct projection of a window (the scan path): each event's kernel at its age from the readout position,
     * weighted relative to the newest event (the weight's common factor cancels in the ratio, and the newest event
     * never underflows); a {@code null} field is the constant channel 1 (time augmentation).
     */
    public Object project(final List<SequenceEvaluator.Past> window, final String field, final long nowMillis, final int component) {
        if (window.isEmpty()) return null;
        final int size = window.size();
        double num = 0, mass = 0;
        if (measure == Measure.legendre) {
            final long origin = window.get(0).millis();
            final double span = byTime ? distance(nowMillis, origin) : size - 1;
            for (int i = 0; i < size; i++) {
                final Double x = value(window.get(i), field);
                if (x == null) continue;
                final double u = span > 0 ? (byTime ? distance(window.get(i).millis(), origin) : i) / span : 1;
                num += x * legendre(2 * u - 1, component);
                mass++;
            }
        } else {
            final long newest = window.get(size - 1).millis();
            // the exponential readout is taken at the newest event (the class comment), fourier's at now
            final double delta = byTime && measure == Measure.fourier ? Math.max(0, distance(nowMillis, newest)) : 0;
            for (int i = 0; i < size; i++) {
                final Double x = value(window.get(i), field);
                if (x == null) continue;
                final double age = byTime ? distance(newest, window.get(i).millis()) : size - 1 - i;
                final double w = Math.exp(-theta * age);
                if (w == 0) continue;
                mass += w;
                num += w * x * basis(age + delta, component);
            }
        }
        if (!(mass > 0)) return null;
        final double value = num / mass;
        return Double.isFinite(value) ? value : null;
    }

    /** The unweighted basis function of a component at an age: L_j(θ·age), or the constant / cos / sin of fourier. */
    private double basis(final double age, final int component) {
        if (measure == Measure.exponential) return laguerreAt(theta * age, component);
        if (component == 0) return 1;
        final int h = (component + 1) / 2;
        return component % 2 == 1 ? Math.cos(h * omega * age) : Math.sin(h * omega * age);
    }

    private static Double value(final SequenceEvaluator.Past p, final String field) {
        return field == null ? Double.valueOf(1d) : SequenceEvaluator.finite(p.values().get(field));
    }

    /** L_0..L_order at u (the three-term recurrence). */
    static double[] laguerre(final double u, final int order) {
        final double[] l = new double[order + 1];
        l[0] = 1;
        if (order >= 1) l[1] = 1 - u;
        for (int j = 1; j < order; j++) l[j + 1] = ((2 * j + 1 - u) * l[j] - j * l[j - 1]) / (j + 1);
        return l;
    }

    /** L_j(u) alone (the same recurrence, no array: the scan path evaluates it per event). */
    static double laguerreAt(final double u, final int j) {
        if (j == 0) return 1;
        double previous = 1, current = 1 - u;
        for (int k = 1; k < j; k++) {
            final double next = ((2 * k + 1 - u) * current - k * previous) / (k + 1);
            previous = current;
            current = next;
        }
        return current;
    }

    /** P_j(y) (Bonnet's recurrence). */
    static double legendre(final double y, final int j) {
        if (j == 0) return 1;
        double previous = 1, current = y;
        for (int k = 1; k < j; k++) {
            final double next = ((2 * k + 1) * y * current - k * previous) / (k + 1);
            previous = current;
            current = next;
        }
        return current;
    }

    /** Coefficients of u^k in P_j(2u − 1): (−1)^(j+k) C(j, k) C(j + k, k). */
    static double[][] shiftedLegendre(final int order) {
        final double[][] c = new double[order + 1][];
        for (int j = 0; j <= order; j++) {
            c[j] = new double[j + 1];
            for (int k = 0; k <= j; k++) {
                c[j][k] = ((j + k) % 2 == 0 ? 1 : -1) * binomial(j, k) * binomial(j + k, k);
            }
        }
        return c;
    }

    private static double binomial(final int n, final int k) {
        double b = 1;
        for (int i = 1; i <= k; i++) b = b * (n - k + i) / i;
        return b;
    }
}
