/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cairo.covering;

import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * The BEHAVIOURAL half of the order-sensitivity contract: it derives the verdict from RESULTS,
 * not from a list.
 * <p>
 * {@code io.questdb.test.griffin.engine.functions.groupby.GroupByFunctionOrderSensitivityTest}
 * checks every aggregate's {@code isOrderSensitive()} against two hardcoded lists. Three mutation
 * experiments measured what that catches: a new misclassified function with no list edit fails it,
 * and a wrong value against a correct list fails it -- but a wrong value TOGETHER WITH the matching
 * wrong list edit passes it silently, because the list is then self-consistent and nothing else
 * consults the truth. That is the hole this class closes. It runs each aggregate over a covering
 * index in per-key mode and compares it against the same query with {@code no_index}, so an
 * aggregate that claims order-invariance it does not have disagrees with its own full scan no
 * matter how tidily the lists were edited to match.
 * <p>
 * <b>Two things make or break it.</b> Each case asserts the plan really reached
 * {@code frames: per-key (unordered)} BEFORE comparing: a case that quietly fell back to the k-way
 * merge would compare the merge against itself and pass while proving nothing. And the baseline arm
 * is {@code /*+ no_index *}{@code /}, never {@code /*+ no_covering *}{@code /} -- the latter does
 * not disable the index, it routes to FilterOnValues plus a serial group by, a third execution
 * path, and has already produced one wrong baseline in this project.
 * <p>
 * <b>The queries are NOT keyed.</b> A group keyed on the index column is confined to one key, and
 * one key's frames arrive in ascending partition order either way, so key-major and timestamp
 * arrival coincide and the comparison would be vacuous. With no grouping column the single group
 * draws from BOTH matching keys, which is the only shape where the two orders can disagree.
 * <p>
 * <b>What it catches, measured by mutation.</b> Flipping
 * {@code ModeDoubleGroupByFunction.isOrderSensitive()} to false makes
 * {@link #testOrderSensitiveAggregatesNeverReachPerKey()} fail on the plan assertion, with no list
 * anywhere consulted. Flipping {@code FirstDoubleGroupByFunction} to false and moving
 * {@code first(value)} into the swept set makes
 * {@link #testOrderInsensitiveAggregatesAgreeWithFullScan()} fail on the VALUE:
 * {@code full scan [1.0], per-key [4.0], budget 4.0E-9}.
 * <p>
 * <b>What it does not catch.</b> The same mutation applied to mode() and swept BY VALUE passes:
 * over this fixture mode() returns the same winner either way. Its tie-break falls out of the
 * count map's SLOT order, and the map ends up with the same occupancy whichever order the same
 * values were inserted in unless probe collisions differ, which this data does not arrange. So a
 * mode() misclassification fails the plan arm and not the value arm, and a fixture built to make
 * the value arm catch it would be pinning a hash layout. Recorded so the gap is not rediscovered
 * as a finding.
 */
public class CoveringIndexAggregateAgreementTest extends AbstractCoveringIndexQueryTest {

    /**
     * Absolute floor under which two results count as agreeing whatever their ratio.
     * <p>
     * Needed because the Welford/Chan recurrences can produce a result that is zero only to
     * within their own rounding, and reassociating them moves it across zero. Measured on data
     * symmetric about its mean, where the true skewness is 0: skewness() returns
     * {@code -4.707887656719998E-17} merged and {@code 3.2079622631234974E-16} per-key -- a SIGN
     * FLIP, and a relative difference of 7.8. Both are zero to the precision the recurrence can
     * offer. The floor sits well above that spread and far below any value a genuinely
     * order-dependent aggregate over this fixture could produce, since every column here is
     * O(1) or larger.
     */
    private static final double ABSOLUTE_FLOOR = 1e-12;
    /**
     * Relative tolerance for results above {@link #ABSOLUTE_FLOOR}. Floating-point reassociation
     * over 5000 rows moves these in the last two or three significant digits -- var_pop() from
     * {@code 8333334.25} to {@code 8333334.24999999} was the widest measured -- which is ~1e-15
     * relative. A 1e-9 budget is six orders of magnitude of headroom for a longer accumulation
     * and still rejects any difference a row-selection or bucketing change would cause.
     */
    private static final double RELATIVE_TOLERANCE = 1e-9;

    /**
     * Aggregates whose {@code isOrderSensitive()} says false and which reach the per-key site.
     * <p>
     * A full sweep of every function in the insensitive list is not what this test is for: many
     * are the same accumulator over a different column type, and the runtime is spent re-proving
     * one shape. The set below picks representatives so that every distinct ACCUMULATOR SHAPE in
     * that list is exercised at least once, since the shape -- not the column type -- is what
     * decides whether reordering can change an answer:
     * <ul>
     *   <li><b>exact integer accumulation</b> -- count, sum/avg over LONG/INT. Two's-complement
     *       addition is associative even on wraparound, so these must agree EXACTLY.</li>
     *   <li><b>bitwise and boolean latches</b> -- bit_or/bit_and/bit_xor, bool_or/bool_and.
     *       Idempotent or involutive combines; exact agreement.</li>
     *   <li><b>extremum selection by value comparison</b> -- min/max over DOUBLE, LONG and
     *       STRING. These select a row, but on the VALUE, not on arrival position, which is the
     *       distinction the classification rule turns on; STRING is here because its comparison
     *       runs over a stored heap pointer rather than a register.</li>
     *   <li><b>floating-point running sum, and ratios of such sums</b> -- sum, avg, ksum, nsum,
     *       vwap, weighted_avg, geomean. Reassociation moves the last bits.</li>
     *   <li><b>Welford/Chan online recurrence</b> -- var_pop/samp, stddev_pop/samp, skewness,
     *       kurtosis, corr, covar_pop/samp, regr_slope/intercept/r2, weighted_stddev. Every
     *       member of this group is listed rather than sampled: they are the fourteen the
     *       classification javadoc was WRONG about (it admitted them as "a pure ratio of sums",
     *       which they are not), and they are the only group whose two arms differ at all, so
     *       they are what keeps the tolerance from being dead code.</li>
     *   <li><b>hash set and sketch accumulators</b> -- count_distinct, approx_count_distinct,
     *       approx_percentile over LONG. Insertion order changes internal layout; the reported
     *       answer must not move. approx_percentile over DOUBLE is deliberately absent: it IS
     *       order-sensitive (auto-resize re-buckets what is already recorded) and lives in the
     *       control set below.</li>
     *   <li><b>counter-only mode()</b> -- mode() over BOOLEAN, which keeps a {@code +1/-1}
     *       counter and no map. Its LONG/DOUBLE/STRING siblings keep an open-addressed map whose
     *       slot order is insertion-dependent and are in the control set.</li>
     *   <li><b>widening fixed-point decimal accumulation</b> -- sum/avg over DECIMAL64 and
     *       DECIMAL128, including the rescaling avg(). These promote to a wider accumulator on
     *       overflow rather than throwing, which is exactly why they stay order-insensitive
     *       while their DECIMAL256 siblings do not.</li>
     * </ul>
     * <p>
     * Three insensitive aggregates are absent because they do not reach the per-key site at all
     * and so cannot be compared here: {@code count()} with no argument is answered by the
     * vectorized path, {@code max(ts)} over the designated timestamp is answered from metadata,
     * and the three-argument {@code approx_percentile()} does not support parallelism. If a
     * future change routes any of them through per-key, add it -- the per-key assertion below is
     * what will notice.
     */
    private static final String[] ORDER_INSENSITIVE_AGGREGATES = {
            // exact integer accumulation
            "count(value)", "sum(l)", "sum(i)", "avg(l)",
            // bitwise and boolean latches
            "bit_or(i)", "bit_and(i)", "bit_xor(i)", "bool_or(b)", "bool_and(b)",
            // extremum selection by value comparison
            "min(value)", "max(value)", "min(l)", "max(l)", "min(s)", "max(s)",
            // floating-point running sum, and ratios of such sums
            "sum(value)", "avg(value)", "ksum(value)", "nsum(value)",
            "vwap(value, w)", "weighted_avg(value, w)", "geomean(w)",
            // Welford/Chan online recurrence
            "var_pop(value)", "var_samp(value)", "stddev_pop(value)", "stddev_samp(value)",
            "skewness(value)", "kurtosis(value)",
            "corr(value, w)", "covar_pop(value, w)", "covar_samp(value, w)",
            "regr_slope(value, w)", "regr_intercept(value, w)", "regr_r2(value, w)",
            "weighted_stddev(value, w)",
            // hash set and sketch accumulators
            "count_distinct(s)", "approx_count_distinct(l)", "approx_percentile(l, 0.5)",
            // counter-only mode()
            "mode(b)",
            // widening fixed-point decimal accumulation
            "sum(d64)", "sum(d128)", "avg(d64)", "avg(d128)", "avg(d128, 2)",
            // decimal accumulators that compare rather than combine, so the width is irrelevant
            "min(d256)", "max(d256)", "count(d256)",
    };

    /**
     * Aggregates whose {@code isOrderSensitive()} says true. The scan must DECLINE the ordering
     * opt-out for every one of them, so none may reach per-key -- and they must still answer what
     * a full scan answers.
     * <p>
     * This is the arm that catches the mutation the reflective test cannot see. Flipping, say,
     * {@code ModeDoubleGroupByFunction} to false and moving its entry to the insensitive list
     * leaves that test self-consistent and green; here {@code mode(value)} starts reaching
     * per-key and this case fails on the plan assertion before it even compares a value.
     * <p>
     * {@code sum(d256)}/{@code avg(d256)} are the decimal regression: their FIXED 256-bit
     * accumulator throws on a partial sum that another order would not produce, so they must not
     * be reordered. The value they can be made to disagree on is in
     * {@link CoveringIndexOrderSensitiveTest#testSumAvgOverDecimal256KeepTheMerge()}, which
     * builds a fixture that actually overflows; this fixture only pins that they stay off the
     * per-key path.
     */
    private static final String[] ORDER_SENSITIVE_AGGREGATES = {
            "first(value)", "last(value)", "first_not_null(value)", "last_not_null(value)",
            "mode(value)", "mode(l)", "mode(s)",
            "array_agg(value)", "string_agg(s, ',')",
            "arg_min(value, l)", "arg_max(value, l)",
            "approx_percentile(value, 0.5)",
            "sum(d256)", "avg(d256)",
    };

    @Test
    public void testOrderInsensitiveAggregatesAgreeWithFullScan() throws Exception {
        assertMemoryLeak(() -> {
            createMixedTypeTelemetry();
            final List<String> moved = new ArrayList<>();
            for (String agg : ORDER_INSENSITIVE_AGGREGATES) {
                final String indexed = select(agg, false);
                // Without this the comparison below is worthless: an aggregate that fell back to
                // the k-way merge would be compared against a merge and agree trivially.
                assertQuery(indexed).noLeakCheck().assertsPlanContaining("frames: per-key (unordered)");
                final StringSink perKey = new StringSink();
                final StringSink fullScan = new StringSink();
                printSql(indexed, perKey);
                printSql(select(agg, true), fullScan);
                assertAgreesWithinRounding(agg, fullScan, perKey);
                if (!perKey.toString().equals(fullScan.toString())) {
                    moved.add(agg);
                }
            }
            // Non-vacuity guard for the TOLERANCE. If every case were bit-identical, the
            // comparison would collapse into a string equality that proves nothing about the
            // rounding argument the insensitive classification actually rests on, and a future
            // fixture change that flattened the data would hide that.
            Assert.assertFalse(
                    "no aggregate's two arms differed at all, so the rounding tolerance was never"
                            + " exercised -- the fixture no longer discriminates between key-major"
                            + " and timestamp arrival",
                    moved.isEmpty()
            );
        });
    }

    @Test
    public void testOrderSensitiveAggregatesNeverReachPerKey() throws Exception {
        assertMemoryLeak(() -> {
            createMixedTypeTelemetry();
            for (String agg : ORDER_SENSITIVE_AGGREGATES) {
                final String indexed = select(agg, false);
                assertQuery(indexed).noLeakCheck().assertsPlanContaining("CoveringIndex on: param_id");
                assertQuery(indexed).noLeakCheck().assertsPlanNotContaining("frames: per-key");
                assertSameResult(indexed, select(agg, true));
            }
        });
    }

    /**
     * Cell-by-cell comparison that admits a difference only inside
     * {@code max(ABSOLUTE_FLOOR, RELATIVE_TOLERANCE * max(|a|, |b|))}, and only for cells both
     * arms render as a number. Anything else -- a string, a boolean, a decimal, a NULL, a
     * different row count, a different header -- must match exactly, because for those a
     * difference cannot be rounding.
     */
    private static void assertAgreesWithinRounding(String agg, StringSink fullScan, StringSink perKey) {
        final String[] expected = fullScan.toString().split("\n", -1);
        final String[] actual = perKey.toString().split("\n", -1);
        Assert.assertEquals(agg + ": row count differs\n" + fullScan + "\nvs\n" + perKey,
                expected.length, actual.length);
        for (int row = 0; row < expected.length; row++) {
            final String[] expectedCells = expected[row].split("\t", -1);
            final String[] actualCells = actual[row].split("\t", -1);
            Assert.assertEquals(agg + ": column count differs on row " + row,
                    expectedCells.length, actualCells.length);
            for (int col = 0; col < expectedCells.length; col++) {
                final String e = expectedCells[col];
                final String a = actualCells[col];
                if (e.equals(a)) {
                    continue;
                }
                // Row 0 is the header, so it is never a number and never gets the tolerance.
                final double ev = row == 0 ? Double.NaN : parseOrNaN(e);
                final double av = row == 0 ? Double.NaN : parseOrNaN(a);
                if (Double.isNaN(ev) || Double.isNaN(av)) {
                    Assert.fail(agg + ": per-key and full scan disagree at row " + row + " column "
                            + col + ", and the values are not both numeric so the difference"
                            + " cannot be rounding -- full scan [" + e + "], per-key [" + a + ']');
                }
                final double budget = Math.max(
                        ABSOLUTE_FLOOR,
                        RELATIVE_TOLERANCE * Math.max(Math.abs(ev), Math.abs(av))
                );
                Assert.assertTrue(
                        agg + ": per-key and full scan disagree by more than rounding at row " + row
                                + " column " + col + " -- full scan [" + e + "], per-key [" + a
                                + "], budget " + budget,
                        Math.abs(ev - av) <= budget
                );
            }
        }
    }

    /**
     * The cell as a double, or NaN when it is not a number. NaN is safe as the "not a number"
     * marker here because the caller treats it as "compare exactly", and two cells that both
     * render NaN are already equal as strings by then.
     */
    private static double parseOrNaN(String cell) {
        try {
            return Double.parseDouble(cell);
        } catch (NumberFormatException e) {
            return Double.NaN;
        }
    }

    private static String select(String agg, boolean fullScan) {
        return "SELECT " + (fullScan ? "/*+ no_index */ " : "") + agg
                + " FROM agg_tel WHERE param_id IN ('SFID','HOTMIC')";
    }

    /**
     * One table carrying a column per accumulator shape, all of them covered by the POSTING index
     * so that every aggregate below reaches the covering scan rather than falling out of it on a
     * missing column.
     * <p>
     * 10 000 rows over four keys, two of them selected, is 2500 rows per (key, partition) pair --
     * comfortably above the ~32 rows per pair below which per-key mode declines on density, so the
     * per-key assertions are not fighting the crossover heuristic. Every fifth {@code value} is
     * NULL so the aggregates' null handling is on the path too. {@code w} is a small positive
     * weight, kept away from zero because vwap()/weighted_avg()/weighted_stddev() divide by its
     * sum, and correlated with nothing so corr() and regr_* have a non-degenerate answer.
     */
    private void createMixedTypeTelemetry() throws Exception {
        execute("CREATE TABLE agg_tel (" +
                "  ts TIMESTAMP," +
                "  param_id SYMBOL INDEX TYPE POSTING INCLUDE (value, w, l, i, b, s, d64, d128, d256)," +
                "  value DOUBLE," +
                "  w DOUBLE," +
                "  l LONG," +
                "  i INT," +
                "  b BOOLEAN," +
                "  s STRING," +
                "  d64 DECIMAL(18,4)," +
                "  d128 DECIMAL(38,10)," +
                "  d256 DECIMAL(40,5)" +
                ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO agg_tel SELECT" +
                " (x * 1_000_000L)::timestamp," +
                " CASE WHEN x % 4 = 0 THEN 'SFID' WHEN x % 4 = 1 THEN 'HOTMIC'" +
                "      WHEN x % 4 = 2 THEN 'KCAS' ELSE 'CALT' END," +
                " CASE WHEN x % 5 = 0 THEN NULL ELSE x::double END," +
                " (1 + x % 7)::double," +
                " x," +
                " (x % 1000)::int," +
                " x % 3 = 0," +
                " 'v' || (x % 97)," +
                " (x % 100_000)::DECIMAL(18,4)," +
                " (x % 100_000)::DECIMAL(38,10)," +
                " (x % 100_000)::DECIMAL(40,5)" +
                " FROM long_sequence(10_000)");
    }
}
