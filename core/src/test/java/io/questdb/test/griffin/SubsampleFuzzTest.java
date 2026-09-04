/*+*****************************************************************************
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

package io.questdb.test.griffin;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;

/**
 * Randomized differential and invariant fuzz coverage for the SUBSAMPLE methods that
 * {@link SubsampleTest} only covers with hand-built fixtures: {@code m4}, {@code minmax},
 * {@code uniform}, {@code cadence} and {@code sdt}. ({@code lttb} already has three randomized
 * tests in SubsampleTest; this file deliberately does not duplicate them.)
 * <p>
 * <b>Oracles.</b> m4, minmax, uniform and cadence are fully deterministic given (n, target), so
 * each has an independent in-test reference implementation compared row-for-row against SQL
 * output. The bucket-boundary arithmetic is re-derived here with {@link BigInteger} rather than
 * reusing {@code SubsampleAlgorithm.bucketOffset}'s decomposition identity - a reference that
 * called the production helper would prove nothing about it. sdt's exact output depends on
 * floating-point door geometry, so it is pinned with strong invariants (endpoints, ordered
 * subset, and the compression band itself: every dropped point must lie within compdev of the
 * line reconstructed from the surrounding kept points).
 * <p>
 * <b>Seeds.</b> Each test draws a fresh random root seed per run and threads it through the
 * {@code ctx} string of every assertion, so a CI failure is reproducible by pasting the printed
 * seed into {@code FIXED_SEEDS}. A few fixed seeds run alongside as permanent regressions.
 * <p>
 * <b>Swept dimensions.</b> row count against the target boundary (n &lt; target, n == target,
 * n == target+1, n &gt;&gt; target), target extremes (2, and a target above n), NULL density
 * (none, sparse, all-NULL), NaN values, duplicate timestamps, long runs of equal values, both
 * timestamp units (TIMESTAMP and TIMESTAMP_NS), plain / partitioned+WAL tables, and constant
 * versus bind-variable arguments.
 */
public class SubsampleFuzzTest extends AbstractCairoTest {

    /**
     * Permanent regression seeds; the per-run random seed is added on top of these.
     */
    private static final long[] FIXED_SEEDS = {0xDEADBEEFL, 42L, 20240101L};
    private static final String TS_NS = "TIMESTAMP_NS";
    private static final String TS_US = "TIMESTAMP";
    /**
     * 2024-01-01T00:00:00Z in micros.
     */
    private static final long EPOCH_US = 1704067200000000L;

    /**
     * cadence(stride) differential against an independent reference, swept across the
     * stride-versus-row-count boundary. The reference re-derives the documented rule: ordinal 0
     * always kept; stride == 1 keeps everything; stride &gt; totalRows keeps ONLY ordinal 0 (no
     * last-row pin); otherwise ordinals stride, 2*stride, ... plus a pinned last ordinal.
     * <p>
     * cadence is position-only, so NULL rows are NOT dropped - they occupy ordinals like any
     * other row. Fixtures therefore include NULLs to pin that distinction.
     */
    @Test
    public void testCadenceDifferentialAgainstReference() throws Exception {
        assertMemoryLeak(() -> {
            final long root = newRootSeed();
            int tableId = 0;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (long seed : seeds(root)) {
                    // strides straddling the row count: 1 (no-op), > n (first only), and divisors
                    // that do and do not land on the final ordinal.
                    final int[][] combos = {{40, 1}, {40, 7}, {40, 40}, {40, 41}, {40, 100}, {200, 3}, {1, 5}, {2, 2}};
                    for (int[] combo : combos) {
                        final int n = combo[0];
                        final int stride = combo[1];
                        for (String tsType : new String[]{TS_US, TS_NS}) {
                            final String table = "t_cad_" + tableId++;
                            final Series s = new Series(n);
                            generate(new Rnd(seed, seed * 31 + stride), s, true, false, false);
                            createAndInsert(table, s, tsType, false);

                            final String ctx = "seed=" + seed + " n=" + n + " stride=" + stride + " ts=" + tsType;
                            final int[] expected = referenceCadence(n, stride);
                            final Out out = run(compiler,
                                    "SELECT price, ts FROM " + table + " SUBSAMPLE cadence(" + stride + ")", n, tsType);
                            assertSelectionEquals(ctx, s, expected, out);
                        }
                    }
                }
            }
        });
    }

    /**
     * A seeded cadence offset is derived from a splitmix64 mix of the seed, which this test
     * deliberately does NOT reimplement (copying that constant-for-constant would assert nothing).
     * Instead it pins the contract that holds for EVERY offset: ordinal 0 kept, the output an
     * ordered subset, the gaps between consecutive interior selections exactly {@code stride}, and
     * the whole selection stable across repeated executions of the same seed.
     */
    @Test
    public void testCadenceSeededOffsetInvariants() throws Exception {
        assertMemoryLeak(() -> {
            final long root = newRootSeed();
            int tableId = 0;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (long seed : seeds(root)) {
                    final int n = 300;
                    // Unique timestamps AND unique values: this test recovers the selected ORDINALS
                    // by matching output rows back to input rows, which is only well defined when no
                    // two input rows are identical. (Duplicate-timestamp/flat-run fixtures are
                    // covered by the differential tests, which compare positionally.)
                    final Series s = new Series(n);
                    generateUnique(new Rnd(seed, seed + 5), s);
                    final String table = "t_cadseed_" + tableId++;
                    createAndInsert(table, s, TS_US, false);

                    for (int stride : new int[]{3, 11, 50}) {
                        // The seed VALUE is fuzzed; the resulting offset is opaque by design.
                        final long cadenceSeed = new Rnd(seed + stride, seed - stride).nextLong();
                        final String sql = "SELECT price, ts FROM " + table +
                                " SUBSAMPLE cadence(" + stride + ", " + cadenceSeed + ")";
                        final String ctx = "seed=" + seed + " stride=" + stride + " cadenceSeed=" + cadenceSeed;

                        final Out out = run(compiler, sql, n);
                        final int[] kept = matchSubset(ctx, s, out);

                        Assert.assertTrue(ctx + " must keep at least the first row", out.count >= 1);
                        Assert.assertEquals(ctx + " ordinal 0 kept", 0, kept[0]);
                        Assert.assertEquals(ctx + " last ordinal pinned", n - 1, kept[out.count - 1]);
                        // Interior steps are exactly `stride` apart; only the first hop (0 ->
                        // stride+offset) and the final pinned hop may be shorter.
                        for (int i = 2; i < out.count - 1; i++) {
                            Assert.assertEquals(ctx + " interior step " + i, stride, kept[i] - kept[i - 1]);
                        }
                        // Determinism: a second execution of the same statement must not move.
                        final Out again = run(compiler, sql, n);
                        assertSameOutput(ctx + " repeat execution", out, again);
                    }
                }
            }
        });
    }

    /**
     * m4(v, target) differential against an independent reference. m4 buckets by TIME (equal
     * intervals over [firstTs, lastTs]), emitting first/min/max/last per bucket, deduplicated,
     * ascending, then capping the whole output at target. NULL and NaN values are dropped before
     * bucketing - the reference compacts them out the same way.
     */
    /**
     * m4 / minmax bucket-boundary arithmetic at UNIT timestamp resolution.
     * <p>
     * The exact-integer boundary helper is
     * {@code span/numBuckets*bucket + span%numBuckets*bucket/numBuckets}. Its remainder term shifts a
     * boundary by less than {@code numBuckets} time units, so it only changes the selection when
     * consecutive rows are spaced on that same order. Every other differential test in this class
     * spaces rows 1e6 micros apart, which masks the term completely - deleting it outright leaves
     * them all green. This test spaces rows 1-5 units apart, where the term decides bucket membership.
     * <p>
     * Verified non-vacuous by mutation: replacing the helper's body with
     * {@code return span / numBuckets * bucket;} fails this test (and, before it existed, was caught
     * only by a single hand-written 20-row golden in MinMaxWindowFunctionTest).
     * <p>
     * TIMESTAMP only, deliberately: the fixture writes TIMESTAMP_NS rows as micros x1000, which
     * re-inflates the spacing to 1000 units and masks the term again. Cross-unit equivalence is
     * covered by {@link #testSelectionIsInvariantAcrossTimestampUnits()}.
     */
    @Test
    public void testBucketBoundariesAtUnitTimestampResolution() throws Exception {
        assertMemoryLeak(() -> {
            final long root = newRootSeed();
            int tableId = 0;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (long seed : seeds(root)) {
                    // Targets whose numBuckets (target/4) generally does NOT divide the span evenly.
                    final int[][] combos = {{300, 36}, {300, 52}, {200, 28}, {150, 100}, {400, 12}, {90, 40}};
                    for (int[] combo : combos) {
                        final int n = combo[0];
                        final int m = combo[1];
                        for (String method : new String[]{"m4", "minmax"}) {
                            final String table = "t_bb_" + tableId++;
                            final Series s = new Series(n);
                            generate(new Rnd(seed, seed * 29 + m), s, true, true, true, 1L);
                            createAndInsert(table, s, TS_US, false);

                            final Series live = s.compactNonNull();
                            final long span = live.n > 0 ? live.tss[live.n - 1] - live.tss[0] : 0;
                            final String ctx = "seed=" + seed + " n=" + n + " m=" + m + " method=" + method +
                                    " live=" + live.n + " span=" + span;
                            final int[] expected = "m4".equals(method)
                                    ? referenceM4(live, m)
                                    : referenceMinMax(live, m);
                            final Out out = run(compiler,
                                    "SELECT price, ts FROM " + table + " SUBSAMPLE " + method + "(price, " + m + ")",
                                    n, TS_US);
                            assertSelectionEquals(ctx, live, expected, out);
                            Assert.assertTrue(ctx + " count <= target", out.count <= m);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testEqualTimestampDifferentialAgainstReference() throws Exception {
        assertMemoryLeak(() -> {
            final Series s = new Series(7);
            final double[] values = {50, 40, 30, 20, 10, 100, 60};
            for (int i = 0; i < s.n; i++) {
                s.tss[i] = EPOCH_US;
                s.vals[i] = values[i];
            }

            final int[] expectedM4 = referenceM4(s, 4);
            final int[] expectedMinMax = referenceMinMax(s, 2);
            Assert.assertArrayEquals("M4 zero-span reference", new int[]{0, 4, 5, 6}, expectedM4);
            Assert.assertArrayEquals("MinMax zero-span reference", new int[]{4, 5}, expectedMinMax);

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                int tableId = 0;
                for (String tsType : new String[]{TS_US, TS_NS}) {
                    final String table = "t_equal_ts_" + tableId++;
                    createAndInsert(table, s, tsType, false);
                    assertSelectionEquals(
                            "M4 zero-span ts=" + tsType,
                            s,
                            expectedM4,
                            run(compiler, "SELECT price, ts FROM " + table + " SUBSAMPLE m4(price, 4)", s.n, tsType)
                    );
                    assertSelectionEquals(
                            "MinMax zero-span ts=" + tsType,
                            s,
                            expectedMinMax,
                            run(compiler, "SELECT price, ts FROM " + table + " SUBSAMPLE minmax(price, 2)", s.n, tsType)
                    );
                }
            }
        });
    }

    @Test
    public void testM4DifferentialAgainstReference() throws Exception {
        assertMemoryLeak(() -> {
            final long root = newRootSeed();
            int tableId = 0;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (long seed : seeds(root)) {
                    // target 2 exercises the numBuckets floor (target/4 == 0 -> 1) plus the cap;
                    // 400/50 gives many buckets; the rest straddle the n/target boundary.
                    final int[][] combos = {{300, 2}, {300, 8}, {120, 17}, {400, 50}, {40, 39}, {40, 40}, {40, 41}, {40, 100}};
                    for (int[] combo : combos) {
                        final int n = combo[0];
                        final int m = combo[1];
                        for (String tsType : new String[]{TS_US, TS_NS}) {
                            final String table = "t_m4_" + tableId++;
                            final Series s = new Series(n);
                            generate(new Rnd(seed, seed * 17 + m), s, true, true, true);
                            createAndInsert(table, s, tsType, false);

                            final Series live = s.compactNonNull();
                            final String ctx = "seed=" + seed + " n=" + n + " m=" + m + " ts=" + tsType +
                                    " live=" + live.n;
                            final int[] expected = referenceM4(live, m);
                            final Out out = run(compiler,
                                    "SELECT price, ts FROM " + table + " SUBSAMPLE m4(price, " + m + ")", n, tsType);
                            assertSelectionEquals(ctx, live, expected, out);
                            Assert.assertTrue(ctx + " count <= target", out.count <= m);
                        }
                    }
                }
            }
        });
    }

    /**
     * minmax(v, target) differential against an independent reference: time buckets of
     * target/2 (min 1), min and max per bucket emitted in ascending index order and
     * deduplicated, empty buckets skipped, output capped at target.
     */
    @Test
    public void testMinMaxDifferentialAgainstReference() throws Exception {
        assertMemoryLeak(() -> {
            final long root = newRootSeed();
            int tableId = 0;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (long seed : seeds(root)) {
                    final int[][] combos = {{300, 2}, {300, 9}, {120, 16}, {400, 50}, {40, 39}, {40, 40}, {40, 41}, {40, 100}};
                    for (int[] combo : combos) {
                        final int n = combo[0];
                        final int m = combo[1];
                        for (String tsType : new String[]{TS_US, TS_NS}) {
                            final String table = "t_mm_" + tableId++;
                            final Series s = new Series(n);
                            generate(new Rnd(seed, seed * 13 + m), s, true, true, true);
                            createAndInsert(table, s, tsType, false);

                            final Series live = s.compactNonNull();
                            final String ctx = "seed=" + seed + " n=" + n + " m=" + m + " ts=" + tsType +
                                    " live=" + live.n;
                            final int[] expected = referenceMinMax(live, m);
                            final Out out = run(compiler,
                                    "SELECT price, ts FROM " + table + " SUBSAMPLE minmax(price, " + m + ")", n, tsType);
                            assertSelectionEquals(ctx, live, expected, out);
                            Assert.assertTrue(ctx + " count <= target", out.count <= m);
                        }
                    }
                }
            }
        });
    }

    /**
     * uniform(target) differential against an independent reference. The reference rounds
     * {@code i * (n-1) / (target-1)} to nearest (half up) for each i, then drops consecutive
     * duplicates - the evenly-spaced-selection contract, re-derived rather than copied.
     * Like cadence, uniform is position-only, so NULL rows keep their ordinals.
     */
    @Test
    public void testUniformDifferentialAgainstReference() throws Exception {
        assertMemoryLeak(() -> {
            final long root = newRootSeed();
            int tableId = 0;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (long seed : seeds(root)) {
                    final int[][] combos = {{300, 2}, {300, 7}, {120, 17}, {400, 53}, {40, 39}, {40, 40}, {40, 41}, {40, 100}, {1, 2}};
                    for (int[] combo : combos) {
                        final int n = combo[0];
                        final int m = combo[1];
                        for (String tsType : new String[]{TS_US, TS_NS}) {
                            final String table = "t_uni_" + tableId++;
                            final Series s = new Series(n);
                            generate(new Rnd(seed, seed * 7 + m), s, true, false, false);
                            createAndInsert(table, s, tsType, false);

                            final String ctx = "seed=" + seed + " n=" + n + " m=" + m + " ts=" + tsType;
                            final int[] expected = referenceUniform(n, m);
                            final Out out = run(compiler,
                                    "SELECT price, ts FROM " + table + " SUBSAMPLE uniform(" + m + ")", n, tsType);
                            assertSelectionEquals(ctx, s, expected, out);
                            Assert.assertTrue(ctx + " count <= target", out.count <= m);
                        }
                    }
                }
            }
        });
    }

    /**
     * The timestamp UNIT must not change which rows any method selects. A wall-clock-identical
     * series stored as TIMESTAMP and as TIMESTAMP_NS must yield the same selection, because every
     * method's decision rule is either position-based (uniform/cadence) or scale-invariant in the
     * timestamp axis (m4/minmax bucket boundaries, sdt's slope corridor).
     * <p>
     * This is the class of bug that shipped in lttb's gap threshold, which was parsed as micros
     * and compared against raw nanosecond timestamps - making '1h' behave as 3.6s on a
     * TIMESTAMP_NS column. Comparing values (not rendered timestamps) keeps the two units
     * comparable.
     */
    @Test
    public void testSelectionIsInvariantAcrossTimestampUnits() throws Exception {
        assertMemoryLeak(() -> {
            final long root = newRootSeed();
            int tableId = 0;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (long seed : seeds(root)) {
                    final int n = 250;
                    final Series s = new Series(n);
                    generate(new Rnd(seed, seed + 77), s, true, false, false);
                    final String us = "t_unit_us_" + tableId;
                    final String ns = "t_unit_ns_" + tableId++;
                    createAndInsert(us, s, TS_US, false);
                    createAndInsert(ns, s, TS_NS, false);

                    final String[] methods = {
                            "m4(price, 20)", "minmax(price, 20)", "uniform(20)", "cadence(7)", "sdt(price, 0.75)"
                    };
                    for (String method : methods) {
                        final String ctx = "seed=" + seed + " method=" + method;
                        final Out a = run(compiler, "SELECT price, ts FROM " + us + " SUBSAMPLE " + method, n);
                        final Out b = run(compiler, "SELECT price, ts FROM " + ns + " SUBSAMPLE " + method, n);
                        Assert.assertEquals(ctx + " row count differs across timestamp units", a.count, b.count);
                        for (int i = 0; i < a.count; i++) {
                            Assert.assertEquals(ctx + " value[" + i + "] differs across timestamp units",
                                    a.vals[i], b.vals[i], 0.0);
                            // micros stored as nanos: the same instant scaled by exactly 1000.
                            Assert.assertEquals(ctx + " ts[" + i + "] differs across timestamp units",
                                    a.tss[i] * 1000L, b.tss[i]);
                        }
                    }
                }
            }
        });
    }

    /**
     * sdt(v, compdev) compression band. The exact door geometry is floating point, but the
     * guarantee is not: for each consecutive pair of kept points this interpolates the straight
     * line between them and bounds the deviation of every dropped row in between. Also pins
     * endpoints, the ordered-subset property, and determinism.
     * <p>
     * <b>The bound asserted here is 2*compdev, not compdev.</b> That is the true guarantee of
     * swinging-door trending, and the difference is not slack for rounding. The door corridor
     * proves each skipped point lies within compdev of SOME line in the still-feasible slope cone;
     * the line actually reconstructed - anchor to the next archived point - is one particular
     * member of that cone, and can sit up to compdev away from the line that witnessed a given
     * point, so the worst-case reconstruction error is additive. Fuzzing this suite drives the
     * observed ratio to ~1.90, i.e. genuinely above compdev and genuinely below 2*compdev.
     * <p>
     * NOTE: {@code SdtWindowFunctionFactory}'s class javadoc states the tighter claim - kept "only
     * when it cannot be represented, within compdev, by a straight line drawn between the last two
     * kept points". That wording overstates the algorithm's guarantee; the behaviour is correct
     * classic SDT and the documentation is what is imprecise.
     */
    @Test
    public void testSdtCompressionBandInvariants() throws Exception {
        assertMemoryLeak(() -> {
            final long root = newRootSeed();
            int tableId = 0;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (long seed : seeds(root)) {
                    final int n = 400;
                    // Strictly increasing timestamps and no NULLs: a duplicate timestamp or a NULL
                    // is a documented series RESET, which legitimately breaks the interpolation
                    // invariant. Those paths are covered by testSdtSubsetAndEndpointInvariants.
                    final Series s = new Series(n);
                    generate(new Rnd(seed, seed + 3), s, false, false, false);
                    final String table = "t_sdt_" + tableId++;
                    createAndInsert(table, s, TS_US, false);

                    for (double compdev : new double[]{0.0, 0.25, 2.0, 50.0}) {
                        final String sql = "SELECT price, ts FROM " + table +
                                " SUBSAMPLE sdt(price, " + compdev + ")";
                        final String ctx = "seed=" + seed + " compdev=" + compdev;
                        final Out out = run(compiler, sql, n);
                        final int[] kept = matchSubset(ctx, s, out);

                        Assert.assertTrue(ctx + " at least the endpoints are kept", out.count >= 2);
                        Assert.assertEquals(ctx + " first row kept", 0, kept[0]);
                        Assert.assertEquals(ctx + " last row kept", n - 1, kept[out.count - 1]);

                        // Every dropped row must sit within compdev of the reconstruction.
                        for (int k = 1; k < out.count; k++) {
                            final int lo = kept[k - 1];
                            final int hi = kept[k];
                            final long t0 = s.tss[lo];
                            final long t1 = s.tss[hi];
                            final double v0 = s.vals[lo];
                            final double v1 = s.vals[hi];
                            for (int i = lo + 1; i < hi; i++) {
                                final double frac = (double) (s.tss[i] - t0) / (double) (t1 - t0);
                                final double lineV = v0 + frac * (v1 - v0);
                                final double err = Math.abs(s.vals[i] - lineV);
                                // 1e-9 absolute + relative slack absorbs double rounding in the
                                // door slopes; it is far below any compdev under test.
                                final double slack = 1e-9 + 1e-9 * Math.abs(lineV);
                                Assert.assertTrue(
                                        ctx + " dropped row " + i + " deviates " + err +
                                                ", above the 2*compdev reconstruction bound (compdev=" + compdev + ")",
                                        err <= 2 * compdev + slack
                                );
                            }
                        }
                        assertSameOutput(ctx + " repeat execution", out, run(compiler, sql, n));
                    }
                }
            }
        });
    }

    /**
     * sdt over the awkward shapes: duplicate timestamps, long runs of identical values, NULLs
     * (including an all-NULL column) and NaN. Exact selection is not pinned here - duplicate
     * timestamps and NULLs reset the door series - but the structural contract still holds:
     * the output is an ordered subset and never empty for a non-empty input.
     */
    @Test
    public void testSdtSubsetAndEndpointInvariants() throws Exception {
        assertMemoryLeak(() -> {
            final long root = newRootSeed();
            int tableId = 0;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (long seed : seeds(root)) {
                    final int n = 200;
                    for (int variant = 0; variant < 4; variant++) {
                        final Series s = new Series(n);
                        switch (variant) {
                            // duplicate timestamps + flat runs
                            case 0 -> generate(new Rnd(seed, seed + 1), s, true, false, true);
                            // sparse NULLs and NaNs
                            case 1 -> generate(new Rnd(seed, seed + 2), s, true, true, true);
                            // every value NULL
                            case 2 -> {
                                generate(new Rnd(seed, seed + 3), s, false, false, false);
                                for (int i = 0; i < n; i++) {
                                    s.nulls[i] = true;
                                }
                            }
                            // one long constant run
                            default -> {
                                generate(new Rnd(seed, seed + 4), s, false, false, false);
                                for (int i = 0; i < n; i++) {
                                    s.vals[i] = 7.5;
                                }
                            }
                        }
                        final String table = "t_sdtinv_" + tableId++;
                        final String tsType = variant % 2 == 0 ? TS_US : TS_NS;
                        createAndInsert(table, s, tsType, false);

                        final String sql = "SELECT price, ts FROM " + table + " SUBSAMPLE sdt(price, 1.0)";
                        final String ctx = "seed=" + seed + " variant=" + variant;
                        final Out out = run(compiler, sql, n, tsType);
                        Assert.assertTrue(ctx + " non-empty output for non-empty input", out.count >= 1);
                        Assert.assertTrue(ctx + " output cannot exceed input", out.count <= n);
                        matchSubset(ctx, s, out);
                        assertSameOutput(ctx + " repeat execution", out, run(compiler, sql, n, tsType));
                    }
                }
            }
        });
    }

    /**
     * The same fuzz oracles over a partitioned WAL table whose rows span many daily partitions,
     * so the scan crosses page-frame boundaries rather than reading one contiguous frame. A
     * pass1/pass2 traversal that desynchronised across frames would surface here and nowhere else
     * in this file.
     */
    @Test
    public void testSelectionAcrossPartitionedWalPageFrames() throws Exception {
        assertMemoryLeak(() -> {
            final long root = newRootSeed();
            int tableId = 0;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (long seed : seeds(root)) {
                    final int n = 1500;
                    final Series s = new Series(n);
                    // ~6h steps: 1500 rows spread over roughly a year of daily partitions.
                    generateSpread(new Rnd(seed, seed + 11), s, 6 * 3600L * 1_000_000L);
                    final String table = "t_wal_" + tableId++;
                    createAndInsert(table, s, TS_US, true);

                    final String ctx = "seed=" + seed + " n=" + n;
                    final Series live = s.compactNonNull();

                    final Out m4 = run(compiler,
                            "SELECT price, ts FROM " + table + " SUBSAMPLE m4(price, 40)", n);
                    assertSelectionEquals(ctx + " m4", live, referenceM4(live, 40), m4);

                    final Out mm = run(compiler,
                            "SELECT price, ts FROM " + table + " SUBSAMPLE minmax(price, 40)", n);
                    assertSelectionEquals(ctx + " minmax", live, referenceMinMax(live, 40), mm);

                    final Out uni = run(compiler,
                            "SELECT price, ts FROM " + table + " SUBSAMPLE uniform(40)", n);
                    assertSelectionEquals(ctx + " uniform", s, referenceUniform(n, 40), uni);

                    final Out cad = run(compiler,
                            "SELECT price, ts FROM " + table + " SUBSAMPLE cadence(37)", n);
                    assertSelectionEquals(ctx + " cadence", s, referenceCadence(n, 37), cad);
                }
            }
        });
    }

    /**
     * A bind-variable target/stride must select exactly what the equivalent literal selects.
     * Constants are validated at compile time while bind variables are re-read per execution, so
     * the two paths are genuinely different code - and re-binding between executions must take
     * effect rather than reusing the first run's resolved value.
     */
    @Test
    public void testBindVariableArgumentsMatchConstants() throws Exception {
        assertMemoryLeak(() -> {
            final long root = newRootSeed();
            int tableId = 0;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (long seed : seeds(root)) {
                    final int n = 300;
                    final Series s = new Series(n);
                    generate(new Rnd(seed, seed + 23), s, true, true, false);
                    final String table = "t_bind_" + tableId++;
                    createAndInsert(table, s, TS_US, false);

                    // Re-bound within one compiled statement: the second value must take effect.
                    for (int arg : new int[]{5, 23}) {
                        final String ctx = "seed=" + seed + " arg=" + arg;
                        for (String method : new String[]{"uniform", "cadence", "m4", "minmax"}) {
                            final boolean valued = "m4".equals(method) || "minmax".equals(method);
                            final String litSql = "SELECT price, ts FROM " + table + " SUBSAMPLE " +
                                    method + "(" + (valued ? "price, " : "") + arg + ")";
                            final String bindSql = "SELECT price, ts FROM " + table + " SUBSAMPLE " +
                                    method + "(" + (valued ? "price, " : "") + "$1)";
                            final Out lit = run(compiler, litSql, n);
                            bindVariableService.setLong(0, arg);
                            final Out bound = run(compiler, bindSql, n);
                            assertSameOutput(ctx + " method=" + method + " bind vs literal", lit, bound);
                        }
                    }
                }
            }
        });
    }

    // ---------------------------------------------------------------------------------------
    // reference implementations
    // ---------------------------------------------------------------------------------------

    private static int[] identity(int n) {
        final int[] all = new int[n];
        for (int i = 0; i < n; i++) {
            all[i] = i;
        }
        return all;
    }

    /**
     * Exact {@code floor(span * bucket / numBuckets)} for span &gt;= 0, computed in
     * {@link BigInteger} so the oracle does not inherit the production decomposition it exists
     * to check.
     */
    private static long exactBucketOffset(long span, int bucket, int numBuckets) {
        return BigInteger.valueOf(span)
                .multiply(BigInteger.valueOf(bucket))
                .divide(BigInteger.valueOf(numBuckets))
                .longValueExact();
    }

    /**
     * Independent M4: time buckets, first/min/max/last each, deduplicated, capped at target.
     */
    private static int[] referenceM4(Series s, int target) {
        final int[] out = new int[Math.max(target, 4) + 8];
        int outCount = 0;
        if (s.n <= 0 || target <= 0) {
            return new int[0];
        }
        if (s.n <= target) {
            // Production short-circuits bucketing when the buffered rows already fit the target:
            // bucketing would dedup first/min/max/last and could drop rows a caller asked to keep.
            return identity(s.n);
        }
        int numBuckets = Math.max(1, target / 4);
        final long minTs = s.tss[0];
        final long maxTs = s.tss[s.n - 1];
        final long span = maxTs - minTs;
        if (span <= 0) {
            numBuckets = 1;
        }
        int dataIdx = 0;
        for (int bucket = 0; bucket < numBuckets; bucket++) {
            final long startTs = minTs + exactBucketOffset(span, bucket, numBuckets);
            final long endTs = bucket < numBuckets - 1
                    ? minTs + exactBucketOffset(span, bucket + 1, numBuckets)
                    : Long.MAX_VALUE;
            int firstIdx = -1, lastIdx = -1, minIdx = -1, maxIdx = -1;
            double minVal = 0, maxVal = 0;
            boolean hasData = false;
            while (dataIdx < s.n) {
                final long ts = s.tss[dataIdx];
                if (bucket < numBuckets - 1 && ts >= endTs) {
                    break;
                }
                if (ts >= startTs) {
                    final double v = s.vals[dataIdx];
                    if (firstIdx == -1) {
                        firstIdx = dataIdx;
                    }
                    lastIdx = dataIdx;
                    if (!hasData) {
                        minVal = maxVal = v;
                        minIdx = maxIdx = dataIdx;
                        hasData = true;
                    } else {
                        if (v < minVal) {
                            minVal = v;
                            minIdx = dataIdx;
                        }
                        if (v > maxVal) {
                            maxVal = v;
                            maxIdx = dataIdx;
                        }
                    }
                }
                dataIdx++;
            }
            if (firstIdx == -1) {
                continue;
            }
            // ascending, deduplicated
            final int[] four = {firstIdx, minIdx, maxIdx, lastIdx};
            java.util.Arrays.sort(four);
            for (int i = 0; i < 4; i++) {
                if (i == 0 || four[i] != four[i - 1]) {
                    out[outCount++] = four[i];
                }
            }
        }
        final int capped = Math.min(outCount, target);
        final int[] result = new int[capped];
        System.arraycopy(out, 0, result, 0, capped);
        return result;
    }

    /**
     * Independent MinMax: time buckets of target/2, min and max per bucket, capped at target.
     */
    private static int[] referenceMinMax(Series s, int target) {
        final int[] out = new int[Math.max(target, 2) + 8];
        int outCount = 0;
        if (s.n <= 0 || target <= 0) {
            return new int[0];
        }
        if (s.n <= target) {
            return identity(s.n);
        }
        int numBuckets = Math.max(1, target / 2);
        final long minTs = s.tss[0];
        final long maxTs = s.tss[s.n - 1];
        final long span = maxTs - minTs;
        if (span <= 0) {
            numBuckets = 1;
        }
        int dataIdx = 0;
        for (int bucket = 0; bucket < numBuckets; bucket++) {
            final long startTs = minTs + exactBucketOffset(span, bucket, numBuckets);
            final long endTs = bucket < numBuckets - 1
                    ? minTs + exactBucketOffset(span, bucket + 1, numBuckets)
                    : Long.MAX_VALUE;
            int minIdx = -1, maxIdx = -1;
            double minVal = 0, maxVal = 0;
            boolean hasData = false;
            while (dataIdx < s.n) {
                final long ts = s.tss[dataIdx];
                if (bucket < numBuckets - 1 && ts >= endTs) {
                    break;
                }
                if (ts >= startTs) {
                    final double v = s.vals[dataIdx];
                    if (!hasData) {
                        minVal = maxVal = v;
                        minIdx = maxIdx = dataIdx;
                        hasData = true;
                    } else {
                        if (v < minVal) {
                            minVal = v;
                            minIdx = dataIdx;
                        }
                        if (v > maxVal) {
                            maxVal = v;
                            maxIdx = dataIdx;
                        }
                    }
                }
                dataIdx++;
            }
            if (!hasData) {
                continue;
            }
            if (minIdx == maxIdx) {
                out[outCount++] = minIdx;
            } else {
                out[outCount++] = Math.min(minIdx, maxIdx);
                out[outCount++] = Math.max(minIdx, maxIdx);
            }
        }
        final int capped = Math.min(outCount, target);
        final int[] result = new int[capped];
        System.arraycopy(out, 0, result, 0, capped);
        return result;
    }

    /**
     * Independent uniform: target positions evenly spaced over [0, n-1], each rounded to the
     * nearest ordinal (ties up), consecutive duplicates dropped.
     */
    private static int[] referenceUniform(int n, int target) {
        if (n <= target) {
            return identity(n);
        }
        final int[] out = new int[target];
        int outCount = 0;
        final long range = n - 1;
        final long divisor = target - 1;
        int prev = -1;
        for (long i = 0; i < target; i++) {
            // round-half-up of i*range/divisor, computed exactly
            final BigInteger num = BigInteger.valueOf(i).multiply(BigInteger.valueOf(range))
                    .multiply(BigInteger.TWO).add(BigInteger.valueOf(divisor));
            final int pos = num.divide(BigInteger.valueOf(divisor * 2)).intValueExact();
            if (pos != prev) {
                out[outCount++] = pos;
                prev = pos;
            }
        }
        final int[] result = new int[outCount];
        System.arraycopy(out, 0, result, 0, outCount);
        return result;
    }

    /**
     * Independent cadence with no seed offset.
     */
    private static int[] referenceCadence(int n, int stride) {
        if (stride == 1) {
            return identity(n);
        }
        if (n <= 0) {
            return new int[0];
        }
        final int[] out = new int[n];
        int outCount = 0;
        out[outCount++] = 0;
        if (stride <= n) {
            for (long pos = stride; pos < n; pos += stride) {
                out[outCount++] = (int) pos;
            }
            if (out[outCount - 1] != n - 1) {
                out[outCount++] = n - 1;
            }
        }
        final int[] result = new int[outCount];
        System.arraycopy(out, 0, result, 0, outCount);
        return result;
    }

    // ---------------------------------------------------------------------------------------
    // fixtures and helpers
    // ---------------------------------------------------------------------------------------

    /**
     * Fresh per-run seed so every CI run explores new inputs; printed via assertion context.
     */
    private static long newRootSeed() {
        return System.nanoTime();
    }

    private static long[] seeds(long root) {
        final long[] all = new long[FIXED_SEEDS.length + 1];
        System.arraycopy(FIXED_SEEDS, 0, all, 0, FIXED_SEEDS.length);
        all[FIXED_SEEDS.length] = root;
        return all;
    }

    /**
     * Random walk with occasional spikes. {@code dupTs} injects ~1/4 duplicate timestamps,
     * {@code withNulls} ~10% NULLs (capped at n/6), {@code withNaN} a few explicit NaN values,
     * and every run emits at least one long flat run of identical values.
     */
    private static void generate(Rnd rnd, Series s, boolean dupTs, boolean withNulls, boolean withNaN) {
        generate(rnd, s, dupTs, withNulls, withNaN, 1_000_000L);
    }

    /**
     * As {@link #generate(Rnd, Series, boolean, boolean, boolean)}, with an explicit timestamp step
     * unit.
     * <p>
     * The step is what decides whether bucket-boundary arithmetic is observable at all. Production
     * cuts buckets at {@code span/numBuckets*bucket + span%numBuckets*bucket/numBuckets}; the
     * remainder term moves a boundary by strictly less than {@code numBuckets} time units. At the
     * default 1e6-micros spacing no boundary perturbation that small can ever move a row across a
     * bucket, so the term is invisible to a differential. A step of 1 makes it load-bearing.
     */
    private static void generate(Rnd rnd, Series s, boolean dupTs, boolean withNulls, boolean withNaN, long stepUnit) {
        long ts = EPOCH_US;
        double base = 0;
        int nullCount = 0;
        final int runStart = s.n > 20 ? rnd.nextInt(s.n - 15) : Integer.MAX_VALUE;
        final double runValue = rnd.nextDouble() * 10;
        for (int i = 0; i < s.n; i++) {
            s.tss[i] = ts;
            ts += dupTs && rnd.nextInt(4) == 0 ? 0 : (1 + rnd.nextInt(5)) * stepUnit;
            base += rnd.nextDouble() - 0.5;
            double v = base;
            if (rnd.nextInt(25) == 0) {
                v += rnd.nextBoolean() ? 1000 : -1000;
            }
            if (i >= runStart && i < runStart + 10) {
                v = runValue; // long run of equal values
            }
            s.vals[i] = v;
            s.nulls[i] = false;
            s.nans[i] = false;
            if (withNulls && nullCount < s.n / 6 && rnd.nextInt(10) == 0) {
                s.nulls[i] = true;
                nullCount++;
            } else if (withNaN && rnd.nextInt(40) == 0) {
                s.nans[i] = true;
            }
        }
    }

    /**
     * Strictly increasing timestamps and strictly distinct values, so an output row can be mapped
     * back to exactly one input row.
     */
    private static void generateUnique(Rnd rnd, Series s) {
        long ts = EPOCH_US;
        for (int i = 0; i < s.n; i++) {
            s.tss[i] = ts;
            ts += (1 + rnd.nextInt(5)) * 1_000_000L;
            s.vals[i] = i + rnd.nextDouble() * 0.5; // strictly increasing => distinct
            s.nulls[i] = false;
            s.nans[i] = false;
        }
    }

    /**
     * Strictly increasing timestamps at a fixed step, so rows spread across many partitions.
     */
    private static void generateSpread(Rnd rnd, Series s, long stepMicros) {
        long ts = EPOCH_US;
        double base = 0;
        for (int i = 0; i < s.n; i++) {
            s.tss[i] = ts;
            ts += stepMicros;
            base += rnd.nextDouble() - 0.5;
            s.vals[i] = rnd.nextInt(25) == 0 ? base + 500 : base;
            s.nulls[i] = false;
            s.nans[i] = false;
        }
    }

    private static void createAndInsert(String table, Series s, String tsType, boolean wal) throws Exception {
        final String tsCast = TS_NS.equals(tsType) ? "timestamp_ns" : "timestamp";
        final long tsScale = TS_NS.equals(tsType) ? 1000L : 1L;
        execute("CREATE TABLE " + table + " (price DOUBLE, ts " + tsType + ") TIMESTAMP(ts)" +
                (wal ? " PARTITION BY DAY WAL" : ""));
        final StringBuilder sb = new StringBuilder("INSERT INTO ").append(table).append(" VALUES ");
        for (int i = 0; i < s.n; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('(');
            if (s.nulls[i]) {
                sb.append("null");
            } else if (s.nans[i]) {
                sb.append("cast('NaN' as double)");
            } else {
                // Double.toString round-trips exactly through the SQL double literal parser
                sb.append(s.vals[i]);
            }
            sb.append(",cast(").append(s.tss[i] * tsScale).append(" as ").append(tsCast).append("))");
        }
        execute(sb.toString());
        if (wal) {
            drainWalQueue();
        }
    }

    private static Out run(SqlCompiler compiler, String sql, int capacity) throws Exception {
        return run(compiler, sql, capacity, TS_US);
    }

    private static Out run(SqlCompiler compiler, String sql, int capacity, String tsType) throws Exception {
        final Out out = new Out(capacity);
        out.tsScale = TS_NS.equals(tsType) ? 1000L : 1L;
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                out.vals[out.count] = record.getDouble(0);
                out.tss[out.count] = record.getTimestamp(1);
                out.count++;
            }
        }
        return out;
    }

    /**
     * Row-for-row comparison of SQL output against reference-selected indices of {@code s}.
     * Fixture timestamps are held in micros, so a TIMESTAMP_NS table reads back scaled by 1000.
     */
    private static void assertSelectionEquals(String ctx, Series s, int[] expected, Out out) {
        Assert.assertEquals(ctx + " row count", expected.length, out.count);
        for (int i = 0; i < expected.length; i++) {
            final int idx = expected[i];
            Assert.assertEquals(ctx + " ts[" + i + "] (expected input row " + idx + ")",
                    s.tss[idx] * out.tsScale, out.tss[i]);
            if (s.nulls[idx] || s.nans[idx]) {
                Assert.assertTrue(ctx + " value[" + i + "] expected NaN/NULL", Double.isNaN(out.vals[i]));
            } else {
                Assert.assertEquals(ctx + " value[" + i + "]", s.vals[idx], out.vals[i], 0.0);
            }
        }
    }

    /**
     * Verifies the output is an order-preserving subset of the input and returns the input index
     * chosen for each output row.
     */
    private static int[] matchSubset(String ctx, Series s, Out out) {
        final int[] kept = new int[Math.max(out.count, 1)];
        int in = 0;
        for (int i = 0; i < out.count; i++) {
            while (in < s.n && !rowMatches(s, in, out, i)) {
                in++;
            }
            Assert.assertTrue(ctx + ": output row " + i + " (ts=" + out.tss[i] + ") is not an order-preserving input row",
                    in < s.n);
            kept[i] = in;
            in++;
        }
        return kept;
    }

    private static boolean rowMatches(Series s, int in, Out out, int i) {
        if (s.tss[in] * out.tsScale != out.tss[i]) {
            return false;
        }
        if (s.nulls[in] || s.nans[in]) {
            return Double.isNaN(out.vals[i]);
        }
        return s.vals[in] == out.vals[i];
    }

    private static void assertSameOutput(String ctx, Out a, Out b) {
        Assert.assertEquals(ctx + " row count", a.count, b.count);
        for (int i = 0; i < a.count; i++) {
            Assert.assertEquals(ctx + " ts[" + i + "]", a.tss[i], b.tss[i]);
            Assert.assertEquals(ctx + " value[" + i + "]", a.vals[i], b.vals[i], 0.0);
        }
    }

    /**
     * Input fixture: timestamps in MICROS plus per-row NULL / NaN markers.
     */
    private static final class Series {
        final int n;
        final boolean[] nans;
        final boolean[] nulls;
        final long[] tss;
        final double[] vals;

        Series(int n) {
            this.n = n;
            this.tss = new long[n];
            this.vals = new double[n];
            this.nulls = new boolean[n];
            this.nans = new boolean[n];
        }

        private Series(int n, long[] tss, double[] vals, boolean[] nulls, boolean[] nans) {
            this.n = n;
            this.tss = tss;
            this.vals = vals;
            this.nulls = nulls;
            this.nans = nans;
        }

        /**
         * The rows a value-reading method actually buffers: NULL and NaN values are dropped
         * before bucketing, so m4/minmax oracles run over this compacted view.
         */
        Series compactNonNull() {
            final long[] t = new long[n];
            final double[] v = new double[n];
            int k = 0;
            for (int i = 0; i < n; i++) {
                if (!nulls[i] && !nans[i]) {
                    t[k] = tss[i];
                    v[k] = vals[i];
                    k++;
                }
            }
            return new Series(k, t, v, new boolean[n], new boolean[n]);
        }
    }

    /**
     * SQL output rows. {@code tsScale} converts fixture micros into the column's native unit.
     */
    private static final class Out {
        final long[] tss;
        final double[] vals;
        int count;
        long tsScale = 1L;

        Out(int capacity) {
            this.tss = new long[Math.max(capacity, 1)];
            this.vals = new double[Math.max(capacity, 1)];
        }
    }
}
