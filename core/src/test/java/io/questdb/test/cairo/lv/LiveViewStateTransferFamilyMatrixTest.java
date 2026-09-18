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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.lv.LiveViewAccumulatorDescriptor;
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRoot;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.lv.LiveViewWindowStateManifest;
import io.questdb.cairo.lv.LiveViewWindowStatePlan;
import io.questdb.griffin.engine.window.WindowAccumulatorDescriptor;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * The state-transfer family matrix: every durable component family
 * {@link LiveViewAccumulatorDescriptor#familyCodecVersion} admits, sealed under one
 * setting of {@code cairo.sql.window.map.fusion.enabled} and restarted under the other
 * - or the same - in all four combinations.
 * <p>
 * Each case runs the same script. Phase one seeds a view with rows that put every
 * distinguishing state a family has into the head root: an identity/empty key that has
 * seen only null contributions, a key with one contribution, a key with several, and
 * whatever the family's own accumulator carries beyond a count (a nonzero Kahan
 * compensation, Welford's mean and M2, an extremum that a later row must not move). The
 * case then reads the head window root back off disk and compares the manifest to the
 * storage plan's byte for byte and every key's payload to an image this test builds on its
 * own from the inserted values, through the descriptor's slot layout alone. The restart
 * flips the switch or leaves it, and must take the {@code timeline_restore} route on the
 * very root the case just inspected; phase two then adds the first contributing row for
 * the empty key, a continuation that changes nothing, and an anchor crossing, and reads
 * the root back again.
 * <p>
 * The byte-level comparison is what makes the matrix a state-<i>transfer</i> matrix rather
 * than a results one. Two runtimes that disagree on how a private map's state is gathered
 * into a window root can still both answer the recompute correctly, because each reads
 * back what it wrote; they cannot both match an image computed from the values. And a
 * function that quietly fell out of the group and kept a root of its own would still
 * answer the recompute, which is why every case asserts the plan's families, projection
 * count and empty residual list before it looks at a single row.
 */
@RunWith(Parameterized.class)
public class LiveViewStateTransferFamilyMatrixTest extends AbstractLiveViewTest {

    private static final String DAY_ONE = "2026-01-01T";
    private static final String DAY_TWO = "2026-01-02T";
    private static final String FRAME = "over (partition by account_id, bucket order by created_at "
            + "rows between unbounded preceding and current row)";
    private static final int NO_ARGUMENT = WindowAccumulatorDescriptor.NO_ARGUMENT_COLUMN_INDEX;
    private static final String NULL_KEY = "<null>";
    /**
     * One SUM/AVG/COUNT component past the inline leaf budget, computed the way the plan
     * computes it so the case follows the constant rather than pinning a number beside it.
     */
    private static final int WIDE_COLUMNS =
            (LiveViewCheckpointContracts.MAX_INLINE_LEAF_STATE_BYTES - Long.BYTES)
                    / (Double.BYTES + Long.BYTES) + 1;
    private final boolean isRestartFused;
    private final boolean isSealFused;
    /**
     * Per seal in insertion order: the keys the freeze imaged, then the anchor map's size
     * at that moment. This is what tells a complete capture from an incremental one.
     */
    private final LongList sealStats = new LongList();

    public LiveViewStateTransferFamilyMatrixTest(String sealMode, String restartMode) {
        this.isSealFused = "fused".equals(sealMode);
        this.isRestartFused = "fused".equals(restartMode);
    }

    @Parameterized.Parameters(name = "seal {0}, restart {1}")
    public static Collection<Object[]> combinations() {
        return Arrays.asList(new Object[][]{
                {"unfused", "unfused"},
                {"fused", "fused"},
                {"unfused", "fused"},
                {"fused", "unfused"},
        });
    }

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpSealMode() {
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, String.valueOf(isSealFused));
        // One logical boundary per commit, so every insert below seals a head the case can read.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
        sealStats.clear();
    }

    @Test
    public void testDoubleKahanSumCount() throws Exception {
        // 1e16 + 1 is not representable, so the first two rows leave a compensation of -1
        // in the checkpoint, and a restore that dropped it would answer 1e16 + 1 -> 1e16
        // for the next row where the compensated total answers 1e16 + 2.
        runMatrix(new Family("amount double", "ksum(amount) over w as k, count(amount) over w as c", "k, c") {
            @Override
            void expectAfterPhaseOne(ExpectedImages images) {
                images.key("acct-1", DAY_ONE).component(2, kahan(1e16, 1.0));
                images.key("acct-2", DAY_ONE).component(2, kahan());
                Assert.assertNotEquals(
                        "the fixture must leave a nonzero compensation in the checkpoint",
                        0.0,
                        kahanCompensation(1e16, 1.0),
                        0.0
                );
            }

            @Override
            void expectAfterPhaseTwo(ExpectedImages images) {
                images.key("acct-1", DAY_TWO).component(2, kahan(0.5));
                images.key("acct-2", DAY_ONE).component(2, kahan(5.0));
            }

            @Override
            int[] families() {
                return new int[]{WindowAccumulatorDescriptor.FAMILY_DOUBLE_KAHAN_SUM_COUNT};
            }

            @Override
            String oracle() {
                return "ksum(amount) " + FRAME + " as k, count(amount) " + FRAME + " as c";
            }

            @Override
            void phaseOne(LiveViewRefreshJob job) throws Exception {
                insert(job, DAY_ONE, 0, "'acct-1', 1e16");
                insert(job, DAY_ONE, 10, "'acct-2', null");
                insert(job, DAY_ONE, 20, "'acct-1', 1.0");
            }

            @Override
            void phaseTwo(LiveViewRefreshJob job) throws Exception {
                insert(job, DAY_ONE, 30, "'acct-1', 1.0");
                insert(job, DAY_ONE, 40, "'acct-1', -1e16");
                insert(job, DAY_ONE, 50, "'acct-2', 5.0");
                insert(job, DAY_TWO, 0, "'acct-1', 0.5");
            }
        });
    }

    @Test
    public void testDoubleMax() throws Exception {
        runMatrix(new Family("x double", "max(x) over w as mx", "mx") {
            @Override
            void expectAfterPhaseOne(ExpectedImages images) {
                images.key("acct-1", DAY_ONE).component(2, extremum(9.0));
                images.key("acct-2", DAY_ONE).component(2, extremum(Double.NaN));
            }

            @Override
            void expectAfterPhaseTwo(ExpectedImages images) {
                images.key("acct-1", DAY_TWO).component(2, extremum(0.25));
                images.key("acct-2", DAY_ONE).component(2, extremum(-2.0));
            }

            @Override
            int[] families() {
                return new int[]{WindowAccumulatorDescriptor.FAMILY_DOUBLE_MAX};
            }

            @Override
            String oracle() {
                return "max(x) " + FRAME + " as mx";
            }

            @Override
            void phaseOne(LiveViewRefreshJob job) throws Exception {
                insert(job, DAY_ONE, 0, "'acct-1', 3.0");
                insert(job, DAY_ONE, 10, "'acct-1', 9.0");
                insert(job, DAY_ONE, 20, "'acct-2', null");
                insert(job, DAY_ONE, 30, "'acct-1', 5.0");
            }

            @Override
            void phaseTwo(LiveViewRefreshJob job) throws Exception {
                insert(job, DAY_ONE, 40, "'acct-2', -2.0");
                // Below the running maximum: the continuation must leave the state alone.
                insert(job, DAY_ONE, 50, "'acct-1', 1.0");
                assertHeadImage("acct-1", 2, extremum(9.0));
                insert(job, DAY_ONE, 60, "'acct-1', 12.0");
                insert(job, DAY_TWO, 0, "'acct-1', 0.25");
            }
        });
    }

    @Test
    public void testDoubleMin() throws Exception {
        runMatrix(new Family("x double", "min(x) over w as mn", "mn") {
            @Override
            void expectAfterPhaseOne(ExpectedImages images) {
                images.key("acct-1", DAY_ONE).component(2, extremum(-9.0));
                images.key("acct-2", DAY_ONE).component(2, extremum(Double.NaN));
            }

            @Override
            void expectAfterPhaseTwo(ExpectedImages images) {
                images.key("acct-1", DAY_TWO).component(2, extremum(0.25));
                images.key("acct-2", DAY_ONE).component(2, extremum(2.0));
            }

            @Override
            int[] families() {
                return new int[]{WindowAccumulatorDescriptor.FAMILY_DOUBLE_MIN};
            }

            @Override
            String oracle() {
                return "min(x) " + FRAME + " as mn";
            }

            @Override
            void phaseOne(LiveViewRefreshJob job) throws Exception {
                insert(job, DAY_ONE, 0, "'acct-1', 3.0");
                insert(job, DAY_ONE, 10, "'acct-1', -9.0");
                insert(job, DAY_ONE, 20, "'acct-2', null");
                insert(job, DAY_ONE, 30, "'acct-1', 5.0");
            }

            @Override
            void phaseTwo(LiveViewRefreshJob job) throws Exception {
                insert(job, DAY_ONE, 40, "'acct-2', 2.0");
                // Above the running minimum: the continuation must leave the state alone.
                insert(job, DAY_ONE, 50, "'acct-1', 1.0");
                assertHeadImage("acct-1", 2, extremum(-9.0));
                insert(job, DAY_ONE, 60, "'acct-1', -12.0");
                insert(job, DAY_TWO, 0, "'acct-1', 0.25");
            }
        });
    }

    @Test
    public void testDoubleSumCount() throws Exception {
        runMatrix(new Family(
                "amount double",
                "sum(amount) over w as s, avg(amount) over w as a, count(amount) over w as c",
                "s, a, c"
        ) {
            @Override
            void expectAfterPhaseOne(ExpectedImages images) {
                images.key("acct-1", DAY_ONE).component(2, sumCount(4.0, 2));
                images.key("acct-2", DAY_ONE).component(2, sumCount(0.0, 0));
                images.key("acct-3", DAY_ONE).component(2, sumCount(10.0, 1));
            }

            @Override
            void expectAfterPhaseTwo(ExpectedImages images) {
                images.key("acct-1", DAY_TWO).component(2, sumCount(4.0, 1));
                images.key("acct-2", DAY_ONE).component(2, sumCount(7.0, 1));
                images.key("acct-3", DAY_ONE).component(2, sumCount(10.0, 1));
            }

            @Override
            int[] families() {
                return new int[]{WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT};
            }

            @Override
            String oracle() {
                return "sum(amount) " + FRAME + " as s, avg(amount) " + FRAME + " as a, count(amount) " + FRAME + " as c";
            }

            @Override
            void phaseOne(LiveViewRefreshJob job) throws Exception {
                insert(job, DAY_ONE, 0, "'acct-1', 1.5");
                insert(job, DAY_ONE, 10, "'acct-2', null");
                insert(job, DAY_ONE, 20, "'acct-3', 10.0");
                insert(job, DAY_ONE, 30, "'acct-1', 2.5");
                insert(job, DAY_ONE, 40, "'acct-3', null");
            }

            @Override
            void phaseTwo(LiveViewRefreshJob job) throws Exception {
                insert(job, DAY_ONE, 50, "'acct-2', 7.0");
                insert(job, DAY_ONE, 60, "'acct-1', 3.0");
                assertHeadImage("acct-1", 2, sumCount(7.0, 3));
                insert(job, DAY_TWO, 0, "'acct-1', 4.0");
            }
        });
    }

    @Test
    public void testDoubleWelford() throws Exception {
        runMatrix(new Family(
                "x double",
                "var_pop(x) over w as vp, var_samp(x) over w as vs, stddev_pop(x) over w as sp, "
                        + "stddev_samp(x) over w as ss, count(x) over w as c",
                "vp, vs, sp, ss, c"
        ) {
            @Override
            void expectAfterPhaseOne(ExpectedImages images) {
                images.key("acct-1", DAY_ONE).component(2, welford(2.0, 4.0, 4.0, 4.0));
                images.key("acct-2", DAY_ONE).component(2, welford(3.0));
                images.key("acct-3", DAY_ONE).component(2, welford());
            }

            @Override
            void expectAfterPhaseTwo(ExpectedImages images) {
                images.key("acct-1", DAY_ONE).component(2, welford(2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0));
                images.key("acct-2", DAY_TWO).component(2, welford(1.0));
                images.key("acct-3", DAY_ONE).component(2, welford(6.0));
            }

            @Override
            int[] families() {
                return new int[]{WindowAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD};
            }

            @Override
            String oracle() {
                return "var_pop(x) " + FRAME + " as vp, var_samp(x) " + FRAME + " as vs, stddev_pop(x) " + FRAME
                        + " as sp, stddev_samp(x) " + FRAME + " as ss, count(x) " + FRAME + " as c";
            }

            @Override
            void phaseOne(LiveViewRefreshJob job) throws Exception {
                insert(job, DAY_ONE, 0, "'acct-1', 2.0");
                insert(job, DAY_ONE, 10, "'acct-2', 3.0");
                insert(job, DAY_ONE, 20, "'acct-3', null");
                insert(job, DAY_ONE, 30, "'acct-1', 4.0");
                insert(job, DAY_ONE, 40, "'acct-1', 4.0");
                insert(job, DAY_ONE, 50, "'acct-1', 4.0");
            }

            @Override
            void phaseTwo(LiveViewRefreshJob job) throws Exception {
                insert(job, DAY_ONE, 60, "'acct-3', 6.0");
                insert(job, DAY_ONE, 70, "'acct-1', 5.0");
                insert(job, DAY_ONE, 80, "'acct-1', 5.0");
                insert(job, DAY_ONE, 90, "'acct-1', 7.0");
                insert(job, DAY_ONE, 100, "'acct-1', 9.0");
                insert(job, DAY_TWO, 0, "'acct-2', 1.0");
            }
        });
    }

    @Test
    public void testLongMax() throws Exception {
        // 2^53 + 1 and its neighbours: exact as LONGs, indistinguishable once widened to
        // DOUBLE. The DATE and TIMESTAMP calls are the same family over the same word, and
        // the argument type keeps them three components.
        runMatrix(new Family(
                "l long, dt date, ts2 timestamp",
                "max(l) over w as ml, max(dt) over w as md, max(ts2) over w as mt",
                "ml, md, mt"
        ) {
            @Override
            void expectAfterPhaseOne(ExpectedImages images) {
                images.key("acct-1", DAY_ONE)
                        .component(2, extremum(9_007_199_254_740_995L))
                        .component(3, extremum(1_700_000_000_001L))
                        .component(4, extremum(1_700_000_000_000_003L));
                images.key("acct-2", DAY_ONE)
                        .component(2, extremum(Numbers.LONG_NULL))
                        .component(3, extremum(Numbers.LONG_NULL))
                        .component(4, extremum(Numbers.LONG_NULL));
            }

            @Override
            void expectAfterPhaseTwo(ExpectedImages images) {
                images.key("acct-1", DAY_TWO)
                        .component(2, extremum(-5L))
                        .component(3, extremum(1L))
                        .component(4, extremum(1L));
                images.key("acct-2", DAY_ONE)
                        .component(2, extremum(1L))
                        .component(3, extremum(5L))
                        .component(4, extremum(7L));
            }

            @Override
            int[] families() {
                return new int[]{
                        WindowAccumulatorDescriptor.FAMILY_LONG_MAX,
                        WindowAccumulatorDescriptor.FAMILY_LONG_MAX,
                        WindowAccumulatorDescriptor.FAMILY_LONG_MAX,
                };
            }

            @Override
            String oracle() {
                return "max(l) " + FRAME + " as ml, max(dt) " + FRAME + " as md, max(ts2) " + FRAME + " as mt";
            }

            @Override
            void phaseOne(LiveViewRefreshJob job) throws Exception {
                insert(job, DAY_ONE, 0, "'acct-1', 9007199254740993, cast(1700000000001 as date), "
                        + "cast(1700000000000003 as timestamp)");
                insert(job, DAY_ONE, 10, "'acct-2', null, null, null");
                insert(job, DAY_ONE, 20, "'acct-1', 9007199254740995, cast(1700000000000 as date), "
                        + "cast(1700000000000001 as timestamp)");
            }

            @Override
            void phaseTwo(LiveViewRefreshJob job) throws Exception {
                insert(job, DAY_ONE, 30, "'acct-2', 1, cast(5 as date), cast(7 as timestamp)");
                // One below the running maximum on every column: nothing may move.
                insert(job, DAY_ONE, 40, "'acct-1', 9007199254740994, cast(1700000000000 as date), "
                        + "cast(1700000000000002 as timestamp)");
                assertHeadImage("acct-1", 2, extremum(9_007_199_254_740_995L));
                insert(job, DAY_ONE, 50, "'acct-1', 9007199254740997, cast(1700000000009 as date), "
                        + "cast(1700000000000009 as timestamp)");
                insert(job, DAY_TWO, 0, "'acct-1', -5, cast(1 as date), cast(1 as timestamp)");
            }
        });
    }

    @Test
    public void testLongMin() throws Exception {
        runMatrix(new Family(
                "l long, dt date, ts2 timestamp",
                "min(l) over w as ml, min(dt) over w as md, min(ts2) over w as mt",
                "ml, md, mt"
        ) {
            @Override
            void expectAfterPhaseOne(ExpectedImages images) {
                images.key("acct-1", DAY_ONE)
                        .component(2, extremum(9_007_199_254_740_993L))
                        .component(3, extremum(1_700_000_000_000L))
                        .component(4, extremum(1_700_000_000_000_001L));
                images.key("acct-2", DAY_ONE)
                        .component(2, extremum(Numbers.LONG_NULL))
                        .component(3, extremum(Numbers.LONG_NULL))
                        .component(4, extremum(Numbers.LONG_NULL));
            }

            @Override
            void expectAfterPhaseTwo(ExpectedImages images) {
                images.key("acct-1", DAY_TWO)
                        .component(2, extremum(9_007_199_254_740_999L))
                        .component(3, extremum(1_700_000_000_100L))
                        .component(4, extremum(1_700_000_000_000_100L));
                images.key("acct-2", DAY_ONE)
                        .component(2, extremum(1L))
                        .component(3, extremum(5L))
                        .component(4, extremum(7L));
            }

            @Override
            int[] families() {
                return new int[]{
                        WindowAccumulatorDescriptor.FAMILY_LONG_MIN,
                        WindowAccumulatorDescriptor.FAMILY_LONG_MIN,
                        WindowAccumulatorDescriptor.FAMILY_LONG_MIN,
                };
            }

            @Override
            String oracle() {
                return "min(l) " + FRAME + " as ml, min(dt) " + FRAME + " as md, min(ts2) " + FRAME + " as mt";
            }

            @Override
            void phaseOne(LiveViewRefreshJob job) throws Exception {
                insert(job, DAY_ONE, 0, "'acct-1', 9007199254740995, cast(1700000000001 as date), "
                        + "cast(1700000000000003 as timestamp)");
                insert(job, DAY_ONE, 10, "'acct-2', null, null, null");
                insert(job, DAY_ONE, 20, "'acct-1', 9007199254740993, cast(1700000000000 as date), "
                        + "cast(1700000000000001 as timestamp)");
            }

            @Override
            void phaseTwo(LiveViewRefreshJob job) throws Exception {
                insert(job, DAY_ONE, 30, "'acct-2', 1, cast(5 as date), cast(7 as timestamp)");
                // One above the running minimum on every column: nothing may move.
                insert(job, DAY_ONE, 40, "'acct-1', 9007199254740994, cast(1700000000001 as date), "
                        + "cast(1700000000000002 as timestamp)");
                assertHeadImage("acct-1", 2, extremum(9_007_199_254_740_993L));
                insert(job, DAY_ONE, 50, "'acct-1', 9007199254740991, cast(1699999999999 as date), "
                        + "cast(1699999999999999 as timestamp)");
                insert(job, DAY_TWO, 0, "'acct-1', 9007199254740999, cast(1700000000100 as date), "
                        + "cast(1700000000000100 as timestamp)");
            }
        });
    }

    @Test
    public void testNonNullCount() throws Exception {
        // Five standalone counters, one per contribution rule the count factories
        // apply: a finite test for DOUBLE and for the LONG that widens into it, a null test
        // for VARCHAR, SYMBOL and DECIMAL.
        runMatrix(new Family(
                "d double, l long, v varchar, s2 symbol, dec decimal(18,3)",
                "count(d) over w as cd, count(l) over w as cl, count(v) over w as cv, "
                        + "count(s2) over w as cs, count(dec) over w as cdec",
                "cd, cl, cv, cs, cdec"
        ) {
            @Override
            void expectAfterPhaseOne(ExpectedImages images) {
                images.key("acct-1", DAY_ONE)
                        .component(2, count(1))
                        .component(3, count(2))
                        .component(4, count(1))
                        .component(5, count(2))
                        .component(6, count(1));
                images.key("acct-2", DAY_ONE)
                        .component(2, count(0))
                        .component(3, count(0))
                        .component(4, count(0))
                        .component(5, count(0))
                        .component(6, count(0));
            }

            @Override
            void expectAfterPhaseTwo(ExpectedImages images) {
                images.key("acct-1", DAY_TWO)
                        .component(2, count(0))
                        .component(3, count(1))
                        .component(4, count(1))
                        .component(5, count(1))
                        .component(6, count(0));
                images.key("acct-2", DAY_ONE)
                        .component(2, count(1))
                        .component(3, count(1))
                        .component(4, count(1))
                        .component(5, count(1))
                        .component(6, count(1));
            }

            @Override
            int[] families() {
                return new int[]{
                        WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                        WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                        WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                        WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                        WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                };
            }

            @Override
            String oracle() {
                return "count(d) " + FRAME + " as cd, count(l) " + FRAME + " as cl, count(v) " + FRAME
                        + " as cv, count(s2) " + FRAME + " as cs, count(dec) " + FRAME + " as cdec";
            }

            @Override
            void phaseOne(LiveViewRefreshJob job) throws Exception {
                insert(job, DAY_ONE, 0, "'acct-1', 1.0, 1, 'v1', 'z', 1.5::decimal(18,3)");
                insert(job, DAY_ONE, 10, "'acct-2', null, null, null, null, null");
                insert(job, DAY_ONE, 20, "'acct-1', null, null, null, null, null");
                insert(job, DAY_ONE, 30, "'acct-1', null, 2, null, 'y', null");
            }

            @Override
            void phaseTwo(LiveViewRefreshJob job) throws Exception {
                insert(job, DAY_ONE, 40, "'acct-2', 2.0, 2, 'v2', 'x', 2.5::decimal(18,3)");
                insert(job, DAY_ONE, 50, "'acct-1', 3.0, null, 'v3', null, 2.25::decimal(18,3)");
                assertHeadImage("acct-1", 2, count(2));
                assertHeadImage("acct-1", 3, count(2));
                assertHeadImage("acct-1", 6, count(2));
                insert(job, DAY_TWO, 0, "'acct-1', null, 4, 'v4', 'x', null");
            }
        });
    }

    @Test
    public void testRowCount() throws Exception {
        // One counter serves all three calls: count(*) and row_number() are the same
        // component, and count(account_id) over the window's own partition key reads it
        // through the guard that answers zero for the NULL-key partition.
        runMatrix(new Family(
                "amount double",
                "count(*) over w as r, row_number() over w as rn, count(account_id) over w as c",
                "r, rn, c"
        ) {
            @Override
            void expectAfterPhaseOne(ExpectedImages images) {
                images.key("acct-1", DAY_ONE).component(NO_ARGUMENT, count(2));
                images.key(null, DAY_ONE).component(NO_ARGUMENT, count(2));
                images.key("acct-2", DAY_ONE).component(NO_ARGUMENT, count(1));
            }

            @Override
            void expectAfterPhaseTwo(ExpectedImages images) {
                images.key("acct-1", DAY_TWO).component(NO_ARGUMENT, count(1));
                images.key(null, DAY_ONE).component(NO_ARGUMENT, count(3));
                images.key("acct-2", DAY_ONE).component(NO_ARGUMENT, count(2));
            }

            @Override
            int[] families() {
                return new int[]{WindowAccumulatorDescriptor.FAMILY_ROW_COUNT};
            }

            @Override
            void assertPlanShape(LiveViewWindowStatePlan plan) {
                super.assertPlanShape(plan);
                boolean guarded = false;
                for (int i = 0, n = plan.getProjectionCount(); i < n; i++) {
                    guarded |= plan.getProjection(i).isPartitionKeyGuarded();
                }
                Assert.assertTrue("count(account_id) must read the row count through the partition-key guard", guarded);
            }

            @Override
            String oracle() {
                return "count(*) " + FRAME + " as r, "
                        + "row_number() over (partition by account_id, bucket order by created_at) as rn, "
                        + "count(account_id) " + FRAME + " as c";
            }

            @Override
            void phaseOne(LiveViewRefreshJob job) throws Exception {
                insert(job, DAY_ONE, 0, "'acct-1', 1.0");
                insert(job, DAY_ONE, 10, "null, 2.0");
                insert(job, DAY_ONE, 20, "'acct-2', 3.0");
                insert(job, DAY_ONE, 30, "null, 4.0");
                insert(job, DAY_ONE, 40, "'acct-1', 5.0");
            }

            @Override
            void phaseTwo(LiveViewRefreshJob job) throws Exception {
                insert(job, DAY_ONE, 50, "null, 6.0");
                insert(job, DAY_ONE, 60, "'acct-2', 7.0");
                insert(job, DAY_TWO, 0, "'acct-1', 8.0");
            }
        });
    }

    @Test
    public void testSharedComponentsBeyondTheInlineBudgetKeepFunctionRoots() throws Exception {
        // WIDE_COLUMNS SUM/COUNT pairs is one component more than the leaf holds. The
        // canonical prefix is the window root's manifest; the last pair is a runtime-only
        // member of the group - its slots sit in the same map value, its bytes go to the
        // function root each of its two projections keeps - and both of those roots have to
        // come back correctly whichever way the switch is set on either side of the restart.
        final StringBuilder columns = new StringBuilder();
        final StringBuilder projections = new StringBuilder();
        final StringBuilder aliases = new StringBuilder();
        final StringBuilder oracle = new StringBuilder();
        for (int i = 1; i <= WIDE_COLUMNS; i++) {
            if (i > 1) {
                columns.append(", ");
                projections.append(", ");
                aliases.append(", ");
                oracle.append(", ");
            }
            columns.append("q").append(i).append(" double");
            projections.append("sum(q").append(i).append(") over w as s").append(i)
                    .append(", count(q").append(i).append(") over w as c").append(i);
            aliases.append("s").append(i).append(", c").append(i);
            oracle.append("sum(q").append(i).append(") ").append(FRAME).append(" as s").append(i)
                    .append(", count(q").append(i).append(") ").append(FRAME).append(" as c").append(i);
        }
        final int inlineComponents = WIDE_COLUMNS - 1;
        runMatrix(new Family(columns.toString(), projections.toString(), aliases.toString()) {
            @Override
            void assertPlanShape(LiveViewWindowStatePlan plan) {
                Assert.assertEquals("every pair must be in the group", WIDE_COLUMNS, plan.getComponentCount());
                Assert.assertEquals(inlineComponents, plan.getDurableComponentCount());
                Assert.assertEquals(inlineComponents, plan.getManifest().getComponentCount());
                Assert.assertEquals(
                        "a component past the budget is a runtime-only member, not a residual",
                        0,
                        plan.getResidualFunctions().size()
                );
                int runtimeOnly = 0;
                for (int i = 0, n = plan.getProjectionCount(); i < n; i++) {
                    if (!plan.isDurableProjection(i)) {
                        runtimeOnly++;
                    }
                }
                Assert.assertEquals("the last pair's two projections must keep their own roots", 2, runtimeOnly);
                for (int i = 0; i < WIDE_COLUMNS; i++) {
                    Assert.assertEquals(
                            WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT,
                            plan.getComponent(i).getFamily()
                    );
                }
            }

            @Override
            void expectAfterPhaseOne(ExpectedImages images) {
                final KeyImage one = images.key("acct-1", DAY_ONE);
                final KeyImage two = images.key("acct-2", DAY_ONE);
                for (int i = 1; i <= inlineComponents; i++) {
                    one.component(i + 1, sumCount(3.0 * i, 2));
                    two.component(i + 1, sumCount(0.0, 0));
                }
            }

            @Override
            void expectAfterPhaseTwo(ExpectedImages images) {
                final KeyImage one = images.key("acct-1", DAY_TWO);
                final KeyImage two = images.key("acct-2", DAY_ONE);
                for (int i = 1; i <= inlineComponents; i++) {
                    one.component(i + 1, sumCount(1.0 * i, 1));
                    two.component(i + 1, sumCount(5.0 * i, 1));
                }
            }

            @Override
            int expectedFunctionRoots() {
                return 2;
            }

            @Override
            int[] families() {
                throw new UnsupportedOperationException("assertPlanShape checks the prefix");
            }

            @Override
            String oracle() {
                return oracle.toString();
            }

            @Override
            void phaseOne(LiveViewRefreshJob job) throws Exception {
                insert(job, DAY_ONE, 0, "'acct-1', " + wideValues(1));
                insert(job, DAY_ONE, 10, "'acct-2', " + wideNulls());
                insert(job, DAY_ONE, 20, "'acct-1', " + wideValues(2));
            }

            @Override
            void phaseTwo(LiveViewRefreshJob job) throws Exception {
                insert(job, DAY_ONE, 30, "'acct-2', " + wideValues(5));
                insert(job, DAY_ONE, 40, "'acct-1', " + wideValues(3));
                insert(job, DAY_TWO, 0, "'acct-1', " + wideValues(1));
            }
        });
    }

    private static long anchorOf(String day) {
        return ts(day + "00:00:00.000000Z");
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private static FieldValue[] count(long count) {
        return new FieldValue[]{new FieldValue(WindowAccumulatorDescriptor.FIELD_NON_NULL_COUNT, count)};
    }

    /**
     * A SYMBOL partition key reaches the root as the STRING its value resolves to: a
     * little-endian length prefix, {@link TableUtils#NULL_LEN} for the NULL key, then
     * UTF-16 code units.
     */
    private static String decodeKey(byte[] key) {
        Assert.assertTrue("a key must carry its length prefix", key.length >= Integer.BYTES);
        final int length = (key[0] & 0xff) | (key[1] & 0xff) << 8 | (key[2] & 0xff) << 16 | (key[3] & 0xff) << 24;
        if (length == TableUtils.NULL_LEN) {
            Assert.assertEquals("the NULL key must carry nothing beyond its length", Integer.BYTES, key.length);
            return NULL_KEY;
        }
        Assert.assertEquals("a STRING key must be its length prefix plus two bytes per char", Integer.BYTES + 2 * length, key.length);
        final char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            final int at = Integer.BYTES + 2 * i;
            chars[i] = (char) ((key[at] & 0xff) | (key[at + 1] & 0xff) << 8);
        }
        return new String(chars);
    }

    private static FieldValue[] extremum(double value) {
        return new FieldValue[]{new FieldValue(WindowAccumulatorDescriptor.FIELD_EXTREMUM, Double.doubleToRawLongBits(value))};
    }

    private static FieldValue[] extremum(long value) {
        return new FieldValue[]{new FieldValue(WindowAccumulatorDescriptor.FIELD_EXTREMUM, value)};
    }

    private static String hex(byte[] bytes) {
        final StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
        }
        return sb.toString();
    }

    /**
     * Kahan summation as {@code ksum} runs it, restated here so the expected image is
     * computed from the values rather than read back out of the runtime under test.
     */
    private static FieldValue[] kahan(double... values) {
        double sum = 0;
        double c = 0;
        long count = 0;
        for (double d : values) {
            final double y = d - c;
            final double t = sum + y;
            c = (t - sum) - y;
            sum = t;
            count++;
        }
        return new FieldValue[]{
                new FieldValue(WindowAccumulatorDescriptor.FIELD_SUM, Double.doubleToRawLongBits(sum)),
                new FieldValue(WindowAccumulatorDescriptor.FIELD_KAHAN_COMPENSATION, Double.doubleToRawLongBits(c)),
                new FieldValue(WindowAccumulatorDescriptor.FIELD_NON_NULL_COUNT, count),
        };
    }

    private static double kahanCompensation(double... values) {
        return Double.longBitsToDouble(kahan(values)[1].bits);
    }

    private static FieldValue[] sumCount(double sum, long count) {
        return new FieldValue[]{
                new FieldValue(WindowAccumulatorDescriptor.FIELD_SUM, Double.doubleToRawLongBits(sum)),
                new FieldValue(WindowAccumulatorDescriptor.FIELD_NON_NULL_COUNT, count),
        };
    }

    /**
     * Welford's online update as the dispersion functions run it: {@code mean += (d - mean) / n;
     * m2 += (d - mean) * (d - oldMean)}, in that order and with those operands, so the bytes
     * agree rather than merely the values within tolerance.
     */
    private static FieldValue[] welford(double... values) {
        double mean = 0;
        double m2 = 0;
        long count = 0;
        for (double d : values) {
            count++;
            final double oldMean = mean;
            mean += (d - mean) / count;
            m2 += (d - mean) * (d - oldMean);
        }
        return new FieldValue[]{
                new FieldValue(WindowAccumulatorDescriptor.FIELD_MEAN, Double.doubleToRawLongBits(mean)),
                new FieldValue(WindowAccumulatorDescriptor.FIELD_M2, Double.doubleToRawLongBits(m2)),
                new FieldValue(WindowAccumulatorDescriptor.FIELD_NON_NULL_COUNT, count),
        };
    }

    private static String wideNulls() {
        final StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= WIDE_COLUMNS; i++) {
            if (i > 1) {
                sb.append(", ");
            }
            sb.append("null");
        }
        return sb.toString();
    }

    private static String wideValues(int ordinal) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= WIDE_COLUMNS; i++) {
            if (i > 1) {
                sb.append(", ");
            }
            sb.append(ordinal * i).append(".0");
        }
        return sb.toString();
    }

    /**
     * Reads the head window root back and compares it to {@code expected}: the manifest
     * against the storage plan's bytes, the inline width, every key's whole payload against
     * the image built from the inserted values, and the function directory's size.
     */
    private void assertHeadRoot(Family family, ExpectedImages expected) {
        final LiveViewInstance instance = instance();
        final LiveViewWindowStatePlan plan = storagePlan();
        try (
                Path checkpointsDir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader timeline = openTimelineReader(instance);
                LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                LiveViewCheckpointWindowRoot windowRoot = new LiveViewCheckpointWindowRoot(configuration);
                LiveViewCheckpointPartitionMapReader partitions = new LiveViewCheckpointPartitionMapReader(configuration);
                LiveViewCheckpointFunctionDirectory directory = new LiveViewCheckpointFunctionDirectory(configuration)
        ) {
            final LiveViewCheckpointTimelineEntry newest = new LiveViewCheckpointTimelineEntry();
            Assert.assertTrue("the view must have sealed a boundary", timeline.last(pin.getTimelineRootRef(), newest));
            root.of(checkpointsDir, newest.rootRef);
            final LiveViewCheckpointPageRef stateRootRef = new LiveViewCheckpointPageRef();
            root.getStateRootRef(stateRootRef);
            Assert.assertFalse("an anchored seal must publish a state root", stateRootRef.isNull());
            Assert.assertTrue(
                    "the state root must be a window root whichever way the switch is set",
                    windowRoot.ofIfWindowRoot(checkpointsDir, stateRootRef)
            );

            Assert.assertArrayEquals(
                    "the root's manifest must be the storage plan's, byte for byte",
                    plan.getManifest().getEncoded(),
                    windowRoot.getManifest()
            );
            Assert.assertArrayEquals(plan.getWindowIdentity(), windowRoot.getWindowIdentity());
            Assert.assertEquals(plan.getTotalInlineStateBytes(), windowRoot.getTotalInlineStateBytes());

            final Map<String, byte[]> actual = new HashMap<>();
            final LiveViewCheckpointPageRef mapRootRef = new LiveViewCheckpointPageRef();
            windowRoot.getPartitionMapRootRef(mapRootRef);
            partitions.of(checkpointsDir);
            partitions.iterateAll(mapRootRef, entry -> {
                final String key = decodeKey(entry.getKey());
                final byte[] payload = LiveViewCheckpointWindowRoot.readWindowState(entry, plan.getTotalInlineStateBytes());
                Assert.assertNull("key " + key + " appears twice in the root", actual.put(key, Arrays.copyOf(payload, payload.length)));
            });

            Assert.assertEquals(
                    "the root must hold exactly the keys the rows named",
                    new TreeSet<>(expected.byKey.keySet()),
                    new TreeSet<>(actual.keySet())
            );
            for (Map.Entry<String, byte[]> e : actual.entrySet()) {
                final byte[] expectedPayload = expected.byKey.get(e.getKey()).toPayload(plan);
                if (!Arrays.equals(expectedPayload, e.getValue())) {
                    Assert.fail("key " + e.getKey() + " payload mismatch\nexpected " + hex(expectedPayload)
                            + "\nactual   " + hex(e.getValue()));
                }
            }

            final LiveViewCheckpointPageRef directoryRef = new LiveViewCheckpointPageRef();
            root.getFunctionDirectoryRef(directoryRef);
            final int functionRoots;
            if (directoryRef.isNull()) {
                functionRoots = 0;
            } else {
                directory.of(checkpointsDir, directoryRef);
                functionRoots = directory.size();
            }
            Assert.assertEquals(
                    "function roots must exist for exactly the projections outside the manifest",
                    family.expectedFunctionRoots(),
                    functionRoots
            );
        }
    }

    /**
     * One component of one key off the head root, for a continuation row that must leave
     * the state where it was.
     */
    private void assertHeadImage(String account, int argumentColumnIndex, FieldValue[] fields) {
        final LiveViewInstance instance = instance();
        final LiveViewWindowStatePlan plan = storagePlan();
        int componentIndex = -1;
        for (int i = 0, n = plan.getDurableComponentCount(); i < n; i++) {
            if (plan.getComponent(i).getArgumentColumnIndex() == argumentColumnIndex) {
                componentIndex = i;
                break;
            }
        }
        Assert.assertTrue("no durable component over column " + argumentColumnIndex, componentIndex >= 0);
        final LiveViewAccumulatorDescriptor component = plan.getComponent(componentIndex);
        final byte[] expected = new byte[component.getStateLength()];
        KeyImage.writeFields(component, fields, expected, 0);
        try (
                Path checkpointsDir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader timeline = openTimelineReader(instance);
                LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                LiveViewCheckpointWindowRoot windowRoot = new LiveViewCheckpointWindowRoot(configuration);
                LiveViewCheckpointPartitionMapReader partitions = new LiveViewCheckpointPartitionMapReader(configuration)
        ) {
            final LiveViewCheckpointTimelineEntry newest = new LiveViewCheckpointTimelineEntry();
            Assert.assertTrue(timeline.last(pin.getTimelineRootRef(), newest));
            root.of(checkpointsDir, newest.rootRef);
            final LiveViewCheckpointPageRef stateRootRef = new LiveViewCheckpointPageRef();
            root.getStateRootRef(stateRootRef);
            Assert.assertTrue(windowRoot.ofIfWindowRoot(checkpointsDir, stateRootRef));
            final LiveViewCheckpointPageRef mapRootRef = new LiveViewCheckpointPageRef();
            windowRoot.getPartitionMapRootRef(mapRootRef);
            partitions.of(checkpointsDir);
            final int offset = plan.getManifest().getComponentStateOffset(componentIndex);
            final boolean[] found = new boolean[1];
            partitions.iterateAll(mapRootRef, entry -> {
                if (!account.equals(decodeKey(entry.getKey()))) {
                    return;
                }
                found[0] = true;
                final byte[] payload = LiveViewCheckpointWindowRoot.readWindowState(entry, plan.getTotalInlineStateBytes());
                Assert.assertArrayEquals(
                        "key " + account + " component over column " + argumentColumnIndex,
                        expected,
                        Arrays.copyOfRange(payload, offset, offset + expected.length)
                );
            });
            Assert.assertTrue("the root must hold key " + account, found[0]);
        }
    }

    /**
     * The plan-level half of the family assertion: the storage plan exists in both modes,
     * names exactly the families the case expects, persists every projection and leaves no
     * residual - so a function that fell out of the group cannot answer the oracle in its
     * place. The runtime binding is then checked against the switch separately.
     */
    private void assertPlanAndRuntimeMode(Family family, boolean isFused) {
        final LiveViewWindow window = window();
        final LiveViewWindowStatePlan plan = window.getCheckpointStoragePlan();
        Assert.assertNotNull("an anchored window must carry a storage plan in both modes", plan);
        family.assertPlanShape(plan);
        Assert.assertEquals("runtime sharing must follow the switch", isFused, window.isWindowStateFused());
        if (isFused) {
            Assert.assertSame(
                    "the fused runtime must bind the very plan the seal writes under",
                    plan,
                    window.getCheckpointWindowStatePlan()
            );
        } else {
            Assert.assertNull("the switch must leave the window unbound", window.getCheckpointWindowStatePlan());
        }
    }

    private void assertViewMatchesOracle(Family family) throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(select created_at, account_id, " + family.oracle()
                        + " from (select *, " + bucket + " as bucket from tx)) order by 2, 1",
                "(select created_at, account_id, " + family.aliases + " from lv) order by 2, 1",
                LOG,
                true
        );
    }

    /**
     * Appends one row, seals it, and records what the seal imaged against how many keys
     * the window held, so the case can tell a complete capture from an incremental one.
     */
    private void insert(LiveViewRefreshJob job, String day, int secondOfDay, String values) throws Exception {
        execute("insert into tx values ('" + day + String.format("09:%02d:%02d.000000Z", secondOfDay / 60, secondOfDay % 60)
                + "', " + values + ")");
        drainWalQueue();
        driveRefreshToQuiescence(job);
        final LiveViewWindow window = window();
        sealStats.add(window.getCheckpointLastFreezeKeyCount());
        sealStats.add(window.getAnchorMapSize());
    }

    private LiveViewInstance instance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    private LiveViewCheckpointMetaStore openStore(LiveViewInstance instance) {
        final LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(configuration);
        try (Path dir = checkpointsDir(instance)) {
            store.of(dir);
        }
        return store;
    }

    private LiveViewCheckpointTimelineReader openTimelineReader(LiveViewInstance instance) {
        final LiveViewCheckpointTimelineReader reader = new LiveViewCheckpointTimelineReader(configuration);
        try (Path dir = checkpointsDir(instance)) {
            reader.of(dir);
        }
        return reader;
    }

    private void restartCycle() throws Exception {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
        try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(resumed);
        }
    }

    private void runMatrix(Family family) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tx (created_at timestamp, account_id symbol, " + family.columns + ") "
                    + "timestamp(created_at) partition by hour wal");
            execute("create live view lv flush every 100ms start from beginning as "
                    + "select created_at, account_id, " + family.projections
                    + " from tx window w as (partition by account_id order by created_at anchor daily '00:00')");

            final ExpectedImages afterPhaseOne = new ExpectedImages();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                family.phaseOne(job);

                assertPlanAndRuntimeMode(family, isSealFused);
                family.expectAfterPhaseOne(afterPhaseOne);
                assertHeadRoot(family, afterPhaseOne);

                // The first seal imaged the whole domain it had; the last imaged only the
                // one key its row touched out of several - an incremental capture against
                // the baseline the earlier seals established.
                Assert.assertEquals("the first seal must image every key", sealStats.getQuick(1), sealStats.getQuick(0));
                final int last = sealStats.size() - 2;
                Assert.assertTrue("phase one must end with several live keys", sealStats.getQuick(last + 1) > 1);
                Assert.assertEquals("the last seal must image only the touched key", 1L, sealStats.getQuick(last));
                Assert.assertNotEquals(
                        "phase one must leave an incremental baseline behind",
                        Numbers.LONG_NULL,
                        window().getCheckpointBaselineGeneration()
                );

                assertViewMatchesOracle(family);
                assertNoRefreshFaults("lv");
            }

            // The restart: the registry is dropped, the view's SQL recompiles under the
            // restart setting and the runtime comes back off the root inspected above.
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, String.valueOf(isRestartFused));
            sealStats.clear();
            restartCycle();

            assertPlanAndRuntimeMode(family, isRestartFused);
            assertRestoredFromTimeline("lv");
            // Nothing rebuilt and nothing republished: the head is the very root phase one sealed.
            assertHeadRoot(family, afterPhaseOne);
            assertViewMatchesOracle(family);
            assertNoRefreshFaults("lv");

            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                family.phaseTwo(resumed);

                final ExpectedImages afterPhaseTwo = new ExpectedImages();
                family.expectAfterPhaseTwo(afterPhaseTwo);
                assertHeadRoot(family, afterPhaseTwo);
                // A restore re-establishes the incremental baseline against the root it came
                // back on, in both runtime modes: the first seal after it images only the key
                // its row touched rather than rescanning the restored domain, and so does the
                // last seal of phase two.
                Assert.assertTrue("the restored domain must hold several keys", sealStats.getQuick(1) > 1);
                Assert.assertEquals(
                        "the first seal after a restore must build on the restored baseline",
                        1L,
                        sealStats.getQuick(0)
                );
                final int last = sealStats.size() - 2;
                Assert.assertTrue(sealStats.getQuick(last + 1) > 1);
                Assert.assertEquals("the last seal after restart must be incremental", 1L, sealStats.getQuick(last));

                assertViewMatchesOracle(family);
                assertNoRefreshFaults("lv");
                // Still the restore, still on the roots the previous process published.
                assertRestoredFromTimeline("lv");
            }
        });
    }

    private LiveViewWindowStatePlan storagePlan() {
        final LiveViewWindowStatePlan plan = window().getCheckpointStoragePlan();
        Assert.assertNotNull("an anchored window must carry a storage plan in both modes", plan);
        return plan;
    }

    private LiveViewWindow window() {
        final LiveViewWindow window = instance().getAnchorWindow();
        Assert.assertNotNull("the anchored view must have built its window", window);
        return window;
    }

    /**
     * The image the head root must hold per key: the anchor the key's newest row fell
     * into and, per durable component, the family's fields computed from the values.
     */
    private static final class ExpectedImages {
        final Map<String, KeyImage> byKey = new HashMap<>();

        KeyImage key(String account, String anchorDay) {
            final KeyImage image = new KeyImage(anchorOf(anchorDay));
            byKey.put(account == null ? NULL_KEY : account, image);
            return image;
        }
    }

    private static final class FieldValue {
        final long bits;
        final int field;

        FieldValue(int field, long bits) {
            this.field = field;
            this.bits = bits;
        }
    }

    private static final class KeyImage {
        private final long anchorValue;
        private final Map<Integer, FieldValue[]> fieldsByArgument = new HashMap<>();

        KeyImage(long anchorValue) {
            this.anchorValue = anchorValue;
        }

        /**
         * Writes {@code fields} into {@code image} through the descriptor's own slot
         * layout, and requires every slot of the component to be named exactly once, so an
         * expectation cannot leave a slot unasserted.
         */
        static void writeFields(LiveViewAccumulatorDescriptor component, FieldValue[] fields, byte[] image, int offset) {
            Assert.assertEquals(
                    "the expectation must name every slot of family " + component.getFamily(),
                    component.getSlotCount(),
                    fields.length
            );
            final IntList slots = new IntList();
            for (FieldValue field : fields) {
                final int slot = component.getFieldSlot(field.field);
                Assert.assertTrue("family " + component.getFamily() + " has no field " + field.field, slot >= 0);
                Assert.assertFalse("field " + field.field + " named twice", slots.contains(slot));
                slots.add(slot);
                putLongLE(image, offset + slot * Long.BYTES, field.bits);
            }
        }

        private static void putLongLE(byte[] payload, int offset, long value) {
            for (int i = 0; i < Long.BYTES; i++) {
                payload[offset + i] = (byte) (value >>> (i * Byte.SIZE));
            }
        }

        KeyImage component(int argumentColumnIndex, FieldValue[] fields) {
            Assert.assertNull("component over column " + argumentColumnIndex + " expected twice", fieldsByArgument.put(argumentColumnIndex, fields));
            return this;
        }

        byte[] toPayload(LiveViewWindowStatePlan plan) {
            final LiveViewWindowStateManifest manifest = plan.getManifest();
            final byte[] payload = new byte[plan.getTotalInlineStateBytes()];
            putLongLE(payload, LiveViewWindowStatePlan.ANCHOR_STATE_OFFSET, anchorValue);
            final List<Integer> seen = new ArrayList<>();
            for (int i = 0, n = manifest.getComponentCount(); i < n; i++) {
                final LiveViewAccumulatorDescriptor component = plan.getComponent(i);
                final FieldValue[] fields = fieldsByArgument.get(component.getArgumentColumnIndex());
                Assert.assertNotNull(
                        "no expectation for the component over column " + component.getArgumentColumnIndex(),
                        fields
                );
                seen.add(component.getArgumentColumnIndex());
                writeFields(component, fields, payload, manifest.getComponentStateOffset(i));
            }
            Assert.assertEquals("every expected component must be in the manifest", fieldsByArgument.size(), seen.size());
            return payload;
        }
    }

    /**
     * One durable family, with everything a matrix case needs to drive it: the base
     * columns, the view's window calls, the families they must compile to, the two insert
     * phases, the expected images after each, and the generic recompute of the same window.
     */
    private abstract static class Family {
        final String aliases;
        final String columns;
        final String projections;

        Family(String columns, String projections, String aliases) {
            this.columns = columns;
            this.projections = projections;
            this.aliases = aliases;
        }

        void assertPlanShape(LiveViewWindowStatePlan plan) {
            final int[] expected = families();
            final int[] actual = new int[plan.getDurableComponentCount()];
            for (int i = 0; i < actual.length; i++) {
                actual[i] = plan.getComponent(i).getFamily();
            }
            Arrays.sort(expected);
            Arrays.sort(actual);
            Assert.assertArrayEquals("the plan must compile to exactly the expected families", expected, actual);
            Assert.assertEquals(plan.getDurableComponentCount(), plan.getManifest().getComponentCount());
            Assert.assertEquals("every call must be in the group", 0, plan.getResidualFunctions().size());
            for (int i = 0, n = plan.getProjectionCount(); i < n; i++) {
                Assert.assertTrue("projection " + i + " must be durable", plan.isDurableProjection(i));
            }
        }

        abstract void expectAfterPhaseOne(ExpectedImages images);

        abstract void expectAfterPhaseTwo(ExpectedImages images);

        int expectedFunctionRoots() {
            return 0;
        }

        abstract int[] families();

        abstract String oracle();

        abstract void phaseOne(LiveViewRefreshJob job) throws Exception;

        abstract void phaseTwo(LiveViewRefreshJob job) throws Exception;
    }
}
