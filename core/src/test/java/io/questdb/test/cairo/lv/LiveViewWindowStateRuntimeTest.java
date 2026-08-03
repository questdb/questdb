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
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewAccumulatorDescriptor;
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreReader;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewStatePageWriter;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.lv.LiveViewWindowStatePlan;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Coverage for runtime window-state fusion: the anchored window owning one map value per
 * partition that carries the anchor, the bucket bookkeeping and every grouped
 * accumulator component, with the grouped SELECT-list calls reduced to read-only
 * projections of it.
 * <p>
 * Three things carry this, and the cases are built around them.
 * <ul>
 *     <li><b>One value, one probe.</b> The group's whole per-row work - the crossing
 *     reset, every contributor update and every output's projection - runs against a
 *     value {@code processRow} has already loaded, so a grouped function's private map
 *     is never allocated at all. What proves it is that the outputs still match a
 *     from-base recompute while those maps hold nothing.</li>
 *     <li><b>The component codec is a claim about two implementations.</b> A fused entry
 *     inlines the accumulator at the manifest's offset with no length of its own, so the
 *     descriptor's encoding must be byte-for-byte the image the contributing function's
 *     own {@code freezeCheckpointState} writes. That is checked directly rather than
 *     inferred.</li>
 *     <li><b>Adoption is reversible, and both directions move the state.</b> A window may
 *     be rebound while it holds a live frontier - the upgrade lifecycle does exactly that
 *     - and dropping the accumulators instead of migrating them would silently restart
 *     every partition at zero.</li>
 * </ul>
 */
public class LiveViewWindowStateRuntimeTest extends AbstractLiveViewTest {

    private static final String DAILY_ANCHOR = "2026-01-01T";

    @Before
    public void setUpCheckpointCadence() {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testAdoptingAndDecliningThePlanCarriesTheAccumulatorsBothWays() throws Exception {
        assertMemoryLeak(() -> {
            createTargetView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insertAccount(job, DAILY_ANCHOR + "09:00:00.000000Z", "acct-1", 5.0);
                insertAccount(job, DAILY_ANCHOR + "09:00:10.000000Z", "acct-2", 7.0);
                insertAccount(job, DAILY_ANCHOR + "09:00:20.000000Z", "acct-1", 11.0);

                final LiveViewWindow window = window();
                final LiveViewWindowStatePlan plan = window.getCheckpointWindowStatePlan();
                Assert.assertNotNull("the target shape must adopt the plan", plan);
                Assert.assertEquals(2, window.getAnchorMapSize());

                // Handing the state back must reconstruct the private maps the functions
                // own outside a group, accumulator for accumulator...
                Assert.assertFalse(window.bindCheckpointWindowStatePlan(null));
                for (int i = 0, n = plan.getProjectionCount(); i < n; i++) {
                    final WindowFunction function = plan.getProjectionFunction(i);
                    Assert.assertFalse("projection " + i + " must own its state again", function.isWindowStateOwned());
                    Assert.assertEquals(
                            "projection " + i + " must hold every partition",
                            2,
                            function.getPartitionMap().size()
                    );
                }
                // ...and every later row must keep computing against them.
                insertAccount(job, DAILY_ANCHOR + "09:00:30.000000Z", "acct-1", 2.0);
                assertViewMatchesRecompute();

                // Adopting again must take them back the same way, rather than starting
                // the accumulators over.
                Assert.assertTrue(window.bindCheckpointWindowStatePlan(plan));
                for (int i = 0, n = plan.getProjectionCount(); i < n; i++) {
                    final WindowFunction function = plan.getProjectionFunction(i);
                    Assert.assertTrue("projection " + i + " must be fused", function.isWindowStateOwned());
                    Assert.assertFalse(
                            "a fused projection's private map must be released",
                            function.getPartitionMap().isOpen()
                    );
                }
                insertAccount(job, DAILY_ANCHOR + "09:00:40.000000Z", "acct-1", 4.0);
                assertViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testAFrontierSweepReclaimsTheGroupThroughTheOneMap() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_STALE_PERCENT, 50);
        assertMemoryLeak(() -> {
            createTargetView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                // Three accounts in one bucket, then two bucket advances, so the first
                // bucket falls behind the retained pair and its accounts are reclaimable.
                insertAccount(job, "2026-01-01T09:00:00.000000Z", "acct-1", 1.0);
                insertAccount(job, "2026-01-01T09:00:10.000000Z", "acct-2", 2.0);
                insertAccount(job, "2026-01-01T09:00:20.000000Z", "acct-3", 3.0);
                insertAccount(job, "2026-01-02T09:00:00.000000Z", "acct-4", 4.0);
                insertAccount(job, "2026-01-03T09:00:00.000000Z", "acct-5", 5.0);

                final LiveViewWindow window = window();
                Assert.assertTrue("the sweep must have fired", window.getCompactionCount() > 0);
                // The sweep rebuilt one map, and the accumulators rode across inside the
                // entries it kept. No grouped function had a second map to prune.
                Assert.assertEquals(2, window.getAnchorMapSize());
                for (int i = 0, n = window.getCheckpointWindowStatePlan().getProjectionCount(); i < n; i++) {
                    final Map map = window.getCheckpointWindowStatePlan().getProjectionFunction(i).getPartitionMap();
                    Assert.assertFalse("a fused projection keeps no map to sweep", map.isOpen());
                }
                assertViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testAGroupedProjectionKeepsNoPrivateStateAndStillEmitsTheRightValues() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // sum, avg and count over one column: one accumulator, three read-only
            // projections of it, and the derived count reads the host's counter slot.
            execute("create live view lv flush every 100ms start from beginning as "
                    + "select created_at, cod_acct_no, sum(amt_txn) over w as s, "
                    + "avg(amt_txn) over w as a, count(amt_txn) over w as c "
                    + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insertAccount(job, DAILY_ANCHOR + "09:00:00.000000Z", "acct-1", 5.0);
                insertAccount(job, DAILY_ANCHOR + "09:00:10.000000Z", "acct-2", 7.0);
                insertAccount(job, DAILY_ANCHOR + "09:00:20.000000Z", "acct-1", 11.0);
                // A null contributes to neither the sum nor the counter, which is what
                // lets one counter serve all three.
                insertAccount(job, DAILY_ANCHOR + "09:00:30.000000Z", "acct-1", null);
                // A bucket crossing, so the component is reset in place under a new anchor.
                insertAccount(job, "2026-01-02T09:00:00.000000Z", "acct-1", 3.0);

                final LiveViewWindowStatePlan plan = window().getCheckpointWindowStatePlan();
                Assert.assertNotNull(plan);
                Assert.assertEquals("one accumulator serves all three calls", 1, plan.getComponentCount());
                Assert.assertEquals(3, plan.getProjectionCount());
                for (int i = 0; i < 3; i++) {
                    final WindowFunction function = plan.getProjectionFunction(i);
                    Assert.assertTrue("projection " + i + " must be fused", function.isWindowStateOwned());
                    Assert.assertFalse(
                            "a fused projection must never allocate its private map",
                            function.getPartitionMap().isOpen()
                    );
                }
                assertDerivedViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testALegacyHeadRestoresItsPerFunctionRootsIntoTheFusedValue() throws Exception {
        assertMemoryLeak(() -> {
            createTargetView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insertAccount(job, DAILY_ANCHOR + "09:00:00.000000Z", "acct-1", 5.0);

                final LiveViewWindow window = window();
                final LiveViewWindowStatePlan plan = window.getCheckpointWindowStatePlan();
                Assert.assertNotNull(plan);

                // A head from before the fused root existed: one anchor root and one root
                // per function. The build with no plan to persist is what writes it.
                Assert.assertFalse(window.bindCheckpointWindowStatePlan(null));
                insertAccount(job, DAILY_ANCHOR + "09:00:10.000000Z", "acct-2", 7.0);
                insertAccount(job, DAILY_ANCHOR + "09:00:20.000000Z", "acct-1", 11.0);

                // Upgrade: the runtime adopts the plan while the durable head is still
                // the legacy shape. Restoring it now has to walk each per-function root
                // into the private map the function owns outside a group and hoist the
                // result into the window's fused value - there is nowhere else for it to
                // go, and dropping it would restart every account's sum at zero.
                Assert.assertTrue(window.bindCheckpointWindowStatePlan(plan));
                final byte[] before = snapshotWindow(window);
                restoreHead();
                Assert.assertArrayEquals(
                        "the legacy head must restore into the fused value",
                        before,
                        snapshotWindow(window)
                );
                for (int i = 0, n = plan.getProjectionCount(); i < n; i++) {
                    Assert.assertFalse(
                            "the adapter must hand the private maps back after hoisting",
                            plan.getProjectionFunction(i).getPartitionMap().isOpen()
                    );
                }

                // And the next seal converts, leaving a head that restores on its own.
                insertAccount(job, DAILY_ANCHOR + "09:00:30.000000Z", "acct-1", 2.0);
                final byte[] converted = snapshotWindow(window);
                restoreHead();
                Assert.assertArrayEquals(
                        "the converted head must restore independently",
                        converted,
                        snapshotWindow(window)
                );
                assertViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testAGroupPastTheLeafBudgetFusesThePrefixAndResidualizesTheRest() throws Exception {
        assertMemoryLeak(() -> {
            // One more (sum, nonNullCount) component than the leaf budget carries beside
            // the anchor. The group used to lose the fusion whole at this point and send
            // every function back to a root of its own; it now keeps the prefix of the
            // canonical order that fits and hands one function back.
            final int fitting =
                    (LiveViewCheckpointContracts.MAX_INLINE_LEAF_STATE_BYTES - Long.BYTES) / (Double.BYTES + Long.BYTES);
            final int columns = fitting + 1;
            final StringBuilder ddl = new StringBuilder();
            final StringBuilder projections = new StringBuilder();
            for (int i = 1; i <= columns; i++) {
                ddl.append(", q").append(i).append(" double");
                projections.append(", sum(q").append(i).append(") over w as s").append(i);
            }
            execute("create table tx (created_at timestamp, cod_acct_no symbol" + ddl + ") "
                    + "timestamp(created_at) partition by hour wal");
            execute("create live view lv flush every 100ms start from beginning as "
                    + "select created_at, cod_acct_no" + projections + " from tx "
                    + "window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insertWideAccount(job, DAILY_ANCHOR + "09:00:00.000000Z", "acct-1", columns);
                insertWideAccount(job, DAILY_ANCHOR + "09:00:10.000000Z", "acct-2", columns);
                insertWideAccount(job, DAILY_ANCHOR + "09:00:20.000000Z", "acct-1", columns);
                // A bucket crossing, so the fused prefix resets in place and the residual
                // resets through its own map.
                insertWideAccount(job, "2026-01-02T09:00:00.000000Z", "acct-1", columns);

                final LiveViewWindowStatePlan plan = window().getCheckpointWindowStatePlan();
                Assert.assertNotNull(plan);
                Assert.assertEquals(fitting, plan.getComponentCount());
                Assert.assertEquals(fitting, plan.getProjectionCount());
                Assert.assertEquals(1, plan.getResidualFunctions().size());
                Assert.assertEquals(
                        Long.BYTES + fitting * (Double.BYTES + Long.BYTES),
                        plan.getTotalInlineStateBytes()
                );
                for (int i = 0; i < fitting; i++) {
                    Assert.assertTrue(plan.getProjectionFunction(i).isWindowStateOwned());
                }
                Assert.assertFalse(plan.getResidualFunctions().getQuick(0).isWindowStateOwned());
                assertWideViewMatchesRecompute(columns);

                // And the head that seals a truncated group beside its residual restores
                // on its own.
                final byte[] before = snapshotWindow(window());
                restoreHead();
                Assert.assertArrayEquals(before, snapshotWindow(window()));
                assertWideViewMatchesRecompute(columns);
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testADecimalArgumentFusesItsCountAndLeavesItsSumOnItsOwnRoot() throws Exception {
        assertMemoryLeak(() -> {
            // DECIMAL has window factories of its own and never widens into the DOUBLE
            // ones, so a DECIMAL sum and avg keep accumulators - a Decimal256 beside a
            // null-state flag, and one beside a counter - that the component families do
            // not describe. Both stay residual and keep their own maps. The count over the
            // same column is a different matter: it is the shared counting implementation
            // under this width's null test, so it joins the group and the window owns it.
            // The recompute below runs the unfused implementations of all three, so a
            // count that counted different rows from its own factory would surface here.
            execute("create table tx (created_at timestamp, cod_acct_no symbol, amt_txn decimal(38,2)) "
                    + "timestamp(created_at) partition by hour wal");
            execute("create live view lv flush every 100ms start from beginning as "
                    + "select created_at, cod_acct_no, sum(amt_txn) over w as s, "
                    + "avg(amt_txn) over w as a, count(amt_txn) over w as c "
                    + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insertDecimalAccount(job, DAILY_ANCHOR + "09:00:00.000000Z", "acct-1", "5.25");
                insertDecimalAccount(job, DAILY_ANCHOR + "09:00:10.000000Z", "acct-2", "7.50");
                insertDecimalAccount(job, DAILY_ANCHOR + "09:00:20.000000Z", "acct-1", "11.75");
                // A null joins neither the accumulators nor the counter.
                insertDecimalAccount(job, DAILY_ANCHOR + "09:00:30.000000Z", "acct-1", null);
                // A bucket crossing, so the fused counter is reset in place while the two
                // residuals reset through their own maps.
                insertDecimalAccount(job, "2026-01-02T09:00:00.000000Z", "acct-1", "3.00");

                final LiveViewWindowStatePlan plan = window().getCheckpointWindowStatePlan();
                Assert.assertNotNull(plan);
                Assert.assertEquals("only the count is a component", 1, plan.getComponentCount());
                Assert.assertEquals(1, plan.getProjectionCount());
                Assert.assertEquals("sum and avg stay residual", 2, plan.getResidualFunctions().size());
                Assert.assertEquals(
                        LiveViewAccumulatorDescriptor.CONTRIBUTION_TYPED_NOT_NULL,
                        plan.getComponent(0).getContributionKind()
                );
                Assert.assertTrue(plan.getProjectionFunction(0).isWindowStateOwned());
                for (int i = 0; i < 2; i++) {
                    final WindowFunction residual = plan.getResidualFunctions().getQuick(i);
                    Assert.assertFalse(residual.isWindowStateOwned());
                    // Their whole image is fixed width and fits the per-component budget,
                    // so the seal carries it in the leaf instead of a data page.
                    Assert.assertTrue(
                            residual.getName() + " must inline its image",
                            LiveViewCheckpointContracts.isInlineableStateLength(residual.checkpointStateFixedLength())
                    );
                }
                assertDerivedViewMatchesRecompute();

                // And the head that seals a fused counter beside two inline residual roots
                // restores on its own.
                final byte[] before = snapshotWindow(window());
                restoreHead();
                Assert.assertArrayEquals(before, snapshotWindow(window()));
                assertDerivedViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testAnIntegralArgumentSumsAndCountsExactlyTheSameRows() throws Exception {
        assertMemoryLeak(() -> {
            // A LONG column reaches sum(D), avg(D) and count(D) by widening, so the
            // three share one accumulator here as they do over a DOUBLE column. What
            // the sharing rests on is that the widening carries the null across:
            // LongFunction.getDouble answers NaN for LONG NULL, so such a row joins
            // neither the sum nor the counter, and the folded count reads the counter
            // the sum keeps. The recompute below runs the unfused implementations, so
            // a predicate that differed between the two would surface as a mismatch.
            execute("create table tx (created_at timestamp, cod_acct_no symbol, amt_txn long) "
                    + "timestamp(created_at) partition by hour wal");
            execute("create live view lv flush every 100ms start from beginning as "
                    + "select created_at, cod_acct_no, sum(amt_txn) over w as s, "
                    + "avg(amt_txn) over w as a, count(amt_txn) over w as c "
                    + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insertAccount(job, DAILY_ANCHOR + "09:00:00.000000Z", "acct-1", 5L);
                insertAccount(job, DAILY_ANCHOR + "09:00:10.000000Z", "acct-2", 7L);
                insertAccount(job, DAILY_ANCHOR + "09:00:20.000000Z", "acct-1", 11L);
                insertAccount(job, DAILY_ANCHOR + "09:00:30.000000Z", "acct-1", null);
                // The largest value a LONG carries beside its null sentinel, so a
                // widening that confused the two would move the sum and not only the
                // count.
                insertAccount(job, DAILY_ANCHOR + "09:00:40.000000Z", "acct-2", Long.MAX_VALUE);
                // A bucket crossing, so the accumulator is reset in place.
                insertAccount(job, "2026-01-02T09:00:00.000000Z", "acct-1", 3L);

                final LiveViewWindowStatePlan plan = window().getCheckpointWindowStatePlan();
                Assert.assertNotNull(plan);
                Assert.assertEquals("one accumulator serves all three calls", 1, plan.getComponentCount());
                Assert.assertEquals(3, plan.getProjectionCount());
                // Keyed by the column's own type: the widening is what the shared
                // predicate was proved through, so it is part of the identity.
                Assert.assertEquals(ColumnType.LONG, plan.getComponent(0).getArgumentColumnType());
                for (int i = 0; i < 3; i++) {
                    final WindowFunction function = plan.getProjectionFunction(i);
                    Assert.assertTrue("projection " + i + " must be fused", function.isWindowStateOwned());
                    Assert.assertFalse(
                            "a fused projection must never allocate its private map",
                            function.getPartitionMap().isOpen()
                    );
                }
                assertDerivedViewMatchesRecompute();

                // And the head that seals it restores on its own.
                final byte[] before = snapshotWindow(window());
                restoreHead();
                Assert.assertArrayEquals(before, snapshotWindow(window()));
                assertDerivedViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testTheComponentCodecWritesExactlyTheContributorsImage() throws Exception {
        assertMemoryLeak(() -> {
            // The claim the fused leaf rests on: what the descriptor packs into the
            // payload at the manifest's offset is byte-for-byte what the contributing
            // function's own freezeCheckpointState would have written into a state page.
            // The two are separate implementations of one codec, and a leaf carries no
            // length that would catch them diverging.
            // One view naming every family the plan admits, so each new family is held to
            // the claim rather than only the two the target shape happens to use. The
            // non-null count is over a column the window does not partition by, which is
            // what keeps it a component of its own beside the row count: a count over the
            // partition key would read that row count instead.
            execute("create table tx (created_at timestamp, cod_acct_no symbol, br_code symbol, amt_txn double) "
                    + "timestamp(created_at) partition by hour wal");
            execute("create live view lv flush every 100ms start from beginning as "
                    + "select created_at, cod_acct_no, sum(amt_txn) over w as s, "
                    + "count(br_code) over w as c, count(*) over w as r, "
                    + "stddev_samp(amt_txn) over w as sd "
                    + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insertBranchAccount(job, DAILY_ANCHOR + "09:00:00.000000Z", "acct-1", "br-1", 5.0);
                insertBranchAccount(job, DAILY_ANCHOR + "09:00:10.000000Z", "acct-1", "br-2", 11.0);

                final LiveViewWindowStatePlan plan = window().getCheckpointWindowStatePlan();
                Assert.assertNotNull(plan);
                Assert.assertEquals(
                        "the view must produce one component per family",
                        4,
                        plan.getComponentCount()
                );
                for (int c = 0, n = plan.getComponentCount(); c < n; c++) {
                    assertComponentCodecMatchesContributor(plan, c);
                }
            }
        });
    }

    @Test
    public void testACountOverThePartitionKeyReadsTheRowCountAndAnswersZeroForTheNullKey() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // Every row of a partition carries the same account, so count(cod_acct_no) is
            // the partition's row count wherever the account is present. It therefore reads
            // the counter count(*) already keeps rather than persisting a second one - one
            // component and a 16-byte entry where the two used to cost 24.
            //
            // The NULL-account partition is what the guard is for, and it is the whole of
            // the difference between the two outputs: count(*) counts its rows and
            // count(cod_acct_no) counts none of them. The recompute below runs the unfused
            // implementations of both, so a guard that fired on the wrong partitions - or
            // never - would surface there.
            execute("create live view lv flush every 100ms start from beginning as "
                    + "select created_at, cod_acct_no, count(*) over w as r, "
                    + "count(cod_acct_no) over w as c "
                    + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insertAccount(job, DAILY_ANCHOR + "09:00:00.000000Z", "acct-1", 5.0);
                insertAccount(job, DAILY_ANCHOR + "09:00:10.000000Z", "acct-2", 7.0);
                insertAccount(job, DAILY_ANCHOR + "09:00:20.000000Z", "acct-1", 11.0);
                // The NULL-key partition, twice, so its row count runs ahead of its count
                // rather than merely differing on the first row.
                insertAccount(job, DAILY_ANCHOR + "09:00:30.000000Z", null, 3.0);
                insertAccount(job, DAILY_ANCHOR + "09:00:40.000000Z", null, 4.0);
                // A bucket crossing, so the shared counter resets in place under both.
                insertAccount(job, "2026-01-02T09:00:00.000000Z", "acct-1", 3.0);
                insertAccount(job, "2026-01-02T09:00:10.000000Z", null, 6.0);

                final LiveViewWindow window = window();
                final LiveViewWindowStatePlan plan = window.getCheckpointWindowStatePlan();
                Assert.assertNotNull(plan);
                Assert.assertEquals("one counter serves both calls", 1, plan.getComponentCount());
                Assert.assertEquals(2, plan.getProjectionCount());
                Assert.assertEquals(
                        LiveViewAccumulatorDescriptor.FAMILY_ROW_COUNT,
                        plan.getComponent(0).getFamily()
                );
                Assert.assertEquals(Long.BYTES + Long.BYTES, plan.getTotalInlineStateBytes());
                Assert.assertFalse("count(*) reads the slot straight", plan.getProjection(0).isPartitionKeyGuarded());
                Assert.assertTrue("count(k) corrects it", plan.getProjection(1).isPartitionKeyGuarded());
                // The guarded call keeps the partition's count where the component keeps
                // its row count, so it must never be the one that maintains the component.
                Assert.assertSame(plan.getProjectionFunction(0), plan.getContributor(0));
                for (int i = 0; i < 2; i++) {
                    final WindowFunction function = plan.getProjectionFunction(i);
                    Assert.assertTrue("projection " + i + " must be fused", function.isWindowStateOwned());
                    Assert.assertFalse(
                            "a fused projection must never allocate its private map",
                            function.getPartitionMap().isOpen()
                    );
                }
                assertPartitionKeyCountViewMatchesRecompute();

                // The head that seals one counter for two different outputs restores on its
                // own, NULL-key partition included.
                final byte[] before = snapshotWindow(window);
                restoreHead();
                Assert.assertArrayEquals(before, snapshotWindow(window));
                assertPartitionKeyCountViewMatchesRecompute();

                // Handing the state back is the one place the guard has no base row to read
                // and takes the entry's own key instead. A row count copied through
                // unguarded would leave the NULL-account partition counting rows it never
                // counted, which the next row's output would carry forward.
                Assert.assertFalse(window.bindCheckpointWindowStatePlan(null));
                insertAccount(job, "2026-01-02T09:00:20.000000Z", null, 7.0);
                insertAccount(job, "2026-01-02T09:00:30.000000Z", "acct-1", 8.0);
                assertPartitionKeyCountViewMatchesRecompute();

                Assert.assertTrue(window.bindCheckpointWindowStatePlan(plan));
                insertAccount(job, "2026-01-02T09:00:40.000000Z", null, 9.0);
                assertPartitionKeyCountViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testARowCountServesCountStarAndRowNumberFromOneSlot() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            execute("create live view lv flush every 100ms start from beginning as "
                    + "select created_at, cod_acct_no, count(*) over w as c, row_number() over w as rn "
                    + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insertAccount(job, DAILY_ANCHOR + "09:00:00.000000Z", "acct-1", 5.0);
                insertAccount(job, DAILY_ANCHOR + "09:00:10.000000Z", "acct-2", 7.0);
                insertAccount(job, DAILY_ANCHOR + "09:00:20.000000Z", "acct-1", 11.0);
                // A null argument still counts: neither call reads the column at all.
                insertAccount(job, DAILY_ANCHOR + "09:00:30.000000Z", "acct-1", null);
                // Two rows of one account at one timestamp. This is where a peer-inclusive
                // count would run ahead of the row number and the shared counter would be
                // wrong for one of them; QuestDB's count over an unbounded frame stops at
                // the current row instead, which is what makes the two one accumulator.
                insertAccount(job, DAILY_ANCHOR + "09:00:35.000000Z", "acct-1", 1.0);
                insertAccount(job, DAILY_ANCHOR + "09:00:35.000000Z", "acct-1", 2.0);
                // A bucket crossing, so the counter is reset in place under a new anchor
                // and row_number starts over at 1 with it.
                insertAccount(job, "2026-01-02T09:00:00.000000Z", "acct-1", 3.0);

                final LiveViewWindowStatePlan plan = window().getCheckpointWindowStatePlan();
                Assert.assertNotNull(plan);
                Assert.assertEquals("one counter serves both calls", 1, plan.getComponentCount());
                Assert.assertEquals(2, plan.getProjectionCount());
                for (int i = 0; i < 2; i++) {
                    final WindowFunction function = plan.getProjectionFunction(i);
                    Assert.assertTrue("projection " + i + " must be fused", function.isWindowStateOwned());
                    Assert.assertFalse(
                            "a fused projection must never allocate its private map",
                            function.getPartitionMap().isOpen()
                    );
                }
                assertRowCountViewMatchesRecompute();
                assertRowCountsAgree();

                // And the head that seals it restores on its own.
                final byte[] before = snapshotWindow(window());
                restoreHead();
                Assert.assertArrayEquals(before, snapshotWindow(window()));
                assertRowCountViewMatchesRecompute();
                assertRowCountsAgree();
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testOneWelfordAccumulatorServesEveryDispersionCall() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            execute("create live view lv flush every 100ms start from beginning as "
                    + "select created_at, cod_acct_no, stddev_samp(amt_txn) over w as ss, "
                    + "stddev_pop(amt_txn) over w as sp, var_samp(amt_txn) over w as vs, "
                    + "var_pop(amt_txn) over w as vp, count(amt_txn) over w as c "
                    + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insertAccount(job, DAILY_ANCHOR + "09:00:00.000000Z", "acct-1", 5.0);
                insertAccount(job, DAILY_ANCHOR + "09:00:10.000000Z", "acct-2", 7.0);
                insertAccount(job, DAILY_ANCHOR + "09:00:20.000000Z", "acct-1", 11.0);
                // A single-row partition, where the sample forms are NULL and the
                // population ones are zero - the arithmetic that separates the four.
                insertAccount(job, DAILY_ANCHOR + "09:00:25.000000Z", "acct-3", 4.0);
                // A null contributes to neither the dispersion nor the counter, which is
                // what lets the folded count read Welford's own.
                insertAccount(job, DAILY_ANCHOR + "09:00:30.000000Z", "acct-1", null);
                insertAccount(job, DAILY_ANCHOR + "09:00:40.000000Z", "acct-1", 2.0);
                // A bucket crossing, so the accumulator is reset in place under a new
                // anchor rather than carried across it.
                insertAccount(job, "2026-01-02T09:00:00.000000Z", "acct-1", 3.0);

                final LiveViewWindowStatePlan plan = window().getCheckpointWindowStatePlan();
                Assert.assertNotNull(plan);
                Assert.assertEquals("one accumulator serves all five calls", 1, plan.getComponentCount());
                Assert.assertEquals(5, plan.getProjectionCount());
                for (int i = 0; i < 5; i++) {
                    final WindowFunction function = plan.getProjectionFunction(i);
                    Assert.assertTrue("projection " + i + " must be fused", function.isWindowStateOwned());
                    Assert.assertFalse(
                            "a fused projection must never allocate its private map",
                            function.getPartitionMap().isOpen()
                    );
                }
                assertWelfordViewMatchesRecompute();

                final byte[] before = snapshotWindow(window());
                restoreHead();
                Assert.assertArrayEquals(before, snapshotWindow(window()));
                assertWelfordViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    /**
     * The window's whole runtime state as bytes - key, anchor value and every fused
     * component per partition - which is what a restore has to reproduce.
     */
    private static byte[] snapshotWindow(LiveViewWindow window) {
        try (MemoryCARW sink = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            window.snapshot(sink);
            final int length = (int) sink.getAppendOffset();
            final byte[] out = new byte[length];
            for (int i = 0; i < length; i++) {
                out[i] = sink.getByte(i);
            }
            return out;
        }
    }

    private static ObjList<WindowFunction> unwrapWindowFunctions(LiveViewInstance instance) {
        RecordCursorFactory factory = instance.getCompiledFactory();
        while (factory != null) {
            if (factory instanceof WindowRecordCursorFactory windowFactory) {
                return windowFactory.getWindowFunctions();
            }
            if (factory instanceof QueryProgress) {
                factory = factory.getBaseFactory();
                continue;
            }
            break;
        }
        throw new IllegalStateException("compiled factory does not contain a WindowRecordCursorFactory");
    }

    /**
     * Fills every one of a component's slots with a distinct value, freezes it twice -
     * once through the component descriptor's payload codec and once through the
     * contributing function's own state-page writer - and requires the two images to be
     * identical, then round-trips the descriptor's own decoder over them.
     * <p>
     * Slot by slot rather than field by field, so a family this case has never heard of
     * is still covered the day it is added: the two implementations have to agree on
     * every byte of the image, not only on the ones a named getter reaches.
     */
    private void assertComponentCodecMatchesContributor(LiveViewWindowStatePlan plan, int componentIndex) {
        final LiveViewAccumulatorDescriptor component = plan.getComponent(componentIndex);
        final WindowFunction contributor = plan.getContributor(componentIndex);
        final int slotCount = component.getSlotCount();
        final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        for (int i = 0; i < slotCount; i++) {
            valueTypes.add(component.getSlotColumnType(i));
        }
        final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.LONG);
        Map scratch = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes);
        try (MemoryCARW sink = Vm.getCARWInstance(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            final MapKey key = scratch.withKey();
            key.putLong(1);
            final MapValue value = key.createValue();
            // Distinct per slot, so a codec that transposed two fields of the same width
            // would fail rather than pass on equal bytes.
            for (int i = 0; i < slotCount; i++) {
                if (component.getSlotColumnType(i) == ColumnType.DOUBLE) {
                    value.putDouble(i, 17.5 + i);
                } else {
                    value.putLong(i, 3L + i);
                }
            }

            final byte[] fromDescriptor = new byte[component.getStateLength()];
            component.freezeStateInto(value, 0, fromDescriptor, 0);

            final long emitted = new LiveViewStatePageWriter().of(sink).freeze(contributor, value);
            Assert.assertEquals(
                    "the contributor's image is the component's width",
                    component.getStateLength(),
                    emitted
            );
            final byte[] fromContributor = new byte[(int) emitted];
            for (int i = 0; i < emitted; i++) {
                fromContributor[i] = sink.getByte(i);
            }
            Assert.assertArrayEquals(
                    "component " + componentIndex + " codec differs from its contributor's",
                    fromContributor,
                    fromDescriptor
            );

            // And the inverse puts the same numbers back into the same slots.
            for (int i = 0; i < slotCount; i++) {
                if (component.getSlotColumnType(i) == ColumnType.DOUBLE) {
                    value.putDouble(i, 0.0);
                } else {
                    value.putLong(i, 0L);
                }
            }
            component.restoreStateFrom(fromDescriptor, 0, value, 0);
            for (int i = 0; i < slotCount; i++) {
                if (component.getSlotColumnType(i) == ColumnType.DOUBLE) {
                    Assert.assertEquals(17.5 + i, value.getDouble(i), 0.0);
                } else {
                    Assert.assertEquals(3L + i, value.getLong(i));
                }
            }
        } finally {
            Misc.free(scratch);
        }
    }

    /**
     * The {@link #assertViewMatchesRecompute()} counterpart for the three-call shape.
     */
    private void assertDerivedViewMatchesRecompute() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String frame = "over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(select created_at, cod_acct_no, "
                        + "sum(amt_txn) " + frame + " as s, "
                        + "avg(amt_txn) " + frame + " as a, "
                        + "count(amt_txn) " + frame + " as c "
                        + "from (select created_at, cod_acct_no, amt_txn, " + bucket + " as bucket from tx)"
                        + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
    }

    /**
     * The claim the shared row counter rests on, read off the view itself: every row's
     * {@code count(*)} equals its {@code row_number()}. Comparing the two output columns
     * to each other rather than to an oracle is what makes the check independent of which
     * of two rows at one timestamp came first - and tied timestamps are the only place the
     * two could ever have disagreed.
     */
    private void assertRowCountsAgree() throws Exception {
        assertQuery("select count(*) as disagreeing from lv where c != rn")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("disagreeing\n0\n");
    }

    /**
     * The {@link #assertViewMatchesRecompute()} counterpart for the shape whose count is
     * over the window's own partition key. The two output columns differ on exactly one
     * partition - the NULL-account one - which is what the guard has to produce.
     */
    private void assertPartitionKeyCountViewMatchesRecompute() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String frame = "over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(select created_at, cod_acct_no, "
                        + "count(*) " + frame + " as r, "
                        + "count(cod_acct_no) " + frame + " as c "
                        + "from (select created_at, cod_acct_no, amt_txn, " + bucket + " as bucket from tx)"
                        + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
    }

    /**
     * The {@link #assertViewMatchesRecompute()} counterpart for the row-count shape.
     */
    private void assertRowCountViewMatchesRecompute() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String frame = "over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(select created_at, cod_acct_no, "
                        + "count(*) " + frame + " as c, "
                        + "row_number() over (partition by cod_acct_no, bucket order by created_at) as rn "
                        + "from (select created_at, cod_acct_no, amt_txn, " + bucket + " as bucket from tx)"
                        + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
    }

    /**
     * The {@link #assertViewMatchesRecompute()} counterpart for the Welford shape.
     */
    private void assertWelfordViewMatchesRecompute() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String frame = "over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(select created_at, cod_acct_no, "
                        + "stddev_samp(amt_txn) " + frame + " as ss, "
                        + "stddev_pop(amt_txn) " + frame + " as sp, "
                        + "var_samp(amt_txn) " + frame + " as vs, "
                        + "var_pop(amt_txn) " + frame + " as vp, "
                        + "count(amt_txn) " + frame + " as c "
                        + "from (select created_at, cod_acct_no, amt_txn, " + bucket + " as bucket from tx)"
                        + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
    }

    /**
     * Compares the view against a from-base recompute of the same window. ANCHOR is
     * live-view syntax, so the daily bucket is written out as an ordinary partition term.
     */
    private void assertViewMatchesRecompute() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(select created_at, cod_acct_no, "
                        + "sum(amt_txn) over (partition by cod_acct_no, bucket order by created_at "
                        + "rows between unbounded preceding and current row) as cumulative_sum, "
                        + "count(cod_acct_no) over (partition by cod_acct_no, bucket order by created_at "
                        + "rows between unbounded preceding and current row) as cumulative_count "
                        + "from (select created_at, cod_acct_no, amt_txn, " + bucket + " as bucket from tx)"
                        + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
    }

    private void createBaseTable() throws Exception {
        execute("create table tx (created_at timestamp, cod_acct_no symbol, amt_txn double) "
                + "timestamp(created_at) partition by hour wal");
    }

    private void createTargetView() throws Exception {
        createBaseTable();
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, cod_acct_no, sum(amt_txn) over w as cumulative_sum, "
                + "count(cod_acct_no) over w as cumulative_count "
                + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
    }

    /**
     * Appends one row and drives the view to quiescence. The amount is a {@link Number}
     * rather than a {@code Double} because the amount column's type is a case's to
     * choose - a DOUBLE for most of them, a LONG for the one that covers widening - and
     * the literal is written out the same way either way.
     */
    private void insertAccount(LiveViewRefreshJob job, String timestamp, String account, Number amount) throws Exception {
        execute("insert into tx values ('" + timestamp + "', "
                + (account == null ? "null" : "'" + account + "'") + ", "
                + (amount == null ? "null" : amount.toString()) + ")");
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    /**
     * Appends one row of the four-column base a case needs when it wants a SYMBOL the
     * window does not partition by.
     */
    private void insertBranchAccount(
            LiveViewRefreshJob job,
            String timestamp,
            String account,
            String branch,
            Number amount
    ) throws Exception {
        execute("insert into tx values ('" + timestamp + "', '" + account + "', "
                + (branch == null ? "null" : "'" + branch + "'") + ", "
                + (amount == null ? "null" : amount.toString()) + ")");
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    /**
     * Compares the first fused output and the residual one against a from-base recompute.
     * The two are the ends of the truncated group: one is read off the window's own value
     * and one off the function's private map, and both have to agree with the unfused
     * implementation.
     */
    private void assertWideViewMatchesRecompute(int columns) throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String frame = "over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(select created_at, cod_acct_no, "
                        + "sum(q1) " + frame + " as s1, "
                        + "sum(q" + columns + ") " + frame + " as sn "
                        + "from (select *, " + bucket + " as bucket from tx)) order by 2, 1",
                "(select created_at, cod_acct_no, s1, s" + columns + " as sn from lv) order by 2, 1",
                LOG,
                true
        );
    }

    /**
     * Inserts one row whose amount is a DECIMAL. A bare numeric literal is a DOUBLE and
     * does not convert into a DECIMAL column, so the value carries its own cast.
     */
    private void insertDecimalAccount(LiveViewRefreshJob job, String timestamp, String account, String amount)
            throws Exception {
        execute("insert into tx values ('" + timestamp + "', '" + account + "', "
                + (amount == null ? "null" : amount) + "::decimal(38,2))");
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    /**
     * Inserts one row into the wide base table, giving column {@code qk} the value
     * {@code k} so a partition's running sums are predictable and distinct per column.
     */
    private void insertWideAccount(LiveViewRefreshJob job, String timestamp, String account, int columns)
            throws Exception {
        final StringBuilder values = new StringBuilder();
        for (int i = 1; i <= columns; i++) {
            values.append(", ").append(i).append(".0");
        }
        execute("insert into tx values ('" + timestamp + "', '" + account + "'" + values + ")");
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private LiveViewInstance instance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    /**
     * Restores the published head over the live runtime, through the same reader the
     * refresh worker uses after a restart.
     */
    private void restoreHead() {
        final LiveViewInstance instance = instance();
        try (
                Path checkpointsDir = checkpointsDir(instance);
                LiveViewCheckpointTimelineStoreReader reader =
                        new LiveViewCheckpointTimelineStoreReader(configuration)
        ) {
            reader.of(checkpointsDir);
            reader.restoreLatest(
                    instance.getLiveViewToken().getTableId(),
                    unwrapWindowFunctions(instance),
                    instance.getAnchorWindow()
            );
        }
    }

    private LiveViewWindow window() {
        final LiveViewInstance instance = instance();
        // Touch the compiled factory so a case that never unwraps it still fails loudly
        // if the view compiled no window at all.
        Assert.assertTrue(unwrapWindowFunctions(instance).size() > 0);
        final LiveViewWindow window = instance.getAnchorWindow();
        Assert.assertNotNull("the anchored view must have built its window", window);
        return window;
    }
}
