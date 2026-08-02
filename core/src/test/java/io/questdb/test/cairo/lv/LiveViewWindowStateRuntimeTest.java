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
    public void testTheComponentCodecWritesExactlyTheContributorsImage() throws Exception {
        assertMemoryLeak(() -> {
            // The claim the fused leaf rests on: what the descriptor packs into the
            // payload at the manifest's offset is byte-for-byte what the contributing
            // function's own freezeCheckpointState would have written into a state page.
            // The two are separate implementations of one codec, and a leaf carries no
            // length that would catch them diverging.
            createTargetView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insertAccount(job, DAILY_ANCHOR + "09:00:00.000000Z", "acct-1", 5.0);
                insertAccount(job, DAILY_ANCHOR + "09:00:10.000000Z", "acct-1", 11.0);

                final LiveViewWindowStatePlan plan = window().getCheckpointWindowStatePlan();
                Assert.assertNotNull(plan);
                for (int c = 0, n = plan.getComponentCount(); c < n; c++) {
                    assertComponentCodecMatchesContributor(plan, c, 17.5, 3);
                }
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
     * Fills one scratch map value with {@code sum} and {@code count}, freezes it twice -
     * once through the component descriptor's payload codec and once through the
     * contributing function's own state-page writer - and requires the two images to be
     * identical.
     */
    private void assertComponentCodecMatchesContributor(
            LiveViewWindowStatePlan plan,
            int componentIndex,
            double sum,
            long count
    ) {
        final LiveViewAccumulatorDescriptor component = plan.getComponent(componentIndex);
        final WindowFunction contributor = plan.getContributor(componentIndex);
        final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        for (int i = 0, n = component.getSlotCount(); i < n; i++) {
            valueTypes.add(component.getSlotColumnType(i));
        }
        final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.LONG);
        Map scratch = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes);
        try (MemoryCARW sink = Vm.getCARWInstance(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            final MapKey key = scratch.withKey();
            key.putLong(1);
            final MapValue value = key.createValue();
            final int sumSlot = component.getFieldSlot(LiveViewAccumulatorDescriptor.FIELD_SUM);
            if (sumSlot >= 0) {
                value.putDouble(sumSlot, sum);
            }
            value.putLong(component.getFieldSlot(LiveViewAccumulatorDescriptor.FIELD_NON_NULL_COUNT), count);

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

            // And the inverse puts the same numbers back into the slots.
            value.putLong(component.getFieldSlot(LiveViewAccumulatorDescriptor.FIELD_NON_NULL_COUNT), 0);
            if (sumSlot >= 0) {
                value.putDouble(sumSlot, 0);
            }
            component.restoreStateFrom(fromDescriptor, 0, value, 0);
            if (sumSlot >= 0) {
                Assert.assertEquals(sum, value.getDouble(sumSlot), 0.0);
            }
            Assert.assertEquals(
                    count,
                    value.getLong(component.getFieldSlot(LiveViewAccumulatorDescriptor.FIELD_NON_NULL_COUNT))
            );
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

    private void insertAccount(LiveViewRefreshJob job, String timestamp, String account, Double amount) throws Exception {
        execute("insert into tx values ('" + timestamp + "', '" + account + "', "
                + (amount == null ? "null" : amount.toString()) + ")");
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
