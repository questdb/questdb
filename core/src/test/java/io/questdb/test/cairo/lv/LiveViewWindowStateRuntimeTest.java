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
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowAccumulatorDescriptor;
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

import java.util.ArrayList;
import java.util.Collections;

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
    public void testAGroupPastTheLeafBudgetKeepsEveryMemberInOneMap() throws Exception {
        assertMemoryLeak(() -> {
            // One more (sum, nonNullCount) component than the leaf budget carries beside
            // the anchor. The group used to lose the fusion whole at this point and send
            // every function back to a root of its own, then to keep the prefix and hand one
            // function back; it now keeps every member in the one map and only the leaf's
            // payload stops at the budget. The overflowing member's bytes go to the function
            // root it keeps, written out of the group.
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
                // A bucket crossing, which every member's component resets through in place -
                // the runtime-only one included, since the reset is the group's.
                insertWideAccount(job, "2026-01-02T09:00:00.000000Z", "acct-1", columns);

                final LiveViewWindowStatePlan plan = window().getCheckpointWindowStatePlan();
                Assert.assertNotNull(plan);
                Assert.assertEquals(columns, plan.getComponentCount());
                Assert.assertEquals(columns, plan.getProjectionCount());
                Assert.assertEquals(fitting, plan.getDurableComponentCount());
                Assert.assertEquals(0, plan.getResidualFunctions().size());
                Assert.assertEquals(
                        Long.BYTES + fitting * (Double.BYTES + Long.BYTES),
                        plan.getTotalInlineStateBytes()
                );
                // Every member owns no state of its own, the overflowing one included: one
                // map, one probe a row, and one private map fewer than the truncation left.
                for (int i = 0; i < columns; i++) {
                    Assert.assertTrue(plan.getProjectionFunction(i).isWindowStateOwned());
                    final Map privateMap = plan.getProjectionFunction(i).getPartitionMap();
                    Assert.assertTrue(privateMap == null || !privateMap.isOpen());
                }
                Assert.assertEquals(1, countRuntimeOnlyProjections(plan));
                assertWideViewMatchesRecompute(columns);

                // And the head that seals a truncated group restores on its own: the fused
                // root puts the durable prefix back and the runtime-only member's own root
                // puts its slice back into the same entries.
                final byte[] before = snapshotWindow(window());
                restoreHead();
                Assert.assertArrayEquals(before, snapshotWindow(window()));
                assertWideViewMatchesRecompute(columns);
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testAManyMemberSealWalksTheKeyDomainOncePerDisposition() throws Exception {
        assertSealWalksOncePerDisposition(8);
    }

    @Test
    public void testAMixedDispositionSealWalksTheKeyDomainOncePerBucket() throws Exception {
        assertMemoryLeak(() -> {
            final int runtimeOnlyMembers = 4;
            final int columns = createGroupPastTheLeafBudget(runtimeOnlyMembers);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insertWideAccount(job, DAILY_ANCHOR + "09:00:00.000000Z", "acct-1", columns);
                insertWideAccount(job, DAILY_ANCHOR + "09:00:10.000000Z", "acct-2", columns);
                insertWideAccount(job, DAILY_ANCHOR + "09:00:20.000000Z", "acct-1", columns);

                final LiveViewWindowStatePlan plan = window().getCheckpointWindowStatePlan();
                Assert.assertNotNull(plan);
                Assert.assertEquals(runtimeOnlyMembers, countRuntimeOnlyProjections(plan));

                // One member is put back on a complete freeze while its siblings stay
                // incremental. A state-format version bump does this in the field - it
                // leaves one member without a predecessor root it can build on - and the
                // disposition is what decides WHICH map the walk reads, so such a member
                // cannot share its siblings' walk. The seal must then make two member
                // walks rather than silently freezing one member off the wrong map.
                final WindowFunction odd = runtimeOnlyMember(plan);
                Assert.assertNotNull("the fixture must expose a runtime-only member", odd);
                odd.requireCheckpointFullScan();

                final long before = window().getCheckpointFreezeScanCount();
                Assert.assertTrue("the fixture must already have sealed", before > 0);
                insertWideAccount(job, DAILY_ANCHOR + "09:00:30.000000Z", "acct-1", columns);
                final long walks = window().getCheckpointFreezeScanCount() - before;

                Assert.assertEquals(
                        "a boundary whose members disagree on the incremental disposition must"
                                + " walk once for the window state and once per bucket",
                        3,
                        walks
                );
                assertWideViewMatchesRecompute(columns);

                // Both buckets' roots have to come back, which is what says the split fanned
                // each member's image into its own root rather than crossing them over.
                final byte[] snapshot = snapshotWindow(window());
                restoreHead();
                Assert.assertArrayEquals(snapshot, snapshotWindow(window()));
                assertWideViewMatchesRecompute(columns);
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testAOneMemberSealWalksTheKeyDomainOncePerDisposition() throws Exception {
        assertSealWalksOncePerDisposition(1);
    }

    @Test
    public void testARuntimeOnlyMemberSealsAndRestoresThroughTheGroup() throws Exception {
        // The member the leaf budget leaves out of the manifest keeps a root of its own, and
        // that root is written out of the group's map and read back into it. What the case
        // holds is the two halves the group now owns for it. An incremental seal names the
        // window's dirty keys, so an account no cadence touched has to survive on the
        // predecessor's entries; and the frontier sweep's removals are the window's, so a
        // key it evicts has to leave the member's root as well - a root that kept one would
        // fail the restore outright, since the window state root no longer names it.
        //
        // A lost slice restores as identity rather than as an error, so the assertion is the
        // whole runtime image before and after a restore of the published head.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_STALE_PERCENT, 50);
        assertMemoryLeak(() -> {
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
                insertWideAccount(job, DAILY_ANCHOR + "09:00:20.000000Z", "acct-3", columns);
                // One account moves in this cadence, so the seal above it names one key and
                // the other two stand on the entries the predecessor root already holds.
                insertWideAccount(job, DAILY_ANCHOR + "09:00:30.000000Z", "acct-1", columns);

                final LiveViewWindowStatePlan plan = window().getCheckpointWindowStatePlan();
                Assert.assertNotNull(plan);
                Assert.assertEquals(1, countRuntimeOnlyProjections(plan));
                final byte[] incremental = snapshotWindow(window());
                restoreHead();
                Assert.assertArrayEquals(incremental, snapshotWindow(window()));
                assertWideViewMatchesRecompute(columns);

                // Two bucket advances leave the first bucket behind the retained pair, so
                // the sweep drops its three accounts.
                insertWideAccount(job, "2026-01-02T09:00:00.000000Z", "acct-4", columns);
                insertWideAccount(job, "2026-01-03T09:00:00.000000Z", "acct-5", columns);
                // One sweep for the whole group, not one per member: the key domain the
                // members share is the window's one map, so the rebuild that drops the
                // stale bucket drops it for every component in the value at once and no
                // member has a second domain to prune.
                Assert.assertEquals("the shared key domain is compacted once", 1, window().getCompactionCount());
                Assert.assertEquals("only the two retained buckets survive", 2, window().getAnchorMapSize());
                for (int i = 0; i < columns; i++) {
                    final Map privateMap = plan.getProjectionFunction(i).getPartitionMap();
                    Assert.assertTrue(
                            "member " + i + " must keep no key domain of its own to sweep",
                            privateMap == null || !privateMap.isOpen()
                    );
                }
                final byte[] swept = snapshotWindow(window());
                restoreHead();
                Assert.assertArrayEquals(swept, snapshotWindow(window()));
                assertWideViewMatchesRecompute(columns);
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testAFusedIntKeyedAnchorMapStaysUnorderedAtTheServerEntryLimit() throws Exception {
        // The fused shape at the server's 32: the window's own slots plus one
        // (sum, count) component. This is the tightest shipping shape there is - it
        // lands exactly on the limit - so it is the one a slot added to the window's
        // prefix would push onto OrderedMap first.
        assertAnchorMapImplementation(true, 32, "Unordered4Map");
    }

    @Test
    public void testANarrowIntKeyedAnchorMapStaysUnorderedAtTheTighterEntryLimit() throws Exception {
        // The unfused shape at 16, the embedded default and the tighter of the two the
        // product ships, which the window's own value slots land exactly on. The test
        // harness defaults to 32, so the case sets it rather than inheriting it.
        assertAnchorMapImplementation(false, 16, "Unordered4Map");
    }

    @Test
    public void testDecliningThePlanEndsARuntimeOnlyMembersIncrementalBaseline() throws Exception {
        // The one member that leaves a group holding a root of its own, which is what makes
        // its baseline the group's rather than its own: while it is bound, the keys it
        // touches go into the window's dirty set and its own stands empty. A seal taken
        // after it leaves must therefore not read that empty set as "nothing moved" - the
        // root it would publish keeps the predecessor's entry for every key the group
        // touched in between, and a restart reads those back as live.
        //
        // Four rows per boundary, so the three rows in the middle move the accumulators
        // without publishing and the fourth is what seals after the decline.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 4);
        assertMemoryLeak(() -> {
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
                int second = 0;
                for (int i = 1; i <= 4; i++) {
                    insertWideAt(job, second++, "acct-" + i, columns);
                }

                final LiveViewWindow window = window();
                final LiveViewWindowStatePlan plan = window.getCheckpointWindowStatePlan();
                Assert.assertNotNull(plan);
                Assert.assertEquals(1, countRuntimeOnlyProjections(plan));
                final WindowFunction member = runtimeOnlyMember(plan);

                // Roll forward one row at a time until a boundary lands, so what follows
                // starts on a published root and on a fresh cadence. Which row seals is the
                // cadence's business rather than this case's; that one did is the case's.
                long generation = member.getCheckpointBaselineGeneration();
                while (member.getCheckpointBaselineGeneration() == generation) {
                    Assert.assertTrue("a boundary must land within a few cadences", second < 40);
                    insertWideAt(job, second++, "acct-4", columns);
                }
                generation = member.getCheckpointBaselineGeneration();
                Assert.assertFalse(
                        "the member's root must be on the boundary that just published",
                        member.isCheckpointFullScanRequired()
                );

                // Three rows that move three accounts and publish nothing. Only the window's
                // dirty set knows about them.
                insertWideAt(job, second++, "acct-1", columns);
                insertWideAt(job, second++, "acct-2", columns);
                insertWideAt(job, second++, "acct-3", columns);
                Assert.assertEquals(
                        "no boundary may have been published in between",
                        generation,
                        member.getCheckpointBaselineGeneration()
                );

                // Eligibility changes under the live frontier: every member takes its own
                // map and its own root back, carrying the accumulator the group held.
                Assert.assertFalse(window.bindCheckpointWindowStatePlan(null));

                // The fourth row of this boundary seals the runtime the decline produced,
                // and touches only one of the three accounts that moved above.
                insertWideAt(job, second, "acct-1", columns);
                Assert.assertNotEquals(
                        "the row after the decline must have sealed",
                        generation,
                        member.getCheckpointBaselineGeneration()
                );
                final String sealed = describeDeclinedGroupState(plan);
                restoreHead();
                Assert.assertEquals(
                        "the seal after the decline must publish every key rather than the ones its own "
                                + "dirty set names, which is only what moved after the decline",
                        sealed,
                        describeDeclinedGroupState(plan)
                );
                assertWideViewMatchesRecompute(columns);

                // And back the other way: adopting again must take every member's
                // accumulator into the group rather than restart it, the runtime-only one
                // included, and the row after it must keep counting from where the restore
                // left off.
                Assert.assertTrue(window.bindCheckpointWindowStatePlan(plan));
                for (int i = 0, n = plan.getProjectionCount(); i < n; i++) {
                    Assert.assertTrue(
                            "projection " + i + " must be fused again",
                            plan.getProjectionFunction(i).isWindowStateOwned()
                    );
                }
                insertWideAt(job, second + 1, "acct-2", columns);
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
                        WindowAccumulatorDescriptor.CONTRIBUTION_TYPED_NOT_NULL,
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
    public void testACompensatedTotalFusesAndKeepsTheCountItAlreadyMaintains() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // ksum keeps (sum, compensation, count) in the order its own implementation
            // stores them, so the group carries the same three slots and the count folds
            // onto the counter it ends with. What the recompute proves is that the fused
            // arithmetic is the compensated one: a projection reading a plain total out of
            // this slice would differ from it by the compensation on data that cancels,
            // which is what the magnitudes below are chosen to produce.
            execute("create live view lv flush every 100ms start from beginning as "
                    + "select created_at, cod_acct_no, ksum(amt_txn) over w as k, "
                    + "count(amt_txn) over w as c "
                    + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insertAccount(job, DAILY_ANCHOR + "09:00:00.000000Z", "acct-1", 1e16);
                insertAccount(job, DAILY_ANCHOR + "09:00:10.000000Z", "acct-1", 1.0);
                insertAccount(job, DAILY_ANCHOR + "09:00:20.000000Z", "acct-1", -1e16);
                insertAccount(job, DAILY_ANCHOR + "09:00:30.000000Z", "acct-2", 7.5);
                // A null joins neither the total nor the counter.
                insertAccount(job, DAILY_ANCHOR + "09:00:40.000000Z", "acct-1", null);
                // A bucket crossing, so the component is zeroed in place under a new anchor.
                insertAccount(job, "2026-01-02T09:00:00.000000Z", "acct-1", 3.0);

                final LiveViewWindowStatePlan plan = window().getCheckpointWindowStatePlan();
                Assert.assertNotNull(plan);
                Assert.assertEquals("the count folds onto the total's counter", 1, plan.getComponentCount());
                Assert.assertEquals(2, plan.getProjectionCount());
                Assert.assertEquals(0, plan.getResidualFunctions().size());
                Assert.assertTrue(plan.getProjection(1).isDerived());
                for (int i = 0; i < 2; i++) {
                    final WindowFunction function = plan.getProjectionFunction(i);
                    Assert.assertTrue("projection " + i + " must be fused", function.isWindowStateOwned());
                    Assert.assertFalse(
                            "a fused projection must never allocate its private map",
                            function.getPartitionMap().isOpen()
                    );
                }
                assertKahanViewMatchesRecompute();

                final byte[] before = snapshotWindow(window());
                restoreHead();
                Assert.assertArrayEquals(before, snapshotWindow(window()));
                assertKahanViewMatchesRecompute();
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testTheExtremaFuseAndCarryTheirEmptyStateBothWays() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // A running extremum has no counter to say whether a partition has contributed,
            // so its empty state is the slot's own null sentinel - NaN here - and every path
            // below has to preserve that reading rather than a flag beside it: the anchor
            // crossing that re-arms a partition, the group handing the state back, and the
            // seal that images the slot.
            execute("create live view lv flush every 100ms start from beginning as "
                    + "select created_at, cod_acct_no, max(amt_txn) over w as mx, "
                    + "min(amt_txn) over w as mn, count(amt_txn) over w as c "
                    + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insertAccount(job, DAILY_ANCHOR + "09:00:00.000000Z", "acct-1", 5.0);
                insertAccount(job, DAILY_ANCHOR + "09:00:10.000000Z", "acct-2", 7.0);
                insertAccount(job, DAILY_ANCHOR + "09:00:20.000000Z", "acct-1", 11.0);
                // An account whose only row is null contributes to neither extremum nor the
                // counter, so its whole slice stays at the identity the component resets to.
                insertAccount(job, DAILY_ANCHOR + "09:00:30.000000Z", "acct-3", null);
                insertAccount(job, DAILY_ANCHOR + "09:00:40.000000Z", "acct-1", 2.0);
                // A bucket crossing. The maximum is re-armed in place, so a state that kept
                // the previous bucket's 11.0 would emit it for this row.
                insertAccount(job, "2026-01-02T09:00:00.000000Z", "acct-1", 3.0);

                final LiveViewWindow window = window();
                final LiveViewWindowStatePlan plan = window.getCheckpointWindowStatePlan();
                Assert.assertNotNull(plan);
                Assert.assertEquals("a maximum, a minimum and a counter", 3, plan.getComponentCount());
                Assert.assertEquals(3, plan.getProjectionCount());
                Assert.assertEquals(0, plan.getResidualFunctions().size());
                for (int i = 0; i < 3; i++) {
                    final WindowFunction function = plan.getProjectionFunction(i);
                    Assert.assertTrue("projection " + i + " must be fused", function.isWindowStateOwned());
                    Assert.assertFalse(
                            "a fused projection must never allocate its private map",
                            function.getPartitionMap().isOpen()
                    );
                }
                assertExtremaViewMatchesRecompute();

                // The head that seals the extrema restores them exactly - the component
                // codec writes one word per slot and the manifest names where it sits.
                final byte[] before = snapshotWindow(window);
                restoreHead();
                Assert.assertArrayEquals(before, snapshotWindow(window));
                assertExtremaViewMatchesRecompute();

                // Handing the state back has to carry an empty slice as an empty slice:
                // acct-3 has contributed nothing, and the private implementation reads that
                // slot's NaN as "no row yet" only because it is the very state its own
                // resetPartition writes.
                Assert.assertFalse(window.bindCheckpointWindowStatePlan(null));
                insertAccount(job, "2026-01-02T09:01:00.000000Z", "acct-3", 8.0);
                assertExtremaViewMatchesRecompute();

                // And adopting again takes them back the same way rather than starting the
                // extrema over.
                Assert.assertTrue(window.bindCheckpointWindowStatePlan(plan));
                insertAccount(job, "2026-01-02T09:02:00.000000Z", "acct-3", 4.0);
                insertAccount(job, "2026-01-02T09:03:00.000000Z", "acct-1", 1.0);
                assertExtremaViewMatchesRecompute();
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
                    + "stddev_samp(amt_txn) over w as sd, ksum(amt_txn) over w as k, "
                    + "max(amt_txn) over w as mx, min(amt_txn) over w as mn, "
                    + "max(created_at) over w as mt "
                    + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insertBranchAccount(job, DAILY_ANCHOR + "09:00:00.000000Z", "acct-1", "br-1", 5.0);
                insertBranchAccount(job, DAILY_ANCHOR + "09:00:10.000000Z", "acct-1", "br-2", 11.0);

                final LiveViewWindowStatePlan plan = window().getCheckpointWindowStatePlan();
                Assert.assertNotNull(plan);
                Assert.assertEquals(
                        "the view must produce one component per family",
                        8,
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
                        WindowAccumulatorDescriptor.FAMILY_ROW_COUNT,
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

    /**
     * How many of the plan's projections are runtime-only members - grouped in the map, on
     * a function root of their own.
     */
    private static int countRuntimeOnlyProjections(LiveViewWindowStatePlan plan) {
        int count = 0;
        for (int i = 0, n = plan.getProjectionCount(); i < n; i++) {
            if (!plan.isDurableProjection(i)) {
                count++;
            }
        }
        return count;
    }

    /**
     * Every projection function's own partition-map state once the group has handed it
     * back, as one sorted image: the projection, its partition key and the whole-state
     * image its component codec writes for that key.
     * <p>
     * The codec is the seal's own, so this is exactly the state a root round-trips - which
     * is what lets a restore be compared against the runtime that produced it.
     */
    private static String describeDeclinedGroupState(LiveViewWindowStatePlan plan) {
        final ArrayList<String> lines = new ArrayList<>();
        for (int i = 0, n = plan.getProjectionCount(); i < n; i++) {
            final WindowFunction function = plan.getProjectionFunction(i);
            final Map map = function.getPartitionMap();
            Assert.assertNotNull("projection " + i + " must own a map again", map);
            Assert.assertTrue("projection " + i + " must own an open map again", map.isOpen());
            final LiveViewAccumulatorDescriptor component = plan.getProjection(i).getFunctionComponent();
            final byte[] image = new byte[component.getStateLength()];
            final MapRecordCursor cursor = map.getCursor();
            final MapRecord record = map.getRecord();
            final int keyIndex = function.getCheckpointKeyStartIndex();
            while (cursor.hasNext()) {
                component.freezeStateInto(record.getValue(), 0, image, 0);
                final StringBuilder line = new StringBuilder();
                line.append(i).append('|').append(record.getStrA(keyIndex)).append('|');
                for (int b = 0; b < image.length; b++) {
                    line.append(String.format("%02x", image[b]));
                }
                lines.add(line.toString());
            }
        }
        Collections.sort(lines);
        return String.join("\n", lines);
    }

    /**
     * The one projection the leaf budget leaves out of the manifest, which is the only
     * member that carries a function root of its own while the group owns its state.
     */
    private static WindowFunction runtimeOnlyMember(LiveViewWindowStatePlan plan) {
        for (int i = 0, n = plan.getProjectionCount(); i < n; i++) {
            if (!plan.isDurableProjection(i)) {
                return plan.getProjectionFunction(i);
            }
        }
        throw new IllegalStateException("the shape must produce one runtime-only member");
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
     * Requires an anchored view's own partition map to land on {@code expected} at
     * {@code maxEntrySize}.
     * <p>
     * {@code MapFactory} selects on the raw key-plus-value byte sum, so the window's own
     * value slots are what stands between an anchored view and the slower
     * {@code OrderedMap}: they are charged to every anchored view there is, ahead of any
     * component. A slot added to that prefix is cheap in isolation and can still cost a
     * whole map implementation, which no correctness test would notice - hence this one.
     * <p>
     * The configured limit is asserted beside the implementation, so neither half can pass
     * for the other's reason: a limit that silently defaulted would make the implementation
     * assertion vacuous.
     */
    private void assertAnchorMapImplementation(
            boolean isFused,
            int maxEntrySize,
            String expected
    ) throws Exception {
        // Both settings are global and the harness only resets overrides between CLASSES,
        // so they are put back before returning: every other case in this class asserts the
        // fused shape, and a leaked kill switch would leave them compiling the other one.
        setProperty(PropertyKey.CAIRO_SQL_UNORDERED_MAP_MAX_ENTRY_SIZE, maxEntrySize);
        if (!isFused) {
            // The kill switch is what produces the unfused shape without changing the
            // SQL, so both cases measure the same view and differ only in what the
            // anchor map's value carries.
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "false");
        }
        try {
            assertAnchorMapImplementation0(isFused, maxEntrySize, expected);
        } finally {
            setProperty(PropertyKey.CAIRO_SQL_UNORDERED_MAP_MAX_ENTRY_SIZE, (String) null);
            setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, (String) null);
        }
    }

    private void assertAnchorMapImplementation0(
            boolean isFused,
            int maxEntrySize,
            String expected
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tx (created_at timestamp, acct int, amt_txn double) "
                    + "timestamp(created_at) partition by hour wal");
            execute("create live view lv flush every 100ms start from beginning as "
                    + "select created_at, acct, sum(amt_txn) over w as cumulative_sum from tx "
                    + "window w as (partition by acct order by created_at anchor daily '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                execute("insert into tx values ('" + DAILY_ANCHOR + "09:00:00.000000Z', 1, 1.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                Assert.assertEquals(
                        "the limit the selection is made against must be the configured one",
                        maxEntrySize,
                        configuration.getSqlUnorderedMapMaxEntrySize()
                );
                Assert.assertEquals(
                        "the fixture must build the shape this case is about",
                        isFused,
                        window().getCheckpointWindowStatePlan() != null
                );
                Assert.assertEquals(
                        "the anchor map must keep the fastest implementation its shape allows",
                        expected,
                        window().getAnchorMapImplementation()
                );
                assertNoRefreshFaults("lv");
            }
        });
    }

    /**
     * Seals a group carrying {@code runtimeOnlyMembers} members past the leaf budget and
     * requires the boundary to walk the key domain a number of times that does not depend
     * on how many those are.
     * <p>
     * Every runtime-only member reads the same keys out of the same map: the encoded key,
     * the anchor probe an incremental freeze makes and the removal set are properties of
     * the key, not of the member, and only the state image and the logical charge are each
     * member's own. So one walk serves all of them, and the only thing that can force a
     * second is a member that disagrees on whether the freeze may build on its own
     * predecessor root - which decides which map is walked. This fixture has no such
     * disagreement, so the count is the window state's own walk plus one.
     * <p>
     * Asserting the same number at two widths is the point. A per-member walk passes at one
     * member and fails at eight, which is exactly the shape of the cost being removed.
     */
    private void assertSealWalksOncePerDisposition(int runtimeOnlyMembers) throws Exception {
        assertMemoryLeak(() -> {
            final int columns = createGroupPastTheLeafBudget(runtimeOnlyMembers);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                insertWideAccount(job, DAILY_ANCHOR + "09:00:00.000000Z", "acct-1", columns);
                insertWideAccount(job, DAILY_ANCHOR + "09:00:10.000000Z", "acct-2", columns);

                final LiveViewWindowStatePlan plan = window().getCheckpointWindowStatePlan();
                Assert.assertNotNull(plan);
                Assert.assertEquals(
                        "the fixture must put exactly this many members past the leaf budget",
                        runtimeOnlyMembers,
                        countRuntimeOnlyProjections(plan)
                );

                // One commit is one boundary at this cadence, so the delta is one seal's.
                final long before = window().getCheckpointFreezeScanCount();
                Assert.assertTrue("the fixture must already have sealed", before > 0);
                insertWideAccount(job, DAILY_ANCHOR + "09:00:20.000000Z", "acct-1", columns);
                final long walks = window().getCheckpointFreezeScanCount() - before;

                Assert.assertEquals(
                        "one seal must walk the key domain twice - once for the window state and"
                                + " once for the " + runtimeOnlyMembers + " runtime-only members that"
                                + " share a disposition - rather than once per member",
                        2,
                        walks
                );
                assertWideViewMatchesRecompute(columns);

                // The shared walk has to produce what the per-member ones did, so the roots
                // it wrote must still restore into the same runtime byte for byte.
                final byte[] before0 = snapshotWindow(window());
                restoreHead();
                Assert.assertArrayEquals(before0, snapshotWindow(window()));
                assertWideViewMatchesRecompute(columns);
                assertNoRefreshFaults("lv");
            }
        });
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
     * The {@link #assertViewMatchesRecompute()} counterpart for the compensated-total
     * shape. The recompute runs the unfused {@code ksum}, so a fused total that dropped
     * the compensation term - or a folded count reading a counter the total does not keep -
     * surfaces here.
     */
    private void assertKahanViewMatchesRecompute() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String frame = "over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(select created_at, cod_acct_no, "
                        + "ksum(amt_txn) " + frame + " as k, "
                        + "count(amt_txn) " + frame + " as c "
                        + "from (select created_at, cod_acct_no, amt_txn, " + bucket + " as bucket from tx)"
                        + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
    }

    /**
     * The {@link #assertViewMatchesRecompute()} counterpart for the extremum shape. The
     * recompute runs the unfused max/min implementations, so a fused extremum that missed
     * an anchor crossing - or read an empty partition as a running one - surfaces here.
     */
    private void assertExtremaViewMatchesRecompute() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String frame = "over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(select created_at, cod_acct_no, "
                        + "max(amt_txn) " + frame + " as mx, "
                        + "min(amt_txn) " + frame + " as mn, "
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

    /**
     * Builds a base table and an anchored view whose group carries exactly
     * {@code runtimeOnlyMembers} members past the leaf payload budget, and so that many
     * roots the seal has to write out of the group's own map.
     *
     * @return the SELECT list's width, which the recompute helpers key off
     */
    private int createGroupPastTheLeafBudget(int runtimeOnlyMembers) throws Exception {
        final int fitting =
                (LiveViewCheckpointContracts.MAX_INLINE_LEAF_STATE_BYTES - Long.BYTES) / (Double.BYTES + Long.BYTES);
        final int columns = fitting + runtimeOnlyMembers;
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
        return columns;
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
     * Compares the first durable output and the runtime-only one against a from-base
     * recompute. The two are the ends of the truncated group: both read the window's own
     * value, and one of them persists on a root of its own.
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
     * As {@link #insertWideAccount}, timestamped {@code second} seconds into the anchor
     * bucket's morning - so a case that steps rows one at a time to find the cadence does
     * not have to spell every timestamp out.
     */
    private void insertWideAt(LiveViewRefreshJob job, int second, String account, int columns)
            throws Exception {
        insertWideAccount(
                job,
                DAILY_ANCHOR + String.format("09:%02d:%02d.000000Z", second / 60, second % 60),
                account,
                columns
        );
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
