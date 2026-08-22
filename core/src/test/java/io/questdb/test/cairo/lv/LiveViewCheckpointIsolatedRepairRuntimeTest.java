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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.lv.LiveViewFunctionSnapshot;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRepairRuntime;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Coverage for the isolated repair runtime: a correction that converges below the runtime
 * frontier replays through a second compiled runtime of the view's own SELECT rather than
 * through the primary one the forward drain stands in.
 * <p>
 * Such a repair rebuilds the state of the range it repairs and proves, by converging, that
 * the state above that range was already correct. Replaying through the primary runtime
 * therefore meant taking the whole window state aside first and putting it back afterwards
 * - a copy as large as the state itself, paid twice per repair. The isolated runtime
 * removes the exchange rather than making it cheaper.
 * <p>
 * Every case holds the same two things at once: the from-base recompute oracle says the
 * output is right, and the primary runtime says the repair never reached it. The second
 * half is what an end-state comparison cannot see - a copy-aside repair produces exactly
 * the same rows - so it is asserted on the primary's own state image and on the counter
 * naming which runtime carried the replay.
 * <p>
 * The view is the same reported customer shape the per-segment cases use: an anchored
 * WINDOW carrying an unbounded cumulative sum and count per account, over a base spanning
 * several anchor days so closed segments exist at all.
 */
public class LiveViewCheckpointIsolatedRepairRuntimeTest extends AbstractLiveViewTest {

    @Test
    public void testABaseSchemaRecompileDropsTheIsolatedRuntime() throws Exception {
        // The isolated runtime mirrors the primary's compiled shape, so it may not outlive
        // it: a replay through functions compiled against the old base metadata would stage
        // roots the rebuilt view cannot read. The recompile is driven directly - the drift
        // that reaches it in production needs a stranded WAL symbol dictionary, which is
        // LiveViewSmokeTest's fixture and says nothing more about this - and the next
        // correction has to build a fresh runtime rather than inherit the freed one.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                commit(row(2, 3, "acct-1"), job);
                final LiveViewRepairRuntime first = viewInstance().getRepairRuntime();
                Assert.assertNotNull(first);

                viewInstance().prepareForBaseSchemaRecompile();
                Assert.assertNull(
                        "a base-schema recompile must take the isolated runtime with the primary",
                        viewInstance().getRepairRuntime()
                );

                commit(row(3, 3, "acct-1"), job);
                final LiveViewRepairRuntime rebuilt = viewInstance().getRepairRuntime();
                Assert.assertNotNull(
                        "the next converging repair must build a runtime against the recompiled shape",
                        rebuilt
                );
                Assert.assertNotSame(first, rebuilt);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAConvergingRepairLeavesThePrimaryStateByteForByteUnchanged() throws Exception {
        // The invariant, stated on the state itself. The correction carries no head row, so
        // there is nothing the primary runtime is entitled to change: every accumulator it
        // holds describes the current anchor day, and the corrected row is two days below it.
        //
        // The image alone does not separate the two routes - a copy-aside repair puts the same
        // bytes back, which is what makes it correct - so the counter beside it is what names
        // the route, and the image is the net under a future isolation that leaks.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // Head rows, so the runtime stands in the fifth day and the seeded days are
                // all closed below it.
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                MemoryCARW before = null;
                MemoryCARW after = null;
                try {
                    before = imageOfPrimaryRuntime();
                    commit(row(2, 3, "acct-1"), job);
                    Assert.assertEquals(
                            "the correction must repair on the isolated runtime",
                            1,
                            job.isolatedReplayTurnCountForTest()
                    );
                    after = imageOfPrimaryRuntime();
                    assertImagesEqual(before, after);
                } finally {
                    Misc.free(before);
                    Misc.free(after);
                }
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAConvergingRepairLeavesThePrimaryFrontierAndCompactionCountersUnchanged() throws Exception {
        // The scalars the state image does not carry: the anchor snapshot writes entries
        // alone, so the copy-aside path decides them through the wipe's resetFrontier() and
        // the restore's rebuild-from-entries, while an isolated replay runs neither.
        //
        // On this shape no compaction has swept and no tombstone exists, so the copy-aside
        // path lands on the same numbers and this case does not separate the two routes.
        // It pins the invariant - a repair reclaims nothing and compacts nothing - which is
        // what would go red if a future isolation let the replay's frontier reach here.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                final LiveViewWindow window = anchorWindow();
                final long mapSize = window.getAnchorMapSize();
                final long tombstones = window.getTombstoneCount();
                final long compactions = window.getCompactionCount();
                final long compactedPartitions = window.getCompactedPartitionCount();

                commit(row(2, 3, "acct-1"), job);

                Assert.assertEquals("the anchor map must hold the same entries", mapSize, window.getAnchorMapSize());
                Assert.assertEquals("tombstones must survive the repair", tombstones, window.getTombstoneCount());
                Assert.assertEquals("no compaction belongs to a repair", compactions, window.getCompactionCount());
                Assert.assertEquals(
                        "no partition is reclaimed by a repair",
                        compactedPartitions,
                        window.getCompactedPartitionCount()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testACorrectionInsideTheActiveSegmentStillPromotesItsReplay() throws Exception {
        // The other half of the rule. A row landing in the segment the runtime is standing
        // in has no anchor boundary between it and the frontier, so the repair's influence
        // runs to the end of the base table: its replay state IS the new runtime, and there
        // is nothing to run beside. The deep correction that follows proves the counter is
        // reporting the branch rather than sitting at zero.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 3, "acct-1") + ", " + row(5, 4, "acct-2"), job);

                // Below the head and inside the fifth day, which is the runtime's own segment.
                commit(row(5, 1, "acct-2"), job);
                Assert.assertEquals(
                        "a repair whose influence reaches the frontier may not run beside it",
                        0,
                        job.isolatedReplayTurnCountForTest()
                );
                Assert.assertNull(viewInstance().getRepairRuntime());
                assertViewMatchesRecompute();

                // The same view, a correction two days below: converging, and isolated.
                commit(row(2, 3, "acct-1"), job);
                Assert.assertEquals(1, job.isolatedReplayTurnCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testASecondCorrectionReusesTheSameIsolatedRuntime() throws Exception {
        // The runtime is compiled once per view, not once per repair: a pass draining a
        // backlog of segments would otherwise pay a SQL compile per segment.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                commit(row(2, 3, "acct-1"), job);
                final LiveViewRepairRuntime first = viewInstance().getRepairRuntime();
                Assert.assertNotNull(first);

                commit(row(3, 3, "acct-2"), job);
                Assert.assertSame(
                        "the second correction must replay in the runtime the first one built",
                        first,
                        viewInstance().getRepairRuntime()
                );
                Assert.assertEquals(2, job.isolatedReplayTurnCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAYieldedRepairResumesInTheIsolatedRuntime() throws Exception {
        // A repair that parks on its turn budget leaves its half-built state in the isolated
        // runtime and continues there on the next turn. What must not happen is the resumed
        // turn finding the primary runtime instead - it holds the forward drain's state, not
        // the replay's, and the rest of the replay would accumulate on top of it.
        // The per-segment loop owns its pinned reader across every segment and so calls the
        // executor with no leave to yield; the union range is the repair that can park.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_PER_SEGMENT_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);

                MemoryCARW before = null;
                MemoryCARW after = null;
                try {
                    before = imageOfPrimaryRuntime();
                    commit(row(2, 3, "acct-1"), job);
                    Assert.assertTrue(
                            "a one-row budget must take the repair across several turns",
                            job.isolatedReplayTurnCountForTest() > 1
                    );
                    after = imageOfPrimaryRuntime();
                    assertImagesEqual(before, after);
                } finally {
                    Misc.free(before);
                    Misc.free(after);
                }
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testDecliningTheIsolatedRuntimeRepairsThroughThePrimary() throws Exception {
        // The escape hatch, and the control column a measurement runs against: the same
        // correction on the same view, on the copy-aside route every converging repair took
        // before the isolated runtime existed.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_ISOLATED_RUNTIME_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);
                commit(row(2, 3, "acct-1"), job);

                Assert.assertEquals(0, job.isolatedReplayTurnCountForTest());
                Assert.assertNull(
                        "a declined isolated runtime must not be compiled at all",
                        viewInstance().getRepairRuntime()
                );
                Assert.assertEquals(1, job.segmentRepairCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTheIsolatedRuntimeHoldsNoKeysBetweenRepairs() throws Exception {
        // What the isolation buys, made visible: the replay's state is the repaired segment's
        // keys rather than the view's whole domain, and the runtime hands them back once the
        // repair ends instead of holding a segment's key set for the view's life.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverThreeDays());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1") + ", " + row(5, 2, "acct-2"), job);
                commit(row(2, 3, "acct-1"), job);

                final LiveViewRepairRuntime runtime = viewInstance().getRepairRuntime();
                Assert.assertNotNull(runtime);
                final LiveViewWindow isolatedWindow = runtime.getAnchorWindow();
                Assert.assertNotNull("the isolated runtime must carry an anchor window of its own", isolatedWindow);
                Assert.assertNotSame(anchorWindow(), isolatedWindow);
                Assert.assertEquals(
                        "the isolated runtime must hold no keys once the repair has ended",
                        0,
                        isolatedWindow.getAnchorMapSize()
                );
                Assert.assertTrue(
                        "the primary runtime must still hold the view's whole key domain",
                        anchorWindow().getAnchorMapSize() >= 4
                );
                assertViewMatchesRecompute();
            }
        });
    }

    private LiveViewWindow anchorWindow() {
        final LiveViewWindow window = viewInstance().getAnchorWindow();
        Assert.assertNotNull("the view must carry an anchored window", window);
        return window;
    }

    private void assertImagesEqual(MemoryCARW expected, MemoryCARW actual) {
        final long size = expected.getAppendOffset();
        Assert.assertEquals(
                "the primary runtime's state image changed size across the repair",
                size,
                actual.getAppendOffset()
        );
        for (long i = 0; i < size; i++) {
            if (expected.getByte(i) != actual.getByte(i)) {
                Assert.fail("the primary runtime's state changed across the repair, at byte " + i);
            }
        }
    }

    private void assertViewMatchesRecompute() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String recompute = "select created_at, account_id, "
                + "sum(amount) over (partition by account_id, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_sum, "
                + "count(account_id) over (partition by account_id, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_count "
                + "from (select created_at, account_id, amount, " + bucket + " as bucket from tx)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + recompute + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    private void commit(String values, LiveViewRefreshJob job) throws Exception {
        // Names the columns rather than relying on their count: one case adds a column to the
        // base half way through, and a positional INSERT would stop matching it.
        execute("insert into tx (created_at, account_id, amount) values " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private void createView(String seedRows) throws Exception {
        execute("create table tx (created_at timestamp, account_id symbol nocache index capacity 4, "
                + "amount double) timestamp(created_at) partition by hour wal");
        execute("insert into tx (created_at, account_id, amount) values " + seedRows);
        drainWalQueue();
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, account_id, "
                + "sum(amount) over w as cumulative_sum, "
                + "count(account_id) over w as cumulative_count "
                + "from tx window w as (partition by account_id order by created_at anchor daily '00:00')");
    }

    /**
     * The primary runtime's whole checkpointable state, serialised through the same
     * freeze contract a checkpoint reads it under: every function that carries its own
     * state, then the anchor map. Two images taken either side of a repair are what say
     * whether the repair reached it.
     */
    private MemoryCARW imageOfPrimaryRuntime() {
        final MemoryCARW mem = Vm.getCARWInstance(64 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
        try {
            final LiveViewInstance instance = viewInstance();
            final ObjList<WindowFunction> functions = unwrapWindowFunctions(instance);
            for (int i = 0, n = functions.size(); i < n; i++) {
                final WindowFunction f = functions.getQuick(i);
                // The same filter the freeze applies: a function with no checkpoint state
                // has none to image, and one whose state the anchored window owns is imaged
                // once, by the window, rather than once per projection of it.
                if (!f.supportsCheckpointState() || f.isWindowStateOwned()) {
                    continue;
                }
                LiveViewFunctionSnapshot.write(mem, f);
            }
            final LiveViewWindow window = instance.getAnchorWindow();
            if (window != null) {
                window.snapshot(mem);
            }
            return mem;
        } catch (Throwable t) {
            Misc.free(mem);
            throw t;
        }
    }

    /**
     * One row of {@code account} at {@code hour} on 2026-01-{@code day}, as an INSERT tuple.
     * The day is what carries the case: with a daily anchor it is also the segment.
     */
    private String row(int day, int hour, String account) {
        return "('2026-01-" + String.format("%02d", day) + "T" + String.format("%02d", hour)
                + ":00:00.000000Z', '" + account + "', 1.0)";
    }

    /**
     * Four accounts on each of 2026-01-02, 2026-01-03 and 2026-01-04. The wider key domain is
     * what makes an untouched primary runtime worth asserting: a repair of one account in one
     * old day leaves twelve entries the primary must still hold exactly.
     */
    private String seedFourAccountsOverThreeDays() {
        final StringBuilder rows = new StringBuilder();
        for (int day = 2; day <= 4; day++) {
            for (int account = 1; account <= 4; account++) {
                if (rows.length() > 0) {
                    rows.append(", ");
                }
                rows.append(row(day, account, "acct-" + account));
            }
        }
        return rows.toString();
    }

    /**
     * Two accounts on each of 2026-01-02, 2026-01-03 and 2026-01-04 - three anchor days that
     * are all closed once the head reaches the fifth.
     */
    private String seedThreeDays() {
        return row(2, 1, "acct-1") + ", " + row(2, 2, "acct-2") + ", "
                + row(3, 1, "acct-1") + ", " + row(3, 2, "acct-2") + ", "
                + row(4, 1, "acct-1") + ", " + row(4, 2, "acct-2");
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }
}
