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
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * The checkpoint capture ledger: what a seal's freeze actually walked, split into the
 * window root's capture and the function roots'.
 * <p>
 * This is the reading the layout-removal change's structural claims are made in, and
 * nothing else in the system carries it. The published artifacts cannot: an incremental
 * root and a complete one both name the whole live key domain, because the incremental
 * one keeps every key it did not touch from its predecessor. Elapsed time cannot either -
 * a complete walk of a small domain beats an incremental walk that had to map an older
 * segment to compare against - so a build that silently stopped freezing incrementally
 * would pass every existing seal test and every timing comparison.
 * <p>
 * Every case runs under both settings of {@code cairo.sql.window.map.fusion.enabled} and
 * expects the same numbers from each. That is the point of the parameterization rather
 * than a convenience: the storage layout no longer follows the runtime binding, so a
 * fused seal and an unfused one walk the same key domain, publish the same root and split
 * the same way between the window root and the roots left to functions. A reading that
 * moved with the switch would be the layout following the runtime again.
 */
@RunWith(Parameterized.class)
public class LiveViewCheckpointCaptureLedgerTest extends AbstractLiveViewTest {

    private static final String DAY = "2026-01-01T11:00:";
    private static final String NEXT_DAY = "2026-01-02T11:00:";
    // Enough accounts that a batch touching one is unmistakably narrower than the domain.
    // Eight is also small enough to seed as a literal row list, so the case reads as the
    // rows it inserts rather than as a generator.
    private static final int SEEDED_ACCOUNTS = 8;
    private final boolean isFused;

    public LiveViewCheckpointCaptureLedgerTest(String mode) {
        this.isFused = "fused".equals(mode);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> modes() {
        return Arrays.asList(new Object[][]{{"fused"}, {"unfused"}});
    }

    @Before
    public void setUpFusionMode() {
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, String.valueOf(isFused));
        // One row per logical boundary, so every commit below seals a head of its own and
        // the ledger delta across a commit is one seal's reading rather than several.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
    }

    @Test
    public void testACompleteCaptureWalksTheWholeAnchorMap() throws Exception {
        // The seed seals once rather than once per row, so the reading below is the view's
        // one complete capture rather than a complete capture plus the incremental ones that
        // followed it inside the same drain.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 8 * SEEDED_ACCOUNTS);
        assertMemoryLeak(() -> {
            createFusedView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final Ledger ledger = new Ledger();
                driveRefreshToQuiescence(job);

                // The view's first seal builds on nothing, so it is complete by definition and
                // has to read every key the seed left. This is the reading a steady incremental
                // cadence is measured against, and the one a restore or a rebinding returns to.
                final Ledger.Delta first = ledger.delta();
                Assert.assertEquals(1, first.windowCaptures);
                Assert.assertEquals(
                        "the view's first seal cannot be incremental",
                        0,
                        first.windowIncrementalCaptures
                );
                Assert.assertEquals(SEEDED_ACCOUNTS, anchorMapSize());
                Assert.assertEquals(SEEDED_ACCOUNTS, first.windowKeysVisited);
                Assert.assertEquals(SEEDED_ACCOUNTS, first.windowKeysImaged);
                Assert.assertEquals(
                        "a complete capture removes by omission rather than by naming removals",
                        0,
                        first.windowKeysRemoved
                );
                Assert.assertEquals(
                        "a seal with no predecessor root has nothing to compare against",
                        0,
                        first.windowElisionProbes
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAResidualFunctionRootIsChargedApartFromTheWindowRoot() throws Exception {
        assertMemoryLeak(() -> {
            // An anchored DECIMAL sum accumulates as scaled integers, which is outside every
            // inline family, so it stays on a root of its own beside the DOUBLE sum the plan
            // does carry - in both runtime modes.
            createResidualDecimalView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final Ledger ledger = new Ledger();

                commit("('" + DAY + "20.000000Z', 'acct-3', 1.0, 1.25::decimal(38,2))", job);

                final Ledger.Delta delta = ledger.delta();
                Assert.assertEquals(1, delta.windowCaptures);
                Assert.assertEquals(1, delta.windowIncrementalCaptures);
                Assert.assertEquals(1, delta.windowKeysVisited);
                Assert.assertEquals(1, delta.windowKeysImaged);
                // The residual is one root, walked apart from the window's, and it walks its
                // own dirty set rather than its whole private map.
                Assert.assertEquals(
                        "the DECIMAL sum must keep exactly one root of its own",
                        1,
                        delta.functionCaptures
                );
                Assert.assertEquals(1, delta.functionIncrementalCaptures);
                Assert.assertEquals(1, delta.functionKeysVisited);
                Assert.assertEquals(1, delta.functionKeysImaged);
                Assert.assertEquals(SEEDED_ACCOUNTS, anchorMapSize());
                assertViewMatchesRecomputeWithDecimal();
            }
        });
    }

    @Test
    public void testARingBackedResidualScansItsDomainWhileTheWindowStaysIncremental() throws Exception {
        assertMemoryLeak(() -> {
            // A bounded RANGE frame keeps the live rows behind its tail in a ring, and
            // freezeFunction excludes ring state from dirty-key capture, so its root is a
            // complete scan on every seal however warm its predecessor is. The window root
            // beside it is not: the two dispositions have to be readable apart, which is the
            // whole reason the ledger splits them.
            createRingResidualView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final Ledger ledger = new Ledger();

                commit("('" + DAY + "20.000000Z', 'acct-3', 1.0)", job);

                final Ledger.Delta delta = ledger.delta();
                Assert.assertEquals(1, delta.windowCaptures);
                Assert.assertEquals(1, delta.windowIncrementalCaptures);
                Assert.assertEquals(1, delta.windowKeysVisited);
                Assert.assertEquals(1, delta.windowKeysImaged);
                Assert.assertEquals(1, delta.functionCaptures);
                Assert.assertEquals(
                        "ring state is exempt from dirty-key capture and must scan complete",
                        0,
                        delta.functionIncrementalCaptures
                );
                Assert.assertEquals(
                        "a complete ring scan reads every key the function holds",
                        SEEDED_ACCOUNTS,
                        delta.functionKeysVisited
                );
                Assert.assertEquals(SEEDED_ACCOUNTS, delta.functionKeysImaged);
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testASteadyIncrementalCaptureWalksTheDirtySetRatherThanTheDomain() throws Exception {
        assertMemoryLeak(() -> {
            createFusedView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final Ledger ledger = new Ledger();

                // Two commits, two keys each, against a domain of eight. Two seals that each
                // read two keys is the claim; two seals that each read eight would answer
                // every existing assertion in the suite identically.
                commit("('" + DAY + "20.000000Z', 'acct-1', 1.0), "
                        + "('" + DAY + "21.000000Z', 'acct-2', 2.0)", job);
                commit("('" + DAY + "30.000000Z', 'acct-3', 3.0), "
                        + "('" + DAY + "31.000000Z', 'acct-4', 4.0)", job);

                final Ledger.Delta delta = ledger.delta();
                Assert.assertEquals(2, delta.windowCaptures);
                Assert.assertEquals(
                        "both seals must build on the root the seal before them published",
                        2,
                        delta.windowIncrementalCaptures
                );
                Assert.assertEquals(4, delta.windowKeysVisited);
                Assert.assertEquals(4, delta.windowKeysImaged);
                Assert.assertEquals(0, delta.windowKeysRemoved);
                // Every one of the four rows lands in the bucket its key's anchor already
                // names, so the predecessor could be holding the entry each seal is about to
                // write and the only way to find out is to look.
                Assert.assertEquals(
                        "a key whose anchor held has to be compared against the predecessor",
                        4,
                        delta.windowElisionProbes
                );
                Assert.assertEquals(
                        "every projection is durable, so nothing may keep a root of its own",
                        0,
                        delta.functionCaptures
                );
                // The domain the two seals did not walk. Without this the counts above would
                // also be satisfied by a view that had lost six of its keys.
                Assert.assertEquals(SEEDED_ACCOUNTS, anchorMapSize());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAKeyTheCheckpointDoesNotHoldYetIsNotProbedFor() throws Exception {
        assertMemoryLeak(() -> {
            createFusedView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final Ledger ledger = new Ledger();

                // A ninth account, inside the bucket the other eight are in. Its anchor has
                // not moved - it has no anchor yet - and the head root has no entry to
                // compare its payload against, which the freeze knows from the marker the
                // row that created the key wrote.
                commit("('" + DAY + "20.000000Z', 'acct-9', 9.0)", job);

                final Ledger.Delta delta = ledger.delta();
                Assert.assertEquals(1, delta.windowCaptures);
                Assert.assertEquals(1, delta.windowIncrementalCaptures);
                Assert.assertEquals(1, delta.windowKeysVisited);
                Assert.assertEquals(1, delta.windowKeysImaged);
                Assert.assertEquals(
                        "a key absent from the predecessor root cannot be elided against it",
                        0,
                        delta.windowElisionProbes
                );
                Assert.assertEquals(SEEDED_ACCOUNTS + 1, anchorMapSize());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testASealOverKeysThatCrossedAnAnchorBoundaryProbesNothing() throws Exception {
        assertMemoryLeak(() -> {
            createFusedView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final Ledger ledger = new Ledger();

                // The next day's bucket for two keys the seed already sealed. Each row resets
                // its key's accumulators and writes a new anchor value, and the anchor value
                // leads the payload, so neither entry can be the one the head root holds.
                commit("('" + NEXT_DAY + "00.000000Z', 'acct-1', 1.0)", job);
                commit("('" + NEXT_DAY + "01.000000Z', 'acct-2', 2.0)", job);

                final Ledger.Delta delta = ledger.delta();
                Assert.assertEquals(2, delta.windowCaptures);
                Assert.assertEquals(2, delta.windowIncrementalCaptures);
                Assert.assertEquals(2, delta.windowKeysVisited);
                Assert.assertEquals(2, delta.windowKeysImaged);
                Assert.assertEquals(
                        "an imaged key whose anchor moved is owed no predecessor lookup",
                        0,
                        delta.windowElisionProbes
                );
                // The two seals still published what they imaged, which is what says the
                // skipped lookup cost the roots nothing: the recompute below reads the
                // restarted view off those roots rather than off the live runtime.
                Assert.assertEquals(SEEDED_ACCOUNTS, anchorMapSize());
                assertViewMatchesRecompute();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                assertRestoredFromTimeline("lv");
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTheFirstSealAfterARestartIsIncrementalAgainstTheRestoredRoot() throws Exception {
        assertMemoryLeak(() -> {
            createFusedView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                final Ledger ledger = new Ledger();

                commit("('" + DAY + "20.000000Z', 'acct-3', 1.0)", job);

                assertRestoredFromTimeline("lv");
                // A restore leaves the runtime holding exactly what the root holds, which is
                // the position a publication leaves it in - so it re-establishes the
                // incremental baseline rather than demoting the next seal to a full scan. The
                // performance matrix reads this: "checkpoint restore and its first reseal"
                // measures an incremental first reseal, not a rescan of the restored domain.
                final Ledger.Delta delta = ledger.delta();
                Assert.assertEquals(1, delta.windowCaptures);
                Assert.assertEquals(1, delta.windowIncrementalCaptures);
                Assert.assertEquals(1, delta.windowKeysVisited);
                Assert.assertEquals(1, delta.windowKeysImaged);
                Assert.assertEquals(
                        "the restored root is a predecessor the first reseal can elide against",
                        1,
                        delta.windowElisionProbes
                );
                Assert.assertEquals(SEEDED_ACCOUNTS, anchorMapSize());
                assertViewMatchesRecompute();
            }
        });
    }

    private long anchorMapSize() {
        final LiveViewWindow window = viewInstance().getAnchorWindow();
        Assert.assertNotNull("the view must carry an anchored window", window);
        return window.getAnchorMapSize();
    }

    private void assertViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(select created_at, account_id, "
                        + "sum(amount) over (partition by account_id, bucket order by created_at "
                        + "rows between unbounded preceding and current row) as cumulative_sum, "
                        + "count(account_id) over (partition by account_id, bucket order by created_at "
                        + "rows between unbounded preceding and current row) as cumulative_count "
                        + "from (select *, timestamp_floor('1d', created_at) as bucket from tx)) order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    private void assertViewMatchesRecomputeWithDecimal() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(select created_at, account_id, "
                        + "sum(amount) over (partition by account_id, bucket order by created_at "
                        + "rows between unbounded preceding and current row) as cumulative_sum, "
                        + "sum(fee) over (partition by account_id, bucket order by created_at "
                        + "rows between unbounded preceding and current row) as cumulative_fee "
                        + "from (select *, timestamp_floor('1d', created_at) as bucket from tx)) order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    private void commit(String values, LiveViewRefreshJob job) throws Exception {
        execute("insert into tx values " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private void createFusedView() throws Exception {
        createTable("amount double");
        seedAccounts("x * 1.0");
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, account_id, "
                + "sum(amount) over w as cumulative_sum, "
                + "count(account_id) over w as cumulative_count "
                + "from tx window w as (partition by account_id order by created_at anchor daily '00:00')");
    }

    private void createResidualDecimalView() throws Exception {
        createTable("amount double, fee decimal(38,2)");
        seedAccounts("x * 1.0, (x * 0.25)::decimal(38,2)");
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, account_id, "
                + "sum(amount) over w as cumulative_sum, "
                + "sum(fee) over w as cumulative_fee "
                + "from tx window w as (partition by account_id order by created_at anchor daily '00:00')");
    }

    private void createRingResidualView() throws Exception {
        createTable("amount double");
        seedAccounts("x * 1.0");
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, account_id, "
                + "sum(amount) over w as cumulative_sum, "
                + "sum(amount) over r as ring_sum "
                + "from tx window w as (partition by account_id order by created_at anchor daily '00:00'), "
                + "r as (partition by account_id order by created_at "
                + "range between '30' second preceding and current row)");
    }

    private void createTable(String columns) throws Exception {
        execute("create table tx (created_at timestamp, account_id symbol nocache index capacity 4, "
                + columns + ") timestamp(created_at) partition by hour wal");
    }

    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    /**
     * One row per account, a second apart, all inside the 2026-01-01 anchor bucket.
     */
    private void seedAccounts(String values) throws Exception {
        execute("insert into tx select ('" + DAY + "00.000000Z'::timestamp + x * 1_000_000)::timestamp, "
                + "('acct-' || x)::symbol, " + values + " from long_sequence(" + SEEDED_ACCOUNTS + ")");
        drainWalQueue();
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("the view must be registered", instance);
        return instance;
    }

    /**
     * The view's lifetime capture counters, and the difference between two readings of them.
     * Every counter on the instance is cumulative, so a case's own reading is always a
     * delta; holding the previous reading in one object keeps the ten of them from drifting
     * out of step with each other.
     */
    private final class Ledger {
        private long functionCaptures;
        private long functionIncrementalCaptures;
        private long functionKeysImaged;
        private long functionKeysVisited;
        private long windowCaptures;
        private long windowElisionProbes;
        private long windowIncrementalCaptures;
        private long windowKeysImaged;
        private long windowKeysRemoved;
        private long windowKeysVisited;

        private Ledger() {
            final LiveViewInstance instance = viewInstance();
            windowElisionProbes = instance.getCheckpointCaptureWindowElisionProbes();
            windowCaptures = instance.getCheckpointCaptureWindowRoots();
            windowIncrementalCaptures = instance.getCheckpointCaptureWindowRootsIncremental();
            windowKeysVisited = instance.getCheckpointCaptureWindowKeysVisited();
            windowKeysImaged = instance.getCheckpointCaptureWindowKeysImaged();
            windowKeysRemoved = instance.getCheckpointCaptureWindowKeysRemoved();
            functionCaptures = instance.getCheckpointCaptureFunctionRoots();
            functionIncrementalCaptures = instance.getCheckpointCaptureFunctionRootsIncremental();
            functionKeysVisited = instance.getCheckpointCaptureFunctionKeysVisited();
            functionKeysImaged = instance.getCheckpointCaptureFunctionKeysImaged();
        }

        Delta delta() {
            final LiveViewInstance instance = viewInstance();
            final Delta delta = new Delta();
            delta.windowElisionProbes =
                    instance.getCheckpointCaptureWindowElisionProbes() - windowElisionProbes;
            delta.windowCaptures = instance.getCheckpointCaptureWindowRoots() - windowCaptures;
            delta.windowIncrementalCaptures =
                    instance.getCheckpointCaptureWindowRootsIncremental() - windowIncrementalCaptures;
            delta.windowKeysVisited = instance.getCheckpointCaptureWindowKeysVisited() - windowKeysVisited;
            delta.windowKeysImaged = instance.getCheckpointCaptureWindowKeysImaged() - windowKeysImaged;
            delta.windowKeysRemoved = instance.getCheckpointCaptureWindowKeysRemoved() - windowKeysRemoved;
            delta.functionCaptures = instance.getCheckpointCaptureFunctionRoots() - functionCaptures;
            delta.functionIncrementalCaptures =
                    instance.getCheckpointCaptureFunctionRootsIncremental() - functionIncrementalCaptures;
            delta.functionKeysVisited = instance.getCheckpointCaptureFunctionKeysVisited() - functionKeysVisited;
            delta.functionKeysImaged = instance.getCheckpointCaptureFunctionKeysImaged() - functionKeysImaged;
            return delta;
        }

        private final class Delta {
            private long functionCaptures;
            private long functionIncrementalCaptures;
            private long functionKeysImaged;
            private long functionKeysVisited;
            private long windowCaptures;
            private long windowElisionProbes;
            private long windowIncrementalCaptures;
            private long windowKeysImaged;
            private long windowKeysRemoved;
            private long windowKeysVisited;
        }
    }
}
