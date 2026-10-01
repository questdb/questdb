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
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.std.Numbers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The START FROM contract: which base rows a live view contains, and when.
 * <p>
 * A row belongs to a live view iff its designated timestamp sits at or above the view's
 * resolved START FROM boundary. Nothing else decides it - not the commit that carried the
 * row, not whether the row predates CREATE, not which refresh path happened to process it.
 * The initial seed, the forward-append drain and every applied-base replay apply that one
 * predicate, so the view's contents are a function of the data alone.
 * <p>
 * The tests below pin the two halves of that contract. Membership: pre-CREATE rows above
 * the boundary are seeded in, post-CREATE rows below it are kept out, and BEGINNING admits
 * everything including a row back-dated below the base's CREATE-time minimum. Stability: the
 * row set does not change when an O3 commit or a restart forces a replay - which is what the
 * old design got wrong, since it excluded pre-CREATE rows by seqTxn on the forward path and
 * by timestamp on every replay path, so a replay could add rows the incremental drain had
 * excluded.
 */
public class LiveViewStartFromSeedTest extends AbstractLiveViewTest {

    @After
    public void unpinClock() {
        // Restore the "unset" clock. currentMicros is a static that outlives the class, and
        // several tests here deliberately pin it ABOVE the test data (a boundary in the
        // future). Leaving that behind would hand the next class in the JVM a clock above ITS
        // data, which is how this class first broke LiveViewFuzzTest.
        setCurrentMicros(-1);
    }

    @Before
    public void pinClockBelowTestData() {
        // Pin the CREATE moment below the (2026) test data, so START FROM NOW resolves to a
        // boundary the test rows sit above and the seed has something to admit. Tests that
        // need the boundary to fall inside the data set the clock themselves.
        setCurrentMicros(0L);
    }

    @Test
    public void testBeginningAdmitsRowBackDatedBelowCreateTimeMinimum() throws Exception {
        // BEGINNING has no lower bound. It used to anchor one at the earliest base row visible
        // at CREATE, which silently rejected a row back-dated below that minimum - even though
        // the user asked for the base's whole history.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-01-01T00:00:20.000000Z', 20)," +
                    "('2026-01-01T00:00:30.000000Z', 30)");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);

                // ts 10 sits below the base's minimum-at-CREATE (ts 20).
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:10.000000Z', 10)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }

            assertQuery("SELECT ts, x, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:10.000000Z\t10\t1\n" +
                            "2026-01-01T00:00:20.000000Z\t20\t2\n" +
                            "2026-01-01T00:00:30.000000Z\t30\t3\n");
            assertQuery("SELECT o3_rejected_count, below_lower_bound_count FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("o3_rejected_count\tbelow_lower_bound_count\n0\t0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCancelledSeedSweepStopsInsideTheFilterAndDoesNotInvalidate() throws Exception {
        // The seed's filter loop is the one place in the refresh path that pulls an
        // unbounded run of base rows without producing any: a WHERE that rejects
        // everything walks the whole scan range while the caller's row budget never
        // ticks. It is therefore where a DROP, an invalidation or a shutdown would
        // otherwise be invisible, and now it consults the refresh circuit breaker.
        // Being cancelled is not a refresh failure: the view stays valid and stays
        // SEEDING with nothing swept, which is exactly the state a later turn resumes
        // from (or, for the events that actually trip it, the state the view is torn
        // down in).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, sym, x) VALUES " +
                    "('2026-04-01T00:00:10.000000Z', 'a', 10)," +
                    "('2026-04-01T00:00:20.000000Z', 'a', 20)," +
                    "('2026-04-01T00:00:30.000000Z', 'a', 30)");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, sym, x, count(*) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE sym = 'a'");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                instance.cancelRefresh();

                drainJob(job);
                drainWalQueue();

                Assert.assertFalse("a cancellation must not invalidate the view", instance.isInvalid());
                Assert.assertEquals(
                        LiveViewState.SEED_STATE_SEEDING,
                        instance.getStateReader().getSeedState()
                );
            }

            assertQuery("SELECT ts, sym, x, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tsym\tx\trn\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testExplicitBoundaryAdmitsRowExactlyOnIt() throws Exception {
        // The boundary is inclusive: a row whose timestamp equals it belongs to the view.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-04-01T00:00:14.999999Z', 1)," +
                    "('2026-04-01T00:00:15.000000Z', 2)," +
                    "('2026-04-01T00:00:15.000001Z', 3)");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '2026-04-01T00:00:15.000000Z' AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
            }

            assertQuery("SELECT ts, x, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-04-01T00:00:15.000000Z\t2\t1\n" +
                            "2026-04-01T00:00:15.000001Z\t3\t2\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testExplicitBoundarySeedsOnlyQualifyingRowsThroughFilter() throws Exception {
        // A boundary that cuts through the base's history, under a WHERE clause: the seed
        // feeds the snapshot through the same compiled filter the drain uses, so rn counts
        // only rows that are both above the boundary and match the filter.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, sym, x) VALUES " +
                    "('2026-04-01T00:00:10.000000Z', 'a', 10)," +
                    "('2026-04-01T00:00:20.000000Z', 'b', 20)," +
                    "('2026-04-01T00:00:30.000000Z', 'a', 30)," +
                    "('2026-04-01T00:00:40.000000Z', 'b', 40)," +
                    "('2026-04-01T00:00:50.000000Z', 'a', 50)");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '2026-04-01T00:00:30.000000Z' AS " +
                    "SELECT ts, sym, x, count(*) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE sym = 'a'");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
            }

            assertQuery("SELECT ts, sym, x, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tsym\tx\trn\n" +
                            "2026-04-01T00:00:30.000000Z\ta\t30\t1\n" +
                            "2026-04-01T00:00:50.000000Z\ta\t50\t2\n");
            assertNoRefreshFaults("lv");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFutureBoundarySeedsNothingThenAdmitsRowsThatReachIt() throws Exception {
        // A boundary above every row in the base is valid: the seed qualifies nothing, and
        // later rows join the view on event time rather than on arrival time.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:10.000000Z', 10)");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '2027-01-01T00:00:00.000000Z' AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");

                // One row below the boundary, one on the far side of it.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-12-31T23:59:59.999999Z', 1)," +
                        "('2027-01-01T00:00:01.000000Z', 2)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }

            assertQuery("SELECT ts, x, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2027-01-01T00:00:01.000000Z\t2\t1\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testEmptySeedThenO3DoesNotReplaySubBoundaryHistory() throws Exception {
        // A seed that qualifies no row must not leave a head checkpoint behind. latestSeenTs is
        // stamped per emitted row, so a zero-row seed would write maxTs = LONG_NULL, and the O3
        // head-hit path floors its replay at headMaxTs + 1 - Long.MIN_VALUE + 1 - which admits
        // every base row. The first out-of-order commit then replayed the base's whole history
        // into the view, sub-boundary rows and all, and renumbered it from an empty accumulator.
        //
        // A zero-row seed is the normal case, not a corner: START FROM NOW over a base of past
        // data qualifies nothing, and so does any boundary in the future.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Pre-CREATE history, entirely BELOW the boundary: the seed must qualify none of it.
            execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:10.000000Z', 10)");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '2027-01-01T00:00:00.000000Z' AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");

                // One commit carrying two rows out of order, both ABOVE the boundary. The
                // second row is late relative to the first, which is what drives the O3 replay.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2027-01-01T00:00:20.000000Z', 20)," +
                        "('2027-01-01T00:00:15.000000Z', 15)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }

            assertQuery("SELECT ts, x, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2027-01-01T00:00:15.000000Z\t15\t1\n" +
                            "2027-01-01T00:00:20.000000Z\t20\t2\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testNanoBaseSeedsOnSubMicrosecondBoundary() throws Exception {
        // A TIMESTAMP_NANO base keeps its precision: the literal parses against the base's
        // driver, so a boundary between two rows one nanosecond apart separates them.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP_NS, x INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-04-01T00:00:15.000000499Z', 1)," +
                    "('2026-04-01T00:00:15.000000500Z', 2)," +
                    "('2026-04-01T00:00:15.000000501Z', 3)");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '2026-04-01T00:00:15.000000500Z' AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
            }

            assertQuery("SELECT ts, x, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-04-01T00:00:15.000000500Z\t2\t1\n" +
                            "2026-04-01T00:00:15.000000501Z\t3\t2\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testNowExcludesPostCreateRowsBelowTheBoundaryOnEveryPath() throws Exception {
        // The boundary bounds both directions. With the CREATE moment inside the data, rows
        // that arrive after CREATE but are timestamped below it are excluded - whether they
        // arrive in order (forward-append) or out of order (replay). The counters record it.
        assertMemoryLeak(() -> {
            setCurrentMicros(1_775_001_600_000_000L); // 2026-04-01T00:00:00Z, the CREATE moment
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // In order, above the boundary.
                execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:10.000000Z', 10)");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                // In order relative to the view's own output, but below the boundary.
                execute("INSERT INTO base (ts, x) VALUES ('2026-03-31T23:00:00.000000Z', 1)");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                // Out of order (below the view's max ts) AND below the boundary: the O3 path
                // must reject it rather than pull it in through a replay.
                execute("INSERT INTO base (ts, x) VALUES ('2026-03-31T22:00:00.000000Z', 2)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }

            assertQuery("SELECT ts, x, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-04-01T00:00:10.000000Z\t10\t1\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testNowSeedsPreCreateFutureRowsAndO3ReplayDoesNotChangeThem() throws Exception {
        // The case the old design got wrong, end to end. The base holds two rows dated above
        // the CREATE moment. The old view excluded them while it drained forward (they came
        // from commits at or below the CREATE head) but included them the first time anything
        // triggered a replay - so an O3 commit silently grew the view and renumbered every rn.
        //
        // Now they are seeded at CREATE, and the O3 commit adds exactly the one row it carries.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-01-01T00:00:10.000000Z', 10)," +
                    "('2026-01-01T00:00:20.000000Z', 20)");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);

                // The seed alone must already hold the two pre-CREATE rows.
                assertQuery("SELECT ts, x, rn FROM lv")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("ts\tx\trn\n" +
                                "2026-01-01T00:00:10.000000Z\t10\t1\n" +
                                "2026-01-01T00:00:20.000000Z\t20\t2\n");

                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-01-01T00:00:30.000000Z', 30)," +
                        "('2026-01-01T00:00:40.000000Z', 40)");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                // Out of order, between the seeded rows and the drained ones: forces the
                // head-miss replay to rebuild the whole view from the applied base.
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:25.000000Z', 25)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }

            assertQuery("SELECT ts, x, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:10.000000Z\t10\t1\n" +
                            "2026-01-01T00:00:20.000000Z\t20\t2\n" +
                            "2026-01-01T00:00:25.000000Z\t25\t3\n" +
                            "2026-01-01T00:00:30.000000Z\t30\t4\n" +
                            "2026-01-01T00:00:40.000000Z\t40\t5\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testSeedYieldsAcrossTurnsAndResumesAtTheBoundary() throws Exception {
        // A multi-turn seed whose boundary cuts through the base. The sweep yields on its turn
        // budget and resumes by skipping dataOffset rows of the base cursor, so dataOffset and
        // that cursor have to agree on what row zero is. They do because the cursor is opened
        // at the boundary on every turn - a resume that re-derived the offset against a
        // differently-based cursor would rewind into rows an earlier turn had already fed and
        // double-advance the window accumulators.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1); // one row per turn
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Four rows below the boundary, four at or above it.
            execute("INSERT INTO base (ts, sym, x) VALUES " +
                    "('2026-04-01T00:00:01.000000Z', 'a', 1)," +
                    "('2026-04-01T00:00:02.000000Z', 'a', 2)," +
                    "('2026-04-01T00:00:03.000000Z', 'a', 3)," +
                    "('2026-04-01T00:00:04.000000Z', 'a', 4)," +
                    "('2026-04-01T00:00:05.000000Z', 'a', 5)," +
                    "('2026-04-01T00:00:06.000000Z', 'a', 6)," +
                    "('2026-04-01T00:00:07.000000Z', 'a', 7)," +
                    "('2026-04-01T00:00:08.000000Z', 'a', 8)");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '2026-04-01T00:00:05.000000Z' AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn, " +
                    "sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s " +
                    "FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
            }

            // rn and the running sum both start at the boundary row: the four sub-boundary
            // rows never reached the accumulators. The frame is wide enough to span all four
            // admitted rows, so a leaked sub-boundary row would show up in s as well as rn.
            assertQuery("SELECT ts, x, rn, s FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\ts\n" +
                            "2026-04-01T00:00:05.000000Z\t5\t1\t5.0\n" +
                            "2026-04-01T00:00:06.000000Z\t6\t2\t11.0\n" +
                            "2026-04-01T00:00:07.000000Z\t7\t3\t18.0\n" +
                            "2026-04-01T00:00:08.000000Z\t8\t4\t26.0\n");
            assertNoRefreshFaults("lv");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testSeededRowsSurviveRestartWithoutReSeeding() throws Exception {
        // The seed is durable. After it completes, a restart finds an ACTIVE view with no seed
        // target, serves the seeded rows off disk, and drains forward from where the sweep left
        // off - it does not re-seed, which would duplicate every row it had already committed.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-01-01T00:00:10.000000Z', 10)," +
                    "('2026-01-01T00:00:20.000000Z', 20)");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
            }

            // Simulate restart: drop the in-memory registry, rebuild it from disk.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertEquals(
                    "a completed seed must not re-arm across a restart",
                    LiveViewState.SEED_STATE_ACTIVE,
                    instance.getStateReader().getSeedState()
            );
            Assert.assertEquals(
                    Numbers.LONG_NULL,
                    instance.getStateReader().getSeedTargetSeqTxn()
            );

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:30.000000Z', 30)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }

            assertQuery("SELECT ts, x, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:10.000000Z\t10\t1\n" +
                            "2026-01-01T00:00:20.000000Z\t20\t2\n" +
                            "2026-01-01T00:00:30.000000Z\t30\t3\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testStraddlingCommitScansOnlySubFloorPrefix() throws Exception {
        // A commit straddling the boundary (min ts below, max ts at/above) cannot be skipped in
        // O(1): the skip-prefix cursor must walk its sub-floor prefix. This guards that the O(1)
        // short-circuit does NOT over-skip a straddling commit - the above-boundary rows still
        // land in the view, and the visit counter reflects exactly the sub-floor prefix length.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            // Future boundary: the seed qualifies nothing, so latestSeenTs stays unset and the next
            // in-order commit reaches the incremental drain (a cross-commit O3 would divert instead).
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '2027-01-01T00:00:00.000000Z' AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);

                // One in-order commit: 3 rows below the boundary, then 2 at/above it.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-12-31T23:59:57.000000Z', 1)," +
                        "('2026-12-31T23:59:58.000000Z', 2)," +
                        "('2026-12-31T23:59:59.000000Z', 3)," +
                        "('2027-01-01T00:00:01.000000Z', 4)," +
                        "('2027-01-01T00:00:02.000000Z', 5)");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                final LiveViewInstance inst = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertEquals("3 sub-floor rows dropped", 3, inst.getBelowLowerBoundCount());
                Assert.assertEquals("straddling commit: the cursor visits exactly the sub-floor prefix",
                        3, inst.getLowerBoundRowsScanned());
                assertNoRefreshFaults("lv");
            }

            assertQuery("SELECT ts, x, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2027-01-01T00:00:01.000000Z\t4\t1\n" +
                            "2027-01-01T00:00:02.000000Z\t5\t2\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testWhollySubFloorInOrderCommitSkippedInConstantTime() throws Exception {
        // Regression for the linear sub-boundary walk: an in-order commit whose every row sits
        // below the view's lower bound must be dropped in O(1) using the commit's max ts, not by
        // visiting each row through TimestampLowerBoundCursor. A future boundary with an empty seed
        // keeps latestSeenTs unset, so the commit reaches the incremental in-order drain (not O3).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '2027-01-01T00:00:00.000000Z' AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);

                // One in-order commit of 5_000 rows, every one well below the 2027 boundary.
                final int rowCount = 5_000;
                execute("INSERT INTO base (ts, x) " +
                        "SELECT timestamp_sequence('2026-01-01T00:00:00.000000Z', 1000), x::int " +
                        "FROM long_sequence(" + rowCount + ")");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                final LiveViewInstance inst = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertEquals("every sub-floor row is tallied as dropped",
                        rowCount, inst.getBelowLowerBoundCount());
                Assert.assertEquals("a wholly sub-floor commit is skipped in O(1): no row is visited",
                        0, inst.getLowerBoundRowsScanned());
                assertNoRefreshFaults("lv");
            }

            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
            execute("DROP LIVE VIEW lv");
        });
    }
}
