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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Numbers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The START FROM boundary on the replay paths.
 * <p>
 * A live view's contents are a function of the base data and the view's resolved START FROM
 * boundary: a row belongs to the view iff its designated timestamp sits at or above that
 * boundary. {@link LiveViewStartFromSeedTest} pins that for the initial seed and the forward
 * drain. This class pins it for the paths that re-derive the view from the <i>applied</i> base
 * table - the O3 head-miss and head-hit replays, and the dedup-coupled applied-base drain -
 * because a replay that floors its scan anywhere other than the boundary changes the view's
 * contents just by running, and a replay runs on every out-of-order commit, dedup replacement,
 * REPLACE_RANGE, metadata drift, mid-drain failure and checkpoint-less restart.
 * <p>
 * The deletion side of the contract gets the same treatment. A REPLACE_RANGE commit's deleted
 * band is visible only through its range metadata, and both drains clamp that range low up to
 * the view's boundary (deleting base rows below the boundary removes nothing the view holds).
 * The clamp has to leave a usable timestamp behind: LONG_NULL means "no trigger" to every
 * downstream reader, so a clamp that lands on it makes the drain miss the deletion entirely.
 */
public class LiveViewStartFromReplayTest extends AbstractLiveViewTest {

    @After
    public void unpinClock() {
        // currentMicros is a static that outlives the class; hand the next one a clean slate.
        // See the clock-hygiene note on LiveViewStartFromSeedTest.
        setCurrentMicros(-1);
    }

    @Before
    public void pinClockBelowTestData() {
        // Below the 2026 test data, so a START FROM NOW view admits it all. Pinned
        // unconditionally: inheriting whatever clock the previous class left behind would move
        // the boundary of every NOW view here.
        setCurrentMicros(0L);
    }

    @Test
    public void testBeginningReplaceRangeFromMinTimestampRemovesDeletedRows() throws Exception {
        // A REPLACE_RANGE whose range low is Long.MIN_VALUE - "replace everything up to hi" -
        // against a BEGINNING view, whose boundary is Long.MIN_VALUE too. Clamping the range low
        // to the boundary then yields Long.MIN_VALUE, which IS Numbers.LONG_NULL, and LONG_NULL
        // is what both drains and the replay read as "this commit has no trigger timestamp".
        //
        // The raw-WAL drain did still spot the O3 (LONG_NULL compares below the frontier), but it
        // handed LONG_NULL on as the replay's lateRowTs, so the head-miss REPLACE_RANGE fell back
        // to the lowest row the recompute produced - 00:02 here - and froze everything below it.
        // The view kept its derived row for the deleted 00:01 as a ghost: a row the base no longer
        // holds, which no recompute of the view's SELECT can produce.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-01-01T00:00:01.000000Z', 1)," +
                    "('2026-01-01T00:00:02.000000Z', 2)," +
                    "('2026-01-01T00:00:03.000000Z', 3)");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");

                // Replace [MIN, 00:03) with a single row at 00:02: the base loses 00:01 outright
                // and keeps a rewritten 00:02.
                final TableToken baseToken = engine.verifyTableName("base");
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    appendRow(walWriter, ts("2026-01-01T00:00:02.000000Z"), 20);
                    walWriter.commitWithParams(
                            Numbers.LONG_NULL,
                            ts("2026-01-01T00:00:03.000000Z"),
                            WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                    );
                }
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertLiveViewValid();
            }

            assertQuery("SELECT ts, x, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:02.000000Z\t20\t1\n" +
                            "2026-01-01T00:00:03.000000Z\t3\t2\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDedupReplacementAboveTheBoundaryReplaysFromTheBoundary() throws Exception {
        // A dedup replacement above the boundary routes through the applied-base drain's replay.
        // The recompute must start at the boundary, not at the base's first row: the view's
        // numbering has to come out the same as if the replacement had been there all along.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL " +
                    "DEDUP UPSERT KEYS(ts)");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-01-01T00:00:01.000000Z', 1)," +
                    "('2026-01-01T00:00:02.000000Z', 2)," +
                    "('2026-01-01T00:00:03.000000Z', 3)," +
                    "('2026-01-01T00:00:04.000000Z', 4)," +
                    "('2026-01-01T00:00:05.000000Z', 5)");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '2026-01-01T00:00:03.000000Z' AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);

                // Replaces the row at 00:04, which the view holds.
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:04.000000Z', 400)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertLiveViewValid();
            }

            assertQuery("SELECT ts, x, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:03.000000Z\t3\t1\n" +
                            "2026-01-01T00:00:04.000000Z\t400\t2\n" +
                            "2026-01-01T00:00:05.000000Z\t5\t3\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDedupReplacementBelowTheBoundaryStaysExcluded() throws Exception {
        // The same path, triggered from below the boundary. The replacement rewrites a base row
        // the view does not hold, so the replay it forces must be a no-op for the view: the
        // sub-boundary row stays out and the numbering above the boundary does not shift.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL " +
                    "DEDUP UPSERT KEYS(ts)");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-01-01T00:00:01.000000Z', 1)," +
                    "('2026-01-01T00:00:02.000000Z', 2)," +
                    "('2026-01-01T00:00:03.000000Z', 3)," +
                    "('2026-01-01T00:00:04.000000Z', 4)," +
                    "('2026-01-01T00:00:05.000000Z', 5)");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '2026-01-01T00:00:03.000000Z' AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);

                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:01.000000Z', 100)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertLiveViewValid();
            }

            assertQuery("SELECT ts, x, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:03.000000Z\t3\t1\n" +
                            "2026-01-01T00:00:04.000000Z\t4\t2\n" +
                            "2026-01-01T00:00:05.000000Z\t5\t3\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDedupReplacementSpanningTheBoundaryClearsTheEmptiedView() throws Exception {
        // The same commit shape as the sibling test, but the replacement empties the view outright:
        // the only row it held drops out of the filter, so the recompute produces nothing at all.
        //
        // The head-miss replay's zero-surviving-row branch clears the emptied range with a
        // pure-delete REPLACE_RANGE, and it too keyed that off the raw trigger, refusing to fire
        // for a trigger below the view's bound. So this commit cleared nothing and the view went
        // on serving a row whose base row no longer passes its SELECT - while the watermark moved
        // past the commit that removed it, making the ghost permanent.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL " +
                    "DEDUP UPSERT KEYS(ts)");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-01-01T00:00:01.000000Z', 1)," +
                    "('2026-01-01T00:00:03.000000Z', 3)");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '2026-01-01T00:00:03.000000Z' AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");

                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 10)," +
                        "('2026-01-01T00:00:03.000000Z', -3)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertLiveViewValid();
            }

            // No base row at or above the boundary passes x > 0 any more, so the view holds nothing.
            assertQuery("SELECT ts, x, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDedupReplacementSpanningTheBoundaryDropsTheStaleRow() throws Exception {
        // One commit that reaches BELOW the boundary and also replaces a row ABOVE it, with the
        // replacement failing the view's WHERE - so the view's lowest row must leave the view
        // while the base row itself stays put.
        //
        // The replay deletes and rewrites its output from the triggering commit's lowest touched
        // timestamp, precisely so a replacement that drops a row out of the filter cannot strand
        // it (the recompute's own lowest surviving row sits ABOVE such a row, so flooring the
        // delete there would step over it). But the trigger here is 00:01 - the commit's
        // sub-boundary row - and the replay refused to use a trigger below the view's bound at
        // all, falling back to the lowest surviving row, 00:04. The dropped 00:03 row sat below
        // that and survived as a ghost, duplicating rn=1 with the row that legitimately took it.
        //
        // A commit reaching below the boundary is routine once a boundary cuts the base, so the
        // trigger belongs clamped UP to the boundary - the lowest timestamp the view can hold -
        // not thrown away.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL " +
                    "DEDUP UPSERT KEYS(ts)");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-01-01T00:00:01.000000Z', 1)," +
                    "('2026-01-01T00:00:02.000000Z', 2)," +
                    "('2026-01-01T00:00:03.000000Z', 3)," +
                    "('2026-01-01T00:00:04.000000Z', 4)," +
                    "('2026-01-01T00:00:05.000000Z', 5)");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '2026-01-01T00:00:03.000000Z' AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");

                // One commit, two dedup replacements: 00:01 sits below the boundary (so it only
                // drags the commit's min timestamp down there), and 00:03 - the view's lowest row -
                // comes back with a value its WHERE rejects.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 10)," +
                        "('2026-01-01T00:00:03.000000Z', -3)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertLiveViewValid();
            }

            assertQuery("SELECT ts, x, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:04.000000Z\t4\t1\n" +
                            "2026-01-01T00:00:05.000000Z\t5\t2\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testHeadHitReplayStaysAtOrAboveTheBoundary() throws Exception {
        // Head-hit is the one applied-base scan that does not floor at the boundary: it starts at
        // headMaxTs + 1, and relies on every head having been written from output the boundary was
        // already applied to. This asserts the property rather than the implementation - a
        // head-hit replay under a finite boundary must not pull the sub-boundary row into the view
        // and must not renumber what the head already covers.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // 00:01 sits below the boundary and must never surface, on any path.
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-01-01T00:00:01.000000Z', 1)," +
                    "('2026-01-01T00:00:02.000000Z', 2)," +
                    "('2026-01-01T00:00:03.000000Z', 3)");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '2026-01-01T00:00:02.000000Z' AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);

                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                Assert.assertEquals(
                        "the seed's commit must leave a head checkpoint at its max output ts",
                        ts("2026-01-01T00:00:03.000000Z"),
                        instance.getHeadCheckpointMaxTs()
                );

                // Forward append. The checkpoint cadence (1M rows / 5 min) does not fire, so the
                // head stays at 00:03 while the frontier moves to 00:07.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-01-01T00:00:05.000000Z', 5)," +
                        "('2026-01-01T00:00:07.000000Z', 7)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals(ts("2026-01-01T00:00:03.000000Z"), instance.getHeadCheckpointMaxTs());
                Assert.assertEquals(ts("2026-01-01T00:00:07.000000Z"), instance.getLatestSeenTs());

                // Out of order, and above the head: head-hit eligible. The replay rolls state back
                // to the head (which covers 00:02 and 00:03) and re-reads the base from 00:04 up.
                execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:06.000000Z', 6)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertLiveViewValid();
            }

            assertQuery("SELECT ts, x, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:02.000000Z\t2\t1\n" +
                            "2026-01-01T00:00:03.000000Z\t3\t2\n" +
                            "2026-01-01T00:00:05.000000Z\t5\t3\n" +
                            "2026-01-01T00:00:06.000000Z\t6\t4\n" +
                            "2026-01-01T00:00:07.000000Z\t7\t5\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testReplaceRangeBelowTheBoundaryLeavesTheViewUnchanged() throws Exception {
        // A replace band that ends at the boundary touches only base rows the view never held.
        // Whatever it deletes or inserts down there, the view's rows and their numbering must come
        // out identical - the deletion side clamps to the boundary and finds an empty range.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-01-01T00:00:01.000000Z', 1)," +
                    "('2026-01-01T00:00:02.000000Z', 2)," +
                    "('2026-01-01T00:00:03.000000Z', 3)," +
                    "('2026-01-01T00:00:04.000000Z', 4)");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '2026-01-01T00:00:03.000000Z' AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);

                // [00:01, 00:03) is entirely below the boundary: it deletes 00:01 and 00:02 and
                // puts a single row back at 00:02.
                final TableToken baseToken = engine.verifyTableName("base");
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    appendRow(walWriter, ts("2026-01-01T00:00:02.000000Z"), 20);
                    walWriter.commitWithParams(
                            ts("2026-01-01T00:00:01.000000Z"),
                            ts("2026-01-01T00:00:03.000000Z"),
                            WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                    );
                }
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertLiveViewValid();
            }

            assertQuery("SELECT ts, x, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:03.000000Z\t3\t1\n" +
                            "2026-01-01T00:00:04.000000Z\t4\t2\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testReplaceRangeSpanningTheBoundaryConverges() throws Exception {
        // A replace band that straddles the boundary: it deletes and rewrites base rows on both
        // sides of it in one atomic commit. The view must end up holding exactly the post-replace
        // base rows at or above the boundary - the deleted 00:03 gone, the rewritten 00:04 in its
        // new form, and nothing from the band's sub-boundary half.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-01-01T00:00:01.000000Z', 1)," +
                    "('2026-01-01T00:00:02.000000Z', 2)," +
                    "('2026-01-01T00:00:03.000000Z', 3)," +
                    "('2026-01-01T00:00:04.000000Z', 4)," +
                    "('2026-01-01T00:00:05.000000Z', 5)");
            drainWalQueue();

            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM '2026-01-01T00:00:03.000000Z' AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
                assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");

                // [00:02, 00:05) spans the boundary at 00:03. It deletes 00:02, 00:03 and 00:04,
                // then inserts one row below the boundary (00:02) and one above it (00:04).
                final TableToken baseToken = engine.verifyTableName("base");
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    appendRow(walWriter, ts("2026-01-01T00:00:02.000000Z"), 20);
                    appendRow(walWriter, ts("2026-01-01T00:00:04.000000Z"), 40);
                    walWriter.commitWithParams(
                            ts("2026-01-01T00:00:02.000000Z"),
                            ts("2026-01-01T00:00:05.000000Z"),
                            WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                    );
                }
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertLiveViewValid();
            }

            assertQuery("SELECT ts, x, rn FROM lv")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:04.000000Z\t40\t1\n" +
                            "2026-01-01T00:00:05.000000Z\t5\t2\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    private void assertLiveViewValid() {
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        Assert.assertFalse("LV must stay valid across a replay", instance.isInvalid());
    }
}
