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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * The view state writers this feature adds carry the REFRESH LIMIT frozen block (#7162) unchanged (design section
 * 10, rule 2): the invalidation, the base-change invalidation of the base identity check, the backlog update of an
 * invalid view, RESUME with and without SKIP BACKLOG, the refresh-interval ranges of a base UPDATE and of a base
 * ATTACH PARTITION, and a checkpoint with its restore. The block holds the backfill frontier, which keeps the refresh
 * boundary above the rows users backfilled into the frozen zone, also once the base and the view lose their newest
 * partitions and their own rows would let the boundary fall below the backfill.
 * <p>
 * Each writer is followed by a reload of the view states from disk, as at startup, so only what the writer
 * persisted counts, and by a FULL refresh, which rebuilds everything above the boundary: a lost frontier lets it
 * delete the backfilled row.
 */
public class MatViewFrozenZoneStateWritersTest extends AbstractCairoTest {
    // The view's rows after createViewWithBackfill(): the backfilled row in the frozen zone and the base's row above it.
    private static final String ROWS = """
            sym\tprice\tts
            a\t1.0\t2024-09-10T18:00:00.000000Z
            a\t6.0\t2024-09-12T12:00:00.000000Z
            """;

    @Test
    public void testAttachRangeKeepsBackfillFrontier() throws Exception {
        // The refresh-interval range of a base ATTACH PARTITION reaches a valid view's refresh intervals and an invalid
        // view's backlog. The base partition it attaches lies in the frozen zone, so no refresh writes its row into
        // the view, and the backfilled row stays.
        assertMemoryLeak(() -> {
            createViewWithBackfill();
            final TableToken viewToken = engine.verifyTableName("price_1h");

            detachAndAttachBasePartition("2024-09-10");
            restartStates();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertRows(ROWS);
            assertSurvivesRestartAndFull(ROWS);

            // the range reaches the backlog of the invalid view, and RESUME catches up on it
            invalidate(viewToken);
            detachAndAttachBasePartition("2024-09-10");
            restartStates();
            Assert.assertTrue(engine.getMatViewStateStore().getViewState(viewToken).isInvalid());
            execute("REFRESH MATERIALIZED VIEW price_1h RESUME");
            drainWalAndMatViewQueues();
            restartStates();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertRows(ROWS);
            assertSurvivesRestartAndFull(ROWS);
        });
    }

    @Test
    public void testBaseChangeInvalidationKeepsBackfillFrontier() throws Exception {
        // The refresh that finds the base table replaced invalidates the view through a view state event, which
        // carries the frozen block. FULL then rebuilds the view from the new base above the boundary only.
        assertMemoryLeak(() -> {
            createViewWithBackfill();
            final TableToken viewToken = engine.verifyTableName("price_1h");

            execute("DROP TABLE base");
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 5.0, '2024-09-10T12:00'),
                    ('a', 6.0, '2024-09-12T12:00')
                    """);
            drainWalQueue();
            // the restart loses the drop's queued invalidation, as a crash does, so the refresh's base identity
            // check catches the change
            restartStates();
            Assert.assertFalse(engine.getMatViewStateStore().getViewState(viewToken).isInvalid());
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views()").noLeakCheck().noRandomAccess()
                    .returns("view_status\tinvalidation_reason\ninvalid\t" + MatViewRefreshJob.BASE_TABLE_CHANGED_REASON + "\n");
            assertRows(ROWS);
            assertSurvivesRestartAndFull(ROWS);
        });
    }

    @Test
    public void testCheckpointRestoreKeepsBackfillFrontier() throws Exception {
        // CHECKPOINT CREATE copies the view's state with its frozen block, and a restore brings it back, so a FULL
        // after the restore keeps the backfilled row. CHECKPOINT CREATE needs sync(), which Windows does not have.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createViewWithBackfill();
            final String frontier = queryResult("SELECT backfill_max_ts FROM materialized_views()");
            execute("CHECKPOINT CREATE");
            // a refresh after the checkpoint does not survive the restore
            execute("INSERT INTO base VALUES ('a', 7.0, '2024-09-12T13:00')");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertRows(ROWS + "a\t7.0\t2024-09-12T13:00:00.000000Z\n");
            engine.clear();
            engine.closeNameRegistry();
            try (Path triggerPath = new Path()) {
                triggerPath.of(configuration.getDbRoot()).parent().concat(TableUtils.RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME);
                Assert.assertTrue(Files.touch(triggerPath.$()));
            }
            engine.checkpointRecover();
            engine.reloadTableNames();
            engine.getMetadataCache().onStartupAsyncHydrator();
            engine.buildViewGraphs();
            execute("CHECKPOINT RELEASE");

            assertQuery("SELECT backfill_max_ts FROM materialized_views()")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns(frontier);
            assertRows(ROWS);
            assertSurvivesRestartAndFull(ROWS);
        });
    }

    @Test
    public void testStateWritersKeepBackfillFrontier() throws Exception {
        assertMemoryLeak(() -> {
            createViewWithBackfill();
            final String rows = ROWS;
            assertSurvivesRestartAndFull(rows);

            final TableToken viewToken = engine.verifyTableName("price_1h");
            // the invalidation
            invalidate(viewToken);
            restartStates();
            assertSurvivesRestartAndFull(rows);

            // the backlog update of the invalid view, then RESUME catching up
            invalidate(viewToken);
            execute("INSERT INTO base VALUES ('a', 7.0, '2024-09-12T13:00')");
            drainWalQueue();
            engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken);
            drainWalAndMatViewQueues();
            restartStates();
            execute("REFRESH MATERIALIZED VIEW price_1h RESUME");
            drainWalAndMatViewQueues();
            restartStates();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            final String caughtUpRows = rows + """
                    a\t7.0\t2024-09-12T13:00:00.000000Z
                    """;
            assertRows(caughtUpRows);
            assertSurvivesRestartAndFull(caughtUpRows);

            // the refresh-interval range of a base UPDATE
            execute("UPDATE base SET price = 8.0 WHERE ts = '2024-09-12T13:00'");
            drainWalAndMatViewQueues();
            final String updatedRows = rows + """
                    a\t8.0\t2024-09-12T13:00:00.000000Z
                    """;
            assertSurvivesRestartAndFull(updatedRows);

            // RESUME SKIP BACKLOG
            invalidate(viewToken);
            execute("REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG");
            drainWalAndMatViewQueues();
            assertSurvivesRestartAndFull(updatedRows);
        });
    }

    private void assertRows(String expected) throws Exception {
        assertQuery("price_1h ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(expected);
    }

    // Reloads the view states, rebuilds the view with FULL and checks that the backfilled row is still there.
    private void assertSurvivesRestartAndFull(String expected) throws Exception {
        restartStates();
        execute("REFRESH MATERIALIZED VIEW price_1h FULL");
        // FULL on an invalid view waits for the view's WAL, which the apply job then redelivers it after
        drainWalAndMatViewQueues();
        drainWalAndMatViewQueues();
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views()").noLeakCheck().noRandomAccess()
                .returns("view_status\tinvalidation_reason\nvalid\t\n");
        assertRows(expected);
    }

    // A base and a REFRESH LIMIT 2 DAYS view whose frozen zone holds a backfilled row. The base and the view then
    // lose their newest partitions: a boundary derived from their rows alone, two days below 2024-09-12T12:00, would
    // fall below the backfilled row.
    private void createViewWithBackfill() throws Exception {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                ('a', 5.0, '2024-09-10T12:00'),
                ('a', 6.0, '2024-09-12T12:00'),
                ('a', 9.0, '2024-09-13T12:00')
                """);
        drainWalQueue();
        execute("CREATE MATERIALIZED VIEW price_1h REFRESH MANUAL DEFERRED AS (SELECT sym, last(price) AS price, ts FROM base SAMPLE BY 1h) PARTITION BY DAY");
        execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT 2 DAYS");
        drainWalAndMatViewQueues();
        // anchor min(2024-09-13T12:00, now), boundary two days below it, at 2024-09-11T12:00
        currentMicros = parseFloorPartialTimestamp("2024-09-13T12:30:00.000000Z");
        execute("REFRESH MATERIALIZED VIEW price_1h FULL");
        drainWalAndMatViewQueues();
        assertQuery("SELECT backfill_max_ts FROM materialized_views()").noLeakCheck().noRandomAccess()
                .returns("backfill_max_ts\n2024-09-11T12:00:00.000000Z\n");
        // a backfilled row in the frozen zone
        execute("INSERT INTO price_1h VALUES ('a', 1.0, '2024-09-10T18:00')");
        drainWalAndMatViewQueues();

        execute("ALTER TABLE base DROP PARTITION LIST '2024-09-13'");
        execute("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-09-13'");
        drainWalAndMatViewQueues();
    }

    // Detaches a base partition and attaches it back; the attach publishes its range to the base's views.
    private static void detachAndAttachBasePartition(String partition) throws Exception {
        execute("ALTER TABLE base DETACH PARTITION LIST '" + partition + "'");
        drainWalQueue();
        final TableToken baseToken = engine.verifyTableName("base");
        try (Path path = new Path(); Path other = new Path()) {
            path.of(configuration.getDbRoot()).concat(baseToken).concat(partition).put(TableUtils.DETACHED_DIR_MARKER).$();
            other.of(configuration.getDbRoot()).concat(baseToken).concat(partition).put(configuration.getAttachPartitionSuffix()).$();
            Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);
        }
        execute("ALTER TABLE base ATTACH PARTITION LIST '" + partition + "'");
        drainWalAndMatViewQueues();
    }

    private static String queryResult(String sql) throws Exception {
        sink.clear();
        printSql(sql, sink);
        return sink.toString();
    }

    private static void invalidate(TableToken viewToken) {
        engine.getMatViewStateStore().enqueueInvalidate(viewToken, "test invalidation");
        drainWalAndMatViewQueues();
    }

    // A restart of the view states: the store reloads them from disk, as at startup.
    private static void restartStates() {
        engine.getMatViewStateStore().clear();
        engine.hydrateMatViewStateStore();
    }
}
