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
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * The view state writers this feature adds carry the REFRESH LIMIT frozen block (#7162) unchanged (design section
 * 10, rule 2): the invalidation, the backlog update of an invalid view, RESUME with and without SKIP BACKLOG, and
 * the refresh-interval range of a base UPDATE. The block holds the backfill frontier, which keeps the refresh
 * boundary above the rows users backfilled into the frozen zone, also once the base and the view lose their newest
 * partitions and their own rows would let the boundary fall below the backfill.
 * <p>
 * Each writer is followed by a reload of the view states from disk, as at startup, so only what the writer
 * persisted counts, and by a FULL refresh, which rebuilds everything above the boundary: a lost frontier lets it
 * delete the backfilled row.
 */
public class MatViewFrozenZoneStateWritersTest extends AbstractCairoTest {

    @Test
    public void testStateWritersKeepBackfillFrontier() throws Exception {
        assertMemoryLeak(() -> {
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

            // The base and the view lose their newest partitions: a boundary derived from their rows alone, two days
            // below 2024-09-12T12:00, would fall below the backfilled row.
            execute("ALTER TABLE base DROP PARTITION LIST '2024-09-13'");
            execute("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-09-13'");
            drainWalAndMatViewQueues();
            final String rows = """
                    sym\tprice\tts
                    a\t1.0\t2024-09-10T18:00:00.000000Z
                    a\t6.0\t2024-09-12T12:00:00.000000Z
                    """;
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
