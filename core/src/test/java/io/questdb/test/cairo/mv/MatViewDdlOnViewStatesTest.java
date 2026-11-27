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
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.griffin.SqlException;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * View column and partition DDL on views that are invalid, refresh on a timer or refresh by period. The DDL changes
 * the view's columns and rows and nothing else (invariant 12 of the design): an invalid view stays invalid with its
 * reason and its backlog, a period view keeps its last refreshed period, and a timer view keeps refreshing on its
 * timer. Expected rows: a view bucket holds the last price of its symbol's base rows within the hour.
 */
public class MatViewDdlOnViewStatesTest extends AbstractCairoTest {
    private static final String VIEW_SQL = "SELECT ts, sym, last(price) AS price, last(amount) AS amount FROM base SAMPLE BY 1h";

    @Test
    public void testDdlOnInvalidViewKeepsItInvalidWithBacklog() throws Exception {
        assertMemoryLeak(() -> {
            createBaseWithRows();
            execute("CREATE MATERIALIZED VIEW price_1h AS (" + VIEW_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final TableToken viewToken = engine.verifyTableName("price_1h");
            engine.getMatViewStateStore().enqueueInvalidate(viewToken, "test invalidation");
            drainWalAndMatViewQueues();
            // a base change the invalid view misses, and keeps in its backlog
            execute("INSERT INTO base VALUES ('c', 3.5, 30.0, '2024-01-03T00:30:00.000000Z')");
            drainWalAndMatViewQueues();

            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra INT");
            execute("ALTER MATERIALIZED VIEW price_1h DROP COLUMN amount");
            execute("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-01-01'");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_status, invalidation_reason, refresh_base_table_txn FROM materialized_views()")
                    .noLeakCheck().noRandomAccess().returns("""
                            view_status\tinvalidation_reason\trefresh_base_table_txn
                            invalid\ttest invalidation\t1
                            """);
            assertRows("""
                    ts\tsym\tprice\textra
                    2024-01-02T00:00:00.000000Z\tb\t2.5\tnull
                    """);

            // RESUME catches up on the backlog; the dropped day stays empty, as the base did not change there
            execute("REFRESH MATERIALIZED VIEW price_1h RESUME");
            drainWalAndMatViewQueues();
            assertViewValid();
            assertRows("""
                    ts\tsym\tprice\textra
                    2024-01-02T00:00:00.000000Z\tb\t2.5\tnull
                    2024-01-03T00:00:00.000000Z\tc\t3.5\tnull
                    """);
        });
    }

    @Test
    public void testDdlOnPeriodViewKeepsItsPeriod() throws Exception {
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2024-01-01T00:00:00.000000Z");
            createBaseWithRows();
            execute("CREATE MATERIALIZED VIEW price_1h REFRESH IMMEDIATE PERIOD (LENGTH 1d) AS (" + VIEW_SQL + ") PARTITION BY DAY");
            currentMicros = parseFloorPartialTimestamp("2024-01-03T01:00:00.000000Z");
            drainWalAndMatViewQueues();
            final MatViewState state = engine.getMatViewStateStore().getViewState(engine.verifyTableName("price_1h"));
            final long periodHi = parseFloorPartialTimestamp("2024-01-03T00:00:00.000000Z");
            Assert.assertEquals(periodHi, state.getLastPeriodHi());
            assertRows("""
                    ts\tsym\tprice\tamount
                    2024-01-01T00:00:00.000000Z\ta\t1.5\t10.0
                    2024-01-02T00:00:00.000000Z\tb\t2.5\t20.0
                    """);

            execute("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-01-01'");
            execute("ALTER MATERIALIZED VIEW price_1h DROP COLUMN amount");
            drainWalAndMatViewQueues();
            Assert.assertEquals(periodHi, state.getLastPeriodHi());
            assertQuery("SELECT refresh_base_table_txn FROM materialized_views()").noLeakCheck().noRandomAccess()
                    .returns("refresh_base_table_txn\n1\n");
            assertViewValid();

            // a row in the current period reaches the view once the period completes
            execute("INSERT INTO base VALUES ('c', 3.5, 30.0, '2024-01-03T00:30:00.000000Z')");
            drainWalAndMatViewQueues();
            assertRows("""
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\tb\t2.5
                    """);
            // the next base commit after the period ends triggers the refresh, which leaves the new day's row to the
            // next period
            currentMicros = parseFloorPartialTimestamp("2024-01-04T01:00:00.000000Z");
            execute("INSERT INTO base VALUES ('d', 4.5, 40.0, '2024-01-04T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewValid();
            Assert.assertEquals(parseFloorPartialTimestamp("2024-01-04T00:00:00.000000Z"), state.getLastPeriodHi());
            assertRows("""
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\tb\t2.5
                    2024-01-03T00:00:00.000000Z\tc\t3.5
                    """);
        });
    }

    @Test
    public void testDdlOnTimerViewKeepsItsTimer() throws Exception {
        assertMemoryLeak(() -> {
            currentMicros = parseFloorPartialTimestamp("2024-02-01T00:00:00.000000Z");
            createBaseWithRows();
            execute("CREATE MATERIALIZED VIEW price_1h REFRESH EVERY 1h AS (" + VIEW_SQL + ") PARTITION BY DAY");
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainWalQueue();
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues();
            assertRows("""
                    ts\tsym\tprice\tamount
                    2024-01-01T00:00:00.000000Z\ta\t1.5\t10.0
                    2024-01-02T00:00:00.000000Z\tb\t2.5\t20.0
                    """);

            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra INT");
            execute("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-01-02'");
            execute("INSERT INTO base VALUES ('c', 3.5, 30.0, '2024-01-03T00:30:00.000000Z')");
            drainWalAndMatViewQueues();
            // the base row waits for the timer
            assertRows("""
                    ts\tsym\tprice\tamount\textra
                    2024-01-01T00:00:00.000000Z\ta\t1.5\t10.0\tnull
                    """);
            currentMicros += Micros.HOUR_MICROS;
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues();
            assertViewValid();
            assertRows("""
                    ts\tsym\tprice\tamount\textra
                    2024-01-01T00:00:00.000000Z\ta\t1.5\t10.0\tnull
                    2024-01-03T00:00:00.000000Z\tc\t3.5\t30.0\tnull
                    """);
        });
    }

    private void assertRows(String expected) throws Exception {
        assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(expected);
    }

    private void assertViewValid() throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views()").noLeakCheck().noRandomAccess()
                .returns("view_status\tinvalidation_reason\nvalid\t\n");
    }

    // Base rows a on 2024-01-01 and b on 2024-01-02, both in their day's first hour.
    private static void createBaseWithRows() throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                ('a', 1.5, 10.0, '2024-01-01T00:10:00.000000Z'),
                ('b', 2.5, 20.0, '2024-01-02T00:20:00.000000Z')
                """);
        drainWalQueue();
    }
}
