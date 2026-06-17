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

import io.questdb.PropertyKey;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class MatViewRefreshRetryTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public MatViewRefreshRetryTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testLargeTransaction() throws Exception {
        // Here, we're reproducing scenario when the sample by iterator step estimation was
        // too generous, and we ended up with large transactions even in a single iteration.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_INSERT_AS_SELECT_BATCH_SIZE, 100);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, 1000);
        assertMemoryLeak(() -> {
            CharSequence sqlText = "create table base_price (" +
                    "sym varchar, price double, ts #TIMESTAMP" +
                    ") timestamp(ts) partition by DAY WAL;";

            sqlText = sqlText.toString().replaceAll("#TIMESTAMP", timestampType.getTypeName());
            engine.execute(sqlText, sqlExecutionContext);

            execute(
                    "insert into base_price select " +
                            "  rnd_symbol(10000,4,32,100) sym, " +
                            "  rnd_double() price, " +
                            "  timestamp_sequence(400000000000, 1000000) ts " +
                            "from long_sequence(10000);"
            );
            drainWalAndMatViewQueues();

            execute(
                    "create materialized view price_1h as (" +
                            "  select ts, sym, avg(price) as avg_price from base_price sample by 10s" +
                            ") partition by hour"
            );

            drainWalAndMatViewQueues();
            assertQuery("select view_name, view_status from materialized_views")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            view_name\tview_status
                            price_1h\tvalid
                            """);

            assertQuery("select count() from wal_transactions('price_1h')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            56
                            """);
        });
    }

    @Test
    public void testOom() throws Exception {
        testOom(false);
    }

    @Test
    public void testOomParallelSql() throws Exception {
        testOom(true);
    }

    private void testOom(boolean enableParallelSql) throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_OOM_RETRY_TIMEOUT, 1);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, String.valueOf(enableParallelSql));
        // Defer OOM refreshes without backoff and cap them low, so a few re-drives exhaust the limit.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_TIMEOUT, 0);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BUSY_RETRY_LIMIT, 2);
        assertMemoryLeak(() -> {
            CharSequence sqlText = "create table base_price (" +
                    "sym varchar, price double, ts #TIMESTAMP" +
                    ") timestamp(ts) partition by DAY WAL;";

            sqlText = sqlText.toString().replaceAll("#TIMESTAMP", timestampType.getTypeName());
            engine.execute(sqlText, sqlExecutionContext);

            execute(
                    "insert into base_price select " +
                            "  rnd_symbol(10000,4,32,100) sym, " +
                            "  rnd_double() price, " +
                            "  timestamp_sequence(400000000000, 500000) ts " +
                            "from long_sequence(100000);"
            );
            drainWalAndMatViewQueues();

            execute(
                    "create materialized view price_1h as (" +
                            "  select ts, sym, avg(price) as avg_price from base_price sample by 10s" +
                            ") partition by hour"
            );

            // Set RSS limit, so that the refresh will fail due to OOM.
            Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 500 * 1024); // 500KB gap
            drainWalAndMatViewQueues();
            // OOM no longer invalidates immediately: the incremental refresh is deferred and the
            // view reports "retrying" while it backs off.
            assertQuery("select view_name, view_status from materialized_views")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            view_name\tview_status
                            price_1h\tretrying
                            """);

            // Keep re-driving the deferred refresh. Once the retry limit is exceeded the view is
            // invalidated, which releases base-table WAL retention. Extra re-drives are no-ops once
            // the view is invalid.
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            for (int i = 0; i < 5; i++) {
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues();
            }
            assertQuery("select view_name, view_status from materialized_views")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            view_name\tview_status
                            price_1h\tinvalid
                            """);

            // Now, remove the limit and run full refresh. This time, it should succeed.
            Unsafe.setRssMemLimit(0);
            execute("refresh materialized view price_1h full;");
            drainWalAndMatViewQueues();
            assertQuery("select view_name, view_status from materialized_views")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            view_name\tview_status
                            price_1h\tvalid
                            """);
        });
    }
}
