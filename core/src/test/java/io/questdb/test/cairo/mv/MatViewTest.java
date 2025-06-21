/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewRefreshExecutionContext;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestTimestampCounterFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Files;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.cairo.TableUtils.DETACHED_DIR_MARKER;
import static io.questdb.griffin.model.IntervalUtils.parseFloorPartialTimestamp;


@RunWith(Parameterized.class)
public class MatViewTest extends AbstractCairoTest {
    private final int rowsPerQuery;

    public MatViewTest(int rowsPerQuery) {
        this.rowsPerQuery = rowsPerQuery;
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // override default to test copy
        inputRoot = TestUtils.getCsvRoot();
        inputWorkRoot = TestUtils.unchecked(() -> temp.newFolder("imports" + System.nanoTime()).getAbsolutePath());
        AbstractCairoTest.setUpStatic();
    }

    @Parameterized.Parameters(name = "rows_per_query={0}")
    public static Collection<Object[]> testParams() {
        // only run a single combination per CI run
        final Rnd rnd = TestUtils.generateRandom(LOG);
        if (rnd.nextInt(100) > 50) {
            return Arrays.asList(new Object[][]{{-1}});
        }
        return Arrays.asList(new Object[][]{{1}});
    }

    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        if (rowsPerQuery > 0) {
            setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, rowsPerQuery);
        }
    }

    @Test
    public void testAlterMixed() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "  sym symbol, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute("insert into base_price(sym, price, ts) values ('gbpusd', 1.320, '2024-09-10T12:01')");
            // set refresh limit
            execute("alter materialized view price_1h set refresh limit 2 hours;");
            currentMicros = parseFloorPartialTimestamp("2024-09-10T16:00:00.000000Z");
            drainQueues();

            // expect new limit
            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-09-10T16:00:00.000000Z\t2024-09-10T16:00:00.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t1\t1\t2\tHOUR\t\t0\t\n",
                    "materialized_views",
                    null
            );

            // insert a few old timestamps
            execute(
                    "insert into base_price(sym, price, ts) values ('gbpusd', 1.320, '2024-09-01T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-01T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-01T12:02')"
            );
            currentMicros = parseFloorPartialTimestamp("2024-09-10T16:00:00.000000Z");
            drainQueues();

            // all old timestamps should be ignored
            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.32\t2024-09-10T12:00:00.000000Z\n",
                    "price_1h order by sym, ts"
            );

            // change symbol capacity
            execute("alter materialized view price_1h alter column sym symbol capacity 1000;");
            drainQueues();

            // expect new capacity
            assertSql(
                    "column\tsymbolCapacity\nsym\t1024\n",
                    "select \"column\", symbolCapacity from (show columns from price_1h) where type = 'SYMBOL'"
            );

            // change TTL
            execute("alter materialized view price_1h set TTL 2 DAYS;");
            drainQueues();

            // insert future timestamps
            execute(
                    "insert into base_price(sym, price, ts) values ('gbpusd', 1.320, '2024-09-30T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-30T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-30T12:02')"
            );
            drainQueues();

            // older partition should be dropped due to TTL
            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-30T12:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-30T12:00:00.000000Z\n",
                    "price_1h order by sym, ts"
            );
        });
    }

    @Test
    public void testAlterRefreshLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "  sym symbol, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute(
                    "insert into base_price(sym, price, ts) values ('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = parseFloorPartialTimestamp("2024-01-01T01:01:01.842574Z");
            drainQueues();

            // expect no limit
            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t1\t1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n",
                    "price_1h order by sym, ts"
            );

            // set refresh limit
            execute("alter materialized view price_1h set refresh limit 1 day;");
            drainQueues();

            // expect new limit
            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t1\t1\t1\tDAY\t\t0\t\n",
                    "materialized_views",
                    null
            );

            // insert a few old timestamps and a single newer one
            execute(
                    "insert into base_price(sym, price, ts) values ('gbpusd', 1.320, '2024-09-01T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-01T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-01T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T15:02')"
            );
            currentMicros = parseFloorPartialTimestamp("2024-09-10T16:00:00.000000Z");
            drainQueues();

            // the older timestamps should be ignored
            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T15:00:00.000000Z\n" + // the newer timestamp
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n",
                    "price_1h order by sym, ts"
            );

            // disable refresh limit
            execute("alter materialized view price_1h set refresh limit 0 hour;");
            drainQueues();

            // expect new limit
            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-09-10T16:00:00.000000Z\t2024-09-10T16:00:00.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t2\t2\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );

            // insert old timestamps once again
            execute(
                    "insert into base_price(sym, price, ts) values ('gbpusd', 1.320, '2024-09-01T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-01T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-01T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T15:02')"
            );
            drainQueues();

            // the older timestamps should be aggregated
            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-01T12:00:00.000000Z\n" + // old timestamp
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T15:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-01T12:00:00.000000Z\n" + // old timestamp
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n",
                    "price_1h order by sym, ts"
            );
        });
    }

    @Test
    public void testAlterRefreshLimitInvalidStatement() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "  sym symbol, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h set refresh",
                    44,
                    "'limit' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h set refresh;",
                    44,
                    "'limit' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h set refresh limit;",
                    51,
                    "missing argument, should be <number> <unit> or <number_with_unit>"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h set refresh limit foobar;",
                    51,
                    "invalid argument, should be <number> <unit> or <number_with_unit>"
            );

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t\t\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t-1\t0\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );
        });
    }

    @Test
    public void testAlterRefreshTimer() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute(
                    "create materialized view price_1h refresh start '2999-12-12T12:00:00.000000Z' every 1h as (" +
                            "select sym, last(price) as price, ts from base_price sample by 1h" +
                            ") partition by day"
            );

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            // no refresh should happen as the start timestamp is in future
            final String start = "1999-01-01T01:01:01.842574Z";
            currentMicros = parseFloorPartialTimestamp(start);
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n",
                    "price_1h order by sym"
            );
            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental_timer\tbase_price\t\t\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t-1\t1\t0\t\t2999-12-12T12:00:00.000000Z\t1\tHOUR\n",
                    "materialized_views",
                    null
            );

            // the view should refresh after we change the timer schedule
            execute("alter materialized view price_1h set refresh start '" + start + "' every 1m;");
            drainQueues();
            // we need timers to tick
            currentMicros += Timestamps.MINUTE_MICROS;
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental_timer\tbase_price\t1999-01-01T01:02:01.842574Z\t1999-01-01T01:02:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t1\t1\t0\t\t1999-01-01T01:01:01.842574Z\t1\tMINUTE\n",
                    "materialized_views",
                    null
            );
            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n",
                    "price_1h order by sym"
            );
        });
    }

    @Test
    public void testAlterRefreshTimerInvalidStatement() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "  sym symbol, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            currentMicros = 0;
            execute("create materialized view price_1h as select sym, last(price) as price, ts from base_price sample by 1h");
            execute("create materialized view price_1h_t refresh every 1h as select sym, last(price) as price, ts from base_price sample by 1h");

            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h set refresh every 1h",
                    45,
                    "materialized view must be of timer refresh type"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh",
                    46,
                    "'start' or 'every' or 'limit' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh start",
                    52,
                    "START timestamp"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh start 'foobar'",
                    53,
                    "invalid START timestamp value"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh start '2020-09-10T20:00:00.000000Z'",
                    82,
                    "'every' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh start '2020-09-10T20:00:00.000000Z' barbaz",
                    83,
                    "'every' expected"
            );
            assertExceptionNoLeakCheck(
                    "ALTER MATERIALIZED VIEW 'price_1h_t' SET REFRESH START '2020-09-10T20:00:00.000000Z' EVERY",
                    90,
                    "interval expected"
            );
            assertExceptionNoLeakCheck(
                    "ALTER MATERIALIZED VIEW 'price_1h_t' SET REFRESH START '2020-09-10T20:00:00.000000Z' EVERY foobaz",
                    91,
                    "Invalid unit: foobaz"
            );
            assertExceptionNoLeakCheck(
                    "ALTER MATERIALIZED VIEW 'price_1h_t' SET REFRESH EVERY foobaz",
                    55,
                    "Invalid unit: foobaz"
            );
            assertExceptionNoLeakCheck(
                    "ALTER MATERIALIZED VIEW 'price_1h_t' SET REFRESH EVERY 1s;",
                    55,
                    "unsupported interval unit: s, supported units are 'm', 'h', 'd', 'w', 'y', 'M'"
            );

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t\t\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t-1\t0\t0\t\t\t0\t\n" +
                            "price_1h_t\tincremental_timer\tbase_price\t\t\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h_t~3\t\tvalid\t-1\t0\t0\t\t1970-01-01T00:00:00.000000Z\t1\tHOUR\n",
                    "materialized_views order by view_name",
                    null,
                    true
            );
        });
    }

    @Test
    public void testAlterSymbolCapacity() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "  sym symbol, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute(
                    "insert into base_price(sym, price, ts) values ('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = parseFloorPartialTimestamp("2024-01-01T01:01:01.842574Z");
            drainQueues();

            // expect default capacity
            assertSql(
                    "column\tsymbolCapacity\nsym\t128\n",
                    "select \"column\", symbolCapacity from (show columns from price_1h) where type = 'SYMBOL'"
            );

            // change sym capacity
            execute("alter materialized view price_1h alter column sym symbol capacity 1000");
            drainQueues();

            // expect larger capacity
            assertSql(
                    "column\tsymbolCapacity\nsym\t1024\n",
                    "select \"column\", symbolCapacity from (show columns from price_1h) where type = 'SYMBOL'"
            );

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t1\t1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n",
                    "price_1h order by sym"
            );
        });
    }

    @Test
    public void testAlterSymbolCapacityInvalidStatement() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "  sym symbol, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            assertExceptionNoLeakCheck(
                    "alter materialized foobar;",
                    19,
                    "'view' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view foobar;",
                    24,
                    "table does not exist [table=foobar]"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h",
                    32,
                    "'alter' or 'resume' or 'suspend' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h foobar",
                    33,
                    "'alter' or 'resume' or 'suspend' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter",
                    38,
                    "'column' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter foobar;",
                    39,
                    "'column' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column foobar",
                    46,
                    "column 'foobar' does not exist in materialized view 'price_1h'"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column sym;",
                    49,
                    "'symbol' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column sym foobar;",
                    50,
                    "'symbol' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column price symbol;",
                    46,
                    "column 'price' is not of symbol type"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column sym symbol",
                    56,
                    "'capacity' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column sym symbol capacity;",
                    65,
                    "numeric capacity expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column sym symbol capacity -42;",
                    66,
                    "min symbol capacity is 2"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column sym symbol capacity 1073741825;",
                    66,
                    "max symbol capacity is 1073741824"
            );
            assertExceptionNoLeakCheck(
                    "ALTER MATERIALIZED VIEW price_1h ALTER COLUMN sym SYMBOL CAPACITY 42 foobar;",
                    69,
                    "unexpected token [foobar] while trying to change symbol capacity"
            );

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t\t\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t-1\t0\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );
        });
    }

    @Test
    public void testAlterTtl() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            execute("create materialized view price_1h as (select sym, last(price) as price, ts from base_price sample by 1h) partition by DAY;");
            execute("alter materialized view price_1h set ttl 2d;");

            execute(
                    "insert into base_price values('gbpusd', 1.310, '2024-09-10T12:05')" +
                            ",('gbpusd', 1.311, '2024-09-11T13:03')" +
                            ",('gbpusd', 1.312, '2024-09-12T13:03')" +
                            ",('gbpusd', 1.313, '2024-09-13T13:03')" +
                            ",('gbpusd', 1.314, '2024-09-14T13:03')"
            );
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.312\t2024-09-12T13:00:00.000000Z\n" +
                            "gbpusd\t1.313\t2024-09-13T13:00:00.000000Z\n" +
                            "gbpusd\t1.314\t2024-09-14T13:00:00.000000Z\n",
                    "price_1h order by ts, sym",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testAlterTtlInvalidStatement() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "  sym symbol, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h set",
                    36,
                    "'ttl' or 'refresh' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h set ttl",
                    40,
                    "missing argument, should be <number> <unit> or <number_with_unit>"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h set ttl;",
                    41,
                    "missing argument, should be <number> <unit> or <number_with_unit>"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h set ttl foobar",
                    41,
                    "invalid argument, should be <number> <unit> or <number_with_unit>"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h set ttl 1 hour;",
                    41,
                    "TTL value must be an integer multiple of partition size"
            );

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t\t\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t-1\t0\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );
        });
    }

    @Test
    public void testAsOfJoinBinarySearchHintInMatView() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table trades as (\n" +
                            "  select \n" +
                            "    rnd_double() price,\n" +
                            "    rnd_double() volume,\n" +
                            "    ('2025'::timestamp + x * 200_000_000L + rnd_int(0, 10_000, 0))::timestamp as ts,\n" +
                            "  from long_sequence(5_000)\n" +
                            ") timestamp(ts) partition by day wal;"
            );

            execute(
                    "create table prices as (\n" +
                            "  select \n" +
                            "    rnd_double() bid,\n" +
                            "    rnd_double() ask,\n" +
                            "    rnd_boolean() valid,\n" +
                            "    ('2025'::timestamp + x * 1_000_000L + rnd_int(0, 10_000, 0))::timestamp as ts,\n" +
                            "  from long_sequence(1_000_000)\n" +
                            ") timestamp(ts) partition by day wal;\n"
            );

            final String mvWithoutHint = "create materialized view daily_summary \n" +
                    "WITH BASE trades\n" +
                    "as (\n" +
                    "select trades.ts, count(*), sum(volume), min(price), max(price), avg(price)\n" +
                    "FROM trades\n" +
                    "asof join (select * from prices where valid) prices\n" +
                    "sample by 1d\n" +
                    ");";
            final String mvWithHint = "create materialized view daily_summary \n" +
                    "WITH BASE trades\n" +
                    "as (\n" +
                    "select /*+ USE_ASOF_BINARY_SEARCH(trades prices) */ trades.ts, count(*), sum(volume), min(price), max(price), avg(price)\n" +
                    "FROM trades\n" +
                    "asof join (select * from prices where valid) prices\n" +
                    "sample by 1d\n" +
                    ");";

            // sanity test: without the hint it does not use binary search
            sink.clear();
            printSql("EXPLAIN " + mvWithoutHint);
            TestUtils.assertContains(sink, "AsOf Join");
            TestUtils.assertNotContains(sink, "Fast Scan");

            // but it does with the hint
            sink.clear();
            printSql("EXPLAIN " + mvWithHint);
            TestUtils.assertContains(sink, "Filtered AsOf Join Fast Scan");

            // ok, now the real data: first try the view without the hint
            execute(mvWithoutHint);
            drainQueues();
            final String expectedView = "ts\tcount\tsum\tmin\tmax\tavg\n" +
                    "2025-01-01T00:00:00.000000Z\t431\t215.12906540853268\t0.0031075670450616544\t0.9975907992178104\t0.4923297830071461\n" +
                    "2025-01-02T00:00:00.000000Z\t432\t214.8933638390628\t0.0027013057617086833\t0.9997998069306392\t0.5363814932706943\n" +
                    "2025-01-03T00:00:00.000000Z\t432\t211.63403995544482\t0.0014510055926236776\t0.9979936641680203\t0.4900138748185357\n" +
                    "2025-01-04T00:00:00.000000Z\t432\t225.1870697913935\t0.0026339327135822543\t0.9996217482017493\t0.49406088823120226\n" +
                    "2025-01-05T00:00:00.000000Z\t432\t213.8124549264717\t0.00985149958244913\t0.9981734770138071\t0.4728684440748092\n" +
                    "2025-01-06T00:00:00.000000Z\t432\t214.8994847762188\t0.0010433040681515626\t0.9998120012952196\t0.48758818235506823\n" +
                    "2025-01-07T00:00:00.000000Z\t432\t220.0881500794553\t0.0014542249844708977\t0.9973956570924076\t0.5131734923704387\n" +
                    "2025-01-08T00:00:00.000000Z\t432\t218.41372811829154\t8.166095924849737E-4\t0.9976953158075262\t0.5276052830143888\n" +
                    "2025-01-09T00:00:00.000000Z\t432\t220.10482943202246\t0.0011023415061862663\t0.9974983068581821\t0.493060539742248\n" +
                    "2025-01-10T00:00:00.000000Z\t432\t208.43848337612906\t0.0028067126112681917\t0.9976283386812487\t0.5136095078793146\n" +
                    "2025-01-11T00:00:00.000000Z\t432\t213.02005186038846\t8.598501058093566E-4\t0.999708216046598\t0.5040670959429089\n" +
                    "2025-01-12T00:00:00.000000Z\t249\t119.80938485754517\t0.007906045439897036\t0.9962991313334122\t0.4923923393746041\n";
            assertQuery(expectedView, "SELECT * FROM daily_summary", "ts", true, true);

            // now, recreate the view with hint
            execute("drop materialized view daily_summary");
            execute(mvWithHint);
            drainQueues();

            // it must result in the same data
            assertQuery(expectedView, "SELECT * FROM daily_summary", "ts", true, true);
        });
    }

    @Test
    public void testBaseTableInvalidateOnAttachPartition() throws Exception {
        final String partition = "2024-01-01";
        testBaseTableInvalidateOnOperation(
                () -> {
                    // insert a few rows to have a detachable partition
                    execute(
                            "insert into base_price (sym, price, ts) values('gbpusd', 1.223, '" + partition + "T00:01')," +
                                    "('gbpusd', 1.423, '2024-01-02T00:01');"
                    );
                    execute("alter table base_price detach partition list '" + partition + "';");
                    drainQueues();
                    // rename to .attachable
                    try (Path path = new Path(); Path other = new Path()) {
                        TableToken tableToken = engine.verifyTableName("base_price");
                        path.of(configuration.getDbRoot()).concat(tableToken).concat(partition).put(DETACHED_DIR_MARKER).$();
                        other.of(configuration.getDbRoot()).concat(tableToken).concat(partition).put(configuration.getAttachPartitionSuffix()).$();
                        Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);
                    }
                },
                "alter table base_price attach partition list '" + partition + "';",
                "attach partition operation"
        );
    }

    @Test
    public void testBaseTableInvalidateOnChangeColumnType() throws Exception {
        testBaseTableInvalidateOnOperation("alter table base_price alter column amount type long;", "change column type operation");
    }

    @Test
    public void testBaseTableInvalidateOnDedupEnable() throws Exception {
        testBaseTableNoInvalidateOnOperation("alter table base_price dedup enable upsert keys(ts);");
    }

    @Test
    public void testBaseTableInvalidateOnDetachPartition() throws Exception {
        testBaseTableInvalidateOnOperation("alter table base_price detach partition where ts > 0;", "detach partition operation");
    }

    @Test
    public void testBaseTableInvalidateOnDropColumn() throws Exception {
        testBaseTableInvalidateOnOperation("alter table base_price drop column amount;", "drop column operation");
    }

    @Test
    public void testBaseTableInvalidateOnDropPartition() throws Exception {
        testBaseTableInvalidateOnOperation("alter table base_price drop partition where ts > 0;", "drop partition operation");
    }

    @Test
    public void testBaseTableInvalidateOnDropTable() throws Exception {
        testBaseTableInvalidateOnOperation("drop table base_price;", "base table is dropped or renamed");
    }

    @Test
    public void testBaseTableInvalidateOnRenameColumn() throws Exception {
        testBaseTableInvalidateOnOperation("alter table base_price rename column amount to amount2;", "rename column operation");
    }

    @Test
    public void testBaseTableInvalidateOnTruncate() throws Exception {
        testBaseTableInvalidateOnOperation("truncate table base_price;", "truncate operation");
    }

    @Test
    public void testBaseTableInvalidateOnUpdate() throws Exception {
        testBaseTableInvalidateOnOperation("update base_price set amount = 42;", "update operation");
    }

    @Test
    public void testBaseTableNameCaseSensitivity() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE 'GLBXMDP3_mbp1_es' ( " +
                            "       ts_event TIMESTAMP, " +
                            "       action SYMBOL, " +
                            "       price DOUBLE, " +
                            "       size INT, " +
                            "       sequence LONG " +
                            ") TIMESTAMP(ts_event) PARTITION BY HOUR WAL " +
                            "DEDUP UPSERT KEYS(ts_event,sequence);"
            );
            final String viewSql = "SELECT ts_event AS time, " +
                    "  first(price) AS open, " +
                    "  max(price)   AS high, " +
                    "  min(price)   AS low, " +
                    "  last(price)  AS close, " +
                    "  sum(size)    AS volume " +
                    "FROM glbxmdp3_mbp1_es " +
                    "WHERE action = 'T' " +
                    "SAMPLE BY 1s ALIGN TO CALENDAR";
            execute("CREATE MATERIALIZED VIEW 'mv_es_ohlcv_1s' WITH BASE 'glbxmdp3_mbp1_es' as (" + viewSql + ") partition by DAY");

            currentMicros = parseFloorPartialTimestamp("2024-10-24T17:22:09.842574Z");
            drainQueues();

            execute(
                    "insert into 'GLBXMDP3_mbp1_es' values ('2024-09-10T12:01', 'T', 42, 42, 0)" +
                            ",('2024-09-10T12:02', 'T', 42, 42, 1)" +
                            ",('2024-09-10T12:03', 'T', 42, 42, 2)"
            );

            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "mv_es_ohlcv_1s\tincremental\tglbxmdp3_mbp1_es\t2024-10-24T17:22:09.842574Z\t2024-10-24T17:22:09.842574Z\tSELECT ts_event AS time,   first(price) AS open,   max(price)   AS high,   min(price)   AS low,   last(price)  AS close,   sum(size)    AS volume FROM glbxmdp3_mbp1_es WHERE action = 'T' SAMPLE BY 1s ALIGN TO CALENDAR\tmv_es_ohlcv_1s~2\t\tvalid\t1\t1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertEquals(
                        compiler,
                        sqlExecutionContext,
                        viewSql,
                        "mv_es_ohlcv_1s"
                );
            }
        });
    }

    @Test
    public void testBaseTableNoDataRangeReplaceCommit() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute(
                    "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:00')"
            );

            currentMicros = parseFloorPartialTimestamp("2024-10-24T17:22:09.842574Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h",
                    "ts",
                    true,
                    true
            );

            final TableToken baseToken = engine.getTableTokenIfExists("base_price");
            Assert.assertNotNull(baseToken);
            try (WalWriter writer = engine.getWalWriter(baseToken)) {
                writer.commitWithParams(
                        parseFloorPartialTimestamp("2024-09-10T00:00:00.000000Z"),
                        parseFloorPartialTimestamp("2024-09-10T13:00"),
                        WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                );
            }

            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testBaseTableRename1() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute(
                    "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = parseFloorPartialTimestamp("2024-10-24T17:22:09.842574Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-10-24T17:22:09.842574Z\t2024-10-24T17:22:09.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t1\t1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );

            currentMicros = parseFloorPartialTimestamp("2024-10-24T18");
            execute("rename table base_price to base_price2");
            execute("refresh materialized view 'price_1h' full;");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-10-24T18:00:00.000000Z\t2024-10-24T18:00:00.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t[-105]: table does not exist [table=base_price]\tinvalid\t1\t-1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );

            // Create another base table instead of the one that was renamed.
            // This table is non-WAL, so mat view should be still invalid.
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY BYPASS WAL"
            );
            currentMicros = parseFloorPartialTimestamp("2024-10-24T19");
            execute("refresh materialized view 'price_1h' full;");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-10-24T18:00:00.000000Z\t2024-10-24T19:00:00.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\tbase table is not a WAL table\tinvalid\t1\t-1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );
        });
    }

    @Test
    public void testBaseTableRename2() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute(
                    "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = parseFloorPartialTimestamp("2024-10-24T17:22:09.842574Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-10-24T17:22:09.842574Z\t2024-10-24T17:22:09.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t1\t1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );

            execute("rename table base_price to base_price2");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-10-24T17:22:09.842574Z\t2024-10-24T17:22:09.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\tbase table is dropped or renamed\tinvalid\t1\t-1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );
        });
    }

    @Test
    public void testBaseTableRenameAndThenRenameBack() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            execute(
                    "insert into base_price " +
                            "select 'gbpusd', 1.320 + x / 1000.0, timestamp_sequence('2024-09-10T12:02', 1000000*60*5) " +
                            "from long_sequence(24 * 20 * 5)"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");
            currentMicros = parseFloorPartialTimestamp("2024-10-24T19");
            drainQueues();

            execute("rename table base_price to base_price2");
            execute("rename table base_price2 to base_price");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-10-24T19:00:00.000000Z\t2024-10-24T19:00:00.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\ttable rename operation\tinvalid\t1\t3\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );
        });
    }

    @Test
    public void testBaseTableSwappedWithRename() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute(
                    "create table base_price2 (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute(
                    "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = parseFloorPartialTimestamp("2024-10-24T17:22:09.842574Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-10-24T17:22:09.842574Z\t2024-10-24T17:22:09.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~3\t\tvalid\t1\t1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );

            // Swap the tables with each other.
            currentMicros = parseFloorPartialTimestamp("2024-10-24T18");
            execute("rename table base_price to base_price_tmp");
            execute("rename table base_price2 to base_price");
            execute("rename table base_price_tmp to base_price2");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-10-24T18:00:00.000000Z\t2024-10-24T18:00:00.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~3\tbase table is dropped or renamed\tinvalid\t1\t1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );
        });
    }

    @Test
    public void testBaseTableTruncateDoesNotInvalidateFreshMatView() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL;"
            );

            final String viewSql = "select sym, last(price) as price, ts from base_price sample by 1h";
            execute("create materialized view price_1h as (" + viewSql + ") partition by DAY");

            execute("truncate table base_price;");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\tview_status\n" +
                            "price_1h\tvalid\n",
                    "select view_name, view_status from materialized_views",
                    null,
                    false
            );
        });
    }

    @Test
    public void testBaseTableWalPurgedDespiteInvalidMatViewState() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, amount int, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute(
                    "insert into base_price (sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );
            drainQueues();
            TableToken baseTableToken = engine.getTableTokenIfExists("base_price");
            currentMicros = parseFloorPartialTimestamp("2024-10-24T17:22:09.842574Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\tbase_table_name\tview_status\n" +
                            "price_1h\tbase_price\tvalid\n",
                    "select view_name, base_table_name, view_status from materialized_views",
                    null,
                    false
            );

            execute("alter table base_price drop column amount;");
            execute("insert into base_price (sym, price, ts) values('gbpusd', 1.330, '2024-09-15T12:01')");

            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\tbase_table_name\tview_status\tinvalidation_reason\n" +
                            "price_1h\tbase_price\tinvalid\t" + "drop column operation" + "\n",
                    "select view_name, base_table_name, view_status, invalidation_reason from materialized_views",
                    null,
                    false
            );

            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(baseTableToken).concat(WalUtils.WAL_NAME_BASE).put(1);
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                engine.releaseInactiveTableSequencers();
                drainPurgeJob();

                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
            }
        });
    }

    @Test
    public void testBatchInsert() throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_INSERT_AS_SELECT_BATCH_SIZE, 10);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, 2);

        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by MONTH WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute("insert into base_price select concat('sym', x), x, timestamp_sequence('2022-02-24', 1000000*60*60*2) from long_sequence(30);");

            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "sym30\t30.0\t2022-02-23T12:00:00.000000Z\n" +
                            "sym27\t27.0\t2022-02-23T13:00:00.000000Z\n" +
                            "sym28\t28.0\t2022-02-23T13:00:00.000000Z\n" +
                            "sym29\t29.0\t2022-02-23T13:00:00.000000Z\n" +
                            "sym25\t25.0\t2022-02-23T14:00:00.000000Z\n" +
                            "sym26\t26.0\t2022-02-23T14:00:00.000000Z\n" +
                            "sym22\t22.0\t2022-02-23T15:00:00.000000Z\n" +
                            "sym23\t23.0\t2022-02-23T15:00:00.000000Z\n" +
                            "sym24\t24.0\t2022-02-23T15:00:00.000000Z\n" +
                            "sym20\t20.0\t2022-02-23T16:00:00.000000Z\n" +
                            "sym21\t21.0\t2022-02-23T16:00:00.000000Z\n" +
                            "sym17\t17.0\t2022-02-23T17:00:00.000000Z\n" +
                            "sym18\t18.0\t2022-02-23T17:00:00.000000Z\n" +
                            "sym19\t19.0\t2022-02-23T17:00:00.000000Z\n" +
                            "sym14\t14.0\t2022-02-23T18:00:00.000000Z\n" +
                            "sym15\t15.0\t2022-02-23T18:00:00.000000Z\n" +
                            "sym16\t16.0\t2022-02-23T18:00:00.000000Z\n" +
                            "sym12\t12.0\t2022-02-23T19:00:00.000000Z\n" +
                            "sym13\t13.0\t2022-02-23T19:00:00.000000Z\n" +
                            "sym10\t10.0\t2022-02-23T20:00:00.000000Z\n" +
                            "sym11\t11.0\t2022-02-23T20:00:00.000000Z\n" +
                            "sym9\t9.0\t2022-02-23T20:00:00.000000Z\n" +
                            "sym7\t7.0\t2022-02-23T21:00:00.000000Z\n" +
                            "sym8\t8.0\t2022-02-23T21:00:00.000000Z\n" +
                            "sym4\t4.0\t2022-02-23T22:00:00.000000Z\n" +
                            "sym5\t5.0\t2022-02-23T22:00:00.000000Z\n" +
                            "sym6\t6.0\t2022-02-23T22:00:00.000000Z\n" +
                            "sym2\t2.0\t2022-02-23T23:00:00.000000Z\n" +
                            "sym3\t3.0\t2022-02-23T23:00:00.000000Z\n" +
                            "sym1\t1.0\t2022-02-24T00:00:00.000000Z\n",
                    "price_1h order by ts, sym",
                    "ts",
                    true,
                    true
            );

            // Expect 3 (30 rows / 10 rows per batch) commits.
            assertQueryNoLeakCheck(
                    "writerTxn\tsequencerTxn\n" +
                            "3\t3\n",
                    "select writerTxn, sequencerTxn from wal_tables() where name = 'price_1h'",
                    null,
                    false
            );
        });
    }

    @Test
    public void testCheckMatViewModification() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");
            // copy
            assertCannotModifyMatView("copy price_1h from 'test-numeric-headers.csv' with header true");
            // rename table
            assertCannotModifyMatView("rename table price_1h to price_1h_bak");
            // update
            assertCannotModifyMatView("update price_1h set price = 1.1");
            // insert
            assertCannotModifyMatView("insert into base_price values('gbpusd', 1.319, '2024-09-10T12:05')");
            // insert as select
            assertCannotModifyMatView("insert into price_1h select sym, last(price) as price, ts from base_price sample by 1h");
            // alter
            assertCannotModifyMatView("alter table price_1h add column x int");
            assertCannotModifyMatView("alter table price_1h rename column sym to sym2");
            assertCannotModifyMatView("alter table price_1h alter column sym type varchar");
            assertCannotModifyMatView("alter table price_1h drop column sym");
            assertCannotModifyMatView("alter table price_1h drop partition where ts > 0");
            assertCannotModifyMatView("alter table price_1h dedup disable");
            assertCannotModifyMatView("alter table price_1h set type bypass wal");
            assertCannotModifyMatView("alter table price_1h set ttl 3 weeks");
            assertCannotModifyMatView("alter table price_1h set param o3MaxLag = 20s");
            assertCannotModifyMatView("alter table price_1h resume wal");
            // reindex
            assertCannotModifyMatView("reindex table price_1h");
            // truncate
            assertCannotModifyMatView("truncate table price_1h");
            // drop
            assertCannotModifyMatView("drop table price_1h");
            // vacuum
            assertCannotModifyMatView("vacuum table price_1h");
        });
    }

    @Test
    public void testCreateDropCreate() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");
            TableToken matViewToken1 = engine.verifyTableName("price_1h");

            execute(
                    "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h order by ts, sym",
                    "ts",
                    true,
                    true
            );

            dropMatView();
            drainQueues();

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");
            TableToken matViewToken2 = engine.verifyTableName("price_1h");
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h order by ts, sym",
                    "ts",
                    true,
                    true
            );

            Assert.assertNull(engine.getMatViewStateStore().getViewState(matViewToken1));
            Assert.assertNotNull(engine.getMatViewStateStore().getViewState(matViewToken2));
        });
    }

    @Test
    public void testCreateMatViewLoop() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE 'trades' (" +
                            "symbol SYMBOL CAPACITY 256 CACHE, " +
                            "side SYMBOL CAPACITY 256 CACHE, " +
                            "price DOUBLE, " +
                            "amount DOUBLE, " +
                            "timestamp TIMESTAMP " +
                            ") timestamp(timestamp) PARTITION BY HOUR WAL;"
            );

            execute("INSERT INTO trades VALUES('BTC-USD', 'BUY', 29432.50, 0.5, '2023-08-15T09:30:45.789Z');");
            execute("INSERT INTO trades VALUES('BTC-USD', 'SELL', 29435.20, 1.2, '2023-08-15T09:31:12.345Z');");

            drainQueues();
            execute("CREATE MATERIALIZED VIEW a AS (\n" +
                    "  SELECT\n" +
                    "    timestamp,\n" +
                    "    symbol,\n" +
                    "    avg(price) AS avg_price\n" +
                    "  FROM trades\n" +
                    "  SAMPLE BY 1d\n" +
                    ") partition by HOUR;");

            execute("create MATERIALIZED view b as (\n" +
                    "  SELECT\n" +
                    "    timestamp,\n" +
                    "    symbol,\n" +
                    "    avg(avg_price) AS avg_price\n" +
                    "  FROM a\n" +
                    "  SAMPLE BY 2d\n" +
                    ") partition by HOUR;");

            execute("create MATERIALIZED view c as (\n" +
                    "  SELECT\n" +
                    "    timestamp,\n" +
                    "    symbol,\n" +
                    "    avg(avg_price) AS avg_price\n" +
                    "  FROM b\n" +
                    "  SAMPLE BY 2d\n" +
                    ") partition by HOUR;");

            drainQueues();
            execute("drop MATERIALIZED VIEW a;");

            drainQueues();

            try {
                execute(
                        "create MATERIALIZED view A as (\n" +
                                "  SELECT\n" +
                                "    timestamp,\n" +
                                "    symbol,\n" +
                                "    avg(avg_price) AS avg_price\n" +
                                "  FROM c\n" +
                                "  SAMPLE BY 2d\n" +
                                ") partition by HOUR;"
                );
                Assert.fail("Expected a dependency loop exception");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "circular dependency detected");
            }
            // mat view table should be dropped
            TableToken token = engine.getTableTokenIfExists("a");
            Assert.assertNull(token);
        });
    }

    @Test
    public void testCreateMatViewNonDeterministicFunctionCompatibility() throws Exception {
        // Verifies that even if someone was able to create a mat view with non-deterministic function
        // on an older version, it'll be marked as invalid on the next refresh.
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            try (var ctx = new SqlExecutionContextImpl(engine, 1) {
                @Override
                public boolean allowNonDeterministicFunctions() {
                    return true;
                }
            }) {
                ctx.with(sqlExecutionContext.getSecurityContext(), bindVariableService, sqlExecutionContext.getRandom(), sqlExecutionContext.getRequestFd(), circuitBreaker);
                execute("create materialized view price_1h as select sym, last(price) as price, ts from base_price where ts in today() sample by 42h", ctx);
            }

            execute(
                    "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );
            currentMicros = parseFloorPartialTimestamp("2023-01-01T01:01:01.123456Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2023-01-01T01:01:01.123456Z\t2023-01-01T01:01:01.123456Z\tselect sym, last(price) as price, ts from base_price where ts in today() sample by 42h\tprice_1h~2\t[65]: non-deterministic function cannot be used in materialized view: today\tinvalid\t-1\t1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null,
                    false
            );
        });
    }

    @Test
    public void testDisableParallelSqlExecution() throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, "false");
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            final String viewSql = "select sym, last(price) as price, ts from base_price sample by 1h";
            createMatView(viewSql);

            execute(
                    "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            drainQueues();

            assertViewMatchesSqlOverBaseTable(viewSql);
        });
    }

    @Test
    public void testDropAll() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");
            TableToken matViewToken = engine.verifyTableName("price_1h");

            execute(
                    "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h order by ts, sym",
                    "ts",
                    true,
                    true
            );

            // mat view should be deleted
            execute("drop all;");

            drainQueues();

            assertQueryNoLeakCheck(
                    "count\n" +
                            "0\n",
                    "select count() from materialized_views();",
                    null,
                    false,
                    true
            );
            assertQueryNoLeakCheck(
                    "count\n" +
                            "0\n",
                    "select count() from tables();",
                    null,
                    false,
                    true
            );

            Assert.assertNull(engine.getMatViewStateStore().getViewState(matViewToken));
        });
    }

    @Test
    public void testEnableDedupWithFewerKeysDoesNotInvalidateMatViews() throws Exception {
        testEnableDedupWithSubsetKeys("alter table base_price dedup enable upsert keys(ts);", false);
    }

    @Test
    public void testEnableDedupWithMoreKeysInvalidatesMatViews() throws Exception {
        testEnableDedupWithSubsetKeys("alter table base_price dedup enable upsert keys(ts, amount);", false);
    }

    @Test
    public void testEnableDedupWithSameKeysDoesNotInvalidateMatViews() throws Exception {
        testEnableDedupWithSubsetKeys("alter table base_price dedup enable upsert keys(ts, sym);", false);
    }

    @Test
    public void testFullRefresh() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp, extra_col long" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = parseFloorPartialTimestamp("2024-01-01T01:01:01.842574Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t1\t1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );

            final String expected = "sym\tprice\tts\n" +
                    "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                    "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                    "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n";
            assertQueryNoLeakCheck(expected, "price_1h order by sym");

            execute("alter table base_price drop column extra_col");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\tdrop column operation\tinvalid\t1\t2\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );

            execute("refresh materialized view price_1h full");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t2\t2\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );
            assertQueryNoLeakCheck(expected, "price_1h order by sym");
        });
    }

    @Test
    public void testFullRefreshDroppedBaseColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp, extra_col long" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = parseFloorPartialTimestamp("2024-01-01T01:01:01.842574Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t1\t1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );

            final String expected = "sym\tprice\tts\n" +
                    "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                    "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                    "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n";
            assertQueryNoLeakCheck(expected, "price_1h order by sym");

            execute("alter table base_price drop column sym;");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\tdrop column operation\tinvalid\t1\t2\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );

            execute("refresh materialized view price_1h full;");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t[7]: Invalid column: sym\tinvalid\t-1\t2\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );
            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n",
                    "price_1h",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testFullRefreshFail1() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            createMatView("select sym, last(price) as price, ts from base_price where npe() sample by 1h");

            execute("insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01');");
            currentMicros = parseFloorPartialTimestamp("2001-01-01T01:01:01.000000Z");
            drainQueues();

            // The view is expected to be invalid due to npe() in where clause.
            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2001-01-01T01:01:01.000000Z\t2001-01-01T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price where npe() sample by 1h\tprice_1h~2\t[-1]: unexpected filter error\tinvalid\t-1\t1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );

            execute("refresh materialized view price_1h full");
            drainQueues();

            // The view is expected to be still invalid.
            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2001-01-01T01:01:01.000000Z\t2001-01-01T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price where npe() sample by 1h\tprice_1h~2\t[-1]: unexpected filter error\tinvalid\t-1\t1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );
        });
    }

    @Test
    public void testFullRefreshFail2() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute("insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01');");
            currentMicros = parseFloorPartialTimestamp("2001-01-01T01:01:01.000000Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2001-01-01T01:01:01.000000Z\t2001-01-01T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t1\t1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );

            execute("alter table base_price drop column price");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2001-01-01T01:01:01.000000Z\t2001-01-01T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\tdrop column operation\tinvalid\t1\t2\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );

            execute("refresh materialized view price_1h full");
            drainQueues();

            // The view is expected to be still invalid.
            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2001-01-01T01:01:01.000000Z\t2001-01-01T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t[17]: Invalid column: price\tinvalid\t-1\t2\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );
        });
    }

    @Test
    public void testFullRefreshOfDroppedView() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");
            drainQueues();

            assertSql(
                    "count\n" +
                            "1\n",
                    "select count() from materialized_views"
            );

            execute("refresh materialized view price_1h full");
            execute("drop materialized view price_1h");

            drainQueues();

            assertSql(
                    "count\n" +
                            "0\n",
                    "select count() from materialized_views"
            );
        });
    }

    @Test
    public void testFullRefreshOfEmptyBaseTable() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute("refresh materialized view price_1h full");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t\t\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t-1\t0\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );
        });
    }

    @Test
    public void testIncrementalRefresh() throws Exception {
        testIncrementalRefresh0("select sym, last(price) as price, ts from base_price sample by 1h");
    }

    @Test
    public void testIncrementalRefreshStatement() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price ( " +
                            "sym varchar, price double, ts timestamp " +
                            ") timestamp(ts) partition by DAY WAL"
            );
            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = parseFloorPartialTimestamp("2024-01-01T01:01:01.842574Z");
            // this statement will notify refresh job before the WAL apply job,
            // but technically that's redundant
            execute("refresh materialized view price_1h incremental");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t1\t1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n",
                    "price_1h order by sym"
            );
        });
    }

    @Test
    public void testIncrementalRefreshStatementOnTimerMatView() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute(
                    "create materialized view price_1h refresh start '2199-12-12T12:00:00.000000Z' every 1h as (" +
                            "select sym, last(price) as price, ts from base_price sample by 1h" +
                            ") partition by day"
            );

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            // no refresh should happen as the start timestamp is in future
            currentMicros = parseFloorPartialTimestamp("2024-01-01T01:01:01.842574Z");
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n",
                    "price_1h order by sym"
            );
            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental_timer\tbase_price\t\t\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t-1\t1\t0\t\t2199-12-12T12:00:00.000000Z\t1\tHOUR\n",
                    "materialized_views",
                    null
            );

            // the view should refresh after an explicit incremental refresh call
            execute("refresh materialized view price_1h incremental;");
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental_timer\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t1\t1\t0\t\t2199-12-12T12:00:00.000000Z\t1\tHOUR\n",
                    "materialized_views",
                    null
            );
            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n",
                    "price_1h order by sym"
            );
        });
    }

    @Test
    public void testIncrementalRefreshWithViewWhereClauseSymbolFilters() throws Exception {
        testIncrementalRefresh0(
                "select sym, last(price) as price, ts from base_price " +
                        "WHERE sym = 'gbpusd' or sym = 'jpyusd' " +
                        "sample by 1h"
        );
    }

    @Test
    public void testIncrementalRefreshWithViewWhereClauseTimestampFilters() throws Exception {
        testIncrementalRefresh0(
                "select sym, last(price) price, ts from base_price " +
                        "WHERE ts > 0 or ts < '2040-01-01' " +
                        "sample by 1h"
        );
    }

    @Test
    public void testIndexSampleByAlignToCalendar() throws Exception {
        assertMemoryLeak(() -> {
            final String viewName = "x_view";
            final String out = "select k, s, lat, lon";
            final String viewQuery = "select k, s, first(lat) lat, last(lon) lon " +
                    "from x " +
                    "where s in ('a') " +
                    "sample by 1h align to calendar";

            final long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-03-27T23:30:00.000000Z");
            final long step = 100000000L;
            final int N = 100;
            final int K = 5;
            final String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b',null) s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            final String expected = "k\ts\tlat\tlon\n" +
                    "2021-03-27T23:00:00.000000Z\ta\t142.30215575416736\t165.69007104574442\n" +
                    "2021-03-28T00:00:00.000000Z\ta\t106.0418967098362\tnull\n" +
                    "2021-03-28T01:00:00.000000Z\ta\t79.9245166429184\t168.04971262491318\n" +
                    "2021-03-28T02:00:00.000000Z\ta\t6.612327943200507\t128.42101395467057\n";

            assertQueryNoLeakCheck(expected, outSelect(out, viewQuery), "k", true, true);
            assertQueryNoLeakCheck(expected, outSelect(out, viewName), "k", true, true);
        });
    }

    @Test
    public void testIndexSampleByAlignToCalendarDSTForwardEdge() throws Exception {
        assertMemoryLeak(() -> {
            String viewName = "x_view";
            String out = "select to_timezone(k, 'Europe/Berlin') k, s, lat, lon";
            String viewQuery = "select k, s, first(lat) lat, last(lon) lon " +
                    "from x " +
                    "where s in ('a') " +
                    "sample by 1h align to calendar time zone 'Europe/Berlin'";

            long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-03-28T00:59:00.000000Z");
            long step = 60 * 1000000L;
            final int N = 100;
            final int K = 5;
            final String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a') s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            final String expected = "k\ts\tlat\tlon\n" +
                    "2021-03-28T01:00:00.000000Z\ta\t144.77803379943109\t15.276535618609202\n" +
                    "2021-03-28T03:00:00.000000Z\ta\tnull\t127.43011035722469\n" +
                    "2021-03-28T04:00:00.000000Z\ta\t60.30746433578906\t128.42101395467057\n";

            assertQueryNoLeakCheck(expected, outSelect(out, viewQuery), null, true, true);
            assertQueryNoLeakCheck(expected, outSelect(out, viewName), null, true, true);
        });
    }

    @Test
    public void testIndexSampleByAlignToCalendarDSTForwardEdge2() throws Exception {
        assertMemoryLeak(() -> {
            final String viewName = "x_view";
            final String out = "select to_timezone(k, 'Europe/Berlin') k, s, lat, lon";
            final String viewQuery = "select k, s, first(lat) lat, last(lon) lon " +
                    "from x " +
                    "where s in ('a') " +
                    "sample by 1h align to calendar time zone 'Europe/Berlin'";

            final long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-03-28T01:00:00.000000Z");
            final long step = 60 * 1000000L;
            final int N = 100;
            final int K = 5;
            final String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a') s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            final String expected = "k\ts\tlat\tlon\n" +
                    "2021-03-28T03:00:00.000000Z\ta\t144.77803379943109\tnull\n" +
                    "2021-03-28T04:00:00.000000Z\ta\t98.27279585461298\t128.42101395467057\n";

            assertQueryNoLeakCheck(expected, outSelect(out, viewQuery), null, true, true);
            assertQueryNoLeakCheck(expected, outSelect(out, viewName), null, true, true);
        });
    }

    @Test
    public void testIndexSampleByAlignToCalendarDSTForwardEdge3() throws Exception {
        assertMemoryLeak(() -> {
            final String viewName = "x_view";
            final String out = "select to_timezone(k, 'Europe/Berlin') k, s, lat, lon";
            final String viewQuery = "select k, s, first(lat) lat, last(lon) lon " +
                    "from x " +
                    "where s in ('a') " +
                    "sample by 1h align to calendar time zone 'Europe/Berlin'";

            final long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-03-28T01:59:00.000000Z");
            final long step = 60 * 1000000L;
            final int N = 100;
            final int K = 5;
            final String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a') s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            final String expected = "k\ts\tlat\tlon\n" +
                    "2021-03-28T03:00:00.000000Z\ta\t144.77803379943109\t15.276535618609202\n" +
                    "2021-03-28T04:00:00.000000Z\ta\tnull\t127.43011035722469\n" +
                    "2021-03-28T05:00:00.000000Z\ta\t60.30746433578906\t128.42101395467057\n";

            assertQueryNoLeakCheck(expected, outSelect(out, viewQuery), null, true, true);
            assertQueryNoLeakCheck(expected, outSelect(out, viewName), null, true, true);
        });
    }

    @Test
    public void testIndexSampleByAlignToCalendarDSTForwardLocalMidnight() throws Exception {
        assertMemoryLeak(() -> {
            final String viewName = "x_view";
            final String out = "select to_timezone(k, 'Europe/Berlin') k, s, lat, lon";
            final String viewQuery = "select k, s, first(lat) lat, last(lon) lon " +
                    "from x " +
                    "where s in ('a') " +
                    "sample by 1h align to calendar time zone 'Europe/Berlin'";

            final long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-03-27T23:01:00.000000Z");
            final long step = 60 * 1000000L;
            final int N = 100;
            final int K = 5;
            final String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b',null) s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            final String expected = "k\ts\tlat\tlon\n" +
                    "2021-03-28T00:00:00.000000Z\ta\t142.30215575416736\t167.4566019970139\n" +
                    "2021-03-28T01:00:00.000000Z\ta\t33.45558404694713\t128.42101395467057\n";
            assertQueryNoLeakCheck(expected, outSelect(out, viewQuery), null, true, true);
            assertQueryNoLeakCheck(expected, outSelect(out, viewName), null, true, true);
        });
    }

    @Test
    public void testIndexSampleByAlignToCalendarWithTimezoneBerlinShiftBack() throws Exception {
        assertMemoryLeak(() -> {
            final String viewName = "x_view";
            final String out = "select to_timezone(k, 'Europe/Berlin'), k, s, lat, lon";
            final String viewQuery = "select k, s, first(lat) lat, last(k) lon " +
                    "from x " +
                    "where s in ('a') " +
                    "sample by 1d align to calendar time zone 'Europe/Berlin'";

            final long startTs = TimestampFormatUtils.parseUTCTimestamp("2020-10-23T20:30:00.000000Z");
            final long step = 50 * 60 * 1000000L;
            final int N = 120;
            final int K = 5;
            final String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b',null) s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            final String expected = "to_timezone\tk\ts\tlat\tlon\n" +
                    "2020-10-24T00:00:00.000000Z\t2020-10-23T22:00:00.000000Z\ta\t142.30215575416736\t2020-10-24T19:50:00.000000Z\n" +
                    "2020-10-25T00:00:00.000000Z\t2020-10-24T22:00:00.000000Z\ta\tnull\t2020-10-25T20:00:00.000000Z\n" +
                    "2020-10-26T00:00:00.000000Z\t2020-10-25T23:00:00.000000Z\ta\t33.45558404694713\t2020-10-26T21:50:00.000000Z\n" +
                    "2020-10-27T00:00:00.000000Z\t2020-10-26T23:00:00.000000Z\ta\t6.612327943200507\t2020-10-27T22:00:00.000000Z\n" +
                    "2020-10-28T00:00:00.000000Z\t2020-10-27T23:00:00.000000Z\ta\tnull\t2020-10-27T23:40:00.000000Z\n";

            assertQueryNoLeakCheck(expected, outSelect(out, viewQuery), "k", true, true);
            assertQueryNoLeakCheck(expected, outSelect(out, viewName), "k", true, true);
        });
    }

    @Test
    public void testIndexSampleByAlignToCalendarWithTimezoneBerlinShiftBackHourlyWithOffset() throws Exception {
        assertMemoryLeak(() -> {
            final String viewName = "x_view";
            final String out = "select to_timezone(k, 'Europe/Berlin'), k, s, lat, lon";
            final String viewQuery = "select k, s, first(lat) lat, last(k) lon " +
                    "from x " +
                    "where s in ('a') and k between '2021-03-27 21:00' and '2021-03-28 04:00' " +
                    "sample by 1h align to calendar time zone 'Europe/Berlin' with offset '00:15'";

            final long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-03-26T20:30:00.000000Z");
            final long step = 13 * 60 * 1000000L;
            final int N = 1000;
            final int K = 25;
            final String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b') s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            final String expected = "to_timezone\tk\ts\tlat\tlon\n" +
                    "2021-03-27T21:15:00.000000Z\t2021-03-27T20:15:00.000000Z\ta\t132.09083798490755\t2021-03-27T21:12:00.000000Z\n" +
                    "2021-03-27T22:15:00.000000Z\t2021-03-27T21:15:00.000000Z\ta\t179.5841357536068\t2021-03-27T21:51:00.000000Z\n" +
                    "2021-03-27T23:15:00.000000Z\t2021-03-27T22:15:00.000000Z\ta\t77.68770182183965\t2021-03-27T22:56:00.000000Z\n" +
                    "2021-03-28T00:15:00.000000Z\t2021-03-27T23:15:00.000000Z\ta\tnull\t2021-03-27T23:48:00.000000Z\n" +
                    "2021-03-28T01:15:00.000000Z\t2021-03-28T00:15:00.000000Z\ta\t3.6703591550328163\t2021-03-28T01:06:00.000000Z\n" +
                    "2021-03-28T03:15:00.000000Z\t2021-03-28T01:15:00.000000Z\ta\tnull\t2021-03-28T02:11:00.000000Z\n" +
                    "2021-03-28T04:15:00.000000Z\t2021-03-28T02:15:00.000000Z\ta\tnull\t2021-03-28T02:37:00.000000Z\n" +
                    "2021-03-28T05:15:00.000000Z\t2021-03-28T03:15:00.000000Z\ta\t38.20430552091481\t2021-03-28T03:16:00.000000Z\n";
            assertQueryNoLeakCheck(expected, outSelect(out, viewQuery), "k", true, true);
            assertQueryNoLeakCheck(expected, outSelect(out, viewName), "k", true, true);
        });
    }

    @Test
    public void testIndexSampleByAlignToCalendarWithTimezoneBerlinShiftForward() throws Exception {
        assertMemoryLeak(() -> {
            final String viewName = "x_view";
            final String out = "select to_timezone(k, 'Europe/Berlin'), k, s, lat, lon";
            final String viewQuery = "select k, s, first(lat) lat, last(k) lon " +
                    "from x " +
                    "where s in ('a') " +
                    "sample by 1d align to calendar time zone 'Europe/Berlin'";

            final long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-03-25T23:30:00.000000Z");
            final long step = 50 * 60 * 1000000L;
            final int N = 120;
            final int K = 5;
            final String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b',null) s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);
            final String expected = "to_timezone\tk\ts\tlat\tlon\n" +
                    "2021-03-26T00:00:00.000000Z\t2021-03-25T23:00:00.000000Z\ta\t142.30215575416736\t2021-03-26T22:50:00.000000Z\n" +
                    "2021-03-27T00:00:00.000000Z\t2021-03-26T23:00:00.000000Z\ta\tnull\t2021-03-27T22:10:00.000000Z\n" +
                    "2021-03-28T00:00:00.000000Z\t2021-03-27T23:00:00.000000Z\ta\t109.94209864193589\t2021-03-28T20:40:00.000000Z\n" +
                    "2021-03-29T00:00:00.000000Z\t2021-03-28T22:00:00.000000Z\ta\t70.00560222114518\t2021-03-29T16:40:00.000000Z\n" +
                    "2021-03-30T00:00:00.000000Z\t2021-03-29T22:00:00.000000Z\ta\t13.290235514836048\t2021-03-30T02:40:00.000000Z\n";

            assertQueryNoLeakCheck(expected, outSelect(out, viewQuery), "k", true, true);
            assertQueryNoLeakCheck(expected, outSelect(out, viewName), "k", true, true);
        });
    }

    @Test
    public void testIndexSampleByAlignToCalendarWithTimezoneLondonShiftBack() throws Exception {
        assertMemoryLeak(() -> {
            final String viewName = "x_view";
            final String out = "select to_timezone(k, 'Europe/London'), k, s, lat, lon";
            final String viewQuery = "select k, s, first(lat) lat, last(k) lon " +
                    "from x " +
                    "where s in ('a') " +
                    "sample by 1d align to calendar time zone 'Europe/London'";

            final long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-03-25T23:30:00.000000Z");
            final long step = 50 * 60 * 1000000L;
            final int N = 120;
            final int K = 5;
            final String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b',null) s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            final String expected = "to_timezone\tk\ts\tlat\tlon\n" +
                    "2021-03-26T00:00:00.000000Z\t2021-03-26T00:00:00.000000Z\ta\t142.30215575416736\t2021-03-26T22:50:00.000000Z\n" +
                    "2021-03-27T00:00:00.000000Z\t2021-03-27T00:00:00.000000Z\ta\tnull\t2021-03-27T23:00:00.000000Z\n" +
                    "2021-03-28T00:00:00.000000Z\t2021-03-28T00:00:00.000000Z\ta\t33.45558404694713\t2021-03-28T20:40:00.000000Z\n" +
                    "2021-03-29T00:00:00.000000Z\t2021-03-28T23:00:00.000000Z\ta\t70.00560222114518\t2021-03-29T16:40:00.000000Z\n" +
                    "2021-03-30T00:00:00.000000Z\t2021-03-29T23:00:00.000000Z\ta\t13.290235514836048\t2021-03-30T02:40:00.000000Z\n";

            assertQueryNoLeakCheck(expected, outSelect(out, viewQuery), "k", true, true);
            assertQueryNoLeakCheck(expected, outSelect(out, viewName), "k", true, true);
        });
    }

    @Test
    public void testIndexSampleByAlignToCalendarWithTimezoneLondonShiftForwardHourly() throws Exception {
        assertMemoryLeak(() -> {
            final String viewName = "x_view";
            final String out = "select to_timezone(k, 'Europe/London'), k, s, lat, lastk";
            final String viewQuery = "select k, s, first(lat) lat, last(k) lastk " +
                    "from x " +
                    "where s in ('a') and k between '2020-10-24 21:00:00' and '2020-10-25 05:00:00'" +
                    "sample by 1h align to calendar time zone 'Europe/London'";

            final long startTs = TimestampFormatUtils.parseUTCTimestamp("2020-10-23T20:30:00.000000Z");
            final long step = 259 * 1000000L;
            final int N = 1000;
            final int K = 25;
            final String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b') s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            final String expected = "to_timezone\tk\ts\tlat\tlastk\n" +
                    "2020-10-24T22:00:00.000000Z\t2020-10-24T21:00:00.000000Z\ta\t154.93777586404912\t2020-10-24T21:49:28.000000Z\n" +
                    "2020-10-24T23:00:00.000000Z\t2020-10-24T22:00:00.000000Z\ta\t43.799859246867385\t2020-10-24T22:54:13.000000Z\n" +
                    "2020-10-25T00:00:00.000000Z\t2020-10-24T23:00:00.000000Z\ta\t38.34194069380561\t2020-10-24T23:41:42.000000Z\n" +
                    "2020-10-25T01:00:00.000000Z\t2020-10-25T00:00:00.000000Z\ta\t4.158342987512034\t2020-10-25T01:51:12.000000Z\n" +
                    "2020-10-25T02:00:00.000000Z\t2020-10-25T02:00:00.000000Z\ta\t95.73868763606973\t2020-10-25T02:47:19.000000Z\n" +
                    "2020-10-25T03:00:00.000000Z\t2020-10-25T03:00:00.000000Z\ta\tnull\t2020-10-25T03:43:26.000000Z\n" +
                    "2020-10-25T04:00:00.000000Z\t2020-10-25T04:00:00.000000Z\ta\t34.49948946607576\t2020-10-25T04:56:49.000000Z\n";

            assertQueryNoLeakCheck(expected, outSelect(out, viewQuery), "k", true, true);
            assertQueryNoLeakCheck(expected, outSelect(out, viewName), "k", true, true);
        });
    }

    @Test
    public void testInsertAfterTruncate() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL;"
            );

            execute("insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01');");
            drainQueues();
            execute("truncate table base_price;");
            drainQueues();

            final String view1Sql = "select sym, last(price) as price, ts from base_price sample by 1h";
            execute("create materialized view price_1h as (" + view1Sql + ") partition by DAY");
            drainQueues();
            final String view2Sql = "select sym, last(price) as price, ts from base_price sample by 1d";
            execute("create materialized view price_1d as (" + view2Sql + ") partition by month");
            drainQueues();

            execute(
                    "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02');"
            );
            drainQueues();

            final String expected1 = "sym\tprice\tts\n" +
                    "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                    "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                    "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n";
            assertQueryNoLeakCheck(expected1, "price_1h order by sym");
            assertQueryNoLeakCheck(expected1, view1Sql + " order by sym");

            final String expected2 = "sym\tprice\tts\n" +
                    "gbpusd\t1.321\t2024-09-10T00:00:00.000000Z\n" +
                    "jpyusd\t103.21\t2024-09-10T00:00:00.000000Z\n";
            assertQueryNoLeakCheck(expected2, "price_1d order by sym");
            assertQueryNoLeakCheck(expected2, view2Sql + " order by sym");
        });
    }

    @Test
    public void testManyTimerMatViews() throws Exception {
        assertMemoryLeak(() -> {
            final int views = 32;
            final long start = parseFloorPartialTimestamp("2024-12-12T00:00:00.000000Z");

            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            // Create first and last mat view before all others to verify timer list sort logic.
            createNthTimerMatView(start, 0);
            createNthTimerMatView(start, (views - 1));
            for (int i = 1; i < views - 1; i++) {
                createNthTimerMatView(start, i);
            }

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = start;
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);

            for (int i = 0; i < views; i++) {
                drainMatViewTimerQueue(timerJob);
                drainQueues();

                assertQueryNoLeakCheck(
                        "sym\tprice\tts\n" +
                                "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                                "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n",
                        "price_1h_" + i + " order by sym"
                );

                currentMicros += Timestamps.SECOND_MICROS;
            }

            // Drop all mat views. All timers should be removed as well.
            dropNthTimerMatView(views - 1);
            dropNthTimerMatView(0);
            for (int i = 1; i < views - 1; i++) {
                dropNthTimerMatView(i);
            }

            currentMicros += Timestamps.DAY_MICROS;
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    "count\n" +
                            "0\n",
                    "select count() from materialized_views",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testMatViewTableRename() throws Exception {
        // Mat views may not support renames, but the table can be renamed
        // during replication from a temp name, so the renaming has to be supported on storage level
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute(
                    "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = parseFloorPartialTimestamp("2024-10-24T17:22:09.842574Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-10-24T17:22:09.842574Z\t2024-10-24T17:22:09.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t1\t1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );

            TableToken matViewToken = engine.verifyTableName("price_1h");
            TableToken updatedToken = matViewToken.renamed("price_1h_renamed");

            engine.applyTableRename(matViewToken, updatedToken);

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h_renamed\tincremental\tbase_price\t2024-10-24T17:22:09.842574Z\t2024-10-24T17:22:09.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t1\t1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );

            execute(
                    "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1, '2024-09-10T12:02')" +
                            ",('jpyusd', 1, '2024-09-10T12:02')" +
                            ",('gbpusd', 1, '2024-09-10T13:02')"
            );
            drainQueues();

            assertSql("sym\tprice\tts\n" +
                            "gbpusd\t1.0\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t1.0\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.0\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h_renamed"
            );
        });
    }

    @Test
    public void testQueryError() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, amount int, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price where npe() sample by 1h");

            execute(
                    "insert into base_price (sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\tbase_table_name\tview_status\tinvalidation_reason\n" +
                            "price_1h\tbase_price\tinvalid\t[-1]: unexpected filter error\n",
                    "select view_name, base_table_name, view_status, invalidation_reason from materialized_views",
                    null,
                    false
            );
        });
    }

    @Test
    public void testQueryError2() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x (" +
                            "sym varchar, price double, amount int, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute("create table y (sym varchar)");
            execute("insert into x values ('foo', 3, 42, '2024-09-10T12:01')");

            execute(
                    "create materialized view x_1h with base x as ( " +
                            "  select x.sym, last(x.price) as price, x.ts from x join y on (sym) sample by 1h " +
                            ") partition by week"
            );

            execute("drop table y");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\tbase_table_name\tview_status\tinvalidation_reason\n" +
                            "x_1h\tx\tinvalid\t[58]: table does not exist [table=y]\n",
                    "select view_name, base_table_name, view_status, invalidation_reason from materialized_views",
                    null,
                    false
            );
        });
    }

    @Test
    public void testQueryWithCte() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table exchanges (" +
                            " uid symbol, amount double, ts timestamp " +
                            ") timestamp(ts) partition by day wal;"
            );
            execute("create table aux_start_date (ts timestamp);");

            execute(
                    "insert into exchanges values('foo', 1.320, '2024-09-10T12:01')" +
                            ",('foo', 1.323, '2024-09-10T12:02')" +
                            ",('bar', 103.21, '2024-09-10T12:02')" +
                            ",('foo', 1.321, '2024-09-10T13:02')"
            );
            execute("insert into aux_start_date values('2024-09-10')");
            drainQueues();

            final String expected = "ts\tuid\tamount\n" +
                    "2000-01-01T00:00:00.000000Z\tbar\t103.21\n" +
                    "2000-01-01T00:00:00.000000Z\tfoo\t1.321\n";
            final String viewSql = "with starting_point as ( " +
                    "  select ts from aux_start_date " +
                    "  union " +
                    "  select '2024-09-10' " +
                    "), " +
                    "latest_query as ( " +
                    "  select * " +
                    "  from exchanges " +
                    "  where ts >= (select min(ts) from starting_point) " +
                    "  latest on ts partition by uid " +
                    ") " +
                    "select ts, uid, first(amount) as amount " +
                    "from latest_query " +
                    "sample by 100y";
            assertQueryNoLeakCheck(expected, viewSql, "ts");

            execute("create materialized view exchanges_100y as (" + viewSql + ") partition by year");
            drainQueues();

            assertQueryNoLeakCheck(expected, "exchanges_100y", "ts", true, true);
        });
    }

    @Test
    public void testRangeRefresh() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x ( " +
                            "sym varchar, price double, ts timestamp " +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute("create table y (sym varchar)");

            execute(
                    "create materialized view x_1h with base x as " +
                            "select x.sym, last(x.price) as price, x.ts " +
                            "from x join y on (sym) " +
                            "sample by 1h"
            );

            final String insertOlderRows = "insert into x values ('gbpusd', 1.320, '2024-09-09T12:01')" +
                    ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                    ",('jpyusd', 103.21, '2024-09-11T12:02')" +
                    ",('gbpusd', 1.321, '2024-09-12T13:02')";
            execute(insertOlderRows);
            currentMicros = parseFloorPartialTimestamp("2024-01-01T01:01:01.842574Z");
            drainQueues();
            assertQueryNoLeakCheck("sym\tprice\tts\n", "x_1h order by sym");
            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\trefresh_base_table_txn\tbase_table_txn\n" +
                            "x_1h\tincremental\tx\tvalid\t\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\t1\t1\n",
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason, " +
                            "last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );

            // Insert data into y. Range refresh should aggregate rows within the interval only.
            execute("insert into y values ('gbpusd'),('jpyusd')");
            execute("refresh materialized view x_1h range from '2024-09-10T12:02' to '2024-09-11T12:02'");
            drainQueues();
            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-11T12:00:00.000000Z\n",
                    "x_1h order by sym, ts"
            );
            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\trefresh_base_table_txn\tbase_table_txn\n" +
                            "x_1h\tincremental\tx\tvalid\t\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\t1\t1\n",
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason, " +
                            "last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );

            // Insert a row with newer timestamp. This time incremental refresh should only aggregate the new row.
            execute("insert into x (sym, price, ts) values ('gbpusd', 1.320, '2024-09-13T13:13');");
            drainQueues();
            final String expected = "sym\tprice\tts\n" +
                    "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                    "gbpusd\t1.32\t2024-09-13T13:00:00.000000Z\n" + // newer timestamp
                    "jpyusd\t103.21\t2024-09-11T12:00:00.000000Z\n";
            assertQueryNoLeakCheck(expected, "x_1h order by sym, ts");
            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\trefresh_base_table_txn\tbase_table_txn\n" +
                            "x_1h\tincremental\tx\tvalid\t\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\t2\t2\n",
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason, " +
                            "last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );

            // Make the view invalid. Range refresh should be ignored.
            execute("truncate table x;");
            execute(insertOlderRows);
            drainQueues();
            execute("refresh materialized view x_1h range from '2024-09-10T12:02' to '2024-09-11T12:02';");
            assertQueryNoLeakCheck(expected, "x_1h order by sym, ts");
            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\trefresh_base_table_txn\tbase_table_txn\n" +
                            "x_1h\tincremental\tx\tinvalid\ttruncate operation\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\t2\t4\n",
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason, " +
                            "last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );
        });
    }

    @Test
    public void testRangeRefreshIgnoresRefreshLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price ( " +
                            "sym varchar, price double, ts timestamp " +
                            ") timestamp(ts) partition by DAY WAL"
            );
            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute(
                    "insert into base_price values ('gbpusd', 1.320, '2024-09-09T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-11T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-12T13:02')"
            );
            currentMicros = parseFloorPartialTimestamp("2024-09-13T00:00:00.000000Z");
            drainQueues();
            final String ogExpected = "sym\tprice\tts\n" +
                    "gbpusd\t1.32\t2024-09-09T12:00:00.000000Z\n" +
                    "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                    "gbpusd\t1.321\t2024-09-12T13:00:00.000000Z\n" +
                    "jpyusd\t103.21\t2024-09-11T12:00:00.000000Z\n";
            assertQueryNoLeakCheck(ogExpected, "price_1h order by sym, ts");
            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\trefresh_base_table_txn\tbase_table_txn\n" +
                            "price_1h\tincremental\tbase_price\tvalid\t\t2024-09-13T00:00:00.000000Z\t2024-09-13T00:00:00.000000Z\t1\t1\n",
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason, " +
                            "last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );

            execute("alter materialized view price_1h set refresh limit 8h;");

            // Insert rows with older timestamps. They should be ignored due to the refresh limit.
            execute(
                    "insert into base_price values ('gbpusd', 2.431, '2024-09-09T00:01')" +
                            ",('jpyusd', 214.32, '2024-09-11T00:02')"
            );
            drainQueues();
            assertQueryNoLeakCheck(ogExpected, "price_1h order by sym, ts");
            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\trefresh_base_table_txn\tbase_table_txn\n" +
                            "price_1h\tincremental\tbase_price\tvalid\t\t2024-09-13T00:00:00.000000Z\t2024-09-13T00:00:00.000000Z\t1\t2\n",
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason, " +
                            "last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );

            // Run range refresh. The newly inserted rows should now be reflected in the mat view.
            execute("refresh materialized view price_1h range from '2024-09-09' to '2024-09-12';");
            drainQueues();
            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t2.431\t2024-09-09T00:00:00.000000Z\n" + // new row
                            "gbpusd\t1.32\t2024-09-09T12:00:00.000000Z\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-12T13:00:00.000000Z\n" +
                            "jpyusd\t214.32\t2024-09-11T00:00:00.000000Z\n" + // new row
                            "jpyusd\t103.21\t2024-09-11T12:00:00.000000Z\n",
                    "price_1h order by sym, ts"
            );
            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\trefresh_base_table_txn\tbase_table_txn\n" +
                            "price_1h\tincremental\tbase_price\tvalid\t\t2024-09-13T00:00:00.000000Z\t2024-09-13T00:00:00.000000Z\t1\t2\n",
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason, " +
                            "last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );
        });
    }

    @Test
    public void testRecursiveInvalidation() throws Exception {
        assertMemoryLeak(() -> {
            long startTs = TimestampFormatUtils.parseUTCTimestamp("2025-02-18T00:00:00.000000Z");
            long step = 100000000L;
            final int N = 100;

            String tableName = "base";
            String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b',null) s, ";
            execute(createTableSql(tableName, columns, null, startTs, step, N));
            drainQueues();

            String view1Name = "v1_base";
            String view1Query = "select k, s, first(lat) lat, last(lon) lon from " + tableName + " sample by 1h";
            createMatView(view1Name, view1Query);
            drainQueues();

            String view2Name = "v2_v1";
            String view2Query = "select k, s, first(lat) lat, last(lon) lon from " + view1Name + " sample by 2h";
            createMatView(view2Name, view2Query);
            drainQueues();

            String view3Name = "v3_v1";
            String view3Query = "select k, s, first(lat) lat, last(lon) lon from " + view1Name + " sample by 2h";
            createMatView(view3Name, view3Query);
            drainQueues();

            String view4Name = "v4_v3";
            String view4Query = "select k, s, first(lat) lat, last(lon) lon from " + view3Name + " sample by 4h";
            createMatView(view4Name, view4Query);

            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\n" +
                            "v1_base\tincremental\tbase\tvalid\t\n" +
                            "v2_v1\tincremental\tv1_base\tvalid\t\n" +
                            "v3_v1\tincremental\tv1_base\tvalid\t\n" +
                            "v4_v3\tincremental\tv3_v1\tvalid\t\n",
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason from materialized_views order by view_name",
                    null,
                    true
            );

            execute("truncate table " + tableName);
            long ts = TimestampFormatUtils.parseUTCTimestamp("2025-05-17T00:00:00.000000Z");
            execute("insert into " + tableName + " " + generateSelectSql(columns, ts, step, N, N));

            drainQueues();

            // all views should be invalid
            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\n" +
                            "v1_base\tincremental\tbase\tinvalid\ttruncate operation\n" +
                            "v2_v1\tincremental\tv1_base\tinvalid\tbase materialized view is invalidated\n" +
                            "v3_v1\tincremental\tv1_base\tinvalid\tbase materialized view is invalidated\n" +
                            "v4_v3\tincremental\tv3_v1\tinvalid\tbase materialized view is invalidated\n",
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason from materialized_views order by view_name",
                    null,
                    true
            );

            execute("refresh materialized view " + view1Name + " full");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\n" +
                            "v1_base\tincremental\tbase\tvalid\t\n" +
                            "v2_v1\tincremental\tv1_base\tinvalid\tbase materialized view is invalidated\n" +
                            "v3_v1\tincremental\tv1_base\tinvalid\tbase materialized view is invalidated\n" +
                            "v4_v3\tincremental\tv3_v1\tinvalid\tbase materialized view is invalidated\n",
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason from materialized_views order by view_name",
                    null,
                    true
            );

            // Refresh the rest
            execute("refresh materialized view " + view2Name + " full");
            drainQueues();
            execute("refresh materialized view " + view3Name + " full");
            drainQueues();
            execute("refresh materialized view " + view4Name + " full");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\n" +
                            "v1_base\tincremental\tbase\tvalid\t\n" +
                            "v2_v1\tincremental\tv1_base\tvalid\t\n" +
                            "v3_v1\tincremental\tv1_base\tvalid\t\n" +
                            "v4_v3\tincremental\tv3_v1\tvalid\t\n",
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason from materialized_views order by view_name",
                    null,
                    true
            );
        });
    }

    @Test
    public void testRecursiveInvalidationOnDropMatView() throws Exception {
        assertMemoryLeak(() -> {
            long startTs = TimestampFormatUtils.parseUTCTimestamp("2025-02-18T00:00:00.000000Z");
            long step = 100000000L;
            final int N = 100;

            String tableName = "base";
            String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b',null) s, ";
            execute(createTableSql(tableName, columns, null, startTs, step, N));
            drainQueues();

            String view1Name = "v1_base";
            String view1Query = "select k, s, first(lat) lat, last(lon) lon from " + tableName + " sample by 1h";
            createMatView(view1Name, view1Query);
            drainQueues();

            String view2Name = "v2_v1";
            String view2Query = "select k, s, first(lat) lat, last(lon) lon from " + view1Name + " sample by 2h";
            createMatView(view2Name, view2Query);
            drainQueues();

            String view3Name = "v3_v1";
            String view3Query = "select k, s, first(lat) lat, last(lon) lon from " + view1Name + " sample by 2h";
            createMatView(view3Name, view3Query);
            drainQueues();

            String view4Name = "v4_v3";
            String view4Query = "select k, s, first(lat) lat, last(lon) lon from " + view3Name + " sample by 4h";
            createMatView(view4Name, view4Query);

            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\n" +
                            "v1_base\tincremental\tbase\tvalid\t\n" +
                            "v2_v1\tincremental\tv1_base\tvalid\t\n" +
                            "v3_v1\tincremental\tv1_base\tvalid\t\n" +
                            "v4_v3\tincremental\tv3_v1\tvalid\t\n",
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason from materialized_views order by view_name",
                    null,
                    true
            );

            execute("drop materialized view v1_base");

            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\n" +
                            "v2_v1\tincremental\tv1_base\tinvalid\tbase table is dropped or renamed\n" +
                            "v3_v1\tincremental\tv1_base\tinvalid\tbase table is dropped or renamed\n" +
                            "v4_v3\tincremental\tv3_v1\tinvalid\tbase materialized view is invalidated\n",
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason from materialized_views order by view_name",
                    null,
                    true
            );
        });
    }

    @Test
    public void testRecursiveInvalidationOnFailedRefresh() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, amount int, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("price_1h", "select sym, last(price) as price, ts from base_price where npe() sample by 1h");
            createMatView("price_1d", "select sym, last(price) as price, ts from price_1h sample by 1d");
            createMatView("price_1d_2", "select sym, last(price) as price, ts from price_1h sample by 1d");
            createMatView("price_1w", "select sym, last(price) as price, ts from price_1d sample by 1w");

            execute(
                    "insert into base_price (sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\n" +
                            "price_1d\tincremental\tprice_1h\tinvalid\tbase materialized view refresh failed\n" +
                            "price_1d_2\tincremental\tprice_1h\tinvalid\tbase materialized view refresh failed\n" +
                            "price_1h\tincremental\tbase_price\tinvalid\t[-1]: unexpected filter error\n" +
                            "price_1w\tincremental\tprice_1d\tinvalid\tbase materialized view is invalidated\n",
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason from materialized_views order by view_name",
                    null,
                    true
            );
        });
    }

    @Test
    public void testRefreshExecutionContextBansWrongInsertions() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute(
                    "create table y (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            final MatViewRefreshExecutionContext refreshExecutionContext = new MatViewRefreshExecutionContext(engine, 1, 1);

            try (TableReader baseReader = engine.getReader("x")) {
                refreshExecutionContext.of(baseReader);

                // Base table writes should be permitted.
                engine.execute("insert into x values('gbpusd', 1.320, '2024-09-10T12:01')", refreshExecutionContext);
                // Everything else should be banned.
                try {
                    engine.execute("insert into y values('gbpusd', 1.320, '2024-09-10T12:01')", refreshExecutionContext);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Write permission denied");
                }
            }
        });
    }

    @Test
    public void testRefreshSkipsUnchangedBuckets() throws Exception {
        // Verify that incremental refresh skips unchanged SAMPLE BY buckets.
        assertMemoryLeak(() -> {
            TestTimestampCounterFactory.COUNTER.set(0);

            execute(
                    "create table base_price (" +
                            "  price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL;"
            );

            final String viewSql = "select ts, test_timestamp_counter(ts) ts0, max(price) max_price " +
                    "from base_price " +
                    "sample by 1d";
            createMatView(viewSql);

            execute("insert into base_price(price, ts) values (1.320, '2024-01-01T00:01'), (1.321, '2024-01-30T00:01');");

            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\tbase_table_name\tview_status\n" +
                            "price_1h\tbase_price\tvalid\n",
                    "select view_name, base_table_name, view_status from materialized_views",
                    null,
                    false
            );

            // The function must have been called for two days only although the full interval is 30 days.
            Assert.assertEquals(2, TestTimestampCounterFactory.COUNTER.get());

            final String expected = "ts\tts0\tmax_price\n" +
                    "2024-01-01T00:00:00.000000Z\t2024-01-01T00:00:00.000000Z\t1.32\n" +
                    "2024-01-30T00:00:00.000000Z\t2024-01-30T00:00:00.000000Z\t1.321\n";
            assertQueryNoLeakCheck(expected, viewSql + " order by ts", "ts", true, true);
            assertQueryNoLeakCheck(expected, "price_1h order by ts", "ts", true, true);
        });
    }

    @Test
    public void testResumeSuspendMatView() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp, extra_col long" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = parseFloorPartialTimestamp("2024-01-01T01:01:01.842574Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t1\t1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n",
                    "price_1h order by sym"
            );

            // suspend mat view
            execute("alter materialized view price_1h suspend wal");

            execute("insert into base_price(sym, price, ts) values('jpyusd', 103.14, '2024-09-10T13:04')");
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n",
                    "price_1h order by sym"
            );

            assertQueryNoLeakCheck(
                    "name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\n" +
                            "base_price\tfalse\t2\t0\t2\t\t\t0\n" +
                            "price_1h\ttrue\t1\t0\t2\t\t\t0\n",
                    "wal_tables()",
                    null,
                    false
            );

            // resume mat view
            execute("alter materialized view price_1h resume wal");
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.14\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h order by sym"
            );

            assertQueryNoLeakCheck(
                    "name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\n" +
                            "base_price\tfalse\t2\t0\t2\t\t\t0\n" +
                            "price_1h\tfalse\t2\t0\t2\t\t\t0\n",
                    "wal_tables()",
                    null,
                    false
            );

            // suspend mat view again
            execute("alter materialized view price_1h suspend wal");

            execute("insert into base_price(sym, price, ts) values('jpyusd', 103.17, '2024-09-10T13:22')");
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.14\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h order by sym"
            );

            assertQueryNoLeakCheck(
                    "name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\n" +
                            "base_price\tfalse\t3\t0\t3\t\t\t0\n" +
                            "price_1h\ttrue\t2\t0\t3\t\t\t0\n",
                    "wal_tables()",
                    null,
                    false
            );

            // resume mat view from txn
            execute("alter materialized view price_1h resume wal from txn 3");
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.17\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h order by sym"
            );

            assertQueryNoLeakCheck(
                    "name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\n" +
                            "base_price\tfalse\t3\t0\t3\t\t\t0\n" +
                            "price_1h\tfalse\t3\t0\t3\t\t\t0\n",
                    "wal_tables()",
                    null,
                    false
            );
        });
    }

    @Test
    public void testSampleByDST() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            execute(
                    "insert into base_price values" +
                            " ('gbpusd', 1.320, '2024-10-26T00:00')" +
                            ",('gbpusd', 1.321, '2024-10-26T01:00')" +
                            ",('gbpusd', 1.324, '2024-10-27T00:00')" +
                            ",('gbpusd', 1.325, '2024-10-27T01:00')" +
                            ",('gbpusd', 1.326, '2024-10-27T02:00')" +
                            ",('gbpusd', 1.327, '2024-10-28T00:00')" +
                            ",('gbpusd', 1.328, '2024-10-28T01:00')"
            );
            drainQueues();

            final String expected = "sym\tfirst\tlast\tcount\tts\n" +
                    "gbpusd\t1.32\t1.321\t2\t2024-10-25T22:00:00.000000Z\n" +
                    "gbpusd\t1.324\t1.326\t3\t2024-10-26T22:00:00.000000Z\n" +
                    "gbpusd\t1.327\t1.328\t2\t2024-10-27T23:00:00.000000Z\n";
            assertQueryNoLeakCheck(
                    expected,
                    "select sym, first(price) as first, last(price) as last, count() count, ts " +
                            "from base_price " +
                            "sample by 1d ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin' " +
                            "order by ts, sym",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testSampleByNoFillAlignToCalendarTimezoneOffset() throws Exception {
        assertMemoryLeak(() -> {
            final String viewName = "x_view";
            final String out = "select to_timezone(k, 'PST') k, c";
            final String viewQuery = "select k, count() c from x sample by 2h align to calendar time zone 'PST' with offset '00:42'";
            final long startTs = TimestampFormatUtils.parseUTCTimestamp("1970-01-03T00:20:00.000000Z");
            final long step = 300000000;
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewQuery, startTs, step, N, K);

            final String expected = "k\tc\n" +
                    "1970-01-02T14:42:00.000000Z\t5\n" +
                    "1970-01-02T16:42:00.000000Z\t24\n" +
                    "1970-01-02T18:42:00.000000Z\t24\n" +
                    "1970-01-02T20:42:00.000000Z\t24\n" +
                    "1970-01-02T22:42:00.000000Z\t23\n";

            assertQueryNoLeakCheck(expected, outSelect(out, viewQuery), null, true, true);
            assertQueryNoLeakCheck(expected, outSelect(out, viewName), null, true, true);
        });
    }

    @Test
    public void testSampleByNoFillNotKeyedAlignToCalendarMisalignedTimezone() throws Exception {
        // IRAN timezone is +4:30, which doesn't align well with 1hr sample
        assertMemoryLeak(() -> {
            final String viewName = "x_view";
            final String viewQuery = "select k, count() c from x sample by 1h align to calendar time zone 'Iran'";
            final long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-03-28T00:15:00.000000Z");
            final long step = 6 * 60000000;
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewQuery, startTs, step, N, K);

            final String expected = "k\tc\n" +
                    "2021-03-28T04:00:00.000000Z\t3\n" +
                    "2021-03-28T05:00:00.000000Z\t10\n" +
                    "2021-03-28T06:00:00.000000Z\t10\n" +
                    "2021-03-28T07:00:00.000000Z\t10\n" +
                    "2021-03-28T08:00:00.000000Z\t10\n" +
                    "2021-03-28T09:00:00.000000Z\t10\n" +
                    "2021-03-28T10:00:00.000000Z\t10\n" +
                    "2021-03-28T11:00:00.000000Z\t10\n" +
                    "2021-03-28T12:00:00.000000Z\t10\n" +
                    "2021-03-28T13:00:00.000000Z\t10\n" +
                    "2021-03-28T14:00:00.000000Z\t7\n";

            final String out = "select to_timezone(k, 'Iran') k, c";
            assertQueryNoLeakCheck(expected, outSelect(out, viewQuery), null, true, true);
            assertQueryNoLeakCheck(expected, outSelect(out, viewName), null, true, true);
        });
    }

    @Test
    public void testSampleByNoFillNotKeyedAlignToCalendarTimezone() throws Exception {
        assertMemoryLeak(() -> {
            final String viewName = "x_view";
            final String viewQuery = "select k, count() c from x sample by 1h align to calendar time zone 'Europe/Berlin'";
            final long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-03-28T00:15:00.000000Z");
            final long step = 6 * 60000000;
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewQuery, startTs, step, N, K);

            final String expected = "k\tc\n" +
                    "2021-03-28T01:00:00.000000Z\t8\n" +
                    "2021-03-28T03:00:00.000000Z\t10\n" +
                    "2021-03-28T04:00:00.000000Z\t10\n" +
                    "2021-03-28T05:00:00.000000Z\t10\n" +
                    "2021-03-28T06:00:00.000000Z\t10\n" +
                    "2021-03-28T07:00:00.000000Z\t10\n" +
                    "2021-03-28T08:00:00.000000Z\t10\n" +
                    "2021-03-28T09:00:00.000000Z\t10\n" +
                    "2021-03-28T10:00:00.000000Z\t10\n" +
                    "2021-03-28T11:00:00.000000Z\t10\n" +
                    "2021-03-28T12:00:00.000000Z\t2\n";

            final String out = "select to_timezone(k, 'Europe/Berlin') k, c";
            assertQueryNoLeakCheck(expected, outSelect(out, viewQuery), null, true, true);
            assertQueryNoLeakCheck(expected, outSelect(out, viewName), null, true, true);
        });
    }

    @Test
    public void testSampleByNoFillNotKeyedAlignToCalendarTimezoneFixedFormat() throws Exception {
        assertMemoryLeak(() -> testAlignToCalendarTimezoneOffset("GMT+01:00"));
    }

    @Test
    public void testSampleByNoFillNotKeyedAlignToCalendarTimezoneOct() throws Exception {
        // We are going over spring time change. Because time is "expanding" we don't have
        // to do anything special. Our UTC timestamps will show "gap" and data doesn't
        // have to change
        assertMemoryLeak(() -> {
            final String viewName = "x_view";
            final String viewQuery = "select k, count() c from x sample by 1h align to calendar time zone 'Europe/Berlin'";
            final long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-10-31T00:15:00.000000Z");
            final long step = 6 * 60000000;
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewQuery, startTs, step, N, K);

            final String expected = "k\tc\n" +
                    "2021-10-31T02:00:00.000000Z\t18\n" +
                    "2021-10-31T03:00:00.000000Z\t10\n" +
                    "2021-10-31T04:00:00.000000Z\t10\n" +
                    "2021-10-31T05:00:00.000000Z\t10\n" +
                    "2021-10-31T06:00:00.000000Z\t10\n" +
                    "2021-10-31T07:00:00.000000Z\t10\n" +
                    "2021-10-31T08:00:00.000000Z\t10\n" +
                    "2021-10-31T09:00:00.000000Z\t10\n" +
                    "2021-10-31T10:00:00.000000Z\t10\n" +
                    "2021-10-31T11:00:00.000000Z\t2\n";
            final String out = "select to_timezone(k, 'Europe/Berlin') k, c";
            assertQueryNoLeakCheck(expected, outSelect(out, viewQuery));
            assertQueryNoLeakCheck(expected, outSelect(out, viewName));
        });
    }

    @Test
    public void testSampleByNoFillNotKeyedAlignToCalendarTimezoneOctMin() throws Exception {
        // We are going over spring time change. Because time is "expanding" we don't have
        // to do anything special. Our UTC timestamps will show "gap" and data doesn't
        // have to change
        assertMemoryLeak(() -> {
            final String viewName = "x_view";
            final String viewQuery = "select k, count() c from x sample by 30m align to calendar time zone 'Europe/Berlin'";
            final long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-10-31T00:15:00.000000Z");
            final long step = 6 * 60000000;
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewQuery, startTs, step, N, K);

            final String expected = "k\tc\n" +
                    "2021-10-31T02:00:00.000000Z\t8\n" +
                    "2021-10-31T02:30:00.000000Z\t10\n" +
                    "2021-10-31T03:00:00.000000Z\t5\n" +
                    "2021-10-31T03:30:00.000000Z\t5\n" +
                    "2021-10-31T04:00:00.000000Z\t5\n" +
                    "2021-10-31T04:30:00.000000Z\t5\n" +
                    "2021-10-31T05:00:00.000000Z\t5\n" +
                    "2021-10-31T05:30:00.000000Z\t5\n" +
                    "2021-10-31T06:00:00.000000Z\t5\n" +
                    "2021-10-31T06:30:00.000000Z\t5\n" +
                    "2021-10-31T07:00:00.000000Z\t5\n" +
                    "2021-10-31T07:30:00.000000Z\t5\n" +
                    "2021-10-31T08:00:00.000000Z\t5\n" +
                    "2021-10-31T08:30:00.000000Z\t5\n" +
                    "2021-10-31T09:00:00.000000Z\t5\n" +
                    "2021-10-31T09:30:00.000000Z\t5\n" +
                    "2021-10-31T10:00:00.000000Z\t5\n" +
                    "2021-10-31T10:30:00.000000Z\t5\n" +
                    "2021-10-31T11:00:00.000000Z\t2\n";

            final String out = "select to_timezone(k, 'Europe/Berlin') k, c";
            assertQueryNoLeakCheck(expected, outSelect(out, viewQuery), null, true, true);
            assertQueryNoLeakCheck(expected, outSelect(out, viewName), null, true, true);
        });
    }

    @Test
    public void testSampleByNoFillNotKeyedAlignToCalendarTimezoneOffset() throws Exception {
        assertMemoryLeak(() -> testAlignToCalendarTimezoneOffset("PST"));
    }

    @Test
    public void testSampleByNoFillNotKeyedAlignToCalendarUTC() throws Exception {
        assertMemoryLeak(() -> {
            final String viewName = "x_view";
            final String viewQuery = "select k, count() c from x sample by 90m align to calendar";
            final long startTs = 172800000000L;
            final long step = 300000000;
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewQuery, startTs, step, N, K);

            final String expected = "k\tc\n" +
                    "1970-01-03T00:00:00.000000Z\t18\n" +
                    "1970-01-03T01:30:00.000000Z\t18\n" +
                    "1970-01-03T03:00:00.000000Z\t18\n" +
                    "1970-01-03T04:30:00.000000Z\t18\n" +
                    "1970-01-03T06:00:00.000000Z\t18\n" +
                    "1970-01-03T07:30:00.000000Z\t10\n";

            assertQueryNoLeakCheck(expected, viewQuery, "k", true, true);
            assertQueryNoLeakCheck(expected, viewName, "k", true, true);
        });
    }

    @Test
    public void testSampleByNoFillNotKeyedAlignToCalendarUTCOffset() throws Exception {
        assertMemoryLeak(() -> {
            final String viewName = "x_view";
            final String viewQuery = "select k, count() c from x sample by 90m align to calendar with offset '00:42'";
            final long startTs = 172800000000L;
            final long step = 300000000;
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewQuery, startTs, step, N, K);

            final String expected = "k\tc\n" +
                    "1970-01-02T23:12:00.000000Z\t9\n" +
                    "1970-01-03T00:42:00.000000Z\t18\n" +
                    "1970-01-03T02:12:00.000000Z\t18\n" +
                    "1970-01-03T03:42:00.000000Z\t18\n" +
                    "1970-01-03T05:12:00.000000Z\t18\n" +
                    "1970-01-03T06:42:00.000000Z\t18\n" +
                    "1970-01-03T08:12:00.000000Z\t1\n";

            assertQueryNoLeakCheck(expected, viewQuery, "k", true, true);
            assertQueryNoLeakCheck(expected, viewName, "k", true, true);
        });
    }

    @Test
    public void testSelfJoinQuery() throws Exception {
        // Verify that the detached base table reader used by the refresh job
        // can be safely used in the mat view query multiple times.
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym symbol index, sym2 symbol, price double, ts timestamp, extra_col long" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            final String viewSql = "select a.sym sym_a, b.sym sym_b, a.sym2 sym2_a, b.sym2 sym2_b, last(b.price) as price, a.ts " +
                    "from (base_price where sym = 'foobar') a " +
                    "asof join (base_price where sym = 'barbaz') b on (sym2) " +
                    "sample by 1h";
            createMatView(viewSql);

            execute(
                    "insert into base_price(sym, sym2, price, ts) values('foobar', 's1', 1.320, '2024-09-10T12:01')" +
                            ",('foobar', 's1', 1.323, '2024-09-10T12:02')" +
                            ",('barbaz', 's1', 103.21, '2024-09-10T12:02')" +
                            ",('foobar', 's1', 1.321, '2024-09-10T13:02')" +
                            ",('barbaz', 's1', 103.23, '2024-09-10T13:02')"
            );

            currentMicros = parseFloorPartialTimestamp("2024-01-01T01:01:01.842574Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\tbase_table_name\tview_status\n" +
                            "price_1h\tbase_price\tvalid\n",
                    "select view_name, base_table_name, view_status from materialized_views",
                    null,
                    false
            );

            final String expected = "sym_a\tsym_b\tsym2_a\tsym2_b\tprice\tts\n" +
                    "foobar\t\ts1\t\tnull\t2024-09-10T12:00:00.000000Z\n" +
                    "foobar\tbarbaz\ts1\ts1\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                    "foobar\tbarbaz\ts1\ts1\t103.23\t2024-09-10T13:00:00.000000Z\n";
            assertQueryNoLeakCheck(expected, viewSql + " order by ts, sym_a, sym_b", "ts", true);
            assertQueryNoLeakCheck(expected, "price_1h order by ts, sym_a, sym_b", "ts", true, true);
        });
    }

    @Test
    public void testSimpleCancelRefresh() throws Exception {
        assertMemoryLeak(() -> {
            final SOCountDownLatch started = new SOCountDownLatch(1);
            final SOCountDownLatch stopped = new SOCountDownLatch(1);
            final AtomicBoolean refreshed = new AtomicBoolean(true);

            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            String viewSql = "select sym, last(price) as price, ts from base_price where sleep(120000) sample by 1h";
            createMatView(viewSql);
            drainQueues();

            execute(
                    "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );
            drainWalQueue();

            new Thread(
                    () -> {
                        started.countDown();
                        try {
                            try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine)) {
                                refreshed.set(job.run(0));
                            }
                        } finally {
                            stopped.countDown();
                        }
                    }, "mat_view_refresh_thread"
            ).start();

            started.await();

            long queryId = -1;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                String activityQuery = "select query_id, query from query_activity() where query ='" + viewSql + "'";
                try (final RecordCursorFactory factory = CairoEngine.select(compiler, activityQuery, sqlExecutionContext)) {
                    while (stopped.getCount() != 0) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            if (cursor.hasNext()) {
                                queryId = cursor.getRecord().getLong(0);
                                break;
                            }
                        }
                    }
                } catch (SqlException e) {
                    Assert.fail(e.getMessage());
                }
            }

            Assert.assertTrue(queryId > 0);
            execute("cancel query " + queryId);
            stopped.await();
            Assert.assertFalse(refreshed.get());

            drainWalQueue();
            assertQueryNoLeakCheck(
                    "view_name\tview_status\n" +
                            "price_1h\tinvalid\n",
                    "select view_name, view_status from materialized_views",
                    null,
                    false
            );
        });
    }

    @Test
    public void testSimpleRefresh() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute(
                    "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h order by ts, sym",
                    "ts",
                    true,
                    true
            );

            execute(
                    "insert into base_price values('gbpusd', 1.319, '2024-09-10T12:05')" +
                            ",('gbpusd', 1.325, '2024-09-10T13:03')"
            );
            drainQueues();

            String expected = "sym\tprice\tts\n" +
                    "gbpusd\t1.319\t2024-09-10T12:00:00.000000Z\n" +
                    "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                    "gbpusd\t1.325\t2024-09-10T13:00:00.000000Z\n";

            assertQueryNoLeakCheck(expected, "select sym, last(price) as price, ts from base_price sample by 1h order by ts, sym", "ts", true, true);
            assertQueryNoLeakCheck(expected, "price_1h order by ts, sym", "ts", true, true);
        });
    }

    @Test
    public void testSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "  sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            final String viewSql = "select sym0, last(price0) price, ts0 " +
                    "from (select ts as ts0, sym as sym0, price as price0 from base_price) " +
                    "sample by 1h";

            createMatView(viewSql);
            execute(
                    "insert into base_price " +
                            "select 'gbpusd', 1.320 + x / 1000.0, timestamp_sequence('2024-09-10T12:02', 1000000*60*5) " +
                            "from long_sequence(24 * 20 * 5)"
            );
            drainQueues();

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertEquals(
                        compiler,
                        sqlExecutionContext,
                        viewSql + " order by ts0, sym0",
                        "price_1h order by ts0, sym0"
                );
            }
        });
    }

    @Test
    public void testTimerMatViewBigJumpsClockAfterTickBoundary() throws Exception {
        testTimerMatViewBigJumps("2024-12-12T12:00:01.000000Z", "2024-12-12T13:00:01.000000Z", 2 * Timestamps.HOUR_MICROS);
    }

    @Test
    public void testTimerMatViewBigJumpsClockAtTickBoundary() throws Exception {
        testTimerMatViewBigJumps("2024-12-12T12:00:00.000000Z", "2024-12-12T12:00:00.000000Z", Timestamps.HOUR_MICROS);
    }

    @Test
    public void testTimerMatViewSmallHourJumps() throws Exception {
        testTimerMatViewSmallJumps(
                "2024-12-12T00:00:00.000000Z",
                "2d",
                "2024-12-11T00:00:00.000000Z",
                Timestamps.HOUR_MICROS,
                23
        );
    }

    @Test
    public void testTimerMatViewSmallMinuteJumps1() throws Exception {
        testTimerMatViewSmallJumps(
                "2024-12-12T01:30:00.000000Z",
                "30m",
                "2024-12-12T01:00:00.000000Z",
                Timestamps.MINUTE_MICROS,
                29
        );
    }

    @Test
    public void testTimerMatViewSmallMinuteJumps2() throws Exception {
        testTimerMatViewSmallJumps(
                "2024-12-12T01:30:00.000000Z",
                "30m",
                "2024-12-12T01:45:00.000000Z",
                Timestamps.MINUTE_MICROS,
                14
        );
    }

    @Test
    public void testTimerMatViewSmoke() throws Exception {
        assertMemoryLeak(() -> {
            final long start = parseFloorPartialTimestamp("2024-12-12T00:00:00.000000Z");

            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createNthTimerMatView(start, 0);

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = start;
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n",
                    "price_1h_0 order by sym"
            );

            // Tick the timer once again, this time with no new transaction.
            currentMicros += 2 * Timestamps.HOUR_MICROS;
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            // Insert new rows
            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.321, '2024-09-10T14:02')" +
                            ",('jpyusd', 103.22, '2024-09-10T14:03')"
            );

            // Do more timer ticks.
            currentMicros += 2 * Timestamps.HOUR_MICROS;
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T14:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.22\t2024-09-10T14:00:00.000000Z\n",
                    "price_1h_0 order by sym"
            );
        });
    }

    @Test
    public void testTimerMatViewStartEpsilon1() throws Exception {
        assertMemoryLeak(() -> {
            final long start = parseFloorPartialTimestamp("2024-12-12T00:00:00.000000Z");

            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createNthTimerMatView(start, 0);

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = start + 30 * Timestamps.SECOND_MICROS;
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            // The current time is within the [start, start+epsilon] interval, so the view should refresh.
            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n",
                    "price_1h_0 order by sym"
            );
        });
    }

    @Test
    public void testTimerMatViewStartEpsilon2() throws Exception {
        assertMemoryLeak(() -> {
            final long start = parseFloorPartialTimestamp("2024-12-12T00:00:00.000000Z");

            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createNthTimerMatView(start, 0);

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = start + 61 * Timestamps.SECOND_MICROS;
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            // The current time is after the [start, start+epsilon] interval, so the view shouldn't refresh.
            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n",
                    "price_1h_0 order by sym"
            );

            currentMicros = start + Timestamps.HOUR_MICROS;
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            // It's the next hourly interval now, so the view should refresh.
            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n",
                    "price_1h_0 order by sym"
            );
        });
    }

    @Test
    public void testTimestampGetsRefreshedOnInvalidation() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, amount int, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            final String viewQuery = "select sym, last(price) as price, ts from base_price sample by 1d";
            createMatView(viewQuery);

            execute(
                    "insert into base_price (sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );
            drainQueues();

            final String expected = "sym\tprice\tts\n" +
                    "gbpusd\t1.321\t2024-09-10T00:00:00.000000Z\n" +
                    "jpyusd\t103.21\t2024-09-10T00:00:00.000000Z\n";
            assertQueryNoLeakCheck(expected, viewQuery, "ts", true, true);
            assertQueryNoLeakCheck(expected, "price_1h", "ts", true, true);

            currentMicros = parseFloorPartialTimestamp("2020-01-01T01:01:01.000000Z");
            execute("drop table base_price;");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental\tbase_price\t2020-01-01T01:01:01.000000Z\t2020-01-01T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1d\tprice_1h~2\tbase table is dropped or renamed\tinvalid\t1\t-1\t0\t\t\t0\t\n",
                    "materialized_views",
                    null
            );
        });
    }

    @Test
    public void testTtl() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            execute("create materialized view price_1h as (select sym, last(price) as price, ts from base_price sample by 1h) partition by DAY ttl 2 days");

            execute(
                    "insert into base_price values('gbpusd', 1.310, '2024-09-10T12:05')" +
                            ",('gbpusd', 1.311, '2024-09-11T13:03')" +
                            ",('gbpusd', 1.312, '2024-09-12T13:03')" +
                            ",('gbpusd', 1.313, '2024-09-13T13:03')" +
                            ",('gbpusd', 1.314, '2024-09-14T13:03')"
            );
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.312\t2024-09-12T13:00:00.000000Z\n" +
                            "gbpusd\t1.313\t2024-09-13T13:00:00.000000Z\n" +
                            "gbpusd\t1.314\t2024-09-14T13:00:00.000000Z\n",
                    "price_1h order by ts, sym",
                    "ts",
                    true,
                    true
            );
        });
    }

    private static void assertCannotModifyMatView(String updateSql) {
        try {
            execute(updateSql);
        } catch (SqlException e) {
            Assert.assertTrue(e.getMessage().contains("cannot modify materialized view"));
        }
    }

    private static void assertViewMatchesSqlOverBaseTable(String viewSql) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            TestUtils.assertEquals(
                    compiler,
                    sqlExecutionContext,
                    viewSql + " order by ts, sym",
                    "price_1h order by ts, sym"
            );
        }
    }

    private static void createMatView(String viewSql) throws SqlException {
        execute("create materialized view price_1h as (" + viewSql + ") partition by DAY");
    }

    private static void createMatView(String viewName, String viewSql) throws SqlException {
        execute("create materialized view " + viewName + " as (" + viewSql + ") partition by DAY");
    }

    private static void createNthTimerMatView(long start, int n) throws SqlException {
        sink.clear();
        TimestampFormatUtils.appendDateTimeUSec(sink, start + n * Timestamps.SECOND_MICROS);
        execute(
                "create materialized view price_1h_" + n + " refresh start '" + sink + "' every 1m as (" +
                        "select sym, last(price) as price, ts from base_price sample by 1h" +
                        ") partition by day;"
        );
    }

    private static void dropNthTimerMatView(int n) throws SqlException {
        execute("drop materialized view if exists price_1h_" + n + ";");
    }

    private String copySql(int from, int count) {
        return "select * from tmp where n >= " + from + " and n < " + (from + count);
    }

    private String createTableSql(String tableName, String columns, @Nullable String index, long startTs, long step, int count) {
        String indexStr = index == null ? "" : ",index(" + index + ") ";
        return "create table " + tableName + " as (" + generateSelectSql(columns, startTs, step, 0, count) + ")" +
                indexStr +
                " timestamp(k) partition by DAY WAL";
    }

    private void drainQueues() {
        drainWalAndMatViewQueues();
        // purge job may create MatViewRefreshList for existing tables by calling engine.getDependentMatViews();
        // this affects refresh logic in some scenarios, so make sure to run it
        drainPurgeJob();
    }

    private void dropMatView() throws SqlException {
        execute("drop materialized view price_1h;");
    }

    private String generateSelectSql(String columns, long startTs, long step, int init, int count) {
        return "select" +
                " x + " + init + " as n," +
                columns +
                " timestamp_sequence(" + startTs + ", " + step + ") k" +
                " from" +
                " long_sequence(" + count + ")";
    }

    private String outSelect(String out, String in) {
        return out + " from (" + in + ")";
    }

    private void testAlignToCalendarTimezoneOffset(final String timezone) throws Exception {
        final String viewName = "x_view";
        final String viewQuery = "select k, count() c from x sample by 90m align to calendar time zone '" + timezone + "' with offset '00:42'";
        final long startTs = 172800000000L;
        final long step = 300000000;
        final int N = 100;
        final int K = 5;
        updateViewIncrementally(viewQuery, startTs, step, N, K);

        final String expected = "k\tc\n" +
                "1970-01-02T23:42:00.000000Z\t15\n" +
                "1970-01-03T01:12:00.000000Z\t18\n" +
                "1970-01-03T02:42:00.000000Z\t18\n" +
                "1970-01-03T04:12:00.000000Z\t18\n" +
                "1970-01-03T05:42:00.000000Z\t18\n" +
                "1970-01-03T07:12:00.000000Z\t13\n";

        assertQueryNoLeakCheck(expected, viewQuery, "k", true, true);
        assertQueryNoLeakCheck(expected, viewName, "k", true, true);
    }

    private void testBaseTableInvalidateOnOperation(String operationSql, String invalidationReason) throws Exception {
        testBaseTableInvalidateOnOperation(null, operationSql, invalidationReason);
    }

    private void testBaseTableInvalidateOnOperation(
            @Nullable TestUtils.LeakProneCode runBeforeMatViewCreate,
            String operationSql,
            String invalidationReason
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, amount int, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            if (runBeforeMatViewCreate != null) {
                runBeforeMatViewCreate.run();
            }

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute(
                    "insert into base_price (sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );
            drainQueues();

            currentMicros = parseFloorPartialTimestamp("2024-10-24T17:22:09.842574Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\tbase_table_name\tview_status\n" +
                            "price_1h\tbase_price\tvalid\n",
                    "select view_name, base_table_name, view_status from materialized_views",
                    null,
                    false
            );

            execute(operationSql);
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\tbase_table_name\tview_status\tinvalidation_reason\n" +
                            "price_1h\tbase_price\tinvalid\t" + invalidationReason + "\n",
                    "select view_name, base_table_name, view_status, invalidation_reason from materialized_views",
                    null,
                    false
            );
        });
    }

    private void testBaseTableNoInvalidateOnOperation(String operationSql) throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, amount int, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute(
                    "insert into base_price (sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );
            drainQueues();

            currentMicros = parseFloorPartialTimestamp("2024-10-24T17:22:09.842574Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\tbase_table_name\tview_status\n" +
                            "price_1h\tbase_price\tvalid\n",
                    "select view_name, base_table_name, view_status from materialized_views",
                    null,
                    false
            );

            execute(operationSql);
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\tbase_table_name\tview_status\tinvalidation_reason\n" +
                            "price_1h\tbase_price\tvalid\t\n",
                    "select view_name, base_table_name, view_status, invalidation_reason from materialized_views",
                    null,
                    false
            );
        });
    }

    private void testEnableDedupWithSubsetKeys(String enableDedupSql, boolean expectInvalid) throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE base_price (" +
                            "  sym VARCHAR, price DOUBLE, amount INT, ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym);"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute(
                    "insert into base_price (sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );
            drainQueues();

            currentMicros = parseFloorPartialTimestamp("2024-10-24T17:22:09.842574Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\tbase_table_name\tview_status\n" +
                            "price_1h\tbase_price\tvalid\n",
                    "select view_name, base_table_name, view_status from materialized_views",
                    null,
                    false
            );

            execute(enableDedupSql);
            drainQueues();

            assertQueryNoLeakCheck(
                    "view_name\tbase_table_name\tview_status\n" +
                            "price_1h\tbase_price\t" + (expectInvalid ? "invalid" : "valid") + "\n",
                    "select view_name, base_table_name, view_status from materialized_views",
                    null,
                    false
            );
        });
    }

    private void testIncrementalRefresh0(String viewSql) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
        assertMemoryLeak(() -> {
            execute("create table base_price (" +
                    "sym varchar, price double, ts timestamp" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            createMatView(viewSql);

            execute(
                    "insert into base_price " +
                            "select 'gbpusd', 1.320 + x / 1000.0, timestamp_sequence('2024-09-10T12:02', 1000000*60*5) " +
                            "from long_sequence(24 * 20 * 5)"
            );
            drainQueues();

            assertQueryNoLeakCheck(
                    "sequencerTxn\tminTimestamp\tmaxTimestamp\n" +
                            "1\t2024-09-10T12:00:00.000000Z\t2024-09-18T19:00:00.000000Z\n",
                    "select sequencerTxn, minTimestamp, maxTimestamp from wal_transactions('price_1h')",
                    null,
                    false
            );

            execute(
                    "insert into base_price values('gbpusd', 1.319, '2024-09-10T12:05')" +
                            ",('gbpusd', 1.325, '2024-09-10T13:03')"
            );
            drainQueues();

            assertQueryNoLeakCheck(
                    "sequencerTxn\tminTimestamp\tmaxTimestamp\n" +
                            "1\t2024-09-10T12:00:00.000000Z\t2024-09-18T19:00:00.000000Z\n" +
                            "2\t2024-09-10T12:00:00.000000Z\t2024-09-10T13:00:00.000000Z\n",
                    "select sequencerTxn, minTimestamp, maxTimestamp from wal_transactions('price_1h')",
                    null,
                    false
            );

            assertViewMatchesSqlOverBaseTable(viewSql);
        });
    }

    private void testTimerMatViewBigJumps(String start, String initialClock, long clockJump) throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute(
                    "create materialized view price_1h refresh start '" + start + "' every 1h as (" +
                            "select sym, last(price) as price, ts from base_price sample by 1h" +
                            ") partition by day"
            );

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = parseFloorPartialTimestamp(initialClock);
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n",
                    "price_1h order by sym"
            );
            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit_value\trefresh_limit_unit\ttimer_start\ttimer_interval_value\ttimer_interval_unit\n" +
                            "price_1h\tincremental_timer\tbase_price\t" + initialClock + "\t" + initialClock + "\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t1\t1\t0\t\t" + start + "\t1\tHOUR\n",
                    "materialized_views",
                    null
            );

            execute("insert into base_price(sym, price, ts) values('jpyusd', 104.57, '2024-09-10T13:02')");

            currentMicros += clockJump;
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t104.57\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h order by sym"
            );
        });
    }

    private void testTimerMatViewSmallJumps(String start, String every, String initialClock, long clockJump, int ticksBeforeRefresh) throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute(
                    "create materialized view price_1h refresh start '" + start + "' every " + every + " as (" +
                            "select sym, last(price) as price, ts from base_price sample by 1h" +
                            ") partition by day"
            );

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = parseFloorPartialTimestamp(initialClock);
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);

            for (int i = 0; i < ticksBeforeRefresh; i++) {
                drainMatViewTimerQueue(timerJob);
                drainQueues();

                assertQueryNoLeakCheck(
                        "sym\tprice\tts\n",
                        "price_1h order by sym"
                );
                assertQueryNoLeakCheck(
                        "view_name\tbase_table_name\tview_status\tinvalidation_reason\n" +
                                "price_1h\tbase_price\tvalid\t\n",
                        "select view_name, base_table_name, view_status, invalidation_reason from materialized_views",
                        null
                );

                currentMicros += clockJump;
            }

            currentMicros += clockJump;
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n",
                    "price_1h order by sym"
            );
            assertQueryNoLeakCheck(
                    "view_name\tbase_table_name\tview_status\tinvalidation_reason\n" +
                            "price_1h\tbase_price\tvalid\t\n",
                    "select view_name, base_table_name, view_status, invalidation_reason from materialized_views",
                    null
            );
        });
    }

    private void updateViewIncrementally(String viewQuery, long startTs, long step, int N, int K) throws SqlException {
        updateViewIncrementally(viewQuery, " rnd_double(0)*100 a, rnd_symbol(5,4,4,1) b,", startTs, step, N, K);
    }

    private void updateViewIncrementally(String viewQuery, String columns, long startTs, long step, int N, int K) throws SqlException {
        updateViewIncrementally("x_view", viewQuery, columns, null, startTs, step, N, K);
    }

    private void updateViewIncrementally(String viewName, String viewQuery, String columns, @Nullable String index, long startTs, long step, int N, int K) throws SqlException {
        Rnd rnd = new Rnd();
        int initSize = rnd.nextInt(N / K) + 1;
        int remainingSize = N - initSize;
        int chunkSize = remainingSize / K;
        int tail = remainingSize % K;

        // create full tmp table in one go
        execute(createTableSql("tmp", columns, index, startTs, step, N));
        drainQueues();
        execute("create table " + "x" + " as (" + copySql(1, initSize) + ") timestamp(k) partition by DAY WAL");
        drainQueues();
        createMatView(viewName, viewQuery);
        drainQueues();

        int prev = initSize + 1;
        for (int i = 0; i < K; i++) {
            int size = chunkSize + (i < tail ? 1 : 0);
            execute("insert into x " + copySql(prev, size));
            prev = prev + size;
            drainWalAndMatViewQueues();
            remainingSize -= size;
        }

        Assert.assertEquals(0, remainingSize);
    }
}
