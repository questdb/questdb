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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewRefreshSqlExecutionContext;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.mv.WalTxnRangeLoader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.catalogue.MatViewsFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestTimestampCounterFactory;
import io.questdb.jit.JitUtil;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Files;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.nanotime.Nanos;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.cairo.TableUtils.DETACHED_DIR_MARKER;
import static io.questdb.cairo.wal.WalUtils.EVENT_FILE_NAME;
import static io.questdb.cairo.wal.WalUtils.WAL_NAME_BASE;
import static io.questdb.test.tools.TestUtils.generateRandom;


public class MatViewTest extends AbstractCairoTest {
    private final int rowsPerQuery;
    private final TestTimestampType timestampType;

    public MatViewTest() {
        final Rnd rnd = generateRandom(LOG);
        this.rowsPerQuery = rnd.nextInt(100) > 50 ? -1 : 1;
        this.timestampType = TestUtils.getTimestampType(rnd);
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // override default to test copy
        inputRoot = TestUtils.getCsvRoot();
        inputWorkRoot = TestUtils.unchecked(() -> temp.newFolder("imports" + System.nanoTime()).getAbsolutePath());
        AbstractCairoTest.setUpStatic();
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
    public void testAlterAddIndexInvalidStatement() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "  sym symbol, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            currentMicros = 0;
            execute("create materialized view price_1h as select sym, last(price) as price, ts from base_price sample by 1h");

            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter",
                    38,
                    "'column' expected"
            );

            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter;",
                    38,
                    "'column' expected"
            );

            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter blah",
                    39,
                    "'column' expected"
            );

            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column",
                    45,
                    "column name expected"
            );

            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column;",
                    45,
                    "column name expected"
            );

            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column xyz",
                    46,
                    "column 'xyz' does not exist in materialized view 'price_1h'"
            );

            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column price",
                    51,
                    "'symbol capacity', 'add index' or 'drop index' expected"
            );

            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column price;",
                    51,
                    "'symbol capacity', 'add index' or 'drop index' expected"
            );

            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column price x",
                    52,
                    "'symbol capacity', 'add index' or 'drop index' expected"
            );

            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column price ADD",
                    55,
                    "'index' expected"
            );

            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column price ADD something",
                    56,
                    "'index' expected"
            );

            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column price ADD index",
                    46,
                    "column 'price' is of type 'DOUBLE'. Index supports column type 'SYMBOL' only."
            );

            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column sym ADD index xxx",
                    60,
                    "'capacity' keyword expected"
            );

            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column sym ADD index Capacity",
                    68,
                    "index capacity value expected"
            );

            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column sym ADD index Capacity S",
                    69,
                    "index capacity value must be numeric"
            );
        });
    }

    @Test
    public void testAlterMixed() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "  sym symbol, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute("insert into base_price(sym, price, ts) values ('gbpusd', 1.320, '2024-09-10T12:01')");
            // set refresh limit
            execute("alter materialized view price_1h set refresh limit 2 hours;");
            currentMicros = MicrosTimestampDriver.INSTANCE.parseFloorLiteral("2024-09-10T13:00:00.000000Z");
            drainQueues();

            // expect new limit
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit\trefresh_limit_unit
                            price_1h\timmediate\tbase_price\t2024-09-10T13:00:00.000000Z\t2024-09-10T13:00:00.000000Z\tvalid\t1\t1\t2\tHOUR
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_status, refresh_base_table_txn, base_table_txn, " +
                            "refresh_limit, refresh_limit_unit " +
                            "from materialized_views",
                    null
            );

            // insert a few old timestamps
            execute(
                    "insert into base_price(sym, price, ts) values ('gbpusd', 1.320, '2024-09-01T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-01T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-01T12:02')"
            );
            currentMicros = MicrosTimestampDriver.INSTANCE.parseFloorLiteral("2024-09-10T16:00:00.000000Z");
            drainQueues();

            // all old timestamps should be ignored
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp("""
                            sym\tprice\tts
                            gbpusd\t1.32\t2024-09-10T12:00:00.000000Z
                            """),
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
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-30T12:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-30T12:00:00.000000Z
                                    """),
                    "price_1h order by sym, ts"
            );
        });
    }

    @Test
    public void testAlterRefreshLimit() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "  sym symbol, price double, ts #TIMESTAMP" +
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
            final String matViewsSql = "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                    "view_status, refresh_base_table_txn, base_table_txn, " +
                    "refresh_limit, refresh_limit_unit " +
                    "from materialized_views";
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit\trefresh_limit_unit
                            price_1h\timmediate\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tvalid\t1\t1\t0\t
                            """,
                    matViewsSql,
                    null
            );

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp("""
                            sym\tprice\tts
                            gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                            gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                            jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                            """),
                    "price_1h order by sym, ts"
            );

            // set refresh limit
            execute("alter materialized view price_1h set refresh limit 1 day;");
            drainQueues();

            // expect new limit
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit\trefresh_limit_unit
                            price_1h\timmediate\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tvalid\t1\t1\t1\tDAY
                            """,
                    matViewsSql,
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
                    replaceExpectedTimestamp("sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T15:00:00.000000Z\n" + // the newer timestamp
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n"),
                    "price_1h order by sym, ts"
            );

            // disable refresh limit
            execute("alter materialized view price_1h set refresh limit 0 hour;");
            drainQueues();

            // expect new limit
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit\trefresh_limit_unit
                            price_1h\timmediate\tbase_price\t2024-09-10T16:00:00.000000Z\t2024-09-10T16:00:00.000000Z\tvalid\t2\t2\t0\t
                            """,
                    matViewsSql,
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
                    replaceExpectedTimestamp("sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-01T12:00:00.000000Z\n" + // old timestamp
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T15:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-01T12:00:00.000000Z\n" + // old timestamp
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n"),
                    "price_1h order by sym, ts"
            );
        });
    }

    @Test
    public void testAlterRefreshLimitFullRefresh() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "  sym symbol, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute(
                    "insert into base_price(sym, price, ts) values ('gbpusd', 1.320, '2023-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2023-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2023-11-10T12:02')" +
                            ",('gbpusd', 1.321, '2023-11-10T13:02')"
            );
            drainWalQueue();

            execute(
                    "create materialized view price_1h refresh manual deferred as " +
                            "select sym, last(price) as price, ts from base_price sample by 1h;"
            );
            execute("alter materialized view price_1h set refresh limit 2 months;");

            currentMicros = parseFloorPartialTimestamp("2024-01-01T01:01:01.000000Z");
            drainQueues();

            // expect no refresh
            final String matViewsSql = "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                    "view_status, refresh_base_table_txn, base_table_txn, " +
                    "refresh_limit, refresh_limit_unit " +
                    "from materialized_views";
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit\trefresh_limit_unit
                            price_1h\tmanual\tbase_price\t\t\tvalid\t-1\t1\t2\tMONTH
                            """,
                    matViewsSql,
                    null
            );
            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n",
                    "price_1h order by sym, ts"
            );

            // full refresh should respect the refresh limit,
            // so only the 2023-11-10 rows should be aggregated in the view
            execute("refresh materialized view price_1h full;");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit\trefresh_limit_unit
                            price_1h\tmanual\tbase_price\t2024-01-01T01:01:01.000000Z\t2024-01-01T01:01:01.000000Z\tvalid\t1\t1\t2\tMONTH
                            """,
                    matViewsSql,
                    null
            );
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp("""
                            sym\tprice\tts
                            gbpusd\t1.321\t2023-11-10T13:00:00.000000Z
                            jpyusd\t103.21\t2023-11-10T12:00:00.000000Z
                            """),
                    "price_1h order by sym, ts"
            );
        });
    }

    @Test
    public void testAlterRefreshLimitInvalidStatement() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "  sym symbol, price double, ts #TIMESTAMP" +
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
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_limit\trefresh_limit_unit
                            price_1h\timmediate\tbase_price\t\t\tvalid\t-1\t0\t0\t
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_status, refresh_base_table_txn, base_table_txn, " +
                            "refresh_limit, refresh_limit_unit " +
                            "from materialized_views",
                    null
            );
        });
    }

    @Test
    public void testAlterRefreshLimitPeriodMatView() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "  sym symbol, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute(
                    "insert into base_price(sym, price, ts) values ('gbpusd', 1.320, '2023-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2023-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2023-12-31T12:02')" +
                            ",('gbpusd', 1.321, '2023-12-31T13:02')"
            );
            drainWalQueue();

            execute(
                    "create materialized view price_1h refresh period(length 24h) as " +
                            "select sym, last(price) as price, ts from base_price sample by 1h;"
            );
            execute("alter materialized view price_1h set refresh limit 2 months;");

            // period refresh should respect the refresh limit,
            // so only the 2023-12-31 rows should be aggregated in the view
            currentMicros = parseFloorPartialTimestamp("2024-01-01T01:01:01.000000Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status\trefresh_period_hi\trefresh_base_table_txn\tbase_table_txn\tperiod_length\tperiod_length_unit\trefresh_limit\trefresh_limit_unit
                            price_1h\timmediate\tbase_price\t2024-01-01T01:01:01.000000Z\t2024-01-01T01:01:01.000000Z\tvalid\t2024-01-02T00:00:00.000000Z\t1\t1\t24\tHOUR\t2\tMONTH
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_status, refresh_period_hi, refresh_base_table_txn, base_table_txn, " +
                            "period_length, period_length_unit, refresh_limit, refresh_limit_unit " +
                            "from materialized_views",
                    null
            );
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp("""
                            sym\tprice\tts
                            gbpusd\t1.321\t2023-12-31T13:00:00.000000Z
                            jpyusd\t103.21\t2023-12-31T12:00:00.000000Z
                            """),
                    "price_1h order by sym, ts"
            );
        });
    }

    @Test
    public void testAlterRefreshParamsImmediateToManual() throws Exception {
        testAlterRefreshParamsToManual("immediate");
    }

    @Test
    public void testAlterRefreshParamsManualToManual() throws Exception {
        testAlterRefreshParamsToManual("manual");
    }

    @Test
    public void testAlterRefreshParamsPeriodToManual() throws Exception {
        testAlterRefreshParamsToManual("period(length 1h)");
    }

    @Test
    public void testAlterRefreshParamsTimerToManual() throws Exception {
        testAlterRefreshParamsToManual("every 10m");
    }

    @Test
    public void testAlterRefreshParamsToImmediate() throws Exception {
        testAlterRefreshParamsToTarget(
                "immediate",
                new TestRefreshParams()
                        .ofDeferred()
                        .ofImmediate()
        );
    }

    @Test
    public void testAlterRefreshParamsToManual() throws Exception {
        testAlterRefreshParamsToTarget(
                "manual",
                new TestRefreshParams()
                        .ofDeferred()
                        .ofManual()
        );
    }

    @Test
    public void testAlterRefreshParamsToPeriod() throws Exception {
        testAlterRefreshParamsToTarget(
                "immediate period(length 12h time zone 'Europe/London' delay 1h)",
                new TestRefreshParams()
                        .ofDeferred()
                        .ofPeriod()
        );
    }

    @Test
    public void testAlterRefreshParamsToTimer() throws Exception {
        testAlterRefreshParamsToTarget(
                "every 42m start '1970-01-01T00:00:00.000000Z' time zone 'Europe/Sofia'",
                new TestRefreshParams()
                        .ofDeferred()
                        .ofTimer()
        );
    }

    @Test
    public void testAlterRefreshTimer1() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute(
                    "create materialized view price_1h refresh every 1h deferred start '2260-12-12T12:00:00.000000Z' as (" +
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
            final String matViewsSql = "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                    "view_status, refresh_base_table_txn, base_table_txn, " +
                    "timer_time_zone, timer_start, timer_interval, timer_interval_unit " +
                    "from materialized_views";
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status\trefresh_base_table_txn\tbase_table_txn\ttimer_time_zone\ttimer_start\ttimer_interval\ttimer_interval_unit
                            price_1h\ttimer\tbase_price\t\t\tvalid\t-1\t1\t\t2260-12-12T12:00:00.000000Z\t1\tHOUR
                            """,
                    matViewsSql,
                    null
            );

            // the view should refresh after we change the timer schedule
            execute("alter materialized view price_1h set refresh every 1m start '" + start + "';");
            drainQueues();
            // we need timers to tick
            currentMicros += Micros.MINUTE_MICROS;
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status\trefresh_base_table_txn\tbase_table_txn\ttimer_time_zone\ttimer_start\ttimer_interval\ttimer_interval_unit
                            price_1h\ttimer\tbase_price\t1999-01-01T01:02:01.842574Z\t1999-01-01T01:02:01.842574Z\tvalid\t1\t1\t\t1999-01-01T01:01:01.842574Z\t1\tMINUTE
                            """,
                    matViewsSql,
                    null
            );
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp("""
                            sym\tprice\tts
                            gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                            gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                            jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                            """),
                    "price_1h order by sym"
            );
        });
    }

    @Test
    public void testAlterRefreshTimer2() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute(
                    "create materialized view price_1h refresh manual deferred as (" +
                            "select sym, last(price) as price, ts from base_price sample by 1h" +
                            ") partition by day"
            );
            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            // no refresh should happen as the view is manual
            final String start = "1999-01-01T01:01:01.842574Z";
            currentMicros = parseFloorPartialTimestamp(start);
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n",
                    "price_1h order by sym"
            );
            final String matViewsSql = "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                    "view_status, refresh_base_table_txn, base_table_txn, " +
                    "timer_time_zone, timer_start, timer_interval, timer_interval_unit " +
                    "from materialized_views";
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status\trefresh_base_table_txn\tbase_table_txn\ttimer_time_zone\ttimer_start\ttimer_interval\ttimer_interval_unit
                            price_1h\tmanual\tbase_price\t\t\tvalid\t-1\t1\t\t\t0\t
                            """,
                    matViewsSql,
                    null
            );

            // the view should refresh after we change the timer schedule
            execute("alter materialized view price_1h set refresh every 1m start '" + start + "';");
            drainQueues();
            // we need timers to tick
            currentMicros += Micros.MINUTE_MICROS;
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status\trefresh_base_table_txn\tbase_table_txn\ttimer_time_zone\ttimer_start\ttimer_interval\ttimer_interval_unit
                            price_1h\ttimer\tbase_price\t1999-01-01T01:02:01.842574Z\t1999-01-01T01:02:01.842574Z\tvalid\t1\t1\t\t1999-01-01T01:01:01.842574Z\t1\tMINUTE
                            """,
                    matViewsSql,
                    null
            );
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp("""
                            sym\tprice\tts
                            gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                            gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                            jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                            """),
                    "price_1h order by sym"
            );
        });
    }

    @Test
    public void testAlterRefreshTimerInvalidStatement() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "  sym symbol, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            currentMicros = 0;
            execute("create materialized view price_1h as select sym, last(price) as price, ts from base_price sample by 1h");
            execute("create materialized view price_1h_t refresh every 1h as select sym, last(price) as price, ts from base_price sample by 1h");

            assertExceptionNoLeakCheck(
                    "alter materialized view base_price set refresh every 1h",
                    24,
                    "materialized view name expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h set refresh every foobar",
                    51,
                    "Invalid unit: foobar"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h set refresh every 42h foobar",
                    55,
                    "unexpected token [foobar]"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h set refresh immediate foobar",
                    55,
                    "unexpected token [foobar]"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h set refresh manual foobar",
                    52,
                    "unexpected token [foobar]"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh",
                    46,
                    "'every' or 'limit' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh start",
                    47,
                    "'every' or 'limit' expected"
            );
            assertExceptionNoLeakCheck(
                    "ALTER MATERIALIZED VIEW 'price_1h_t' SET REFRESH EVERY",
                    54,
                    "interval expected"
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
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh every 1h start",
                    61,
                    "START timestamp"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh every 3d start 'foobar'",
                    62,
                    "invalid START timestamp value"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh every 3d foobar",
                    56,
                    "unexpected token [foobar]"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh every 3d start '2020-09-10T20:00:00.000000Z' time zone 'foobar'",
                    102,
                    "invalid timezone: foobar"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh every 3d start '2020-09-10T20:00:00.000000Z' time zone 'Europe/London' foobar",
                    118,
                    "unexpected token [foobar]"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh every 1M start '2020-09-10T20:00:00.000000Z' barbaz",
                    92,
                    "unexpected token [barbaz]"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh every -1M start '2020-09-10T20:00:00.000000Z'",
                    53,
                    "positive number expected: -"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh every 0M start '2020-09-10T20:00:00.000000Z'",
                    53,
                    "positive number expected: 0M"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh period foobar",
                    54,
                    "'(' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh manual period length",
                    61,
                    "'(' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh every 2h period (start)",
                    64,
                    "'length' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh period( length",
                    61,
                    "LENGTH interval expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh period( length 30m",
                    65,
                    "'time zone' or 'delay' or ')' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh period ( length 1d ) foobar",
                    68,
                    "unexpected token [foobar]"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh period ( length 2h time foobar )",
                    71,
                    "'zone' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh period(length 30m time zone)",
                    74,
                    "TIME ZONE name expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh period (length 1h time zone 'foobar')",
                    75,
                    "invalid timezone: foobar"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh period (length 1h time zone delay)",
                    75,
                    "TIME ZONE name expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh period (length 1h time zone 'Europe/Sofia' delay foobar)",
                    96,
                    "Invalid unit: foobar"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh period (length 1h time zone 'Europe/Sofia' delay 2h)",
                    96,
                    "delay cannot be equal to or greater than length"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh period (length 1h time zone 'Europe/Sofia' delay -2h)",
                    96,
                    "positive number expected: -"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh period (length 1h time zone 'Europe/Sofia' delay 0h)",
                    96,
                    "positive number expected: 0h"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh period (length -1h delay 42m)",
                    62,
                    "positive number expected: -"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh period (length 0m)",
                    62,
                    "positive number expected: 0m"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh period (length 1h time zone 'Europe/Sofia' delay 30m foobar",
                    100,
                    "')' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h_t set refresh period (length 25h delay 2h)",
                    62,
                    "maximum supported length interval is 24 hours: 25h"
            );

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status\trefresh_base_table_txn\tbase_table_txn\ttimer_time_zone\ttimer_start\ttimer_interval\ttimer_interval_unit
                            price_1h\timmediate\tbase_price\t\t\tvalid\t-1\t0\t\t\t0\t
                            price_1h_t\ttimer\tbase_price\t\t\tvalid\t-1\t0\t\t1970-01-01T00:00:00.000000Z\t1\tHOUR
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_status, refresh_base_table_txn, base_table_txn, " +
                            "timer_time_zone, timer_start, timer_interval, timer_interval_unit " +
                            "from materialized_views " +
                            "order by view_name",
                    null,
                    true
            );
        });
    }

    @Test
    public void testAlterSymbolCapacity() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "  sym symbol, price double, ts #TIMESTAMP" +
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
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tvalid\t1\t1
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_status, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );
        });
    }

    @Test
    public void testAlterSymbolCapacityInvalidStatement() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "  sym symbol, price double, ts #TIMESTAMP" +
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
                    "'symbol capacity', 'add index' or 'drop index' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column sym foobar;",
                    50,
                    "'symbol capacity', 'add index' or 'drop index' expected"
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
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t\t\tvalid\t-1\t0
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_status, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );
        });
    }

    @Test
    public void testAlterTtl() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
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
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.312\t2024-09-12T13:00:00.000000Z
                                    gbpusd\t1.313\t2024-09-13T13:00:00.000000Z
                                    gbpusd\t1.314\t2024-09-14T13:00:00.000000Z
                                    """),
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
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "  sym symbol, price double, ts #TIMESTAMP" +
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
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t\t\tvalid\t-1\t0
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_status, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );
        });
    }

    @Test
    public void testAsOfJoinBinarySearchHintInMatView() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("""
                    CREATE TABLE 'trades' (\s
                    price DOUBLE,
                    volume DOUBLE,
                    ts #TIMESTAMP
                    ) timestamp(ts) PARTITION BY DAY WAL""");

            execute("""
                    insert into trades
                      select\s
                        rnd_double() price,
                        rnd_double() volume,
                        ('2025'::timestamp + x * 200_000_000L + rnd_int(0, 10_000, 0))::timestamp as ts,
                      from long_sequence(5_000)
                    """
            );
            executeWithRewriteTimestamp("""
                    CREATE TABLE 'prices' (\s
                    bid DOUBLE,
                    ask DOUBLE,
                    valid BOOLEAN,
                    ts #TIMESTAMP
                    ) timestamp(ts) PARTITION BY DAY WAL
                    """);

            execute("""
                    insert into prices\s
                      select\s
                        rnd_double() bid,
                        rnd_double() ask,
                        rnd_boolean() valid,
                        ('2025'::timestamp + x * 1_000_000L + rnd_int(0, 10_000, 0))::timestamp as ts,
                      from long_sequence(1_000_000)
                    """
            );

            final String mvWithoutHint = """
                    create materialized view daily_summary\s
                    WITH BASE trades
                    as (
                    select trades.ts, count(*), sum(volume), min(price), max(price), avg(price)
                    FROM trades
                    asof join (select * from prices where valid) prices
                    sample by 1d
                    );""";
            final String mvWithLinearHint = """
                    create materialized view daily_summary\s
                    WITH BASE trades
                    as (
                    select /*+ ASOF_LINEAR(trades prices) */ trades.ts, count(*), sum(volume), min(price), max(price), avg(price)
                    FROM trades
                    asof join (select * from prices where valid) prices
                    sample by 1d
                    );""";

            // without the hint it does use Fast (=default)
            sink.clear();
            printSql("EXPLAIN " + mvWithoutHint);
            TestUtils.assertContains(sink, "Filtered AsOf Join Fast");

            // LINEAR hint -> does NOT use Fast
            sink.clear();
            printSql("EXPLAIN " + mvWithLinearHint);
            TestUtils.assertContains(sink, "AsOf Join");
            TestUtils.assertNotContains(sink, "Fast");

            // ok, now the real data: first try the view without the hint
            execute(mvWithoutHint);
            drainQueues();
            final String expectedView = """
                    ts\tcount\tsum\tmin\tmax\tavg
                    2025-01-01T00:00:00.000000Z\t431\t215.12906540853268\t0.0031075670450616544\t0.9975907992178104\t0.4923297830071461
                    2025-01-02T00:00:00.000000Z\t432\t214.8933638390628\t0.0027013057617086833\t0.9997998069306392\t0.5363814932706943
                    2025-01-03T00:00:00.000000Z\t432\t211.63403995544482\t0.0014510055926236776\t0.9979936641680203\t0.4900138748185357
                    2025-01-04T00:00:00.000000Z\t432\t225.1870697913935\t0.0026339327135822543\t0.9996217482017493\t0.49406088823120226
                    2025-01-05T00:00:00.000000Z\t432\t213.8124549264717\t0.00985149958244913\t0.9981734770138071\t0.4728684440748092
                    2025-01-06T00:00:00.000000Z\t432\t214.8994847762188\t0.0010433040681515626\t0.9998120012952196\t0.48758818235506823
                    2025-01-07T00:00:00.000000Z\t432\t220.0881500794553\t0.0014542249844708977\t0.9973956570924076\t0.5131734923704387
                    2025-01-08T00:00:00.000000Z\t432\t218.41372811829154\t8.166095924849737E-4\t0.9976953158075262\t0.5276052830143888
                    2025-01-09T00:00:00.000000Z\t432\t220.10482943202246\t0.0011023415061862663\t0.9974983068581821\t0.493060539742248
                    2025-01-10T00:00:00.000000Z\t432\t208.43848337612906\t0.0028067126112681917\t0.9976283386812487\t0.5136095078793146
                    2025-01-11T00:00:00.000000Z\t432\t213.02005186038846\t8.598501058093566E-4\t0.999708216046598\t0.5040670959429089
                    2025-01-12T00:00:00.000000Z\t249\t119.80938485754517\t0.007906045439897036\t0.9962991313334122\t0.4923923393746041
                    """;
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expectedView), "SELECT * FROM daily_summary", "ts", true, true);

            // now, recreate the view with avoid hint
            execute("drop materialized view daily_summary");
            execute(mvWithLinearHint);
            drainQueues();

            // it must result in the same data
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expectedView), "SELECT * FROM daily_summary", "ts", true, true);
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
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, amount int, ts #TIMESTAMP" +
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

            currentMicros = MicrosTimestampDriver.floor("2024-10-24T17:22:09.842574Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """,
                    "select view_name, base_table_name, view_status from materialized_views",
                    null,
                    false
            );

            execute("alter table base_price dedup enable upsert keys(ts);");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            price_1h\tbase_price\tvalid\t
                            """,
                    "select view_name, base_table_name, view_status, invalidation_reason from materialized_views",
                    null,
                    false
            );
        });
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
            executeWithRewriteTimestamp(
                    "CREATE TABLE 'GLBXMDP3_mbp1_es' ( " +
                            "       ts_event #TIMESTAMP, " +
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
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            mv_es_ohlcv_1s\timmediate\tglbxmdp3_mbp1_es\t2024-10-24T17:22:09.842574Z\t2024-10-24T17:22:09.842574Z\tSELECT ts_event AS time,   first(price) AS open,   max(price)   AS high,   min(price)   AS low,   last(price)  AS close,   sum(size)    AS volume FROM glbxmdp3_mbp1_es WHERE action = 'T' SAMPLE BY 1s ALIGN TO CALENDAR\tvalid\t1\t1
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
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
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute(
                    "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:00')"
            );

            currentMicros = MicrosTimestampDriver.floor("2024-10-24T17:22:09.842574Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    """),
                    "price_1h",
                    "ts",
                    true,
                    true
            );

            final TableToken baseToken = engine.getTableTokenIfExists("base_price");
            Assert.assertNotNull(baseToken);
            try (WalWriter writer = engine.getWalWriter(baseToken)) {
                writer.commitWithParams(
                        timestampType.getDriver().parseFloorLiteral("2024-09-10T00:00:00.000000Z"),
                        timestampType.getDriver().parseFloorLiteral("2024-09-10T13:00"),
                        WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                );
            }

            drainQueues();

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    """),
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
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
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
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-10-24T17:22:09.842574Z\t2024-10-24T17:22:09.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t1\t1
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );

            currentMicros = parseFloorPartialTimestamp("2024-10-24T18");
            execute("rename table base_price to base_price2");
            execute("refresh materialized view 'price_1h' full;");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\tinvalidation_reason\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-10-24T18:00:00.000000Z\t2024-10-24T18:00:00.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tinvalid\t[-105]: table does not exist [table=base_price]\t1\t-1
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, invalidation_reason, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );

            // Create another base table instead of the one that was renamed.
            // This table is non-WAL, so mat view should be still invalid.
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY BYPASS WAL"
            );
            currentMicros = parseFloorPartialTimestamp("2024-10-24T19");
            execute("refresh materialized view 'price_1h' full;");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\tinvalidation_reason\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-10-24T18:00:00.000000Z\t2024-10-24T19:00:00.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tinvalid\tbase table is not a WAL table\t1\t-1
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, invalidation_reason, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );
        });
    }

    @Test
    public void testBaseTableRename2() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
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
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\tinvalidation_reason\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-10-24T17:22:09.842574Z\t2024-10-24T17:22:09.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t\t1\t1
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, invalidation_reason, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );

            execute("rename table base_price to base_price2");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\tinvalidation_reason\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-10-24T17:22:09.842574Z\t2024-10-24T17:22:09.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tinvalid\tbase table is dropped or renamed\t1\t-1
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, invalidation_reason, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );
        });
    }

    @Test
    public void testBaseTableRenameAndThenRenameBack() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
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
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\tinvalidation_reason\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-10-24T19:00:00.000000Z\t2024-10-24T19:00:00.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tinvalid\ttable rename operation\t1\t3
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, invalidation_reason, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );
        });
    }

    @Test
    public void testBaseTableSwappedWithRename() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            executeWithRewriteTimestamp(
                    "create table base_price2 (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
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
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-10-24T17:22:09.842574Z\t2024-10-24T17:22:09.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t1\t1
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );

            // Swap the tables with each other.
            currentMicros = parseFloorPartialTimestamp("2024-10-24T18:00:00.000000Z");
            execute("rename table base_price to base_price_tmp");
            execute("rename table base_price2 to base_price");
            execute("rename table base_price_tmp to base_price2");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\tinvalidation_reason\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-10-24T18:00:00.000000Z\t2024-10-24T18:00:00.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tinvalid\tbase table is dropped or renamed\t1\t1
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, invalidation_reason, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );
        });
    }

    @Test
    public void testBaseTableTimestampTypeChangeThrowError() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp_ns" +
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
                    """
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """,
                    "select view_name, base_table_name, view_status from materialized_views",
                    null,
                    false
            );

            execute("drop table base_price;");
            drainQueues();
            assertQueryNoLeakCheck(
                    """
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            price_1h\tbase_price\tinvalid\tbase table is dropped or renamed
                            """,
                    "select view_name, base_table_name, view_status, invalidation_reason from materialized_views",
                    null,
                    false
            );

            // recreate the base table with a different timestamp type
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute(
                    "insert into base_price (sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );
            drainQueues();

            // revalidate the view
            execute("refresh materialized view price_1h full;)");
            drainQueues();
            assertQueryNoLeakCheck(
                    """
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            price_1h\tbase_price\tinvalid\t[-1]: timestamp type mismatch between materialized view and query [view=TIMESTAMP_NS, query=TIMESTAMP]
                            """,
                    "select view_name, base_table_name, view_status, invalidation_reason from materialized_views",
                    null,
                    false
            );
        });
    }

    @Test
    public void testBaseTableTruncateDoesNotInvalidateFreshMatView() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL;"
            );

            final String viewSql = "select sym, last(price) as price, ts from base_price sample by 1h";
            execute("create materialized view price_1h refresh immediate deferred as (" + viewSql + ") partition by DAY");

            execute("truncate table base_price;");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\tview_status
                            price_1h\tvalid
                            """,
                    "select view_name, view_status from materialized_views",
                    null,
                    false
            );
        });
    }

    @Test
    public void testBaseTableWalNotPurgedOnFullRefreshOfInvalidView() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 5);
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, amount int, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute(
                    "create materialized view price_1h refresh manual as " +
                            "select sym, last(price) as price, ts from base_price sample by 1h"
            );
            execute(
                    "insert into base_price (sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')"
            );

            currentMicros = parseFloorPartialTimestamp("2024-10-24T17:22:09.842574Z");
            drainWalAndMatViewQueues();

            for (int i = 0; i < 10; i++) {
                execute(
                        "insert into base_price (sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                                ",('gbpusd', 1.323, '2024-09-10T12:02')"
                );
            }

            // make the view invalid
            execute("rename table base_price to base_price2;");
            drainWalAndMatViewQueues();

            execute("rename table base_price2 to base_price;");
            drainWalQueue();

            final TableToken baseTableToken = engine.getTableTokenIfExists("base_price");
            Assert.assertNotNull(baseTableToken);
            final TableToken viewToken = engine.getTableTokenIfExists("price_1h");
            Assert.assertNotNull(viewToken);

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp("""
                            sym\tprice\tts
                            gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                            """),
                    "price_1h",
                    "ts",
                    true,
                    true
            );

            final MatViewState viewState = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertNotNull(viewState);
            Assert.assertTrue(viewState.isInvalid());

            // simulate a running full refresh by locking the state
            Assert.assertTrue(viewState.tryLock());
            try {
                engine.releaseInactiveTableSequencers();
                drainPurgeJob();

                // WAL segments should not be purged
                try (Path path = new Path()) {
                    path.of(configuration.getDbRoot()).concat(baseTableToken).concat(WalUtils.WAL_NAME_BASE).put(1).concat("1");
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));
                }
            } finally {
                viewState.unlock();
            }

            execute("refresh materialized view price_1h full;");
            drainWalAndMatViewQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """,
                    "select view_name, base_table_name, view_status from materialized_views",
                    null,
                    false
            );
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp("""
                            sym\tprice\tts
                            gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                            """),
                    "price_1h",
                    "ts",
                    true,
                    true
            );

            engine.releaseInactiveTableSequencers();
            drainPurgeJob();

            // WAL segments should now be purged
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(baseTableToken).concat(WalUtils.WAL_NAME_BASE).put(1).concat("1");
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
            }
        });
    }

    @Test
    public void testBaseTableWalNotPurgedOnInitialRefresh() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 5);
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, amount int, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute(
                    "create materialized view price_1h refresh manual deferred as " +
                            "select sym, last(price) as price, ts from base_price sample by 1h"
            );
            for (int i = 0; i < 10; i++) {
                execute(
                        "insert into base_price (sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                                ",('gbpusd', 1.323, '2024-09-10T12:02')"
                );
            }

            currentMicros = parseFloorPartialTimestamp("2024-10-24T17:22:09.842574Z");
            drainWalAndMatViewQueues();

            final TableToken baseTableToken = engine.getTableTokenIfExists("base_price");
            Assert.assertNotNull(baseTableToken);
            final TableToken viewToken = engine.getTableTokenIfExists("price_1h");
            Assert.assertNotNull(viewToken);

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n",
                    "price_1h",
                    "ts",
                    true,
                    true
            );

            final MatViewState viewState = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertNotNull(viewState);

            // simulate a running first incremental refresh by locking the state
            Assert.assertTrue(viewState.tryLock());
            try {
                engine.releaseInactiveTableSequencers();
                drainPurgeJob();

                // WAL segments should not be purged
                try (Path path = new Path()) {
                    path.of(configuration.getDbRoot()).concat(baseTableToken).concat(WalUtils.WAL_NAME_BASE).put(1).concat("1");
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));
                }
            } finally {
                viewState.unlock();
            }

            execute("refresh materialized view price_1h incremental;");
            drainWalAndMatViewQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """,
                    "select view_name, base_table_name, view_status from materialized_views",
                    null,
                    false
            );
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp("""
                            sym\tprice\tts
                            gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                            """),
                    "price_1h",
                    "ts",
                    true,
                    true
            );

            engine.releaseInactiveTableSequencers();
            drainPurgeJob();

            // WAL segments should now be purged
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(baseTableToken).concat(WalUtils.WAL_NAME_BASE).put(1).concat("1");
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
            }
        });
    }

    @Test
    public void testBaseTableWalPurgedDespiteInvalidMatViewState() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, amount int, ts #TIMESTAMP" +
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
            final TableToken baseTableToken = engine.getTableTokenIfExists("base_price");
            Assert.assertNotNull(baseTableToken);
            currentMicros = parseFloorPartialTimestamp("2024-10-24T17:22:09.842574Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """,
                    "select view_name, base_table_name, view_status from materialized_views",
                    null,
                    false
            );

            execute("alter table base_price drop column amount;");
            execute("insert into base_price (sym, price, ts) values('gbpusd', 1.330, '2024-09-15T12:01')");

            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            price_1h\tbase_price\tinvalid\tdrop column operation
                            """,
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
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by MONTH WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute("insert into base_price select concat('sym', x), x, timestamp_sequence('2022-02-24', 1000000*60*60*2) from long_sequence(30);");

            drainQueues();

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp("""
                            sym\tprice\tts
                            sym30\t30.0\t2022-02-23T12:00:00.000000Z
                            sym27\t27.0\t2022-02-23T13:00:00.000000Z
                            sym28\t28.0\t2022-02-23T13:00:00.000000Z
                            sym29\t29.0\t2022-02-23T13:00:00.000000Z
                            sym25\t25.0\t2022-02-23T14:00:00.000000Z
                            sym26\t26.0\t2022-02-23T14:00:00.000000Z
                            sym22\t22.0\t2022-02-23T15:00:00.000000Z
                            sym23\t23.0\t2022-02-23T15:00:00.000000Z
                            sym24\t24.0\t2022-02-23T15:00:00.000000Z
                            sym20\t20.0\t2022-02-23T16:00:00.000000Z
                            sym21\t21.0\t2022-02-23T16:00:00.000000Z
                            sym17\t17.0\t2022-02-23T17:00:00.000000Z
                            sym18\t18.0\t2022-02-23T17:00:00.000000Z
                            sym19\t19.0\t2022-02-23T17:00:00.000000Z
                            sym14\t14.0\t2022-02-23T18:00:00.000000Z
                            sym15\t15.0\t2022-02-23T18:00:00.000000Z
                            sym16\t16.0\t2022-02-23T18:00:00.000000Z
                            sym12\t12.0\t2022-02-23T19:00:00.000000Z
                            sym13\t13.0\t2022-02-23T19:00:00.000000Z
                            sym10\t10.0\t2022-02-23T20:00:00.000000Z
                            sym11\t11.0\t2022-02-23T20:00:00.000000Z
                            sym9\t9.0\t2022-02-23T20:00:00.000000Z
                            sym7\t7.0\t2022-02-23T21:00:00.000000Z
                            sym8\t8.0\t2022-02-23T21:00:00.000000Z
                            sym4\t4.0\t2022-02-23T22:00:00.000000Z
                            sym5\t5.0\t2022-02-23T22:00:00.000000Z
                            sym6\t6.0\t2022-02-23T22:00:00.000000Z
                            sym2\t2.0\t2022-02-23T23:00:00.000000Z
                            sym3\t3.0\t2022-02-23T23:00:00.000000Z
                            sym1\t1.0\t2022-02-24T00:00:00.000000Z
                            """),
                    "price_1h order by ts, sym",
                    "ts",
                    true,
                    true
            );

            // Expect 3 (30 rows / 10 rows per batch) commits.
            assertQueryNoLeakCheck(
                    """
                            writerTxn\tsequencerTxn
                            3\t3
                            """,
                    "select writerTxn, sequencerTxn from wal_tables() where name = 'price_1h'",
                    null,
                    false
            );
        });
    }

    @Test
    public void testCheckMatViewModification() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
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
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
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
                    replaceExpectedTimestamp("""
                            sym\tprice\tts
                            gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                            jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                            gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                            """),
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
                    replaceExpectedTimestamp("""
                            sym\tprice\tts
                            gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                            jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                            gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                            """),
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
            executeWithRewriteTimestamp(
                    "CREATE TABLE 'trades' (" +
                            "symbol SYMBOL CAPACITY 256 CACHE, " +
                            "side SYMBOL CAPACITY 256 CACHE, " +
                            "price DOUBLE, " +
                            "amount DOUBLE, " +
                            "timestamp #TIMESTAMP " +
                            ") timestamp(timestamp) PARTITION BY HOUR WAL;"
            );

            execute("INSERT INTO trades VALUES('BTC-USD', 'BUY', 29432.50, 0.5, '2023-08-15T09:30:45.789Z');");
            execute("INSERT INTO trades VALUES('BTC-USD', 'SELL', 29435.20, 1.2, '2023-08-15T09:31:12.345Z');");

            drainQueues();
            execute("""
                    CREATE MATERIALIZED VIEW a AS (
                      SELECT
                        timestamp,
                        symbol,
                        avg(price) AS avg_price
                      FROM trades
                      SAMPLE BY 1d
                    ) partition by HOUR;""");

            execute("""
                    create MATERIALIZED view b as (
                      SELECT
                        timestamp,
                        symbol,
                        avg(avg_price) AS avg_price
                      FROM a
                      SAMPLE BY 2d
                    ) partition by HOUR;""");

            execute("""
                    create MATERIALIZED view c as (
                      SELECT
                        timestamp,
                        symbol,
                        avg(avg_price) AS avg_price
                      FROM b
                      SAMPLE BY 2d
                    ) partition by HOUR;""");

            drainQueues();
            execute("drop MATERIALIZED VIEW a;");

            drainQueues();

            try {
                execute(
                        """
                                create MATERIALIZED view A as (
                                  SELECT
                                    timestamp,
                                    symbol,
                                    avg(avg_price) AS avg_price
                                  FROM c
                                  SAMPLE BY 2d
                                ) partition by HOUR;"""
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
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
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
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2023-01-01T01:01:01.123456Z\t2023-01-01T01:01:01.123456Z\tselect sym, last(price) as price, ts from base_price where ts in today() sample by 42h\tinvalid\t-1\t1
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );
        });
    }

    @Test
    public void testDedupKeysNoRowsRangeReplace() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL DEDUP UPSERT KEYS(ts);"
            );
            createMatView("select sym, last(price) as price, ts from base_price where sym <> 'gbpusd' sample by 1h");

            execute(
                    "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')"
            );
            currentMicros = parseFloorPartialTimestamp("2023-01-01T01:01:01.123456Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp("""
                            sym\tprice\tts
                            jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                            """),
                    "price_1h",
                    "ts",
                    true,
                    true
            );

            // Replace the jpyusd symbol value with gbpusd, so that we get a no data range replace commit.
            execute("insert into base_price values('gbpusd', 103.21, '2024-09-10T12:02')");
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n",
                    "price_1h",
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\tinvalidation_reason\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2023-01-01T01:01:01.123456Z\t2023-01-01T01:01:01.123456Z\tselect sym, last(price) as price, ts from base_price where sym <> 'gbpusd' sample by 1h\tvalid\t\t2\t2
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, invalidation_reason, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );
        });
    }

    @Test
    public void testDisableParallelSqlExecution() throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, "false");
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
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
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
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
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    """),
                    "price_1h order by ts, sym",
                    "ts",
                    true,
                    true
            );

            // mat view should be deleted
            execute("drop all;");

            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            count
                            0
                            """,
                    "select count() from materialized_views();",
                    null,
                    false,
                    true
            );
            assertQueryNoLeakCheck(
                    """
                            count
                            0
                            """,
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
        testEnableDedupWithSubsetKeys("alter table base_price dedup enable upsert keys(ts);");
    }

    @Test
    public void testEnableDedupWithMoreKeysInvalidatesMatViews() throws Exception {
        testEnableDedupWithSubsetKeys("alter table base_price dedup enable upsert keys(ts, amount);");
    }

    @Test
    public void testEnableDedupWithSameKeysDoesNotInvalidateMatViews() throws Exception {
        testEnableDedupWithSubsetKeys("alter table base_price dedup enable upsert keys(ts, sym);");
    }

    @Test
    public void testEstimateRowsPerBucket() {
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);

        // Basic case: 1 billion rows, hourly bucket, daily partitions, 30 partitions
        // Expected: (1B * 1hour) / (24hours * 30 partitions) = 1B / 720 ≈ 1,388,888
        testEstimateRowsPerBucket(1_000_000_000L, Micros.HOUR_MICROS, Micros.DAY_MICROS, 30, 1_000_000, 2_000_000);

        // Small table: 1000 rows
        testEstimateRowsPerBucket(1_000L, Micros.HOUR_MICROS, Micros.DAY_MICROS, 30, 1, 100);

        // Large table: 5 billion rows
        // Expected: (5B * 1hour) / (24hours * 30 partitions) ≈ 6,944,444
        testEstimateRowsPerBucket(5_000_000_000L, Micros.HOUR_MICROS, Micros.DAY_MICROS, 30, 5_000_000, 10_000_000);

        // Daily bucket
        // Expected: (1B * 24hours) / (24hours * 30 partitions) = 1B / 30 ≈ 33,333,333
        testEstimateRowsPerBucket(1_000_000_000L, Micros.DAY_MICROS, Micros.DAY_MICROS, 30, 30_000_000, 40_000_000);

        // Weekly bucket
        // Expected: (1B * 168hours) / (24hours * 30 partitions) = 1B * 7 / 30 ≈ 233,333,333
        testEstimateRowsPerBucket(1_000_000_000L, Micros.WEEK_MICROS, Micros.DAY_MICROS, 30, 200_000_000, 300_000_000);

        // Monthly bucket (30 days)
        // Expected: (1B * 720hours) / (24hours * 30 partitions) = 1B * 30 / 30 = 1B
        testEstimateRowsPerBucket(1_000_000_000L, Micros.MONTH_MICROS_APPROX, Micros.DAY_MICROS, 30, 900_000_000, 1_100_000_000);

        // Edge case: Weekly partitions with hourly bucket
        // Expected: (1B * 1hour) / (168hours * 4 partitions) ≈ 1,488,095
        testEstimateRowsPerBucket(1_000_000_000L, Micros.HOUR_MICROS, Micros.WEEK_MICROS, 4, 1_000_000, 2_000_000);

        // Edge case: Single partition
        // Expected: (1B * 1hour) / (720hours * 1 partition) ≈ 1,388,888
        testEstimateRowsPerBucket(1_000_000_000L, Micros.HOUR_MICROS, Micros.MONTH_MICROS_APPROX, 1, 1_000_000, 2_000_000);

        // Edge case: Many partitions (1000)
        // Expected: (1B * 1hour) / (24hours * 1000 partitions) ≈ 41,666
        testEstimateRowsPerBucket(1_000_000_000L, Micros.HOUR_MICROS, Micros.DAY_MICROS, 1000, 30_000, 50_000);

        // Edge case: Zero partition count (should return 1)
        final long result = MatViewRefreshJob.estimateRowsPerBucket(1_000_000_000L, Micros.HOUR_MICROS, Micros.HOUR_MICROS, 0);
        Assert.assertEquals("expected 1 for zero partitions", 1, result);

        // Overflow prevention test: Very large rows and bucket that would overflow with naive multiplication
        // Should not overflow, should return a reasonable positive value
        testEstimateRowsPerBucket(Long.MAX_VALUE / 2, Micros.HOUR_MICROS, Micros.DAY_MICROS, 1, 1, Long.MAX_VALUE / 2);

        // Test with nanoseconds (1000x microseconds)
        // Expected: same ratio as microseconds ≈ 1,388,888
        testEstimateRowsPerBucket(1_000_000_000L, Nanos.HOUR_NANOS, Nanos.DAY_NANOS, 30, 1_000_000, 2_000_000);

        // Very small bucket compared to partition
        // Expected: very small number, but at least 1
        testEstimateRowsPerBucket(1_000_000_000L, Micros.MILLI_MICROS, Micros.DAY_MICROS, 30, 1, 1000);

        // Bucket larger than partition duration (unusual but possible)
        // Expected: (1B * 168hours) / (24hours * 1) = 1B * 7 = 7B
        testEstimateRowsPerBucket(1_000_000_000L, Micros.WEEK_MICROS, Micros.DAY_MICROS, 1, 6_000_000_000L, 8_000_000_000L);
    }

    @Test
    public void testFullRefresh() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP, extra_col long" +
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

            final String matViewsSql = "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                    "view_sql, view_status, refresh_base_table_txn, base_table_txn " +
                    "from materialized_views";
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t1\t1
                            """,
                    matViewsSql,
                    null
            );

            final String expected = """
                    sym\tprice\tts
                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                    """;
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), "price_1h order by sym");

            execute("alter table base_price drop column extra_col");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tinvalid\t1\t2
                            """,
                    matViewsSql,
                    null
            );

            execute("refresh materialized view price_1h full");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t2\t2
                            """,
                    matViewsSql,
                    null
            );
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), "price_1h order by sym");
        });
    }

    @Test
    public void testFullRefreshDroppedBaseColumn() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP, extra_col long" +
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

            final String matViewsSql = "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                    "view_sql, view_status, refresh_base_table_txn, base_table_txn " +
                    "from materialized_views";
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t1\t1
                            """,
                    matViewsSql,
                    null
            );

            final String expected = """
                    sym\tprice\tts
                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                    """;
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), "price_1h order by sym");

            execute("alter table base_price drop column sym;");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tinvalid\t1\t2
                            """,
                    matViewsSql,
                    null
            );

            execute("refresh materialized view price_1h full;");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tinvalid\t-1\t2
                            """,
                    matViewsSql,
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
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            createMatView("select sym, last(price) as price, ts from base_price where npe() sample by 1h");

            execute("insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01');");
            currentMicros = parseFloorPartialTimestamp("2001-01-01T01:01:01.000000Z");
            drainQueues();

            // The view is expected to be invalid due to npe() in where clause.
            final String matViewsSql = "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                    "view_sql, view_status, refresh_base_table_txn, base_table_txn " +
                    "from materialized_views";
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2001-01-01T01:01:01.000000Z\t2001-01-01T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price where npe() sample by 1h\tinvalid\t-1\t1
                            """,
                    matViewsSql,
                    null
            );

            execute("refresh materialized view price_1h full");
            drainQueues();

            // The view is expected to be still invalid.
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2001-01-01T01:01:01.000000Z\t2001-01-01T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price where npe() sample by 1h\tinvalid\t-1\t1
                            """,
                    matViewsSql,
                    null
            );
        });
    }

    @Test
    public void testFullRefreshFail2() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute("insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01');");
            currentMicros = parseFloorPartialTimestamp("2001-01-01T01:01:01.000000Z");
            drainQueues();

            final String matViewsSql = "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                    "view_sql, view_status, refresh_base_table_txn, base_table_txn " +
                    "from materialized_views";
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2001-01-01T01:01:01.000000Z\t2001-01-01T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t1\t1
                            """,
                    matViewsSql,
                    null
            );

            execute("alter table base_price drop column price");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2001-01-01T01:01:01.000000Z\t2001-01-01T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tinvalid\t1\t2
                            """,
                    matViewsSql,
                    null
            );

            execute("refresh materialized view price_1h full");
            drainQueues();

            // The view is expected to be still invalid.
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2001-01-01T01:01:01.000000Z\t2001-01-01T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tinvalid\t-1\t2
                            """,
                    matViewsSql,
                    null
            );
        });
    }

    @Test
    public void testFullRefreshOfDroppedView() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");
            drainQueues();

            assertSql(
                    """
                            count
                            1
                            """,
                    "select count() from materialized_views"
            );

            execute("refresh materialized view price_1h full");
            execute("drop materialized view price_1h");

            drainQueues();

            assertSql(
                    """
                            count
                            0
                            """,
                    "select count() from materialized_views"
            );
        });
    }

    @Test
    public void testFullRefreshOfEmptyBaseTable() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute("refresh materialized view price_1h full");
            currentMicros = parseFloorPartialTimestamp("2024-12-31T01:00:00.000000Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-12-31T01:00:00.000000Z\t2024-12-31T01:00:00.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t0\t0
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );
        });
    }

    @Test
    public void testHugeSampleByInterval() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE Samples (" +
                            "  Time TIMESTAMP," +
                            "  DeviceId INT," +
                            "  Register SYMBOL INDEX," +
                            "  Value DOUBLE" +
                            ") timestamp(Time) PARTITION BY MONTH WAL " +
                            "DEDUP UPSERT KEYS(Time, DeviceId, Register);"
            );
            execute(
                    "CREATE MATERIALIZED VIEW Samples_latest AS" +
                            "  SELECT" +
                            "    Time as UnixEpoch," +
                            "    last(Time) AS Time," +
                            "    DeviceId," +
                            "    Register," +
                            "    last(Value) AS Value" +
                            "  FROM" +
                            "    Samples" +
                            "  SAMPLE BY 1000y;"
            );
            execute("INSERT INTO Samples (Time, DeviceId, Register, Value) VALUES ('2025-08-08T12:57:07.388314Z', 1, 'hello', 123);");

            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            UnixEpoch\tTime\tDeviceId\tRegister\tValue
                            1970-01-01T00:00:00.000000Z\t2025-08-08T12:57:07.388314Z\t1\thello\t123.0
                            """,
                    "Samples_latest",
                    "UnixEpoch",
                    true,
                    true
            );
        });
    }

    @Test
    public void testImmediatePeriodMatView() throws Exception {
        testPeriodRefresh("immediate", null, true);
    }

    @Test
    public void testImmediatePeriodMatViewFullRefreshNoTimerJob() throws Exception {
        testPeriodRefresh("immediate", "full", false);
    }

    @Test
    public void testImmediatePeriodMatViewFullRefreshRunTimerJob() throws Exception {
        testPeriodRefresh("immediate", "full", true);
    }

    @Test
    public void testImmediatePeriodMatViewIncrementalRefreshNoTimerJob() throws Exception {
        testPeriodRefresh("immediate", "incremental", false);
    }

    @Test
    public void testImmediatePeriodMatViewIncrementalRefreshRunTimerJob() throws Exception {
        testPeriodRefresh("immediate", "incremental", true);
    }

    @Test
    public void testImmediatePeriodMatViewWithTz() throws Exception {
        testPeriodWithTzRefresh("immediate", null, true);
    }

    @Test
    public void testImmediatePeriodWithTzMatViewFullRefreshNoTimerJob() throws Exception {
        testPeriodWithTzRefresh("immediate", "full", false);
    }

    @Test
    public void testImmediatePeriodWithTzMatViewFullRefreshRunTimerJob() throws Exception {
        testPeriodWithTzRefresh("immediate", "full", true);
    }

    @Test
    public void testImmediatePeriodWithTzMatViewIncrementalRefreshNoTimerJob() throws Exception {
        testPeriodWithTzRefresh("immediate", "incremental", false);
    }

    @Test
    public void testImmediatePeriodWithTzMatViewIncrementalRefreshRunTimerJob() throws Exception {
        testPeriodWithTzRefresh("immediate", "incremental", true);
    }

    @Test
    public void testIncrementalRefreshOnExistingTable() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 10);
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price ( " +
                            "sym varchar, price double, ts #TIMESTAMP " +
                            ") timestamp(ts) partition by DAY WAL"
            );

            // we want multiple WAL segments
            for (int i = 0; i < 100; i++) {
                execute(
                        "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                                ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                                ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                                ",('gbpusd', 1.321, '2024-09-10T13:02')" +
                                ",('jpyusd', 103.21, '2024-09-10T14:02')"
                );
            }
            drainWalQueue();
            drainPurgeJob();

            // Now, some WAL segments are already purged, so initial incremental refresh should not try
            // to read WAL transactions, but simply refresh everything in the [min_ts, max_ts] interval.
            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");
            currentMicros = parseFloorPartialTimestamp("2024-12-31T01:01:01.000000Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-12-31T01:01:01.000000Z\t2024-12-31T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t100\t100
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T14:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );
        });
    }

    @Test
    public void testIncrementalRefreshRecoversWhenWalSegmentIsGone() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 10);
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price ( " +
                            "sym varchar, price double, ts #TIMESTAMP " +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T14:02')"
            );
            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            currentMicros = parseFloorPartialTimestamp("2024-12-31T01:01:01.000000Z");
            drainQueues();

            // we want multiple WAL segments
            for (int i = 0; i < 10; i++) {
                execute(
                        "insert into base_price(sym, price, ts) values('gbpusd', 1.420, '2024-09-10T12:01')" +
                                ",('gbpusd', 1.423, '2024-09-10T12:02')" +
                                ",('jpyusd', 103.31, '2024-09-10T12:02')" +
                                ",('gbpusd', 1.521, '2024-09-10T13:02')" +
                                ",('jpyusd', 103.51, '2024-09-10T14:02')"
                );
            }
            drainWalQueue();

            // delete one of WAL segments
            final TableToken baseTableToken = engine.getTableTokenIfExists("base_price");
            Assert.assertNotNull(baseTableToken);
            try (Path path = new Path()) {
                path.of(engine.getConfiguration().getDbRoot()).concat(baseTableToken)
                        .concat(WAL_NAME_BASE).put(1).slash().put(5).concat(EVENT_FILE_NAME);
                engine.getConfiguration().getFilesFacade().removeQuiet(path.$());
            }

            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-12-31T01:01:01.000000Z\t2024-12-31T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t11\t11
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.423\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.521\t2024-09-10T13:00:00.000000Z
                                    jpyusd\t103.31\t2024-09-10T12:00:00.000000Z
                                    jpyusd\t103.51\t2024-09-10T14:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );
        });
    }

    @Test
    public void testIncrementalRefreshStatement() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price ( " +
                            "sym varchar, price double, ts #TIMESTAMP " +
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
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t1\t1
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );
        });
    }

    @Test
    public void testIncrementalRefreshStatementOnTimerMatView() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute(
                    "create materialized view price_1h refresh every 1h deferred start '2199-12-12T12:00:00.000000Z' as (" +
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
            final String matViewsSql = "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                    "view_sql, view_status, refresh_base_table_txn, base_table_txn " +
                    "from materialized_views";
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\ttimer\tbase_price\t\t\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t-1\t1
                            """,
                    matViewsSql,
                    null
            );

            // the view should refresh after an explicit incremental refresh call
            execute("refresh materialized view price_1h incremental;");
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\ttimer\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t1\t1
                            """,
                    matViewsSql,
                    null
            );
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );
        });
    }

    @Test
    public void testIncrementalRefreshTransactionLogV2() throws Exception {
        testIncrementalRefreshTransactionLogV2("select sym, last(price) as price, ts from base_price sample by 1h");
    }

    @Test
    public void testIncrementalRefreshTransactionLogV2WithViewWhereClauseSymbolFilters() throws Exception {
        testIncrementalRefreshTransactionLogV2(
                "select sym, last(price) as price, ts from base_price " +
                        "WHERE sym = 'gbpusd' or sym = 'jpyusd' " +
                        "sample by 1h"
        );
    }

    @Test
    public void testIncrementalRefreshTransactionLogV2WithViewWhereClauseTimestampFilters() throws Exception {
        testIncrementalRefreshTransactionLogV2(
                "select sym, last(price) price, ts from base_price " +
                        "WHERE ts > 0 or ts < '2040-01-01' " +
                        "sample by 1h"
        );
    }

    @Test
    public void testIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym symbol, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            execute("create materialized view price_1h as (select sym, last(price) as price, ts from base_price sample by 1h) partition by DAY");

            execute(
                    "insert into base_price values('gbpusd', 1.310, '2024-09-10T12:05')" +
                            ",('gbpusd', 1.311, '2024-09-11T13:03')" +
                            ",('eurusd', 1.312, '2024-09-12T13:03')" +
                            ",('gbpusd', 1.313, '2024-09-13T13:03')" +
                            ",('eurusd', 1.314, '2024-09-14T13:03')"
            );

            drainQueues();

            execute("alter materialized view price_1h alter column sym add index");

            drainQueues();

            // index already exists - exception
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column sym add index",
                    46,
                    "column 'sym' already indexed"
            );

            String sql = "select * from price_1h where sym = 'eurusd';";
            assertQueryNoLeakCheck(
                    """
                            sym\tprice\tts
                            eurusd\t1.312\t2024-09-12T13:00:00.000000Z
                            eurusd\t1.314\t2024-09-14T13:00:00.000000Z
                            """,
                    sql,
                    "ts",
                    true,
                    false
            );

            assertSql("""
                            indexBlockCapacity
                            2
                            """,
                    "select indexBlockCapacity from (show columns from price_1h) where column = 'sym'"
            );

            assertSql(
                    """
                            QUERY PLAN
                            DeferredSingleSymbolFilterPageFrame
                                Index forward scan on: sym
                                  filter: sym=2
                                Frame forward scan on: price_1h
                            """,
                    "explain " + sql
            );

            execute("alter materialized view price_1h alter column sym drop index");

            drainQueues();

            // not indexed
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column sym drop index",
                    46,
                    "column 'sym' is not indexed"
            );

            drainQueues();

            if (JitUtil.isJitSupported()) {
                assertSql(
                        """
                                QUERY PLAN
                                Async JIT Filter workers: 1
                                  filter: sym='eurusd'
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: price_1h
                                """,
                        "explain " + sql
                );
            } else {
                assertSql(
                        """
                                QUERY PLAN
                                Async Filter workers: 1
                                  filter: sym='eurusd'
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: price_1h
                                """,
                        "explain " + sql
                );
            }

            execute("alter materialized view price_1h alter column sym add index");

            drainQueues();

            assertSql(
                    """
                            QUERY PLAN
                            DeferredSingleSymbolFilterPageFrame
                                Index forward scan on: sym
                                  filter: sym=2
                                Frame forward scan on: price_1h
                            """,
                    "explain " + sql
            );
        });
    }

    @Test
    public void testIndexEmptyMV() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_INDEX_VALUE_BLOCK_SIZE, 312);

            execute(
                    "create table base_price (" +
                            "sym symbol, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            execute("create materialized view price_1h as (select sym, last(price) as price, ts from base_price sample by 1h) partition by DAY");


            drainQueues();

            execute("alter materialized view price_1h alter column sym add index");

            drainQueues();

            // index already exists - exception
            assertExceptionNoLeakCheck(
                    "alter materialized view price_1h alter column sym add index",
                    46,
                    "column 'sym' already indexed"
            );

            execute(
                    "insert into base_price values('gbpusd', 1.310, '2024-09-10T12:05')" +
                            ",('gbpusd', 1.311, '2024-09-11T13:03')" +
                            ",('eurusd', 1.312, '2024-09-12T13:03')" +
                            ",('gbpusd', 1.313, '2024-09-13T13:03')" +
                            ",('eurusd', 1.314, '2024-09-14T13:03')"
            );

            drainQueues();

            String sql = "select * from price_1h where sym = 'eurusd';";
            assertQueryNoLeakCheck(
                    """
                            sym\tprice\tts
                            eurusd\t1.312\t2024-09-12T13:00:00.000000Z
                            eurusd\t1.314\t2024-09-14T13:00:00.000000Z
                            """,
                    sql,
                    "ts",
                    true,
                    false
            );

            assertSql(
                    """
                            QUERY PLAN
                            DeferredSingleSymbolFilterPageFrame
                                Index forward scan on: sym
                                  filter: sym=2
                                Frame forward scan on: price_1h
                            """,
                    "explain " + sql
            );

            assertSql("indexBlockCapacity\n" +
                            Numbers.ceilPow2(configuration.getIndexValueBlockSize()) + "\n",
                    "select indexBlockCapacity from (show columns from price_1h) where column = 'sym'"
            );
        });
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

            final long startTs = timestampType.getDriver().parseFloorLiteral("2021-03-27T23:30:00.000000Z");
            final long step = timestampType.getDriver().fromMicros(100000000L);
            final int N = 100;
            final int K = 5;
            final String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b',null) s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            final String expected = """
                    k\ts\tlat\tlon
                    2021-03-27T23:00:00.000000Z\ta\t142.30215575416736\t165.69007104574442
                    2021-03-28T00:00:00.000000Z\ta\t106.0418967098362\tnull
                    2021-03-28T01:00:00.000000Z\ta\t79.9245166429184\t168.04971262491318
                    2021-03-28T02:00:00.000000Z\ta\t6.612327943200507\t128.42101395467057
                    """;

            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewQuery), "k", true, true);
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewName), "k", true, true);
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

            long startTs = timestampType.getDriver().parseFloorLiteral("2021-03-28T00:59:00.000000Z");
            long step = timestampType.getDriver().fromMicros(60 * 1000000L);
            final int N = 100;
            final int K = 5;
            final String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a') s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            final String expected = """
                    k\ts\tlat\tlon
                    2021-03-28T01:00:00.000000Z\ta\t144.77803379943109\t15.276535618609202
                    2021-03-28T03:00:00.000000Z\ta\tnull\t127.43011035722469
                    2021-03-28T04:00:00.000000Z\ta\t60.30746433578906\t128.42101395467057
                    """;

            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewQuery), null, true, true);
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewName), null, true, true);
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

            final long startTs = timestampType.getDriver().parseFloorLiteral("2021-03-28T01:00:00.000000Z");
            final long step = timestampType.getDriver().fromMicros(60 * 1000000L);
            final int N = 100;
            final int K = 5;
            final String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a') s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            final String expected = """
                    k\ts\tlat\tlon
                    2021-03-28T03:00:00.000000Z\ta\t144.77803379943109\tnull
                    2021-03-28T04:00:00.000000Z\ta\t98.27279585461298\t128.42101395467057
                    """;

            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewQuery), null, true, true);
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewName), null, true, true);
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

            final long startTs = timestampType.getDriver().parseFloorLiteral("2021-03-28T01:59:00.000000Z");
            final long step = timestampType.getDriver().fromMicros(60 * 1000000L);
            final int N = 100;
            final int K = 5;
            final String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a') s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            final String expected = """
                    k\ts\tlat\tlon
                    2021-03-28T03:00:00.000000Z\ta\t144.77803379943109\t15.276535618609202
                    2021-03-28T04:00:00.000000Z\ta\tnull\t127.43011035722469
                    2021-03-28T05:00:00.000000Z\ta\t60.30746433578906\t128.42101395467057
                    """;

            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewQuery), null, true, true);
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewName), null, true, true);
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

            final long startTs = timestampType.getDriver().parseFloorLiteral("2021-03-27T23:01:00.000000Z");
            final long step = timestampType.getDriver().fromMicros(60 * 1000000L);
            final int N = 100;
            final int K = 5;
            final String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b',null) s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            final String expected = """
                    k\ts\tlat\tlon
                    2021-03-28T00:00:00.000000Z\ta\t142.30215575416736\t167.4566019970139
                    2021-03-28T01:00:00.000000Z\ta\t33.45558404694713\t128.42101395467057
                    """;
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewQuery), null, true, true);
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewName), null, true, true);
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

            final long startTs = timestampType.getDriver().parseFloorLiteral("2020-10-23T20:30:00.000000Z");
            final long step = timestampType.getDriver().fromMicros(50 * 60 * 1000000L);
            final int N = 120;
            final int K = 5;
            final String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b',null) s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            final String expected = """
                    to_timezone\tk\ts\tlat\tlon
                    2020-10-24T00:00:00.000000Z\t2020-10-23T22:00:00.000000Z\ta\t142.30215575416736\t2020-10-24T19:50:00.000000Z
                    2020-10-25T00:00:00.000000Z\t2020-10-24T22:00:00.000000Z\ta\tnull\t2020-10-25T20:00:00.000000Z
                    2020-10-26T00:00:00.000000Z\t2020-10-25T23:00:00.000000Z\ta\t33.45558404694713\t2020-10-26T21:50:00.000000Z
                    2020-10-27T00:00:00.000000Z\t2020-10-26T23:00:00.000000Z\ta\t6.612327943200507\t2020-10-27T22:00:00.000000Z
                    2020-10-28T00:00:00.000000Z\t2020-10-27T23:00:00.000000Z\ta\tnull\t2020-10-27T23:40:00.000000Z
                    """;

            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewQuery), "k", true, true);
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewName), "k", true, true);
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

            final long startTs = timestampType.getDriver().parseFloorLiteral("2021-03-26T20:30:00.000000Z");
            final long step = timestampType.getDriver().fromMicros(13 * 60 * 1000000L);
            final int N = 1000;
            final int K = 25;
            final String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b') s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            final String expected = """
                    to_timezone\tk\ts\tlat\tlon
                    2021-03-27T21:15:00.000000Z\t2021-03-27T20:15:00.000000Z\ta\t132.09083798490755\t2021-03-27T21:12:00.000000Z
                    2021-03-27T22:15:00.000000Z\t2021-03-27T21:15:00.000000Z\ta\t179.5841357536068\t2021-03-27T21:51:00.000000Z
                    2021-03-27T23:15:00.000000Z\t2021-03-27T22:15:00.000000Z\ta\t77.68770182183965\t2021-03-27T22:56:00.000000Z
                    2021-03-28T00:15:00.000000Z\t2021-03-27T23:15:00.000000Z\ta\tnull\t2021-03-27T23:48:00.000000Z
                    2021-03-28T01:15:00.000000Z\t2021-03-28T00:15:00.000000Z\ta\t3.6703591550328163\t2021-03-28T01:06:00.000000Z
                    2021-03-28T03:15:00.000000Z\t2021-03-28T01:15:00.000000Z\ta\tnull\t2021-03-28T02:11:00.000000Z
                    2021-03-28T04:15:00.000000Z\t2021-03-28T02:15:00.000000Z\ta\tnull\t2021-03-28T02:37:00.000000Z
                    2021-03-28T05:15:00.000000Z\t2021-03-28T03:15:00.000000Z\ta\t38.20430552091481\t2021-03-28T03:16:00.000000Z
                    """;
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewQuery), "k", true, true);
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewName), "k", true, true);
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

            final long startTs = timestampType.getDriver().parseFloorLiteral("2021-03-25T23:30:00.000000Z");
            final long step = timestampType.getDriver().fromMicros(50 * 60 * 1000000L);
            final int N = 120;
            final int K = 5;
            final String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b',null) s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);
            final String expected = """
                    to_timezone\tk\ts\tlat\tlon
                    2021-03-26T00:00:00.000000Z\t2021-03-25T23:00:00.000000Z\ta\t142.30215575416736\t2021-03-26T22:50:00.000000Z
                    2021-03-27T00:00:00.000000Z\t2021-03-26T23:00:00.000000Z\ta\tnull\t2021-03-27T22:10:00.000000Z
                    2021-03-28T00:00:00.000000Z\t2021-03-27T23:00:00.000000Z\ta\t109.94209864193589\t2021-03-28T20:40:00.000000Z
                    2021-03-29T00:00:00.000000Z\t2021-03-28T22:00:00.000000Z\ta\t70.00560222114518\t2021-03-29T16:40:00.000000Z
                    2021-03-30T00:00:00.000000Z\t2021-03-29T22:00:00.000000Z\ta\t13.290235514836048\t2021-03-30T02:40:00.000000Z
                    """;

            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewQuery), "k", true, true);
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewName), "k", true, true);
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

            final long startTs = timestampType.getDriver().parseFloorLiteral("2021-03-25T23:30:00.000000Z");
            final long step = timestampType.getDriver().fromMicros(50 * 60 * 1000000L);
            final int N = 120;
            final int K = 5;
            final String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b',null) s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            final String expected = """
                    to_timezone\tk\ts\tlat\tlon
                    2021-03-26T00:00:00.000000Z\t2021-03-26T00:00:00.000000Z\ta\t142.30215575416736\t2021-03-26T22:50:00.000000Z
                    2021-03-27T00:00:00.000000Z\t2021-03-27T00:00:00.000000Z\ta\tnull\t2021-03-27T23:00:00.000000Z
                    2021-03-28T00:00:00.000000Z\t2021-03-28T00:00:00.000000Z\ta\t33.45558404694713\t2021-03-28T20:40:00.000000Z
                    2021-03-29T00:00:00.000000Z\t2021-03-28T23:00:00.000000Z\ta\t70.00560222114518\t2021-03-29T16:40:00.000000Z
                    2021-03-30T00:00:00.000000Z\t2021-03-29T23:00:00.000000Z\ta\t13.290235514836048\t2021-03-30T02:40:00.000000Z
                    """;

            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewQuery), "k", true, true);
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewName), "k", true, true);
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

            final long startTs = timestampType.getDriver().parseFloorLiteral("2020-10-23T20:30:00.000000Z");
            final long step = timestampType.getDriver().fromMicros(259 * 1000000L);
            final int N = 1000;
            final int K = 25;
            final String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b') s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            final String expected = """
                    to_timezone\tk\ts\tlat\tlastk
                    2020-10-24T22:00:00.000000Z\t2020-10-24T21:00:00.000000Z\ta\t154.93777586404912\t2020-10-24T21:49:28.000000Z
                    2020-10-24T23:00:00.000000Z\t2020-10-24T22:00:00.000000Z\ta\t43.799859246867385\t2020-10-24T22:54:13.000000Z
                    2020-10-25T00:00:00.000000Z\t2020-10-24T23:00:00.000000Z\ta\t38.34194069380561\t2020-10-24T23:41:42.000000Z
                    2020-10-25T01:00:00.000000Z\t2020-10-25T00:00:00.000000Z\ta\t4.158342987512034\t2020-10-25T01:51:12.000000Z
                    2020-10-25T02:00:00.000000Z\t2020-10-25T02:00:00.000000Z\ta\t95.73868763606973\t2020-10-25T02:47:19.000000Z
                    2020-10-25T03:00:00.000000Z\t2020-10-25T03:00:00.000000Z\ta\tnull\t2020-10-25T03:43:26.000000Z
                    2020-10-25T04:00:00.000000Z\t2020-10-25T04:00:00.000000Z\ta\t34.49948946607576\t2020-10-25T04:56:49.000000Z
                    """;

            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewQuery), "k", true, true);
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewName), "k", true, true);
        });
    }

    @Test
    public void testInsertAfterTruncate() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
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

            final String expected1 = """
                    sym\tprice\tts
                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                    """;
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected1), "price_1h order by sym");
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected1), view1Sql + " order by sym");

            final String expected2 = """
                    sym\tprice\tts
                    gbpusd\t1.321\t2024-09-10T00:00:00.000000Z
                    jpyusd\t103.21\t2024-09-10T00:00:00.000000Z
                    """;
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected2), "price_1d order by sym");
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected2), view2Sql + " order by sym");
        });
    }

    @Test
    public void testLargeSampleByInterval() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE Samples (" +
                            "  Time TIMESTAMP," +
                            "  DeviceId INT," +
                            "  Register SYMBOL INDEX," +
                            "  Value DOUBLE" +
                            ") timestamp(Time) PARTITION BY MONTH WAL " +
                            "DEDUP UPSERT KEYS(Time, DeviceId, Register);"
            );
            execute(
                    "CREATE MATERIALIZED VIEW Samples_latest AS" +
                            "  SELECT" +
                            "    Time as UnixEpoch," +
                            "    last(Time) AS Time," +
                            "    DeviceId," +
                            "    Register," +
                            "    last(Value) AS Value" +
                            "  FROM" +
                            "    Samples" +
                            "  SAMPLE BY 2y;"
            );
            execute("INSERT INTO Samples (Time, DeviceId, Register, Value) VALUES ('2025-08-08T12:57:07.388314Z', 1, 'hello', 123);");

            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            UnixEpoch\tTime\tDeviceId\tRegister\tValue
                            2024-01-01T00:00:00.000000Z\t2025-08-08T12:57:07.388314Z\t1\thello\t123.0
                            """,
                    "Samples_latest",
                    "UnixEpoch",
                    true,
                    true
            );
        });
    }

    @Test
    public void testLargeSampleByIntervalButFewRows() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table x (ts #TIMESTAMP) timestamp(ts) partition by day wal;");
            execute("insert into x values('2000-01-01T12:00'),('2010-01-01T12:00'),('2020-01-01T12:00')");
            drainQueues();

            final String expected = """
                    ts\tfirst_ts
                    2000-01-01T00:00:00.000000Z\t2000-01-01T12:00:00.000000Z
                    2010-01-01T00:00:00.000000Z\t2010-01-01T12:00:00.000000Z
                    2020-01-01T00:00:00.000000Z\t2020-01-01T12:00:00.000000Z
                    """;
            final String viewSql = "select ts, first(ts) as first_ts from x sample by 10y";
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), viewSql, "ts", true, true);

            execute("create materialized view x_1y as (" + viewSql + ") partition by year");
            drainQueues();

            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), "x_1y", "ts", true, true);
        });
    }

    @Test
    public void testManualDeferredMatView() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            currentMicros = parseFloorPartialTimestamp("2001-01-01T01:01:01.000000Z");
            execute(
                    "create materialized view price_1h refresh manual deferred as " +
                            "select sym, last(price) as price, ts from base_price sample by 1h;"
            );

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = parseFloorPartialTimestamp("2099-01-01T01:01:01.000000Z");
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n",
                    "price_1h order by sym"
            );
            final String matViewsSql = "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                    "view_sql, view_status, refresh_base_table_txn, base_table_txn " +
                    "from materialized_views";
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\tmanual\tbase_price\t\t\tselect sym, last(price) as price, ts from base_price sample by 1h;\tvalid\t-1\t1
                            """,
                    matViewsSql,
                    null
            );

            // the view should refresh after an explicit incremental refresh call
            execute("refresh materialized view price_1h incremental;");
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\tmanual\tbase_price\t2099-01-01T01:01:01.000000Z\t2099-01-01T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h;\tvalid\t1\t1
                            """,
                    matViewsSql,
                    null
            );
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );

            execute("insert into base_price(sym, price, ts) values('gbpusd', 1.333, '2024-09-10T22:01')");

            // full refresh should also work
            currentMicros = parseFloorPartialTimestamp("2100-01-01T01:01:01.000000Z");
            execute("refresh materialized view price_1h full;");
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\tmanual\tbase_price\t2100-01-01T01:01:01.000000Z\t2100-01-01T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h;\tvalid\t2\t2
                            """,
                    matViewsSql,
                    null
            );
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    gbpusd\t1.333\t2024-09-10T22:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );
        });
    }

    @Test
    public void testManualMatView() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            currentMicros = parseFloorPartialTimestamp("2001-01-01T01:01:01.000000Z");
            execute(
                    "create materialized view price_1h refresh manual as " +
                            "select sym, last(price) as price, ts from base_price sample by 1h;"
            );

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = parseFloorPartialTimestamp("2099-01-01T01:01:01.000000Z");
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );
            final String matViewsSql = "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                    "view_sql, view_status, refresh_base_table_txn, base_table_txn " +
                    "from materialized_views";
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\tmanual\tbase_price\t2099-01-01T01:01:01.000000Z\t2099-01-01T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h;\tvalid\t1\t1
                            """,
                    matViewsSql,
                    null
            );

            // insert a few more rows - they won't be reflected in the view until we refresh it explicitly
            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-11T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-11T12:02')"
            );
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );

            // the view should refresh after an explicit incremental refresh call
            execute("refresh materialized view price_1h incremental;");
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\tmanual\tbase_price\t2099-01-01T01:01:01.000000Z\t2099-01-01T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h;\tvalid\t2\t2
                            """,
                    matViewsSql,
                    null
            );
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    gbpusd\t1.323\t2024-09-11T12:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );

            execute("insert into base_price(sym, price, ts) values('gbpusd', 1.333, '2024-09-10T22:01')");

            // full refresh should also work
            currentMicros = parseFloorPartialTimestamp("2100-01-01T01:01:01.000000Z");
            execute("refresh materialized view price_1h full;");
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\tmanual\tbase_price\t2100-01-01T01:01:01.000000Z\t2100-01-01T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h;\tvalid\t3\t3
                            """,
                    matViewsSql,
                    null
            );
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    gbpusd\t1.333\t2024-09-10T22:00:00.000000Z
                                    gbpusd\t1.323\t2024-09-11T12:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );
        });
    }

    @Test
    public void testManualPeriodMatViewFullRefreshNoTimerJob() throws Exception {
        testPeriodRefresh("manual deferred", "full", false);
    }

    @Test
    public void testManualPeriodMatViewFullRefreshRunTimerJob() throws Exception {
        testPeriodRefresh("manual deferred", "full", true);
    }

    @Test
    public void testManualPeriodMatViewIncrementalRefreshNoTimerJob() throws Exception {
        testPeriodRefresh("manual deferred", "incremental", false);
    }

    @Test
    public void testManualPeriodMatViewIncrementalRefreshRunTimerJob() throws Exception {
        testPeriodRefresh("manual deferred", "incremental", true);
    }

    @Test
    public void testManualPeriodWithTzIncrementalRefreshRunTimerJob() throws Exception {
        testPeriodWithTzRefresh("manual deferred", "incremental", true);
    }

    @Test
    public void testManualPeriodWithTzMatViewFullRefreshNoTimerJob() throws Exception {
        testPeriodWithTzRefresh("manual deferred", "full", false);
    }

    @Test
    public void testManualPeriodWithTzMatViewFullRefreshRunTimerJob() throws Exception {
        testPeriodWithTzRefresh("manual deferred", "full", true);
    }

    @Test
    public void testManualPeriodWithTzMatViewIncrementalRefreshNoTimerJob() throws Exception {
        testPeriodWithTzRefresh("manual deferred", "incremental", false);
    }

    @Test
    public void testManyTimerMatViews() throws Exception {
        assertMemoryLeak(() -> {
            final int views = 32;
            final long start = MicrosTimestampDriver.INSTANCE.parseFloorLiteral("2024-12-12T00:00:00.000000Z");

            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            final long viewStart = timestampType.getDriver().fromMicros(start);
            // Create first and last mat view before all others to verify timer list sort logic.
            createNthTimerMatView(timestampType.getDriver(), viewStart, 0);
            createNthTimerMatView(timestampType.getDriver(), viewStart, (views - 1));
            for (int i = 1; i < views - 1; i++) {
                createNthTimerMatView(timestampType.getDriver(), viewStart, i);
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
                        replaceExpectedTimestamp(
                                """
                                        sym\tprice\tts
                                        gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                        gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                        jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                        """),
                        "price_1h_" + i + " order by sym"
                );

                currentMicros += Micros.SECOND_MICROS;
            }

            // Drop all mat views. All timers should be removed as well.
            dropNthTimerMatView(views - 1);
            dropNthTimerMatView(0);
            for (int i = 1; i < views - 1; i++) {
                dropNthTimerMatView(i);
            }

            currentMicros += Micros.DAY_MICROS;
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            count
                            0
                            """,
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
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute(
                    "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = MicrosTimestampDriver.floor("2024-10-24T17:22:09.842574Z");
            drainQueues();

            final String matViewsSql = "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                    "view_sql, view_status, invalidation_reason, refresh_base_table_txn, base_table_txn " +
                    "from materialized_views";
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\tinvalidation_reason\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-10-24T17:22:09.842574Z\t2024-10-24T17:22:09.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t\t1\t1
                            """,
                    matViewsSql,
                    null
            );

            TableToken matViewToken = engine.verifyTableName("price_1h");
            TableToken updatedToken = matViewToken.renamed("price_1h_renamed");

            engine.applyTableRename(matViewToken, updatedToken);

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\tinvalidation_reason\trefresh_base_table_txn\tbase_table_txn
                            price_1h_renamed\timmediate\tbase_price\t2024-10-24T17:22:09.842574Z\t2024-10-24T17:22:09.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t\t1\t1
                            """,
                    matViewsSql,
                    null
            );

            execute(
                    "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1, '2024-09-10T12:02')" +
                            ",('jpyusd', 1, '2024-09-10T12:02')" +
                            ",('gbpusd', 1, '2024-09-10T13:02')"
            );
            drainQueues();

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp("""
                            sym\tprice\tts
                            gbpusd\t1.0\t2024-09-10T12:00:00.000000Z
                            jpyusd\t1.0\t2024-09-10T12:00:00.000000Z
                            gbpusd\t1.0\t2024-09-10T13:00:00.000000Z
                            """),
                    "price_1h_renamed",
                    "ts",
                    true,
                    true

            );
        });
    }

    @Test
    public void testMinRefreshInterval() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "  sym symbol, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute(
                    "insert into base_price(sym, price, ts) values ('gbpusd', 1.320, '2023-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2023-09-10T12:01')" +
                            ",('jpyusd', 103.21, '2023-09-10T12:01')" +
                            ",('jpyusd', 103.22, '2023-09-10T12:01')"
            );
            drainWalQueue();

            execute(
                    "create materialized view price_1h as " +
                            "select sym, last(price) as price, ts from base_price sample by 1s;"
            );

            currentMicros = parseFloorPartialTimestamp("2024-01-01T01:01:01.000000Z");
            drainQueues();

            execute(
                    "insert into base_price(sym, price, ts) values ('gbpusd', 1.320, '2023-09-10T12:00')" +
                            ",('jpyusd', 103.21, '2023-09-10T12:00')"
            );
            drainQueues();

            // We've only inserted '2023-09-10T12:00' timestamps, so only this second should be refreshed
            // and inserted in the last transaction.
            final TableToken viewToken = engine.getTableTokenIfExists("price_1h");
            Assert.assertNotNull(viewToken);
            try (
                    Path path = new Path();
                    TableReader viewReader = engine.getReader(viewToken);
                    WalTxnRangeLoader txnRangeLoader = new WalTxnRangeLoader(configuration)
            ) {
                final LongList intervals = new LongList();
                final long seqTxn = viewReader.getSeqTxn();
                txnRangeLoader.load(engine, path, viewToken, intervals, seqTxn - 1, seqTxn);
                Assert.assertEquals(2, intervals.size());
                Assert.assertEquals(parseFloorPartialTimestamp("2023-09-10T12:00:00.000000Z"), intervals.getQuick(0));
                Assert.assertEquals(parseFloorPartialTimestamp("2023-09-10T12:00:00.999999Z"), intervals.getQuick(1));
            }

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status\trefresh_period_hi\trefresh_base_table_txn\tbase_table_txn\tperiod_length\tperiod_length_unit\trefresh_limit\trefresh_limit_unit
                            price_1h\timmediate\tbase_price\t2024-01-01T01:01:01.000000Z\t2024-01-01T01:01:01.000000Z\tvalid\t\t2\t2\t0\t\t0\t
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_status, refresh_period_hi, refresh_base_table_txn, base_table_txn, " +
                            "period_length, period_length_unit, refresh_limit, refresh_limit_unit " +
                            "from materialized_views",
                    null
            );
            assertQueryNoLeakCheck(
                    """
                            sym\tprice\tts
                            gbpusd\t1.32\t2023-09-10T12:00:00.000000Z
                            gbpusd\t1.323\t2023-09-10T12:01:00.000000Z
                            jpyusd\t103.21\t2023-09-10T12:00:00.000000Z
                            jpyusd\t103.22\t2023-09-10T12:01:00.000000Z
                            """,
                    "price_1h order by sym, ts"
            );
        });
    }

    @Test
    public void testPeriodMatViewSmoke() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            currentMicros = parseFloorPartialTimestamp("2000-01-01T00:00:00.000000Z");
            execute(
                    "create materialized view price_1h refresh immediate period (length 4h) as " +
                            "select sym, last(price) as price, ts from base_price sample by 1h"
            );
            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2000-01-01T02:01')" +
                            ",('gbpusd', 1.323, '2000-01-01T04:02')" +
                            ",('jpyusd', 103.21, '2000-01-01T02:02')" +
                            ",('gbpusd', 1.321, '2000-01-01T04:02')" +
                            ",('jpyusd', 103.21, '2000-01-02T01:00')" +
                            ",('gbpusd', 1.321, '2000-01-02T01:00')"
            );

            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);

            // no refresh should happen as the first period hasn't finished
            currentMicros = parseFloorPartialTimestamp("2000-01-01T00:00:00.000000Z");
            drainMatViewTimerQueue(timerJob);
            drainQueues();
            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n",
                    "price_1h order by sym"
            );
            final String matViewsSql = "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                    "view_sql, view_status, refresh_base_table_txn, base_table_txn, refresh_period_hi, timer_time_zone, timer_start, " +
                    "timer_interval, timer_interval_unit, period_length, period_length_unit, period_delay, period_delay_unit " +
                    "from materialized_views";
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_period_hi\ttimer_time_zone\ttimer_start\ttimer_interval\ttimer_interval_unit\tperiod_length\tperiod_length_unit\tperiod_delay\tperiod_delay_unit
                            price_1h\timmediate\tbase_price\t2000-01-01T00:00:00.000000Z\t2000-01-01T00:00:00.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t1\t1\t\t\t2000-01-01T00:00:00.000000Z\t0\t\t4\tHOUR\t0\t
                            """,
                    matViewsSql,
                    null
            );

            // the first period still hasn't finished
            currentMicros = parseFloorPartialTimestamp("2000-01-01T03:59:59.999999Z");
            drainMatViewTimerQueue(timerJob);
            drainQueues();
            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n",
                    "price_1h order by sym"
            );

            // the first period has finished
            currentMicros = parseFloorPartialTimestamp("2000-01-01T04:00:00.000000Z");
            drainMatViewTimerQueue(timerJob);
            drainQueues();
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.32\t2000-01-01T02:00:00.000000Z
                                    jpyusd\t103.21\t2000-01-01T02:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );

            // let's insert some rows for the first period as well as for the incomplete one
            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.323, '2000-01-01T03:01')" +
                            ",('jpyusd', 103.29, '2000-01-01T03:02')" +
                            ",('gbpusd', 1.322, '2000-01-01T05:00')" +
                            ",('jpyusd', 103.23, '2000-01-02T05:01')"
            );

            // only the rows for the first period should be reflected in the view
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.32\t2000-01-01T02:00:00.000000Z
                                    gbpusd\t1.323\t2000-01-01T03:00:00.000000Z
                                    jpyusd\t103.21\t2000-01-01T02:00:00.000000Z
                                    jpyusd\t103.29\t2000-01-01T03:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );

            // all periods have finished, so expect everything to be reflected
            currentMicros = parseFloorPartialTimestamp("2000-01-03T00:00:00.000000Z");
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            final String finalExpected = replaceExpectedTimestamp("""
                    sym\tprice\tts
                    gbpusd\t1.32\t2000-01-01T02:00:00.000000Z
                    gbpusd\t1.323\t2000-01-01T03:00:00.000000Z
                    gbpusd\t1.321\t2000-01-01T04:00:00.000000Z
                    gbpusd\t1.322\t2000-01-01T05:00:00.000000Z
                    gbpusd\t1.321\t2000-01-02T01:00:00.000000Z
                    jpyusd\t103.21\t2000-01-01T02:00:00.000000Z
                    jpyusd\t103.29\t2000-01-01T03:00:00.000000Z
                    jpyusd\t103.21\t2000-01-02T01:00:00.000000Z
                    jpyusd\t103.23\t2000-01-02T05:00:00.000000Z
                    """);
            assertQueryNoLeakCheck(
                    finalExpected,
                    "price_1h order by sym"
            );

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_period_hi\ttimer_time_zone\ttimer_start\ttimer_interval\ttimer_interval_unit\tperiod_length\tperiod_length_unit\tperiod_delay\tperiod_delay_unit
                            price_1h\timmediate\tbase_price\t2000-01-03T00:00:00.000000Z\t2000-01-03T00:00:00.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t2\t2\t2000-01-03T00:00:00.000000Z\t\t2000-01-01T00:00:00.000000Z\t0\t\t4\tHOUR\t0\t
                            """,
                    matViewsSql,
                    null
            );

            // insert some rows in the current incomplete period
            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.424, '2000-01-03T01:02')" +
                            ",('jpyusd', 104.31, '2000-01-03T01:03')"
            );
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            // no rows should be inserted into the view, yet the last refresh txn
            // should be bumped to let WalPurgeJob do its job
            assertQueryNoLeakCheck(
                    finalExpected,
                    "price_1h order by sym"
            );

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn\trefresh_period_hi\ttimer_time_zone\ttimer_start\ttimer_interval\ttimer_interval_unit\tperiod_length\tperiod_length_unit\tperiod_delay\tperiod_delay_unit
                            price_1h\timmediate\tbase_price\t2000-01-03T00:00:00.000000Z\t2000-01-03T00:00:00.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t3\t3\t2000-01-03T00:00:00.000000Z\t\t2000-01-01T00:00:00.000000Z\t0\t\t4\tHOUR\t0\t
                            """,
                    matViewsSql,
                    null
            );
        });
    }

    @Test
    public void testQueryError() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, amount int, ts #TIMESTAMP" +
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
                    """
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            price_1h\tbase_price\tinvalid\t[-1]: unexpected filter error
                            """,
                    "select view_name, base_table_name, view_status, invalidation_reason from materialized_views",
                    null,
                    false
            );
        });
    }

    @Test
    public void testQueryError2() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table x (" +
                            "sym varchar, price double, amount int, ts #TIMESTAMP" +
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
                    """
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            x_1h\tx\tinvalid\t[58]: table does not exist [table=y]
                            """,
                    "select view_name, base_table_name, view_status, invalidation_reason from materialized_views",
                    null,
                    false
            );
        });
    }

    @Test
    public void testQueryTimestampMixedWithAggregates() throws Exception {
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL;");
            execute("INSERT INTO x VALUES ('2010-01-01T01'),('2010-01-01T01'),('2020-01-01T01'),('2030-01-01T01');");
            drainWalQueue();

            execute(
                    """
                            CREATE MATERIALIZED VIEW x_view AS
                            SELECT ts, count()::double / datediff('h', ts, dateadd('d', 1, ts, 'Europe/Copenhagen')) AS Coverage
                            FROM 'x'
                            SAMPLE BY 1d ALIGN TO CALENDAR TIME ZONE 'Europe/Copenhagen';
                            """
            );

            currentMicros = parseFloorPartialTimestamp("2024-01-01T01:01:01.000000Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status\trefresh_period_hi\trefresh_base_table_txn\tbase_table_txn\tperiod_length\tperiod_length_unit\trefresh_limit\trefresh_limit_unit
                            x_view\timmediate\tx\t2024-01-01T01:01:01.000000Z\t2024-01-01T01:01:01.000000Z\tvalid\t\t1\t1\t0\t\t0\t
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_status, refresh_period_hi, refresh_base_table_txn, base_table_txn, " +
                            "period_length, period_length_unit, refresh_limit, refresh_limit_unit " +
                            "from materialized_views",
                    null
            );
            assertQueryNoLeakCheck(
                    """
                            ts\tCoverage
                            2009-12-31T23:00:00.000000Z\t0.08333333333333333
                            2019-12-31T23:00:00.000000Z\t0.041666666666666664
                            2029-12-31T23:00:00.000000Z\t0.041666666666666664
                            """,
                    "x_view",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testQueryWithCte() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table exchanges (" +
                            " uid symbol, amount double, ts #TIMESTAMP " +
                            ") timestamp(ts) partition by day wal;"
            );
            executeWithRewriteTimestamp("create table aux_start_date (ts #TIMESTAMP);");

            execute(
                    "insert into exchanges values('foo', 1.320, '2024-09-10T12:01')" +
                            ",('foo', 1.323, '2024-09-10T12:02')" +
                            ",('bar', 103.21, '2024-09-10T12:02')" +
                            ",('foo', 1.321, '2024-09-10T13:02')"
            );
            execute("insert into aux_start_date values('2024-09-10')");
            drainQueues();

            final String expected = """
                    ts\tuid\tamount
                    2020-01-01T00:00:00.000000Z\tbar\t103.21
                    2020-01-01T00:00:00.000000Z\tfoo\t1.321
                    """;
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
                    "sample by 10y";
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), viewSql, "ts");

            execute("create materialized view exchanges_10y as (" + viewSql + ") partition by year");
            drainQueues();

            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), "exchanges_10y", "ts", true, true);
        });
    }

    @Test
    public void testRangeRefresh() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table x ( " +
                            "sym varchar, price double, ts #TIMESTAMP " +
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
                    """
                            view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\trefresh_base_table_txn\tbase_table_txn
                            x_1h\timmediate\tx\tvalid\t\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\t1\t1
                            """,
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
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-11T12:00:00.000000Z
                                    """),
                    "x_1h order by sym, ts"
            );
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\trefresh_base_table_txn\tbase_table_txn
                            x_1h\timmediate\tx\tvalid\t\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\t1\t1
                            """,
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
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), "x_1h order by sym, ts");
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\trefresh_base_table_txn\tbase_table_txn
                            x_1h\timmediate\tx\tvalid\t\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\t2\t2
                            """,
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
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), "x_1h order by sym, ts");
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\trefresh_base_table_txn\tbase_table_txn
                            x_1h\timmediate\tx\tinvalid\ttruncate operation\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\t2\t4
                            """,
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason, " +
                            "last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );
        });
    }

    @Test
    public void testRangeRefreshIgnoresRefreshLimitHours() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price ( " +
                            "sym varchar, price double, ts #TIMESTAMP " +
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
            final String ogExpected = replaceExpectedTimestamp("""
                    sym\tprice\tts
                    gbpusd\t1.32\t2024-09-09T12:00:00.000000Z
                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                    gbpusd\t1.321\t2024-09-12T13:00:00.000000Z
                    jpyusd\t103.21\t2024-09-11T12:00:00.000000Z
                    """);
            assertQueryNoLeakCheck(ogExpected, "price_1h order by sym, ts");
            final String matViewsSql = "select view_name, refresh_type, base_table_name, view_status, invalidation_reason, " +
                    "last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                    "refresh_base_table_txn, base_table_txn " +
                    "from materialized_views";
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\tvalid\t\t2024-09-13T00:00:00.000000Z\t2024-09-13T00:00:00.000000Z\t1\t1
                            """,
                    matViewsSql,
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
                    """
                            view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\tvalid\t\t2024-09-13T00:00:00.000000Z\t2024-09-13T00:00:00.000000Z\t2\t2
                            """,
                    matViewsSql,
                    null
            );

            // Run range refresh. The newly inserted rows should now be reflected in the mat view.
            execute("refresh materialized view price_1h range from '2024-09-09' to '2024-09-12';");
            drainQueues();
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            "sym\tprice\tts\n" +
                                    "gbpusd\t2.431\t2024-09-09T00:00:00.000000Z\n" + // new row
                                    "gbpusd\t1.32\t2024-09-09T12:00:00.000000Z\n" +
                                    "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                    "gbpusd\t1.321\t2024-09-12T13:00:00.000000Z\n" +
                                    "jpyusd\t214.32\t2024-09-11T00:00:00.000000Z\n" + // new row
                                    "jpyusd\t103.21\t2024-09-11T12:00:00.000000Z\n"),
                    "price_1h order by sym, ts"
            );
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\tvalid\t\t2024-09-13T00:00:00.000000Z\t2024-09-13T00:00:00.000000Z\t2\t2
                            """,
                    matViewsSql,
                    null
            );
        });
    }

    @Test
    public void testRangeRefreshIgnoresRefreshLimitMonths() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price ( " +
                            "sym varchar, price double, ts #TIMESTAMP " +
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
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.32\t2024-09-09T12:00:00.000000Z
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-12T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-11T12:00:00.000000Z
                                    """),
                    "price_1h order by sym, ts"
            );
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\tvalid\t\t2024-09-13T00:00:00.000000Z\t2024-09-13T00:00:00.000000Z\t1\t1
                            """,
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason, " +
                            "last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );

            execute("alter materialized view price_1h set refresh limit 1M;");

            // Insert rows with older and newer timestamps. The older rows should be ignored due to the refresh limit.
            execute(
                    "insert into base_price values ('gbpusd', 2.431, '2024-08-01T00:01')" +
                            ",('jpyusd', 214.32, '2024-08-01T00:02')" +
                            ",('gbpusd', 2.432, '2024-09-12T01:01')" +
                            ",('jpyusd', 214.32, '2024-09-12T01:02')"
            );
            drainQueues();
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            "sym\tprice\tts\n" +
                                    "gbpusd\t1.32\t2024-09-09T12:00:00.000000Z\n" +
                                    "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                    "gbpusd\t2.432\t2024-09-12T01:00:00.000000Z\n" + // newer row
                                    "gbpusd\t1.321\t2024-09-12T13:00:00.000000Z\n" +
                                    "jpyusd\t103.21\t2024-09-11T12:00:00.000000Z\n" +
                                    "jpyusd\t214.32\t2024-09-12T01:00:00.000000Z\n"), // newer row
                    "price_1h order by sym, ts"
            );
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\tvalid\t\t2024-09-13T00:00:00.000000Z\t2024-09-13T00:00:00.000000Z\t2\t2
                            """,
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason, " +
                            "last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );

            // Run range refresh. The older rows should now be reflected in the mat view.
            execute("refresh materialized view price_1h range from '2024-08-01' to '2024-09-12';");
            drainQueues();
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            "sym\tprice\tts\n" +
                                    "gbpusd\t2.431\t2024-08-01T00:00:00.000000Z\n" + // older row
                                    "gbpusd\t1.32\t2024-09-09T12:00:00.000000Z\n" +
                                    "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                    "gbpusd\t2.432\t2024-09-12T01:00:00.000000Z\n" +
                                    "gbpusd\t1.321\t2024-09-12T13:00:00.000000Z\n" +
                                    "jpyusd\t214.32\t2024-08-01T00:00:00.000000Z\n" + // older row
                                    "jpyusd\t103.21\t2024-09-11T12:00:00.000000Z\n" +
                                    "jpyusd\t214.32\t2024-09-12T01:00:00.000000Z\n"),
                    "price_1h order by sym, ts"
            );
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\tvalid\t\t2024-09-13T00:00:00.000000Z\t2024-09-13T00:00:00.000000Z\t2\t2
                            """,
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
            long startTs = timestampType.getDriver().parseFloorLiteral("2025-02-18T00:00:00.000000Z");
            long step = timestampType.getDriver().fromMicros(100000000L);
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
                    """
                            view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason
                            v1_base\timmediate\tbase\tvalid\t
                            v2_v1\timmediate\tv1_base\tvalid\t
                            v3_v1\timmediate\tv1_base\tvalid\t
                            v4_v3\timmediate\tv3_v1\tvalid\t
                            """,
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason from materialized_views order by view_name",
                    null,
                    true
            );

            execute("truncate table " + tableName);
            long ts = timestampType.getDriver().parseFloorLiteral("2025-05-17T00:00:00.000000Z");
            execute("insert into " + tableName + " " + generateSelectSql(columns, ts, step, N, N));

            drainQueues();

            // all views should be invalid
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason
                            v1_base\timmediate\tbase\tinvalid\ttruncate operation
                            v2_v1\timmediate\tv1_base\tinvalid\tbase materialized view is invalidated
                            v3_v1\timmediate\tv1_base\tinvalid\tbase materialized view is invalidated
                            v4_v3\timmediate\tv3_v1\tinvalid\tbase materialized view is invalidated
                            """,
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason from materialized_views order by view_name",
                    null,
                    true
            );

            execute("refresh materialized view " + view1Name + " full");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason
                            v1_base\timmediate\tbase\tvalid\t
                            v2_v1\timmediate\tv1_base\tinvalid\tbase materialized view is invalidated
                            v3_v1\timmediate\tv1_base\tinvalid\tbase materialized view is invalidated
                            v4_v3\timmediate\tv3_v1\tinvalid\tbase materialized view is invalidated
                            """,
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
                    """
                            view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason
                            v1_base\timmediate\tbase\tvalid\t
                            v2_v1\timmediate\tv1_base\tvalid\t
                            v3_v1\timmediate\tv1_base\tvalid\t
                            v4_v3\timmediate\tv3_v1\tvalid\t
                            """,
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason from materialized_views order by view_name",
                    null,
                    true
            );
        });
    }

    @Test
    public void testRecursiveInvalidationOnDropMatView() throws Exception {
        assertMemoryLeak(() -> {
            long startTs = timestampType.getDriver().parseFloorLiteral("2025-02-18T00:00:00.000000Z");
            long step = timestampType.getDriver().fromMicros(10L);
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
                    """
                            view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason
                            v1_base\timmediate\tbase\tvalid\t
                            v2_v1\timmediate\tv1_base\tvalid\t
                            v3_v1\timmediate\tv1_base\tvalid\t
                            v4_v3\timmediate\tv3_v1\tvalid\t
                            """,
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason from materialized_views order by view_name",
                    null,
                    true
            );

            execute("drop materialized view v1_base");

            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason
                            v2_v1\timmediate\tv1_base\tinvalid\tbase table is dropped or renamed
                            v3_v1\timmediate\tv1_base\tinvalid\tbase table is dropped or renamed
                            v4_v3\timmediate\tv3_v1\tinvalid\tbase materialized view is invalidated
                            """,
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason from materialized_views order by view_name",
                    null,
                    true
            );
        });
    }

    @Test
    public void testRecursiveInvalidationOnFailedRefresh() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, amount int, ts #TIMESTAMP" +
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
                    """
                            view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason
                            price_1d\timmediate\tprice_1h\tinvalid\tbase materialized view refresh failed
                            price_1d_2\timmediate\tprice_1h\tinvalid\tbase materialized view refresh failed
                            price_1h\timmediate\tbase_price\tinvalid\t[-1]: unexpected filter error
                            price_1w\timmediate\tprice_1d\tinvalid\tbase materialized view is invalidated
                            """,
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason from materialized_views order by view_name",
                    null,
                    true
            );
        });
    }

    @Test
    public void testRefreshExecutionContextBansWrongInsertions() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table x (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            executeWithRewriteTimestamp(
                    "create table y (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            final MatViewRefreshSqlExecutionContext refreshExecutionContext = new MatViewRefreshSqlExecutionContext(engine, 0);

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
    public void testRefreshIntervalsCachingSmoke() throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_INTERVALS_UPDATE_PERIOD, "5s");
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute(
                    "create materialized view price_1h refresh manual as " +
                            "select sym, last(price) as price, ts from base_price sample by 1h;"
            );
            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = parseFloorPartialTimestamp("2099-01-01T01:01:01.000000Z");
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            final TableToken viewToken = engine.getTableTokenIfExists("price_1h");
            Assert.assertNotNull(viewToken);
            final MatViewState viewState = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertNotNull(viewState);

            // nothing is cached after a successful refresh
            Assert.assertEquals(-1, viewState.getRefreshIntervalsBaseTxn());
            Assert.assertEquals(0, viewState.getRefreshIntervals().size());

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-11T12:01')" +
                            ",('jpyusd', 103.21, '2024-09-11T12:02')"
            );
            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-12T13:01')" +
                            ",('jpyusd', 103.21, '2024-09-12T13:02')"
            );
            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-12T03:01')" +
                            ",('jpyusd', 103.21, '2024-09-12T23:02')"
            );
            currentMicros += 6 * Micros.SECOND_MICROS;
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            // the newly inserted intervals should be in the cache
            Assert.assertEquals(4, viewState.getRefreshIntervalsBaseTxn());
            final LongList expectedIntervals = new LongList();
            expectedIntervals.add(timestampType.getDriver().parseFloorLiteral("2024-09-11T12:01"), timestampType.getDriver().parseFloorLiteral("2024-09-11T12:02"));
            expectedIntervals.add(timestampType.getDriver().parseFloorLiteral("2024-09-12T03:01"), timestampType.getDriver().parseFloorLiteral("2024-09-12T23:02"));
            TestUtils.assertEquals(expectedIntervals, viewState.getRefreshIntervals());

            // at this point, new rows shouldn't be reflected in the view
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );
            final String matViewsSql = "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                    "view_sql, view_status, refresh_base_table_txn, base_table_txn " +
                    "from materialized_views";
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\tmanual\tbase_price\t2099-01-01T01:01:01.000000Z\t2099-01-01T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h;\tvalid\t1\t4
                            """,
                    matViewsSql,
                    null
            );

            // WalPurgeJob should be able to delete WAL segments freely
            final TableToken baseTableToken = engine.getTableTokenIfExists("base_price");
            Assert.assertNotNull(baseTableToken);
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(baseTableToken).concat(WalUtils.WAL_NAME_BASE).put(1);
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                engine.releaseInactiveTableSequencers();
                drainPurgeJob();

                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
            }

            // the new rows should be reflected in the view after an explicit incremental refresh call
            execute("refresh materialized view price_1h incremental;");
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            // nothing is cached after a successful refresh
            Assert.assertEquals(-1, viewState.getRefreshIntervalsBaseTxn());
            Assert.assertEquals(0, viewState.getRefreshIntervals().size());

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\tmanual\tbase_price\t2099-01-01T01:01:07.000000Z\t2099-01-01T01:01:07.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h;\tvalid\t4\t4
                            """,
                    matViewsSql,
                    null
            );
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    gbpusd\t1.32\t2024-09-11T12:00:00.000000Z
                                    gbpusd\t1.32\t2024-09-12T03:00:00.000000Z
                                    gbpusd\t1.32\t2024-09-12T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-11T12:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-12T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-12T23:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );
        });
    }

    @Test
    public void testRefreshIntervalsCapacity() throws Exception {
        final int capacity = 10;
        setProperty(PropertyKey.CAIRO_MAT_VIEW_MAX_REFRESH_INTERVALS, capacity);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_INTERVALS_UPDATE_PERIOD, "10s");
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute(
                    "create materialized view price_1h refresh manual as " +
                            "select sym, last(price) as price, ts from base_price sample by 1h;"
            );
            execute("insert into base_price(sym, price, ts) values ('gbpusd', 1.320, '2024-09-10T12:01')");

            currentMicros = parseFloorPartialTimestamp("2025-01-01T01:01:01.000000Z");
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            for (int i = 0; i < 3 * capacity; i++) {
                execute("insert into base_price(sym, price, ts) values ('gbpusd', " + i + ", (" + i + "*1000000)::timestamp)");
            }

            final TableToken viewToken = engine.getTableTokenIfExists("price_1h");
            Assert.assertNotNull(viewToken);
            final MatViewState viewState = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertNotNull(viewState);

            // nothing is cached after a successful refresh
            Assert.assertEquals(-1, viewState.getRefreshIntervalsBaseTxn());
            Assert.assertEquals(0, viewState.getRefreshIntervals().size());

            currentMicros += 11 * Micros.SECOND_MICROS;
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            // the newly inserted intervals should be in the cache
            Assert.assertEquals(3 * capacity + 1, viewState.getRefreshIntervalsBaseTxn());
            final int intervalsSize = viewState.getRefreshIntervals().size();
            Assert.assertEquals(2 * capacity, intervalsSize);
            for (int i = 0; i < capacity - 1; i++) {
                Assert.assertEquals(timestampType.getDriver().fromSeconds(i), viewState.getRefreshIntervals().getQuick(2 * i));
                Assert.assertEquals(timestampType.getDriver().fromSeconds(i), viewState.getRefreshIntervals().getQuick(2 * i + 1));
            }
            // The latest intervals should be squashed into the last one.
            Assert.assertEquals(timestampType.getDriver().fromSeconds(capacity - 1), viewState.getRefreshIntervals().getQuick(intervalsSize - 2));
            Assert.assertEquals(timestampType.getDriver().fromSeconds(3 * capacity - 1), viewState.getRefreshIntervals().getQuick(intervalsSize - 1));

            // at this point, new rows shouldn't be reflected in the view
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.32\t2024-09-10T12:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );
            final String matViewsSql = "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                    "view_sql, view_status, refresh_base_table_txn, base_table_txn " +
                    "from materialized_views";
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\tmanual\tbase_price\t2025-01-01T01:01:01.000000Z\t2025-01-01T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h;\tvalid\t1\t31
                            """,
                    matViewsSql,
                    null
            );

            // WalPurgeJob should be able to delete WAL segments freely
            final TableToken baseTableToken = engine.getTableTokenIfExists("base_price");
            Assert.assertNotNull(baseTableToken);
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(baseTableToken).concat(WalUtils.WAL_NAME_BASE).put(1);
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                engine.releaseInactiveTableSequencers();
                drainPurgeJob();

                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
            }

            // the new rows should be reflected in the view after an explicit incremental refresh call
            execute("refresh materialized view price_1h incremental;");
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            // nothing is cached after a successful refresh
            Assert.assertEquals(-1, viewState.getRefreshIntervalsBaseTxn());
            Assert.assertEquals(0, viewState.getRefreshIntervals().size());

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\tmanual\tbase_price\t2025-01-01T01:01:12.000000Z\t2025-01-01T01:01:12.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h;\tvalid\t31\t31
                            """,
                    matViewsSql,
                    null
            );
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t29.0\t1970-01-01T00:00:00.000000Z
                                    gbpusd\t1.32\t2024-09-10T12:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );
        });
    }

    @Test
    public void testRefreshSkipsUnchangedBuckets() throws Exception {
        // Verify that incremental refresh skips unchanged SAMPLE BY buckets.
        assertMemoryLeak(() -> {
            TestTimestampCounterFactory.COUNTER.set(0);

            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "  price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL;"
            );

            final String viewSql = "select ts, test_timestamp_counter(ts) ts0, max(price) max_price " +
                    "from base_price " +
                    "sample by 1d";
            createMatView(viewSql);

            execute("insert into base_price(price, ts) values (1.320, '2024-01-01T00:01'), (1.321, '2024-01-30T00:01');");

            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """,
                    "select view_name, base_table_name, view_status from materialized_views",
                    null,
                    false
            );

            // The function must have been called for two days only although the full interval is 30 days.
            Assert.assertEquals(2, TestTimestampCounterFactory.COUNTER.get());

            final String expected = """
                    ts\tts0\tmax_price
                    2024-01-01T00:00:00.000000Z\t2024-01-01T00:00:00.000000Z\t1.32
                    2024-01-30T00:00:00.000000Z\t2024-01-30T00:00:00.000000Z\t1.321
                    """;
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), viewSql + " order by ts", "ts", true, true);
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), "price_1h order by ts", "ts", true, true);
        });
    }

    @Test
    public void testResumeSuspendMatView() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP, extra_col long" +
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
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-01-01T01:01:01.842574Z\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t1\t1
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );

            // suspend mat view
            execute("alter materialized view price_1h suspend wal");

            execute("insert into base_price(sym, price, ts) values('jpyusd', 103.14, '2024-09-10T13:04')");
            drainQueues();

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );

            assertQueryNoLeakCheck(
                    """
                            name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure
                            base_price\tfalse\t2\t0\t2\t\t\t0
                            price_1h\ttrue\t1\t0\t3\t\t\t0
                            """,
                    "wal_tables()",
                    null,
                    false
            );

            // resume mat view
            execute("alter materialized view price_1h resume wal");
            drainQueues();

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    jpyusd\t103.14\t2024-09-10T13:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );

            assertQueryNoLeakCheck(
                    """
                            name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure
                            base_price\tfalse\t2\t0\t2\t\t\t0
                            price_1h\tfalse\t3\t0\t3\t\t\t0
                            """,
                    "wal_tables()",
                    null,
                    false
            );

            // suspend mat view again
            execute("alter materialized view price_1h suspend wal");

            execute("insert into base_price(sym, price, ts) values('jpyusd', 103.17, '2024-09-10T13:22')");
            drainQueues();

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    jpyusd\t103.14\t2024-09-10T13:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );

            assertQueryNoLeakCheck(
                    """
                            name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure
                            base_price\tfalse\t3\t0\t3\t\t\t0
                            price_1h\ttrue\t3\t0\t5\t\t\t0
                            """,
                    "wal_tables()",
                    null,
                    false
            );

            // resume mat view from txn
            execute("alter materialized view price_1h resume wal from txn 3");
            drainQueues();

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    jpyusd\t103.17\t2024-09-10T13:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );

            assertQueryNoLeakCheck(
                    """
                            name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure
                            base_price\tfalse\t3\t0\t3\t\t\t0
                            price_1h\tfalse\t5\t0\t5\t\t\t0
                            """,
                    "wal_tables()",
                    null,
                    false
            );
        });
    }

    @Test
    public void testSampleByDST() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
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

            final String expected = """
                    sym\tfirst\tlast\tcount\tts
                    gbpusd\t1.32\t1.321\t2\t2024-10-25T22:00:00.000000Z
                    gbpusd\t1.324\t1.326\t3\t2024-10-26T22:00:00.000000Z
                    gbpusd\t1.327\t1.328\t2\t2024-10-27T23:00:00.000000Z
                    """;
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(expected),
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
    public void testSampleByNanos() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1000000000n");

            execute(
                    "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = parseFloorPartialTimestamp("2024-10-24T17:22:09.842574Z");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\tinvalidation_reason\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2024-10-24T17:22:09.842574Z\t2024-10-24T17:22:09.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1000000000n\tvalid\t\t1\t1
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, invalidation_reason, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );

            final String expected = timestampType == TestTimestampType.MICRO
                    ? """
                    sym\tprice\tts
                    gbpusd\t1.32\t2024-09-10T12:01:00.000000Z
                    gbpusd\t1.323\t2024-09-10T12:02:00.000000Z
                    jpyusd\t103.21\t2024-09-10T12:02:00.000000Z
                    gbpusd\t1.321\t2024-09-10T13:02:00.000000Z
                    """
                    : """
                    sym\tprice\tts
                    gbpusd\t1.32\t2024-09-10T12:01:00.000000000Z
                    gbpusd\t1.323\t2024-09-10T12:02:00.000000000Z
                    jpyusd\t103.21\t2024-09-10T12:02:00.000000000Z
                    gbpusd\t1.321\t2024-09-10T13:02:00.000000000Z
                    """;
            assertQueryNoLeakCheck(
                    expected,
                    "price_1h",
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
            final long startTs = timestampType.getDriver().parseFloorLiteral("1970-01-03T00:20:00.000000Z");
            final long step = timestampType.getDriver().fromMicros(300000000);
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewQuery, startTs, step, N, K);

            final String expected = """
                    k\tc
                    1970-01-02T14:42:00.000000Z\t5
                    1970-01-02T16:42:00.000000Z\t24
                    1970-01-02T18:42:00.000000Z\t24
                    1970-01-02T20:42:00.000000Z\t24
                    1970-01-02T22:42:00.000000Z\t23
                    """;

            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewQuery), null, true, true);
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewName), null, true, true);
        });
    }

    @Test
    public void testSampleByNoFillNotKeyedAlignToCalendarMisalignedTimezone() throws Exception {
        // IRAN timezone is +4:30, which doesn't align well with 1hr sample
        assertMemoryLeak(() -> {
            final String viewName = "x_view";
            final String viewQuery = "select k, count() c from x sample by 1h align to calendar time zone 'Iran'";
            final long startTs = timestampType.getDriver().parseFloorLiteral("2021-03-28T00:15:00.000000Z");
            final long step = timestampType.getDriver().fromMicros(6 * 60000000);
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewQuery, startTs, step, N, K);

            final String expected = """
                    k\tc
                    2021-03-28T04:00:00.000000Z\t3
                    2021-03-28T05:00:00.000000Z\t10
                    2021-03-28T06:00:00.000000Z\t10
                    2021-03-28T07:00:00.000000Z\t10
                    2021-03-28T08:00:00.000000Z\t10
                    2021-03-28T09:00:00.000000Z\t10
                    2021-03-28T10:00:00.000000Z\t10
                    2021-03-28T11:00:00.000000Z\t10
                    2021-03-28T12:00:00.000000Z\t10
                    2021-03-28T13:00:00.000000Z\t10
                    2021-03-28T14:00:00.000000Z\t7
                    """;

            final String out = "select to_timezone(k, 'Iran') k, c";
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewQuery), null, true, true);
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewName), null, true, true);
        });
    }

    @Test
    public void testSampleByNoFillNotKeyedAlignToCalendarTimezone() throws Exception {
        assertMemoryLeak(() -> {
            final String viewName = "x_view";
            final String viewQuery = "select k, count() c from x sample by 1h align to calendar time zone 'Europe/Berlin'";
            final long startTs = timestampType.getDriver().parseFloorLiteral("2021-03-28T00:15:00.000000Z");
            final long step = timestampType.getDriver().fromMicros(6 * 60000000);
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewQuery, startTs, step, N, K);

            final String expected = """
                    k\tc
                    2021-03-28T01:00:00.000000Z\t8
                    2021-03-28T03:00:00.000000Z\t10
                    2021-03-28T04:00:00.000000Z\t10
                    2021-03-28T05:00:00.000000Z\t10
                    2021-03-28T06:00:00.000000Z\t10
                    2021-03-28T07:00:00.000000Z\t10
                    2021-03-28T08:00:00.000000Z\t10
                    2021-03-28T09:00:00.000000Z\t10
                    2021-03-28T10:00:00.000000Z\t10
                    2021-03-28T11:00:00.000000Z\t10
                    2021-03-28T12:00:00.000000Z\t2
                    """;

            final String out = "select to_timezone(k, 'Europe/Berlin') k, c";
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewQuery), null, true, true);
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewName), null, true, true);
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
            final long startTs = timestampType.getDriver().parseFloorLiteral("2021-10-31T00:15:00.000000Z");
            final long step = timestampType.getDriver().fromMicros(6 * 60000000);
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewQuery, startTs, step, N, K);

            final String expected = """
                    k\tc
                    2021-10-31T02:00:00.000000Z\t18
                    2021-10-31T03:00:00.000000Z\t10
                    2021-10-31T04:00:00.000000Z\t10
                    2021-10-31T05:00:00.000000Z\t10
                    2021-10-31T06:00:00.000000Z\t10
                    2021-10-31T07:00:00.000000Z\t10
                    2021-10-31T08:00:00.000000Z\t10
                    2021-10-31T09:00:00.000000Z\t10
                    2021-10-31T10:00:00.000000Z\t10
                    2021-10-31T11:00:00.000000Z\t2
                    """;
            final String out = "select to_timezone(k, 'Europe/Berlin') k, c";
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewQuery));
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewName));
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
            final long startTs = timestampType.getDriver().parseFloorLiteral("2021-10-31T00:15:00.000000Z");
            final long step = timestampType.getDriver().fromMicros(6 * 60000000);
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewQuery, startTs, step, N, K);

            final String expected = """
                    k\tc
                    2021-10-31T02:00:00.000000Z\t8
                    2021-10-31T02:30:00.000000Z\t10
                    2021-10-31T03:00:00.000000Z\t5
                    2021-10-31T03:30:00.000000Z\t5
                    2021-10-31T04:00:00.000000Z\t5
                    2021-10-31T04:30:00.000000Z\t5
                    2021-10-31T05:00:00.000000Z\t5
                    2021-10-31T05:30:00.000000Z\t5
                    2021-10-31T06:00:00.000000Z\t5
                    2021-10-31T06:30:00.000000Z\t5
                    2021-10-31T07:00:00.000000Z\t5
                    2021-10-31T07:30:00.000000Z\t5
                    2021-10-31T08:00:00.000000Z\t5
                    2021-10-31T08:30:00.000000Z\t5
                    2021-10-31T09:00:00.000000Z\t5
                    2021-10-31T09:30:00.000000Z\t5
                    2021-10-31T10:00:00.000000Z\t5
                    2021-10-31T10:30:00.000000Z\t5
                    2021-10-31T11:00:00.000000Z\t2
                    """;

            final String out = "select to_timezone(k, 'Europe/Berlin') k, c";
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewQuery), null, true, true);
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), outSelect(out, viewName), null, true, true);
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
            final long startTs = timestampType.getDriver().fromMicros(172800000000L);
            final long step = timestampType.getDriver().fromMicros(300000000);
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewQuery, startTs, step, N, K);

            final String expected = """
                    k\tc
                    1970-01-03T00:00:00.000000Z\t18
                    1970-01-03T01:30:00.000000Z\t18
                    1970-01-03T03:00:00.000000Z\t18
                    1970-01-03T04:30:00.000000Z\t18
                    1970-01-03T06:00:00.000000Z\t18
                    1970-01-03T07:30:00.000000Z\t10
                    """;

            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), viewQuery, "k", true, true);
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), viewName, "k", true, true);
        });
    }

    @Test
    public void testSampleByNoFillNotKeyedAlignToCalendarUTCOffset() throws Exception {
        assertMemoryLeak(() -> {
            final String viewName = "x_view";
            final String viewQuery = "select k, count() c from x sample by 90m align to calendar with offset '00:42'";
            final long startTs = timestampType.getDriver().fromMicros(172800000000L);
            final long step = timestampType.getDriver().fromMicros(300000000);
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewQuery, startTs, step, N, K);

            final String expected = """
                    k\tc
                    1970-01-02T23:12:00.000000Z\t9
                    1970-01-03T00:42:00.000000Z\t18
                    1970-01-03T02:12:00.000000Z\t18
                    1970-01-03T03:42:00.000000Z\t18
                    1970-01-03T05:12:00.000000Z\t18
                    1970-01-03T06:42:00.000000Z\t18
                    1970-01-03T08:12:00.000000Z\t1
                    """;

            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), viewQuery, "k", true, true);
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), viewName, "k", true, true);
        });
    }

    @Test
    public void testSelfJoinQuery() throws Exception {
        // Verify that the detached base table reader used by the refresh job
        // can be safely used in the mat view query multiple times.
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym symbol index, sym2 symbol, price double, ts #TIMESTAMP, extra_col long" +
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
                    """
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """,
                    "select view_name, base_table_name, view_status from materialized_views",
                    null,
                    false
            );

            final String expected = """
                    sym_a\tsym_b\tsym2_a\tsym2_b\tprice\tts
                    foobar\t\ts1\t\tnull\t2024-09-10T12:00:00.000000Z
                    foobar\tbarbaz\ts1\ts1\t103.21\t2024-09-10T12:00:00.000000Z
                    foobar\tbarbaz\ts1\ts1\t103.23\t2024-09-10T13:00:00.000000Z
                    """;
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), viewSql + " order by ts, sym_a, sym_b", "ts", true);
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), "price_1h order by ts, sym_a, sym_b", "ts", true, true);
        });
    }

    @Test
    public void testSimpleCancelRefresh() throws Exception {
        assertMemoryLeak(() -> {
            final SOCountDownLatch started = new SOCountDownLatch(1);
            final SOCountDownLatch stopped = new SOCountDownLatch(1);
            final AtomicBoolean refreshed = new AtomicBoolean(true);

            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
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
                            try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine, 0)) {
                                refreshed.set(job.run(0));
                            }
                        } finally {
                            Path.clearThreadLocals();
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
                    """
                            view_name\tview_status
                            price_1h\tinvalid
                            """,
                    "select view_name, view_status from materialized_views",
                    null,
                    false
            );
        });
    }

    @Test
    public void testSimpleRefresh() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
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
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    """),
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

            String expected = """
                    sym\tprice\tts
                    gbpusd\t1.319\t2024-09-10T12:00:00.000000Z
                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                    gbpusd\t1.325\t2024-09-10T13:00:00.000000Z
                    """;

            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), "select sym, last(price) as price, ts from base_price sample by 1h order by ts, sym", "ts", true, true);
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), "price_1h order by ts, sym", "ts", true, true);
        });
    }

    @Test
    public void testSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "  sym varchar, price double, ts #TIMESTAMP" +
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
        testTimerMatViewBigJumps(
                null,
                "2024-12-12T12:00:01.000000Z",
                "2024-12-12T13:00:01.000000Z",
                2 * Micros.HOUR_MICROS
        );
    }

    @Test
    public void testTimerMatViewBigJumpsClockAfterTickBoundaryWithTz() throws Exception {
        testTimerMatViewBigJumps(
                "Europe/London",
                "2024-12-12T12:00:01.000000Z",
                "2024-12-12T13:00:01.000000Z",
                2 * Micros.HOUR_MICROS
        );
    }

    @Test
    public void testTimerMatViewBigJumpsClockAtTickBoundary() throws Exception {
        testTimerMatViewBigJumps(
                null,
                "2024-12-12T12:00:00.000000Z",
                "2024-12-12T12:00:00.000000Z",
                Micros.HOUR_MICROS
        );
    }

    @Test
    public void testTimerMatViewBigJumpsClockAtTickBoundaryWithTz() throws Exception {
        testTimerMatViewBigJumps(
                "Europe/London",
                "2024-12-12T12:00:00.000000Z",
                "2024-12-12T12:00:00.000000Z",
                Micros.HOUR_MICROS
        );
    }

    @Test
    public void testTimerMatViewDeferredRefresh() throws Exception {
        assertMemoryLeak(() -> {
            final long start = parseFloorPartialTimestamp("2024-12-12T00:00:00.000000Z");

            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createNthTimerMatView(timestampType.getDriver(), timestampType.getDriver().fromMicros(start), 0);

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = start + 61 * Micros.SECOND_MICROS;
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            // The current time is after the [start, start+epsilon] interval, so the view shouldn't refresh.
            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n",
                    "price_1h_0 order by sym"
            );

            currentMicros = start + Micros.HOUR_MICROS;
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            // It's the next hourly interval now, so the view should refresh.
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    """),
                    "price_1h_0 order by sym"
            );
        });
    }

    @Test
    public void testTimerMatViewRefreshAfterCreation() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            currentMicros = parseFloorPartialTimestamp("2024-12-12T01:00:00.000000Z");
            execute(
                    "create materialized view price_1h refresh every 1m as (" +
                            "select sym, last(price) as price, ts from base_price sample by 1h" +
                            ") partition by day;"
            );

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            // move clock one hour before, so that the timer doesn't trigger
            currentMicros = parseFloorPartialTimestamp("2024-12-12T00:00:00.000000Z");
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            // the view should still get refreshed since it's non-deferred
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );
        });
    }

    @Test
    public void testTimerMatViewSmallHourJumps() throws Exception {
        testTimerMatViewSmallJumps(
                null,
                "2024-12-12T00:00:00.000000Z",
                "2d",
                "2024-12-11T00:00:00.000000Z",
                Micros.HOUR_MICROS,
                23
        );
    }

    @Test
    public void testTimerMatViewSmallHourJumpsWithTz() throws Exception {
        testTimerMatViewSmallJumps(
                "Europe/London",
                "2024-12-12T00:00:00.000000Z",
                "2d",
                "2024-12-11T00:00:00.000000Z",
                Micros.HOUR_MICROS,
                23
        );
    }

    @Test
    public void testTimerMatViewSmallMinuteJumps1() throws Exception {
        testTimerMatViewSmallJumps(
                null,
                "2024-12-12T01:30:00.000000Z",
                "30m",
                "2024-12-12T01:00:00.000000Z",
                Micros.MINUTE_MICROS,
                29
        );
    }

    @Test
    public void testTimerMatViewSmallMinuteJumps2() throws Exception {
        testTimerMatViewSmallJumps(
                null,
                "2024-12-12T01:30:00.000000Z",
                "30m",
                "2024-12-12T01:45:00.000000Z",
                Micros.MINUTE_MICROS,
                14
        );
    }

    @Test
    public void testTimerMatViewSmallMinuteJumpsWithTz1() throws Exception {
        testTimerMatViewSmallJumps(
                "Europe/London",
                "2024-12-12T01:30:00.000000Z",
                "30m",
                "2024-12-12T01:00:00.000000Z",
                Micros.MINUTE_MICROS,
                29
        );
    }

    @Test
    public void testTimerMatViewSmallMinuteJumpsWithTz2() throws Exception {
        testTimerMatViewSmallJumps(
                "Europe/London",
                "2024-12-12T01:30:00.000000Z",
                "30m",
                "2024-12-12T01:45:00.000000Z",
                Micros.MINUTE_MICROS,
                14
        );
    }

    @Test
    public void testTimerMatViewSmoke() throws Exception {
        assertMemoryLeak(() -> {
            final long start = parseFloorPartialTimestamp("2024-12-12T00:00:00.000000Z");

            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createNthTimerMatView(timestampType.getDriver(), timestampType.getDriver().fromMicros(start), 0);

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
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    """),
                    "price_1h_0 order by sym"
            );

            // Tick the timer once again, this time with no new transaction.
            currentMicros += 2 * Micros.HOUR_MICROS;
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            // Insert new rows
            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.321, '2024-09-10T14:02')" +
                            ",('jpyusd', 103.22, '2024-09-10T14:03')"
            );

            // Do more timer ticks.
            currentMicros += 2 * Micros.HOUR_MICROS;
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T14:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    jpyusd\t103.22\t2024-09-10T14:00:00.000000Z
                                    """),
                    "price_1h_0 order by sym"
            );
        });
    }

    @Test
    public void testTimerPeriodMatView() throws Exception {
        testPeriodRefresh("every 1h deferred", null, true);
    }

    @Test
    public void testTimerPeriodMatViewFullRefresh() throws Exception {
        testPeriodRefresh("every 1h deferred", "full", true);
    }

    @Test
    public void testTimerPeriodMatViewIncrementalRefresh() throws Exception {
        testPeriodRefresh("every 1h deferred", "incremental", true);
    }

    @Test
    public void testTimerPeriodWithTzMatView() throws Exception {
        testPeriodWithTzRefresh("every 1h deferred", null, true);
    }

    @Test
    public void testTimerPeriodWithTzMatViewFullRefresh() throws Exception {
        testPeriodWithTzRefresh("every 1h deferred", "full", true);
    }

    @Test
    public void testTimerPeriodWithTzMatViewIncrementalRefresh() throws Exception {
        testPeriodWithTzRefresh("every 1h deferred", "incremental", true);
    }

    @Test
    public void testTimestampGetsRefreshedOnInvalidation() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, amount int, ts #TIMESTAMP" +
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

            final String expected = """
                    sym\tprice\tts
                    gbpusd\t1.321\t2024-09-10T00:00:00.000000Z
                    jpyusd\t103.21\t2024-09-10T00:00:00.000000Z
                    """;
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), viewQuery, "ts", true, true);
            assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), "price_1h", "ts", true, true);

            currentMicros = parseFloorPartialTimestamp("2020-01-01T01:01:01.000000Z");
            execute("drop table base_price;");
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn
                            price_1h\timmediate\tbase_price\t2020-01-01T01:01:01.000000Z\t2020-01-01T01:01:01.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1d\tinvalid\t1\t-1
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );
        });
    }

    @Test
    public void testTtl() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
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
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.312\t2024-09-12T13:00:00.000000Z
                                    gbpusd\t1.313\t2024-09-13T13:00:00.000000Z
                                    gbpusd\t1.314\t2024-09-14T13:00:00.000000Z
                                    """),
                    "price_1h order by ts, sym",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testViewInvalidatedOnRefreshAfterBaseTableRename() throws Exception {
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
            execute(
                    "create materialized view price_1h refresh manual as " +
                            "select sym, last(price) as price, ts from base_price sample by 1h;"
            );
            execute(
                    "insert into base_price(sym, price, ts) values ('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')"
            );
            execute(
                    "insert into base_price(sym, price, ts) values ('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = parseFloorPartialTimestamp("2025-01-01T01:01:01.000000Z");
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-11T12:01')" +
                            ",('jpyusd', 103.21, '2024-09-11T12:02')"
            );
            currentMicros += 6 * Micros.SECOND_MICROS;

            drainMatViewTimerQueue(timerJob);
            drainQueues();

            execute("refresh materialized view price_1h incremental;");
            execute("drop table base_price;");
            execute("rename table base_price2 to base_price;");

            drainQueues();

            // the view must be marked as invalid as the result of refresh attempt
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\tinvalidation_reason\trefresh_base_table_txn\tbase_table_txn
                            price_1h\tmanual\tbase_price\t2025-01-01T01:01:01.000000Z\t2025-01-01T01:01:07.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h;\tinvalid\t[-1]: unexpected txn numbers, base table may have been renamed [view=price_1h, fromBaseTxn=2, toBaseTxn=1]\t2\t1
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, invalidation_reason, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );
        });
    }

    @Test
    public void testViewInvalidatedOnRefreshIntervalsUpdateAfterBaseTableRename() throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_INTERVALS_UPDATE_PERIOD, "5s");
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
            execute(
                    "create materialized view price_1h refresh manual as " +
                            "select sym, last(price) as price, ts from base_price sample by 1h;"
            );
            execute(
                    "insert into base_price(sym, price, ts) values ('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')"
            );
            execute(
                    "insert into base_price(sym, price, ts) values ('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = parseFloorPartialTimestamp("2099-01-01T01:01:01.000000Z");
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-11T12:01')" +
                            ",('jpyusd', 103.21, '2024-09-11T12:02')"
            );
            currentMicros += 6 * Micros.SECOND_MICROS;

            drainMatViewTimerQueue(timerJob);

            execute("drop table base_price;");
            execute("rename table base_price2 to base_price;");

            drainQueues();

            // the view must be marked as invalid since the base table was dropped
            assertQueryNoLeakCheck(
                    """
                            view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\tinvalidation_reason\trefresh_base_table_txn\tbase_table_txn
                            price_1h\tmanual\tbase_price\t2099-01-01T01:01:07.000000Z\t2099-01-01T01:01:07.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h;\tinvalid\tbase table is dropped or renamed\t2\t1
                            """,
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, invalidation_reason, refresh_base_table_txn, base_table_txn " +
                            "from materialized_views",
                    null
            );
        });
    }

    @Test
    public void testWeeklySampleTimestampOutOfRangeIssue6089() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE historical_prices (" +
                            "  symbol SYMBOL," +
                            "  market SYMBOL," +
                            "  timestamp #TIMESTAMP," +
                            "  price DOUBLE," +
                            "  volume LONG" +
                            ") timestamp(timestamp) PARTITION BY DAY WAL"
            );

            execute(
                    "CREATE MATERIALIZED VIEW 'historical_prices_1week' AS (" +
                            "  SELECT timestamp, symbol, market, " +
                            "  first(price) AS open, max(price) AS high, " +
                            "  min(price) AS low, last(price) AS close, " +
                            "  sum(volume) AS volume" +
                            "  FROM historical_prices" +
                            "  SAMPLE BY 1w" +
                            ") PARTITION BY MONTH TTL 5 YEARS"
            );

            execute(
                    "INSERT INTO historical_prices VALUES" +
                            "('HP', 'NYSE', '2025-08-31T15:49:00.309937Z', 28.50, 100)," +
                            "('HP', 'NYSE', '2025-08-31T15:49:00.309937Z', 28.55, 120)," +
                            "('HP', 'NYSE', '2025-08-31T15:49:00.309937Z', 28.52, 80)"
            );

            drainQueues();

            assertSql(
                    timestampType == TestTimestampType.MICRO
                            ? """
                            symbol\tmarket\ttimestamp\tprice\tvolume
                            HP\tNYSE\t2025-08-31T15:49:00.309937Z\t28.5\t100
                            HP\tNYSE\t2025-08-31T15:49:00.309937Z\t28.55\t120
                            HP\tNYSE\t2025-08-31T15:49:00.309937Z\t28.52\t80
                            """
                            : """
                            symbol\tmarket\ttimestamp\tprice\tvolume
                            HP\tNYSE\t2025-08-31T15:49:00.309937000Z\t28.5\t100
                            HP\tNYSE\t2025-08-31T15:49:00.309937000Z\t28.55\t120
                            HP\tNYSE\t2025-08-31T15:49:00.309937000Z\t28.52\t80
                            """,
                    "select * from historical_prices"
            );

            final String expected = timestampType == TestTimestampType.MICRO
                    ? """
                    timestamp\tsymbol\tmarket\topen\thigh\tlow\tclose\tvolume
                    2025-08-25T00:00:00.000000Z\tHP\tNYSE\t28.5\t28.55\t28.5\t28.52\t300
                    """
                    : """
                    timestamp\tsymbol\tmarket\topen\thigh\tlow\tclose\tvolume
                    2025-08-25T00:00:00.000000000Z\tHP\tNYSE\t28.5\t28.55\t28.5\t28.52\t300
                    """;
            assertQueryNoLeakCheck(
                    expected,
                    "SELECT timestamp, symbol, market, " +
                            "first(price) AS open, max(price) AS high, min(price) AS low, last(price) AS close, sum(volume) AS volume " +
                            "FROM historical_prices " +
                            "SAMPLE BY 1w",
                    "timestamp",
                    true,
                    true
            );

            // Assert that materialized view status is valid
            assertSql(
                    """
                            view_name\tview_status
                            historical_prices_1week\tvalid
                            """,
                    "select view_name, view_status from materialized_views where view_name = 'historical_prices_1week'"
            );

            // Assert that view returns aggregated data
            assertQueryNoLeakCheck(
                    expected,
                    "historical_prices_1week",
                    "timestamp",
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

    private static void createNthTimerMatView(TimestampDriver driver, long start, int n) throws SqlException {
        sink.clear();
        driver.append(sink, start + driver.fromSeconds(n));
        execute(
                "create materialized view price_1h_" + n + " refresh every 1m deferred start '" + sink + "' as (" +
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

    private void executeWithRewriteTimestamp(CharSequence sqlText) throws SqlException {
        sqlText = sqlText.toString().replaceAll("#TIMESTAMP", timestampType.getTypeName());
        engine.execute(sqlText, sqlExecutionContext);
    }

    private String generateSelectSql(String columns, long startTs, long step, int init, int count) {
        return "select" +
                " x + " + init + " as n," +
                columns +
                (ColumnType.isTimestampMicro(timestampType.getTimestampType()) ?
                        " timestamp_sequence(" + startTs + ", " + step + ") k" :
                        " timestamp_sequence_ns(" + startTs + ", " + step + ") k") +
                " from" +
                " long_sequence(" + count + ")";
    }

    private String outSelect(String out, String in) {
        return out + " from (" + in + ")";
    }

    private String replaceExpectedTimestamp(String expected) {
        return ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? expected : expected.replaceAll(".000000Z", ".000000000Z");
    }

    private void testAlignToCalendarTimezoneOffset(final String timezone) throws Exception {
        final String viewName = "x_view";
        final String viewQuery = "select k, count() c from x sample by 90m align to calendar time zone '" + timezone + "' with offset '00:42'";
        final long startTs = timestampType.getDriver().fromMicros(172800000000L);
        final long step = timestampType.getDriver().fromMicros(300000000);
        final int N = 100;
        final int K = 5;
        updateViewIncrementally(viewQuery, startTs, step, N, K);

        final String expected = """
                k\tc
                1970-01-02T23:42:00.000000Z\t15
                1970-01-03T01:12:00.000000Z\t18
                1970-01-03T02:42:00.000000Z\t18
                1970-01-03T04:12:00.000000Z\t18
                1970-01-03T05:42:00.000000Z\t18
                1970-01-03T07:12:00.000000Z\t13
                """;

        assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), viewQuery, "k", true, true);
        assertQueryNoLeakCheck(replaceExpectedTimestamp(expected), viewName, "k", true, true);
    }

    private void testAlterRefreshParamsToManual(String initialRefreshType) throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            currentMicros = parseFloorPartialTimestamp("2020-12-12T00:00:00.000000Z");
            execute(
                    "create materialized view price_1h refresh " + initialRefreshType + " as " +
                            "select sym, last(price) as price, ts from base_price sample by 1h;"
            );

            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n",
                    "price_1h order by sym"
            );

            execute("alter materialized view price_1h set refresh manual;");
            drainWalQueue();

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-12-12T12:01')" +
                            ",('gbpusd', 1.323, '2024-12-12T12:02')" +
                            ",('jpyusd', 103.21, '2024-12-13T12:02')" +
                            ",('jpyusd', 1.321, '2024-12-13T13:02')"
            );

            // no refresh should happen even if we move the clock one day ahead
            currentMicros += Micros.DAY_MICROS;
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n",
                    "price_1h order by sym"
            );

            execute("refresh materialized view price_1h incremental;");
            drainQueues();

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-12-12T12:00:00.000000Z
                                    jpyusd\t103.21\t2024-12-13T12:00:00.000000Z
                                    jpyusd\t1.321\t2024-12-13T13:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );
            assertQueryNoLeakCheck(
                    """
                            view_name\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\ttimer_interval\ttimer_interval_unit\ttimer_time_zone\ttimer_start\tperiod_length\tperiod_length_unit\tperiod_delay\tperiod_delay_unit
                            price_1h\tbase_price\t2020-12-13T00:00:00.000000Z\t2020-12-13T00:00:00.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h;\tvalid\t0\t\t\t\t0\t\t0\t
                            """,
                    "select view_name, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, timer_interval, timer_interval_unit, timer_time_zone, timer_start, " +
                            "period_length, period_length_unit, period_delay, period_delay_unit " +
                            "from materialized_views",
                    null
            );
        });
    }

    private void testAlterRefreshParamsToTarget(String targetRefreshType, TestRefreshParams targetRefreshParams) throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            currentMicros = parseFloorPartialTimestamp("2020-12-12T00:00:00.000000Z");
            execute(
                    "create materialized view price_1h refresh manual deferred as " +
                            "select sym, last(price) as price, ts from base_price sample by 1h;"
            );

            drainQueues();

            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n",
                    "price_1h order by sym"
            );

            execute("alter materialized view price_1h set refresh " + targetRefreshType);
            drainWalQueue();

            execute("refresh materialized view price_1h incremental;");
            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2020-12-12T12:01')" +
                            ",('gbpusd', 1.323, '2020-12-12T13:02')" +
                            ",('jpyusd', 103.21, '2020-12-12T12:02')" +
                            ",('jpyusd', 1.321, '2020-12-12T13:02')"
            );
            currentMicros += 2 * Micros.DAY_MICROS;
            drainQueues();

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.32\t2020-12-12T12:00:00.000000Z
                                    gbpusd\t1.323\t2020-12-12T13:00:00.000000Z
                                    jpyusd\t103.21\t2020-12-12T12:00:00.000000Z
                                    jpyusd\t1.321\t2020-12-12T13:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );
            assertQueryNoLeakCheck(
                    """
                            view_name\tview_status
                            price_1h\tvalid
                            """,
                    "select view_name, view_status from materialized_views",
                    null
            );

            final TableToken viewToken = engine.getTableTokenIfExists("price_1h");
            Assert.assertNotNull(viewToken);
            final MatViewDefinition viewDefinition = engine.getMatViewGraph().getViewDefinition(viewToken);
            Assert.assertNotNull(viewDefinition);
            final MatViewState viewState = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertNotNull(viewState);
            Assert.assertSame(viewDefinition, viewState.getViewDefinition());

            Assert.assertEquals(targetRefreshParams.refreshType, viewDefinition.getRefreshType());
            Assert.assertEquals(targetRefreshParams.deferred, viewDefinition.isDeferred());
            Assert.assertEquals(targetRefreshParams.periodDelay, viewDefinition.getPeriodDelay());
            Assert.assertEquals(targetRefreshParams.periodDelayUnit, viewDefinition.getPeriodDelayUnit());
            Assert.assertEquals(targetRefreshParams.periodLength, viewDefinition.getPeriodLength());
            Assert.assertEquals(targetRefreshParams.periodLengthUnit, viewDefinition.getPeriodLengthUnit());
            Assert.assertEquals(targetRefreshParams.timerInterval, viewDefinition.getTimerInterval());
            Assert.assertEquals(targetRefreshParams.timerUnit, viewDefinition.getTimerUnit());
            Assert.assertEquals(targetRefreshParams.timerStartUs, viewDefinition.getTimerStartUs());
            Assert.assertEquals(targetRefreshParams.timerTimeZone, viewDefinition.getTimerTimeZone());
        });
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
                    """
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """,
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

    private void testEnableDedupWithSubsetKeys(String enableDedupSql) throws Exception {
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
                    """
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """,
                    "select view_name, base_table_name, view_status from materialized_views",
                    null,
                    false
            );

            execute(enableDedupSql);
            drainQueues();

            assertQueryNoLeakCheck(
                    """
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """,
                    "select view_name, base_table_name, view_status from materialized_views",
                    null,
                    false
            );
        });
    }

    private void testEstimateRowsPerBucket(long tableRows, long bucket, long partitionDuration, int partitionCount, long expectedLo, long expectedHi) {
        long result = MatViewRefreshJob.estimateRowsPerBucket(tableRows, bucket, partitionDuration, partitionCount);
        Assert.assertTrue("Expected from " + expectedLo + " to " + expectedHi + ", got " + result, result >= expectedLo && result < expectedHi);
    }

    private void testIncrementalRefreshTransactionLogV2(String viewSql) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
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
                    """
                            sequencerTxn\tminTimestamp\tmaxTimestamp
                            1\t2024-09-10T12:00:00.000000Z\t2024-09-18T19:00:00.000000Z
                            """,
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
                    """
                            sequencerTxn\tminTimestamp\tmaxTimestamp
                            1\t2024-09-10T12:00:00.000000Z\t2024-09-18T19:00:00.000000Z
                            2\t\t
                            3\t2024-09-10T12:00:00.000000Z\t2024-09-10T13:00:00.000000Z
                            """,
                    "select sequencerTxn, minTimestamp, maxTimestamp from wal_transactions('price_1h')",
                    null,
                    false
            );

            assertViewMatchesSqlOverBaseTable(viewSql);
        });
    }

    private void testPeriodRefresh(@NotNull String viewType, @Nullable String refreshType, boolean runTimerJob) throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            currentMicros = parseFloorPartialTimestamp("2000-01-01T00:00:00.000000Z");
            execute(
                    "create materialized view price_1h refresh " + viewType + " period (length 1d)  as " +
                            "select sym, last(price) as price, ts from base_price sample by 1d"
            );
            execute(
                    "insert into base_price(sym, price, ts) values ('gbpusd', 1.320, '1999-12-31T09:01')" +
                            ",('gbpusd', 1.320, '2000-01-01T12:01')" +
                            ",('gbpusd', 1.323, '2000-01-01T12:02')" +
                            ",('jpyusd', 103.21, '2000-01-01T12:02')" +
                            ",('gbpusd', 1.321, '2000-01-01T13:02')" +
                            ",('jpyusd', 103.21, '2000-01-02T01:00')" +
                            ",('gbpusd', 1.321, '2000-01-02T01:00')"
            );
            drainWalQueue();

            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);

            // only the 1999-12-31 period should be refreshed
            currentMicros = parseFloorPartialTimestamp("2000-01-01T00:00:00.000000Z");
            if (refreshType != null) {
                execute("refresh materialized view price_1h " + refreshType);
            }
            if (runTimerJob) {
                drainMatViewTimerQueue(timerJob);
            }
            drainQueues();
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.32\t1999-12-31T00:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );
            final String matViewsSql = "select view_name, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                    "view_sql, view_status, timer_time_zone, timer_start, " +
                    "period_length, period_length_unit, period_delay, period_delay_unit " +
                    "from materialized_views";
            assertQueryNoLeakCheck(
                    """
                            view_name\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\ttimer_time_zone\ttimer_start\tperiod_length\tperiod_length_unit\tperiod_delay\tperiod_delay_unit
                            price_1h\tbase_price\t2000-01-01T00:00:00.000000Z\t2000-01-01T00:00:00.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1d\tvalid\t\t2000-01-01T00:00:00.000000Z\t1\tDAY\t0\t
                            """,
                    matViewsSql,
                    null
            );

            // the second period still hasn't finished
            currentMicros = parseFloorPartialTimestamp("2000-01-01T23:59:59.999999Z");
            if (refreshType != null) {
                execute("refresh materialized view price_1h " + refreshType);
            }
            if (runTimerJob) {
                drainMatViewTimerQueue(timerJob);
            }
            drainQueues();
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.32\t1999-12-31T00:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );

            // the first period has finished - only half of the rows should be aggregated
            currentMicros = parseFloorPartialTimestamp("2000-01-02T00:00:00.000000Z");
            if (refreshType != null) {
                execute("refresh materialized view price_1h " + refreshType);
            }
            if (runTimerJob) {
                drainMatViewTimerQueue(timerJob);
            }
            drainQueues();
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.32\t1999-12-31T00:00:00.000000Z
                                    gbpusd\t1.321\t2000-01-01T00:00:00.000000Z
                                    jpyusd\t103.21\t2000-01-01T00:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );

            // finally, the second period has finished - all rows should be aggregated
            currentMicros = parseFloorPartialTimestamp("2000-01-03T00:00:01.000000Z");
            if (refreshType != null) {
                execute("refresh materialized view price_1h " + refreshType);
            }
            if (runTimerJob) {
                drainMatViewTimerQueue(timerJob);
            }
            drainQueues();

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.32\t1999-12-31T00:00:00.000000Z
                                    gbpusd\t1.321\t2000-01-01T00:00:00.000000Z
                                    gbpusd\t1.321\t2000-01-02T00:00:00.000000Z
                                    jpyusd\t103.21\t2000-01-01T00:00:00.000000Z
                                    jpyusd\t103.21\t2000-01-02T00:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );

            assertQueryNoLeakCheck(
                    """
                            view_name\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\ttimer_time_zone\ttimer_start\tperiod_length\tperiod_length_unit\tperiod_delay\tperiod_delay_unit
                            price_1h\tbase_price\t2000-01-03T00:00:01.000000Z\t2000-01-03T00:00:01.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1d\tvalid\t\t2000-01-01T00:00:00.000000Z\t1\tDAY\t0\t
                            """,
                    matViewsSql,
                    null
            );
        });
    }

    private void testPeriodWithTzRefresh(@NotNull String viewType, @Nullable String refreshType, boolean runTimerJob) throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            currentMicros = parseFloorPartialTimestamp("2020-01-01T00:00:00.000000Z");
            execute(
                    "create materialized view price_1h refresh " + viewType + " period (length 1d time zone 'Europe/Berlin' delay 1h) as " +
                            "select sym, last(price) as price, ts from base_price sample by 1d"
            );
            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2020-01-01T12:01')" +
                            ",('gbpusd', 1.323, '2020-01-01T12:02')" +
                            ",('jpyusd', 103.21, '2020-01-01T12:02')" +
                            ",('gbpusd', 1.321, '2020-01-01T13:02')" +
                            ",('jpyusd', 103.21, '2020-01-02T01:00')" +
                            ",('gbpusd', 1.321, '2020-01-02T01:00')"
            );

            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);

            // no refresh should happen as the first period hasn't finished
            currentMicros = parseFloorPartialTimestamp("2020-01-01T00:00:00.000000Z");
            if (refreshType != null) {
                execute("refresh materialized view price_1h " + refreshType);
            }
            if (runTimerJob) {
                drainMatViewTimerQueue(timerJob);
            }
            drainQueues();
            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n",
                    "price_1h order by sym"
            );
            final String matViewsSql = "select view_name, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                    "view_sql, view_status, timer_time_zone, timer_start, " +
                    "period_length, period_length_unit, period_delay, period_delay_unit " +
                    "from materialized_views";
            assertQueryNoLeakCheck(
                    """
                            view_name\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\ttimer_time_zone\ttimer_start\tperiod_length\tperiod_length_unit\tperiod_delay\tperiod_delay_unit
                            price_1h\tbase_price\t2020-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1d\tvalid\tEurope/Berlin\t2020-01-01T00:00:00.000000Z\t1\tDAY\t1\tHOUR
                            """,
                    matViewsSql,
                    null
            );

            // the first period still hasn't finished due to 1h delay
            currentMicros = parseFloorPartialTimestamp("2020-01-01T22:59:59.000000Z");
            if (refreshType != null) {
                execute("refresh materialized view price_1h " + refreshType);
            }
            if (runTimerJob) {
                drainMatViewTimerQueue(timerJob);
            }
            drainQueues();
            assertQueryNoLeakCheck(
                    "sym\tprice\tts\n",
                    "price_1h order by sym"
            );

            // the first period has finished - only half of the rows should be aggregated
            currentMicros = parseFloorPartialTimestamp("2020-01-02T00:00:00.000001Z");
            if (refreshType != null) {
                execute("refresh materialized view price_1h " + refreshType);
            }
            if (runTimerJob) {
                drainMatViewTimerQueue(timerJob);
            }
            drainQueues();
            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.321\t2020-01-01T00:00:00.000000Z
                                    jpyusd\t103.21\t2020-01-01T00:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );

            // finally, the second period has finished - all rows should be aggregated
            currentMicros = parseFloorPartialTimestamp("2020-01-03T00:00:01.000000Z");
            if (refreshType != null) {
                execute("refresh materialized view price_1h " + refreshType);
            }
            if (runTimerJob) {
                drainMatViewTimerQueue(timerJob);
            }
            drainQueues();

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.321\t2020-01-01T00:00:00.000000Z
                                    gbpusd\t1.321\t2020-01-02T00:00:00.000000Z
                                    jpyusd\t103.21\t2020-01-01T00:00:00.000000Z
                                    jpyusd\t103.21\t2020-01-02T00:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );

            assertQueryNoLeakCheck(
                    """
                            view_name\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\ttimer_time_zone\ttimer_start\tperiod_length\tperiod_length_unit\tperiod_delay\tperiod_delay_unit
                            price_1h\tbase_price\t2020-01-03T00:00:01.000000Z\t2020-01-03T00:00:01.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1d\tvalid\tEurope/Berlin\t2020-01-01T00:00:00.000000Z\t1\tDAY\t1\tHOUR
                            """,
                    matViewsSql,
                    null
            );
        });
    }

    private void testTimerMatViewBigJumps(String timeZone, String start, String initialClock, long clockJump) throws Exception {
        assertMemoryLeak(() -> {
            final TimeZoneRules tzRules = timeZone != null ? Micros.getTimezoneRules(DateLocaleFactory.EN_LOCALE, timeZone) : null;

            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            if (timeZone != null) {
                execute(
                        "create materialized view price_1h refresh every h start '" + start + "' time zone '" + timeZone + "' as (" +
                                "select sym, last(price) as price, ts from base_price sample by 1h" +
                                ") partition by day"
                );
            } else {
                execute(
                        "create materialized view price_1h refresh every 1h start '" + start + "' as (" +
                                "select sym, last(price) as price, ts from base_price sample by 1h" +
                                ") partition by day"
                );
            }

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            long initialMicros = parseFloorPartialTimestamp(initialClock);
            if (tzRules != null) {
                initialMicros += tzRules.getOffset(initialMicros);
            }
            currentMicros = initialMicros;
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );
            final StringSink tsSink = new StringSink();
            MicrosFormatUtils.appendDateTimeUSec(tsSink, currentMicros);
            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn\ttimer_time_zone\ttimer_start\ttimer_interval\ttimer_interval_unit\n" +
                            "price_1h\ttimer\tbase_price\t" + tsSink + "\t" + tsSink + "\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t1\t1\t" + (timeZone != null ? timeZone : "") + "\t" + start + "\t1\tHOUR\n",
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, refresh_base_table_txn, base_table_txn, " +
                            "timer_time_zone, timer_start, timer_interval, timer_interval_unit " +
                            "from materialized_views",
                    null
            );

            execute("insert into base_price(sym, price, ts) values('jpyusd', 104.57, '2024-09-10T13:02')");

            currentMicros += clockJump;
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    jpyusd\t104.57\t2024-09-10T13:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );
        });
    }

    private void testTimerMatViewSmallJumps(String timeZone, String start, String every, String initialClock, long clockJump, int ticksBeforeRefresh) throws Exception {
        assertMemoryLeak(() -> {
            final TimeZoneRules tzRules = timeZone != null ? Micros.getTimezoneRules(DateLocaleFactory.EN_LOCALE, timeZone) : null;
            final int interval = CommonUtils.getStrideMultiple(every, 0);
            final char unit = CommonUtils.getStrideUnit(every, -1);
            final String unitStr = MatViewsFunctionFactory.getIntervalUnit(unit);

            executeWithRewriteTimestamp(
                    "create table base_price (" +
                            "sym varchar, price double, ts #TIMESTAMP" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            if (timeZone != null) {
                execute(
                        "create materialized view price_1h refresh every " + every + " deferred start '" + start + "' time zone '" + timeZone + "' as (" +
                                "select sym, last(price) as price, ts from base_price sample by 1h" +
                                ") partition by day"
                );
            } else {
                execute(
                        "create materialized view price_1h refresh every " + every + " deferred start '" + start + "' as (" +
                                "select sym, last(price) as price, ts from base_price sample by 1h" +
                                ") partition by day"
                );
            }

            execute(
                    "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                            ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            long initialMicros = parseFloorPartialTimestamp(initialClock);
            if (tzRules != null) {
                initialMicros += tzRules.getOffset(initialMicros);
            }
            currentMicros = initialMicros;
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);

            for (int i = 0; i < ticksBeforeRefresh; i++) {
                drainMatViewTimerQueue(timerJob);
                drainQueues();

                assertQueryNoLeakCheck(
                        "sym\tprice\tts\n",
                        "price_1h order by sym"
                );
                assertQueryNoLeakCheck(
                        """
                                view_name\tbase_table_name\tview_status\tinvalidation_reason
                                price_1h\tbase_price\tvalid\t
                                """,
                        "select view_name, base_table_name, view_status, invalidation_reason from materialized_views",
                        null
                );

                currentMicros += clockJump;
            }

            currentMicros += clockJump;
            drainMatViewTimerQueue(timerJob);
            drainQueues();

            assertQueryNoLeakCheck(
                    replaceExpectedTimestamp(
                            """
                                    sym\tprice\tts
                                    gbpusd\t1.323\t2024-09-10T12:00:00.000000Z
                                    gbpusd\t1.321\t2024-09-10T13:00:00.000000Z
                                    jpyusd\t103.21\t2024-09-10T12:00:00.000000Z
                                    """),
                    "price_1h order by sym"
            );
            final StringSink tsSink = new StringSink();
            MicrosTimestampDriver.INSTANCE.append(tsSink, currentMicros);
            assertQueryNoLeakCheck(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_sql\tview_status\trefresh_base_table_txn\tbase_table_txn\ttimer_time_zone\ttimer_start\ttimer_interval\ttimer_interval_unit\n" +
                            "price_1h\ttimer\tbase_price\t" + tsSink + "\t" + tsSink + "\tselect sym, last(price) as price, ts from base_price sample by 1h\tvalid\t1\t1\t" + (timeZone != null ? timeZone : "") + "\t" + start + "\t" + interval + "\t" + unitStr + "\n",
                    "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                            "view_sql, view_status, refresh_base_table_txn, base_table_txn, " +
                            "timer_time_zone, timer_start, timer_interval, timer_interval_unit " +
                            "from materialized_views",
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

    private static class TestRefreshParams {
        boolean deferred;
        int periodDelay;
        char periodDelayUnit;
        int periodLength;
        char periodLengthUnit;
        int refreshType;
        int timerInterval;
        long timerStartUs = Numbers.LONG_NULL;
        String timerTimeZone;
        char timerUnit;

        TestRefreshParams ofDeferred() {
            deferred = true;
            return this;
        }

        TestRefreshParams ofImmediate() {
            refreshType = MatViewDefinition.REFRESH_TYPE_IMMEDIATE;
            return this;
        }

        TestRefreshParams ofManual() {
            refreshType = MatViewDefinition.REFRESH_TYPE_MANUAL;
            return this;
        }

        TestRefreshParams ofPeriod() throws NumericException {
            refreshType = MatViewDefinition.REFRESH_TYPE_IMMEDIATE;
            this.periodLength = 12;
            this.periodLengthUnit = 'h';
            this.periodDelay = 1;
            this.periodDelayUnit = 'h';
            this.timerStartUs = MicrosTimestampDriver.INSTANCE.parseFloorLiteral("2020-12-12T00:00:00.000000Z");
            this.timerTimeZone = "Europe/London";
            return this;
        }

        TestRefreshParams ofTimer() {
            refreshType = MatViewDefinition.REFRESH_TYPE_TIMER;
            this.timerInterval = 42;
            this.timerUnit = 'm';
            this.timerStartUs = 0;
            this.timerTimeZone = "Europe/Sofia";
            return this;
        }
    }
}
