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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.questdb.griffin.model.IntervalUtils.parseFloorPartialTimestamp;


public class MatViewTest extends AbstractCairoTest {

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
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ENABLED, "true");
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void testBaseTableRename() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (" +
                    "sym varchar, price double, ts timestamp" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute("insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                    ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                    ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                    ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );
            drainWalQueue();

            currentMicros = parseFloorPartialTimestamp("2024-10-24T17:22:09.842574Z");
            MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine);
            refreshJob.run(0);
            drainWalQueue();

            assertSql(
                    "name\tbase_table_name\tlast_refresh_timestamp\trefresh_pending\tview_sql\tview_table_dir_name\tlast_error\tlast_error_code\n" +
                            "price_1h\tbase_price\t2024-10-24T17:22:09.842574Z\tfalse\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tnull\n",
                    "views"
            );

            execute("rename table base_price to base_price2");
            assertSql("refresh_mat_view\n" +
                    "true\n", "select refresh_mat_view('price_1h')");
            currentMicros = parseFloorPartialTimestamp("2024-10-24T18");
            refreshJob.run(0);
            drainWalQueue();

            assertSql(
                    "name\tbase_table_name\tlast_refresh_timestamp\trefresh_pending\tview_sql\tview_table_dir_name\tlast_error\tlast_error_code\n" +
                            "price_1h\tbase_price\t2024-10-24T18:00:00.000000Z\tfalse\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\ttable does not exist [table=base_price]\t-105\n",
                    "views"
            );

            // Create another base table instead of the one that was renamed
            execute("create table base_price (" +
                    "sym varchar, price double, ts timestamp" +
                    ") timestamp(ts) partition by DAY BYPASS WAL"
            );
            assertSql("refresh_mat_view\n" +
                    "true\n", "select refresh_mat_view('price_1h')");
            currentMicros = parseFloorPartialTimestamp("2024-10-24T19");
            refreshJob.run(0);
            drainWalQueue();

            assertSql(
                    "name\tbase_table_name\tlast_refresh_timestamp\trefresh_pending\tview_sql\tview_table_dir_name\tlast_error\tlast_error_code\n" +
                            "price_1h\tbase_price\t2024-10-24T19:00:00.000000Z\tfalse\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\tBase table is not WAL table\tnull\n",
                    "views"
            );
        });
    }

    @Test
    public void testCheckMatViewModification() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (" +
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
            execute("create table base_price (" +
                    "sym varchar, price double, ts timestamp" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");
            TableToken matViewToken1 = engine.verifyTableName("price_1h");

            execute("insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                    ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                    ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                    ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );

            drainWalQueue();

            MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine);
            refreshJob.run(0);
            drainWalQueue();

            assertSql(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h order by ts, sym"
            );

            dropMatView();
            refreshJob.run(0);

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");
            TableToken matViewToken2 = engine.verifyTableName("price_1h");
            refreshJob.run(0);
            drainWalQueue();

            assertSql(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h order by ts, sym"
            );

            Assert.assertNull(engine.getMaterializedViewGraph().getViewRefreshState(matViewToken1));
            Assert.assertNotNull(engine.getMaterializedViewGraph().getViewRefreshState(matViewToken2));
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

            drainWalQueue();

            MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine);
            refreshJob.run(0);
            drainWalQueue();

            assertSql(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h order by ts, sym"
            );

            // mat view should be deleted
            execute("drop all;");

            refreshJob.run(0);
            drainWalQueue();

            assertSql(
                    "count\n" +
                            "0\n",
                    "select count() from views();"
            );
            assertSql(
                    "count\n" +
                            "0\n",
                    "select count() from tables();"
            );

            Assert.assertNull(engine.getMaterializedViewGraph().getViewRefreshState(matViewToken));
        });
    }

    @Test
    public void testIncrementalRefresh() throws Exception {
        testIncrementalRefresh0("select sym, last(price) as price, ts from base_price sample by 1h");
    }

    @Test
    public void testIncrementalRefreshWithViewWhereClauseSymbolFilters() throws Exception {
        testIncrementalRefresh0("select sym, last(price) as price, ts from base_price " +
                "WHERE sym = 'gbpusd' or sym = 'jpyusd' " +
                "sample by 1h");
    }

    @Test
    public void testIncrementalRefreshWithViewWhereClauseTimestampFilters() throws Exception {
        testIncrementalRefresh0("select sym, last(price) price, ts from base_price " +
                "WHERE ts > 0 or ts < '2040-01-01' " +
                "sample by 1h");
    }

    @Test
    public void testSimpleRefresh() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (" +
                    "sym varchar, price double, ts timestamp" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute("insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                    ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                    ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                    ",('gbpusd', 1.321, '2024-09-10T13:02')"
            );
            drainWalQueue();

            MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine);
            refreshJob.run(0);
            drainWalQueue();

            assertSql(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h order by ts, sym"
            );

            execute("insert into base_price values('gbpusd', 1.319, '2024-09-10T12:05')" +
                    ",('gbpusd', 1.325, '2024-09-10T13:03')"
            );
            drainWalQueue();

            refreshJob.run(0);
            drainWalQueue();

            String expected = "sym\tprice\tts\n" +
                    "gbpusd\t1.319\t2024-09-10T12:00:00.000000Z\n" +
                    "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                    "gbpusd\t1.325\t2024-09-10T13:00:00.000000Z\n";

            assertSql(expected, "select sym, last(price) as price, ts from base_price sample by 1h order by ts, sym");
            assertSql(expected, "price_1h order by ts, sym");
        });
    }

    @Test
    public void testSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (" +
                    "  sym varchar, price double, ts timestamp" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            final String viewSql = "select sym0, last(price0) price, ts0 " +
                    "from (select ts as ts0, sym as sym0, price as price0 from base_price) " +
                    "sample by 1h";

            createMatView(viewSql);
            execute("insert into base_price " +
                    "select 'gbpusd', 1.320 + x / 1000.0, timestamp_sequence('2024-09-10T12:02', 1000000*60*5) " +
                    "from long_sequence(24 * 20 * 5)"
            );
            drainWalQueue();

            MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine);
            refreshJob.run(0);
            drainWalQueue();

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

    private void dropMatView() throws SqlException {
        execute("drop materialized view price_1h;");
    }

    private void testIncrementalRefresh0(String viewSql) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
        assertMemoryLeak(() -> {
            execute("create table base_price (" +
                    "sym varchar, price double, ts timestamp" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            createMatView(viewSql);
            execute("insert into base_price " +
                    "select 'gbpusd', 1.320 + x / 1000.0, timestamp_sequence('2024-09-10T12:02', 1000000*60*5) " +
                    "from long_sequence(24 * 20 * 5)"
            );
            drainWalQueue();

            MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine);
            refreshJob.run(0);

            assertSql(
                    "sequencerTxn\tminTimestamp\tmaxTimestamp\n" +
                            "1\t2024-09-10T12:00:00.000000Z\t2024-09-18T19:00:00.000000Z\n",
                    "select sequencerTxn, minTimestamp, maxTimestamp from wal_transactions('price_1h')"
            );

            execute("insert into base_price values('gbpusd', 1.319, '2024-09-10T12:05')" +
                    ",('gbpusd', 1.325, '2024-09-10T13:03')"
            );
            drainWalQueue();

            refreshJob.run(0);
            drainWalQueue();

            assertSql(
                    "sequencerTxn\tminTimestamp\tmaxTimestamp\n" +
                            "1\t2024-09-10T12:00:00.000000Z\t2024-09-18T19:00:00.000000Z\n" +
                            "2\t2024-09-10T12:00:00.000000Z\t2024-09-10T13:00:00.000000Z\n",
                    "select sequencerTxn, minTimestamp, maxTimestamp from wal_transactions('price_1h')"
            );

            assertViewMatchesSqlOverBaseTable(viewSql);
        });
    }
}
