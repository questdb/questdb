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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Files;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.cairo.TableUtils.DETACHED_DIR_MARKER;
import static io.questdb.griffin.model.IntervalUtils.parseFloorPartialTimestamp;


public class MatViewTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // override default to test copy
        inputRoot = TestUtils.getCsvRoot();
        inputWorkRoot = TestUtils.unchecked(() -> temp.newFolder("imports" + System.nanoTime()).getAbsolutePath());
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ENABLED, "true");
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ENABLED, "true");
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
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
        testBaseTableInvalidateOnOperation("alter table base_price dedup enable upsert keys(ts);", "enable deduplication operation");
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
    public void testBaseTableRename() throws Exception {
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

            assertSql(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tbase_table_txn\tapplied_base_table_txn\n" +
                            "price_1h\tincremental\tbase_price\t2024-10-24T17:22:09.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t1\t1\n",
                    "mat_views"
            );

            execute("rename table base_price to base_price2");
            execute("refresh materialized view 'price_1h' full;");
            currentMicros = parseFloorPartialTimestamp("2024-10-24T18");
            drainQueues();

            assertSql(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tbase_table_txn\tapplied_base_table_txn\n" +
                            "price_1h\tincremental\tbase_price\t2024-10-24T18:00:00.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\ttable does not exist [table=base_price]\tinvalid\t1\t-1\n",
                    "mat_views"
            );

            // Create another base table instead of the one that was renamed.
            // This table is non-WAL, so mat view should be still invalid.
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY BYPASS WAL"
            );
            execute("refresh materialized view 'price_1h' full;");
            currentMicros = parseFloorPartialTimestamp("2024-10-24T19");
            drainQueues();

            assertSql(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tbase_table_txn\tapplied_base_table_txn\n" +
                            "price_1h\tincremental\tbase_price\t2024-10-24T19:00:00.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\tbase table is not a WAL table\tinvalid\t1\t-1\n",
                    "mat_views"
            );
        });
    }

    @Test
    public void testBaseTableRenameAndThenRenameBack_doNotDrainWAL() throws Exception {
        testBaseTableRenameAndThenRenameBack(false);
    }

    @Test
    public void testBaseTableRenameAndThenRenameBack_drainWAL() throws Exception {
        testBaseTableRenameAndThenRenameBack(true);
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

            assertSql(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tbase_table_txn\tapplied_base_table_txn\n" +
                            "price_1h\tincremental\tbase_price\t2024-10-24T17:22:09.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~3\t\tvalid\t1\t1\n",
                    "mat_views"
            );

            // Swap the tables with each other.
            execute("rename table base_price to base_price_tmp");
            execute("rename table base_price2 to base_price");
            execute("rename table base_price_tmp to base_price2");
            currentMicros = parseFloorPartialTimestamp("2024-10-24T18");
            drainQueues();

            assertSql(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tbase_table_txn\tapplied_base_table_txn\n" +
                            "price_1h\tincremental\tbase_price\t2024-10-24T17:22:09.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~3\ttable rename operation\tinvalid\t1\t1\n",
                    "mat_views"
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

            assertSql(
                    "view_name\tview_status\n" +
                            "price_1h\tvalid\n",
                    "select view_name, view_status from mat_views"
            );
        });
    }

    @Test
    public void testBatchInsert() throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_INSERT_AS_SELECT_BATCH_SIZE, "10");
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");

            execute("insert into base_price select concat('sym', x), x, x::timestamp from long_sequence(30);");

            drainQueues();

            assertSql(
                    "sym\tprice\tts\n" +
                            "sym1\t1.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym10\t10.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym11\t11.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym12\t12.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym13\t13.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym14\t14.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym15\t15.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym16\t16.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym17\t17.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym18\t18.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym19\t19.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym2\t2.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym20\t20.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym21\t21.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym22\t22.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym23\t23.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym24\t24.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym25\t25.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym26\t26.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym27\t27.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym28\t28.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym29\t29.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym3\t3.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym30\t30.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym4\t4.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym5\t5.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym6\t6.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym7\t7.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym8\t8.0\t1970-01-01T00:00:00.000000Z\n" +
                            "sym9\t9.0\t1970-01-01T00:00:00.000000Z\n",
                    "price_1h order by ts, sym"
            );

            // Expect 3 (30 rows / 10 rows per batch) commits.
            assertSql(
                    "writerTxn\tsequencerTxn\n" +
                            "3\t3\n",
                    "select writerTxn, sequencerTxn from wal_tables() where name = 'price_1h'"
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

            assertSql(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h order by ts, sym"
            );

            dropMatView();
            drainQueues();

            createMatView("select sym, last(price) as price, ts from base_price sample by 1h");
            TableToken matViewToken2 = engine.verifyTableName("price_1h");
            drainQueues();

            assertSql(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h order by ts, sym"
            );

            Assert.assertNull(engine.getMatViewGraph().getViewRefreshState(matViewToken1));
            Assert.assertNotNull(engine.getMatViewGraph().getViewRefreshState(matViewToken2));
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

            assertSql(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h order by ts, sym"
            );

            // mat view should be deleted
            execute("drop all;");

            drainQueues();

            assertSql(
                    "count\n" +
                            "0\n",
                    "select count() from mat_views();"
            );
            assertSql(
                    "count\n" +
                            "0\n",
                    "select count() from tables();"
            );

            Assert.assertNull(engine.getMatViewGraph().getViewRefreshState(matViewToken));
        });
    }

    @Test
    public void testEnableDedupWithFewerKeysDoesNotInvalidateMatViews() throws Exception {
        testEnableDedupWithSubsetKeys("alter table base_price dedup enable upsert keys(ts);", false);
    }

    @Test
    public void testEnableDedupWithMoreKeysInvalidatesMatViews() throws Exception {
        testEnableDedupWithSubsetKeys("alter table base_price dedup enable upsert keys(ts, amount);", true);
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

            assertSql(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tbase_table_txn\tapplied_base_table_txn\n" +
                            "price_1h\tincremental\tbase_price\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t1\t1\n",
                    "mat_views"
            );

            final String expected = "sym\tprice\tts\n" +
                    "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                    "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                    "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n";
            assertSql(expected, "price_1h order by sym");

            execute("alter table base_price drop column extra_col");
            drainQueues();

            assertSql(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tbase_table_txn\tapplied_base_table_txn\n" +
                            "price_1h\tincremental\tbase_price\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\tdrop column operation\tinvalid\t1\t2\n",
                    "mat_views"
            );

            execute("refresh materialized view price_1h full");
            drainQueues();

            assertSql(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tbase_table_txn\tapplied_base_table_txn\n" +
                            "price_1h\tincremental\tbase_price\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t2\t2\n",
                    "mat_views"
            );
            assertSql(expected, "price_1h order by sym");
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
            // this statement will notify refresh job before the WAL apply job,
            // but technically that's redundant
            execute("refresh materialized view price_1h incremental");
            drainQueues();

            assertSql(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tbase_table_txn\tapplied_base_table_txn\n" +
                            "price_1h\tincremental\tbase_price\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t1\t1\n",
                    "mat_views"
            );

            assertSql(
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
            String viewName = "x_view";
            String out = "select k, s, lat, lon";
            String viewQuery = "select k, s, first(lat) lat, last(lon) lon " +
                    "from x " +
                    "where s in ('a') " +
                    "sample by 1h align to calendar";

            long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-03-27T23:30:00.000000Z");
            long step = 100000000L;
            final int N = 100;
            final int K = 5;
            String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b',null) s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            String expected = "k\ts\tlat\tlon\n" +
                    "2021-03-27T23:00:00.000000Z\ta\t142.30215575416736\t165.69007104574442\n" +
                    "2021-03-28T00:00:00.000000Z\ta\t106.0418967098362\tnull\n" +
                    "2021-03-28T01:00:00.000000Z\ta\t79.9245166429184\t168.04971262491318\n" +
                    "2021-03-28T02:00:00.000000Z\ta\t6.612327943200507\t128.42101395467057\n";

            assertSql(expected, outSelect(out, viewQuery));
            assertSql(expected, outSelect(out, viewName));
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
            String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a') s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            String expected = "k\ts\tlat\tlon\n" +
                    "2021-03-28T01:00:00.000000Z\ta\t144.77803379943109\t15.276535618609202\n" +
                    "2021-03-28T03:00:00.000000Z\ta\tnull\t127.43011035722469\n" +
                    "2021-03-28T04:00:00.000000Z\ta\t60.30746433578906\t128.42101395467057\n";

            assertSql(expected, outSelect(out, viewQuery));
            assertSql(expected, outSelect(out, viewName));
        });
    }

    @Test
    public void testIndexSampleByAlignToCalendarDSTForwardEdge2() throws Exception {
        assertMemoryLeak(() -> {
            String viewName = "x_view";
            String out = "select to_timezone(k, 'Europe/Berlin') k, s, lat, lon";
            String viewQuery = "select k, s, first(lat) lat, last(lon) lon " +
                    "from x " +
                    "where s in ('a') " +
                    "sample by 1h align to calendar time zone 'Europe/Berlin'";

            long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-03-28T01:00:00.000000Z");
            long step = 60 * 1000000L;
            final int N = 100;
            final int K = 5;
            String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a') s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            String expected = "k\ts\tlat\tlon\n" +
                    "2021-03-28T03:00:00.000000Z\ta\t144.77803379943109\tnull\n" +
                    "2021-03-28T04:00:00.000000Z\ta\t98.27279585461298\t128.42101395467057\n";

            assertSql(expected, outSelect(out, viewQuery));
            assertSql(expected, outSelect(out, viewName));
        });
    }

    @Test
    public void testIndexSampleByAlignToCalendarDSTForwardEdge3() throws Exception {
        assertMemoryLeak(() -> {
            String viewName = "x_view";
            String out = "select to_timezone(k, 'Europe/Berlin') k, s, lat, lon";
            String viewQuery = "select k, s, first(lat) lat, last(lon) lon " +
                    "from x " +
                    "where s in ('a') " +
                    "sample by 1h align to calendar time zone 'Europe/Berlin'";

            long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-03-28T01:59:00.000000Z");
            long step = 60 * 1000000L;
            final int N = 100;
            final int K = 5;
            String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a') s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            String expected = "k\ts\tlat\tlon\n" +
                    "2021-03-28T03:00:00.000000Z\ta\t144.77803379943109\t15.276535618609202\n" +
                    "2021-03-28T04:00:00.000000Z\ta\tnull\t127.43011035722469\n" +
                    "2021-03-28T05:00:00.000000Z\ta\t60.30746433578906\t128.42101395467057\n";

            assertSql(expected, outSelect(out, viewQuery));
            assertSql(expected, outSelect(out, viewName));
        });
    }

    @Test
    public void testIndexSampleByAlignToCalendarDSTForwardLocalMidnight() throws Exception {
        assertMemoryLeak(() -> {
            String viewName = "x_view";
            String out = "select to_timezone(k, 'Europe/Berlin') k, s, lat, lon";
            String viewQuery = "select k, s, first(lat) lat, last(lon) lon " +
                    "from x " +
                    "where s in ('a') " +
                    "sample by 1h align to calendar time zone 'Europe/Berlin'";

            long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-03-27T23:01:00.000000Z");
            long step = 60 * 1000000L;
            final int N = 100;
            final int K = 5;
            String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b',null) s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);
            String expected = "k\ts\tlat\tlon\n" +
                    "2021-03-28T00:00:00.000000Z\ta\t142.30215575416736\t167.4566019970139\n" +
                    "2021-03-28T01:00:00.000000Z\ta\t33.45558404694713\t128.42101395467057\n";

            assertSql(expected, outSelect(out, viewQuery));
            assertSql(expected, outSelect(out, viewName));
        });
    }

    @Test
    public void testIndexSampleByAlignToCalendarWithTimezoneBerlinShiftBack() throws Exception {
        assertMemoryLeak(() -> {
            String viewName = "x_view";
            String out = "select to_timezone(k, 'Europe/Berlin'), k, s, lat, lon";
            String viewQuery = "select k, s, first(lat) lat, last(k) lon " + //TODO(eugene): last(k) or last(lon) ?
                    "from x " +
                    "where s in ('a') " +
                    "sample by 1d align to calendar time zone 'Europe/Berlin'";

            long startTs = TimestampFormatUtils.parseUTCTimestamp("2020-10-23T20:30:00.000000Z");
            long step = 50 * 60 * 1000000L;
            final int N = 120;
            final int K = 5;
            String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b',null) s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            String expected = "to_timezone\tk\ts\tlat\tlon\n" +
                    "2020-10-24T00:00:00.000000Z\t2020-10-23T22:00:00.000000Z\ta\t142.30215575416736\t2020-10-24T19:50:00.000000Z\n" +
                    "2020-10-25T00:00:00.000000Z\t2020-10-24T22:00:00.000000Z\ta\tnull\t2020-10-25T20:00:00.000000Z\n" +
                    "2020-10-26T00:00:00.000000Z\t2020-10-25T23:00:00.000000Z\ta\t33.45558404694713\t2020-10-26T21:50:00.000000Z\n" +
                    "2020-10-27T00:00:00.000000Z\t2020-10-26T23:00:00.000000Z\ta\t6.612327943200507\t2020-10-27T22:00:00.000000Z\n" +
                    "2020-10-28T00:00:00.000000Z\t2020-10-27T23:00:00.000000Z\ta\tnull\t2020-10-27T23:40:00.000000Z\n";

            assertSql(expected, outSelect(out, viewQuery));
            assertSql(expected, outSelect(out, viewName));
        });
    }

    @Test
    public void testIndexSampleByAlignToCalendarWithTimezoneBerlinShiftBackHourlyWithOffset() throws Exception {
        assertMemoryLeak(() -> {
            String viewName = "x_view";
            String out = "select to_timezone(k, 'Europe/Berlin'), k, s, lat, lon";
            String viewQuery = "select k, s, first(lat) lat, last(k) lon " + //TODO(eugene): last(k) or last(lon) ?
                    "from x " +
                    "where s in ('a') and k between '2021-03-27 21:00' and  '2021-03-28 04:00'" +
                    "sample by 1h align to calendar time zone 'Europe/Berlin' with offset '00:15'";

            long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-03-26T20:30:00.000000Z");
            long step = 13 * 60 * 1000000L;
            final int N = 1000;
            final int K = 25;
            String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b') s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);
            String expected = "to_timezone\tk\ts\tlat\tlon\n" +
                    "2021-03-27T21:15:00.000000Z\t2021-03-27T20:15:00.000000Z\ta\t132.09083798490755\t2021-03-27T21:12:00.000000Z\n" +
                    "2021-03-27T22:15:00.000000Z\t2021-03-27T21:15:00.000000Z\ta\t179.5841357536068\t2021-03-27T21:51:00.000000Z\n" +
                    "2021-03-27T23:15:00.000000Z\t2021-03-27T22:15:00.000000Z\ta\t77.68770182183965\t2021-03-27T22:56:00.000000Z\n" +
                    "2021-03-28T00:15:00.000000Z\t2021-03-27T23:15:00.000000Z\ta\tnull\t2021-03-27T23:48:00.000000Z\n" +
                    "2021-03-28T01:15:00.000000Z\t2021-03-28T00:15:00.000000Z\ta\t3.6703591550328163\t2021-03-28T01:06:00.000000Z\n" +
                    "2021-03-28T03:15:00.000000Z\t2021-03-28T01:15:00.000000Z\ta\tnull\t2021-03-28T02:11:00.000000Z\n" +
                    "2021-03-28T04:15:00.000000Z\t2021-03-28T02:15:00.000000Z\ta\tnull\t2021-03-28T02:37:00.000000Z\n" +
                    "2021-03-28T05:15:00.000000Z\t2021-03-28T03:15:00.000000Z\ta\t38.20430552091481\t2021-03-28T03:16:00.000000Z\n";

            assertSql(expected, outSelect(out, viewQuery));
            assertSql(expected, outSelect(out, viewName));
        });
    }

    @Test
    public void testIndexSampleByAlignToCalendarWithTimezoneBerlinShiftForward() throws Exception {
        assertMemoryLeak(() -> {
            String viewName = "x_view";
            String out = "select to_timezone(k, 'Europe/Berlin'), k, s, lat, lon";
            String viewQuery = "select k, s, first(lat) lat, last(k) lon " + //TODO(eugene): last(k) or last(lon) ?
                    "from x " +
                    "where s in ('a') " +
                    "sample by 1d align to calendar time zone 'Europe/Berlin'";

            long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-03-25T23:30:00.000000Z");
            long step = 50 * 60 * 1000000L;
            final int N = 120;
            final int K = 5;
            String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b',null) s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);
            String expected = "to_timezone\tk\ts\tlat\tlon\n" +
                    "2021-03-26T00:00:00.000000Z\t2021-03-25T23:00:00.000000Z\ta\t142.30215575416736\t2021-03-26T22:50:00.000000Z\n" +
                    "2021-03-27T00:00:00.000000Z\t2021-03-26T23:00:00.000000Z\ta\tnull\t2021-03-27T22:10:00.000000Z\n" +
                    "2021-03-28T00:00:00.000000Z\t2021-03-27T23:00:00.000000Z\ta\t109.94209864193589\t2021-03-28T20:40:00.000000Z\n" +
                    "2021-03-29T00:00:00.000000Z\t2021-03-28T22:00:00.000000Z\ta\t70.00560222114518\t2021-03-29T16:40:00.000000Z\n" +
                    "2021-03-30T00:00:00.000000Z\t2021-03-29T22:00:00.000000Z\ta\t13.290235514836048\t2021-03-30T02:40:00.000000Z\n";

            assertSql(expected, outSelect(out, viewQuery));
            assertSql(expected, outSelect(out, viewName));
        });
    }

    @Test
    @Ignore
    public void testIndexSampleByAlignToCalendarWithTimezoneLondonShiftBack() throws Exception {
        assertMemoryLeak(() -> {
            String viewName = "x_view";
            String out = "select to_timezone(k, 'Europe/London'), k, s, lat, lon";
            String viewQuery = "select k, s, first(lat) lat, last(k) lon " + //TODO(eugene): last(k) or last(lon) ?
                    "from x " +
                    "where s in ('a') " +
                    "sample by 1d align to calendar time zone 'Europe/London'";

            long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-03-25T23:30:00.000000Z");
            long step = 50 * 60 * 1000000L;
            final int N = 120;
            final int K = 5;
            String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b',null) s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            String expected = "to_timezone\tk\ts\tlat\tlon\n" +
                    "2021-03-26T00:00:00.000000Z\t2021-03-26T00:00:00.000000Z\ta\t142.30215575416736\t2021-03-26T22:50:00.000000Z\n" +
                    "2021-03-27T00:00:00.000000Z\t2021-03-27T00:00:00.000000Z\ta\tnull\t2021-03-27T23:00:00.000000Z\n" +
                    "2021-03-28T00:00:00.000000Z\t2021-03-28T00:00:00.000000Z\ta\t33.45558404694713\t2021-03-28T20:40:00.000000Z\n" +
                    "2021-03-29T00:00:00.000000Z\t2021-03-28T23:00:00.000000Z\ta\t70.00560222114518\t2021-03-29T16:40:00.000000Z\n" +
                    "2021-03-30T00:00:00.000000Z\t2021-03-29T23:00:00.000000Z\ta\t13.290235514836048\t2021-03-30T02:40:00.000000Z\n";

            assertSql(expected, outSelect(out, viewQuery));
            assertSql(expected, outSelect(out, viewName));
        });
    }

    @Test
    @Ignore
    public void testIndexSampleByAlignToCalendarWithTimezoneLondonShiftForwardHourly() throws Exception {
        assertMemoryLeak(() -> {
            String viewName = "x_view";
            String out = "select to_timezone(k, 'Europe/London'), k, s, lat, lastk";
            String viewQuery = "select k, s, first(lat) lat, last(k) lastk " +
                    "from x " +
                    "where s in ('a') and k between '2020-10-24 21:00:00' and '2020-10-25 05:00:00'" +
                    "sample by 1h align to calendar time zone 'Europe/London'";

            long startTs = TimestampFormatUtils.parseUTCTimestamp("2020-10-23T20:30:00.000000Z");
            long step = 259 * 1000000L;
            final int N = 1000;
            final int K = 25;
            String columns = " rnd_double(1)*180 lat, rnd_double(1)*180 lon, rnd_symbol('a','b') s,";
            updateViewIncrementally(viewName, viewQuery, columns, "s", startTs, step, N, K);

            String expected = "to_timezone\tk\ts\tlat\tlastk\n" +
                    "2020-10-24T22:00:00.000000Z\t2020-10-24T21:00:00.000000Z\ta\t154.93777586404912\t2020-10-24T21:49:28.000000Z\n" +
                    "2020-10-24T23:00:00.000000Z\t2020-10-24T22:00:00.000000Z\ta\t43.799859246867385\t2020-10-24T22:54:13.000000Z\n" +
                    "2020-10-25T00:00:00.000000Z\t2020-10-24T23:00:00.000000Z\ta\t38.34194069380561\t2020-10-24T23:41:42.000000Z\n" +
                    "2020-10-25T01:00:00.000000Z\t2020-10-25T00:00:00.000000Z\ta\t4.158342987512034\t2020-10-25T01:51:12.000000Z\n" +
                    "2020-10-25T02:00:00.000000Z\t2020-10-25T02:00:00.000000Z\ta\t95.73868763606973\t2020-10-25T02:47:19.000000Z\n" +
                    "2020-10-25T03:00:00.000000Z\t2020-10-25T03:00:00.000000Z\ta\tnull\t2020-10-25T03:43:26.000000Z\n" +
                    "2020-10-25T04:00:00.000000Z\t2020-10-25T04:00:00.000000Z\ta\t34.49948946607576\t2020-10-25T04:56:49.000000Z\n";

            assertSql(expected, outSelect(out, viewQuery));
            assertSql(expected, outSelect(out, viewName));
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
            assertSql(expected1, "price_1h order by sym");
            assertSql(expected1, view1Sql + " order by sym");

            final String expected2 = "sym\tprice\tts\n" +
                    "gbpusd\t1.321\t2024-09-10T00:00:00.000000Z\n" +
                    "jpyusd\t103.21\t2024-09-10T00:00:00.000000Z\n";
            assertSql(expected2, "price_1d order by sym");
            assertSql(expected2, view2Sql + " order by sym");
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

            assertSql(
                    "view_name\tbase_table_name\tview_status\tinvalidation_reason\n" +
                            "price_1h\tbase_price\tinvalid\t[-1] unexpected filter error\n",
                    "select view_name, base_table_name, view_status, invalidation_reason from mat_views"
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

            assertSql(
                    "view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\n" +
                            "v1_base\tincremental\tbase\tvalid\t\n" +
                            "v2_v1\tincremental\tv1_base\tvalid\t\n" +
                            "v3_v1\tincremental\tv1_base\tvalid\t\n" +
                            "v4_v3\tincremental\tv3_v1\tvalid\t\n",
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason from mat_views order by view_name"
            );

            execute("truncate table " + tableName);
            long ts = TimestampFormatUtils.parseUTCTimestamp("2025-05-17T00:00:00.000000Z");
            execute("insert into " + tableName + " " + generateSelectSql(columns, ts, step, N, N));

            drainQueues();

            // all views should be invalid
            assertSql(
                    "view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\n" +
                            "v1_base\tincremental\tbase\tinvalid\ttruncate operation\n" +
                            "v2_v1\tincremental\tv1_base\tinvalid\ttruncate operation\n" +
                            "v3_v1\tincremental\tv1_base\tinvalid\ttruncate operation\n" +
                            "v4_v3\tincremental\tv3_v1\tinvalid\ttruncate operation\n",
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason from mat_views order by view_name"
            );

            execute("refresh materialized view " + view1Name + " full");
            drainQueues();

            assertSql(
                    "view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\n" +
                            "v1_base\tincremental\tbase\tvalid\t\n" +
                            "v2_v1\tincremental\tv1_base\tinvalid\ttruncate operation\n" +
                            "v3_v1\tincremental\tv1_base\tinvalid\ttruncate operation\n" +
                            "v4_v3\tincremental\tv3_v1\tinvalid\ttruncate operation\n",
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason from mat_views order by view_name"
            );

            // Refresh the rest
            execute("refresh materialized view " + view2Name + " full");
            drainQueues();
            execute("refresh materialized view " + view3Name + " full");
            drainQueues();
            execute("refresh materialized view " + view4Name + " full");
            drainQueues();

            assertSql(
                    "view_name\trefresh_type\tbase_table_name\tview_status\tinvalidation_reason\n" +
                            "v1_base\tincremental\tbase\tvalid\t\n" +
                            "v2_v1\tincremental\tv1_base\tvalid\t\n" +
                            "v3_v1\tincremental\tv1_base\tvalid\t\n" +
                            "v4_v3\tincremental\tv3_v1\tvalid\t\n",
                    "select view_name, refresh_type, base_table_name, view_status, invalidation_reason from mat_views order by view_name"
            );
        });
    }

    @Test
    public void testResumeSuspendMatView() throws Exception {
        assertMemoryLeak(() -> {
            // create table and mat view
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

            assertSql(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tbase_table_txn\tapplied_base_table_txn\n" +
                            "price_1h\tincremental\tbase_price\t2024-01-01T01:01:01.842574Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\t\tvalid\t1\t1\n",
                    "mat_views"
            );

            assertSql(
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

            assertSql(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n",
                    "price_1h order by sym"
            );

            assertSql(
                    "name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\n" +
                            "base_price\tfalse\t2\t0\t2\t\t\t0\n" +
                            "price_1h\ttrue\t1\t0\t2\t\t\t0\n",
                    "wal_tables()"
            );

            // resume mat view
            execute("alter materialized view price_1h resume wal");
            drainQueues();

            assertSql(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.14\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h order by sym"
            );

            assertSql(
                    "name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\n" +
                            "base_price\tfalse\t2\t0\t2\t\t\t0\n" +
                            "price_1h\tfalse\t2\t0\t2\t\t\t0\n",
                    "wal_tables()"
            );

            // suspend mat view again
            execute("alter materialized view price_1h suspend wal");

            execute("insert into base_price(sym, price, ts) values('jpyusd', 103.17, '2024-09-10T13:22')");
            drainQueues();

            assertSql(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.14\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h order by sym"
            );

            assertSql(
                    "name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\n" +
                            "base_price\tfalse\t3\t0\t3\t\t\t0\n" +
                            "price_1h\ttrue\t2\t0\t3\t\t\t0\n",
                    "wal_tables()"
            );

            // resume mat view from txn
            execute("alter materialized view price_1h resume wal from txn 3");
            drainQueues();

            assertSql(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.17\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h order by sym"
            );

            assertSql(
                    "name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\n" +
                            "base_price\tfalse\t3\t0\t3\t\t\t0\n" +
                            "price_1h\tfalse\t3\t0\t3\t\t\t0\n",
                    "wal_tables()"
            );
        });
    }

    @Test
    @Ignore
    public void testSampleByDST() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (" +
                    "sym varchar, price double, ts timestamp" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            execute("insert into base_price values" +
                    " ('gbpusd', 1.320, '2024-10-26T00:00')" +
                    ",('gbpusd', 1.321, '2024-10-26T01:00')" +

                    ",('gbpusd', 1.324, '2024-10-27T00:00')" +
                    ",('gbpusd', 1.325, '2024-10-27T01:00')" +
                    ",('gbpusd', 1.326, '2024-10-27T02:00')" +

                    ",('gbpusd', 1.327, '2024-10-28T00:00')" +
                    ",('gbpusd', 1.328, '2024-10-28T01:00')"
            );
            drainQueues();
            String exp = "sym\tfirst\tlast\tts\tberlin\n" +
                    "gbpusd\t1.32\t1.321\t2024-10-25T22:00:00.000000Z\t2024-10-26T00:00:00.000000Z\n" +
                    "gbpusd\t1.325\t1.326\t2024-10-27T00:00:00.000000Z\t2024-10-27T02:00:00.000000Z\n" +
                    "gbpusd\t1.327\t1.328\t2024-10-27T23:00:00.000000Z\t2024-10-28T00:00:00.000000Z\n";

            assertSql(
                    exp,
                    "select sym, first(price) as first, last(price) as last, count() count, ts from base_price" +
                            " sample by 1d ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin' order by ts, sym"
            );
        });
    }

    @Test
    public void testSampleByNoFillAlignToCalendarTimezoneOffset() throws Exception {
        assertMemoryLeak(() -> {
            String viewName = "x_view";
            String out = "select to_timezone(k, 'PST') k, c";
            String viewQuery = "select k, count() c from x sample by 2h align to calendar time zone 'PST' with offset '00:42'";
            long startTs = TimestampFormatUtils.parseUTCTimestamp("1970-01-03T00:20:00.000000Z");
            long step = 300000000;
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewName, viewQuery, startTs, step, N, K);
            String expected = "k\tc\n" +
                    "1970-01-02T14:42:00.000000Z\t5\n" +
                    "1970-01-02T16:42:00.000000Z\t24\n" +
                    "1970-01-02T18:42:00.000000Z\t24\n" +
                    "1970-01-02T20:42:00.000000Z\t24\n" +
                    "1970-01-02T22:42:00.000000Z\t23\n";

            assertSql(expected, outSelect(out, viewQuery));
            assertSql(expected, outSelect(out, viewName));
        });
    }

    @Test
    public void testSampleByNoFillNotKeyedAlignToCalendarMisalignedTimezone() throws Exception {
        // IRAN timezone is +4:30, which doesn't align well with 1hr sample
        assertMemoryLeak(() -> {
            String viewName = "x_view";
            String viewQuery = "select k, count() c from x sample by 1h align to calendar time zone 'Iran'";
            long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-03-28T00:15:00.000000Z");
            long step = 6 * 60000000;
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewName, viewQuery, startTs, step, N, K);
            String expected = "k\tc\n" +
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

            String out = "select to_timezone(k, 'Iran') k, c";
            assertSql(expected, outSelect(out, viewQuery));
            assertSql(expected, outSelect(out, viewName));
        });
    }

    @Test
    public void testSampleByNoFillNotKeyedAlignToCalendarTimezone() throws Exception {
        assertMemoryLeak(() -> {
            String viewName = "x_view";
            String viewQuery = "select k, count() c from x sample by 1h align to calendar time zone 'Europe/Berlin'";
            long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-03-28T00:15:00.000000Z");
            long step = 6 * 60000000;
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewName, viewQuery, startTs, step, N, K);
            String expected = "k\tc\n" +
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
            String out = "select to_timezone(k, 'Europe/Berlin') k, c";
            assertSql(expected, outSelect(out, viewQuery));
            assertSql(expected, outSelect(out, viewName));
        });
    }

    @Test
    public void testSampleByNoFillNotKeyedAlignToCalendarTimezoneFixedFormat() throws Exception {
        assertMemoryLeak(() -> testAlignToCalendarTimezoneOffset("GMT+01:00"));
    }

    @Test
    @Ignore
    public void testSampleByNoFillNotKeyedAlignToCalendarTimezoneOct() throws Exception {
        // We are going over spring time change. Because time is "expanding" we don't have
        // to do anything special. Our UTC timestamps will show "gap" and data doesn't
        // have to change
        assertMemoryLeak(() -> {
            String viewName = "x_view";
            String viewQuery = "select k, count() c from x sample by 1h align to calendar time zone 'Europe/Berlin'";
            long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-10-31T00:15:00.000000Z");
            long step = 6 * 60000000;
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewName, viewQuery, startTs, step, N, K);
            String expected = "k\tc\n" +
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
            String out = "select to_timezone(k, 'Europe/Berlin') k, c";
            assertSql(expected, outSelect(out, viewQuery));
            //TODO(eugene): Sample by bug around DST ?
            assertSql(expected, outSelect(out, viewName));
        });
    }

    @Test
    @Ignore
    public void testSampleByNoFillNotKeyedAlignToCalendarTimezoneOctMin() throws Exception {
        // We are going over spring time change. Because time is "expanding" we don't have
        // to do anything special. Our UTC timestamps will show "gap" and data doesn't
        // have to change
        assertMemoryLeak(() -> {
            String viewName = "x_view";
            String viewQuery = "select k, count() c from x sample by 30m align to calendar time zone 'Europe/Berlin'";
            long startTs = TimestampFormatUtils.parseUTCTimestamp("2021-10-31T00:15:00.000000Z");
            long step = 6 * 60000000;
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewName, viewQuery, startTs, step, N, K);
            String expected = "k\tc\n" +
                    "2021-10-31T02:00:00.000000Z\t3\n" +
                    "2021-10-31T02:30:00.000000Z\t15\n" +
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

            String out = "select to_timezone(k, 'Europe/Berlin') k, c";
            assertSql(expected, outSelect(out, viewQuery));
            //TODO(eugene): Sample by bug around DST ?
            assertSql(expected, outSelect(out, viewName));
        });
    }

    @Test
    public void testSampleByNoFillNotKeyedAlignToCalendarTimezoneOffset() throws Exception {
        assertMemoryLeak(() -> testAlignToCalendarTimezoneOffset("PST"));
    }

    @Test
    public void testSampleByNoFillNotKeyedAlignToCalendarUTC() throws Exception {
        assertMemoryLeak(() -> {
            String viewName = "x_view";
            String viewQuery = "select k, count() c from x sample by 90m align to calendar";
            long startTs = 172800000000L;
            long step = 300000000;
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewName, viewQuery, startTs, step, N, K);
            String expected = "k\tc\n" +
                    "1970-01-03T00:00:00.000000Z\t18\n" +
                    "1970-01-03T01:30:00.000000Z\t18\n" +
                    "1970-01-03T03:00:00.000000Z\t18\n" +
                    "1970-01-03T04:30:00.000000Z\t18\n" +
                    "1970-01-03T06:00:00.000000Z\t18\n" +
                    "1970-01-03T07:30:00.000000Z\t10\n";
            assertSql(expected, viewQuery);
            assertSql(expected, viewName);
        });
    }

    @Test
    public void testSampleByNoFillNotKeyedAlignToCalendarUTCOffset() throws Exception {
        assertMemoryLeak(() -> {
            String viewName = "x_view";
            String viewQuery = "select k, count() c from x sample by 90m align to calendar with offset '00:42'";
            long startTs = 172800000000L;
            long step = 300000000;
            final int N = 100;
            final int K = 5;
            updateViewIncrementally(viewName, viewQuery, startTs, step, N, K);
            String expected = "k\tc\n" +
                    "1970-01-02T23:12:00.000000Z\t9\n" +
                    "1970-01-03T00:42:00.000000Z\t18\n" +
                    "1970-01-03T02:12:00.000000Z\t18\n" +
                    "1970-01-03T03:42:00.000000Z\t18\n" +
                    "1970-01-03T05:12:00.000000Z\t18\n" +
                    "1970-01-03T06:42:00.000000Z\t18\n" +
                    "1970-01-03T08:12:00.000000Z\t1\n";
            assertSql(expected, viewQuery);
            assertSql(expected, viewName);
        });
    }

    // TODO(puzpuzpuz): we need to fix txn for all readers that belong to the base table
    //                  instead of using single detached reader instance; index cursors are
    //                  mutable and lead to bugs when shared between multiple factories/cursors
    @Ignore
    @Test
    public void testSelfJoinQuery() throws Exception {
        // Here we want to verify that the detached base table reader used by the refresh job
        // can be safely used in the mat view query multiple times.
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym symbol index, sym2 symbol, price double, ts timestamp, extra_col long" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            createMatView(
                    "select a.sym sym_a, b.sym sym_b, a.sym2 sym2_a, b.sym2 sym2_b, last(b.price) as price, a.ts " +
                            "from (base_price where sym = 'foobar') a " +
                            "asof join (base_price where sym = 'barbaz') b on (sym2) " +
                            "sample by 1h"
            );

            execute(
                    "insert into base_price(sym, sym2, price, ts) values('foobar', 's1', 1.320, '2024-09-10T12:01')" +
                            ",('foobar', 's1', 1.323, '2024-09-10T12:02')" +
                            ",('barbaz', 's1', 103.21, '2024-09-10T12:02')" +
                            ",('foobar', 's1', 1.321, '2024-09-10T13:02')"
            );

            currentMicros = parseFloorPartialTimestamp("2024-01-01T01:01:01.842574Z");
            drainQueues();

            assertSql(
                    "view_name\tbase_table_name\tview_status\n" +
                            "price_1h\tbase_price\tvalid\n",
                    "select view_name, base_table_name, view_status from mat_views"
            );

            assertSql(
                    "sym_a\tsym_b\tsym2_a\tsym2_b\tprice\tts\n" +
                            "foobar\tbarbaz\ts1\ts1\t103.21\t2024-09-10T12:00:00.000000Z\n",
                    "price_1h"
            );
        });
    }

    @Test
    public void testSimpleCancelRefresh() throws Exception {
        assertMemoryLeak(() -> {
            SOCountDownLatch started = new SOCountDownLatch(1);
            SOCountDownLatch stopped = new SOCountDownLatch(1);
            AtomicBoolean refreshed = new AtomicBoolean(true);

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

            new Thread(() -> {
                started.countDown();
                try {
                    try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine)) {
                        refreshed.set(job.run(0));
                    }
                } finally {
                    stopped.countDown();
                }
            }, "mat_view_refresh_thread").start();

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
            assertSql(
                    "view_name\tview_status\n" +
                            "price_1h\tinvalid\n",
                    "select view_name, view_status from mat_views"
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

            assertSql(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n",
                    "price_1h order by ts, sym"
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

            assertSql(
                    "sym\tprice\tts\n" +
                            "gbpusd\t1.312\t2024-09-12T13:00:00.000000Z\n" +
                            "gbpusd\t1.313\t2024-09-13T13:00:00.000000Z\n" +
                            "gbpusd\t1.314\t2024-09-14T13:00:00.000000Z\n",
                    "price_1h order by ts, sym"
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

    private String copyDataSql(String dst, String src, int from, int count) {
        return "insert into " + dst + " " + copySql(src, from, count);
    }

    private String copySql(String src, int from, int count) {
        return "select * from " + src + " where n >= " + from + " and n < " + (from + count);
    }

    private String createTableSql(String tableName, String columns, @Nullable String index, long startTs, long step, int count) {
        String indexStr = index == null ? "" : ",index(" + index + ") ";
        return "create table " + tableName + " as (" + generateSelectSql(columns, startTs, step, 0, count) + ")" +
                indexStr +
                " timestamp(k) partition by DAY WAL";
    }

    private String createTableSql(String tableName, String src, int from, int count) {
        return "create table " + tableName + " as (" + copySql(src, from, count) + ") timestamp(k) partition by DAY WAL";
    }

    private void drainQueues() {
        drainWalQueue();
        try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine)) {
            while (refreshJob.run(0)) {
            }
            drainWalQueue();
        }
        // purge job may create MatViewRefreshList for existing tables by calling engine.getDependentMatViews();
        // this affects refresh logic in some scenarios, so make sure to run it
        runWalPurgeJob();
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

    private void testAlignToCalendarTimezoneOffset(final String timezone) throws SqlException {
        String viewName = "x_view";
        String viewQuery = "select k, count() c from x sample by 90m align to calendar time zone '" + timezone + "' with offset '00:42'";
        long startTs = 172800000000L;
        long step = 300000000;
        final int N = 100;
        final int K = 5;
        updateViewIncrementally(viewName, viewQuery, startTs, step, N, K);
        String expected = "k\tc\n" +
                "1970-01-02T23:42:00.000000Z\t15\n" +
                "1970-01-03T01:12:00.000000Z\t18\n" +
                "1970-01-03T02:42:00.000000Z\t18\n" +
                "1970-01-03T04:12:00.000000Z\t18\n" +
                "1970-01-03T05:42:00.000000Z\t18\n" +
                "1970-01-03T07:12:00.000000Z\t13\n";
        assertSql(expected, viewQuery);
        assertSql(expected, viewName);
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

            assertSql(
                    "view_name\tbase_table_name\tview_status\n" +
                            "price_1h\tbase_price\tvalid\n",
                    "select view_name, base_table_name, view_status from mat_views"
            );

            execute(operationSql);
            drainQueues();

            assertSql(
                    "view_name\tbase_table_name\tview_status\tinvalidation_reason\n" +
                            "price_1h\tbase_price\tinvalid\t" + invalidationReason + "\n",
                    "select view_name, base_table_name, view_status, invalidation_reason from mat_views"
            );
        });
    }

    private void testBaseTableRenameAndThenRenameBack(boolean drainWalQueue) throws Exception {
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
            if (drainWalQueue) {
                drainWalQueue();
            }

            execute("rename table base_price2 to base_price");
            drainQueues();

            assertSql(
                    "view_name\trefresh_type\tbase_table_name\tlast_refresh_timestamp\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\tbase_table_txn\tapplied_base_table_txn\n" +
                            "price_1h\tincremental\tbase_price\t2024-10-24T19:00:00.000000Z\tselect sym, last(price) as price, ts from base_price sample by 1h\tprice_1h~2\ttable rename operation\tinvalid\t1\t3\n",
                    "mat_views"
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

            assertSql(
                    "view_name\tbase_table_name\tview_status\n" +
                            "price_1h\tbase_price\tvalid\n",
                    "select view_name, base_table_name, view_status from mat_views"
            );

            execute(enableDedupSql);
            drainQueues();

            assertSql(
                    "view_name\tbase_table_name\tview_status\n" +
                            "price_1h\tbase_price\t" + (expectInvalid ? "invalid" : "valid") + "\n",
                    "select view_name, base_table_name, view_status from mat_views"
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

            assertSql(
                    "sequencerTxn\tminTimestamp\tmaxTimestamp\n" +
                            "1\t2024-09-10T12:00:00.000000Z\t2024-09-18T19:00:00.000000Z\n",
                    "select sequencerTxn, minTimestamp, maxTimestamp from wal_transactions('price_1h')"
            );

            execute(
                    "insert into base_price values('gbpusd', 1.319, '2024-09-10T12:05')" +
                            ",('gbpusd', 1.325, '2024-09-10T13:03')"
            );
            drainQueues();

            assertSql(
                    "sequencerTxn\tminTimestamp\tmaxTimestamp\n" +
                            "1\t2024-09-10T12:00:00.000000Z\t2024-09-18T19:00:00.000000Z\n" +
                            "2\t2024-09-10T12:00:00.000000Z\t2024-09-10T13:00:00.000000Z\n",
                    "select sequencerTxn, minTimestamp, maxTimestamp from wal_transactions('price_1h')"
            );

            assertViewMatchesSqlOverBaseTable(viewSql);
        });
    }

    private void updateViewIncrementally(String viewName, String viewQuery, long startTs, long step, int N, int K) throws SqlException {
        updateViewIncrementally(viewName, viewQuery, " rnd_double(0)*100 a, rnd_symbol(5,4,4,1) b,", startTs, step, N, K);
    }

    private void updateViewIncrementally(String viewName, String viewQuery, String columns, long startTs, long step, int N, int K) throws SqlException {
        updateViewIncrementally(viewName, viewQuery, columns, null, startTs, step, N, K);
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
        execute(createTableSql("x", "tmp", 1, initSize));
        drainQueues();
        createMatView(viewName, viewQuery);
        drainQueues();

        try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine)) {
            refreshJob.run(0);
            drainWalQueue();

            int prev = initSize + 1;
            for (int i = 0; i < K; i++) {
                int size = chunkSize + (i < tail ? 1 : 0);
                execute(copyDataSql("x", "tmp", prev, size));
                prev = prev + size;
                drainWalQueue();
                refreshJob.run(0);
                drainWalQueue();
                remainingSize -= size;
            }
        }

        Assert.assertEquals(0, remainingSize);
    }
}
