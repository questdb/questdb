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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class CreateMatViewTest extends AbstractCairoTest {
    private static final String TABLE1 = "table1";
    private static final String TABLE2 = "table2";
    private static final String TABLE3 = "table3";

    @BeforeClass
    public static void setUpStatic() throws Exception {
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
    public void testCreateDropConcurrent() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "  sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            final int iterations = 25;
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final AtomicInteger errorCounter = new AtomicInteger();
            final AtomicInteger createCounter = new AtomicInteger();

            final Thread creator = new Thread(() -> {
                final MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine);
                try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                    barrier.await();
                    for (int i = 0; i < iterations; i++) {
                        execute(
                                "create materialized view if not exists price_1h as (" +
                                        "  select sym, last(price) as price, ts from base_price sample by 1h" +
                                        ") partition by DAY",
                                executionContext
                        );
                        execute("insert into base_price values('gbpusd', 1.320, now())", executionContext);
                        drainWalQueue();
                        refreshJob.run(0);
                        createCounter.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    errorCounter.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                }
            });
            creator.start();

            final Thread dropper = new Thread(() -> {
                try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                    barrier.await();
                    int knownCount;
                    int droppedAt = 0;
                    while ((knownCount = createCounter.get()) < iterations) {
                        if (knownCount > droppedAt) {
                            execute("drop materialized view if exists price_1h", executionContext);
                            droppedAt = createCounter.get();
                        } else {
                            Os.sleep(1);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    errorCounter.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                }
            });
            dropper.start();

            creator.join();
            dropper.join();

            Assert.assertEquals(0, errorCounter.get());
        });
    }

    @Test
    public void testCreateMatViewBaseTableDoesNotExist() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("create materialized view testView as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist [table=" + TABLE1 + "]");
            }
            assertNull(getMatViewDefinition("testView"));
        });
    }

    @Test
    public void testCreateMatViewExpressionKey() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, k || '10' as k, max(v) as v_max from " + TABLE1 + " sample by 30s";
            execute("create materialized view test as (" + query + ") partition by week");
            assertMatViewDefinition("test", query, TABLE1, 30, 's');
            assertMatViewMetadata("test", query, TABLE1, 30, 's');

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertEquals(0, metadata.getTimestampIndex());
                assertTrue(metadata.isDedupKey(0));
                assertTrue(metadata.isDedupKey(1));
                assertFalse(metadata.isDedupKey(2));
            }
        });
    }

    @Test
    public void testCreateMatViewFunctionKey() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, concat(k, '10') as k, max(v) as v_max from " + TABLE1 + " sample by 30s";
            execute("create materialized view test as (" + query + ") partition by week");
            assertMatViewDefinition("test", query, TABLE1, 30, 's');
            assertMatViewMetadata("test", query, TABLE1, 30, 's');

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertEquals(0, metadata.getTimestampIndex());
                assertTrue(metadata.isDedupKey(0));
                assertTrue(metadata.isDedupKey(1));
                assertFalse(metadata.isDedupKey(2));
            }
        });
    }

    @Test
    public void testCreateMatViewGroupByTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select timestamp_floor('1m', ts) as ts, avg(v) from " + TABLE1 + " order by ts";
            execute("create materialized view test as (" + query + ") partition by day");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
            assertMatViewDefinition("test", query, TABLE1, 1, 'm');
            assertMatViewMetadata("test", query, TABLE1, 1, 'm');
        });
    }

    @Test
    public void testCreateMatViewInvalidTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            try {
                final String query = "select ts, k, avg(v) from " + TABLE1 + " sample by 30s";
                execute("create materialized view testView as (" + query + ") timestamp(k) partition by week");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "TIMESTAMP column expected [actual=SYMBOL]");
            }
            assertNull(getMatViewDefinition("testView"));
        });
    }

    @Test
    public void testCreateMatViewKeyedSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, k, avg(v), last(v) from " + TABLE1 + " sample by 30s";
            execute("create materialized view test as (" + query + ") partition by day");

            assertQuery("ts\tk\tavg\tlast\n", "test", "ts", true, true);

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertTrue(metadata.isDedupKey(0));
                assertTrue(metadata.isDedupKey(1));
                assertFalse(metadata.isDedupKey(2));
                assertFalse(metadata.isDedupKey(3));
            }
            assertMatViewDefinition("test", query, TABLE1, 30, 's');
            assertMatViewMetadata("test", query, TABLE1, 30, 's');
        });
    }

    @Test
    public void testCreateMatViewModelToSink() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, k, avg(v) from " + TABLE1 + " sample by 30s";
            final String sql = "create materialized view test as (" + query + "), index (k capacity 1024) partition by day" +
                    (Os.isWindows() ? "" : " in volume vol1");

            sink.clear();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final ExecutionModel model = compiler.testCompileModel(sql, sqlExecutionContext);
                assertEquals(model.getModelType(), ExecutionModel.CREATE_MAT_VIEW);
                ((Sinkable) model).toSink(sink);
                TestUtils.assertEquals("create materialized view test with base table1 as (" + query +
                        "), index(k capacity 1024) timestamp(ts) partition by DAY" +
                        (Os.isWindows() ? "" : " in volume 'vol1'"), sink);
            }
        });
    }

    @Test
    public void testCreateMatViewMultipleTables() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);
            createTable(TABLE3);

            try {
                execute("create materialized view test as (select t1.ts, avg(t1.v) from " + TABLE1 + " as t1 " +
                        "join " + TABLE2 + " as t2 on v sample by 30s) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "More than one table used in query, base table has to be set using 'WITH BASE'");
            }
            assertNull(getMatViewDefinition("test"));

            try {
                execute("create materialized view test as (select ts, avg(v) from " + TABLE3 + " sample by 30s " +
                        "union select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "More than one table used in query, base table has to be set using 'WITH BASE'");
            }
            assertNull(getMatViewDefinition("test"));

            try {
                execute("create materialized view test as (select ts, avg(v) from " + TABLE3 + " sample by 30s " +
                        "union select t1.ts, avg(t1.v) from " + TABLE1 + " as t1 join " + TABLE2 + " as t2 on v sample by 30s) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "More than one table used in query, base table has to be set using 'WITH BASE'");
            }
            assertNull(getMatViewDefinition("test"));
        });
    }

    @Test
    public void testCreateMatViewNoPartitionBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            try {
                execute("create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s)");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "'partition by' expected");
            }
            assertNull(getMatViewDefinition("test"));

            try {
                execute("create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by 3d");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "'HOUR', 'DAY', 'WEEK', 'MONTH' or 'YEAR' expected");
            }
            assertNull(getMatViewDefinition("test"));

            try {
                execute("create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by NONE");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Materialized view has to be partitioned");
            }
            assertNull(getMatViewDefinition("test"));
        });
    }

    @Test
    public void testCreateMatViewNoSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            try {
                execute("create materialized view test as (select * from " + TABLE1 + " where v % 2 = 0) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Materialized view query requires a sampling interval");
            }
            assertNull(getMatViewDefinition("test"));
        });
    }

    @Test
    public void testCreateMatViewNonDeterministicFunction() throws Exception {
        final String[][] functions = new String[][]{
                {"sysdate()", "sysdate"},
                {"systimestamp()", "systimestamp"},
                {"today()", "today"},
                {"yesterday()", "yesterday"},
                {"tomorrow()", "tomorrow"},
                {"rnd_bin()", "rnd_bin"},
                {"rnd_bin(4,4,4)", "rnd_bin"},
                {"rnd_byte()", "rnd_byte"},
                {"rnd_byte(1,4)", "rnd_byte"},
                {"rnd_boolean()", "rnd_boolean"},
                {"rnd_char()", "rnd_char"},
                {"rnd_date()", "rnd_date"},
                {"rnd_date(1,4,5)", "rnd_date"},
                {"rnd_double()", "rnd_double"},
                {"rnd_double(5)", "rnd_double"},
                {"rnd_float()", "rnd_float"},
                {"rnd_float(5)", "rnd_float"},
                {"rnd_int()", "rnd_int"},
                {"rnd_int(1,4,5)", "rnd_int"},
                {"rnd_short()", "rnd_short"},
                {"rnd_short(1,5)", "rnd_short"},
                {"rnd_long()", "rnd_long"},
                {"rnd_long(1,4,5)", "rnd_long"},
                {"rnd_long256()", "rnd_long256"},
                {"rnd_long256(3)", "rnd_long256"},
                {"rnd_ipv4()", "rnd_ipv4"},
                {"rnd_ipv4('2.2.2.2/16', 2)", "rnd_ipv4"},
                {"rnd_str(1,4,5)", "rnd_str"},
                {"rnd_str(1,4,5,6)", "rnd_str"},
                {"rnd_str('abc','def','hij')", "rnd_str"},
                {"rnd_varchar(1,4,5)", "rnd_varchar"},
                {"rnd_varchar('abc','def','hij')", "rnd_varchar"},
                {"rnd_symbol(1,4,5,6)", "rnd_symbol"},
                {"rnd_symbol('abc','def','hij')", "rnd_symbol"},
                {"rnd_timestamp(to_timestamp('2024-03-01', 'yyyy-mm-dd'), to_timestamp('2024-04-01', 'yyyy-mm-dd'), 0)", "rnd_timestamp"},
                {"rnd_uuid4()", "rnd_uuid4"},
                {"rnd_uuid4(5)", "rnd_uuid4"},
                {"rnd_geohash(5)", "rnd_geohash"}
        };

        for (String[] func : functions) {
            testCreateMatViewNonDeterministicFunction(func[0], func[1]);
        }
    }

    @Test
    public void testCreateMatViewNonOptimizedSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, avg(v) from (select ts, k, v+10 as v from " + TABLE1 + ") sample by 30s";
            execute("create materialized view test as (" + query + ") partition by week");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
            assertMatViewDefinition("test", query, TABLE1, 30, 's');
            assertMatViewMetadata("test", query, TABLE1, 30, 's');
        });
    }

    @Test
    public void testCreateMatViewNonOptimizedSampleByMultipleTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, 1L::timestamp as ts2, avg(v) from (select ts, k, v+10 as v from " + TABLE1 + ") sample by 30s";
            execute("create materialized view test as (" + query + ") partition by week");
            assertMatViewDefinition("test", query, TABLE1, 30, 's');
            assertMatViewMetadata("test", query, TABLE1, 30, 's');

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertEquals(0, metadata.getTimestampIndex());
                assertTrue(metadata.isDedupKey(0));
                assertTrue(metadata.isDedupKey(1));
                assertFalse(metadata.isDedupKey(2));
            }
        });
    }

    @Test
    public void testCreateMatViewNonWalBaseTable() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1, false);

            try {
                execute("create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "The base table has to be WAL enabled");
            }
            assertNull(getMatViewDefinition("test"));
        });
    }

    @Test
    public void testCreateMatViewRewrittenSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, avg(v) from " + TABLE1 + " sample by 30s";
            execute("create materialized view test as (" + query + ") partition by day");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
            assertMatViewDefinition("test", query, TABLE1, 30, 's');
            assertMatViewMetadata("test", query, TABLE1, 30, 's');
        });
    }

    @Test
    public void testCreateMatViewRewrittenSampleByMultipleTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE3);

            final String query = "select ts, 1L::timestamp as ts2, avg(v) from " + TABLE3 + " sample by 30s";
            execute("create materialized view test_view as (" + query + ") partition by day");

            assertQuery("ts\tts2\tavg\n", "test_view", "ts", true, true);
            assertMatViewDefinition("test_view", query, TABLE3, 30, 's');
            assertMatViewMetadata("test_view", query, TABLE3, 30, 's');
        });
    }

    @Test
    public void testCreateMatViewSampleByFromTo() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE2);

            final String from = "2024-03-01";
            final String to = "2024-06-30";
            final String query = "select ts, avg(v) from " + TABLE2 + " where ts in '2024' sample by 1d from '" + from + "' to '" + to + "'";
            execute("create materialized view test as (" + query + ") partition by day");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
            assertMatViewDefinition("test", query, TABLE2, 1, 'd', parseAsMicros(from), parseAsMicros(to), null, null);
            assertMatViewMetadata("test", query, TABLE2, 1, 'd', parseAsMicros(from), parseAsMicros(to), null, null);
        });
    }

    @Test
    public void testCreateMatViewSampleByTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String tz = "Europe/Berlin";
            final String query = "select ts, avg(v) from " + TABLE1 + " where ts in '2024' sample by 1d align to calendar time zone '" + tz + "'";
            execute("create materialized view test as (" + query + ") partition by day");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
            assertMatViewDefinition("test", query, TABLE1, 1, 'd', Numbers.LONG_NULL, Numbers.LONG_NULL, tz, null);
            assertMatViewMetadata("test", query, TABLE1, 1, 'd', Numbers.LONG_NULL, Numbers.LONG_NULL, tz, null);
        });
    }

    @Test
    public void testCreateMatViewSampleByTimeZoneWithOffset() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String tz = "Europe/Berlin";
            final String offset = "00:45";
            final String query = "select ts, avg(v) from " + TABLE1 + " where ts in '2024' sample by 1d align to calendar time zone '" + tz + "' with offset '" + offset + "'";
            execute("create materialized view test as (" + query + ") partition by day");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
            assertMatViewDefinition("test", query, TABLE1, 1, 'd', Numbers.LONG_NULL, Numbers.LONG_NULL, tz, offset);
            assertMatViewMetadata("test", query, TABLE1, 1, 'd', Numbers.LONG_NULL, Numbers.LONG_NULL, tz, offset);
        });
    }

    @Test
    public void testCreateMatViewSampleByWithOffset() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String offset = "00:45";
            final String query = "select ts, avg(v) from " + TABLE1 + " where ts in '2024' sample by 1d align to calendar with offset '" + offset + "'";
            execute("create materialized view test as (" + query + ") partition by day");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
            assertMatViewDefinition("test", query, TABLE1, 1, 'd', Numbers.LONG_NULL, Numbers.LONG_NULL, null, offset);
            assertMatViewMetadata("test", query, TABLE1, 1, 'd', Numbers.LONG_NULL, Numbers.LONG_NULL, null, offset);
        });
    }

    @Test
    public void testCreateMatViewWithBase() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            final String query = "select t1.ts, avg(t1.v) from " + TABLE1 + " as t1 join " + TABLE2 + " as t2 on v sample by 60s";
            execute("create materialized view test with base " + TABLE1 + " as (" + query + ") partition by day");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
            assertMatViewDefinition("test", query, TABLE1, 60, 's');
            assertMatViewMetadata("test", query, TABLE1, 60, 's');
        });
    }

    @Test
    public void testCreateMatViewWithExistingTableName() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            try {
                execute("create materialized view " + TABLE2 + " as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "a table already exists with the requested name");
            }

            try {
                execute("create materialized view if not exists " + TABLE2 + " as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "a table already exists with the requested name");
            }

            final String query = "select ts, avg(v) from " + TABLE2 + " sample by 4h";
            execute("create materialized view test as (" + query + ") partition by day");

            // without IF NOT EXISTS
            try {
                execute("create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "view already exists");
            }

            // with IF NOT EXISTS
            execute("create materialized view if not exists test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day");
            assertMatViewDefinition("test", query, TABLE2, 4, 'h');
            assertMatViewMetadata("test", query, TABLE2, 4, 'h');

            try {
                execute("create table test(ts timestamp, col varchar) timestamp(ts) partition by day wal");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "a materialized view already exists with the requested name");
            }
        });
    }

    @Test
    public void testCreateMatViewWithIndex() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, k, avg(v) from " + TABLE1 + " sample by 30s";
            execute("create materialized view test as (" + query + "), index (k) partition by day");

            assertQuery("ts\tk\tavg\n", "test", "ts", true, true);
            assertMatViewDefinition("test", query, TABLE1, 30, 's');
            assertMatViewMetadata("test", query, TABLE1, 30, 's');

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertTrue(metadata.isDedupKey(0));
                assertTrue(metadata.isDedupKey(1));
                assertFalse(metadata.isDedupKey(2));

                assertFalse(metadata.isColumnIndexed(0));
                assertTrue(metadata.isColumnIndexed(1));
                assertFalse(metadata.isColumnIndexed(2));
            }
        });
    }

    @Test
    public void testCreateMatViewWithOperator() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s";
            execute("create materialized view test as (" + query + ") partition by day");

            assertQuery("ts\tdoubleV\tavg\n", "test", "ts", true, true);
            assertMatViewDefinition("test", query, TABLE1, 30, 's');
            assertMatViewMetadata("test", query, TABLE1, 30, 's');
        });
    }

    @Test
    public void testCreateRefreshConcurrent() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "  sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            final int iterations = 10;
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final AtomicInteger errorCounter = new AtomicInteger();
            final AtomicInteger createCounter = new AtomicInteger();

            final Thread creator = new Thread(() -> {
                try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                    barrier.await();
                    for (int i = 0; i < iterations; i++) {
                        execute(
                                "create materialized view if not exists price_1h as (" +
                                        "  select sym, last(price) as price, ts from base_price sample by 1h" +
                                        ") partition by DAY",
                                executionContext
                        );
                        execute("insert into base_price values('gbpusd', 1.320, now())", executionContext);
                        drainWalQueue();
                        execute("drop materialized view if exists price_1h", executionContext);
                        createCounter.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    errorCounter.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                }
            });
            creator.start();

            final Thread refresher = new Thread(() -> {
                final MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine);
                try {
                    barrier.await();
                    while (createCounter.get() < iterations) {
                        if (!refreshJob.run(0)) {
                            Os.sleep(1);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    errorCounter.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                }
            });
            refresher.start();

            creator.join();
            refresher.join();

            Assert.assertEquals(0, errorCounter.get());
        });
    }

    private static void assertMatViewDefinition(
            String name,
            String query,
            String baseTableName,
            long samplingInterval,
            char samplingIntervalUnit,
            long fromMicros,
            long toMicros,
            String timeZone,
            String timeZoneOffset
    ) {
        final MatViewDefinition matViewDefinition = getMatViewDefinition(name);
        assertNotNull(matViewDefinition);
        assertTrue(matViewDefinition.getMatViewToken().isMatView());
        assertTrue(matViewDefinition.getMatViewToken().isWal());
        assertEquals(query, matViewDefinition.getMatViewSql());
        assertEquals(baseTableName, matViewDefinition.getBaseTableName());
        assertEquals(samplingInterval, matViewDefinition.getSamplingInterval());
        assertEquals(samplingIntervalUnit, matViewDefinition.getSamplingIntervalUnit());
        assertEquals(fromMicros, matViewDefinition.getFromMicros());
        assertEquals(toMicros, matViewDefinition.getToMicros());
        assertEquals(timeZone, timeZone != null ? matViewDefinition.getTimeZone() : null);
        assertEquals(timeZoneOffset != null ? timeZoneOffset : "00:00", matViewDefinition.getTimeZoneOffset());
    }

    private static void assertMatViewDefinition(String name, String query, String baseTableName, int samplingInterval, char samplingIntervalUnit) {
        assertMatViewDefinition(name, query, baseTableName, samplingInterval, samplingIntervalUnit, Numbers.LONG_NULL, Numbers.LONG_NULL, null, null);
    }

    private static void assertMatViewMetadata(String name, String query, String baseTableName, int samplingInterval, char samplingIntervalUnit) {
        assertMatViewMetadata(name, query, baseTableName, samplingInterval, samplingIntervalUnit, Numbers.LONG_NULL, Numbers.LONG_NULL, null, null);
    }

    private static void assertMatViewMetadata(
            String name,
            String query,
            String baseTableName,
            long samplingInterval,
            char samplingIntervalUnit,
            long fromMicros,
            long toMicros,
            String timeZone,
            String timeZoneOffset
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        final TableToken matViewToken = engine.getTableTokenIfExists(name);

        long fd = -1;
        try (MemoryCMR mem = Vm.getCMRInstance();
             Path path = new Path()) {
            path.of(configuration.getRoot()).concat(matViewToken.getDirName());
            final int rootLen = path.size();

            try {
                fd = mapRO(ff, path, mem, TableUtils.MAT_VIEW_QUERY_FILE_NAME);

                final String queryMeta = Chars.toString(mem.getStrA(0L));
                assertEquals(query, queryMeta);
            } finally {
                mem.close();
                ff.close(fd);
            }

            path.trimTo(rootLen);
            try {
                fd = mapRO(ff, path, mem, TableUtils.MAT_VIEW_FILE_NAME);
                long offset = TableUtils.MV_HEADER_SIZE;

                final CharSequence baseTableNameMeta = mem.getStrA(offset);
                offset += Vm.getStorageLength(baseTableNameMeta);
                assertEquals(baseTableNameMeta, baseTableName);

                final long fromMicrosMeta = mem.getLong(offset);
                offset += Long.BYTES;
                assertEquals(fromMicros, fromMicrosMeta);

                final long toMicrosMeta = mem.getLong(offset);
                offset += Long.BYTES;
                assertEquals(toMicros, toMicrosMeta);

                final long samplingIntervalMeta = mem.getLong(offset);
                offset += Long.BYTES;
                assertEquals(samplingInterval, samplingIntervalMeta);

                final char samplingIntervalUnitMeta = mem.getChar(offset);
                offset += Character.BYTES;
                assertEquals(samplingIntervalUnit, samplingIntervalUnitMeta);

                final CharSequence timeZoneMeta = mem.getStrA(offset);
                offset += Vm.getStorageLength(timeZoneMeta);
                assertEquals(timeZone, timeZoneMeta != null ? timeZoneMeta.toString() : null);

                final CharSequence timeZoneOffsetMeta = mem.getStrA(offset);
                assertEquals(timeZoneOffset != null ? timeZoneOffset : "00:00", timeZoneOffsetMeta.toString());
            } finally {
                ff.close(fd);
            }
        }
    }

    private static MatViewDefinition getMatViewDefinition(String viewName) {
        final TableToken matViewToken = engine.getTableTokenIfExists(viewName);
        if (matViewToken == null) {
            return null;
        }
        return engine.getMatViewGraph().getMatViewDefinition(matViewToken);
    }

    private static long mapRO(FilesFacade ff, Path path, MemoryCMR mem, String fileName) {
        path.concat(fileName);
        final LPSZ $path = path.$();
        final long fd = ff.openRO($path);
        if (fd < 1) {
            fail("unable to open query file [path=" + path + "]");
        }

        final long fileLen = ff.length(fd);
        mem.of(ff, $path, fileLen, fileLen, MemoryTag.MMAP_DEFAULT);
        return fd;
    }

    private void createTable(String tableName) throws SqlException {
        createTable(tableName, true);
    }

    private void createTable(String tableName, boolean walEnabled) throws SqlException {
        execute(
                "create table if not exists " + tableName +
                        " (ts timestamp, k symbol, v long)" +
                        " timestamp(ts) partition by day" + (walEnabled ? "" : " bypass") + " wal"
        );
        for (int i = 0; i < 9; i++) {
            execute("insert into " + tableName + " values (" + (i * 10000000) + ", 'k" + i + "', " + i + ")");
        }
    }

    private long parseAsMicros(String date) throws ParseException {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.parse(date).getTime() * 1000;
    }

    private void testCreateMatViewNonDeterministicFunction(String func, String columnName) throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            try {
                execute("create materialized view test as (select ts, " + func + ", avg(v) from " + TABLE1 + " sample by 30s) partition by month");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Non-deterministic column: " + columnName);
            }
            assertNull(getMatViewDefinition("test"));
        });
    }

    protected void assertQuery(String expected, String query, String expectedTimestamp, boolean supportsRandomAccess, boolean expectSize) throws Exception {
        assertQueryFullFatNoLeakCheck(expected, query, expectedTimestamp, supportsRandomAccess, expectSize, false);
    }
}
