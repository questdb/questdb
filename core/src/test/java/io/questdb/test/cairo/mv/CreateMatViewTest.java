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
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewRefreshState;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.std.Chars;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
                try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine);
                     SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
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
                    while ((knownCount = createCounter.get()) < iterations && errorCounter.get() == 0) {
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
            execute("CREATE MATERIALIZED VIEW test AS (" + query + ") PARTITION BY WEEK TTL 3 WEEKS;");
            assertMatViewDefinition("test", query, TABLE1, 30, 's');
            assertMatViewMetadata("test", query, TABLE1, 30, 's');

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertEquals(0, metadata.getTimestampIndex());
                assertTrue(metadata.isDedupKey(0));
                assertTrue(metadata.isDedupKey(1));
                assertFalse(metadata.isDedupKey(2));
                assertEquals(3 * 7 * 24, metadata.getTtlHoursOrMonths());
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
                assertEquals(0, metadata.getTtlHoursOrMonths());
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
    public void testCreateMatViewInvalidTtl() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            try {
                execute("create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day ttl 12 hours");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "TTL value must be an integer multiple of partition size");
            }
            assertNull(getMatViewDefinition("test"));
        });
    }

    @Test
    public void testCreateMatViewKeyedSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, k, avg(v), last(v) from " + TABLE1 + " sample by 30s";
            execute("create materialized view test as (" + query + ") partition by day ttl 1 week");

            assertQuery("ts\tk\tavg\tlast\n", "test", "ts", true, true);

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertTrue(metadata.isDedupKey(0));
                assertTrue(metadata.isDedupKey(1));
                assertFalse(metadata.isDedupKey(2));
                assertFalse(metadata.isDedupKey(3));
                assertEquals(7 * 24, metadata.getTtlHoursOrMonths());
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
            final String sql = "create materialized view test as (" + query + "), index (k capacity 1024) partition by day ttl 3 days" +
                    (Os.isWindows() ? "" : " in volume vol1");

            sink.clear();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final ExecutionModel model = compiler.testCompileModel(sql, sqlExecutionContext);
                assertEquals(ExecutionModel.CREATE_MAT_VIEW, model.getModelType());
                ((Sinkable) model).toSink(sink);
                TestUtils.assertEquals(
                        "create materialized view test with base table1 as (" + query +
                                "), index(k capacity 1024) timestamp(ts) partition by DAY TTL 3 DAYS" +
                                (Os.isWindows() ? "" : " in volume 'vol1'"),
                        sink
                );
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
                TestUtils.assertContains(e.getFlyweightMessage(), "more than one table used in query, base table has to be set using 'WITH BASE'");
            }
            assertNull(getMatViewDefinition("test"));

            try {
                execute("create materialized view test as (select ts, avg(v) from " + TABLE3 + " sample by 30s " +
                        "union select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "more than one table used in query, base table has to be set using 'WITH BASE'");
            }
            assertNull(getMatViewDefinition("test"));

            try {
                execute("create materialized view test as (select ts, avg(v) from " + TABLE3 + " sample by 30s " +
                        "union select t1.ts, avg(t1.v) from " + TABLE1 + " as t1 join " + TABLE2 + " as t2 on v sample by 30s) partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "more than one table used in query, base table has to be set using 'WITH BASE'");
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
                TestUtils.assertContains(e.getFlyweightMessage(), "materialized view has to be partitioned");
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
                assertEquals(0, metadata.getTtlHoursOrMonths());
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
                TestUtils.assertContains(e.getFlyweightMessage(), "base table has to be WAL enabled");
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
    public void testCreateMatViewSampleByFill() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE2);
            final String fill = "select ts, avg(v) from " + TABLE2 + " where ts in '2024' sample by 1d fill(null)";
            try {
                execute("create materialized view test as (" + fill + ") partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "FILL is not supported for materialized view");
            }
        });
    }

    @Test
    public void testCreateMatViewSampleByFromTo() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE2);
            final String from = "select ts, avg(v) from " + TABLE2 + " where ts in '2024' sample by 1d from '2024-03-01'";
            final String to = "select ts, avg(v) from " + TABLE2 + " where ts in '2024' sample by 1d to '2024-06-30'";
            try {
                execute("create materialized view test as (" + from + ") partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "FROM is not supported for materialized view");
            }
            try {
                execute("create materialized view test as (" + to + ") partition by day");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "TO is not supported for materialized view");
            }
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
            assertMatViewDefinition("test", query, TABLE1, 1, 'd', tz, null);
            assertMatViewMetadata("test", query, TABLE1, 1, 'd', tz, null);
        });
    }

    @Test
    public void testCreateMatViewSampleByTimeZoneFixedFormat() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String tz = "GMT+02:00";
            final String query = "select ts, avg(v) from " + TABLE1 + " where ts in '2024' sample by 1d align to calendar time zone '" + tz + "'";
            execute("create materialized view test as (" + query + ") partition by day");

            assertQuery("ts\tavg\n", "test", "ts", true, true);
            assertMatViewDefinition("test", query, TABLE1, 1, 'd', tz, null);
            assertMatViewMetadata("test", query, TABLE1, 1, 'd', tz, null);
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
            assertMatViewDefinition("test", query, TABLE1, 1, 'd', tz, offset);
            assertMatViewMetadata("test", query, TABLE1, 1, 'd', tz, offset);
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
            assertMatViewDefinition("test", query, TABLE1, 1, 'd', null, offset);
            assertMatViewMetadata("test", query, TABLE1, 1, 'd', null, offset);
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

                assertEquals(0, metadata.getTtlHoursOrMonths());
            }
        });
    }

    @Test
    public void testCreateMatViewWithNonDedupBaseKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x " +
                            " (ts timestamp, k1 symbol, k2 symbol, v long)" +
                            " timestamp(ts) partition by day wal dedup upsert keys(ts, k1);"
            );
            execute(
                    "create table y " +
                            " (ts timestamp, k1 symbol, k2 symbol, v long)" +
                            " timestamp(ts) partition by day wal;"
            );

            final String[] queries = new String[]{
                    "create materialized view x_hourly as (select ts, k2, avg(v) from x sample by 1h) partition by day;",
                    "create materialized view x_hourly as (select xx.ts, xx.k2, avg(xx.v) from x as xx sample by 1h) partition by day;",
                    "create materialized view x_hourly as (select ts, k1, k2, avg(v) from x sample by 1h) partition by day;",
                    "create materialized view x_hourly as (select ts, concat(k1, k2) k, avg(v) from x sample by 1h) partition by day;",
                    "create materialized view x_hourly as (select ts, k, avg(v) from (select concat(k1, k2) k, v, ts from x) sample by 1h) partition by day;",
                    "create materialized view x_hourly as (select ts, k, avg(v) from (select concat(k2, 'foobar') k, v, ts from x) sample by 1h) partition by day;",
                    "create materialized view x_hourly as (select ts, k, avg(v) from (select concat('foobar', k2) k, v, ts from x) sample by 1h) partition by day;",
                    "create materialized view x_hourly as (select ts, k, avg(v) from (select ts, k2 as k, v from x) sample by 1h) partition by day;",
                    "create materialized view test with base x as (select t1.ts, t1.k2, avg(t1.v) from x as t1 join y as t2 on v sample by 1m) partition by day",
                    "create materialized view test with base x as (select t1.ts, t2.k1, avg(t1.v) from x as t1 join y as t2 on v sample by 1m) partition by day",
                    "create materialized view test with base x as (select \"t1\".\"ts\", \"t2\".\"k1\", avg(\"t1\".\"v\") from \"x\" as \"t1\" join \"y\" as \"t2\" on \"v\" sample by 1m) partition by day",
                    "create materialized view test with base x as (select t1.ts, t2.k1, avg(t1.v) from x as t1 join y as t2 on v sample by 1m) partition by day",
            };

            for (String query : queries) {
                try {
                    execute(query);
                    fail("Expected SqlException missing for " + query);
                } catch (SqlException e) {
                    TestUtils.assertContains(query, e.getFlyweightMessage(), "base table");
                }
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
                        drainWalQueue();
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
                try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine)) {
                    try {
                        barrier.await();
                        while (createCounter.get() < iterations && errorCounter.get() == 0) {
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
                }
            });
            refresher.start();

            creator.join();
            refresher.join();

            Assert.assertEquals(0, errorCounter.get());
        });
    }

    @Test
    public void testIgnoreUnknownDefinitionBlock() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                createTable(TABLE1);

                final String query = "select ts, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s";
                execute("create materialized view test as (" + query + ") partition by day");

                final TableToken matViewToken = engine.getTableTokenIfExists("test");
                final MatViewDefinition matViewDefinition = engine.getMatViewGraph().getViewDefinition(matViewToken);
                assertNotNull(matViewDefinition);

                try (BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode())) {
                    writer.of(path.of(configuration.getDbRoot()).concat(matViewToken).concat(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME).$());
                    // Add unknown block.
                    AppendableBlock block = writer.append();
                    block.putStr("foobar");
                    block.commit(MatViewDefinition.MAT_VIEW_DEFINITION_FORMAT_MSG_TYPE + 1);
                    // Then write mat view definition.
                    block = writer.append();
                    MatViewDefinition.append(matViewDefinition, block);
                    block.commit(MatViewDefinition.MAT_VIEW_DEFINITION_FORMAT_MSG_TYPE);
                    writer.commit();
                }

                // Reader should ignore unknown block.
                try (BlockFileReader reader = new BlockFileReader(configuration)) {
                    path.of(configuration.getDbRoot());
                    final int rootLen = path.size();
                    MatViewDefinition actualDefinition = MatViewDefinition.readFrom(
                            reader,
                            path,
                            rootLen,
                            matViewToken
                    );

                    assertEquals(matViewDefinition.getMatViewSql(), actualDefinition.getMatViewSql());
                    assertEquals(matViewDefinition.getBaseTableName(), actualDefinition.getBaseTableName());
                    assertEquals(matViewDefinition.getSamplingInterval(), actualDefinition.getSamplingInterval());
                    assertEquals(matViewDefinition.getSamplingIntervalUnit(), actualDefinition.getSamplingIntervalUnit());
                    assertEquals(matViewDefinition.getTimeZone(), actualDefinition.getTimeZone());
                    assertEquals(matViewDefinition.getTimeZoneOffset(), actualDefinition.getTimeZoneOffset());
                }
            }
        });
    }

    @Test
    public void testIgnoreUnknownStateBlock() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                createTable(TABLE1);

                final String query = "select ts, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s";
                execute("create materialized view test as (" + query + ") partition by day");

                final TableToken matViewToken = engine.getTableTokenIfExists("test");
                final MatViewDefinition matViewDefinition = engine.getMatViewGraph().getViewDefinition(matViewToken);
                assertNotNull(matViewDefinition);
                final MatViewRefreshState matViewRefreshState = engine.getMatViewGraph().getViewRefreshState(matViewToken);
                assertNotNull(matViewRefreshState);

                try (BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode())) {
                    writer.of(path.of(configuration.getDbRoot()).concat(matViewToken).concat(MatViewRefreshState.MAT_VIEW_STATE_FILE_NAME).$());
                    // Add unknown block.
                    AppendableBlock block = writer.append();
                    block.putStr("foobar");
                    block.commit(MatViewRefreshState.MAT_VIEW_STATE_FORMAT_MSG_TYPE + 1);
                    // Then write mat view state.
                    block = writer.append();
                    MatViewRefreshState.append(matViewRefreshState, block);
                    block.commit(MatViewRefreshState.MAT_VIEW_STATE_FORMAT_MSG_TYPE);
                    writer.commit();
                }

                // Reader should ignore unknown block.
                try (BlockFileReader reader = new BlockFileReader(configuration)) {
                    reader.of(path.of(configuration.getDbRoot()).concat(matViewToken).concat(MatViewRefreshState.MAT_VIEW_STATE_FILE_NAME).$());
                    MatViewRefreshState actualState = new MatViewRefreshState(
                            matViewDefinition,
                            false,
                            (event, tableToken, baseTableTxn, errorMessage, latencyUs) -> {
                            }
                    );
                    MatViewRefreshState.readFrom(reader, actualState);

                    assertEquals(matViewRefreshState.isInvalid(), actualState.isInvalid());
                    assertEquals(matViewRefreshState.getLastRefreshBaseTxn(), actualState.getLastRefreshBaseTxn());
                    assertEquals(matViewRefreshState.getInvalidationReason(), actualState.getInvalidationReason());
                }
            }
        });
    }

    private static void assertMatViewDefinition(
            String name,
            String query,
            String baseTableName,
            long samplingInterval,
            char samplingIntervalUnit,
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
        assertEquals(timeZone, timeZone != null ? matViewDefinition.getTimeZone() : null);
        assertEquals(timeZoneOffset != null ? timeZoneOffset : "00:00", matViewDefinition.getTimeZoneOffset());
    }

    private static void assertMatViewDefinition(String name, String query, String baseTableName, int samplingInterval, char samplingIntervalUnit) {
        assertMatViewDefinition(name, query, baseTableName, samplingInterval, samplingIntervalUnit, null, null);
    }

    private static void assertMatViewMetadata(String name, String query, String baseTableName, int samplingInterval, char samplingIntervalUnit) {
        assertMatViewMetadata(name, query, baseTableName, samplingInterval, samplingIntervalUnit, null, null);
    }

    private static void assertMatViewMetadata(
            String name,
            String query,
            String baseTableName,
            long samplingInterval,
            char samplingIntervalUnit,
            String timeZone,
            String timeZoneOffset
    ) {
        final TableToken matViewToken = engine.getTableTokenIfExists(name);
        try (BlockFileReader reader = new BlockFileReader(configuration); Path path = new Path()) {
            path.of(configuration.getDbRoot());
            final int rootLen = path.size();
            MatViewDefinition mvd = MatViewDefinition.readFrom(
                    reader,
                    path,
                    rootLen,
                    matViewToken
            );

            assertEquals(mvd.getMatViewSql(), query);

            assertEquals(mvd.getBaseTableName(), baseTableName);
            assertEquals(mvd.getSamplingInterval(), samplingInterval);
            assertEquals(mvd.getSamplingIntervalUnit(), samplingIntervalUnit);
            assertEquals(Chars.toString(mvd.getTimeZone()), timeZone);
            assertEquals(mvd.getTimeZoneOffset(), timeZoneOffset != null ? timeZoneOffset : "00:00");
        }
    }

    private static MatViewDefinition getMatViewDefinition(String viewName) {
        final TableToken matViewToken = engine.getTableTokenIfExists(viewName);
        if (matViewToken == null) {
            return null;
        }
        return engine.getMatViewGraph().getViewDefinition(matViewToken);
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
