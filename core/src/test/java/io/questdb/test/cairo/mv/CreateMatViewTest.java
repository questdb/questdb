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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateReader;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class CreateMatViewTest extends AbstractCairoTest {
    private static final String TABLE1 = "table1";
    private static final String TABLE2 = "table2";
    private static final String TABLE3 = "table3";

    @Before
    public void setUp() {
        super.setUp();
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
                try (
                        MatViewRefreshJob refreshJob = createMatViewRefreshJob(engine);
                        SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)
                ) {
                    barrier.await();
                    for (int i = 0; i < iterations; i++) {
                        // Use test alloc() function to make sure that we always free the factory.
                        execute(
                                "create materialized view if not exists price_1h as (" +
                                        "  select sym, alloc(42), last(price) as price, ts from base_price sample by 1h" +
                                        ") partition by DAY",
                                executionContext
                        );
                        execute("insert into base_price values('gbpusd', 1.320, now())", executionContext);
                        drainWalQueue();
                        drainMatViewQueue(refreshJob);
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
    public void testCreateImmediateMatViewModelToSink() throws Exception {
        final String query = "select ts, k, avg(v) from " + TABLE1 + " sample by 45s";
        final DdlSerializationTest[] tests = new DdlSerializationTest[]{
                new DdlSerializationTest(
                        "create materialized view test as (" + query + "), index (k capacity 1024) partition by day ttl 3 days" +
                                (Os.isWindows() ? "" : " in volume vol1"),
                        "create materialized view test with base " + TABLE1 + " refresh immediate as (" +
                                "select-choose ts, k, avg(v) avg from (table1 sample by 45s align to calendar with offset '00:00')" +
                                "), index(k capacity 1024) partition by DAY TTL 3 DAYS" +
                                (Os.isWindows() ? "" : " in volume 'vol1'")
                ),
                new DdlSerializationTest(
                        "create materialized view test refresh immediate deferred as (" + query + "), index (k capacity 1024) partition by day ttl 3 days" +
                                (Os.isWindows() ? "" : " in volume vol1"),
                        "create materialized view test with base " + TABLE1 + " refresh immediate deferred as (" +
                                "select-choose ts, k, avg(v) avg from (table1 sample by 45s align to calendar with offset '00:00')" +
                                "), index(k capacity 1024) partition by DAY TTL 3 DAYS" +
                                (Os.isWindows() ? "" : " in volume 'vol1'")
                ),
        };
        testModelToSink(tests);
    }

    @Test
    public void testCreateManualMatViewModelToSink() throws Exception {
        final String query = "select ts, k, max(v) from " + TABLE1 + " sample by 30s";
        final DdlSerializationTest[] tests = new DdlSerializationTest[]{
                new DdlSerializationTest(
                        "create materialized view test refresh manual as " + query,
                        "create materialized view test with base " + TABLE1 + " refresh manual as (" +
                                "select-choose ts, k, max(v) max from (table1 sample by 30s align to calendar with offset '00:00')" +
                                ")"
                ),
                new DdlSerializationTest(
                        "create materialized view test refresh manual deferred as " + query,
                        "create materialized view test with base " + TABLE1 + " refresh manual deferred as (" +
                                "select-choose ts, k, max(v) max from (table1 sample by 30s align to calendar with offset '00:00')" +
                                ")"
                ),
        };
        testModelToSink(tests);
    }

    @Test
    public void testCreateMatViewAsExpectedPosition() throws Exception {
        assertMemoryLeak(() -> {
            assertExceptionNoLeakCheck(
                    "create materialized view test select as sym",
                    30,
                    "'refresh' or 'as' expected"
            );
            assertNull(getMatViewDefinition("test"));
        });
    }

    @Test
    public void testCreateMatViewBaseTableDoesNotExist() throws Exception {
        assertException(
                "create materialized view testView as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day",
                61,
                "table does not exist [table=" + TABLE1 + "]"
        );
        assertNull(getMatViewDefinition("testView"));
    }

    @Test
    public void testCreateMatViewBaseTableNoReference() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);
            final String sql = "with t as (select ts, avg(v) as avgv from " + TABLE2 + ") select ts, avgv from t sample by 30s";
            assertExceptionNoLeakCheck(
                    "create materialized view test with base " + TABLE1 + " as (" + sql + ") partition by day",
                    108,
                    "base table is not referenced in materialized view query"
            );
            assertNull(getMatViewDefinition("test"));
        });
    }

    @Test
    public void testCreateMatViewBaseTableSelfUnion() throws Exception {
        testCreateMatViewBaseTableSelfUnion("timestamp");
    }

    @Test
    public void testCreateMatViewBaseTableSelfUnionWithNanos() throws Exception {
        testCreateMatViewBaseTableSelfUnion("timestamp_ns");
    }

    @Test
    public void testCreateMatViewCopySymbolCapacity() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            final String sql = "select ts, k, k2, min(v) as v from " + TABLE1 + " sample by 1h";
            execute("create materialized view test with base " + TABLE1 + " as (" + sql + ") partition by day");
            assertSql("column\tsymbolCapacity\nk\t2048\nk2\t512\n", "select \"column\", symbolCapacity from (show columns from test) where type = 'SYMBOL'");
        });
    }

    @Test
    public void testCreateMatViewCopySymbolCapacityAlias() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            final String sql = "select ts, k as kkk, min(v) as v from " + TABLE1 + " sample by 1h";
            execute("create materialized view test with base " + TABLE1 + " as (" + sql + ") partition by day");
            assertSql("column\tsymbolCapacity\nkkk\t2048\n", "select \"column\", symbolCapacity from (show columns from test) where type = 'SYMBOL'");
        });
    }

    @Test
    public void testCreateMatViewCopySymbolCapacityExpr() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            final String sql = "select ts, cast(k || '_' as symbol) as k, min(v) as v from " + TABLE1 + " sample by 1h";
            execute("create materialized view test with base " + TABLE1 + " as (" + sql + ") partition by day");
            // assert default symbol capacity (128)
            assertSql("column\tsymbolCapacity\nk\t128\n", "select \"column\", symbolCapacity from (show columns from test) where type = 'SYMBOL'");
        });
    }

    @Test
    public void testCreateMatViewCopySymbolCapacityJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);
            final String sql = "select ts, k, avg(v), first(k2) as fk2 from ( select t1.ts, t1.k, t1.v, t2.k2 from " + TABLE2 + " as t2 asof join " + TABLE1 + " as t1) timestamp(ts) sample by 30s";
            execute("create materialized view test with base " + TABLE1 + " as (" + sql + ") partition by day");
            assertSql("column\tsymbolCapacity\nk\t2048\nfk2\t128\n", "select \"column\", symbolCapacity from (show columns from test) where type = 'SYMBOL'");
        });
    }

    @Test
    public void testCreateMatViewCopySymbolCapacityNestedAlias() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            final String sql = "with t as ( select ts, k as kk, v as vv from " + TABLE1 + ") select ts, kk as kkk, avg(vv) as vvv from t sample by 1h";
            execute("create materialized view test with base " + TABLE1 + " as (" + sql + ") partition by day");
            assertSql("column\tsymbolCapacity\nkkk\t2048\n", "select \"column\", symbolCapacity from (show columns from test) where type = 'SYMBOL'");
        });
    }

    @Test
    public void testCreateMatViewCopySymbolCapacityNotFromBase() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            final String sql = "select ts, cast('aaa' as symbol) as kkk, min(v) as v from " + TABLE1 + " sample by 1h";
            execute("create materialized view test with base " + TABLE1 + " as (" + sql + ") partition by day");
            // assert default symbol capacity (128)
            assertSql("column\tsymbolCapacity\nkkk\t128\n", "select \"column\", symbolCapacity from (show columns from test) where type = 'SYMBOL'");
        });
    }

    @Test
    public void testCreateMatViewDisabled() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_MAT_VIEW_ENABLED, "false");
            createTable(TABLE1);
            assertExceptionNoLeakCheck(
                    "create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day",
                    0,
                    "materialized views are disabled"
            );
            assertNull(getMatViewDefinition("test"));
        });
    }

    @Test
    public void testCreateMatViewExpressionKey() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, k || '10' as k, max(v) as v_max from " + TABLE1 + " sample by 30s";
            execute("CREATE MATERIALIZED VIEW test AS (" + query + ") PARTITION BY WEEK TTL 3 WEEKS;");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertEquals(0, metadata.getTimestampIndex());
                assertFalse(metadata.isDedupKey(0));
                assertFalse(metadata.isDedupKey(1));
                assertFalse(metadata.isDedupKey(2));
                assertEquals(3 * 7 * 24, metadata.getTtlHoursOrMonths());
            }
        });
    }

    @Test
    public void testCreateMatViewExpressionKeyCaseInsensitivity() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            // notice upper-case column names
            final String query = "select TS, K || '10' as k, max(v) as v_max from " + TABLE1 + " sample by 30s";
            execute("CREATE MATERIALIZED VIEW test AS (" + query + ") PARTITION BY WEEK TTL 3 WEEKS;");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertEquals(0, metadata.getTimestampIndex());
                assertFalse(metadata.isDedupKey(0));
                assertFalse(metadata.isDedupKey(1));
                assertFalse(metadata.isDedupKey(2));
                assertEquals(3 * 7 * 24, metadata.getTtlHoursOrMonths());
            }
        });
    }

    @Test
    public void testCreateMatViewFailsOnUnionWithNonBaseTable() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            assertExceptionNoLeakCheck(
                    "create materialized view test with base " + TABLE1 + " as (" +
                            "  with b as (" +
                            "    select ts, v from " + TABLE2 +
                            "    union all" +
                            "    select ts, v from " + TABLE1 +
                            "  )" +
                            "  select a.ts, avg(a.v)" +
                            "  from " + TABLE1 + " a " +
                            "  left outer join b on a.ts = b.ts" +
                            "  sample by 1d" +
                            ") partition by day",
                    106,
                    "union on base table is not supported for materialized views: " + TABLE1
            );
        });
    }

    @Test
    public void testCreateMatViewFunctionKey() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, concat(k, '10') as k, max(v) as v_max from " + TABLE1 + " sample by 30s";
            execute("create materialized view test as (" + query + ") partition by week");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertEquals(0, metadata.getTimestampIndex());
                assertFalse(metadata.isDedupKey(0));
                assertFalse(metadata.isDedupKey(1));
                assertFalse(metadata.isDedupKey(2));
                assertEquals(0, metadata.getTtlHoursOrMonths());
            }
        });
    }

    @Test
    public void testCreateMatViewGroupByNoTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            assertExceptionNoLeakCheck(
                    "create materialized view test as (select avg(v) from " + TABLE1 + ") partition by day",
                    34,
                    "TIMESTAMP column is not present in select list"
            );
            assertNull(getMatViewDefinition("test"));
        });
    }

    @Test
    public void testCreateMatViewGroupByTimestamp1() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select timestamp_floor('1m', ts) as ts1, avg(v) from " + TABLE1 + " order by ts1";
            execute("create materialized view test as (" + query + ") partition by day");

            assertQuery0("ts1\tavg\n", "test", "ts1");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 1, 'm', null, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 1, 'm', null, null);
        });
    }

    @Test
    public void testCreateMatViewGroupByTimestamp2() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select timestamp_floor('1m', ts, 0::timestamp) as ts2, avg(v) from " + TABLE1;
            execute("create materialized view test as (" + query + ") partition by day");

            assertQuery0("ts2\tavg\n", "test", "ts2");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 1, 'm', null, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 1, 'm', null, null);
        });
    }

    @Test
    public void testCreateMatViewGroupByTimestamp3() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts as ts2, avg from (select timestamp_floor('1m', ts) as ts, avg(v) avg from " + TABLE1 + ")";
            assertExceptionNoLeakCheck(
                    "create materialized view test as (" + query + ") partition by day",
                    34,
                    "TIMESTAMP column does not exist or not present in select list [name=ts]"
            );
            assertNull(getMatViewDefinition("testView"));
        });
    }

    @Test
    public void testCreateMatViewInvalidColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            assertExceptionNoLeakCheck(
                    "create materialized view test as (select ts, avg(no_col_like_this) from " + TABLE1 + " sample by 30s) partition by DAY",
                    49,
                    "Invalid column: no_col_like_this"
            );
            assertNull(getMatViewDefinition("test"));
        });
    }

    @Test
    public void testCreateMatViewInvalidPartitionBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            assertExceptionNoLeakCheck(
                    "create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by 3d",
                    92,
                    "'HOUR', 'DAY', 'WEEK', 'MONTH' or 'YEAR' expected"
            );
            assertNull(getMatViewDefinition("test"));

            assertExceptionNoLeakCheck(
                    "create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by NONE",
                    92,
                    "materialized view has to be partitioned"
            );
            assertNull(getMatViewDefinition("test"));
        });
    }

    @Test
    public void testCreateMatViewInvalidTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, k, avg(v) from " + TABLE1 + " sample by 30s";
            assertExceptionNoLeakCheck(
                    "create materialized view testView as (" + query + ") timestamp(k) partition by week",
                    96,
                    "TIMESTAMP column expected [actual=SYMBOL]"
            );
            assertNull(getMatViewDefinition("testView"));
        });
    }

    @Test
    public void testCreateMatViewInvalidTtl() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            assertExceptionNoLeakCheck(
                    "create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day ttl 12 hours",
                    100,
                    "TTL value must be an integer multiple of partition size"
            );
            assertNull(getMatViewDefinition("test"));
        });
    }

    @Test
    public void testCreateMatViewKeyedSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, k, avg(v), last(v) from " + TABLE1 + " sample by 30s";
            execute("create materialized view test as (" + query + ") partition by day ttl 1 week");

            assertQuery0("ts\tk\tavg\tlast\n", "test", "ts");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertFalse(metadata.isDedupKey(0));
                assertFalse(metadata.isDedupKey(1));
                assertFalse(metadata.isDedupKey(2));
                assertFalse(metadata.isDedupKey(3));
                assertEquals(7 * 24, metadata.getTtlHoursOrMonths());
            }
        });
    }

    @Test
    public void testCreateMatViewManual() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, k, max(v) as v_max from " + TABLE1 + " sample by 30s";
            execute("CREATE MATERIALIZED VIEW test REFRESH MANUAL AS " + query);

            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_MANUAL, "test", query, TABLE1, 30, 's', null, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_MANUAL, "test", query, TABLE1, 30, 's', null, null);

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertEquals(0, metadata.getTimestampIndex());
                assertFalse(metadata.isDedupKey(0));
                assertFalse(metadata.isDedupKey(1));
            }
        });
    }

    @Test
    public void testCreateMatViewMultipleTables() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);
            createTable(TABLE3);

            assertExceptionNoLeakCheck(
                    "create materialized view test as (select t1.ts, avg(t1.v) from " + TABLE1 + " as t1 " +
                            "join " + TABLE2 + " as t2 on v sample by 30s) partition by day",
                    34,
                    "more than one table used in query, base table has to be set using 'WITH BASE'"
            );
            assertNull(getMatViewDefinition("test"));

            assertExceptionNoLeakCheck(
                    "create materialized view test as (select ts, avg(v) from " + TABLE3 + " sample by 30s " +
                            "union select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day",
                    34,
                    "more than one table used in query, base table has to be set using 'WITH BASE'"
            );
            assertNull(getMatViewDefinition("test"));

            assertExceptionNoLeakCheck(
                    "create materialized view test as (select ts, avg(v) from " + TABLE3 + " sample by 30s " +
                            "union select t1.ts, avg(t1.v) from " + TABLE1 + " as t1 join " + TABLE2 +
                            " as t2 on v sample by 30s) partition by day",
                    34,
                    "more than one table used in query, base table has to be set using 'WITH BASE'"
            );
            assertNull(getMatViewDefinition("test"));
        });
    }

    @Test
    public void testCreateMatViewNoAggrFunction() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            assertExceptionNoLeakCheck(
                    "create materialized view test as (select ts from " + TABLE1 + " sample by 10s) partition by day",
                    66,
                    "at least one aggregation function must be present in 'select' clause"
            );
            assertNull(getMatViewDefinition("test"));
        });
    }

    @Test
    public void testCreateMatViewNoDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table stocks_d1_ohlcv (" +
                    "ts timestamp, ticker symbol, close double" +
                    ") timestamp(ts) partition by day WAL");

            final String query =
                    "  WITH t1 AS (\n" +
                            "    SELECT ts, ticker, greatest(close, close + 1) as close\n" +
                            "    FROM stocks_d1_ohlcv\n" +
                            "  )\n" +
                            "  SELECT ts, ticker, avg(close)\n" +
                            "  FROM t1\n" +
                            "  SAMPLE BY 1d\n" +
                            "  ORDER BY ticker, ts\n";
            assertExceptionNoLeakCheck(
                    "create materialized view test_view as (" + query + ") partition by month",
                    39,
                    "materialized view query is required to have designated timestamp"
            );
            assertNull(getMatViewDefinition("testView"));
        });
    }

    @Test
    public void testCreateMatViewNoPartitionBy() throws Exception {
        testCreateMatViewNoPartitionBy(true);
    }

    @Test
    public void testCreateMatViewNoPartitionByInvalidTtl() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            assertExceptionNoLeakCheck(
                    "create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) ttl 12 hours",
                    83,
                    "TTL value must be an integer multiple of partition size"
            );
            assertNull(getMatViewDefinition("test"));
        });
    }

    @Test
    public void testCreateMatViewNoPartitionByNoParentheses() throws Exception {
        testCreateMatViewNoPartitionBy(false);
    }

    @Test
    public void testCreateMatViewNoSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            assertExceptionNoLeakCheck(
                    "create materialized view test as (select * from " + TABLE1 + " where v % 2 = 0) partition by day",
                    34,
                    "TIMESTAMP column is not present in select list"
            );
            assertNull(getMatViewDefinition("test"));
        });
    }

    @Test
    public void testCreateMatViewNoTables() throws Exception {
        assertMemoryLeak(() -> {
            assertExceptionNoLeakCheck(
                    "create materialized view test as (select 1 from long_sequence(1)) partition by day",
                    34,
                    "missing base table, materialized views have to be based on a table"
            );
            assertNull(getMatViewDefinition("test"));
        });
    }

    @Test
    public void testCreateMatViewNoTimestampInSelect() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            assertExceptionNoLeakCheck(
                    "create materialized view test as (select k, max(v) as v_max from " + TABLE1 + " sample by 30s) partition by day",
                    34,
                    "TIMESTAMP column does not exist or not present in select list [name=ts]"
            );
            assertNull(getMatViewDefinition("test"));
        });
    }

    @Test
    public void testCreateMatViewNonDeterministicFunction() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            // nested in SELECT
            assertExceptionNoLeakCheck(
                    "create materialized view test as select coalesce(ts, now()) ts, avg(v) from " + TABLE1 + " sample by 30s",
                    53,
                    "non-deterministic function cannot be used in materialized view: now"
            );
            assertNull(getMatViewDefinition("test"));

            // WHERE clause
            assertExceptionNoLeakCheck(
                    "create materialized view test as (select ts, avg(v) from " + TABLE1 + " where ts in today() sample by 30s) partition by month",
                    76,
                    "non-deterministic function cannot be used in materialized view: today"
            );
            assertNull(getMatViewDefinition("test"));

            assertExceptionNoLeakCheck(
                    "create materialized view test as select ts, avg(v) from " + TABLE1 + " where k is not null and now() = '2020-01-01' sample by 30s",
                    87,
                    "non-deterministic function cannot be used in materialized view: now"
            );
            assertNull(getMatViewDefinition("test"));

            // IN (cursor)
            assertExceptionNoLeakCheck(
                    "create materialized view test as select ts, avg(v) from " + TABLE1 + " where ts in (select now()) sample by 30s",
                    83,
                    "non-deterministic function cannot be used in materialized view: now"
            );
            assertNull(getMatViewDefinition("test"));

            // JOIN
            assertExceptionNoLeakCheck(
                    "create materialized view test with base " + TABLE1 + " as select t1.ts, max(t2.dt) from " + TABLE1 + " t1 " +
                            "left outer join (select k, sysdate() dt from " + TABLE2 + ") t2 on (k) sample by 30s",
                    117,
                    "non-deterministic function cannot be used in materialized view: sysdate"
            );
            assertNull(getMatViewDefinition("test"));

            // UNION
            assertExceptionNoLeakCheck(
                    "create materialized view test with base " + TABLE1 + " as select t1.ts, max(t2.ts) from " + TABLE1 + " t1 " +
                            "left outer join (select 'a' k, '2020-01-01' ts union all select 'b' k, systimestamp() ts) t2 on (k) sample by 30s",
                    161,
                    "non-deterministic function cannot be used in materialized view: systimestamp"
            );
            assertNull(getMatViewDefinition("test"));

            // ORDER BY
            assertExceptionNoLeakCheck(
                    "create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s order by rnd_int(), ts)",
                    87,
                    "non-deterministic function cannot be used in materialized view: rnd_int"
            );
            assertNull(getMatViewDefinition("test"));
        });
    }

    @Test
    public void testCreateMatViewNonDeterministicFunctionInSelect() throws Exception {
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

        assertMemoryLeak(() -> {
            for (String[] func : functions) {
                testCreateMatViewNonDeterministicFunctionInSelect(func[0], func[1]);
            }
        });
    }

    @Test
    public void testCreateMatViewNonOptimizedSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, avg(v) from (select ts, k, v+10 as v from " + TABLE1 + ") sample by 30s";
            execute("create materialized view test as (" + query + ") partition by week");

            assertQuery0("ts\tavg\n", "test", "ts");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);
        });
    }

    @Test
    public void testCreateMatViewNonOptimizedSampleByMultipleNanoTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1, true, "timestamp_ns");

            final String query = "select ts, 1L::timestamp_ns as ts2, avg(v) from (select ts, k, v+10 as v from " + TABLE1 + ") sample by 30s";
            execute("create materialized view test as (" + query + ") partition by week");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertEquals(0, metadata.getTimestampIndex());
                assertFalse(metadata.isDedupKey(0));
                assertFalse(metadata.isDedupKey(1));
                assertFalse(metadata.isDedupKey(2));
                assertEquals(0, metadata.getTtlHoursOrMonths());
            }
        });
    }

    @Test
    public void testCreateMatViewNonOptimizedSampleByMultipleTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, 1L::timestamp as ts2, avg(v) from (select ts, k, v+10 as v from " + TABLE1 + ") sample by 30s";
            execute("create materialized view test as (" + query + ") partition by week");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertEquals(0, metadata.getTimestampIndex());
                assertFalse(metadata.isDedupKey(0));
                assertFalse(metadata.isDedupKey(1));
                assertFalse(metadata.isDedupKey(2));
                assertEquals(0, metadata.getTtlHoursOrMonths());
            }
        });
    }

    @Test
    public void testCreateMatViewNonWalBaseTable() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1, false, "timestamp");
            assertExceptionNoLeakCheck(
                    "create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day",
                    57,
                    "base table has to be WAL enabled"
            );
            assertNull(getMatViewDefinition("test"));
        });
    }

    @Test
    public void testCreateMatViewRewrittenSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, avg(v) from " + TABLE1 + " sample by 30s";
            execute("create materialized view test as (" + query + ") partition by day");

            assertQuery0("ts\tavg\n", "test", "ts");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);
        });
    }

    @Test
    public void testCreateMatViewRewrittenSampleByMultipleTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE3);

            final String query = "select ts, 1L::timestamp as ts2, avg(v) from " + TABLE3 + " sample by 30s";
            execute("create materialized view test_view as (" + query + ") partition by day");

            assertQuery0("ts\tts2\tavg\n", "test_view", "ts");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test_view", query, TABLE3, 30, 's', null, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test_view", query, TABLE3, 30, 's', null, null);
        });
    }

    @Test
    public void testCreateMatViewSampleByAlignToFirstObservation() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            final String query = "select ts, avg(v) from " + TABLE1 + " sample by 1d align to first observation";
            assertExceptionNoLeakCheck(
                    "create materialized view test as (" + query + ") partition by day",
                    77,
                    "ALIGN TO FIRST OBSERVATION on base table is not supported for materialized views: " + TABLE1
            );
        });
    }

    @Test
    public void testCreateMatViewSampleByFill() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            assertExceptionNoLeakCheck(
                    "create materialized view test as (" +
                            "  select ts, avg(v) from " + TABLE1 + " where ts in '2024' sample by 1d fill(null)" +
                            ") partition by day",
                    103,
                    "FILL on base table is not supported for materialized views: " + TABLE1
            );
            assertExceptionNoLeakCheck(
                    "create materialized view test as (" +
                            "  with b as (" +
                            "    select ts, avg(v) from " + TABLE1 + " sample by 1d fill(null)" +
                            "  )" +
                            "  select a.ts, avg(a.v)" +
                            "  from " + TABLE1 + " a " +
                            "  left outer join b on a.ts = b.ts" +
                            "  sample by 1d" +
                            ") partition by day",
                    99,
                    "FILL on base table is not supported for materialized views: " + TABLE1
            );
        });
    }

    @Test
    public void testCreateMatViewSampleByFromTo() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE2);
            final String from = "select ts, avg(v) from " + TABLE2 + " where ts in '2024' sample by 1d from '2024-03-01'";
            final String to = "select ts, avg(v) from " + TABLE2 + " where ts in '2024' sample by 1d to '2024-06-30'";
            assertExceptionNoLeakCheck(
                    "create materialized view test as (" + from + ") partition by day",
                    101,
                    "FROM-TO on base table is not supported for materialized views: " + TABLE2
            );
            assertExceptionNoLeakCheck(
                    "create materialized view test as (" + to + ") partition by day",
                    99,
                    "FROM-TO on base table is not supported for materialized views: " + TABLE2
            );

            assertExceptionNoLeakCheck(
                    "create materialized view test as (" +
                            "  with b as (" +
                            "    select ts, avg(v) from " + TABLE2 + " sample by 1d from '2024-03-01' to '2024-06-30'" +
                            "  )" +
                            "  select a.ts, avg(a.v)" +
                            "  from " + TABLE2 + " a " +
                            "  left outer join b on a.ts = b.ts" +
                            "  sample by 1d" +
                            ") partition by day",
                    99,
                    "FROM-TO on base table is not supported for materialized views: " + TABLE2
            );
        });
    }

    @Test
    public void testCreateMatViewSampleByNestedFill() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE2);
            final String fill = "with t as ( select ts, avg(v) from " + TABLE2 + " sample by 1d fill(null)) select ts, avg(v) from t sample by 1d";
            assertExceptionNoLeakCheck(
                    "create materialized view test as (" + fill + ") partition by day",
                    94,
                    "FILL on base table is not supported for materialized views: " + TABLE2
            );
        });
    }

    @Test
    public void testCreateMatViewSampleByNestedFromTo() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE2);
            final String from = "with t as (select ts, avg(v) from " + TABLE2 + " sample by 1d from '2024-03-01') select ts, avg(v) from t sample by 1d";
            final String to = "with t as (select ts, avg(v) from " + TABLE2 + " sample by 1d to '2024-06-30') select ts, avg(v) from t sample by 1d";
            assertExceptionNoLeakCheck(
                    "create materialized view test as (" + from + ") partition by day",
                    93,
                    "FROM-TO on base table is not supported for materialized views: " + TABLE2
            );
            assertExceptionNoLeakCheck(
                    "create materialized view test as (" + to + ") partition by day",
                    91,
                    "FROM-TO on base table is not supported for materialized views: " + TABLE2
            );
        });
    }

    @Test
    public void testCreateMatViewSampleByTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String tz = "Europe/Berlin";
            final String query = "select ts, avg(v) from " + TABLE1 + " where ts in '2024' sample by 1d align to calendar time zone '" + tz + "'";
            execute("create materialized view test as (" + query + ") partition by day");

            assertQuery0("ts\tavg\n", "test", "ts");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 1, 'd', tz, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 1, 'd', tz, null);
        });
    }

    @Test
    public void testCreateMatViewSampleByTimeZoneFixedFormat() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String tz = "GMT+02:00";
            final String query = "select ts, avg(v) from " + TABLE1 + " where ts in '2024' sample by 1d align to calendar time zone '" + tz + "'";
            execute("create materialized view test as (" + query + ") partition by day");

            assertQuery0("ts\tavg\n", "test", "ts");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 1, 'd', tz, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 1, 'd', tz, null);
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

            assertQuery0("ts\tavg\n", "test", "ts");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 1, 'd', tz, offset);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 1, 'd', tz, offset);
        });
    }

    @Test
    public void testCreateMatViewSampleByWithOffset() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String offset = "00:45";
            final String query = "select ts, avg(v) from " + TABLE1 + " where ts in '2024' sample by 1d align to calendar with offset '" + offset + "'";
            execute("create materialized view test as (" + query + ") partition by day");

            assertQuery0("ts\tavg\n", "test", "ts");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 1, 'd', null, offset);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 1, 'd', null, offset);
        });
    }

    @Test
    public void testCreateMatViewSetRefreshLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, k, max(v) as v_max from " + TABLE1 + " sample by 30s";
            execute("CREATE MATERIALIZED VIEW test AS (" + query + ") PARTITION BY WEEK TTL 3 WEEKS;");
            execute("ALTER MATERIALIZED VIEW test SET REFRESH LIMIT 1m;");
            drainWalQueue();
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null, -1, 0, (char) 0, Numbers.LONG_NULL, null, 0, (char) 0, 0, (char) 0);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null, -1, 0, (char) 0, Numbers.LONG_NULL, null, 0, (char) 0, 0, (char) 0);

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertEquals(0, metadata.getTimestampIndex());
                assertFalse(metadata.isDedupKey(0));
                assertFalse(metadata.isDedupKey(1));
                assertFalse(metadata.isDedupKey(2));
                assertEquals(3 * 7 * 24, metadata.getTtlHoursOrMonths());
            }
        });
    }

    @Test
    public void testCreateMatViewTsAlias() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            final String query = "select ts, ts as ts1, max(v) as v_max from " + TABLE1 + " sample by 30s";
            execute("create materialized view test as (" + query + ") partition by week");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);
        });
    }

    @Test
    public void testCreateMatViewTsCast() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            final String query = "select ts, last(ts) as last_ts, cast(ts as long) as tm, concat(k, '10') as k, max(v) as v_max from " + TABLE1 + " sample by 30s";
            execute("create materialized view test as (" + query + ") partition by week");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);
        });
    }

    @Test
    public void testCreateMatViewTtlNoPartitionBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, k, max(v) as v_max from " + TABLE1 + " sample by 1h";
            execute("CREATE MATERIALIZED VIEW test AS (" + query + ") TTL 1 MONTH;");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 1, 'h', null, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 1, 'h', null, null);

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertEquals(0, metadata.getTimestampIndex());
                assertFalse(metadata.isDedupKey(0));
                assertFalse(metadata.isDedupKey(1));
                assertFalse(metadata.isDedupKey(2));
                assertEquals(-1, metadata.getTtlHoursOrMonths());
            }
        });
    }

    @Test
    public void testCreateMatViewWindowFunctions() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            final String query = "select ts, max(v) over (order by ts) as v_max from " + TABLE1 + " sample by 30s";
            assertExceptionNoLeakCheck(
                    "create materialized view test as (" + query + ") partition by week",
                    45,
                    "window function on base table is not supported for materialized views: " + TABLE1
            );
            assertExceptionNoLeakCheck(
                    "create materialized view test as (" +
                            "  with b as (" +
                            "    select ts, avg(v) over (order by ts) as v_max from " + TABLE1 + " sample by 30s" +
                            "  )" +
                            "  select a.ts, avg(a.v)" +
                            "  from " + TABLE1 + " a " +
                            "  left outer join b on a.ts = b.ts" +
                            "  sample by 1d" +
                            ") partition by day",
                    62,
                    "window function on base table is not supported for materialized views: " + TABLE1
            );
        });
    }

    @Test
    public void testCreateMatViewWithBase() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            final String query = "select t1.ts, avg(t1.v) from " + TABLE1 + " as t1 join " + TABLE2 + " as t2 on v sample by 60s";
            execute("create materialized view test with base " + TABLE1 + " as (" + query + ") partition by day");

            assertQuery0("ts\tavg\n", "test", "ts");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 60, 's', null, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 60, 's', null, null);
        });
    }

    @Test
    public void testCreateMatViewWithExistingTableName() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            assertExceptionNoLeakCheck(
                    "create materialized view " + TABLE2 + " as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day",
                    25,
                    "table with the requested name already exists"
            );

            assertExceptionNoLeakCheck(
                    "create materialized view if not exists " + TABLE2 + " as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day",
                    39,
                    "table with the requested name already exists"
            );

            final String query = "select ts, avg(v) from " + TABLE2 + " sample by 4h";
            execute("create materialized view test as (" + query + ") partition by day");

            // without IF NOT EXISTS
            // assertMatViewDefinition() fails with "definition is null" when this assertException is called!
            assertExceptionNoLeakCheck(
                    "create materialized view test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day",
                    25,
                    "view already exists"
            );

            // with IF NOT EXISTS
            execute("create materialized view if not exists test as (select ts, avg(v) from " + TABLE1 + " sample by 30s) partition by day");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE2, 4, 'h', null, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE2, 4, 'h', null, null);

            assertExceptionNoLeakCheck(
                    "create table test(ts timestamp, col varchar) timestamp(ts) partition by day wal",
                    13,
                    "materialized view with the requested name already exists"
            );
        });
    }

    @Test
    public void testCreateMatViewWithIndex() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, k, avg(v) from " + TABLE1 + " sample by 30s";
            execute("create materialized view test as (" + query + "), index (k) partition by day");

            assertQuery0("ts\tk\tavg\n", "test", "ts");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertFalse(metadata.isDedupKey(0));
                assertFalse(metadata.isDedupKey(1));
                assertFalse(metadata.isDedupKey(2));

                assertFalse(metadata.isColumnIndexed(0));
                assertTrue(metadata.isColumnIndexed(1));
                assertFalse(metadata.isColumnIndexed(2));

                assertEquals(0, metadata.getTtlHoursOrMonths());
            }
        });
    }

    @Test
    public void testCreateMatViewWithInvalidVolume() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            try {
                execute(
                        "CREATE MATERIALIZED VIEW testView AS (SELECT ts, avg(v) FROM " + TABLE1 +
                                " SAMPLE BY 30s) PARTITION BY DAY IN VOLUME aaa"
                );
                fail("CREATE statement should have failed");
            } catch (SqlException e) {
                if (Os.isWindows()) {
                    TestUtils.assertContains("'in volume' is not supported on Windows", e.getFlyweightMessage());
                    Assert.assertEquals(103, e.getPosition());
                } else {
                    TestUtils.assertContains("volume alias is not allowed [alias=aaa]", e.getFlyweightMessage());
                    Assert.assertEquals(110, e.getPosition());
                }
            }
        });
    }

    @Test
    public void testCreateMatViewWithNonBaseTableKeys() throws Exception {
        testCreateMatViewWithNonBaseTableKeys("timestamp");
    }

    @Test
    public void testCreateMatViewWithNonBaseTableKeysWithNanos() throws Exception {
        testCreateMatViewWithNonBaseTableKeys("timestamp_ns");
    }

    @Test
    public void testCreateMatViewWithNonDedupBaseKeys() throws Exception {
        testCreateMatViewWithNonDedupBaseKeys("timestamp");
    }

    @Test
    public void testCreateMatViewWithNonDedupBaseKeysWithNanos() throws Exception {
        testCreateMatViewWithNonDedupBaseKeys("timestamp_ns");
    }

    @Test
    public void testCreateMatViewWithOperator() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s";
            execute("create materialized view test as (" + query + ") partition by day");

            assertQuery0("ts\tdoubleV\tavg\n", "test", "ts");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 30, 's', null, null);
        });
    }

    @Test
    public void testCreatePeriodMatView1() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            currentMicros = parseFloorPartialTimestamp("2002-01-01T23:01:00.000000Z");
            final long expectedStart = parseFloorPartialTimestamp("2002-01-02T00:00:00.000000Z");
            final String query = "select ts, k, max(v) as v_max from " + TABLE1 + " sample by 30s";
            execute(
                    "CREATE MATERIALIZED VIEW test REFRESH EVERY 6h PERIOD (LENGTH 1d TIME ZONE 'Europe/Berlin' DELAY 3h) AS (" +
                            query +
                            ") PARTITION BY MONTH;"
            );
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_TIMER, "test", query, TABLE1, 30, 's', null, null, 0, 6, 'h', expectedStart, "Europe/Berlin", 1, 'd', 3, 'h');
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_TIMER, "test", query, TABLE1, 30, 's', null, null, 0, 6, 'h', expectedStart, "Europe/Berlin", 1, 'd', 3, 'h');

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertEquals(0, metadata.getTimestampIndex());
                assertFalse(metadata.isDedupKey(0));
                assertFalse(metadata.isDedupKey(1));
            }
        });
    }

    @Test
    public void testCreatePeriodMatView2() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            currentMicros = parseFloorPartialTimestamp("2002-01-01T12:00:00.000000Z");
            final long expectedStart = parseFloorPartialTimestamp("2002-01-01T12:00:00.000000Z");
            final String query = "select ts, k, max(v) as v_max from " + TABLE1 + " sample by 60s";
            execute(
                    "CREATE MATERIALIZED VIEW test REFRESH MANUAL PERIOD (LENGTH 12h) AS (" +
                            query +
                            ") PARTITION BY MONTH;"
            );
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_MANUAL, "test", query, TABLE1, 60, 's', null, null, 0, 0, (char) 0, expectedStart, null, 12, 'h', 0, (char) 0);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_MANUAL, "test", query, TABLE1, 60, 's', null, null, 0, 0, (char) 0, expectedStart, null, 12, 'h', 0, (char) 0);

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertEquals(0, metadata.getTimestampIndex());
                assertFalse(metadata.isDedupKey(0));
                assertFalse(metadata.isDedupKey(1));
            }
        });
    }

    @Test
    public void testCreatePeriodMatViewModelToSink() throws Exception {
        final String query = "select ts, k, avg(v) from " + TABLE1 + " sample by 30s";
        final DdlSerializationTest[] tests = new DdlSerializationTest[]{
                new DdlSerializationTest(
                        "create materialized view test1 refresh immediate period (length 12h time zone 'Europe/Sofia' delay 1h) as " + query,
                        "create materialized view test1 with base " + TABLE1 + " refresh immediate period (length 12h time zone 'Europe/Sofia' delay 1h) " +
                                "as (select-choose ts, k, avg(v) avg from (table1 sample by 30s align to calendar with offset '00:00'))"
                ),
                new DdlSerializationTest(
                        "create materialized view test2 refresh manual period (length 10h time zone 'Europe/Sofia' delay 2h) as " + query,
                        "create materialized view test2 with base " + TABLE1 + " refresh manual period (length 10h time zone 'Europe/Sofia' delay 2h) " +
                                "as (select-choose ts, k, avg(v) avg from (table1 sample by 30s align to calendar with offset '00:00'))"
                ),
                new DdlSerializationTest(
                        "create materialized view test3 refresh every 2d period (length 8h time zone 'Europe/Sofia' delay 3h) as " + query,
                        "create materialized view test3 with base " + TABLE1 + " refresh every 2d period (length 8h time zone 'Europe/Sofia' delay 3h) " +
                                "as (select-choose ts, k, avg(v) avg from (table1 sample by 30s align to calendar with offset '00:00'))"
                ),
                new DdlSerializationTest(
                        "CREATE MATERIALIZED VIEW 'test4' REFRESH MANUAL PERIOD (LENGTH 60m) AS (" + query + ")",
                        "create materialized view test4 with base " + TABLE1 + " refresh manual period (length 60m) " +
                                "as (select-choose ts, k, avg(v) avg from (table1 sample by 30s align to calendar with offset '00:00'))"
                ),
                new DdlSerializationTest(
                        "CREATE MATERIALIZED VIEW 'test4' REFRESH MANUAL DEFERRED PERIOD (LENGTH 60m) AS (" + query + ")",
                        "create materialized view test4 with base " + TABLE1 + " refresh manual deferred period (length 60m) " +
                                "as (select-choose ts, k, avg(v) avg from (table1 sample by 30s align to calendar with offset '00:00'))"
                ),
        };
        testModelToSink(tests);
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
                        // Use test alloc() function to make sure that we always free the factory.
                        execute(
                                "create materialized view if not exists price_1h as (" +
                                        "  select sym, alloc(42), last(price) as price, ts from base_price sample by 1h" +
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
                try (MatViewRefreshJob refreshJob = createMatViewRefreshJob()) {
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
    public void testCreateTimerMatView1() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String start = "2002-01-01T00:00:00.000000Z";
            final long startEpoch = parseFloorPartialTimestamp(start);
            final String query = "select ts, k, max(v) as v_max from " + TABLE1 + " sample by 30s";
            execute(
                    "CREATE MATERIALIZED VIEW test REFRESH EVERY 5m START '" + start + "' TIME ZONE 'Europe/Berlin' AS (" +
                            query +
                            ") PARTITION BY YEAR;"
            );
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_TIMER, "test", query, TABLE1, 30, 's', null, null, 0, 5, 'm', startEpoch, "Europe/Berlin", 0, (char) 0, 0, (char) 0);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_TIMER, "test", query, TABLE1, 30, 's', null, null, 0, 5, 'm', startEpoch, "Europe/Berlin", 0, (char) 0, 0, (char) 0);

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertEquals(0, metadata.getTimestampIndex());
                assertFalse(metadata.isDedupKey(0));
                assertFalse(metadata.isDedupKey(1));
            }
        });
    }

    @Test
    public void testCreateTimerMatView2() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String start = "2002-01-01T00:00:00.000000Z";
            final long startEpoch = parseFloorPartialTimestamp(start);
            final String query = "select ts, k, max(v) as v_max from " + TABLE1 + " sample by 30s";
            currentMicros = startEpoch;
            execute(
                    "CREATE MATERIALIZED VIEW test REFRESH EVERY 5m AS (" +
                            query +
                            ") PARTITION BY YEAR;"
            );
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_TIMER, "test", query, TABLE1, 30, 's', null, null, 0, 5, 'm', startEpoch, null, 0, (char) 0, 0, (char) 0);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_TIMER, "test", query, TABLE1, 30, 's', null, null, 0, 5, 'm', startEpoch, null, 0, (char) 0, 0, (char) 0);

            try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists("test"))) {
                assertEquals(0, metadata.getTimestampIndex());
                assertFalse(metadata.isDedupKey(0));
                assertFalse(metadata.isDedupKey(1));
            }

            final String start2 = "2001-02-03T00:00:00.000000Z";
            final long startEpoch2 = parseFloorPartialTimestamp(start2);
            currentMicros = startEpoch2;
            execute("ALTER MATERIALIZED VIEW test SET REFRESH EVERY 15d;");
            drainWalQueue();

            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_TIMER, "test", query, TABLE1, 30, 's', null, null, 0, 15, 'd', startEpoch2, null, 0, (char) 0, 0, (char) 0);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_TIMER, "test", query, TABLE1, 30, 's', null, null, 0, 15, 'd', startEpoch2, null, 0, (char) 0, 0, (char) 0);
        });
    }

    @Test
    public void testCreateTimerMatViewModelToSink() throws Exception {
        final String query = "select ts, k, avg(v) from " + TABLE1 + " sample by 30s";
        final DdlSerializationTest[] tests = new DdlSerializationTest[]{
                new DdlSerializationTest(
                        "create materialized view test refresh every 42m start '2021-01-01T01:01:00.000000Z' time zone 'Europe/Berlin' as " + query,
                        "create materialized view test with base " + TABLE1 + " refresh " +
                                "every 42m start '2021-01-01T01:01:00.000000Z' time zone 'Europe/Berlin' as (" +
                                "select-choose ts, k, avg(v) avg from (table1 sample by 30s align to calendar with offset '00:00')" +
                                ")"
                ),
                new DdlSerializationTest(
                        "create materialized view test refresh every 42m deferred start '2021-01-01T01:01:00.000000Z' time zone 'Europe/Berlin' as " + query,
                        "create materialized view test with base " + TABLE1 + " refresh " +
                                "every 42m deferred start '2021-01-01T01:01:00.000000Z' time zone 'Europe/Berlin' as (" +
                                "select-choose ts, k, avg(v) avg from (table1 sample by 30s align to calendar with offset '00:00')" +
                                ")"
                ),
        };
        testModelToSink(tests);
    }

    @Test
    public void testEmptyBaseTableSqlInDefinitionBlock() throws Exception {
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
                    AppendableBlock block = writer.append();
                    block.putInt(matViewDefinition.getRefreshType());
                    block.putStr(null);
                    block.putLong(matViewDefinition.getSamplingInterval());
                    block.putChar(matViewDefinition.getSamplingIntervalUnit());
                    block.putStr(matViewDefinition.getTimeZone());
                    block.putStr(matViewDefinition.getTimeZoneOffset());
                    block.putStr(matViewDefinition.getMatViewSql());
                    block.commit(MatViewDefinition.MAT_VIEW_DEFINITION_FORMAT_MSG_TYPE);
                    writer.commit();
                }

                // Reader should fail to load unknown definition file.
                try (BlockFileReader reader = new BlockFileReader(configuration)) {
                    path.of(configuration.getDbRoot());
                    final int rootLen = path.size();
                    MatViewDefinition actualDefinition = new MatViewDefinition();
                    MatViewDefinition.readFrom(
                            engine,
                            actualDefinition,
                            reader,
                            path,
                            rootLen,
                            matViewToken
                    );
                    Assert.fail("exception expected");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "base table name for materialized view is empty");
                }

                try (BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode())) {
                    writer.of(path.of(configuration.getDbRoot()).concat(matViewToken).concat(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME).$());
                    AppendableBlock block = writer.append();
                    block.putInt(matViewDefinition.getRefreshType());
                    block.putStr(matViewDefinition.getBaseTableName());
                    block.putLong(matViewDefinition.getSamplingInterval());
                    block.putChar(matViewDefinition.getSamplingIntervalUnit());
                    block.putStr(matViewDefinition.getTimeZone());
                    block.putStr(matViewDefinition.getTimeZoneOffset());
                    block.putStr(null);
                    block.commit(MatViewDefinition.MAT_VIEW_DEFINITION_FORMAT_MSG_TYPE);
                    writer.commit();
                }

                // Reader should fail to load unknown definition file.
                try (BlockFileReader reader = new BlockFileReader(configuration)) {
                    path.of(configuration.getDbRoot());
                    final int rootLen = path.size();
                    MatViewDefinition actualDefinition = new MatViewDefinition();
                    MatViewDefinition.readFrom(
                            engine,
                            actualDefinition,
                            reader,
                            path,
                            rootLen,
                            matViewToken
                    );
                    Assert.fail("exception expected");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "materialized view SQL is empty");
                }
            }
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
                    block.commit(MatViewDefinition.MAT_VIEW_DEFINITION_FORMAT_MSG_TYPE + 42);
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
                    MatViewDefinition actualDefinition = new MatViewDefinition();
                    MatViewDefinition.readFrom(
                            engine,
                            actualDefinition,
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
                final MatViewState matViewState = engine.getMatViewStateStore().getViewState(matViewToken);
                assertNotNull(matViewState);

                final String invalidationReason = "test invalidation reason";

                try (BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode())) {
                    writer.of(path.of(configuration.getDbRoot()).concat(matViewToken).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
                    // Add unknown block.
                    AppendableBlock block = writer.append();
                    block.putStr("foobar");
                    block.commit(MatViewState.MAT_VIEW_STATE_FORMAT_MSG_TYPE - 1);
                    // Then write mat view state.
                    block = writer.append();
                    MatViewState.appendState(
                            matViewState.getLastRefreshBaseTxn(),
                            matViewState.isInvalid(),
                            invalidationReason,
                            block
                    );
                    block.commit(MatViewState.MAT_VIEW_STATE_FORMAT_MSG_TYPE);
                    block = writer.append();
                    MatViewState.appendTs(matViewState.getLastRefreshFinishTimestampUs(), block);
                    block.commit(MatViewState.MAT_VIEW_STATE_FORMAT_EXTRA_TS_MSG_TYPE);
                    writer.commit();
                }

                // Reader should ignore unknown block.
                try (BlockFileReader reader = new BlockFileReader(configuration)) {
                    reader.of(path.of(configuration.getDbRoot()).concat(matViewToken).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
                    MatViewStateReader actualState = new MatViewStateReader().of(reader, matViewToken);

                    assertEquals(matViewState.isInvalid(), actualState.isInvalid());
                    assertEquals(matViewState.getLastRefreshBaseTxn(), actualState.getLastRefreshBaseTxn());
                    assertEquals(matViewState.getLastRefreshFinishTimestampUs(), actualState.getLastRefreshTimestampUs());
                    TestUtils.assertEquals(invalidationReason, actualState.getInvalidationReason());
                }
            }
        });
    }

    @Test
    public void testMatViewDefinitionInvalidTz() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            final String query = "select ts, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s";
            execute("create materialized view test as (" + query + ") partition by day");
            final TableToken matViewToken = engine.getTableTokenIfExists("test");
            MatViewDefinition def = new MatViewDefinition();
            try {
                def.init(
                        MatViewDefinition.REFRESH_TYPE_IMMEDIATE,
                        false,
                        ColumnType.TIMESTAMP_MICRO,
                        matViewToken,
                        query,
                        TABLE1,
                        30,
                        'K',
                        "Europe/Berlin",
                        "00:00",
                        0,
                        0,
                        (char) 0,
                        Numbers.LONG_NULL,
                        null,
                        0,
                        (char) 0,
                        0,
                        (char) 0
                );
                Assert.fail("exception expected");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid sampling interval and/or unit: 30, K");
            }

            try {
                def.init(
                        MatViewDefinition.REFRESH_TYPE_IMMEDIATE,
                        false,
                        ColumnType.TIMESTAMP_MICRO,
                        matViewToken,
                        query,
                        TABLE1,
                        30,
                        's',
                        "Oceania",
                        "00:00",
                        0,
                        0,
                        (char) 0,
                        Numbers.LONG_NULL,
                        "Europe/Berlin",
                        0,
                        (char) 0,
                        0,
                        (char) 0
                );
                Assert.fail("exception expected");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid timezone: Oceania");
            }

            try {
                def.init(
                        MatViewDefinition.REFRESH_TYPE_IMMEDIATE,
                        false,
                        ColumnType.TIMESTAMP_MICRO,
                        matViewToken,
                        query,
                        TABLE1,
                        30,
                        's',
                        "Europe/Berlin",
                        "T00:00",
                        0,
                        0,
                        (char) 0,
                        Numbers.LONG_NULL,
                        null,
                        0,
                        (char) 0,
                        0,
                        (char) 0
                );
                Assert.fail("exception expected");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid offset: T00:00");
            }
        });
    }

    @Test
    public void testMatViewStateFileBackwardsCompatibility() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                createTable(TABLE1);

                final String query = "select ts, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s";
                execute("create materialized view test as (" + query + ") partition by day");
                drainQueues();

                final TableToken matViewToken = engine.getTableTokenIfExists("test");
                final MatViewDefinition matViewDefinition = engine.getMatViewGraph().getViewDefinition(matViewToken);
                assertNotNull(matViewDefinition);
                final MatViewState matViewState = engine.getMatViewStateStore().getViewState(matViewToken);
                assertNotNull(matViewState);

                final String invalidationReason = "test invalidation reason";

                // add V1 block, no last refresh timestamp / period hi
                try (BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode())) {
                    writer.of(path.of(configuration.getDbRoot()).concat(matViewToken).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
                    final AppendableBlock block = writer.append();
                    block.putBool(matViewState.isInvalid());
                    block.putLong(matViewState.getLastRefreshBaseTxn());
                    block.putStr(invalidationReason);
                    block.commit(MatViewState.MAT_VIEW_STATE_FORMAT_MSG_TYPE);
                    writer.commit();
                }

                // expect V1 state format, no last refresh timestamp / period hi
                try (BlockFileReader reader = new BlockFileReader(configuration)) {
                    reader.of(path.of(configuration.getDbRoot()).concat(matViewToken).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
                    MatViewStateReader actualState = new MatViewStateReader().of(reader, matViewToken);

                    assertEquals(matViewState.isInvalid(), actualState.isInvalid());
                    assertEquals(matViewState.getLastRefreshBaseTxn(), actualState.getLastRefreshBaseTxn());
                    assertEquals(Numbers.LONG_NULL, actualState.getLastRefreshTimestampUs());
                    TestUtils.assertEquals(invalidationReason, actualState.getInvalidationReason());
                    assertEquals(Numbers.LONG_NULL, actualState.getLastPeriodHi());

                    // add V1-V3 blocks
                    try (BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode())) {
                        writer.of(path.of(configuration.getDbRoot()).concat(matViewToken).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
                        MatViewState.append(actualState, writer);
                    }
                }

                // expect V3 state format
                try (BlockFileReader reader = new BlockFileReader(configuration)) {
                    reader.of(path.of(configuration.getDbRoot()).concat(matViewToken).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
                    MatViewStateReader actualState = new MatViewStateReader().of(reader, matViewToken);

                    assertEquals(matViewState.isInvalid(), actualState.isInvalid());
                    assertEquals(matViewState.getLastRefreshBaseTxn(), actualState.getLastRefreshBaseTxn());
                    assertEquals(Numbers.LONG_NULL, actualState.getLastRefreshTimestampUs());
                    TestUtils.assertEquals(invalidationReason, actualState.getInvalidationReason());
                    assertEquals(Numbers.LONG_NULL, actualState.getLastPeriodHi());
                }

                // add V1 block, then V2 block
                try (BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode())) {
                    writer.of(path.of(configuration.getDbRoot()).concat(matViewToken).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
                    AppendableBlock block = writer.append();
                    block.putBool(matViewState.isInvalid());
                    block.putLong(matViewState.getLastRefreshBaseTxn());
                    block.putStr(invalidationReason);
                    block.commit(MatViewState.MAT_VIEW_STATE_FORMAT_MSG_TYPE);

                    block = writer.append();
                    MatViewState.appendTs(matViewState.getLastRefreshFinishTimestampUs(), block);
                    block.commit(MatViewState.MAT_VIEW_STATE_FORMAT_EXTRA_TS_MSG_TYPE);

                    block = writer.append();
                    MatViewState.appendPeriodHi(matViewState.getLastPeriodHi(), block);
                    block.commit(MatViewState.MAT_VIEW_STATE_FORMAT_EXTRA_PERIOD_MSG_TYPE);
                    writer.commit();
                }

                // expect V3 state format
                try (BlockFileReader reader = new BlockFileReader(configuration)) {
                    reader.of(path.of(configuration.getDbRoot()).concat(matViewToken).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
                    MatViewStateReader actualState = new MatViewStateReader().of(reader, matViewToken);

                    assertEquals(matViewState.isInvalid(), actualState.isInvalid());
                    assertEquals(matViewState.getLastRefreshBaseTxn(), actualState.getLastRefreshBaseTxn());
                    assertEquals(matViewState.getLastRefreshFinishTimestampUs(), actualState.getLastRefreshTimestampUs());
                    TestUtils.assertEquals(invalidationReason, actualState.getInvalidationReason());
                    assertEquals(matViewState.getLastPeriodHi(), actualState.getLastPeriodHi());
                }
            }
        });
    }

    @Test
    public void testReadDefinitionThrowsOnUnknownRefreshType() throws Exception {
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
                    MatViewDefinition unknownDefinition = new MatViewDefinition();
                    unknownDefinition.init(
                            MatViewDefinition.REFRESH_TYPE_IMMEDIATE + 42,
                            false,
                            ColumnType.TIMESTAMP_MICRO,
                            matViewToken,
                            matViewDefinition.getMatViewSql(),
                            TABLE1,
                            matViewDefinition.getSamplingInterval(),
                            matViewDefinition.getSamplingIntervalUnit(),
                            matViewDefinition.getTimeZone(),
                            matViewDefinition.getTimeZoneOffset(),
                            matViewDefinition.getRefreshLimitHoursOrMonths(),
                            matViewDefinition.getTimerInterval(),
                            matViewDefinition.getTimerUnit(),
                            matViewDefinition.getTimerStartUs(),
                            matViewDefinition.getTimerTimeZone(),
                            matViewDefinition.getPeriodDelay(),
                            matViewDefinition.getPeriodDelayUnit(),
                            matViewDefinition.getPeriodLength(),
                            matViewDefinition.getPeriodLengthUnit()
                    );
                    AppendableBlock block = writer.append();
                    MatViewDefinition.append(unknownDefinition, block);
                    block.commit(MatViewDefinition.MAT_VIEW_DEFINITION_FORMAT_MSG_TYPE);
                    writer.commit();
                }

                // Reader should throw due to unknown refresh type.
                try (BlockFileReader reader = new BlockFileReader(configuration)) {
                    path.of(configuration.getDbRoot());
                    final int rootLen = path.size();
                    try {
                        MatViewDefinition.readFrom(
                                engine,
                                new MatViewDefinition(),
                                reader,
                                path,
                                rootLen,
                                matViewToken
                        );
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "unsupported refresh type");
                    }
                }
            }
        });
    }

    @Test
    public void testShowCreateImmediateMatView() throws Exception {
        final String query = "select ts, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s";
        final DdlSerializationTest[] tests = new DdlSerializationTest[]{
                new DdlSerializationTest(
                        "test1",
                        "create materialized view test1 as (" + query + ") partition by day TTL 3 WEEKS",
                        "ddl\n" +
                                "CREATE MATERIALIZED VIEW 'test1' WITH BASE 'table1' REFRESH IMMEDIATE AS (\n" +
                                "select ts, v+v doubleV, avg(v) from table1 sample by 30s\n" +
                                ") PARTITION BY DAY TTL 3 WEEKS;\n"
                ),
                new DdlSerializationTest(
                        "test2",
                        "create materialized view test2 refresh immediate deferred as (" + query + ") partition by day TTL 3 WEEKS",
                        "ddl\n" +
                                "CREATE MATERIALIZED VIEW 'test2' WITH BASE 'table1' REFRESH IMMEDIATE DEFERRED AS (\n" +
                                "select ts, v+v doubleV, avg(v) from table1 sample by 30s\n" +
                                ") PARTITION BY DAY TTL 3 WEEKS;\n"
                ),
        };
        testShowCreateMaterializedView(tests, "timestamp");
    }

    @Test
    public void testShowCreateManualMatView() throws Exception {
        final String query = "select ts, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s";
        final DdlSerializationTest[] tests = new DdlSerializationTest[]{
                new DdlSerializationTest(
                        "test1",
                        "create materialized view test1 refresh manual as " + query,
                        "ddl\n" +
                                "CREATE MATERIALIZED VIEW 'test1' WITH BASE 'table1' REFRESH MANUAL AS (\n" +
                                "select ts, v+v doubleV, avg(v) from table1 sample by 30s\n" +
                                ") PARTITION BY DAY;\n"
                ),
                new DdlSerializationTest(
                        "test2",
                        "create materialized view test2 refresh manual deferred as " + query,
                        "ddl\n" +
                                "CREATE MATERIALIZED VIEW 'test2' WITH BASE 'table1' REFRESH MANUAL DEFERRED AS (\n" +
                                "select ts, v+v doubleV, avg(v) from table1 sample by 30s\n" +
                                ") PARTITION BY DAY;\n"
                ),
        };
        testShowCreateMaterializedView(tests, "timestamp");
    }

    @Test
    public void testShowCreateMatViewFail() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s";
            execute("create materialized view test as (" + query + ") partition by day");

            assertExceptionNoLeakCheck(
                    "show create materialized test",
                    25,
                    "'view' expected"
            );
        });
    }

    @Test
    public void testShowCreateMatViewFail2() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            assertExceptionNoLeakCheck(
                    "show create materialized view " + TABLE1,
                    30,
                    "materialized view name expected, got table name"
            );
        });
    }

    @Test
    public void testShowCreateMatViewFail3() throws Exception {
        assertException(
                "show create materialized view 'test';",
                30,
                "materialized view does not exist [view=test]"
        );
    }

    @Test
    public void testShowCreatePeriodMatView() throws Exception {
        final String query = "select ts, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s";
        final DdlSerializationTest[] tests = new DdlSerializationTest[]{
                new DdlSerializationTest(
                        "test1",
                        "create materialized view test1 refresh immediate period (length 12h time zone 'Europe/Sofia' delay 1h) as " + query,
                        "ddl\n" +
                                "CREATE MATERIALIZED VIEW 'test1' WITH BASE '" + TABLE1 + "' REFRESH IMMEDIATE PERIOD (LENGTH 12h TIME ZONE 'Europe/Sofia' DELAY 1h) AS (\n" +
                                "select ts, v+v doubleV, avg(v) from table1 sample by 30s\n" +
                                ") PARTITION BY DAY;\n"
                ),
                new DdlSerializationTest(
                        "test2",
                        "create materialized view test2 refresh manual period (length 10h time zone 'Europe/Sofia' delay 2h) as " + query,
                        "ddl\n" +
                                "CREATE MATERIALIZED VIEW 'test2' WITH BASE '" + TABLE1 + "' REFRESH MANUAL PERIOD (LENGTH 10h TIME ZONE 'Europe/Sofia' DELAY 2h) AS (\n" +
                                "select ts, v+v doubleV, avg(v) from table1 sample by 30s\n" +
                                ") PARTITION BY DAY;\n"
                ),
                new DdlSerializationTest(
                        "test3",
                        "create materialized view test3 refresh every 2d period(length 8h time zone 'Europe/Sofia' delay 3h) as " + query,
                        "ddl\n" +
                                "CREATE MATERIALIZED VIEW 'test3' WITH BASE '" + TABLE1 + "' REFRESH EVERY 2d PERIOD (LENGTH 8h TIME ZONE 'Europe/Sofia' DELAY 3h) AS (\n" +
                                "select ts, v+v doubleV, avg(v) from table1 sample by 30s\n" +
                                ") PARTITION BY DAY;\n"
                ),
                new DdlSerializationTest(
                        "test4",
                        "CREATE MATERIALIZED VIEW 'test4' REFRESH MANUAL PERIOD (LENGTH 60m) AS (" + query + ")",
                        "ddl\n" +
                                "CREATE MATERIALIZED VIEW 'test4' WITH BASE '" + TABLE1 + "' REFRESH MANUAL PERIOD (LENGTH 60m) AS (\n" +
                                "select ts, v+v doubleV, avg(v) from table1 sample by 30s\n" +
                                ") PARTITION BY DAY;\n"
                ),
                new DdlSerializationTest(
                        "test5",
                        "CREATE MATERIALIZED VIEW 'test5' REFRESH MANUAL DEFERRED PERIOD (LENGTH 60m) AS (" + query + ")",
                        "ddl\n" +
                                "CREATE MATERIALIZED VIEW 'test5' WITH BASE '" + TABLE1 + "' REFRESH MANUAL DEFERRED PERIOD (LENGTH 60m) AS (\n" +
                                "select ts, v+v doubleV, avg(v) from table1 sample by 30s\n" +
                                ") PARTITION BY DAY;\n"
                ),
        };
        testShowCreateMaterializedView(tests, "timestamp");
    }

    @Test
    public void testShowCreatePeriodMatViewWithNanoTs() throws Exception {
        final String query = "select ts, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s";
        final DdlSerializationTest[] tests = new DdlSerializationTest[]{
                new DdlSerializationTest(
                        "test1",
                        "create materialized view test1 refresh immediate period (length 12h time zone 'Europe/Sofia' delay 1h) as " + query,
                        "ddl\n" +
                                "CREATE MATERIALIZED VIEW 'test1' WITH BASE '" + TABLE1 + "' REFRESH IMMEDIATE PERIOD (LENGTH 12h TIME ZONE 'Europe/Sofia' DELAY 1h) AS (\n" +
                                "select ts, v+v doubleV, avg(v) from table1 sample by 30s\n" +
                                ") PARTITION BY DAY;\n"
                ),
                new DdlSerializationTest(
                        "test2",
                        "create materialized view test2 refresh manual period (length 10h time zone 'Europe/Sofia' delay 2h) as " + query,
                        "ddl\n" +
                                "CREATE MATERIALIZED VIEW 'test2' WITH BASE '" + TABLE1 + "' REFRESH MANUAL PERIOD (LENGTH 10h TIME ZONE 'Europe/Sofia' DELAY 2h) AS (\n" +
                                "select ts, v+v doubleV, avg(v) from table1 sample by 30s\n" +
                                ") PARTITION BY DAY;\n"
                ),
                new DdlSerializationTest(
                        "test3",
                        "create materialized view test3 refresh every 2d period(length 8h time zone 'Europe/Sofia' delay 3h) as " + query,
                        "ddl\n" +
                                "CREATE MATERIALIZED VIEW 'test3' WITH BASE '" + TABLE1 + "' REFRESH EVERY 2d PERIOD (LENGTH 8h TIME ZONE 'Europe/Sofia' DELAY 3h) AS (\n" +
                                "select ts, v+v doubleV, avg(v) from table1 sample by 30s\n" +
                                ") PARTITION BY DAY;\n"
                ),
                new DdlSerializationTest(
                        "test4",
                        "CREATE MATERIALIZED VIEW 'test4' REFRESH MANUAL PERIOD (LENGTH 60m) AS (" + query + ")",
                        "ddl\n" +
                                "CREATE MATERIALIZED VIEW 'test4' WITH BASE '" + TABLE1 + "' REFRESH MANUAL PERIOD (LENGTH 60m) AS (\n" +
                                "select ts, v+v doubleV, avg(v) from table1 sample by 30s\n" +
                                ") PARTITION BY DAY;\n"
                ),
                new DdlSerializationTest(
                        "test5",
                        "CREATE MATERIALIZED VIEW 'test5' REFRESH MANUAL DEFERRED PERIOD (LENGTH 60m) AS (" + query + ")",
                        "ddl\n" +
                                "CREATE MATERIALIZED VIEW 'test5' WITH BASE '" + TABLE1 + "' REFRESH MANUAL DEFERRED PERIOD (LENGTH 60m) AS (\n" +
                                "select ts, v+v doubleV, avg(v) from table1 sample by 30s\n" +
                                ") PARTITION BY DAY;\n"
                ),
        };
        testShowCreateMaterializedView(tests, "timestamp_ns");
    }

    @Test
    public void testShowCreateTimerMatView() throws Exception {
        final String query = "select ts, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s";
        final DdlSerializationTest[] tests = new DdlSerializationTest[]{
                new DdlSerializationTest(
                        "test1",
                        "create materialized view test1 refresh every 7d start '2020-01-01T02:23:59.900000Z' time zone 'Europe/Paris' as " + query,
                        "ddl\n" +
                                "CREATE MATERIALIZED VIEW 'test1' WITH BASE 'table1' REFRESH EVERY 7d START '2020-01-01T02:23:59.900000Z' TIME ZONE 'Europe/Paris' AS (\n" +
                                "select ts, v+v doubleV, avg(v) from table1 sample by 30s\n" +
                                ") PARTITION BY DAY;\n"
                ),
                new DdlSerializationTest(
                        "test2",
                        "create materialized view 'test2' refresh every 7d deferred start '2020-01-01T02:23:59.900000Z' time zone 'Europe/Paris' as " + query,
                        "ddl\n" +
                                "CREATE MATERIALIZED VIEW 'test2' WITH BASE 'table1' REFRESH EVERY 7d DEFERRED START '2020-01-01T02:23:59.900000Z' TIME ZONE 'Europe/Paris' AS (\n" +
                                "select ts, v+v doubleV, avg(v) from table1 sample by 30s\n" +
                                ") PARTITION BY DAY;\n"
                ),
        };
        testShowCreateMaterializedView(tests, "timestamp");
    }

    @Test
    public void testShowCreateTimerMatViewWithNanoTs() throws Exception {
        final String query = "select ts, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s";
        final DdlSerializationTest[] tests = new DdlSerializationTest[]{
                new DdlSerializationTest(
                        "test1",
                        "create materialized view test1 refresh every 7d start '2020-01-01T02:23:59.900000Z' time zone 'Europe/Paris' as " + query,
                        "ddl\n" +
                                "CREATE MATERIALIZED VIEW 'test1' WITH BASE 'table1' REFRESH EVERY 7d START '2020-01-01T02:23:59.900000Z' TIME ZONE 'Europe/Paris' AS (\n" +
                                "select ts, v+v doubleV, avg(v) from table1 sample by 30s\n" +
                                ") PARTITION BY DAY;\n"
                ),
                new DdlSerializationTest(
                        "test2",
                        "create materialized view 'test2' refresh every 7d deferred start '2020-01-01T02:23:59.900000000Z' time zone 'Europe/Paris' as " + query,
                        "ddl\n" +
                                "CREATE MATERIALIZED VIEW 'test2' WITH BASE 'table1' REFRESH EVERY 7d DEFERRED START '2020-01-01T02:23:59.900000Z' TIME ZONE 'Europe/Paris' AS (\n" +
                                "select ts, v+v doubleV, avg(v) from table1 sample by 30s\n" +
                                ") PARTITION BY DAY;\n"
                ),
        };
        testShowCreateMaterializedView(tests, "timestamp_ns");
    }

    @Test
    public void testUnknownDefinitionBlock() throws Exception {
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
                    block.commit(MatViewDefinition.MAT_VIEW_DEFINITION_FORMAT_MSG_TYPE + 42);
                    writer.commit();
                }

                // Reader should fail to load unknown definition file.
                try (BlockFileReader reader = new BlockFileReader(configuration)) {
                    path.of(configuration.getDbRoot());
                    final int rootLen = path.size();
                    MatViewDefinition actualDefinition = new MatViewDefinition();
                    MatViewDefinition.readFrom(
                            engine,
                            actualDefinition,
                            reader,
                            path,
                            rootLen,
                            matViewToken
                    );
                    Assert.fail("exception expected");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "cannot read materialized view definition, block not found");
                }
            }
        });
    }

    @Test
    public void testUnknownStateBlock() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                createTable(TABLE1);

                final String query = "select ts, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s";
                execute("create materialized view test as (" + query + ") partition by day");

                final TableToken matViewToken = engine.getTableTokenIfExists("test");
                final MatViewDefinition matViewDefinition = engine.getMatViewGraph().getViewDefinition(matViewToken);
                assertNotNull(matViewDefinition);
                final MatViewState matViewState = engine.getMatViewStateStore().getViewState(matViewToken);
                assertNotNull(matViewState);

                try (BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode())) {
                    writer.of(path.of(configuration.getDbRoot()).concat(matViewToken).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
                    // Add unknown block.
                    AppendableBlock block = writer.append();
                    block.putStr("foobar");
                    block.commit(MatViewState.MAT_VIEW_STATE_FORMAT_MSG_TYPE - 1);
                    writer.commit();
                }

                // Reader should fail to load unknown state file.
                try (BlockFileReader reader = new BlockFileReader(configuration)) {
                    reader.of(path.of(configuration.getDbRoot()).concat(matViewToken).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
                    new MatViewStateReader().of(reader, matViewToken);
                    Assert.fail("exception expected");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "cannot read materialized view state, block not found");
                }
            }
        });
    }

    private static void assertMatViewDefinition(
            int refreshType,
            String name,
            String query,
            String baseTableName,
            long samplingInterval,
            char samplingIntervalUnit,
            String timeZone,
            String timeZoneOffset,
            int refreshLimitHoursOrMonths,
            int timerInterval,
            char timerUnit,
            long timerStart,
            String timerTimeZone,
            int periodLength,
            char periodLengthUnit,
            int periodDelay,
            char periodDelayUnit
    ) {
        final MatViewDefinition matViewDefinition = getMatViewDefinition(name);
        assertNotNull(matViewDefinition);
        assertEquals(refreshType, matViewDefinition.getRefreshType());
        assertTrue(matViewDefinition.getMatViewToken().isMatView());
        assertTrue(matViewDefinition.getMatViewToken().isWal());
        assertEquals(query, matViewDefinition.getMatViewSql());
        assertEquals(baseTableName, matViewDefinition.getBaseTableName());
        assertEquals(samplingInterval, matViewDefinition.getSamplingInterval());
        assertEquals(samplingIntervalUnit, matViewDefinition.getSamplingIntervalUnit());
        assertEquals(timeZone, timeZone != null ? matViewDefinition.getTimeZone() : null);
        assertEquals(timeZoneOffset != null ? timeZoneOffset : "00:00", matViewDefinition.getTimeZoneOffset());
        assertEquals(refreshLimitHoursOrMonths, matViewDefinition.getRefreshLimitHoursOrMonths());
        assertEquals(timerInterval, matViewDefinition.getTimerInterval());
        assertEquals(timerUnit, matViewDefinition.getTimerUnit());
        assertEquals(timerStart, matViewDefinition.getTimerStartUs());
        assertEquals(timerTimeZone, timerTimeZone != null ? matViewDefinition.getTimerTimeZone() : null);
        assertEquals(periodLength, matViewDefinition.getPeriodLength());
        assertEquals(periodLengthUnit, matViewDefinition.getPeriodLengthUnit());
        assertEquals(periodDelay, matViewDefinition.getPeriodDelay());
        assertEquals(periodDelayUnit, matViewDefinition.getPeriodDelayUnit());
    }

    private static void assertMatViewDefinition(int refreshType, String name, String query, String baseTableName, int samplingInterval, char samplingIntervalUnit, String timeZone, String timeZoneOffset) {
        assertMatViewDefinition(refreshType, name, query, baseTableName, samplingInterval, samplingIntervalUnit, timeZone, timeZoneOffset, 0, 0, (char) 0, Numbers.LONG_NULL, null, 0, (char) 0, 0, (char) 0);
    }

    private static void assertMatViewDefinitionFile(int refreshType, String name, String query, String baseTableName, int samplingInterval, char samplingIntervalUnit, String timeZone, String timeZoneOffset) {
        assertMatViewDefinitionFile(refreshType, name, query, baseTableName, samplingInterval, samplingIntervalUnit, timeZone, timeZoneOffset, 0, 0, (char) 0, Numbers.LONG_NULL, null, 0, (char) 0, 0, (char) 0);
    }

    private static void assertMatViewDefinitionFile(
            int refreshType,
            String name,
            String query,
            String baseTableName,
            long samplingInterval,
            char samplingIntervalUnit,
            String timeZone,
            String timeZoneOffset,
            int refreshLimitHoursOrMonths,
            int timerInterval,
            char timerUnit,
            long timerStart,
            String timerTimeZone,
            int periodLength,
            char periodLengthUnit,
            int periodDelay,
            char periodDelayUnit
    ) {
        final TableToken matViewToken = engine.getTableTokenIfExists(name);
        try (
                BlockFileReader reader = new BlockFileReader(configuration);
                Path path = new Path()
        ) {
            path.of(configuration.getDbRoot());
            final int rootLen = path.size();
            MatViewDefinition matViewDefinition = new MatViewDefinition();
            MatViewDefinition.readFrom(
                    engine,
                    matViewDefinition,
                    reader,
                    path,
                    rootLen,
                    matViewToken
            );

            assertEquals(refreshType, matViewDefinition.getRefreshType());
            assertEquals(query, matViewDefinition.getMatViewSql());
            assertEquals(baseTableName, matViewDefinition.getBaseTableName());
            assertEquals(samplingInterval, matViewDefinition.getSamplingInterval());
            assertEquals(samplingIntervalUnit, matViewDefinition.getSamplingIntervalUnit());
            assertEquals(timeZone, Chars.toString(matViewDefinition.getTimeZone()));
            assertEquals(timeZoneOffset != null ? timeZoneOffset : "00:00", matViewDefinition.getTimeZoneOffset());
            assertEquals(refreshLimitHoursOrMonths, matViewDefinition.getRefreshLimitHoursOrMonths());
            assertEquals(timerInterval, matViewDefinition.getTimerInterval());
            assertEquals(timerUnit, matViewDefinition.getTimerUnit());
            assertEquals(timerStart, matViewDefinition.getTimerStartUs());
            assertEquals(timerTimeZone, Chars.toString(matViewDefinition.getTimerTimeZone()));
            assertEquals(periodLength, matViewDefinition.getPeriodLength());
            assertEquals(periodLengthUnit, matViewDefinition.getPeriodLengthUnit());
            assertEquals(periodDelay, matViewDefinition.getPeriodDelay());
            assertEquals(periodDelayUnit, matViewDefinition.getPeriodDelayUnit());
        }
    }

    private static MatViewDefinition getMatViewDefinition(String viewName) {
        final TableToken matViewToken = engine.getTableTokenIfExists(viewName);
        if (matViewToken == null) {
            return null;
        }
        return engine.getMatViewGraph().getViewDefinition(matViewToken);
    }

    private void assertQuery0(String expected, String query, String expectedTimestamp) throws Exception {
        assertQueryFullFatNoLeakCheck(expected, query, expectedTimestamp, true, true, false);
    }

    private void createTable(String tableName, boolean walEnabled, String timestampType) throws SqlException {
        execute(
                "create table if not exists " + tableName +
                        " (ts " + timestampType + ", k symbol capacity 2048, k2 symbol capacity 512, v long)" +
                        " timestamp(ts) partition by day" + (walEnabled ? "" : " bypass") + " wal"
        );
        for (int i = 0; i < 9; i++) {
            execute("insert into " + tableName + " values (" + (i * 10000000) + ", 'k" + i + "', " + "'k2_" + i + "', " + i + ")");
        }
    }

    private void createTable(String tableName) throws SqlException {
        createTable(tableName, true, "timestamp");
    }

    private void drainQueues() {
        drainWalAndMatViewQueues();
        // purge job may create MatViewRefreshList for existing tables by calling engine.getDependentMatViews();
        // this affects refresh logic in some scenarios, so make sure to run it
        drainPurgeJob();
    }

    private void testCreateMatViewBaseTableSelfUnion(String timestampType) throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1, true, timestampType);

            final String query = "with cte1 as (" +
                    "  select ts, k, sum(v) sum_v from " + TABLE1 + " where k = 'k0' sample by 1d" +
                    "), " +
                    "cte2 as (" +
                    "  select ts, k, sum(v) sum_v from " + TABLE1 + " where k = 'k1' sample by 1d" +
                    ") " +
                    "select ts, k, last(sum_v) as sum_v " +
                    "from (" +
                    "  select *" +
                    "  from (" +
                    "    select c1.ts, c1.k, c1.sum_v, c2.sum_v " +
                    "    from cte1 c1 left join cte2 c2 on c1.ts = c2.ts" +
                    "    union" +
                    "    select c2.ts, c2.k, c2.sum_v, c1.sum_v " +
                    "    from cte2 c2 left join cte1 c1 on c2.ts = c1.ts" +
                    "  )" +
                    "  order by ts asc" +
                    ") timestamp(ts) " +
                    "sample by 1d";
            execute("create materialized view test as (" + query + ") partition by month;");
            assertQuery0("ts\tk\tsum_v\n", "test", "ts");
            assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 1, 'd', null, null);
            assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, "test", query, TABLE1, 1, 'd', null, null);
        });
    }

    private void testCreateMatViewNoPartitionBy(boolean useParentheses) throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            testCreateMatViewNoPartitionBy(10, 's', PartitionBy.DAY, useParentheses);
            testCreateMatViewNoPartitionBy(1, 'm', PartitionBy.DAY, useParentheses);
            testCreateMatViewNoPartitionBy(2, 'm', PartitionBy.MONTH, useParentheses);
            testCreateMatViewNoPartitionBy(1, 'h', PartitionBy.MONTH, useParentheses);
            testCreateMatViewNoPartitionBy(2, 'h', PartitionBy.YEAR, useParentheses);
            testCreateMatViewNoPartitionBy(70, 'm', PartitionBy.YEAR, useParentheses);
            testCreateMatViewNoPartitionBy(12, 'M', PartitionBy.YEAR, useParentheses);
            testCreateMatViewNoPartitionBy(2, 'y', PartitionBy.YEAR, useParentheses);
        });
    }

    private void testCreateMatViewNoPartitionBy(int samplingInterval, char samplingIntervalUnit, int expectedPartitionBy, boolean useParentheses) throws SqlException {
        final String sampleBy = String.valueOf(samplingInterval) + samplingIntervalUnit;
        final String matViewName = TABLE1 + '_' + sampleBy;
        final String query = "select ts, avg(v) from " + TABLE1 + " sample by " + sampleBy;
        if (useParentheses) {
            execute("create materialized view " + matViewName + " as (" + query + ")");
        } else {
            execute("create materialized view " + matViewName + " as " + query);
        }

        try (TableMetadata metadata = engine.getTableMetadata(engine.getTableTokenIfExists(matViewName))) {
            assertEquals(expectedPartitionBy, metadata.getPartitionBy());
        }
        assertMatViewDefinition(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, matViewName, query, TABLE1, samplingInterval, samplingIntervalUnit, null, null);
        assertMatViewDefinitionFile(MatViewDefinition.REFRESH_TYPE_IMMEDIATE, matViewName, query, TABLE1, samplingInterval, samplingIntervalUnit, null, null);
    }

    private void testCreateMatViewNonDeterministicFunctionInSelect(String func, String columnName) throws Exception {
        createTable(TABLE1);

        assertExceptionNoLeakCheck(
                "create materialized view test as (select ts, " + func + ", avg(v) from " + TABLE1 + " sample by 30s) partition by month",
                45,
                "non-deterministic function cannot be used in materialized view: " + columnName
        );
        assertNull(getMatViewDefinition("test"));

        assertExceptionNoLeakCheck(
                "create materialized view test as (select ts, f, a from (select ts, " + func + " f, avg(v) a from " + TABLE1 + " sample by 30s)) partition by month",
                67,
                "non-deterministic function cannot be used in materialized view: " + columnName
        );
        assertNull(getMatViewDefinition("test"));
    }

    private void testCreateMatViewWithNonBaseTableKeys(String timestampType) throws Exception {
        final String[] queries = new String[]{
                "create materialized view test1 with base x as (select t1.ts, t2.k1, avg(t1.v) from x as t1 join y as t2 on v sample by 1m) partition by day",
                "create materialized view test2 with base x as (select \"t1\".\"ts\", \"t2\".\"k1\", avg(\"t1\".\"v\") from \"x\" as \"t1\" join \"y\" as \"t2\" on \"v\" sample by 1m) partition by day",
                // test table alias case-insensitivity
                "create materialized view test3 with base x as (select \"t1\".\"ts\", \"t2\".\"k1\", avg(\"t1\".\"v\") from \"x\" as \"T1\" join \"y\" as \"T2\" on \"v\" sample by 1m) partition by day",
                "create materialized view test4 with base x as (select t1.ts, t2.k1, avg(t1.v) from x as t1 join y as t2 on v sample by 1m) partition by day",
                // test table name case-insensitivity
                "create materialized view test5 with base x as (select t1.ts, t2.k1, avg(t1.v) from x as T1 join y as T2 on v sample by 1m) partition by day",
        };

        testCreateMatViewWithNonDedupBaseKeys(queries, timestampType);
    }

    private void testCreateMatViewWithNonDedupBaseKeys(String timestampType) throws Exception {
        final String[] queries = new String[]{
                "create materialized view x_hourly1 as (select ts, k2, avg(v) from x sample by 1h) partition by day;",
                "create materialized view x_hourly2 as (select xx.ts, xx.k2, avg(xx.v) from x as xx sample by 1h) partition by day;",
                "create materialized view x_hourly3 as (select ts, k1, k2, avg(v) from x sample by 1h) partition by day;",
                "create materialized view x_hourly4 as (select ts, concat(k1, k2) k, avg(v) from x sample by 1h) partition by day;",
                "create materialized view x_hourly5 as (select ts, k, avg(v) from (select concat(k1, k2) k, v, ts from x) sample by 1h) partition by day;",
                "create materialized view x_hourly6 as (select ts, k, avg(v) from (select concat(k2, 'foobar') k, v, ts from x) sample by 1h) partition by day;",
                "create materialized view x_hourly7 as (select ts, k, avg(v) from (select concat('foobar', k2) k, v, ts from x) sample by 1h) partition by day;",
                "create materialized view x_hourly8 as (select ts, k, avg(v) from (select ts, k2 as k, v from x) sample by 1h) partition by day;",
                "create materialized view test with base x as (select t1.ts, t1.k2, avg(t1.v) from x as t1 join y as t2 on v sample by 1m) partition by day"
        };

        testCreateMatViewWithNonDedupBaseKeys(queries, timestampType);
    }

    private void testCreateMatViewWithNonDedupBaseKeys(String[] queries, String timestampType) throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x " +
                            " (ts " + timestampType + ", k1 symbol, k2 symbol, v long)" +
                            " timestamp(ts) partition by day wal dedup upsert keys(ts, k1);"
            );
            execute(
                    "create table y " +
                            " (ts " + timestampType + ", k1 symbol, k2 symbol, v long)" +
                            " timestamp(ts) partition by day wal;"
            );

            for (int i = 0, n = queries.length; i < n; i++) {
                execute(queries[i]);
            }
        });
    }

    private void testModelToSink(DdlSerializationTest[] tests) throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (DdlSerializationTest test : tests) {
                    sink.clear();
                    final ExecutionModel model = compiler.testCompileModel(test.ddl, sqlExecutionContext);
                    assertEquals(ExecutionModel.CREATE_MAT_VIEW, model.getModelType());
                    ((Sinkable) model).toSink(sink);
                    TestUtils.assertEquals(
                            "ddl: " + test.ddl,
                            test.expected,
                            sink
                    );
                }
            }
        });
    }

    private void testShowCreateMaterializedView(DdlSerializationTest[] tests, String timestamp) throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1, true, timestamp);
            for (DdlSerializationTest test : tests) {
                execute(test.ddl);
                assertQueryNoLeakCheck(
                        test.expected,
                        "show create materialized view " + test.viewName,
                        null,
                        false
                );
            }
        });
    }

    private static class DdlSerializationTest {
        final String ddl;
        final String expected;
        final @Nullable String viewName;

        public DdlSerializationTest(String ddl, String expected) {
            this.viewName = null;
            this.ddl = ddl;
            this.expected = expected;
        }

        public DdlSerializationTest(@Nullable String viewName, String ddl, String expected) {
            this.viewName = viewName;
            this.ddl = ddl;
            this.expected = expected;
        }
    }
}
