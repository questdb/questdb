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

package io.questdb.test.griffin.engine.table;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerWrapper;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.async.PageFrameReduceJob;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.QueryFutureUpdateListener;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.FilteredRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.jit.JitUtil;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.SynchronizedJob;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.CustomisableRunnable;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ANY;

public class AsyncFilteredRecordCursorFactoryTest extends AbstractCairoTest {
    private static final int QUEUE_CAPACITY = 4;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Having a single shard is important for many tests in this suite. See resetTaskCapacities
        // method for more detail.
        setProperty(CAIRO_PAGE_FRAME_SHARD_COUNT, 1);
        // We intentionally use a small capacity for the reduce queue to exhibit various edge cases.
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, QUEUE_CAPACITY);

        AbstractCairoTest.setUpStatic();
    }

    @Override
    public void setUp() {
        // 0 means max timeout (Long.MAX_VALUE millis)
        node1.setProperty(PropertyKey.QUERY_TIMEOUT, 0);
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        node1.setProperty(
                PropertyKey.CAIRO_SQL_JIT_MODE,
                JitUtil.isJitSupported() ? SqlJitMode.toString(SqlJitMode.JIT_MODE_ENABLED) : SqlJitMode.toString(SqlJitMode.JIT_MODE_FORCE_SCALAR)
        );
        super.setUp();
    }

    @Test
    public void testDeferredSymbolInFilter() throws Exception {
        withPool(
                (engine, compiler, sqlExecutionContext) -> {
                    // JIT compiler doesn't support IN operator for symbols.
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    execute(
                            compiler,
                            "create table x as (select rnd_symbol('A','B') s, timestamp_sequence(20000000, 100000) t from long_sequence(500000)) timestamp(t) partition by hour",
                            sqlExecutionContext
                    );

                    snapshotMemoryUsage();
                    final String sql = "select * from x where s in ('C','D') limit 10";
                    try (final RecordCursorFactory factory = (compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory())) {
                        Assert.assertEquals(AsyncFilteredRecordCursorFactory.class, factory.getBaseFactory().getClass());

                        assertCursor(
                                "s\tt\n",
                                factory,
                                true,
                                false,
                                false,
                                sqlExecutionContext
                        );

                        execute(compiler,
                                "insert into x select rnd_symbol('C','D') s, timestamp_sequence(100000000000, 100000) from long_sequence(100)",
                                sqlExecutionContext);

                        // Verify that all symbol tables (original and views) are refreshed to include the new symbols.
                        assertCursor(
                                """
                                        s\tt
                                        C\t1970-01-02T03:46:40.000000Z
                                        C\t1970-01-02T03:46:40.100000Z
                                        D\t1970-01-02T03:46:40.200000Z
                                        C\t1970-01-02T03:46:40.300000Z
                                        D\t1970-01-02T03:46:40.400000Z
                                        C\t1970-01-02T03:46:40.500000Z
                                        D\t1970-01-02T03:46:40.600000Z
                                        D\t1970-01-02T03:46:40.700000Z
                                        C\t1970-01-02T03:46:40.800000Z
                                        D\t1970-01-02T03:46:40.900000Z
                                        """,
                                factory,
                                true,
                                false,
                                false,
                                sqlExecutionContext
                        );
                    }

                    resetTaskCapacities();
                }, new AtomicBooleanCircuitBreaker(engine)
        );
    }

    @Test
    public void testDeferredSymbolInFilter2() throws Exception {
        withPool((engine, compiler, sqlExecutionContext) -> testDeferredSymbolInFilter0(compiler, sqlExecutionContext));
    }

    @Test
    public void testDeferredSymbolInFilter2TwoPools() throws Exception {
        withDoublePool((engine, compiler, sqlExecutionContext) -> testDeferredSymbolInFilter0(compiler, sqlExecutionContext));
    }

    @Test
    public void testFaultToleranceImplicitCastException() throws Exception {
        withPool0(
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    execute(
                            compiler,
                            "create table x as (" +
                                    " select rnd_double() a, rnd_symbol('a', 'b', 'c') s, timestamp_sequence(20000000, 1000000) t" +
                                    " from long_sequence(4)" +
                                    ") timestamp(t) partition by hour",
                            sqlExecutionContext
                    );
                    final String sql = "select * from x where a > '2022-03-08T18:03:57.609765Z'";
                    try {
                        try (final RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                            try (final RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                //noinspection StatementWithEmptyBody
                                while (cursor.hasNext()) {
                                } // drain cursor until exception
                                Assert.fail();
                            }
                        }
                    } catch (Throwable e) {
                        String stringType = ColumnType.nameOf(ColumnType.STRING);
                        TestUtils.assertContains(e.getMessage(), "inconvertible value: `2022-03-08T18:03:57.609765Z` [" + stringType + " -> DOUBLE]");
                    }
                }, 3, 3
        );
    }

    @Test
    public void testFaultToleranceNegativeLimitImplicitCastException() throws Exception {
        withPool0(
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    execute(
                            compiler,
                            "create table x as (" +
                                    " select rnd_double() a, rnd_symbol('a', 'b', 'c') s, timestamp_sequence(20000000, 1000000) t" +
                                    " from long_sequence(4)" +
                                    ") timestamp(t) partition by hour",
                            sqlExecutionContext
                    );
                    final String sql = "select * from x where a > '2022-03-08T18:03:57.609765Z' limit -1";
                    try {
                        try (final RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                            try (final RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                //noinspection StatementWithEmptyBody
                                while (cursor.hasNext()) {
                                } // drain cursor until exception
                                Assert.fail();
                            }
                        }
                    } catch (Throwable e) {
                        String stringType = ColumnType.nameOf(ColumnType.STRING);
                        TestUtils.assertContains(e.getMessage(), "inconvertible value: `2022-03-08T18:03:57.609765Z` [" + stringType + " -> DOUBLE]");
                    }
                }, 4, 4
        );
    }

    @Test
    public void testFaultToleranceNegativeLimitNpe() throws Exception {
        withPool0(
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    execute(
                            compiler,
                            "create table x as (" +
                                    " select rnd_double() a, rnd_symbol('a', 'b', 'c') s, timestamp_sequence(20000000, 1000000) t" +
                                    " from long_sequence(4)" +
                                    ") timestamp(t) partition by hour", sqlExecutionContext
                    );
                    final String sql = "select * from x where npe() limit -1";
                    try {
                        try (final RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                            try (final RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                //noinspection StatementWithEmptyBody
                                while (cursor.hasNext()) {
                                } // drain cursor until exception
                                Assert.fail();
                            }
                        }
                    } catch (Throwable e) {
                        TestUtils.assertContains(e.getMessage(), "unexpected filter error");
                    }
                }, 4, 4
        );
    }

    @Test
    public void testFaultToleranceNpe() throws Exception {
        withPool0(
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    execute(
                            compiler,
                            "create table x as (" +
                                    " select rnd_double() a, rnd_symbol('a', 'b', 'c') s, timestamp_sequence(20000000, 1000000) t" +
                                    " from long_sequence(4)" +
                                    ") timestamp(t) partition by hour",
                            sqlExecutionContext
                    );
                    final String sql = "select * from x where npe()";
                    try {
                        try (final RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                            try (final RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                //noinspection StatementWithEmptyBody
                                while (cursor.hasNext()) {
                                } // drain cursor until exception
                                Assert.fail();
                            }
                        }
                    } catch (Throwable e) {
                        TestUtils.assertContains(e.getMessage(), "unexpected filter error");
                    }
                }, 4, 4
        );
    }

    @Test
    public void testFaultToleranceSampleByFilterNpe() throws Exception {
        withPool0(
                (engine, compiler, sqlExecutionContext) -> {
                    execute(
                            compiler,
                            "create table x as (" +
                                    "select timestamp_sequence(0, 100000) timestamp," +
                                    " rnd_symbol('ETH_BTC','BTC_ETH') symbol," +
                                    " rnd_float() price," +
                                    " x row_id" +
                                    " from long_sequence(20000)" +
                                    ") timestamp (timestamp) partition by hour", sqlExecutionContext
                    );
                    final String sql = "select timestamp, count() as trades" +
                            " from x" +
                            " where symbol like '%_ETH' and (row_id != 100 or npe())" +
                            " sample by 1h";
                    try {
                        try (final RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                            try (final RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                //noinspection StatementWithEmptyBody
                                while (cursor.hasNext()) {
                                } // drain cursor until exception
                                Assert.fail();
                            }
                        }
                    } catch (Throwable e) {
                        TestUtils.assertContains(e.getMessage(), "unexpected filter error");
                    }

                    assertSql(
                            """
                                    QUERY PLAN
                                    Radix sort light
                                      keys: [timestamp]
                                        Async Group By workers: 1
                                          keys: [timestamp]
                                          values: [count(*)]
                                          filter: (symbol ~ .*?.ETH [state-shared] and row_id!=100)
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                                    """,
                            "explain select timestamp, count() as trades" +
                                    " from x" +
                                    " where symbol like '%_ETH' and (row_id != 100)" +
                                    " sample by 1h"
                    );
                }, 4, 4
        );
    }

    @Test
    public void testFaultToleranceWrongSharedWorkerConfiguration() throws Exception {
        withPool0(
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    execute(
                            compiler,
                            "create table x as (select rnd_double() a, rnd_symbol('a', 'b', 'c') s, timestamp_sequence(20000000, 100000) t from long_sequence(20000)) timestamp(t) partition by hour",
                            sqlExecutionContext
                    );
                    final String sql = "select sum(a) from x where s='a'";
                    try {
                        // !!! test depends on thread scheduling
                        // should return the expected result or fail with a CairoException
                        assertQueryNoLeakCheck(
                                compiler,
                                "sum\n3354.3807411307785\n",
                                sql,
                                null,
                                false,
                                sqlExecutionContext,
                                true
                        );
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "timeout, query aborted");
                    }
                }, 4, 1
        ); // sharedQueryWorkerCount < workerCount
    }

    @Test
    public void testFullQueueNegativeLimit() throws Exception {
        testFullQueue("x where a > 0.42 limit -3");
    }

    @Test
    public void testFullQueueNoLimit() throws Exception {
        testFullQueue("x where a > 0.42");
    }

    @Test
    public void testFullQueuePositiveLimit() throws Exception {
        testFullQueue("x where a > 0.42 limit 3");
    }

    @Test
    public void testJitFullFwdCursorBwdSwitch() throws Exception {
        assertQuery(
                """
                        a\tb\tk
                        67.00476391801053\tBB\t1970-01-19T12:26:40.000000Z
                        37.62501709498378\tBB\t1970-01-22T23:46:40.000000Z
                        """,
                "x where b = 'BB' limit -2",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol('AA','BB','CC') b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                true,
                true
        );
    }

    @Test
    public void testJitIntervalFwdCursorBwdSwitch() throws Exception {
        assertQuery(
                """
                        a\tb\tk
                        37.62501709498378\tBB\t1970-01-22T23:46:40.000000Z
                        """,
                "x where k > '1970-01-21T20:00:00' and b = 'BB' limit -2",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol('AA','BB','CC') b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                true,
                true
        );
    }

    @Test
    public void testLimitBinVariable() throws Exception {
        withPool(
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    execute(
                            compiler,
                            "create table x as (select rnd_double() a, timestamp_sequence(20000000, 100000) t from long_sequence(2000000)) timestamp(t) partition by hour",
                            sqlExecutionContext
                    );
                    final String sql = "x where a > 0.345747032 and a < 0.34575 limit $1";
                    try (RecordCursorFactory f = (compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory())) {
                        Assert.assertEquals(AsyncFilteredRecordCursorFactory.class, f.getBaseFactory().getClass());
                    }

                    sqlExecutionContext.getBindVariableService().setLong(0, 3);
                    assertQueryNoLeakCheck(
                            compiler,
                            """
                                    a\tt
                                    0.34574819315105954\t1970-01-01T15:03:20.500000Z
                                    0.34574734261660356\t1970-01-02T02:14:37.600000Z
                                    0.34574784156471083\t1970-01-02T08:17:06.600000Z
                                    """,
                            sql,
                            "t",
                            true,
                            sqlExecutionContext,
                            false
                    );

                    // greater
                    sqlExecutionContext.getBindVariableService().setLong(0, 5);
                    assertQueryNoLeakCheck(
                            compiler,
                            """
                                    a\tt
                                    0.34574819315105954\t1970-01-01T15:03:20.500000Z
                                    0.34574734261660356\t1970-01-02T02:14:37.600000Z
                                    0.34574784156471083\t1970-01-02T08:17:06.600000Z
                                    0.34574958643398823\t1970-01-02T20:31:57.900000Z
                                    """,
                            sql,
                            "t",
                            true,
                            sqlExecutionContext,
                            false
                    );

                    // lower
                    sqlExecutionContext.getBindVariableService().setLong(0, 2);
                    assertQueryNoLeakCheck(
                            compiler,
                            """
                                    a\tt
                                    0.34574819315105954\t1970-01-01T15:03:20.500000Z
                                    0.34574734261660356\t1970-01-02T02:14:37.600000Z
                                    """,
                            sql,
                            "t",
                            true,
                            sqlExecutionContext,
                            false
                    );

                    // negative
                    sqlExecutionContext.getBindVariableService().setLong(0, -2);
                    assertQueryNoLeakCheck(
                            compiler,
                            """
                                    a\tt
                                    0.34574784156471083\t1970-01-02T08:17:06.600000Z
                                    0.34574958643398823\t1970-01-02T20:31:57.900000Z
                                    """,
                            sql,
                            "t",
                            true,
                            sqlExecutionContext,
                            true // cursor for negative limit accumulates row ids, so it supports size
                    );

                    resetTaskCapacities();
                }, new AtomicBooleanCircuitBreaker(engine)
        );
    }

    @Test
    public void testNegativeLimit() throws Exception {
        withPool(
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    execute(
                            compiler,
                            "create table x as (select rnd_double() a, timestamp_sequence(20000000, 100000) t from long_sequence(2000000)) timestamp(t) partition by hour",
                            sqlExecutionContext
                    );
                    final String sql = "x where a > 0.345747032 and a < 0.34575 limit -5";
                    try (RecordCursorFactory f = (compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory())) {
                        Assert.assertEquals(AsyncFilteredRecordCursorFactory.class, f.getBaseFactory().getClass());
                    }

                    assertQueryNoLeakCheck(
                            compiler,
                            """
                                    a\tt
                                    0.34574819315105954\t1970-01-01T15:03:20.500000Z
                                    0.34574734261660356\t1970-01-02T02:14:37.600000Z
                                    0.34574784156471083\t1970-01-02T08:17:06.600000Z
                                    0.34574958643398823\t1970-01-02T20:31:57.900000Z
                                    """,
                            sql,
                            "t",
                            true,
                            sqlExecutionContext,
                            true
                    );

                    resetTaskCapacities();
                }, new AtomicBooleanCircuitBreaker(engine)
        );
    }

    @Test
    public void testNoLimit() throws Exception {
        testNoLimit(true, SqlJitMode.JIT_MODE_DISABLED, AsyncFilteredRecordCursorFactory.class);
    }

    @Test
    public void testNoLimitDisabledParallelFilterJit() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());
        // JIT should be ignored since it's only supported for parallel filters.
        testNoLimit(false, SqlJitMode.JIT_MODE_ENABLED, FilteredRecordCursorFactory.class);
    }

    @Test
    public void testNoLimitDisabledParallelFilterNonJit() throws Exception {
        testNoLimit(false, SqlJitMode.JIT_MODE_DISABLED, FilteredRecordCursorFactory.class);
    }

    @Test
    public void testNoLimitJit() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());
        testNoLimit(true, SqlJitMode.JIT_MODE_ENABLED, AsyncJitFilteredRecordCursorFactory.class);
    }

    @Test
    public void testPageFrameSequenceJit() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());
        testPageFrameSequence(SqlJitMode.JIT_MODE_ENABLED, AsyncJitFilteredRecordCursorFactory.class);
    }

    @Test
    public void testPageFrameSequenceNonJit() throws Exception {
        testPageFrameSequence(SqlJitMode.JIT_MODE_DISABLED, AsyncFilteredRecordCursorFactory.class);
    }

    @Test
    public void testPositiveLimit() throws Exception {
        final SqlExecutionCircuitBreakerConfiguration configuration = engine.getConfiguration().getCircuitBreakerConfiguration();
        try (SqlExecutionCircuitBreakerWrapper wrapper = new SqlExecutionCircuitBreakerWrapper(engine, configuration)) {
            wrapper.init(new AtomicBooleanCircuitBreaker(engine));
            withPool(
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                        execute(
                                compiler,
                                "create table x as (select rnd_double() a, timestamp_sequence(20000000, 100000) t from long_sequence(2000000)) timestamp(t) partition by hour",
                                sqlExecutionContext
                        );
                        final String sql = "x where a > 0.345747032 and a < 0.34575 limit 5";
                        try (RecordCursorFactory f = (compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory())) {
                            Assert.assertEquals(AsyncFilteredRecordCursorFactory.class, f.getBaseFactory().getClass());
                        }

                        assertQueryNoLeakCheck(
                                compiler,
                                """
                                        a\tt
                                        0.34574819315105954\t1970-01-01T15:03:20.500000Z
                                        0.34574734261660356\t1970-01-02T02:14:37.600000Z
                                        0.34574784156471083\t1970-01-02T08:17:06.600000Z
                                        0.34574958643398823\t1970-01-02T20:31:57.900000Z
                                        """,
                                sql,
                                "t",
                                true,
                                sqlExecutionContext,
                                false
                        );
                    }, wrapper
            );
        }
    }

    @Test
    public void testPositiveLimitGroupBy() throws Exception {
        final SqlExecutionCircuitBreakerConfiguration configuration = engine.getConfiguration().getCircuitBreakerConfiguration();
        try (SqlExecutionCircuitBreakerWrapper wrapper = new SqlExecutionCircuitBreakerWrapper(engine, configuration)) {
            wrapper.init(new NetworkSqlExecutionCircuitBreaker(engine, configuration, MemoryTag.NATIVE_CB2));
            withPool(
                    (engine, compiler, sqlExecutionContext) -> {
                        execute(
                                compiler,
                                "create table x as (select rnd_double() a, timestamp_sequence(20000000, 100000) t from long_sequence(2000000)) timestamp(t) partition by hour",
                                sqlExecutionContext
                        );
                        final String sql = "select sum(a) from (x where a > 0.345747032 and a < 0.34575 limit 5)";

                        assertQueryNoLeakCheck(
                                compiler,
                                """
                                        sum
                                        1.382992963766362
                                        """,
                                sql,
                                null,
                                false,
                                sqlExecutionContext,
                                true
                        );
                    }, wrapper
            );
        }
    }

    @Test
    public void testPreTouchEnabled() throws Exception {
        withPool(
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);

                    execute("create table x as (select rnd_double() a, timestamp_sequence(20000000, 100000) t from long_sequence(100000)) timestamp(t) partition by hour", sqlExecutionContext);
                    final String sql = "select /*+ ENABLE_PRE_TOUCH(x) */ 'foobar' as c1, t as c2, a as c3, sqrt(a) as c4 from x where a > 0.345747032 and a < 0.34585 limit 5";
                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            sql,
                            sink,
                            """
                                    c1\tc2\tc3\tc4
                                    foobar\t1970-01-01T00:29:28.300000Z\t0.3458428093770707\t0.5880840155769163
                                    foobar\t1970-01-01T00:34:42.600000Z\t0.3457731257014821\t0.5880247662313911
                                    foobar\t1970-01-01T00:42:39.700000Z\t0.3457641654104435\t0.5880171472078374
                                    foobar\t1970-01-01T00:52:14.800000Z\t0.345765350101064\t0.5880181545675813
                                    foobar\t1970-01-01T00:58:31.000000Z\t0.34580598176419974\t0.5880527032198728
                                    """
                    );
                }, new NetworkSqlExecutionCircuitBreaker(engine, engine.getConfiguration().getCircuitBreakerConfiguration(), MemoryTag.NATIVE_CB2)
        );
    }

    @Test
    public void testSymbolEqualsBindVariableFilter() throws Exception {
        testSymbolEqualsBindVariableFilter(SqlJitMode.JIT_MODE_DISABLED, AsyncFilteredRecordCursorFactory.class);
    }

    @Test
    public void testSymbolEqualsBindVariableFilterJit() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());
        testSymbolEqualsBindVariableFilter(SqlJitMode.JIT_MODE_ENABLED, AsyncJitFilteredRecordCursorFactory.class);
    }

    @Test
    public void testSymbolRegexBindVariableFilter() throws Exception {
        withPool(
                (engine, compiler, sqlExecutionContext) -> {
                    // JIT compiler doesn't support ~ operator for symbols.
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    execute(
                            compiler,
                            "create table x as (select rnd_symbol('A','B','C') s, timestamp_sequence(20000000, 100000) t from long_sequence(500000)) timestamp(t) partition by hour",
                            sqlExecutionContext
                    );

                    final String sql = "select * from x where s ~ $1 limit 10";
                    try (RecordCursorFactory f = (compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory())) {
                        Assert.assertEquals(AsyncFilteredRecordCursorFactory.class, f.getBaseFactory().getClass());
                    }

                    bindVariableService.clear();
                    bindVariableService.setStr(0, "C");

                    assertQueryNoLeakCheck(
                            compiler,
                            """
                                    s\tt
                                    C\t1970-01-01T00:00:20.300000Z
                                    C\t1970-01-01T00:00:20.400000Z
                                    C\t1970-01-01T00:00:20.500000Z
                                    C\t1970-01-01T00:00:20.600000Z
                                    C\t1970-01-01T00:00:21.100000Z
                                    C\t1970-01-01T00:00:22.300000Z
                                    C\t1970-01-01T00:00:22.600000Z
                                    C\t1970-01-01T00:00:23.000000Z
                                    C\t1970-01-01T00:00:23.200000Z
                                    C\t1970-01-01T00:00:23.300000Z
                                    """,
                            sql,
                            "t",
                            true,
                            sqlExecutionContext,
                            false
                    );

                    resetTaskCapacities();
                }, new NetworkSqlExecutionCircuitBreaker(engine, engine.getConfiguration().getCircuitBreakerConfiguration(), MemoryTag.NATIVE_CB2)
        );
    }

    private static Class<?> getClass(SqlExecutionCircuitBreaker circuitBreaker) {
        if (circuitBreaker instanceof SqlExecutionCircuitBreakerWrapper) {
            return getClass(((SqlExecutionCircuitBreakerWrapper) circuitBreaker).getDelegate());
        }
        return circuitBreaker.getClass();
    }

    private void resetTaskCapacities() {
        // Tests that involve LIMIT clause may lead to only a fraction of the page frames being
        // reduced and/or collected before the factory gets closed. When that happens, row id and
        // column lists' capacities in the reduce queue's tasks don't get reset to initial values,
        // so the memory leak check fails. As a workaround, we clean up the memory manually.
        final long maxPageFrameRows = configuration.getPageFrameReduceRowIdListCapacity();
        final RingQueue<PageFrameReduceTask> tasks = engine.getMessageBus().getPageFrameReduceQueue(0);
        for (int i = 0; i < tasks.getCycle(); i++) {
            PageFrameReduceTask task = tasks.get(i);
            Assert.assertTrue("Row id list capacity exceeds max page frame rows", task.getFilteredRows().getCapacity() <= maxPageFrameRows);
            task.reset();
        }
    }

    private void testDeferredSymbolInFilter0(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        // JIT compiler doesn't support IN operator for symbols.
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        execute(
                compiler,
                "create table x as (select rnd_symbol('A','B') s, timestamp_sequence(20000000, 100000) t from long_sequence(500000)) timestamp(t) partition by hour",
                sqlExecutionContext
        );

        snapshotMemoryUsage();
        final String sql = "select * from x where s in ('C','D') limit 10";
        try (final RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            Assert.assertEquals(AsyncFilteredRecordCursorFactory.class, factory.getBaseFactory().getClass());

            assertCursor(
                    "s\tt\n",
                    factory,
                    true,
                    false,
                    false,
                    sqlExecutionContext
            );

            execute(
                    compiler,
                    "insert into x select rnd_symbol('C','D') s, timestamp_sequence(1000000000, 100000) from long_sequence(100)",
                    sqlExecutionContext
            );
            // Verify that all symbol tables (original and views) are refreshed to include the new symbols.
            assertCursor(
                    """
                            s\tt
                            C\t1970-01-01T00:16:40.000000Z
                            C\t1970-01-01T00:16:40.100000Z
                            D\t1970-01-01T00:16:40.200000Z
                            C\t1970-01-01T00:16:40.300000Z
                            D\t1970-01-01T00:16:40.400000Z
                            C\t1970-01-01T00:16:40.500000Z
                            D\t1970-01-01T00:16:40.600000Z
                            D\t1970-01-01T00:16:40.700000Z
                            C\t1970-01-01T00:16:40.800000Z
                            D\t1970-01-01T00:16:40.900000Z
                            """,
                    factory,
                    true,
                    false,
                    false,
                    sqlExecutionContext
            );
        }
        resetTaskCapacities();
    }

    private void testFullQueue(String query) throws Exception {
        final int pageFrameRows = 100;
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, pageFrameRows);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, pageFrameRows);
        Assert.assertEquals(pageFrameRows, configuration.getSqlPageFrameMaxRows());
        Assert.assertEquals(Numbers.ceilPow2(pageFrameRows), configuration.getPageFrameReduceQueueCapacity());

        withPool((engine, compiler, sqlExecutionContext) -> {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            execute(
                    compiler,
                    "create table x as (" +
                            "  select rnd_double() a," +
                            "  timestamp_sequence(0, 100000) t from long_sequence(" + (10 * pageFrameRows * QUEUE_CAPACITY) + ")" +
                            ") timestamp(t) partition by hour",
                    sqlExecutionContext
            );

            try (
                    RecordCursorFactory f1 = (compiler.compile(query, sqlExecutionContext).getRecordCursorFactory());
                    RecordCursorFactory f2 = (compiler.compile(query, sqlExecutionContext).getRecordCursorFactory())
            ) {
                Assert.assertEquals(AsyncFilteredRecordCursorFactory.class, f1.getBaseFactory().getClass());
                Assert.assertEquals(AsyncFilteredRecordCursorFactory.class, f2.getBaseFactory().getClass());

                try (
                        RecordCursor c1 = f1.getCursor(sqlExecutionContext);
                        RecordCursor c2 = f2.getCursor(sqlExecutionContext)
                ) {
                    StringSink sink1 = new StringSink();
                    StringSink sink2 = new StringSink();

                    Record r1 = c1.getRecord();
                    Record r2 = c2.getRecord();

                    // We expect both cursors to be able to make progress even though only one of them
                    // occupies the reduce queue most of the time. The second one should be using a local task.
                    while (c1.hasNext()) {
                        TestUtils.println(r1, f1.getMetadata(), sink1);
                        if (c2.hasNext()) {
                            TestUtils.println(r2, f2.getMetadata(), sink2);
                        }
                    }

                    Assert.assertFalse(c1.hasNext());
                    Assert.assertFalse(c2.hasNext());

                    TestUtils.assertEquals(sink1, sink2);
                }
            }

            resetTaskCapacities();
        });
    }

    private void testNoLimit(boolean parallelFilterEnabled, int jitMode, Class<?> expectedFactoryClass) throws Exception {
        sqlExecutionContext.setParallelFilterEnabled(parallelFilterEnabled);
        try {
            withPool(
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(jitMode);
                        execute(
                                compiler,
                                "create table x as (select x, rnd_double() a, timestamp_sequence(20000000, 100000) t from long_sequence(2000000)) timestamp(t) partition by hour",
                                sqlExecutionContext
                        );
                        final String sql = "x where a > 0.345747032 and a < 0.34575";
                        try (RecordCursorFactory f = (compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory())) {
                            Assert.assertEquals(expectedFactoryClass, f.getBaseFactory().getClass());
                        }

                        assertQueryNoLeakCheck(
                                compiler,
                                """
                                        x\ta\tt
                                        541806\t0.34574819315105954\t1970-01-01T15:03:20.500000Z
                                        944577\t0.34574734261660356\t1970-01-02T02:14:37.600000Z
                                        1162067\t0.34574784156471083\t1970-01-02T08:17:06.600000Z
                                        1602980\t0.34574958643398823\t1970-01-02T20:31:57.900000Z
                                        """,
                                sql,
                                "t",
                                true,
                                sqlExecutionContext,
                                false
                        );
                    }, new NetworkSqlExecutionCircuitBreaker(engine, engine.getConfiguration().getCircuitBreakerConfiguration(), MemoryTag.NATIVE_CB2)
            );
        } finally {
            sqlExecutionContext.setParallelFilterEnabled(true);
        }
    }

    private void testPageFrameSequence(int jitMode, Class<?> expectedFactoryClass) throws Exception {
        withPool((engine, compiler, sqlExecutionContext) -> {
            sqlExecutionContext.setJitMode(jitMode);

            execute(
                    compiler,
                    "create table x as (select rnd_double() a, timestamp_sequence(20000000, 100000) t from long_sequence(2000000)) timestamp(t) partition by hour",
                    sqlExecutionContext
            );
            try (RecordCursorFactory f = (compiler.compile("x where a > 0.34", sqlExecutionContext).getRecordCursorFactory())) {

                Assert.assertEquals(expectedFactoryClass, f.getBaseFactory().getClass());
                SCSequence subSeq = new SCSequence();
                PageFrameSequence<?> frameSequence = f.execute(sqlExecutionContext, subSeq, ORDER_ANY);

                int frameCount = 0;
                while (frameCount < frameSequence.getFrameCount()) {
                    long cursor = frameSequence.next();
                    if (cursor < 0) {
                        continue;
                    }
                    PageFrameReduceTask task = frameSequence.getTask(cursor);
                    PageFrameSequence<?> taskSequence = task.getFrameSequence();
                    Assert.assertEquals(frameSequence, taskSequence);
                    frameCount++;
                    frameSequence.collect(cursor, false);
                }
                frameSequence.await();
                Misc.freeIfCloseable(frameSequence.getSymbolTableSource());
                frameSequence.clear();
            }
        });
    }

    private void testSymbolEqualsBindVariableFilter(int jitMode, Class<?> expectedFactoryClass) throws Exception {
        withPool((engine, compiler, sqlExecutionContext) -> {
            sqlExecutionContext.setJitMode(jitMode);
            execute(
                    compiler,
                    "create table x as (select rnd_symbol('A','B','C') s, timestamp_sequence(20000000, 100000) t from long_sequence(500000)) timestamp(t) partition by hour",
                    sqlExecutionContext
            );

            final String sql = "select * from x where s = $1 limit 10";
            try (RecordCursorFactory f = (compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory())) {
                Assert.assertEquals(expectedFactoryClass, f.getBaseFactory().getClass());
            }

            bindVariableService.clear();
            bindVariableService.setStr(0, "C");

            assertQueryNoLeakCheck(
                    compiler,
                    """
                            s\tt
                            C\t1970-01-01T00:00:20.300000Z
                            C\t1970-01-01T00:00:20.400000Z
                            C\t1970-01-01T00:00:20.500000Z
                            C\t1970-01-01T00:00:20.600000Z
                            C\t1970-01-01T00:00:21.100000Z
                            C\t1970-01-01T00:00:22.300000Z
                            C\t1970-01-01T00:00:22.600000Z
                            C\t1970-01-01T00:00:23.000000Z
                            C\t1970-01-01T00:00:23.200000Z
                            C\t1970-01-01T00:00:23.300000Z
                            """,
                    sql,
                    "t",
                    true,
                    sqlExecutionContext,
                    false
            );

            resetTaskCapacities();
        });
    }

    private void withDoublePool(CustomisableRunnable runnable) throws Exception {
        final int sharedPoolWorkerCount = 1;
        final int stealingPoolWorkerCount = 4;
        final AtomicInteger errorCounter = new AtomicInteger();
        final Rnd rnd = new Rnd();

        assertMemoryLeak(() -> {
            final WorkerPool sharedPool = new TestWorkerPool("pool0", sharedPoolWorkerCount);
            TestUtils.setupWorkerPool(sharedPool, engine);
            sharedPool.start();

            try (final WorkerPool stealingPool = new TestWorkerPool("pool1", stealingPoolWorkerCount)) {

                SOCountDownLatch doneLatch = new SOCountDownLatch(1);

                stealingPool.assign(new SynchronizedJob() {
                    boolean run = true;

                    @Override
                    protected boolean runSerially() {
                        if (run) {
                            try {
                                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                                    runnable.run(
                                            engine, compiler, new DelegatingSqlExecutionContext() {
                                                @Override
                                                public Rnd getRandom() {
                                                    return rnd;
                                                }

                                                @Override
                                                public int getSharedQueryWorkerCount() {
                                                    return sharedPoolWorkerCount;
                                                }
                                            }
                                    );
                                }
                            } catch (Throwable e) {
                                e.printStackTrace(System.out);
                                errorCounter.incrementAndGet();
                            } finally {
                                doneLatch.countDown();
                                run = false;
                            }
                            return true;
                        }
                        return false;
                    }
                });

                stealingPool.start();

                try {
                    doneLatch.await();
                    Assert.assertEquals(0, errorCounter.get());
                } finally {
                    sharedPool.halt();
                }
            }
        });
    }

    private void withPool(CustomisableRunnable runnable) throws Exception {
        withPool(runnable, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
    }

    private void withPool(CustomisableRunnable runnable, SqlExecutionCircuitBreaker circuitBreaker) throws Exception {
        int workerCount = 4;
        withPool0(runnable, workerCount, workerCount, circuitBreaker);
    }

    private void withPool0(CustomisableRunnable runnable, int workerCount, int sharedQueryWorkerCount) throws Exception {
        withPool0(runnable, workerCount, sharedQueryWorkerCount, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
    }

    private void withPool0(CustomisableRunnable runnable, int workerCount, int sharedQueryWorkerCount, SqlExecutionCircuitBreaker circuitBreaker) throws Exception {
        assertMemoryLeak(() -> {
            final TestWorkerPool pool = new TestWorkerPool(workerCount);
            TestUtils.setupWorkerPool(pool, engine);
            final ObjList<PageFrameReduceJob> pageFrameReduceJobs = pool.getPageFrameReduceJobs();
            pool.start();

            try {
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    runnable.run(
                            engine, compiler, new DelegatingSqlExecutionContext() {
                                @Override
                                public @NotNull SqlExecutionCircuitBreaker getCircuitBreaker() {
                                    return circuitBreaker;
                                }

                                @Override
                                public int getSharedQueryWorkerCount() {
                                    return sharedQueryWorkerCount;
                                }
                            }
                    );
                }

                for (int i = 0, n = pageFrameReduceJobs.size(); i < n; i++) {
                    final PageFrameReduceJob job = pageFrameReduceJobs.getQuick(i);
                    // if the job was never used the circuit breaker is null,
                    // if the job was used the type of the circuit breaker has to match with the one from SqlExecutionContext
                    assert job.getCircuitBreaker() == null || getClass(job.getCircuitBreaker()) == getClass(circuitBreaker);
                }
            } catch (Throwable e) {
                e.printStackTrace(System.out);
                throw e;
            } finally {
                pool.halt();
            }
        });
    }

    private static abstract class DelegatingSqlExecutionContext implements SqlExecutionContext {

        @Override
        public boolean allowNonDeterministicFunctions() {
            return sqlExecutionContext.allowNonDeterministicFunctions();
        }

        @Override
        public void clearWindowContext() {
            sqlExecutionContext.clearWindowContext();
        }

        @Override
        public void configureWindowContext(
                @Nullable VirtualRecord partitionByRecord,
                @Nullable RecordSink partitionBySink,
                @Nullable ColumnTypes keyTypes,
                boolean isOrdered,
                int orderByDirection,
                int orderByPos,
                boolean baseSupportsRandomAccess,
                int framingMode,
                long rowsLo,
                char rowsLoUnit,
                int rowsLoExprPos,
                long rowsHi,
                char rowsHiUnit,
                int rowsHiExprPos,
                int exclusionKind,
                int exclusionKindPos,
                int timestampIndex,
                int timestampType,
                boolean ignoreNulls,
                int nullsDescPos
        ) {
            sqlExecutionContext.configureWindowContext(
                    partitionByRecord,
                    partitionBySink,
                    keyTypes,
                    isOrdered,
                    orderByDirection,
                    orderByPos,
                    baseSupportsRandomAccess,
                    framingMode,
                    rowsLo,
                    rowsLoUnit,
                    rowsLoExprPos,
                    rowsHi,
                    rowsHiUnit,
                    rowsHiExprPos,
                    exclusionKind,
                    exclusionKindPos,
                    timestampIndex,
                    timestampType,
                    ignoreNulls,
                    nullsDescPos
            );
        }

        @Override
        public BindVariableService getBindVariableService() {
            return sqlExecutionContext.getBindVariableService();
        }

        @Override
        public @NotNull CairoEngine getCairoEngine() {
            return sqlExecutionContext.getCairoEngine();
        }

        @Override
        public @NotNull SqlExecutionCircuitBreaker getCircuitBreaker() {
            return sqlExecutionContext.getCircuitBreaker();
        }

        @Override
        public boolean getCloneSymbolTables() {
            return sqlExecutionContext.getCloneSymbolTables();
        }

        @Override
        public Decimal128 getDecimal128() {
            return sqlExecutionContext.getDecimal128();
        }

        @Override
        public Decimal256 getDecimal256() {
            return sqlExecutionContext.getDecimal256();
        }

        @Override
        public Decimal64 getDecimal64() {
            return sqlExecutionContext.getDecimal64();
        }

        @Override
        public int getIntervalFunctionType() {
            return sqlExecutionContext.getIntervalFunctionType();
        }

        @Override
        public int getJitMode() {
            return sqlExecutionContext.getJitMode();
        }

        @Override
        public long getMicrosecondTimestamp() {
            return sqlExecutionContext.getMicrosecondTimestamp();
        }

        @Override
        public long getNanosecondTimestamp() {
            return sqlExecutionContext.getNanosecondTimestamp();
        }

        @Override
        public long getNow(int timestampType) {
            return sqlExecutionContext.getNow(timestampType);
        }

        @Override
        public int getNowTimestampType() {
            return sqlExecutionContext.getNowTimestampType();
        }

        @Override
        public QueryFutureUpdateListener getQueryFutureUpdateListener() {
            return sqlExecutionContext.getQueryFutureUpdateListener();
        }

        @Override
        public Rnd getRandom() {
            return sqlExecutionContext.getRandom();
        }

        @Override
        public long getRequestFd() {
            return sqlExecutionContext.getRequestFd();
        }

        @Override
        public @NotNull SecurityContext getSecurityContext() {
            return sqlExecutionContext.getSecurityContext();
        }

        @Override
        public SqlExecutionCircuitBreaker getSimpleCircuitBreaker() {
            return sqlExecutionContext.getSimpleCircuitBreaker();
        }

        @Override
        public WindowContext getWindowContext() {
            return sqlExecutionContext.getWindowContext();
        }

        @Override
        public void initNow() {
            sqlExecutionContext.initNow();
        }

        @Override
        public boolean isCacheHit() {
            return sqlExecutionContext.isCacheHit();
        }

        @Override
        public boolean isParallelFilterEnabled() {
            return sqlExecutionContext.isParallelFilterEnabled();
        }

        @Override
        public boolean isParallelGroupByEnabled() {
            return sqlExecutionContext.isParallelGroupByEnabled();
        }

        @Override
        public boolean isParallelReadParquetEnabled() {
            return sqlExecutionContext.isParallelReadParquetEnabled();
        }

        @Override
        public boolean isParallelTopKEnabled() {
            return sqlExecutionContext.isParallelTopKEnabled();
        }

        @Override
        public boolean isTimestampRequired() {
            return sqlExecutionContext.isTimestampRequired();
        }

        @Override
        public boolean isWalApplication() {
            return sqlExecutionContext.isWalApplication();
        }

        @Override
        public void popTimestampRequiredFlag() {
            sqlExecutionContext.popTimestampRequiredFlag();
        }

        @Override
        public void pushTimestampRequiredFlag(boolean flag) {
            sqlExecutionContext.pushTimestampRequiredFlag(flag);
        }

        @Override
        public void resetFlags() {
            sqlExecutionContext.resetFlags();
        }

        @Override
        public void setAllowNonDeterministicFunction(boolean value) {
            sqlExecutionContext.setAllowNonDeterministicFunction(value);
        }

        @Override
        public void setCacheHit(boolean value) {
            sqlExecutionContext.setCacheHit(value);
        }

        @Override
        public void setCancelledFlag(AtomicBoolean cancelled) {
            sqlExecutionContext.setCancelledFlag(cancelled);
        }

        @Override
        public void setCloneSymbolTables(boolean cloneSymbolTables) {
            sqlExecutionContext.setCloneSymbolTables(cloneSymbolTables);
        }

        @Override
        public void setIntervalFunctionType(int intervalType) {
            sqlExecutionContext.setIntervalFunctionType(intervalType);
        }

        @Override
        public void setJitMode(int jitMode) {
            sqlExecutionContext.setJitMode(jitMode);
        }

        @Override
        public void setNowAndFixClock(long now, int nowTimestampType) {
            sqlExecutionContext.setNowAndFixClock(now, nowTimestampType);
        }

        @Override
        public void setParallelFilterEnabled(boolean parallelFilterEnabled) {
            sqlExecutionContext.setParallelFilterEnabled(parallelFilterEnabled);
        }

        @Override
        public void setParallelGroupByEnabled(boolean parallelGroupByEnabled) {
            sqlExecutionContext.setParallelGroupByEnabled(parallelGroupByEnabled);
        }

        @Override
        public void setParallelReadParquetEnabled(boolean parallelReadParquetEnabled) {
            sqlExecutionContext.setParallelReadParquetEnabled(parallelReadParquetEnabled);
        }

        @Override
        public void setParallelTopKEnabled(boolean parallelTopKEnabled) {
            sqlExecutionContext.setParallelTopKEnabled(parallelTopKEnabled);
        }

        @Override
        public void setRandom(Rnd rnd) {
            sqlExecutionContext.setRandom(rnd);
        }

        @Override
        public void setUseSimpleCircuitBreaker(boolean value) {
            sqlExecutionContext.setUseSimpleCircuitBreaker(value);
        }

        @Override
        public void storeTelemetry(short event, short origin) {
            sqlExecutionContext.storeTelemetry(event, origin);
        }

        @Override
        public void toSink(@NotNull CharSink<?> sink) {
            sqlExecutionContext.toSink(sink);
        }
    }
}
