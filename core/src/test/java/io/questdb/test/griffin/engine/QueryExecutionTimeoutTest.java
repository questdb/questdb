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

package io.questdb.test.griffin.engine;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.network.Net;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.griffin.CustomisableRunnable;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This test verifies that various factories use circuit breaker and thus can time out or detect broken connection.
 */
@SuppressWarnings("SameParameterValue")
public class QueryExecutionTimeoutTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        SqlExecutionCircuitBreakerConfiguration config = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public int getCircuitBreakerThrottle() {
                return 0;
            }

            @Override
            public long getQueryTimeout() {
                return NetworkSqlExecutionCircuitBreaker.TIMEOUT_FAIL_ON_FIRST_CHECK;
            }
        };
        circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine, config) {
            @Override
            protected boolean testConnection(long fd) {
                return false;
            }

            {
                setTimeout(-1000); // fail on first check
            }
        };
        ((SqlExecutionContextImpl) sqlExecutionContext).with(circuitBreaker);
        super.setUp();
    }

    @Test
    public void testDisconnectAbortsParallelGroupBy() throws Exception {
        // End-to-end pin for the parallel connection probe: a client that sends a byte and
        // closes mid-scan (FIN behind unread data, the shape a peek probe cannot see) must
        // abort a parallel GROUP BY well before any timeout. Either the owner-thread Phase-2
        // check or the per-worker reduce-path check may detect it first; both run the real
        // NetworkFacade probe against a real socket pair.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100_000);
        Misc.free(circuitBreaker);
        circuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                engine,
                new DefaultSqlExecutionCircuitBreakerConfiguration() {
                    @Override
                    public long getCircuitBreakerConnectionCheckThrottle() {
                        return 10;
                    }

                    @Override
                    public long getQueryTimeout() {
                        return 600_000;
                    }
                }
        );
        executeWithPool(4, 16, (engine, compiler, sqlExecutionContext) -> {
            final long acceptFd = Net.socketTcp(true);
            Assert.assertTrue(acceptFd > 0);
            long sockAddr = 0;
            long clientFd = -1;
            long serverFd = -1;
            long buf = 0;
            final AtomicBoolean isClientClosed = new AtomicBoolean();
            try {
                int port = bindToFreePort(acceptFd);
                Net.listen(acceptFd, 16);
                sockAddr = Net.sockaddr("127.0.0.1", port);
                clientFd = Net.socketTcp(true);
                TestUtils.assertConnect(clientFd, sockAddr);
                serverFd = Net.accept(acceptFd);
                Net.configureNonBlocking(serverFd);
                circuitBreaker.of(serverFd);

                buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
                Unsafe.getUnsafe().putByte(buf, (byte) 'X');

                execute(compiler, "create table disconnect_test as (select x from long_sequence(5_000_000))", sqlExecutionContext);

                final long clientFdToClose = clientFd;
                final long bufToSend = buf;
                final AtomicLong closedAtMs = new AtomicLong();
                TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                    @Override
                    public boolean onGet(Record rec, int count) {
                        if (count == 100_000 && isClientClosed.compareAndSet(false, true)) {
                            closedAtMs.set(System.currentTimeMillis());
                            Net.send(clientFdToClose, bufToSend, 1);
                            Net.close(clientFdToClose);
                        }
                        return true;
                    }
                });
                try (
                        RecordCursorFactory factory = compiler.compile(
                                "select sum(x) from disconnect_test where test_latched_counter()",
                                sqlExecutionContext
                        ).getRecordCursorFactory();
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    cursor.hasNext();
                    Assert.fail("query must abort when the client disconnects mid-scan");
                } catch (CairoException e) {
                    Assert.assertTrue("expected an interruption, got: " + e.getFlyweightMessage(), e.isInterruption());
                    Assert.assertTrue("query aborted before the client disconnected", isClientClosed.get());
                    long detectMs = System.currentTimeMillis() - closedAtMs.get();
                    Assert.assertTrue("disconnect detected too slowly: " + detectMs + " ms", detectMs < 5_000);
                } finally {
                    TestLatchedCounterFunctionFactory.reset(null);
                }
            } finally {
                if (buf != 0) {
                    Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
                }
                if (clientFd != -1 && !isClientClosed.get()) {
                    Net.close(clientFd);
                }
                if (serverFd != -1) {
                    Net.close(serverFd);
                }
                if (sockAddr != 0) {
                    Net.freeSockAddr(sockAddr);
                }
                Net.close(acceptFd);
            }
        });
    }

    @Test
    public void testLatestByAllIndexedWithManyWorkersAndMinimalQueue() throws Exception {
        executeWithPool(
                3,
                1,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInLatestByAllIndexed(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testLatestByAllIndexedWithManyWorkersAndRegularQueue() throws Exception {
        executeWithPool(
                3,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInLatestByAllIndexed(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testLatestByAllIndexedWithOneWorkerAndMinimalQueue() throws Exception {
        executeWithPool(
                1,
                1,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInLatestByAllIndexed(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testLatestByAllIndexedWithOneWorkerAndRegularQueue() throws Exception {
        executeWithPool(
                1,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInLatestByAllIndexed(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutAbortsParallelGroupByDuringPhaseTwoWait() throws Exception {
        // Every frame is in flight on a worker parked at its frame's first row, so the spinning
        // owner is the only place query.timeout can fire; the pre-fix Phase-2 loop re-armed the
        // wrapper timer per iteration and would surface the timeout only after the workers
        // released, seconds later.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100_000);
        Misc.free(circuitBreaker);
        circuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                engine,
                new DefaultSqlExecutionCircuitBreakerConfiguration() {
                    @Override
                    public boolean checkConnection() {
                        return false;
                    }

                    @Override
                    public long getQueryTimeout() {
                        return 300;
                    }
                }
        );
        executeWithPool(4, 16, (engine, compiler, sqlExecutionContext) -> {
            execute(compiler, "create table phase2_timeout_test as (select x from long_sequence(400_000))", sqlExecutionContext);

            final Thread ownerThread = Thread.currentThread();
            final ThreadLocal<Boolean> hasParked = ThreadLocal.withInitial(() -> Boolean.FALSE);
            TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    if (Thread.currentThread() != ownerThread && !hasParked.get()) {
                        hasParked.set(Boolean.TRUE);
                        Os.sleep(3_000);
                    }
                    return true;
                }
            });
            try (
                    RecordCursorFactory factory = compiler.compile(
                            "select sum(x) from phase2_timeout_test where test_latched_counter()",
                            sqlExecutionContext
                    ).getRecordCursorFactory()
            ) {
                circuitBreaker.resetTimer();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    cursor.hasNext();
                    Assert.fail("query must time out while the owner waits for in-flight frames");
                } catch (CairoException e) {
                    // Delivery of the abort is gated on the in-flight frames draining, so wall-clock
                    // elapsed time cannot discriminate; the runtime recorded at throw time can.
                    String msg = e.getFlyweightMessage().toString();
                    TestUtils.assertContains(msg, "timeout, query aborted");
                    int runtimeStart = msg.indexOf("runtime=");
                    Assert.assertTrue("no runtime in: " + msg, runtimeStart >= 0);
                    runtimeStart += "runtime=".length();
                    long runtimeMs = Long.parseLong(msg.substring(runtimeStart, msg.indexOf("ms", runtimeStart)));
                    Assert.assertTrue(
                            "timeout must fire from the owner's Phase-2 wait while the workers hold their frames, got: " + msg,
                            runtimeMs < 2_000
                    );
                }
            } finally {
                TestLatchedCounterFunctionFactory.reset(null);
            }
        });
    }

    @Test
    public void testTimeoutInLatestByAll() throws Exception {
        assertMemoryLeak(() -> assertTimeout(
                "create table xx(value long256, ts timestamp) timestamp(ts)",
                "insert into xx values(null, 0)",
                "select * from xx latest on ts partition by value"
        ));
    }

    @Test
    public void testTimeoutInLatestByAllFiltered() throws Exception {
        assertMemoryLeak(() -> assertTimeout(
                "create table x as " +
                        "(select  rnd_double(0)*100 a, " +
                        "rnd_str(2,4,4) b, " +
                        "timestamp_sequence(0, 100000000000) k " +
                        "from long_sequence(20)) " +
                        "timestamp(k) partition by DAY",
                "select * from x latest by b where b = 'HNR'"
        ));
    }

    @Test
    public void testTimeoutInLatestByAllIndexed() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                testTimeoutInLatestByAllIndexed(compiler, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testTimeoutInLatestByValue() throws Exception {
        assertMemoryLeak(() -> assertTimeout(
                "create table x as " +
                        "(select  rnd_double(0)*100 a, " +
                        "rnd_symbol(5,4,4,1) b, " +
                        "timestamp_sequence(0, 100000000000) k " +
                        "from long_sequence(20)) " +
                        "timestamp(k) partition by DAY",
                "select * from x where b = 'RXGZ' latest on k partition by b"
        ));
    }

    @Test
    public void testTimeoutInLatestByValueFiltered() throws Exception {
        assertMemoryLeak(() -> assertTimeout(
                "create table trades(symbol symbol, side symbol, ts timestamp) timestamp(ts)",
                "insert into trades " +
                        "select 'BTC' || x, 'buy' || x, dateadd( 's', x::int, now() ) " +
                        "from long_sequence(10000)",
                "SELECT * FROM trades " +
                        "WHERE symbol in ('BTC1') " +
                        "AND side in 'buy1' " +
                        "LATEST ON ts " +
                        "PARTITION BY symbol;"
        ));
    }

    @Test
    public void testTimeoutInLatestByValueIndexed() throws Exception {
        assertMemoryLeak(() -> assertTimeout(
                "create table x as " +
                        "(select rnd_double(0)*100 a, " +
                        "rnd_symbol(5,4,4,1) b, " +
                        "timestamp_sequence(0, 100000000000) k " +
                        "from long_sequence(200)), " +
                        "index(b) timestamp(k) partition by DAY",
                "select * from x where b = 'PEHN' and a < 22 and test_match() latest on k partition by b"
        ));
    }

    @Test
    public void testTimeoutInLatestByValueList() throws Exception {
        assertMemoryLeak(() -> assertTimeout(
                "create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_symbol('a', 'b', 'c', 'd', 'e', 'f') s, " +
                        "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) Partition by DAY",
                "select ts, x, s from t latest on ts partition by s"
        ));
    }

    @Test
    public void testTimeoutInLatestByValueListWithFindAllDistinctSymbolsAndFilter() throws Exception {
        assertMemoryLeak(() -> assertTimeout(
                "create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_symbol('a', 'b', null) s, " +
                        "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) Partition by DAY",
                "selecT * from t where x%2 = 1 latest on ts partition by s"
        ));
    }

    @Test
    public void testTimeoutInLatestByValueListWithFindAllDistinctSymbolsAndNoFilter() throws Exception {
        assertMemoryLeak(() -> assertTimeout(
                "create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_symbol('a', 'b', 'c', 'd', 'e', 'f') s, " +
                        "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) Partition by DAY",
                "select ts, x, s from t latest on ts partition by s"
        ));
    }

    @Test
    public void testTimeoutInLatestByValueListWithFindSelectedSymbolsAndFilter() throws Exception {
        assertMemoryLeak(() -> assertTimeout(
                "create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_symbol('a', 'b', null) s, " +
                        "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) Partition by DAY",
                "select * from t " +
                        "where s in ('a', 'b') and x%2 = 0 " +
                        "latest on ts partition by s"
        ));
    }

    @Test
    public void testTimeoutInLatestByValueListWithFindSelectedSymbolsAndNoFilter() throws Exception {
        assertMemoryLeak(() -> assertTimeout(
                "create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_symbol('a', 'b', null) s, " +
                        "timestamp_sequence(0, 60*60*1000*1000L) ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) Partition by DAY",
                "select * from t where s in ('a', null) latest on ts partition by s"
        ));
    }

    @Test
    public void testTimeoutInLatestByValues() throws Exception {
        assertMemoryLeak(() -> assertTimeout(
                "create table x as " +
                        "(select rnd_double(0)*100 a, " +
                        "rnd_symbol(5,4,4,1) b, " +
                        "timestamp_sequence(0, 100000000000) k " +
                        "from long_sequence(20)) " +
                        "timestamp(k) partition by DAY",
                "select * from x where b in (select list('RXGZ', 'HYRX', null, 'UCLA') a from long_sequence(10)) latest on k partition by b"
        ));
    }

    @Test
    public void testTimeoutInLatestByValuesFiltered() throws Exception {
        assertMemoryLeak(() -> assertTimeout(
                "create table x as " +
                        "(select rnd_double(0)*100 a, " +
                        "rnd_symbol(5,4,4,1) b, " +
                        "timestamp_sequence(0, 100000000000) k " +
                        "from long_sequence(20)) " +
                        "timestamp(k) partition by DAY",
                "select * from x where b in (select rnd_symbol('RXGZ', 'HYRX', null, 'UCLA') a from long_sequence(10)) and a > 12 and a < 50 and test_match() latest on k partition by b"
        ));
    }

    @Test
    public void testTimeoutInLatestByValuesIndexed() throws Exception {
        assertMemoryLeak(() -> assertTimeout(
                "create table x as " +
                        "(select rnd_double(0)*100 a, " +
                        "rnd_symbol(5,4,4,1) b, " +
                        "timestamp_sequence(0, 10000000000) k " +
                        "from long_sequence(300)), " +
                        "index(b) timestamp(k) partition by DAY",
                "select * from x where b in ('XYZ', 'HYRX') and a > 30 and test_match() latest on k partition by b"
        ));
    }

    @Test
    public void testTimeoutInMultiHashJoin() throws Exception {
        circuitBreaker.setTimeout(1);
        assertMemoryLeak(() -> {
            try {
                assertTimeout(
                        "create table grouptest as " +
                                "(select cast(x%1000000 as int) as i, x as l from long_sequence(100000) );\n",
                        """
                                select * from\s
                                (
                                  select *\s
                                  from grouptest gt1
                                  join grouptest gt2 on i
                                )
                                join grouptest gt3 on i"""
                );
            } finally {
                resetTimeout();
            }
        });
    }

    @Test
    public void testTimeoutInNonVectorizedKeyedGroupBy() throws Exception {
        assertMemoryLeak(() -> assertTimeout(
                "create table grouptest as (select x as i, x as l from long_sequence(10000) );",
                """
                        select i, avg(l), max(l)\s
                        from grouptest\s
                        group by i"""
        ));
    }

    @Test
    public void testTimeoutInNonVectorizedNonKeyedGroupBy() throws Exception {
        assertMemoryLeak(() -> assertTimeout(
                "create table grouptest as (select x as i, x as l from long_sequence(10000) );",
                "select avg(cast(l as int)), max(l) from grouptest"
        ));
    }

    @Test
    public void testTimeoutInOrderedRowNumber() throws Exception {
        assertMemoryLeak(() -> assertTimeout(
                "create table rntest as (select x as key from long_sequence(1000));\n",
                "select row_number() over (partition by key%1000 order by key ), key  \n" +
                        "from rntest"
        ));
    }

    @Test
    public void testTimeoutInParallelKeyedGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                testTimeoutInParallelKeyedGroupBy(compiler, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testTimeoutInParallelKeyedGroupByWithManyWorkersAndMinimalQueue() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        executeWithPool(
                3,
                1,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInParallelKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInParallelKeyedGroupByWithManyWorkersAndRegularQueue() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        executeWithPool(
                3,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInParallelKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInParallelKeyedGroupByWithOneWorkerAndMinimalQueue() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        executeWithPool(
                1,
                1,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInParallelKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInParallelKeyedGroupByWithOneWorkerAndRegularQueue() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        executeWithPool(
                1,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInParallelKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInParallelNonKeyedGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                testTimeoutInParallelNonKeyedGroupBy(compiler, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testTimeoutInParallelNonKeyedGroupByWithManyWorkersAndMinimalQueue() throws Exception {
        executeWithPool(
                3,
                1,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInParallelNonKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInParallelNonKeyedGroupByWithManyWorkersAndRegularQueue() throws Exception {
        executeWithPool(
                3,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInParallelNonKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInParallelNonKeyedGroupByWithOneWorkersAndRegularQueue() throws Exception {
        executeWithPool(
                1,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInParallelNonKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInRowNumber() throws Exception {
        assertMemoryLeak(() -> assertTimeout(
                "create table rntest as (select x as key from long_sequence(1000));\n",
                "select row_number() over (partition by key%1000 ), key from rntest"
        ));
    }

    @Test
    public void testTimeoutInVectorizedKeyedGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                testTimeoutInVectorizedKeyedGroupBy(compiler, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testTimeoutInVectorizedKeyedGroupByWithManyWorkersAndMinimalQueue() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        executeWithPool(
                3,
                1,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInVectorizedKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInVectorizedKeyedGroupByWithManyWorkersAndRegularQueue() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        executeWithPool(
                3,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInVectorizedKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInVectorizedKeyedGroupByWithOneWorkerAndMinimalQueue() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        executeWithPool(
                1,
                1,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInVectorizedKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInVectorizedKeyedGroupByWithOneWorkerAndRegularQueue() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        executeWithPool(
                1,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInVectorizedKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test
    public void testTimeoutInVectorizedNonKeyedGroupBy() throws Exception {
        executeWithPool(
                4,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInVectorizedNonKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test // triggers timeout when processing task in main thread because queue is too small
    public void testTimeoutInVectorizedNonKeyedGroupByWithManyWorkersAndMinimalQueue() throws Exception {
        executeWithPool(
                3,
                1,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInVectorizedNonKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test // triggers timeout at end of task creation in main thread
    public void testTimeoutInVectorizedNonKeyedGroupByWithManyWorkersAndRegularQueue() throws Exception {
        executeWithPool(
                3,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInVectorizedNonKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    @Test // triggers timeout at end of task creation in main thread
    public void testTimeoutInVectorizedNonKeyedGroupByWithOneWorkersAndRegularQueue() throws Exception {
        executeWithPool(
                1,
                16,
                (engine, compiler, sqlExecutionContext) -> testTimeoutInVectorizedNonKeyedGroupBy(compiler, sqlExecutionContext)
        );
    }

    private static int bindToFreePort(long fd) {
        for (int port = 24_000; port < 25_000; port++) {
            if (Net.bindTcp(fd, 0, port)) {
                return port;
            }
        }
        throw new AssertionError("could not bind a free port");
    }

    private void assertTimeout(String ddl, String query) {
        assertTimeout(ddl, null, query);
    }

    private void assertTimeout(String ddl, String query, SqlCompiler compiler, SqlExecutionContext context) {
        assertTimeout(ddl, null, query, compiler, context);
    }

    private void assertTimeout(String ddl, String dml, String query) {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            assertTimeout(ddl, dml, query, compiler, sqlExecutionContext);
        }
    }

    private void assertTimeout(String ddl, String dml, String query, SqlCompiler compiler, SqlExecutionContext context) {
        try {
            if (dml != null || query != null) {
                unsetTimeout();
            }
            execute(compiler, ddl, context);
            if (dml != null) {
                if (query == null) {
                    resetTimeout();
                }

                execute(compiler, dml, context);
            }

            if (query != null) {
                resetTimeout();
                snapshotMemoryUsage();
                CompiledQuery cc = compiler.compile(query, context);
                try (
                        RecordCursorFactory factory = cc.getRecordCursorFactory();
                        RecordCursor cursor = factory.getCursor(context)
                ) {
                    cursor.hasNext();
                }
                assertFactoryMemoryUsage();
            }

            Assert.fail("Cairo timeout exception expected!");
        } catch (SqlException se) {
            resetTimeout();
            TestUtils.assertContains(se.getFlyweightMessage(), "timeout, query aborted");
        } catch (CairoException ce) {
            resetTimeout();
            TestUtils.assertContains(ce.getFlyweightMessage(), "timeout, query aborted");
            Assert.assertTrue("Exception should be interrupted! " + ce, ce.isInterruption());
        }
    }

    private void executeWithPool(
            int workerCount,
            int queueSize,
            CustomisableRunnable runnable
    ) throws Exception {
        assertMemoryLeak(() -> {
            if (workerCount > 0) {
                final CairoConfiguration configuration1 = new DefaultTestCairoConfiguration(root) {
                    @Override
                    public int getGroupByMergeShardQueueCapacity() {
                        return queueSize;
                    }

                    @Override
                    public int getLatestByQueueCapacity() {
                        return queueSize;
                    }

                    @Override
                    public int getPageFrameReduceQueueCapacity() {
                        return queueSize;
                    }

                    @Override
                    public int getSqlPageFrameMaxRows() {
                        return configuration.getSqlPageFrameMaxRows();
                    }

                    @Override
                    public int getVectorAggregateQueueCapacity() {
                        return queueSize;
                    }
                };

                WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
                    @Override
                    public long getSleepTimeout() {
                        return 1;
                    }

                    @Override
                    public int getWorkerCount() {
                        return workerCount - 1;
                    }
                });
                execute(pool, runnable, configuration1);
            } else {
                final CairoConfiguration configuration1 = new DefaultTestCairoConfiguration(root);
                execute(null, runnable, configuration1);
            }
        });
    }

    private void resetTimeout() {
        circuitBreaker.setTimeout(-1000);
    }

    private void testTimeoutInLatestByAllIndexed(SqlCompiler compiler, @SuppressWarnings("unused") SqlExecutionContext context) {
        assertTimeout(
                "create table x as " +
                        "(" +
                        "select" +
                        " timestamp_sequence(0, 100000000000) k," +
                        " rnd_double(0)*100 a1," +
                        " rnd_double(0)*100 a2," +
                        " rnd_double(0)*100 a3," +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b" +
                        " from long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "select * from (select a,k,b from x latest on k partition by b) where a > 40",
                compiler,
                context
        );
    }

    private void testTimeoutInParallelKeyedGroupBy(SqlCompiler compiler, @SuppressWarnings("unused") SqlExecutionContext context) {
        assertTimeout(
                "create table grouptest as (select cast(x%1000000 as int) as i, (x%100) as price, (x%1000) as quantity from long_sequence(10000) );",
                "select i, vwap(price, quantity) from grouptest group by i order by i",
                compiler,
                context
        );
    }

    private void testTimeoutInParallelNonKeyedGroupBy(SqlCompiler compiler, @SuppressWarnings("unused") SqlExecutionContext context) {
        assertTimeout(
                "create table grouptest as (select (x%100) as price, (x%1000) as quantity from long_sequence(10000) );",
                "select vwap(price, quantity) from grouptest",
                compiler,
                context
        );
    }

    private void testTimeoutInVectorizedKeyedGroupBy(SqlCompiler compiler, @SuppressWarnings("unused") SqlExecutionContext context) {
        assertTimeout(
                "create table grouptest as (select cast(x%1000000 as int) as i, x as l from long_sequence(10000) );",
                "select i, avg(l), max(l) from grouptest group by i",
                compiler,
                context
        );
    }

    private void testTimeoutInVectorizedNonKeyedGroupBy(SqlCompiler compiler, @SuppressWarnings("unused") SqlExecutionContext context) {
        assertTimeout(
                "create table grouptest as (select cast(x%1000000 as int) as i, x as l from long_sequence(10000) );",
                "select avg(l), max(l) from grouptest",
                compiler,
                context
        );
    }

    private void unsetTimeout() {
        circuitBreaker.setTimeout(Long.MAX_VALUE);
    }

    protected static void execute(
            @Nullable WorkerPool pool,
            CustomisableRunnable runnable,
            CairoConfiguration configuration
    ) throws Exception {
        final int workerCount = pool == null ? 1 : pool.getWorkerCount() + 1;
        try (
                final CairoEngine engine = new CairoEngine(configuration);
                final SqlExecutionContextImpl sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine, workerCount)
        ) {
            sqlExecutionContext.with(circuitBreaker);
            if (pool != null) {
                WorkerPoolUtils.setupQueryJobs(pool, engine);
                pool.start(LOG);
            }

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                runnable.run(engine, compiler, sqlExecutionContext);
            }
            Assert.assertEquals("busy writer", 0, engine.getBusyWriterCount());
            Assert.assertEquals("busy reader", 0, engine.getBusyReaderCount());
        } finally {
            if (pool != null) {
                pool.halt();
            }
        }
    }
}
