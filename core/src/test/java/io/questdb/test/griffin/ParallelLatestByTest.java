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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.engine.table.LatestByAllIndexedJob;
import io.questdb.griffin.engine.table.LatestByAllIndexedRecordCursorFactory;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.AbstractCairoTest.replaceTimestampSuffix;
import static io.questdb.test.AbstractCairoTest.replaceTimestampSuffix1;

public class ParallelLatestByTest extends AbstractTest {
    private final boolean convertToParquet;
    private final StringSink sink = new StringSink();
    private final TestTimestampType timestampType;

    public ParallelLatestByTest() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        this.convertToParquet = rnd.nextBoolean();
        this.timestampType = TestUtils.getTimestampType(rnd);
    }

    @Before
    public void setUp() {
        SharedRandom.RANDOM.set(new Rnd());
        super.setUp();
    }

    @Test
    public void testLatestByAllParallel1() throws Exception {
        executeWithPool(4, 8, this::testLatestByAll);
    }

    @Test
    public void testLatestByAllParallel2() throws Exception {
        executeWithPool(8, 4, this::testLatestByAll);
    }

    @Test
    public void testLatestByAllParallel3() throws Exception {
        executeWithPool(4, 0, this::testLatestByAll);
    }

    @Test
    public void testLatestByAllVanilla() throws Exception {
        executeVanilla(this::testLatestByAll);
    }

    @Test
    public void testLatestByFilteredParallel1() throws Exception {
        executeWithPool(4, 8, this::testLatestByFiltered);
    }

    @Test
    public void testLatestByFilteredParallel2() throws Exception {
        executeWithPool(8, 4, this::testLatestByFiltered);
    }

    @Test
    public void testLatestByFilteredParallel3() throws Exception {
        executeWithPool(4, 0, this::testLatestByFiltered);
    }

    @Test
    public void testLatestByFilteredVanilla() throws Exception {
        executeVanilla(this::testLatestByFiltered);
    }

    @Test
    public void testLatestByOwnerSurfacesWorkerFailureAndQueueRemainsReusable() throws Exception {
        executeVanilla(() -> {
            final Thread ownerThread = Thread.currentThread();
            final CountDownLatch workerFailureEntered = new CountDownLatch(1);
            final AtomicBoolean failWorkerDecode = new AtomicBoolean(true);
            final AtomicReference<Thread> failureThread = new AtomicReference<>();
            final UnsupportedOperationException injectedFailure =
                    new UnsupportedOperationException("injected latest-by parquet decode failure");
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public int getLatestByQueueCapacity() {
                    return 4;
                }

                @Override
                public ParquetPartitionDecoder newParquetPartitionDecoder() {
                    return new ParquetPartitionDecoder() {
                        @Override
                        public void of(ParquetPartitionDecoder other) {
                            if (failWorkerDecode.get()) {
                                if (Thread.currentThread() == ownerThread) {
                                    try {
                                        Assert.assertTrue(
                                                "worker did not enter the parquet decoder",
                                                workerFailureEntered.await(10, TimeUnit.SECONDS)
                                        );
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        throw new AssertionError(e);
                                    }
                                } else if (failWorkerDecode.compareAndSet(true, false)) {
                                    failureThread.set(Thread.currentThread());
                                    workerFailureEntered.countDown();
                                    throw injectedFailure;
                                }
                            }
                            super.of(other);
                        }
                    };
                }

                @Override
                public boolean useWithinLatestByOptimisation() {
                    return true;
                }
            };

            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = engine.getSqlCompiler();
                 SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine, 2)) {
                final LatestByAllIndexedJob job = new LatestByAllIndexedJob(engine.getMessageBus());
                final Job.WorkerContext workerContext = new Job.WorkerContext() {
                    @Override
                    public int carrierId() {
                        return -1;
                    }

                    @Override
                    public boolean isTerminating() {
                        return false;
                    }
                };
                final AtomicBoolean workerRunning = new AtomicBoolean(true);
                final AtomicReference<Throwable> unexpectedWorkerFailure = new AtomicReference<>();
                final CountDownLatch workerReady = new CountDownLatch(1);
                final Thread worker = new Thread(() -> {
                    workerReady.countDown();
                    while (workerRunning.get()) {
                        try {
                            if (!job.run(workerContext)) {
                                Os.pause();
                            }
                        } catch (Throwable th) {
                            if (th != injectedFailure) {
                                unexpectedWorkerFailure.compareAndSet(null, th);
                            }
                        }
                    }
                }, "latest-by-error-worker");
                InterruptedException interruptedException = null;
                worker.start();
                try {
                    Assert.assertTrue("latest-by worker did not start", workerReady.await(10, TimeUnit.SECONDS));
                    CairoEngine.execute(
                            compiler,
                            "CREATE TABLE x (sym SYMBOL INDEX, geo GEOHASH(6c), v INT, ts TIMESTAMP) "
                                    + "TIMESTAMP(ts) PARTITION BY DAY",
                            sqlExecutionContext,
                            null
                    );
                    CairoEngine.execute(
                            compiler,
                            "INSERT INTO x VALUES "
                                    + "('a', #gk1gj8, 1, '2020-01-01T00:00:00'), "
                                    + "('b', #mbx5c0, 2, '2020-01-01T01:00:00'), "
                                    + "('c', #gk1gj8, 5, '2020-01-01T02:00:00'), "
                                    + "('d', #mbx5c0, 6, '2020-01-01T03:00:00'), "
                                    + "('e', #gk1gj8, 7, '2020-01-01T04:00:00'), "
                                    + "('f', #mbx5c0, 8, '2020-01-01T05:00:00'), "
                                    + "('a', #zzzzzz, 3, '2020-01-02T00:00:00'), "
                                    + "('b', #zzzzzz, 4, '2020-01-02T01:00:00')",
                            sqlExecutionContext,
                            null
                    );
                    CairoEngine.execute(
                            compiler,
                            "ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-02'",
                            sqlExecutionContext,
                            null
                    );

                    final String query = "SELECT sym, v, ts FROM x "
                            + "WHERE geo WITHIN(#gk1gj8, #mbx5c0) "
                            + "LATEST ON ts PARTITION BY sym ORDER BY sym";
                    try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
                        TestUtils.assertFactoryInTree(factory, LatestByAllIndexedRecordCursorFactory.class);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            try {
                                cursor.hasNext();
                                Assert.fail("expected the worker decode failure");
                            } catch (Throwable th) {
                                Assert.assertSame(injectedFailure, th);
                            }
                        }
                    }

                    Assert.assertNotNull("a worker must execute the failing latest-by task", failureThread.get());
                    Assert.assertNotSame(ownerThread, failureThread.get());

                    try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext);
                         RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        TestUtils.assertCursor(
                                """
                                        sym\tv\tts
                                        c\t5\t2020-01-01T02:00:00.000000Z
                                        d\t6\t2020-01-01T03:00:00.000000Z
                                        e\t7\t2020-01-01T04:00:00.000000Z
                                        f\t8\t2020-01-01T05:00:00.000000Z
                                        """,
                                cursor,
                                factory.getMetadata(),
                                true,
                                sink
                        );
                    }
                } finally {
                    workerRunning.set(false);
                    while (worker.isAlive()) {
                        try {
                            worker.join();
                        } catch (InterruptedException e) {
                            if (interruptedException == null) {
                                interruptedException = e;
                            }
                        }
                    }
                    if (interruptedException != null) {
                        Thread.currentThread().interrupt();
                    }
                }
                if (interruptedException != null) {
                    throw interruptedException;
                }
                Assert.assertNull(unexpectedWorkerFailure.get());
            }
        });
    }

    @Test
    public void testLatestByTimestampParallel1() throws Exception {
        executeWithPool(4, 8, this::testLatestByTimestamp);
    }

    @Test
    public void testLatestByTimestampParallel2() throws Exception {
        executeWithPool(8, 4, this::testLatestByTimestamp);
    }

    @Test
    public void testLatestByTimestampParallel3() throws Exception {
        executeWithPool(4, 0, this::testLatestByTimestamp);
    }

    @Test
    public void testLatestByTimestampVanilla() throws Exception {
        executeVanilla(this::testLatestByTimestamp);
    }

    @Test
    public void testLatestByWithinParallel1() throws Exception {
        executeWithPool(4, 8, this::testLatestByWithin);
    }

    @Test
    public void testLatestByWithinParallel2() throws Exception {
        executeWithPool(8, 8, this::testLatestByWithin);
    }

    @Test
    public void testLatestByWithinParallel3() throws Exception {
        executeWithPool(8, 0, this::testLatestByWithin);
    }

    @Test
    public void testLatestByWithinVanilla() throws Exception {
        executeVanilla(this::testLatestByWithin);
    }

    private static void execute(
            @Nullable WorkerPool pool,
            LatestByRunnable runnable,
            CairoConfiguration configuration
    ) throws Exception {
        final int workerCount = pool == null ? 1 : pool.getWorkerCount();
        try (
                final CairoEngine engine = new CairoEngine(configuration);
                final SqlCompiler compiler = engine.getSqlCompiler()
        ) {
            try (final SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine, workerCount)
            ) {
                try {
                    if (pool != null) {
                        TestUtils.setupWorkerPool(pool, engine);
                        pool.start(LOG);
                    }

                    runnable.run(engine, compiler, sqlExecutionContext);
                    Assert.assertEquals(0, engine.getBusyWriterCount());
                    Assert.assertEquals(0, engine.getBusyReaderCount());
                } finally {
                    if (pool != null) {
                        pool.halt();
                    }
                }
            }
        }
    }

    private static void executeVanilla(LatestByRunnable code) throws Exception {
        executeVanilla(() -> execute(null, code, new DefaultTestCairoConfiguration(root)));
    }

    private static void executeWithPool(
            int workerCount,
            int queueCapacity,
            LatestByRunnable runnable
    ) throws Exception {
        executeVanilla(() -> {
            if (workerCount > 0) {
                final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                };

                WorkerPool pool = new TestWorkerPool(workerCount, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
                execute(pool, runnable, configuration);
            } else {
                // we need to create entire engine
                final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {

                    @Override
                    public int getLatestByQueueCapacity() {
                        return queueCapacity;
                    }

                    @Override
                    public boolean useWithinLatestByOptimisation() {
                        return true;
                    }
                };
                execute(null, runnable, configuration);
            }
        });
    }

    private void assertQuery(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String expected,
            String ddl,
            String ddl2,
            String query
    ) throws SqlException {
        CairoEngine.execute(compiler, ddl, sqlExecutionContext, null);
        if (ddl2 != null) {
            CairoEngine.execute(compiler, ddl2, sqlExecutionContext, null);
        }

        CompiledQuery cc = compiler.compile(query, sqlExecutionContext);
        RecordCursorFactory factory = cc.getRecordCursorFactory();

        try {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, sink);
            }
        } finally {
            Misc.free(factory);
        }
    }

    private void testLatestByAll(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final String expected = replaceTimestampSuffix1("""
                a\tb\tk
                23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z
                12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z
                48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z
                49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z
                40.455469747939254\t\t1970-01-22T23:46:40.000000Z
                """, timestampType.getTypeName());

        final String ddl = "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000)::" + timestampType.getTypeName() + " k" +
                " from" +
                " long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY";
        final String ddl2 = convertToParquet ? "alter table x convert partition to parquet where k >= 0" : null;

        final String query = "select * from x latest on k partition by b";

        assertQuery(compiler, sqlExecutionContext, expected, ddl, ddl2, query);
    }

    private void testLatestByFiltered(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final String expected = replaceTimestampSuffix("""
                a\tk\tb
                78.83065830055033\t1970-01-04T11:20:00.000000Z\tVTJW
                51.85631921367574\t1970-01-19T12:26:40.000000Z\tCPSW
                50.25890936351257\t1970-01-20T16:13:20.000000Z\tRXGZ
                72.604681060764\t1970-01-22T23:46:40.000000Z\t
                """, timestampType.getTypeName());

        final String ddl = "create table x as " +
                "(" +
                "select" +
                " timestamp_sequence(0, 100000000000)::" + timestampType.getTypeName() + " k," +
                " rnd_double(0)*100 a1," +
                " rnd_double(0)*100 a2," +
                " rnd_double(0)*100 a3," +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b" +
                " from long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY";
        final String ddl2 = convertToParquet ? "alter table x convert partition to parquet where k >= 0" : null;

        final String query = "select * from (select a,k,b from x latest on k partition by b) where a > 40";

        assertQuery(compiler, sqlExecutionContext, expected, ddl, ddl2, query);
    }

    private void testLatestByTimestamp(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final String expected = replaceTimestampSuffix("""
                a\tb\tk
                11.427984775756228\t\t1970-01-01T00:00:00.000000Z
                42.17768841969397\tVTJW\t1970-01-02T03:46:40.000000Z
                """, timestampType.getTypeName());

        final String ddl = "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000)::" + timestampType.getTypeName() + " k" +
                " from" +
                " long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY";

        final String query = "select * from x where k < '1970-01-03' latest on k partition by b";
        final String ddl2 = convertToParquet ? "alter table x convert partition to parquet where k >= 0" : null;

        assertQuery(compiler, sqlExecutionContext, expected, ddl, ddl2, query);
    }

    private void testLatestByWithin(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final String expected = replaceTimestampSuffix("""
                ts\tsym\tlon\tlat\tgeo
                1970-01-12T13:41:40.000000Z\tb\t74.35404787498278\t87.57691791453159\tgk1gj8
                1970-01-12T13:45:00.000000Z\tc\t96.30622421207782\t30.899377111184336\tmbx5c0
                """, timestampType.getTypeName());

        final String ddl = "create table x as " +
                "(" +
                "select" +
                " timestamp_sequence(0, 100000000)::" + timestampType.getTypeName() + " ts," +
                " rnd_symbol('a','b','c') sym," +
                " rnd_double(0)*100 lon," +
                " rnd_double(0)*100 lat," +
                " rnd_geohash(30) geo" +
                " from long_sequence(10000)" +
                "), index(sym) timestamp(ts) partition by DAY";
        final String ddl2 = convertToParquet ? "alter table x convert partition to parquet where ts >= 0" : null;

        final String query = "select * from x where geo within(#gk1gj8, #mbx5c0) latest on ts partition by sym";

        assertQuery(compiler, sqlExecutionContext, expected, ddl, ddl2, query);
    }

    static void executeVanilla(TestUtils.LeakProneCode code) throws Exception {
        TestUtils.assertMemoryLeak(code);
    }

    @FunctionalInterface
    interface LatestByRunnable {
        void run(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws Exception;
    }
}
