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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.engine.table.EarliestByAllIndexedJob;
import io.questdb.mp.WorkerPool;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.test.AbstractCairoTest.replaceTimestampSuffix;
import static io.questdb.test.AbstractCairoTest.replaceTimestampSuffix1;

@RunWith(Parameterized.class)
public class ParallelEarliestByTest extends AbstractTest {
    private final boolean isConvertToParquet;
    private final StringSink sink = new StringSink();
    private final TestTimestampType timestampType;

    public ParallelEarliestByTest(TestTimestampType timestampType, boolean isConvertToParquet) {
        this.isConvertToParquet = isConvertToParquet;
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}, parquet={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO, false},
                {TestTimestampType.MICRO, true},
                {TestTimestampType.NANO, false},
                {TestTimestampType.NANO, true},
        });
    }

    @Before
    public void setUp() {
        SharedRandom.RANDOM.set(new Rnd());
        super.setUp();
    }

    @Test
    public void testEarliestByAllIndexedReentrantAfterException() throws Exception {
        // Reuses EarliestByAllIndexedRecordCursor across two cursor acquisitions on the same
        // factory. The first cursor trips on an injected openRO failure for the partition 2
        // index files; the of() call for the second cursor must fully reset shared state so
        // the retry produces the correct complete result. Runs on the parallel path (non-zero
        // queue capacity) to exercise the try/catch/finally pipeline in buildTreeMap.
        final AtomicBoolean failNext = new AtomicBoolean(true);
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (failNext.get()
                        && Utf8s.containsAscii(name, "1970-01-02")
                        && (Utf8s.endsWithAscii(name, ".k") || Utf8s.endsWithAscii(name, ".v"))) {
                    return -1;
                }
                return TestFilesFacadeImpl.INSTANCE.openRO(name);
            }
        };

        final String expected = replaceTimestampSuffix1("""
                s\tts
                a\t1970-01-01T00:00:00.000000Z
                b\t1970-01-01T01:00:00.000000Z
                c\t1970-01-02T02:00:00.000000Z
                """, timestampType.getTypeName());

        executeWithPoolAndFilesFacade(4, 8, ff, (engine, compiler, sqlExecutionContext) -> {
            CairoEngine.execute(
                    compiler,
                    "CREATE TABLE t (s SYMBOL INDEX, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY",
                    sqlExecutionContext,
                    null
            );
            CairoEngine.execute(
                    compiler,
                    """
                            INSERT INTO t VALUES\s
                            ('a', 1, '1970-01-01T00:00:00'), ('b', 2, '1970-01-01T01:00:00'),\s
                            ('a', 3, '1970-01-02T00:00:00'), ('b', 4, '1970-01-02T01:00:00'), ('c', 5, '1970-01-02T02:00:00'),\s
                            ('a', 6, '1970-01-03T00:00:00'), ('b', 7, '1970-01-03T01:00:00'), ('c', 8, '1970-01-03T02:00:00')""",
                    sqlExecutionContext,
                    null
            );

            final CompiledQuery cc = compiler.compile(
                    "SELECT s, ts FROM t EARLIEST ON ts PARTITION BY s",
                    sqlExecutionContext
            );
            try (RecordCursorFactory factory = cc.getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                        // drain until the injected FS fault trips
                    }
                    Assert.fail("expected CairoException from injected FS failure");
                } catch (CairoException expected2) {
                    // pass - cursor threw mid-scan
                }

                failNext.set(false);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, sink);
                }
            }
        });
    }

    @Test
    public void testEarliestByAllIndexedWorkerException() throws Exception {
        // Injects a filesystem failure on the indexed frame open while the parallel path is
        // active. The query thread must surface the CairoException, the shared circuit breaker
        // must be cancelled (see buildTreeMap's catch block), and the outer assertMemoryLeak
        // must confirm no native memory leaks from the argumentsAddress or the frame cache.
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.containsAscii(name, "1970-01-02")
                        && (Utf8s.endsWithAscii(name, ".k") || Utf8s.endsWithAscii(name, ".v"))) {
                    return -1;
                }
                return TestFilesFacadeImpl.INSTANCE.openRO(name);
            }
        };

        executeWithPoolAndFilesFacade(4, 8, ff, (engine, compiler, sqlExecutionContext) -> {
            CairoEngine.execute(
                    compiler,
                    "CREATE TABLE x AS (" +
                            "SELECT" +
                            " rnd_double(0)*100 a," +
                            " rnd_symbol(5,4,4,1) b," +
                            " timestamp_sequence(0, 100_000_000_000)::" + timestampType.getTypeName() + " k" +
                            " FROM long_sequence(20)" +
                            "), INDEX(b) TIMESTAMP(k) PARTITION BY DAY",
                    sqlExecutionContext,
                    null
            );

            final CompiledQuery cc = compiler.compile(
                    "SELECT * FROM x EARLIEST ON k PARTITION BY b",
                    sqlExecutionContext
            );
            try (RecordCursorFactory factory = cc.getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                        // drain until the injected FS fault trips
                    }
                    Assert.fail("expected CairoException from injected FS failure");
                } catch (CairoException expected) {
                    // pass - exception propagated to the query thread; buildTreeMap's
                    // catch block has already invoked sharedCircuitBreaker.cancel() so
                    // any in-flight workers abort, and processTasks() in the finally has
                    // drained the queue before this cursor.close() runs.
                }
            }
        });
    }

    @Test
    public void testEarliestByAllParallel1() throws Exception {
        executeWithPool(4, 8, this::testEarliestByAll);
    }

    @Test
    public void testEarliestByAllParallel2() throws Exception {
        executeWithPool(8, 4, this::testEarliestByAll);
    }

    @Test
    public void testEarliestByAllParallel3() throws Exception {
        executeWithPool(4, 0, this::testEarliestByAll);
    }

    @Test
    public void testEarliestByAllVanilla() throws Exception {
        executeVanilla(this::testEarliestByAll);
    }

    @Test
    public void testEarliestByFilteredParallel1() throws Exception {
        executeWithPool(4, 8, this::testEarliestByFiltered);
    }

    @Test
    public void testEarliestByFilteredParallel2() throws Exception {
        executeWithPool(8, 4, this::testEarliestByFiltered);
    }

    @Test
    public void testEarliestByFilteredParallel3() throws Exception {
        executeWithPool(4, 0, this::testEarliestByFiltered);
    }

    @Test
    public void testEarliestByFilteredVanilla() throws Exception {
        executeVanilla(this::testEarliestByFiltered);
    }

    @Test
    public void testEarliestByTimestampParallel1() throws Exception {
        executeWithPool(4, 8, this::testEarliestByTimestamp);
    }

    @Test
    public void testEarliestByTimestampParallel2() throws Exception {
        executeWithPool(8, 4, this::testEarliestByTimestamp);
    }

    @Test
    public void testEarliestByTimestampParallel3() throws Exception {
        executeWithPool(4, 0, this::testEarliestByTimestamp);
    }

    @Test
    public void testEarliestByTimestampVanilla() throws Exception {
        executeVanilla(this::testEarliestByTimestamp);
    }

    @Test
    public void testEarliestByWithinParallel1() throws Exception {
        executeWithPool(4, 8, this::testEarliestByWithin);
    }

    @Test
    public void testEarliestByWithinParallel2() throws Exception {
        executeWithPool(8, 8, this::testEarliestByWithin);
    }

    @Test
    public void testEarliestByWithinParallel3() throws Exception {
        executeWithPool(8, 0, this::testEarliestByWithin);
    }

    @Test
    public void testEarliestByWithinVanilla() throws Exception {
        executeVanilla(this::testEarliestByWithin);
    }

    private static void execute(
            @Nullable WorkerPool pool,
            EarliestByRunnable runnable,
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
                        pool.assign(new EarliestByAllIndexedJob(engine.getMessageBus()));
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

    static void executeVanilla(TestUtils.LeakProneCode code) throws Exception {
        TestUtils.assertMemoryLeak(code);
    }

    private static void executeVanilla(EarliestByRunnable code) throws Exception {
        executeVanilla(() -> execute(null, code, new DefaultTestCairoConfiguration(root)));
    }

    private static void executeWithPool(
            int workerCount,
            int queueCapacity,
            EarliestByRunnable runnable
    ) throws Exception {
        executeVanilla(() -> {
            if (workerCount > 0) {
                final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                };

                WorkerPool pool = new WorkerPool(() -> workerCount);
                execute(pool, runnable, configuration);
            } else {
                // Reuse the LATEST BY queue capacity knob because EARLIEST shares its
                // configuration (see MessageBusImpl). A zero queue forces the inline path.
                final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {

                    @Override
                    public int getLatestByQueueCapacity() {
                        return queueCapacity;
                    }

                    @Override
                    public boolean useWithinByOptimisation() {
                        return true;
                    }
                };
                execute(null, runnable, configuration);
            }
        });
    }

    private static void executeWithPoolAndFilesFacade(
            int workerCount,
            int queueCapacity,
            FilesFacade ff,
            EarliestByRunnable runnable
    ) throws Exception {
        executeVanilla(() -> {
            // Non-zero queue capacity keeps the parallel queue enabled so
            // EarliestByAllIndexedRecordCursor publishes tasks to sibling workers.
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return ff;
                }

                @Override
                public int getLatestByQueueCapacity() {
                    return queueCapacity;
                }

                @Override
                public boolean useWithinByOptimisation() {
                    return true;
                }
            };
            final WorkerPool pool = new WorkerPool(() -> workerCount);
            execute(pool, runnable, configuration);
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

    private void testEarliestByAll(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final String expected = replaceTimestampSuffix1("""
                a\tb\tk
                11.427984775756228\t\t1970-01-01T00:00:00.000000Z
                42.17768841969397\tVTJW\t1970-01-02T03:46:40.000000Z
                23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z
                70.94360487171201\tPEHN\t1970-01-04T11:20:00.000000Z
                97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z
                """, timestampType.getTypeName());

        final String ddl = "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100_000_000_000)::" + timestampType.getTypeName() + " k" +
                " from" +
                " long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY";
        final String ddl2 = isConvertToParquet ? "alter table x convert partition to parquet where k >= 0" : null;

        final String query = "select * from x earliest on k partition by b";

        assertQuery(compiler, sqlExecutionContext, expected, ddl, ddl2, query);
    }

    private void testEarliestByFiltered(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final String expected = replaceTimestampSuffix("""
                a\tk\tb
                78.83065830055033\t1970-01-04T11:20:00.000000Z\tVTJW
                51.85631921367574\t1970-01-19T12:26:40.000000Z\tCPSW
                """, timestampType.getTypeName());

        final String ddl = "create table x as " +
                "(" +
                "select" +
                " timestamp_sequence(0, 100_000_000_000)::" + timestampType.getTypeName() + " k," +
                " rnd_double(0)*100 a1," +
                " rnd_double(0)*100 a2," +
                " rnd_double(0)*100 a3," +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b" +
                " from long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY";
        final String ddl2 = isConvertToParquet ? "alter table x convert partition to parquet where k >= 0" : null;

        final String query = "select * from (select a,k,b from x earliest on k partition by b) where a > 40";

        assertQuery(compiler, sqlExecutionContext, expected, ddl, ddl2, query);
    }

    private void testEarliestByTimestamp(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final String expected = replaceTimestampSuffix("""
                a\tb\tk
                92.050039469858\t\t1970-01-20T16:13:20.000000Z
                """, timestampType.getTypeName());

        final String ddl = "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100_000_000_000)::" + timestampType.getTypeName() + " k" +
                " from" +
                " long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY";

        final String query = "select * from x where k > '1970-01-20' earliest on k partition by b";
        final String ddl2 = isConvertToParquet ? "alter table x convert partition to parquet where k >= 0" : null;

        assertQuery(compiler, sqlExecutionContext, expected, ddl, ddl2, query);
    }

    private void testEarliestByWithin(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        // With a 30-bit geohash filter on a 10000-row rnd table, the earliest rows matching
        // each of these two prefixes happen to be the same pair as the latest ones
        // (each prefix matches only a handful of rows clustered around these timestamps).
        final String expected = replaceTimestampSuffix("""
                ts\tsym\tlon\tlat\tgeo
                1970-01-12T13:41:40.000000Z\tb\t74.35404787498278\t87.57691791453159\tgk1gj8
                1970-01-12T13:45:00.000000Z\tc\t96.30622421207782\t30.899377111184336\tmbx5c0
                """, timestampType.getTypeName());

        final String ddl = "create table x as " +
                "(" +
                "select" +
                " timestamp_sequence(0, 100_000_000)::" + timestampType.getTypeName() + " ts," +
                " rnd_symbol('a','b','c') sym," +
                " rnd_double(0)*100 lon," +
                " rnd_double(0)*100 lat," +
                " rnd_geohash(30) geo" +
                " from long_sequence(10000)" +
                "), index(sym) timestamp(ts) partition by DAY";
        final String ddl2 = isConvertToParquet ? "alter table x convert partition to parquet where ts >= 0" : null;

        final String query = "select * from x where geo within(#gk1gj8, #mbx5c0) earliest on ts partition by sym";

        assertQuery(compiler, sqlExecutionContext, expected, ddl, ddl2, query);
    }

    @FunctionalInterface
    interface EarliestByRunnable {
        void run(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws Exception;
    }
}
