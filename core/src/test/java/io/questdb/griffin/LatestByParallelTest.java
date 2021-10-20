/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import io.questdb.WorkerPoolAwareConfiguration;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.engine.table.LatestByAllIndexedJob;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.IOException;

public class LatestByParallelTest {
    protected static final StringSink sink = new StringSink();
    private final static Log LOG = LogFactory.getLog(LatestByParallelTest.class);
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    protected static CharSequence root;
    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static void setupStatic() {
        try {
            root = temp.newFolder("dbRoot").getAbsolutePath();
        } catch (IOException e) {
            throw new ExceptionInInitializerError();
        }
    }

    @Before
    public void setUp() {
        SharedRandom.RANDOM.set(new Rnd());
        TestUtils.createTestPath(root);
    }

    @After
    public void tearDown() {
        TestUtils.removeTestPath(root);
    }

    @Test
    public void testLatestByAllParallel1() throws Exception {
        executeWithPool(4, 8, LatestByParallelTest::testLatestByAll);
    }

    @Test
    public void testLatestByAllParallel2() throws Exception {
        executeWithPool(8, 4, LatestByParallelTest::testLatestByAll);
    }

    @Test
    public void testLatestByAllParallel3() throws Exception {
        executeWithPool(4, 0, LatestByParallelTest::testLatestByAll);
    }

    @Test
    public void testLatestByAllVanilla() throws Exception {
        executeVanilla(LatestByParallelTest::testLatestByAll);
    }

    @Test
    public void testLatestByFilteredParallel1() throws Exception {
        executeWithPool(4, 8, LatestByParallelTest::testLatestByFiltered);
    }

    @Test
    public void testLatestByFilteredParallel2() throws Exception {
        executeWithPool(8, 4, LatestByParallelTest::testLatestByFiltered);
    }

    @Test
    public void testLatestByFilteredParallel3() throws Exception {
        executeWithPool(4, 0, LatestByParallelTest::testLatestByFiltered);
    }

    @Test
    public void testLatestByFilteredVanilla() throws Exception {
        executeVanilla(LatestByParallelTest::testLatestByFiltered);
    }

    @Test
    public void testLatestByTimestampParallel1() throws Exception {
        executeWithPool(4, 8, LatestByParallelTest::testLatestByTimestamp);
    }

    @Test
    public void testLatestByTimestampParallel2() throws Exception {
        executeWithPool(8, 4, LatestByParallelTest::testLatestByTimestamp);
    }

    @Test
    public void testLatestByTimestampParallel3() throws Exception {
        executeWithPool(4, 0, LatestByParallelTest::testLatestByTimestamp);
    }

    @Test
    public void testLatestByTimestampVanilla() throws Exception {
        executeVanilla(LatestByParallelTest::testLatestByTimestamp);
    }

    private static void testLatestByFiltered(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {

        final String expected = "a\tk\tb\n" +
                "78.83065830055033\t1970-01-04T11:20:00.000000Z\tVTJW\n" +
                "51.85631921367574\t1970-01-19T12:26:40.000000Z\tCPSW\n" +
                "50.25890936351257\t1970-01-20T16:13:20.000000Z\tRXGZ\n" +
                "72.604681060764\t1970-01-22T23:46:40.000000Z\t\n";

        final String ddl = "create table x as " +
                "(" +
                "select" +
                " timestamp_sequence(0, 100000000000) k," +
                " rnd_double(0)*100 a1," +
                " rnd_double(0)*100 a2," +
                " rnd_double(0)*100 a3," +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b" +
                " from long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY";

        final String query = "select a,k,b from x latest by b where a > 40";

        assertQuery(compiler, sqlExecutionContext, expected, ddl, query);
    }

    private static void testLatestByAll(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {

        final String expected = "a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n";

        final String ddl = "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY";

        final String query = "select * from x latest by b";

        assertQuery(compiler, sqlExecutionContext, expected, ddl, query);
    }

    private static void testLatestByTimestamp(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {

        final String expected = "a\tb\tk\n" +
                "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                "42.17768841969397\tVTJW\t1970-01-02T03:46:40.000000Z\n";

        final String ddl = "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY";

        final String query = "select * from x latest by b where k < '1970-01-03'";

        assertQuery(compiler, sqlExecutionContext, expected, ddl, query);
    }

    private static void assertQuery(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String expected,
            String ddl,
            String query
    ) throws SqlException {
        compiler.compile(ddl, sqlExecutionContext);

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

    protected static void executeWithPool(
            int workerCount,
            int queueCapacity,
            LatestByRunnable runnable
    ) throws Exception {
        executeVanilla(() -> {
            if (workerCount > 0) {

                int[] affinity = new int[workerCount];
                for (int i = 0; i < workerCount; i++) {
                    affinity[i] = -1;
                }

                WorkerPool pool = new WorkerPool(
                        new WorkerPoolAwareConfiguration() {
                            @Override
                            public int[] getWorkerAffinity() {
                                return affinity;
                            }

                            @Override
                            public int getWorkerCount() {
                                return workerCount;
                            }

                            @Override
                            public boolean haltOnError() {
                                return false;
                            }

                            @Override
                            public boolean isEnabled() {
                                return true;
                            }
                        }
                );

                final CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return FilesFacadeImpl.INSTANCE;
                    }
                };

                execute(pool, runnable, configuration);
            } else {
                // we need to create entire engine
                final CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return FilesFacadeImpl.INSTANCE;
                    }

                    @Override
                    public int getLatestByQueueCapacity() {
                        return queueCapacity;
                    }
                };
                execute(null, runnable, configuration);
            }
        });
    }

    protected static void execute(
            @Nullable WorkerPool pool,
            LatestByRunnable runnable,
            CairoConfiguration configuration
    ) throws Exception {
        final int workerCount = pool == null ? 1 : pool.getWorkerCount();
        try (
                final CairoEngine engine = new CairoEngine(configuration);
                final SqlCompiler compiler = new SqlCompiler(engine)
        ) {
            try (final SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, workerCount)
            ) {
                try {
                    if (pool != null) {
                        pool.assignCleaner(Path.CLEANER);
                        pool.assign(new LatestByAllIndexedJob(engine.getMessageBus()));
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

    protected static void executeVanilla(LatestByRunnable code) throws Exception {
        executeVanilla(() -> execute(null, code, new DefaultCairoConfiguration(root)));
    }

    static void executeVanilla(TestUtils.LeakProneCode code) throws Exception {
        TestUtils.assertMemoryLeak(code);
    }

    @FunctionalInterface
    interface LatestByRunnable {
        void run(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws Exception;
    }

}

