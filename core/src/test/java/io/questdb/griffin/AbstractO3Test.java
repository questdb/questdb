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
import io.questdb.cairo.*;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

public class AbstractO3Test {
    protected static final StringSink sink = new StringSink();
    protected static final StringSink sink2 = new StringSink();
    private final static Log LOG = LogFactory.getLog(O3Test.class);
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    protected static CharSequence root;

    @BeforeClass
    public static void setupStatic() {
        try {
            root = temp.newFolder("dbRoot").getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    @Before
    public void setUp() {
        SharedRandom.RANDOM.set(new Rnd());
        // instantiate these paths so that they are not included in memory leak test
        Path.PATH.get();
        Path.PATH2.get();
        TestUtils.createTestPath(root);
    }

    @After
    public void tearDown() {
        TestUtils.removeTestPath(root);
    }

    protected static void assertIndexConsistency(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String table
    ) throws SqlException {
        TestUtils.assertSqlCursors(compiler, sqlExecutionContext, table + " where sym = 'googl' order by ts", "x where sym = 'googl'", LOG);
    }

    protected static void assertIndexConsistency(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        assertIndexConsistency(
                compiler,
                sqlExecutionContext,
                "y"
        );
    }

    protected static void assertIndexConsistencySink(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        printSqlResult(
                compiler,
                sqlExecutionContext,
                "y where sym = 'googl' order by ts"
        );

        TestUtils.printSql(
                compiler,
                sqlExecutionContext,
                "x where sym = 'googl'",
                sink2
        );
        TestUtils.assertEquals(sink, sink2);
    }

    protected static void printSqlResult(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String sql
    ) throws SqlException {
        TestUtils.printSql(compiler, sqlExecutionContext, sql, sink);
    }

    protected static void assertIndexResultAgainstFile(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String resourceName
    ) throws SqlException, URISyntaxException {
        AbstractO3Test.assertSqlResultAgainstFile(compiler, sqlExecutionContext, "x where sym = 'googl'", resourceName);
    }

    protected static void assertO3DataCursors(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String referenceTableDDL,
            String referenceSQL,
            String o3InsertSQL,
            String assertSQL
    ) throws SqlException {
        // create third table, which will contain both X and 1AM
        compiler.compile(referenceTableDDL, sqlExecutionContext);
        compiler.compile(o3InsertSQL, sqlExecutionContext);
        TestUtils.assertSqlCursors(compiler, sqlExecutionContext, referenceSQL, assertSQL, LOG);
        engine.releaseAllReaders();
        TestUtils.assertSqlCursors(compiler, sqlExecutionContext, referenceSQL, assertSQL, LOG);
    }

    protected static void assertO3DataConsistency(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            final String referenceTableDDL,
            final String o3InsertSQL,
            final String resourceName
    ) throws SqlException, URISyntaxException {
        // create third table, which will contain both X and 1AM
        compiler.compile(referenceTableDDL, sqlExecutionContext);

        // expected outcome - output ignored, but useful for debug
        // y ordered with 'order by ts' is not the same order as OOO insert into x when there are several records
        // with the same ts value
        AbstractO3Test.printSqlResult(compiler, sqlExecutionContext, "y order by ts");
        compiler.compile(o3InsertSQL, sqlExecutionContext);
        AbstractO3Test.assertSqlResultAgainstFile(compiler, sqlExecutionContext, "x", resourceName);

        // check that reader can process out of order partition layout after fresh open
        engine.releaseAllReaders();
        AbstractO3Test.assertSqlResultAgainstFile(compiler, sqlExecutionContext, "x", resourceName);
    }

    protected static void assertSqlResultAgainstFile(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String sql,
            String resourceName
    ) throws URISyntaxException, SqlException {
        AbstractO3Test.printSqlResult(compiler, sqlExecutionContext, sql);
        URL url = O3Test.class.getResource(resourceName);
        Assert.assertNotNull(url);
        TestUtils.assertEquals(new File(url.toURI()), sink);
    }

    protected static void assertMemoryLeak(TestUtils.LeakProneCode code) throws Exception {
        TestUtils.assertMemoryLeak(code);
    }

    static void executeVanilla(TestUtils.LeakProneCode code) throws Exception {
        O3Utils.initBuf();
        try {
            AbstractO3Test.assertMemoryLeak(code);
        } finally {
            O3Utils.freeBuf();
        }
    }

    protected static void executeWithPool(
            int workerCount,
            O3Runnable runnable
    ) throws Exception {
        executeWithPool(
                workerCount,
                runnable,
                FilesFacadeImpl.INSTANCE
        );
    }

    protected static void executeWithPool(
            int workerCount,
            O3Runnable runnable,
            FilesFacade ff
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
                        return ff;
                    }
                };

                execute(pool, runnable, configuration);
            } else {
                // we need to create entire engine
                final CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }

                    @Override
                    public int getO3CallbackQueueCapacity() {
                        return 0;
                    }

                    @Override
                    public int getO3PartitionQueueCapacity() {
                        return 0;
                    }

                    @Override
                    public int getO3OpenColumnQueueCapacity() {
                        return 0;
                    }

                    @Override
                    public int getO3CopyQueueCapacity() {
                        return 0;
                    }

                    @Override
                    public int getO3PurgeDiscoveryQueueCapacity() {
                        return 0;
                    }

                    @Override
                    public int getO3PurgeQueueCapacity() {
                        return 0;
                    }

                    @Override
                    public int getO3PartitionUpdateQueueCapacity() {
                        return 0;
                    }
                };
                execute(null, runnable, configuration);
            }
        });
    }

    protected static void execute(@Nullable WorkerPool pool, O3Runnable runnable, CairoConfiguration configuration) throws Exception {
        try (
                final CairoEngine engine = new CairoEngine(configuration);
                final SqlCompiler compiler = new SqlCompiler(engine);
                final SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
        ) {
            try {
                if (pool != null) {
                    pool.assignCleaner(Path.CLEANER);
                    pool.assign(new O3CallbackJob(engine.getMessageBus()));
                    pool.assign(new O3PartitionJob(engine.getMessageBus()));
                    pool.assign(new O3OpenColumnJob(engine.getMessageBus()));
                    pool.assign(new O3CopyJob(engine.getMessageBus()));
                    pool.assign(new O3PurgeDiscoveryJob(engine.getMessageBus(), pool.getWorkerCount()));
                    pool.assign(new O3PurgeJob(engine.getMessageBus()));

                    O3Utils.initBuf(pool.getWorkerCount() + 1);
                    pool.start(LOG);
                } else {
                    O3Utils.initBuf();
                }

                runnable.run(engine, compiler, sqlExecutionContext);
                Assert.assertEquals(0, engine.getBusyWriterCount());
                Assert.assertEquals(0, engine.getBusyReaderCount());
            } finally {
                if (pool != null) {
                    pool.halt();
                }
                O3Utils.freeBuf();
            }
        }
    }

    protected static void executeVanilla(O3Runnable code) throws Exception {
        executeVanilla(() -> execute(null, code, new DefaultCairoConfiguration(root)));
    }

    static void assertO3DataConsistency(
            final CairoEngine engine,
            final SqlCompiler compiler,
            final SqlExecutionContext sqlExecutionContext,
            final String referenceTableDDL,
            final String o3InsertSQL
    ) throws SqlException {
        // create third table, which will contain both X and 1AM
        compiler.compile(referenceTableDDL, sqlExecutionContext);

        // expected outcome
        printSqlResult(compiler, sqlExecutionContext, "y order by ts");

        compiler.compile(o3InsertSQL, sqlExecutionContext);

        TestUtils.printSql(
                compiler,
                sqlExecutionContext,
                "x",
                sink2
        );

        TestUtils.assertEquals(sink, sink2);

        engine.releaseAllReaders();

        TestUtils.printSql(
                compiler,
                sqlExecutionContext,
                "x",
                sink2
        );

        TestUtils.assertEquals(sink, sink2);
    }
}
