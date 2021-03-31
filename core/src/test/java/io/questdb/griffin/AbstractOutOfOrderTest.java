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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
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
import org.junit.Assert;
import org.junit.Before;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.atomic.AtomicBoolean;

public class AbstractOutOfOrderTest extends AbstractCairoTest {
    protected static final StringSink sink2 = new StringSink();
    private final static Log LOG = LogFactory.getLog(OutOfOrderTest.class);

    @Before
    public void setUp3() {
        configuration = new DefaultCairoConfiguration(root) {
            @Override
            public boolean isOutOfOrderEnabled() {
                return true;
            }
        };

        SharedRandom.RANDOM.set(new Rnd());

        // instantiate these paths so that they are not included in memory leak test
        Path.PATH.get();
        Path.PATH2.get();
    }

    protected static void assertSqlCursors(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String expected, String actual) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(expected, sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor1 = factory.getCursor(sqlExecutionContext)) {
                try (RecordCursorFactory factory2 = compiler.compile(actual, sqlExecutionContext).getRecordCursorFactory()) {
                    try (RecordCursor cursor2 = factory2.getCursor(sqlExecutionContext)) {
                        TestUtils.assertEquals(cursor1, factory.getMetadata(), cursor2, factory2.getMetadata());
                    }
                }
            }
        }
    }

    protected static void assertIndexConsistency(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String table
    ) throws SqlException {
        assertSqlCursors(compiler, sqlExecutionContext, table + " where sym = 'googl' order by ts", "x where sym = 'googl'");
    }

    protected static void assertIndexConsistency(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        AbstractOutOfOrderTest.assertIndexConsistency(
                compiler,
                sqlExecutionContext,
                "y"
        );
    }

    protected static void printSqlResult(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String sql
    ) throws SqlException {
        TestUtils.printSql(compiler, sqlExecutionContext, sql, AbstractCairoTest.sink);
    }

    protected static void assertIndexResultAgainstFile(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String resourceName
    ) throws SqlException, URISyntaxException {
        AbstractOutOfOrderTest.assertSqlResultAgainstFile(compiler, sqlExecutionContext, "x where sym = 'googl'", resourceName);
    }

    protected static void assertOutOfOrderDataConsistency(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String referenceTableDDL,
            String referenceSQL,
            String outOfOrderInsertSQL,
            String assertSQL
    ) throws SqlException {
        // create third table, which will contain both X and 1AM
        compiler.compile(referenceTableDDL, sqlExecutionContext);
        compiler.compile(outOfOrderInsertSQL, sqlExecutionContext);
        assertSqlCursors(compiler, sqlExecutionContext, referenceSQL, assertSQL);

        engine.releaseAllReaders();
        assertSqlCursors(compiler, sqlExecutionContext, referenceSQL, assertSQL);

        // writer is always "x"
        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x")) {
            Assert.assertTrue(w.reconcileAttachedPartitionsWithScoreboard());
        }
    }

    protected static void assertOutOfOrderDataConsistency(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            final String referenceTableDDL,
            final String outOfOrderSQL,
            final String resourceName
    ) throws SqlException, URISyntaxException {
        // create third table, which will contain both X and 1AM
        compiler.compile(referenceTableDDL, sqlExecutionContext);
        // expected outcome - output ignored, but useful for debug
        // TODO: below output of y is not used anywhere
        // TODO: Use above method assertOutOfOrderDataConsistency to compare x against y
        // TODO: but unstable records sorting have to be solved first
        // TODO: e.g. y ordered with order by ts is not the same order as OOO merge when there are several records
        // TODO: with same ts value
        AbstractOutOfOrderTest.printSqlResult(compiler, sqlExecutionContext, "y order by ts");
        compiler.compile(outOfOrderSQL, sqlExecutionContext);
        AbstractOutOfOrderTest.assertSqlResultAgainstFile(compiler, sqlExecutionContext, "x", resourceName);

        // check that reader can process out of order partition layout after fresh open
        engine.releaseAllReaders();
        AbstractOutOfOrderTest.assertSqlResultAgainstFile(compiler, sqlExecutionContext, "x", resourceName);
    }

    protected static void assertSqlResultAgainstFile(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String sql,
            String resourceName
    ) throws URISyntaxException, SqlException {
        AbstractOutOfOrderTest.printSqlResult(compiler, sqlExecutionContext, sql);
        URL url = OutOfOrderTest.class.getResource(resourceName);
        Assert.assertNotNull(url);
        TestUtils.assertEquals(new File(url.toURI()), sink);
    }

    protected static void assertMemoryLeak(TestUtils.LeakProneCode code) throws Exception {
        TestUtils.assertMemoryLeak(code);
    }

    static void executeVanilla(TestUtils.LeakProneCode code) throws Exception {
        OutOfOrderUtils.initBuf();
        try {
            AbstractOutOfOrderTest.assertMemoryLeak(code);
        } finally {
            OutOfOrderUtils.freeBuf();
        }
    }

    protected static void executeWithPool(int workerCount, boolean enableRename, OutOfOrderCode runnable) throws Exception {
        executeWithPool(
                workerCount,
                enableRename,
                runnable,
                FilesFacadeImpl.INSTANCE
        );
    }

    protected static void executeWithPool(
            int workerCount,
            boolean enableRename,
            OutOfOrderCode runnable,
            FilesFacade ff
    ) throws Exception {
        executeVanilla(() -> {
            if (workerCount > 0) {
                int[] affinity = new int[workerCount];
                for (int i = 0; i < workerCount; i++) {
                    affinity[i] = -1;
                }

                AtomicBoolean atomicEnableRename = new AtomicBoolean(enableRename);
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

                    @Override
                    public boolean isOutOfOrderEnabled() {
                        return true;
                    }

                    @Override
                    public boolean isOutOfOrderRenameEnabled() {
                        return atomicEnableRename.get();
                    }
                };

                if (runnable instanceof OutOfOrderCodeWithFlag) {
                    ((OutOfOrderCodeWithFlag) runnable).delegateFlag(atomicEnableRename);
                }

                try {
                    execute0((engine, compiler, sqlExecutionContext) -> {
                        pool.assignCleaner(Path.CLEANER);
                        pool.assign(new OutOfOrderSortJob(engine.getMessageBus()));
                        pool.assign(new OutOfOrderPartitionJob(engine.getMessageBus()));
                        pool.assign(new OutOfOrderOpenColumnJob(engine.getMessageBus()));
                        pool.assign(new OutOfOrderCopyJob(engine.getMessageBus()));

                        OutOfOrderUtils.initBuf(pool.getWorkerCount() + 1);
                        pool.start(LOG);
                        runnable.run(engine, compiler, sqlExecutionContext);
                    }, configuration);
                } finally {
                    pool.halt();
                }
            } else {
                // we need to create entire engine
                final CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }

                    @Override
                    public int getOutOfOrderSortQueueCapacity() {
                        return 0;
                    }

                    @Override
                    public int getOutOfOrderPartitionQueueCapacity() {
                        return 0;
                    }

                    @Override
                    public int getOutOfOrderOpenColumnQueueCapacity() {
                        return 0;
                    }

                    @Override
                    public int getOutOfOrderCopyQueueCapacity() {
                        return 0;
                    }

                    @Override
                    public boolean isOutOfOrderEnabled() {
                        return true;
                    }

                    @Override
                    public boolean isOutOfOrderRenameEnabled() {
                        return enableRename;
                    }
                };

                OutOfOrderUtils.initBuf();
                execute0(runnable, configuration);
            }
        });
    }

    protected static void execute0(OutOfOrderCode runnable, CairoConfiguration configuration) throws Exception {
        try (
                final CairoEngine engine = new CairoEngine(configuration);
                final SqlCompiler compiler = new SqlCompiler(engine);
                final SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
        ) {
            runnable.run(engine, compiler, sqlExecutionContext);
            Assert.assertEquals(0, engine.getBusyWriterCount());
            Assert.assertEquals(0, engine.getBusyReaderCount());
        } finally {
            OutOfOrderUtils.freeBuf();
        }
    }

    protected static void executeVanilla(OutOfOrderCode code) throws Exception {
        executeVanilla(() -> {
            OutOfOrderUtils.initBuf();
            execute0(code, configuration);
        });
    }

    protected static void executeWithPool(int workerCount, OutOfOrderCode runnable) throws Exception {
        AbstractOutOfOrderTest.executeWithPool(workerCount, true, runnable);
    }

    protected static void executeWithPool(int workerCount, OutOfOrderCode runnable, FilesFacade ff) throws Exception {
        AbstractOutOfOrderTest.executeWithPool(workerCount, true, runnable, ff);
    }
}
