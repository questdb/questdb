/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.Metrics;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultTestCairoConfiguration;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.FilesFacade;
import io.questdb.std.Rnd;
import io.questdb.std.TestFilesFacadeImpl;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

public class AbstractO3Test {
    protected static final StringSink sink = new StringSink();
    protected static final StringSink sink2 = new StringSink();
    private final static Log LOG = LogFactory.getLog(O3Test.class);
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    protected static int dataAppendPageSize = -1;
    protected static int o3MemMaxPages = -1;
    protected static CharSequence root;
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(20 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

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
        dataAppendPageSize = -1;
        o3MemMaxPages = -1;
    }

    protected static void assertIndexConsistency(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String table,
            CairoEngine engine
    ) throws SqlException {
        TestUtils.assertEquals(compiler, sqlExecutionContext, table + " where sym = 'googl' order by ts", "x where sym = 'googl'");
        TestUtils.assertIndexBlockCapacity(sqlExecutionContext, engine, "x", "sym");
    }

    protected static void assertIndexConsistency(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            CairoEngine engine
    ) throws SqlException {
        assertIndexConsistency(
                compiler,
                sqlExecutionContext,
                "y",
                engine
        );
    }

    protected static void assertIndexConsistencySink(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        TestUtils.assertEquals(
                compiler,
                sqlExecutionContext,
                "y where sym = 'googl' order by ts",
                "x where sym = 'googl'"
        );
    }

    protected static void assertIndexResultAgainstFile(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String resourceName
    ) throws SqlException, URISyntaxException {
        AbstractO3Test.assertSqlResultAgainstFile(compiler, sqlExecutionContext, "x where sym = 'googl'", resourceName);
    }

    static void assertMaxTimestamp(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String expectedSql
    ) throws SqlException {
        TestUtils.printSql(
                compiler,
                executionContext,
                expectedSql,
                sink2
        );

        assertMaxTimestamp(engine, executionContext, sink2);
    }

    static void assertMaxTimestamp(
            CairoEngine engine,
            SqlExecutionContext executionContext,
            CharSequence expected
    ) {
        try (
                final TableWriter w = engine.getWriter(
                        executionContext.getCairoSecurityContext(),
                        engine.getTableToken("x"),
                        "test"
                )
        ) {
            sink.clear();
            sink.put("max\n");
            TimestampFormatUtils.appendDateTimeUSec(sink, w.getMaxTimestamp());
            sink.put('\n');
            TestUtils.assertEquals(expected, sink);
            Assert.assertEquals(0, w.getO3RowCount());
        }
    }

    protected static void assertMemoryLeak(TestUtils.LeakProneCode code) throws Exception {
        TestUtils.assertMemoryLeak(code);
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

    static void assertO3DataConsistency(
            final CairoEngine engine,
            final SqlCompiler compiler,
            final SqlExecutionContext sqlExecutionContext,
            final String referenceTableDDL,
            final String o3InsertSQL
    ) throws SqlException {
        // create third table, which will contain both X and 1AM
        compiler.compile(referenceTableDDL, sqlExecutionContext);

        compiler.compile(o3InsertSQL, sqlExecutionContext);

        TestUtils.assertEquals(
                compiler,
                sqlExecutionContext,
                "y order by ts",
                "x"
        );

        engine.releaseAllReaders();

        TestUtils.assertEquals(
                compiler,
                sqlExecutionContext,
                "y order by ts",
                "x"
        );

        engine.releaseAllWriters();

        TestUtils.assertEquals(
                compiler,
                sqlExecutionContext,
                "y order by ts",
                "x"
        );
    }

    protected static void assertO3DataCursors(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            @Nullable String referenceTableDDL,
            String referenceSQL,
            String o3InsertSQL,
            String assertSQL,
            String countReferenceSQL,
            String countAssertSQL
    ) throws SqlException {
        // create third table, which will contain both X and 1AM
        if (referenceTableDDL != null) {
            compiler.compile(referenceTableDDL, sqlExecutionContext);
        }
        compiler.compile(o3InsertSQL, sqlExecutionContext);
        TestUtils.assertEquals(compiler, sqlExecutionContext, referenceSQL, assertSQL);
        engine.releaseAllReaders();
        TestUtils.assertEquals(compiler, sqlExecutionContext, referenceSQL, assertSQL);

        TestUtils.assertEquals(
                compiler,
                sqlExecutionContext,
                "select count() from " + countReferenceSQL,
                "select count() from " + countAssertSQL
        );
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

    static void assertXCount(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        printSqlResult(compiler, sqlExecutionContext, "select count() from x");
        TestUtils.assertEquals(sink2, sink);
    }

    protected static void assertXCountY(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        TestUtils.assertEquals(compiler, sqlExecutionContext, "select count() from x", "select count() from y");
        assertMaxTimestamp(compiler.getEngine(), compiler, sqlExecutionContext, "select max(ts) from y");
    }

    static void executeVanilla(TestUtils.LeakProneCode code) throws Exception {
        AbstractO3Test.assertMemoryLeak(code);
    }

    protected static void executeVanilla(CustomisableRunnable code) throws Exception {
        executeVanilla(() -> TestUtils.execute(null, code, new DefaultTestCairoConfiguration(root), LOG));
    }

    protected static void executeVanillaWithMetrics(CustomisableRunnable code) throws Exception {
        executeVanilla(() -> TestUtils.execute(null, code, new DefaultTestCairoConfiguration(root), Metrics.enabled(), LOG));
    }

    protected static void executeWithPool(
            int workerCount,
            CustomisableRunnable runnable
    ) throws Exception {
        executeWithPool(
                workerCount,
                runnable,
                TestFilesFacadeImpl.INSTANCE
        );
    }

    protected static void executeWithPool(
            int workerCount,
            CustomisableRunnable runnable,
            FilesFacade ff
    ) throws Exception {
        executeVanilla(() -> {
            if (workerCount > 0) {
                WorkerPool pool = new WorkerPool(() -> workerCount);

                final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                    @Override
                    public boolean disableColumnPurgeJob() {
                        return false;
                    }

                    @Override
                    public long getDataAppendPageSize() {
                        return dataAppendPageSize > 0 ? dataAppendPageSize : super.getDataAppendPageSize();
                    }

                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }

                    @Override
                    public int getO3ColumnMemorySize() {
                        return dataAppendPageSize > 0 ? dataAppendPageSize : super.getO3ColumnMemorySize();
                    }

                    @Override
                    public int getO3MemMaxPages() {
                        return o3MemMaxPages > 0 ? o3MemMaxPages : super.getO3MemMaxPages();
                    }
                };

                TestUtils.execute(pool, runnable, configuration, LOG);
            } else {
                // we need to create entire engine
                final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                    @Override
                    public boolean disableColumnPurgeJob() {
                        return false;
                    }

                    @Override
                    public long getDataAppendPageSize() {
                        return dataAppendPageSize > 0 ? dataAppendPageSize : super.getDataAppendPageSize();
                    }

                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }

                    @Override
                    public int getO3CallbackQueueCapacity() {
                        return 0;
                    }

                    @Override
                    public int getO3ColumnMemorySize() {
                        return dataAppendPageSize > 0 ? dataAppendPageSize : super.getO3ColumnMemorySize();
                    }

                    @Override
                    public int getO3CopyQueueCapacity() {
                        return 0;
                    }

                    @Override
                    public int getO3MemMaxPages() {
                        return o3MemMaxPages > 0 ? o3MemMaxPages : super.getO3MemMaxPages();
                    }

                    @Override
                    public int getO3OpenColumnQueueCapacity() {
                        return 0;
                    }

                    @Override
                    public int getO3PartitionQueueCapacity() {
                        return 0;
                    }

                    @Override
                    public int getO3PurgeDiscoveryQueueCapacity() {
                        return 0;
                    }
                };
                TestUtils.execute(null, runnable, configuration, LOG);
            }
        });
    }

    protected static TableWriter getWriter(SqlExecutionContext sqlExecutionContext, String tableName, String test) {
        CairoEngine engine = sqlExecutionContext.getCairoEngine();
        return engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), engine.getTableToken(tableName), test);
    }

    protected static void printSqlResult(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String sql
    ) throws SqlException {
        TestUtils.printSql(compiler, sqlExecutionContext, sql, sink);
    }
}
