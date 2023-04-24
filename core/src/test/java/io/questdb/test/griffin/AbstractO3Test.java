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

package io.questdb.test.griffin;

import io.questdb.Metrics;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.mp.WorkerPool;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.FilesFacade;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

public class AbstractO3Test extends AbstractTest {
    protected static final StringSink sink = new StringSink();
    protected static final StringSink sink2 = new StringSink();
    protected static int dataAppendPageSize = -1;
    protected static int o3MemMaxPages = -1;
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(20 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();
    private RecordToRowCopier copier;


    @Before
    public void clearRecordToRowCopier() {
        copier = null;
    }

    @Before
    public void setUp() {
        SharedRandom.RANDOM.set(new Rnd());
        // instantiate these paths so that they are not included in memory leak test
        Path.PATH.get();
        Path.PATH2.get();
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        dataAppendPageSize = -1;
        o3MemMaxPages = -1;
        super.tearDown();
    }

    protected static void assertIndexConsistency(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String table,
            CairoEngine engine
    ) throws SqlException {
        TestUtils.assertEquals(compiler, sqlExecutionContext, table + " where sym = 'googl' order by ts", "x where sym = 'googl'");
        TestUtils.assertIndexBlockCapacity(engine, "x", "sym");
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

        assertMaxTimestamp(engine, sink2);
    }

    static void assertMaxTimestamp(CairoEngine engine, CharSequence expected) {
        try (
                final TableWriter w = TestUtils.getWriter(engine, "x")
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

    protected static void assertXY(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        TestUtils.assertEquals(
                compiler,
                sqlExecutionContext,
                "select * from x",
                "select * from y"
        );

        TestUtils.assertEquals(
                compiler,
                sqlExecutionContext,
                "select count() from x",
                "select count() from y"
        );
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

    protected static void printSqlResult(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String sql
    ) throws SqlException {
        TestUtils.printSql(compiler, sqlExecutionContext, sql, sink);
    }

    protected void insertUncommitted(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String sql,
            TableWriter writer
    ) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            RecordMetadata metadata = factory.getMetadata();
            int timestampIndex = writer.getMetadata().getTimestampIndex();
            EntityColumnFilter toColumnFilter = new EntityColumnFilter();
            toColumnFilter.of(metadata.getColumnCount());
            if (null == copier) {
                copier = RecordToRowCopierUtils.generateCopier(
                        new BytecodeAssembler(),
                        metadata,
                        writer.getMetadata(),
                        toColumnFilter
                );
            }
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    long timestamp = record.getTimestamp(timestampIndex);
                    TableWriter.Row row = writer.newRow(timestamp);
                    copier.copy(record, row);
                    row.append();
                }
            }
        }
    }
}
