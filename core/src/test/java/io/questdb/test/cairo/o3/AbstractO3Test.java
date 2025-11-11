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

package io.questdb.test.cairo.o3;

import io.questdb.Metrics;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.mp.WorkerPool;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.FilesFacade;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.griffin.CustomisableRunnable;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class AbstractO3Test extends AbstractTest {
    protected static final TimestampDriver MICRO_DRIVER = MicrosTimestampDriver.INSTANCE;
    protected static final HashSet<CharSequence> runTests = new HashSet<>();
    protected static final StringSink sink = new StringSink();
    protected static final StringSink sink2 = new StringSink();
    protected static long cairoCommitLatency = 30_000_000;
    protected static int commitMode = CommitMode.NOSYNC;
    protected static int dataAppendPageSize = -1;
    protected static boolean mixedIOEnabled;
    protected static boolean mixedIOEnabledFFDefault;
    protected static int o3ColumnMemorySize = -1;
    protected static int o3MemMaxPages = -1;
    protected static long partitionO3SplitThreshold = -1;
    protected final Rnd rnd;
    protected final TestTimestampType timestampType;
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(20 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();
    private RecordToRowCopier copier;

    public AbstractO3Test() {
        this.rnd = TestUtils.generateRandom(LOG);
        this.timestampType = TestUtils.getTimestampType(rnd);
    }

    @BeforeClass
    public static void setUpClass() {
        ColumnType.makeUtf8DefaultString();
        runTests.clear();
    }

    @Before
    public void setUp() {
        Rnd value = new Rnd();
        SharedRandom.RANDOM.set(value);

        // instantiate these paths so that they are not included in memory leak test
        Path.PATH.get();
        Path.PATH2.get();
        super.setUp();
        mixedIOEnabledFFDefault = TestFilesFacadeImpl.INSTANCE.allowMixedIO(root);
        mixedIOEnabled = mixedIOEnabledFFDefault;
        copier = null;
        Metrics.ENABLED.clear();
    }

    @After
    public void tearDown() throws Exception {
        commitMode = CommitMode.NOSYNC;
        mixedIOEnabled = mixedIOEnabledFFDefault;
        dataAppendPageSize = -1;
        o3ColumnMemorySize = -1;
        o3MemMaxPages = -1;
        partitionO3SplitThreshold = -1;
        cairoCommitLatency = 30_000_000;
        super.tearDown();
    }

    protected static void assertIndexConsistency(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String table,
            CairoEngine engine
    ) throws SqlException {
        try (TableReader reader = engine.getReader("x")) {
            int symIndex = reader.getMetadata().getColumnIndexQuiet("sym");
            if (symIndex == -1 || !reader.getMetadata().isColumnIndexed(symIndex)) {
                return;
            }
        }
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

    static void assertMaxTimestamp(CairoEngine engine, SqlExecutionContext executionContext) throws SqlException {
        engine.print("select max(ts) from y", sink2, executionContext);
        assertMaxTimestamp(engine, sink2);
    }

    static void assertMaxTimestamp(CairoEngine engine, CharSequence expected) {
        try (
                final TableWriter w = TestUtils.getWriter(engine, "x")
        ) {
            TimestampDriver driver = ColumnType.getTimestampDriver(w.getTimestampType());
            sink.clear();
            sink.put("max\n");
            driver.append(sink, w.getMaxTimestamp());
            sink.put('\n');
            TestUtils.assertEquals(expected, sink);
            Assert.assertEquals(0, w.getO3RowCount());
        }
    }

    protected static void assertMemoryLeak(TestUtils.LeakProneCode code) throws Exception {
        TestUtils.assertMemoryLeak(code);
    }

    static void assertO3DataConsistency(
            final CairoEngine engine,
            final SqlCompiler compiler,
            final SqlExecutionContext sqlExecutionContext,
            final String referenceTableDDL,
            final String o3InsertSQL
    ) throws SqlException {
        // create third table, which will contain both X and 1AM
        engine.execute(referenceTableDDL, sqlExecutionContext);

        try (InsertOperation op = compiler.compile(o3InsertSQL, sqlExecutionContext).popInsertOperation()) {
            op.execute(sqlExecutionContext);
        }

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

    static void assertO3DataConsistencyStableSort(
            final CairoEngine engine,
            final SqlCompiler compiler,
            final SqlExecutionContext sqlExecutionContext,
            final @Nullable String referenceTableDDL,
            final @Nullable String o3InsertSQL
    ) throws SqlException {
        // create third table, which will contain both X and 1AM
        if (referenceTableDDL != null) {
            engine.execute(referenceTableDDL, sqlExecutionContext);
        }
        if (o3InsertSQL != null) {
            engine.execute(o3InsertSQL, sqlExecutionContext);
        }

        TestUtils.assertEqualsExactOrder(
                compiler,
                sqlExecutionContext,
                "y order by ts, commit",
                "x"
        );

        engine.releaseAllReaders();

        TestUtils.assertEqualsExactOrder(
                compiler,
                sqlExecutionContext,
                "y order by ts, commit",
                "x"
        );
    }

    static void assertXCount(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        printSqlResult(compiler, sqlExecutionContext, "select count() from x");
        TestUtils.assertEquals(sink2, sink);
    }

    protected static void assertXCountY(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        TestUtils.assertEquals(compiler, sqlExecutionContext, "select count() from x", "select count() from y");
        assertMaxTimestamp(engine, sqlExecutionContext);
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
        executeVanilla(() -> TestUtils.execute(null, code, new DefaultTestCairoConfiguration(root), LOG));
    }

    protected static void printSqlResult(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String sql
    ) throws SqlException {
        TestUtils.printSql(compiler, sqlExecutionContext, sql, sink);
    }

    protected void executeVanilla(CustomisableRunnableWithTimestampType code) throws Exception {
        executeVanilla(() -> TestUtils.execute(
                null,
                (engine, compiler, context) -> code.run(engine, compiler, context, timestampType.getTypeName()),
                new DefaultTestCairoConfiguration(root), LOG
        ));
    }

    protected void executeWithPool(
            int workerCount,
            CustomisableRunnableWithTimestampType runnable
    ) throws Exception {
        executeWithPool(
                workerCount,
                runnable,
                TestFilesFacadeImpl.INSTANCE
        );
    }

    protected void executeWithPool(
            int workerCount,
            CustomisableRunnableWithTimestampType runnable,
            FilesFacade ff
    ) throws Exception {
        executeVanilla(() -> {
            if (workerCount > 0) {
                final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                    @Override
                    public boolean disableColumnPurgeJob() {
                        return false;
                    }

                    @Override
                    public long getCommitLatency() {
                        return cairoCommitLatency;
                    }

                    @Override
                    public int getCommitMode() {
                        return commitMode;
                    }

                    @Override
                    public long getDataAppendPageSize() {
                        return dataAppendPageSize > 0 ? dataAppendPageSize : super.getDataAppendPageSize();
                    }

                    @Override
                    public @NotNull FilesFacade getFilesFacade() {
                        return ff;
                    }

                    @Override
                    public int getO3ColumnMemorySize() {
                        if (o3ColumnMemorySize != -1) {
                            return o3ColumnMemorySize;
                        }
                        return dataAppendPageSize > 0 ? dataAppendPageSize : super.getO3ColumnMemorySize();
                    }

                    @Override
                    public int getO3MemMaxPages() {
                        return o3MemMaxPages > 0 ? o3MemMaxPages : super.getO3MemMaxPages();
                    }

                    @Override
                    public long getPartitionO3SplitMinSize() {
                        return partitionO3SplitThreshold > -1 ? partitionO3SplitThreshold : super.getPartitionO3SplitMinSize();
                    }

                    @Override
                    public CharSequence getSqlCopyInputRoot() {
                        // reuse root as input root for tests
                        return root;
                    }

                    @Override
                    public boolean isWriterMixedIOEnabled() {
                        // Allow enabling mixed I/O only if the ff allows it.
                        return mixedIOEnabledFFDefault && mixedIOEnabled;
                    }
                };
                WorkerPool pool = new WorkerPool(() -> workerCount);
                TestUtils.execute(
                        pool,
                        (engine, compiler, sqlExecutionContext) -> runnable.run(engine, compiler, sqlExecutionContext, timestampType.getTypeName()),
                        configuration,
                        LOG
                );
            } else {
                // we need to create entire engine
                final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                    @Override
                    public boolean disableColumnPurgeJob() {
                        return false;
                    }

                    @Override
                    public long getCommitLatency() {
                        return cairoCommitLatency;
                    }

                    @Override
                    public int getCommitMode() {
                        return commitMode;
                    }

                    @Override
                    public long getDataAppendPageSize() {
                        return dataAppendPageSize > 0 ? dataAppendPageSize : super.getDataAppendPageSize();
                    }

                    @Override
                    public @NotNull FilesFacade getFilesFacade() {
                        return ff;
                    }

                    @Override
                    public int getO3CallbackQueueCapacity() {
                        return 0;
                    }

                    @Override
                    public int getO3ColumnMemorySize() {
                        if (o3ColumnMemorySize != -1) {
                            return o3ColumnMemorySize;
                        }
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

                    @Override
                    public long getPartitionO3SplitMinSize() {
                        return partitionO3SplitThreshold > -1 ? partitionO3SplitThreshold : super.getPartitionO3SplitMinSize();
                    }

                    @Override
                    public CharSequence getSqlCopyInputRoot() {
                        return root;
                    }

                    @Override
                    public boolean isWriterMixedIOEnabled() {
                        // Allow enabling mixed I/O only if the ff allows it.
                        return mixedIOEnabledFFDefault && mixedIOEnabled;
                    }
                };
                TestUtils.execute(
                        null,
                        (engine, compiler, sqlExecutionContext) -> runnable.run(engine, compiler, sqlExecutionContext, timestampType.getTypeName()),
                        configuration,
                        LOG
                );
            }
        });
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
            if (copier == null) {
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
                    copier.copy(sqlExecutionContext, record, row);
                    row.append();
                }
            }
        }
    }

    @FunctionalInterface
    public interface CustomisableRunnableWithTimestampType {
        void run(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampType) throws Exception;
    }
}
