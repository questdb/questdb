/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.TableWriter.Row;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler.RecordToRowCopier;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class O3CommitLagTest extends AbstractO3Test {
    private final static Log LOG = LogFactory.getLog(O3CommitLagTest.class);
    private long minTimestamp;
    private long maxTimestamp;
    private RecordToRowCopier copier;

    @Before
    public void clearRecordToRowCopier() {
        copier = null;
    }

    @Test
    public void testBigUncommittedCheckStrColFixedAndVarMappedSizes() throws Exception {
        executeWithPool(0, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
            try (TableModel tableModel = new TableModel(engine.getConfiguration(), "table", PartitionBy.DAY)) {
                tableModel
                        .col("id", ColumnType.STRING)
                        .timestamp("ts");
                testBigUncommittedMove1(engine, compiler, sqlExecutionContext, tableModel);
            }
        });
    }

    @Test
    public void testBigUncommittedMovesTimestampOnEdge() throws Exception {
        executeWithPool(0, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
            try (TableModel tableModel = new TableModel(engine.getConfiguration(), "table", PartitionBy.DAY)) {
                tableModel
                        .col("id", ColumnType.LONG)
                        .timestamp("ts");
                testBigUncommittedMove1(engine, compiler, sqlExecutionContext, tableModel);
            }
        });
    }

    @Test
    public void testBigUncommittedToMove() throws Exception {
        executeWithPool(0, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
            try (TableModel tableModel = new TableModel(engine.getConfiguration(), "table", PartitionBy.DAY)) {
                tableModel
                        .col("id", ColumnType.LONG)
                        .col("ok", ColumnType.FLOAT)
                        .col("str", ColumnType.STRING)
                        .timestamp("ts");
                testBigUncommittedMove1(engine, compiler, sqlExecutionContext, tableModel);
            }
        });
    }

    @Test
    public void testCommitLagEndingAtPartitionBoundaryContended() throws Exception {
        executeWithPool(0, this::testCommitLagEndingAtPartitionBoundary0);
    }

    @Test
    public void testCommitLagEndingAtPartitionBoundaryParallel() throws Exception {
        executeWithPool(2, this::testCommitLagEndingAtPartitionBoundary0);
    }

    @Test
    public void testCommitLagEndingAtPartitionBoundaryPlus1Contended() throws Exception {
        executeWithPool(0, this::testCommitLagEndingAtPartitionBoundaryPlus10);
    }

    @Test
    public void testCommitLagEndingAtPartitionBoundaryPlus1Parallel() throws Exception {
        executeWithPool(2, this::testCommitLagEndingAtPartitionBoundaryPlus10);
    }

    @Test
    public void testCommitLagEndingAtPartitionBoundaryPlus1WithRollbackContended() throws Exception {
        executeWithPool(0, this::testCommitLagEndingAtPartitionBoundaryPlus1WithRollback0);
    }

    @Test
    public void testCommitLagEndingAtPartitionBoundaryPlus1WithRollbackParallel() throws Exception {
        executeWithPool(2, this::testCommitLagEndingAtPartitionBoundaryPlus1WithRollback0);
    }

    @Test
    public void testCommitLagEndingAtPartitionBoundaryWithRollbackContended() throws Exception {
        executeWithPool(0, this::testCommitLagEndingAtPartitionBoundaryWithRollback0);
    }

    @Test
    public void testCommitLagEndingAtPartitionBoundaryWithRollbackParallel() throws Exception {
        executeWithPool(2, this::testCommitLagEndingAtPartitionBoundaryWithRollback0);
    }

    @Test
    public void testCommitLagStaggeringPartitionsContended() throws Exception {
        executeWithPool(0, this::testCommitLagStaggeringPartitions0);
    }

    @Test
    public void testCommitLagStaggeringPartitionsParallel() throws Exception {
        executeWithPool(2, this::testCommitLagStaggeringPartitions0);
    }

    @Test
    public void testCommitLagStaggeringPartitionsWithRollbackContended() throws Exception {
        executeWithPool(0, this::testCommitLagStaggeringPartitionsWithRollback0);
    }

    @Test
    public void testCommitLagStaggeringPartitionsWithRollbackParallel() throws Exception {
        executeWithPool(2, this::testCommitLagStaggeringPartitionsWithRollback0);
    }

    @Test
    public void testCommitLagWithInOrderBatchFollowedByO3BatchContended() throws Exception {
        executeWithPool(0, this::testCommitLagWithInOrderBatchFollowedByO3Batch0);
    }

    @Test
    public void testCommitLagWithInOrderBatchFollowedByO3BatchParallel() throws Exception {
        executeWithPool(2, this::testCommitLagWithInOrderBatchFollowedByO3Batch0);
    }

    @Test
    public void testCommitLagWithLargeO3Contended() throws Exception {
        executeWithPool(0, this::testCommitLagWithLargeO3);
    }

    @Test
    public void testCommitLagWithLargeO3Parallel() throws Exception {
        executeWithPool(2, this::testCommitLagWithLargeO3);
    }

    @Test
    public void testCommitLagWithinPartitionContended() throws Exception {
        executeWithPool(0, this::testCommitLagWithinPartition);
    }

    @Test
    public void testCommitLagWithinPartitionParallel() throws Exception {
        executeWithPool(2, this::testCommitLagWithinPartition);
    }

    @Test
    public void testCommitLagWithinPartitionWithRollbackContended() throws Exception {
        executeWithPool(0, this::testCommitLagWithinPartitionWithRollback);
    }

    @Test
    public void testCommitLagWithinPartitionWithRollbackParallel() throws Exception {
        executeWithPool(2, this::testCommitLagWithinPartitionWithRollback);
    }

    @Test
    public void testContinuousBatchedCommitContended() throws Exception {
        executeWithPool(0, this::testContinuousBatchedCommit0);
    }

    @Test
    public void testContinuousBatchedCommitParallel() throws Exception {
        executeWithPool(2, this::testContinuousBatchedCommit0);
    }

    @Test
    public void testLargeLagWithRowLimitContended() throws Exception {
        executeWithPool(0, this::testLargeLagWithRowLimit);
    }

    @Test
    public void testLargeLagWithinPartitionContended() throws Exception {
        executeWithPool(0, this::testLargeLagWithinPartition);
    }

    @Test
    public void testLargeLagWithinPartitionParallel() throws Exception {
        executeWithPool(2, this::testLargeLagWithinPartition);
    }

    @Test
    public void testNoLagContended() throws Exception {
        executeWithPool(0, this::testNoLag0);
    }

    @Test
    public void testNoLagEndingAtPartitionBoundaryContended() throws Exception {
        executeWithPool(0, this::testNoLagEndingAtPartitionBoundary);
    }

    @Test
    public void testNoLagEndingAtPartitionBoundaryParallel() throws Exception {
        executeWithPool(2, this::testNoLagEndingAtPartitionBoundary);
    }

    @Test
    public void testNoLagParallel() throws Exception {
        executeWithPool(2, this::testNoLag0);
    }

    @Test
    public void testNoLagWithRollbackContended() throws Exception {
        executeWithPool(0, this::testNoLagWithRollback);
    }

    @Test
    public void testNoLagWithRollbackParallel() throws Exception {
        executeWithPool(2, this::testNoLagWithRollback);
    }

    @Test
    public void testRowCountWhenLagIsNextDay() throws Exception {
        executeWithPool(0, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
            String[] dates = new String[]{
                    "2021-01-01T00:05:00.000000Z",
                    "2021-01-01T00:01:00.000000Z",
                    "2021-01-02T00:05:31.000000Z",
                    "2021-01-01T00:01:30.000000Z",
                    "2021-01-02T00:00:30.000000Z",
            };

            // Run same test taking 2-n lines from dates
            for (int length = 2; length <= dates.length; length++) {
                String tableName = "lll" + length;
                LOG.info().$("========= LENGTH ").$(length).$(" ================").$();
                compiler.compile("create table " + tableName + "( " +
                        "ts timestamp" +
                        ") timestamp(ts) partition by DAY " +
                        " WITH maxUncommittedRows=1, commitLag=120s", sqlExecutionContext);

                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "test")) {
                    for (int i = 0; i < length; i++) {
                        long ts = IntervalUtils.parseFloorPartialDate(dates[i]);
                        Row r = writer.newRow(ts);
                        r.append();

                        Assert.assertEquals(i + 1, writer.size());
                        if (writer.getMetadata().getMaxUncommittedRows() < writer.getUncommittedRowCount()) {
                            writer.commitWithLag(CommitMode.NOSYNC);
                        }
                        Assert.assertEquals(i + 1, writer.size());
                    }

                    writer.commit();

                    sink.clear();
                    TestUtils.printSql(compiler, sqlExecutionContext, "select count() from " + tableName, sink);
                    TestUtils.assertEquals(
                            "count\n" + length + "\n"
                            , sink);

                    Assert.assertEquals(length, writer.size());
                    Assert.assertEquals(length > 2 ? 2 : 1, writer.getPartitionCount());
                }
            }
        });
    }

    @Test
    public void testVarColumnPageBoundariesAppend() throws Exception {
        dataAppendPageSize = (int) Files.PAGE_SIZE;
        executeWithPool(0,
                (CairoEngine engine,
                 SqlCompiler compiler,
                 SqlExecutionContext sqlExecutionContext) -> {
                    int longsPerPage = dataAppendPageSize / 8;
                    int hi = (longsPerPage + 8) * 2;
                    int lo = (longsPerPage - 8) * 2;
                    for (int i = lo; i < hi; i++) {
                        LOG.info().$("=========== iteration ").$(i).$(" ===================").$();
                        testVarColumnPageBoundaryIterationWithColumnTop(engine, compiler, sqlExecutionContext, i, 1000);
                        compiler.compile("drop table x", sqlExecutionContext);
                    }
                });
    }

    @Test
    public void testVarColumnPageBoundariesAppendRndPageSize() throws Exception {
        int rndPagesMultiplier = new Rnd(Os.currentTimeMicros(), Os.currentTimeNanos()).nextInt(129);
        int multiplier = Numbers.ceilPow2(rndPagesMultiplier);
        dataAppendPageSize = (int) Files.PAGE_SIZE * multiplier;
        LOG.info().$("Testing with random pages size of ").$(dataAppendPageSize).$();

        executeWithPool(0,
                (CairoEngine engine,
                 SqlCompiler compiler,
                 SqlExecutionContext sqlExecutionContext) -> {
                    int longsPerPage = dataAppendPageSize / 8;
                    int hi = (longsPerPage + 8) * 2;
                    int lo = (longsPerPage - 8) * 2;
                    for (int i = lo; i < hi; i++) {
                        LOG.info().$("=========== iteration ").$(i).$(" ===================").$();
                        testVarColumnPageBoundaryIterationWithColumnTop(engine, compiler, sqlExecutionContext, i, 1000);
                        compiler.compile("drop table x", sqlExecutionContext);
                    }
                });
    }

    @Test
    public void testVarColumnPageBoundaries4() throws Exception {
        dataAppendPageSize = (int) Files.PAGE_SIZE;
        executeWithPool(0,
                (CairoEngine engine,
                 SqlCompiler compiler,
                 SqlExecutionContext sqlExecutionContext) -> {
                    int longsPerPage = dataAppendPageSize / 8;
                    int hi = (longsPerPage + 8) * 2;
                    int lo = (longsPerPage - 8) * 2;
                    int maxUncommitted = new Rnd(Os.currentTimeMicros(), Os.currentTimeNanos()).nextInt(hi);

                    int initialCountLo = longsPerPage / 2 - 1;
                    int additionalCountLo = longsPerPage / 2 - 1;

                    for (int initialCount = initialCountLo; initialCount < initialCountLo + 3; initialCount++) {
                        for (int additionalCount = additionalCountLo; additionalCount < additionalCountLo + 3; additionalCountLo++) {
                            for (int i = lo; i < hi; i++) {
                                LOG.info().$("=========== iteration ").$(i).$(", max uncommitted ").$(longsPerPage).$(" ===================").$();
                                testVarColumnMergeWithColumnTops(engine, compiler, sqlExecutionContext, initialCount, longsPerPage / 2, i, maxUncommitted);
                                compiler.compile("drop table x", sqlExecutionContext);
                            }
                        }
                    }
                });
    }

    @Test
    public void testVarColumnPageBoundaries3() throws Exception {
        dataAppendPageSize = (int) Files.PAGE_SIZE;
        executeWithPool(0,
                (CairoEngine engine,
                 SqlCompiler compiler,
                 SqlExecutionContext sqlExecutionContext) -> {
                    int longsPerPage = dataAppendPageSize / 8;
                    int hi = (longsPerPage + 8) * 2;
                    int lo = (longsPerPage - 8) * 2;
                    int maxUncommitted = new Rnd(Os.currentTimeMicros(), Os.currentTimeNanos()).nextInt(hi);

                    int initialCountLo = longsPerPage / 2 - 1;
                    int additionalCountLo = longsPerPage / 2 - 1;

                    for (int initialCount = initialCountLo; initialCount < initialCountLo + 3; initialCount++) {
                        for (int additionalCount = additionalCountLo; additionalCount < additionalCountLo + 3; additionalCountLo++) {
                            for (int i = lo; i < hi; i++) {
                                LOG.info().$("=========== iteration ").$(i).$(", max uncommitted ").$(longsPerPage).$(" ===================").$();
                                testVarColumnMergeWithColumnTops(engine, compiler, sqlExecutionContext, initialCount, additionalCount, i, maxUncommitted);
                                compiler.compile("drop table x", sqlExecutionContext);
                            }
                        }
                    }
                });
    }

    @Test
    public void testVarColumnPageBoundariesInO3Memory() throws Exception {
        dataAppendPageSize = (int) Files.PAGE_SIZE;
        executeWithPool(0,
                (CairoEngine engine,
                 SqlCompiler compiler,
                 SqlExecutionContext sqlExecutionContext) -> {
                    int longsPerPage = dataAppendPageSize / 8;
                    int hi = (longsPerPage + 8) * 2;
                    int lo = (longsPerPage - 8) * 2;
                    for (int i = lo; i < hi; i++) {
                        LOG.info().$("=========== iteration ").$(i).$(", max uncommitted ").$(longsPerPage).$(" ===================").$();
                        testVarColumnPageBoundaryIterationWithColumnTop(engine, compiler, sqlExecutionContext, i, longsPerPage);
                        compiler.compile("drop table x", sqlExecutionContext);
                    }
                });
    }

    @Test
    public void testVarColumnPageBoundariesRndMaxUncommitted() throws Exception {
        dataAppendPageSize = (int) Files.PAGE_SIZE;
        executeWithPool(0,
                (CairoEngine engine,
                 SqlCompiler compiler,
                 SqlExecutionContext sqlExecutionContext) -> {
                    int longsPerPage = dataAppendPageSize / 8;
                    int hi = (longsPerPage + 8) * 2;
                    int lo = (longsPerPage - 8) * 2;
                    int maxUncommitted = new Rnd(Os.currentTimeMicros(), Os.currentTimeNanos()).nextInt(hi);
                    for (int i = lo; i < hi; i++) {
                        LOG.info().$("=========== iteration ").$(i).$(", max uncommitted ").$(maxUncommitted).$(" ===================").$();
                        testVarColumnPageBoundaryIterationWithColumnTop(engine, compiler, sqlExecutionContext, i, maxUncommitted);
                        compiler.compile("drop table x", sqlExecutionContext);
                    }
                });
    }

    private void appendRows(String ts1, String ts2, TableWriter tw, int count, Rnd rnd) throws NumericException {
        for (int i = 0; i < count; i++) {
            Row row = tw.newRow(IntervalUtils.parseFloorPartialDate(ts1) + rnd.nextLong(Timestamps.DAY_MICROS));

            row.putStr(0, "cc");
            row.putLong(2, 11111L);
            row.putStr(3, "dd");
            row.putLong(4, 22222L);
            row.append();

            row = tw.newRow(IntervalUtils.parseFloorPartialDate(ts2));
            row.putStr(0, "cc");
            row.putLong(2, 333333L);
            row.putStr(3, "dd");
            row.putLong(4, 444444L);
            row.append();
        }
    }

    private void assertXY(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        TestUtils.printSql(compiler, sqlExecutionContext, "select count() from x", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select count() from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void insertUncommitted(
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String sql,
            TableWriter writer
    ) throws SqlException {
        minTimestamp = Long.MAX_VALUE;
        maxTimestamp = Long.MIN_VALUE;
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            RecordMetadata metadata = factory.getMetadata();
            int timestampIndex = writer.getMetadata().getTimestampIndex();
            EntityColumnFilter toColumnFilter = new EntityColumnFilter();
            toColumnFilter.of(metadata.getColumnCount());
            if (null == copier) {
                copier = SqlCompiler.assembleRecordToRowCopier(new BytecodeAssembler(), metadata, writer.getMetadata(), toColumnFilter);
            }
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    long timestamp = record.getTimestamp(timestampIndex);
                    if (timestamp > maxTimestamp) {
                        maxTimestamp = timestamp;
                    }
                    if (timestamp < minTimestamp) {
                        minTimestamp = timestamp;
                    }
                    Row row = writer.newRow(timestamp);
                    copier.copy(record, row);
                    row.append();
                }
            }
        }
    }

    private void testBigUncommittedMove1(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            TableModel tableModel
    ) throws NumericException, SqlException {
        // Create empty tables of same structure
        TestUtils.createPopulateTable("o3",
                compiler,
                sqlExecutionContext,
                tableModel,
                0,
                "2021-04-27",
                0
        );

        TestUtils.createPopulateTable("ordered",
                compiler,
                sqlExecutionContext,
                tableModel,
                0,
                "2021-04-27",
                0
        );

        int longColIndex = -1;
        int floatColIndex = -1;
        int strColIndex = -1;
        for (int i = 0; i < tableModel.getColumnCount(); i++) {
            switch (ColumnType.tagOf(tableModel.getColumnType(i))) {
                case ColumnType.LONG:
                    longColIndex = i;
                    break;
                case ColumnType.FLOAT:
                    floatColIndex = i;
                    break;
                case ColumnType.STRING:
                    strColIndex = i;
                    break;
            }
        }

        long start = IntervalUtils.parseFloorPartialDate("2021-04-27T08:00:00");
        long[] testCounts = new long[]{2 * 1024 * 1024, 16 * 8 * 1024 * 5, 2_000_000};
        for (int c = 0; c < testCounts.length; c++) {
            long idCount = testCounts[c];

            // Create big commit with a big part before OOO starts
            // which exceeds default MAMemoryImpl size in one or all columns
            int iterations = 2;
            String[] varCol = new String[]{"abc", "aldfjkasdlfkj", "as", "2021-04-27T12:00:00", "12345678901234578"};

            // Add 2 batches
            try (TableWriter o3 = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "o3", "testing");
                 TableWriter ordered = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "ordered", "testing")) {
                for (int i = 0; i < iterations; i++) {
                    long backwards = iterations - i - 1;
                    final Rnd rnd = new Rnd();
                    for (int id = 0; id < idCount; id++) {
                        long timestamp = start + backwards * idCount + id;
                        Row row = o3.newRow(timestamp);
                        if (longColIndex > -1) {
                            row.putLong(longColIndex, timestamp);
                        }
                        if (floatColIndex > -1) {
                            row.putFloat(floatColIndex, rnd.nextFloat());
                        }
                        if (strColIndex > -1) {
                            row.putStr(strColIndex, varCol[id % varCol.length]);
                        }
                        row.append();

                        timestamp = start + i * idCount + id;
                        row = ordered.newRow(timestamp);
                        if (longColIndex > -1) {
                            row.putLong(longColIndex, timestamp);
                        }
                        if (floatColIndex > -1) {
                            row.putFloat(floatColIndex, rnd.nextFloat());
                        }
                        if (strColIndex > -1) {
                            row.putStr(strColIndex, varCol[id % varCol.length]);
                        }
                        row.append();
                    }
                }

                o3.commit();
                ordered.commit();
            }
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, "ordered", "o3", LOG);
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, "ordered", "o3", LOG);

            engine.releaseAllReaders();
            try (TableWriter o3 = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "o3", "testing");
                 TableWriter ordered = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "ordered", "testing")) {
                o3.truncate();
                ordered.truncate();
            }
        }
    }

    private void testCommitLagEndingAtPartitionBoundary0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException, NumericException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L) ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        // i=184 is the last entry in date 1970-01-06
        sql = "create table y as (select * from x where i<=150) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y", "testing")) {
            sql = "select * from x where i>150 and i<200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long commitLag = maxTimestamp - TimestampFormatUtils.parseTimestamp("1970-01-06T23:59:59.000Z");
            maxTimestamp -= commitLag;
            writer.commitWithLag(commitLag);
            TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            sql = "select * from x where i>=200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            commitLag = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= commitLag;
            writer.commitWithLag(commitLag);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void testCommitLagEndingAtPartitionBoundaryPlus10(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException, NumericException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L) ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        // i=184 is the last entry in date 1970-01-06
        sql = "create table y as (select * from x where i<=150) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y", "testing")) {
            sql = "select * from x where i>150 and i<200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long commitLag = maxTimestamp - TimestampFormatUtils.parseTimestamp("1970-01-07T00:00:00.000Z");
            maxTimestamp -= commitLag;
            writer.commitWithLag(commitLag);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            sql = "select * from x where i>=200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            commitLag = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= commitLag;
            writer.commitWithLag(commitLag);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }

        assertXY(compiler, sqlExecutionContext);
    }

    private void testCommitLagEndingAtPartitionBoundaryPlus1WithRollback0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, NumericException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L) ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        // i=184 is the last entry in date 1970-01-06
        sql = "create table y as (select * from x where i<=150) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y", "testing")) {
            sql = "select * from x where i>150 and i<200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long commitLag = maxTimestamp - TimestampFormatUtils.parseTimestamp("1970-01-07T00:00:00.000Z");
            minTimestamp = maxTimestamp - commitLag;
            writer.commitWithLag(commitLag);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<='1970-01-07T00:00:00.000Z'", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.rollback();
            Assert.assertFalse(writer.inTransaction());
            TestUtils.printSql(compiler, sqlExecutionContext, "select count(*) from y", sink);
            TestUtils.assertEquals("count\n185\n", sink);

            sql = "select * from x where i>=200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            commitLag = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= commitLag;
            writer.commitWithLag(commitLag);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp) and (i<=185 or i>=200)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=185 or i>=200", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        TestUtils.printSql(compiler, sqlExecutionContext, "select count() from (select * from x where i<=185 or i>=200)", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select count() from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void testCommitLagEndingAtPartitionBoundaryWithRollback0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, NumericException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L) ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        // i=184 is the last entry in date 1970-01-06
        sql = "create table y as (select * from x where i<=150) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y", "testing")) {
            sql = "select * from x where i>150 and i<200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long commitLag = maxTimestamp - TimestampFormatUtils.parseTimestamp("1970-01-06T23:59:59.000Z");
            minTimestamp = maxTimestamp - commitLag;
            writer.commitWithLag(commitLag);
            TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from x where ts<='1970-01-06T23:59:59.000Z'", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.rollback();
            TestUtils.printSql(compiler, sqlExecutionContext, "select count(*) from y", sink);
            TestUtils.assertEquals("count\n184\n", sink);

            sql = "select * from x where i>=200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            commitLag = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= commitLag;
            writer.commitWithLag(commitLag);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp) and (i<=184 or i>=200)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=184 or i>=200", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void testCommitLagStaggeringPartitions0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L) ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        // i=184 is the last entry in date 1970-01-06
        sql = "create table y as (select * from x where i<=150) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y", "testing")) {
            sql = "select * from x where i>150 and i<200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long commitLag = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= commitLag;
            writer.commitWithLag(commitLag);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            sql = "select * from x where i>=200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            commitLag = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= commitLag;
            writer.commitWithLag(commitLag);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }

        assertXY(compiler, sqlExecutionContext);
    }

    private void testCommitLagStaggeringPartitionsWithRollback0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L) ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        // i=184 is the last entry in date 1970-01-06
        sql = "create table y as (select * from x where i<=150) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y", "testing")) {
            sql = "select * from x where i>150 and i<200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long commitLag = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= commitLag;
            writer.commitWithLag(commitLag);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.rollback();
            TestUtils.printSql(compiler, sqlExecutionContext, "select count(*) from y", sink);
            TestUtils.assertEquals("count\n175\n", sink);

            sql = "select * from x where i>=200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            commitLag = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= commitLag;
            writer.commitWithLag(commitLag);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp) and (i<=175 or i>=200)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=175 or i>=200", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void testCommitLagWithInOrderBatchFollowedByO3Batch0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L) ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        sql = "create table y as (select * from x where  i<=250) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=250", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y", "testing")) {
            sql = "select * from x where i>250 and i<300";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            sql = "select * from x where i>=300 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long commitLag = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= commitLag;
            writer.commitWithLag(commitLag);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }

        assertXY(compiler, sqlExecutionContext);
    }

    private void testCommitLagWithLargeO3(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L) ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        sql = "create table y as (select * from x where (i<=50 or i>=100) and i<=250) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where (i<=50 or i>=100) and i<=250", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y", "testing")) {
            sql = "select * from x where i>250 or (i>50 and i<100) order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long commitLag = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= commitLag;
            writer.commitWithLag(commitLag);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }

        assertXY(compiler, sqlExecutionContext);
    }

    private void testCommitLagWithinPartition(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L) ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        sql = "create table y as (select * from x where i<=490) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=490", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y", "testing")) {
            sql = "select * from x where i>490 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long commitLag = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= commitLag;
            writer.commitWithLag(commitLag);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }
        assertXY(compiler, sqlExecutionContext);
    }

    private void testCommitLagWithinPartitionWithRollback(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L) ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        sql = "create table y as (select * from x where i<=250) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=250", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y", "testing")) {
            sql = "select * from x where i>250 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long commitLag = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= commitLag;
            writer.commitWithLag(commitLag);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.rollback();
            TestUtils.printSql(compiler, sqlExecutionContext, "select count(*) from y", sink);
            TestUtils.assertEquals("count\n375\n", sink);

            sql = "select * from x where i>380 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.commit();
            TestUtils.printSql(compiler, sqlExecutionContext, "select count(*) from y", sink);
            TestUtils.assertEquals("count\n495\n", sink);
        }

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=375 or i>380", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        TestUtils.printSql(compiler, sqlExecutionContext, "select count() from (select * from x where i<=375 or i>380)", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select count() from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void testContinuousBatchedCommit0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        int nTotalRows = 50000;
        int nInitialStateRows = 150;
        long microsBetweenRows = 100000000;
        int maxBatchedRows = 10;
        int maxConcurrentBatches = 4;
        int nRowsPerCommit = 100;
        long commitLag = microsBetweenRows * (nRowsPerCommit / 2);

        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(0L," + microsBetweenRows + "L) ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(" + nTotalRows + ")" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        sql = "create table y as (select * from x where  i<=" + nInitialStateRows + ") partition by DAY";
        compiler.compile(sql, sqlExecutionContext);
        LOG.info().$("committed initial state").$();

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=" + nInitialStateRows, sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        Random rand = new Random(0);
        IntList batchRowEnd = new IntList((int) ((nTotalRows - nInitialStateRows) * 0.6 * maxBatchedRows));
        int atRow = nInitialStateRows;
        batchRowEnd.add(-atRow); // negative row means this has been commited
        while (atRow < nTotalRows) {
            int nRows = rand.nextInt(maxBatchedRows) + 1;
            atRow += nRows;
            batchRowEnd.add(atRow);
        }

        int nCommitsWithLag = 0;
        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y", "testing")) {
            int nHeadBatch = 1;
            int nRowsAppended = 0;
            while (nHeadBatch < batchRowEnd.size()) {
                int nBatch = nHeadBatch + rand.nextInt(maxConcurrentBatches);
                while (nBatch >= batchRowEnd.size() || batchRowEnd.get(nBatch) < 0) {
                    nBatch--;
                }
                assert nBatch >= nHeadBatch;
                if (nBatch == nHeadBatch) {
                    do {
                        nHeadBatch++;
                    } while (nHeadBatch < batchRowEnd.size() && batchRowEnd.get(nHeadBatch) < 0);
                }
                int fromRow = Math.abs(batchRowEnd.get(nBatch - 1));
                int toRow = batchRowEnd.get(nBatch);
                batchRowEnd.set(nBatch, -toRow);

                LOG.info().$("inserting rows from ").$(fromRow).$(" to ").$(toRow).$();
                sql = "select * from x where ts>=cast(" + fromRow * microsBetweenRows + " as timestamp) and ts<cast(" + toRow * microsBetweenRows + " as timestamp)";
                insertUncommitted(compiler, sqlExecutionContext, sql, writer);

                nRowsAppended += toRow - fromRow;
                if (nRowsAppended >= nRowsPerCommit) {
                    LOG.info().$("committing with lag").$();
                    nRowsAppended = 0;
                    writer.commitWithLag(commitLag);
                    nCommitsWithLag++;
                }
            }
            writer.commit();
        }
        LOG.info().$("committed final state with ").$(nCommitsWithLag).$(" commits with lag").$();

        assertXY(compiler, sqlExecutionContext);
    }

    private void testLargeLagWithRowLimit(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L) ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        sql = "create table y as (select * from x where i<=250) partition by DAY WITH maxUncommittedRows=100, commitLag=10s";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=250", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y", "testing")) {
            sql = "select * from x where i>250 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long commitLag = (maxTimestamp - minTimestamp) * 3 / 4;
            maxTimestamp -= commitLag;
            writer.commitWithLag(commitLag);
            TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from x limit 400", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from y", sink2);
            TestUtils.assertEquals(sink, sink2);
            writer.commit();
        }
        assertXY(compiler, sqlExecutionContext);
    }

    private void testLargeLagWithinPartition(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L) ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        sql = "create table y as (select * from x where i<=250) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=250", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y", "testing")) {
            sql = "select * from x where i>250 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long commitLag = (maxTimestamp - minTimestamp) * 3 / 4;
            maxTimestamp -= commitLag;
            writer.commitWithLag(commitLag);
            TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }
        assertXY(compiler, sqlExecutionContext);
    }

    private void testNoLag0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L) ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        sql = "create table y as (select * from x where i<=150) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y", "testing")) {
            sql = "select * from x where i>150 and i<=495 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.commit();
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=495", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            sql = "select * from x where i>495 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.commit();
        }

        assertXY(compiler, sqlExecutionContext);
    }

    private void testNoLagEndingAtPartitionBoundary(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L) ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        // i=184 is the last entry in date 1970-01-06
        sql = "create table y as (select * from x where i<=150) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y", "testing")) {
            sql = "select * from x where i>150 and i<=184 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.commit();
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=184", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            sql = "select * from x where i>184 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.commit();
        }
        assertXY(compiler, sqlExecutionContext);
    }

    private void testNoLagWithRollback(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L) ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        sql = "create table y as (select * from x where i<=250) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=250", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y", "testing")) {
            sql = "select * from x where i>250 and i<=375 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.commit();
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=375", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            sql = "select * from x where i>375 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.rollback();
            TestUtils.printSql(compiler, sqlExecutionContext, "select count(*) from y", sink);
            TestUtils.assertEquals("count\n375\n", sink);

            sql = "select * from x where i>380 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.commit();
            TestUtils.printSql(compiler, sqlExecutionContext, "select count(*) from y", sink);
            TestUtils.assertEquals("count\n495\n", sink);
        }

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=375 or i>380", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        TestUtils.printSql(compiler, sqlExecutionContext, "select count() from (select * from x where i<=375 or i>380)", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select count() from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void testVarColumnPageBoundaryIterationWithColumnTop(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, int iteration, int maxUncommittedRows) throws SqlException, NumericException {
        // Day 1 '1970-01-01'
        int appendCount = iteration / 2;
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " 'aa' as str," +
                        " timestamp_sequence('1970-01-01T11:00:00',1000L) ts," +
                        " x " +
                        " from long_sequence(1)" +
                        ") timestamp (ts) partition by DAY with maxUncommittedRows = " + maxUncommittedRows,
                sqlExecutionContext
        );

        // Day 2 '1970-01-02'
        compiler.compile(
                "insert into x " +
                        "select" +
                        " 'aa' as str," +
                        " timestamp_sequence('1970-01-02T00:00:00',1000L) ts," +
                        " x " +
                        " from long_sequence(1)",
                sqlExecutionContext
        );

        compiler.compile("alter table x add column str2 string", sqlExecutionContext).execute(null).await();
        compiler.compile("alter table x add column y long", sqlExecutionContext).execute(null).await();

        if (iteration % 2 == 0) {
            engine.releaseAllWriters();
        }

        final String ts1 = "1970-01-01";
        final String ts2 = "1970-01-02T10:00:01.000000Z";

        Rnd rnd = new Rnd();
        try (TableWriter tw = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "test")) {
            int halfCount = appendCount / 2;
            appendRows(ts1, ts2, tw, halfCount, rnd);
            tw.commitWithLag(Timestamps.HOUR_MICROS);

            TestUtils.assertSql(compiler, sqlExecutionContext, "select * from x where str = 'aa'", sink,
                    "str\tts\tx\tstr2\ty\n" +
                            "aa\t1970-01-01T11:00:00.000000Z\t1\t\tNaN\n" +
                            "aa\t1970-01-02T00:00:00.000000Z\t1\t\tNaN\n");

            appendRows(ts1, ts2, tw, appendCount - halfCount, rnd);
            tw.commitWithLag(Timestamps.HOUR_MICROS);

            TestUtils.assertSql(compiler, sqlExecutionContext, "select * from x where str = 'aa'", sink,
                    "str\tts\tx\tstr2\ty\n" +
                            "aa\t1970-01-01T11:00:00.000000Z\t1\t\tNaN\n" +
                            "aa\t1970-01-02T00:00:00.000000Z\t1\t\tNaN\n");

            if (iteration % 2 == 0) {
                tw.commit();
            }
        }

        if (iteration % 3 == 0) {
            engine.releaseAllWriters();
        }

        TestUtils.assertSql(compiler, sqlExecutionContext, "select * from x where str = 'aa'", sink,
                "str\tts\tx\tstr2\ty\n" +
                        "aa\t1970-01-01T11:00:00.000000Z\t1\t\tNaN\n" +
                        "aa\t1970-01-02T00:00:00.000000Z\t1\t\tNaN\n");
    }

    private void testVarColumnMergeWithColumnTops(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, int initialCount,  int additionalCount, int iteration, int maxUncommittedRows) throws SqlException, NumericException {
        // Day 1 '1970-01-01'
        int appendCount = iteration / 2;
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " 'aa' as str," +
                        " timestamp_sequence('1970-01-01T11:00:00',1000L) ts," +
                        " x " +
                        " from long_sequence(" + initialCount + ")" +
                        ") timestamp (ts) partition by DAY with maxUncommittedRows = " + maxUncommittedRows,
                sqlExecutionContext
        );

        compiler.compile("alter table x add column str2 string", sqlExecutionContext).execute(null).await();
        compiler.compile("alter table x add column y long", sqlExecutionContext).execute(null).await();
        compiler.compile(
                "insert into x " +
                        "select" +
                        " 'aa' as str," +
                        " timestamp_sequence('1970-01-01T12:00:00',1000L) ts," +
                        " x," +
                        " '77' as str2," +
                        " x * 1000 as y " +
                        " from long_sequence(" + additionalCount + ")",
                sqlExecutionContext
        );

        // Day 2 '1970-01-02'
        compiler.compile(
                "insert into x " +
                        "select" +
                        " 'aa' as str," +
                        " timestamp_sequence('1970-01-02T00:00:00',1000L) ts," +
                        " x, " +
                        " '11' as str2," +
                        " x * 1000 as y " +
                        " from long_sequence(" + initialCount + ")",
                sqlExecutionContext
        );

        compiler.compile(
                "insert into x " +
                        "select" +
                        " 'aa' as str," +
                        " timestamp_sequence('1970-01-02T01:00:00',1000L) ts," +
                        " x, " +
                        " '33' as str2," +
                        " x * 222 as y " +
                        " from long_sequence(" + additionalCount + ")",
                sqlExecutionContext
        );


        if (iteration % 2 == 0) {
            engine.releaseAllWriters();
        }

        final String ts1 = "1970-01-01T11:00:00.000000Z";
        final String ts2 = "1970-01-02T11:00:00.000000Z";

        // We will not insert value 'aa' in str column. The count of rows with 'aa' is our invariant
        int aaCount = 2 * (initialCount + additionalCount);
        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "select count() from x where str = 'aa'", sink,
                "count\n" +
                        aaCount + "\n");
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where str = 'aa'", sink2);

        Rnd rnd = new Rnd();
        try (TableWriter tw = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "test")) {
            int halfCount = appendCount / 2;
            appendRows(ts1, ts2, tw, halfCount, rnd);
            tw.commitWithLag(Timestamps.HOUR_MICROS);

            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "select * from x where str = 'aa'",
                    sink,
                    sink2);

            appendRows(ts1, ts2, tw, appendCount - halfCount, rnd);
            tw.commitWithLag(Timestamps.HOUR_MICROS);

            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "select * from x where str = 'aa'",
                    sink,
                    sink2);

            if (iteration % 2 == 0) {
                tw.commit();
            }
        }

        if (iteration % 3 == 0) {
            engine.releaseAllWriters();
        }

        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "select * from x where str = 'aa'",
                sink,
                sink2);
    }
}
