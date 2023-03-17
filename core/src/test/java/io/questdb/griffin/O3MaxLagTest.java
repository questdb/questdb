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

import io.questdb.cairo.*;
import io.questdb.cairo.TableWriter.Row;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.griffin.wal.fuzz.FuzzTransaction;
import io.questdb.griffin.wal.fuzz.FuzzTransactionGenerator;
import io.questdb.griffin.wal.fuzz.FuzzTransactionOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class O3MaxLagTest extends AbstractO3Test {
    private final static Log LOG = LogFactory.getLog(O3MaxLagTest.class);
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
    public void testContinuousBatchedCommitContended() throws Exception {
        executeWithPool(0, this::testContinuousBatchedCommit0);
    }

    @Test
    public void testContinuousBatchedCommitParallel() throws Exception {
        executeWithPool(2, this::testContinuousBatchedCommit0);
    }

    @Test
    public void testFuzzParallel() throws Exception {
        executeWithPool(2, this::testFuzz0);
    }

    @Test
    public void testIntermediateCommitCreatesEmptyPartition() throws Exception {
        executeWithPool(
                2, ((engine, compiler, sqlExecutionContext) -> testFuzz00(
                        engine, compiler, sqlExecutionContext, new Rnd(473693170209L, 1669125220064L))
                )
        );
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
    public void testMetadataReshuffle() throws Exception {
        executeWithPool(
                2, (engine, compiler, sqlExecutionContext) -> testFuzz00(
                        engine, compiler, sqlExecutionContext, new Rnd(22306374689795L, 1669046069639L)
                )
        );
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
    public void testO3MaxLagEndingAtPartitionBoundaryContended() throws Exception {
        executeWithPool(0, this::testO3MaxLagEndingAtPartitionBoundary0);
    }

    @Test
    public void testO3MaxLagEndingAtPartitionBoundaryParallel() throws Exception {
        executeWithPool(2, this::testO3MaxLagEndingAtPartitionBoundary0);
    }

    @Test
    public void testO3MaxLagEndingAtPartitionBoundaryPlus1Contended() throws Exception {
        executeWithPool(0, this::testO3MaxLagEndingAtPartitionBoundaryPlus10);
    }

    @Test
    public void testO3MaxLagEndingAtPartitionBoundaryPlus1Parallel() throws Exception {
        executeWithPool(2, this::testO3MaxLagEndingAtPartitionBoundaryPlus10);
    }

    @Test
    public void testO3MaxLagStaggeredPartitionsContended() throws Exception {
        executeWithPool(0, this::testO3MaxLagStaggeredPartitions0);
    }

    @Test
    public void testO3MaxLagStaggeredPartitionsParallel() throws Exception {
        executeWithPool(2, this::testO3MaxLagStaggeredPartitions0);
    }

    @Test
    public void testO3MaxLagWithInOrderBatchFollowedByO3BatchContended() throws Exception {
        executeWithPool(0, this::testO3MaxLagWithInOrderBatchFollowedByO3Batch0);
    }

    @Test
    public void testO3MaxLagWithInOrderBatchFollowedByO3BatchParallel() throws Exception {
        executeWithPool(2, this::testO3MaxLagWithInOrderBatchFollowedByO3Batch0);
    }

    @Test
    public void testO3MaxLagWithLargeO3Contended() throws Exception {
        executeWithPool(0, this::testO3MaxLagWithLargeO3);
    }

    @Test
    public void testO3MaxLagWithLargeO3Parallel() throws Exception {
        executeWithPool(2, this::testO3MaxLagWithLargeO3);
    }

    @Test
    public void testO3MaxLagWithinPartitionContended() throws Exception {
        executeWithPool(0, this::testO3MaxLagWithinPartition);
    }

    @Test
    public void testO3MaxLagWithinPartitionParallel() throws Exception {
        executeWithPool(2, this::testO3MaxLagWithinPartition);
    }

    @Test
    public void testRollbackFuzzCreatesEmptyPartition() throws Exception {
        executeWithPool(2, this::testRollbackCreatesEmptyPartition);
    }

    @Test
    public void testRollbackFuzzParallel() throws Exception {
        executeWithPool(2, this::testRollbackFuzz);
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
                        " WITH maxUncommittedRows=1, o3MaxLag=120s", sqlExecutionContext);

                try (TableWriter writer = getWriter(engine, tableName)) {
                    for (int i = 0; i < length; i++) {
                        long ts = IntervalUtils.parseFloorPartialTimestamp(dates[i]);
                        Row r = writer.newRow(ts);
                        r.append();

                        Assert.assertEquals(i + 1, writer.size());
                        if (writer.getMetadata().getMaxUncommittedRows() < writer.getUncommittedRowCount()) {
                            writer.ic(CommitMode.NOSYNC);
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

                    int initialCountLo = longsPerPage - 1;
                    int additionalCountLo = longsPerPage - 1;

                    for (int initialCount = initialCountLo; initialCount < initialCountLo + 3; initialCount++) {
                        for (int additionalCount = additionalCountLo; additionalCount < additionalCountLo + 3; additionalCount++) {
                            for (int i = lo; i < hi; i++) {
                                LOG.info().$("=========== count1 ").$(initialCount).$(" count2 = ").$(additionalCount).$("iteration ").$(i).$(", max uncommitted ").$(maxUncommitted).$(" ===================").$();
                                testVarColumnMergeWithColumnTops(
                                        engine,
                                        compiler,
                                        sqlExecutionContext,
                                        initialCount,
                                        additionalCount,
                                        i,
                                        maxUncommitted,
                                        new Rnd());
                                compiler.compile("drop table x", sqlExecutionContext);
                                compiler.compile("drop table y", sqlExecutionContext);
                            }
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
    public void testVarColumnPageBoundariesFail() throws Exception {
        dataAppendPageSize = (int) Files.PAGE_SIZE;
        executeWithPool(
                0,
                (CairoEngine engine,
                 SqlCompiler compiler,
                 SqlExecutionContext sqlExecutionContext) -> {
                    int longsPerPage = dataAppendPageSize / 8;
                    testVarColumnMergeWithColumnTops(
                            engine,
                            compiler,
                            sqlExecutionContext,
                            longsPerPage - 1,
                            longsPerPage,
                            8,
                            0,
                            new Rnd()
                    );
                    compiler.compile("drop table x", sqlExecutionContext);
                    compiler.compile("drop table y", sqlExecutionContext);
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

    private static void replayTransactions(Rnd rnd, TableWriter w, ObjList<FuzzTransaction> transactions, int virtualTimestampIndex) {
        for (int i = 0, n = transactions.size(); i < n; i++) {
            FuzzTransaction tx = transactions.getQuick(i);
            ObjList<FuzzTransactionOperation> ops = tx.operationList;
            for (int j = 0, k = ops.size(); j < k; j++) {
                ops.getQuick(j).apply(rnd, w, virtualTimestampIndex);
            }
            w.ic();
        }
    }

    private static void testFuzz00(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            Rnd rnd
    ) throws SqlException, NumericException {
        long microsBetweenRows = 1000000L;
        int nTotalRows = 12000;
        // create initial table "x"
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

        compiler.compile("create table y as (select * from x order by t)", sqlExecutionContext);
        compiler.compile("create table z as (select * from x order by t)", sqlExecutionContext);

        TestUtils.assertEquals(compiler, sqlExecutionContext, "y order by ts", "x");

        long minTs = TimestampFormatUtils.parseTimestamp("2022-11-11T14:28:00.000000Z");
        long maxTs = TimestampFormatUtils.parseTimestamp("2022-11-12T14:28:00.000000Z");
        int txCount = Math.max(1, rnd.nextInt(50));
        int rowCount = Math.max(1, txCount * rnd.nextInt(200) * 1000);
        try (
                TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), engine.getTableToken("x"), "test");
                TableWriter w2 = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), engine.getTableToken("y"), "test")
        ) {
            ObjList<FuzzTransaction> transactions = FuzzTransactionGenerator.generateSet(
                    w.getMetadata(),
                    rnd,
                    minTs,
                    maxTs,
                    rowCount,
                    txCount,
                    true,
                    rnd.nextDouble(),
                    rnd.nextDouble(),
                    rnd.nextDouble(),
                    rnd.nextDouble(),
                    rnd.nextDouble(),
                    rnd.nextDouble(),
                    rnd.nextDouble(),
                    1, // insert only
                    0,
                    5,
                    new String[]{"ABC", "CDE", "XYZ"}
            );

            Rnd rnd1 = new Rnd();
            replayTransactions(rnd1, w, transactions, -1);
            w.commit();

            Rnd rnd2 = new Rnd();
            replayTransactions(rnd2, w2, transactions, w.getMetadata().getTimestampIndex());
            w2.commit();

            TestUtils.assertEquals(compiler, sqlExecutionContext, "y order by ts", "x");
        }
    }

    private void appendRows(TableWriter tw, int count, Rnd rnd) throws NumericException {
        for (int i = 0; i < count; i++) {
            long timestamp = IntervalUtils.parseFloorPartialTimestamp("1970-01-01T11:00:00.000000Z") + rnd.nextLong(Timestamps.DAY_MICROS);
            Row row = tw.newRow(timestamp);

            row.putStr(0, "cc");
            row.putLong(2, 11111L);
            row.putStr(3, "dd");
            row.putLong(4, 22222L);
            row.append();

            row = tw.newRow(IntervalUtils.parseFloorPartialTimestamp("1970-01-02T11:00:00.000000Z"));
            row.putStr(0, "cc");
            row.putLong(2, 333333L);
            row.putStr(3, "dd");
            row.putLong(4, 444444L);
            row.append();
        }
    }

    private void appendRowsWithDroppedColumn(TableWriter tw, int count, Rnd rnd) throws NumericException {
        for (int i = 0; i < count; i++) {
            long timestamp = IntervalUtils.parseFloorPartialTimestamp("1970-01-01") + rnd.nextLong(Timestamps.DAY_MICROS);
            Row row = tw.newRow(timestamp);

            row.putStr(0, "cc");
            row.putLong(2, 11111L);
            row.putStr(4, "dd");
            row.putLong(5, 22222L);
            row.append();

            row = tw.newRow(IntervalUtils.parseFloorPartialTimestamp("1970-01-02T10:00:01.000000Z"));
            row.putStr(0, "cc");
            row.putLong(2, 333333L);
            row.putStr(4, "dd");
            row.putLong(5, 444444L);
            row.append();
        }
    }

    private void assertXY(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
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

    private TableWriter getWriter(CairoEngine engine, String token) {
        return engine.getWriter(
                AllowAllCairoSecurityContext.INSTANCE,
                engine.getTableToken(token),
                "test"
        );
    }

    private void insertUncommitted(
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

        long start = IntervalUtils.parseFloorPartialTimestamp("2021-04-27T08:00:00");
        long[] testCounts = new long[]{2 * 1024 * 1024, 16 * 8 * 1024 * 5, 2_000_000};
        //noinspection ForLoopReplaceableByForEach
        for (int c = 0; c < testCounts.length; c++) {
            long idCount = testCounts[c];

            // Create big commit with a big part before OOO starts
            // which exceeds default MAMemoryImpl size in one or all columns
            int iterations = 2;
            String[] varCol = new String[]{"abc", "aldfjkasdlfkj", "as", "2021-04-27T12:00:00", "12345678901234578"};

            // Add 2 batches
            try (TableWriter o3 = getWriter(engine, "o3");
                 TableWriter ordered = getWriter(engine, "ordered")) {
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
            TestUtils.assertEquals(compiler, sqlExecutionContext, "ordered", "o3");
            engine.releaseAllWriters();
            TestUtils.assertEquals(compiler, sqlExecutionContext, "ordered", "o3");

            engine.releaseAllReaders();
            try (TableWriter o3 = getWriter(engine, "o3");
                 TableWriter ordered = getWriter(engine, "ordered")) {
                o3.truncate();
                ordered.truncate();
            }
        }
    }

    private void testContinuousBatchedCommit0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        int nTotalRows = 50000;
        int nInitialStateRows = 150;
        long microsBetweenRows = 100000000;
        int maxBatchedRows = 10;
        int maxConcurrentBatches = 4;
        int nRowsPerCommit = 100;
        long o3MaxLag = microsBetweenRows * (nRowsPerCommit / 2);

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

        sql = "create table y as (select * from x where  i<=" + nInitialStateRows + " order by ts asc) timestamp(ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);
        LOG.info().$("committed initial state").$();

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=" + nInitialStateRows, sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        Rnd rnd = new Rnd();
        IntList batchRowEnd = new IntList((int) ((nTotalRows - nInitialStateRows) * 0.6 * maxBatchedRows));
        int atRow = nInitialStateRows;
        batchRowEnd.add(-atRow); // negative row means this has been committed
        while (atRow < nTotalRows) {
            int nRows = rnd.nextInt(maxBatchedRows) + 1;
            atRow += nRows;
            batchRowEnd.add(atRow);
        }

        int nCommitsWithLag = 0;
        try (TableWriter writer = getWriter(engine, "y")) {
            int nHeadBatch = 1;
            int nRowsAppended = 0;
            while (nHeadBatch < batchRowEnd.size()) {
                int nBatch = nHeadBatch + rnd.nextInt(maxConcurrentBatches);
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
                    writer.ic(o3MaxLag);
                    nCommitsWithLag++;
                }
            }
            writer.commit();
        }
        LOG.info().$("committed final state with ").$(nCommitsWithLag).$(" commits with lag").$();

        assertXY(compiler, sqlExecutionContext);
    }

    private void testFuzz0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, NumericException {
        testFuzz00(engine, compiler, sqlExecutionContext, TestUtils.generateRandom(LOG));
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

        sql = "create table y as (select * from x where i<=250 order by ts asc) timestamp(ts) partition by DAY WITH maxUncommittedRows=100, o3MaxLag=10s";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=250", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = getWriter(engine, "y")) {
            sql = "select * from x where i>250 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.ic();
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

        sql = "create table y as (select * from x where i<=250 order by ts asc) timestamp(ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=250", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = getWriter(engine, "y")) {
            sql = "select * from x where i>250 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.ic();
            TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from x", sink);
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

        sql = "create table y as (select * from x where i<=150 order by ts asc) timestamp(ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = getWriter(engine, "y")) {
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
        sql = "create table y as (select * from x where i<=150 order by ts asc) timestamp(ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = getWriter(engine, "y")) {
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

        sql = "create table y as (select * from x where i<=250 order by ts asc) timestamp(ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=250", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = getWriter(engine, "y")) {
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

    private void testO3MaxLagEndingAtPartitionBoundary0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
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
        sql = "create table y as (select * from x where i<=150 order by ts asc) timestamp(ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = getWriter(engine, "y")) {
            sql = "select * from x where i>150 and i<200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.ic();

            sql = "select * from x where i>=200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.ic();
            writer.commit();
        }

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void testO3MaxLagEndingAtPartitionBoundaryPlus10(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
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
        sql = "create table y as (select * from x where i<=150 order by ts asc) timestamp(ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = getWriter(engine, "y")) {
            sql = "select * from x where i>150 and i<200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.ic();
            sql = "select * from x where i>=200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.ic();
            writer.commit();
        }

        assertXY(compiler, sqlExecutionContext);
    }

    private void testO3MaxLagStaggeredPartitions0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
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
                " from long_sequence(150)" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        // i=184 is the last entry in date 1970-01-06
        sql = "create table y as (select * from x) timestamp(ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        // "x1" is overlapping "x", timestamp is offset by 1us to prevent nondeterministic
        // behaviour of ordering identical timestamp values
        sql = "create table x1 as (" +
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
                " timestamp_sequence(500000000000L + 120 * 100000000L + 1,100000000L) ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(50)" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        // "x2" us is overlapping "x1" timestamp is offset by 2us to prevent nondeterministic
        //        // behaviour of ordering identical timestamp values
        sql = "create table x2 as (" +
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
                " timestamp_sequence(500000000000L + 170 * 100000000L + 2,100000000L) ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(50)" +
                "), index(sym) timestamp (ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        try (TableWriter writer = getWriter(engine, "y")) {
            sql = "select * from x1 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.ic();
            Assert.assertTrue(writer.hasO3());

            sql = "select * from x2 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.ic();

            writer.commit();
        }

        TestUtils.printSql(compiler, sqlExecutionContext, "(select * from x union all x1 union all x2) order by ts", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        TestUtils.printSql(compiler, sqlExecutionContext, "select count() from (x union all x1 union all x2)", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select count() from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void testO3MaxLagWithInOrderBatchFollowedByO3Batch0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
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

        sql = "create table y as (select * from x where i<=250 order by ts asc) timestamp(ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=250", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = getWriter(engine, "y")) {
            sql = "select * from x where i>250 and i<300";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            sql = "select * from x where i>=300 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.ic();
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }

        assertXY(compiler, sqlExecutionContext);
    }

    private void testO3MaxLagWithLargeO3(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
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

        sql = "create table y as (select * from x where (i<=50 or i>=100) and i<=250 order by ts asc) timestamp(ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where (i<=50 or i>=100) and i<=250", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = getWriter(engine, "y")) {
            sql = "select * from x where i>250 or (i>50 and i<100) order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.ic();
            writer.commit();
        }

        assertXY(compiler, sqlExecutionContext);
    }

    private void testO3MaxLagWithinPartition(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
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

        sql = "create table y as (select * from x where i<=490 order by ts asc) timestamp(ts) partition by DAY";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=490", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = getWriter(engine, "y")) {
            sql = "select * from x where i>490 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.ic();
            writer.commit();
        }
        assertXY(compiler, sqlExecutionContext);
    }

    // This test rely on specific random seeds and will no longer test the intended scenario if fuzz generation is changed.
    // The scenario is to create records in partition '1970-01-02' and then commit with a lag so that the records are
    // moved to memory and only partition '1970-01-01' is committed to the disk.
    // If the partition '1970-01-02' is kept in the _txn partition tables then any rollback can wipe out records in '1970-01-01'.
    private void testRollbackCreatesEmptyPartition(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        final int nTotalRows = 74571;
        final long microsBetweenRows = 1161956;
        final double fraction = 0.8908162001320725;
        SharedRandom.RANDOM.get().reset(3417617481941903354L, 772640833522954936L);
        testRollbackFuzz0(engine, compiler, sqlExecutionContext, nTotalRows, (int) (nTotalRows * fraction), microsBetweenRows);
    }

    private void testRollbackFuzz(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final int nTotalRows = rnd.nextInt(79000);
        final long microsBetweenRows = rnd.nextLong(3090985);
        final double fraction = rnd.nextDouble();
        testRollbackFuzz0(engine, compiler, sqlExecutionContext, nTotalRows, (int) (nTotalRows * fraction), microsBetweenRows);
    }

    private void testRollbackFuzz0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            int nTotalRows,
            int lim,
            long microsBetweenRows
    ) throws SqlException {
        // table "x" is in order
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
        // table "z" is out of order - reshuffled "x"
        compiler.compile("create table z as (select * from x order by f)", sqlExecutionContext);
        // table "z" is our target table, where we exercise O3 and rollbacks
        compiler.compile("create table y as (select * from x where 1 <> 1) timestamp(ts) partition by day", sqlExecutionContext);

        try (TableWriter w = getWriter(engine, "y")) {

            insertUncommitted(compiler, sqlExecutionContext, "z limit " + lim, w);

            w.ic();

            final long o3Uncommitted = w.getO3RowCount();

            long expectedRowCount = w.size() - o3Uncommitted;

            w.rollback();

            Assert.assertEquals(expectedRowCount, w.size());

            TestUtils.assertEquals(
                    compiler,
                    sqlExecutionContext,
                    "(z limit " + lim + ") order by ts limit " + (lim - o3Uncommitted),
                    "y"
            );

            // insert remaining data (that we did not try to insert yet)
            insertUncommitted(compiler, sqlExecutionContext, "z limit " + lim + ", " + nTotalRows, w);
            w.ic();

            // insert data that we rolled back
            insertUncommitted(compiler, sqlExecutionContext, "(z limit " + lim + ") order by ts limit -" + o3Uncommitted, w);
            w.ic();

            w.commit();
        }
        assertXY(compiler, sqlExecutionContext);
    }

    private void testVarColumnMergeWithColumnTops(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            int initialCount,
            int additionalCount,
            int iteration,
            int maxUncommittedRows,
            Rnd rnd
    ) throws SqlException, NumericException {
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

        // We will not insert value 'aa' in str column. The count of rows with 'aa' is our invariant
        int aaCount = 2 * (initialCount + additionalCount);
        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "select count() from x where str = 'aa'", sink,
                "count\n" +
                        aaCount + "\n");
        compiler.compile("create table y as (select * from x where str = 'aa')", sqlExecutionContext);

        try (TableWriter tw = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), engine.getTableToken("x"), "test")) {
            int halfCount = appendCount / 2;
            appendRows(tw, halfCount, rnd);
            tw.ic(Timestamps.HOUR_MICROS);

            TestUtils.assertEquals(
                    compiler,
                    sqlExecutionContext,
                    "y",
                    "select * from x where str = 'aa'"
            );

            appendRows(tw, appendCount - halfCount, rnd);
            tw.ic(Timestamps.HOUR_MICROS);

            TestUtils.assertEquals(
                    compiler,
                    sqlExecutionContext,
                    "y",
                    "select * from x where str = 'aa'"
            );

            if (iteration % 2 == 0) {
                tw.commit();
            }
        }

        if (iteration % 3 == 0) {
            engine.releaseAllWriters();
        }

        TestUtils.assertEquals(
                compiler,
                sqlExecutionContext,
                "y",
                "select * from x where str = 'aa'"
        );
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

        compiler.compile("alter table x add column dummy string", sqlExecutionContext).execute(null).await();
        compiler.compile("alter table x drop column dummy", sqlExecutionContext).execute(null).await();
        compiler.compile("alter table x add column str2 string", sqlExecutionContext).execute(null).await();
        compiler.compile("alter table x add column y long", sqlExecutionContext).execute(null).await();

        if (iteration % 2 == 0) {
            engine.releaseAllWriters();
        }

        Rnd rnd = new Rnd();
        try (TableWriter tw = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), engine.getTableToken("x"), "test")) {
            int halfCount = appendCount / 2;
            appendRowsWithDroppedColumn(tw, halfCount, rnd);
            tw.ic(Timestamps.HOUR_MICROS);

            TestUtils.assertSql(compiler, sqlExecutionContext, "select * from x where str = 'aa'", sink,
                    "str\tts\tx\tstr2\ty\n" +
                            "aa\t1970-01-01T11:00:00.000000Z\t1\t\tNaN\n" +
                            "aa\t1970-01-02T00:00:00.000000Z\t1\t\tNaN\n");

            appendRowsWithDroppedColumn(tw, appendCount - halfCount, rnd);
            tw.ic(Timestamps.HOUR_MICROS);

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
}
