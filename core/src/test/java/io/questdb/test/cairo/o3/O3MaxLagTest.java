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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriter.Row;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static io.questdb.test.AbstractCairoTest.replaceTimestampSuffix;

@RunWith(Parameterized.class)
public class O3MaxLagTest extends AbstractO3Test {
    private final static Log LOG = LogFactory.getLog(O3MaxLagTest.class);
    private static final Utf8StringSink utf8Sink = new Utf8StringSink();

    public O3MaxLagTest(TestTimestampType timestampType) {
        super(timestampType);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testBigUncommittedCheckStrColFixedAndVarMappedSizes() throws Exception {
        executeWithPool(0, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName) -> {
            TableModel tableModel = new TableModel(engine.getConfiguration(), "table", PartitionBy.DAY);
            tableModel
                    .col("id", ColumnType.STRING)
                    .timestamp("ts", timestampType.getTimestampType());
            testBigUncommittedMove1(engine, compiler, sqlExecutionContext, tableModel);
        });
    }

    @Test
    public void testBigUncommittedMovesTimestampOnEdge() throws Exception {
        executeWithPool(0, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName) -> {
            TableModel tableModel = new TableModel(engine.getConfiguration(), "table", PartitionBy.DAY);
            tableModel
                    .col("id", ColumnType.LONG)
                    .timestamp("ts", timestampType.getTimestampType());
            testBigUncommittedMove1(engine, compiler, sqlExecutionContext, tableModel);
        });
    }

    @Test
    public void testBigUncommittedToMove() throws Exception {
        executeWithPool(0, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName) -> {
            TableModel tableModel = new TableModel(engine.getConfiguration(), "table", PartitionBy.DAY);
            tableModel
                    .col("id", ColumnType.LONG)
                    .col("ok", ColumnType.FLOAT)
                    .col("str", ColumnType.STRING)
                    .timestamp("ts", timestampType.getTimestampType());
            testBigUncommittedMove1(engine, compiler, sqlExecutionContext, tableModel);
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
    public void testRowCountWhenLagIsNextDay() throws Exception {
        executeWithPool(0, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName) -> {
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
                engine.execute("create table " + tableName + "( " +
                        "ts " + timestampTypeName +
                        ") timestamp(ts) partition by DAY " +
                        " WITH maxUncommittedRows=1, o3MaxLag=120s", sqlExecutionContext);

                try (TableWriter writer = TestUtils.getWriter(engine, tableName)) {
                    int effectiveMaxUncommitted;
                    try (TableMetadata tableMetadata = engine.getTableMetadata(engine.verifyTableName(tableName))) {
                        effectiveMaxUncommitted = tableMetadata.getMaxUncommittedRows();
                    }

                    for (int i = 0; i < length; i++) {
                        long ts = timestampType.getDriver().parseFloorLiteral(dates[i]);
                        Row r = writer.newRow(ts);
                        r.append();

                        Assert.assertEquals(i + 1, writer.size());
                        if (effectiveMaxUncommitted < writer.getUncommittedRowCount()) {
                            writer.ic();
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
        executeWithPool(
                0,
                (
                        CairoEngine engine,
                        SqlCompiler compiler,
                        SqlExecutionContext sqlExecutionContext,
                        String timestampTypeName
                ) -> {
                    int longsPerPage = dataAppendPageSize / 8;
                    int hi = (longsPerPage + 8) * 2;
                    int lo = (longsPerPage - 8) * 2;
                    int maxUncommitted = TestUtils.generateRandom(null).nextInt(hi);

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
                                        new Rnd()
                                );
                                engine.execute("drop table x", sqlExecutionContext);
                                engine.execute("drop table y", sqlExecutionContext);
                            }
                        }
                    }
                }
        );
    }

    @Test
    public void testVarColumnPageBoundariesAppend() throws Exception {
        dataAppendPageSize = (int) Files.PAGE_SIZE;
        executeWithPool(
                0,
                (
                        CairoEngine engine,
                        SqlCompiler compiler,
                        SqlExecutionContext sqlExecutionContext,
                        String timestampTypeName
                ) -> {
                    int longsPerPage = dataAppendPageSize / 8;
                    int hi = (longsPerPage + 8) * 2;
                    int lo = (longsPerPage - 8) * 2;
                    for (int i = lo; i < hi; i++) {
                        LOG.info().$("=========== iteration ").$(i).$(" ===================").$();
                        testVarColumnPageBoundaryIterationWithColumnTop(engine, compiler, sqlExecutionContext, i, 1000);
                        engine.execute("drop table x", sqlExecutionContext);
                    }
                }
        );
    }

    @Test
    public void testVarColumnPageBoundariesAppendRndPageSize() throws Exception {
        int rndPagesMultiplier = TestUtils.generateRandom(null).nextInt(129);
        int multiplier = Numbers.ceilPow2(rndPagesMultiplier);

        dataAppendPageSize = (int) Files.PAGE_SIZE * multiplier;
        LOG.info().$("Testing with random pages size of ").$(dataAppendPageSize).$();

        executeWithPool(
                0,
                (
                        CairoEngine engine,
                        SqlCompiler compiler,
                        SqlExecutionContext sqlExecutionContext,
                        String timestampTypeName
                ) -> {
                    int longsPerPage = dataAppendPageSize / 8;
                    int hi = (longsPerPage + 8) * 2;
                    int lo = (longsPerPage - 8) * 2;
                    for (int i = lo; i < hi; i++) {
                        LOG.info().$("=========== iteration ").$(i).$(" ===================").$();
                        testVarColumnPageBoundaryIterationWithColumnTop(engine, compiler, sqlExecutionContext, i, 1000);
                        engine.execute("drop table x", sqlExecutionContext);
                    }
                }
        );
    }

    @Test
    public void testVarColumnPageBoundariesFail() throws Exception {
        dataAppendPageSize = (int) Files.PAGE_SIZE;
        executeWithPool(
                0,
                (
                        CairoEngine engine,
                        SqlCompiler compiler,
                        SqlExecutionContext sqlExecutionContext,
                        String timestampTypeName
                ) -> {
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
                    engine.execute("drop table x", sqlExecutionContext);
                    engine.execute("drop table y", sqlExecutionContext);
                }
        );
    }

    @Test
    public void testVarColumnPageBoundariesInO3Memory() throws Exception {
        dataAppendPageSize = (int) Files.PAGE_SIZE;
        executeWithPool(
                0,
                (
                        CairoEngine engine,
                        SqlCompiler compiler,
                        SqlExecutionContext sqlExecutionContext,
                        String timestampTypeName
                ) -> {
                    int longsPerPage = dataAppendPageSize / 8;
                    int hi = (longsPerPage + 8) * 2;
                    int lo = (longsPerPage - 8) * 2;
                    for (int i = lo; i < hi; i++) {
                        LOG.info().$("=========== iteration ").$(i).$(", max uncommitted ").$(longsPerPage).$(" ===================").$();
                        testVarColumnPageBoundaryIterationWithColumnTop(engine, compiler, sqlExecutionContext, i, longsPerPage);
                        engine.execute("drop table x", sqlExecutionContext);
                    }
                }
        );
    }

    @Test
    public void testVarColumnPageBoundariesRndMaxUncommitted() throws Exception {
        dataAppendPageSize = (int) Files.PAGE_SIZE;
        executeWithPool(
                0,
                (
                        CairoEngine engine,
                        SqlCompiler compiler,
                        SqlExecutionContext sqlExecutionContext,
                        String timestampTypeName
                ) -> {
                    int longsPerPage = dataAppendPageSize / 8;
                    int hi = (longsPerPage + 8) * 2;
                    int lo = (longsPerPage - 8) * 2;
                    int maxUncommitted = TestUtils.generateRandom(null).nextInt(hi);
                    for (int i = lo; i < hi; i++) {
                        LOG.info().$("=========== iteration ").$(i).$(", max uncommitted ").$(maxUncommitted).$(" ===================").$();
                        testVarColumnPageBoundaryIterationWithColumnTop(engine, compiler, sqlExecutionContext, i, maxUncommitted);
                        engine.execute("drop table x", sqlExecutionContext);
                    }
                }
        );
    }

    private void appendRows(TableWriter tw, int count, Rnd rnd) throws NumericException {
        TableMetadata metadata = tw.getMetadata();
        TimestampDriver driver = timestampType.getDriver();
        for (int i = 0; i < count; i++) {
            long timestamp = driver.parseFloorLiteral("1970-01-01T11:00:00.000000Z") + rnd.nextLong(driver.fromDays(1));
            Row row = tw.newRow(timestamp);

            putVariantStr(metadata, row, 0, "cc");
            row.putLong(2, 11111L);
            putVariantStr(metadata, row, 3, "dd");
            row.putLong(4, 22222L);
            row.append();

            row = tw.newRow(driver.parseFloorLiteral("1970-01-02T11:00:00.000000Z"));
            putVariantStr(metadata, row, 0, "cc");
            row.putLong(2, 333333L);
            putVariantStr(metadata, row, 3, "dd");
            row.putLong(4, 444444L);
            row.append();
        }
    }

    private void appendRowsWithDroppedColumn(TableWriter tw, int count, Rnd rnd) throws NumericException {
        TableMetadata metadata = tw.getMetadata();
        TimestampDriver driver = timestampType.getDriver();
        for (int i = 0; i < count; i++) {
            long timestamp = driver.parseFloorLiteral("1970-01-01") + rnd.nextLong(driver.fromDays(1));
            Row row = tw.newRow(timestamp);

            putVariantStr(metadata, row, 0, "cc");
            row.putLong(2, 11111L);
            putVariantStr(metadata, row, 4, "dd");
            row.putLong(5, 22222L);
            row.append();

            row = tw.newRow(driver.parseFloorLiteral("1970-01-02T10:00:01.000000Z"));
            putVariantStr(metadata, row, 0, "cc");
            row.putLong(2, 333333L);
            putVariantStr(metadata, row, 4, "dd");
            row.putLong(5, 444444L);
            row.append();
        }
    }

    private void putVariantStr(RecordMetadata metadata, Row row, int col, String value) {
        switch (metadata.getColumnType(col)) {
            case ColumnType.STRING:
                row.putStr(col, value);
                break;
            case ColumnType.VARCHAR:
                utf8Sink.clear();
                utf8Sink.put(value);
                row.putVarchar(col, utf8Sink);
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    private void testBigUncommittedMove1(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            TableModel tableModel
    ) throws NumericException, SqlException {
        // Create empty tables of same structure
        TestUtils.createPopulateTable(
                "o3",
                compiler,
                sqlExecutionContext,
                tableModel,
                0,
                "2021-04-27",
                0
        );

        TestUtils.createPopulateTable(
                "ordered",
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

        long start = MicrosTimestampDriver.floor("2021-04-27T08:00:00");
        long[] testCounts = new long[]{2 * 1024 * 1024, 16 * 8 * 1024 * 5, 2_000_000};
        //noinspection ForLoopReplaceableByForEach
        for (int c = 0; c < testCounts.length; c++) {
            long idCount = testCounts[c];

            // Create big commit with a big part before OOO starts
            // which exceeds default MAMemoryImpl size in one or all columns
            int iterations = 2;
            String[] varCol = new String[]{"abc", "aldfjkasdlfkj", "as", "2021-04-27T12:00:00", "12345678901234578"};

            // Add 2 batches
            try (TableWriter o3 = TestUtils.getWriter(engine, "o3")) {
                try (TableWriter ordered = TestUtils.getWriter(engine, "ordered")) {
                    TableMetadata metadata = o3.getMetadata();
                    for (int i = 0; i < iterations; i++) {
                        long backwards = iterations - i - 1;
                        final Rnd rnd = new Rnd();
                        for (int id = 0; id < idCount; id++) {
                            long timestamp = start + backwards * idCount + id;
                            Row row = o3.newRow(timestamp);
                            if (longColIndex > -1) {
                                row.putLong(longColIndex, timestamp);
                            }
                            float rndFloat = rnd.nextFloat();
                            if (floatColIndex > -1) {
                                row.putFloat(floatColIndex, rndFloat);
                            }
                            if (strColIndex > -1) {
                                putVariantStr(metadata, row, strColIndex, varCol[id % varCol.length]);
                            }
                            row.append();

                            timestamp = start + i * idCount + id;
                            row = ordered.newRow(timestamp);
                            if (longColIndex > -1) {
                                row.putLong(longColIndex, timestamp);
                            }
                            if (floatColIndex > -1) {
                                row.putFloat(floatColIndex, rndFloat);
                            }
                            if (strColIndex > -1) {
                                putVariantStr(metadata, row, strColIndex, varCol[id % varCol.length]);
                            }
                            row.append();
                        }
                    }

                    o3.commit();
                    ordered.commit();
                }
            }
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, "ordered", "o3", LOG);
            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, "ordered", "o3", LOG);

            engine.releaseAllReaders();
            try (TableWriter o3 = TestUtils.getWriter(engine, "o3")) {
                try (TableWriter ordered = TestUtils.getWriter(engine, "ordered")) {
                    o3.truncate();
                    ordered.truncate();
                }
            }
        }
    }

    private void testContinuousBatchedCommit0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName) throws SqlException {
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
                " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(0L," + microsBetweenRows + "L)::" + timestampTypeName + " ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(" + nTotalRows + ")" +
                "), index(sym) timestamp (ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        sql = "create table y as (select * from x where  i<=" + nInitialStateRows + " order by ts asc) timestamp(ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);
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
        try (TableWriter writer = TestUtils.getWriter(engine, "y")) {
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

    private void testLargeLagWithRowLimit(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + " ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        sql = "create table y as (select * from x where i<=250 order by ts asc) timestamp(ts) partition by DAY WITH maxUncommittedRows=100, o3MaxLag=10s";
        engine.execute(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=250", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = TestUtils.getWriter(engine, "y")) {
            sql = "select * from x where i>250 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.ic();
            writer.commit();
        }
        assertXY(compiler, sqlExecutionContext);
    }

    private void testLargeLagWithinPartition(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + " ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        sql = "create table y as (select * from x where i<=250 order by ts asc) timestamp(ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=250", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = TestUtils.getWriter(engine, "y")) {
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
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + " ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        sql = "create table y as (select * from x where i<=150 order by ts asc) timestamp(ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = TestUtils.getWriter(engine, "y")) {
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

    private void testNoLagEndingAtPartitionBoundary(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + " ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        // i=184 is the last entry in date 1970-01-06
        sql = "create table y as (select * from x where i<=150 order by ts asc) timestamp(ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = TestUtils.getWriter(engine, "y")) {
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

    private void testNoLagWithRollback(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000::" + timestampTypeName + " timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + " ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        sql = "create table y as (select * from x where i<=250 order by ts asc) timestamp(ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=250", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = TestUtils.getWriter(engine, "y")) {
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

    private void testO3MaxLagEndingAtPartitionBoundary0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000::" + timestampTypeName + " timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + " ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        // i=184 is the last entry in date 1970-01-06
        sql = "create table y as (select * from x where i<=150 order by ts asc) timestamp(ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = TestUtils.getWriter(engine, "y")) {
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

    private void testO3MaxLagEndingAtPartitionBoundaryPlus10(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + " ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        // i=184 is the last entry in date 1970-01-06
        sql = "create table y as (select * from x where i<=150 order by ts asc) timestamp(ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = TestUtils.getWriter(engine, "y")) {
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

    private void testO3MaxLagStaggeredPartitions0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + " ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(150)" +
                "), index(sym) timestamp (ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        // i=184 is the last entry in date 1970-01-06
        sql = "create table y as (select * from x) timestamp(ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

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
                " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L + 120 * 100000000L + 1,100000000L)::" + timestampTypeName + " ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(50)" +
                "), index(sym) timestamp (ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        // "x2" us is overlapping "x1" timestamp is offset by 2us to prevent nondeterministic
        //        // behaviour of ordering identical timestamp values
        sql = "create table x2 as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L + 170 * 100000000L + 2,100000000L)::" + timestampTypeName + " ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(50)" +
                "), index(sym) timestamp (ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        try (TableWriter writer = TestUtils.getWriter(engine, "y")) {
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

    private void testO3MaxLagWithInOrderBatchFollowedByO3Batch0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + " ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        sql = "create table y as (select * from x where i<=250 order by ts asc) timestamp(ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=250", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = TestUtils.getWriter(engine, "y")) {
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

    private void testO3MaxLagWithLargeO3(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + " ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        sql = "create table y as (select * from x where (i<=50 or i>=100) and i<=250 order by ts asc) timestamp(ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where (i<=50 or i>=100) and i<=250", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = TestUtils.getWriter(engine, "y")) {
            sql = "select * from x where i>250 or (i>50 and i<100) order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.ic();
            writer.commit();
        }

        assertXY(compiler, sqlExecutionContext);
    }

    private void testO3MaxLagWithinPartition(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName) throws SqlException {
        String sql = "create table x as (" +
                "select" +
                " cast(x as int) i," +
                " rnd_symbol('msft','ibm', 'googl') sym," +
                " round(rnd_double(0)*100, 3) amt," +
                " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) ik," +
                " rnd_long() j," +
                " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + " ts," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() t" +
                " from long_sequence(500)" +
                "), index(sym) timestamp (ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        sql = "create table y as (select * from x where i<=490 order by ts asc) timestamp(ts) partition by DAY";
        engine.execute(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=490", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = TestUtils.getWriter(engine, "y")) {
            sql = "select * from x where i>490 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            writer.ic();
            writer.commit();
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
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 'aa' as str," +
                        " timestamp_sequence('1970-01-01T11:00:00',1000L)::" + timestampType.getTypeName() + " ts," +
                        " x " +
                        " from long_sequence(" + initialCount + ")" +
                        ") timestamp (ts) partition by DAY with maxUncommittedRows = " + maxUncommittedRows,
                sqlExecutionContext
        );

        sqlExecutionContext.getCairoEngine().execute("alter table x add column str2 string", sqlExecutionContext);
        sqlExecutionContext.getCairoEngine().execute("alter table x add column y long", sqlExecutionContext);
        CairoEngine cairoEngine2 = sqlExecutionContext.getCairoEngine();
        cairoEngine2.execute(
                "insert into x " +
                        "select" +
                        " 'aa' as str," +
                        " timestamp_sequence('1970-01-01T12:00:00',1000L) ts," +
                        " x," +
                        " '77' as str2," +
                        " x * 1000 as y " +
                        " from long_sequence(" + additionalCount + ")", sqlExecutionContext
        );

        // Day 2 '1970-01-02'
        CairoEngine cairoEngine1 = sqlExecutionContext.getCairoEngine();
        cairoEngine1.execute(
                "insert into x " +
                        "select" +
                        " 'aa' as str," +
                        " timestamp_sequence('1970-01-02T00:00:00',1000L) ts," +
                        " x, " +
                        " '11' as str2," +
                        " x * 1000 as y " +
                        " from long_sequence(" + initialCount + ")", sqlExecutionContext
        );

        CairoEngine cairoEngine = sqlExecutionContext.getCairoEngine();
        cairoEngine.execute(
                "insert into x " +
                        "select" +
                        " 'aa' as str," +
                        " timestamp_sequence('1970-01-02T01:00:00',1000L) ts," +
                        " x, " +
                        " '33' as str2," +
                        " x * 222 as y " +
                        " from long_sequence(" + additionalCount + ")", sqlExecutionContext
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
                        aaCount + "\n"
        );
        engine.execute("create table y as (select * from x where str = 'aa')", sqlExecutionContext);

        try (TableWriter tw = TestUtils.getWriter(engine, "x")) {
            int halfCount = appendCount / 2;
            appendRows(tw, halfCount, rnd);
            tw.ic(MicrosTimestampDriver.INSTANCE.fromHours(1));

            TestUtils.assertEquals(
                    compiler,
                    sqlExecutionContext,
                    "y",
                    "select * from x where str = 'aa'"
            );

            appendRows(tw, appendCount - halfCount, rnd);
            tw.ic(MicrosTimestampDriver.INSTANCE.fromHours(1));

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
        sqlExecutionContext.getCairoEngine().execute(
                "create table x as (" +
                        "select" +
                        " 'aa' as str," +
                        " timestamp_sequence('1970-01-01T11:00:00',1000L)::" + timestampType.getTypeName() + " ts," +
                        " x " +
                        " from long_sequence(1)" +
                        ") timestamp (ts) partition by DAY with maxUncommittedRows = " + maxUncommittedRows,
                sqlExecutionContext
        );

        // Day 2 '1970-01-02'
        engine.execute(
                "insert into x " +
                        "select" +
                        " 'aa' as str," +
                        " timestamp_sequence('1970-01-02T00:00:00',1000L) ts," +
                        " x " +
                        " from long_sequence(1)", sqlExecutionContext
        );

        engine.execute("alter table x add column dummy string", sqlExecutionContext);
        engine.execute("alter table x drop column dummy", sqlExecutionContext);
        engine.execute("alter table x add column str2 string", sqlExecutionContext);
        engine.execute("alter table x add column y long", sqlExecutionContext);

        if (iteration % 2 == 0) {
            engine.releaseAllWriters();
        }

        Rnd rnd = new Rnd();
        try (TableWriter tw = TestUtils.getWriter(engine, "x")) {
            int halfCount = appendCount / 2;
            appendRowsWithDroppedColumn(tw, halfCount, rnd);
            tw.ic(MicrosTimestampDriver.INSTANCE.fromHours(1));

            TestUtils.assertSql(compiler, sqlExecutionContext, "select * from x where str = 'aa'", sink,
                    replaceTimestampSuffix("str\tts\tx\tstr2\ty\n" +
                            "aa\t1970-01-01T11:00:00.000000Z\t1\t\tnull\n" +
                            "aa\t1970-01-02T00:00:00.000000Z\t1\t\tnull\n", timestampType.getTypeName())
            );

            appendRowsWithDroppedColumn(tw, appendCount - halfCount, rnd);
            tw.ic(MicrosTimestampDriver.INSTANCE.fromHours(1));

            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "select * from x where str = 'aa'", sink,
                    replaceTimestampSuffix("str\tts\tx\tstr2\ty\n" +
                            "aa\t1970-01-01T11:00:00.000000Z\t1\t\tnull\n" +
                            "aa\t1970-01-02T00:00:00.000000Z\t1\t\tnull\n", timestampType.getTypeName())
            );

            if (iteration % 2 == 0) {
                tw.commit();
            }
        }

        if (iteration % 3 == 0) {
            engine.releaseAllWriters();
        }

        TestUtils.assertSql(compiler, sqlExecutionContext, "select * from x where str = 'aa'", sink,
                replaceTimestampSuffix("str\tts\tx\tstr2\ty\n" +
                        "aa\t1970-01-01T11:00:00.000000Z\t1\t\tnull\n" +
                        "aa\t1970-01-02T00:00:00.000000Z\t1\t\tnull\n", timestampType.getTypeName())
        );
    }
}
