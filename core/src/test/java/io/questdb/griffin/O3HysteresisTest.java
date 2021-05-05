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
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class O3HysteresisTest extends AbstractO3Test {
    private final static Log LOG = LogFactory.getLog(O3HysteresisTest.class);
    private long minTimestamp;
    private long maxTimestamp;
    private RecordToRowCopier copier;

    @Before
    public void clearRecordToRowCopier() {
        copier = null;
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
    public void testHysteresisEndingAtPartitionBoundaryContended() throws Exception {
        executeWithPool(0, this::testHysteresisEndingAtPartitionBoundary0);
    }

    @Test
    public void testHysteresisEndingAtPartitionBoundaryParallel() throws Exception {
        executeWithPool(2, this::testHysteresisEndingAtPartitionBoundary0);
    }

    @Test
    public void testHysteresisEndingAtPartitionBoundaryPlus1Contended() throws Exception {
        executeWithPool(0, this::testHysteresisEndingAtPartitionBoundaryPlus10);
    }

    @Test
    public void testHysteresisEndingAtPartitionBoundaryPlus1Parallel() throws Exception {
        executeWithPool(2, this::testHysteresisEndingAtPartitionBoundaryPlus10);
    }

    @Test
    public void testHysteresisEndingAtPartitionBoundaryPlus1WithRollbackContended() throws Exception {
        executeWithPool(0, this::testHysteresisEndingAtPartitionBoundaryPlus1WithRollback0);
    }

    @Test
    public void testHysteresisEndingAtPartitionBoundaryPlus1WithRollbackParallel() throws Exception {
        executeWithPool(2, this::testHysteresisEndingAtPartitionBoundaryPlus1WithRollback0);
    }

    @Test
    public void testHysteresisEndingAtPartitionBoundaryWithRollbackContended() throws Exception {
        executeWithPool(0, this::testHysteresisEndingAtPartitionBoundaryWithRollback0);
    }

    @Test
    public void testHysteresisEndingAtPartitionBoundaryWithRollbackParallel() throws Exception {
        executeWithPool(2, this::testHysteresisEndingAtPartitionBoundaryWithRollback0);
    }

    @Test
    public void testHysteresisStaggeringPartitionsContended() throws Exception {
        executeWithPool(0, this::testHysteresisStaggeringPartitions0);
    }

    @Test
    public void testHysteresisStaggeringPartitionsParallel() throws Exception {
        executeWithPool(2, this::testHysteresisStaggeringPartitions0);
    }

    @Test
    public void testHysteresisStaggeringPartitionsWithRollbackContended() throws Exception {
        executeWithPool(0, this::testHysteresisStaggeringPartitionsWithRollback0);
    }

    @Test
    public void testHysteresisStaggeringPartitionsWithRollbackParallel() throws Exception {
        executeWithPool(2, this::testHysteresisStaggeringPartitionsWithRollback0);
    }

    @Test
    public void testHysteresisWithInOrderBatchFollowedByO3BatchContended() throws Exception {
        executeWithPool(0, this::testHysteresisWithInOrderBatchFollowedByO3Batch0);
    }

    @Test
    public void testHysteresisWithInOrderBatchFollowedByO3BatchParallel() throws Exception {
        executeWithPool(2, this::testHysteresisWithInOrderBatchFollowedByO3Batch0);
    }

    @Test
    public void testHysteresisWithLargeO3Contended() throws Exception {
        executeWithPool(0, this::testHysteresisWithLargeO3);
    }

    @Test
    public void testHysteresisWithLargeO3Parallel() throws Exception {
        executeWithPool(2, this::testHysteresisWithLargeO3);
    }

    @Test
    public void testHysteresisWithinPartitionContended() throws Exception {
        executeWithPool(0, this::testHysteresisWithinPartition);
    }

    @Test
    public void testHysteresisWithinPartitionParallel() throws Exception {
        executeWithPool(2, this::testHysteresisWithinPartition);
    }

    @Test
    public void testHysteresisWithinPartitionWithRollbackContended() throws Exception {
        executeWithPool(0, this::testHysteresisWithinPartitionWithRollback);
    }

    @Test
    public void testHysteresisWithinPartitionWithRollbackParallel() throws Exception {
        executeWithPool(2, this::testHysteresisWithinPartitionWithRollback);
    }

    @Test
    public void testLargeHysteresisWithinPartitionContended() throws Exception {
        executeWithPool(0, this::testLargeHysteresisWithinPartition);
    }

    @Test
    public void testLargeHysteresisWithRowLimitContended() throws Exception {
        executeWithPool(0, this::testLargeHysteresisWithRowLimit);
    }

    @Test
    public void testLargeHysteresisWithinPartitionParallel() throws Exception {
        executeWithPool(2, this::testLargeHysteresisWithinPartition);
    }

    @Test
    public void testNoHysteresisContended() throws Exception {
        executeWithPool(0, this::testNoHysteresis0);
    }

    @Test
    public void testNoHysteresisEndingAtPartitionBoundaryContended() throws Exception {
        executeWithPool(0, this::testNoHysteresisEndingAtPartitionBoundary);
    }

    @Test
    public void testNoHysteresisEndingAtPartitionBoundaryParallel() throws Exception {
        executeWithPool(2, this::testNoHysteresisEndingAtPartitionBoundary);
    }

    @Test
    public void testNoHysteresisParallel() throws Exception {
        executeWithPool(2, this::testNoHysteresis0);
    }

    @Test
    public void testNoHysteresisWithRollbackContended() throws Exception {
        executeWithPool(0, this::testNoHysteresisWithRollback);
    }

    @Test
    public void testNoHysteresisWithRollbackParallel() throws Exception {
        executeWithPool(2, this::testNoHysteresisWithRollback);
    }

    @Test
    public void testBigUncommittedToMove() throws Exception {
        executeWithPool(0, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
            try(TableModel tableModel = new TableModel(engine.getConfiguration(), "table", PartitionBy.DAY)) {
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
    public void testBigUncommittedMovesTimestampOnEdge() throws Exception {
        executeWithPool(0, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
            try(TableModel tableModel = new TableModel(engine.getConfiguration(), "table", PartitionBy.DAY)) {
                tableModel
                        .col("id", ColumnType.LONG)
                        .timestamp("ts");
                testBigUncommittedMove1(engine, compiler, sqlExecutionContext, tableModel);
            }
        });
    }

    @Test
    public void testBigUncommittedCheckStrColFixedAndVarMappedSizes() throws Exception {
        executeWithPool(0, (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) -> {
            try(TableModel tableModel = new TableModel(engine.getConfiguration(), "table", PartitionBy.DAY)) {
                tableModel
                        .col("id", ColumnType.STRING)
                        .timestamp("ts");
                testBigUncommittedMove1(engine, compiler, sqlExecutionContext, tableModel);
            }
        });
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
        int flotColIndex = -1;
        int strColIndex = -1;
        for(int i = 0; i < tableModel.getColumnCount(); i++) {
            switch (tableModel.getColumnType(i)) {
                case ColumnType.LONG:
                    longColIndex = i;
                    break;
                case ColumnType.FLOAT:
                    flotColIndex = i;
                    break;
                case ColumnType.STRING:
                    strColIndex = i;
                    break;
            }
        }

        long start = IntervalUtils.parseFloorPartialDate("2021-04-27T08:00:00");
        for (int mils = 2; mils < 6; mils +=2) {
            long idCount = mils * 1_000_000L;

            // Create big commit with has big part before OOO starts
            // which exceed default AppendOnlyVirtualMemory size in one or all columns
            int iterations = 2;
            String[] varCol = new String[]{"abc", "aldfjkasdlfkj", "as", "2021-04-27T12:00:00"};

            // Add 2 batches
            try (TableWriter o3 = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "o3");
                 TableWriter ordered = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "ordered")) {
                for (int i = 0; i < iterations; i++) {
                    long backwards = iterations - i - 1;
                    final Rnd rnd = new Rnd();
                    for (int id = 0; id < idCount; id++) {
                        long timestamp = start + backwards * idCount + id;
                        Row row = o3.newRow(timestamp);
                        if (longColIndex > -1) {
                            row.putLong(longColIndex, timestamp);
                        }
                        if (flotColIndex > -1) {
                            row.putFloat(flotColIndex, rnd.nextFloat());
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
                        if (flotColIndex > -1) {
                            row.putFloat(flotColIndex, rnd.nextFloat());
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

            assertSqlCursors(compiler, sqlExecutionContext, "ordered", "o3", LOG);
            start += idCount * iterations;
        }
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

    private void testContinuousBatchedCommit0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        int nTotalRows = 50000;
        int nInitialStateRows = 150;
        long microsBetweenRows = 100000000;
        int maxBatchedRows = 10;
        int maxConcurrentBatches = 4;
        int nRowsPerCommit = 100;
        long lastTimestampHysteresisInMicros = microsBetweenRows * (nRowsPerCommit / 2);

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

        int nCommitsWithHysteresis = 0;
        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
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
                    LOG.info().$("committing with hysteresis").$();
                    nRowsAppended = 0;
                    writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                    nCommitsWithHysteresis++;
                }
            }
            writer.commit();
        }
        LOG.info().$("committed final state with ").$(nCommitsWithHysteresis).$(" commits with hysteresis").$();

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink2, sink);
    }

    private void testHysteresisEndingAtPartitionBoundary0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException, NumericException {
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

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
            sql = "select * from x where i>150 and i<200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long lastTimestampHysteresisInMicros = maxTimestamp - TimestampFormatUtils.parseTimestamp("1970-01-06T23:59:59.000Z");
            maxTimestamp -= lastTimestampHysteresisInMicros;
            writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
            TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            sql = "select * from x where i>=200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= lastTimestampHysteresisInMicros;
            writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void testHysteresisEndingAtPartitionBoundaryPlus10(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException, NumericException {
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

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
            sql = "select * from x where i>150 and i<200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long lastTimestampHysteresisInMicros = maxTimestamp - TimestampFormatUtils.parseTimestamp("1970-01-07T00:00:00.000Z");
            maxTimestamp -= lastTimestampHysteresisInMicros;
            writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            sql = "select * from x where i>=200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= lastTimestampHysteresisInMicros;
            writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void testHysteresisEndingAtPartitionBoundaryPlus1WithRollback0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException, NumericException {
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

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
            sql = "select * from x where i>150 and i<200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long lastTimestampHysteresisInMicros = maxTimestamp - TimestampFormatUtils.parseTimestamp("1970-01-07T00:00:00.000Z");
            minTimestamp = maxTimestamp - lastTimestampHysteresisInMicros;
            writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<='1970-01-07T00:00:00.000Z'", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.rollback();
            TestUtils.printSql(compiler, sqlExecutionContext, "select count(*) from y", sink);
            TestUtils.assertEquals("count\n185\n", sink);

            sql = "select * from x where i>=200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= lastTimestampHysteresisInMicros;
            writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp) and (i<=185 or i>=200)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=185 or i>=200", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void testHysteresisEndingAtPartitionBoundaryWithRollback0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException, NumericException {
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

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
            sql = "select * from x where i>150 and i<200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long lastTimestampHysteresisInMicros = maxTimestamp - TimestampFormatUtils.parseTimestamp("1970-01-06T23:59:59.000Z");
            minTimestamp = maxTimestamp - lastTimestampHysteresisInMicros;
            writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
            TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from x where ts<='1970-01-06T23:59:59.000Z'", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.rollback();
            TestUtils.printSql(compiler, sqlExecutionContext, "select count(*) from y", sink);
            TestUtils.assertEquals("count\n184\n", sink);

            sql = "select * from x where i>=200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= lastTimestampHysteresisInMicros;
            writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp) and (i<=184 or i>=200)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=184 or i>=200", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void testHysteresisStaggeringPartitions0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
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

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
            sql = "select * from x where i>150 and i<200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= lastTimestampHysteresisInMicros;
            writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            sql = "select * from x where i>=200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= lastTimestampHysteresisInMicros;
            writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void testHysteresisStaggeringPartitionsWithRollback0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
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

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
            sql = "select * from x where i>150 and i<200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= lastTimestampHysteresisInMicros;
            writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.rollback();
            TestUtils.printSql(compiler, sqlExecutionContext, "select count(*) from y", sink);
            TestUtils.assertEquals("count\n175\n", sink);

            sql = "select * from x where i>=200 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= lastTimestampHysteresisInMicros;
            writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp) and (i<=175 or i>=200)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=175 or i>=200", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void testHysteresisWithInOrderBatchFollowedByO3Batch0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
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

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
            sql = "select * from x where i>250 and i<300";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            sql = "select * from x where i>=300 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= lastTimestampHysteresisInMicros;
            writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink2, sink);
    }

    private void testHysteresisWithLargeO3(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
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

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
            sql = "select * from x where i>250 or (i>50 and i<100) order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= lastTimestampHysteresisInMicros;
            writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void testHysteresisWithinPartition(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
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

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
            sql = "select * from x where i>490 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= lastTimestampHysteresisInMicros;
            writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void testHysteresisWithinPartitionWithRollback(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
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

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
            sql = "select * from x where i>250 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
            maxTimestamp -= lastTimestampHysteresisInMicros;
            writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
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
    }

    private void testLargeHysteresisWithinPartition(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
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

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
            sql = "select * from x where i>250 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) * 3 / 4;
            maxTimestamp -= lastTimestampHysteresisInMicros;
            writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
            TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from x where ts<=cast(" + maxTimestamp + " as timestamp)", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            writer.commit();
        }

        TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from x", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void testLargeHysteresisWithRowLimit(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
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

        sql = "create table y as (select * from x where i<=250) partition by DAY WITH o3MaxUncommittedRows=100, o3CommitHysteresis=10s";
        compiler.compile(sql, sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=250", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
            sql = "select * from x where i>250 order by f";
            insertUncommitted(compiler, sqlExecutionContext, sql, writer);
            long lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) * 3 / 4;
            maxTimestamp -= lastTimestampHysteresisInMicros;
            writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
            TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from x limit 400", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from y", sink2);
            TestUtils.assertEquals(sink, sink2);
            writer.commit();
        }
        TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from x", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void testNoHysteresis0(
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

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
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

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void testNoHysteresisEndingAtPartitionBoundary(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
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

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
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

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }

    private void testNoHysteresisWithRollback(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
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

        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
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
            TestUtils.assertEquals( "count\n495\n", sink);
        }

        TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=375 or i>380", sink);
        TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
        TestUtils.assertEquals(sink, sink2);
    }
}
