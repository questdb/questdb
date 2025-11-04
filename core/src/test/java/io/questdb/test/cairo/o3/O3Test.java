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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.TxnScoreboard;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.WriterRowUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.Job;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Chars;
import io.questdb.std.Decimal256;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.cairo.TestRecord;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.vm.Vm.getStorageLength;
import static io.questdb.test.AbstractCairoTest.replaceTimestampSuffix;
import static io.questdb.test.AbstractCairoTest.replaceTimestampSuffix1;

public class O3Test extends AbstractO3Test {
    private final StringBuilder tstData = new StringBuilder();

    @Override
    @Before
    public void setUp() {
        super.setUp();
        Vect.resetPerformanceCounters();
        partitionO3SplitThreshold = 512 * (1L << 10);
        LOG.info().$("partitionO3SplitThreshold = ").$(partitionO3SplitThreshold).$();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        int count = Vect.getPerformanceCountersCount();
        if (count > 0) {
            tstData.setLength(0);
            tstData.append(testName.getMethodName()).append(",");
            long total = 0;
            for (int i = 0; i < count; i++) {
                long val = Vect.getPerformanceCounter(i);
                tstData.append(val).append("\t");
                total += val;
            }
            tstData.append(total);

            Os.sleep(10);
            System.err.flush();
            System.err.println(tstData);
            System.err.flush();
        }
        partitionO3SplitThreshold = -1;
        super.tearDown();
    }

    @Test
    // test case is contributed by Zhongwei Yao
    public void testAddColumnO3Fuzz() throws Exception {
        executeWithPool(
                0, (engine, compiler, sqlExecutionContext, timestampTypeName) -> {
                    final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
                    final String tableName = "ABC";
                    TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                            .col("productId", ColumnType.INT)
                            .col("productName", ColumnType.STRING)
                            .col("category", ColumnType.SYMBOL)
                            .col("price", ColumnType.DOUBLE)
                            .timestamp(timestampType.getTimestampType())
                            .col("supplier", ColumnType.SYMBOL);

                    TestUtils.createTable(engine, model);

                    AtomicInteger errorCount = new AtomicInteger();
                    short[] columnTypes = new short[]{ColumnType.INT, ColumnType.STRING, ColumnType.SYMBOL, ColumnType.DOUBLE};
                    IntList newColTypes = new IntList();
                    CyclicBarrier barrier = new CyclicBarrier(1);

                    try (TableWriter writer = TestUtils.getWriter(engine, tableName)) {
                        Thread writerT = new Thread(() -> {
                            try {
                                int i = 0;
                                ObjList<CharSequence> newCols = new ObjList<>();
                                TimestampDriver driver = ColumnType.getTimestampDriver(writer.getTimestampType());
                                // the number of iterations here (70) is critical to reproduce O3 crash
                                // also row counts (1000) at every iteration, do not wind these down please
                                // to high values make for a long-running unit test
                                while (i < 70) {
                                    ++i;
                                    final long ts = driver.parseFloorLiteral("2013-03-04T00:00:00.000Z");

                                    Rnd rnd = new Rnd();
                                    appendNProducts(ts, rnd, writer, driver);

                                    CharSequence newCol = "price" + i;
                                    short colType = columnTypes[i % columnTypes.length];
                                    writer.addColumn(newCol, colType);
                                    newCols.add(newCol);
                                    newColTypes.add(colType);

                                    appendNWithNewColumn(rnd, writer, newCols, newColTypes);

                                    writer.commit();
                                    LOG.info().$("writer:").$(i).$("put once with ColumnType:").$(colType).$();
                                    barrier.await();
                                }
                                barrier.await();
                            } catch (Throwable e) {
                                //noinspection CallToPrintStackTrace
                                e.printStackTrace();
                                errorCount.incrementAndGet();
                            } finally {
                                Path.clearThreadLocals();
                                LOG.info().$("write is done").$();
                            }
                        });

                        writerT.start();
                        writerT.join();
                    }
                    Assert.assertEquals(0, errorCount.get());
                }
        );
    }

    @Test
    public void testAppendIntoColdWriterContended() throws Exception {
        executeWithPool(0, O3Test::testAppendIntoColdWriter0);
    }

    @Test
    public void testAppendOrderStabilityParallel() throws Exception {
        executeWithPool(4, O3Test::testAppendOrderStability);
    }

    @Test
    public void testAppendToLastPartition() throws Exception {
        executeWithPool(4, O3Test::testAppendToLastPartition);
    }

    @Test
    public void testBench() throws Exception {
        // On OSX, it's not trivial to increase the open file limit per process
        Assume.assumeFalse(Os.isOSX());

        switch (rnd.nextInt(3)) {
            case 0:
                LOG.info().$("executing vanilla bench").$();
                executeVanilla((engine, compiler, context) -> testBench0(engine, compiler, context, timestampType.getTypeName()));
                break;
            case 1:
                LOG.info().$("executing contended bench").$();
                executeWithPool(0, O3Test::testBench0);
                break;
            case 2:
                LOG.info().$("executing parallel bench").$();
                executeWithPool(4, O3Test::testBench0);
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Test
    public void testColumnTopLastAppend() throws Exception {
        executeVanilla(O3Test::testColumnTopLastAppendColumn0);
    }

    @Test
    public void testColumnTopLastAppendBlankContended() throws Exception {
        executeWithPool(0, O3Test::testColumnTopLastAppendBlankColumn0);
    }

    @Test
    public void testColumnTopLastAppendContended() throws Exception {
        executeWithPool(0, O3Test::testColumnTopLastAppendColumn0);
    }

    @Test
    public void testColumnTopLastDataMerge() throws Exception {
        executeVanilla(O3Test::testColumnTopLastDataMergeData0);
    }

    @Test
    public void testColumnTopLastDataMerge2Data() throws Exception {
        executeVanilla(O3Test::testColumnTopLastDataMerge2Data0);
    }

    @Test
    public void testColumnTopLastDataMerge2DataContended() throws Exception {
        executeWithPool(0, O3Test::testColumnTopLastDataMerge2Data0);
    }

    @Test
    public void testColumnTopLastDataMerge2DataParallel() throws Exception {
        executeWithPool(4, O3Test::testColumnTopLastDataMerge2Data0);
    }

    @Test
    public void testColumnTopLastDataMergeDataContended() throws Exception {
        executeWithPool(0, O3Test::testColumnTopLastDataMergeData0);
    }

    @Test
    public void testColumnTopLastDataMergeDataParallel() throws Exception {
        executeWithPool(4, O3Test::testColumnTopLastDataMergeData0);
    }

    @Test
    public void testColumnTopLastDataOOOData() throws Exception {
        executeVanilla(O3Test::testColumnTopLastDataOOOData0);
    }

    @Test
    public void testColumnTopLastDataOOODataContended() throws Exception {
        executeWithPool(0, O3Test::testColumnTopLastDataOOOData0);
    }

    @Test
    public void testColumnTopLastDataOOODataParallel() throws Exception {
        executeWithPool(4, O3Test::testColumnTopLastDataOOOData0);
    }

    @Test
    public void testColumnTopLastOOOData() throws Exception {
        executeVanilla(O3Test::testColumnTopLastOOOData0);
    }

    @Test
    public void testColumnTopLastOOODataContended() throws Exception {
        executeWithPool(0, O3Test::testColumnTopLastOOOData0);
    }

    @Test
    public void testColumnTopLastOOODataParallel() throws Exception {
        executeWithPool(4, O3Test::testColumnTopLastOOOData0);
    }

    @Test
    public void testColumnTopLastOOOPrefix() throws Exception {
        executeVanilla(O3Test::testColumnTopLastOOOPrefix0);
    }

    @Test
    public void testColumnTopLastOOOPrefixContended() throws Exception {
        executeWithPool(0, O3Test::testColumnTopLastOOOPrefix0);
    }

    @Test
    public void testColumnTopLastOOOPrefixParallel() throws Exception {
        executeWithPool(4, O3Test::testColumnTopLastOOOPrefix0);
    }

    @Test
    public void testColumnTopLastParallel() throws Exception {
        executeWithPool(4, O3Test::testColumnTopLastAppendColumn0);
    }

    @Test
    public void testColumnTopMidAppend() throws Exception {
        executeVanilla(O3Test::testColumnTopMidAppendColumn0);
    }

    @Test
    public void testColumnTopMidAppendBlank() throws Exception {
        executeVanilla(O3Test::testColumnTopMidAppendBlankColumn0);
    }

    @Test
    public void testColumnTopMidAppendBlankContended() throws Exception {
        executeWithPool(0, O3Test::testColumnTopMidAppendBlankColumn0);
    }

    @Test
    public void testColumnTopMidAppendBlankParallel() throws Exception {
        executeWithPool(4, O3Test::testColumnTopMidAppendBlankColumn0);
    }

    @Test
    public void testColumnTopMidAppendContended() throws Exception {
        executeWithPool(0, O3Test::testColumnTopMidAppendColumn0);
    }

    @Test
    public void testColumnTopMidAppendParallel() throws Exception {
        executeWithPool(4, O3Test::testColumnTopMidAppendColumn0);
    }

    @Test
    public void testColumnTopMidDataMergeData() throws Exception {
        executeVanilla(O3Test::testColumnTopMidDataMergeData0);
    }

    @Test
    public void testColumnTopMidDataMergeDataContended() throws Exception {
        executeWithPool(0, O3Test::testColumnTopMidDataMergeData0);
    }

    @Test
    public void testColumnTopMidDataMergeDataParallel() throws Exception {
        executeWithPool(4, O3Test::testColumnTopMidDataMergeData0);
    }

    @Test
    public void testColumnTopMidMergeBlank() throws Exception {
        executeVanilla(O3Test::testColumnTopMidMergeBlankColumn0);
    }

    @Test
    public void testColumnTopMidMergeBlankContended() throws Exception {
        executeWithPool(0, O3Test::testColumnTopMidMergeBlankColumn0);
    }

    @Test
    public void testColumnTopMidMergeBlankGeoHash() throws Exception {
        executeVanilla(O3Test::testColumnTopMidMergeBlankColumnGeoHash0);
    }

    @Test
    public void testColumnTopMidMergeBlankParallel() throws Exception {
        executeWithPool(4, O3Test::testColumnTopMidMergeBlankColumn0);
    }

    @Test
    public void testColumnTopMidOOOData() throws Exception {
        executeVanilla(O3Test::testColumnTopMidOOOData0);
    }

    @Test
    public void testColumnTopMidOOODataContended() throws Exception {
        executeWithPool(0, O3Test::testColumnTopMidOOOData0);
    }

    @Test
    public void testColumnTopMidOOODataParallel() throws Exception {
        executeWithPool(4, O3Test::testColumnTopMidOOOData0);
    }

    @Test
    public void testColumnTopMidOOODataUtf8Contended() throws Exception {
        executeWithPool(0, O3Test::testColumnTopMidOOODataUtf80);
    }

    @Test
    public void testColumnTopMidOOODataUtf8Parallel() throws Exception {
        executeWithPool(4, O3Test::testColumnTopMidOOODataUtf80);
    }

    @Test
    public void testColumnTopMoveUncommittedContended() throws Exception {
        executeWithPool(0, O3Test::testColumnTopMoveUncommitted0);
    }

    @Test
    public void testColumnTopMoveUncommittedLastPart() throws Exception {
        executeVanilla(O3Test::testColumnTopMoveUncommittedLastPart0);
    }

    @Test
    public void testColumnTopMoveUncommittedLastPartContended() throws Exception {
        executeWithPool(0, O3Test::testColumnTopMoveUncommittedLastPart0);
    }

    @Test
    public void testColumnTopMoveUncommittedLastPartParallel() throws Exception {
        executeWithPool(4, O3Test::testColumnTopMoveUncommittedLastPart0);
    }

    @Test
    public void testColumnTopMoveUncommittedParallel() throws Exception {
        executeWithPool(4, O3Test::testColumnTopMoveUncommitted0);
    }

    @Test
    public void testColumnTopNewPartitionMiddleOfTableContended() throws Exception {
        executeWithPool(0, O3Test::testColumnTopNewPartitionMiddleOfTable0);
    }

    @Test
    public void testColumnTopNewPartitionMiddleOfTableParallel() throws Exception {
        executeWithPool(4, O3Test::testColumnTopNewPartitionMiddleOfTable0);
    }

    @Test
    public void testDecimalO3Insert() throws Exception {
        executeVanilla(O3Test::testDecimalO3Insert0);
    }

    @Test
    public void testDecimalO3InsertContended() throws Exception {
        executeWithPool(0, O3Test::testDecimalO3Insert0);
    }

    @Test
    public void testDecimalO3InsertParallel() throws Exception {
        executeWithPool(4, O3Test::testDecimalO3Insert0);
    }

    @Test
    public void testInsertNullTimestamp() throws Exception {
        executeWithPool(
                2, (engine, compiler, sqlExecutionContext, timestampTypeName) -> {
                    engine.execute(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " rnd_symbol('msft','ibm', 'googl') sym," +
                                    " timestamp_sequence(10000000000,1000000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(100)" +
                                    "), index(sym) timestamp (ts) partition by DAY",
                            sqlExecutionContext
                    );

                    TestUtils.printSql(
                            compiler,
                            sqlExecutionContext,
                            "select count() from x",
                            sink2
                    );

                    TestUtils.printSql(
                            compiler,
                            sqlExecutionContext,
                            "select max(ts) from x",
                            sink
                    );

                    final String expectedMaxTimestamp = Chars.toString(sink);

                    // to_timestamp produces NULL because values does not match the pattern
                    try {
                        engine.execute("insert into x values(0, 'abc', to_timestamp('2019-08-15T16:03:06.595', 'yyyy-MM-dd:HH:mm:ss.SSSUUU'))", sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "designated timestamp column cannot be NULL");
                    }

                    assertXCount(
                            compiler,
                            sqlExecutionContext
                    );

                    assertMaxTimestamp(
                            engine,
                            expectedMaxTimestamp
                    );
                }
        );
    }

    @Test
    public void testInsertTouchesNotLastPartition() throws Exception {
        executeVanilla(O3Test::testOOOTouchesNotLastPartition0);
    }

    @Test
    public void testInsertTouchesNotLastPartitionParallel() throws Exception {
        executeWithPool(4, O3Test::testOOOTouchesNotLastPartition0);
    }

    @Test
    public void testInsertTouchesNotLastPartitionTopParallel() throws Exception {
        executeWithPool(4, O3Test::testOOOTouchesNotLastPartitionTop0);
    }

    @Test
    public void testInsertTouchesNotLastTopPartition() throws Exception {
        executeVanilla(O3Test::testOOOTouchesNotLastPartitionTop0);
    }

    @Test
    public void testLagOverflowBySize() throws Exception {
        executeVanilla(O3Test::testLagOverflowBySize0);
    }

    @Test
    public void testLagOverflowBySizeContended() throws Exception {
        executeWithPool(0, O3Test::testLagOverflowBySize0);
    }

    @Test
    public void testLagOverflowBySizeParallel() throws Exception {
        executeWithPool(4, O3Test::testLagOverflowBySize0);
    }

    @Test
    public void testLagOverflowMidCommit() throws Exception {
        executeVanilla(O3Test::testLagOverflowMidCommit0);
    }

    @Test
    public void testLagOverflowMidCommitContended() throws Exception {
        executeWithPool(0, O3Test::testLagOverflowMidCommit0);
    }

    @Test
    public void testLagOverflowMidCommitParallel() throws Exception {
        executeWithPool(4, O3Test::testLagOverflowMidCommit0);
    }

    @Test
    public void testLargeO3MaxLagContended() throws Exception {
        executeWithPool(0, O3Test::testLargeO3MaxLag0);
    }

    @Test
    public void testManyPartitionsParallel() throws Exception {
        executeWithPool(4, O3Test::testManyPartitionsParallel);
    }

    @Test
    public void testO3EdgeBugContended() throws Exception {
        executeWithPool(0, O3Test::testO3EdgeBug);
    }

    @Test
    public void testOOOFollowedByAnotherOOOParallel() throws Exception {
        executeWithPool(4, O3Test::testOooFollowedByAnotherOOO0);
    }

    @Test
    public void testPartitionedDataAppendOOData() throws Exception {
        executeVanilla(O3Test::testPartitionedDataAppendOOData0);
    }

    @Test
    public void testPartitionedDataAppendOODataContended() throws Exception {
        executeWithPool(0, O3Test::testPartitionedDataAppendOOData0);
    }

    @Test
    public void testPartitionedDataAppendOODataIndexed() throws Exception {
        executeVanilla(O3Test::testPartitionedDataAppendOODataIndexed0);
    }

    @Test
    public void testPartitionedDataAppendOODataIndexedContended() throws Exception {
        executeWithPool(0, O3Test::testPartitionedDataAppendOODataIndexed0);
    }

    @Test
    public void testPartitionedDataAppendOODataIndexedParallel() throws Exception {
        executeWithPool(4, O3Test::testPartitionedDataAppendOODataIndexed0);
    }

    @Test
    public void testPartitionedDataAppendOODataNotNullStrTail() throws Exception {
        executeVanilla(O3Test::testPartitionedDataAppendOODataNotNullStrTail0);
    }

    @Test
    public void testPartitionedDataAppendOODataNotNullStrTailContended() throws Exception {
        executeWithPool(0, O3Test::testPartitionedDataAppendOODataNotNullStrTail0);
    }

    @Test
    public void testPartitionedDataAppendOODataNotNullStrTailParallel() throws Exception {
        executeWithPool(4, O3Test::testPartitionedDataAppendOODataNotNullStrTail0);
    }

    @Test
    public void testPartitionedDataAppendOODataParallel() throws Exception {
        executeWithPool(4, O3Test::testPartitionedDataAppendOOData0);
    }

    @Test
    public void testPartitionedDataAppendOOPrependOOData() throws Exception {
        executeVanilla(O3Test::testPartitionedDataAppendOOPrependOOData0);
    }

    @Test
    public void testPartitionedDataAppendOOPrependOODataContended() throws Exception {
        executeWithPool(0, O3Test::testPartitionedDataAppendOOPrependOOData0);
    }

    @Test
    public void testPartitionedDataAppendOOPrependOODataParallel() throws Exception {
        executeWithPool(4, O3Test::testPartitionedDataAppendOOPrependOOData0);
    }

    @Test
    public void testPartitionedDataMergeData() throws Exception {
        executeVanilla(O3Test::testPartitionedDataMergeData0);
    }

    @Test
    public void testPartitionedDataMergeDataContended() throws Exception {
        executeWithPool(0, O3Test::testPartitionedDataMergeData0);
    }

    @Test
    public void testPartitionedDataMergeDataParallel() throws Exception {
        executeWithPool(4, O3Test::testPartitionedDataMergeData0);
    }

    @Test
    public void testPartitionedDataMergeEnd() throws Exception {
        executeVanilla(O3Test::testPartitionedDataMergeEnd0);
    }

    @Test
    public void testPartitionedDataMergeEndContended() throws Exception {
        executeWithPool(0, O3Test::testPartitionedDataMergeEnd0);
    }

    @Test
    public void testPartitionedDataMergeEndParallel() throws Exception {
        executeWithPool(4, O3Test::testPartitionedDataMergeEnd0);
    }

    @Test
    public void testPartitionedDataOOData() throws Exception {
        executeVanilla(O3Test::testPartitionedDataOOData0);
    }

    @Test
    public void testPartitionedDataOODataContended() throws Exception {
        executeWithPool(0, O3Test::testPartitionedDataOOData0);
    }

    @Test
    public void testPartitionedDataOODataParallel() throws Exception {
        executeWithPool(4, O3Test::testPartitionedDataOOData0);
    }

    @Test
    public void testPartitionedDataOODataPbOOData() throws Exception {
        executeVanilla(O3Test::testPartitionedDataOODataPbOOData0);
    }

    @Test
    public void testPartitionedDataOODataPbOODataContended() throws Exception {
        executeWithPool(0, O3Test::testPartitionedDataOODataPbOOData0);
    }

    @Test
    public void testPartitionedDataOODataPbOODataDropColumnContended() throws Exception {
        executeWithPool(0, O3Test::testPartitionedDataOODataPbOODataDropColumn0);
    }

    @Test
    public void testPartitionedDataOODataPbOODataDropColumnParallel() throws Exception {
        executeWithPool(4, O3Test::testPartitionedDataOODataPbOODataDropColumn0);
    }

    @Test
    public void testPartitionedDataOODataPbOODataParallel() throws Exception {
        executeWithPool(4, O3Test::testPartitionedDataOODataPbOOData0);
    }

    @Test
    public void testPartitionedDataOOIntoLastIndexSearchBug() throws Exception {
        executeVanilla(O3Test::testPartitionedDataOOIntoLastIndexSearchBug0);
    }

    @Test
    public void testPartitionedDataOOIntoLastIndexSearchBugContended() throws Exception {
        executeWithPool(0, O3Test::testPartitionedDataOOIntoLastIndexSearchBug0);
    }

    @Test
    public void testPartitionedDataOOIntoLastIndexSearchBugParallel() throws Exception {
        executeWithPool(4, O3Test::testPartitionedDataOOIntoLastIndexSearchBug0);
    }

    @Test
    public void testPartitionedDataOOIntoLastOverflowIntoNewPartition() throws Exception {
        executeVanilla(O3Test::testPartitionedDataOOIntoLastOverflowIntoNewPartition0);
    }

    @Test
    public void testPartitionedDataOOIntoLastOverflowIntoNewPartitionContended() throws Exception {
        executeWithPool(0, O3Test::testPartitionedDataOOIntoLastOverflowIntoNewPartition0);
    }

    @Test
    public void testPartitionedDataOOIntoLastOverflowIntoNewPartitionParallel() throws Exception {
        executeWithPool(4, O3Test::testPartitionedDataOOIntoLastOverflowIntoNewPartition0);
    }

    @Test
    public void testPartitionedOOData() throws Exception {
        executeVanilla(O3Test::testPartitionedOOData0);
    }

    @Test
    public void testPartitionedOODataContended() throws Exception {
        executeWithPool(0, O3Test::testPartitionedOOData0);
    }

    @Test
    public void testPartitionedOODataOOCollapsed() throws Exception {
        executeVanilla(O3Test::testPartitionedOODataOOCollapsed0);
    }

    @Test
    public void testPartitionedOODataOOCollapsedContended() throws Exception {
        executeWithPool(0, O3Test::testPartitionedOODataOOCollapsed0);
    }

    @Test
    public void testPartitionedOODataOOCollapsedParallel() throws Exception {
        executeWithPool(4, O3Test::testPartitionedOODataOOCollapsed0);
    }

    @Test
    public void testPartitionedOODataParallel() throws Exception {
        executeWithPool(4, O3Test::testPartitionedOOData0);
    }

    @Test
    public void testPartitionedOODataUpdateMinTimestamp() throws Exception {
        executeVanilla(O3Test::testPartitionedOODataUpdateMinTimestamp0);
    }

    @Test
    public void testPartitionedOODataUpdateMinTimestampContended() throws Exception {
        executeWithPool(0, O3Test::testPartitionedOODataUpdateMinTimestamp0);
    }

    @Test
    public void testPartitionedOODataUpdateMinTimestampParallel() throws Exception {
        executeWithPool(4, O3Test::testPartitionedOODataUpdateMinTimestamp0);
    }

    @Test
    public void testPartitionedOOMerge() throws Exception {
        executeVanilla(O3Test::testPartitionedOOMerge0);
    }

    @Test
    public void testPartitionedOOMergeContended() throws Exception {
        executeWithPool(0, O3Test::testPartitionedOOMerge0);
    }

    @Test
    public void testPartitionedOOMergeData() throws Exception {
        executeVanilla(O3Test::testPartitionedOOMergeData0);
    }

    @Test
    public void testPartitionedOOMergeDataContended() throws Exception {
        executeWithPool(0, O3Test::testPartitionedOOMergeData0);
    }

    @Test
    public void testPartitionedOOMergeDataParallel() throws Exception {
        executeWithPool(4, O3Test::testPartitionedOOMergeData0);
    }

    @Test
    public void testPartitionedOOMergeOO() throws Exception {
        executeVanilla(O3Test::testPartitionedOOMergeOO0);
    }

    @Test
    public void testPartitionedOOMergeOOContended() throws Exception {
        executeWithPool(0, O3Test::testPartitionedOOMergeOO0);
    }

    @Test
    public void testPartitionedOOMergeOOParallel() throws Exception {
        executeWithPool(4, O3Test::testPartitionedOOMergeOO0);
    }

    @Test
    public void testPartitionedOOMergeParallel() throws Exception {
        executeWithPool(4, O3Test::testPartitionedOOMerge0);
    }

    @Test
    public void testPartitionedOOONullSetters() throws Exception {
        executeVanilla(O3Test::testPartitionedOOONullSetters0);
    }

    @Test
    public void testPartitionedOOONullSettersContended() throws Exception {
        executeWithPool(0, O3Test::testPartitionedOOONullSetters0);
    }

    @Test
    public void testPartitionedOOONullSettersParallel() throws Exception {
        executeWithPool(4, O3Test::testPartitionedOOONullSetters0);
    }

    @Test
    public void testPartitionedOOONullStrSetters() throws Exception {
        executeVanilla(O3Test::testPartitionedOOONullStrSetters0);
    }

    @Test
    public void testPartitionedOOPrefixesExistingPartitions() throws Exception {
        executeVanilla(O3Test::testPartitionedOOPrefixesExistingPartitions0);
    }

    @Test
    public void testPartitionedOOPrefixesExistingPartitionsContended() throws Exception {
        executeWithPool(0, O3Test::testPartitionedOOPrefixesExistingPartitions0);
    }

    @Test
    public void testPartitionedOOPrefixesExistingPartitionsParallel() throws Exception {
        executeWithPool(4, O3Test::testPartitionedOOPrefixesExistingPartitions0);
    }

    @Test
    public void testPartitionedOOTopAndBottom() throws Exception {
        executeVanilla(O3Test::testPartitionedOOTopAndBottom0);
    }

    @Test
    public void testPartitionedOOTopAndBottomContended() throws Exception {
        executeWithPool(0, O3Test::testPartitionedOOTopAndBottom0);
    }

    @Test
    public void testPartitionedOOTopAndBottomParallel() throws Exception {
        executeWithPool(4, O3Test::testPartitionedOOTopAndBottom0);
    }

    @Test
    public void testReadPartitionWhileMergingVarColumnWithColumnTop() throws Exception {
        // Read the partition with a column top at the same time
        // when TableWriter is merges it with O3 data.

        Assume.assumeTrue(partitionO3SplitThreshold > 2000);
        AtomicReference<SqlCompiler> compilerRef = new AtomicReference<>();
        AtomicReference<SqlExecutionContext> executionContextRef = new AtomicReference<>();
        AtomicBoolean compared = new AtomicBoolean(false);

        // Read the partition while it's being merged
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.containsAscii(name, "2020-02-05.") && Utf8s.containsAscii(name, Files.SEPARATOR + "last.d")) {
                    try {
                        TestUtils.assertSqlCursors(
                                compilerRef.get(),
                                executionContextRef.get(),
                                "zz order by ts",
                                "x",
                                LOG,
                                ColumnType.defaultStringImplementationIsUtf8()
                        );
                        compared.set(true);
                    } catch (SqlException e) {
                        throw new RuntimeException(e);
                    }
                }
                return super.openRW(name, opts);
            }
        };

        executeWithPool(
                0,
                (CairoEngine engine, SqlCompiler compiler, SqlExecutionContext executionContext, String timestampTypeName
                ) -> {
                    compilerRef.set(compiler);
                    executionContextRef.set(executionContext);

                    engine.execute(
                            "create table x as (" +
                                    "select" +
                                    " cast(x as int) i," +
                                    " -x j," +
                                    " timestamp_sequence('2020-02-03T13', 60*1000000L)::" + timestampTypeName + " ts" +
                                    " from long_sequence(60*24*2+300)" +
                                    ") timestamp (ts) partition by DAY",
                            executionContext
                    );

                    engine.execute("alter table x add column k uuid", executionContext);
                    engine.execute("alter table x add column kb binary", executionContext);
                    engine.execute("alter table x add column ks string", executionContext);
                    engine.execute("alter table x add column kv1 varchar", executionContext);
                    engine.execute("alter table x add column kv2 varchar", executionContext);
                    engine.execute("alter table x add column arr double[]", executionContext);
                    engine.execute("alter table x add column last int", executionContext);

                    engine.execute(
                            "create table y as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " timestamp_sequence('2020-02-05T20:01:05', 60*1000000L)::" + timestampTypeName + " ts," +
                                    " rnd_uuid4() as k," +
                                    " rnd_bin() as kb," +
                                    " rnd_str(5,16,2) as ks," +
                                    " rnd_varchar(1,40,1) as kv1," +
                                    " rnd_varchar(1,1,1) as kv2," +
                                    " rnd_double_array(1,1) as arr," +
                                    " -1 as last" +
                                    " from long_sequence(10))",
                            executionContext
                    );

                    engine.execute(
                            "create table z as (" +
                                    "select" +
                                    " cast(x as int) * 1000000 i," +
                                    " -x - 1000000L as j," +
                                    " timestamp_sequence('2020-02-05T17:01:05', 60*1000000L)::" + timestampTypeName + " ts," +
                                    " rnd_uuid4() as k," +
                                    " rnd_bin() as kb," +
                                    " rnd_str(5,16,2) as ks," +
                                    " rnd_varchar(1,40,1) as kv1," +
                                    " rnd_varchar(1,1,1) as kv2," +
                                    " rnd_double_array(1,1) as arr," +
                                    " -2 as last" +
                                    " from long_sequence(100))",
                            executionContext
                    );

                    // Create table zz with first 2 inserts to be ready to compare.
                    engine.execute(
                            "create table zz as (select * from x union all select * from y)",
                            executionContext
                    );

                    engine.execute("insert into x select * from y", executionContext);
                    engine.execute("insert into x select * from z", executionContext);

                    engine.execute("insert into zz select * from z", executionContext);

                    TestUtils.assertSqlCursors(
                            compiler,
                            executionContext,
                            "zz order by ts",
                            "x",
                            LOG,
                            ColumnType.defaultStringImplementationIsUtf8()
                    );
                },
                ff
        );

        Assert.assertTrue("comparison did not happen", compared.get());
    }

    @Test
    public void testRebuildIndexLastPartitionWithColumnTop() throws Exception {
        executeWithPool(0, O3Test::testRebuildIndexLastPartitionWithColumnTop);
    }

    @Test
    public void testRebuildIndexLastPartitionWithColumnTopParallel() throws Exception {
        executeWithPool(0, O3Test::testRebuildIndexLastPartitionWithColumnTop);
    }

    @Test
    public void testRebuildIndexWithColumnTopPrevPartitionParallel() throws Exception {
        executeWithPool(4, O3Test::testRebuildIndexWithColumnTopPrevPartition);
    }

    @Test
    public void testRebuildIndexWithColumnTopPrevPartitionParallelContended() throws Exception {
        executeWithPool(0, O3Test::testRebuildIndexWithColumnTopPrevPartition);
    }

    @Test
    public void testRepeatedIngest() throws Exception {
        executeWithPool(4, O3Test::testRepeatedIngest0);
    }

    @Test
    public void testSendDuplicatesContended() throws Exception {
        executeWithPool(4, O3Test::testSendDuplicates);
    }

    @Test
    public void testStringColumnPageBoundaries() throws Exception {
        dataAppendPageSize = (int) Files.PAGE_SIZE;

        // 03 memory size >> append page size. Why? It increases the probability of having an unallocated address
        // right after the end of o3 memory. If there is an access beyond the allocated O3 memory then OS detects this
        // and throws SEGFAULT.
        o3ColumnMemorySize = (int) Files.PAGE_SIZE * 1024 * 4;
        executeWithPool(
                0,
                (engine, compiler, sqlExecutionContext, timestampTypeName) -> {
                    int longsPerO3Page = o3ColumnMemorySize / 8;
                    int half = longsPerO3Page / 2;
                    engine.execute(
                            "create table x (str string, ts " + timestampTypeName + ") timestamp(ts) partition by day;",
                            sqlExecutionContext
                    );

                    try (TableWriterAPI writer = engine.getTableWriterAPI("x", "testing")) {
                        TimestampDriver driver = ColumnType.getTimestampDriver(writer.getMetadata().getTimestampType());
                        // 1970-01-02: all in order -> it goes into column memory. note that we are not committing yet.
                        long offsetMicros = driver.parseFloorLiteral("1970-01-02");
                        for (int i = 0; i < half; i++) {
                            TableWriter.Row row = writer.newRow(offsetMicros + driver.fromMicros(i));
                            row.putStr(0, "aa");
                            row.append();
                        }

                        // 1970-01-01 is older than the previous batch -> we switch to O3 and starts writing into O3 mem
                        for (int i = 0; i < half - 1; i++) {
                            // why 'half - 1'? String uses the n+1 storage model. So 'half' + 'half-1' should fit precisely
                            // into a single 03 page!
                            TableWriter.Row row = writer.newRow(driver.fromMicros(i));
                            row.putStr(0, "aa");
                            row.append();
                        }

                        // Let's commit everything at once!
                        // This will trigger O3 commit and copies data from column memory to O3 memory so they can be sorted.
                        // If it tries to copy to an area beyond the allocated O3 memory then OS will throw SEGFAULT
                        writer.commit();
                    }

                    // Q: No assert? Where is an assert?!
                    // A: We are testing that we didn't crash during O3 commit. 'Not crashing' is our definition of success here!
                }
        );
    }

    @Test
    public void testTwoTablesCompeteForBuffer() throws Exception {
        executeWithPool(4, O3Test::testTwoTablesCompeteForBuffer0);
    }

    @Test
    public void testVanillaO3MaxLag() throws Exception {
        executeVanilla(O3Test::testVanillaO3MaxLag0);
    }

    @Test
    public void testVanillaO3MaxLagContended() throws Exception {
        executeWithPool(0, O3Test::testVanillaO3MaxLag0);
    }

    @Test
    public void testVanillaO3MaxLagParallel() throws Exception {
        executeWithPool(4, O3Test::testVanillaO3MaxLag0);
    }

    @Test
    public void testVanillaO3MaxLagSinglePartition() throws Exception {
        executeVanilla(O3Test::testVanillaO3MaxLagSinglePartition0);
    }

    @Test
    public void testVanillaO3MaxLagSinglePartitionContended() throws Exception {
        executeWithPool(0, O3Test::testVanillaO3MaxLagSinglePartition0);
    }

    @Test
    public void testVanillaO3MaxLagSinglePartitionParallel() throws Exception {
        executeWithPool(4, O3Test::testVanillaO3MaxLagSinglePartition0);
    }

    @Test
    public void testVarColumnCopyLargePrefix() throws Exception {
        partitionO3SplitThreshold = 100 * (1L << 30); // 100GB, effectively no partition split

        ConcurrentLinkedQueue<Long> writeLen = new ConcurrentLinkedQueue<>();
        executeWithPool(
                0,
                (engine, compiler, sqlExecutionContext, timestampTypeName) -> {
                    Assume.assumeTrue(engine.getConfiguration().isWriterMixedIOEnabled());

                    String strColVal =
                            "2022-09-22T17:06:37.036305Z I i.q.c.O3CopyJob o3 copy [blockType=2, columnType=131080, dstFixFd=397, dstFixSize=1326000000, dstFixOffset=0, dstVarFd=0, dstVarSize=0, dstVarOffset=0, srcDataLo=0, srcDataHi=164458776, srcDataMax=165250000, srcOooLo=0, srcOooHi=0, srcOooMax=500000, srcOooPartitionLo=0, srcOooPartitionHi=499999, mixedIOFlag=true]";
                    int len = getStorageLength(strColVal);
                    int records = Integer.MAX_VALUE / len + 5;

                    // String
                    String tableName = "testVarColumnCopyLargePrefix";
                    engine.execute(
                            "create table " + tableName + " as ( " +
                                    "select " +
                                    "'" + strColVal + "' as str, " +
                                    " timestamp_sequence('2022-02-24', 1000)::" + timestampTypeName + " ts" +
                                    " from long_sequence(" + records + ")" +
                                    ") timestamp (ts) partition by DAY", sqlExecutionContext
                    );

                    TimestampDriver driver = timestampType.getDriver();
                    long maxTimestamp = driver.parseFloorLiteral("2022-02-24") + driver.fromMicros(records * 1000L);
                    CharSequence o3Ts = driver.toMSecString(maxTimestamp - driver.fromMicros(2000));
                    engine.execute("insert into " + tableName + " VALUES('abcd', '" + o3Ts + "')", sqlExecutionContext);

                    // Check that there was an attempt to write a file bigger than 2GB
                    long max = 0;
                    for (Long wLen : writeLen) {
                        if (wLen > max) {
                            max = wLen;
                        }
                    }
                    Assert.assertTrue(max > (2L << 30));

                    TestUtils.assertSql(
                            compiler,
                            sqlExecutionContext,
                            "select * from " + tableName + " limit -5,5",
                            sink,
                            replaceTimestampSuffix1(
                                    "str\tts\n" +
                                            strColVal + "\t2022-02-24T00:51:43.301000Z\n" +
                                            strColVal + "\t2022-02-24T00:51:43.302000Z\n" +
                                            strColVal + "\t2022-02-24T00:51:43.303000Z\n" +
                                            "abcd\t2022-02-24T00:51:43.303000Z\n" +
                                            strColVal + "\t2022-02-24T00:51:43.304000Z\n", timestampTypeName)
                    );
                }, new TestFilesFacadeImpl() {
                    @Override
                    public long write(long fd, long address, long len, long offset) {
                        writeLen.add(len);
                        return super.write(fd, address, len, offset);
                    }
                }
        );
    }

    @Test
    public void testVarColumnPageBoundariesAppend() throws Exception {
        dataAppendPageSize = (int) Files.PAGE_SIZE;
        executeWithPool(
                0,
                (engine, compiler, sqlExecutionContext, timestampTypeName
                ) -> {
                    int longsPerPage = dataAppendPageSize / 8;
                    int hi = (longsPerPage + 8) * 2;
                    int lo = (longsPerPage - 8) * 2;
                    for (int i = lo; i < hi; i++) {
                        LOG.info().$("=========== iteration ").$(i).$(" ===================").$();
                        testVarColumnPageBoundaryIterationWithColumnTop(engine, compiler, sqlExecutionContext, i, replaceTimestampSuffix("12:00:01.000000Z", timestampTypeName));
                        engine.execute("drop table x", sqlExecutionContext);
                    }
                }
        );
    }

    @Test
    public void testVarColumnPageBoundariesInsertInTheMiddle() throws Exception {
        dataAppendPageSize = (int) Files.PAGE_SIZE;
        executeWithPool(
                0,
                (engine, compiler, sqlExecutionContext, timestampTypeName
                ) -> {
                    int longsPerPage = dataAppendPageSize / 8;
                    int hi = (longsPerPage + 8) * 2;
                    int lo = (longsPerPage - 8) * 2;
                    for (int i = lo; i < hi; i++) {
                        LOG.info().$("=========== iteration ").$(i).$(" ===================").$();
                        testVarColumnPageBoundaryIterationWithColumnTop(engine, compiler, sqlExecutionContext, i, replaceTimestampSuffix1("11:00:00.002500Z", timestampTypeName));
                        engine.execute("drop table x", sqlExecutionContext);
                    }
                }
        );
    }

    @Test
    public void testVarColumnPageBoundariesPrepend() throws Exception {
        dataAppendPageSize = (int) Files.PAGE_SIZE;
        executeWithPool(
                0,
                (engine, compiler, sqlExecutionContext, timestampTypeName
                ) -> {
                    int longsPerPage = dataAppendPageSize / 8;
                    int hi = (longsPerPage + 8) * 2;
                    int lo = (longsPerPage - 8) * 2;
                    for (int i = lo; i < hi; i++) {
                        LOG.info().$("=========== iteration ").$(i).$(" ===================").$();
                        testVarColumnPageBoundaryIterationWithColumnTop(engine, compiler, sqlExecutionContext, i, replaceTimestampSuffix("00:00:01.000000Z", timestampTypeName));
                        engine.execute("drop table x", sqlExecutionContext);
                    }
                }
        );
    }

    @Test
    public void testWriterOpensCorrectTxnPartitionOnRestart() throws Exception {
        executeWithPool(0, O3Test::testWriterOpensCorrectTxnPartitionOnRestart0);
    }

    @Test
    public void testWriterOpensUnmappedPage() throws Exception {
        executeWithPool(0, O3Test::testWriterOpensUnmappedPage);
    }

    private static void assertLag(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String createTableWith,
            String selectFrom,
            String o3InsertSql,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
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
                        " timestamp_sequence(500000000000L,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2, 1) arr," +
                        " from long_sequence(0)" +
                        "), index(sym) timestamp (ts) partition by MONTH " +
                        createTableWith,
                sqlExecutionContext
        );

        engine.execute(
                "create table top as (" +
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
                        // the formula below creates lag-friendly timestamp distribution
                        " cast(500000000000L + ((x-1)/4) * 1000000L + (4-(x-1)%4)*80009  as timestamp)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2, 1) arr," +
                        selectFrom +
                        ")",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all top)",
                "y order by ts",
                o3InsertSql,
                "x",
                "y",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext, engine);

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );
    }

    private static void assertO3DataCursors(
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
            engine.execute(referenceTableDDL, sqlExecutionContext);
        }
        if (o3InsertSQL != null) {
            engine.execute(o3InsertSQL, sqlExecutionContext);
        }
        testXAndIndex(
                engine,
                compiler,
                sqlExecutionContext,
                referenceSQL,
                assertSQL
        );

        TestUtils.assertEquals(
                compiler,
                sqlExecutionContext,
                "select count() from " + countReferenceSQL,
                "select count() from " + countAssertSQL
        );
    }

    private static void dropTableY(CairoEngine engine, SqlExecutionContext sqlExecutionContext) throws SqlException {
        engine.execute("drop table y", sqlExecutionContext);
    }

    private static void testAppendIntoColdWriter0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String timestampTypeName
    ) throws SqlException {
        // create table with roughly 2AM data
        engine.execute(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_long() j," +
                        " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + " ts" +
                        " from long_sequence(500)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        final String expected = replaceTimestampSuffix1("""
                i\tj\tts\tv
                1\t4689592037643856\t1970-01-06T18:53:20.000000Z\tnull
                2\t4729996258992366\t1970-01-06T18:55:00.000000Z\tnull
                3\t7746536061816329025\t1970-01-06T18:56:40.000000Z\tnull
                4\t-6945921502384501475\t1970-01-06T18:58:20.000000Z\tnull
                5\t8260188555232587029\t1970-01-06T19:00:00.000000Z\tnull
                6\t8920866532787660373\t1970-01-06T19:01:40.000000Z\tnull
                7\t-7611843578141082998\t1970-01-06T19:03:20.000000Z\tnull
                8\t-5354193255228091881\t1970-01-06T19:05:00.000000Z\tnull
                9\t-2653407051020864006\t1970-01-06T19:06:40.000000Z\tnull
                10\t-1675638984090602536\t1970-01-06T19:08:20.000000Z\tnull
                11\t8754899401270281932\t1970-01-06T19:10:00.000000Z\tnull
                12\t3614738589890112276\t1970-01-06T19:11:40.000000Z\tnull
                13\t7513930126251977934\t1970-01-06T19:13:20.000000Z\tnull
                14\t-7489826605295361807\t1970-01-06T19:15:00.000000Z\tnull
                15\t-4094902006239100839\t1970-01-06T19:16:40.000000Z\tnull
                16\t-4474835130332302712\t1970-01-06T19:18:20.000000Z\tnull
                17\t-6943924477733600060\t1970-01-06T19:20:00.000000Z\tnull
                18\t8173439391403617681\t1970-01-06T19:21:40.000000Z\tnull
                19\t3394168647660478011\t1970-01-06T19:23:20.000000Z\tnull
                20\t5408639942391651698\t1970-01-06T19:25:00.000000Z\tnull
                21\t7953532976996720859\t1970-01-06T19:26:40.000000Z\tnull
                22\t-8968886490993754893\t1970-01-06T19:28:20.000000Z\tnull
                23\t6236292340460979716\t1970-01-06T19:30:00.000000Z\tnull
                24\t8336855953317473051\t1970-01-06T19:31:40.000000Z\tnull
                25\t-3985256597569472057\t1970-01-06T19:33:20.000000Z\tnull
                26\t-8284534269888369016\t1970-01-06T19:35:00.000000Z\tnull
                27\t9116006198143953886\t1970-01-06T19:36:40.000000Z\tnull
                28\t-6856503215590263904\t1970-01-06T19:38:20.000000Z\tnull
                29\t-8671107786057422727\t1970-01-06T19:40:00.000000Z\tnull
                30\t5539350449504785212\t1970-01-06T19:41:40.000000Z\tnull
                31\t4086802474270249591\t1970-01-06T19:43:20.000000Z\tnull
                32\t7039584373105579285\t1970-01-06T19:45:00.000000Z\tnull
                33\t-4485747798769957016\t1970-01-06T19:46:40.000000Z\tnull
                34\t-4100339045953973663\t1970-01-06T19:48:20.000000Z\tnull
                35\t-7475784581806461658\t1970-01-06T19:50:00.000000Z\tnull
                36\t5926373848160552879\t1970-01-06T19:51:40.000000Z\tnull
                37\t375856366519011353\t1970-01-06T19:53:20.000000Z\tnull
                38\t2811900023577169860\t1970-01-06T19:55:00.000000Z\tnull
                39\t8416773233910814357\t1970-01-06T19:56:40.000000Z\tnull
                40\t6600081143067978388\t1970-01-06T19:58:20.000000Z\tnull
                41\t8349358446893356086\t1970-01-06T20:00:00.000000Z\tnull
                42\t7700030475747712339\t1970-01-06T20:01:40.000000Z\tnull
                43\t8000176386900538697\t1970-01-06T20:03:20.000000Z\tnull
                44\t-8479285918156402508\t1970-01-06T20:05:00.000000Z\tnull
                45\t3958193676455060057\t1970-01-06T20:06:40.000000Z\tnull
                46\t9194293009132827518\t1970-01-06T20:08:20.000000Z\tnull
                47\t7759636733976435003\t1970-01-06T20:10:00.000000Z\tnull
                48\t8942747579519338504\t1970-01-06T20:11:40.000000Z\tnull
                49\t-7166640824903897951\t1970-01-06T20:13:20.000000Z\tnull
                50\t7199909180655756830\t1970-01-06T20:15:00.000000Z\tnull
                51\t-8889930662239044040\t1970-01-06T20:16:40.000000Z\tnull
                52\t-4442449726822927731\t1970-01-06T20:18:20.000000Z\tnull
                53\t-3546540271125917157\t1970-01-06T20:20:00.000000Z\tnull
                54\t6404066507400987550\t1970-01-06T20:21:40.000000Z\tnull
                55\t6854658259142399220\t1970-01-06T20:23:20.000000Z\tnull
                56\t-4842723177835140152\t1970-01-06T20:25:00.000000Z\tnull
                57\t-5986859522579472839\t1970-01-06T20:26:40.000000Z\tnull
                58\t8573481508564499209\t1970-01-06T20:28:20.000000Z\tnull
                59\t5476540218465058302\t1970-01-06T20:30:00.000000Z\tnull
                60\t7709707078566863064\t1970-01-06T20:31:40.000000Z\tnull
                61\t6270672455202306717\t1970-01-06T20:33:20.000000Z\tnull
                62\t-8480005421611953360\t1970-01-06T20:35:00.000000Z\tnull
                63\t-8955092533521658248\t1970-01-06T20:36:40.000000Z\tnull
                64\t1205595184115760694\t1970-01-06T20:38:20.000000Z\tnull
                65\t3619114107112892010\t1970-01-06T20:40:00.000000Z\tnull
                66\t8325936937764905778\t1970-01-06T20:41:40.000000Z\tnull
                67\t-7723703968879725602\t1970-01-06T20:43:20.000000Z\tnull
                68\t-6186964045554120476\t1970-01-06T20:45:00.000000Z\tnull
                69\t-4986232506486815364\t1970-01-06T20:46:40.000000Z\tnull
                70\t-7885528361265853230\t1970-01-06T20:48:20.000000Z\tnull
                71\t-6794405451419334859\t1970-01-06T20:50:00.000000Z\tnull
                72\t-6253307669002054137\t1970-01-06T20:51:40.000000Z\tnull
                73\t6820495939660535106\t1970-01-06T20:53:20.000000Z\tnull
                74\t3152466304308949756\t1970-01-06T20:55:00.000000Z\tnull
                75\t3705833798044144433\t1970-01-06T20:56:40.000000Z\tnull
                76\t6993925225312419449\t1970-01-06T20:58:20.000000Z\tnull
                77\t7304706257487484767\t1970-01-06T21:00:00.000000Z\tnull
                78\t6179044593759294347\t1970-01-06T21:01:40.000000Z\tnull
                79\t4238042693748641409\t1970-01-06T21:03:20.000000Z\tnull
                80\t5334238747895433003\t1970-01-06T21:05:00.000000Z\tnull
                81\t-7439145921574737517\t1970-01-06T21:06:40.000000Z\tnull
                82\t-7153335833712179123\t1970-01-06T21:08:20.000000Z\tnull
                83\t7392877322819819290\t1970-01-06T21:10:00.000000Z\tnull
                84\t5536695302686527374\t1970-01-06T21:11:40.000000Z\tnull
                85\t-8811278461560712840\t1970-01-06T21:13:20.000000Z\tnull
                86\t-4371031944620334155\t1970-01-06T21:15:00.000000Z\tnull
                87\t-5228148654835984711\t1970-01-06T21:16:40.000000Z\tnull
                88\t6953604195888525841\t1970-01-06T21:18:20.000000Z\tnull
                89\t7585187984144261203\t1970-01-06T21:20:00.000000Z\tnull
                90\t-6919361415374675248\t1970-01-06T21:21:40.000000Z\tnull
                91\t5942480340792044027\t1970-01-06T21:23:20.000000Z\tnull
                92\t2968650253814730084\t1970-01-06T21:25:00.000000Z\tnull
                93\t9036423629723776443\t1970-01-06T21:26:40.000000Z\tnull
                94\t-7316123607359392486\t1970-01-06T21:28:20.000000Z\tnull
                95\t7641144929328646356\t1970-01-06T21:30:00.000000Z\tnull
                96\t8171230234890248944\t1970-01-06T21:31:40.000000Z\tnull
                97\t-7689224645273531603\t1970-01-06T21:33:20.000000Z\tnull
                98\t-7611030538224290496\t1970-01-06T21:35:00.000000Z\tnull
                99\t-7266580375914176030\t1970-01-06T21:36:40.000000Z\tnull
                100\t-5233802075754153909\t1970-01-06T21:38:20.000000Z\tnull
                101\t-4692986177227268943\t1970-01-06T21:40:00.000000Z\tnull
                102\t7528475600160271422\t1970-01-06T21:41:40.000000Z\tnull
                103\t6473208488991371747\t1970-01-06T21:43:20.000000Z\tnull
                104\t-4091897709796604687\t1970-01-06T21:45:00.000000Z\tnull
                105\t-3107239868490395663\t1970-01-06T21:46:40.000000Z\tnull
                106\t7522482991756933150\t1970-01-06T21:48:20.000000Z\tnull
                107\t5866052386674669514\t1970-01-06T21:50:00.000000Z\tnull
                108\t8831607763082315932\t1970-01-06T21:51:40.000000Z\tnull
                109\t3518554007419864093\t1970-01-06T21:53:20.000000Z\tnull
                110\t571924429013198086\t1970-01-06T21:55:00.000000Z\tnull
                111\t5271904137583983788\t1970-01-06T21:56:40.000000Z\tnull
                112\t-6487422186320825289\t1970-01-06T21:58:20.000000Z\tnull
                113\t-5935729153136649272\t1970-01-06T22:00:00.000000Z\tnull
                114\t-5028301966399563827\t1970-01-06T22:01:40.000000Z\tnull
                115\t-4608960730952244094\t1970-01-06T22:03:20.000000Z\tnull
                116\t-7387846268299105911\t1970-01-06T22:05:00.000000Z\tnull
                117\t7848851757452822827\t1970-01-06T22:06:40.000000Z\tnull
                118\t6373284943859989837\t1970-01-06T22:08:20.000000Z\tnull
                119\t4014104627539596639\t1970-01-06T22:10:00.000000Z\tnull
                120\t5867661438830308598\t1970-01-06T22:11:40.000000Z\tnull
                121\t-6365568807668711866\t1970-01-06T22:13:20.000000Z\tnull
                122\t-3214230645884399728\t1970-01-06T22:15:00.000000Z\tnull
                123\t9029468389542245059\t1970-01-06T22:16:40.000000Z\tnull
                124\t4349785000461902003\t1970-01-06T22:18:20.000000Z\tnull
                125\t-8081265393416742311\t1970-01-06T22:20:00.000000Z\tnull
                126\t-8663526666273545842\t1970-01-06T22:21:40.000000Z\tnull
                127\t7122109662042058469\t1970-01-06T22:23:20.000000Z\tnull
                128\t6079275973105085025\t1970-01-06T22:25:00.000000Z\tnull
                129\t8155981915549526575\t1970-01-06T22:26:40.000000Z\tnull
                130\t-4908948886680892316\t1970-01-06T22:28:20.000000Z\tnull
                131\t8587391969565958670\t1970-01-06T22:30:00.000000Z\tnull
                132\t4167328623064065836\t1970-01-06T22:31:40.000000Z\tnull
                133\t-8906871108655466881\t1970-01-06T22:33:20.000000Z\tnull
                134\t-5512653573876168745\t1970-01-06T22:35:00.000000Z\tnull
                135\t-6161552193869048721\t1970-01-06T22:36:40.000000Z\tnull
                136\t-8425379692364264520\t1970-01-06T22:38:20.000000Z\tnull
                137\t9131882544462008265\t1970-01-06T22:40:00.000000Z\tnull
                138\t-6626590012581323602\t1970-01-06T22:41:40.000000Z\tnull
                139\t8654368763944235816\t1970-01-06T22:43:20.000000Z\tnull
                140\t1504966027220213191\t1970-01-06T22:45:00.000000Z\tnull
                141\t2474001847338644868\t1970-01-06T22:46:40.000000Z\tnull
                142\t8977823376202838087\t1970-01-06T22:48:20.000000Z\tnull
                143\t-7995393784734742820\t1970-01-06T22:50:00.000000Z\tnull
                144\t-6190031864817509934\t1970-01-06T22:51:40.000000Z\tnull
                145\t8702525427024484485\t1970-01-06T22:53:20.000000Z\tnull
                146\t2762535352290012031\t1970-01-06T22:55:00.000000Z\tnull
                147\t-8408704077728333147\t1970-01-06T22:56:40.000000Z\tnull
                148\t-4116381468144676168\t1970-01-06T22:58:20.000000Z\tnull
                149\t8611582118025429627\t1970-01-06T23:00:00.000000Z\tnull
                150\t2235053888582262602\t1970-01-06T23:01:40.000000Z\tnull
                151\t915906628308577949\t1970-01-06T23:03:20.000000Z\tnull
                152\t1761725072747471430\t1970-01-06T23:05:00.000000Z\tnull
                153\t5407260416602246268\t1970-01-06T23:06:40.000000Z\tnull
                154\t-5710210982977201267\t1970-01-06T23:08:20.000000Z\tnull
                155\t-9128506055317587235\t1970-01-06T23:10:00.000000Z\tnull
                156\t9063592617902736531\t1970-01-06T23:11:40.000000Z\tnull
                157\t-2406077911451945242\t1970-01-06T23:13:20.000000Z\tnull
                158\t-6003256558990918704\t1970-01-06T23:15:00.000000Z\tnull
                159\t6623443272143014835\t1970-01-06T23:16:40.000000Z\tnull
                160\t-8082754367165748693\t1970-01-06T23:18:20.000000Z\tnull
                161\t-1438352846894825721\t1970-01-06T23:20:00.000000Z\tnull
                162\t-5439556746612026472\t1970-01-06T23:21:40.000000Z\tnull
                163\t-7256514778130150964\t1970-01-06T23:23:20.000000Z\tnull
                164\t-2605516556381756042\t1970-01-06T23:25:00.000000Z\tnull
                165\t-7103100524321179064\t1970-01-06T23:26:40.000000Z\tnull
                166\t9144172287200792483\t1970-01-06T23:28:20.000000Z\tnull
                167\t-5024542231726589509\t1970-01-06T23:30:00.000000Z\tnull
                168\t-2768987637252864412\t1970-01-06T23:31:40.000000Z\tnull
                169\t-3289070757475856942\t1970-01-06T23:33:20.000000Z\tnull
                170\t7277991313017866925\t1970-01-06T23:35:00.000000Z\tnull
                171\t6574958665733670985\t1970-01-06T23:36:40.000000Z\tnull
                172\t-5817309269683380708\t1970-01-06T23:38:20.000000Z\tnull
                173\t-8910603140262731534\t1970-01-06T23:40:00.000000Z\tnull
                174\t7035958104135945276\t1970-01-06T23:41:40.000000Z\tnull
                175\t9169223215810156269\t1970-01-06T23:43:20.000000Z\tnull
                176\t7973684666911773753\t1970-01-06T23:45:00.000000Z\tnull
                177\t9143800334706665900\t1970-01-06T23:46:40.000000Z\tnull
                178\t8907283191913183400\t1970-01-06T23:48:20.000000Z\tnull
                179\t7505077128008208443\t1970-01-06T23:50:00.000000Z\tnull
                180\t6624299878707135910\t1970-01-06T23:51:40.000000Z\tnull
                181\t4990844051702733276\t1970-01-06T23:53:20.000000Z\tnull
                182\t3446015290144635451\t1970-01-06T23:55:00.000000Z\tnull
                183\t3393210801760647293\t1970-01-06T23:56:40.000000Z\tnull
                184\t-8193596495481093333\t1970-01-06T23:58:20.000000Z\tnull
                10\t3500000\t1970-01-06T23:58:20.000000Z\t10.2
                185\t9130722816060153827\t1970-01-07T00:00:00.000000Z\tnull
                186\t4385246274849842834\t1970-01-07T00:01:40.000000Z\tnull
                187\t-7709579215942154242\t1970-01-07T00:03:20.000000Z\tnull
                188\t-6912707344119330199\t1970-01-07T00:05:00.000000Z\tnull
                189\t-6265628144430971336\t1970-01-07T00:06:40.000000Z\tnull
                190\t-2656704586686189855\t1970-01-07T00:08:20.000000Z\tnull
                191\t-5852887087189258121\t1970-01-07T00:10:00.000000Z\tnull
                192\t-5616524194087992934\t1970-01-07T00:11:40.000000Z\tnull
                193\t8889492928577876455\t1970-01-07T00:13:20.000000Z\tnull
                194\t5398991075259361292\t1970-01-07T00:15:00.000000Z\tnull
                195\t-4947578609540920695\t1970-01-07T00:16:40.000000Z\tnull
                196\t-1550912036246807020\t1970-01-07T00:18:20.000000Z\tnull
                197\t-3279062567400130728\t1970-01-07T00:20:00.000000Z\tnull
                198\t-6187389706549636253\t1970-01-07T00:21:40.000000Z\tnull
                199\t-5097437605148611401\t1970-01-07T00:23:20.000000Z\tnull
                200\t-9053195266501182270\t1970-01-07T00:25:00.000000Z\tnull
                201\t1064753200933634719\t1970-01-07T00:26:40.000000Z\tnull
                202\t2155318342410845737\t1970-01-07T00:28:20.000000Z\tnull
                203\t4437331957970287246\t1970-01-07T00:30:00.000000Z\tnull
                204\t8152044974329490473\t1970-01-07T00:31:40.000000Z\tnull
                205\t6108846371653428062\t1970-01-07T00:33:20.000000Z\tnull
                206\t4641238585508069993\t1970-01-07T00:35:00.000000Z\tnull
                207\t-5315599072928175674\t1970-01-07T00:36:40.000000Z\tnull
                208\t-8755128364143858197\t1970-01-07T00:38:20.000000Z\tnull
                209\t5294917053935522538\t1970-01-07T00:40:00.000000Z\tnull
                210\t5824745791075827139\t1970-01-07T00:41:40.000000Z\tnull
                211\t-8757007522346766135\t1970-01-07T00:43:20.000000Z\tnull
                212\t-1620198143795539853\t1970-01-07T00:45:00.000000Z\tnull
                213\t9161691782935400339\t1970-01-07T00:46:40.000000Z\tnull
                214\t5703149806881083206\t1970-01-07T00:48:20.000000Z\tnull
                215\t-6071768268784020226\t1970-01-07T00:50:00.000000Z\tnull
                216\t-5336116148746766654\t1970-01-07T00:51:40.000000Z\tnull
                217\t8009040003356908243\t1970-01-07T00:53:20.000000Z\tnull
                218\t5292387498953709416\t1970-01-07T00:55:00.000000Z\tnull
                219\t-6786804316219531143\t1970-01-07T00:56:40.000000Z\tnull
                220\t-1798101751056570485\t1970-01-07T00:58:20.000000Z\tnull
                221\t-8323443786521150653\t1970-01-07T01:00:00.000000Z\tnull
                222\t-7714378722470181347\t1970-01-07T01:01:40.000000Z\tnull
                223\t-2888119746454814889\t1970-01-07T01:03:20.000000Z\tnull
                224\t-8546113611224784332\t1970-01-07T01:05:00.000000Z\tnull
                225\t7158971986470055172\t1970-01-07T01:06:40.000000Z\tnull
                226\t5746626297238459939\t1970-01-07T01:08:20.000000Z\tnull
                227\t7574443524652611981\t1970-01-07T01:10:00.000000Z\tnull
                228\t-8994301462266164776\t1970-01-07T01:11:40.000000Z\tnull
                229\t4099611147050818391\t1970-01-07T01:13:20.000000Z\tnull
                230\t-9147563299122452591\t1970-01-07T01:15:00.000000Z\tnull
                231\t-7400476385601852536\t1970-01-07T01:16:40.000000Z\tnull
                232\t-8642609626818201048\t1970-01-07T01:18:20.000000Z\tnull
                233\t-2000273984235276379\t1970-01-07T01:20:00.000000Z\tnull
                234\t-166300099372695016\t1970-01-07T01:21:40.000000Z\tnull
                235\t-3416748419425937005\t1970-01-07T01:23:20.000000Z\tnull
                236\t6351664568801157821\t1970-01-07T01:25:00.000000Z\tnull
                237\t3084117448873356811\t1970-01-07T01:26:40.000000Z\tnull
                238\t6601850686822460257\t1970-01-07T01:28:20.000000Z\tnull
                239\t7759595275644638709\t1970-01-07T01:30:00.000000Z\tnull
                240\t4360855047041000285\t1970-01-07T01:31:40.000000Z\tnull
                241\t6087087705757854416\t1970-01-07T01:33:20.000000Z\tnull
                242\t-5103414617212558357\t1970-01-07T01:35:00.000000Z\tnull
                243\t8574802735490373479\t1970-01-07T01:36:40.000000Z\tnull
                244\t2387397055355257412\t1970-01-07T01:38:20.000000Z\tnull
                245\t8072168822566640807\t1970-01-07T01:40:00.000000Z\tnull
                246\t-3293392739929464726\t1970-01-07T01:41:40.000000Z\tnull
                247\t-8749723816463910031\t1970-01-07T01:43:20.000000Z\tnull
                248\t6127579245089953588\t1970-01-07T01:45:00.000000Z\tnull
                249\t-6883412613642983200\t1970-01-07T01:46:40.000000Z\tnull
                250\t-7153690499922882896\t1970-01-07T01:48:20.000000Z\tnull
                251\t7107508275327837161\t1970-01-07T01:50:00.000000Z\tnull
                252\t-8260644133007073640\t1970-01-07T01:51:40.000000Z\tnull
                253\t-7336930007738575369\t1970-01-07T01:53:20.000000Z\tnull
                254\t5552835357100545895\t1970-01-07T01:55:00.000000Z\tnull
                255\t4534912711595148130\t1970-01-07T01:56:40.000000Z\tnull
                256\t-7228011205059401944\t1970-01-07T01:58:20.000000Z\tnull
                257\t-6703401424236463520\t1970-01-07T02:00:00.000000Z\tnull
                258\t-8857660828600848720\t1970-01-07T02:01:40.000000Z\tnull
                259\t-3105499275013799956\t1970-01-07T02:03:20.000000Z\tnull
                260\t-8371487291073160693\t1970-01-07T02:05:00.000000Z\tnull
                261\t2383285963471887250\t1970-01-07T02:06:40.000000Z\tnull
                262\t1488156692375549016\t1970-01-07T02:08:20.000000Z\tnull
                263\t2151565237758036093\t1970-01-07T02:10:00.000000Z\tnull
                264\t4107109535030235684\t1970-01-07T02:11:40.000000Z\tnull
                265\t-8534688874718947140\t1970-01-07T02:13:20.000000Z\tnull
                266\t-3491277789316049618\t1970-01-07T02:15:00.000000Z\tnull
                267\t8815523022464325728\t1970-01-07T02:16:40.000000Z\tnull
                268\t4959459375462458218\t1970-01-07T02:18:20.000000Z\tnull
                269\t7037372650941669660\t1970-01-07T02:20:00.000000Z\tnull
                270\t4502522085684189707\t1970-01-07T02:21:40.000000Z\tnull
                271\t8850915006829016608\t1970-01-07T02:23:20.000000Z\tnull
                272\t-8095658968635787358\t1970-01-07T02:25:00.000000Z\tnull
                273\t-6716055087713781882\t1970-01-07T02:26:40.000000Z\tnull
                274\t-8425895280081943671\t1970-01-07T02:28:20.000000Z\tnull
                275\t8880550034995457591\t1970-01-07T02:30:00.000000Z\tnull
                276\t8464194176491581201\t1970-01-07T02:31:40.000000Z\tnull
                277\t6056145309392106540\t1970-01-07T02:33:20.000000Z\tnull
                278\t6121305147479698964\t1970-01-07T02:35:00.000000Z\tnull
                279\t2282781332678491916\t1970-01-07T02:36:40.000000Z\tnull
                280\t3527911398466283309\t1970-01-07T02:38:20.000000Z\tnull
                281\t6176277818569291296\t1970-01-07T02:40:00.000000Z\tnull
                282\t-8656750634622759804\t1970-01-07T02:41:40.000000Z\tnull
                283\t7058145725055366226\t1970-01-07T02:43:20.000000Z\tnull
                284\t-8849142892360165671\t1970-01-07T02:45:00.000000Z\tnull
                285\t-1134031357796740497\t1970-01-07T02:46:40.000000Z\tnull
                286\t-6782883555378798844\t1970-01-07T02:48:20.000000Z\tnull
                287\t6405448934035934123\t1970-01-07T02:50:00.000000Z\tnull
                288\t-8425483167065397721\t1970-01-07T02:51:40.000000Z\tnull
                289\t-8719797095546978745\t1970-01-07T02:53:20.000000Z\tnull
                290\t9089874911309539983\t1970-01-07T02:55:00.000000Z\tnull
                291\t-7202923278768687325\t1970-01-07T02:56:40.000000Z\tnull
                292\t-6571406865336879041\t1970-01-07T02:58:20.000000Z\tnull
                293\t-3396992238702724434\t1970-01-07T03:00:00.000000Z\tnull
                294\t-8205259083320287108\t1970-01-07T03:01:40.000000Z\tnull
                295\t-9029407334801459809\t1970-01-07T03:03:20.000000Z\tnull
                296\t-4058426794463997577\t1970-01-07T03:05:00.000000Z\tnull
                297\t6517485707736381444\t1970-01-07T03:06:40.000000Z\tnull
                298\t579094601177353961\t1970-01-07T03:08:20.000000Z\tnull
                299\t750145151786158348\t1970-01-07T03:10:00.000000Z\tnull
                300\t5048272224871876586\t1970-01-07T03:11:40.000000Z\tnull
                301\t-4547802916868961458\t1970-01-07T03:13:20.000000Z\tnull
                302\t-1832315370633201942\t1970-01-07T03:15:00.000000Z\tnull
                303\t-8888027247206813045\t1970-01-07T03:16:40.000000Z\tnull
                304\t3352215237270276085\t1970-01-07T03:18:20.000000Z\tnull
                305\t6937484962759020303\t1970-01-07T03:20:00.000000Z\tnull
                306\t7797019568426198829\t1970-01-07T03:21:40.000000Z\tnull
                307\t2691623916208307891\t1970-01-07T03:23:20.000000Z\tnull
                308\t6184401532241477140\t1970-01-07T03:25:00.000000Z\tnull
                309\t-8653777305694768077\t1970-01-07T03:26:40.000000Z\tnull
                310\t8756159220596318848\t1970-01-07T03:28:20.000000Z\tnull
                311\t4579251508938058953\t1970-01-07T03:30:00.000000Z\tnull
                312\t425369166370563563\t1970-01-07T03:31:40.000000Z\tnull
                313\t5478379480606573987\t1970-01-07T03:33:20.000000Z\tnull
                314\t-4284648096271470489\t1970-01-07T03:35:00.000000Z\tnull
                315\t-1741953200710332294\t1970-01-07T03:36:40.000000Z\tnull
                316\t-4450383397583441126\t1970-01-07T03:38:20.000000Z\tnull
                317\t8984932460293088377\t1970-01-07T03:40:00.000000Z\tnull
                318\t9058067501760744164\t1970-01-07T03:41:40.000000Z\tnull
                319\t8490886945852172597\t1970-01-07T03:43:20.000000Z\tnull
                320\t-8841102831894340636\t1970-01-07T03:45:00.000000Z\tnull
                321\t8503557900983561786\t1970-01-07T03:46:40.000000Z\tnull
                322\t1508637934261574620\t1970-01-07T03:48:20.000000Z\tnull
                323\t663602980874300508\t1970-01-07T03:50:00.000000Z\tnull
                324\t788901813531436389\t1970-01-07T03:51:40.000000Z\tnull
                325\t6793615437970356479\t1970-01-07T03:53:20.000000Z\tnull
                326\t6380499796471875623\t1970-01-07T03:55:00.000000Z\tnull
                327\t2006083905706813287\t1970-01-07T03:56:40.000000Z\tnull
                328\t5513479607887040119\t1970-01-07T03:58:20.000000Z\tnull
                329\t5343275067392229138\t1970-01-07T04:00:00.000000Z\tnull
                330\t4527121849171257172\t1970-01-07T04:01:40.000000Z\tnull
                331\t4847320715984654162\t1970-01-07T04:03:20.000000Z\tnull
                332\t7092246624397344208\t1970-01-07T04:05:00.000000Z\tnull
                333\t6445007901796870697\t1970-01-07T04:06:40.000000Z\tnull
                334\t1669226447966988582\t1970-01-07T04:08:20.000000Z\tnull
                335\t5953039264407551685\t1970-01-07T04:10:00.000000Z\tnull
                336\t7592940205308166826\t1970-01-07T04:11:40.000000Z\tnull
                337\t-7414829143044491558\t1970-01-07T04:13:20.000000Z\tnull
                338\t-6819946977256689384\t1970-01-07T04:15:00.000000Z\tnull
                339\t-7186310556474199346\t1970-01-07T04:16:40.000000Z\tnull
                340\t-8814330552804983713\t1970-01-07T04:18:20.000000Z\tnull
                341\t-8960406850507339854\t1970-01-07T04:20:00.000000Z\tnull
                342\t-8793423647053878901\t1970-01-07T04:21:40.000000Z\tnull
                343\t5941398229034918748\t1970-01-07T04:23:20.000000Z\tnull
                344\t5980197440602572628\t1970-01-07T04:25:00.000000Z\tnull
                345\t2106240318003963024\t1970-01-07T04:26:40.000000Z\tnull
                346\t9200214878918264613\t1970-01-07T04:28:20.000000Z\tnull
                347\t-8211260649542902334\t1970-01-07T04:30:00.000000Z\tnull
                348\t5068939738525201696\t1970-01-07T04:31:40.000000Z\tnull
                349\t3820631780839257855\t1970-01-07T04:33:20.000000Z\tnull
                350\t-9219078548506735248\t1970-01-07T04:35:00.000000Z\tnull
                351\t8737100589707440954\t1970-01-07T04:36:40.000000Z\tnull
                352\t9044897286885345735\t1970-01-07T04:38:20.000000Z\tnull
                353\t-7381322665528955510\t1970-01-07T04:40:00.000000Z\tnull
                354\t6174532314769579955\t1970-01-07T04:41:40.000000Z\tnull
                355\t-8930904012891908076\t1970-01-07T04:43:20.000000Z\tnull
                356\t-6765703075406647091\t1970-01-07T04:45:00.000000Z\tnull
                357\t8810110521992874823\t1970-01-07T04:46:40.000000Z\tnull
                358\t7570866088271751947\t1970-01-07T04:48:20.000000Z\tnull
                359\t-7274175842748412916\t1970-01-07T04:50:00.000000Z\tnull
                360\t6753412894015940665\t1970-01-07T04:51:40.000000Z\tnull
                361\t2106204205501581842\t1970-01-07T04:53:20.000000Z\tnull
                362\t2307279172463257591\t1970-01-07T04:55:00.000000Z\tnull
                363\t812677186520066053\t1970-01-07T04:56:40.000000Z\tnull
                364\t4621844195437841424\t1970-01-07T04:58:20.000000Z\tnull
                365\t-7724577649125721868\t1970-01-07T05:00:00.000000Z\tnull
                366\t-7171265782561774995\t1970-01-07T05:01:40.000000Z\tnull
                367\t6966461743143051249\t1970-01-07T05:03:20.000000Z\tnull
                368\t7629109032541741027\t1970-01-07T05:05:00.000000Z\tnull
                369\t-7212878484370155026\t1970-01-07T05:06:40.000000Z\tnull
                370\t5963775257114848600\t1970-01-07T05:08:20.000000Z\tnull
                371\t3771494396743411509\t1970-01-07T05:10:00.000000Z\tnull
                372\t8798087869168938593\t1970-01-07T05:11:40.000000Z\tnull
                373\t8984775562394712402\t1970-01-07T05:13:20.000000Z\tnull
                374\t3792128300541831563\t1970-01-07T05:15:00.000000Z\tnull
                375\t7101009950667960843\t1970-01-07T05:16:40.000000Z\tnull
                376\t-6460532424840798061\t1970-01-07T05:18:20.000000Z\tnull
                377\t-5044078842288373275\t1970-01-07T05:20:00.000000Z\tnull
                378\t-3323322733858034601\t1970-01-07T05:21:40.000000Z\tnull
                379\t-7665470829783532891\t1970-01-07T05:23:20.000000Z\tnull
                380\t6738282533394287579\t1970-01-07T05:25:00.000000Z\tnull
                381\t6146164804821006241\t1970-01-07T05:26:40.000000Z\tnull
                382\t-7398902448022205322\t1970-01-07T05:28:20.000000Z\tnull
                383\t-2471456524133707236\t1970-01-07T05:30:00.000000Z\tnull
                384\t9041413988802359580\t1970-01-07T05:31:40.000000Z\tnull
                385\t5922689877598858022\t1970-01-07T05:33:20.000000Z\tnull
                386\t5168847330186110459\t1970-01-07T05:35:00.000000Z\tnull
                387\t8987698540484981038\t1970-01-07T05:36:40.000000Z\tnull
                388\t-7228768303272348606\t1970-01-07T05:38:20.000000Z\tnull
                389\t5700115585432451578\t1970-01-07T05:40:00.000000Z\tnull
                390\t7879490594801163253\t1970-01-07T05:41:40.000000Z\tnull
                391\t-5432682396344996498\t1970-01-07T05:43:20.000000Z\tnull
                392\t-3463832009795858033\t1970-01-07T05:45:00.000000Z\tnull
                393\t-8555544472620366464\t1970-01-07T05:46:40.000000Z\tnull
                394\t5205180235397887203\t1970-01-07T05:48:20.000000Z\tnull
                395\t2364286642781155412\t1970-01-07T05:50:00.000000Z\tnull
                396\t5494476067484139960\t1970-01-07T05:51:40.000000Z\tnull
                397\t7357244054212773895\t1970-01-07T05:53:20.000000Z\tnull
                398\t-8506266080452644687\t1970-01-07T05:55:00.000000Z\tnull
                399\t-1905597357123382478\t1970-01-07T05:56:40.000000Z\tnull
                400\t-5496131157726548905\t1970-01-07T05:58:20.000000Z\tnull
                401\t-7474351066761292033\t1970-01-07T06:00:00.000000Z\tnull
                402\t-6482694999745905510\t1970-01-07T06:01:40.000000Z\tnull
                403\t-8026283444976158481\t1970-01-07T06:03:20.000000Z\tnull
                404\t5804262091839668360\t1970-01-07T06:05:00.000000Z\tnull
                405\t7297601774924170699\t1970-01-07T06:06:40.000000Z\tnull
                406\t-4229502740666959541\t1970-01-07T06:08:20.000000Z\tnull
                407\t8842585385650675361\t1970-01-07T06:10:00.000000Z\tnull
                408\t7046578844650327247\t1970-01-07T06:11:40.000000Z\tnull
                409\t8070302167413932495\t1970-01-07T06:13:20.000000Z\tnull
                410\t4480750444572460865\t1970-01-07T06:15:00.000000Z\tnull
                411\t6205872689407104125\t1970-01-07T06:16:40.000000Z\tnull
                412\t9029088579359707814\t1970-01-07T06:18:20.000000Z\tnull
                413\t-8737543979347648559\t1970-01-07T06:20:00.000000Z\tnull
                414\t-6522954364450041026\t1970-01-07T06:21:40.000000Z\tnull
                415\t-6221841196965409356\t1970-01-07T06:23:20.000000Z\tnull
                416\t6484482332827923784\t1970-01-07T06:25:00.000000Z\tnull
                417\t7036584259400395476\t1970-01-07T06:26:40.000000Z\tnull
                418\t-6795628328806886847\t1970-01-07T06:28:20.000000Z\tnull
                419\t7576110962745644701\t1970-01-07T06:30:00.000000Z\tnull
                420\t8537223925650740475\t1970-01-07T06:31:40.000000Z\tnull
                421\t8737613628813682249\t1970-01-07T06:33:20.000000Z\tnull
                422\t4598876523645326656\t1970-01-07T06:35:00.000000Z\tnull
                423\t6436453824498875972\t1970-01-07T06:36:40.000000Z\tnull
                424\t4634177780953489481\t1970-01-07T06:38:20.000000Z\tnull
                425\t6390608559661380246\t1970-01-07T06:40:00.000000Z\tnull
                426\t8282637062702131151\t1970-01-07T06:41:40.000000Z\tnull
                427\t5360746485515325739\t1970-01-07T06:43:20.000000Z\tnull
                428\t-7910490643543561037\t1970-01-07T06:45:00.000000Z\tnull
                429\t8321277364671502705\t1970-01-07T06:46:40.000000Z\tnull
                430\t3987576220753016999\t1970-01-07T06:48:20.000000Z\tnull
                431\t3944678179613436885\t1970-01-07T06:50:00.000000Z\tnull
                432\t6153381060986313135\t1970-01-07T06:51:40.000000Z\tnull
                433\t8278953979466939153\t1970-01-07T06:53:20.000000Z\tnull
                434\t6831200789490300310\t1970-01-07T06:55:00.000000Z\tnull
                435\t5175638765020222775\t1970-01-07T06:56:40.000000Z\tnull
                436\t7090323083171574792\t1970-01-07T06:58:20.000000Z\tnull
                437\t6598154038796950493\t1970-01-07T07:00:00.000000Z\tnull
                438\t6418970788912980120\t1970-01-07T07:01:40.000000Z\tnull
                439\t-7518902569991053841\t1970-01-07T07:03:20.000000Z\tnull
                440\t6083279743811422804\t1970-01-07T07:05:00.000000Z\tnull
                441\t7459338290943262088\t1970-01-07T07:06:40.000000Z\tnull
                442\t7657422372928739370\t1970-01-07T07:08:20.000000Z\tnull
                443\t6235849401126045090\t1970-01-07T07:10:00.000000Z\tnull
                444\t8227167469487474861\t1970-01-07T07:11:40.000000Z\tnull
                445\t4794469881975683047\t1970-01-07T07:13:20.000000Z\tnull
                446\t3861637258207773908\t1970-01-07T07:15:00.000000Z\tnull
                447\t8485507312523128674\t1970-01-07T07:16:40.000000Z\tnull
                448\t-5106801657083469087\t1970-01-07T07:18:20.000000Z\tnull
                449\t-7069883773042994098\t1970-01-07T07:20:00.000000Z\tnull
                450\t7415337004567900118\t1970-01-07T07:21:40.000000Z\tnull
                451\t9026435187365103026\t1970-01-07T07:23:20.000000Z\tnull
                452\t-6517956255651384489\t1970-01-07T07:25:00.000000Z\tnull
                453\t-5611837907908424613\t1970-01-07T07:26:40.000000Z\tnull
                454\t-4036499202601723677\t1970-01-07T07:28:20.000000Z\tnull
                455\t8197069319221391729\t1970-01-07T07:30:00.000000Z\tnull
                456\t1732923061962778685\t1970-01-07T07:31:40.000000Z\tnull
                457\t1737550138998374432\t1970-01-07T07:33:20.000000Z\tnull
                458\t1432925274378784738\t1970-01-07T07:35:00.000000Z\tnull
                459\t4698698969091611703\t1970-01-07T07:36:40.000000Z\tnull
                460\t3843127285248668146\t1970-01-07T07:38:20.000000Z\tnull
                461\t2004830221820243556\t1970-01-07T07:40:00.000000Z\tnull
                462\t5341431345186701123\t1970-01-07T07:41:40.000000Z\tnull
                463\t-8490120737538725244\t1970-01-07T07:43:20.000000Z\tnull
                464\t9158482703525773397\t1970-01-07T07:45:00.000000Z\tnull
                465\t7702559600184398496\t1970-01-07T07:46:40.000000Z\tnull
                466\t-6167105618770444067\t1970-01-07T07:48:20.000000Z\tnull
                467\t-6141734738138509500\t1970-01-07T07:50:00.000000Z\tnull
                468\t-7300976680388447983\t1970-01-07T07:51:40.000000Z\tnull
                469\t6260580881559018466\t1970-01-07T07:53:20.000000Z\tnull
                470\t1658444875429025955\t1970-01-07T07:55:00.000000Z\tnull
                471\t7920520795110290468\t1970-01-07T07:56:40.000000Z\tnull
                472\t-5701911565963471026\t1970-01-07T07:58:20.000000Z\tnull
                473\t-6446120489339099836\t1970-01-07T08:00:00.000000Z\tnull
                474\t6527501025487796136\t1970-01-07T08:01:40.000000Z\tnull
                475\t1851817982979037709\t1970-01-07T08:03:20.000000Z\tnull
                476\t2439907409146962686\t1970-01-07T08:05:00.000000Z\tnull
                477\t4160567228070722087\t1970-01-07T08:06:40.000000Z\tnull
                478\t3250595453661431788\t1970-01-07T08:08:20.000000Z\tnull
                479\t7780743197986640723\t1970-01-07T08:10:00.000000Z\tnull
                480\t-3261700233985485037\t1970-01-07T08:11:40.000000Z\tnull
                481\t-3578120825657825955\t1970-01-07T08:13:20.000000Z\tnull
                482\t7443603913302671026\t1970-01-07T08:15:00.000000Z\tnull
                483\t7794592287856397845\t1970-01-07T08:16:40.000000Z\tnull
                484\t-5391587298431311641\t1970-01-07T08:18:20.000000Z\tnull
                485\t9202397484277640888\t1970-01-07T08:20:00.000000Z\tnull
                486\t-6951348785425447115\t1970-01-07T08:21:40.000000Z\tnull
                487\t-4645139889518544281\t1970-01-07T08:23:20.000000Z\tnull
                488\t-7924422932179070052\t1970-01-07T08:25:00.000000Z\tnull
                489\t-6861664727068297324\t1970-01-07T08:26:40.000000Z\tnull
                490\t-6251867197325094983\t1970-01-07T08:28:20.000000Z\tnull
                491\t8177920927333375630\t1970-01-07T08:30:00.000000Z\tnull
                492\t8210594435353205032\t1970-01-07T08:31:40.000000Z\tnull
                493\t8417830123562577846\t1970-01-07T08:33:20.000000Z\tnull
                494\t6785355388782691241\t1970-01-07T08:35:00.000000Z\tnull
                495\t-5892588302528885225\t1970-01-07T08:36:40.000000Z\tnull
                496\t-1185822981454562836\t1970-01-07T08:38:20.000000Z\tnull
                497\t-5296023984443079410\t1970-01-07T08:40:00.000000Z\tnull
                498\t6829382503979752449\t1970-01-07T08:41:40.000000Z\tnull
                499\t3669882909701240516\t1970-01-07T08:43:20.000000Z\tnull
                500\t8068645982235546347\t1970-01-07T08:45:00.000000Z\tnull
                10\t3500000\t1970-01-07T08:45:00.000000Z\t10.2
                """, timestampTypeName);

        try (TableWriter w = TestUtils.getWriter(engine, "x")) {

            TimestampDriver driver = ColumnType.getTimestampDriver(w.getTimestampType());
            TableWriter.Row row;
            // lets add column
            w.addColumn("v", ColumnType.DOUBLE);

            // this row goes into a non-recent partition
            // triggering O3
            row = w.newRow(driver.fromMicros(518300000000L));
            row.putInt(0, 10);
            row.putLong(1, 3500000L);
            // skip over the timestamp
            row.putDouble(3, 10.2);
            row.append();

            // another O3 row, this time it is appended to last partition
            row = w.newRow(driver.fromMicros(549900000000L));
            row.putInt(0, 10);
            row.putLong(1, 3500000L);
            // skip over the timestamp
            row.putDouble(3, 10.2);
            row.append();
            w.commit();
        }

        TestUtils.printSql(
                compiler,
                executionContext,
                "x",
                sink2
        );

        TestUtils.assertEquals(expected, sink2);
    }

    private static void testAppendOrderStability(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x (" +
                        "seq long, " +
                        "sym symbol, " +
                        "ts " + timestampTypeName +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        String[] symbols = {"AA", "BB"};
        long[] seq = new long[symbols.length];

        // insert some records in order
        final Rnd rnd = new Rnd();
        try (TableWriter w = TestUtils.getWriter(engine, "x")) {
            TimestampDriver driver = ColumnType.getTimestampDriver(w.getTimestampType());
            long t = 0;
            for (int i = 0; i < 1000; i++) {
                TableWriter.Row r = w.newRow(driver.fromMicros(t++));
                int index = rnd.nextInt(1);
                r.putLong(0, seq[index]++);
                r.putSym(1, symbols[index]);
                r.append();
            }
            w.ic();

            // now do out of order
            for (int i = 0; i < 100_000; i++) {
                TableWriter.Row r;

                // symbol 0

                r = w.newRow(driver.fromMicros(t + 1));
                r.putLong(0, seq[0]++);
                r.putSym(1, symbols[0]);
                r.append();

                r = w.newRow(driver.fromMicros(t + 1));
                r.putLong(0, seq[0]++);
                r.putSym(1, symbols[0]);
                r.append();

                // symbol 1

                r = w.newRow(driver.fromMicros(t));
                r.putLong(0, seq[1]++);
                r.putSym(1, symbols[1]);
                r.append();

                r = w.newRow(driver.fromMicros(t));
                r.putLong(0, seq[1]++);
                r.putSym(1, symbols[1]);
                r.append();

                t += 2;
            }
            w.commit();
        }

        // now verify that sequence did not get mixed up in the table
        long[] actualSeq = new long[symbols.length];
        try (
                RecordCursorFactory f = compiler.compile("x", sqlExecutionContext).getRecordCursorFactory();
                RecordCursor cursor = f.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                int index = record.getInt(1);
                Assert.assertEquals(record.getLong(0), actualSeq[index]++);
            }
        }
    }

    private static void testAppendToLastPartition(CairoEngine engine,
                                                  SqlCompiler compiler,
                                                  SqlExecutionContext sqlExecutionContext,
                                                  String timestampTypeName) throws SqlException {
        engine.execute(
                "create table x (" +
                        " a int," +
                        " b double," +
                        " ts " + timestampTypeName +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        final Rnd rnd = new Rnd();
        try (TableWriter w = TestUtils.getWriter(engine, "x")) {
            TimestampDriver driver = ColumnType.getTimestampDriver(w.getTimestampType());
            long ts = 1000000 * 1000L;
            long step = 1000000;
            TxnScoreboard txnScoreboard = w.getTxnScoreboard();
            for (int i = 0; i < 1000; i++) {
                TableWriter.Row r;
                r = w.newRow(driver.fromMicros(ts - step));
                r.putInt(0, rnd.nextInt());
                r.putDouble(1, rnd.nextDouble());
                r.append();

                r = w.newRow(driver.fromMicros(ts));
                r.putInt(0, rnd.nextInt());
                r.putDouble(1, rnd.nextDouble());
                r.append();

                r = w.newRow(driver.fromMicros(ts + step));
                r.putInt(0, rnd.nextInt());
                r.putDouble(1, rnd.nextDouble());
                r.append();

                ts += step;

                long txn = w.getTxn();
                txnScoreboard.acquireTxn(0, txn);
                w.commit();
                txnScoreboard.releaseTxn(0, txn);
            }
        }
    }

    private static void testBench0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName) throws SqlException {
        // create table with roughly 2AM data
        engine.execute(
                "create table x as (" +
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
                        " timestamp_sequence(10000000000,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1,30,1) varc," +
                        " rnd_varchar(1,1,1) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(1000000)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        engine.execute(
                "create table append as (" +
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
                        " timestamp_sequence(3000000000l,100000000L)::" + timestampTypeName + " ts," + // mid partition for "x"
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1,30,1) varc," +
                        " rnd_varchar(1,1,1) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(1000000)" +
                        ")",
                sqlExecutionContext
        );

        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "y order by ts",
                "insert into x select * from append",
                "x",
                "y",
                "x"
        );
        assertXCountY(engine, compiler, sqlExecutionContext);
    }

    private static void testColumnTopLastAppendBlankColumn0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String timestampTypeName
    ) throws SqlException {
        // create table with roughly 2AM data
        engine.execute(
                "create table x as (" +
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
                        " rnd_char() t," +
                        " rnd_varchar(1,40,1) varc," +
                        " rnd_varchar(1,1,0) varc2," +
                        " rnd_double_array(2, 1) arr, " +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                executionContext
        );

        engine.execute("alter table x add column v double", executionContext);
        engine.execute("alter table x add column v1 float", executionContext);
        engine.execute("alter table x add column v2 int", executionContext);
        engine.execute("alter table x add column v3 byte", executionContext);
        engine.execute("alter table x add column v4 short", executionContext);
        engine.execute("alter table x add column v5 boolean", executionContext);
        engine.execute("alter table x add column v6 date", executionContext);
        engine.execute("alter table x add column v7 timestamp", executionContext);
        engine.execute("alter table x add column v8 symbol", executionContext);
        engine.execute("alter table x add column v10 char", executionContext);
        engine.execute("alter table x add column v11 string", executionContext);
        engine.execute("alter table x add column v12 binary", executionContext);
        engine.execute("alter table x add column v9 long", executionContext);
        engine.execute("alter table x add column v13 varchar", executionContext);
        engine.execute("alter table x add column v14 varchar", executionContext);
        engine.execute("alter table x add column v15 double[][]", executionContext);

        engine.execute(
                "create table append as (" +
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
                        " timestamp_sequence(549900000000L + 1,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,1,0) varc," +
                        " rnd_varchar(1,60,1) varc2," +
                        " rnd_double_array(2, 1) arr, " +
                        //  ------------------- new columns ------------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1,30, 0) v13," +
                        " rnd_varchar(1,1,1) v14," +
                        " rnd_double_array(2,1) v15," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        // create third table, which will contain both X and 1AM
        assertO3DataCursors(
                engine,
                compiler,
                executionContext,
                "create table y as (x union all append)",
                "y order by ts",
                "insert into x select * from append",
                "x",
                "y",
                "x"
        );

        assertIndexConsistency(compiler, executionContext, engine);
        assertXCountY(engine, compiler, executionContext);
    }

    private static void testColumnTopLastAppendColumn0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 1 as commit," +
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
                        " rnd_char() t," +
                        " rnd_varchar(1,30,1) varc," +
                        " rnd_varchar(1,1,1) varc2," +
                        " rnd_double_array(1, 1) arr, " +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("alter table x add column v double", sqlExecutionContext);
        engine.execute("alter table x add column v1 float", sqlExecutionContext);
        engine.execute("alter table x add column v2 int", sqlExecutionContext);
        engine.execute("alter table x add column v3 byte", sqlExecutionContext);
        engine.execute("alter table x add column v4 short", sqlExecutionContext);
        engine.execute("alter table x add column v5 boolean", sqlExecutionContext);
        engine.execute("alter table x add column v6 date", sqlExecutionContext);
        engine.execute("alter table x add column v7 timestamp", sqlExecutionContext);
        engine.execute("alter table x add column v8 symbol", sqlExecutionContext);
        engine.execute("alter table x add column v10 char", sqlExecutionContext);
        engine.execute("alter table x add column v11 string", sqlExecutionContext);
        engine.execute("alter table x add column v12 binary", sqlExecutionContext);
        engine.execute("alter table x add column v9 long", sqlExecutionContext);
        engine.execute("alter table x add column v13 varchar", sqlExecutionContext);
        engine.execute("alter table x add column v14 varchar", sqlExecutionContext);
        engine.execute("alter table x add column v15 double[][][]", sqlExecutionContext);

        // ---- new columns ----
        engine.execute(
                "insert into x " +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(549900000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,1,1) varc," +
                        " rnd_varchar(1,40,1) varc2," +
                        " rnd_double_array(1, 1) arr, " +
                        // ---- new columns ----
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1,1,1) v13," +
                        " rnd_varchar(1,40,1) v14," +
                        " rnd_double_array(3, 1) v15, " +
                        " from long_sequence(10)", sqlExecutionContext
        );

        engine.execute(
                "create table append as (" +
                        "select" +
                        " 3 as commit," +
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
                        " timestamp_sequence(550800000000L + 1,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,50,1) varc," +
                        " rnd_varchar(1,1,1) varc2," +
                        " rnd_double_array(1, 1) arr, " +
                        // --------- new columns -----------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1,20,1) v13," +
                        " rnd_varchar(1,1,1) v14," +
                        " rnd_double_array(3, 1) v15, " +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        assertO3DataConsistencyStableSort(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "insert into x select * from append"
        );

        assertIndexConsistency(compiler, sqlExecutionContext, engine);
        assertXCountY(engine, compiler, sqlExecutionContext);
    }

    private static void testColumnTopLastDataMerge2Data0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext ectx,
            String timestampTypeName
    ) throws SqlException {

        // merge in the middle, however data prefix is such
        // that we can deal with by reducing column top rather than copying columns

        engine.execute(
                "create table x as (" +
                        "select" +
                        " 0 as commit," +
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
                        " rnd_char() t," +
                        " rnd_varchar(1,30,1) varc," +
                        " rnd_varchar(1,1,1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                ectx
        );

        engine.execute("alter table x add column v double", ectx);
        engine.execute("alter table x add column v1 float", ectx);
        engine.execute("alter table x add column v2 int", ectx);
        engine.execute("alter table x add column v3 byte", ectx);
        engine.execute("alter table x add column v4 short", ectx);
        engine.execute("alter table x add column v5 boolean", ectx);
        engine.execute("alter table x add column v6 date", ectx);
        engine.execute("alter table x add column v7 timestamp", ectx);
        engine.execute("alter table x add column v8 symbol", ectx);
        engine.execute("alter table x add column v10 char", ectx);
        engine.execute("alter table x add column v11 string", ectx);
        engine.execute("alter table x add column v12 binary", ectx);
        engine.execute("alter table x add column v9 long", ectx);
        engine.execute("alter table x add column v13 varchar", ectx);
        engine.execute("alter table x add column v14 varchar", ectx);
        engine.execute("alter table x add column arr2 double[][]", ectx);

        engine.execute("create table z as (select * from x)", ectx);

        engine.execute(
                "create table append as ( " +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(549920000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,30,1) varc," +
                        " rnd_varchar(1,1,1) varc2," +
                        " rnd_double_array(2,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1,30,0) v13," +
                        " rnd_varchar(1,1,1) v14," +
                        " rnd_double_array(2,1) arr2," +
                        " from long_sequence(500)" +
                        ")",
                ectx
        );

        engine.execute(
                "create table append1 as (" +
                        "select " +
                        " 2 as commit," +
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
                        " timestamp_sequence(549920000000L,50000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,30,1) varc," +
                        " rnd_varchar(1,1,1) varc2," +
                        " rnd_double_array(2,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1,30,1) v13," +
                        " rnd_varchar(1,1,1) v14," +
                        " rnd_double_array(2,1) arr2," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                ectx
        );

        engine.execute("insert into x select * from append", ectx);

        assertO3DataConsistencyStableSort(
                engine,
                compiler,
                ectx,
                "create table y as (select * from z union all append union all append1)",
                "insert into x select * from append1"
        );

        // 599820000000
        engine.execute(
                "create table append2 as (" +
                        "select" +
                        " 3 as commit," +
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
                        " timestamp_sequence(599820000001L,50000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,30,1) varc," +
                        " rnd_varchar(1,1,1) varc2," +
                        " rnd_double_array(2,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1,30,1) v13," +
                        " rnd_varchar(1,1,1) v14," +
                        " rnd_double_array(2,1) arr2," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                ectx
        );

        TestUtils.printSql(compiler, ectx, "select count() from (x union all append2)", sink2);
        TestUtils.printSql(compiler, ectx, "select max(ts) from (x union all append2)", sink);

        final String expectedMaxTimestamp = Chars.toString(sink);

        // straight append
//        engine.insert("insert into x select * from append2", ectx);

        // to re-run the comparison, drop table "y"
        engine.execute("drop table y", ectx);

        assertO3DataConsistencyStableSort(
                engine,
                compiler,
                ectx,
                "create table y as (select * from z union all append union all append1 union all append2)",
                "insert into x select * from append2"
        );

        assertXCount(compiler, ectx);

        assertMaxTimestamp(
                engine,
                expectedMaxTimestamp
        );
    }

    private static void testColumnTopLastDataMergeData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {

        //
        // ----- last partition
        //
        // +-----------+
        // |   empty   |
        // |           |
        // +-----------+   <-- top -->       +---------+
        // |           |                     |   data  |
        // |           |   +----------+      +---------+
        // |           |   |   OOO    |      |   ooo   |
        // |           | < |  block   |  ==  +---------+
        // |           |   | (narrow) |      |   data  |
        // |           |   +----------+      +---------+
        // +-----------+
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 1 as commit," +
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
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 0) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("alter table x add column v double", sqlExecutionContext);
        engine.execute("alter table x add column v1 float", sqlExecutionContext);
        engine.execute("alter table x add column v2 int", sqlExecutionContext);
        engine.execute("alter table x add column v3 byte", sqlExecutionContext);
        engine.execute("alter table x add column v4 short", sqlExecutionContext);
        engine.execute("alter table x add column v5 boolean", sqlExecutionContext);
        engine.execute("alter table x add column v6 date", sqlExecutionContext);
        engine.execute("alter table x add column v7 timestamp", sqlExecutionContext);
        engine.execute("alter table x add column v8 symbol", sqlExecutionContext);
        engine.execute("alter table x add column v10 char", sqlExecutionContext);
        engine.execute("alter table x add column v11 string", sqlExecutionContext);
        engine.execute("alter table x add column v12 binary", sqlExecutionContext);
        engine.execute("alter table x add column v9 long", sqlExecutionContext);
        engine.execute("alter table x add column v13 varchar", sqlExecutionContext);
        engine.execute("alter table x add column v14 varchar", sqlExecutionContext);
        engine.execute("alter table x add column arr2 double[]", sqlExecutionContext);

        engine.execute("create table z as (select * from x)", sqlExecutionContext);

        engine.execute(
                "create table append as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(549920000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 0) varc2," +
                        " rnd_double_array(2,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1, 40, 1) v13," +
                        " rnd_varchar(1, 1, 1) v14," +
                        " rnd_double_array(1,1) arr2," +
                        " from long_sequence(500)" +
                        ")",
                sqlExecutionContext
        );

        engine.execute(
                "create table append2 as (" +
                        "select" +
                        " 3 as commit," +
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
                        " timestamp_sequence(549900000000L,50000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 0) varc2," +
                        " rnd_double_array(2,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1, 40, 1) v13," +
                        " rnd_varchar(1, 1, 1) v14," +
                        " rnd_double_array(1,1) arr2," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("insert into x select * from append", sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select count() from (x union all append2)", sink2);
        TestUtils.printSql(compiler, sqlExecutionContext, "select max(ts) from (x union all append2)", sink);
        final String expectedMaxTimestamp = Chars.toString(sink);

        assertO3DataConsistencyStableSort(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (select * from (z union all append union all append2))",
                "insert into x select * from append2"
        );

        assertXCount(compiler, sqlExecutionContext);

        assertMaxTimestamp(
                engine,
                expectedMaxTimestamp
        );
    }

    private static void testColumnTopLastDataOOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext ectx,
            String timestampTypeName
    ) throws SqlException {

        //
        // ----- last partition
        //
        // +-----------+
        // |   empty   |
        // |           |
        // +-----------+   <-- top -->       +---------+
        // |           |                     |   data  |
        // |           |   +----------+      +---------+
        // |           |   |   OOO    |      |   ooo   |
        // |           | < |  block   |  ==  +---------+
        // |           |   | (narrow) |      |   data  |
        // |           |   +----------+      +---------+
        // +-----------+
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 0 as commit," +
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
                        " rnd_char() t," +
                        " rnd_varchar(1,30,1) varc," +
                        " rnd_varchar(1,1,0) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                ectx
        );

        engine.execute("alter table x add column v double", ectx);
        engine.execute("alter table x add column v1 float", ectx);
        engine.execute("alter table x add column v2 int", ectx);
        engine.execute("alter table x add column v3 byte", ectx);
        engine.execute("alter table x add column v4 short", ectx);
        engine.execute("alter table x add column v5 boolean", ectx);
        engine.execute("alter table x add column v6 date", ectx);
        engine.execute("alter table x add column v7 timestamp", ectx);
        engine.execute("alter table x add column v8 symbol", ectx);
        engine.execute("alter table x add column v10 char", ectx);
        engine.execute("alter table x add column v11 string", ectx);
        engine.execute("alter table x add column v12 binary", ectx);
        engine.execute("alter table x add column v9 long", ectx);
        engine.execute("alter table x add column v13 varchar", ectx);
        engine.execute("alter table x add column v14 varchar", ectx);
        engine.execute("alter table x add column arr2 double[][]", ectx);

        engine.execute("create table z as (select * from x)", ectx);

        engine.execute(
                "create table append1 as ( " +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(549920000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,30,1) varc," +
                        " rnd_varchar(1,1,0) varc2," +
                        " rnd_double_array(2,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1,30,1) v13," +
                        " rnd_varchar(1,1,0) v14," +
                        " rnd_double_array(2,1) arr2," +
                        " from long_sequence(500)" +
                        ")",
                ectx
        );

        engine.execute(
                "create table append2 as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(549920000000L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,30,1) varc," +
                        " rnd_varchar(1,1,0) varc2," +
                        " rnd_double_array(2,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1,30,1) v13," +
                        " rnd_varchar(1,1,0) v14," +
                        " rnd_double_array(2,1) arr2," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                ectx
        );

        engine.execute("insert into x select * from append1", ectx);

        TestUtils.printSql(compiler, ectx, "select count() from (x union all append2)", sink2);
        TestUtils.printSql(compiler, ectx, "select max(ts) from (x union all append2)", sink);
        final String expectedMaxTimestamp = Chars.toString(sink);

        assertO3DataConsistencyStableSort(
                engine,
                compiler,
                ectx,
                "create table y as (select * from z union all append1 union all append2)",
                "insert into x select * from append2"
        );

        assertXCount(compiler, ectx);

        assertMaxTimestamp(
                engine,
                expectedMaxTimestamp
        );
    }

    private static void testColumnTopLastOOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext ectx,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 0 as commit," +
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
                        " rnd_char() t," +
                        " rnd_varchar(1,70,1) varc," +
                        " rnd_varchar(1,1,0) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                ectx
        );

        engine.execute("alter table x add column v double", ectx);
        engine.execute("alter table x add column v1 float", ectx);
        engine.execute("alter table x add column v2 int", ectx);
        engine.execute("alter table x add column v3 byte", ectx);
        engine.execute("alter table x add column v4 short", ectx);
        engine.execute("alter table x add column v5 boolean", ectx);
        engine.execute("alter table x add column v6 date", ectx);
        engine.execute("alter table x add column v7 timestamp", ectx);
        engine.execute("alter table x add column v8 symbol", ectx);
        engine.execute("alter table x add column v10 char", ectx);
        engine.execute("alter table x add column v11 string", ectx);
        engine.execute("alter table x add column v12 binary", ectx);
        engine.execute("alter table x add column v9 long", ectx);
        engine.execute("alter table x add column v13 varchar", ectx);
        engine.execute("alter table x add column v14 varchar", ectx);
        engine.execute("alter table x add column arr2 double[][]", ectx);

        engine.execute("create table z as (select * from x)", ectx);

        engine.execute(
                "create table append1 as ( " +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(549920000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,70,1) varc," +
                        " rnd_varchar(1,1,0) varc2," +
                        " rnd_double_array(2,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1,70,1) v13," +
                        " rnd_varchar(1,1,0) v14," +
                        " rnd_double_array(2,1) arr2," +
                        " from long_sequence(500)" +
                        ")",
                ectx
        );

        engine.execute(
                "create table append2 as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(546600000000L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1,70,1) varc," +
                        " rnd_varchar(1,1,0) varc2," +
                        " rnd_double_array(2,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1,70,1) v13," +
                        " rnd_varchar(1,1,0) v14," +
                        " rnd_double_array(2,1) arr2," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                ectx
        );

        engine.execute("insert into x select * from append1", ectx);
        TestUtils.printSql(compiler, ectx, "select count() from (x union all append2)", sink2);
        TestUtils.printSql(compiler, ectx, "select max(ts) from (x union all append2)", sink);
        final String expectedMaxTimestamp = Chars.toString(sink);

        assertO3DataConsistencyStableSort(
                engine,
                compiler,
                ectx,
                "create table y as (select * from z union all append1 union all append2)",
                "insert into x select * from append2"
        );

        assertXCount(compiler, ectx);

        assertMaxTimestamp(
                engine,
                expectedMaxTimestamp
        );
    }

    private static void testColumnTopLastOOOPrefix0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 0 as commit," +
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
                        " timestamp_sequence(500000000000L,330000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1,40,1) varc," +
                        " rnd_varchar(1,1,0) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("alter table x add column v double", sqlExecutionContext);
        engine.execute("alter table x add column v1 float", sqlExecutionContext);
        engine.execute("alter table x add column v2 int", sqlExecutionContext);
        engine.execute("alter table x add column v3 byte", sqlExecutionContext);
        engine.execute("alter table x add column v4 short", sqlExecutionContext);
        engine.execute("alter table x add column v5 boolean", sqlExecutionContext);
        engine.execute("alter table x add column v6 date", sqlExecutionContext);
        engine.execute("alter table x add column v7 timestamp", sqlExecutionContext);
        engine.execute("alter table x add column v8 symbol", sqlExecutionContext);
        engine.execute("alter table x add column v10 char", sqlExecutionContext);
        engine.execute("alter table x add column v11 string", sqlExecutionContext);
        engine.execute("alter table x add column v12 binary", sqlExecutionContext);
        engine.execute("alter table x add column v13 long256", sqlExecutionContext);
        engine.execute("alter table x add column v9 long", sqlExecutionContext);
        engine.execute("alter table x add column v14 varchar", sqlExecutionContext);
        engine.execute("alter table x add column v15 varchar", sqlExecutionContext);
        engine.execute("alter table x add column arr2 double[][]", sqlExecutionContext);

        engine.execute("create table w as (select * from x)", sqlExecutionContext);

        engine.execute(
                "create table append1 as (" +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(664670000000L,10000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1,40,1) varc," +
                        " rnd_varchar(1,1,0) varc2," +
                        " rnd_double_array(2,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long256() v13," +
                        " rnd_long() v9," +
                        " rnd_varchar(1,40,1) v14," +
                        " rnd_varchar(1,1,0) v15," +
                        " rnd_double_array(2,1) arr2," +
                        " from long_sequence(500)" +
                        ")",
                sqlExecutionContext
        );

        engine.execute(
                "create table append2 as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(604800000000L,10000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1,40,1) varc," +
                        " rnd_varchar(1,1,0) varc2," +
                        " rnd_double_array(2,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long256() v13," +
                        " rnd_long() v9," +
                        " rnd_varchar(1,40,1) v14," +
                        " rnd_varchar(1,1,0) v15," +
                        " rnd_double_array(2,1) arr2," +
                        " from long_sequence(400)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("insert into x select * from append1", sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select count() from (x union all append2)", sink2);
        TestUtils.printSql(compiler, sqlExecutionContext, "select max(ts) from (x union all append2)", sink);

        final String expectedMaxTimestamp = Chars.toString(sink);

        assertO3DataConsistencyStableSort(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (select * from w union all append1 union all append2)",
                "insert into x select * from append2"
        );

        assertXCount(compiler, sqlExecutionContext);
        assertMaxTimestamp(engine, expectedMaxTimestamp);
    }

    private static void testColumnTopMidAppendBlankColumn0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String timestampTypeName
    ) throws SqlException {
        // create table with roughly 2AM data
        engine.execute(
                "create table x as (" +
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
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                executionContext
        );

        engine.execute("alter table x add column v double", executionContext);
        engine.execute("alter table x add column v1 float", executionContext);
        engine.execute("alter table x add column v2 int", executionContext);
        engine.execute("alter table x add column v3 byte", executionContext);
        engine.execute("alter table x add column v4 short", executionContext);
        engine.execute("alter table x add column v5 boolean", executionContext);
        engine.execute("alter table x add column v6 date", executionContext);
        engine.execute("alter table x add column v7 timestamp", executionContext);
        engine.execute("alter table x add column v8 symbol", executionContext);
        engine.execute("alter table x add column v10 char", executionContext);
        engine.execute("alter table x add column v11 string", executionContext);
        engine.execute("alter table x add column v12 binary", executionContext);
        engine.execute("alter table x add column v13 long256", executionContext);
        engine.execute("alter table x add column v9 long", executionContext);
        engine.execute("alter table x add column v14 varchar", executionContext);
        engine.execute("alter table x add column v15 varchar", executionContext);
        engine.execute("alter table x add column arr2 double[][]", executionContext);

        engine.execute(
                "create table append as (" +
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
                        " timestamp_sequence(518300000010L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        //  ------------------- new columns ------------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long256() v13," +
                        " rnd_long() v9," +
                        " rnd_varchar(1, 40, 1) v14," +
                        " rnd_varchar(1, 1, 1) v15," +
                        " rnd_double_array(2,1) arr2," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        // create third table, which will contain both X and 1AM
        assertO3DataCursors(
                engine,
                compiler,
                executionContext,
                "create table y as (x union all append)",
                "y order by ts",
                "insert into x select * from append",
                "x",
                "y",
                "x"
        );

        assertIndexConsistency(compiler, executionContext, engine);
        assertXCountY(engine, compiler, executionContext);
    }

    private static void testColumnTopMidAppendColumn0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 0 as commit," +
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
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 30, 1) varc," +
                        " rnd_varchar(1, 1, 0) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("alter table x add column v double", sqlExecutionContext);
        engine.execute("alter table x add column v1 float", sqlExecutionContext);
        engine.execute("alter table x add column v2 int", sqlExecutionContext);
        engine.execute("alter table x add column v3 byte", sqlExecutionContext);
        engine.execute("alter table x add column v4 short", sqlExecutionContext);
        engine.execute("alter table x add column v5 boolean", sqlExecutionContext);
        engine.execute("alter table x add column v6 date", sqlExecutionContext);
        engine.execute("alter table x add column v7 timestamp", sqlExecutionContext);
        engine.execute("alter table x add column v8 symbol", sqlExecutionContext);
        engine.execute("alter table x add column v10 char", sqlExecutionContext);
        engine.execute("alter table x add column v11 string", sqlExecutionContext);
        engine.execute("alter table x add column v12 binary", sqlExecutionContext);
        engine.execute("alter table x add column v9 long", sqlExecutionContext);
        engine.execute("alter table x add column v13 long256", sqlExecutionContext);
        engine.execute("alter table x add column v14 varchar", sqlExecutionContext);
        engine.execute("alter table x add column v15 varchar", sqlExecutionContext);
        engine.execute("alter table x add column arr2 double[]", sqlExecutionContext);

        engine.execute("create table w as (select * from x)", sqlExecutionContext);

        engine.execute(
                "create table append1 as (" +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(549900000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 30, 1) varc," +
                        " rnd_varchar(1, 1, 0) varc2," +
                        " rnd_double_array(1,1) arr," +
                        // ---- new columns ----
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_long256() v13," +
                        " rnd_varchar(1, 30, 1) v14," +
                        " rnd_varchar(1, 1, 0) v15," +
                        " rnd_double_array(1,1) arr2," +
                        " from long_sequence(1000)" +
                        ")",
                sqlExecutionContext
        );

        engine.execute(
                "create table append2 as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(604700000001,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 30, 1) varc," +
                        " rnd_varchar(1, 1, 0) varc2," +
                        " rnd_double_array(1,1) arr," +
                        // --------- new columns -----------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_long256() v13," +
                        " rnd_varchar(1, 30, 1) v14," +
                        " rnd_varchar(1, 1, 0) v15," +
                        " rnd_double_array(1,1) arr2," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("insert into x select * from append1", sqlExecutionContext);
        assertO3DataConsistencyStableSort(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (w union all append1 union all append2)",
                "insert into x select * from append2"
        );

        assertIndexConsistency(compiler, sqlExecutionContext, engine);
        assertXCountY(engine, compiler, sqlExecutionContext);
    }

    private static void testColumnTopMidDataMergeData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 0 as commit," +
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
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                executionContext
        );

        engine.execute("alter table x add column v double", executionContext);
        engine.execute("alter table x add column v1 float", executionContext);
        engine.execute("alter table x add column v2 int", executionContext);
        engine.execute("alter table x add column v3 byte", executionContext);
        engine.execute("alter table x add column v4 short", executionContext);
        engine.execute("alter table x add column v5 boolean", executionContext);
        engine.execute("alter table x add column v6 date", executionContext);
        engine.execute("alter table x add column v7 timestamp", executionContext);
        engine.execute("alter table x add column v8 symbol", executionContext);
        engine.execute("alter table x add column v10 char", executionContext);
        engine.execute("alter table x add column v11 string", executionContext);
        engine.execute("alter table x add column v12 binary", executionContext);
        engine.execute("alter table x add column v9 long", executionContext);
        engine.execute("alter table x add column v13 varchar", executionContext);
        engine.execute("alter table x add column v14 varchar", executionContext);
        engine.execute("alter table x add column arr2 double[]", executionContext);

        engine.execute("create table w as (select * from x)", executionContext);

        engine.execute(
                "create table append1 as ( " +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(549920000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(1,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1, 40, 1) v13," +
                        " rnd_varchar(1, 1, 1) v14," +
                        " rnd_double_array(1,1) arr2," +
                        " from long_sequence(1000)" +
                        ")",
                executionContext
        );

        engine.execute(
                "create table append2 as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(549900000000L,50000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(1,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1, 40, 1) v13," +
                        " rnd_varchar(1, 1, 1) v14," +
                        " rnd_double_array(1,1) arr2," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        engine.execute("insert into x select * from append1", executionContext);

        TestUtils.printSql(compiler, executionContext, "select count() from (x union all append2)", sink2);
        TestUtils.printSql(compiler, executionContext, "select max(ts) from (x union all append2)", sink);
        final String expectedMaxTimestamp = Chars.toString(sink);

        assertO3DataConsistencyStableSort(
                engine,
                compiler,
                executionContext,
                "create table y as (select * from w union all append1 union all append2)",
                "insert into x select * from append2"
        );

        assertXCount(compiler, executionContext);

        assertMaxTimestamp(
                engine,
                expectedMaxTimestamp
        );
    }

    private static void testColumnTopMidMergeBlankColumn0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
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
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                executionContext
        );

        engine.execute("alter table x add column v double", executionContext);
        engine.execute("alter table x add column v1 float", executionContext);
        engine.execute("alter table x add column v2 int", executionContext);
        engine.execute("alter table x add column v3 byte", executionContext);
        engine.execute("alter table x add column v4 short", executionContext);
        engine.execute("alter table x add column v5 boolean", executionContext);
        engine.execute("alter table x add column v6 date", executionContext);
        engine.execute("alter table x add column v7 timestamp", executionContext);
        engine.execute("alter table x add column v8 symbol index", executionContext);
        engine.execute("alter table x add column v10 char", executionContext);
        engine.execute("alter table x add column v11 string", executionContext);
        engine.execute("alter table x add column v12 binary", executionContext);
        engine.execute("alter table x add column v13 long256", executionContext);
        engine.execute("alter table x add column v9 long", executionContext);
        engine.execute("alter table x add column v14 varchar", executionContext);
        engine.execute("alter table x add column v15 varchar", executionContext);
        engine.execute("alter table x add column arr2 double[][]", executionContext);

        engine.execute(
                "create table append as (" +
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
                        " timestamp_sequence(518300000000L-1000L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        //  ------------------- new columns ------------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long256() v13," +
                        " rnd_long() v9," +
                        " rnd_varchar(1, 40, 1) v14," +
                        " rnd_varchar(1, 1, 1) v15," +
                        " rnd_double_array(2,1) arr2," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        // create third table, which will contain both X and 1AM
        assertO3DataCursors(
                engine,
                compiler,
                executionContext,
                "create table y as (x union all append)",
                "y order by ts",
                "insert into x select * from append",
                "x",
                "y",
                "x"
        );

        assertIndexConsistency(compiler, executionContext, engine);
        assertXCountY(engine, compiler, executionContext);
    }

    private static void testColumnTopMidMergeBlankColumnGeoHash0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_geohash(4) geo1," +
                        " rnd_geohash(15) geo2," +
                        " rnd_geohash(16) geo4," +
                        " rnd_geohash(40) geo8" +
                        " from long_sequence(500)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        engine.execute("alter table x add column v1 geohash(1c)", executionContext);
        engine.execute("alter table x add column v2 geohash(2c)", executionContext);
        engine.execute("alter table x add column v4 geohash(4c)", executionContext);
        engine.execute("alter table x add column v8 geohash(10c)", executionContext);

        engine.execute(
                "create table append as (" +
                        "select" +
                        " cast(x as int) i," +
                        " timestamp_sequence(518300000000L-1000L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_geohash(4) geo1," +
                        " rnd_geohash(15) geo2," +
                        " rnd_geohash(16) geo4," +
                        " rnd_geohash(40) geo8," +
                        //  ------------------- new columns ------------------
                        " rnd_geohash(5) v1," +
                        " rnd_geohash(10) v2," +
                        " rnd_geohash(20) v4," +
                        " rnd_geohash(50) v8" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        // create third table, which will contain both X and 1AM
        assertO3DataCursors(
                engine,
                compiler,
                executionContext,
                "create table y as (x union all append)",
                "y order by ts",
                "insert into x select * from append",
                "x",
                "y",
                "x"
        );
        assertXCountY(engine, compiler, executionContext);
    }

    private static void testColumnTopMidOOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 0 as commit," +
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
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("alter table x add column v double", sqlExecutionContext);
        engine.execute("alter table x add column v1 float", sqlExecutionContext);
        engine.execute("alter table x add column v2 int", sqlExecutionContext);
        engine.execute("alter table x add column v3 byte", sqlExecutionContext);
        engine.execute("alter table x add column v4 short", sqlExecutionContext);
        engine.execute("alter table x add column v5 boolean", sqlExecutionContext);
        engine.execute("alter table x add column v6 date", sqlExecutionContext);
        engine.execute("alter table x add column v7 timestamp", sqlExecutionContext);
        engine.execute("alter table x add column v8 symbol", sqlExecutionContext);
        engine.execute("alter table x add column v10 char", sqlExecutionContext);
        engine.execute("alter table x add column v11 string", sqlExecutionContext);
        engine.execute("alter table x add column v12 binary", sqlExecutionContext);
        engine.execute("alter table x add column v9 long", sqlExecutionContext);
        engine.execute("alter table x add column v13 varchar", sqlExecutionContext);
        engine.execute("alter table x add column v14 varchar", sqlExecutionContext);
        engine.execute("alter table x add column arr2 double[][]", sqlExecutionContext);

        engine.execute("create table w as (select * from x)", sqlExecutionContext);

        engine.execute(
                "create table append1 as (" +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(549920000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1, 40, 1) v13," +
                        " rnd_varchar(1, 1, 1) v14," +
                        " rnd_double_array(2,1) arr2," +
                        " from long_sequence(1000)" +
                        ")",
                sqlExecutionContext
        );

        engine.execute(
                "create table append2 as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(546600000000L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1, 40, 1) v13," +
                        " rnd_varchar(1, 1, 1) v14," +
                        " rnd_double_array(2,1) arr2," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("insert into x select * from append1", sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select count() from (x union all append2)", sink2);
        TestUtils.printSql(compiler, sqlExecutionContext, "select max(ts) from (x union all append2)", sink);
        final String expectedMaxTimestamp = Chars.toString(sink);

        assertO3DataConsistencyStableSort(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (select * from w union all append1 union all append2)",
                "insert into x select * from append2"
        );

        assertXCount(compiler, sqlExecutionContext);

        assertMaxTimestamp(
                engine,
                expectedMaxTimestamp
        );
    }

    private static void testColumnTopMidOOODataUtf80(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table 'привет от штиблет' as (" +
                        "select" +
                        " 0 as commit," +
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
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("alter table 'привет от штиблет' add column v double", sqlExecutionContext);
        engine.execute("alter table 'привет от штиблет' add column v1 float", sqlExecutionContext);
        engine.execute("alter table 'привет от штиблет' add column v2 int", sqlExecutionContext);
        engine.execute("alter table 'привет от штиблет' add column v3 byte", sqlExecutionContext);
        engine.execute("alter table 'привет от штиблет' add column v4 short", sqlExecutionContext);
        engine.execute("alter table 'привет от штиблет' add column v5 boolean", sqlExecutionContext);
        engine.execute("alter table 'привет от штиблет' add column v6 date", sqlExecutionContext);
        engine.execute("alter table 'привет от штиблет' add column v7 timestamp", sqlExecutionContext);
        engine.execute("alter table 'привет от штиблет' add column v8 symbol", sqlExecutionContext);
        engine.execute("alter table 'привет от штиблет' add column v10 char", sqlExecutionContext);
        engine.execute("alter table 'привет от штиблет' add column v11 string", sqlExecutionContext);
        engine.execute("alter table 'привет от штиблет' add column v12 binary", sqlExecutionContext);
        engine.execute("alter table 'привет от штиблет' add column v9 long", sqlExecutionContext);
        engine.execute("alter table 'привет от штиблет' add column v13 varchar", sqlExecutionContext);
        engine.execute("alter table 'привет от штиблет' add column v14 varchar", sqlExecutionContext);
        engine.execute("alter table 'привет от штиблет' add column arr2 double[][]", sqlExecutionContext);

        engine.execute("create table w as (select * from 'привет от штиблет')", sqlExecutionContext);

        engine.execute(
                "create table append1 as (" +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(549920000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1, 40, 1) v13," +
                        " rnd_varchar(1, 1, 1) v14," +
                        " rnd_double_array(2,1) arr2," +
                        " from long_sequence(1000)" +
                        ")",
                sqlExecutionContext
        );

        engine.execute(
                "create table append2 as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(546600000000L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1, 40, 1) v13," +
                        " rnd_varchar(1, 1, 1) v14," +
                        " rnd_double_array(2,1) arr2," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("insert into 'привет от штиблет' select * from append1", sqlExecutionContext);

        TestUtils.printSql(compiler, sqlExecutionContext, "select count() from ('привет от штиблет' union all append2)", sink2);
        TestUtils.printSql(compiler, sqlExecutionContext, "select max(ts) from ('привет от штиблет' union all append2)", sink);
        final String expectedMaxTimestamp = Chars.toString(sink);

        engine.execute("insert into 'привет от штиблет' select * from append2", sqlExecutionContext);
        assertO3DataConsistencyStableSort(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (select * from w union all append1 union all append2)",
                "create table x as (select * from 'привет от штиблет')"
        );

        engine.print("select count() from 'привет от штиблет'", sink, sqlExecutionContext);
        TestUtils.assertEquals(sink2, sink);

        try (
                final TableWriter w = TestUtils.getWriter(engine, "привет от штиблет")
        ) {
            sink.clear();
            sink.put("max\n");
            TimestampDriver driver = ColumnType.getTimestampDriver(w.getTimestampType());
            driver.append(sink, w.getMaxTimestamp());
            sink.put('\n');
            TestUtils.assertEquals(expectedMaxTimestamp, sink);
            Assert.assertEquals(0, w.getO3RowCount());
        }
    }

    private static void testColumnTopMoveUncommitted0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {

        //
        // ----- prev to last partition
        //
        // +---------------+
        // |   committed   |
        // |               |
        // +---------------+ <---- top
        // |               |
        // |  uncommitted  | <----- last partition boundary
        // |               |
        // +---------------+
        // |       O3      |
        // |               |
        // |               |
        // +---------------+
        engine.execute(
                "create table x as (" +
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
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("alter table x add column v double", sqlExecutionContext);
        engine.execute("alter table x add column v1 float", sqlExecutionContext);
        engine.execute("alter table x add column v2 int", sqlExecutionContext);
        engine.execute("alter table x add column v3 byte", sqlExecutionContext);
        engine.execute("alter table x add column v4 short", sqlExecutionContext);
        engine.execute("alter table x add column v5 boolean", sqlExecutionContext);
        engine.execute("alter table x add column v6 date", sqlExecutionContext);
        engine.execute("alter table x add column v7 timestamp", sqlExecutionContext);
        engine.execute("alter table x add column v8 symbol", sqlExecutionContext);
        engine.execute("alter table x add column v10 char", sqlExecutionContext);
        engine.execute("alter table x add column v11 string", sqlExecutionContext);
        engine.execute("alter table x add column v12 binary", sqlExecutionContext);
        engine.execute("alter table x add column v9 long", sqlExecutionContext);
        engine.execute("alter table x add column v13 varchar", sqlExecutionContext);
        engine.execute("alter table x add column v14 varchar", sqlExecutionContext);
        engine.execute("alter table x add column arr2 double[][]", sqlExecutionContext);

        // prepare data in APPEND table (non-partitioned, unordered) such, that these rows cover the rest of the
        // last partition and will cause a new partition with uncommitted data to be created
        engine.execute(
                "create table append as (" +
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
                        " timestamp_sequence(549900000001L,1000000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1, 40, 1) v13," +
                        " rnd_varchar(1, 1, 1) v14," +
                        " rnd_double_array(2,1) arr2," +
                        " from long_sequence(100)" +
                        ")",
                sqlExecutionContext
        );

        // Insert the O3 data, the conventional O3 logic should be triggered because this table does not
        // have designated timestamp. Space out O3 such that it hits the last partition.

        //        --------     new columns here ---------------
        engine.execute(
                "insert into append " +
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
                        " cast(639900000001L + ((x-1)/4) * 1000L + (4-(x-1)%4)*89 as timestamp)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1, 40, 1) v13," +
                        " rnd_varchar(1, 1, 1) v14," +
                        " rnd_double_array(2,1) arr2," +
                        " from long_sequence(100)", sqlExecutionContext
        );

        // copy "x" away before we modify it, get rid of timestamp by ordering table differently
        engine.execute("create table y as (x order by 1)", sqlExecutionContext);
        // add "append" table to the mix
        engine.execute("insert into y select * from append", sqlExecutionContext);

        TestUtils.printSql(
                compiler,
                sqlExecutionContext,
                "y order by ts",
                sink
        );

        String expected = Chars.toString(sink);

        engine.execute("insert into x select * from append", sqlExecutionContext);

        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "x",
                sink,
                expected
        );

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );
    }

    private static void testColumnTopMoveUncommittedLastPart0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {

        //
        // ----- last partition
        //
        // +---------------+
        // |   committed   |
        // |               |
        // +---------------+ <---- top
        // |               |
        // |  uncommitted  |
        // |               |
        // +---------------+
        // |       O3      |
        // |               |
        // |               |
        // +---------------+
        engine.execute(
                "create table x as (" +
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
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("alter table x add column v double", sqlExecutionContext);
        engine.execute("alter table x add column v1 float", sqlExecutionContext);
        engine.execute("alter table x add column v2 int", sqlExecutionContext);
        engine.execute("alter table x add column v3 byte", sqlExecutionContext);
        engine.execute("alter table x add column v4 short", sqlExecutionContext);
        engine.execute("alter table x add column v5 boolean", sqlExecutionContext);
        engine.execute("alter table x add column v6 date", sqlExecutionContext);
        engine.execute("alter table x add column v7 timestamp", sqlExecutionContext);
        engine.execute("alter table x add column v8 symbol", sqlExecutionContext);
        engine.execute("alter table x add column v10 char", sqlExecutionContext);
        engine.execute("alter table x add column v11 string", sqlExecutionContext);
        engine.execute("alter table x add column v12 binary", sqlExecutionContext);
        engine.execute("alter table x add column v9 long", sqlExecutionContext);
        engine.execute("alter table x add column v13 varchar", sqlExecutionContext);
        engine.execute("alter table x add column v14 varchar", sqlExecutionContext);
        engine.execute("alter table x add column arr2 double[][]", sqlExecutionContext);

        // prepare data in APPEND table (non-partitioned, unordered) such, that first 100 rows are in order
        // and another 100 rows are out of order
        engine.execute(
                "create table append as (" +
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
                        " timestamp_sequence(549900000001L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1, 40, 1) v13," +
                        " rnd_varchar(1, 1, 1) v14," +
                        " rnd_double_array(2,1) arr2," +
                        " from long_sequence(100)" +
                        ")",
                sqlExecutionContext
        );

        // insert the O3 data, the conventional O3 logic should be triggered because this table does not
        // have designated timestamp

        //        --------     new columns here ---------------
        engine.execute(
                "insert into append " +
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
                        " cast(549900000001L + ((x-1)/4) * 1000L + (4-(x-1)%4)*89 as timestamp)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
//        --------     new columns here ---------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1, 40, 1) v13," +
                        " rnd_varchar(1, 1, 1) v14," +
                        " rnd_double_array(2,1) arr2," +
                        " from long_sequence(100)", sqlExecutionContext
        );

        // copy "x" away before we modify it, get rid of timestamp by ordering table differently
        engine.execute("create table y as (x order by 1)", sqlExecutionContext);
        // add "append" table to the mix
        engine.execute("insert into y select * from append", sqlExecutionContext);

        TestUtils.printSql(
                compiler,
                sqlExecutionContext,
                "y order by ts",
                sink
        );

        String expected = Chars.toString(sink);

        engine.execute("insert into x select * from append", sqlExecutionContext);

        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "x",
                sink,
                expected
        );

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );
    }

    private static void testColumnTopNewPartitionMiddleOfTable0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        // 1970-01-06
        engine.execute(
                "create table x as (" +
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
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // 1970-01-09
        engine.execute(
                "insert into x " +
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
                        " timestamp_sequence(700000000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)", sqlExecutionContext
        );

        engine.execute("alter table x add column v double", sqlExecutionContext);
        engine.execute("alter table x add column v1 float", sqlExecutionContext);
        engine.execute("alter table x add column v2 int", sqlExecutionContext);
        engine.execute("alter table x add column v3 byte", sqlExecutionContext);
        engine.execute("alter table x add column v4 short", sqlExecutionContext);
        engine.execute("alter table x add column v5 boolean", sqlExecutionContext);
        engine.execute("alter table x add column v6 date", sqlExecutionContext);
        engine.execute("alter table x add column v7 timestamp", sqlExecutionContext);
        engine.execute("alter table x add column v8 symbol index", sqlExecutionContext);
        engine.execute("alter table x add column v10 char", sqlExecutionContext);
        engine.execute("alter table x add column v11 string", sqlExecutionContext);
        engine.execute("alter table x add column v12 binary", sqlExecutionContext);
        engine.execute("alter table x add column v9 long", sqlExecutionContext);
        engine.execute("alter table x add column v13 varchar", sqlExecutionContext);
        engine.execute("alter table x add column v14 varchar", sqlExecutionContext);
        engine.execute("alter table x add column arr2 double[][]", sqlExecutionContext);

        // 1970-01-08
        engine.execute(
                "create table append as (" +
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
                        " timestamp_sequence(610000000000L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        //  ------------------- new columns ------------------
                        " rnd_double() v," +
                        " rnd_float() v1," +
                        " rnd_int() v2," +
                        " rnd_byte() v3," +
                        " rnd_short() v4," +
                        " rnd_boolean() v5," +
                        " rnd_date() v6," +
                        " rnd_timestamp(10,100000,356) v7," +
                        " rnd_symbol('AAA','BBB', null) v8," +
                        " rnd_char() v10," +
                        " rnd_str() v11," +
                        " rnd_bin() v12," +
                        " rnd_long() v9," +
                        " rnd_varchar(1, 40, 1) v13," +
                        " rnd_varchar(1, 1, 1) v14," +
                        " rnd_double_array(2,1) arr2," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "y order by ts",
                "insert into x select * from append",
                "x",
                "y",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext, engine);
        assertXCountY(engine, compiler, sqlExecutionContext);
    }

    private static void testDecimalO3Insert0(CairoEngine engine,
                                             SqlCompiler compiler,
                                             SqlExecutionContext sqlExecutionContext,
                                             String timestampTypeName) throws SqlException {
        engine.execute(
                "create table x (" +
                        " id int," +
                        " dec8 decimal(2,1)," +
                        " dec16 decimal(4,2)," +
                        " dec32 decimal(8,2)," +
                        " dec64 decimal(15,4)," +
                        " dec128 decimal(30,6)," +
                        " dec256 decimal(56,8)," +
                        " ts " + timestampTypeName +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        try (TableWriter w = TestUtils.getWriter(engine, "x")) {
            TimestampDriver driver = ColumnType.getTimestampDriver(w.getTimestampType());

            long factor = w.getTimestampType() == ColumnType.TIMESTAMP_MICRO ? 1 : 1000;
            // Insert ordered data first
            long baseTs = driver.parseFloorLiteral("2022-01-01T00:00:00.000Z");
            for (int i = 0; i < 100; i++) {
                TableWriter.Row r = w.newRow(baseTs + i * 1000000L * factor);
                r.putInt(0, i);
                WriterRowUtils.putDecimal(1, Decimal256.fromLong(i, 1), ColumnType.getDecimalType(2, 1), r);
                WriterRowUtils.putDecimal(2, Decimal256.fromLong(i * 100 + i, 2), ColumnType.getDecimalType(4, 2), r);
                WriterRowUtils.putDecimal(3, Decimal256.fromLong(i * 10_000 + i, 2), ColumnType.getDecimalType(8, 2), r);
                WriterRowUtils.putDecimal(4, Decimal256.fromLong(i * 10_000_000 + i, 4), ColumnType.getDecimalType(15, 4), r);
                WriterRowUtils.putDecimal(5, Decimal256.fromLong(i * 100_000_000L + i, 6), ColumnType.getDecimalType(30, 6), r);
                WriterRowUtils.putDecimal(6, Decimal256.fromLong(i * 1_000_000_000L + i, 8), ColumnType.getDecimalType(56, 8), r);
                r.append();
            }
            w.commit();

            // Now insert O3 data in the middle and beginning
            for (int i = 0; i < 50; i++) {
                // Insert in the middle (O3)
                TableWriter.Row r = w.newRow(baseTs + (25 + i) * 500000L * factor);
                r.putInt(0, 10000 + i);
                WriterRowUtils.putDecimal(1, Decimal256.fromLong(i, 1), ColumnType.getDecimalType(2, 1), r);
                WriterRowUtils.putDecimal(2, Decimal256.fromLong(i * 100 + i + 100, 2), ColumnType.getDecimalType(4, 2), r);
                WriterRowUtils.putDecimal(3, Decimal256.fromLong(i * 10_000 + i + 100, 2), ColumnType.getDecimalType(8, 2), r);
                WriterRowUtils.putDecimal(4, Decimal256.fromLong(i * 10_000_000 + i + 100, 4), ColumnType.getDecimalType(15, 4), r);
                WriterRowUtils.putDecimal(5, Decimal256.fromLong(i * 100_000_000L + i + 100, 6), ColumnType.getDecimalType(30, 6), r);
                WriterRowUtils.putDecimal(6, Decimal256.fromLong(i * 1_000_000_000L + i + 100, 8), ColumnType.getDecimalType(56, 8), r);
                r.append();
            }

            // Insert at the beginning (O3)
            for (int i = 0; i < 25; i++) {
                TableWriter.Row r = w.newRow(baseTs - (25 - i) * 1000000L * factor);
                r.putInt(0, 20000 + i);
                WriterRowUtils.putDecimal(1, Decimal256.fromLong(i, 1), ColumnType.getDecimalType(2, 1), r);
                WriterRowUtils.putDecimal(2, Decimal256.fromLong(i * 100 + i + 200, 2), ColumnType.getDecimalType(4, 2), r);
                WriterRowUtils.putDecimal(3, Decimal256.fromLong(i * 10_000 + i + 200, 2), ColumnType.getDecimalType(8, 2), r);
                WriterRowUtils.putDecimal(4, Decimal256.fromLong(i * 10_000_000 + i + 200, 4), ColumnType.getDecimalType(15, 4), r);
                WriterRowUtils.putDecimal(5, Decimal256.fromLong(i * 100_000_000L + i + 200, 6), ColumnType.getDecimalType(30, 6), r);
                WriterRowUtils.putDecimal(6, Decimal256.fromLong(i * 1_000_000_000L + i + 200, 8), ColumnType.getDecimalType(56, 8), r);
                r.append();
            }

            w.commit();
        }

        String suffix = timestampTypeName.endsWith("_NS") ? "000Z\n" : "Z\n";
        TestUtils.assertSql(
                compiler, sqlExecutionContext,
                "select * from x",
                sink, "id\tdec8\tdec16\tdec32\tdec64\tdec128\tdec256\tts\n" +
                        "20000\t0.0\t2.00\t2.00\t0.0200\t0.000200\t0.00000200\t2021-12-31T23:59:35.000000" + suffix +
                        "20001\t0.1\t3.01\t102.01\t1000.0201\t100.000201\t10.00000201\t2021-12-31T23:59:36.000000" + suffix +
                        "20002\t0.2\t4.02\t202.02\t2000.0202\t200.000202\t20.00000202\t2021-12-31T23:59:37.000000" + suffix +
                        "20003\t0.3\t5.03\t302.03\t3000.0203\t300.000203\t30.00000203\t2021-12-31T23:59:38.000000" + suffix +
                        "20004\t0.4\t6.04\t402.04\t4000.0204\t400.000204\t40.00000204\t2021-12-31T23:59:39.000000" + suffix +
                        "20005\t0.5\t7.05\t502.05\t5000.0205\t500.000205\t50.00000205\t2021-12-31T23:59:40.000000" + suffix +
                        "20006\t0.6\t8.06\t602.06\t6000.0206\t600.000206\t60.00000206\t2021-12-31T23:59:41.000000" + suffix +
                        "20007\t0.7\t9.07\t702.07\t7000.0207\t700.000207\t70.00000207\t2021-12-31T23:59:42.000000" + suffix +
                        "20008\t0.8\t10.08\t802.08\t8000.0208\t800.000208\t80.00000208\t2021-12-31T23:59:43.000000" + suffix +
                        "20009\t0.9\t11.09\t902.09\t9000.0209\t900.000209\t90.00000209\t2021-12-31T23:59:44.000000" + suffix +
                        "20010\t1.0\t12.10\t1002.10\t10000.0210\t1000.000210\t100.00000210\t2021-12-31T23:59:45.000000" + suffix +
                        "20011\t1.1\t13.11\t1102.11\t11000.0211\t1100.000211\t110.00000211\t2021-12-31T23:59:46.000000" + suffix +
                        "20012\t1.2\t14.12\t1202.12\t12000.0212\t1200.000212\t120.00000212\t2021-12-31T23:59:47.000000" + suffix +
                        "20013\t1.3\t15.13\t1302.13\t13000.0213\t1300.000213\t130.00000213\t2021-12-31T23:59:48.000000" + suffix +
                        "20014\t1.4\t16.14\t1402.14\t14000.0214\t1400.000214\t140.00000214\t2021-12-31T23:59:49.000000" + suffix +
                        "20015\t1.5\t17.15\t1502.15\t15000.0215\t1500.000215\t150.00000215\t2021-12-31T23:59:50.000000" + suffix +
                        "20016\t1.6\t18.16\t1602.16\t16000.0216\t1600.000216\t160.00000216\t2021-12-31T23:59:51.000000" + suffix +
                        "20017\t1.7\t19.17\t1702.17\t17000.0217\t1700.000217\t170.00000217\t2021-12-31T23:59:52.000000" + suffix +
                        "20018\t1.8\t20.18\t1802.18\t18000.0218\t1800.000218\t180.00000218\t2021-12-31T23:59:53.000000" + suffix +
                        "20019\t1.9\t21.19\t1902.19\t19000.0219\t1900.000219\t190.00000219\t2021-12-31T23:59:54.000000" + suffix +
                        "20020\t2.0\t22.20\t2002.20\t20000.0220\t2000.000220\t200.00000220\t2021-12-31T23:59:55.000000" + suffix +
                        "20021\t2.1\t23.21\t2102.21\t21000.0221\t2100.000221\t210.00000221\t2021-12-31T23:59:56.000000" + suffix +
                        "20022\t2.2\t24.22\t2202.22\t22000.0222\t2200.000222\t220.00000222\t2021-12-31T23:59:57.000000" + suffix +
                        "20023\t2.3\t25.23\t2302.23\t23000.0223\t2300.000223\t230.00000223\t2021-12-31T23:59:58.000000" + suffix +
                        "20024\t2.4\t26.24\t2402.24\t24000.0224\t2400.000224\t240.00000224\t2021-12-31T23:59:59.000000" + suffix +
                        "0\t0.0\t0.00\t0.00\t0.0000\t0.000000\t0.00000000\t2022-01-01T00:00:00.000000" + suffix +
                        "1\t0.1\t1.01\t100.01\t1000.0001\t100.000001\t10.00000001\t2022-01-01T00:00:01.000000" + suffix +
                        "2\t0.2\t2.02\t200.02\t2000.0002\t200.000002\t20.00000002\t2022-01-01T00:00:02.000000" + suffix +
                        "3\t0.3\t3.03\t300.03\t3000.0003\t300.000003\t30.00000003\t2022-01-01T00:00:03.000000" + suffix +
                        "4\t0.4\t4.04\t400.04\t4000.0004\t400.000004\t40.00000004\t2022-01-01T00:00:04.000000" + suffix +
                        "5\t0.5\t5.05\t500.05\t5000.0005\t500.000005\t50.00000005\t2022-01-01T00:00:05.000000" + suffix +
                        "6\t0.6\t6.06\t600.06\t6000.0006\t600.000006\t60.00000006\t2022-01-01T00:00:06.000000" + suffix +
                        "7\t0.7\t7.07\t700.07\t7000.0007\t700.000007\t70.00000007\t2022-01-01T00:00:07.000000" + suffix +
                        "8\t0.8\t8.08\t800.08\t8000.0008\t800.000008\t80.00000008\t2022-01-01T00:00:08.000000" + suffix +
                        "9\t0.9\t9.09\t900.09\t9000.0009\t900.000009\t90.00000009\t2022-01-01T00:00:09.000000" + suffix +
                        "10\t1.0\t10.10\t1000.10\t10000.0010\t1000.000010\t100.00000010\t2022-01-01T00:00:10.000000" + suffix +
                        "11\t1.1\t11.11\t1100.11\t11000.0011\t1100.000011\t110.00000011\t2022-01-01T00:00:11.000000" + suffix +
                        "12\t1.2\t12.12\t1200.12\t12000.0012\t1200.000012\t120.00000012\t2022-01-01T00:00:12.000000" + suffix +
                        "10000\t0.0\t1.00\t1.00\t0.0100\t0.000100\t0.00000100\t2022-01-01T00:00:12.500000" + suffix +
                        "13\t1.3\t13.13\t1300.13\t13000.0013\t1300.000013\t130.00000013\t2022-01-01T00:00:13.000000" + suffix +
                        "10001\t0.1\t2.01\t101.01\t1000.0101\t100.000101\t10.00000101\t2022-01-01T00:00:13.000000" + suffix +
                        "10002\t0.2\t3.02\t201.02\t2000.0102\t200.000102\t20.00000102\t2022-01-01T00:00:13.500000" + suffix +
                        "14\t1.4\t14.14\t1400.14\t14000.0014\t1400.000014\t140.00000014\t2022-01-01T00:00:14.000000" + suffix +
                        "10003\t0.3\t4.03\t301.03\t3000.0103\t300.000103\t30.00000103\t2022-01-01T00:00:14.000000" + suffix +
                        "10004\t0.4\t5.04\t401.04\t4000.0104\t400.000104\t40.00000104\t2022-01-01T00:00:14.500000" + suffix +
                        "15\t1.5\t15.15\t1500.15\t15000.0015\t1500.000015\t150.00000015\t2022-01-01T00:00:15.000000" + suffix +
                        "10005\t0.5\t6.05\t501.05\t5000.0105\t500.000105\t50.00000105\t2022-01-01T00:00:15.000000" + suffix +
                        "10006\t0.6\t7.06\t601.06\t6000.0106\t600.000106\t60.00000106\t2022-01-01T00:00:15.500000" + suffix +
                        "16\t1.6\t16.16\t1600.16\t16000.0016\t1600.000016\t160.00000016\t2022-01-01T00:00:16.000000" + suffix +
                        "10007\t0.7\t8.07\t701.07\t7000.0107\t700.000107\t70.00000107\t2022-01-01T00:00:16.000000" + suffix +
                        "10008\t0.8\t9.08\t801.08\t8000.0108\t800.000108\t80.00000108\t2022-01-01T00:00:16.500000" + suffix +
                        "17\t1.7\t17.17\t1700.17\t17000.0017\t1700.000017\t170.00000017\t2022-01-01T00:00:17.000000" + suffix +
                        "10009\t0.9\t10.09\t901.09\t9000.0109\t900.000109\t90.00000109\t2022-01-01T00:00:17.000000" + suffix +
                        "10010\t1.0\t11.10\t1001.10\t10000.0110\t1000.000110\t100.00000110\t2022-01-01T00:00:17.500000" + suffix +
                        "18\t1.8\t18.18\t1800.18\t18000.0018\t1800.000018\t180.00000018\t2022-01-01T00:00:18.000000" + suffix +
                        "10011\t1.1\t12.11\t1101.11\t11000.0111\t1100.000111\t110.00000111\t2022-01-01T00:00:18.000000" + suffix +
                        "10012\t1.2\t13.12\t1201.12\t12000.0112\t1200.000112\t120.00000112\t2022-01-01T00:00:18.500000" + suffix +
                        "19\t1.9\t19.19\t1900.19\t19000.0019\t1900.000019\t190.00000019\t2022-01-01T00:00:19.000000" + suffix +
                        "10013\t1.3\t14.13\t1301.13\t13000.0113\t1300.000113\t130.00000113\t2022-01-01T00:00:19.000000" + suffix +
                        "10014\t1.4\t15.14\t1401.14\t14000.0114\t1400.000114\t140.00000114\t2022-01-01T00:00:19.500000" + suffix +
                        "20\t2.0\t20.20\t2000.20\t20000.0020\t2000.000020\t200.00000020\t2022-01-01T00:00:20.000000" + suffix +
                        "10015\t1.5\t16.15\t1501.15\t15000.0115\t1500.000115\t150.00000115\t2022-01-01T00:00:20.000000" + suffix +
                        "10016\t1.6\t17.16\t1601.16\t16000.0116\t1600.000116\t160.00000116\t2022-01-01T00:00:20.500000" + suffix +
                        "21\t2.1\t21.21\t2100.21\t21000.0021\t2100.000021\t210.00000021\t2022-01-01T00:00:21.000000" + suffix +
                        "10017\t1.7\t18.17\t1701.17\t17000.0117\t1700.000117\t170.00000117\t2022-01-01T00:00:21.000000" + suffix +
                        "10018\t1.8\t19.18\t1801.18\t18000.0118\t1800.000118\t180.00000118\t2022-01-01T00:00:21.500000" + suffix +
                        "22\t2.2\t22.22\t2200.22\t22000.0022\t2200.000022\t220.00000022\t2022-01-01T00:00:22.000000" + suffix +
                        "10019\t1.9\t20.19\t1901.19\t19000.0119\t1900.000119\t190.00000119\t2022-01-01T00:00:22.000000" + suffix +
                        "10020\t2.0\t21.20\t2001.20\t20000.0120\t2000.000120\t200.00000120\t2022-01-01T00:00:22.500000" + suffix +
                        "23\t2.3\t23.23\t2300.23\t23000.0023\t2300.000023\t230.00000023\t2022-01-01T00:00:23.000000" + suffix +
                        "10021\t2.1\t22.21\t2101.21\t21000.0121\t2100.000121\t210.00000121\t2022-01-01T00:00:23.000000" + suffix +
                        "10022\t2.2\t23.22\t2201.22\t22000.0122\t2200.000122\t220.00000122\t2022-01-01T00:00:23.500000" + suffix +
                        "24\t2.4\t24.24\t2400.24\t24000.0024\t2400.000024\t240.00000024\t2022-01-01T00:00:24.000000" + suffix +
                        "10023\t2.3\t24.23\t2301.23\t23000.0123\t2300.000123\t230.00000123\t2022-01-01T00:00:24.000000" + suffix +
                        "10024\t2.4\t25.24\t2401.24\t24000.0124\t2400.000124\t240.00000124\t2022-01-01T00:00:24.500000" + suffix +
                        "25\t2.5\t25.25\t2500.25\t25000.0025\t2500.000025\t250.00000025\t2022-01-01T00:00:25.000000" + suffix +
                        "10025\t2.5\t26.25\t2501.25\t25000.0125\t2500.000125\t250.00000125\t2022-01-01T00:00:25.000000" + suffix +
                        "10026\t2.6\t27.26\t2601.26\t26000.0126\t2600.000126\t260.00000126\t2022-01-01T00:00:25.500000" + suffix +
                        "26\t2.6\t26.26\t2600.26\t26000.0026\t2600.000026\t260.00000026\t2022-01-01T00:00:26.000000" + suffix +
                        "10027\t2.7\t28.27\t2701.27\t27000.0127\t2700.000127\t270.00000127\t2022-01-01T00:00:26.000000" + suffix +
                        "10028\t2.8\t29.28\t2801.28\t28000.0128\t2800.000128\t280.00000128\t2022-01-01T00:00:26.500000" + suffix +
                        "27\t2.7\t27.27\t2700.27\t27000.0027\t2700.000027\t270.00000027\t2022-01-01T00:00:27.000000" + suffix +
                        "10029\t2.9\t30.29\t2901.29\t29000.0129\t2900.000129\t290.00000129\t2022-01-01T00:00:27.000000" + suffix +
                        "10030\t3.0\t31.30\t3001.30\t30000.0130\t3000.000130\t300.00000130\t2022-01-01T00:00:27.500000" + suffix +
                        "28\t2.8\t28.28\t2800.28\t28000.0028\t2800.000028\t280.00000028\t2022-01-01T00:00:28.000000" + suffix +
                        "10031\t3.1\t32.31\t3101.31\t31000.0131\t3100.000131\t310.00000131\t2022-01-01T00:00:28.000000" + suffix +
                        "10032\t3.2\t33.32\t3201.32\t32000.0132\t3200.000132\t320.00000132\t2022-01-01T00:00:28.500000" + suffix +
                        "29\t2.9\t29.29\t2900.29\t29000.0029\t2900.000029\t290.00000029\t2022-01-01T00:00:29.000000" + suffix +
                        "10033\t3.3\t34.33\t3301.33\t33000.0133\t3300.000133\t330.00000133\t2022-01-01T00:00:29.000000" + suffix +
                        "10034\t3.4\t35.34\t3401.34\t34000.0134\t3400.000134\t340.00000134\t2022-01-01T00:00:29.500000" + suffix +
                        "30\t3.0\t30.30\t3000.30\t30000.0030\t3000.000030\t300.00000030\t2022-01-01T00:00:30.000000" + suffix +
                        "10035\t3.5\t36.35\t3501.35\t35000.0135\t3500.000135\t350.00000135\t2022-01-01T00:00:30.000000" + suffix +
                        "10036\t3.6\t37.36\t3601.36\t36000.0136\t3600.000136\t360.00000136\t2022-01-01T00:00:30.500000" + suffix +
                        "31\t3.1\t31.31\t3100.31\t31000.0031\t3100.000031\t310.00000031\t2022-01-01T00:00:31.000000" + suffix +
                        "10037\t3.7\t38.37\t3701.37\t37000.0137\t3700.000137\t370.00000137\t2022-01-01T00:00:31.000000" + suffix +
                        "10038\t3.8\t39.38\t3801.38\t38000.0138\t3800.000138\t380.00000138\t2022-01-01T00:00:31.500000" + suffix +
                        "32\t3.2\t32.32\t3200.32\t32000.0032\t3200.000032\t320.00000032\t2022-01-01T00:00:32.000000" + suffix +
                        "10039\t3.9\t40.39\t3901.39\t39000.0139\t3900.000139\t390.00000139\t2022-01-01T00:00:32.000000" + suffix +
                        "10040\t4.0\t41.40\t4001.40\t40000.0140\t4000.000140\t400.00000140\t2022-01-01T00:00:32.500000" + suffix +
                        "33\t3.3\t33.33\t3300.33\t33000.0033\t3300.000033\t330.00000033\t2022-01-01T00:00:33.000000" + suffix +
                        "10041\t4.1\t42.41\t4101.41\t41000.0141\t4100.000141\t410.00000141\t2022-01-01T00:00:33.000000" + suffix +
                        "10042\t4.2\t43.42\t4201.42\t42000.0142\t4200.000142\t420.00000142\t2022-01-01T00:00:33.500000" + suffix +
                        "34\t3.4\t34.34\t3400.34\t34000.0034\t3400.000034\t340.00000034\t2022-01-01T00:00:34.000000" + suffix +
                        "10043\t4.3\t44.43\t4301.43\t43000.0143\t4300.000143\t430.00000143\t2022-01-01T00:00:34.000000" + suffix +
                        "10044\t4.4\t45.44\t4401.44\t44000.0144\t4400.000144\t440.00000144\t2022-01-01T00:00:34.500000" + suffix +
                        "35\t3.5\t35.35\t3500.35\t35000.0035\t3500.000035\t350.00000035\t2022-01-01T00:00:35.000000" + suffix +
                        "10045\t4.5\t46.45\t4501.45\t45000.0145\t4500.000145\t450.00000145\t2022-01-01T00:00:35.000000" + suffix +
                        "10046\t4.6\t47.46\t4601.46\t46000.0146\t4600.000146\t460.00000146\t2022-01-01T00:00:35.500000" + suffix +
                        "36\t3.6\t36.36\t3600.36\t36000.0036\t3600.000036\t360.00000036\t2022-01-01T00:00:36.000000" + suffix +
                        "10047\t4.7\t48.47\t4701.47\t47000.0147\t4700.000147\t470.00000147\t2022-01-01T00:00:36.000000" + suffix +
                        "10048\t4.8\t49.48\t4801.48\t48000.0148\t4800.000148\t480.00000148\t2022-01-01T00:00:36.500000" + suffix +
                        "37\t3.7\t37.37\t3700.37\t37000.0037\t3700.000037\t370.00000037\t2022-01-01T00:00:37.000000" + suffix +
                        "10049\t4.9\t50.49\t4901.49\t49000.0149\t4900.000149\t490.00000149\t2022-01-01T00:00:37.000000" + suffix +
                        "38\t3.8\t38.38\t3800.38\t38000.0038\t3800.000038\t380.00000038\t2022-01-01T00:00:38.000000" + suffix +
                        "39\t3.9\t39.39\t3900.39\t39000.0039\t3900.000039\t390.00000039\t2022-01-01T00:00:39.000000" + suffix +
                        "40\t4.0\t40.40\t4000.40\t40000.0040\t4000.000040\t400.00000040\t2022-01-01T00:00:40.000000" + suffix +
                        "41\t4.1\t41.41\t4100.41\t41000.0041\t4100.000041\t410.00000041\t2022-01-01T00:00:41.000000" + suffix +
                        "42\t4.2\t42.42\t4200.42\t42000.0042\t4200.000042\t420.00000042\t2022-01-01T00:00:42.000000" + suffix +
                        "43\t4.3\t43.43\t4300.43\t43000.0043\t4300.000043\t430.00000043\t2022-01-01T00:00:43.000000" + suffix +
                        "44\t4.4\t44.44\t4400.44\t44000.0044\t4400.000044\t440.00000044\t2022-01-01T00:00:44.000000" + suffix +
                        "45\t4.5\t45.45\t4500.45\t45000.0045\t4500.000045\t450.00000045\t2022-01-01T00:00:45.000000" + suffix +
                        "46\t4.6\t46.46\t4600.46\t46000.0046\t4600.000046\t460.00000046\t2022-01-01T00:00:46.000000" + suffix +
                        "47\t4.7\t47.47\t4700.47\t47000.0047\t4700.000047\t470.00000047\t2022-01-01T00:00:47.000000" + suffix +
                        "48\t4.8\t48.48\t4800.48\t48000.0048\t4800.000048\t480.00000048\t2022-01-01T00:00:48.000000" + suffix +
                        "49\t4.9\t49.49\t4900.49\t49000.0049\t4900.000049\t490.00000049\t2022-01-01T00:00:49.000000" + suffix +
                        "50\t5.0\t50.50\t5000.50\t50000.0050\t5000.000050\t500.00000050\t2022-01-01T00:00:50.000000" + suffix +
                        "51\t5.1\t51.51\t5100.51\t51000.0051\t5100.000051\t510.00000051\t2022-01-01T00:00:51.000000" + suffix +
                        "52\t5.2\t52.52\t5200.52\t52000.0052\t5200.000052\t520.00000052\t2022-01-01T00:00:52.000000" + suffix +
                        "53\t5.3\t53.53\t5300.53\t53000.0053\t5300.000053\t530.00000053\t2022-01-01T00:00:53.000000" + suffix +
                        "54\t5.4\t54.54\t5400.54\t54000.0054\t5400.000054\t540.00000054\t2022-01-01T00:00:54.000000" + suffix +
                        "55\t5.5\t55.55\t5500.55\t55000.0055\t5500.000055\t550.00000055\t2022-01-01T00:00:55.000000" + suffix +
                        "56\t5.6\t56.56\t5600.56\t56000.0056\t5600.000056\t560.00000056\t2022-01-01T00:00:56.000000" + suffix +
                        "57\t5.7\t57.57\t5700.57\t57000.0057\t5700.000057\t570.00000057\t2022-01-01T00:00:57.000000" + suffix +
                        "58\t5.8\t58.58\t5800.58\t58000.0058\t5800.000058\t580.00000058\t2022-01-01T00:00:58.000000" + suffix +
                        "59\t5.9\t59.59\t5900.59\t59000.0059\t5900.000059\t590.00000059\t2022-01-01T00:00:59.000000" + suffix +
                        "60\t6.0\t60.60\t6000.60\t60000.0060\t6000.000060\t600.00000060\t2022-01-01T00:01:00.000000" + suffix +
                        "61\t6.1\t61.61\t6100.61\t61000.0061\t6100.000061\t610.00000061\t2022-01-01T00:01:01.000000" + suffix +
                        "62\t6.2\t62.62\t6200.62\t62000.0062\t6200.000062\t620.00000062\t2022-01-01T00:01:02.000000" + suffix +
                        "63\t6.3\t63.63\t6300.63\t63000.0063\t6300.000063\t630.00000063\t2022-01-01T00:01:03.000000" + suffix +
                        "64\t6.4\t64.64\t6400.64\t64000.0064\t6400.000064\t640.00000064\t2022-01-01T00:01:04.000000" + suffix +
                        "65\t6.5\t65.65\t6500.65\t65000.0065\t6500.000065\t650.00000065\t2022-01-01T00:01:05.000000" + suffix +
                        "66\t6.6\t66.66\t6600.66\t66000.0066\t6600.000066\t660.00000066\t2022-01-01T00:01:06.000000" + suffix +
                        "67\t6.7\t67.67\t6700.67\t67000.0067\t6700.000067\t670.00000067\t2022-01-01T00:01:07.000000" + suffix +
                        "68\t6.8\t68.68\t6800.68\t68000.0068\t6800.000068\t680.00000068\t2022-01-01T00:01:08.000000" + suffix +
                        "69\t6.9\t69.69\t6900.69\t69000.0069\t6900.000069\t690.00000069\t2022-01-01T00:01:09.000000" + suffix +
                        "70\t7.0\t70.70\t7000.70\t70000.0070\t7000.000070\t700.00000070\t2022-01-01T00:01:10.000000" + suffix +
                        "71\t7.1\t71.71\t7100.71\t71000.0071\t7100.000071\t710.00000071\t2022-01-01T00:01:11.000000" + suffix +
                        "72\t7.2\t72.72\t7200.72\t72000.0072\t7200.000072\t720.00000072\t2022-01-01T00:01:12.000000" + suffix +
                        "73\t7.3\t73.73\t7300.73\t73000.0073\t7300.000073\t730.00000073\t2022-01-01T00:01:13.000000" + suffix +
                        "74\t7.4\t74.74\t7400.74\t74000.0074\t7400.000074\t740.00000074\t2022-01-01T00:01:14.000000" + suffix +
                        "75\t7.5\t75.75\t7500.75\t75000.0075\t7500.000075\t750.00000075\t2022-01-01T00:01:15.000000" + suffix +
                        "76\t7.6\t76.76\t7600.76\t76000.0076\t7600.000076\t760.00000076\t2022-01-01T00:01:16.000000" + suffix +
                        "77\t7.7\t77.77\t7700.77\t77000.0077\t7700.000077\t770.00000077\t2022-01-01T00:01:17.000000" + suffix +
                        "78\t7.8\t78.78\t7800.78\t78000.0078\t7800.000078\t780.00000078\t2022-01-01T00:01:18.000000" + suffix +
                        "79\t7.9\t79.79\t7900.79\t79000.0079\t7900.000079\t790.00000079\t2022-01-01T00:01:19.000000" + suffix +
                        "80\t8.0\t80.80\t8000.80\t80000.0080\t8000.000080\t800.00000080\t2022-01-01T00:01:20.000000" + suffix +
                        "81\t8.1\t81.81\t8100.81\t81000.0081\t8100.000081\t810.00000081\t2022-01-01T00:01:21.000000" + suffix +
                        "82\t8.2\t82.82\t8200.82\t82000.0082\t8200.000082\t820.00000082\t2022-01-01T00:01:22.000000" + suffix +
                        "83\t8.3\t83.83\t8300.83\t83000.0083\t8300.000083\t830.00000083\t2022-01-01T00:01:23.000000" + suffix +
                        "84\t8.4\t84.84\t8400.84\t84000.0084\t8400.000084\t840.00000084\t2022-01-01T00:01:24.000000" + suffix +
                        "85\t8.5\t85.85\t8500.85\t85000.0085\t8500.000085\t850.00000085\t2022-01-01T00:01:25.000000" + suffix +
                        "86\t8.6\t86.86\t8600.86\t86000.0086\t8600.000086\t860.00000086\t2022-01-01T00:01:26.000000" + suffix +
                        "87\t8.7\t87.87\t8700.87\t87000.0087\t8700.000087\t870.00000087\t2022-01-01T00:01:27.000000" + suffix +
                        "88\t8.8\t88.88\t8800.88\t88000.0088\t8800.000088\t880.00000088\t2022-01-01T00:01:28.000000" + suffix +
                        "89\t8.9\t89.89\t8900.89\t89000.0089\t8900.000089\t890.00000089\t2022-01-01T00:01:29.000000" + suffix +
                        "90\t9.0\t90.90\t9000.90\t90000.0090\t9000.000090\t900.00000090\t2022-01-01T00:01:30.000000" + suffix +
                        "91\t9.1\t91.91\t9100.91\t91000.0091\t9100.000091\t910.00000091\t2022-01-01T00:01:31.000000" + suffix +
                        "92\t9.2\t92.92\t9200.92\t92000.0092\t9200.000092\t920.00000092\t2022-01-01T00:01:32.000000" + suffix +
                        "93\t9.3\t93.93\t9300.93\t93000.0093\t9300.000093\t930.00000093\t2022-01-01T00:01:33.000000" + suffix +
                        "94\t9.4\t94.94\t9400.94\t94000.0094\t9400.000094\t940.00000094\t2022-01-01T00:01:34.000000" + suffix +
                        "95\t9.5\t95.95\t9500.95\t95000.0095\t9500.000095\t950.00000095\t2022-01-01T00:01:35.000000" + suffix +
                        "96\t9.6\t96.96\t9600.96\t96000.0096\t9600.000096\t960.00000096\t2022-01-01T00:01:36.000000" + suffix +
                        "97\t9.7\t97.97\t9700.97\t97000.0097\t9700.000097\t970.00000097\t2022-01-01T00:01:37.000000" + suffix +
                        "98\t9.8\t98.98\t9800.98\t98000.0098\t9800.000098\t980.00000098\t2022-01-01T00:01:38.000000" + suffix +
                        "99\t9.9\t99.99\t9900.99\t99000.0099\t9900.000099\t990.00000099\t2022-01-01T00:01:39.000000" + suffix
        );
    }

    private static void testLagOverflowBySize0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        assertLag(
                engine,
                compiler,
                sqlExecutionContext,
                "with maxUncommittedRows=10000, o3MaxLag=5d",
                " from long_sequence(100000)",
                "insert batch 20000 o3MaxLag 3d into x select * from top",
                timestampTypeName
        );
    }

    private static void testLagOverflowMidCommit0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        assertLag(
                engine,
                compiler,
                sqlExecutionContext,
                "with maxUncommittedRows=400",
                " from long_sequence(1000000)",
                "insert batch 100000 o3MaxLag 180s into x select * from top",
                timestampTypeName
        );
    }

    private static void testLargeO3MaxLag0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
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
                        " timestamp_sequence(500000000000L,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(0)" +
                        "), index(sym) timestamp (ts) partition by MONTH",
                sqlExecutionContext
        );

        engine.execute("alter TABLE x SET PARAM maxUncommittedRows = 2000000", sqlExecutionContext);

        engine.execute(
                "create table top as (" +
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
                        // the formula below creates lag-friendly timestamp distribution
                        " cast(500000000000L + ((x-1)/4) * 1000L + (4-(x-1)%4)*89  as timestamp)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(2096128)" +
                        ")",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all top)",
                "y order by ts",
                "insert batch 2000000 o3MaxLag 180s into x select * from top",
                "x",
                "y",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext, engine);

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );
    }

    private static void testManyPartitionsParallel(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " timestamp_sequence(0, 10* 1000000L) ts " +
                        " from long_sequence(500)" +
                        ") timestamp(ts) partition by HOUR",
                sqlExecutionContext
        );

        for (int i = 0; i < 20; i++) {
            engine.execute(
                    "insert into x select" +
                            " cast(x as int) i," +
                            " cast(abs(rnd_int(0, 400, 0) * 60 * 60 * 1000000L) as timestamp) ts" +
                            " from long_sequence(200)", sqlExecutionContext
            );
        }

        TestUtils.assertSql(
                compiler, sqlExecutionContext, "select sum(i) from x", sink, """
                        sum
                        527250
                        """
        );
    }

    private static void testO3EdgeBug(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(10000000000,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(1000)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        engine.execute(
                "create table 1am as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(3000000000l,10000000L)::" + timestampTypeName + " ts," + // mid partition for "x"
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(801)" +
                        ")",
                sqlExecutionContext
        );

        engine.execute(
                "create table tail as (" +
                        "select" +
                        " 3 as commit," +
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
                        " timestamp_sequence(20000000000,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        engine.execute("create table y as (x union all 1am union all tail)", sqlExecutionContext);

        // The query above generates expected result, but there is a problem using it
        // This test produces duplicate timestamps. Those are being sorted in different order by OOO implementation
        // and the reference query. So they cannot be directly compared. The parts with duplicate timestamps will
        // look different. If this test ever breaks, uncomment the reference query and compare results visually.

        // insert 1AM data into X
        engine.execute("insert into x select * from 1am", sqlExecutionContext);
        engine.execute("insert into x select * from tail", sqlExecutionContext);

        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                null,
                "y order by ts, commit",
                null,
                "x",
                "y",
                "x"
        );

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );
    }

    private static void testOOOTouchesNotLastPartition0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        long day = MICRO_DRIVER.fromDays(1);
        long hour = MICRO_DRIVER.fromHours(1);
        long min = hour / 60;
        long sec = min / 60;
        long minsPerDay = day / min;

        engine.execute(
                "create table x as ( " +
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
                        " timestamp_sequence(0L, " + min + "L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(" + 2 * minsPerDay + ")" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute(
                "create table append as (" +
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
                        " timestamp_sequence(" + (day - min) + "L, " + sec + "L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(60)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "y order by ts, i desc",
                "insert into x select * from append",
                "x",
                "y",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext, engine);
        assertXCountY(engine, compiler, sqlExecutionContext);
    }

    private static void testOOOTouchesNotLastPartitionTop0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        long day = MICRO_DRIVER.fromDays(1);
        long hour = MICRO_DRIVER.fromHours(1);
        long min = hour / 60;
        long minsPerDay = day / min;

        // crate records from Jan 1 01:00:00 to Jan 2 01:00:00 with 1 min interval
        engine.execute(
                "create table x as ( " +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(" + hour + "L, " + min + "L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(" + minsPerDay + ")" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // crate 60 records from Jan 1 00:01:00 to Jan 1 01:00:00 with 1 min interval
        // so that Jan 1 01:00:00 present in both record sets
        engine.execute(
                "create table append as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(" + min + "L, " + min + "L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(60)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "y order by ts, commit",
                "insert into x select * from append",
                "x",
                "y",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext, engine);
        assertXCountY(engine, compiler, sqlExecutionContext);
    }

    private static void testOooFollowedByAnotherOOO0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(10000000000,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        engine.execute(
                "create table 1am as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(9993000000,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(507)" +
                        ")",
                sqlExecutionContext
        );

        engine.execute(
                "create table tail as (" +
                        "select" +
                        " 3 as commit," +
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
                        " timestamp_sequence(9997000010L,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("create table y as (x union all 1am union all tail)", sqlExecutionContext);

        // insert 1AM data into X
        engine.execute("insert into x select * from 1am", sqlExecutionContext);
        engine.execute("insert into x select * from tail", sqlExecutionContext);

        testXAndIndex(engine, compiler, sqlExecutionContext, "y order by ts, commit", "x");

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );
    }

    private static void testPartitionedDataAppendOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String timestampTypeName
    ) throws SqlException {
        // create table with roughly 2AM data
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 1 as commit," +
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
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                executionContext
        );

        engine.execute(
                "create table append as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(518300000010L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        // create third table, which will contain both X and 1AM
        assertO3DataCursors(
                engine,
                compiler,
                executionContext,
                "create table y as (x union all append)",
                "y order by ts, commit",
                "insert into x select * from append",
                "x",
                "y",
                "x"
        );

        assertIndexConsistency(compiler, executionContext, engine);

        // x ends with timestamp 549900000000
        // straight append
        engine.execute(
                "create table append2 as (" +
                        "select" +
                        " 3 as commit," +
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
                        " timestamp_sequence(549900000001L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        engine.execute("drop table y", executionContext);

        // create third table, which will contain both X and 1AM
        assertO3DataCursors(
                engine,
                compiler,
                executionContext,
                "create table y as (x union all append2)",
                "y order by ts, commit",
                "insert into x select * from append2",
                "x",
                "y",
                "x"
        );


        TestUtils.printSql(
                compiler,
                executionContext,
                "select count() from y",
                sink2
        );

        assertXCount(compiler, executionContext);
        assertMaxTimestamp(engine, executionContext);
    }

    private static void testPartitionedDataAppendOODataIndexed0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 1 as commit," +
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
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute(
                "create table append as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(518300000010L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "y where sym = 'googl' order by ts, commit",
                "insert into x select * from append",
                "x where sym = 'googl'",
                "y where sym = 'googl'",
                "x where sym = 'googl'"
        );

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );
    }

    private static void testPartitionedDataAppendOODataNotNullStrTail0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
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
                        " cast(null as binary) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(510)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute(
                "create table append as (" +
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
                        " timestamp_sequence(518300000010L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "y order by ts",
                "insert into x select * from append",
                "x",
                "y",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext, engine);

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );
    }

    private static void testPartitionedDataAppendOOPrependOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        // create table with roughly 2AM data
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 1 as commit," +
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
                        " cast(null as binary) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(510)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // all records but one is appended to middle partition
        // last record is prepended to the last partition
        engine.execute(
                "create table append as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(518390000000L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(101)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "y order by ts, commit",
                "insert into x select * from append",
                "x",
                "y",
                "x"
        );

        assertIndexConsistency(
                compiler,
                sqlExecutionContext,
                engine
        );

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );
    }

    private static void testPartitionedDataMergeData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 0 as commit," +
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
                        " timestamp_sequence(500000000000L,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " cast(now() as long256) l256," +
                        " rnd_varchar(1, 30, 1) varc," +
                        " rnd_varchar(1, 1, 0) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute(
                "create table middle as (" +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(500288000000L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " cast(now() as long256) l256," +
                        " rnd_varchar(1, 30, 1) varc," +
                        " rnd_varchar(1, 1, 0) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        assertO3DataConsistencyStableSort(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all middle)",
                "insert into x select * from middle"
        );

        // check that reader can process out of order partition layout after fresh open
        engine.releaseAllReaders();

        assertO3DataConsistencyStableSort(
                engine,
                compiler,
                sqlExecutionContext,
                null,
                null
        );

        // create third table, which will contain both X and 1AM
        TestUtils.assertEqualsExactOrder(
                compiler,
                sqlExecutionContext,
                "y where sym = 'googl' order by ts, commit",
                "x where sym = 'googl'"
        );

        assertXCountY(engine, compiler, sqlExecutionContext);
    }

    private static void testPartitionedDataMergeEnd0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(500000000000L,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(295)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute(
                "create table middle as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(500288000000L,100000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(61)" + // <--- these counts are important to align the data
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all middle)",
                "y where sym = 'googl' order by ts, commit",
                "insert into x select * from middle",
                "x where sym = 'googl'",
                "y",
                "x"
        );

        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                null,
                "y order by ts, commit",
                null,
                "x",
                "y",
                "x"
        );

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );
    }

    private static void testPartitionedDataOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(500000000000L,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        engine.execute(
                "create table middle as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(500288000000L,10L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        assertO3DataConsistencyStableSort(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all middle)",
                "insert into x select * from middle"
        );

        assertIndexConsistency(compiler, sqlExecutionContext, engine);

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );
    }

    private static void testPartitionedDataOODataPbOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(10000000000,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(1000)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        engine.execute(
                "create table 1am as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(72700000000L,1000000L)::" + timestampTypeName + " ts," + // mid partition for "x"
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(100)" +
                        ")",
                sqlExecutionContext
        );

        engine.execute(
                "create table top2 as (" +
                        "select" +
                        " 3 as commit," +
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
                        " cast(86400000000L as timestamp)::" + timestampTypeName + " ts," + // these row should go on top of second partition
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(100)" +
                        ")",
                sqlExecutionContext
        );

        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (select * from x union all select * from 1am union all select * from top2)",
                "y order by ts, commit, i",
                "insert into x select * from (1am union all top2)",
                "x",
                "y",
                "x"
        );

        assertIndexConsistency(
                compiler,
                sqlExecutionContext,
                engine
        );
        assertXCountY(engine, compiler, sqlExecutionContext);
    }

    private static void testPartitionedDataOODataPbOODataDropColumn0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(10000000000,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(1000)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        engine.execute(
                "create table 1am as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(72700000000L-4,1000000L)::" + timestampTypeName + " ts," + // mid partition for "x"
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(100)" +
                        ")",
                sqlExecutionContext
        );

        engine.execute(
                "create table append2 as (" +
                        "select" +
                        " 3 as commit," +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " (to_timestamp('2018-01', 'yyyy-MM') + x * 720000000)::" + timestampTypeName + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(109890000000L-1,1000000L)::" + timestampTypeName + " ts," + // mid partition for "x"
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(100)" +
                        ")",
                sqlExecutionContext
        );

        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (select * from x union all 1am)",
                "insert into x select * from 1am"
        );

        assertIndexConsistency(
                compiler,
                sqlExecutionContext,
                engine
        );

        engine.execute("alter table x drop column c", sqlExecutionContext);

        dropTableY(engine, sqlExecutionContext);
        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (select * from x union all append2)",
                "y order by ts, commit",
                "insert into x select * from append2",
                "x",
                "y",
                "x"
        );

        assertIndexConsistency(
                compiler,
                sqlExecutionContext,
                "y",
                engine
        );

        TestUtils.printSql(
                compiler,
                sqlExecutionContext,
                "select count() from y",
                sink2
        );
        assertXCount(compiler, sqlExecutionContext);
        assertMaxTimestamp(engine, sqlExecutionContext);
    }

    private static void testPartitionedDataOOIntoLastIndexSearchBug0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(10000000000,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(1000)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        engine.execute(
                "create table 1am as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(3000000000l,10000000L)::" + timestampTypeName + " ts," + // mid partition for "x"
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(1000)" +
                        ")",
                sqlExecutionContext
        );

        engine.execute(
                "create table tail as (" +
                        "select" +
                        " 3 as commit," +
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
                        " timestamp_sequence(20000000000,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        engine.execute("create table y as (x union all 1am union all tail)", sqlExecutionContext);

        // The query above generates expected result, but there is a problem using it
        // This test produces duplicate timestamps. Those are being sorted in different order by OOO implementation
        // and the reference query. So they cannot be directly compared. The parts with duplicate timestamps will
        // look different. If this test ever breaks, uncomment the reference query and compare results visually.

        // insert 1AM data into X
        engine.execute("insert into x select * from 1am", sqlExecutionContext);
        engine.execute("insert into x select * from tail", sqlExecutionContext);

        // It is necessary to release cached "x" reader because as of yet
        // reader cannot reload any partition other than "current".
        // Cached reader will assume smaller partition size for "1970-01-01", which out-of-order insert updated
        testXAndIndex(
                engine,
                compiler,
                sqlExecutionContext,
                "y order by ts, commit",
                "x"
        );

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );
    }

    private static void testPartitionedDataOOIntoLastOverflowIntoNewPartition0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(10000000000,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(1000)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        engine.execute(
                "create table 1am as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(3000000000l,100000000L)::" + timestampTypeName + " ts," + // mid partition for "x"
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(1000)" +
                        ")",
                sqlExecutionContext
        );

        engine.execute(
                "create table tail as (" +
                        "select" +
                        " 3 as commit," +
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
                        " timestamp_sequence(40000000000,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute("create table y as (x union all 1am union all tail)", sqlExecutionContext);

        engine.execute("insert into x select * from 1am", sqlExecutionContext);
        engine.execute("insert into x select * from tail", sqlExecutionContext);

        testXAndIndex(
                engine,
                compiler,
                sqlExecutionContext,
                "y order by ts, commit",
                "x"
        );

        // check if the same remains true when we open fresh TableReader instance
        engine.releaseAllReaders();

        testXAndIndex(
                engine,
                compiler,
                sqlExecutionContext,
                "y order by ts, commit",
                "x"
        );

        assertXCountY(engine, compiler, sqlExecutionContext);
    }

    private static void testPartitionedOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
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
                        " timestamp_sequence(10000000000,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 30, 1) varc," +
                        " rnd_varchar(1, 1, 0) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        engine.execute(
                "create table 1am as (" +
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
                        " timestamp_sequence(0,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 30, 1) varc," +
                        " rnd_varchar(1, 1, 0) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        ")",
                sqlExecutionContext
        );

        engine.execute(
                "create table tail as (" +
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
                        " timestamp_sequence(40000000000,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 30, 1) varc," +
                        " rnd_varchar(1, 1, 0) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        engine.execute("create table y as (select * from x union all 1am union all tail)", sqlExecutionContext);

        engine.execute("insert into x select * from 1am", sqlExecutionContext);
        engine.execute("insert into x select * from tail", sqlExecutionContext);

        testXAndIndex(
                engine,
                compiler,
                sqlExecutionContext,
                "y order by ts",
                "x"
        );

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );
    }

    private static void testPartitionedOODataOOCollapsed0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        // top edge of data timestamp equals of of
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(500288000000L,10L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 30, 1) varc," +
                        " rnd_varchar(1, 1, 0) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        engine.execute(
                "create table middle as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(500000000000L,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 30, 1) varc," +
                        " rnd_varchar(1, 1, 0) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all middle)",
                "y order by ts, commit",
                "insert into x select * from middle",
                "x",
                "y",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext, engine);

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );
    }

    private static void testPartitionedOODataUpdateMinTimestamp0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
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
                        " timestamp_sequence(500000000000L,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 30, 1) varc," +
                        " rnd_varchar(1, 1, 0) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        engine.execute(
                "create table 1am as (" +
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
                        " timestamp_sequence(450000000000L,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 30, 1) varc," +
                        " rnd_varchar(1, 1, 0) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(500)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute(
                "create table prev as (" +
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
                        " timestamp_sequence(0L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 30, 1) varc," +
                        " rnd_varchar(1, 1, 0) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(3000)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (select * from x union all select * from 1am union all select * from prev)",
                "y order by ts",
                "insert into x select * from (1am union all prev)",
                "x",
                "y",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext, engine);

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );
    }

    private static void testPartitionedOOMerge0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(10000000000,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        engine.execute(
                "create table 1am as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(9993000000,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(507)" +
                        ")",
                sqlExecutionContext
        );

        engine.execute(
                "create table tail as (" +
                        "select" +
                        " 3 as commit," +
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
                        " timestamp_sequence(20000000000,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        engine.execute("create table y as (x union all 1am union all tail)", sqlExecutionContext);

        // insert 1AM data into X
        engine.execute("insert into x select * from 1am", sqlExecutionContext);
        engine.execute("insert into x select * from tail", sqlExecutionContext);

        testXAndIndex(engine, compiler, sqlExecutionContext, "y order by ts, commit", "x");

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );
    }

    private static void testPartitionedOOMergeData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 1 as commit," +
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
                        " timestamp_sequence(10000000000,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(1500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        engine.execute(
                "create table 1am as (" +
                        "select" +
                        " 2 as commit," +
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
                        " timestamp_sequence(1000000000L,100000000L)::" + timestampTypeName + " ts," +
                        // 1000000000L
                        // 2200712364240
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(100)" +
                        ")",
                sqlExecutionContext
        );

        engine.execute(
                "create table tail as (" +
                        "select" +
                        " 3 as commit," +
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
                        " timestamp_sequence(11500000000L,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        engine.execute("create table y as (select * from x union all 1am union all tail)", sqlExecutionContext);

        // insert 1AM data into X
        engine.execute("insert into x select * from 1am", sqlExecutionContext);
        engine.execute("insert into x select * from tail", sqlExecutionContext);

        testXAndIndex(engine, compiler, sqlExecutionContext, "y order by ts, commit", "x");

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );
    }

    private static void testPartitionedOOMergeOO0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x_1 as (" +
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
                        " timestamp_shuffle(0,100000000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 1, 1) varc," +
                        " rnd_varchar(1, 40, 1) varc2," +
                        " rnd_double_array(1,1) arr," +
                        " from long_sequence(1000)" +
                        ")",
                sqlExecutionContext
        );

        engine.execute(
                "create table y as (select * from x_1 order by ts) timestamp(ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute(
                "create table x as (select * from x_1), index(sym) timestamp(ts) partition by DAY",
                sqlExecutionContext
        );

        final String sqlTemplate = "select * from ";
        engine.print(sqlTemplate + "y", sink, sqlExecutionContext);

        String expected = Chars.toString(sink);

        engine.print(sqlTemplate + "x", sink, sqlExecutionContext);
        TestUtils.assertEquals(expected, sink);

        assertIndexConsistencySink(compiler, sqlExecutionContext);
    }

    private static void testPartitionedOOONullSetters0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException, NumericException {
        engine.execute("create table x (a int, b int, c int, ts " + timestampTypeName + ") timestamp(ts) partition by DAY", sqlExecutionContext);
        try (TableWriter w = TestUtils.getWriter(engine, "x")) {
            TableWriter.Row r;
            TimestampDriver driver = ColumnType.getTimestampDriver(w.getTimestampType());

            r = w.newRow(driver.parseFloorLiteral("2013-02-10T00:10:00.000000Z"));
            r.putInt(2, 30);
            r.append();

            r = w.newRow(driver.parseFloorLiteral("2013-02-10T00:05:00.000000Z"));
            r.putInt(2, 10);
            r.append();

            r = w.newRow(driver.parseFloorLiteral("2013-02-10T00:06:00.000000Z"));
            r.putInt(2, 20);
            r.append();

            w.commit();

            r = w.newRow(driver.parseFloorLiteral("2013-02-10T00:11:00.000000Z"));
            r.putInt(2, 40);
            r.append();

            w.commit();
        }

        engine.print("x", sink, sqlExecutionContext);

        final String expected = replaceTimestampSuffix1("""
                a\tb\tc\tts
                null\tnull\t10\t2013-02-10T00:05:00.000000Z
                null\tnull\t20\t2013-02-10T00:06:00.000000Z
                null\tnull\t30\t2013-02-10T00:10:00.000000Z
                null\tnull\t40\t2013-02-10T00:11:00.000000Z
                """, timestampTypeName);

        TestUtils.assertEquals(expected, sink);
    }

    private static void testPartitionedOOONullStrSetters0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String timestampTypeName) throws SqlException {
        final int commits = 4;
        final int rows = 1_000;
        engine.execute("create table x (s string, ts " + timestampTypeName + ") timestamp(ts) partition by DAY", sqlExecutionContext);
        try (TableWriter w = TestUtils.getWriter(engine, "x")) {
            TableWriter.Row r;

            // Here we're trying to make sure that null setters write to the currently active O3 memory.
            long ts = engine.getConfiguration().getMicrosecondClock().getTicks();
            int columnType = w.getMetadata().getColumnType(0);
            Utf8StringSink utf8Sequence = new Utf8StringSink();
            utf8Sequence.put("Lorem ipsum dolor sit amet, consectetur adipiscing elit.");

            for (int i = 0; i < commits; i++) {
                for (int j = 0; j < rows; j++) {
                    r = w.newRow(ts);
                    if (j % 2 == 0) {
                        switch (columnType) {
                            case ColumnType.STRING:
                                r.putStr(0, "Lorem ipsum dolor sit amet, consectetur adipiscing elit.");
                                break;
                            case ColumnType.VARCHAR:
                                r.putVarchar(0, utf8Sequence);
                                break;
                        }
                    }
                    r.append();
                    if (j % 100 == 0) {
                        ts -= 1_000;
                    } else {
                        ts += 1;
                    }
                }
                // Setting the lag is important since we want some rows to remain pending after each commit.
                w.ic(1_000L);
            }
            // Commit pending O3 rows.
            w.commit();
        }

        engine.print("select count() from x", sink, sqlExecutionContext);

        final String expected = "count\n" + (commits * rows) + "\n";

        TestUtils.assertEquals(expected, sink);
    }

    private static void testPartitionedOOPrefixesExistingPartitions0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
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
                        " timestamp_sequence(500000000000L,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1,30,0) varc," +
                        " rnd_varchar(1,1,0) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(1)" +
                        "), index(sym capacity 1024) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        engine.execute(
                "create table top as (" +
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
                        " timestamp_sequence(15000000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(10,50,0) varc," +
                        " rnd_varchar(1,1,0) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(1)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (select * from x union all select * from top)",
                "y order by ts",
                "insert into x select * from top",
                "x",
                "y",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext, engine);

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );
    }

    private static void testPartitionedOOTopAndBottom0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
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
                        " timestamp_sequence(500000000000L,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1,30,1) varc," +
                        " rnd_varchar(1,1,1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        engine.execute(
                "create table top as (" +
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
                        " timestamp_sequence(150000000000L,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1,30,1) varc," +
                        " rnd_varchar(1,1,1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute(
                "create table bottom as (" +
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
                        " timestamp_sequence(500500000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1,30,1) varc," +
                        " rnd_varchar(1,1,1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (select * from x union all select * from top union all select * from bottom)",
                "y order by ts",
                "insert into x select * from (top union all bottom)",
                "x",
                "y",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext, engine);
        assertXCountY(engine, compiler, sqlExecutionContext);
    }

    private static void testRebuildIndexLastPartitionWithColumnTop(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {

        engine.execute(
                "CREATE TABLE monthly_col_top(" +
                        "ts " + timestampTypeName + ", metric SYMBOL, diagnostic SYMBOL, sensorChannel SYMBOL" +
                        ") timestamp(ts) partition by MONTH",
                sqlExecutionContext
        );

        engine.execute(
                "INSERT INTO monthly_col_top (ts, metric, diagnostic, sensorChannel) VALUES" +
                        "('2022-06-08T01:40:00.000000Z', '1', 'true', '2')," +
                        "('2022-06-08T02:41:00.000000Z', '2', 'true', '2')," +
                        "('2022-06-08T02:42:00.000000Z', '3', 'true', '1')," +
                        "('2022-06-08T02:43:00.000000Z', '4', 'true', '1')", sqlExecutionContext
        );

        engine.execute("ALTER TABLE monthly_col_top ADD COLUMN loggerChannel SYMBOL INDEX", sqlExecutionContext);

        engine.execute(
                "INSERT INTO monthly_col_top (ts, metric, loggerChannel) VALUES" +
                        "('2022-06-08T02:50:00.000000Z', '5', '3')," +
                        "('2022-06-08T02:50:00.000000Z', '6', '3')," +
                        "('2022-06-08T02:50:00.000000Z', '7', '1')," +
                        "('2022-06-08T02:50:00.000000Z', '8', '1')," +
                        "('2022-06-08T02:50:00.000000Z', '9', '2')," +
                        "('2022-06-08T02:50:00.000000Z', '10', '2')," +
                        "('2022-06-08T03:50:00.000000Z', '11', '2')," +
                        "('2022-06-08T03:50:00.000000Z', '12', '2')," +
                        "('2022-06-08T04:50:00.000000Z', '13', '2')," +
                        "('2022-06-08T04:50:00.000000Z', '14', '2')", sqlExecutionContext
        );

        // OOO in the middle
        engine.execute(
                "INSERT INTO monthly_col_top (ts, metric, sensorChannel, 'loggerChannel') VALUES" +
                        "('2022-06-08T03:30:00.000000Z', '15', '2', '3')," +
                        "('2022-06-08T03:30:00.000000Z', '16', '2', '3')", sqlExecutionContext
        );


        TestUtils.assertSql(
                compiler, sqlExecutionContext, "select * from monthly_col_top where loggerChannel = '2'", sink,
                "ts\tmetric\tdiagnostic\tsensorChannel\tloggerChannel\n" +
                        replaceTimestampSuffix1("""
                                2022-06-08T02:50:00.000000Z\t9\t\t\t2
                                2022-06-08T02:50:00.000000Z\t10\t\t\t2
                                2022-06-08T03:50:00.000000Z\t11\t\t\t2
                                2022-06-08T03:50:00.000000Z\t12\t\t\t2
                                2022-06-08T04:50:00.000000Z\t13\t\t\t2
                                2022-06-08T04:50:00.000000Z\t14\t\t\t2
                                """, timestampTypeName)
        );

        // OOO appends to last partition
        engine.execute(
                "INSERT INTO monthly_col_top (ts, metric, sensorChannel, 'loggerChannel') VALUES" +
                        "('2022-06-08T05:30:00.000000Z', '17', '4', '3')," +
                        "('2022-06-08T04:50:00.000000Z', '18', '4', '3')", sqlExecutionContext
        );

        TestUtils.assertSql(
                compiler, sqlExecutionContext, "select * from monthly_col_top where loggerChannel = '3'", sink,
                "ts\tmetric\tdiagnostic\tsensorChannel\tloggerChannel\n" +
                        replaceTimestampSuffix1("""
                                2022-06-08T02:50:00.000000Z\t5\t\t\t3
                                2022-06-08T02:50:00.000000Z\t6\t\t\t3
                                2022-06-08T03:30:00.000000Z\t15\t\t2\t3
                                2022-06-08T03:30:00.000000Z\t16\t\t2\t3
                                2022-06-08T04:50:00.000000Z\t18\t\t4\t3
                                2022-06-08T05:30:00.000000Z\t17\t\t4\t3
                                """, timestampTypeName)
        );

        // OOO merges and appends to last partition
        engine.execute(
                "INSERT INTO monthly_col_top (ts, metric, sensorChannel, 'loggerChannel') VALUES" +
                        "('2022-06-08T05:30:00.000000Z', '19', '4', '3')," +
                        "('2022-06-08T02:50:00.000000Z', '20', '4', '3')," +
                        "('2022-06-08T02:50:00.000000Z', '21', '4', '3')", sqlExecutionContext
        );

        TestUtils.assertSql(
                compiler, sqlExecutionContext, "select * from monthly_col_top where loggerChannel = '3'", sink,
                "ts\tmetric\tdiagnostic\tsensorChannel\tloggerChannel\n" +
                        replaceTimestampSuffix1("""
                                2022-06-08T02:50:00.000000Z\t5\t\t\t3
                                2022-06-08T02:50:00.000000Z\t6\t\t\t3
                                2022-06-08T02:50:00.000000Z\t20\t\t4\t3
                                2022-06-08T02:50:00.000000Z\t21\t\t4\t3
                                2022-06-08T03:30:00.000000Z\t15\t\t2\t3
                                2022-06-08T03:30:00.000000Z\t16\t\t2\t3
                                2022-06-08T04:50:00.000000Z\t18\t\t4\t3
                                2022-06-08T05:30:00.000000Z\t17\t\t4\t3
                                2022-06-08T05:30:00.000000Z\t19\t\t4\t3
                                """, timestampTypeName)
        );
    }

    private static void testRebuildIndexWithColumnTopPrevPartition(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {

        engine.execute(
                "CREATE TABLE monthly_col_top(" +
                        "ts " + timestampTypeName + ", metric SYMBOL, diagnostic SYMBOL, sensorChannel SYMBOL" +
                        ") timestamp(ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute(
                "INSERT INTO monthly_col_top (ts, metric, diagnostic, sensorChannel) VALUES" +
                        "('2022-06-08T01:40:00.000000Z', '1', 'true', '2')," +
                        "('2022-06-08T02:41:00.000000Z', '2', 'true', '2')," +
                        "('2022-06-08T02:42:00.000000Z', '3', 'true', '1')," +
                        "('2022-06-08T02:43:00.000000Z', '4', 'true', '1')", sqlExecutionContext
        );

        engine.execute("ALTER TABLE monthly_col_top ADD COLUMN loggerChannel SYMBOL INDEX", sqlExecutionContext)
        ;

        engine.execute(
                "INSERT INTO monthly_col_top (ts, metric, loggerChannel) VALUES" +
                        "('2022-06-08T02:50:00.000000Z', '5', '3')," +
                        "('2022-06-08T02:55:00.000000Z', '6', '3')," +
                        "('2022-06-08T02:59:00.000000Z', '7', '1')", sqlExecutionContext
        );

        engine.execute(
                "INSERT batch 6 INTO monthly_col_top (ts, metric, 'loggerChannel') VALUES" +
                        "('2022-06-09T02:50:00.000000Z', '9', '2')," +
                        "('2022-06-09T02:50:00.000000Z', '10', '2')," +
                        "('2022-06-09T03:50:00.000000Z', '11', '2')," +
                        "('2022-06-09T03:50:00.000000Z', '12', '2')," +
                        "('2022-06-09T04:50:00.000000Z', '13', '2')," +
                        "('2022-06-09T04:50:00.000000Z', '14', '2')", sqlExecutionContext
        );

        // OOO append prev partition
        engine.execute(
                "INSERT batch 2 INTO monthly_col_top (ts, metric, sensorChannel, 'loggerChannel') VALUES" +
                        "('2022-06-08T03:30:00.000000Z', '15', '2', '3')," +
                        "('2022-06-08T03:30:00.000000Z', '16', '2', '3')", sqlExecutionContext
        );


        TestUtils.assertSql(
                compiler, sqlExecutionContext, "select * from monthly_col_top where loggerChannel = '3'", sink,
                replaceTimestampSuffix1("""
                        ts\tmetric\tdiagnostic\tsensorChannel\tloggerChannel
                        2022-06-08T02:50:00.000000Z\t5\t\t\t3
                        2022-06-08T02:55:00.000000Z\t6\t\t\t3
                        2022-06-08T03:30:00.000000Z\t15\t\t2\t3
                        2022-06-08T03:30:00.000000Z\t16\t\t2\t3
                        """, timestampTypeName)
        );

        // OOO insert mid prev partition
        engine.execute(
                "INSERT batch 2 INTO monthly_col_top (ts, metric, 'loggerChannel') VALUES" +
                        "('2022-06-08T02:54:00.000000Z', '17', '3')," +
                        "('2022-06-08T02:56:00.000000Z', '18', '3')", sqlExecutionContext
        );

        TestUtils.assertSql(
                compiler, sqlExecutionContext, "select * from monthly_col_top where loggerChannel = '3'", sink,
                replaceTimestampSuffix1("""
                        ts\tmetric\tdiagnostic\tsensorChannel\tloggerChannel
                        2022-06-08T02:50:00.000000Z\t5\t\t\t3
                        2022-06-08T02:54:00.000000Z\t17\t\t\t3
                        2022-06-08T02:55:00.000000Z\t6\t\t\t3
                        2022-06-08T02:56:00.000000Z\t18\t\t\t3
                        2022-06-08T03:30:00.000000Z\t15\t\t2\t3
                        2022-06-08T03:30:00.000000Z\t16\t\t2\t3
                        """, timestampTypeName)
        );

        // OOO insert overlaps prev partition and adds new rows at the end
        engine.execute(
                "INSERT batch 2 INTO monthly_col_top (ts, metric, 'loggerChannel') VALUES" +
                        "('2022-06-08T03:15:00.000000Z', '19', '3')," +
                        "('2022-06-08T04:30:00.000000Z', '20', '3')," +
                        "('2022-06-08T04:31:00.000000Z', '21', '3')", sqlExecutionContext
        );

        TestUtils.assertSql(
                compiler, sqlExecutionContext, "select * from monthly_col_top where loggerChannel = '3'", sink,
                replaceTimestampSuffix1("""
                        ts\tmetric\tdiagnostic\tsensorChannel\tloggerChannel
                        2022-06-08T02:50:00.000000Z\t5\t\t\t3
                        2022-06-08T02:54:00.000000Z\t17\t\t\t3
                        2022-06-08T02:55:00.000000Z\t6\t\t\t3
                        2022-06-08T02:56:00.000000Z\t18\t\t\t3
                        2022-06-08T03:15:00.000000Z\t19\t\t\t3
                        2022-06-08T03:30:00.000000Z\t15\t\t2\t3
                        2022-06-08T03:30:00.000000Z\t16\t\t2\t3
                        2022-06-08T04:30:00.000000Z\t20\t\t\t3
                        2022-06-08T04:31:00.000000Z\t21\t\t\t3
                        """, timestampTypeName)
        );

        // OOO insert overlaps prev partition and adds new rows at the top
        engine.execute(
                "INSERT batch 2 INTO monthly_col_top (ts, metric, 'loggerChannel') VALUES" +
                        "('2022-06-08T03:15:00.000000Z', '22', '3')," +
                        "('2022-06-08T03:15:00.000000Z', '23', '3')," +
                        "('2022-06-08T00:40:00.000000Z', '24', '3')", sqlExecutionContext
        );

        TestUtils.assertSql(
                compiler, sqlExecutionContext, "select * from monthly_col_top where loggerChannel = '3' order by ts, metric", sink,
                replaceTimestampSuffix1("""
                        ts\tmetric\tdiagnostic\tsensorChannel\tloggerChannel
                        2022-06-08T00:40:00.000000Z\t24\t\t\t3
                        2022-06-08T02:50:00.000000Z\t5\t\t\t3
                        2022-06-08T02:54:00.000000Z\t17\t\t\t3
                        2022-06-08T02:55:00.000000Z\t6\t\t\t3
                        2022-06-08T02:56:00.000000Z\t18\t\t\t3
                        2022-06-08T03:15:00.000000Z\t19\t\t\t3
                        2022-06-08T03:15:00.000000Z\t22\t\t\t3
                        2022-06-08T03:15:00.000000Z\t23\t\t\t3
                        2022-06-08T03:30:00.000000Z\t15\t\t2\t3
                        2022-06-08T03:30:00.000000Z\t16\t\t2\t3
                        2022-06-08T04:30:00.000000Z\t20\t\t\t3
                        2022-06-08T04:31:00.000000Z\t21\t\t\t3
                        """, timestampTypeName)
        );
    }

    private static void testRepeatedIngest0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException, NumericException {

        engine.execute("create table x (a int, ts " + timestampTypeName + ") timestamp(ts) partition by DAY", sqlExecutionContext);

        TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.typeOf(timestampTypeName));
        long ts = driver.parseFloorLiteral("2020-03-10T20:36:00.000000Z");
        long expectedMaxTimestamp = Long.MIN_VALUE;
        int step = 100;
        int rowCount = 10;
        try (TableWriter w = TestUtils.getWriter(engine, "x")) {
            for (int i = 0; i < 20; i++) {
                for (int j = 0; j < rowCount; j++) {
                    long t = ts + (rowCount - j) * step;
                    expectedMaxTimestamp = Math.max(expectedMaxTimestamp, t);
                    TableWriter.Row r = w.newRow(t);
                    r.putInt(0, i * step + j);
                    r.append();
                }
                w.commit();
                ts--;
            }
            Assert.assertEquals(expectedMaxTimestamp, w.getMaxTimestamp());
        }

        final String expected = replaceTimestampSuffix("""
                a\tts
                1909\t2020-03-10T20:36:00.000081Z
                1809\t2020-03-10T20:36:00.000082Z
                1709\t2020-03-10T20:36:00.000083Z
                1609\t2020-03-10T20:36:00.000084Z
                1509\t2020-03-10T20:36:00.000085Z
                1409\t2020-03-10T20:36:00.000086Z
                1309\t2020-03-10T20:36:00.000087Z
                1209\t2020-03-10T20:36:00.000088Z
                1109\t2020-03-10T20:36:00.000089Z
                1009\t2020-03-10T20:36:00.000090Z
                909\t2020-03-10T20:36:00.000091Z
                809\t2020-03-10T20:36:00.000092Z
                709\t2020-03-10T20:36:00.000093Z
                609\t2020-03-10T20:36:00.000094Z
                509\t2020-03-10T20:36:00.000095Z
                409\t2020-03-10T20:36:00.000096Z
                309\t2020-03-10T20:36:00.000097Z
                209\t2020-03-10T20:36:00.000098Z
                109\t2020-03-10T20:36:00.000099Z
                9\t2020-03-10T20:36:00.000100Z
                1908\t2020-03-10T20:36:00.000181Z
                1808\t2020-03-10T20:36:00.000182Z
                1708\t2020-03-10T20:36:00.000183Z
                1608\t2020-03-10T20:36:00.000184Z
                1508\t2020-03-10T20:36:00.000185Z
                1408\t2020-03-10T20:36:00.000186Z
                1308\t2020-03-10T20:36:00.000187Z
                1208\t2020-03-10T20:36:00.000188Z
                1108\t2020-03-10T20:36:00.000189Z
                1008\t2020-03-10T20:36:00.000190Z
                908\t2020-03-10T20:36:00.000191Z
                808\t2020-03-10T20:36:00.000192Z
                708\t2020-03-10T20:36:00.000193Z
                608\t2020-03-10T20:36:00.000194Z
                508\t2020-03-10T20:36:00.000195Z
                408\t2020-03-10T20:36:00.000196Z
                308\t2020-03-10T20:36:00.000197Z
                208\t2020-03-10T20:36:00.000198Z
                108\t2020-03-10T20:36:00.000199Z
                8\t2020-03-10T20:36:00.000200Z
                1907\t2020-03-10T20:36:00.000281Z
                1807\t2020-03-10T20:36:00.000282Z
                1707\t2020-03-10T20:36:00.000283Z
                1607\t2020-03-10T20:36:00.000284Z
                1507\t2020-03-10T20:36:00.000285Z
                1407\t2020-03-10T20:36:00.000286Z
                1307\t2020-03-10T20:36:00.000287Z
                1207\t2020-03-10T20:36:00.000288Z
                1107\t2020-03-10T20:36:00.000289Z
                1007\t2020-03-10T20:36:00.000290Z
                907\t2020-03-10T20:36:00.000291Z
                807\t2020-03-10T20:36:00.000292Z
                707\t2020-03-10T20:36:00.000293Z
                607\t2020-03-10T20:36:00.000294Z
                507\t2020-03-10T20:36:00.000295Z
                407\t2020-03-10T20:36:00.000296Z
                307\t2020-03-10T20:36:00.000297Z
                207\t2020-03-10T20:36:00.000298Z
                107\t2020-03-10T20:36:00.000299Z
                7\t2020-03-10T20:36:00.000300Z
                1906\t2020-03-10T20:36:00.000381Z
                1806\t2020-03-10T20:36:00.000382Z
                1706\t2020-03-10T20:36:00.000383Z
                1606\t2020-03-10T20:36:00.000384Z
                1506\t2020-03-10T20:36:00.000385Z
                1406\t2020-03-10T20:36:00.000386Z
                1306\t2020-03-10T20:36:00.000387Z
                1206\t2020-03-10T20:36:00.000388Z
                1106\t2020-03-10T20:36:00.000389Z
                1006\t2020-03-10T20:36:00.000390Z
                906\t2020-03-10T20:36:00.000391Z
                806\t2020-03-10T20:36:00.000392Z
                706\t2020-03-10T20:36:00.000393Z
                606\t2020-03-10T20:36:00.000394Z
                506\t2020-03-10T20:36:00.000395Z
                406\t2020-03-10T20:36:00.000396Z
                306\t2020-03-10T20:36:00.000397Z
                206\t2020-03-10T20:36:00.000398Z
                106\t2020-03-10T20:36:00.000399Z
                6\t2020-03-10T20:36:00.000400Z
                1905\t2020-03-10T20:36:00.000481Z
                1805\t2020-03-10T20:36:00.000482Z
                1705\t2020-03-10T20:36:00.000483Z
                1605\t2020-03-10T20:36:00.000484Z
                1505\t2020-03-10T20:36:00.000485Z
                1405\t2020-03-10T20:36:00.000486Z
                1305\t2020-03-10T20:36:00.000487Z
                1205\t2020-03-10T20:36:00.000488Z
                1105\t2020-03-10T20:36:00.000489Z
                1005\t2020-03-10T20:36:00.000490Z
                905\t2020-03-10T20:36:00.000491Z
                805\t2020-03-10T20:36:00.000492Z
                705\t2020-03-10T20:36:00.000493Z
                605\t2020-03-10T20:36:00.000494Z
                505\t2020-03-10T20:36:00.000495Z
                405\t2020-03-10T20:36:00.000496Z
                305\t2020-03-10T20:36:00.000497Z
                205\t2020-03-10T20:36:00.000498Z
                105\t2020-03-10T20:36:00.000499Z
                5\t2020-03-10T20:36:00.000500Z
                1904\t2020-03-10T20:36:00.000581Z
                1804\t2020-03-10T20:36:00.000582Z
                1704\t2020-03-10T20:36:00.000583Z
                1604\t2020-03-10T20:36:00.000584Z
                1504\t2020-03-10T20:36:00.000585Z
                1404\t2020-03-10T20:36:00.000586Z
                1304\t2020-03-10T20:36:00.000587Z
                1204\t2020-03-10T20:36:00.000588Z
                1104\t2020-03-10T20:36:00.000589Z
                1004\t2020-03-10T20:36:00.000590Z
                904\t2020-03-10T20:36:00.000591Z
                804\t2020-03-10T20:36:00.000592Z
                704\t2020-03-10T20:36:00.000593Z
                604\t2020-03-10T20:36:00.000594Z
                504\t2020-03-10T20:36:00.000595Z
                404\t2020-03-10T20:36:00.000596Z
                304\t2020-03-10T20:36:00.000597Z
                204\t2020-03-10T20:36:00.000598Z
                104\t2020-03-10T20:36:00.000599Z
                4\t2020-03-10T20:36:00.000600Z
                1903\t2020-03-10T20:36:00.000681Z
                1803\t2020-03-10T20:36:00.000682Z
                1703\t2020-03-10T20:36:00.000683Z
                1603\t2020-03-10T20:36:00.000684Z
                1503\t2020-03-10T20:36:00.000685Z
                1403\t2020-03-10T20:36:00.000686Z
                1303\t2020-03-10T20:36:00.000687Z
                1203\t2020-03-10T20:36:00.000688Z
                1103\t2020-03-10T20:36:00.000689Z
                1003\t2020-03-10T20:36:00.000690Z
                903\t2020-03-10T20:36:00.000691Z
                803\t2020-03-10T20:36:00.000692Z
                703\t2020-03-10T20:36:00.000693Z
                603\t2020-03-10T20:36:00.000694Z
                503\t2020-03-10T20:36:00.000695Z
                403\t2020-03-10T20:36:00.000696Z
                303\t2020-03-10T20:36:00.000697Z
                203\t2020-03-10T20:36:00.000698Z
                103\t2020-03-10T20:36:00.000699Z
                3\t2020-03-10T20:36:00.000700Z
                1902\t2020-03-10T20:36:00.000781Z
                1802\t2020-03-10T20:36:00.000782Z
                1702\t2020-03-10T20:36:00.000783Z
                1602\t2020-03-10T20:36:00.000784Z
                1502\t2020-03-10T20:36:00.000785Z
                1402\t2020-03-10T20:36:00.000786Z
                1302\t2020-03-10T20:36:00.000787Z
                1202\t2020-03-10T20:36:00.000788Z
                1102\t2020-03-10T20:36:00.000789Z
                1002\t2020-03-10T20:36:00.000790Z
                902\t2020-03-10T20:36:00.000791Z
                802\t2020-03-10T20:36:00.000792Z
                702\t2020-03-10T20:36:00.000793Z
                602\t2020-03-10T20:36:00.000794Z
                502\t2020-03-10T20:36:00.000795Z
                402\t2020-03-10T20:36:00.000796Z
                302\t2020-03-10T20:36:00.000797Z
                202\t2020-03-10T20:36:00.000798Z
                102\t2020-03-10T20:36:00.000799Z
                2\t2020-03-10T20:36:00.000800Z
                1901\t2020-03-10T20:36:00.000881Z
                1801\t2020-03-10T20:36:00.000882Z
                1701\t2020-03-10T20:36:00.000883Z
                1601\t2020-03-10T20:36:00.000884Z
                1501\t2020-03-10T20:36:00.000885Z
                1401\t2020-03-10T20:36:00.000886Z
                1301\t2020-03-10T20:36:00.000887Z
                1201\t2020-03-10T20:36:00.000888Z
                1101\t2020-03-10T20:36:00.000889Z
                1001\t2020-03-10T20:36:00.000890Z
                901\t2020-03-10T20:36:00.000891Z
                801\t2020-03-10T20:36:00.000892Z
                701\t2020-03-10T20:36:00.000893Z
                601\t2020-03-10T20:36:00.000894Z
                501\t2020-03-10T20:36:00.000895Z
                401\t2020-03-10T20:36:00.000896Z
                301\t2020-03-10T20:36:00.000897Z
                201\t2020-03-10T20:36:00.000898Z
                101\t2020-03-10T20:36:00.000899Z
                1\t2020-03-10T20:36:00.000900Z
                1900\t2020-03-10T20:36:00.000981Z
                1800\t2020-03-10T20:36:00.000982Z
                1700\t2020-03-10T20:36:00.000983Z
                1600\t2020-03-10T20:36:00.000984Z
                1500\t2020-03-10T20:36:00.000985Z
                1400\t2020-03-10T20:36:00.000986Z
                1300\t2020-03-10T20:36:00.000987Z
                1200\t2020-03-10T20:36:00.000988Z
                1100\t2020-03-10T20:36:00.000989Z
                1000\t2020-03-10T20:36:00.000990Z
                900\t2020-03-10T20:36:00.000991Z
                800\t2020-03-10T20:36:00.000992Z
                700\t2020-03-10T20:36:00.000993Z
                600\t2020-03-10T20:36:00.000994Z
                500\t2020-03-10T20:36:00.000995Z
                400\t2020-03-10T20:36:00.000996Z
                300\t2020-03-10T20:36:00.000997Z
                200\t2020-03-10T20:36:00.000998Z
                100\t2020-03-10T20:36:00.000999Z
                0\t2020-03-10T20:36:00.001000Z
                """, timestampTypeName);

        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "x",
                sink,
                expected
        );
    }

    private static void testSendDuplicates(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        // create table with roughly 2AM data
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 0 as commit," +
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
                        " cast(null as binary) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(510)" +
                        "), index(sym capacity 512) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // all records but one is appended to middle partition
        // last record is prepended to the last partition
        engine.execute(
                "create table append as (" +
                        "select" +
                        " 1 as commit," +
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
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute(
                "create table append2 as (" +
                        "select" +
                        " 2 as commit," +
                        " cast(x+100 as int) i," +
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
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        engine.execute(
                "create table append3 as (" +
                        "select" +
                        " 3 as commit," +
                        " cast(x + 200 as int) i," +
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
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "y order by ts, commit",
                "insert into x select * from append",
                "x",
                "y",
                "x"
        );

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );

        dropTableY(engine, sqlExecutionContext);
        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append2)",
                "y order by i,sym,amt",
                "insert into x select * from append2",
                "x order by i,sym,amt",
                "y",
                "x"
        );

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );

        dropTableY(engine, sqlExecutionContext);
        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append3)",
                "y order by i,sym,amt",
                "insert into x select * from append3",
                "x order by i,sym,amt",
                "y",
                "x"
        );

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );
    }

    private static void testTwoTablesCompeteForBuffer0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
                        "select" +
                        " rnd_str(5,16,10) i," +
                        " rnd_str(5,16,10) sym," +
                        " rnd_str(5,16,10) amt," +
                        " rnd_str(5,16,10) timestamp," +
                        " rnd_str(5,16,10) b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_str(5,16,10) d," +
                        " rnd_str(5,16,10) e," +
                        " rnd_str(5,16,10) f," +
                        " rnd_str(5,16,10) g," +
                        " rnd_str(5,16,10) ik," +
                        " rnd_str(5,16,10) j," +
                        " timestamp_sequence(500000000000L,100000000L)::" + timestampTypeName + " ts," +
                        " rnd_str(5,16,10) l," +
                        " rnd_str(5,16,10) m," +
                        " rnd_str(5,16,10) n," +
                        " rnd_str(5,16,10) t," +
                        " rnd_str(5,16,10) l256" +
                        " from long_sequence(10000)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        engine.execute("create table x1 as (x) timestamp(ts) partition by DAY", executionContext);

        engine.execute(
                "create table y as (" +
                        "select" +
                        " rnd_str(5,16,10) i," +
                        " rnd_str(5,16,10) sym," +
                        " rnd_str(5,16,10) amt," +
                        " rnd_str(5,16,10) timestamp," +
                        " rnd_str(5,16,10) b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_str(5,16,10) d," +
                        " rnd_str(5,16,10) e," +
                        " rnd_str(5,16,10) f," +
                        " rnd_str(5,16,10) g," +
                        " rnd_str(5,16,10) ik," +
                        " rnd_str(5,16,10) j," +
                        " timestamp_sequence(500000080000L,79999631L)::" + timestampTypeName + " ts," +
                        " rnd_str(5,16,10) l," +
                        " rnd_str(5,16,10) m," +
                        " rnd_str(5,16,10) n," +
                        " rnd_str(5,16,10) t," +
                        " rnd_str(5,16,10) l256" +
                        " from long_sequence(10000)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        engine.execute("create table y1 as (y)", executionContext);

        // create expected result sets
        engine.execute("create table z as (x union all y)", executionContext);

        // create another compiler to be used by second pool
        try (SqlCompiler compiler2 = engine.getSqlCompiler();
             final SqlExecutionContext executionContext2 = TestUtils.createSqlExecutionCtx(engine)) {

            final CyclicBarrier barrier = new CyclicBarrier(2);
            final SOCountDownLatch haltLatch = new SOCountDownLatch(2);
            final AtomicInteger errorCount = new AtomicInteger();

            // we have two pairs of tables (x,y) and (x1,y1)
            try (WorkerPool pool1 = new WorkerPool(() -> 1)) {

                pool1.assign(new Job() {
                    private boolean toRun = true;

                    @Override
                    public boolean run(int workerId, @NotNull RunStatus runStatus) {
                        if (toRun) {
                            try {
                                toRun = false;
                                barrier.await();
                                engine.execute("insert into x select * from y", executionContext);
                            } catch (Throwable e) {
                                //noinspection CallToPrintStackTrace
                                e.printStackTrace();
                                errorCount.incrementAndGet();
                            } finally {
                                haltLatch.countDown();
                            }
                        }
                        return false;
                    }
                });

                try (final WorkerPool pool2 = new TestWorkerPool(1)) {

                    pool2.assign(new Job() {
                        private boolean toRun = true;

                        @Override
                        public boolean run(int workerId, @NotNull RunStatus runStatus) {
                            if (toRun) {
                                try {
                                    toRun = false;
                                    barrier.await();
                                    try (InsertOperation op = compiler2.compile("insert into x1 select * from y1", executionContext2).popInsertOperation()) {
                                        op.execute(executionContext2);
                                    }
                                } catch (Throwable e) {
                                    //noinspection CallToPrintStackTrace
                                    e.printStackTrace();
                                    errorCount.incrementAndGet();
                                } finally {
                                    haltLatch.countDown();
                                }
                            }
                            return false;
                        }
                    });

                    pool1.start();
                    pool2.start();

                    haltLatch.await();

                    pool1.halt();
                    pool2.halt();
                }
            }

            Assert.assertEquals(0, errorCount.get());
            TestUtils.assertSqlCursors(compiler, executionContext, "z order by ts", "x", LOG);
            TestUtils.assertSqlCursors(compiler, executionContext, "z order by ts", "x1", LOG);

            engine.releaseAllWriters();
            TestUtils.assertSqlCursors(compiler, executionContext, "z order by ts", "x", LOG);
            TestUtils.assertSqlCursors(compiler, executionContext, "z order by ts", "x1", LOG);
        }
    }

    private static void testVanillaO3MaxLag0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        assertLag(
                engine,
                compiler,
                sqlExecutionContext,
                "",
                " from long_sequence(1000000)",
                "insert batch 100000 o3MaxLag 180s into x select * from top",
                timestampTypeName
        );
    }

    private static void testVanillaO3MaxLagSinglePartition0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException {
        engine.execute(
                "create table x as (" +
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
                        " timestamp_sequence(500000000000L,1000000L)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(0)" +
                        "), index(sym) timestamp (ts) partition by MONTH",
                sqlExecutionContext
        );

        engine.execute(
                "create table top as (" +
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
                        // the formula below creates lag-friendly timestamp distribution
                        " cast(500000000000L + ((x-1)/4) * 1000000L + (4-(x-1)%4)*80009  as timestamp)::" + timestampTypeName + " ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
                        " rnd_varchar(1, 40, 1) varc," +
                        " rnd_varchar(1, 1, 1) varc2," +
                        " rnd_double_array(2,1) arr," +
                        " from long_sequence(500)" +
                        ")",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all top)",
                // combination of row count in "top" table, batch size and lag value land
                // rows into single partition, which causes internal state to corrupt
                // please do not change these values unless you know what you're doing
                "insert batch 100 o3MaxLag 300s into x select * from top"
        );

        assertIndexConsistency(compiler, sqlExecutionContext, engine);

        assertMaxTimestamp(
                engine,
                sqlExecutionContext
        );
    }

    private static void testWriterOpensCorrectTxnPartitionOnRestart0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException, NumericException {
        CairoConfiguration configuration = engine.getConfiguration();
        TableModel x = new TableModel(configuration, "x", PartitionBy.DAY);
        TestUtils.createPopulateTable(
                compiler,
                sqlExecutionContext,
                x.col("id", ColumnType.LONG)
                        .col("ok", ColumnType.DOUBLE)
                        .timestamp("ts", ColumnType.typeOf(timestampTypeName)),
                10,
                "2020-01-01",
                1
        );

        // Insert OOO to create partition dir 2020-01-01.1
        engine.execute("insert into x values(1, 100.0, '2020-01-01T00:01:00')", sqlExecutionContext);
        TestUtils.assertSql(
                compiler, sqlExecutionContext, "select count() from x", sink,
                """
                        count
                        11
                        """
        );

        // Close and open writer. Partition dir 2020-01-01.1 should not be purged
        engine.releaseAllWriters();
        engine.execute("insert into x values(2, 101.0, '2020-01-01T00:02:00')", sqlExecutionContext);
        TestUtils.assertSql(
                compiler, sqlExecutionContext, "select count() from x", sink,
                """
                        count
                        12
                        """
        );
    }

    private static void testWriterOpensUnmappedPage(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String timestampTypeName
    ) throws SqlException, NumericException {
        CairoConfiguration configuration = engine.getConfiguration();
        TableModel tableModel = new TableModel(configuration, "x", PartitionBy.DAY);
        tableModel
                .col("id", ColumnType.LONG)
                .col("str", ColumnType.STRING)
                .col("sym", ColumnType.SYMBOL).indexed(true, 2)
                .timestamp("ts", ColumnType.typeOf(timestampTypeName));

        TestUtils.createPopulateTable(
                "x",
                compiler,
                sqlExecutionContext,
                tableModel,
                0,
                "2021-10-09",
                0
        );


        int idBatchSize = 3 * 1024 * 1024 + 1;
        long batchOnDiskSize = (long) ColumnType.sizeOf(ColumnType.LONG) * idBatchSize;
        long mappedPageSize = configuration.getDataAppendPageSize();
        Assert.assertTrue("Batch size must be greater than mapped page size", batchOnDiskSize > mappedPageSize);
        long pageSize = configuration.getMiscAppendPageSize();
        Assert.assertNotEquals("Batch size must be unaligned with page size", 0, batchOnDiskSize % pageSize);

        long start = MicrosTimestampDriver.floor("2021-10-09T10:00:00");
        String[] varCol = new String[]{"aldfjkasdlfkj", "2021-10-10T12:00:00", "12345678901234578"};
        Utf8Sequence[] varcharCol = new Utf8Sequence[]{
                new Utf8String("aldfjkasdlfkj"),
                new Utf8String("2021-10-10T12:00:00"),
                new Utf8String("12345678901234578")
        };

        int longColIndex = -1;
        int symColIndex = -1;
        int strColIndex = -1;
        int varcharColIndex = -1;

        // Add 2 batches
        int iterations = 2;
        try (TableWriter o3 = TestUtils.getWriter(engine, "x")) {

            TableMetadata metadata = o3.getMetadata();
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                switch (metadata.getColumnType(i)) {
                    case ColumnType.LONG:
                        longColIndex = i;
                        break;
                    case ColumnType.SYMBOL:
                        symColIndex = i;
                        break;
                    case ColumnType.STRING:
                        strColIndex = i;
                        break;
                    case ColumnType.VARCHAR:
                        varcharColIndex = i;
                        break;
                }
            }

            for (int i = 0; i < iterations; i++) {
                for (int id = 0; id < idBatchSize; id++) {
                    // We leave start + idBatchSize out to insert it O3 later
                    long timestamp = start + i * idBatchSize + id + i;
                    TableWriter.Row row = o3.newRow(timestamp);
                    row.putLong(longColIndex, timestamp);
                    row.putSym(symColIndex, "test");
                    if (strColIndex > -1) {
                        row.putStr(strColIndex, varCol[id % varCol.length]);
                    } else {
                        row.putVarchar(varcharColIndex, varcharCol[id % varCol.length]);
                    }
                    row.append();
                }

                // Commit only the first batch
                if (i == 0) {
                    o3.commit();
                }
            }

            // Append one more row out of order
            long timestamp = start + idBatchSize;
            TableWriter.Row row = o3.newRow(timestamp);
            row.putLong(longColIndex, timestamp);
            row.putSym(symColIndex, "test");
            if (strColIndex > -1) {
                row.putStr(strColIndex, varCol[0]);
            } else {
                row.putVarchar(varcharColIndex, varcharCol[0]);
            }
            row.append();

            o3.commit();
        }

        TestUtils.assertSql(
                compiler, sqlExecutionContext, "select count() from x", sink,
                "count\n" + (2 * idBatchSize + 1) + "\n"
        );
        engine.releaseAllReaders();
        try (TableWriter o3 = TestUtils.getWriter(engine, "x")) {
            o3.truncate();
        }
    }

    private static void testXAndIndex(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            CharSequence referenceSql,
            String assertSql
    ) throws SqlException {
        TestUtils.assertSqlCursors(compiler, sqlExecutionContext, referenceSql, assertSql, LOG, true);
        assertIndexConsistency(compiler, sqlExecutionContext, engine);

        // test that after reader is re-opened we can still see the same data
        engine.releaseAllReaders();
        TestUtils.assertSqlCursors(compiler, sqlExecutionContext, referenceSql, assertSql, LOG, true);
        assertIndexConsistency(compiler, sqlExecutionContext, engine);
    }

    private void appendNProducts(long ts, Rnd rnd, TableWriter writer, TimestampDriver driver) {
        int productId = writer.getColumnIndex("productId");
        int productName = writer.getColumnIndex("productName");
        int supplier = writer.getColumnIndex("supplier");
        int category = writer.getColumnIndex("category");
        int price = writer.getColumnIndex("price");
        boolean isSym = ColumnType.isSymbol(writer.getMetadata().getColumnType(productName));

        for (int i = 0; i < 1000; i++) {
            TableWriter.Row r = writer.newRow(ts += driver.fromMicros(60000L * 1000L));
            r.putInt(productId, rnd.nextPositiveInt());
            if (!isSym) {
                r.putStr(productName, rnd.nextString(4));
            } else {
                r.putSym(productName, rnd.nextString(4));
            }
            r.putSym(supplier, rnd.nextString(4));
            r.putSym(category, rnd.nextString(11));
            r.putDouble(price, rnd.nextDouble());
            r.append();
        }
    }

    private void appendNWithNewColumn(
            Rnd rnd,
            TableWriter writer,
            ObjList<CharSequence> newCols,
            IntList colTypes
    ) {
        int productId = writer.getColumnIndex("productId");
        int productName = writer.getColumnIndex("productName");
        int supplier = writer.getColumnIndex("supplier");
        int category = writer.getColumnIndex("category");
        int price = writer.getColumnIndex("price");

        IntHashSet set = new IntHashSet();
        set.add(productId);
        set.add(productName);
        set.add(supplier);
        set.add(category);
        set.add(price);

        IntList indexes = new IntList();
        for (int j = 0, m = newCols.size(); j < m; j++) {
            int columnIndex = writer.getColumnIndex(newCols.getQuick(j));
            indexes.add(columnIndex);
            Assert.assertFalse(set.contains(columnIndex));
            set.add(columnIndex);
        }

        for (int i = 0; i < 1000; i++) {
            TableWriter.Row r = writer.newRow();
            r.putInt(productId, rnd.nextPositiveInt());
            r.putStr(productName, rnd.nextString(10));
            r.putSym(supplier, rnd.nextString(4));
            r.putSym(category, rnd.nextString(11));
            r.putDouble(price, rnd.nextDouble());

            for (int j = 0; j < indexes.size(); ++j) {
                switch (ColumnType.tagOf(colTypes.get(j))) {
                    case ColumnType.BOOLEAN:
                        r.putBool(indexes.get(j), rnd.nextBoolean());
                        break;
                    case ColumnType.BYTE:
                    case ColumnType.GEOBYTE:
                        r.putByte(indexes.get(j), rnd.nextByte());
                        break;
                    case ColumnType.SHORT:
                    case ColumnType.GEOSHORT:
                        r.putShort(indexes.get(j), rnd.nextShort());
                        break;
                    case ColumnType.CHAR:
                        r.putChar(indexes.get(j), rnd.nextChar());
                        break;
                    case ColumnType.INT:
                    case ColumnType.IPv4:
                    case ColumnType.GEOINT:
                        r.putInt(indexes.get(j), rnd.nextInt());
                        break;
                    case ColumnType.LONG:
                        r.putLong(indexes.get(j), rnd.nextLong());
                        break;
                    case ColumnType.DATE:
                        r.putDate(indexes.get(j), rnd.nextLong());
                        break;
                    case ColumnType.TIMESTAMP:
                        r.putTimestamp(indexes.get(j), rnd.nextLong());
                        break;
                    case ColumnType.FLOAT:
                        r.putFloat(indexes.get(j), rnd.nextFloat());
                        break;
                    case ColumnType.DOUBLE:
                        r.putDouble(indexes.get(j), rnd.nextDouble());
                        break;
                    case ColumnType.STRING:
                        r.putStr(indexes.get(j), rnd.nextString(10));
                        break;
                    case ColumnType.SYMBOL:
                        r.putSym(indexes.get(j), rnd.nextString(5));
                        break;
                    case ColumnType.LONG256:
                        r.putLong256(indexes.get(j), rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
                        break;
                    case ColumnType.GEOLONG:
                        r.putGeoHash(indexes.get(j), rnd.nextLong());
                        break;
                    case ColumnType.BINARY:
                        r.putBin(indexes.get(j), (new TestRecord.ArrayBinarySequence()).of(rnd.nextBytes(25)));
                        break;
                    case ColumnType.VAR_ARG:
                        r.putStr(indexes.get(j), rnd.nextString(20));
                        break;
                }
            }
            r.append();
        }
    }

    private void testVarColumnPageBoundaryIterationWithColumnTop(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, int i, String o3Timestamp) throws SqlException {
        // Day 1 '1970-01-01'
        int initialCount = i / 2;
        engine.execute(
                "create table x as (" +
                        "select" +
                        " 'aa' as str," +
                        " timestamp_sequence('1970-01-01T11:00:00',1000L)::" + timestampType.getTypeName() + " ts," +
                        " x " +
                        " from long_sequence(" + initialCount + ")" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // Day 2 '1970-01-02'
        engine.execute(
                "insert into x " +
                        "select" +
                        " 'bb' as str," +
                        " timestamp_sequence('1970-01-02T11:00:00',1000L)::" + timestampType.getTypeName() + " ts," +
                        " x " +
                        " from long_sequence(" + initialCount + ")", sqlExecutionContext
        );

        engine.execute("alter table x add column str2 string", sqlExecutionContext);
        engine.execute("alter table x add column y long", sqlExecutionContext);

        if (i % 2 == 0) {
            engine.releaseAllWriters();
        }

        // O3 insert Day 1
        final String ts1 = "1970-01-01T" + o3Timestamp;
        final String ts2 = "1970-01-02T" + o3Timestamp;
        engine.execute(
                "insert into x " +
                        " select" +
                        " 'cc' as str," +
                        " timestamp_sequence('" + ts1 + "',0L)::" + timestampType.getTypeName() + " ts," +
                        " 11111 as x," +
                        " 'dd' as str2," +
                        " 22222 as y" +
                        " from long_sequence(1)" +
                        "union all " +
                        " select" +
                        " 'cc' as str," +
                        " timestamp_sequence('" + ts2 + "',0L)::" + timestampType.getTypeName() + " ts," +
                        " 11111 as x," +
                        " 'dd' as str2," +
                        " 22222 as y" +
                        " from long_sequence(1)", sqlExecutionContext
        );

        if (i % 2 == 0) {
            engine.releaseAllWriters();
        }

        TestUtils.assertSql(
                compiler, sqlExecutionContext, "select * from x where str = 'cc'", sink,
                "str\tts\tx\tstr2\ty\n" +
                        "cc\t" + ts1 + "\t11111\tdd\t22222\n" +
                        "cc\t" + ts2 + "\t11111\tdd\t22222\n"
        );
    }
}
