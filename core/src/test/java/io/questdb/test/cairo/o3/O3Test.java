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
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.Job;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Chars;
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
import io.questdb.test.TestTimestampType;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.vm.Vm.getStorageLength;
import static io.questdb.test.AbstractCairoTest.replaceTimestampSuffix;
import static io.questdb.test.AbstractCairoTest.replaceTimestampSuffix1;

@RunWith(Parameterized.class)
public class O3Test extends AbstractO3Test {
    private final StringBuilder tstData = new StringBuilder();

    public O3Test(TestTimestampType timestampType) {
        super(timestampType);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

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
        if (Os.type != Os.DARWIN) {
            executeVanilla((engine, compiler, context) -> testBench0(engine, compiler, context, timestampType.getTypeName()));
        }
    }

    @Test
    public void testBenchContended() throws Exception {
        // On OSX, it's not trivial to increase the open file limit per process
        if (Os.type != Os.DARWIN) {
            executeWithPool(0, O3Test::testBench0);
        }
    }

    @Test
    public void testBenchParallel() throws Exception {
        // On OSX, it's not trivial to increase the open file limit per process
        if (Os.type != Os.DARWIN) {
            executeWithPool(4, O3Test::testBench0);
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

        final String expected = replaceTimestampSuffix1("i\tj\tts\tv\n" +
                "1\t4689592037643856\t1970-01-06T18:53:20.000000Z\tnull\n" +
                "2\t4729996258992366\t1970-01-06T18:55:00.000000Z\tnull\n" +
                "3\t7746536061816329025\t1970-01-06T18:56:40.000000Z\tnull\n" +
                "4\t-6945921502384501475\t1970-01-06T18:58:20.000000Z\tnull\n" +
                "5\t8260188555232587029\t1970-01-06T19:00:00.000000Z\tnull\n" +
                "6\t8920866532787660373\t1970-01-06T19:01:40.000000Z\tnull\n" +
                "7\t-7611843578141082998\t1970-01-06T19:03:20.000000Z\tnull\n" +
                "8\t-5354193255228091881\t1970-01-06T19:05:00.000000Z\tnull\n" +
                "9\t-2653407051020864006\t1970-01-06T19:06:40.000000Z\tnull\n" +
                "10\t-1675638984090602536\t1970-01-06T19:08:20.000000Z\tnull\n" +
                "11\t8754899401270281932\t1970-01-06T19:10:00.000000Z\tnull\n" +
                "12\t3614738589890112276\t1970-01-06T19:11:40.000000Z\tnull\n" +
                "13\t7513930126251977934\t1970-01-06T19:13:20.000000Z\tnull\n" +
                "14\t-7489826605295361807\t1970-01-06T19:15:00.000000Z\tnull\n" +
                "15\t-4094902006239100839\t1970-01-06T19:16:40.000000Z\tnull\n" +
                "16\t-4474835130332302712\t1970-01-06T19:18:20.000000Z\tnull\n" +
                "17\t-6943924477733600060\t1970-01-06T19:20:00.000000Z\tnull\n" +
                "18\t8173439391403617681\t1970-01-06T19:21:40.000000Z\tnull\n" +
                "19\t3394168647660478011\t1970-01-06T19:23:20.000000Z\tnull\n" +
                "20\t5408639942391651698\t1970-01-06T19:25:00.000000Z\tnull\n" +
                "21\t7953532976996720859\t1970-01-06T19:26:40.000000Z\tnull\n" +
                "22\t-8968886490993754893\t1970-01-06T19:28:20.000000Z\tnull\n" +
                "23\t6236292340460979716\t1970-01-06T19:30:00.000000Z\tnull\n" +
                "24\t8336855953317473051\t1970-01-06T19:31:40.000000Z\tnull\n" +
                "25\t-3985256597569472057\t1970-01-06T19:33:20.000000Z\tnull\n" +
                "26\t-8284534269888369016\t1970-01-06T19:35:00.000000Z\tnull\n" +
                "27\t9116006198143953886\t1970-01-06T19:36:40.000000Z\tnull\n" +
                "28\t-6856503215590263904\t1970-01-06T19:38:20.000000Z\tnull\n" +
                "29\t-8671107786057422727\t1970-01-06T19:40:00.000000Z\tnull\n" +
                "30\t5539350449504785212\t1970-01-06T19:41:40.000000Z\tnull\n" +
                "31\t4086802474270249591\t1970-01-06T19:43:20.000000Z\tnull\n" +
                "32\t7039584373105579285\t1970-01-06T19:45:00.000000Z\tnull\n" +
                "33\t-4485747798769957016\t1970-01-06T19:46:40.000000Z\tnull\n" +
                "34\t-4100339045953973663\t1970-01-06T19:48:20.000000Z\tnull\n" +
                "35\t-7475784581806461658\t1970-01-06T19:50:00.000000Z\tnull\n" +
                "36\t5926373848160552879\t1970-01-06T19:51:40.000000Z\tnull\n" +
                "37\t375856366519011353\t1970-01-06T19:53:20.000000Z\tnull\n" +
                "38\t2811900023577169860\t1970-01-06T19:55:00.000000Z\tnull\n" +
                "39\t8416773233910814357\t1970-01-06T19:56:40.000000Z\tnull\n" +
                "40\t6600081143067978388\t1970-01-06T19:58:20.000000Z\tnull\n" +
                "41\t8349358446893356086\t1970-01-06T20:00:00.000000Z\tnull\n" +
                "42\t7700030475747712339\t1970-01-06T20:01:40.000000Z\tnull\n" +
                "43\t8000176386900538697\t1970-01-06T20:03:20.000000Z\tnull\n" +
                "44\t-8479285918156402508\t1970-01-06T20:05:00.000000Z\tnull\n" +
                "45\t3958193676455060057\t1970-01-06T20:06:40.000000Z\tnull\n" +
                "46\t9194293009132827518\t1970-01-06T20:08:20.000000Z\tnull\n" +
                "47\t7759636733976435003\t1970-01-06T20:10:00.000000Z\tnull\n" +
                "48\t8942747579519338504\t1970-01-06T20:11:40.000000Z\tnull\n" +
                "49\t-7166640824903897951\t1970-01-06T20:13:20.000000Z\tnull\n" +
                "50\t7199909180655756830\t1970-01-06T20:15:00.000000Z\tnull\n" +
                "51\t-8889930662239044040\t1970-01-06T20:16:40.000000Z\tnull\n" +
                "52\t-4442449726822927731\t1970-01-06T20:18:20.000000Z\tnull\n" +
                "53\t-3546540271125917157\t1970-01-06T20:20:00.000000Z\tnull\n" +
                "54\t6404066507400987550\t1970-01-06T20:21:40.000000Z\tnull\n" +
                "55\t6854658259142399220\t1970-01-06T20:23:20.000000Z\tnull\n" +
                "56\t-4842723177835140152\t1970-01-06T20:25:00.000000Z\tnull\n" +
                "57\t-5986859522579472839\t1970-01-06T20:26:40.000000Z\tnull\n" +
                "58\t8573481508564499209\t1970-01-06T20:28:20.000000Z\tnull\n" +
                "59\t5476540218465058302\t1970-01-06T20:30:00.000000Z\tnull\n" +
                "60\t7709707078566863064\t1970-01-06T20:31:40.000000Z\tnull\n" +
                "61\t6270672455202306717\t1970-01-06T20:33:20.000000Z\tnull\n" +
                "62\t-8480005421611953360\t1970-01-06T20:35:00.000000Z\tnull\n" +
                "63\t-8955092533521658248\t1970-01-06T20:36:40.000000Z\tnull\n" +
                "64\t1205595184115760694\t1970-01-06T20:38:20.000000Z\tnull\n" +
                "65\t3619114107112892010\t1970-01-06T20:40:00.000000Z\tnull\n" +
                "66\t8325936937764905778\t1970-01-06T20:41:40.000000Z\tnull\n" +
                "67\t-7723703968879725602\t1970-01-06T20:43:20.000000Z\tnull\n" +
                "68\t-6186964045554120476\t1970-01-06T20:45:00.000000Z\tnull\n" +
                "69\t-4986232506486815364\t1970-01-06T20:46:40.000000Z\tnull\n" +
                "70\t-7885528361265853230\t1970-01-06T20:48:20.000000Z\tnull\n" +
                "71\t-6794405451419334859\t1970-01-06T20:50:00.000000Z\tnull\n" +
                "72\t-6253307669002054137\t1970-01-06T20:51:40.000000Z\tnull\n" +
                "73\t6820495939660535106\t1970-01-06T20:53:20.000000Z\tnull\n" +
                "74\t3152466304308949756\t1970-01-06T20:55:00.000000Z\tnull\n" +
                "75\t3705833798044144433\t1970-01-06T20:56:40.000000Z\tnull\n" +
                "76\t6993925225312419449\t1970-01-06T20:58:20.000000Z\tnull\n" +
                "77\t7304706257487484767\t1970-01-06T21:00:00.000000Z\tnull\n" +
                "78\t6179044593759294347\t1970-01-06T21:01:40.000000Z\tnull\n" +
                "79\t4238042693748641409\t1970-01-06T21:03:20.000000Z\tnull\n" +
                "80\t5334238747895433003\t1970-01-06T21:05:00.000000Z\tnull\n" +
                "81\t-7439145921574737517\t1970-01-06T21:06:40.000000Z\tnull\n" +
                "82\t-7153335833712179123\t1970-01-06T21:08:20.000000Z\tnull\n" +
                "83\t7392877322819819290\t1970-01-06T21:10:00.000000Z\tnull\n" +
                "84\t5536695302686527374\t1970-01-06T21:11:40.000000Z\tnull\n" +
                "85\t-8811278461560712840\t1970-01-06T21:13:20.000000Z\tnull\n" +
                "86\t-4371031944620334155\t1970-01-06T21:15:00.000000Z\tnull\n" +
                "87\t-5228148654835984711\t1970-01-06T21:16:40.000000Z\tnull\n" +
                "88\t6953604195888525841\t1970-01-06T21:18:20.000000Z\tnull\n" +
                "89\t7585187984144261203\t1970-01-06T21:20:00.000000Z\tnull\n" +
                "90\t-6919361415374675248\t1970-01-06T21:21:40.000000Z\tnull\n" +
                "91\t5942480340792044027\t1970-01-06T21:23:20.000000Z\tnull\n" +
                "92\t2968650253814730084\t1970-01-06T21:25:00.000000Z\tnull\n" +
                "93\t9036423629723776443\t1970-01-06T21:26:40.000000Z\tnull\n" +
                "94\t-7316123607359392486\t1970-01-06T21:28:20.000000Z\tnull\n" +
                "95\t7641144929328646356\t1970-01-06T21:30:00.000000Z\tnull\n" +
                "96\t8171230234890248944\t1970-01-06T21:31:40.000000Z\tnull\n" +
                "97\t-7689224645273531603\t1970-01-06T21:33:20.000000Z\tnull\n" +
                "98\t-7611030538224290496\t1970-01-06T21:35:00.000000Z\tnull\n" +
                "99\t-7266580375914176030\t1970-01-06T21:36:40.000000Z\tnull\n" +
                "100\t-5233802075754153909\t1970-01-06T21:38:20.000000Z\tnull\n" +
                "101\t-4692986177227268943\t1970-01-06T21:40:00.000000Z\tnull\n" +
                "102\t7528475600160271422\t1970-01-06T21:41:40.000000Z\tnull\n" +
                "103\t6473208488991371747\t1970-01-06T21:43:20.000000Z\tnull\n" +
                "104\t-4091897709796604687\t1970-01-06T21:45:00.000000Z\tnull\n" +
                "105\t-3107239868490395663\t1970-01-06T21:46:40.000000Z\tnull\n" +
                "106\t7522482991756933150\t1970-01-06T21:48:20.000000Z\tnull\n" +
                "107\t5866052386674669514\t1970-01-06T21:50:00.000000Z\tnull\n" +
                "108\t8831607763082315932\t1970-01-06T21:51:40.000000Z\tnull\n" +
                "109\t3518554007419864093\t1970-01-06T21:53:20.000000Z\tnull\n" +
                "110\t571924429013198086\t1970-01-06T21:55:00.000000Z\tnull\n" +
                "111\t5271904137583983788\t1970-01-06T21:56:40.000000Z\tnull\n" +
                "112\t-6487422186320825289\t1970-01-06T21:58:20.000000Z\tnull\n" +
                "113\t-5935729153136649272\t1970-01-06T22:00:00.000000Z\tnull\n" +
                "114\t-5028301966399563827\t1970-01-06T22:01:40.000000Z\tnull\n" +
                "115\t-4608960730952244094\t1970-01-06T22:03:20.000000Z\tnull\n" +
                "116\t-7387846268299105911\t1970-01-06T22:05:00.000000Z\tnull\n" +
                "117\t7848851757452822827\t1970-01-06T22:06:40.000000Z\tnull\n" +
                "118\t6373284943859989837\t1970-01-06T22:08:20.000000Z\tnull\n" +
                "119\t4014104627539596639\t1970-01-06T22:10:00.000000Z\tnull\n" +
                "120\t5867661438830308598\t1970-01-06T22:11:40.000000Z\tnull\n" +
                "121\t-6365568807668711866\t1970-01-06T22:13:20.000000Z\tnull\n" +
                "122\t-3214230645884399728\t1970-01-06T22:15:00.000000Z\tnull\n" +
                "123\t9029468389542245059\t1970-01-06T22:16:40.000000Z\tnull\n" +
                "124\t4349785000461902003\t1970-01-06T22:18:20.000000Z\tnull\n" +
                "125\t-8081265393416742311\t1970-01-06T22:20:00.000000Z\tnull\n" +
                "126\t-8663526666273545842\t1970-01-06T22:21:40.000000Z\tnull\n" +
                "127\t7122109662042058469\t1970-01-06T22:23:20.000000Z\tnull\n" +
                "128\t6079275973105085025\t1970-01-06T22:25:00.000000Z\tnull\n" +
                "129\t8155981915549526575\t1970-01-06T22:26:40.000000Z\tnull\n" +
                "130\t-4908948886680892316\t1970-01-06T22:28:20.000000Z\tnull\n" +
                "131\t8587391969565958670\t1970-01-06T22:30:00.000000Z\tnull\n" +
                "132\t4167328623064065836\t1970-01-06T22:31:40.000000Z\tnull\n" +
                "133\t-8906871108655466881\t1970-01-06T22:33:20.000000Z\tnull\n" +
                "134\t-5512653573876168745\t1970-01-06T22:35:00.000000Z\tnull\n" +
                "135\t-6161552193869048721\t1970-01-06T22:36:40.000000Z\tnull\n" +
                "136\t-8425379692364264520\t1970-01-06T22:38:20.000000Z\tnull\n" +
                "137\t9131882544462008265\t1970-01-06T22:40:00.000000Z\tnull\n" +
                "138\t-6626590012581323602\t1970-01-06T22:41:40.000000Z\tnull\n" +
                "139\t8654368763944235816\t1970-01-06T22:43:20.000000Z\tnull\n" +
                "140\t1504966027220213191\t1970-01-06T22:45:00.000000Z\tnull\n" +
                "141\t2474001847338644868\t1970-01-06T22:46:40.000000Z\tnull\n" +
                "142\t8977823376202838087\t1970-01-06T22:48:20.000000Z\tnull\n" +
                "143\t-7995393784734742820\t1970-01-06T22:50:00.000000Z\tnull\n" +
                "144\t-6190031864817509934\t1970-01-06T22:51:40.000000Z\tnull\n" +
                "145\t8702525427024484485\t1970-01-06T22:53:20.000000Z\tnull\n" +
                "146\t2762535352290012031\t1970-01-06T22:55:00.000000Z\tnull\n" +
                "147\t-8408704077728333147\t1970-01-06T22:56:40.000000Z\tnull\n" +
                "148\t-4116381468144676168\t1970-01-06T22:58:20.000000Z\tnull\n" +
                "149\t8611582118025429627\t1970-01-06T23:00:00.000000Z\tnull\n" +
                "150\t2235053888582262602\t1970-01-06T23:01:40.000000Z\tnull\n" +
                "151\t915906628308577949\t1970-01-06T23:03:20.000000Z\tnull\n" +
                "152\t1761725072747471430\t1970-01-06T23:05:00.000000Z\tnull\n" +
                "153\t5407260416602246268\t1970-01-06T23:06:40.000000Z\tnull\n" +
                "154\t-5710210982977201267\t1970-01-06T23:08:20.000000Z\tnull\n" +
                "155\t-9128506055317587235\t1970-01-06T23:10:00.000000Z\tnull\n" +
                "156\t9063592617902736531\t1970-01-06T23:11:40.000000Z\tnull\n" +
                "157\t-2406077911451945242\t1970-01-06T23:13:20.000000Z\tnull\n" +
                "158\t-6003256558990918704\t1970-01-06T23:15:00.000000Z\tnull\n" +
                "159\t6623443272143014835\t1970-01-06T23:16:40.000000Z\tnull\n" +
                "160\t-8082754367165748693\t1970-01-06T23:18:20.000000Z\tnull\n" +
                "161\t-1438352846894825721\t1970-01-06T23:20:00.000000Z\tnull\n" +
                "162\t-5439556746612026472\t1970-01-06T23:21:40.000000Z\tnull\n" +
                "163\t-7256514778130150964\t1970-01-06T23:23:20.000000Z\tnull\n" +
                "164\t-2605516556381756042\t1970-01-06T23:25:00.000000Z\tnull\n" +
                "165\t-7103100524321179064\t1970-01-06T23:26:40.000000Z\tnull\n" +
                "166\t9144172287200792483\t1970-01-06T23:28:20.000000Z\tnull\n" +
                "167\t-5024542231726589509\t1970-01-06T23:30:00.000000Z\tnull\n" +
                "168\t-2768987637252864412\t1970-01-06T23:31:40.000000Z\tnull\n" +
                "169\t-3289070757475856942\t1970-01-06T23:33:20.000000Z\tnull\n" +
                "170\t7277991313017866925\t1970-01-06T23:35:00.000000Z\tnull\n" +
                "171\t6574958665733670985\t1970-01-06T23:36:40.000000Z\tnull\n" +
                "172\t-5817309269683380708\t1970-01-06T23:38:20.000000Z\tnull\n" +
                "173\t-8910603140262731534\t1970-01-06T23:40:00.000000Z\tnull\n" +
                "174\t7035958104135945276\t1970-01-06T23:41:40.000000Z\tnull\n" +
                "175\t9169223215810156269\t1970-01-06T23:43:20.000000Z\tnull\n" +
                "176\t7973684666911773753\t1970-01-06T23:45:00.000000Z\tnull\n" +
                "177\t9143800334706665900\t1970-01-06T23:46:40.000000Z\tnull\n" +
                "178\t8907283191913183400\t1970-01-06T23:48:20.000000Z\tnull\n" +
                "179\t7505077128008208443\t1970-01-06T23:50:00.000000Z\tnull\n" +
                "180\t6624299878707135910\t1970-01-06T23:51:40.000000Z\tnull\n" +
                "181\t4990844051702733276\t1970-01-06T23:53:20.000000Z\tnull\n" +
                "182\t3446015290144635451\t1970-01-06T23:55:00.000000Z\tnull\n" +
                "183\t3393210801760647293\t1970-01-06T23:56:40.000000Z\tnull\n" +
                "184\t-8193596495481093333\t1970-01-06T23:58:20.000000Z\tnull\n" +
                "10\t3500000\t1970-01-06T23:58:20.000000Z\t10.2\n" +
                "185\t9130722816060153827\t1970-01-07T00:00:00.000000Z\tnull\n" +
                "186\t4385246274849842834\t1970-01-07T00:01:40.000000Z\tnull\n" +
                "187\t-7709579215942154242\t1970-01-07T00:03:20.000000Z\tnull\n" +
                "188\t-6912707344119330199\t1970-01-07T00:05:00.000000Z\tnull\n" +
                "189\t-6265628144430971336\t1970-01-07T00:06:40.000000Z\tnull\n" +
                "190\t-2656704586686189855\t1970-01-07T00:08:20.000000Z\tnull\n" +
                "191\t-5852887087189258121\t1970-01-07T00:10:00.000000Z\tnull\n" +
                "192\t-5616524194087992934\t1970-01-07T00:11:40.000000Z\tnull\n" +
                "193\t8889492928577876455\t1970-01-07T00:13:20.000000Z\tnull\n" +
                "194\t5398991075259361292\t1970-01-07T00:15:00.000000Z\tnull\n" +
                "195\t-4947578609540920695\t1970-01-07T00:16:40.000000Z\tnull\n" +
                "196\t-1550912036246807020\t1970-01-07T00:18:20.000000Z\tnull\n" +
                "197\t-3279062567400130728\t1970-01-07T00:20:00.000000Z\tnull\n" +
                "198\t-6187389706549636253\t1970-01-07T00:21:40.000000Z\tnull\n" +
                "199\t-5097437605148611401\t1970-01-07T00:23:20.000000Z\tnull\n" +
                "200\t-9053195266501182270\t1970-01-07T00:25:00.000000Z\tnull\n" +
                "201\t1064753200933634719\t1970-01-07T00:26:40.000000Z\tnull\n" +
                "202\t2155318342410845737\t1970-01-07T00:28:20.000000Z\tnull\n" +
                "203\t4437331957970287246\t1970-01-07T00:30:00.000000Z\tnull\n" +
                "204\t8152044974329490473\t1970-01-07T00:31:40.000000Z\tnull\n" +
                "205\t6108846371653428062\t1970-01-07T00:33:20.000000Z\tnull\n" +
                "206\t4641238585508069993\t1970-01-07T00:35:00.000000Z\tnull\n" +
                "207\t-5315599072928175674\t1970-01-07T00:36:40.000000Z\tnull\n" +
                "208\t-8755128364143858197\t1970-01-07T00:38:20.000000Z\tnull\n" +
                "209\t5294917053935522538\t1970-01-07T00:40:00.000000Z\tnull\n" +
                "210\t5824745791075827139\t1970-01-07T00:41:40.000000Z\tnull\n" +
                "211\t-8757007522346766135\t1970-01-07T00:43:20.000000Z\tnull\n" +
                "212\t-1620198143795539853\t1970-01-07T00:45:00.000000Z\tnull\n" +
                "213\t9161691782935400339\t1970-01-07T00:46:40.000000Z\tnull\n" +
                "214\t5703149806881083206\t1970-01-07T00:48:20.000000Z\tnull\n" +
                "215\t-6071768268784020226\t1970-01-07T00:50:00.000000Z\tnull\n" +
                "216\t-5336116148746766654\t1970-01-07T00:51:40.000000Z\tnull\n" +
                "217\t8009040003356908243\t1970-01-07T00:53:20.000000Z\tnull\n" +
                "218\t5292387498953709416\t1970-01-07T00:55:00.000000Z\tnull\n" +
                "219\t-6786804316219531143\t1970-01-07T00:56:40.000000Z\tnull\n" +
                "220\t-1798101751056570485\t1970-01-07T00:58:20.000000Z\tnull\n" +
                "221\t-8323443786521150653\t1970-01-07T01:00:00.000000Z\tnull\n" +
                "222\t-7714378722470181347\t1970-01-07T01:01:40.000000Z\tnull\n" +
                "223\t-2888119746454814889\t1970-01-07T01:03:20.000000Z\tnull\n" +
                "224\t-8546113611224784332\t1970-01-07T01:05:00.000000Z\tnull\n" +
                "225\t7158971986470055172\t1970-01-07T01:06:40.000000Z\tnull\n" +
                "226\t5746626297238459939\t1970-01-07T01:08:20.000000Z\tnull\n" +
                "227\t7574443524652611981\t1970-01-07T01:10:00.000000Z\tnull\n" +
                "228\t-8994301462266164776\t1970-01-07T01:11:40.000000Z\tnull\n" +
                "229\t4099611147050818391\t1970-01-07T01:13:20.000000Z\tnull\n" +
                "230\t-9147563299122452591\t1970-01-07T01:15:00.000000Z\tnull\n" +
                "231\t-7400476385601852536\t1970-01-07T01:16:40.000000Z\tnull\n" +
                "232\t-8642609626818201048\t1970-01-07T01:18:20.000000Z\tnull\n" +
                "233\t-2000273984235276379\t1970-01-07T01:20:00.000000Z\tnull\n" +
                "234\t-166300099372695016\t1970-01-07T01:21:40.000000Z\tnull\n" +
                "235\t-3416748419425937005\t1970-01-07T01:23:20.000000Z\tnull\n" +
                "236\t6351664568801157821\t1970-01-07T01:25:00.000000Z\tnull\n" +
                "237\t3084117448873356811\t1970-01-07T01:26:40.000000Z\tnull\n" +
                "238\t6601850686822460257\t1970-01-07T01:28:20.000000Z\tnull\n" +
                "239\t7759595275644638709\t1970-01-07T01:30:00.000000Z\tnull\n" +
                "240\t4360855047041000285\t1970-01-07T01:31:40.000000Z\tnull\n" +
                "241\t6087087705757854416\t1970-01-07T01:33:20.000000Z\tnull\n" +
                "242\t-5103414617212558357\t1970-01-07T01:35:00.000000Z\tnull\n" +
                "243\t8574802735490373479\t1970-01-07T01:36:40.000000Z\tnull\n" +
                "244\t2387397055355257412\t1970-01-07T01:38:20.000000Z\tnull\n" +
                "245\t8072168822566640807\t1970-01-07T01:40:00.000000Z\tnull\n" +
                "246\t-3293392739929464726\t1970-01-07T01:41:40.000000Z\tnull\n" +
                "247\t-8749723816463910031\t1970-01-07T01:43:20.000000Z\tnull\n" +
                "248\t6127579245089953588\t1970-01-07T01:45:00.000000Z\tnull\n" +
                "249\t-6883412613642983200\t1970-01-07T01:46:40.000000Z\tnull\n" +
                "250\t-7153690499922882896\t1970-01-07T01:48:20.000000Z\tnull\n" +
                "251\t7107508275327837161\t1970-01-07T01:50:00.000000Z\tnull\n" +
                "252\t-8260644133007073640\t1970-01-07T01:51:40.000000Z\tnull\n" +
                "253\t-7336930007738575369\t1970-01-07T01:53:20.000000Z\tnull\n" +
                "254\t5552835357100545895\t1970-01-07T01:55:00.000000Z\tnull\n" +
                "255\t4534912711595148130\t1970-01-07T01:56:40.000000Z\tnull\n" +
                "256\t-7228011205059401944\t1970-01-07T01:58:20.000000Z\tnull\n" +
                "257\t-6703401424236463520\t1970-01-07T02:00:00.000000Z\tnull\n" +
                "258\t-8857660828600848720\t1970-01-07T02:01:40.000000Z\tnull\n" +
                "259\t-3105499275013799956\t1970-01-07T02:03:20.000000Z\tnull\n" +
                "260\t-8371487291073160693\t1970-01-07T02:05:00.000000Z\tnull\n" +
                "261\t2383285963471887250\t1970-01-07T02:06:40.000000Z\tnull\n" +
                "262\t1488156692375549016\t1970-01-07T02:08:20.000000Z\tnull\n" +
                "263\t2151565237758036093\t1970-01-07T02:10:00.000000Z\tnull\n" +
                "264\t4107109535030235684\t1970-01-07T02:11:40.000000Z\tnull\n" +
                "265\t-8534688874718947140\t1970-01-07T02:13:20.000000Z\tnull\n" +
                "266\t-3491277789316049618\t1970-01-07T02:15:00.000000Z\tnull\n" +
                "267\t8815523022464325728\t1970-01-07T02:16:40.000000Z\tnull\n" +
                "268\t4959459375462458218\t1970-01-07T02:18:20.000000Z\tnull\n" +
                "269\t7037372650941669660\t1970-01-07T02:20:00.000000Z\tnull\n" +
                "270\t4502522085684189707\t1970-01-07T02:21:40.000000Z\tnull\n" +
                "271\t8850915006829016608\t1970-01-07T02:23:20.000000Z\tnull\n" +
                "272\t-8095658968635787358\t1970-01-07T02:25:00.000000Z\tnull\n" +
                "273\t-6716055087713781882\t1970-01-07T02:26:40.000000Z\tnull\n" +
                "274\t-8425895280081943671\t1970-01-07T02:28:20.000000Z\tnull\n" +
                "275\t8880550034995457591\t1970-01-07T02:30:00.000000Z\tnull\n" +
                "276\t8464194176491581201\t1970-01-07T02:31:40.000000Z\tnull\n" +
                "277\t6056145309392106540\t1970-01-07T02:33:20.000000Z\tnull\n" +
                "278\t6121305147479698964\t1970-01-07T02:35:00.000000Z\tnull\n" +
                "279\t2282781332678491916\t1970-01-07T02:36:40.000000Z\tnull\n" +
                "280\t3527911398466283309\t1970-01-07T02:38:20.000000Z\tnull\n" +
                "281\t6176277818569291296\t1970-01-07T02:40:00.000000Z\tnull\n" +
                "282\t-8656750634622759804\t1970-01-07T02:41:40.000000Z\tnull\n" +
                "283\t7058145725055366226\t1970-01-07T02:43:20.000000Z\tnull\n" +
                "284\t-8849142892360165671\t1970-01-07T02:45:00.000000Z\tnull\n" +
                "285\t-1134031357796740497\t1970-01-07T02:46:40.000000Z\tnull\n" +
                "286\t-6782883555378798844\t1970-01-07T02:48:20.000000Z\tnull\n" +
                "287\t6405448934035934123\t1970-01-07T02:50:00.000000Z\tnull\n" +
                "288\t-8425483167065397721\t1970-01-07T02:51:40.000000Z\tnull\n" +
                "289\t-8719797095546978745\t1970-01-07T02:53:20.000000Z\tnull\n" +
                "290\t9089874911309539983\t1970-01-07T02:55:00.000000Z\tnull\n" +
                "291\t-7202923278768687325\t1970-01-07T02:56:40.000000Z\tnull\n" +
                "292\t-6571406865336879041\t1970-01-07T02:58:20.000000Z\tnull\n" +
                "293\t-3396992238702724434\t1970-01-07T03:00:00.000000Z\tnull\n" +
                "294\t-8205259083320287108\t1970-01-07T03:01:40.000000Z\tnull\n" +
                "295\t-9029407334801459809\t1970-01-07T03:03:20.000000Z\tnull\n" +
                "296\t-4058426794463997577\t1970-01-07T03:05:00.000000Z\tnull\n" +
                "297\t6517485707736381444\t1970-01-07T03:06:40.000000Z\tnull\n" +
                "298\t579094601177353961\t1970-01-07T03:08:20.000000Z\tnull\n" +
                "299\t750145151786158348\t1970-01-07T03:10:00.000000Z\tnull\n" +
                "300\t5048272224871876586\t1970-01-07T03:11:40.000000Z\tnull\n" +
                "301\t-4547802916868961458\t1970-01-07T03:13:20.000000Z\tnull\n" +
                "302\t-1832315370633201942\t1970-01-07T03:15:00.000000Z\tnull\n" +
                "303\t-8888027247206813045\t1970-01-07T03:16:40.000000Z\tnull\n" +
                "304\t3352215237270276085\t1970-01-07T03:18:20.000000Z\tnull\n" +
                "305\t6937484962759020303\t1970-01-07T03:20:00.000000Z\tnull\n" +
                "306\t7797019568426198829\t1970-01-07T03:21:40.000000Z\tnull\n" +
                "307\t2691623916208307891\t1970-01-07T03:23:20.000000Z\tnull\n" +
                "308\t6184401532241477140\t1970-01-07T03:25:00.000000Z\tnull\n" +
                "309\t-8653777305694768077\t1970-01-07T03:26:40.000000Z\tnull\n" +
                "310\t8756159220596318848\t1970-01-07T03:28:20.000000Z\tnull\n" +
                "311\t4579251508938058953\t1970-01-07T03:30:00.000000Z\tnull\n" +
                "312\t425369166370563563\t1970-01-07T03:31:40.000000Z\tnull\n" +
                "313\t5478379480606573987\t1970-01-07T03:33:20.000000Z\tnull\n" +
                "314\t-4284648096271470489\t1970-01-07T03:35:00.000000Z\tnull\n" +
                "315\t-1741953200710332294\t1970-01-07T03:36:40.000000Z\tnull\n" +
                "316\t-4450383397583441126\t1970-01-07T03:38:20.000000Z\tnull\n" +
                "317\t8984932460293088377\t1970-01-07T03:40:00.000000Z\tnull\n" +
                "318\t9058067501760744164\t1970-01-07T03:41:40.000000Z\tnull\n" +
                "319\t8490886945852172597\t1970-01-07T03:43:20.000000Z\tnull\n" +
                "320\t-8841102831894340636\t1970-01-07T03:45:00.000000Z\tnull\n" +
                "321\t8503557900983561786\t1970-01-07T03:46:40.000000Z\tnull\n" +
                "322\t1508637934261574620\t1970-01-07T03:48:20.000000Z\tnull\n" +
                "323\t663602980874300508\t1970-01-07T03:50:00.000000Z\tnull\n" +
                "324\t788901813531436389\t1970-01-07T03:51:40.000000Z\tnull\n" +
                "325\t6793615437970356479\t1970-01-07T03:53:20.000000Z\tnull\n" +
                "326\t6380499796471875623\t1970-01-07T03:55:00.000000Z\tnull\n" +
                "327\t2006083905706813287\t1970-01-07T03:56:40.000000Z\tnull\n" +
                "328\t5513479607887040119\t1970-01-07T03:58:20.000000Z\tnull\n" +
                "329\t5343275067392229138\t1970-01-07T04:00:00.000000Z\tnull\n" +
                "330\t4527121849171257172\t1970-01-07T04:01:40.000000Z\tnull\n" +
                "331\t4847320715984654162\t1970-01-07T04:03:20.000000Z\tnull\n" +
                "332\t7092246624397344208\t1970-01-07T04:05:00.000000Z\tnull\n" +
                "333\t6445007901796870697\t1970-01-07T04:06:40.000000Z\tnull\n" +
                "334\t1669226447966988582\t1970-01-07T04:08:20.000000Z\tnull\n" +
                "335\t5953039264407551685\t1970-01-07T04:10:00.000000Z\tnull\n" +
                "336\t7592940205308166826\t1970-01-07T04:11:40.000000Z\tnull\n" +
                "337\t-7414829143044491558\t1970-01-07T04:13:20.000000Z\tnull\n" +
                "338\t-6819946977256689384\t1970-01-07T04:15:00.000000Z\tnull\n" +
                "339\t-7186310556474199346\t1970-01-07T04:16:40.000000Z\tnull\n" +
                "340\t-8814330552804983713\t1970-01-07T04:18:20.000000Z\tnull\n" +
                "341\t-8960406850507339854\t1970-01-07T04:20:00.000000Z\tnull\n" +
                "342\t-8793423647053878901\t1970-01-07T04:21:40.000000Z\tnull\n" +
                "343\t5941398229034918748\t1970-01-07T04:23:20.000000Z\tnull\n" +
                "344\t5980197440602572628\t1970-01-07T04:25:00.000000Z\tnull\n" +
                "345\t2106240318003963024\t1970-01-07T04:26:40.000000Z\tnull\n" +
                "346\t9200214878918264613\t1970-01-07T04:28:20.000000Z\tnull\n" +
                "347\t-8211260649542902334\t1970-01-07T04:30:00.000000Z\tnull\n" +
                "348\t5068939738525201696\t1970-01-07T04:31:40.000000Z\tnull\n" +
                "349\t3820631780839257855\t1970-01-07T04:33:20.000000Z\tnull\n" +
                "350\t-9219078548506735248\t1970-01-07T04:35:00.000000Z\tnull\n" +
                "351\t8737100589707440954\t1970-01-07T04:36:40.000000Z\tnull\n" +
                "352\t9044897286885345735\t1970-01-07T04:38:20.000000Z\tnull\n" +
                "353\t-7381322665528955510\t1970-01-07T04:40:00.000000Z\tnull\n" +
                "354\t6174532314769579955\t1970-01-07T04:41:40.000000Z\tnull\n" +
                "355\t-8930904012891908076\t1970-01-07T04:43:20.000000Z\tnull\n" +
                "356\t-6765703075406647091\t1970-01-07T04:45:00.000000Z\tnull\n" +
                "357\t8810110521992874823\t1970-01-07T04:46:40.000000Z\tnull\n" +
                "358\t7570866088271751947\t1970-01-07T04:48:20.000000Z\tnull\n" +
                "359\t-7274175842748412916\t1970-01-07T04:50:00.000000Z\tnull\n" +
                "360\t6753412894015940665\t1970-01-07T04:51:40.000000Z\tnull\n" +
                "361\t2106204205501581842\t1970-01-07T04:53:20.000000Z\tnull\n" +
                "362\t2307279172463257591\t1970-01-07T04:55:00.000000Z\tnull\n" +
                "363\t812677186520066053\t1970-01-07T04:56:40.000000Z\tnull\n" +
                "364\t4621844195437841424\t1970-01-07T04:58:20.000000Z\tnull\n" +
                "365\t-7724577649125721868\t1970-01-07T05:00:00.000000Z\tnull\n" +
                "366\t-7171265782561774995\t1970-01-07T05:01:40.000000Z\tnull\n" +
                "367\t6966461743143051249\t1970-01-07T05:03:20.000000Z\tnull\n" +
                "368\t7629109032541741027\t1970-01-07T05:05:00.000000Z\tnull\n" +
                "369\t-7212878484370155026\t1970-01-07T05:06:40.000000Z\tnull\n" +
                "370\t5963775257114848600\t1970-01-07T05:08:20.000000Z\tnull\n" +
                "371\t3771494396743411509\t1970-01-07T05:10:00.000000Z\tnull\n" +
                "372\t8798087869168938593\t1970-01-07T05:11:40.000000Z\tnull\n" +
                "373\t8984775562394712402\t1970-01-07T05:13:20.000000Z\tnull\n" +
                "374\t3792128300541831563\t1970-01-07T05:15:00.000000Z\tnull\n" +
                "375\t7101009950667960843\t1970-01-07T05:16:40.000000Z\tnull\n" +
                "376\t-6460532424840798061\t1970-01-07T05:18:20.000000Z\tnull\n" +
                "377\t-5044078842288373275\t1970-01-07T05:20:00.000000Z\tnull\n" +
                "378\t-3323322733858034601\t1970-01-07T05:21:40.000000Z\tnull\n" +
                "379\t-7665470829783532891\t1970-01-07T05:23:20.000000Z\tnull\n" +
                "380\t6738282533394287579\t1970-01-07T05:25:00.000000Z\tnull\n" +
                "381\t6146164804821006241\t1970-01-07T05:26:40.000000Z\tnull\n" +
                "382\t-7398902448022205322\t1970-01-07T05:28:20.000000Z\tnull\n" +
                "383\t-2471456524133707236\t1970-01-07T05:30:00.000000Z\tnull\n" +
                "384\t9041413988802359580\t1970-01-07T05:31:40.000000Z\tnull\n" +
                "385\t5922689877598858022\t1970-01-07T05:33:20.000000Z\tnull\n" +
                "386\t5168847330186110459\t1970-01-07T05:35:00.000000Z\tnull\n" +
                "387\t8987698540484981038\t1970-01-07T05:36:40.000000Z\tnull\n" +
                "388\t-7228768303272348606\t1970-01-07T05:38:20.000000Z\tnull\n" +
                "389\t5700115585432451578\t1970-01-07T05:40:00.000000Z\tnull\n" +
                "390\t7879490594801163253\t1970-01-07T05:41:40.000000Z\tnull\n" +
                "391\t-5432682396344996498\t1970-01-07T05:43:20.000000Z\tnull\n" +
                "392\t-3463832009795858033\t1970-01-07T05:45:00.000000Z\tnull\n" +
                "393\t-8555544472620366464\t1970-01-07T05:46:40.000000Z\tnull\n" +
                "394\t5205180235397887203\t1970-01-07T05:48:20.000000Z\tnull\n" +
                "395\t2364286642781155412\t1970-01-07T05:50:00.000000Z\tnull\n" +
                "396\t5494476067484139960\t1970-01-07T05:51:40.000000Z\tnull\n" +
                "397\t7357244054212773895\t1970-01-07T05:53:20.000000Z\tnull\n" +
                "398\t-8506266080452644687\t1970-01-07T05:55:00.000000Z\tnull\n" +
                "399\t-1905597357123382478\t1970-01-07T05:56:40.000000Z\tnull\n" +
                "400\t-5496131157726548905\t1970-01-07T05:58:20.000000Z\tnull\n" +
                "401\t-7474351066761292033\t1970-01-07T06:00:00.000000Z\tnull\n" +
                "402\t-6482694999745905510\t1970-01-07T06:01:40.000000Z\tnull\n" +
                "403\t-8026283444976158481\t1970-01-07T06:03:20.000000Z\tnull\n" +
                "404\t5804262091839668360\t1970-01-07T06:05:00.000000Z\tnull\n" +
                "405\t7297601774924170699\t1970-01-07T06:06:40.000000Z\tnull\n" +
                "406\t-4229502740666959541\t1970-01-07T06:08:20.000000Z\tnull\n" +
                "407\t8842585385650675361\t1970-01-07T06:10:00.000000Z\tnull\n" +
                "408\t7046578844650327247\t1970-01-07T06:11:40.000000Z\tnull\n" +
                "409\t8070302167413932495\t1970-01-07T06:13:20.000000Z\tnull\n" +
                "410\t4480750444572460865\t1970-01-07T06:15:00.000000Z\tnull\n" +
                "411\t6205872689407104125\t1970-01-07T06:16:40.000000Z\tnull\n" +
                "412\t9029088579359707814\t1970-01-07T06:18:20.000000Z\tnull\n" +
                "413\t-8737543979347648559\t1970-01-07T06:20:00.000000Z\tnull\n" +
                "414\t-6522954364450041026\t1970-01-07T06:21:40.000000Z\tnull\n" +
                "415\t-6221841196965409356\t1970-01-07T06:23:20.000000Z\tnull\n" +
                "416\t6484482332827923784\t1970-01-07T06:25:00.000000Z\tnull\n" +
                "417\t7036584259400395476\t1970-01-07T06:26:40.000000Z\tnull\n" +
                "418\t-6795628328806886847\t1970-01-07T06:28:20.000000Z\tnull\n" +
                "419\t7576110962745644701\t1970-01-07T06:30:00.000000Z\tnull\n" +
                "420\t8537223925650740475\t1970-01-07T06:31:40.000000Z\tnull\n" +
                "421\t8737613628813682249\t1970-01-07T06:33:20.000000Z\tnull\n" +
                "422\t4598876523645326656\t1970-01-07T06:35:00.000000Z\tnull\n" +
                "423\t6436453824498875972\t1970-01-07T06:36:40.000000Z\tnull\n" +
                "424\t4634177780953489481\t1970-01-07T06:38:20.000000Z\tnull\n" +
                "425\t6390608559661380246\t1970-01-07T06:40:00.000000Z\tnull\n" +
                "426\t8282637062702131151\t1970-01-07T06:41:40.000000Z\tnull\n" +
                "427\t5360746485515325739\t1970-01-07T06:43:20.000000Z\tnull\n" +
                "428\t-7910490643543561037\t1970-01-07T06:45:00.000000Z\tnull\n" +
                "429\t8321277364671502705\t1970-01-07T06:46:40.000000Z\tnull\n" +
                "430\t3987576220753016999\t1970-01-07T06:48:20.000000Z\tnull\n" +
                "431\t3944678179613436885\t1970-01-07T06:50:00.000000Z\tnull\n" +
                "432\t6153381060986313135\t1970-01-07T06:51:40.000000Z\tnull\n" +
                "433\t8278953979466939153\t1970-01-07T06:53:20.000000Z\tnull\n" +
                "434\t6831200789490300310\t1970-01-07T06:55:00.000000Z\tnull\n" +
                "435\t5175638765020222775\t1970-01-07T06:56:40.000000Z\tnull\n" +
                "436\t7090323083171574792\t1970-01-07T06:58:20.000000Z\tnull\n" +
                "437\t6598154038796950493\t1970-01-07T07:00:00.000000Z\tnull\n" +
                "438\t6418970788912980120\t1970-01-07T07:01:40.000000Z\tnull\n" +
                "439\t-7518902569991053841\t1970-01-07T07:03:20.000000Z\tnull\n" +
                "440\t6083279743811422804\t1970-01-07T07:05:00.000000Z\tnull\n" +
                "441\t7459338290943262088\t1970-01-07T07:06:40.000000Z\tnull\n" +
                "442\t7657422372928739370\t1970-01-07T07:08:20.000000Z\tnull\n" +
                "443\t6235849401126045090\t1970-01-07T07:10:00.000000Z\tnull\n" +
                "444\t8227167469487474861\t1970-01-07T07:11:40.000000Z\tnull\n" +
                "445\t4794469881975683047\t1970-01-07T07:13:20.000000Z\tnull\n" +
                "446\t3861637258207773908\t1970-01-07T07:15:00.000000Z\tnull\n" +
                "447\t8485507312523128674\t1970-01-07T07:16:40.000000Z\tnull\n" +
                "448\t-5106801657083469087\t1970-01-07T07:18:20.000000Z\tnull\n" +
                "449\t-7069883773042994098\t1970-01-07T07:20:00.000000Z\tnull\n" +
                "450\t7415337004567900118\t1970-01-07T07:21:40.000000Z\tnull\n" +
                "451\t9026435187365103026\t1970-01-07T07:23:20.000000Z\tnull\n" +
                "452\t-6517956255651384489\t1970-01-07T07:25:00.000000Z\tnull\n" +
                "453\t-5611837907908424613\t1970-01-07T07:26:40.000000Z\tnull\n" +
                "454\t-4036499202601723677\t1970-01-07T07:28:20.000000Z\tnull\n" +
                "455\t8197069319221391729\t1970-01-07T07:30:00.000000Z\tnull\n" +
                "456\t1732923061962778685\t1970-01-07T07:31:40.000000Z\tnull\n" +
                "457\t1737550138998374432\t1970-01-07T07:33:20.000000Z\tnull\n" +
                "458\t1432925274378784738\t1970-01-07T07:35:00.000000Z\tnull\n" +
                "459\t4698698969091611703\t1970-01-07T07:36:40.000000Z\tnull\n" +
                "460\t3843127285248668146\t1970-01-07T07:38:20.000000Z\tnull\n" +
                "461\t2004830221820243556\t1970-01-07T07:40:00.000000Z\tnull\n" +
                "462\t5341431345186701123\t1970-01-07T07:41:40.000000Z\tnull\n" +
                "463\t-8490120737538725244\t1970-01-07T07:43:20.000000Z\tnull\n" +
                "464\t9158482703525773397\t1970-01-07T07:45:00.000000Z\tnull\n" +
                "465\t7702559600184398496\t1970-01-07T07:46:40.000000Z\tnull\n" +
                "466\t-6167105618770444067\t1970-01-07T07:48:20.000000Z\tnull\n" +
                "467\t-6141734738138509500\t1970-01-07T07:50:00.000000Z\tnull\n" +
                "468\t-7300976680388447983\t1970-01-07T07:51:40.000000Z\tnull\n" +
                "469\t6260580881559018466\t1970-01-07T07:53:20.000000Z\tnull\n" +
                "470\t1658444875429025955\t1970-01-07T07:55:00.000000Z\tnull\n" +
                "471\t7920520795110290468\t1970-01-07T07:56:40.000000Z\tnull\n" +
                "472\t-5701911565963471026\t1970-01-07T07:58:20.000000Z\tnull\n" +
                "473\t-6446120489339099836\t1970-01-07T08:00:00.000000Z\tnull\n" +
                "474\t6527501025487796136\t1970-01-07T08:01:40.000000Z\tnull\n" +
                "475\t1851817982979037709\t1970-01-07T08:03:20.000000Z\tnull\n" +
                "476\t2439907409146962686\t1970-01-07T08:05:00.000000Z\tnull\n" +
                "477\t4160567228070722087\t1970-01-07T08:06:40.000000Z\tnull\n" +
                "478\t3250595453661431788\t1970-01-07T08:08:20.000000Z\tnull\n" +
                "479\t7780743197986640723\t1970-01-07T08:10:00.000000Z\tnull\n" +
                "480\t-3261700233985485037\t1970-01-07T08:11:40.000000Z\tnull\n" +
                "481\t-3578120825657825955\t1970-01-07T08:13:20.000000Z\tnull\n" +
                "482\t7443603913302671026\t1970-01-07T08:15:00.000000Z\tnull\n" +
                "483\t7794592287856397845\t1970-01-07T08:16:40.000000Z\tnull\n" +
                "484\t-5391587298431311641\t1970-01-07T08:18:20.000000Z\tnull\n" +
                "485\t9202397484277640888\t1970-01-07T08:20:00.000000Z\tnull\n" +
                "486\t-6951348785425447115\t1970-01-07T08:21:40.000000Z\tnull\n" +
                "487\t-4645139889518544281\t1970-01-07T08:23:20.000000Z\tnull\n" +
                "488\t-7924422932179070052\t1970-01-07T08:25:00.000000Z\tnull\n" +
                "489\t-6861664727068297324\t1970-01-07T08:26:40.000000Z\tnull\n" +
                "490\t-6251867197325094983\t1970-01-07T08:28:20.000000Z\tnull\n" +
                "491\t8177920927333375630\t1970-01-07T08:30:00.000000Z\tnull\n" +
                "492\t8210594435353205032\t1970-01-07T08:31:40.000000Z\tnull\n" +
                "493\t8417830123562577846\t1970-01-07T08:33:20.000000Z\tnull\n" +
                "494\t6785355388782691241\t1970-01-07T08:35:00.000000Z\tnull\n" +
                "495\t-5892588302528885225\t1970-01-07T08:36:40.000000Z\tnull\n" +
                "496\t-1185822981454562836\t1970-01-07T08:38:20.000000Z\tnull\n" +
                "497\t-5296023984443079410\t1970-01-07T08:40:00.000000Z\tnull\n" +
                "498\t6829382503979752449\t1970-01-07T08:41:40.000000Z\tnull\n" +
                "499\t3669882909701240516\t1970-01-07T08:43:20.000000Z\tnull\n" +
                "500\t8068645982235546347\t1970-01-07T08:45:00.000000Z\tnull\n" +
                "10\t3500000\t1970-01-07T08:45:00.000000Z\t10.2\n", timestampTypeName);

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
                compiler, sqlExecutionContext, "select sum(i) from x", sink, "sum\n" +
                        "527250\n"
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

        final String expected = replaceTimestampSuffix1("a\tb\tc\tts\n" +
                "null\tnull\t10\t2013-02-10T00:05:00.000000Z\n" +
                "null\tnull\t20\t2013-02-10T00:06:00.000000Z\n" +
                "null\tnull\t30\t2013-02-10T00:10:00.000000Z\n" +
                "null\tnull\t40\t2013-02-10T00:11:00.000000Z\n", timestampTypeName);

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
                        replaceTimestampSuffix1("2022-06-08T02:50:00.000000Z\t9\t\t\t2\n" +
                                "2022-06-08T02:50:00.000000Z\t10\t\t\t2\n" +
                                "2022-06-08T03:50:00.000000Z\t11\t\t\t2\n" +
                                "2022-06-08T03:50:00.000000Z\t12\t\t\t2\n" +
                                "2022-06-08T04:50:00.000000Z\t13\t\t\t2\n" +
                                "2022-06-08T04:50:00.000000Z\t14\t\t\t2\n", timestampTypeName)
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
                        replaceTimestampSuffix1("2022-06-08T02:50:00.000000Z\t5\t\t\t3\n" +
                                "2022-06-08T02:50:00.000000Z\t6\t\t\t3\n" +
                                "2022-06-08T03:30:00.000000Z\t15\t\t2\t3\n" +
                                "2022-06-08T03:30:00.000000Z\t16\t\t2\t3\n" +
                                "2022-06-08T04:50:00.000000Z\t18\t\t4\t3\n" +
                                "2022-06-08T05:30:00.000000Z\t17\t\t4\t3\n", timestampTypeName)
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
                        replaceTimestampSuffix1("2022-06-08T02:50:00.000000Z\t5\t\t\t3\n" +
                                "2022-06-08T02:50:00.000000Z\t6\t\t\t3\n" +
                                "2022-06-08T02:50:00.000000Z\t20\t\t4\t3\n" +
                                "2022-06-08T02:50:00.000000Z\t21\t\t4\t3\n" +
                                "2022-06-08T03:30:00.000000Z\t15\t\t2\t3\n" +
                                "2022-06-08T03:30:00.000000Z\t16\t\t2\t3\n" +
                                "2022-06-08T04:50:00.000000Z\t18\t\t4\t3\n" +
                                "2022-06-08T05:30:00.000000Z\t17\t\t4\t3\n" +
                                "2022-06-08T05:30:00.000000Z\t19\t\t4\t3\n", timestampTypeName)
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
                replaceTimestampSuffix1("ts\tmetric\tdiagnostic\tsensorChannel\tloggerChannel\n" +
                        "2022-06-08T02:50:00.000000Z\t5\t\t\t3\n" +
                        "2022-06-08T02:55:00.000000Z\t6\t\t\t3\n" +
                        "2022-06-08T03:30:00.000000Z\t15\t\t2\t3\n" +
                        "2022-06-08T03:30:00.000000Z\t16\t\t2\t3\n", timestampTypeName)
        );

        // OOO insert mid prev partition
        engine.execute(
                "INSERT batch 2 INTO monthly_col_top (ts, metric, 'loggerChannel') VALUES" +
                        "('2022-06-08T02:54:00.000000Z', '17', '3')," +
                        "('2022-06-08T02:56:00.000000Z', '18', '3')", sqlExecutionContext
        );

        TestUtils.assertSql(
                compiler, sqlExecutionContext, "select * from monthly_col_top where loggerChannel = '3'", sink,
                replaceTimestampSuffix1("ts\tmetric\tdiagnostic\tsensorChannel\tloggerChannel\n" +
                        "2022-06-08T02:50:00.000000Z\t5\t\t\t3\n" +
                        "2022-06-08T02:54:00.000000Z\t17\t\t\t3\n" +
                        "2022-06-08T02:55:00.000000Z\t6\t\t\t3\n" +
                        "2022-06-08T02:56:00.000000Z\t18\t\t\t3\n" +
                        "2022-06-08T03:30:00.000000Z\t15\t\t2\t3\n" +
                        "2022-06-08T03:30:00.000000Z\t16\t\t2\t3\n", timestampTypeName)
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
                replaceTimestampSuffix1("ts\tmetric\tdiagnostic\tsensorChannel\tloggerChannel\n" +
                        "2022-06-08T02:50:00.000000Z\t5\t\t\t3\n" +
                        "2022-06-08T02:54:00.000000Z\t17\t\t\t3\n" +
                        "2022-06-08T02:55:00.000000Z\t6\t\t\t3\n" +
                        "2022-06-08T02:56:00.000000Z\t18\t\t\t3\n" +
                        "2022-06-08T03:15:00.000000Z\t19\t\t\t3\n" +
                        "2022-06-08T03:30:00.000000Z\t15\t\t2\t3\n" +
                        "2022-06-08T03:30:00.000000Z\t16\t\t2\t3\n" +
                        "2022-06-08T04:30:00.000000Z\t20\t\t\t3\n" +
                        "2022-06-08T04:31:00.000000Z\t21\t\t\t3\n", timestampTypeName)
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
                replaceTimestampSuffix1("ts\tmetric\tdiagnostic\tsensorChannel\tloggerChannel\n" +
                        "2022-06-08T00:40:00.000000Z\t24\t\t\t3\n" +
                        "2022-06-08T02:50:00.000000Z\t5\t\t\t3\n" +
                        "2022-06-08T02:54:00.000000Z\t17\t\t\t3\n" +
                        "2022-06-08T02:55:00.000000Z\t6\t\t\t3\n" +
                        "2022-06-08T02:56:00.000000Z\t18\t\t\t3\n" +
                        "2022-06-08T03:15:00.000000Z\t19\t\t\t3\n" +
                        "2022-06-08T03:15:00.000000Z\t22\t\t\t3\n" +
                        "2022-06-08T03:15:00.000000Z\t23\t\t\t3\n" +
                        "2022-06-08T03:30:00.000000Z\t15\t\t2\t3\n" +
                        "2022-06-08T03:30:00.000000Z\t16\t\t2\t3\n" +
                        "2022-06-08T04:30:00.000000Z\t20\t\t\t3\n" +
                        "2022-06-08T04:31:00.000000Z\t21\t\t\t3\n", timestampTypeName)
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

        final String expected = replaceTimestampSuffix("a\tts\n" +
                "1909\t2020-03-10T20:36:00.000081Z\n" +
                "1809\t2020-03-10T20:36:00.000082Z\n" +
                "1709\t2020-03-10T20:36:00.000083Z\n" +
                "1609\t2020-03-10T20:36:00.000084Z\n" +
                "1509\t2020-03-10T20:36:00.000085Z\n" +
                "1409\t2020-03-10T20:36:00.000086Z\n" +
                "1309\t2020-03-10T20:36:00.000087Z\n" +
                "1209\t2020-03-10T20:36:00.000088Z\n" +
                "1109\t2020-03-10T20:36:00.000089Z\n" +
                "1009\t2020-03-10T20:36:00.000090Z\n" +
                "909\t2020-03-10T20:36:00.000091Z\n" +
                "809\t2020-03-10T20:36:00.000092Z\n" +
                "709\t2020-03-10T20:36:00.000093Z\n" +
                "609\t2020-03-10T20:36:00.000094Z\n" +
                "509\t2020-03-10T20:36:00.000095Z\n" +
                "409\t2020-03-10T20:36:00.000096Z\n" +
                "309\t2020-03-10T20:36:00.000097Z\n" +
                "209\t2020-03-10T20:36:00.000098Z\n" +
                "109\t2020-03-10T20:36:00.000099Z\n" +
                "9\t2020-03-10T20:36:00.000100Z\n" +
                "1908\t2020-03-10T20:36:00.000181Z\n" +
                "1808\t2020-03-10T20:36:00.000182Z\n" +
                "1708\t2020-03-10T20:36:00.000183Z\n" +
                "1608\t2020-03-10T20:36:00.000184Z\n" +
                "1508\t2020-03-10T20:36:00.000185Z\n" +
                "1408\t2020-03-10T20:36:00.000186Z\n" +
                "1308\t2020-03-10T20:36:00.000187Z\n" +
                "1208\t2020-03-10T20:36:00.000188Z\n" +
                "1108\t2020-03-10T20:36:00.000189Z\n" +
                "1008\t2020-03-10T20:36:00.000190Z\n" +
                "908\t2020-03-10T20:36:00.000191Z\n" +
                "808\t2020-03-10T20:36:00.000192Z\n" +
                "708\t2020-03-10T20:36:00.000193Z\n" +
                "608\t2020-03-10T20:36:00.000194Z\n" +
                "508\t2020-03-10T20:36:00.000195Z\n" +
                "408\t2020-03-10T20:36:00.000196Z\n" +
                "308\t2020-03-10T20:36:00.000197Z\n" +
                "208\t2020-03-10T20:36:00.000198Z\n" +
                "108\t2020-03-10T20:36:00.000199Z\n" +
                "8\t2020-03-10T20:36:00.000200Z\n" +
                "1907\t2020-03-10T20:36:00.000281Z\n" +
                "1807\t2020-03-10T20:36:00.000282Z\n" +
                "1707\t2020-03-10T20:36:00.000283Z\n" +
                "1607\t2020-03-10T20:36:00.000284Z\n" +
                "1507\t2020-03-10T20:36:00.000285Z\n" +
                "1407\t2020-03-10T20:36:00.000286Z\n" +
                "1307\t2020-03-10T20:36:00.000287Z\n" +
                "1207\t2020-03-10T20:36:00.000288Z\n" +
                "1107\t2020-03-10T20:36:00.000289Z\n" +
                "1007\t2020-03-10T20:36:00.000290Z\n" +
                "907\t2020-03-10T20:36:00.000291Z\n" +
                "807\t2020-03-10T20:36:00.000292Z\n" +
                "707\t2020-03-10T20:36:00.000293Z\n" +
                "607\t2020-03-10T20:36:00.000294Z\n" +
                "507\t2020-03-10T20:36:00.000295Z\n" +
                "407\t2020-03-10T20:36:00.000296Z\n" +
                "307\t2020-03-10T20:36:00.000297Z\n" +
                "207\t2020-03-10T20:36:00.000298Z\n" +
                "107\t2020-03-10T20:36:00.000299Z\n" +
                "7\t2020-03-10T20:36:00.000300Z\n" +
                "1906\t2020-03-10T20:36:00.000381Z\n" +
                "1806\t2020-03-10T20:36:00.000382Z\n" +
                "1706\t2020-03-10T20:36:00.000383Z\n" +
                "1606\t2020-03-10T20:36:00.000384Z\n" +
                "1506\t2020-03-10T20:36:00.000385Z\n" +
                "1406\t2020-03-10T20:36:00.000386Z\n" +
                "1306\t2020-03-10T20:36:00.000387Z\n" +
                "1206\t2020-03-10T20:36:00.000388Z\n" +
                "1106\t2020-03-10T20:36:00.000389Z\n" +
                "1006\t2020-03-10T20:36:00.000390Z\n" +
                "906\t2020-03-10T20:36:00.000391Z\n" +
                "806\t2020-03-10T20:36:00.000392Z\n" +
                "706\t2020-03-10T20:36:00.000393Z\n" +
                "606\t2020-03-10T20:36:00.000394Z\n" +
                "506\t2020-03-10T20:36:00.000395Z\n" +
                "406\t2020-03-10T20:36:00.000396Z\n" +
                "306\t2020-03-10T20:36:00.000397Z\n" +
                "206\t2020-03-10T20:36:00.000398Z\n" +
                "106\t2020-03-10T20:36:00.000399Z\n" +
                "6\t2020-03-10T20:36:00.000400Z\n" +
                "1905\t2020-03-10T20:36:00.000481Z\n" +
                "1805\t2020-03-10T20:36:00.000482Z\n" +
                "1705\t2020-03-10T20:36:00.000483Z\n" +
                "1605\t2020-03-10T20:36:00.000484Z\n" +
                "1505\t2020-03-10T20:36:00.000485Z\n" +
                "1405\t2020-03-10T20:36:00.000486Z\n" +
                "1305\t2020-03-10T20:36:00.000487Z\n" +
                "1205\t2020-03-10T20:36:00.000488Z\n" +
                "1105\t2020-03-10T20:36:00.000489Z\n" +
                "1005\t2020-03-10T20:36:00.000490Z\n" +
                "905\t2020-03-10T20:36:00.000491Z\n" +
                "805\t2020-03-10T20:36:00.000492Z\n" +
                "705\t2020-03-10T20:36:00.000493Z\n" +
                "605\t2020-03-10T20:36:00.000494Z\n" +
                "505\t2020-03-10T20:36:00.000495Z\n" +
                "405\t2020-03-10T20:36:00.000496Z\n" +
                "305\t2020-03-10T20:36:00.000497Z\n" +
                "205\t2020-03-10T20:36:00.000498Z\n" +
                "105\t2020-03-10T20:36:00.000499Z\n" +
                "5\t2020-03-10T20:36:00.000500Z\n" +
                "1904\t2020-03-10T20:36:00.000581Z\n" +
                "1804\t2020-03-10T20:36:00.000582Z\n" +
                "1704\t2020-03-10T20:36:00.000583Z\n" +
                "1604\t2020-03-10T20:36:00.000584Z\n" +
                "1504\t2020-03-10T20:36:00.000585Z\n" +
                "1404\t2020-03-10T20:36:00.000586Z\n" +
                "1304\t2020-03-10T20:36:00.000587Z\n" +
                "1204\t2020-03-10T20:36:00.000588Z\n" +
                "1104\t2020-03-10T20:36:00.000589Z\n" +
                "1004\t2020-03-10T20:36:00.000590Z\n" +
                "904\t2020-03-10T20:36:00.000591Z\n" +
                "804\t2020-03-10T20:36:00.000592Z\n" +
                "704\t2020-03-10T20:36:00.000593Z\n" +
                "604\t2020-03-10T20:36:00.000594Z\n" +
                "504\t2020-03-10T20:36:00.000595Z\n" +
                "404\t2020-03-10T20:36:00.000596Z\n" +
                "304\t2020-03-10T20:36:00.000597Z\n" +
                "204\t2020-03-10T20:36:00.000598Z\n" +
                "104\t2020-03-10T20:36:00.000599Z\n" +
                "4\t2020-03-10T20:36:00.000600Z\n" +
                "1903\t2020-03-10T20:36:00.000681Z\n" +
                "1803\t2020-03-10T20:36:00.000682Z\n" +
                "1703\t2020-03-10T20:36:00.000683Z\n" +
                "1603\t2020-03-10T20:36:00.000684Z\n" +
                "1503\t2020-03-10T20:36:00.000685Z\n" +
                "1403\t2020-03-10T20:36:00.000686Z\n" +
                "1303\t2020-03-10T20:36:00.000687Z\n" +
                "1203\t2020-03-10T20:36:00.000688Z\n" +
                "1103\t2020-03-10T20:36:00.000689Z\n" +
                "1003\t2020-03-10T20:36:00.000690Z\n" +
                "903\t2020-03-10T20:36:00.000691Z\n" +
                "803\t2020-03-10T20:36:00.000692Z\n" +
                "703\t2020-03-10T20:36:00.000693Z\n" +
                "603\t2020-03-10T20:36:00.000694Z\n" +
                "503\t2020-03-10T20:36:00.000695Z\n" +
                "403\t2020-03-10T20:36:00.000696Z\n" +
                "303\t2020-03-10T20:36:00.000697Z\n" +
                "203\t2020-03-10T20:36:00.000698Z\n" +
                "103\t2020-03-10T20:36:00.000699Z\n" +
                "3\t2020-03-10T20:36:00.000700Z\n" +
                "1902\t2020-03-10T20:36:00.000781Z\n" +
                "1802\t2020-03-10T20:36:00.000782Z\n" +
                "1702\t2020-03-10T20:36:00.000783Z\n" +
                "1602\t2020-03-10T20:36:00.000784Z\n" +
                "1502\t2020-03-10T20:36:00.000785Z\n" +
                "1402\t2020-03-10T20:36:00.000786Z\n" +
                "1302\t2020-03-10T20:36:00.000787Z\n" +
                "1202\t2020-03-10T20:36:00.000788Z\n" +
                "1102\t2020-03-10T20:36:00.000789Z\n" +
                "1002\t2020-03-10T20:36:00.000790Z\n" +
                "902\t2020-03-10T20:36:00.000791Z\n" +
                "802\t2020-03-10T20:36:00.000792Z\n" +
                "702\t2020-03-10T20:36:00.000793Z\n" +
                "602\t2020-03-10T20:36:00.000794Z\n" +
                "502\t2020-03-10T20:36:00.000795Z\n" +
                "402\t2020-03-10T20:36:00.000796Z\n" +
                "302\t2020-03-10T20:36:00.000797Z\n" +
                "202\t2020-03-10T20:36:00.000798Z\n" +
                "102\t2020-03-10T20:36:00.000799Z\n" +
                "2\t2020-03-10T20:36:00.000800Z\n" +
                "1901\t2020-03-10T20:36:00.000881Z\n" +
                "1801\t2020-03-10T20:36:00.000882Z\n" +
                "1701\t2020-03-10T20:36:00.000883Z\n" +
                "1601\t2020-03-10T20:36:00.000884Z\n" +
                "1501\t2020-03-10T20:36:00.000885Z\n" +
                "1401\t2020-03-10T20:36:00.000886Z\n" +
                "1301\t2020-03-10T20:36:00.000887Z\n" +
                "1201\t2020-03-10T20:36:00.000888Z\n" +
                "1101\t2020-03-10T20:36:00.000889Z\n" +
                "1001\t2020-03-10T20:36:00.000890Z\n" +
                "901\t2020-03-10T20:36:00.000891Z\n" +
                "801\t2020-03-10T20:36:00.000892Z\n" +
                "701\t2020-03-10T20:36:00.000893Z\n" +
                "601\t2020-03-10T20:36:00.000894Z\n" +
                "501\t2020-03-10T20:36:00.000895Z\n" +
                "401\t2020-03-10T20:36:00.000896Z\n" +
                "301\t2020-03-10T20:36:00.000897Z\n" +
                "201\t2020-03-10T20:36:00.000898Z\n" +
                "101\t2020-03-10T20:36:00.000899Z\n" +
                "1\t2020-03-10T20:36:00.000900Z\n" +
                "1900\t2020-03-10T20:36:00.000981Z\n" +
                "1800\t2020-03-10T20:36:00.000982Z\n" +
                "1700\t2020-03-10T20:36:00.000983Z\n" +
                "1600\t2020-03-10T20:36:00.000984Z\n" +
                "1500\t2020-03-10T20:36:00.000985Z\n" +
                "1400\t2020-03-10T20:36:00.000986Z\n" +
                "1300\t2020-03-10T20:36:00.000987Z\n" +
                "1200\t2020-03-10T20:36:00.000988Z\n" +
                "1100\t2020-03-10T20:36:00.000989Z\n" +
                "1000\t2020-03-10T20:36:00.000990Z\n" +
                "900\t2020-03-10T20:36:00.000991Z\n" +
                "800\t2020-03-10T20:36:00.000992Z\n" +
                "700\t2020-03-10T20:36:00.000993Z\n" +
                "600\t2020-03-10T20:36:00.000994Z\n" +
                "500\t2020-03-10T20:36:00.000995Z\n" +
                "400\t2020-03-10T20:36:00.000996Z\n" +
                "300\t2020-03-10T20:36:00.000997Z\n" +
                "200\t2020-03-10T20:36:00.000998Z\n" +
                "100\t2020-03-10T20:36:00.000999Z\n" +
                "0\t2020-03-10T20:36:00.001000Z\n", timestampTypeName);

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
                "count\n" +
                        "11\n"
        );

        // Close and open writer. Partition dir 2020-01-01.1 should not be purged
        engine.releaseAllWriters();
        engine.execute("insert into x values(2, 101.0, '2020-01-01T00:02:00')", sqlExecutionContext);
        TestUtils.assertSql(
                compiler, sqlExecutionContext, "select count() from x", sink,
                "count\n" +
                        "12\n"
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
