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
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.rules.TestName;

import java.net.URISyntaxException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class O3Test extends AbstractO3Test {
    private static final Log LOG = LogFactory.getLog(O3Test.class);

    private final StringBuilder tstData = new StringBuilder();
    @Rule
    public TestName name = new TestName();

    @Before
    public void setUp4() {
        Vect.resetPerformanceCounters();
    }

    @After
    public void tearDown4() throws InterruptedException {
        int count = Vect.getPerformanceCountersCount();
        if (count > 0) {
            tstData.setLength(0);
            tstData.append(name.getMethodName()).append(",");
            long total = 0;
            for (int i = 0; i < count; i++) {
                long val = Vect.getPerformanceCounter(i);
                tstData.append(val).append("\t");
                total += val;
            }
            tstData.append(total);

            Thread.sleep(10);
            System.err.flush();
            System.err.println(tstData);
            System.err.flush();
        }
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
        // On OSX it's not trivial to increase open file limit per process
        if (Os.type != Os.OSX_AMD64 && Os.type != Os.OSX_ARM64) {
            executeVanilla(O3Test::testBench0);
        }
    }

    @Test
    public void testBenchContended() throws Exception {
        // On OSX it's not trivial to increase open file limit per process
        if (Os.type != Os.OSX_AMD64 && Os.type != Os.OSX_ARM64) {
            executeWithPool(0, O3Test::testBench0);
        }
    }

    @Test
    public void testBenchParallel() throws Exception {
        // On OSX it's not trivial to increase open file limit per process
        if (Os.type != Os.OSX_AMD64 && Os.type != Os.OSX_ARM64) {
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
    public void testColumnTopNewPartitionMiddleOfTableContended() throws Exception {
        executeWithPool(0, O3Test::testColumnTopNewPartitionMiddleOfTable0);
    }

    @Test
    public void testColumnTopNewPartitionMiddleOfTableParallel() throws Exception {
        executeWithPool(4, O3Test::testColumnTopNewPartitionMiddleOfTable0);
    }

    @Test
    public void testInsertNullTimestamp() throws Exception {
        executeWithPool(2, (engine, compiler, sqlExecutionContext) -> {
            compiler.compile(
                    "create table x as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " timestamp_sequence(10000000000,1000000000L) ts" +
                            " from long_sequence(100)" +
                            "), index(sym) timestamp (ts) partition by DAY",
                    sqlExecutionContext
            );

            // to_timestamp produces NULL because values does not match the pattern
            try {
                TestUtils.insert(compiler, sqlExecutionContext, "insert into x values(0, 'abc', to_timestamp('2019-08-15T16:03:06.595', 'yyyy-MM-dd:HH:mm:ss.SSSUUU'))");
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "timestamps before 1970-01-01 are not allowed for O3");
            }
        });
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
    public void testLargeCommitLagContended() throws Exception {
        executeWithPool(0, O3Test::testLargeCommitLag0);
    }

    @Test
    public void testO3EdgeBugContended() throws Exception {
        executeWithPool(0, O3Test::testO3EdgeBug);
    }

    @Test
    public void testOOOFollowedByAnotherOOO() throws Exception {
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
    public void testRepeatedIngest() throws Exception {
        executeWithPool(4, O3Test::testRepeatedIngest0);
    }

    @Test
    public void testTwoTablesCompeteForBuffer() throws Exception {
        executeWithPool(4, O3Test::testTwoTablesCompeteForBuffer0);
    }

    @Test
    public void testVanillaCommitLag() throws Exception {
        executeVanilla(O3Test::testVanillaCommitLag0);
    }

    @Test
    public void testVanillaCommitLagContended() throws Exception {
        executeWithPool(0, O3Test::testVanillaCommitLag0);
    }

    @Test
    public void testVanillaCommitLagParallel() throws Exception {
        executeWithPool(4, O3Test::testVanillaCommitLag0);
    }

    @Test
    public void testVanillaCommitLagSinglePartition() throws Exception {
        executeVanilla(O3Test::testVanillaCommitLagSinglePartition0);
    }

    @Test
    public void testVanillaCommitLagSinglePartitionContended() throws Exception {
        executeWithPool(0, O3Test::testVanillaCommitLagSinglePartition0);
    }

    @Test
    public void testVanillaCommitLagSinglePartitionParallel() throws Exception {
        executeWithPool(4, O3Test::testVanillaCommitLagSinglePartition0);
    }

    @Test
    public void testWriterOpensCorrectTxnPartitionOnRestart() throws Exception {
        executeWithPool(0, O3Test::testWriterOpensCorrectTxnPartitionOnRestart0);
    }

    private static void testWriterOpensCorrectTxnPartitionOnRestart0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, NumericException {
        CairoConfiguration configuration = engine.getConfiguration();
        try (TableModel x = new TableModel(configuration, "x", PartitionBy.DAY)) {
            TestUtils.createPopulateTable(
                    compiler,
                    sqlExecutionContext,
                    x.col("id", ColumnType.LONG)
                            .col("ok", ColumnType.DOUBLE)
                            .timestamp("ts"),
                    10,
                    "2020-01-01",
                    1);

            // Insert OOO to create partition dir 2020-01-01.1
            TestUtils.insert(compiler, sqlExecutionContext, "insert into x values(1, 100.0, '2020-01-01T00:01:00')");
            TestUtils.assertSql(compiler, sqlExecutionContext, "select count() from x", sink,
                    "count\n" +
                            "11\n"
            );

            // Close and open writer. Partition dir 2020-01-01.1 should not be purged
            engine.releaseAllWriters();
            TestUtils.insert(compiler, sqlExecutionContext, "insert into x values(2, 101.0, '2020-01-01T00:02:00')");
            TestUtils.assertSql(compiler, sqlExecutionContext, "select count() from x", sink,
                    "count\n" +
                            "12\n"
            );
        }
    }

    private static void testTwoTablesCompeteForBuffer0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext
    ) throws SqlException {
        compiler.compile(
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
                        " timestamp_sequence(500000000000L,100000000L) ts," +
                        " rnd_str(5,16,10) l," +
                        " rnd_str(5,16,10) m," +
                        " rnd_str(5,16,10) n," +
                        " rnd_str(5,16,10) t," +
                        " rnd_str(5,16,10) l256" +
                        " from long_sequence(10000)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        compiler.compile("create table x1 as (x) timestamp(ts) partition by DAY", executionContext);

        compiler.compile(
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
                        " timestamp_sequence(500000080000L,79999631L) ts," +
                        " rnd_str(5,16,10) l," +
                        " rnd_str(5,16,10) m," +
                        " rnd_str(5,16,10) n," +
                        " rnd_str(5,16,10) t," +
                        " rnd_str(5,16,10) l256" +
                        " from long_sequence(10000)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        compiler.compile("create table y1 as (y)", executionContext);

        // create expected result sets
        compiler.compile("create table z as (x union all y)", executionContext);

        // create another compiler to be used by second pool
        try (SqlCompiler compiler2 = new SqlCompiler(engine)) {

            final CyclicBarrier barrier = new CyclicBarrier(2);
            final SOCountDownLatch haltLatch = new SOCountDownLatch(2);
            final AtomicInteger errorCount = new AtomicInteger();

            // we have two pairs of tables (x,y) and (x1,y1)
            WorkerPool pool1 = new WorkerPool(new WorkerPoolAwareConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return new int[]{-1};
                }

                @Override
                public int getWorkerCount() {
                    return 1;
                }

                @Override
                public boolean haltOnError() {
                    return false;
                }

                @Override
                public boolean isEnabled() {
                    return true;
                }
            });

            pool1.assign(new Job() {
                private boolean toRun = true;

                @Override
                public boolean run(int workerId) {
                    if (toRun) {
                        try {
                            toRun = false;
                            barrier.await();
                            compiler.compile("insert into x select * from y", executionContext);
                        } catch (Throwable e) {
                            e.printStackTrace();
                            errorCount.incrementAndGet();
                        } finally {
                            haltLatch.countDown();
                        }
                    }
                    return false;
                }
            });
            pool1.assignCleaner(Path.CLEANER);

            final WorkerPool pool2 = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return new int[]{-1};
                }

                @Override
                public int getWorkerCount() {
                    return 1;
                }

                @Override
                public boolean haltOnError() {
                    return false;
                }
            });

            pool2.assign(new Job() {
                private boolean toRun = true;

                @Override
                public boolean run(int workerId) {
                    if (toRun) {
                        try {
                            toRun = false;
                            barrier.await();
                            compiler2.compile("insert into x1 select * from y1", executionContext);
                        } catch (Throwable e) {
                            e.printStackTrace();
                            errorCount.incrementAndGet();
                        } finally {
                            haltLatch.countDown();
                        }
                    }
                    return false;
                }
            });

            pool2.assignCleaner(Path.CLEANER);

            pool1.start(null);
            pool2.start(null);

            haltLatch.await();

            pool1.halt();
            pool2.halt();

            Assert.assertEquals(0, errorCount.get());
            TestUtils.assertSqlCursors(compiler, executionContext, "z order by ts", "x", LOG);
            TestUtils.assertSqlCursors(compiler, executionContext, "z order by ts", "x1", LOG);
        }
    }

    private static void testPartitionedOOONullSetters0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext)
            throws SqlException, NumericException {
        compiler.compile("create table x (a int, b int, c int, ts timestamp) timestamp(ts) partition by DAY", sqlExecutionContext);
        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "testing")) {
            TableWriter.Row r;

            r = w.newRow(TimestampFormatUtils.parseUTCTimestamp("2013-02-10T00:10:00.000000Z"));
            r.putInt(2, 30);
            r.append();

            r = w.newRow(TimestampFormatUtils.parseUTCTimestamp("2013-02-10T00:05:00.000000Z"));
            r.putInt(2, 10);
            r.append();

            r = w.newRow(TimestampFormatUtils.parseUTCTimestamp("2013-02-10T00:06:00.000000Z"));
            r.putInt(2, 20);
            r.append();

            w.commit();

            r = w.newRow(TimestampFormatUtils.parseUTCTimestamp("2013-02-10T00:11:00.000000Z"));
            r.putInt(2, 40);
            r.append();

            w.commit();
        }

        printSqlResult(compiler, sqlExecutionContext, "x");

        final String expected = "a\tb\tc\tts\n" +
                "NaN\tNaN\t10\t2013-02-10T00:05:00.000000Z\n" +
                "NaN\tNaN\t20\t2013-02-10T00:06:00.000000Z\n" +
                "NaN\tNaN\t30\t2013-02-10T00:10:00.000000Z\n" +
                "NaN\tNaN\t40\t2013-02-10T00:11:00.000000Z\n";

        TestUtils.assertEquals(expected, sink);
    }

    private static void testPartitionedDataAppendOOPrependOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        // create table with roughly 2AM data
        compiler.compile(
                "create table x as (" +
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
                        " cast(null as binary) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(510)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // all records but one is appended to middle partition
        // last record is prepended to the last partition
        compiler.compile(
                "create table append as (" +
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
                        " timestamp_sequence(518390000000L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
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
                "y order by ts",
                "insert into x select * from append",
                "x"
        );

        assertIndexConsistency(
                compiler,
                sqlExecutionContext
        );
    }

    private static void testPartitionedOOPrefixesExistingPartitions0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(500000000000L,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        compiler.compile(
                "create table top as (" +
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
                        " timestamp_sequence(15000000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(1000)" +
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
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testAppendOrderStability(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x (" +
                        "seq long, " +
                        "sym symbol, " +
                        "ts timestamp" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        String[] symbols = {"AA", "BB"};
        long[] seq = new long[symbols.length];

        // insert some records in order
        final Rnd rnd = new Rnd();
        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "testing")) {
            long t = 0;
            for (int i = 0; i < 1000; i++) {
                TableWriter.Row r = w.newRow(t++);
                int index = rnd.nextInt(1);
                r.putLong(0, seq[index]++);
                r.putSym(1, symbols[index]);
                r.append();
            }
            w.commitWithLag();

            // now do out of order
            for (int i = 0; i < 100_000; i++) {
                TableWriter.Row r;

                // symbol 0

                r = w.newRow(t + 1);
                r.putLong(0, seq[0]++);
                r.putSym(1, symbols[0]);
                r.append();

                r = w.newRow(t + 1);
                r.putLong(0, seq[0]++);
                r.putSym(1, symbols[0]);
                r.append();

                // symbol 1

                r = w.newRow(t);
                r.putLong(0, seq[1]++);
                r.putSym(1, symbols[1]);
                r.append();

                r = w.newRow(t);
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

    private static void testPartitionedOOTopAndBottom0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(500000000000L,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        compiler.compile(
                "create table top as (" +
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
                        " timestamp_sequence(150000000000L,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(500)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile(
                "create table bottom as (" +
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
                        " timestamp_sequence(500500000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
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
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testVanillaCommitLag0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        assertLag(
                engine,
                compiler,
                sqlExecutionContext,
                "",
                " from long_sequence(1000000)",
                "insert batch 100000 commitLag 180s into x select * from top"
        );
    }

    private static void testLagOverflowMidCommit0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        assertLag(
                engine,
                compiler,
                sqlExecutionContext,
                "with maxUncommittedRows=400",
                " from long_sequence(1000000)",
                "insert batch 100000 commitLag 180s into x select * from top"
        );
    }

    private static void assertLag(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            String createTableWith,
            String selectFrom,
            String o3InsertSql
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(500000000000L,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(0)" +
                        "), index(sym) timestamp (ts) partition by MONTH " +
                        createTableWith,
                sqlExecutionContext
        );

        compiler.compile(
                "create table top as (" +
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
                        // the formula below creates lag-friendly timestamp distribution
                        " cast(500000000000L + ((x-1)/4) * 1000000L + (4-(x-1)%4)*80009  as timestamp) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
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
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testLagOverflowBySize0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        assertLag(
                engine,
                compiler,
                sqlExecutionContext,
                "with maxUncommittedRows=10000, commitLag=5d",
                " from long_sequence(100000)",
                "insert batch 20000 commitLag 3d into x select * from top"
        );
    }

    private static void testLargeCommitLag0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(500000000000L,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(0)" +
                        "), index(sym) timestamp (ts) partition by MONTH",
                sqlExecutionContext
        );

        compiler.compile("ALTER TABLE x SET PARAM maxUncommittedRows = 2000000", sqlExecutionContext);

        compiler.compile(
                "create table top as (" +
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
                        // the formula below creates lag-friendly timestamp distribution
                        " cast(500000000000L + ((x-1)/4) * 1000L + (4-(x-1)%4)*89  as timestamp) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
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
                "insert batch 2000000 commitLag 180s into x select * from top",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testVanillaCommitLagSinglePartition0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(500000000000L,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(0)" +
                        "), index(sym) timestamp (ts) partition by MONTH",
                sqlExecutionContext
        );

        compiler.compile(
                "create table top as (" +
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
                        // the formula below creates lag-friendly timestamp distribution
                        " cast(500000000000L + ((x-1)/4) * 1000000L + (4-(x-1)%4)*80009  as timestamp) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
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
                "insert batch 100 commitLag 300s into x select * from top"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedOOMergeOO0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x_1 as (" +
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
                        " timestamp_shuffle(0,100000000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(1000)" +
                        ")",
                sqlExecutionContext
        );

        compiler.compile(
                "create table y as (select * from x_1 order by ts) timestamp(ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile(
                "create table x as (select * from x_1), index(sym) timestamp(ts) partition by DAY",
                sqlExecutionContext
        );

        final String sqlTemplate = "select i,sym,amt,timestamp,b,c,d,e,f,g,ik,ts,l,n,t,m from ";

        printSqlResult(compiler, sqlExecutionContext, sqlTemplate + "y");

        String expected = Chars.toString(sink);

        printSqlResult(compiler, sqlExecutionContext, sqlTemplate + "x");
        TestUtils.assertEquals(expected, sink);

        assertIndexConsistencySink(compiler, sqlExecutionContext);
    }

    private static void testPartitionedOOMergeData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(10000000000,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(1500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        compiler.compile(
                "create table 1am as (" +
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
                        " timestamp_sequence(1000000000L,100000000L) ts," +
                        // 1000000000L
                        // 2200712364240
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(100)" +
                        ")",
                sqlExecutionContext
        );

        compiler.compile(
                "create table tail as (" +
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
                        " timestamp_sequence(11500000000L,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        compiler.compile("create table y as (select * from x union all 1am union all tail)", sqlExecutionContext);

        // expected outcome
        printSqlResult(compiler, sqlExecutionContext, "y order by ts");

        String expected = Chars.toString(sink);

        // insert 1AM data into X
        compiler.compile("insert into x select * from 1am", sqlExecutionContext);
        compiler.compile("insert into x select * from tail", sqlExecutionContext);

        testXAndIndex(engine, compiler, sqlExecutionContext, expected);
    }

    private static void testRepeatedIngest0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, NumericException {

        compiler.compile("create table x (a int, ts timestamp) timestamp(ts) partition by DAY", sqlExecutionContext);


        long ts = TimestampFormatUtils.parseUTCTimestamp("2020-03-10T20:36:00.000000Z");
        long expectedMaxTimestamp = Long.MIN_VALUE;
        int step = 100;
        int rowCount = 10;
        try (TableWriter w = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "x", "testing")) {
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

        final String expected = "a\tts\n" +
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
                "0\t2020-03-10T20:36:00.001000Z\n";

        TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "x",
                sink,
                expected
        );
    }

    private static void testXAndIndex(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, String expected) throws SqlException {
        printSqlResult(compiler, sqlExecutionContext, "x");
        TestUtils.assertEquals(expected, sink);
        assertIndexConsistency(compiler, sqlExecutionContext);

        // test that after reader is re-opened we can still see the same data
        engine.releaseAllReaders();
        printSqlResult(compiler, sqlExecutionContext, "x");
        TestUtils.assertEquals(expected, sink);
        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedOODataUpdateMinTimestamp0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(500000000000L,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        compiler.compile(
                "create table 1am as (" +
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
                        " timestamp_sequence(450000000000L,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(500)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile(
                "create table prev as (" +
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
                        " timestamp_sequence(0L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
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
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedOOMerge0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(10000000000,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        compiler.compile(
                "create table 1am as (" +
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
                        " timestamp_sequence(9993000000,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(507)" +
                        ")",
                sqlExecutionContext
        );

        compiler.compile(
                "create table tail as (" +
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
                        " timestamp_sequence(20000000000,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        compiler.compile("create table y as (x union all 1am union all tail)", sqlExecutionContext);

        // expected outcome
        printSqlResult(compiler, sqlExecutionContext, "y order by ts");

        String expected = Chars.toString(sink);

        // insert 1AM data into X
        compiler.compile("insert into x select * from 1am", sqlExecutionContext);
        compiler.compile("insert into x select * from tail", sqlExecutionContext);

        printSqlResult(compiler, sqlExecutionContext, "x");

        TestUtils.assertEquals(expected, sink);

        testXAndIndex(engine, compiler, sqlExecutionContext, expected);

    }

    private static void testOooFollowedByAnotherOOO0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(10000000000,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        compiler.compile(
                "create table 1am as (" +
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
                        " timestamp_sequence(9993000000,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(507)" +
                        ")",
                sqlExecutionContext
        );

        compiler.compile(
                "create table tail as (" +
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
                        " timestamp_sequence(9997000010L,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        compiler.compile("create table y as (x union all 1am union all tail)", sqlExecutionContext);

        // expected outcome
        printSqlResult(compiler, sqlExecutionContext, "y order by ts");

        String expected = Chars.toString(sink);

        // insert 1AM data into X
        compiler.compile("insert into x select * from 1am", sqlExecutionContext);

        compiler.compile("insert into x select * from tail", sqlExecutionContext);

        printSqlResult(compiler, sqlExecutionContext, "x");

        TestUtils.assertEquals(expected, sink);

        testXAndIndex(engine, compiler, sqlExecutionContext, expected);
    }

    private static void testPartitionedOODataOOCollapsed0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        // top edge of data timestamp equals of of
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(500288000000L,10L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        compiler.compile(
                "create table middle as (" +
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
                        " timestamp_sequence(500000000000L,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
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
                "y order by ts",
                "insert into x select * from middle",
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(10000000000,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        compiler.compile(
                "create table 1am as (" +
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
                        " timestamp_sequence(0,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(500)" +
                        ")",
                sqlExecutionContext
        );

        compiler.compile(
                "create table tail as (" +
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
                        " timestamp_sequence(40000000000,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        compiler.compile("create table y as (select * from x union all 1am union all tail)", sqlExecutionContext);

        // expected outcome
        printSqlResult(compiler, sqlExecutionContext, "y order by ts");

        String expected = Chars.toString(sink);
        compiler.compile("insert into x select * from 1am", sqlExecutionContext);
        compiler.compile("insert into x select * from tail", sqlExecutionContext);

        testXAndIndex(
                engine,
                compiler,
                sqlExecutionContext,
                expected
        );
    }

    private static void testPartitionedDataOOIntoLastOverflowIntoNewPartition0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(10000000000,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(1000)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        compiler.compile(
                "create table 1am as (" +
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
                        " timestamp_sequence(3000000000l,100000000L) ts," + // mid partition for "x"
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(1000)" +
                        ")",
                sqlExecutionContext
        );

        compiler.compile(
                "create table tail as (" +
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
                        " timestamp_sequence(40000000000,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        compiler.compile("create table y as (x union all 1am union all tail)", sqlExecutionContext);

        // expected outcome
        printSqlResult(compiler, sqlExecutionContext, "y order by ts");

        compiler.compile("insert into x select * from 1am", sqlExecutionContext);
        compiler.compile("insert into x select * from tail", sqlExecutionContext);

        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "x",
                "/o3/testPartitionedDataOOIntoLastOverflowIntoNewPartition.txt"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);

        // check if the same remains true when we open fresh TableReader instance

        engine.releaseAllReaders();
        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "x",
                "/o3/testPartitionedDataOOIntoLastOverflowIntoNewPartition.txt"
        );
        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedDataOOIntoLastIndexSearchBug0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(10000000000,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(1000)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        compiler.compile(
                "create table 1am as (" +
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
                        " timestamp_sequence(3000000000l,10000000L) ts," + // mid partition for "x"
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(1000)" +
                        ")",
                sqlExecutionContext
        );

        compiler.compile(
                "create table tail as (" +
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
                        " timestamp_sequence(20000000000,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        compiler.compile("create table y as (x union all 1am union all tail)", sqlExecutionContext);

        // expected outcome
        printSqlResult(compiler, sqlExecutionContext, "y order by ts");

        final String expected = Chars.toString(sink);

        // The query above generates expected result, but there is a problem using it
        // This test produces duplicate timestamps. Those are being sorted in different order by OOO implementation
        // and the reference query. So they cannot be directly compared. The parts with duplicate timestamps will
        // look different. If this test ever breaks, uncomment the reference query and compare results visually.

        // insert 1AM data into X
        compiler.compile("insert into x select * from 1am", sqlExecutionContext);
        compiler.compile("insert into x select * from tail", sqlExecutionContext);

        // It is necessary to release cached "x" reader because as of yet
        // reader cannot reload any partition other than "current".
        // Cached reader will assume smaller partition size for "1970-01-01", which out-of-order insert updated
        testXAndIndex(
                engine,
                compiler,
                sqlExecutionContext,
                expected
        );
    }

    private static void testO3EdgeBug(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(10000000000,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(1000)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        compiler.compile(
                "create table 1am as (" +
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
                        " timestamp_sequence(3000000000l,10000000L) ts," + // mid partition for "x"
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(801)" +
                        ")",
                sqlExecutionContext
        );

        compiler.compile(
                "create table tail as (" +
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
                        " timestamp_sequence(20000000000,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        compiler.compile("create table y as (x union all 1am union all tail)", sqlExecutionContext);

        // expected outcome
        printSqlResult(compiler, sqlExecutionContext, "y order by ts");

        final String expected = Chars.toString(sink);

        // The query above generates expected result, but there is a problem using it
        // This test produces duplicate timestamps. Those are being sorted in different order by OOO implementation
        // and the reference query. So they cannot be directly compared. The parts with duplicate timestamps will
        // look different. If this test ever breaks, uncomment the reference query and compare results visually.

        // insert 1AM data into X
        compiler.compile("insert into x select * from 1am", sqlExecutionContext);
        compiler.compile("insert into x select * from tail", sqlExecutionContext);

        // It is necessary to release cached "x" reader because as of yet
        // reader cannot reload any partition other than "current".
        // Cached reader will assume smaller partition size for "1970-01-01", which out-of-order insert updated
        testXAndIndex(
                engine,
                compiler,
                sqlExecutionContext,
                expected
        );
    }

    private static void testPartitionedDataMergeData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(500000000000L,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " cast(now() as long256) l256" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile(
                "create table middle as (" +
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
                        " timestamp_sequence(500288000000L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " cast(now() as long256) l256" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all middle)",
                "y order by ts, i desc",
                "insert into x select * from middle",
                "x"
        );

        // check that reader can process out of order partition layout after fresh open
        String filteredColumnSelect = "select i,sym,amt,timestamp,b,c,d,e,f,g,ik,j,ts,l,m,n,t from x";
        engine.releaseAllReaders();
        AbstractO3Test.assertSqlResultAgainstFile(compiler,
                sqlExecutionContext,
                filteredColumnSelect,
                "/o3/testPartitionedDataMergeData.txt");

        AbstractO3Test.assertSqlResultAgainstFile(compiler, sqlExecutionContext,
                filteredColumnSelect + " where sym = 'googl'",
                "/o3/testPartitionedDataMergeData_Index.txt");
    }

    private static void testPartitionedDataMergeEnd0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(500000000000L,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(295)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile(
                "create table middle as (" +
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
                        " timestamp_sequence(500288000000L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(61)" + // <--- these counts are important to align the data
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all middle)",
                "insert into x select * from middle",
                "/o3/testPartitionedDataMergeEnd.txt"
        );

        assertIndexResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "/o3/testPartitionedDataMergeEnd_Index.txt"
        );
    }

    private static void testPartitionedDataOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(500000000000L,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        compiler.compile(
                "create table middle as (" +
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
                        " timestamp_sequence(500288000000L,10L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(500)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all middle)",
                "insert into x select * from middle",
                "/o3/testPartitionedDataOOData.txt"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedDataAppendOODataNotNullStrTail0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
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
                        " cast(null as binary) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(510)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile(
                "create table append as (" +
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
                        " timestamp_sequence(518300000010L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
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
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedDataAppendOODataIndexed0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
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
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile(
                "create table append as (" +
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
                        " timestamp_sequence(518300000010L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
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
                "y where sym = 'googl' order by ts",
                "insert into x select * from append",
                "x where sym = 'googl'"
        );
    }

    private static void testColumnTopLastDataOOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {

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
        compiler.compile(
                "create table x as (" +
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
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
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
                        " timestamp_sequence(549920000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
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
                        " rnd_long() v9" +
                        " from long_sequence(500)",
                sqlExecutionContext
        );

        compiler.compile(
                "create table append as (" +
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
                        " timestamp_sequence(549920000000L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
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
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("insert into x select * from append", sqlExecutionContext);

        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "x",
                "/o3/testColumnTopLastDataOOOData.txt"
        );
    }

    private static void testColumnTopLastDataMergeData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {

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
        compiler.compile(
                "create table x as (" +
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
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
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
                        " timestamp_sequence(549920000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
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
                        " rnd_long() v9" +
                        " from long_sequence(500)",
                sqlExecutionContext
        );

        compiler.compile(
                "create table append as (" +
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
                        " timestamp_sequence(549900000000L,50000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
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
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("insert into x select * from append", sqlExecutionContext);

        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "x",
                "/o3/testColumnTopLastDataMergeData.txt"
        );
    }

    private static void testColumnTopMidDataMergeData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {

        compiler.compile(
                "create table x as (" +
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
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
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
                        " timestamp_sequence(549920000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
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
                        " rnd_long() v9" +
                        " from long_sequence(1000)",
                sqlExecutionContext
        );

        compiler.compile(
                "create table append as (" +
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
                        " timestamp_sequence(549900000000L,50000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
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
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("insert into x select * from append", sqlExecutionContext);

        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "x",
                "/o3/testColumnTopMidDataMergeData.txt"
        );
    }

    private static void testColumnTopLastDataMerge2Data0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {

        // merge in the middle, however data prefix is such
        // that we can deal with by reducing column top rather than copying columns

        compiler.compile(
                "create table x as (" +
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
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
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
                        " timestamp_sequence(549920000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
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
                        " rnd_long() v9" +
                        " from long_sequence(500)",
                sqlExecutionContext
        );

        compiler.compile(
                "create table append as (" +
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
                        " timestamp_sequence(549920000000L,50000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
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
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("insert into x select * from append", sqlExecutionContext);

        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "x",
                "/o3/testColumnTopLastDataMerge2Data.txt"
        );

        // 599820000000
        compiler.compile(
                "create table append2 as (" +
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
                        " timestamp_sequence(599820000001L,50000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
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
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // straight append
        compiler.compile("insert into x select * from append2", sqlExecutionContext);

        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "x",
                "/o3/testColumnTopLastDataMerge2DataStep2.txt"
        );
    }

    private static void testColumnTopLastOOOPrefix0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(500000000000L,330000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " CAST(now() as LONG256) l256" + // Semi-random to not change saved txt file result
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v13 long256", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
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
                        " timestamp_sequence(664670000000L,10000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " CAST(now() as LONG256) l256," +
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
                        " CAST(now() as LONG256) v13," +
                        " rnd_long() v9" +
                        " from long_sequence(500)",
                sqlExecutionContext
        );

        compiler.compile(
                "create table append as (" +
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
                        " timestamp_sequence(604800000000L,10000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " CAST(now() as LONG256) l256," +
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
                        " CAST(now() as LONG256) v13," +
                        " rnd_long() v9" +
                        " from long_sequence(400)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("insert into x select * from append", sqlExecutionContext);

        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "select i,sym,amt,timestamp,b,c,d,e,f,g,ik,j,ts,l,m,n,t,v,v1,v2,v3,v4,v5,v6,v7,v8,v10,v11,v12,v9 from x",
                "/o3/testColumnTopLastOOOPrefix.txt"
        );
    }

    private static void testColumnTopLastOOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
        compiler.compile(
                "create table x as (" +
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
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
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
                        " timestamp_sequence(549920000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
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
                        " rnd_long() v9" +
                        " from long_sequence(500)",
                sqlExecutionContext
        );

        compiler.compile(
                "create table append as (" +
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
                        " timestamp_sequence(546600000000L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
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
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("insert into x select * from append", sqlExecutionContext);

        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "x",
                "/o3/testColumnTopLastOOOData.txt"
        );
    }

    private static void testColumnTopMidOOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
        compiler.compile(
                "create table x as (" +
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
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
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
                        " timestamp_sequence(549920000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
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
                        " rnd_long() v9" +
                        " from long_sequence(1000)",
                sqlExecutionContext
        );

        compiler.compile(
                "create table append as (" +
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
                        " timestamp_sequence(546600000000L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
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
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("insert into x select * from append", sqlExecutionContext);

        assertSqlResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "x",
                "/o3/testColumnTopMidOOOData.txt"
        );
    }

    private static void testPartitionedDataAppendOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext
    ) throws SqlException {
        // create table with roughly 2AM data
        compiler.compile(
                "create table x as (" +
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
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                executionContext
        );

        compiler.compile(
                "create table append as (" +
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
                        " timestamp_sequence(518300000010L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
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
                "x"
        );

        assertIndexConsistency(compiler, executionContext);

        // x ends with timestamp 549900000000
        // straight append
        compiler.compile(
                "create table append2 as (" +
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
                        " timestamp_sequence(549900000001L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                executionContext
        );

        // create third table, which will contain both X and 1AM
        assertO3DataCursors(
                engine,
                compiler,
                executionContext,
                "create table y2 as (y union all append2)",
                "y2 order by ts",
                "insert into x select * from append2",
                "x"
        );
    }

    private static void testColumnTopMidAppendBlankColumn0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext
    ) throws SqlException {
        // create table with roughly 2AM data
        compiler.compile(
                "create table x as (" +
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
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                executionContext
        );

        compiler.compile("alter table x add column v double", executionContext);
        compiler.compile("alter table x add column v1 float", executionContext);
        compiler.compile("alter table x add column v2 int", executionContext);
        compiler.compile("alter table x add column v3 byte", executionContext);
        compiler.compile("alter table x add column v4 short", executionContext);
        compiler.compile("alter table x add column v5 boolean", executionContext);
        compiler.compile("alter table x add column v6 date", executionContext);
        compiler.compile("alter table x add column v7 timestamp", executionContext);
        compiler.compile("alter table x add column v8 symbol", executionContext);
        compiler.compile("alter table x add column v10 char", executionContext);
        compiler.compile("alter table x add column v11 string", executionContext);
        compiler.compile("alter table x add column v12 binary", executionContext);
        compiler.compile("alter table x add column v13 long256", executionContext);
        compiler.compile("alter table x add column v9 long", executionContext);

        compiler.compile(
                "create table append as (" +
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
                        " timestamp_sequence(518300000010L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
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
                        " rnd_long() v9" +
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
                "x"
        );

        assertIndexConsistency(compiler, executionContext);
    }

    private static void testColumnTopLastAppendBlankColumn0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext
    ) throws SqlException {
        // create table with roughly 2AM data
        compiler.compile(
                "create table x as (" +
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
                        "), index(sym) timestamp (ts) partition by DAY",
                executionContext
        );

        compiler.compile("alter table x add column v double", executionContext);
        compiler.compile("alter table x add column v1 float", executionContext);
        compiler.compile("alter table x add column v2 int", executionContext);
        compiler.compile("alter table x add column v3 byte", executionContext);
        compiler.compile("alter table x add column v4 short", executionContext);
        compiler.compile("alter table x add column v5 boolean", executionContext);
        compiler.compile("alter table x add column v6 date", executionContext);
        compiler.compile("alter table x add column v7 timestamp", executionContext);
        compiler.compile("alter table x add column v8 symbol", executionContext);
        compiler.compile("alter table x add column v10 char", executionContext);
        compiler.compile("alter table x add column v11 string", executionContext);
        compiler.compile("alter table x add column v12 binary", executionContext);
        compiler.compile("alter table x add column v9 long", executionContext);

        compiler.compile(
                "create table append as (" +
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
                        " timestamp_sequence(549900000000L + 1,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
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
                        " rnd_long() v9" +
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
                "x"
        );

        assertIndexConsistency(compiler, executionContext);
    }

    private static void testColumnTopMidMergeBlankColumn0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext executionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
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
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(500)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                executionContext
        );

        compiler.compile("alter table x add column v double", executionContext);
        compiler.compile("alter table x add column v1 float", executionContext);
        compiler.compile("alter table x add column v2 int", executionContext);
        compiler.compile("alter table x add column v3 byte", executionContext);
        compiler.compile("alter table x add column v4 short", executionContext);
        compiler.compile("alter table x add column v5 boolean", executionContext);
        compiler.compile("alter table x add column v6 date", executionContext);
        compiler.compile("alter table x add column v7 timestamp", executionContext);
        compiler.compile("alter table x add column v8 symbol index", executionContext);
        compiler.compile("alter table x add column v10 char", executionContext);
        compiler.compile("alter table x add column v11 string", executionContext);
        compiler.compile("alter table x add column v12 binary", executionContext);
        compiler.compile("alter table x add column v13 long256", executionContext);
        compiler.compile("alter table x add column v9 long", executionContext);

        compiler.compile(
                "create table append as (" +
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
                        " timestamp_sequence(518300000000L-1000L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256," +
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
                        " rnd_long() v9" +
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
                "x"
        );

        assertIndexConsistency(compiler, executionContext);
    }

    private static void testColumnTopNewPartitionMiddleOfTable0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        // 1970-01-06
        compiler.compile(
                "create table x as (" +
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
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // 1970-01-09
        compiler.compile(
                "insert into x " +
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
                        " timestamp_sequence(700000000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(500)",
                sqlExecutionContext
        );

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol index", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        // 1970-01-08
        compiler.compile(
                "create table append as (" +
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
                        " timestamp_sequence(610000000000L,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
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
                        " rnd_long() v9" +
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
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testColumnTopMidAppendColumn0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
        compiler.compile(
                "create table x as (" +
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
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
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
                        " timestamp_sequence(549900000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
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
                        " rnd_long() v9" +
                        " from long_sequence(1000)" +
                        "",
                sqlExecutionContext
        );

        compiler.compile(
                "create table append as (" +
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
                        " timestamp_sequence(604700000001,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
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
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "insert into x select * from append",
                "/o3/testColumnTopMidAppendColumn.txt"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testOOOTouchesNotLastPartition0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        long day = Timestamps.DAY_MICROS;
        long hour = Timestamps.HOUR_MICROS;
        long min = hour / 60;
        long sec = min / 60;
        long minsPerDay = day / min;

        compiler.compile(
                "create table x as ( " +
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
                        " timestamp_sequence(0L, " + min + "L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(" + 2 * minsPerDay + ")" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile(
                "create table append as (" +
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
                        " timestamp_sequence(" + (day - min) + "L, " + sec + "L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
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
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testOOOTouchesNotLastPartitionTop0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        long day = Timestamps.DAY_MICROS;
        long hour = Timestamps.HOUR_MICROS;
        long min = hour / 60;
        long minsPerDay = day / min;

        // crate records from Jan 1 01:00:00 to Jan 2 01:00:00 with 1 min interval
        compiler.compile(
                "create table x as ( " +
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
                        " timestamp_sequence(" + hour + "L, " + min + "L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(" + minsPerDay + ")" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // crate 60 records from Jan 1 00:01:00 to Jan 1 01:00:00 with 1 min interval
        // so that Jan 1 01:00:00 present in both record sets
        compiler.compile(
                "create table append as (" +
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
                        " timestamp_sequence(" + min + "L, " + min + "L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
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
                "x"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testColumnTopLastAppendColumn0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
        compiler.compile(
                "create table x as (" +
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
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        compiler.compile("alter table x add column v double", sqlExecutionContext);
        compiler.compile("alter table x add column v1 float", sqlExecutionContext);
        compiler.compile("alter table x add column v2 int", sqlExecutionContext);
        compiler.compile("alter table x add column v3 byte", sqlExecutionContext);
        compiler.compile("alter table x add column v4 short", sqlExecutionContext);
        compiler.compile("alter table x add column v5 boolean", sqlExecutionContext);
        compiler.compile("alter table x add column v6 date", sqlExecutionContext);
        compiler.compile("alter table x add column v7 timestamp", sqlExecutionContext);
        compiler.compile("alter table x add column v8 symbol", sqlExecutionContext);
        compiler.compile("alter table x add column v10 char", sqlExecutionContext);
        compiler.compile("alter table x add column v11 string", sqlExecutionContext);
        compiler.compile("alter table x add column v12 binary", sqlExecutionContext);
        compiler.compile("alter table x add column v9 long", sqlExecutionContext);

        compiler.compile(
                "insert into x " +
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
                        " timestamp_sequence(549900000000L,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
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
                        " rnd_long() v9" +
                        " from long_sequence(10)" +
                        "",
                sqlExecutionContext
        );

        compiler.compile(
                "create table append as (" +
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
                        " timestamp_sequence(550800000000L + 1,100000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
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
                        " rnd_long() v9" +
                        " from long_sequence(100)" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (x union all append)",
                "insert into x select * from append",
                "/o3/testColumnTopLastAppendColumn.txt"
        );

        assertIndexConsistency(compiler, sqlExecutionContext);
    }

    private static void testPartitionedDataOODataPbOOData0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException, URISyntaxException {
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(10000000000,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(1000)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        compiler.compile(
                "create table 1am as (" +
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
                        " timestamp_sequence(72700000000L,1000000L) ts," + // mid partition for "x"
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(100)" +
                        ")",
                sqlExecutionContext
        );

        compiler.compile(
                "create table top2 as (" +
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
                        " cast(86400000000L as timestamp) ts," + // these row should go on top of second partition
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t" +
                        " from long_sequence(100)" +
                        ")",
                sqlExecutionContext
        );

        // create third table, which will contain both X and 1AM
        assertO3DataConsistency(
                engine,
                compiler,
                sqlExecutionContext,
                "create table y as (select * from x union all select * from 1am union all select * from top2)",
                "insert into x select * from (1am union all top2)",
                "/o3/testPartitionedDataOODataPbOOData.txt"
        );

        assertIndexResultAgainstFile(
                compiler,
                sqlExecutionContext,
                "/o3/testPartitionedDataOODataPbOOData_Index.txt"
        );
    }

    private static void testPartitionedDataOODataPbOODataDropColumn0(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(10000000000,100000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(1000)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        compiler.compile(
                "create table 1am as (" +
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
                        " timestamp_sequence(72700000000L-4,1000000L) ts," + // mid partition for "x"
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(100)" +
                        ")",
                sqlExecutionContext
        );

        compiler.compile(
                "create table append2 as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(109890000000L-1,1000000L) ts," + // mid partition for "x"
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
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
                sqlExecutionContext
        );

        compiler.compile("alter table x drop column c", sqlExecutionContext);

        assertO3DataCursors(
                engine,
                compiler,
                sqlExecutionContext,
                "create table z as (select * from x union all append2)",
                "z order by ts",
                "insert into x select * from append2",
                "x"
        );

        assertIndexConsistency(
                compiler,
                sqlExecutionContext,
                "z"
        );
    }

    private static void testAppendToLastPartition(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        compiler.compile(
                "create table x (" +
                        " a int," +
                        " b double," +
                        " ts timestamp" +
                        ") timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        final Rnd rnd = new Rnd();
        try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "testing")) {
            long ts = 1000000 * 1000L;
            long step = 1000000;
            TxnScoreboard txnScoreboard = w.getTxnScoreboard();
            for (int i = 0; i < 1000; i++) {
                TableWriter.Row r;
                r = w.newRow(ts - step);
                r.putInt(0, rnd.nextInt());
                r.putDouble(1, rnd.nextDouble());
                r.append();

                r = w.newRow(ts);
                r.putInt(0, rnd.nextInt());
                r.putDouble(1, rnd.nextDouble());
                r.append();

                r = w.newRow(ts + step);
                r.putInt(0, rnd.nextInt());
                r.putDouble(1, rnd.nextDouble());
                r.append();

                ts += step;

                long txn = w.getTxn();
                txnScoreboard.acquireTxn(txn);
                w.commit();
                txnScoreboard.releaseTxn(txn);
            }
        }
    }

    private static void testBench0(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext sqlExecutionContext) throws SqlException {
        // create table with roughly 2AM data
        compiler.compile(
                "create table x as (" +
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
                        " timestamp_sequence(10000000000,1000000L) ts," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
                        " from long_sequence(1000000)" +
                        "), index(sym) timestamp (ts) partition by DAY",
                sqlExecutionContext
        );

        // create table with 1AM data

        compiler.compile(
                "create table append as (" +
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
                        " timestamp_sequence(3000000000l,100000000L) ts," + // mid partition for "x"
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_char() t," +
                        " rnd_long256() l256" +
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
                "x"
        );
    }
}
