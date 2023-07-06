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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.ServerMain;
import io.questdb.cairo.*;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cutlass.line.LineTcpSender;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.network.Net;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.cutlass.line.AbstractLinePartitionReadOnlyTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.*;

public class LineTcpPartitionReadOnlyTest extends AbstractLinePartitionReadOnlyTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration(
                        "cairo.max.uncommitted.rows=500",
                        "cairo.commit.lag=2000",
                        "cairo.o3.max.lag=2000"
                )
        );
    }

    @Test
    public void testActivePartitionReadOnlyAndNoO3() throws Exception {
        String tableName = testName.getMethodName();
        SOCountDownLatch sendComplete = new SOCountDownLatch(1);
        assertServerMainWithLineTCP(
                tableName,
                () -> {
                    long[] timestampNano = {lastPartitionTs};
                    try (LineTcpSender sender = LineTcpSender.newSender(Net.parseIPv4("127.0.0.1"), ILP_PORT, ILP_BUFFER_SIZE)) {
                        for (int tick = 0; tick < 5000; tick++) {
                            int tickerId = 0;
                            timestampNano[tickerId] += lineTsStep;
                            sender.metric(tableName)
                                    .tag("s", "lobster")
                                    .field("l", 88)
                                    .field("i", 2124)
                                    .at(timestampNano[tickerId]);
                        }
                        sender.flush();
                    } finally {
                        sendComplete.countDown();
                    }
                },
                sendComplete,
                TABLE_START_CONTENT,  // <-- read only, remains intact
                false, false, true, true
        );
    }

    @Test
    public void testActivePartitionReadOnlyAndO3OverActivePartition() throws Exception {
        final String tableName = testName.getMethodName();
        SOCountDownLatch sendComplete = new SOCountDownLatch(1);
        assertServerMainWithLineTCP(
                tableName,
                () -> {
                    long[] timestampNano = {firstPartitionTs, lastPartitionTs, futurePartitionTs};
                    try (LineTcpSender sender = LineTcpSender.newSender(Net.parseIPv4("127.0.0.1"), ILP_PORT, ILP_BUFFER_SIZE)) {
                        for (int tick = 0; tick < 5000; tick++) {
                            int tickerId = tick % timestampNano.length;
                            timestampNano[tickerId] += lineTsStep;
                            sender.metric(tableName)
                                    .tag("s", "lobster")
                                    .field("l", 88)
                                    .field("i", 2124)
                                    .at(timestampNano[tickerId]);
                        }
                        sender.flush();
                    } finally {
                        sendComplete.countDown();
                    }
                },
                sendComplete,
                "min\tmax\tcount\n" +
                        "2022-12-08T00:00:00.001000Z\t2022-12-08T23:56:06.447339Z\t1944\n" + // +1667
                        "2022-12-09T00:01:17.517546Z\t2022-12-09T23:57:23.964885Z\t278\n" +
                        "2022-12-10T00:02:35.035092Z\t2022-12-10T23:58:41.482431Z\t278\n" +
                        "2022-12-11T00:03:52.552638Z\t2022-12-11T23:59:58.999977Z\t278\n" + // ignored 1667
                        "2022-12-12T00:00:00.001000Z\t2022-12-12T00:00:01.666000Z\t1666\n",    // new
                false, false, false, true, false
        );
    }

    @Test
    public void testActivePartitionReadOnlyAndO3UnderActivePartition() throws Exception {
        final String tableName = testName.getMethodName();
        SOCountDownLatch sendComplete = new SOCountDownLatch(1);
        assertServerMainWithLineTCP(
                tableName,
                () -> {
                    long[] timestampNano = {firstPartitionTs, secondPartitionTs, lastPartitionTs};
                    try (LineTcpSender sender = LineTcpSender.newSender(Net.parseIPv4("127.0.0.1"), ILP_PORT, ILP_BUFFER_SIZE)) {
                        for (int tick = 0; tick < 4; tick++) {
                            int tickerId = tick % timestampNano.length;
                            timestampNano[tickerId] += lineTsStep;
                            sender.metric(tableName)
                                    .tag("s", "lobster")
                                    .field("l", 88)
                                    .field("i", 2124)
                                    .at(timestampNano[tickerId]);
                        }
                        sender.flush();
                    } finally {
                        sendComplete.countDown();
                    }
                },
                sendComplete,
                "min\tmax\tcount\n" +
                        "2022-12-08T00:05:11.070207Z\t2022-12-08T23:56:06.447339Z\t277\n" +
                        "2022-12-09T00:00:00.001000Z\t2022-12-09T23:57:23.964885Z\t279\n" + // +1
                        "2022-12-10T00:02:35.035092Z\t2022-12-10T23:58:41.482431Z\t278\n" +
                        "2022-12-11T00:03:52.552638Z\t2022-12-11T23:59:58.999977Z\t278\n",
                true, false, true, true
        );
    }

    @Test
    public void testTableIsReadOnly() throws Exception {
        final String tableName = testName.getMethodName();
        SOCountDownLatch sendComplete = new SOCountDownLatch(1);
        assertServerMainWithLineTCP(
                tableName,
                () -> {
                    long[] timestampNano = {secondPartitionTs, lastPartitionTs};
                    try (LineTcpSender sender = LineTcpSender.newSender(Net.parseIPv4("127.0.0.1"), ILP_PORT, ILP_BUFFER_SIZE)) {
                        for (int tick = 0; tick < 4; tick++) {
                            int tickerId = tick % timestampNano.length;
                            timestampNano[tickerId] += lineTsStep;
                            sender.metric(tableName)
                                    .tag("s", "lobster")
                                    .field("l", 88)
                                    .field("i", 2124)
                                    .at(timestampNano[tickerId]);
                        }
                        sender.flush();
                    } finally {
                        sendComplete.countDown();
                    }
                },
                sendComplete,
                TABLE_START_CONTENT,
                true, true, true, true
        );
    }

    private static void assertServerMainWithLineTCP(
            String tableName,
            Runnable test,
            SOCountDownLatch sendComplete,
            String finallyExpected,
            boolean... partitionIsReadOnly
    ) throws Exception {
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain(getServerMainArgs());
                    SqlCompiler compiler = new SqlCompiler(qdb.getEngine());
                    SqlExecutionContext context = TestUtils.createSqlExecutionCtx(qdb.getEngine())
            ) {
                qdb.start();
                CairoEngine engine = qdb.getEngine();

                // create a table with 4 partitions and 1111 rows
                CairoConfiguration cairoConfig = qdb.getConfiguration().getCairoConfiguration();

                try (
                        TableModel tableModel = new TableModel(cairoConfig, tableName, PartitionBy.DAY)
                                .col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).symbolCapacity(32)
                                .timestamp("ts")
                ) {
                    compiler.compile("create table " + tableName + " (l long, i int, s symbol, ts timestamp) timestamp(ts) partition by day bypass wal", context);
                    compiler.compile(insertFromSelectPopulateTableStmt(tableModel, 1111, firstPartitionName, 4), context);
                }

                // set partition read-only state
                TableToken tableToken = engine.getTableTokenIfExists(tableName);
                try (TableWriter writer = getWriter(engine, tableToken)) {
                    TxWriter txWriter = writer.getTxWriter();
                    int partitionCount = txWriter.getPartitionCount();
                    Assert.assertTrue(partitionCount <= partitionIsReadOnly.length);
                    for (int i = 0; i < partitionCount; i++) {
                        txWriter.setPartitionReadOnly(i, partitionIsReadOnly[i]);
                    }
                    txWriter.bumpTruncateVersion();
                    txWriter.commit(writer.getDenseSymbolMapWriters()); // default commit mode
                }

                // check read only state
                checkPartitionReadOnlyState(engine, tableToken, partitionIsReadOnly);

                assertSql(
                        compiler,
                        context,
                        "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO CALENDAR",
                        Misc.getThreadLocalBuilder(),
                        TABLE_START_CONTENT);

                // so that we know when the table writer is returned to the pool
                final SOCountDownLatch tableWriterReturnedToPool = new SOCountDownLatch(1);
                engine.setPoolListener((factoryType, thread, token, event, segment, position) -> {
                    if (token != null && Chars.equalsNc(tableName, token.getTableName()) && PoolListener.isWalOrWriter(factoryType) && event == PoolListener.EV_RETURN) {
                        tableWriterReturnedToPool.countDown();
                    }
                });

                // run the test
                test.run();
                sendComplete.await();

                // wait for the table writer to be returned to the pool
                tableWriterReturnedToPool.await();

                // check read only state, no changes
                checkPartitionReadOnlyState(engine, tableToken, partitionIsReadOnly);

                // check expected results
                assertSql(
                        compiler,
                        context,
                        "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO CALENDAR",
                        Misc.getThreadLocalBuilder(),
                        finallyExpected);
            }
        });
    }
}
