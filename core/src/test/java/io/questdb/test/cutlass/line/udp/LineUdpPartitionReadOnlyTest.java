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

package io.questdb.test.cutlass.line.udp;

import io.questdb.ServerMain;
import io.questdb.cairo.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cutlass.line.LineUdpSender;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.cutlass.line.AbstractLinePartitionReadOnlyTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.createTable;
import static io.questdb.test.tools.TestUtils.*;

public class LineUdpPartitionReadOnlyTest extends AbstractLinePartitionReadOnlyTest {

    @Test
    public void testActivePartitionReadOnlyAndNoO3UDP() throws Exception {
        String tableName = testName.getMethodName();
        int numEvents = 5000;
        SOCountDownLatch sendComplete = new SOCountDownLatch(1);
        assertServerMainWithLineUDP(
                tableName,
                () -> {
                    long[] timestampNano = {lastPartitionTs};
                    try (LineUdpSender sender = new LineUdpSender(NetworkFacadeImpl.INSTANCE, 0, Net.parseIPv4("127.0.0.1"), ILP_PORT, 200, 1)) {
                        for (int tick = 0; tick < numEvents; tick++) {
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
                0,
                TABLE_START_CONTENT,  // <-- read only, remains intact
                false, false, false, true
        );
    }

    @Test
    public void testActivePartitionReadOnlyAndO3OverActivePartitionUDP() throws Exception {
        final String tableName = testName.getMethodName();
        int numEvents = 5000;
        SOCountDownLatch sendComplete = new SOCountDownLatch(1);
        assertServerMainWithLineUDP(
                tableName,
                () -> {
                    long[] timestampNano = {firstPartitionTs, secondPartitionTs, thirdPartitionTs, lastPartitionTs, futurePartitionTs};
                    try (LineUdpSender sender = new LineUdpSender(NetworkFacadeImpl.INSTANCE, 0, Net.parseIPv4("127.0.0.1"), ILP_PORT, 200, 1)) {
                        for (int tick = 0; tick < numEvents; tick++) {
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
                5000,
                null,
                false, false, false, true, false
        );
    }

    @Test
    public void testActivePartitionReadOnlyAndO3UnderActivePartitionUDP() throws Exception {
        final String tableName = testName.getMethodName();
        int numEvents = 5000;
        SOCountDownLatch sendComplete = new SOCountDownLatch(1);
        assertServerMainWithLineUDP(
                tableName,
                () -> {
                    long[] timestampNano = {firstPartitionTs, secondPartitionTs, thirdPartitionTs, lastPartitionTs};
                    try (LineUdpSender sender = new LineUdpSender(NetworkFacadeImpl.INSTANCE, 0, Net.parseIPv4("127.0.0.1"), ILP_PORT, 200, 1)) {
                        for (int tick = 0; tick < numEvents; tick++) {
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
                1666,
                null,
                true, false, true, true
        );
    }

    @Test
    public void testTableIsReadOnlyUDP() throws Exception {
        final String tableName = testName.getMethodName();
        int numEvents = 5000;
        SOCountDownLatch sendComplete = new SOCountDownLatch(1);
        assertServerMainWithLineUDP(
                tableName,
                () -> {
                    long[] timestampNano = {firstPartitionTs, secondPartitionTs, thirdPartitionTs, lastPartitionTs};
                    try (LineUdpSender sender = new LineUdpSender(NetworkFacadeImpl.INSTANCE, 0, Net.parseIPv4("127.0.0.1"), ILP_PORT, 200, 1)) {
                        for (int tick = 0; tick < numEvents; tick++) {
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
                0,
                TABLE_START_CONTENT,
                true, true, true, true
        );
    }

    private static void assertServerMainWithLineUDP(
            String tableName,
            Runnable test,
            SOCountDownLatch sendComplete,
            int expectedNewEvents,
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
                TableToken tableToken;
                try (
                        TableModel tableModel = new TableModel(cairoConfig, tableName, PartitionBy.DAY)
                                .col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).symbolCapacity(32)
                                .timestamp("ts");
                        MemoryMARW mem = Vm.getMARWInstance();
                        Path path = new Path().of(cairoConfig.getRoot())
                ) {
                    tableToken = engine.lockTableName(tableName, 1, false);
                    Assert.assertNotNull(tableToken);
                    engine.registerTableToken(tableToken);
                    createTable(cairoConfig, mem, path.concat(tableToken), tableModel, 1, tableToken.getDirName());
                    compiler.compile(insertFromSelectPopulateTableStmt(tableModel, 1111, firstPartitionName, 4), context);
                    engine.unlockTableName(tableToken);
                }
                Assert.assertNotNull(tableToken);

                // set partition read-only state
                final long[] partitionSizes = new long[partitionIsReadOnly.length];
                try (TableWriter writer = engine.getWriter(tableToken, "read-only-state")) {
                    TxWriter txWriter = writer.getTxWriter();
                    int partitionCount = txWriter.getPartitionCount();
                    Assert.assertTrue(partitionCount <= partitionIsReadOnly.length);
                    for (int i = 0; i < partitionCount; i++) {
                        txWriter.setPartitionReadOnly(i, partitionIsReadOnly[i]);
                        partitionSizes[i] = writer.getPartitionSize(i);
                    }
                    txWriter.bumpTruncateVersion();
                    txWriter.commit(CommitMode.NOSYNC, writer.getDenseSymbolMapWriters()); // default commit mode
                }

                // check read only state
                checkPartitionReadOnlyState(engine, tableToken, partitionIsReadOnly);

                assertSql(
                        compiler,
                        context,
                        "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO CALENDAR",
                        Misc.getThreadLocalBuilder(),
                        TABLE_START_CONTENT);

                // run the test
                test.run();
                sendComplete.await();

                // check expected results
                if (expectedNewEvents > 0) {
                    long start = System.currentTimeMillis();
                    while (System.currentTimeMillis() - start < 2000L) {
                        try (TableReader reader = engine.getReader(tableToken)) {
                            if (1111 + expectedNewEvents <= reader.size()) {
                                break;
                            }
                        }
                        Os.sleep(1L);
                    }

                    try (TableReader reader = engine.getReader(tableToken)) {
                        int partitionCount = reader.getPartitionCount();
                        Assert.assertTrue(partitionCount <= partitionIsReadOnly.length);
                        for (int i = 0; i < partitionCount; i++) {
                            long newPartitionSize = reader.getTxFile().getPartitionSize(i);
                            if (!reader.getTxFile().isPartitionReadOnly(i)) {
                                Assert.assertTrue(partitionSizes[i] < newPartitionSize);
                            } else {
                                Assert.assertEquals(partitionSizes[i], newPartitionSize);
                            }
                        }
                    }
                }

                // check read only state, no changes
                checkPartitionReadOnlyState(engine, tableToken, partitionIsReadOnly);

                if (finallyExpected != null) {
                    assertSql(
                            compiler,
                            context,
                            "SELECT min(ts), max(ts), count() FROM " + tableName + " SAMPLE BY 1d ALIGN TO CALENDAR",
                            Misc.getThreadLocalBuilder(),
                            finallyExpected);
                }

                engine.unlockTableName(tableToken);
            }
        });
    }
}
