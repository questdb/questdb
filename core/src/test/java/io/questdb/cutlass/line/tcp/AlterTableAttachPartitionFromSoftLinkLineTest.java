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

package io.questdb.cutlass.line.tcp;

import io.questdb.AbstractBootstrapTest;
import io.questdb.Bootstrap;
import io.questdb.ServerMain;
import io.questdb.cairo.*;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cutlass.line.LineTcpSender;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.network.Net;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.Path;
import org.junit.*;
import org.junit.rules.TestName;

import java.util.concurrent.TimeUnit;

import static io.questdb.cairo.TableUtils.createTable;
import static io.questdb.test.tools.TestUtils.insertFromSelectPopulateTableStmt;
import static io.questdb.test.tools.TestUtils.assertSql;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class AlterTableAttachPartitionFromSoftLinkLineTest extends AbstractBootstrapTest {
    private static final String firstPartitionName = "2022-12-08";
    private static final long firstPartitionTs;
    private static final String futurePartitionName = "2022-12-12";
    private static final long futurePartitionTs;
    private static final String lastPartitionName = "2022-12-11";
    private static final long lastPartitionTs;
    private static final long lineTsStepNanos = TimeUnit.MILLISECONDS.toNanos(1L); // min resolution of Timestamps.toString(long)
    private static final String secondPartitionName = "2022-12-09";
    private static final long secondPartitionTs;
    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractBootstrapTest.setUpStatic();
        try {
            createDummyConfiguration(
                    "cairo.max.uncommitted.rows=500",
                    "cairo.commit.lag=2000",
                    "cairo.o3.max.lag=2000" // millis
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testActivePartitionReadOnlyAndNoO3() throws Exception {
        String tableName = testName.getMethodName();
        assertServerMainWithLine(
                tableName,
                () -> {
                    long[] timestampNano = {lastPartitionTs * 1000L};
                    try (LineTcpSender sender = LineTcpSender.newSender(Net.parseIPv4("127.0.0.1"), ILP_PORT, ILP_BUFFER_SIZE)) {
                        // send data hitting a read-only partition
                        for (int tick = 0; tick < 1000; tick++) {
                            int tickerId = 0;
                            timestampNano[tickerId] += lineTsStepNanos;
                            sender.metric(tableName)
                                    .tag("s", "lobster")
                                    .field("l", 88)
                                    .field("i", 2124)
                                    .at(timestampNano[tickerId]);
                        }
                        sender.flush();
                    }
                },
                "min\tmax\tcount\n" +
                        "2022-12-08T00:05:11.070207Z\t2022-12-09T00:01:17.517546Z\t278\n" +
                        "2022-12-09T00:06:28.587753Z\t2022-12-10T00:02:35.035092Z\t278\n" +
                        "2022-12-10T00:07:46.105299Z\t2022-12-11T00:03:52.552638Z\t278\n" +
                        "2022-12-11T00:09:03.622845Z\t2022-12-11T23:59:58.999977Z\t277\n",  // <-- read only, remains intact
                false, false, false, true
        );
    }

    @Test
    public void testActivePartitionReadOnlyAndO3OverActivePartition() throws Exception {
        final String tableName = testName.getMethodName();
        assertServerMainWithLine(
                tableName,
                () -> {
                    long[] timestampNano = {lastPartitionTs * 1000L, futurePartitionTs * 1000L};
                    try (LineTcpSender sender = LineTcpSender.newSender(Net.parseIPv4("127.0.0.1"), ILP_PORT, ILP_BUFFER_SIZE)) {
                        for (int tick = 0; tick < 4; tick++) {
                            int tickerId = tick % timestampNano.length;
                            timestampNano[tickerId] += lineTsStepNanos;
                            sender.metric(tableName)
                                    .tag("s", "lobster")
                                    .field("l", 88)
                                    .field("i", 2124)
                                    .at(timestampNano[tickerId]);
                        }
                        sender.flush();
                    }
                },
                "min\tmax\tcount\n" +
                        "2022-12-08T00:05:11.070207Z\t2022-12-09T00:01:17.517546Z\t278\n" +
                        "2022-12-09T00:06:28.587753Z\t2022-12-10T00:02:35.035092Z\t278\n" +
                        "2022-12-10T00:07:46.105299Z\t2022-12-11T00:03:52.552638Z\t278\n" +
                        "2022-12-11T00:09:03.622845Z\t2022-12-11T23:59:58.999977Z\t277\n" + // <-- read only, remains intact
                        "2022-12-12T00:09:03.622845Z\t2022-12-12T23:59:58.999977Z\t1\n", // TODO amend the dates
                false, false, false, true
        );
    }

    @Test
    public void testActivePartitionReadOnlyAndO3UnderActivePartition() throws Exception {
        final String tableName = testName.getMethodName();
        assertServerMainWithLine(
                tableName,
                () -> {
                    long[] timestampNano = {secondPartitionTs * 1000L, lastPartitionTs * 1000L}; // last is read-only
                    try (LineTcpSender sender = LineTcpSender.newSender(Net.parseIPv4("127.0.0.1"), ILP_PORT, ILP_BUFFER_SIZE)) {
                        for (int tick = 0; tick < 4; tick++) {
                            int tickerId = tick % timestampNano.length;
                            timestampNano[tickerId] += lineTsStepNanos;
                            sender.metric(tableName)
                                    .tag("s", "lobster")
                                    .field("l", 88)
                                    .field("i", 2124)
                                    .at(timestampNano[tickerId]);
                        }
                        sender.flush();
                    }
                },
                "min\tmax\tcount\n" +
                        "2022-12-08T00:05:11.070207Z\t2022-12-09T00:01:17.517546Z\t278\n" +
                        "2022-12-09T00:06:28.587753Z\t2022-12-10T00:02:35.035092Z\t280\n" + // update dates
                        "2022-12-10T00:07:46.105299Z\t2022-12-11T00:03:52.552638Z\t278\n" +
                        "2022-12-11T00:09:03.622845Z\t2022-12-11T23:59:58.999977Z\t277\n",
                true, false, true, true
        );
    }

    @Test
    public void testTableIsReadOnly() throws Exception {
        final String tableName = testName.getMethodName();
        assertServerMainWithLine(
                tableName,
                () -> {
                    long[] timestampNano = {secondPartitionTs * 1000L, lastPartitionTs * 1000L};
                    try (LineTcpSender sender = LineTcpSender.newSender(Net.parseIPv4("127.0.0.1"), ILP_PORT, ILP_BUFFER_SIZE)) {
                        for (int tick = 0; tick < 5000; tick++) {
                            int tickerId = tick % timestampNano.length;
                            timestampNano[tickerId] += lineTsStepNanos;
                            sender.metric(tableName)
                                    .tag("s", "lobster")
                                    .field("l", 88)
                                    .field("i", 2124)
                                    .at(timestampNano[tickerId]);
                        }
                        sender.flush();
                    }
                },
                // no change
                "min\tmax\tcount\n" +
                        "2022-12-08T00:05:11.070207Z\t2022-12-09T00:01:17.517546Z\t278\n" +
                        "2022-12-09T00:06:28.587753Z\t2022-12-10T00:02:35.035092Z\t278\n" +
                        "2022-12-10T00:07:46.105299Z\t2022-12-11T00:03:52.552638Z\t278\n" +
                        "2022-12-11T00:09:03.622845Z\t2022-12-11T23:59:58.999977Z\t277\n",
                true, true, true, true
        );
    }

    private static void assertServerMainWithLine(
            String tableName,
            Runnable test,
            String finallyExpected,
            boolean... partitionIsReadOnly
    ) throws Exception {
        assertMemoryLeak(() -> {
            try (
                    ServerMain qdb = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
                    CairoEngine engine = qdb.getCairoEngine();
                    SqlCompiler compiler = new SqlCompiler(engine);
                    SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(
                            AllowAllCairoSecurityContext.INSTANCE,
                            null,
                            null,
                            -1,
                            null)
            ) {
                qdb.start();

                // create a table with 4 partitions and 1111 rows
                CairoConfiguration cairoConfig = qdb.getConfiguration().getCairoConfiguration();
                try (
                        TableModel tableModel = new TableModel(cairoConfig, tableName, PartitionBy.DAY)
                                .col("l", ColumnType.LONG)
                                .col("i", ColumnType.INT)
                                .col("s", ColumnType.SYMBOL).symbolCapacity(32)
                                .timestamp("ts");
                        MemoryMARW mem = Vm.getMARWInstance();
                        Path path = new Path().of(cairoConfig.getRoot()).concat(tableName)
                ) {
                    createTable(cairoConfig, mem, path, tableModel, 1);
                    compiler.compile(insertFromSelectPopulateTableStmt(tableModel, 1111, firstPartitionName, 4), context);
                }
                assertSql(
                        compiler,
                        context,
                        "SELECT min(ts), max(ts), count() FROM " + tableName + " sample by 1d",
                        Misc.getThreadLocalBuilder(),
                        "min\tmax\tcount\n" +
                                "2022-12-08T00:05:11.070207Z\t2022-12-09T00:01:17.517546Z\t278\n" +
                                "2022-12-09T00:06:28.587753Z\t2022-12-10T00:02:35.035092Z\t278\n" +
                                "2022-12-10T00:07:46.105299Z\t2022-12-11T00:03:52.552638Z\t278\n" +
                                "2022-12-11T00:09:03.622845Z\t2022-12-11T23:59:58.999977Z\t277\n");

                // set partition read-only state
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "read-only-state")) {
                    TxWriter txWriter = writer.getTxWriter();
                    int partitionCount = txWriter.getPartitionCount();
                    Assert.assertEquals(partitionIsReadOnly.length, partitionCount);
                    for (int i = 0; i < partitionCount; i++) {
                        txWriter.setPartitionReadOnly(i, partitionIsReadOnly[i]);
                    }
                    txWriter.bumpTruncateVersion();
                    txWriter.commit(CommitMode.NOSYNC, writer.getDenseSymbolMapWriters()); // default commit mode
                }
                // check read only state
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                    TxReader txFile = reader.getTxFile();
                    for (int i = 0, n = txFile.getPartitionCount(); i < n; i++) {
                        Assert.assertEquals(txFile.isPartitionReadOnly(i), partitionIsReadOnly[i]);
                        Assert.assertEquals(txFile.isPartitionReadOnlyByPartitionTimestamp(txFile.getPartitionTimestamp(i)), partitionIsReadOnly[i]);
                    }
                }

                // so that we know when the table writer is returned to the pool
                final SOCountDownLatch tableWriterReturnedToPool = new SOCountDownLatch(1);
                engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                    if (Chars.equalsNc(tableName, name)) {
                        if (factoryType == PoolListener.SRC_WRITER && event == PoolListener.EV_RETURN) {
                            tableWriterReturnedToPool.countDown();
                        }
                    }
                });

                // run the test
                test.run();

                // wait for the table writer to be returned to the pool
                tableWriterReturnedToPool.await();

                // check expected results
                assertSql(compiler, context, "SELECT min(ts), max(ts), count() FROM " + tableName + " sample by 1d", Misc.getThreadLocalBuilder(), finallyExpected);
            }
        });
    }

    static {
        try {
            secondPartitionTs = TimestampFormatUtils.parseTimestamp(secondPartitionName + "T00:00:00.000Z");
            lastPartitionTs = TimestampFormatUtils.parseTimestamp(lastPartitionName + "T00:00:00.000Z");
            futurePartitionTs = TimestampFormatUtils.parseTimestamp(futurePartitionName + "T00:00:00.000Z");
            firstPartitionTs = TimestampFormatUtils.parseTimestamp(firstPartitionName + "T00:00:00.000Z");
        } catch (NumericException impossible) {
            throw new RuntimeException(impossible);
        }
    }

    static {
        LogFactory.getLog(AlterTableAttachPartitionFromSoftLinkLineTest.class);
    }
}
