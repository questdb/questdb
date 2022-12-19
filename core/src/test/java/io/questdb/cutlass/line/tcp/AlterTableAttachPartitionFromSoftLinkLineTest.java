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

import static io.questdb.cairo.TableUtils.createTable;
import static io.questdb.test.tools.TestUtils.insertFromSelectPopulateTableStmt;
import static io.questdb.test.tools.TestUtils.assertSql;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class AlterTableAttachPartitionFromSoftLinkLineTest extends AbstractBootstrapTest {

    private static final String activePartitionName = "2022-12-11";
    private static final long activePartitionTs;
    private static final String futurePartitionName = "2022-12-12";
    private static final long futurePartitionTs;
    private static final String readOnlyPartitionName = "2022-12-08";
    private static final long readOnlyPartitionTs;
    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractBootstrapTest.setUpStatic();
        try {
            createDummyConfiguration(
                    "cairo.max.uncommitted.rows=500",
                    "cairo.commit.lag=128",
                    "cairo.o3.max.lag=128"
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testServerMainLineTcpSenderWithActivePartitionReadOnly() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final int partitionCount = 4;
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
                    compiler.compile(insertFromSelectPopulateTableStmt(tableModel, 1111, readOnlyPartitionName, partitionCount), context);
                }
                assertSql(
                        compiler,
                        context,
                        "SELECT min(ts), max(ts), count() FROM " + tableName + " sample by 1d",
                        Misc.getThreadLocalBuilder(),
                        "min\tmax\tcount\n" +
                                "2022-12-08T00:05:11.070207Z\t2022-12-09T00:01:17.517546Z\t278\n" + // <-- read only
                                "2022-12-09T00:06:28.587753Z\t2022-12-10T00:02:35.035092Z\t278\n" + // <-- read only
                                "2022-12-10T00:07:46.105299Z\t2022-12-11T00:03:52.552638Z\t278\n" + // <-- read only
                                "2022-12-11T00:09:03.622845Z\t2022-12-11T23:59:58.999977Z\t277\n"); // <-- read only

                // make all partitions read-only
                try (TableWriter writer = qdb.getCairoEngine().getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "read-only-flag")) {
                    TxWriter txWriter = writer.getTxWriter();
                    Assert.assertEquals(partitionCount, txWriter.getPartitionCount());
                    for (int i = 0; i < partitionCount; i++) {
                        txWriter.setPartitionReadOnly(i, true);
                    }
                    txWriter.bumpTruncateVersion();
                    txWriter.commit(cairoConfig.getCommitMode(), writer.getDenseSymbolMapWriters());
                }

                // check read only flag
                qdb.getCairoEngine().releaseAllWriters();
                qdb.getCairoEngine().releaseAllReaders();
                try (TableReader reader = qdb.getCairoEngine().getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                    TxReader txFile = reader.getTxFile();
                    for (int i = 0; i < partitionCount; i++) {
                        Assert.assertTrue(txFile.isPartitionReadOnly(i));
                        Assert.assertTrue(txFile.isPartitionReadOnlyByPartitionTimestamp(txFile.getPartitionTimestamp(i)));
                    }
                }

                // so that we know when the table writer is returned to the pool
                final SOCountDownLatch tableWriterReturnedToPool = new SOCountDownLatch(1);
                qdb.getCairoEngine().setPoolListener((factoryType, thread, name, event, segment, position) -> {
                    if (Chars.equalsNc(tableName, name)) {
                        if (factoryType == PoolListener.SRC_WRITER && event == PoolListener.EV_RETURN) {
                            tableWriterReturnedToPool.countDown();
                        }
                    }
                });

                long[] timestampNano = {activePartitionTs * 1000L, futurePartitionTs * 1000L};

                try (LineTcpSender sender = LineTcpSender.newSender(Net.parseIPv4("127.0.0.1"), ILP_PORT, ILP_BUFFER_SIZE)) {
                    // send data hitting a read-only partition
                    for (int tick = 0; tick < 2; tick++) { // two rows
                        int tickerId = 0;
                        timestampNano[tickerId] += 100_000L;
                        sender.metric(tableName)
                                .tag("s", "lobster")
                                .field("l", 88)
                                .field("i", 2124)
                                .at(timestampNano[tickerId]);
                    }
                    sender.flush();
                }
                tableWriterReturnedToPool.await();
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
            }
        });
    }

    @Test
    public void testServerMainLineTcpSenderWithActivePartitionReadOnlyAndNoO3() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final int partitionCount = 4;
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
                    compiler.compile(insertFromSelectPopulateTableStmt(tableModel, 1111, readOnlyPartitionName, partitionCount), context);
                }
                assertSql(
                        compiler,
                        context,
                        "SELECT min(ts), max(ts), count() FROM " + tableName + " sample by 1d",
                        Misc.getThreadLocalBuilder(),
                        "min\tmax\tcount\n" +
                                "2022-12-08T00:05:11.070207Z\t2022-12-09T00:01:17.517546Z\t278\n" + // <-- read only
                                "2022-12-09T00:06:28.587753Z\t2022-12-10T00:02:35.035092Z\t278\n" + // <-- read only
                                "2022-12-10T00:07:46.105299Z\t2022-12-11T00:03:52.552638Z\t278\n" + // <-- read only
                                "2022-12-11T00:09:03.622845Z\t2022-12-11T23:59:58.999977Z\t277\n"); // <-- read only

                // make all partitions read-only
                try (TableWriter writer = qdb.getCairoEngine().getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "read-only-flag")) {
                    TxWriter txWriter = writer.getTxWriter();
                    Assert.assertEquals(partitionCount, txWriter.getPartitionCount());
                    for (int i = 0; i < partitionCount; i++) {
                        txWriter.setPartitionReadOnly(i, true);
                    }
                    txWriter.bumpTruncateVersion();
                    txWriter.commit(cairoConfig.getCommitMode(), writer.getDenseSymbolMapWriters());
                }

                // check read only flag
                qdb.getCairoEngine().releaseAllWriters();
                qdb.getCairoEngine().releaseAllReaders();
                try (TableReader reader = qdb.getCairoEngine().getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                    TxReader txFile = reader.getTxFile();
                    for (int i = 0; i < partitionCount; i++) {
                        Assert.assertTrue(txFile.isPartitionReadOnly(i));
                        Assert.assertTrue(txFile.isPartitionReadOnlyByPartitionTimestamp(txFile.getPartitionTimestamp(i)));
                    }
                }

                // so that we know when the table writer is returned to the pool
                final SOCountDownLatch tableWriterReturnedToPool = new SOCountDownLatch(1);
                qdb.getCairoEngine().setPoolListener((factoryType, thread, name, event, segment, position) -> {
                    if (Chars.equalsNc(tableName, name)) {
                        if (factoryType == PoolListener.SRC_WRITER && event == PoolListener.EV_RETURN) {
                            tableWriterReturnedToPool.countDown();
                        }
                    }
                });

                long[] timestampNano = {activePartitionTs * 1000L, futurePartitionTs * 1000L};

                try (LineTcpSender sender = LineTcpSender.newSender(Net.parseIPv4("127.0.0.1"), ILP_PORT, ILP_BUFFER_SIZE)) {
                    // send data hitting a read-only partition
                    for (int tick = 0; tick < 1000; tick++) {
                        int tickerId = 0;
                        timestampNano[tickerId] += 100_000L;
                        sender.metric(tableName)
                                .tag("s", "lobster")
                                .field("l", 88)
                                .field("i", 2124)
                                .at(timestampNano[tickerId]);
                    }
                    sender.flush();
                }
                tableWriterReturnedToPool.await();
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
            }
        });
    }

    @Test
    @Ignore
    // TODO bug in o3Commit will fix in follow up PR
    public void testServerMainLineTcpSenderWithActivePartitionReadOnlyAndO3() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final int partitionCount = 4;
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
                    compiler.compile(insertFromSelectPopulateTableStmt(tableModel, 1111, readOnlyPartitionName, partitionCount), context);
                }
                assertSql(
                        compiler,
                        context,
                        "SELECT min(ts), max(ts), count() FROM " + tableName + " sample by 1d",
                        Misc.getThreadLocalBuilder(),
                        "min\tmax\tcount\n" +
                                "2022-12-08T00:05:11.070207Z\t2022-12-09T00:01:17.517546Z\t278\n" + // <-- read only
                                "2022-12-09T00:06:28.587753Z\t2022-12-10T00:02:35.035092Z\t278\n" + // <-- read only
                                "2022-12-10T00:07:46.105299Z\t2022-12-11T00:03:52.552638Z\t278\n" + // <-- read only
                                "2022-12-11T00:09:03.622845Z\t2022-12-11T23:59:58.999977Z\t277\n"); // <-- read only

                // make all partitions read-only
                try (TableWriter writer = qdb.getCairoEngine().getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "read-only-flag")) {
                    TxWriter txWriter = writer.getTxWriter();
                    Assert.assertEquals(partitionCount, txWriter.getPartitionCount());
                    for (int i = 0; i < partitionCount; i++) {
                        txWriter.setPartitionReadOnly(i, true);
                    }
                    txWriter.bumpTruncateVersion();
                    txWriter.commit(cairoConfig.getCommitMode(), writer.getDenseSymbolMapWriters());
                }

                // check read only flag
                qdb.getCairoEngine().releaseAllWriters();
                qdb.getCairoEngine().releaseAllReaders();
                try (TableReader reader = qdb.getCairoEngine().getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                    TxReader txFile = reader.getTxFile();
                    for (int i = 0; i < partitionCount; i++) {
                        Assert.assertTrue(txFile.isPartitionReadOnly(i));
                        Assert.assertTrue(txFile.isPartitionReadOnlyByPartitionTimestamp(txFile.getPartitionTimestamp(i)));
                    }
                }

                // so that we know when the table writer is returned to the pool
                final SOCountDownLatch tableWriterReturnedToPool = new SOCountDownLatch(1);
                qdb.getCairoEngine().setPoolListener((factoryType, thread, name, event, segment, position) -> {
                    if (Chars.equalsNc(tableName, name)) {
                        if (factoryType == PoolListener.SRC_WRITER && event == PoolListener.EV_RETURN) {
                            tableWriterReturnedToPool.countDown();
                        }
                    }
                });

                long[] timestampNano = {activePartitionTs * 1000L, futurePartitionTs * 1000L};

                try (LineTcpSender sender = LineTcpSender.newSender(Net.parseIPv4("127.0.0.1"), ILP_PORT, ILP_BUFFER_SIZE)) {
                    // send data hitting a read-only partition
                    for (int tick = 0; tick < 2; tick++) { // two rows
                        int tickerId = 0;
                        timestampNano[tickerId] += 100_000L;
                        sender.metric(tableName)
                                .tag("s", "lobster")
                                .field("l", 88)
                                .field("i", 2124)
                                .at(timestampNano[tickerId]);
                    }
                    sender.flush();

                    // send data from the future, which will create new partition
                    for (int tick = 0; tick < 2; tick++) { // two more rows
                        int tickerId = 1;
                        timestampNano[tickerId] += 100_000L;
                        sender.metric(tableName)
                                .tag("s", "lobster")
                                .field("l", 88)
                                .field("i", 15566551)
                                .at(timestampNano[tickerId]);
                    }
                    sender.flush();
                }
                tableWriterReturnedToPool.await();
                assertSql(
                        compiler,
                        context,
                        "SELECT min(ts), max(ts), count() FROM " + tableName + " sample by 1d",
                        Misc.getThreadLocalBuilder(),
                        "min\tmax\tcount\n" +
                                "2022-12-08T00:05:11.070207Z\t2022-12-09T00:01:17.517546Z\t278\n" + // <-- read only
                                "2022-12-09T00:06:28.587753Z\t2022-12-10T00:02:35.035092Z\t278\n" + // <-- read only
                                "2022-12-10T00:07:46.105299Z\t2022-12-11T00:03:52.552638Z\t278\n" + // <-- read only
                                "2022-12-11T00:09:03.622845Z\t2022-12-11T23:59:58.999977Z\t277\n" + // <-- read only
                                "2022-12-12T00:09:03.622845Z\t2022-12-12T23:59:58.999977Z\t2\n"); // TODO amend the dates
            }
        });
    }

    @Test
    public void testServerMainLineTcpSenderWithReadOnlyPartitionO3() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final int partitionCount = 4;
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
                    compiler.compile(insertFromSelectPopulateTableStmt(tableModel, 1111, readOnlyPartitionName, partitionCount), context);
                }
                assertSql(
                        compiler,
                        context,
                        "SELECT min(ts), max(ts), count() FROM " + tableName + " sample by 1d",
                        Misc.getThreadLocalBuilder(),
                        "min\tmax\tcount\n" +
                                "2022-12-08T00:05:11.070207Z\t2022-12-09T00:01:17.517546Z\t278\n" + // <-- read only
                                "2022-12-09T00:06:28.587753Z\t2022-12-10T00:02:35.035092Z\t278\n" +
                                "2022-12-10T00:07:46.105299Z\t2022-12-11T00:03:52.552638Z\t278\n" +
                                "2022-12-11T00:09:03.622845Z\t2022-12-11T23:59:58.999977Z\t277\n");

                // make the eldest (1st) partition read-only
                try (TableWriter writer = qdb.getCairoEngine().getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "read-only-flag")) {
                    TxWriter txWriter = writer.getTxWriter();
                    txWriter.setPartitionReadOnlyByTimestamp(readOnlyPartitionTs, true);
                    txWriter.bumpTruncateVersion();
                    txWriter.commit(cairoConfig.getCommitMode(), writer.getDenseSymbolMapWriters());
                }

                // check read only flag
                qdb.getCairoEngine().releaseAllWriters();
                qdb.getCairoEngine().releaseAllReaders();

                try (TableReader reader = qdb.getCairoEngine().getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                    TxReader txFile = reader.getTxFile();
                    Assert.assertNotNull(txFile);
                    Assert.assertTrue(txFile.isPartitionReadOnly(0));
                    Assert.assertTrue(txFile.isPartitionReadOnlyByPartitionTimestamp(readOnlyPartitionTs));
                    Assert.assertEquals(partitionCount, txFile.getPartitionCount());
                    for (int i = 1; i < partitionCount; i++) {
                        Assert.assertFalse(txFile.isPartitionReadOnly(i));
                        Assert.assertFalse(txFile.isPartitionReadOnlyByPartitionTimestamp(txFile.getPartitionTimestamp(i)));
                    }
                }

                // so that we know when the table writer is returned to the pool
                final SOCountDownLatch tableWriterReturnedToPool = new SOCountDownLatch(1);
                qdb.getCairoEngine().setPoolListener((factoryType, thread, name, event, segment, position) -> {
                    if (Chars.equalsNc(tableName, name)) {
                        if (factoryType == PoolListener.SRC_WRITER && event == PoolListener.EV_RETURN) {
                            tableWriterReturnedToPool.countDown();
                        }
                    }
                });

                long[] timestampNano = {readOnlyPartitionTs * 1000L, activePartitionTs * 1000L};

                try (LineTcpSender sender = LineTcpSender.newSender(Net.parseIPv4("127.0.0.1"), ILP_PORT, ILP_BUFFER_SIZE)) {
                    // send O3 hitting a read-only partition
                    for (int tick = 0; tick < 1000; tick++) {
                        int tickerId = tick % 2;
                        timestampNano[tickerId] += 100_000L;
                        sender.metric(tableName)
                                .tag("s", "oyster")
                                .field("l", 44)
                                .field("i", 2023)
                                .at(timestampNano[tickerId]);
                    }
                    sender.flush();

                    // send only hitting the active partition
                    for (int tick = 0; tick < 5; tick++) {
                        int tickerId = 1;
                        timestampNano[tickerId] += 100_000L;
                        sender.metric(tableName)
                                .tag("s", "oyster")
                                .field("l", 44)
                                .field("i", 2023)
                                .at(timestampNano[tickerId]);
                    }
                    sender.flush();
                }
                tableWriterReturnedToPool.await();
                assertSql(
                        compiler,
                        context,
                        "SELECT min(ts), max(ts), count() FROM " + tableName + " sample by 1d",
                        Misc.getThreadLocalBuilder(),
                        "min\tmax\tcount\n" +
                                "2022-12-08T00:05:11.070207Z\t2022-12-09T00:01:17.517546Z\t278\n" + // no change
                                "2022-12-09T00:06:28.587753Z\t2022-12-10T00:02:35.035092Z\t278\n" +
                                "2022-12-10T00:07:46.105299Z\t2022-12-11T00:03:52.552638Z\t783\n" +
                                "2022-12-11T00:09:03.622845Z\t2022-12-11T23:59:58.999977Z\t277\n"); // 505 as expected
            }
        });
    }

    static {
        try {
            readOnlyPartitionTs = TimestampFormatUtils.parseTimestamp(readOnlyPartitionName + "T00:00:00.000Z");
            activePartitionTs = TimestampFormatUtils.parseTimestamp(activePartitionName + "T00:00:00.000Z");
            futurePartitionTs = TimestampFormatUtils.parseTimestamp(futurePartitionName + "T00:00:00.000Z");
        } catch (NumericException impossible) {
            throw new RuntimeException(impossible);
        }
    }

    static {
        LogFactory.getLog(AlterTableAttachPartitionFromSoftLinkLineTest.class);
    }
}
