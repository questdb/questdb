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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.createTable;
import static io.questdb.test.tools.TestUtils.insertFromSelectPopulateTableStmt;
import static io.questdb.test.tools.TestUtils.assertSql;
import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class AlterTableAttachPartitionFromSoftLinkLineTest extends AbstractBootstrapTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractBootstrapTest.setUpStatic();
        try {
            createDummyConfiguration(
                    "cairo.max.uncommitted.rows=1",
                    "cairo.commit.lag=200",
                    "cairo.o3.max.lag=200"
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testServerMainLineTcpSenderWithReadOnlyPartition() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "banana";
            final String readOnlyPartitionName = "2022-12-08";
            final String activePartitionName = "2022-12-12";
            final long readOnlyPartitionTs;
            final long activePartitionTs;
            try {
                readOnlyPartitionTs = TimestampFormatUtils.parseTimestamp(readOnlyPartitionName + "T00:00:00.000Z");
                activePartitionTs = TimestampFormatUtils.parseTimestamp(activePartitionName + "T00:00:00.000Z");
            } catch (NumericException impossible) {
                throw new RuntimeException(impossible);
            }
            final String expected = "min\tmax\tcount\n" +
                    "2022-12-08T00:05:11.070207Z\t2022-12-09T00:01:17.517546Z\t278\n" +
                    "2022-12-09T00:06:28.587753Z\t2022-12-10T00:02:35.035092Z\t278\n" +
                    "2022-12-10T00:07:46.105299Z\t2022-12-11T00:03:52.552638Z\t278\n" +
                    "2022-12-11T00:09:03.622845Z\t2022-12-11T23:59:58.999977Z\t277\n";

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

                // create a table with 2 partitions and 10000 rows
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
                    compiler.compile(insertFromSelectPopulateTableStmt(tableModel, 1111, readOnlyPartitionName, 4), context);
                }
                assertSql(
                        compiler,
                        context,
                        "SELECT min(ts), max(ts), count() FROM " + tableName + " sample by 1d",
                        Misc.getThreadLocalBuilder(),
                        expected);

                // make the eldest partition read-only
                try (TableWriter writer = qdb.getCairoEngine().getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "read-only-flag")) {
                    TxWriter txWriter = writer.getTxWriter();
                    txWriter.setPartitionReadOnlyByTimestamp(readOnlyPartitionTs, true);
                    txWriter.bumpTruncateVersion();
                    txWriter.commit(cairoConfig.getCommitMode(), writer.getDenseSymbolMapWriters());
                }
                qdb.getCairoEngine().releaseAllWriters();
                qdb.getCairoEngine().releaseAllReaders();
                try (TableReader reader = qdb.getCairoEngine().getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                    TxReader txFile = reader.getTxFile();
                    Assert.assertNotNull(txFile);
                    Assert.assertTrue(txFile.isPartitionReadOnly(0));
                    Assert.assertFalse(txFile.isPartitionReadOnly(1));
                    Assert.assertTrue(txFile.isPartitionReadOnlyByPartitionTimestamp(readOnlyPartitionTs));
                    Assert.assertFalse(txFile.isPartitionReadOnlyByPartitionTimestamp(activePartitionTs));
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
                    // send O3 hitting a read-only partition, will fail and roll back
                    for (int tick = 0; tick < 2; tick++) {
                        timestampNano[tick % 2] += 100_000L;
                        sender.metric(tableName)
                                .tag("s", "oyster")
                                .field("l", 44)
                                .field("i", 2023)
                                .at(timestampNano[tick % 2]);
                    }
                }
                tableWriterReturnedToPool.await();
                // no change
                assertSql(
                        compiler,
                        context,
                        "SELECT min(ts), max(ts), count() FROM " + tableName + " sample by 1d",
                        Misc.getThreadLocalBuilder(),
                        expected);

                tableWriterReturnedToPool.setCount(1);
                try (LineTcpSender sender = LineTcpSender.newSender(Net.parseIPv4("127.0.0.1"), ILP_PORT, ILP_BUFFER_SIZE)) {
                    // send only hitting the active partition
                    for (int tick = 0; tick < 5; tick++) {
                        timestampNano[1] += 100_000L;
                        sender.metric(tableName)
                                .tag("s", "oyster")
                                .field("l", 44)
                                .field("i", 2023)
                                .at(timestampNano[1]);
                    }
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
                                "2022-12-11T00:09:03.622845Z\t2022-12-12T00:00:00.000600Z\t282\n"); // the extra 5
            }
        });
    }

    static {
        LogFactory.getLog(AlterTableAttachPartitionFromSoftLinkLineTest.class);
    }
}
