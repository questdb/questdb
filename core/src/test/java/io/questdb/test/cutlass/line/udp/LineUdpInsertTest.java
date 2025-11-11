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

package io.questdb.test.cutlass.line.udp;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cutlass.line.AbstractLineSender;
import io.questdb.cutlass.line.LineUdpSender;
import io.questdb.cutlass.line.udp.AbstractLineProtoUdpReceiver;
import io.questdb.cutlass.line.udp.DefaultLineUdpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpReceiver;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LinuxMMLineUdpReceiver;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public abstract class LineUdpInsertTest extends AbstractCairoTest {

    protected static final int LOCALHOST = Net.parseIPv4("127.0.0.1");
    protected static boolean useLegacyString = true;
    protected static final LineUdpReceiverConfiguration RCVR_CONF = new DefaultLineUdpReceiverConfiguration() {
        @Override
        public boolean isUseLegacyStringDefault() {
            return useLegacyString;
        }
    };

    protected static final int PORT = RCVR_CONF.getPort();

    protected static void assertReader(String tableName, String expected) {
        refreshTablesInBaseEngine();
        assertReader(tableName, expected, (String[]) null);
    }

    protected static void assertReader(String tableName, String expected, String... expectedExtraStringColumns) {
        refreshTablesInBaseEngine();
        try (TableReader reader = newOffPoolReader(new DefaultTestCairoConfiguration(root), tableName)) {
            TestUtils.assertReader(expected, reader, sink);
            if (expectedExtraStringColumns != null) {
                TableReaderMetadata meta = reader.getMetadata();
                Assert.assertEquals(2 + expectedExtraStringColumns.length, meta.getColumnCount());
                for (String colName : expectedExtraStringColumns) {
                    Assert.assertEquals(ColumnType.STRING, meta.getColumnType(colName));
                }
            }
        }
    }

    protected static void assertType(String tableName,
                                     String targetColumnName,
                                     int columnType,
                                     String expected,
                                     Consumer<AbstractLineSender> senderConsumer,
                                     String... expectedExtraStringColumns) throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                final SOCountDownLatch waitForData = new SOCountDownLatch(1);
                engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                    if (event == PoolListener.EV_RETURN && name.getTableName().equals(tableName)
                            && name.equals(engine.verifyTableName(tableName))) {
                        waitForData.countDown();
                    }
                });
                try (AbstractLineProtoUdpReceiver receiver = createLineProtoReceiver(engine)) {
                    if (columnType != ColumnType.UNDEFINED) {
                        TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE);
                        TestUtils.createTable(engine, model.col(targetColumnName, columnType).timestamp());
                    }
                    receiver.start();
                    try (AbstractLineSender sender = createLineProtoSender()) {
                        sender.disableValidation();
                        senderConsumer.accept(sender);
                        sender.flush();
                    }
                    Os.sleep(250L); // allow reader to hit the readout
                }

                if (!waitForData.await(TimeUnit.SECONDS.toNanos(30L))) {
                    Assert.fail();
                }

                assertReader(tableName, expected, expectedExtraStringColumns);
            }
        });
    }

    protected static AbstractLineProtoUdpReceiver createLineProtoReceiver(CairoEngine engine) {
        AbstractLineProtoUdpReceiver lpr;
        if (Os.isLinux()) {
            lpr = new LinuxMMLineUdpReceiver(RCVR_CONF, engine, null);
        } else {
            lpr = new LineUdpReceiver(RCVR_CONF, engine, null);
        }
        return lpr;
    }

    protected static AbstractLineSender createLineProtoSender() {
        return new LineUdpSender(NetworkFacadeImpl.INSTANCE, 0, LOCALHOST, PORT, 200, 1);
    }
}
