/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.DatabaseCheckpointStatus;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cutlass.line.LineUdpSender;
import io.questdb.cutlass.line.udp.AbstractLineProtoUdpReceiver;
import io.questdb.cutlass.line.udp.DefaultLineUdpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpReceiver;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LinuxMMLineUdpReceiver;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.mp.WorkerPool;
import io.questdb.network.Net;
import io.questdb.network.NetworkError;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class LinuxLineUdpProtoReceiverTest extends AbstractCairoTest {

    private final static ReceiverFactory GENERIC_FACTORY =
            (configuration, engine, workerPool, localPool, sharedQueryWorkerCount, functionFactoryCache, snapshotAgent) -> new LineUdpReceiver(configuration, engine, workerPool);
    private final static ReceiverFactory LINUX_FACTORY =
            (configuration, engine, workerPool, localPool, sharedQueryWorkerCount, functionFactoryCache, snapshotAgent) -> new LinuxMMLineUdpReceiver(configuration, engine, workerPool);

    @Test
    public void testGenericCannotBindSocket() throws Exception {
        assertCannotBindSocket(GENERIC_FACTORY);
    }

    @Test
    public void testGenericCannotJoin() throws Exception {
        assertCannotJoin(GENERIC_FACTORY);
    }

    @Test
    public void testGenericCannotOpenSocket() throws Exception {
        assertCannotOpenSocket(GENERIC_FACTORY);
    }

    @Test
    public void testGenericCannotSetReceiveBuffer() throws Exception {
        assertCannotSetReceiveBuffer(GENERIC_FACTORY);
    }

    @Test
    public void testGenericFrequentCommit() throws Exception {
        assertFrequentCommit(GENERIC_FACTORY);
    }

    @Test
    public void testGenericSimpleReceive() throws Exception {
        assertReceive(new DefaultLineUdpReceiverConfiguration(), GENERIC_FACTORY);
    }

    @Test
    public void testLinuxCannotBindSocket() throws Exception {
        Assume.assumeTrue(Os.isLinux());
        assertCannotBindSocket(LINUX_FACTORY);
    }

    @Test
    public void testLinuxCannotJoin() throws Exception {
        Assume.assumeTrue(Os.isLinux());
        assertCannotJoin(LINUX_FACTORY);
    }

    @Test
    public void testLinuxCannotOpenSocket() throws Exception {
        Assume.assumeTrue(Os.isLinux());
        assertCannotOpenSocket(LINUX_FACTORY);
    }

    @Test
    public void testLinuxCannotSetReceiveBuffer() throws Exception {
        Assume.assumeTrue(Os.isLinux());
        assertCannotSetReceiveBuffer(LINUX_FACTORY);
    }

    @Test
    public void testLinuxFrequentCommit() throws Exception {
        Assume.assumeTrue(Os.isLinux());
        assertFrequentCommit(LINUX_FACTORY);
    }

    @Test
    public void testLinuxSimpleReceive() throws Exception {
        Assume.assumeTrue(Os.isLinux());
        assertReceive(new DefaultLineUdpReceiverConfiguration(), LINUX_FACTORY);
    }

    private void assertCannotBindSocket(ReceiverFactory factory) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            NetworkFacade nf = new NetworkFacadeImpl() {
                @Override
                public boolean bindUdp(long fd, int ipv4Address, int port) {
                    return false;
                }
            };
            LineUdpReceiverConfiguration receiverCfg = new DefaultLineUdpReceiverConfiguration() {
                @Override
                public NetworkFacade getNetworkFacade() {
                    return nf;
                }
            };
            assertConstructorFail(receiverCfg, factory);
        });
    }

    private void assertCannotJoin(ReceiverFactory factory) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            NetworkFacade nf = new NetworkFacadeImpl() {
                @Override
                public boolean join(long fd, int bindIPv4Address, int groupIPv4Address) {
                    return false;
                }
            };
            LineUdpReceiverConfiguration receiverCfg = new DefaultLineUdpReceiverConfiguration() {
                @Override
                public NetworkFacade getNetworkFacade() {
                    return nf;
                }
            };

            assertConstructorFail(receiverCfg, factory);
        });
    }

    private void assertCannotOpenSocket(ReceiverFactory factory) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            NetworkFacade nf = new NetworkFacadeImpl() {
                @Override
                public long socketUdp() {
                    return -1;
                }
            };
            LineUdpReceiverConfiguration receiverCfg = new DefaultLineUdpReceiverConfiguration() {
                @Override
                public NetworkFacade getNetworkFacade() {
                    return nf;
                }
            };
            assertConstructorFail(receiverCfg, factory);
        });
    }

    private void assertCannotSetReceiveBuffer(ReceiverFactory factory) throws Exception {
        NetworkFacade nf = new NetworkFacadeImpl() {
            @Override
            public int setRcvBuf(long fd, int size) {
                return -1;
            }
        };

        LineUdpReceiverConfiguration configuration = new DefaultLineUdpReceiverConfiguration() {
            @Override
            public NetworkFacade getNetworkFacade() {
                return nf;
            }

            @Override
            public int getReceiveBufferSize() {
                return 2048;
            }
        };
        assertReceive(configuration, factory);
    }

    private void assertConstructorFail(LineUdpReceiverConfiguration receiverCfg, ReceiverFactory factory) {
        try (CairoEngine engine = new CairoEngine(configuration)) {
            try {
                factory.create(receiverCfg, engine, null, true, 0, null, null);
                Assert.fail();
            } catch (NetworkError ignore) {
            }
        }
    }

    private void assertFrequentCommit(ReceiverFactory factory) throws Exception {
        LineUdpReceiverConfiguration configuration = new DefaultLineUdpReceiverConfiguration() {
            @Override
            public int getCommitRate() {
                return 0;
            }
        };
        assertReceive(configuration, factory);
    }

    private void assertReceive(LineUdpReceiverConfiguration receiverCfg, ReceiverFactory factory) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String expected = "colour\tshape\tsize\ttimestamp\n" +
                    "blue\tx square\t3.4\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tx square\t3.4\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tx square\t3.4\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tx square\t3.4\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tx square\t3.4\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tx square\t3.4\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tx square\t3.4\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tx square\t3.4\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tx square\t3.4\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tx square\t3.4\t1970-01-01T00:01:40.000000Z\n";

            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoUdpReceiver receiver = factory.create(receiverCfg, engine, null, false, 0, null, null)) {
                    // create table
                    String tableName = "tab";
                    TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                            .col("colour", ColumnType.SYMBOL)
                            .col("shape", ColumnType.SYMBOL)
                            .col("size", ColumnType.DOUBLE)
                            .timestamp();
                    TestUtils.createTable(engine, model);

                    // warm writer up
                    try (TableWriter w = getWriter(engine, tableName)) {
                        w.warmUp();
                    }

                    receiver.start();

                    try (LineUdpSender sender = new LineUdpSender(NetworkFacadeImpl.INSTANCE, 0, Net.parseIPv4("127.0.0.1"), receiverCfg.getPort(), 1400, 1)) {
                        for (int i = 0; i < 10; i++) {
                            sender.metric(tableName).tag("colour", "blue").tag("shape", "x square").field("size", 3.4).$(100000000000L);
                        }
                        sender.flush();
                    }

                    try (TableReader reader = newOffPoolReader(configuration, tableName, engine)) {
                        int count = 1000000;
                        while (true) {
                            if (count-- > 0 && reader.size() < 10) {
                                reader.reload();
                                Os.pause();
                            } else {
                                break;
                            }
                        }

                        Assert.assertTrue(count > 0);
                        receiver.close();

                        TestUtils.assertReader(expected, reader, sink);
                    }
                }
            }
        });
    }

    @FunctionalInterface
    private interface ReceiverFactory {
        AbstractLineProtoUdpReceiver create(
                io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration configuration,
                CairoEngine engine,
                WorkerPool workerPool,
                boolean isWorkerPoolLocal,
                int sharedQueryWorkerCount,
                @Nullable FunctionFactoryCache functionFactoryCache,
                @Nullable DatabaseCheckpointStatus snapshotAgent
        );
    }
}
