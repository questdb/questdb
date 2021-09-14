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

package io.questdb.cutlass.line.udp;

import io.questdb.WorkerPoolAwareConfiguration;
import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.line.LineProtoSender;
import io.questdb.network.Net;
import io.questdb.network.NetworkError;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Os;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.locks.LockSupport;

public class LinuxLineProtoReceiverTest extends AbstractCairoTest {

    private final static ReceiverFactory LINUX_FACTORY =
            (configuration, engine, workerPool, localPool, workScheduler1, functionFactoryCache, metrics) -> new LinuxMMLineProtoReceiver(configuration, engine, workerPool);

    private final static ReceiverFactory GENERIC_FACTORY =
            (configuration, engine, workerPool, localPool, workScheduler1, functionFactoryCache, metrics) -> new LineProtoReceiver(configuration, engine, workerPool);

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
        if (Os.type != Os.LINUX_AMD64) {
            return;
        }
        assertCannotBindSocket(LINUX_FACTORY);
    }

    @Test
    public void testLinuxCannotJoin() throws Exception {
        if (Os.type != Os.LINUX_AMD64) {
            return;
        }
        assertCannotJoin(LINUX_FACTORY);
    }

    @Test
    public void testLinuxCannotOpenSocket() throws Exception {
        if (Os.type != Os.LINUX_AMD64) {
            return;
        }
        assertCannotOpenSocket(LINUX_FACTORY);
    }

    @Test
    public void testLinuxCannotSetReceiveBuffer() throws Exception {
        if (Os.type != Os.LINUX_AMD64) {
            return;
        }
        assertCannotSetReceiveBuffer(LINUX_FACTORY);
    }

    @Test
    public void testLinuxFrequentCommit() throws Exception {
        if (Os.type != Os.LINUX_AMD64) {
            return;
        }
        assertFrequentCommit(LINUX_FACTORY);
    }

    @Test
    public void testLinuxSimpleReceive() throws Exception {
        if (Os.type != Os.LINUX_AMD64) {
            return;
        }
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
                factory.create(receiverCfg, engine, null, true, null, null, metrics);
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
                    "blue\tx square\t3.4000000000000004\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tx square\t3.4000000000000004\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tx square\t3.4000000000000004\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tx square\t3.4000000000000004\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tx square\t3.4000000000000004\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tx square\t3.4000000000000004\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tx square\t3.4000000000000004\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tx square\t3.4000000000000004\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tx square\t3.4000000000000004\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tx square\t3.4000000000000004\t1970-01-01T00:01:40.000000Z\n";

            try (CairoEngine engine = new CairoEngine(configuration)) {


                try (AbstractLineProtoReceiver receiver = factory.create(receiverCfg, engine, null, false, null, null, metrics)) {

                    // create table

                    try (TableModel model = new TableModel(configuration, "tab", PartitionBy.NONE)
                            .col("colour", ColumnType.SYMBOL)
                            .col("shape", ColumnType.SYMBOL)
                            .col("size", ColumnType.DOUBLE)
                            .timestamp()) {
                        CairoTestUtils.create(model);
                    }

                    // warm writer up
                    try (TableWriter w = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "tab", "testing")) {
                        w.warmUp();
                    }

                    receiver.start();

                    try (LineProtoSender sender = new LineProtoSender(NetworkFacadeImpl.INSTANCE, 0, Net.parseIPv4("127.0.0.1"), receiverCfg.getPort(), 1400, 1)) {
                        for (int i = 0; i < 10; i++) {
                            sender.metric("tab").tag("colour", "blue").tag("shape", "x square").field("size", 3.4).$(100000000000L);
                        }
                        sender.flush();
                    }

                    try (TableReader reader = new TableReader(new DefaultCairoConfiguration(root), "tab")) {
                        int count = 1000000;
                        while (true) {
                            if (count-- > 0 && reader.size() < 10) {
                                reader.reload();
                                LockSupport.parkNanos(1);
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

    private interface ReceiverFactory extends WorkerPoolAwareConfiguration.ServerFactory<AbstractLineProtoReceiver, LineUdpReceiverConfiguration> {
    }
}