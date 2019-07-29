/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cutlass.line.udp;

import com.questdb.cairo.*;
import com.questdb.cairo.security.AllowAllCairoSecurityContext;
import com.questdb.mp.Job;
import com.questdb.mp.SOCountDownLatch;
import com.questdb.mp.Worker;
import com.questdb.network.Net;
import com.questdb.network.NetworkFacade;
import com.questdb.network.NetworkFacadeImpl;
import com.questdb.std.Misc;
import com.questdb.std.ObjHashSet;
import com.questdb.std.Os;
import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.locks.LockSupport;

public class LinuxLineProtoReceiverTest extends AbstractCairoTest {

    private final static ReceiverFactory LINUX_FACTORY = LinuxLineProtoReceiver::new;
    private final static ReceiverFactory GENERIC_FACTORY = GenericLineProtoReceiver::new;

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
        assertReceive(new TestLineUdpReceiverConfiguration(), GENERIC_FACTORY);
    }

    @Test
    public void testLinuxCannotBindSocket() throws Exception {
        if (Os.type != Os.LINUX) {
            return;
        }
        assertCannotBindSocket(LINUX_FACTORY);
    }

    @Test
    public void testLinuxCannotJoin() throws Exception {
        if (Os.type != Os.LINUX) {
            return;
        }
        assertCannotJoin(LINUX_FACTORY);
    }

    @Test
    public void testLinuxCannotOpenSocket() throws Exception {
        if (Os.type != Os.LINUX) {
            return;
        }
        assertCannotOpenSocket(LINUX_FACTORY);
    }

    @Test
    public void testLinuxCannotSetReceiveBuffer() throws Exception {
        if (Os.type != Os.LINUX) {
            return;
        }
        assertCannotSetReceiveBuffer(LINUX_FACTORY);
    }

    @Test
    public void testLinuxFrequentCommit() throws Exception {
        if (Os.type != Os.LINUX) {
            return;
        }
        assertFrequentCommit(LINUX_FACTORY);
    }

    @Test
    public void testLinuxSimpleReceive() throws Exception {
        if (Os.type != Os.LINUX) {
            return;
        }
        assertReceive(new TestLineUdpReceiverConfiguration(), LINUX_FACTORY);
    }

    private void assertCannotBindSocket(ReceiverFactory factory) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            NetworkFacade nf = new NetworkFacadeImpl() {
                @Override
                public boolean bindUdp(long fd, int port) {
                    return false;
                }
            };
            LineUdpReceiverConfiguration receiverCfg = new TestLineUdpReceiverConfiguration() {
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
            LineUdpReceiverConfiguration receiverCfg = new TestLineUdpReceiverConfiguration() {
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
            LineUdpReceiverConfiguration receiverCfg = new TestLineUdpReceiverConfiguration() {
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

        LineUdpReceiverConfiguration configuration = new TestLineUdpReceiverConfiguration() {
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
        try (CairoEngine engine = new CairoEngine(new DefaultCairoConfiguration(root), null)) {
            try {
                factory.createReceiver(receiverCfg, engine, AllowAllCairoSecurityContext.INSTANCE);
                Assert.fail();
            } catch (CairoException ignore) {
            }
        }
    }

    private void assertFrequentCommit(ReceiverFactory factory) throws Exception {
        LineUdpReceiverConfiguration configuration = new TestLineUdpReceiverConfiguration() {
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
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000000Z\n";

            try (CairoEngine engine = new CairoEngine(new DefaultCairoConfiguration(root), null)) {

                Job receiver = factory.createReceiver(receiverCfg, engine, AllowAllCairoSecurityContext.INSTANCE);

                try {

                    SOCountDownLatch workerHaltLatch = new SOCountDownLatch(1);

                    // create table

                    try (TableModel model = new TableModel(configuration, "tab", PartitionBy.NONE)
                            .col("colour", ColumnType.SYMBOL)
                            .col("shape", ColumnType.SYMBOL)
                            .col("size", ColumnType.DOUBLE)
                            .timestamp()) {
                        CairoTestUtils.create(model);
                    }

                    // warm writer up
                    try (TableWriter w = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "tab")) {
                        w.warmUp();
                    }

                    ObjHashSet<Job> jobs = new ObjHashSet<>();
                    jobs.add(receiver);
                    Worker worker = new Worker(jobs, workerHaltLatch);
                    worker.start();

                    try (LineProtoSender sender = new LineProtoSender(NetworkFacadeImpl.INSTANCE, 0, receiverCfg.getBindIPv4Address(), receiverCfg.getPort(), 1400)) {
                        for (int i = 0; i < 10; i++) {
                            sender.metric("tab").tag("colour", "blue").tag("shape", "square").field("size", 3.4, 4).$(100000000);
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
                        worker.halt();
                        workerHaltLatch.await();

                        StringSink sink = new StringSink();
                        RecordCursorPrinter printer = new RecordCursorPrinter(sink);
                        printer.print(reader.getCursor(), reader.getMetadata(), true);
                        TestUtils.assertEquals(expected, sink);
                    }
                } finally {
                    Misc.free(receiver);
                }
            }
        });
    }

    private static class TestLineUdpReceiverConfiguration implements LineUdpReceiverConfiguration {

        @Override
        public int getBindIPv4Address() {
            return Net.parseIPv4("127.0.0.1");
        }

        @Override
        public int getCommitRate() {
            return 1024 * 1024;
        }

        @Override
        public int getGroupIPv4Address() {
            return Net.parseIPv4("224.1.1.1");
        }

        @Override
        public int getMsgBufferSize() {
            return 2048;
        }

        @Override
        public int getMsgCount() {
            return 10000;
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return NetworkFacadeImpl.INSTANCE;
        }

        @Override
        public int getPort() {
            return 4567;
        }

        @Override
        public int getReceiveBufferSize() {
            return -1;
        }
    }
}