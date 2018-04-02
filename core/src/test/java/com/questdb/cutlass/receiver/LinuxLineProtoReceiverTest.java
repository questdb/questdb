/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.cutlass.receiver;

import com.questdb.cairo.*;
import com.questdb.cairo.pool.WriterPool;
import com.questdb.common.ColumnType;
import com.questdb.common.PartitionBy;
import com.questdb.cutlass.client.LineProtoSender;
import com.questdb.mp.Job;
import com.questdb.mp.Worker;
import com.questdb.std.*;
import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
        assertReceive(new TestReceiverConfiguration(), GENERIC_FACTORY);
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
        assertReceive(new TestReceiverConfiguration(), LINUX_FACTORY);
    }

    private void assertCannotBindSocket(ReceiverFactory factory) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            NetFacade nf = new NetFacadeImpl() {
                @Override
                public boolean bindUdp(long fd, CharSequence IPv4Address, int port) {
                    return false;
                }
            };
            ReceiverConfiguration receiverCfg = new TestReceiverConfiguration() {
                @Override
                public NetFacade getNetFacade() {
                    return nf;
                }
            };
            assertConstructorFail(receiverCfg, factory);
        });
    }

    private void assertCannotJoin(ReceiverFactory factory) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            NetFacade nf = new NetFacadeImpl() {
                @Override
                public boolean join(long fd, CharSequence bindIPv4Address, CharSequence groupIPv4Address) {
                    return false;
                }

            };
            ReceiverConfiguration receiverCfg = new TestReceiverConfiguration() {
                @Override
                public NetFacade getNetFacade() {
                    return nf;
                }
            };

            assertConstructorFail(receiverCfg, factory);
        });
    }

    private void assertCannotOpenSocket(ReceiverFactory factory) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            NetFacade nf = new NetFacadeImpl() {
                @Override
                public long socketUdp() {
                    return -1;
                }
            };
            ReceiverConfiguration receiverCfg = new TestReceiverConfiguration() {
                @Override
                public NetFacade getNetFacade() {
                    return nf;
                }
            };
            assertConstructorFail(receiverCfg, factory);
        });
    }

    private void assertCannotSetReceiveBuffer(ReceiverFactory factory) throws Exception {
        NetFacade nf = new NetFacadeImpl() {
            @Override
            public int setRcvBuf(long fd, int size) {
                return -1;
            }
        };

        ReceiverConfiguration configuration = new TestReceiverConfiguration() {
            @Override
            public NetFacade getNetFacade() {
                return nf;
            }

            @Override
            public int getReceiveBufferSize() {
                return 2048;
            }
        };
        assertReceive(configuration, factory);
    }

    private void assertConstructorFail(ReceiverConfiguration receiverCfg, ReceiverFactory factory) {
        CairoConfiguration cairoCfg = new DefaultCairoConfiguration(root);
        try (WriterPool pool = new WriterPool(cairoCfg)) {
            try {
                factory.createReceiver(receiverCfg, cairoCfg, pool);
                Assert.fail();
            } catch (CairoException ignore) {
            }
        }
    }

    private void assertFrequentCommit(ReceiverFactory factory) throws Exception {
        ReceiverConfiguration configuration = new TestReceiverConfiguration() {
            @Override
            public int getCommitRate() {
                return 0;
            }
        };
        assertReceive(configuration, factory);
    }

    private void assertReceive(ReceiverConfiguration receiverCfg, ReceiverFactory factory) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String expected = "colour\tshape\tsize\ttimestamp\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000Z\n" +
                    "blue\tsquare\t3.400000000000\t1970-01-01T00:01:40.000Z\n";

            CairoConfiguration cairoCfg = new DefaultCairoConfiguration(root);
            try (WriterPool pool = new WriterPool(cairoCfg)) {

                Job receiver = factory.createReceiver(receiverCfg, cairoCfg, pool);

                try {

                    CountDownLatch workerHaltLatch = new CountDownLatch(1);

                    // create table

                    try (TableModel model = new TableModel(configuration, "tab", PartitionBy.NONE)
                            .col("colour", ColumnType.SYMBOL)
                            .col("shape", ColumnType.SYMBOL)
                            .col("size", ColumnType.DOUBLE)
                            .timestamp()) {
                        CairoTestUtils.create(model);
                    }

                    // warm writer up
                    try (TableWriter w = pool.get("tab")) {
                        w.warmUp();
                    }

                    ObjHashSet<Job> jobs = new ObjHashSet<>();
                    jobs.add(receiver);
                    Worker worker = new Worker(jobs, workerHaltLatch);
                    worker.start();

                    try (LineProtoSender sender = new LineProtoSender("127.0.0.1", receiverCfg.getPort(), 1400)) {
                        for (int i = 0; i < 10; i++) {
                            sender.metric("tab").tag("colour", "blue").tag("shape", "square").field("size", 3.4, 4).$(100000000);
                        }
                        sender.flush();
                    }

                    try (TableReader reader = new TableReader(cairoCfg, "tab")) {
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
                        Assert.assertTrue(workerHaltLatch.await(3, TimeUnit.SECONDS));

                        StringSink sink = new StringSink();
                        RecordCursorPrinter printer = new RecordCursorPrinter(sink);
                        printer.print(reader.getCursor(), true, reader.getMetadata());
                        TestUtils.assertEquals(expected, sink);
                    }
                } finally {
                    Misc.free(receiver);
                }
            }
        });
    }

    private static class TestReceiverConfiguration implements ReceiverConfiguration {

        @Override
        public int getCommitRate() {
            return 1024 * 1024;
        }

        @Override
        public CharSequence getBindIPv4Address() {
            return "127.0.0.1";
        }

        @Override
        public CharSequence getGroupIPv4Address() {
            return "234.5.6.7";
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
        public int getPort() {
            return 4567;
        }

        @Override
        public int getReceiveBufferSize() {
            return -1;
        }

        @Override
        public NetFacade getNetFacade() {
            return NetFacadeImpl.INSTANCE;
        }
    }
}