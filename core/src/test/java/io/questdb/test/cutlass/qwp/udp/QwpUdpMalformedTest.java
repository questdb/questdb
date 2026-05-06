/*+*****************************************************************************
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

package io.questdb.test.cutlass.qwp.udp;

import io.questdb.cairo.CairoEngine;
import io.questdb.client.cutlass.qwp.client.QwpUdpSender;
import io.questdb.client.network.NetworkFacadeImpl;
import io.questdb.cutlass.qwp.server.DefaultQwpUdpReceiverConfiguration;
import io.questdb.cutlass.qwp.server.LinuxMMQwpUdpReceiver;
import io.questdb.cutlass.qwp.server.QwpUdpReceiver;
import io.questdb.cutlass.qwp.server.QwpUdpReceiverConfiguration;
import io.questdb.network.Net;
import io.questdb.network.NetworkError;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
public class QwpUdpMalformedTest extends AbstractCairoTest {

    private static final int LOCALHOST = Net.parseIPv4("127.0.0.1");
    private static final int PORT = 19_002;
    private static final QwpUdpReceiverConfiguration NO_AUTO_CREATE_CONF = new DefaultQwpUdpReceiverConfiguration() {
        @Override
        public int getMaxUncommittedDatagrams() {
            return 10;
        }

        @Override
        public int getPort() {
            return PORT;
        }

        @Override
        public boolean isAutoCreateNewTables() {
            return false;
        }

        @Override
        public boolean isOwnThread() {
            return false;
        }
    };
    private static final QwpUdpReceiverConfiguration RCVR_CONF = new DefaultQwpUdpReceiverConfiguration() {
        @Override
        public int getMaxUncommittedDatagrams() {
            return 10;
        }

        @Override
        public int getPort() {
            return PORT;
        }

        @Override
        public boolean isOwnThread() {
            return false;
        }
    };
    private static final int VALID_MAGIC = 0x31505751;
    private final ReceiverFactory receiverFactory;

    @SuppressWarnings("unused")
    public QwpUdpMalformedTest(String name, ReceiverFactory factory) {
        this.receiverFactory = factory;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();
        params.add(new Object[]{"base", (ReceiverFactory) QwpUdpReceiver::new});
        if (Os.isLinux()) {
            params.add(new Object[]{"recvmmsg", (ReceiverFactory) LinuxMMQwpUdpReceiver::new});
        }
        return params;
    }

    @Test
    public void testAutoCreateDisabledSkipsUnknownTable() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(NO_AUTO_CREATE_CONF, engine)) {
                sendValidRow("nonexistent_table", 1L, 1_000_000L);
                drainReceiver(receiver);

                // Datagram was received and processed, but no table was created
                Assert.assertEquals(1, receiver.getProcessedCount());
                Assert.assertEquals(0, receiver.getTotalDroppedCount());
            }
        });
    }

    @Test
    public void testBindFailureThrowsNetworkError() throws Exception {
        QwpUdpReceiverConfiguration nonLocalBind = new DefaultQwpUdpReceiverConfiguration() {
            @Override
            public int getBindIPv4Address() {
                return Net.parseIPv4("8.8.8.8");
            }

            @Override
            public int getPort() {
                return PORT;
            }

            @Override
            public boolean isOwnThread() {
                return false;
            }
        };

        assertMemoryLeak(() -> {
            try {
                receiverFactory.create(nonLocalBind, engine).close();
                Assert.fail("expected NetworkError for non-local bind address");
            } catch (NetworkError expected) {
                // Constructor cleaned up socket and re-threw
            }
        });
    }

    @Test
    public void testContinuesWhenReceiveBufferSizeFails() throws Exception {
        QwpUdpReceiverConfiguration rcvBufFail = new DefaultQwpUdpReceiverConfiguration() {
            @Override
            public int getMaxUncommittedDatagrams() {
                return 10;
            }

            @Override
            public io.questdb.network.NetworkFacade getNetworkFacade() {
                return new io.questdb.network.NetworkFacadeImpl() {
                    @Override
                    public int setRcvBuf(long fd, int size) {
                        return -1;
                    }
                };
            }

            @Override
            public int getPort() {
                return PORT;
            }

            @Override
            public int getReceiveBufferSize() {
                return 4_194_304;
            }

            @Override
            public boolean isOwnThread() {
                return false;
            }
        };

        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(rcvBufFail, engine)) {
                sendValidRow("rcvbuf_test", 1L, 1_000_000L);
                drainReceiver(receiver);

                Assert.assertEquals(1, receiver.getProcessedCount());
                Assert.assertEquals(0, receiver.getTotalDroppedCount());
            }

            drainWalQueue();
            assertSql(
                    "count\n1\n",
                    "SELECT count() FROM rcvbuf_test"
            );
        });
    }

    @Test
    public void testCreateFailsWhenJoinGroupFails() throws Exception {
        QwpUdpReceiverConfiguration multicastJoinFail = new DefaultQwpUdpReceiverConfiguration() {
            @Override
            public io.questdb.network.NetworkFacade getNetworkFacade() {
                return new io.questdb.network.NetworkFacadeImpl() {
                    @Override
                    public boolean join(long fd, int bindIPv4, int groupIPv4) {
                        return false;
                    }
                };
            }

            @Override
            public int getPort() {
                return PORT;
            }

            @Override
            public boolean isOwnThread() {
                return false;
            }

            @Override
            public boolean isUnicast() {
                return false;
            }
        };

        assertMemoryLeak(() -> {
            try {
                receiverFactory.create(multicastJoinFail, engine).close();
                Assert.fail("expected NetworkError for multicast join failure");
            } catch (NetworkError expected) {
                Assert.assertTrue(expected.getMessage().contains("cannot join group"));
            }
        });
    }

    @Test
    public void testCreateFailsWhenSocketCannotOpen() throws Exception {
        QwpUdpReceiverConfiguration brokenSocket = new DefaultQwpUdpReceiverConfiguration() {
            @Override
            public io.questdb.network.NetworkFacade getNetworkFacade() {
                return new io.questdb.network.NetworkFacadeImpl() {
                    @Override
                    public long socketUdp() {
                        return -1;
                    }
                };
            }

            @Override
            public int getPort() {
                return PORT;
            }

            @Override
            public boolean isOwnThread() {
                return false;
            }
        };

        assertMemoryLeak(() -> {
            try {
                receiverFactory.create(brokenSocket, engine).close();
                Assert.fail("expected NetworkError for failed socket creation");
            } catch (NetworkError expected) {
                // socketUdp() returned -1
            }
        });
    }

    @Test
    public void testDoubleCloseIsNoOp() throws Exception {
        assertMemoryLeak(() -> {
            QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine);
            receiver.close();
            receiver.close(); // no-op since fd is already -1
        });
    }

    @Test
    public void testDuplicateDatagramBothIngested() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                // identical data sent twice
                sendValidRow("dup_test", 1L, 1_000_000L);
                sendValidRow("dup_test", 1L, 1_000_000L);
                drainReceiver(receiver);

                Assert.assertEquals(0, receiver.getTotalDroppedCount());
            }

            drainWalQueue();
            assertSql(
                    "count\n2\n",
                    "SELECT count() FROM dup_test"
            );
        });
    }

    @Test
    public void testInvalidFlagsBothCompressions() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                // flags = 0x03 means both LZ4 (0x01) and Zstd (0x02) set
                byte[] header = createHeader(
                        0x34504C49, // "ILP4"
                        (byte) 1, (byte) 0x03, 0, 0L
                );
                sendRawBytes(header);
                sendValidRow("bad_flags", 1L, 1_000_000L);
                drainReceiver(receiver);

                // header validator throws INVALID_MAGIC for conflicting flags
                Assert.assertEquals(1, receiver.getDroppedBadMagicCount());
            }

            drainWalQueue();
            assertSql(
                    "count\n1\n",
                    "SELECT count() FROM bad_flags"
            );
        });
    }

    @Test
    public void testInvalidMagicAllZeros() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                sendRawBytes(new byte[12]);
                sendValidRow("magic_zeros", 1L, 1_000_000L);
                drainReceiver(receiver);

                Assert.assertEquals(1, receiver.getDroppedBadMagicCount());
            }

            drainWalQueue();
            assertSql(
                    "count\n1\n",
                    "SELECT count() FROM magic_zeros"
            );
        });
    }

    @Test
    public void testInvalidMagicILP3() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                byte[] header = createHeader(
                        0x33504C49, // "ILP3" in little-endian
                        (byte) 1, (byte) 0, 0, 0L
                );
                sendRawBytes(header);
                sendValidRow("magic_ilp3", 1L, 1_000_000L);
                drainReceiver(receiver);

                Assert.assertEquals(1, receiver.getDroppedBadMagicCount());
            }

            drainWalQueue();
            assertSql(
                    "count\n1\n",
                    "SELECT count() FROM magic_ilp3"
            );
        });
    }

    @Test
    public void testMultipleMalformedThenValid() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                // too short
                sendRawBytes(new byte[]{0, 0, 0, 0});
                // bad magic
                sendRawBytes(new byte[12]);
                // bad version
                byte[] badVersion = createHeader(
                        VALID_MAGIC,
                        (byte) 2, (byte) 0, 0, 0L
                );
                sendRawBytes(badVersion);
                // valid row
                sendValidRow("multi_bad", 1L, 1_000_000L);
                drainReceiver(receiver);

                Assert.assertEquals(1, receiver.getDroppedTooShortCount());
                Assert.assertEquals(1, receiver.getDroppedBadMagicCount());
                Assert.assertEquals(1, receiver.getDroppedBadVersionCount());
            }

            drainWalQueue();
            assertSql(
                    "count\n1\n",
                    "SELECT count() FROM multi_bad"
            );
        });
    }

    @Test
    public void testMalformedDatagramDoesNotCountTowardsMaxUncommittedDatagrams() throws Exception {
        QwpUdpReceiverConfiguration conf = new DefaultQwpUdpReceiverConfiguration() {
            @Override
            public long getCommitInterval() {
                return TimeUnit.SECONDS.toMillis(5);
            }

            @Override
            public int getMaxUncommittedDatagrams() {
                return 2;
            }

            @Override
            public int getPort() {
                return PORT;
            }

            @Override
            public boolean isOwnThread() {
                return false;
            }
        };

        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(conf, engine)) {
                sendRawBytes(new byte[12]);
                sendValidRow("noise_then_valid", 1L, 1_000_000L);
                drainReceiver(receiver);

                Assert.assertEquals(1, receiver.getDroppedBadMagicCount());

                drainWalQueue();
                assertSql("count\n0\n", "SELECT count() FROM noise_then_valid");
            }

            drainWalQueue();
            assertSql("count\n1\n", "SELECT count() FROM noise_then_valid");
        });
    }

    @Test
    public void testOutOfOrderTimestampsBothIngested() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                sendValidRow("ooo_test", 1L, 2_000_000L);
                sendValidRow("ooo_test", 2L, 1_000_000L);
                drainReceiver(receiver);

                Assert.assertEquals(0, receiver.getTotalDroppedCount());
            }

            drainWalQueue();
            assertSql(
                    "count\n2\n",
                    "SELECT count() FROM ooo_test"
            );
        });
    }

    @Test
    public void testPayloadLengthExceedsDatagram() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                byte[] header = createHeader(
                        VALID_MAGIC,
                        (byte) 1, (byte) 0, 1, 1000L
                );
                sendRawBytes(header);
                sendValidRow("trunc_test", 1L, 1_000_000L);
                drainReceiver(receiver);

                Assert.assertEquals(1, receiver.getDroppedTruncatedCount());
            }

            drainWalQueue();
            assertSql(
                    "count\n1\n",
                    "SELECT count() FROM trunc_test"
            );
        });
    }

    @Test
    public void testPayloadTooLarge() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                // payloadLength = 0x7FFFFFFF (>16MB default max) triggers PAYLOAD_TOO_LARGE
                byte[] header = createHeader(
                        VALID_MAGIC,
                        (byte) 1, (byte) 0, 1, 0x7FFFFFFFL
                );
                sendRawBytes(header);
                sendValidRow("too_large", 1L, 1_000_000L);
                drainReceiver(receiver);

                // PAYLOAD_TOO_LARGE hits the default branch -> droppedParseErrorCount
                Assert.assertEquals(1, receiver.getDroppedParseErrorCount());
                Assert.assertEquals(0, receiver.getDroppedTruncatedCount());
            }

            drainWalQueue();
            assertSql(
                    "count\n1\n",
                    "SELECT count() FROM too_large"
            );
        });
    }

    @Test
    public void testRandomBytes() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                Random rnd = new Random(42);
                byte[] garbage = new byte[256];
                rnd.nextBytes(garbage);
                sendRawBytes(garbage);
                sendValidRow("random_test", 1L, 1_000_000L);
                drainReceiver(receiver);

                Assert.assertEquals(1, receiver.getTotalDroppedCount());
            }

            drainWalQueue();
            assertSql(
                    "count\n1\n",
                    "SELECT count() FROM random_test"
            );
        });
    }

    @Test
    public void testTooShortDatagram() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                sendRawBytes(new byte[]{0, 0, 0, 0});
                sendValidRow("short_test", 1L, 1_000_000L);
                drainReceiver(receiver);

                Assert.assertEquals(1, receiver.getDroppedTooShortCount());
            }

            drainWalQueue();
            assertSql(
                    "count\n1\n",
                    "SELECT count() FROM short_test"
            );
        });
    }

    @Test
    public void testTooShortElevenBytes() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                sendRawBytes(new byte[11]);
                sendValidRow("short11", 1L, 1_000_000L);
                drainReceiver(receiver);

                Assert.assertEquals(1, receiver.getDroppedTooShortCount());
            }

            drainWalQueue();
            assertSql(
                    "count\n1\n",
                    "SELECT count() FROM short11"
            );
        });
    }

    @Test
    public void testTruncatedColumnData() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                // Build a table block with valid schema but malformed SYMBOL column data.
                // The SYMBOL column data starts with a dictionary count varint.
                // We fill it with 0x80 continuation bytes that never terminate,
                // causing a varint overflow during cursor initialization.
                byte[] schema = new byte[]{
                        1, 'x',             // table name: varint(1) + "x"
                        1,                   // row count = 1
                        1,                   // column count = 1
                        0x00,                // schema mode FULL
                        1, 's',              // column name: varint(1) + "s"
                        0x09,                // TYPE_SYMBOL (not nullable)
                };
                // 10 varint continuation bytes (0x80) with no terminator -> varint overflow
                byte[] badData = new byte[]{
                        (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
                        (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80
                };
                int payloadLen = schema.length + badData.length;
                byte[] header = createHeader(
                        0x31505751, // "ILP4"
                        (byte) 1, (byte) 0, 1, payloadLen
                );
                byte[] datagram = new byte[header.length + payloadLen];
                System.arraycopy(header, 0, datagram, 0, header.length);
                System.arraycopy(schema, 0, datagram, header.length, schema.length);
                System.arraycopy(badData, 0, datagram, header.length + schema.length, badData.length);
                sendRawBytes(datagram);
                sendValidRow("trunc_col", 1L, 1_000_000L);
                drainReceiver(receiver);

                Assert.assertEquals(1, receiver.getDroppedParseErrorCount());
            }

            drainWalQueue();
            assertSql(
                    "count\n1\n",
                    "SELECT count() FROM trunc_col"
            );
        });
    }

    @Test
    public void testTruncatedTableName() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                byte[] header = createHeader(
                        VALID_MAGIC,
                        (byte) 1, (byte) 0, 1, 2L
                );
                byte[] datagram = new byte[header.length + 2];
                System.arraycopy(header, 0, datagram, 0, header.length);
                datagram[header.length] = (byte) 0xFF;
                datagram[header.length + 1] = (byte) 0xFF;
                sendRawBytes(datagram);
                sendValidRow("trunc_name", 1L, 1_000_000L);
                drainReceiver(receiver);

                Assert.assertEquals(1, receiver.getDroppedParseErrorCount());
            }

            drainWalQueue();
            assertSql(
                    "count\n1\n",
                    "SELECT count() FROM trunc_name"
            );
        });
    }

    @Test
    public void testWrongVersion() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                byte[] header = createHeader(
                        VALID_MAGIC,
                        (byte) 2, (byte) 0, 0, 0L
                );
                sendRawBytes(header);
                sendValidRow("version_test", 1L, 1_000_000L);
                drainReceiver(receiver);

                Assert.assertEquals(1, receiver.getDroppedBadVersionCount());
            }

            drainWalQueue();
            assertSql(
                    "count\n1\n",
                    "SELECT count() FROM version_test"
            );
        });
    }

    @Test
    public void testZeroPayloadWithNonZeroTableCount() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(RCVR_CONF, engine)) {
                byte[] header = createHeader(
                        VALID_MAGIC,
                        (byte) 1, (byte) 0, 1, 0L
                );
                sendRawBytes(header);
                sendValidRow("zero_payload", 1L, 1_000_000L);
                drainReceiver(receiver);

                Assert.assertEquals(1, receiver.getDroppedParseErrorCount());
            }

            drainWalQueue();
            assertSql(
                    "count\n1\n",
                    "SELECT count() FROM zero_payload"
            );
        });
    }

    private static byte[] createHeader(int magic, byte version, byte flags, int tableCount, long payloadLength) {
        byte[] buf = new byte[12];
        // magic (little-endian)
        buf[0] = (byte) (magic & 0xFF);
        buf[1] = (byte) ((magic >> 8) & 0xFF);
        buf[2] = (byte) ((magic >> 16) & 0xFF);
        buf[3] = (byte) ((magic >> 24) & 0xFF);
        // version
        buf[4] = version;
        // flags
        buf[5] = flags;
        // table count (little-endian uint16)
        buf[6] = (byte) (tableCount & 0xFF);
        buf[7] = (byte) ((tableCount >> 8) & 0xFF);
        // payload length (little-endian uint32)
        buf[8] = (byte) (payloadLength & 0xFF);
        buf[9] = (byte) ((payloadLength >> 8) & 0xFF);
        buf[10] = (byte) ((payloadLength >> 16) & 0xFF);
        buf[11] = (byte) ((payloadLength >> 24) & 0xFF);
        return buf;
    }

    private static void drainReceiver(QwpUdpReceiver receiver) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        boolean everReceived = false;
        while (System.nanoTime() < deadline) {
            boolean received = receiver.runSerially();
            if (received) {
                everReceived = true;
            } else if (everReceived) {
                break;
            }
            Os.pause();
        }
        Assert.assertTrue("timeout: receiver did not process any datagrams", everReceived);
    }

    private static QwpUdpSender newSender() {
        return new QwpUdpSender(NetworkFacadeImpl.INSTANCE, 0, LOCALHOST, PORT, 0);
    }

    private static void sendRawBytes(byte[] data) {
        long fd = Net.socketUdp();
        Assert.assertTrue("failed to open UDP socket", fd > -1);
        long mem = 0;
        long sockaddr = 0;
        try {
            mem = Unsafe.malloc(data.length, MemoryTag.NATIVE_DEFAULT);
            for (int i = 0; i < data.length; i++) {
                Unsafe.putByte(mem + i, data[i]);
            }
            sockaddr = Net.sockaddr(LOCALHOST, PORT);
            int sent = Net.sendTo(fd, mem, data.length, sockaddr);
            Assert.assertEquals(data.length, sent);
        } finally {
            if (sockaddr != 0) {
                Net.freeSockAddr(sockaddr);
            }
            if (mem != 0) {
                Unsafe.free(mem, data.length, MemoryTag.NATIVE_DEFAULT);
            }
            Net.close(fd);
        }
    }

    private static void sendValidRow(String table, long id, long ts) {
        try (QwpUdpSender sender = newSender()) {
            sender.table(table)
                    .longColumn("id", id)
                    .at(ts, ChronoUnit.MICROS);
            sender.flush();
        }
    }

    @FunctionalInterface
    public interface ReceiverFactory {
        QwpUdpReceiver create(QwpUdpReceiverConfiguration config, CairoEngine engine);
    }
}
