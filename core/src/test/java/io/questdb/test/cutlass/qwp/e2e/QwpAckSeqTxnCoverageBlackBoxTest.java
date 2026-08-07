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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.PropertyKey;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.TestServerMain;
import io.questdb.test.cutlass.qwp.AbstractQwpBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Asserts that an OK ack reports the per-table seqTxn of the work its batch
 * committed. That {@code (table, seqTxn)} pair is the only thing a durable-ack
 * client can pin a store-and-forward slot on: {@code CursorWebSocketSendLoop}
 * treats an OK frame carrying zero table entries as trivially durable and
 * releases the slot as soon as earlier entries drain.
 * <p>
 * The test drives the real client against the real server and reads the acks
 * off the wire through a TCP tee, decoding them with the client's own
 * {@link WebSocketResponse}. Nothing reaches into server internals, so the
 * assertion pins the protocol contract rather than an implementation detail.
 * <p>
 * {@link #testOkAckReportsSeqTxnBelowCap()} is the control: it holds the batch
 * under {@code qwp.max.uncommitted.rows} so the append-time force commit never
 * fires. Both cases send the same rows through the same code path, so the pair
 * isolates the force commit as the only variable.
 */
public class QwpAckSeqTxnCoverageBlackBoxTest extends AbstractQwpBootstrapTest {

    private static final int ROW_COUNT = 200;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
    }

    @Test
    public void testOkAckReportsSeqTxnBelowCap() throws Exception {
        assertAckReportsSeqTxn("1000000", "ack_cov_below");
    }

    @Test
    public void testOkAckReportsSeqTxnWhenAppendTimeCommitFires() throws Exception {
        // QwpWalAppender force-commits the table block once it crosses
        // qwp.max.uncommitted.rows, draining the writer before QwpTudCache runs.
        assertAckReportsSeqTxn("10", "ack_cov_over");
    }

    private void assertAckReportsSeqTxn(String maxUncommittedRows, String table) throws Exception {
        try (TestServerMain serverMain = startWithEnvVariables(
                PropertyKey.QWP_MAX_UNCOMMITTED_ROWS.getEnvVarName(), maxUncommittedRows
        )) {
            serverMain.execute("CREATE TABLE " + table + " (val LONG, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");

            final List<byte[]> serverFrames;
            try (AckTee tee = new AckTee(HTTP_PORT)) {
                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + tee.localPort())
                        .build()) {
                    for (int i = 0; i < ROW_COUNT; i++) {
                        sender.table(table)
                                .longColumn("val", i)
                                .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
                serverFrames = tee.serverBinaryFrames();
            }

            // The rows land either way - an unreported seqTxn is a tracking gap,
            // not a lost write.
            TestUtils.assertEventually(() ->
                    serverMain.assertSql("SELECT count() FROM " + table, "count\n" + ROW_COUNT + "\n"));

            int okAckCount = 0;
            long reportedSeqTxn = Long.MIN_VALUE;
            final StringBuilder observed = new StringBuilder();
            final WebSocketResponse response = new WebSocketResponse();
            for (int f = 0, n = serverFrames.size(); f < n; f++) {
                final byte[] payload = serverFrames.get(f);
                final long ptr = Unsafe.malloc(payload.length, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < payload.length; i++) {
                        Unsafe.putByte(ptr + i, payload[i]);
                    }
                    if (!response.readFrom(ptr, payload.length) || !response.isSuccess()) {
                        continue;
                    }
                    okAckCount++;
                    final int entries = response.getTableEntryCount();
                    observed.append("[seq=").append(response.getSequence())
                            .append(" tables=").append(entries);
                    for (int e = 0; e < entries; e++) {
                        observed.append(' ').append(response.getTableName(e))
                                .append("->").append(response.getTableSeqTxn(e));
                        if (table.equals(response.getTableName(e))) {
                            reportedSeqTxn = Math.max(reportedSeqTxn, response.getTableSeqTxn(e));
                        }
                    }
                    observed.append(']');
                } finally {
                    Unsafe.free(ptr, payload.length, MemoryTag.NATIVE_DEFAULT);
                }
            }

            Assert.assertTrue("the server sent no OK ack", okAckCount > 0);
            Assert.assertTrue(
                    "no OK ack reported a seqTxn for " + table + ", so a durable-ack client reads "
                            + "the batch as trivially durable and trims its store-and-forward slot "
                            + "before the upload tracker has seen the WAL segment; acks seen: " + observed,
                    reportedSeqTxn > 0
            );
        }
    }

    /**
     * Transparent TCP relay that forwards both directions and tees the
     * server-to-client bytes into a buffer. The test decodes frames after the
     * connection closes, which keeps the relay free of protocol knowledge and
     * off the assertion thread.
     */
    private static final class AckTee implements Closeable {
        private final ServerSocket listener;
        private final ByteArrayOutputStream serverToClient = new ByteArrayOutputStream();
        private final List<Socket> sockets = new ArrayList<>();

        private AckTee(int upstreamPort) throws IOException {
            listener = new ServerSocket(0, 4, InetAddress.getLoopbackAddress());
            final Thread acceptor = new Thread(() -> {
                try {
                    final Socket downstream = listener.accept();
                    final Socket upstream = new Socket(InetAddress.getLoopbackAddress(), upstreamPort);
                    synchronized (sockets) {
                        sockets.add(downstream);
                        sockets.add(upstream);
                    }
                    pump(downstream.getInputStream(), upstream.getOutputStream(), null);
                    pump(upstream.getInputStream(), downstream.getOutputStream(), serverToClient);
                } catch (IOException ignore) {
                    // the test tore the relay down
                }
            }, "qwp-ack-tee-acceptor");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        @Override
        public void close() {
            synchronized (sockets) {
                for (int i = 0, n = sockets.size(); i < n; i++) {
                    try {
                        sockets.get(i).close();
                    } catch (IOException ignore) {
                        // already gone
                    }
                }
            }
            try {
                listener.close();
            } catch (IOException ignore) {
                // already gone
            }
        }

        private static int endOfHttpHeaders(byte[] b) {
            for (int i = 0; i + 3 < b.length; i++) {
                if (b[i] == '\r' && b[i + 1] == '\n' && b[i + 2] == '\r' && b[i + 3] == '\n') {
                    return i + 4;
                }
            }
            return 0;
        }

        private int localPort() {
            return listener.getLocalPort();
        }

        private void pump(InputStream in, OutputStream out, ByteArrayOutputStream tee) {
            final Thread t = new Thread(() -> {
                final byte[] buf = new byte[8192];
                try {
                    int n;
                    while ((n = in.read(buf)) > 0) {
                        if (tee != null) {
                            synchronized (tee) {
                                tee.write(buf, 0, n);
                            }
                        }
                        out.write(buf, 0, n);
                        out.flush();
                    }
                } catch (IOException ignore) {
                    // peer closed
                }
            }, "qwp-ack-tee-pump");
            t.setDaemon(true);
            t.start();
        }

        /**
         * Returns the payloads of complete server-to-client BINARY frames. The
         * stream opens with the HTTP 101 response, and RFC 6455 s5.1 leaves
         * server frames unmasked, so each payload is the raw QWP response.
         */
        private List<byte[]> serverBinaryFrames() {
            final byte[] b;
            synchronized (serverToClient) {
                b = serverToClient.toByteArray();
            }
            final List<byte[]> out = new ArrayList<>();
            int i = endOfHttpHeaders(b);
            while (i + 2 <= b.length) {
                final int opcode = b[i] & 0x0F;
                final int b1 = b[i + 1] & 0xFF;
                final boolean masked = (b1 & 0x80) != 0;
                long len = b1 & 0x7F;
                int off = i + 2;
                if (len == 126) {
                    if (off + 2 > b.length) {
                        break;
                    }
                    len = ((b[off] & 0xFFL) << 8) | (b[off + 1] & 0xFFL);
                    off += 2;
                } else if (len == 127) {
                    if (off + 8 > b.length) {
                        break;
                    }
                    len = 0;
                    for (int k = 0; k < 8; k++) {
                        len = (len << 8) | (b[off + k] & 0xFFL);
                    }
                    off += 8;
                }
                if (masked) {
                    off += 4;
                }
                if (off + len > b.length) {
                    break;
                }
                if (opcode == 0x2) {
                    final byte[] payload = new byte[(int) len];
                    System.arraycopy(b, off, payload, 0, (int) len);
                    out.add(payload);
                }
                i = off + (int) len;
            }
            return out;
        }
    }
}
