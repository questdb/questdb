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

import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.std.Os;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Pins the post-CLOSE read-drain of server-initiated fatal closes
 * ({@code QwpIngressUpgradeProcessor.gracefulCloseAndDrain}): after the server
 * emits a fatal WebSocket CLOSE + FIN it must NOT close the fd while the peer
 * may still be streaming, because a close under unread/in-flight client bytes
 * forces a TCP RST — and an RST destroys whatever the peer has not yet read
 * from its receive queue, including the final cumulative/durable ACK and the
 * CLOSE frame the whole teardown exists to deliver. A store-and-forward client
 * that loses that final durable ack replays every committed-but-unacked batch
 * on reconnect: duplicates on tables without DEDUP UPSERT KEYS (the CI
 * signature this drain fixes: {@code count == total + committed-at-demote}).
 * <p>
 * The vehicle is the TEXT-frame reject (CLOSE 1003), the simplest
 * deterministic server-initiated fatal close; the drain is shared by ALL such
 * closes, including the role-change deferred close whose durable-ack story the
 * enterprise witness {@code SqlFailoverQwpDeferredCloseExactlyOnceTest} pins
 * end-to-end.
 * <p>
 * Drain exits pinned here:
 * <ul>
 *   <li><b>Peer close</b> (implicit in the first test's teardown): the socket
 *       close after the assertions sends FIN and the server disconnects.</li>
 *   <li><b>Deadline</b>: a live writer that never reads its receive queue and
 *       never closes is cut off at
 *       {@link QwpIngressProcessorState#CLOSE_DRAIN_TIMEOUT_MICROS} — and not
 *       before, which is precisely the pre-fix bug (immediate fd close, RST
 *       under the goodbye).</li>
 *   <li><b>Silent peer</b>: reaped by the transport idle timeout — existing
 *       dispatcher behavior, not re-pinned here.</li>
 * </ul>
 */
public class QwpServerCloseDrainTest extends AbstractQwpWebSocketTest {

    @Test
    public void testCloseDrainDeadlineBoundsLingerAgainstLiveWriter() throws Exception {
        runInContext((port) -> {
            try (Socket socket = new Socket("localhost", port)) {
                socket.setSoTimeout(10_000);
                performWebSocketHandshake(socket, port);
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();

                out.write(createMaskedFrame(WebSocketOpcode.TEXT,
                        "text is not QWP".getBytes(StandardCharsets.UTF_8), true));
                out.flush();
                assertCloseFrameThenEof(in);

                // A live writer that never reads its receive queue and never
                // closes: the server cannot wait on its FIN forever, but must
                // not cut it off before the drain budget either -- an early
                // close is exactly the RST-under-the-goodbye bug. The 100ms
                // write cadence doubles as the recv-event stream the deadline
                // check rides on.
                long drainStartNanos = System.nanoTime();
                byte[] pipelined = createMaskedFrame(WebSocketOpcode.BINARY, new byte[64], true);
                long cutOffMillis = -1;
                for (int i = 0; i < 300; i++) {
                    try {
                        out.write(pipelined);
                        out.flush();
                    } catch (IOException expected) {
                        cutOffMillis = (System.nanoTime() - drainStartNanos) / 1_000_000L;
                        break;
                    }
                    Os.sleep(100);
                }
                Assert.assertTrue(
                        "server must cut off a live writer that ignores the CLOSE (drain deadline)",
                        cutOffMillis >= 0);
                Assert.assertTrue(
                        "server must linger for the drain budget before cutting off a live writer; "
                                + "an early cut-off is the RST race that destroys the final ACK/CLOSE "
                                + "in the peer's receive queue [cutOffMillis=" + cutOffMillis + ']',
                        cutOffMillis >= 3_000);
                Assert.assertTrue(
                        "drain deadline must bound the linger [cutOffMillis=" + cutOffMillis + ']',
                        cutOffMillis <= 20_000);
            }
        });
    }

    @Test
    public void testFatalCloseLingersWhileStreamingPeerDrainsItsReceiveQueue() throws Exception {
        runInContext((port) -> {
            try (Socket socket = new Socket("localhost", port)) {
                socket.setSoTimeout(10_000);
                performWebSocketHandshake(socket, port);
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();

                // Server-initiated fatal close: TEXT frames are rejected with
                // CLOSE 1003; the frame and the FIN behind it arrive intact.
                out.write(createMaskedFrame(WebSocketOpcode.TEXT,
                        "text is not QWP".getBytes(StandardCharsets.UTF_8), true));
                out.flush();
                assertCloseFrameThenEof(in);

                // Behave like a mid-stream writer that has not yet processed
                // the goodbye: keep pipelining frames. Pre-fix the server had
                // already closed the fd, so the first write elicited an RST
                // and a follow-up write failed; post-fix the server drains
                // and discards until we close.
                byte[] pipelined = createMaskedFrame(WebSocketOpcode.BINARY, new byte[64], true);
                try {
                    for (int i = 0; i < 3; i++) {
                        out.write(pipelined);
                        out.flush();
                        Os.sleep(200);
                    }
                } catch (IOException e) {
                    Assert.fail("server must drain frames pipelined behind its fatal CLOSE instead of "
                            + "resetting the connection -- an RST destroys the final ACK/CLOSE still "
                            + "queued in the peer's receive buffer: " + e);
                }
            } // socket close -> FIN -> the server's drain exits promptly
        });
    }

    /**
     * Reads the server's unmasked CLOSE frame (code 1003, UNSUPPORTED_DATA),
     * then the EOF of the server's half-close directly behind it.
     */
    private static void assertCloseFrameThenEof(InputStream in) throws Exception {
        int byte0 = in.read();
        Assert.assertNotEquals("expected a CLOSE frame before EOF", -1, byte0);
        Assert.assertEquals("expected CLOSE opcode", WebSocketOpcode.CLOSE, byte0 & 0x0F);
        int byte1 = in.read();
        Assert.assertNotEquals(-1, byte1);
        Assert.assertFalse("server frames must not be masked", (byte1 & 0x80) != 0);
        int payloadLen = byte1 & 0x7F;
        Assert.assertTrue("CLOSE payload must carry the status code", payloadLen >= 2);
        byte[] payload = new byte[payloadLen];
        int totalRead = 0;
        while (totalRead < payloadLen) {
            int n = in.read(payload, totalRead, payloadLen - totalRead);
            Assert.assertNotEquals("unexpected end of stream in CLOSE payload", -1, n);
            totalRead += n;
        }
        int closeCode = ((payload[0] & 0xFF) << 8) | (payload[1] & 0xFF);
        Assert.assertEquals(WebSocketCloseCode.UNSUPPORTED_DATA, closeCode);

        // shutdown(WR) behind the CLOSE frame: EOF, not an exception -- the
        // goodbye must arrive on a clean half-close, never a reset.
        Assert.assertEquals("expected EOF (server FIN) after the CLOSE frame", -1, in.read());
    }

    private static byte[] createMaskedFrame(int opcode, byte[] payload, boolean fin) {
        byte[] maskKey = {0x12, 0x34, 0x56, 0x78};
        int payloadLen = payload.length;
        int headerLen = (payloadLen <= 125) ? 6 : (payloadLen <= 65_535) ? 8 : 14;

        byte[] frame = new byte[headerLen + payloadLen];
        int offset = 0;
        frame[offset++] = (byte) ((fin ? 0x80 : 0x00) | (opcode & 0x0F));
        if (payloadLen <= 125) {
            frame[offset++] = (byte) (0x80 | payloadLen);
        } else if (payloadLen <= 65_535) {
            frame[offset++] = (byte) (0x80 | 126);
            frame[offset++] = (byte) ((payloadLen >> 8) & 0xFF);
            frame[offset++] = (byte) (payloadLen & 0xFF);
        } else {
            frame[offset++] = (byte) (0x80 | 127);
            for (int i = 7; i >= 0; i--) {
                frame[offset++] = (byte) (((long) payloadLen >> (i * 8)) & 0xFF);
            }
        }
        System.arraycopy(maskKey, 0, frame, offset, 4);
        offset += 4;
        for (int i = 0; i < payloadLen; i++) {
            frame[offset + i] = (byte) (payload[i] ^ maskKey[i % 4]);
        }
        return frame;
    }

    private static void performWebSocketHandshake(Socket socket, int port) throws Exception {
        OutputStream out = socket.getOutputStream();
        InputStream in = socket.getInputStream();

        byte[] keyBytes = new byte[16];
        for (int i = 0; i < 16; i++) {
            keyBytes[i] = (byte) (i + 1);
        }
        String wsKey = Base64.getEncoder().encodeToString(keyBytes);

        String request = "GET /write/v4 HTTP/1.1\r\n" +
                "Host: localhost:" + port + "\r\n" +
                "Upgrade: websocket\r\n" +
                "Connection: Upgrade\r\n" +
                "Sec-WebSocket-Key: " + wsKey + "\r\n" +
                "Sec-WebSocket-Version: 13\r\n" +
                "\r\n";
        out.write(request.getBytes(StandardCharsets.UTF_8));
        out.flush();

        StringBuilder response = new StringBuilder();
        while (true) {
            int b = in.read();
            Assert.assertNotEquals("unexpected end of stream during handshake", -1, b);
            response.append((char) b);
            int len = response.length();
            if (len >= 4
                    && response.charAt(len - 4) == '\r' && response.charAt(len - 3) == '\n'
                    && response.charAt(len - 2) == '\r' && response.charAt(len - 1) == '\n') {
                break;
            }
        }
        Assert.assertTrue(
                "expected 101 Switching Protocols, got: " + response.toString().split("\r\n")[0],
                response.toString().startsWith("HTTP/1.1 101"));
    }
}
