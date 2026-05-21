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

import io.questdb.cutlass.http.DefaultHttpContextConfiguration;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRequestHandlerFactory;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.codec.QwpServerInfoProvider;
import io.questdb.cutlass.qwp.server.QwpWebSocketHttpProcessor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.ObjHashSet;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Regression for the sibling bug to the 101-handshake send-fragmentation fix in
 * {@code QwpWebSocketUpgradeProcessor.onHeadersReady}.
 * <p>
 * The 101 success path stages the response in {@code onHeadersReady} and
 * defers the {@code rawSocket.send(...)} to {@code onRequestComplete}, which is
 * allowed to propagate {@code PeerIsSlowToReadException} into the framework's
 * park-on-write path. Before the matching fix to the reject branches, the
 * 400 / 426 / 421 paths in the same method called {@code rawSocket.send(...)}
 * directly and converted PISR into a fatal {@code HttpException}, so any
 * partial-send forced a disconnect mid-response and the client never received
 * the full reject body.
 * <p>
 * Each test pins the HTTP send-fragmentation chunk to a value smaller than the
 * reject body so the first send returns one fragment and the second triggers
 * PISR in {@code HttpResponseSink.sendBuffer}. With the fix the residual
 * fragments flush through {@code resumeSend} and the client receives the
 * complete response before the server disconnects.
 */
public class QwpUpgradeRejectFragmentationTest extends AbstractCairoTest {

    private static final Log LOG = LogFactory.getLog(QwpUpgradeRejectFragmentationTest.class);
    // Smaller than any reject body the cases below produce so the response
    // must be split across at least two sends. The first chunk lands on the
    // wire; the second trips PISR in HttpResponseSink.sendBuffer. Before the
    // fix the buggy code caught the PISR and threw HttpException, killing
    // the connection; after the fix the framework parks-on-write and the
    // residual flushes through resumeSend.
    private static final int SEND_FRAGMENTATION_CHUNK_SIZE = 50;
    // Canonical 400 Bad Request body written when the Origin header is
    // present. Hardcoded so the test asserts on exact wire bytes; the
    // server-side templates are package-private.
    private static final byte[] EXPECTED_400_ORIGIN_REJECT = (
            "HTTP/1.1 400 Bad Request\r\n"
                    + "Content-Type: text/plain\r\n"
                    + "Content-Length: 42\r\n"
                    + "\r\n"
                    + "Origin header not allowed on QWP WebSocket"
    ).getBytes(StandardCharsets.US_ASCII);
    // Canonical 421 Misdirected Request body written when the server role is
    // REPLICA. The X-QuestDB-Role header tells the client where to retry, so
    // a truncated reject leaves it blind to the redirect.
    private static final byte[] EXPECTED_421_REPLICA_REJECT = (
            "HTTP/1.1 421 Misdirected Request\r\n"
                    + "Connection: close\r\n"
                    + "Content-Length: 0\r\n"
                    + "X-QuestDB-Role: REPLICA\r\n"
                    + "\r\n"
    ).getBytes(StandardCharsets.US_ASCII);
    // Canonical 426 Upgrade Required body written by
    // QwpWebSocketUpgradeProcessor.UPGRADE_REQUIRED_RESPONSE.
    private static final byte[] EXPECTED_426_RESPONSE = (
            "HTTP/1.1 426 Upgrade Required\r\n"
                    + "Upgrade: websocket\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Sec-WebSocket-Version: 13\r\n"
                    + "Content-Length: 0\r\n"
                    + "\r\n"
    ).getBytes(StandardCharsets.US_ASCII);

    @Test
    public void test400OriginRejectIsFullyDeliveredUnderSendFragmentation() throws Exception {
        runWithFragmentedSend(port -> {
            String request = "GET /write/v4 HTTP/1.1\r\n"
                    + "Host: localhost:" + port + "\r\n"
                    + "Origin: http://evil.example.com\r\n"
                    + "Upgrade: websocket\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Sec-WebSocket-Key: AQIDBAUGBwgJCgsMDQ4PEA==\r\n"
                    + "Sec-WebSocket-Version: 13\r\n"
                    + "\r\n";
            assertFullRejectDelivered(port, request, EXPECTED_400_ORIGIN_REJECT);
        });
    }

    @Test
    public void test421ReplicaRoleRejectIsFullyDeliveredUnderSendFragmentation() throws Exception {
        node1.getConfigurationOverrides().setQwpServerInfoProvider(new ReplicaRoleProvider());
        runWithFragmentedSend(port -> {
            String request = "GET /write/v4 HTTP/1.1\r\n"
                    + "Host: localhost:" + port + "\r\n"
                    + "Upgrade: websocket\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Sec-WebSocket-Key: AQIDBAUGBwgJCgsMDQ4PEA==\r\n"
                    + "Sec-WebSocket-Version: 13\r\n"
                    + "\r\n";
            assertFullRejectDelivered(port, request, EXPECTED_421_REPLICA_REJECT);
        });
    }

    @Test
    public void test426VersionRejectIsFullyDeliveredUnderSendFragmentation() throws Exception {
        runWithFragmentedSend(port -> {
            String request = "GET /write/v4 HTTP/1.1\r\n"
                    + "Host: localhost:" + port + "\r\n"
                    + "Upgrade: websocket\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Sec-WebSocket-Key: AQIDBAUGBwgJCgsMDQ4PEA==\r\n"
                    + "Sec-WebSocket-Version: 99\r\n"
                    + "\r\n";
            assertFullRejectDelivered(port, request, EXPECTED_426_RESPONSE);
        });
    }

    private static void assertFullRejectDelivered(int port, String request, byte[] expected) throws Exception {
        try (Socket socket = new Socket("localhost", port)) {
            socket.setSoTimeout(5_000);

            OutputStream out = socket.getOutputStream();
            out.write(request.getBytes(StandardCharsets.US_ASCII));
            out.flush();

            byte[] received = drainUntilEof(socket.getInputStream());
            Assert.assertArrayEquals(
                    "Server delivered only "
                            + received.length + " of "
                            + expected.length
                            + " expected bytes. Server response: <<<"
                            + new String(received, StandardCharsets.US_ASCII)
                            + ">>>",
                    expected,
                    received
            );
        }
    }

    private static byte[] drainUntilEof(InputStream in) throws IOException {
        byte[] buf = new byte[256];
        int total = 0;
        while (true) {
            int n;
            try {
                n = in.read(buf, total, buf.length - total);
            } catch (IOException e) {
                // Socket reset by peer manifests as IOException on some OSes
                // when the server disconnects mid-response. Treat as EOF so
                // the assertion sees the partial payload rather than failing
                // with an opaque socket error.
                break;
            }
            if (n < 0) {
                break;
            }
            total += n;
            if (total == buf.length) {
                byte[] grown = new byte[buf.length * 2];
                System.arraycopy(buf, 0, grown, 0, total);
                buf = grown;
            }
        }
        byte[] out = new byte[total];
        System.arraycopy(buf, 0, out, 0, total);
        return out;
    }

    private void runWithFragmentedSend(PortTest test) throws Exception {
        final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(
                configuration,
                new DefaultHttpContextConfiguration() {
                    @Override
                    public int getForceSendFragmentationChunkSize() {
                        return SEND_FRAGMENTATION_CHUNK_SIZE;
                    }
                }
        ) {
            @Override
            public int getBindPort() {
                return 0;
            }
        };

        assertMemoryLeak(() -> {
            try (
                    TestWorkerPool workerPool = new TestWorkerPool(1);
                    HttpServer server = new HttpServer(httpConfig, workerPool, PlainSocketFactory.INSTANCE)
            ) {
                server.bind(new HttpRequestHandlerFactory() {
                    @Override
                    public ObjHashSet<String> getUrls() {
                        return httpConfig.getContextPathQWP();
                    }

                    @Override
                    public QwpWebSocketHttpProcessor newInstance() {
                        return new QwpWebSocketHttpProcessor(engine, httpConfig);
                    }
                });
                WorkerPoolUtils.setupWriterJobs(workerPool, engine);
                workerPool.start(LOG);
                try {
                    test.run(server.getPort());
                } finally {
                    workerPool.halt();
                    Path.clearThreadLocals();
                }
            }
        });
    }

    @FunctionalInterface
    private interface PortTest {
        void run(int port) throws Exception;
    }

    private static final class ReplicaRoleProvider implements QwpServerInfoProvider {

        @Override
        public int getCapabilities() {
            return 0;
        }

        @Override
        public CharSequence getClusterId() {
            return "";
        }

        @Override
        public long getEpoch() {
            return 0L;
        }

        @Override
        public CharSequence getNodeId() {
            return "";
        }

        @Override
        public byte role() {
            return QwpEgressMsgKind.ROLE_REPLICA;
        }
    }
}
