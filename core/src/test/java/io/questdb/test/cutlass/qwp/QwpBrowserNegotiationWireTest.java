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

package io.questdb.test.cutlass.qwp;

import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Wire coverage of the browser-only URL carriers. A browser WebSocket cannot
 * set {@code X-QWP-Accept-Encoding} or {@code X-QWP-Max-Batch-Rows} on egress,
 * and cannot read the {@code X-QWP-Max-Batch-Size} response header on ingress,
 * so the server also reads {@code qwp_accept_encoding},
 * {@code qwp_max_batch_rows} and {@code qwp_browser_handshake} from the upgrade
 * URL. The unit tests around {@code negotiateMaxBatchRows},
 * {@code negotiateAcceptEncoding} and {@code writeServerInfoFrame} cover the
 * functions in isolation; only a real upgrade proves {@code onHeadersReady}
 * reads the right URL parameter on the right route and applies it to the
 * connection.
 */
public class QwpBrowserNegotiationWireTest extends AbstractQwpBootstrapTest {

    /**
     * Offset of the little-endian {@code capabilities} int inside a QWP
     * SERVER_INFO message: the 12-byte header, then msg_kind, role and the
     * 8-byte epoch.
     */
    private static final int SERVER_INFO_CAPABILITIES_OFFSET = QwpConstants.HEADER_SIZE + 10;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testBrowserUrlAcceptEncodingNegotiatesCompression() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain ignored = startFragmented()) {
                byte[] negotiated = readServerInfo("?qwp_accept_encoding=zstd");
                Assert.assertNotEquals(
                        "the browser URL carrier must advertise CAP_COMPRESSION so the client"
                                + " knows a codec/level trailer follows",
                        0,
                        readCapabilities(negotiated) & QwpEgressMsgKind.CAP_COMPRESSION
                );
                Assert.assertEquals(
                        "the trailer must name the negotiated codec",
                        QwpConstants.COMPRESSION_ZSTD,
                        negotiated[negotiated.length - 2]
                );
                byte level = negotiated[negotiated.length - 1];
                Assert.assertTrue(
                        "the trailer must name a usable zstd level, got " + level,
                        level >= QwpConstants.COMPRESSION_ZSTD_MIN_LEVEL
                                && level <= QwpConstants.COMPRESSION_ZSTD_MAX_LEVEL
                );

                // Control: without the carrier the bit stays clear, so the
                // client never reads two bytes the server did not write.
                byte[] plain = readServerInfo("");
                Assert.assertEquals(
                        "a connection that requested no encoding must not advertise CAP_COMPRESSION",
                        0,
                        readCapabilities(plain) & QwpEgressMsgKind.CAP_COMPRESSION
                );
            }
        });
    }

    /**
     * The two carriers share a value grammar but not a delivery path: the
     * header arrives verbatim, the URL goes through
     * {@code HttpHeaderParser.urlDecode}, which re-keys a parameter on every
     * unescaped {@code '='}. The percent-encoded form must therefore reach the
     * negotiator with its level intact, and the raw form must be dropped whole
     * rather than half-applied -- if it were ever half-applied the server would
     * compress while telling the client it had not.
     */
    @Test
    public void testBrowserUrlAcceptEncodingRequiresPercentEncodedLevel() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain ignored = startFragmented()) {
                byte[] encoded = readServerInfo("?qwp_accept_encoding=zstd%3Blevel%3D5");
                Assert.assertNotEquals(
                        "a percent-encoded carrier must still advertise CAP_COMPRESSION",
                        0,
                        readCapabilities(encoded) & QwpEgressMsgKind.CAP_COMPRESSION
                );
                Assert.assertEquals(
                        "the trailer must name the negotiated codec",
                        QwpConstants.COMPRESSION_ZSTD,
                        encoded[encoded.length - 2]
                );
                Assert.assertEquals(
                        "the ;level=N parameter must survive percent-encoding",
                        5,
                        encoded[encoded.length - 1]
                );

                // The same value unescaped: urlDecode re-keys on the second
                // '=', so qwp_accept_encoding is absent and the request reads
                // as "no preference". The wire is then genuinely raw, and
                // CAP_COMPRESSION stays clear so the client is told so rather
                // than left decoding uncompressed frames as zstd.
                byte[] unescaped = readServerInfo("?qwp_accept_encoding=zstd;level=5");
                Assert.assertEquals(
                        "an unescaped ';level=N' loses the whole parameter, so no codec may be advertised",
                        0,
                        readCapabilities(unescaped) & QwpEgressMsgKind.CAP_COMPRESSION
                );
            }
        });
    }

    /**
     * Both carriers present at once, naming zstd at different levels. The
     * trailer's level says which one {@code onHeadersReady} applied, so this
     * pins the precedence at the CALL SITE. The unit test around
     * {@code negotiateAcceptEncoding} pins the function; its two arguments have
     * the same type, so swapping them at the call site compiles silently and
     * lets a reverse proxy override the codec the browser asked for -- the
     * threat the production javadoc names.
     * <p>
     * Both directions are asserted so that "the URL wins" cannot be confused
     * with "the higher level wins" or "the lower level wins".
     */
    @Test
    public void testBrowserUrlAcceptEncodingWinsOverProxyInjectedHeader() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain ignored = startFragmented()) {
                assertNegotiatedZstdLevel(5, "?qwp_accept_encoding=zstd%3Blevel%3D5", "zstd;level=1");
                assertNegotiatedZstdLevel(1, "?qwp_accept_encoding=zstd%3Blevel%3D1", "zstd;level=5");
            }
        });
    }

    @Test
    public void testBrowserUrlHandshakePushesIngressServerInfo() throws Exception {
        // The ingress counterpart of the two egress carriers below. The unit
        // tests drive qwp_browser_handshake through a mock request header, so
        // only a real upgrade proves the parameter survives route matching on
        // /write/v4 and reaches getUrlParam.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain ignored = startFragmented()) {
                try (Socket socket = new Socket("127.0.0.1", HTTP_PORT)) {
                    socket.setSoTimeout(60_000);
                    QwpWireTestFixtures.performWriteHandshake(socket, "?qwp_browser_handshake=v1");
                    byte[] frame = QwpWireTestFixtures.readServerFrame(socket.getInputStream());
                    Assert.assertEquals(
                            "the browser ingress handshake frame is status + u32 cap + capability mask",
                            6,
                            frame.length
                    );
                    Assert.assertEquals(
                            "STATUS_SERVER_INFO must be the first frame after the upgrade",
                            QwpConstants.STATUS_SERVER_INFO,
                            frame[0]
                    );
                    int maxBatchSize = (frame[1] & 0xFF)
                            | (frame[2] & 0xFF) << 8
                            | (frame[3] & 0xFF) << 16
                            | (frame[4] & 0xFF) << 24;
                    Assert.assertTrue(
                            "the advertised batch cap must be usable, got " + maxBatchSize,
                            maxBatchSize > 0 && maxBatchSize <= QwpConstants.DEFAULT_MAX_BATCH_SIZE
                    );
                    // This server has no durable-ack registry, so the verdict
                    // must read false here rather than being absent: the bit is
                    // the only durable-ack signal a browser can read.
                    Assert.assertEquals(
                            "the capability mask must report durable ACK off",
                            0,
                            frame[5] & QwpConstants.SERVER_INFO_CAP_DURABLE_ACK
                    );
                }
            }
        });
    }

    @Test
    public void testBrowserUrlMaxBatchRowsCapsResultBatches() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String sql = "SELECT x FROM long_sequence(3)";
            try (final TestServerMain ignored = startFragmented()) {
                Assert.assertEquals(
                        "a browser URL row cap of 1 must split three rows across three batches",
                        3,
                        countResultBatches("?qwp_max_batch_rows=1", sql)
                );
                Assert.assertEquals(
                        "the same query without the carrier must stay in one batch,"
                                + " so the assertion above pins the carrier and not the query shape",
                        1,
                        countResultBatches("", sql)
                );
            }
        });
    }

    private static int countResultBatches(String query, String sql) throws Exception {
        try (Socket socket = new Socket("127.0.0.1", HTTP_PORT)) {
            socket.setSoTimeout(60_000);
            QwpWireTestFixtures.performReadHandshake(socket, query);
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            // SERVER_INFO is pushed unsolicited on connect.
            QwpWireTestFixtures.readServerFrame(in);

            out.write(QwpWireTestFixtures.maskedFrame(
                    WebSocketOpcode.BINARY,
                    QwpWireTestFixtures.buildQueryRequest(1L, sql)
            ));
            out.flush();

            int batches = 0;
            for (int i = 0; i < 32; i++) {
                byte[] frame = QwpWireTestFixtures.readServerFrame(in);
                Assert.assertTrue("truncated QWP message", frame.length > QwpConstants.HEADER_SIZE);
                byte kind = frame[QwpConstants.HEADER_SIZE];
                if (kind == QwpEgressMsgKind.RESULT_BATCH) {
                    batches++;
                } else if (kind == QwpEgressMsgKind.RESULT_END) {
                    return batches;
                }
            }
            Assert.fail("no RESULT_END arrived after " + batches + " batches");
            return -1; // unreachable
        }
    }

    private static int readCapabilities(byte[] serverInfo) {
        return (serverInfo[SERVER_INFO_CAPABILITIES_OFFSET] & 0xFF)
                | (serverInfo[SERVER_INFO_CAPABILITIES_OFFSET + 1] & 0xFF) << 8
                | (serverInfo[SERVER_INFO_CAPABILITIES_OFFSET + 2] & 0xFF) << 16
                | (serverInfo[SERVER_INFO_CAPABILITIES_OFFSET + 3] & 0xFF) << 24;
    }

    private static void assertNegotiatedZstdLevel(int expectedLevel, String query, String headerValue) throws Exception {
        byte[] info = readServerInfo(query, "X-QWP-Accept-Encoding: " + headerValue + "\r\n");
        Assert.assertNotEquals(
                "a URL-carrier request must advertise CAP_COMPRESSION whichever carrier won",
                0,
                readCapabilities(info) & QwpEgressMsgKind.CAP_COMPRESSION
        );
        Assert.assertEquals(
                "the trailer must name the negotiated codec",
                QwpConstants.COMPRESSION_ZSTD,
                info[info.length - 2]
        );
        Assert.assertEquals(
                "the browser's URL carrier must win over the header a proxy could inject",
                expectedLevel,
                info[info.length - 1]
        );
    }

    private static byte[] readServerInfo(String query) throws Exception {
        return readServerInfo(query, "");
    }

    private static byte[] readServerInfo(String query, String extraHeaders) throws Exception {
        try (Socket socket = new Socket("127.0.0.1", HTTP_PORT)) {
            socket.setSoTimeout(60_000);
            QwpWireTestFixtures.performReadHandshake(socket, query, extraHeaders);
            byte[] frame = QwpWireTestFixtures.readServerFrame(socket.getInputStream());
            QwpWireTestFixtures.assertQwpMessageKind(frame, QwpEgressMsgKind.SERVER_INFO);
            return frame;
        }
    }
}
