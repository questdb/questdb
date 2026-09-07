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
 * Wire coverage of the two browser-only egress carriers. A browser WebSocket
 * cannot set {@code X-QWP-Accept-Encoding} or {@code X-QWP-Max-Batch-Rows}, so
 * the server also reads them from the upgrade URL. The unit tests around
 * {@code negotiateMaxBatchRows} and {@code writeServerInfoFrame} cover the
 * functions in isolation; only a real upgrade proves {@code onHeadersReady}
 * reads the right URL parameter and applies it to the connection.
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

    private static byte[] readServerInfo(String query) throws Exception {
        try (Socket socket = new Socket("127.0.0.1", HTTP_PORT)) {
            socket.setSoTimeout(60_000);
            QwpWireTestFixtures.performReadHandshake(socket, query);
            byte[] frame = QwpWireTestFixtures.readServerFrame(socket.getInputStream());
            Assert.assertTrue("truncated SERVER_INFO", frame.length > QwpConstants.HEADER_SIZE);
            Assert.assertEquals(
                    "SERVER_INFO must be the first frame after the upgrade",
                    QwpEgressMsgKind.SERVER_INFO,
                    frame[QwpConstants.HEADER_SIZE]
            );
            return frame;
        }
    }
}
