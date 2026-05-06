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

import io.questdb.PropertyKey;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Exercises zstd compression of {@code RESULT_BATCH} frames end-to-end.
 * <p>
 * The feature is negotiated at WebSocket upgrade time via
 * {@code X-QWP-Accept-Encoding}, not per-query. Each test opens its own
 * connection so the negotiated codec starts clean.
 * <p>
 * Coverage:
 * <ul>
 *   <li>{@code compression=zstd} at default level 3 round-trips correctly
 *       across a highly compressible column (rotating symbols).</li>
 *   <li>Explicit {@code compression_level} values at the ends of the clamp
 *       range ({@code 1} and {@code 22}) still decode correctly. The server
 *       clamps 22 down to 9 internally; the client doesn't need to know.</li>
 *   <li>{@code compression=raw} (the library default) streams without the
 *       compression machinery engaging -- regression guard that the header is
 *       genuinely absent and the server-side codec stays at NONE.</li>
 *   <li>Explicit {@code compression=auto} negotiates zstd and completes
 *       multi-batch streaming correctly.</li>
 *   <li>Compression interoperates with artificial network fragmentation.</li>
 * </ul>
 */
public class QwpEgressCompressionTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testAutoExplicitlyNegotiatesZstd() throws Exception {
        // Coverage for the opt-in {@code compression=auto} path: advertises
        // {@code zstd,raw}, server picks zstd, multi-batch streaming round-trips
        // correctly. The library default is {@code raw}, so this test passes
        // the opt-in explicitly.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startQuestDB()) {
                serverMain.execute("CREATE TABLE many(id LONG, v DOUBLE, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute(
                        "INSERT INTO many SELECT x, CAST(x * 0.5 AS DOUBLE), x::TIMESTAMP FROM long_sequence(20000)");
                serverMain.awaitTable("many");
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";compression=auto;")) {
                    client.connect();
                    Assert.assertEquals("negotiated QWP version",
                            QwpConstants.MAX_SUPPORTED_VERSION, client.getNegotiatedQwpVersion());
                    assertSumMany(client);
                }
            }
        });
    }

    @Test
    public void testDefaultCompressionPreferenceIsRaw() {
        // Unit-level pin: both construction paths must default to "raw" so no
        // connection-string-less client secretly negotiates zstd. Regression
        // guard for the PR 6991 follow-up that flipped the default from "auto".
        try (QwpQueryClient fromConfig = QwpQueryClient.fromConfig(
                "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
            Assert.assertEquals("raw", fromConfig.getCompressionPreference());
        }
        try (QwpQueryClient plain = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
            Assert.assertEquals("raw", plain.getCompressionPreference());
        }
    }

    @Test
    public void testDefaultConnectionSendsRawBatches() throws Exception {
        // End-to-end pin: a connection with no {@code compression=} key must
        // receive batches WITHOUT the FLAG_ZSTD bit set. Reads the flags byte
        // from each batch's QWP header directly so the assertion survives
        // any future decoder API changes.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startQuestDB()) {
                serverMain.execute("CREATE TABLE d(id LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO d SELECT x, x::TIMESTAMP FROM long_sequence(500)");
                serverMain.awaitTable("d");
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    final int[] batchesSeen = {0};
                    client.execute("SELECT * FROM d", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            batchesSeen[0]++;
                            // QWP header layout: byte[5] is the flags field.
                            byte flags = Unsafe.getByte(
                                    batch.payloadAddr() + QwpConstants.HEADER_OFFSET_FLAGS);
                            Assert.assertEquals(
                                    "default connection must not receive zstd-compressed batches "
                                            + "[flags=0x" + Integer.toHexString(flags & 0xFF) + "]",
                                    0,
                                    flags & QwpConstants.FLAG_ZSTD);
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("unexpected query error [status=" + status + "]: " + message);
                        }
                    });
                    Assert.assertTrue("expected at least one batch", batchesSeen[0] >= 1);
                }
            }
        });
    }

    @Test
    public void testCompressionRawBypassesZstd() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startQuestDB()) {
                serverMain.execute("CREATE TABLE r(id LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO r SELECT x, x::TIMESTAMP FROM long_sequence(500)");
                serverMain.awaitTable("r");
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";compression=raw;")) {
                    client.connect();
                    Assert.assertEquals("negotiated QWP version",
                            QwpConstants.MAX_SUPPORTED_VERSION, client.getNegotiatedQwpVersion());
                    assertLongSum(client, "SELECT * FROM r", 500, 500L * 501L / 2L);
                }
            }
        });
    }

    @Test
    public void testCompressionWithFragmentation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.DEBUG_HTTP_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "23",
                    PropertyKey.DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "23"
            )) {
                serverMain.execute("CREATE TABLE f(id LONG, s SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute(
                        "INSERT INTO f SELECT x, CAST('s_' || (x % 32) AS SYMBOL), x::TIMESTAMP FROM long_sequence(4000)");
                serverMain.awaitTable("f");
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";compression=zstd;")) {
                    client.connect();
                    Assert.assertEquals("negotiated QWP version",
                            QwpConstants.MAX_SUPPORTED_VERSION, client.getNegotiatedQwpVersion());
                    assertLongSum(client, "SELECT * FROM f", 4000, 4000L * 4001L / 2L);
                }
            }
        });
    }

    @Test
    public void testLevel1DecodesCorrectly() throws Exception {
        runLevelSmoke(1);
    }

    @Test
    public void testLevel22IsClampedAndDecodesCorrectly() throws Exception {
        // Level 22 on the wire; server clamps it down to MAX_LEVEL (9).
        // The client must decode correctly regardless of the level the
        // server actually used.
        runLevelSmoke(22);
    }

    @Test
    public void testZstdRoundTripsHighlyCompressibleSymbols() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startQuestDB()) {
                // Rotating symbols over 10k rows gives a ~50x compressible payload.
                serverMain.execute("CREATE TABLE z(id LONG, s SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute(
                        "INSERT INTO z SELECT x, CAST('s_' || (x % 8) AS SYMBOL), x::TIMESTAMP FROM long_sequence(10000)");
                serverMain.awaitTable("z");
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";compression=zstd;")) {
                    client.connect();
                    Assert.assertEquals("negotiated QWP version",
                            QwpConstants.MAX_SUPPORTED_VERSION, client.getNegotiatedQwpVersion());
                    assertLongSum(client, "SELECT * FROM z", 10000, 10_000L * 10_001L / 2L);
                }
            }
        });
    }

    private static TestServerMain startQuestDB() {
        return AbstractBootstrapTest.startWithEnvVariables();
    }

    private void assertLongSum(QwpQueryClient client, String sql, int expectedRows, long expectedSum) {
        final int[] rows = {0};
        final long[] sum = {0};
        client.execute(sql, new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                int n = batch.getRowCount();
                long base = batch.valuesAddr(0);
                int[] idx = batch.nonNullIndex(0);
                for (int r = 0; r < n; r++) {
                    sum[0] += Unsafe.getLong(base + 8L * idx[r]);
                }
                rows[0] += n;
            }

            @Override
            public void onEnd(long totalRows) {
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("compression test error [status=" + status + "]: " + message);
            }
        });
        Assert.assertEquals("rowCount for " + sql, expectedRows, rows[0]);
        Assert.assertEquals("sum for " + sql, expectedSum, sum[0]);
    }

    private void assertSumMany(QwpQueryClient client) {
        assertLongSum(client, "SELECT * FROM many", 20_000, 20_000L * 20_001L / 2L);
    }

    private void runLevelSmoke(int level) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startQuestDB()) {
                serverMain.execute("CREATE TABLE L(id LONG, v DOUBLE, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute(
                        "INSERT INTO L SELECT x, CAST(x * 1.5 AS DOUBLE), x::TIMESTAMP FROM long_sequence(2000)");
                serverMain.awaitTable("L");
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT
                                + ";compression=zstd;compression_level=" + level + ";")) {
                    client.connect();
                    Assert.assertEquals("negotiated QWP version",
                            QwpConstants.MAX_SUPPORTED_VERSION, client.getNegotiatedQwpVersion());
                    assertLongSum(client, "SELECT * FROM L", 2000, 2000L * 2001L / 2L);
                }
            }
        });
    }
}
