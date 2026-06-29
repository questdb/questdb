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

import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.std.str.DirectUtf8Sequence;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.egress.QwpEgressMetrics;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * End-to-end coverage of the per-query {@code QUERY_FLAG_RESET_DICT} flag over
 * the wire. The client requests a reset via {@code execute(..., true)}, which
 * appends the {@code query_flags} trailer when the server advertised
 * {@code CAP_QUERY_FLAGS}; the server clears its connection-scoped SYMBOL dict
 * at the query boundary and ships {@code CACHE_RESET} before the first
 * {@code RESULT_BATCH}.
 * <p>
 * Caps are left at their defaults so the only reset trigger under test is the
 * flag, not a soft cap -- this is the complement to
 * {@link QwpEgressCacheResetWireTest}, which drives the cap path. Each test
 * asserts both the server-side reset metric and that the client still resolves
 * every symbol after the flush: correct resolution proves the {@code CACHE_RESET}
 * landed before the batch and the following delta restarted at {@code deltaStart=0},
 * since a misordered frame or non-zero delta would desync the client decoder and
 * surface as a decode error or a missing symbol.
 */
public class QwpEgressQueryFlagsResetWireTest extends AbstractQwpBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    /**
     * Raw-wire coverage of the request-id pre-seed in
     * {@code QwpEgressUpgradeProcessor.handleQueryRequest}: it peeks the request
     * id straight off the payload before decoding, so a decode failure -- here a
     * malformed {@code query_flags} trailer introduced by this feature -- still
     * reports the right id in the {@code QUERY_ERROR} frame instead of 0.
     * <p>
     * The high-level {@link QwpQueryClient} cannot drive this: it never emits a
     * malformed trailer, and its {@code onError(status, message)} callback does
     * not surface the request id (a single in-flight query is assumed). So this
     * test hand-builds the frame on a raw socket and decodes the
     * {@code QUERY_ERROR} off the wire. Deleting the pre-seed line drops the
     * echoed id to 0 and fails here.
     */
    @Test
    public void testMalformedQueryFlagsTrailerEchoesRequestId() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // A distinctive, clearly non-zero request id so a regression to 0 is obvious.
            final long requestId = 0x0123456789ABCDEFL;
            try (final TestServerMain ignored = startFragmented()) {
                try (Socket socket = new Socket("127.0.0.1", HTTP_PORT)) {
                    socket.setSoTimeout(60_000);
                    performReadHandshake(socket);

                    OutputStream out = socket.getOutputStream();
                    InputStream in = socket.getInputStream();

                    // QUERY_REQUEST with a valid header but a lone continuation byte
                    // (0x80) where the optional query_flags varint should be -- the
                    // decoder runs off the frame end and throws QwpParseException.
                    out.write(maskedBinaryFrame(buildMalformedQueryRequest(requestId)));
                    out.flush();

                    // SERVER_INFO is pushed first on connect; skip past it to the error.
                    byte[] frame = readFrameUntilKind(in, QwpEgressMsgKind.QUERY_ERROR);

                    long echoedRequestId = readLongLE(frame, QwpConstants.HEADER_SIZE + 1);
                    Assert.assertEquals(
                            "QUERY_ERROR on a malformed query_flags trailer must echo the sent request id, not 0",
                            requestId,
                            echoedRequestId
                    );
                    Assert.assertEquals(
                            "a malformed trailer is a client protocol error, so it maps to PARSE_ERROR",
                            QwpConstants.STATUS_PARSE_ERROR,
                            frame[QwpConstants.HEADER_SIZE + 1 + 8]
                    );
                }
            }
        });
    }

    /**
     * A SELECT carrying the flag on a connection whose dict an earlier query
     * already grew fires exactly one {@code CACHE_RESET}, and the flagged query
     * still resolves every symbol from its own freshly populated dict.
     */
    @Test
    public void testResetFlagFiresCacheResetBeforeBatch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented("QDB_METRICS_ENABLED", "true")) {
                serverMain.execute("CREATE TABLE flag_fires(sym SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO flag_fires VALUES
                            ('a', 1::TIMESTAMP), ('b', 2::TIMESTAMP),
                            ('c', 3::TIMESTAMP), ('d', 4::TIMESTAMP)
                        """);
                serverMain.awaitTable("flag_fires");
                QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                long resetsBefore = metrics.cacheResetDictCount();

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();

                    // Query 1 (no flag): grows the connection-scoped dict to {a,b,c,d}.
                    java.util.Set<String> q1 = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM flag_fires", collectInto(q1));
                    Assert.assertEquals(java.util.Set.of("a", "b", "c", "d"), q1);
                    Assert.assertEquals("no flag yet => no reset", resetsBefore, metrics.cacheResetDictCount());

                    // Query 2 (flag): the non-empty dict is reset before the query
                    // streams, then repopulated; the client must resolve everything.
                    java.util.Set<String> q2 = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM flag_fires", collectInto(q2), true);
                    Assert.assertEquals(java.util.Set.of("a", "b", "c", "d"), q2);
                    Assert.assertEquals("flag on a non-empty dict must fire exactly one reset",
                            resetsBefore + 1, metrics.cacheResetDictCount());
                }
            }
        });
    }

    /**
     * Empty-dict guard, over the wire: the first flagged query on a fresh
     * connection asks for a reset, but the dict is empty so the server emits no
     * {@code CACHE_RESET} frame. The query still streams its symbols normally.
     */
    @Test
    public void testResetFlagOnEmptyDictEmitsNoFrame() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented("QDB_METRICS_ENABLED", "true")) {
                serverMain.execute("CREATE TABLE flag_empty(sym SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO flag_empty VALUES ('a', 1::TIMESTAMP), ('b', 2::TIMESTAMP)");
                serverMain.awaitTable("flag_empty");
                QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                long resetsBefore = metrics.cacheResetDictCount();

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    // First query on the connection: dict is empty, so the flag is
                    // a no-op and no CACHE_RESET goes out.
                    java.util.Set<String> got = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM flag_empty", collectInto(got), true);
                    Assert.assertEquals(java.util.Set.of("a", "b"), got);
                    Assert.assertEquals("empty dict => no reset frame",
                            resetsBefore, metrics.cacheResetDictCount());
                }
            }
        });
    }

    /**
     * Deferred emit through the error path: a SELECT carrying the flag clears
     * the dict and stages the reset BEFORE compilation, then fails to compile
     * (missing table). The query returns via {@code QUERY_ERROR} without
     * emitting, so the staged {@code CACHE_RESET} surfaces on the next
     * successful SELECT. This is the sibling of
     * {@link #testResetFlagOnNonSelectDefersToNextSelect} for the
     * {@code catch(Throwable)} path rather than the EXEC_DONE path.
     */
    @Test
    public void testResetFlagOnFailedSelectDefersToNextSelect() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented("QDB_METRICS_ENABLED", "true")) {
                serverMain.execute("CREATE TABLE flag_fail(sym SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO flag_fail VALUES
                            ('a', 1::TIMESTAMP), ('b', 2::TIMESTAMP),
                            ('c', 3::TIMESTAMP), ('d', 4::TIMESTAMP)
                        """);
                serverMain.awaitTable("flag_fail");
                QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                long resetsBefore = metrics.cacheResetDictCount();

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();

                    // Query 1 (no flag): grows the dict to {a,b,c,d}.
                    java.util.Set<String> q1 = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM flag_fail", collectInto(q1));
                    Assert.assertEquals(java.util.Set.of("a", "b", "c", "d"), q1);

                    // Query 2 (flag): the reset is applied before compilation, then
                    // the SELECT fails to compile (missing table). The dict is
                    // cleared and the reset staged, but no CACHE_RESET is emitted.
                    boolean[] errored = {false};
                    byte[] status = {0};
                    client.execute("SELECT sym FROM flag_fail_missing", expectError(errored, status), true);
                    Assert.assertTrue("the flagged query must fail to compile", errored[0]);
                    Assert.assertEquals("a missing table is a SqlException => PARSE_ERROR",
                            QwpConstants.STATUS_PARSE_ERROR, status[0]);
                    Assert.assertEquals("the flag must clear the dict and count one reset even on error",
                            resetsBefore + 1, metrics.cacheResetDictCount());

                    // Query 3 (no flag): the deferred CACHE_RESET must land before
                    // this query's first batch, so the client resolves everything.
                    java.util.Set<String> q3 = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM flag_fail", collectInto(q3));
                    Assert.assertEquals(java.util.Set.of("a", "b", "c", "d"), q3);
                    Assert.assertEquals("query 3 must not fire its own reset",
                            resetsBefore + 1, metrics.cacheResetDictCount());
                }
            }
        });
    }

    /**
     * Deferred emit: a non-SELECT carrying the flag clears the server dict
     * immediately but returns via EXEC_DONE without streaming, so the staged
     * {@code CACHE_RESET} surfaces on the next result-producing query. The
     * follow-up SELECT must still resolve every symbol, proving the deferred
     * frame restored client/server lockstep. This is the trickiest path in the
     * change.
     */
    @Test
    public void testResetFlagOnNonSelectDefersToNextSelect() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented("QDB_METRICS_ENABLED", "true")) {
                serverMain.execute("CREATE TABLE flag_defer(sym SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO flag_defer VALUES
                            ('a', 1::TIMESTAMP), ('b', 2::TIMESTAMP),
                            ('c', 3::TIMESTAMP), ('d', 4::TIMESTAMP)
                        """);
                serverMain.awaitTable("flag_defer");
                QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                long resetsBefore = metrics.cacheResetDictCount();

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();

                    // Query 1 (no flag): grows the dict to {a,b,c,d}.
                    java.util.Set<String> q1 = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM flag_defer", collectInto(q1));
                    Assert.assertEquals(java.util.Set.of("a", "b", "c", "d"), q1);

                    // Query 2 (non-SELECT, flag): clears the dict and stages the
                    // reset, but EXEC_DONE returns before any CACHE_RESET is sent.
                    // Inserts a duplicate symbol so query 3's symbol set is unchanged.
                    boolean[] execDone = {false};
                    client.execute("INSERT INTO flag_defer VALUES ('a', 5::TIMESTAMP)", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.fail("non-SELECT must not stream batches");
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.fail("non-SELECT must complete via onExecDone");
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("insert must succeed: " + message);
                        }

                        @Override
                        public void onExecDone(short opType, long rowsAffected) {
                            execDone[0] = true;
                        }
                    }, true);
                    Assert.assertTrue("non-SELECT must complete", execDone[0]);
                    Assert.assertEquals("the non-SELECT flag must clear the dict and count one reset",
                            resetsBefore + 1, metrics.cacheResetDictCount());

                    // Query 3 (no flag): the deferred CACHE_RESET must land before
                    // this query's first batch, so the client resolves everything.
                    java.util.Set<String> q3 = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM flag_defer", collectInto(q3));
                    Assert.assertEquals(java.util.Set.of("a", "b", "c", "d"), q3);
                    Assert.assertEquals("query 3 must not fire its own reset",
                            resetsBefore + 1, metrics.cacheResetDictCount());
                }
            }
        });
    }

    /**
     * Two flagged non-SELECTs back to back: the first clears the (non-empty)
     * dict and counts one reset; the second runs against the now-empty dict, so
     * the empty-dict guard makes it a no-op that counts no further reset. A
     * final SELECT confirms the single deferred {@code CACHE_RESET} still
     * restored client/server lockstep.
     */
    @Test
    public void testResetFlagOnRepeatedNonSelectsCountsOnce() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented("QDB_METRICS_ENABLED", "true")) {
                serverMain.execute("CREATE TABLE flag_twice(sym SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO flag_twice VALUES ('a', 1::TIMESTAMP), ('b', 2::TIMESTAMP)");
                serverMain.awaitTable("flag_twice");
                QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                long resetsBefore = metrics.cacheResetDictCount();

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();

                    // Query 1 (no flag): grows the dict to {a,b}.
                    java.util.Set<String> q1 = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM flag_twice", collectInto(q1));
                    Assert.assertEquals(java.util.Set.of("a", "b"), q1);

                    // Query 2 (non-SELECT, flag): clears the non-empty dict and
                    // counts one reset. The duplicate symbol keeps the set unchanged.
                    boolean[] done2 = {false};
                    client.execute("INSERT INTO flag_twice VALUES ('a', 3::TIMESTAMP)", expectExecDone(done2), true);
                    Assert.assertTrue("first non-SELECT must complete", done2[0]);
                    Assert.assertEquals("first flagged non-SELECT clears the dict and counts one reset",
                            resetsBefore + 1, metrics.cacheResetDictCount());

                    // Query 3 (non-SELECT, flag): the dict is already empty (a
                    // non-SELECT never repopulates it), so the empty-dict guard
                    // makes the flag a no-op -- no second reset is counted.
                    boolean[] done3 = {false};
                    client.execute("INSERT INTO flag_twice VALUES ('b', 4::TIMESTAMP)", expectExecDone(done3), true);
                    Assert.assertTrue("second non-SELECT must complete", done3[0]);
                    Assert.assertEquals("the second back-to-back flagged non-SELECT must not double-count",
                            resetsBefore + 1, metrics.cacheResetDictCount());

                    // Query 4 (no flag): the single deferred CACHE_RESET staged by
                    // query 2 surfaces here, so the client still resolves everything.
                    java.util.Set<String> q4 = new java.util.HashSet<>();
                    client.execute("SELECT sym FROM flag_twice", collectInto(q4));
                    Assert.assertEquals(java.util.Set.of("a", "b"), q4);
                    Assert.assertEquals("query 4 must not fire its own reset",
                            resetsBefore + 1, metrics.cacheResetDictCount());
                }
            }
        });
    }

    /**
     * Per-query scoping over a run of flagged SELECTs: the first (empty-dict)
     * query is a no-op, then every subsequent flagged query resets the dict the
     * previous one left behind. Each query resolves its own symbols.
     */
    @Test
    public void testResetFlagScopesEachQuery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented("QDB_METRICS_ENABLED", "true")) {
                serverMain.execute("CREATE TABLE flag_scope(sym SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO flag_scope VALUES ('a', 1::TIMESTAMP), ('b', 2::TIMESTAMP)");
                serverMain.awaitTable("flag_scope");
                QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                long resetsBefore = metrics.cacheResetDictCount();

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    final int runs = 4;
                    for (int i = 0; i < runs; i++) {
                        java.util.Set<String> got = new java.util.HashSet<>();
                        client.execute("SELECT sym FROM flag_scope", collectInto(got), true);
                        Assert.assertEquals(java.util.Set.of("a", "b"), got);
                    }
                    // First run starts empty (no reset); each later run resets the
                    // dict the prior run repopulated.
                    Assert.assertEquals("one reset per flagged query after the first",
                            resetsBefore + (runs - 1), metrics.cacheResetDictCount());
                }
            }
        });
    }

    /**
     * msg_kind(1) + request_id(8 LE) + sql_len(varint) + sql + initial_credit(varint)
     * + bind_count(varint) + a malformed query_flags trailer. The header is well
     * formed so the processor seeds the request id; only the trailer is broken.
     */
    private static byte[] buildMalformedQueryRequest(long requestId) {
        byte[] sql = "SELECT 1".getBytes(StandardCharsets.UTF_8);
        // sql_len, initial_credit and bind_count are all < 128, so each is a
        // single-byte varint -- no general encoder needed.
        byte[] p = new byte[1 + 8 + 1 + sql.length + 1 + 1 + 1];
        int i = 0;
        p[i++] = QwpEgressMsgKind.QUERY_REQUEST;
        for (int s = 0; s < 8; s++) {
            p[i++] = (byte) (requestId >>> (8 * s)); // little-endian, matches Unsafe.getLong
        }
        p[i++] = (byte) sql.length; // sql_len
        System.arraycopy(sql, 0, p, i, sql.length);
        i += sql.length;
        p[i++] = 0x00; // initial_credit = 0
        p[i++] = 0x00; // bind_count = 0
        p[i] = (byte) 0x80; // malformed query_flags: continuation byte with no successor
        return p;
    }

    private static QwpColumnBatchHandler collectInto(java.util.Set<String> sink) {
        return new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                for (int r = 0; r < batch.getRowCount(); r++) {
                    DirectUtf8Sequence v = batch.getStrA(0, r);
                    if (v != null) {
                        sink.add(v.toString());
                    }
                }
            }

            @Override
            public void onEnd(long total) {
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("query must succeed: " + message);
            }
        };
    }

    /**
     * A handler that expects the query to fail: it records that {@code onError}
     * fired and the status byte, and fails the test if the query streams or
     * completes instead.
     */
    private static QwpColumnBatchHandler expectError(boolean[] erroredSink, byte[] statusSink) {
        return new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                Assert.fail("a failing query must not stream batches");
            }

            @Override
            public void onEnd(long totalRows) {
                Assert.fail("a failing query must not complete via onEnd");
            }

            @Override
            public void onError(byte status, String message) {
                erroredSink[0] = true;
                statusSink[0] = status;
            }

            @Override
            public void onExecDone(short opType, long rowsAffected) {
                Assert.fail("a failing query must not complete via onExecDone");
            }
        };
    }

    /**
     * A handler that expects a non-SELECT to complete via EXEC_DONE: it records
     * the completion and fails the test if the statement streams or errors.
     */
    private static QwpColumnBatchHandler expectExecDone(boolean[] doneSink) {
        return new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                Assert.fail("non-SELECT must not stream batches");
            }

            @Override
            public void onEnd(long totalRows) {
                Assert.fail("non-SELECT must complete via onExecDone");
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("statement must succeed: " + message);
            }

            @Override
            public void onExecDone(short opType, long rowsAffected) {
                doneSink[0] = true;
            }
        };
    }

    /**
     * Wraps {@code payload} in a masked client-to-server BINARY frame (FIN set).
     * Client frames must be masked per RFC 6455.
     */
    private static byte[] maskedBinaryFrame(byte[] payload) {
        byte[] maskKey = {0x12, 0x34, 0x56, 0x78};
        int payloadLen = payload.length;
        int headerLen = (payloadLen <= 125) ? 6 : (payloadLen <= 65_535) ? 8 : 14;
        byte[] frame = new byte[headerLen + payloadLen];
        int offset = 0;
        frame[offset++] = (byte) (0x80 | (WebSocketOpcode.BINARY & 0x0F));
        if (payloadLen <= 125) {
            frame[offset++] = (byte) (0x80 | payloadLen);
        } else if (payloadLen <= 65_535) {
            frame[offset++] = (byte) (0x80 | 126);
            frame[offset++] = (byte) ((payloadLen >> 8) & 0xFF);
            frame[offset++] = (byte) (payloadLen & 0xFF);
        } else {
            frame[offset++] = (byte) (0x80 | 127);
            for (int b = 7; b >= 0; b--) {
                frame[offset++] = (byte) (((long) payloadLen >> (b * 8)) & 0xFF);
            }
        }
        System.arraycopy(maskKey, 0, frame, offset, 4);
        offset += 4;
        for (int b = 0; b < payloadLen; b++) {
            frame[offset + b] = (byte) (payload[b] ^ maskKey[b % 4]);
        }
        return frame;
    }

    /**
     * Performs the WebSocket upgrade against the egress read endpoint and reads
     * exactly up to the {@code \r\n\r\n} header boundary, leaving any pushed QWP
     * frames (SERVER_INFO first) unconsumed in the stream.
     */
    private static void performReadHandshake(Socket socket) throws Exception {
        OutputStream out = socket.getOutputStream();
        InputStream in = socket.getInputStream();

        byte[] keyBytes = new byte[16];
        for (int i = 0; i < 16; i++) {
            keyBytes[i] = (byte) (i + 1);
        }
        String wsKey = Base64.getEncoder().encodeToString(keyBytes);

        String request = "GET /read/v1 HTTP/1.1\r\n" +
                "Host: localhost\r\n" +
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
            Assert.assertNotEquals("Unexpected end of stream during handshake", -1, b);
            response.append((char) b);
            int len = response.length();
            if (len >= 4
                    && response.charAt(len - 4) == '\r' && response.charAt(len - 3) == '\n'
                    && response.charAt(len - 2) == '\r' && response.charAt(len - 1) == '\n') {
                break;
            }
        }
        Assert.assertTrue(
                "Expected 101 Switching Protocols, got: " + response.toString().split("\r\n")[0],
                response.toString().startsWith("HTTP/1.1 101")
        );
    }

    private static byte[] readBinaryFrame(InputStream in) throws Exception {
        int b0 = readByte(in);
        int opcode = b0 & 0x0F;
        Assert.assertEquals("server must reply with a BINARY frame, not opcode 0x" + Integer.toHexString(opcode),
                WebSocketOpcode.BINARY, opcode);
        int b1 = readByte(in);
        Assert.assertEquals("server frames must not be masked", 0, b1 & 0x80);
        int payloadLen = b1 & 0x7F;
        if (payloadLen == 126) {
            payloadLen = (readByte(in) << 8) | readByte(in);
        } else if (payloadLen == 127) {
            long extended = 0;
            for (int i = 0; i < 8; i++) {
                extended = (extended << 8) | readByte(in);
            }
            payloadLen = (int) extended;
        }
        byte[] payload = new byte[payloadLen];
        int read = 0;
        while (read < payloadLen) {
            int n = in.read(payload, read, payloadLen - read);
            Assert.assertNotEquals("Unexpected end of stream while reading frame payload", -1, n);
            read += n;
        }
        return payload;
    }

    private static int readByte(InputStream in) throws Exception {
        int b = in.read();
        Assert.assertNotEquals("Unexpected end of stream", -1, b);
        return b & 0xFF;
    }

    /**
     * Reads binary WebSocket frames until one whose QWP msg_kind (the byte at
     * {@link QwpConstants#HEADER_SIZE}) equals {@code kind}, skipping earlier
     * frames such as the unsolicited SERVER_INFO. Fails if not found promptly.
     */
    private static byte[] readFrameUntilKind(InputStream in, byte kind) throws Exception {
        for (int attempt = 0; attempt < 8; attempt++) {
            byte[] payload = readBinaryFrame(in);
            if (payload.length > QwpConstants.HEADER_SIZE && payload[QwpConstants.HEADER_SIZE] == kind) {
                return payload;
            }
        }
        Assert.fail("did not receive a frame with msg_kind 0x" + Integer.toHexString(kind & 0xFF));
        return null; // unreachable
    }

    private static long readLongLE(byte[] buf, int offset) {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v |= (long) (buf[offset + i] & 0xFF) << (8 * i);
        }
        return v;
    }
}
