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
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.cutlass.http.HttpException;
import io.questdb.cutlass.qwp.codec.QwpEgressFrameWriter;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.cutlass.qwp.server.egress.QwpEgressUpgradeProcessor;
import io.questdb.cutlass.qwp.server.egress.QwpRowExceedsBufferException;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlException;
import io.questdb.jit.JitUtil;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * End-to-end coverage of the query-details flags of {@code QUERY_REQUEST}:
 * {@code QUERY_FLAG_ERROR_POSITION}, {@code QUERY_FLAG_STATS},
 * {@code QUERY_FLAG_COUNT_ROWS}, {@code QUERY_FLAG_SKIP_ROWS} and
 * {@code QUERY_FLAG_MAX_ROWS}, which give a QWP client what /exec reports: the
 * error position, the timings and JIT use, and the size of a result it pages
 * through.
 * <p>
 * The tests drive the raw wire and decode the server's frames here, so they pin
 * the bytes of each trailer and also send requests a correct client never
 * sends, such as one cut short; {@link QwpEgressQueryDetailsClientTest} sends
 * the same flags through the pinned {@code java-questdb-client}. What they
 * assert is the server's output, checked against independent oracles: /exec on
 * the same server for positions, row counts and JIT use, and SQL {@code LIMIT}
 * and {@code count(*)} through the pinned client for row windows. The servers
 * run with the randomised send and receive fragmentation of
 * {@link AbstractQwpBootstrapTest}, so the trailers also cross parked sends.
 */
public class QwpEgressQueryDetailsWireTest extends AbstractQwpQueryDetailsTest {
    private static final int DETAILS = QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION
            | QwpEgressMsgKind.QUERY_FLAG_STATS | QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS;
    private static final int WINDOW = QwpEgressMsgKind.QUERY_FLAG_SKIP_ROWS | QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS;

    @Test
    public void testCancelledQueryReportsNoPosition() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startFragmented();
                 WireConnection conn = new WireConnection(maxBatchRows(7))) {
                // One byte of credit lets one batch out, then the stream waits for
                // CREDIT; the CANCEL lands while it waits and takes effect when the
                // CREDIT resumes it.
                conn.send(QwpWireTestFixtures.buildQueryRequest(5, "SELECT x FROM long_sequence(1000)", 1, QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION, 0, 0));
                byte[] first = conn.readFrame();
                QwpWireTestFixtures.assertQwpMessageKind(first, QwpEgressMsgKind.RESULT_BATCH);
                conn.send(cancelFrame(5));
                conn.send(QwpWireTestFixtures.buildCreditFrame(5, 1 << 20));
                Response r = conn.readResponse(5);
                QueryError error = r.queryError();
                Assert.assertEquals(QwpConstants.STATUS_CANCELLED, error.status);
                Assert.assertEquals("cancelled by client", error.message);
                Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, error.readPosition());
                error.assertEnd();
            }
        });
    }

    @Test
    public void testCapabilityAdvertised() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startFragmented();
                 WireConnection conn = new WireConnection("")) {
                WireReader rd = new WireReader(conn.serverInfo, QwpConstants.HEADER_SIZE + 1 + 1 + 8);
                int capabilities = rd.i32();
                Assert.assertEquals(QwpEgressMsgKind.CAP_QUERY_DETAILS, capabilities & QwpEgressMsgKind.CAP_QUERY_DETAILS);
                Assert.assertEquals(QwpEgressMsgKind.CAP_QUERY_FLAGS, capabilities & QwpEgressMsgKind.CAP_QUERY_FLAGS);
                // released clients would send this server their per-query timeouts
                Assert.assertEquals(0, capabilities & QwpEgressMsgKind.CAP_QUERY_TIMEOUT);
            }
        });
    }

    @Test
    public void testCountAbortedByQueryTimeout() throws Exception {
        // Counting reads the rows the window leaves out, under the query's
        // circuit breaker. A count that outlasts query.timeout ends the query with
        // QUERY_ERROR, before the rows of the window go out, and leaves the
        // connection usable.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startFragmented(
                    PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "500ms",
                    PropertyKey.HTTP_FROZEN_CLOCK.getEnvVarName(), "false"
            );
                 WireConnection conn = new WireConnection("")) {
                // the filter hides the size of the result, so it has to be counted
                Response r = conn.query(1, "SELECT x FROM long_sequence(1_000_000_000_000) WHERE x % 2 = 0",
                        QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION | QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS
                                | QwpEgressMsgKind.QUERY_FLAG_STATS | QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS, 0, 5);
                Assert.assertEquals("the window rows must not go out", 0, r.batches);
                QueryError error = r.queryError();
                Assert.assertEquals(QwpConstants.STATUS_LIMIT_EXCEEDED, error.status);
                TestUtils.assertContains(error.message, "timeout, query aborted");
                Assert.assertEquals(0, error.readPosition());
                error.assertEnd();

                Response next = conn.query(2, "SELECT x FROM long_sequence(3)", QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS
                        | QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS, 0, 2);
                assertSequence(next.longs, 1, 2);
                ResultEnd end = next.resultEnd();
                Assert.assertEquals(3, end.readRowCount());
                end.assertEnd();
            }
        });
    }

    @Test
    public void testCountWithoutWindowEqualsStreamedRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startFragmented();
                 WireConnection conn = new WireConnection("")) {
                // 40_000 rows span three batches of the default 16_384-row cap
                for (long n : new long[]{0, 1, 1_000, 40_000}) {
                    Response r = conn.query(1, "SELECT x FROM long_sequence(" + n + ")", QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS, 0, 0);
                    assertSequence(r.longs, 1, n);
                    ResultEnd end = r.resultEnd();
                    Assert.assertEquals(n, end.totalRows);
                    Assert.assertEquals(n, end.readRowCount());
                    end.assertEnd();
                }
            }
        });
    }

    @Test
    public void testDetailsDoNotLeakToNextQuery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented();
                 WireConnection conn = new WireConnection("")) {
                createWindowTable(serverMain, 100);
                Response windowed = conn.query(1, "SELECT x FROM qd_window", DETAILS | WINDOW, 10, 5);
                assertSequence(windowed.longs, 10, 5);
                ResultEnd end = windowed.resultEnd();
                Assert.assertEquals(100, end.readRowCount());
                end.readStats();
                end.assertEnd();

                // the same SQL with no flags: every row, and a baseline RESULT_END
                Response plain = conn.query(2, "SELECT x FROM qd_window", 0, 0, 0);
                assertSequence(plain.longs, 0, 100);
                ResultEnd plainEnd = plain.resultEnd();
                Assert.assertEquals(100, plainEnd.totalRows);
                plainEnd.assertEnd();

                Response failed = conn.query(3, "SELECT x FROM qd_missing", DETAILS, 0, 0);
                Assert.assertEquals(14, failed.queryError().readPosition());
                Response baselineError = conn.query(4, "SELECT x FROM qd_missing", 0, 0, 0);
                QueryError error = baselineError.queryError();
                Assert.assertEquals("[14] table does not exist [table=qd_missing]", error.message);
                error.assertEnd();

                Response done = conn.query(5, "INSERT INTO qd_window VALUES (100, 100::TIMESTAMP)", 0, 0, 0);
                ExecDone execDone = done.execDone();
                Assert.assertEquals(1, execDone.rowsAffected);
                execDone.assertEnd();
            }
        });
    }

    @Test
    public void testEmptyQueryIsExecDoneOfTypeEmpty() throws Exception {
        // /exec answers an empty query with an "empty query" notice; egress ends
        // it with EXEC_DONE whose op_type is EMPTY.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startFragmented(PropertyKey.HTTP_FROZEN_CLOCK.getEnvVarName(), "true");
                 WireConnection conn = new WireConnection("")) {
                for (String sql : new String[]{"", "   ", "-- only a comment", "/* only a block comment */"}) {
                    ExecDone plain = conn.query(1, sql, 0, 0, 0).execDone();
                    Assert.assertEquals(sql, CompiledQuery.EMPTY, plain.opType);
                    Assert.assertEquals(0, plain.rowsAffected);
                    plain.assertEnd();

                    ExecDone detailed = conn.query(2, sql, DETAILS | WINDOW, 1, 1).execDone();
                    Assert.assertEquals(sql, CompiledQuery.EMPTY, detailed.opType);
                    detailed.readStats().assertFrozen(false);
                    detailed.assertEnd();
                }
            }
        });
    }

    @Test
    public void testErrorMessageByErrorType() {
        // the bare message drops the [n] prefix getMessage() adds
        final SqlException sqlException = SqlException.$(14, "table does not exist [table=t]");
        TestUtils.assertEquals("table does not exist [table=t]", QwpEgressUpgradeProcessor.errorMessage(sqlException, true));
        TestUtils.assertEquals("[14] table does not exist [table=t]", QwpEgressUpgradeProcessor.errorMessage(sqlException, false));
        final CairoException cairoException = CairoException.critical(2).put("could not open");
        TestUtils.assertEquals("could not open", QwpEgressUpgradeProcessor.errorMessage(cairoException, true));
        TestUtils.assertEquals("[2] could not open", QwpEgressUpgradeProcessor.errorMessage(cairoException, false));
        // an error without the flyweight message keeps getMessage(), or its class name
        TestUtils.assertEquals("boom", QwpEgressUpgradeProcessor.errorMessage(new IllegalStateException("boom"), true));
        TestUtils.assertEquals("boom", QwpEgressUpgradeProcessor.errorMessage(new IllegalStateException("boom"), false));
        TestUtils.assertEquals("NullPointerException", QwpEgressUpgradeProcessor.errorMessage(new NullPointerException(), true));
        TestUtils.assertEquals("NullPointerException", QwpEgressUpgradeProcessor.errorMessage(new NullPointerException(), false));
        // a flyweight container whose message is null
        final CairoError noMessage = new CairoError((Throwable) null);
        TestUtils.assertEquals("CairoError", QwpEgressUpgradeProcessor.errorMessage(noMessage, true));
        TestUtils.assertEquals("CairoError", QwpEgressUpgradeProcessor.errorMessage(noMessage, false));
    }

    @Test
    public void testErrorOnResumeThroughCreditReportsPosition() throws Exception {
        // An error after the first batch, while the stream resumes on CREDIT,
        // ends the query through handleCredit's error path. A send chunk that
        // takes the 7-row batch and the error in one piece keeps every send of
        // the test from parking, whatever the seed.
        sendChunk = 4_096;
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startFragmented();
                 WireConnection conn = new WireConnection(maxBatchRows(7))) {
                for (int flagged = 0; flagged < 2; flagged++) {
                    final long requestId = 10 + flagged;
                    conn.send(QwpWireTestFixtures.buildQueryRequest(requestId, "SELECT x FROM long_sequence(100)", 1,
                            flagged == 1 ? QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION : 0, 0, 0));
                    final Response r = new Response();
                    final int batchBytes = conn.readBatch(r, requestId);
                    QwpEgressUpgradeProcessor.DEBUG_FORCE_INTERNAL_ERROR_ON_RESUME = 1;
                    try {
                        conn.send(QwpWireTestFixtures.buildCreditFrame(requestId, batchBytes));
                        conn.readResponse(requestId, r);
                    } finally {
                        QwpEgressUpgradeProcessor.DEBUG_FORCE_INTERNAL_ERROR_ON_RESUME = 0;
                    }
                    Assert.assertEquals(1, r.batches);
                    final QueryError error = r.queryError();
                    Assert.assertEquals(QwpConstants.STATUS_INTERNAL_ERROR, error.status);
                    if (flagged == 1) {
                        Assert.assertEquals("synthetic internal error on resume (DEBUG_FORCE_INTERNAL_ERROR_ON_RESUME)", error.message);
                        Assert.assertEquals(0, error.readPosition());
                    } else {
                        Assert.assertEquals("[0] synthetic internal error on resume (DEBUG_FORCE_INTERNAL_ERROR_ON_RESUME)", error.message);
                    }
                    error.assertEnd();
                }
            }
        });
    }

    @Test
    public void testErrorOnResumeThroughParkedSendReportsPosition() throws Exception {
        // A 100-row batch, some 830 bytes, overflows a 256-byte send chunk, so
        // its send parks and the stream resumes through resumeSend, where the
        // next batch fails. The error frame, some 110 bytes, goes out whole.
        sendChunk = 256;
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startFragmented();
                 WireConnection conn = new WireConnection(maxBatchRows(100))) {
                for (int flagged = 0; flagged < 2; flagged++) {
                    final long requestId = 20 + flagged;
                    QwpEgressUpgradeProcessor.DEBUG_FORCE_INTERNAL_ERROR_ON_RESUME = 1;
                    try {
                        conn.send(QwpWireTestFixtures.buildQueryRequest(requestId, "SELECT x FROM long_sequence(1000)", 0,
                                flagged == 1 ? QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION : 0, 0, 0));
                        final Response r = conn.readResponse(requestId);
                        Assert.assertEquals(1, r.batches);
                        assertSequence(r.longs, 1, 100);
                        final QueryError error = r.queryError();
                        Assert.assertEquals(QwpConstants.STATUS_INTERNAL_ERROR, error.status);
                        if (flagged == 1) {
                            Assert.assertEquals("synthetic internal error on resume (DEBUG_FORCE_INTERNAL_ERROR_ON_RESUME)", error.message);
                            Assert.assertEquals(0, error.readPosition());
                        } else {
                            Assert.assertEquals("[0] synthetic internal error on resume (DEBUG_FORCE_INTERNAL_ERROR_ON_RESUME)", error.message);
                        }
                        error.assertEnd();
                    } finally {
                        QwpEgressUpgradeProcessor.DEBUG_FORCE_INTERNAL_ERROR_ON_RESUME = 0;
                    }
                }
            }
        });
    }

    @Test
    public void testErrorPositionByErrorType() {
        // the SQL engine's errors carry a position into the SQL text, as on /exec
        Assert.assertEquals(17, QwpEgressUpgradeProcessor.errorPosition(SqlException.$(17, "bad sql")));
        Assert.assertEquals(9, QwpEgressUpgradeProcessor.errorPosition(ImplicitCastException.instance().position(9)));
        Assert.assertEquals(4, QwpEgressUpgradeProcessor.errorPosition(CairoException.nonCritical().position(4).put("x")));
        // a CairoException that names no position reports 0, as /exec does
        Assert.assertEquals(0, QwpEgressUpgradeProcessor.errorPosition(CairoException.queryTimedOut()));
        Assert.assertEquals(0, QwpEgressUpgradeProcessor.errorPosition(new CairoError("broken")));
        // errors that do not come from the SQL text point nowhere
        Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, QwpEgressUpgradeProcessor.errorPosition(
                QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("truncated")));
        Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, QwpEgressUpgradeProcessor.errorPosition(
                QwpRowExceedsBufferException.instance(3, 1_024, 1, false)));
        Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, QwpEgressUpgradeProcessor.errorPosition(
                HttpException.instance("egress send buffer too small")));
        Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, QwpEgressUpgradeProcessor.errorPosition(
                new NullPointerException()));
        Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, QwpEgressUpgradeProcessor.errorPosition(
                new OutOfMemoryError()));
    }

    @Test
    public void testErrorPositionIsUtf16Offset() throws Exception {
        // The position counts UTF-16 code units, the unit of a JavaScript string
        // index: 'é' and '€' are one unit each but 2 and 3 UTF-8 bytes, and the
        // emoji is two units and 4 bytes.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startFragmented();
                 WireConnection conn = new WireConnection("")) {
                final String sql = "SELECT 'é€\uD83D\uDE00' s FROM qd_missing";
                QueryError error = conn.query(1, sql, QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION, 0, 0).queryError();
                Assert.assertEquals("table does not exist [table=qd_missing]", error.message);
                Assert.assertEquals(sql.indexOf("qd_missing"), error.readPosition());
                // 21 UTF-16 units precede the table name, against 26 UTF-8 bytes
                Assert.assertEquals(21, sql.indexOf("qd_missing"));
                Assert.assertEquals(26, sql.substring(0, 21).getBytes(StandardCharsets.UTF_8).length);
                Assert.assertEquals(execPosition(sql), sql.indexOf("qd_missing"));
                error.assertEnd();
            }
        });
    }

    @Test
    public void testErrorPositionMatchesExec() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented();
                 WireConnection conn = new WireConnection("")) {
                createWindowTable(serverMain, 10);
                serverMain.execute("CREATE TABLE qd_arr (a DOUBLE[], b DOUBLE[])");
                serverMain.execute("INSERT INTO qd_arr VALUES (ARRAY[1.0, 2.0], ARRAY[1.0, 2.0, 3.0])");
                // {sql, status, bare message, position}
                final Object[][] cases = {
                        // compile errors
                        {"SELECT x FROM qd_missing", QwpConstants.STATUS_PARSE_ERROR,
                                "table does not exist [table=qd_missing]", 14},
                        {"SELECT nope FROM qd_window", QwpConstants.STATUS_PARSE_ERROR,
                                "Invalid column: nope", 7},
                        {"SELECT x FROM qd_window WHERE", QwpConstants.STATUS_PARSE_ERROR,
                                "empty where clause", 24},
                        // a non-SELECT: the DROP fails at execution
                        {"DROP TABLE qd_missing", QwpConstants.STATUS_PARSE_ERROR,
                                "table does not exist [table=qd_missing]", 11},
                        // a runtime error raised while the rows stream
                        {"SELECT a + b FROM qd_arr", QwpConstants.STATUS_INTERNAL_ERROR,
                                "arrays have incompatible shapes [leftShape=[2], rightShape=[3]]", 7},
                };
                long requestId = 1;
                for (Object[] c : cases) {
                    final String sql = (String) c[0];
                    QueryError error = conn.query(requestId++, sql, QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION, 0, 0).queryError();
                    Assert.assertEquals(sql, (byte) c[1], error.status);
                    Assert.assertEquals(sql, c[2], error.message);
                    final int position = error.readPosition();
                    Assert.assertEquals(sql, (int) c[3], position);
                    Assert.assertEquals(sql, execPosition(sql), position);
                    error.assertEnd();
                }
            }
        });
    }

    @Test
    public void testErrorPositionOfCopyFromStdin() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented();
                 WireConnection conn = new WireConnection("")) {
                createWindowTable(serverMain, 1);
                QueryError error = conn.query(1, "COPY qd_window FROM stdin", QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION, 0, 0).queryError();
                Assert.assertEquals(QwpConstants.STATUS_PARSE_ERROR, error.status);
                Assert.assertEquals("COPY ... FROM is not supported on egress", error.message);
                Assert.assertEquals(0, error.readPosition());
                error.assertEnd();

                QueryError baseline = conn.query(2, "COPY qd_window FROM stdin", 0, 0, 0).queryError();
                Assert.assertEquals("COPY ... FROM is not supported on egress", baseline.message);
                baseline.assertEnd();
            }
        });
    }

    @Test
    public void testJitCompiledMatchesExec() throws Exception {
        assertJitCompiled(true);
    }

    @Test
    public void testJitCompiledWhenJitDisabled() throws Exception {
        assertJitCompiled(false);
    }

    @Test
    public void testQueryErrorPositionFollowsTruncatedMessage() throws Exception {
        // The message is cut at a character boundary to fit its cap; the position
        // still follows it, in the 4 bytes the caller reserves past the cap.
        TestUtils.assertMemoryLeak(() -> {
            final int cap = 64;
            final long buf = Unsafe.malloc(cap, MemoryTag.NATIVE_DEFAULT);
            try {
                final String msg = "x".repeat(40) + "\u20AC".repeat(10); // 40 + 30 bytes
                for (int position : new int[]{QwpEgressMsgKind.NO_ERROR_POSITION, 0, 1, Integer.MAX_VALUE}) {
                    final int msgCap = cap - 12 - Integer.BYTES;
                    final long end = QwpEgressFrameWriter.writeQueryError(buf, 42, QwpConstants.STATUS_PARSE_ERROR, msg, msgCap, position);
                    final byte[] body = new byte[(int) (end - buf)];
                    for (int i = 0; i < body.length; i++) {
                        body[i] = Unsafe.getByte(buf + i);
                    }
                    // msg_kind, request_id, status and msg_len, 46 message bytes, the position
                    Assert.assertEquals(12 + 46 + Integer.BYTES, body.length);
                    Assert.assertEquals(QwpEgressMsgKind.QUERY_ERROR, body[0]);
                    final WireReader rd = new WireReader(body, 1);
                    Assert.assertEquals(42, rd.i64());
                    Assert.assertEquals(QwpConstants.STATUS_PARSE_ERROR, rd.u8());
                    // 48 bytes of cap: the 40 ASCII bytes and two whole 3-byte euro signs
                    Assert.assertEquals("x".repeat(40) + "\u20AC\u20AC", rd.utf8(rd.u16()));
                    Assert.assertEquals(position, rd.i32());
                    Assert.assertEquals(0, rd.remaining());
                }
            } finally {
                Unsafe.free(buf, cap, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testQueryStatsBlockEncoding() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int cap = QwpEgressFrameWriter.QUERY_STATS_MAX_BYTES;
            final long buf = Unsafe.malloc(cap, MemoryTag.NATIVE_DEFAULT);
            try {
                final long[] values = {0, 1, 127, 128, 16_383, 16_384, 1_000_000_000L, Long.MAX_VALUE};
                for (long v : values) {
                    for (boolean isJit : new boolean[]{false, true}) {
                        final long end = QwpEgressFrameWriter.writeQueryStats(buf, v, Long.MAX_VALUE - v, v / 2, v / 3, isJit);
                        Assert.assertTrue("stats block overruns its bound", end - buf <= cap);
                        final byte[] block = new byte[(int) (end - buf)];
                        for (int i = 0; i < block.length; i++) {
                            block[i] = Unsafe.getByte(buf + i);
                        }
                        final WireReader rd = new WireReader(block, 0);
                        Assert.assertEquals(v, rd.varint());
                        Assert.assertEquals(Long.MAX_VALUE - v, rd.varint());
                        Assert.assertEquals(v / 2, rd.varint());
                        Assert.assertEquals(v / 3, rd.varint());
                        Assert.assertEquals(isJit ? QwpEgressMsgKind.STATS_FLAG_JIT_COMPILED : 0, rd.u8());
                        Assert.assertEquals(0, rd.remaining());
                    }
                }
            } finally {
                Unsafe.free(buf, cap, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRejectedSecondRequestLeavesStreamingQueryDetails() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented(PropertyKey.HTTP_FROZEN_CLOCK.getEnvVarName(), "false");
                 WireConnection conn = new WireConnection(maxBatchRows(7))) {
                createWindowTable(serverMain, 100);
                // the first query streams one batch, then waits for credit
                conn.send(QwpWireTestFixtures.buildQueryRequest(1, "SELECT x FROM qd_window", 1, DETAILS | WINDOW, 5, 50));
                Response r = new Response();
                conn.readBatch(r, 1);
                Assert.assertEquals(7, r.longs.size());

                // a second request while it waits: rejected with a baseline frame,
                // although it asked for the position, as its flags are never read
                conn.send(QwpWireTestFixtures.buildQueryRequest(2, "SELECT x FROM qd_missing", 0, DETAILS, 0, 0));
                QueryError rejected = conn.readResponse(2).queryError();
                Assert.assertEquals(QwpConstants.STATUS_PARSE_ERROR, rejected.status);
                Assert.assertEquals("Phase 1 egress supports a single in-flight query per connection", rejected.message);
                rejected.assertEnd();

                // the first query resumes with its own window, count and statistics
                conn.send(QwpWireTestFixtures.buildCreditFrame(1, 1 << 20));
                conn.readResponse(1, r);
                assertSequence(r.longs, 5, 50);
                ResultEnd end = r.resultEnd();
                Assert.assertEquals(50, end.totalRows);
                Assert.assertEquals(100, end.readRowCount());
                Stats stats = end.readStats();
                Assert.assertTrue(stats.toString(), stats.clientWaitNanos > 0);
                end.assertEnd();
            }
        });
    }

    @Test
    public void testRequestThatFailsToDecodeGetsBaselineError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startFragmented();
                 WireConnection conn = new WireConnection("")) {
                // the flags ask for the position, but skip_rows is cut short, so
                // the request does not decode and its flags are not applied
                byte[] request = QwpWireTestFixtures.buildQueryRequest(7, "SELECT 1", 0, QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION, 0, 0);
                request = Arrays.copyOf(request, request.length + 1);
                request[request.length - 2] = (byte) (QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION | QwpEgressMsgKind.QUERY_FLAG_SKIP_ROWS);
                request[request.length - 1] = (byte) 0x80;
                conn.send(request);
                QueryError error = conn.readResponse(7).queryError();
                Assert.assertEquals(QwpConstants.STATUS_PARSE_ERROR, error.status);
                Assert.assertEquals("incomplete varint: buffer underflow", error.message);
                error.assertEnd();

                // the connection stays usable, and the next request's flags apply
                QueryError next = conn.query(8, "SELECT x FROM qd_missing", QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION, 0, 0).queryError();
                Assert.assertEquals(14, next.readPosition());
                next.assertEnd();
            }
        });
    }

    /**
     * A released c-questdb-client or Python client with a per-query timeout sets
     * {@code QUERY_FLAG_TIMEOUT} and appends {@code timeout_ms}, but only for a
     * server that advertises {@code CAP_QUERY_TIMEOUT}. This server does not, so
     * those clients refuse such a query instead. Sent anyway, the reserved bit
     * selects none of the query details: the error and the end of a result come
     * back as the baseline frames those clients decode, with no trailer.
     */
    @Test
    public void testReservedTimeoutFlagGetsBaselineFrames() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented();
                 WireConnection conn = new WireConnection("")) {
                createWindowTable(serverMain, 3);
                final long flags = QwpEgressMsgKind.QUERY_FLAG_RESET_DICT | QwpEgressMsgKind.QUERY_FLAG_TIMEOUT;
                conn.send(withTimeoutMs(QwpWireTestFixtures.buildQueryRequest(1, "SELECT x FROM qd_missing", 0, flags, 0, 0), 5_000));
                QueryError error = conn.readResponse(1).queryError();
                Assert.assertEquals(QwpConstants.STATUS_PARSE_ERROR, error.status);
                Assert.assertEquals("[14] table does not exist [table=qd_missing]", error.message);
                error.assertEnd();

                conn.send(withTimeoutMs(QwpWireTestFixtures.buildQueryRequest(2, "SELECT x FROM qd_window", 0, flags, 0, 0), 5_000));
                Response r = conn.readResponse(2);
                assertSequence(r.longs, 0, 3);
                ResultEnd end = r.resultEnd();
                Assert.assertEquals(3, end.totalRows);
                end.assertEnd();
            }
        });
    }

    @Test
    public void testResultEndFollowsABatchThatFillsTheSendBuffer() throws Exception {
        // RESULT_END goes out in one send with the last batch when the send
        // buffer holds both, the longest RESULT_END the request's trailers can
        // make included, and in a send of its own when it does not. The length
        // of the one value moves the batch across that line, a byte at a time,
        // with and without the trailers. The send fragmentation parks every
        // send, so the stream also resumes after the batch it sent alone, then
        // finds no more rows and sends RESULT_END; a socket buffer that takes the
        // whole batch lets the send complete instead.
        // the smallest send buffer the handshake takes, rounded up
        final String sendBufferSize = "163840";
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startFragmented(
                    PropertyKey.HTTP_SEND_BUFFER_SIZE.getEnvVarName(), sendBufferSize,
                    PropertyKey.HTTP_FROZEN_CLOCK.getEnvVarName(), "true"
            )) {
                assertResultEndFollowsABatchThatFillsTheSendBuffer(Integer.parseInt(sendBufferSize));
            }
            try (TestServerMain ignored = startWithEnvVariables(
                    PropertyKey.HTTP_SEND_BUFFER_SIZE.getEnvVarName(), sendBufferSize,
                    PropertyKey.HTTP_NET_CONNECTION_SNDBUF.getEnvVarName(), "4194304",
                    PropertyKey.HTTP_FROZEN_CLOCK.getEnvVarName(), "true"
            )) {
                assertResultEndFollowsABatchThatFillsTheSendBuffer(Integer.parseInt(sendBufferSize));
            }
        });
    }

    @Test
    public void testSkipAbortedByQueryTimeout() throws Exception {
        // Skipping to the start of a row window reads the skipped rows of a result
        // whose filter hides where they end, under the query's circuit breaker. A
        // skip that outlasts query.timeout ends the query with QUERY_ERROR before
        // a batch goes out, frees the cursor and its factory, and leaves the
        // connection usable.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startFragmented(
                    PropertyKey.QUERY_TIMEOUT.getEnvVarName(), "500ms",
                    PropertyKey.HTTP_FROZEN_CLOCK.getEnvVarName(), "false"
            );
                 WireConnection conn = new WireConnection("")) {
                final String sql = "SELECT x FROM long_sequence(1_000_000_000_000) WHERE x % 2 = 0";
                for (int positioned = 0; positioned < 2; positioned++) {
                    final long flags = WINDOW | (positioned == 1 ? QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION : 0);
                    Response r = conn.query(1 + positioned, sql, flags, 4_000_000_000L, 10);
                    Assert.assertEquals("no batch goes out", 0, r.batches);
                    QueryError error = r.queryError();
                    Assert.assertEquals(QwpConstants.STATUS_LIMIT_EXCEEDED, error.status);
                    if (positioned == 1) {
                        // the position /exec reports for a timeout
                        TestUtils.assertContains(error.message, "timeout, query aborted");
                        Assert.assertFalse(error.message, error.message.startsWith("["));
                        Assert.assertEquals(0, error.readPosition());
                    } else {
                        Assert.assertTrue(error.message, error.message.startsWith("[-1] timeout, query aborted"));
                    }
                    error.assertEnd();
                }

                Response next = conn.query(3, "SELECT x FROM long_sequence(5)", WINDOW | QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS, 1, 2);
                assertSequence(next.longs, 2, 2);
                ResultEnd end = next.resultEnd();
                Assert.assertEquals(5, end.readRowCount());
                end.assertEnd();
            }
        });
    }

    @Test
    public void testStatsFrozenClock() throws Exception {
        // A frozen HTTP clock makes every duration exactly 0, so the frame bytes
        // are fully known: the block is present on each terminator that carries
        // one, in its place after the other fields.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented(PropertyKey.HTTP_FROZEN_CLOCK.getEnvVarName(), "true");
                 WireConnection conn = new WireConnection("")) {
                createWindowTable(serverMain, 20);
                ResultEnd end = conn.query(1, "SELECT x FROM qd_window", QwpEgressMsgKind.QUERY_FLAG_STATS, 0, 0).resultEnd();
                Assert.assertEquals(20, end.totalRows);
                end.readStats().assertFrozen(false);
                end.assertEnd();

                ResultEnd counted = conn.query(2, "SELECT x FROM qd_window", QwpEgressMsgKind.QUERY_FLAG_STATS
                        | QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS | QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS, 0, 3).resultEnd();
                Assert.assertEquals(3, counted.totalRows);
                Assert.assertEquals(20, counted.readRowCount());
                counted.readStats().assertFrozen(false);
                counted.assertEnd();

                ExecDone insert = conn.query(3, "INSERT INTO qd_window VALUES (20, 20::TIMESTAMP)", QwpEgressMsgKind.QUERY_FLAG_STATS, 0, 0).execDone();
                Assert.assertEquals(CompiledQuery.INSERT, insert.opType);
                Assert.assertEquals(1, insert.rowsAffected);
                insert.readStats().assertFrozen(false);
                insert.assertEnd();

                ExecDone ddl = conn.query(4, "CREATE TABLE qd_other (x LONG)", QwpEgressMsgKind.QUERY_FLAG_STATS, 0, 0).execDone();
                Assert.assertEquals(CompiledQuery.CREATE_TABLE, ddl.opType);
                ddl.readStats().assertFrozen(false);
                ddl.assertEnd();

                // statistics are not reported on errors
                QueryError error = conn.query(5, "SELECT x FROM qd_missing", QwpEgressMsgKind.QUERY_FLAG_STATS, 0, 0).queryError();
                Assert.assertEquals("[14] table does not exist [table=qd_missing]", error.message);
                error.assertEnd();
            }
        });
    }

    @Test
    public void testStatsMeasureClientWait() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startFragmented(PropertyKey.HTTP_FROZEN_CLOCK.getEnvVarName(), "false");
                 WireConnection conn = new WireConnection(maxBatchRows(7))) {
                conn.send(QwpWireTestFixtures.buildQueryRequest(1, "SELECT x FROM long_sequence(100)", 1, QwpEgressMsgKind.QUERY_FLAG_STATS, 0, 0));
                Response r = new Response();
                conn.readBatch(r, 1);
                // the query waits for credit from the moment it sent the batch
                final long waitMillis = 300;
                Thread.sleep(waitMillis);
                conn.send(QwpWireTestFixtures.buildCreditFrame(1, 1 << 20));
                conn.readResponse(1, r);
                assertSequence(r.longs, 1, 100);
                Stats stats = r.resultEnd().readStats();
                Assert.assertTrue(stats.toString(), stats.clientWaitNanos >= waitMillis * 1_000_000L);
                Assert.assertTrue(stats.toString(), stats.executeNanos >= stats.clientWaitNanos + stats.compileNanos);
                Assert.assertEquals(0, stats.countNanos);
            }
        });
    }

    @Test
    public void testStatsMeasureExecution() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startFragmented(PropertyKey.HTTP_FROZEN_CLOCK.getEnvVarName(), "false");
                 WireConnection conn = new WireConnection("")) {
                final long sleepMillis = 250;
                ResultEnd end = conn.query(1, "sleep(" + sleepMillis / 1000.0 + ")", QwpEgressMsgKind.QUERY_FLAG_STATS, 0, 0).resultEnd();
                Assert.assertEquals(1, end.totalRows);
                Stats stats = end.readStats();
                Assert.assertTrue(stats.toString(), stats.executeNanos >= sleepMillis * 1_000_000L);
                Assert.assertTrue(stats.toString(), stats.compileNanos > 0);
                Assert.assertTrue(stats.toString(), stats.executeNanos >= stats.compileNanos + stats.clientWaitNanos);
                Assert.assertEquals(0, stats.countNanos);
                Assert.assertFalse(stats.isJitCompiled);
                end.assertEnd();

                ExecDone ddl = conn.query(2, "CREATE TABLE qd_stats AS (SELECT x FROM long_sequence(1000))", QwpEgressMsgKind.QUERY_FLAG_STATS, 0, 0).execDone();
                Assert.assertEquals(CompiledQuery.CREATE_TABLE_AS_SELECT, ddl.opType);
                Stats ddlStats = ddl.readStats();
                Assert.assertTrue(ddlStats.toString(), ddlStats.compileNanos > 0);
                Assert.assertTrue(ddlStats.toString(), ddlStats.executeNanos >= ddlStats.compileNanos);
                Assert.assertEquals(0, ddlStats.countNanos);
                Assert.assertEquals(0, ddlStats.clientWaitNanos);
                ddl.assertEnd();
            }
        });
    }

    @Test
    public void testStatsReportZeroCompileOnCacheHit() throws Exception {
        // One HTTP worker, so the second run finds the factory the first run
        // put back into the worker's select cache, as /exec reports compiler: 0.
        // The bootstrap configuration turns the cache off.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented(
                    PropertyKey.HTTP_WORKER_COUNT.getEnvVarName(), "1",
                    PropertyKey.HTTP_QUERY_CACHE_ENABLED.getEnvVarName(), "true",
                    PropertyKey.HTTP_FROZEN_CLOCK.getEnvVarName(), "false"
            );
                 WireConnection conn = new WireConnection("")) {
                createWindowTable(serverMain, 10);
                Stats first = conn.query(1, "SELECT x FROM qd_window", QwpEgressMsgKind.QUERY_FLAG_STATS, 0, 0).resultEnd().readStats();
                Assert.assertTrue(first.toString(), first.compileNanos > 0);
                Stats second = conn.query(2, "SELECT x FROM qd_window", QwpEgressMsgKind.QUERY_FLAG_STATS, 0, 0).resultEnd().readStats();
                Assert.assertEquals(second.toString(), 0, second.compileNanos);
                Assert.assertTrue(second.toString(), second.executeNanos > 0);
            }
        });
    }

    @Test
    public void testWindowAcrossBatchesWithCredit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented();
                 WireConnection conn = new WireConnection(maxBatchRows(7))) {
                createWindowTable(serverMain, 100);
                // 20 rows from row 3 cross three 7-row batches, each released by
                // its own CREDIT
                conn.send(QwpWireTestFixtures.buildQueryRequest(1, "SELECT x FROM qd_window", 1,
                        QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS | WINDOW, 3, 20));
                Response r = new Response();
                for (int batch = 1; batch <= 2; batch++) {
                    // a batch overdraws the credit by its size less the one byte,
                    // so granting exactly its size lets the next batch out
                    final int batchBytes = conn.readBatch(r, 1);
                    Assert.assertEquals(7L * batch, r.longs.size());
                    conn.send(QwpWireTestFixtures.buildCreditFrame(1, batchBytes));
                }
                conn.readResponse(1, r);
                Assert.assertEquals(3, r.batches);
                assertSequence(r.longs, 3, 20);
                ResultEnd end = r.resultEnd();
                Assert.assertEquals(2, end.finalSeq);
                Assert.assertEquals(20, end.totalRows);
                Assert.assertEquals(100, end.readRowCount());
                end.assertEnd();
            }
        });
    }

    @Test
    public void testWindowAcrossPartialEmits() throws Exception {
        // 2 KB rows: a send buffer holds about a hundred, so the buffered window
        // leaves in several frames, each a prefix of what is left. The window
        // ends with the last of them, not when the rows are buffered.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented(PropertyKey.HTTP_SEND_BUFFER_SIZE.getEnvVarName(), "262144");
                 WireConnection conn = new WireConnection("")) {
                serverMain.execute("CREATE TABLE qd_wide AS (SELECT x, lpad(x::VARCHAR, 2_000, '0') v, x::TIMESTAMP ts "
                        + "FROM long_sequence(3_000)) TIMESTAMP(ts) PARTITION BY DAY");
                for (int counted = 0; counted < 2; counted++) {
                    final long flags = WINDOW | (counted == 1 ? QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS | QwpEgressMsgKind.QUERY_FLAG_STATS : 0);
                    final Response r = conn.query(1 + counted, "SELECT x, v FROM qd_wide", flags, 100, 1_000);
                    Assert.assertTrue("batches: " + r.batches, r.batches > 2);
                    assertSequence(r.longs, 101, 1_000);
                    Assert.assertEquals(1_000, r.strings.size());
                    for (int i = 0; i < 1_000; i++) {
                        final String x = Long.toString(101 + i);
                        Assert.assertEquals("0".repeat(2_000 - x.length()) + x, r.strings.getQuick(i));
                    }
                    final ResultEnd end = r.resultEnd();
                    Assert.assertEquals(r.batches - 1, end.finalSeq);
                    Assert.assertEquals(1_000, end.totalRows);
                    if (counted == 1) {
                        Assert.assertEquals(3_000, end.readRowCount());
                        end.readStats().assertFrozen(false);
                    }
                    end.assertEnd();
                }
            }
        });
    }

    @Test
    public void testWindowAppliesToExplain() throws Exception {
        // EXPLAIN streams the plan as rows, so the window and the count apply
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented();
                 WireConnection conn = new WireConnection("")) {
                createWindowTable(serverMain, 10);
                final String sql = "EXPLAIN SELECT x FROM qd_window WHERE x > 5 ORDER BY x DESC";
                final Response full = conn.query(1, sql, 0, 0, 0);
                final int planRows = full.strings.size();
                Assert.assertTrue("plan rows: " + planRows, planRows > 2);
                Assert.assertEquals(planRows, full.resultEnd().totalRows);
                Response r = conn.query(2, sql, QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS | WINDOW, 1, 2);
                Assert.assertEquals(2, r.strings.size());
                Assert.assertEquals(full.strings.getQuick(1), r.strings.getQuick(0));
                Assert.assertEquals(full.strings.getQuick(2), r.strings.getQuick(1));
                ResultEnd end = r.resultEnd();
                Assert.assertEquals(2, end.totalRows);
                Assert.assertEquals(planRows, end.readRowCount());
                end.assertEnd();
            }
        });
    }

    @Test
    public void testWindowIgnoredByNonSelect() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented();
                 WireConnection conn = new WireConnection("")) {
                createWindowTable(serverMain, 10);
                ExecDone insert = conn.query(1, "INSERT INTO qd_window SELECT x + 10, (x + 10)::TIMESTAMP FROM long_sequence(30)",
                        QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS | WINDOW, 5, 2).execDone();
                Assert.assertEquals(CompiledQuery.INSERT_AS_SELECT, insert.opType);
                Assert.assertEquals(30, insert.rowsAffected);
                insert.assertEnd();
                serverMain.awaitTable("qd_window");
                ResultEnd end = conn.query(2, "SELECT x FROM qd_window", QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS, 0, 0).resultEnd();
                Assert.assertEquals(40, end.readRowCount());
            }
        });
    }

    @Test
    public void testWindowMatchesSqlLimit() throws Exception {
        assertWindowMatchesSqlLimit(true);
    }

    @Test
    public void testWindowMatchesSqlLimitSerialNonWal() throws Exception {
        // serial filters and aggregation, over a table written without WAL, in
        // page frames of 128 rows, so a skip crosses many frame boundaries
        assertWindowMatchesSqlLimit(
                false,
                PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED.getEnvVarName(), "false",
                PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED.getEnvVarName(), "false",
                PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS.getEnvVarName(), "128",
                PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS.getEnvVarName(), "64"
        );
    }

    @Test
    public void testWindowOnParquetPartitions() throws Exception {
        // The row window hands max_rows to skipRows() as the bound on the rows
        // read after the skip, which a parquet frame uses to decode only those.
        // A requested count lifts the bound when the cursor does not know its
        // size, as under the filter below, and counts the rest by reading it.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented();
                 WireConnection conn = new WireConnection(maxBatchRows(32));
                 QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                client.connect();
                serverMain.execute("CREATE TABLE qd_parquet (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
                // 5 hourly partitions of 360 rows each
                serverMain.execute("INSERT INTO qd_parquet SELECT x - 1, ((x - 1) * 10_000_000)::TIMESTAMP FROM long_sequence(1_800)");
                serverMain.awaitTable("qd_parquet");
                serverMain.execute("ALTER TABLE qd_parquet CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-01T04'");
                serverMain.awaitTable("qd_parquet");
                final String sql = "SELECT x FROM qd_parquet";
                final long[][] windows = {{0, 5}, {355, 10}, {360, 360}, {719, 2}, {1_000, 1_000}, {1_799, 5}, {0, 0}};
                long requestId = 1;
                for (long[] w : windows) {
                    for (int counted = 0; counted < 2; counted++) {
                        final long flags = WINDOW | (counted == 1 ? QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS : 0);
                        Response r = conn.query(requestId++, sql, flags, w[0], w[1]);
                        final long expectedRows = Math.max(0, Math.min(w[1], 1_800 - w[0]));
                        assertSequence(r.longs, w[0], expectedRows);
                        ResultEnd end = r.resultEnd();
                        if (counted == 1) {
                            Assert.assertEquals(1_800, end.readRowCount());
                        }
                        end.assertEnd();
                    }
                }
                final LongList filtered = new LongList();
                client.execute("SELECT x FROM qd_parquet WHERE x % 7 = 0 LIMIT 40, 60", collectLongs(filtered));
                Response r = conn.query(requestId, "SELECT x FROM qd_parquet WHERE x % 7 = 0",
                        WINDOW | QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS, 40, 20);
                TestUtils.assertEquals(filtered, r.longs);
                Assert.assertEquals(258, r.resultEnd().readRowCount());
            }
        });
    }

    @Test
    public void testWindowWithBindVariables() throws Exception {
        // The bind section precedes the flags. The factory compiled for the bind
        // types is reused from the cache for new values, and the window applies
        // to each execution alike.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented(PropertyKey.HTTP_QUERY_CACHE_ENABLED.getEnvVarName(), "true");
                 WireConnection conn = new WireConnection("")) {
                createWindowTable(serverMain, 1_000);
                final String sql = "SELECT x FROM qd_window WHERE x >= $1 AND x < $2";
                final long flags = QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS | WINDOW;
                final long[][] cases = {
                        // lo, hi, skip, max
                        {100, 200, 10, 5}, {100, 200, 95, 10}, {900, 1_000, 0, 1_000}, {5_000, 6_000, 0, 10},
                };
                long requestId = 1;
                for (long[] c : cases) {
                    conn.send(QwpWireTestFixtures.buildQueryRequest(requestId, sql, 0, flags, c[2], c[3], c[0], c[1]));
                    Response r = conn.readResponse(requestId++);
                    final long matching = Math.max(0, Math.min(c[1], 1_000) - c[0]);
                    final long expectedRows = Math.max(0, Math.min(c[3], matching - c[2]));
                    assertSequence(r.longs, c[0] + c[2], expectedRows);
                    ResultEnd end = r.resultEnd();
                    Assert.assertEquals(expectedRows, end.totalRows);
                    Assert.assertEquals(matching, end.readRowCount());
                    end.assertEnd();
                }
            }
        });
    }

    /**
     * Each capability and each query flag is a bit of its own, and the query
     * details leave alone the per-query timeout bits that released clients
     * already use: capability 0x08 and flag 0x02, followed by {@code timeout_ms}
     * (c-questdb-client 7.2.0 and py-questdb-client 6.0.0).
     */
    @Test
    public void testWireBitsAreDistinctAndLeaveTimeoutBitsFree() {
        Assert.assertEquals(0x08, QwpEgressMsgKind.CAP_QUERY_TIMEOUT);
        Assert.assertEquals(0x02, QwpEgressMsgKind.QUERY_FLAG_TIMEOUT);
        assertDistinctBits(
                QwpEgressMsgKind.CAP_ZONE,
                QwpEgressMsgKind.CAP_QUERY_FLAGS,
                QwpEgressMsgKind.CAP_COMPRESSION,
                QwpEgressMsgKind.CAP_QUERY_TIMEOUT,
                QwpEgressMsgKind.CAP_QUERY_DETAILS
        );
        assertDistinctBits(
                QwpEgressMsgKind.QUERY_FLAG_RESET_DICT,
                QwpEgressMsgKind.QUERY_FLAG_TIMEOUT,
                QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION,
                QwpEgressMsgKind.QUERY_FLAG_STATS,
                QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS,
                QwpEgressMsgKind.QUERY_FLAG_SKIP_ROWS,
                QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS
        );
    }

    private void assertJitCompiled(boolean isJitEnabled) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented(
                    PropertyKey.CAIRO_SQL_JIT_MODE.getEnvVarName(), isJitEnabled ? "on" : "off");
                 WireConnection conn = new WireConnection("")) {
                createWindowTable(serverMain, 1_000);
                final String[] queries = {
                        "SELECT x FROM qd_window WHERE x > 500",
                        "SELECT x FROM qd_window",
                        "SELECT x FROM qd_window WHERE x > 500 ORDER BY x DESC",
                };
                boolean isAnyJit = false;
                long requestId = 1;
                for (String sql : queries) {
                    for (int windowed = 0; windowed < 2; windowed++) {
                        final long flags = QwpEgressMsgKind.QUERY_FLAG_STATS | (windowed == 1 ? WINDOW : 0);
                        Stats stats = conn.query(requestId++, sql, flags, 1, 3).resultEnd().readStats();
                        Assert.assertEquals(sql, execJitCompiled(sql), stats.isJitCompiled);
                        isAnyJit |= stats.isJitCompiled;
                    }
                }
                Assert.assertEquals("the filters must exercise both values of the bit",
                        isJitEnabled && JitUtil.isJitSupported(), isAnyJit);
            }
        });
    }

    private void assertWindowMatchesSqlLimit(boolean isWal, String... env) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented(env);
                 WireConnection conn = new WireConnection(maxBatchRows(64));
                 QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                client.connect();
                final int rows = 1_000;
                createWindowTable(serverMain, rows, isWal);
                // y gets a column top at row 500, and NULLs at every 4th row after it
                serverMain.execute("ALTER TABLE qd_window ADD COLUMN y LONG");
                serverMain.execute("INSERT INTO qd_window SELECT x + 999, (x + 999)::TIMESTAMP, "
                        + "CASE WHEN x % 4 = 0 THEN NULL ELSE x END FROM long_sequence(500)");
                serverMain.awaitTable("qd_window");
                serverMain.execute("CREATE TABLE qd_empty (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
                serverMain.awaitTable("qd_empty");
                final String[] queries = {
                        // a table scan: the page-frame factory, whose cursor knows its size
                        "SELECT x FROM qd_window",
                        // a column added after data: a column top, then NULLs
                        "SELECT y FROM qd_window",
                        "SELECT x FROM qd_empty",
                        // a filter: the size is unknown until the rows are counted
                        "SELECT x FROM qd_window WHERE x % 3 = 0",
                        // an interval scan: unknown size, counted from the partitions
                        "SELECT x FROM qd_window WHERE ts BETWEEN '1970-01-01T00:00:00.000100' AND '1970-01-01T00:00:00.000399'",
                        // a backward scan
                        "SELECT x FROM qd_window ORDER BY ts DESC",
                        // a sort that is not the table order
                        "SELECT x FROM qd_window ORDER BY x % 10, x",
                        // aggregation
                        "SELECT x % 37 k FROM qd_window GROUP BY k ORDER BY k",
                        // the table's own LIMIT under the window
                        "SELECT x FROM qd_window LIMIT 250, 700",
                        // a CTE, a union, a join and a window function
                        "WITH t AS (SELECT x FROM qd_window WHERE x > 100) SELECT x FROM t",
                        "SELECT x FROM qd_window UNION ALL SELECT x + 10_000 FROM qd_window WHERE x < 50",
                        "SELECT a.x FROM qd_window a JOIN qd_window b ON a.x = b.x * 2",
                        "SELECT row_number() OVER (ORDER BY ts DESC) FROM qd_window",
                };
                final long[][] windows = {
                        {0, 10}, {10, 10}, {0, 0}, {995, 10}, {329, 1}, {333, 5}, {1_000, 10}, {1_495, 10},
                        {5_000, 1}, {0, Long.MAX_VALUE}, {17, Long.MAX_VALUE}, {0, 1_000_000},
                        {0, Long.MAX_VALUE - 1}, {Long.MAX_VALUE, 1}, {3_000_000_000L, 3_000_000_000L},
                };
                long requestId = 1;
                for (String sql : queries) {
                    final LongList all = new LongList();
                    client.execute(sql, collectLongs(all));
                    final long expectedCount = all.size();
                    Assert.assertEquals(sql, expectedCount, execCount(sql));
                    for (long[] w : windows) {
                        final long skip = w[0];
                        final long max = w[1];
                        // the pinned client runs the SQL LIMIT of the same window
                        final LongList limited = new LongList();
                        if (max > 0 && skip < expectedCount) {
                            client.execute("SELECT * FROM (" + sql + ") LIMIT " + skip + ", "
                                    + (skip + Math.min(max, expectedCount - skip)), collectLongs(limited));
                        }
                        final String msg = sql + " [skip=" + skip + ", max=" + max + ']';
                        for (int counted = 0; counted < 2; counted++) {
                            final long flags = WINDOW | (counted == 1 ? QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS : 0);
                            Response r = conn.query(requestId++, sql, flags, skip, max);
                            assertLongs(msg, limited, r.longs);
                            ResultEnd end = r.resultEnd();
                            Assert.assertEquals(msg, limited.size(), end.totalRows);
                            if (counted == 1) {
                                Assert.assertEquals(msg, expectedCount, end.readRowCount());
                            }
                            end.assertEnd();
                        }
                    }
                }
            }
        });
    }

    private static void assertDistinctBits(int... bits) {
        int union = 0;
        for (int bit : bits) {
            Assert.assertEquals("0x" + Integer.toHexString(bit) + " must be a single bit", 1, Integer.bitCount(bit));
            Assert.assertEquals("0x" + Integer.toHexString(bit) + " is taken twice", 0, union & bit);
            union |= bit;
        }
    }

    private static void assertLongs(String message, LongList expected, LongList actual) {
        Assert.assertEquals(message, expected.size(), actual.size());
        for (int i = 0, n = expected.size(); i < n; i++) {
            Assert.assertEquals(message + " at " + i, expected.getQuick(i), actual.getQuick(i));
        }
    }

    /**
     * Sends queries whose last and only batch takes, with its WebSocket header,
     * from 4 bytes less to 4 bytes more than the send buffer leaves beside the
     * longest RESULT_END the request can get, with and without the trailers, and
     * asserts that every one ends with that RESULT_END.
     */
    private static void assertResultEndFollowsABatchThatFillsTheSendBuffer(int sendBufferSize) throws Exception {
        try (WireConnection conn = new WireConnection("")) {
            // the bytes of the batch besides the value: headers, prelude, schema,
            // lengths, the same for every value longer than 64 KiB
            final int calibrationLength = 100_000;
            conn.send(QwpWireTestFixtures.buildQueryRequest(1, valueOfLength(calibrationLength), 0, 0, 0, 0));
            final Response calibration = new Response();
            final int overhead = conn.readBatch(calibration, 1) - calibrationLength;
            conn.readResponse(1, calibration);
            calibration.resultEnd().assertEnd();

            long requestId = 2;
            for (int withTrailers = 0; withTrailers < 2; withTrailers++) {
                final long flags = withTrailers == 1
                        ? QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS | QwpEgressMsgKind.QUERY_FLAG_STATS
                        : 0;
                final int resultEndMaxBytes = QwpEgressUpgradeProcessor.resultEndWorstCase(withTrailers == 1, withTrailers == 1);
                // the batch frame takes a 10-byte WebSocket header and its QWP bytes
                for (int frameSize = sendBufferSize - resultEndMaxBytes - 4; frameSize <= sendBufferSize - resultEndMaxBytes + 4; frameSize++) {
                    final int length = frameSize - 10 - overhead;
                    final Response r = conn.query(requestId++, valueOfLength(length), flags, 0, 0);
                    final String msg = "frame=" + frameSize + ", trailers=" + (withTrailers == 1);
                    Assert.assertEquals(msg, 1, r.batches);
                    Assert.assertEquals(msg, length, r.strings.getQuick(0).length());
                    final ResultEnd end = r.resultEnd();
                    Assert.assertEquals(msg, 1, end.totalRows);
                    if (withTrailers == 1) {
                        Assert.assertEquals(msg, 1, end.readRowCount());
                        end.readStats().assertFrozen(false);
                    }
                    end.assertEnd();
                }
            }
        }
    }

    private static void assertSequence(LongList actual, long first, long count) {
        Assert.assertEquals(count, actual.size());
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(first + i, actual.getQuick(i));
        }
    }

    private static byte[] cancelFrame(long requestId) {
        byte[] p = new byte[9];
        p[0] = QwpEgressMsgKind.CANCEL;
        for (int s = 0; s < 8; s++) {
            p[1 + s] = (byte) (requestId >>> (8 * s));
        }
        return p;
    }

    private static QwpColumnBatchHandler collectLongs(LongList sink) {
        return new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                for (int row = 0; row < batch.getRowCount(); row++) {
                    sink.add(batch.isNull(0, row) ? Numbers.LONG_NULL : batch.getLongValue(0, row));
                }
            }

            @Override
            public void onEnd(long totalRows) {
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("oracle query failed: " + message);
            }
        };
    }

    private static String maxBatchRows(int rows) {
        return "X-QWP-Max-Batch-Rows: " + rows + "\r\n";
    }

    /**
     * A query of one row and one VARCHAR column, whose value is {@code length}
     * ASCII chars.
     */
    private static String valueOfLength(int length) {
        return "SELECT lpad('x', " + length + ", 'x')::VARCHAR v FROM long_sequence(1)";
    }

    /**
     * Appends {@code timeout_ms:varint} to a request that ends with its
     * {@code query_flags}, as a client that sets {@code QUERY_FLAG_TIMEOUT} does.
     */
    private static byte[] withTimeoutMs(byte[] request, long timeoutMs) {
        final byte[] p = Arrays.copyOf(request, request.length + 10);
        return Arrays.copyOf(p, QwpVarint.encode(p, request.length, timeoutMs));
    }


    private static final class ExecDone extends Terminator {
        final short opType;
        final long rowsAffected;

        ExecDone(WireReader rd) {
            super(rd);
            opType = (short) rd.u8();
            rowsAffected = rd.varint();
        }
    }

    private static final class QueryError extends Terminator {
        final String message;
        final byte status;

        QueryError(WireReader rd) {
            super(rd);
            status = (byte) rd.u8();
            message = rd.utf8(rd.u16());
        }

        int readPosition() {
            return rd.i32();
        }
    }

    /**
     * Frames of one query: the LONG values of a single-column result and the
     * terminator.
     */
    private static final class Response {
        final LongList longs = new LongList();
        final ObjList<String> strings = new ObjList<>();
        int batches;
        byte[] columnTypes;
        byte[] terminator;

        ExecDone execDone() {
            return new ExecDone(terminatorReader(QwpEgressMsgKind.EXEC_DONE));
        }

        QueryError queryError() {
            return new QueryError(terminatorReader(QwpEgressMsgKind.QUERY_ERROR));
        }

        ResultEnd resultEnd() {
            return new ResultEnd(terminatorReader(QwpEgressMsgKind.RESULT_END));
        }

        private WireReader terminatorReader(byte kind) {
            Assert.assertNotNull(terminator);
            Assert.assertEquals("terminator kind", kind, terminator[QwpConstants.HEADER_SIZE]);
            return new WireReader(terminator, QwpConstants.HEADER_SIZE + 1 + 8);
        }
    }

    private static final class ResultEnd extends Terminator {
        final long finalSeq;
        final long totalRows;

        ResultEnd(WireReader rd) {
            super(rd);
            finalSeq = rd.varint();
            totalRows = rd.varint();
        }

        long readRowCount() {
            return rd.varint();
        }
    }

    private record Stats(long compileNanos, long executeNanos, long countNanos, long clientWaitNanos,
                         boolean isJitCompiled) {
        void assertFrozen(boolean isJitCompiled) {
            Assert.assertEquals(toString(), new Stats(0, 0, 0, 0, isJitCompiled), this);
        }
    }

    private abstract static class Terminator {
        final WireReader rd;

        Terminator(WireReader rd) {
            this.rd = rd;
        }

        void assertEnd() {
            Assert.assertEquals("bytes left after the last field", 0, rd.remaining());
        }

        Stats readStats() {
            final long compile = rd.varint();
            final long execute = rd.varint();
            final long count = rd.varint();
            final long wait = rd.varint();
            final int flags = rd.u8();
            Assert.assertEquals("unknown stats_flags bits", 0, flags & ~QwpEgressMsgKind.STATS_FLAG_JIT_COMPILED);
            return new Stats(compile, execute, count, wait, (flags & QwpEgressMsgKind.STATS_FLAG_JIT_COMPILED) != 0);
        }
    }

    private static final class WireConnection implements AutoCloseable {
        final byte[] serverInfo;
        private final InputStream in;
        private final OutputStream out;
        private final Socket socket;

        WireConnection(String extraHeaders) throws Exception {
            socket = new Socket("127.0.0.1", HTTP_PORT);
            try {
                socket.setSoTimeout(60_000);
                QwpWireTestFixtures.performReadHandshake(socket, "", extraHeaders);
                in = socket.getInputStream();
                out = socket.getOutputStream();
                serverInfo = QwpWireTestFixtures.readServerFrame(in);
                QwpWireTestFixtures.assertQwpMessageKind(serverInfo, QwpEgressMsgKind.SERVER_INFO);
            } catch (Throwable th) {
                socket.close();
                throw th;
            }
        }

        @Override
        public void close() throws Exception {
            socket.close();
        }

        Response query(long requestId, String sql, long flags, long skipRows, long maxRows) throws Exception {
            send(QwpWireTestFixtures.buildQueryRequest(requestId, sql, 0, flags, skipRows, maxRows));
            return readResponse(requestId);
        }

        /**
         * Reads the next frame, which must be a RESULT_BATCH of the query, and
         * returns its size, which is what it costs in credit.
         */
        int readBatch(Response r, long requestId) throws Exception {
            final byte[] frame = readFrame();
            QwpWireTestFixtures.assertQwpMessageKind(frame, QwpEgressMsgKind.RESULT_BATCH);
            decodeBatch(frame, r, requestId);
            return frame.length;
        }

        byte[] readFrame() throws Exception {
            return QwpWireTestFixtures.readServerFrame(in);
        }

        Response readResponse(long requestId) throws Exception {
            final Response r = new Response();
            readResponse(requestId, r);
            return r;
        }

        void readResponse(long requestId, Response r) throws Exception {
            while (true) {
                final byte[] frame = readFrame();
                Assert.assertTrue(frame.length > QwpConstants.HEADER_SIZE);
                final byte kind = frame[QwpConstants.HEADER_SIZE];
                QwpWireTestFixtures.assertQwpMessageKind(frame, kind);
                switch (kind) {
                    case QwpEgressMsgKind.RESULT_BATCH -> decodeBatch(frame, r, requestId);
                    case QwpEgressMsgKind.RESULT_END, QwpEgressMsgKind.EXEC_DONE, QwpEgressMsgKind.QUERY_ERROR -> {
                        Assert.assertEquals("terminator request id", requestId,
                                new WireReader(frame, QwpConstants.HEADER_SIZE + 1).i64());
                        r.terminator = frame;
                        return;
                    }
                    case QwpEgressMsgKind.CACHE_RESET -> {
                    }
                    default -> Assert.fail("unexpected msg_kind 0x" + Integer.toHexString(kind & 0xFF));
                }
            }
        }

        void send(byte[] payload) throws Exception {
            out.write(QwpWireTestFixtures.maskedFrame(WebSocketOpcode.BINARY, payload));
            out.flush();
        }

        /**
         * Decodes a raw RESULT_BATCH: prelude, delta symbol dictionary, then the
         * table block, whose rows are collected when the result is a single LONG
         * column.
         */
        private static void decodeBatch(byte[] frame, Response r, long requestId) {
            Assert.assertEquals("compression is not negotiated", 0, frame[QwpConstants.HEADER_OFFSET_FLAGS] & QwpConstants.FLAG_ZSTD);
            final WireReader rd = new WireReader(frame, QwpConstants.HEADER_SIZE + 1);
            Assert.assertEquals(requestId, rd.i64());
            Assert.assertEquals("batch_seq", r.batches, rd.varint());
            rd.varint(); // delta start
            for (long entries = rd.varint(); entries > 0; entries--) {
                rd.skip((int) rd.varint());
            }
            Assert.assertEquals("result sets have no table name", 0, rd.varint());
            final int rows = (int) rd.varint();
            if (r.batches == 0) {
                r.columnTypes = new byte[(int) rd.varint()];
                for (int c = 0; c < r.columnTypes.length; c++) {
                    rd.skip((int) rd.varint());
                    r.columnTypes[c] = (byte) rd.u8();
                }
            }
            r.batches++;
            for (byte type : r.columnTypes) {
                if (type != QwpConstants.TYPE_LONG && type != QwpConstants.TYPE_VARCHAR) {
                    // the rows of other shapes are not decoded; only their count is checked
                    for (int row = 0; row < rows; row++) {
                        r.longs.add(Numbers.LONG_NULL);
                    }
                    return;
                }
            }
            // each column: null flag, optional bitmap, then the non-NULL values;
            // column 0, when LONG, lands in longs and the first VARCHAR column
            // in strings, NULLs included
            int firstVarcharColumn = -1;
            for (int c = r.columnTypes.length - 1; c > -1; c--) {
                if (r.columnTypes[c] == QwpConstants.TYPE_VARCHAR) {
                    firstVarcharColumn = c;
                }
            }
            for (int c = 0; c < r.columnTypes.length; c++) {
                final boolean hasNulls = rd.u8() != 0;
                final int bitmapStart = rd.pos;
                if (hasNulls) {
                    rd.skip((rows + 7) >>> 3);
                }
                int nonNullCount = rows;
                for (int row = 0; hasNulls && row < rows; row++) {
                    if ((frame[bitmapStart + (row >>> 3)] & (1 << (row & 7))) != 0) {
                        nonNullCount--;
                    }
                }
                final int valuesStart = rd.pos;
                final int heapStart = valuesStart + 4 * (nonNullCount + 1);
                int dense = 0;
                for (int row = 0; row < rows; row++) {
                    final boolean isNull = hasNulls && (frame[bitmapStart + (row >>> 3)] & (1 << (row & 7))) != 0;
                    if (r.columnTypes[c] == QwpConstants.TYPE_LONG) {
                        final long value = isNull ? Numbers.LONG_NULL : rd.i64();
                        if (c == 0) {
                            r.longs.add(value);
                        }
                    } else if (c == firstVarcharColumn) {
                        String value = null;
                        if (!isNull) {
                            final int lo = new WireReader(frame, valuesStart + 4 * dense).i32();
                            final int hi = new WireReader(frame, valuesStart + 4 * (dense + 1)).i32();
                            value = new String(frame, heapStart + lo, hi - lo, StandardCharsets.UTF_8);
                            dense++;
                        }
                        r.strings.add(value);
                    }
                }
                if (r.columnTypes[c] == QwpConstants.TYPE_VARCHAR) {
                    rd.pos = valuesStart;
                    rd.skip(4 * (nonNullCount + 1) + new WireReader(frame, valuesStart + 4 * nonNullCount).i32());
                }
            }
            Assert.assertEquals("bytes left after the last column", 0, rd.remaining());
        }
    }

    /**
     * Little-endian reader over a server frame.
     */
    private static final class WireReader {
        private final byte[] buf;
        private int pos;

        WireReader(byte[] buf, int pos) {
            this.buf = buf;
            this.pos = pos;
        }

        int i32() {
            int v = 0;
            for (int i = 0; i < 4; i++) {
                v |= (buf[pos++] & 0xFF) << (8 * i);
            }
            return v;
        }

        long i64() {
            long v = 0;
            for (int i = 0; i < 8; i++) {
                v |= (long) (buf[pos++] & 0xFF) << (8 * i);
            }
            return v;
        }

        int remaining() {
            return buf.length - pos;
        }

        void skip(int bytes) {
            Assert.assertTrue("frame ends before " + bytes + " bytes", bytes <= remaining());
            pos += bytes;
        }

        int u16() {
            return (buf[pos++] & 0xFF) | (buf[pos++] & 0xFF) << 8;
        }

        int u8() {
            Assert.assertTrue("frame ends before the next field", remaining() > 0);
            return buf[pos++] & 0xFF;
        }

        String utf8(int len) {
            skip(len);
            return new String(buf, pos - len, len, StandardCharsets.UTF_8);
        }

        long varint() {
            long v = 0;
            for (int shift = 0; ; shift += 7) {
                final int b = u8();
                v |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return v;
                }
                Assert.assertTrue("varint longer than 10 bytes", shift < 63);
            }
        }
    }
}
