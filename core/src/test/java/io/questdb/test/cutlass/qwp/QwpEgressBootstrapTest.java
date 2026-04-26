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
import io.questdb.cutlass.qwp.server.egress.QwpEgressProcessorState;
import io.questdb.cutlass.qwp.server.egress.QwpEgressUpgradeProcessor;
import io.questdb.griffin.CompiledQuery;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * End-to-end Phase-1 smoke test for QWP egress: boot an embedded QuestDB,
 * populate a table via SQL, open {@link QwpQueryClient} against /read/v1,
 * issue a SELECT, and assert the decoded batches match.
 */
public class QwpEgressBootstrapTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testAllPrimitiveTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                // part_ts is a separate designated timestamp so the existing ts column can still
                // round-trip a NULL (row 1) without violating WAL partition assignment.
                serverMain.execute("""
                        CREATE TABLE allp(
                            b BOOLEAN,
                            bt BYTE,
                            sh SHORT,
                            ch CHAR,
                            i INT,
                            l LONG,
                            f FLOAT,
                            d DOUBLE,
                            dt DATE,
                            ts TIMESTAMP,
                            s STRING,
                            v VARCHAR,
                            sy SYMBOL,
                            part_ts TIMESTAMP
                        ) TIMESTAMP(part_ts) PARTITION BY DAY WAL
                        """);
                // Row 0: all set. Row 1: nulls (only types that can hold NULL).
                serverMain.execute("""
                        INSERT INTO allp VALUES
                            (true,  127, 32767, 'A', 999,  999999999999L, 1.5f, 3.14,
                             '2024-01-01'::DATE, '2024-01-01T00:00:00.000Z', 'hello', 'world', 'SYM1', 1::TIMESTAMP),
                            (false, 0,   0,     'B', NULL, NULL,          NULL, NULL,
                             NULL,               NULL,                       NULL,    NULL,    NULL,   2::TIMESTAMP)
                        """);
                serverMain.awaitTable("allp");

                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    // Explicit column list (not SELECT *) so the test still sees 13 columns and
                    // the per-column assertions keep addressing the same logical columns.
                    client.execute(
                            "SELECT b, bt, sh, ch, i, l, f, d, dt, ts, s, v, sy FROM allp",
                            new QwpColumnBatchHandler() {
                                @Override
                                public void onBatch(QwpColumnBatch batch) {
                                    Assert.assertEquals(13, batch.getColumnCount());
                                    Assert.assertEquals(2, batch.getRowCount());

                                    // Row 0 assertions
                                    Assert.assertTrue(batch.getBoolValue(0, 0));                       // BOOLEAN
                                    Assert.assertEquals((byte) 127, batch.getByteValue(1, 0));         // BYTE
                                    Assert.assertEquals((short) 32767, batch.getShortValue(2, 0));     // SHORT
                                    Assert.assertEquals('A', batch.getCharValue(3, 0));                // CHAR
                                    Assert.assertEquals(999, batch.getIntValue(4, 0));                 // INT
                                    Assert.assertEquals(999_999_999_999L, batch.getLongValue(5, 0));   // LONG
                                    Assert.assertEquals(1.5f, batch.getFloatValue(6, 0), 0.0f);        // FLOAT
                                    Assert.assertEquals(3.14, batch.getDoubleValue(7, 0), 1e-9);       // DOUBLE
                                    Assert.assertTrue(batch.getLongValue(8, 0) > 0);                   // DATE
                                    Assert.assertTrue(batch.getLongValue(9, 0) > 0);                   // TIMESTAMP
                                    Assert.assertEquals("hello", batch.getString(10, 0));         // STRING
                                    Assert.assertEquals("world", batch.getString(11, 0));         // VARCHAR
                                    Assert.assertEquals("SYM1", batch.getString(12, 0));          // SYMBOL

                                    // Row 1: NULL-capable types come back as null
                                    Assert.assertTrue(batch.isNull(4, 1));   // INT
                                    Assert.assertTrue(batch.isNull(5, 1));   // LONG
                                    Assert.assertTrue(batch.isNull(6, 1));   // FLOAT
                                    Assert.assertTrue(batch.isNull(7, 1));   // DOUBLE
                                    Assert.assertTrue(batch.isNull(8, 1));   // DATE
                                    Assert.assertTrue(batch.isNull(9, 1));   // TIMESTAMP
                                    Assert.assertTrue(batch.isNull(10, 1));  // STRING
                                    Assert.assertTrue(batch.isNull(11, 1));  // VARCHAR
                                    Assert.assertTrue(batch.isNull(12, 1));  // SYMBOL
                                    // BOOLEAN/BYTE/SHORT/CHAR cannot represent NULL in QuestDB -- stored values round-trip.
                                }

                                @Override
                                public void onEnd(long totalRows) {
                                }

                                @Override
                                public void onError(byte status, String message) {
                                    Assert.fail("egress query error: " + message);
                                }
                            });
                }
            }
        });
    }

    /**
     * C5: ARRAY columns. Phase-1 client doesn't expose typed array accessors yet, but the
     * raw-bytes pass-through must work end-to-end without crashing. Verifies the wire format
     * (server emit -> client decode) -- the per-row bytes land at known offsets and the schema
     * surfaces the correct wire type.
     */
    @Test
    public void testArrayColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                // QuestDB SQL only supports DOUBLE[] arrays today; LONG[] support exists in the
                // wire format but isn't surfaced via SQL CREATE TABLE. Restrict to DOUBLE[] here.
                serverMain.execute("CREATE TABLE arr_t(d DOUBLE[], ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO arr_t VALUES (ARRAY[1.0, 2.0, 3.0], 1::TIMESTAMP)");
                serverMain.execute("INSERT INTO arr_t VALUES (ARRAY[4.0, 5.0], 2::TIMESTAMP)");
                serverMain.awaitTable("arr_t");

                final int[] count = {0};
                final byte[] dWireType = {0};
                final ObjList<double[]> rowElements = new ObjList<>();
                final IntList rowDims = new IntList();
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    // No ORDER BY -- QuestDB does not support ORDER BY on DOUBLE[] columns.
                    // Monotonic designated timestamps keep WAL scan order aligned with insert order.
                    client.execute("SELECT d FROM arr_t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            dWireType[0] = batch.getColumnWireType(0);
                            for (int r = 0; r < batch.getRowCount(); r++, count[0]++) {
                                Assert.assertFalse("array row " + r + " must be non-null", batch.isNull(0, r));
                                rowDims.add(batch.getArrayNDims(0, r));
                                rowElements.add(batch.getDoubleArrayElements(0, r));
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }
                Assert.assertEquals(2, count[0]);
                Assert.assertEquals(io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_DOUBLE_ARRAY, dWireType[0]);
                // Both rows are 1-D arrays; element content must round-trip exactly.
                Assert.assertEquals(1, rowDims.getQuick(0));
                Assert.assertEquals(1, rowDims.getQuick(1));
                Assert.assertArrayEquals(new double[]{1.0, 2.0, 3.0}, rowElements.getQuick(0), 0.0);
                Assert.assertArrayEquals(new double[]{4.0, 5.0}, rowElements.getQuick(1), 0.0);
            }
        });
    }

    /**
     * Two back-to-back queries on one WebSocket connection must both succeed. Exercises
     * the dispatch loop resetting between requests and the per-query state cleanup.
     */
    @Test
    public void testBackToBackQueriesOnOneConnection() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE cc(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO cc VALUES (1, 1::TIMESTAMP)");
                serverMain.awaitTable("cc");
                final int[] rows = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    for (int i = 0; i < 2; i++) {
                        client.execute("SELECT x FROM cc", new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                for (int r = 0; r < batch.getRowCount(); r++) rows[0]++;
                            }

                            @Override
                            public void onEnd(long totalRows) {
                            }

                            @Override
                            public void onError(byte status, String message) {
                                Assert.fail("egress error: " + message);
                            }
                        });
                    }
                }
                Assert.assertEquals(2, rows[0]);
            }
        });
    }

    /**
     * BINARY column round-trip. Wire format mirrors VARCHAR -- opaque bytes via offsets + heap.
     * Verifies:
     * - the new TYPE_BINARY (0x17) wire code surfaces in the schema
     * - non-null binary values are non-null on the client and round-trip byte-for-byte
     * - explicit NULL binary surfaces as isNull
     * - the zero-alloc {@code getBinaryA} view returns the same content as the heap-allocating
     * {@code getBinary} byte[] convenience
     */
    @Test
    public void testBinaryColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE bin_t(b BINARY, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                // Two non-null random binary values + one explicit NULL.
                serverMain.execute("INSERT INTO bin_t SELECT rnd_bin(8, 8, 0), x::TIMESTAMP FROM long_sequence(2)");
                serverMain.execute("INSERT INTO bin_t VALUES (CAST(NULL AS BINARY), 3::TIMESTAMP)");
                serverMain.awaitTable("bin_t");

                final boolean[] nullFlags = new boolean[3];
                final int[] sizes = new int[3];
                final byte[][] heapCopies = new byte[3][];
                final byte[][] viewCopies = new byte[3][];
                final byte[] wireType = {0};
                final int[] count = {0};

                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT b FROM bin_t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            wireType[0] = batch.getColumnWireType(0);
                            for (int r = 0; r < batch.getRowCount(); r++, count[0]++) {
                                if (batch.isNull(0, r)) {
                                    nullFlags[count[0]] = true;
                                    continue;
                                }
                                // Zero-alloc native view
                                io.questdb.client.std.bytes.DirectByteSequence view = batch.getBinaryA(0, r);
                                int sz = view.size();
                                sizes[count[0]] = sz;
                                byte[] viaView = new byte[sz];
                                for (int i = 0; i < sz; i++) viaView[i] = view.byteAt(i);
                                viewCopies[count[0]] = viaView;
                                // Heap-allocating convenience
                                heapCopies[count[0]] = batch.getBinary(0, r);
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }

                Assert.assertEquals(3, count[0]);
                Assert.assertEquals(io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_BINARY, wireType[0]);
                // Two non-nulls, one null
                Assert.assertFalse(nullFlags[0]);
                Assert.assertFalse(nullFlags[1]);
                Assert.assertTrue("explicit NULL must surface as null", nullFlags[2]);
                // rnd_bin(8, 8, 0) generates 8-byte values
                Assert.assertEquals(8, sizes[0]);
                Assert.assertEquals(8, sizes[1]);
                // Native view and heap copy must agree
                Assert.assertArrayEquals(viewCopies[0], heapCopies[0]);
                Assert.assertArrayEquals(viewCopies[1], heapCopies[1]);
            }
        });
    }

    /**
     * Regression for C7: closing the client while a user thread is blocked inside
     * {@code execute()} must not livelock. Before the fix, the I/O thread shut
     * down without pushing a sentinel onto the events queue, leaving any thread
     * blocked on {@code events.take()} stuck forever. After the fix, the I/O
     * thread emits a synthetic error event in its {@code finally} block.
     */
    @Test(timeout = 30_000)
    public void testCloseWhileExecuteDoesNotHang() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE slow(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO slow SELECT x, x::TIMESTAMP FROM long_sequence(50000)");
                serverMain.awaitTable("slow");

                final java.util.concurrent.CountDownLatch firstBatchSeen = new java.util.concurrent.CountDownLatch(1);
                final java.util.concurrent.CountDownLatch executeReturned = new java.util.concurrent.CountDownLatch(1);
                final java.util.concurrent.atomic.AtomicReference<String> errorMessage = new java.util.concurrent.atomic.AtomicReference<>();
                final java.util.concurrent.atomic.AtomicBoolean endSeen = new java.util.concurrent.atomic.AtomicBoolean();

                final QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT);
                client.connect();

                Thread queryThread = new Thread(() -> {
                    try {
                        client.execute("SELECT x FROM slow ORDER BY x", new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                firstBatchSeen.countDown();
                                // Sleep just long enough that close() lands while we're still
                                // mid-onBatch (so the I/O thread is parked on pendingRelease.take()
                                // when shutdown arrives). MUST NOT be interrupted by the test --
                                // an interrupt would also pop the user thread out of any blocking
                                // events.take() it later parks on, masking the C7 fix that this
                                // test exists to regression-check (the I/O thread's finally-block
                                // synthetic error is what must wake events.take, not an interrupt).
                                try {
                                    Thread.sleep(2_000);
                                } catch (InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                }
                            }

                            @Override
                            public void onEnd(long totalRows) {
                                endSeen.set(true);
                            }

                            @Override
                            public void onError(byte status, String message) {
                                errorMessage.set(message);
                            }
                        });
                    } finally {
                        executeReturned.countDown();
                    }
                }, "qwp-execute-under-test");
                queryThread.setDaemon(true);
                queryThread.start();

                // Wait for the query to actually be in-flight before we close.
                Assert.assertTrue("first batch not received within 15s",
                        firstBatchSeen.await(15, java.util.concurrent.TimeUnit.SECONDS));

                // Close from the main thread while the query thread is parked in onBatch.
                // Without the C7 fix, the user thread would hang on events.take() forever
                // after the I/O thread exited and the user returned from its in-handler
                // sleep -- the I/O thread would be gone but no sentinel event would be
                // there to unblock the next take().
                client.close();

                // No interrupt() here: the user thread must exit onBatch on its own and
                // then wake from events.take() on the C7 sentinel error. 15s window
                // covers the in-handler sleep + close + take + return.
                Assert.assertTrue("execute() did not return within 15s after close()",
                        executeReturned.await(15, java.util.concurrent.TimeUnit.SECONDS));
                Assert.assertNotNull("expected a synthetic error notifying that the query was aborted",
                        errorMessage.get());
                Assert.assertFalse("RESULT_END must not have been delivered after close()", endSeen.get());
            }
        });
    }

    /**
     * Phase 1 supports a single in-flight query per connection. If a second
     * QUERY_REQUEST arrives while the first is still streaming (e.g., the send side
     * is parked), the server must reject it with QUERY_ERROR rather than overwrite
     * streamingFactory/streamingCursor (which would leak native resources).
     * <p>
     * Hard to trigger via the public QwpQueryClient which serialises queries per
     * connection; this regression test instead asserts the state guard directly.
     */
    @Test
    public void testConcurrentQueryRejectedInPhaseOne() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            io.questdb.cairo.CairoConfiguration cfg = new DefaultTestCairoConfiguration(root);
            try (QwpEgressProcessorState state = new QwpEgressProcessorState(cfg)) {
                Assert.assertFalse("state starts inactive", state.isStreamingActive());
                // Simulate a streaming-active state without actual native resources by
                // calling beginStreaming with null factory/cursor. The defensive endStreaming
                // inside beginStreaming is idempotent for null.
                state.beginStreaming(1L, null, null, 0, 0, false, 0L, null);
                Assert.assertTrue(state.isStreamingActive());
                // A second beginStreaming must not double-free (endStreaming handles nulls)
                // and must transition to the new requestId cleanly.
                state.beginStreaming(2L, null, null, 0, 0, false, 0L, null);
                Assert.assertTrue(state.isStreamingActive());
                state.endStreaming();
                Assert.assertFalse(state.isStreamingActive());
            }
        });
    }

    @Test
    public void testDecimal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE dec(d64 DECIMAL(18,4), d128 DECIMAL(38,6), ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO dec VALUES (1234.5678m, 987654321.123456m, 1::TIMESTAMP)");
                serverMain.execute("INSERT INTO dec VALUES (CAST(NULL AS DECIMAL(18,4)), CAST(NULL AS DECIMAL(38,6)), 2::TIMESTAMP)");
                serverMain.awaitTable("dec");

                final Long[] d64 = new Long[2];
                final long[][] d128 = new long[2][];
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("SELECT d64, d128 FROM dec", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                d64[r] = batch.isNull(0, r) ? null : batch.getLongValue(0, r);
                                d128[r] = batch.isNull(1, r) ? null : new long[]{
                                        batch.getDecimal128Low(1, r),
                                        batch.getDecimal128High(1, r)
                                };
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: " + message);
                        }
                    });
                }
                // 1234.5678 with scale 4 -> unscaled = 12345678
                Assert.assertEquals(Long.valueOf(12_345_678L), d64[0]);
                Assert.assertNull(d64[1]);
                Assert.assertNotNull(d128[0]);
                Assert.assertNull(d128[1]);
            }
        });
    }

    /**
     * Regression: {@code streamResults} used to send {@code RESULT_END} to the
     * kernel BEFORE calling {@code state.endStreaming()} to release the cursor
     * (and therefore the {@code TableReader}). On a fast loopback the client
     * could parse {@code RESULT_END}, fire {@code onEnd}, and race back into a
     * follow-up {@code DROP TABLE} on the same connection before the server
     * thread reached {@code endStreaming}, surfacing as "could not lock '...'
     * [reason='busyReader']" on the DROP. Fixed by releasing the cursor first
     * (see commit "Release egress cursor before sending RESULT_END").
     * <p>
     * Runs under forced send/recv fragmentation so the final send parks and
     * resumes through the dispatcher, widening the window between {@code
     * rawSocket.send} and the trailing {@code endStreaming} under the buggy
     * ordering. Without the fix the DROP fails with "busyReader" on at least
     * one iteration; with the fix the reader is back in the pool before any
     * RESULT_END bytes leave the kernel.
     */
    @Test
    public void testDropAfterSelectReleasesReaderInTime() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.DEBUG_HTTP_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "23",
                    PropertyKey.DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "23"
            )) {
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    final int iterations = 20;
                    for (int i = 0; i < iterations; i++) {
                        final int iter = i;
                        final String table = "drop_race_" + iter;
                        serverMain.execute("CREATE TABLE " + table + "(x LONG, ts TIMESTAMP) "
                                + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                        serverMain.execute("INSERT INTO " + table
                                + " VALUES (1, 1::TIMESTAMP), (2, 2::TIMESTAMP), (3, 3::TIMESTAMP)");
                        serverMain.awaitTable(table);

                        final long[] sum = {0};
                        client.execute("SELECT x FROM " + table, new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                for (int r = 0; r < batch.getRowCount(); r++) {
                                    sum[0] += batch.getLongValue(0, r);
                                }
                            }

                            @Override
                            public void onEnd(long totalRows) {
                            }

                            @Override
                            public void onError(byte status, String message) {
                                Assert.fail("SELECT failed at iteration " + iter + ": " + message);
                            }
                        });
                        Assert.assertEquals("iteration " + iter, 6L, sum[0]);

                        // By the time execute() returned the server had already shipped
                        // RESULT_END. With the fix in place the TableReader was released
                        // BEFORE that send, so this DROP is free to take the exclusive
                        // lock. Without the fix it races into "busyReader".
                        final short[] dropOp = {-1};
                        client.execute("DROP TABLE " + table, new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                Assert.fail("unexpected batch for DROP at iteration " + iter);
                            }

                            @Override
                            public void onEnd(long totalRows) {
                                Assert.fail("unexpected end for DROP at iteration " + iter);
                            }

                            @Override
                            public void onError(byte status, String message) {
                                Assert.fail("DROP TABLE " + table + " failed (iteration " + iter
                                        + ", status=" + status + "): " + message);
                            }

                            @Override
                            public void onExecDone(short opType, long rowsAffected) {
                                dropOp[0] = opType;
                            }
                        });
                        Assert.assertEquals("DROP should succeed at iteration " + iter,
                                CompiledQuery.DROP, dropOp[0]);
                    }
                }
            }
        });
    }

    /**
     * Regression: a per-connection schema-cache poisoning bug in
     * {@link QwpEgressUpgradeProcessor}'s retry-once loop. Pre-fix,
     * {@code findOrAllocateSchemaId()} was called inside the retry loop
     * BEFORE {@code getCursor()}. When cursor acquisition threw
     * {@code TableReferenceOutOfDateException} (e.g. because the cached
     * factory was compiled against a now-dropped table), the catch block
     * freed the factory but did NOT remove the fingerprint from the
     * per-connection schema cache. The retry attempt then saw the
     * fingerprint as "reuse" and shipped the first batch in reference
     * mode against an id the client had never registered; the client
     * decoder rejected with "schema id N not registered on this connection".
     * Post-fix: schema-id allocation moved AFTER the retry loop, so a
     * failed cursor acquisition can never poison the per-connection cache.
     * <p>
     * Trigger: seed connection runs a SELECT (server caches the factory
     * in {@code selectCache} by SQL text); DROP+CREATE the table under
     * the same name with the same shape (factory becomes stale); a fresh
     * connection runs the same SELECT -- {@code selectCache.poll} returns
     * the stale factory, {@code getCursor} throws, and pre-fix the retry
     * would ship reference mode against an id the client never registered.
     * The query cache must be enabled; the test default is off and would
     * hide the bug because the no-op cache never returns a stale factory.
     */
    @Test
    public void testDropRecreateDoesNotPoisonSchemaCache() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_QUERY_CACHE_ENABLED.getEnvVarName(), "true"
            )) {
                final String table = "schema_cache_retry_t";
                final String createSql = "CREATE TABLE " + table
                        + "(v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL";
                final String insertSql = "INSERT INTO " + table + " VALUES (1, 1::TIMESTAMP)";
                final String selectSql = "SELECT v FROM " + table;

                // Seed: create the table and run the SELECT once so the
                // server compiles a RecordCursorFactory and parks it in
                // selectCache on stream end.
                serverMain.execute(createSql);
                serverMain.execute(insertSql);
                serverMain.awaitTable(table);
                try (QwpQueryClient seed = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    seed.connect();
                    assertSelectReturnsOneRow(seed, selectSql, "seed");
                }

                // Trigger: DROP+CREATE (same name + shape -> new internal table
                // id; cached factory is now stale) then run the same SELECT on
                // a fresh connection. Iterate so a regression reproduces even
                // on the rare "lucky" first cycle after a drop.
                final int cycles = 5;
                for (int i = 0; i < cycles; i++) {
                    serverMain.execute("DROP TABLE " + table);
                    serverMain.execute(createSql);
                    serverMain.execute(insertSql);
                    serverMain.awaitTable(table);
                    try (QwpQueryClient trigger = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                        trigger.connect();
                        assertSelectReturnsOneRow(trigger, selectSql, "trigger-" + i);
                    }
                }
            }
        });
    }

    /**
     * C5: empty result set must still produce one RESULT_BATCH (with 0 rows + the schema)
     * followed by RESULT_END. Otherwise the client never sees the schema and onEnd would
     * never fire. Verifies the empty-cursor branch in {@code streamResults}.
     */
    @Test
    public void testEmptyResultSet() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE empty_t(id LONG, name STRING, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                // No INSERT -- table is empty.

                final int[] batchCount = {0};
                final int[] rowCount = {0};
                final int[] schemaColCount = {-1};
                final boolean[] endSeen = {false};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT id, name FROM empty_t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            batchCount[0]++;
                            schemaColCount[0] = batch.getColumnCount();
                            rowCount[0] += batch.getRowCount();
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            endSeen[0] = true;
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }
                Assert.assertEquals("expected one batch (with the schema, zero rows)", 1, batchCount[0]);
                Assert.assertEquals(0, rowCount[0]);
                Assert.assertEquals("schema must surface even with no rows", 2, schemaColCount[0]);
                Assert.assertTrue("RESULT_END must always fire", endSeen[0]);
            }
        });
    }

    /**
     * Spec {@code docs/QWP_EGRESS_EXTENSION.md:278-279} requires the server to send
     * one RESULT_BATCH with row_count = 0 on every empty result, including when the
     * schema id is reused from an earlier query on the same connection. Otherwise
     * the client's per-query onBatch callback never fires and downstream consumers
     * that rely on it to learn per-query column metadata silently break.
     * <p>
     * Two queries on one connection with identical column shape: query 1 returns
     * rows (schema id allocated, full schema shipped), query 2 returns an empty
     * result with the same shape (schemaAlreadyKnown == true). Both queries must
     * deliver at least one RESULT_BATCH to the client.
     */
    @Test
    public void testEmptyResultSetOnReusedSchemaStillDeliversOneBatch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE reuse_t(id LONG, name STRING, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO reuse_t VALUES (1, 'one', 1::TIMESTAMP), "
                        + "(2, 'two', 2::TIMESTAMP), (3, 'three', 3::TIMESTAMP)");
                serverMain.awaitTable("reuse_t");

                final int[] q1Batches = {0};
                final int[] q1Rows = {0};
                final int[] q2Batches = {0};
                final int[] q2Rows = {0};
                final int[] q2SchemaCols = {-1};
                final boolean[] q2EndSeen = {false};

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();

                    // Query 1: populates the connection-scoped schema cache with a fresh id
                    // for shape (id LONG, name STRING).
                    client.execute("SELECT id, name FROM reuse_t WHERE id >= 0", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            q1Batches[0]++;
                            q1Rows[0] += batch.getRowCount();
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error on q1: " + message);
                        }
                    });

                    // Query 2: identical column shape -> server hits schemaAlreadyKnown=true.
                    // WHERE predicate filters everything out -> empty cursor.
                    client.execute("SELECT id, name FROM reuse_t WHERE id < 0", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            q2Batches[0]++;
                            q2SchemaCols[0] = batch.getColumnCount();
                            q2Rows[0] += batch.getRowCount();
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            q2EndSeen[0] = true;
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error on q2: " + message);
                        }
                    });
                }
                Assert.assertEquals("q1 must deliver at least one batch", 1, q1Batches[0]);
                Assert.assertEquals(3, q1Rows[0]);
                Assert.assertTrue("q2 RESULT_END must still fire", q2EndSeen[0]);
                Assert.assertEquals(0, q2Rows[0]);
                Assert.assertEquals("empty-result q2 on reused schema must still deliver one RESULT_BATCH (spec sec. 7)",
                        1, q2Batches[0]);
                Assert.assertEquals("schema must surface to the handler on q2 even with no rows",
                        2, q2SchemaCols[0]);
            }
        });
    }

    /**
     * Regression / defense-in-depth for C1: many queries that fail at various stages
     * of the server-side handle path (compile error, table-not-found, non-SELECT)
     * must not leak native resources. assertMemoryLeak catches any leaked factory,
     * cursor, or buffer scratch.
     */
    @Test
    public void testFailedQueriesDoNotLeak() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE leakcheck(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO leakcheck VALUES (1, 1::TIMESTAMP), (2, 2::TIMESTAMP), (3, 3::TIMESTAMP)");
                serverMain.awaitTable("leakcheck");

                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    final int[] errorCount = {0};
                    final int[] successCount = {0};
                    final int[] execDoneCount = {0};
                    QwpColumnBatchHandler handler = new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            successCount[0]++;
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            errorCount[0]++;
                        }

                        @Override
                        public void onExecDone(short opType, long rowsAffected) {
                            execDoneCount[0]++;
                        }
                    };
                    // Mix of success and failure paths -- each failure exercises a different
                    // catch arm in handleQueryRequest. With C1 unfixed, the
                    // factory.getCursor() / metadata path leaks RecordCursorFactory
                    // instances on any failure between getCursor and beginStreaming.
                    for (int i = 0; i < 25; i++) {
                        client.execute("SELECT x FROM leakcheck ORDER BY x", handler);    // ok (batch)
                        client.execute("SELECT FROM leakcheck", handler);                  // syntax error
                        client.execute("SELECT * FROM does_not_exist", handler);           // table not found
                        client.execute("INSERT INTO leakcheck VALUES (4, 4::TIMESTAMP)", handler);       // DDL succeeds -> execDone
                    }
                    Assert.assertEquals(25, successCount[0]);
                    Assert.assertEquals(50, errorCount[0]);
                    Assert.assertEquals(25, execDoneCount[0]);
                }
            }
        });
    }

    @Test
    public void testFromConfigConnectString() throws Exception {
        // The fromConfig(String) factory mirrors Sender.fromConfig: schema::key=value;...
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE cs(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO cs VALUES (42, 1::TIMESTAMP), (43, 2::TIMESTAMP)");
                serverMain.awaitTable("cs");

                LongList rows = new LongList();
                String conf = "ws::addr=127.0.0.1:" + HTTP_PORT + ";path=/read/v1;client_id=conf-test/1.0;buffer_pool_size=2;";
                try (QwpQueryClient client = QwpQueryClient.fromConfig(conf)) {
                    client.connect();
                    client.execute("SELECT x FROM cs ORDER BY x", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                rows.add(batch.getLongValue(0, r));
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: " + message);
                        }
                    });
                }
                Assert.assertEquals(2, rows.size());
                Assert.assertEquals(42L, rows.getQuick(0));
                Assert.assertEquals(43L, rows.getQuick(1));
            }
        });
    }

    @Test
    public void testFromConfigRejectsBadSchema() {
        try {
            QwpQueryClient.fromConfig("http::addr=localhost:9000;").close();
            Assert.fail("expected unsupported-schema error");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("unsupported schema"));
        }
        try {
            QwpQueryClient.fromConfig("ws::").close();
            Assert.fail("expected missing-addr error");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("addr"));
        }
        try {
            QwpQueryClient.fromConfig("ws::addr=h:9000;buffer_pool_size=0;").close();
            Assert.fail("expected bad pool-size error");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("buffer_pool_size"));
        }
    }

    @Test
    public void testGeohash() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE geo(g20 GEOHASH(20b), g40 GEOHASH(40b), ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                // Geohash literal: #<base-32-chars>; 4 chars = 20 bits, 8 chars = 40 bits
                serverMain.execute("INSERT INTO geo VALUES (#dr5r, #dr5rsjut, 1::TIMESTAMP)");
                serverMain.execute(
                        "INSERT INTO geo VALUES (CAST(NULL AS GEOHASH(20b)), CAST(NULL AS GEOHASH(40b)), 2::TIMESTAMP)"
                );
                serverMain.awaitTable("geo");

                final Long[] g20Values = new Long[2];
                final Long[] g40Values = new Long[2];
                final int[] precisionBits = new int[2];
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("SELECT g20, g40 FROM geo", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            precisionBits[0] = batch.getGeohashPrecisionBits(0);
                            precisionBits[1] = batch.getGeohashPrecisionBits(1);
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                g20Values[r] = batch.isNull(0, r) ? null : batch.getGeohashValue(0, r);
                                g40Values[r] = batch.isNull(1, r) ? null : batch.getGeohashValue(1, r);
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: " + message);
                        }
                    });
                }
                Assert.assertEquals(20, precisionBits[0]);
                Assert.assertEquals(40, precisionBits[1]);
                Assert.assertNotNull(g20Values[0]);
                Assert.assertNotNull(g40Values[0]);
                Assert.assertNull(g20Values[1]);
                Assert.assertNull(g40Values[1]);
            }
        });
    }

    /**
     * C3: GeoHash NULL handling across all four storage widths. QuestDB stores NULL as -1 at
     * each width (BYTE_NULL, SHORT_NULL, INT_NULL, NULL). Sign-extension when {@code Record.getGeoByte/Short/Int/Long}
     * returns into a {@code long} must always produce -1L for the comparison in
     * {@code QwpResultBatchBuffer.appendRow} to fire.
     */
    @Test
    public void testGeohashNullAcrossAllWidths() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                // 4->GEOBYTE, 8->GEOSHORT, 16->GEOINT, 40/60->GEOLONG.
                serverMain.execute("""
                        CREATE TABLE geo_nulls(
                            g4 GEOHASH(4b),
                            g8 GEOHASH(8b),
                            g16 GEOHASH(16b),
                            g40 GEOHASH(40b),
                            g60 GEOHASH(60b),
                            ts TIMESTAMP
                        ) TIMESTAMP(ts) PARTITION BY DAY WAL
                        """);
                serverMain.execute("""
                        INSERT INTO geo_nulls VALUES (
                            CAST(NULL AS GEOHASH(4b)),
                            CAST(NULL AS GEOHASH(8b)),
                            CAST(NULL AS GEOHASH(16b)),
                            CAST(NULL AS GEOHASH(40b)),
                            CAST(NULL AS GEOHASH(60b)),
                            1::TIMESTAMP
                        )
                        """);
                // A non-null row to ensure the column isn't always null.
                serverMain.execute("INSERT INTO geo_nulls VALUES (#0, #00, #0000, #00000000, #000000000000, 2::TIMESTAMP)");
                serverMain.awaitTable("geo_nulls");

                final boolean[][] isNull = new boolean[2][5];
                final int[] count = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT g4, g8, g16, g40, g60 FROM geo_nulls", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++, count[0]++) {
                                for (int c = 0; c < 5; c++) isNull[count[0]][c] = batch.isNull(c, r);
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: " + message);
                        }
                    });
                }
                Assert.assertEquals(2, count[0]);
                for (int c = 0; c < 5; c++) {
                    Assert.assertTrue("NULL row, col " + c + " (width-class " + (c == 0 ? "byte" : c == 1 ? "short" : c == 2 ? "int" : "long") + ")", isNull[0][c]);
                }
                for (int c = 0; c < 5; c++) {
                    Assert.assertFalse("non-null row, col " + c, isNull[1][c]);
                }
            }
        });
    }

    @Test
    public void testIpv4NullSentinel() throws Exception {
        // IPv4 is a distinct wire type (TYPE_IPv4 = 0x18), not just TYPE_INT. The schema
        // surfaces the wire type so a client can render addresses correctly. NULL is signalled
        // via the standard null bitmap; on the server side the value 0 (Numbers.IPv4_NULL --
        // i.e. the address 0.0.0.0) is detected before the value is appended, marking the row
        // NULL. QuestDB itself treats 0.0.0.0 as NULL, so both '0.0.0.0' and explicit NULL
        // inserts come back as null. A non-zero address surfaces as non-null with the address
        // bits intact.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE ipx(addr IPv4, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO ipx VALUES
                            ('0.0.0.0',     1::TIMESTAMP),
                            (NULL,          2::TIMESTAMP),
                            ('192.168.1.1', 3::TIMESTAMP)
                        """);
                serverMain.awaitTable("ipx");

                final boolean[] nullSeen = new boolean[3];
                final long[] valueSeen = new long[3];
                final byte[] wireType = {0};
                final int[] count = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT addr FROM ipx", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            wireType[0] = batch.getColumnWireType(0);
                            for (int r = 0; r < batch.getRowCount(); r++, count[0]++) {
                                nullSeen[count[0]] = batch.isNull(0, r);
                                if (!nullSeen[count[0]]) {
                                    valueSeen[count[0]] = batch.getIntValue(0, r);
                                }
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: " + message);
                        }
                    });
                }
                Assert.assertEquals(3, count[0]);
                Assert.assertEquals(io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_IPv4, wireType[0]);
                Assert.assertTrue("'0.0.0.0' is the IPv4 NULL sentinel -- must surface as null", nullSeen[0]);
                Assert.assertTrue("explicit NULL must surface as null", nullSeen[1]);
                Assert.assertFalse("real address must surface as non-null", nullSeen[2]);
                // 192.168.1.1 in network byte order = 0xC0A80101 = 3232235777. QuestDB stores IPv4 as int
                // in big-endian order; verify the round-trip preserves the bit pattern.
                Assert.assertEquals(0xC0A80101L, valueSeen[2] & 0xFFFFFFFFL);
            }
        });
    }

    @Test
    public void testLargeResultSet() throws Exception {
        // 20,000 rows -> spans at least 5 batches with MAX_ROWS_PER_BATCH=4096.
        // Exercises: schema-reference mode (mode 0x01) after the first batch,
        // client recv-buffer growth, multi-batch reassembly on decode loop.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE big(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute(
                        "INSERT INTO big SELECT x, x::TIMESTAMP FROM long_sequence(20000)"
                );
                serverMain.awaitTable("big");

                int[] count = {0};
                int[] batches = {0};
                long[] sum = {0};
                long[] serverTotalRows = {-1};
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("SELECT x FROM big", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            batches[0]++;
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                sum[0] += batch.getLongValue(0, r);
                                count[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            serverTotalRows[0] = totalRows;
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: " + message);
                        }
                    });
                }
                Assert.assertEquals(20_000, count[0]);
                Assert.assertTrue("expected multi-batch streaming, got " + batches[0] + " batches",
                        batches[0] > 1);
                // sum(1..20000) = n(n+1)/2 = 20000 * 20001 / 2
                Assert.assertEquals(200_010_000L, sum[0]);
                Assert.assertEquals("RESULT_END.total_rows must equal rows streamed", 20_000L, serverTotalRows[0]);
            }
        });
    }

    /**
     * C5: many unique symbols across multiple batches -> exercises both the per-batch dict
     * growth and the schema-reference (mode 0x01) path on batches 2+.
     */
    @Test
    public void testManyUniqueSymbolsSchemaReference() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE sym_t(s SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                // Spans multiple batches by using 2 * MAX_ROWS_PER_BATCH rows with
                // 1_000 unique symbols. Sized off the constant so a future bump to
                // the batch cap still leaves this test meaningfully multi-batch.
                int totalRows = 2 * QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH;
                serverMain.execute("INSERT INTO sym_t SELECT 'sym_' || (x % 1000)::STRING, x::TIMESTAMP FROM long_sequence("
                        + totalRows + ")");
                serverMain.awaitTable("sym_t");

                final int[] rows = {0};
                final int[] batches = {0};
                final java.util.Set<String> distinct = new java.util.HashSet<>();
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT s FROM sym_t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            batches[0]++;
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                distinct.add(batch.getString(0, r));
                                rows[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }
                Assert.assertEquals(totalRows, rows[0]);
                Assert.assertTrue("expected multi-batch streaming, got " + batches[0], batches[0] > 1);
                Assert.assertEquals("1000 unique symbols expected", 1000, distinct.size());
            }
        });
    }

    /**
     * Regression for C2: a multi-batch query must produce strictly monotonic batch
     * sequence numbers. The streaming-state bookkeeping advances batch_seq the
     * moment the bytes are committed to the response sink (before any potential
     * PeerIsSlowToReadException), so resume continues with the next sequence.
     * Without the fix, parking and resuming could re-emit a batch labelled with
     * the previous seq number.
     */
    @Test
    public void testMultiBatchSeqIsMonotonic() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE seqcheck(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                // 20_000 rows across multiple batches (server caps batches at 4096 rows).
                serverMain.execute(
                        "INSERT INTO seqcheck SELECT x, x::TIMESTAMP FROM long_sequence(20000)");
                serverMain.awaitTable("seqcheck");

                final LongList seenSeqs = new LongList();
                final long[] firstValuePerBatch = {-1};
                final long[] totalRows = {0};

                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("SELECT x FROM seqcheck ORDER BY x", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            seenSeqs.add(batch.batchSeq());
                            // Capture the first value of each batch: must equal totalRows + 1
                            // (rows are 1-indexed). Any duplicate batch with different rows
                            // shows up here as a non-contiguous start value.
                            if (batch.getRowCount() > 0 && firstValuePerBatch[0] == -1) {
                                firstValuePerBatch[0] = batch.getLongValue(0, 0);
                            }
                            totalRows[0] += batch.getRowCount();
                        }

                        @Override
                        public void onEnd(long rows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }

                Assert.assertEquals(20_000, totalRows[0]);
                Assert.assertTrue("expected multiple batches, got " + seenSeqs.size(), seenSeqs.size() > 1);
                Assert.assertEquals("first streamed row must be x=1", 1L, firstValuePerBatch[0]);
                // Each seq must equal its index in the stream (no duplicates, no gaps).
                for (int i = 0; i < seenSeqs.size(); i++) {
                    Assert.assertEquals("batch index " + i + " expected seq=" + i, i, seenSeqs.getQuick(i));
                }
            }
        });
    }

    /**
     * C2: NaN is QuestDB's NULL sentinel for FLOAT/DOUBLE. The egress wire format inherits this
     * convention -- both an explicit NULL and a "legitimate" NaN (e.g., 0.0/0.0) come back as null.
     * Pin the documented behavior.
     */
    @Test
    public void testNaNMapsToNull() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE nans(d DOUBLE, f FLOAT, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO nans VALUES (1.5, 2.5, 1::TIMESTAMP)");
                // 0/0 produces NaN at SQL level -- indistinguishable from explicit NULL on the wire.
                serverMain.execute("INSERT INTO nans VALUES (0.0/0.0, CAST(0.0/0.0 AS FLOAT), 2::TIMESTAMP)");
                serverMain.execute("INSERT INTO nans VALUES (NULL, NULL, 3::TIMESTAMP)");
                serverMain.awaitTable("nans");

                final boolean[] nullD = new boolean[3];
                final boolean[] nullF = new boolean[3];
                final int[] count = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT d, f FROM nans", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++, count[0]++) {
                                nullD[count[0]] = batch.isNull(0, r);
                                nullF[count[0]] = batch.isNull(1, r);
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: " + message);
                        }
                    });
                }
                Assert.assertEquals(3, count[0]);
                Assert.assertFalse("real value is non-null", nullD[0]);
                Assert.assertFalse("real value is non-null", nullF[0]);
                Assert.assertTrue("NaN must surface as null over the wire (QuestDB NaN = NULL convention)", nullD[1]);
                Assert.assertTrue("NaN must surface as null over the wire (QuestDB NaN = NULL convention)", nullF[1]);
                Assert.assertTrue("explicit NULL", nullD[2]);
                Assert.assertTrue("explicit NULL", nullF[2]);
            }
        });
    }

    @Test
    public void testNonSelectSucceedsViaExecDone() throws Exception {
        // DDL over egress lands via EXEC_DONE (msg_kind 0x16). Historically this
        // test asserted a PARSE_ERROR rejection; since the DDL / INSERT / UPDATE
        // execution landed, the same DROP now round-trips with a success ack
        // carrying the op type and zero rows affected.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE dummy(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                final short[] opType = {-1};
                final long[] rowsAffected = {-1L};
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("DROP TABLE dummy", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.fail("unexpected batch");
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.fail("unexpected end");
                        }

                        @Override
                        public void onError(byte s, String m) {
                            Assert.fail("unexpected error: " + m);
                        }

                        @Override
                        public void onExecDone(short ot, long ra) {
                            opType[0] = ot;
                            rowsAffected[0] = ra;
                        }
                    });
                }
                // CompiledQuery.DROP == 7 (see io.questdb.griffin.CompiledQuery).
                Assert.assertEquals(7, opType[0]);
                Assert.assertEquals(0L, rowsAffected[0]);
            }
        });
    }

    /**
     * Boundary: exactly MAX_ROWS_PER_BATCH rows. Streams in a single full batch;
     * RESULT_END arrives with the same row count and no trailing empty batch.
     */
    @Test
    public void testResultExactlyOneFullBatch() throws Exception {
        runBatchBoundary(QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH);
    }

    /**
     * Boundary: MAX_ROWS_PER_BATCH + 1 rows. Streams as one full batch + a second
     * batch carrying the single trailing row. Exercises the split-across-batches path.
     */
    @Test
    public void testResultOneOverBatchBoundary() throws Exception {
        runBatchBoundary(QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH + 1);
    }

    /**
     * Boundary: single-row result. Exercises the "short and fast" cursor path where
     * the full batch + RESULT_END flush together without streaming re-entry.
     */
    @Test
    public void testResultSingleRow() throws Exception {
        runBatchBoundary(1);
    }

    @Test
    public void testSelectLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO t VALUES (1, 1::TIMESTAMP), (2, 2::TIMESTAMP), (3, 3::TIMESTAMP)");
                serverMain.awaitTable("t");

                LongList collected = new LongList();
                long[] totalRowsHolder = {-1L};
                boolean[] errorSeen = {false};

                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("SELECT x FROM t ORDER BY x", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.assertEquals(1, batch.getColumnCount());
                            Assert.assertEquals("x", batch.getColumnName(0));
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                collected.add(batch.getLongValue(0, r));
                            }
                        }

                        @Override
                        public void onEnd(long rows) {
                            totalRowsHolder[0] = rows;
                        }

                        @Override
                        public void onError(byte status, String message) {
                            errorSeen[0] = true;
                            Assert.fail("egress query error: status=" + status + " msg=" + message);
                        }
                    });
                }

                Assert.assertFalse(errorSeen[0]);
                Assert.assertEquals(3, collected.size());
                Assert.assertEquals(1L, collected.getQuick(0));
                Assert.assertEquals(2L, collected.getQuick(1));
                Assert.assertEquals(3L, collected.getQuick(2));
                Assert.assertEquals(3L, totalRowsHolder[0]);
            }
        });
    }

    @Test
    public void testSelectMixedTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE mixed(id LONG, px DOUBLE, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO mixed VALUES
                            (1, 1.5, 'AAPL', '2024-01-01T00:00:00.000Z'),
                            (2, 2.5, 'MSFT', '2024-01-01T00:00:01.000Z'),
                            (3, 3.5, 'AAPL', '2024-01-01T00:00:02.000Z')
                        """);
                serverMain.awaitTxn("mixed", 1);

                final int[] rows = {0};
                final String[] expectedSym = {"AAPL", "MSFT", "AAPL"};
                final double[] expectedPx = {1.5, 2.5, 3.5};

                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("SELECT id, px, sym, ts FROM mixed ORDER BY id", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.assertEquals(4, batch.getColumnCount());
                            for (int r = 0; r < batch.getRowCount(); r++, rows[0]++) {
                                Assert.assertEquals(rows[0] + 1, batch.getLongValue(0, r));
                                Assert.assertEquals(expectedPx[rows[0]], batch.getDoubleValue(1, r), 1e-9);
                                Assert.assertEquals(expectedSym[rows[0]], batch.getString(2, r));
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            // reachable
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: status=" + status + " msg=" + message);
                        }
                    });
                }
                Assert.assertEquals(3, rows[0]);
            }
        });
    }

    @Test
    public void testSelectWithNulls() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE n(x LONG, s STRING, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO n VALUES (1, 'a', 1::TIMESTAMP), (NULL, NULL, 2::TIMESTAMP), (3, 'c', 3::TIMESTAMP)");
                serverMain.awaitTable("n");

                List<Object[]> rows = new ArrayList<>();
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("SELECT x, s FROM n", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                rows.add(new Object[]{
                                        batch.isNull(0, r) ? null : batch.getLongValue(0, r),
                                        batch.getString(1, r)
                                });
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: status=" + status + " msg=" + message);
                        }
                    });
                }
                Assert.assertEquals(3, rows.size());
                Assert.assertEquals(1L, rows.get(0)[0]);
                Assert.assertEquals("a", rows.get(0)[1]);
                Assert.assertNull(rows.get(1)[0]);
                Assert.assertNull(rows.get(1)[1]);
                Assert.assertEquals(3L, rows.get(2)[0]);
                Assert.assertEquals("c", rows.get(2)[1]);
            }
        });
    }

    /**
     * Back-pressure / resume state machine. Streams ~100 000 8-byte rows (~800 KB) while
     * the client handler deliberately sleeps between batches. The server's TCP send
     * buffer will fill mid-stream, {@code rawSocket.send} will throw
     * {@code PeerIsSlowToReadException}, and the upgrade processor will park the
     * connection. {@code resumeSend} must then flush the deferred bytes and continue
     * from the cursor's current position until all rows arrive.
     * <p>
     * Regression: without the resume path wired up, either (a) no rows arrive after the
     * park, or (b) the stream restarts and emits duplicates / wrong total_rows.
     */
    @Test
    public void testSlowConsumerTriggersResumeSend() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                final int totalRows = 100_000;
                serverMain.execute("CREATE TABLE slow_t(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute(
                        "INSERT INTO slow_t SELECT x, x::TIMESTAMP FROM long_sequence(" + totalRows + ")"
                );
                serverMain.awaitTable("slow_t");

                final int[] rows = {0};
                final int[] batches = {0};
                final long[] sum = {0L};
                final long[] serverTotalRows = {-1L};
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("SELECT x FROM slow_t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            batches[0]++;
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                sum[0] += batch.getLongValue(0, r);
                                rows[0]++;
                            }
                            if (batches[0] <= 3) {
                                // Stall the first few callbacks so the server's send buffer fills
                                // and it parks on PeerIsSlowToReadException. The client I/O thread
                                // blocks on freeBuffers.take() while the user thread sleeps here.
                                try {
                                    Thread.sleep(50);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        }

                        @Override
                        public void onEnd(long trs) {
                            serverTotalRows[0] = trs;
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }
                Assert.assertEquals(totalRows, rows[0]);
                Assert.assertTrue("expected multi-batch streaming, got " + batches[0], batches[0] > 1);
                Assert.assertEquals((long) totalRows * (totalRows + 1) / 2L, sum[0]);
                Assert.assertEquals("RESULT_END.total_rows must equal rows streamed after resumeSend",
                        totalRows, serverTotalRows[0]);
            }
        });
    }

    // testTextFrameRejectsConnection deleted: it never sent a TEXT frame. The real
    // close-on-malformed-frame coverage lives in testFragmentedBinaryFrameRejectsConnection.
    // CANCEL / CREDIT decoder coverage lives in QwpEgressRequestDecoderTest#testCancelBody
    // and #testCreditBody. End-to-end CANCEL / CREDIT client emission is a Phase 2 item.

    @Test
    public void testSqlSyntaxError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain ignored = startWithEnvVariables()) {
                final byte[] errorStatus = {(byte) 0xFF};
                final String[] errorMsg = {null};
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    // Missing table name after FROM -> syntax error at a specific position.
                    client.execute("SELECT * FROM", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.fail("unexpected batch on malformed SQL");
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.fail("unexpected end on malformed SQL");
                        }

                        @Override
                        public void onError(byte status, String message) {
                            errorStatus[0] = status;
                            errorMsg[0] = message;
                        }
                    });
                }
                Assert.assertNotNull("expected error message", errorMsg[0]);
                Assert.assertEquals("PARSE_ERROR status expected", 0x05, errorStatus[0]);
                // SqlException.getMessage() format: "[<position>] <text>"
                Assert.assertTrue(
                        "message should contain position marker, got: " + errorMsg[0],
                        errorMsg[0].startsWith("[") && errorMsg[0].contains("]")
                );
            }
        });
    }

    /**
     * Regression: the server's per-HttpServer {@code selectCache} may hand back
     * a factory whose {@code TableReader} is stale if the table was dropped and
     * recreated between compile time and cursor open. That surfaces as
     * {@link io.questdb.cairo.sql.TableReferenceOutOfDateException} on
     * {@code getCursor} / {@code getPageFrameCursor}. The server must catch it,
     * discard the stale cached factory, recompile, and retry the query; the
     * client should see the result as if the cache miss were the normal path.
     */
    @Test
    public void testStaleCachedFactoryRetriesCompile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(id LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO t VALUES (1, 0::TIMESTAMP)");
                serverMain.awaitTable("t");

                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    // First query warms the per-server select cache.
                    long[] firstRows = {0};
                    client.execute("SELECT id FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            firstRows[0] += batch.getRowCount();
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("first query failed: " + message);
                        }
                    });
                    Assert.assertEquals(1L, firstRows[0]);
                }

                // Drop and recreate the table with an identical schema. The cached
                // factory's TableReader now points at a dead tableId.
                serverMain.execute("DROP TABLE t");
                serverMain.execute("CREATE TABLE t(id LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO t VALUES (42, 0::TIMESTAMP)");
                serverMain.awaitTable("t");

                // Second query (new connection, but the server-wide select cache
                // still carries the stale factory). Server must detect + retry.
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    long[] secondRows = {0};
                    long[] secondSum = {0};
                    client.execute("SELECT id FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0, n = batch.getRowCount(); r < n; r++) {
                                secondSum[0] += batch.getLongValue(0, r);
                                secondRows[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("stale-factory retry path failed to recover: " + message);
                        }
                    });
                    Assert.assertEquals("retry must still produce the single row from the new table",
                            1L, secondRows[0]);
                    Assert.assertEquals("retry must read from the NEW table (id=42 from the re-inserted row)",
                            42L, secondSum[0]);
                }
            }
        });
    }

    /**
     * C1: spec divergence on symbol dictionaries. {@code docs/QWP_EGRESS_EXTENSION.md}
     * sec 12 says egress uses connection-scoped delta dictionaries; the Phase-1 implementation
     * sends an inline per-batch dict every time. This test pins the actual wire behavior
     * so spec or implementation drift gets caught loudly.
     */
    @Test
    public void testSymbolDictResentEachQuery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE syms(s SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO syms VALUES ('A', 1::TIMESTAMP), ('B', 2::TIMESTAMP), "
                        + "('C', 3::TIMESTAMP), ('A', 4::TIMESTAMP), ('B', 5::TIMESTAMP)");
                serverMain.awaitTable("syms");

                final List<String> firstRun = new ArrayList<>();
                final List<String> secondRun = new ArrayList<>();
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT s FROM syms", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) firstRun.add(batch.getString(0, r));
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: " + message);
                        }
                    });
                    // Second query on the SAME connection: under the published spec the dict
                    // would be a connection-scoped delta (no entries to retransmit). Under the
                    // current implementation the inline dict is sent again. Either way, the
                    // string values must match -- and they do. The behavioral pin here is that
                    // both runs succeed independently with the same string contents.
                    client.execute("SELECT s FROM syms", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) secondRun.add(batch.getString(0, r));
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: " + message);
                        }
                    });
                }
                Assert.assertEquals(List.of("A", "B", "C", "A", "B"), firstRun);
                Assert.assertEquals(List.of("A", "B", "C", "A", "B"), secondRun);
            }
        });
    }

    @Test
    public void testTableNotFound() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain ignored = startWithEnvVariables()) {
                final String[] errorMsg = {null};
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("SELECT * FROM no_such_table", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.fail("unexpected batch");
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.fail("unexpected end");
                        }

                        @Override
                        public void onError(byte status, String message) {
                            errorMsg[0] = message;
                        }
                    });
                }
                Assert.assertNotNull(errorMsg[0]);
                // Expect the message to reference the unknown table
                Assert.assertTrue(
                        "message should mention the unknown table, got: " + errorMsg[0],
                        errorMsg[0].toLowerCase().contains("no_such_table")
                                || errorMsg[0].toLowerCase().contains("table does not exist")
                                || errorMsg[0].toLowerCase().contains("not found")
                );
            }
        });
    }

    /**
     * C5: TIMESTAMP_NANOS round-trip -- verifies the dedicated wire type code rather than the
     * default TIMESTAMP (microseconds). Stored separately in the schema; same 8-byte int64
     * payload so the only thing being verified is correct wire-type plumbing.
     */
    @Test
    public void testTimestampNanos() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE ts_n(ts TIMESTAMP_NS, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO ts_n VALUES
                            ('2024-01-01T00:00:00.000000001Z', 1),
                            ('2024-01-01T00:00:00.000000002Z', 2)
                        """);
                serverMain.awaitTxn("ts_n", 1);

                final int[] count = {0};
                final long[] firstTs = {0};
                final long[] secondTs = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT ts, v FROM ts_n ORDER BY ts", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.assertEquals(io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_TIMESTAMP_NANOS,
                                    batch.getColumnWireType(0));
                            for (int r = 0; r < batch.getRowCount(); r++, count[0]++) {
                                if (count[0] == 0) firstTs[0] = batch.getLongValue(0, r);
                                if (count[0] == 1) secondTs[0] = batch.getLongValue(0, r);
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }
                Assert.assertEquals(2, count[0]);
                Assert.assertEquals(1L, secondTs[0] - firstTs[0]);
            }
        });
    }

    @Test
    public void testUuidAndLong256() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE wide(u UUID, l256 LONG256, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute(
                        "INSERT INTO wide VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', CAST('0x01' AS LONG256), 1::TIMESTAMP)"
                );
                serverMain.execute(
                        "INSERT INTO wide VALUES (CAST(NULL AS UUID), CAST(NULL AS LONG256), 2::TIMESTAMP)"
                );
                serverMain.awaitTable("wide");

                final Object[][] rows = new Object[2][];
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    // Explicit projection keeps the on-wire schema at 2 columns so the
                    // assertions below keep indexing into [0]=u and [1]=l256.
                    client.execute("SELECT u, l256 FROM wide", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.assertEquals(2, batch.getColumnCount());
                            Assert.assertEquals(2, batch.getRowCount());
                            for (int r = 0; r < 2; r++) {
                                rows[r] = new Object[]{
                                        batch.isNull(0, r) ? null : new long[]{
                                                batch.getUuidLo(0, r),
                                                batch.getUuidHi(0, r)
                                        },
                                        batch.isNull(1, r) ? null : new long[]{
                                                batch.getLong256Word(1, r, 0),
                                                batch.getLong256Word(1, r, 1),
                                                batch.getLong256Word(1, r, 2),
                                                batch.getLong256Word(1, r, 3)
                                        }
                                };
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: " + message);
                        }
                    });
                }
                // UUID round-trip: 2 longs (lo, hi)
                Assert.assertNotNull(rows[0][0]);
                Assert.assertEquals(2, ((long[]) rows[0][0]).length);
                // LONG256 round-trip: 4 longs, least significant first. 0x01 -> {1, 0, 0, 0}
                long[] l256 = (long[]) rows[0][1];
                Assert.assertEquals(1L, l256[0]);
                Assert.assertEquals(0L, l256[1]);
                Assert.assertEquals(0L, l256[2]);
                Assert.assertEquals(0L, l256[3]);
                // NULL rows
                Assert.assertNull(rows[1][0]);
                Assert.assertNull(rows[1][1]);
            }
        });
    }

    private static void assertSelectReturnsOneRow(QwpQueryClient client, String sql, String label) {
        final long[] sum = {0};
        final long[] rows = {0};
        client.execute(sql, new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                int n = batch.getRowCount();
                for (int r = 0; r < n; r++) {
                    sum[0] += batch.getLongValue(0, r);
                }
                rows[0] += n;
            }

            @Override
            public void onEnd(long totalRows) {
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail(label + ": SELECT failed (status=" + status + "): " + message);
            }
        });
        Assert.assertEquals(label + ": row count", 1L, rows[0]);
        Assert.assertEquals(label + ": sum of v", 1L, sum[0]);
    }

    private void runBatchBoundary(int totalRows) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE boundary_t(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute(
                        "INSERT INTO boundary_t SELECT x, x::TIMESTAMP FROM long_sequence(" + totalRows + ")"
                );
                serverMain.awaitTable("boundary_t");

                final int[] count = {0};
                final int[] batches = {0};
                final long[] sum = {0L};
                final long[] serverTotalRows = {-1L};
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    client.execute("SELECT x FROM boundary_t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            batches[0]++;
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                sum[0] += batch.getLongValue(0, r);
                                count[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long trs) {
                            serverTotalRows[0] = trs;
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: " + message);
                        }
                    });
                }
                Assert.assertEquals(totalRows, count[0]);
                Assert.assertEquals((long) totalRows * (totalRows + 1) / 2L, sum[0]);
                Assert.assertEquals(totalRows, serverTotalRows[0]);
                if (totalRows <= QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH) {
                    Assert.assertEquals("expected exactly one batch", 1, batches[0]);
                } else {
                    Assert.assertTrue("expected > 1 batch, got " + batches[0], batches[0] > 1);
                }
            }
        });
    }
}
