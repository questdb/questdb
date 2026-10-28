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
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.TestServerMain;
import io.questdb.test.cutlass.qwp.AbstractQwpBootstrapTest;
import io.questdb.test.tools.LogCapture;
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
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
 * <p>
 * The over-cap case only covers the force commit while all {@code ROW_COUNT}
 * rows reach {@code QwpWalAppender} as ONE table block, which is what leaves
 * {@code QwpTudCache} a drained writer to reconcile. The sender therefore pins
 * every auto-flush trigger instead of inheriting the client's defaults, and
 * {@link #EXPECTED_SEQ_TXN} asserts the resulting one-transaction shape against
 * {@code wal_tables()} so a split fails here rather than quietly degrading the
 * case into a second copy of the control.
 */
public class QwpAckSeqTxnCoverageBlackBoxTest extends AbstractQwpBootstrapTest {

    private static final int AUTO_FLUSH_INTERVAL_MILLIS = 3_600_000;
    private static final int CLOSE_FLUSH_TIMEOUT_MILLIS = 5_000;
    /**
     * The batch commits as a single WAL transaction, so both the table's
     * sequencer txn and the seqTxn the acks must report are 1.
     */
    private static final long EXPECTED_SEQ_TXN = 1;
    private static final int PROBE_LEN = 6;
    private static final int RELAY_PROBE_TIMEOUT_MS = 30_000;
    private static final int DEFERRED_FRAME_COUNT = 20;
    private static final int ROW_COUNT = 200;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
    }

    /**
     * {@link AckTee} has to service more than one connection: the QWP sender
     * keeps a foreground reconnect factory wired even in memory mode, so a wire
     * failure makes it dial the same relay port again. A relay that accepts once
     * lets the reconnect finish its TCP handshake against the listen backlog and
     * then never services it, and the run parks until the 20-minute suite
     * timeout fires instead of failing with a cause.
     */
    @Test
    public void testAckTeeRelaysSequentialConnections() throws Exception {
        assertMemoryLeak(() -> {
            final int connectionCount = 2;
            final CountDownLatch upstreamServed = new CountDownLatch(connectionCount);
            try (ServerSocket upstream = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
                final Thread upstreamThread = new Thread(() -> {
                    for (int i = 0; i < connectionCount; i++) {
                        try (Socket peer = upstream.accept()) {
                            final byte[] probe = new byte[PROBE_LEN];
                            readFully(peer.getInputStream(), probe);
                            final byte[] reply = new String(probe, StandardCharsets.US_ASCII)
                                    .replace("ping", "pong").getBytes(StandardCharsets.US_ASCII);
                            peer.getOutputStream().write(reply);
                            peer.getOutputStream().flush();
                            upstreamServed.countDown();
                        } catch (IOException e) {
                            return;
                        }
                    }
                }, "qwp-relay-probe-upstream");
                upstreamThread.setDaemon(true);
                upstreamThread.start();

                try (AckTee tee = new AckTee(upstream.getLocalPort())) {
                    for (int i = 0; i < connectionCount; i++) {
                        try (Socket client = new Socket(InetAddress.getLoopbackAddress(), tee.localPort())) {
                            // The read timeout is not a coordination guess. It only turns the
                            // single-shot-accept hang into a reportable failure: a relay that
                            // never accepts connection 1 can never service it, so no legitimate
                            // timing window exists for this bound to race with. The green path
                            // is event-driven - the blocking read returns the instant the relay
                            // forwards the reply, and upstreamServed latches the second service.
                            client.setSoTimeout(RELAY_PROBE_TIMEOUT_MS);
                            client.getOutputStream().write(("ping-" + i).getBytes(StandardCharsets.US_ASCII));
                            client.getOutputStream().flush();
                            final byte[] echoed = new byte[PROBE_LEN];
                            readFully(client.getInputStream(), echoed);
                            Assert.assertEquals(
                                    "the relay did not forward connection " + i,
                                    "pong-" + i,
                                    new String(echoed, StandardCharsets.US_ASCII)
                            );
                        }
                    }
                }

                Assert.assertTrue(
                        "the relay never carried both connections through to the upstream",
                        upstreamServed.await(RELAY_PROBE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                );
                upstreamThread.join();

                // close() joins the acceptor and every pump, so this holds without
                // any waiting. A relay thread outliving close() is what leaves a
                // half-torn-down connection behind for the next test in the JVM.
                for (Thread t : Thread.getAllStackTraces().keySet()) {
                    Assert.assertFalse(
                            "a relay thread outlived AckTee.close(): " + t.getName(),
                            t.getName().startsWith("qwp-ack-tee-")
                    );
                }
            }
        });
    }

    @Test
    public void testBoundaryAndStandaloneFrameReportSeqTxnsToRealSender() throws Exception {
        assertMemoryLeak(() -> {
            try (TestServerMain server = startWithEnvVariables(
                    PropertyKey.QWP_MAX_UNCOMMITTED_ROWS.getEnvVarName(), "1000000")) {
                server.execute("CREATE TABLE boundary_a (val LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.execute("CREATE TABLE boundary_b (val LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                final List<byte[]> frames;
                try (AckTee tee = new AckTee(HTTP_PORT, 0, 2)) {
                    try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                            .address("localhost:" + tee.localPort())
                            .autoFlushRows(0).autoFlushBytes(0)
                            .autoFlushIntervalMillis(AUTO_FLUSH_INTERVAL_MILLIS).build()) {
                        sender.table("boundary_a").longColumn("val", 1).at(1_000_000L, ChronoUnit.MICROS);
                        sender.flush();
                        sender.table("boundary_a").longColumn("val", 2).at(2_000_000L, ChronoUnit.MICROS);
                        sender.table("boundary_b").longColumn("val", 3).at(3_000_000L, ChronoUnit.MICROS);
                        long last = sender.flushAndGetSequence();
                        Assert.assertTrue("the real sender must consume the boundary/current ACKs",
                                sender.awaitAckedFsn(last, RELAY_PROBE_TIMEOUT_MS));
                    }
                    tee.close();
                    Assert.assertTrue("relay must release both actual client frames together", tee.hasCoalescedFrames);
                    frames = tee.serverBinaryFrames();
                }
                TestUtils.assertEventually(() -> server.assertSql("SELECT val FROM boundary_a", "val\n1\n2\n"));
                TestUtils.assertEventually(() -> server.assertSql("SELECT val FROM boundary_b", "val\n3\n"));
                server.assertSql("SELECT sequencerTxn FROM wal_tables() WHERE name = 'boundary_a'", "sequencerTxn\n2\n");
                server.assertSql("SELECT sequencerTxn FROM wal_tables() WHERE name = 'boundary_b'", "sequencerTxn\n1\n");
                long aTxn = -1;
                long bTxn = -1;
                long lastAck = -1;
                WebSocketResponse response = new WebSocketResponse();
                for (byte[] payload : frames) {
                    Assert.assertTrue(readResponse(response, payload));
                    if (response.isSuccess()) {
                        lastAck = Math.max(lastAck, response.getSequence());
                        for (int i = 0; i < response.getTableEntryCount(); i++) {
                            if ("boundary_a".equals(response.getTableName(i))) {
                                aTxn = Math.max(aTxn, response.getTableSeqTxn(i));
                            } else if ("boundary_b".equals(response.getTableName(i))) {
                                bTxn = Math.max(bTxn, response.getTableSeqTxn(i));
                            }
                        }
                    }
                }
                Assert.assertEquals(1, lastAck);
                Assert.assertEquals(2, aTxn);
                Assert.assertEquals(1, bTxn);
            }
        });
    }

    @Test
    public void testOkAckReportsSeqTxnBelowCap() throws Exception {
        assertMemoryLeak(() -> assertAckReportsSeqTxn("1000000", "ack_cov_below"));
    }

    @Test
    public void testOkAckReportsSeqTxnWhenAppendTimeCommitFires() throws Exception {
        // QwpWalAppender force-commits the table block once it crosses
        // qwp.max.uncommitted.rows, draining the writer before QwpTudCache runs.
        assertMemoryLeak(() -> assertAckReportsSeqTxn("10", "ack_cov_over"));
    }

    /**
     * The ack decision this pins lives in the private
     * {@code QwpIngressUpgradeProcessor.handleBinaryMessage}. Driving the real
     * client through a real server is the only seam that reaches it: the
     * state-level tests can exercise the predicate but not the decision.
     * <p>
     * In transactional mode the client sends every auto-flush with
     * FLAG_DEFER_COMMIT and commits only on an explicit {@code flush()}. With the
     * connection cap low enough that the appender's force-commit fires, those
     * deferred frames become durable before the commit frame arrives, so the
     * server may ack them. Before this behaviour existed no ack could appear
     * until the commit frame, and a mid-group reconnect replayed rows the server
     * had already written.
     */
    @Test
    public void testDeferredFrameIsAckedOnceItsRowsAreForceCommitted() throws Exception {
        assertMemoryLeak(() -> assertDeferredFramesAcked("1", "ack_defer_forced", true));
    }

    /**
     * Complement of the above: with no force-commit, a deferred frame's rows are
     * still rollback-able, so nothing may be acked before the commit frame.
     * Without this arm the test above passes against a server that acks every
     * deferred frame unconditionally -- the data-loss direction.
     */
    @Test
    public void testDeferredFrameIsNotAckedWhileItsRowsAreUncommitted() throws Exception {
        assertMemoryLeak(() -> assertDeferredFramesAcked("1000000", "ack_defer_buffered", false));
    }

    /**
     * A frame that joins a commit batch and fails on its own value is NACKed only
     * after the batch's earlier frames are committed and acked: the appender
     * cancels the bad block, the batch is intact, and the server commits it before
     * answering the bad frame with its own status.
     */
    @Test
    public void testBadValueInCommitBatchIsNackedAfterEarlierFramesAreAcked() throws Exception {
        assertMemoryLeak(() -> {
            try (TestServerMain server = startWithEnvVariables()) {
                server.execute("CREATE TABLE u (id UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                final List<SenderError> errors = new CopyOnWriteArrayList<>();
                // The tee hands the server the connection's first three frames in one write.
                // They share one schema, so the second and third join the first's commit
                // batch, and the third fails on its own value while joining.
                try (AckTee tee = new AckTee(HTTP_PORT, 0, 3)) {
                    final Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                            .address("localhost:" + tee.localPort())
                            .autoFlushRows(0)
                            .autoFlushBytes(0)
                            .autoFlushIntervalMillis(AUTO_FLUSH_INTERVAL_MILLIS)
                            .closeFlushTimeoutMillis(CLOSE_FLUSH_TIMEOUT_MILLIS)
                            .errorHandler(errors::add)
                            .build();
                    try {
                        sender.table("u").stringColumn("id", "11111111-1111-1111-1111-111111111111").at(1_000_000L, ChronoUnit.MICROS);
                        sender.flush();
                        sender.table("u").stringColumn("id", "22222222-2222-2222-2222-222222222222").at(2_000_000L, ChronoUnit.MICROS);
                        final long secondFsn = sender.flushAndGetSequence();
                        sender.table("u").stringColumn("id", "not-a-uuid").at(3_000_000L, ChronoUnit.MICROS);
                        sender.flush();
                        Assert.assertTrue(
                                "the frames before the bad one must be acked; errors: " + errors,
                                sender.awaitAckedFsn(secondFsn, RELAY_PROBE_TIMEOUT_MS)
                        );
                        TestUtils.assertEventually(() -> {
                            final SenderError terminal = ((QwpWebSocketSender) sender).getLastTerminalError();
                            Assert.assertNotNull("the bad frame must latch a terminal error; errors: " + errors, terminal);
                            Assert.assertEquals(SenderError.Category.SCHEMA_MISMATCH, terminal.getCategory());
                        });
                    } finally {
                        try {
                            sender.close();
                        } catch (Throwable ignore) {
                            // close rethrows the latched terminal error
                        }
                    }
                    tee.close();
                    Assert.assertTrue("the three frames never reached the server in one write", tee.hasCoalescedFrames);
                }
                TestUtils.assertEventually(() -> server.assertSql(
                        "SELECT count() FROM wal_tables() WHERE writerTxn < sequencerTxn", "count\n0\n"));
                server.assertSql("SELECT id FROM u",
                        "id\n11111111-1111-1111-1111-111111111111\n22222222-2222-2222-2222-222222222222\n");
                // One commit for both: the second frame joined the first's batch.
                server.assertSql("SELECT sequencerTxn FROM wal_tables() WHERE name = 'u'", "sequencerTxn\n1\n");
            }
        });
    }

    /**
     * With one table allowed per connection, the server refuses every frame for
     * a second table, and the sender reconnects and replays from its ack
     * watermark. A reconnect opens with the client's rowless symbol-dictionary
     * catch-up, so when the tee releases it together with the replayed frames,
     * the first replayed frame closes a rowless deferred group in the same
     * receive pass as the refused frame behind it. The server must commit AND
     * ack that frame before the refusal, or every further replay writes its
     * rows again until the client's poison detector halts the sender.
     */
    @Test
    public void testReplayAfterReconnectDoesNotDuplicateBoundaryCommittedRows() throws Exception {
        assertMemoryLeak(() -> {
            try (TestServerMain server = startWithEnvVariables(
                    PropertyKey.QWP_MAX_TABLES_PER_CONNECTION.getEnvVarName(), "1")) {
                server.execute("CREATE TABLE a (s SYMBOL, val LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                server.execute("CREATE TABLE b (s SYMBOL, val LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                final List<SenderError> errors = new CopyOnWriteArrayList<>();
                // Connection 0 commits a's first row and is refused b's. Connection 1 is the
                // first reconnect: its first three frames are the dictionary catch-up and the
                // replayed b and a frames, which the tee hands the server in one write.
                try (AckTee tee = new AckTee(HTTP_PORT, 1, 3)) {
                    try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                            .address("localhost:" + tee.localPort())
                            .autoFlushRows(0)
                            .autoFlushBytes(0)
                            .autoFlushIntervalMillis(AUTO_FLUSH_INTERVAL_MILLIS)
                            .reconnectInitialBackoffMillis(10)
                            .reconnectMaxBackoffMillis(50)
                            .poisonMinEscalationWindowMillis(0)
                            .closeFlushTimeoutMillis(CLOSE_FLUSH_TIMEOUT_MILLIS)
                            .errorHandler(errors::add)
                            .build()) {
                        sender.table("a").symbol("s", "x").longColumn("val", 1).at(1_000_000L, ChronoUnit.MICROS);
                        sender.flush();
                        sender.table("b").symbol("s", "y").longColumn("val", 2).at(2_000_000L, ChronoUnit.MICROS);
                        sender.flush();
                        sender.table("a").symbol("s", "z").longColumn("val", 3).at(3_000_000L, ChronoUnit.MICROS);
                        final long last = sender.flushAndGetSequence();
                        Assert.assertTrue(
                                "the server never acked every frame; errors: " + errors,
                                sender.awaitAckedFsn(last, RELAY_PROBE_TIMEOUT_MS)
                        );
                        Assert.assertNull(
                                "the sender latched a terminal error; errors: " + errors,
                                ((QwpWebSocketSender) sender).getLastTerminalError()
                        );
                    }
                    tee.close();
                    Assert.assertTrue(
                            "the reconnect's catch-up and replayed frames never reached the server in one write",
                            tee.hasCoalescedFrames
                    );
                }
                // Every frame is acked, so its commit already has a sequencer txn.
                TestUtils.assertEventually(() -> server.assertSql(
                        "SELECT count() FROM wal_tables() WHERE writerTxn < sequencerTxn", "count\n0\n"));
                server.assertSql("SELECT s, val FROM a", "s\tval\nx\t1\nz\t3\n");
                server.assertSql("SELECT s, val FROM b", "s\tval\ny\t2\n");
            }
        });
    }

    private void assertDeferredFramesAcked(
            String maxUncommittedRows,
            String table,
            boolean expectAckBeforeCommit
    ) throws Exception {
        // The last-resort clamp in setHighestProcessedSequence absorbs a regressed
        // ack path silently -- it refuses the advance and logs critical, so the
        // wire looks correct and only the scream distinguishes the two. Capture it,
        // or bypassing the withhold branch entirely is untestable from out here.
        final LogCapture capture = new LogCapture();
        capture.start();
        try (TestServerMain serverMain = startWithEnvVariables(
                PropertyKey.QWP_MAX_UNCOMMITTED_ROWS.getEnvVarName(), maxUncommittedRows
        )) {
            serverMain.execute("CREATE TABLE " + table + " (val LONG, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");

            final List<byte[]> serverFrames;
            try (AckTee tee = new AckTee(HTTP_PORT)) {
                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + tee.localPort())
                        // Transactional mode is what makes auto-flush emit
                        // FLAG_DEFER_COMMIT; without it no deferred frame exists
                        // and the test would be vacuous in both arms.
                        .transactional(true)
                        // One row per frame, so every auto-flush is its own
                        // deferred frame and the sequence numbering is legible.
                        .autoFlushRows(1)
                        .autoFlushBytes(0)
                        .autoFlushIntervalMillis(AUTO_FLUSH_INTERVAL_MILLIS)
                        .build()) {
                    for (int i = 0; i < DEFERRED_FRAME_COUNT; i++) {
                        sender.table(table)
                                .longColumn("val", i)
                                .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    }
                    // Deliberately NOT flushed here: close() sends the single
                    // commit frame that closes the group, so any ack at a LOWER
                    // sequence than the last one covered a deferred frame while
                    // the group was still open. That is the property under test.
                }
                tee.close();
                serverFrames = tee.serverBinaryFrames();
            }

            TestUtils.assertEventually(() -> serverMain.assertSql(
                    "SELECT count() FROM " + table, "count\n" + DEFERRED_FRAME_COUNT + "\n"));

            int okAckCount = 0;
            long minAckSeq = Long.MAX_VALUE;
            long maxAckSeq = Long.MIN_VALUE;
            final StringBuilder observed = new StringBuilder();
            final WebSocketResponse response = new WebSocketResponse();
            for (int f = 0, n = serverFrames.size(); f < n; f++) {
                if (readResponse(response, serverFrames.get(f)) && response.isSuccess()) {
                    okAckCount++;
                    minAckSeq = Math.min(minAckSeq, response.getSequence());
                    maxAckSeq = Math.max(maxAckSeq, response.getSequence());
                    observed.append("[seq=").append(response.getSequence()).append(']');
                }
            }

            // Both arms require the group-closing ack, so its presence never
            // discriminates. What discriminates is whether anything was acked
            // BEFORE it -- comparing sequences rather than counting frames keeps
            // this independent of the ack batch threshold and of how many
            // sequences the handshake consumes.
            Assert.assertTrue("the server sent no OK ack at all; acks seen: " + observed, okAckCount > 0);
            // The client sends DEFERRED_FRAME_COUNT deferred frames at sequences
            // 0..n-1, then close() sends the group-closing commit frame at n.
            final long commitSeq = DEFERRED_FRAME_COUNT;
            Assert.assertEquals(
                    "the group-closing frame must always be acked, whichever arm this is; "
                            + "acks seen: " + observed,
                    commitSeq,
                    maxAckSeq
            );
            if (expectAckBeforeCommit) {
                Assert.assertTrue(
                        "the force-commit made each deferred frame's rows durable, so an ack must "
                                + "cover one before the group-closing frame; acks seen: " + observed,
                        minAckSeq < commitSeq
                );
            } else {
                // Pin WHICH sequence, not merely that the distinct count is one.
                // "min == max" alone is satisfied by a server that acked only a
                // deferred frame and never the commit frame -- the data-loss
                // direction -- because a single sample is trivially its own min.
                Assert.assertEquals(
                        "no force-commit fired, so every deferred frame's rows are still "
                                + "rollback-able and only the group-closing frame may be acked; "
                                + "acks seen: " + observed,
                        commitSeq,
                        minAckSeq
                );
            }

            // Nothing may reach the containment. It exists to catch a regressed
            // ack path, so a run that trips it is a run whose correct-looking
            // wire output is being produced by the backstop, not by the decision
            // under test.
            capture.drain();
            capture.assertNotLogged("tried to advance over uncommitted deferred rows");
        } finally {
            capture.stop();
        }
    }

    private static void readFully(InputStream in, byte[] dst) throws IOException {
        int read = 0;
        while (read < dst.length) {
            final int n = in.read(dst, read, dst.length - read);
            if (n < 0) {
                throw new IOException("peer closed after " + read + " of " + dst.length + " bytes");
            }
            read += n;
        }
    }

    // Parses one server frame payload; readFrom copies what it keeps, so the native
    // copy is freed before the caller reads the response.
    private static boolean readResponse(WebSocketResponse response, byte[] payload) {
        final long ptr = Unsafe.malloc(payload.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < payload.length; i++) {
                Unsafe.putByte(ptr + i, payload[i]);
            }
            return response.readFrom(ptr, payload.length);
        } finally {
            Unsafe.free(ptr, payload.length, MemoryTag.NATIVE_DEFAULT);
        }
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
                        // Batch explicitly rather than inheriting the client's auto-flush
                        // defaults. Whether the over-cap case covers the append-time force
                        // commit at all hinges on the whole batch arriving as one table
                        // block: a split that leaves the tail under the cap commits that
                        // tail through commitAll exactly like the control does, and the
                        // case silently stops discriminating. Pinning all three triggers
                        // takes DEFAULT_WS_AUTO_FLUSH_ROWS / _BYTES / _INTERVAL_NANOS out
                        // of that argument. The WebSocket transport rejects
                        // disableAutoFlush() and rejects an infinite interval, so the
                        // interval gets an hour - orders of magnitude past the loop below,
                        // and it only ever fires from inside at().
                        .autoFlushRows(0)
                        .autoFlushBytes(0)
                        .autoFlushIntervalMillis(AUTO_FLUSH_INTERVAL_MILLIS)
                        .build()) {
                    for (int i = 0; i < ROW_COUNT; i++) {
                        sender.table(table)
                                .longColumn("val", i)
                                .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
                // Snapshot after close(), which returns only once every relayed socket is
                // closed and every pump thread has terminated. No pump can still be
                // writing into the buffer, so the snapshot needs no argument about when
                // pump() tees relative to when it forwards.
                tee.close();
                serverFrames = tee.serverBinaryFrames();
            }

            // The rows land either way - an unreported seqTxn is a tracking gap,
            // not a lost write.
            TestUtils.assertEventually(() ->
                    serverMain.assertSql("SELECT count() FROM " + table, "count\n" + ROW_COUNT + "\n"));

            // Pin the precondition the over-cap case's coverage rests on. One sequencer
            // txn means the appender saw the whole batch as a single table block: over the
            // cap that commit can only be the force commit inside the append, under it only
            // commitAll's. Any split - a future client flush trigger, a server-side batch
            // limit - commits at least twice and fails here, loudly, rather than leaving a
            // tail that commits through commitAll and satisfies the ack assertion below on
            // the unfixed code too.
            serverMain.assertSql(
                    "SELECT sequencerTxn FROM wal_tables() WHERE name = '" + table + "'",
                    "sequencerTxn\n" + EXPECTED_SEQ_TXN + "\n"
            );

            int okAckCount = 0;
            long reportedSeqTxn = Long.MIN_VALUE;
            final StringBuilder observed = new StringBuilder();
            final WebSocketResponse response = new WebSocketResponse();
            for (int f = 0, n = serverFrames.size(); f < n; f++) {
                if (!readResponse(response, serverFrames.get(f)) || !response.isSuccess()) {
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
            }

            Assert.assertTrue("the server sent no OK ack", okAckCount > 0);
            // Cross-check against the table's own sequencer txn rather than against zero:
            // an ack that reports a stale or lower seqTxn covers less WAL than it claims,
            // and "> 0" waves that through.
            Assert.assertEquals(
                    "no OK ack reported the final seqTxn for " + table + ", so a durable-ack client "
                            + "reads the batch as trivially durable and trims its store-and-forward "
                            + "slot before the upload tracker has seen the WAL segment; acks seen: " + observed,
                    EXPECTED_SEQ_TXN,
                    reportedSeqTxn
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
        private final Thread acceptor;
        // Zero-based accept order of the one connection whose first
        // coalescedFrameCount client frames reach the server in one write.
        private final int coalescedConnectionIndex;
        private final int coalescedFrameCount;
        // Set once the coalesced connection's held frames went out together.
        private volatile boolean hasCoalescedFrames;
        private final ServerSocket listener;
        private final List<Thread> pumps = new ArrayList<>();
        private final ByteArrayOutputStream serverToClient = new ByteArrayOutputStream();
        private final List<Socket> sockets = new ArrayList<>();
        // Guarded by sockets. close() raises it before it closes the listener, so
        // the acceptor tells a shutdown apart from a transient accept failure and
        // never registers a connection that close() has already walked past.
        private boolean isClosed;

        private AckTee(int upstreamPort) throws IOException {
            this(upstreamPort, -1, 0);
        }

        private AckTee(int upstreamPort, int coalescedConnectionIndex, int coalescedFrameCount) throws IOException {
            this.coalescedConnectionIndex = coalescedConnectionIndex;
            this.coalescedFrameCount = coalescedFrameCount;
            listener = new ServerSocket(0, 4, InetAddress.getLoopbackAddress());
            acceptor = new Thread(() -> acceptLoop(upstreamPort), "qwp-ack-tee-acceptor");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        /**
         * Returns only once the relay is quiescent: the listener and every
         * relayed socket are closed, and the acceptor and both pump threads of
         * every connection have terminated. Idempotent, so a caller that closes
         * early to freeze the tee buffer can still leave the try-with-resources
         * close in place.
         */
        @Override
        public void close() {
            synchronized (sockets) {
                isClosed = true;
                closeQuietly(listener);
                for (int i = 0, n = sockets.size(); i < n; i++) {
                    closeQuietly(sockets.get(i));
                }
            }
            // Join outside the monitor. The acceptor needs the monitor to observe
            // the shutdown flag, so joining while holding it would deadlock.
            joinQuietly(acceptor);
            // The acceptor is the only thread that registers pumps, so the list is
            // final once it has terminated. Every pump's socket is already closed,
            // which is what unblocks the pump out of read().
            final List<Thread> live;
            synchronized (sockets) {
                live = new ArrayList<>(pumps);
            }
            for (int i = 0, n = live.size(); i < n; i++) {
                joinQuietly(live.get(i));
            }
        }

        private static void closeQuietly(Closeable closeable) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (IOException ignore) {
                    // already gone
                }
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

        private static void joinQuietly(Thread thread) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        /**
         * Services connections until close(). The QWP sender reconnects through
         * the same port after a wire failure, so a single accept would leave the
         * reconnect's completed TCP handshake sitting in the backlog forever.
         */
        private void acceptLoop(int upstreamPort) {
            for (int connectionIndex = 0; ; connectionIndex++) {
                Socket downstream = null;
                Socket upstream = null;
                try {
                    downstream = listener.accept();
                    upstream = new Socket(InetAddress.getLoopbackAddress(), upstreamPort);
                } catch (IOException e) {
                    // The downstream is already accepted when the upstream dial
                    // fails, so drop it here - otherwise it leaks and the client
                    // waits on a connection nothing will ever service.
                    closeQuietly(downstream);
                    closeQuietly(upstream);
                    synchronized (sockets) {
                        if (isClosed) {
                            return;
                        }
                    }
                    continue;
                }
                synchronized (sockets) {
                    if (isClosed) {
                        closeQuietly(downstream);
                        closeQuietly(upstream);
                        return;
                    }
                    sockets.add(downstream);
                    sockets.add(upstream);
                }
                try {
                    final int heldFrameCount = connectionIndex == coalescedConnectionIndex ? coalescedFrameCount : 0;
                    pump(downstream.getInputStream(), upstream.getOutputStream(), null, heldFrameCount);
                    pump(upstream.getInputStream(), downstream.getOutputStream(), serverToClient, 0);
                } catch (IOException e) {
                    closeQuietly(downstream);
                    closeQuietly(upstream);
                }
            }
        }

        // Forward the client's HTTP handshake, then hold its first frameCount
        // frames and forward them in one transport write.
        private void coalesceFirstFrames(InputStream in, OutputStream out, int frameCount) throws IOException {
            int suffix = 0;
            while (suffix != 0x0d0a0d0a) {
                int next = in.read();
                if (next < 0) {
                    throw new IOException("client closed during handshake");
                }
                out.write(next);
                suffix = (suffix << 8) | next;
            }
            out.flush();
            ByteArrayOutputStream frames = new ByteArrayOutputStream();
            for (int frame = 0; frame < frameCount; frame++) {
                byte[] header = new byte[2];
                readFully(in, header);
                if ((header[0] & 0x0f) != 2) {
                    throw new IOException("expected a client binary frame");
                }
                frames.write(header);
                int length = header[1] & 0x7f;
                if (length == 126) {
                    byte[] extended = new byte[2];
                    readFully(in, extended);
                    frames.write(extended);
                    length = ((extended[0] & 0xff) << 8) | (extended[1] & 0xff);
                } else if (length == 127) {
                    throw new IOException("unexpected large client frame");
                }
                byte[] payload = new byte[length + ((header[1] & 0x80) == 0 ? 0 : 4)];
                readFully(in, payload);
                frames.write(payload);
            }
            out.write(frames.toByteArray());
            out.flush();
            hasCoalescedFrames = true;
        }

        private int localPort() {
            return listener.getLocalPort();
        }

        private void pump(InputStream in, OutputStream out, ByteArrayOutputStream tee, int heldFrameCount) {
            final Thread t = new Thread(() -> {
                final byte[] buf = new byte[8192];
                try {
                    if (heldFrameCount > 0) {
                        coalesceFirstFrames(in, out, heldFrameCount);
                    }
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
            // Register before start() so close() always has the thread to join.
            // Only the acceptor calls this, and close() snapshots the list after
            // it has joined the acceptor, so the pair is never observed split.
            synchronized (sockets) {
                pumps.add(t);
            }
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
