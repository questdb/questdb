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

package io.questdb.test.cutlass.websocket;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.wal.DurableAckRegistry;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.cutlass.qwp.server.QwpIngressUpgradeProcessor;
import io.questdb.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import static io.questdb.test.cutlass.websocket.QwpIngressWireHarness.*;

/**
 * Commit batching on a QWP ingress connection: which frames share a WAL commit, and
 * how acks, NACKs and closes cover the batch when a frame or the batch commit fails.
 */
public class QwpIngressCommitBatchTest extends AbstractCairoTest {
    // Fuzz frame kinds, drawn uniformly from [0, FUZZ_DATA_KINDS) when an iteration's only
    // faults are the frames' own data errors, otherwise from [0, FUZZ_KINDS). Every kind
    // without a name of its own is a plain one-row frame, the common case.
    private static final int FUZZ_DATA_KINDS = 8;
    private static final int FUZZ_DEFERRED = 8;
    private static final int FUZZ_KINDS = 11;
    private static final int FUZZ_MALFORMED = 5;
    private static final int FUZZ_PING = 10;
    private static final int FUZZ_PLAIN = 0;
    private static final int FUZZ_ROWLESS_DEFERRED = 9;
    private static final int FUZZ_SCHEMA_MISMATCH = 6;
    private static final int FUZZ_TWO_BLOCKS = 4;

    @Test
    public void testBatchCommitsBeforeReachingCap() throws Exception {
        assertBatchCommitsBeforeReachingCap(false);
    }

    @Test
    public void testBatchCommittedAfterTransientGateRefusalIsAckedBeforeRoleChangeClose() throws Exception {
        assertGateRefusalWithBatchPending(true);
    }

    @Test
    public void testBatchRefusedAfterGateRefusalIsNotAckedBeforeRoleChangeClose() throws Exception {
        assertGateRefusalWithBatchPending(false);
    }

    /**
     * On a durable-ack connection with no work awaiting upload, the server waits for
     * the client to echo its role-change CLOSE before tearing the connection down: the
     * echo proves the client read the frames before that CLOSE. A CLOSE the client
     * pipelined before the server's CLOSE reached it proves nothing of the kind, so it
     * must not complete the handshake, whether it directly follows the refused batch
     * or sits behind another frame.
     */
    @Test
    public void testClientClosePipelinedBeforeRoleChangeCloseIsNotTheEcho() throws Exception {
        assertPipelinedClientCloseIsNotTheEcho(clientCloseFrame());
    }

    @Test
    public void testClientClosePipelinedBehindPingBeforeRoleChangeCloseIsNotTheEcho() throws Exception {
        assertPipelinedClientCloseIsNotTheEcho(createMaskedFrame(WebSocketOpcode.PING, new byte[0]), clientCloseFrame());
    }

    /**
     * As {@link #testClientClosePipelinedBeforeRoleChangeCloseIsNotTheEcho}, but the
     * client's pipelined CLOSE straddles two reads: the batch commits at the end of the
     * first read with the head of that CLOSE buffered, and the demote refuses the
     * commit. Completing that CLOSE in the next read must not complete the handshake.
     */
    @Test
    public void testClientCloseStraddlingReadsBeforeRoleChangeCloseIsNotTheEcho() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final byte[] close = clientCloseFrame();
            try (DemotableEngine demotableEngine = new DemotableEngine()) {
                demotableEngine.demoteAtNextCommit();
                try (Connection connection = new Connection(
                        demotableEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        true,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                        Arrays.copyOfRange(close, 0, close.length - 1),
                        Arrays.copyOfRange(close, close.length - 1, close.length)
                )) {
                    Assert.assertFalse(connection.deliverFrames(3));
                    Assert.assertTrue("test setup: the demote must land at the batch commit", demotableEngine.isReadOnlyMode());
                    assertOnlyRoleChangeCloseSent(connection.sentFrames());
                    Assert.assertFalse(
                            "a CLOSE begun before the server's CLOSE must not complete the handshake",
                            connection.deliverFrames(1)
                    );
                    assertOnlyRoleChangeCloseSent(connection.sentFrames());
                }
                assertTableEmpty(demotableEngine);
            }
        });
    }

    /**
     * The appender reuses its column resolution for a repeated schema only while
     * the writer's metadata is unchanged: after a rename, the same column name in
     * the same schema must resolve again (here: auto-create a new {@code v}), not
     * land in the renamed column's slot.
     */
    @Test
    public void testColumnResolutionFollowsRenameBetweenFrames() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (Connection connection = new Connection(
                    engine,
                    httpConfig,
                    AllowAllSecurityContext.INSTANCE,
                    false,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L))
            )) {
                connection.deliverFrames(1);
                drainWalQueue();
                execute("ALTER TABLE tab RENAME COLUMN v TO w");
                drainWalQueue();
                connection.deliverFrames(1);
            }
            drainWalQueue();
            assertQuery("SELECT w, v FROM tab").noLeakCheck().expectSize().returns("""
                    w\tv
                    100\tnull
                    null\t200
                    """);
        });
    }

    /**
     * Random frame sequences through the real handler in one receive pass, with random
     * faults: refused INSERTs, read-only flips (transient or persistent), a small
     * max-uncommitted-rows cap, durable acks, deferred groups, malformed and
     * schema-mismatched frames and PINGs. Every iteration checks the ack contract
     * against the tables' contents, with A the highest cumulative OK ack:
     * <ul>
     * <li>I1: every row of every frame at or below A is committed;</li>
     * <li>I2: no row of a frame above A is committed (the client replays from A + 1),
     * and no row is committed twice.</li>
     * </ul>
     * Iterations whose only faults are the frames' own data errors also check I3:
     * the frames before the first bad frame are committed and acked, and the bad
     * frame is NACKed with its own status, as one commit per frame would leave it.
     */
    @Test
    public void testCommitBatchAckContractFuzz() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final int iterations = 150;
            // The hook engine below shares the root but cannot create tables: create them first.
            for (int iteration = 0; iteration < iterations; iteration++) {
                execute("CREATE TABLE fa" + iteration + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("CREATE TABLE fb" + iteration + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            }
            final AtomicLong readOnlyCalls = new AtomicLong();
            final AtomicLong readOnlyFromCall = new AtomicLong(-1);
            final AtomicBoolean isReadOnlyTransient = new AtomicBoolean();
            try (CairoEngine hookEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                @Override
                public boolean isReadOnlyMode() {
                    final long call = readOnlyCalls.incrementAndGet();
                    final long from = readOnlyFromCall.get();
                    return from > 0 && (isReadOnlyTransient.get() ? call == from : call >= from);
                }
            }) {
                for (int iteration = 0; iteration < iterations; iteration++) {
                    runCommitBatchFuzzIteration(rnd, hookEngine, iteration, readOnlyCalls, readOnlyFromCall, isReadOnlyTransient);
                }
            }
        });
    }

    /**
     * A commit batch holds frames of one table only: a frame for another table
     * commits the batch's table first. One writer's commit is all or nothing,
     * so no failure can commit part of a batch.
     */
    @Test
    public void testCommitBatchNeverSpansTables() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a"));
            final long bBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"));

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestInOnePass(
                    processor,
                    httpConfig,
                    AllowAllSecurityContext.INSTANCE,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L))
            );

            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n300\n");
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n200\n");
            Assert.assertEquals("a's frames are split by b's: two commits",
                    2, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a")) - aBefore);
            Assert.assertEquals(1, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b")) - bBefore);
            Assert.assertEquals(2, maxCumulativeOkAck(sent));
        });
    }

    /**
     * A commit failure on one table must not commit another table's rows from
     * frames the client will replay. Here b's commit is refused: frame 0 (a) is
     * committed and acked on its own, frame 1 (b) is NACKed, and frame 2 (a),
     * behind the gap, is neither committed nor acked -- one commit per frame
     * would leave exactly this, so the replay duplicates nothing.
     */
    @Test
    public void testCommitFailureOnOneTableCommitsNoLaterFramesOfAnother() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestInOnePass(
                    processor,
                    httpConfig,
                    denyCommitTo("b"),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L))
            );

            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n");
            assertQuery("SELECT count() FROM b").noLeakCheck().inferRandomAccess().sizeMayVary().returns("count\n0\n");
            Assert.assertEquals(0, maxCumulativeOkAck(sent));
            Assert.assertTrue("the failed commit must be NACKed at the batch's first frame", hasErrorResponseForSeq(sent, 1));
        });
    }

    /**
     * A deferred frame starts a client transaction: the commit batch before it
     * must commit on its own, never inside the client's group.
     */
    @Test
    public void testDeferredFrameCommitsPendingBatchFirst() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestInOnePass(
                    processor,
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, deferred(oneRowMessage(300L, 3_000_000L)))
            );

            drainWalQueue();
            assertQuery("SELECT v FROM tab").noLeakCheck().expectSize().returns("v\n100\n200\n");
            Assert.assertEquals("the open deferred frame must stay unacked", 1, maxCumulativeOkAck(sent));
        });
    }

    @Test
    public void testDurableAckCarriesBoundaryCommitBeforeStandaloneCapFailure() throws Exception {
        assertStandaloneFrameCapFailure(true);
    }

    /**
     * As {@link #assertStandaloneFrameCapFailure} on a durable-ack connection, but
     * the standalone frame cap-commits its first block, then fails its second
     * block's schema. Its partial commit survives, independently of the prefix.
     */
    @Test
    public void testDurableAckCarriesBoundaryCommitBeforeStandaloneSchemaFailure() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = httpConfigWithMaxUncommittedRows(2);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long txnBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("tab"));
            final byte[] mismatchedBlock = varcharTableBlock("tab", "not-a-long", 4_000_000L);

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestDurableInOnePass(
                    processor,
                    httpConfig,
                    AllowAllSecurityContext.INSTANCE,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                            tableBlock("tab", 200L, 2_000_000L, 300L, 3_000_000L),
                            mismatchedBlock
                    ))
            );

            drainWalQueue();
            assertQuery("SELECT v FROM tab").noLeakCheck().expectSize().returns("v\n100\n200\n300\n");
            Assert.assertEquals("the boundary-committed prefix must be acked", 0, maxCumulativeOkAck(sent));
            Assert.assertTrue("the mismatched frame must be NACKed by its own sequence", hasErrorResponseForSeq(sent, 1));
            Assert.assertTrue("the prefix ack must name the boundary commit",
                    okAckSeqTxn(sent, 0, "tab") > txnBefore);
        });
    }

    @Test
    public void testDurableAckCarriesCommitBeforeCapBoundaryFailure() throws Exception {
        assertBatchCommitsBeforeReachingCap(true);
    }

    /**
     * Durable-ack connections batch commits too: the single ack after the batch
     * commit names that commit's seqTxn, which a durable-ack client waits on for
     * every frame the ack covers.
     */
    @Test
    public void testDurableAckFramesShareOneCommit() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long txnBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("tab"));

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestDurableInOnePass(
                    processor,
                    httpConfig,
                    AllowAllSecurityContext.INSTANCE,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(400L, 4_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(500L, 5_000_000L))
            );

            drainWalQueue();
            try (TableReader reader = engine.getReader("tab")) {
                Assert.assertEquals(5, reader.size());
            }
            Assert.assertEquals("five frames in one receive pass must commit as one WAL txn",
                    txnBefore + 1, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("tab")));
            Assert.assertEquals(4, maxCumulativeOkAck(sent));
            Assert.assertEquals("one ack after the batch commit", 1, countOkAcks(sent));
            Assert.assertEquals("the ack must name the batch's seqTxn", txnBefore + 1, okAckSeqTxn(sent, 4, "tab"));
        });
    }

    @Test
    public void testEmptyFrameSeparatesCommitBatches() throws Exception {
        assertConservativeBoundaryTransactions(2, 2,
                clientEncodedFrame(1, "a"), clientEncodedFrame(0), clientEncodedFrame(1, "a"));
    }

    /**
     * A failed batch commit is NACKed at the batch's first frame: nothing from it
     * on is committed, and it is the frame the client replays first, so a
     * deterministic failure keeps striking the same frame.
     */
    @Test
    public void testFailedBatchCommitIsNackedAtFirstFrame() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestInOnePass(
                    processor,
                    httpConfig,
                    denyCommitTo("b"),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 300L, 3_000_000L))
            );

            Assert.assertEquals(-1, maxCumulativeOkAck(sent));
            Assert.assertTrue("NACK must name the batch's first frame", hasErrorResponseForSeq(sent, 0));
            Assert.assertFalse(hasErrorResponseForSeq(sent, 2));
        });
    }

    /**
     * As {@link #testFailedFrameInCommitBatchKeepsEarlierFramesCommittedAndAcked},
     * with a two-block failing frame and a batch whose rows start past an earlier
     * commit in the same WAL segment. A two-block frame cannot join, so the batch
     * commits and is acked first; the frame then appends 300 and fails on its
     * second block, and its rollback must discard 300 and nothing the batch
     * committed.
     */
    @Test
    public void testFailedFrameAfterCommitInSegmentDropsOnlyItsOwnRows() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final byte[] mismatchedBlock = varcharTableBlock("tab", "not-a-long", 4_000_000L);

            // seq=0 arrives alone and commits: the segment's open txn now starts at
            // row 1. seq=1 and seq=2 arrive together; seq=1 opens the batch, seq=2
            // appends 300 and then fails on its second block.
            ObjList<byte[]> sent = ingestInTwoPasses(
                    httpConfig,
                    1,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                            tableBlock("tab", 300L, 3_000_000L),
                            mismatchedBlock
                    ))
            );

            drainWalQueue();
            assertQuery("SELECT v FROM tab").noLeakCheck().expectSize().returns("v\n100\n200\n");
            Assert.assertEquals("the batch must be acked through the frame before the failed one", 1, maxCumulativeOkAck(sent));
            Assert.assertTrue("the failed frame must be NACKed by its own sequence", hasErrorResponseForSeq(sent, 2));
            Assert.assertFalse("the batch's frame must not be NACKed", hasErrorResponseForSeq(sent, 1));
        });
    }

    /**
     * A frame that fails right behind a commit batch costs only its own rows. Its
     * schema differs from the batch's, so it cannot join: the batch commits and is
     * acked before the frame's error goes out, as one commit per frame would leave
     * it. The client's poison detector strikes the frame a NACK names and replays
     * from its acked watermark, so losing the batch here would replay good frames
     * behind a NACK for a later one.
     */
    @Test
    public void testFailedFrameInCommitBatchKeepsEarlierFramesCommittedAndAcked() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            // seq=0,1 valid, seq=2 schema mismatch, seq=3 valid but behind the gap.
            ObjList<byte[]> sent = ingestInOnePass(
                    processor,
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowVarcharMessage("x", 3_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(400L, 4_000_000L))
            );

            drainWalQueue();
            assertQuery("SELECT v FROM tab").noLeakCheck().expectSize().returns("v\n100\n200\n");
            int ackIndex = indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 1);
            int errorIndex = indexOfErrorResponse(sent, 2);
            Assert.assertTrue("the frames before the failed one must be acked", ackIndex >= 0);
            Assert.assertTrue("the failed frame must be NACKed by its own sequence", errorIndex >= 0);
            Assert.assertTrue("the ack must precede the error", ackIndex < errorIndex);
            Assert.assertEquals("no ack may cover the failed frame or the tail behind it", 1, maxCumulativeOkAck(sent));
        });
    }

    /**
     * The multi-table shape of
     * {@link #testFailedFrameAfterCommitInSegmentDropsOnlyItsOwnRows}: the
     * pinned client's flush packs every table with rows into one message, so a
     * frame can append a good block to the batch's table and then fail on
     * another table's block. The frame cannot join, so the batch commits first,
     * and the frame's rollback must drop its good block from the batch's table
     * while the batch's rows stay committed.
     */
    @Test
    public void testFailedMultiTableFrameDropsItsBlockFromBatchTable() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final byte[] mismatchedBlock = varcharTableBlock("tab", "not-a-long", 4_000_000L);

            ObjList<byte[]> sent = ingestInTwoPasses(
                    httpConfig,
                    1,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                            tableBlock("a", 300L, 3_000_000L),
                            mismatchedBlock
                    ))
            );

            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n200\n");
            assertQuery("SELECT count() FROM tab").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
            Assert.assertEquals("the batch must be acked through the frame before the failed one", 1, maxCumulativeOkAck(sent));
            Assert.assertTrue("the failed frame must be NACKed by its own sequence", hasErrorResponseForSeq(sent, 2));
            Assert.assertFalse("the batch's frame must not be NACKed", hasErrorResponseForSeq(sent, 1));
        });
    }

    @Test
    public void testForeignMultiTableFrameRetainsThreeCommits() throws Exception {
        assertConservativeBoundaryTransactions(3, 3,
                clientEncodedFrame(1, "a"), clientEncodedFrame(1, "b", "c"));
    }

    @Test
    public void testForeignTableCapCannotCommitBeforeRefusedBatch() throws Exception {
        assertForeignTableCapBoundary(false);
    }

    @Test
    public void testForeignTableCapSeparatePassControl() throws Exception {
        assertForeignTableCapBoundary(true);
    }

    /**
     * As {@link #testOversizedFrameCommitsAndAcksTheBatchBeforeClosing}, for a fragmented
     * BINARY frame, which is refused with a fatal CLOSE.
     */
    @Test
    public void testFragmentedFrameCommitsAndAcksTheBatchBeforeClosing() throws Exception {
        assertBatchCommittedBeforeFatalClose(createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L), false));
    }

    @Test
    public void testMalformedFirstSchemaSalvagesPendingPrefix() throws Exception {
        assertMalformedPreparationSalvagesPrefix(false);
    }

    @Test
    public void testMalformedHeaderSalvagesPendingPrefix() throws Exception {
        assertMalformedPreparationSalvagesPrefix(true);
    }

    @Test
    public void testMultiTableFrameStartingWithBatchTableIsStandalone() throws Exception {
        assertConservativeBoundaryTransactions(3, 3,
                clientEncodedFrame(1, "a"), clientEncodedFrame(1, "a", "b"));
    }

    @Test
    public void testNothingFollowsRoleChangeCloseAtClientClose() throws Exception {
        assertNothingFollowsRoleChangeClose(clientCloseFrame());
    }

    @Test
    public void testNothingFollowsRoleChangeCloseAtContinuationFrame() throws Exception {
        assertNothingFollowsRoleChangeClose(createMaskedFrame(WebSocketOpcode.CONTINUATION, new byte[]{'x'}));
    }

    @Test
    public void testNothingFollowsRoleChangeCloseAtFragmentedFrame() throws Exception {
        assertNothingFollowsRoleChangeClose(createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L), false));
    }

    @Test
    public void testNothingFollowsRoleChangeCloseAtOversizedFrame() throws Exception {
        assertNothingFollowsRoleChangeClose(oversizedFrameHeader(new DefaultHttpServerConfiguration(configuration).getRecvBufferSize() + 1L));
    }

    @Test
    public void testNothingFollowsRoleChangeCloseAtPing() throws Exception {
        assertNothingFollowsRoleChangeClose(createMaskedFrame(WebSocketOpcode.PING, new byte[0]));
    }

    @Test
    public void testNothingFollowsRoleChangeCloseAtText() throws Exception {
        assertNothingFollowsRoleChangeClose(createMaskedFrame(WebSocketOpcode.TEXT, new byte[]{'x'}));
    }

    /**
     * As {@link #testPingBehindFailedBatchCommitIsAnsweredAfterTheNack}, for an oversized
     * frame behind the batch: the batch's NACK goes out, then the server closes for the
     * oversized frame.
     */
    @Test
    public void testOversizedFrameBehindFailedBatchCommitIsClosedAfterTheNack() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (Connection connection = new Connection(
                    engine,
                    httpConfig,
                    denyCommitTo("tab"),
                    false,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                    oversizedFrameHeader(httpConfig.getRecvBufferSize() + 1L)
            )) {
                connection.deliverFrames(3);
                final ObjList<byte[]> sent = connection.sentFrames();
                Assert.assertEquals("the batch's NACK, then the CLOSE", 2, sent.size());
                Assert.assertEquals("the NACK names the batch's first frame", 0, indexOfErrorResponse(sent, 0));
                Assert.assertEquals("the oversized frame must still be closed", WebSocketOpcode.CLOSE, sent.getQuick(1)[0] & 0x0F);
                Assert.assertEquals(WebSocketCloseCode.MESSAGE_TOO_BIG, closeCode(sent.getQuick(1)));
            }
            drainWalQueue();
            assertQuery("SELECT v FROM tab").noLeakCheck().expectSize().returns("v\n");
        });
    }

    /**
     * A fatal CLOSE for an oversized frame flushes the acks of the frames before
     * it; those frames must have been committed first, as one commit per frame
     * would have done.
     */
    @Test
    public void testOversizedFrameCommitsAndAcksTheBatchBeforeClosing() throws Exception {
        final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
        assertBatchCommittedBeforeFatalClose(oversizedFrameHeader(httpConfig.getRecvBufferSize() + 1L));
    }

    /**
     * An oversized frame behind the batch on a durable-ack connection: the demote
     * refuses the batch commit and the server sends its role-change CLOSE, not a
     * second one for the oversized frame. That frame's payload can never be
     * buffered, so the bytes the client streams as its payload must not be parsed as
     * frames; here they spell a CLOSE that would complete the handshake.
     */
    @Test
    public void testOversizedFramePayloadIsNotParsedAfterRoleChangeClose() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (DemotableEngine demotableEngine = new DemotableEngine()) {
                demotableEngine.demoteAtNextCommit();
                try (Connection connection = new Connection(
                        demotableEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        true,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                        oversizedFrameHeader(httpConfig.getRecvBufferSize() + 1L),
                        clientCloseFrame()
                )) {
                    Assert.assertFalse(connection.deliverFrames(3));
                    Assert.assertTrue("test setup: the demote must land at the batch commit", demotableEngine.isReadOnlyMode());
                    assertOnlyRoleChangeCloseSent(connection.sentFrames());
                    Assert.assertFalse(
                            "payload bytes of the oversized frame must not be parsed as a CLOSE",
                            connection.deliverFrames(1)
                    );
                    assertOnlyRoleChangeCloseSent(connection.sentFrames());
                }
                assertTableEmpty(demotableEngine);
            }
        });
    }

    /**
     * Control for {@link #assertNothingFollowsRoleChangeClose}: a demote that refuses
     * the batch commit while earlier committed work still awaits durable upload defers
     * the role-change CLOSE. No CLOSE is on the wire, so the connection keeps serving
     * the client until the final durable ack can go out, and the PING behind the batch
     * gets its PONG.
     */
    @Test
    public void testPingBehindDemoteRefusedBatchIsAnsweredWhileCloseIsDeferred() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (DemotableEngine demotableEngine = new DemotableEngine()) {
                try (Connection connection = new Connection(
                        demotableEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        true,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L)),
                        createMaskedFrame(WebSocketOpcode.PING, new byte[0])
                )) {
                    // The first frame commits on its own; its durable upload never catches up.
                    Assert.assertFalse(connection.deliverFrames(1));
                    demotableEngine.demoteAtNextCommit();
                    Assert.assertFalse(connection.deliverFrames(3));
                    Assert.assertTrue("test setup: the demote must land at the batch commit", demotableEngine.isReadOnlyMode());
                    final ObjList<byte[]> sent = connection.sentFrames();
                    Assert.assertEquals("a deferred close sends no CLOSE", -1, indexOfCloseFrame(sent));
                    Assert.assertEquals("the PING behind the refused batch must get its PONG", WebSocketOpcode.PONG, sent.getLast()[0] & 0x0F);
                    Assert.assertEquals("the refused batch must not be acked", 0, maxCumulativeOkAck(sent));
                }
                drainWalQueue(demotableEngine);
                try (TableReader reader = demotableEngine.getReader("tab")) {
                    Assert.assertEquals("only the first frame is committed", 1, reader.size());
                }
            }
        });
    }

    /**
     * Control for {@link #assertNothingFollowsRoleChangeClose}: a batch commit that
     * fails for a reason other than a demote sends a NACK, not a CLOSE, so the frame
     * behind the batch is still handled -- here a PING gets its PONG after the NACK.
     */
    @Test
    public void testPingBehindFailedBatchCommitIsAnsweredAfterTheNack() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (Connection connection = new Connection(
                    engine,
                    httpConfig,
                    denyCommitTo("tab"),
                    false,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.PING, new byte[0])
            )) {
                Assert.assertFalse(connection.deliverFrames(3));
                final ObjList<byte[]> sent = connection.sentFrames();
                Assert.assertEquals("the batch's NACK, then the PONG", 2, sent.size());
                Assert.assertEquals("the NACK names the batch's first frame", 0, indexOfErrorResponse(sent, 0));
                Assert.assertEquals("the PING must get its PONG", WebSocketOpcode.PONG, sent.getQuick(1)[0] & 0x0F);
            }
            drainWalQueue();
            assertQuery("SELECT v FROM tab").noLeakCheck().expectSize().returns("v\n");
        });
    }

    /**
     * Plain frames already in the receive buffer share one WAL commit, and the
     * single ack after it covers all of them.
     */
    @Test
    public void testPipelinedFramesShareOneCommit() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long txnBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("tab"));

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestInOnePass(
                    processor,
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(400L, 4_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(500L, 5_000_000L))
            );

            drainWalQueue();
            try (TableReader reader = engine.getReader("tab")) {
                Assert.assertEquals(5, reader.size());
            }
            Assert.assertEquals("five frames in one receive pass must commit as one WAL txn",
                    1, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("tab")) - txnBefore);
            Assert.assertEquals(4, maxCumulativeOkAck(sent));
            Assert.assertEquals("one ack after the batch commit", 1, countOkAcks(sent));
        });
    }

    /**
     * A plain frame closing a rowless deferred group commits and acks on its own, so a
     * later NACK cannot strand its rows.
     */
    @Test
    public void testPlainFrameAfterRowlessDeferredFrameIsAckedBeforeLaterNack() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestInOnePass(
                    processor,
                    httpConfig,
                    denyCommitTo("b"),
                    createMaskedFrame(WebSocketOpcode.BINARY, rowlessDeferredMessage()),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L))
            );

            Assert.assertEquals("the ack must cover the committed frame and stop at the refused one", 1, maxCumulativeOkAck(sent));
            Assert.assertTrue("the refused frame must be NACKed by its own sequence", hasErrorResponseForSeq(sent, 2));
            Assert.assertFalse("a NACK at seq=1 would replay committed rows", hasErrorResponseForSeq(sent, 1));
            int ackIndex = indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 1);
            int errorIndex = indexOfErrorResponse(sent, 2);
            Assert.assertTrue("the committed frame must be acked", ackIndex >= 0);
            Assert.assertTrue("the ack must precede the error", ackIndex < errorIndex);

            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n");
        });
    }

    @Test
    public void testRefusedBatchPreventsMultiTableForeignWriterLookup() throws Exception {
        assertRefusedBatchPreventsForeignWriterLookup("b", "c");
    }

    @Test
    public void testRefusedBatchPreventsRepeatedBlockForeignWriterLookup() throws Exception {
        assertRefusedBatchPreventsForeignWriterLookup("b", "b");
    }

    @Test
    public void testRefusedBatchPreventsSingleBlockForeignWriterLookup() throws Exception {
        assertRefusedBatchPreventsForeignWriterLookup("b");
    }

    @Test
    public void testRenameWindowMidBatchAcksNothing() throws Exception {
        assertRenameWindowMidBatchAcksNothing(false);
    }

    @Test
    public void testRenameWindowMidBatchWithRefusedInsertAcksNothing() throws Exception {
        assertRenameWindowMidBatchAcksNothing(true);
    }

    @Test
    public void testRepeatedBlockFrameSeparatesCommitBatches() throws Exception {
        assertConservativeBoundaryTransactions(3, 4,
                clientEncodedFrame(1, "a"), clientEncodedFrame(1, "a", "a"), clientEncodedFrame(1, "a"));
    }

    @Test
    public void testRowlessRepeatedBlockFrameSeparatesCommitBatches() throws Exception {
        assertConservativeBoundaryTransactions(2, 2,
                clientEncodedFrame(1, "a"), clientEncodedFrame(0, "a", "a"), clientEncodedFrame(1, "a"));
    }

    @Test
    public void testRowlessSingletonSeparatesCommitBatches() throws Exception {
        assertConservativeBoundaryTransactions(2, 2,
                clientEncodedFrame(1, "a"), clientEncodedFrame(0, "a"), clientEncodedFrame(1, "a"));
    }

    @Test
    public void testRowlessThenRowBearingMultiBlockIsStandalone() throws Exception {
        assertConservativeBoundaryTransactions(3, 3,
                clientEncodedFrame(1, "a"),
                createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(tableBlock("b"), tableBlock("a", 200, 2_000_000))),
                clientEncodedFrame(1, "a"));
    }

    @Test
    public void testSingleBlockFramesRetainOneCommit() throws Exception {
        assertConservativeBoundaryTransactions(1, 3,
                clientEncodedFrame(1, "a"), clientEncodedFrame(1, "a"), clientEncodedFrame(1, "a"));
    }

    @Test
    public void testStandaloneFrameCapFailureIsNackedAfterBoundaryAck() throws Exception {
        assertStandaloneFrameCapFailure(false);
    }

    private static void assertOnlyRoleChangeCloseSent(ObjList<byte[]> sent) {
        Assert.assertEquals("the server's role-change CLOSE must be the only frame it sends", 1, sent.size());
        Assert.assertEquals("the frame must be a CLOSE", WebSocketOpcode.CLOSE, sent.getQuick(0)[0] & 0x0F);
        Assert.assertEquals("a role-change CLOSE is reconnect-eligible", WebSocketCloseCode.NORMAL_CLOSURE, closeCode(sent.getQuick(0)));
    }

    private static void assertTableEmpty(CairoEngine engine) {
        drainWalQueue(engine);
        try (TableReader reader = engine.getReader("tab")) {
            Assert.assertEquals("nothing of the refused batch may be committed", 0, reader.size());
        }
    }

    private static byte[] clientCloseFrame() {
        return createMaskedFrame(WebSocketOpcode.CLOSE, new byte[]{0x03, (byte) 0xE8});
    }

    private static int closeCode(byte[] serverCloseFrame) {
        // server frames are unmasked and CLOSE payloads are short: the code is at offset 2
        return (serverCloseFrame[2] & 0xFF) << 8 | (serverCloseFrame[3] & 0xFF);
    }

    private static int countOkAcks(ObjList<byte[]> frames) {
        int count = 0;
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (isBinaryFrame(f) && f[2] == QwpConstants.STATUS_OK) {
                count++;
            }
        }
        return count;
    }

    private static SecurityContext denyCommitTo(String tableName) {
        // On the QWP path authorizeInsert runs only at commit (TableUpdateDetails.authorizeCommit).
        return new AllowAllSecurityContext() {
            @Override
            public void authorizeInsert(TableToken tableToken) {
                if (tableToken.getTableName().equals(tableName)) {
                    throw CairoException.nonCritical().put("test: commit denied for ").put(tableName);
                }
            }
        };
    }

    private static SecurityContext denyCommitsAfterFirst(String tableName) {
        // On the QWP path authorizeInsert runs only at commit, and only for a commit
        // with rows (TableUpdateDetails.commit): the first such commit passes.
        return new AllowAllSecurityContext() {
            private int commits;

            @Override
            public void authorizeInsert(TableToken tableToken) {
                if (tableToken.getTableName().equals(tableName) && ++commits > 1) {
                    throw CairoException.nonCritical().put("test: commit denied for ").put(tableName);
                }
            }
        };
    }

    private static long errorStatusForSeq(ObjList<byte[]> frames, long seq) {
        final int index = indexOfErrorResponse(frames, seq);
        return index < 0 ? -1 : frames.getQuick(index)[2];
    }

    private static HttpFullFatServerConfiguration httpConfigWithMaxUncommittedRows(long maxUncommittedRows) {
        final LineHttpProcessorConfiguration lineConfig = new DefaultLineHttpProcessorConfiguration(configuration) {
            @Override
            public long getQwpMaxUncommittedRows() {
                return maxUncommittedRows;
            }
        };
        return new DefaultHttpServerConfiguration(configuration) {
            @Override
            public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
                return lineConfig;
            }
        };
    }

    private static int indexOfCloseFrame(ObjList<byte[]> frames) {
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (f.length >= 2 && (f[0] & 0x0F) == WebSocketOpcode.CLOSE) {
                return i;
            }
        }
        return -1;
    }

    private static int indexOfErrorResponse(ObjList<byte[]> frames, long seq) {
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (isBinaryFrame(f) && f[2] != QwpConstants.STATUS_OK && readLeLong(f, 3) == seq) {
                return i;
            }
        }
        return -1;
    }

    /**
     * The seqTxn that the cumulative OK-ACK for {@code seq} names for
     * {@code table}, or -1 when there is no such ack or it has no entry for the
     * table. After [status][seq LE] an OK-ACK carries [count u16 LE], then per
     * table [name length u16 LE][name UTF-8][seqTxn i64 LE].
     */
    private static long okAckSeqTxn(ObjList<byte[]> frames, long seq, String table) {
        final byte[] name = table.getBytes(StandardCharsets.UTF_8);
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (!isBinaryFrame(f) || f[2] != QwpConstants.STATUS_OK || readLeLong(f, 3) != seq) {
                continue;
            }
            int pos = 13;
            for (int entry = 0, count = readLeShort(f, 11); entry < count; entry++) {
                int nameLen = readLeShort(f, pos);
                pos += 2;
                boolean isMatch = Arrays.equals(f, pos, pos + nameLen, name, 0, name.length);
                pos += nameLen;
                if (isMatch) {
                    return readLeLong(f, pos);
                }
                pos += 8;
            }
        }
        return -1;
    }

    private static byte[] oversizedFrameHeader(long len) {
        // BINARY, FIN, masked, 64-bit length above the configured receive buffer;
        // the payload never needs to arrive: the header alone is refused.
        byte[] header = new byte[2 + 8 + 4];
        header[0] = (byte) (0x80 | WebSocketOpcode.BINARY);
        header[1] = (byte) (0x80 | 127);
        for (int i = 0; i < 8; i++) {
            header[2 + i] = (byte) (len >>> (56 - 8 * i));
        }
        System.arraycopy(DEFAULT_MASK_KEY, 0, header, 10, 4);
        return header;
    }

    private static int readLeShort(byte[] buf, int offset) {
        return (buf[offset] & 0xFF) | (buf[offset + 1] & 0xFF) << 8;
    }

    private static byte[][] toArray(ObjList<byte[]> frames) {
        byte[][] array = new byte[frames.size()][];
        for (int i = 0, n = frames.size(); i < n; i++) {
            array[i] = frames.getQuick(i);
        }
        return array;
    }

    /**
     * The appender commits a writer on its own once its uncommitted rows reach
     * maxUncommittedRows; a commit batch never lets that happen. With a cap of 2,
     * frame 1 cannot join frame 0's batch, so frame 0 commits on its own (the one
     * allowed commit) and frame 1 opens a new batch; frame 2 cannot join it either,
     * and frame 1's commit is denied. The wire matches one commit per frame: frame 0
     * committed and acked, frame 1 NACKed, frame 2 refused behind it. A durable ack
     * must also name frame 0's commit.
     */
    private void assertBatchCommitsBeforeReachingCap(boolean isDurableAck) throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = httpConfigWithMaxUncommittedRows(2);
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long before = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"));

            final ObjList<byte[]> sent = new ObjList<>();
            ingestInOnePass(
                    engine,
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    denyCommitsAfterFirst("b"),
                    isDurableAck,
                    sent,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 300L, 3_000_000L))
            );

            drainWalQueue();
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n100\n");
            Assert.assertEquals("only frame 0 committed", 1, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b")) - before);
            Assert.assertEquals("the committed frame must be acked", 0, maxCumulativeOkAck(sent));
            Assert.assertTrue("the denied commit must be NACKed at its batch's first frame", hasErrorResponseForSeq(sent, 1));
            Assert.assertFalse(hasErrorResponseForSeq(sent, 0));
            Assert.assertFalse("a frame behind the refused batch gets no response", hasErrorResponseForSeq(sent, 2));
            if (isDurableAck) {
                Assert.assertEquals("the ack must name frame 0's commit", before + 1, okAckSeqTxn(sent, 0, "b"));
            }
        });
    }

    private void assertBatchCommittedBeforeFatalClose(byte[] badFrame) throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            final ObjList<byte[]> sent = new ObjList<>();
            try {
                ingestInOnePass(
                        engine,
                        processor,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        sent,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                        badFrame
                );
            } catch (ServerDisconnectException ignore) {
                // the fatal CLOSE ends with a drain and disconnect
            }
            final int closeIndex = indexOfCloseFrame(sent);
            Assert.assertTrue("test setup: the bad frame must draw a fatal CLOSE", closeIndex >= 0);
            drainWalQueue();
            assertQuery("SELECT v FROM tab").noLeakCheck().expectSize().returns("v\n100\n200\n");
            Assert.assertEquals("the frames before the bad one must be acked ahead of the CLOSE", 1, maxCumulativeOkAck(sent));
            Assert.assertTrue("the ack must precede the CLOSE", indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 1) < closeIndex);
        });
    }

    private void assertConservativeBoundaryTransactions(long expectedTransactions, long expectedRows, byte[]... frames) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE c (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            long before = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a"))
                    + engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"))
                    + engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("c"));
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            ingestInOnePass(new QwpIngressUpgradeProcessor(engine, httpConfig), httpConfig, frames);
            drainWalQueue();
            try (TableReader a = engine.getReader("a"); TableReader b = engine.getReader("b"); TableReader c = engine.getReader("c")) {
                Assert.assertEquals(expectedRows, a.size() + b.size() + c.size());
            }
            long after = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a"))
                    + engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"))
                    + engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("c"));
            Assert.assertEquals("only single-block same-table frames may share a commit", expectedTransactions, after - before);
        });
    }

    private void assertForeignTableCapBoundary(boolean isSeparatePass) throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = httpConfigWithMaxUncommittedRows(2);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            long bBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"));
            try (Connection connection = new Connection(
                    engine,
                    httpConfig,
                    denyCommitTo("a"),
                    false,
                    clientEncodedFrame(1, "a"),
                    clientEncodedFrame(2, "b")
            )) {
                if (isSeparatePass) {
                    connection.deliverFrames(1);
                    connection.deliverFrames(1);
                } else {
                    connection.deliverFrames(2);
                }
            }
            // Inspect storage after teardown rolled back all open transactions.
            drainWalQueue();
            try (TableReader a = engine.getReader("a"); TableReader b = engine.getReader("b")) {
                Assert.assertEquals(0, a.size());
                Assert.assertEquals("later B data must not survive refused A", 0, b.size());
            }
            Assert.assertEquals("later B must not publish a WAL transaction", bBefore,
                    engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b")));
        });
    }

    /**
     * The read-only gate refuses frame 2 with a batch of frames 0 and 1 pending. The
     * engine is scripted by call count: frame 0 checks at preparation and before its
     * lookup (calls 1, 2) and opens the batch; frame 1 joins it, checked only at
     * preparation (call 3); frame 2's preparation sees the demote (call 4).
     * <p>
     * A transient demote reverts before the batch commit's checks: the commit
     * succeeds, so the batch must be acked before the role-change CLOSE, or the
     * client replays rows the server holds. A demote that reverts this fast is an
     * enterprise in-place role switch, which OSS cannot produce, hence the script.
     * A demote that holds refuses the commit too: nothing is committed or acked, and
     * the client replays the whole batch on a primary.
     */
    private void assertGateRefusalWithBatchPending(boolean isDemoteTransient) throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger readOnlyChecks = new AtomicInteger();
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (CairoEngine demotableEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                @Override
                public boolean isReadOnlyMode() {
                    final int call = readOnlyChecks.incrementAndGet();
                    return isDemoteTransient ? call == 4 : call >= 4;
                }
            }) {
                final ObjList<byte[]> sent = new ObjList<>();
                try {
                    ingestInOnePass(
                            demotableEngine,
                            new QwpIngressUpgradeProcessor(demotableEngine, httpConfig),
                            httpConfig,
                            AllowAllSecurityContext.INSTANCE,
                            false,
                            sent,
                            createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                            createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                            createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L))
                    );
                } catch (ServerDisconnectException ignore) {
                    // the role-change CLOSE tells the dispatcher to drop the fd
                }

                Assert.assertTrue("test setup: the batch commit must have re-checked the gate after the refusal",
                        readOnlyChecks.get() >= 5);
                drainWalQueue(demotableEngine);
                try (TableReader reader = demotableEngine.getReader("tab")) {
                    Assert.assertEquals("the batch commits only if the demote reverts", isDemoteTransient ? 2 : 0, reader.size());
                }
                final int closeIndex = indexOfCloseFrame(sent);
                Assert.assertTrue("the gate refusal must close the connection", closeIndex >= 0);
                Assert.assertFalse("a role-change refusal sends no error response", hasErrorResponseForSeq(sent, 0));
                Assert.assertFalse("a role-change refusal sends no error response", hasErrorResponseForSeq(sent, 2));
                if (isDemoteTransient) {
                    final int ackIndex = indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 1);
                    Assert.assertTrue("the committed batch must be acked, or the client replays it after the close", ackIndex >= 0);
                    Assert.assertTrue("the ack must precede the CLOSE", ackIndex < closeIndex);
                    Assert.assertEquals("no ack may cover the refused frame", 1, maxCumulativeOkAck(sent));
                } else {
                    Assert.assertEquals("nothing committed, nothing acked", -1, maxCumulativeOkAck(sent));
                }
            }
        });
    }

    private void assertMalformedPreparationSalvagesPrefix(boolean isHeader) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            byte[] malformed = oneRowMessage(200, 2_000_000);
            // Truncate either the header or first schema, before any table lookup.
            malformed = Arrays.copyOf(malformed, isHeader ? 3 : QwpConstants.HEADER_SIZE + 8);
            HttpFullFatServerConfiguration config = new DefaultHttpServerConfiguration(configuration);
            ObjList<byte[]> sent = ingestInOnePass(new QwpIngressUpgradeProcessor(engine, config), config,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100, 1_000_000)),
                    createMaskedFrame(WebSocketOpcode.BINARY, malformed),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300, 3_000_000)));
            drainWalQueue();
            assertQuery("SELECT v FROM tab").noLeakCheck().expectSize().returns("v\n100\n");
            Assert.assertEquals(0, maxCumulativeOkAck(sent));
            Assert.assertTrue(hasErrorResponseForSeq(sent, 1));
            Assert.assertFalse(hasErrorResponseForSeq(sent, 0));
        });
    }

    /**
     * A control frame, a fragment or an oversized frame behind a commit batch makes
     * the batch commit first. When a demote refuses that commit, the server sends its
     * role-change CLOSE and half-closes; without durable acks the connection then
     * drains instead of waiting for the echo. Nothing may follow that CLOSE: on a real
     * socket a write after the half-close fails, and the abortive teardown can discard
     * the client's unread final ack.
     */
    private void assertNothingFollowsRoleChangeClose(byte[] trailingFrame) throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (DemotableEngine demotableEngine = new DemotableEngine()) {
                demotableEngine.demoteAtNextCommit();
                try (Connection connection = new Connection(
                        demotableEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                        trailingFrame
                )) {
                    connection.deliverFrames(3);
                    Assert.assertTrue("test setup: the demote must land at the batch commit", demotableEngine.isReadOnlyMode());
                    assertOnlyRoleChangeCloseSent(connection.sentFrames());
                }
                assertTableEmpty(demotableEngine);
            }
        });
    }

    private void assertPipelinedClientCloseIsNotTheEcho(byte[]... pipelinedFrames) throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final ObjList<byte[]> frames = new ObjList<>();
            frames.add(createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)));
            frames.add(createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)));
            for (byte[] frame : pipelinedFrames) {
                frames.add(frame);
            }
            frames.add(clientCloseFrame()); // the echo, sent after the client read the server's CLOSE
            try (DemotableEngine demotableEngine = new DemotableEngine()) {
                demotableEngine.demoteAtNextCommit();
                try (Connection connection = new Connection(demotableEngine, httpConfig, AllowAllSecurityContext.INSTANCE, true, toArray(frames))) {
                    Assert.assertFalse(
                            "a CLOSE pipelined before the server's CLOSE must not complete the handshake",
                            connection.deliverFrames(frames.size() - 1)
                    );
                    Assert.assertTrue("test setup: the demote must land at the batch commit", demotableEngine.isReadOnlyMode());
                    assertOnlyRoleChangeCloseSent(connection.sentFrames());
                    Assert.assertTrue("the client's echo completes the handshake", connection.deliverFrames(1));
                    assertOnlyRoleChangeCloseSent(connection.sentFrames());
                }
                assertTableEmpty(demotableEngine);
            }
        });
    }

    private void assertRefusedBatchPreventsForeignWriterLookup(String... tables) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE c (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            AtomicInteger foreignLookups = new AtomicInteger();
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            try (CairoEngine lookupEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                @Override
                public WalWriter getWalWriter(TableToken token) {
                    if (!token.getTableName().equals("a")) {
                        foreignLookups.incrementAndGet();
                    }
                    return super.getWalWriter(token);
                }
            }) {
                ingestInOnePass(lookupEngine, new QwpIngressUpgradeProcessor(lookupEngine, httpConfig),
                        httpConfig, denyCommitTo("a"), clientEncodedFrame(1, "a"), clientEncodedFrame(1, tables));
            }
            Assert.assertEquals("refused batch must stop before foreign writer acquisition", 0, foreignLookups.get());
        });
    }

    /**
     * A RENAME TABLE lands while a commit batch is pending, frozen in the window where
     * the sequencer already logged it but the name registry still maps the old name
     * (CairoEngine.rename publishes the sequencer txn first). The next frame is a batch
     * boundary: the rename moved the table's structure version. The batch's own commit
     * is then refused -- by the sequencer's token check, or by an INSERT refusal that
     * lands with the rename -- so nothing of the batch may be acked; the client replays
     * it by name.
     */
    private void assertRenameWindowMidBatchAcksNothing(boolean isInsertRefused) throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final AtomicBoolean isFrozen = new AtomicBoolean();
            final AtomicBoolean isDenied = new AtomicBoolean();
            final AtomicLong renameTxn = new AtomicLong(-1);
            final AtomicReference<QwpIngressProcessorState> stateRef = new AtomicReference<>();
            try (CairoEngine hookEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                @Override
                public boolean isReadOnlyMode() {
                    // The first gate check with a batch pending is frame 1's preparation.
                    final QwpIngressProcessorState state = stateRef.get();
                    if (!isFrozen.get() && state != null && state.isCommitBatchPending()) {
                        isFrozen.set(true);
                        try (WalWriter w = getWalWriter(verifyTableName("tab"))) {
                            renameTxn.set(w.renameTable("tab", "tab_new", AllowAllSecurityContext.INSTANCE));
                        }
                        isDenied.set(isInsertRefused);
                    }
                    return false;
                }
            }) {
                final TableToken token = hookEngine.verifyTableName("tab");
                final long txnBefore = hookEngine.getTableSequencerAPI().lastTxn(token);
                final SecurityContext securityContext = new AllowAllSecurityContext() {
                    @Override
                    public void authorizeInsert(TableToken tableToken) {
                        if (isDenied.get()) {
                            throw CairoException.authorization().put("test: insert denied on ").put(tableToken.getTableName());
                        }
                    }
                };
                final ObjList<byte[]> sent;
                try (Connection connection = new Connection(
                        hookEngine,
                        httpConfig,
                        securityContext,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L))
                )) {
                    stateRef.set(connection.getState());
                    connection.deliverFrames(2);
                    sent = connection.sentFrames();
                }
                Assert.assertTrue("test setup: the rename window must have been frozen", isFrozen.get());
                Assert.assertEquals("test setup: the rename is the only txn", txnBefore + 1, renameTxn.get());
                Assert.assertEquals("no data txn: the batch's commit was refused",
                        renameTxn.get(), hookEngine.getTableSequencerAPI().lastTxn(token));
                Assert.assertEquals("nothing committed may be acked", -1, maxCumulativeOkAck(sent));
                Assert.assertTrue("the batch is refused at its first frame", hasErrorResponseForSeq(sent, 0));
                Assert.assertFalse("the boundary frame is dropped and replayed with the batch", hasErrorResponseForSeq(sent, 1));
            }
        });
    }

    /**
     * A multi-block frame behind a one-frame batch is a boundary, so the batch commits
     * first; that is the one allowed commit. The frame's first block then reaches the
     * cap and its commit is denied. Only the prefix survives and is acked, and the
     * frame is NACKed by its own sequence. A durable ack must also name the boundary
     * commit, although the frame fails before its own ordinary commit.
     */
    private void assertStandaloneFrameCapFailure(boolean isDurableAck) throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = httpConfigWithMaxUncommittedRows(2);
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long txnBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"));

            final ObjList<byte[]> sent = new ObjList<>();
            ingestInOnePass(
                    engine,
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    denyCommitsAfterFirst("b"),
                    isDurableAck,
                    sent,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                            tableBlock("b", 200L, 2_000_000L, 300L, 3_000_000L),
                            tableBlock("b", 400L, 4_000_000L)
                    ))
            );

            drainWalQueue();
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n100\n");
            Assert.assertEquals("the boundary-committed prefix must be acked", 0, maxCumulativeOkAck(sent));
            Assert.assertTrue("the denied frame must be NACKed by its own sequence", hasErrorResponseForSeq(sent, 1));
            Assert.assertFalse("a NACK at seq=0 would replay committed rows", hasErrorResponseForSeq(sent, 0));
            if (isDurableAck) {
                Assert.assertTrue("the prefix ack must name the boundary commit", okAckSeqTxn(sent, 0, "b") > txnBefore);
            }
        });
    }

    private ObjList<byte[]> ingestDurableInOnePass(
            QwpIngressUpgradeProcessor processor,
            HttpFullFatServerConfiguration httpConfig,
            SecurityContext securityContext,
            byte[]... frames
    ) throws Exception {
        final ObjList<byte[]> sent = new ObjList<>();
        ingestInOnePass(engine, processor, httpConfig, securityContext, true, sent, frames);
        return sent;
    }

    private ObjList<byte[]> ingestInOnePass(
            QwpIngressUpgradeProcessor processor,
            HttpFullFatServerConfiguration httpConfig,
            byte[]... frames
    ) throws Exception {
        return ingestInOnePass(processor, httpConfig, AllowAllSecurityContext.INSTANCE, frames);
    }

    private ObjList<byte[]> ingestInOnePass(
            QwpIngressUpgradeProcessor processor,
            HttpFullFatServerConfiguration httpConfig,
            SecurityContext securityContext,
            byte[]... frames
    ) throws Exception {
        return ingestInOnePass(engine, processor, httpConfig, securityContext, frames);
    }

    /**
     * Delivers every frame in one receive pass, with the connection state bound to
     * {@code stateEngine}, whose {@code isReadOnlyMode()} the gate and the commit
     * path consult.
     */
    private ObjList<byte[]> ingestInOnePass(
            CairoEngine stateEngine,
            QwpIngressUpgradeProcessor processor,
            HttpFullFatServerConfiguration httpConfig,
            SecurityContext securityContext,
            byte[]... frames
    ) throws Exception {
        final ObjList<byte[]> sent = new ObjList<>();
        ingestInOnePass(stateEngine, processor, httpConfig, securityContext, false, sent, frames);
        return sent;
    }

    private void ingestInOnePass(
            CairoEngine stateEngine,
            QwpIngressUpgradeProcessor processor,
            HttpFullFatServerConfiguration httpConfig,
            SecurityContext securityContext,
            boolean isDurableAckEnabled,
            ObjList<byte[]> sent,
            byte[]... frames
    ) throws Exception {
        byte[] wire = concat(frames);
        assert wire.length <= RECV_BUFFER_SIZE;
        PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
        long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
        try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
            QwpIngressProcessorState state = new QwpIngressProcessorState(
                    RECV_BUFFER_SIZE,
                    httpConfig.getSendBufferSize(),
                    stateEngine,
                    httpConfig.getLineHttpProcessorConfiguration()
            );
            state.of(-1, securityContext);
            state.setDurableAckEnabled(isDurableAckEnabled);
            getLV().set(context, state);
            // Every frame arrives in one receive pass, as from a client with many in flight.
            drive(processor, context, nf, wire.length);
        } finally {
            sent.addAll(rawSocket.sentFrames);
            Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Like {@link #ingestInOnePass} but the first {@code firstPassFrameCount}
     * frames arrive in one receive pass and the rest in a second one on the same
     * connection, so the first pass's commit batch is committed -- and its rows
     * sit committed in the cached writers' segments -- before the second pass's
     * batch forms.
     */
    private ObjList<byte[]> ingestInTwoPasses(
            HttpFullFatServerConfiguration httpConfig,
            int firstPassFrameCount,
            byte[]... frames
    ) throws Exception {
        try (Connection connection = new Connection(engine, httpConfig, AllowAllSecurityContext.INSTANCE, false, frames)) {
            connection.deliverFrames(firstPassFrameCount);
            connection.deliverFrames(frames.length - firstPassFrameCount);
            return connection.sentFrames();
        }
    }

    private void runCommitBatchFuzzIteration(
            Rnd rnd,
            CairoEngine hookEngine,
            int iteration,
            AtomicLong readOnlyCalls,
            AtomicLong readOnlyFromCall,
            AtomicBoolean isReadOnlyTransient
    ) throws Exception {
        final String[] tables = {"fa" + iteration, "fb" + iteration};
        final boolean isDataErrorsOnly = rnd.nextInt(3) == 0;
        final boolean isDurable = rnd.nextBoolean();
        final long cap = rnd.nextInt(3) == 0 ? 1 + rnd.nextInt(4) : 1_000_000;
        readOnlyCalls.set(0);
        readOnlyFromCall.set(!isDataErrorsOnly && rnd.nextInt(3) == 0 ? 1 + rnd.nextInt(30) : -1);
        isReadOnlyTransient.set(rnd.nextBoolean());
        final int refusedInsertCall = !isDataErrorsOnly && rnd.nextInt(3) == 0 ? 1 + rnd.nextInt(6) : -1;
        final boolean isRefusalPersistent = rnd.nextBoolean();
        final boolean isCommitFaulty = refusedInsertCall > 0 || readOnlyFromCall.get() > 0;
        final AtomicInteger insertCalls = new AtomicInteger();
        final SecurityContext securityContext = new AllowAllSecurityContext() {
            @Override
            public void authorizeInsert(TableToken tableToken) {
                final int call = insertCalls.incrementAndGet();
                if (refusedInsertCall > 0 && (isRefusalPersistent ? call >= refusedInsertCall : call == refusedInsertCall)) {
                    throw CairoException.authorization().put("fuzz: insert refused");
                }
            }
        };

        // Frame values are seq * 10 + row, so a committed value names its frame.
        final StringBuilder kinds = new StringBuilder();
        final ObjList<byte[]> wireFrames = new ObjList<>();
        final ObjList<LongList> frameValues = new ObjList<>();
        final LongList frameErrorStatus = new LongList();
        final int frameCount = 2 + rnd.nextInt(10);
        int seq = 0;
        for (int f = 0; f < frameCount; f++) {
            int kind = rnd.nextInt(isDataErrorsOnly ? FUZZ_DATA_KINDS : FUZZ_KINDS);
            if ((cap < 1_000_000 && kind == FUZZ_TWO_BLOCKS) || (kind == FUZZ_DEFERRED && (cap < 1_000_000 || isCommitFaulty))) {
                // Outside the commit batch's contract, with or without batching: a frame
                // of more than one block, or a deferred group, can be split by the
                // appender's own commit at a small cap, and commitAll commits a group's
                // tables one by one; a later commit of the same frame or group that then
                // fails NACKs rows already in the WAL.
                kind = FUZZ_PLAIN;
            }
            final String table = tables[rnd.nextInt(2)];
            final long v = seq * 10L;
            final long ts = 1_000_000L + seq * 1_000L;
            final LongList values = new LongList();
            long errorStatus = -1;
            final byte[] message;
            kinds.append(kind).append(table.charAt(1)).append(' ');
            switch (kind) {
                case FUZZ_TWO_BLOCKS -> {
                    message = qwpMessage(tableBlock(table, v, ts), tableBlock(table, v + 1, ts + 1));
                    values.add(v);
                    values.add(v + 1);
                }
                case FUZZ_MALFORMED -> {
                    message = Arrays.copyOf(oneRowMessage(table, v, ts), QwpConstants.HEADER_SIZE + 8);
                    errorStatus = QwpConstants.STATUS_PARSE_ERROR;
                }
                case FUZZ_SCHEMA_MISMATCH -> {
                    message = oneRowVarcharMessage(table, "x", ts);
                    errorStatus = QwpConstants.STATUS_SCHEMA_MISMATCH;
                }
                case FUZZ_DEFERRED -> {
                    message = deferred(oneRowMessage(table, v, ts));
                    values.add(v);
                }
                case FUZZ_ROWLESS_DEFERRED -> message = rowlessDeferredMessage();
                case FUZZ_PING -> {
                    // a control frame, no sequence
                    wireFrames.add(createMaskedFrame(WebSocketOpcode.PING, new byte[0]));
                    continue;
                }
                default -> {
                    message = oneRowMessage(table, v, ts);
                    values.add(v);
                }
            }
            wireFrames.add(createMaskedFrame(WebSocketOpcode.BINARY, message));
            frameValues.add(values);
            frameErrorStatus.add(errorStatus);
            seq++;
        }
        final int lastSeq = seq - 1;
        final byte[][] frames = toArray(wireFrames);

        final HttpFullFatServerConfiguration httpConfig = httpConfigWithMaxUncommittedRows(cap);
        final ObjList<byte[]> sent = new ObjList<>();
        try {
            ingestInOnePass(hookEngine, new QwpIngressUpgradeProcessor(hookEngine, httpConfig), httpConfig,
                    securityContext, isDurable, sent, frames);
        } catch (ServerDisconnectException ignore) {
            // a role-change CLOSE tells the dispatcher to drop the fd
        }
        drainWalQueue(hookEngine);

        final String context = "iteration=" + iteration + ", frames=" + frameCount + ", dataErrorsOnly=" + isDataErrorsOnly
                + ", durable=" + isDurable + ", cap=" + cap + ", readOnlyFrom=" + readOnlyFromCall.get()
                + ", readOnlyTransient=" + isReadOnlyTransient.get() + ", refusedInsert=" + refusedInsertCall
                + ", refusalPersistent=" + isRefusalPersistent + ", kinds=" + kinds;
        final long acked = maxCumulativeOkAck(sent);
        final LongList committed = new LongList();
        for (String table : tables) {
            try (
                    TableReader reader = hookEngine.getReader(table);
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                while (cursor.hasNext()) {
                    committed.add(cursor.getRecord().getLong(0));
                }
            }
        }
        committed.sort();
        for (int i = 0, n = committed.size(); i < n; i++) {
            final long value = committed.getQuick(i);
            Assert.assertTrue("I2: a row committed twice [" + context + ", value=" + value + "]",
                    i == 0 || committed.getQuick(i - 1) != value);
            Assert.assertTrue("I2: a row of a frame above the ack is committed [" + context + ", value=" + value
                    + ", acked=" + acked + "]", value / 10 <= acked);
        }
        for (int s = 0; s <= acked; s++) {
            final LongList values = frameValues.getQuick(s);
            for (int i = 0, n = values.size(); i < n; i++) {
                Assert.assertTrue("I1: an acked frame's row is not committed [" + context + ", seq=" + s + "]",
                        committed.binarySearch(values.getQuick(i), Vect.BIN_SEARCH_SCAN_UP) >= 0);
            }
        }
        if (isDataErrorsOnly) {
            int firstError = -1;
            for (int s = 0; s <= lastSeq && firstError < 0; s++) {
                if (frameErrorStatus.getQuick(s) >= 0) {
                    firstError = s;
                }
            }
            Assert.assertEquals("I3: frames before the first bad frame are acked [" + context + "]",
                    firstError < 0 ? lastSeq : firstError - 1, acked);
            for (int s = 0; s < (firstError < 0 ? lastSeq + 1 : firstError); s++) {
                Assert.assertFalse("I3: a good frame before the bad one is NACKed [" + context + ", seq=" + s + "]",
                        hasErrorResponseForSeq(sent, s));
            }
            if (firstError >= 0) {
                Assert.assertEquals("I3: the bad frame gets its own status [" + context + "]",
                        frameErrorStatus.getQuick(firstError), errorStatusForSeq(sent, firstError));
            }
        }
    }

    /**
     * One server connection fed a scripted client byte stream, frame by frame, over a
     * socket whose half-close succeeds, as a real socket's does: a server CLOSE leaves
     * the connection draining or awaiting the client's echo instead of disconnecting at
     * once.
     */
    private static final class Connection implements QuietCloseable {
        private final TestableContext context;
        private final byte[][] frames;
        private final HalfClosingNetworkFacade nf;
        private final QwpIngressUpgradeProcessor processor;
        private final RecordingRawSocket rawSocket;
        private final long recvBuf;
        private final long sendBuf;
        private final QwpIngressProcessorState state;
        private int nextFrame;

        Connection(
                CairoEngine engine,
                HttpFullFatServerConfiguration httpConfig,
                SecurityContext securityContext,
                boolean isDurableAckEnabled,
                byte[]... frames
        ) throws Exception {
            this.frames = frames;
            this.processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            this.nf = new HalfClosingNetworkFacade(concat(frames));
            this.recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            this.sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            this.rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
            this.context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE);
            this.state = new QwpIngressProcessorState(
                    RECV_BUFFER_SIZE,
                    httpConfig.getSendBufferSize(),
                    engine,
                    httpConfig.getLineHttpProcessorConfiguration()
            );
            state.of(-1, securityContext);
            state.setDurableAckEnabled(isDurableAckEnabled);
            getLV().set(context, state);
        }

        @Override
        public void close() {
            context.close();
            Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        }

        /**
         * Delivers the client's next {@code count} frames in one receive pass. Returns
         * true when the server tore the connection down.
         */
        boolean deliverFrames(int count) throws Exception {
            int bytes = 0;
            for (int i = 0; i < count; i++) {
                bytes += frames[nextFrame++].length;
            }
            try {
                drive(processor, context, nf, bytes);
                return false;
            } catch (ServerDisconnectException e) {
                return true;
            }
        }

        QwpIngressProcessorState getState() {
            return state;
        }

        ObjList<byte[]> sentFrames() {
            return rawSocket.sentFrames;
        }
    }

    /**
     * An engine the test demotes to read-only. {@link #demoteAtNextCommit()} arms the
     * demote, and it lands when the next commit takes the role-switch read lock. That
     * commit's in-lock read-only re-check then refuses it -- the race
     * {@code TableUpdateDetails.commit} guards against -- while every frame prepared
     * before it passed the live read-only gate. Durable uploads never catch up, so
     * committed work stays pending a durable ack.
     */
    private static final class DemotableEngine extends CairoEngine {
        private final AtomicBoolean isDemoteArmed = new AtomicBoolean();
        private final AtomicBoolean isReadOnly = new AtomicBoolean();
        private final DurableAckRegistry laggingRegistry = new DurableAckRegistry() {
            @Override
            public long getLocalDurableSeqTxn(CharSequence tableDirName) {
                return -1L;
            }

            @Override
            public long getReplicatedDurableSeqTxn(CharSequence tableDirName) {
                return -1L;
            }

            @Override
            public boolean isEnabled() {
                return true;
            }

            @Override
            public boolean isTierSetAvailable(int tiers) {
                return true;
            }

            @Override
            public void onTableDropped(TableToken tableToken) {
            }
        };
        private Lock roleSwitchReadLock;

        DemotableEngine() {
            super(new DefaultTestCairoConfiguration(root));
        }

        @Override
        public @NotNull DurableAckRegistry getDurableAckRegistry() {
            return laggingRegistry;
        }

        @Override
        public Lock getRoleSwitchReadLock() {
            if (roleSwitchReadLock == null) {
                roleSwitchReadLock = new DemotingLock(super.getRoleSwitchReadLock());
            }
            return roleSwitchReadLock;
        }

        @Override
        public boolean isReadOnlyMode() {
            return isReadOnly.get();
        }

        void demoteAtNextCommit() {
            isDemoteArmed.set(true);
        }

        private final class DemotingLock implements Lock {
            private final Lock delegate;

            DemotingLock(Lock delegate) {
                this.delegate = delegate;
            }

            @Override
            public void lock() {
                delegate.lock();
                onAcquired();
            }

            @Override
            public void lockInterruptibly() throws InterruptedException {
                delegate.lockInterruptibly();
                onAcquired();
            }

            @Override
            public @NotNull Condition newCondition() {
                return delegate.newCondition();
            }

            @Override
            public boolean tryLock() {
                if (delegate.tryLock()) {
                    onAcquired();
                    return true;
                }
                return false;
            }

            @Override
            public boolean tryLock(long time, @NotNull TimeUnit unit) throws InterruptedException {
                if (delegate.tryLock(time, unit)) {
                    onAcquired();
                    return true;
                }
                return false;
            }

            @Override
            public void unlock() {
                delegate.unlock();
            }

            private void onAcquired() {
                if (isDemoteArmed.compareAndSet(true, false)) {
                    isReadOnly.set(true);
                }
            }
        }
    }

    /**
     * A facade whose half-close succeeds, as a real socket's does, so a fatal CLOSE
     * leaves the connection draining instead of disconnecting at once.
     */
    private static class HalfClosingNetworkFacade extends PhasedNetworkFacade {
        HalfClosingNetworkFacade(byte[] data) {
            super(data);
        }

        @Override
        public int shutdown(long fd, int how) {
            return 0;
        }
    }
}
