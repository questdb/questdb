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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.wal.DurableAckRegistry;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.cutlass.qwp.server.QwpIngressUpgradeProcessor;
import io.questdb.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.griffin.SqlException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.std.TestFilesFacadeImpl;
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
    private static final int FUZZ_BAD_SECOND_BLOCK = 7;
    // The schemas a fuzz frame's block writes to its table: v as a LONG, v as a VARCHAR,
    // and, from FUZZ_BLOCK_NEW_COLUMN on, v and a column new to the table, one per frame.
    private static final int FUZZ_BLOCK_NEW_COLUMN = 2;
    private static final int FUZZ_BLOCK_V_LONG = 0;
    private static final int FUZZ_BLOCK_V_VARCHAR = 1;
    private static final int FUZZ_DATA_KINDS = 9;
    private static final int FUZZ_DEFERRED = 9;
    private static final int FUZZ_KINDS = 12;
    private static final int FUZZ_MALFORMED = 5;
    private static final int FUZZ_NEW_COLUMN_SECOND_BLOCK = 8;
    // Seeds of testCommitBatchFuzzPinnedRewindSeed: four frames of fb, then a frame that
    // appends to fa and then cannot join on its second block, of fa.
    private static final long FUZZ_PINNED_SEED0 = 641_439L;
    private static final long FUZZ_PINNED_SEED1 = 8_483_050L;
    private static final int FUZZ_PING = 11;
    private static final int FUZZ_PLAIN = 0;
    private static final int FUZZ_ROWLESS_DEFERRED = 10;
    private static final int FUZZ_SCHEMA_MISMATCH = 6;
    // Seeds of testCommitBatchFuzzGeneratesRewindShapes.
    private static final long FUZZ_SHAPES_SEED0 = 253_408L;
    private static final long FUZZ_SHAPES_SEED1 = 3_351_329L;
    private static final int FUZZ_TWO_BLOCKS = 4;
    // The index of fuzz table fc, which takes v as a VARCHAR of digits in every frame.
    private static final int FUZZ_VARCHAR_TABLE = 2;
    /**
     * A batch of three tables takes the role-switch read lock once for all its commits, and
     * no table's commit takes it again: what each batch pays for the rare role switch.
     */
    @Test
    public void testBatchCommitTakesRoleSwitchLockOnce() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE c (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final AtomicInteger lockCalls = new AtomicInteger();
            try (CairoEngine countingEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                private Lock readLock;

                @Override
                public Lock getRoleSwitchReadLock() {
                    if (readLock == null) {
                        final Lock delegate = super.getRoleSwitchReadLock();
                        readLock = new Lock() {
                            @Override
                            public void lock() {
                                lockCalls.incrementAndGet();
                                delegate.lock();
                            }

                            @Override
                            public void lockInterruptibly() throws InterruptedException {
                                lockCalls.incrementAndGet();
                                delegate.lockInterruptibly();
                            }

                            @Override
                            public @NotNull Condition newCondition() {
                                return delegate.newCondition();
                            }

                            @Override
                            public boolean tryLock() {
                                lockCalls.incrementAndGet();
                                return delegate.tryLock();
                            }

                            @Override
                            public boolean tryLock(long time, @NotNull TimeUnit unit) throws InterruptedException {
                                lockCalls.incrementAndGet();
                                return delegate.tryLock(time, unit);
                            }

                            @Override
                            public void unlock() {
                                delegate.unlock();
                            }
                        };
                    }
                    return readLock;
                }
            }) {
                final ObjList<byte[]> sent;
                try (Connection connection = new Connection(
                        countingEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("c", 300L, 3_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 400L, 4_000_000L))
                )) {
                    connection.deliverFrames(4);
                    sent = connection.sentFrames();
                    Assert.assertEquals("one acquisition commits the three tables", 1, lockCalls.get());
                }
                Assert.assertEquals("one ack covers the pass", 3, maxCumulativeOkAck(sent));
                drainWalQueue(countingEngine);
                assertHookQuery(countingEngine, "SELECT v FROM a").expectSize().returns("v\n100\n400\n");
                assertHookQuery(countingEngine, "SELECT v FROM b").expectSize().returns("v\n200\n");
                assertHookQuery(countingEngine, "SELECT v FROM c").expectSize().returns("v\n300\n");
            }
        });
    }

    // assertQuery's output for column v of the first ten frames of oneRowFramesThen().
    private static final String V_100_TO_109 = """
            v
            100
            101
            102
            103
            104
            105
            106
            107
            108
            109
            """;

    @Test
    public void testBatchCommitsBeforeReachingCap() throws Exception {
        assertBatchCommitsBeforeReachingCap(false);
    }

    @Test
    public void testBatchCommitsEachTableBeforeFrameFailingAfterAppend() throws Exception {
        assertBatchCommitsBeforeFrameFailingAfterAppend(false);
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
     * As {@link #testCommitFailureOnLaterTableAcksFramesBeforeItsFirstFrame}, but the batch
     * commits at a frame that cannot join it, the fourth, which needs a column a lacks,
     * instead of at the end of the receive pass. b's commit fails after a's succeeded:
     * the ack covers frame 0, the only frame before b's first, the NACK names frame 1,
     * where the client's replay starts, and the fourth frame is not processed.
     */
    @Test
    public void testBoundaryCommitFailureOnLaterTableNacksItsFirstFrame() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestInOnePass(
                    processor,
                    httpConfig,
                    failCommitAfterJoin("b"),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(tableBlockWithExtraColumn("a", 400L, 7L, 4_000_000L)))
            );

            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n300\n");
            assertQuery("SELECT count() FROM b").noLeakCheck().inferRandomAccess().sizeMayVary().returns("count\n0\n");
            Assert.assertEquals("the ack stops before the failed table's first frame", 0, maxCumulativeOkAck(sent));
            Assert.assertTrue("the failed commit must be NACKed at the failed table's first frame", hasErrorResponseForSeq(sent, 1));
            Assert.assertFalse("no NACK may name a frame the ack covers", hasErrorResponseForSeq(sent, 0));
            Assert.assertFalse(hasErrorResponseForSeq(sent, 2));
            Assert.assertFalse("the frame that cannot join is not processed", hasErrorResponseForSeq(sent, 3));
            Assert.assertTrue("the ack must precede the NACK",
                    indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 0) < indexOfErrorResponse(sent, 1));
        });
    }

    // QuestDB table names are case-insensitive, but a client may buffer each spelling apart
    // and send two blocks of one table, with different columns, in every frame.
    @Test
    public void testCaseVariantBlocksWithDifferentColumnsJoinCommitBatch() throws Exception {
        assertBlocksWithDifferentColumnsJoinCommitBatch("WEATHER");
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
     * Random frame sequences of three tables through the real handler in one receive
     * pass, with random faults: refused INSERTs, read-only flips (transient or
     * persistent), writer acquisitions that fail (transient or persistent, with a
     * CairoException or an unexpected error), a small max-uncommitted-rows cap, durable
     * acks, deferred groups, malformed and schema-mismatched frames, frames of two tables,
     * frames whose second block fails after the first appended or needs a new column, and
     * PINGs. Table fc takes v as a VARCHAR of digits, so its bad value fails in fc's own
     * schema. Every iteration checks the ack contract against the tables' contents, with A
     * the highest cumulative OK ack:
     * <ul>
     * <li>I1: every row of every frame at or below A is committed;</li>
     * <li>I2: no row is committed twice, and no row of a frame above A is committed
     * (the client replays from A + 1) -- unless a commit of several tables, a batch's
     * or a multi-table frame's, failed on one table after others committed. Then the
     * client gets a NACK above A or a CLOSE, the first frame with rows above A is the
     * failed table's first frame, and no row of that table from that frame on is
     * committed; rows of tables that committed may be, and the replay sends them
     * again.</li>
     * </ul>
     * Iterations whose only faults are the frames' own data errors also check I3:
     * the frames before the first bad frame are committed and acked, and the bad
     * frame is NACKed with its own status, as one commit per frame would leave it. At the
     * cap of 1,000,000 they check I4 too: before the first bad frame, a batch ends only at a
     * frame with a block that switches its table's schema, so S such frames make at most
     * S + 1 batches, each committing a table t at most once. A switching frame may commit
     * on its own, and each new column is a commit of its own, so t commits at most
     * S + 1 + S_t + A_t times, S_t being the switching frames with a block of t, and A_t the
     * frames that add a column to t.
     */
    @Test
    public void testCommitBatchAckContractFuzz() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final ObjList<FuzzIteration> iterations = new ObjList<>();
            for (int iteration = 0; iteration < 150; iteration++) {
                iterations.add(generateCommitBatchFuzzIteration(rnd, iteration));
            }
            runCommitBatchFuzz(iterations);
        });
    }

    /**
     * The generator of {@link #testCommitBatchAckContractFuzz}, from fixed seeds, produces
     * every shape that ends a batch at a failed frame: a first block that fails in a table
     * already in the batch and in one new to it, a second block that fails after the first
     * appended, a second block that needs a new column on a table the first block put in
     * the batch, writer acquisitions that fail with a CairoException and unexpectedly in a
     * pass that holds a frame whose second block takes its first block's rows back out,
     * and the shape of {@link #testCommitBatchFuzzPinnedRewindSeed}.
     */
    @Test
    public void testCommitBatchFuzzGeneratesRewindShapes() {
        final Rnd rnd = new Rnd(FUZZ_SHAPES_SEED0, FUZZ_SHAPES_SEED1);
        int batchTableFirstBlockFailures = 0;
        int newTableFirstBlockFailures = 0;
        int badSecondBlocks = 0;
        int newColumnSecondBlocks = 0;
        int cairoWriterFaults = 0;
        int unexpectedWriterFaults = 0;
        int pinnedRewindShapes = 0;
        for (int iteration = 0; iteration < 150; iteration++) {
            final FuzzIteration it = generateCommitBatchFuzzIteration(rnd, iteration);
            batchTableFirstBlockFailures += it.hasBatchTableFirstBlockFailure() ? 1 : 0;
            newTableFirstBlockFailures += it.hasNewTableFirstBlockFailure() ? 1 : 0;
            badSecondBlocks += it.hasBadSecondBlockAfterAppend() ? 1 : 0;
            newColumnSecondBlocks += it.hasNewColumnSecondBlockOnBatchTable() ? 1 : 0;
            cairoWriterFaults += it.hasWriterFaultWithRewindTrigger(false) ? 1 : 0;
            unexpectedWriterFaults += it.hasWriterFaultWithRewindTrigger(true) ? 1 : 0;
            pinnedRewindShapes += it.hasPinnedRewindShape() ? 1 : 0;
        }
        Assert.assertTrue("a batch table's first block fails", batchTableFirstBlockFailures > 0);
        Assert.assertTrue("a new table's first block fails", newTableFirstBlockFailures > 0);
        Assert.assertTrue("a second block fails after the first appended", badSecondBlocks > 0);
        Assert.assertTrue("a second block needs a new column on a batch table", newColumnSecondBlocks > 0);
        Assert.assertTrue("a writer acquisition fails with a CairoException in a pass with a rewind trigger", cairoWriterFaults > 0);
        Assert.assertTrue("a writer acquisition fails unexpectedly in a pass with a rewind trigger", unexpectedWriterFaults > 0);
        Assert.assertTrue("the pinned rewind shape", pinnedRewindShapes > 0);
    }

    /**
     * An iteration of {@link #testCommitBatchAckContractFuzz} from a fixed seed whose only
     * faults are data errors, at the cap of 1,000,000: its first bad frame fails on its
     * second block after the first appended, behind more frames of one table than I4 lets
     * the table commit. A rewind that leaves the first block's row in its writer breaks
     * I2, and committing the batch's frames one by one breaks I4.
     */
    @Test
    public void testCommitBatchFuzzPinnedRewindSeed() throws Exception {
        assertMemoryLeak(() -> {
            final FuzzIteration it = generateCommitBatchFuzzIteration(new Rnd(FUZZ_PINNED_SEED0, FUZZ_PINNED_SEED1), 0);
            Assert.assertTrue("test setup: the seed must generate the pinned rewind shape [" + it + ']', it.hasPinnedRewindShape());
            final ObjList<FuzzIteration> iterations = new ObjList<>();
            iterations.add(it);
            runCommitBatchFuzz(iterations);
        });
    }

    /**
     * A commit batch holds frames of any tables, interleaved in any order: each table
     * commits once, and one ack covers every frame.
     */
    @Test
    public void testCommitBatchSpansInterleavedTables() throws Exception {
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
            Assert.assertEquals("b's frame between a's does not split a's commit",
                    1, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a")) - aBefore);
            Assert.assertEquals(1, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b")) - bBefore);
            Assert.assertEquals(2, maxCumulativeOkAck(sent));
            Assert.assertEquals("one ack after the batch commit", 1, countOkAcks(sent));
        });
    }

    @Test
    public void testCommitFailureAtFirstOfThreeTablesAcksNothing() throws Exception {
        assertCommitFailureAtTable(0, "", "", "");
    }

    @Test
    public void testCommitFailureAtLastOfThreeTablesAcksFramesBeforeItsFirstFrame() throws Exception {
        assertCommitFailureAtTable(2, "100\n400\n", "200\n500\n", "");
    }

    @Test
    public void testCommitFailureAtMiddleOfThreeTablesAcksFramesBeforeItsFirstFrame() throws Exception {
        assertCommitFailureAtTable(1, "100\n400\n", "", "");
    }

    /**
     * The batch's tables commit one at a time in the order of their first frames, and
     * a failure stops the commits. Here b's commit fails after a's succeeded, as a
     * storage fault would: the ack covers frame 0, the only frame before b's first, and
     * frame 1 is NACKed, so the client replays from it. Frame 2's row committed with a,
     * and the replay sends it a second time: a partial batch failure duplicates rather
     * than loses.
     */
    @Test
    public void testCommitFailureOnLaterTableAcksFramesBeforeItsFirstFrame() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestInOnePass(
                    processor,
                    httpConfig,
                    failCommitAfterJoin("b"),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L))
            );

            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n300\n");
            assertQuery("SELECT count() FROM b").noLeakCheck().inferRandomAccess().sizeMayVary().returns("count\n0\n");
            Assert.assertEquals("the ack stops before the failed table's first frame", 0, maxCumulativeOkAck(sent));
            Assert.assertTrue("the failed commit must be NACKed at the failed table's first frame", hasErrorResponseForSeq(sent, 1));
            Assert.assertFalse(hasErrorResponseForSeq(sent, 0));
            Assert.assertFalse(hasErrorResponseForSeq(sent, 2));
            Assert.assertTrue("the ack must precede the NACK",
                    indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 0) < indexOfErrorResponse(sent, 1));
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

    /**
     * A demote lands as a batch of two tables takes the role-switch lock to commit. The
     * batch reads the node's role once under the lock, before any table commits, so
     * neither table commits, no ack goes out, and the role-change CLOSE leaves the client
     * to replay the whole batch, without a row of a arriving twice. The tables' own
     * commits run under the batch's hold and take the lock no more.
     */
    @Test
    public void testDemoteAtBatchCommitCommitsNoTable() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (DemotableEngine demotableEngine = new DemotableEngine()) {
                demotableEngine.demoteAtNextCommit();
                try (Connection connection = new Connection(
                        demotableEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L))
                )) {
                    connection.deliverFrames(3);
                    Assert.assertTrue("test setup: the demote must land at the batch commit", demotableEngine.isReadOnlyMode());
                    final ObjList<byte[]> sent = connection.sentFrames();
                    Assert.assertEquals("only the CLOSE", 1, sent.size());
                    Assert.assertEquals(-1, maxCumulativeOkAck(sent));
                    Assert.assertEquals(0, indexOfCloseFrame(sent));
                    Assert.assertEquals(WebSocketCloseCode.NORMAL_CLOSURE, closeCode(sent.getQuick(0)));
                }
                drainWalQueue(demotableEngine);
                try (TableReader a = demotableEngine.getReader("a"); TableReader b = demotableEngine.getReader("b")) {
                    Assert.assertEquals("a commits nothing", 0, a.size());
                    Assert.assertEquals("b commits nothing", 0, b.size());
                }
            }
        });
    }

    /**
     * A frame appends to a and then fails on b, and a demote refuses the batch commit owed
     * before the frame's answer: the server sends its role-change CLOSE and nothing else,
     * commits nothing, and the client replays the whole batch.
     */
    @Test
    public void testDemoteAtBoundaryCommitAfterRewindCommitsNothing() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (DemotableEngine demotableEngine = new DemotableEngine()) {
                demotableEngine.demoteAtNextCommit();
                try (Connection connection = new Connection(
                        demotableEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                                tableBlock("a", 300L, 3_000_000L),
                                varcharTableBlock("b", "not-a-long", 3_000_000L)
                        ))
                )) {
                    connection.deliverFrames(3);
                    Assert.assertTrue("test setup: the demote must land at the batch commit", demotableEngine.isReadOnlyMode());
                    assertOnlyRoleChangeCloseSent(connection.sentFrames());
                }
                drainWalQueue(demotableEngine);
                try (TableReader a = demotableEngine.getReader("a"); TableReader b = demotableEngine.getReader("b")) {
                    Assert.assertEquals(0, a.size() + b.size());
                }
            }
        });
    }

    /**
     * A demote requested as the batch takes the role-switch lock waits for the lock, as a
     * role switch's write lock does. The batch hands the lock over after a's commit, the
     * demote lands, and the batch reads the role again before b's: b is refused, the ack
     * covers the frame before b's first, and the role-change CLOSE follows it, with no
     * error response. The demote waits for one table's commit, not the whole batch's;
     * the client's replay from b's first frame sends a's later row again.
     */
    @Test
    public void testDemoteRequestedDuringBatchCommitLandsBetweenTables() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (DemotableEngine demotableEngine = new DemotableEngine()) {
                // The batch commit's hold is the pass's only acquisition of the lock.
                demotableEngine.requestDemoteAtCommit(1);
                final TableToken aToken = demotableEngine.verifyTableName("a");
                final TableToken bToken = demotableEngine.verifyTableName("b");
                final long aTxnBefore = demotableEngine.getTableSequencerAPI().lastTxn(aToken);
                final long bTxnBefore = demotableEngine.getTableSequencerAPI().lastTxn(bToken);
                try (Connection connection = new Connection(
                        demotableEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L))
                )) {
                    connection.deliverFrames(3);
                    Assert.assertTrue("test setup: the demote must land within the batch commit", demotableEngine.isReadOnlyMode());
                    final ObjList<byte[]> sent = connection.sentFrames();
                    Assert.assertEquals("the ack stops before b's first frame", 0, maxCumulativeOkAck(sent));
                    final int closeIndex = indexOfCloseFrame(sent);
                    Assert.assertTrue("the demote must close the connection", closeIndex >= 0);
                    Assert.assertEquals(WebSocketCloseCode.NORMAL_CLOSURE, closeCode(sent.getQuick(closeIndex)));
                    Assert.assertTrue("the ack must precede the CLOSE", indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 0) < closeIndex);
                    for (int seq = 0; seq < 3; seq++) {
                        Assert.assertFalse("a role-change refusal sends no error response", hasErrorResponseForSeq(sent, seq));
                    }
                }
                Assert.assertEquals("a commits before the demote", aTxnBefore + 1,
                        demotableEngine.getTableSequencerAPI().lastTxn(aToken));
                Assert.assertEquals("b, after the demote, commits nothing", bTxnBefore,
                        demotableEngine.getTableSequencerAPI().lastTxn(bToken));
                drainWalQueue(demotableEngine);
                assertHookQuery(demotableEngine, "SELECT v FROM a").expectSize().returns("v\n100\n300\n");
                try (TableReader b = demotableEngine.getReader("b")) {
                    Assert.assertEquals(0, b.size());
                }
            }
        });
    }

    /**
     * Another connection drops a table while the batch holds rows of it. The table joined
     * before the drop, so its commit fails after the tables before it committed: a
     * commits, c, after b, does not, the ack covers the frame before b's first, and the
     * NACK names b's first frame. The client's replay from it sends a's later row again,
     * as after a storage fault.
     */
    @Test
    public void testDroppedTableInBatchCommitsTablesBeforeIt() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE c (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final AtomicReference<QwpIngressProcessorState> stateRef = new AtomicReference<>();
            final AtomicBoolean isDropped = new AtomicBoolean();
            try (CairoEngine hookEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                @Override
                public boolean isReadOnlyMode() {
                    // Each frame's preparation checks the gate: drop b once all three tables
                    // joined. This engine shares the root with the test's, which holds the
                    // table registry, so it drops b where a commit sees it: in b's sequencer.
                    final QwpIngressProcessorState state = stateRef.get();
                    if (state != null && !isDropped.get() && state.getCommitBatchTableCount() == 3) {
                        isDropped.set(true);
                        getTableSequencerAPI().dropTable(verifyTableName("b"), false);
                    }
                    return false;
                }
            }) {
                final long aBefore = hookEngine.getTableSequencerAPI().lastTxn(hookEngine.verifyTableName("a"));
                final long cBefore = hookEngine.getTableSequencerAPI().lastTxn(hookEngine.verifyTableName("c"));
                final ObjList<byte[]> sent;
                try (Connection connection = new Connection(
                        hookEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("c", 300L, 3_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 400L, 4_000_000L))
                )) {
                    stateRef.set(connection.getState());
                    connection.deliverFrames(4);
                    sent = connection.sentFrames();
                }
                Assert.assertTrue("test setup: b must have been dropped mid-batch", isDropped.get());
                drainWalQueue(hookEngine);
                assertHookQuery(hookEngine, "SELECT v FROM a").expectSize().returns("v\n100\n400\n");
                try (TableReader c = hookEngine.getReader("c")) {
                    Assert.assertEquals("c, after b, commits nothing", 0, c.size());
                }
                Assert.assertEquals("a commits its batch rows once", aBefore + 1,
                        hookEngine.getTableSequencerAPI().lastTxn(hookEngine.verifyTableName("a")));
                Assert.assertEquals("c must not publish a WAL transaction", cBefore,
                        hookEngine.getTableSequencerAPI().lastTxn(hookEngine.verifyTableName("c")));
                Assert.assertEquals("the ack stops before b's first frame", 0, maxCumulativeOkAck(sent));
                Assert.assertFalse(hasErrorResponseForSeq(sent, 0));
                Assert.assertEquals("the NACK names b's first frame",
                        QwpConstants.STATUS_WRITE_ERROR, errorStatusForSeq(sent, 1));
                Assert.assertFalse(hasErrorResponseForSeq(sent, 2));
                Assert.assertFalse(hasErrorResponseForSeq(sent, 3));
                Assert.assertTrue("the ack must precede the NACK",
                        indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 0) < indexOfErrorResponse(sent, 1));
            }
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
    public void testDurableAckNamesBatchCommitsBeforeWriterAcquisitionFailure() throws Exception {
        assertWriterAcquisitionFailureAfterAppend(false, true);
    }

    @Test
    public void testDurableAckNamesEachCommitBeforeFrameFailingAfterAppend() throws Exception {
        assertBatchCommitsBeforeFrameFailingAfterAppend(true);
    }

    @Test
    public void testEmptyFrameJoinsCommitBatch() throws Exception {
        assertCommitsPerTable(1, 2,
                clientEncodedFrame(1, "a"), clientEncodedFrame(0), clientEncodedFrame(1, "a"));
    }

    /**
     * A batch of frames that name no table holds no table to commit: its commit takes no
     * lock and checks nothing, and the ack covers every frame.
     */
    @Test
    public void testEmptyFramesAloneAreAcked() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            final ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    clientEncodedFrame(0),
                    clientEncodedFrame(0),
                    clientEncodedFrame(0)
            );
            Assert.assertEquals(2, maxCumulativeOkAck(sent));
            Assert.assertEquals("one ack after the batch commit", 1, countOkAcks(sent));
            for (int seq = 0; seq < 3; seq++) {
                Assert.assertFalse(hasErrorResponseForSeq(sent, seq));
            }
        });
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
                    denyCommitAfterJoinTo("b"),
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
     * A frame's only block, of a table already in the batch, writes a VARCHAR, a STRING, a
     * BINARY and a DOUBLE[] value and then fails on a value that is not a number. The
     * cancel rewinds each column to the batch's rows, the batch commits once before the
     * frame's NACK, and a frame on a fresh connection appends at the rewound positions:
     * every good row reads back as sent.
     */
    @Test
    public void testFailedFirstBlockAppendRewindsEveryColumnType() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE t (x VARCHAR, s STRING, bin BINARY, arr DOUBLE[], v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long tBefore = lastTxn(engine, "t");
            final byte[][] frames = new byte[6][];
            for (int i = 0; i < 5; i++) {
                frames[i] = typedFrame(i, Integer.toString(100 + i));
            }
            frames[5] = typedFrame(5, "not-a-long");
            final ObjList<byte[]> sent = new ObjList<>();
            ingestInOnePass(
                    engine,
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    AllowAllSecurityContext.INSTANCE,
                    false,
                    64 * 1024,
                    sent,
                    frames
            );
            Assert.assertEquals("the batch commits once", tBefore + 1, lastTxn(engine, "t"));
            Assert.assertEquals(4, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, 5));

            final ObjList<byte[]> sentAfter = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    typedFrame(6, "106")
            );
            Assert.assertEquals(0, maxCumulativeOkAck(sentAfter));
            Assert.assertEquals(tBefore + 2, lastTxn(engine, "t"));
            drainWalQueue();
            assertQuery("SELECT x, s, base64(bin, 100) bin, arr, v, ts FROM t").noLeakCheck().expectSize().timestamp("ts").returns("""
                    x\ts\tbin\tarr\tv\tts
                    x0-\u00e9\u20ac\ts0\tAAECAA==\t[0.0,0.5]\t100\t1970-01-01T00:00:01.000000Z
                    x1-\u00e9\u20ac\ts1\tAQECAw==\t[1.0,1.5]\t101\t1970-01-01T00:00:02.000000Z
                    x2-\u00e9\u20ac\ts2\tAgECBg==\t[2.0,2.5]\t102\t1970-01-01T00:00:03.000000Z
                    x3-\u00e9\u20ac\ts3\tAwECCQ==\t[3.0,3.5]\t103\t1970-01-01T00:00:04.000000Z
                    x4-\u00e9\u20ac\ts4\tBAECDA==\t[4.0,4.5]\t104\t1970-01-01T00:00:05.000000Z
                    x6-\u00e9\u20ac\ts6\tBgECEg==\t[6.0,6.5]\t106\t1970-01-01T00:00:07.000000Z
                    """);
        });
    }

    /**
     * As {@link #testFailedFrameInCommitBatchKeepsEarlierFramesCommittedAndAcked},
     * with a two-block failing frame and a batch whose rows start past an earlier
     * commit in the same WAL segment. The frame joins the batch, appends 300 next to
     * the batch's 200 and fails on its second block: 300 goes back out of the writer, the
     * batch commits and acks 200, and the frame is NACKed, so 300 is discarded and nothing
     * the batch or the first pass committed.
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
     * The first frame of a receive pass appends to a and then fails on b, with no batch
     * pending: its row goes back out of a's writer, a leaves the batch the frame was
     * opening, and the frame is NACKed. Nothing is committed.
     */
    @Test
    public void testFailedFrameOpeningBatchCommitsNothing() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            final long bBefore = lastTxn(engine, "b");
            final ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                            tableBlock("a", 100L, 1_000_000L),
                            varcharTableBlock("b", "not-a-long", 1_000_001L)
                    ))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n");
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n");
            Assert.assertEquals(aBefore, lastTxn(engine, "a"));
            Assert.assertEquals(bBefore, lastTxn(engine, "b"));
            Assert.assertEquals(-1, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, 0));
        });
    }

    /**
     * The multi-table shape of
     * {@link #testFailedFrameAfterCommitInSegmentDropsOnlyItsOwnRows}: the
     * pinned client's flush packs every table with rows into one message, so a
     * frame can append a good block to the batch's table and then fail on
     * another table's block. The frame's good block must go back out of the batch's
     * table while the batch's own rows end up committed and acked.
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

    /**
     * A frame's first block brings n into the batch, and its second block, of n again,
     * fails on a value. n's writer goes back to the savepoint of no rows its first block
     * marked: n leaves the batch, the batch commits a once, and the frame gets its own
     * NACK. n takes rows on a fresh connection, without the failed frame's.
     */
    @Test
    public void testFailedSecondBlockOfNewTableRewindsItsFirstBlock() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE n (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            final long nBefore = lastTxn(engine, "n");
            final ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                            tableBlock("n", 300L, 3_000_000L),
                            varcharTableBlock("n", "not-a-long", 3_000_001L)
                    ))
            );
            Assert.assertEquals("the batch commits a once", aBefore + 1, lastTxn(engine, "a"));
            Assert.assertEquals("n commits nothing", nBefore, lastTxn(engine, "n"));
            Assert.assertEquals(1, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, 2));
            Assert.assertTrue("the ack must precede the NACK",
                    indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 1) < indexOfErrorResponse(sent, 2));

            final ObjList<byte[]> sentAfter = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("n", 400L, 4_000_000L))
            );
            Assert.assertEquals(0, maxCumulativeOkAck(sentAfter));
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n200\n");
            assertQuery("SELECT v FROM n").noLeakCheck().expectSize().returns("v\n400\n");
        });
    }

    /**
     * As {@link #testFailedFirstBlockAppendRewindsEveryColumnType}, but the failing frame's
     * first block, of the batch's table, appends a row with every column type, and its
     * second block fails on b's value. The appended row goes back out of t's writer through
     * the savepoint the frame marked, the batch commits once before the frame's NACK, and a
     * frame on a fresh connection appends at the rewound positions: every good row reads
     * back as sent.
     */
    @Test
    public void testFailedSecondBlockRewindsEveryColumnType() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE t (x VARCHAR, s STRING, bin BINARY, arr DOUBLE[], v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long tBefore = lastTxn(engine, "t");
            final long bBefore = lastTxn(engine, "b");
            final byte[][] frames = new byte[6][];
            for (int i = 0; i < 5; i++) {
                frames[i] = typedFrame(i, Integer.toString(100 + i));
            }
            frames[5] = typedFrameThenBadBlock(5, "105");
            final ObjList<byte[]> sent = new ObjList<>();
            ingestInOnePass(
                    engine,
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    AllowAllSecurityContext.INSTANCE,
                    false,
                    64 * 1024,
                    sent,
                    frames
            );
            Assert.assertEquals("the batch commits once", tBefore + 1, lastTxn(engine, "t"));
            Assert.assertEquals(bBefore, lastTxn(engine, "b"));
            Assert.assertEquals(4, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, 5));
            Assert.assertTrue("the ack must precede the NACK",
                    indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 4) < indexOfErrorResponse(sent, 5));

            final ObjList<byte[]> sentAfter = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    typedFrame(6, "106")
            );
            Assert.assertEquals(0, maxCumulativeOkAck(sentAfter));
            Assert.assertEquals(tBefore + 2, lastTxn(engine, "t"));
            drainWalQueue();
            assertQuery("SELECT x, s, base64(bin, 100) bin, arr, v, ts FROM t").noLeakCheck().expectSize().timestamp("ts").returns("""
                    x\ts\tbin\tarr\tv\tts
                    x0-\u00e9\u20ac\ts0\tAAECAA==\t[0.0,0.5]\t100\t1970-01-01T00:00:01.000000Z
                    x1-\u00e9\u20ac\ts1\tAQECAw==\t[1.0,1.5]\t101\t1970-01-01T00:00:02.000000Z
                    x2-\u00e9\u20ac\ts2\tAgECBg==\t[2.0,2.5]\t102\t1970-01-01T00:00:03.000000Z
                    x3-\u00e9\u20ac\ts3\tAwECCQ==\t[3.0,3.5]\t103\t1970-01-01T00:00:04.000000Z
                    x4-\u00e9\u20ac\ts4\tBAECDA==\t[4.0,4.5]\t104\t1970-01-01T00:00:05.000000Z
                    x6-\u00e9\u20ac\ts6\tBgECEg==\t[6.0,6.5]\t106\t1970-01-01T00:00:07.000000Z
                    """);
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n");
        });
    }

    @Test
    public void testFailedThirdBlockRewindsBatchTableBeforeNewTable() throws Exception {
        assertThirdBlockFailureRewindsBothTables(false);
    }

    @Test
    public void testFailedThirdBlockRewindsNewTableBeforeBatchTable() throws Exception {
        assertThirdBlockFailureRewindsBothTables(true);
    }

    /**
     * A frame's only block, of a table already in the batch, writes values onto the next
     * page of the column's file and then fails on a value that is not a number. To rewind
     * the column, the cancel maps its first page again, and that mapping fails: the writer
     * is left mid columnar write, where its batch rows cannot commit, and refuses to go
     * back to its savepoint. The table cache distresses, the batch is abandoned with
     * nothing committed and nothing acked, and the NACK names the frame, with
     * INTERNAL_ERROR: the client replays the batch, and a failure that recurs strikes this
     * frame. The table takes rows afterwards.
     */
    @Test
    public void testFirstBlockCancelFailureLosesBatch() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final int batchFrames = 10;
            // The mmap calls since the failing frame's preparation, or -1 before it.
            final AtomicInteger mmapsSinceArmed = new AtomicInteger(-1);
            final AtomicReference<WalWriter> batchWriter = new AtomicReference<>();
            final FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                    if (mmapsSinceArmed.get() > -1) {
                        // 1: v's values cross onto the column's second page; 2: the cancel maps
                        // the first page back. The failed cancel distresses the writer, so
                        // closing it does not cancel again.
                        if (mmapsSinceArmed.incrementAndGet() == 2) {
                            return FilesFacade.MAP_FAILED;
                        }
                    }
                    return super.mmap(fd, len, offset, flags, memoryTag);
                }
            };
            final AtomicInteger readOnlyChecks = new AtomicInteger();
            final AtomicReference<QwpIngressProcessorState> stateRef = new AtomicReference<>();
            final DefaultTestCairoConfiguration hookConfiguration = new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return ff;
                }

                @Override
                public long getWalDataAppendPageSize() {
                    return 16 * 1024;
                }
            };
            try (CairoEngine hookEngine = new CairoEngine(hookConfiguration) {
                @Override
                public boolean isReadOnlyMode() {
                    // Frame 0 checks at its preparation and before it opens the batch (1, 2),
                    // frames 1 to 9 at their preparation (3 to 11), and so does the failing frame.
                    if (readOnlyChecks.incrementAndGet() == batchFrames + 2) {
                        batchWriter.set(stateRef.get().getCommitBatchWriter(0));
                        mmapsSinceArmed.set(0);
                    }
                    return false;
                }
            }) {
                final long aBefore = lastTxn(hookEngine, "a");
                final byte[][] frames = new byte[batchFrames + 1][];
                for (int i = 0; i < batchFrames; i++) {
                    frames[i] = createMaskedFrame(
                            WebSocketOpcode.BINARY,
                            oneRowVarcharMessage("a", Long.toString(100L + i), (i + 1) * 1_000_000L)
                    );
                }
                // The batch holds 10 rows, 80 bytes of v. The failing frame's 2,049 rows reach
                // v's second 16 KiB page at its row 2,038, and its last value is not a number.
                try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer table = new QwpTableBuffer("a")) {
                    final QwpTableBuffer.ColumnBuffer value = table.getOrCreateColumn("v", QwpConstants.TYPE_VARCHAR, false);
                    final QwpTableBuffer.ColumnBuffer timestamp = table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP);
                    final int rows = 2_049;
                    for (int row = 0; row < rows; row++) {
                        value.addString(row < rows - 1 ? Long.toString(1_000L + row) : "not-a-long");
                        timestamp.addLong(11_000_000L + row);
                        table.nextRow();
                    }
                    frames[batchFrames] = encodedFrame(encoder, encoder.encode(table));
                }
                final ObjList<byte[]> sent;
                final QwpIngressProcessorState state = new QwpIngressProcessorState(
                        RECV_BUFFER_SIZE,
                        httpConfig.getSendBufferSize(),
                        hookEngine,
                        httpConfig.getLineHttpProcessorConfiguration()
                );
                try (Connection connection = new Connection(
                        hookEngine,
                        httpConfig,
                        state,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        64 * 1024,
                        frames
                )) {
                    stateRef.set(connection.getState());
                    connection.deliverFrames(frames.length);
                    sent = connection.sentFrames();
                }
                Assert.assertEquals("test setup: the cancel's mapping must fail, and the writer's close must not retry it",
                        2, mmapsSinceArmed.get());
                Assert.assertFalse("the failed cancel must distress a's writer, so the pool expels it",
                        batchWriter.get().isOpen());
                Assert.assertEquals("nothing commits", aBefore, lastTxn(hookEngine, "a"));
                Assert.assertEquals("nothing is acked", -1, maxCumulativeOkAck(sent));
                Assert.assertEquals(QwpConstants.STATUS_INTERNAL_ERROR, errorStatusForSeq(sent, batchFrames));
                TestUtils.assertContains(errorMessageForSeq(sent, batchFrames), "could not mmap");
                Assert.assertFalse("the batch's first frame is not NACKed", hasErrorResponseForSeq(sent, 0));

                final ObjList<byte[]> sentAfter = new ObjList<>();
                ingestInOnePass(
                        hookEngine,
                        new QwpIngressUpgradeProcessor(hookEngine, httpConfig),
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        sentAfter,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowVarcharMessage("a", "200", 20_000_000L))
                );
                Assert.assertEquals(0, maxCumulativeOkAck(sentAfter));
                drainWalQueue(hookEngine);
                assertHookQuery(hookEngine, "SELECT v FROM a").expectSize().returns("v\n200\n");
            }
        });
    }

    /**
     * With column auto-creation off, a frame's only block, of a table new to the batch,
     * names a column the table lacks, which the appender refuses before it writes a row.
     * The batch commits as it stands, once, before the frame's NACK.
     */
    @Test
    public void testFirstBlockColumnRefusalCommitsBatchOnce() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = httpConfigWithoutNewColumns();
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            final ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    oneRowFramesThen(
                            "a",
                            10,
                            createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(tableBlockWithExtraColumn("b", 7L, 8L, 11_000_000L)))
                    )
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns(V_100_TO_109);
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n");
            assertQuery("SELECT \"column\" FROM table_columns('b')").noLeakCheck().noRandomAccess().returns("column\nv\nts\n");
            Assert.assertEquals("the batch commits once", aBefore + 1, lastTxn(engine, "a"));
            Assert.assertEquals(9, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, 10));
            TestUtils.assertContains(errorMessageForSeq(sent, 10), "new columns not allowed");
            Assert.assertTrue("the ack must precede the NACK",
                    indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 9) < indexOfErrorResponse(sent, 10));
        });
    }

    /**
     * A frame's only block, of a table already in the batch, writes a symbol new to the
     * table and then fails on a value that is not a number. The symbol stays in the
     * writer's symbol table, and the batch commits once before the frame's NACK. A row
     * that names the symbol on a fresh connection reads it back.
     */
    @Test
    public void testFirstBlockFailureAfterNewSymbolKeepsBatchAndSymbol() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE s (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long sBefore = lastTxn(engine, "s");
            final GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            final byte[][] frames = new byte[6][];
            for (int i = 0; i < 5; i++) {
                frames[i] = symbolVarcharFrame(dict, i == 0 ? -1 : 0, "s", "x", Long.toString(100L + i), (i + 1) * 1_000_000L);
            }
            frames[5] = symbolVarcharFrame(dict, 0, "s", "z", "not-a-long", 6_000_000L);
            final ObjList<byte[]> sent = ingestInOnePass(new QwpIngressUpgradeProcessor(engine, httpConfig), httpConfig, frames);
            Assert.assertEquals("the batch commits once", sBefore + 1, lastTxn(engine, "s"));
            Assert.assertEquals(4, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, 5));

            final ObjList<byte[]> sentAfter = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    symbolVarcharFrame(new GlobalSymbolDictionary(), -1, "s", "z", "7", 7_000_000L)
            );
            Assert.assertEquals(0, maxCumulativeOkAck(sentAfter));
            drainWalQueue();
            assertQuery("SELECT s, v FROM s").noLeakCheck().expectSize().returns("""
                    s\tv
                    x\t100
                    x\t101
                    x\t102
                    x\t103
                    x\t104
                    z\t7
                    """);
        });
    }

    /**
     * A frame fails on its only block, of a table already in the batch, with the batch's
     * own schema: the writer parses each VARCHAR value into its LONG column and refuses
     * one that is not a number. The cancel of the failed block leaves the writer with its
     * batch rows, so the batch commits as it stands, once, before the frame's NACK.
     */
    @Test
    public void testFirstBlockFailureOfBatchTableCommitsBatchOnce() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            final byte[][] frames = new byte[11][];
            for (int i = 0; i < 10; i++) {
                frames[i] = createMaskedFrame(
                        WebSocketOpcode.BINARY,
                        oneRowVarcharMessage("a", Long.toString(100L + i), (i + 1) * 1_000_000L)
                );
            }
            frames[10] = createMaskedFrame(WebSocketOpcode.BINARY, oneRowVarcharMessage("a", "not-a-long", 11_000_000L));
            final ObjList<byte[]> sent = ingestInOnePass(new QwpIngressUpgradeProcessor(engine, httpConfig), httpConfig, frames);
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns(V_100_TO_109);
            Assert.assertEquals("the batch commits once", aBefore + 1, lastTxn(engine, "a"));
            Assert.assertEquals(9, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, 10));
            Assert.assertTrue("the ack must precede the NACK",
                    indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 9) < indexOfErrorResponse(sent, 10));
        });
    }

    /**
     * A frame fails on its only block, of a table new to the batch: b refuses the VARCHAR
     * value for its LONG column. Nothing of the frame reached a writer and the table cache
     * stayed healthy, so the batch commits as it stands, once, and is acked before the
     * frame's own NACK. Both tables take rows on a fresh connection afterwards.
     */
    @Test
    public void testFirstBlockFailureOfTableNewToBatchCommitsBatchOnce() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            final long bBefore = lastTxn(engine, "b");
            final ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    oneRowFramesThen("a", 10, createMaskedFrame(WebSocketOpcode.BINARY, oneRowVarcharMessage("b", "not-a-long", 11_000_000L)))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns(V_100_TO_109);
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n");
            Assert.assertEquals("the batch commits once", aBefore + 1, lastTxn(engine, "a"));
            Assert.assertEquals(bBefore, lastTxn(engine, "b"));
            Assert.assertEquals(9, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, 10));
            Assert.assertTrue("the ack must precede the NACK",
                    indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 9) < indexOfErrorResponse(sent, 10));

            final ObjList<byte[]> sentAfter = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 200L, 20_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 300L, 21_000_000L))
            );
            Assert.assertEquals(1, maxCumulativeOkAck(sentAfter));
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns(V_100_TO_109 + "200\n");
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n300\n");
        });
    }

    /**
     * The first frame of a receive pass fails on its only block, with no batch pending:
     * it is NACKed, and nothing is committed.
     */
    @Test
    public void testFirstBlockFailureOpeningBatch() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long bBefore = lastTxn(engine, "b");
            final ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowVarcharMessage("b", "not-a-long", 1_000_000L))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n");
            Assert.assertEquals(bBefore, lastTxn(engine, "b"));
            Assert.assertEquals(-1, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, 0));
        });
    }

    /**
     * A frame's only block, of a table new to the batch, needs a new column, and the
     * permission check for it fails unexpectedly. Nothing of the frame reached a writer,
     * but the failure leaves the connection's writers in doubt: the table cache distresses,
     * and a distressed cache commits nothing. The batch is abandoned, nothing is acked, and
     * the NACK names the frame, with INTERNAL_ERROR: the failure recurs on every replay of
     * the batch, and the client's poison detector must strike this frame, not the batch's
     * healthy first frame.
     */
    @Test
    public void testFirstBlockUnexpectedErrorLosesBatch() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final SecurityContext failingAddColumn = new AllowAllSecurityContext() {
                @Override
                public void authorizeAlterTableAddColumn(TableToken tableToken) {
                    throw new IllegalStateException("test: unexpected permission check failure");
                }
            };
            final long aBefore = lastTxn(engine, "a");
            final ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    failingAddColumn,
                    oneRowFramesThen(
                            "a",
                            10,
                            createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(tableBlockWithExtraColumn("b", 7L, 8L, 11_000_000L)))
                    )
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n");
            assertQuery("SELECT \"column\" FROM table_columns('b')").noLeakCheck().noRandomAccess().returns("column\nv\nts\n");
            Assert.assertEquals("nothing commits", aBefore, lastTxn(engine, "a"));
            Assert.assertEquals("nothing is acked", -1, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_INTERNAL_ERROR, errorStatusForSeq(sent, 10));
            TestUtils.assertContains(errorMessageForSeq(sent, 10), "unexpected error");
            Assert.assertFalse("the batch's first frame is not NACKed", hasErrorResponseForSeq(sent, 0));
        });
    }

    @Test
    public void testForeignMultiTableFrameJoinsCommitBatch() throws Exception {
        assertCommitsPerTable(3, 3,
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

    /**
     * A frame appends to a batch table, and its next block needs a column the table
     * lacks, which a joining block may not create. Nothing failed: the frame's first block
     * goes back out, the batch's five frames commit once, and the frame then goes the
     * normal way from its first block, which creates the column and commits.
     */
    @Test
    public void testFrameThatOnlyCouldNotJoinCommitsAfterBatch() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    oneRowFramesThen("a", 5, createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                            tableBlock("a", 105L, 6_000_000L),
                            tableBlockWithExtraColumn("a", 106L, 7L, 6_000_001L)
                    )))
            );
            drainWalQueue();
            assertQuery("SELECT v, w FROM a").noLeakCheck().expectSize().returns("""
                    v\tw
                    100\tnull
                    101\tnull
                    102\tnull
                    103\tnull
                    104\tnull
                    105\tnull
                    106\t7
                    """);
            Assert.assertEquals("the batch, the new column, then the frame", aBefore + 3, lastTxn(engine, "a"));
            Assert.assertEquals(5, maxCumulativeOkAck(sent));
            for (int seq = 0; seq < 6; seq++) {
                Assert.assertFalse(hasErrorResponseForSeq(sent, seq));
            }
        });
    }

    /**
     * A full receive pass: over 1 MiB of one-row frames that alternate between a and b,
     * then a frame that appends to a and fails on b. The frame's row of a goes back out
     * of a's writer, and the batch commits each table once.
     */
    @Test
    public void testFullReceivePassRewindCommitsEachTableOnce() throws Exception {
        // Without fsync, so that a regression to one commit per frame stays fast to detect.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            final long bBefore = lastTxn(engine, "b");
            // Frame i writes v = i, to a when i is even and to b when it is odd.
            final int goodFrames = 24_000;
            final byte[][] frames = new byte[goodFrames + 1][];
            int wireLength = 0;
            for (int i = 0; i < goodFrames; i++) {
                frames[i] = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(i % 2 == 0 ? "a" : "b", i, 1_000_000L + i * 1_000L));
                wireLength += frames[i].length;
            }
            frames[goodFrames] = createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                    tableBlock("a", goodFrames, 1_000_000L + goodFrames * 1_000L),
                    varcharTableBlock("b", "not-a-long", 1_000_000L + goodFrames * 1_000L)
            ));
            Assert.assertTrue("test setup: the frames must fill over 1 MiB", wireLength > 1024 * 1024);
            final ObjList<byte[]> sent = new ObjList<>();
            final QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ingestInOnePass(
                    engine,
                    processor,
                    httpConfig,
                    AllowAllSecurityContext.INSTANCE,
                    false,
                    2 * 1024 * 1024,
                    sent,
                    frames
            );
            Assert.assertEquals("a commits once", aBefore + 1, lastTxn(engine, "a"));
            Assert.assertEquals("b commits once", bBefore + 1, lastTxn(engine, "b"));
            Assert.assertEquals(goodFrames - 1, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, goodFrames));
            drainWalQueue();
            // a holds the even values 0 .. 23,998, whose sum is 2 * (11,999 * 12,000 / 2); b the
            // odd values 1 .. 23,999, whose sum is 12,000 squared.
            assertQuery("SELECT count(), sum(v), min(v), max(v) FROM a").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tsum\tmin\tmax\n12000\t143988000\t0\t23998\n");
            assertQuery("SELECT count(), sum(v), min(v), max(v) FROM b").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tsum\tmin\tmax\n12000\t144000000\t1\t23999\n");
        });
    }

    /**
     * The first frame of a receive pass passes the live read-only gate at its
     * preparation and meets a demote at the gate's second check, before it opens a
     * batch: it touches no writer, nothing commits, and the connection gets the
     * reconnect-eligible CLOSE without an error response.
     */
    @Test
    public void testGateRefusalBeforeOpeningBatchClosesWithoutCommit() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger readOnlyChecks = new AtomicInteger();
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (CairoEngine demotableEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                @Override
                public boolean isReadOnlyMode() {
                    // 1: the frame's preparation; 2: the check before it opens the batch.
                    return readOnlyChecks.incrementAndGet() >= 2;
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
                            createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L))
                    );
                } catch (ServerDisconnectException ignore) {
                    // the role-change CLOSE tells the dispatcher to drop the fd
                }
                Assert.assertTrue("the refusal must close the connection", indexOfCloseFrame(sent) >= 0);
                Assert.assertFalse("a role-change refusal sends no error response", hasErrorResponseForSeq(sent, 0));
                Assert.assertEquals(-1, maxCumulativeOkAck(sent));
                assertTableEmpty(demotableEngine);
            }
        });
    }

    /**
     * Each table keeps its own column resolution, so each must notice its own
     * metadata change: after b's column is renamed, b's next frames resolve again
     * (here: auto-create a new {@code v}) while a's keep theirs.
     */
    @Test
    public void testInterleavedTablesFollowColumnRenameBetweenPasses() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (Connection connection = new Connection(
                    engine,
                    httpConfig,
                    AllowAllSecurityContext.INSTANCE,
                    false,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 400L, 4_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 500L, 5_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 600L, 6_000_000L))
            )) {
                connection.deliverFrames(2);
                drainWalQueue();
                execute("ALTER TABLE b RENAME COLUMN v TO w");
                drainWalQueue();
                connection.deliverFrames(4);
                Assert.assertEquals(5, maxCumulativeOkAck(connection.sentFrames()));
            }
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n300\n500\n");
            assertQuery("SELECT w, v FROM b").noLeakCheck().expectSize().returns("""
                    w\tv
                    200\tnull
                    null\t400
                    null\t600
                    """);
        });
    }

    /**
     * A table dropped and created again with another column order between two receive
     * passes: its frames in the second pass resolve against the new table's columns,
     * not the old table's resolution.
     */
    @Test
    public void testInterleavedTablesFollowRecreatedTableBetweenPasses() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (Connection connection = new Connection(
                    engine,
                    httpConfig,
                    AllowAllSecurityContext.INSTANCE,
                    false,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 400L, 4_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 500L, 5_000_000L))
            )) {
                connection.deliverFrames(2);
                drainWalQueue();
                execute("DROP TABLE b");
                execute("CREATE TABLE b (x LONG, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                connection.deliverFrames(3);
                Assert.assertEquals(4, maxCumulativeOkAck(connection.sentFrames()));
            }
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n300\n500\n");
            assertQuery("SELECT x, v FROM b").noLeakCheck().expectSize().returns("x\tv\nnull\t400\n");
        });
    }

    /**
     * With a max-uncommitted-rows cap of 4, frames [a100], [b200] and [a300] form a batch,
     * then a frame appends a row to a and has three rows of b, which would reach the cap.
     * Its row of a goes back out of a's writer, the batch commits each table once, and the
     * frame then opens a new batch, which commits each table once more.
     */
    @Test
    public void testLaterBlockReachingCapRewindsBatchTable() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = httpConfigWithMaxUncommittedRows(4);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            final long bBefore = lastTxn(engine, "b");
            final ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                            tableBlock("a", 301L, 3_000_001L),
                            tableBlock("b", 401L, 4_000_001L, 402L, 4_000_002L, 403L, 4_000_003L)
                    ))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n300\n301\n");
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n200\n401\n402\n403\n");
            Assert.assertEquals("the batch, then the frame's own batch", aBefore + 2, lastTxn(engine, "a"));
            Assert.assertEquals("the batch, then the frame's own batch", bBefore + 2, lastTxn(engine, "b"));
            Assert.assertEquals(3, maxCumulativeOkAck(sent));
            for (int seq = 0; seq < 4; seq++) {
                Assert.assertFalse(hasErrorResponseForSeq(sent, seq));
            }
        });
    }

    @Test
    public void testMalformedFirstSchemaSalvagesPendingPrefix() throws Exception {
        assertMalformedPreparationSalvagesPrefix(false);
    }

    @Test
    public void testMalformedHeaderSalvagesPendingPrefix() throws Exception {
        assertMalformedPreparationSalvagesPrefix(true);
    }

    /**
     * A malformed WebSocket frame behind a batch of two tables drops the connection
     * without a CLOSE. No ack can reach the client, so the batch must not commit: the
     * client replays it.
     */
    @Test
    public void testMalformedWebSocketFrameRollsBackMultiTableBatch() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a"));
            final long bBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"));
            final ObjList<byte[]> sent = new ObjList<>();
            try {
                ingestInOnePass(
                        engine,
                        new QwpIngressUpgradeProcessor(engine, httpConfig),
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        sent,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                        // an unmasked client frame: the parser refuses it
                        new byte[]{(byte) (0x80 | WebSocketOpcode.BINARY), 0x01, 0x00}
                );
                Assert.fail("a malformed frame must drop the connection");
            } catch (ServerDisconnectException expected) {
                // the dispatcher drops the fd
            }
            Assert.assertEquals("no ack may reach the client", 0, sent.size());
            Assert.assertEquals(aBefore, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a")));
            Assert.assertEquals(bBefore, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b")));
        });
    }

    @Test
    public void testMissingTableAfterAppendRewindsFrame() throws Exception {
        assertMissingTableWithoutAutoCreate(true);
    }

    @Test
    public void testMissingTableEndsBatch() throws Exception {
        assertMissingTableWithoutAutoCreate(false);
    }

    @Test
    public void testMultiTableFrameStartingWithBatchTableJoinsCommitBatch() throws Exception {
        assertCommitsPerTable(2, 3,
                clientEncodedFrame(1, "a"), clientEncodedFrame(1, "a", "b"));
    }

    @Test
    public void testMultiTableFramesCommitOncePerTable() throws Exception {
        assertCommitsPerTable(3, 9,
                clientEncodedFrame(1, "a", "b"),
                clientEncodedFrame(1, "b", "a"),
                clientEncodedFrame(1, "c"),
                clientEncodedFrame(1, "a", "c", "b"),
                clientEncodedFrame(1, "b"));
    }

    /**
     * A frame's only block, of b, new to the batch, fails after crossing onto the next page
     * of b's column, and the cancel's mapping of the first page fails too. b has no batch
     * entry yet, so the batch commits a without b, the frame gets its own NACK, and the
     * frame's clear() frees b's writer. b takes rows on a fresh connection, without any of
     * the failed frame's.
     */
    @Test
    public void testNewTableCancelFailureCommitsBatchWithoutIt() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final int batchFrames = 10;
            // The mmap calls since b joined the failing frame, or -1 before.
            final AtomicInteger mmapsSinceArmed = new AtomicInteger(-1);
            final AtomicReference<WalWriter> bWriter = new AtomicReference<>();
            final FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                    if (mmapsSinceArmed.get() > -1) {
                        // 1: v's values cross onto the column's second page; 2: the cancel maps
                        // the first page back. The failed cancel distresses the writer, so
                        // freeing it does not cancel again.
                        if (mmapsSinceArmed.incrementAndGet() == 2) {
                            return FilesFacade.MAP_FAILED;
                        }
                    }
                    return super.mmap(fd, len, offset, flags, memoryTag);
                }
            };
            final AtomicBoolean isArmable = new AtomicBoolean();
            // b's insert permission is checked as b joins the failing frame, after its lookup.
            final SecurityContext armingContext = new AllowAllSecurityContext() {
                @Override
                public void authorizeInsert(TableToken tableToken) {
                    if (tableToken.getTableName().equals("b") && isArmable.compareAndSet(true, false)) {
                        mmapsSinceArmed.set(0);
                    }
                }
            };
            try (CairoEngine hookEngine = new CairoEngine(pagedConfiguration(ff)) {
                @Override
                public WalWriter getWalWriter(TableToken token) {
                    final WalWriter writer = super.getWalWriter(token);
                    if (token.getTableName().equals("b")) {
                        bWriter.set(writer);
                    }
                    return writer;
                }
            }) {
                final byte[][] frames = new byte[batchFrames + 2][];
                frames[0] = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 50L, 500_000L));
                for (int i = 0; i < batchFrames; i++) {
                    frames[i + 1] = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L + i, (i + 1) * 1_000_000L));
                }
                // b holds one row, 8 bytes of v. The failing frame's rows reach v's second 16 KiB
                // page at its row 2,047, and its last value is not a number.
                frames[batchFrames + 1] = varcharRowsFrame("b", 2_049, 11_000_000L);
                final long aBefore = lastTxn(hookEngine, "a");
                final long bAfterFirstPass;
                final ObjList<byte[]> sent;
                final QwpIngressProcessorState state = new QwpIngressProcessorState(
                        RECV_BUFFER_SIZE,
                        httpConfig.getSendBufferSize(),
                        hookEngine,
                        httpConfig.getLineHttpProcessorConfiguration()
                );
                try (Connection connection = new Connection(hookEngine, httpConfig, state, armingContext, false, 64 * 1024, frames)) {
                    connection.deliverFrames(1);
                    bAfterFirstPass = lastTxn(hookEngine, "b");
                    isArmable.set(true);
                    connection.deliverFrames(batchFrames + 1);
                    sent = connection.sentFrames();
                }
                Assert.assertEquals("test setup: the cancel's mapping must fail, and the writer's free must not retry it",
                        2, mmapsSinceArmed.get());
                Assert.assertFalse("the failed cancel must distress b's writer, so the pool expels it",
                        bWriter.get().isOpen());
                Assert.assertEquals("the batch commits a once", aBefore + 1, lastTxn(hookEngine, "a"));
                Assert.assertEquals("b commits nothing more", bAfterFirstPass, lastTxn(hookEngine, "b"));
                Assert.assertEquals(batchFrames, maxCumulativeOkAck(sent));
                Assert.assertEquals(QwpConstants.STATUS_INTERNAL_ERROR, errorStatusForSeq(sent, batchFrames + 1));
                TestUtils.assertContains(errorMessageForSeq(sent, batchFrames + 1), "could not mmap");
                Assert.assertTrue("the ack must precede the NACK",
                        indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, batchFrames) < indexOfErrorResponse(sent, batchFrames + 1));

                final ObjList<byte[]> sentAfter = new ObjList<>();
                ingestInOnePass(
                        hookEngine,
                        new QwpIngressUpgradeProcessor(hookEngine, httpConfig),
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        sentAfter,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 7L, 20_000_000L))
                );
                Assert.assertEquals(0, maxCumulativeOkAck(sentAfter));
                drainWalQueue(hookEngine);
                assertHookQuery(hookEngine, "SELECT v FROM a").expectSize().returns(V_100_TO_109);
                assertHookQuery(hookEngine, "SELECT v FROM b").expectSize().returns("v\n50\n7\n");
            }
        });
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
                    denyCommitAfterJoinTo("tab"),
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
     * Three tables interleave in one pass, frames a0 b1 c2 a3 b4 c5, and one table's insert
     * permission is refused. The refused table's first frame ends the batch: the frames
     * before it commit, once per table, and are acked, and that frame is NACKed.
     */
    @Test
    public void testPermissionRefusalNacksRefusedTablesFirstFrame() throws Exception {
        assertMemoryLeak(() -> {
            for (int deniedTable = 0; deniedTable < 3; deniedTable++) {
                final String[] tables = {"a" + deniedTable, "b" + deniedTable, "c" + deniedTable};
                final long[] txnBefore = new long[tables.length];
                for (int t = 0; t < tables.length; t++) {
                    execute("CREATE TABLE " + tables[t] + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    txnBefore[t] = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName(tables[t]));
                }
                final byte[][] frames = new byte[6][];
                for (int f = 0; f < frames.length; f++) {
                    frames[f] = createMaskedFrame(
                            WebSocketOpcode.BINARY,
                            oneRowMessage(tables[f % tables.length], (f + 1) * 100L, (f + 1) * 1_000_000L)
                    );
                }
                final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
                final ObjList<byte[]> sent = ingestInOnePass(
                        new QwpIngressUpgradeProcessor(engine, httpConfig),
                        httpConfig,
                        denyCommitTo(tables[deniedTable]),
                        frames
                );
                drainWalQueue();
                // The frames before the refused one hold one row each of the tables before it.
                for (int t = 0; t < tables.length; t++) {
                    final boolean isBeforeRefusal = t < deniedTable;
                    assertQuery("SELECT v FROM " + tables[t]).noLeakCheck().expectSize()
                            .returns(isBeforeRefusal ? "v\n" + (t + 1) * 100 + "\n" : "v\n");
                    Assert.assertEquals("each table before the refusal commits once [denied=" + deniedTable + ", table=" + t + ']',
                            txnBefore[t] + (isBeforeRefusal ? 1 : 0),
                            engine.getTableSequencerAPI().lastTxn(engine.verifyTableName(tables[t])));
                }
                Assert.assertEquals("the ack covers the frames before the refused table's first [denied=" + deniedTable + ']',
                        deniedTable - 1, maxCumulativeOkAck(sent));
                Assert.assertEquals("the NACK names the refused table's first frame [denied=" + deniedTable + ']',
                        QwpConstants.STATUS_WRITE_ERROR, errorStatusForSeq(sent, deniedTable));
                for (int seq = 0; seq < frames.length; seq++) {
                    if (seq != deniedTable) {
                        Assert.assertFalse(hasErrorResponseForSeq(sent, seq));
                    }
                }
            }
        });
    }

    /**
     * The refused table's first rows come in a frame's second block, after the first block
     * appended to a batch table. The first block's row goes back out, the frames before
     * the frame commit, and the frame is NACKed.
     */
    @Test
    public void testPermissionRefusalOfSecondBlockCommitsFramesBefore() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE c (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            final long bBefore = lastTxn(engine, "b");
            final long cBefore = lastTxn(engine, "c");
            final ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    denyCommitTo("c"),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                            tableBlock("a", 300L, 3_000_000L),
                            tableBlock("c", 400L, 3_000_001L)
                    ))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n");
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n200\n");
            assertQuery("SELECT v FROM c").noLeakCheck().expectSize().returns("v\n");
            Assert.assertEquals("a commits once", aBefore + 1, lastTxn(engine, "a"));
            Assert.assertEquals("b commits once", bBefore + 1, lastTxn(engine, "b"));
            Assert.assertEquals("c commits nothing", cBefore, lastTxn(engine, "c"));
            Assert.assertEquals(1, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_WRITE_ERROR, errorStatusForSeq(sent, 2));
            Assert.assertFalse(hasErrorResponseForSeq(sent, 0));
            Assert.assertFalse(hasErrorResponseForSeq(sent, 1));
            Assert.assertTrue("the ack must precede the NACK",
                    indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 1) < indexOfErrorResponse(sent, 2));
        });
    }

    /**
     * A frame's only block, of a table new to the batch, needs a new column, and the node
     * refuses it as read-only: the transient refusal of a demote. Nothing of the frame
     * reached a writer, so the batch commits and is acked first, and the frame gets the
     * reconnect-eligible CLOSE, not an error.
     */
    @Test
    public void testPhaseOneRoleChangeRefusalCommitsBatchBeforeClose() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final SecurityContext refusingAddColumn = new AllowAllSecurityContext() {
                @Override
                public void authorizeAlterTableAddColumn(TableToken tableToken) {
                    throw CairoException.readOnlyAccess();
                }
            };
            final long aBefore = lastTxn(engine, "a");
            final ObjList<byte[]> sent;
            try (Connection connection = new Connection(
                    engine,
                    httpConfig,
                    refusingAddColumn,
                    false,
                    oneRowFramesThen(
                            "a",
                            10,
                            createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(tableBlockWithExtraColumn("b", 7L, 8L, 11_000_000L)))
                    )
            )) {
                connection.deliverFrames(11);
                sent = connection.sentFrames();
            }
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns(V_100_TO_109);
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n");
            Assert.assertEquals("the batch commits once", aBefore + 1, lastTxn(engine, "a"));
            final int closeIndex = indexOfCloseFrame(sent);
            Assert.assertTrue("the refusal must close the connection", closeIndex >= 0);
            Assert.assertEquals(WebSocketCloseCode.NORMAL_CLOSURE, closeCode(sent.getQuick(closeIndex)));
            Assert.assertEquals(9, maxCumulativeOkAck(sent));
            Assert.assertTrue("the ack must precede the CLOSE", indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 9) < closeIndex);
            Assert.assertFalse("a role-change refusal sends no error response", hasErrorResponseForSeq(sent, 10));
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
                    denyCommitAfterJoinTo("tab"),
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

    /**
     * A plain frame that closes a deferred group commits the group on its own, as
     * without batching: the batch before the group commits at its first deferred
     * frame, and the frame after the group opens a new batch.
     */
    @Test
    public void testPlainFrameClosingDeferredGroupCommitsAlone() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a"));
            ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, deferred(oneRowMessage("a", 200L, 2_000_000L))),
                    createMaskedFrame(WebSocketOpcode.BINARY, deferred(oneRowMessage("a", 300L, 3_000_000L))),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 400L, 4_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 500L, 5_000_000L))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n200\n300\n400\n500\n");
            Assert.assertEquals("the batch, the group, the new batch",
                    aBefore + 3, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a")));
            Assert.assertEquals(4, maxCumulativeOkAck(sent));
            Assert.assertTrue("the group's closing frame flushes its ack", indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 3) >= 0);
        });
    }

    /**
     * A frame appends to the batch and then meets a read-only refusal while looking up
     * a table new to the batch: the transient refusal of a demote. The frame's row goes
     * back out of a's writer, the batch commits its frame, and the frame gets the
     * reconnect-eligible CLOSE instead of an error, behind the ack for the batch.
     */
    @Test
    public void testReadOnlyRefusalAfterAppendCommitsBatchBeforeClose() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE c (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (CairoEngine refusingEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                @Override
                public WalWriter getWalWriter(TableToken token) {
                    if (token.getTableName().equals("c")) {
                        throw CairoException.readOnlyAccess();
                    }
                    return super.getWalWriter(token);
                }
            }) {
                final ObjList<byte[]> sent;
                try (Connection connection = new Connection(
                        refusingEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                                tableBlock("a", 200L, 2_000_000L),
                                tableBlock("c", 300L, 3_000_000L)
                        ))
                )) {
                    connection.deliverFrames(2);
                    sent = connection.sentFrames();
                }
                drainWalQueue(refusingEngine);
                try (TableReader a = refusingEngine.getReader("a"); TableReader c = refusingEngine.getReader("c")) {
                    Assert.assertEquals("only the batch's frame commits", 1, a.size());
                    Assert.assertEquals(0, c.size());
                }
                final int closeIndex = indexOfCloseFrame(sent);
                Assert.assertTrue("the refusal must close the connection", closeIndex >= 0);
                Assert.assertEquals(WebSocketCloseCode.NORMAL_CLOSURE, closeCode(sent.getQuick(closeIndex)));
                Assert.assertEquals(0, maxCumulativeOkAck(sent));
                Assert.assertTrue("the ack must precede the CLOSE", indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 0) < closeIndex);
                Assert.assertFalse("a role-change refusal sends no error response", hasErrorResponseForSeq(sent, 1));
            }
        });
    }

    /**
     * A table dropped and re-created between passes leaves a stale entry, which the next
     * lookup evicts. When closing the evicted writer fails, the entry keeps its writer, and
     * the table cache must still not serve it again: the re-created table's two frames in
     * the next pass share one commit batch on the new writer.
     */
    @Test
    public void testRecreatedTableAfterFailedEvictionSharesOneBatch() throws Exception {
        // Closing a WAL writer msyncs its files only when the commit mode is not NOSYNC.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "async");
        final AtomicBoolean isMsyncArmed = new AtomicBoolean();
        final AtomicInteger failedMsyncCount = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public void msync(long addr, long len, boolean async) {
                // Fails the first msync once armed: the close of the evicted writer.
                if (isMsyncArmed.compareAndSet(true, false)) {
                    failedMsyncCount.incrementAndGet();
                    throw CairoException.critical(5).put("could not msync");
                }
                super.msync(addr, len, async);
            }
        };
        assertMemoryLeak(ff, () -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (Connection connection = new Connection(
                    engine,
                    httpConfig,
                    AllowAllSecurityContext.INSTANCE,
                    false,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tab", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tab", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tab", 300L, 3_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("tab", 400L, 4_000_000L))
            )) {
                // The second frame finds tab's entry, which the cache then checks first.
                connection.deliverFrames(2);
                drainWalQueue();
                execute("DROP TABLE tab");
                execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                isMsyncArmed.set(true);
                connection.deliverFrames(2);
                Assert.assertEquals("test setup: closing the evicted writer must fail", 1, failedMsyncCount.get());
                Assert.assertFalse(hasErrorResponseForSeq(connection.sentFrames(), 2));
                Assert.assertFalse(hasErrorResponseForSeq(connection.sentFrames(), 3));
                Assert.assertEquals(3, maxCumulativeOkAck(connection.sentFrames()));
            }
            drainWalQueue();
            assertQuery("SELECT v FROM tab").noLeakCheck().expectSize().returns("v\n300\n400\n");
            Assert.assertEquals("the re-created table takes both frames in one commit",
                    1, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("tab")));
        });
    }

    /**
     * Frame 1 of the second pass redefines dictionary id 0, x since the first pass, as y,
     * and opens the batch. When the batch's last frame fails after it appended to s, its
     * row goes back out, and the batch commits once with y.
     */
    @Test
    public void testRedefiningFrameOpeningBatchKeepsItsSymbols() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE s (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE t (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            final GlobalSymbolDictionary otherDict = new GlobalSymbolDictionary();
            final ObjList<byte[]> sent;
            final long sAfterFirstPass;
            try (Connection connection = new Connection(
                    engine,
                    httpConfig,
                    AllowAllSecurityContext.INSTANCE,
                    false,
                    symbolFrame(dict, -1, "s", "x", 100L, 1_000_000L, null),
                    symbolFrame(otherDict, -1, "s", "y", 200L, 2_000_000L, null),
                    symbolFrame(otherDict, 0, "s", "y", 300L, 3_000_000L, null),
                    symbolFrame(otherDict, 0, "s", "y", 400L, 4_000_000L, "t")
            )) {
                connection.deliverFrames(1);
                sAfterFirstPass = lastTxn(engine, "s");
                connection.deliverFrames(3);
                sent = connection.sentFrames();
            }
            Assert.assertEquals("the second pass commits s once", sAfterFirstPass + 1, lastTxn(engine, "s"));
            drainWalQueue();
            assertQuery("SELECT s, v FROM s").noLeakCheck().expectSize().returns("s\tv\nx\t100\ny\t200\ny\t300\n");
            assertQuery("SELECT v FROM t").noLeakCheck().expectSize().returns("v\n");
            Assert.assertEquals(2, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, 3));
        });
    }

    /**
     * Frame 1 names dictionary id 0 without a delta, meaning x, and frame 2 redefines id 0
     * as y, as a client replaying another sender's dictionary does. Frame 2 joins the batch
     * like any other frame: the writer holds the table's symbol keys, not the client's ids,
     * so frame 1 keeps x. Frame 3 fails after it appended to s: its row goes back out, and
     * the batch commits s once.
     */
    @Test
    public void testRedefiningSymbolDictionaryJoinsCommitBatch() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE s (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE t (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            final GlobalSymbolDictionary otherDict = new GlobalSymbolDictionary();
            final ObjList<byte[]> sent;
            final long sAfterFirstPass;
            try (Connection connection = new Connection(
                    engine,
                    httpConfig,
                    AllowAllSecurityContext.INSTANCE,
                    false,
                    symbolFrame(dict, -1, "s", "x", 100L, 1_000_000L, null),
                    symbolFrame(dict, 0, "s", "x", 200L, 2_000_000L, null),
                    symbolFrame(otherDict, -1, "s", "y", 300L, 3_000_000L, null),
                    symbolFrame(otherDict, 0, "s", "y", 400L, 4_000_000L, "t")
            )) {
                connection.deliverFrames(1);
                sAfterFirstPass = lastTxn(engine, "s");
                connection.deliverFrames(3);
                sent = connection.sentFrames();
            }
            Assert.assertEquals("the second pass commits s once", sAfterFirstPass + 1, lastTxn(engine, "s"));
            drainWalQueue();
            assertQuery("SELECT s, v FROM s").noLeakCheck().expectSize().returns("s\tv\nx\t100\nx\t200\ny\t300\n");
            assertQuery("SELECT v FROM t").noLeakCheck().expectSize().returns("v\n");
            Assert.assertEquals(2, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, 3));
        });
    }

    /**
     * A frame fails on its only block, and the batch commit owed before the frame's answer
     * is refused: the NACK names the batch's first frame with the commit's status, nothing
     * is committed, and the frame gets no response; the client replays it behind the batch.
     */
    @Test
    public void testRefusedBoundaryCommitAfterFailedFirstBlockNacksBatch() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            final long bBefore = lastTxn(engine, "b");
            final ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    denyCommitAfterJoinTo("a"),
                    oneRowFramesThen("a", 10, createMaskedFrame(WebSocketOpcode.BINARY, oneRowVarcharMessage("b", "not-a-long", 11_000_000L)))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n");
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n");
            Assert.assertEquals(aBefore, lastTxn(engine, "a"));
            Assert.assertEquals(bBefore, lastTxn(engine, "b"));
            Assert.assertEquals(-1, maxCumulativeOkAck(sent));
            Assert.assertEquals("the NACK names the batch's first frame, with the commit's status",
                    QwpConstants.STATUS_WRITE_ERROR, errorStatusForSeq(sent, 0));
            for (int seq = 1; seq < 11; seq++) {
                Assert.assertFalse(hasErrorResponseForSeq(sent, seq));
            }
        });
    }

    /**
     * A frame appends to b and then fails on c. Its row goes back out of b's writer, and
     * the batch commit owed before the frame's answer is refused at the batch's first
     * table: the NACK names the batch's first frame with the commit's status, nothing is
     * committed, and the frames after it get no response.
     */
    @Test
    public void testRefusedBoundaryCommitAfterRewindNacksBatch() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE c (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            final long bBefore = lastTxn(engine, "b");
            final ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    denyCommitAfterJoinTo("b"),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 300L, 3_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                            tableBlock("b", 400L, 4_000_000L),
                            varcharTableBlock("c", "not-a-long", 4_000_001L)
                    ))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n");
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n");
            Assert.assertEquals(aBefore, lastTxn(engine, "a"));
            Assert.assertEquals(bBefore, lastTxn(engine, "b"));
            Assert.assertEquals(-1, maxCumulativeOkAck(sent));
            Assert.assertEquals("the NACK names the batch's first frame, with the commit's status",
                    QwpConstants.STATUS_WRITE_ERROR, errorStatusForSeq(sent, 0));
            for (int seq = 1; seq < 5; seq++) {
                Assert.assertFalse("a frame behind the NACK gets no response", hasErrorResponseForSeq(sent, seq));
            }
        });
    }

    @Test
    public void testRefusedFirstTableRollsBackMultiTableFrame() throws Exception {
        assertRefusedFirstTableRollsBackLaterTables("b", "c");
    }

    @Test
    public void testRefusedFirstTableRollsBackRepeatedBlockFrame() throws Exception {
        assertRefusedFirstTableRollsBackLaterTables("b", "b");
    }

    @Test
    public void testRefusedFirstTableRollsBackSingleBlockFrame() throws Exception {
        assertRefusedFirstTableRollsBackLaterTables("b");
    }

    @Test
    public void testRenameWindowMidBatchAcksNothing() throws Exception {
        assertRenameWindowMidBatchAcksNothing(false);
    }

    @Test
    public void testRenameWindowMidBatchWithRefusedInsertAcksNothing() throws Exception {
        assertRenameWindowMidBatchAcksNothing(true);
    }

    /**
     * A RENAME of b reaches b's sequencer while the batch holds rows of a and b, and the
     * table registry still names b by its old token, as it does until the rename
     * finishes. b joined before the rename, so its commit fails on the sequencer's token
     * after a committed: the ack covers the frame before b's first, and the NACK names
     * b's first frame. The client's replay from it sends a's later row again, as after a
     * storage fault.
     */
    @Test
    public void testRenameWindowOfLaterTableCommitsTablesBeforeIt() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final AtomicBoolean isFrozen = new AtomicBoolean();
            final AtomicReference<QwpIngressProcessorState> stateRef = new AtomicReference<>();
            try (CairoEngine hookEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                @Override
                public boolean isReadOnlyMode() {
                    // Frame 2's preparation is the first gate check with both tables in the batch.
                    final QwpIngressProcessorState state = stateRef.get();
                    if (!isFrozen.get() && state != null && state.getCommitBatchTableCount() == 2) {
                        isFrozen.set(true);
                        try (WalWriter w = getWalWriter(verifyTableName("b"))) {
                            w.renameTable("b", "b_new", AllowAllSecurityContext.INSTANCE);
                        }
                    }
                    return false;
                }
            }) {
                final TableToken aToken = hookEngine.verifyTableName("a");
                final long aTxnBefore = hookEngine.getTableSequencerAPI().lastTxn(aToken);
                final ObjList<byte[]> sent;
                try (Connection connection = new Connection(
                        hookEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L))
                )) {
                    stateRef.set(connection.getState());
                    connection.deliverFrames(3);
                    sent = connection.sentFrames();
                }
                Assert.assertTrue("test setup: the rename window must have been frozen", isFrozen.get());
                Assert.assertEquals("test setup: the registry must still name b by its old token",
                        "b", hookEngine.verifyTableName("b").getTableName());
                Assert.assertEquals("a commits its batch rows once", aTxnBefore + 1,
                        hookEngine.getTableSequencerAPI().lastTxn(aToken));
                Assert.assertEquals("the ack stops before b's first frame", 0, maxCumulativeOkAck(sent));
                Assert.assertFalse(hasErrorResponseForSeq(sent, 0));
                Assert.assertEquals("the NACK names b's first frame",
                        QwpConstants.STATUS_INTERNAL_ERROR, errorStatusForSeq(sent, 1));
                Assert.assertFalse(hasErrorResponseForSeq(sent, 2));
                Assert.assertTrue("the ack must precede the NACK",
                        indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 0) < indexOfErrorResponse(sent, 1));
                drainWalQueue(hookEngine);
                assertHookQuery(hookEngine, "SELECT v FROM a").expectSize().returns("v\n100\n300\n");
            }
        });
    }

    // Another session renames b between the passes.
    @Test
    public void testRenamedTableNacksItsFirstFrame() throws Exception {
        assertTableChangeBetweenPassesNacksItsFirstFrame(
                "RENAME TABLE b TO b2",
                null,
                QwpConstants.STATUS_INTERNAL_ERROR
        );
        assertQuery("SELECT v FROM b2").expectSize().returns("v\n200\n");
        assertQuery("SELECT v FROM b").expectSize().returns("v\n400\n600\n");
    }

    @Test
    public void testRepeatedBlockFrameJoinsCommitBatch() throws Exception {
        assertCommitsPerTable(1, 4,
                clientEncodedFrame(1, "a"), clientEncodedFrame(1, "a", "a"), clientEncodedFrame(1, "a"));
    }

    /**
     * With a max-uncommitted-rows cap of 4, a batch of three frames of a, then a frame of b
     * and a: b's block joins, but a's would reach the cap. b's row goes back out and b
     * leaves the batch, which commits a once; the frame then opens a new batch, which
     * commits each table once.
     */
    @Test
    public void testRewindAtCapBoundary() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = httpConfigWithMaxUncommittedRows(4);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            final long bBefore = lastTxn(engine, "b");
            final ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                            tableBlock("b", 400L, 4_000_000L),
                            tableBlock("a", 401L, 4_000_001L)
                    ))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n200\n300\n401\n");
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n400\n");
            Assert.assertEquals("the batch, then the frame", aBefore + 2, lastTxn(engine, "a"));
            Assert.assertEquals(bBefore + 1, lastTxn(engine, "b"));
            Assert.assertEquals(3, maxCumulativeOkAck(sent));
            for (int seq = 0; seq < 4; seq++) {
                Assert.assertFalse(hasErrorResponseForSeq(sent, seq));
            }
        });
    }

    /**
     * A frame appends to a and then fails on b, behind a batch of three frames of each
     * table. Its row goes back out of a's writer, the batch commits each table once, and
     * the frame gets its own NACK after the ack.
     */
    @Test
    public void testRewindCommitsEachTableOnce() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            final long bBefore = lastTxn(engine, "b");
            final ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 400L, 4_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 500L, 5_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 600L, 6_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                            tableBlock("a", 700L, 7_000_000L),
                            varcharTableBlock("b", "not-a-long", 7_000_001L)
                    ))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n300\n500\n");
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n200\n400\n600\n");
            Assert.assertEquals("a commits once", aBefore + 1, lastTxn(engine, "a"));
            Assert.assertEquals("b commits once", bBefore + 1, lastTxn(engine, "b"));
            Assert.assertEquals(5, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, 6));
            Assert.assertTrue("the ack must precede the NACK",
                    indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 5) < indexOfErrorResponse(sent, 6));
        });
    }

    /**
     * A frame appends 2,049 rows to a, the batch's table, crossing onto later pages of its
     * columns, and then fails on b's value. Taking a's rows back out maps the first page of
     * a's column again, and that mapping fails: a's writer is distressed, and so is the
     * table cache. The batch is abandoned with nothing committed, and the NACK names the
     * failed frame, with INTERNAL_ERROR. The client replays the pass on a fresh connection,
     * where the fault is gone: the batch commits a once, and the frame gets its own NACK.
     */
    @Test
    public void testRewindFailureLosesBatchAndReplayCommitsItOnce() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final int batchFrames = 10;
            final AtomicBoolean isMmapFailing = new AtomicBoolean();
            final AtomicInteger failedMmaps = new AtomicInteger();
            final FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                    if (isMmapFailing.compareAndSet(true, false)) {
                        failedMmaps.incrementAndGet();
                        return FilesFacade.MAP_FAILED;
                    }
                    return super.mmap(fd, len, offset, flags, memoryTag);
                }
            };
            final AtomicBoolean isArmable = new AtomicBoolean();
            final AtomicReference<QwpIngressProcessorState> stateRef = new AtomicReference<>();
            final AtomicReference<WalWriter> aWriter = new AtomicReference<>();
            // b's insert permission is checked as b joins the failing frame, after a's block
            // appended and before b's block fails, which maps nothing: b's writer holds a row
            // on the page its append starts on.
            final SecurityContext armingContext = new AllowAllSecurityContext() {
                @Override
                public void authorizeInsert(TableToken tableToken) {
                    if (tableToken.getTableName().equals("b") && isArmable.compareAndSet(true, false)) {
                        aWriter.set(stateRef.get().getCommitBatchWriter(0));
                        isMmapFailing.set(true);
                    }
                }
            };
            try (CairoEngine hookEngine = new CairoEngine(pagedConfiguration(ff))) {
                final byte[][] replayFrames = new byte[batchFrames + 1][];
                for (int i = 0; i < batchFrames; i++) {
                    replayFrames[i] = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L + i, (i + 1) * 1_000_000L));
                }
                // The batch holds 10 rows of a, 80 bytes of v and 160 of the designated
                // timestamp. The failing frame's 2,049 rows of a move both columns onto later
                // 16 KiB pages.
                try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer table = new QwpTableBuffer("a");
                     QwpTableBuffer bad = new QwpTableBuffer("b")) {
                    final QwpTableBuffer.ColumnBuffer value = table.getOrCreateColumn("v", QwpConstants.TYPE_LONG, false);
                    final QwpTableBuffer.ColumnBuffer timestamp = table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP);
                    for (int row = 0; row < 2_049; row++) {
                        value.addLong(1_000L + row);
                        timestamp.addLong(11_000_000L + row);
                        table.nextRow();
                    }
                    bad.getOrCreateColumn("v", QwpConstants.TYPE_VARCHAR, false).addString("not-a-long");
                    bad.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(13_000_000L);
                    bad.nextRow();
                    encoder.setDeferCommit(false);
                    encoder.beginMessage(2, new GlobalSymbolDictionary(), -1, -1);
                    encoder.addTable(table);
                    encoder.addTable(bad);
                    replayFrames[batchFrames] = encodedFrame(encoder, encoder.finishMessage());
                }
                final byte[][] frames = new byte[batchFrames + 2][];
                frames[0] = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 50L, 500_000L));
                System.arraycopy(replayFrames, 0, frames, 1, replayFrames.length);
                final long aBefore = lastTxn(hookEngine, "a");
                final long bAfterFirstPass;
                final ObjList<byte[]> sent;
                final QwpIngressProcessorState state = new QwpIngressProcessorState(
                        RECV_BUFFER_SIZE,
                        httpConfig.getSendBufferSize(),
                        hookEngine,
                        httpConfig.getLineHttpProcessorConfiguration()
                );
                try (Connection connection = new Connection(hookEngine, httpConfig, state, armingContext, false, 64 * 1024, frames)) {
                    stateRef.set(connection.getState());
                    connection.deliverFrames(1);
                    bAfterFirstPass = lastTxn(hookEngine, "b");
                    isArmable.set(true);
                    connection.deliverFrames(batchFrames + 1);
                    sent = connection.sentFrames();
                }
                Assert.assertEquals("test setup: the rewind's mapping must fail", 1, failedMmaps.get());
                Assert.assertTrue("the failed rewind distresses a's writer", aWriter.get().isDistressed());
                Assert.assertEquals("nothing commits", aBefore, lastTxn(hookEngine, "a"));
                Assert.assertEquals(bAfterFirstPass, lastTxn(hookEngine, "b"));
                Assert.assertEquals("only the first pass is acked", 0, maxCumulativeOkAck(sent));
                Assert.assertEquals("the NACK names the failed frame",
                        QwpConstants.STATUS_INTERNAL_ERROR, errorStatusForSeq(sent, batchFrames + 1));
                Assert.assertFalse("the batch's first frame is not NACKed", hasErrorResponseForSeq(sent, 1));

                // The client replays the batch and the failed frame from its acked watermark.
                final ObjList<byte[]> sentAfter = new ObjList<>();
                ingestInOnePass(
                        hookEngine,
                        new QwpIngressUpgradeProcessor(hookEngine, httpConfig),
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        64 * 1024,
                        sentAfter,
                        replayFrames
                );
                Assert.assertEquals("the replay commits a once", aBefore + 1, lastTxn(hookEngine, "a"));
                Assert.assertEquals(batchFrames - 1, maxCumulativeOkAck(sentAfter));
                Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sentAfter, batchFrames));
                drainWalQueue(hookEngine);
                assertHookQuery(hookEngine, "SELECT v FROM a").expectSize().returns(V_100_TO_109);
                assertHookQuery(hookEngine, "SELECT v FROM b").expectSize().returns("v\n50\n");
            }
        });
    }

    /**
     * As {@link #testRewindFailureLosesBatchAndReplayCommitsItOnce}, for a frame that only
     * cannot join: another session adds a column to b, the batch's second table, as the
     * frame arrives, so the frame's block of b cannot join after its block of a appended
     * 2,049 rows. Taking those rows back out maps the first page of a's column again, and
     * that mapping fails. The frame had not failed, so it fails now, with INTERNAL_ERROR:
     * the normal path must not take it onto a's distressed writer. The batch is abandoned
     * with nothing committed, and the NACK names the frame.
     */
    @Test
    public void testRewindFailureOfFrameThatCannotJoinLosesBatch() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final int batchFrames = 10;
            // Fails the next mapping of a file's first page once armed: the appends map later
            // pages, and the rewind maps a column's first page again.
            final AtomicBoolean isFirstPageMapFailing = new AtomicBoolean();
            final AtomicInteger failedMmaps = new AtomicInteger();
            final FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                    if (offset == 0 && isFirstPageMapFailing.compareAndSet(true, false)) {
                        failedMmaps.incrementAndGet();
                        return FilesFacade.MAP_FAILED;
                    }
                    return super.mmap(fd, len, offset, flags, memoryTag);
                }
            };
            final AtomicInteger readOnlyChecks = new AtomicInteger();
            final AtomicReference<QwpIngressProcessorState> stateRef = new AtomicReference<>();
            final AtomicReference<WalWriter> aWriter = new AtomicReference<>();
            try (CairoEngine hookEngine = new CairoEngine(pagedConfiguration(ff)) {
                @Override
                public boolean isReadOnlyMode() {
                    // Frame 0 checks at its preparation and before it opens the batch (1, 2),
                    // frames 1 to 10 at their preparation (3 to 12), and so does the last frame.
                    if (readOnlyChecks.incrementAndGet() == batchFrames + 3) {
                        try (WalWriter writer = getWalWriter(verifyTableName("b"))) {
                            writer.addColumn("z", ColumnType.INT, AllowAllSecurityContext.INSTANCE);
                        }
                        aWriter.set(stateRef.get().getCommitBatchWriter(1));
                        isFirstPageMapFailing.set(true);
                    }
                    return false;
                }
            }) {
                final byte[][] frames = new byte[batchFrames + 2][];
                frames[0] = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 50L, 500_000L));
                for (int i = 0; i < batchFrames; i++) {
                    frames[i + 1] = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L + i, (i + 1) * 1_000_000L));
                }
                // The batch holds 10 rows of a. The frame's 2,049 rows of a move both of a's
                // columns onto later 16 KiB pages.
                try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer table = new QwpTableBuffer("a");
                     QwpTableBuffer other = new QwpTableBuffer("b")) {
                    final QwpTableBuffer.ColumnBuffer value = table.getOrCreateColumn("v", QwpConstants.TYPE_LONG, false);
                    final QwpTableBuffer.ColumnBuffer timestamp = table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP);
                    for (int row = 0; row < 2_049; row++) {
                        value.addLong(1_000L + row);
                        timestamp.addLong(11_000_000L + row);
                        table.nextRow();
                    }
                    other.getOrCreateColumn("v", QwpConstants.TYPE_LONG, false).addLong(51L);
                    other.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(13_000_000L);
                    other.nextRow();
                    encoder.setDeferCommit(false);
                    encoder.beginMessage(2, new GlobalSymbolDictionary(), -1, -1);
                    encoder.addTable(table);
                    encoder.addTable(other);
                    frames[batchFrames + 1] = encodedFrame(encoder, encoder.finishMessage());
                }
                final long aBefore = lastTxn(hookEngine, "a");
                final long bBefore = lastTxn(hookEngine, "b");
                final ObjList<byte[]> sent;
                final QwpIngressProcessorState state = new QwpIngressProcessorState(
                        RECV_BUFFER_SIZE,
                        httpConfig.getSendBufferSize(),
                        hookEngine,
                        httpConfig.getLineHttpProcessorConfiguration()
                );
                try (Connection connection = new Connection(hookEngine, httpConfig, state, AllowAllSecurityContext.INSTANCE, false, 64 * 1024, frames)) {
                    stateRef.set(connection.getState());
                    connection.deliverFrames(frames.length);
                    sent = connection.sentFrames();
                }
                Assert.assertEquals("test setup: the rewind's mapping must fail", 1, failedMmaps.get());
                Assert.assertTrue("the failed rewind distresses a's writer", aWriter.get().isDistressed());
                Assert.assertEquals("nothing commits", aBefore, lastTxn(hookEngine, "a"));
                Assert.assertEquals("only the added column", bBefore + 1, lastTxn(hookEngine, "b"));
                Assert.assertEquals("nothing is acked", -1, maxCumulativeOkAck(sent));
                Assert.assertEquals("the NACK names the frame",
                        QwpConstants.STATUS_INTERNAL_ERROR, errorStatusForSeq(sent, batchFrames + 1));
                TestUtils.assertContains(errorMessageForSeq(sent, batchFrames + 1), "could not rewind the frame's rows");
                Assert.assertFalse("the batch's first frame is not NACKed", hasErrorResponseForSeq(sent, 0));
                drainWalQueue(hookEngine);
                assertHookQuery(hookEngine, "SELECT v FROM a").expectSize().returns("v\n");
                assertHookQuery(hookEngine, "SELECT v, z FROM b").expectSize().returns("v\tz\n");
            }
        });
    }

    /**
     * Frames name their symbols by dictionary ids they ship or that an earlier frame
     * shipped, and the last frame fails after it appended to s: its row goes back out,
     * each row of the batch keeps the symbol its frame named, and the batch commits once.
     */
    @Test
    public void testRewindKeepsSymbolsOfBatchFrames() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE s (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE t (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long sBefore = lastTxn(engine, "s");
            final GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    symbolFrame(dict, -1, "s", "x", 100L, 1_000_000L, null),
                    symbolFrame(dict, 0, "s", "y", 200L, 2_000_000L, null),
                    symbolFrame(dict, 1, "s", "x", 300L, 3_000_000L, null),
                    symbolFrame(dict, 1, "s", "y", 400L, 4_000_000L, "t")
            );
            drainWalQueue();
            assertQuery("SELECT s, v FROM s").noLeakCheck().expectSize().returns("s\tv\nx\t100\ny\t200\nx\t300\n");
            assertQuery("SELECT v FROM t").noLeakCheck().expectSize().returns("v\n");
            Assert.assertEquals("the batch commits once", sBefore + 1, lastTxn(engine, "s"));
            Assert.assertEquals(2, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, 3));
        });
    }

    /**
     * A frame appends to a batch table and then fails on a table new to the batch. Its
     * row goes back out of a's writer, the batch commits a once, and the frame gets the
     * status it failed with, without being processed again.
     */
    @Test
    public void testRewoundFrameGetsTheStatusItFailedWith() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a"));
            final long bBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"));
            ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                            tableBlock("a", 300L, 3_000_000L),
                            varcharTableBlock("b", "not-a-long", 3_000_000L)
                    ))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n200\n");
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n");
            Assert.assertEquals("the batch commits once",
                    aBefore + 1, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a")));
            Assert.assertEquals(bBefore, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b")));
            Assert.assertEquals(1, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, 2));
            Assert.assertTrue("the ack must precede the NACK",
                    indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 1) < indexOfErrorResponse(sent, 2));
        });
    }

    /**
     * The batch hands the role-switch lock to a pending role switch between two tables'
     * commits, and the re-acquire fails. The batch stops at the table it was about to
     * commit, as at a failed commit: the table before it stays committed, the ack covers
     * the frame before that table's first, and the NACK names that frame. Nothing is left
     * holding the lock or releasing it twice.
     */
    @Test
    public void testRoleSwitchHandOffFailureStopsBatchAtNextTable() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final AtomicInteger lockCalls = new AtomicInteger();
            try (CairoEngine failingEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                private Lock readLock;

                @Override
                public Lock getRoleSwitchReadLock() {
                    if (readLock == null) {
                        final Lock delegate = super.getRoleSwitchReadLock();
                        readLock = new Lock() {
                            @Override
                            public void lock() {
                                // The batch's own acquire, then the re-acquire after the hand-off.
                                if (lockCalls.incrementAndGet() == 2) {
                                    throw new IllegalStateException("test: role-switch lock failed");
                                }
                                delegate.lock();
                            }

                            @Override
                            public void lockInterruptibly() throws InterruptedException {
                                delegate.lockInterruptibly();
                            }

                            @Override
                            public @NotNull Condition newCondition() {
                                return delegate.newCondition();
                            }

                            @Override
                            public boolean tryLock() {
                                return delegate.tryLock();
                            }

                            @Override
                            public boolean tryLock(long time, @NotNull TimeUnit unit) throws InterruptedException {
                                return delegate.tryLock(time, unit);
                            }

                            @Override
                            public void unlock() {
                                delegate.unlock();
                            }
                        };
                    }
                    return readLock;
                }

                @Override
                public boolean hasPendingRoleSwitchWriter() {
                    return true;
                }
            }) {
                final TableToken aToken = failingEngine.verifyTableName("a");
                final TableToken bToken = failingEngine.verifyTableName("b");
                final long aTxnBefore = failingEngine.getTableSequencerAPI().lastTxn(aToken);
                final long bTxnBefore = failingEngine.getTableSequencerAPI().lastTxn(bToken);
                final ObjList<byte[]> sent;
                try (Connection connection = new Connection(
                        failingEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L))
                )) {
                    Assert.assertFalse("the failure must not tear the connection down", connection.deliverFrames(3));
                    sent = connection.sentFrames();
                    Assert.assertEquals("test setup: the hand-off must have re-acquired once", 2, lockCalls.get());
                    Assert.assertEquals("no role-switch read hold may remain", 0, failingEngine.getRoleSwitchReadLockCount());
                }
                Assert.assertEquals("a commits before the hand-off", aTxnBefore + 1,
                        failingEngine.getTableSequencerAPI().lastTxn(aToken));
                Assert.assertEquals("b, after it, commits nothing", bTxnBefore,
                        failingEngine.getTableSequencerAPI().lastTxn(bToken));
                Assert.assertEquals("the ack stops before b's first frame", 0, maxCumulativeOkAck(sent));
                Assert.assertFalse(hasErrorResponseForSeq(sent, 0));
                Assert.assertEquals("the NACK names b's first frame",
                        QwpConstants.STATUS_INTERNAL_ERROR, errorStatusForSeq(sent, 1));
                TestUtils.assertContains(errorMessageForSeq(sent, 1), "test: role-switch lock failed");
                Assert.assertFalse(hasErrorResponseForSeq(sent, 2));
                drainWalQueue(failingEngine);
                assertHookQuery(failingEngine, "SELECT v FROM a").expectSize().returns("v\n100\n300\n");
            }
        });
    }

    /**
     * A role switch that leaves the node writable, such as a reverted demote, takes the
     * real write lock while a batch of three tables commits. It gets the lock between a's
     * commit and b's, and the batch then commits b and c: one ack covers the pass.
     */
    @Test
    public void testRoleSwitchWriterGetsLockBetweenTablesAndBatchContinues() throws Exception {
        assertRoleSwitchWriterGetsLockBetweenTables(false);
    }

    /**
     * A demote takes the real role-switch write lock while a batch of three tables
     * commits. It gets the lock between a's commit and b's, not after the last table, and
     * the batch, reading the role again, commits neither b nor c: the ack covers the frame
     * before b's first, and the role-change CLOSE follows, with no error response.
     */
    @Test
    public void testRoleSwitchWriterGetsLockBetweenTablesAndDemoteStopsBatch() throws Exception {
        assertRoleSwitchWriterGetsLockBetweenTables(true);
    }

    /**
     * A frame's first block carries no rows, and its second block, of the batch's table,
     * fails on a value. The appender cancels the failed block, which leaves the writer
     * with the batch's rows: the batch commits once, and the frame then gets its own NACK.
     */
    @Test
    public void testRowlessFirstBlockThenFailedBlockCommitsBatchBeforeNack() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                            tableBlock("a"),
                            varcharTableBlock("a", "not-a-long", 2_000_000L)
                    ))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n");
            Assert.assertEquals("the batch commits once", aBefore + 1, lastTxn(engine, "a"));
            Assert.assertEquals(0, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, 1));
        });
    }

    @Test
    public void testRowlessRepeatedBlockFrameJoinsCommitBatch() throws Exception {
        assertCommitsPerTable(1, 2,
                clientEncodedFrame(1, "a"), clientEncodedFrame(0, "a", "a"), clientEncodedFrame(1, "a"));
    }

    @Test
    public void testRowlessSingletonJoinsCommitBatch() throws Exception {
        assertCommitsPerTable(1, 2,
                clientEncodedFrame(1, "a"), clientEncodedFrame(0, "a"), clientEncodedFrame(1, "a"));
    }

    @Test
    public void testRowlessThenRowBearingMultiBlockJoinsCommitBatch() throws Exception {
        assertCommitsPerTable(1, 3,
                clientEncodedFrame(1, "a"),
                createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(tableBlock("b"), tableBlock("a", 200, 2_000_000))),
                clientEncodedFrame(1, "a"));
    }

    @Test
    public void testSameTableBlocksWithDifferentColumnsJoinCommitBatch() throws Exception {
        assertBlocksWithDifferentColumnsJoinCommitBatch("weather");
    }

    /**
     * A frame's first block brings n into the batch, and its second block, of n again,
     * needs a column n lacks, which a block may not create once its table holds batch
     * rows. Nothing failed: n's first row goes back out of n's writer and n leaves the
     * batch, the batch commits a once, and the frame then goes the normal way from its
     * first block, which creates the column and commits each of n's rows once.
     */
    @Test
    public void testSecondBlockOfNewTableThatCannotJoinCommitsFrameOnce() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE n (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            final long nBefore = lastTxn(engine, "n");
            final ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                            tableBlock("n", 300L, 3_000_000L),
                            tableBlockWithExtraColumn("n", 301L, 7L, 3_000_001L)
                    ))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n200\n");
            assertQuery("SELECT v, w FROM n").noLeakCheck().expectSize().returns("v\tw\n300\tnull\n301\t7\n");
            Assert.assertEquals("the batch commits a once", aBefore + 1, lastTxn(engine, "a"));
            Assert.assertEquals("the new column, then the frame", nBefore + 2, lastTxn(engine, "n"));
            Assert.assertEquals(2, maxCumulativeOkAck(sent));
            for (int seq = 0; seq < 3; seq++) {
                Assert.assertFalse(hasErrorResponseForSeq(sent, seq));
            }
        });
    }

    @Test
    public void testSingleBlockFramesRetainOneCommit() throws Exception {
        assertCommitsPerTable(1, 3,
                clientEncodedFrame(1, "a"), clientEncodedFrame(1, "a"), clientEncodedFrame(1, "a"));
    }

    @Test
    public void testStandaloneFrameCapFailureIsNackedAfterBoundaryAck() throws Exception {
        assertStandaloneFrameCapFailure(false);
    }

    /**
     * b's sequencer records a DROP right after the lookup acquired b's writer, while the
     * table registry still names b, as it does within a drop. b is refused as it joins
     * the batch, before its row appends: a's batch commits, the NACK names b's frame with
     * the sequencer's refusal, and nothing after it commits. Had b joined, its commit would
     * have failed after a's, and a's later row would have committed and been replayed.
     */
    @Test
    public void testSequencerDroppedTableIsRefusedAsItJoins() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final AtomicBoolean isDropped = new AtomicBoolean();
            try (CairoEngine hookEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                @Override
                public WalWriter getWalWriter(TableToken token) {
                    final WalWriter writer = super.getWalWriter(token);
                    if (token.getTableName().equals("b") && isDropped.compareAndSet(false, true)) {
                        getTableSequencerAPI().dropTable(token, false);
                    }
                    return writer;
                }
            }) {
                final TableToken aToken = hookEngine.verifyTableName("a");
                final long aTxnBefore = hookEngine.getTableSequencerAPI().lastTxn(aToken);
                final ObjList<byte[]> sent;
                try (Connection connection = new Connection(
                        hookEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L))
                )) {
                    connection.deliverFrames(3);
                    sent = connection.sentFrames();
                }
                Assert.assertTrue("test setup: b's sequencer must have been dropped", isDropped.get());
                Assert.assertEquals("a's batch commits once", aTxnBefore + 1,
                        hookEngine.getTableSequencerAPI().lastTxn(aToken));
                Assert.assertEquals("the ack covers a's frame", 0, maxCumulativeOkAck(sent));
                Assert.assertEquals("the NACK names b's frame",
                        QwpConstants.STATUS_WRITE_ERROR, errorStatusForSeq(sent, 1));
                TestUtils.assertContains(errorMessageForSeq(sent, 1), "table is dropped");
                Assert.assertFalse(hasErrorResponseForSeq(sent, 0));
                Assert.assertFalse(hasErrorResponseForSeq(sent, 2));
                drainWalQueue(hookEngine);
                assertHookQuery(hookEngine, "SELECT v FROM a").expectSize().returns("v\n100\n");
            }
        });
    }

    // An operator suspends b's WAL between the passes, while suspended tables deny writes.
    @Test
    public void testSuspendedTableNacksItsFirstFrame() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertTableChangeBetweenPassesNacksItsFirstFrame(
                "ALTER TABLE b SUSPEND WAL",
                "ALTER TABLE b RESUME WAL",
                QwpConstants.STATUS_WRITE_ERROR
        );
        assertQuery("SELECT v FROM b").expectSize().returns("v\n200\n400\n600\n");
    }

    // By default a suspended table still takes writes, so the batch commits.
    @Test
    public void testSuspendedTableWithWritesAllowedCommitsBatch() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final ObjList<byte[]> sent;
            try (Connection connection = new Connection(
                    engine,
                    httpConfig,
                    AllowAllSecurityContext.INSTANCE,
                    false,
                    tableChangeBetweenPassesFrames()
            )) {
                connection.deliverFrames(2);
                execute("ALTER TABLE b SUSPEND WAL");
                connection.deliverFrames(4);
                sent = connection.sentFrames();
            }
            Assert.assertEquals("the whole pass is acked", 5, maxCumulativeOkAck(sent));
            for (int seq = 0; seq < 6; seq++) {
                Assert.assertFalse(hasErrorResponseForSeq(sent, seq));
            }
            execute("ALTER TABLE b RESUME WAL");
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n300\n500\n");
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n200\n400\n600\n");
        });
    }

    /**
     * Another session renames b and then renames it back between the passes. b is the same
     * table under its old name, so its frames join pass 2's batch and commit, as its
     * sequencer would take them, and pass 2 is acked whole.
     */
    @Test
    public void testTableRenamedBackJoinsCommitBatch() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken bToken = engine.verifyTableName("b");
            final ObjList<byte[]> sent;
            try (Connection connection = new Connection(
                    engine,
                    httpConfig,
                    AllowAllSecurityContext.INSTANCE,
                    false,
                    tableChangeBetweenPassesFrames()
            )) {
                connection.deliverFrames(2);
                Assert.assertEquals("test setup: pass 1 commits", 1, maxCumulativeOkAck(connection.sentFrames()));
                execute("RENAME TABLE b TO b2");
                execute("RENAME TABLE b2 TO b");
                connection.deliverFrames(4);
                sent = connection.sentFrames();
            }
            Assert.assertEquals("test setup: b keeps its directory", bToken.getDirName(), engine.verifyTableName("b").getDirName());
            Assert.assertEquals("pass 2 is acked whole", 5, maxCumulativeOkAck(sent));
            for (int seq = 0; seq < 6; seq++) {
                Assert.assertFalse(hasErrorResponseForSeq(sent, seq));
            }
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n300\n500\n");
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n200\n400\n600\n");
        });
    }

    /**
     * A table keeps its position in the batch of an earlier pass. In the next pass that
     * position lies past the batch's tables or holds another table, and the table must
     * join the new batch as a new table, not as the stale entry. So must a table that a
     * rowless block opened without joining any batch.
     */
    @Test
    public void testTablesOfEarlierPassRejoinLaterBatch() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            final String[] tables = {"a", "b", "c", "d", "e"};
            final long[] txnBefore = new long[tables.length];
            for (int t = 0; t < tables.length; t++) {
                execute("CREATE TABLE " + tables[t] + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                txnBefore[t] = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName(tables[t]));
            }
            try (Connection connection = new Connection(
                    engine,
                    httpConfig,
                    AllowAllSecurityContext.INSTANCE,
                    false,
                    // pass 1: a takes position 0, b position 1
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                    // pass 2: b's old position 1 is past the one-table batch, then holds d
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("c", 300L, 3_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 400L, 4_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("d", 500L, 5_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 600L, 6_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("c", 700L, 7_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 800L, 8_000_000L)),
                    // e opens without rows, then joins
                    createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(tableBlock("e"))),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("e", 900L, 9_000_000L))
            )) {
                connection.deliverFrames(2);
                connection.deliverFrames(8);
                Assert.assertEquals(9, maxCumulativeOkAck(connection.sentFrames()));
            }
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n600\n");
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n200\n400\n800\n");
            assertQuery("SELECT v FROM c").noLeakCheck().expectSize().returns("v\n300\n700\n");
            assertQuery("SELECT v FROM d").noLeakCheck().expectSize().returns("v\n500\n");
            assertQuery("SELECT v FROM e").noLeakCheck().expectSize().returns("v\n900\n");
            final long[] expectedCommits = {2, 2, 1, 1, 1};
            for (int t = 0; t < tables.length; t++) {
                Assert.assertEquals("one commit per table per pass [table=" + tables[t] + ']',
                        txnBefore[t] + expectedCommits[t],
                        engine.getTableSequencerAPI().lastTxn(engine.verifyTableName(tables[t])));
            }
        });
    }

    /**
     * A frame names a table past the connection's table limit: its lookup fails
     * before it appends anything, so the batch commits, and the frame gets its own
     * NACK.
     */
    @Test
    public void testTooManyTablesFrameIsNackedAfterBatchCommits() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = httpConfigWithMaxTables(2);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE c (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("c", 300L, 3_000_000L))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n");
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n200\n");
            assertQuery("SELECT v FROM c").noLeakCheck().expectSize().returns("v\n");
            Assert.assertEquals(1, maxCumulativeOkAck(sent));
            Assert.assertTrue(hasErrorResponseForSeq(sent, 2));
            Assert.assertTrue("the ack must precede the NACK",
                    indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 1) < indexOfErrorResponse(sent, 2));
        });
    }

    /**
     * A frame's lookup of a table new to the batch fails unexpectedly, which leaves the
     * connection's writers in doubt: the table cache distresses, and a distressed cache
     * commits nothing. The batch is abandoned, nothing is acked, and the NACK names the
     * frame, with INTERNAL_ERROR: the failure recurs on every replay of the batch, and the
     * client's poison detector must strike this frame, not the batch's healthy first frame.
     */
    @Test
    public void testUnexpectedLookupErrorLosesBatch() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (CairoEngine hookEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                @Override
                public WalWriter getWalWriter(TableToken token) {
                    if (token.getTableName().equals("b")) {
                        throw new IllegalStateException("test: unexpected writer failure");
                    }
                    return super.getWalWriter(token);
                }
            }) {
                final long aBefore = lastTxn(hookEngine, "a");
                final long bBefore = lastTxn(hookEngine, "b");
                final ObjList<byte[]> sent = new ObjList<>();
                ingestInOnePass(
                        hookEngine,
                        new QwpIngressUpgradeProcessor(hookEngine, httpConfig),
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        sent,
                        oneRowFramesThen("a", 10, createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 7L, 11_000_000L)))
                );
                Assert.assertEquals("nothing commits", aBefore, lastTxn(hookEngine, "a"));
                Assert.assertEquals(bBefore, lastTxn(hookEngine, "b"));
                Assert.assertEquals("nothing is acked", -1, maxCumulativeOkAck(sent));
                Assert.assertEquals(QwpConstants.STATUS_INTERNAL_ERROR, errorStatusForSeq(sent, 10));
                TestUtils.assertContains(errorMessageForSeq(sent, 10), "unexpected error");
                Assert.assertFalse("the batch's first frame must not be NACKed", hasErrorResponseForSeq(sent, 0));
                drainWalQueue(hookEngine);
                assertHookQuery(hookEngine, "SELECT v FROM a").expectSize().returns("v\n");
                assertHookQuery(hookEngine, "SELECT v FROM b").expectSize().returns("v\n");
            }
        });
    }

    /**
     * A frame's preparation fails unexpectedly behind a batch, at the live read-only gate,
     * which distresses the table cache: a distressed cache commits nothing. The batch is
     * abandoned, nothing is acked, the NACK names the frame, with INTERNAL_ERROR, and the
     * table takes rows afterwards.
     */
    @Test
    public void testUnexpectedPreparationErrorLosesBatch() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final AtomicInteger readOnlyChecks = new AtomicInteger();
            final AtomicBoolean hasFailed = new AtomicBoolean();
            try (CairoEngine hookEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                @Override
                public boolean isReadOnlyMode() {
                    // Frame 0 checks at its preparation and before it opens the batch (1, 2),
                    // frames 1 to 9 at their preparation (3 to 11), and so does frame 10 (12).
                    if (readOnlyChecks.incrementAndGet() == 12) {
                        hasFailed.set(true);
                        throw new IllegalStateException("test: unexpected live gate failure");
                    }
                    return false;
                }
            }) {
                final long aBefore = lastTxn(hookEngine, "a");
                final ObjList<byte[]> sent = new ObjList<>();
                ingestInOnePass(
                        hookEngine,
                        new QwpIngressUpgradeProcessor(hookEngine, httpConfig),
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        sent,
                        oneRowFramesThen("a", 11)
                );
                Assert.assertTrue("test setup: frame 10's preparation must fail", hasFailed.get());
                Assert.assertEquals("nothing commits", aBefore, lastTxn(hookEngine, "a"));
                Assert.assertEquals("nothing is acked", -1, maxCumulativeOkAck(sent));
                Assert.assertEquals(QwpConstants.STATUS_INTERNAL_ERROR, errorStatusForSeq(sent, 10));
                TestUtils.assertContains(errorMessageForSeq(sent, 10), "unexpected error");
                Assert.assertFalse("the batch's first frame must not be NACKed", hasErrorResponseForSeq(sent, 0));

                final ObjList<byte[]> sentAfter = new ObjList<>();
                ingestInOnePass(
                        hookEngine,
                        new QwpIngressUpgradeProcessor(hookEngine, httpConfig),
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        sentAfter,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 200L, 20_000_000L))
                );
                Assert.assertEquals(0, maxCumulativeOkAck(sentAfter));
                drainWalQueue(hookEngine);
                assertHookQuery(hookEngine, "SELECT v FROM a").expectSize().returns("v\n200\n");
            }
        });
    }

    @Test
    public void testWriterAcquisitionCairoErrorAfterAppendCommitsBatchBeforeNack() throws Exception {
        assertWriterAcquisitionFailureAfterAppend(false, false);
    }

    @Test
    public void testWriterAcquisitionUnexpectedErrorAfterAppendLosesBatch() throws Exception {
        assertWriterAcquisitionFailureAfterAppend(true, false);
    }

    /**
     * A data frame without a payload commits the batch before it, as it always did, and
     * is acked on its own.
     */
    @Test
    public void testZeroLengthFrameEndsCommitBatch() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a"));
            ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, new byte[0]),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n300\n");
            Assert.assertEquals("the empty frame splits the batch",
                    aBefore + 2, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a")));
            Assert.assertEquals(2, maxCumulativeOkAck(sent));
            Assert.assertFalse(hasErrorResponseForSeq(sent, 1));
        });
    }

    // A row of table t with a VARCHAR, a STRING, a BINARY, a DOUBLE[] and a LONG, which
    // takes v as a VARCHAR.
    private static void addTypedRow(QwpTableBuffer table, int i, String v) {
        table.getOrCreateColumn("x", QwpConstants.TYPE_VARCHAR, true).addString("x" + i + "-\u00e9\u20ac");
        table.getOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true).addString("s" + i);
        table.getOrCreateColumn("bin", QwpConstants.TYPE_BINARY, true).addBinary(new byte[]{(byte) i, 1, 2, (byte) (i * 3)});
        table.getOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, true).addDoubleArray(new double[]{i, i + 0.5});
        table.getOrCreateColumn("v", QwpConstants.TYPE_VARCHAR, true).addString(v);
        table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(1_000_000L * (i + 1));
        table.nextRow();
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

    private static SecurityContext denyCommitAfterJoinTo(String tableName) {
        // Models a refusal that lands after the table joined: passes the check as the table
        // joins a commit batch, outside the role-switch read lock, and refuses the one under
        // it, in the table's own commit.
        return new AllowAllSecurityContext() {
            @Override
            public void authorizeInsert(TableToken tableToken) {
                if (tableToken.getTableName().equals(tableName) && engine.getRoleSwitchReadLockCount() > 0) {
                    throw CairoException.nonCritical().put("test: commit denied for ").put(tableName);
                }
            }
        };
    }

    private static SecurityContext denyCommitTo(String tableName) {
        // On the QWP path authorizeInsert runs for rows to commit: when a table joins a commit
        // batch, and in each table's commit (TableUpdateDetails.authorizeCommit).
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
        // Denies the table's inserts once it has committed a transaction after this call.
        // On the QWP path authorizeInsert runs only for rows to commit: when a table joins a
        // commit batch, and in each table's commit.
        final long txnBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName(tableName));
        return new AllowAllSecurityContext() {
            @Override
            public void authorizeInsert(TableToken tableToken) {
                if (tableToken.getTableName().equals(tableName) && engine.getTableSequencerAPI().lastTxn(tableToken) > txnBefore) {
                    throw CairoException.nonCritical().put("test: commit denied for ").put(tableName);
                }
            }
        };
    }

    // The message the encoder staged, as a masked BINARY frame.
    private static byte[] encodedFrame(QwpWebSocketEncoder encoder, int length) {
        final long address = encoder.getBuffer().getBufferPtr();
        final byte[] payload = new byte[length];
        for (int i = 0; i < length; i++) {
            payload[i] = Unsafe.getByte(address + i);
        }
        return createMaskedFrame(WebSocketOpcode.BINARY, payload);
    }

    // The message of the error response for seq, or null when there is none. After
    // [status][seq LE] an error response carries [length u16 LE][message UTF-8].
    private static String errorMessageForSeq(ObjList<byte[]> frames, long seq) {
        final int index = indexOfErrorResponse(frames, seq);
        if (index < 0) {
            return null;
        }
        final byte[] frame = frames.getQuick(index);
        return new String(frame, 13, readLeShort(frame, 11), StandardCharsets.UTF_8);
    }

    private static long errorStatusForSeq(ObjList<byte[]> frames, long seq) {
        final int index = indexOfErrorResponse(frames, seq);
        return index < 0 ? -1 : frames.getQuick(index)[2];
    }

    private static SecurityContext failCommitAfterJoin(String tableName) {
        // Passes the table's insert check as it joins a commit batch and refuses the one in
        // the table's own commit: a commit that fails after the table joined, as a storage
        // fault does.
        return new AllowAllSecurityContext() {
            private int checks;

            @Override
            public void authorizeInsert(TableToken tableToken) {
                if (tableToken.getTableName().equals(tableName) && ++checks > 1) {
                    throw CairoException.nonCritical().put("test: commit failed for ").put(tableName);
                }
            }
        };
    }

    private static HttpFullFatServerConfiguration httpConfigWithMaxTables(int maxTables) {
        final LineHttpProcessorConfiguration lineConfig = new DefaultLineHttpProcessorConfiguration(configuration) {
            @Override
            public int getQwpMaxTablesPerConnection() {
                return maxTables;
            }
        };
        return new DefaultHttpServerConfiguration(configuration) {
            @Override
            public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
                return lineConfig;
            }
        };
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

    private static HttpFullFatServerConfiguration httpConfigWithoutNewColumns() {
        final LineHttpProcessorConfiguration lineConfig = new DefaultLineHttpProcessorConfiguration(configuration) {
            @Override
            public boolean autoCreateNewColumns() {
                return false;
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

    private static long lastTxn(CairoEngine engine, String table) {
        return engine.getTableSequencerAPI().lastTxn(engine.verifyTableName(table));
    }

    /**
     * {@code count} one-row frames of {@code table}, with v = 100, 101, ... and ts 1s, 2s,
     * ..., then {@code tail}.
     */
    private static byte[][] oneRowFramesThen(String table, int count, byte[]... tail) {
        final byte[][] frames = new byte[count + tail.length][];
        for (int i = 0; i < count; i++) {
            frames[i] = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(table, 100L + i, (i + 1) * 1_000_000L));
        }
        System.arraycopy(tail, 0, frames, count, tail.length);
        return frames;
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

    // A configuration of the shared root whose WAL columns map 16 KiB pages through ff.
    private static DefaultTestCairoConfiguration pagedConfiguration(FilesFacade ff) {
        return new DefaultTestCairoConfiguration(root) {
            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return ff;
            }

            @Override
            public long getWalDataAppendPageSize() {
                return 16 * 1024;
            }
        };
    }

    /**
     * A one-row frame in delta-dictionary mode for a table with columns {@code s}
     * SYMBOL, {@code v} LONG and a designated timestamp. It ships the dictionary
     * entries above {@code confirmedMaxId}. With a {@code badTable}, a second block
     * follows that writes {@code v} as a VARCHAR, which a LONG column refuses as
     * SCHEMA_MISMATCH.
     */
    private static byte[] symbolFrame(
            GlobalSymbolDictionary dict,
            int confirmedMaxId,
            String table,
            String symbol,
            long v,
            long tsMicros,
            String badTable
    ) {
        final int symbolId = dict.getOrAddSymbol(symbol);
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            encoder.setDeferCommit(false);
            encoder.beginMessage(badTable == null ? 1 : 2, dict, confirmedMaxId, dict.size() - 1);
            try (QwpTableBuffer buffer = new QwpTableBuffer(table)) {
                buffer.getOrCreateColumn("s", QwpConstants.TYPE_SYMBOL, false).addSymbolWithGlobalId(symbol, symbolId);
                buffer.getOrCreateColumn("v", QwpConstants.TYPE_LONG, false).addLong(v);
                buffer.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(tsMicros);
                buffer.nextRow();
                encoder.addTable(buffer);
            }
            if (badTable != null) {
                try (QwpTableBuffer buffer = new QwpTableBuffer(badTable)) {
                    buffer.getOrCreateColumn("v", QwpConstants.TYPE_VARCHAR, false).addString("not-a-long");
                    buffer.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(tsMicros);
                    buffer.nextRow();
                    encoder.addTable(buffer);
                }
            }
            final int length = encoder.finishMessage();
            final long address = encoder.getBuffer().getBufferPtr();
            final byte[] payload = new byte[length];
            for (int i = 0; i < length; i++) {
                payload[i] = Unsafe.getByte(address + i);
            }
            return createMaskedFrame(WebSocketOpcode.BINARY, payload);
        }
    }

    /**
     * As {@link #symbolFrame}, but the frame writes {@code v} as a VARCHAR, which the LONG
     * column parses, and refuses as SCHEMA_MISMATCH when it is not a number. The block
     * writes {@code s} before {@code v}.
     */
    private static byte[] symbolVarcharFrame(
            GlobalSymbolDictionary dict,
            int confirmedMaxId,
            String table,
            String symbol,
            String v,
            long tsMicros
    ) {
        final int symbolId = dict.getOrAddSymbol(symbol);
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            encoder.setDeferCommit(false);
            encoder.beginMessage(1, dict, confirmedMaxId, dict.size() - 1);
            try (QwpTableBuffer buffer = new QwpTableBuffer(table)) {
                buffer.getOrCreateColumn("s", QwpConstants.TYPE_SYMBOL, false).addSymbolWithGlobalId(symbol, symbolId);
                buffer.getOrCreateColumn("v", QwpConstants.TYPE_VARCHAR, false).addString(v);
                buffer.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(tsMicros);
                buffer.nextRow();
                encoder.addTable(buffer);
            }
            return encodedFrame(encoder, encoder.finishMessage());
        }
    }

    // Pass 1 (seq 0, 1) opens a and b; pass 2 (seq 2-5) interleaves them.
    private static byte[][] tableChangeBetweenPassesFrames() {
        return new byte[][]{
                createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L)),
                createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 400L, 4_000_000L)),
                createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 500L, 5_000_000L)),
                createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 600L, 6_000_000L))
        };
    }

    private static byte[][] toArray(ObjList<byte[]> frames) {
        byte[][] array = new byte[frames.size()][];
        for (int i = 0, n = frames.size(); i < n; i++) {
            array[i] = frames.getQuick(i);
        }
        return array;
    }

    /**
     * A one-row frame of table t(x VARCHAR, s STRING, bin BINARY, arr DOUBLE[], v LONG, ts)
     * whose block writes the columns in that order, with values derived from {@code i}, and
     * {@code v} as a VARCHAR, which the LONG column parses: a value that is not a number
     * fails after x, s, bin and arr are written.
     */
    private static byte[] typedFrame(int i, String v) {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer table = new QwpTableBuffer("t")) {
            addTypedRow(table, i, v);
            return encodedFrame(encoder, encoder.encode(table));
        }
    }

    // typedFrame's block of t, then a block of b with a value b's LONG column cannot take.
    private static byte[] typedFrameThenBadBlock(int i, String v) {
        try (
                QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                QwpTableBuffer table = new QwpTableBuffer("t");
                QwpTableBuffer bad = new QwpTableBuffer("b")
        ) {
            addTypedRow(table, i, v);
            bad.getOrCreateColumn("v", QwpConstants.TYPE_VARCHAR, false).addString("not-a-long");
            bad.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(1_000_000L * (i + 1));
            bad.nextRow();
            encoder.setDeferCommit(false);
            encoder.beginMessage(2, new GlobalSymbolDictionary(), -1, -1);
            encoder.addTable(table);
            encoder.addTable(bad);
            return encodedFrame(encoder, encoder.finishMessage());
        }
    }

    // A frame of the table's rows with v as a VARCHAR of digits, except the last row's,
    // which is not a number.
    private static byte[] varcharRowsFrame(String tableName, int rows, long firstTsMicros) {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer table = new QwpTableBuffer(tableName)) {
            final QwpTableBuffer.ColumnBuffer value = table.getOrCreateColumn("v", QwpConstants.TYPE_VARCHAR, false);
            final QwpTableBuffer.ColumnBuffer timestamp = table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP);
            for (int row = 0; row < rows; row++) {
                value.addString(row < rows - 1 ? Long.toString(1_000L + row) : "not-a-long");
                timestamp.addLong(firstTsMicros + row);
                table.nextRow();
            }
            return encodedFrame(encoder, encoder.encode(table));
        }
    }

    /**
     * Frames a0, b1, a2 and b3 form a batch of two tables, and frame 4 appends to a, then
     * fails on b's value, which b's LONG column cannot take. Frame 4's row of a goes back
     * out of a's writer, the batch commits each table once, and frame 4 gets its own NACK.
     * The ack covers frames 0 to 3 and, on a durable-ack connection, names both commits.
     */
    private void assertBatchCommitsBeforeFrameFailingAfterAppend(boolean isDurableAck) throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a"));
            final long bBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"));
            final ObjList<byte[]> sent = new ObjList<>();
            ingestInOnePass(
                    engine,
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    AllowAllSecurityContext.INSTANCE,
                    isDurableAck,
                    sent,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 400L, 4_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                            tableBlock("a", 500L, 5_000_000L),
                            varcharTableBlock("b", "not-a-long", 5_000_000L)
                    ))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n300\n");
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n200\n400\n");
            Assert.assertEquals("a commits once", aBefore + 1, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a")));
            Assert.assertEquals("b commits once", bBefore + 1, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b")));
            Assert.assertEquals(3, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, 4));
            Assert.assertTrue("the ack must precede the NACK",
                    indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 3) < indexOfErrorResponse(sent, 4));
            if (isDurableAck) {
                Assert.assertEquals("the ack must name a's commit", aBefore + 1, okAckSeqTxn(sent, 3, "a"));
                Assert.assertEquals("the ack must name b's commit", bBefore + 1, okAckSeqTxn(sent, 3, "b"));
            }
        });
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

    /**
     * Each frame carries two blocks of table weather: one named weather with column v, one
     * named {@code secondName} with v and h, which the table has. Both join the batch: the
     * pass commits weather once, and one ack covers every frame.
     */
    private void assertBlocksWithDifferentColumnsJoinCommitBatch(String secondName) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE weather (v LONG, h LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long txnBefore = lastTxn(engine, "weather");
            final byte[][] frames = new byte[5][];
            for (int f = 0; f < frames.length; f++) {
                final long ts = (f + 1) * 1_000_000L;
                frames[f] = createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                        tableBlock("weather", f * 10L, ts),
                        tableBlockWithColumn(secondName, f * 10L + 1, "h", f, ts + 1)
                ));
            }
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            final ObjList<byte[]> sent = ingestInOnePass(new QwpIngressUpgradeProcessor(engine, httpConfig), httpConfig, frames);
            Assert.assertEquals("the pass commits weather once", txnBefore + 1, lastTxn(engine, "weather"));
            Assert.assertEquals(frames.length - 1, maxCumulativeOkAck(sent));
            Assert.assertEquals("one ack after the batch commit", 1, countOkAcks(sent));
            drainWalQueue();
            assertQuery("SELECT v, h FROM weather").noLeakCheck().expectSize().returns("""
                    v\th
                    0\tnull
                    1\t0
                    10\tnull
                    11\t1
                    20\tnull
                    21\t2
                    30\tnull
                    31\t3
                    40\tnull
                    41\t4
                    """);
        });
    }

    /**
     * Frames of one receive pass, of tables a, b and c, commit once per table that got
     * rows, and one ack covers them all.
     */
    private void assertCommitsPerTable(long expectedTransactions, long expectedRows, byte[]... frames) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE c (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            long before = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a"))
                    + engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"))
                    + engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("c"));
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            ObjList<byte[]> sent = ingestInOnePass(new QwpIngressUpgradeProcessor(engine, httpConfig), httpConfig, frames);
            drainWalQueue();
            try (TableReader a = engine.getReader("a"); TableReader b = engine.getReader("b"); TableReader c = engine.getReader("c")) {
                Assert.assertEquals(expectedRows, a.size() + b.size() + c.size());
            }
            long after = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a"))
                    + engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"))
                    + engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("c"));
            Assert.assertEquals("the batch commits each table once", expectedTransactions, after - before);
            Assert.assertEquals(frames.length - 1, maxCumulativeOkAck(sent));
            Assert.assertEquals("one ack after the batch commit", 1, countOkAcks(sent));
        });
    }

    /**
     * Three tables interleave in one batch, frames a0 b1 c2 a3 b4 c5, and the commit of
     * table {@code deniedTable} fails after the table joined, as a storage fault would. The tables commit in the order of their first frames and stop at the failed
     * one: the ack covers the frames before its first frame, the NACK names that frame,
     * and the tables after it publish nothing. On a durable-ack connection the ack names
     * the commits of the tables before it.
     */
    private void assertCommitFailureAtTable(int deniedTable, String... expectedValues) throws Exception {
        assertMemoryLeak(() -> {
            for (int durable = 0; durable < 2; durable++) {
                final boolean isDurableAck = durable == 1;
                final String[] tables = {"a" + durable, "b" + durable, "c" + durable};
                final long[] txnBefore = new long[tables.length];
                for (int t = 0; t < tables.length; t++) {
                    execute("CREATE TABLE " + tables[t] + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    txnBefore[t] = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName(tables[t]));
                }
                final byte[][] frames = new byte[6][];
                for (int f = 0; f < frames.length; f++) {
                    frames[f] = createMaskedFrame(
                            WebSocketOpcode.BINARY,
                            oneRowMessage(tables[f % tables.length], (f + 1) * 100L, (f + 1) * 1_000_000L)
                    );
                }
                final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
                final ObjList<byte[]> sent = new ObjList<>();
                ingestInOnePass(engine, new QwpIngressUpgradeProcessor(engine, httpConfig), httpConfig,
                        failCommitAfterJoin(tables[deniedTable]), isDurableAck, sent, frames);
                drainWalQueue();
                for (int t = 0; t < tables.length; t++) {
                    assertQuery("SELECT v FROM " + tables[t]).noLeakCheck().expectSize().returns("v\n" + expectedValues[t]);
                    Assert.assertEquals("only the tables before the refused one commit, once each",
                            txnBefore[t] + (t < deniedTable ? 1 : 0),
                            engine.getTableSequencerAPI().lastTxn(engine.verifyTableName(tables[t])));
                }
                final long ack = deniedTable - 1;
                Assert.assertEquals("the ack stops before the refused table's first frame", ack, maxCumulativeOkAck(sent));
                for (int seq = 0; seq < frames.length; seq++) {
                    Assert.assertEquals("only the refused table's first frame is NACKed [seq=" + seq + ']',
                            seq == deniedTable, hasErrorResponseForSeq(sent, seq));
                }
                if (ack >= 0) {
                    Assert.assertTrue("the ack must precede the NACK",
                            indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, ack) < indexOfErrorResponse(sent, deniedTable));
                    if (isDurableAck) {
                        for (int t = 0; t < deniedTable; t++) {
                            Assert.assertEquals("the ack must name " + tables[t] + "'s commit",
                                    txnBefore[t] + 1, okAckSeqTxn(sent, ack, tables[t]));
                        }
                    }
                }
            }
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
                    denyCommitAfterJoinTo("a"),
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

    // assertQuery against an engine of the test's own, which shares the test engine's root
    // and serves the tables the test wrote through it.
    private QueryAssertion assertHookQuery(CairoEngine hookEngine, String sql) {
        return assertQuery(sql)
                .withEngine(hookEngine)
                .withContext(TestUtils.createSqlExecutionCtx(hookEngine))
                .noLeakCheck();
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
     * With table auto-creation off, a frame names a table that does not exist. As the
     * frame's first block, nothing of the frame touched a writer: the batch commits, and
     * the frame gets its own NACK. Behind a block the frame appended to the batch, that
     * block's row goes back out of the writer first, and the batch commits once.
     */
    private void assertMissingTableWithoutAutoCreate(boolean isAfterAppend) throws Exception {
        assertMemoryLeak(() -> {
            final LineHttpProcessorConfiguration lineConfig = new DefaultLineHttpProcessorConfiguration(configuration) {
                @Override
                public boolean autoCreateNewTables() {
                    return false;
                }
            };
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration) {
                @Override
                public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
                    return lineConfig;
                }
            };
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a"));
            ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, isAfterAppend
                            ? qwpMessage(tableBlock("a", 300L, 3_000_000L), tableBlock("missing", 400L, 4_000_000L))
                            : oneRowMessage("missing", 400L, 4_000_000L))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n200\n");
            Assert.assertEquals("the batch commits once",
                    aBefore + 1, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a")));
            Assert.assertNull(engine.getTableTokenIfExists("missing"));
            Assert.assertEquals(1, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_INTERNAL_ERROR, errorStatusForSeq(sent, 2));
            Assert.assertTrue("the ack must precede the NACK",
                    indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 1) < indexOfErrorResponse(sent, 2));
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

    /**
     * A frame of other tables joins a batch that a's frame opened, acquiring their
     * writers. When a's commit, the batch's first, is refused, the commits stop there:
     * nothing is acked, the NACK names a's frame, and the other tables' rows are
     * rolled back without a WAL transaction.
     */
    private void assertRefusedFirstTableRollsBackLaterTables(String... tables) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE c (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long bBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"));
            final long cBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("c"));
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            ObjList<byte[]> sent = ingestInOnePass(new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig, denyCommitAfterJoinTo("a"), clientEncodedFrame(1, "a"), clientEncodedFrame(1, tables));
            drainWalQueue();
            try (TableReader a = engine.getReader("a"); TableReader b = engine.getReader("b"); TableReader c = engine.getReader("c")) {
                Assert.assertEquals(0, a.size() + b.size() + c.size());
            }
            Assert.assertEquals("b must not publish a WAL transaction", bBefore,
                    engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b")));
            Assert.assertEquals("c must not publish a WAL transaction", cBefore,
                    engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("c")));
            Assert.assertEquals("nothing committed, nothing acked", -1, maxCumulativeOkAck(sent));
            Assert.assertTrue("the NACK names a's frame", hasErrorResponseForSeq(sent, 0));
            Assert.assertFalse(hasErrorResponseForSeq(sent, 1));
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

    // A role switch takes the engine's real write lock while a batch of a, b and c commits.
    // The engine starts it when the batch first asks, between a's commit and b's, whether a
    // role switch is pending, and waits until the switch queues for the lock.
    private void assertRoleSwitchWriterGetsLockBetweenTables(boolean isDemote) throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            final String[] tables = {"a", "b", "c"};
            final TableToken[] tokens = new TableToken[tables.length];
            final long[] txnBefore = new long[tables.length];
            for (String table : tables) {
                execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            }
            final AtomicBoolean isReadOnly = new AtomicBoolean();
            final AtomicInteger committedAtGrant = new AtomicInteger(-1);
            final AtomicReference<Throwable> switchFailure = new AtomicReference<>();
            final AtomicReference<Thread> roleSwitch = new AtomicReference<>();
            try (CairoEngine roleSwitchEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                @Override
                public boolean hasPendingRoleSwitchWriter() {
                    if (roleSwitch.get() == null) {
                        final Thread thread = new Thread(() -> {
                            try {
                                final Lock writeLock = getRoleSwitchWriteLock();
                                writeLock.lock();
                                try {
                                    int committed = 0;
                                    for (int t = 0; t < tokens.length; t++) {
                                        if (getTableSequencerAPI().lastTxn(tokens[t]) > txnBefore[t]) {
                                            committed++;
                                        }
                                    }
                                    committedAtGrant.set(committed);
                                    isReadOnly.set(isDemote);
                                } finally {
                                    writeLock.unlock();
                                }
                            } catch (Throwable th) {
                                switchFailure.set(th);
                            }
                        }, "qwp-role-switch");
                        roleSwitch.set(thread);
                        thread.start();
                        while (!super.hasPendingRoleSwitchWriter() && thread.isAlive()) {
                            Thread.onSpinWait();
                        }
                    }
                    return super.hasPendingRoleSwitchWriter();
                }

                @Override
                public boolean isReadOnlyMode() {
                    return isReadOnly.get();
                }
            }) {
                for (int t = 0; t < tables.length; t++) {
                    tokens[t] = roleSwitchEngine.verifyTableName(tables[t]);
                    txnBefore[t] = roleSwitchEngine.getTableSequencerAPI().lastTxn(tokens[t]);
                }
                final ObjList<byte[]> sent;
                try (Connection connection = new Connection(
                        roleSwitchEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("c", 300L, 3_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 400L, 4_000_000L))
                )) {
                    connection.deliverFrames(4);
                    sent = connection.sentFrames();
                }
                final Thread thread = roleSwitch.get();
                Assert.assertNotNull("test setup: the batch must ask for a pending role switch", thread);
                thread.join();
                Assert.assertNull(switchFailure.get());
                Assert.assertEquals("the role switch gets the lock after a's commit, before b's", 1, committedAtGrant.get());
                for (int seq = 0; seq < 4; seq++) {
                    Assert.assertFalse("no frame gets an error response", hasErrorResponseForSeq(sent, seq));
                }
                drainWalQueue(roleSwitchEngine);
                assertHookQuery(roleSwitchEngine, "SELECT v FROM a").expectSize().returns("v\n100\n400\n");
                if (isDemote) {
                    Assert.assertEquals("the ack stops before b's first frame", 0, maxCumulativeOkAck(sent));
                    final int closeIndex = indexOfCloseFrame(sent);
                    Assert.assertTrue("the demote must close the connection", closeIndex >= 0);
                    Assert.assertEquals(WebSocketCloseCode.NORMAL_CLOSURE, closeCode(sent.getQuick(closeIndex)));
                    Assert.assertTrue("the ack must precede the CLOSE", indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 0) < closeIndex);
                    for (int t = 1; t < tables.length; t++) {
                        Assert.assertEquals(tables[t] + ", after the demote, commits nothing", txnBefore[t],
                                roleSwitchEngine.getTableSequencerAPI().lastTxn(tokens[t]));
                    }
                } else {
                    Assert.assertEquals("one ack covers the pass", 3, maxCumulativeOkAck(sent));
                    Assert.assertEquals("no CLOSE", -1, indexOfCloseFrame(sent));
                    assertHookQuery(roleSwitchEngine, "SELECT v FROM b").expectSize().returns("v\n200\n");
                    assertHookQuery(roleSwitchEngine, "SELECT v FROM c").expectSize().returns("v\n300\n");
                }
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

    /**
     * Pass 1 opens a and b on a connection, {@code ddl} then changes b from another
     * session, and pass 2 interleaves a, b, a, b. b is refused as it joins the batch: a's
     * frame before it commits and is acked, and b's frame is NACKed. After
     * {@code ddlBeforeReplay}, the client replays every frame above its ack on a new
     * connection, and a holds each of its rows once.
     */
    private void assertTableChangeBetweenPassesNacksItsFirstFrame(
            String ddl,
            String ddlBeforeReplay,
            byte refusalStatus
    ) throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final byte[][] frames = tableChangeBetweenPassesFrames();
            final TableToken aToken = engine.verifyTableName("a");
            final long aTxnBefore;
            final ObjList<byte[]> sent;
            try (Connection connection = new Connection(engine, httpConfig, AllowAllSecurityContext.INSTANCE, false, frames)) {
                connection.deliverFrames(2);
                Assert.assertEquals("test setup: pass 1 commits", 1, maxCumulativeOkAck(connection.sentFrames()));
                aTxnBefore = engine.getTableSequencerAPI().lastTxn(aToken);
                execute(ddl);
                connection.deliverFrames(4);
                sent = connection.sentFrames();
            }
            Assert.assertEquals("a commits the frame before b's once", aTxnBefore + 1,
                    engine.getTableSequencerAPI().lastTxn(aToken));
            Assert.assertEquals("pass 2's frame before b's is acked", 2, maxCumulativeOkAck(sent));
            Assert.assertEquals("the NACK names b's first frame of pass 2", refusalStatus, errorStatusForSeq(sent, 3));
            for (int seq = 0; seq < 6; seq++) {
                if (seq != 3) {
                    Assert.assertFalse(hasErrorResponseForSeq(sent, seq));
                }
            }

            if (ddlBeforeReplay != null) {
                execute(ddlBeforeReplay);
            }
            try (Connection replay = new Connection(
                    engine,
                    httpConfig,
                    AllowAllSecurityContext.INSTANCE,
                    false,
                    Arrays.copyOfRange(frames, 3, frames.length)
            )) {
                replay.deliverFrames(3);
                Assert.assertEquals("the replay is acked", 2, maxCumulativeOkAck(replay.sentFrames()));
            }
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n300\n500\n");
        });
    }

    /**
     * A batch of a's frames, then a frame whose blocks append to a and to n, a table new to
     * the batch, in the order given, and whose third block fails on b's value. Both rows go
     * back out: a's writer returns to the batch's rows, and n, which the frame brought into
     * the batch, leaves it. The batch commits a once, the frame gets its own NACK, and both
     * tables take rows on a fresh connection, with nothing of the failed frame among them.
     */
    private void assertThirdBlockFailureRewindsBothTables(boolean isNewTableFirst) throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE n (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            final long nBefore = lastTxn(engine, "n");
            final byte[] batchTableBlock = tableBlock("a", 300L, 3_000_000L);
            final byte[] newTableBlock = tableBlock("n", 301L, 3_000_001L);
            final ObjList<byte[]> sent = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                            isNewTableFirst ? newTableBlock : batchTableBlock,
                            isNewTableFirst ? batchTableBlock : newTableBlock,
                            varcharTableBlock("b", "not-a-long", 3_000_002L)
                    ))
            );
            Assert.assertEquals("the batch commits a once", aBefore + 1, lastTxn(engine, "a"));
            Assert.assertEquals("n commits nothing", nBefore, lastTxn(engine, "n"));
            Assert.assertEquals(1, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, 2));
            Assert.assertTrue("the ack must precede the NACK",
                    indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 1) < indexOfErrorResponse(sent, 2));

            final ObjList<byte[]> sentAfter = ingestInOnePass(
                    new QwpIngressUpgradeProcessor(engine, httpConfig),
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 400L, 4_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("n", 500L, 5_000_000L))
            );
            Assert.assertEquals(1, maxCumulativeOkAck(sentAfter));
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n200\n400\n");
            assertQuery("SELECT v FROM n").noLeakCheck().expectSize().returns("v\n500\n");
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n");
        });
    }

    /**
     * Frames [a100], [c200], [a300], [a301], then [a400][d401] in one pass, through an
     * engine whose writer acquisition always fails for d. The last frame's row of a goes
     * back out of a's writer. A CairoException leaves the table cache healthy: the batch
     * commits each table once, and the frame gets its own NACK; on a durable-ack
     * connection, the ack names both commits. An unexpected failure leaves the
     * connection's writers in doubt and distresses the cache, which commits nothing: the
     * batch is abandoned with nothing acked, and the NACK names the failed frame, with
     * INTERNAL_ERROR.
     */
    private void assertWriterAcquisitionFailureAfterAppend(boolean isUnexpected, boolean isDurableAck) throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE c (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE d (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (CairoEngine hookEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                @Override
                public WalWriter getWalWriter(TableToken token) {
                    if (token.getTableName().equals("d")) {
                        if (isUnexpected) {
                            throw new IllegalStateException("test: d's writer acquisition fails");
                        }
                        throw CairoException.nonCritical().put("test: d's writer acquisition fails");
                    }
                    return super.getWalWriter(token);
                }
            }) {
                final long aBefore = lastTxn(hookEngine, "a");
                final long cBefore = lastTxn(hookEngine, "c");
                final long dBefore = lastTxn(hookEngine, "d");
                final ObjList<byte[]> sent = new ObjList<>();
                ingestInOnePass(
                        hookEngine,
                        new QwpIngressUpgradeProcessor(hookEngine, httpConfig),
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        isDurableAck,
                        sent,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("c", 200L, 2_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 301L, 3_000_001L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                                tableBlock("a", 400L, 4_000_000L),
                                tableBlock("d", 401L, 4_000_001L)
                        ))
                );
                Assert.assertEquals(dBefore, lastTxn(hookEngine, "d"));
                for (int seq = 0; seq < 4; seq++) {
                    Assert.assertFalse("only the failed frame is NACKed [seq=" + seq + ']', hasErrorResponseForSeq(sent, seq));
                }
                TestUtils.assertContains(errorMessageForSeq(sent, 4), "d's writer acquisition fails");
                drainWalQueue(hookEngine);
                assertHookQuery(hookEngine, "SELECT v FROM d").expectSize().returns("v\n");
                if (isUnexpected) {
                    Assert.assertEquals("nothing commits", aBefore, lastTxn(hookEngine, "a"));
                    Assert.assertEquals("nothing commits", cBefore, lastTxn(hookEngine, "c"));
                    Assert.assertEquals("nothing is acked", -1, maxCumulativeOkAck(sent));
                    Assert.assertEquals(QwpConstants.STATUS_INTERNAL_ERROR, errorStatusForSeq(sent, 4));
                    assertHookQuery(hookEngine, "SELECT v FROM a").expectSize().returns("v\n");
                    assertHookQuery(hookEngine, "SELECT v FROM c").expectSize().returns("v\n");
                } else {
                    Assert.assertEquals("a commits once", aBefore + 1, lastTxn(hookEngine, "a"));
                    Assert.assertEquals("c commits once", cBefore + 1, lastTxn(hookEngine, "c"));
                    Assert.assertEquals(3, maxCumulativeOkAck(sent));
                    Assert.assertEquals(QwpConstants.STATUS_WRITE_ERROR, errorStatusForSeq(sent, 4));
                    Assert.assertTrue("the ack must precede the NACK",
                            indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 3) < indexOfErrorResponse(sent, 4));
                    assertHookQuery(hookEngine, "SELECT v FROM a").expectSize().returns("v\n100\n300\n301\n");
                    assertHookQuery(hookEngine, "SELECT v FROM c").expectSize().returns("v\n200\n");
                    if (isDurableAck) {
                        Assert.assertEquals("the ack must name a's commit", aBefore + 1, okAckSeqTxn(sent, 3, "a"));
                        Assert.assertEquals("the ack must name c's commit", cBefore + 1, okAckSeqTxn(sent, 3, "c"));
                    }
                }
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
        ingestInOnePass(stateEngine, processor, httpConfig, securityContext, isDurableAckEnabled, RECV_BUFFER_SIZE, sent, frames);
    }

    /**
     * As above, through a receive buffer of {@code recvBufferSize} bytes, which must hold
     * every frame.
     */
    private void ingestInOnePass(
            CairoEngine stateEngine,
            QwpIngressUpgradeProcessor processor,
            HttpFullFatServerConfiguration httpConfig,
            SecurityContext securityContext,
            boolean isDurableAckEnabled,
            int recvBufferSize,
            ObjList<byte[]> sent,
            byte[]... frames
    ) throws Exception {
        byte[] wire = concat(frames);
        assert wire.length <= recvBufferSize;
        PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
        long recvBuf = Unsafe.malloc(recvBufferSize, MemoryTag.NATIVE_DEFAULT);
        long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
        try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, recvBufferSize)) {
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
            Unsafe.free(recvBuf, recvBufferSize, MemoryTag.NATIVE_DEFAULT);
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

    /**
     * Generates one iteration of {@link #testCommitBatchAckContractFuzz}: its faults and its
     * frames, over the tables fa, fb and fc suffixed with {@code iteration}.
     */
    private static FuzzIteration generateCommitBatchFuzzIteration(Rnd rnd, int iteration) {
        final FuzzIteration it = new FuzzIteration();
        it.iteration = iteration;
        it.tables = new String[]{"fa" + iteration, "fb" + iteration, "fc" + iteration};
        it.isDataErrorsOnly = rnd.nextInt(3) == 0;
        it.isDurable = rnd.nextBoolean();
        it.cap = rnd.nextInt(3) == 0 ? 1 + rnd.nextInt(4) : 1_000_000;
        it.readOnlyFromCall = !it.isDataErrorsOnly && rnd.nextInt(3) == 0 ? 1 + rnd.nextInt(30) : -1;
        it.isReadOnlyTransient = rnd.nextBoolean();
        it.refusedInsertCall = !it.isDataErrorsOnly && rnd.nextInt(3) == 0 ? 1 + rnd.nextInt(6) : -1;
        it.isRefusalPersistent = rnd.nextBoolean();
        it.writerFaultTable = !it.isDataErrorsOnly && rnd.nextInt(3) == 0 ? rnd.nextInt(it.tables.length) : -1;
        it.writerFaultFromCall = 1 + rnd.nextInt(4);
        it.isWriterFaultTransient = rnd.nextBoolean();
        it.isWriterFaultUnexpected = rnd.nextBoolean();

        final int frameCount = 2 + rnd.nextInt(10);
        int seq = 0;
        for (int f = 0; f < frameCount; f++) {
            int kind = rnd.nextInt(it.isDataErrorsOnly ? FUZZ_DATA_KINDS : FUZZ_KINDS);
            if ((it.cap < 1_000_000 && (kind == FUZZ_TWO_BLOCKS || kind == FUZZ_BAD_SECOND_BLOCK || kind == FUZZ_NEW_COLUMN_SECOND_BLOCK))
                    || (kind == FUZZ_DEFERRED && (it.cap < 1_000_000 || it.isCommitFaulty()))
                    || (kind > FUZZ_PLAIN && kind < FUZZ_TWO_BLOCKS)) {
                // Outside the commit batch's contract, with or without batching: a frame
                // of more than one block, or a deferred group, can be split by the
                // appender's own commit at a small cap, and commitAll commits a group's
                // tables one by one; a later commit of the same frame or group that then
                // fails NACKs rows already in the WAL. The kinds without a name are plain.
                kind = FUZZ_PLAIN;
            }
            final int tableIndex = rnd.nextInt(it.tables.length);
            final int secondTableIndex = kind == FUZZ_NEW_COLUMN_SECOND_BLOCK
                    // the new column's table takes v as a LONG
                    ? rnd.nextInt(it.tables.length) % FUZZ_VARCHAR_TABLE
                    : rnd.nextInt(it.tables.length);
            final String table = it.tables[tableIndex];
            final String secondTable = it.tables[secondTableIndex];
            final long v = seq * 10L;
            final long ts = 1_000_000L + seq * 1_000L;
            final LongList values = new LongList();
            final IntList valueTables = new IntList();
            final IntList blocks = new IntList();
            long errorStatus = -1;
            final byte[] message;
            it.kinds.append(kind).append(table.charAt(1));
            if (kind == FUZZ_TWO_BLOCKS || kind == FUZZ_BAD_SECOND_BLOCK || kind == FUZZ_NEW_COLUMN_SECOND_BLOCK) {
                it.kinds.append(secondTable.charAt(1));
            }
            it.kinds.append(' ');
            switch (kind) {
                case FUZZ_TWO_BLOCKS -> {
                    message = qwpMessage(
                            fuzzBlock(it, tableIndex, v, ts, blocks),
                            fuzzBlock(it, secondTableIndex, v + 1, ts + 1, blocks)
                    );
                    values.add(v);
                    valueTables.add(tableIndex);
                    values.add(v + 1);
                    valueTables.add(secondTableIndex);
                }
                case FUZZ_BAD_SECOND_BLOCK -> {
                    message = qwpMessage(fuzzBlock(it, tableIndex, v, ts, blocks), varcharTableBlock(secondTable, "x", ts + 1));
                    blocks.add(secondTableIndex);
                    blocks.add(FUZZ_BLOCK_V_VARCHAR);
                    errorStatus = QwpConstants.STATUS_SCHEMA_MISMATCH;
                }
                case FUZZ_NEW_COLUMN_SECOND_BLOCK -> {
                    message = qwpMessage(
                            fuzzBlock(it, tableIndex, v, ts, blocks),
                            tableBlockWithColumn(secondTable, v + 1, "w" + seq, seq, ts + 1)
                    );
                    blocks.add(secondTableIndex);
                    blocks.add(FUZZ_BLOCK_NEW_COLUMN + seq);
                    values.add(v);
                    valueTables.add(tableIndex);
                    values.add(v + 1);
                    valueTables.add(secondTableIndex);
                }
                case FUZZ_MALFORMED -> {
                    message = Arrays.copyOf(oneRowMessage(table, v, ts), QwpConstants.HEADER_SIZE + 8);
                    errorStatus = QwpConstants.STATUS_PARSE_ERROR;
                }
                case FUZZ_SCHEMA_MISMATCH -> {
                    message = oneRowVarcharMessage(table, "x", ts);
                    blocks.add(tableIndex);
                    blocks.add(FUZZ_BLOCK_V_VARCHAR);
                    errorStatus = QwpConstants.STATUS_SCHEMA_MISMATCH;
                }
                case FUZZ_DEFERRED -> {
                    message = deferred(qwpMessage(fuzzBlock(it, tableIndex, v, ts, blocks)));
                    values.add(v);
                    valueTables.add(tableIndex);
                }
                case FUZZ_ROWLESS_DEFERRED -> message = rowlessDeferredMessage();
                case FUZZ_PING -> {
                    // a control frame, no sequence
                    it.wireFrames.add(createMaskedFrame(WebSocketOpcode.PING, new byte[0]));
                    continue;
                }
                default -> {
                    message = qwpMessage(fuzzBlock(it, tableIndex, v, ts, blocks));
                    values.add(v);
                    valueTables.add(tableIndex);
                }
            }
            it.wireFrames.add(createMaskedFrame(WebSocketOpcode.BINARY, message));
            it.frameKinds.add(kind);
            it.frameBlocks.add(blocks);
            it.frameValues.add(values);
            it.frameValueTables.add(valueTables);
            it.frameErrorStatus.add(errorStatus);
            seq++;
        }
        return it;
    }

    // A one-row block of the iteration's table with v = value, as the table takes v, and
    // its record in blocks.
    private static byte[] fuzzBlock(FuzzIteration it, int tableIndex, long value, long tsMicros, IntList blocks) {
        if (tableIndex == FUZZ_VARCHAR_TABLE) {
            blocks.add(tableIndex);
            blocks.add(FUZZ_BLOCK_V_VARCHAR);
            return varcharTableBlock(it.tables[tableIndex], Long.toString(value), tsMicros);
        }
        blocks.add(tableIndex);
        blocks.add(FUZZ_BLOCK_V_LONG);
        return tableBlock(it.tables[tableIndex], value, tsMicros);
    }

    /**
     * Runs the iterations of {@link #testCommitBatchAckContractFuzz} through an engine whose
     * read-only checks and writer acquisitions fail as each iteration scripts.
     */
    private void runCommitBatchFuzz(ObjList<FuzzIteration> iterations) throws Exception {
        // The hook engine below shares the root but cannot create tables: create them first.
        for (int i = 0, n = iterations.size(); i < n; i++) {
            for (String table : iterations.getQuick(i).tables) {
                execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            }
        }
        final AtomicReference<FuzzIteration> current = new AtomicReference<>();
        try (CairoEngine hookEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
            @Override
            public WalWriter getWalWriter(TableToken tableToken) {
                final FuzzIteration it = current.get();
                if (it != null && it.writerFaultTable > -1 && tableToken.getTableName().equals(it.tables[it.writerFaultTable])) {
                    final long call = ++it.writerCalls;
                    if (it.isWriterFaultTransient ? call == it.writerFaultFromCall : call >= it.writerFaultFromCall) {
                        if (it.isWriterFaultUnexpected) {
                            throw new IllegalStateException("fuzz: writer acquisition fails");
                        }
                        throw CairoException.nonCritical().put("fuzz: writer acquisition fails");
                    }
                }
                return super.getWalWriter(tableToken);
            }

            @Override
            public boolean isReadOnlyMode() {
                final FuzzIteration it = current.get();
                if (it == null) {
                    return false;
                }
                final long call = ++it.readOnlyCalls;
                return it.readOnlyFromCall > 0 && (it.isReadOnlyTransient ? call == it.readOnlyFromCall : call >= it.readOnlyFromCall);
            }
        }) {
            for (int i = 0, n = iterations.size(); i < n; i++) {
                final FuzzIteration it = iterations.getQuick(i);
                current.set(it);
                runCommitBatchFuzzIteration(it, hookEngine);
            }
        }
    }

    private void runCommitBatchFuzzIteration(FuzzIteration it, CairoEngine hookEngine) throws Exception {
        final String[] tables = it.tables;
        final AtomicInteger insertCalls = new AtomicInteger();
        final SecurityContext securityContext = new AllowAllSecurityContext() {
            @Override
            public void authorizeInsert(TableToken tableToken) {
                final int call = insertCalls.incrementAndGet();
                if (it.refusedInsertCall > 0 && (it.isRefusalPersistent ? call >= it.refusedInsertCall : call == it.refusedInsertCall)) {
                    throw CairoException.authorization().put("fuzz: insert refused");
                }
            }
        };
        final long[] txnBefore = new long[tables.length];
        for (int t = 0; t < tables.length; t++) {
            txnBefore[t] = lastTxn(hookEngine, tables[t]);
        }
        final int lastSeq = it.frameValues.size() - 1;
        final boolean isCommitFaulty = it.isCommitFaulty();

        final HttpFullFatServerConfiguration httpConfig = httpConfigWithMaxUncommittedRows(it.cap);
        final ObjList<byte[]> sent = new ObjList<>();
        try {
            // Room for the largest iteration: up to 11 frames of two blocks each.
            ingestInOnePass(hookEngine, new QwpIngressUpgradeProcessor(hookEngine, httpConfig), httpConfig,
                    securityContext, it.isDurable, 4 * RECV_BUFFER_SIZE, sent, toArray(it.wireFrames));
        } catch (ServerDisconnectException ignore) {
            // a role-change CLOSE tells the dispatcher to drop the fd
        }
        drainWalQueue(hookEngine);

        final String context = it.toString();
        final ObjList<LongList> frameValues = it.frameValues;
        final ObjList<IntList> frameValueTables = it.frameValueTables;
        final LongList frameErrorStatus = it.frameErrorStatus;
        final long acked = maxCumulativeOkAck(sent);
        final LongList committed = new LongList();
        // The highest frame with a committed row of each table.
        final LongList lastCommittedFrame = new LongList();
        for (String table : tables) {
            long last = -1;
            try (
                    TableReader reader = hookEngine.getReader(table);
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                while (cursor.hasNext()) {
                    final long value = cursor.getRecord().getLong(0);
                    committed.add(value);
                    last = Math.max(last, value / 10);
                }
            }
            lastCommittedFrame.add(last);
        }
        committed.sort();
        boolean hasCommittedAboveAck = false;
        for (int i = 0, n = committed.size(); i < n; i++) {
            final long value = committed.getQuick(i);
            Assert.assertTrue("I2: a row committed twice [" + context + ", value=" + value + "]",
                    i == 0 || committed.getQuick(i - 1) != value);
            hasCommittedAboveAck |= value / 10 > acked;
            Assert.assertTrue("I2: a row of a frame above the ack is committed [" + context + ", value=" + value
                    + ", acked=" + acked + "]", value / 10 <= acked || isCommitFaulty);
        }
        if (hasCommittedAboveAck) {
            // Only a commit of several tables that failed on one after others committed:
            // a batch commit, or a multi-table frame's own commit. The client must learn
            // to replay: a NACK above the ack, or a CLOSE.
            boolean isTold = indexOfCloseFrame(sent) >= 0;
            for (int s = (int) acked + 1; s <= lastSeq && !isTold; s++) {
                isTold = hasErrorResponseForSeq(sent, s);
            }
            Assert.assertTrue("I2: rows above the ack are committed, but the client is not told to replay ["
                    + context + ", acked=" + acked + "]", isTold);
            // The replay's first frame with rows names the failed table: none of its rows
            // from that frame on are committed. Rowless frames, such as a deferred group's
            // dictionary frame that holds the ack below them, come before it.
            int replayFrom = (int) acked + 1;
            while (replayFrom <= lastSeq && frameValues.getQuick(replayFrom).size() == 0) {
                replayFrom++;
            }
            Assert.assertTrue("I2: rows above the ack are committed, but no frame above it has rows ["
                    + context + ", acked=" + acked + "]", replayFrom <= lastSeq);
            final IntList replayTables = frameValueTables.getQuick(replayFrom);
            boolean hasFailedTable = false;
            for (int i = 0, n = replayTables.size(); i < n; i++) {
                hasFailedTable |= lastCommittedFrame.getQuick(replayTables.getQuick(i)) < replayFrom;
            }
            Assert.assertTrue("I2: rows above the ack are committed, but no table of the first replayed frame failed ["
                    + context + ", acked=" + acked + "]", hasFailedTable);
        }
        for (int s = 0; s <= acked; s++) {
            final LongList values = frameValues.getQuick(s);
            for (int i = 0, n = values.size(); i < n; i++) {
                Assert.assertTrue("I1: an acked frame's row is not committed [" + context + ", seq=" + s + "]",
                        committed.binarySearch(values.getQuick(i), Vect.BIN_SEARCH_SCAN_UP) >= 0);
            }
        }
        if (it.isDataErrorsOnly) {
            final int firstError = it.firstBadFrame();
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
            if (it.cap == 1_000_000) {
                for (int t = 0; t < tables.length; t++) {
                    final long commits = lastTxn(hookEngine, tables[t]) - txnBefore[t];
                    final long bound = it.commitBound(t);
                    Assert.assertTrue("I4: " + tables[t] + " commits more often than its batches allow [" + context
                            + ", commits=" + commits + ", bound=" + bound + "]", commits <= bound);
                }
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
        private final int recvBufferSize;
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
            this(
                    engine,
                    httpConfig,
                    new QwpIngressProcessorState(
                            RECV_BUFFER_SIZE,
                            httpConfig.getSendBufferSize(),
                            engine,
                            httpConfig.getLineHttpProcessorConfiguration()
                    ),
                    securityContext,
                    isDurableAckEnabled,
                    RECV_BUFFER_SIZE,
                    frames
            );
        }

        /**
         * A connection served by {@code state}, which it closes, through a receive buffer of
         * {@code recvBufferSize} bytes.
         */
        Connection(
                CairoEngine engine,
                HttpFullFatServerConfiguration httpConfig,
                QwpIngressProcessorState state,
                SecurityContext securityContext,
                boolean isDurableAckEnabled,
                int recvBufferSize,
                byte[]... frames
        ) throws Exception {
            this.frames = frames;
            this.recvBufferSize = recvBufferSize;
            this.processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            this.nf = new HalfClosingNetworkFacade(concat(frames));
            this.recvBuf = Unsafe.malloc(recvBufferSize, MemoryTag.NATIVE_DEFAULT);
            this.sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            this.rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
            this.context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, recvBufferSize);
            this.state = state;
            state.of(-1, securityContext);
            state.setDurableAckEnabled(isDurableAckEnabled);
            getLV().set(context, state);
        }

        @Override
        public void close() {
            context.close();
            Unsafe.free(recvBuf, recvBufferSize, MemoryTag.NATIVE_DEFAULT);
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
     * commit's read-only check under the lock then refuses it -- the race the role-switch
     * fence guards against -- while every frame prepared before it passed the live
     * read-only gate. Durable uploads never catch up, so
     * committed work stays pending a durable ack.
     */
    private static final class DemotableEngine extends CairoEngine {
        // Commits left before the one the demote lands at; 0 when no demote is armed.
        private final AtomicInteger commitsUntilDemote = new AtomicInteger();
        // Commits left before the one that requests a demote, see requestDemoteAtCommit().
        private final AtomicInteger commitsUntilDemoteRequest = new AtomicInteger();
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
        // A requested demote waits for the last read hold to go, see requestDemoteAtCommit().
        private boolean isDemoteRequested;
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
        public boolean hasPendingRoleSwitchWriter() {
            return isDemoteRequested;
        }

        @Override
        public boolean isReadOnlyMode() {
            return isReadOnly.get();
        }

        void demoteAtCommit(int commit) {
            commitsUntilDemote.set(commit);
        }

        void demoteAtNextCommit() {
            demoteAtCommit(1);
        }

        /**
         * Requests a demote at the {@code commit}-th acquisition of the role-switch read
         * lock. Unlike {@link #demoteAtCommit}, it lands as a role switch's write lock would:
         * once no read hold remains, and until then it shows as a pending role-switch
         * writer, so a batch commit hands it the lock between two tables' commits.
         */
        void requestDemoteAtCommit(int commit) {
            commitsUntilDemoteRequest.set(commit);
        }

        private final class DemotingLock implements Lock {
            private final Lock delegate;
            // Read holds currently open; the connection takes them all on the test thread.
            private int holdCount;

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
                if (--holdCount == 0 && isDemoteRequested) {
                    isDemoteRequested = false;
                    isReadOnly.set(true);
                }
            }

            private void onAcquired() {
                holdCount++;
                if (commitsUntilDemote.get() > 0 && commitsUntilDemote.decrementAndGet() == 0) {
                    isReadOnly.set(true);
                }
                if (commitsUntilDemoteRequest.get() > 0 && commitsUntilDemoteRequest.decrementAndGet() == 0) {
                    isDemoteRequested = true;
                }
            }
        }
    }

    /**
     * One iteration of {@link #testCommitBatchAckContractFuzz}: its faults, its frames, and
     * what each frame should leave behind. A frame's values are its sequence * 10 + its
     * row, so a committed value names its frame. The hook engine counts the iteration's
     * read-only checks and writer acquisitions here.
     */
    private static final class FuzzIteration {
        // Per frame with a sequence, its blocks as (table index, FUZZ_BLOCK_*) pairs.
        final ObjList<IntList> frameBlocks = new ObjList<>();
        // Per frame with a sequence, the status it fails with, or -1.
        final LongList frameErrorStatus = new LongList();
        // Per frame with a sequence, its kind; FUZZ_PLAIN for every plain kind.
        final IntList frameKinds = new IntList();
        // Per frame with a sequence, the table index of each of its values, in order.
        final ObjList<IntList> frameValueTables = new ObjList<>();
        // Per frame with a sequence, the values it writes.
        final ObjList<LongList> frameValues = new ObjList<>();
        final StringBuilder kinds = new StringBuilder();
        final ObjList<byte[]> wireFrames = new ObjList<>();
        long cap;
        boolean isDataErrorsOnly;
        boolean isDurable;
        boolean isReadOnlyTransient;
        boolean isRefusalPersistent;
        boolean isWriterFaultTransient;
        boolean isWriterFaultUnexpected;
        int iteration;
        long readOnlyCalls;
        long readOnlyFromCall;
        int refusedInsertCall;
        String[] tables;
        long writerCalls;
        long writerFaultFromCall;
        // The index of the table whose writer acquisitions fail, or -1.
        int writerFaultTable;

        @Override
        public String toString() {
            return "iteration=" + iteration + ", frames=" + frameValues.size() + ", dataErrorsOnly=" + isDataErrorsOnly
                    + ", durable=" + isDurable + ", cap=" + cap + ", readOnlyFrom=" + readOnlyFromCall
                    + ", readOnlyTransient=" + isReadOnlyTransient + ", refusedInsert=" + refusedInsertCall
                    + ", refusalPersistent=" + isRefusalPersistent + ", writerFaultTable=" + writerFaultTable
                    + ", writerFaultFrom=" + writerFaultFromCall + ", writerFaultTransient=" + isWriterFaultTransient
                    + ", writerFaultUnexpected=" + isWriterFaultUnexpected + ", kinds=" + kinds;
        }

        // I4's bound on a table's commits in an iteration whose only faults are data errors,
        // at the cap of 1,000,000: S + 1 + S_t + A_t over the frames before the first bad
        // frame (see testCommitBatchAckContractFuzz).
        long commitBound(int table) {
            int switchingFrames = 0;
            int switchingFramesOfTable = 0;
            int newColumnsOfTable = 0;
            for (int s = 0, end = goodFrameCount(); s < end; s++) {
                final IntList blocks = frameBlocks.getQuick(s);
                boolean isSwitching = false;
                boolean hasTable = false;
                for (int b = 0, n = blocks.size(); b < n; b += 2) {
                    final int blockTable = blocks.getQuick(b);
                    final int schema = blocks.getQuick(b + 1);
                    final int previousSchema = schemaBefore(s, b, blockTable);
                    isSwitching |= previousSchema > -1 && previousSchema != schema;
                    hasTable |= blockTable == table;
                    if (blockTable == table && schema >= FUZZ_BLOCK_NEW_COLUMN) {
                        newColumnsOfTable++;
                    }
                }
                if (isSwitching) {
                    switchingFrames++;
                    if (hasTable) {
                        switchingFramesOfTable++;
                    }
                }
            }
            return switchingFrames + 1 + switchingFramesOfTable + newColumnsOfTable;
        }

        // The first frame that fails on its own data, or -1.
        int firstBadFrame() {
            for (int s = 0, n = frameErrorStatus.size(); s < n; s++) {
                if (frameErrorStatus.getQuick(s) >= 0) {
                    return s;
                }
            }
            return -1;
        }

        // A data-errors-only iteration at the cap of 1,000,000 whose first bad frame, with a
        // batch pending, fails on its second block after its first block appended.
        boolean hasBadSecondBlockAfterAppend() {
            final int bad = firstBadFrame();
            return isDataErrorsOnly && cap == 1_000_000 && bad > 0
                    && frameKinds.getQuick(bad) == FUZZ_BAD_SECOND_BLOCK
                    && isBatchPendingAt(bad)
                    && isFirstBlockSettled(bad);
        }

        // A data-errors-only iteration at the cap of 1,000,000 whose first bad frame fails on
        // its only block, of fc, which the frame before it put in the batch.
        boolean hasBatchTableFirstBlockFailure() {
            final int bad = firstBadFrame();
            return isDataErrorsOnly && cap == 1_000_000 && bad > 0
                    && frameKinds.getQuick(bad) == FUZZ_SCHEMA_MISMATCH
                    && frameBlocks.getQuick(bad).getQuick(0) == FUZZ_VARCHAR_TABLE
                    && isBatchPendingAt(bad)
                    && frameBlocks.getQuick(bad - 1).getQuick(0) == FUZZ_VARCHAR_TABLE;
        }

        // A frame before the first bad frame, at the cap of 1,000,000, whose first block puts
        // a table in the batch and whose second block needs a new column on that table.
        boolean hasNewColumnSecondBlockOnBatchTable() {
            if (cap != 1_000_000) {
                return false;
            }
            for (int s = 0, end = goodFrameCount(); s < end; s++) {
                final IntList blocks = frameBlocks.getQuick(s);
                if (frameKinds.getQuick(s) == FUZZ_NEW_COLUMN_SECOND_BLOCK
                        && blocks.getQuick(0) == blocks.getQuick(2)
                        && isFirstBlockSettled(s)) {
                    return true;
                }
            }
            return false;
        }

        // A data-errors-only iteration at the cap of 1,000,000 whose first bad frame, with a
        // batch pending, fails on its only block, of a table no frame before it wrote.
        boolean hasNewTableFirstBlockFailure() {
            final int bad = firstBadFrame();
            return isDataErrorsOnly && cap == 1_000_000 && bad > 0
                    && frameKinds.getQuick(bad) == FUZZ_SCHEMA_MISMATCH
                    && isBatchPendingAt(bad)
                    && schemaBefore(bad, 0, frameBlocks.getQuick(bad).getQuick(0)) == -1;
        }

        // The shape of testCommitBatchFuzzPinnedRewindSeed: hasBadSecondBlockAfterAppend(),
        // and a table written by more frames before the bad frame than I4 lets it commit.
        boolean hasPinnedRewindShape() {
            if (!hasBadSecondBlockAfterAppend()) {
                return false;
            }
            final int bad = firstBadFrame();
            for (int t = 0; t < tables.length; t++) {
                int frames = 0;
                for (int s = 0; s < bad; s++) {
                    final IntList blocks = frameBlocks.getQuick(s);
                    boolean hasTable = false;
                    for (int b = 0, n = blocks.size(); b < n; b += 2) {
                        hasTable |= blocks.getQuick(b) == t;
                    }
                    if (hasTable) {
                        frames++;
                    }
                }
                if (frames >= 3 && commitBound(t) < frames) {
                    return true;
                }
            }
            return false;
        }

        // An iteration at the cap of 1,000,000 whose writer acquisitions of a table fail
        // unexpectedly or with a CairoException, as asked, with a frame whose second block
        // takes its first block's rows back out when it meets a batch.
        boolean hasWriterFaultWithRewindTrigger(boolean isUnexpected) {
            if (writerFaultTable < 0 || isWriterFaultUnexpected != isUnexpected || cap != 1_000_000) {
                return false;
            }
            for (int s = 0, n = frameKinds.size(); s < n; s++) {
                final int kind = frameKinds.getQuick(s);
                if (kind == FUZZ_BAD_SECOND_BLOCK || kind == FUZZ_NEW_COLUMN_SECOND_BLOCK) {
                    return true;
                }
            }
            return false;
        }

        boolean isCommitFaulty() {
            return refusedInsertCall > 0 || readOnlyFromCall > 0;
        }

        // The number of frames before the first bad frame, or of all frames.
        private int goodFrameCount() {
            final int bad = firstBadFrame();
            return bad < 0 ? frameValues.size() : bad;
        }

        // Whether the frame before s is a good one-block frame whose block could join a
        // batch, which is then pending at s when the iteration's only faults are data errors.
        private boolean isBatchPendingAt(int s) {
            return s > 0
                    && frameKinds.getQuick(s - 1) == FUZZ_PLAIN
                    && frameErrorStatus.getQuick(s - 1) < 0
                    && isFirstBlockSettled(s - 1);
        }

        // Whether frame s's first block writes its table the schema of the table's previous
        // block, or the table has none, so the block can join a batch.
        private boolean isFirstBlockSettled(int s) {
            final IntList blocks = frameBlocks.getQuick(s);
            final int previousSchema = schemaBefore(s, 0, blocks.getQuick(0));
            return previousSchema == -1 || previousSchema == blocks.getQuick(1);
        }

        // The schema of the table's last block before block b of frame s, or -1 when none.
        private int schemaBefore(int s, int b, int table) {
            int frame = s;
            int block = b - 2;
            while (frame > -1) {
                final IntList blocks = frameBlocks.getQuick(frame);
                for (; block > -1; block -= 2) {
                    if (blocks.getQuick(block) == table) {
                        return blocks.getQuick(block + 1);
                    }
                }
                if (--frame > -1) {
                    block = frameBlocks.getQuick(frame).size() - 2;
                }
            }
            return -1;
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
