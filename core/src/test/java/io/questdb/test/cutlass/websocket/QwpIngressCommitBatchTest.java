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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.wal.DurableAckRegistry;
import io.questdb.cairo.wal.WalUtils;
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
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.cairo.Overrides;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.cairo.wal.WalFlushCountingFacade;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
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
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;

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
    // Seeds of testCommitBatchFuzzGeneratesPrepareFaults' fault Rnd.
    private static final long FUZZ_PREPARE_FAULT_SEED0 = 12_345L;
    private static final long FUZZ_PREPARE_FAULT_SEED1 = 67_890L;
    private static final int FUZZ_ROWLESS_DEFERRED = 10;
    private static final int FUZZ_SCHEMA_MISMATCH = 6;
    // Seeds of testCommitBatchFuzzGeneratesRewindShapes.
    private static final long FUZZ_SHAPES_SEED0 = 253_408L;
    private static final long FUZZ_SHAPES_SEED1 = 3_351_329L;
    private static final int FUZZ_TWO_BLOCKS = 4;
    // The index of fuzz table fc, which takes v as a VARCHAR of digits in every frame.
    private static final int FUZZ_VARCHAR_TABLE = 2;
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
    private final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);

    @Test
    public void testBatchCommitFlushesNoPrivateFileUnderLockAdaptive() throws Exception {
        assertBatchCommitFlushesNoPrivateFileUnderLock(CommitMode.ADAPTIVE, 50_000);
    }

    @Test
    public void testBatchCommitFlushesNoPrivateFileUnderLockAdaptiveNoWindow() throws Exception {
        assertBatchCommitFlushesNoPrivateFileUnderLock(CommitMode.ADAPTIVE, 0);
    }

    @Test
    public void testBatchCommitFlushesNoPrivateFileUnderLockSync() throws Exception {
        assertBatchCommitFlushesNoPrivateFileUnderLock(CommitMode.SYNC, 0);
    }

    @Test
    public void testBatchCommitTakesRoleSwitchLockOnce() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b", "c");
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
     * Frames a, b, a, then one needing a new column of a; b's commit fails after a's: frame 1 is NACKed.
     */
    @Test
    public void testBoundaryCommitFailureOnLaterTableNacksItsFirstFrame() throws Exception {
        assertLaterTableCommitFailureNacksItsFirstFrame(true);
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
    public void testCommitBatchAckContractFuzzAdaptive() throws Exception {
        assertCommitBatchAckContractFuzz(CommitMode.ADAPTIVE, 50_000, 150);
    }

    @Test
    public void testCommitBatchAckContractFuzzAdaptiveNoWindow() throws Exception {
        assertCommitBatchAckContractFuzz(CommitMode.ADAPTIVE, 0, 60);
    }

    @Test
    public void testCommitBatchAckContractFuzzAsync() throws Exception {
        assertCommitBatchAckContractFuzz(CommitMode.ASYNC, 50_000, 150);
    }

    @Test
    public void testCommitBatchAckContractFuzzNosync() throws Exception {
        assertCommitBatchAckContractFuzz(CommitMode.NOSYNC, 50_000, 150);
    }

    @Test
    public void testCommitBatchAckContractFuzzSync() throws Exception {
        // SYNC fdatasyncs every commit: fewer iterations keep the leg within the CI budget.
        assertCommitBatchAckContractFuzz(CommitMode.SYNC, 0, 60);
    }

    /**
     * From fixed seeds of the fault Rnd, the fuzz draws prepare faults, transient and persistent, on iterations
     * with a commit batch of more than one table, under a mode that prepares.
     */
    @Test
    public void testCommitBatchFuzzGeneratesPrepareFaults() {
        final Rnd rnd = new Rnd(FUZZ_SHAPES_SEED0, FUZZ_SHAPES_SEED1);
        final Rnd faultRnd = new Rnd(FUZZ_PREPARE_FAULT_SEED0, FUZZ_PREPARE_FAULT_SEED1);
        int transientFaults = 0;
        int persistentFaults = 0;
        for (int iteration = 0; iteration < 60; iteration++) {
            final FuzzIteration it = generateCommitBatchFuzzIteration(rnd, iteration);
            drawPrepareFault(faultRnd, it);
            if (it.prepareFaultTable > -1 && it.hasPlainFrameOfOtherTableThan(it.prepareFaultTable)) {
                if (it.isPrepareFaultTransient) {
                    transientFaults++;
                } else {
                    persistentFaults++;
                }
            }
        }
        Assert.assertTrue("a transient prepare fault in a multi-table batch", transientFaults > 0);
        Assert.assertTrue("a persistent prepare fault in a multi-table batch", persistentFaults > 0);
    }

    /**
     * From fixed seeds, the fuzz generator produces every shape that ends a batch at a failed frame.
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
     * A fuzz seed whose first bad frame fails on its second block after the first appended: guards I2 and I4.
     */
    @Test
    public void testCommitBatchFuzzPinnedRewindSeed() throws Exception {
        assertMemoryLeak(() -> {
            final FuzzIteration it = generateCommitBatchFuzzIteration(new Rnd(FUZZ_PINNED_SEED0, FUZZ_PINNED_SEED1), 0);
            Assert.assertTrue("test setup: the seed must generate the pinned rewind shape [" + it + ']', it.hasPinnedRewindShape());
            final ObjList<FuzzIteration> iterations = new ObjList<>();
            iterations.add(it);
            runCommitBatchFuzz(iterations, CommitMode.fromString(Overrides.getTestCommitMode()), 50_000);
        });
    }

    /**
     * Each table commits once, and one ack covers every frame.
     */
    @Test
    public void testCommitBatchPreparesOnlyUnderSyncAndAdaptive() throws Exception {
        assertMemoryLeak(() -> {
            final int[] modes = {CommitMode.SYNC, CommitMode.ADAPTIVE, CommitMode.ADAPTIVE, CommitMode.NOSYNC, CommitMode.ASYNC};
            final long[] windows = {0, 50_000, 0, 0, 0};
            for (int m = 0; m < modes.length; m++) {
                final String a = "pa" + m;
                final String b = "pb" + m;
                createTables(a, b);
                try (CairoEngine pinnedEngine = new CairoEngine(pinnedConfiguration(modes[m], windows[m], TestFilesFacadeImpl.INSTANCE))) {
                    assertPinnedMode(pinnedEngine, modes[m], windows[m]);
                    final boolean isPrepared = modes[m] == CommitMode.SYNC || modes[m] == CommitMode.ADAPTIVE;
                    final boolean[] wasPreparedInHook = {false};
                    final ObjList<byte[]> sent;
                    try (Connection connection = new Connection(
                            pinnedEngine,
                            httpConfig,
                            AllowAllSecurityContext.INSTANCE,
                            false,
                            createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(a, 100L, 1_000_000L)),
                            createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(b, 200L, 2_000_000L)),
                            createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(a, 300L, 3_000_000L))
                    )) {
                        final QwpIngressProcessorState state = connection.getState();
                        state.setAfterPreparePhaseHook(() -> wasPreparedInHook[0] = state.getCommitBatchWriter(0).isCommitPrepared()
                                && state.getCommitBatchWriter(1).isCommitPrepared());
                        connection.deliverFrames(3);
                        sent = connection.sentFrames();
                        Assert.assertEquals(CommitMode.toString(modes[m]), isPrepared ? 1 : 0, state.getPreparePhaseCount());
                        Assert.assertEquals(isPrepared, wasPreparedInHook[0]);
                    }
                    Assert.assertEquals("one ack covers the pass", 2, maxCumulativeOkAck(sent));
                    Assert.assertEquals(1, lastTxn(pinnedEngine, a));
                    Assert.assertEquals(1, lastTxn(pinnedEngine, b));
                    drainWalQueue(pinnedEngine);
                    assertHookQuery(pinnedEngine, "SELECT v FROM " + a).expectSize().returns("v\n100\n300\n");
                    assertHookQuery(pinnedEngine, "SELECT v FROM " + b).expectSize().returns("v\n200\n");
                }
            }
        });
    }

    @Test
    public void testCommitBatchSpansInterleavedTables() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
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
     * b's commit fails after a's: frame 0 is acked, frame 1 NACKed, and the replay sends frame 2's row of a again.
     */
    @Test
    public void testCommitFailureOnLaterTableAcksFramesBeforeItsFirstFrame() throws Exception {
        assertLaterTableCommitFailureNacksItsFirstFrame(false);
    }

    /**
     * A deferred frame starts a client transaction: the commit batch before it
     * must commit on its own, never inside the client's group.
     */
    @Test
    public void testDeferredFrameCommitsPendingBatchFirst() throws Exception {
        assertMemoryLeak(() -> {
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
     * The batch reads the role once, before any table commits: nothing commits or is acked, and the CLOSE follows.
     */
    @Test
    public void testDemoteAfterPreparePhaseCommitsNothingAdaptive() throws Exception {
        assertDemoteAfterPreparePhaseCommitsNothing(CommitMode.ADAPTIVE, 50_000, false);
    }

    @Test
    public void testDemoteAfterPreparePhaseCommitsNothingAdaptiveWithDurableAck() throws Exception {
        assertDemoteAfterPreparePhaseCommitsNothing(CommitMode.ADAPTIVE, 50_000, true);
    }

    @Test
    public void testDemoteAfterPreparePhaseCommitsNothingSync() throws Exception {
        assertDemoteAfterPreparePhaseCommitsNothing(CommitMode.SYNC, 0, false);
    }

    @Test
    public void testDemoteAfterPreparePhaseCommitsNothingSyncWithDurableAck() throws Exception {
        assertDemoteAfterPreparePhaseCommitsNothing(CommitMode.SYNC, 0, true);
    }

    @Test
    public void testDemoteAtBatchCommitCommitsNoTable() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
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
     * A frame appends to a and fails on b, and a demote refuses the boundary commit: only the CLOSE goes out.
     */
    @Test
    public void testDemoteAtBoundaryCommitAfterRewindCommitsNothing() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
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

    @Test
    public void testDemoteBetweenTablesOfGroupClosingFrameClosesAtOnce() throws Exception {
        assertDemoteBetweenTablesOfGroupClosingFrame(false);
    }

    @Test
    public void testDemoteBetweenTablesOfGroupClosingFrameDefersClose() throws Exception {
        assertDemoteBetweenTablesOfGroupClosingFrame(true);
    }

    /**
     * Another session drops b while it waits in the batch: a commits, c does not, and b's first frame is NACKed.
     */
    @Test
    public void testDemoteDuringPreparePhaseCommitsNothing() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b", "c");
            final EventFlushFaultFacade ff = new EventFlushFaultFacade();
            try (DemotableEngine demotableEngine = new DemotableEngine(pinnedConfiguration(CommitMode.SYNC, 0, ff))) {
                assertPinnedMode(demotableEngine, CommitMode.SYNC, 0);
                final int[] walIds = warmWriters(demotableEngine, "a", "b", "c");
                // The demote publishes REPLICA as table b's prepare flushes its event file.
                ff.onEventFlush("b~", () -> demotableEngine.isReadOnly.set(true));
                final ObjList<byte[]> sent;
                try (Connection connection = new Connection(
                        demotableEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("c", 300L, 3_000_000L))
                )) {
                    final boolean[] isCPrepared = {false};
                    connection.getState().setAfterPreparePhaseHook(() -> isCPrepared[0] = connection.getState().getCommitBatchWriter(2).isCommitPrepared());
                    connection.deliverFrames(3);
                    Assert.assertTrue("test setup: the demote must land in the prepare phase", ff.hasFired);
                    Assert.assertTrue("the prepare phase finishes after the demote", isCPrepared[0]);
                    sent = connection.sentFrames();
                }
                Assert.assertEquals("only the CLOSE", 1, sent.size());
                Assert.assertEquals(WebSocketCloseCode.NORMAL_CLOSURE, closeCode(sent.getQuick(0)));
                Assert.assertEquals(-1, maxCumulativeOkAck(sent));
                demotableEngine.isReadOnly.set(false);
                assertWritersReplaced(demotableEngine, walIds, "a", "b", "c");
                for (String table : new String[]{"a", "b", "c"}) {
                    Assert.assertEquals(table + " commits nothing", 0, lastTxn(demotableEngine, table));
                }
            }
        });
    }

    @Test
    public void testDroppedTableInBatchCommitsTablesBeforeIt() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b", "c");
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
                final long aBefore = lastTxn(hookEngine, "a");
                final long cBefore = lastTxn(hookEngine, "c");
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
                        lastTxn(hookEngine, "a"));
                Assert.assertEquals("c must not publish a WAL transaction", cBefore,
                        lastTxn(hookEngine, "c"));
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
    public void testDueGroupFlushRunsAfterTheUnlock() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
            final WalFlushCountingFacade ff = new WalFlushCountingFacade();
            final long[] frontierAtUnlock = {-2, -2};
            setCurrentMicros(1_000_000_000L);
            try (CairoEngine lockEngine = new CairoEngine(pinnedConfiguration(CommitMode.ADAPTIVE, 50_000, ff)) {
                private Lock readLock;

                @Override
                public Lock getRoleSwitchReadLock() {
                    if (readLock == null) {
                        final Lock delegate = super.getRoleSwitchReadLock();
                        final CairoEngine self = this;
                        readLock = new ForwardingLock(delegate) {
                            @Override
                            public void unlock() {
                                frontierAtUnlock[0] = self.getTableSequencerAPI().getTxnTracker(self.verifyTableName("a")).getLocalDurableSeqTxn();
                                frontierAtUnlock[1] = self.getTableSequencerAPI().getTxnTracker(self.verifyTableName("b")).getLocalDurableSeqTxn();
                                super.unlock();
                            }
                        };
                    }
                    return readLock;
                }
            }) {
                assertPinnedMode(lockEngine, CommitMode.ADAPTIVE, 50_000);
                final ObjList<byte[]> sent;
                try (Connection connection = new Connection(
                        lockEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 400L, 4_000_000L))
                )) {
                    // The first pass starts each writer's batch; the second, a window later, makes it due.
                    connection.deliverFrames(2);
                    final long frontierBefore = lockEngine.getTableSequencerAPI().getTxnTracker(lockEngine.verifyTableName("a")).getLocalDurableSeqTxn();
                    Assert.assertTrue("the first pass's batch is pending", frontierBefore < 1);
                    setCurrentMicros(1_000_000_000L + 50_000);
                    ff.arm();
                    connection.deliverFrames(2);
                    ff.disarm();
                    sent = connection.sentFrames();
                    Assert.assertEquals("the frontier waits for the flush after the unlock", frontierBefore, frontierAtUnlock[0]);
                    Assert.assertEquals(frontierBefore, frontierAtUnlock[1]);
                    Assert.assertEquals(2, lockEngine.getTableSequencerAPI().getTxnTracker(lockEngine.verifyTableName("a")).getLocalDurableSeqTxn());
                    Assert.assertEquals(2, lockEngine.getTableSequencerAPI().getTxnTracker(lockEngine.verifyTableName("b")).getLocalDurableSeqTxn());
                }
                Assert.assertEquals(3, maxCumulativeOkAck(sent));
                Assert.assertEquals(ff.getLog(), 0, ff.getCount(WalFlushCountingFacade.SEQUENCER, true));
                Assert.assertEquals(ff.getLog(), 0, ff.getCount(WalFlushCountingFacade.PRIVATE, true));
                Assert.assertEquals("one batch flush per table, after the unlock: " + ff.getLog(), 2, ff.getCount(WalFlushCountingFacade.SEQUENCER, false));
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
            final HttpFullFatServerConfiguration customConfig = httpConfigWithMaxUncommittedRows(2);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long txnBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("tab"));
            final byte[] mismatchedBlock = varcharTableBlock("tab", "not-a-long", 4_000_000L);

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, customConfig);
            ObjList<byte[]> sent = ingestDurableInOnePass(
                    processor,
                    customConfig,
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
     * Frames that name no table commit nothing, take no lock, and are all acked.
     */
    @Test
    public void testEmptyFramesAloneAreAcked() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<byte[]> sent = ingestInOnePass(
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
     * A failed block of VARCHAR, STRING, BINARY and DOUBLE[] values rewinds each column to the batch's rows.
     */
    @Test
    public void testFailedDueGroupFlushKeepsTheAckAndNacksTheTablesNextFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
            final SequencerFlushFaultFacade ff = new SequencerFlushFaultFacade();
            final LogCapture capture = new LogCapture();
            setCurrentMicros(1_000_000_000L);
            try (CairoEngine pinnedEngine = new CairoEngine(pinnedConfiguration(CommitMode.ADAPTIVE, 50_000, ff))) {
                assertPinnedMode(pinnedEngine, CommitMode.ADAPTIVE, 50_000);
                final ObjList<byte[]> sent;
                try (Connection connection = new Connection(
                        pinnedEngine,
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
                    setCurrentMicros(1_000_000_000L + 50_000);
                    // a's due flush, after the unlock, fails
                    ff.failNextOutsideLock("a~");
                    capture.start();
                    try {
                        connection.deliverFrames(2);
                        capture.waitFor("group flush failed after a commit batch");
                    } finally {
                        capture.stop();
                    }
                    Assert.assertEquals(1, ff.failures);
                    Assert.assertEquals("the sequenced rows are acked", 3, maxCumulativeOkAck(connection.sentFrames()));
                    Assert.assertFalse(hasErrorResponseForSeq(connection.sentFrames(), 2));
                    Assert.assertFalse(pinnedEngine.isDurabilityFailed());
                    // The table's next frame finds the distressed writer and is refused retriably.
                    connection.deliverFrames(1);
                    Assert.assertEquals(QwpConstants.STATUS_INTERNAL_ERROR, errorStatusForSeq(connection.sentFrames(), 4));
                    TestUtils.assertContains(errorMessageForSeq(connection.sentFrames(), 4), "WAL writer is distressed");
                }
                // The client's replay of the frame gets a fresh writer.
                try (Connection connection = new Connection(
                        pinnedEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 500L, 5_000_000L))
                )) {
                    connection.deliverFrames(1);
                    sent = connection.sentFrames();
                }
                Assert.assertEquals(0, maxCumulativeOkAck(sent));
                drainWalQueue(pinnedEngine);
                assertHookQuery(pinnedEngine, "SELECT v FROM a").expectSize().returns("v\n100\n300\n500\n");
                assertHookQuery(pinnedEngine, "SELECT v FROM b").expectSize().returns("v\n200\n400\n");
            }
        });
    }

    @Test
    public void testFailedFirstBlockAppendRewindsEveryColumnType() throws Exception {
        assertFailedFrameRewindsEveryColumnType(false);
    }

    /**
     * A two-block frame fails after appending next to batch rows that follow an earlier commit in the segment.
     */
    @Test
    public void testFailedFrameAfterCommitInSegmentDropsOnlyItsOwnRows() throws Exception {
        assertMemoryLeak(() -> {
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
     * The pass's first frame appends to a and then fails on b: its row goes back out and nothing commits.
     */
    @Test
    public void testFailedFrameOpeningBatchCommitsNothing() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
            final long aBefore = lastTxn(engine, "a");
            final long bBefore = lastTxn(engine, "b");
            final ObjList<byte[]> sent = ingestInOnePass(
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
     * Like the client's flush, a frame appends to the batch's table and then fails on another table's block.
     */
    @Test
    public void testFailedMultiTableFrameDropsItsBlockFromBatchTable() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "tab");
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
     * Frame [n, bad n]: n's writer returns to its empty savepoint, n leaves the batch, and a commits once.
     */
    @Test
    public void testFailedSecondBlockOfNewTableRewindsItsFirstBlock() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "n");
            final long aBefore = lastTxn(engine, "a");
            final long nBefore = lastTxn(engine, "n");
            final ObjList<byte[]> sent = ingestInOnePass(
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
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("n", 400L, 4_000_000L))
            );
            Assert.assertEquals(0, maxCumulativeOkAck(sentAfter));
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n200\n");
            assertQuery("SELECT v FROM n").noLeakCheck().expectSize().returns("v\n400\n");
        });
    }

    /**
     * The failing frame's first block appends every column type to the batch's table, and its second block fails.
     */
    @Test
    public void testFailedSecondBlockRewindsEveryColumnType() throws Exception {
        assertFailedFrameRewindsEveryColumnType(true);
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
     * The failed block's cancel cannot remap the column's first page: the batch is lost, the frame NACKed.
     */
    @Test
    public void testFatalPrepareFailurePoisonsEngineAndReplayStoresRowsOnce() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b", "c");
            final EventFlushFaultFacade ff = new EventFlushFaultFacade();
            try (CairoEngine pinnedEngine = new CairoEngine(pinnedConfiguration(CommitMode.SYNC, 0, ff))) {
                assertPinnedMode(pinnedEngine, CommitMode.SYNC, 0);
                final byte[][] frames = {
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("c", 300L, 3_000_000L))
                };
                warmWriters(pinnedEngine, "a", "b", "c");
                ff.failEventFlush("b~", true);
                try (Connection connection = new Connection(pinnedEngine, httpConfig, AllowAllSecurityContext.INSTANCE, false, frames)) {
                    connection.deliverFrames(3);
                    Assert.assertTrue(ff.hasFired);
                    Assert.assertTrue("a data-sync failure under SYNC stops the engine", pinnedEngine.isDurabilityFailed());
                    Assert.assertEquals(-1, maxCumulativeOkAck(connection.sentFrames()));
                }
                for (String table : new String[]{"a", "b", "c"}) {
                    Assert.assertEquals(table + " sequences nothing", 0, lastTxn(pinnedEngine, table));
                }
            }
            // A new engine on the root, as after the restart that follows the halt, takes the client's replay.
            try (CairoEngine restarted = new CairoEngine(pinnedConfiguration(CommitMode.SYNC, 0, TestFilesFacadeImpl.INSTANCE))) {
                try (Connection connection = new Connection(
                        restarted,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("c", 300L, 3_000_000L))
                )) {
                    connection.deliverFrames(3);
                    Assert.assertEquals(2, maxCumulativeOkAck(connection.sentFrames()));
                }
                drainWalQueue(restarted);
                assertHookQuery(restarted, "SELECT v FROM a").expectSize().returns("v\n100\n");
                assertHookQuery(restarted, "SELECT v FROM b").expectSize().returns("v\n200\n");
                assertHookQuery(restarted, "SELECT v FROM c").expectSize().returns("v\n300\n");
            }
        });
    }

    @Test
    public void testFirstBlockCancelFailureLosesBatch() throws Exception {
        assertMemoryLeak(() -> {
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
     * With column auto-creation off, a block naming a missing column is refused before any row appends.
     */
    @Test
    public void testFirstBlockColumnRefusalCommitsBatchOnce() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration customConfig = httpConfigWithoutNewColumns();
            createTables("a", "b");
            final long aBefore = lastTxn(engine, "a");
            final ObjList<byte[]> sent = ingestInOnePass(
                    customConfig,
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
     * The failed block's new symbol stays in the writer's symbol table and reads back on a fresh connection.
     */
    @Test
    public void testFirstBlockFailureAfterNewSymbolKeepsBatchAndSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long sBefore = lastTxn(engine, "s");
            final GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            final byte[][] frames = new byte[6][];
            for (int i = 0; i < 5; i++) {
                frames[i] = symbolVarcharFrame(dict, i == 0 ? -1 : 0, "s", "x", Long.toString(100L + i), (i + 1) * 1_000_000L);
            }
            frames[5] = symbolVarcharFrame(dict, 0, "s", "z", "not-a-long", 6_000_000L);
            final ObjList<byte[]> sent = ingestInOnePass(frames);
            Assert.assertEquals("the batch commits once", sBefore + 1, lastTxn(engine, "s"));
            Assert.assertEquals(4, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, 5));

            final ObjList<byte[]> sentAfter = ingestInOnePass(
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
     * A VARCHAR that is not a number fails the block; its cancel leaves the writer with the batch's rows.
     */
    @Test
    public void testFirstBlockFailureOfBatchTableCommitsBatchOnce() throws Exception {
        assertMemoryLeak(() -> {
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
            final ObjList<byte[]> sent = ingestInOnePass(frames);
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
     * b refuses a VARCHAR value for its LONG column; the batch is acked before the frame's NACK.
     */
    @Test
    public void testFirstBlockFailureOfTableNewToBatchCommitsBatchOnce() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
            final long aBefore = lastTxn(engine, "a");
            final long bBefore = lastTxn(engine, "b");
            final ObjList<byte[]> sent = ingestInOnePass(
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
     * With no batch pending, a frame failing on its only block is NACKed and nothing commits.
     */
    @Test
    public void testFirstBlockFailureOpeningBatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long bBefore = lastTxn(engine, "b");
            final ObjList<byte[]> sent = ingestInOnePass(
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
     * An unexpected permission failure for a new column distresses the cache: nothing commits.
     */
    @Test
    public void testFirstBlockUnexpectedErrorLosesBatch() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
            final SecurityContext failingAddColumn = new AllowAllSecurityContext() {
                @Override
                public void authorizeAlterTableAddColumn(TableToken tableToken) {
                    throw new IllegalStateException("test: unexpected permission check failure");
                }
            };
            final long aBefore = lastTxn(engine, "a");
            final ObjList<byte[]> sent = ingestInOnePass(
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
     * A frame's second block needs a new column: its first goes back out, the batch commits, then the frame.
     */
    @Test
    public void testFrameThatOnlyCouldNotJoinCommitsAfterBatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            ObjList<byte[]> sent = ingestInOnePass(
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
     * Over 1 MiB of one-row frames alternating a and b, then a frame that appends to a and fails on b.
     */
    @Test
    public void testFullReceivePassRewindCommitsEachTableOnce() throws Exception {
        // Without fsync, so that a regression to one commit per frame stays fast to detect.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            createTables("a", "b");
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
     * A demote at the gate's second check, before the pass's first frame opens a batch: CLOSE, nothing commits.
     */
    @Test
    public void testGateRefusalBeforeOpeningBatchClosesWithoutCommit() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger readOnlyChecks = new AtomicInteger();
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
     * After b's column is renamed, b's frames resolve again, auto-creating v, while a's keep their resolution.
     */
    @Test
    public void testInterleavedTablesFollowColumnRenameBetweenPasses() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
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
     * A table re-created with another column order between passes: its frames resolve against the new table.
     */
    @Test
    public void testInterleavedTablesFollowRecreatedTableBetweenPasses() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
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
     * Cap 4: [a], [b], [a], then [a, 3 rows of b] would reach it; the frame rewinds and opens a new batch.
     */
    @Test
    public void testLaterBlockReachingCapRewindsBatchTable() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration customConfig = httpConfigWithMaxUncommittedRows(4);
            createTables("a", "b");
            final long aBefore = lastTxn(engine, "a");
            final long bBefore = lastTxn(engine, "b");
            final ObjList<byte[]> sent = ingestInOnePass(
                    customConfig,
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
     * The connection drops without a CLOSE, so no ack can reach the client and the batch must not commit.
     */
    @Test
    public void testMalformedWebSocketFrameRollsBackMultiTableBatch() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
            final long aBefore = lastTxn(engine, "a");
            final long bBefore = lastTxn(engine, "b");
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
            Assert.assertEquals(aBefore, lastTxn(engine, "a"));
            Assert.assertEquals(bBefore, lastTxn(engine, "b"));
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
     * b, new to the batch, fails a block whose cancel cannot remap its column: a commits without b.
     */
    @Test
    public void testNewTableCancelFailureCommitsBatchWithoutIt() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
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
     * Frames a0 b1 c2 a3 b4 c5: the refused table's first frame ends the batch, and the frames before it commit.
     */
    @Test
    public void testPermissionRefusalNacksRefusedTablesFirstFrame() throws Exception {
        assertMemoryLeak(() -> {
            for (int deniedTable = 0; deniedTable < 3; deniedTable++) {
                final String[] tables = {"a" + deniedTable, "b" + deniedTable, "c" + deniedTable};
                final long[] txnBefore = new long[tables.length];
                for (int t = 0; t < tables.length; t++) {
                    execute("CREATE TABLE " + tables[t] + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    txnBefore[t] = lastTxn(engine, tables[t]);
                }
                final byte[][] frames = new byte[6][];
                for (int f = 0; f < frames.length; f++) {
                    frames[f] = createMaskedFrame(
                            WebSocketOpcode.BINARY,
                            oneRowMessage(tables[f % tables.length], (f + 1) * 100L, (f + 1) * 1_000_000L)
                    );
                }
                final ObjList<byte[]> sent = ingestInOnePass(
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
                            lastTxn(engine, tables[t]));
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
     * The refused table's first rows come in a second block, after the first appended to a batch table.
     */
    @Test
    public void testPermissionRefusalOfSecondBlockCommitsFramesBefore() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b", "c");
            final long aBefore = lastTxn(engine, "a");
            final long bBefore = lastTxn(engine, "b");
            final long cBefore = lastTxn(engine, "c");
            final ObjList<byte[]> sent = ingestInOnePass(
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
     * A read-only refusal of a new column, as in a demote: the batch is acked before the role-change CLOSE.
     */
    @Test
    public void testPhaseOneRoleChangeRefusalCommitsBatchBeforeClose() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
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
            createTables("a", "b");

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
     * The batch commits at the group's first deferred frame, and the frame after the group opens a new batch.
     */
    @Test
    public void testPlainFrameClosingDeferredGroupCommitsAlone() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            ObjList<byte[]> sent = ingestInOnePass(
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, deferred(oneRowMessage("a", 200L, 2_000_000L))),
                    createMaskedFrame(WebSocketOpcode.BINARY, deferred(oneRowMessage("a", 300L, 3_000_000L))),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 400L, 4_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 500L, 5_000_000L))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n200\n300\n400\n500\n");
            Assert.assertEquals("the batch, the group, the new batch",
                    aBefore + 3, lastTxn(engine, "a"));
            Assert.assertEquals(4, maxCumulativeOkAck(sent));
            Assert.assertTrue("the group's closing frame flushes its ack", indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 3) >= 0);
        });
    }

    /**
     * A frame appends to a, then its lookup of a new table meets a demote: the batch is acked before the CLOSE.
     */
    @Test
    public void testPrepareFailureAfterDemoteClosesForTheRoleChange() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b", "c");
            final EventFlushFaultFacade ff = new EventFlushFaultFacade();
            try (DemotableEngine demotableEngine = new DemotableEngine(pinnedConfiguration(CommitMode.ADAPTIVE, 50_000, ff))) {
                assertPinnedMode(demotableEngine, CommitMode.ADAPTIVE, 50_000);
                warmWriters(demotableEngine, "a", "b", "c");
                // A demote publishes REPLICA, then b's prepare fails with an error that is not fatal there.
                ff.onEventFlush("b~", () -> demotableEngine.isReadOnly.set(true));
                ff.failEventFlush("b~", false);
                final ObjList<byte[]> sent;
                try (Connection connection = new Connection(
                        demotableEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("c", 300L, 3_000_000L))
                )) {
                    connection.deliverFrames(3);
                    Assert.assertTrue(ff.hasFired);
                    sent = connection.sentFrames();
                }
                Assert.assertEquals("the role-change close, not the error NACK", 1, sent.size());
                Assert.assertEquals(WebSocketCloseCode.NORMAL_CLOSURE, closeCode(sent.getQuick(0)));
                Assert.assertFalse(hasErrorResponseForSeq(sent, 0));
                for (String table : new String[]{"a", "b", "c"}) {
                    Assert.assertEquals(table + " commits nothing", 0, lastTxn(demotableEngine, table));
                }
            }
        });
    }

    @Test
    public void testPrepareFailureAtFirstTableRefusesWholeBatch() throws Exception {
        assertPrepareFailureRefusesWholeBatch(0, "test: prepare failed");
    }

    @Test
    public void testPrepareFailureAtLastTableRefusesWholeBatch() throws Exception {
        // a message the NACK truncates
        assertPrepareFailureRefusesWholeBatch(2, "test: " + "x".repeat(100_000));
    }

    @Test
    public void testPrepareFailureAtMiddleTableRefusesWholeBatch() throws Exception {
        assertPrepareFailureRefusesWholeBatch(1, "test: prepare failed");
    }

    @Test
    public void testReadOnlyRefusalAfterAppendCommitsBatchBeforeClose() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "c");
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
     * Closing the evicted stale entry's writer fails; the re-created table's frames still share one batch.
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
                    1, lastTxn(engine, "tab"));
        });
    }

    /**
     * Pass 2's first frame redefines dictionary id 0 as y and opens the batch; its last frame fails.
     */
    @Test
    public void testRedefiningFrameOpeningBatchKeepsItsSymbols() throws Exception {
        assertSymbolRedefinitionCommitsOnce(true);
    }

    /**
     * Frame 2 redefines dictionary id 0 as y and joins: frame 1 keeps x, as writers hold table symbol keys.
     */
    @Test
    public void testRedefiningSymbolDictionaryJoinsCommitBatch() throws Exception {
        assertSymbolRedefinitionCommitsOnce(false);
    }

    /**
     * The batch commit owed before a failed frame's answer is refused: the NACK names the batch's first frame.
     */
    @Test
    public void testRefusedBoundaryCommitAfterFailedFirstBlockNacksBatch() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
            final long aBefore = lastTxn(engine, "a");
            final long bBefore = lastTxn(engine, "b");
            final ObjList<byte[]> sent = ingestInOnePass(
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
     * A frame appends to b and fails on c, and the boundary commit is refused at the batch's first table.
     */
    @Test
    public void testRefusedBoundaryCommitAfterRewindNacksBatch() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b", "c");
            final long aBefore = lastTxn(engine, "a");
            final long bBefore = lastTxn(engine, "b");
            final ObjList<byte[]> sent = ingestInOnePass(
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
     * A RENAME of b reaches b's sequencer mid-batch: a commits, b's commit fails, and b's first frame is NACKed.
     */
    @Test
    public void testRenameWindowOfLaterTableCommitsTablesBeforeIt() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
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
     * Cap 4: three frames of a, then [b, a], whose block of a would reach the cap: b's row goes back out.
     */
    @Test
    public void testRevokeAfterJoinAbandonsPreparedWriterWithoutFileWorkUnderLock() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b", "c");
            final WalFlushCountingFacade ff = new WalFlushCountingFacade();
            try (CairoEngine pinnedEngine = new CairoEngine(pinnedConfiguration(CommitMode.SYNC, 0, ff))) {
                assertPinnedMode(pinnedEngine, CommitMode.SYNC, 0);
                final int[] walIds = warmWriters(pinnedEngine, "a", "b", "c");
                ff.arm();
                final ObjList<byte[]> sent;
                try (Connection connection = new Connection(
                        pinnedEngine,
                        httpConfig,
                        denyCommitUnderLockTo(pinnedEngine, "b"),
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("c", 300L, 3_000_000L))
                )) {
                    connection.deliverFrames(3);
                    sent = connection.sentFrames();
                }
                ff.disarm();
                Assert.assertEquals("the ack covers a's frame", 0, maxCumulativeOkAck(sent));
                Assert.assertEquals(QwpConstants.STATUS_WRITE_ERROR, errorStatusForSeq(sent, 1));
                TestUtils.assertContains(errorMessageForSeq(sent, 1), "test: commit denied for b");
                // Under the lock only the sequencer log flushes: a's sequencing; b's abandon does no file work.
                Assert.assertEquals(ff.getLog(), 0, ff.getCount(WalFlushCountingFacade.PRIVATE, true));
                Assert.assertEquals(1, lastTxn(pinnedEngine, "a"));
                Assert.assertEquals(0, lastTxn(pinnedEngine, "b"));
                Assert.assertEquals(0, lastTxn(pinnedEngine, "c"));
                // a's writer sequenced and goes back to the pool; b's and c's were prepared and are discarded.
                try (WalWriter w = pinnedEngine.getWalWriter(pinnedEngine.verifyTableName("a"))) {
                    Assert.assertEquals(walIds[0], w.getWalId());
                }
                assertWritersReplaced(pinnedEngine, new int[]{walIds[1], walIds[2]}, "b", "c");
            }
        });
    }

    @Test
    public void testRewindAtCapBoundary() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration customConfig = httpConfigWithMaxUncommittedRows(4);
            createTables("a", "b");
            final long aBefore = lastTxn(engine, "a");
            final long bBefore = lastTxn(engine, "b");
            final ObjList<byte[]> sent = ingestInOnePass(
                    customConfig,
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
     * Behind three frames each of a and b, a frame appends to a and then fails on b.
     */
    @Test
    public void testRewindCommitsEachTableOnce() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
            final long aBefore = lastTxn(engine, "a");
            final long bBefore = lastTxn(engine, "b");
            final ObjList<byte[]> sent = ingestInOnePass(
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
     * Rewinding 2,049 rows of a cannot remap a's first page: the batch is lost, and the replay commits once.
     */
    @Test
    public void testRewindFailureLosesBatchAndReplayCommitsItOnce() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
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
     * As {@link #testRewindFailureLosesBatchAndReplayCommitsItOnce}, for a frame that only cannot join.
     */
    @Test
    public void testRewindFailureOfFrameThatCannotJoinLosesBatch() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
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
     * The last frame fails after appending to s: each batch row keeps the symbol its frame named.
     */
    @Test
    public void testRewindKeepsSymbolsOfBatchFrames() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE t (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long sBefore = lastTxn(engine, "s");
            final GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            ObjList<byte[]> sent = ingestInOnePass(
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
     * A frame appends to a and fails on a new table: it gets its status without being processed again.
     */
    @Test
    public void testRewoundFrameGetsTheStatusItFailedWith() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
            final long aBefore = lastTxn(engine, "a");
            final long bBefore = lastTxn(engine, "b");
            ObjList<byte[]> sent = ingestInOnePass(
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
                    aBefore + 1, lastTxn(engine, "a"));
            Assert.assertEquals(bBefore, lastTxn(engine, "b"));
            Assert.assertEquals(1, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, 2));
            Assert.assertTrue("the ack must precede the NACK",
                    indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 1) < indexOfErrorResponse(sent, 2));
        });
    }

    /**
     * A switch that leaves the node writable gets the lock after the batch, and the next frame commits.
     */
    @Test
    public void testRoleSwitchDuringBatchCommitLetsNextFrameCommit() throws Exception {
        assertRoleSwitchDuringBatchCommit(false, false);
    }

    /**
     * A demote queued while frames alternating a and b commit lands after b: the replay stores no row twice.
     */
    @Test
    public void testRoleSwitchDuringBatchCommitOfInterleavedFramesDemotesAfterLastTable() throws Exception {
        assertRoleSwitchDuringBatchCommit(true, false);
    }

    /**
     * As the interleaved case, with frames that each carry a row of a and of b.
     */
    @Test
    public void testRoleSwitchDuringBatchCommitOfMultiTableFramesDemotesAfterLastTable() throws Exception {
        assertRoleSwitchDuringBatchCommit(true, true);
    }

    @Test
    public void testRowlessFirstBlockThenFailedBlockCommitsBatchBeforeNack() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            ObjList<byte[]> sent = ingestInOnePass(
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
     * Frame [n, n with a new column]: n's row goes back out, a commits, and the frame then goes the normal way.
     */
    @Test
    public void testSecondBlockOfNewTableThatCannotJoinCommitsFrameOnce() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "n");
            final long aBefore = lastTxn(engine, "a");
            final long nBefore = lastTxn(engine, "n");
            final ObjList<byte[]> sent = ingestInOnePass(
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
     * b's sequencer records a DROP right after the lookup: b is refused as it joins, and a's batch commits.
     */
    @Test
    public void testSequencerDroppedTableIsRefusedAsItJoins() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
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
        assertTableChangeBetweenPassesJoinsBatch("ALTER TABLE b RESUME WAL", "ALTER TABLE b SUSPEND WAL");
    }

    /**
     * b renamed and renamed back between passes is the same table: its frames join and pass 2 is acked whole.
     */
    @Test
    public void testTableRenamedBackJoinsCommitBatch() throws Exception {
        assertTableChangeBetweenPassesJoinsBatch(null, "RENAME TABLE b TO b2", "RENAME TABLE b2 TO b");
    }

    /**
     * A table's stale batch slot, from an earlier pass or a rowless block, must not match the new batch.
     */
    @Test
    public void testTablesOfEarlierPassRejoinLaterBatch() throws Exception {
        assertMemoryLeak(() -> {
            final String[] tables = {"a", "b", "c", "d", "e"};
            final long[] txnBefore = new long[tables.length];
            for (int t = 0; t < tables.length; t++) {
                execute("CREATE TABLE " + tables[t] + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                txnBefore[t] = lastTxn(engine, tables[t]);
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
                        lastTxn(engine, tables[t]));
            }
        });
    }

    /**
     * A frame past the connection's table limit fails its lookup before appending: the batch commits first.
     */
    @Test
    public void testTooManyTablesFrameIsNackedAfterBatchCommits() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration customConfig = httpConfigWithMaxTables(2);
            createTables("a", "b", "c");
            ObjList<byte[]> sent = ingestInOnePass(
                    customConfig,
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
     * An unexpected lookup failure distresses the cache: nothing commits, and the NACK names this frame.
     */
    @Test
    public void testUnexpectedErrorAfterPreparePhaseAtBoundaryLosesBatch() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
            try (CairoEngine pinnedEngine = new CairoEngine(pinnedConfiguration(CommitMode.SYNC, 0, TestFilesFacadeImpl.INSTANCE))) {
                final int[] walIds = warmWriters(pinnedEngine, "a", "b");
                final ObjList<byte[]> sent;
                try (Connection connection = new Connection(
                        pinnedEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                        // a new column of a batch table cannot join: a boundary commit inside the frame's handling
                        createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(tableBlockWithColumn("a", 300L, "w", 1, 3_000_000L)))
                )) {
                    connection.getState().setAfterPreparePhaseHook(() -> {
                        throw new IllegalStateException("test: unexpected error after the prepare phase");
                    });
                    connection.deliverFrames(3);
                    sent = connection.sentFrames();
                    Assert.assertEquals(1, connection.getState().getPreparePhaseCount());
                    Assert.assertFalse(connection.getState().isCommitBatchPending());
                }
                Assert.assertEquals(-1, maxCumulativeOkAck(sent));
                Assert.assertEquals(QwpConstants.STATUS_INTERNAL_ERROR, errorStatusForSeq(sent, 0));
                assertWritersReplaced(pinnedEngine, walIds, "a", "b");
                Assert.assertEquals(0, lastTxn(pinnedEngine, "a"));
                Assert.assertEquals(0, lastTxn(pinnedEngine, "b"));
            }
        });
    }

    @Test
    public void testUnexpectedErrorAfterPreparePhaseOnReceivePathDisconnects() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
            try (CairoEngine pinnedEngine = new CairoEngine(pinnedConfiguration(CommitMode.SYNC, 0, TestFilesFacadeImpl.INSTANCE))) {
                final int[] walIds = warmWriters(pinnedEngine, "a", "b");
                try (Connection connection = new Connection(
                        pinnedEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L))
                )) {
                    connection.getState().setAfterPreparePhaseHook(() -> {
                        throw new IllegalStateException("test: unexpected error after the prepare phase");
                    });
                    Assert.assertTrue("the error disconnects", connection.deliverFrames(2));
                    Assert.assertFalse("the frame loop abandons the batch", connection.getState().isCommitBatchPending());
                    connection.onConnectionClosed();
                    Assert.assertEquals(-1, maxCumulativeOkAck(connection.sentFrames()));
                }
                assertWritersReplaced(pinnedEngine, walIds, "a", "b");
                Assert.assertEquals(0, lastTxn(pinnedEngine, "a"));
                Assert.assertEquals(0, lastTxn(pinnedEngine, "b"));
            }
        });
    }

    @Test
    public void testUnexpectedErrorAfterPreparePhaseOnSendPathDisconnects() throws Exception {
        final LogCapture capture = new LogCapture();
        assertMemoryLeak(() -> {
            createTables("a", "b");
            try (CairoEngine pinnedEngine = new CairoEngine(pinnedConfiguration(CommitMode.SYNC, 0, TestFilesFacadeImpl.INSTANCE))) {
                final int[] walIds = warmWriters(pinnedEngine, "a", "b");
                try (Connection connection = new Connection(
                        pinnedEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        // the PONG's send parks, so the frames behind it wait for resumeSend
                        createMaskedFrame(WebSocketOpcode.PING, new byte[0]),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L))
                )) {
                    connection.blockSend(1);
                    Assert.assertTrue("test setup: the PONG must park", connection.deliverFramesUntilSendParks(3));
                    Assert.assertEquals("test setup: the frames wait behind the PONG", 0, connection.getState().getPreparePhaseCount());
                    connection.getState().setAfterPreparePhaseHook(() -> {
                        throw new IllegalStateException("test: unexpected error on the send path");
                    });
                    capture.start();
                    try {
                        try {
                            connection.resumeSend();
                            Assert.fail("the error must disconnect");
                        } catch (ServerDisconnectException expected) {
                            // the dispatcher disconnects
                        }
                        capture.waitFor("test: unexpected error on the send path");
                    } finally {
                        capture.stop();
                    }
                    capture.assertLogged("WebSocket send error");
                    Assert.assertEquals(1, connection.getState().getPreparePhaseCount());
                    connection.onConnectionClosed();
                    Assert.assertEquals(-1, maxCumulativeOkAck(connection.sentFrames()));
                }
                assertWritersReplaced(pinnedEngine, walIds, "a", "b");
                Assert.assertEquals(0, lastTxn(pinnedEngine, "a"));
                Assert.assertEquals(0, lastTxn(pinnedEngine, "b"));
            }
        });
    }

    @Test
    public void testUnexpectedLookupErrorLosesBatch() throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
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
     * An unexpected failure at the read-only gate distresses the cache: nothing commits, the NACK names the frame.
     */
    @Test
    public void testUnexpectedPreparationErrorLosesBatch() throws Exception {
        assertMemoryLeak(() -> {
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
     * A data frame without a payload commits the batch before it and is acked on its own.
     */
    @Test
    public void testZeroLengthFrameEndsCommitBatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            ObjList<byte[]> sent = ingestInOnePass(
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, new byte[0]),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n300\n");
            Assert.assertEquals("the empty frame splits the batch",
                    aBefore + 2, lastTxn(engine, "a"));
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

    private static void assertPinnedMode(CairoEngine engine, int commitMode, long groupWindowUs) {
        Assert.assertEquals(commitMode, engine.getConfiguration().getCommitMode());
        Assert.assertEquals(groupWindowUs, engine.getConfiguration().getAdaptiveCommitGroupWindowUs());
    }

    // Each table's next writer from the pool is a new one: the batch discarded the writers of walIds.
    private static void assertWritersReplaced(CairoEngine engine, int[] walIds, String... tables) {
        for (int i = 0; i < tables.length; i++) {
            try (WalWriter w = engine.getWalWriter(engine.verifyTableName(tables[i]))) {
                Assert.assertNotEquals(tables[i] + "'s writer must be discarded", walIds[i], w.getWalId());
                Assert.assertFalse(w.isCommitPrepared());
                Assert.assertFalse(w.isDistressed());
            }
        }
    }

    private static SecurityContext denyCommitUnderLockTo(CairoEngine engine, String tableName) {
        // A REVOKE that lands between the join and the commit: the check under the role-switch lock refuses.
        return new AllowAllSecurityContext() {
            @Override
            public void authorizeInsert(TableToken tableToken) {
                if (tableToken.getTableName().equals(tableName) && engine.getRoleSwitchReadLockCount() > 0) {
                    throw CairoException.nonCritical().put("test: commit denied for ").put(tableName);
                }
            }
        };
    }

    // Acquires and returns each table's writer, so that the pool holds one; returns their walIds.
    private static int[] warmWriters(CairoEngine engine, String... tables) {
        final int[] walIds = new int[tables.length];
        for (int i = 0; i < tables.length; i++) {
            try (WalWriter w = engine.getWalWriter(engine.verifyTableName(tables[i]))) {
                walIds[i] = w.getWalId();
            }
        }
        return walIds;
    }

    // The payload of the error response for seq, [status][seq LE][length u16 LE][message], or null. Reads the
    // extended payload length of a frame above 125 bytes.
    private static byte[] errorPayloadForSeq(ObjList<byte[]> frames, long seq) {
        for (int i = 0, n = frames.size(); i < n; i++) {
            final byte[] f = frames.getQuick(i);
            if ((f[0] & 0x0F) != WebSocketOpcode.BINARY) {
                continue;
            }
            final int len7 = f[1] & 0x7F;
            final int offset = len7 < 126 ? 2 : len7 == 126 ? 4 : 10;
            if (f[offset] != QwpConstants.STATUS_OK && readLeLong(f, offset + 1) == seq) {
                return Arrays.copyOfRange(f, offset, f.length);
            }
        }
        return null;
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

    private static void createTables(String... names) throws SqlException {
        for (String name : names) {
            execute("CREATE TABLE " + name + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        }
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
        final long txnBefore = lastTxn(engine, tableName);
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
     * {@code count} one-row frames of {@code table}, v = 100, 101, ... at 1s, 2s, ..., then {@code tail}.
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
     * A configuration of the shared root that pins the commit mode and the group window, which
     * DefaultTestCairoConfiguration takes from the CI shard, and reads the test clock and ff.
     */
    private static DefaultTestCairoConfiguration pinnedConfiguration(int commitMode, long groupWindowUs, FilesFacade ff) {
        return new DefaultTestCairoConfiguration(root) {
            @Override
            public long getAdaptiveCommitGroupWindowUs() {
                return groupWindowUs;
            }

            @Override
            public int getCommitMode() {
                return commitMode;
            }

            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return ff;
            }

            @Override
            public @NotNull MicrosecondClock getMicrosecondClock() {
                return testMicrosClock;
            }
        };
    }


    /**
     * A one-row delta-dictionary frame of s, v and ts; {@code badTable} adds a block with v as a VARCHAR.
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
     * As {@link #symbolFrame}, writing s and then v as a VARCHAR, which the LONG column parses.
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
     * A one-row frame of t(x VARCHAR, s STRING, bin BINARY, arr DOUBLE[], v LONG, ts), with v sent as a VARCHAR.
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
     * Frames a0 b1 a2 b3, then [a, bad b]: each table commits once, frames 0-3 are acked, and frame 4 is NACKed.
     */
    /**
     * A batch commit of three tables flushes no writer-private WAL file under the role-switch lock, and, under SYNC
     * and with no group window, only the sequencer files there. Per pass it flushes what the same rows cost when
     * committed table by table through plain WalWriter commits on the same build, under the same clock: the first
     * pass opens the writers, the second runs a window later, when the group flush falls due.
     */
    private void assertBatchCommitFlushesNoPrivateFileUnderLock(int commitMode, long groupWindowUs) throws Exception {
        assertMemoryLeak(() -> {
            final String[] batchTables = {"a", "b", "c"};
            final String[] oracleTables = {"oa", "ob", "oc"};
            createTables(batchTables);
            createTables(oracleTables);
            final WalFlushCountingFacade ff = new WalFlushCountingFacade();
            final long t0 = 1_000_000_000L;
            try (CairoEngine pinnedEngine = new CairoEngine(pinnedConfiguration(commitMode, groupWindowUs, ff))) {
                assertPinnedMode(pinnedEngine, commitMode, groupWindowUs);
                final int[][] batchCounts = new int[2][];
                try (Connection connection = new Connection(
                        pinnedEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("c", 300L, 3_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 400L, 4_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 500L, 5_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("c", 600L, 6_000_000L))
                )) {
                    for (int pass = 0; pass < 2; pass++) {
                        setCurrentMicros(t0 + pass * groupWindowUs);
                        ff.arm();
                        connection.deliverFrames(3);
                        ff.disarm();
                        Assert.assertEquals("no writer-private flush under the lock: " + ff.getLog(), 0, ff.getCount(WalFlushCountingFacade.PRIVATE, true));
                        if (groupWindowUs > 0) {
                            Assert.assertEquals("the group flush runs after the unlock: " + ff.getLog(), 0, ff.getCount(WalFlushCountingFacade.SEQUENCER, true));
                        } else {
                            Assert.assertTrue("the sequencer log flushes per table under the lock: " + ff.getLog(), ff.getCount(WalFlushCountingFacade.SEQUENCER, true) >= batchTables.length);
                        }
                        Assert.assertTrue("the prepares flush the writers' files: " + ff.getLog(), ff.getCount(WalFlushCountingFacade.PRIVATE, false) >= batchTables.length);
                        batchCounts[pass] = new int[]{ff.getCount(WalFlushCountingFacade.PRIVATE), ff.getCount(WalFlushCountingFacade.SEQUENCER)};
                    }
                    Assert.assertEquals(5, maxCumulativeOkAck(connection.sentFrames()));
                }
                // The oracle: the same rows, committed table by table.
                final WalWriter[] writers = new WalWriter[oracleTables.length];
                try {
                    for (int pass = 0; pass < 2; pass++) {
                        setCurrentMicros(t0 + pass * groupWindowUs);
                        ff.arm();
                        for (int t = 0; t < oracleTables.length; t++) {
                            if (writers[t] == null) {
                                writers[t] = pinnedEngine.getWalWriter(pinnedEngine.verifyTableName(oracleTables[t]));
                            }
                            final TableWriter.Row row = writers[t].newRow((pass * 3 + t + 1) * 1_000_000L);
                            row.putLong(0, (pass * 3 + t + 1) * 100L);
                            row.append();
                            writers[t].commit();
                        }
                        ff.disarm();
                        Assert.assertEquals(
                                "pass " + pass + " flushes what plain commits of the same rows flush: " + ff.getLog(),
                                Arrays.toString(batchCounts[pass]),
                                Arrays.toString(new int[]{ff.getCount(WalFlushCountingFacade.PRIVATE), ff.getCount(WalFlushCountingFacade.SEQUENCER)})
                        );
                    }
                } finally {
                    for (WalWriter w : writers) {
                        Misc.free(w);
                    }
                }
                drainWalQueue(pinnedEngine);
                assertHookQuery(pinnedEngine, "SELECT v FROM a").expectSize().returns("v\n100\n400\n");
                assertHookQuery(pinnedEngine, "SELECT v FROM oa").expectSize().returns("v\n100\n400\n");
            }
        });
    }

    private void assertBatchCommitsBeforeFrameFailingAfterAppend(boolean isDurableAck) throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
            final long aBefore = lastTxn(engine, "a");
            final long bBefore = lastTxn(engine, "b");
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
            Assert.assertEquals("a commits once", aBefore + 1, lastTxn(engine, "a"));
            Assert.assertEquals("b commits once", bBefore + 1, lastTxn(engine, "b"));
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
            final HttpFullFatServerConfiguration customConfig = httpConfigWithMaxUncommittedRows(2);
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long before = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"));

            final ObjList<byte[]> sent = new ObjList<>();
            ingestInOnePass(
                    engine,
                    new QwpIngressUpgradeProcessor(engine, customConfig),
                    customConfig,
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
     * Frames of a weather block with v and a {@code secondName} block with v and h join one batch.
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
            final ObjList<byte[]> sent = ingestInOnePass(frames);
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
     * Frames of a, b and c in one pass commit once per table and get one ack.
     */
    private void assertCommitsPerTable(long expectedTransactions, long expectedRows, byte[]... frames) throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b", "c");
            long before = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a"))
                    + engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"))
                    + engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("c"));
            ObjList<byte[]> sent = ingestInOnePass(frames);
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
     * Frames a0 b1 c2 a3 b4 c5; {@code deniedTable}'s commit fails: tables before it commit, it is NACKed.
     */
    private void assertCommitFailureAtTable(int deniedTable, String... expectedValues) throws Exception {
        assertMemoryLeak(() -> {
            for (int durable = 0; durable < 2; durable++) {
                final boolean isDurableAck = durable == 1;
                final String[] tables = {"a" + durable, "b" + durable, "c" + durable};
                final long[] txnBefore = new long[tables.length];
                for (int t = 0; t < tables.length; t++) {
                    execute("CREATE TABLE " + tables[t] + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    txnBefore[t] = lastTxn(engine, tables[t]);
                }
                final byte[][] frames = new byte[6][];
                for (int f = 0; f < frames.length; f++) {
                    frames[f] = createMaskedFrame(
                            WebSocketOpcode.BINARY,
                            oneRowMessage(tables[f % tables.length], (f + 1) * 100L, (f + 1) * 1_000_000L)
                    );
                }
                final ObjList<byte[]> sent = new ObjList<>();
                ingestInOnePass(engine, new QwpIngressUpgradeProcessor(engine, httpConfig), httpConfig,
                        failCommitAfterJoin(tables[deniedTable]), isDurableAck, sent, frames);
                drainWalQueue();
                for (int t = 0; t < tables.length; t++) {
                    assertQuery("SELECT v FROM " + tables[t]).noLeakCheck().expectSize().returns("v\n" + expectedValues[t]);
                    Assert.assertEquals("only the tables before the refused one commit, once each",
                            txnBefore[t] + (t < deniedTable ? 1 : 0),
                            lastTxn(engine, tables[t]));
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

    // A deferred frame appends to a, and the plain frame after it closes the group with a row of b, so the closing
    // frame commits both tables on the normal path. The demote lands at b's commit, after a committed: a's row sits
    // above the ack, and only the role-change CLOSE tells the client to replay. A durable-ack connection defers that
    // CLOSE until its committed work is durable, which the lagging registry never reports, so nothing goes out yet.
    private void assertDemoteAfterPreparePhaseCommitsNothing(int commitMode, long groupWindowUs, boolean isDurableAck) throws Exception {
        assertMemoryLeak(() -> {
            final String a = "a";
            final String b = "b";
            createTables(a, b);
            try (DemotableEngine demotableEngine = new DemotableEngine(pinnedConfiguration(commitMode, groupWindowUs, TestFilesFacadeImpl.INSTANCE))) {
                assertPinnedMode(demotableEngine, commitMode, groupWindowUs);
                final int[] walIds = warmWriters(demotableEngine, a, b);
                // the demote takes the lock right after the prepare phase: the batch's lock() then finds REPLICA
                demotableEngine.demoteAtNextCommit();
                final boolean[] isPrepared = {false};
                final ObjList<byte[]> sent;
                try (Connection connection = new Connection(
                        demotableEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        isDurableAck,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(a, 100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(b, 200L, 2_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(a, 300L, 3_000_000L))
                )) {
                    final QwpIngressProcessorState state = connection.getState();
                    state.setAfterPreparePhaseHook(() -> isPrepared[0] = state.getCommitBatchWriter(0).isCommitPrepared()
                            && state.getCommitBatchWriter(1).isCommitPrepared() && !demotableEngine.isReadOnlyMode());
                    connection.deliverFrames(3);
                    Assert.assertTrue("test setup: the demote must land after the prepare phase", isPrepared[0] && demotableEngine.isReadOnlyMode());
                    sent = connection.sentFrames();
                }
                Assert.assertEquals("only the CLOSE", 1, sent.size());
                Assert.assertEquals(WebSocketCloseCode.NORMAL_CLOSURE, closeCode(sent.getQuick(0)));
                Assert.assertEquals(-1, maxCumulativeOkAck(sent));
                demotableEngine.isReadOnly.set(false);
                assertWritersReplaced(demotableEngine, walIds, a, b);
                Assert.assertEquals(0, lastTxn(demotableEngine, a));
                Assert.assertEquals(0, lastTxn(demotableEngine, b));
            }
        });
    }

    private void assertPrepareFailureRefusesWholeBatch(int failedTable, String failure) throws Exception {
        assertMemoryLeak(() -> {
            final String[] tables = {"a", "b", "c"};
            createTables(tables);
            final EventFlushFaultFacade ff = new EventFlushFaultFacade();
            final byte[][] frames = {
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("c", 300L, 3_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 400L, 4_000_000L))
            };
            try (CairoEngine pinnedEngine = new CairoEngine(pinnedConfiguration(CommitMode.SYNC, 0, ff))) {
                assertPinnedMode(pinnedEngine, CommitMode.SYNC, 0);
                final int[] walIds = warmWriters(pinnedEngine, tables);
                ff.failEventFlush(tables[failedTable] + "~", false, failure);
                final ObjList<byte[]> sent;
                // A small response limit, so that a long message is truncated in a frame the socket takes whole.
                final HttpFullFatServerConfiguration smallSendConfig = new DefaultHttpServerConfiguration(configuration) {
                    @Override
                    public int getSendBufferSize() {
                        return SEND_BUFFER_SIZE;
                    }
                };
                try (Connection connection = new Connection(pinnedEngine, smallSendConfig, AllowAllSecurityContext.INSTANCE, false, frames)) {
                    connection.deliverFrames(frames.length);
                    sent = connection.sentFrames();
                }
                Assert.assertTrue(ff.hasFired);
                Assert.assertFalse("not a data-sync failure: the engine runs on", pinnedEngine.isDurabilityFailed());
                // Head committed the tables before the failed one and the client's replay stored their rows twice;
                // the batch now commits nothing.
                for (String table : tables) {
                    Assert.assertEquals(table + " commits nothing", 0, lastTxn(pinnedEngine, table));
                }
                Assert.assertEquals(-1, maxCumulativeOkAck(sent));
                final byte[] nack = errorPayloadForSeq(sent, 0);
                Assert.assertNotNull("the NACK names the batch's first frame", nack);
                Assert.assertEquals(QwpConstants.STATUS_INTERNAL_ERROR, nack[0]);
                final String expectedText = "commit batch prepare failed [table=" + tables[failedTable] + "]: " + failure;
                final int maxErrorLength = Math.max(0, (int) ((smallSendConfig.getSendBufferSize() - 100) / 1.5));
                Assert.assertEquals(expectedText.substring(0, Math.min(expectedText.length(), maxErrorLength)),
                        new String(nack, 11, readLeShort(nack, 9), StandardCharsets.UTF_8));
                // The tables up to the failed one were prepared or failed in their prepare: discarded. The tables after
                // it go back to the pool, healthy, with their rows rolled back.
                for (int t = 0; t < tables.length; t++) {
                    try (WalWriter w = pinnedEngine.getWalWriter(pinnedEngine.verifyTableName(tables[t]))) {
                        if (t <= failedTable) {
                            Assert.assertNotEquals(tables[t] + "'s writer must be discarded", walIds[t], w.getWalId());
                        } else {
                            Assert.assertEquals(tables[t] + "'s writer goes back to the pool", walIds[t], w.getWalId());
                        }
                        Assert.assertFalse(w.isCommitPrepared());
                        Assert.assertFalse(w.isDistressed());
                        Assert.assertEquals(0, w.getUncommittedRowCount());
                    }
                }
                // The client's replay on a fresh connection stores every row once.
                try (Connection connection = new Connection(pinnedEngine, httpConfig, AllowAllSecurityContext.INSTANCE, false, frames)) {
                    connection.deliverFrames(frames.length);
                    Assert.assertEquals(3, maxCumulativeOkAck(connection.sentFrames()));
                }
                drainWalQueue(pinnedEngine);
                assertHookQuery(pinnedEngine, "SELECT v FROM a").expectSize().returns("v\n100\n400\n");
                assertHookQuery(pinnedEngine, "SELECT v FROM b").expectSize().returns("v\n200\n");
                assertHookQuery(pinnedEngine, "SELECT v FROM c").expectSize().returns("v\n300\n");
            }
        });
    }

    private void assertDemoteBetweenTablesOfGroupClosingFrame(boolean isDurableAck) throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
            try (DemotableEngine demotableEngine = new DemotableEngine()) {
                // a's commit takes the role-switch lock first, b's second.
                demotableEngine.demoteAtCommit(2);
                try (Connection connection = new Connection(
                        demotableEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        isDurableAck,
                        createMaskedFrame(WebSocketOpcode.BINARY, deferred(oneRowMessage("a", 100L, 1_000_000L))),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L))
                )) {
                    connection.deliverFrames(2);
                    Assert.assertTrue("test setup: the demote must land at a commit", demotableEngine.isReadOnlyMode());
                    final ObjList<byte[]> sent = connection.sentFrames();
                    if (isDurableAck) {
                        Assert.assertTrue("the CLOSE must wait for the durable upload", connection.getState().isRoleChangeCloseDeferred());
                        Assert.assertEquals("a deferred CLOSE sends nothing yet: no ack, no NACK", 0, sent.size());
                    } else {
                        Assert.assertFalse(connection.getState().isRoleChangeCloseDeferred());
                        assertOnlyRoleChangeCloseSent(sent);
                    }
                }
                drainWalQueue(demotableEngine);
                try (TableReader a = demotableEngine.getReader("a"); TableReader b = demotableEngine.getReader("b")) {
                    Assert.assertEquals("a commits before the demote", 1, a.size());
                    Assert.assertEquals("b's commit meets the demote", 0, b.size());
                }
            }
        });
    }

    // Five frames of t with every column type form a batch; the sixth fails on its only block's v or on a second
    // block, of b. The batch commits once before the NACK, and a fresh connection appends at the rewound positions.
    private void assertFailedFrameRewindsEveryColumnType(boolean isSecondBlockFailing) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x VARCHAR, s STRING, bin BINARY, arr DOUBLE[], v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            createTables("b");
            final long tBefore = lastTxn(engine, "t");
            final long bBefore = lastTxn(engine, "b");
            final byte[][] frames = new byte[6][];
            for (int i = 0; i < 5; i++) {
                frames[i] = typedFrame(i, Integer.toString(100 + i));
            }
            frames[5] = isSecondBlockFailing ? typedFrameThenBadBlock(5, "105") : typedFrame(5, "not-a-long");
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

            Assert.assertEquals(0, maxCumulativeOkAck(ingestInOnePass(typedFrame(6, "106"))));
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

    private void assertForeignTableCapBoundary(boolean isSeparatePass) throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration customConfig = httpConfigWithMaxUncommittedRows(2);
            createTables("a", "b");
            long bBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"));
            try (Connection connection = new Connection(
                    engine,
                    customConfig,
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

    // Frames a, b, a, and at a boundary a fourth frame that needs a new column of a; b's commit fails after a's.
    // The ack covers frame 0, the NACK names frame 1, and the boundary frame is not processed.
    private void assertLaterTableCommitFailureNacksItsFirstFrame(boolean isAtBoundary) throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
            final byte[][] frames = {
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(tableBlockWithExtraColumn("a", 400L, 7L, 4_000_000L)))
            };
            final ObjList<byte[]> sent = ingestInOnePass(
                    failCommitAfterJoin("b"),
                    isAtBoundary ? frames : Arrays.copyOf(frames, 3)
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n300\n");
            assertQuery("SELECT count() FROM b").noLeakCheck().inferRandomAccess().sizeMayVary().returns("count\n0\n");
            Assert.assertEquals("the ack stops before the failed table's first frame", 0, maxCumulativeOkAck(sent));
            Assert.assertTrue("the failed commit must be NACKed at the failed table's first frame", hasErrorResponseForSeq(sent, 1));
            Assert.assertFalse("no NACK may name a frame the ack covers", hasErrorResponseForSeq(sent, 0));
            Assert.assertFalse(hasErrorResponseForSeq(sent, 2));
            if (isAtBoundary) {
                Assert.assertFalse("the frame that cannot join is not processed", hasErrorResponseForSeq(sent, 3));
            }
            Assert.assertTrue("the ack must precede the NACK",
                    indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 0) < indexOfErrorResponse(sent, 1));
        });
    }

    private void assertMalformedPreparationSalvagesPrefix(boolean isHeader) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            byte[] malformed = oneRowMessage(200, 2_000_000);
            // Truncate either the header or first schema, before any table lookup.
            malformed = Arrays.copyOf(malformed, isHeader ? 3 : QwpConstants.HEADER_SIZE + 8);
            ObjList<byte[]> sent = ingestInOnePass(createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100, 1_000_000)),
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
     * With auto-creation off, a frame names a missing table, as its first block or behind one it appended.
     */
    private void assertMissingTableWithoutAutoCreate(boolean isAfterAppend) throws Exception {
        assertMemoryLeak(() -> {
            final LineHttpProcessorConfiguration lineConfig = new DefaultLineHttpProcessorConfiguration(configuration) {
                @Override
                public boolean autoCreateNewTables() {
                    return false;
                }
            };
            final HttpFullFatServerConfiguration customConfig = new DefaultHttpServerConfiguration(configuration) {
                @Override
                public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
                    return lineConfig;
                }
            };
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long aBefore = lastTxn(engine, "a");
            ObjList<byte[]> sent = ingestInOnePass(
                    customConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, isAfterAppend
                            ? qwpMessage(tableBlock("a", 300L, 3_000_000L), tableBlock("missing", 400L, 4_000_000L))
                            : oneRowMessage("missing", 400L, 4_000_000L))
            );
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n200\n");
            Assert.assertEquals("the batch commits once",
                    aBefore + 1, lastTxn(engine, "a"));
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
     * a's commit, the batch's first, is refused: nothing is acked, and the later tables commit nothing.
     */
    private void assertRefusedFirstTableRollsBackLaterTables(String... tables) throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b", "c");
            final long bBefore = lastTxn(engine, "b");
            final long cBefore = lastTxn(engine, "c");
            ObjList<byte[]> sent = ingestInOnePass(denyCommitAfterJoinTo("a"), clientEncodedFrame(1, "a"), clientEncodedFrame(1, tables));
            drainWalQueue();
            try (TableReader a = engine.getReader("a"); TableReader b = engine.getReader("b"); TableReader c = engine.getReader("c")) {
                Assert.assertEquals(0, a.size() + b.size() + c.size());
            }
            Assert.assertEquals("b must not publish a WAL transaction", bBefore,
                    lastTxn(engine, "b"));
            Assert.assertEquals("c must not publish a WAL transaction", cBefore,
                    lastTxn(engine, "c"));
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

    // A batch of a and b, with n rows each, arrives in one receive pass: frames that
    // alternate between the tables, or frames that each carry a row of both. A role switch
    // queues for the real write lock as the batch reads the node's role, before any table
    // commits, and gets the lock only after the batch's last table: the ack covers the
    // pass. The next pass's frame then meets the role the switch left. After a demote it
    // gets the role-change CLOSE, and the client replays every frame above the ack once the
    // node is writable again. Either way, each row is stored exactly once.
    private void assertRoleSwitchDuringBatchCommit(boolean isDemote, boolean isMultiTableFrame) throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
            // Row i of a holds 1_000 + i and row i of b 2_000 + i, both at i + 1 seconds; the
            // frame after the batch holds a's row n.
            final int n = 10;
            final int batchFrameCount = isMultiTableFrame ? n : 2 * n;
            final byte[][] frames = new byte[batchFrameCount + 1][];
            final StringBuilder expectedA = new StringBuilder("v\n");
            final StringBuilder expectedB = new StringBuilder("v\n");
            int batchBytes = 0;
            for (int i = 0; i < n; i++) {
                final long ts = (i + 1) * 1_000_000L;
                if (isMultiTableFrame) {
                    frames[i] = createMaskedFrame(WebSocketOpcode.BINARY, qwpMessage(
                            tableBlock("a", 1_000 + i, ts),
                            tableBlock("b", 2_000 + i, ts)
                    ));
                    batchBytes += frames[i].length;
                } else {
                    frames[2 * i] = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 1_000 + i, ts));
                    frames[2 * i + 1] = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 2_000 + i, ts));
                    batchBytes += frames[2 * i].length + frames[2 * i + 1].length;
                }
                expectedA.append(1_000 + i).append('\n');
                expectedB.append(2_000 + i).append('\n');
            }
            frames[batchFrameCount] = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 1_000 + n, (n + 1) * 1_000_000L));
            expectedA.append(1_000 + n).append('\n');
            Assert.assertTrue("test setup: the batch must arrive in one receive pass", batchBytes <= RECV_BUFFER_SIZE);

            final AtomicInteger committedAtGrant = new AtomicInteger(-1);
            final TableToken[] tokens = new TableToken[2];
            final long[] txnBefore = new long[2];
            try (RoleSwitchingEngine roleSwitchEngine = new RoleSwitchingEngine(isDemote, switchingEngine -> {
                int committed = 0;
                for (int t = 0; t < tokens.length; t++) {
                    if (switchingEngine.getTableSequencerAPI().lastTxn(tokens[t]) > txnBefore[t]) {
                        committed++;
                    }
                }
                committedAtGrant.set(committed);
            })) {
                tokens[0] = roleSwitchEngine.verifyTableName("a");
                tokens[1] = roleSwitchEngine.verifyTableName("b");
                txnBefore[0] = roleSwitchEngine.getTableSequencerAPI().lastTxn(tokens[0]);
                txnBefore[1] = roleSwitchEngine.getTableSequencerAPI().lastTxn(tokens[1]);
                final ObjList<byte[]> sent;
                try (Connection connection = new Connection(
                        roleSwitchEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        frames
                )) {
                    connection.deliverFrames(batchFrameCount);
                    roleSwitchEngine.awaitRoleSwitch();
                    Assert.assertEquals("the role switch gets the lock after the batch's last table", 2, committedAtGrant.get());
                    Assert.assertEquals("one ack covers the batch", batchFrameCount - 1, maxCumulativeOkAck(connection.sentFrames()));
                    Assert.assertEquals("no CLOSE within the batch's pass", -1, indexOfCloseFrame(connection.sentFrames()));
                    connection.deliverFrames(1);
                    sent = connection.sentFrames();
                }
                for (int seq = 0; seq <= batchFrameCount; seq++) {
                    Assert.assertFalse("no frame gets an error response", hasErrorResponseForSeq(sent, seq));
                }
                Assert.assertEquals("b commits once", txnBefore[1] + 1,
                        roleSwitchEngine.getTableSequencerAPI().lastTxn(tokens[1]));
                if (isDemote) {
                    Assert.assertEquals("a commits once", txnBefore[0] + 1,
                            roleSwitchEngine.getTableSequencerAPI().lastTxn(tokens[0]));
                    Assert.assertEquals("the ack stays at the batch", batchFrameCount - 1, maxCumulativeOkAck(sent));
                    final int closeIndex = indexOfCloseFrame(sent);
                    Assert.assertTrue("the frame after the demote gets the role-change CLOSE", closeIndex >= 0);
                    Assert.assertEquals(WebSocketCloseCode.NORMAL_CLOSURE, closeCode(sent.getQuick(closeIndex)));
                    Assert.assertTrue("the ack must precede the CLOSE",
                            indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, batchFrameCount - 1) < closeIndex);
                    // The client replays every frame above the ack once the node is writable.
                    roleSwitchEngine.revertDemote();
                    final int replayFrom = (int) maxCumulativeOkAck(sent) + 1;
                    try (Connection replay = new Connection(
                            roleSwitchEngine,
                            httpConfig,
                            AllowAllSecurityContext.INSTANCE,
                            false,
                            Arrays.copyOfRange(frames, replayFrom, frames.length)
                    )) {
                        replay.deliverFrames(frames.length - replayFrom);
                        Assert.assertEquals("the replay is acked", frames.length - replayFrom - 1,
                                maxCumulativeOkAck(replay.sentFrames()));
                    }
                } else {
                    Assert.assertEquals("a commits again with the next frame", txnBefore[0] + 2,
                            roleSwitchEngine.getTableSequencerAPI().lastTxn(tokens[0]));
                    Assert.assertEquals("the next frame is acked", batchFrameCount, maxCumulativeOkAck(sent));
                    Assert.assertEquals("no CLOSE", -1, indexOfCloseFrame(sent));
                }
                drainWalQueue(roleSwitchEngine);
                assertHookQuery(roleSwitchEngine, "SELECT v FROM a").expectSize().returns(expectedA.toString());
                assertHookQuery(roleSwitchEngine, "SELECT v FROM b").expectSize().returns(expectedB.toString());
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
            final HttpFullFatServerConfiguration customConfig = httpConfigWithMaxUncommittedRows(2);
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long txnBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"));

            final ObjList<byte[]> sent = new ObjList<>();
            ingestInOnePass(
                    engine,
                    new QwpIngressUpgradeProcessor(engine, customConfig),
                    customConfig,
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

    // Pass 1 writes x as dictionary id 0; pass 2 redefines id 0 as y in its first frame or its second, and its
    // last frame fails after appending to s. Each row keeps the symbol its frame named; s commits pass 2 once.
    private void assertSymbolRedefinitionCommitsOnce(boolean isRedefinedByFirstFrame) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s (s SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE t (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            final GlobalSymbolDictionary otherDict = new GlobalSymbolDictionary();
            final byte[] first = symbolFrame(dict, -1, "s", "x", 100L, 1_000_000L, null);
            final byte[] second = isRedefinedByFirstFrame
                    ? symbolFrame(otherDict, -1, "s", "y", 200L, 2_000_000L, null)
                    : symbolFrame(dict, 0, "s", "x", 200L, 2_000_000L, null);
            final byte[] third = symbolFrame(otherDict, isRedefinedByFirstFrame ? 0 : -1, "s", "y", 300L, 3_000_000L, null);
            final byte[] failing = symbolFrame(otherDict, 0, "s", "y", 400L, 4_000_000L, "t");
            final ObjList<byte[]> sent;
            final long sAfterFirstPass;
            try (Connection connection = new Connection(engine, httpConfig, AllowAllSecurityContext.INSTANCE, false, first, second, third, failing)) {
                connection.deliverFrames(1);
                sAfterFirstPass = lastTxn(engine, "s");
                connection.deliverFrames(3);
                sent = connection.sentFrames();
            }
            Assert.assertEquals("the second pass commits s once", sAfterFirstPass + 1, lastTxn(engine, "s"));
            drainWalQueue();
            assertQuery("SELECT s, v FROM s").noLeakCheck().expectSize()
                    .returns("s\tv\nx\t100\n" + (isRedefinedByFirstFrame ? "y" : "x") + "\t200\ny\t300\n");
            assertQuery("SELECT v FROM t").noLeakCheck().expectSize().returns("v\n");
            Assert.assertEquals(2, maxCumulativeOkAck(sent));
            Assert.assertEquals(QwpConstants.STATUS_SCHEMA_MISMATCH, errorStatusForSeq(sent, 3));
        });
    }

    // Pass 1 opens a and b, the DDL changes b without refusing its commits, and pass 2 joins one batch.
    private void assertTableChangeBetweenPassesJoinsBatch(String ddlBeforeDrain, String... ddl) throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
            final TableToken bToken = engine.verifyTableName("b");
            final ObjList<byte[]> sent;
            try (Connection connection = new Connection(engine, httpConfig, AllowAllSecurityContext.INSTANCE, false, tableChangeBetweenPassesFrames())) {
                connection.deliverFrames(2);
                Assert.assertEquals("test setup: pass 1 commits", 1, maxCumulativeOkAck(connection.sentFrames()));
                for (String sql : ddl) {
                    execute(sql);
                }
                connection.deliverFrames(4);
                sent = connection.sentFrames();
            }
            Assert.assertEquals("test setup: b keeps its directory", bToken.getDirName(), engine.verifyTableName("b").getDirName());
            Assert.assertEquals("pass 2 is acked whole", 5, maxCumulativeOkAck(sent));
            for (int seq = 0; seq < 6; seq++) {
                Assert.assertFalse(hasErrorResponseForSeq(sent, seq));
            }
            if (ddlBeforeDrain != null) {
                execute(ddlBeforeDrain);
            }
            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n300\n500\n");
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n200\n400\n600\n");
        });
    }

    /**
     * {@code ddl} changes b between passes: b's first frame is NACKed, and the replay stores a's rows once.
     */
    private void assertTableChangeBetweenPassesNacksItsFirstFrame(
            String ddl,
            String ddlBeforeReplay,
            byte refusalStatus
    ) throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "b");
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
     * A frame of a and n, in the given order, then a failing block: a keeps its batch rows, n leaves the batch.
     */
    private void assertThirdBlockFailureRewindsBothTables(boolean isNewTableFirst) throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "n", "b");
            final long aBefore = lastTxn(engine, "a");
            final long nBefore = lastTxn(engine, "n");
            final byte[] batchTableBlock = tableBlock("a", 300L, 3_000_000L);
            final byte[] newTableBlock = tableBlock("n", 301L, 3_000_001L);
            final ObjList<byte[]> sent = ingestInOnePass(
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
     * Frames [a], [c], [a], [a], [a, d], where d's writer acquisition fails with a CairoException or unexpectedly.
     */
    private void assertWriterAcquisitionFailureAfterAppend(boolean isUnexpected, boolean isDurableAck) throws Exception {
        assertMemoryLeak(() -> {
            createTables("a", "c", "d");
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

    private ObjList<byte[]> ingestInOnePass(byte[]... frames) throws Exception {
        return ingestInOnePass(httpConfig, AllowAllSecurityContext.INSTANCE, frames);
    }

    private ObjList<byte[]> ingestInOnePass(SecurityContext securityContext, byte[]... frames) throws Exception {
        return ingestInOnePass(httpConfig, securityContext, frames);
    }

    private ObjList<byte[]> ingestInOnePass(HttpFullFatServerConfiguration config, byte[]... frames) throws Exception {
        return ingestInOnePass(config, AllowAllSecurityContext.INSTANCE, frames);
    }

    private ObjList<byte[]> ingestInOnePass(
            HttpFullFatServerConfiguration config,
            SecurityContext securityContext,
            byte[]... frames
    ) throws Exception {
        return ingestInOnePass(new QwpIngressUpgradeProcessor(engine, config), config, securityContext, frames);
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
     * As above, through a receive buffer of {@code recvBufferSize} bytes, which must hold every frame.
     *
     * @return whether the pass left a role-change CLOSE deferred until the connection's committed work is durable
     */
    private boolean ingestInOnePass(
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
            return state.isRoleChangeCloseDeferred();
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
     * One fuzz iteration's faults and frames, over tables fa, fb and fc suffixed with {@code iteration}.
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

    // Draws the iteration's prepare fault from faultRnd. A prepare fault is a commit fault, which the
    // generator keeps out of an iteration whose only faults are data errors, checked for I3 and I4, and away
    // from deferred groups, whose commitAll commits the group's tables one by one, see
    // generateCommitBatchFuzzIteration(). Draws the same values either way, so a seed draws the same faults.
    private static void drawPrepareFault(Rnd faultRnd, FuzzIteration it) {
        final boolean isFaulty = faultRnd.nextInt(3) == 0;
        final int table = faultRnd.nextInt(it.tables.length);
        final long fromCall = 1 + faultRnd.nextInt(3);
        final boolean isTransient = faultRnd.nextBoolean();
        if (isFaulty && !it.isDataErrorsOnly && !it.frameKinds.contains(FUZZ_DEFERRED)) {
            it.prepareFaultTable = table;
            it.prepareFaultFromCall = fromCall;
            it.isPrepareFaultTransient = isTransient;
        }
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

    private void assertCommitBatchAckContractFuzz(int commitMode, long groupWindowUs, int iterationCount) throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            // A separate Rnd for the prepare faults keeps the frames each seed of rnd generates.
            final Rnd faultRnd = TestUtils.generateRandom(LOG);
            final ObjList<FuzzIteration> iterations = new ObjList<>();
            // A local run raises the budget, see CLAUDE.md's fuzzing rule.
            final int budget = Integer.getInteger("questdb.fuzz.qwp.batch.iterations", iterationCount);
            for (int iteration = 0; iteration < budget; iteration++) {
                final FuzzIteration it = generateCommitBatchFuzzIteration(rnd, iteration);
                drawPrepareFault(faultRnd, it);
                iterations.add(it);
            }
            runCommitBatchFuzz(iterations, commitMode, groupWindowUs);
        });
    }

    /**
     * Runs the fuzz iterations through an engine of the commit mode and group window whose read-only checks, writer
     * acquisitions and commit prepares fail as scripted. A prepare fails at the flush of the table's WAL column file,
     * which only a commit makes, and only under SYNC and ADAPTIVE: the modes that prepare.
     */
    private void runCommitBatchFuzz(ObjList<FuzzIteration> iterations, int commitMode, long groupWindowUs) throws Exception {
        // The hook engine below shares the root but cannot create tables: create them first.
        for (int i = 0, n = iterations.size(); i < n; i++) {
            for (String table : iterations.getQuick(i).tables) {
                execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            }
        }
        final AtomicReference<FuzzIteration> current = new AtomicReference<>();
        final PrepareFaultFacade ff = new PrepareFaultFacade(current);
        try (CairoEngine hookEngine = new CairoEngine(pinnedConfiguration(commitMode, groupWindowUs, ff)) {
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

        final HttpFullFatServerConfiguration customConfig = httpConfigWithMaxUncommittedRows(it.cap);
        final ObjList<byte[]> sent = new ObjList<>();
        boolean isRoleChangeCloseDeferred = false;
        try {
            // Room for the largest iteration: up to 11 frames of two blocks each.
            isRoleChangeCloseDeferred = ingestInOnePass(hookEngine, new QwpIngressUpgradeProcessor(hookEngine, customConfig), customConfig,
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
            // to replay: a NACK above the ack, or a CLOSE. A durable-ack connection defers a
            // role-change CLOSE until its committed work is durable, at most for the upload
            // grace, and refuses every frame meanwhile: a deferred CLOSE tells the client too.
            boolean isTold = isRoleChangeCloseDeferred || indexOfCloseFrame(sent) >= 0;
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

        // Makes the server's send numbered sendNumber, counted from 1, park as a slow peer's does.
        void blockSend(int sendNumber) {
            rawSocket.blockedSends.add(sendNumber);
        }

        /**
         * Delivers the client's next {@code count} frames in one receive pass, in which a send may park.
         * Returns true when one did: the frames behind it wait in the receive buffer for {@link #resumeSend()}.
         */
        boolean deliverFramesUntilSendParks(int count) throws Exception {
            try {
                deliverFrames(count);
                return false;
            } catch (PeerIsSlowToReadException e) {
                return true;
            }
        }

        QwpIngressProcessorState getState() {
            return state;
        }

        // What the dispatcher does once the connection is gone.
        void onConnectionClosed() {
            processor.onConnectionClosed(context);
        }

        // What the dispatcher does once the parked send's peer reads.
        void resumeSend() throws Exception {
            processor.resumeSend(context);
        }

        ObjList<byte[]> sentFrames() {
            return rawSocket.sentFrames;
        }
    }

    /**
     * Demotes when the commit after {@link #demoteAtNextCommit()} takes the lock; durable uploads never catch up.
     */
    private static final class DemotableEngine extends CairoEngine {
        // Commits left before the one the demote lands at; 0 when no demote is armed.
        private final AtomicInteger commitsUntilDemote = new AtomicInteger();
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
            this(new DefaultTestCairoConfiguration(root));
        }

        // An engine of configuration, such as one of pinnedConfiguration(), whose commit mode the test chooses.
        DemotableEngine(CairoConfiguration configuration) {
            super(configuration);
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

        void demoteAtCommit(int commit) {
            commitsUntilDemote.set(commit);
        }

        void demoteAtNextCommit() {
            demoteAtCommit(1);
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
                if (commitsUntilDemote.get() > 0 && commitsUntilDemote.decrementAndGet() == 0) {
                    isReadOnly.set(true);
                }
            }
        }
    }

    /**
     * Acts at the first flush of a table's WAL event file while armed: runs an action, or fails the flush, once.
     * A table is named by its directory prefix, such as {@code "b~"}. Arm it after the table's writer is open, so
     * that the flush is a commit's, not the segment open's.
     */
    private static final class EventFlushFaultFacade extends TestFilesFacadeImpl {
        private final java.util.Map<Long, String> eventFds = new java.util.concurrent.ConcurrentHashMap<>();
        // mapping address -> {length, fd} of the event files
        private final java.util.concurrent.ConcurrentSkipListMap<Long, long[]> eventMappings = new java.util.concurrent.ConcurrentSkipListMap<>();
        private volatile Runnable action;
        private volatile String actionTable;
        private volatile String failedTable;
        private volatile String failure;
        volatile boolean hasFired;
        private volatile boolean isDataSyncFailure;

        @Override
        public void barrierFsync(long fd) {
            onFlush(fd);
            super.barrierFsync(fd);
        }

        @Override
        public boolean close(long fd) {
            eventFds.remove(fd);
            return super.close(fd);
        }

        @Override
        public void fdatasync(long fd) {
            onFlush(fd);
            super.fdatasync(fd);
        }

        void failEventFlush(String table, boolean isDataSyncFailure) {
            failEventFlush(table, isDataSyncFailure, "test: prepare failed");
        }

        void failEventFlush(String table, boolean isDataSyncFailure, String failure) {
            this.isDataSyncFailure = isDataSyncFailure;
            this.failure = failure;
            this.failedTable = table;
        }

        @Override
        public void fsync(long fd) {
            onFlush(fd);
            super.fsync(fd);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            final long addr = super.mmap(fd, len, offset, flags, memoryTag);
            if (eventFds.containsKey(fd)) {
                eventMappings.put(addr, new long[]{len, fd});
            }
            return addr;
        }

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            final long newAddr = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
            if (eventFds.containsKey(fd)) {
                eventMappings.remove(addr);
                eventMappings.put(newAddr, new long[]{newSize, fd});
            }
            return newAddr;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            if (!async) {
                final java.util.Map.Entry<Long, long[]> mapping = eventMappings.floorEntry(addr);
                if (mapping != null && addr < mapping.getKey() + mapping.getValue()[0]) {
                    onFlush(mapping.getValue()[1]);
                }
            }
            super.msync(addr, len, async);
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            eventMappings.remove(address);
            super.munmap(address, size, memoryTag);
        }

        void onEventFlush(String table, Runnable action) {
            this.action = action;
            this.actionTable = table;
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (fd > -1 && Utf8s.endsWithAscii(name, WalUtils.EVENT_FILE_NAME)) {
                eventFds.put(fd, Utf8s.stringFromUtf8BytesSafe(name));
            }
            return fd;
        }

        private void onFlush(long fd) {
            final String path = eventFds.get(fd);
            if (path == null) {
                return;
            }
            final String sep = java.io.File.separator;
            final String actionTable = this.actionTable;
            if (actionTable != null && path.contains(sep + actionTable)) {
                this.actionTable = null;
                hasFired = true;
                action.run();
            }
            final String failedTable = this.failedTable;
            if (failedTable != null && path.contains(sep + failedTable)) {
                this.failedTable = null;
                hasFired = true;
                if (isDataSyncFailure) {
                    throw CairoException.dataSyncFailure(this, 5, "msync", failure);
                }
                throw CairoException.critical(0).put(failure);
            }
        }
    }

    /**
     * Fails the commit prepares of the current fuzz iteration's prepare-fault table, as the iteration scripts: at
     * the flush of the table's WAL column file v.d, which a commit makes and a writer's open does not.
     */
    private static final class PrepareFaultFacade extends TestFilesFacadeImpl {
        private final AtomicReference<FuzzIteration> current;
        private final java.util.Map<Long, String> fds = new java.util.concurrent.ConcurrentHashMap<>();
        // mapping address -> {length, fd} of the column files
        private final java.util.concurrent.ConcurrentSkipListMap<Long, long[]> mappings = new java.util.concurrent.ConcurrentSkipListMap<>();

        PrepareFaultFacade(AtomicReference<FuzzIteration> current) {
            this.current = current;
        }

        @Override
        public void barrierFsync(long fd) {
            onFlush(fd);
            super.barrierFsync(fd);
        }

        @Override
        public boolean close(long fd) {
            fds.remove(fd);
            return super.close(fd);
        }

        @Override
        public void fdatasync(long fd) {
            onFlush(fd);
            super.fdatasync(fd);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            final long addr = super.mmap(fd, len, offset, flags, memoryTag);
            if (fds.containsKey(fd)) {
                mappings.put(addr, new long[]{len, fd});
            }
            return addr;
        }

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            final long newAddr = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
            if (fds.containsKey(fd)) {
                mappings.remove(addr);
                mappings.put(newAddr, new long[]{newSize, fd});
            }
            return newAddr;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            if (!async) {
                final java.util.Map.Entry<Long, long[]> mapping = mappings.floorEntry(addr);
                if (mapping != null && addr < mapping.getKey() + mapping.getValue()[0]) {
                    onFlush(mapping.getValue()[1]);
                }
            }
            super.msync(addr, len, async);
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            mappings.remove(address);
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (fd > -1 && Utf8s.endsWithAscii(name, java.io.File.separator + "v.d") && Utf8s.containsAscii(name, java.io.File.separator + "wal")) {
                fds.put(fd, Utf8s.stringFromUtf8BytesSafe(name));
            }
            return fd;
        }

        private void onFlush(long fd) {
            final String path = fds.get(fd);
            final FuzzIteration it = current.get();
            if (path == null || it == null || it.prepareFaultTable < 0
                    || !path.contains(java.io.File.separator + it.tables[it.prepareFaultTable] + "~")) {
                return;
            }
            final long call = ++it.prepareCalls;
            if (it.isPrepareFaultTransient ? call == it.prepareFaultFromCall : call >= it.prepareFaultFromCall) {
                throw CairoException.critical(0).put("fuzz: prepare fails");
            }
        }
    }

    // A Lock that forwards every call to its delegate, for a test to override one of them.
    private static class ForwardingLock implements Lock {
        private final Lock delegate;

        ForwardingLock(Lock delegate) {
            this.delegate = delegate;
        }

        @Override
        public void lock() {
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
    }

    /**
     * Fails, once, the next flush of a table's sequencer file that a thread makes outside the role-switch lock,
     * with an error that is not a data-sync failure.
     */
    private static final class SequencerFlushFaultFacade extends WalFlushCountingFacade {
        private final java.util.Map<Long, String> paths = new java.util.concurrent.ConcurrentHashMap<>();
        volatile int failures;
        private volatile String failedTable;

        @Override
        public boolean close(long fd) {
            paths.remove(fd);
            return super.close(fd);
        }

        @Override
        public void fdatasync(long fd) {
            final String path = paths.get(fd);
            final String failedTable = this.failedTable;
            if (failedTable != null && path != null && path.contains(java.io.File.separator + failedTable)
                    && path.contains(WalUtils.SEQ_DIR) && !SuspensionScope.hasAnyRoleSwitchLock()) {
                this.failedTable = null;
                failures++;
                throw CairoException.critical(5).put("test: group flush failed");
            }
            super.fdatasync(fd);
        }

        void failNextOutsideLock(String table) {
            this.failedTable = table;
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (fd > -1) {
                paths.put(fd, Utf8s.stringFromUtf8BytesSafe(name));
            }
            return fd;
        }
    }

    /**
     * One fuzz iteration: its faults, frames and expected outcome. A value is its frame's sequence * 10 + its row.
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
        boolean isPrepareFaultTransient;
        boolean isReadOnlyTransient;
        boolean isRefusalPersistent;
        boolean isWriterFaultTransient;
        boolean isWriterFaultUnexpected;
        int iteration;
        long prepareCalls;
        long prepareFaultFromCall;
        // The index of the table whose prepares fail, or -1. Drawn from the fault Rnd, see drawPrepareFault().
        int prepareFaultTable = -1;
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
                    + ", writerFaultUnexpected=" + isWriterFaultUnexpected + ", prepareFaultTable=" + prepareFaultTable
                    + ", prepareFaultFrom=" + prepareFaultFromCall + ", prepareFaultTransient=" + isPrepareFaultTransient
                    + ", kinds=" + kinds;
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
        // Whether a frame with rows of a table other than table and one with rows of table exist: a batch of
        // the two can form.
        boolean hasPlainFrameOfOtherTableThan(int table) {
            boolean hasTable = false;
            boolean hasOther = false;
            for (int s = 0, n = frameValueTables.size(); s < n; s++) {
                final IntList valueTables = frameValueTables.getQuick(s);
                for (int i = 0, m = valueTables.size(); i < m; i++) {
                    hasTable |= valueTables.getQuick(i) == table;
                    hasOther |= valueTables.getQuick(i) != table;
                }
            }
            return hasTable && hasOther;
        }

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
            return refusedInsertCall > 0 || readOnlyFromCall > 0 || prepareFaultTable > -1;
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
     * When a batch reads the role under the read lock, queues a real role switch on the write lock.
     */
    private static final class RoleSwitchingEngine extends CairoEngine {
        private final boolean isDemote;
        private final AtomicBoolean isReadOnly = new AtomicBoolean();
        private final Consumer<CairoEngine> onGranted;
        private final AtomicReference<Throwable> switchFailure = new AtomicReference<>();
        // Started and read on the connection's thread only.
        private Thread roleSwitch;

        RoleSwitchingEngine(boolean isDemote, Consumer<CairoEngine> onGranted) {
            super(new DefaultTestCairoConfiguration(root));
            this.isDemote = isDemote;
            this.onGranted = onGranted;
        }

        @Override
        public boolean isReadOnlyMode() {
            if (roleSwitch == null && getRoleSwitchReadLockCount() > 0) {
                roleSwitch = new Thread(this::switchRole, "qwp-role-switch");
                roleSwitch.start();
                // The switch parks in the write lock's StampedLock only while a read hold is open.
                while (!(LockSupport.getBlocker(roleSwitch) instanceof StampedLock) && roleSwitch.isAlive()) {
                    Thread.onSpinWait();
                }
            }
            return isReadOnly.get();
        }

        void awaitRoleSwitch() throws InterruptedException {
            Assert.assertNotNull("test setup: a commit must read the role under its read hold", roleSwitch);
            roleSwitch.join();
            final Throwable failure = switchFailure.get();
            if (failure != null) {
                throw new AssertionError("role switch failed", failure);
            }
        }

        void revertDemote() {
            isReadOnly.set(false);
        }

        private void switchRole() {
            try {
                final Lock writeLock = getRoleSwitchWriteLock();
                writeLock.lock();
                try {
                    onGranted.accept(this);
                    isReadOnly.set(isDemote);
                } finally {
                    writeLock.unlock();
                }
            } catch (Throwable th) {
                switchFailure.set(th);
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
