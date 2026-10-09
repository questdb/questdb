/*******************************************************************************
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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.DurabilityTier;
import io.questdb.cairo.wal.DurableAckRegistry;
import io.questdb.cairo.wal.LocalDurableAckRegistry;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.griffin.SqlException;
import io.questdb.std.CharSequenceLongHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.cairo.wal.GroupCommitTestUtils.ExposedFlusher;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.questdb.test.cairo.wal.GroupCommitTestUtils.commitRow;

/**
 * End-to-end check that the adaptive-commit QWP durable-ack contract never acknowledges data a power loss can
 * lose.
 *
 * <h3>The contract</h3>
 * A QWP client that opted into durable-ack receives a {@code STATUS_DURABLE_ACK} frame for a table only up to the
 * seqTxn the server derives from {@link QwpIngressProcessorState#collectDurableProgress}. For the OSS
 * {@link DurabilityTier#LOCAL} tier that value is
 * {@code engine.getDurableAckRegistry().getLocalDurableSeqTxn(dirName)}, which
 * {@link io.questdb.cairo.wal.LocalDurableAckRegistry} resolves to the table's
 * {@link SeqTxnTracker#getLocalDurableSeqTxn()}: the contiguous device-durable prefix across all concurrent writers.
 * For every seqTxn the durable-ack path acknowledges, the txn's data survives a power loss injected right after the
 * ack. Losing the unacknowledged tail within RPO&le;W is allowed (absent rows, or a visible, resumable suspend); losing
 * an acknowledged txn is not.
 *
 * <h3>Level</h3>
 * The test drives the QWP durable-ack processor path: the real
 * {@link QwpIngressProcessorState#collectDurableProgress(DurableAckRegistry)}, whose snapshot is serialized into
 * the client's {@code STATUS_DURABLE_ACK} frame, against the real {@link io.questdb.cairo.wal.LocalDurableAckRegistry},
 * a two-writer adaptive {@code W>0} table (two concurrently held {@link WalWriter}s sharing one
 * {@link SeqTxnTracker}, flushing out of order), and the {@link CrashFaultFilesFacade} power-loss model. It
 * collects the acked seqTxn the client would receive, injects a crash, recovers, and asserts every acked row is
 * present and correct.
 *
 * <p>The full websocket server (the recv/send frame state machine, {@code QwpIngressUpgradeProcessor}) is not
 * crash-injected: its harness ({@code QwpIngressDeferredCloseDurableAckTest}) runs a second {@code CairoEngine}
 * with a fake watermark registry and no {@link CrashFaultFilesFacade}, and its park/resume send machine cannot be
 * single-stepped around a deterministic crash point. The processor level still exercises the ack derivation
 * (tier selection, the {@code lastSent} de-duplication, the registry&rarr;tracker read) that produces the
 * client-visible durable-ack.
 *
 * <h3>Negative control</h3>
 * The injected power loss rolls back writer A's unflushed (page-cache-only) txn. Feeding the same no-loss
 * assertion an ack frontier that covers A's lost row makes it fire, so the assertion is not vacuous.
 */
public class AdaptiveQwpDurableAckNoLossCrashTest extends AbstractCrashConsistencyTest {

    private static final long CLOCK_START = 1_000_000L;
    private static final String NIGHTLY_PROP = "questdb.fuzz.nightly";
    private static final long WINDOW_US = 1_000_000L; // 1s group window driven by the test microsecond clock

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(Boolean.getBoolean(NIGHTLY_PROP) ? 20 * 60 * 1000L : 3 * 60 * 1000L, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    /**
     * A table dropped while a connection still owes it a durable ack is acked as covered: the registry
     * reports its directory as {@link DurableAckRegistry#TABLE_GONE}, and the client trims every frame
     * it committed to the table. That ack is honest only if the DROP survives a power loss; otherwise the
     * table could come back after the crash, missing the un-fsynced commit the client no longer holds. Under
     * W&gt;0 the drop's own sequencer record is deferred like any commit; the name registry makes the drop
     * durable by syncing {@code tables.d} before the directory stops resolving. Power loss right after the
     * ack, then a fresh engine on the same root: the table stays dropped.
     * {@link #testDroppedTableComesBackWhenItsDropIsNotDurable} is the negative control.
     */
    @Test
    public void testDroppedTableAckSurvivesPowerLoss() throws Exception {
        Assert.assertFalse("the acked-as-gone table must stay dropped after a power loss",
                isDroppedTableBackAfterPowerLoss(new CrashFaultFilesFacade(), null));
    }

    /**
     * Negative control for {@link #testDroppedTableAckSurvivesPowerLoss}: the same scenario with the
     * drop's {@code tables.d} record left in the page cache (its MS_SYNC downgraded to MS_ASYNC). The power
     * loss then undoes the drop and the table comes back, which shows the same assertions can see the outcome
     * the acked-as-gone path relies on never happening.
     */
    @Test
    public void testDroppedTableComesBackWhenItsDropIsNotDurable() throws Exception {
        final boolean[] isSyncDowngraded = {false};
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade() {
            @Override
            public void msync(long addr, long len, boolean async) {
                super.msync(addr, len, async || isSyncDowngraded[0]);
            }
        };
        Assert.assertTrue("control: a drop that is not durable must be undone by the power loss",
                isDroppedTableBackAfterPowerLoss(ff, isSyncDowngraded));
    }

    /**
     * Two concurrent writers of one adaptive {@code W>0} table; writer B flushes its higher seqTxn out of order
     * while writer A's lower seqTxn is still page-cache only. The test collects the acked seqTxn the client would
     * receive at that point through the real {@link QwpIngressProcessorState#collectDurableProgress}, injects a
     * power loss right after, recovers, and asserts every acked seqTxn's row survives. Includes the negative
     * control.
     */
    @Test
    public void testFrontierDerivedDurableAckSurvivesPowerLoss() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(WINDOW_US));
        // Epoch every apply batch so recovery can roll the durable WAL forward into the materialized table; the
        // group-commit fdatasync makes that WAL durable, and the durable-ack reflects it.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            runWithCrashFacade(() -> {
                setCurrentMicros(CLOCK_START);
                execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
                final TableToken tt = engine.verifyTableName("t");
                final String dirName = tt.getDirName();
                final String tableName = tt.getTableName();
                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
                final DurableAckRegistry registry = engine.getDurableAckRegistry();
                Assert.assertTrue("OSS default registry must be enabled for durable-ack", registry.isEnabled());

                // Durable baseline: the first row via the SQL path (writer released, so device-durable), applied with
                // an epoch and marked as on disk. The durable-ack frontier reaches exactly this txn.
                final long ts0 = ts("2024-01-01T00:00:00.000000Z");
                setCurrentMicros(CLOCK_START);
                execute("insert into t values ('2024-01-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                final long baselineTxn = engine.getTableSequencerAPI().lastTxn(tt);
                Assert.assertEquals("baseline commit must be device-durable after the clean release flush",
                        baselineTxn, tracker.getLocalDurableSeqTxn());
                markDurableBaseline();

                final List<long[]> committedRows = new ArrayList<>();
                committedRows.add(new long[]{ts0, 1, baselineTxn});

                final long seqA;
                final long tsA = ts("2024-01-01T01:00:00.000000Z");
                final long ackedSeqTxn;
                WalWriter a = engine.getWalWriter(tt);
                WalWriter b = engine.getWalWriter(tt);
                try {
                    Assert.assertNotEquals("two held writers must be distinct WALs sharing one tracker",
                            a.getWalId(), b.getWalId());

                    // A commits the lower seqTxn, B the higher. Their private WAL is durable before sequencing; the
                    // shared sequencer frontier stays pending until a batch flush.
                    setCurrentMicros(CLOCK_START + 1000L);
                    commitRow(a, tsA, 10);
                    seqA = tracker.getSeqTxn();
                    committedRows.add(new long[]{tsA, 10, seqA});

                    final long tsB1 = ts("2024-01-01T02:00:00.000000Z");
                    setCurrentMicros(CLOCK_START + 2000L);
                    commitRow(b, tsB1, 11);
                    final long seqB1 = tracker.getSeqTxn();
                    Assert.assertEquals("B must have sequenced right after A", seqA + 1, seqB1);
                    committedRows.add(new long[]{tsB1, 11, seqB1});

                    // B flushes the shared sequencer out of order (commit-driven trigger past W). The conservative
                    // pending map may keep the ack frontier behind A even though A's private WAL is safe.
                    final long tsB2 = ts("2024-01-01T03:00:00.000000Z");
                    setCurrentMicros(CLOCK_START + WINDOW_US + 2000L);
                    commitRow(b, tsB2, 12);
                    final long seqB2 = tracker.getSeqTxn();
                    committedRows.add(new long[]{tsB2, 12, seqB2});

                    // (1) Collect the acks the durable-ack path would send, through the real processor method. The
                    // snapshot value for table "t" is what a durable-ack client receives.
                    ackedSeqTxn = collectDurableAck(tableName, dirName, tracker.getSeqTxn(), registry);

                    // The ack equals the registry's local-durable frontier, which equals the tracker's contiguous
                    // device-durable prefix (the baseline), and it never reaches A's unflushed seqTxn. An ack that
                    // reached B's flushed seqTxn would cover A's non-durable txn.
                    Assert.assertEquals("durable-ack must derive from the registry's local-durable frontier",
                            registry.getLocalDurableSeqTxn(dirName), ackedSeqTxn);
                    Assert.assertEquals("registry frontier must be the tracker's contiguous device-durable prefix",
                            tracker.getLocalDurableSeqTxn(), ackedSeqTxn);
                    Assert.assertEquals("acked seqTxn must be the contiguous durable prefix (the baseline)",
                            baselineTxn, ackedSeqTxn);
                    Assert.assertTrue("durable-ack must NOT reach A's un-flushed seqTxn (" + seqA + "), got " + ackedSeqTxn,
                            ackedSeqTxn < seqA);

                    // (2) Power loss right after the ack: drop both writers' pending without a device flush (A's row
                    // and the unacked tail are page-cache only, so lost), roll files back.
                    a.simulatePowerLossDropPending();
                    b.simulatePowerLossDropPending();
                    crashFf.crash(engine.getConfiguration().getDbRoot());
                } finally {
                    a.close();
                    b.close();
                }

                // (3) Recover as a fresh boot would: drop live handles and the stale in-memory sequencer, then
                // RecoveryCoordinator, republish, and drain the durable WAL.
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseInactiveTableSequencers();
                new RecoveryCoordinator(engine).recover();
                engine.notifyWalTxnRepublisher(tt);
                drainWalQueue();

                final boolean suspended = engine.getTableSequencerAPI().isSuspended(tt);
                final Map<Long, Long> recovered = readRowsByTsQuietly("t");

                // Bar 1 (always): no wrong rows. Every readable (ts,v) matches a committed row, and there are never
                // more rows than were committed.
                assertNoSilentlyWrongRows(committedRows, recovered);

                // (4) Every acked seqTxn's row survives and is correct. The acked baseline is materialized with an
                // epoch, so it survives even if the torn unacked tail suspended the table.
                assertEveryAckedRowSurvives(ackedSeqTxn, committedRows, recovered);

                // (5) Negative control: the no-loss assertion is not vacuous. Under the safe W>0 fallback A's private
                // WAL survives (and B's peer sequencer flush can publish it). Remove A from a copy of the recovered
                // result, then check that an ack covering seqA is rejected.
                Assert.assertEquals("safe fallback must preserve A's private WAL row",
                        Long.valueOf(10L), recovered.get(tsA));
                final Map<Long, Long> missingAckedRow = new HashMap<>(recovered);
                missingAckedRow.remove(tsA);
                boolean overClaimCaught = false;
                try {
                    assertEveryAckedRowSurvives(seqA, committedRows, missingAckedRow);
                } catch (AssertionError expected) {
                    overClaimCaught = true;
                }
                Assert.assertTrue(
                        "NEGATIVE CONTROL FAILED: assertEveryAckedRowSurvives did NOT fire for a deliberately-too-high"
                                + " ack frontier (" + seqA + ") when an acknowledged row is missing"
                                + " — the no-acknowledged-loss proof would be vacuous",
                        overClaimCaught);

                // A suspend is an allowed, visible outcome for the unacked tail, not a failure.
                if (suspended) {
                    // The acked baseline is still durable on disk and read back above; the visible suspend is the safe
                    // power-loss outcome for the torn unacked tail.
                    Assert.assertTrue("even under a suspend, the acked baseline must have read back",
                            recovered.containsKey(ts0));
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setCurrentMicros(-1);
        }
    }

    /**
     * Nightly only: repeats the durable-ack no-loss check across many seeded multi-writer interleavings, so the
     * contiguous prefix, and with it the collected ack frontier, advances beyond the baseline by varying amounts
     * before the power loss. Every acked seqTxn survives a clean, non-suspended recovery (under a suspend the acked
     * WAL is still durable on disk), so an ack that over-claims shows up as an acked but absent row.
     */
    @Test
    public void testFrontierDerivedDurableAckSurvivesPowerLossSweepNightly() throws Exception {
        Assume.assumeTrue("QWP durable-ack no-loss sweep is nightly-only; run with -D" + NIGHTLY_PROP + "=true",
                Boolean.getBoolean(NIGHTLY_PROP));
        final long[][] seeds = {{101L, 202L}, {303L, 404L}, {505L, 606L}, {717L, 818L}, {929L, 1030L}, {1111L, 1212L}};
        for (long[] s : seeds) {
            runSeededSweep(s[0], s[1], 24);
        }
    }

    private void runSeededSweep(long s0, long s1, int ops) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(WINDOW_US));
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        try {
            runWithCrashFacade(() -> seededSweepBody(s0, s1, ops));
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setCurrentMicros(-1);
        }
    }

    private void seededSweepBody(long s0, long s1, int ops) throws Exception {
        final String seedMsg = "[seed s0=" + s0 + " s1=" + s1 + "] ";
        final Rnd rnd = new Rnd(s0, s1);
        final String table = "s_" + Math.abs(s0) + "_" + Math.abs(s1);
        long clock = CLOCK_START;
        setCurrentMicros(clock);
        execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal");
        final TableToken tt = engine.verifyTableName(table);
        final String dirName = tt.getDirName();
        final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
        final DurableAckRegistry registry = engine.getDurableAckRegistry();

        // Durable baseline (applied + epoch'd + on disk).
        final long baseTs = ts("2024-01-01T00:00:00.000000Z");
        execute("insert into " + table + " values ('2024-01-01T00:00:00.000000Z', 1)");
        drainWalQueue();
        markDurableBaseline();
        final long baselineTxn = tracker.getLocalDurableSeqTxn();

        final List<long[]> committedRows = new ArrayList<>();
        committedRows.add(new long[]{baseTs, 1, baselineTxn});
        long tsCounter = baseTs;
        long vCounter = 1;

        final int writers = 2 + rnd.nextInt(2); // 2 or 3 concurrent writers on the shared tracker
        final WalWriter[] w = new WalWriter[writers];
        long ackedSeqTxn;
        try {
            for (int i = 0; i < writers; i++) {
                w[i] = engine.getWalWriter(tt);
            }
            for (int op = 0; op < ops; op++) {
                final int roll = rnd.nextInt(100);
                if (roll < 70) {
                    // Commit on a random writer.
                    final WalWriter writer = w[rnd.nextInt(writers)];
                    tsCounter += 60_000_000L;
                    vCounter += 1;
                    setCurrentMicros(clock);
                    commitRow(writer, tsCounter, vCounter);
                    committedRows.add(new long[]{tsCounter, vCounter, tracker.getSeqTxn()});
                } else {
                    // Advance the clock past W and run the background sweep (flushes writers in order, advancing the
                    // contiguous durable prefix beyond the baseline).
                    clock += WINDOW_US + 1000L;
                    setCurrentMicros(clock);
                    try (ExposedFlusher flusher = new ExposedFlusher(engine)) {
                        flusher.flushNow();
                    }
                }
            }

            // Collect the ack the durable-ack path would send at this point, through the real processor method.
            ackedSeqTxn = collectDurableAck(tt.getTableName(), dirName, tracker.getSeqTxn(), registry);
            Assert.assertEquals(seedMsg + "ack must derive from the registry local-durable frontier",
                    registry.getLocalDurableSeqTxn(dirName), ackedSeqTxn);

            // Power loss right after the ack.
            for (WalWriter writer : w) {
                writer.simulatePowerLossDropPending();
            }
        } finally {
            for (WalWriter writer : w) {
                if (writer != null) {
                    writer.close();
                }
            }
        }

        // Recover as a fresh boot would, as in the multi-writer fuzz.
        crashFf.crash(engine.getConfiguration().getDbRoot());
        if (engine.getTableSequencerAPI().isSuspended(tt)) {
            engine.getTableSequencerAPI().getTxnTracker(tt).setUnsuspended();
        }
        engine.getTxnScoreboardPool().remove(tt);
        engine.getTableSequencerAPI().resetForReboot(tt);
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        new RecoveryCoordinator(engine).recover();
        engine.notifyWalTxnRepublisher(tt);
        drainWalQueue();

        final boolean suspended = engine.getTableSequencerAPI().isSuspended(tt);
        final Map<Long, Long> recovered = readRowsByTsQuietly(table);

        // Bar 1 (always): no wrong rows.
        assertNoSilentlyWrongRows(committedRows, recovered);
        // Bar 2 (clean recovery): every acked seqTxn survives; an over-claim shows up as an acked but absent row.
        if (!suspended) {
            assertEveryAckedRowSurvives(ackedSeqTxn, committedRows, recovered);
        }
        engine.releaseAllReaders();
        engine.releaseInactiveTableSequencers();
    }

    /**
     * Drives the QWP durable-ack derivation: builds a {@link QwpIngressProcessorState}, opts it into the LOCAL
     * durable-ack tier, seeds the pending-durable table set through the private consumer the commit path runs per
     * committed table ({@code recordCommittedTable}, bound as {@code this::recordCommittedTable} in
     * {@code tudCache.commitAll}), then calls {@link QwpIngressProcessorState#collectDurableProgress}, whose
     * snapshot is serialized into the client's durable-ack frame (a {@code STATUS_LOCAL_DURABLE_ACK} frame for the
     * LOCAL-only grant here). The ack value comes from the real registry&rarr;tracker read inside
     * {@code collectDurableProgress}.
     * <p>
     * The snapshot is then serialized as the server's send path does for a LOCAL-only grant and parsed back with
     * the pinned client ({@link WebSocketResponse}). The returned value is the client-parsed seqTxn, the number the
     * client's trim watermark advances to, so the no-loss check covers the frame layout and the client's
     * status-byte classification as well as the server-side map.
     */
    private long collectDurableAck(String tableName, String dirName, long committedSeqTxn, DurableAckRegistry registry) throws Exception {
        final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
        final QwpIngressProcessorState state = new QwpIngressProcessorState(
                1024, 1024, engine, httpConfig.getLineHttpProcessorConfiguration());
        try {
            state.of(-1, AllowAllSecurityContext.INSTANCE);
            state.setDurableAckEnabled(true);              // as negotiated via X-QWP-Request-Durable-Ack
            state.setDurableAckTiers(DurabilityTier.LOCAL); // OSS local-fsync tier
            recordCommittedTable(state, tableName, dirName, committedSeqTxn);
            final CharSequenceLongHashMap snapshot = state.collectDurableProgress(registry);
            Assert.assertTrue("durable-ack path must report the pending adaptive table",
                    snapshot.keyIndex(tableName) < 0);
            // Serialize as trySendCollectedDurableAck does for a LOCAL-only grant: the status byte plus
            // the per-table entries, no flavor field. Then let the pinned client read it back.
            final int payloadLen = state.computeDurableAckPayloadSize();
            final long addr = Unsafe.malloc(payloadLen, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(addr, QwpConstants.STATUS_LOCAL_DURABLE_ACK);
                QwpIngressProcessorState.writeTableSeqTxnEntries(addr + 1, snapshot);
                Assert.assertTrue("pinned client must accept the local durable-ack payload as structurally valid",
                        WebSocketResponse.isStructurallyValid(addr, payloadLen));
                final WebSocketResponse response = new WebSocketResponse();
                Assert.assertTrue("pinned client must parse the local durable-ack payload",
                        response.readFrom(addr, payloadLen));
                Assert.assertTrue("pinned client must classify 0x0E as the LOCAL durable ack",
                        response.isLocalDurableAck());
                Assert.assertFalse("0x0E must not be classified as the replicated durable ack",
                        response.isDurableAck());
                Assert.assertEquals("frame must carry exactly the one pending table", 1, response.getTableEntryCount());
                Assert.assertEquals(tableName, response.getTableName(0));
                final long clientAckedSeqTxn = response.getTableSeqTxn(0);
                Assert.assertEquals("client-parsed seqTxn must equal the server snapshot",
                        snapshot.get(tableName), clientAckedSeqTxn);
                return clientAckedSeqTxn;
            } finally {
                Unsafe.free(addr, payloadLen, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            state.close();
        }
    }

    /**
     * Commits one row to a W&gt;0 adaptive table that is not yet locally durable, drops the table, collects
     * the durable ack the connection would receive (it must cover the commit), injects a power loss and
     * boots a fresh engine on the surviving files. Returns whether the dropped table is visible again.
     * {@code isSyncDowngraded}, when given, is raised for the duration of the DROP.
     * <p>
     * {@code tables.d} is mapped once per registry load, so the registry is reloaded after the crash
     * facade is installed: only a mapping made through the facade has its msync modelled.
     */
    private boolean isDroppedTableBackAfterPowerLoss(CrashFaultFilesFacade ff, boolean[] isSyncDowngraded) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(WINDOW_US));
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        final boolean[] isBack = {false};
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            assumeCrashHarnessSupported();
            crashFf = ff;
            crashFf.setDbRoot(root);
            assertMemoryLeak(crashFf, () -> {
                engine.reloadTableNames();
                setCurrentMicros(CLOCK_START);
                execute("create table gone (ts timestamp, v long) timestamp(ts) partition by day wal");
                execute("create table kept (ts timestamp, v long) timestamp(ts) partition by day wal");
                final TableToken tt = engine.verifyTableName("gone");
                final String dirName = tt.getDirName();
                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
                markDurableBaseline();

                final WalWriter w = engine.getWalWriter(tt);
                try {
                    // A commit inside the group window: sequenced, acked, not yet locally durable.
                    setCurrentMicros(CLOCK_START + 1000L);
                    commitRow(w, ts("2024-01-01T00:00:00.000000Z"), 1);
                    final long committedSeqTxn = tracker.getSeqTxn();
                    Assert.assertTrue("test setup: the commit must not be locally durable yet",
                            tracker.getLocalDurableSeqTxn() < committedSeqTxn);

                    if (isSyncDowngraded != null) {
                        isSyncDowngraded[0] = true;
                    }
                    try {
                        execute("drop table gone");
                    } finally {
                        if (isSyncDowngraded != null) {
                            isSyncDowngraded[0] = false;
                        }
                    }
                    final DurableAckRegistry registry = engine.getDurableAckRegistry();
                    Assert.assertEquals(DurableAckRegistry.TABLE_GONE, registry.getLocalDurableSeqTxn(dirName));
                    Assert.assertEquals("the dropped table's pending commit must be acked as covered",
                            committedSeqTxn, collectDurableAck("gone", dirName, committedSeqTxn, registry));

                    // Power loss right after the ack, then boot a fresh engine on what survived.
                    w.simulatePowerLossDropPending();
                    crashFf.crash(engine.getConfiguration().getDbRoot());
                    try (CairoEngine rebooted = new CairoEngine(configuration)) {
                        Assert.assertNotNull("the table that was not dropped must come back",
                                rebooted.getTableTokenIfExists("kept"));
                        isBack[0] = rebooted.getTableTokenIfExists("gone") != null;
                        if (!isBack[0]) {
                            Assert.assertNull(rebooted.getTableTokenByDirName(dirName));
                            Assert.assertEquals(DurableAckRegistry.TABLE_GONE,
                                    LocalDurableAckRegistry.resolveLocalDurableSeqTxn(rebooted, dirName));
                        }
                    }
                } finally {
                    w.close();
                }
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseInactiveTableSequencers();
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setCurrentMicros(-1);
        }
        return isBack[0];
    }

    /**
     * For every committed row whose seqTxn is at or below {@code ackFrontier}, that is, every row the durable-ack
     * path acknowledged, asserts the row is present and correct after recovery. Throws {@link AssertionError} on the
     * first acknowledged row that is absent or wrong; the negative control targets this too.
     */
    private static void assertEveryAckedRowSurvives(long ackFrontier, List<long[]> committedRows, Map<Long, Long> recovered) {
        for (long[] r : committedRows) {
            if (r[2] <= ackFrontier) {
                final Long got = recovered.get(r[0]);
                Assert.assertNotNull("ACKNOWLEDGED-DATA LOSS: acked row (ts=" + r[0] + ", v=" + r[1]
                        + ", seqTxn=" + r[2] + " <= ackFrontier=" + ackFrontier + ") is absent after recovery", got);
                Assert.assertEquals("acked row corrupted after crash (ts=" + r[0] + ")", Long.valueOf(r[1]), got);
            }
        }
    }

    /**
     * Bar 1: every readable (ts,v) matches a committed row and there are never more rows than were committed. A
     * rolled-back (absent) tail is fine; a wrong surviving row is not.
     */
    private static void assertNoSilentlyWrongRows(List<long[]> committedRows, Map<Long, Long> recovered) {
        final Map<Long, Long> committedByTs = new HashMap<>();
        for (long[] r : committedRows) {
            committedByTs.put(r[0], r[1]);
        }
        for (Map.Entry<Long, Long> e : recovered.entrySet()) {
            final Long committedV = committedByTs.get(e.getKey());
            Assert.assertNotNull("recovered a row (ts=" + e.getKey() + ") that was never committed", committedV);
            Assert.assertEquals("silently-wrong row at ts=" + e.getKey(), committedV, e.getValue());
        }
        Assert.assertTrue("recovered more rows than were ever committed",
                recovered.size() <= committedRows.size());
    }

    /**
     * Invokes the private per-table consumer the QWP commit path runs ({@code recordCommittedTable}, bound as
     * {@code this::recordCommittedTable} and driven by {@code tudCache.commitAll} on every commit), seeding the
     * connection's pending-durable table set as a real commit does. The acked value is still produced by the real
     * registry read inside {@code collectDurableProgress}.
     */
    private static void recordCommittedTable(QwpIngressProcessorState state, String tableName, String dirName, long seqTxn) throws Exception {
        final Method m = QwpIngressProcessorState.class.getDeclaredMethod(
                "recordCommittedTable", String.class, String.class, long.class);
        m.setAccessible(true);
        m.invoke(state, tableName, dirName, seqTxn);
    }

    private Map<Long, Long> readRowsByTs(String table) {
        final Map<Long, Long> out = new HashMap<>();
        try (RecordCursorFactory f = select("select ts, v from " + table)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                final Record r = c.getRecord();
                while (c.hasNext()) {
                    out.put(r.getTimestamp(0), r.getLong(1));
                }
            }
        } catch (SqlException e) {
            throw new RuntimeException("readRowsByTs failed for " + table, e);
        }
        return out;
    }

    /**
     * Like {@link #readRowsByTs} but returns whatever is readable instead of throwing on a torn unacknowledged
     * tail (an acceptable, visible power-loss outcome); Bar 1 still checks the surviving rows.
     */
    private Map<Long, Long> readRowsByTsQuietly(String table) {
        try {
            return readRowsByTs(table);
        } catch (CairoException | CairoError | InternalError e) {
            return new HashMap<>();
        } catch (RuntimeException e) {
            if (e.getCause() instanceof CairoException || e.getCause() instanceof CairoError) {
                return new HashMap<>();
            }
            throw e;
        }
    }

    private static long ts(String s) {
        try {
            return MicrosFormatUtils.parseTimestamp(s);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
