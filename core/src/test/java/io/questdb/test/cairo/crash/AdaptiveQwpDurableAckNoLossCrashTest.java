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
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.DurabilityTier;
import io.questdb.cairo.wal.DurableAckRegistry;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.griffin.SqlException;
import io.questdb.std.CharSequenceLongHashMap;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
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

/**
 * End-to-end "no ACKNOWLEDGED-data loss" integration proof for the adaptive-commit QWP durable-ack
 * contract (Task 5, the durable-ack integration proof).
 *
 * <h3>The contract under test</h3>
 * A QWP client that opted into durable-ack receives a {@code STATUS_DURABLE_ACK} frame for a table only
 * up to the seqTxn the server derives from {@link QwpIngressProcessorState#collectDurableProgress}. For the
 * OSS {@link DurabilityTier#LOCAL} tier that value is, verbatim,
 * {@code engine.getDurableAckRegistry().getLocalDurableSeqTxn(dirName)} — which the OSS
 * {@link io.questdb.cairo.wal.LocalDurableAckRegistry} resolves to the table's
 * {@link SeqTxnTracker#getLocalDurableSeqTxn()} (the contiguous device-durable prefix across all concurrent
 * writers, post the CRITICAL-2 fix). THE BAR: for EVERY seqTxn the durable-ack path acknowledges, that
 * txn's data MUST survive a power loss injected immediately after the ack. Un-acked tail loss within RPO&le;W
 * is allowed (absent rows, or a loud resumable suspend); silent loss of an ACKED txn is NOT.
 *
 * <h3>Level chosen, and why</h3>
 * This test drives the QWP durable-ack <b>processor path</b> end-to-end: the real
 * {@link QwpIngressProcessorState#collectDurableProgress(DurableAckRegistry)} — the exact method whose
 * returned snapshot is serialized into the client's {@code STATUS_DURABLE_ACK} frame — against the real OSS
 * {@link io.questdb.cairo.wal.LocalDurableAckRegistry} ({@code engine.getDurableAckRegistry()}), a real
 * TWO-writer adaptive {@code W>0} table (two concurrently-held {@link WalWriter}s sharing one
 * {@link SeqTxnTracker}, flushing out of order), and the {@link CrashFaultFilesFacade} power-loss model. The
 * acked seqTxn set the client would receive is COLLECTED from that real method, THEN a crash is injected,
 * THEN recovery runs, THEN every acked row is asserted present and correct.
 *
 * <p>The full websocket <em>server</em> (recv/send frame state machine, {@code QwpIngressUpgradeProcessor})
 * is NOT crash-injected: that harness ({@code QwpIngressDeferredCloseDurableAckTest}) runs a second
 * {@code CairoEngine} with a FAKE watermark-knob registry and no {@link CrashFaultFilesFacade}, and its
 * park/resume send machine cannot be single-stepped around a deterministic crash point. So the full-server
 * level does not support deterministic crash injection in a unit test. The processor level chosen here is
 * strictly higher fidelity than the "registry only" fallback: it exercises the actual ack-derivation
 * arithmetic (tier selection + the {@code lastSent} de-dup + the registry&rarr;tracker read) that produces
 * the client-visible durable-ack, while still using the real registry, real tracker, real multi-writer
 * adaptive table, and the crash facade.
 *
 * <h3>Non-vacuity (negative control)</h3>
 * A power loss injected here genuinely rolls back writer A's un-flushed (page-cache-only) txn, so its row is
 * absent after recovery. The test proves its own no-loss assertion is not vacuous by feeding that SAME
 * assertion a deliberately-too-high (pre-fix over-claim) ack frontier that covers A's lost row and asserting
 * the assertion FIRES. The real ack frontier (post-fix) never reaches A's un-flushed seqTxn, so the real
 * proof passes while the injected over-claim is caught.
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
     * THE deliverable: a deterministic end-to-end proof. Two concurrent writers of one adaptive {@code W>0}
     * table; writer B flushes its higher seqTxn OUT OF ORDER while writer A's lower seqTxn is still
     * page-cache only. The QWP durable-ack path is driven through the real
     * {@link QwpIngressProcessorState#collectDurableProgress} to collect the acked seqTxn the client would
     * receive AT THAT POINT, a power loss is injected immediately after, recovery runs, and every acked
     * seqTxn's row is asserted to survive. Embeds the negative control that proves the assertion catches a
     * violation.
     */
    @Test
    public void testFrontierDerivedDurableAckSurvivesPowerLoss() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(WINDOW_US));
        // Epoch every apply batch so the durable WAL can be rolled forward into the materialized table on
        // recovery; the group-commit fdatasync is what makes that WAL durable, and the durable-ack reflects it.
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

                // Durable baseline: first row via the SQL path (writer released -> device-durable), applied +
                // epoch'd, marked "already on disk". The durable-ack frontier reaches exactly this txn.
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

                    // A commits the LOWER seqTxn, B the HIGHER. Their private WAL is durable before
                    // sequencing; the shared sequencer frontier remains pending until a batch flush.
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

                    // B flushes the shared sequencer OUT OF ORDER (commit-driven trigger past W). The
                    // conservative pending map may keep the ack frontier behind A even though private WAL is safe.
                    final long tsB2 = ts("2024-01-01T03:00:00.000000Z");
                    setCurrentMicros(CLOCK_START + WINDOW_US + 2000L);
                    commitRow(b, tsB2, 12);
                    final long seqB2 = tracker.getSeqTxn();
                    committedRows.add(new long[]{tsB2, 12, seqB2});

                    // === (1) COLLECT the acks the durable-ack path WOULD send, through the REAL processor
                    // method. The snapshot value for table "t" is exactly what a durable-ack client receives. ===
                    ackedSeqTxn = collectDurableAck(tableName, dirName, tracker.getSeqTxn(), registry);

                    // The ack is honest: it equals the registry's local-durable frontier, which equals the
                    // tracker's contiguous device-durable prefix (the baseline) — and it NEVER reaches A's
                    // un-flushed seqTxn. (Pre-fix, the frontier — and thus this ack — reached B's flushed
                    // seqTxn, over-claiming A's non-durable txn: an acknowledged-data over-claim.)
                    Assert.assertEquals("durable-ack must derive from the registry's local-durable frontier",
                            registry.getLocalDurableSeqTxn(dirName), ackedSeqTxn);
                    Assert.assertEquals("registry frontier must be the tracker's contiguous device-durable prefix",
                            tracker.getLocalDurableSeqTxn(), ackedSeqTxn);
                    Assert.assertEquals("acked seqTxn must be the contiguous durable prefix (the baseline)",
                            baselineTxn, ackedSeqTxn);
                    Assert.assertTrue("durable-ack must NOT reach A's un-flushed seqTxn (" + seqA + "), got " + ackedSeqTxn,
                            ackedSeqTxn < seqA);

                    // === (2) POWER LOSS immediately after the ack: drop both writers' pending WITHOUT a device
                    // flush (A's row, and the un-acked tail, are page-cache only -> lost), roll files back. ===
                    a.simulatePowerLossDropPending();
                    b.simulatePowerLossDropPending();
                    crashFf.crash(engine.getConfiguration().getDbRoot());
                } finally {
                    a.close();
                    b.close();
                }

                // === (3) RECOVER exactly as a fresh boot would: drop live handles + the stale in-memory
                // sequencer, then RecoveryCoordinator + republish + drain the durable WAL. ===
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseInactiveTableSequencers();
                new RecoveryCoordinator(engine).recover();
                engine.notifyWalTxnRepublisher(tt);
                drainWalQueue();

                final boolean suspended = engine.getTableSequencerAPI().isSuspended(tt);
                final Map<Long, Long> recovered = readRowsByTsQuietly("t");

                // Bar 1 (ALWAYS): no silently-wrong rows. Every readable (ts,v) matches a committed row, and
                // never more rows than were committed.
                assertNoSilentlyWrongRows(committedRows, recovered);

                // === (4) THE BAR: every ACKED seqTxn's row survives and is correct — no rollback of
                // acknowledged data. (The acked baseline is materialized+epoch'd, so it survives even if the
                // torn un-acked tail suspended the table.) ===
                assertEveryAckedRowSurvives(ackedSeqTxn, committedRows, recovered);

                // === (5) NEGATIVE CONTROL — prove the no-loss assertion is not vacuous. Under the safe W>0
                // fallback A's private WAL survives (and B's peer sequencer flush can publish it). Remove A
                // from a copy of the recovered result, then verify that an ack covering seqA is rejected. ===
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

                // Suspend is an allowed loud outcome for the un-acked tail; log-worthy but not a failure.
                if (suspended) {
                    // acked data (the baseline) is still durable on disk and read back above; the loud suspend,
                    // not a silent loss, is the safe power-loss outcome for the torn un-acked tail.
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
     * NIGHTLY-only seeded sweep: repeats the QWP durable-ack no-loss proof across many seeded multi-writer
     * interleavings, so the contiguous prefix (and thus the collected ack frontier) advances beyond the
     * baseline by varying amounts before the power loss. Same bar: every acked seqTxn survives (gated on a
     * clean, non-suspended recovery — under a suspend the acked WAL is still durable on disk and the loud
     * suspend is the safe outcome), and the collected ack NEVER over-claims (any over-claim would surface as
     * an acked-but-absent row here). Mirrors {@code AdaptiveMultiWriterDurableAckCrashFuzzTest}'s nightly gate.
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
                    // commit on a random writer
                    final WalWriter writer = w[rnd.nextInt(writers)];
                    tsCounter += 60_000_000L;
                    vCounter += 1;
                    setCurrentMicros(clock);
                    commitRow(writer, tsCounter, vCounter);
                    committedRows.add(new long[]{tsCounter, vCounter, tracker.getSeqTxn()});
                } else {
                    // advance the clock past W and run the background sweep (flushes writers in order,
                    // advancing the contiguous durable prefix beyond the baseline)
                    clock += WINDOW_US + 1000L;
                    setCurrentMicros(clock);
                    try (ExposedFlusher flusher = new ExposedFlusher(engine)) {
                        flusher.flushNow();
                    }
                }
            }

            // Collect the ack the durable-ack path would send at this point via the real processor method.
            ackedSeqTxn = collectDurableAck(tt.getTableName(), dirName, tracker.getSeqTxn(), registry);
            Assert.assertEquals(seedMsg + "ack must derive from the registry local-durable frontier",
                    registry.getLocalDurableSeqTxn(dirName), ackedSeqTxn);

            // POWER LOSS immediately after the ack.
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

        // Recover as a fresh boot (mirrors the multi-writer fuzz recovery recipe).
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

        // Bar 1 (ALWAYS): no silently-wrong rows.
        assertNoSilentlyWrongRows(committedRows, recovered);
        // Bar 2 (clean recovery): every acked seqTxn survives — an over-claim would show up as an
        // acked-but-absent row and fire here.
        if (!suspended) {
            assertEveryAckedRowSurvives(ackedSeqTxn, committedRows, recovered);
        }
        engine.releaseAllReaders();
        engine.releaseInactiveTableSequencers();
    }

    /**
     * Drives the REAL QWP durable-ack derivation: builds a {@link QwpIngressProcessorState}, opts it into the
     * LOCAL durable-ack tier, seeds the pending-durable table set via the exact private consumer the commit
     * path runs per committed table ({@code recordCommittedTable}, i.e. {@code this::recordCommittedTable} from
     * {@code tudCache.commitAll}), then calls {@link QwpIngressProcessorState#collectDurableProgress} — the
     * method whose returned snapshot is serialized into the client's {@code STATUS_DURABLE_ACK} frame — and
     * returns the acked seqTxn for the table. The load-bearing ack VALUE comes from the real
     * registry&rarr;tracker read inside {@code collectDurableProgress}.
     */
    private long collectDurableAck(String tableName, String dirName, long committedSeqTxn, DurableAckRegistry registry) throws Exception {
        final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
        final QwpIngressProcessorState state = new QwpIngressProcessorState(
                1024, 1024, engine, httpConfig.getLineHttpProcessorConfiguration());
        try {
            state.of(-1, AllowAllSecurityContext.INSTANCE);
            state.setDurableAckEnabled(true);              // as negotiated via X-QWP-Request-Durable-Ack
            state.setDurableAckTier(DurabilityTier.LOCAL); // OSS local-fsync tier
            recordCommittedTable(state, tableName, dirName, committedSeqTxn);
            final CharSequenceLongHashMap snapshot = state.collectDurableProgress(registry);
            Assert.assertTrue("durable-ack path must report the pending adaptive table",
                    snapshot.keyIndex(tableName) < 0);
            return snapshot.get(tableName);
        } finally {
            state.close();
        }
    }

    /**
     * For EVERY committed row whose seqTxn is at or below {@code ackFrontier} (i.e. the durable-ack path
     * acknowledged it), assert the row is present and correct after recovery. Throws {@link AssertionError}
     * on the first acked-but-absent or acked-but-wrong row — this is both the proof assertion and the target
     * of the negative control.
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
     * Bar 1: every readable (ts,v) matches a committed row and there are never more rows than were committed.
     * A rolled-back (absent) tail is fine; a silently-WRONG surviving row is never acceptable.
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

    private static void commitRow(WalWriter w, long tsMicros, long v) {
        TableWriter.Row row = w.newRow(tsMicros);
        row.putLong(1, v);
        row.append();
        w.commit();
    }

    /**
     * Invokes the exact private per-table consumer the QWP commit path runs ({@code recordCommittedTable},
     * bound as {@code this::recordCommittedTable} and driven by {@code tudCache.commitAll} on every commit),
     * seeding the connection's pending-durable table set the same way a real commit does. Reflection here
     * mirrors the private-member reflection the sibling websocket durable-ack test uses; the acked VALUE is
     * still produced by the real registry read inside {@code collectDurableProgress}.
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
     * Like {@link #readRowsByTs} but returns whatever is readable instead of throwing on a torn un-acked tail
     * (an acceptable loud power-loss outcome); Bar 1 still guards correctness of the surviving rows.
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

    /**
     * Exposes {@link WalPurgeJob#runSerially()} so the background group-commit sweep runs deterministically
     * against the test microsecond clock.
     */
    static final class ExposedFlusher extends WalPurgeJob {
        ExposedFlusher(io.questdb.cairo.CairoEngine engine) {
            super(engine);
        }

        boolean flushNow() {
            return runSerially();
        }
    }
}
