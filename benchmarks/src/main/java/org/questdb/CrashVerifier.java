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

package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.std.Chars;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

/**
 * CRASH-CONSISTENCY / POWER-CUT DURABILITY VERIFIER
 * <p>
 * PURPOSE: After CrashIngestWriter is hard-killed (kill -9) or power-cut (dm-flakey drop_writes),
 * this tool reopens the same QuestDB database root and verifies consistency / durability against the
 * deterministic row formulas and the acknowledged watermark recorded in _progress.
 * <p>
 * COMMIT MODE (via -DcommitMode=SYNC|NOSYNC|adaptive, default SYNC — matching the writer):
 * SYNC / NOSYNC — the NON-WAL (bypass wal) path. Reopen, bit-check every row, and assert
 * count % K == 0 (no torn commit) and count >= watermark (acked rows survived). Verdicts:
 * CONSISTENT / LOUD_FAILURE / SILENT_CORRUPTION. Unchanged from the original harness.
 * adaptive — the WAL path. On reopen run the PRODUCTION ADAPTIVE RECOVERY TRIPLE
 * (RecoveryCoordinator.recover() → notifyWalTxnRepublisher → drainWalQueue) so the durable epoch
 * rolls forward exactly as a real reboot does, then bit-check and assert the SP-D4 oracle against
 * the captured (C = committed seqTxn, Wm = localDurableSeqTxn):
 * - No silent corruption (HARD, every mode) — SILENT_CORRUPTION on any wrong value / torn commit.
 * - Clean reopen — a suspend that never clears is a DURABILITY_FAILURE.
 * - W=0 (adaptive == SYNC, zero loss): recovered frontier F >= C ⇒ DURABLE.
 * - W>0 (RPO contract): every acked txn survives (F >= Wm) ⇒ RPO_OK; else DURABILITY_FAILURE.
 * <p>
 * VERDICTS (printed to stdout; the harness parses the first word):
 * CONSISTENT count=<n> watermark=<w>       — SYNC/NOSYNC path, all bars hold.
 * DURABLE ...                              — adaptive W=0, full committed history survived.
 * RPO_OK ...                               — adaptive W>0, every acked txn survived (RPO<=W).
 * DURABILITY_FAILURE ...                   — an acked txn was lost, or a suspend never cleared (exit 3).
 * LOUD_FAILURE: <msg>                      — CairoException on open/query (detected torn state, exit 1).
 * SILENT_CORRUPTION ...                    — wrong value / gap / torn commit boundary (exit 2, SERIOUS).
 * <p>
 * Usage: java -cp benchmarks/target/benchmarks.jar \
 * [-DcommitMode=SYNC|NOSYNC|adaptive] [-Dgroup.window.us=W] [-Depoch.interval.ms=N] \
 * [-Droll.forward.enabled=true|false] \
 * org.questdb.CrashVerifier <db-root>
 */
public class CrashVerifier {

    static final boolean REBASE = Boolean.getBoolean("rebase");

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: CrashVerifier [-DcommitMode=SYNC|NOSYNC|adaptive]"
                    + " [-Dgroup.window.us=W] [-Depoch.interval.ms=N] [-Droll.forward.enabled=BOOL] <db-root>");
            System.exit(1);
        }
        final String dbRoot = args[0];
        final String[] SYMBOLS = CrashIngestWriter.SYMBOLS;

        // Commit mode must match the writer (mode is not stored on disk, so we pass it consistently).
        final String modeProp = System.getProperty("commitMode", "SYNC");
        final int modeInt = CrashIngestWriter.parseCommitMode(modeProp);

        // -Drecover.as=nosync restarts the ENGINE under a different global commit
        // mode than the one the data was written under, while still applying the
        // ADAPTIVE oracle. This is the commit-mode flip as the Java suite frames
        // it: crashed while adaptive, restarted under nosync, must STILL roll
        // forward. The current mode says how the table will be written NEXT; only
        // the durable enrolment record says how its state was LEFT, and recovery
        // has to decide from the record. Skipping the roll-forward here serves the
        // torn pre-flip state -- a silently wrong read, which is worse than a
        // failure to open.
        //
        // Done at RESTART rather than as a mid-run ALTER on purpose: a sequenced
        // `alter table ... set param commit_mode=` published while this process
        // still holds the WalWriter open deadlocks against itself -- the apply job
        // waits on metadata the writer holds, times out at 5s, and SUSPENDS the
        // table. Mirrors AdaptiveCommitModeFlipCrashTest's global-flip arm.
        final String recoverAs = System.getProperty("recover.as", "");
        final int engineMode = recoverAs.isEmpty()
                ? modeInt
                : CrashIngestWriter.parseCommitMode(recoverAs);
        if (!recoverAs.isEmpty()) {
            System.out.println("recover.as=" + recoverAs + ": engine restarts on commitMode="
                    + engineMode + " while applying the " + modeProp + " oracle");
        }
        // Adaptive knobs, kept consistent with the writer so recovery + apply behave identically.
        final long groupWindowUs = Long.getLong("group.window.us", 0L);
        final long epochIntervalMs = Long.getLong("epoch.interval.ms", 1000L);
        // Negative-control hook (spec matrix cell #5): disabling roll-forward must lose/short data.
        final boolean rollForward = Boolean.parseBoolean(System.getProperty("roll.forward.enabled", "true"));

        System.out.println("commitMode=" + modeProp + " (" + modeInt + ")"
                + (modeInt == CommitMode.ADAPTIVE
                ? " group.window.us=" + groupWindowUs + " epoch.interval.ms=" + epochIntervalMs
                  + " roll.forward.enabled=" + rollForward
                : ""));

        final CairoConfiguration cfg = new DefaultCairoConfiguration(dbRoot) {
            @Override
            public int getCommitMode() {
                return engineMode;
            }

            @Override
            public long getAdaptiveCommitGroupWindowUs() {
                return groupWindowUs;
            }

            @Override
            public long getAdaptiveEpochIntervalMs() {
                return epochIntervalMs;
            }

            @Override
            public boolean isAdaptiveRecoveryRollForwardEnabled() {
                return rollForward;
            }
        };

        if (modeInt == CommitMode.ADAPTIVE) {
            verifyAdaptive(cfg, dbRoot, SYMBOLS, groupWindowUs);
        } else {
            verifyNonAdaptive(cfg, dbRoot, SYMBOLS);
        }
    }

    /**
     * SYNC / NOSYNC (NON-WAL) verification — the original, proven path. Reopen, bit-check, assert
     * count on a K-row boundary and count >= watermark (the bare row count from _progress).
     */
    private static void verifyNonAdaptive(CairoConfiguration cfg, String dbRoot, String[] SYMBOLS) throws Exception {
        final int K = CrashIngestWriter.K;

        // Read the acknowledged watermark (bare committed row count) written by CrashIngestWriter.
        long watermark = 0L;
        final File progressFile = new File(dbRoot, "_progress");
        if (progressFile.exists()) {
            try {
                // First line is the bare committed row count in every mode.
                watermark = Long.parseLong(firstLine(progressFile));
            } catch (NumberFormatException e) {
                System.out.println("WARN: could not parse _progress file: " + e.getMessage());
            }
        } else {
            System.out.println("WARN: no _progress file found (killed before first commit?)");
        }
        System.out.println("watermark=" + watermark + " (acknowledged committed rows before kill)");

        final long count;
        try (CairoEngine engine = new CairoEngine(cfg)) {
            // load() swaps the default NO-OP mat view / live view stores for the
            // real ones. Without it matViewStateStore stays NoOpMatViewStateStore,
            // every notifyMatViewBaseTableCommit is silently discarded, and a
            // materialized view is created but NEVER refreshed -- which is exactly
            // how the base/view oracle ended up reading an empty view.
            engine.load();
            final SqlExecutionContextImpl ctx = newRootContext(engine, cfg);
            count = bitCheckRows(engine, ctx, SYMBOLS);
        } catch (CairoException e) {
            // Detected corruption or recovery failure — engine or query threw.
            System.out.println("LOUD_FAILURE: " + e.getMessage());
            System.exit(1);
            return; // unreachable
        }

        // count must be a multiple of K (each commit is exactly K rows); a partial count means an
        // in-flight commit leaked through — silent corruption even if all values happen to be correct.
        if (count % K != 0) {
            System.out.printf(
                    "SILENT_CORRUPTION count=%d is not a multiple of K=%d"
                            + " — partial in-flight commit visible (torn commit boundary)%n",
                    count, K);
            System.exit(2);
        }

        // count may legally EXCEED watermark (committed but _progress not yet written at kill), never less.
        if (count < watermark) {
            System.out.printf(
                    "SILENT_CORRUPTION count=%d < watermark=%d"
                            + " — acknowledged committed rows were lost%n",
                    count, watermark);
            System.exit(2);
        }

        System.out.printf("CONSISTENT count=%d watermark=%d%n", count, watermark);
    }

    /**
     * ADAPTIVE / WAL verification (SP-D4). Reopen, run the production recovery triple, bit-check, and
     * apply the adaptive durability oracle against the captured (C, Wm).
     */
    private static void verifyAdaptive(CairoConfiguration cfg, String dbRoot, String[] SYMBOLS, long W) throws Exception {
        final int K = CrashIngestWriter.K;

        // Parse _progress: line 1 = committed row count; "C=" = committed seqTxn; "Wm=" = local-durable seqTxn.
        long rowsWatermark = 0L;
        long committedSeqTxn = 0L;      // C
        long localDurableSeqTxn = -1L;  // Wm
        final File progressFile = new File(dbRoot, "_progress");
        if (progressFile.exists()) {
            try {
                final List<String> lines = Files.readAllLines(progressFile.toPath(), StandardCharsets.US_ASCII);
                if (!lines.isEmpty()) {
                    rowsWatermark = Long.parseLong(lines.get(0).trim());
                }
                for (String line : lines) {
                    final String t = line.trim();
                    if (t.startsWith("C=")) {
                        committedSeqTxn = Long.parseLong(t.substring(2).trim());
                    } else if (t.startsWith("Wm=")) {
                        localDurableSeqTxn = Long.parseLong(t.substring(3).trim());
                    }
                }
            } catch (NumberFormatException e) {
                System.out.println("WARN: could not parse _progress file: " + e.getMessage());
            }
        } else {
            System.out.println("WARN: no _progress file found (killed before first commit?)");
        }
        System.out.println("watermark rows=" + rowsWatermark
                + " C=" + committedSeqTxn + " Wm=" + localDurableSeqTxn
                + " (C=committed seqTxn, Wm=durable-ack frontier, captured pre-cut)");

        final long count;
        final boolean suspended;
        final long lastTxn;
        final String resolvedDir;
        try (CairoEngine engine = new CairoEngine(cfg)) {
            // load() swaps the default NO-OP mat view / live view stores for the
            // real ones. Without it matViewStateStore stays NoOpMatViewStateStore,
            // every notifyMatViewBaseTableCommit is silently discarded, and a
            // materialized view is created but NEVER refreshed -- which is exactly
            // how the base/view oracle ended up reading an empty view.
            engine.load();
            // CairoEngine.completeInit() already ran RecoveryCoordinator.recover() at construction; we run
            // the PRODUCTION ADAPTIVE RECOVERY TRIPLE explicitly here (idempotent re-run) to be faithful to
            // the reboot path and match AdaptiveGroupCommitCrashTest: recover → republish → drain.
            final TableToken token = engine.verifyTableName(CrashIngestWriter.TABLE_NAME);
            new RecoveryCoordinator(engine).recover();
            engine.notifyWalTxnRepublisher(token);
            drainWalQueue(engine);

            suspended = engine.getTableSequencerAPI().isSuspended(token);
            lastTxn = engine.getTableSequencerAPI().lastTxn(token);
            // Captured INSIDE the engine block: the verdict below needs it to prove
            // which directory the name actually resolved to after recovery.
            resolvedDir = token.getDirName();

            final SqlExecutionContextImpl ctx = newRootContext(engine, cfg);
            count = bitCheckRows(engine, ctx, SYMBOLS);
            // Sibling table, when present: recovered in the SAME recover() pass,
            // so a fault confined to the second table would otherwise be invisible.
            // Its contents must satisfy the same identity + ordering oracle; its
            // COUNT may legitimately differ from the primary's, because the cut can
            // land between the two commits.
            // MAT-VIEW: the view may LAG the base (refresh is async) but must never
            // LEAD it. sum(cnt) over the view is the number of base rows the view
            // believes it has aggregated; if that exceeds the base's recovered row
            // count, the view is reporting work the base no longer has -- a phantom.
            if (CrashIngestWriter.MAT_VIEW) {
                // The in-memory MatViewStateStore is anonymous memory that a crash
                // cannot touch, so a fresh engine must rebuild it from the RECOVERED
                // on-disk _mv.s before the view can be read as the crash left it.
                engine.hydrateMatViewStateStore();
                // Drive the refresh to quiescence first, as
                // AdaptiveMatViewLazyGapCrashSweepTest's drainWalAndMatViewQueues
                // does: the view legitimately LAGS while refresh is async, so
                // judging mid-flight measures the lag, not the invariant.
                try (io.questdb.cairo.mv.MatViewRefreshJob mvRefresh =
                             new io.questdb.cairo.mv.MatViewRefreshJob(0, engine, 1)) {
                    for (int mvi = 0; mvi < CrashIngestWriter.MV_VARIANTS.length; mvi++) {
                        engine.getMatViewStateStore().enqueueIncrementalRefresh(
                                engine.verifyTableName(CrashIngestWriter.MV_VARIANTS[mvi][0]));
                    }
                    // run() == false means "nothing refreshed THIS pass", NOT "the queue is
                    // empty". Stopping at the first false leaves queued tasks unprocessed --
                    // which is exactly why an enqueued RANGE_REFRESH (the surgical repair)
                    // never executed and the view stayed stranded. Drain a bounded number of
                    // passes instead.
                    for (int i = 0; i < 200; i++) {
                        mvRefresh.run();
                    }
                    // The refresh commits to the VIEW's WAL; the view TABLE only shows it once
                    // that WAL is applied. Draining only BEFORE the refresh reads the view table
                    // as it stood before the repair landed -- the same "measured before the work
                    // finished" error as sampling the base count ahead of the refresh drain.
                    drainWalQueue(engine);
                } catch (Throwable t) {
                    System.out.println("LOUD_FAILURE: mat view refresh failed during reconciliation: " + t);
                    System.exit(1);
                }
                try (SqlCompilerImpl c2 = new SqlCompilerImpl(engine);
                     RecordCursorFactory f2 = c2.compile(
                                     "select sum(cnt) total from " + CrashIngestWriter.MV_NAME, ctx)
                             .getRecordCursorFactory();
                     RecordCursor cur = f2.getCursor(ctx)) {
                    long viewTotal = Long.MIN_VALUE;
                    if (cur.hasNext()) {
                        viewTotal = cur.getRecord().getLong(0);
                    }
                    // sum() over an EMPTY view returns NULL (Long.MIN_VALUE). Left
                    // unhandled, `viewTotal > count` compares MIN_VALUE and passes
                    // unconditionally -- the check fires but can never fail. An
                    // empty view is not evidence of consistency; it means the
                    // refresh never ran and the dimension was not exercised.
                    if (viewTotal == Long.MIN_VALUE) {
                        System.out.println("LOUD_FAILURE: matview is EMPTY after recovery"
                                + " (refresh never ran; base recovered " + count
                                + " rows) - this run proves nothing about base/view consistency");
                        System.exit(1);
                    }
                    // RE-READ THE BASE HERE, adjacent to the view read and after
                    // everything above has gone quiescent. `count` was sampled
                    // BEFORE the refresh drain, and the refresh drives further WAL
                    // apply on the base -- so comparing the view against `count`
                    // compares two DIFFERENT MOMENTS and reports a "lead" that is
                    // purely the base advancing between the two reads. That is what
                    // produced "view aggregates 708000 but base recovered 707000":
                    // a stale snapshot, not a phantom. A fresh replay of the same
                    // boundary showed the healthy shape (base 470000, view 469000).
                    final long baseNow = bitCheckRows(engine, ctx, SYMBOLS);
                    // EVERY view type, not just the immediate one. The surgical repair runs from the
                    // load path and is type-agnostic BY CONSTRUCTION -- which is worth nothing until
                    // each type has actually been crashed and checked. A view may legitimately LAG,
                    // or be EMPTY (a manual/deferred view that never refreshed); what it must never
                    // do is LEAD the base while reporting itself usable.
                    boolean anyPhantom = false;
                    for (int mvi = 0; mvi < CrashIngestWriter.MV_VARIANTS.length; mvi++) {
                        final String mvName = CrashIngestWriter.MV_VARIANTS[mvi][0];
                        long total = Long.MIN_VALUE;
                        try (SqlCompilerImpl vc = new SqlCompilerImpl(engine);
                             RecordCursorFactory vf = vc.compile("select sum(cnt) total from " + mvName, ctx).getRecordCursorFactory();
                             RecordCursor vcur = vf.getCursor(ctx)) {
                            if (vcur.hasNext()) {
                                total = vcur.getRecord().getLong(0);
                            }
                        }
                        String st = "<unread>";
                        try (SqlCompilerImpl sc = new SqlCompilerImpl(engine);
                             RecordCursorFactory sf = sc.compile(
                                     "select view_status from materialized_views() where view_name = '" + mvName + "'", ctx).getRecordCursorFactory();
                             RecordCursor scur = sf.getCursor(ctx)) {
                            if (scur.hasNext()) {
                                final CharSequence v = scur.getRecord().getStrA(0);
                                st = v != null ? v.toString() : "<null>";
                            }
                        }
                        final boolean valid = Chars.equalsIgnoreCase(st, "valid");
                        final String verdict;
                        if (total == Long.MIN_VALUE) {
                            verdict = "EMPTY";
                        } else if (total > baseNow && valid) {
                            verdict = "PHANTOM";
                            anyPhantom = true;
                        } else if (total > baseNow) {
                            verdict = "leads-but-" + st;
                        } else if (total == baseNow) {
                            verdict = "EXACT";
                        } else {
                            verdict = "lags-by-" + (baseNow - total);
                        }
                        System.out.println("  MV " + mvName + " [" + CrashIngestWriter.MV_VARIANTS[mvi][1]
                                + "] total=" + (total == Long.MIN_VALUE ? "null" : total)
                                + " base=" + baseNow + " status=" + st + " -> " + verdict);
                    }
                    if (anyPhantom) {
                        System.out.println("SILENT_CORRUPTION matview leads base while reporting valid (see MV lines)");
                        System.exit(2);
                    }

                } catch (SqlException e) {
                    System.out.println("LOUD_FAILURE: matview unreadable after recovery: "
                            + e.getFlyweightMessage());
                    System.exit(1);
                }
            }

            if (CrashIngestWriter.SIBLING_TABLE) {
                try {
                    long siblingCount = bitCheckRows(engine, ctx, SYMBOLS, CrashIngestWriter.SIBLING_NAME);
                    System.out.println("sibling " + CrashIngestWriter.SIBLING_NAME
                            + " rows=" + siblingCount + " (primary=" + count + ")");
                } catch (SqlException e) {
                    System.out.println("SILENT_CORRUPTION sibling table unreadable after recovery: "
                            + e.getFlyweightMessage());
                    System.exit(2);
                }
            }
        } catch (CairoException e) {
            System.out.println("LOUD_FAILURE: " + e.getMessage());
            System.exit(1);
            return; // unreachable
        }

        // HARD BAR (every mode/W): no torn commit boundary.
        if (count % K != 0) {
            System.out.printf(
                    "SILENT_CORRUPTION count=%d is not a multiple of K=%d"
                            + " — partial in-flight commit visible (torn commit boundary)%n",
                    count, K);
            System.exit(2);
        }

        // Recovered frontier, in SEQUENCER TXNS.
        //
        // `count / K` was a PROXY for this and is wrong whenever a txn is not a
        // K-row data txn. Structural DDL (ADD COLUMN) is sequenced as its own txn
        // carrying ZERO rows, so Wm -- a real seqTxn -- counts it while count/K
        // does not. The frontier then appears to lag the ack by exactly the number
        // of structural txns and the run reports DURABILITY_FAILURE on healthy
        // data: observed as F=807 < Wm=815 across every boundary of a DDL run,
        // the 8 being the ADD COLUMNs performed.
        //
        // lastTxn is the sequencer's own recovered frontier -- the same unit Wm is
        // measured in -- so the RPO bar compares like with like. The row-level
        // identity/contiguity checks above remain the ground truth for WHAT
        // survived; this is only the frontier the durability bar is drawn against.
        final long F = lastTxn;
        System.out.printf("recovered: count=%d F=%d lastTxn=%d C=%d Wm=%d W=%d suspended=%b%n",
                count, F, lastTxn, committedSeqTxn, localDurableSeqTxn, W, suspended);

        // REBASE WAL: a DESTRUCTIVE operation that deliberately discards pending WAL
        // transactions, so the F >= Wm durability bar does not apply and asserting it
        // would fail the harness on correct behaviour. What MUST hold is the
        // "Rename != publish" invariant: after a crash anywhere in the publish window
        // the name resolves to a directory that EXISTS and is coherent -- never a
        // durable pointer to a half-built or absent dir. The identity/contiguity
        // oracle above already ran against whatever the name resolved to; reaching
        // here means the registry resolved, the dir opened, and the rows were clean.
        if (REBASE) {
            if (suspended) {
                // Expected: the rebase hard-suspends, and recovery must leave the
                // REBASED table usable, not stuck in the suspension it was rebased out of.
                System.out.printf("DURABILITY_FAILURE rebased table left suspended after recovery (count=%d)%n", count);
                System.exit(3);
            }
            // ANTI-VACUITY: name the dir in the verdict. The workload rebases once and
            // then idles, so every swept boundary is post-rebase and MUST resolve to a
            // rebased dir -- not the original t~1. Without this, a run where the rebase
            // never fired would still print DURABLE and look like coverage. A verdict
            // that cannot distinguish "the operation ran" from "it never happened" is
            // not evidence, which is the same trap the 3000-row iteration fell into.
            final String dir = resolvedDir;
            if (dir.endsWith("~1")) {
                System.out.printf("LOUD_FAILURE rebase never took effect: name still resolves to %s "
                        + "(expected a rebased dir); this boundary proves nothing%n", dir);
                System.exit(4);
            }
            System.out.printf("DURABLE rebase: name resolved to existing coherent dir=%s, count=%d rows contiguous%n",
                    dir, count);
            System.exit(0);
        }

        // Clean reopen: a suspend that never clears is a failure (a clean readable prefix is required).
        if (suspended) {
            System.out.printf(
                    "DURABILITY_FAILURE table left suspended after recovery (F=%d C=%d Wm=%d)%n",
                    F, committedSeqTxn, localDurableSeqTxn);
            System.exit(3);
        }

        if (W == 0) {
            // Adaptive W=0 == SYNC: the full committed history must survive (zero loss).
            if (F >= committedSeqTxn) {
                System.out.printf("DURABLE count=%d F=%d C=%d (adaptive W=0 == SYNC, zero loss)%n",
                        count, F, committedSeqTxn);
            } else {
                System.out.printf(
                        "DURABILITY_FAILURE F=%d < C=%d — a committed txn was lost under zero-loss adaptive (W=0)%n",
                        F, committedSeqTxn);
                System.exit(3);
            }
        } else {
            // Adaptive W>0 (RPO contract). PRIMARY BAR: every acked txn survives, i.e. the recovered
            // frontier is never below the durable-ack (F >= Wm). Wm == -1 means nothing was acked-durable
            // (e.g. cut before the first group-commit flush) — vacuously safe.
            if (localDurableSeqTxn >= 0 && F < localDurableSeqTxn) {
                System.out.printf(
                        "DURABILITY_FAILURE F=%d < Wm=%d — an ACKED (durable) txn was lost (RPO contract broken)%n",
                        F, localDurableSeqTxn);
                System.exit(3);
            } else {
                // Any un-flushed loss is bounded to (Wm, C] (RPO <= W); C may legally exceed F when the
                // cut landed within the window. Report the observed at-risk loss.
                final long lost = Math.max(0, committedSeqTxn - F);
                System.out.printf(
                        "RPO_OK F=%d >= Wm=%d (every acked txn survived); at-risk txns lost=%d in (Wm=%d, C=%d] (RPO<=W=%d)%n",
                        F, localDurableSeqTxn, lost, localDurableSeqTxn, committedSeqTxn, W);
            }
        }
    }

    /**
     * Bit-check every row 0..count-1 against the deterministic CrashIngestWriter formulas. Returns the
     * consistent row count; prints SILENT_CORRUPTION and exits (2) on the first wrong value / gap.
     */
    /** The profile's extra columns, appended to the oracle's projection (empty when it has none). */
    private static String payloadColumns() {
        switch (CrashIngestWriter.PROFILE) {
            case "varchar":
                return ", vc";
            case "array":
                return ", arr";
            case "wide":
                return ", w0i, w0d, w0s";
            default:
                return "";
        }
    }

    /**
     * Verifies the profile's payload for one row against what CrashIngestWriter.putExtras
     * wrote for that id. Returns null when the row is good, else a description of the
     * mismatch. A recovered row whose PAYLOAD is wrong is corruption even when its id,
     * ordering and contiguity are all correct -- which is all the oracle used to check.
     */
    private static String checkPayload(io.questdb.cairo.sql.Record r, long id) {
        switch (CrashIngestWriter.PROFILE) {
            case "varchar": {
                final CharSequence vc = r.getVarcharA(4) == null ? null : r.getVarcharA(4).toString();
                final String want = CrashIngestWriter.VARCHARS[(int) (id % CrashIngestWriter.VARCHARS.length)].toString();
                return (vc != null && want.contentEquals(vc)) ? null : "varchar mismatch: got=" + vc + " want=" + want;
            }
            case "array": {
                final io.questdb.cairo.arr.ArrayView a = r.getArray(4, io.questdb.cairo.ColumnType.encodeArrayType(io.questdb.cairo.ColumnType.DOUBLE, 1));
                final int wantLen = (int) (id % 10);
                if (a == null) {
                    return wantLen == 0 ? null : "array null, want length " + wantLen;
                }
                if (a.getDimLen(0) != wantLen) {
                    return "array length: got=" + a.getDimLen(0) + " want=" + wantLen;
                }
                for (int j = 0; j < wantLen; j++) {
                    final double got = a.getDouble(j);
                    if (got != (double) (id + j)) {
                        return "array[" + j + "]: got=" + got + " want=" + (double) (id + j);
                    }
                }
                return null;
            }
            case "wide": {
                if (r.getInt(4) != (int) id) {
                    return "wide i0: got=" + r.getInt(4) + " want=" + id;
                }
                if (r.getDouble(5) != id * 1.5) {
                    return "wide d0: got=" + r.getDouble(5) + " want=" + (id * 1.5);
                }
                return null;
            }
            default:
                return null;
        }
    }

    private static long bitCheckRows(CairoEngine engine, SqlExecutionContextImpl ctx, String[] SYMBOLS) throws SqlException {
        return bitCheckRows(engine, ctx, SYMBOLS, CrashIngestWriter.TABLE_NAME);
    }

    /**
     * Same identity + ordering oracle, applied to a named table.
     * <p>
     * With -Dsibling.table=true a SECOND table is written in the same commit
     * cadence with IDENTICAL formulas, so this one oracle validates both and any
     * divergence between them is itself a finding. The sibling exists to exercise
     * RecoveryCoordinator PER-TABLE loop -- both tables are mid-flight at the cut
     * and must be recovered in one recover() pass (the W3 dimension).
     */
    private static long bitCheckRows(CairoEngine engine, SqlExecutionContextImpl ctx, String[] SYMBOLS, String table) throws SqlException {
        // Include the PROFILE'S OWN columns. Selecting only id/v/s/ts meant the array,
        // varchar and wide profiles wrote their columns and the oracle never read them
        // back -- a torn array or varchar aux vector, which is exactly what those
        // dimensions exist to catch, would have passed silently. Every extra column is a
        // deterministic function of id, so each is checkable.
        final String sql = "select id, v, s, ts" + payloadColumns() + " from " + table + " order by ts asc";
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
             RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
            // TWO INDEPENDENT PROPERTIES, checked separately.
            //
            // The old check asserted `row N has id == N`, which conflates them and
            // is only true when timestamp order happens to equal insertion order.
            // Under the o3 profile -- where rows are deliberately written with
            // out-of-order timestamps, exactly as AdaptiveO3CrashSweepTest does --
            // that proxy fails on healthy data: row 2 legitimately holds id 3.
            // It reported SILENT_CORRUPTION at every boundary, which is how it was
            // caught: a real defect does not land on the same early row every time.
            //
            //   IDENTITY  each row's v and s must match the formulas for ITS OWN id,
            //             and the ids present must form the contiguous prefix
            //             {0..count-1} with no gaps and no duplicates.
            //   ORDERING  ts must be non-decreasing across the cursor. This is the
            //             REAL invariant (a table is ordered by its designated
            //             timestamp) rather than a proxy for it, and it holds under
            //             o3 too -- which is the point of the merge path.
            //
            // Strictly stronger than what it replaces: it also catches duplicate ids,
            // which the positional check could not see.
            long rowIndex = 0L;
            long prevTs = Long.MIN_VALUE;
            final java.util.BitSet seen = new java.util.BitSet();
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                final Record rec = cursor.getRecord();
                while (cursor.hasNext()) {
                    final long actualId = rec.getLong(0);
                    final long actualV = rec.getLong(1);
                    final CharSequence actualS = rec.getSymA(2);
                    final long actualTs = rec.getTimestamp(3);

                    final long expectedV = actualId * 2_654_435_761L;
                    final String expectedS = SYMBOLS[(int) (actualId % SYMBOLS.length)];

                    // The profile's OWN columns, checked against what putExtras wrote for
                    // this id. Without this the array/varchar/wide dimensions verified only
                    // that rows came back -- never that their payload survived intact.
                    final String payloadErr = checkPayload(rec, actualId);
                    if (payloadErr != null) {
                        System.out.printf("SILENT_CORRUPTION payload id=%d profile=%s: %s%n",
                                actualId, CrashIngestWriter.PROFILE, payloadErr);
                        System.exit(2);
                    }

                    if (actualId < 0 || actualV != expectedV
                            || !expectedS.equals(String.valueOf(actualS))) {
                        System.out.printf(
                                "SILENT_CORRUPTION row=%d id=%d"
                                        + " expected_v=%d actual_v=%d"
                                        + " expected_s=%s actual_s=%s%n",
                                rowIndex, actualId, expectedV, actualV, expectedS, actualS);
                        System.exit(2);
                    }
                    if (actualId <= Integer.MAX_VALUE) {
                        if (seen.get((int) actualId)) {
                            System.out.printf("SILENT_CORRUPTION duplicate id=%d at row=%d%n",
                                    actualId, rowIndex);
                            System.exit(2);
                        }
                        seen.set((int) actualId);
                    }
                    if (actualTs < prevTs) {
                        System.out.printf(
                                "SILENT_CORRUPTION table not ordered by designated timestamp"
                                        + " at row=%d ts=%d prevTs=%d%n",
                                rowIndex, actualTs, prevTs);
                        System.exit(2);
                    }
                    prevTs = actualTs;
                    rowIndex++;
                }
            }
            // Contiguity: the surviving ids must be {0..rowIndex-1} exactly. A gap
            // means a row vanished from the middle of committed history.
            if (rowIndex <= Integer.MAX_VALUE) {
                final int nextClear = seen.nextClearBit(0);
                if (nextClear < rowIndex) {
                    System.out.printf("SILENT_CORRUPTION id gap: %d missing but count=%d%n",
                            nextClear, rowIndex);
                    System.exit(2);
                }
            }
            return rowIndex;
        }
    }

    /**
     * Run the WAL apply pipeline to materialize every durable WAL txn into the table — the tail of the
     * production recovery path. Mirrors TestUtils.drainWalQueue: apply, then CheckWalTransactionsJob to
     * pick up any un-notified txn, then apply again.
     */
    private static void drainWalQueue(CairoEngine engine) {
        try (ApplyWal2TableJob applyJob = new ApplyWal2TableJob(engine, 0)) {
            applyJob.drain(0);
            new CheckWalTransactionsJob(engine).run();
            applyJob.drain(0);
        }
    }

    private static SqlExecutionContextImpl newRootContext(CairoEngine engine, CairoConfiguration cfg) {
        return new SqlExecutionContextImpl(engine, 1)
                .with(cfg.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null, null, -1, null);
    }

    private static String firstLine(File f) {
        try {
            final List<String> lines = Files.readAllLines(f.toPath(), StandardCharsets.US_ASCII);
            return lines.isEmpty() ? "" : lines.get(0).trim();
        } catch (Exception e) {
            return "";
        }
    }
}
