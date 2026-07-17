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
 *
 * PURPOSE: After CrashIngestWriter is hard-killed (kill -9) or power-cut (dm-flakey drop_writes),
 * this tool reopens the same QuestDB database root and verifies consistency / durability against the
 * deterministic row formulas and the acknowledged watermark recorded in _progress.
 *
 * COMMIT MODE (via -DcommitMode=SYNC|NOSYNC|adaptive, default SYNC — matching the writer):
 *   SYNC / NOSYNC — the NON-WAL (bypass wal) path. Reopen, bit-check every row, and assert
 *     count % K == 0 (no torn commit) and count >= watermark (acked rows survived). Verdicts:
 *     CONSISTENT / LOUD_FAILURE / SILENT_CORRUPTION. Unchanged from the original harness.
 *   adaptive — the WAL path. On reopen run the PRODUCTION ADAPTIVE RECOVERY TRIPLE
 *     (RecoveryCoordinator.recover() → notifyWalTxnRepublisher → drainWalQueue) so the durable epoch
 *     rolls forward exactly as a real reboot does, then bit-check and assert the SP-D4 oracle against
 *     the captured (C = committed seqTxn, Wm = localDurableSeqTxn):
 *       - No silent corruption (HARD, every mode) — SILENT_CORRUPTION on any wrong value / torn commit.
 *       - Clean reopen — a suspend that never clears is a DURABILITY_FAILURE.
 *       - W=0 (adaptive == SYNC, zero loss): recovered frontier F >= C ⇒ DURABLE.
 *       - W>0 (RPO contract): every acked txn survives (F >= Wm) ⇒ RPO_OK; else DURABILITY_FAILURE.
 *
 * VERDICTS (printed to stdout; the harness parses the first word):
 *   CONSISTENT count=<n> watermark=<w>       — SYNC/NOSYNC path, all bars hold.
 *   DURABLE ...                              — adaptive W=0, full committed history survived.
 *   RPO_OK ...                               — adaptive W>0, every acked txn survived (RPO<=W).
 *   DURABILITY_FAILURE ...                   — an acked txn was lost, or a suspend never cleared (exit 3).
 *   LOUD_FAILURE: <msg>                      — CairoException on open/query (detected torn state, exit 1).
 *   SILENT_CORRUPTION ...                    — wrong value / gap / torn commit boundary (exit 2, SERIOUS).
 *
 * Usage: java -cp benchmarks/target/benchmarks.jar \
 *            [-DcommitMode=SYNC|NOSYNC|adaptive] [-Dgroup.window.us=W] [-Depoch.interval.ms=N] \
 *            [-Droll.forward.enabled=true|false] \
 *            org.questdb.CrashVerifier <db-root>
 */
public class CrashVerifier {

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
                return modeInt;
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
        try (CairoEngine engine = new CairoEngine(cfg)) {
            // CairoEngine.completeInit() already ran RecoveryCoordinator.recover() at construction; we run
            // the PRODUCTION ADAPTIVE RECOVERY TRIPLE explicitly here (idempotent re-run) to be faithful to
            // the reboot path and match AdaptiveGroupCommitCrashTest: recover → republish → drain.
            final TableToken token = engine.verifyTableName(CrashIngestWriter.TABLE_NAME);
            new RecoveryCoordinator(engine).recover();
            engine.notifyWalTxnRepublisher(token);
            drainWalQueue(engine);

            suspended = engine.getTableSequencerAPI().isSuspended(token);
            lastTxn = engine.getTableSequencerAPI().lastTxn(token);

            final SqlExecutionContextImpl ctx = newRootContext(engine, cfg);
            count = bitCheckRows(engine, ctx, SYMBOLS);
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

        // recovered materialized frontier, in txns (each txn = K rows). Bit-checked rows are the ground
        // truth of what survived AND is correct; lastTxn (the sequencer durable frontier) is cross-reported.
        final long F = count / K;
        System.out.printf("recovered: count=%d F=%d lastTxn=%d C=%d Wm=%d W=%d suspended=%b%n",
                count, F, lastTxn, committedSeqTxn, localDurableSeqTxn, W, suspended);

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
    private static long bitCheckRows(CairoEngine engine, SqlExecutionContextImpl ctx, String[] SYMBOLS) throws SqlException {
        final String sql = "select id, v, s from " + CrashIngestWriter.TABLE_NAME + " order by ts asc";
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
             RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
            long rowIndex = 0L;
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                final Record rec = cursor.getRecord();
                while (cursor.hasNext()) {
                    final long actualId = rec.getLong(0);
                    final long actualV = rec.getLong(1);
                    final CharSequence actualS = rec.getSymA(2);

                    // Expected deterministic values — same formulas as CrashIngestWriter.
                    final long expectedId = rowIndex;
                    final long expectedV = rowIndex * 2_654_435_761L;
                    final String expectedS = SYMBOLS[(int) (rowIndex % SYMBOLS.length)];

                    if (actualId != expectedId || actualV != expectedV
                            || !expectedS.equals(String.valueOf(actualS))) {
                        System.out.printf(
                                "SILENT_CORRUPTION row=%d"
                                        + " expected_id=%d actual_id=%d"
                                        + " expected_v=%d actual_v=%d"
                                        + " expected_s=%s actual_s=%s%n",
                                rowIndex, expectedId, actualId, expectedV, actualV,
                                expectedS, actualS);
                        System.exit(2);
                    }
                    rowIndex++;
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
