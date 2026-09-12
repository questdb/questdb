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

import io.questdb.MessageBusImpl;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.DefaultDdlListener;
import io.questdb.cairo.DefaultLifecycleManager;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * INGEST WRITER FOR CRASH-CONSISTENCY AND POWER-CUT DURABILITY HARNESSES
 * <p>
 * PURPOSE: Ingest rows into QuestDB in configurable commit mode, recording an acknowledged-commit
 * watermark after every successful commit(). The process is then hard-killed (kill -9) or power-cut
 * (dm-flakey drop_writes) by the harness script, and CrashVerifier reopens the DB to verify
 * consistency / durability.
 * <p>
 * COMMIT MODE (via -DcommitMode=SYNC|NOSYNC|adaptive, default SYNC):
 * SYNC:   msync(MS_SYNC) is called on all dirty mmap pages before the _txn commit record
 * is written.  On a real block device this pushes data to stable storage before
 * the commit is acknowledged — surviving both process crashes AND power cuts.
 * Uses the NON-WAL (bypass wal) direct-TableWriter path (the existing, proven harness).
 * NOSYNC: commits are acknowledged without forcing pages to stable storage.  Data lives
 * in the OS page cache and survives a process kill (page cache persists), but is
 * LOST on a power cut (page cache discarded).  Also the NON-WAL path.
 * adaptive: the WAL path (CommitMode.ADAPTIVE). Rows flow through a WalWriter into the WAL
 * sequencer; the apply job (ApplyWal2TableJob + CheckWalTransactionsJob) materializes
 * them and fires durable epochs; the group-commit window (cairo.adaptive.commit.group.window)
 * batches the WAL device flush. After each commit the writer records BOTH the committed
 * sequencer txn (C) and the durable-ack frontier localDurableSeqTxn (Wm). CrashVerifier
 * reopens, runs the production recovery triple, and asserts the adaptive durability oracle
 * (see the SP-D4 protocol spec) against (C, Wm).
 * <p>
 * ADAPTIVE KNOBS:
 * -Dgroup.window.us=<W>   -> cairo.adaptive.commit.group.window (0 = synchronous/zero-loss;
 * >0 = batched WAL fdatasync bounded to W microseconds, RPO<=W).
 * -Depoch.interval.ms=<n> -> cairo.adaptive.epoch.interval (min interval between durable epochs
 * per table; default 1000ms, production default; 0 epochs every apply batch).
 * <p>
 * HARNESS #1 — PROCESS-CRASH-CONSISTENCY (crash-consistency-pkill.sh):
 * kill -9 tests that QuestDB's recovery path leaves a CONSISTENT state after an abrupt mid-write kill.
 * Page cache is NOT discarded by a kill, so BOTH SYNC and NOSYNC survive process kills.
 * <p>
 * HARNESS #2 — POWER-CUT DURABILITY (power-cut-dmflakey.sh):
 * dm-flakey with drop_writes discards un-fsync'd writes at the block layer, exactly
 * modelling a power failure.  SYNC- / adaptive-durable data should survive; NOSYNC data may be lost.
 * <p>
 * SCHEMA: t (id long, v long, s symbol index, ts timestamp) partition by DAY.
 * NON-WAL (bypass wal) for SYNC/NOSYNC; WAL for adaptive. Same deterministic values either way.
 * <p>
 * DETERMINISTIC VALUES:
 * row[i].id = i
 * row[i].v  = i * 2654435761L  (Knuth multiplicative hash — easy to verify)
 * row[i].s  = SYMBOLS[i % SYMBOLS.length]  (exercises symbol maps + .k/.v index files)
 * row[i].ts = BASE_TS + i * 1_000_000L (1 second per row → multiple partitions)
 * <p>
 * WATERMARK (_progress, atomic tmp→fsync→rename→dir-fsync so it is never half-written):
 * NON-WAL: a single bare number = committed row count (unchanged; the pkill harness parses this).
 * adaptive: first line = committed row count, then "C=<committedSeqTxn>" and "Wm=<localDurableSeqTxn>".
 * The first line stays the bare row count so `head -1 _progress` works in all modes.
 * <p>
 * Usage: java -cp benchmarks/target/benchmarks.jar \
 * [-DcommitMode=SYNC|NOSYNC|adaptive] [-Dgroup.window.us=W] [-Depoch.interval.ms=N] \
 * [-Dmax.rows=N] \
 * org.questdb.CrashIngestWriter <db-root>
 */
public class CrashIngestWriter {

    static final String TABLE_NAME = "t";

    /**
     * -Dsibling.table=true adds a SECOND adaptive WAL table, written in the same
     * loop and committed in the same cadence as the primary.
     * <p>
     * The point is RecoveryCoordinator's PER-TABLE loop: with one table the loop
     * body runs once and its cross-table behaviour is never exercised. Both
     * tables are mid-flight when the cut lands, so both must be recovered in a
     * single recover() pass -- the dimension AdaptiveMultiTableLazyGapCrashSweepTest
     * (W3) covers and no single-table crash test can reach.
     */
    /**
     * -Dflip.at.rows=N fires {@code ALTER TABLE t SET PARAM commit_mode='nosync'}
     * once N rows have been committed, mid-ingest, with the WalWriter still open.
     * <p>
     * The point is what recovery DECIDES FROM. Rows before the flip were written
     * under adaptive and may be lazily ahead of their epoch; rows after it were
     * not. At restart the table's effective mode says only how it will be written
     * NEXT -- it says nothing about how the existing state was LEFT. Recovery must
     * therefore roll forward from the DURABLE ENROLMENT RECORD, not from the
     * current mode, or it serves the torn pre-flip state.
     * <p>
     * Mirrors AdaptiveCommitModeFlipCrashTest#testCrashedAdaptiveTableRollsForwardAfterGlobalFlipToNosync.
     * <p>
     * The oracle needs no change: Wm freezes at the flip (nosync never advances the
     * durable-ack frontier), so F >= Wm still expresses exactly the right bar --
     * everything acked while adaptive was in force must survive.
     */

    /**
     * -Dddl.every.rows=N issues a structural change (ADD COLUMN) every N committed
     * rows, mid-ingest, through the WalWriter's own API.
     * <p>
     * Structural changes are the one thing adaptive CANNOT apply lazily: a column
     * add is a one-shot metadata write with no epoch behind it, so it takes the
     * SYNC grade and its ordering against the data is load-bearing. A crash
     * between the column file appearing and the segment metadata naming it leaves
     * a segment that can never be applied, and the table is suspended with
     * "WAL segment column too short for committed row range [... actual=-1]".
     * That is the dimension RandomizedAdaptiveCrashFuzzTest covers via its
     * change-column-type / rename-column cases.
     * <p>
     * NEW COLUMNS ARE APPENDED, never inserted, so columns 0..3 keep their
     * positions and the identity oracle is untouched. Rows after the add simply
     * leave the new column null.
     * <p>
     * Uses WalWriter.addColumn rather than `alter table ... add column` SQL on
     * purpose: a sequenced ALTER published while this process holds the WalWriter
     * open deadlocks against itself -- the apply job waits on metadata the writer
     * holds, times out at 5s and SUSPENDS the table. The writer's own API is the
     * path the engine uses internally and takes no such lock.
     */
    static final long DDL_EVERY_ROWS = Long.getLong("ddl.every.rows", -1L);

    /**
     * -Dmat.view=true creates a materialized view over the ingest table.
     * <p>
     * The view and its base are refreshed by a DIFFERENT mechanism than the base
     * is written by, so a crash can leave them at different points. The bar is
     * NOT that they agree -- the view legitimately LAGS, since refresh is async --
     * it is that the view never shows MORE than the base supports. A view row
     * whose count exceeds what the recovered base actually contains is a phantom:
     * the view recorded work the base no longer has.
     * <p>
     * Mirrors AdaptiveMatViewLazyGapCrashSweepTest (W4), the one recovery path the
     * adaptive design docs left explicitly open.
     */
    /**
     * ALTER TABLE ... REBASE WAL, fired ONCE after this many rows, then the writer goes
     * IDLE rather than resuming ingestion. Idling is deliberate: the flush sweep crashes at
     * the LAST boundaries of the recording, so making the rebase the final recorded activity
     * puts those boundaries inside the rename/publish window -- which is the whole point of
     * the "Rename != publish" invariant. Continuing to ingest would bury the publish under
     * thousands of later flushes and the sweep would never land on it.
     */
    static final long REBASE_AT_ROWS = Long.getLong("rebase.at.rows", -1L);
    static final boolean MAT_VIEW = Boolean.getBoolean("mat.view");
    static final String MV_NAME = "mv";
    /** {view name, REFRESH clause}. MV_NAME stays first so existing single-view checks keep working. */
    static final String[][] MV_VARIANTS = {
            {MV_NAME, "immediate"},
            {"mv_timer", "every 1m"},
            {"mv_manual", "manual"},
            {"mv_period", "immediate period (length 1h)"},
            {"mv_deferred", "manual deferred"},
            // The deferred flag is ORTHOGONAL to the refresh type, so the combination needs its
            // own entry -- testing each separately does not cover them together.
            {"mv_period_deferred", "manual deferred period (length 1h)"},
    };

    static final boolean SIBLING_TABLE = Boolean.getBoolean("sibling.table");
    static final String SIBLING_NAME = "t2";
    // Small fixed symbol set to exercise symbol dictionary + .k/.v index file writes on each commit
    static final String[] SYMBOLS = {"alpha", "beta", "gamma", "delta"};
    // Commit every K rows
    static final int K = 1_000;
    // Microsecond timestamps: start at 2024-01-01 00:00:00 UTC, 1 second apart
    // This advances the day partition boundary every 86_400 rows (exercises partition commits)
    static final long BASE_TS = 1_704_067_200_000_000L;
    // Max rows (50 million); process is typically killed long before this. Overridable via
    // -Dmax.rows so the dry-run smoke can bound the run and exit cleanly (no kill / no root).
    static final long MAX_ROWS = 50_000_000L;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: CrashIngestWriter [-DcommitMode=SYNC|NOSYNC|adaptive]"
                    + " [-Dgroup.window.us=W] [-Depoch.interval.ms=N] [-Dmax.rows=N] <db-root>");
            System.exit(1);
        }
        final String dbRoot = args[0];
        new File(dbRoot).mkdirs();

        // Commit mode is configured via -DcommitMode=SYNC|NOSYNC|adaptive (default: SYNC).
        final String commitModeProp = System.getProperty("commitMode", "SYNC");
        int commitModeInt = parseCommitMode(commitModeProp);
        // Per-table commit mode was REMOVED from the product (see "Remove per-table commit
        // mode"), taking the `commit_mode` table param and resolveEffectiveCommitMode with it.
        // The instance mode is now the only mode, so requestedMode == commitModeInt always.
        final int requestedMode = commitModeInt;
        final int commitModeFinal = commitModeInt;
        System.out.println("commitMode=" + commitModeProp + " (" + commitModeInt + ")");

        // -Dbatched=false forces the per-file msync(MS_SYNC) path (the proven baseline);
        // default true uses the batched flush optimization (sync_file_range + _cv device flush).
        final boolean batchedSync = Boolean.parseBoolean(System.getProperty("batched", "true"));
        System.out.println("batchedColumnSync=" + batchedSync);

        // ADAPTIVE knobs. group.window.us = the RPO window (0 = synchronous zero-loss WAL fdatasync;
        // >0 = batched device flush bounded to W). epoch.interval.ms = min per-table durable-epoch
        // cadence (default 1000ms = production default; 0 = epoch every apply batch).
        final long groupWindowUs = Long.getLong("group.window.us", 0L);
        final long epochIntervalMs = Long.getLong("epoch.interval.ms", 1000L);
        // -Dmax.rows caps the run so the smoke can exit cleanly without a kill.
        final long maxRows = Long.getLong("max.rows", MAX_ROWS);
        if (requestedMode == CommitMode.ADAPTIVE) {
            System.out.println("group.window.us=" + groupWindowUs + " epoch.interval.ms=" + epochIntervalMs);
        }
        System.out.println("max.rows=" + maxRows);

        final CairoConfiguration cfg = new DefaultCairoConfiguration(dbRoot) {
            @Override
            public int getCommitMode() {
                return commitModeFinal;
            }

            // REBASE WAL refuses unless suspension actually blocks writes.
            @Override
            public boolean isWalApplySuspendedWriteDenied() {
                return REBASE_AT_ROWS > 0 || super.isWalApplySuspendedWriteDenied();
            }

            @Override
            public boolean isAdaptiveEpochColumnSyncBatched() {
                return batchedSync;
            }

            // Adaptive group-commit window (cairo.adaptive.commit.group.window). Only ADAPTIVE reads it.
            @Override
            public long getAdaptiveCommitGroupWindowUs() {
                return groupWindowUs;
            }

            // Adaptive durable-epoch cadence (cairo.adaptive.epoch.interval).
            @Override
            public long getAdaptiveEpochIntervalMs() {
                return epochIntervalMs;
            }
        };

        // requestedMode, NOT commitModeInt: under per.table.mode the INSTANCE is
        // nosync but the workload must still run the WAL/adaptive path, because
        // the whole point is that the TABLE's override carries durability on a
        // nosync instance. Dispatching on the lowered instance mode created a
        // bypass-WAL table with no sequencer at all.
        if (requestedMode == CommitMode.ADAPTIVE) {
            runAdaptiveWal(cfg, dbRoot, maxRows);
        } else {
            runBypassWal(cfg, dbRoot, maxRows);
        }
    }

    /**
     * SYNC / NOSYNC path — the ORIGINAL, proven harness: a NON-WAL (bypass wal) table driven by a
     * direct TableWriter, committing every K rows and recording a bare row-count watermark. Unchanged
     * behavior (the regression guard on the existing path); the pkill harness parses this bare number.
     */
    private static void runBypassWal(CairoConfiguration cfg, String dbRoot, long maxRows) throws Exception {
        // Step 1: create the NON-WAL table via DDL engine (same pattern as SyncCostProfiler)
        createTable(cfg, false);

        // Step 2: open a direct TableWriter (bypasses WAL overhead, exercises the commit
        // path whose consistency we are testing)
        final TableToken token = new TableToken(TABLE_NAME, TABLE_NAME, null, 0, false, false, false);
        try (CairoEngine writerEngine = new CairoEngine(cfg);
             TableWriter writer = new TableWriter(
                     cfg,
                     token,
                     null,
                     new MessageBusImpl(cfg),
                     true,
                     DefaultLifecycleManager.INSTANCE,
                     cfg.getDbRoot(),
                     DefaultDdlListener.INSTANCE,
                     writerEngine
             )) {

            long committedRows = 0L;
            final Path progressPath = Path.of(dbRoot, "_progress");
            final Path progressTmp = Path.of(dbRoot, "_progress.tmp");

            // Step 3: ingest rows; never exit cleanly (unless -Dmax.rows reached) — wait for kill -9
            for (long id = 0; id < maxRows; id++) {
                // ts increases monotonically by 1 second per row, crossing day boundaries
                final long ts = tsFor(id);
                final TableWriter.Row row = writer.newRow(ts);
                row.putLong(0, id);                           // col 0: id
                row.putLong(1, id * 2_654_435_761L);          // col 1: v = Knuth hash
                row.putSym(2, SYMBOLS[(int) (id % SYMBOLS.length)]); // col 2: s
                // col 3: ts is the designated timestamp, set by newRow(ts)
                putExtras(row, id);
                row.append();

                if ((id + 1) % K == 0) {
                    // commit(): in SYNC mode msync(MS_SYNC) all dirty pages before _txn write;
                    // in NOSYNC mode commits without forcing pages to storage.
                    writer.commit();
                    committedRows = id + 1;

                    // Durably record the acknowledged watermark (bare row count — the original format).
                    writeProgressDurably(dbRoot, progressPath, progressTmp,
                            Long.toString(committedRows).getBytes(StandardCharsets.US_ASCII));

                    System.out.println("committed " + committedRows);
                    System.out.flush();
                }
            }
            System.out.println("reached maxRows=" + maxRows + " without kill; exiting normally");
        }
    }

    /**
     * ADAPTIVE / WAL path (the SP-D4 extension). Rows flow through a WalWriter into the WAL sequencer;
     * after each commit the apply job materializes them and fires durable epochs, and the group-commit
     * flush (WalPurgeJob, age-gated by W) advances the durable-ack frontier. This mirrors a running
     * server: WAL commits + a background apply worker + the group-commit flusher coexist.
     *
     * <p>After each commit we capture BOTH:
     * <ul>
     *   <li>C  = tracker.getSeqTxn()            — the committed sequencer txn (what was acked as committed)</li>
     *   <li>Wm = tracker.getLocalDurableSeqTxn() — the durable-ack frontier (the WAL fdatasync high-water)</li>
     * </ul>
     * Under W=0, Wm advances synchronously with C on each commit (adaptive == SYNC, zero loss). Under
     * W>0, Wm lags C by up to ~W (the at-risk window); WalPurgeJob.runSerially() self-limits to the W
     * cadence via its age gate, so calling it every commit reproduces the server's bounded flush.
     */
    private static void runAdaptiveWal(CairoConfiguration cfg, String dbRoot, long maxRows) throws Exception {
        // Create the WAL table (its own short-lived engine, mirroring the bypass-wal flow).
        createTable(cfg, true);

        final Path progressPath = Path.of(dbRoot, "_progress");
        final Path progressTmp = Path.of(dbRoot, "_progress.tmp");

        try (CairoEngine engine = new CairoEngine(cfg)) {
            // load() swaps the default NO-OP mat view / live view stores for the
            // real ones. Without it matViewStateStore stays NoOpMatViewStateStore,
            // every notifyMatViewBaseTableCommit is silently discarded, and a
            // materialized view is created but NEVER refreshed -- which is exactly
            // how the base/view oracle ended up reading an empty view.
            engine.load();
            // hydrate too: load() creates the real store but leaves it EMPTY. The
            // view was registered by createTable's own short-lived engine, so this
            // engine must rebuild that registry from the on-disk _mv state or
            // enqueueIncrementalRefresh targets a view it does not know about and
            // silently does nothing -- which is why the view stayed empty.
            engine.hydrateMatViewStateStore();
            final TableToken token = engine.verifyTableName(TABLE_NAME);
            boolean rebaseRequested = false;
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);

            // Hold the WalWriter open across the run (server-like); drive the apply + group-commit
            // flush jobs synchronously after each commit so the durable frontier actually advances.
            final TableToken token2 = SIBLING_TABLE ? engine.verifyTableName(SIBLING_NAME) : null;
            // The mat view is refreshed by its OWN job, not by ApplyWal2TableJob.
            // Without driving it the view stays EMPTY, sum(cnt) returns NULL, and
            // the base/view oracle compares Long.MIN_VALUE against the row count
            // and passes unconditionally -- a check that fires but can never fail.
            final io.questdb.cairo.mv.MatViewRefreshJob mvJob =
                    MAT_VIEW ? new io.questdb.cairo.mv.MatViewRefreshJob(0, engine, 1) : null;
            final TableToken mvToken = MAT_VIEW ? engine.verifyTableName(MV_NAME) : null;
            final TableToken[] mvTokens;
            if (MAT_VIEW) {
                mvTokens = new TableToken[MV_VARIANTS.length];
                for (int mvi = 0; mvi < MV_VARIANTS.length; mvi++) {
                    mvTokens[mvi] = engine.verifyTableName(MV_VARIANTS[mvi][0]);
                }
            } else {
                mvTokens = new TableToken[0];
            }
            try (WalWriter w = engine.getWalWriter(token);
                 WalWriter w2 = SIBLING_TABLE ? engine.getWalWriter(token2) : null;
                 ApplyWal2TableJob applyJob = new ApplyWal2TableJob(engine, 0);
                 ExposedFlusher purgeJob = new ExposedFlusher(engine)) {
                final CheckWalTransactionsJob checkJob = new CheckWalTransactionsJob(engine);

                long committedRows = 0L;
                int ddlSeq = 0;
                for (long id = 0; id < maxRows; id++) {
                    final long ts = tsFor(id);
                    final TableWriter.Row row = w.newRow(ts);
                    row.putLong(0, id);                           // col 0: id
                    row.putLong(1, id * 2_654_435_761L);          // col 1: v = Knuth hash
                    row.putSym(2, SYMBOLS[(int) (id % SYMBOLS.length)]); // col 2: s
                    // col 3: ts is the designated timestamp, set by newRow(ts)
                    putExtras(row, id);
                    row.append();

                    // Same row into the sibling: identical formulas, so ONE oracle
                    // validates both and any divergence between them is a finding.
                    if (w2 != null) {
                        final TableWriter.Row r2 = w2.newRow(ts);
                        r2.putLong(0, id);
                        r2.putLong(1, id * 2_654_435_761L);
                        r2.putSym(2, SYMBOLS[(int) (id % SYMBOLS.length)]);
                        putExtras(r2, id);
                        r2.append();
                    }

                    if ((id + 1) % K == 0) {
                        // WAL commit → one sequencer txn. Under W=0 this fdatasyncs before returning
                        // (Wm advances now); under W>0 the device flush is deferred to the purge sweep.
                        w.commit();
                        if (w2 != null) {
                            w2.commit();
                        }

                        // Structural DDL under load: append a column, never insert.
                        if (DDL_EVERY_ROWS > 0 && (id + 1) % DDL_EVERY_ROWS == 0) {
                            final String col = "dyn_" + ddlSeq++;
                            // Root security context, NOT null: AlterOperation.authorize
                            // rejects an empty context with "alter security context is
                            // empty". Same context createTable uses for its DDL.
                            w.addColumn(col, io.questdb.cairo.ColumnType.INT,
                                    cfg.getFactoryProvider().getSecurityContextFactory().getRootContext());
                            System.out.println("ddl.every.rows: added column " + col
                                    + " after " + (id + 1) + " rows");
                        }

                        // Mid-run commit-mode flip, fired exactly once.

                        // Materialize the committed WAL into the table + fire the durable epoch. Mirrors
                        // TestUtils.drainWalQueue: apply, then CheckWalTransactionsJob to pick up any txn
                        // the commit did not notify, then apply again.
                        applyJob.drain(0);
                        checkJob.runSerially();
                        applyJob.drain(0);
                        if (mvJob != null) {
                            // The refresh job DRAINS a queue; it does not scan for
                            // stale views. Nothing enqueues work here because the
                            // enqueue normally comes from the SQL/commit path, so
                            // polling run() alone left the view permanently EMPTY.
                            // Refresh EVERY variant, not just the immediate one. A view can only be
                            // left AHEAD by a crash if it refreshed from base txns that were then
                            // lost, so a MANUAL/DEFERRED/TIMER view that never refreshes can never
                            // exercise the repair path -- it would look "covered" while testing
                            // nothing. Driving them here stands in for the explicit
                            // REFRESH MATERIALIZED VIEW a user issues against a manual view.
                            for (int mvi = 0; mvi < mvTokens.length; mvi++) {
                                engine.getMatViewStateStore().enqueueIncrementalRefresh(mvTokens[mvi]);
                            }
                            // TO EXHAUSTION, then apply again. This is the pattern
                            // AdaptiveMatViewLazyGapCrashSweepTest uses:
                            //   drainWalQueue -> while(refreshJob.run()) -> drainWalQueue
                            // A single run() leaves the view EMPTY, which makes the
                            // base/view oracle vacuous (sum over no rows is NULL).
                            //noinspection StatementWithEmptyBody
                            while (mvJob.run()) ;
                            applyJob.drain(0);
                        }

                        // Group-commit device flush: advances localDurableSeqTxn for commits older than W
                        // (a no-op set under W=0, where commit already fdatasync'd). Age-gated, so calling
                        // it every commit self-limits to the W cadence.
                        purgeJob.flushNow();

                        committedRows = id + 1;
                        final long committedSeqTxn = tracker.getSeqTxn();       // C
                        final long localDurableSeqTxn = tracker.getLocalDurableSeqTxn(); // Wm

                        // _progress: first line = bare committed row count (so `head -1` works in all
                        // modes), then the adaptive frontiers C and Wm.
                        final String content = committedRows
                                + "\nC=" + committedSeqTxn
                                + "\nWm=" + localDurableSeqTxn + "\n";
                        writeProgressDurably(dbRoot, progressPath, progressTmp,
                                content.getBytes(StandardCharsets.US_ASCII));

                        System.out.println("committed rows=" + committedRows
                                + " C=" + committedSeqTxn + " Wm=" + localDurableSeqTxn);
                        System.out.flush();

                        if (REBASE_AT_ROWS > 0 && (id + 1) >= REBASE_AT_ROWS) {
                            rebaseRequested = true;
                            break;
                        }
                    }
                }
                if (!rebaseRequested) {
                    System.out.println("reached maxRows=" + maxRows + " without kill; exiting normally");
                }
            }

            // REBASE WAL runs with the WalWriters CLOSED (the try-with-resources above has
            // exited): it hard-suspends the table and rebuilds it into a fresh directory,
            // which a live writer on the old token would block. See CairoEngine.rebaseWalTable0
            // -- it refuses unless isHardSuspended() AND writes are denied under suspension.
            if (rebaseRequested) {
                engine.getTableSequencerAPI().setHardSuspended(token, true);
                System.out.println("rebase.at.rows: hard-suspended " + TABLE_NAME);
                try (SqlCompilerImpl rbCompiler = new SqlCompilerImpl(engine)) {
                    final SqlExecutionContextImpl rbCtx = new SqlExecutionContextImpl(engine, 1)
                            .with(cfg.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                                    null, null, -1, null);
                    CairoEngine.execute(rbCompiler,
                            "alter table " + TABLE_NAME + " rebase wal", rbCtx, null);
                }
                final TableToken rebased = engine.verifyTableName(TABLE_NAME);
                System.out.println("rebase.at.rows: REBASED " + TABLE_NAME
                        + " dir " + token.getDirName() + " -> " + rebased.getDirName());
                System.out.flush();
                // Go IDLE, alive. The sweep crashes at the LAST recorded boundaries, so
                // stopping here is what puts them in the publish window. Staying alive is
                // also what the sweep's liveness assertion checks -- exiting here would be
                // read as "the workload was not running", which is how a vacuous iteration
                // was caught before.
                for (;;) {
                    Thread.sleep(1000);
                }
            }
        }
    }

    /**
     * Durably record the _progress watermark so it survives a power cut on ALL filesystems:
     * write-tmp → fsync tmp CONTENT → atomic rename → fsync the DIRECTORY. Without the fsyncs the
     * watermark is lost on XFS after a cut (XFS does not auto-flush a rename-over-existing like ext4's
     * auto_da_alloc heuristic), leaving the verifier with an empty _progress. The harness must hold its
     * own bookkeeping to the same durability bar as the QuestDB data it is verifying.
     */
    private static void writeProgressDurably(String dbRoot, Path progressPath, Path progressTmp, byte[] content)
            throws IOException {
        try (FileChannel ch = FileChannel.open(progressTmp,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ch.write(ByteBuffer.wrap(content));
            ch.force(true); // fsync tmp content+size BEFORE the rename
        }
        // rename(2) is atomic on POSIX — verifier sees either old or new value, never torn.
        Files.move(progressTmp, progressPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        // fsync the directory so the rename (the new dirent) is itself durable. Best-effort:
        // opening a directory channel is unsupported on some platforms (e.g. Windows) → ignore.
        try (FileChannel dir = FileChannel.open(Path.of(dbRoot), StandardOpenOption.READ)) {
            dir.force(true);
        } catch (IOException ignore) {
            // directory fsync not supported here; the content fsync above is the essential part
        }
    }

    /**
     * Parse the -DcommitMode property value into a CommitMode int constant.
     * SYNC / NOSYNC use the NON-WAL direct-TableWriter path; adaptive uses the WAL path.
     */
    static int parseCommitMode(String name) {
        return switch (name.toUpperCase()) {
            case "SYNC" -> CommitMode.SYNC;
            case "NOSYNC" -> CommitMode.NOSYNC;
            case "ADAPTIVE" -> CommitMode.ADAPTIVE;
            default -> throw new IllegalArgumentException(
                    "Unknown commitMode '" + name + "'; expected SYNC, NOSYNC or adaptive");
        };
    }


    // ---------------------------------------------------------------------
    // WORKLOAD PROFILES (-Dschema.profile)
    //
    // Columns 0..3 (id, v, s, ts) are FIXED in every profile so the identity
    // oracle in CrashVerifier is untouched. Profiles vary (a) which index the
    // s column carries, (b) extra columns appended at index 4+, and (c) the
    // timestamp pattern. Each closes a dimension the Java crash suite covers:
    //
    //   bitmap   .k/.v bitmap index        AdaptiveIndexedSymbolLazyGap, MapLengthGuard
    //   posting  posting index chain       PostingIndex* suite
    //   covering posting + include(v)      covering suite
    //   none     no index                  plain (ts, v long) tests
    //   varchar  + vc varchar              AdaptiveEpochCrashTest, VarcharPowerLoss*
    //   array    + arr double[]            ArrayCrashConsistencyTest
    //   wide     + 12 mixed-type columns   BatchedFlushDurabilityCrashTest
    //   o3       out-of-order timestamps   AdaptiveO3CrashSweep, AdaptiveO3LazyGap
    // ---------------------------------------------------------------------
    static final String PROFILE = System.getProperty("schema.profile", "bitmap");

    /** Extra column DDL appended after the fixed 0..3 columns. */
    private static String extraColumnsDdl() {
        switch (PROFILE) {
            case "varchar": return ", vc varchar";
            case "array":   return ", arr double[]";
            case "wide": {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 4; i++) {
                    sb.append(", w").append(i).append("i int");
                    sb.append(", w").append(i).append("d double");
                    sb.append(", w").append(i).append("s varchar");
                }
                return sb.toString();
            }
            default: return "";
        }
    }

    /** Index clause on the s column. */
    private static String indexClause() {
        switch (PROFILE) {
            case "bitmap":   return " index";
            case "posting":  return " index type posting";
            case "covering": return " index type posting include (v)";
            case "none": case "varchar": case "array": case "wide": case "o3": return "";
            default:
                throw new IllegalArgumentException("unknown schema.profile: " + PROFILE);
        }
    }

    /**
     * Designated timestamp for a row.
     * <p>
     * The o3 profile decouples commit order from timestamp order: every 4th row
     * lands BELOW the running maximum, so each commit from then on has
     * minTimestamp &lt; the table's max and engages the O3 merge path rather than
     * a pure tail append. Mirrors AdaptiveO3CrashSweepTest's zig-zag.
     */
    private static long tsFor(long id) {
        if ("o3".equals(PROFILE) && (id % 4) == 3) {
            return BASE_TS + (id - 2) * 1_000_000L + 250_000L;
        }
        return BASE_TS + id * 1_000_000L;
    }

    /** Write the profile's extra columns, starting at index 4. */
    private static void putExtras(TableWriter.Row row, long id) {
        switch (PROFILE) {
            case "varchar":
                // Length varies per row so the aux vector holds genuinely
                // variable offsets -- the torn-aux-tail shape.
                row.putVarchar(4, VARCHARS[(int) (id % VARCHARS.length)]);
                break;
            case "array": {
                // Length VARIES per row (0..9), so the array aux vector carries
                // genuinely variable offsets. A fixed length would make every
                // entry the same width and never exercise the torn-tail shape
                // ArrayCrashConsistencyTest is about.
                final io.questdb.cairo.arr.DirectArray a = ARRAY.get();
                a.setType(io.questdb.cairo.ColumnType.encodeArrayType(io.questdb.cairo.ColumnType.DOUBLE, 1));
                final int len = (int) (id % 10);
                a.setDimLen(0, len);
                a.applyShape();
                for (int j = 0; j < len; j++) {
                    a.putDouble(j, id + j);
                }
                row.putArray(4, a);
                break;
            }
            case "wide":
                for (int i = 0; i < 4; i++) {
                    row.putInt(4 + i * 3, (int) (id + i));
                    row.putDouble(5 + i * 3, id * 1.5 + i);
                    row.putVarchar(6 + i * 3, VARCHARS[(int) ((id + i) % VARCHARS.length)]);
                }
                break;
            default:
                break;
        }
    }

    // Utf8String, not String: Row.putVarchar takes a Utf8Sequence. Lengths
    // vary per row so the aux vector carries genuinely variable offsets --
    // the torn-aux-tail shape VarcharPowerLossCorruptionTest is about.
    // One DirectArray per thread, reused across rows: allocating one per row
    // would dominate the workload and starve the commit cadence the crash
    // timing depends on.
    private static final ThreadLocal<io.questdb.cairo.arr.DirectArray> ARRAY =
            ThreadLocal.withInitial(io.questdb.cairo.arr.DirectArray::new);

    private static final io.questdb.std.str.Utf8String[] VARCHARS = {
            new io.questdb.std.str.Utf8String("a"),
            new io.questdb.std.str.Utf8String("bb"),
            new io.questdb.std.str.Utf8String("ccc"),
            new io.questdb.std.str.Utf8String("dddddddd"),
            new io.questdb.std.str.Utf8String("eeeeeeeeeeeeeeee"),
            new io.questdb.std.str.Utf8String("ffffffffffffffffffffffffffffffff"),
            new io.questdb.std.str.Utf8String("g"),
            new io.questdb.std.str.Utf8String("hh")
    };

    /**
     * Create the table with an indexed symbol column, partitioned by DAY.
     * Indexing s exercises the .k/.v symbol index files on every commit.
     *
     * @param walMode true → WAL table (adaptive path); false → NON-WAL (bypass wal, SYNC/NOSYNC path)
     */
    private static void createTable(CairoConfiguration cfg, boolean walMode) {
        try (CairoEngine engine = new CairoEngine(cfg)) {
            // load() swaps the default NO-OP mat view / live view stores for the
            // real ones. Without it matViewStateStore stays NoOpMatViewStateStore,
            // every notifyMatViewBaseTableCommit is silently discarded, and a
            // materialized view is created but NEVER refreshed -- which is exactly
            // how the base/view oracle ended up reading an empty view.
            engine.load();
            // hydrate too: load() creates the real store but leaves it EMPTY. The
            // view was registered by createTable's own short-lived engine, so this
            // engine must rebuild that registry from the on-disk _mv state or
            // enqueueIncrementalRefresh targets a view it does not know about and
            // silently does nothing -- which is why the view stayed empty.
            engine.hydrateMatViewStateStore();
            final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(
                            cfg.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null, null, -1, null
                    );
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                // id, v: long columns (fixed-width, exercises data vector files)
                // s: symbol with index (exercises symbol char file, offset file, .k/.v index)
                // ts: designated timestamp (triggers _txn / _cv / partition metadata on commit)
                // partition by DAY: multiple partitions (exercises cross-partition commit paths)
                // -Dschema.profile selects WHICH INDEX the s column carries. Every
                // profile keeps the same COLUMN POSITIONS (0=id, 1=v, 2=s, 3=ts) so
                // the row-writing path and the identity oracle are untouched -- only
                // the index implementation under test changes.
                //
                //   bitmap   .k/.v bitmap index      (the default; BitmapIndexWriter)
                //   posting  posting index chain     (PostingIndexWriter)
                //   covering posting + include(v)    (covering index read path)
                //   none     no index                (baseline: isolates index faults)
                final String withClause = "";
                final String ddl = "create table " + TABLE_NAME
                        + " (id long, v long, s symbol" + indexClause() + ", ts timestamp"
                        + extraColumnsDdl() + ")"
                        + " timestamp(ts) partition by DAY " + (walMode ? "wal" : "bypass wal")
                        + withClause;
                System.out.println("schema.profile=" + PROFILE + " ddl=" + ddl);
                CairoEngine.execute(compiler, ddl, ctx, null);
                if (MAT_VIEW && walMode) {
                    // EVERY refresh type over the SAME base, so one crash exercises all of them.
                    // The surgical repair runs from the load path and is type-agnostic by design;
                    // that claim is only worth anything if each type is actually crashed and checked.
                    // IMMEDIATE  -- refreshed by the base-commit notification (the original case)
                    // TIMER      -- driven by MatViewTimerJob on an interval
                    // MANUAL     -- only ever refreshed on explicit request
                    // PERIOD     -- refreshed a whole period at a time (own hi-watermark)
                    // DEFERRED   -- orthogonal flag; no initial refresh until asked
                    for (String[] mv : MV_VARIANTS) {
                        CairoEngine.execute(compiler,
                                "create materialized view " + mv[0] + " refresh " + mv[1] + " as ("
                                        + "select ts, count() cnt from " + TABLE_NAME + " sample by 1h"
                                        + ") partition by DAY",
                                ctx, null);
                        System.out.println("mat.view=true: created " + mv[0] + " (refresh " + mv[1] + ") over " + TABLE_NAME);
                    }
                }
                if (SIBLING_TABLE && walMode) {
                    CairoEngine.execute(compiler,
                            ddl.replaceFirst("create table " + TABLE_NAME, "create table " + SIBLING_NAME),
                            ctx, null);
                    System.out.println("sibling.table=true: also created " + SIBLING_NAME);
                }
            } catch (SqlException e) {
                throw new RuntimeException("DDL failed", e);
            }
        }
    }

    /**
     * Exposes {@link WalPurgeJob#runSerially()} so the harness can drive the adaptive group-commit
     * device flush deterministically in-process (same trick as AdaptiveGroupCommitCrashTest.ExposedFlusher).
     */
    static final class ExposedFlusher extends WalPurgeJob {
        ExposedFlusher(CairoEngine engine) {
            super(engine);
        }

        boolean flushNow() {
            return runSerially();
        }
    }
}
