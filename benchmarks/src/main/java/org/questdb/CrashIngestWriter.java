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
 *
 * PURPOSE: Ingest rows into QuestDB in configurable commit mode, recording an acknowledged-commit
 * watermark after every successful commit(). The process is then hard-killed (kill -9) or power-cut
 * (dm-flakey drop_writes) by the harness script, and CrashVerifier reopens the DB to verify
 * consistency / durability.
 *
 * COMMIT MODE (via -DcommitMode=SYNC|NOSYNC|adaptive, default SYNC):
 *   SYNC:   msync(MS_SYNC) is called on all dirty mmap pages before the _txn commit record
 *           is written.  On a real block device this pushes data to stable storage before
 *           the commit is acknowledged — surviving both process crashes AND power cuts.
 *           Uses the NON-WAL (bypass wal) direct-TableWriter path (the existing, proven harness).
 *   NOSYNC: commits are acknowledged without forcing pages to stable storage.  Data lives
 *           in the OS page cache and survives a process kill (page cache persists), but is
 *           LOST on a power cut (page cache discarded).  Also the NON-WAL path.
 *   adaptive: the WAL path (CommitMode.ADAPTIVE). Rows flow through a WalWriter into the WAL
 *           sequencer; the apply job (ApplyWal2TableJob + CheckWalTransactionsJob) materializes
 *           them and fires durable epochs; the group-commit window (cairo.adaptive.commit.group.window.us)
 *           batches the WAL device flush. After each commit the writer records BOTH the committed
 *           sequencer txn (C) and the durable-ack frontier localDurableSeqTxn (Wm). CrashVerifier
 *           reopens, runs the production recovery triple, and asserts the adaptive durability oracle
 *           (see the SP-D4 protocol spec) against (C, Wm).
 *
 * ADAPTIVE KNOBS:
 *   -Dgroup.window.us=<W>   -> cairo.adaptive.commit.group.window.us (0 = synchronous/zero-loss;
 *                              >0 = batched WAL fdatasync bounded to W microseconds, RPO<=W).
 *   -Depoch.interval.ms=<n> -> cairo.adaptive.epoch.interval.ms (min interval between durable epochs
 *                              per table; default 1000ms, production default; 0 epochs every apply batch).
 *
 * HARNESS #1 — PROCESS-CRASH-CONSISTENCY (crash-consistency-pkill.sh):
 *   kill -9 tests that QuestDB's recovery path leaves a CONSISTENT state after an abrupt mid-write kill.
 *   Page cache is NOT discarded by a kill, so BOTH SYNC and NOSYNC survive process kills.
 *
 * HARNESS #2 — POWER-CUT DURABILITY (power-cut-dmflakey.sh):
 *   dm-flakey with drop_writes discards un-fsync'd writes at the block layer, exactly
 *   modelling a power failure.  SYNC- / adaptive-durable data should survive; NOSYNC data may be lost.
 *
 * SCHEMA: t (id long, v long, s symbol index, ts timestamp) partition by DAY.
 *   NON-WAL (bypass wal) for SYNC/NOSYNC; WAL for adaptive. Same deterministic values either way.
 *
 * DETERMINISTIC VALUES:
 *   row[i].id = i
 *   row[i].v  = i * 2654435761L  (Knuth multiplicative hash — easy to verify)
 *   row[i].s  = SYMBOLS[i % SYMBOLS.length]  (exercises symbol maps + .k/.v index files)
 *   row[i].ts = BASE_TS + i * 1_000_000L (1 second per row → multiple partitions)
 *
 * WATERMARK (_progress, atomic tmp→fsync→rename→dir-fsync so it is never half-written):
 *   NON-WAL: a single bare number = committed row count (unchanged; the pkill harness parses this).
 *   adaptive: first line = committed row count, then "C=<committedSeqTxn>" and "Wm=<localDurableSeqTxn>".
 *             The first line stays the bare row count so `head -1 _progress` works in all modes.
 *
 * Usage: java -cp benchmarks/target/benchmarks.jar \
 *            [-DcommitMode=SYNC|NOSYNC|adaptive] [-Dgroup.window.us=W] [-Depoch.interval.ms=N] \
 *            [-Dmax.rows=N] \
 *            org.questdb.CrashIngestWriter <db-root>
 */
public class CrashIngestWriter {

    static final String TABLE_NAME = "t";
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
        final int commitModeInt = parseCommitMode(commitModeProp);
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
        if (commitModeInt == CommitMode.ADAPTIVE) {
            System.out.println("group.window.us=" + groupWindowUs + " epoch.interval.ms=" + epochIntervalMs);
        }
        System.out.println("max.rows=" + maxRows);

        final CairoConfiguration cfg = new DefaultCairoConfiguration(dbRoot) {
            @Override
            public int getCommitMode() {
                return commitModeInt;
            }

            @Override
            public boolean isAdaptiveEpochColumnSyncBatched() {
                return batchedSync;
            }

            // Adaptive group-commit window (cairo.adaptive.commit.group.window.us). Only ADAPTIVE reads it.
            @Override
            public long getAdaptiveCommitGroupWindowUs() {
                return groupWindowUs;
            }

            // Adaptive durable-epoch cadence (cairo.adaptive.epoch.interval.ms).
            @Override
            public long getAdaptiveEpochIntervalMs() {
                return epochIntervalMs;
            }
        };

        if (commitModeInt == CommitMode.ADAPTIVE) {
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
                final long ts = BASE_TS + id * 1_000_000L;
                final TableWriter.Row row = writer.newRow(ts);
                row.putLong(0, id);                           // col 0: id
                row.putLong(1, id * 2_654_435_761L);          // col 1: v = Knuth hash
                row.putSym(2, SYMBOLS[(int) (id % SYMBOLS.length)]); // col 2: s (indexed)
                // col 3: ts is the designated timestamp, set by newRow(ts) — no putLong needed
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
            final TableToken token = engine.verifyTableName(TABLE_NAME);
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);

            // Hold the WalWriter open across the run (server-like); drive the apply + group-commit
            // flush jobs synchronously after each commit so the durable frontier actually advances.
            try (WalWriter w = engine.getWalWriter(token);
                 ApplyWal2TableJob applyJob = new ApplyWal2TableJob(engine, 0);
                 ExposedFlusher purgeJob = new ExposedFlusher(engine)) {
                final CheckWalTransactionsJob checkJob = new CheckWalTransactionsJob(engine);

                long committedRows = 0L;
                for (long id = 0; id < maxRows; id++) {
                    final long ts = BASE_TS + id * 1_000_000L;
                    final TableWriter.Row row = w.newRow(ts);
                    row.putLong(0, id);                           // col 0: id
                    row.putLong(1, id * 2_654_435_761L);          // col 1: v = Knuth hash
                    row.putSym(2, SYMBOLS[(int) (id % SYMBOLS.length)]); // col 2: s (indexed)
                    // col 3: ts is the designated timestamp, set by newRow(ts)
                    row.append();

                    if ((id + 1) % K == 0) {
                        // WAL commit → one sequencer txn. Under W=0 this fdatasyncs before returning
                        // (Wm advances now); under W>0 the device flush is deferred to the purge sweep.
                        w.commit();

                        // Materialize the committed WAL into the table + fire the durable epoch. Mirrors
                        // TestUtils.drainWalQueue: apply, then CheckWalTransactionsJob to pick up any txn
                        // the commit did not notify, then apply again.
                        applyJob.drain(0);
                        checkJob.runSerially();
                        applyJob.drain(0);

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
                    }
                }
                System.out.println("reached maxRows=" + maxRows + " without kill; exiting normally");
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

    /**
     * Create the table with an indexed symbol column, partitioned by DAY.
     * Indexing s exercises the .k/.v symbol index files on every commit.
     *
     * @param walMode true → WAL table (adaptive path); false → NON-WAL (bypass wal, SYNC/NOSYNC path)
     */
    private static void createTable(CairoConfiguration cfg, boolean walMode) {
        try (CairoEngine engine = new CairoEngine(cfg)) {
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
                CairoEngine.execute(compiler,
                        "create table " + TABLE_NAME
                                + " (id long, v long, s symbol index, ts timestamp)"
                                + " timestamp(ts) partition by DAY " + (walMode ? "wal" : "bypass wal"),
                        ctx, null);
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
