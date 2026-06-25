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
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
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
 * PURPOSE: Ingest rows into QuestDB in configurable commit mode (SYNC or NOSYNC),
 * recording an acknowledged-commit watermark after every successful commit(). The process
 * is then hard-killed (kill -9) or power-cut (dm-flakey drop_writes) by the harness
 * script, and CrashVerifier reopens the DB to verify consistency / durability.
 *
 * COMMIT MODE (via -DcommitMode=SYNC|NOSYNC, default SYNC):
 *   SYNC:   msync(MS_SYNC) is called on all dirty mmap pages before the _txn commit record
 *           is written.  On a real block device this pushes data to stable storage before
 *           the commit is acknowledged — surviving both process crashes AND power cuts.
 *   NOSYNC: commits are acknowledged without forcing pages to stable storage.  Data lives
 *           in the OS page cache and survives a process kill (page cache persists), but is
 *           LOST on a power cut (page cache discarded).
 *
 * HARNESS #1 — PROCESS-CRASH-CONSISTENCY (crash-consistency-pkill.sh):
 *   kill -9 tests that QuestDB's recovery path (torn-aux guards, _txn/_cv A/B metadata,
 *   _todo recovery file) leaves a CONSISTENT state after an abrupt mid-write kill.
 *   Page cache is NOT discarded by a kill, so BOTH SYNC and NOSYNC survive process kills.
 *
 * HARNESS #2 — POWER-CUT DURABILITY (power-cut-dmflakey.sh):
 *   dm-flakey with drop_writes discards un-fsync'd writes at the block layer, exactly
 *   modelling a power failure.  SYNC-committed data should survive; NOSYNC data may be lost.
 *
 * SCHEMA: t (id long, v long, s symbol index, ts timestamp) partition by DAY, NON-WAL
 *
 * DETERMINISTIC VALUES:
 *   row[i].id = i
 *   row[i].v  = i * 2654435761L  (Knuth multiplicative hash — easy to verify)
 *   row[i].s  = SYMBOLS[i % SYMBOLS.length]  (exercises symbol maps + .k/.v index files)
 *   row[i].ts = BASE_TS + i * 1_000_000L (1 second per row → multiple partitions)
 *
 * WATERMARK: after each commit() returns, atomically write committed row count to
 *   <root>/_progress via write-to-temp+rename, so it is never half-written.
 *
 * Usage: java -cp benchmarks/target/benchmarks.jar \
 *            [-DcommitMode=SYNC|NOSYNC] \
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
    // Max rows (50 million); process is typically killed long before this
    static final long MAX_ROWS = 50_000_000L;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: CrashIngestWriter [-DcommitMode=SYNC|NOSYNC] <db-root>");
            System.exit(1);
        }
        final String dbRoot = args[0];
        new File(dbRoot).mkdirs();

        // Commit mode is configured via -DcommitMode=SYNC|NOSYNC (default: SYNC).
        // SYNC:   msync(MS_SYNC) on all dirty mmap pages before the _txn commit record →
        //         data reaches stable storage before commit is acknowledged (process-crash-
        //         consistent AND power-cut-durable when paired with a real block device).
        // NOSYNC: commits acknowledged without forcing pages to storage — survives process
        //         kills (page cache persists) but NOT power cuts (page cache discarded).
        final String commitModeProp = System.getProperty("commitMode", "SYNC");
        final int commitModeInt = parseCommitMode(commitModeProp);
        System.out.println("commitMode=" + commitModeProp + " (" + commitModeInt + ")");

        // -Dbatched=false forces the per-file msync(MS_SYNC) path (the proven baseline);
        // default true uses the batched flush optimization (sync_file_range + _cv device flush).
        final boolean batchedSync = Boolean.parseBoolean(System.getProperty("batched", "true"));
        System.out.println("batchedColumnSync=" + batchedSync);

        final CairoConfiguration cfg = new DefaultCairoConfiguration(dbRoot) {
            @Override
            public int getCommitMode() {
                return commitModeInt;
            }

            @Override
            public boolean isBatchedColumnSyncEnabled() {
                return batchedSync;
            }
        };

        // Step 1: create the NON-WAL table via DDL engine (same pattern as SyncCostProfiler)
        // NON-WAL is required so we use the direct TableWriter path (no WAL sequencer),
        // and so the _txn / _cv / _todo recovery files are exercised directly.
        createTable(cfg);

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

            // Step 3: ingest rows; never exit cleanly — wait for kill -9
            for (long id = 0; id < MAX_ROWS; id++) {
                // ts increases monotonically by 1 second per row, crossing day boundaries
                final long ts = BASE_TS + id * 1_000_000L;
                final TableWriter.Row row = writer.newRow(ts);
                row.putLong(0, id);                           // col 0: id
                row.putLong(1, id * 2_654_435_761L);          // col 1: v = Knuth hash
                row.putSym(2, SYMBOLS[(int)(id % SYMBOLS.length)]); // col 2: s (indexed)
                // col 3: ts is the designated timestamp, set by newRow(ts) — no putLong needed
                row.append();

                if ((id + 1) % K == 0) {
                    // commit(): in SYNC mode msync(MS_SYNC) all dirty pages before _txn write;
                    // in NOSYNC mode commits without forcing pages to storage.
                    writer.commit();
                    committedRows = id + 1;

                    // Durably record the acknowledged watermark so it survives a power cut on ALL
                    // filesystems: write-tmp → fsync tmp CONTENT → atomic rename → fsync the DIRECTORY.
                    // Without the fsyncs the watermark is lost on XFS after a cut (XFS does not
                    // auto-flush a rename-over-existing like ext4's auto_da_alloc heuristic), leaving
                    // the verifier with an empty _progress. The harness must hold its own bookkeeping
                    // to the same durability bar as the QuestDB data it is verifying.
                    final byte[] wm = Long.toString(committedRows).getBytes(StandardCharsets.US_ASCII);
                    try (FileChannel ch = FileChannel.open(progressTmp,
                            StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                        ch.write(ByteBuffer.wrap(wm));
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

                    System.out.println("committed " + committedRows);
                    System.out.flush();
                }
            }
            System.out.println("reached MAX_ROWS=" + MAX_ROWS + " without kill; exiting normally");
        }
    }

    /**
     * Parse the -DcommitMode property value into a CommitMode int constant.
     * Mirrors the same helper in CommitModeBenchmark; NOSYNC and SYNC are the two
     * modes relevant to the durability harnesses (ASYNC is equivalent to NOSYNC for
     * our purposes and is not exposed here).
     */
    static int parseCommitMode(String name) {
        return switch (name.toUpperCase()) {
            case "SYNC"   -> CommitMode.SYNC;
            case "NOSYNC" -> CommitMode.NOSYNC;
            default -> throw new IllegalArgumentException(
                    "Unknown commitMode '" + name + "'; expected SYNC or NOSYNC");
        };
    }

    /**
     * Create the NON-WAL table with an indexed symbol column.
     * Indexing s exercises the .k/.v symbol index files on every commit,
     * which are among the files that must be consistent after a process crash.
     */
    private static void createTable(CairoConfiguration cfg) {
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
                                + " timestamp(ts) partition by DAY bypass wal",
                        ctx, null);
            } catch (SqlException e) {
                throw new RuntimeException("DDL failed", e);
            }
        }
    }
}
