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
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * PROCESS-CRASH-CONSISTENCY INGEST WRITER
 *
 * PURPOSE: Ingest rows into QuestDB in SYNC commit mode, recording an acknowledged-commit
 * watermark after every successful commit(). The process is then hard-killed via kill -9
 * by the harness script, and CrashVerifier reopens the DB to verify consistency.
 *
 * WHAT THIS TESTS — PROCESS-CRASH-CONSISTENCY, NOT POWER-LOSS DURABILITY:
 *   kill -9 terminates the JVM without running any shutdown hooks or finally-blocks,
 *   but it does NOT flush the OS page cache. This means:
 *     - Any data written to mmap'd memory-mapped files lives in the OS page cache and
 *       IS visible to a subsequently launched process reading the same files.
 *     - SYNC commit mode calls msync(MS_SYNC) before writing the _txn commit record,
 *       which ensures committed data pages are durable before the txn is finalised.
 *     - The test proves QuestDB's recovery path (torn-aux guards, _txn/_cv A/B metadata,
 *       _todo recovery file) leaves a CONSISTENT state after an abrupt mid-write kill.
 *   Power-loss durability (flushing page cache to disk) requires a separate dm-log-writes
 *   harness to simulate actual storage failures — this test does NOT cover that scenario.
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
 * Usage: java -cp benchmarks/target/benchmarks.jar org.questdb.CrashIngestWriter <db-root>
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
            System.err.println("Usage: CrashIngestWriter <db-root>");
            System.exit(1);
        }
        final String dbRoot = args[0];
        new File(dbRoot).mkdirs();

        // SYNC commit mode: msync(MS_SYNC) is called on all dirty mmap pages before the txn
        // commit record is written, ensuring committed data is visible from the OS page cache
        // before the next process reads it — critical for process-crash-consistency.
        final CairoConfiguration cfg = new DefaultCairoConfiguration(dbRoot) {
            @Override
            public int getCommitMode() {
                return CommitMode.SYNC;
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
                    // commit() in SYNC mode: msync(MS_SYNC) all dirty pages → write txn record
                    writer.commit();
                    committedRows = id + 1;

                    // Atomically record the acknowledged watermark (write-to-tmp + rename).
                    // rename(2) is atomic on POSIX — verifier sees either old or new value, never torn.
                    Files.writeString(progressTmp, Long.toString(committedRows), StandardCharsets.US_ASCII);
                    Files.move(progressTmp, progressPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

                    System.out.println("committed " + committedRows);
                    System.out.flush();
                }
            }
            System.out.println("reached MAX_ROWS=" + MAX_ROWS + " without kill; exiting normally");
        }
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
