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
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.NotNull;

/**
 * Deterministic sync-cost profiler: counts syscalls + bytes (not timing).
 *
 * msync calls, msync bytes, fsync calls, fdatasync calls are DETERMINISTIC for a
 * fixed workload — they are exactly what the Goal-2 fdatasync+narrowing upgrade changes.
 * Counting them gives noise-free before/after comparison.
 *
 * Usage:
 *   java -cp benchmarks/target/benchmarks.jar org.questdb.SyncCostProfiler
 * or:
 *   mvn -pl benchmarks exec:java -Dexec.mainClass=org.questdb.SyncCostProfiler
 */
public class SyncCostProfiler {

    private static final String TABLE_NAME = "bench";
    private static final String[] SYMBOLS = {"alpha", "beta", "gamma", "delta", "epsilon"};
    /** 256 KiB — same as CommitModeBenchmark so file-extends happen at the same rate */
    private static final long APPEND_PAGE_SIZE = 256 * 1024L;

    /** Total rows inserted per mode run */
    private static final int TOTAL_ROWS = 200_000;
    /** Commit batch size — gives 200 commits */
    private static final int ROWS_PER_COMMIT = 1_000;

    public static void main(String[] args) throws Exception {
        System.out.println("QuestDB SyncCostProfiler");
        System.out.println("Workload: " + TOTAL_ROWS + " rows, " + ROWS_PER_COMMIT + " rows/commit, "
                + (TOTAL_ROWS / ROWS_PER_COMMIT) + " commits per mode");
        System.out.println("DB root: $HOME (real disk)");
        System.out.println();

        // Header. The flush-batching optimization moves cost between these columns: MS_SYNC msync/cmt and
        // fdatasync/cmt should DROP (columns no longer per-file device-flush), while MS_ASYNC msync/cmt and
        // the new sfr/cmt (sync_file_range) should RISE (the kick+drain that replace them).
        String hdr = String.format(
                "%-10s  %7s  %10s  %12s  %11s  %11s  %8s  %10s  %13s",
                "Mode", "Commits",
                "msync/cmt", "msyncMB/cmt",
                "MS_SYNC/cmt", "MS_ASYN/cmt", "sfr/cmt",
                "fsync/cmt", "fdatasync/cmt");
        System.out.println(hdr);
        System.out.println("-".repeat(hdr.length()));

        for (int mode : new int[]{CommitMode.NOSYNC, CommitMode.ASYNC, CommitMode.SYNC}) {
            runMode(mode);
        }

        System.out.println();
        System.out.println("Done.");
    }

    private static void runMode(int mode) throws Exception {
        String modeName = modeName(mode);
        String dbRoot = System.getProperty("user.home") + "/qdb-synccost-" + System.nanoTime();
        new java.io.File(dbRoot).mkdirs();

        CountingFilesFacade cff = new CountingFilesFacade();

        CairoConfiguration cfg = new DefaultCairoConfiguration(dbRoot) {
            @Override
            public int getCommitMode() {
                return mode;
            }

            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return cff;
            }

            @Override
            public long getDataAppendPageSize() {
                return APPEND_PAGE_SIZE;
            }

            @Override
            public long getMiscAppendPageSize() {
                return Files.ceilPageSize(APPEND_PAGE_SIZE);
            }
        };

        // Create table schema (DDL syncs not counted — counters reset after)
        executeDdl(
                "create table " + TABLE_NAME
                        + " (ts timestamp, l long, d double, v varchar, s symbol)"
                        + " timestamp(ts) partition by DAY bypass wal",
                cfg, cff
        );

        // Reset counters AFTER DDL so only insert/commit syncs are measured
        cff.reset();

        TableToken token = new TableToken(TABLE_NAME, TABLE_NAME, null, 0, false, false, false);
        CairoEngine writerEngine = new CairoEngine(cfg);
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
        );

        Rnd rnd = new Rnd(42L, 42L); // fixed seed — deterministic workload
        Utf8StringSink varcharSink = new Utf8StringSink();
        long ts = 0;
        int commits = 0;

        for (int i = 0; i < TOTAL_ROWS; i++) {
            TableWriter.Row row = writer.newRow(ts++);
            row.putLong(1, rnd.nextLong());
            row.putDouble(2, rnd.nextDouble());
            int varLen = 8 + (rnd.nextPositiveInt() % 57);
            varcharSink.clear();
            rnd.nextUtf8AsciiStr(varLen, varcharSink);
            row.putVarchar(3, varcharSink);
            row.putSym(4, SYMBOLS[rnd.nextPositiveInt() % SYMBOLS.length]);
            row.append();

            if ((i + 1) % ROWS_PER_COMMIT == 0) {
                writer.commit();
                commits++;
            }
        }
        // Final commit for any trailing rows
        writer.commit();
        commits++;

        long msyncCalls = cff.msyncCalls;
        long msyncBytesTotal = cff.msyncBytesTotal;
        long msyncSyncCalls = cff.msyncSyncCalls;
        long msyncAsyncCalls = cff.msyncAsyncCalls;
        long syncFileRangeCalls = cff.syncFileRangeCalls;
        long fsyncCalls = cff.fsyncCalls;
        long fdatasyncCalls = cff.fdatasyncCalls;

        try {
            writer.close();
        } catch (Exception ignored) {}
        try {
            writerEngine.close();
        } catch (Exception ignored) {}

        deleteDirectory(new java.io.File(dbRoot));

        double msyncPerCommit = (double) msyncCalls / commits;
        double msyncMBTotal = msyncBytesTotal / (1024.0 * 1024.0);
        double msyncMBPerCommit = msyncMBTotal / commits;
        double msyncSyncPerCommit = (double) msyncSyncCalls / commits;
        double msyncAsyncPerCommit = (double) msyncAsyncCalls / commits;
        double sfrPerCommit = (double) syncFileRangeCalls / commits;
        double fsyncPerCommit = (double) fsyncCalls / commits;
        double fdatasyncPerCommit = (double) fdatasyncCalls / commits;

        System.out.printf(
                "%-10s  %7d  %10.2f  %12.4f  %11.2f  %11.2f  %8.2f  %10.2f  %13.2f%n",
                modeName, commits,
                msyncPerCommit, msyncMBPerCommit,
                msyncSyncPerCommit, msyncAsyncPerCommit, sfrPerCommit,
                fsyncPerCommit, fdatasyncPerCommit
        );
    }

    // ---- Counting FilesFacade ----

    /**
     * Wraps FilesFacadeImpl.INSTANCE and counts msync/fsync/fdatasync calls + msync bytes.
     * Single-writer thread, so plain longs are fine (no atomics needed).
     */
    static class CountingFilesFacade extends FilesFacadeImpl {

        long msyncCalls = 0;
        long msyncBytesTotal = 0;
        long msyncAsyncCalls = 0;
        long msyncSyncCalls = 0;
        long syncFileRangeCalls = 0;
        long fsyncCalls = 0;
        long fdatasyncCalls = 0;
        long fsyncAndCloseCalls = 0;

        void reset() {
            msyncCalls = 0;
            msyncBytesTotal = 0;
            msyncAsyncCalls = 0;
            msyncSyncCalls = 0;
            syncFileRangeCalls = 0;
            fsyncCalls = 0;
            fdatasyncCalls = 0;
            fsyncAndCloseCalls = 0;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            msyncCalls++;
            msyncBytesTotal += len;
            if (async) {
                msyncAsyncCalls++;
            } else {
                msyncSyncCalls++;
            }
            super.msync(addr, len, async);
        }

        @Override
        public void fsync(long fd) {
            fsyncCalls++;
            super.fsync(fd);
        }

        @Override
        public int syncFileRange(long fd, long offset, long nbytes, int flags) {
            syncFileRangeCalls++;
            return super.syncFileRange(fd, offset, nbytes, flags);
        }

        @Override
        public void fdatasync(long fd) {
            fdatasyncCalls++;
            super.fdatasync(fd);
        }

        @Override
        public void fsyncAndClose(long fd) {
            fsyncAndCloseCalls++;
            super.fsyncAndClose(fd);
        }
    }

    // ---- helpers ----

    private static String modeName(int mode) {
        if (mode == CommitMode.NOSYNC) return "NOSYNC";
        if (mode == CommitMode.ASYNC)  return "ASYNC";
        if (mode == CommitMode.SYNC)   return "SYNC";
        return "UNKNOWN";
    }

    private static void executeDdl(String ddl, CairoConfiguration cfg, CountingFilesFacade cff) {
        try (CairoEngine engine = new CairoEngine(cfg)) {
            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(
                            cfg.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null,
                            null,
                            -1,
                            null
                    );
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                CairoEngine.execute(compiler, ddl, ctx, null);
            } catch (SqlException e) {
                throw new RuntimeException("DDL failed: " + ddl, e);
            }
        }
    }

    private static void deleteDirectory(java.io.File dir) {
        if (dir == null || !dir.exists()) return;
        java.io.File[] children = dir.listFiles();
        if (children != null) {
            for (java.io.File child : children) {
                if (child.isDirectory()) deleteDirectory(child);
                else child.delete();
            }
        }
        dir.delete();
    }
}
