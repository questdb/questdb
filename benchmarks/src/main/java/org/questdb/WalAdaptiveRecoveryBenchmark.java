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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Files;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8StringSink;

/**
 * SP-C recovery-time harness (plain timed main, NOT JMH — the crash/reopen sequence doesn't fit JMH's
 * shared-{@code @State} lifecycle). Measures {@code RecoveryCoordinator.recover()} + WAL catch-up time
 * as a function of the un-applied post-epoch tail — i.e. "how long does a restart take to become
 * current" under ADAPTIVE, which the durable-epoch cadence bounds.
 *
 * <p>Per tail size {@code T} (in committed-but-un-applied txns):
 * <ol>
 *   <li><b>Build</b> (untimed): open engine1 (ADAPTIVE, epoch every apply batch), ingest + drain a
 *       WARMUP so the table is materialized and a durable epoch lands at WARMUP end; then ingest
 *       {@code T} more txns and DO NOT drain them (they stay in the WAL, past the epoch). Close.</li>
 *   <li><b>Recover</b> (timed): open engine2. Its ctor runs {@code RecoveryCoordinator.recover()}
 *       (rolls the table to the epoch cut — a no-op here since WARMUP==epoch), then we drain the WAL
 *       queue to the frontier (re-derives the {@code T}-txn tail). We report ctor time (boot incl.
 *       recover) and catch-up drain time separately, plus the total.</li>
 * </ol>
 *
 * <p><b>What this shows:</b> recovery/catch-up time is ~linear in the un-applied tail, so an operator
 * bounds worst-case recovery by choosing {@code cairo.adaptive.epoch.interval.ms} (which bounds how
 * far the tail can run past the last epoch). This is a TIMING proxy on a CLEAN reopen — crash
 * CORRECTNESS (torn-tail rewind, zero corruption, every acked txn survives) is SP-D's job, already
 * covered by the adaptive crash-fuzz + power-loss harness; do not read a durability verdict here.
 *
 * <p>Uses a small group-commit window ({@code W=5ms}) for the BUILD phase only, to keep tail ingest
 * from being dominated by per-commit fdatasync — recovery replays from the WAL regardless of W.
 *
 * <p>Run (uber-jar on the classpath; needs the module args — see the SP-C spec):
 * <pre>
 *   JAVA_TOOL_OPTIONS="...--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED..." \
 *     java -cp benchmarks/target/benchmarks.jar org.questdb.WalAdaptiveRecoveryBenchmark [tailSizes...]
 * </pre>
 * Default tail sizes: {@code 500 2000}. Optional args override them (e.g. {@code 250 1000 4000}).
 */
public class WalAdaptiveRecoveryBenchmark {

    private static final long APPEND_PAGE_SIZE = 256 * 1024L;
    private static final int COLUMN_COUNT = 20;
    private static final int GROUP_WINDOW_US = 5000; // W=5ms for build-phase ingest speed only
    private static final int ROWS_PER_BATCH = 500;
    private static final String[] SYMBOLS = {"alpha", "beta", "gamma", "delta", "epsilon"};
    private static final String TABLE_NAME = "walrecoverybench";
    private static final int WARMUP_BATCHES = 200;

    private final Rnd rnd = new Rnd();
    private final Utf8StringSink varcharSink = new Utf8StringSink();
    private int symbolColIndex;
    private long ts;
    private int varcharColIndex;

    public static void main(String[] args) {
        int[] tailSizes = {500, 2000};
        if (args.length > 0) {
            tailSizes = new int[args.length];
            for (int i = 0; i < args.length; i++) {
                tailSizes[i] = Integer.parseInt(args[i].trim());
            }
        }
        System.out.println("=== SP-C adaptive recovery-time harness (shared box, RELATIVE only) ===");
        System.out.println("mode=ADAPTIVE  W(build)=" + GROUP_WINDOW_US + "us  epoch=every-apply-batch"
                + "  rows/txn=" + ROWS_PER_BATCH + "  warmup=" + WARMUP_BATCHES + " txns");
        System.out.printf("%-10s %-12s %-16s %-16s %-16s %-14s%n",
                "tailTxns", "tailRows", "bootRecover(ms)", "catchupDrain(ms)", "total(ms)", "catchup(rows/s)");
        final WalAdaptiveRecoveryBenchmark bench = new WalAdaptiveRecoveryBenchmark();
        for (int tail : tailSizes) {
            bench.runOne(tail);
        }
        System.out.println("=== done ===");
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

    private CairoConfiguration config(String dbRoot) {
        return new DefaultCairoConfiguration(dbRoot) {
            @Override
            public long getAdaptiveCommitGroupWindowUs() {
                return GROUP_WINDOW_US;
            }

            @Override
            public long getAdaptiveEpochIntervalMs() {
                return 0; // epoch on every apply batch -> last epoch lands at WARMUP end
            }

            @Override
            public int getCommitMode() {
                return CommitMode.ADAPTIVE;
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
    }

    private void drainToFrontier(ApplyWal2TableJob applyJob, CheckWalTransactionsJob checkJob) {
        for (int round = 0; round < 256; round++) {
            applyJob.drain(0);
            boolean more = checkJob.run();
            applyJob.drain(0);
            if (!more) {
                break;
            }
        }
    }

    private void executeDdl(CairoEngine engine, String ddl) {
        SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                .with(
                        engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
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

    private void ingestBatch(WalWriter walWriter) {
        final int varIdx = varcharColIndex;
        final int symIdx = symbolColIndex;
        for (int i = 0; i < ROWS_PER_BATCH; i++) {
            TableWriter.Row row = walWriter.newRow(ts++);
            for (int c = 1; c < varIdx; c++) {
                row.putLong(c, rnd.nextLong());
            }
            int varLen = 8 + (rnd.nextPositiveInt() % 57);
            varcharSink.clear();
            rnd.nextUtf8AsciiStr(varLen, varcharSink);
            row.putVarchar(varIdx, varcharSink);
            row.putSym(symIdx, SYMBOLS[rnd.nextPositiveInt() % SYMBOLS.length]);
            row.append();
        }
        walWriter.commit();
    }

    private void runOne(int tailTxns) {
        final String baseDir = new java.io.File("/data").isDirectory() ? "/data" : System.getProperty("user.home");
        final String dbRoot = baseDir + "/qdb-walrecoverybench-" + System.nanoTime();
        new java.io.File(dbRoot).mkdirs();
        try {
            // ---- Phase 1: BUILD (untimed) ----
            {
                final CairoEngine engine = new CairoEngine(config(dbRoot));
                try {
                    final int longCols = Math.max(0, COLUMN_COUNT - 2);
                    final StringBuilder ddl = new StringBuilder("create table ").append(TABLE_NAME).append(" (ts timestamp");
                    for (int c = 0; c < longCols; c++) {
                        ddl.append(", c").append(c).append(" long");
                    }
                    varcharColIndex = 1 + longCols;
                    symbolColIndex = varcharColIndex + 1;
                    ddl.append(", v varchar, s symbol) timestamp(ts) partition by DAY wal");
                    executeDdl(engine, ddl.toString());

                    final TableToken token = engine.verifyTableName(TABLE_NAME);
                    ts = 0;
                    rnd.reset();

                    final WalWriter walWriter = engine.getWalWriter(token);
                    final ApplyWal2TableJob applyJob = new ApplyWal2TableJob(engine, 0);
                    final CheckWalTransactionsJob checkJob = new CheckWalTransactionsJob(engine);
                    try {
                        // WARMUP: ingest + drain so the table is materialized and an epoch lands at the end.
                        for (int b = 0; b < WARMUP_BATCHES; b++) {
                            ingestBatch(walWriter);
                        }
                        drainToFrontier(applyJob, checkJob);
                        // TAIL: commit T more txns, DO NOT drain -> they stay un-applied, past the epoch.
                        for (int b = 0; b < tailTxns; b++) {
                            ingestBatch(walWriter);
                        }
                    } finally {
                        applyJob.close();
                        walWriter.close();
                    }
                } finally {
                    engine.close();
                }
            }

            // ---- Phase 2: RECOVER (timed) ----
            final long tCtor0 = System.nanoTime();
            final CairoEngine engine2 = new CairoEngine(config(dbRoot)); // ctor runs RecoveryCoordinator.recover()
            final long tCtor1 = System.nanoTime();
            long tDrain1;
            try {
                final ApplyWal2TableJob applyJob = new ApplyWal2TableJob(engine2, 0);
                final CheckWalTransactionsJob checkJob = new CheckWalTransactionsJob(engine2);
                try {
                    drainToFrontier(applyJob, checkJob);
                } finally {
                    applyJob.close();
                }
                tDrain1 = System.nanoTime();
            } finally {
                engine2.close();
            }

            final double bootMs = (tCtor1 - tCtor0) / 1e6;
            final double drainMs = (tDrain1 - tCtor1) / 1e6;
            final double totalMs = (tDrain1 - tCtor0) / 1e6;
            final long tailRows = (long) tailTxns * ROWS_PER_BATCH;
            final double rowsPerSec = drainMs > 0 ? (tailRows / (drainMs / 1000.0)) : 0;
            System.out.printf("%-10d %-12d %-16.2f %-16.2f %-16.2f %-14.0f%n",
                    tailTxns, tailRows, bootMs, drainMs, totalMs, rowsPerSec);
        } finally {
            deleteDirectory(new java.io.File(dbRoot));
        }
    }
}
