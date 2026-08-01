/*+*****************************************************************************
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
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.Micros;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Locale;

/**
 * Steady-state companion to {@link LiveViewHighCardinalityBenchmark}, which measures one
 * incremental batch immediately after the seed and so compares two builds at a fixed state
 * size. This one seeds once and then runs many incremental batches, so per-batch refresh
 * cost can be watched as retained window state grows - the shape of the reported
 * failure, where the view kept up early and fell progressively behind.
 * <p>
 * Same customer schema and live-view DDL, a brand-new {@code cod_acct_no} per row by
 * default, and a forced checkpoint per batch so the seal is measured rather than skipped.
 * <p>
 * Build and run:
 * <pre>
 * mvn -pl benchmarks -am package -o -DskipTests -Dmaven.test.skip=true
 *
 * java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED -Xmx12g \
 *     -cp benchmarks/target/benchmarks.jar \
 *     org.questdb.LiveViewSteadyStateBenchmark \
 *     --seed=2400000 --batch=135000 --batches=3 --checkpoint-rows=135000
 * </pre>
 */
public class LiveViewSteadyStateBenchmark {

    private static final long START_TS = 1_785_496_035_000_000L;
    private static final long TS_STEP_MICROS = 444L;

    public static void main(String[] args) throws Exception {
        long seedRows = 1_000_000L;
        int batchRows = 135_000;
        int batches = 15;
        long checkpointRows = 135_000L;
        long checkpointDurationMicros = 24L * Micros.HOUR_MICROS;
        boolean isIndexed = true;
        boolean isSymbolPreSized = true;
        int recycleAccounts = 0; // 0 = every row a brand new account
        for (String arg : args) {
            if (arg.startsWith("--seed=")) {
                seedRows = Long.parseLong(arg.substring(7));
            } else if (arg.startsWith("--batch=")) {
                batchRows = Integer.parseInt(arg.substring(8));
            } else if (arg.startsWith("--batches=")) {
                batches = Integer.parseInt(arg.substring(10));
            } else if (arg.startsWith("--checkpoint-rows=")) {
                checkpointRows = Long.parseLong(arg.substring(18));
            } else if (arg.startsWith("--checkpoint-duration-us=")) {
                checkpointDurationMicros = Long.parseLong(arg.substring(25));
            } else if (arg.startsWith("--presize-symbol=")) {
                isSymbolPreSized = Boolean.parseBoolean(arg.substring(17));
            } else if (arg.startsWith("--index=")) {
                isIndexed = Boolean.parseBoolean(arg.substring(8));
            } else if (arg.startsWith("--recycle-accounts=")) {
                recycleAccounts = Integer.parseInt(arg.substring(19));
            } else {
                throw new IllegalArgumentException("unknown argument: " + arg);
            }
        }

        System.out.printf(
                Locale.ROOT,
                "# seed=%d batch=%d batches=%d checkpointRows=%d preSizeSymbol=%s index=%s recycleAccounts=%d%n",
                seedRows, batchRows, batches, checkpointRows, isSymbolPreSized, isIndexed, recycleAccounts
        );

        final Path dbRoot = Files.createTempDirectory("lv-steady-");
        CairoEngine engine = null;
        final long finalCheckpointRows = checkpointRows;
        final long finalCheckpointDuration = checkpointDurationMicros;
        try {
            final CairoConfiguration configuration = new DefaultCairoConfiguration(dbRoot.toString()) {
                @Override
                public long getLiveViewCheckpointMaxDurationMicros() {
                    return finalCheckpointDuration;
                }

                @Override
                public long getLiveViewCheckpointRows() {
                    return finalCheckpointRows;
                }

                @Override
                public boolean isDevModeEnabled() {
                    return true;
                }
            };
            engine = new CairoEngine(configuration);
            engine.load();
            final SqlExecutionContext sqlCtx = new SqlExecutionContextImpl(engine, 1).with(
                    configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                    null, null, -1, null
            );

            final long totalRows = seedRows + (long) batchRows * batches;
            final String capacity = isSymbolPreSized ? " capacity " + symbolCapacity(totalRows) : "";
            final String indexClause = isIndexed ? " index capacity 4" : "";
            engine.execute(
                    "create table mm_transaction_live_created_at ("
                            + "created_at timestamp, "
                            + "cod_acct_no symbol" + capacity + " nocache" + indexClause + ", "
                            + "amt_txn double"
                            + ") timestamp(created_at) partition by hour wal",
                    sqlCtx
            );
            engine.execute(insertSql(1, seedRows, recycleAccounts), sqlCtx);
            drainWal(engine);

            engine.execute(
                    "create live view mm_transaction_live_created_at_view "
                            + "flush every 5s start from beginning as "
                            + "select created_at, cod_acct_no, "
                            + "sum(amt_txn) over w as cumulative_sum, "
                            + "count(cod_acct_no) over w as cumulative_count "
                            + "from mm_transaction_live_created_at "
                            + "window w as (partition by cod_acct_no order by created_at anchor daily '12:00')",
                    sqlCtx
            );
            final LiveViewInstance instance = engine.getLiveViewRegistry()
                    .getViewInstance("mm_transaction_live_created_at_view");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final long seedStart = System.nanoTime();
                drainLiveView(engine, instance, job);
                System.out.printf(Locale.ROOT, "# seed_ms=%.3f seed_checkpoint_ms=%.3f%n",
                        (System.nanoTime() - seedStart) / 1e6, instance.getHeadCheckpointWriteMicros() / 1e3);

                System.out.println("batch\tstate_rows\tbase_apply_ms\trefresh_ms\tcheckpoint_ms\trefresh_ex_cp_ms\trows_per_sec\tstate_bytes\tlag_seqtxn\tfaults");
                long firstRow = seedRows + 1;
                for (int b = 0; b < batches; b++) {
                    engine.execute(insertSql(firstRow, batchRows, recycleAccounts), sqlCtx);
                    final long baseStart = System.nanoTime();
                    drainWal(engine);
                    final long baseNanos = System.nanoTime() - baseStart;

                    final long checkpointRootIdBefore = instance.getHeadCheckpointRootId();
                    final long refreshStart = System.nanoTime();
                    drainLiveView(engine, instance, job);
                    final long refreshNanos = System.nanoTime() - refreshStart;
                    final boolean isCheckpointWritten = instance.getHeadCheckpointRootId() != checkpointRootIdBefore;
                    final double checkpointMs = isCheckpointWritten ? instance.getHeadCheckpointWriteMicros() / 1e3 : 0.0;

                    final long expected = firstRow - 1 + batchRows;
                    if (instance.getLvRowsTotal() != expected) {
                        throw new IllegalStateException("row mismatch: expected " + expected + ", got " + instance.getLvRowsTotal());
                    }
                    final long baseSeqTxn = engine.getTableSequencerAPI()
                            .getTxnTracker(engine.getTableTokenIfExists("mm_transaction_live_created_at"))
                            .getWriterTxn();
                    System.out.printf(
                            Locale.ROOT,
                            "%d\t%d\t%.3f\t%.3f\t%.3f\t%.3f\t%.0f\t%d\t%d\t%d%n",
                            b,
                            expected,
                            baseNanos / 1e6,
                            refreshNanos / 1e6,
                            checkpointMs,
                            refreshNanos / 1e6 - checkpointMs,
                            batchRows / (refreshNanos / 1e9),
                            instance.getHeadCheckpointStateBytes(),
                            baseSeqTxn - instance.getLastProcessedSeqTxn(),
                            instance.getRefreshFaultCount()
                    );
                    firstRow += batchRows;
                }
            }
        } finally {
            engine = Misc.free(engine);
            deleteRecursively(dbRoot);
        }
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult postVisitDirectory(Path d, IOException exc) throws IOException {
                Files.delete(d);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static void drainLiveView(CairoEngine engine, LiveViewInstance instance, LiveViewRefreshJob job) {
        instance.setLastFlushTimeUs(Numbers.LONG_NULL);
        for (int i = 0; i < 4_096; i++) {
            boolean isProgressed = false;
            while (job.run()) {
                isProgressed = true;
            }
            drainWal(engine);
            if (!isProgressed) {
                return;
            }
            instance.setLastFlushTimeUs(Numbers.LONG_NULL);
        }
        throw new IllegalStateException("live view did not quiesce");
    }

    private static void drainWal(CairoEngine engine) {
        try (ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 0)) {
            while (walApplyJob.run()) {
                // drain
            }
            if (new CheckWalTransactionsJob(engine).run()) {
                while (walApplyJob.run()) {
                    // drain
                }
            }
        }
    }

    private static String insertSql(long firstRow, long rows, int recycleAccounts) {
        final String acct = recycleAccounts > 0
                ? "'acct-' || ((x + " + (firstRow - 1) + ") % " + recycleAccounts + ")::string"
                : "'acct-' || (x + " + (firstRow - 1) + ")::string";
        return "insert into mm_transaction_live_created_at "
                + "select (" + START_TS + " + (x + " + (firstRow - 1) + ") * " + TS_STEP_MICROS + ")::timestamp, "
                + acct + ", "
                + "((x + " + (firstRow - 1) + ") % 2001 - 1000) * 0.01 "
                + "from long_sequence(" + rows + ")";
    }

    private static int symbolCapacity(long rows) {
        long capacity = 16;
        while (capacity < rows && capacity < (1L << 30)) {
            capacity <<= 1;
        }
        return (int) Math.min(capacity, 1L << 30);
    }
}
