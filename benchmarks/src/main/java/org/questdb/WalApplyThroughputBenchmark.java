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
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;

/**
 * Measures WAL-apply throughput on a plain (non-dedup, non-LV, non-mat-view) WAL
 * table. Its purpose is the live-view dedup-base neutrality gate: the dedup work
 * added a {@code SeqTxnTracker.recordApplied(...)} call to every applied commit in
 * {@code ApplyWal2TableJob} (a handful of plain volatile writes per committed
 * batch, for all WAL tables, never per row). This benchmark isolates that
 * per-commit apply cost so a before/after run can confirm the delta stays inside
 * noise.
 * <p>
 * Each iteration writes {@code COMMITS_PER_ITER} separate WAL transactions of
 * {@code rowsPerCommit} rows each (untimed - only the WAL write, not the apply),
 * then times a single WAL drain that applies all of them. The reported figure is
 * microseconds per applied commit. The {@code rowsPerCommit = 1} sweep maximises
 * the per-commit fraction of the work (where recordApplied lives); the larger
 * sweep amortises the fixed per-commit cost across more rows.
 * <p>
 * Run before (a commit prior to the dedup work) and after (branch HEAD) and
 * compare us_per_commit; the neutrality claim is that the delta sits inside
 * run-to-run noise. Manual timing (not JMH) so it runs with a plain
 * {@code java ... org.questdb.WalApplyThroughputBenchmark} launch, no fork /
 * add-exports gymnastics.
 */
public class WalApplyThroughputBenchmark {

    private static final int COMMITS_PER_ITER = 1_000;
    private static final int ITERATIONS = 8;
    private static final long START_EPOCH_MICROS = 1_704_067_200_000_000L; // 2024-01-01T00:00:00Z
    private static final int WARMUP_ITERATIONS = 3;
    private static final int[] ROWS_PER_COMMIT_SWEEP = {1, 50};

    public static void main(String[] args) throws Exception {
        System.out.printf("WalApplyThroughputBenchmark: %d commits/iter, %d iterations (+%d warmup)%n",
                COMMITS_PER_ITER, ITERATIONS, WARMUP_ITERATIONS);
        System.out.println();
        System.out.printf("%-16s %14s %14s %14s %14s %16s%n",
                "rowsPerCommit", "us_per_commit", "median_us", "min_us", "p99_us", "rows_per_sec");

        for (int rowsPerCommit : ROWS_PER_COMMIT_SWEEP) {
            runScenario(rowsPerCommit);
        }

        LogFactory.haltInstance();
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
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

    private static void drainWal(CairoEngine engine) {
        try (ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 0)) {
            //noinspection StatementWithEmptyBody
            while (walApplyJob.run()) ;
            if (new CheckWalTransactionsJob(engine).run()) {
                //noinspection StatementWithEmptyBody
                while (walApplyJob.run()) ;
            }
        }
    }

    private static double percentile(long[] sorted, double pct) {
        if (sorted.length == 0) {
            return 0;
        }
        int idx = (int) Math.ceil(pct * sorted.length / 100.0) - 1;
        idx = Math.max(0, Math.min(idx, sorted.length - 1));
        return sorted[idx];
    }

    private static void runScenario(int rowsPerCommit) throws Exception {
        final Path dbRoot = Files.createTempDirectory("walApplyBench-");
        try {
            final CairoConfiguration configuration = new BenchConfig(dbRoot.toString());
            try (CairoEngine engine = new CairoEngine(configuration)) {
                engine.load();
                final SqlExecutionContext sqlCtx = new SqlExecutionContextImpl(engine, 1).with(
                        configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null, null, -1, null
                );

                engine.execute(
                        "create table t (sym symbol, price double, ts timestamp) timestamp(ts) partition by DAY WAL",
                        sqlCtx
                );

                final long[] timings = new long[ITERATIONS];
                long tsMicros = START_EPOCH_MICROS;
                for (int i = 0; i < WARMUP_ITERATIONS + ITERATIONS; i++) {
                    tsMicros = runIteration(engine, sqlCtx, tsMicros, rowsPerCommit, timings, i - WARMUP_ITERATIONS);
                }

                Arrays.sort(timings);
                double sum = 0;
                for (long t : timings) sum += t;
                final double avgTotalNs = sum / timings.length;
                final double usPerCommit = avgTotalNs / 1_000.0 / COMMITS_PER_ITER;
                final double medianUsPerCommit = percentile(timings, 50) / 1_000.0 / COMMITS_PER_ITER;
                final double minUsPerCommit = timings[0] / 1_000.0 / COMMITS_PER_ITER;
                final double p99UsPerCommit = percentile(timings, 99) / 1_000.0 / COMMITS_PER_ITER;
                final double rowsPerSec = (double) COMMITS_PER_ITER * rowsPerCommit / (avgTotalNs / 1_000_000_000.0);

                System.out.printf("%-16d %14.3f %14.3f %14.3f %14.3f %16.0f%n",
                        rowsPerCommit, usPerCommit, medianUsPerCommit, minUsPerCommit, p99UsPerCommit, rowsPerSec);
            }
        } finally {
            deleteRecursively(dbRoot);
        }
    }

    private static long runIteration(
            CairoEngine engine,
            SqlExecutionContext sqlCtx,
            long tsMicros,
            int rowsPerCommit,
            long[] timings,
            int recordIdx
    ) throws Exception {
        // Untimed: write COMMITS_PER_ITER separate WAL transactions. Strictly
        // increasing timestamps keep the base append-only (no O3), so the drain
        // measures the plain forward-apply path.
        final StringBuilder sb = new StringBuilder();
        for (int c = 0; c < COMMITS_PER_ITER; c++) {
            sb.setLength(0);
            sb.append("insert into t(sym, price, ts) values ");
            for (int r = 0; r < rowsPerCommit; r++) {
                if (r > 0) {
                    sb.append(',');
                }
                sb.append("('S").append(r).append("',").append(100.0 + r).append(',')
                        .append(tsMicros).append("::timestamp)");
                tsMicros++;
            }
            engine.execute(sb.toString(), sqlCtx);
        }

        // Timed: apply all pending WAL transactions (fires recordApplied per commit).
        final long t0 = System.nanoTime();
        drainWal(engine);
        final long elapsed = System.nanoTime() - t0;
        if (recordIdx >= 0) {
            timings[recordIdx] = elapsed;
        }
        return tsMicros;
    }

    private static final class BenchConfig extends DefaultCairoConfiguration {
        BenchConfig(CharSequence root) {
            super(root);
        }

        @Override
        public boolean isDevModeEnabled() {
            return true;
        }
    }
}
