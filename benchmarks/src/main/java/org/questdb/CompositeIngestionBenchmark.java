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
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.griffin.SqlCompiler;
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
 * Measures the high-frequency small-commit ingestion cost of a COMPOSITE table (time + {@code exch}
 * dimension, i.e. {@code partition by day, exch}) against a byte-for-byte identical PLAIN twin
 * ({@code partition by day} only).
 * <p>
 * Mechanics: today, a composite table forces a full commit on every WAL apply -- it cannot defer/batch
 * successive small transactions behind the WAL LAG the way a plain table can, because each apply may
 * touch several exch "cells" at once and the composite writer has no lag-coalescing path yet. This
 * benchmark quantifies that per-commit gap for a realistic high-frequency ingestion shape (many small
 * transactions, each spanning multiple {@code exch} values -- e.g. multi-exchange tick ingestion). The
 * follow-up (#5, cell-aware WAL-LAG batching) is meant to close this gap; this benchmark exists to
 * quantify it, not to assert a threshold on it.
 * <p>
 * Twin tables {@code ci} (composite) and {@code pi} (plain) are created ONCE in {@link #main}. The
 * measured unit is one "commit": insert a small batch of {@link #BATCH_ROWS} rows (one per {@code exch}
 * value, so every batch drives a multi-cell commit on the composite side), then drain the WAL
 * (ApplyWal2TableJob + CheckWalTransactionsJob) so the batch is actually applied/committed before the
 * next one starts. The composite loop and the plain loop run the identical batch sequence (same
 * timestamps/exch/px per iteration index, computed deterministically) but are timed separately, each
 * for {@link #WARMUP_ITERATIONS} (unrecorded) + {@link #K} (recorded) iterations. All rows for a run
 * land inside a single day partition (small fixed timestamp step) so the numbers reflect commit/apply
 * overhead rather than partition-creation noise.
 * <p>
 * Tunables (system properties): {@code composite.bench.k} (measured iterations/table, default 2000),
 * {@code composite.bench.warmup} (warmup iterations/table, default 100), {@code composite.bench.exch}
 * (distinct exch values = rows/batch, default 6; {@code exch=1} makes every batch a SINGLE-CELL ordered
 * append-only commit -- the composite single-cell fast-append's target shape), {@code
 * composite.bench.step.us} (microseconds between consecutive rows, default 1000), {@code
 * composite.bench.fastappend} (boolean, default {@code false}) -- overrides {@link
 * CairoConfiguration#isWalCompositeFastAppendEnabled()} for this run's engine so the SAME benchmark can
 * measure flag-off (today's full composite O3 path on every commit, the baseline) vs flag-on (the
 * fast-append early-return, when the commit shape and the kept-open cell qualify) across two separate
 * process invocations. The engagement of the fast path is reported directly from the writer's own static
 * counters ({@link TableWriter#getCompositeFastAppendEligibleCount()} / {@link
 * TableWriter#getCompositeFastAppendCommittedCount()}) around the composite loop, so a run's printed
 * output is also the engagement proof, not just a timing number.
 * <p>
 * Build (note {@code -am} so the benchmark links the in-tree core, not the installed jar) and run via
 * this class's plain {@code main} (manual {@code System.nanoTime} timing + percentile table -- the
 * metric here is commit-loop throughput, not a single-op JMH micro):
 * <pre>
 * mvn -pl benchmarks -am package -o -DskipTests
 * java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED \
 *      --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED \
 *      -cp benchmarks/target/benchmarks.jar org.questdb.CompositeIngestionBenchmark
 * </pre>
 */
public class CompositeIngestionBenchmark {

    // Distinct exch values; also the number of rows per batch (one row per exch => every batch is a
    // multi-cell commit on the composite side).
    private static final int NUM_EXCH = Integer.getInteger("composite.bench.exch", 6);
    private static final int BATCH_ROWS = NUM_EXCH;
    // Microseconds between consecutive rows (within and across batches). Kept small and fixed so the
    // whole run (warmup + measured, both tables) stays inside a single day partition -- the numbers
    // reflect per-commit apply overhead, not partition-creation overhead.
    private static final long STEP_MICROS = Long.getLong("composite.bench.step.us", 1000L);
    private static final int WARMUP_ITERATIONS = Integer.getInteger("composite.bench.warmup", 100);
    private static final int K = Integer.getInteger("composite.bench.k", 2000);
    // Composite single-cell fast-append (spec 1) override: forces isWalCompositeFastAppendEnabled() for
    // this run's engine regardless of the production default (off). Default false preserves this
    // benchmark's pre-existing behavior (full composite O3 path on every commit).
    private static final boolean FASTAPPEND_ENABLED = Boolean.getBoolean("composite.bench.fastappend");
    // Anchor seed data at 2024-01-01T00:00:00Z.
    private static final String BASE_TS = "2024-01-01T00:00:00.000000Z";

    public static void main(String[] args) throws Exception {
        System.out.printf(
                "CompositeIngestionBenchmark: %d exch values, %d rows/batch, %d warmup + %d measured commits/table, fastappend=%b%n",
                NUM_EXCH, BATCH_ROWS, WARMUP_ITERATIONS, K, FASTAPPEND_ENABLED);
        System.out.println();

        final Path root = Files.createTempDirectory("composite-ingest-bench-");
        try {
            final CairoConfiguration configuration = new DefaultCairoConfiguration(root.toString()) {
                @Override
                public boolean isWalCompositeFastAppendEnabled() {
                    return FASTAPPEND_ENABLED;
                }
            };
            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = engine.getSqlCompiler()) {

                // Suppress per-query progress logging: this loop calls engine.execute() thousands of
                // times; unthrottled per-query logging would flood stdout and add I/O noise inside the
                // timed window.
                final SqlExecutionContext ctx = new SqlExecutionContextImpl(engine, 1) {
                    @Override
                    public boolean shouldLogSql() {
                        return false;
                    }
                }.with(
                        configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null, null, -1, null
                );

                engine.execute(
                        "create table ci (ts timestamp, exch symbol, px double)" +
                                " timestamp(ts) partition by day, exch wal", ctx);
                engine.execute(
                        "create table pi (ts timestamp, exch symbol, px double)" +
                                " timestamp(ts) partition by day wal", ctx);

                System.out.printf("%-14s %10s %10s %10s %10s %10s%n",
                        "table", "avg_us", "p50_us", "p90_us", "p99_us", "min_us");

                // Engagement proof (Task 4): snapshot the writer's own static fast-append counters
                // immediately around the ci loop -- they are JVM-wide (see TableWriter's own field docs)
                // but nothing else touches "ci" in this window, and "pi" (dimCount==0) never increments
                // them regardless of the flag, so a before/after delta here attributes cleanly.
                final long fastAppendEligibleBefore = TableWriter.getCompositeFastAppendEligibleCount();
                final long fastAppendCommittedBefore = TableWriter.getCompositeFastAppendCommittedCount();
                final long[] compositeTimings = runCommitLoop(engine, ctx, "ci");
                final long fastAppendEligibleDelta = TableWriter.getCompositeFastAppendEligibleCount() - fastAppendEligibleBefore;
                final long fastAppendCommittedDelta = TableWriter.getCompositeFastAppendCommittedCount() - fastAppendCommittedBefore;
                final Stats compositeStats = Stats.of(compositeTimings);
                printStats("composite(ci)", compositeStats);
                final long totalCiCommits = WARMUP_ITERATIONS + K;
                System.out.printf(
                        "composite fast-append engagement: flag=%b eligible=%d committed=%d of %d total ci commits (%d warmup + %d measured)%n",
                        FASTAPPEND_ENABLED, fastAppendEligibleDelta, fastAppendCommittedDelta, totalCiCommits,
                        WARMUP_ITERATIONS, K);
                final long compositeRows = queryRowCount(compiler, ctx, "ci");
                final long expectedRows = (long) (WARMUP_ITERATIONS + K) * BATCH_ROWS;
                if (compositeRows != expectedRows) {
                    throw new IllegalStateException(
                            "composite commit loop landed " + compositeRows + " rows, expected " + expectedRows +
                                    " -- WAL apply likely did not run to completion.");
                }

                final long[] plainTimings = runCommitLoop(engine, ctx, "pi");
                final Stats plainStats = Stats.of(plainTimings);
                printStats("plain(pi)", plainStats);
                final long plainRows = queryRowCount(compiler, ctx, "pi");
                if (plainRows != expectedRows) {
                    throw new IllegalStateException(
                            "plain commit loop landed " + plainRows + " rows, expected " + expectedRows +
                                    " -- WAL apply likely did not run to completion.");
                }

                System.out.println();
                System.out.printf(
                        "composite/plain per-commit ratio -- avg: %.2fx  p50: %.2fx  p90: %.2fx  p99: %.2fx%n",
                        compositeStats.avg / plainStats.avg,
                        compositeStats.p50 / plainStats.p50,
                        compositeStats.p90 / plainStats.p90,
                        compositeStats.p99 / plainStats.p99
                );
            }
        } finally {
            deleteRecursively(root);
        }

        LogFactory.haltInstance();
    }

    /**
     * Deterministic per-iteration batch: {@link #BATCH_ROWS} rows, one per exch value (0..NUM_EXCH-1),
     * strictly increasing timestamps. Computed purely from {@code tableName} and {@code iterationIndex}
     * so the composite ({@code ci}) and plain ({@code pi}) loops insert byte-identical batches.
     */
    private static String buildBatchInsertSql(String tableName, long iterationIndex) {
        final StringBuilder sb = new StringBuilder(32 + BATCH_ROWS * 48);
        sb.append("insert into ").append(tableName).append("(ts, exch, px) values ");
        for (int j = 0; j < BATCH_ROWS; j++) {
            if (j > 0) {
                sb.append(',');
            }
            final long offsetMicros = iterationIndex * BATCH_ROWS * STEP_MICROS + (long) j * STEP_MICROS;
            final int exchIdx = j % NUM_EXCH;
            final double px = 100.0 + ((iterationIndex * BATCH_ROWS + j) % 997);
            sb.append("(('").append(BASE_TS).append("'::timestamp + ").append(offsetMicros)
                    .append("L)::timestamp,'EXCH").append(exchIdx).append("',").append(px).append(')');
        }
        return sb.toString();
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

    private static void printStats(String label, Stats s) {
        System.out.printf("%-14s %10.2f %10.2f %10.2f %10.2f %10.2f%n",
                label, s.avg, s.p50, s.p90, s.p99, s.min);
    }

    private static long queryRowCount(SqlCompiler compiler, SqlExecutionContext ctx, String tableName) throws Exception {
        try (var factory = compiler.compile("select count() from " + tableName, ctx).getRecordCursorFactory();
             var cursor = factory.getCursor(ctx)) {
            if (cursor.hasNext()) {
                return cursor.getRecord().getLong(0);
            }
        }
        return -1;
    }

    /**
     * Runs {@link #WARMUP_ITERATIONS} + {@link #K} commit iterations against {@code tableName}: insert
     * one small batch, drain the WAL so it is actually committed, time the whole insert+drain pair. Only
     * the last {@link #K} iterations are recorded (the first {@link #WARMUP_ITERATIONS} still insert
     * real rows -- they are just excluded from the timing stats).
     */
    private static long[] runCommitLoop(CairoEngine engine, SqlExecutionContext ctx, String tableName) throws Exception {
        final long[] timings = new long[K];
        for (int i = 0; i < WARMUP_ITERATIONS + K; i++) {
            final String sql = buildBatchInsertSql(tableName, i);
            final long t0 = System.nanoTime();
            engine.execute(sql, ctx);
            drainWal(engine);
            final long elapsed = System.nanoTime() - t0;
            if (i >= WARMUP_ITERATIONS) {
                timings[i - WARMUP_ITERATIONS] = elapsed;
            }
        }
        return timings;
    }

    /** Commit-latency summary, all fields in microseconds. */
    private static final class Stats {
        final double avg;
        final double min;
        final double p50;
        final double p90;
        final double p99;

        private Stats(double avg, double p50, double p90, double p99, double min) {
            this.avg = avg;
            this.p50 = p50;
            this.p90 = p90;
            this.p99 = p99;
            this.min = min;
        }

        static Stats of(long[] timingsNanos) {
            final long[] sorted = timingsNanos.clone();
            Arrays.sort(sorted);
            double sum = 0;
            for (long t : sorted) {
                sum += t;
            }
            final double avgUs = (sorted.length == 0 ? 0 : sum / sorted.length) / 1000.0;
            final double p50Us = percentile(sorted, 50) / 1000.0;
            final double p90Us = percentile(sorted, 90) / 1000.0;
            final double p99Us = percentile(sorted, 99) / 1000.0;
            final double minUs = (sorted.length == 0 ? 0 : sorted[0]) / 1000.0;
            return new Stats(avgUs, p50Us, p90Us, p99Us, minUs);
        }
    }
}
