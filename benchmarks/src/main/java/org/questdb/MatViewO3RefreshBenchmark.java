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
import io.questdb.cairo.mv.MatViewRefreshJob;
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
 * Demonstrates the wasted-recompute cost of an aggregated incremental refresh
 * after an O3 historical write into the base table of a mat view.
 * <p>
 * Mechanics, in MatViewRefreshJob:
 * <ol>
 *   <li>findRefreshIntervals collapses the cached refresh intervals to a single
 *       [minTs, maxTs] envelope = [getQuick(0), getQuick(size-1)].</li>
 *   <li>estimateBucketsForRows produces an iterator step proportional to
 *       totalBuckets * rowsPerQuery / tableRows, clamped by
 *       matViewMaxRefreshStepUs (default 1 year).</li>
 *   <li>SampleByIntervalIterator advances by `step` buckets; each step-group is
 *       skipped only if it intersects no cached interval. When step >= the
 *       O3-to-current gap, a single step-group covers both endpoints and the
 *       cursor's range filter is set to the full envelope -- every non-empty
 *       bucket inside is recomputed.</li>
 * </ol>
 * <p>
 * The benchmark builds a 1-minute OHLC mat view over NUM_SYMBOLS symbols, seeds
 * SEED_MINUTES of base data, and measures the cost of refreshing after a
 * (currentTick + O3 tick) WAL pair. Two sweeps:
 * <ul>
 *   <li>matViewRowsPerQueryEstimate -- 1M (close to production default) vs 1k
 *       (forces step to shrink toward a single bucket).</li>
 *   <li>o3LagMinutes -- how far back the O3 tick lands. 0 disables the O3
 *       insert (baseline).</li>
 * </ul>
 * <p>
 * Headline numbers on a 512-symbol, 1440-minute seed get filled in by the
 * first run; see the README/PR description for the latest baseline.
 */
public class MatViewO3RefreshBenchmark {

    private static final int ITERATIONS = 20;
    private static final int NUM_SYMBOLS = 512;
    // Anchor seed data at 2024-01-01T00:00:00Z (in microseconds since the epoch).
    private static final long SEED_EPOCH_MICROS = 1_704_067_200_000_000L;
    private static final int SEED_MINUTES = 24 * 60; // 1 day of seed data
    private static final int WARMUP_ITERATIONS = 2;
    private static final long[] O3_LAG_MINUTES_SWEEP = {0, 30, 120, 720};
    private static final long[] ROWS_PER_QUERY_SWEEP = {1_000_000L, 1_000L};

    public static void main(String[] args) throws Exception {
        System.out.printf("MatViewO3RefreshBenchmark: %d symbols, %d-minute seed, %d iterations/scenario%n",
                NUM_SYMBOLS, SEED_MINUTES, ITERATIONS);
        System.out.println();

        System.out.printf("%-18s %-14s %12s %12s %12s %12s %14s%n",
                "rowsPerQuery", "o3LagMinutes", "avg_ms", "median_ms", "p99_ms", "min_ms", "rowsEmitted");

        for (long rowsPerQuery : ROWS_PER_QUERY_SWEEP) {
            for (long o3LagMinutes : O3_LAG_MINUTES_SWEEP) {
                runScenario(rowsPerQuery, o3LagMinutes);
            }
        }

        LogFactory.haltInstance();
    }

    private static void appendInsertRow(StringBuilder sb, long tsMicros, int symbolId, double price) {
        sb.append("('S").append(symbolId).append("',")
                .append(price).append(",")
                .append(tsMicros).append("::timestamp)");
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

    private static void drainMatView(CairoEngine engine) {
        try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine, 1)) {
            //noinspection StatementWithEmptyBody
            while (refreshJob.run(0)) ;
        }
    }

    private static void drainWal(CairoEngine engine) {
        try (ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 0)) {
            //noinspection StatementWithEmptyBody
            while (walApplyJob.run(0)) ;
            if (new CheckWalTransactionsJob(engine).run(0)) {
                //noinspection StatementWithEmptyBody
                while (walApplyJob.run(0)) ;
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

    private static long queryRowCount(SqlCompiler compiler, SqlExecutionContext ctx, String tableName) throws Exception {
        try (var factory = compiler.compile("select count() from " + tableName, ctx).getRecordCursorFactory();
             var cursor = factory.getCursor(ctx)) {
            if (cursor.hasNext()) {
                return cursor.getRecord().getLong(0);
            }
        }
        return -1;
    }

    private static void runScenario(long rowsPerQuery, long o3LagMinutes) throws Exception {
        final Path dbRoot = Files.createTempDirectory("mvO3bench-");
        try {
            final CairoConfiguration configuration = new ScenarioConfig(dbRoot.toString(), rowsPerQuery);

            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = engine.getSqlCompiler()) {

                // Required to swap NoOpMatViewStateStore for the real store so mat view
                // refresh notifications are actually delivered.
                engine.load();

                final SqlExecutionContext sqlCtx = new SqlExecutionContextImpl(engine, 1).with(
                        configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null, null, -1, null
                );

                // Schema.
                engine.execute(
                        "create table ohlc_base (sym symbol, price double, ts timestamp) " +
                                "timestamp(ts) partition by DAY WAL",
                        sqlCtx
                );
                engine.execute(
                        "create materialized view ohlc_1m refresh immediate as " +
                                "select sym, ts, first(price) o, max(price) h, min(price) l, last(price) c " +
                                "from ohlc_base sample by 1m",
                        sqlCtx
                );

                // Seed: NUM_SYMBOLS symbols x SEED_MINUTES minute ticks starting at
                // SEED_EPOCH_MICROS. Iterations write past the end of the seed range.
                final long oneMinuteMicros = 60_000_000L;
                seedBaseTable(engine, sqlCtx, SEED_EPOCH_MICROS, oneMinuteMicros);
                drainWal(engine);
                drainMatView(engine);
                drainWal(engine);
                final long rowsAfterSeed = queryRowCount(compiler, sqlCtx, "ohlc_1m");

                // Warmup + measurement loop. Each iteration writes 1 current tick and
                // (optionally) 1 O3 tick at o3LagMinutes back, then drains WAL (so the
                // base table catches up) and times the mat view refresh drain.
                final long[] timings = new long[ITERATIONS];
                long iterTsMicros = SEED_EPOCH_MICROS + ((long) SEED_MINUTES) * oneMinuteMicros;
                for (int i = 0; i < WARMUP_ITERATIONS + ITERATIONS; i++) {
                    runIteration(engine, sqlCtx, iterTsMicros, o3LagMinutes, oneMinuteMicros, timings, i - WARMUP_ITERATIONS);
                    iterTsMicros += oneMinuteMicros;
                }

                final long rowsAfterIters = queryRowCount(compiler, sqlCtx, "ohlc_1m");

                Arrays.sort(timings);
                double avg = 0;
                for (long t : timings) avg += t;
                avg /= timings.length;
                final double median = percentile(timings, 50);
                final double p99 = percentile(timings, 99);
                final long min = timings[0];

                System.out.printf("%-18d %-14d %12.3f %12.3f %12.3f %12.3f %14d%n",
                        rowsPerQuery,
                        o3LagMinutes,
                        avg / 1_000_000.0,
                        median / 1_000_000.0,
                        p99 / 1_000_000.0,
                        min / 1_000_000.0,
                        rowsAfterIters - rowsAfterSeed
                );
            }
        } finally {
            deleteRecursively(dbRoot);
        }
    }

    private static void runIteration(
            CairoEngine engine,
            SqlExecutionContext sqlCtx,
            long iterTsMicros,
            long o3LagMinutes,
            long oneMinuteMicros,
            long[] timings,
            int recordIdx
    ) throws Exception {
        // Insert current tick. Each WAL txn = single row.
        final StringBuilder currentInsert = new StringBuilder("insert into ohlc_base(sym, price, ts) values ");
        appendInsertRow(currentInsert, iterTsMicros, 0, 100.0 + recordIdx);
        engine.execute(currentInsert.toString(), sqlCtx);

        if (o3LagMinutes > 0) {
            final long o3Ts = iterTsMicros - o3LagMinutes * oneMinuteMicros;
            final StringBuilder o3Insert = new StringBuilder("insert into ohlc_base(sym, price, ts) values ");
            appendInsertRow(o3Insert, o3Ts, 0, 50.0 + recordIdx);
            engine.execute(o3Insert.toString(), sqlCtx);
        }

        // Apply WAL to the base table (not timed -- the WAL apply also fires the
        // mat view notification which enqueues a refresh).
        drainWal(engine);

        // Time only the mat view refresh drain.
        final long t0 = System.nanoTime();
        drainMatView(engine);
        final long elapsed = System.nanoTime() - t0;
        // Apply the mat view's own WAL outputs so the next iteration sees them.
        drainWal(engine);
        if (recordIdx >= 0) {
            timings[recordIdx] = elapsed;
        }
    }

    private static void seedBaseTable(CairoEngine engine, SqlExecutionContext sqlCtx, long seedStartMicros, long oneMinuteMicros) throws Exception {
        // Bulk seed via INSERT AS SELECT with a long_sequence cross-join.
        // Layout: NUM_SYMBOLS rows per minute, SEED_MINUTES minutes total.
        final String sql = "insert into ohlc_base(sym, price, ts) " +
                "select " +
                "  'S' || (s.x - 1) as sym, " +
                "  100.0 + ((m.x - 1) % 100) * 0.1 as price, " +
                "  (" + seedStartMicros + " + (m.x - 1) * " + oneMinuteMicros + ")::timestamp as ts " +
                "from long_sequence(" + SEED_MINUTES + ") m " +
                "cross join long_sequence(" + NUM_SYMBOLS + ") s";
        engine.execute(sql, sqlCtx);
    }

    private static final class ScenarioConfig extends DefaultCairoConfiguration {
        private final long rowsPerQuery;

        ScenarioConfig(CharSequence root, long rowsPerQuery) {
            super(root);
            this.rowsPerQuery = rowsPerQuery;
        }

        @Override
        public long getMatViewRowsPerQueryEstimate() {
            return rowsPerQuery;
        }

        @Override
        public boolean isDevModeEnabled() {
            return true;
        }
    }
}
