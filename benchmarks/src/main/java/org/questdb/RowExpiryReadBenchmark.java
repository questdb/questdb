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
import io.questdb.cairo.RowExpiryCleanupJob;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.mv.MatViewRefreshJob;
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
 * Measures the read-time cost of the EXPIRE ROWS read filter on passthrough materialized views, and the
 * cost of one physical-cleanup sweep, relative to an unpolicied passthrough view. A standalone harness with
 * a {@code main()}, not a JMH benchmark; run it with
 * {@code java -cp benchmarks/target/benchmarks.jar org.questdb.RowExpiryReadBenchmark}.
 * <p>
 * Setup: a base table of {@code NUM_SYMBOLS} symbols x {@code DAYS} x {@code TICKS_PER_DAY} ticks (so each
 * symbol appears in every daily partition), mirrored into four passthrough views:
 * <ul>
 *   <li>{@code mv_none}   - no policy (baseline; reads scan all rows);</li>
 *   <li>{@code mv_ts}     - WHEN ts &lt; T on the designated timestamp (the only mode that frees disk);</li>
 *   <li>{@code mv_latest} - KEEP LATEST PARTITION BY sym (rewrites to LATEST ON; sym is an indexed symbol,
 *       so the keep-set is the latest row per symbol);</li>
 *   <li>{@code mv_max}    - KEEP HIGHEST v PARTITION BY sym (a window keep-filter; full scan + per-key max).</li>
 * </ul>
 * It times two representative reads ({@code count(*)} over the whole keep-set, and a single-symbol lookup)
 * against each view, then runs one {@link RowExpiryCleanupJob} sweep over {@code mv_ts} and one over
 * {@code mv_latest} and reports the partition count each sweep leaves behind.
 * <p>
 * Sample run (256 symbols x 8 days x 288 ticks/day = ~590k rows; laptop, numbers in ms, avg):
 * <pre>
 *   view       query              avg_ms
 *   mv_none    count()             0.05     no policy, so the count comes straight from the transaction file
 *   mv_none    lookup sym='S5'     0.23     symbol index over all 8 partitions
 *   mv_ts      count()             1.64     the keep-filter is a plain comparison, but count() now scans the kept rows
 *   mv_ts      lookup sym='S5'     0.11     the flipped comparison prunes the partitions below the threshold
 *   mv_latest  count()             0.13     LATEST ON over an indexed symbol ~ O(#keys)
 *   mv_latest  lookup sym='S5'     0.10     index fast path
 *   mv_max     count()            43.8      window keep-filter: full scan + per-key max
 *   mv_max     lookup sym='S5'     0.43     the filter on the window's PARTITION BY key is pushed below it
 *   cleanup mv_ts:      5.4 ms, partitions 8 -> 4
 *   cleanup mv_latest:  0.2 ms, partitions 8 -> 8   (structural policy: the sweep frees nothing)
 * </pre>
 * Takeaways: a WHEN predicate on the designated timestamp is the only mode here that frees disk, and the one
 * whose keep-filter still prunes partitions - but any policy costs a {@code count()} its transaction-file
 * fast path. KEEP LATEST on an indexed key reads near baseline and reclaims nothing: its sweep returns at
 * {@code RowExpiryUtil.isStructuralPolicy} before it opens a reader, so a shorter CLEANUP EVERY changes
 * nothing for it and the expired rows stay on disk (hidden by the read filter) until a full refresh. A
 * {@code count()} over a window mode pays a full-view scan on every read; a filter on the policy's
 * PARTITION BY key is pushed below the window and stays cheap.
 */
public class RowExpiryReadBenchmark {

    private static final int DAYS = 8;
    private static final int ITERATIONS = 30;
    private static final int NUM_SYMBOLS = 256;
    private static final long SEED_EPOCH_MICROS = 1_704_067_200_000_000L; // 2024-01-01T00:00:00Z
    private static final int TICKS_PER_DAY = 288; // every 5 minutes
    private static final int WARMUP_ITERATIONS = 5;

    public static void main(String[] args) throws Exception {
        final long totalRows = (long) NUM_SYMBOLS * DAYS * TICKS_PER_DAY;
        System.out.printf("RowExpiryReadBenchmark: %d symbols x %d days x %d ticks/day = %d base rows%n%n",
                NUM_SYMBOLS, DAYS, TICKS_PER_DAY, totalRows);

        final Path dbRoot = Files.createTempDirectory("rowExpiryBench-");
        try {
            final CairoConfiguration configuration = new BenchConfig(dbRoot.toString());
            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = engine.getSqlCompiler()) {
                engine.load();
                final SqlExecutionContext ctx = new SqlExecutionContextImpl(engine, 1).with(
                        configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);

                engine.execute("create table base (sym symbol index, v double, ts timestamp) timestamp(ts) partition by DAY WAL", ctx);
                final String seed = "insert into base(sym, v, ts) select " +
                        "'S' || (s.x - 1), " +
                        "rnd_double() * 1000.0, " +
                        "(" + SEED_EPOCH_MICROS + " + (m.x - 1) * " + (86_400_000_000L / TICKS_PER_DAY) + ")::timestamp " +
                        "from long_sequence(" + ((long) DAYS * TICKS_PER_DAY) + ") m cross join long_sequence(" + NUM_SYMBOLS + ") s";
                engine.execute(seed, ctx);
                drainWal(engine);

                final long midThresholdMicros = SEED_EPOCH_MICROS + (DAYS / 2L) * 86_400_000_000L;
                engine.execute("create materialized view mv_none as (select * from base)", ctx);
                engine.execute("create materialized view mv_ts as (select * from base) expire rows when ts < cast(" + midThresholdMicros + " as timestamp)", ctx);
                engine.execute("create materialized view mv_latest as (select * from base) expire rows keep latest partition by sym", ctx);
                engine.execute("create materialized view mv_max as (select * from base) expire rows keep highest v partition by sym", ctx);
                drainWal(engine);
                drainMatView(engine);
                drainWal(engine);

                System.out.printf("%-10s %-26s %12s %12s %12s %12s%n", "view", "query", "avg_ms", "median_ms", "p99_ms", "min_ms");
                for (String view : new String[]{"mv_none", "mv_ts", "mv_latest", "mv_max"}) {
                    timeQuery(compiler, ctx, view, "count()", "select count() from " + view);
                    timeQuery(compiler, ctx, view, "lookup sym='S5'", "select sym, v, ts from " + view + " where sym = 'S5'");
                }

                // One cleanup sweep per mode: mv_ts reclaims the partitions below its threshold, while the
                // structural mv_latest sweep returns at RowExpiryUtil.isStructuralPolicy and frees nothing.
                System.out.println();
                sweep(engine, compiler, ctx, "mv_ts");
                sweep(engine, compiler, ctx, "mv_latest");
            }
        } finally {
            deleteRecursively(dbRoot);
            LogFactory.haltInstance();
        }
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
            while (refreshJob.run()) ;
        }
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

    private static long rowCount(SqlCompiler compiler, SqlExecutionContext ctx, String sql) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(ctx)) {
            return cursor.hasNext() ? cursor.getRecord().getLong(0) : -1;
        }
    }

    private static void sweep(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext ctx, String view) throws Exception {
        final TableToken token = engine.verifyTableName(view);
        final String predicate;
        try (TableMetadata m = engine.getTableMetadata(token)) {
            predicate = m.getExpiryPredicate();
        }
        final long before = rowCount(compiler, ctx, "select count() from table_partitions('" + view + "')");
        final long t0 = System.nanoTime();
        final boolean reclaimed;
        try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
            reclaimed = job.cleanupTable(token, predicate);
        }
        drainWal(engine);
        final double sweepMs = (System.nanoTime() - t0) / 1_000_000.0;
        final long after = rowCount(compiler, ctx, "select count() from table_partitions('" + view + "')");
        System.out.printf("cleanup %-10s %8.3f ms, partitions %d -> %d%s%n",
                view + ':', sweepMs, before, after, reclaimed ? "" : "   (freed nothing)");
    }

    private static void timeQuery(SqlCompiler compiler, SqlExecutionContext ctx, String view, String label, String sql) throws Exception {
        final long[] timings = new long[ITERATIONS];
        try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
            for (int i = 0; i < WARMUP_ITERATIONS + ITERATIONS; i++) {
                final long t0 = System.nanoTime();
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) ;
                }
                final long elapsed = System.nanoTime() - t0;
                if (i >= WARMUP_ITERATIONS) {
                    timings[i - WARMUP_ITERATIONS] = elapsed;
                }
            }
        }
        Arrays.sort(timings);
        double avg = 0;
        for (long t : timings) {
            avg += t;
        }
        avg /= timings.length;
        System.out.printf("%-10s %-26s %12.3f %12.3f %12.3f %12.3f%n",
                view, label, avg / 1_000_000.0, percentile(timings, 50) / 1_000_000.0,
                percentile(timings, 99) / 1_000_000.0, timings[0] / 1_000_000.0);
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
