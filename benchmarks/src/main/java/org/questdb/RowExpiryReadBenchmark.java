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
 * cost of one physical-cleanup sweep, relative to an unpolicied passthrough view.
 * <p>
 * Setup: a base table of {@code NUM_SYMBOLS} symbols x {@code DAYS} x {@code TICKS_PER_DAY} ticks (so each
 * symbol appears in every daily partition), mirrored into three passthrough views:
 * <ul>
 *   <li>{@code mv_none}   - no policy (baseline; reads scan all rows);</li>
 *   <li>{@code mv_latest} - KEEP LATEST PARTITION BY sym (rewrites to LATEST ON; sym is an indexed symbol,
 *       so the keep-set is the latest row per symbol);</li>
 *   <li>{@code mv_max}    - KEEP HIGHEST v PARTITION BY sym (a window keep-filter; full scan + per-key max).</li>
 * </ul>
 * It times two representative reads ({@code count(*)} over the whole keep-set, and a single-symbol lookup)
 * against each view, then one {@link RowExpiryCleanupJob} sweep over {@code mv_latest} (whose older
 * partitions are fully superseded and reclaimed in one pass).
 * <p>
 * Sample run (256 symbols x 8 days x 288 ticks/day = ~590k rows; laptop, numbers in ms, avg):
 * <pre>
 *   view       query              avg_ms
 *   mv_none    count()             0.07     baseline (full scan, no policy)
 *   mv_none    lookup sym='S5'     0.63
 *   mv_latest  count()             0.30     LATEST ON over an indexed symbol ~ O(#keys)
 *   mv_latest  lookup sym='S5'     0.26     index fast path
 *   mv_max     count()            22.7      window keep-filter: full scan + per-key max
 *   mv_max     lookup sym='S5'    21.0      window must scan all rows even for one symbol
 *   cleanup mv_latest: 8.3 ms, partitions 8 -> 1   (one-pass survivor scan + 7 wipes)
 * </pre>
 * Takeaways: KEEP LATEST on an indexed key is near-baseline; window modes (keep-max/min, top-N, window WHEN)
 * pay a full-view scan per read, so index the key, prefer KEEP LATEST where it fits, and keep CLEANUP EVERY
 * aggressive to keep the physical residue (and thus the read cost) small.
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

                engine.execute("create materialized view mv_none as (select * from base)", ctx);
                engine.execute("create materialized view mv_latest as (select * from base) expire rows keep latest partition by sym", ctx);
                engine.execute("create materialized view mv_max as (select * from base) expire rows keep highest v partition by sym", ctx);
                drainWal(engine);
                drainMatView(engine);
                drainWal(engine);

                System.out.printf("%-10s %-26s %12s %12s %12s %12s%n", "view", "query", "avg_ms", "median_ms", "p99_ms", "min_ms");
                for (String view : new String[]{"mv_none", "mv_latest", "mv_max"}) {
                    timeQuery(compiler, ctx, view, "count()", "select count() from " + view);
                    timeQuery(compiler, ctx, view, "lookup sym='S5'", "select sym, v, ts from " + view + " where sym = 'S5'");
                }

                // One cleanup sweep over mv_latest: DAYS-1 fully-superseded partitions reclaimed in one pass.
                System.out.println();
                final TableToken token = engine.verifyTableName("mv_latest");
                final String predicate;
                try (TableMetadata m = engine.getTableMetadata(token)) {
                    predicate = m.getExpiryPredicate();
                }
                final long before = rowCount(compiler, ctx, "select count() from table_partitions('mv_latest')");
                final long t0 = System.nanoTime();
                try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                    job.cleanupTable(token, predicate);
                }
                drainWal(engine);
                final double sweepMs = (System.nanoTime() - t0) / 1_000_000.0;
                final long after = rowCount(compiler, ctx, "select count() from table_partitions('mv_latest')");
                System.out.printf("cleanup mv_latest: %.3f ms, partitions %d -> %d%n", sweepMs, before, after);
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
