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
import io.questdb.griffin.TextPlanSink;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;

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
 * symbol appears in every daily partition), mirrored into five passthrough views:
 * <ul>
 *   <li>{@code mv_none}   - no policy (baseline);</li>
 *   <li>{@code mv_ts}     - WHEN ts &lt; T on the designated timestamp (the only mode that frees disk);</li>
 *   <li>{@code mv_val}    - WHEN v &lt; 500.0 on a value column;</li>
 *   <li>{@code mv_latest} - KEEP LATEST PARTITION BY sym (rewrites to LATEST ON; sym is an indexed symbol,
 *       so the keep-set is the latest row per symbol);</li>
 *   <li>{@code mv_max}    - KEEP HIGHEST v PARTITION BY sym (a window keep-filter; full scan + per-key max).</li>
 * </ul>
 * Four reads run against each view - a whole-view {@code count()}, a single-symbol lookup, a JIT-compilable
 * value filter, and a one-day range - and each row reports the plan features the policy left standing. Then
 * one {@link RowExpiryCleanupJob} sweep runs over {@code mv_ts} and one over {@code mv_latest}.
 * <p>
 * Sample run (256 symbols x 8 days x 288 ticks/day = ~590k rows; one laptop, avg ms):
 * <pre>
 *   view       query             avg_ms   plan
 *   mv_none    count()             0.05   Count                                        row count from the txn file
 *   mv_none    lookup sym='S5'     0.25   -                                            symbol index
 *   mv_none    filter v&gt;900        1.44   Async JIT Filter + Count
 *   mv_none    one day             0.06   Interval forward scan + Count
 *   mv_ts      count()             0.05   Interval forward scan + Count                keep-filter flips to ts &gt;= T
 *   mv_ts      lookup sym='S5'     0.08   Interval forward scan
 *   mv_ts      filter v&gt;900        0.73   Async JIT Filter + Interval forward scan     JIT survives
 *   mv_ts      one day             0.02   Interval forward scan + Count
 *   mv_val     count()             7.06   Count + case(                                ~130x: no txn-file count
 *   mv_val     lookup sym='S5'     0.13   case(
 *   mv_val     filter v&gt;900        8.84   Count + case(                                ~6x: the CASE takes the JIT away
 *   mv_val     one day             1.26   Interval forward scan + Count + case(
 *   mv_latest  count()             0.12   Count                                        LATEST ON ~ O(#keys)
 *   mv_latest  lookup sym='S5'     0.08   -
 *   mv_latest  filter v&gt;900        0.09   Count
 *   mv_latest  one day             0.09   Count
 *   mv_max     count()            48.05   Count + CachedWindowLight + case(            ~900x: window over the whole view
 *   mv_max     lookup sym='S5'     0.44   CachedWindowLight + case(                    filter on the PARTITION BY key
 *   mv_max     filter v&gt;900       47.43   Count + CachedWindowLight + case(
 *   mv_max     one day            48.14   Count + CachedWindowLight + case(            the day restriction saves nothing
 *   cleanup mv_ts:     15.4 ms, partitions 8 -> 4
 *   cleanup mv_latest:  0.2 ms, partitions 8 -> 8   (structural policy: the sweep frees nothing)
 * </pre>
 * Takeaways. A WHEN predicate on the designated timestamp is the mode to reach for: its keep-filter flips to
 * a bare {@code ts >= T}, which prunes partitions and leaves the caller's own filter JIT-compilable, and it
 * is the only mode here that frees disk. The threshold has to be provably non-null for that flip - a
 * timestamp literal or a clock call, not a {@code cast(...)}, which the parser treats as possibly-null and
 * serves through the CASE form instead.
 * <p>
 * A value predicate always reads through that CASE form. The optimiser merges it with the caller's WHERE,
 * so the caller's filter loses JIT compilation too, and {@code count()} can no longer take the row count
 * from the transaction file.
 * <p>
 * A window keep-filter (KEEP HIGHEST/LOWEST, top-N, a WHEN with a window function) computes the window over
 * the whole view on every read, which is why the one-day query costs as much as the whole-view count. Each
 * such cursor also holds a row chain sized by the view; it is charged to the per-query memory budget
 * ({@code CachedWindowLightRecordCursorFactory} binds the execution context's memory tracker), so an
 * oversized read fails with a memory-limit error rather than taking the server down. KEEP LATEST is the
 * exception: it rewrites to LATEST ON and reads near baseline over an indexed symbol.
 * <p>
 * KEEP LATEST reads cheaply but reclaims nothing: its sweep returns at
 * {@code RowExpiryUtil.isStructuralPolicy} before it opens a reader, so a shorter CLEANUP EVERY changes
 * nothing for it and the expired rows stay on disk, hidden by the read filter, until a full refresh.
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

                // A timestamp LITERAL, not cast(<micros> as timestamp): only a provably non-null threshold
                // lets the parser flip NOT(ts < T) to the bare ts >= T that prunes partitions, and a CAST
                // counts as possibly-null.
                final String midThreshold = "'2024-01-05T00:00:00.000000Z'";
                engine.execute("create materialized view mv_none as (select * from base)", ctx);
                engine.execute("create materialized view mv_ts as (select * from base) expire rows when ts < " + midThreshold, ctx);
                engine.execute("create materialized view mv_val as (select * from base) expire rows when v < 500.0", ctx);
                engine.execute("create materialized view mv_latest as (select * from base) expire rows keep latest partition by sym", ctx);
                engine.execute("create materialized view mv_max as (select * from base) expire rows keep highest v partition by sym", ctx);
                drainWal(engine);
                drainMatView(engine);
                drainWal(engine);

                // One day out of DAYS, above mv_ts's threshold so the rows of that day survive every policy.
                final String oneDay = "'2024-01-06T00:00:00.000000Z'";
                final String nextDay = "'2024-01-07T00:00:00.000000Z'";
                System.out.printf("%-10s %-26s %12s %12s %12s %12s  %s%n",
                        "view", "query", "avg_ms", "median_ms", "p99_ms", "min_ms", "plan");
                for (String view : new String[]{"mv_none", "mv_ts", "mv_val", "mv_latest", "mv_max"}) {
                    timeQuery(compiler, ctx, view, "count()", "select count() from " + view);
                    timeQuery(compiler, ctx, view, "lookup sym='S5'", "select sym, v, ts from " + view + " where sym = 'S5'");
                    // A caller filter that is JIT-compilable on its own: a value policy's CASE keep-filter
                    // merges into it and takes the JIT compilation away.
                    timeQuery(compiler, ctx, view, "filter v>900", "select count() from " + view + " where v > 900.0");
                    // One day out of DAYS: a timestamp keep-filter still prunes, a window one reads the
                    // whole view regardless.
                    timeQuery(compiler, ctx, view, "one day", "select count() from " + view
                            + " where ts >= " + oneDay + " and ts < " + nextDay);
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

    // The parts of the execution plan a policy can take away: JIT compilation of the caller's filter, the
    // interval scan that prunes partitions, and the transaction-file row count. The window keep-filter shows
    // up as CachedWindowLight, which reads the whole view on every cursor.
    private static String planTag(RecordCursorFactory factory, SqlExecutionContext ctx) {
        final TextPlanSink planSink = new TextPlanSink();
        planSink.of(factory, ctx);
        final CharSequence sink = planSink.getSink();
        final StringBuilder tags = new StringBuilder();
        for (String marker : new String[]{"Async JIT Filter", "Interval forward scan", "Count", "CachedWindowLight", "case("}) {
            if (Chars.contains(sink, marker)) {
                tags.append(tags.length() == 0 ? "" : " + ").append(marker);
            }
        }
        return tags.length() == 0 ? "-" : tags.toString();
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
        final String plan;
        try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
            plan = planTag(factory, ctx);
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
        System.out.printf("%-10s %-26s %12.3f %12.3f %12.3f %12.3f  %s%n",
                view, label, avg / 1_000_000.0, percentile(timings, 50) / 1_000_000.0,
                percentile(timings, 99) / 1_000_000.0, timings[0] / 1_000_000.0, plan);
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
