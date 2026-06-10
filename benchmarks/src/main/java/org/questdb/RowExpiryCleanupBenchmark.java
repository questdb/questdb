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
import io.questdb.std.LongList;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Measures the physical-cleanup (REPLACE_RANGE) churn of EXPIRE ROWS across modes. A sweep classifies each
 * non-active partition as WIPE (empty REPLACE_RANGE, no rewrite), REPLACE (rewrite the survivors) or SKIP;
 * the cost driver is the SURVIVORS REWRITTEN by REPLACE and whether a partition is re-REPLACED on later
 * sweeps (re-churn). Churn is derived by diffing {@code table_partitions(view)} (minTimestamp -> numRows)
 * before/after each sweep: a partition that disappears was WIPED; one whose numRows shrinks was REPLACED and
 * its surviving rows were rewritten.
 * <p>
 * Part 1 - one sweep over identical seeded data (30720 rows, 10 daily partitions, 64 symbols), threshold at
 * the mid-point of the time range. {@code rowsRewritten} (survivors copied by REPLACE) is the cost driver.
 * Sample run (laptop, ms):
 * <pre>
 *   mode                  wiped  replaced  rowsRewritten     ms   notes
 *   ts < T  (designated)      5         0              0   10.6   day-aligned threshold -> whole partitions wiped, no rewrite
 *   ts2 < T (2nd ts)          0         9          13712   21.1   no bounds alignment -> every partition partial -> rewrite ~half the table
 *   KEEP LATEST               9         0              0    4.0   recurring keys -> old partitions fully superseded -> wiped
 *   KEEP HIGHEST v            0         9             61   44.3   one max row per key survives per partition -> all partial, but rewrite ~#keys only
 * </pre>
 * Part 2 - re-churn over 8 sweeps as the threshold advances one day per epoch; cumulative:
 * <pre>
 *   mode               wiped  replaced  rowsRewritten   totalMs
 *   ts  (designated)       8         0              0      15.8   each partition wiped exactly once -> stays flat
 *   ts2 (2nd ts)          0        72         121111      86.0   each partition re-REPLACED every sweep -> rewrite grows linearly
 * </pre>
 * Takeaway: align expiry with the designated timestamp (or use KEEP LATEST on recurring keys) so cleanup
 * WIPES whole partitions in O(1) with zero rewrite; expiring on a second, uncorrelated timestamp forces a
 * REPLACE of every partition on every sweep, rewriting survivors repeatedly (~120k row-rewrites vs 0 here).
 */
public class RowExpiryCleanupBenchmark {

    private static final long DAY = 86_400_000_000L;
    private static final int DAYS = 10;
    private static final int EPOCHS = 8;
    private static final int NUM_SYMBOLS = 64;
    private static final long SEED_EPOCH = 1_704_067_200_000_000L; // 2024-01-01
    private static final int TICKS_PER_DAY = 48;

    public static void main(String[] args) throws Exception {
        final Path dbRoot = Files.createTempDirectory("rowExpiryCleanup-");
        try {
            final CairoConfiguration configuration = new BenchConfig(dbRoot.toString());
            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = engine.getSqlCompiler()) {
                engine.load();
                final SqlExecutionContext ctx = new SqlExecutionContextImpl(engine, 1).with(
                        configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);

                // ts = designated partition key (one tick per TICKS_PER_DAY); ts2 = a SECOND timestamp,
                // uncorrelated with ts/partition; v = value for keep-max.
                engine.execute("create table base (sym symbol index, v double, ts timestamp, ts2 timestamp) timestamp(ts) partition by DAY WAL", ctx);
                engine.execute("insert into base(sym, v, ts, ts2) select " +
                        // v: random per row, uncorrelated with ts, so each symbol's single max is scattered
                        // across partitions -> keep-max leaves ~1 survivor per key per partition (all partial).
                        "'S' || (s.x - 1), rnd_double() * 1000.0, " +
                        "(" + SEED_EPOCH + " + (m.x - 1) * " + (DAY / TICKS_PER_DAY) + ")::timestamp, " +
                        "(" + SEED_EPOCH + " + (rnd_long() % " + ((long) DAYS * TICKS_PER_DAY) + " + " + ((long) DAYS * TICKS_PER_DAY) + ") % " + ((long) DAYS * TICKS_PER_DAY) + " * " + (DAY / TICKS_PER_DAY) + ")::timestamp " +
                        "from long_sequence(" + ((long) DAYS * TICKS_PER_DAY) + ") m cross join long_sequence(" + NUM_SYMBOLS + ") s", ctx);
                drainWal(engine);
                final long baseRows = rowCount(compiler, ctx, "select count() from base");
                System.out.printf("RowExpiryCleanupBenchmark: %d rows, %d partitions, %d symbols%n%n", baseRows, DAYS, NUM_SYMBOLS);

                final long midThreshold = SEED_EPOCH + (DAYS / 2L) * DAY;

                System.out.println("Part 1 - one sweep over identical data (threshold = mid-range):");
                System.out.printf("%-26s %8s %9s %14s %10s %10s%n", "mode", "wiped", "replaced", "rowsRewritten", "rowsGone", "ms");
                onceSweep(engine, compiler, ctx, "v_ts", "expire rows when ts < cast(" + midThreshold + " as timestamp)");
                onceSweep(engine, compiler, ctx, "v_ts2", "expire rows when ts2 < cast(" + midThreshold + " as timestamp)");
                onceSweep(engine, compiler, ctx, "v_latest", "expire rows keep latest partition by sym");
                onceSweep(engine, compiler, ctx, "v_max", "expire rows keep highest v partition by sym");

                System.out.println();
                System.out.println("Part 2 - re-churn over " + EPOCHS + " sweeps, threshold advancing 1 day/epoch (cumulative):");
                System.out.printf("%-26s %8s %9s %14s %10s%n", "mode", "wiped", "replaced", "rowsRewritten", "totalMs");
                churn(engine, compiler, ctx, "c_ts", "ts");
                churn(engine, compiler, ctx, "c_ts2", "ts2");
            }
        } finally {
            deleteRecursively(dbRoot);
            LogFactory.haltInstance();
        }
    }

    private static void churn(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext ctx, String view, String col) throws Exception {
        engine.execute("create materialized view " + view + " as (select * from base)", ctx);
        drainWal(engine);
        drainMatView(engine);
        drainWal(engine);
        final TableToken token = engine.verifyTableName(view);
        int wiped = 0, replaced = 0;
        long rewritten = 0;
        double ms = 0;
        for (int e = 1; e <= EPOCHS; e++) {
            final long threshold = SEED_EPOCH + (long) e * DAY;
            engine.execute("alter materialized view " + view + " set expire rows when " + col + " < cast(" + threshold + " as timestamp)", ctx);
            drainWal(engine);
            final long[] r = sweepAndMeasure(engine, compiler, ctx, view, token);
            wiped += r[0];
            replaced += r[1];
            rewritten += r[2];
            ms += r[3] / 1_000_000.0;
        }
        System.out.printf("%-26s %8d %9d %14d %10.3f%n", view + " (" + col + ")", wiped, replaced, rewritten, ms);
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

    private static void onceSweep(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext ctx, String view, String expireClause) throws Exception {
        engine.execute("create materialized view " + view + " as (select * from base) " + expireClause, ctx);
        drainWal(engine);
        drainMatView(engine);
        drainWal(engine);
        final TableToken token = engine.verifyTableName(view);
        final long visibleBefore = rowCount(compiler, ctx, "select count() from " + view);
        final long[] r = sweepAndMeasure(engine, compiler, ctx, view, token);
        final long visibleAfter = rowCount(compiler, ctx, "select count() from " + view);
        final String check = visibleBefore == visibleAfter ? "ok" : "MISMATCH " + visibleBefore + "->" + visibleAfter;
        System.out.printf("%-26s %8d %9d %14d %10d %10.3f   visible=%d %s%n", view, r[0], r[1], r[2], r[4], r[3] / 1_000_000.0, visibleAfter, check);
    }

    private static int indexOf(LongList list, long value) {
        for (int i = 0, n = list.size(); i < n; i++) {
            if (list.getQuick(i) == value) {
                return i;
            }
        }
        return -1;
    }

    private static long rowCount(SqlCompiler compiler, SqlExecutionContext ctx, String sql) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(ctx)) {
            return cursor.hasNext() ? cursor.getRecord().getLong(0) : -1;
        }
    }

    // Snapshot as parallel lists [partitionFloor, numRows] per partition (ascending). The partition is keyed
    // by its DAY floor, NOT raw minTimestamp: a REPLACE that drops a partition's earliest rows shifts its
    // minTimestamp, so keying by minTimestamp would misread that REPLACE as a WIPE.
    private static void snapshot(SqlCompiler compiler, SqlExecutionContext ctx, String view, LongList ts, LongList rows) throws Exception {
        ts.clear();
        rows.clear();
        try (RecordCursorFactory factory = compiler.compile("select minTimestamp, numRows from table_partitions('" + view + "') order by minTimestamp", ctx).getRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(ctx)) {
            final var record = cursor.getRecord();
            while (cursor.hasNext()) {
                final long minTs = record.getTimestamp(0);
                ts.add(minTs - Math.floorMod(minTs, DAY));
                rows.add(record.getLong(1));
            }
        }
    }

    // Returns [wipedPartitions, replacedPartitions, rowsRewritten, sweepNanos, rowsRemoved].
    private static long[] sweepAndMeasure(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext ctx, String view, TableToken token) throws Exception {
        final String predicate;
        try (TableMetadata m = engine.getTableMetadata(token)) {
            predicate = m.getExpiryPredicate();
        }
        final LongList beforeTs = new LongList();
        final LongList beforeRows = new LongList();
        final LongList afterTs = new LongList();
        final LongList afterRows = new LongList();
        snapshot(compiler, ctx, view, beforeTs, beforeRows);
        final long t0 = System.nanoTime();
        try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
            job.cleanupTable(token, predicate, 0);
        }
        drainWal(engine);
        final long nanos = System.nanoTime() - t0;
        snapshot(compiler, ctx, view, afterTs, afterRows);

        long wiped = 0, replaced = 0, rewritten = 0, removed = 0;
        for (int i = 0, n = beforeTs.size(); i < n; i++) {
            final long pTs = beforeTs.getQuick(i);
            final long beforeN = beforeRows.getQuick(i);
            final int j = indexOf(afterTs, pTs);
            if (j < 0) {
                wiped++;
                removed += beforeN;
            } else {
                final long afterN = afterRows.getQuick(j);
                if (afterN < beforeN) {
                    replaced++;
                    rewritten += afterN; // the survivors that were rewritten into the new partition version
                    removed += beforeN - afterN;
                }
            }
        }
        return new long[]{wiped, replaced, rewritten, nanos, removed};
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
