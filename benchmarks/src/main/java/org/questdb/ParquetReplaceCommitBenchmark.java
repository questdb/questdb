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
import io.questdb.cairo.O3ParquetCounters;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8String;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

/**
 * Times the WAL apply of a replace-range commit ({@code WAL_DEDUP_MODE_REPLACE_RANGE}), as
 * materialized-view refresh issues, on a Parquet partition and on its native twin.
 * <p>
 * Each run creates a WAL table of {@link #DAYS} daily partitions of {@link #ROWS_PER_DAY} rows in
 * row groups of {@link #ROW_GROUP_SIZE}. The {@code HISTORICAL} placement converts every day but
 * the last to Parquet; {@code LAST} creates the table {@code FORMAT PARQUET}. Each iteration commits
 * the same window to the target day: a replace brings new values at the timestamps it covers, so
 * the partition keeps its shape; a delete brings no rows, and an untimed commit restores them.
 * <p>
 * The Parquet path updates in place until projected dead bytes cross the rewrite thresholds (ratio
 * 0.5, 1 GiB), so averages include amortized rewrites. {@code avg_x} and {@code p50_x} give the
 * Parquet time as a multiple of the native one. {@code in_place}, {@code rewrite}, {@code drop_rg}
 * and {@code noop} count the paths taken, from {@link O3ParquetCounters}, and {@code day_mb} shows
 * the target day's size before and after the run. {@code workers > 0} adds a shared write pool, in
 * which native O3 copies columns in parallel while Parquet O3 runs a partition in one task.
 * <p>
 * Manual timing (not JMH). Run with:
 * <pre>
 * java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
 *      -cp benchmarks/target/benchmarks.jar org.questdb.ParquetReplaceCommitBenchmark [scenario,...]
 * </pre>
 * The optional argument is a comma-separated list of scenario names to run. Logging goes to stdout
 * at ERROR level unless {@code -Dout=<log.conf>} is set.
 */
public class ParquetReplaceCommitBenchmark {
    private static final int COL_AMOUNT = 3;
    private static final int COL_ID = 4;
    private static final int COL_NOTE = 5;
    private static final int COL_PRICE = 2;
    private static final int COL_SIDE = 1;
    private static final int COL_SYM = 0;
    private static final int DAYS = 3;
    private static final long DAY_MICROS = 86_400_000_000L;
    private static final int ITERATIONS = 20;
    private static final ObjList<Utf8String> NOTES = new ObjList<>();
    private static final int ROWS_PER_DAY = 1_000_000;
    private static final int ROW_GROUP_SIZE = 100_000;
    private static final long START_MICROS = 1_704_067_200_000_000L; // 2024-01-01T00:00:00Z
    private static final long STEP_MICROS = DAY_MICROS / ROWS_PER_DAY;
    private static final ObjList<String> SYMBOLS = new ObjList<>();
    private static final int WARMUP_ITERATIONS = 3;
    private static final int[] WORKER_COUNT_SWEEP = {0, 4};
    private static boolean isHeaderPrinted;

    public static void main(String[] args) throws Exception {
        configureQuietLogging();
        final String filter = args.length > 0 ? "," + args[0] + "," : null;

        final Rnd rnd = new Rnd(7, 77);
        for (int i = 0; i < 100; i++) {
            SYMBOLS.add("S" + i);
        }
        for (int i = 0; i < 1024; i++) {
            final StringBuilder sb = new StringBuilder();
            for (int j = 0, n = 5 + rnd.nextInt(12); j < n; j++) {
                sb.append((char) ('a' + rnd.nextInt(26)));
            }
            NOTES.add(new Utf8String(sb.toString()));
        }

        for (Scenario scenario : Scenario.values()) {
            if (filter != null && !filter.contains("," + scenario.label + ",")) {
                continue;
            }
            for (int workers : WORKER_COUNT_SWEEP) {
                final Result nat = runScenario(scenario, workers, false);
                print(nat, null);
                final Result pq = runScenario(scenario, workers, true);
                print(pq, nat);
            }
        }

        LogFactory.haltInstance();
    }

    /**
     * Keeps the per-commit INFO logs of the O3 path out of the result table. A user-supplied
     * {@code -Dout} log configuration wins.
     */
    private static void configureQuietLogging() throws IOException {
        if (System.getProperty(LogFactory.CONFIG_SYSTEM_PROPERTY) != null) {
            return;
        }
        final Path conf = Files.createTempFile("pqReplaceBenchLog-", ".conf");
        conf.toFile().deleteOnExit();
        Files.writeString(conf, """
                writers=stdout
                w.stdout.class=io.questdb.log.LogConsoleWriter
                w.stdout.level=ERROR
                """);
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, conf.toString());
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

    private static void drainWal(ApplyWal2TableJob applyJob, CheckWalTransactionsJob checkJob) {
        //noinspection StatementWithEmptyBody
        while (applyJob.run()) ;
        if (checkJob.run()) {
            //noinspection StatementWithEmptyBody
            while (applyJob.run()) ;
        }
    }

    private static String formatRatio(double value, double base) {
        return base > 0 ? String.format("%.2fx", value / base) : "-";
    }

    private static int parquetRowGroupCount(CairoEngine engine, TableToken token, int partitionIndex) {
        try (TableReader reader = engine.getReader(token)) {
            reader.openPartition(partitionIndex);
            return reader.getAndInitParquetPartitionDecoder(partitionIndex).metadata().getRowGroupCount();
        }
    }

    private static double percentile(long[] sorted, double pct) {
        int idx = (int) Math.ceil(pct * sorted.length / 100.0) - 1;
        idx = Math.max(0, Math.min(idx, sorted.length - 1));
        return sorted[idx];
    }

    private static void print(Result r, Result nat) {
        // printed late, so that the engine's startup log lines come before the table
        if (!isHeaderPrinted) {
            isHeaderPrinted = true;
            System.out.printf(
                    "%nParquetReplaceCommitBenchmark: %d daily partitions of %,d rows, %,d-row row groups, %d iterations (+%d warmup)%n%n",
                    DAYS, ROWS_PER_DAY, ROW_GROUP_SIZE, ITERATIONS, WARMUP_ITERATIONS
            );
            System.out.printf(
                    "%-14s %-8s %-8s %10s %10s %10s %10s %9s %9s %16s %9s %8s %8s %6s%n",
                    "scenario", "workers", "format", "avg_ms", "p50_ms", "p90_ms", "max_ms", "avg_x", "p50_x",
                    "day_mb", "in_place", "rewrite", "drop_rg", "noop"
            );
        }
        final String dayMb = String.format("%.1f->%.1f", r.dayBytesStart / 1_048_576.0, r.dayBytesEnd / 1_048_576.0);
        System.out.printf(
                "%-14s %-8d %-8s %10.3f %10.3f %10.3f %10.3f %9s %9s %16s %9s %8s %8s %6s%n",
                r.scenario.label,
                r.workers,
                r.isParquet ? "parquet" : "native",
                r.avgNs / 1_000_000.0,
                r.p50Ns / 1_000_000.0,
                r.p90Ns / 1_000_000.0,
                r.maxNs / 1_000_000.0,
                nat != null ? formatRatio(r.avgNs, nat.avgNs) : "-",
                nat != null ? formatRatio(r.p50Ns, nat.p50Ns) : "-",
                dayMb,
                r.isParquet ? Long.toString(r.inPlace) : "-",
                r.isParquet ? Long.toString(r.rewrite) : "-",
                r.isParquet ? Long.toString(r.dropRowGroup) : "-",
                r.isParquet ? Long.toString(r.noop) : "-"
        );
    }

    private static long queryLong(CairoEngine engine, SqlExecutionContext ctx, String sql) throws SqlException {
        try (
                RecordCursorFactory factory = engine.select(sql, ctx);
                RecordCursor cursor = factory.getCursor(ctx)
        ) {
            return cursor.hasNext() ? cursor.getRecord().getLong(0) : -1;
        }
    }

    private static Result runIterations(
            CairoEngine engine,
            SqlExecutionContext ctx,
            ApplyWal2TableJob applyJob,
            CheckWalTransactionsJob checkJob,
            TableToken token,
            Scenario scenario,
            int workers,
            boolean isParquet
    ) throws Exception {
        final int targetDay = scenario.placement.dayIndex;
        final long dayStart = START_MICROS + targetDay * DAY_MICROS;
        final String dayName = String.format("2024-01-%02d", targetDay + 1);
        final String daySizeSql = "SELECT sum(diskSize) FROM table_partitions('t') WHERE name LIKE '" + dayName + "%'";
        if (isParquet) {
            final int rowGroups = parquetRowGroupCount(engine, token, targetDay);
            if (rowGroups != ROWS_PER_DAY / ROW_GROUP_SIZE) {
                throw new IllegalStateException("unexpected row group count [scenario=" + scenario.label
                        + ", rowGroups=" + rowGroups + ']');
            }
        }

        // the window's inclusive bounds; a window over no row falls strictly between two rows
        final long lo;
        final long hi;
        if (scenario.windowRows > 0) {
            lo = dayStart + scenario.firstRow * STEP_MICROS;
            hi = dayStart + (scenario.firstRow + scenario.windowRows - 1) * STEP_MICROS;
        } else {
            lo = dayStart + scenario.firstRow * STEP_MICROS + 1;
            hi = lo + STEP_MICROS - 2;
        }
        final int incomingRows = scenario.isDelete ? 0 : scenario.windowRows;

        final long dayBytesStart = queryLong(engine, ctx, daySizeSql);
        final Rnd rnd = new Rnd(123, 456);
        final long[] timings = new long[ITERATIONS];
        long inPlace = 0;
        long rewrite = 0;
        long dropRowGroup = 0;
        long noop = 0;
        for (int i = 0; i < WARMUP_ITERATIONS + ITERATIONS; i++) {
            writeReplaceCommit(engine, token, rnd, dayStart, scenario.firstRow, incomingRows, lo, hi);

            final long inPlaceBefore = O3ParquetCounters.getInPlaceCount();
            final long rewriteBefore = O3ParquetCounters.getRewriteCount();
            final long dropBefore = O3ParquetCounters.getDropInPlaceCount() + O3ParquetCounters.getDropRewriteCount();
            final long noopBefore = O3ParquetCounters.getReplaceNoopCount();
            final long t0 = System.nanoTime();
            drainWal(applyJob, checkJob);
            final long elapsed = System.nanoTime() - t0;
            if (i >= WARMUP_ITERATIONS) {
                timings[i - WARMUP_ITERATIONS] = elapsed;
                inPlace += O3ParquetCounters.getInPlaceCount() - inPlaceBefore;
                rewrite += O3ParquetCounters.getRewriteCount() - rewriteBefore;
                dropRowGroup += O3ParquetCounters.getDropInPlaceCount() + O3ParquetCounters.getDropRewriteCount() - dropBefore;
                noop += O3ParquetCounters.getReplaceNoopCount() - noopBefore;
            }

            if (scenario.isDelete) {
                // untimed: bring the deleted rows back, so the next delete finds them
                writeReplaceCommit(engine, token, rnd, dayStart, scenario.firstRow, scenario.windowRows, lo, hi);
                drainWal(applyJob, checkJob);
            }
        }

        // a commit that suspends the table or loses rows would otherwise report as fast
        if (engine.getTableSequencerAPI().isSuspended(token)) {
            throw new IllegalStateException("table suspended [scenario=" + scenario.label + ']');
        }
        final long rowCount = queryLong(engine, ctx, "SELECT count() FROM t");
        if (rowCount != (long) DAYS * ROWS_PER_DAY) {
            throw new IllegalStateException("unexpected row count [scenario=" + scenario.label + ", rows=" + rowCount + ']');
        }
        final long parquetParts = queryLong(
                engine,
                ctx,
                "SELECT count() FROM table_partitions('t') WHERE isParquet AND name LIKE '" + dayName + "%'"
        );
        if ((parquetParts > 0) != isParquet) {
            throw new IllegalStateException("unexpected target partition format [scenario=" + scenario.label
                    + ", parquetParts=" + parquetParts + ']');
        }
        final long dayBytesEnd = queryLong(engine, ctx, daySizeSql);

        Arrays.sort(timings);
        double sum = 0;
        for (long t : timings) {
            sum += t;
        }
        return new Result(
                scenario,
                workers,
                isParquet,
                sum / timings.length,
                percentile(timings, 50),
                percentile(timings, 90),
                timings[timings.length - 1],
                dayBytesStart,
                dayBytesEnd,
                inPlace,
                rewrite,
                dropRowGroup,
                noop
        );
    }

    private static Result runScenario(Scenario scenario, int workers, boolean isParquet) throws Exception {
        final Path dbRoot = Files.createTempDirectory("pqReplaceBench-");
        try {
            final CairoConfiguration configuration = new BenchConfig(dbRoot.toString());
            try (CairoEngine engine = new CairoEngine(configuration)) {
                engine.load();
                final WorkerPool pool = workers > 0 ? startWriterPool(engine, workers) : null;
                try (
                        ApplyWal2TableJob applyJob = new ApplyWal2TableJob(engine, 0);
                        CheckWalTransactionsJob checkJob = new CheckWalTransactionsJob(engine)
                ) {
                    final SqlExecutionContext ctx = new SqlExecutionContextImpl(engine, 1).with(
                            configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null,
                            new Rnd(42, 4242)
                    );
                    final TableToken token = seedTable(engine, ctx, applyJob, checkJob, scenario.placement, isParquet);
                    return runIterations(engine, ctx, applyJob, checkJob, token, scenario, workers, isParquet);
                } finally {
                    Misc.free(pool);
                }
            }
        } finally {
            deleteRecursively(dbRoot);
        }
    }

    private static TableToken seedTable(
            CairoEngine engine,
            SqlExecutionContext ctx,
            ApplyWal2TableJob applyJob,
            CheckWalTransactionsJob checkJob,
            Placement placement,
            boolean isParquet
    ) throws SqlException {
        final boolean isFormatParquet = isParquet && placement == Placement.LAST;
        engine.execute(
                "CREATE TABLE t (sym SYMBOL CAPACITY 256, side SYMBOL CAPACITY 4, price DOUBLE, amount DOUBLE, "
                        + "id LONG, note VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY"
                        + (isFormatParquet ? " FORMAT PARQUET" : "") + " WAL",
                ctx
        );
        // the fixed seed of ctx's Rnd gives the native and Parquet runs the same rows
        engine.execute(
                "INSERT INTO t SELECT rnd_symbol(100, 4, 4, 0), rnd_symbol('buy', 'sell'), rnd_double() * 100, "
                        + "rnd_double() * 10, rnd_long(), rnd_varchar(5, 16, 0), "
                        + "timestamp_sequence(" + START_MICROS + ", " + STEP_MICROS + ") "
                        + "FROM long_sequence(" + (long) DAYS * ROWS_PER_DAY + ")",
                ctx
        );
        drainWal(applyJob, checkJob);
        if (isParquet && !isFormatParquet) {
            final String lastDay = String.format("2024-01-%02d", DAYS);
            engine.execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '" + lastDay + "'", ctx);
            drainWal(applyJob, checkJob);
        }
        final TableToken token = engine.verifyTableName("t");
        if (engine.getTableSequencerAPI().isSuspended(token)) {
            throw new IllegalStateException("table suspended after seeding");
        }
        return token;
    }

    private static WorkerPool startWriterPool(CairoEngine engine, int workers) throws SqlException {
        final WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "pq-replace-bench-write";
            }

            @Override
            public int getWorkerCount() {
                return workers;
            }
        });
        try {
            WorkerPoolUtils.setupWriterJobs(pool, engine);
            pool.start();
            return pool;
        } catch (Throwable th) {
            Misc.free(pool);
            throw th;
        }
    }

    /**
     * Writes one replace commit over the inclusive window [lo, hi] that brings {@code rowCount}
     * rows at the timestamps of rows {@code firstRow ..} of the day.
     */
    private static void writeReplaceCommit(
            CairoEngine engine,
            TableToken token,
            Rnd rnd,
            long dayStart,
            int firstRow,
            int rowCount,
            long lo,
            long hi
    ) {
        try (WalWriter ww = engine.getWalWriter(token)) {
            for (int i = 0; i < rowCount; i++) {
                final TableWriter.Row row = ww.newRow(dayStart + (long) (firstRow + i) * STEP_MICROS);
                row.putSym(COL_SYM, SYMBOLS.getQuick(rnd.nextInt(SYMBOLS.size())));
                row.putSym(COL_SIDE, rnd.nextBoolean() ? "buy" : "sell");
                row.putDouble(COL_PRICE, rnd.nextDouble() * 100);
                row.putDouble(COL_AMOUNT, rnd.nextDouble() * 10);
                row.putLong(COL_ID, rnd.nextLong());
                row.putVarchar(COL_NOTE, NOTES.getQuick(rnd.nextInt(NOTES.size())));
                row.append();
            }
            ww.commitWithParams(lo, hi + 1, WAL_DEDUP_MODE_REPLACE_RANGE);
        }
    }

    private enum Placement {
        // the middle day: Parquet through CONVERT PARTITION TO PARQUET
        HISTORICAL(1),
        // the last day: Parquet through FORMAT PARQUET
        LAST(DAYS - 1);

        final int dayIndex;

        Placement(int dayIndex) {
            this.dayIndex = dayIndex;
        }
    }

    private enum Scenario {
        // a window that falls between two rows and brings none
        NOOP("noop", Placement.HISTORICAL, 500_000, 0, false),
        // 1k rows inside row group 4
        MID_1K("mid_1k", Placement.HISTORICAL, 450_000, 1_000, false),
        // the day's last 1k rows, inside its last row group
        TAIL_1K("tail_1k", Placement.HISTORICAL, ROWS_PER_DAY - 1_000, 1_000, false),
        // exactly row group 4
        RG_100K("rg_100k", Placement.HISTORICAL, 400_000, 100_000, false),
        // the second half of the day, row groups 5 to 9
        HALF_500K("half_500k", Placement.HISTORICAL, 500_000, 500_000, false),
        // the whole day
        DAY_1M("day_1m", Placement.HISTORICAL, 0, ROWS_PER_DAY, false),
        // deletes 1k rows inside row group 4
        DEL_1K("del_1k", Placement.HISTORICAL, 450_000, 1_000, true),
        // deletes exactly row group 4
        DEL_100K("del_100k", Placement.HISTORICAL, 400_000, 100_000, true),
        // the last 1k rows of the table's last partition
        LAST_TAIL_1K("last_tail_1k", Placement.LAST, ROWS_PER_DAY - 1_000, 1_000, false);

        final int firstRow;
        final boolean isDelete;
        final String label;
        final Placement placement;
        final int windowRows;

        Scenario(String label, Placement placement, int firstRow, int windowRows, boolean isDelete) {
            this.label = label;
            this.placement = placement;
            this.firstRow = firstRow;
            this.windowRows = windowRows;
            this.isDelete = isDelete;
        }
    }

    private static final class BenchConfig extends DefaultCairoConfiguration {
        BenchConfig(CharSequence root) {
            super(root);
        }

        // the server defaults, which DefaultCairoConfiguration does not use

        @Override
        public double getPartitionEncoderParquetMinCompressionRatio() {
            return 1.2;
        }

        @Override
        public int getPartitionEncoderParquetRowGroupSize() {
            return ROW_GROUP_SIZE;
        }
    }

    private record Result(
            Scenario scenario,
            int workers,
            boolean isParquet,
            double avgNs,
            double p50Ns,
            double p90Ns,
            long maxNs,
            long dayBytesStart,
            long dayBytesEnd,
            long inPlace,
            long rewrite,
            long dropRowGroup,
            long noop
    ) {
    }
}
