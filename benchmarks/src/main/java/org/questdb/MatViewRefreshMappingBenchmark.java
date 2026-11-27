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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;

/**
 * Measures the incremental refresh of a materialized view by the copy path its column mapping takes. The refresh
 * maps the query's columns to the view's by name:
 * <ul>
 *     <li>{@code identity}: the view's columns are the query's, in the query's order, the case of every view created
 *     with CREATE. The refresh copies the query's rows with a generated copier, as before name mapping;</li>
 *     <li>{@code reordered}: an {@code ALTER MATERIALIZED VIEW ... AS} puts the same columns in another order, so the
 *     refresh copies through a column projection and a copier that names the view column of each query column;</li>
 *     <li>{@code unmapped}: a view {@code ADD COLUMN} adds a column that the query does not produce, which takes the
 *     same path and leaves the column NULL.</li>
 * </ul>
 * Each invocation appends {@link #MINUTES_PER_BATCH} minutes of rows for {@link #SYMBOLS} symbols to the base table and
 * applies its WAL outside the measurement, then measures one run of the refresh job, which recomputes those
 * buckets of the 1-minute OHLC view and writes them to the view's WAL, {@code MINUTES_PER_BATCH * SYMBOLS} rows. The
 * view's WAL applies after the measurement.
 * <p>
 * The {@code identity} case runs on builds without name mapping as well; the other two need
 * {@code ALTER MATERIALIZED VIEW ... AS} and view {@code ADD COLUMN}. Run it with
 * {@code java -cp benchmarks/target/benchmarks.jar org.questdb.MatViewRefreshMappingBenchmark}, or through the JMH
 * main with {@code -p mapping=identity} on such a build.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(value = 1, jvmArgsAppend = {"--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"})
public class MatViewRefreshMappingBenchmark {
    private static final int MINUTES_PER_BATCH = 30;
    private static final long MINUTE_MICROS = 60_000_000L;
    private static final int ROWS_PER_SYMBOL_MINUTE = 2;
    // 2024-01-01T00:00:00Z
    private static final long START_MICROS = 1_704_067_200_000_000L;
    private static final int SYMBOLS = 512;
    @Param({"identity", "reordered", "unmapped"})
    public String mapping;
    private SqlCompiler compiler;
    private SqlExecutionContext context;
    private Path dbRoot;
    private CairoEngine engine;
    private long nextBatchMicros;
    private MatViewRefreshJob refreshJob;
    private ApplyWal2TableJob walApplyJob;

    public static void main(String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include(MatViewRefreshMappingBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @TearDown(Level.Invocation)
    public void applyViewWal() {
        drainWal();
    }

    @Setup(Level.Invocation)
    public void insertBatch() throws SqlException {
        insertRows(nextBatchMicros, MINUTES_PER_BATCH);
        nextBatchMicros += MINUTES_PER_BATCH * MINUTE_MICROS;
        // the base table's WAL applies outside the measurement, and notifies the view
        drainWal();
    }

    @Benchmark
    public boolean refresh() {
        boolean isRefreshed = false;
        while (refreshJob.run()) {
            isRefreshed = true;
        }
        return isRefreshed;
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        dbRoot = Files.createTempDirectory("mvMappingBench-");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(dbRoot.toString());
        engine = new CairoEngine(configuration);
        // swaps the no-op mat view state store for the real one
        engine.load();
        context = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null,
                null,
                -1,
                null
        );
        compiler = engine.getSqlCompiler();
        refreshJob = new MatViewRefreshJob(0, engine, 1);
        walApplyJob = new ApplyWal2TableJob(engine, 0);

        engine.execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL", context);
        engine.execute(
                "CREATE MATERIALIZED VIEW ohlc AS (" +
                        "SELECT ts, sym, first(price) AS open, max(price) AS high, min(price) AS low, last(price) AS close, sum(amount) AS volume " +
                        "FROM trades SAMPLE BY 1m" +
                        ") PARTITION BY DAY",
                context
        );
        // a day of history, built by the view's first refresh
        nextBatchMicros = START_MICROS;
        insertRows(nextBatchMicros, 24 * 60);
        nextBatchMicros += 24 * 60 * MINUTE_MICROS;
        drainWal();
        refresh();
        drainWal();

        switch (mapping) {
            case "identity" -> {
            }
            case "reordered" -> engine.execute(
                    "ALTER MATERIALIZED VIEW ohlc AS (" +
                            "SELECT sym, sum(amount) AS volume, last(price) AS close, min(price) AS low, max(price) AS high, first(price) AS open, ts " +
                            "FROM trades SAMPLE BY 1m" +
                            ")",
                    context
            );
            case "unmapped" -> engine.execute("ALTER MATERIALIZED VIEW ohlc ADD COLUMN note VARCHAR", context);
            default -> throw new IllegalArgumentException("unknown mapping: " + mapping);
        }
        drainWal();
        // a refresh of the new mapping before the measurement, which compiles the SQL and builds the copier
        insertBatch();
        refresh();
        applyViewWal();
        final long expectedRows = countRows("(SELECT ts, sym, last(price) AS close FROM trades SAMPLE BY 1m)");
        if (countRows("ohlc") != expectedRows) {
            throw new IllegalStateException("the view did not refresh [rows=" + countRows("ohlc") + ", expected=" + expectedRows + ']');
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        refreshJob.close();
        walApplyJob.close();
        compiler.close();
        context.close();
        engine.close();
        deleteRecursively(dbRoot);
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

    private long countRows(String tableName) throws SqlException {
        try (
                RecordCursorFactory factory = compiler.compile("SELECT count() FROM " + tableName, context).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(context)
        ) {
            return cursor.hasNext() ? cursor.getRecord().getLong(0) : -1;
        }
    }

    private void drainWal() {
        //noinspection StatementWithEmptyBody
        while (walApplyJob.run()) ;
        if (new CheckWalTransactionsJob(engine).run()) {
            //noinspection StatementWithEmptyBody
            while (walApplyJob.run()) ;
        }
    }

    // One WAL transaction with ROWS_PER_SYMBOL_MINUTE rows per symbol and minute, from fromMicros on.
    private void insertRows(long fromMicros, int minutes) throws SqlException {
        final long rowsPerMinute = (long) SYMBOLS * ROWS_PER_SYMBOL_MINUTE;
        final long stepMicros = MINUTE_MICROS / rowsPerMinute;
        engine.execute(
                "INSERT INTO trades SELECT " +
                        "'s' || (x % " + SYMBOLS + "), " +
                        "100.0 + (x % 97) * 0.25, " +
                        "1.0 + (x % 13), " +
                        "(" + fromMicros + " + (x - 1) * " + stepMicros + ")::timestamp " +
                        "FROM long_sequence(" + rowsPerMinute * minutes + ")",
                context
        );
    }
}
