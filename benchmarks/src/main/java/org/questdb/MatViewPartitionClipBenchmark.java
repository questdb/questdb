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
import io.questdb.cairo.TableToken;
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
 * Measures a year-long {@code REFRESH MATERIALIZED VIEW ... RANGE} of an hourly view over a base table with partition
 * gaps. A refresh that keeps view rows whose base partitions are gone recomputes only the buckets that overlap an
 * existing base partition, so it splits the range at every partition gap and writes one view WAL transaction per
 * piece; a build without it writes the whole range in one transaction.
 * <ul>
 *     <li>{@code base}: {@code HOUR}, a {@code PARTITION BY HOUR} base with one hour in six populated over 2024,
 *     1,463 partition gaps; {@code DAY}, a {@code PARTITION BY DAY} base with no weekend partitions over 2024, 52
 *     gaps;</li>
 *     <li>{@code stage}: {@code refresh}, the refresh job's run for the RANGE, which writes the view's WAL;
 *     {@code apply}, the WAL apply of what that run wrote; {@code full}, a {@code REFRESH ... FULL}, which rebuilds the
 *     whole view in a single pass without the split, refresh and apply together, as the reference that the change must
 *     leave alone.</li>
 * </ul>
 * The view WAL transactions that one RANGE writes go to stdout once per trial. Run it with
 * {@code java -cp benchmarks/target/benchmarks.jar org.questdb.MatViewPartitionClipBenchmark}; it runs unchanged on
 * builds without the split.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@Fork(value = 1, jvmArgsAppend = {"--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"})
public class MatViewPartitionClipBenchmark {
    private static final long DAY_MICROS = 86_400_000_000L;
    private static final int DAYS = 366;
    private static final int HOUR_BASE_POPULATED_HOUR_STEP = 6;
    private static final long HOUR_MICROS = 3_600_000_000L;
    private static final String RANGE_SQL = "REFRESH MATERIALIZED VIEW v RANGE FROM '2024-01-01' TO '2025-01-01'";
    private static final int ROWS_PER_POPULATED_HOUR = 60;
    // 2024-01-01T00:00:00Z, a Monday
    private static final long START_MICROS = 1_704_067_200_000_000L;
    @Param({"HOUR", "DAY"})
    public String base;
    @Param({"refresh", "apply", "full"})
    public String stage;
    private SqlCompiler compiler;
    private SqlExecutionContext context;
    private Path dbRoot;
    private CairoEngine engine;
    private MatViewRefreshJob refreshJob;
    private long viewTxnBefore;
    private TableToken viewToken;
    private ApplyWal2TableJob walApplyJob;

    public static void main(String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include(MatViewPartitionClipBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @TearDown(Level.Invocation)
    public void afterInvocation() {
        if ("refresh".equals(stage)) {
            drainWal();
        }
    }

    @Setup(Level.Invocation)
    public void beforeInvocation() throws SqlException {
        viewTxnBefore = engine.getTableSequencerAPI().lastTxn(viewToken);
        switch (stage) {
            case "refresh" -> engine.execute(RANGE_SQL, context);
            case "apply" -> {
                engine.execute(RANGE_SQL, context);
                runRefreshJob();
            }
            case "full" -> {
            }
            default -> throw new IllegalArgumentException("unknown stage: " + stage);
        }
    }

    @Benchmark
    public long run() throws SqlException {
        switch (stage) {
            case "refresh" -> runRefreshJob();
            case "apply" -> drainWal();
            case "full" -> {
                engine.execute("REFRESH MATERIALIZED VIEW v FULL", context);
                runRefreshJob();
                drainWal();
            }
            default -> throw new IllegalArgumentException("unknown stage: " + stage);
        }
        return engine.getTableSequencerAPI().lastTxn(viewToken) - viewTxnBefore;
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        dbRoot = Files.createTempDirectory("mvClipBench-");
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

        engine.execute("CREATE TABLE base (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts) PARTITION BY " + base + " WAL", context);
        engine.execute("CREATE MATERIALIZED VIEW v AS (SELECT ts, count() AS c, sum(v) AS s FROM base SAMPLE BY 1h) PARTITION BY DAY", context);
        viewToken = engine.verifyTableName("v");
        final long minuteMicros = HOUR_MICROS / ROWS_PER_POPULATED_HOUR;
        if ("HOUR".equals(base)) {
            final long rows = (long) DAYS * 24 / HOUR_BASE_POPULATED_HOUR_STEP * ROWS_PER_POPULATED_HOUR;
            // row x lands in populated hour (x - 1) / ROWS_PER_POPULATED_HOUR, every HOUR_BASE_POPULATED_HOUR_STEP hours
            engine.execute(
                    "INSERT INTO base SELECT (" + START_MICROS
                            + " + ((x - 1) / " + ROWS_PER_POPULATED_HOUR + ") * " + HOUR_BASE_POPULATED_HOUR_STEP * HOUR_MICROS
                            + " + ((x - 1) % " + ROWS_PER_POPULATED_HOUR + ") * " + minuteMicros + ")::TIMESTAMP, x::DOUBLE "
                            + "FROM long_sequence(" + rows + ")",
                    context
            );
        } else {
            final long rows = (long) DAYS * 24 * ROWS_PER_POPULATED_HOUR;
            // every hour of every day, without Saturdays and Sundays; 2024-01-01 is a Monday
            engine.execute(
                    "INSERT INTO base SELECT ts, v FROM ("
                            + "SELECT (" + START_MICROS + " + (x - 1) * " + minuteMicros + ")::TIMESTAMP ts, x::DOUBLE v "
                            + "FROM long_sequence(" + rows + ")"
                            + ") WHERE ((ts::LONG - " + START_MICROS + ") / " + DAY_MICROS + ") % 7 < 5",
                    context
            );
        }
        drainWal();
        // the view's first build
        runRefreshJob();
        drainWal();
        final long expectedRows = countRows("(SELECT ts, count() FROM base SAMPLE BY 1h)");
        if (countRows("v") != expectedRows) {
            throw new IllegalStateException("the view did not build [rows=" + countRows("v") + ", expected=" + expectedRows + ']');
        }
        // one RANGE outside the measurement, which compiles the SQL and reports the view WAL transactions it writes
        final long txnBefore = engine.getTableSequencerAPI().lastTxn(viewToken);
        engine.execute(RANGE_SQL, context);
        runRefreshJob();
        drainWal();
        System.out.println("[clip] base=" + base + ", partitions=" + countRows("table_partitions('base')")
                + ", view WAL txns per RANGE=" + (engine.getTableSequencerAPI().lastTxn(viewToken) - txnBefore));
        if (countRows("v") != expectedRows) {
            throw new IllegalStateException("the RANGE changed the view [rows=" + countRows("v") + ", expected=" + expectedRows + ']');
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

    private void runRefreshJob() {
        //noinspection StatementWithEmptyBody
        while (refreshJob.run()) ;
    }
}
