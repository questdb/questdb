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
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.parquet.CopyExportRequestJob;
import io.questdb.cutlass.parquet.ParquetExportMode;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * {@code COPY (...) TO ... WITH FORMAT PARQUET} of a query that exports a table's SYMBOL column through the
 * CURSOR_BASED path, which writes the column as SYMBOL: its keys, with a dictionary per row group of the symbols the
 * row group uses.
 * <p>
 * Shapes:
 * <ul>
 *   <li>CROSS_JOIN - every row of the table, through a CROSS JOIN with one row;</li>
 *   <li>GROUP_BY - one row per symbol, with a count and a sum;</li>
 *   <li>ORDER_BY_LIMIT - the ten rows of the largest v, the newest rows; with 1_000_000 symbols they hold the
 *   symbols with the largest keys.</li>
 * </ul>
 * {@code symbolCount} sets how many distinct symbols the column holds; with 1_000_000, every row has its own.
 * <pre>
 *   mvn -pl core install -DskipTests &amp;&amp; mvn -pl benchmarks package -DskipTests
 *   java -jar benchmarks/target/benchmarks.jar ParquetExportCursorSymbolBenchmark
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
@Fork(value = 1, jvmArgsAppend = {
        "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED",
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED"
})
public class ParquetExportCursorSymbolBenchmark {
    private static final String EXPORT_FILE = "bench_export";
    private static final long ROW_COUNT = 1_000_000;
    private static final String TABLE = "trades";
    @Param({"CROSS_JOIN", "GROUP_BY", "ORDER_BY_LIMIT"})
    public String shape;
    @Param({"8", "1000000"})
    public long symbolCount;
    private SqlCompiler compiler;
    private CopyExportRequestJob copyJob;
    private String copySql;
    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private Path exportDir;
    private long queuedCopyExportCount;
    private Path root;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ParquetExportCursorSymbolBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    public long export() throws Exception {
        try (
                RecordCursorFactory factory = compiler.compile(copySql, ctx).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(ctx)
        ) {
            if (!cursor.hasNext()) {
                throw new IllegalStateException("COPY returned no export id");
            }
        }
        queuedCopyExportCount++;
        if (!copyJob.run()) {
            throw new IllegalStateException("the queued export did not run");
        }
        return Files.size(exportDir.resolve(EXPORT_FILE + ".parquet"));
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        // JMH skips the trial's @TearDown when its @Setup throws
        try {
            setUpTrial();
        } catch (Throwable th) {
            freeTrialResources();
            throw th;
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        try {
            if (copyJob != null) {
                verifyCopyExportsFinished();
            }
        } finally {
            freeTrialResources();
        }
    }

    private String aggregates(String query) throws Exception {
        final String sql = "SELECT count(), count_distinct(sym), min(sym), max(sym) FROM (" + query + ")";
        final StringSink sink = new StringSink();
        try (
                RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(ctx)
        ) {
            CursorPrinter.println(cursor, factory.getMetadata(), sink);
        }
        return sink.toString();
    }

    private String exportedQuery() {
        return switch (shape) {
            case "CROSS_JOIN" -> "SELECT t.sym, t.v, t.ts FROM " + TABLE + " t CROSS JOIN long_sequence(1)";
            case "GROUP_BY" -> "SELECT sym, count() c, sum(v) s FROM " + TABLE;
            case "ORDER_BY_LIMIT" -> "SELECT * FROM " + TABLE + " ORDER BY v DESC LIMIT 10";
            default -> throw new IllegalArgumentException("unknown shape: " + shape);
        };
    }

    private void freeTrialResources() throws IOException {
        copyJob = Misc.free(copyJob);
        compiler = Misc.free(compiler);
        ctx = Misc.free(ctx);
        engine = Misc.free(engine);
        if (root != null) {
            try (Stream<Path> paths = Files.walk(root)) {
                paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException ignore) {
                    }
                });
            }
            root = null;
        }
    }

    private void setUpTrial() throws Exception {
        queuedCopyExportCount = 0;
        root = Files.createTempDirectory("parquet-export-cursor-symbol-bench-");
        exportDir = Files.createDirectories(root.resolve("export"));
        final Path dbRoot = Files.createDirectories(root.resolve("db"));
        final CairoConfiguration configuration = new DefaultCairoConfiguration(dbRoot.toString()) {
            @Override
            public CharSequence getSqlCopyExportRoot() {
                return exportDir.toString();
            }

            @Override
            public CharSequence getSqlCopyInputRoot() {
                return exportDir.toString();
            }
        };
        engine = new CairoEngine(configuration);
        engine.load();
        ctx = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null, null, -1, null
        );
        compiler = engine.getSqlCompiler();
        engine.execute(
                "CREATE TABLE " + TABLE + " (sym SYMBOL CAPACITY " + Math.max(symbolCount, 128) + " NOCACHE, v LONG, ts TIMESTAMP)"
                        + " TIMESTAMP(ts) PARTITION BY DAY",
                ctx
        );
        // Row x holds the symbol s((x - 1) % symbolCount), whose key is (x - 1) % symbolCount.
        engine.execute(
                "INSERT INTO " + TABLE + " SELECT 's' || ((x - 1) % " + symbolCount + "), x,"
                        + " timestamp_sequence('2024-01-01', 100_000L) FROM long_sequence(" + ROW_COUNT + ")",
                ctx
        );

        final String query = exportedQuery();
        try (RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
            final ParquetExportMode mode = ParquetExportMode.determineExportMode(factory, false, ctx);
            if (mode != ParquetExportMode.CURSOR_BASED) {
                throw new IllegalStateException("shape=" + shape + " exports via " + mode + ", expected CURSOR_BASED");
            }
        }
        copySql = "COPY (" + query + ") TO '" + EXPORT_FILE + "' WITH FORMAT PARQUET";
        copyJob = new CopyExportRequestJob(engine);
        export();
        final String expected = aggregates(query);
        final String actual = aggregates("SELECT * FROM read_parquet('" + EXPORT_FILE + ".parquet')");
        if (!expected.equals(actual)) {
            throw new IllegalStateException("export of " + shape + " differs from the query: " + actual + " vs " + expected);
        }
        System.out.println("[bench] shape=" + shape + " symbolCount=" + symbolCount + " aggregates=" + actual.replace('\n', ' '));
    }

    private void verifyCopyExportsFinished() throws Exception {
        // CopyExportRequestJob logs a failed export and still reports that it ran.
        final String sql = "SELECT count() FROM \"" + engine.getConfiguration().getSystemTableNamePrefix()
                + "copy_export_log\" WHERE phase = 'success' AND status = 'finished'";
        try (
                RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(ctx)
        ) {
            final long finishedCount = cursor.hasNext() ? cursor.getRecord().getLong(0) : 0;
            if (finishedCount != queuedCopyExportCount) {
                throw new IllegalStateException(
                        finishedCount + " of " + queuedCopyExportCount + " queued exports finished"
                );
            }
        }
    }
}
