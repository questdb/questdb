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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark for the symbol pattern-index fast path.
 *
 * <p>Compares indexed-symbol {@code LIKE}/{@code ~} queries that use the
 * {@link io.questdb.griffin.engine.table.SymbolPatternIndexRecordCursorFactory} fast path
 * against a plain scan+filter baseline on two table variants:
 * <ul>
 *   <li>{@code t_bitmap} — standard bitmap index ({@code sym symbol index})</li>
 *   <li>{@code t_covering} — posting/covering index ({@code sym symbol index type posting include (price)})</li>
 * </ul>
 *
 * <p>Two-family symbol scheme:
 * <ul>
 *   <li>~100 RARE symbols with prefix {@code r} (matched by {@code LIKE 'r%'})</li>
 *   <li>~9900 COMMON symbols with prefix {@code c} (matched by {@code LIKE 'c%'})</li>
 *   <li>~1% NULL symbols</li>
 * </ul>
 *
 * <pre>
 * mvn -q install -pl core -DskipTests
 * mvn -q package -pl benchmarks -DskipTests
 * java -Dsympat.rows=200000 -Dquestdb.log.level=E -cp benchmarks/target/benchmarks.jar org.questdb.SymbolPatternIndexBenchmark
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(0)
@State(Scope.Benchmark)
public class SymbolPatternIndexBenchmark {

    static final long ROWS = Long.getLong("sympat.rows", 10_000_000L);

    private static boolean dataReady;
    private static SqlExecutionContextImpl sharedCtx;
    private static CairoEngine sharedEngine;
    private static java.nio.file.Path tmpDir;

    // Smoke: compile a fresh LIKE query each invocation and drain its result
    @Benchmark
    public long smoke() throws Exception {
        ensureData();
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(sharedEngine)) {
            try (RecordCursorFactory factory = compiler.compile(
                    "SELECT sym, price FROM t_bitmap WHERE sym LIKE 'r%'", sharedCtx
            ).getRecordCursorFactory()) {
                return drain(factory);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("questdb.log.level", "E");
        LogFactory.haltInstance();

        var opts = new OptionsBuilder()
                .include(SymbolPatternIndexBenchmark.class.getSimpleName())
                .resultFormat(ResultFormatType.TEXT)
                .forks(0)
                .warmupIterations(2).warmupTime(TimeValue.seconds(1))
                .measurementIterations(3).measurementTime(TimeValue.seconds(1))
                .build();
        Collection<RunResult> results = new Runner(opts).run();
        printSummary(results);
        if (sharedEngine != null) {
            Misc.free(sharedEngine);
            deleteDirRecursive(tmpDir.toFile());
        }
    }

    // ---------------------------------------------------------------------------
    // Shared data: built once, reused across all benchmarks / param combinations
    // ---------------------------------------------------------------------------

    static synchronized void ensureData() throws Exception {
        if (dataReady) return;

        tmpDir = Files.createTempDirectory("sympat-bench");
        CairoConfiguration config = new DefaultCairoConfiguration(tmpDir.toString()) {
            @Override
            public int getRndFunctionMemoryMaxPages() {
                return 8192;
            }
        };
        sharedEngine = new CairoEngine(config);
        sharedCtx = new SqlExecutionContextImpl(sharedEngine, 1)
                .with(config.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null, null, -1, null);

        for (String tbl : new String[]{"t_bitmap", "t_covering"}) {
            String indexClause = tbl.equals("t_covering")
                    ? "sym symbol index type posting include (price)"
                    : "sym symbol index";
            sharedEngine.execute(
                    "CREATE TABLE " + tbl + " ("
                            + indexClause + ", "
                            + "price double, "
                            + "val long, "
                            + "ts timestamp"
                            + ") timestamp(ts) partition by day bypass wal",
                    sharedCtx);

            // Two-family symbol assignment via a 0-9999 uniform random bucket k:
            //   k < 100              -> NULL          (~1%)
            //   100 <= k < 200       -> 'r' || (k-100)  100 distinct rare symbols
            //   200 <= k < 10000     -> 'c' || (k-200)  9800 distinct common symbols
            // Note: k range 200-10000 gives 9800 distinct values (k-200 in 0..9799),
            // which is close enough to the target of ~9900 COMMON symbols.
            sharedEngine.execute(
                    "INSERT INTO " + tbl + " SELECT "
                            + "  CASE"
                            + "    WHEN k < 100 THEN NULL"
                            + "    WHEN k < 200 THEN concat('r', cast(k - 100 as string))"
                            + "    ELSE concat('c', cast(k - 200 as string))"
                            + "  END, "
                            + "  rnd_double() * 1000, "
                            + "  x, "
                            + "  dateadd('s', cast(x as int), '2024-01-01T00:00:00.000000Z'::timestamp) "
                            + "FROM (SELECT x, rnd_int(0, 9999, 0) k FROM long_sequence(" + ROWS + "))",
                    sharedCtx);
        }
        sharedEngine.releaseAllWriters();

        // Register a shutdown hook so the temp dir is cleaned up on normal exit
        // (mirrors the LimitState pattern in PostingIndexBenchmarkSuite).
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (sharedEngine != null) Misc.free(sharedEngine);
            if (tmpDir != null) deleteDirRecursive(tmpDir.toFile());
        }));

        dataReady = true;
    }

    // ---------------------------------------------------------------------------
    // Utility: drain all rows from a RecordCursorFactory, summing numeric columns
    // ---------------------------------------------------------------------------

    static long drain(RecordCursorFactory factory) throws SqlException {
        long sum = 0;
        var meta = factory.getMetadata();
        int cols = meta.getColumnCount();
        try (RecordCursor cursor = factory.getCursor(sharedCtx)) {
            Record rec = cursor.getRecord();
            while (cursor.hasNext()) {
                for (int c = 0; c < cols; c++) {
                    sum += switch (ColumnType.tagOf(meta.getColumnType(c))) {
                        case ColumnType.DOUBLE -> (long) rec.getDouble(c);
                        case ColumnType.LONG, ColumnType.TIMESTAMP -> rec.getLong(c);
                        default -> 1;
                    };
                }
            }
        }
        return sum;
    }

    // ---------------------------------------------------------------------------
    // Utility: recursive directory delete (verbatim from PostingIndexBenchmarkSuite)
    // ---------------------------------------------------------------------------

    private static void deleteDirRecursive(File dir) {
        try {
            Files.walkFileTree(dir.toPath(), new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult postVisitDirectory(java.nio.file.Path d, IOException e) throws IOException {
                    Files.delete(d);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes a) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException ignored) {
        }
    }

    // ---------------------------------------------------------------------------
    // Summary: no-op stub for Task 1; real output added in Task 3
    // ---------------------------------------------------------------------------

    private static void printSummary(Collection<RunResult> results) { /* Task 3 */ }
}
