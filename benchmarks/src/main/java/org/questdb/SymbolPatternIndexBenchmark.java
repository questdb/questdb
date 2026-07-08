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
import io.questdb.griffin.engine.table.SymbolPatternIndexRecordCursorFactory;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
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
 *   <li>~50 RARE symbols with prefix {@code r} (matched by {@code LIKE 'r%'})</li>
 *   <li>~9850 COMMON symbols with prefix {@code c} (matched by {@code LIKE 'c%'})</li>
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
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(0)
@State(Scope.Benchmark)
public class SymbolPatternIndexBenchmark {

    static final long ROWS = Long.getLong("sympat.rows", 10_000_000L);

    // Canonical scenario ordering for the summary table
    private static final String[] SCEN = {"pos_bitmap", "pos_covering", "covering_broad", "pos_broad", "neg_bitmap", "neg_broad"};
    private static final Map<String, String> PRED = Map.of(
            "pos_bitmap", "LIKE 'r%'",
            "pos_covering", "LIKE 'r%'",
            "covering_broad", "LIKE 'c%'",
            "pos_broad", "LIKE 'c%'",
            "neg_bitmap", "NOT LIKE 'c%'",
            "neg_broad", "NOT LIKE 'r%'");
    // Populated by probeAllPaths() after the JMH run (Fork(0) keeps engine in-process)
    private static final Map<String, String> TAKEN = new LinkedHashMap<>();

    private static boolean isDataReady;
    private static SqlExecutionContextImpl sharedCtx;
    private static CairoEngine sharedEngine;
    private static java.nio.file.Path tmpDir;

    @State(Scope.Benchmark)
    public static class ScenarioState {
        @Param({"pos_bitmap", "pos_covering", "covering_broad", "pos_broad", "neg_bitmap", "neg_broad"})
        String scenario;
        RecordCursorFactory fastFactory;
        RecordCursorFactory baselineFactory;
        String taken;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            ensureData();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(sharedEngine)) {
                fastFactory = compiler.compile(scenarioFastSql(scenario), sharedCtx).getRecordCursorFactory();
                baselineFactory = compiler.compile(scenarioBaselineSql(scenario), sharedCtx).getRecordCursorFactory();
            }
            taken = probePath();
        }

        // Determine which path the FAST factory actually takes: probe the runtime bitmap
        // counters (index vs fallback), else covering (covering does not touch those counters),
        // else scan. Reset -> run once -> read.
        private String probePath() throws SqlException {
            SymbolPatternIndexRecordCursorFactory.resetTestCounters();
            drain(fastFactory);
            if (SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0) return "index";
            if (SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0) return "fallback";
            return (scenario.equals("pos_covering") || scenario.equals("covering_broad")) ? "covering" : "scan";
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            fastFactory = Misc.free(fastFactory);
            baselineFactory = Misc.free(baselineFactory);
        }
    }

    @Benchmark
    public long fast(ScenarioState s) throws SqlException {
        return drain(s.fastFactory);
    }

    @Benchmark
    public long baseline(ScenarioState s) throws SqlException {
        return drain(s.baselineFactory);
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
        probeAllPaths();
        printSummary(results);
        if (sharedEngine != null) {
            Misc.free(sharedEngine);
            deleteDirRecursive(tmpDir.toFile());
        }
    }

    // Shared data: built once, reused across all benchmarks / param combinations

    static synchronized void ensureData() throws Exception {
        if (isDataReady) return;

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
            //   100 <= k < 150       -> 'r' || (k-100)  50 distinct rare symbols  (~0.5%)
            //   150 <= k < 10000     -> 'c' || (k-150)  9850 distinct common symbols (~98.5%)
            // Threshold sizing: neg `NOT LIKE 'c%'` complement = 50 rare + 1 NULL key = 51 <= 100 (default
            // cairo.sql.symbol.pattern.index.threshold) -> index path taken.
            sharedEngine.execute(
                    "INSERT INTO " + tbl + " SELECT "
                            + "  CASE"
                            + "    WHEN k < 100 THEN NULL"
                            + "    WHEN k < 150 THEN concat('r', cast(k - 100 as string))"
                            + "    ELSE concat('c', cast(k - 150 as string))"
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

        isDataReady = true;
    }

    // Utility: drain all rows from a RecordCursorFactory, summing numeric columns

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

    // Utility: recursive directory delete (verbatim from PostingIndexBenchmarkSuite)

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

    // Scenario SQL: single source of truth shared by ScenarioState.setup() and
    // probeAllPaths() so the two can never drift.
    // Returns the SELECT (without the hint) for the given scenario.

    static String scenarioFastSql(String scenario) {
        String table = (scenario.equals("pos_covering") || scenario.equals("covering_broad")) ? "t_covering" : "t_bitmap";
        String proj = (scenario.equals("pos_covering") || scenario.equals("covering_broad")) ? "sym, price" : "sym, price, val";
        String pred = switch (scenario) {
            case "pos_bitmap", "pos_covering" -> "sym like 'r%'";
            case "covering_broad", "pos_broad" -> "sym like 'c%'";
            case "neg_bitmap" -> "sym not like 'c%'";
            case "neg_broad" -> "sym not like 'r%'";
            default -> throw new IllegalStateException(scenario);
        };
        return "select " + proj + " from " + table + " where " + pred;
    }

    static String scenarioBaselineSql(String scenario) {
        String table = (scenario.equals("pos_covering") || scenario.equals("covering_broad")) ? "t_covering" : "t_bitmap";
        String proj = (scenario.equals("pos_covering") || scenario.equals("covering_broad")) ? "sym, price" : "sym, price, val";
        String pred = switch (scenario) {
            case "pos_bitmap", "pos_covering" -> "sym like 'r%'";
            case "covering_broad", "pos_broad" -> "sym like 'c%'";
            case "neg_bitmap" -> "sym not like 'c%'";
            case "neg_broad" -> "sym not like 'r%'";
            default -> throw new IllegalStateException(scenario);
        };
        return "select /*+ no_symbol_pattern_index no_covering */ " + proj + " from " + table + " where " + pred;
    }

    // probeAllPaths: recompute taken labels after JMH run.
    // Fork(0) keeps sharedEngine alive in-process, so we can probe directly.

    private static void probeAllPaths() throws Exception {
        if (sharedEngine == null) return;
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(sharedEngine)) {
            for (String scen : SCEN) {
                try (RecordCursorFactory factory = compiler.compile(scenarioFastSql(scen), sharedCtx).getRecordCursorFactory()) {
                    SymbolPatternIndexRecordCursorFactory.resetTestCounters();
                    drain(factory);
                    String label;
                    if (SymbolPatternIndexRecordCursorFactory.testIndexInvocations.get() > 0) {
                        label = "index";
                    } else if (SymbolPatternIndexRecordCursorFactory.testFallbackInvocations.get() > 0) {
                        label = "fallback";
                    } else {
                        label = (scen.equals("pos_covering") || scen.equals("covering_broad")) ? "covering" : "scan";
                    }
                    TAKEN.put(scen, label);
                }
            }
        }
    }

    // Summary: per-scenario speedup + path taken + baseline-plan disclosure

    private static void printSummary(Collection<RunResult> results) {
        Map<String, Double> fast = new HashMap<>();
        Map<String, Double> base = new HashMap<>();
        for (RunResult rr : results) {
            String m = rr.getParams().getBenchmark();
            String scen = rr.getParams().getParam("scenario");
            double score = rr.getPrimaryResult().getScore();
            if (m.endsWith(".fast")) fast.put(scen, score);
            else if (m.endsWith(".baseline")) base.put(scen, score);
        }
        System.out.println();
        System.out.println("=== Symbol pattern-index benchmark (" + ROWS + " rows, avg ms/op, lower=better) ===");
        System.out.printf("%-13s  %-16s  %13s  %10s  %8s   %s%n",
                "scenario", "predicate", "baseline(ms)", "fast(ms)", "speedup", "taken");
        System.out.printf("%-13s  %-16s  %13s  %10s  %8s   %s%n",
                "-------------", "----------------", "-------------", "----------", "--------", "--------");
        for (String scen : SCEN) {
            Double b = base.get(scen), f = fast.get(scen);
            if (b == null || f == null) continue;
            String taken = TAKEN.getOrDefault(scen, "?");
            System.out.printf("%-13s  %-16s  %13.2f  %10.2f  %7.1fx   %s%n",
                    scen, PRED.get(scen), b, f, b / f, taken);
        }
        System.out.println();
        System.out.println("Notes:");
        System.out.println("  fast     = optimizer's chosen plan (no hints)");
        System.out.println("  baseline = /*+ no_symbol_pattern_index no_covering */ scan+filter plan");
        System.out.println("             (may run in parallel via AsyncFilteredRecordCursorFactory)");
        System.out.println("  taken    = fast path's runtime decision: index / covering / fallback");
    }
}
