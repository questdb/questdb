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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Measures a raw timestamp/value projection with a covered residual predicate.
 * Key and residual selectivity use coprime deterministic cycles, preventing the
 * residual distribution from correlating with either indexed key. The default
 * 100-million-row trial yields 1 million and 10 million indexed candidates for
 * the two key selectivities; use the JMH {@code rowCount} parameter to rescale it.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@Fork(0)
public class CoveringIndexPostFilterBenchmark {
    private static final int WORKERS = Integer.getInteger("covering.postfilter.workers", 8);
    private static final String ROOT = System.getProperty("java.io.tmpdir") + java.io.File.separator + "covering-postfilter-bench";
    private static final DefaultCairoConfiguration CONFIGURATION = new DefaultCairoConfiguration(ROOT) {
        @Override
        public int getSqlPageFrameMaxRows() {
            return 100_000;
        }
    };

    private static SqlCompiler compiler;
    private static SqlExecutionContext context;
    private static CairoEngine engine;
    private static long environmentRowCount = -1;
    private static WorkerPool pool;

    @Param({"100000000"})
    public long rowCount;

    @Param({
            "covering-k1-r10", "no_covering-k1-r10", "no_index-k1-r10",
            "covering-k1-r100", "no_covering-k1-r100", "no_index-k1-r100",
            "covering-k10-r10", "no_covering-k10-r10", "no_index-k10-r10",
            "covering-k10-r100", "no_covering-k10-r100", "no_index-k10-r100"
    })
    public String scenario;

    private long expectedChecksum;
    private long expectedRows;
    private RecordCursorFactory factory;
    private long lastRowCount;

    public static void main(String[] args) throws Exception {
        final Options options = args.length > 0
                ? new CommandLineOptions(args)
                : new OptionsBuilder().include(CoveringIndexPostFilterBenchmark.class.getSimpleName()).build();
        try {
            new Runner(options).run();
        } finally {
            closeEnvironment();
        }
    }

    @Benchmark
    public long run() throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            final long checksum = drain(cursor);
            if (lastRowCount != expectedRows || checksum != expectedChecksum) {
                throw new IllegalStateException("route returned a different result [scenario=" + scenario + ']');
            }
            return checksum;
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        ensureEnvironment(rowCount);
        final String route = scenario.substring(0, scenario.indexOf('-'));
        final String query = query(scenario, route);
        verifyRoute(query, route);
        factory = compiler.compile(query, context).getRecordCursorFactory();
        try (RecordCursorFactory oracleFactory = compiler.compile(query(scenario, "no_index"), context).getRecordCursorFactory();
             RecordCursor oracleCursor = oracleFactory.getCursor(context)) {
            expectedChecksum = drain(oracleCursor);
            expectedRows = lastRowCount;
        }
        try (RecordCursor cursor = factory.getCursor(context)) {
            final long checksum = drain(cursor);
            if (lastRowCount != expectedRows || checksum != expectedChecksum) {
                throw new IllegalStateException("route checksum mismatch [scenario=" + scenario + ']');
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        factory = Misc.free(factory);
    }

    private static synchronized void closeEnvironment() {
        compiler = Misc.free(compiler);
        if (pool != null) {
            pool.halt();
            pool = null;
        }
        engine = Misc.free(engine);
        context = null;
        environmentRowCount = -1;
        LogFactory.haltInstance();
    }

    private long drain(RecordCursor cursor) {
        final Record record = cursor.getRecord();
        long checksum = 0;
        long rows = 0;
        while (cursor.hasNext()) {
            checksum = checksum * 31 + record.getTimestamp(0);
            checksum = checksum * 31 + Double.doubleToLongBits(record.getDouble(1));
            rows++;
        }
        lastRowCount = rows;
        return checksum;
    }

    private static synchronized void ensureEnvironment(long rowCount) throws Exception {
        if (engine != null) {
            if (environmentRowCount == rowCount) {
                return;
            }
            closeEnvironment();
        }
        Files.createDirectories(Paths.get(ROOT));
        engine = new CairoEngine(CONFIGURATION);
        context = new SqlExecutionContextImpl(engine, WORKERS) {
            @Override
            public boolean shouldLogSql() {
                return false;
            }
        }.with(
                CONFIGURATION.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null,
                null,
                -1,
                null
        );
        engine.execute("DROP TABLE IF EXISTS post_filter", context);
        engine.execute("""
                CREATE TABLE post_filter (
                    ts TIMESTAMP,
                    sym SYMBOL INDEX TYPE POSTING INCLUDE (value, residual),
                    value DOUBLE,
                    residual INT
                ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                """, context);
        final long spacing = Math.max(1, 16L * 86_400_000_000L / rowCount);
        engine.execute(
                "INSERT INTO post_filter SELECT " +
                        "('2024-01-01'::timestamp + (x - 1) * " + spacing + "L)::timestamp, " +
                        "(CASE WHEN x % 1000 < 10 THEN 'k1' WHEN x % 1000 < 110 THEN 'k10' ELSE 'noise' || (x % 64) END)::symbol, " +
                        "(x % 10_007)::double, (x % 997)::int " +
                        "FROM long_sequence(" + rowCount + ')',
                context
        );
        engine.releaseAllWriters();
        environmentRowCount = rowCount;
        pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "covering-postfilter-bench";
            }

            @Override
            public int getWorkerCount() {
                return WORKERS;
            }
        });
        WorkerPoolUtils.setupQueryJobs(pool, engine);
        pool.start();
        compiler = engine.getSqlCompiler();
    }

    private static String query(String scenario, String route) {
        final String[] parts = scenario.split("-");
        final String hint = switch (route) {
            case "covering" -> "";
            case "no_covering" -> "/*+ no_covering */ ";
            case "no_index" -> "/*+ no_index */ ";
            default -> throw new IllegalArgumentException("unknown route: " + route);
        };
        final int residualLimit = "r10".equals(parts[2]) ? 100 : 997;
        return "SELECT " + hint + "ts, value FROM post_filter WHERE sym = '" + parts[1] +
                "' AND residual < " + residualLimit;
    }

    private static void verifyRoute(String query, String route) throws Exception {
        final StringBuilder plan = new StringBuilder();
        try (RecordCursorFactory explainFactory = compiler.compile("EXPLAIN " + query, context).getRecordCursorFactory();
             RecordCursor cursor = explainFactory.getCursor(context)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                plan.append(record.getStrA(0)).append('\n');
            }
        }
        final String text = plan.toString();
        final boolean isValid = switch (route) {
            case "covering" -> text.contains("Async JIT Filter") && text.contains("CoveringIndex");
            case "no_covering" -> !text.contains("CoveringIndex") && text.contains("Index");
            case "no_index" -> !text.contains("CoveringIndex") && !text.contains("Index forward scan");
            default -> false;
        };
        if (!isValid) {
            throw new IllegalStateException("unexpected route [route=" + route + ", plan=" + text + ']');
        }
    }
}
