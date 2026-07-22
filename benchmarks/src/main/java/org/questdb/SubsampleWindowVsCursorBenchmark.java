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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end A/B of the SUBSAMPLE keep-flag-WINDOW migration (Task 1's
 * {@code CairoConfiguration.isSubsampleWindowEnabled()} kill-switch) against the
 * pre-migration {@code SubsampleRecordCursorFactory}, on identical SQL.
 * <p>
 * Unlike {@link SubsampleSortFusionBenchmark} (which isolates just the sort
 * step under a synthetic unordered-input scenario), this benchmark drives the
 * real desugaring path end to end: {@code windowEnabled=true} compiles
 * {@code SUBSAMPLE <method>(...)} into keep-flag window functions fed by the
 * engine's normal sort/scan machinery; {@code windowEnabled=false} forces the
 * same SQL through the old custom cursor. Same query text, same table, same
 * data - the only variable is which factory chain the optimiser builds.
 * <p>
 * {@code sdt} has no cursor fallback (always migrates) and is intentionally
 * excluded from this benchmark's method grid.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
public class SubsampleWindowVsCursorBenchmark {

    private static final String START_TS = "2024-01-01T00:00:00.000000Z";
    private static final long STEP_MICROS = 1_000L; // 1ms between rows

    @Param({"uniform", "cadence", "m4", "minmax", "lttb"})
    public String method;

    @Param({"100000", "1000000"})
    public int rows;

    @Param({"500"})
    public int target;

    @Param({"true", "false"})
    public boolean windowEnabled;

    private SqlCompilerImpl compiler;
    private SqlExecutionContext ctx;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private Path tempRoot;
    private WorkerPool workerPool;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SubsampleWindowVsCursorBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void run(Blackhole bh) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            long count = 0;
            while (cursor.hasNext()) {
                bh.consume(record.getTimestamp(0));
                bh.consume(record.getDouble(1));
                count++;
            }
            bh.consume(count);
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        tempRoot = java.nio.file.Files.createTempDirectory("subsamplewindowvscursorbench-");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(tempRoot.toString()) {
            @Override
            public boolean isSubsampleWindowEnabled() {
                return windowEnabled;
            }
        };
        engine = new CairoEngine(configuration);

        final int workers = 4;
        workerPool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "subsamplewindowvscursorbench";
            }

            @Override
            public int getWorkerCount() {
                return workers;
            }
        });
        WorkerPoolUtils.setupQueryJobs(workerPool, engine);
        workerPool.start();

        ctx = new SqlExecutionContextImpl(engine, workers)
                .with(
                        configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null,
                        null,
                        -1,
                        null
                );
        compiler = new SqlCompilerImpl(engine);

        seedTable();

        final String sql = buildSql();
        factory = compiler.compile(sql, ctx).getRecordCursorFactory();
        assertRouting(factory);

        // Correctness guard: the row count returned by this (windowEnabled) path must match
        // the row count returned by the OTHER path on the exact same data. A divergence here
        // is a real migration bug, not a benchmark artifact - fail loudly.
        final long thisCount = drainCount(factory);
        final long otherCount = countViaOtherPath(sql);
        if (thisCount != otherCount) {
            throw new IllegalStateException(
                    "CORRECTNESS GUARD TRIPPED: method=" + method + " rows=" + rows + " target=" + target
                            + " windowEnabled=" + windowEnabled
                            + " thisPathCount=" + thisCount + " otherPathCount=" + otherCount
                            + " sql=" + sql);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        factory = Misc.free(factory);
        compiler = Misc.free(compiler);
        if (workerPool != null) {
            workerPool.halt();
            workerPool = null;
        }
        engine = Misc.free(engine);
        if (tempRoot != null && java.nio.file.Files.exists(tempRoot)) {
            try (java.util.stream.Stream<Path> stream = java.nio.file.Files.walk(tempRoot)) {
                stream.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        java.nio.file.Files.deleteIfExists(path);
                    } catch (Exception ignore) {
                    }
                });
            }
            tempRoot = null;
        }
    }

    /**
     * Compiles + runs the SAME sql through a throwaway engine configured for the OPPOSITE value
     * of {@code windowEnabled}, and returns its row count. Used only by the correctness guard in
     * {@link #setUp()}, outside the timed benchmark loop.
     */
    private long countViaOtherPath(String sql) throws Exception {
        final boolean otherWindowEnabled = !windowEnabled;
        Path otherTempRoot = java.nio.file.Files.createTempDirectory("subsamplewindowvscursorbench-guard-");
        final CairoConfiguration otherConfiguration = new DefaultCairoConfiguration(otherTempRoot.toString()) {
            @Override
            public boolean isSubsampleWindowEnabled() {
                return otherWindowEnabled;
            }
        };
        try (CairoEngine otherEngine = new CairoEngine(otherConfiguration)) {
            final SqlExecutionContext otherCtx = new SqlExecutionContextImpl(otherEngine, 1)
                    .with(
                            otherConfiguration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null,
                            null,
                            -1,
                            null
                    );
            try (SqlCompilerImpl otherCompiler = new SqlCompilerImpl(otherEngine)) {
                seedTable(otherEngine, otherCtx);
                try (RecordCursorFactory otherFactory = otherCompiler.compile(sql, otherCtx).getRecordCursorFactory()) {
                    return drainCount(otherFactory, otherCtx);
                }
            }
        } finally {
            try (java.util.stream.Stream<Path> stream = java.nio.file.Files.walk(otherTempRoot)) {
                stream.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        java.nio.file.Files.deleteIfExists(path);
                    } catch (Exception ignore) {
                    }
                });
            }
        }
    }

    private long drainCount(RecordCursorFactory f) throws SqlException {
        return drainCount(f, ctx);
    }

    private long drainCount(RecordCursorFactory f, SqlExecutionContext execCtx) throws SqlException {
        long count = 0;
        try (RecordCursor cursor = f.getCursor(execCtx)) {
            while (cursor.hasNext()) {
                count++;
            }
        }
        return count;
    }

    private void assertRouting(RecordCursorFactory root) {
        RecordCursorFactory cur = root;
        boolean foundCursorFactory = false;
        while (cur != null) {
            if (cur.getClass().getSimpleName().equals("SubsampleRecordCursorFactory")) {
                foundCursorFactory = true;
                break;
            }
            RecordCursorFactory next = cur.getBaseFactory();
            if (next == cur) {
                break;
            }
            cur = next;
        }
        // When windowEnabled=false, the old cursor factory MUST appear in the chain.
        // When windowEnabled=true, it must NOT (the query desugars to window functions instead).
        if (foundCursorFactory != !windowEnabled) {
            throw new IllegalStateException("routing drift: method=" + method + " windowEnabled=" + windowEnabled
                    + " expected SubsampleRecordCursorFactory present=" + !windowEnabled
                    + " but found=" + foundCursorFactory
                    + " root=" + root.getClass().getSimpleName());
        }
    }

    private String buildSql() {
        final String subsampleClause;
        switch (method) {
            case "uniform":
                subsampleClause = "uniform(" + target + ")";
                break;
            case "cadence":
                subsampleClause = "cadence(" + target + ")";
                break;
            case "m4":
                subsampleClause = "m4(value, " + target + ")";
                break;
            case "minmax":
                subsampleClause = "minmax(value, " + target + ")";
                break;
            case "lttb":
                subsampleClause = "lttb(value, " + target + ")";
                break;
            default:
                throw new IllegalStateException("unknown method: " + method);
        }
        return "SELECT ts, value FROM x SUBSAMPLE " + subsampleClause;
    }

    private void seedTable() throws SqlException {
        seedTable(engine, ctx);
    }

    private void seedTable(CairoEngine eng, SqlExecutionContext execCtx) throws SqlException {
        eng.execute(
                "CREATE TABLE x (ts TIMESTAMP, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY",
                execCtx
        );
        // Deterministic (non-random) value generation: the correctness guard stands up a SECOND,
        // independently-seeded table on the opposite windowEnabled path and compares row counts.
        // rnd_double() would make the two tables' data diverge, and some SUBSAMPLE methods
        // (e.g. m4's bucket boundaries) can legitimately return a slightly different row count
        // for different data even at the same (rows, target) - which would trip the guard on a
        // false positive. A deterministic "noisy" formula keeps both tables byte-identical so any
        // count mismatch is a genuine window-vs-cursor divergence.
        eng.execute(
                "INSERT INTO x SELECT "
                        + "timestamp_sequence('" + START_TS + "'::timestamp, " + STEP_MICROS + ") AS ts, "
                        + "sin(x::double / 37.0) * 50 + cos(x::double / 11.0) * 25 + (x % 97) AS value "
                        + "FROM long_sequence(" + rows + ")",
                execCtx
        );
    }
}
