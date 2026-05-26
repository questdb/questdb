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
import io.questdb.griffin.engine.window.CachedWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.DeferredEmitWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
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
 * End-to-end window-function LEAD/LAG benchmark comparing the deferred-emit streaming path
 * ({@link DeferredEmitWindowRecordCursorFactory}) against the existing cached path
 * ({@link CachedWindowRecordCursorFactory}) and (for some shapes) the immediate-emit streaming
 * path ({@link WindowRecordCursorFactory}). Path selection is gated by the session flag
 * {@code cairo.sql.window.streaming.lead.enabled}.
 * <p>
 * <h3>Single-function shapes (validate Phases 3, 4, 5)</h3>
 * <ul>
 *   <li>{@code S1_LEAD_NO_PARTITION} — {@code LEAD(x,1) OVER ()}. Phase 3.</li>
 *   <li>{@code S2_LAG_DESC_NO_PARTITION} — {@code LAG(x,1) OVER (ORDER BY ts DESC)}.
 *       Phase 4 normalises to LEAD-ASC and streams.</li>
 *   <li>{@code S3_LEAD_PARTITIONED} — {@code LEAD(x,1) OVER (PARTITION BY sym)}. Phase 5.</li>
 *   <li>{@code S4_LAG_DESC_PARTITIONED} —
 *       {@code LAG(x,1) OVER (PARTITION BY sym ORDER BY ts DESC)}. Phase 4 + 5. The original
 *       triggering query shape.</li>
 * </ul>
 * <p>
 * <h3>Mixed-function shapes (Phase 6 candidates)</h3>
 * Each Q-prefixed shape corresponds to a row of the mixed-LAG-LEAD plan probe table. All currently
 * route through {@link CachedWindowRecordCursorFactory} under both {@code STREAMING} and
 * {@code CACHED} paths because the cursor doesn't yet support mixed functions; the benchmark
 * establishes a baseline that Phase 6 would improve.
 * <ul>
 *   <li>{@code Q1_MIXED_NO_ORDER} — {@code LAG(x,1) OVER () + LEAD(x,1) OVER ()}.
 *       Cached, no sort trees.</li>
 *   <li>{@code Q2_MIXED_ASC} — both functions {@code OVER (ORDER BY ts ASC)}. Cached, orders
 *       dismissed (natural).</li>
 *   <li>{@code Q3_MIXED_DESC} — both {@code OVER (ORDER BY ts DESC)}. Cached, two sort trees
 *       (LEAD and LAG disagree on scan direction internally).</li>
 *   <li>{@code Q4_MIXED_PARTITION_NO_ORDER} — both {@code OVER (PARTITION BY sym)}. Cached,
 *       partitioned, no sort trees.</li>
 *   <li>{@code Q5_MIXED_PARTITION_ASC} — both
 *       {@code OVER (PARTITION BY sym ORDER BY ts ASC)}. Cached, partitioned, orders
 *       dismissed.</li>
 *   <li>{@code Q6_MIXED_PARTITION_DESC} — both
 *       {@code OVER (PARTITION BY sym ORDER BY ts DESC)}. Cached, partitioned, two sort trees.
 *       Worst-case current cost.</li>
 *   <li>{@code Q7_MIXED_INVERSE_NO_PARTITION} —
 *       {@code LAG(x,1) OVER (ORDER BY ts DESC) + LEAD(x,1) OVER (ORDER BY ts ASC)}. LAG and
 *       LEAD compute the same row value but the planner doesn't unify them; LAG gets a sort
 *       tree, LEAD is unordered.</li>
 *   <li>{@code Q8_MIXED_INVERSE_PARTITION} — same as Q7 but partitioned.</li>
 *   <li>{@code Q9_DUAL_LEAD} — {@code LEAD(x,1) OVER (ORDER BY ts ASC) + LEAD(x,3)
 *       OVER (ORDER BY ts ASC)}. Two LEADs with different offsets, both cached today.</li>
 *   <li>{@code Q10_DUAL_LAG} — {@code LAG(x,1) OVER (ORDER BY ts ASC) + LAG(x,3) OVER (ORDER BY
 *       ts ASC)}. Two LAGs, both ZERO_PASS + lookahead=0 — already streams via the existing
 *       {@link WindowRecordCursorFactory}. Baseline: this is the floor that Phase 6 would aim
 *       to bring mixed-LAG-LEAD shapes down to.</li>
 *   <li>{@code Q11_MIXED_DESC_OUTER_DESC} —
 *       {@code LAG(x,1) OVER (ORDER BY ts DESC) + LEAD(x,1) OVER (ORDER BY ts DESC)
 *       ORDER BY ts DESC}. Outer DESC triggers backward scan; both OVER orders dismiss.
 *       Cached, no sort trees, backward base scan.</li>
 * </ul>
 * <p>
 * Params:
 * <ul>
 *   <li>{@code path}: {@code STREAMING} (flag on, dispatches to DeferredEmitWindow where the
 *       planner can) or {@code CACHED} (flag off, baseline).</li>
 *   <li>{@code shape}: one of the 15 query shapes above.</li>
 *   <li>{@code rowCount}: total row count in the seed table. Defaults to 1M; pass
 *       {@code -p rowCount=100000,1000000,10000000} for a full sweep.</li>
 *   <li>{@code partitionCardinality}: number of distinct symbol values.</li>
 * </ul>
 * <p>
 * A {@code @Setup(Level.Trial)} routing guard walks the compiled factory chain to confirm each
 * {@code shape}+{@code path} pair hits the expected factory class. Silent routing drift would
 * corrupt the numbers; the assertion catches it at setup rather than mid-run.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
public class WindowLeadLagBenchmark {

    private static final int SYMBOL_CAPACITY = 2_000_000;

    @Param({"CACHED", "STREAMING"})
    public String path;

    @Param({"1000", "100000"})
    public int partitionCardinality;

    @Param({"1000000"})
    public int rowCount;

    @Param({
            // Single-function shapes (Phases 3-5 streaming).
            "S1_LEAD_NO_PARTITION",
            "S2_LAG_DESC_NO_PARTITION",
            "S3_LEAD_PARTITIONED",
            "S4_LAG_DESC_PARTITIONED",
            // Mixed and multi-function shapes (Phase 6 candidates).
            "Q1_MIXED_NO_ORDER",
            "Q2_MIXED_ASC",
            "Q3_MIXED_DESC",
            "Q4_MIXED_PARTITION_NO_ORDER",
            "Q5_MIXED_PARTITION_ASC",
            "Q6_MIXED_PARTITION_DESC",
            "Q7_MIXED_INVERSE_NO_PARTITION",
            "Q8_MIXED_INVERSE_PARTITION",
            "Q9_DUAL_LEAD",
            "Q10_DUAL_LAG",
            "Q11_MIXED_DESC_OUTER_DESC"
    })
    public String shape;

    private SqlCompilerImpl compiler;
    private SqlExecutionContext ctx;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private Path tempRoot;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(WindowLeadLagBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void run(Blackhole bh) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            final int columnCount = factory.getMetadata().getColumnCount();
            while (cursor.hasNext()) {
                // Drain every output column. For the partitioned shapes the first column is sym
                // (SYMBOL) so cannot be read as long; everything else is x/ts/window-result which
                // are long. Probe column 0 by type to avoid the cast issue, drain the rest as long.
                if (factory.getMetadata().getColumnType(0) == io.questdb.cairo.ColumnType.SYMBOL) {
                    bh.consume(record.getSymA(0));
                } else {
                    bh.consume(record.getLong(0));
                }
                for (int c = 1; c < columnCount; c++) {
                    bh.consume(record.getLong(c));
                }
            }
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        tempRoot = java.nio.file.Files.createTempDirectory("windowleadlagbench-");
        final boolean streaming = "STREAMING".equals(path);
        final CairoConfiguration configuration = new DefaultCairoConfiguration(tempRoot.toString()) {
            @Override
            public boolean getSqlWindowStreamingLeadEnabled() {
                return streaming;
            }
        };
        engine = new CairoEngine(configuration);
        ctx = new SqlExecutionContextImpl(engine, 1)
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
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        factory = Misc.free(factory);
        compiler = Misc.free(compiler);
        engine = Misc.free(engine);
        if (tempRoot != null && java.nio.file.Files.exists(tempRoot)) {
            try (java.util.stream.Stream<Path> stream = java.nio.file.Files.walk(tempRoot)) {
                stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        java.nio.file.Files.deleteIfExists(p);
                    } catch (Exception ignore) {
                    }
                });
            }
            tempRoot = null;
        }
    }

    private void assertRouting(RecordCursorFactory root) {
        final Class<?> expected = expectedFactory();
        RecordCursorFactory cur = root;
        while (cur != null) {
            if (expected.isInstance(cur)) {
                return;
            }
            RecordCursorFactory next = cur.getBaseFactory();
            if (next == cur) {
                break;
            }
            cur = next;
        }
        throw new IllegalStateException(
                "routing drift: expected " + expected.getSimpleName()
                        + " in factory chain but did not find it. path=" + path
                        + " shape=" + shape
                        + " root=" + (root != null ? root.getClass().getSimpleName() : "null")
        );
    }

    private String buildSql() {
        return switch (shape) {
            // -- Single-function shapes -----------------------------------------------------
            case "S1_LEAD_NO_PARTITION" ->
                    "SELECT x, ts, lead(x, 1) OVER () FROM t";
            case "S2_LAG_DESC_NO_PARTITION" ->
                    "SELECT x, ts, lag(x, 1) OVER (ORDER BY ts DESC) FROM t";
            case "S3_LEAD_PARTITIONED" ->
                    "SELECT x, ts, lead(x, 1) OVER (PARTITION BY sym) FROM t";
            case "S4_LAG_DESC_PARTITIONED" ->
                    "SELECT x, ts, lag(x, 1) OVER (PARTITION BY sym ORDER BY ts DESC) FROM t";
            // -- Mixed-function shapes (Phase 6 candidates) ---------------------------------
            case "Q1_MIXED_NO_ORDER" ->
                    "SELECT x, ts, lag(x, 1) OVER (), lead(x, 1) OVER () FROM t";
            case "Q2_MIXED_ASC" ->
                    "SELECT x, ts, lag(x, 1) OVER (ORDER BY ts ASC), lead(x, 1) OVER (ORDER BY ts ASC) FROM t";
            case "Q3_MIXED_DESC" ->
                    "SELECT x, ts, lag(x, 1) OVER (ORDER BY ts DESC), lead(x, 1) OVER (ORDER BY ts DESC) FROM t";
            case "Q4_MIXED_PARTITION_NO_ORDER" ->
                    "SELECT x, ts, lag(x, 1) OVER (PARTITION BY sym), lead(x, 1) OVER (PARTITION BY sym) FROM t";
            case "Q5_MIXED_PARTITION_ASC" ->
                    "SELECT x, ts, lag(x, 1) OVER (PARTITION BY sym ORDER BY ts ASC), lead(x, 1) OVER (PARTITION BY sym ORDER BY ts ASC) FROM t";
            case "Q6_MIXED_PARTITION_DESC" ->
                    "SELECT x, ts, lag(x, 1) OVER (PARTITION BY sym ORDER BY ts DESC), lead(x, 1) OVER (PARTITION BY sym ORDER BY ts DESC) FROM t";
            case "Q7_MIXED_INVERSE_NO_PARTITION" ->
                    "SELECT x, ts, lag(x, 1) OVER (ORDER BY ts DESC), lead(x, 1) OVER (ORDER BY ts ASC) FROM t";
            case "Q8_MIXED_INVERSE_PARTITION" ->
                    "SELECT x, ts, lag(x, 1) OVER (PARTITION BY sym ORDER BY ts DESC), lead(x, 1) OVER (PARTITION BY sym ORDER BY ts ASC) FROM t";
            case "Q9_DUAL_LEAD" ->
                    "SELECT x, ts, lead(x, 1) OVER (ORDER BY ts ASC), lead(x, 3) OVER (ORDER BY ts ASC) FROM t";
            case "Q10_DUAL_LAG" ->
                    "SELECT x, ts, lag(x, 1) OVER (ORDER BY ts ASC), lag(x, 3) OVER (ORDER BY ts ASC) FROM t";
            case "Q11_MIXED_DESC_OUTER_DESC" ->
                    "SELECT x, ts, lag(x, 1) OVER (ORDER BY ts DESC), lead(x, 1) OVER (ORDER BY ts DESC) FROM t ORDER BY ts DESC";
            default -> throw new IllegalArgumentException("unknown shape: " + shape);
        };
    }

    private Class<?> expectedFactory() {
        // Q10 (dual LAG ASC) always streams via the existing immediate-emit Window factory because
        // both LAGs are ZERO_PASS + lookahead=0; no positive-lookahead function -> the deferred-emit
        // dispatch doesn't apply.
        if ("Q10_DUAL_LAG".equals(shape)) {
            return WindowRecordCursorFactory.class;
        }
        if ("CACHED".equals(path)) {
            return CachedWindowRecordCursorFactory.class;
        }
        // STREAMING. With Phase 6, every shape that has at least one positive-lookahead function
        // and where every window function's value fits in 8 bytes routes through DeferredEmit. All
        // remaining shapes (S1-S4, Q1-Q9, Q11) qualify.
        return DeferredEmitWindowRecordCursorFactory.class;
    }

    private void seedTable() throws SqlException {
        engine.execute(
                "CREATE TABLE t ("
                        + "sym SYMBOL CAPACITY " + SYMBOL_CAPACITY + ","
                        + "x LONG,"
                        + "ts TIMESTAMP"
                        + ") TIMESTAMP(ts) PARTITION BY DAY",
                ctx
        );
        engine.execute(
                "INSERT INTO t "
                        + "SELECT "
                        + "(x % " + partitionCardinality + ")::SYMBOL AS sym, "
                        + "x AS x, "
                        + "timestamp_sequence('2024-01-01T00:00:00.000000Z'::timestamp, 1000) AS ts "
                        + "FROM long_sequence(" + rowCount + ")",
                ctx
        );
    }
}
