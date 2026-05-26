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
 * ({@link CachedWindowRecordCursorFactory}). Path selection is gated by the session flag
 * {@code cairo.sql.window.streaming.lead.enabled}.
 * <p>
 * Params:
 * <ul>
 *   <li>{@code path}: {@code STREAMING} (flag on, runs through DeferredEmitWindow where the
 *       planner can dispatch to it) or {@code CACHED} (flag off, baseline — every LEAD-bearing
 *       query routes through CachedWindow).</li>
 *   <li>{@code shape}: which query shape to benchmark. Six shapes:
 *       <ul>
 *         <li>{@code LEAD_NO_PARTITION} — `LEAD(x,1) OVER ()`. Phase 3.</li>
 *         <li>{@code LAG_DESC_NO_PARTITION} — `LAG(x,1) OVER (ORDER BY ts DESC)`.
 *             Phase 4 normalises to LEAD-ASC and streams.</li>
 *         <li>{@code LEAD_PARTITIONED} — `LEAD(x,1) OVER (PARTITION BY sym)`. Phase 5.</li>
 *         <li>{@code LAG_DESC_PARTITIONED} — `LAG(x,1) OVER (PARTITION BY sym ORDER BY ts DESC)`.
 *             Phase 4+5 — the original triggering query shape.</li>
 *         <li>{@code LAG_PLUS_LEAD_NO_PARTITION} — `LAG(x,1) OVER () + LEAD(x,1) OVER ()`.
 *             Currently CachedWindow under both paths (Phase 6 would stream).</li>
 *         <li>{@code LAG_PLUS_LEAD_PARTITIONED} — `LAG(x,1) OVER (PARTITION BY sym) + LEAD(x,1)
 *             OVER (PARTITION BY sym)`. Currently CachedWindow under both paths (Phase 6 would
 *             stream).</li>
 *       </ul></li>
 *   <li>{@code rowCount}: total row count in the seed table.</li>
 *   <li>{@code partitionCardinality}: number of distinct symbol values (drives PARTITION BY load).
 *       For shapes without PARTITION BY this is still applied to the data shape but doesn't
 *       affect the plan.</li>
 * </ul>
 * <p>
 * A {@code @Setup(Level.Trial)} routing guard walks the compiled factory chain to confirm each
 * {@code shape}+{@code path} pair hits the expected factory class. Silent routing drift would
 * corrupt the numbers — e.g. if a planner change quietly stopped dispatching LAG_DESC_PARTITIONED
 * to DeferredEmit, STREAMING and CACHED would converge and the numbers would be meaningless.
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

    @Param({"100000", "1000000", "10000000"})
    public int rowCount;

    @Param({
            "LEAD_NO_PARTITION",
            "LAG_DESC_NO_PARTITION",
            "LEAD_PARTITIONED",
            "LAG_DESC_PARTITIONED",
            "LAG_PLUS_LEAD_NO_PARTITION",
            "LAG_PLUS_LEAD_PARTITIONED"
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
            // Consume every output row; column count varies per shape, but the first long column
            // is always present. Iterating drives the full window evaluation including any
            // end-of-cursor flush for the deferred-emit path.
            while (cursor.hasNext()) {
                bh.consume(record.getLong(0));
                // Consume the window column (1) too so the deferred-emit slot read is exercised.
                bh.consume(record.getLong(1));
                // For partitioned shapes there's a third column (the LAG/LEAD output); consume if
                // metadata says we have it.
                if (factory.getMetadata().getColumnCount() > 2) {
                    bh.consume(record.getLong(2));
                }
                if (factory.getMetadata().getColumnCount() > 3) {
                    bh.consume(record.getLong(3));
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
            case "LEAD_NO_PARTITION" ->
                    "SELECT x, ts, lead(x, 1) OVER () FROM t";
            case "LAG_DESC_NO_PARTITION" ->
                    "SELECT x, ts, lag(x, 1) OVER (ORDER BY ts DESC) FROM t";
            case "LEAD_PARTITIONED" ->
                    "SELECT x, ts, lead(x, 1) OVER (PARTITION BY sym) FROM t";
            case "LAG_DESC_PARTITIONED" ->
                    "SELECT x, ts, lag(x, 1) OVER (PARTITION BY sym ORDER BY ts DESC) FROM t";
            case "LAG_PLUS_LEAD_NO_PARTITION" ->
                    "SELECT x, ts, lag(x, 1) OVER (), lead(x, 1) OVER () FROM t";
            case "LAG_PLUS_LEAD_PARTITIONED" ->
                    "SELECT x, ts, lag(x, 1) OVER (PARTITION BY sym), lead(x, 1) OVER (PARTITION BY sym) FROM t";
            default -> throw new IllegalArgumentException("unknown shape: " + shape);
        };
    }

    private Class<?> expectedFactory() {
        if ("CACHED".equals(path)) {
            // Flag off: every LEAD-bearing query routes through CachedWindow.
            return CachedWindowRecordCursorFactory.class;
        }
        // STREAMING. Path depends on the shape.
        return switch (shape) {
            // Single-LEAD shapes (with or without PARTITION BY) stream via DeferredEmit.
            case "LEAD_NO_PARTITION", "LEAD_PARTITIONED" -> DeferredEmitWindowRecordCursorFactory.class;
            // LAG-DESC normalises to LEAD-ASC then streams via DeferredEmit (Phase 4 + Phase 5).
            case "LAG_DESC_NO_PARTITION", "LAG_DESC_PARTITIONED" -> DeferredEmitWindowRecordCursorFactory.class;
            // Mixed LAG+LEAD: cursor doesn't yet support mixed, planner falls back to CachedWindow.
            // Phase 6 would change this expectation.
            case "LAG_PLUS_LEAD_NO_PARTITION", "LAG_PLUS_LEAD_PARTITIONED" -> CachedWindowRecordCursorFactory.class;
            default -> throw new IllegalArgumentException("unknown shape: " + shape);
        };
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
        // Uniformly distributed symbols, monotonic timestamps. Step is chosen so partition cardinality
        // stays bounded by the param even when rowCount grows.
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

    static {
        // Suppress JMH warnings about referenced-but-otherwise-unused classes that we touch only for
        // routing assertions.
        @SuppressWarnings("unused")
        Class<?>[] referencedForRouting = {WindowRecordCursorFactory.class};
    }
}
