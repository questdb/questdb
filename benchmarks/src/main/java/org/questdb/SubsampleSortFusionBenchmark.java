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
 * Isolates the cost of SUBSAMPLE's <b>fused</b> in-operator sort
 * ({@code SubsampleRecordCursor.nativeSortBufferByTimestamp}) against a
 * <b>composed</b> engine sort ({@code ORDER BY ts}) feeding SUBSAMPLE with its
 * internal sort skipped.
 * <p>
 * Both modes:
 * <ul>
 *   <li>drive an <b>unordered</b> parallel SAMPLE BY (async GROUP BY, one bucket
 *       per row, {@code workers>1}) whose output is hash-iteration order and has
 *       {@code timestampIndex == -1} — so SUBSAMPLE takes its buffered fallback
 *       path in <em>both</em> modes (no fast path in either);</li>
 *   <li>sort the same number of rows exactly once.</li>
 * </ul>
 * The only difference is <b>where</b> the sort runs:
 * <ul>
 *   <li>{@code FUSED}: input is unordered → SUBSAMPLE's {@code isSorted} tracking
 *       trips and it runs its own {@code nativeSortBufferByTimestamp} (a
 *       hand-rolled index-array quicksort + Java reshuffle loop).</li>
 *   <li>{@code COMPOSED}: an inner {@code ORDER BY ts} sorts via the engine's
 *       sort factory; SUBSAMPLE sees ascending input, {@code isSorted} stays true,
 *       and it skips its internal sort.</li>
 * </ul>
 * COMPOSED is measured <em>conservatively</em>: it still pays a second
 * materialization (engine sort buffer + SUBSAMPLE's fallback buffer) that the
 * real fast-path composition would remove. If COMPOSED is neutral-or-better even
 * with that penalty, the fused sort is not carrying its weight.
 * <p>
 * A {@code @Setup} routing guard confirms each mode's factory chain (SUBSAMPLE
 * present in both; a Sort* factory below SUBSAMPLE only in COMPOSED) so silent
 * routing drift can't corrupt the numbers.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
public class SubsampleSortFusionBenchmark {

    private static final String START_TS = "2024-01-01T00:00:00.000000Z";
    // one row per bucket: STEP == SAMPLE BY width, so SUBSAMPLE's input row count
    // (= number of buckets to sort) tracks rowCount.
    private static final long STEP_MICROS = 1_000_000L; // 1s, matches SAMPLE BY 1s

    // FUSED    : raw unordered SAMPLE BY -> SUBSAMPLE fallback path (useDirectAccess=false):
    //            full-row RecordChain + compact 24-byte (rowid,ts,value) buffer sorted in place
    //            by SUBSAMPLE's own nativeSortBufferByTimestamp.
    // COMPOSED : inner ORDER BY ts sorts via the engine (EncodedSort); that ordered, random-access,
    //            forward cursor makes SUBSAMPLE take its fast path (useDirectAccess=true): it reads
    //            rows via recordAt and never runs its internal sort. This is the "compose the sort as
    //            a pipeline operator" design.
    // Both materialize the full row set once; the isolated variable is which sort runs (SUBSAMPLE's
    // compact fused sort vs the engine's general full-row sort).
    @Param({"FUSED", "COMPOSED"})
    public String mode;

    @Param({"1000000", "5000000"})
    public int rowCount;

    @Param({"1000"})
    public int targetPoints;

    // >1 forces the async/parallel GROUP BY path whose hash-ordered output is
    // genuinely unordered (and has timestampIndex == -1).
    @Param({"8"})
    public int workers;

    private SqlCompilerImpl compiler;
    private SqlExecutionContext ctx;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private Path tempRoot;
    private WorkerPool workerPool;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SubsampleSortFusionBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void run(Blackhole bh) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                bh.consume(record.getTimestamp(0));
                bh.consume(record.getDouble(1));
            }
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        tempRoot = java.nio.file.Files.createTempDirectory("subsamplesortfusionbench-");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(tempRoot.toString());
        engine = new CairoEngine(configuration);

        workerPool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "subsamplesortfusionbench";
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

        factory = compiler.compile(buildSql(), ctx).getRecordCursorFactory();
        assertRouting(factory);
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

    private void assertRouting(RecordCursorFactory root) {
        RecordCursorFactory subsample = null;
        RecordCursorFactory cur = root;
        while (cur != null) {
            if (cur.getClass().getSimpleName().equals("SubsampleRecordCursorFactory")) {
                subsample = cur;
                break;
            }
            RecordCursorFactory next = cur.getBaseFactory();
            if (next == cur) {
                break;
            }
            cur = next;
        }
        if (subsample == null) {
            throw new IllegalStateException("routing drift: SubsampleRecordCursorFactory not found. mode=" + mode
                    + " root=" + root.getClass().getSimpleName());
        }
        // Is there a Sort* factory BELOW the subsample factory?
        boolean sortBelow = false;
        cur = subsample.getBaseFactory();
        while (cur != null) {
            if (cur.getClass().getSimpleName().contains("Sort")) {
                sortBelow = true;
                break;
            }
            RecordCursorFactory next = cur.getBaseFactory();
            if (next == cur) {
                break;
            }
            cur = next;
        }
        final boolean expectSortBelow = !"FUSED".equals(mode);
        if (sortBelow != expectSortBelow) {
            throw new IllegalStateException("routing drift: mode=" + mode
                    + " expected sortBelowSubsample=" + expectSortBelow
                    + " but found=" + sortBelow
                    + " root=" + root.getClass().getSimpleName());
        }
        // Hard-confirm the fast path engages ONLY for COMPOSED_FASTPATH; otherwise the
        // "single-materialization" claim (and the whole isolation) is invalid.
        try {
            java.lang.reflect.Field cursorField = subsample.getClass().getDeclaredField("cursor");
            cursorField.setAccessible(true);
            Object cursorObj = cursorField.get(subsample);
            java.lang.reflect.Field udaField = cursorObj.getClass().getDeclaredField("useDirectAccess");
            udaField.setAccessible(true);
            boolean uda = udaField.getBoolean(cursorObj);
            boolean expectUda = "COMPOSED".equals(mode);
            if (uda != expectUda) {
                throw new IllegalStateException("routing drift: mode=" + mode
                        + " expected useDirectAccess=" + expectUda + " but got " + uda);
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("useDirectAccess reflection failed for mode=" + mode, e);
        }
    }

    private String buildSql() {
        if ("COMPOSED".equals(mode)) {
            // pre-order by ts; the sorted random-access cursor triggers SUBSAMPLE's fast path.
            return "SELECT ts, avg FROM ("
                    + "SELECT ts, avg(d) avg FROM tab SAMPLE BY 1s ORDER BY ts"
                    + ") SUBSAMPLE lttb(avg, " + targetPoints + ")";
        }
        // FUSED
        return "SELECT ts, avg(d) avg FROM tab SAMPLE BY 1s SUBSAMPLE lttb(avg, " + targetPoints + ")";
    }

    private void seedTable() throws SqlException {
        engine.execute(
                "CREATE TABLE tab (d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                ctx
        );
        engine.execute(
                "INSERT INTO tab SELECT rnd_double() AS d, "
                        + "timestamp_sequence('" + START_TS + "'::timestamp, " + STEP_MICROS + ") AS ts "
                        + "FROM long_sequence(" + rowCount + ")",
                ctx
        );
    }
}
