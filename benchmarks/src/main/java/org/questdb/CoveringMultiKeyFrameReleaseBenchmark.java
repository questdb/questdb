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
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
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

import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

/**
 * Streams the page frames of a multi-key covering index scan ({@code sym IN (...)}) the way
 * QWP egress does: one frame at a time, reading every row of a covered column. With release,
 * the caller frees each frame's buffers before it pulls the next one
 * ({@code releaseConsumedFrames()}); without, the cursor keeps every frame until it closes.
 * 8M rows in four day partitions, 4M of them match.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@Fork(1)
public class CoveringMultiKeyFrameReleaseBenchmark {

    private static final int ROWS = 8_000_000;
    private static final String SQL = "SELECT price, qty, i, f, ts FROM t WHERE sym IN ('A', 'nope')";

    @Param({"100000", "1000000"})
    public int pageFrameRows;

    @Param({"false", "true"})
    public boolean release;

    private SqlCompilerImpl compiler;
    private SqlExecutionContext context;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private Path tempRoot;

    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder()
                .include(CoveringMultiKeyFrameReleaseBenchmark.class.getSimpleName())
                .build();
        new Runner(options).run();
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        tempRoot = java.nio.file.Files.createTempDirectory("covering-frame-release-bench-");
        final int frameRows = pageFrameRows;
        final CairoConfiguration configuration = new DefaultCairoConfiguration(tempRoot.toString()) {
            @Override
            public int getSqlPageFrameMaxRows() {
                return frameRows;
            }
        };
        engine = new CairoEngine(configuration);
        context = new SqlExecutionContextImpl(engine, 1)
                .with(
                        configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null,
                        null,
                        -1,
                        null
                );
        compiler = new SqlCompilerImpl(engine);
        engine.execute("CREATE TABLE t (sym SYMBOL, price DOUBLE, qty LONG, i INT, f FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", context);
        engine.execute("INSERT INTO t SELECT CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END, x * 0.5, x, x::INT, (x % 1000)::FLOAT, (x * 43_200L)::TIMESTAMP FROM long_sequence(" + ROWS + ")", context);
        engine.execute("ALTER TABLE t ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty, i, f)", context);
        factory = compiler.compile(SQL, context).getRecordCursorFactory();
        // qty of the 4M even rows: 2 * (1 + ... + 4M)
        final long expected = (long) ROWS / 2 * (ROWS / 2 + 1);
        final long checksum = stream();
        if (checksum != expected) {
            throw new IllegalStateException("checksum " + checksum + " != " + expected);
        }
    }

    @Benchmark
    public long stream() throws SqlException {
        long checksum = 0;
        try (PageFrameCursor cursor = factory.getPageFrameCursor(context, PartitionFrameCursorFactory.ORDER_ASC)) {
            while (true) {
                if (release) {
                    cursor.releaseConsumedFrames();
                }
                final PageFrame frame = cursor.next(0);
                if (frame == null) {
                    return checksum;
                }
                final long qty = frame.getPageAddress(1);
                for (long r = 0, n = frame.getPartitionHi() - frame.getPartitionLo(); r < n; r++) {
                    checksum += Unsafe.getLong(qty + r * Long.BYTES);
                }
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        factory = Misc.free(factory);
        compiler = Misc.free(compiler);
        engine = Misc.free(engine);
        try (var paths = java.nio.file.Files.walk(tempRoot)) {
            paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }
}
