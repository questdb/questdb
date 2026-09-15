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
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.engine.table.LttbAlgorithm;
import io.questdb.griffin.engine.table.M4Algorithm;
import io.questdb.griffin.engine.table.MinMaxAlgorithm;
import io.questdb.griffin.engine.table.SubsampleAlgorithm;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Isolates selection from scanning and buffering. A real network breaker exercises clock and
 * cancellation checks; fd=-1 excludes socket syscalls. Dense targets, empty buckets and tiny
 * segments expose accidental per-bucket/per-segment real checks that sparse targets can hide.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(2)
public class SubsampleAlgorithmBenchmark {
    @Param({"1000001"})
    public int rows;

    @Param({"m4_sparse", "m4_dense", "m4_empty", "minmax_sparse", "lttb_sparse", "lttb_dense", "lttb_tiny_gaps", "lttb_rescale"})
    public String scenario;

    private SubsampleAlgorithm algorithm;
    private long buffer;
    private NetworkSqlExecutionCircuitBreaker circuitBreaker;
    private CairoEngine engine;
    private DirectLongList selected;
    private int target;
    private Path tempRoot;

    @Benchmark
    public void run(Blackhole blackhole) {
        circuitBreaker.resetTimer();
        algorithm.select(buffer, rows, target, false, selected, circuitBreaker);
        blackhole.consume(selected.size());
        blackhole.consume(selected.get(selected.size() / 2));
    }

    @Setup
    public void setUp() throws Exception {
        tempRoot = Files.createTempDirectory("subsample-algorithm-bench-");
        engine = new CairoEngine(new DefaultCairoConfiguration(tempRoot.toString()));
        circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine, new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public int getCircuitBreakerThrottle() {
                return 2_000_000;
            }
        });
        circuitBreaker.setCancelledFlag(new AtomicBoolean());
        final boolean hasTinyGaps = scenario.equals("lttb_tiny_gaps");
        final boolean hasEmptyBuckets = scenario.equals("m4_empty");
        final boolean hasHugeValues = scenario.equals("lttb_rescale");
        target = scenario.endsWith("sparse") ? 500 : rows / 2;
        algorithm = scenario.startsWith("m4") ? M4Algorithm.INSTANCE
                : scenario.startsWith("minmax") ? MinMaxAlgorithm.INSTANCE
                : new LttbAlgorithm(hasTinyGaps ? 1 : 0);
        selected = new DirectLongList(rows, MemoryTag.NATIVE_DEFAULT);
        buffer = Unsafe.malloc((long) rows * SubsampleAlgorithm.ENTRY_SIZE, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < rows; i++) {
            final long ts = hasEmptyBuckets ? (i == rows - 1 ? 1_000_000_000L : 0)
                    : hasTinyGaps ? (long) (i / 8) * 1000 + i % 8 : i;
            final long address = buffer + (long) i * SubsampleAlgorithm.ENTRY_SIZE;
            Unsafe.getUnsafe().putLong(address, ts);
            Unsafe.getUnsafe().putDouble(address + 8, hasHugeValues ? Double.MAX_VALUE : i % 97);
        }
        algorithm.select(buffer, rows, target, false, selected, circuitBreaker);
        if (selected.size() == 0 || selected.size() > rows) {
            throw new IllegalStateException("invalid selection size: " + selected.size());
        }
        long previous = -1;
        for (long i = 0; i < selected.size(); i++) {
            final long index = selected.get(i);
            if (index <= previous || index >= rows) {
                throw new IllegalStateException("invalid selected index: " + index);
            }
            previous = index;
        }
    }

    @TearDown
    public void tearDown() throws Exception {
        if (algorithm instanceof LttbAlgorithm lttb) {
            lttb.close();
        }
        selected = Misc.free(selected);
        if (buffer != 0) {
            buffer = Unsafe.free(buffer, (long) rows * SubsampleAlgorithm.ENTRY_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
        circuitBreaker = Misc.free(circuitBreaker);
        engine = Misc.free(engine);
        if (tempRoot != null) {
            try (var paths = Files.walk(tempRoot)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(path);
                }
            }
        }
    }
}
