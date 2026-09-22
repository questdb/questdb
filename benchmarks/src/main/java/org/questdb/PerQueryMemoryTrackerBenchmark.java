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
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Os;
import io.questdb.std.PerQueryMemoryTrackerProvider;
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

import java.util.concurrent.TimeUnit;

/**
 * Measures the per-allocation overhead the per-query memory tracker adds on top
 * of a plain native {@code malloc}/{@code free} pair. This isolates the cost
 * the per-query memory limit feature pays even when a deployment leaves all
 * limits at the default {@code 0} (unlimited), which is the case the PR #7184
 * review asked to quantify.
 * <p>
 * Three modes are compared at each allocation size:
 * <ul>
 *     <li>{@code nullTracker} -- the pre-feature path: only the global RSS
 *     counter is updated ({@code Unsafe.malloc(size, tag)}).</li>
 *     <li>{@code unlimitedTracker} -- a tracker with limit {@code 0}, what every
 *     upgraded deployment pays: the limit check short-circuits, but the
 *     per-query counter still does one extra atomic add on malloc and one on
 *     free.</li>
 *     <li>{@code limitedTracker} -- a tracker with a large, never-breached
 *     limit, what an opted-in deployment pays: the limit comparison runs in
 *     addition to the counter update.</li>
 * </ul>
 * The per-query counter is per-tracker, so it is uncontended for a
 * single-threaded query; a parallel query shares one tracker across its worker
 * threads, where the counter becomes a shared atomic among those workers (the
 * global RSS counter is already a process-wide shared atomic in every mode).
 * This benchmark measures the single-threaded per-call cost; it does not model
 * the parallel-query contention.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class PerQueryMemoryTrackerBenchmark {

    private static final int MEMORY_TAG = MemoryTag.NATIVE_DEFAULT;

    @Param({"32", "1024", "65536"})
    private int size;

    private CairoConfiguration limitedConfig;
    private PerQueryMemoryTrackerProvider limitedProvider;
    private MemoryTracker limitedTracker;
    private CairoConfiguration unlimitedConfig;
    private PerQueryMemoryTrackerProvider unlimitedProvider;
    private MemoryTracker unlimitedTracker;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(PerQueryMemoryTrackerBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setup() {
        Os.init();
        unlimitedConfig = new DefaultCairoConfiguration(".");
        unlimitedProvider = new PerQueryMemoryTrackerProvider(unlimitedConfig);
        unlimitedTracker = unlimitedProvider.acquire(AllowAllSecurityContext.INSTANCE, 1, MemoryTrackerWorkload.QUERY);

        // A limit large enough never to be breached by a single allocation, so the
        // limit check runs its comparison on every malloc but always passes.
        limitedConfig = new DefaultCairoConfiguration(".") {
            @Override
            public long getQueryMemoryLimitBytes() {
                return Long.MAX_VALUE;
            }
        };
        limitedProvider = new PerQueryMemoryTrackerProvider(limitedConfig);
        limitedTracker = limitedProvider.acquire(AllowAllSecurityContext.INSTANCE, 2, MemoryTrackerWorkload.QUERY);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        unlimitedTracker.close();
        unlimitedProvider.close();
        limitedTracker.close();
        limitedProvider.close();
    }

    @Benchmark
    public long limitedTracker() {
        long ptr = Unsafe.malloc(size, MEMORY_TAG, limitedTracker);
        Unsafe.free(ptr, size, MEMORY_TAG, limitedTracker);
        return ptr;
    }

    @Benchmark
    public long nullTracker() {
        long ptr = Unsafe.malloc(size, MEMORY_TAG);
        Unsafe.free(ptr, size, MEMORY_TAG);
        return ptr;
    }

    @Benchmark
    public long unlimitedTracker() {
        long ptr = Unsafe.malloc(size, MEMORY_TAG, unlimitedTracker);
        Unsafe.free(ptr, size, MEMORY_TAG, unlimitedTracker);
        return ptr;
    }
}
