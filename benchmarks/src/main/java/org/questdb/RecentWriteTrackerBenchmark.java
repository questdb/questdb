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

import io.questdb.cairo.TableToken;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark to measure allocation behavior of different ConcurrentHashMap.compute() patterns.
 * <p>
 * Tests whether capturing lambdas allocate on each invocation.
 * <p>
 * Run with: mvn package -pl benchmarks -DskipTests && java -jar benchmarks/target/benchmarks.jar RecentWriteTrackerBenchmark
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgs = {"-Xms256m", "-Xmx256m"})
public class RecentWriteTrackerBenchmark {

    private static final int NUM_TABLES = 100;

    // Shared state for all approaches
    private final ConcurrentHashMap<TableToken, WriteStats> map = new ConcurrentHashMap<>();
    // For non-capturing approach: thread-local state holder
    private final ThreadLocal<WriteContext> writeContextThreadLocal = ThreadLocal.withInitial(WriteContext::new);
    private int index;
    private TableToken[] tableTokens;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RecentWriteTrackerBenchmark.class.getSimpleName())
                .addProfiler(GCProfiler.class)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setup() {
        tableTokens = new TableToken[NUM_TABLES];
        for (int i = 0; i < NUM_TABLES; i++) {
            tableTokens[i] = new TableToken("table" + i, "table" + i, null, i, false, false, false);
        }
        // Pre-populate the map to test the "update existing" path
        for (TableToken token : tableTokens) {
            map.put(token, new WriteStats(0, 0));
        }
    }

    @Setup(Level.Invocation)
    public void setupInvocation() {
        index = (index + 1) % NUM_TABLES;
    }

    /**
     * Inline capturing lambda (original approach before refactoring).
     */
    @Benchmark
    public void testCapturingLambdaInline(Blackhole bh) {
        TableToken tableToken = tableTokens[index];
        long timestamp = System.nanoTime();
        long rowCount = index * 100L;

        // This lambda captures timestamp and rowCount
        map.compute(tableToken, (key, existing) -> {
            if (existing == null) {
                return new WriteStats(timestamp, rowCount);
            } else {
                existing.update(timestamp, rowCount);
                return existing;
            }
        });

        bh.consume(map.get(tableToken));
    }

    /**
     * Current approach: capturing lambda with static method.
     * The lambda captures timestamp and rowCount.
     */
    @Benchmark
    public void testCapturingLambdaWithStaticMethod(Blackhole bh) {
        TableToken tableToken = tableTokens[index];
        long timestamp = System.nanoTime();
        long rowCount = index * 100L;

        // This lambda captures timestamp and rowCount
        map.compute(tableToken, (key, existing) -> recordWrite0(timestamp, rowCount, existing));

        bh.consume(map.get(tableToken));
    }

    /**
     * Alternative: avoid compute() entirely using get/putIfAbsent pattern.
     * This is lock-free for existing entries.
     */
    @Benchmark
    public void testGetThenPutIfAbsent(Blackhole bh) {
        TableToken tableToken = tableTokens[index];
        long timestamp = System.nanoTime();
        long rowCount = index * 100L;

        WriteStats stats = map.get(tableToken);
        if (stats != null) {
            // Fast path: entry exists, just update
            stats.update(timestamp, rowCount);
        } else {
            // Slow path: need to create
            WriteStats newStats = new WriteStats(timestamp, rowCount);
            WriteStats existing = map.putIfAbsent(tableToken, newStats);
            if (existing != null) {
                // Someone else inserted, update theirs
                existing.update(timestamp, rowCount);
            }
        }

        bh.consume(map.get(tableToken));
    }

    /**
     * Non-capturing approach using ThreadLocal context.
     * The lambda doesn't capture any local variables.
     */
    @Benchmark
    public void testNonCapturingWithThreadLocal(Blackhole bh) {
        TableToken tableToken = tableTokens[index];
        long timestamp = System.nanoTime();
        long rowCount = index * 100L;

        WriteContext ctx = writeContextThreadLocal.get();
        ctx.timestamp = timestamp;
        ctx.rowCount = rowCount;

        // This lambda doesn't capture local variables - it accesses ThreadLocal
        map.compute(tableToken, (key, existing) -> {
            WriteContext c = writeContextThreadLocal.get();
            if (existing == null) {
                return new WriteStats(c.timestamp, c.rowCount);
            } else {
                existing.update(c.timestamp, c.rowCount);
                return existing;
            }
        });

        bh.consume(map.get(tableToken));
    }

    private static WriteStats recordWrite0(long timestamp, long rowCount, WriteStats existing) {
        if (existing == null) {
            return new WriteStats(timestamp, rowCount);
        } else {
            existing.update(timestamp, rowCount);
            return existing;
        }
    }

    // Context holder for ThreadLocal approach
    private static class WriteContext {
        long rowCount;
        long timestamp;
    }

    // Simple WriteStats class for benchmarking
    public static class WriteStats {
        private volatile long rowCount;
        private volatile long timestamp;

        WriteStats(long timestamp, long rowCount) {
            this.timestamp = timestamp;
            this.rowCount = rowCount;
        }

        void update(long timestamp, long rowCount) {
            this.timestamp = timestamp;
            this.rowCount = rowCount;
        }
    }
}
