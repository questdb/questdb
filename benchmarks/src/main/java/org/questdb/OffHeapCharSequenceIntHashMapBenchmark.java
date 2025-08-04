/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.OffHeapCharSequenceIntHashMap;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class OffHeapCharSequenceIntHashMapBenchmark {

    private CharSequenceIntHashMap heapMap;
    private List<String> keys;
    @Param({"100", "1000", "10000"})
    private int mapSize;
    private OffHeapCharSequenceIntHashMap offHeapMap;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(OffHeapCharSequenceIntHashMapBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setup() {
        // Generate test keys
        keys = new ArrayList<>(mapSize);
        Rnd rnd = new Rnd();
        for (int i = 0; i < mapSize; i++) {
            keys.add("symbol_" + i + "_" + rnd.nextString(10));
        }

        // Initialize maps
        heapMap = new CharSequenceIntHashMap();
        offHeapMap = new OffHeapCharSequenceIntHashMap();

        // Pre-populate maps
        for (int i = 0; i < keys.size(); i++) {
            heapMap.put(keys.get(i), i);
            offHeapMap.put(keys.get(i), i);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (offHeapMap != null) {
            offHeapMap.close();
        }
    }

    @Benchmark
    public int testHeapMapGet() {
        int sum = 0;
        for (String key : keys) {
            sum += heapMap.get(key);
        }
        return sum;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public int testHeapMapKeyIndex() {
        int sum = 0;
        for (String key : keys) {
            sum += heapMap.keyIndex(key);
        }
        return sum;
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Measurement(iterations = 5)
    @Warmup(iterations = 1)
    public void testHeapMapMemoryChurn() {
        // Test memory churn by creating and destroying many maps
        for (int i = 0; i < 100; i++) {
            CharSequenceIntHashMap map = new CharSequenceIntHashMap();
            for (int j = 0; j < 100; j++) {
                map.put("key" + j, j);
            }
            // Map goes out of scope, eligible for GC
        }
    }

    @Benchmark
    public void testHeapMapPutAndClear() {
        heapMap.clear();
        for (int i = 0; i < keys.size(); i++) {
            heapMap.put(keys.get(i), i);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Measurement(iterations = 10)
    @Warmup(iterations = 2)
    public void testHeapMapRepeatClearAndPopulate() {
        // Simulate WAL writer usage pattern: clear and repopulate many times
        for (int round = 0; round < 100; round++) {
            heapMap.clear();
            for (int i = 0; i < keys.size(); i++) {
                heapMap.put(keys.get(i), i);
            }
        }
    }

    @Benchmark
    public int testOffHeapMapGet() {
        int sum = 0;
        for (String key : keys) {
            sum += offHeapMap.get(key);
        }
        return sum;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public int testOffHeapMapKeyIndex() {
        int sum = 0;
        for (String key : keys) {
            sum += offHeapMap.keyIndex(key);
        }
        return sum;
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Measurement(iterations = 5)
    @Warmup(iterations = 1)
    public void testOffHeapMapMemoryChurn() {
        // Test memory churn by creating and destroying many maps
        for (int i = 0; i < 100; i++) {
            try (OffHeapCharSequenceIntHashMap map = new OffHeapCharSequenceIntHashMap()) {
                for (int j = 0; j < 100; j++) {
                    map.put("key" + j, j);
                }
            } // Off-heap memory explicitly freed
        }
    }

    @Benchmark
    public void testOffHeapMapPutAndClear() {
        offHeapMap.clear();
        for (int i = 0; i < keys.size(); i++) {
            offHeapMap.put(keys.get(i), i);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Measurement(iterations = 10)
    @Warmup(iterations = 2)
    public void testOffHeapMapRepeatClearAndPopulate() {
        // Simulate WAL writer usage pattern: clear and repopulate many times
        for (int round = 0; round < 100; round++) {
            offHeapMap.clear();
            for (int i = 0; i < keys.size(); i++) {
                offHeapMap.put(keys.get(i), i);
            }
        }
    }
}