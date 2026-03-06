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

import io.questdb.std.DirectLongLongHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class DirectLongLongHashMapRemoveBenchmark {
    
    private static final double LOAD_FACTOR = 0.5;
    private static final long NO_ENTRY_KEY = -1;
    private static final long NO_ENTRY_VALUE = -1;
    
    private DirectLongLongHashMap map;
    private Rnd rnd;
    private long[] keysToRemove;
    private int removeIndex;

    @Param({"8192"}) // , "131072", "1048576", "4194304"
    public int mapSize;

    @Param({"RANDOM", "CLUSTERED"})
    public RemovalPattern pattern;

    public enum RemovalPattern {
        RANDOM,      // Remove random keys
        CLUSTERED,   // Remove keys that hash to the same location
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(DirectLongLongHashMapRemoveBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        rnd = new Rnd();
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        if (map != null)
            map.close();
        map = new DirectLongLongHashMap(mapSize, LOAD_FACTOR, NO_ENTRY_KEY, NO_ENTRY_VALUE, MemoryTag.NATIVE_DEFAULT);
        rnd.reset();

        keysToRemove = new long[mapSize];

        switch (pattern) {
            case RANDOM:
                setupRandomPattern();
                break;
            case CLUSTERED:
                setupClusteredPattern();
                break;
        }

        removeIndex = 0;
    }

    private void setupRandomPattern() {
        for (int i = 0; i < mapSize; i++) {
            long key = rnd.nextLong();
            if (key == NO_ENTRY_KEY) key++;
            map.put(key, i);
            keysToRemove[i] = key;
        }
    }

    private void setupClusteredPattern() {
        // Worst-case scenario. Keys should hash to similar locations with long probe sequences
        int capacity = map.capacity();
        for (int i = 0; i < mapSize; i++) {
            long key = (long) i * capacity;
            map.put(key, key);
            keysToRemove[i] = key;
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (map != null) {
            map.close();
        }
    }

    @Benchmark
    public void testRemoveAt() {
        long key = keysToRemove[removeIndex];
        long index = map.keyIndex(key);

        // Remove the key and re-insert it back to maintain map size for consistent benchmarking
        map.removeAt(index);
        map.put(key, removeIndex);
        // Cycle through keys
        removeIndex = (removeIndex + 1) & (mapSize - 1);
    }
}