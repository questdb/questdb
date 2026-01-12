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

import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByUtf8SequenceLongHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.Utf8SequenceLongHashMap;
import io.questdb.std.str.Utf8StringSink;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class GroupByUtf8SequenceLongHashMapBenchmark {
    private static final GroupByAllocator allocator = new FastGroupByAllocator(128 * 1024, Numbers.SIZE_1GB);
    private static final int initialCapacity = 64;
    private static final double loadFactor = 0.5;
    private static final GroupByUtf8SequenceLongHashMap groupByUtf8SequenceLongHashMap = new GroupByUtf8SequenceLongHashMap(initialCapacity, loadFactor, 0, 0);
    private static final Rnd rnd = new Rnd();
    private static final Utf8SequenceLongHashMap utf8SequenceLongHashMap = new Utf8SequenceLongHashMap(initialCapacity);
    private static long mapPtr = 0;
    @Param({"10", "50", "200", "1000"})
    public int size;
    Utf8StringSink sink = new Utf8StringSink();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(GroupByUtf8SequenceLongHashMapBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void reset() {
        allocator.clear();
        groupByUtf8SequenceLongHashMap.setAllocator(allocator);
        mapPtr = 0;
        rnd.reset();
        sink.clear();
        utf8SequenceLongHashMap.clear();
    }

    @Benchmark
    public void testGroupByUtf8SequenceLongHashMap() {
        sink.clear();
        rnd.nextUtf8Str(size, sink);
        long value = rnd.nextLong(size);
        groupByUtf8SequenceLongHashMap.of(mapPtr).put(sink, value);
        mapPtr = groupByUtf8SequenceLongHashMap.ptr();
    }

    @Benchmark
    public void testUtf8SequenceLongHashMap() {
        sink.clear();
        rnd.nextUtf8Str(size, sink);
        long value = rnd.nextLong(size);
        utf8SequenceLongHashMap.put(sink, value);
    }
}
