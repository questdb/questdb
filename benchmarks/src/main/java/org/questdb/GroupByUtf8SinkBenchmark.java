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

import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByUtf8Sink;
import io.questdb.griffin.engine.groupby.StableAwareUtf8StringHolder;
import io.questdb.std.Hash;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * The per-row work of the VARCHAR GROUP BY functions on their buffers, over a set of values that a
 * GROUP BY keeps in its map: max() and min() compare a row with the value they hold and replace it,
 * and an operator that reads the result hashes the bytes at ptr().
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class GroupByUtf8SinkBenchmark {
    private static final int VALUE_COUNT = 1024;
    private final long[] holderPtrs = new long[VALUE_COUNT];
    private final long[] sinkPtrs = new long[VALUE_COUNT];
    private final Utf8StringSink[] values = new Utf8StringSink[VALUE_COUNT];
    @Param({"8", "64", "1024"})
    public int size;
    private GroupByAllocator allocator;
    private StableAwareUtf8StringHolder holder;
    private GroupByUtf8Sink sink;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(GroupByUtf8SinkBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    // max(): read the held value, compare it with the row, keep the larger
    @Benchmark
    public long compareAndReplace() {
        long acc = 0;
        for (int i = 0; i < VALUE_COUNT; i++) {
            final Utf8StringSink row = values[(i + 1) & (VALUE_COUNT - 1)];
            sink.of(sinkPtrs[i]);
            if (Utf8s.compare(sink, row) < 0) {
                sink.clear();
                sink.put(row);
                sinkPtrs[i] = sink.ptr();
            }
            acc += sink.size();
        }
        return acc;
    }

    // a hash map keyed by the result of first() or last(): hash the bytes at ptr()
    @Benchmark
    public long hashHolderValue() {
        long acc = 0;
        for (int i = 0; i < VALUE_COUNT; i++) {
            holder.of(holderPtrs[i]);
            acc += Hash.hashMem64(holder.ptr(), holder.size());
        }
        return acc;
    }

    // a hash map keyed by the result of max() or min(): hash the bytes at ptr()
    @Benchmark
    public long hashSinkValue() {
        long acc = 0;
        for (int i = 0; i < VALUE_COUNT; i++) {
            sink.of(sinkPtrs[i]);
            acc += Hash.hashMem64(sink.ptr(), sink.size());
        }
        return acc;
    }

    @Setup(Level.Iteration)
    public void setUp() {
        allocator = new FastGroupByAllocator(128 * 1024, Numbers.SIZE_1GB);
        sink = new GroupByUtf8Sink();
        sink.setAllocator(allocator);
        holder = new StableAwareUtf8StringHolder();
        holder.setAllocator(allocator);
        final Rnd rnd = new Rnd();
        for (int i = 0; i < VALUE_COUNT; i++) {
            values[i] = new Utf8StringSink();
            rnd.nextUtf8AsciiStr(size, values[i]);
            sink.of(0).put(values[i]);
            sinkPtrs[i] = sink.ptr();
            holder.of(0).clearAndSet(values[i]);
            holderPtrs[i] = holder.colouredPtr();
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        allocator.close();
    }
}
