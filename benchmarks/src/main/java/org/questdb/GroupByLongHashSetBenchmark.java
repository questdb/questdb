/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.FastMap;
import io.questdb.cairo.map.MapKey;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByLongHashSet;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class GroupByLongHashSetBenchmark {

    private static final long N = 1_000_000;
    private static final GroupByAllocator allocator = new GroupByAllocator(new DefaultCairoConfiguration(null) {
        @Override
        public int getGroupByAllocatorDefaultChunkSize() {
            return 128 * 1024;
        }
    });
    private static final FastMap fmap = new FastMap(1024 * 1024, new SingleColumnType(ColumnType.LONG), null, 16, 0.7f, Integer.MAX_VALUE);
    private static final GroupByLongHashSet gbset = new GroupByLongHashSet(16, 0.7, 0);
    private static long ptr = 0;
    private final Rnd rnd = new Rnd();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(GroupByLongHashSetBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public long baseline() {
        return rnd.nextLong(N);
    }

    @Setup(Level.Iteration)
    public void reset() {
        fmap.close();
        fmap.reopen();
        allocator.close();
        gbset.setAllocator(allocator);
        ptr = 0;
        rnd.reset();
    }

    @Benchmark
    public void testFastMap() {
        MapKey key = fmap.withKey();
        key.putLong(rnd.nextLong(N));
        key.createValue();
    }

    @Benchmark
    public void testGroupByLongHashSet() {
        long value = rnd.nextLong(N);
        int index = gbset.of(ptr).keyIndex(value);
        if (index >= 0) {
            gbset.addAt(index, value);
            ptr = gbset.ptr();
        }
    }
}
