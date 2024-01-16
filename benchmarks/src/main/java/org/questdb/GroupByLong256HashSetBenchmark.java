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
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByLong128HashSet;
import io.questdb.griffin.engine.groupby.GroupByLong256HashSet;
import io.questdb.std.Long256;
import io.questdb.std.Numbers;
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
public class GroupByLong256HashSetBenchmark {

    private static final long N = 1_000_000;
    private static final GroupByAllocator allocator = new GroupByAllocator(new DefaultCairoConfiguration(null) {
        @Override
        public long getGroupByAllocatorDefaultChunkSize() {
            return 128 * 1024;
        }

        @Override
        public long getGroupByAllocatorMaxChunkSize() {
            return Numbers.SIZE_1GB * 8;
        }
    });
    private static final OrderedMap fmap = new OrderedMap(1024 * 1024, new SingleColumnType(ColumnType.LONG256), null, 16, 0.7f, Integer.MAX_VALUE);
    private static final GroupByLong256HashSet gbset = new GroupByLong256HashSet(16, 0.7, 0);
    private static long ptr = 0;
    private final Rnd rnd = new Rnd();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(GroupByLong256HashSetBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public long baseline() {
        rnd.nextLong(N);
        rnd.nextLong(N);
        rnd.nextLong(N);
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
        key.putLong256(rnd.nextLong(N), rnd.nextLong(N), rnd.nextLong(N), rnd.nextLong(N));
        key.createValue();
    }

    @Benchmark
    public void testGroupByLong256HashSet() {
        long l0 = rnd.nextLong(N);
        long l1 = rnd.nextLong(N);
        long l2 = rnd.nextLong(N);
        long l3 = rnd.nextLong(N);
        int index = gbset.of(ptr).keyIndex(l0, l1, l2, l3);
        if (index >= 0) {
            gbset.addAt(index, l0, l1, l2, l3);
            ptr = gbset.ptr();
        }
    }
}
