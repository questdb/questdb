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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.OrderedMap;
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
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class OrderedMapAggregateBenchmark {

    private static final int keyCapacity = 1024;
    private static final double loadFactor = 0.7;
    private static final int nKeys = 100;
    private static final int pageSize = 32 * 1024;
    private final OrderedMap map;
    private final Rnd rnd = new Rnd();

    public OrderedMapAggregateBenchmark() {
        SingleColumnType keyTypes = new SingleColumnType(ColumnType.LONG);
        SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);
        map = new OrderedMap(pageSize, keyTypes, valueTypes, keyCapacity, loadFactor, Integer.MAX_VALUE);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(OrderedMapAggregateBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void reset() {
        map.close();
        map.reopen();
        rnd.reset();
    }

    @Benchmark
    public void testAggregateConst() {
        MapKey key = map.withKey();
        key.putLong(rnd.nextLong(nKeys));
        MapValue values = key.createValue();
        values.minLong(0, 42);
    }

    @Benchmark
    public void testAggregateRandom() {
        MapKey key = map.withKey();
        key.putLong(rnd.nextLong(nKeys));
        MapValue values = key.createValue();
        values.minLong(0, Numbers.LONG_NULL + rnd.nextLong() & 1);
    }
}
