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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.Unordered2Map;
import io.questdb.cairo.map.Unordered4Map;
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
public class MapWriteShortBenchmark {

    private static final double loadFactor = 0.7;
    private static final Unordered2Map unordered2map = new Unordered2Map(new SingleColumnType(ColumnType.SHORT), new SingleColumnType(ColumnType.LONG));
    private static final Unordered4Map unordered4map = new Unordered4Map(new SingleColumnType(ColumnType.SHORT), new SingleColumnType(ColumnType.LONG), 64, loadFactor, Integer.MAX_VALUE);
    private final Rnd rnd = new Rnd();
    // aim for L1, L2
    @Param({"3000", "30000"})
    public int size;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MapWriteShortBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void reset() {
        rnd.reset();

        unordered2map.clear();
        unordered4map.clear();
    }

    @Benchmark
    public void testUnordered2Map() {
        MapKey key = unordered2map.withKey();
        key.putInt(rnd.nextShort() % size);
        MapValue values = key.createValue();
        values.putLong(0, rnd.nextLong());
    }

    @Benchmark
    public void testUnordered4Map() {
        MapKey key = unordered4map.withKey();
        key.putInt(rnd.nextShort() % size);
        MapValue values = key.createValue();
        values.putLong(0, rnd.nextLong());
    }
}
