/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.map.CompactMap;
import io.questdb.cairo.map.FastMap;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
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
public class QMapWriteBenchmark {

    private static final double loadFactor = 0.5;
    private static final int M = 25;
    private static CompactMap qmap = new CompactMap(1024 * 1024, new SingleColumnType(ColumnType.STRING), new SingleColumnType(ColumnType.LONG), 64, loadFactor, 1024, Integer.MAX_VALUE);
    private static FastMap map = new FastMap(1024 * 1024, new SingleColumnType(ColumnType.STRING), new SingleColumnType(ColumnType.LONG), 64, loadFactor, 1024);


    private final Rnd rnd = new Rnd();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(QMapWriteBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public CharSequence baseline() {
        return rnd.nextChars(M);
    }

    @Setup(Level.Iteration)
    public void reset() {
        System.out.print(" [q=" + qmap.size() + ", l=" + map.size() + ", cap=" + qmap.getKeyCapacity() + "] ");
        map.clear();
        qmap.clear();
        rnd.reset();
    }

    @Benchmark
    public void testDirectMap() {
        MapKey key = map.withKey();
        key.putStr(rnd.nextChars(M));
        MapValue values = key.createValue();
        values.putLong(0, 20);
    }

    @Benchmark
    public void testQMap() {
        MapKey key = qmap.withKey();
        key.putStr(rnd.nextChars(M));
        MapValue value = key.createValue();
        value.putLong(0, 20);
    }
}
