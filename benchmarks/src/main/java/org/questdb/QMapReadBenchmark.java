/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package org.questdb;

import com.questdb.cairo.SingleColumnType;
import com.questdb.cairo.map.CompactMap;
import com.questdb.cairo.map.FastMap;
import com.questdb.cairo.map.MapKey;
import com.questdb.cairo.map.MapValue;
import com.questdb.std.Rnd;
import com.questdb.store.ColumnType;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class QMapReadBenchmark {

    private static final int N = 5000000;
    private static final double loadFactor = 0.5;
    private static final int M = 25;
    private static final Rnd rnd = new Rnd();
    private static CompactMap qmap = new CompactMap(1024 * 1024, new SingleColumnType(ColumnType.STRING), new SingleColumnType(ColumnType.LONG), N, loadFactor);
    private static FastMap map = new FastMap(1024 * 1024, new SingleColumnType(ColumnType.STRING), new SingleColumnType(ColumnType.LONG), N, loadFactor);

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(QMapReadBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void reset() {
        System.out.print(" [q=" + qmap.size() + ", l=" + map.size() + ", cap=" + qmap.getKeyCapacity() + "] ");
    }

    @Benchmark
    public MapValue testDirectMap() {
        MapKey key = map.withKey();
        key.putStr(rnd.nextChars(M));
        return key.findValue();
    }

    @Benchmark
    public MapValue testQMap() {
        MapKey key = qmap.withKey();
        key.putStr(rnd.nextChars(M));
        return key.findValue();
    }

    static {
        for (int i = 0; i < N; i++) {
            MapKey key = qmap.withKey();
            key.putStr(rnd.nextChars(M));
            MapValue value = key.createValue();
            value.putLong(0, i);
        }

        for (int i = 0; i < N; i++) {
            MapKey key = map.withKey();
            key.putStr(rnd.nextChars(M));
            MapValue values = key.createValue();
            values.putLong(0, 20);
        }

    }
}
