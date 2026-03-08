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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.*;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class OrderedMapMergeBenchmark {

    private static final int commonKeysRatio = 10;
    private static final int keyCapacity = 1024;
    private static final double loadFactor = 0.7;
    private static final int nKeys = 1_500_000;
    private static final int nMaps = 64;
    private static final int pageSize = 32 * 1024;
    private final OrderedMap destMap;
    private final MapValueMergeFunction mergeFunction = (destValue, srcValue) -> {
        destValue.addLong(0, srcValue.getLong(0));
        destValue.addLong(1, srcValue.getLong(1));
        destValue.addDouble(2, srcValue.getDouble(2));
        destValue.addLong(3, srcValue.getLong(3));
    };
    private final ObjList<OrderedMap> srcMaps = new ObjList<>(nMaps);

    public OrderedMapMergeBenchmark() {
        ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.LONG);
        keyTypes.add(ColumnType.INT);
        ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(ColumnType.LONG);
        valueTypes.add(ColumnType.LONG);
        valueTypes.add(ColumnType.DOUBLE);
        valueTypes.add(ColumnType.LONG);
        destMap = new OrderedMap(pageSize, keyTypes, valueTypes, keyCapacity, loadFactor, Integer.MAX_VALUE);
        for (int i = 0; i < nMaps; i++) {
            srcMaps.add(new OrderedMap(pageSize, keyTypes, valueTypes, keyCapacity, loadFactor, Integer.MAX_VALUE));
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(OrderedMapMergeBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Setup(Level.Invocation)
    public void reset() {
        destMap.close();
        destMap.reopen();
        for (int i = 0; i < nMaps; i++) {
            srcMaps.get(i).close();
            srcMaps.get(i).reopen();
        }

        for (int i = 0; i < nMaps; i++) {
            Rnd rnd = new Rnd(i + 1, i + 1);
            for (int j = 0; j < nKeys / nMaps; j++) {
                MapKey key = srcMaps.get(i).withKey();
                if (j % commonKeysRatio == 0) {
                    key.putLong(j);
                    key.putInt(j);
                } else {
                    key.putLong(rnd.nextLong());
                    key.putInt(rnd.nextInt());
                }
                MapValue value = key.createValue();
                value.putLong(0, j);
                value.putLong(1, j);
                value.putDouble(2, j);
                value.putLong(3, j);
            }
        }

        Rnd rnd = new Rnd();
        for (int j = 0; j < nKeys / nMaps; j++) {
            MapKey key = destMap.withKey();
            if (j % commonKeysRatio == 0) {
                key.putLong(j);
                key.putInt(j);
            } else {
                key.putLong(rnd.nextLong());
                key.putInt(rnd.nextInt());
            }
            MapValue value = key.createValue();
            value.putLong(0, j);
            value.putLong(1, j);
            value.putDouble(2, j);
            value.putLong(3, j);
        }
    }

    @Benchmark
    public long testMerge() {
        long sizeEstimate = 0;
        for (int i = 0; i < nMaps; i++) {
            final Map srcMap = srcMaps.getQuick(i);
            sizeEstimate += srcMap.size();
        }

        if (sizeEstimate > 0) {
            destMap.setKeyCapacity((int) sizeEstimate);
        }

        for (int i = 0; i < nMaps; i++) {
            destMap.merge(srcMaps.getQuick(i), mergeFunction);
            srcMaps.getQuick(i).close();
        }

        return destMap.size();
    }
}
