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

import io.questdb.cairo.*;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.map.UnorderedVarcharMap;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.*;
import io.questdb.std.str.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class UnorderedVarcharMapBenchmark {

    private static final String AUX_MEM_FILENAME = "aux";
    private static final String DATA_MEM_FILENAME = "data";
    private static final int MAX_SIZE = 1000;
    private static final int MIN_SIZE = 5;
    private static final int ROW_COUNT = 1_000_000;
    private static final int WORD_COUNT = 1_000;
    private MemoryCMR auxReadMem;
    private MemoryCMR dataReadMem;
    private OrderedMap orderedMap;
    private final Utf8SplitString stableUtf8String = new Utf8SplitString(() -> true);
    private final Utf8SplitString unstableUtf8String = new Utf8SplitString(() -> false);
    private UnorderedVarcharMap varcharMap;

    public static void main(String[] args) throws RunnerException {
        ensureFileDoesNotExist(AUX_MEM_FILENAME);
        ensureFileDoesNotExist(DATA_MEM_FILENAME);
        Options opt = new OptionsBuilder()
                .include(UnorderedVarcharMapBenchmark.class.getSimpleName())
                .warmupIterations(1)
                .measurementIterations(2)
                .forks(1)
                // .addProfiler(AsyncProfiler.class, "output=flamegraph")
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public void benchOrderedMap() {
        orderedMap.clear();
        for (int i = 0; i < ROW_COUNT; i++) {
            Utf8Sequence value = VarcharTypeDriver.getSplitValue(auxReadMem, dataReadMem, i, 0);

            MapKey mapKey = orderedMap.withKey();
            mapKey.putVarchar(value);
            MapValue mapValue = mapKey.createValue();
            if (mapValue.isNew()) {
                mapValue.putInt(0, 1);
            } else {
                mapValue.addInt(0, 1);
            }
        }
    }

    @Benchmark
    public void benchVarcharMapStable() {
        varcharMap.clear();
        for (int i = 0; i < ROW_COUNT; i++) {
            Utf8Sequence value = VarcharTypeDriver.getSplitValue(
                    auxReadMem.addressOf(0),
                    auxReadMem.addressHi(),
                    dataReadMem.addressOf(0),
                    dataReadMem.addressHi(),
                    i,
                    stableUtf8String
            );

            MapKey mapKey = varcharMap.withKey();
            mapKey.putVarchar(value);
            MapValue mapValue = mapKey.createValue();
            if (mapValue.isNew()) {
                mapValue.putInt(0, 1);
            } else {
                mapValue.addInt(0, 1);
            }
        }
    }

    @Benchmark
    public void benchVarcharMapUnstable() {
        varcharMap.clear();
        for (int i = 0; i < ROW_COUNT; i++) {
            Utf8Sequence value = VarcharTypeDriver.getSplitValue(
                    auxReadMem.addressOf(0),
                    auxReadMem.addressHi(),
                    dataReadMem.addressOf(0),
                    dataReadMem.addressHi(),
                    i,
                    unstableUtf8String
            );

            MapKey mapKey = varcharMap.withKey();
            mapKey.putVarchar(value);
            MapValue mapValue = mapKey.createValue();
            if (mapValue.isNew()) {
                mapValue.putInt(0, 1);
            } else {
                mapValue.addInt(0, 1);
            }
        }
    }

    @Setup(Level.Trial)
    public void createMap() {
        SingleColumnType values = new SingleColumnType(ColumnType.INT);
        varcharMap = new UnorderedVarcharMap(values, WORD_COUNT, 0.6, 5, 128 * 1024, 4 * Numbers.SIZE_1GB);

        // generous initial heap size to avoid resizing
        orderedMap = new OrderedMap(Numbers.SIZE_1MB, new SingleColumnType(ColumnType.VARCHAR), values, WORD_COUNT, 0.6, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
    }

    @Setup(Level.Trial)
    public void createMem() {
        FilesFacade ff = FilesFacadeImpl.INSTANCE;
        try (
                MemoryMA auxAppendMem = Vm.getPMARInstance(null);
                MemoryMA dataAppendMem = Vm.getPMARInstance(null)
        ) {
            try (Path path = new Path()) {
                path.of(AUX_MEM_FILENAME);
                auxAppendMem.of(ff, path.$(), ff.getMapPageSize(), MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE);
                path.of(DATA_MEM_FILENAME);
                dataAppendMem.of(ff, path.$(), ff.getMapPageSize(), MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE);
            }
            Utf8StringSink sink = new Utf8StringSink();
            long[] seeds0 = new long[WORD_COUNT];
            long[] seeds1 = new long[WORD_COUNT];

            Rnd strGen = new Rnd();
            for (int i = 0; i < WORD_COUNT; i++) {
                sink.clear();
                seeds0[i] = strGen.getSeed0();
                seeds1[i] = strGen.getSeed1();

                int size = strGen.nextInt(MAX_SIZE - MIN_SIZE) + MIN_SIZE;
                strGen.nextUtf8AsciiStr(size, sink);
            }

            Rnd rnd = new Rnd();
            for (int i = 0; i < ROW_COUNT; i++) {
                sink.clear();
                int index = rnd.nextInt(WORD_COUNT);
                strGen.reset(seeds0[index], seeds1[index]);
                int size = strGen.nextInt(MAX_SIZE);
                strGen.nextUtf8AsciiStr(size, sink);
                VarcharTypeDriver.appendValue(auxAppendMem, dataAppendMem, sink);
            }
        }

        try (Path path = new Path()) {
            LPSZ lpsz = path.of(AUX_MEM_FILENAME).$();
            auxReadMem = Vm.getCMRInstance(ff, lpsz, -1, MemoryTag.NATIVE_DEFAULT);
            path.of(DATA_MEM_FILENAME).$();
            dataReadMem = Vm.getCMRInstance(ff, lpsz, -1, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void ensureFileDoesNotExist(String file) throws RunnerException {
        try (Path path = new Path()) {
            path.of(file);
            if (Files.exists(path.$())) {
                throw new RunnerException("File " + file + " already exists. Delete it before running the benchmark.");
            }
        }
    }
}

