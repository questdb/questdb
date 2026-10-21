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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.Chars;
import io.questdb.std.DirectIntCharSequenceHashMap;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.NativeMemoryBudget;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.Path;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.ThreadParams;

import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

/**
 * Run resident lookups with -t 1, 4 and 8. Keys uses a precomputed permutation;
 * Chain reads the next key from the preceding value. These measurements do not model page-cache
 * eviction; that requires OS fault and I/O counters on a separate cold-page run.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = {"--enable-native-access=ALL-UNNAMED", "--sun-misc-unsafe-memory-access=allow", "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"})
public class SymbolMapCacheBenchmark {
    private static final String FILL_VALUE = "symbol-value";

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 100)
    @Measurement(iterations = 30)
    @OperationsPerInvocation(1024)
    public int coldFillNative(FillingState state, LocalState local, FillCounters counters) {
        int inserted = 0;
        for (int i = 0; i < 1024; i++) {
            if (state.map.tryPutIfAbsent(local.rnd.nextInt(), state.source, FILL_VALUE.length()) == DirectIntCharSequenceHashMap.INSERTED) {
                inserted++;
            }
        }
        counters.admitted += inserted;
        counters.attempted += 1024;
        return inserted;
    }

    @Benchmark
    public int heapChain(ResidentState state, LocalState local) {
        return local.nextKey = SymbolMapCacheBenchmarkData.nextKey(state.heap.getQuick(local.nextKey));
    }

    @Benchmark
    public int heapKeys(ResidentState state, LocalState local) {
        int key = state.data.keys[local.position++ & (state.symbolCount - 1)];
        return SymbolMapCacheBenchmarkData.nextKey(state.heap.getQuick(key));
    }

    @Benchmark
    public char heapHit(ResidentState state, LocalState local) {
        String value = state.heap.getQuick(local.rnd.nextPositiveInt() & (state.symbolCount - 1));
        return value.charAt(value.length() - 1);
    }

    @Benchmark
    public char mapHit(ResidentState state, LocalState local) {
        DirectString value = state.map.get(local.rnd.nextPositiveInt() & (state.symbolCount - 1), local.view);
        return value.charAt(value.length() - 1);
    }

    @Benchmark
    public int mapChain(ResidentState state, LocalState local) {
        return local.nextKey = SymbolMapCacheBenchmarkData.nextKey(state.map.get(local.nextKey, local.view));
    }

    @Benchmark
    public int mapKeys(ResidentState state, LocalState local) {
        int key = state.data.keys[local.position++ & (state.symbolCount - 1)];
        return SymbolMapCacheBenchmarkData.nextKey(state.map.get(key, local.view));
    }

    @Benchmark
    public char mmapResident(ResidentState state, LocalState local) {
        int key = local.rnd.nextPositiveInt() & (state.symbolCount - 1);
        DirectString value = state.values.getStr(state.offsets.getLong((long) key * Long.BYTES), local.view);
        return value.charAt(value.length() - 1);
    }

    @Benchmark
    public int stoppedAdmission(StoppedState state, LocalState local) {
        return state.map.tryPutIfAbsent(local.rnd.nextInt(), 0, 0);
    }

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class FillCounters {
        public long admitted;
        public long attempted;
    }

    @State(Scope.Benchmark)
    public static class FillingState {
        private DirectIntCharSequenceHashMap map;
        private long source;

        @Setup(Level.Trial)
        public void allocateSource() {
            source = Unsafe.malloc((long) FILL_VALUE.length() * Character.BYTES, MemoryTag.NATIVE_DEFAULT);
            Chars.copyStrChars(FILL_VALUE, 0, FILL_VALUE.length(), source);
        }

        @TearDown(Level.Trial)
        public void freeSource() {
            Unsafe.free(source, (long) FILL_VALUE.length() * Character.BYTES, MemoryTag.NATIVE_DEFAULT);
        }

        @Setup(Level.Iteration)
        public void setup() {
            map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_DEFAULT, new NativeMemoryBudget(64 * 1024 * 1024));
        }

        @TearDown(Level.Iteration)
        public void tearDown() {
            map.close();
        }
    }

    @State(Scope.Thread)
    public static class LocalState {
        private final Rnd rnd = new Rnd();
        private final DirectString view = new DirectString();
        private int nextKey;
        private int position;

        @Setup(Level.Trial)
        public void setup(ThreadParams threads) {
            // Keep runs reproducible without making every writer insert the same keys.
            rnd.reset(0xdeadbeef + (long) threads.getThreadIndex(), 0xdee4c0ed);
            position = threads.getThreadIndex() * 17;
        }
    }

    @State(Scope.Benchmark)
    public static class ResidentState {
        @Param({"64", "65536"})
        public int symbolCount;
        private final ObjList<String> heap = new ObjList<>();
        private SymbolMapCacheBenchmarkData data;
        private DirectIntCharSequenceHashMap map;
        private MemoryCMARW offsets;
        private java.nio.file.Path root;
        private MemoryCMARW values;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            data = new SymbolMapCacheBenchmarkData(symbolCount);
            root = Files.createTempDirectory("symbol-cache-jmh");
            map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_DEFAULT, null);
            try (Path path = new Path()) {
                offsets = Vm.getSmallCMARWInstance(FilesFacadeImpl.INSTANCE, path.of(root.toString()).concat("offsets").$(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                values = Vm.getSmallCMARWInstance(FilesFacadeImpl.INSTANCE, path.of(root.toString()).concat("values").$(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
            }
            for (int i = 0; i < symbolCount; i++) {
                String value = data.values.getQuick(i);
                heap.add(value);
                long offset = values.putStr(value);
                if (map.tryPutIfAbsent(i, values.addressOf(offset + Integer.BYTES), value.length()) != DirectIntCharSequenceHashMap.INSERTED) {
                    throw new AssertionError("resident symbol was not cached");
                }
                offsets.putLong(offset);
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            map.close();
            offsets.close();
            values.close();
            Files.delete(root.resolve("offsets"));
            Files.delete(root.resolve("values"));
            Files.delete(root);
        }
    }

    @State(Scope.Benchmark)
    public static class StoppedState {
        private DirectIntCharSequenceHashMap map;

        @Setup(Level.Trial)
        public void setup() {
            map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_DEFAULT, new NativeMemoryBudget(0));
            map.tryPutIfAbsent(0, 0, 0);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            map.close();
        }
    }
}
