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

import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.cairo.wal.SymbolMap;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectString;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
// This benchmark compares the speed of some hash maps implementation for the Wal Writer Symbols map.
// In this use case, we are only appending new keys to the map, and at tx completion we are clearing the whole
// map (thus the .clear in each Invocation).
public class WalWriterSymbolBenchmark {
    private static final double loadFactor = 0.7;
    private static final CharSequenceIntHashMap hmap = new CharSequenceIntHashMap(State.nSymbols, loadFactor, SymbolTable.VALUE_NOT_FOUND);
    private static final SymbolMap symbolHashMap = new SymbolMap(State.nSymbols, loadFactor, SymbolTable.VALUE_NOT_FOUND);

    @org.openjdk.jmh.annotations.State(Scope.Thread)
    public static class State {
        public static final int nSymbols = 1000;
        public static final int avgReadPerSymbol = 10;
        public static final int symbolLength = 32;

        private final Rnd rnd;
        public CharSequence[] symbols;
        public int[] indices;
        private long lo;

        public State() {
            this.rnd = new Rnd();
            this.indices = new int[nSymbols * avgReadPerSymbol];
            this.symbols = new CharSequence[nSymbols];
        }

        @Setup(Level.Trial)
        public void setup() {
            // To have an apple-to-apple comparison, we cannot rely on String
            // instead we're using off-heap memory to store the Strings.
            this.lo = Unsafe.malloc(nSymbols * symbolLength * 2, MemoryTag.NATIVE_DEFAULT);

            for (int i = 0; i < nSymbols; i++) {
                CharSequence symbol = rnd.nextChars(symbolLength);
                for (int j = 0; j < symbolLength; j++) {
                    Unsafe.getUnsafe().putChar(lo + (i * symbolLength + j) * 2, symbol.charAt(j));
                }
                DirectString ds = new DirectString();
                ds.of(lo + (i * symbolLength) * 2, symbolLength);
                symbols[i] = ds;
            }

            for (int i = 0; i < nSymbols * avgReadPerSymbol; i++) {
                indices[i] = rnd.nextInt(nSymbols);
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            if (this.lo != 0) {
                Unsafe.free(lo, nSymbols * symbolLength * 2, MemoryTag.NATIVE_DEFAULT);
                this.lo = 0;
            }
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(WalWriterSymbolBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .forks(0)
                .addProfiler(GCProfiler.class)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public void CharSequenceIntHashMap(State state) {
        for (int i = 0; i < state.indices.length; i++) {
            CharSequence key = state.symbols[state.indices[i]];
            final int index = hmap.keyIndex(key);
            if (index > -1) {
                hmap.putAt(index, key, i);
            }
        }

        for (int i = 0; i < hmap.size(); i++) {
            if (hmap.keys().getQuick(i) == null) {
                throw new AssertionError("null key at index " + i);
            }
        }

        hmap.clear();
    }

    @Benchmark
    public void WalWriterSymbolMap(State state) {
        for (int i = 0; i < state.indices.length; i++) {
            CharSequence key = state.symbols[state.indices[i]];
            final int hashCode = Chars.hashCode(key);
            final int index = symbolHashMap.keyIndex(key, hashCode);
            if (index > -1) {
                symbolHashMap.putAt(index, key, i, hashCode);
            }
        }

        for (int offset = symbolHashMap.nextOffset(); offset >= 0; offset = symbolHashMap.nextOffset(offset)) {
            if (symbolHashMap.get(offset) == null) {
                throw new AssertionError("null value at offset " + offset);
            }
        }

        symbolHashMap.clear();
    }
}
