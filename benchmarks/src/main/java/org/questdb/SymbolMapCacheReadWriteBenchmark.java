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

import io.questdb.std.Chars;
import io.questdb.std.DirectIntCharSequenceHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectString;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
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
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.infra.ThreadParams;
import org.openjdk.jmh.runner.IterationType;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * One writer inserts a finite batch while the other workers read existing keys.
 * Every iteration starts with a fresh map, so filling cannot silently turn into
 * stopped admissions. The separate-map control performs the same writes without
 * modifying the map used by readers. Compare the read subgroup, not the group mean.
 * Use -t 2 -tg 1,1 for local checks; inspect the measured read/write overlap in the log.
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 100)
@Measurement(iterations = 30)
@Fork(value = 1, jvmArgsAppend = {"--enable-native-access=ALL-UNNAMED", "--sun-misc-unsafe-memory-access=allow", "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"})
public class SymbolMapCacheReadWriteBenchmark {
    private static final int BATCH_SIZE = 65536;
    private static final String FILL_VALUE = "symbol-value";
    private static final int HOT_KEYS = 1024;

    @Benchmark
    @Group("mixed")
    @GroupThreads(1)
    @OperationsPerInvocation(BATCH_SIZE)
    public long read(SharedState state, LocalState local, ThreadParams threads) {
        DirectIntCharSequenceHashMap map = state.readMap;
        int[] keys = state.data.keys;
        int position = threads.getThreadIndex() * 17;
        long start = System.nanoTime();
        long checksum = 0;
        for (int i = 0; i < BATCH_SIZE; i++) {
            checksum += SymbolMapCacheBenchmarkData.nextKey(map.get(keys[(position + i) & (HOT_KEYS - 1)], local.view));
        }
        state.readEnds[threads.getThreadIndex()] = System.nanoTime();
        state.readStarts[threads.getThreadIndex()] = start;
        if (checksum != (long) BATCH_SIZE * (HOT_KEYS - 1) / 2) {
            throw new AssertionError("concurrent read returned a different value");
        }
        return checksum;
    }

    @Benchmark
    @Group("mixed")
    @GroupThreads(1)
    @OperationsPerInvocation(BATCH_SIZE)
    public int write(SharedState state) {
        DirectIntCharSequenceHashMap map = state.writeMap;
        long source = state.source;
        int admitted = 0;
        state.writeStart = System.nanoTime();
        for (int i = 0; i < BATCH_SIZE; i++) {
            if (map.tryPutIfAbsent(HOT_KEYS + i, source, FILL_VALUE.length()) == DirectIntCharSequenceHashMap.INSERTED) {
                admitted++;
            }
        }
        state.writeEnd = System.nanoTime();
        if (admitted != BATCH_SIZE) {
            throw new AssertionError("writer stopped filling during the measurement");
        }
        return admitted;
    }

    @State(Scope.Thread)
    public static class LocalState {
        private final DirectString view = new DirectString();
    }

    @State(Scope.Group)
    public static class SharedState {
        @Param({"shared", "separate"})
        public String target;
        private SymbolMapCacheBenchmarkData data;
        private long[] readEnds;
        private DirectIntCharSequenceHashMap readMap;
        private long[] readStarts;
        private long source;
        private long writeEnd;
        private DirectIntCharSequenceHashMap writeMap;
        private long writeStart;

        @Setup(Level.Trial)
        public void allocateSource() {
            if (!target.equals("shared") && !target.equals("separate")) {
                throw new IllegalArgumentException("expected shared/separate");
            }
            data = new SymbolMapCacheBenchmarkData(HOT_KEYS);
            source = Unsafe.malloc((long) FILL_VALUE.length() * Character.BYTES, MemoryTag.NATIVE_DEFAULT);
            Chars.copyStrChars(FILL_VALUE, 0, FILL_VALUE.length(), source);
        }

        @TearDown(Level.Trial)
        public void freeSource() {
            Unsafe.free(source, (long) FILL_VALUE.length() * Character.BYTES, MemoryTag.NATIVE_DEFAULT);
        }

        @Setup(Level.Iteration)
        public void setup(BenchmarkParams params) {
            readStarts = new long[params.getThreads()];
            readEnds = new long[params.getThreads()];
            writeStart = writeEnd = 0;
            readMap = newMap();
            writeMap = target.equals("shared") ? readMap : newMap();
        }

        @TearDown(Level.Iteration)
        public void tearDown(IterationParams iteration) {
            try {
                double overlap = 1;
                for (int i = 0; i < readStarts.length; i++) {
                    if (readStarts[i] != 0) {
                        long duration = readEnds[i] - readStarts[i];
                        long concurrent = Math.max(0, Math.min(readEnds[i], writeEnd) - Math.max(readStarts[i], writeStart));
                        overlap = Math.min(overlap, (double) concurrent / duration);
                    }
                }
                if (iteration.getType() == IterationType.MEASUREMENT) {
                    System.out.printf(Locale.ROOT, "MIXED target=%s min_read_overlap=%.4f admitted=%d%n", target, overlap, writeMap.size() - HOT_KEYS);
                }
            } finally {
                if (writeMap != readMap) {
                    writeMap.close();
                }
                readMap.close();
            }
        }

        private DirectIntCharSequenceHashMap newMap() {
            // Keep the index capacity fixed to isolate concurrent record publication.
            DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(HOT_KEYS + BATCH_SIZE, 0.7, MemoryTag.NATIVE_DEFAULT, null);
            for (int i = 0; i < HOT_KEYS; i++) {
                String value = data.values.getQuick(i);
                Chars.copyStrChars(value, 0, value.length(), source);
                if (map.tryPutIfAbsent(i, source, value.length()) != DirectIntCharSequenceHashMap.INSERTED) {
                    map.close();
                    throw new AssertionError("could not prefill reader hot set");
                }
            }
            Chars.copyStrChars(FILL_VALUE, 0, FILL_VALUE.length(), source);
            return map;
        }
    }
}
