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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.MapBatchProber;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.map.Unordered8Map;
import io.questdb.cairo.map.UnorderedVarcharMap;
import io.questdb.griffin.engine.functions.groupby.CountLongConstGroupByFunction;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.DirectUtf8String;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Benchmarks comparing per-key vs batched hash GROUP BY aggregation
 * for OrderedMap (LONG key), OrderedMap (STRING key), Unordered8Map (LONG key),
 * and UnorderedVarcharMap.
 * <p>
 * Each benchmark method simulates a full page frame aggregation: iterate ROW_COUNT rows,
 * look up keys in the map, and update a count(*) aggregation via CountLongConstGroupByFunction.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class BatchHashGroupByBenchmark {
    private static final int BATCH_SIZE = 256;
    private static final int ROW_COUNT = 1_000_000;
    private final CountLongConstGroupByFunction countFn = new CountLongConstGroupByFunction();
    // Number of distinct keys. Controls cache pressure:
    // small = hot cache, large = many cache misses during probe.
    @Param({"100", "10000", "100000"})
    public int distinctKeys;
    // Pre-generated random LONG keys (ROW_COUNT entries).
    private long longKeysAddr;
    private OrderedMap orderedMapLong;
    private MapBatchProber orderedMapLongProber;
    private OrderedMap orderedMapStr;
    private MapBatchProber orderedMapStrProber;
    private long strBufAddr;
    private long strBufCapacity;
    // Pre-generated VARCHAR key data: per-row ptr + size arrays, backed by a contiguous buffer.
    private long strKeyPtrsAddr;
    private long strKeySizesAddr;
    private Unordered8Map unordered8Map;
    private MapBatchProber unordered8MapProber;
    private UnorderedVarcharMap varcharMap;
    private MapBatchProber varcharMapProber;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(BatchHashGroupByBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void orderedMapLongBatched() {
        orderedMapLong.reopen();
        orderedMapLongProber.reopen();
        for (int rowOffset = 0; rowOffset < ROW_COUNT; rowOffset += BATCH_SIZE) {
            int batchSize = Math.min(BATCH_SIZE, ROW_COUNT - rowOffset);
            orderedMapLongProber.resetBatch();
            for (int i = 0; i < batchSize; i++) {
                long k = Unsafe.getUnsafe().getLong(longKeysAddr + (long) (rowOffset + i) * Long.BYTES);
                orderedMapLongProber.putLong(k);
            }
            orderedMapLongProber.hashAndPrefetch(batchSize);
            for (int i = 0; i < batchSize; i++) {
                MapValue value = orderedMapLongProber.probeWithHash(i);
                if (value.isNew()) {
                    countFn.computeFirst(value, null, 0);
                } else {
                    countFn.computeNext(value, null, 0);
                }
            }
        }
    }

    @Benchmark
    public void orderedMapLongPerKey() {
        orderedMapLong.reopen();
        for (int i = 0; i < ROW_COUNT; i++) {
            long k = Unsafe.getUnsafe().getLong(longKeysAddr + (long) i * Long.BYTES);
            MapKey key = orderedMapLong.withKey();
            key.putLong(k);
            MapValue value = key.createValue();
            if (value.isNew()) {
                countFn.computeFirst(value, null, 0);
            } else {
                countFn.computeNext(value, null, 0);
            }
        }
    }

    // ==================== OrderedMap LONG key ====================

    @Benchmark
    public void orderedMapVarcharBatched() {
        orderedMapStr.reopen();
        orderedMapStrProber.reopen();
        DirectUtf8String flyweight = new DirectUtf8String();
        for (int rowOffset = 0; rowOffset < ROW_COUNT; rowOffset += BATCH_SIZE) {
            int batchSize = Math.min(BATCH_SIZE, ROW_COUNT - rowOffset);
            orderedMapStrProber.resetBatch();
            for (int i = 0; i < batchSize; i++) {
                long ptr = Unsafe.getUnsafe().getLong(strKeyPtrsAddr + (long) (rowOffset + i) * Long.BYTES);
                int sz = Unsafe.getUnsafe().getInt(strKeySizesAddr + (long) (rowOffset + i) * Integer.BYTES);
                flyweight.of(ptr, ptr + sz, true);
                orderedMapStrProber.beginKey();
                orderedMapStrProber.putVarchar(flyweight);
                orderedMapStrProber.endKey();
            }
            orderedMapStrProber.hashAndPrefetch(batchSize);
            for (int i = 0; i < batchSize; i++) {
                MapValue value = orderedMapStrProber.probeWithHash(i);
                if (value.isNew()) {
                    countFn.computeFirst(value, null, 0);
                } else {
                    countFn.computeNext(value, null, 0);
                }
            }
        }
    }

    @Setup(Level.Trial)
    public void setUp() {
        // Initialize the count function: it stores a LONG value at index 0.
        ArrayColumnTypes valueColumnTypes = new ArrayColumnTypes();
        countFn.initValueTypes(valueColumnTypes);

        SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

        // OrderedMap with LONG key.
        orderedMapLong = new OrderedMap(Numbers.SIZE_1MB, new SingleColumnType(ColumnType.LONG), valueTypes, distinctKeys, 0.7, Integer.MAX_VALUE
        );
        orderedMapLongProber = orderedMapLong.createBatchProber(BATCH_SIZE);

        // OrderedMap with VARCHAR key.
        orderedMapStr = new OrderedMap(4 * Numbers.SIZE_1MB, new SingleColumnType(ColumnType.VARCHAR), valueTypes, distinctKeys, 0.7, Integer.MAX_VALUE);
        orderedMapStrProber = orderedMapStr.createBatchProber(BATCH_SIZE);

        // Unordered8Map with LONG key.
        unordered8Map = new Unordered8Map(ColumnType.LONG, valueTypes, distinctKeys, 0.7, Integer.MAX_VALUE);
        unordered8MapProber = unordered8Map.createBatchProber(BATCH_SIZE);

        // UnorderedVarcharMap.
        varcharMap = new UnorderedVarcharMap(valueTypes, distinctKeys, 0.7, Integer.MAX_VALUE, 128 * 1024, 4 * Numbers.SIZE_1GB);
        varcharMapProber = varcharMap.createBatchProber(BATCH_SIZE);

        // Pre-generate LONG keys.
        longKeysAddr = Unsafe.malloc((long) ROW_COUNT * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        Rnd rnd = new Rnd();
        for (int i = 0; i < ROW_COUNT; i++) {
            // Use positive keys to avoid Unordered8Map zero-key edge case noise.
            Unsafe.getUnsafe().putLong(longKeysAddr + (long) i * Long.BYTES, rnd.nextLong(distinctKeys) + 1);
        }

        // Pre-generate VARCHAR key data in a contiguous native buffer.
        strKeyPtrsAddr = Unsafe.malloc((long) ROW_COUNT * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        strKeySizesAddr = Unsafe.malloc((long) ROW_COUNT * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);

        long[] distinctPtrs = new long[distinctKeys];
        int[] distinctSizes = new int[distinctKeys];
        // Estimate buffer size: each key is "key_" + digits, max ~10 bytes.
        strBufCapacity = (long) distinctKeys * 16;
        strBufAddr = Unsafe.malloc(strBufCapacity, MemoryTag.NATIVE_DEFAULT);
        long writePos = strBufAddr;
        try (DirectUtf8Sink sink = new DirectUtf8Sink(64)) {
            for (int i = 0; i < distinctKeys; i++) {
                sink.clear();
                sink.put("key_").put(i);
                int sz = sink.size();
                Vect.memcpy(writePos, sink.ptr(), sz);
                distinctPtrs[i] = writePos;
                distinctSizes[i] = sz;
                writePos += sz;
            }
        }

        rnd.reset();
        for (int i = 0; i < ROW_COUNT; i++) {
            int idx = rnd.nextInt(distinctKeys);
            Unsafe.getUnsafe().putLong(strKeyPtrsAddr + (long) i * Long.BYTES, distinctPtrs[idx]);
            Unsafe.getUnsafe().putInt(strKeySizesAddr + (long) i * Integer.BYTES, distinctSizes[idx]);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        Misc.free(orderedMapLongProber);
        Misc.free(orderedMapStrProber);
        Misc.free(unordered8MapProber);
        Misc.free(varcharMapProber);
        Misc.free(orderedMapLong);
        Misc.free(orderedMapStr);
        Misc.free(unordered8Map);
        Misc.free(varcharMap);
        if (longKeysAddr != 0) {
            Unsafe.free(longKeysAddr, (long) ROW_COUNT * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            longKeysAddr = 0;
        }
        if (strBufAddr != 0) {
            Unsafe.free(strBufAddr, strBufCapacity, MemoryTag.NATIVE_DEFAULT);
            strBufAddr = 0;
        }
        if (strKeyPtrsAddr != 0) {
            Unsafe.free(strKeyPtrsAddr, (long) ROW_COUNT * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            strKeyPtrsAddr = 0;
        }
        if (strKeySizesAddr != 0) {
            Unsafe.free(strKeySizesAddr, (long) ROW_COUNT * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            strKeySizesAddr = 0;
        }
    }

    @Benchmark
    public void testOrderedMapVarcharPerKey() {
        orderedMapStr.reopen();
        DirectUtf8String flyweight = new DirectUtf8String();
        for (int i = 0; i < ROW_COUNT; i++) {
            long ptr = Unsafe.getUnsafe().getLong(strKeyPtrsAddr + (long) i * Long.BYTES);
            int sz = Unsafe.getUnsafe().getInt(strKeySizesAddr + (long) i * Integer.BYTES);
            flyweight.of(ptr, ptr + sz, true);
            MapKey key = orderedMapStr.withKey();
            key.putVarchar(flyweight);
            MapValue value = key.createValue();
            if (value.isNew()) {
                countFn.computeFirst(value, null, 0);
            } else {
                countFn.computeNext(value, null, 0);
            }
        }
    }

    @Benchmark
    public void testUnordered8MapBatched() {
        unordered8Map.reopen();
        unordered8MapProber.reopen();
        for (int rowOffset = 0; rowOffset < ROW_COUNT; rowOffset += BATCH_SIZE) {
            int batchSize = Math.min(BATCH_SIZE, ROW_COUNT - rowOffset);
            unordered8MapProber.resetBatch();
            for (int i = 0; i < batchSize; i++) {
                long k = Unsafe.getUnsafe().getLong(longKeysAddr + (long) (rowOffset + i) * Long.BYTES);
                unordered8MapProber.putLong(k);
            }
            unordered8MapProber.hashAndPrefetch(batchSize);
            for (int i = 0; i < batchSize; i++) {
                MapValue value = unordered8MapProber.probeWithHash(i);
                if (value.isNew()) {
                    countFn.computeFirst(value, null, 0);
                } else {
                    countFn.computeNext(value, null, 0);
                }
            }
        }
    }

    @Benchmark
    public void testUnordered8MapPerKey() {
        unordered8Map.reopen();
        for (int i = 0; i < ROW_COUNT; i++) {
            long k = Unsafe.getUnsafe().getLong(longKeysAddr + (long) i * Long.BYTES);
            MapKey key = unordered8Map.withKey();
            key.putLong(k);
            MapValue value = key.createValue();
            if (value.isNew()) {
                countFn.computeFirst(value, null, 0);
            } else {
                countFn.computeNext(value, null, 0);
            }
        }
    }

    @Benchmark
    public void testVarcharMapBatched() {
        varcharMap.reopen();
        varcharMapProber.reopen();
        DirectUtf8String flyweight = new DirectUtf8String();
        for (int rowOffset = 0; rowOffset < ROW_COUNT; rowOffset += BATCH_SIZE) {
            int batchSize = Math.min(BATCH_SIZE, ROW_COUNT - rowOffset);
            varcharMapProber.resetBatch();
            for (int i = 0; i < batchSize; i++) {
                long ptr = Unsafe.getUnsafe().getLong(strKeyPtrsAddr + (long) (rowOffset + i) * Long.BYTES);
                int sz = Unsafe.getUnsafe().getInt(strKeySizesAddr + (long) (rowOffset + i) * Integer.BYTES);
                flyweight.of(ptr, ptr + sz, true);
                varcharMapProber.beginKey();
                varcharMapProber.putVarchar(flyweight);
                varcharMapProber.endKey();
            }
            varcharMapProber.hashAndPrefetch(batchSize);
            for (int i = 0; i < batchSize; i++) {
                MapValue value = varcharMapProber.probeWithHash(i);
                if (value.isNew()) {
                    countFn.computeFirst(value, null, 0);
                } else {
                    countFn.computeNext(value, null, 0);
                }
            }
        }
    }

    @Benchmark
    public void testVarcharMapPerKey() {
        varcharMap.reopen();
        DirectUtf8String flyweight = new DirectUtf8String();
        for (int i = 0; i < ROW_COUNT; i++) {
            long ptr = Unsafe.getUnsafe().getLong(strKeyPtrsAddr + (long) i * Long.BYTES);
            int sz = Unsafe.getUnsafe().getInt(strKeySizesAddr + (long) i * Integer.BYTES);
            flyweight.of(ptr, ptr + sz, true);
            MapKey key = varcharMap.withKey();
            key.putVarchar(flyweight);
            MapValue value = key.createValue();
            if (value.isNew()) {
                countFn.computeFirst(value, null, 0);
            } else {
                countFn.computeNext(value, null, 0);
            }
        }
    }
}
