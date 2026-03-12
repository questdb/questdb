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
import io.questdb.cairo.O3CopyJob;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
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

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class O3CopyJobVectBenchmark {
    private static final int INDEX_ENTRY_BYTES = Long.BYTES * 2;
    private static final long IN_ORDER_BIT = Long.MIN_VALUE;
    private static final int O3_TIMESTAMP_COLUMN_TYPE = ColumnType.setDesignatedTimestampBit(ColumnType.TIMESTAMP, true);

    @Param({"65536", "1048576"})
    public int mergedRowCount;

    @Param({"5", "25", "50"})
    public int oooPercent;

    private long dst32Addr;
    private long dst64Addr;
    private long dstTimestampAddr;
    private long inOrder32Addr;
    private long inOrder64Addr;
    private long mergeIndexAddr;
    private int oooRowCount;
    private long ooo32Addr;
    private long ooo64Addr;

    public static void main(String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include(O3CopyJobVectBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public long o3MergeShuffle32Bit() {
        O3CopyJob.mergeCopy(
                ColumnType.SYMBOL,
                mergeIndexAddr,
                mergedRowCount,
                inOrder32Addr,
                0,
                ooo32Addr,
                0,
                dst32Addr,
                0,
                0
        );
        return Unsafe.getUnsafe().getInt(dst32Addr + ((long) (mergedRowCount - 1) << 2));
    }

    @Benchmark
    public long o3MergeShuffle64Bit() {
        O3CopyJob.mergeCopy(
                ColumnType.LONG,
                mergeIndexAddr,
                mergedRowCount,
                inOrder64Addr,
                0,
                ooo64Addr,
                0,
                dst64Addr,
                0,
                0
        );
        return Unsafe.getUnsafe().getLong(dst64Addr + ((long) (mergedRowCount - 1) << 3));
    }

    @Benchmark
    public long o3TimestampCopyIndex() {
        O3CopyJob.mergeCopy(
                O3_TIMESTAMP_COLUMN_TYPE,
                mergeIndexAddr,
                mergedRowCount,
                0,
                0,
                0,
                0,
                dstTimestampAddr,
                0,
                0
        );
        return Unsafe.getUnsafe().getLong(dstTimestampAddr + ((long) (mergedRowCount - 1) << 3));
    }

    @Setup
    public void setUp() {
        Os.init();

        oooRowCount = Math.max(1, mergedRowCount * oooPercent / 100);
        if (oooRowCount >= mergedRowCount) {
            oooRowCount = mergedRowCount / 2;
        }
        final int inOrderRowCount = mergedRowCount - oooRowCount;

        mergeIndexAddr = Unsafe.malloc((long) mergedRowCount * INDEX_ENTRY_BYTES, MemoryTag.NATIVE_DEFAULT);

        inOrder32Addr = Unsafe.malloc((long) inOrderRowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        ooo32Addr = Unsafe.malloc((long) oooRowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        dst32Addr = Unsafe.malloc((long) mergedRowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);

        inOrder64Addr = Unsafe.malloc((long) inOrderRowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        ooo64Addr = Unsafe.malloc((long) oooRowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        dst64Addr = Unsafe.malloc((long) mergedRowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        dstTimestampAddr = Unsafe.malloc((long) mergedRowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);

        for (int i = 0; i < inOrderRowCount; i++) {
            Unsafe.getUnsafe().putInt(inOrder32Addr + ((long) i << 2), mix32(i));
            Unsafe.getUnsafe().putLong(inOrder64Addr + ((long) i << 3), mix64(i));
        }
        for (int i = 0; i < oooRowCount; i++) {
            final int value = i + inOrderRowCount;
            Unsafe.getUnsafe().putInt(ooo32Addr + ((long) i << 2), mix32(value));
            Unsafe.getUnsafe().putLong(ooo64Addr + ((long) i << 3), mix64(value));
        }

        buildMergeIndex(inOrderRowCount);
    }

    @TearDown
    public void tearDown() {
        mergeIndexAddr = Unsafe.free(mergeIndexAddr, (long) mergedRowCount * INDEX_ENTRY_BYTES, MemoryTag.NATIVE_DEFAULT);

        final int inOrderRowCount = mergedRowCount - oooRowCount;
        inOrder32Addr = Unsafe.free(inOrder32Addr, (long) inOrderRowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        ooo32Addr = Unsafe.free(ooo32Addr, (long) oooRowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        dst32Addr = Unsafe.free(dst32Addr, (long) mergedRowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);

        inOrder64Addr = Unsafe.free(inOrder64Addr, (long) inOrderRowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        ooo64Addr = Unsafe.free(ooo64Addr, (long) oooRowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        dst64Addr = Unsafe.free(dst64Addr, (long) mergedRowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        dstTimestampAddr = Unsafe.free(dstTimestampAddr, (long) mergedRowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
    }

    private void buildMergeIndex(int inOrderRowCount) {
        int inOrderRow = 0;
        int oooRow = 0;

        for (int i = 0; i < mergedRowCount; i++) {
            final long entryAddr = mergeIndexAddr + (long) i * INDEX_ENTRY_BYTES;
            Unsafe.getUnsafe().putLong(entryAddr, 1_000_000L + i);

            final boolean pickOoo = oooRow < oooRowCount
                    && (inOrderRow >= inOrderRowCount || ((long) (i + 1) * oooRowCount / mergedRowCount) > oooRow);

            if (pickOoo) {
                Unsafe.getUnsafe().putLong(entryAddr + Long.BYTES, oooRow++);
            } else {
                Unsafe.getUnsafe().putLong(entryAddr + Long.BYTES, IN_ORDER_BIT | inOrderRow++);
            }
        }
    }

    private static int mix32(int value) {
        int x = value * 0x9E3779B9;
        x ^= x >>> 16;
        x *= 0x85EBCA6B;
        x ^= x >>> 13;
        return x;
    }

    private static long mix64(long value) {
        long x = value + 0x9E3779B97F4A7C15L;
        x ^= x >>> 30;
        x *= 0xBF58476D1CE4E5B9L;
        x ^= x >>> 27;
        x *= 0x94D049BB133111EBL;
        x ^= x >>> 31;
        return x;
    }
}
