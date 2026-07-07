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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.ByteFunction;
import io.questdb.griffin.engine.functions.CharFunction;
import io.questdb.griffin.engine.functions.DateFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.FloatFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.IPv4Function;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.ShortFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UuidFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.junit.Assert;

/**
 * Shared test helpers for {@code *GroupByFunctionKeyedBatchTest} classes. Each test
 * asserts that the override of {@link GroupByFunction#computeKeyedBatch} produces
 * the same final value region (byte-for-byte) as the default implementation that
 * loops over {@code computeFirst}/{@code computeNext}.
 * <p>
 * A single minimal {@link PageFrameMemoryRecord} subclass, {@link TestFrameRecord},
 * backs both the direct-column fast path (via {@code getPageAddress}) and the
 * record-based slow path (via {@code getByte}/{@code getShort}/{@code getInt}/
 * {@code getLong}/{@code getFloat}/{@code getDouble}). Toggle
 * {@link TestFrameRecord#setPageAvailable} to force the slow path by making
 * {@code getPageAddress} return 0, which matches the "column top" condition the
 * production code checks for.
 * <p>
 * A set of {@code Indirect*Arg} wrappers delegate {@code getByte}/{@code getShort}/
 * etc. to the underlying record without implementing
 * {@link io.questdb.griffin.engine.functions.columns.ColumnFunction}. Using one
 * as an aggregator argument forces
 * {@link io.questdb.griffin.engine.groupby.GroupByUtils#directArgColumnIndex} to
 * return {@code -1}, which exercises the {@code argColumnIndex < 0} side of the
 * {@code argAddr} ternary in every {@code computeKeyedBatch} override.
 */
final class KeyedBatchTestUtils {

    // Standard layout shared by all per-type equivalence tests: eight entries,
    // split 4/4 between primed (existing) and fresh (new) slots. The priming
    // batch targets entries 0..3 via rows 0, 2, 4, 6; the test batch pairs
    // each of the eight entries with a distinct row index.
    private static final int ENTRY_COUNT = 8;
    private static final boolean[] PRIME_IS_NEW = {true, true, true, true};
    private static final long[] PRIME_ROWS = {0, 2, 4, 6};
    private static final boolean[] TEST_IS_NEW = {false, false, false, false, true, true, true, true};
    private static final long[] TEST_ROWS = {3, 1, 6, 5, 0, 1, 2, 5};

    private KeyedBatchTestUtils() {
    }

    /**
     * Allocates a native buffer holding the supplied primitive values
     * contiguously. The returned address is freed by {@link TestFrameRecord#close}
     * when the record is closed. One overload per primitive type accepted by
     * the test record's slow-path getters.
     */
    static long allocArgBuffer(byte[] values) {
        final long bytes = values.length;
        final long addr = Unsafe.malloc(bytes, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < values.length; i++) {
            Unsafe.putByte(addr + i, values[i]);
        }
        return addr;
    }

    static long allocArgBuffer(char[] values) {
        final long bytes = (long) values.length * Character.BYTES;
        final long addr = Unsafe.malloc(bytes, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < values.length; i++) {
            Unsafe.putChar(addr + (long) i * Character.BYTES, values[i]);
        }
        return addr;
    }

    static long allocArgBuffer(short[] values) {
        final long bytes = (long) values.length * Short.BYTES;
        final long addr = Unsafe.malloc(bytes, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < values.length; i++) {
            Unsafe.putShort(addr + (long) i * Short.BYTES, values[i]);
        }
        return addr;
    }

    static long allocArgBuffer(int[] values) {
        final long bytes = (long) values.length * Integer.BYTES;
        final long addr = Unsafe.malloc(bytes, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < values.length; i++) {
            Unsafe.putInt(addr + (long) i * Integer.BYTES, values[i]);
        }
        return addr;
    }

    static long allocArgBuffer(long[] values) {
        final long bytes = (long) values.length * Long.BYTES;
        final long addr = Unsafe.malloc(bytes, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < values.length; i++) {
            Unsafe.putLong(addr + (long) i * Long.BYTES, values[i]);
        }
        return addr;
    }

    static long allocArgBuffer(float[] values) {
        final long bytes = (long) values.length * Float.BYTES;
        final long addr = Unsafe.malloc(bytes, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < values.length; i++) {
            Unsafe.putFloat(addr + (long) i * Float.BYTES, values[i]);
        }
        return addr;
    }

    static long allocArgBuffer(double[] values) {
        final long bytes = (long) values.length * Double.BYTES;
        final long addr = Unsafe.malloc(bytes, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < values.length; i++) {
            Unsafe.putDouble(addr + (long) i * Double.BYTES, values[i]);
        }
        return addr;
    }

    /**
     * Allocates a native buffer of {@code l0.length * 32} bytes, laid out as
     * 4 consecutive longs per row ({@code l0}, {@code l1}, {@code l2}, {@code l3}),
     * matching the on-disk representation of a {@code LONG256} column.
     */
    static long allocArgBufferLong256(long[] l0, long[] l1, long[] l2, long[] l3) {
        Assert.assertEquals(l0.length, l1.length);
        Assert.assertEquals(l0.length, l2.length);
        Assert.assertEquals(l0.length, l3.length);
        final long bytes = (long) l0.length * 32L;
        final long addr = Unsafe.malloc(bytes, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < l0.length; i++) {
            final long base = addr + (long) i * 32L;
            Unsafe.putLong(base, l0[i]);
            Unsafe.putLong(base + 8, l1[i]);
            Unsafe.putLong(base + 16, l2[i]);
            Unsafe.putLong(base + 24, l3[i]);
        }
        return addr;
    }

    /**
     * Allocates a native buffer of {@code lo.length * 16} bytes, laid out as
     * {@code (lo, hi)} long pairs per row, matching the on-disk representation
     * of a {@code UUID} (or {@code LONG128}) column.
     */
    static long allocArgBufferUuid(long[] lo, long[] hi) {
        Assert.assertEquals(lo.length, hi.length);
        final long bytes = (long) lo.length * 16L;
        final long addr = Unsafe.malloc(bytes, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < lo.length; i++) {
            final long base = addr + (long) i * 16L;
            Unsafe.putLong(base, lo[i]);
            Unsafe.putLong(base + 8, hi[i]);
        }
        return addr;
    }

    /**
     * Allocates a zero-initialized value region of
     * {@code entryCount * valueSize} bytes and initializes each entry via
     * {@link GroupByFunction#setEmpty} so it matches the etalon seed that
     * {@link io.questdb.cairo.map.Map#setBatchEmptyValue} produces in
     * production.
     */
    static long allocEtalonRegion(GroupByFunction function, int entryCount, long valueSize, FlyweightPackedMapValue flyweight) {
        final long bytes = (long) entryCount * valueSize;
        final long addr = Unsafe.malloc(bytes, MemoryTag.NATIVE_DEFAULT);
        Unsafe.setMemory(addr, bytes, (byte) 0);
        for (int i = 0; i < entryCount; i++) {
            flyweight.of(addr + (long) i * valueSize);
            function.setEmpty(flyweight);
        }
        return addr;
    }

    /**
     * Byte-for-byte equality check over two native regions. Reports the first
     * diverging offset with both byte values, which narrows down the failing
     * column within the value region.
     */
    static void assertBytesEqual(long baseA, long baseB, long totalBytes) {
        for (long i = 0; i < totalBytes; i++) {
            final byte a = Unsafe.getByte(baseA + i);
            final byte b = Unsafe.getByte(baseB + i);
            if (a != b) {
                Assert.fail("value regions diverge at byte offset " + i
                        + ": ref=0x" + Integer.toHexString(a & 0xff)
                        + ", override=0x" + Integer.toHexString(b & 0xff));
            }
        }
    }

    /**
     * Asserts that {@code function.computeKeyedBatch} produces the same final
     * value region as the reference loop over {@code computeFirst} /
     * {@code computeNext}. The batch layout intentionally hits every branch
     * combination exposed by a typical override:
     * <ul>
     *     <li>entries 0..3 are primed (existing state) with a mix of non-null
     *         and null row values; entries 4..7 stay at the etalon (new
     *         entries);</li>
     *     <li>the test batch then pairs each of those eight entries with a
     *         distinct row, producing every {isNew, isNull} combination in a
     *         single pass.</li>
     * </ul>
     * The caller supplies an already-allocated argument buffer along with its
     * element size (in bytes); ownership transfers to the returned record so
     * the buffer is freed on {@code close()}.
     */
    static void assertEquivalence(GroupByFunction function, boolean fastPath, int elemSize, long argBufferAddr, long argBufferSize) {
        try (TestFrameRecord record = new TestFrameRecord(elemSize, argBufferAddr, argBufferSize)) {
            record.setPageAvailable(fastPath);

            final ArrayColumnTypes types = new ArrayColumnTypes();
            final long valueSize = initFunctionTypes(function, types);
            final FlyweightPackedMapValue flyweightA = new FlyweightPackedMapValue(types);
            final FlyweightPackedMapValue flyweightB = new FlyweightPackedMapValue(types);

            final long regionBytes = (long) ENTRY_COUNT * valueSize;
            final long baseA = allocEtalonRegion(function, ENTRY_COUNT, valueSize, flyweightA);
            final long baseB = allocEtalonRegion(function, ENTRY_COUNT, valueSize, flyweightB);
            try {
                // Priming: entries 0..3 receive an initial value each, marked
                // as new, establishing a non-etalon starting state on both
                // regions through the reference path.
                final long[] primeOffsets = {0, valueSize, 2 * valueSize, 3 * valueSize};
                final long primeBatch = buildBatchBuffer(PRIME_ROWS, primeOffsets, PRIME_IS_NEW);
                try {
                    runReferencePath(function, record, flyweightA, baseA, primeBatch, PRIME_ROWS.length, 0);
                    runReferencePath(function, record, flyweightB, baseB, primeBatch, PRIME_ROWS.length, 0);
                } finally {
                    Unsafe.free(primeBatch, (long) PRIME_ROWS.length * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                }

                // Sanity check: priming uses the same code on both regions, so
                // divergence here would point at the test harness, not the
                // override.
                assertBytesEqual(baseA, baseB, regionBytes);

                // Test batch: existing entries first, then new entries. Row
                // indexes alternate null / non-null values from the caller's
                // ARG_VALUES to cover {isNew, isNull} × {true, false}.
                final long[] testOffsets = {
                        0, valueSize, 2 * valueSize, 3 * valueSize,
                        4 * valueSize, 5 * valueSize, 6 * valueSize, 7 * valueSize
                };
                final long testBatch = buildBatchBuffer(TEST_ROWS, testOffsets, TEST_IS_NEW);
                try {
                    runReferencePath(function, record, flyweightA, baseA, testBatch, TEST_ROWS.length, 1000);
                    function.computeKeyedBatch(record, flyweightB, baseB, testBatch, TEST_ROWS.length, 1000);
                } finally {
                    Unsafe.free(testBatch, (long) TEST_ROWS.length * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                }

                assertBytesEqual(baseA, baseB, regionBytes);
            } finally {
                Unsafe.free(baseA, regionBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(baseB, regionBytes, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    /**
     * Packs (rowIndex, entryOffset, isNew) triples into a native buffer of
     * packed longs matching the layout consumed by {@code computeKeyedBatch}.
     * Each entry occupies 8 bytes.
     */
    static long buildBatchBuffer(long[] rowIndexes, long[] entryOffsets, boolean[] isNewFlags) {
        final int n = rowIndexes.length;
        Assert.assertEquals(n, entryOffsets.length);
        Assert.assertEquals(n, isNewFlags.length);
        final long bytes = (long) n * Long.BYTES;
        final long addr = Unsafe.malloc(bytes, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < n; i++) {
            final long encoded = Map.encodeBatchEntry(rowIndexes[i], entryOffsets[i], isNewFlags[i]);
            Unsafe.putLong(addr + (long) i * Long.BYTES, encoded);
        }
        return addr;
    }

    /**
     * Initializes a function's value-column indices via
     * {@link GroupByFunction#initValueTypes} and returns the resulting total
     * value-region size in bytes, derived from the registered column types.
     */
    static long initFunctionTypes(GroupByFunction function, ArrayColumnTypes types) {
        function.initValueTypes(types);
        long size = 0;
        for (int i = 0, n = types.getColumnCount(); i < n; i++) {
            size += ColumnType.sizeOf(types.getColumnType(i));
        }
        return size;
    }

    /**
     * Runs the default {@link GroupByFunction#computeKeyedBatch} skeleton
     * inline: for each entry, positions {@code flyweight} at the entry's value
     * region, calls {@code setRowIndex} on the record, and dispatches to
     * {@code computeFirst} / {@code computeNext}.
     * <p>
     * Intentionally not delegated to {@code GroupByFunction#computeKeyedBatch}
     * so the equivalence test does not depend on the default implementation
     * remaining unchanged.
     */
    static void runReferencePath(
            GroupByFunction function,
            PageFrameMemoryRecord record,
            FlyweightPackedMapValue flyweight,
            long baseValueAddress,
            long batchAddr,
            long rowCount,
            long baseRowId
    ) {
        for (long i = 0; i < rowCount; i++) {
            final long encoded = Unsafe.getLong(batchAddr + (i << 3));
            final long valueOffset = Map.decodeBatchOffset(encoded);
            final int rowIndex = Map.decodeBatchRowIndex(encoded);
            final boolean isNew = Map.isNewBatchEntry(encoded);
            record.setRowIndex(rowIndex);
            flyweight.of(baseValueAddress + valueOffset);
            if (isNew) {
                function.computeFirst(flyweight, record, baseRowId + rowIndex);
            } else {
                function.computeNext(flyweight, record, baseRowId + rowIndex);
            }
        }
    }

    static final class IndirectBoolArg extends BooleanFunction {
        private final int columnIndex;

        IndirectBoolArg(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public boolean getBool(Record rec) {
            return rec.getBool(columnIndex);
        }
    }

    static final class IndirectByteArg extends ByteFunction {
        private final int columnIndex;

        IndirectByteArg(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public byte getByte(Record rec) {
            return rec.getByte(columnIndex);
        }
    }

    static final class IndirectCharArg extends CharFunction {
        private final int columnIndex;

        IndirectCharArg(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public char getChar(Record rec) {
            return rec.getChar(columnIndex);
        }
    }

    static final class IndirectDateArg extends DateFunction {
        private final int columnIndex;

        IndirectDateArg(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public long getDate(Record rec) {
            return rec.getDate(columnIndex);
        }
    }

    static final class IndirectDoubleArg extends DoubleFunction {
        private final int columnIndex;

        IndirectDoubleArg(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public double getDouble(Record rec) {
            return rec.getDouble(columnIndex);
        }
    }

    static final class IndirectFloatArg extends FloatFunction {
        private final int columnIndex;

        IndirectFloatArg(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public float getFloat(Record rec) {
            return rec.getFloat(columnIndex);
        }
    }

    static final class IndirectIPv4Arg extends IPv4Function {
        private final int columnIndex;

        IndirectIPv4Arg(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public int getIPv4(Record rec) {
            return rec.getIPv4(columnIndex);
        }
    }

    static final class IndirectIntArg extends IntFunction {
        private final int columnIndex;

        IndirectIntArg(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public int getInt(Record rec) {
            return rec.getInt(columnIndex);
        }
    }

    static final class IndirectLong256Arg extends Long256Function {
        private final int columnIndex;

        IndirectLong256Arg(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void getLong256(Record rec, CharSink<?> sink) {
            rec.getLong256(columnIndex, sink);
        }

        @Override
        public Long256 getLong256A(Record rec) {
            return rec.getLong256A(columnIndex);
        }

        @Override
        public Long256 getLong256B(Record rec) {
            return rec.getLong256B(columnIndex);
        }
    }

    static final class IndirectLongArg extends LongFunction {
        private final int columnIndex;

        IndirectLongArg(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public long getLong(Record rec) {
            return rec.getLong(columnIndex);
        }
    }

    static final class IndirectShortArg extends ShortFunction {
        private final int columnIndex;

        IndirectShortArg(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public short getShort(Record rec) {
            return rec.getShort(columnIndex);
        }
    }

    static final class IndirectStrArg extends StrFunction {
        private final int columnIndex;

        IndirectStrArg(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return rec.getStrA(columnIndex);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return rec.getStrB(columnIndex);
        }

        @Override
        public int getStrLen(Record rec) {
            return rec.getStrLen(columnIndex);
        }
    }

    static final class IndirectTimestampArg extends TimestampFunction {
        private final int columnIndex;

        IndirectTimestampArg(int columnIndex, int timestampType) {
            super(timestampType);
            this.columnIndex = columnIndex;
        }

        @Override
        public long getTimestamp(Record rec) {
            return rec.getTimestamp(columnIndex);
        }
    }

    static final class IndirectUuidArg extends UuidFunction {
        private final int columnIndex;

        IndirectUuidArg(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public long getLong128Hi(Record rec) {
            return rec.getLong128Hi(columnIndex);
        }

        @Override
        public long getLong128Lo(Record rec) {
            return rec.getLong128Lo(columnIndex);
        }
    }

    static final class IndirectVarcharArg extends VarcharFunction {
        private final int columnIndex;

        IndirectVarcharArg(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            return rec.getVarcharA(columnIndex);
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            return rec.getVarcharB(columnIndex);
        }

        @Override
        public int getVarcharSize(Record rec) {
            return rec.getVarcharSize(columnIndex);
        }
    }

    /**
     * Minimal {@link PageFrameMemoryRecord} that services both code paths of
     * {@code computeKeyedBatch}:
     * <ul>
     *     <li>the direct-column fast path via {@link #getPageAddress},</li>
     *     <li>the record-based slow path via {@code getByte}/{@code getShort}/
     *         {@code getInt}/{@code getLong}/{@code getFloat}/{@code getDouble}.</li>
     * </ul>
     * The underlying native buffer is a flat array of primitive values, indexed
     * by {@code rowIndex} and stepping by {@code elemSize} bytes per row.
     * {@link #setPageAvailable} flips between paths without rebuilding the
     * buffer.
     */
    static final class TestFrameRecord extends PageFrameMemoryRecord {
        private final long bufferAddr;
        private final long bufferSize;
        private final int elemSize;
        private final Long256Impl long256 = new Long256Impl();
        private boolean pageAvailable = true;

        TestFrameRecord(int elemSize, long bufferAddr, long bufferSize) {
            this.elemSize = elemSize;
            this.bufferAddr = bufferAddr;
            this.bufferSize = bufferSize;
        }

        @Override
        public void close() {
            if (bufferAddr != 0) {
                Unsafe.free(bufferAddr, bufferSize, MemoryTag.NATIVE_DEFAULT);
            }
        }

        @Override
        public boolean getBool(int columnIndex) {
            return Unsafe.getByte(bufferAddr + rowIndex * elemSize) != 0;
        }

        @Override
        public byte getByte(int columnIndex) {
            return Unsafe.getByte(bufferAddr + rowIndex * elemSize);
        }

        @Override
        public char getChar(int columnIndex) {
            return Unsafe.getChar(bufferAddr + rowIndex * elemSize);
        }

        @Override
        public double getDouble(int columnIndex) {
            return Unsafe.getDouble(bufferAddr + rowIndex * elemSize);
        }

        @Override
        public float getFloat(int columnIndex) {
            return Unsafe.getFloat(bufferAddr + rowIndex * elemSize);
        }

        @Override
        public byte getGeoByte(int columnIndex) {
            return Unsafe.getByte(bufferAddr + rowIndex * elemSize);
        }

        @Override
        public int getGeoInt(int columnIndex) {
            return Unsafe.getInt(bufferAddr + rowIndex * elemSize);
        }

        @Override
        public long getGeoLong(int columnIndex) {
            return Unsafe.getLong(bufferAddr + rowIndex * elemSize);
        }

        @Override
        public short getGeoShort(int columnIndex) {
            return Unsafe.getShort(bufferAddr + rowIndex * elemSize);
        }

        @Override
        public int getIPv4(int columnIndex) {
            return Unsafe.getInt(bufferAddr + rowIndex * elemSize);
        }

        @Override
        public int getInt(int columnIndex) {
            return Unsafe.getInt(bufferAddr + rowIndex * elemSize);
        }

        @Override
        public long getLong(int columnIndex) {
            return Unsafe.getLong(bufferAddr + rowIndex * elemSize);
        }

        @Override
        public long getLong128Hi(int columnIndex) {
            return Unsafe.getLong(bufferAddr + rowIndex * elemSize + 8);
        }

        @Override
        public long getLong128Lo(int columnIndex) {
            return Unsafe.getLong(bufferAddr + rowIndex * elemSize);
        }

        @Override
        public Long256 getLong256A(int columnIndex) {
            final long base = bufferAddr + rowIndex * elemSize;
            long256.setAll(
                    Unsafe.getLong(base),
                    Unsafe.getLong(base + 8),
                    Unsafe.getLong(base + 16),
                    Unsafe.getLong(base + 24)
            );
            return long256;
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return pageAvailable ? bufferAddr : 0;
        }

        @Override
        public short getShort(int columnIndex) {
            return Unsafe.getShort(bufferAddr + rowIndex * elemSize);
        }

        @Override
        public int getStrLen(int columnIndex) {
            return Unsafe.getInt(bufferAddr + rowIndex * elemSize);
        }

        @Override
        public long getTimestamp(int columnIndex) {
            return getLong(columnIndex);
        }

        @Override
        public int getVarcharSize(int columnIndex) {
            return Unsafe.getInt(bufferAddr + rowIndex * elemSize);
        }

        void setPageAvailable(boolean value) {
            this.pageAvailable = value;
        }
    }
}
