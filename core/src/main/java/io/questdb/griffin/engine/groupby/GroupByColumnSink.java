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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;

/**
 * Specialized flyweight char sink used in vectorized reducers from WINDOW JOIN factories.
 * <p>
 * Uses provided {@link GroupByAllocator} to allocate the underlying buffer. Grows the buffer when needed.
 * <p>
 * Buffer layout is the following:
 * <pre>
 * | capacity (in chars) |  size (in bytes)  | column values array |
 * +---------------------+-------------------+---------------------+
 * |       4 bytes       |      4 bytes      |          -          |
 * +---------------------+-------------------+---------------------+
 * </pre>
 */
public class GroupByColumnSink implements Mutable {
    private static final long HEADER_SIZE = 2 * Integer.BYTES;
    private static final long SIZE_OFFSET = Integer.BYTES;
    private static final Decimal128 decimal128 = new Decimal128();
    private static final Decimal256 decimal256 = new Decimal256();
    private final int initialCapacity;
    private GroupByAllocator allocator;
    private long ptr;

    public GroupByColumnSink(int initialCapacity) {
        this.initialCapacity = initialCapacity;
    }

    public int capacity() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr) : 0;
    }

    public void checkCapacity(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("Negative capacity. Integer overflow may be?");
        }

        final int oldCapacity = capacity();

        long oldPtr = ptr;
        if (capacity > oldCapacity) {
            final int newCapacity = Math.max(oldCapacity << 1, capacity);
            ptr = allocator.realloc(oldPtr, oldCapacity + HEADER_SIZE, newCapacity + HEADER_SIZE);
            Unsafe.getUnsafe().putInt(ptr, newCapacity);
        }
    }

    @Override
    public void clear() {
        if (ptr != 0) {
            Unsafe.getUnsafe().putInt(this.ptr + SIZE_OFFSET, 0);
        }
    }

    public GroupByColumnSink of(long ptr) {
        if (ptr == 0) {
            this.ptr = allocator.malloc(HEADER_SIZE + initialCapacity);
            Unsafe.getUnsafe().putInt(this.ptr, initialCapacity);
            Unsafe.getUnsafe().putInt(this.ptr + SIZE_OFFSET, 0);
        } else {
            this.ptr = ptr;
        }
        return this;
    }

    public long ptr() {
        return ptr;
    }

    public void put(Record record, Function function, short argType) {
        switch (argType) {
            case ColumnType.BYTE:
                putByte(function.getByte(record));
                break;
            case ColumnType.BOOLEAN:
                putByte(function.getBool(record) ? (byte) 1 : (byte) 0);
                break;
            case ColumnType.GEOBYTE:
                putByte(function.getGeoByte(record));
                break;
            case ColumnType.SHORT:
                putShort(function.getShort(record));
                break;
            case ColumnType.GEOSHORT:
                putShort(function.getGeoShort(record));
                break;
            case ColumnType.INT:
                putInt(function.getInt(record));
                break;
            case ColumnType.IPv4:
                putInt(function.getIPv4(record));
                break;
            case ColumnType.FLOAT:
                putFloat(function.getFloat(record));
                break;
            case ColumnType.GEOINT:
                putInt(function.getGeoInt(record));
                break;
            case ColumnType.LONG:
                putLong(function.getLong(record));
                break;
            case ColumnType.GEOLONG:
                putLong(function.getGeoLong(record));
                break;
            case ColumnType.DOUBLE:
                putDouble(function.getDouble(record));
                break;
            case ColumnType.DATE:
                putLong(function.getDate(record));
                break;
            case ColumnType.TIMESTAMP:
                putLong(function.getTimestamp(record));
                break;
            case ColumnType.LONG128, ColumnType.UUID:
                putLong128(function.getLong128Lo(record), function.getLong128Hi(record));
                break;
            case ColumnType.CHAR:
                putChar(function.getChar(record));
                break;
            case ColumnType.DECIMAL8:
                putByte(function.getDecimal8(record));
                break;
            case ColumnType.DECIMAL16:
                putShort(function.getDecimal16(record));
                break;
            case ColumnType.DECIMAL32:
                putInt(function.getDecimal32(record));
                break;
            case ColumnType.DECIMAL64:
                putLong(function.getDecimal64(record));
                break;
            case ColumnType.DECIMAL128:
                function.getDecimal128(record, decimal128);
                putDecimal128();
                break;
            case ColumnType.DECIMAL256:
                function.getDecimal256(record, decimal256);
                putDecimal256();
                break;
        }
    }

    public void putAt(int index, Record record, Function function, short argType) {
        switch (argType) {
            case ColumnType.BYTE:
                putByteAt(index, function.getByte(record));
                break;
            case ColumnType.BOOLEAN:
                putByteAt(index, function.getBool(record) ? (byte) 1 : (byte) 0);
                break;
            case ColumnType.GEOBYTE:
                putByteAt(index, function.getGeoByte(record));
                break;
            case ColumnType.SHORT:
                putShortAt(index, function.getShort(record));
                break;
            case ColumnType.GEOSHORT:
                putShortAt(index, function.getGeoShort(record));
                break;
            case ColumnType.INT:
                putIntAt(index, function.getInt(record));
                break;
            case ColumnType.IPv4:
                putIntAt(index, function.getIPv4(record));
                break;
            case ColumnType.FLOAT:
                putFloatAt(index, function.getFloat(record));
                break;
            case ColumnType.GEOINT:
                putIntAt(index, function.getGeoInt(record));
                break;
            case ColumnType.LONG:
                putLongAt(index, function.getLong(record));
                break;
            case ColumnType.GEOLONG:
                putLongAt(index, function.getGeoLong(record));
                break;
            case ColumnType.DOUBLE:
                putDoubleAt(index, function.getDouble(record));
                break;
            case ColumnType.DATE:
                putLongAt(index, function.getDate(record));
                break;
            case ColumnType.TIMESTAMP:
                putLongAt(index, function.getTimestamp(record));
                break;
            case ColumnType.LONG128, ColumnType.UUID:
                putLong128At(index, function.getLong128Lo(record), function.getLong128Hi(record));
                break;
            case ColumnType.CHAR:
                putCharAt(index, function.getChar(record));
                break;
            case ColumnType.DECIMAL8:
                putByteAt(index, function.getDecimal8(record));
                break;
            case ColumnType.DECIMAL16:
                putShortAt(index, function.getDecimal16(record));
                break;
            case ColumnType.DECIMAL32:
                putIntAt(index, function.getDecimal32(record));
                break;
            case ColumnType.DECIMAL64:
                putLongAt(index, function.getDecimal64(record));
                break;
            case ColumnType.DECIMAL128:
                function.getDecimal128(record, decimal128);
                putDecimal128At(index);
                break;
            case ColumnType.DECIMAL256:
                function.getDecimal256(record, decimal256);
                putDecimal256At(index);
                break;
        }
    }

    public void putByteAt(int index, byte value) {
        assert index < size();
        Unsafe.getUnsafe().putByte(ptr + HEADER_SIZE + index, value);
    }

    public void putDouble(double value) {
        long ptr = reserve(Double.BYTES);
        Unsafe.getUnsafe().putDouble(ptr, value);
    }

    public void putDoubleAt(int index, double value) {
        long offset = 8L * index;
        assert offset <= size() - 8;
        Unsafe.getUnsafe().putDouble(ptr + HEADER_SIZE + offset, value);
    }

    public void putFloatAt(int index, float value) {
        long offset = 4L * index;
        assert offset <= size() - 4;
        Unsafe.getUnsafe().putFloat(ptr + HEADER_SIZE + offset, value);
    }

    public void putIntAt(int index, int value) {
        long offset = 4L * index;
        assert offset <= size() - 4;
        Unsafe.getUnsafe().putInt(ptr + HEADER_SIZE + offset, value);
    }

    public void putLongAt(int index, long value) {
        long offset = 8L * index;
        assert offset <= size() - 8;
        Unsafe.getUnsafe().putLong(ptr + HEADER_SIZE + offset, value);
    }

    public void putShortAt(int index, short value) {
        long offset = 2L * index;
        assert offset <= size() - 2;
        Unsafe.getUnsafe().putShort(ptr + HEADER_SIZE + offset, value);
    }

    public void resetPtr() {
        ptr = 0;
    }

    public void setAllocator(GroupByAllocator allocator) {
        this.allocator = allocator;
    }

    public int size() {
        return ptr != 0 ? Unsafe.getUnsafe().getInt(ptr + SIZE_OFFSET) : 0;
    }

    public long startAddress() {
        return ptr + HEADER_SIZE;
    }

    private void putByte(byte value) {
        long ptr = reserve(Byte.BYTES);
        Unsafe.getUnsafe().putByte(ptr, value);
    }

    private void putChar(char value) {
        long ptr = reserve(Character.BYTES);
        Unsafe.getUnsafe().putChar(ptr, value);
    }

    private void putCharAt(int index, char value) {
        long offset = 2L * index;
        assert offset <= size() - 2;
        Unsafe.getUnsafe().putChar(ptr + HEADER_SIZE + offset, value);
    }

    private void putDecimal128() {
        long ptr = reserve(16);
        Unsafe.getUnsafe().putLong(ptr, decimal128.getHigh());
        Unsafe.getUnsafe().putLong(ptr + 8, decimal128.getLow());
    }

    private void putDecimal128At(int index) {
        long offset = 16L * index;
        assert offset <= size() - 16;
        long p = ptr + HEADER_SIZE + offset;
        Unsafe.getUnsafe().putLong(p, decimal128.getHigh());
        Unsafe.getUnsafe().putLong(p + 8, decimal128.getLow());
    }

    private void putDecimal256() {
        long ptr = reserve(32);
        Unsafe.getUnsafe().putLong(ptr, decimal256.getHh());
        Unsafe.getUnsafe().putLong(ptr + 8, decimal256.getHl());
        Unsafe.getUnsafe().putLong(ptr + 16, decimal256.getLh());
        Unsafe.getUnsafe().putLong(ptr + 24, decimal256.getLl());
    }

    private void putDecimal256At(int index) {
        long offset = 32L * index;
        assert offset <= size() - 32;
        long p = ptr + HEADER_SIZE + offset;
        Unsafe.getUnsafe().putLong(p, decimal256.getHh());
        Unsafe.getUnsafe().putLong(p + 8, decimal256.getHl());
        Unsafe.getUnsafe().putLong(p + 16, decimal256.getLh());
        Unsafe.getUnsafe().putLong(p + 24, decimal256.getLl());
    }

    private void putFloat(float value) {
        long ptr = reserve(Float.BYTES);
        Unsafe.getUnsafe().putFloat(ptr, value);
    }

    private void putInt(int value) {
        long ptr = reserve(Integer.BYTES);
        Unsafe.getUnsafe().putInt(ptr, value);
    }

    private void putLong(long value) {
        long ptr = reserve(Long.BYTES);
        Unsafe.getUnsafe().putLong(ptr, value);
    }

    private void putLong128(long lo, long hi) {
        long ptr = reserve(16);
        Unsafe.getUnsafe().putLong(ptr, lo);
        Unsafe.getUnsafe().putLong(ptr + 8, hi);
    }

    private void putLong128At(int index, long lo, long hi) {
        long offset = 16L * index;
        assert offset <= size() - 16;
        long p = ptr + HEADER_SIZE + offset;
        Unsafe.getUnsafe().putLong(p, lo);
        Unsafe.getUnsafe().putLong(p + 8, hi);
    }

    private void putShort(short value) {
        long ptr = reserve(Short.BYTES);
        Unsafe.getUnsafe().putShort(ptr, value);
    }

    /**
     * Reserve a specific amount of space (in bytes) and returns a pointer to its start.
     */
    private long reserve(int reservedSize) {
        int currentSize = size();
        int newSize = currentSize + reservedSize;
        checkCapacity(newSize);
        Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, newSize);
        return ptr + HEADER_SIZE + currentSize;
    }
}
