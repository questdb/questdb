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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.engine.join.JoinRecord;
import io.questdb.std.Unsafe;

public class GroupByColumnSink {
    private static final long HEADER_SIZE = 2 * Integer.BYTES;
    private static final long SIZE_OFFSET = Integer.BYTES;
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

    public void clear() {
        if (ptr != 0) {
            Unsafe.getUnsafe().putInt(ptr + SIZE_OFFSET, 0);
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

    public void put(JoinRecord record, int colIndex, int colTag) {
        switch (colTag) {
            case ColumnType.BYTE:
                putByte(record.getByte(colIndex));
                break;
            case ColumnType.BOOLEAN:
                putByte(record.getBool(colIndex) ? (byte) 1 : (byte) 0);
                break;
            case ColumnType.GEOBYTE:
                putByte(record.getGeoByte(colIndex));
                break;
            case ColumnType.SHORT:
                putShort(record.getShort(colIndex));
                break;
            case ColumnType.GEOSHORT:
                putShort(record.getGeoShort(colIndex));
                break;
            case ColumnType.INT:
                putInt(record.getInt(colIndex));
                break;
            case ColumnType.IPv4:
                putInt(record.getIPv4(colIndex));
                break;
            case ColumnType.FLOAT:
                putFloat(record.getFloat(colIndex));
                break;
            case ColumnType.GEOINT:
                putInt(record.getGeoInt(colIndex));
                break;
            case ColumnType.LONG:
                putLong(record.getLong(colIndex));
                break;
            case ColumnType.GEOLONG:
                putLong(record.getGeoLong(colIndex));
                break;
            case ColumnType.DOUBLE:
                putDouble(record.getDouble(colIndex));
                break;
            case ColumnType.DATE:
                putLong(record.getDate(colIndex));
                break;
            case ColumnType.TIMESTAMP:
                putLong(record.getTimestamp(colIndex));
                break;
            case ColumnType.LONG128, ColumnType.UUID:
                putLong128(record.getLong128Lo(colIndex), record.getLong128Hi(colIndex));
                break;
            case ColumnType.CHAR:
                putChar(record.getChar(colIndex));
                break;
        }
    }

    public void putDouble(double value) {
        long ptr = reserve(Double.BYTES);
        Unsafe.getUnsafe().putDouble(ptr, value);
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
        Unsafe.getUnsafe().putLong(ptr + 8L, hi);
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
