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

package io.questdb.cairo.map;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.MemoryTag;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.bytes.Bytes;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Unordered2Map is a look-up table (not a hash table) with keys up to 2 bytes in size used
 * to store intermediate data of group by, sample by queries. It provides {@link MapKey} and
 * {@link MapValue}, as well as {@link RecordCursor} interfaces for data access and modification.
 * The preferred way to create an Unordered2Map is {@link MapFactory}.
 * <p>
 * Map iteration provided by {@link RecordCursor} does not preserve the key insertion order, hence
 * the unordered map name.
 * <strong>Important!</strong>
 * Key and value structures must match the ones provided via lists of columns ({@link ColumnTypes})
 * to the map constructor. Later put* calls made on {@link MapKey} and {@link MapValue} must match
 * the declared column types to guarantee memory access safety.
 */
public class Unordered2Map implements Map, Reopenable {
    static final long KEY_SIZE = Short.BYTES;
    private static final int TABLE_CAPACITY = Short.toUnsignedInt((short) -1) + 1;

    private final Unordered2MapCursor cursor;
    private final long entrySize;
    private final Key key;
    private final int memoryTag;
    private final Unordered2MapRecord record;
    private final Unordered2MapValue value;
    private final Unordered2MapValue value2;
    private final Unordered2MapValue value3;
    private boolean hasZero;
    private long keyMemStart; // Key look-up memory start pointer.
    private long memLimit; // Look-up table memory limit pointer.
    private long memStart; // Look-up table memory start pointer.
    private int size = 0;

    public Unordered2Map(
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes
    ) {
        this(keyTypes, valueTypes, MemoryTag.NATIVE_UNORDERED_MAP);
    }

    Unordered2Map(
            @NotNull @Transient ColumnTypes keyTypes,
            @Nullable @Transient ColumnTypes valueTypes,
            int memoryTag
    ) {
        try {
            this.memoryTag = memoryTag;

            final int keyColumnCount = keyTypes.getColumnCount();
            long keySize = 0;
            for (int i = 0; i < keyColumnCount; i++) {
                final int columnType = keyTypes.getColumnType(i);
                final int size = ColumnType.sizeOf(columnType);
                if (size > 0) {
                    keySize += size;
                } else {
                    keySize = -1;
                    break;
                }
            }
            if (keySize <= 0 || keySize > KEY_SIZE) {
                throw CairoException.nonCritical().put("unexpected key size: ").put(keySize);
            }

            long valueOffset = 0;
            long[] valueOffsets = null;
            long valueSize = 0;
            if (valueTypes != null) {
                int valueColumnCount = valueTypes.getColumnCount();
                valueOffsets = new long[valueColumnCount];

                for (int i = 0; i < valueColumnCount; i++) {
                    valueOffsets[i] = valueOffset;
                    final int columnType = valueTypes.getColumnType(i);
                    final int size = ColumnType.sizeOf(columnType);
                    if (size <= 0) {
                        throw CairoException.nonCritical().put("value type is not supported: ").put(ColumnType.nameOf(columnType));
                    }
                    valueOffset += size;
                    valueSize += size;
                }
            }

            this.entrySize = Bytes.align2b(KEY_SIZE + valueSize);

            final long sizeBytes = entrySize * TABLE_CAPACITY;
            memStart = Unsafe.malloc(sizeBytes, memoryTag);
            Vect.memset(memStart, sizeBytes, 0);
            memLimit = memStart + sizeBytes;
            keyMemStart = Unsafe.malloc(KEY_SIZE, memoryTag);
            Unsafe.getUnsafe().putShort(keyMemStart, (short) 0);

            value = new Unordered2MapValue(valueSize, valueOffsets);
            value2 = new Unordered2MapValue(valueSize, valueOffsets);
            value3 = new Unordered2MapValue(valueSize, valueOffsets);

            record = new Unordered2MapRecord(valueSize, valueOffsets, value, keyTypes, valueTypes);
            cursor = new Unordered2MapCursor(record, this);
            key = new Key();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        size = 0;
        hasZero = false;
        Vect.memset(memStart, entrySize * TABLE_CAPACITY, 0);
        Unsafe.getUnsafe().putShort(keyMemStart, (short) 0);
    }

    @Override
    public void close() {
        if (memStart != 0) {
            memStart = memLimit = Unsafe.free(memStart, entrySize * TABLE_CAPACITY, memoryTag);
            keyMemStart = Unsafe.free(keyMemStart, KEY_SIZE, memoryTag);
            size = 0;
            hasZero = false;
        }
    }

    @Override
    public MapRecordCursor getCursor() {
        return cursor.init(memStart, memLimit, hasZero, size);
    }

    @Override
    public int getKeyCapacity() {
        return TABLE_CAPACITY;
    }

    @Override
    public MapRecord getRecord() {
        return record;
    }

    @Override
    public boolean isOpen() {
        return memStart != 0;
    }

    @Override
    public void merge(Map srcMap, MapValueMergeFunction mergeFunc) {
        assert this != srcMap;
        long srcSize = srcMap.size();
        if (srcSize == 0) {
            return;
        }
        Unordered2Map src2Map = (Unordered2Map) srcMap;

        // First, we handle zero key.
        if (src2Map.hasZero) {
            if (hasZero) {
                mergeFunc.merge(
                        valueAt(memStart),
                        src2Map.valueAt(src2Map.memStart)
                );
            } else {
                Vect.memcpy(memStart, src2Map.memStart, entrySize);
                hasZero = true;
                size++;
            }
            // Check if zero was the only element in the source map.
            if (srcSize == 1) {
                return;
            }
        }

        // Then we handle all non-zero keys.
        long destAddr = memStart + entrySize;
        long srcAddr = src2Map.memStart + entrySize;
        for (int i = 1; i < TABLE_CAPACITY; i++, destAddr += entrySize, srcAddr += entrySize) {
            short srcKey = Unsafe.getUnsafe().getShort(srcAddr);
            if (srcKey == 0) {
                continue;
            }

            short destKey = Unsafe.getUnsafe().getShort(destAddr);
            if (destKey != 0) {
                // Match found, merge values.
                mergeFunc.merge(
                        valueAt(destAddr),
                        src2Map.valueAt(srcAddr)
                );
            } else {
                // Not present in destination table, so we can simply copy it.
                Vect.memcpy(destAddr, srcAddr, entrySize);
                size++;
            }
        }
    }

    @Override
    public void reopen(int keyCapacity, long heapSize) {
        reopen();
    }

    public void reopen() {
        if (memStart == 0) {
            restoreInitialCapacity();
        }
    }

    @Override
    public void restoreInitialCapacity() {
        if (memStart == 0) {
            final long sizeBytes = entrySize * TABLE_CAPACITY;
            memStart = Unsafe.malloc(sizeBytes, memoryTag);
            memLimit = memStart + sizeBytes;
        }

        if (keyMemStart == 0) {
            keyMemStart = Unsafe.malloc(KEY_SIZE, memoryTag);
        }

        clear();
    }

    @Override
    public void setKeyCapacity(int newKeyCapacity) {
        // no-op
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public MapValue valueAt(long startAddress) {
        return valueOf(startAddress, false, value);
    }

    @Override
    public MapKey withKey() {
        return key.init();
    }

    private long getStartAddress(short key) {
        return memStart + entrySize * Short.toUnsignedInt(key);
    }

    private Unordered2MapValue valueOf(long startAddress, boolean newValue, Unordered2MapValue value) {
        return value.of(startAddress, memLimit, newValue);
    }

    long entrySize() {
        return entrySize;
    }

    boolean isZeroKey(long startAddress) {
        return Unsafe.getUnsafe().getShort(startAddress) == 0;
    }

    class Key implements MapKey {
        protected long appendAddress;

        @Override
        public long commit() {
            assert appendAddress <= keyMemStart + KEY_SIZE;
            return KEY_SIZE; // we don't need to track the actual key size
        }

        @Override
        public void copyFrom(MapKey srcKey) {
            Key src2Key = (Key) srcKey;
            copyFromRawKey(src2Key.startAddress());
        }

        @Override
        public MapValue createValue() {
            short key = Unsafe.getUnsafe().getShort(keyMemStart);
            if (key != 0) {
                long startAddress = getStartAddress(key);
                short k = Unsafe.getUnsafe().getShort(startAddress);
                size += (k == 0) ? 1 : 0;
                Unsafe.getUnsafe().putShort(startAddress, key);
                return valueOf(startAddress, k == 0, value);
            }

            if (hasZero) {
                return valueOf(memStart, false, value);
            }
            size++;
            hasZero = true;
            return valueOf(memStart, true, value);
        }

        @Override
        public MapValue createValue(long hashCode) {
            return createValue();
        }

        @Override
        public MapValue findValue() {
            return findValue(value);
        }

        @Override
        public MapValue findValue2() {
            return findValue(value2);
        }

        @Override
        public MapValue findValue3() {
            return findValue(value3);
        }

        @Override
        public long hash() {
            return 0; // no-op
        }

        public Key init() {
            appendAddress = keyMemStart;
            return this;
        }

        @Override
        public void put(Record record, RecordSink sink) {
            sink.copy(record, this);
        }

        @Override
        public void putArray(ArrayView view) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putBin(BinarySequence value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putBool(boolean value) {
            Unsafe.getUnsafe().putByte(appendAddress, (byte) (value ? 1 : 0));
            appendAddress += 1L;
        }

        @Override
        public void putByte(byte value) {
            Unsafe.getUnsafe().putByte(appendAddress, value);
            appendAddress += 1L;
        }

        @Override
        public void putChar(char value) {
            Unsafe.getUnsafe().putChar(appendAddress, value);
            appendAddress += 2L;
        }

        @Override
        public void putDate(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putDecimal128(Decimal128 decimal128) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putDecimal256(Decimal256 decimal256) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putDouble(double value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putFloat(float value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putIPv4(int value) {
            putInt(value);
        }

        @Override
        public void putInt(int value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putInterval(Interval interval) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putLong(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putLong128(long lo, long hi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putLong256(Long256 value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putLong256(long l0, long l1, long l2, long l3) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putRecord(Record value) {
            // no-op
        }

        @Override
        public void putShort(short value) {
            Unsafe.getUnsafe().putShort(appendAddress, value);
            appendAddress += 2L;
        }

        @Override
        public void putStr(CharSequence value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putStr(CharSequence value, int lo, int hi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putTimestamp(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putVarchar(Utf8Sequence value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void skip(int bytes) {
            appendAddress += bytes;
        }

        private MapValue findValue(Unordered2MapValue value) {
            short key = Unsafe.getUnsafe().getShort(keyMemStart);
            if (key != 0) {
                long startAddress = getStartAddress(key);
                short k = Unsafe.getUnsafe().getShort(startAddress);
                return k != 0 ? valueOf(startAddress, false, value) : null;
            }

            return hasZero ? valueOf(memStart, false, value) : null;
        }

        void copyFromRawKey(long srcPtr) {
            short srcKey = Unsafe.getUnsafe().getShort(srcPtr);
            Unsafe.getUnsafe().putShort(appendAddress, srcKey);
            appendAddress += KEY_SIZE;
        }

        long startAddress() {
            return keyMemStart;
        }
    }
}
