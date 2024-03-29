/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.std.*;
import io.questdb.std.bytes.Bytes;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Unordered16Map is a general purpose off-heap hash table with keys up to 16 bytes in size used
 * to store intermediate data of group by, sample by queries. It provides {@link MapKey} and
 * {@link MapValue}, as well as {@link RecordCursor} interfaces for data access and modification.
 * The preferred way to create an Unordered16Map is {@link MapFactory}.
 * <p>
 * Map iteration provided by {@link RecordCursor} does not preserve the key insertion order, hence
 * the unordered map name.
 * <strong>Important!</strong>
 * Key and value structures must match the ones provided via lists of columns ({@link ColumnTypes})
 * to the map constructor. Later put* calls made on {@link MapKey} and {@link MapValue} must match
 * the declared column types to guarantee memory access safety.
 * <p>
 * Keys must be fixed-size and up to 16 bytes total. Only insertions and updates operations are
 * supported meaning that a key can't be removed from the map once it was inserted.
 * <p>
 * The hash table is organized into the following parts:
 * <ul>
 * <li>1. Off-heap memory for writing new keys</li>
 * <li>2. Off-heap memory for zero key and value</li>
 * <li>3. Off-heap memory for key-value pairs, i.e. the hash table with open addressing</li>
 * </ul>
 * The hash table uses linear probing.
 * <p>
 * Key-value pairs stored in the hash table may have the following layout:
 * <pre>
 * |   Key columns 0..K   | Optional padding | Value columns 0..V |
 * +----------------------+------------------+--------------------+
 * |    up to 16 bytes    |        -         |         -          |
 * +----------------------+------------------+--------------------+
 * </pre>
 */
public class Unordered16Map implements Map, Reopenable {

    static final long KEY_SIZE = 2 * Long.BYTES;
    private static final long MAX_SAFE_INT_POW_2 = 1L << 31;
    private static final int MIN_KEY_CAPACITY = 16;
    private final Unordered16MapCursor cursor;
    private final long entrySize;
    private final Key key;
    private final double loadFactor;
    private final int maxResizes;
    private final int memoryTag;
    private final Unordered16MapRecord record;
    private final Unordered16MapValue value;
    private final Unordered16MapValue value2;
    private final Unordered16MapValue value3;
    private int free;
    private boolean hasZero;
    private int initialKeyCapacity;
    private int keyCapacity;
    private long keyMemStart; // Key look-up memory start pointer.
    private long mask;
    private long memLimit; // Hash table memory limit pointer.
    private long memStart; // Hash table memory start pointer.
    private int nResizes;
    private int size = 0;
    private long zeroMemStart; // Zero key-value pair memory start pointer.

    public Unordered16Map(
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes
    ) {
        this(keyTypes, valueTypes, keyCapacity, loadFactor, maxResizes, MemoryTag.NATIVE_UNORDERED_MAP);
    }

    Unordered16Map(
            @NotNull @Transient ColumnTypes keyTypes,
            @Nullable @Transient ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes,
            int memoryTag
    ) {
        assert loadFactor > 0 && loadFactor < 1d;

        this.memoryTag = memoryTag;
        this.loadFactor = loadFactor;
        this.keyCapacity = (int) (keyCapacity / loadFactor);
        this.keyCapacity = this.initialKeyCapacity = Math.max(Numbers.ceilPow2(this.keyCapacity), MIN_KEY_CAPACITY);
        this.maxResizes = maxResizes;
        mask = this.keyCapacity - 1;
        free = (int) (this.keyCapacity * loadFactor);
        nResizes = 0;

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

        this.entrySize = Bytes.align8b(KEY_SIZE + valueSize);
        final long sizeBytes = entrySize * this.keyCapacity;
        memStart = Unsafe.malloc(sizeBytes, memoryTag);
        Vect.memset(memStart, sizeBytes, 0);
        memLimit = memStart + sizeBytes;
        keyMemStart = Unsafe.malloc(KEY_SIZE, memoryTag);
        Unsafe.getUnsafe().putLong(keyMemStart, 0);
        Unsafe.getUnsafe().putLong(keyMemStart + 8L, 0);
        zeroMemStart = Unsafe.malloc(entrySize, memoryTag);
        Vect.memset(zeroMemStart, entrySize, 0);

        value = new Unordered16MapValue(valueSize, valueOffsets);
        value2 = new Unordered16MapValue(valueSize, valueOffsets);
        value3 = new Unordered16MapValue(valueSize, valueOffsets);

        record = new Unordered16MapRecord(valueSize, valueOffsets, value, keyTypes, valueTypes);
        cursor = new Unordered16MapCursor(record, this);
        key = new Key();
    }

    @Override
    public void clear() {
        free = (int) (keyCapacity * loadFactor);
        size = 0;
        nResizes = 0;
        hasZero = false;
        Vect.memset(memStart, memLimit - memStart, 0);
        Unsafe.getUnsafe().putLong(keyMemStart, 0);
        Unsafe.getUnsafe().putLong(keyMemStart + 8L, 0);
        Vect.memset(zeroMemStart, entrySize, 0);
    }

    @Override
    public void close() {
        if (memStart != 0) {
            memLimit = memStart = Unsafe.free(memStart, memLimit - memStart, memoryTag);
            keyMemStart = Unsafe.free(keyMemStart, KEY_SIZE, memoryTag);
            zeroMemStart = Unsafe.free(zeroMemStart, entrySize, memoryTag);
            free = 0;
            size = 0;
            hasZero = false;
        }
    }

    @Override
    public MapRecordCursor getCursor() {
        if (hasZero) {
            return cursor.init(memStart, memLimit, zeroMemStart, size + 1);
        }
        return cursor.init(memStart, memLimit, 0, size);
    }

    @Override
    public int getKeyCapacity() {
        return keyCapacity;
    }

    @Override
    public MapRecord getRecord() {
        return record;
    }

    @Override
    public void merge(Map srcMap, MapValueMergeFunction mergeFunc) {
        assert this != srcMap;
        long srcSize = srcMap.size();
        if (srcSize == 0) {
            return;
        }
        Unordered16Map src16Map = (Unordered16Map) srcMap;

        // First, we handle zero key.
        if (src16Map.hasZero) {
            if (hasZero) {
                mergeFunc.merge(
                        valueAt(zeroMemStart),
                        src16Map.valueAt(src16Map.zeroMemStart)
                );
            } else {
                Vect.memcpy(zeroMemStart, src16Map.zeroMemStart, entrySize);
                hasZero = true;
            }
            // Check if zero was the only element in the source map.
            if (srcSize == 1) {
                return;
            }
        }

        // Then we handle all non-zero keys.
        OUTER:
        for (long srcAddr = src16Map.memStart; srcAddr < src16Map.memLimit; srcAddr += entrySize) {
            long key1 = Unsafe.getUnsafe().getLong(srcAddr);
            long key2 = Unsafe.getUnsafe().getLong(srcAddr + 8L);
            if (key1 == 0 && key2 == 0) {
                continue;
            }

            long destAddr = getStartAddress(Hash.hashLong128_64(key1, key2) & mask);
            for (; ; ) {
                long k1 = Unsafe.getUnsafe().getLong(destAddr);
                long k2 = Unsafe.getUnsafe().getLong(destAddr + 8L);
                if (k1 == 0 && k2 == 0) {
                    break;
                } else if (k1 == key1 && k2 == key2) {
                    // Match found, merge values.
                    mergeFunc.merge(
                            valueAt(destAddr),
                            src16Map.valueAt(srcAddr)
                    );
                    continue OUTER;
                }
                destAddr = getNextAddress(destAddr);
            }

            Vect.memcpy(destAddr, srcAddr, entrySize);
            size++;
            if (--free == 0) {
                rehash();
            }
        }
    }

    @Override
    public void reopen(int keyCapacity, int heapSize) {
        if (memStart == 0) {
            keyCapacity = (int) (keyCapacity / loadFactor);
            initialKeyCapacity = Math.max(Numbers.ceilPow2(keyCapacity), MIN_KEY_CAPACITY);
            restoreInitialCapacity();
        }
    }

    public void reopen() {
        if (memStart == 0) {
            // handles both mem and offsets
            restoreInitialCapacity();
        }
    }

    @Override
    public void restoreInitialCapacity() {
        if (memStart == 0 || keyCapacity != initialKeyCapacity) {
            keyCapacity = initialKeyCapacity;
            mask = keyCapacity - 1;
            final long sizeBytes = entrySize * keyCapacity;
            if (memStart == 0) {
                memStart = Unsafe.malloc(sizeBytes, memoryTag);
            } else {
                memStart = Unsafe.realloc(memStart, memLimit - memStart, sizeBytes, memoryTag);
            }
            memLimit = memStart + sizeBytes;
        }

        if (keyMemStart == 0) {
            keyMemStart = Unsafe.malloc(KEY_SIZE, memoryTag);
        }

        if (zeroMemStart == 0) {
            zeroMemStart = Unsafe.malloc(entrySize, memoryTag);
        }

        clear();
    }

    @Override
    public void setKeyCapacity(int newKeyCapacity) {
        long requiredCapacity = (long) (newKeyCapacity / loadFactor);
        if (requiredCapacity > MAX_SAFE_INT_POW_2) {
            throw CairoException.nonCritical().put("map capacity overflow");
        }
        rehash(Numbers.ceilPow2((int) requiredCapacity));
    }

    @Override
    public long size() {
        return hasZero ? size + 1 : size;
    }

    @Override
    public MapValue valueAt(long startAddress) {
        return valueOf(startAddress, false, value);
    }

    @Override
    public MapKey withKey() {
        return key.init();
    }

    private Unordered16MapValue asNew(long startAddress, long key1, long key2, long hashCode, Unordered16MapValue value) {
        Unsafe.getUnsafe().putLong(startAddress, key1);
        Unsafe.getUnsafe().putLong(startAddress + 8L, key2);
        if (--free == 0) {
            rehash();
            // Index may have changed after rehash, so we need to find the key.
            startAddress = getStartAddress(hashCode & mask);
            for (; ; ) {
                long k1 = Unsafe.getUnsafe().getLong(startAddress);
                long k2 = Unsafe.getUnsafe().getLong(startAddress + 8L);
                if (k1 == key1 && k2 == key2) {
                    break;
                }
                startAddress = getNextAddress(startAddress);
            }
        }
        size++;
        return valueOf(startAddress, true, value);
    }

    // Advance through the map data structure sequentially,
    // avoiding multiplication and pseudo-random access.
    private long getNextAddress(long entryAddress) {
        entryAddress += entrySize;
        if (entryAddress < memLimit) {
            return entryAddress;
        }
        return memStart;
    }

    private long getStartAddress(long memStart, long index) {
        return memStart + entrySize * index;
    }

    private long getStartAddress(long index) {
        return getStartAddress(memStart, index);
    }

    private Unordered16MapValue probe0(long key1, long key2, long startAddress, long hashCode, Unordered16MapValue value) {
        for (; ; ) {
            startAddress = getNextAddress(startAddress);
            long k1 = Unsafe.getUnsafe().getLong(startAddress);
            long k2 = Unsafe.getUnsafe().getLong(startAddress + 8L);
            if (k1 == 0 && k2 == 0) {
                return asNew(startAddress, key1, key2, hashCode, value);
            } else if (k1 == key1 && k2 == key2) {
                return valueOf(startAddress, false, value);
            }
        }
    }

    private Unordered16MapValue probeReadOnly(long key1, long key2, long startAddress, Unordered16MapValue value) {
        for (; ; ) {
            startAddress = getNextAddress(startAddress);
            long k1 = Unsafe.getUnsafe().getLong(startAddress);
            long k2 = Unsafe.getUnsafe().getLong(startAddress + 8L);
            if (k1 == 0 && k2 == 0) {
                return null;
            } else if (k1 == key1 && k2 == key2) {
                return valueOf(startAddress, false, value);
            }
        }
    }

    private void rehash() {
        rehash((long) keyCapacity << 1);
    }

    private void rehash(long newKeyCapacity) {
        if (nResizes == maxResizes) {
            throw LimitOverflowException.instance().put("limit of ").put(maxResizes).put(" resizes exceeded in unordered map");
        }
        if (newKeyCapacity > MAX_SAFE_INT_POW_2) {
            throw CairoException.nonCritical().put("map capacity overflow");
        }
        if (newKeyCapacity <= keyCapacity) {
            return;
        }

        final long newSizeBytes = entrySize * newKeyCapacity;
        final long newMemStart = Unsafe.malloc(newSizeBytes, memoryTag);
        final long newMemLimit = newMemStart + newSizeBytes;
        Vect.memset(newMemStart, newSizeBytes, 0);
        final int newMask = (int) newKeyCapacity - 1;

        for (long addr = memStart; addr < memLimit; addr += entrySize) {
            long key1 = Unsafe.getUnsafe().getLong(addr);
            long key2 = Unsafe.getUnsafe().getLong(addr + 8L);
            if (key1 == 0 && key2 == 0) {
                continue;
            }

            long newAddr = getStartAddress(newMemStart, Hash.hashLong128_64(key1, key2) & newMask);
            while (Unsafe.getUnsafe().getLong(newAddr) != 0 || Unsafe.getUnsafe().getLong(newAddr + 8L) != 0) {
                newAddr += entrySize;
                if (newAddr >= newMemLimit) {
                    newAddr = newMemStart;
                }
            }
            Vect.memcpy(newAddr, addr, entrySize);
        }

        Unsafe.free(memStart, memLimit - memStart, memoryTag);

        memStart = newMemStart;
        memLimit = newMemStart + newSizeBytes;
        mask = newMask;
        free += (int) ((newKeyCapacity - keyCapacity) * loadFactor);
        keyCapacity = (int) newKeyCapacity;
        nResizes++;
    }

    private Unordered16MapValue valueOf(long startAddress, boolean newValue, Unordered16MapValue value) {
        return value.of(startAddress, memLimit, newValue);
    }

    long entrySize() {
        return entrySize;
    }

    boolean isZeroKey(long startAddress) {
        long key1 = Unsafe.getUnsafe().getLong(startAddress);
        long key2 = Unsafe.getUnsafe().getLong(startAddress + 8L);
        return key1 == 0 && key2 == 0;
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
            Key srcFastKey = (Key) srcKey;
            copyFromRawKey(srcFastKey.startAddress());
        }

        @Override
        public MapValue createValue() {
            long key1 = Unsafe.getUnsafe().getLong(keyMemStart);
            long key2 = Unsafe.getUnsafe().getLong(keyMemStart + 8L);
            if (key1 == 0 && key2 == 0) {
                return createZeroKeyValue();
            }
            return createNonZeroKeyValue(key1, key2, Hash.hashLong128_64(key1, key2));
        }

        @Override
        public MapValue createValue(long hashCode) {
            long key1 = Unsafe.getUnsafe().getLong(keyMemStart);
            long key2 = Unsafe.getUnsafe().getLong(keyMemStart + 8L);
            if (key1 == 0 && key2 == 0) {
                return createZeroKeyValue();
            }
            return createNonZeroKeyValue(key1, key2, hashCode);
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
            long key1 = Unsafe.getUnsafe().getLong(keyMemStart);
            long key2 = Unsafe.getUnsafe().getLong(keyMemStart + 8L);
            return Hash.hashLong128_64(key1, key2);
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
            putLong(value);
        }

        @Override
        public void putDouble(double value) {
            Unsafe.getUnsafe().putDouble(appendAddress, value);
            appendAddress += 8L;
        }

        @Override
        public void putFloat(float value) {
            Unsafe.getUnsafe().putFloat(appendAddress, value);
            appendAddress += 4L;
        }

        @Override
        public void putIPv4(int value) {
            putInt(value);
        }

        @Override
        public void putInt(int value) {
            Unsafe.getUnsafe().putInt(appendAddress, value);
            appendAddress += 4L;
        }

        @Override
        public void putLong(long value) {
            Unsafe.getUnsafe().putLong(appendAddress, value);
            appendAddress += 8L;
        }

        @Override
        public void putLong128(long lo, long hi) {
            Unsafe.getUnsafe().putLong(appendAddress, lo);
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES, hi);
            appendAddress += 16L;
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
            putLong(value);
        }

        @Override
        public void putVarchar(Utf8Sequence value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void skip(int bytes) {
            appendAddress += bytes;
        }

        private MapValue createNonZeroKeyValue(long key1, long key2, long hashCode) {
            long index = hashCode & mask;
            long startAddress = getStartAddress(index);
            long k1 = Unsafe.getUnsafe().getLong(startAddress);
            long k2 = Unsafe.getUnsafe().getLong(startAddress + 8L);
            if (k1 == 0 && k2 == 0) {
                return asNew(startAddress, key1, key2, hashCode, value);
            } else if (k1 == key1 && k2 == key2) {
                return valueOf(startAddress, false, value);
            }
            return probe0(key1, key2, startAddress, hashCode, value);
        }

        private MapValue createZeroKeyValue() {
            if (hasZero) {
                return valueOf(zeroMemStart, false, value);
            }
            hasZero = true;
            return valueOf(zeroMemStart, true, value);
        }

        private MapValue findValue(Unordered16MapValue value) {
            long key1 = Unsafe.getUnsafe().getLong(keyMemStart);
            long key2 = Unsafe.getUnsafe().getLong(keyMemStart + 8L);
            if (key1 == 0 && key2 == 0) {
                return hasZero ? valueOf(zeroMemStart, false, value) : null;
            }

            long hashCode = Hash.hashLong128_64(key1, key2);
            long index = hashCode & mask;
            long startAddress = getStartAddress(index);
            long k1 = Unsafe.getUnsafe().getLong(startAddress);
            long k2 = Unsafe.getUnsafe().getLong(startAddress + 8L);
            if (k1 == 0 && k2 == 0) {
                return null;
            } else if (k1 == key1 && k2 == key2) {
                return valueOf(startAddress, false, value);
            }
            return probeReadOnly(key1, key2, startAddress, value);
        }

        void copyFromRawKey(long srcPtr) {
            long srcKey1 = Unsafe.getUnsafe().getLong(srcPtr);
            long srcKey2 = Unsafe.getUnsafe().getLong(srcPtr + 8L);
            Unsafe.getUnsafe().putLong(appendAddress, srcKey1);
            Unsafe.getUnsafe().putLong(appendAddress + 8L, srcKey2);
            appendAddress += KEY_SIZE;
        }

        long startAddress() {
            return keyMemStart;
        }
    }
}
