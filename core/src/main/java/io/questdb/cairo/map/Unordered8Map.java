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
 * Unordered8Map is a general purpose off-heap hash table with keys up to 8 bytes in size used
 * to store intermediate data of group by, sample by queries. It provides {@link MapKey} and
 * {@link MapValue}, as well as {@link RecordCursor} interfaces for data access and modification.
 * The preferred way to create an Unordered8Map is {@link MapFactory}.
 * <p>
 * Map iteration provided by {@link RecordCursor} does not preserve the key insertion order, hence
 * the unordered map name.
 * <strong>Important!</strong>
 * Key and value structures must match the ones provided via lists of columns ({@link ColumnTypes})
 * to the map constructor. Later put* calls made on {@link MapKey} and {@link MapValue} must match
 * the declared column types to guarantee memory access safety.
 * <p>
 * Keys must be fixed-size and up to 8 bytes total. Only insertions and updates operations are
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
 * |    up to 8 bytes     |        -         |         -          |
 * +----------------------+------------------+--------------------+
 * </pre>
 */
public class Unordered8Map implements Map, Reopenable {

    static final long KEY_SIZE = Long.BYTES;
    private static final byte EMPTY_META_BYTE = (byte) 0b10000000;
    private static final long MAX_SAFE_INT_POW_2 = 1L << 31;
    private static final int MIN_KEY_CAPACITY = 16; // Must be at least 8
    private final Unordered8MapCursor cursor;
    private final long entrySize;
    private final Key key;
    private final double loadFactor;
    private final int maxResizes;
    private final int memoryTag;
    private final Unordered8MapRecord record;
    private final Unordered8MapValue value;
    private final Unordered8MapValue value2;
    private final Unordered8MapValue value3;
    private int free;
    private long groupMask;
    private long initialKeyCapacity;
    private long keyCapacity;
    private long mask;
    private long memStart; // Hash table memory start pointer.
    private long metaMemStart; // Metadata memory start pointer.
    private int nResizes;
    private int size = 0;

    public Unordered8Map(
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes
    ) {
        this(keyTypes, valueTypes, keyCapacity, loadFactor, maxResizes, MemoryTag.NATIVE_UNORDERED_MAP);
    }

    Unordered8Map(
            @NotNull @Transient ColumnTypes keyTypes,
            @Nullable @Transient ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes,
            int memoryTag
    ) {
        assert loadFactor > 0 && loadFactor < 1d;

        try {
            this.memoryTag = memoryTag;
            this.loadFactor = loadFactor;
            this.keyCapacity = (long) (keyCapacity / loadFactor);
            this.keyCapacity = this.initialKeyCapacity = Math.max(Numbers.ceilPow2(this.keyCapacity), MIN_KEY_CAPACITY);
            this.maxResizes = maxResizes;
            mask = this.keyCapacity - 1;
            groupMask = (this.keyCapacity >>> 3) - 1;
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
            final long metaSizeBytes = this.keyCapacity;
            memStart = Unsafe.malloc(sizeBytes + metaSizeBytes, memoryTag);
            // It's not necessary to zero hash table memory,
            // but we do this to avoid minor page faults on inserts.
            Vect.memset(memStart, sizeBytes, 0);
            metaMemStart = memStart + sizeBytes;
            Vect.memset(metaMemStart, metaSizeBytes, EMPTY_META_BYTE & 0xFF);

            value = new Unordered8MapValue(valueSize, valueOffsets);
            value2 = new Unordered8MapValue(valueSize, valueOffsets);
            value3 = new Unordered8MapValue(valueSize, valueOffsets);

            record = new Unordered8MapRecord(valueSize, valueOffsets, value, keyTypes, valueTypes);
            cursor = new Unordered8MapCursor(record, this);
            key = new Key();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        free = (int) (keyCapacity * loadFactor);
        size = 0;
        nResizes = 0;
        Vect.memset(memStart, keyCapacity * entrySize, 0);
        Vect.memset(metaMemStart, keyCapacity, EMPTY_META_BYTE & 0xFF);
    }

    @Override
    public void close() {
        if (memStart != 0) {
            metaMemStart = memStart = Unsafe.free(memStart, keyCapacity * (entrySize + 1), memoryTag);
            free = 0;
            size = 0;
        }
    }

    @Override
    public MapRecordCursor getCursor() {
        return cursor.init(memStart, size, keyCapacity);
    }

    @Override
    public int getKeyCapacity() {
        return (int) keyCapacity;
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
        Unordered8Map src8Map = (Unordered8Map) srcMap;

        // TODO: use meta
        // TODO: use SWAR
        OUTER:
        for (long srcIndex = 0; srcIndex < src8Map.keyCapacity; srcIndex++) {
            if (Unsafe.getUnsafe().getByte(src8Map.metaMemStart + srcIndex) == EMPTY_META_BYTE) {
                continue;
            }

            long srcAddr = entryAddress(src8Map.memStart, srcIndex);
            long key = Unsafe.getUnsafe().getLong(srcAddr);
            long hashCode = Hash.hashLong64(key);

            long destIndex = (h1(hashCode) & groupMask) << 3;
            long destAddr;
            for (; ; ) {
                destAddr = entryAddress(destIndex);
                if (Unsafe.getUnsafe().getByte(metaMemStart + destIndex) == EMPTY_META_BYTE) {
                    break;
                }

                long k = Unsafe.getUnsafe().getLong(destAddr);
                if (k == key) {
                    // Match found, merge values.
                    mergeFunc.merge(
                            valueAt(destAddr),
                            src8Map.valueAt(srcAddr)
                    );
                    continue OUTER;
                }
                destIndex = (destIndex + 1) & mask;
            }

            Vect.memcpy(destAddr, srcAddr, entrySize);
            Unsafe.getUnsafe().putByte(metaMemStart + destIndex, (byte) h2(hashCode));
            size++;
            if (--free == 0) {
                rehash();
            }
        }
    }

    @Override
    public void reopen(int keyCapacity, long heapSize) {
        if (memStart == 0) {
            keyCapacity = (int) (keyCapacity / loadFactor);
            initialKeyCapacity = Math.max(Numbers.ceilPow2(keyCapacity), MIN_KEY_CAPACITY);
            restoreInitialCapacity();
        }
    }

    public void reopen() {
        if (memStart == 0) {
            restoreInitialCapacity();
        }
    }

    @Override
    public void restoreInitialCapacity() {
        if (memStart == 0 || keyCapacity != initialKeyCapacity) {
            final long oldKeyCapacity = keyCapacity;
            keyCapacity = initialKeyCapacity;
            mask = keyCapacity - 1;
            groupMask = (this.keyCapacity >>> 3) - 1;
            if (memStart == 0) {
                memStart = Unsafe.malloc(keyCapacity * (entrySize + 1), memoryTag);
            } else {
                memStart = Unsafe.realloc(memStart, oldKeyCapacity * (entrySize + 1), keyCapacity * (entrySize + 1), memoryTag);
            }
            metaMemStart = memStart + keyCapacity * entrySize;
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
        return size;
    }

    @Override
    public MapValue valueAt(long startAddress) {
        return valueOf(startAddress, false, value);
    }

    @Override
    public MapKey withKey() {
        return key;
    }

    private static long h1(long hashCode) {
        return hashCode >>> 7;
    }

    private static long h2(long hashCode) {
        return hashCode & 0x7FL;
    }

    private Unordered8MapValue asNew(long index, long key, long hashCode, Unordered8MapValue value) {
        long addr = entryAddress(index);
        Unsafe.getUnsafe().putLong(addr, key);
        Unsafe.getUnsafe().putByte(metaMemStart + index, (byte) h2(hashCode));
        if (--free == 0) {
            rehash();
            // Index may have changed after rehash, so we need to find the key.
            index = (h1(hashCode) & groupMask) << 3;
            for (; ; ) {
                // TODO use SWAR
                // We don't need to look at metadata since we know that the key is there.
                addr = entryAddress(index);
                long k = Unsafe.getUnsafe().getLong(addr);
                if (k == key) {
                    break;
                }
                index = (index + 1) & mask;
            }
        }
        size++;
        return valueOf(addr, true, value);
    }

    private long entryAddress(long memStart, long index) {
        return memStart + entrySize * index;
    }

    private long entryAddress(long index) {
        return memStart + entrySize * index;
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
        final long newMemStart = Unsafe.malloc(newSizeBytes + newKeyCapacity, memoryTag);
        final long newMetaMemStart = newMemStart + newSizeBytes;
        Vect.memset(newMemStart, newSizeBytes, 0);
        Vect.memset(newMetaMemStart, newKeyCapacity, EMPTY_META_BYTE & 0xFF);
        final long newMask = (int) newKeyCapacity - 1;
        final long newGroupMask = (newKeyCapacity >>> 3) - 1;

        for (long index = 0; index < keyCapacity; index++) {
            // TODO use SWAR
            if (Unsafe.getUnsafe().getByte(metaMemStart + index) == EMPTY_META_BYTE) {
                continue;
            }
            long addr = entryAddress(index);
            long key = Unsafe.getUnsafe().getLong(addr);

            long hashCode = Hash.hashLong64(key);
            long newIndex = (h1(hashCode) & newGroupMask) << 3;
            // TODO use SWAR
            while (Unsafe.getUnsafe().getByte(newMetaMemStart + newIndex) != EMPTY_META_BYTE) {
                newIndex = (newIndex + 1) & newMask;
            }
            long newAddr = entryAddress(newMemStart, newIndex);
            Vect.memcpy(newAddr, addr, entrySize);
            Unsafe.getUnsafe().putByte(newMetaMemStart + newIndex, (byte) h2(hashCode));
        }

        Unsafe.free(memStart, keyCapacity * (entrySize + 1), memoryTag);

        memStart = newMemStart;
        metaMemStart = newMetaMemStart;
        mask = newMask;
        groupMask = newGroupMask;
        free += (int) ((newKeyCapacity - keyCapacity) * loadFactor);
        keyCapacity = (int) newKeyCapacity;
        nResizes++;
    }

    private Unordered8MapValue valueOf(long startAddress, boolean newValue, Unordered8MapValue value) {
        return value.of(startAddress, newValue);
    }

    long entrySize() {
        return entrySize;
    }

    boolean isEmptyKey(long index) {
        return Unsafe.getUnsafe().getByte(metaMemStart + index) == EMPTY_META_BYTE;
    }

    class Key implements MapKey {
        private long key;

        @Override
        public long commit() {
            return KEY_SIZE; // we don't need to track the actual key size
        }

        @Override
        public void copyFrom(MapKey srcKey) {
            this.key = ((Key) srcKey).key;
        }

        @Override
        public MapValue createValue() {
            return createValue(key, Hash.hashLong64(key));
        }

        @Override
        public MapValue createValue(long hashCode) {
            return createValue(key, hashCode);
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
            return Hash.hashLong64(key);
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
            throw new UnsupportedOperationException();
        }

        @Override
        public void putByte(byte value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putChar(char value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putDate(long value) {
            putLong(value);
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
            throw new UnsupportedOperationException();
        }

        @Override
        public void putInt(int value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putLong(long value) {
            this.key = value;
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
            throw new UnsupportedOperationException();
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
            throw new UnsupportedOperationException();
        }

        private MapValue createValue(long key, long hashCode) {
            long index = (h1(hashCode) & groupMask) << 3;
            long h2word = SwarUtils.broadcast(h2(hashCode));
            for (; ; ) {
                long metaWord = Unsafe.getUnsafe().getLong(metaMemStart + index);
                long h2SearchWord = SwarUtils.markZeroBytes(metaWord ^ h2word);
                // If we've found a match for the h2 byte, slow down and check the key.
                while (h2SearchWord != 0) {
                    long pos = SwarUtils.indexOfFirstMarkedByte(h2SearchWord);
                    long addr = entryAddress(index + pos);
                    if (Unsafe.getUnsafe().getLong(addr) == key) {
                        return valueOf(addr, false, value);
                    }
                    h2SearchWord &= h2SearchWord - 1;
                }
                long emptyKeysWord = metaWord & 0x8080808080808080L;
                if (emptyKeysWord != 0) {
                    long pos = SwarUtils.indexOfFirstMarkedByte(emptyKeysWord);
                    return asNew(index + pos, key, hashCode, value);
                }
                index = (index + 8) & mask;
            }
        }

        private MapValue findValue(Unordered8MapValue value) {
            long hashCode = Hash.hashLong64(key);
            long index = (h1(hashCode) & groupMask) << 3;
            long h2word = SwarUtils.broadcast(h2(hashCode));
            for (; ; ) {
                long metaWord = Unsafe.getUnsafe().getLong(metaMemStart + index);
                long h2SearchWord = SwarUtils.markZeroBytes(metaWord ^ h2word);
                // If we've found a match for the h2 byte, slow down and check the key.
                while (h2SearchWord != 0) {
                    long pos = SwarUtils.indexOfFirstMarkedByte(h2SearchWord);
                    long addr = entryAddress(index + pos);
                    if (Unsafe.getUnsafe().getLong(addr) == key) {
                        return valueOf(addr, false, value);
                    }
                    h2SearchWord &= h2SearchWord - 1;
                }
                long emptyKeysWord = metaWord & 0x8080808080808080L;
                if (emptyKeysWord != 0) {
                    return null;
                }
                index = (index + 8) & mask;
            }
        }

        void copyFromRawKey(long srcPtr) {
            this.key = Unsafe.getUnsafe().getLong(srcPtr);
        }
    }
}
