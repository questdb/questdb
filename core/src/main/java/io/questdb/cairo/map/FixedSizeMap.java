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

import static io.questdb.std.Numbers.MAX_SAFE_INT_POW_2;

/**
 * FixedSizeMap a.k.a. FastMap is a general purpose ordered off-heap hash table used to store
 * intermediate data of join, group by, sample by queries, but not only. It provides {@link MapKey}
 * and {@link MapValue}, as well as {@link RecordCursor} interfaces for data access and modification.
 * The preferred way to create an OrderedMap is {@link MapFactory}.
 * <p>
 * Map iteration provided by {@link RecordCursor} preserves the key insertion order, hence
 * the ordered map name.
 * <strong>Important!</strong>
 * Key and value structures must match the ones provided via lists of columns ({@link ColumnTypes})
 * to the map constructor. Later put* calls made on {@link MapKey} and {@link MapValue} must match
 * the declared column types to guarantee memory access safety.
 * <p>
 * Keys and values are expected to be fixed-size. Only insertions and updates operations are
 * supported meaning that a key can't  be removed from the map once it was inserted.
 * <p>
 * The hash table is organized into two main parts:
 * <ul>
 * <li>1. Off-heap list for heap offsets and cached hash codes</li>
 * <li>2. Off-heap memory for key-value pairs a.k.a. "key memory"</li>
 * </ul>
 * The offset list contains [compressed_offset] items. An offset value contains an offset to
 * the address of a key-value pair in the key memory compressed to an int. Key-value
 * pair addresses are 8 byte aligned, so a FastMap is capable of holding up to 32GB of data.
 * <p>
 * The offset list is used as a hash table with linear probing. So, a table resize allocates a new
 * offset list and copies offsets there while the key memory stays as is.
 * <p>
 * Key-value pairs stored in the key memory may have the following layout:
 * <pre>
 * | Key columns 0..K | Value columns 0..V |
 * +------------------+--------------------+
 * |        -         |         -          |
 * +------------------+--------------------+
 */
public class FixedSizeMap implements Map, Reopenable {

    private static final long MAX_HEAP_SIZE = (Integer.toUnsignedLong(-1) - 1) << 3;
    private static final int MIN_KEY_CAPACITY = 16;
    private final FixedSizeMapCursor cursor;
    private final int heapMemoryTag;
    private final Key key = new Key();
    private final long keySize;
    private final int listMemoryTag;
    private final double loadFactor;
    private final int maxResizes;
    private final FixedSizeMapRecord record;
    private final FixedSizeMapValue value;
    private final FixedSizeMapValue value2;
    private final FixedSizeMapValue value3;
    private final long valueSize;
    private int free;
    private long heapLimit; // Heap memory limit pointer.
    private long heapSize;
    private long heapStart; // Heap memory start pointer.
    private long initialHeapSize;
    private int initialKeyCapacity;
    private long kPos;      // Current key-value memory pointer (contains searched key / pending key-value pair).
    private int keyCapacity;
    private long mask;
    private int nResizes;
    // Holds compressed offsets.
    // Offsets are shifted by +1 (0 -> 1, 1 -> 2, etc.), so that we fill the memory with 0.
    private DirectIntList offsets;
    private int size = 0;

    public FixedSizeMap(
            long heapSize,
            @Transient @NotNull ColumnTypes keyTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes
    ) {
        this(heapSize, keyTypes, null, keyCapacity, loadFactor, maxResizes);
    }

    public FixedSizeMap(
            long heapSize,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes,
            int memoryTag
    ) {
        this(heapSize, keyTypes, valueTypes, keyCapacity, loadFactor, maxResizes, memoryTag, memoryTag);
    }

    public FixedSizeMap(
            long heapSize,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes
    ) {
        this(heapSize, keyTypes, valueTypes, keyCapacity, loadFactor, maxResizes, MemoryTag.NATIVE_FAST_MAP, MemoryTag.NATIVE_FAST_MAP_INT_LIST);
    }

    FixedSizeMap(
            long heapSize,
            @NotNull @Transient ColumnTypes keyTypes,
            @Nullable @Transient ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes,
            int heapMemoryTag,
            int listMemoryTag
    ) {
        assert heapSize > 3;
        assert loadFactor > 0 && loadFactor < 1d;

        try {
            this.heapMemoryTag = heapMemoryTag;
            this.listMemoryTag = listMemoryTag;
            initialHeapSize = heapSize;
            this.loadFactor = loadFactor;
            heapStart = kPos = Unsafe.malloc(heapSize, heapMemoryTag);
            this.heapSize = heapSize;
            heapLimit = heapStart + heapSize;
            this.keyCapacity = (int) (keyCapacity / loadFactor);
            this.keyCapacity = this.initialKeyCapacity = Math.max(Numbers.ceilPow2(this.keyCapacity), MIN_KEY_CAPACITY);
            mask = this.keyCapacity - 1;
            free = (int) (this.keyCapacity * loadFactor);
            offsets = new DirectIntList(this.keyCapacity, listMemoryTag);
            offsets.setPos(this.keyCapacity);
            offsets.zero(0);
            nResizes = 0;
            this.maxResizes = maxResizes;

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
            this.keySize = keySize;

            if (keySize == -1) {
                throw CairoException.nonCritical().put("var-size keys are not supported");
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
                        close();
                        throw CairoException.nonCritical().put("value type is not supported: ").put(ColumnType.nameOf(columnType));
                    }
                    valueOffset += size;
                    valueSize += size;
                }
            }
            this.valueSize = valueSize;

            value = new FixedSizeMapValue(valueSize, valueOffsets);
            value2 = new FixedSizeMapValue(valueSize, valueOffsets);
            value3 = new FixedSizeMapValue(valueSize, valueOffsets);

            assert keySize + valueSize <= heapLimit - heapStart : "page size is too small to fit a single key";

            record = new FixedSizeMapRecord(keySize, valueSize, valueOffsets, value, keyTypes, valueTypes);
            cursor = new FixedSizeMapCursor(record, this);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        kPos = heapStart;
        free = (int) (keyCapacity * loadFactor);
        size = 0;
        offsets.zero(0);
        nResizes = 0;
    }

    @Override
    public void close() {
        Misc.free(offsets);
        if (heapStart != 0) {
            heapStart = Unsafe.free(heapStart, heapSize, heapMemoryTag);
            heapLimit = kPos = 0;
            free = 0;
            size = 0;
            heapSize = 0;
        }
    }

    public long getAppendOffset() {
        return kPos;
    }

    @Override
    public MapRecordCursor getCursor() {
        return cursor.init(heapStart, heapLimit, size);
    }

    @Override
    public long getHeapSize() {
        return heapLimit - heapStart;
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
    public long getUsedHeapSize() {
        return kPos - heapStart;
    }

    @Override
    public boolean isOpen() {
        return heapStart != 0;
    }

    @Override
    public void merge(Map srcMap, MapValueMergeFunction mergeFunc) {
        assert this != srcMap;
        if (srcMap.size() == 0) {
            return;
        }
        final FixedSizeMap srcFixedMap = (FixedSizeMap) srcMap;

        long entrySize = keySize + valueSize;
        long alignedEntrySize = Bytes.align8b(entrySize);

        OUTER:
        for (long srcAddress = srcFixedMap.heapStart; srcAddress < srcFixedMap.kPos; srcAddress += alignedEntrySize) {
            long hashCode = Hash.hashMem64(srcAddress, keySize);
            long index = hashCode & mask;

            int destRawOffset;
            while ((destRawOffset = getRawOffset(offsets, index)) != 0) {
                long destAddress = heapStart + getOffset(destRawOffset);
                if (Vect.memeq(destAddress, srcAddress, keySize)) {
                    // Match found, merge values.
                    mergeFunc.merge(valueAt(destAddress), srcMap.valueAt(srcAddress));
                    continue OUTER;
                }
                index = (index + 1) & mask;
            }

            if (kPos + entrySize > heapLimit) {
                resize(entrySize, kPos);
            }
            Vect.memcpy(kPos, srcAddress, entrySize);
            setOffset(offsets, index, kPos - heapStart);
            kPos += alignedEntrySize;
            size++;
            if (--free == 0) {
                rehash();
            }
        }
    }

    @Override
    public void reopen(int keyCapacity, long heapSize) {
        if (heapStart == 0) {
            keyCapacity = (int) (keyCapacity / loadFactor);
            initialKeyCapacity = Math.max(Numbers.ceilPow2(keyCapacity), MIN_KEY_CAPACITY);
            initialHeapSize = heapSize;
            restoreInitialCapacity();
        }
    }

    public void reopen() {
        if (heapStart == 0) {
            // handles both mem and offsets
            restoreInitialCapacity();
        }
    }

    @Override
    public void restoreInitialCapacity() {
        if (heapSize != initialHeapSize || keyCapacity != initialKeyCapacity) {
            try {
                heapStart = kPos = Unsafe.realloc(heapStart, heapLimit - heapStart, heapSize = initialHeapSize, heapMemoryTag);
                heapLimit = heapStart + initialHeapSize;
                keyCapacity = initialKeyCapacity;
                keyCapacity = keyCapacity < MIN_KEY_CAPACITY ? MIN_KEY_CAPACITY : Numbers.ceilPow2(keyCapacity);
                mask = keyCapacity - 1;
                offsets.resetCapacity();
                offsets.setCapacity(keyCapacity);
                offsets.setPos(keyCapacity);
                clear();
            } catch (Throwable t) {
                close();
                throw t;
            }
        }
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
        return valueOf(startAddress, startAddress + keySize, false, value);
    }

    @Override
    public MapKey withKey() {
        return key.init();
    }

    private static long getOffset(int rawOffset) {
        return ((long) rawOffset - 1) << 3;
    }

    private static int getRawOffset(DirectIntList offsets, long index) {
        return offsets.get(index);
    }

    private static void setOffset(DirectIntList offsets, long index, long offset) {
        offsets.set(index, (int) ((offset >> 3) + 1));
    }

    private FixedSizeMapValue asNew(Key keyWriter, long index, FixedSizeMapValue value) {
        // Align current pointer to 8 bytes, so that we can store compressed offsets.
        kPos = Bytes.align8b(keyWriter.appendAddress + valueSize);
        setOffset(offsets, index, keyWriter.startAddress - heapStart);
        size++;
        if (--free == 0) {
            rehash();
        }
        return valueOf(keyWriter.startAddress, keyWriter.appendAddress, true, value);
    }

    private FixedSizeMapValue probe0(Key keyWriter, long index, FixedSizeMapValue value) {
        int rawOffset;
        while ((rawOffset = getRawOffset(offsets, index = (index + 1) & mask)) != 0) {
            long offset = getOffset(rawOffset);
            if (keyWriter.eq(offset)) {
                long startAddress = heapStart + offset;
                return valueOf(startAddress, startAddress + keySize, false, value);
            }
        }
        return asNew(keyWriter, index, value);
    }

    private FixedSizeMapValue probeReadOnly(Key keyWriter, long index, FixedSizeMapValue value) {
        int rawOffset;
        while ((rawOffset = getRawOffset(offsets, index = (index + 1) & mask)) != 0) {
            long offset = getOffset(rawOffset);
            if (keyWriter.eq(offset)) {
                long startAddress = heapStart + offset;
                return valueOf(startAddress, startAddress + keySize, false, value);
            }
        }
        return null;
    }

    private void rehash() {
        rehash((long) keyCapacity << 1);
    }

    private void rehash(long newKeyCapacity) {
        if (newKeyCapacity > MAX_SAFE_INT_POW_2) {
            throw CairoException.nonCritical().put("map capacity overflow");
        }
        if (newKeyCapacity <= keyCapacity) {
            return;
        }

        mask = (int) newKeyCapacity - 1;
        DirectIntList newOffsets = new DirectIntList(newKeyCapacity, listMemoryTag);
        newOffsets.setPos(newKeyCapacity);
        newOffsets.zero(0);

        for (int i = 0, k = (int) offsets.size(); i < k; i++) {
            int rawOffset = getRawOffset(offsets, i);
            if (rawOffset == 0) {
                continue;
            }
            long offset = getOffset(rawOffset);
            long hashCode = Hash.hashMem64(heapStart + offset, keySize);
            long index = hashCode & mask;
            while (getRawOffset(newOffsets, index) != 0) {
                index = (index + 1) & mask;
            }
            setOffset(newOffsets, index, offset);
        }
        offsets.close();
        offsets = newOffsets;
        free += (int) ((newKeyCapacity - keyCapacity) * loadFactor);
        keyCapacity = (int) newKeyCapacity;
    }

    // Returns delta between new and old heapStart addresses.
    private long resize(long entrySize, long appendAddress) {
        assert appendAddress >= heapStart;
        if (nResizes < maxResizes) {
            nResizes++;
            long kCapacity = (heapLimit - heapStart) << 1;
            long target = appendAddress + entrySize - heapStart;
            if (kCapacity < target) {
                kCapacity = Numbers.ceilPow2(target);
            }
            if (kCapacity > MAX_HEAP_SIZE) {
                throw LimitOverflowException.instance().put("limit of ").put(MAX_HEAP_SIZE).put(" memory exceeded in FixedSizeMap");
            }
            long kAddress = Unsafe.realloc(heapStart, heapSize, kCapacity, heapMemoryTag);

            this.heapSize = kCapacity;
            long delta = kAddress - heapStart;
            kPos += delta;
            assert kPos > 0;

            this.heapStart = kAddress;
            this.heapLimit = kAddress + kCapacity;

            return delta;
        } else {
            throw LimitOverflowException.instance().put("limit of ").put(maxResizes).put(" resizes exceeded in FixedSizeMap");
        }
    }

    private FixedSizeMapValue valueOf(long startAddress, long valueAddress, boolean newValue, FixedSizeMapValue value) {
        return value.of(startAddress, valueAddress, heapLimit, newValue);
    }

    long keySize() {
        return keySize;
    }

    long valueSize() {
        return valueSize;
    }

    class Key implements MapKey {
        private long appendAddress;
        private long startAddress;

        @Override
        public long commit() {
            return keySize;
        }

        @Override
        public void copyFrom(MapKey srcKey) {
            Key srcFixedKey = (Key) srcKey;
            copyFromRawKey(srcFixedKey.startAddress, keySize);
        }

        public void copyFromRawKey(long srcPtr, long srcSize) {
            assert srcSize == keySize;
            Vect.memcpy(appendAddress, srcPtr, srcSize);
            appendAddress += srcSize;
        }

        @Override
        public MapValue createValue() {
            long hashCode = hash();
            return createValue(hashCode);
        }

        @Override
        public MapValue createValue(long hashCode) {
            long index = hashCode & mask;
            int rawOffset = getRawOffset(offsets, index);
            long offset;
            if (rawOffset == 0) {
                return asNew(this, index, value);
            } else if (eq(offset = getOffset(rawOffset))) {
                long startAddress = heapStart + offset;
                return valueOf(startAddress, startAddress + keySize, false, value);
            }
            return probe0(this, index, value);
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
            return Hash.hashMem64(startAddress, keySize);
        }

        public Key init() {
            reset();
            checkCapacity(keySize);
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
            Unsafe.getUnsafe().putLong(appendAddress, value.getLong0());
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES, value.getLong1());
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES * 2, value.getLong2());
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES * 3, value.getLong3());
            appendAddress += 32L;
        }

        @Override
        public void putLong256(long l0, long l1, long l2, long l3) {
            Unsafe.getUnsafe().putLong(appendAddress, l0);
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES, l1);
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES * 2, l2);
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES * 3, l3);
            appendAddress += 32L;
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

        public void reset() {
            startAddress = kPos;
            appendAddress = kPos;
        }

        @Override
        public void skip(int bytes) {
            appendAddress += bytes;
        }

        private void checkCapacity(long requiredKeySize) {
            long requiredSize = requiredKeySize + valueSize;
            if (appendAddress + requiredSize > heapLimit) {
                long delta = resize(requiredSize, appendAddress);
                startAddress += delta;
                appendAddress += delta;
                assert startAddress > 0;
                assert appendAddress > 0;
            }
        }

        private boolean eq(long offset) {
            return Vect.memeq(heapStart + offset, startAddress, keySize);
        }

        private MapValue findValue(FixedSizeMapValue value) {
            long hashCode = hash();
            long index = hashCode & mask;
            int rawOffset = getRawOffset(offsets, index);
            long offset;
            if (rawOffset == 0) {
                return null;
            } else if (eq(offset = getOffset(rawOffset))) {
                long startAddress = heapStart + offset;
                return valueOf(startAddress, startAddress + keySize, false, value);
            } else {
                return probeReadOnly(this, index, value);
            }
        }
    }
}
