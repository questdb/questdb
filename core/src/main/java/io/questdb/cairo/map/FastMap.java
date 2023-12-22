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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * FastMap is a general purpose off-heap hash table used to store intermediate data of join,
 * group by, sample by queries, but not only. It provides {@link MapKey} and {@link MapValue},
 * as well as {@link RecordCursor} interfaces for data access and modification.
 * The preferred way to create a FastMap is {@link MapFactory}.
 * <p>
 * <strong>Important!</strong>
 * Key and value structures must match the ones provided via lists of columns ({@link ColumnTypes})
 * to the map constructor. Later put* calls made on {@link MapKey} and {@link MapValue} must match
 * the declared column types to guarantee memory access safety.
 * <p>
 * Keys may be var-size, i.e. a key may contain string or binary columns, while values are expected
 * to be fixed-size. Only insertions and updates operations are supported meaning that a key can't
 * be removed from the map once it was inserted.
 * <p>
 * Map iteration provided by {@link RecordCursor} preserves the key insertion order.
 * <p>
 * The hash table is organized into two main parts:
 * <ul>
 * <li>1. Off-heap list for heap offsets and cached hash codes</li>
 * <li>2. Off-heap memory for key-value pairs a.k.a. "key memory"</li>
 * </ul>
 * The offset list contains [compressed_offset, hash_code] pairs. An offset value contains an offset to
 * the address of a key-value pair in the key memory compressed to an int. Key-value pair addresses are
 * 8 byte aligned, so a FastMap is capable of holding up to 32GB of data.
 * <p>
 * The offset list is used as a hash table with linear probing. So, a table resize allocates a new
 * offset list and copies offsets there while the key memory stays as is.
 * <p>
 * Key-value pairs stored in the key memory may have the following layout:
 * <pre>
 * |       length         | Key columns 0..K | Value columns 0..V |
 * +----------------------+------------------+--------------------+
 * |      4 bytes         |        -         |         -          |
 * +----------------------+------------------+--------------------+
 * </pre>
 * Length field is present for var-size keys only. It stores key length in bytes.
 */
public class FastMap implements Map, Reopenable {

    private static final long MAX_HEAP_SIZE = (Integer.toUnsignedLong(-1) - 1) << 3;
    private static final int MIN_INITIAL_CAPACITY = 128;
    private final FastMapCursor cursor;
    private final int heapMemoryTag;
    private final int initialKeyCapacity;
    private final int initialPageSize;
    private final BaseKey key;
    private final int keyOffset;
    // Set to -1 when key is var-size.
    private final int keySize;
    private final int listMemoryTag;
    private final double loadFactor;
    private final int maxResizes;
    private final MergeFunction mergeRef;
    private final FastMapRecord record;
    private final FastMapValue value;
    private final FastMapValue value2;
    private final FastMapValue value3;
    private final int valueColumnCount;
    private final int valueSize;
    private long capacity;
    private int free;
    private long heapLimit; // Heap memory limit pointer.
    private long heapStart; // Heap memory start pointer.
    private long kPos;      // Current key-value memory pointer (contains searched key / pending key-value pair).
    private int keyCapacity;
    private int mask;
    private int nResizes;
    // Holds [compressed_offset, hash_code] pairs.
    // Offsets are shifted by +1 (0 -> 1, 1 -> 2, etc.), so that we fill the memory with 0.
    private DirectIntList offsets;
    private int size = 0;

    public FastMap(
            int pageSize,
            @Transient @NotNull ColumnTypes keyTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes
    ) {
        this(pageSize, keyTypes, null, keyCapacity, loadFactor, maxResizes);
    }

    public FastMap(
            int pageSize,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes,
            int memoryTag
    ) {
        this(pageSize, keyTypes, valueTypes, keyCapacity, loadFactor, maxResizes, memoryTag, memoryTag);
    }

    public FastMap(
            int pageSize,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes
    ) {
        this(pageSize, keyTypes, valueTypes, keyCapacity, loadFactor, maxResizes, MemoryTag.NATIVE_FAST_MAP, MemoryTag.NATIVE_FAST_MAP_INT_LIST);
    }

    FastMap(
            int pageSize,
            @NotNull @Transient ColumnTypes keyTypes,
            @Nullable @Transient ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes,
            int heapMemoryTag,
            int listMemoryTag
    ) {
        assert pageSize > 3;
        assert loadFactor > 0 && loadFactor < 1d;

        this.heapMemoryTag = heapMemoryTag;
        this.listMemoryTag = listMemoryTag;
        initialKeyCapacity = keyCapacity;
        initialPageSize = pageSize;
        this.loadFactor = loadFactor;
        heapStart = kPos = Unsafe.malloc(capacity = pageSize, heapMemoryTag);
        heapLimit = heapStart + pageSize;
        this.keyCapacity = (int) (keyCapacity / loadFactor);
        this.keyCapacity = this.keyCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(this.keyCapacity);
        mask = this.keyCapacity - 1;
        free = (int) (this.keyCapacity * loadFactor);
        offsets = new DirectIntList((long) this.keyCapacity << 1, listMemoryTag);
        offsets.setPos((long) this.keyCapacity << 1);
        offsets.zero(0);
        nResizes = 0;
        this.maxResizes = maxResizes;

        final int keyColumnCount = keyTypes.getColumnCount();
        int keySize = 0;
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

        // Reserve 4 bytes for key length in case of var-size keys.
        keyOffset = keySize != -1 ? 0 : Integer.BYTES;

        int valueOffset = 0;
        long[] valueOffsets = null;
        int valueSize = 0;
        if (valueTypes != null) {
            valueColumnCount = valueTypes.getColumnCount();
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
        } else {
            valueColumnCount = 0;
        }
        this.valueSize = valueSize;

        value = new FastMapValue(valueSize, valueOffsets);
        value2 = new FastMapValue(valueSize, valueOffsets);
        value3 = new FastMapValue(valueSize, valueOffsets);

        assert keySize + valueSize <= heapLimit - heapStart : "page size is too small to fit a single key";
        if (keySize == -1) {
            record = new FastMapVarSizeRecord(valueSize, valueOffsets, value, keyTypes, valueTypes);
            key = new VarSizeKey();
            mergeRef = this::mergeVarSizeKey;
        } else {
            record = new FastMapFixedSizeRecord(keySize, valueSize, valueOffsets, value, keyTypes, valueTypes);
            key = new FixedSizeKey();
            mergeRef = this::mergeFixedSizeKey;
        }
        cursor = new FastMapCursor(record, this);
    }

    @Override
    public void clear() {
        kPos = heapStart;
        free = (int) (keyCapacity * loadFactor);
        size = 0;
        offsets.zero(0);
    }

    @Override
    public final void close() {
        Misc.free(offsets);
        if (heapStart != 0) {
            Unsafe.free(heapStart, capacity, heapMemoryTag);
            heapLimit = heapStart = kPos = 0;
            free = 0;
            size = 0;
            capacity = 0;
        }
    }

    public long getAppendOffset() {
        return kPos;
    }

    @Override
    public MapRecordCursor getCursor() {
        return cursor.init(heapStart, heapLimit, size);
    }

    public long getHeapSize() {
        return heapLimit - heapStart;
    }

    public int getKeyCapacity() {
        return keyCapacity;
    }

    @Override
    public MapRecord getRecord() {
        return record;
    }

    @TestOnly
    public long getUsedHeapSize() {
        return kPos - heapStart;
    }

    public int getValueColumnCount() {
        return valueColumnCount;
    }

    @Override
    public void merge(Map srcMap, MapValueMergeFunction mergeFunc) {
        assert this != srcMap;
        if (srcMap.size() == 0) {
            return;
        }
        mergeRef.merge((FastMap) srcMap, mergeFunc);
    }

    public void reopen() {
        if (heapStart == 0) {
            // handles both mem and offsets
            restoreInitialCapacity();
        }
    }

    @Override
    public void restoreInitialCapacity() {
        if (capacity != initialPageSize || keyCapacity != offsets.getCapacity()) {
            heapStart = kPos = Unsafe.realloc(heapStart, heapLimit - heapStart, capacity = initialPageSize, heapMemoryTag);
            heapLimit = heapStart + initialPageSize;
            keyCapacity = (int) (initialKeyCapacity / loadFactor);
            keyCapacity = keyCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(keyCapacity);
            mask = keyCapacity - 1;
            free = (int) (keyCapacity * loadFactor);
            offsets.resetCapacity();
            offsets.setCapacity((long) keyCapacity << 1);
            offsets.setPos((long) keyCapacity << 1);
            offsets.zero(0);
            nResizes = 0;
        }
    }

    @Override
    public void setKeyCapacity(int newKeyCapacity) {
        if (newKeyCapacity > keyCapacity) {
            rehash(Numbers.ceilPow2((int) (newKeyCapacity / loadFactor)));
        }
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public MapValue valueAt(long startAddress) {
        int keySize = this.keySize;
        if (keySize == -1) {
            keySize = Unsafe.getUnsafe().getInt(startAddress);
        }
        return valueOf(startAddress, startAddress + keyOffset + keySize, false, value);
    }

    @Override
    public MapKey withKey() {
        return key.init();
    }

    private static int getHashCode(DirectIntList offsets, int index) {
        return offsets.get(((long) index << 1) | 1);
    }

    private static long getOffset(DirectIntList offsets, int index) {
        return ((long) offsets.get((long) index << 1) - 1) << 3;
    }

    private static void setHashCode(DirectIntList offsets, int index, int hashCode) {
        offsets.set(((long) index << 1) | 1, hashCode);
    }

    private static void setOffset(DirectIntList offsets, int index, long offset) {
        offsets.set((long) index << 1, (int) ((offset >> 3) + 1));
    }

    private FastMapValue asNew(BaseKey keyWriter, int index, int hashCode, FastMapValue value) {
        // Align current pointer to 8 bytes, so that we can store compressed offsets.
        kPos = Bytes.align8b(keyWriter.appendAddress + valueSize);
        setOffset(offsets, index, keyWriter.startAddress - heapStart);
        setHashCode(offsets, index, hashCode);
        if (--free == 0) {
            rehash();
        }
        size++;
        return valueOf(keyWriter.startAddress, keyWriter.appendAddress, true, value);
    }

    private void mergeFixedSizeKey(FastMap srcMap, MapValueMergeFunction mergeFunc) {
        assert keySize >= 0;
        setKeyCapacity(size + srcMap.size);

        int len = keySize + valueSize;
        long alignedLen = Bytes.align8b(len);

        OUTER:
        for (int i = 0, k = (int) (srcMap.offsets.size() / 2); i < k; i++) {
            long offset = getOffset(srcMap.offsets, i);
            if (offset < 0) {
                continue;
            }

            long srcStartAddress = srcMap.heapStart + offset;
            int hashCode = getHashCode(srcMap.offsets, i);
            int index = hashCode & mask;

            long destOffset;
            long destStartAddress;
            while ((destOffset = getOffset(offsets, index)) > -1) {
                if (
                        hashCode == getHashCode(offsets, index)
                                && Vect.memeq((destStartAddress = heapStart + destOffset), srcStartAddress, keySize)
                ) {
                    // Match found, merge values.
                    mergeFunc.merge(
                            valueAt(destStartAddress),
                            srcMap.valueAt(srcStartAddress)
                    );
                    continue OUTER;
                }
                index = (index + 1) & mask;
            }

            assert free > 0;
            if (kPos + len > heapLimit) {
                resize(len);
            }
            Vect.memcpy(kPos, srcStartAddress, len);
            setOffset(offsets, index, kPos - heapStart);
            setHashCode(offsets, index, hashCode);
            kPos += alignedLen;
            free--;
            size++;
        }
    }

    private void mergeVarSizeKey(FastMap srcMap, MapValueMergeFunction mergeFunc) {
        assert keySize == -1;
        setKeyCapacity(size + srcMap.size);

        OUTER:
        for (int i = 0, k = (int) (srcMap.offsets.size() / 2); i < k; i++) {
            long offset = getOffset(srcMap.offsets, i);
            if (offset < 0) {
                continue;
            }

            long srcStartAddress = srcMap.heapStart + offset;
            int srcKeySize = Unsafe.getUnsafe().getInt(srcStartAddress);
            int hashCode = getHashCode(srcMap.offsets, i);
            int index = hashCode & mask;

            long destOffset;
            long destStartAddress;
            while ((destOffset = getOffset(offsets, index)) > -1) {
                if (
                        hashCode == getHashCode(offsets, index)
                                && Unsafe.getUnsafe().getInt((destStartAddress = heapStart + destOffset)) == srcKeySize
                                && Vect.memeq(destStartAddress + keyOffset, srcStartAddress + keyOffset, srcKeySize)
                ) {
                    // Match found, merge values.
                    mergeFunc.merge(
                            valueAt(destStartAddress),
                            srcMap.valueAt(srcStartAddress)
                    );
                    continue OUTER;
                }
                index = (index + 1) & mask;
            }

            assert free > 0;
            int len = keyOffset + srcKeySize + valueSize;
            if (kPos + len > heapLimit) {
                resize(len);
            }
            Vect.memcpy(kPos, srcStartAddress, len);
            setOffset(offsets, index, kPos - heapStart);
            setHashCode(offsets, index, hashCode);
            kPos = Bytes.align8b(kPos + len);
            free--;
            size++;
        }
    }

    private FastMapValue probe0(BaseKey keyWriter, int index, int hashCode, int keySize, FastMapValue value) {
        long offset;
        while ((offset = getOffset(offsets, index = (++index & mask))) > -1) {
            if (hashCode == getHashCode(offsets, index) && keyWriter.eq(offset)) {
                long startAddress = heapStart + offset;
                return valueOf(startAddress, startAddress + keyOffset + keySize, false, value);
            }
        }
        return asNew(keyWriter, index, hashCode, value);
    }

    private FastMapValue probeReadOnly(BaseKey keyWriter, int index, int hashCode, int keySize, FastMapValue value) {
        long offset;
        while ((offset = getOffset(offsets, index = (++index & mask))) > -1) {
            if (hashCode == getHashCode(offsets, index) && keyWriter.eq(offset)) {
                long startAddress = heapStart + offset;
                return valueOf(startAddress, startAddress + keyOffset + keySize, false, value);
            }
        }
        return null;
    }

    private void rehash() {
        rehash(keyCapacity << 1);
    }

    private void rehash(int newKeyCapacity) {
        if (newKeyCapacity <= keyCapacity) {
            return;
        }

        mask = newKeyCapacity - 1;
        DirectIntList newOffsets = new DirectIntList((long) newKeyCapacity << 1, listMemoryTag);
        newOffsets.setPos((long) newKeyCapacity << 1);
        newOffsets.zero(0);

        for (int i = 0, k = (int) (offsets.size() / 2); i < k; i++) {
            long offset = getOffset(offsets, i);
            if (offset < 0) {
                continue;
            }
            int hashCode = getHashCode(offsets, i);
            int index = hashCode & mask;
            while (getOffset(newOffsets, index) > -1) {
                index = (index + 1) & mask;
            }
            setOffset(newOffsets, index, offset);
            setHashCode(newOffsets, index, hashCode);
        }
        offsets.close();
        offsets = newOffsets;
        free += (int) ((newKeyCapacity - keyCapacity) * loadFactor);
        keyCapacity = newKeyCapacity;
    }

    private void resize(int size) {
        if (nResizes < maxResizes) {
            nResizes++;
            long kCapacity = (heapLimit - heapStart) << 1;
            long target = key.appendAddress + size + valueSize - heapStart;
            if (kCapacity < target) {
                kCapacity = Numbers.ceilPow2(target);
            }
            if (kCapacity > MAX_HEAP_SIZE) {
                throw LimitOverflowException.instance().put("limit of ").put(MAX_HEAP_SIZE).put(" memory exceeded in FastMap");
            }
            long kAddress = Unsafe.realloc(heapStart, capacity, kCapacity, heapMemoryTag);

            this.capacity = kCapacity;
            long delta = kAddress - heapStart;
            kPos += delta;
            key.startAddress += delta;
            key.appendAddress += delta;

            assert kPos > 0;
            assert key.startAddress > 0;
            assert key.appendAddress > 0;

            this.heapStart = kAddress;
            this.heapLimit = kAddress + kCapacity;
        } else {
            throw LimitOverflowException.instance().put("limit of ").put(maxResizes).put(" resizes exceeded in FastMap");
        }
    }

    private FastMapValue valueOf(long startAddress, long valueAddress, boolean newValue, FastMapValue value) {
        return value.of(startAddress, valueAddress, heapLimit, newValue);
    }

    int keySize() {
        return keySize;
    }

    int valueSize() {
        return valueSize;
    }

    @FunctionalInterface
    private interface MergeFunction {
        void merge(FastMap srcMap, MapValueMergeFunction mergeFunc);
    }

    abstract class BaseKey implements MapKey {
        protected long appendAddress;
        protected long startAddress;

        @Override
        public MapValue createValue() {
            int keySize = commit();
            // calculate hash remembering "key" structure
            // [ key size | key block | value block ]
            int hashCode = hash();
            return createValue(keySize, hashCode);
        }

        @Override
        public MapValue createValue(int hashCode) {
            int keySize = commit();
            return createValue(keySize, hashCode);
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

        public BaseKey init() {
            startAddress = kPos;
            appendAddress = kPos + keyOffset;
            return this;
        }

        @Override
        public void put(Record record, RecordSink sink) {
            sink.copy(record, this);
        }

        @Override
        public void putRecord(Record value) {
            // no-op
        }

        private MapValue createValue(int keySize, int hashCode) {
            int index = hashCode & mask;
            long offset = getOffset(offsets, index);
            if (offset < 0) {
                return asNew(this, index, hashCode, value);
            } else if (hashCode == getHashCode(offsets, index) && eq(offset)) {
                long startAddress = heapStart + offset;
                return valueOf(startAddress, startAddress + keyOffset + keySize, false, value);
            }
            return probe0(this, index, hashCode, keySize, value);
        }

        private MapValue findValue(FastMapValue value) {
            int keySize = commit();
            int hashCode = hash();
            int index = hashCode & mask;
            long offset = getOffset(offsets, index);

            if (offset < 0) {
                return null;
            } else if (hashCode == getHashCode(offsets, index) && eq(offset)) {
                long startAddress = heapStart + offset;
                return valueOf(startAddress, startAddress + keyOffset + keySize, false, value);
            } else {
                return probeReadOnly(this, index, hashCode, keySize, value);
            }
        }

        protected void checkSize(int requiredKeySize) {
            if (appendAddress + requiredKeySize + valueSize > heapLimit) {
                resize(requiredKeySize);
            }
        }

        abstract void copyFromRawKey(long ptr, int size);

        protected abstract boolean eq(long offset);
    }

    class FixedSizeKey extends BaseKey {

        @Override
        public int commit() {
            return keySize;
        }

        @Override
        public void copyFrom(MapKey srcKey) {
            FixedSizeKey srcFastKey = (FixedSizeKey) srcKey;
            copyFromRawKey(srcFastKey.startAddress, keySize);
        }

        @Override
        public int hash() {
            return Hash.hashMem32(startAddress, keySize);
        }

        public FixedSizeKey init() {
            super.init();
            checkSize(keySize);
            return this;
        }

        @Override
        public void putBin(BinarySequence value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putBool(boolean value) {
            assert appendAddress + Byte.BYTES <= heapLimit;
            Unsafe.getUnsafe().putByte(appendAddress, (byte) (value ? 1 : 0));
            appendAddress += Byte.BYTES;
        }

        @Override
        public void putByte(byte value) {
            assert appendAddress + Byte.BYTES <= heapLimit;
            Unsafe.getUnsafe().putByte(appendAddress, value);
            appendAddress += Byte.BYTES;
        }

        @Override
        public void putChar(char value) {
            assert appendAddress + Character.BYTES <= heapLimit;
            Unsafe.getUnsafe().putChar(appendAddress, value);
            appendAddress += Character.BYTES;
        }

        @Override
        public void putDate(long value) {
            putLong(value);
        }

        @Override
        public void putDouble(double value) {
            assert appendAddress + Double.BYTES <= heapLimit;
            Unsafe.getUnsafe().putDouble(appendAddress, value);
            appendAddress += Double.BYTES;
        }

        @Override
        public void putFloat(float value) {
            assert appendAddress + Float.BYTES <= heapLimit;
            Unsafe.getUnsafe().putFloat(appendAddress, value);
            appendAddress += Float.BYTES;
        }

        @Override
        public void putInt(int value) {
            assert appendAddress + Integer.BYTES <= heapLimit;
            Unsafe.getUnsafe().putInt(appendAddress, value);
            appendAddress += Integer.BYTES;
        }

        @Override
        public void putLong(long value) {
            assert appendAddress + Long.BYTES <= heapLimit;
            Unsafe.getUnsafe().putLong(appendAddress, value);
            appendAddress += Long.BYTES;
        }

        @Override
        public void putLong128(long lo, long hi) {
            assert appendAddress + 16 <= heapLimit;
            Unsafe.getUnsafe().putLong(appendAddress, lo);
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES, hi);
            appendAddress += 16;
        }

        @Override
        public void putLong256(Long256 value) {
            assert appendAddress + Long256.BYTES <= heapLimit;
            Unsafe.getUnsafe().putLong(appendAddress, value.getLong0());
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES, value.getLong1());
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES * 2, value.getLong2());
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES * 3, value.getLong3());
            appendAddress += Long256.BYTES;
        }

        @Override
        public void putShort(short value) {
            assert appendAddress + Short.BYTES <= heapLimit;
            Unsafe.getUnsafe().putShort(appendAddress, value);
            appendAddress += Short.BYTES;
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
        public void skip(int bytes) {
            appendAddress += bytes;
        }

        @Override
        void copyFromRawKey(long srcPtr, int srcSize) {
            assert srcSize == keySize;
            Vect.memcpy(appendAddress, srcPtr, srcSize);
            appendAddress += srcSize;
        }

        @Override
        protected boolean eq(long offset) {
            return Vect.memeq(heapStart + offset, startAddress, keySize);
        }
    }

    class VarSizeKey extends BaseKey {
        private int len;

        @Override
        public int commit() {
            Unsafe.getUnsafe().putInt(startAddress, len = (int) (appendAddress - startAddress - keyOffset));
            return len;
        }

        @Override
        public void copyFrom(MapKey srcKey) {
            VarSizeKey srcFastKey = (VarSizeKey) srcKey;
            copyFromRawKey(srcFastKey.startAddress + keyOffset, srcFastKey.len);
        }

        @Override
        public int hash() {
            return Hash.hashMem32(startAddress + keyOffset, len);
        }

        @Override
        public void putBin(BinarySequence value) {
            if (value == null) {
                putVarSizeNull();
            } else {
                long len = value.length() + Integer.BYTES;
                if (len > Integer.MAX_VALUE) {
                    throw CairoException.nonCritical().put("binary column is too large");
                }

                checkSize((int) len);
                int l = (int) (len - Integer.BYTES);
                Unsafe.getUnsafe().putInt(appendAddress, l);
                value.copyTo(appendAddress + Integer.BYTES, 0, l);
                appendAddress += len;
            }
        }

        @Override
        public void putBool(boolean value) {
            checkSize(1);
            Unsafe.getUnsafe().putByte(appendAddress, (byte) (value ? 1 : 0));
            appendAddress += 1;
        }

        @Override
        public void putByte(byte value) {
            checkSize(1);
            Unsafe.getUnsafe().putByte(appendAddress, value);
            appendAddress += 1;
        }

        @Override
        public void putChar(char value) {
            checkSize(Character.BYTES);
            Unsafe.getUnsafe().putChar(appendAddress, value);
            appendAddress += Character.BYTES;
        }

        @Override
        public void putDate(long value) {
            putLong(value);
        }

        @Override
        public void putDouble(double value) {
            checkSize(Double.BYTES);
            Unsafe.getUnsafe().putDouble(appendAddress, value);
            appendAddress += Double.BYTES;
        }

        @Override
        public void putFloat(float value) {
            checkSize(Float.BYTES);
            Unsafe.getUnsafe().putFloat(appendAddress, value);
            appendAddress += Float.BYTES;
        }

        @Override
        public void putInt(int value) {
            checkSize(Integer.BYTES);
            Unsafe.getUnsafe().putInt(appendAddress, value);
            appendAddress += Integer.BYTES;
        }

        @Override
        public void putLong(long value) {
            checkSize(Long.BYTES);
            Unsafe.getUnsafe().putLong(appendAddress, value);
            appendAddress += Long.BYTES;
        }

        @Override
        public void putLong128(long lo, long hi) {
            checkSize(16);
            Unsafe.getUnsafe().putLong(appendAddress, lo);
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES, hi);
            appendAddress += 16;
        }

        @Override
        public void putLong256(Long256 value) {
            checkSize(Long256.BYTES);
            Unsafe.getUnsafe().putLong(appendAddress, value.getLong0());
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES, value.getLong1());
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES * 2, value.getLong2());
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES * 3, value.getLong3());
            appendAddress += Long256.BYTES;
        }

        @Override
        public void putShort(short value) {
            checkSize(Short.BYTES);
            Unsafe.getUnsafe().putShort(appendAddress, value);
            appendAddress += Short.BYTES;
        }

        @Override
        public void putStr(CharSequence value) {
            if (value == null) {
                putVarSizeNull();
                return;
            }

            int len = value.length();
            checkSize((len << 1) + Integer.BYTES);
            Unsafe.getUnsafe().putInt(appendAddress, len);
            appendAddress += Integer.BYTES;
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putChar(appendAddress + ((long) i << 1), value.charAt(i));
            }
            appendAddress += (long) len << 1;
        }

        @Override
        public void putStr(CharSequence value, int lo, int hi) {
            int len = hi - lo;
            checkSize((len << 1) + Integer.BYTES);
            Unsafe.getUnsafe().putInt(appendAddress, len);
            appendAddress += Integer.BYTES;
            for (int i = lo; i < hi; i++) {
                Unsafe.getUnsafe().putChar(appendAddress + ((long) (i - lo) << 1), value.charAt(i));
            }
            appendAddress += (long) len << 1;
        }

        @Override
        public void putStrLowerCase(CharSequence value) {
            if (value == null) {
                putVarSizeNull();
                return;
            }

            int len = value.length();
            checkSize((len << 1) + Integer.BYTES);
            Unsafe.getUnsafe().putInt(appendAddress, len);
            appendAddress += Integer.BYTES;
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putChar(appendAddress + ((long) i << 1), Character.toLowerCase(value.charAt(i)));
            }
            appendAddress += (long) len << 1;
        }

        @Override
        public void putStrLowerCase(CharSequence value, int lo, int hi) {
            int len = hi - lo;
            checkSize((len << 1) + Integer.BYTES);
            Unsafe.getUnsafe().putInt(appendAddress, len);
            appendAddress += Integer.BYTES;
            for (int i = lo; i < hi; i++) {
                Unsafe.getUnsafe().putChar(appendAddress + ((long) (i - lo) << 1), Character.toLowerCase(value.charAt(i)));
            }
            appendAddress += (long) len << 1;
        }

        @Override
        public void putTimestamp(long value) {
            putLong(value);
        }

        @Override
        public void skip(int bytes) {
            checkSize(bytes);
            appendAddress += bytes;
        }

        private void putVarSizeNull() {
            checkSize(4);
            Unsafe.getUnsafe().putInt(appendAddress, TableUtils.NULL_LEN);
            appendAddress += Integer.BYTES;
        }

        @Override
        void copyFromRawKey(long srcPtr, int srcSize) {
            checkSize(srcSize);
            Vect.memcpy(appendAddress, srcPtr, srcSize);
            appendAddress += srcSize;
        }

        @Override
        protected boolean eq(long offset) {
            long a = heapStart + offset;
            long b = startAddress;
            // Check the length first.
            if (Unsafe.getUnsafe().getInt(a) != Unsafe.getUnsafe().getInt(b)) {
                return false;
            }
            return Vect.memeq(a + keyOffset, b + keyOffset, len);
        }
    }
}
