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
 * <li>1. Off-heap list for heap offsets</li>
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
 * |       length         | Value columns 0..V | Key columns 0..K |
 * +----------------------+--------------------+------------------+
 * |      4 bytes         |         -          |        -         |
 * +----------------------+--------------------+------------------+
 * </pre>
 * Length field is present for var-size keys only. It stores full key-value pair length in bytes.
 */
public class FastMap implements Map, Reopenable {

    private static final long MAX_HEAP_SIZE = (Integer.toUnsignedLong(-1) - 1) << 3;
    private static final int MIN_INITIAL_CAPACITY = 128;
    private final FastMapCursor cursor;
    private final int initialKeyCapacity;
    private final int initialPageSize;
    private final BaseKey key;
    private final int keyOffset;
    // Set to -1 when key is var-size.
    private final int keySize;
    private final int listMemoryTag;
    private final double loadFactor;
    private final int mapMemoryTag;
    private final int maxResizes;
    private final FastMapRecord record;
    private final FastMapValue value;
    private final FastMapValue value2;
    private final FastMapValue value3;
    private final int valueColumnCount;
    private final int valueSize;
    private long capacity;
    private int free;
    private long kLimit; // Key memory limit pointer.
    private long kPos;   // Current key memory pointer.
    private long kStart; // Key memory start pointer.
    private int keyCapacity;
    private int mask;
    private int nResizes;
    // Offsets are shifted by +1 (0 -> 1, 1 -> 2, etc.), so that we fill the memory with 0.
    private DirectLongList offsets;
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
        this(pageSize, keyTypes, valueTypes, keyCapacity, loadFactor, maxResizes, MemoryTag.NATIVE_FAST_MAP, MemoryTag.NATIVE_FAST_MAP_LONG_LIST);
    }

    @TestOnly
    FastMap(
            int pageSize,
            @NotNull @Transient ColumnTypes keyTypes,
            @Nullable @Transient ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes,
            int mapMemoryTag,
            int listMemoryTag
    ) {
        assert pageSize > 3;
        assert loadFactor > 0 && loadFactor < 1d;

        this.mapMemoryTag = mapMemoryTag;
        this.listMemoryTag = listMemoryTag;
        initialKeyCapacity = keyCapacity;
        initialPageSize = pageSize;
        this.loadFactor = loadFactor;
        kStart = kPos = Unsafe.malloc(capacity = pageSize, mapMemoryTag);
        kLimit = kStart + pageSize;
        this.keyCapacity = (int) (keyCapacity / loadFactor);
        this.keyCapacity = this.keyCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(this.keyCapacity);
        mask = this.keyCapacity - 1;
        free = (int) (this.keyCapacity * loadFactor);
        offsets = new DirectLongList(this.keyCapacity, listMemoryTag);
        offsets.setPos(this.keyCapacity);
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
        int offset = keySize != -1 ? 0 : 4;
        int[] valueOffsets = null;
        int valueSize = 0;
        if (valueTypes != null) {
            valueColumnCount = valueTypes.getColumnCount();
            valueOffsets = new int[valueColumnCount];

            for (int i = 0; i < valueColumnCount; i++) {
                valueOffsets[i] = offset;
                final int columnType = valueTypes.getColumnType(i);
                final int size = ColumnType.sizeOf(columnType);
                if (size <= 0) {
                    close();
                    throw CairoException.nonCritical().put("value type is not supported: ").put(ColumnType.nameOf(columnType));
                }
                offset += size;
                valueSize += size;
            }
        } else {
            valueColumnCount = 0;
        }
        this.valueSize = valueSize;
        keyOffset = offset;

        value = new FastMapValue(valueOffsets);
        value2 = new FastMapValue(valueOffsets);
        value3 = new FastMapValue(valueOffsets);

        record = new FastMapRecord(valueOffsets, keyOffset, value, keyTypes, valueTypes);

        assert keySize + valueSize < kLimit - kStart : "page size is too small to fit a single key";
        cursor = new FastMapCursor(record, this);
        key = keySize == -1 ? new VarSizeKey() : new FixedSizeKey();
    }

    @Override
    public void clear() {
        kPos = kStart;
        free = (int) (keyCapacity * loadFactor);
        size = 0;
        offsets.zero(0);
    }

    @Override
    public final void close() {
        Misc.free(offsets);
        if (kStart != 0) {
            Unsafe.free(kStart, capacity, mapMemoryTag);
            kLimit = kStart = kPos = 0;
            free = 0;
            size = 0;
            capacity = 0;
        }
    }

    public long getAppendOffset() {
        return kPos;
    }

    public long getAreaSize() {
        return kLimit - kStart;
    }

    @Override
    public RecordCursor getCursor() {
        return cursor.init(kStart, kLimit, size);
    }

    public int getKeyCapacity() {
        return keyCapacity;
    }

    @Override
    public MapRecord getRecord() {
        return record;
    }

    public int getValueColumnCount() {
        return valueColumnCount;
    }

    public void reopen() {
        if (kStart == 0) {
            // handles both mem and offsets
            restoreInitialCapacity();
        }
    }

    @Override
    public void restoreInitialCapacity() {
        kStart = kPos = Unsafe.realloc(kStart, kLimit - kStart, capacity = initialPageSize, mapMemoryTag);
        kLimit = kStart + initialPageSize;
        keyCapacity = (int) (initialKeyCapacity / loadFactor);
        keyCapacity = keyCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(keyCapacity);
        mask = keyCapacity - 1;
        free = (int) (keyCapacity * loadFactor);
        offsets.resetCapacity();
        offsets.setCapacity(keyCapacity);
        offsets.setPos(keyCapacity);
        offsets.zero(0);
        nResizes = 0;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public MapValue valueAt(long address) {
        return valueOf(address, false, value);
    }

    @Override
    public MapKey withKey() {
        return key.init();
    }

    private static long getPackedOffset(DirectLongList offsets, int index) {
        return offsets.get(index);
    }

    private static void setPackedOffset(DirectLongList offsets, int index, long offset, int hashCode) {
        offsets.set(index, Numbers.encodeLowHighInts((int) ((offset >> 3) + 1), hashCode));
    }

    private static void setPackedOffset(DirectLongList offsets, int index, long packedOffset) {
        offsets.set(index, packedOffset);
    }

    private static int unpackHashCode(long packedOffset) {
        return Numbers.decodeHighInt(packedOffset);
    }

    private static long unpackOffset(long packedOffset) {
        return (Integer.toUnsignedLong(Numbers.decodeLowInt(packedOffset)) - 1) << 3;
    }

    private FastMapValue asNew(BaseKey keyWriter, int index, int hashCode, FastMapValue value) {
        kPos = keyWriter.appendAddress;
        // Align current pointer to 8 bytes, so that we can store compressed offsets.
        if ((kPos & 0x7) != 0) {
            kPos |= 0x7;
            kPos++;
        }
        setPackedOffset(offsets, index, keyWriter.startAddress - kStart, hashCode);
        if (--free == 0) {
            rehash();
        }
        size++;
        return valueOf(keyWriter.startAddress, true, value);
    }

    private FastMapValue probe0(BaseKey keyWriter, int index, int hashCode, FastMapValue value) {
        long packedOffset;
        long offset;
        while ((offset = unpackOffset(packedOffset = getPackedOffset(offsets, index = (++index & mask)))) > -1) {
            if (hashCode == unpackHashCode(packedOffset) && keyWriter.eq(offset)) {
                return valueOf(kStart + offset, false, value);
            }
        }
        return asNew(keyWriter, index, hashCode, value);
    }

    private FastMapValue probeReadOnly(BaseKey keyWriter, int index, long hashCode, FastMapValue value) {
        long packedOffset;
        long offset;
        while ((offset = unpackOffset(packedOffset = getPackedOffset(offsets, index = (++index & mask)))) > -1) {
            if (hashCode == unpackHashCode(packedOffset) && keyWriter.eq(offset)) {
                return valueOf(kStart + offset, false, value);
            }
        }
        return null;
    }

    private void rehash() {
        int capacity = keyCapacity << 1;
        mask = capacity - 1;
        DirectLongList newOffsets = new DirectLongList(capacity, listMemoryTag);
        newOffsets.setPos(capacity);
        newOffsets.zero(0);

        for (int i = 0, k = (int) offsets.size(); i < k; i++) {
            long packedOffset = getPackedOffset(offsets, i);
            long offset = unpackOffset(packedOffset);
            if (offset < 0) {
                continue;
            }
            int hashCode = unpackHashCode(packedOffset);
            int index = hashCode & mask;
            while (unpackOffset(getPackedOffset(newOffsets, index)) > -1) {
                index = (index + 1) & mask;
            }
            setPackedOffset(newOffsets, index, packedOffset);
        }
        offsets.close();
        offsets = newOffsets;
        free += (capacity - keyCapacity) * loadFactor;
        keyCapacity = capacity;
    }

    private void resize(int size) {
        if (nResizes < maxResizes) {
            nResizes++;
            long kCapacity = (kLimit - kStart) << 1;
            long target = key.appendAddress + size - kStart;
            if (kCapacity < target) {
                kCapacity = Numbers.ceilPow2(target);
            }
            if (kCapacity > MAX_HEAP_SIZE) {
                throw LimitOverflowException.instance().put("limit of ").put(MAX_HEAP_SIZE).put(" memory exceeded in FastMap");
            }
            long kAddress = Unsafe.realloc(this.kStart, this.capacity, kCapacity, mapMemoryTag);

            this.capacity = kCapacity;
            long d = kAddress - this.kStart;
            kPos += d;
            key.startAddress += d;
            key.appendAddress += d;

            assert kPos > 0;
            assert key.startAddress > 0;
            assert key.appendAddress > 0;

            this.kStart = kAddress;
            this.kLimit = kAddress + kCapacity;
        } else {
            throw LimitOverflowException.instance().put("limit of ").put(maxResizes).put(" resizes exceeded in FastMap");
        }
    }

    private FastMapValue valueOf(long address, boolean newValue, FastMapValue value) {
        return value.of(address, kLimit, newValue);
    }

    int keySize() {
        return keySize;
    }

    int valueSize() {
        return valueSize;
    }

    private abstract class BaseKey implements MapKey {
        protected long appendAddress;
        protected long startAddress;

        @Override
        public MapValue createValue() {
            return createValue(value);
        }

        @Override
        public MapValue createValue2() {
            return createValue(value2);
        }

        @Override
        public MapValue createValue3() {
            return createValue(value3);
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

        private MapValue createValue(FastMapValue value) {
            commit();
            // calculate hash remembering "key" structure
            // [ len | value block | key offset block | key data block ]
            int hashCode = hash();
            int index = hashCode & mask;
            long packedOffset = getPackedOffset(offsets, index);
            long offset = unpackOffset(packedOffset);

            if (offset < 0) {
                return asNew(this, index, hashCode, value);
            } else if (hashCode == unpackHashCode(packedOffset) && eq(offset)) {
                return valueOf(kStart + offset, false, value);
            } else {
                return probe0(this, index, hashCode, value);
            }
        }

        private MapValue findValue(FastMapValue value) {
            commit();
            int hashCode = hash();
            int index = hashCode & mask;
            long packedOffset = getPackedOffset(offsets, index);
            long offset = unpackOffset(packedOffset);

            if (offset < 0) {
                return null;
            } else if (hashCode == unpackHashCode(packedOffset) && eq(offset)) {
                return valueOf(kStart + offset, false, value);
            } else {
                return probeReadOnly(this, index, hashCode, value);
            }
        }

        protected void checkSize(int size) {
            if (appendAddress + size > kLimit) {
                resize(size);
            }
        }

        protected void commit() {
            // no-op
        }

        protected abstract boolean eq(long offset);

        protected abstract int hash();
    }

    private class FixedSizeKey extends BaseKey {

        public FixedSizeKey init() {
            super.init();
            checkSize(keySize + valueSize);
            return this;
        }

        @Override
        public void putBin(BinarySequence value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putBool(boolean value) {
            assert appendAddress + Byte.BYTES <= kLimit;
            Unsafe.getUnsafe().putByte(appendAddress, (byte) (value ? 1 : 0));
            appendAddress += Byte.BYTES;
        }

        @Override
        public void putByte(byte value) {
            assert appendAddress + Byte.BYTES <= kLimit;
            Unsafe.getUnsafe().putByte(appendAddress, value);
            appendAddress += Byte.BYTES;
        }

        @Override
        public void putChar(char value) {
            assert appendAddress + Character.BYTES <= kLimit;
            Unsafe.getUnsafe().putChar(appendAddress, value);
            appendAddress += Character.BYTES;
        }

        @Override
        public void putDate(long value) {
            putLong(value);
        }

        @Override
        public void putDouble(double value) {
            assert appendAddress + Double.BYTES <= kLimit;
            Unsafe.getUnsafe().putDouble(appendAddress, value);
            appendAddress += Double.BYTES;
        }

        @Override
        public void putFloat(float value) {
            assert appendAddress + Float.BYTES <= kLimit;
            Unsafe.getUnsafe().putFloat(appendAddress, value);
            appendAddress += Float.BYTES;
        }

        @Override
        public void putInt(int value) {
            assert appendAddress + Integer.BYTES <= kLimit;
            Unsafe.getUnsafe().putInt(appendAddress, value);
            appendAddress += Integer.BYTES;
        }

        @Override
        public void putLong(long value) {
            assert appendAddress + Long.BYTES <= kLimit;
            Unsafe.getUnsafe().putLong(appendAddress, value);
            appendAddress += Long.BYTES;
        }

        @Override
        public void putLong128(long lo, long hi) {
            assert appendAddress + 16 <= kLimit;
            Unsafe.getUnsafe().putLong(appendAddress, lo);
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES, hi);
            appendAddress += 16;
        }

        @Override
        public void putLong256(Long256 value) {
            assert appendAddress + Long256.BYTES <= kLimit;
            Unsafe.getUnsafe().putLong(appendAddress, value.getLong0());
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES, value.getLong1());
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES * 2, value.getLong2());
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES * 3, value.getLong3());
            appendAddress += Long256.BYTES;
        }

        @Override
        public void putShort(short value) {
            assert appendAddress + Short.BYTES <= kLimit;
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
        public void putStrLowerCase(CharSequence value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putStrLowerCase(CharSequence value, int lo, int hi) {
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
        protected boolean eq(long offset) {
            return Vect.memeq(kStart + offset + keyOffset, startAddress + keyOffset, keySize);
        }

        @Override
        protected int hash() {
            return Hash.hashMem32(startAddress + keyOffset, keySize);
        }
    }

    private class VarSizeKey extends BaseKey {
        private int len;

        @Override
        public void putBin(BinarySequence value) {
            if (value == null) {
                putNull();
            } else {
                long len = value.length() + 4;
                if (len > Integer.MAX_VALUE) {
                    throw CairoException.nonCritical().put("binary column is too large");
                }

                checkSize((int) len);
                int l = (int) (len - 4);
                Unsafe.getUnsafe().putInt(appendAddress, l);
                value.copyTo(appendAddress + 4L, 0L, l);
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
            checkSize(2);
            Unsafe.getUnsafe().putShort(appendAddress, value);
            appendAddress += 2;
        }

        @Override
        public void putStr(CharSequence value) {
            if (value == null) {
                putNull();
                return;
            }

            int len = value.length();
            checkSize((len << 1) + 4);
            Unsafe.getUnsafe().putInt(appendAddress, len);
            appendAddress += 4;
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putChar(appendAddress + ((long) i << 1), value.charAt(i));
            }
            appendAddress += (long) len << 1;
        }

        @Override
        public void putStr(CharSequence value, int lo, int hi) {
            int len = hi - lo;
            checkSize((len << 1) + 4);
            Unsafe.getUnsafe().putInt(appendAddress, len);
            appendAddress += 4;
            for (int i = lo; i < hi; i++) {
                Unsafe.getUnsafe().putChar(appendAddress + ((long) (i - lo) << 1), value.charAt(i));
            }
            appendAddress += (long) len << 1;
        }

        @Override
        public void putStrLowerCase(CharSequence value) {
            if (value == null) {
                putNull();
                return;
            }

            int len = value.length();
            checkSize((len << 1) + 4);
            Unsafe.getUnsafe().putInt(appendAddress, len);
            appendAddress += 4;
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putChar(appendAddress + ((long) i << 1), Character.toLowerCase(value.charAt(i)));
            }
            appendAddress += (long) len << 1;
        }

        @Override
        public void putStrLowerCase(CharSequence value, int lo, int hi) {
            int len = hi - lo;
            checkSize((len << 1) + 4);
            Unsafe.getUnsafe().putInt(appendAddress, len);
            appendAddress += 4;
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

        private void putNull() {
            checkSize(4);
            Unsafe.getUnsafe().putInt(appendAddress, TableUtils.NULL_LEN);
            appendAddress += 4;
        }

        @Override
        protected void commit() {
            Unsafe.getUnsafe().putInt(startAddress, len = (int) (appendAddress - startAddress));
        }

        @Override
        protected boolean eq(long offset) {
            long a = kStart + offset;
            long b = startAddress;

            // Check the length first.
            if (Unsafe.getUnsafe().getInt(a) != Unsafe.getUnsafe().getInt(b)) {
                return false;
            }

            return Vect.memeq(a + keyOffset, b + keyOffset, this.len - keyOffset);
        }

        @Override
        protected int hash() {
            return Hash.hashMem32(startAddress + keyOffset, len - keyOffset);
        }
    }
}
