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
import io.questdb.cairo.vm.MemoryCARWSpillable;
import io.questdb.cairo.vm.Vm;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
    private final int initialHeapSize;
    private final int initialKeyCapacity;
    private final BaseKey key;
    private final int keyOffset;
    // Set to -1 when key is var-size.
    private final int keySize;
    private final int listMemoryTag;
    private final double loadFactor;
    private final int mapMemoryTag;
    private final FastMapRecord record;
    private final FastMapValue value;
    private final FastMapValue value2;
    private final FastMapValue value3;
    private final int valueColumnCount;
    private final int valueSize;
    private int free;
    private int keyCapacity;
    private int mask;
    private MemoryCARWSpillable mem;
    // Offsets are shifted by +1 (0 -> 1, 1 -> 2, etc.), so that we fill the memory with 0.
    private DirectLongList offsets;
    private int size = 0;

    public FastMap(
            int heapSize,
            @Transient @NotNull ColumnTypes keyTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes
    ) {
        this(heapSize, keyTypes, null, keyCapacity, loadFactor, maxResizes);
    }

    public FastMap(
            int heapSize,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes,
            int memoryTag
    ) {
        this(heapSize, keyTypes, valueTypes, keyCapacity, loadFactor, maxResizes, memoryTag, memoryTag);
    }

    public FastMap(
            int heapSize,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes
    ) {
        this(heapSize, keyTypes, valueTypes, keyCapacity, loadFactor, maxResizes, MemoryTag.NATIVE_FAST_MAP, MemoryTag.NATIVE_FAST_MAP_LONG_LIST);
    }

    FastMap(
            int heapSize,
            @NotNull @Transient ColumnTypes keyTypes,
            @Nullable @Transient ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes,
            int mapMemoryTag,
            int listMemoryTag
    ) {
        assert heapSize > 3;
        assert loadFactor > 0 && loadFactor < 1d;

        this.mapMemoryTag = mapMemoryTag;
        this.listMemoryTag = listMemoryTag;
        initialKeyCapacity = keyCapacity;
        initialHeapSize = heapSize;
        this.loadFactor = loadFactor;
        this.keyCapacity = (int) (keyCapacity / loadFactor);
        this.keyCapacity = this.keyCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(this.keyCapacity);
        mask = this.keyCapacity - 1;
        free = (int) (this.keyCapacity * loadFactor);
        offsets = new DirectLongList(this.keyCapacity, listMemoryTag);
        offsets.setPos(this.keyCapacity);
        offsets.zero(0);
        long maxPagesFromMaxHeap = MAX_HEAP_SIZE/heapSize;
        int maxPages = (int) maxPagesFromMaxHeap;
        long maxPagesFromMaxResizes = 1L;
        int nResizes = 0;
        while(nResizes < maxResizes){
            maxPagesFromMaxResizes <<= 1;
            if(maxPagesFromMaxResizes > maxPagesFromMaxHeap) {
                break;
            }
            nResizes++;
        }
        if(nResizes == maxResizes){
            maxPages = (int) maxPagesFromMaxResizes;
        }
        // TODO: what to set for default size
        this.mem = new MemoryCARWSpillable(heapSize, maxPages, heapSize*3, Path.getThreadLocal("/tmp/questdb/fastmap"), mapMemoryTag, MemoryTag.MMAP_FAST_MAP);

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

        assert keySize + valueSize < heapSize : "page size is too small to fit a single key";
        cursor = new FastMapCursor(record, this);
        key = keySize == -1 ? new VarSizeKey() : new FixedSizeKey();
    }

    @Override
    public void clear() {
        mem.clear();
        free = (int) (keyCapacity * loadFactor);
        size = 0;
        offsets.zero(0);
    }

    @Override
    public final void close() {
        Misc.free(offsets);
        if(mem.getAddress() != 0){
            mem.close();
            free = 0;
            size = 0;
        }
    }

    public long getAppendOffset() {
        return mem.getAppendOffset();
    }

    public long getAreaSize() {
        return mem.size();
    }

    @Override
    public RecordCursor getCursor() {
        return cursor.init(mem.getAddress(), mem.getAddress() + mem.size(), size);
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
        if (mem.getAddress() == 0) {
            // handles both mem and offsets
            restoreInitialCapacity();
        }
    }

    @Override
    public void restoreInitialCapacity() {
        mem.truncate();
        keyCapacity = (int) (initialKeyCapacity / loadFactor);
        keyCapacity = keyCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(keyCapacity);
        mask = keyCapacity - 1;
        free = (int) (keyCapacity * loadFactor);
        offsets.resetCapacity();
        offsets.setCapacity(keyCapacity);
        offsets.setPos(keyCapacity);
        offsets.zero(0);
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
        long appendOffset = keyWriter.appendOffset;
        // Align current pointer to 8 bytes, so that we can store compressed offsets.
        if ((appendOffset & 0x7) != 0) {
            appendOffset |= 0x7;
            appendOffset++;
        }
        mem.jumpTo(appendOffset);
        setPackedOffset(offsets, index, keyWriter.startOffset, hashCode);
        if (--free == 0) {
            rehash();
        }
        size++;
        return valueOf(mem.getAddress() + keyWriter.startOffset, true, value);
    }

    private FastMapValue probe0(BaseKey keyWriter, int index, int hashCode, FastMapValue value) {
        long packedOffset;
        long offset;
        while ((offset = unpackOffset(packedOffset = getPackedOffset(offsets, index = (++index & mask)))) > -1) {
            if (hashCode == unpackHashCode(packedOffset) && keyWriter.eq(offset)) {
                return valueOf(mem.getAddress() + offset, false, value);
            }
        }
        return asNew(keyWriter, index, hashCode, value);
    }

    private FastMapValue probeReadOnly(BaseKey keyWriter, int index, long hashCode, FastMapValue value) {
        long packedOffset;
        long offset;
        while ((offset = unpackOffset(packedOffset = getPackedOffset(offsets, index = (++index & mask)))) > -1) {
            if (hashCode == unpackHashCode(packedOffset) && keyWriter.eq(offset)) {
                return valueOf(mem.getAddress() + offset, false, value);
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
        free += (int) ((capacity - keyCapacity) * loadFactor);
        keyCapacity = capacity;
    }

    private FastMapValue valueOf(long address, boolean newValue, FastMapValue value) {
        return value.of(address, (mem.getAddress() + mem.size()), newValue);
    }

    int keySize() {
        return keySize;
    }

    int valueSize() {
        return valueSize;
    }

    private abstract class BaseKey implements MapKey {
        protected long appendOffset;
        protected long startOffset;

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
            startOffset = mem.getAppendOffset();
            appendOffset = startOffset + keyOffset;
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
                return valueOf(mem.getAddress() + offset, false, value);
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
                return valueOf(mem.getAddress() + offset, false, value);
            } else {
                return probeReadOnly(this, index, hashCode, value);
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
            mem.appendAddressFor(startOffset, valueSize + keySize);
            return this;
        }

        @Override
        public void putBin(BinarySequence value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putBool(boolean value) {
            mem.putBool(appendOffset, value);
            appendOffset += Byte.BYTES;
        }

        @Override
        public void putByte(byte value) {
            mem.putByte(appendOffset, value);
            appendOffset += Byte.BYTES;
        }

        @Override
        public void putChar(char value) {
            mem.putChar(appendOffset, value);
            appendOffset += Character.BYTES;
        }

        @Override
        public void putDate(long value) {
            putLong(value);
        }

        @Override
        public void putDouble(double value) {
            mem.putDouble(appendOffset, value);
            appendOffset += Double.BYTES;
        }

        @Override
        public void putFloat(float value) {
            mem.putFloat(appendOffset, value);
            appendOffset += Float.BYTES;
        }

        @Override
        public void putInt(int value) {
            mem.putInt(appendOffset, value);
            appendOffset += Integer.BYTES;
        }

        @Override
        public void putLong(long value) {
            mem.putLong(appendOffset, value);
            appendOffset += Long.BYTES;
        }

        @Override
        public void putLong128(long lo, long hi) {
            mem.putLong128(appendOffset, lo, hi);
            appendOffset += 16;
        }

        @Override
        public void putLong256(Long256 value) {
            mem.putLong256(appendOffset, value);
            appendOffset += Long256.BYTES;
        }

        @Override
        public void putShort(short value) {
            mem.putShort(appendOffset, value);
            appendOffset += Short.BYTES;
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
            appendOffset += bytes;
        }

        @Override
        protected boolean eq(long offset) {
            return Vect.memeq(mem.getAddress() + offset + keyOffset, mem.getAddress() + startOffset + keyOffset, keySize);
        }

        @Override
        protected int hash() {
            return Hash.hashMem32(mem.getAddress() + startOffset + keyOffset, keySize);
        }
    }

    private class VarSizeKey extends BaseKey {
        private int len;

        @Override
        public void putBin(BinarySequence value) {
            // this looks like having different contract
            // with that in MemoryCARW
            // Here it only accepts 32bit, while mem accepts 2^64
            mem.putBin32(appendOffset, value);
            appendOffset += value != null ? value.length() + Integer.BYTES : Integer.BYTES;
        }

        @Override
        public void putBool(boolean value) {
            mem.putBool(appendOffset, value);
            appendOffset += 1;
        }

        @Override
        public void putByte(byte value) {
            mem.putByte(appendOffset, value);
            appendOffset += 1;
        }

        @Override
        public void putChar(char value) {
            mem.putChar(appendOffset, value);
            appendOffset += Character.BYTES;
        }

        @Override
        public void putDate(long value) {
            putLong(value);
        }

        @Override
        public void putDouble(double value) {
            mem.putDouble(appendOffset, value);
            appendOffset += Double.BYTES;
        }

        @Override
        public void putFloat(float value) {
            mem.putFloat(appendOffset, value);
            appendOffset += Float.BYTES;
        }

        @Override
        public void putInt(int value) {
            mem.putInt(appendOffset, value);
            appendOffset += Integer.BYTES;
        }

        @Override
        public void putLong(long value) {
            mem.putLong(appendOffset, value);
            appendOffset += Long.BYTES;
        }

        @Override
        public void putLong128(long lo, long hi) {
            mem.putLong128(appendOffset, lo, hi);
            appendOffset += Long.BYTES * 2;
        }

        @Override
        public void putLong256(Long256 value) {
            mem.putLong256(appendOffset, value);
            appendOffset += Long256.BYTES;
        }

        @Override
        public void putShort(short value) {
            mem.putShort(appendOffset, value);
            appendOffset += 2;
        }

        @Override
        public void putStr(CharSequence value) {
            // TODO: surprisingly putStr has same contract
            mem.putStr(appendOffset, value);
            appendOffset += value != null ? Vm.getStorageLength(value.length()) : Vm.getStorageLength(0);
        }

        @Override
        public void putStr(CharSequence value, int lo, int hi) {
            int len = hi - lo;
            mem.putStr(appendOffset, value, lo, len);
            appendOffset += value != null ? Vm.getStorageLength(len) : Vm.getStorageLength(0);
        }

        @Override
        public void putStrLowerCase(CharSequence value) {
            mem.putStrLowerCase(appendOffset, value);
            appendOffset += Vm.getStorageLength(value);
        }

        @Override
        public void putStrLowerCase(CharSequence value, int lo, int hi) {
            int len = hi - lo;
            mem.putStrLowerCase(appendOffset, value, lo, len);
            appendOffset += value != null ? Vm.getStorageLength(len) : Vm.getStorageLength(0);
        }

        @Override
        public void putTimestamp(long value) {
            putLong(value);
        }

        @Override
        public void skip(int bytes) {
            mem.skip((long) bytes);
            appendOffset += bytes;
        }

        @Override
        protected void commit() {
            mem.putInt(startOffset, len = (int) (appendOffset - startOffset));
        }

        @Override
        protected boolean eq(long offset) {
            long a = mem.getAddress() + offset;
            long b = mem.getAddress() + startOffset;

            // Check the length first.
            if (Unsafe.getUnsafe().getInt(a) != Unsafe.getUnsafe().getInt(b)) {
                return false;
            }

            return Vect.memeq(a + keyOffset, b + keyOffset, this.len - keyOffset);
        }

        @Override
        protected int hash() {
            return Hash.hashMem32(mem.getAddress() + startOffset + keyOffset, len - keyOffset);
        }
    }
}
