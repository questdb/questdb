/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

public class FastMap implements Map, Reopenable {

    private static final long MAX_HEAP_SIZE = ((long) (Integer.MAX_VALUE - 1)) << 3;
    private static final int MIN_INITIAL_CAPACITY = 128;
    private static final long OFFSET_SLOT_SIZE = 2;
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
    private long kLimit; // Heap limit pointer.
    private long kPos;   // Current heap pointer.
    private long kStart; // Heap start pointer.
    private int keyCapacity;
    private int mask;
    private int nResizes;
    // Offsets are shifted by +1 (0 -> 1, 1 -> 2, etc.), so that we fill the memory
    // with 0 instead of -1 when clearing/rehashing.
    // Each offset slot contains a [offset, hash code] pair.
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
        offsets = new DirectIntList(this.keyCapacity * OFFSET_SLOT_SIZE, listMemoryTag);
        offsets.setPos(this.keyCapacity * OFFSET_SLOT_SIZE);
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

        assert keySize + valueSize < kLimit - kStart : "page size is too small for to fit a single key";
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

    public long getAreaSize() {
        return kLimit - kStart;
    }

    @Override
    public RecordCursor getCursor() {
        return cursor.init(kStart, size);
    }

    public int getKeyCapacity() {
        return keyCapacity;
    }

    @Override
    public MapRecord getRecord() {
        return record;
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
        offsets.setCapacity(keyCapacity * OFFSET_SLOT_SIZE);
        offsets.setPos(keyCapacity * OFFSET_SLOT_SIZE);
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

    private static int getHashCode(DirectIntList offsets, int index) {
        return offsets.get(index * OFFSET_SLOT_SIZE + 1);
    }

    private static long getOffset(DirectIntList offsets, int index) {
        return ((long) (offsets.get(index * OFFSET_SLOT_SIZE) - 1)) << 3;
    }

    private static void setHashCode(DirectIntList offsets, int index, int hashCode) {
        offsets.set(index * OFFSET_SLOT_SIZE + 1, hashCode);
    }

    private static void setOffset(DirectIntList offsets, int index, long offset) {
        offsets.set(index * OFFSET_SLOT_SIZE, (int) ((offset >> 3) + 1));
    }

    private FastMapValue asNew(BaseKey keyWriter, int index, int hashCode, FastMapValue value) {
        kPos = keyWriter.appendAddress;
        // Align current pointer to 8 bytes, so that we can store compressed offsets.
        if ((kPos & 0x7) != 0) {
            kPos |= 0x7;
            kPos++;
        }
        setOffset(index, keyWriter.startAddress - kStart);
        setHashCode(index, hashCode);
        if (--free == 0) {
            rehash();
        }
        size++;
        return valueOf(keyWriter.startAddress, true, value);
    }

    private int getHashCode(int index) {
        return getHashCode(offsets, index);
    }

    private long getOffset(int index) {
        return getOffset(offsets, index);
    }

    private FastMapValue probe0(BaseKey keyWriter, int index, int hashCode, FastMapValue value) {
        long offset;
        while ((offset = getOffset(index = (++index & mask))) > -1) {
            if (hashCode == getHashCode(index) && keyWriter.eq(offset)) {
                return valueOf(kStart + offset, false, value);
            }
        }
        return asNew(keyWriter, index, hashCode, value);
    }

    private FastMapValue probeReadOnly(BaseKey keyWriter, int index, long hashCode, FastMapValue value) {
        long offset;
        while ((offset = getOffset(index = (++index & mask))) > -1) {
            if (hashCode == getHashCode(index) && keyWriter.eq(offset)) {
                return valueOf(kStart + offset, false, value);
            }
        }
        return null;
    }

    private void rehash() {
        int capacity = keyCapacity << 1;
        mask = capacity - 1;
        DirectIntList newOffsets = new DirectIntList(capacity * OFFSET_SLOT_SIZE, listMemoryTag);
        newOffsets.setPos(capacity * OFFSET_SLOT_SIZE);
        newOffsets.zero(0);

        for (int i = 0, k = (int) (offsets.size() / OFFSET_SLOT_SIZE); i < k; i++) {
            long offset = getOffset(i);
            if (offset < 0) {
                continue;
            }
            int hashCode = getHashCode(i);
            int index = hashCode & mask;
            while (getOffset(newOffsets, index) > -1) {
                index = (index + 1) & mask;
            }
            setOffset(newOffsets, index, offset);
            setHashCode(newOffsets, index, hashCode);
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
                System.out.println("boom");
            }
            assert kCapacity <= MAX_HEAP_SIZE : "Max FastMap heap size reached: " + kCapacity;
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

    private void setHashCode(int index, int hashCode) {
        setHashCode(offsets, index, hashCode);
    }

    private void setOffset(int index, long offset) {
        setOffset(offsets, index, offset);
    }

    private FastMapValue valueOf(long address, boolean newValue, FastMapValue value) {
        return value.of(address, newValue);
    }

    long getAppendOffset() {
        return kPos;
    }

    int getValueColumnCount() {
        return valueColumnCount;
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
        public void putBool(boolean value) {
            Unsafe.getUnsafe().putByte(appendAddress, (byte) (value ? 1 : 0));
            appendAddress += 1;
        }

        @Override
        public void putByte(byte value) {
            Unsafe.getUnsafe().putByte(appendAddress, value);
            appendAddress += 1;
        }

        @Override
        public void putChar(char value) {
            Unsafe.getUnsafe().putChar(appendAddress, value);
            appendAddress += Character.BYTES;
        }

        @Override
        public void putDate(long value) {
            putLong(value);
        }

        @Override
        public void putDouble(double value) {
            Unsafe.getUnsafe().putDouble(appendAddress, value);
            appendAddress += Double.BYTES;
        }

        @Override
        public void putFloat(float value) {
            Unsafe.getUnsafe().putFloat(appendAddress, value);
            appendAddress += Float.BYTES;
        }

        @Override
        public void putInt(int value) {
            Unsafe.getUnsafe().putInt(appendAddress, value);
            appendAddress += Integer.BYTES;
        }

        @Override
        public void putLong(long value) {
            Unsafe.getUnsafe().putLong(appendAddress, value);
            appendAddress += Long.BYTES;
        }

        @Override
        public void putLong128LittleEndian(long hi, long lo) {
            Unsafe.getUnsafe().putLong(appendAddress, lo);
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES, hi);
            appendAddress += 16;
        }

        @Override
        public void putLong256(Long256 value) {
            Unsafe.getUnsafe().putLong(appendAddress, value.getLong0());
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES, value.getLong1());
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES * 2, value.getLong2());
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES * 3, value.getLong3());
            appendAddress += Long256.BYTES;
        }

        @Override
        public void putRecord(Record value) {
            // no-op
        }

        @Override
        public void putShort(short value) {
            Unsafe.getUnsafe().putShort(appendAddress, value);
            appendAddress += 2;
        }

        @Override
        public void putTimestamp(long value) {
            putLong(value);
        }

        @Override
        public void skip(int bytes) {
            appendAddress += bytes;
        }

        private MapValue createValue(FastMapValue value) {
            commit();
            // calculate hash remembering "key" structure
            // [ len | value block | key offset block | key data block ]
            int hashCode = hash();
            int index = hashCode & mask;
            long offset = getOffset(index);

            if (offset < 0) {
                return asNew(this, index, hashCode, value);
            } else if (hashCode == getHashCode(index) && eq(offset)) {
                return valueOf(kStart + offset, false, value);
            } else {
                return probe0(this, index, hashCode, value);
            }
        }

        private MapValue findValue(FastMapValue value) {
            commit();
            int hashCode = hash();
            int index = hashCode & mask;
            long offset = getOffset(index);

            if (offset < 0) {
                return null;
            } else if (eq(offset)) {
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
        protected boolean eq(long offset) {
            long a = kStart + offset + keyOffset;
            long b = startAddress + keyOffset;

            int len = keySize;
            int i = 0;
            for (; i + 7 < len; i += 8) {
                if (Unsafe.getUnsafe().getLong(a + i) != Unsafe.getUnsafe().getLong(b + i)) {
                    return false;
                }
            }
            for (; i + 3 < len; i += 4) {
                if (Unsafe.getUnsafe().getInt(a + i) != Unsafe.getUnsafe().getInt(b + i)) {
                    return false;
                }
            }
            for (; i < len; i++) {
                if (Unsafe.getUnsafe().getByte(a + i) != Unsafe.getUnsafe().getByte(b + i)) {
                    return false;
                }
            }
            return true;
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
            super.putBool(value);
        }

        @Override
        public void putByte(byte value) {
            checkSize(1);
            super.putByte(value);
        }

        @Override
        public void putChar(char value) {
            checkSize(Character.BYTES);
            super.putChar(value);
        }

        @Override
        public void putDouble(double value) {
            checkSize(Double.BYTES);
            super.putDouble(value);
        }

        @Override
        public void putFloat(float value) {
            checkSize(Float.BYTES);
            super.putFloat(value);
        }

        @Override
        public void putInt(int value) {
            checkSize(Integer.BYTES);
            super.putInt(value);
        }

        @Override
        public void putLong(long value) {
            checkSize(Long.BYTES);
            super.putLong(value);
        }

        @Override
        public void putLong128LittleEndian(long hi, long lo) {
            checkSize(16);
            super.putLong128LittleEndian(hi, lo);
        }

        @Override
        public void putLong256(Long256 value) {
            checkSize(Long256.BYTES);
            super.putLong256(value);
        }

        @Override
        public void putShort(short value) {
            checkSize(2);
            super.putShort(value);
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
        public void skip(int bytes) {
            checkSize(bytes);
            super.skip(bytes);
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

            // skip to the data
            a += keyOffset;
            b += keyOffset;

            int len = this.len - keyOffset;
            int i = 0;
            for (; i + 7 < len; i += 8) {
                if (Unsafe.getUnsafe().getLong(a + i) != Unsafe.getUnsafe().getLong(b + i)) {
                    return false;
                }
            }
            for (; i + 3 < len; i += 4) {
                if (Unsafe.getUnsafe().getInt(a + i) != Unsafe.getUnsafe().getInt(b + i)) {
                    return false;
                }
            }
            for (; i < len; i++) {
                if (Unsafe.getUnsafe().getByte(a + i) != Unsafe.getUnsafe().getByte(b + i)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        protected int hash() {
            return Hash.hashMem32(startAddress + keyOffset, len - keyOffset);
        }
    }
}
