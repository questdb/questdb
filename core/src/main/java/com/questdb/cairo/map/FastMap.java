/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo.map;

import com.questdb.cairo.*;
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.std.*;
import org.jetbrains.annotations.NotNull;

public class FastMap implements Map {

    private static final HashFunction DEFAULT_HASH = Hash::hashMem;
    private static final int MIN_INITIAL_CAPACITY = 128;
    private final double loadFactor;
    private final Key key = new Key();
    private final FastMapValue value;
    private final FastMapCursor cursor;
    private final FastMapRecord record;
    private final int valueColumnCount;
    private final HashFunction hashFunction;
    private long capacity;
    private int keyBlockOffset;
    private int keyDataOffset;
    private DirectLongList offsets;
    private long kStart;
    private long kLimit;
    private long kPos;
    private int free;
    private int keyCapacity;
    private int size = 0;
    private int mask;

    public FastMap(int pageSize,
                   @Transient @NotNull ColumnTypes keyTypes,
                   int keyCapacity,
                   double loadFactor
    ) {
        this(pageSize, keyTypes, null, keyCapacity, loadFactor, DEFAULT_HASH);
    }

    public FastMap(int pageSize,
                   @Transient @NotNull ColumnTypes keyTypes,
                   @Transient @NotNull ColumnTypes valueTypes,
                   int keyCapacity,
                   double loadFactor
    ) {
        this(pageSize, keyTypes, valueTypes, keyCapacity, loadFactor, DEFAULT_HASH);
    }

    FastMap(int pageSize,
            @Transient ColumnTypes keyTypes,
            @Transient ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            HashFunction hashFunction

    ) {
        assert pageSize > 3;
        assert loadFactor > 0 && loadFactor < 1d;

        this.loadFactor = loadFactor;
        this.kStart = kPos = Unsafe.malloc(this.capacity = pageSize);
        this.kLimit = kStart + pageSize;

        this.keyCapacity = (int) (keyCapacity / loadFactor);
        this.keyCapacity = this.keyCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(this.keyCapacity);
        this.mask = this.keyCapacity - 1;
        this.free = (int) (this.keyCapacity * loadFactor);
        this.offsets = new DirectLongList(this.keyCapacity);
        this.offsets.setPos(this.keyCapacity);
        this.offsets.zero(-1);
        this.hashFunction = hashFunction;

        int[] valueOffsets;
        int offset = 4;
        if (valueTypes != null) {
            this.valueColumnCount = valueTypes.getColumnCount();
            final int columnSplit = valueColumnCount;
            valueOffsets = new int[columnSplit];

            for (int i = 0; i < columnSplit; i++) {
                valueOffsets[i] = offset;
                switch (valueTypes.getColumnType(i)) {
                    case ColumnType.BYTE:
                    case ColumnType.BOOLEAN:
                        offset++;
                        break;
                    case ColumnType.SHORT:
                    case ColumnType.CHAR:
                        offset += 2;
                        break;
                    case ColumnType.INT:
                    case ColumnType.FLOAT:
                    case ColumnType.SYMBOL:
                        offset += 4;
                        break;
                    case ColumnType.LONG:
                    case ColumnType.DOUBLE:
                    case ColumnType.DATE:
                    case ColumnType.TIMESTAMP:
                        offset += 8;
                        break;
                    default:
                        close();
                        throw CairoException.instance(0).put("value type is not supported: ").put(ColumnType.nameOf(valueTypes.getColumnType(i)));
                }
            }
            this.value = new FastMapValue(valueOffsets);
            this.keyBlockOffset = offset;
            this.keyDataOffset = this.keyBlockOffset + 4 * keyTypes.getColumnCount();
            this.record = new FastMapRecord(valueOffsets, columnSplit, keyDataOffset, keyBlockOffset, value, keyTypes);
        } else {
            this.valueColumnCount = 0;
            this.value = new FastMapValue(null);
            this.keyBlockOffset = offset;
            this.keyDataOffset = this.keyBlockOffset + 4 * keyTypes.getColumnCount();
            this.record = new FastMapRecord(null, 0, keyDataOffset, keyBlockOffset, value, keyTypes);
        }
        assert this.keyBlockOffset < kLimit - kStart : "page size is too small for number of columns";
        this.cursor = new FastMapCursor(record);
    }

    @Override
    public void clear() {
        kPos = kStart;
        free = (int) (keyCapacity * loadFactor);
        size = 0;
        offsets.zero(-1);
    }

    @Override
    public final void close() {
        offsets = Misc.free(offsets);
        if (kStart != 0) {
            Unsafe.free(kStart, capacity);
            kStart = 0;
        }
    }

    @Override
    public RecordCursor getCursor() {
        return cursor.init(kStart, size);
    }

    @Override
    public MapRecord getRecord() {
        return record;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public MapValue valueAt(long address) {
        value.of(address, false);
        return value;
    }

    @Override
    public MapKey withKey() {
        return key.init();
    }

    @Override
    public MapKey withKeyAsLong(long value) {
        key.startAddress = kPos;
        key.appendAddress = kPos + keyDataOffset;
        // we need nextColOffset in case we need to resize
        if (key.appendAddress + 8 > kLimit) {
            key.nextColOffset = kPos + keyBlockOffset;
            resize(8);
        }
        Unsafe.getUnsafe().putLong(key.appendAddress, value);
        key.appendAddress += 8;
        return key;
    }

    private FastMapValue asNew(Key keyWriter, int index) {
        kPos = keyWriter.appendAddress;
        offsets.set(index, keyWriter.startAddress - kStart);
        if (--free == 0) {
            rehash();
        }
        size++;
        return value.of(keyWriter.startAddress, true);
    }

    private boolean eq(Key keyWriter, long offset) {
        long a = kStart + offset;
        long b = keyWriter.startAddress;

        // check length first
        if (Unsafe.getUnsafe().getInt(a) != Unsafe.getUnsafe().getInt(b)) {
            return false;
        }

        long lim = b + keyWriter.len;

        // skip to the data
        a += keyDataOffset;
        b += keyDataOffset;

        while (b < lim - 8) {
            if (Unsafe.getUnsafe().getLong(a) != Unsafe.getUnsafe().getLong(b)) {
                return false;
            }
            a += 8;
            b += 8;
        }

        while (b < lim) {
            if (Unsafe.getUnsafe().getByte(a++) != Unsafe.getUnsafe().getByte(b++)) {
                return false;
            }
        }
        return true;
    }

    long getAppendOffset() {
        return kPos;
    }

    int getValueColumnCount() {
        return valueColumnCount;
    }

    private int keyIndex() {
        return hashFunction.hash(key.startAddress + keyDataOffset, key.len - keyDataOffset) & mask;
    }

    private FastMapValue probe0(Key keyWriter, int index) {
        long offset;
        while ((offset = offsets.get(index = (++index & mask))) != -1) {
            if (eq(keyWriter, offset)) {
                return value.of(kStart + offset, false);
            }
        }
        return asNew(keyWriter, index);
    }

    private FastMapValue probeReadOnly(Key keyWriter, int index) {
        long offset;
        while ((offset = offsets.get(index = (++index & mask))) != -1) {
            if (eq(keyWriter, offset)) {
                return value.of(kStart + offset, false);
            }
        }
        return null;
    }

    private void rehash() {
        int capacity = keyCapacity << 1;
        mask = capacity - 1;
        DirectLongList pointers = new DirectLongList(capacity);
        pointers.setPos(capacity);
        pointers.zero(-1);

        for (int i = 0, k = this.offsets.size(); i < k; i++) {
            long offset = this.offsets.get(i);
            if (offset == -1) {
                continue;
            }
            int index = hashFunction.hash(kStart + offset + keyDataOffset, Unsafe.getUnsafe().getInt(kStart + offset) - keyDataOffset) & mask;
            while (pointers.get(index) != -1) {
                index = (index + 1) & mask;
            }
            pointers.set(index, offset);
        }
        this.offsets.close();
        this.offsets = pointers;
        this.free += (capacity - keyCapacity) * loadFactor;
        this.keyCapacity = capacity;
    }

    private void resize(int size) {
        long kCapacity = (kLimit - kStart) << 1;
        long target = key.appendAddress + size - kStart;
        if (kCapacity < target) {
            kCapacity = Numbers.ceilPow2(target);
        }
        long kAddress = Unsafe.realloc(this.kStart, this.capacity, kCapacity);

        this.capacity = kCapacity;
        long d = kAddress - this.kStart;
        kPos += d;
        long colOffsetDelta = key.nextColOffset - key.startAddress;
        key.startAddress += d;
        key.appendAddress += d;
        key.nextColOffset = key.startAddress + colOffsetDelta;

        assert kPos > 0;
        assert key.startAddress > 0;
        assert key.appendAddress > 0;
        assert key.nextColOffset > 0;

        this.kStart = kAddress;
        this.kLimit = kAddress + kCapacity;
    }

    @FunctionalInterface
    public interface HashFunction {
        int hash(long address, int len);
    }

    public class Key implements MapKey {
        private long startAddress;
        private long appendAddress;
        private int len;
        private long nextColOffset;

        public MapValue createValue() {
            commit();
            // calculate hash remembering "key" structure
            // [ len | value block | key offset block | key data block ]
            int index = keyIndex();
            long offset = offsets.get(index);

            if (offset == -1) {
                return asNew(this, index);
            } else if (eq(this, offset)) {
                return value.of(kStart + offset, false);
            } else {
                return probe0(this, index);
            }
        }

        public MapValue findValue() {
            commit();
            int index = keyIndex();
            long offset = offsets.get(index);

            if (offset == -1) {
                return null;
            } else if (eq(this, offset)) {
                return value.of(kStart + offset, false);
            } else {
                return probeReadOnly(this, index);
            }
        }

        @Override
        public void put(Record record, RecordSink sink) {
            sink.copy(record, this);
        }

        public Key init() {
            startAddress = kPos;
            appendAddress = kPos + keyDataOffset;
            nextColOffset = kPos + keyBlockOffset;
            return this;
        }

        @Override
        public void putBin(BinarySequence value) {
            if (value == null) {
                putNull();
            } else {
                long len = value.length() + 4;
                if (len > Integer.MAX_VALUE) {
                    throw CairoException.instance(0).put("binary column is too large");
                }

                checkSize((int) len);
                int l = (int) (len - 4);
                Unsafe.getUnsafe().putInt(appendAddress, l);
                value.copyTo(appendAddress + 4L, 0L, l);
                appendAddress += len;
                writeOffset();
            }
        }

        @Override
        public void putBool(boolean value) {
            checkSize(1);
            Unsafe.getUnsafe().putByte(appendAddress, (byte) (value ? 1 : 0));
            appendAddress += 1;
            writeOffset();
        }

        @Override
        public void putByte(byte value) {
            checkSize(1);
            Unsafe.getUnsafe().putByte(appendAddress, value);
            appendAddress += 1;
            writeOffset();
        }

        @Override
        public void putDate(long value) {
            putLong(value);
        }

        @Override
        public void putDouble(double value) {
            checkSize(8);
            Unsafe.getUnsafe().putDouble(appendAddress, value);
            appendAddress += 8;
            writeOffset();
        }

        @Override
        public void putFloat(float value) {
            checkSize(4);
            Unsafe.getUnsafe().putFloat(appendAddress, value);
            appendAddress += 4;
            writeOffset();
        }

        @Override
        public void putInt(int value) {
            checkSize(4);
            Unsafe.getUnsafe().putInt(appendAddress, value);
            appendAddress += 4;
            writeOffset();
        }

        @Override
        public void putLong(long value) {
            checkSize(8);
            Unsafe.getUnsafe().putLong(appendAddress, value);
            appendAddress += 8;
            writeOffset();
        }

        @Override
        public void putShort(short value) {
            checkSize(2);
            Unsafe.getUnsafe().putShort(appendAddress, value);
            appendAddress += 2;
            writeOffset();
        }

        @Override
        public void putChar(char value) {
            checkSize(Character.BYTES);
            Unsafe.getUnsafe().putChar(appendAddress, value);
            appendAddress += Character.BYTES;
            writeOffset();
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
                Unsafe.getUnsafe().putChar(appendAddress + (i << 1), value.charAt(i));
            }
            appendAddress += len << 1;
            writeOffset();
        }

        @Override
        public void putStr(CharSequence value, int lo, int hi) {
            int len = hi - lo;
            checkSize((len << 1) + 4);
            Unsafe.getUnsafe().putInt(appendAddress, len);
            appendAddress += 4;
            for (int i = lo; i < hi; i++) {
                Unsafe.getUnsafe().putChar(appendAddress + ((i - lo) << 1), value.charAt(i));
            }
            appendAddress += len << 1;
            writeOffset();
        }

        @Override
        @SuppressWarnings("unused")
        public void putTimestamp(long value) {
            putLong(value);
        }

        private void checkSize(int size) {
            if (appendAddress + size > kLimit) {
                resize(size);
            }
        }

        private void commit() {
            Unsafe.getUnsafe().putInt(startAddress, len = (int) (appendAddress - startAddress));
        }

        private void putNull() {
            checkSize(4);
            Unsafe.getUnsafe().putInt(appendAddress, TableUtils.NULL_LEN);
            appendAddress += 4;
            writeOffset();
        }

        private void writeOffset() {
            long len = appendAddress - startAddress;
            if (len > Integer.MAX_VALUE) {
                throw CairoException.instance(0).put("row data is too large");
            }
            Unsafe.getUnsafe().putInt(nextColOffset, (int) len);
            nextColOffset += 4;
        }
    }
}