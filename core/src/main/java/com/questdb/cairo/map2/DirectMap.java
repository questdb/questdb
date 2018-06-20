/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.cairo.map2;

import com.questdb.cairo.CairoException;
import com.questdb.cairo.ColumnTypes;
import com.questdb.cairo.TableUtils;
import com.questdb.cairo.sql.Record;
import com.questdb.common.ColumnType;
import com.questdb.std.*;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

public class DirectMap implements Mutable, Iterable<DirectMapRecord>, Closeable {

    private static final HashFunction DEFAULT_HASH = Hash::hashMem;
    private static final int MIN_INITIAL_CAPACITY = 128;
    private final double loadFactor;
    private final Key keyWriter = new Key();
    private final DirectMapValues values;
    private final DirectMapIterator iterator;
    private final DirectMapRecord record;
    private final int valueColumnCount;
    private final HashFunction hashFunction;
    private long address;
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

    public DirectMap(int pageSize,
                     ColumnTypes keyTypes,
                     ColumnTypes valueTypes,
                     int keyCapacity,
                     double loadFactor
    ) {
        this(pageSize, keyTypes, valueTypes, keyCapacity, loadFactor, DEFAULT_HASH);
    }

    DirectMap(int pageSize,
              ColumnTypes keyTypes,
              ColumnTypes valueTypes,
              int keyCapacity,
              double loadFactor,
              HashFunction hashFunction

    ) {
        assert pageSize > 3;
        assert loadFactor > 0 && loadFactor < 1d;

        this.loadFactor = loadFactor;
        this.address = Unsafe.malloc(this.capacity = (pageSize + Unsafe.CACHE_LINE_SIZE));
        this.kStart = kPos = this.address + (this.address & (Unsafe.CACHE_LINE_SIZE - 1));
        this.kLimit = kStart + pageSize;

        this.keyCapacity = (int) (keyCapacity / loadFactor);
        this.keyCapacity = this.keyCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(this.keyCapacity);
        this.mask = this.keyCapacity - 1;
        this.free = (int) (this.keyCapacity * loadFactor);
        this.offsets = new DirectLongList(this.keyCapacity);
        this.offsets.setPos(this.keyCapacity);
        this.offsets.zero(-1);
        this.valueColumnCount = valueTypes.getColumnCount();
        final int columnSplit = valueColumnCount;
        int[] valueOffsets = new int[columnSplit];
        this.hashFunction = hashFunction;

        int offset = 4;
        for (int i = 0; i < columnSplit; i++) {
            valueOffsets[i] = offset;
            switch (valueTypes.getColumnType(i)) {
                case ColumnType.BYTE:
                case ColumnType.BOOLEAN:
                    offset++;
                    break;
                case ColumnType.SHORT:
                    offset += 2;
                    break;
                case ColumnType.INT:
                case ColumnType.FLOAT:
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

        this.values = new DirectMapValues(valueOffsets);
        this.keyBlockOffset = offset;
        this.keyDataOffset = this.keyBlockOffset + 4 * keyTypes.getColumnCount();
        this.record = new DirectMapRecord(valueOffsets, keyDataOffset, keyBlockOffset, values, keyTypes);
        this.iterator = new DirectMapIterator(record);
    }

    int getValueColumnCount() {
        return valueColumnCount;
    }

    public void clear() {
        kPos = kStart;
        free = (int) (keyCapacity * loadFactor);
        size = 0;
        offsets.zero(-1);
    }

    @Override
    public final void close() {
        offsets.close();
        if (address != 0) {
            Unsafe.free(address, capacity);
            address = 0;
        }
    }

    public DirectMapRecord recordAt(long rowid) {
        return record.of(rowid);
    }

    private int keyIndex() {
        return hashFunction.hash(keyWriter.startAddress + keyDataOffset, keyWriter.len - keyDataOffset) & mask;
    }

    public DirectMapValues getOrCreateValues() {
        keyWriter.commit();
        // calculate hash remembering "key" structure
        // [ len | value block | key offset block | key data block ]
        int index = keyIndex();
        long offset = offsets.get(index);

        if (offset == -1) {
            return asNew(keyWriter, index);
        } else if (eq(keyWriter, offset)) {
            return values.of(kStart + offset, false);
        } else {
            return probe0(keyWriter, index);
        }
    }

    public DirectMapValues getValues() {
        keyWriter.commit();
        int index = keyIndex();
        long offset = offsets.get(index);

        if (offset == -1) {
            return null;
        } else if (eq(keyWriter, offset)) {
            return values.of(kStart + offset, false);
        } else {
            return probeReadOnly(keyWriter, index);
        }
    }

    @Override
    @NotNull
    public DirectMapIterator iterator() {
        return iterator.init(kStart, size);
    }

    public Key withKey() {
        return keyWriter.init();
    }

    public void withKeyAsLong(long value) {
        keyWriter.startAddress = kPos;
        keyWriter.appendAddress = keyWriter.startAddress + keyDataOffset;
        if (keyWriter.appendAddress + 8 > kLimit) {
            resize(8);
        }
        Unsafe.getUnsafe().putLong(keyWriter.appendAddress, value);
        keyWriter.appendAddress += 8;
    }

    public int size() {
        return size;
    }

    private DirectMapValues asNew(Key keyWriter, int index) {
        kPos = keyWriter.appendAddress;
        offsets.set(index, keyWriter.startAddress - kStart);
        if (--free == 0) {
            rehash();
        }
        size++;
        return values.of(keyWriter.startAddress, true);
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

    private DirectMapValues probe0(Key keyWriter, int index) {
        long offset;
        while ((offset = offsets.get(index = (++index & mask))) != -1) {
            if (eq(keyWriter, offset)) {
                return values.of(kStart + offset, false);
            }
        }
        return asNew(keyWriter, index);
    }

    private DirectMapValues probeReadOnly(Key keyWriter, int index) {
        long offset;
        while ((offset = offsets.get(index = (++index & mask))) != -1) {
            if (eq(keyWriter, offset)) {
                return values.of(kStart + offset, false);
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
        long target = keyWriter.appendAddress + size - kStart;
        if (kCapacity < target) {
            kCapacity = Numbers.ceilPow2(target);
        }
        long kAddress = Unsafe.malloc(kCapacity + Unsafe.CACHE_LINE_SIZE);
        long kStart = kAddress + (kAddress & (Unsafe.CACHE_LINE_SIZE - 1));

        Unsafe.getUnsafe().copyMemory(this.kStart, kStart, (kLimit - this.kStart));
        Unsafe.free(this.address, this.capacity);


        this.capacity = kCapacity + Unsafe.CACHE_LINE_SIZE;
        long d = kStart - this.kStart;
        kPos += d;
        keyWriter.startAddress += d;
        keyWriter.appendAddress += d;
        keyWriter.nextColOffset = keyWriter.startAddress + keyBlockOffset;

        assert kPos > 0;
        assert keyWriter.startAddress > 0;
        assert keyWriter.appendAddress > 0;
        assert keyWriter.nextColOffset > 0;

        this.address = kAddress;
        this.kStart = kStart;
        this.kLimit = kStart + kCapacity;
    }

    long getAppendOffset() {
        return kPos;
    }

    @FunctionalInterface
    public interface HashFunction {
        int hash(long address, int len);
    }

    public class Key {
        private long startAddress;
        private long appendAddress;
        private int len;
        private long nextColOffset;

        private void commit() {
            Unsafe.getUnsafe().putInt(startAddress, len = (int) (appendAddress - startAddress));
        }

        public Key init() {
            startAddress = kPos;
            appendAddress = kPos + keyDataOffset;
            nextColOffset = kPos + keyBlockOffset;
            return this;
        }

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
                long offset = 4L;
                long pos = 0;

                Unsafe.getUnsafe().putInt(appendAddress, l);
                while (true) {
                    long copied = value.copyTo(appendAddress + offset, pos, l);
                    if (copied == l) {
                        break;
                    }
                    l -= copied;
                    pos += copied;
                    offset += copied;
                }
                appendAddress += len;
                writeOffset();
            }
        }

        public void putRecord(Record record, RecordSink sink) {
            sink.copy(record, this);
        }

        public void putBool(boolean value) {
            checkSize(1);
            Unsafe.getUnsafe().putByte(appendAddress, (byte) (value ? 1 : 0));
            appendAddress += 1;
            writeOffset();
        }

        public void putByte(byte value) {
            checkSize(1);
            Unsafe.getUnsafe().putByte(appendAddress, value);
            appendAddress += 1;
            writeOffset();
        }

        public void putDouble(double value) {
            checkSize(8);
            Unsafe.getUnsafe().putDouble(appendAddress, value);
            appendAddress += 8;
            writeOffset();
        }

        public void putFloat(float value) {
            checkSize(4);
            Unsafe.getUnsafe().putFloat(appendAddress, value);
            appendAddress += 4;
            writeOffset();
        }

        public void putInt(int value) {
            checkSize(4);
            Unsafe.getUnsafe().putInt(appendAddress, value);
            appendAddress += 4;
            writeOffset();
        }

        public void putLong(long value) {
            checkSize(8);
            Unsafe.getUnsafe().putLong(appendAddress, value);
            appendAddress += 8;
            writeOffset();
        }

        public void putShort(short value) {
            checkSize(2);
            Unsafe.getUnsafe().putShort(appendAddress, value);
            appendAddress += 2;
            writeOffset();
        }

        public void putDate(long value) {
            putLong(value);
        }

        @SuppressWarnings("unused")
        public void putTimestamp(long value) {
            putLong(value);
        }

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

        private void checkSize(int size) {
            if (appendAddress + size > kLimit) {
                resize(size);
            }
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