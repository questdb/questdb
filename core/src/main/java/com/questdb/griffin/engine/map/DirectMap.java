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

package com.questdb.griffin.engine.map;

import com.questdb.cairo.CairoException;
import com.questdb.cairo.TableUtils;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.common.ColumnType;
import com.questdb.std.*;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

public class DirectMap implements Mutable, Iterable<DirectMapEntry>, Closeable {

    private static final int MIN_INITIAL_CAPACITY = 128;
    private final float loadFactor;
    private final Key key = new Key();
    private final Values values;
    private final DirectMapIterator iterator;
    private final DirectMapEntry entry;
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

    public DirectMap(
            int keyInitialCapacity,
            int pageSize,
            float loadFactor,
            RecordMetadata keyMetadata,
            RecordMetadata valueMetadata) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("pageSize must be > 0");
        }
        this.loadFactor = loadFactor;
        this.address = Unsafe.malloc(this.capacity = (pageSize + Unsafe.CACHE_LINE_SIZE));
        this.kStart = kPos = this.address + (this.address & (Unsafe.CACHE_LINE_SIZE - 1));
        this.kLimit = kStart + pageSize;

        this.keyCapacity = (int) (keyInitialCapacity / loadFactor);
        this.keyCapacity = this.keyCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(this.keyCapacity);
        this.mask = keyCapacity - 1;
        this.free = (int) (keyCapacity * loadFactor);
        this.offsets = new DirectLongList(keyCapacity);
        this.offsets.setPos(keyCapacity);
        this.offsets.zero(-1);
        final int columnSplit = valueMetadata.getColumnCount();
        int[] valueOffsets = new int[columnSplit];

        int offset = 4;
        for (int i = 0; i < columnSplit; i++) {
            valueOffsets[i] = offset;
            switch (valueMetadata.getColumnType(i)) {
                case ColumnType.BYTE:
                case ColumnType.BOOLEAN:
                    offset++;
                    break;
                case ColumnType.SHORT:
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
                    offset += 8;
                    break;
                default:
                    throw CairoException.instance(0).put("Unsupported map value type: ").put(ColumnType.nameOf(valueMetadata.getColumnType(i)));
            }
        }

        this.values = new Values(valueOffsets);
        this.keyBlockOffset = offset;
        this.keyDataOffset = this.keyBlockOffset + 4 * keyMetadata.getColumnCount();
        this.entry = new DirectMapEntry(valueOffsets, keyDataOffset, keyBlockOffset, keyMetadata);
        this.iterator = new DirectMapIterator(entry);
    }

    public void clear() {
        kPos = kStart;
        free = (int) (keyCapacity * loadFactor);
        size = 0;
        offsets.zero(-1);
    }

    @Override
    public void close() {
        offsets.close();
        if (address != 0) {
            Unsafe.free(address, capacity);
            address = 0;
        }
    }

    public DirectMapEntry entryAt(long rowid) {
        return entry.init(rowid);
    }

    @Override
    @NotNull
    public DirectMapIterator iterator() {
        return iterator.init(kStart, size);
    }

    public Key key() {
        return key.init();
    }

    public long keyIndex(Key key) {
        key.commit();
        // calculate hash remembering "key" structure
        // [ len | value block | key offset block | key data block ]
        final long index = Hash.hashMem(key.startAddress + keyDataOffset, key.len - keyDataOffset) & mask;
        final long offset = offsets.get(index);

        if (offset == -1) {
            return index;
        } else if (eq(key, offset)) {
            // rollback added key
            key.cancel();
            return -offset - 1;
        } else {
            return probe(key, index);
        }
    }

    public int size() {
        return size;
    }

    public Values valuesAt(Key key, long keyIndex) {
        if (keyIndex < 0) {
            return values.of(kStart + (-keyIndex - 1));
        }
        return appendValues(key, keyIndex);
    }

    private Values appendValues(Key key, long keyIndex) {
        offsets.set(keyIndex, key.startAddress - kStart);
        if (--free == 0) {
            rehash();
        }
        size++;
        return values.of(key.startAddress);
    }

    private boolean eq(Key key, long offset) {
        long a = kStart + offset;
        long b = key.startAddress;

        // check length first
        if (Unsafe.getUnsafe().getInt(a) != Unsafe.getUnsafe().getInt(b)) {
            return false;
        }

        long lim = b + key.len;

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

    private long probe(Key key, long index) {
        long offset;
        long p = index;
        while ((offset = offsets.get(p = (++p & mask))) != -1) {
            if (eq(key, offset)) {
                // rollback added key
                kPos = key.startAddress;
                return -offset - 1;
            }
        }
        return p;
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
            int index = Hash.hashMem(kStart + offset + keyDataOffset, Unsafe.getUnsafe().getInt(kStart + offset) - keyDataOffset) & mask;
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

    private void resize() {
        long kCapacity = (kLimit - kStart) << 1;
        long kAddress = Unsafe.malloc(kCapacity + Unsafe.CACHE_LINE_SIZE);
        long kStart = kAddress + (kAddress & (Unsafe.CACHE_LINE_SIZE - 1));

        Unsafe.getUnsafe().copyMemory(this.kStart, kStart, kCapacity >> 1);
        Unsafe.free(this.address, this.capacity);

        long d = kStart - this.kStart;
        key.startAddress += d;
        key.appendAddress += d;
        key.nextColOffset += d;

        this.address = kAddress;
        this.kStart = kStart;
        this.kLimit = kStart + kCapacity;
    }

    public static final class Values {
        private final int valueOffsets[];
        private long address;

        public Values(int[] valueOffsets) {
            this.valueOffsets = valueOffsets;
        }

        public byte getByte(int index) {
            return Unsafe.getUnsafe().getByte(address0(index));
        }

        public double getDouble(int index) {
            return Unsafe.getUnsafe().getDouble(address0(index));
        }

        public float getFloat(int index) {
            return Unsafe.getUnsafe().getFloat(address0(index));
        }

        public int getInt(int index) {
            return Unsafe.getUnsafe().getInt(address0(index));
        }

        public long getLong(int index) {
            return Unsafe.getUnsafe().getLong(address0(index));
        }

        public short getShort(int index) {
            return Unsafe.getUnsafe().getShort(address0(index));
        }

        public void putByte(int index, byte value) {
            Unsafe.getUnsafe().putByte(address0(index), value);
        }

        public void putDouble(int index, double value) {
            Unsafe.getUnsafe().putDouble(address0(index), value);
        }

        public void putFloat(int index, float value) {
            Unsafe.getUnsafe().putFloat(address0(index), value);
        }

        public void putInt(int index, int value) {
            Unsafe.getUnsafe().putInt(address0(index), value);
        }

        public void putLong(int index, long value) {
            Unsafe.getUnsafe().putLong(address0(index), value);
        }

        public void putShort(int index, short value) {
            Unsafe.getUnsafe().putShort(address0(index), value);
        }

        private long address0(int index) {
            return address + Unsafe.arrayGet(valueOffsets, index);
        }

        Values of(long address) {
            this.address = address;
            return this;
        }
    }

    public class Key {
        private long startAddress;
        private long appendAddress;

        private int len;
        private long nextColOffset;

        public void cancel() {
            kPos = startAddress;
        }

        public Key init() {
            startAddress = kPos;
            appendAddress = startAddress + keyDataOffset;
            nextColOffset = startAddress + keyBlockOffset;
            return this;
        }

        public void put(long address, int len) {
            checkSize(len);
            Unsafe.getUnsafe().copyMemory(address, appendAddress, len);
            appendAddress += len;
            writeOffset();
        }

        public void putBin(DirectInputStream stream) {
            long length = stream.size();
            checkSize((int) length);
            length = stream.copyTo(appendAddress, 0, length);
            appendAddress += length;
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
                resize();
            }
        }

        private void commit() {
            Unsafe.getUnsafe().putInt(startAddress, len = (int) (appendAddress - startAddress));
            kPos = appendAddress;
        }

        private void putNull() {
            checkSize(4);
            Unsafe.getUnsafe().putInt(appendAddress, TableUtils.NULL_LEN);
            appendAddress += 4;
            writeOffset();
        }

        private void writeOffset() {
            Unsafe.getUnsafe().putInt(nextColOffset, (int) (appendAddress - startAddress));
            nextColOffset += 4;
        }
    }
}