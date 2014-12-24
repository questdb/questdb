/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.collections;

import com.nfsdb.journal.column.ColumnType;
import com.nfsdb.journal.utils.Hash;
import com.nfsdb.journal.utils.Unsafe;

import java.io.Closeable;
import java.util.Iterator;

public class DirectCompositeKeyIntMap implements Closeable, Iterable<DirectCompositeKeyIntMap.Entry> {

    private final int seed = 0xdeadbeef;
    private final double loadFactor;
    private final Key keyBuilder = new Key();
    private final Entry entry = new Entry();
    private final EntryIterator iterator = new EntryIterator();
    private final ColumnType[] keyColumnTypes;
    private DirectIntList values;
    private DirectLongList keyOffsets;
    private long kAddress;
    private long kStart;
    private long kLimit;
    private long kPos;
    private int free;
    private long keyCapacity;

    public DirectCompositeKeyIntMap(ColumnType[] keyColumnTypes) {
        this(67, 4 * 1024, 0.5d, keyColumnTypes);
    }

    public DirectCompositeKeyIntMap(long capacity, long keyAreaCapacity, double loadFactor, ColumnType[] keyColumnTypes) {
        this.loadFactor = loadFactor;
        this.kAddress = Unsafe.getUnsafe().allocateMemory(keyAreaCapacity + AbstractDirectList.CACHE_LINE_SIZE);
        this.kStart = kPos = this.kAddress + (this.kAddress & (AbstractDirectList.CACHE_LINE_SIZE - 1));
        this.kLimit = kStart + keyAreaCapacity;

        this.keyCapacity = Primes.next((long) (capacity / loadFactor));
        this.free = (int) (keyCapacity * loadFactor);
        this.keyOffsets = new DirectLongList(keyCapacity);
        this.keyOffsets.zero((byte) -1);
        this.keyOffsets.setPos(keyCapacity);
        this.values = new DirectIntList(keyCapacity);
        this.keyColumnTypes = keyColumnTypes;
    }

    public void put(Key key, int v) {
        long index = Hash.hashXX(kStart + key.offset, key.len, seed) % keyCapacity;
        long offset = keyOffsets.get(index);

        if (offset == -1) {
            keyOffsets.set(index, key.offset);
            values.set(index, v);
            if (--free == 0) {
                rehash();
            }
        } else if (eq(key, offset)) {
            values.set(index, v);
            // rollback added key
            kPos = kStart + key.offset;
        } else {
            probe(key, index, v);
        }
    }

    private void probe(Key key, long index, int v) {
        long offset;
        while ((offset = keyOffsets.get(index = (++index % keyCapacity))) != -1) {
            if (eq(key, offset)) {
                values.set(index, v);
                kPos = kStart + key.offset;
                return;
            }
        }
        keyOffsets.set(index, key.offset);
        values.set(index, v);
        free--;
        if (free == 0) {
            rehash();
        }
    }

    public Iterator<Entry> iterator() {
        iterator.index = 0;
        return iterator;
    }

    private boolean eq(Key key, long offset) {
        long a = kStart + offset;
        long b = kStart + key.offset;

        if (Unsafe.getUnsafe().getInt(a) != Unsafe.getUnsafe().getInt(b)) {
            return false;
        }


        long lim = b + key.len;

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

    public Key withKey() {
        return keyBuilder.begin();
    }

    private void resize() {
        long kCapacity = (kLimit - kStart) << 1;
        long kAddress = Unsafe.getUnsafe().allocateMemory(kCapacity + AbstractDirectList.CACHE_LINE_SIZE);
        long kStart = kAddress + (kAddress & (AbstractDirectList.CACHE_LINE_SIZE - 1));

        Unsafe.getUnsafe().copyMemory(this.kStart, kStart, kCapacity >> 1);
        Unsafe.getUnsafe().freeMemory(this.kAddress);

        this.kPos = kStart + (this.kPos - this.kStart);
        this.kAddress = kAddress;
        this.kStart = kStart;
        this.kLimit = kStart + kCapacity;
    }

    private void rehash() {
        long capacity = Primes.next(keyCapacity << 1);
        DirectLongList pointers = new DirectLongList(capacity);
        pointers.zero((byte) -1);
        DirectIntList values = new DirectIntList(capacity);
        pointers.setPos(capacity);
        values.setPos(capacity);

        for (int i = 0, sz = this.keyOffsets.size(); i < sz; i++) {
            long offset = this.keyOffsets.get(i);
            if (offset == -1) {
                continue;
            }
            long index = Hash.hashXX(offset + 4 + kStart, Unsafe.getUnsafe().getInt(kStart + offset), seed) % capacity;
            while (pointers.get(index) != -1) {
                index = (index + 1) % capacity;
            }
            pointers.set(index, offset);
            values.set(index, this.values.get(i));
        }
        this.keyOffsets.free();
        this.values.free();
        this.keyOffsets = pointers;
        this.values = values;
        this.free += (capacity - keyCapacity) * loadFactor;
        this.keyCapacity = capacity;
    }

    public void free() {
        if (kAddress != 0) {
            Unsafe.getUnsafe().freeMemory(kAddress);
            kAddress = 0;
        }
        values.free();
        keyOffsets.free();
    }

    @Override
    public void close() {
        free();
    }

    public class Entry {
        public final Key key = keyBuilder;
        public int value;
    }

    public class EntryIterator extends AbstractImmutableIterator<Entry> {

        private long index;

        @Override
        public boolean hasNext() {
            if (index >= keyCapacity) {
                return false;
            }

            while (index < keyCapacity && (entry.key.offset = keyOffsets.get(index)) == -1) {
                index++;
            }

            if (entry.key.offset != -1) {
                entry.value = values.get(index++);
                entry.key.rPos = kStart + entry.key.offset + 4;
                return true;
            }

            return false;
        }

        @Override
        public Entry next() {
            return entry;
        }
    }

    /**
     * Column count is fixed. Key structure:
     * <p>
     * len[4] | column2 offset [4] | column 3 offset [4] ... | data1 | data2 ...
     * </p>
     * <p/>
     * To offset of column 0 data skip all of the header information:
     * <p/>
     * offset = 4 + columnCount * 4
     * <p/>
     * To get offset of column 1 and onwards:
     * <p/>
     * offset = 4 + (columnIndex - 1) * 4
     * <p/>
     * To get length of column 0:
     * <p/>
     * len = column1Offset - 4 - 4 * columnCount
     * <p/>
     * To get length of column 1 and onwards:
     * <p/>
     * len = column2Offset - column1Offset
     */
    public class Key {
        private long offset;
        private int len;
        private long rPos;
        private char[] strBuf = null;
        private long nextColOffset;

        private void checkSize(int size) {
            if (kPos + size > kLimit) {
                resize();
            }
        }

        public long getLong(int index) {
            return Unsafe.getUnsafe().getLong(getColumnAddress(index));
        }

        public Key putLong(long value) {
            checkSize(8);
            Unsafe.getUnsafe().putLong(kPos, value);
            kPos += 8;
            writeOffset();
            return this;
        }

        public void put(long address, int len) {
            checkSize(len + 4);
            Unsafe.getUnsafe().putInt(kPos, len);
            Unsafe.getUnsafe().copyMemory(address, kPos += 4, len);
            kPos += len;
            writeOffset();
        }

        private void writeOffset() {
            Unsafe.getUnsafe().putInt(nextColOffset, (int) (kPos - offset));
            nextColOffset += 4;
        }

        public Key putStr(CharSequence value) {
            int len = value.length();
            checkSize(len << 1);
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putChar(kPos + (i << 1), value.charAt(i));
            }
            kPos += len << 1;
            writeOffset();

            return this;
        }

        private long getColumnAddress(int index) {
            if (index == 0) {
                return rPos + keyColumnTypes.length * 4;
            } else {
                return Unsafe.getUnsafe().getInt(rPos + (index - 1) * 4) + offset;
            }
        }

        public String getStr(int index) {
            long address = getColumnAddress(index);
            int len = (int) (getColumnAddress(index + 1) - address) >> 1;
            if (strBuf == null || strBuf.length < len) {
                strBuf = new char[len];
            }
            Unsafe.getUnsafe().copyMemory(null, address, strBuf, sun.misc.Unsafe.ARRAY_CHAR_BASE_OFFSET, ((long) len) << 1);
            return new String(strBuf);
        }

        public Key $() {
            Unsafe.getUnsafe().putInt(kStart + offset, len = (int) (kPos - kStart - offset));
            return this;
        }

        public Key begin() {
            keyBuilder.offset = kPos - kStart;
            nextColOffset = kPos + 4;
            kPos += 4 + 4 * keyColumnTypes.length;
            return this;
        }
    }
}
