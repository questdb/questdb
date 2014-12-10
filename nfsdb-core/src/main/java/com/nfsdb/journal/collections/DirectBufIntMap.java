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

import com.nfsdb.journal.utils.DirectMemoryBuffer;
import com.nfsdb.journal.utils.Hash;
import com.nfsdb.journal.utils.MemoryBuffer;
import com.nfsdb.journal.utils.Unsafe;

import java.io.Closeable;

public class DirectBufIntMap implements Closeable {

    private final int seed = 0xdeadbeef;
    private final DirectMemoryBuffer memBuf = new DirectMemoryBuffer();
    private final double loadFactor;
    private final Key keyBuilder = new Key();
    private final Entry entry = new Entry();
    private final EntryIterator iterator = new EntryIterator();
    private DirectIntList values;
    private DirectLongList keyOffsets;
    private long kAddress;
    private long kStart;
    private long kLimit;
    private long kPos;
    private int free;
    private long keyCapacity;

    public DirectBufIntMap() {
        this(67, 4 * 1024, 0.5d);
    }

    public DirectBufIntMap(long capacity, long keyAreaCapacity, double loadFactor) {
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
    }

    public void put(MemoryBuffer b, int v) {
        long index = Hash.hashXX(b, seed) % keyCapacity;
        long address = keyOffsets.get(index);

        if (address == -1) {
            keyOffsets.set(index, add(b));
            values.set(index, v);
            if (--free == 0) {
                rehash();
            }
        } else if (eq(b, address)) {
            values.set(index, v);
        } else {
            probe(b, index, v);
        }
    }

    private void probe(MemoryBuffer b, long index, int v) {
        long offset;
        while ((offset = keyOffsets.get(index = (++index % keyCapacity))) != -1) {
            if (eq(b, offset)) {
                values.set(index, v);
                return;
            }
        }
        keyOffsets.set(index, add(b));
        values.set(index, v);
        free--;
        if (free == 0) {
            rehash();
        }
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

    public Iterable<Entry> iterator() {
        iterator.index = 0;
        iterator.len = keyCapacity;
        return iterator;
    }
    public int get(MemoryBuffer b) {
        int h = Hash.hashXX(b, seed);
        long p = h % keyCapacity;
        long address = keyOffsets.get(p);

        if (address == -1) {
            return -1;
        }

        if (eq(b, address)) {
            return values.get(p);
        }

        long pp = p;
        do {
            address = keyOffsets.get(++p % keyCapacity);
            if (address == -1) {
                return -1;
            }

            if (eq(b, address)) {
                return values.get(p);
            }
        } while (p != pp);

        return -1;
    }

    private boolean eq(MemoryBuffer b, long offset) {
        int len = b.length();
        if (len != Unsafe.getUnsafe().getInt(kStart + offset)) {
            return false;
        }

        int p = 0;
        long a = kStart + offset + 4;
        while (p < len - 4) {
            if (b.getInt(p) != Unsafe.getUnsafe().getInt(a + p)) {
                return false;
            }
            p += 4;
        }

        while (p < len) {
            if (b.getByte(p) != Unsafe.getUnsafe().getByte(a + p)) {
                return false;
            }
            p++;
        }
        return true;
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

    private long add(MemoryBuffer b) {
        long address = kPos - kStart;

        int len = b.length();
        if (kPos + len + 4 > kLimit) {
            resize();
        }

        Unsafe.getUnsafe().putInt(kPos, len);
        kPos += 4;

        int p = 0;
        while (p < len - 4) {
            Unsafe.getUnsafe().putInt(kPos + p, b.getInt(p));
            p += 4;
        }

        while (p < len) {
            Unsafe.getUnsafe().putByte(kPos + p, b.getByte(p));
            p++;
        }

        kPos += len;

        return address;
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
            long index = Hash.hashXX(memBuf.init(offset + 4 + kStart, Unsafe.getUnsafe().getInt(kStart + offset)), seed) % capacity;
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
        private long len;

        @Override
        public boolean hasNext() {
            if (index >= len) {
                return false;
            }

            entry.key.offset = keyOffsets.get(index);
            if (entry.key.offset != -1) {
                entry.value = values.get(index++);
                return true;
            }

            return scan(index++);

        }

        private boolean scan(long index) {
            while (index < len && (entry.key.offset = keyOffsets.get(index)) == -1) {
                index++;
            }

            if (entry.key.offset == -1) {
                return false;
            }
            entry.value = values.get(index);
            this.index = index + 1;
            return true;

        }

        @Override
        public Entry next() {
            return entry;
        }
    }

    public class Key {
        private long offset;
        private int len;

        private void checkSize(int size) {
            if (kPos + size > kLimit) {
                resize();
            }
        }

        public Key putLong(long value) {
            checkSize(8);
            Unsafe.getUnsafe().putLong(kPos, value);
            kPos += 8;
            return this;
        }

        public void put(long address, int len) {
            checkSize(len + 4);
            Unsafe.getUnsafe().putInt(kPos, len);
            Unsafe.getUnsafe().copyMemory(address, kPos += 4, len);
            kPos += len;
        }

        public Key put(CharSequence value) {
            int len = value.length();
            checkSize(len << 1 + 4);
            Unsafe.getUnsafe().putInt(kPos, value.length());
            kPos += 4;
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putChar(kPos + (i << 1), value.charAt(i));
            }
            kPos += len << 1;
            return this;
        }

        public Key $() {
            Unsafe.getUnsafe().putInt(kStart + offset, len = (int) (kPos - kStart - offset));
            return this;
        }

        public Key begin() {
            keyBuilder.offset = kPos - kStart;
            kPos += 4;
            return this;
        }
    }
}
