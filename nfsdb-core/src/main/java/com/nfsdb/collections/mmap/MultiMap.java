/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.collections.mmap;

import com.nfsdb.collections.AbstractDirectList;
import com.nfsdb.collections.DirectLongList;
import com.nfsdb.collections.DirectMemoryStructure;
import com.nfsdb.collections.Primes;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.lang.cst.RecordMetadata;
import com.nfsdb.utils.Hash;
import com.nfsdb.utils.Unsafe;

import java.util.ArrayList;
import java.util.List;

public class MultiMap extends DirectMemoryStructure {

    private final float loadFactor;
    private final Key key = new Key();
    private final MapRecordSource recordSource;
    private final MapValues values;
    private int keyBlockOffset;
    private int keyDataOffset;
    private DirectLongList offsets;
    private long kStart;
    private long kLimit;
    private long kPos;
    private int free;
    private int keyCapacity;
    private int size = 0;

    private MultiMap(int capacity, long dataSize, float loadFactor, List<ColumnMetadata> valueColumns, List<ColumnMetadata> keyColumns, List<MapRecordValueInterceptor> interceptors) {
        this.loadFactor = loadFactor;
        this.address = Unsafe.getUnsafe().allocateMemory(dataSize + AbstractDirectList.CACHE_LINE_SIZE);
        this.kStart = kPos = this.address + (this.address & (AbstractDirectList.CACHE_LINE_SIZE - 1));
        this.kLimit = kStart + dataSize;

        this.keyCapacity = Primes.next((int) (capacity / loadFactor));
        this.free = (int) (keyCapacity * loadFactor);
        this.offsets = new DirectLongList(keyCapacity);
        this.offsets.zero((byte) -1);
        this.offsets.setPos(keyCapacity);
        int columnSplit = valueColumns.size();
        int[] valueOffsets = new int[columnSplit];

        int offset = 4;
        for (int i = 0; i < valueOffsets.length; i++) {
            valueOffsets[i] = offset;
            switch (valueColumns.get(i).type) {
                case INT:
                case FLOAT:
                    offset += 4;
                    break;
                case LONG:
                case DOUBLE:
                case DATE:
                    offset += 8;
                    break;
                default:
                    throw new JournalRuntimeException("value type is not supported: " + valueColumns.get(i));
            }
        }

        this.values = new MapValues(valueOffsets);
        MapMetadata metadata = new MapMetadata(valueColumns, keyColumns);
        this.keyBlockOffset = offset;
        this.keyDataOffset = this.keyBlockOffset + 4 * keyColumns.size();
        MapRecord record = new MapRecord(metadata, valueOffsets, keyDataOffset, keyBlockOffset);
        this.recordSource = new MapRecordSource(record, metadata, this.values, interceptors);
    }

    public Key claimKey() {
        return key.init();
    }

    public MapValues claimSlot(Key key) {
        // calculate hash remembering "key" structure
        // [ len | value block | key offset block | key data block ]
        int index = Hash.hashMem(key.startAddr + keyBlockOffset, key.len - keyBlockOffset) % keyCapacity;
        long offset = offsets.get(index);

        if (offset == -1) {
            offsets.set(index, key.startAddr - kStart);
            if (--free == 0) {
                rehash();
            }
            size++;
            return values.init(key.startAddr, true);
        } else if (eq(key, offset)) {
            // rollback added key
            kPos = key.startAddr;
            return values.init(kStart + offset, false);
        } else {
            return probe0(key, index);
        }
    }

    public void clear() {
        kPos = kStart;
        free = (int) (keyCapacity * loadFactor);
        size = 0;
        offsets.clear((byte) -1);
    }

    public RecordMetadata getMetadata() {
        return recordSource.getMetadata();
    }

    public MapRecordSource getRecordSource() {
        return recordSource.init(kStart, size);
    }

    public int size() {
        return size;
    }

    @Override
    protected void freeInternal() {
        offsets.free();
    }

    private boolean eq(Key key, long offset) {
        long a = kStart + offset;
        long b = key.startAddr;

        // check length first
        if (Unsafe.getUnsafe().getInt(a) != Unsafe.getUnsafe().getInt(b)) {
            return false;
        }

        long lim = b + key.len;

        // skip to the data
        a += keyBlockOffset;
        b += keyBlockOffset;

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

    private MapValues probe0(Key key, int index) {
        long offset;
        while ((offset = offsets.get(index = (++index % keyCapacity))) != -1) {
            if (eq(key, offset)) {
                kPos = key.startAddr;
                return values.init(kStart + offset, false);
            }
        }
        offsets.set(index, key.startAddr - kStart);
        free--;
        if (free == 0) {
            rehash();
        }

        size++;
        return values.init(key.startAddr, true);
    }

    private void rehash() {
        int capacity = Primes.next(keyCapacity << 1);
        DirectLongList pointers = new DirectLongList(capacity);
        pointers.zero((byte) -1);
        pointers.setPos(capacity);

        for (int i = 0, sz = this.offsets.size(); i < sz; i++) {
            long offset = this.offsets.get(i);
            if (offset == -1) {
                continue;
            }
            long index = Hash.hashMem(kStart + offset + keyBlockOffset, Unsafe.getUnsafe().getInt(kStart + offset) - keyBlockOffset) % capacity;
            while (pointers.get(index) != -1) {
                index = (index + 1) % capacity;
            }
            pointers.set(index, offset);
        }
        this.offsets.free();
        this.offsets = pointers;
        this.free += (capacity - keyCapacity) * loadFactor;
        this.keyCapacity = capacity;
    }

    private void resize() {
        long kCapacity = (kLimit - kStart) << 1;
        long kAddress = Unsafe.getUnsafe().allocateMemory(kCapacity + AbstractDirectList.CACHE_LINE_SIZE);
        long kStart = kAddress + (kAddress & (AbstractDirectList.CACHE_LINE_SIZE - 1));

        Unsafe.getUnsafe().copyMemory(this.kStart, kStart, kCapacity >> 1);
        Unsafe.getUnsafe().freeMemory(this.address);

        long d = kStart - this.kStart;
        key.startAddr += d;
        key.appendAddr += d;
        key.nextColOffset += d;


        this.address = kAddress;
        this.kStart = kStart;
        this.kLimit = kStart + kCapacity;
    }

    public static class Builder {
        private final List<ColumnMetadata> valueColumns = new ArrayList<>();
        private final List<ColumnMetadata> keyColumns = new ArrayList<>();
        private final List<MapRecordValueInterceptor> interceptors = new ArrayList<>();
        private int capacity = 67;
        private long dataSize = 4096;
        private float loadFactor = 0.5f;

        public MultiMap build() {
            return new MultiMap(capacity, dataSize, loadFactor, valueColumns, keyColumns, interceptors);
        }

        public Builder interceptor(MapRecordValueInterceptor interceptor) {
            interceptors.add(interceptor);
            return this;
        }

        public Builder keyColumn(ColumnMetadata metadata) {
            keyColumns.add(metadata);
            return this;
        }

        public Builder setCapacity(int capacity) {
            this.capacity = capacity;
            return this;
        }

        public Builder setDataSize(long dataSize) {
            this.dataSize = dataSize;
            return this;
        }

        public Builder setLoadFactor(float loadFactor) {
            this.loadFactor = loadFactor;
            return this;
        }

        public Builder valueColumn(ColumnMetadata metadata) {
            valueColumns.add(metadata);
            return this;
        }
    }

    public class Key {
        private long startAddr;
        private long appendAddr;

        private int len;
        private long nextColOffset;

        public Key commit() {
            Unsafe.getUnsafe().putInt(startAddr, len = (int) (appendAddr - startAddr));
            kPos = appendAddr;
            return this;
        }

        public Key init() {
            startAddr = kPos;
            appendAddr = startAddr + keyDataOffset;
            nextColOffset = startAddr + keyBlockOffset;
            return this;
        }

        public void put(long address, int len) {
            checkSize(len);
            Unsafe.getUnsafe().copyMemory(address, appendAddr, len);
            appendAddr += len;
            writeOffset();
        }

        public Key putInt(int value) {
            checkSize(4);
            Unsafe.getUnsafe().putInt(appendAddr, value);
            appendAddr += 4;
            writeOffset();
            return this;
        }

        public Key putLong(long value) {
            checkSize(8);
            Unsafe.getUnsafe().putLong(appendAddr, value);
            appendAddr += 8;
            writeOffset();
            return this;
        }

        public Key putStr(CharSequence value) {
            int len = value.length();
            checkSize(len << 1);
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putChar(appendAddr + (i << 1), value.charAt(i));
            }
            appendAddr += len << 1;
            writeOffset();
            return this;
        }

        private void checkSize(int size) {
            if (appendAddr + size > kLimit) {
                resize();
            }
        }

        private void writeOffset() {
            Unsafe.getUnsafe().putInt(nextColOffset, (int) (appendAddr - startAddr));
            nextColOffset += 4;
        }
    }
}