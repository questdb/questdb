/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.ql.collections;

import com.nfsdb.collections.DirectInputStream;
import com.nfsdb.collections.DirectMemoryStructure;
import com.nfsdb.collections.LongList;
import com.nfsdb.collections.Mutable;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.RecordMetadata;
import com.nfsdb.utils.Hash;
import com.nfsdb.utils.Numbers;
import com.nfsdb.utils.Unsafe;

import java.util.ArrayList;
import java.util.List;

public class MultiMap extends DirectMemoryStructure implements Mutable {

    private static final int MIN_INITIAL_CAPACITY = 128;
    private final float loadFactor;
    private final KeyWriter keyWriter = new KeyWriter();
    private final MapRecordSource recordSource;
    private final MapValues values;
    private final MapMetadata metadata;
    private int keyBlockOffset;
    private int keyDataOffset;
    private LongList offsets;
    private long kStart;
    private long kLimit;
    private long kPos;
    private int free;
    private int keyCapacity;
    private int size = 0;
    private int mask;

    private MultiMap(int capacity, long dataSize, float loadFactor, List<RecordColumnMetadata> valueColumns, List<RecordColumnMetadata> keyColumns, List<MapRecordValueInterceptor> interceptors) {
        this.loadFactor = loadFactor;
        this.address = Unsafe.getUnsafe().allocateMemory(dataSize + Unsafe.CACHE_LINE_SIZE);
        this.kStart = kPos = this.address + (this.address & (Unsafe.CACHE_LINE_SIZE - 1));
        this.kLimit = kStart + dataSize;

        this.keyCapacity = (int) (capacity / loadFactor);
        this.keyCapacity = this.keyCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(this.keyCapacity);
        this.mask = keyCapacity - 1;
        this.free = (int) (keyCapacity * loadFactor);
        this.offsets = new LongList(keyCapacity);
        this.offsets.setPos(keyCapacity);
        this.offsets.zero((byte) -1);
        int columnSplit = valueColumns.size();
        int[] valueOffsets = new int[columnSplit];

        int offset = 4;
        for (int i = 0; i < valueOffsets.length; i++) {
            valueOffsets[i] = offset;
            switch (valueColumns.get(i).getType()) {
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
        this.metadata = new MapMetadata(valueColumns, keyColumns);
        this.keyBlockOffset = offset;
        this.keyDataOffset = this.keyBlockOffset + 4 * keyColumns.size();
        MapRecord record = new MapRecord(metadata, valueOffsets, keyDataOffset, keyBlockOffset);
        this.recordSource = new MapRecordSource(record, this.values, interceptors);
    }

    public void clear() {
        kPos = kStart;
        free = (int) (keyCapacity * loadFactor);
        size = 0;
        offsets.fill(-1);
    }

    public RecordCursor<Record> getCursor() {
        return recordSource.init(kStart, size);
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    public MapValues getOrCreateValues(KeyWriter keyWriter) {
        keyWriter.commit();
        // calculate hash remembering "key" structure
        // [ len | value block | key offset block | key data block ]
        int index = Hash.hashMem(keyWriter.startAddr + keyBlockOffset, keyWriter.len - keyBlockOffset) & mask;
        long offset = offsets.getQuick(index);

        if (offset == -1) {
            offsets.setQuick(index, keyWriter.startAddr - kStart);
            if (--free == 0) {
                rehash();
            }
            size++;
            return values.init(keyWriter.startAddr, true);
        } else if (eq(keyWriter, offset)) {
            // rollback added key
            kPos = keyWriter.startAddr;
            return values.init(kStart + offset, false);
        } else {
            return probe0(keyWriter, index);
        }
    }

    public MapValues getValues(KeyWriter keyWriter) {
        keyWriter.commit();
        // rollback key right away
        kPos = keyWriter.startAddr;
        int index = Hash.hashMem(keyWriter.startAddr + keyBlockOffset, keyWriter.len - keyBlockOffset) & mask;
        long offset = offsets.get(index);

        if (offset == -1) {
            return null;
        } else if (eq(keyWriter, offset)) {
            return values.init(kStart + offset, false);
        } else {
            return probeReadOnly(keyWriter, index);
        }
    }

    public KeyWriter keyWriter() {
        return keyWriter.init();
    }

    public int size() {
        return size;
    }

    private boolean eq(KeyWriter keyWriter, long offset) {
        long a = kStart + offset;
        long b = keyWriter.startAddr;

        // check length first
        if (Unsafe.getUnsafe().getInt(a) != Unsafe.getUnsafe().getInt(b)) {
            return false;
        }

        long lim = b + keyWriter.len;

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

    private MapValues probe0(KeyWriter keyWriter, int index) {
        long offset;
        while ((offset = offsets.get(index = (++index & mask))) != -1) {
            if (eq(keyWriter, offset)) {
                kPos = keyWriter.startAddr;
                return values.init(kStart + offset, false);
            }
        }
        offsets.setQuick(index, keyWriter.startAddr - kStart);
        free--;
        if (free == 0) {
            rehash();
        }

        size++;
        return values.init(keyWriter.startAddr, true);
    }

    private MapValues probeReadOnly(KeyWriter keyWriter, int index) {
        long offset;
        while ((offset = offsets.get(index = (++index & mask))) != -1) {
            if (eq(keyWriter, offset)) {
                return values.init(kStart + offset, false);
            }
        }
        return null;
    }

    private void rehash() {
        int capacity = keyCapacity << 1;
        mask = capacity - 1;
        LongList pointers = new LongList(capacity);
        pointers.zero((byte) -1);
        pointers.setPos(capacity);

        for (int i = 0, k = this.offsets.size(); i < k; i++) {
            long offset = this.offsets.get(i);
            if (offset == -1) {
                continue;
            }
            int index = Hash.hashMem(kStart + offset + keyBlockOffset, Unsafe.getUnsafe().getInt(kStart + offset) - keyBlockOffset) & mask;
            while (pointers.get(index) != -1) {
                index = (index + 1) & mask;
            }
            pointers.setQuick(index, offset);
        }
        this.offsets = pointers;
        this.free += (capacity - keyCapacity) * loadFactor;
        this.keyCapacity = capacity;
    }

    private void resize() {
        long kCapacity = (kLimit - kStart) << 1;
        long kAddress = Unsafe.getUnsafe().allocateMemory(kCapacity + Unsafe.CACHE_LINE_SIZE);
        long kStart = kAddress + (kAddress & (Unsafe.CACHE_LINE_SIZE - 1));

        Unsafe.getUnsafe().copyMemory(this.kStart, kStart, kCapacity >> 1);
        Unsafe.getUnsafe().freeMemory(this.address);

        long d = kStart - this.kStart;
        keyWriter.startAddr += d;
        keyWriter.appendAddr += d;
        keyWriter.nextColOffset += d;


        this.address = kAddress;
        this.kStart = kStart;
        this.kLimit = kStart + kCapacity;
    }

    public static class Builder {
        private final List<RecordColumnMetadata> valueColumns = new ArrayList<>();
        private final List<RecordColumnMetadata> keyColumns = new ArrayList<>();
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

        public Builder keyColumn(RecordColumnMetadata metadata) {
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

        public Builder valueColumn(RecordColumnMetadata metadata) {
            valueColumns.add(metadata);
            return this;
        }
    }

    public class KeyWriter {
        private long startAddr;
        private long appendAddr;

        private int len;
        private long nextColOffset;

        public KeyWriter commit() {
            Unsafe.getUnsafe().putInt(startAddr, len = (int) (appendAddr - startAddr));
            kPos = appendAddr;
            return this;
        }

        public KeyWriter init() {
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

        public KeyWriter putBin(DirectInputStream stream) {
            long length = stream.size();
            checkSize((int) length);
            length = stream.copyTo(appendAddr, 0, length);
            appendAddr += length;
            return this;
        }

        public KeyWriter putBoolean(boolean value) {
            checkSize(1);
            Unsafe.getUnsafe().putByte(appendAddr, (byte) (value ? 1 : 0));
            appendAddr += 1;
            writeOffset();
            return this;
        }

        public KeyWriter putByte(byte value) {
            checkSize(1);
            Unsafe.getUnsafe().putByte(appendAddr, value);
            appendAddr += 1;
            writeOffset();
            return this;
        }

        public KeyWriter putDouble(double value) {
            checkSize(8);
            Unsafe.getUnsafe().putDouble(appendAddr, value);
            appendAddr += 8;
            writeOffset();
            return this;
        }

        public KeyWriter putFloat(float value) {
            checkSize(4);
            Unsafe.getUnsafe().putFloat(appendAddr, value);
            appendAddr += 4;
            writeOffset();
            return this;
        }

        public KeyWriter putInt(int value) {
            checkSize(4);
            Unsafe.getUnsafe().putInt(appendAddr, value);
            appendAddr += 4;
            writeOffset();
            return this;
        }

        public KeyWriter putLong(long value) {
            checkSize(8);
            Unsafe.getUnsafe().putLong(appendAddr, value);
            appendAddr += 8;
            writeOffset();
            return this;
        }

        public KeyWriter putShort(short value) {
            checkSize(2);
            Unsafe.getUnsafe().putShort(appendAddr, value);
            appendAddr += 2;
            writeOffset();
            return this;
        }

        public KeyWriter putStr(CharSequence value) {
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