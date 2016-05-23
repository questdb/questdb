/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.questdb.ql.impl.map;

import com.questdb.ex.JournalRuntimeException;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Hash;
import com.questdb.misc.Numbers;
import com.questdb.misc.Unsafe;
import com.questdb.ql.RecordCursor;
import com.questdb.std.*;
import com.questdb.store.VariableColumn;

public class MultiMap extends DirectMemoryStructure implements Mutable {

    private static final int MIN_INITIAL_CAPACITY = 128;
    private final float loadFactor;
    private final KeyWriter keyWriter = new KeyWriter();
    private final MapRecordSource recordSource;
    private final MapValues values;
    private final MapMetadata metadata;
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

    public MultiMap(
            int pageSize,
            @Transient RecordMetadata keySourceMetadata,
            @Transient ObjHashSet<String> keyNames,
            @Transient ObjList<RecordColumnMetadata> valueColumns,
            ObjList<MapRecordValueInterceptor> interceptors) {
        this(64, pageSize, 0.5f, keySourceMetadata, keyNames, valueColumns, interceptors);
    }

    private MultiMap(int capacity,
                     int pageSize,
                     float loadFactor,
                     @Transient RecordMetadata keySourceMetadata,
                     @Transient ObjHashSet<String> keyNames,
                     @Transient ObjList<RecordColumnMetadata> valueColumns,
                     ObjList<MapRecordValueInterceptor> interceptors) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("pageSize must be > 0");
        }
        this.loadFactor = loadFactor;
        this.address = Unsafe.getUnsafe().allocateMemory(pageSize + Unsafe.CACHE_LINE_SIZE);
        this.kStart = kPos = this.address + (this.address & (Unsafe.CACHE_LINE_SIZE - 1));
        this.kLimit = kStart + pageSize;

        this.keyCapacity = (int) (capacity / loadFactor);
        this.keyCapacity = this.keyCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(this.keyCapacity);
        this.mask = keyCapacity - 1;
        this.free = (int) (keyCapacity * loadFactor);
        this.offsets = new DirectLongList(keyCapacity);
        this.offsets.setPos(keyCapacity);
        this.offsets.zero(-1);
        int columnSplit = valueColumns.size();
        int[] valueOffsets = new int[columnSplit];

        int offset = 4;
        for (int i = 0; i < valueOffsets.length; i++) {
            valueOffsets[i] = offset;
            switch (valueColumns.get(i).getType()) {
                case BYTE:
                case BOOLEAN:
                    offset++;
                    break;
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
        this.metadata = new MapMetadata(keySourceMetadata, keyNames, valueColumns);
        this.keyBlockOffset = offset;
        this.keyDataOffset = this.keyBlockOffset + 4 * keyNames.size();
        MapRecord record = new MapRecord(metadata, valueOffsets, keyDataOffset, keyBlockOffset);
        this.recordSource = new MapRecordSource(record, this.values, interceptors);
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
        super.close();
    }

    public RecordCursor getCursor() {
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
        long offset = offsets.get(index);

        if (offset == -1) {
            offsets.set(index, keyWriter.startAddr - kStart);
            if (--free == 0) {
                rehash();
            }
            size++;
            return values.of(keyWriter.startAddr, true);
        } else if (eq(keyWriter, offset)) {
            // rollback added key
            kPos = keyWriter.startAddr;
            return values.of(kStart + offset, false);
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
            return values.of(kStart + offset, false);
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
                return values.of(kStart + offset, false);
            }
        }
        offsets.set(index, keyWriter.startAddr - kStart);
        free--;
        if (free == 0) {
            rehash();
        }

        size++;
        return values.of(keyWriter.startAddr, true);
    }

    private MapValues probeReadOnly(KeyWriter keyWriter, int index) {
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
            int index = Hash.hashMem(kStart + offset + keyBlockOffset, Unsafe.getUnsafe().getInt(kStart + offset) - keyBlockOffset) & mask;
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
            if (value == null) {
                return putNull();
            }

            int len = value.length();
            checkSize((len << 1) + 4);
            Unsafe.getUnsafe().putInt(appendAddr, len);
            appendAddr += 4;
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

        private KeyWriter putNull() {
            checkSize(4);
            Unsafe.getUnsafe().putInt(appendAddr, VariableColumn.NULL_LEN);
            appendAddr += 4;
            writeOffset();
            return this;
        }

        private void writeOffset() {
            Unsafe.getUnsafe().putInt(nextColOffset, (int) (appendAddr - startAddr));
            nextColOffset += 4;
        }
    }
}