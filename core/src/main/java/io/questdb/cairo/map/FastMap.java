/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

public class FastMap implements Map {

    private static final HashFunction DEFAULT_HASH = Hash::hashMem;
    private static final int MIN_INITIAL_CAPACITY = 128;
    private final double loadFactor;
    private final Key key = new Key();
    private final FastMapValue value;
    private final FastMapValue value2;
    private final FastMapValue value3;
    private final FastMapCursor cursor;
    private final FastMapRecord record;
    private final int valueColumnCount;
    private final HashFunction hashFunction;
    private long capacity;
    private final int keyBlockOffset;
    private final int keyDataOffset;
    private DirectLongList offsets;
    private long kStart;
    private long kLimit;
    private long kPos;
    private int free;
    private int keyCapacity;
    private int size = 0;
    private int mask;
    private int nResizes;
    private final int maxResizes;

    public FastMap(int pageSize,
                   @Transient @NotNull ColumnTypes keyTypes,
                   int keyCapacity,
                   double loadFactor,
                   int maxResizes
    ) {
        this(pageSize, keyTypes, null, keyCapacity, loadFactor, DEFAULT_HASH, maxResizes);
    }

    public FastMap(int pageSize,
                   @Transient @NotNull ColumnTypes keyTypes,
                   @Transient @Nullable ColumnTypes valueTypes,
                   int keyCapacity,
                   double loadFactor,
                   int maxResizes
    ) {
        this(pageSize, keyTypes, valueTypes, keyCapacity, loadFactor, DEFAULT_HASH, maxResizes);
    }

    FastMap(int pageSize,
            @Transient ColumnTypes keyTypes,
            @Transient ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            HashFunction hashFunction,
            int maxResizes

    ) {
        assert pageSize > 3;
        assert loadFactor > 0 && loadFactor < 1d;

        this.loadFactor = loadFactor;
        this.kStart = kPos = Unsafe.malloc(this.capacity = pageSize, MemoryTag.NATIVE_FAST_MAP);
        this.kLimit = kStart + pageSize;

        this.keyCapacity = (int) (keyCapacity / loadFactor);
        this.keyCapacity = this.keyCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(this.keyCapacity);
        this.mask = this.keyCapacity - 1;
        this.free = (int) (this.keyCapacity * loadFactor);
        this.offsets = new DirectLongList(this.keyCapacity);
        this.offsets.setPos(this.keyCapacity);
        this.offsets.zero(-1);
        this.hashFunction = hashFunction;
        this.nResizes = 0;
        this.maxResizes = maxResizes;

        int[] valueOffsets;
        int offset = 4;
        if (valueTypes != null) {
            this.valueColumnCount = valueTypes.getColumnCount();
            final int columnSplit = valueColumnCount;
            valueOffsets = new int[columnSplit];

            for (int i = 0; i < columnSplit; i++) {
                valueOffsets[i] = offset;
                final int columnType = valueTypes.getColumnType(i);
                switch (ColumnType.tagOf(columnType)) {
                    case ColumnType.BYTE:
                    case ColumnType.BOOLEAN:
                    case ColumnType.GEOBYTE:
                        offset++;
                        break;
                    case ColumnType.SHORT:
                    case ColumnType.CHAR:
                    case ColumnType.GEOSHORT:
                        offset += 2;
                        break;
                    case ColumnType.INT:
                    case ColumnType.FLOAT:
                    case ColumnType.SYMBOL:
                    case ColumnType.GEOINT:
                        offset += 4;
                        break;
                    case ColumnType.LONG:
                    case ColumnType.DOUBLE:
                    case ColumnType.DATE:
                    case ColumnType.TIMESTAMP:
                    case ColumnType.GEOLONG:
                        offset += 8;
                        break;
                    case ColumnType.LONG256:
                        offset += Long256.BYTES;
                        break;
                    default:
                        close();
                        throw CairoException.instance(0).put("value type is not supported: ").put(ColumnType.nameOf(columnType));
                }
            }
            this.value = new FastMapValue(valueOffsets);
            this.value2 = new FastMapValue(valueOffsets);
            this.value3 = new FastMapValue(valueOffsets);
            this.keyBlockOffset = offset;
            this.keyDataOffset = this.keyBlockOffset + 4 * keyTypes.getColumnCount();
            this.record = new FastMapRecord(valueOffsets, columnSplit, keyDataOffset, keyBlockOffset, value, keyTypes);
        } else {
            this.valueColumnCount = 0;
            this.value = new FastMapValue(null);
            this.value2 = new FastMapValue(null);
            this.value3 = new FastMapValue(null);
            this.keyBlockOffset = offset;
            this.keyDataOffset = this.keyBlockOffset + 4 * keyTypes.getColumnCount();
            this.record = new FastMapRecord(null, 0, keyDataOffset, keyBlockOffset, value, keyTypes);
        }
        assert this.keyBlockOffset < kLimit - kStart : "page size is too small for number of columns";
        this.cursor = new FastMapCursor(record, this);
    }

    private static boolean eqMixed(long a, long b, long lim) {
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

    private static boolean eqLong(long a, long b, long lim) {
        while (b < lim) {
            if (Unsafe.getUnsafe().getLong(a) != Unsafe.getUnsafe().getLong(b)) {
                return false;
            }
            a += 8;
            b += 8;
        }
        return true;
    }

    private static boolean eqInt(long a, long b, long lim) {
        while (b < lim) {
            if (Unsafe.getUnsafe().getInt(a) != Unsafe.getUnsafe().getInt(b)) {
                return false;
            }
            a += 4;
            b += 4;
        }
        return true;
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
            Unsafe.free(kStart, capacity, MemoryTag.NATIVE_FAST_MAP);
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
        return valueOf(address, false, this.value);
    }

    private FastMapValue asNew(Key keyWriter, int index, FastMapValue value) {
        kPos = keyWriter.appendAddress;
        offsets.set(index, keyWriter.startAddress - kStart);
        if (--free == 0) {
            rehash();
        }
        size++;
        return valueOf(keyWriter.startAddress, true, value);
    }

    @Override
    public MapKey withKey() {
        return key.init();
    }

    private FastMapValue probe0(Key keyWriter, int index, FastMapValue value) {
        long offset;
        while ((offset = offsets.get(index = (++index & mask))) != -1) {
            if (eq(keyWriter, offset)) {
                return valueOf(kStart + offset, false, value);
            }
        }
        return asNew(keyWriter, index, value);
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

        long d = lim - b;
        if (d % Long.BYTES == 0) {
            return eqLong(a, b, lim);
        }

        if (d % Integer.BYTES == 0) {
            return eqInt(a, b, lim);
        }

        return eqMixed(a, b, lim);
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

    private FastMapValue probeReadOnly(Key keyWriter, int index, FastMapValue value) {
        long offset;
        while ((offset = offsets.get(index = (++index & mask))) != -1) {
            if (eq(keyWriter, offset)) {
                return valueOf(kStart + offset, false, value);
            }
        }
        return null;
    }

    private void resize(int size) {
        if (nResizes < maxResizes) {
            nResizes++;
            long kCapacity = (kLimit - kStart) << 1;
            long target = key.appendAddress + size - kStart;
            if (kCapacity < target) {
                kCapacity = Numbers.ceilPow2(target);
            }
            long kAddress = Unsafe.realloc(this.kStart, this.capacity, kCapacity, MemoryTag.NATIVE_FAST_MAP);

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
        } else {
            throw LimitOverflowException.instance().put("limit of ").put(maxResizes).put(" resizes exceeded in FastMap");
        }
    }

    private void rehash() {
        int capacity = keyCapacity << 1;
        mask = capacity - 1;
        DirectLongList pointers = new DirectLongList(capacity);
        pointers.setPos(capacity);
        pointers.zero(-1);

        for (long i = 0, k = this.offsets.size(); i < k; i++) {
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

    private FastMapValue valueOf(long address, boolean _new, FastMapValue value) {
        return value.of(address, _new);
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

        private MapValue createValue(FastMapValue value) {
            commit();
            // calculate hash remembering "key" structure
            // [ len | value block | key offset block | key data block ]
            int index = keyIndex();
            long offset = offsets.get(index);

            if (offset == -1) {
                return asNew(this, index, value);
            } else if (eq(this, offset)) {
                return valueOf(kStart + offset, false, value);
            } else {
                return probe0(this, index, value);
            }
        }

        private MapValue findValue(FastMapValue value) {
            commit();
            int index = keyIndex();
            long offset = offsets.get(index);

            if (offset == -1) {
                return null;
            } else if (eq(this, offset)) {
                return valueOf(kStart + offset, false, value);
            } else {
                return probeReadOnly(this, index, value);
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
            checkSize(Double.BYTES);
            Unsafe.getUnsafe().putDouble(appendAddress, value);
            appendAddress += Double.BYTES;
            writeOffset();
        }

        @Override
        public void putFloat(float value) {
            checkSize(Float.BYTES);
            Unsafe.getUnsafe().putFloat(appendAddress, value);
            appendAddress += Float.BYTES;
            writeOffset();
        }

        @Override
        public void putInt(int value) {
            checkSize(Integer.BYTES);
            Unsafe.getUnsafe().putInt(appendAddress, value);
            appendAddress += Integer.BYTES;
            writeOffset();
        }

        @Override
        public void putLong(long value) {
            checkSize(Long.BYTES);
            Unsafe.getUnsafe().putLong(appendAddress, value);
            appendAddress += Long.BYTES;
            writeOffset();
        }

        @Override
        public void putLong256(Long256 value) {
            checkSize(Long256.BYTES);
            Unsafe.getUnsafe().putLong(appendAddress, value.getLong0());
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES, value.getLong1());
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES * 2, value.getLong2());
            Unsafe.getUnsafe().putLong(appendAddress + Long.BYTES * 3, value.getLong3());
            appendAddress += Long256.BYTES;
            writeOffset();
        }

        @Override
        public void skip(int bytes) {
            checkSize(bytes);
            appendAddress += bytes;
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
                Unsafe.getUnsafe().putChar(appendAddress + ((long) i << 1), value.charAt(i));
            }
            appendAddress += (long) len << 1;
            writeOffset();
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
            writeOffset();
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
            writeOffset();
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
            writeOffset();
        }

        @Override
        public void putRecord(Record value) {
            // noop
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