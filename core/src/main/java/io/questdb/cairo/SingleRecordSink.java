/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo;

import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Utf8Sequence;

public final class SingleRecordSink implements RecordSinkSPI, Mutable, Reopenable {
    private static final int INITIAL_CAPACITY_BYTES = 8;
    private final long maxHeapSize;
    private final int memoryTag;
    private long appendAddress;
    private long heapLimit;
    private long heapStart;

    public SingleRecordSink(long maxHeapSizeBytes, int memoryTag) {
        this.memoryTag = memoryTag;
        this.maxHeapSize = maxHeapSizeBytes;
    }

    @Override
    public void clear() {
        appendAddress = heapStart;
    }

    @Override
    public void close() {
        if (appendAddress != 0) {
            Unsafe.free(heapStart, heapLimit - heapStart, memoryTag);
            appendAddress = 0;
            heapStart = 0;
        }
    }

    public boolean memeq(SingleRecordSink other) {
        long thisSize = appendAddress - heapStart;
        long otherSize = other.appendAddress - other.heapStart;
        if (thisSize != otherSize) {
            return false;
        }
        return Vect.memeq(heapStart, other.heapStart, thisSize);
    }

    @Override
    public void putArray(ArrayView value) {
        long byteCount = ArrayTypeDriver.getPlainValueSize(value);
        checkCapacity(byteCount);
        ArrayTypeDriver.appendPlainValue(appendAddress, value);
        appendAddress += byteCount;
    }

    @Override
    public void putBin(BinarySequence value) {
        if (value == null) {
            putVarSizeNull();
        } else {
            long len = value.length() + 4L;
            if (len > Integer.MAX_VALUE) {
                throw CairoException.nonCritical().put("binary column is too large");
            }

            checkCapacity((int) len);
            int l = (int) (len - Integer.BYTES);
            Unsafe.getUnsafe().putInt(appendAddress, l);
            value.copyTo(appendAddress + Integer.BYTES, 0, l);
            appendAddress += len;
        }
    }

    @Override
    public void putBool(boolean value) {
        checkCapacity(1);
        Unsafe.getUnsafe().putBoolean(null, appendAddress, value);
        appendAddress += 1;
    }

    @Override
    public void putByte(byte value) {
        checkCapacity(1);
        Unsafe.getUnsafe().putByte(appendAddress, value);
        appendAddress += 1;
    }

    @Override
    public void putChar(char value) {
        checkCapacity(2);
        Unsafe.getUnsafe().putChar(appendAddress, value);
        appendAddress += 2;
    }

    @Override
    public void putDate(long value) {
        checkCapacity(8);
        Unsafe.getUnsafe().putLong(appendAddress, value);
        appendAddress += 8;
    }

    @Override
    public void putDecimal128(Decimal128 decimal128) {
        checkCapacity(16);
        Decimal128.put(decimal128, appendAddress);
        appendAddress += 16;
    }

    @Override
    public void putDecimal256(Decimal256 decimal256) {
        checkCapacity(32L);
        Decimal256.put(decimal256, appendAddress);
        appendAddress += 32L;
    }

    @Override
    public void putDouble(double value) {
        checkCapacity(8);
        Unsafe.getUnsafe().putDouble(appendAddress, value);
        appendAddress += 8;
    }

    @Override
    public void putFloat(float value) {
        checkCapacity(4);
        Unsafe.getUnsafe().putFloat(appendAddress, value);
        appendAddress += 4;
    }

    @Override
    public void putIPv4(int value) {
        checkCapacity(4);
        Unsafe.getUnsafe().putInt(appendAddress, value);
        appendAddress += 4;
    }

    @Override
    public void putInt(int value) {
        checkCapacity(4);
        Unsafe.getUnsafe().putInt(appendAddress, value);
        appendAddress += 4;
    }

    @Override
    public void putInterval(Interval interval) {
        checkCapacity(16);
        Unsafe.getUnsafe().putLong(appendAddress, interval.getLo());
        Unsafe.getUnsafe().putLong(appendAddress + 8, interval.getHi());
        appendAddress += 16;
    }

    @Override
    public void putLong(long value) {
        checkCapacity(8);
        Unsafe.getUnsafe().putLong(appendAddress, value);
        appendAddress += 8;
    }

    @Override
    public void putLong128(long lo, long hi) {
        checkCapacity(16);
        Unsafe.getUnsafe().putLong(appendAddress, lo);
        Unsafe.getUnsafe().putLong(appendAddress + 8, hi);
        appendAddress += 16;
    }

    @Override
    public void putLong256(Long256 value) {
        checkCapacity(32);
        Unsafe.getUnsafe().putLong(appendAddress, value.getLong0());
        Unsafe.getUnsafe().putLong(appendAddress + 8, value.getLong1());
        Unsafe.getUnsafe().putLong(appendAddress + 16, value.getLong2());
        Unsafe.getUnsafe().putLong(appendAddress + 24, value.getLong3());
        appendAddress += 32;
    }

    @Override
    public void putLong256(long l0, long l1, long l2, long l3) {
        checkCapacity(32);
        Unsafe.getUnsafe().putLong(appendAddress, l0);
        Unsafe.getUnsafe().putLong(appendAddress + 8, l1);
        Unsafe.getUnsafe().putLong(appendAddress + 16, l2);
        Unsafe.getUnsafe().putLong(appendAddress + 24, l3);
        appendAddress += 32;
    }

    @Override
    public void putRecord(Record value) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void putShort(short value) {
        checkCapacity(2);
        Unsafe.getUnsafe().putShort(appendAddress, value);
        appendAddress += 2;
    }

    @Override
    public void putStr(CharSequence value) {
        if (value == null) {
            putVarSizeNull();
            return;
        }

        int len = value.length();
        checkCapacity(((long) len << 1) + 4L);
        Unsafe.getUnsafe().putInt(appendAddress, len);
        appendAddress += 4L;
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putChar(appendAddress + ((long) i << 1), value.charAt(i));
        }
        appendAddress += (long) len << 1;
    }

    @Override
    public void putStr(CharSequence value, int lo, int hi) {
        int len = hi - lo;
        checkCapacity(((long) len << 1) + 4L);
        Unsafe.getUnsafe().putInt(appendAddress, len);
        appendAddress += 4L;
        for (int i = lo; i < hi; i++) {
            Unsafe.getUnsafe().putChar(appendAddress + ((long) (i - lo) << 1), value.charAt(i));
        }
        appendAddress += (long) len << 1;
    }

    @Override
    public void putTimestamp(long value) {
        checkCapacity(8);
        Unsafe.getUnsafe().putLong(appendAddress, value);
        appendAddress += 8;
    }

    @Override
    public void putVarchar(Utf8Sequence value) {
        int byteCount = VarcharTypeDriver.getSingleMemValueByteCount(value);
        checkCapacity(byteCount);
        VarcharTypeDriver.appendPlainValue(appendAddress, value, false);
        appendAddress += byteCount;
    }

    @Override
    public void reopen() {
        if (appendAddress == 0) {
            heapStart = Unsafe.malloc(INITIAL_CAPACITY_BYTES, memoryTag);
            heapLimit = heapStart + INITIAL_CAPACITY_BYTES;
        }
        appendAddress = heapStart;
    }

    @Override
    public void skip(int bytes) {
        checkCapacity(bytes);
        appendAddress += bytes;
    }

    private void checkCapacity(long requiredSize) {
        if (appendAddress + requiredSize > heapLimit) {
            resize(requiredSize, appendAddress);
        }
    }

    private void putVarSizeNull() {
        checkCapacity(4L);
        Unsafe.getUnsafe().putInt(appendAddress, TableUtils.NULL_LEN);
        appendAddress += 4L;
    }

    private void resize(long entrySize, long appendAddress) {
        assert appendAddress >= heapStart;
        long currentCapacity = heapLimit - heapStart;
        long newCapacity = currentCapacity << 1;
        long target = appendAddress + entrySize - heapStart;
        if (newCapacity < target) {
            newCapacity = Numbers.ceilPow2(target);
        }
        if (newCapacity > maxHeapSize) {
            throw LimitOverflowException.instance().put("limit of ").put(maxHeapSize).put(" memory exceeded in ASOF join");
        }
        long newAddress = Unsafe.realloc(heapStart, currentCapacity, newCapacity, memoryTag);

        long delta = newAddress - heapStart;
        this.heapStart = newAddress;
        this.heapLimit = newAddress + newCapacity;
        this.appendAddress += delta;
    }
}
