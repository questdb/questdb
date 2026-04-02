/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;


/**
 * A lightweight {@link RecordSinkSPI} that writes fixed-size key columns sequentially
 * to a pre-allocated off-heap buffer. Used for batched GROUP BY key packing where
 * multiple keys are written contiguously into the buffer.
 * <p>
 * Also owns a pre-allocated hashes buffer for the native hashAndPrefetch call.
 * <p>
 * Only fixed-size put methods are supported. Var-size methods throw
 * {@link UnsupportedOperationException}.
 */
public class BatchKeySink implements RecordSinkSPI, Reopenable {
    private final int batchSize;
    private final int keySize;
    private long appendAddr;
    private long hashesAddr;
    private long keysAddr;

    public BatchKeySink(int batchSize, int keySize) {
        this.batchSize = batchSize;
        this.keySize = keySize;
    }

    @Override
    public void close() {
        if (keysAddr != 0) {
            Unsafe.free(keysAddr, (long) batchSize * keySize, MemoryTag.NATIVE_DEFAULT);
            keysAddr = 0;
        }
        if (hashesAddr != 0) {
            Unsafe.free(hashesAddr, (long) batchSize * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            hashesAddr = 0;
        }
        appendAddr = 0;
    }

    public long getHashesAddr() {
        return hashesAddr;
    }

    public long getKeysAddr() {
        return keysAddr;
    }

    @Override
    public void reopen() {
        if (keysAddr == 0) {
            keysAddr = Unsafe.malloc((long) batchSize * keySize, MemoryTag.NATIVE_DEFAULT);
            hashesAddr = Unsafe.malloc((long) batchSize * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
        appendAddr = keysAddr;
    }

    /**
     * Resets the append position to the start of the keys buffer.
     */
    public void resetAppendAddr() {
        appendAddr = keysAddr;
    }

    @Override
    public void putBool(boolean value) {
        Unsafe.getUnsafe().putByte(appendAddr, (byte) (value ? 1 : 0));
        appendAddr += 1;
    }

    @Override
    public void putByte(byte value) {
        Unsafe.getUnsafe().putByte(appendAddr, value);
        appendAddr += 1;
    }

    @Override
    public void putChar(char value) {
        Unsafe.getUnsafe().putChar(appendAddr, value);
        appendAddr += 2;
    }

    @Override
    public void putDate(long value) {
        Unsafe.getUnsafe().putLong(appendAddr, value);
        appendAddr += 8;
    }

    @Override
    public void putDecimal128(Decimal128 value) {
        Decimal128.put(value, appendAddr);
        appendAddr += 16;
    }

    @Override
    public void putDecimal256(Decimal256 value) {
        Decimal256.put(value, appendAddr);
        appendAddr += 32;
    }

    @Override
    public void putDouble(double value) {
        Unsafe.getUnsafe().putDouble(appendAddr, value);
        appendAddr += 8;
    }

    @Override
    public void putFloat(float value) {
        Unsafe.getUnsafe().putFloat(appendAddr, value);
        appendAddr += 4;
    }

    @Override
    public void putIPv4(int value) {
        Unsafe.getUnsafe().putInt(appendAddr, value);
        appendAddr += 4;
    }

    @Override
    public void putInt(int value) {
        Unsafe.getUnsafe().putInt(appendAddr, value);
        appendAddr += 4;
    }

    @Override
    public void putInterval(Interval interval) {
        Unsafe.getUnsafe().putLong(appendAddr, interval.getLo());
        Unsafe.getUnsafe().putLong(appendAddr + 8, interval.getHi());
        appendAddr += 16;
    }

    @Override
    public void putLong(long value) {
        Unsafe.getUnsafe().putLong(appendAddr, value);
        appendAddr += 8;
    }

    @Override
    public void putLong128(long lo, long hi) {
        Unsafe.getUnsafe().putLong(appendAddr, lo);
        Unsafe.getUnsafe().putLong(appendAddr + 8, hi);
        appendAddr += 16;
    }

    @Override
    public void putLong256(Long256 value) {
        Unsafe.getUnsafe().putLong(appendAddr, value.getLong0());
        Unsafe.getUnsafe().putLong(appendAddr + 8, value.getLong1());
        Unsafe.getUnsafe().putLong(appendAddr + 16, value.getLong2());
        Unsafe.getUnsafe().putLong(appendAddr + 24, value.getLong3());
        appendAddr += 32;
    }

    @Override
    public void putLong256(long l0, long l1, long l2, long l3) {
        Unsafe.getUnsafe().putLong(appendAddr, l0);
        Unsafe.getUnsafe().putLong(appendAddr + 8, l1);
        Unsafe.getUnsafe().putLong(appendAddr + 16, l2);
        Unsafe.getUnsafe().putLong(appendAddr + 24, l3);
        appendAddr += 32;
    }

    @Override
    public void putShort(short value) {
        Unsafe.getUnsafe().putShort(appendAddr, value);
        appendAddr += 2;
    }

    @Override
    public void putTimestamp(long value) {
        Unsafe.getUnsafe().putLong(appendAddr, value);
        appendAddr += 8;
    }

    @Override
    public void skip(int bytes) {
        appendAddr += bytes;
    }

    @Override
    public void putArray(ArrayView view) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putBin(BinarySequence value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putRecord(Record value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putStr(CharSequence value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putStr(CharSequence value, int lo, int hi) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putVarchar(Utf8Sequence value) {
        throw new UnsupportedOperationException();
    }
}
