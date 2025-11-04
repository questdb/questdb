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

package io.questdb.cairo.vm.api;

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.vm.Vm;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Long256;
import io.questdb.std.Long256Acceptor;
import io.questdb.std.Long256FromCharSequenceDecoder;
import io.questdb.std.Long256Impl;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

// contiguous appendable readable writable
public interface MemoryCARW extends MemoryCR, MemoryARW, MemoryCA, MemoryMAT {

    @Override
    default long getAddress() {
        return getPageAddress(0);
    }

    @Override
    default void putArray(ArrayView array) {
        long size = ArrayTypeDriver.getPlainValueSize(array);
        long addr = appendAddressFor(size);
        ArrayTypeDriver.appendPlainValue(addr, array);
    }

    @Override
    default long putBin(BinarySequence value) {
        if (value != null) {
            final long len = value.length();
            long addr = appendAddressFor(len + Long.BYTES);
            Unsafe.getUnsafe().putLong(addr, len);
            value.copyTo(addr + Long.BYTES, 0, len);
            return getAppendOffset();
        }
        return putNullBin();
    }

    @Override
    default long putBin(long from, long len) {
        if (len > 0) {
            long addr = appendAddressFor(len + Long.BYTES);
            Unsafe.getUnsafe().putLong(addr, len);
            Vect.memcpy(addr + Long.BYTES, from, len);
            return getAppendOffset();
        }
        return putNullBin();
    }

    @Override
    default void putBlockOfBytes(long from, long len) {
        Vect.memcpy(appendAddressFor(len), from, len);
    }

    @Override
    default void putBool(boolean value) {
        putByte((byte) (value ? 1 : 0));
    }

    @Override
    default void putBool(long offset, boolean value) {
        putByte(offset, (byte) (value ? 1 : 0));
    }

    @Override
    default void putByte(byte value) {
        Unsafe.getUnsafe().putByte(appendAddressFor(Byte.BYTES), value);
    }

    @Override
    default void putByte(long offset, byte value) {
        Unsafe.getUnsafe().putByte(appendAddressFor(offset, Byte.BYTES), value);
    }

    @Override
    default void putChar(char value) {
        Unsafe.getUnsafe().putChar(appendAddressFor(Character.BYTES), value);
    }

    @Override
    default void putChar(long offset, char value) {
        Unsafe.getUnsafe().putChar(appendAddressFor(offset, Character.BYTES), value);
    }

    @Override
    default void putDecimal128(long high, long low) {
        Decimal128.put(high, low, appendAddressFor(16));
    }

    @Override
    default void putDecimal256(long hh, long hl, long lh, long ll) {
        Decimal256.put(hh, hl, lh, ll, appendAddressFor(32));
    }

    @Override
    default void putDecimal128(long offset, long high, long low) {
        Decimal128.put(high, low, appendAddressFor(offset, 16));
    }

    @Override
    default void putDecimal256(long offset, long hh, long hl, long lh, long ll) {
        Decimal256.put(hh, hl, lh, ll, appendAddressFor(offset, 32));
    }

    @Override
    default void putDouble(double value) {
        Unsafe.getUnsafe().putDouble(appendAddressFor(Double.BYTES), value);
    }

    @Override
    default void putDouble(long offset, double value) {
        Unsafe.getUnsafe().putDouble(appendAddressFor(offset, Double.BYTES), value);
    }

    @Override
    default void putFloat(float value) {
        Unsafe.getUnsafe().putFloat(appendAddressFor(Float.BYTES), value);
    }

    @Override
    default void putFloat(long offset, float value) {
        Unsafe.getUnsafe().putFloat(appendAddressFor(offset, Float.BYTES), value);
    }

    @Override
    default void putInt(int value) {
        Unsafe.getUnsafe().putInt(appendAddressFor(Integer.BYTES), value);
    }

    @Override
    default void putInt(long offset, int value) {
        Unsafe.getUnsafe().putInt(appendAddressFor(offset, Integer.BYTES), value);
    }

    @Override
    default void putLong(long value) {
        Unsafe.getUnsafe().putLong(appendAddressFor(Long.BYTES), value);
    }

    @Override
    default void putLong(long offset, long value) {
        Unsafe.getUnsafe().putLong(appendAddressFor(offset, Long.BYTES), value);
    }

    @Override
    default void putLong128(long lo, long hi) {
        long addr = appendAddressFor(2 * Long.BYTES);
        Unsafe.getUnsafe().putLong(addr, lo);
        Unsafe.getUnsafe().putLong(addr + Long.BYTES, hi);
    }

    @Override
    default void putLong256(long l0, long l1, long l2, long l3) {
        final long addr = appendAddressFor(32);
        Unsafe.getUnsafe().putLong(addr, l0);
        Unsafe.getUnsafe().putLong(addr + Long.BYTES, l1);
        Unsafe.getUnsafe().putLong(addr + Long.BYTES * 2, l2);
        Unsafe.getUnsafe().putLong(addr + Long.BYTES * 3, l3);
    }

    @Override
    default void putLong256(Long256 value) {
        putLong256(
                value.getLong0(),
                value.getLong1(),
                value.getLong2(),
                value.getLong3()
        );
    }

    @Override
    default void putLong256(CharSequence hexString) {
        final int len;
        if (hexString == null || (len = hexString.length()) == 0) {
            putLong256Null();
        } else {
            putLong256(hexString, 2, len);
        }
    }

    @Override
    default void putLong256(long offset, Long256 value) {
        putLong256(
                offset,
                value.getLong0(),
                value.getLong1(),
                value.getLong2(),
                value.getLong3()
        );
    }

    @Override
    default void putLong256(long offset, long l0, long l1, long l2, long l3) {
        final long addr = appendAddressFor(offset, Long256.BYTES);
        Unsafe.getUnsafe().putLong(addr, l0);
        Unsafe.getUnsafe().putLong(addr + Long.BYTES, l1);
        Unsafe.getUnsafe().putLong(addr + Long.BYTES * 2, l2);
        Unsafe.getUnsafe().putLong(addr + Long.BYTES * 3, l3);
    }

    default void putLong256(CharSequence hexString, int start, int end, Long256Acceptor acceptor) {
        Long256FromCharSequenceDecoder.decode(hexString, start, end, acceptor);
    }

    default void putLong256Null() {
        Long256Impl.putNull(appendAddressFor(Long256.BYTES));
    }

    @Override
    default void putLong256Utf8(@Nullable Utf8Sequence hexString) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long putNullBin() {
        putLong(TableUtils.NULL_LEN);
        return getAppendOffset();
    }

    @Override
    default long putNullStr() {
        putInt(TableUtils.NULL_LEN);
        return getAppendOffset();
    }

    @Override
    default void putNullStr(long offset) {
        putInt(offset, TableUtils.NULL_LEN);
    }

    @Override
    default void putShort(short value) {
        Unsafe.getUnsafe().putShort(appendAddressFor(Short.BYTES), value);
    }

    @Override
    default void putShort(long offset, short value) {
        Unsafe.getUnsafe().putShort(appendAddressFor(offset, Short.BYTES), value);
    }

    @Override
    default long putStr(CharSequence value) {
        return value != null ? putStrUnsafe(value, 0, value.length()) : putNullStr();
    }

    @Override
    default long putStr(char value) {
        if (value != 0) {
            long addr = appendAddressFor(Integer.BYTES + Character.BYTES);
            Unsafe.getUnsafe().putInt(addr, 1);
            Unsafe.getUnsafe().putChar(addr + Integer.BYTES, value);
            return getAppendOffset();
        }
        return putNullStr();
    }

    @Override
    default long putStr(CharSequence value, int pos, int len) {
        if (value != null) {
            return putStrUnsafe(value, pos, len);
        }
        return putNullStr();
    }

    @Override
    default void putStr(long offset, CharSequence value) {
        if (value != null) {
            putStr(offset, value, 0, value.length());
        } else {
            putNullStr(offset);
        }
    }

    @Override
    default void putStr(long offset, CharSequence value, int pos, int len) {
        final long addr = appendAddressFor(offset, Vm.getStorageLength(len));
        Unsafe.getUnsafe().putInt(addr, len);
        Chars.copyStrChars(value, pos, len, addr + Vm.STRING_LENGTH_BYTES);
    }

    default long putStrUnsafe(CharSequence value, int pos, int len) {
        final long storageLen = Vm.getStorageLength(len);
        final long addr = appendAddressFor(storageLen);
        Unsafe.getUnsafe().putInt(addr, len);
        Chars.copyStrChars(value, pos, len, addr + Integer.BYTES);
        return getAppendOffset();
    }

    @Override
    default long putStrUtf8(DirectUtf8Sequence value) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long putVarchar(@NotNull Utf8Sequence value, int lo, int hi) {
        final long offset = getAppendOffset();
        value.writeTo(appendAddressFor(hi - lo), lo, hi);
        return offset;
    }

    @Override
    default void putVarchar(long offset, @NotNull Utf8Sequence value, int lo, int hi) {
        value.writeTo(appendAddressFor(offset, hi - lo), lo, hi);
    }

    void shiftAddressRight(long shiftRightOffset);

    @Override
    default void zeroMem(int length) {
        Unsafe.getUnsafe().setMemory(appendAddressFor(length), length, (byte) 0);
    }
}
