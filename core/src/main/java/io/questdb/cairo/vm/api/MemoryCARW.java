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

package io.questdb.cairo.vm.api;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.std.*;

public interface MemoryCARW extends MemoryCR, MemoryARW {

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

    default long putBin(long from, long len) {
        if (len > 0) {
            long addr = appendAddressFor(len + Long.BYTES);
            Unsafe.getUnsafe().putLong(addr, len);
            Vect.memcpy(from, addr + Long.BYTES, len);
            return getAppendOffset();
        }
        return putNullBin();
    }

    @Override
    default void putBlockOfBytes(long from, long len) {
        Vect.memcpy(from, appendAddressFor(len), len);
    }

    default void putBool(boolean value) {
        putByte((byte) (value ? 1 : 0));
    }

    default void putByte(byte b) {
        Unsafe.getUnsafe().putByte(appendAddressFor(Byte.BYTES), b);
    }

    default void putChar(char value) {
        Unsafe.getUnsafe().putChar(appendAddressFor(Character.BYTES), value);
    }

    default void putDouble(double value) {
        Unsafe.getUnsafe().putDouble(appendAddressFor(Double.BYTES), value);
    }

    default void putFloat(float value) {
        Unsafe.getUnsafe().putFloat(appendAddressFor(Float.BYTES), value);
    }

    default void putInt(int value) {
        Unsafe.getUnsafe().putInt(appendAddressFor(Integer.BYTES), value);
    }

    default void putLong(long value) {
        Unsafe.getUnsafe().putLong(appendAddressFor(Long.BYTES), value);
    }

    default void putLong128(long l1, long l2) {
        long addr = appendAddressFor(16);
        Unsafe.getUnsafe().putLong(addr, l1);
        Unsafe.getUnsafe().putLong(addr + 8, l2);
    }

    default void putLong256(long l0, long l1, long l2, long l3) {
        final long addr = appendAddressFor(32);
        Unsafe.getUnsafe().putLong(addr, l0);
        Unsafe.getUnsafe().putLong(addr + Long.BYTES, l1);
        Unsafe.getUnsafe().putLong(addr + Long.BYTES * 2, l2);
        Unsafe.getUnsafe().putLong(addr + Long.BYTES * 3, l3);
    }

    default void putLong256(Long256 value) {
        putLong256(
                value.getLong0(),
                value.getLong1(),
                value.getLong2(),
                value.getLong3());
    }

    default void putLong256(CharSequence hexString) {
        final int len;
        if (hexString == null || (len = hexString.length()) == 0) {
            putLong256Null();
        } else {
            putLong256(hexString, 2, len);
        }
    }

    default long putNullBin() {
        putLong(TableUtils.NULL_LEN);
        return getAppendOffset();
    }

    default long putNullStr() {
        putInt(TableUtils.NULL_LEN);
        return getAppendOffset();
    }

    default void putShort(short value) {
        Unsafe.getUnsafe().putShort(appendAddressFor(Short.BYTES), value);
    }

    default long putStr(CharSequence value) {
        return value != null ? putStrUnsafe(value, 0, value.length()) : putNullStr();
    }

    default long putStr(char value) {
        if (value != 0) {
            long addr = appendAddressFor(Integer.BYTES + Character.BYTES);
            Unsafe.getUnsafe().putInt(addr, 1);
            Unsafe.getUnsafe().putChar(addr + Integer.BYTES, value);
            return getAppendOffset();
        }
        return putNullStr();
    }

    default long putStr(CharSequence value, int pos, int len) {
        if (value != null) {
            return putStrUnsafe(value, pos, len);
        }
        return putNullStr();
    }

    default void putBool(long offset, boolean value) {
        putByte(offset, (byte) (value ? 1 : 0));
    }

    default void putByte(long offset, byte value) {
        Unsafe.getUnsafe().putByte(appendAddressFor(offset, Byte.BYTES), value);
    }

    default void putChar(long offset, char value) {
        Unsafe.getUnsafe().putChar(appendAddressFor(offset, Character.BYTES), value);
    }

    default void putDouble(long offset, double value) {
        Unsafe.getUnsafe().putDouble(appendAddressFor(offset, Double.BYTES), value);
    }

    default void putFloat(long offset, float value) {
        Unsafe.getUnsafe().putFloat(appendAddressFor(offset, Float.BYTES), value);
    }

    default void putInt(long offset, int value) {
        Unsafe.getUnsafe().putInt(appendAddressFor(offset, Integer.BYTES), value);
    }

    default void putLong(long offset, long value) {
        Unsafe.getUnsafe().putLong(appendAddressFor(offset, Long.BYTES), value);
    }

    default void putLong256(long offset, Long256 value) {
        putLong256(
                offset,
                value.getLong0(),
                value.getLong1(),
                value.getLong2(),
                value.getLong3());
    }

    default void putLong256(long offset, long l0, long l1, long l2, long l3) {
        final long addr = appendAddressFor(offset, Long256.BYTES);
        Unsafe.getUnsafe().putLong(addr, l0);
        Unsafe.getUnsafe().putLong(addr + Long.BYTES, l1);
        Unsafe.getUnsafe().putLong(addr + Long.BYTES * 2, l2);
        Unsafe.getUnsafe().putLong(addr + Long.BYTES * 3, l3);
    }

    default void putNullStr(long offset) {
        putInt(offset, TableUtils.NULL_LEN);
    }

    default void putShort(long offset, short value) {
        Unsafe.getUnsafe().putShort(appendAddressFor(offset, Short.BYTES), value);
    }

    default void putStr(long offset, CharSequence value) {
        if (value != null) {
            putStr(offset, value, 0, value.length());
        } else {
            putNullStr(offset);
        }
    }

    default void putStr(long offset, CharSequence value, int pos, int len) {
        final long addr = appendAddressFor(offset, Vm.getStorageLength(len));
        Unsafe.getUnsafe().putInt(addr, len);
        Chars.copyStrChars(value, pos, len, addr + Vm.STRING_LENGTH_BYTES);
    }

    default void putLong256(CharSequence hexString, int start, int end, Long256Acceptor acceptor) {
        try {
            Long256FromCharSequenceDecoder.decode(hexString, start, end, acceptor);
        } catch (NumericException e) {
            throw CairoException.instance(0).put("invalid long256 [hex=").put(hexString).put(']');
        }
    }

    default void putLong256Null() {
        Long256Impl.putNull(appendAddressFor(Long256.BYTES));
    }

    default long putStrUnsafe(CharSequence value, int pos, int len) {
        final long storageLen = Vm.getStorageLength(len);
        final long addr = appendAddressFor(storageLen);
        Unsafe.getUnsafe().putInt(addr, len);
        Chars.copyStrChars(value, pos, len, addr + Integer.BYTES);
        return getAppendOffset();
    }

    void replacePage(long address, long size);
}
