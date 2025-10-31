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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.vm.Vm;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.DirectByteSequenceView;
import io.questdb.std.Long256;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectString;

// contiguous readable
public interface MemoryCR extends MemoryC, MemoryR {

    long addressHi();

    default boolean checkOffsetMapped(long offset) {
        return offset <= size();
    }

    default ArrayView getArray(long offset, BorrowedArray array) {
        return ArrayTypeDriver.getPlainValue(addressOf(offset), array);
    }

    default BinarySequence getBin(long offset, DirectByteSequenceView view) {
        final long addr = addressOf(offset);
        final long len = Unsafe.getUnsafe().getLong(addr);
        if (len > -1) {
            return view.of(addr + Long.BYTES, len);
        }
        return null;
    }

    @Override
    default long getBinLen(long offset) {
        return getLong(offset);
    }

    @Override
    default boolean getBool(long offset) {
        return getByte(offset) == 1;
    }

    @Override
    default byte getByte(long offset) {
        assert addressOf(offset + Byte.BYTES) > 0;
        return Unsafe.getUnsafe().getByte(addressOf(offset));
    }

    @Override
    default char getChar(long offset) {
        assert addressOf(offset + Character.BYTES) > 0;
        return Unsafe.getUnsafe().getChar(addressOf(offset));
    }

    @Override
    default void getDecimal128(long offset, Decimal128 sink) {
        final long addr = addressOf(offset + 16L);
        sink.ofRaw(
                Unsafe.getUnsafe().getLong(addr - 16L),
                Unsafe.getUnsafe().getLong(addr - 8L)
        );
    }

    @Override
    default short getDecimal16(long offset) {
        return getShort(offset);
    }

    @Override
    default void getDecimal256(long offset, Decimal256 sink) {
        final long addr = addressOf(offset + 32L);
        sink.ofRawAddress(addr - 32L);
    }

    @Override
    default int getDecimal32(long offset) {
        return getInt(offset);
    }

    @Override
    default long getDecimal64(long offset) {
        return getLong(offset);
    }

    @Override
    default byte getDecimal8(long offset) {
        return getByte(offset);
    }

    @Override
    default double getDouble(long offset) {
        assert addressOf(offset + Double.BYTES) > 0;
        return Unsafe.getUnsafe().getDouble(addressOf(offset));
    }

    long getFd();

    @Override
    default float getFloat(long offset) {
        assert addressOf(offset + Float.BYTES) > 0;
        return Unsafe.getUnsafe().getFloat(addressOf(offset));
    }

    @Override
    default int getIPv4(long offset) {
        return getInt(offset);
    }

    @Override
    default int getInt(long offset) {
        assert addressOf(offset + Integer.BYTES) > 0;
        return Unsafe.getUnsafe().getInt(addressOf(offset));
    }

    @Override
    default long getLong(long offset) {
        assert offset > -1 && addressOf(offset + Long.BYTES) > 0;
        return Unsafe.getUnsafe().getLong(addressOf(offset));
    }

    @Override
    default void getLong256(long offset, CharSink<?> sink) {
        final long addr = addressOf(offset + Long256.BYTES);
        Numbers.appendLong256FromUnsafe(addr - Long256.BYTES, sink);
    }

    @Override
    default long getPageSize() {
        return size();
    }

    @Override
    default short getShort(long offset) {
        return Unsafe.getUnsafe().getShort(addressOf(offset));
    }

    default DirectString getStr(long offset, DirectString view) {
        long addr = addressOf(offset);
        assert addr > 0;
        if (!checkOffsetMapped(offset + 4)) {
            throw CairoException.critical(0)
                    .put("string is outside of file boundary [offset=")
                    .put(offset)
                    .put(", size=")
                    .put(size())
                    .put(']');
        }

        final int len = Unsafe.getUnsafe().getInt(addr);
        if (len != TableUtils.NULL_LEN) {
            if (checkOffsetMapped(Vm.getStorageLength(len) + offset)) {
                return view.of(addr + Vm.STRING_LENGTH_BYTES, len);
            }
            throw CairoException.critical(0)
                    .put("string is outside of file boundary [offset=")
                    .put(offset)
                    .put(", len=")
                    .put(len)
                    .put(", size=")
                    .put(size())
                    .put(']');
        }
        return null;
    }

    @Override
    default int getStrLen(long offset) {
        return getInt(offset);
    }

    default boolean isFileBased() {
        return false;
    }

    default void map() {
    }
}
