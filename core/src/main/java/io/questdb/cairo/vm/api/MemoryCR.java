/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.std.str.AbstractCharSequence;
import io.questdb.std.str.CharSink;

//contiguous readable 
public interface MemoryCR extends MemoryC, MemoryR {
    default BinarySequence getBin(long offset, ByteSequenceView view) {
        final long addr = addressOf(offset);
        final long len = Unsafe.getUnsafe().getLong(addr);
        if (len > -1) {
            return view.of(addr + Long.BYTES, len);
        }
        return null;
    }

    default long getBinLen(long offset) {
        return getLong(offset);
    }

    default boolean getBool(long offset) {
        return getByte(offset) == 1;
    }

    default byte getByte(long offset) {
        assert addressOf(offset + Byte.BYTES) > 0;
        return Unsafe.getUnsafe().getByte(addressOf(offset));
    }

    default double getDouble(long offset) {
        assert addressOf(offset + Double.BYTES) > 0;
        return Unsafe.getUnsafe().getDouble(addressOf(offset));
    }

    default float getFloat(long offset) {
        assert addressOf(offset + Float.BYTES) > 0;
        return Unsafe.getUnsafe().getFloat(addressOf(offset));
    }

    default int getInt(long offset) {
        assert addressOf(offset + Integer.BYTES) > 0;
        return Unsafe.getUnsafe().getInt(addressOf(offset));
    }

    default long getLong(long offset) {
        assert addressOf(offset + Long.BYTES) > 0;
        return Unsafe.getUnsafe().getLong(addressOf(offset));
    }

    default long getPageSize() {
        return size();
    }

    default short getShort(long offset) {
        return Unsafe.getUnsafe().getShort(addressOf(offset));
    }

    default void getLong256(long offset, CharSink sink) {
        final long addr = addressOf(offset + Long256.BYTES);
        final long a, b, c, d;
        a = Unsafe.getUnsafe().getLong(addr - Long.BYTES * 4);
        b = Unsafe.getUnsafe().getLong(addr - Long.BYTES * 3);
        c = Unsafe.getUnsafe().getLong(addr - Long.BYTES * 2);
        d = Unsafe.getUnsafe().getLong(addr - Long.BYTES);
        Numbers.appendLong256(a, b, c, d, sink);
    }

    default char getChar(long offset) {
        assert addressOf(offset + Character.BYTES) > 0;
        return Unsafe.getUnsafe().getChar(addressOf(offset));
    }

    default int getStrLen(long offset) {
        return getInt(offset);
    }

    default CharSequence getStr(long offset, CharSequenceView view) {
        long addr = addressOf(offset);
        final int len = Unsafe.getUnsafe().getInt(addr);
        if (len != TableUtils.NULL_LEN) {
            if (len + 4 + offset <= size()) {
                return view.of(addr + Vm.STRING_LENGTH_BYTES, len);
            }
            throw CairoException.critical(0)
                    .put("String is outside of file boundary [offset=")
                    .put(offset)
                    .put(", len=")
                    .put(len)
                    .put(", size=")
                    .put(size())
                    .put(']');
        }
        return null;
    }

    class ByteSequenceView implements BinarySequence {
        private long address;
        private long len = -1;

        @Override
        public byte byteAt(long index) {
            return Unsafe.getUnsafe().getByte(address + index);
        }

        @Override
        public void copyTo(long address, final long start, final long length) {
            long bytesRemaining = Math.min(length, this.len - start);
            long addr = this.address + start;
            Vect.memcpy(address, addr, bytesRemaining);
        }

        @Override
        public long length() {
            return len;
        }

        public ByteSequenceView of(long address, long len) {
            this.address = address;
            this.len = len;
            return this;
        }
    }

    class CharSequenceView extends AbstractCharSequence {
        private int len;
        private long address;

        @Override
        public int length() {
            return len;
        }

        @Override
        public char charAt(int index) {
            return Unsafe.getUnsafe().getChar(address + index * 2L);
        }

        public CharSequenceView of(long address, int len) {
            this.address = address;
            this.len = len;
            return this;
        }
    }
}
