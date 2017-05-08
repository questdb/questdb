/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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
 ******************************************************************************/

package com.questdb.std;

import com.questdb.misc.ByteBuffers;
import com.questdb.misc.Numbers;
import com.questdb.misc.Unsafe;
import com.questdb.std.str.AbstractCharSequence;
import sun.nio.ch.DirectBuffer;

import java.io.Closeable;
import java.nio.ByteBuffer;

public class VirtualMemory implements Closeable {
    private final LongList pages = new LongList();
    private final int pageSize;
    private final int bits;
    private final int mod;
    private final CharSequenceView csview = new CharSequenceView();
    private final ByteSequenceView bsview = new ByteSequenceView();
    private long appendPointer = 0;
    private long pageHi = 0;
    private long roOffsetLo = 0;
    private long roOffsetHi = 0;
    private long baseOffset = 0;
    private long roPtr;

    public VirtualMemory(int pageSize) {
        this.pageSize = Numbers.ceilPow2(pageSize);
        this.bits = Numbers.msb(this.pageSize);
        this.mod = this.pageSize - 1;
    }

    @Override
    public void close() {
        for (int i = 0, n = pages.size(); i < n; i++) {
            release(pages.getQuick(i));
        }
    }

    public final ByteSequenceView getBin(long offset) {
        final long len = getLong(offset);
        if (len == -1) {
            return null;
        }

        bsview.of(offset + 8, len);
        return bsview;
    }

    public final byte getByte(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi - 1) {
            return Unsafe.getUnsafe().getByte(roPtr + offset);
        }
        return getByte0(offset);
    }

    public final double getDouble(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi - 8) {
            return Unsafe.getUnsafe().getDouble(roPtr + offset);
        }
        return getDouble0(offset);
    }

    public final float getFloat(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi - 4) {
            return Unsafe.getUnsafe().getFloat(roPtr + offset);
        }
        return getFloat0(offset);
    }

    public final int getInt(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi - 4) {
            return Unsafe.getUnsafe().getInt(roPtr + offset);
        }
        return getInt0(offset);
    }

    public long getLong(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi - 8) {
            return Unsafe.getUnsafe().getLong(roPtr + offset);
        }
        return getLong0(offset);
    }

    public final short getShort(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi - 2) {
            return Unsafe.getUnsafe().getShort(roPtr + offset);
        }
        return getShort0(offset);
    }

    public final CharSequence getStr(long offset) {
        final int len = getInt(offset);
        if (len == -1) {
            return null;
        }

        csview.of(offset + 4, len);
        return csview;
    }

    public final long putBin(ByteBuffer buf) {
        if (buf instanceof DirectBuffer) {
            int pos = buf.position();
            int len = buf.remaining();
            buf.position(pos + len);
            return putBin(ByteBuffers.getAddress(buf) + pos, len);
        }
        return putBin0(buf);
    }

    public final long putBin(long from, long len) {
        final long offset = baseOffset + appendPointer;
        putLong(len > 0 ? len : -1);
        if (len < 1) {
            return offset;
        }

        if (len < pageHi - appendPointer) {
            Unsafe.getUnsafe().copyMemory(from, appendPointer, len);
            appendPointer += len;
        } else {
            putBinSlit(from, len);
        }

        return offset;
    }

    public void putByte(byte b) {
        if (pageHi == appendPointer) {
            nextPage();
        }
        Unsafe.getUnsafe().putByte(appendPointer++, b);
    }

    public final void putDouble(double value) {
        if (pageHi - appendPointer > 7) {
            Unsafe.getUnsafe().putDouble(appendPointer, value);
            appendPointer += 8;
        } else {
            putDoubleBytes(value);
        }
    }

    public final void putFloat(float value) {
        if (pageHi - appendPointer > 3) {
            Unsafe.getUnsafe().putFloat(appendPointer, value);
            appendPointer += 4;
        } else {
            putFloatBytes(value);
        }
    }

    public final void putInt(int value) {
        if (pageHi - appendPointer > 3) {
            Unsafe.getUnsafe().putInt(appendPointer, value);
            appendPointer += 4;
        } else {
            putIntBytes(value);
        }
    }

    public final void putLong(long value) {
        if (pageHi - appendPointer > 7) {
            Unsafe.getUnsafe().putLong(appendPointer, value);
            appendPointer += 8;
        } else {
            putLongBytes(value);
        }
    }

    public final void putShort(short value) {
        if (pageHi - appendPointer > 1) {
            Unsafe.getUnsafe().putShort(appendPointer, value);
            appendPointer += 2;
        } else {
            putShortBytes(value);
        }
    }

    public final long putStr(CharSequence value) {
        final long offset = baseOffset + appendPointer;
        if (value == null) {
            putInt(-1);
            return offset;
        }

        int l = value.length();
        putInt(l);

        if (pageHi - appendPointer < l * 2) {
            putStrSplit(value, l);
        } else {
            copyStrChars(value, 0, l, appendPointer);
            appendPointer += l * 2;
        }

        return offset;
    }

    private static void copyStrChars(CharSequence value, int pos, int len, long address) {
        for (int i = 0; i < len; i++) {
            char c = value.charAt(i + pos);
            Unsafe.getUnsafe().putChar(address + 2 * i, c);
        }
    }

    protected long allocate(int pageSize) {
        return Unsafe.getUnsafe().allocateMemory(pageSize);
    }

    private void computeHotPage(int page) {
        roOffsetLo = (page << bits) - 1;
        roOffsetHi = roOffsetLo + pageSize + 1;
        roPtr = pages.getQuick(page) - roOffsetLo - 1;
    }

    private void copyBufBytes(ByteBuffer buf, int pos, int len) {
        for (int i = 0; i < len; i++) {
            byte c = buf.get(pos + i);
            Unsafe.getUnsafe().putByte(appendPointer + i, c);
        }
    }

    private byte getByte0(long offset) {
        int page = pageIndex(offset);
        int pageOffset = pageOffset(offset);
        computeHotPage(page);
        return Unsafe.getUnsafe().getByte(pages.getQuick(page) + pageOffset);
    }

    private double getDouble0(long offset) {
        int page = pageIndex(offset);
        int pageOffset = pageOffset(offset);
        computeHotPage(page);

        if (pageSize - pageOffset > 7) {
            return Unsafe.getUnsafe().getDouble(pages.getQuick(page) + pageOffset);
        }
        return getDoubleBytes(page, pageOffset);
    }

    double getDoubleBytes(int page, int pageOffset) {
        return Double.longBitsToDouble(getLongBytes(page, pageOffset));
    }

    private float getFloat0(long offset) {
        int page = pageIndex(offset);
        int pageOffset = pageOffset(offset);
        computeHotPage(page);

        if (pageSize - pageOffset > 3) {
            return Unsafe.getUnsafe().getFloat(pages.getQuick(page) + pageOffset);
        }
        return getFloatBytes(page, pageOffset);
    }

    float getFloatBytes(int page, int pageOffset) {
        return Float.intBitsToFloat(getIntBytes(page, pageOffset));
    }

    private int getInt0(long offset) {
        int page = pageIndex(offset);
        int pageOffset = pageOffset(offset);
        computeHotPage(page);

        if (pageSize - pageOffset > 3) {
            return Unsafe.getUnsafe().getInt(pages.getQuick(page) + pageOffset);
        }
        return getIntBytes(page, pageOffset);
    }

    int getIntBytes(int page, int pageOffset) {
        int value = 0;
        long pageAddress = pages.getQuick(page);

        for (int i = 0; i < 4; i++) {
            if (pageOffset == pageSize) {
                pageAddress = pages.getQuick(++page);
                pageOffset = 0;
            }
            int b = Unsafe.getUnsafe().getByte(pageAddress + pageOffset++) & 0xff;
            value = (b << (8 * i)) | value;
        }
        return value;
    }

    private long getLong0(long offset) {
        int page = pageIndex(offset);
        int pageOffset = pageOffset(offset);
        computeHotPage(page);

        if (pageSize - pageOffset > 7) {
            Unsafe.getUnsafe().getLong(pages.getQuick(page) + pageOffset);
        }
        return getLongBytes(page, pageOffset);
    }

    long getLongBytes(int page, int pageOffset) {
        long value = 0;
        long pageAddress = pages.getQuick(page);

        for (int i = 0; i < 8; i++) {
            if (pageOffset == pageSize) {
                pageAddress = pages.getQuick(++page);
                pageOffset = 0;
            }
            long b = Unsafe.getUnsafe().getByte(pageAddress + pageOffset++) & 0xff;
            value = (b << (8 * i)) | value;
        }
        return value;
    }

    private short getShort0(long offset) {
        int page = pageIndex(offset);
        int pageOffset = pageOffset(offset);
        computeHotPage(page);

        if (pageSize - pageOffset > 1) {
            return Unsafe.getUnsafe().getShort(pages.getQuick(page) + pageOffset);
        }

        return getShortBytes(page, pageOffset);
    }

    short getShortBytes(int page, int pageOffset) {
        short value = 0;
        long pageAddress = pages.getQuick(page);

        for (int i = 0; i < 2; i++) {
            if (pageOffset == pageSize) {
                pageAddress = pages.getQuick(++page);
                pageOffset = 0;
            }
            short b = (short) (Unsafe.getUnsafe().getByte(pageAddress + pageOffset++) & 0xff);
            value = (short) ((b << (8 * i)) | value);
        }

        return value;
    }

    private void nextPage() {
        appendPointer = allocate(pageSize);
        pageHi = appendPointer + pageSize;
        pages.add(appendPointer);
        baseOffset = (pages.size() << bits) - pageHi;
    }

    private int pageIndex(long offset) {
        return (int) (offset >> bits);
    }

    private int pageOffset(long offset) {
        return (int) (offset & mod);
    }

    private long putBin0(ByteBuffer buf) {
        final long offset = baseOffset + appendPointer;

        if (buf == null) {
            putLong(-1);
            return offset;
        }

        int pos = buf.position();
        int len = buf.remaining();
        buf.position(pos + len);

        putLong(len);

        if (len < pageHi - appendPointer) {
            copyBufBytes(buf, pos, len);
            appendPointer += len;
        } else {
            putBinSplit(buf, pos, len);
        }

        return offset;
    }

    private void putBinSlit(long start, long len) {
        do {
            int half = (int) (pageHi - appendPointer);
            if (len <= half) {
                Unsafe.getUnsafe().copyMemory(start, appendPointer, len);
                appendPointer += len;
                break;
            }

            Unsafe.getUnsafe().copyMemory(start, appendPointer, half);
            nextPage();
            len -= half;
            start += half;
        } while (true);
    }

    private void putBinSplit(ByteBuffer buf, int pos, int len) {
        int start = pos;
        do {
            int half = (int) (pageHi - appendPointer);

            if (len <= half) {
                copyBufBytes(buf, start, len);
                appendPointer += len;
                break;
            }

            copyBufBytes(buf, start, half);
            nextPage();
            len -= half;
            start += half;
        } while (true);
    }

    void putDoubleBytes(double value) {
        putLongBytes(Double.doubleToLongBits(value));
    }

    void putFloatBytes(float value) {
        putIntBytes(Float.floatToIntBits(value));
    }

    void putIntBytes(int value) {
        putByte((byte) (value & 0xff));
        putByte((byte) ((value >> 8) & 0xff));
        putByte((byte) ((value >> 16) & 0xff));
        putByte((byte) ((value >> 24) & 0xff));
    }

    void putLongBytes(long value) {
        putByte((byte) (value & 0xffL));
        putByte((byte) ((value >> 8) & 0xffL));
        putByte((byte) ((value >> 16) & 0xffL));
        putByte((byte) ((value >> 24) & 0xffL));
        putByte((byte) ((value >> 32) & 0xffL));
        putByte((byte) ((value >> 40) & 0xffL));
        putByte((byte) ((value >> 48) & 0xffL));
        putByte((byte) ((value >> 56) & 0xffL));
    }

    void putShortBytes(short value) {
        putByte((byte) (value & 0xff));
        putByte((byte) ((value >> 8) & 0xff));
    }

    private void putSplitChar(char c) {
        Unsafe.getUnsafe().putByte(pageHi - 1, (byte) (c >> 8));
        nextPage();
        Unsafe.getUnsafe().putByte(appendPointer++, (byte) c);
    }

    private void putStrSplit(CharSequence value, int len) {
        int start = 0;
        do {
            int half = (int) ((pageHi - appendPointer) / 2);

            if (len <= half) {
                copyStrChars(value, start, len, appendPointer);
                appendPointer += len * 2;
                break;
            }

            copyStrChars(value, start, half, appendPointer);

            if (half * 2 < pageHi - appendPointer) {
                putSplitChar(value.charAt(start + half++));
            } else {
                nextPage();
            }

            len -= half;
            start += half;
        } while (true);
    }

    protected void release(long address) {
        Unsafe.getUnsafe().freeMemory(address);
    }

    public class CharSequenceView extends AbstractCharSequence {
        private long offset;
        private int len;
        private int lastIndex;
        private int page;
        private long pageAddress;
        private int pageOffset;

        @Override
        public int length() {
            return len;
        }

        @Override
        public char charAt(int index) {
            char c;
            if (index == lastIndex + 1 && pageOffset < mod) {
                // sequential read
                c = Unsafe.getUnsafe().getChar(pageAddress + pageOffset);
                pageOffset += 2;
            } else {
                c = updatePosAndGet(index);
            }
            lastIndex = index;
            return c;
        }

        void of(long offset, int len) {
            this.offset = offset;
            this.len = len;
            this.lastIndex = -1;
            this.page = pageIndex(offset);
            this.pageAddress = pages.getQuick(page);
            this.pageOffset = pageOffset(offset);
        }

        private char updatePosAndGet(int index) {
            char c;
            long offset = this.offset + index * 2;
            page = pageIndex(offset);
            pageAddress = pages.getQuick(page);
            pageOffset = pageOffset(offset);

            if (pageSize - pageOffset > 1) {
                c = Unsafe.getUnsafe().getChar(pageAddress + pageOffset);
                pageOffset += 2;
            } else {
                c = (char) (Unsafe.getUnsafe().getByte(pageAddress + pageOffset) << 8);
                pageAddress = pages.getQuick(++page);
                c = (char) (c | Unsafe.getUnsafe().getByte(pageAddress));
                pageOffset = 1;
            }
            return c;
        }
    }

    public class ByteSequenceView {
        private long offset;
        private long len = -1;
        private long lastIndex = -1;
        private int page;
        private long pageAddress;
        private int pageOffset;

        public byte byteAt(long index) {
            byte c;

            if (index == lastIndex + 1 && pageOffset < pageSize) {
                c = Unsafe.getUnsafe().getByte(pageAddress + pageOffset);
                pageOffset++;
            } else {
                c = updatePosAndGet(index);
            }
            lastIndex = index;
            return c;
        }

        public long length() {
            return len;
        }

        void of(long offset, long len) {
            this.offset = offset;
            this.len = len;
            this.lastIndex = -1;
            this.page = pageIndex(offset);
            this.pageAddress = pages.getQuick(page);
            this.pageOffset = pageOffset(offset);
        }

        private byte updatePosAndGet(long index) {
            byte c;
            long offset = this.offset + index;
            page = pageIndex(offset);
            pageAddress = pages.getQuick(page);
            pageOffset = pageOffset(offset);
            c = Unsafe.getUnsafe().getByte(pageAddress + pageOffset);
            pageOffset++;
            return c;
        }
    }
}