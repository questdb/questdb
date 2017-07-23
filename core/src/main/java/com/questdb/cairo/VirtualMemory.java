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

package com.questdb.cairo;

import com.questdb.misc.ByteBuffers;
import com.questdb.misc.Numbers;
import com.questdb.misc.Unsafe;
import com.questdb.std.LongList;
import com.questdb.std.str.AbstractCharSequence;
import sun.nio.ch.DirectBuffer;

import java.io.Closeable;
import java.nio.ByteBuffer;

public class VirtualMemory implements Closeable {
    private static final int STRING_LENGTH_BYTES = 4;
    protected final LongList pages = new LongList(4, 0);
    protected long pageSize;
    private ByteSequenceView bsview;
    private CharSequenceView csview;
    private int bits;
    private long mod;
    private long appendPointer = -1;
    private long pageHi = -1;
    private long baseOffset = 1;
    private long roOffsetLo = 0;
    private long roOffsetHi = 0;
    private long absolutePointer;
    private ViewSupplier<CharSequenceView> csViewSupplier;
    private ViewSupplier<ByteSequenceView> bsViewSupplier;

    public VirtualMemory(long pageSize) {
        this();
        setPageSize(pageSize);
    }

    protected VirtualMemory() {
        csViewSupplier = VirtualMemory::createCharSequenceView;
        bsViewSupplier = VirtualMemory::createByteSequenceView;
    }

    public static int getStorageLength(CharSequence s) {
        if (s == null) {
            return STRING_LENGTH_BYTES;
        }

        return STRING_LENGTH_BYTES + s.length() * 2;
    }

    public long addressOf(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi) {
            return absolutePointer + offset;
        }
        return addressOf0(offset);
    }

    public void clearHotPage() {
        roOffsetLo = roOffsetHi = 0;
    }

    @Override
    public void close() {
        int n = pages.size() - 1;
        if (n > -1) {
            for (int i = 0; i < n; i++) {
                release(pages.getQuick(i));
            }
            releaseLast(pages.getQuick(n));
        }
        pages.clear();
        appendPointer = -1;
        pageHi = -1;
        baseOffset = 1;
        clearHotPage();
    }

    public final long getAppendOffset() {
        return baseOffset + appendPointer;
    }

    public final ByteSequenceView getBin(long offset) {
        final long len = getLong(offset);
        if (len == -1) {
            return null;
        }
        return getByteSequenceView().of(offset + 8, len);
    }

    public final byte getByte(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi - 1) {
            return Unsafe.getUnsafe().getByte(absolutePointer + offset);
        }
        return getByte0(offset);
    }

    public final double getDouble(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi - 8) {
            return Unsafe.getUnsafe().getDouble(absolutePointer + offset);
        }
        return getDouble0(offset);
    }

    public final float getFloat(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi - 4) {
            return Unsafe.getUnsafe().getFloat(absolutePointer + offset);
        }
        return getFloat0(offset);
    }

    public final int getInt(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi - 4) {
            return Unsafe.getUnsafe().getInt(absolutePointer + offset);
        }
        return getInt0(offset);
    }

    public long getLong(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi - 8) {
            return Unsafe.getUnsafe().getLong(absolutePointer + offset);
        }
        return getLong0(offset);
    }

    public final short getShort(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi - 2) {
            return Unsafe.getUnsafe().getShort(absolutePointer + offset);
        }
        return getShort0(offset);
    }

    public final CharSequence getStr(long offset) {
        final int len = getInt(offset);
        if (len == -1) {
            return null;
        }
        return getCharSequenceView().of(offset + STRING_LENGTH_BYTES, len);
    }

    /**
     * Updates append pointer with address for the given offset. All put* functions will be
     * appending from this offset onwards effectively overwriting data. Size of virtual memory remains
     * unaffected until the moment memory has to be extended.
     *
     * @param offset position from 0 in virtual memory.
     */
    public void jumpTo(long offset) {
        assert offset >= 0;
        final long p = offset - baseOffset;
        if (p >= pageHi - pageSize && p < pageHi) {
            appendPointer = p;
        } else {
            jumpTo0(offset);
        }
    }

    public long pageRemaining(long offset) {
        return pageSize - pageOffset(offset);
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
        final long offset = getAppendOffset();
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
            pageAt(getAppendOffset() + 1);
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

    public final long putNullBin() {
        final long offset = getAppendOffset();
        putLong(-1);
        return offset;
    }

    public final long putNullStr() {
        final long offset = getAppendOffset();
        putInt(-1);
        return offset;
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
        final long offset = getAppendOffset();
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

    /**
     * Skips given number of bytes. Same as logically appending 0-bytes. Advantage of this method is that
     * no memory write takes place.
     *
     * @param bytes number of bytes to skip
     */
    public void skip(long bytes) {
        assert bytes > 0;
        if (pageHi - appendPointer > bytes) {
            appendPointer += bytes;
        } else {
            skip0(bytes);
        }
    }

    private static void copyStrChars(CharSequence value, int pos, int len, long address) {
        for (int i = 0; i < len; i++) {
            char c = value.charAt(i + pos);
            Unsafe.getUnsafe().putChar(address + 2 * i, c);
        }
    }

    private long addressOf0(long offset) {
        return computeHotPage(pageIndex(offset)) + pageOffset(offset);
    }

    protected long allocateNextPage(int page) {
        return Unsafe.getUnsafe().allocateMemory(pageSize);
    }

    protected void cachePageAddress(int index, long address) {
        pages.extendAndSet(index, address);
    }

    /**
     * Computes boundaries of read-only memory page to enable fast-path check of offsets
     */
    private long computeHotPage(int page) {
        long pageAddress = getPageAddress(page);
        roOffsetLo = pageOffset(page) - 1;
        roOffsetHi = roOffsetLo + pageSize + 1;
        absolutePointer = pageAddress - roOffsetLo - 1;
        return pageAddress;
    }

    private void copyBufBytes(ByteBuffer buf, int pos, int len) {
        for (int i = 0; i < len; i++) {
            byte c = buf.get(pos + i);
            Unsafe.getUnsafe().putByte(appendPointer + i, c);
        }
    }

    private ByteSequenceView createByteSequenceView() {
        bsViewSupplier = (mem) -> mem.bsview;
        return bsview = new ByteSequenceView();
    }

    private CharSequenceView createCharSequenceView() {
        csViewSupplier = (mem) -> mem.csview;
        return csview = new CharSequenceView();
    }

    private byte getByte0(long offset) {
        return Unsafe.getUnsafe().getByte(computeHotPage(pageIndex(offset)) + pageOffset(offset));
    }

    private ByteSequenceView getByteSequenceView() {
        return bsViewSupplier.get(this);
    }

    private CharSequenceView getCharSequenceView() {
        return csViewSupplier.get(this);
    }

    private double getDouble0(long offset) {
        int page = pageIndex(offset);
        long pageOffset = pageOffset(offset);

        if (pageSize - pageOffset > 7) {
            return Unsafe.getUnsafe().getDouble(computeHotPage(page) + pageOffset);
        }
        return getDoubleBytes(page, pageOffset);
    }

    double getDoubleBytes(int page, long pageOffset) {
        return Double.longBitsToDouble(getLongBytes(page, pageOffset));
    }

    private float getFloat0(long offset) {
        int page = pageIndex(offset);
        long pageOffset = pageOffset(offset);

        if (pageSize - pageOffset > 3) {
            return Unsafe.getUnsafe().getFloat(computeHotPage(page) + pageOffset);
        }
        return getFloatBytes(page, pageOffset);
    }

    float getFloatBytes(int page, long pageOffset) {
        return Float.intBitsToFloat(getIntBytes(page, pageOffset));
    }

    private int getInt0(long offset) {
        int page = pageIndex(offset);
        long pageOffset = pageOffset(offset);

        if (pageSize - pageOffset > 3) {
            return Unsafe.getUnsafe().getInt(computeHotPage(page) + pageOffset);
        }
        return getIntBytes(page, pageOffset);
    }

    int getIntBytes(int page, long pageOffset) {
        int value = 0;
        long pageAddress = getPageAddress(page);

        for (int i = 0; i < 4; i++) {
            if (pageOffset == pageSize) {
                pageAddress = getPageAddress(++page);
                pageOffset = 0;
            }
            int b = Unsafe.getUnsafe().getByte(pageAddress + pageOffset++) & 0xff;
            value = (b << (8 * i)) | value;
        }
        return value;
    }

    private long getLong0(long offset) {
        int page = pageIndex(offset);
        long pageOffset = pageOffset(offset);

        if (pageSize - pageOffset > 7) {
            return Unsafe.getUnsafe().getLong(computeHotPage(page) + pageOffset);
        }
        return getLongBytes(page, pageOffset);
    }

    long getLongBytes(int page, long pageOffset) {
        long value = 0;
        long pageAddress = getPageAddress(page);

        for (int i = 0; i < 8; i++) {
            if (pageOffset == pageSize) {
                pageAddress = getPageAddress(++page);
                pageOffset = 0;
            }
            long b = Unsafe.getUnsafe().getByte(pageAddress + pageOffset++) & 0xff;
            value = (b << (8 * i)) | value;
        }
        return value;
    }

    /**
     * Provides address of page for read operations. Memory writes never call this.
     */
    protected long getPageAddress(int page) {
        return pages.getQuick(page);
    }

    private short getShort0(long offset) {
        int page = pageIndex(offset);
        long pageOffset = pageOffset(offset);

        if (pageSize - pageOffset > 1) {
            return Unsafe.getUnsafe().getShort(computeHotPage(page) + pageOffset);
        }

        return getShortBytes(page, pageOffset);
    }

    short getShortBytes(int page, long pageOffset) {
        short value = 0;
        long pageAddress = getPageAddress(page);

        for (int i = 0; i < 2; i++) {
            if (pageOffset == pageSize) {
                pageAddress = getPageAddress(++page);
                pageOffset = 0;
            }
            short b = (short) (Unsafe.getUnsafe().getByte(pageAddress + pageOffset++) & 0xff);
            value = (short) ((b << (8 * i)) | value);
        }

        return value;
    }

    private void jumpTo0(long offset) {
        final int page = pageIndex(offset);
        updateLimits(page, mapWritePage(page));
        appendPointer += pageOffset(offset);
    }

    protected long mapWritePage(int page) {
        long address;
        if (page < pages.size()) {
            address = pages.getQuick(page);
            if (address == 0) {
                address = allocateNextPage(page);
                cachePageAddress(page, address);
            }
        } else {
            address = allocateNextPage(page);
            cachePageAddress(page, address);
        }
        return address;
    }

    private void pageAt(long offset) {
        int page = pageIndex(offset);
        updateLimits(page, mapWritePage(page));
    }

    protected final int pageIndex(long offset) {
        return (int) (offset >> bits);
    }

    protected final long pageOffset(int page) {
        return ((long) page << bits);
    }

    private long pageOffset(long offset) {
        return offset & mod;
    }

    private long putBin0(ByteBuffer buf) {
        final long offset = getAppendOffset();

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
            pageAt(getAppendOffset() + half);  // +1?
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
            pageAt(getAppendOffset() + half); // +1?
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
        pageAt(baseOffset + pageHi);
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
                pageAt(getAppendOffset() + half * 2);
            }

            len -= half;
            start += half;
        } while (true);
    }

    protected void release(long address) {
        if (address != 0) {
            Unsafe.getUnsafe().freeMemory(address);
        }
    }

    protected void releaseLast(long address) {
        release(address);
    }

    protected final void setPageSize(long pageSize) {
        this.pageSize = Numbers.ceilPow2(pageSize);
        this.bits = Numbers.msb(this.pageSize);
        this.mod = this.pageSize - 1;
    }

    private void skip0(long bytes) {
        jumpTo(getAppendOffset() + bytes);
    }

    protected final void updateLimits(int page, long pageAddress) {
        pageHi = pageAddress + this.pageSize;
        baseOffset = pageOffset(page + 1) - pageHi;
        this.appendPointer = pageAddress;
    }

    @FunctionalInterface
    private interface ViewSupplier<T> {
        T get(VirtualMemory mem);
    }

    public class CharSequenceView extends AbstractCharSequence {
        private long offset;
        private int len;
        private int lastIndex;
        private int page;
        private long pageAddress;
        private long pageOffset;

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

        CharSequenceView of(long offset, int len) {
            this.offset = offset;
            this.len = len;
            this.lastIndex = -1;
            this.page = pageIndex(offset);
            this.pageAddress = getPageAddress(page);
            this.pageOffset = pageOffset(offset);
            return this;
        }

        private char updatePosAndGet(int index) {
            char c;
            long offset = this.offset + index * 2;
            page = pageIndex(offset);
            pageAddress = getPageAddress(page);
            pageOffset = pageOffset(offset);

            if (pageSize - pageOffset > 1) {
                c = Unsafe.getUnsafe().getChar(pageAddress + pageOffset);
                pageOffset += 2;
            } else {
                c = (char) (Unsafe.getUnsafe().getByte(pageAddress + pageOffset) << 8);
                pageAddress = getPageAddress(++page);
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
        private long pageOffset;

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

        ByteSequenceView of(long offset, long len) {
            this.offset = offset;
            this.len = len;
            this.lastIndex = -1;
            this.page = pageIndex(offset);
            this.pageAddress = getPageAddress(page);
            this.pageOffset = pageOffset(offset);
            return this;
        }

        private byte updatePosAndGet(long index) {
            byte c;
            long offset = this.offset + index;
            page = pageIndex(offset);
            pageAddress = getPageAddress(page);
            pageOffset = pageOffset(offset);
            c = Unsafe.getUnsafe().getByte(pageAddress + pageOffset);
            pageOffset++;
            return c;
        }
    }
}