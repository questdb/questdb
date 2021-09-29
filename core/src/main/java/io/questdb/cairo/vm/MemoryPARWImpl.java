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

package io.questdb.cairo.vm;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.AbstractCharSequence;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cairo.vm.Vm.STRING_LENGTH_BYTES;

public class MemoryPARWImpl implements MemoryARW {
    private static final Log LOG = LogFactory.getLog(MemoryPARWImpl.class);
    protected final LongList pages = new LongList(4, 0);
    private final ByteSequenceView bsview = new ByteSequenceView();
    private final CharSequenceView csview = new CharSequenceView();
    private final CharSequenceView csview2 = new CharSequenceView();
    private final Long256Impl long256 = new Long256Impl();
    private final Long256Impl long256B = new Long256Impl();
    private final int maxPages;
    private final InPageLong256FromCharSequenceDecoder inPageLong256Decoder = new InPageLong256FromCharSequenceDecoder();
    private final StraddlingPageLong256FromCharSequenceDecoder straddlingPageLong256Decoder = new StraddlingPageLong256FromCharSequenceDecoder();
    private long extendSegmentSize;
    private int extendSegmentMsb;
    private long extendSegmentMod;
    private long appendPointer = -1;
    private long pageHi = -1;
    private long pageLo = -1;
    private long baseOffset = 1;
    private long roOffsetLo = 0;
    private long roOffsetHi = 0;
    private long absolutePointer;
    protected int memoryTag;

    public MemoryPARWImpl(long pageSize, int maxPages, int memoryTag) {
        setExtendSegmentSize(pageSize);
        this.maxPages = maxPages;
        this.memoryTag = memoryTag;
    }

    protected MemoryPARWImpl() {
        maxPages = Integer.MAX_VALUE;
        memoryTag = MemoryTag.NATIVE_DEFAULT;
    }

    @Override
    public long appendAddressFor(long bytes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        // clear releases all pages but first
        clear();
        int n = pages.size();
        if (n > 0) {
            release(pages.getQuick(0));
            pages.setQuick(0, 0);
            pages.clear();
        }
    }

    public final long getAppendOffset() {
        return baseOffset + appendPointer;
    }

    public long getExtendSegmentSize() {
        return extendSegmentSize;
    }

    /**
     * Updates append pointer with address for the given offset. All put* functions will be
     * appending from this offset onwards effectively overwriting data. Size of virtual memory remains
     * unaffected until the moment memory has to be extended.
     *
     * @param offset position from 0 in virtual memory.
     */
    @Override
    public void jumpTo(long offset) {
        assert offset > -1;
        final long p = offset - baseOffset;
        if (p > pageLo && p < pageHi) {
            appendPointer = p;
        } else {
            jumpTo0(offset);
        }
    }

    @Override
    public final long putBin(BinarySequence value) {
        if (value == null) {
            putLong(TableUtils.NULL_LEN);
        } else {
            final long len = value.length();
            putLong(len);
            final long remaining = pageHi - appendPointer;
            if (len < remaining) {
                putBinSequence(value, 0, len);
                appendPointer += len;
            } else {
                putBin0(value, len, remaining);
            }
        }
        return getAppendOffset();
    }

    @Override
    public final long putBin(long from, long len) {
        putLong(len > 0 ? len : TableUtils.NULL_LEN);
        if (len < 1) {
            return getAppendOffset();
        }

        if (len < pageHi - appendPointer) {
            Vect.memcpy(from, appendPointer, len);
            appendPointer += len;
        } else {
            putBinSlit(from, len);
        }

        return getAppendOffset();
    }

    @Override
    public final void putBlockOfBytes(long from, long len) {
        if (len < pageHi - appendPointer) {
            Vect.memcpy(from, appendPointer, len);
            appendPointer += len;
        } else {
            putBinSlit(from, len);
        }
    }

    @Override
    public void putBool(boolean value) {
        putByte((byte) (value ? 1 : 0));
    }

    @Override
    public void putByte(byte b) {
        if (pageHi == appendPointer) {
            pageAt(getAppendOffset() + 1);
        }
        Unsafe.getUnsafe().putByte(appendPointer++, b);
    }

    @Override
    public final void putChar(char value) {
        if (pageHi - appendPointer > 1) {
            Unsafe.getUnsafe().putChar(appendPointer, value);
            appendPointer += 2;
        } else {
            putCharBytes(value);
        }
    }

    @Override
    public final void putDouble(double value) {
        if (pageHi - appendPointer > 7) {
            Unsafe.getUnsafe().putDouble(appendPointer, value);
            appendPointer += 8;
        } else {
            putDoubleBytes(value);
        }
    }

    @Override
    public final void putFloat(float value) {
        if (pageHi - appendPointer > 3) {
            Unsafe.getUnsafe().putFloat(appendPointer, value);
            appendPointer += 4;
        } else {
            putFloatBytes(value);
        }
    }

    @Override
    public final void putInt(int value) {
        if (pageHi - appendPointer > 3) {
            Unsafe.getUnsafe().putInt(appendPointer, value);
            appendPointer += 4;
        } else {
            putIntBytes(value);
        }
    }

    @Override
    public final void putLong(long value) {
        if (pageHi - appendPointer > 7) {
            Unsafe.getUnsafe().putLong(appendPointer, value);
            appendPointer += 8;
        } else {
            putLongBytes(value);
        }
    }

    @Override
    public final void putLong128(long l1, long l2) {
        if (pageHi - appendPointer > 15) {
            Unsafe.getUnsafe().putLong(appendPointer, l1);
            Unsafe.getUnsafe().putLong(appendPointer + Long.BYTES, l2);
            appendPointer += 16;
        } else {
            putLong(l1);
            putLong(l2);
        }
    }

    @Override
    public final void putLong256(long l0, long l1, long l2, long l3) {
        if (pageHi - appendPointer > Long256.BYTES - 1) {
            Unsafe.getUnsafe().putLong(appendPointer, l0);
            Unsafe.getUnsafe().putLong(appendPointer + Long.BYTES, l1);
            Unsafe.getUnsafe().putLong(appendPointer + Long.BYTES * 2, l2);
            Unsafe.getUnsafe().putLong(appendPointer + Long.BYTES * 3, l3);
            appendPointer += Long256.BYTES;
        } else {
            putLong(l0);
            putLong(l1);
            putLong(l2);
            putLong(l3);
        }
    }

    @Override
    public final void putLong256(Long256 value) {
        putLong256(
                value.getLong0(),
                value.getLong1(),
                value.getLong2(),
                value.getLong3()
        );
    }

    @Override
    public final void putLong256(CharSequence hexString) {
        if (pageHi - appendPointer < 4 * Long.BYTES) {
            straddlingPageLong256Decoder.putLong256(hexString);
        } else {
            inPageLong256Decoder.putLong256(hexString);
        }
    }

    @Override
    public final void putLong256(@NotNull CharSequence hexString, int start, int end) {
        if (pageHi - appendPointer < 4 * Long.BYTES) {
            straddlingPageLong256Decoder.putLong256(hexString, start, end);
        } else {
            inPageLong256Decoder.putLong256(hexString, start, end);
        }
    }

    @Override
    public final long putNullBin() {
        putLong(TableUtils.NULL_LEN);
        return getAppendOffset();
    }

    @Override
    public final long putNullStr() {
        putInt(TableUtils.NULL_LEN);
        return getAppendOffset();
    }

    @Override
    public final void putShort(short value) {
        if (pageHi - appendPointer > 1) {
            Unsafe.getUnsafe().putShort(appendPointer, value);
            appendPointer += 2;
        } else {
            putShortBytes(value);
        }
    }

    @Override
    public final long putStr(CharSequence value) {
        return value == null ? putNullStr() : putStr0(value, 0, value.length());
    }

    @Override
    public final long putStr(char value) {
        if (value == 0) {
            return putNullStr();
        } else {
            putInt(1);
            if (pageHi - appendPointer < Character.BYTES) {
                putSplitChar(value);
            } else {
                Unsafe.getUnsafe().putChar(appendPointer, value);
                appendPointer += Character.BYTES;
            }
            return getAppendOffset();
        }
    }

    @Override
    public final long putStr(CharSequence value, int pos, int len) {
        if (value == null) {
            return putNullStr();
        }
        return putStr0(value, pos, len);
    }

    /**
     * Skips given number of bytes. Same as logically appending 0-bytes. Advantage of this method is that
     * no memory write takes place.
     *
     * @param bytes number of bytes to skip
     */
    public void skip(long bytes) {
        assert bytes >= 0;
        if (pageHi - appendPointer > bytes) {
            appendPointer += bytes;
        } else {
            skip0(bytes);
        }
    }

    @Override
    public void truncate() {
        clear();
    }

    protected final void setExtendSegmentSize(long extendSegmentSize) {
        clear();
        this.extendSegmentSize = Numbers.ceilPow2(extendSegmentSize);
        this.extendSegmentMsb = Numbers.msb(this.extendSegmentSize);
        this.extendSegmentMod = this.extendSegmentSize - 1;
    }

    @Override
    public long appendAddressFor(long offset, long bytes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putBlockOfBytes(long offset, long from, long len) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putBool(long offset, boolean value) {
        putByte(offset, (byte) (value ? 1 : 0));
    }

    @Override
    public final void putByte(long offset, byte value) {
        if (roOffsetLo < offset && offset < roOffsetHi - 1) {
            Unsafe.getUnsafe().putByte(absolutePointer + offset, value);
        } else {
            putByteRnd(offset, value);
        }
    }

    @Override
    public void putChar(long offset, char value) {
        if (roOffsetLo < offset && offset < roOffsetHi - 2) {
            Unsafe.getUnsafe().putChar(absolutePointer + offset, value);
        } else {
            putCharBytes(offset, value);
        }
    }

    @Override
    public void putDouble(long offset, double value) {
        if (roOffsetLo < offset && offset < roOffsetHi - 8) {
            Unsafe.getUnsafe().putDouble(absolutePointer + offset, value);
        } else {
            putDoubleBytes(offset, value);
        }
    }

    @Override
    public void putFloat(long offset, float value) {
        if (roOffsetLo < offset && offset < roOffsetHi - 4) {
            Unsafe.getUnsafe().putFloat(absolutePointer + offset, value);
        } else {
            putFloatBytes(offset, value);
        }
    }

    @Override
    public void putInt(long offset, int value) {
        if (roOffsetLo < offset && offset < roOffsetHi - Integer.BYTES) {
            Unsafe.getUnsafe().putInt(absolutePointer + offset, value);
        } else {
            putIntBytes(offset, value);
        }
    }

    @Override
    public void putLong(long offset, long value) {
        if (roOffsetLo < offset && offset < roOffsetHi - 8) {
            Unsafe.getUnsafe().putLong(absolutePointer + offset, value);
        } else {
            putLongBytes(offset, value);
        }
    }

    @Override
    public void putLong256(long offset, Long256 value) {
        putLong256(
                offset,
                value.getLong0(),
                value.getLong1(),
                value.getLong2(),
                value.getLong3()
        );
    }

    @Override
    public void putLong256(long offset, long l0, long l1, long l2, long l3) {
        if (roOffsetLo < offset && offset < roOffsetHi - Long256.BYTES) {
            Unsafe.getUnsafe().putLong(absolutePointer + offset, l0);
            Unsafe.getUnsafe().putLong(absolutePointer + offset + Long.BYTES, l1);
            Unsafe.getUnsafe().putLong(absolutePointer + offset + Long.BYTES * 2, l2);
            Unsafe.getUnsafe().putLong(absolutePointer + offset + Long.BYTES * 3, l3);
        } else {
            putLong(offset, l0);
            putLong(offset + Long.BYTES, l1);
            putLong(offset + Long.BYTES * 2, l2);
            putLong(offset + Long.BYTES * 3, l3);
        }
    }

    @Override
    public final void putNullStr(long offset) {
        putInt(offset, TableUtils.NULL_LEN);
    }

    @Override
    public void putShort(long offset, short value) {
        if (roOffsetLo < offset && offset < roOffsetHi - 2) {
            Unsafe.getUnsafe().putShort(absolutePointer + offset, value);
        } else {
            putShortBytes(offset, value);
        }
    }

    @Override
    public void putStr(long offset, CharSequence value) {
        if (value == null) {
            putNullStr(offset);
        } else {
            putStr(offset, value, 0, value.length());
        }
    }

    @Override
    public void putStr(long offset, CharSequence value, int pos, int len) {
        putInt(offset, len);
        if (roOffsetLo < offset && offset < roOffsetHi - len * 2L - 4) {
            copyStrChars(value, pos, len, absolutePointer + offset + 4);
        } else {
            putStrSplit(offset + 4, value, pos, len);
        }
    }

    @Override
    public void zero() {
        throw new UnsupportedOperationException();
    }

    public void clear() {
        releaseAllPagesButFirst();
        appendPointer = -1;
        pageHi = -1;
        pageLo = -1;
        baseOffset = 1;
        clearHotPage();
    }

    public void clearHotPage() {
        roOffsetLo = roOffsetHi = 0;
    }

    public void copyTo(long address, long offset, long len) {
        final long pageSize = getPageSize();
        while (len > 0) {
            final int page = pageIndex(offset);
            final long pageAddress = getPageAddress(page);
            assert pageAddress > 0;
            final long offsetInPage = offsetInPage(offset);
            final long bytesToCopy = Math.min(len, pageSize - offsetInPage);
            Vect.memcpy(pageAddress + offsetInPage, address, bytesToCopy);
            len -= bytesToCopy;
            offset += bytesToCopy;
            address += bytesToCopy;
        }
    }

    public final BinarySequence getBin(long offset) {
        final long len = getLong(offset);
        if (len == -1) {
            return null;
        }
        return bsview.of(offset + 8, len);
    }

    public final long getBinLen(long offset) {
        return getLong(offset);
    }

    public boolean getBool(long offset) {
        return getByte(offset) == 1;
    }

    public final byte getByte(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi - 1) {
            return Unsafe.getUnsafe().getByte(absolutePointer + offset);
        }
        return getByte0(offset);
    }

    public final double getDouble(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi - Double.BYTES) {
            return Unsafe.getUnsafe().getDouble(absolutePointer + offset);
        }
        return getDouble0(offset);
    }

    public final float getFloat(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi - Float.BYTES) {
            return Unsafe.getUnsafe().getFloat(absolutePointer + offset);
        }
        return getFloat0(offset);
    }

    public final int getInt(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi - Integer.BYTES) {
            return Unsafe.getUnsafe().getInt(absolutePointer + offset);
        }
        return getInt0(offset);
    }

    public long getLong(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi - Long.BYTES) {
            return Unsafe.getUnsafe().getLong(absolutePointer + offset);
        }
        return getLong0(offset);
    }

    /**
     * Provides address of page for read operations. Memory writes never call this.
     *
     * @param page page index, starting from 0
     * @return native address of page
     */
    public long getPageAddress(int page) {
        if (page < pages.size()) {
            return pages.getQuick(page);
        }
        return 0L;
    }

    public int getPageCount() {
        return pages.size();
    }

    public long getPageSize() {
        return getExtendSegmentSize();
    }

    public final short getShort(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi - Short.BYTES) {
            return Unsafe.getUnsafe().getShort(absolutePointer + offset);
        }
        return getShort0(offset);
    }

    public final CharSequence getStr(long offset) {
        return getStr0(offset, csview);
    }

    public final CharSequence getStr2(long offset) {
        return getStr0(offset, csview2);
    }

    public Long256 getLong256A(long offset) {
        getLong256(offset, long256);
        return long256;
    }

    public void getLong256(long offset, CharSink sink) {
        final long a, b, c, d;
        if (roOffsetLo < offset && offset < roOffsetHi - Long256.BYTES) {
            a = Unsafe.getUnsafe().getLong(absolutePointer + offset);
            b = Unsafe.getUnsafe().getLong(absolutePointer + offset + Long.BYTES);
            c = Unsafe.getUnsafe().getLong(absolutePointer + offset + Long.BYTES * 2);
            d = Unsafe.getUnsafe().getLong(absolutePointer + offset + Long.BYTES * 3);
        } else {
            a = getLong(offset);
            b = getLong(offset + Long.BYTES);
            c = getLong(offset + Long.BYTES * 2);
            d = getLong(offset + Long.BYTES * 3);
        }
        Numbers.appendLong256(a, b, c, d, sink);
    }

    public void getLong256(long offset, Long256Acceptor sink) {
        if (roOffsetLo < offset && offset < roOffsetHi - Long256.BYTES) {
            sink.setAll(
                    Unsafe.getUnsafe().getLong(absolutePointer + offset),
                    Unsafe.getUnsafe().getLong(absolutePointer + offset + Long.BYTES),
                    Unsafe.getUnsafe().getLong(absolutePointer + offset + Long.BYTES * 2),
                    Unsafe.getUnsafe().getLong(absolutePointer + offset + Long.BYTES * 3)
            );
        } else {
            sink.setAll(
                    getLong(offset),
                    getLong(offset + Long.BYTES),
                    getLong(offset + Long.BYTES * 2),
                    getLong(offset + Long.BYTES * 3)
            );
        }
    }

    public Long256 getLong256B(long offset) {
        getLong256(offset, long256B);
        return long256B;
    }

    public final char getChar(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi - Character.BYTES) {
            return Unsafe.getUnsafe().getChar(absolutePointer + offset);
        }
        return getChar0(offset);
    }

    public final int getStrLen(long offset) {
        return getInt(offset);
    }

    @Override
    public void extend(long size) {
        assert size > 0;
        mapWritePage(pageIndex(size - 1));
    }

    @Override
    public long size() {
        return getAppendOffset();
    }

    public long addressOf(long offset) {
        if (roOffsetLo < offset && offset < roOffsetHi) {
            return absolutePointer + offset;
        }
        return addressOf0(offset);
    }

    @Override
    public long offsetInPage(long offset) {
        return offset & extendSegmentMod;
    }

    @Override
    public final int pageIndex(long offset) {
        return (int) (offset >> extendSegmentMsb);
    }

    @Override
    public long getGrownLength() {
        throw new UnsupportedOperationException();
    }

    public final CharSequence getStr0(long offset, CharSequenceView view) {
        final int len = getInt(offset);
        if (len == TableUtils.NULL_LEN) {
            return null;
        }

        if (len == 0) {
            return "";
        }

        return view.of(offset + STRING_LENGTH_BYTES, len);
    }

    public long hash(long offset, long size) {
        if (roOffsetLo < offset && offset < roOffsetHi - size) {
            long n = size - (size % 8);
            long address = absolutePointer + offset;

            long h = 179426491L;
            for (long i = 0; i < n; i += 8) {
                h = (h << 5) - h + Unsafe.getUnsafe().getLong(address + i);
            }

            for (; n < size; n++) {
                h = (h << 5) - h + Unsafe.getUnsafe().getByte(address + n);
            }
            return h;
        }

        return hash0(offset, size);
    }

    public boolean isMapped(long offset, long len) {
        int pageIndex = pageIndex(offset);
        int pageEndIndex = pageIndex(offset + len - 1);
        if (pageIndex == pageEndIndex) {
            return getPageAddress(pageIndex) > 0;
        }
        return false;
    }

    public long pageRemaining(long offset) {
        return getPageSize() - offsetInPage(offset);
    }

    private static void copyStrChars(CharSequence value, int pos, int len, long address) {
        for (int i = 0; i < len; i++) {
            char c = value.charAt(i + pos);
            Unsafe.getUnsafe().putChar(address + 2L * i, c);
        }
    }

    private long addressOf0(long offset) {
        return computeHotPage(pageIndex(offset)) + offsetInPage(offset);
    }

    protected long allocateNextPage(int page) {
        LOG.debug().$("new page [size=").$(getExtendSegmentSize()).I$();
        if (page >= maxPages) {
            throw LimitOverflowException.instance().put("Maximum number of pages (").put(maxPages).put(") breached in VirtualMemory");
        }
        return Unsafe.malloc(getExtendSegmentSize(), memoryTag);
    }

    protected long cachePageAddress(int index, long address) {
        pages.extendAndSet(index, address);
        return address;
    }

    /**
     * Computes boundaries of read-only memory page to enable fast-path check of offsets
     */
    private long computeHotPage(int page) {
        long pageAddress = getPageAddress(page);
        assert pageAddress > 0;
        roOffsetLo = pageOffset(page) - 1;
        roOffsetHi = roOffsetLo + getPageSize() + 1;
        absolutePointer = pageAddress - roOffsetLo - 1;
        return pageAddress;
    }

    private byte getByte0(long offset) {
        return Unsafe.getUnsafe().getByte(computeHotPage(pageIndex(offset)) + offsetInPage(offset));
    }

    private char getChar0(long offset) {
        int page = pageIndex(offset);
        long pageOffset = offsetInPage(offset);
        final long pageSize = getPageSize();

        if (pageSize - pageOffset > 1) {
            return Unsafe.getUnsafe().getChar(computeHotPage(page) + pageOffset);
        }

        return getCharBytes(page, pageOffset, pageSize);
    }

    char getCharBytes(int page, long pageOffset, long pageSize) {
        char value = 0;
        long pageAddress = getPageAddress(page);

        for (int i = 0; i < 2; i++) {
            if (pageOffset == pageSize) {
                pageAddress = getPageAddress(++page);
                pageOffset = 0;
            }
            char b = (char) (Unsafe.getUnsafe().getByte(pageAddress + pageOffset++));
            value = (char) ((b << (8 * i)) | value);
        }

        return value;
    }

    private double getDouble0(long offset) {
        int page = pageIndex(offset);
        long pageOffset = offsetInPage(offset);
        final long pageSize = getPageSize();

        if (pageSize - pageOffset > 7) {
            return Unsafe.getUnsafe().getDouble(computeHotPage(page) + pageOffset);
        }
        return getDoubleBytes(page, pageOffset, pageSize);
    }

    double getDoubleBytes(int page, long pageOffset, long pageSize) {
        return Double.longBitsToDouble(getLongBytes(page, pageOffset, pageSize));
    }

    private float getFloat0(long offset) {
        int page = pageIndex(offset);
        long pageOffset = offsetInPage(offset);

        if (getPageSize() - pageOffset > 3) {
            return Unsafe.getUnsafe().getFloat(computeHotPage(page) + pageOffset);
        }
        return getFloatBytes(page, pageOffset);
    }

    float getFloatBytes(int page, long pageOffset) {
        return Float.intBitsToFloat(getIntBytes(page, pageOffset));
    }

    private int getInt0(long offset) {
        int page = pageIndex(offset);
        long pageOffset = offsetInPage(offset);

        if (getPageSize() - pageOffset > 3) {
            return Unsafe.getUnsafe().getInt(computeHotPage(page) + pageOffset);
        }
        return getIntBytes(page, pageOffset);
    }

    int getIntBytes(int page, long pageOffset) {
        int value = 0;
        long pageAddress = getPageAddress(page);
        final long pageSize = getPageSize();

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
        long pageOffset = offsetInPage(offset);
        final long pageSize = getPageSize();

        if (pageSize - pageOffset > 7) {
            return Unsafe.getUnsafe().getLong(computeHotPage(page) + pageOffset);
        }
        return getLongBytes(page, pageOffset, pageSize);
    }

    long getLongBytes(int page, long pageOffset, long pageSize) {
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

    private short getShort0(long offset) {
        int page = pageIndex(offset);
        long pageOffset = offsetInPage(offset);
        final long pageSize = getPageSize();

        if (pageSize - pageOffset > 1) {
            return Unsafe.getUnsafe().getShort(computeHotPage(page) + pageOffset);
        }

        return getShortBytes(page, pageOffset, pageSize);
    }

    short getShortBytes(int page, long pageOffset, long pageSize) {
        short value = 0;
        long pageAddress = getPageAddress(page);

        for (int i = 0; i < 2; i++) {
            if (pageOffset == pageSize) {
                pageAddress = getPageAddress(++page);
                assert pageAddress != 0;
                pageOffset = 0;
            }
            short b = (short) (Unsafe.getUnsafe().getByte(pageAddress + pageOffset++) & 0xff);
            value = (short) ((b << (8 * i)) | value);
        }

        return value;
    }

    private void jumpTo0(long offset) {
        int page = pageIndex(offset);
        pageLo = mapWritePage(page);
        pageHi = pageLo + getPageSize();
        baseOffset = pageOffset(page + 1) - pageHi;
        appendPointer = pageLo + offsetInPage(offset);
        pageLo--;
    }

    private long mapRandomWritePage(long offset) {
        int page = pageIndex(offset);
        long pageAddress = mapWritePage(page);
        assert pageAddress != 0;
        roOffsetLo = pageOffset(page) - 1;
        roOffsetHi = roOffsetLo + getPageSize() + 1;
        absolutePointer = pageAddress - roOffsetLo - 1;
        return pageAddress;
    }

    protected long mapWritePage(int page) {
        long address;
        if (page < pages.size()) {
            address = pages.getQuick(page);
            if (address != 0) {
                return address;
            }
        }
        return cachePageAddress(page, allocateNextPage(page));
    }

    private void pageAt(long offset) {
        int page = pageIndex(offset);
        updateLimits(page, mapWritePage(page));
    }

    protected final long pageOffset(int page) {
        return ((long) page << extendSegmentMsb);
    }

    private void putBin0(BinarySequence value, long len, long remaining) {
        long pos = 0;
        do {
            putBinSequence(value, pos, remaining);
            pos += remaining;
            len -= remaining;

            pageAt(baseOffset + pageHi);
            remaining = pageHi - appendPointer;
            if (len < remaining) {
                putBinSequence(value, pos, len);
                appendPointer += len;
                break;
            }
        } while (true);
    }

    private void putBinSequence(BinarySequence value, long pos, long len) {
        value.copyTo(appendPointer, pos, len);
    }

    private void putBinSlit(long start, long len) {
        do {
            int half = (int) (pageHi - appendPointer);
            if (len <= half) {
                Vect.memcpy(start, appendPointer, len);
                appendPointer += len;
                break;
            }

            Vect.memcpy(start, appendPointer, half);
            pageAt(getAppendOffset() + half);  // +1?
            len -= half;
            start += half;
        } while (true);
    }

    private void putByteRnd(long offset, byte value) {
        Unsafe.getUnsafe().putByte(mapRandomWritePage(offset) + offsetInPage(offset), value);
    }

    void putCharBytes(char value) {
        putByte((byte) (value & 0xff));
        putByte((byte) ((value >> 8) & 0xff));
    }

    void putCharBytes(long offset, char value) {
        putByte(offset, (byte) (value & 0xff));
        putByte(offset + 1, (byte) ((value >> 8) & 0xff));
    }

    void putDoubleBytes(double value) {
        putLongBytes(Double.doubleToLongBits(value));
    }

    void putDoubleBytes(long offset, double value) {
        putLongBytes(offset, Double.doubleToLongBits(value));
    }

    void putFloatBytes(float value) {
        putIntBytes(Float.floatToIntBits(value));
    }

    void putFloatBytes(long offset, float value) {
        putIntBytes(offset, Float.floatToIntBits(value));
    }

    void putIntBytes(int value) {
        putByte((byte) (value & 0xff));
        putByte((byte) ((value >> 8) & 0xff));
        putByte((byte) ((value >> 16) & 0xff));
        putByte((byte) ((value >> 24) & 0xff));
    }

    void putIntBytes(long offset, int value) {
        putByte(offset, (byte) (value & 0xff));
        putByte(offset + 1, (byte) ((value >> 8) & 0xff));
        putByte(offset + 2, (byte) ((value >> 16) & 0xff));
        putByte(offset + 3, (byte) ((value >> 24) & 0xff));
    }

    private void putLong256Null() {
        Long256Impl.putNull(appendPointer);
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

    void putLongBytes(long offset, long value) {
        putByte(offset, (byte) (value & 0xffL));
        putByte(offset + 1, (byte) ((value >> 8) & 0xffL));
        putByte(offset + 2, (byte) ((value >> 16) & 0xffL));
        putByte(offset + 3, (byte) ((value >> 24) & 0xffL));
        putByte(offset + 4, (byte) ((value >> 32) & 0xffL));
        putByte(offset + 5, (byte) ((value >> 40) & 0xffL));
        putByte(offset + 6, (byte) ((value >> 48) & 0xffL));
        putByte(offset + 7, (byte) ((value >> 56) & 0xffL));
    }

    void putShortBytes(short value) {
        putByte((byte) (value & 0xff));
        putByte((byte) ((value >> 8) & 0xff));
    }

    void putShortBytes(long offset, short value) {
        putByte(offset, (byte) (value & 0xff));
        putByte(offset + 1, (byte) ((value >> 8) & 0xff));
    }

    private void putSplitChar(char c) {
        putByte((byte) c);
        putByte((byte) (c >> 8));
    }

    private long putStr0(CharSequence value, int pos, int len) {
        putInt(len);
        if (pageHi - appendPointer < (long) len << 1) {
            putStrSplit(value, pos, len);
        } else {
            copyStrChars(value, pos, len, appendPointer);
            appendPointer += len * 2L;
        }
        return getAppendOffset();
    }

    private void putStrSplit(long offset, CharSequence value, int pos, int len) {
        int start = pos;
        do {
            int half = (int) ((roOffsetHi - offset) / 2);

            if (len <= half) {
                copyStrChars(value, start, len, absolutePointer + offset);
                break;
            }

            copyStrChars(value, start, half, absolutePointer + offset);
            offset += half * 2L;
            if (offset < roOffsetHi) {
                char c = value.charAt(start + half);
                putByte(offset, (byte) c);
                putByte(offset + 1, (byte) (c >> 8));
                offset += 2;
                half++;
            } else {
                mapRandomWritePage(offset);
            }

            len -= half;
            start += half;
        } while (true);
    }

    private void putStrSplit(CharSequence value, int pos, int len) {
        int end = pos + len;
        int at = pos;
        while (at < end) {
            putSplitChar(value.charAt(at++));
        }
    }

    protected void release(long address) {
        if (address != 0) {
            Unsafe.free(address, getPageSize(), memoryTag);
        }
    }

    protected void releaseAllPagesButFirst() {
        final int n = pages.size();
        for (int i = 1; i < n; i++) {
            release(pages.getQuick(i));
            pages.setQuick(i, 0);
        }
        if (n > 0) {
            pages.setPos(1);
        }
    }

    private void skip0(long bytes) {
        jumpTo(getAppendOffset() + bytes);
    }

    protected final void updateLimits(int page, long pageAddress) {
        pageLo = pageAddress - 1;
        pageHi = pageAddress + getPageSize();
        baseOffset = pageOffset(page + 1) - pageHi;
        this.appendPointer = pageAddress;
    }

    public class CharSequenceView extends AbstractCharSequence {
        private int len;
        private long offset;

        @Override
        public int length() {
            return len;
        }

        @Override
        public char charAt(int index) {
            return getChar(offset + index * 2L);
        }

        CharSequenceView of(long offset, int len) {
            this.offset = offset;
            this.len = len;
            return this;
        }
    }

    private class ByteSequenceView implements BinarySequence {
        private long offset;
        private long len = -1;
        private long lastIndex = -1;
        private long readAddress;
        private long readLimit;

        @Override
        public byte byteAt(long index) {
            try {
                if (index == lastIndex + 1 && readAddress < readLimit) {
                    return Unsafe.getUnsafe().getByte(readAddress++);
                }
                return updatePosAndGet(index);
            } finally {
                lastIndex = index;
            }
        }

        @Override
        public void copyTo(long address, final long start, final long length) {
            MemoryPARWImpl.this.copyTo(address, this.offset + start, Math.min(length, this.len - start));
        }

        @Override
        public long length() {
            return len;
        }

        private void calculateBlobAddress(long offset) {
            final int page = pageIndex(offset);
            final long pa = getPageAddress(page);
            this.readAddress = pa + offsetInPage(offset);
            this.readLimit = pa + getPageSize();
        }

        ByteSequenceView of(long offset, long len) {
            this.offset = offset;
            this.len = len;
            this.lastIndex = -1;
            calculateBlobAddress(offset);
            return this;
        }

        private byte updatePosAndGet(long index) {
            calculateBlobAddress(this.offset + index);
            return Unsafe.getUnsafe().getByte(readAddress++);
        }
    }

    private class InPageLong256FromCharSequenceDecoder extends Long256FromCharSequenceDecoder {
        @Override
        public void setAll(long l0, long l1, long l2, long l3) {
            Unsafe.getUnsafe().putLong(appendPointer, l0);
            Unsafe.getUnsafe().putLong(appendPointer + 8, l1);
            Unsafe.getUnsafe().putLong(appendPointer + 16, l2);
            Unsafe.getUnsafe().putLong(appendPointer + 24, l3);
        }

        private void putLong256(CharSequence hexString) {
            final int len;
            if (hexString == null || (len = hexString.length()) == 0) {
                putLong256Null();
                appendPointer += Long256.BYTES;
            } else {
                putLong256(hexString, 2, len);
            }
        }

        private void putLong256(CharSequence hexString, int start, int end) {
            try {
                decode(hexString, start, end, inPageLong256Decoder);
            } catch (NumericException e) {
                throw CairoException.instance(0).put("invalid long256 [hex=").put(hexString).put(']');
            }
            appendPointer += Long256.BYTES;
        }
    }

    private class StraddlingPageLong256FromCharSequenceDecoder extends Long256FromCharSequenceDecoder {
        @Override
        public void setAll(long l0, long l1, long l2, long l3) {
            putLong(l0);
            putLong(l1);
            putLong(l2);
            putLong(l3);
        }

        private void putLong256(CharSequence hexString) {
            final int len;
            if (hexString == null || (len = hexString.length()) == 0) {
                putLong(Long256Impl.NULL_LONG256.getLong0());
                putLong(Long256Impl.NULL_LONG256.getLong1());
                putLong(Long256Impl.NULL_LONG256.getLong2());
                putLong(Long256Impl.NULL_LONG256.getLong3());
            } else {
                putLong256(hexString, 2, len);
            }
        }

        private void putLong256(CharSequence hexString, int start, int end) {
            try {
                decode(hexString, start, end, this);
            } catch (NumericException e) {
                throw CairoException.instance(0).put("invalid long256 [hex=").put(hexString).put(']');
            }
        }
    }
}