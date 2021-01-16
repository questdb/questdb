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

package io.questdb.cairo;

import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.AbstractCharSequence;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

/**
 * A version of {@link VirtualMemory} that uses a single contiguous memory region instead of pages. Note that it still has the concept of a page such that the contiguous memory region will grow in page sizes.
 *
 * @author Patrick Mackinlay
 */
public class ContiguousVirtualMemory implements BigMem, Mutable {
    static final int STRING_LENGTH_BYTES = 4;
    private static final Log LOG = LogFactory.getLog(ContiguousVirtualMemory.class);
    private final ByteSequenceView bsview = new ByteSequenceView();
    private final CharSequenceView csview = new CharSequenceView();
    private final CharSequenceView csview2 = new CharSequenceView();
    private final Long256Impl long256 = new Long256Impl();
    private final Long256Impl long256B = new Long256Impl();
    private final int maxPages;
    private final InPageLong256FromCharSequenceDecoder inPageLong256Decoder = new InPageLong256FromCharSequenceDecoder();
    private long pageSize;
    private long baseAddress = 0;
    private long baseAddressHi = 0;
    private long appendAddress = 0;

    public ContiguousVirtualMemory(long pageSize, int maxPages) {
        this.maxPages = maxPages;
        setPageSize(pageSize);
    }

    public static int getStorageLength(CharSequence s) {
        if (s != null) {
            return STRING_LENGTH_BYTES + s.length() * 2;
        }
        return STRING_LENGTH_BYTES;

    }

    public long addressOf(long offset) {
        return baseAddress + offset;
    }

    public long getAllocatedSize() {
        return baseAddressHi - baseAddress;
    }

    public void replacePage(long address, long size) {
        this.baseAddress = this.appendAddress = address;
        this.baseAddressHi = baseAddress + size;
    }

    @Override
    public void clear() {
        releaseMemory();
    }

    @Override
    public void close() {
        releaseMemory();
        baseAddress = 0;
        baseAddressHi = 0;
        appendAddress = 0;
    }

    @Override
    public final long putBin(BinarySequence value) {
        final long offset = getAppendOffset();
        if (value != null) {
            final long len = value.length();
            checkLimits(len + Long.BYTES);
            putLong(len);
            value.copyTo(appendAddress, 0, len);
            appendAddress += len;
        } else {
            putLong(TableUtils.NULL_LEN);
        }
        return offset;
    }

    public final long putBin(long from, long len) {
        checkLimits(len + Long.BYTES);
        final long offset = getAppendOffset();
        putLong(len > 0 ? len : TableUtils.NULL_LEN);
        if (len < 1) {
            return offset;
        }

        Unsafe.getUnsafe().copyMemory(from, appendAddress, len);
        appendAddress += len;
        return offset;
    }

    public void putBool(boolean value) {
        putByte((byte) (value ? 1 : 0));
    }

    public void putBool(long offset, boolean value) {
        putByte(offset, (byte) (value ? 1 : 0));
    }

    public final void putByte(long offset, byte value) {
        checkLimits(offset, Byte.BYTES);
        Unsafe.getUnsafe().putByte(baseAddress + offset, value);
    }

    public void putByte(byte b) {
        checkLimits(Byte.BYTES);
        Unsafe.getUnsafe().putByte(appendAddress, b);
        appendAddress++;
    }

    public void putChar(long offset, char value) {
        checkLimits(offset, Character.BYTES);
        Unsafe.getUnsafe().putChar(baseAddress + offset, value);
    }

    public final void putChar(char value) {
        checkLimits(Character.BYTES);
        Unsafe.getUnsafe().putChar(appendAddress, value);
        appendAddress += Character.BYTES;
    }

    public void putDouble(long offset, double value) {
        checkLimits(offset, Double.BYTES);
        Unsafe.getUnsafe().putDouble(baseAddress + offset, value);
    }

    public final void putDouble(double value) {
        checkLimits(Double.BYTES);
        Unsafe.getUnsafe().putDouble(appendAddress, value);
        appendAddress += Double.BYTES;
    }

    public void putFloat(long offset, float value) {
        checkLimits(offset, Float.BYTES);
        Unsafe.getUnsafe().putFloat(baseAddress + offset, value);
    }

    public final void putFloat(float value) {
        checkLimits(Float.BYTES);
        Unsafe.getUnsafe().putFloat(appendAddress, value);
        appendAddress += Float.BYTES;
    }

    public void putInt(long offset, int value) {
        checkLimits(offset, Integer.BYTES);
        Unsafe.getUnsafe().putInt(baseAddress + offset, value);
    }

    public final void putInt(int value) {
        checkLimits(Integer.BYTES);
        Unsafe.getUnsafe().putInt(appendAddress, value);
        appendAddress += Integer.BYTES;
    }

    public void putLong(long offset, long value) {
        checkLimits(offset, Long.BYTES);
        putLongUnsafe(offset, value);
    }

    public void putLongUnsafe(long offset, long value) {
        Unsafe.getUnsafe().putLong(baseAddress + offset, value);
    }

    public final void putLong(long value) {
        checkLimits(8);
        Unsafe.getUnsafe().putLong(appendAddress, value);
        appendAddress += Long.BYTES;
    }

    public final void putLong128(long l1, long l2) {
        putLong(l1);
        putLong(l2);
    }

    public void putLong256(long offset, Long256 value) {
        putLong256(
                offset,
                value.getLong0(),
                value.getLong1(),
                value.getLong2(),
                value.getLong3());
    }

    public void putLong256(long offset, long l0, long l1, long l2, long l3) {
        putLong(offset, l0);
        putLong(offset + Long.BYTES, l1);
        putLong(offset + Long.BYTES * 2, l2);
        putLong(offset + Long.BYTES * 3, l3);
    }

    public final void putLong256(long l0, long l1, long l2, long l3) {
        putLong(l0);
        putLong(l1);
        putLong(l2);
        putLong(l3);
    }

    public final void putLong256(Long256 value) {
        putLong256(
                value.getLong0(),
                value.getLong1(),
                value.getLong2(),
                value.getLong3());
    }

    public final void putLong256(CharSequence hexString) {
        inPageLong256Decoder.putLong256(hexString);
    }

    public final void putLong256(@NotNull CharSequence hexString, int start, int end) {
        inPageLong256Decoder.putLong256(hexString, start, end);
    }

    public final long putNullBin() {
        final long offset = getAppendOffset();
        putLong(TableUtils.NULL_LEN);
        return offset;
    }

    public final long putNullStr() {
        final long offset = getAppendOffset();
        putInt(TableUtils.NULL_LEN);
        return offset;
    }

    public final void putNullStr(long offset) {
        putInt(offset, TableUtils.NULL_LEN);
    }

    public void putShort(long offset, short value) {
        checkLimits(offset, 2);
        Unsafe.getUnsafe().putShort(baseAddress + offset, value);
    }

    public final void putShort(short value) {
        checkLimits(2);
        Unsafe.getUnsafe().putShort(appendAddress, value);
        appendAddress += 2;
    }

    public final long putStr(CharSequence value) {
        return value != null ? putStr0(value, 0, value.length()) : putNullStr();
    }

    public final long putStr(char value) {
        if (value != 0) {
            checkLimits(6);
            final long offset = getAppendOffset();
            putInt(1);
            Unsafe.getUnsafe().putChar(appendAddress, value);
            appendAddress += Character.BYTES;
            return offset;
        }
        return putNullStr();
    }

    public final long putStr(CharSequence value, int pos, int len) {
        if (value != null) {
            return putStr0(value, pos, len);
        }
        return putNullStr();
    }

    public void putStr(long offset, CharSequence value) {
        if (value != null) {
            putStr(offset, value, 0, value.length());
        } else {
            putNullStr(offset);
        }
    }

    public void putStr(long offset, CharSequence value, int pos, int len) {
        checkLimits(offset, len * 2L + STRING_LENGTH_BYTES);
        putInt(offset, len);
        copyStrChars(value, pos, len, baseAddress + offset + STRING_LENGTH_BYTES);
    }

    public final long getAppendOffset() {
        return appendAddress - baseAddress;
    }

    public final BinarySequence getBin(long offset) {
        final long len = getLong(offset);
        if (len > -1) {
            return bsview.of(offset + 8, len);
        }
        return null;
    }

    public final long getBinLen(long offset) {
        return getLong(offset);
    }

    public boolean getBool(long offset) {
        return getByte(offset) == 1;
    }

    public final byte getByte(long offset) {
        return Unsafe.getUnsafe().getByte(addressOf(offset));
    }

    public final char getChar(long offset) {
        return Unsafe.getUnsafe().getChar(addressOf(offset));
    }

    public final double getDouble(long offset) {
        return Unsafe.getUnsafe().getDouble(addressOf(offset));
    }

    public final float getFloat(long offset) {
        return Unsafe.getUnsafe().getFloat(addressOf(offset));
    }

    public final int getInt(long offset) {
        return Unsafe.getUnsafe().getInt(addressOf(offset));
    }

    public long getLong(long offset) {
        return Unsafe.getUnsafe().getLong(addressOf(offset));
    }

    public void getLong256(long offset, CharSink sink) {
        final long a, b, c, d;
        a = getLong(offset);
        b = getLong(offset + Long.BYTES);
        c = getLong(offset + Long.BYTES * 2);
        d = getLong(offset + Long.BYTES * 3);
        Numbers.appendLong256(a, b, c, d, sink);
    }

    public void getLong256(long offset, Long256Sink sink) {
        sink.setLong0(getLong(offset));
        sink.setLong1(getLong(offset + Long.BYTES));
        sink.setLong2(getLong(offset + Long.BYTES * 2));
        sink.setLong3(getLong(offset + Long.BYTES * 3));
    }

    public Long256 getLong256A(long offset) {
        getLong256(offset, long256);
        return long256;
    }

    public Long256 getLong256B(long offset) {
        getLong256(offset, long256B);
        return long256B;
    }

    public final short getShort(long offset) {
        return Unsafe.getUnsafe().getShort(addressOf(offset));
    }

    public final CharSequence getStr(long offset) {
        return getStr0(offset, csview);
    }

    public final CharSequence getStr0(long offset, CharSequenceView view) {
        final int len = getInt(offset);
        if (len != TableUtils.NULL_LEN) {
            return view.of(offset + STRING_LENGTH_BYTES, len);
        }
        return null;

    }

    public final CharSequence getStr2(long offset) {
        return getStr0(offset, csview2);
    }

    public final int getStrLen(long offset) {
        return getInt(offset);
    }

    public long hash(long offset, long size) {
        long n = size - (size & 7);
        long h = 179426491L;
        for (long i = 0; i < n; i += 8) {
            h = (h << 5) - h + getLong(offset + i);
        }

        for (; n < size; n++) {
            h = (h << 5) - h + getByte(offset + n);
        }
        return h;
    }

    /**
     * Updates append pointer with address for the given offset. All put* functions will be
     * appending from this offset onwards effectively overwriting data. Size of virtual memory remains
     * unaffected until the moment memory has to be extended.
     *
     * @param offset position from 0 in virtual memory.
     */
    public void jumpTo(long offset) {
        checkLimits(offset, 0);
        appendAddress = baseAddress + offset;
    }

    /**
     * Skips given number of bytes. Same as logically appending 0-bytes. Advantage of this method is that
     * no memory write takes place.
     *
     * @param bytes number of bytes to skip
     */
    public void skip(long bytes) {
        checkLimits(bytes);
        appendAddress += bytes;
    }

    public void zero() {
        long baseLength = baseAddressHi - baseAddress;
        Unsafe.getUnsafe().setMemory(baseAddress, baseLength, (byte) 0);
    }

    private static void copyStrChars(CharSequence value, int pos, int len, long address) {
        for (int i = 0; i < len; i++) {
            char c = value.charAt(i + pos);
            Unsafe.getUnsafe().putChar(address + 2L * i, c);
        }
    }

    private void checkAndExtend(long addressHi) {
        assert appendAddress <= baseAddressHi;
        assert addressHi >= baseAddress;
        if (addressHi <= baseAddressHi) {
            return;
        }
        doExtend(addressHi);
    }

    protected final void checkLimits(long size) {
        checkAndExtend(appendAddress + size);
    }

    protected final void checkLimits(long offset, long size) {
        checkAndExtend(baseAddress + offset + size);
    }

    private void doExtend(long addressHi) {
        long newSize = addressHi - baseAddress;
        long nPages = (newSize / pageSize) + 1;
        newSize = nPages * pageSize;
        final long oldSize = getMemorySize();
        if (nPages > maxPages) {
            throw LimitOverflowException.instance().put("Maximum number of pages (").put(maxPages).put(") breached in VirtualMemory");
        }
        final long newBaseAddress = reallocateMemory(baseAddress, getMemorySize(), newSize);
        if (oldSize > 0) {
            LOG.info().$("extended [oldBase=").$(baseAddress).$(", newBase=").$(newBaseAddress).$(", oldSize=").$(oldSize).$(", newSize=").$(newSize).$(']').$();
        }
        handleMemoryReallocation(newBaseAddress, newSize);
    }

    protected long getMapPageSize() {
        return pageSize;
    }

    protected final long getMemorySize() {
        return baseAddressHi - baseAddress;
    }

    protected final void handleMemoryReallocation(long newBaseAddress, long newSize) {
        assert newBaseAddress != 0;
        long appendOffset = appendAddress - baseAddress;
        baseAddress = newBaseAddress;
        baseAddressHi = baseAddress + newSize;
        appendAddress = baseAddress + appendOffset;
        if (appendAddress > baseAddressHi) {
            appendAddress = baseAddressHi;
        }
    }

    protected final void handleMemoryReleased() {
        baseAddress = 0;
        baseAddressHi = 0;
        appendAddress = 0;
    }

    private void putLong256Null() {
        checkLimits(32);
        Long256Impl.putNull(appendAddress);
    }

    private long putStr0(CharSequence value, int pos, int len) {
        checkLimits(len * 2L + STRING_LENGTH_BYTES);
        final long offset = getAppendOffset();
        Unsafe.getUnsafe().putInt(appendAddress, len);
        copyStrChars(value, pos, len, appendAddress + Integer.BYTES);
        appendAddress += len * 2L + Integer.BYTES;
        return offset;
    }

    protected long reallocateMemory(long currentBaseAddress, long currentSize, long newSize) {
        if (currentBaseAddress != 0) {
            return Unsafe.realloc(currentBaseAddress, currentSize, newSize);
        }
        return Unsafe.malloc(newSize);
    }

    protected void releaseMemory() {
        if (baseAddress != 0) {
            long baseLength = baseAddressHi - baseAddress;
            Unsafe.free(baseAddress, baseLength);
            handleMemoryReleased();
        }
    }

    protected final void setPageSize(long pageSize) {
        this.pageSize = Numbers.ceilPow2(pageSize);
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
            return ContiguousVirtualMemory.this.getChar(offset + index * 2L);
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

        @Override
        public byte byteAt(long index) {
            return getByte(offset + index);
        }

        @Override
        public void copyTo(long address, final long start, final long length) {
            long bytesRemaining = Math.min(length, this.len - start);
            long offset = this.offset + start;
            Unsafe.getUnsafe().copyMemory(baseAddress + offset, address, bytesRemaining);
        }

        @Override
        public long length() {
            return len;
        }

        ByteSequenceView of(long offset, long len) {
            this.offset = offset;
            this.len = len;
            return this;
        }
    }

    private class InPageLong256FromCharSequenceDecoder extends Long256FromCharSequenceDecoder {
        @Override
        public void onDecoded(long l0, long l1, long l2, long l3) {
            checkLimits(Long256.BYTES);
            Unsafe.getUnsafe().putLong(appendAddress, l0);
            Unsafe.getUnsafe().putLong(appendAddress + 8, l1);
            Unsafe.getUnsafe().putLong(appendAddress + 16, l2);
            Unsafe.getUnsafe().putLong(appendAddress + 24, l3);
        }

        private void putLong256(CharSequence hexString, int start, int end) {
            try {
                decode(hexString, start, end, inPageLong256Decoder);
            } catch (NumericException e) {
                throw CairoException.instance(0).put("invalid long256 [hex=").put(hexString).put(']');
            }
            appendAddress += Long256.BYTES;
        }

        private void putLong256(CharSequence hexString) {
            final int len;
            if (hexString == null || (len = hexString.length()) == 0) {
                putLong256Null();
                appendAddress += Long256.BYTES;
            } else {
                putLong256(hexString, 2, len);
            }
        }
    }
}