/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.cairo.map;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import org.jetbrains.annotations.NotNull;

/**
 * Spillable heap memory intended for FastMap. Upon initialization, it will create an instance of MemoryCARWImpl and grow
 * in multiple of pageSize. When size grows beyond diskSpillThreshold, an instance of MemoryCMARWImpl is created to
 * enable spilling to disk. After spilled, the size grows in multiple of extendSegmentSize.
 * The extend() behavior follows the underlying MemoryCARWImpl and MemoryCMARWImpl, which has different behavior
 * in some edge cases.
 */
public class SpillableHeapMemory implements MemoryCARW, Mutable {
    private static final Log LOG = LogFactory.getLog(SpillableHeapMemory.class);
    // flag
    private final long diskSpillThreshold;
    private final long extendSegmentMsb;
    private final FilesFacade ff = new FilesFacadeImpl();
    private final int maxPages;
    private final long maxSize;
    private final int nativeMemoryTag;
    // for non-mmap
    private final long pageSize;
    // general
    private final int spillMemoryTag;
    // for mmap
    private final Path spillPath;
    private boolean isSpilled;
    private MemoryCARW mem;
    private long opts = CairoConfiguration.O_NONE;

    public SpillableHeapMemory(long pageSize,
                               long diskSpillThreshold,
                               long extendSegmentSize,
                               long maxSize,
                               Path spillFilePath) {
        this(pageSize,
                diskSpillThreshold,
                extendSegmentSize,
                maxSize,
                spillFilePath,
                MemoryTag.NATIVE_DEFAULT,
                MemoryTag.MMAP_DEFAULT);
    }

    SpillableHeapMemory(long pageSize,
                        long diskSpillThreshold,
                        long extendSegmentSize,
                        long maxSize,
                        Path spillFilePath,
                        int nativeMemoryTag,
                        int spillMemoryTag) {
        this.pageSize = Numbers.ceilPow2(pageSize);
        extendSegmentSize = Numbers.ceilPow2(extendSegmentSize);
        this.extendSegmentMsb = Numbers.msb(extendSegmentSize);
        this.maxSize = Numbers.ceilPow2(maxSize);
        this.diskSpillThreshold = Numbers.ceilPow2(diskSpillThreshold);

        assert this.maxSize > this.pageSize :
                String.format("maxSize (%s) should be larger than pageSize (%s)",
                        this.maxSize, this.pageSize);
        assert diskSpillThreshold > pageSize :
                "diskSpillThreshold should be larger than pageSize";
        if (maySpill()) {
            // if it is configured not to spill, extendSegmentMsb is irrelevant
            assert this.extendSegmentMsb > Numbers.msb(this.pageSize) :
                    "extendSegmentMsb should be larger than pageSizeMsb";
            assert maxSize > (1L << this.extendSegmentMsb) :
                    "maxSize should be larger than extendSegmentSize";
        }

        this.maxPages = getMaxPages(this.maxSize, this.pageSize);

        this.nativeMemoryTag = nativeMemoryTag;
        this.spillMemoryTag = spillMemoryTag;
        mem = initMem();
        spillPath = spillFilePath;

        // pre-check spillPath can be used
        if (maySpill()) {
            assert spillPath != null;
            ff.touch(spillPath);
            ff.remove(spillPath);
        }
    }

    @Override
    public long addressOf(long offset) {
        return mem.addressOf(offset);
    }

    @Override
    public long appendAddressFor(long bytes) {
        checkAndExtend(getAppendAddress() + bytes);
        return mem.appendAddressFor(bytes);
    }

    @Override
    public long appendAddressFor(long offset, long bytes) {
        checkAndExtend(mem.getAddress() + offset + bytes);
        return mem.addressOf(offset);
    }

    /**
     * Since this is created for FastMap, it is expected to just reset the append-pointer to 0 and treat the memory as
     * empty without setting bytes to 0.
     */
    @Override
    public void clear() {
        mem.jumpTo(0);
    }

    @Override
    public void close() {
        mem.close();
        if (isSpilled) {
            ff.remove(spillPath);
            isSpilled = false;
        }
    }

    @Override
    public void extend(long size) {
        checkAndExtend(mem.getAddress() + size);
    }

    @Override
    public long getAppendOffset() {
        return mem.getAppendOffset();
    }

    @Override
    public BinarySequence getBin(long offset) {
        return mem.getBin(offset);
    }

    /**
     * This method does not make sense since this memory consists of different memory types. However, this is required
     * since this class implements MemoryCARW. We in fact could opt to not implement MemoryCMARW, but instead we
     * implement it to derive MemoryCARW default methods.
     *
     * @return
     */
    @Override
    public long getExtendSegmentSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long256 getLong256A(long offset) {
        return mem.getLong256A(offset);
    }

    @Override
    public Long256 getLong256B(long offset) {
        return mem.getLong256B(offset);
    }

    @Override
    public long getPageAddress(int pageIndex) {
        return mem.getPageAddress(pageIndex);
    }

    @Override
    public int getPageCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getPageSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStr(long offset) {
        return mem.getStr(offset);
    }

    @Override
    public CharSequence getStr2(long offset) {
        return mem.getStr2(offset);
    }

    public boolean isSpilled() {
        return isSpilled;
    }

    /**
     * Updates append pointer with address for the given offset. All put* functions will be appending from this offset
     * onwards effectively overwriting data. Size of virtual memory remains unaffected until the moment memory has to be
     * extended.
     *
     * @param offset position from 0 in virtual memory.
     */
    @Override
    public void jumpTo(long offset) {
        checkAndExtend(mem.getAddress() + offset);
        mem.jumpTo(offset);
    }

    @Override
    public long offsetInPage(long offset) {
        return mem.offsetInPage(offset);
    }

    @Override
    public int pageIndex(long offset) {
        return mem.pageIndex(offset);
    }

    public void putBin32(long offset, BinarySequence value) {
        if (value != null) {
            final long len = value.length();
            if (len + Integer.BYTES > Integer.MAX_VALUE) {
                throw CairoException.nonCritical().put("binary column is too large");
            }
            final long addr = appendAddressFor(offset, len + Integer.BYTES);
            Unsafe.getUnsafe().putInt(addr, (int) len);
            value.copyTo(addr + Integer.BYTES, 0, len);
        } else {
            putNullBin32(offset);
        }
    }

    public void putLong128(long offset, long lo, long hi) {
        final long addr = appendAddressFor(offset, 2 * Long.BYTES);
        Unsafe.getUnsafe().putLong(addr, lo);
        Unsafe.getUnsafe().putLong(addr + Long.BYTES, hi);
    }

    @Override
    public void putLong256(@NotNull CharSequence hexString, int start, int end) {
        mem.putLong256(hexString, start, end);
    }

    public void putNullBin32(long offset) {
        putInt(offset, TableUtils.NULL_LEN);
    }

    public void putStrLowerCase(long offset, CharSequence value) {
        if (value != null) {
            putStrLowerCase(offset, value, 0, value.length());
        } else {
            putNullStr(offset);
        }
    }

    public void putStrLowerCase(long offset, CharSequence value, int pos, int len) {
        final long addr = appendAddressFor(offset, Vm.getStorageLength(len));
        Unsafe.getUnsafe().putInt(addr, len);
        Chars.copyStrCharsLowerCase(value, pos, len, addr + Vm.STRING_LENGTH_BYTES);
    }

    @Override
    public long resize(long size) {
        extend(size);
        return mem.getAddress();
    }

    @Override
    public void shiftAddressRight(long shiftRightOffset) {
        mem.shiftAddressRight(shiftRightOffset);
    }

    @Override
    public long size() {
        return mem.size();
    }

    /**
     * Skips given number of bytes. Same as logically appending 0-bytes. Advantage of this method is that no memory
     * write takes place.
     *
     * @param bytes number of bytes to skip
     */
    @Override
    public void skip(long bytes) {
        checkAndExtend(getAppendAddress() + bytes);
        mem.skip(bytes);
    }

    /**
     * Reset the appendOffset and resize to 1 page
     */
    @Override
    public void truncate() {
        if (isSpilled) {
            mem.close();
            ff.remove(spillPath);
            mem = initMem();
            isSpilled = false;
        }
        mem.truncate();
        assert mem.size() == pageSize;
    }

    @Override
    public void zero() {
        mem.zero();
    }

    private long candidateExtendedSpilledSize(long size) {
        final long nPages = size > 0 ? ((size - 1) >>> extendSegmentMsb) + 1 : 1;
        size = nPages << extendSegmentMsb;
        if (size > maxSize) {
            throw LimitOverflowException.instance().put("Maximum memory sizes (").put(maxSize).put(
                    ") breached in VirtualMemory");
        }
        return size;
    }

    private void checkAndExtend(long address) {
        if (address <= mem.getAddress() + mem.size()) {
            return;
        }
        long sz = address - mem.getAddress();
        if (!isSpilled && (sz > diskSpillThreshold)) {
            final MemoryCARW cmarw = initFileBasedMem(candidateExtendedSpilledSize(sz), mem.getAppendOffset());
            Vect.memmove(cmarw.getAddress(), mem.getAddress(), mem.size());
            LOG.debug().$("Start spilling. curSz: ").$(mem.size()).$(", requestedSz: ").$(sz).$(", newSz: ").$(
                    cmarw.size()).$();
            mem.close(); // close CARW
            mem = cmarw;
            isSpilled = true;
            return;
        }

        // MemoryCMARWImpl does not have memory breached checking, so adding one here. In addition, default
        // MemoryCMARWImpl behavior is different with MemoryCARWImpl in edge-cases.
        // If newSize is a multiple of pageSize (n):
        // - MemoryCARWImpl extends to n pages
        // - MemoryCMARWImpl extends to (n+1) pages
        // Here we calculate the expected size in advance and minus 1, to make both MemoryImpl extends the same way,
        // i.e. always extend to n pages in above case.
        if (isSpilled) {
            sz = candidateExtendedSpilledSize(sz) - 1;
        }

        mem.extend(sz);
    }

    private long getAppendAddress() {
        return mem.getAddress() + mem.getAppendOffset();
    }

    private int getMaxPages(long maxSize, long pageSize) {
        long maxPages = maxSize / pageSize;
        return maxPages > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) maxPages;
    }

    private MemoryCMARWImpl initFileBasedMem(long size, long startAppendOffset) {
        final MemoryCMARWImpl mem = new MemoryCMARWImpl(ff, spillPath, Numbers.ceilPow2(1L << extendSegmentMsb),
                size, spillMemoryTag, opts);
        mem.jumpTo(startAppendOffset);
        return mem;
    }

    private MemoryCARWImpl initMem() {
        return new MemoryCARWImpl(pageSize, maxPages, nativeMemoryTag);
    }

    private boolean maySpill() {
        return maxSize > diskSpillThreshold;
    }

}
