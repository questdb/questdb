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

package io.questdb.cairo.vm;

import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;

/**
 * A version of {@link MemoryPARWImpl} that uses a single contiguous memory region instead of pages.
 * Note that it still has the concept of a page such that the contiguous memory region will extend in page sizes.
 *
 * @author Patrick Mackinlay
 */
public class MemoryCARWImpl extends AbstractMemoryCR implements MemoryCARW, Mutable {
    private static final Log LOG = LogFactory.getLog(MemoryCARWImpl.class);
    private final Long256Acceptor long256Acceptor = this::putLong256;
    private final int maxPages;
    private final int memoryTag;
    private long appendAddress = 0;
    private long sizeMsb;

    public MemoryCARWImpl(long pageSize, int maxPages, int memoryTag) {
        this.memoryTag = memoryTag;
        this.maxPages = maxPages;
        setPageSize(pageSize);
    }

    @Override
    public long appendAddressFor(long bytes) {
        checkAndExtend(appendAddress + bytes);
        long result = appendAddress;
        appendAddress += bytes;
        return result;
    }

    @Override
    public long appendAddressFor(long offset, long bytes) {
        checkAndExtend(pageAddress + offset + bytes);
        return addressOf(offset);
    }

    @Override
    public void clear() {
        super.clear();
        if (pageAddress != 0) {
            long baseLength = lim - pageAddress;
            Unsafe.free(pageAddress, baseLength, memoryTag);
            handleMemoryReleased();
            size = 0;
        }
    }

    @Override
    public void close() {
        clear();
        pageAddress = 0;
        lim = 0;
        appendAddress = 0;
    }

    @Override
    public void extend(long size) {
        checkAndExtend(pageAddress + size);
    }

    @Override
    public final long getAppendOffset() {
        return appendAddress - pageAddress;
    }

    @Override
    public long getExtendSegmentSize() {
        return 1L << sizeMsb;
    }

    @Override
    public long getPageSize() {
        return getExtendSegmentSize();
    }

    /**
     * Updates append pointer with address for the given offset. All put* functions will be
     * appending from this offset onwards effectively overwriting data. Size of virtual memory remains
     * unaffected until the moment memory has to be extended.
     *
     * @param offset position from 0 in virtual memory.
     */
    public void jumpTo(long offset) {
        checkAndExtend(pageAddress + offset);
        appendAddress = pageAddress + offset;
    }

    public final void putLong256(@NotNull CharSequence hexString, int start, int end) {
        putLong256(hexString, start, end, long256Acceptor);
    }

    @Override
    public long size() {
        return size;
    }

    /**
     * Skips given number of bytes. Same as logically appending 0-bytes. Advantage of this method is that
     * no memory write takes place.
     *
     * @param bytes number of bytes to skip
     */
    @Override
    public void skip(long bytes) {
        checkAndExtend(appendAddress + bytes);
        appendAddress += bytes;
    }

    @Override
    public void truncate() {
        // our internal "extend" implementation will reduce size
        // as well as extend it
        extend0(0);
        // reset append offset
        appendAddress = pageAddress;
    }

    @Override
    public void zero() {
        long baseLength = lim - pageAddress;
        Vect.memset(pageAddress, baseLength, 0);
    }

    private void checkAndExtend(long address) {
        assert appendAddress <= lim;
        assert address >= pageAddress;
        if (address <= lim) {
            return;
        }
        extend0(address - pageAddress);
    }

    private void extend0(long size) {
        long nPages = size > 0 ? ((size - 1) >>> sizeMsb) + 1 : 1;
        size = nPages << sizeMsb;
        final long oldSize = size();

        // sometimes the resize request ends up being the same
        // as existing memory size
        if (size == oldSize) {
            return;
        }

        if (nPages > maxPages) {
            throw LimitOverflowException.instance().put("Maximum number of pages (").put(maxPages).put(") breached in VirtualMemory");
        }
        final long newBaseAddress = reallocateMemory(pageAddress, size(), size);
        if (oldSize > 0) {
            LOG.debug().$("extended [oldBase=").$(pageAddress).$(", newBase=").$(newBaseAddress).$(", oldSize=").$(oldSize).$(", newSize=").$(size).$(']').$();
        }
        handleMemoryReallocation(newBaseAddress, size);
    }

    protected final void handleMemoryReallocation(long newBaseAddress, long newSize) {
        assert newBaseAddress != 0;
        long appendOffset = appendAddress - pageAddress;
        pageAddress = newBaseAddress;
        lim = pageAddress + newSize;
        appendAddress = pageAddress + appendOffset;
        if (appendAddress > lim) {
            appendAddress = lim;
        }
        this.size = newSize;
    }

    protected final void handleMemoryReleased() {
        pageAddress = 0;
        lim = 0;
        appendAddress = 0;
        size = 0;
    }

    protected long reallocateMemory(long currentBaseAddress, long currentSize, long newSize) {
        if (currentBaseAddress != 0) {
            return Unsafe.realloc(currentBaseAddress, currentSize, newSize, memoryTag);
        }
        return Unsafe.malloc(newSize, memoryTag);
    }

    protected final void setPageSize(long size) {
        this.sizeMsb = Numbers.msb(Numbers.ceilPow2(size));
    }
}
