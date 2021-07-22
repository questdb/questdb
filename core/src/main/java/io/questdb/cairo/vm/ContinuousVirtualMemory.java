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

import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * A version of {@link PagedVirtualMemory} that uses a single contiguous memory region instead of pages. Note that it still has the concept of a page such that the contiguous memory region will extend in page sizes.
 *
 * @author Patrick Mackinlay
 */
public class ContinuousVirtualMemory extends AbstractContinuousMemory
        implements ContinuousReadWriteVirtualMemory, Mutable, Closeable {
    private static final Log LOG = LogFactory.getLog(ContinuousVirtualMemory.class);
    private final int maxPages;
    private final Long256Acceptor long256Acceptor = this::putLong256;
    private long pageSize;
    private long pageSizeMsb;
    private long baseAddress = 0;
    private long baseAddressHi = 0;
    private long appendAddress = 0;

    public ContinuousVirtualMemory(long pageSize, int maxPages) {
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
        checkAndExtend(baseAddress + offset + bytes);
        return addressOf(offset);
    }

    @Override
    public final long getAppendOffset() {
        return appendAddress - baseAddress;
    }

    @Override
    public void clear() {
        if (baseAddress != 0) {
            long baseLength = baseAddressHi - baseAddress;
            Unsafe.free(baseAddress, baseLength);
            handleMemoryReleased();
        }
    }

    @Override
    public void close() {
        clear();
        baseAddress = 0;
        baseAddressHi = 0;
        appendAddress = 0;
    }

    @Override
    public long getPageAddress(int pageIndex) {
        return baseAddress;
    }

    @Override
    public int getPageCount() {
        return baseAddress == 0 ? 0 : 1;
    }

    @Override
    public void extend(long size) {
        long nPages = (size >>> pageSizeMsb) + 1;
        size = nPages << pageSizeMsb;
        final long oldSize = size();
        if (nPages > maxPages) {
            throw LimitOverflowException.instance().put("Maximum number of pages (").put(maxPages).put(") breached in VirtualMemory");
        }
        final long newBaseAddress = reallocateMemory(baseAddress, size(), size);
        if (oldSize > 0) {
            LOG.debug().$("extended [oldBase=").$(baseAddress).$(", newBase=").$(newBaseAddress).$(", oldSize=").$(oldSize).$(", newSize=").$(size).$(']').$();
        }
        handleMemoryReallocation(newBaseAddress, size);
    }

    @Override
    public long size() {
        return baseAddressHi - baseAddress;
    }

    @Override
    public long addressOf(long offset) {
        return baseAddress + offset;
    }

    /**
     * Updates append pointer with address for the given offset. All put* functions will be
     * appending from this offset onwards effectively overwriting data. Size of virtual memory remains
     * unaffected until the moment memory has to be extended.
     *
     * @param offset position from 0 in virtual memory.
     */
    public void jumpTo(long offset) {
        checkAndExtend(baseAddress + offset);
        appendAddress = baseAddress + offset;
    }

    public final void putLong256(@NotNull CharSequence hexString, int start, int end) {
        putLong256(hexString, start, end, long256Acceptor);
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
        // our "extend" implementation will reduce size as well as
        // extend it
        extend(0);
        // reset append offset
        appendAddress = baseAddress;
    }

    public void replacePage(long address, long size) {
        long appendOffset = getAppendOffset();
        this.baseAddress = this.appendAddress = address;
        this.baseAddressHi = baseAddress + size;
        jumpTo(appendOffset);
    }

    public long resize(long size) {
        checkAndExtend(baseAddress + size);
        return baseAddress;
    }

    public void zero() {
        long baseLength = baseAddressHi - baseAddress;
        Vect.memset(baseAddress, baseLength, 0);
    }

    private void checkAndExtend(long address) {
        assert appendAddress <= baseAddressHi;
        assert address >= baseAddress;
        if (address <= baseAddressHi) {
            return;
        }
        extend(address - baseAddress);
    }

    protected long getMapPageSize() {
        return pageSize;
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

    protected long reallocateMemory(long currentBaseAddress, long currentSize, long newSize) {
        if (currentBaseAddress != 0) {
            return Unsafe.realloc(currentBaseAddress, currentSize, newSize);
        }
        return Unsafe.malloc(newSize);
    }

    protected final void setPageSize(long pageSize) {
        this.pageSize = Numbers.ceilPow2(pageSize);
        this.pageSizeMsb = Numbers.msb(this.pageSize);
    }
}