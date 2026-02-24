/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;

import java.util.Arrays;

// paged mapped readable
public class MemoryPMRImpl extends AbstractMemoryCR implements MemoryMR {
    private static final Log LOG = LogFactory.getLog(MemoryPMRImpl.class);
    private final long[] mappedAddresses;
    private final long[] mappedLengths;
    private final long[] mappedPageIndexes;
    private final long[] pageAccessCounters;
    private final int[] pinCounters;
    private final int pageSizeMsb;
    private final int maxMappedPages;
    private final long pageSize;
    private final long pageSizeMask;
    private long accessCounter;
    private long fd = -1;
    private int madviseOpts = -1;
    private int mappedPageCount;
    private int memoryTag = MemoryTag.MMAP_DEFAULT;

    public MemoryPMRImpl(long pageSize, int maxMappedPages) {
        if (pageSize <= 0 || !Numbers.isPow2(pageSize)) {
            throw new IllegalArgumentException("page size must be a positive power of two");
        }
        if (maxMappedPages < 2) {
            throw new IllegalArgumentException("maxMappedPages must be >= 2");
        }
        this.pageSize = pageSize;
        this.pageSizeMask = pageSize - 1;
        this.pageSizeMsb = Numbers.msb(pageSize);
        this.maxMappedPages = maxMappedPages;
        this.mappedAddresses = new long[maxMappedPages];
        this.mappedLengths = new long[maxMappedPages];
        this.mappedPageIndexes = new long[maxMappedPages];
        this.pageAccessCounters = new long[maxMappedPages];
        this.pinCounters = new int[maxMappedPages];
        Arrays.fill(mappedPageIndexes, -1L);
    }

    @Override
    public long addressHi() {
        return size > 0 ? addressOf(size) : 0;
    }

    @Override
    public long addressOf(long offset) {
        if (offset < 0 || offset > size) {
            throw CairoException.critical(0)
                    .put("offset is outside of file boundary [offset=").put(offset)
                    .put(", size=").put(size)
                    .put(']');
        }

        if (offset == size) {
            if (size == 0) {
                return 0;
            }
            final long lastOffset = size - 1;
            final int slot = ensureMapped(pageIndex(lastOffset));
            return mappedAddresses[slot] + offsetInPage(lastOffset) + 1;
        }

        final int slot = ensureMapped(pageIndex(offset));
        return mappedAddresses[slot] + offsetInPage(offset);
    }

    @Override
    public boolean checkOffsetMapped(long offset) {
        return offset >= 0 && offset <= size;
    }

    @Override
    public void close() {
        clear();
        releaseMappedPages();
        if (ff != null && ff.close(fd)) {
            LOG.debug().$("closed [fd=").$(fd).I$();
        }
        fd = -1;
        ff = null;
        size = 0;
        accessCounter = 0;
        mappedPageCount = 0;
    }

    @Override
    public long detachFdClose() {
        final long detachedFd = fd;
        fd = -1;
        close();
        return detachedFd;
    }

    @Override
    public void extend(long newSize) {
        if (newSize < 0) {
            throw CairoException.critical(0).put("invalid size [size=").put(newSize).put(']');
        }
        size = newSize;
    }

    @Override
    public long getFd() {
        return fd;
    }

    @Override
    public long getLong(long offset) {
        if (offset < 0 || offset > size - Long.BYTES) {
            throw CairoException.critical(0)
                    .put("long read is outside of file boundary [offset=").put(offset)
                    .put(", size=").put(size)
                    .put(']');
        }

        final int slot = ensureMapped(pageIndex(offset));
        final long inPageOffset = offsetInPage(offset);
        if (mappedLengths[slot] - inPageOffset >= Long.BYTES) {
            return Unsafe.getUnsafe().getLong(mappedAddresses[slot] + inPageOffset);
        }
        long value = 0;
        long bitShift = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            value |= (Unsafe.getUnsafe().getByte(addressOf(offset + i)) & 0xffL) << bitShift;
            bitShift += Byte.SIZE;
        }
        return value;
    }

    @Override
    public long getPageAddress(int pageIndex) {
        final int slot = findSlot(pageIndex);
        return slot > -1 ? mappedAddresses[slot] : 0;
    }

    @Override
    public int getPageCount() {
        return mappedPageCount;
    }

    public long getMappedLength(int slot) {
        if (slot < 0 || slot >= maxMappedPages || mappedPageIndexes[slot] < 0) {
            throw CairoException.critical(0).put("page slot is not mapped [slot=").put(slot).put(']');
        }
        return mappedLengths[slot];
    }

    @Override
    public long getPageSize() {
        return pageSize;
    }

    @Override
    public boolean isMapped(long offset, long len) {
        if (len < 0 || offset < 0 || offset > size) {
            return false;
        }
        if (len == 0) {
            return true;
        }
        if (offset > size - len) {
            return false;
        }
        final long hi = offset + len - 1;
        final int loPage = pageIndex(offset);
        final int hiPage = pageIndex(hi);
        if (loPage != hiPage) {
            return false;
        }
        final int slot = findSlot(loPage);
        return slot > -1 && offsetInPage(offset) + len <= mappedLengths[slot];
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, int opts, int madviseOpts) {
        close();
        this.ff = ff;
        this.memoryTag = memoryTag;
        this.madviseOpts = madviseOpts;
        this.fd = TableUtils.openRO(ff, name, LOG);
        if (size < 0) {
            this.size = ff.length(fd);
            if (this.size < 0) {
                close();
                throw CairoException.critical(ff.errno()).put("could not get length: ").put(name);
            }
        } else {
            this.size = size;
        }
    }

    @Override
    public long offsetInPage(long offset) {
        return offset & pageSizeMask;
    }

    @Override
    public int pageIndex(long offset) {
        return (int) (offset >> pageSizeMsb);
    }

    public int pin(long offset) {
        if (offset < 0 || offset >= size) {
            throw CairoException.critical(0)
                    .put("cannot pin offset outside of file boundary [offset=").put(offset)
                    .put(", size=").put(size)
                    .put(']');
        }
        final int slot = ensureMapped(pageIndex(offset));
        pinCounters[slot]++;
        return slot;
    }

    public void unpin(int slot) {
        if (slot > -1 && slot < maxMappedPages && pinCounters[slot] > 0) {
            pinCounters[slot]--;
        }
    }

    private int ensureMapped(int pageIndex) {
        int slot = findSlot(pageIndex);
        if (slot > -1) {
            touch(slot);
            return slot;
        }

        slot = findFreeSlot();
        if (slot < 0) {
            slot = findEvictionCandidate();
            if (slot < 0) {
                throw CairoException.critical(0).put("cannot map page, all mapped pages are pinned");
            }
            unmap(slot);
        }

        final long mapOffset = ((long) pageIndex) << pageSizeMsb;
        final long mapLen = Math.min(pageSize, size - mapOffset);
        final long mappedAddress = TableUtils.mapRO(ff, fd, mapLen, mapOffset, memoryTag);
        ff.madvise(mappedAddress, mapLen, madviseOpts);

        mappedAddresses[slot] = mappedAddress;
        mappedLengths[slot] = mapLen;
        mappedPageIndexes[slot] = pageIndex;
        pinCounters[slot] = 0;
        mappedPageCount++;
        touch(slot);
        return slot;
    }

    private int findEvictionCandidate() {
        int candidate = -1;
        long oldestAccess = Long.MAX_VALUE;
        for (int i = 0; i < maxMappedPages; i++) {
            if (mappedPageIndexes[i] > -1 && pinCounters[i] == 0 && pageAccessCounters[i] < oldestAccess) {
                oldestAccess = pageAccessCounters[i];
                candidate = i;
            }
        }
        return candidate;
    }

    private int findFreeSlot() {
        for (int i = 0; i < maxMappedPages; i++) {
            if (mappedPageIndexes[i] < 0) {
                return i;
            }
        }
        return -1;
    }

    private int findSlot(int pageIndex) {
        for (int i = 0; i < maxMappedPages; i++) {
            if (mappedPageIndexes[i] == pageIndex) {
                return i;
            }
        }
        return -1;
    }

    private void releaseMappedPages() {
        for (int i = 0; i < maxMappedPages; i++) {
            if (mappedPageIndexes[i] > -1) {
                unmap(i);
            } else {
                pinCounters[i] = 0;
                pageAccessCounters[i] = 0;
            }
        }
    }

    private void touch(int slot) {
        pageAccessCounters[slot] = ++accessCounter;
    }

    private void unmap(int slot) {
        ff.munmap(mappedAddresses[slot], mappedLengths[slot], memoryTag);
        LOG.debug()
                .$("unmapped [pageIndex=").$(mappedPageIndexes[slot])
                .$(", size=").$(mappedLengths[slot])
                .$(", memoryTag=").$(memoryTag)
                .I$();
        mappedAddresses[slot] = 0;
        mappedLengths[slot] = 0;
        mappedPageIndexes[slot] = -1;
        pinCounters[slot] = 0;
        pageAccessCounters[slot] = 0;
        mappedPageCount--;
    }
}
