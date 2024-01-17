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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

// contiguous mapped appendable readable writable
public class MemoryCMARWImpl extends AbstractMemoryCR implements MemoryCMARW, MemoryCARW, MemoryMAR {
    private static final Log LOG = LogFactory.getLog(MemoryCMARWImpl.class);
    private final Long256Acceptor long256Acceptor = this::putLong256;
    private long appendAddress = 0;
    private long extendSegmentMsb;
    private int madviseOpts = -1;
    private int memoryTag = MemoryTag.MMAP_DEFAULT;
    private long minMappedMemorySize = -1;

    public MemoryCMARWImpl(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, long opts) {
        of(ff, name, extendSegmentSize, size, memoryTag, opts, -1);
    }

    public MemoryCMARWImpl() {
    }

    @Override
    public long appendAddressFor(long bytes) {
        checkAndExtend(appendAddress + bytes);
        final long result = appendAddress;
        appendAddress += bytes;
        return result;
    }

    @Override
    public long appendAddressFor(long offset, long bytes) {
        checkAndExtend(pageAddress + offset + bytes);
        return pageAddress + offset;
    }

    @Override
    public void close(boolean truncate, byte truncateMode) {
        if (pageAddress != 0) {
            final long truncateSize;
            if (truncate) {
                long appendOffset = getAppendOffset();
                truncateSize = truncateMode == Vm.TRUNCATE_TO_PAGE ? Files.ceilPageSize(appendOffset) : appendOffset;

                long sz = Math.min(size, truncateSize);
                if (appendOffset < sz) {
                    try {
                        // If this is a lazy close of unused memory the underlying file can already be truncated
                        //  using another fd and memset can lead to SIGBUS on Linux.
                        // Check the physical file length before trying to memset to the mapped memory.
                        sz = Math.min(sz, ff.length(fd));
                        if (appendOffset < sz) {
                            Vect.memset(pageAddress + appendOffset, sz - appendOffset, 0);
                        }
                    } catch (CairoException e) {
                        LOG.error().$("cannot determine file length to safely truncate [fd=").$(fd)
                                .$(", errno=").$(e.getErrno())
                                .$(", error=").$(e.getFlyweightMessage())
                                .I$();
                    }
                }
            } else {
                truncateSize = -1L;
            }
            ff.munmap(pageAddress, size, memoryTag);
            this.pageAddress = 0;
            try {
                Vm.bestEffortClose(ff, LOG, fd, truncateSize, truncateMode);
            } finally {
                fd = -1;
            }
        }
        if (ff != null && ff.close(fd)) {
            LOG.debug().$("closed [fd=").$(fd).$(']').$();
            fd = -1;
        }
        size = 0;
        ff = null;
    }

    @Override
    public void close() {
        close(true);
    }

    @Override
    public void extend(long newSize) {
        if (newSize > size) {
            extend0(newSize);
        }
    }

    @Override
    public long getAppendAddress() {
        return appendAddress;
    }

    @Override
    public long getAppendAddressSize() {
        return lim - appendAddress;
    }

    @Override
    public long getAppendOffset() {
        return appendAddress - pageAddress;
    }

    @Override
    public long getExtendSegmentSize() {
        return extendSegmentMsb;
    }

    @Override
    public void jumpTo(long offset) {
        checkAndExtend(pageAddress + offset);
        appendAddress = pageAddress + offset;
        assert appendAddress <= lim;
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, int memoryTag, long opts) {
        of(ff, name, extendSegmentSize, -1, memoryTag, opts);
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, long opts, int madviseOpts) {
        this.extendSegmentMsb = Numbers.msb(extendSegmentSize);
        this.minMappedMemorySize = extendSegmentSize;
        this.madviseOpts = madviseOpts;
        openFile(ff, name, opts);
        try {
            map(ff, name, size, memoryTag);
        } catch (Throwable th) {
            ff.close(fd);
            throw th;
        }
    }

    @Override
    public void of(FilesFacade ff, int fd, @Nullable LPSZ fileName, long size, int memoryTag) {
        close();
        assert fd > 0;
        this.ff = ff;
        this.extendSegmentMsb = ff.getMapPageSize();
        this.minMappedMemorySize = this.extendSegmentMsb;
        this.fd = fd;
        map(ff, fileName, size, memoryTag);
    }

    @Override
    public void of(FilesFacade ff, int fd, @Nullable LPSZ fileName, long extendSegmentSize, long size, int memoryTag) {
        of(ff, fd, null, size, memoryTag);
        this.extendSegmentMsb = Numbers.msb(extendSegmentSize);
    }

    @Override
    public void putLong256(@NotNull CharSequence hexString, int start, int end) {
        putLong256(hexString, start, end, long256Acceptor);
    }

    @Override
    public void setTruncateSize(long size) {
        jumpTo(size);
    }

    @Override
    public void skip(long bytes) {
        checkAndExtend(appendAddress + bytes);
        appendAddress += bytes;
    }

    @Override
    public void switchTo(int fd, long offset, boolean truncate, byte truncateMode) {
        close(truncate, truncateMode);
        this.fd = fd;
        map(ff, null, offset, memoryTag);
    }

    public void sync(boolean async) {
        ff.msync(pageAddress, size, async);
    }

    @Override
    public void truncate() {
        if (pageAddress != 0) {
            // try to remap to min size
            final long fileSize = ff.length(fd);
            long sz = Math.min(fileSize, minMappedMemorySize);
            try {
                // we are remapping file to make it smaller, should not need
                // to allocate space; we already have it
                this.pageAddress = TableUtils.mremap(
                        ff,
                        fd,
                        pageAddress,
                        size,
                        sz,
                        Files.MAP_RW,
                        memoryTag
                );
            } catch (Throwable e) {
                appendAddress = pageAddress;
                long truncatedToSize = Vm.bestEffortTruncate(ff, LOG, fd, 0);
                if (truncatedToSize != 0) {
                    if (truncatedToSize > 0) {
                        Vect.memset(pageAddress, truncatedToSize, 0);
                        this.size = sz;
                    } else {
                        Vect.memset(pageAddress, size, 0);
                    }
                    this.lim = pageAddress + size;
                }
                throw e;
            }

            this.size = sz;
            this.lim = pageAddress + sz;
            appendAddress = pageAddress;
            Vect.memset(pageAddress, sz, 0);

            // try to truncate the file to remove tail data
            if (ff.truncate(fd, Files.ceilPageSize(size))) {
                return;
            }

            // we could not truncate, this might happen on Windows when area of the same file is mapped
            // by another process

            long mem = TableUtils.mapRW(ff, fd, ff.length(fd), memoryTag);
            Vect.memset(mem + sz, fileSize - sz, 0);
            ff.munmap(mem, fileSize, memoryTag);
        }
    }

    @Override
    public void zero() {
        long baseLength = lim - pageAddress;
        Vect.memset(pageAddress, baseLength, 0);
    }

    private void checkAndExtend(long address) {
        if (address <= lim) {
            return;
        }
        extend0(address - pageAddress);
    }

    private void extend0(long newSize) {
        long nPages = (newSize >>> extendSegmentMsb) + 1;
        newSize = nPages << extendSegmentMsb;
        long offset = appendAddress - pageAddress;
        long previousSize = size;
        assert size > 0;
        TableUtils.allocateDiskSpace(ff, fd, newSize);
        try {
            this.pageAddress = TableUtils.mremap(
                    ff,
                    fd,
                    pageAddress,
                    previousSize,
                    newSize,
                    Files.MAP_RW,
                    memoryTag
            );
            ff.madvise(pageAddress, newSize, madviseOpts);
        } catch (Throwable e) {
            appendAddress = pageAddress + previousSize;
            close(false);
            throw e;
        }
        size = newSize;
        lim = pageAddress + newSize;
        appendAddress = pageAddress + offset;
    }

    private void map0(FilesFacade ff, long size) {
        try {
            this.pageAddress = TableUtils.mapRW(ff, fd, size, memoryTag);
            this.lim = pageAddress + size;
            ff.madvise(pageAddress, size, madviseOpts);
        } catch (Throwable e) {
            close(false);
            throw e;
        }
    }

    private void openFile(FilesFacade ff, LPSZ name, long opts) {
        close();
        this.ff = ff;
        fd = TableUtils.openFileRWOrFail(ff, name, opts);
    }

    protected void map(FilesFacade ff, @Nullable Utf8Sequence name, long size, int memoryTag) {
        this.memoryTag = memoryTag;
        // file either did not exist when length() was called or empty
        if (size < 1) {
            this.size = minMappedMemorySize;
            TableUtils.allocateDiskSpace(ff, fd, this.size);
            map0(ff, minMappedMemorySize);
            this.appendAddress = pageAddress;
        } else {
            this.size = size;
            map0(ff, size);
            this.appendAddress = pageAddress + size;
        }
        if (name != null) {
            LOG.debug().$("open [file=").$(name)
                    .$(", fd=").$(fd)
                    .$(", pageSize=").$(size)
                    .$(", size=").$(this.size)
                    .I$();
        } else {
            LOG.debug().$("open [fd=").$(fd)
                    .$(", pageSize=").$(size)
                    .$(", size=").$(this.size).I$();
        }
    }
}
