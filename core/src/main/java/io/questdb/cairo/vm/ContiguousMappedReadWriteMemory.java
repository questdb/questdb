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

import io.questdb.cairo.TableUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ContiguousMappedReadWriteMemory extends AbstractContiguousMemory
        implements MappedReadWriteMemory, ContiguousReadWriteVirtualMemory {
    private static final Log LOG = LogFactory.getLog(ContiguousMappedReadWriteMemory.class);
    private final Long256Acceptor long256Acceptor = this::putLong256;
    protected long page = 0;
    protected FilesFacade ff;
    protected long fd = -1;
    protected long size = 0;
    protected long appendAddress;
    private long minMappedMemorySize;
    private long grownLength;
    private long extendSegmentMsb;

    public ContiguousMappedReadWriteMemory(FilesFacade ff, LPSZ name, long pageSize, long size) {
        of(ff, name, pageSize, size);
    }

    public ContiguousMappedReadWriteMemory() {
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
        checkAndExtend(page + offset + bytes);
        return page + offset;
    }

    public void sync(boolean async) {
        if (page != 0 && ff.msync(page, size, async) == 0) {
            return;
        }
        LOG.error().$("could not msync [fd=").$(fd).$(']').$();
    }

    @Override
    public void close() {
        if (page != 0) {
            ff.munmap(page, size);
            long truncateSize = getAppendOffset();
            this.page = 0;
            try {
                VmUtils.bestEffortClose(ff, LOG, fd, true, truncateSize, Files.PAGE_SIZE);
            } finally {
                fd = -1;
            }
        }
        if (fd != -1) {
            ff.close(fd);
            LOG.debug().$("closed [fd=").$(fd).$(']').$();
            fd = -1;
        }
        grownLength = 0;
        size = 0;
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size) {
        this.extendSegmentMsb = Numbers.msb(extendSegmentSize);
        this.minMappedMemorySize = ff.getMapPageSize();
        openFile(ff, name);
        map(ff, name, size);
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize) {
        of(ff, name, extendSegmentSize, Long.MAX_VALUE);
    }

    @Override
    public boolean isDeleted() {
        return !ff.exists(fd);
    }

    @Override
    public long getFd() {
        return fd;
    }

    public long getGrownLength() {
        return grownLength;
    }

    @Override
    public long getPageAddress(int pageIndex) {
        return page;
    }

    @Override
    public int getPageCount() {
        return page == 0 ? 0 : 1;
    }

    @Override
    public void extend(long newSize) {
        if (newSize > grownLength) {
            grownLength = newSize;
        }

        if (newSize > size) {
            extend0(newSize);
        }
    }

    public long size() {
        return size;
    }

    public long addressOf(long offset) {
        assert offset <= size : "offset=" + offset + ", size=" + size + ", fd=" + fd;
        return page + offset;
    }

    @Override
    public void growToFileSize() {
        extend(ff.length(fd));
    }

    @Override
    public void jumpTo(long offset) {
        checkAndExtend(page + offset);
        appendAddress = page + offset;
    }

    @Override
    public void putLong256(@NotNull CharSequence hexString, int start, int end) {
        putLong256(hexString, start, end, long256Acceptor);
    }

    @Override
    public void skip(long bytes) {
        checkAndExtend(appendAddress + bytes);
        appendAddress += bytes;
    }

    @Override
    public long getAppendOffset() {
        return appendAddress - page;
    }

    @Override
    public void truncate() {
        grownLength = 0;
        long fileSize = ff.length(fd);
        if (page != 0) {
            // try to remap to min size
            long sz = Math.min(fileSize, minMappedMemorySize);
            try {
                this.page = TableUtils.mremap(
                        ff,
                        fd,
                        this.page,
                        this.size,
                        sz,
                        Files.MAP_RW
                );
            } catch (Throwable e) {
                jumpTo(0);
                close();
                throw e;
            }

            this.size = sz;
            Vect.memset(page, sz, 0);

            // try to truncate the file to remove tail data
            if (ff.truncate(fd, size)) {
                return;
            }

            // we could not truncate, this might happen on Windows when area of the same file is mapped
            // by another process

            long mem = TableUtils.mapRW(ff, fd, ff.length(fd));
            Vect.memset(mem + sz, fileSize - sz, 0);
            ff.munmap(mem, fileSize);
            return;
        }

        // we did not have anything mapped, try to truncate to zero if we can
        if (fileSize > 0) {
            ff.truncate(fd, 0);
        }
    }

    public void of(FilesFacade ff, long fd, @Nullable CharSequence name, long size) {
        close();
        this.ff = ff;
        this.minMappedMemorySize = ff.getMapPageSize();
        this.fd = fd;
        if (fd != -1) {
            map(ff, name, size);
        }
    }

    private void checkAndExtend(long address) {
        if (address <= page + size) {
            return;
        }
        extend(address - page);
    }

    private void extend0(long newSize) {
        long nPages = (newSize >>> extendSegmentMsb) + 1;
        newSize = nPages << extendSegmentMsb;
        long offset = appendAddress - page;
        long previousSize = size;
        TableUtils.allocateDiskSpace(ff, fd, newSize);
        if (previousSize > 0) {
            try {
                this.page = TableUtils.mremap(
                        ff,
                        fd,
                        this.page,
                        previousSize,
                        newSize,
                        Files.MAP_RW
                );
            } catch (Throwable e) {
                jumpTo(previousSize);
                close();
                throw e;
            }
        } else {
            try {
                page = TableUtils.mapRW(ff, fd, newSize);
            } catch (Throwable e) {
                close();
                throw e;
            }
        }
        size = newSize;
        appendAddress = page + offset;
    }

    protected void map(FilesFacade ff, @Nullable CharSequence name, long size) {
        size = Math.min(ff.length(fd), size);
        if (size == 0) {
            this.size = minMappedMemorySize;
            TableUtils.allocateDiskSpace(ff, fd, this.size);
            map0(ff, minMappedMemorySize);
            this.appendAddress = page;
        } else if (size > 0) {
            this.size = size;
            map0(ff, size);
            this.appendAddress = page + size;
        } else {
            this.page = 0;
            this.appendAddress = 0;
        }
        LOG.debug().$("open ").$(name).$(" [fd=").$(fd).$(", pageSize=").$(size).$(", size=").$(this.size).$(']').$();
    }

    private void map0(FilesFacade ff, long size) {
        try {
            this.page = TableUtils.mapRW(ff, fd, size);
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    private void openFile(FilesFacade ff, LPSZ name) {
        close();
        this.ff = ff;
        fd = TableUtils.openFileRWOrFail(ff, name);
    }
}
