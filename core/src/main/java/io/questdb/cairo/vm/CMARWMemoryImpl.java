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
import io.questdb.cairo.vm.api.CARWMemory;
import io.questdb.cairo.vm.api.CMARWMemory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CMARWMemoryImpl extends AbstractCRMemory implements CMARWMemory, CARWMemory {
    private static final Log LOG = LogFactory.getLog(CMARWMemoryImpl.class);
    private final Long256Acceptor long256Acceptor = this::putLong256;
    private long appendAddress = 0;
    private long minMappedMemorySize;
    private long extendSegmentMsb;

    @Override
    public void setTruncateSize(long size) {
        jumpTo(size);
    }

    @Override
    public void putBlockOfBytes(long offset, long from, long len) {
        throw new UnsupportedOperationException();
    }

    public CMARWMemoryImpl(FilesFacade ff, LPSZ name, long extendSegmentSize, long size) {
        of(ff, name, extendSegmentSize, size);
    }

    public CMARWMemoryImpl() {
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
    public void close() {
        if (pageAddress != 0) {
            ff.munmap(pageAddress, size);
            long truncateSize = getAppendOffset();
            this.pageAddress = 0;
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
        ff = null;
    }

    @Override
    public long getFd() {
        return fd;
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size) {
        this.extendSegmentMsb = Numbers.msb(extendSegmentSize);
        this.minMappedMemorySize = ff.getMapPageSize();
        openFile(ff, name);
        map(ff, name, size);
    }

    @Override
    public void extend(long newSize) {
        if (newSize > size) {
            extend0(newSize);
        }
    }

    @Override
    public void jumpTo(long offset) {
        checkAndExtend(pageAddress + offset);
        appendAddress = pageAddress + offset;
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
        return appendAddress - pageAddress;
    }

    @Override
    public void truncate() {
        grownLength = 0;
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
                        this.pageAddress,
                        this.size,
                        sz,
                        Files.MAP_RW
                );
            } catch (Throwable e) {
                appendAddress = pageAddress;
                long truncatedToSize = VmUtils.bestEffortTruncate(ff, LOG, fd, 0, Files.PAGE_SIZE);
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
            if (ff.truncate(fd, size)) {
                return;
            }

            // we could not truncate, this might happen on Windows when area of the same file is mapped
            // by another process

            long mem = TableUtils.mapRW(ff, fd, ff.length(fd));
            Vect.memset(mem + sz, fileSize - sz, 0);
            ff.munmap(mem, fileSize);
        }
    }

    public void of(FilesFacade ff, long fd, @Nullable CharSequence name, long size) {
        close();
        assert fd > 0;
        this.ff = ff;
        this.minMappedMemorySize = ff.getMapPageSize();
        this.fd = fd;
        map(ff, name, size);
    }

    public void sync(boolean async) {
        if (pageAddress != 0 && ff.msync(pageAddress, size, async) == 0) {
            return;
        }
        LOG.error().$("could not msync [fd=").$(fd).$(']').$();
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
                    this.pageAddress,
                    previousSize,
                    newSize,
                    Files.MAP_RW
            );
        } catch (Throwable e) {
            appendAddress = pageAddress + previousSize;
            close();
            throw e;
        }
        size = newSize;
        lim = pageAddress + newSize;
        appendAddress = pageAddress + offset;
        grownLength = newSize;
    }

    protected void map(FilesFacade ff, @Nullable CharSequence name, long size) {
        size = Math.min(ff.length(fd), size);
        if (size == 0) {
            this.size = minMappedMemorySize;
            TableUtils.allocateDiskSpace(ff, fd, this.size);
            map0(ff, minMappedMemorySize);
            this.appendAddress = pageAddress;
        } else {
            this.size = size;
            map0(ff, size);
            this.appendAddress = pageAddress + size;
        }
        LOG.debug().$("open ").$(name).$(" [fd=").$(fd).$(", pageSize=").$(size).$(", size=").$(this.size).$(']').$();
    }

    private void map0(FilesFacade ff, long size) {
        try {
            this.pageAddress = TableUtils.mapRW(ff, fd, size);
            this.lim = pageAddress + size;
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
