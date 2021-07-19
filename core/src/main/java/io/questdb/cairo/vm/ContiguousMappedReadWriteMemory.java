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
    protected long page = -1;
    protected FilesFacade ff;
    protected long fd = -1;
    protected long size = 0;
    protected long appendAddress;
    private long grownLength;
    private long pageSizeMsb;

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

    @Override
    public void close() {
        if (page != -1) {
            ff.munmap(page, size);
            long truncateSize = getAppendOffset();
            this.page = -1;
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
    public void of(FilesFacade ff, LPSZ name, long pageSize, long size) {
        this.pageSizeMsb = Numbers.msb(pageSize);
        openFile(ff, name);
        map(ff, name, size);
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long pageSize) {
        this.pageSizeMsb = Numbers.msb(pageSize);
        openFile(ff, name);
        // open the whole file based on its length
        map(ff, name, Long.MAX_VALUE);
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
    public void extend(long newSize) {
        if (newSize > grownLength) {
            grownLength = newSize;
        }

        if (newSize > size) {
            setSize0(newSize);
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
        if (page != -1) {
            ff.munmap(page, size);
            this.size = 0;
            this.page = -1;
        }
        ff.truncate(fd, 0);
    }

    public void of(FilesFacade ff, long fd, @Nullable CharSequence name, long size) {
        close();
        this.ff = ff;
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

    protected void map(FilesFacade ff, @Nullable CharSequence name, long size) {
        size = Math.min(ff.length(fd), size);
        if (size == 0) {
            this.size = 16 * 1024 * 1024;
            TableUtils.allocateDiskSpace(ff, fd, this.size);
            map0(ff, name, 16 * 1024 * 1024);
            this.appendAddress = page;
        } else if (size > 0) {
            this.size = size;
            map0(ff, name, size);
            this.appendAddress = page + size;
        } else {
            this.page = -1;
            this.appendAddress = -1;
        }
        LOG.debug().$("open ").$(name).$(" [fd=").$(fd).$(", pageSize=").$(size).$(", size=").$(this.size).$(']').$();
    }

    private void map0(FilesFacade ff, @Nullable CharSequence name, long size) {
        this.page = ff.mmap(fd, size, 0, Files.MAP_RW);
        if (page == FilesFacade.MAP_FAILED) {
            long fd = this.fd;
            long fileLen = ff.length(fd);
            close();
            throw CairoException.instance(ff.errno())
                    .put("Could not mmap ").put(name)
                    .put(" [size=").put(size)
                    .put(", fd=").put(fd)
                    .put(", memUsed=").put(Unsafe.getMemUsed())
                    .put(", fileLen=").put(fileLen)
                    .put(']');
        }
    }

    private void openFile(FilesFacade ff, LPSZ name) {
        close();
        this.ff = ff;
        fd = TableUtils.openFileRWOrFail(ff, name);
    }

    private void setSize0(long newSize) {
        long nPages = (newSize >>> pageSizeMsb) + 1;
        newSize = nPages << pageSizeMsb;
        long offset = appendAddress - page;
        long previousSize = size;
        TableUtils.allocateDiskSpace(ff, fd, newSize);
        if (previousSize > 0) {
            long page = ff.mremap(fd, this.page, previousSize, newSize, 0, Files.MAP_RW);
            if (page == FilesFacade.MAP_FAILED) {
                long fd = this.fd;
                // Closing memory will truncate size to current append offset.
                // Since the failed resize can occur before append offset can be
                // explicitly set, we must assume that file size should be
                // equal to previous memory size
                jumpTo(previousSize);
                close();
                throw CairoException.instance(ff.errno()).put("Could not remap file [previousSize=").put(previousSize).put(", newSize=").put(newSize).put(", fd=").put(fd).put(']');
            }
            this.page = page;
        } else {
            if (page != -1) {
                System.out.println("ok");
            }
            assert page == -1;
            page = ff.mmap(fd, newSize, 0, Files.MAP_RW);
            if (page == FilesFacade.MAP_FAILED) {
                long fd = this.fd;
                close();
                throw CairoException.instance(ff.errno()).put("Could not remap file [previousSize=").put(previousSize).put(", newSize=").put(newSize).put(", fd=").put(fd).put(']');
            }
        }
        size = newSize;
        appendAddress = page + offset;
    }
}
