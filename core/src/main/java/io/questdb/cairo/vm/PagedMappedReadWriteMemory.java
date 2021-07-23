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
import io.questdb.cairo.vm.api.MARWMemory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;

public class PagedMappedReadWriteMemory extends PagedVirtualMemory implements MARWMemory {
    private static final Log LOG = LogFactory.getLog(PagedMappedReadWriteMemory.class);
    private FilesFacade ff;
    private long fd = -1;

    @Override
    public void setTruncateSize(long size) {
        jumpTo(size);
    }

    public PagedMappedReadWriteMemory(FilesFacade ff, LPSZ name, long maxPageSize) {
        of(ff, name, maxPageSize);
    }

    public PagedMappedReadWriteMemory() {
    }

    @Override
    public void close() {
        long size = getAppendOffset();
        super.close();
        if (isOpen()) {
            try {
                VmUtils.bestEffortClose(ff, LOG, fd, true, size, Files.PAGE_SIZE);
            } finally {
                fd = -1;
            }
        }
    }

    @Override
    public FilesFacade getFilesFacade() {
        return ff;
    }

    @Override
    public long getPageAddress(int page) {
        return mapWritePage(page);
    }

    public void truncate() {
        // We may have many pages papped. Keep one, unmap all others and
        // truncate file to the size of first page
        final long firstPage = getPageAddress(0);
        final long pageSize = getExtendSegmentSize();
        Vect.memset(firstPage, pageSize, 0);
        releaseAllPagesButFirst();
        jumpTo(0);
        long fileSize = ff.length(fd);
        if (fileSize > pageSize) {
            if (ff.truncate(fd, pageSize)) {
                return;
            }

            // we could not truncate the file; we have to clear it via memory mapping
            long mem = ff.mmap(fd, fileSize, 0, Files.MAP_RW);
            Vect.memset(mem + pageSize, fileSize - pageSize, 0);
            ff.munmap(mem, fileSize);
            LOG.debug().$("could not truncate, zeroed [fd=").$(fd).$(']').$();
        }
    }

    @Override
    protected long allocateNextPage(int page) {
        return TableUtils.mapRW(ff, fd, getExtendSegmentSize(), pageOffset(page));
    }

    @Override
    protected void release(long address) {
        ff.munmap(address, getPageSize());
    }

    public long getFd() {
        return fd;
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size) {
        close();
        this.ff = ff;
        fd = TableUtils.openFileRWOrFail(ff, name);
        of0(ff, name, extendSegmentSize, size);
    }

    @Override
    public void partialFile(FilesFacade ff, LPSZ name, long size) {
        of(ff, name, ff.getMapPageSize(), size);
    }

    @Override
    public void wholeFile(FilesFacade ff, LPSZ name) {
        of(ff, name, ff.getMapPageSize());
    }

    public final void of(FilesFacade ff, LPSZ name, long extendSegmentSize) {
        close();
        this.ff = ff;
        fd = TableUtils.openFileRWOrFail(ff, name);
        of0(ff, name, extendSegmentSize, ff.length(fd));
    }

    public final void of(FilesFacade ff, long fd, long pageSize) {
        close();
        this.ff = ff;
        this.fd = fd;
        long size = ff.length(fd);
        setExtendSegmentSize(pageSize);
        ensurePagesListCapacity(size);
        try {
            // we may not be able to map page here
            // make sure we close file before bailing out
            jumpTo(size);
        } catch (Throwable e) {
            ff.close(fd);
            this.fd = -1;
            throw e;
        }
    }

    public void sync(int pageIndex, boolean async) {
        if (ff.msync(pages.getQuick(pageIndex), getExtendSegmentSize(), async) == 0) {
            return;
        }
        LOG.error().$("could not msync [fd=").$(fd).$(']').$();
    }

    public void sync(boolean async) {
        for (int i = 0, n = pages.size(); i < n; i++) {
            sync(i, async);
        }
    }

    private void of0(FilesFacade ff, LPSZ name, long pageSize, long size) {
        setExtendSegmentSize(pageSize);
        ensurePagesListCapacity(size);
        LOG.debug().$("open ").$(name).$(" [fd=").$(fd).$(']').$();
        try {
            // we may not be able to map page here
            // make sure we close file before bailing out
            jumpTo(size);
        } catch (Throwable e) {
            ff.close(fd);
            fd = -1;
            throw e;
        }
    }
}
