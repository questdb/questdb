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
import io.questdb.cairo.vm.api.MAMemory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;

public class MAMemoryImpl extends PagedVirtualMemory implements MAMemory {
    private static final Log LOG = LogFactory.getLog(MAMemoryImpl.class);
    private FilesFacade ff;
    private long fd = -1;
    private long pageAddress = 0;
    private int mappedPage;

    public MAMemoryImpl(FilesFacade ff, LPSZ name, long pageSize) {
        of(ff, name, pageSize);
    }

    public MAMemoryImpl() {
    }

    @Override
    public void close() {
        close(true);
    }

    @Override
    public long getPageAddress(int page) {
        if (page == mappedPage) {
            return pageAddress;
        }
        return 0L;
    }

    public void truncate() {
        if (fd == -1) {
            // are we closed ?
            return;
        }
        releaseCurrentPage();
        if (!ff.truncate(Math.abs(fd), getExtendSegmentSize())) {
            throw CairoException.instance(ff.errno()).put("Cannot truncate fd=").put(fd).put(" to ").put(getExtendSegmentSize()).put(" bytes");
        }
        updateLimits(0, pageAddress = mapPage(0));
        LOG.debug().$("truncated [fd=").$(fd).$(']').$();
    }

    @Override
    protected long mapWritePage(int page) {
        releaseCurrentPage();
        return pageAddress = mapPage(page);
    }

    @Override
    protected void release(long address) {
        ff.munmap(address, getPageSize());
    }

    public final void close(boolean truncate) {
        long sz = getAppendOffset();
        releaseCurrentPage();
        super.close();
        if (fd != -1) {
            try {
                VmUtils.bestEffortClose(ff, LOG, fd, truncate, sz, Files.PAGE_SIZE);
            } finally {
                fd = -1;
            }
        }
    }

    public void ensureFileSize(int page) {
        long target = pageOffset(page + 1);
        if (ff.length(fd) < target && !ff.allocate(fd, target)) {
            throw CairoException.instance(ff.errno()).put("Appender resize failed fd=").put(fd).put(", size=").put(target);
        }
    }

    public long getAppendAddress() {
        long appendOffset = getAppendOffset();
        return getPageAddress(pageIndex(appendOffset)) + offsetInPage(appendOffset);
    }

    public long getAppendAddressSize() {
        return getPageSize() - offsetInPage(getAppendOffset());
    }

    @Override
    public FilesFacade getFilesFacade() {
        return ff;
    }

    public long getFd() {
        return fd;
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size) {
        of(ff, name, extendSegmentSize);
    }

    @Override
    public void wholeFile(FilesFacade ff, LPSZ name) {
        throw new UnsupportedOperationException();
    }

    public long mapPage(int page) {
        // set page to "not mapped" in case mapping fails
        final long address = TableUtils.mapRW(ff, fd, getExtendSegmentSize(), pageOffset(page));
        mappedPage = page;
        return address;
    }

    public final void of(FilesFacade ff, LPSZ name, long extendSegmentSize) {
        close();
        this.ff = ff;
        mappedPage = -1;
        setExtendSegmentSize(extendSegmentSize);
        fd = TableUtils.openFileRWOrFail(ff, name);
        LOG.debug().$("open ").$(name).$(" [fd=").$(fd).$(", extendSegmentSize=").$(extendSegmentSize).$(']').$();
    }

    public void sync(boolean async) {
        if (pageAddress != 0) {
            if (ff.msync(pageAddress, getExtendSegmentSize(), async) == 0) {
                return;
            }
            LOG.error().$("could not msync [fd=").$(fd).$(", errno=").$(ff.errno()).$(']').$();
        }
    }

    void releaseCurrentPage() {
        if (pageAddress != 0) {
            release(pageAddress);
            pageAddress = 0;
        }
    }
}
