/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;

public class MemoryPDARImpl extends MemoryPARWImpl implements MemoryMAR {
    private static final Log LOG = LogFactory.getLog(MemoryPDARImpl.class);
    private long pageAddress;
    private long offsetInPage;
    private FilesFacade ff;
    private long fd = -1;
    private int pageIndex;

    public MemoryPDARImpl(FilesFacade ff, LPSZ name, long pageSize, int memoryTag, long opts) {
        this.pageAddress = Unsafe.malloc(pageSize, memoryTag);
        this.pageIndex = 0;
        this.offsetInPage = 0;
        of(ff, name, pageSize, memoryTag, opts);
    }

    public MemoryPDARImpl() {
        this.pageAddress = 0;
        setExtendSegmentSize(0);
    }

    @Override
    public final void close(boolean truncate) {
        long sz = getAppendOffset();
        if (pageAddress != 0) {
            flushPage();
            Unsafe.free(pageAddress, getExtendSegmentSize(), memoryTag);
            pageAddress = 0;
            pageIndex = -1;
        }
        super.close();
        if (fd != -1) {
            try {
                Vm.bestEffortClose(ff, LOG, fd, truncate, sz);
            } finally {
                fd = -1;
            }
        }
    }

    public void sync(boolean async) {
        if (ff.fsync(fd) == 0) {
            return;
        }
        LOG.error().$("could not fsync [fd=").$(fd).$(", errno=").$(ff.errno()).$(']').$();
    }

    public final void of(FilesFacade ff, LPSZ name, long extendSegmentSize, int memoryTag, long opts) {
        flushPage();
        if (this.ff != null) {
            final long sz = getAppendOffset();
            // this is not the first invocation
            realloc(extendSegmentSize, memoryTag, 0);
            super.close();
            if (fd != -1) {
                try {
                    Vm.bestEffortClose(ff, LOG, fd, true, sz);
                } finally {
                    fd = -1;
                }
            }
        } else {
            realloc(extendSegmentSize, memoryTag, 0);
        }
        this.memoryTag = memoryTag;
        this.ff = ff;
        setExtendSegmentSize(extendSegmentSize);
        long result;
        final long fd1 = ff.openRW(name, opts);
        if (fd1 > -1) {
            LOG.debug().$("open [file=").$(name).$(", fd=").$(fd1).$(']').$();
            result = fd1;
        } else {
            throw CairoException.instance(ff.errno()).put("could not open read-write [file=").put(name).put(']');
        }
        fd = result;
        LOG.debug().$("open ").$(name).$(" [fd=").$(fd).$(", extendSegmentSize=").$(extendSegmentSize).$(']').$();
    }

    @Override
    public void flush() {
        flushPage();
    }

    @Override
    public void close() {
        close(true);
    }

    public void truncate() {
        if (fd == -1) {
            // are we closed ?
            return;
        }
        flushPage();
        if (!ff.truncate(Math.abs(fd), getExtendSegmentSize())) {
            throw CairoException.instance(ff.errno()).put("Cannot truncate fd=").put(fd).put(" to ").put(getExtendSegmentSize()).put(" bytes");
        }
        updateLimits(0, pageAddress);
        LOG.debug().$("truncated [fd=").$(fd).$(']').$();
    }

    @Override
    public long getPageAddress(int page) {
        if (page == pageIndex) {
            return pageAddress;
        }
        return 0L;
    }

    @Override
    protected long mapWritePage(int page, long offset) {
        if (pageIndex == page) {
            if (getAppendOffset() == 0 && offset > 0) {
                // page has not been appended yet, but jump() must have been called to
                // set the append position
                this.offsetInPage = offsetInPage(offset - 1);
            }
            return pageAddress;
        }
        flushPage();
        this.pageIndex = page;
        this.offsetInPage = offsetInPage(offset - 1);
        return pageAddress;
    }

    @Override
    protected void release(long address) {
        assert false;
    }

    public long getFd() {
        return fd;
    }

    @Override
    public FilesFacade getFilesFacade() {
        return ff;
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, long opts) {
        of(ff, name, extendSegmentSize, memoryTag, opts);
    }

    @Override
    public void wholeFile(FilesFacade ff, LPSZ name, int memoryTag) {
        of(ff, name, ff.getMapPageSize(), memoryTag, CairoConfiguration.O_NONE);
    }

    void flushPage() {
        if (pageIndex > -1 && ff != null) {
            final long offset = pageOffset(pageIndex);
            ff.write(fd, pageAddress + offsetInPage, getAppendOffset() - offset - offsetInPage, offset+offsetInPage);
            // prevent double-flush
            pageIndex = -1;
        }
    }

    private void realloc(long extendSegmentSize, int memoryTag, int pageIndex) {
        if (pageAddress == 0 || extendSegmentSize > getExtendSegmentSize()) {
            if (pageAddress != 0) {
                Unsafe.free(pageAddress, getExtendSegmentSize(), this.memoryTag);
            }
            this.pageAddress = Unsafe.malloc(extendSegmentSize, memoryTag);
        }
        this.pageIndex = pageIndex;
        this.offsetInPage = 0;
    }
}
