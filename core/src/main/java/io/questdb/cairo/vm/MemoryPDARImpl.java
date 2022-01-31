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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;

public class MemoryPDARImpl extends MemoryPARWImpl implements MemoryMAR {
    private static final Log LOG = LogFactory.getLog(MemoryPDARImpl.class);
    private FilesFacade ff;
    private long fd = -1;
    private final long pageAddress;
    private int mappedPage;

    public MemoryPDARImpl(FilesFacade ff, LPSZ name, long pageSize, int memoryTag) {
        this.pageAddress = Unsafe.malloc(pageSize, memoryTag);
        of(ff, name, pageSize, memoryTag);
    }

    @Override
    public final void close(boolean truncate) {
        long sz = getAppendOffset();
        flushPage();
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
        if (pageAddress != 0) {
            if (ff.msync(pageAddress, getExtendSegmentSize(), async) == 0) {
                return;
            }
            LOG.error().$("could not msync [fd=").$(fd).$(", errno=").$(ff.errno()).$(']').$();
        }
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
        flushPage();
        if (!ff.truncate(Math.abs(fd), getExtendSegmentSize())) {
            throw CairoException.instance(ff.errno()).put("Cannot truncate fd=").put(fd).put(" to ").put(getExtendSegmentSize()).put(" bytes");
        }
        updateLimits(0, pageAddress);
        LOG.debug().$("truncated [fd=").$(fd).$(']').$();
    }

    @Override
    protected long mapWritePage(int page) {
        flushPage();
        mappedPage = page;
        return pageAddress;
    }

    @Override
    protected void release(long address) {
        ff.munmap(address, getPageSize(), memoryTag);
    }

    @Override
    public FilesFacade getFilesFacade() {
        return ff;
    }

    public long getFd() {
        return fd;
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag) {
        of(ff, name, extendSegmentSize, memoryTag);
    }

    @Override
    public void wholeFile(FilesFacade ff, LPSZ name, int memoryTag) {
        of(ff, name, ff.getMapPageSize(), memoryTag);
    }

    public final void of(FilesFacade ff, LPSZ name, long extendSegmentSize, int memoryTag) {
        this.memoryTag = memoryTag;
        this.ff = ff;
        close();
        mappedPage = -1;
        setExtendSegmentSize(extendSegmentSize);
        fd = TableUtils.openFileRWOrFail(ff, name);
        LOG.debug().$("open ").$(name).$(" [fd=").$(fd).$(", extendSegmentSize=").$(extendSegmentSize).$(']').$();
    }

    void flushPage() {
        if (mappedPage != -1) {
            final long offset = pageOffset(mappedPage);
            ff.write(fd, pageAddress, getAppendOffset() - offset, offset);
            mappedPage = -1;
        }
    }
}
