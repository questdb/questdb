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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;

//paged mapped appendable readable 
public class MemoryPMARImpl extends MemoryPARWImpl implements MemoryMAR {
    private static final Log LOG = LogFactory.getLog(MemoryPMARImpl.class);
    private FilesFacade ff;
    private long fd = -1;
    private long pageAddress = 0;
    private int mappedPage;

    public MemoryPMARImpl(FilesFacade ff, LPSZ name, long pageSize, int memoryTag, long opts) {
        of(ff, name, pageSize, memoryTag, opts);
    }

    public MemoryPMARImpl() {
    }

    @Override
    public final void close(boolean truncate) {
        this.close(truncate, Vm.TRUNCATE_TO_PAGE);
    }

    public final void close(boolean truncate, byte truncateMode) {
        long sz = getAppendOffset();
        releaseCurrentPage();
        super.close();
        if (fd != -1) {
            try {
                Vm.bestEffortClose(ff, LOG, fd, truncate, sz, truncateMode);
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

    public final void of(FilesFacade ff, LPSZ name, long extendSegmentSize, int memoryTag, long opts) {
        close();
        this.memoryTag = memoryTag;
        this.ff = ff;
        mappedPage = -1;
        setExtendSegmentSize(extendSegmentSize);
        fd = TableUtils.openFileRWOrFail(ff, name, opts);
        LOG.debug().$("open ").$(name).$(" [fd=").$(fd).$(", extendSegmentSize=").$(extendSegmentSize).$(']').$();
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
        releaseCurrentPage();
        if (!ff.truncate(Math.abs(fd), getExtendSegmentSize())) {
            throw CairoException.critical(ff.errno()).put("Cannot truncate fd=").put(fd).put(" to ").put(getExtendSegmentSize()).put(" bytes");
        }
        updateLimits(0, pageAddress = mapPage(0));
        LOG.debug().$("truncated [fd=").$(fd).$(']').$();
    }

    @Override
    public long getPageAddress(int page) {
        if (page == mappedPage) {
            return pageAddress;
        }
        return 0L;
    }

    @Override
    protected long mapWritePage(int page, long offset) {
        releaseCurrentPage();
        return pageAddress = mapPage(page);
    }

    @Override
    protected void release(long address) {
        ff.munmap(address, getPageSize(), memoryTag);
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

    public long mapPage(int page) {
        // set page to "not mapped" in case mapping fails
        final long address = TableUtils.mapRW(ff, fd, getExtendSegmentSize(), pageOffset(page), memoryTag);
        mappedPage = page;
        return address;
    }

    void releaseCurrentPage() {
        if (pageAddress != 0) {
            release(pageAddress);
            pageAddress = 0;
        }
    }
}
