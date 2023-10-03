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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import org.jetbrains.annotations.TestOnly;

// paged mapped appendable readable 
public class MemoryPMARImpl extends MemoryPARWImpl implements MemoryMAR {
    private static final Log LOG = LogFactory.getLog(MemoryPMARImpl.class);
    private final int commitMode;
    private int fd = -1;
    private FilesFacade ff;
    private int madviseOpts = -1;
    private int mappedPage;
    private long pageAddress = 0;

    @TestOnly
    public MemoryPMARImpl(FilesFacade ff, LPSZ name, long pageSize, int memoryTag, long opts) {
        this(CommitMode.NOSYNC);
        of(ff, name, pageSize, 0, memoryTag, opts, -1);
    }

    public MemoryPMARImpl(int commitMode) {
        this.commitMode = commitMode;
    }

    public final void close(boolean truncate, byte truncateMode) {
        long sz = truncate ? getAppendOffset() : -1L;
        releaseCurrentPage();
        super.close();
        if (fd != -1) {
            try {
                Vm.bestEffortClose(ff, LOG, fd, sz, truncateMode);
            } finally {
                fd = -1;
            }
        }
    }

    @Override
    public void close() {
        close(true);
    }

    public int getFd() {
        return fd;
    }

    @Override
    public FilesFacade getFilesFacade() {
        return ff;
    }

    @Override
    public long getPageAddress(int page) {
        if (page == mappedPage) {
            return pageAddress;
        }
        return 0L;
    }

    public long mapPage(int page) {
        // set page to "not mapped" in case mapping fails
        final long address = TableUtils.mapRW(ff, fd, getExtendSegmentSize(), pageOffset(page), memoryTag);
        mappedPage = page;
        ff.madvise(address, getExtendSegmentSize(), madviseOpts);
        return address;
    }

    @Override
    public final void of(FilesFacade ff, LPSZ name, long extendSegmentSize, int memoryTag, long opts) {
        of(ff, name, extendSegmentSize, 0, memoryTag, opts, -1);
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, long opts) {
        of(ff, name, extendSegmentSize, memoryTag, opts);
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, long opts, int madviseOpts) {
        close();
        this.memoryTag = memoryTag;
        this.madviseOpts = madviseOpts;
        this.ff = ff;
        mappedPage = -1;
        setExtendSegmentSize(extendSegmentSize);
        fd = TableUtils.openFileRWOrFail(ff, name, opts);
        LOG.debug().$("open ").$(name).$(" [fd=").$(fd).$(", extendSegmentSize=").$(extendSegmentSize).$(']').$();
    }

    @Override
    public void switchTo(int fd, long offset, boolean truncate, byte truncateMode) {
        close(truncate, truncateMode);
        this.fd = fd;
        jumpTo(offset);
    }

    public void sync(boolean async) {
        if (pageAddress != 0 && commitMode != CommitMode.NOSYNC) {
            ff.msync(pageAddress, getPageSize(), commitMode == CommitMode.ASYNC);
        }
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
    public void wholeFile(FilesFacade ff, LPSZ name, int memoryTag) {
        of(ff, name, ff.getMapPageSize(), 0, memoryTag, CairoConfiguration.O_NONE, -1);
    }

    @Override
    protected long mapWritePage(int page, long offset) {
        releaseCurrentPage();
        return pageAddress = mapPage(page);
    }

    @Override
    protected void release(long address) {
        if (commitMode != CommitMode.NOSYNC) {
            ff.msync(address, getPageSize(), commitMode == CommitMode.ASYNC);
        }
        ff.munmap(address, getPageSize(), memoryTag);
    }

    void releaseCurrentPage() {
        if (pageAddress != 0) {
            release(pageAddress);
            pageAddress = 0;
        }
    }
}
