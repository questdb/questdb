/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import org.jetbrains.annotations.Nullable;

// paged mapped appendable readable writable
public class MemoryPMARWImpl extends MemoryPARWImpl implements MemoryMARW {
    private static final Log LOG = LogFactory.getLog(MemoryPMARWImpl.class);
    private boolean closeFdOnClose = true;
    private long fd = -1;
    private FilesFacade ff;
    private int madviseOpts = -1;

    @Override
    public void close(boolean truncate, byte truncateMode) {
        final long truncateSize = truncate ? getAppendOffset() : -1L;
        super.close();
        if (fd != -1) {
            try {
                if (closeFdOnClose) {
                    Vm.bestEffortClose(ff, LOG, fd, truncateSize, truncateMode);
                } else if (truncate) {
                    Vm.bestEffortTruncate(ff, LOG, fd, truncateSize, truncateMode);
                }
            } finally {
                fd = -1;
            }
        }
        ff = null;
    }

    @Override
    public void close() {
        close(true);
    }

    @Override
    public long detachFdClose() {
        try {
            final long fd = this.fd;
            closeFdOnClose = false;
            close();
            assert this.fd == -1;
            return fd;
        } finally {
            closeFdOnClose = true;
        }
    }

    @Override
    public long getFd() {
        return fd;
    }

    @Override
    public FilesFacade getFilesFacade() {
        return ff;
    }

    @Override
    public long getPageAddress(int page) {
        if (page < pages.size()) {
            final long address = pages.getQuick(page);
            if (address != 0) {
                return address;
            }
        }
        return mapPage(page);
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, int memoryTag, int opts) {
        of(ff, name, extendSegmentSize, -1, memoryTag, opts, -1);
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, int opts, int madviseOpts) {
        close();
        this.memoryTag = memoryTag;
        this.madviseOpts = madviseOpts;
        this.ff = ff;
        this.closeFdOnClose = true;
        setExtendSegmentSize(extendSegmentSize);
        fd = TableUtils.openFileRWOrFail(ff, name, opts);
        if (size > 0) {
            jumpTo(size);
        }
        LOG.debug().$("open ").$(name).$(" [fd=").$(fd).$(", extendSegmentSize=").$(extendSegmentSize).$(']').$();
    }

    @Override
    public void of(FilesFacade ff, long fd, @Nullable LPSZ fileName, long size, int memoryTag) {
        close();
        assert fd > 0;
        this.ff = ff;
        this.fd = fd;
        this.closeFdOnClose = true;
        this.memoryTag = memoryTag;
        this.madviseOpts = -1;
        setExtendSegmentSize(ff.getMapPageSize());
        if (size > 0) {
            jumpTo(size);
        }
    }

    @Override
    public void of(FilesFacade ff, long fd, boolean keepFdOpen, @Nullable LPSZ fileName, long extendSegmentSize, long size, int memoryTag) {
        close();
        assert fd > 0;
        this.ff = ff;
        this.fd = fd;
        this.closeFdOnClose = !keepFdOpen;
        this.memoryTag = memoryTag;
        this.madviseOpts = -1;
        setExtendSegmentSize(extendSegmentSize);
        if (size > 0) {
            jumpTo(size);
        }
    }

    @Override
    public void switchTo(FilesFacade ff, long fd, long extendSegmentSize, long offset, boolean truncate, byte truncateMode) {
        close(truncate, truncateMode);
        this.ff = ff;
        this.fd = fd;
        this.closeFdOnClose = true;
        this.madviseOpts = -1;
        setExtendSegmentSize(extendSegmentSize);
        if (offset > 0) {
            jumpTo(offset);
        }
    }

    @Override
    public void setTruncateSize(long size) {
        jumpTo(size);
    }

    @Override
    public void sync(boolean async) {
        if (fd == -1) {
            return;
        }
        final long pageSize = getPageSize();
        for (int i = 0, n = pages.size(); i < n; i++) {
            final long address = pages.getQuick(i);
            if (address != 0) {
                ff.msync(address, pageSize, async);
            }
        }
    }

    @Override
    public void truncate() {
        if (fd == -1) {
            return;
        }
        super.close();
        if (!ff.truncate(Math.abs(fd), 0)) {
            throw CairoException.critical(ff.errno()).put("Cannot truncate fd=").put(fd).put(" to 0 bytes");
        }
    }

    @Override
    protected long mapWritePage(int page, long offset) {
        if (page < pages.size()) {
            final long address = pages.getQuick(page);
            if (address != 0) {
                return address;
            }
        }
        return mapPage(page);
    }

    @Override
    protected void release(long address) {
        if (address != 0) {
            ff.munmap(address, getPageSize(), memoryTag);
        }
    }

    private long mapPage(int page) {
        if (fd == -1) {
            return 0;
        }
        final long address = TableUtils.mapRW(ff, fd, getExtendSegmentSize(), pageOffset(page), memoryTag);
        ff.madvise(address, getExtendSegmentSize(), madviseOpts);
        return cachePageAddress(page, address);
    }
}
