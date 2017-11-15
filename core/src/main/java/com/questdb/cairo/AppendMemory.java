/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.Files;
import com.questdb.std.FilesFacade;
import com.questdb.std.Os;
import com.questdb.std.str.LPSZ;

public class AppendMemory extends VirtualMemory {
    private static final Log LOG = LogFactory.getLog(AppendMemory.class);
    private FilesFacade ff;
    private long fd = -1;
    private long pageAddress = 0;
    private long size;

    public AppendMemory(FilesFacade ff, LPSZ name, long pageSize) {
        of(ff, name, pageSize);
    }

    public AppendMemory() {
        size = 0;
    }

    @Override
    public void close() {
        close(true);
    }

    @Override
    public void jumpTo(long offset) {
        updateSize();
        super.jumpTo(offset);
    }

    @Override
    protected long mapWritePage(int page) {
        releaseCurrentPage();
        long address = mapPage(page);
        return pageAddress = address;
    }

    public final void close(boolean truncate) {
        long sz = getAppendOffset();
        releaseCurrentPage();
        super.close();
        if (fd != -1) {
            try {
                if (truncate) {
                    if (ff.truncate(fd, sz)) {
                        LOG.info().$("truncated and closed [fd=").$(fd).$(']').$();
                    } else {
                        if (!ff.supportsTruncateMappedFiles()) {
                            // Windows does truncate file if it has a mapped page somewhere, could be another handle and process.
                            // To make it work size needs to be rounded up to nearest page.
                            long n = sz / getMapPageSize();
                            if (ff.truncate(fd, (n + 1) * getMapPageSize())) {
                                LOG.info().$("truncated and closed, second attempt [fd=").$(fd).$(']').$();
                                return;
                            }
                        }
                        LOG.info().$("closed without truncate [fd=").$(fd).$(", errno=").$(Os.errno()).$(']').$();
                    }
                } else {
                    LOG.info().$("closed [fd=").$(fd).$(']').$();
                }
            } finally {
                closeFd();
            }
        }
    }

    public final void setSize(long size) {
        this.size = size;
        jumpTo(size);
    }

    public long getFd() {
        return fd;
    }

    public final void of(FilesFacade ff, LPSZ name, long pageSize) {
        close();
        this.ff = ff;
        setPageSize(pageSize);
        fd = ff.openRW(name);
        if (fd == -1) {
            throw CairoException.instance(ff.errno()).put("Cannot open ").put(name);
        }
        LOG.info().$("open ").$(name).$(" [fd=").$(fd).$(']').$();
    }

    public long size() {
        if (size < getAppendOffset()) {
            size = getAppendOffset();
        }
        return size;
    }

    @Override
    protected void release(int page, long address) {
        ff.munmap(address, getPageSize(page));
    }

    public void truncate() {
        if (fd == -1) {
            // are we closed ?
            return;
        }

        this.size = 0;
        releaseCurrentPage();
        if (!ff.truncate(fd, getMapPageSize())) {
            throw CairoException.instance(Os.errno()).put("Cannot truncate fd=").put(fd).put(" to ").put(getMapPageSize()).put(" bytes");
        }
        updateLimits(0, pageAddress = mapPage(0));
    }

    private void closeFd() {
        ff.close(fd);
        fd = -1;
    }

    private long mapPage(int page) {
        long target = pageOffset(page + 1);
        if (ff.length(fd) < target && !ff.truncate(fd, target)) {
            throw CairoException.instance(ff.errno()).put("Appender resize failed fd=").put(fd).put(", size=").put(target);
        }
        long offset = pageOffset(page);
        long address = ff.mmap(fd, getMapPageSize(), offset, Files.MAP_RW);
        if (address == -1) {
            throw CairoException.instance(ff.errno()).put("Cannot mmap(append) fd=").put(fd).put(", offset=").put(offset).put(", size=").put(getMapPageSize());
        }
        return address;
    }

    private void releaseCurrentPage() {
        if (pageAddress != 0) {
            release(0, pageAddress);
            pageAddress = 0;
        }
    }

    private void updateSize() {
        this.size = Math.max(this.size, getAppendOffset());
    }
}
