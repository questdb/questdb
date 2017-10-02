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
import com.questdb.misc.Files;
import com.questdb.misc.Os;
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

    @Override
    protected void release(long address) {
        ff.munmap(address, pageSize);
    }

    public final void close(boolean truncate) {
        long sz = getAppendOffset();
        super.close();
        releaseCurrentPage();
        if (fd != -1) {
            if (truncate) {
                ff.truncate(fd, sz);
                LOG.info().$("Truncated and closed [").$(fd).$(']').$();
            } else {
                LOG.info().$("Closed [").$(fd).$(']').$();
            }
            closeFd();
        }

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
        LOG.info().$("Open ").$(name).$(" [").$(fd).$(']').$();
    }

    public final void setSize(long size) {
        this.size = size;
        jumpTo(size);
    }

    public long size() {
        if (size < getAppendOffset()) {
            size = getAppendOffset();
        }
        return size;
    }

    public void truncate() {
        if (fd == -1) {
            // are we closed ?
            return;
        }

        this.size = 0;
        releaseCurrentPage();
        if (!ff.truncate(fd, pageSize)) {
            throw CairoException.instance(Os.errno()).put("Cannot truncate fd=").put(fd).put(" to ").put(pageSize).put(" bytes");
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
        long address = ff.mmap(fd, pageSize, offset, Files.MAP_RW);
        if (address == -1) {
            throw CairoException.instance(ff.errno()).put("Cannot mmap(append) fd=").put(fd).put(", offset=").put(offset).put(", size=").put(pageSize);
        }
        return address;
    }

    private void releaseCurrentPage() {
        if (pageAddress != 0) {
            release(pageAddress);
            pageAddress = 0;
        }
    }

    private void updateSize() {
        this.size = Math.max(this.size, getAppendOffset());
    }
}
