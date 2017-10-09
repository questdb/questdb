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

import com.questdb.misc.Files;
import com.questdb.std.str.LPSZ;

public class ReadWriteMemory extends VirtualMemory {
    private final FilesFacade ff;
    private long fd = -1;
    private long size;

    public ReadWriteMemory(FilesFacade ff, LPSZ name, long maxPageSize, long size, long defaultPageSize) {
        this(ff);
        of(name, maxPageSize, size, defaultPageSize);
    }

    public ReadWriteMemory(FilesFacade ff) {
        this.ff = ff;
        size = 0;
    }

    @Override
    public void close() {
        long size = size();
        super.close();
        if (fd != -1) {
            ff.truncate(fd, size);
            ff.close(fd);
            fd = -1;
        }
    }

    @Override
    public void jumpTo(long offset) {
        this.size = Math.max(this.size, getAppendOffset());
        super.jumpTo(offset);
    }

    @Override
    protected long allocateNextPage(int page) {
        long address;
        long offset = pageOffset(page);
        final long pageSize = getMapPageSize();

        if (ff.length(fd) < offset + pageSize) {
            ff.truncate(fd, offset + pageSize);
        }

        address = ff.mmap(fd, pageSize, offset, Files.MAP_RW);

        if (address == -1) {
            throw CairoException.instance(ff.errno()).put("Cannot mmap(RW) fd=").put(fd).put(", offset").put(offset).put(", size").put(pageSize);
        }
        return address;
    }

    @Override
    protected long getPageAddress(int page) {
        return mapWritePage(page);
    }

    @Override
    protected void release(int page, long address) {
        ff.munmap(address, getPageSize(page));
    }

    public long getFd() {
        return fd;
    }

    public final void of(LPSZ name, long maxPageSize, long size, long defaultPageSize) {
        close();

        fd = ff.openRW(name);
        if (fd == -1) {
            throw CairoException.instance(ff.errno()).put("Cannot open file: ").put(name);
        }
        configurePageSize(size, defaultPageSize, maxPageSize);
    }

    public long size() {
        if (size < getAppendOffset()) {
            size = getAppendOffset();
        }
        return size;
    }

    protected final void configurePageSize(long size, long defaultPageSize, long maxPageSize) {
        if (size > maxPageSize) {
            setPageSize(maxPageSize);
        } else {
            setPageSize(Math.max(defaultPageSize, (size / ff.getPageSize()) * ff.getPageSize()));
        }
        pages.ensureCapacity((int) (size / getMapPageSize() + 1));
        this.size = size;
    }

}
