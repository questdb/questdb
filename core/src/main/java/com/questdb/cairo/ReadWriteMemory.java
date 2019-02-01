/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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
import com.questdb.std.str.LPSZ;

public class ReadWriteMemory extends VirtualMemory {
    private static final Log LOG = LogFactory.getLog(ReadWriteMemory.class);
    private FilesFacade ff;
    private long fd = -1;

    public ReadWriteMemory(FilesFacade ff, LPSZ name, long maxPageSize) {
        of(ff, name, maxPageSize);
    }

    public ReadWriteMemory() {
    }

    @Override
    public void close() {
        long size = getAppendOffset();
        super.close();
        if (isOpen()) {
            try {
                AppendMemory.bestEffortClose(ff, LOG, fd, true, size, getMapPageSize());
            } finally {
                fd = -1;
            }
        }
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
            throw CairoException.instance(ff.errno()).put("Cannot mmap read-write fd=").put(fd).put(", offset=").put(offset).put(", size=").put(pageSize);
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

    public boolean isOpen() {
        return fd != -1;
    }

    public final void of(FilesFacade ff, LPSZ name, long pageSize) {
        close();
        this.ff = ff;
        fd = ff.openRW(name);
        if (fd == -1) {
            throw CairoException.instance(ff.errno()).put("Cannot open file: ").put(name);
        }
        long size = ff.length(fd);
        setPageSize(pageSize);
        ensurePagesListCapacity(size);
        LOG.info().$("open ").$(name).$(" [fd=").$(fd).$(']').$();
        try {
            // we may not be able to map page here
            // make sure we close file before bailing out
            jumpTo(size);
        } catch (CairoException e) {
            ff.close(fd);
            fd = -1;
            throw e;
        }
    }
}
