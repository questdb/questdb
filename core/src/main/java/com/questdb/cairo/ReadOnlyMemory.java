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

public class ReadOnlyMemory extends VirtualMemory {
    private long fd = -1;
    private long size = 0;
    private long lastPageSize;

    public ReadOnlyMemory(LPSZ name, long maxPageSize, long size) {
        of(name, maxPageSize, size);
    }

    public ReadOnlyMemory() {
    }

    @Override
    public void close() {
        super.close();
        if (fd != -1) {
            Files.close(fd);
            fd = -1;
        }
    }

    @Override
    protected long getPageAddress(int page) {
        long address = super.getPageAddress(page);
        if (address != 0) {
            return address;
        }
        return mapPage(page);
    }

    @Override
    protected void release(long address) {
        Files.munmap(address, pageSize);
    }

    @Override
    protected void releaseLast(long address) {
        Files.munmap(address, lastPageSize);
    }

    public void of(LPSZ name, long maxPageSize, long size) {
        close();

        assert size > 0;

        this.size = size;
        this.lastPageSize = Files.PAGE_SIZE;

        boolean exists = Files.exists(name);
        if (!exists) {
            throw new RuntimeException("file not  found");
        }
        fd = Files.openRO(name);
        if (fd == -1) {
            throw new RuntimeException("cannot open file");
        }

        if (size > maxPageSize) {
            setPageSize(maxPageSize);
        } else {
            setPageSize(Math.max(Files.PAGE_SIZE, (size / Files.PAGE_SIZE) * Files.PAGE_SIZE));
        }
        pages.ensureCapacity((int) (size / this.pageSize + 1));
    }

    private long mapPage(int page) {
        long address;
        long offset = pageOffset(page);
        long sz = size - offset;
        if (sz > pageSize) {
            address = Files.mmap(fd, pageSize, offset, Files.MAP_RO);
        } else {
            address = Files.mmap(fd, sz, offset, Files.MAP_RO);
            this.lastPageSize = sz;
        }
        if (address == -1) {
            throw new RuntimeException("Cannot mmap");
        }
        cachePageAddress(page, address);
        return address;
    }
}
