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
import com.questdb.std.str.LPSZ;

public class ReadOnlyMemory extends VirtualMemory implements ReadOnlyColumn {
    private static final Log LOG = LogFactory.getLog(ReadOnlyMemory.class);
    private FilesFacade ff;
    private long fd = -1;
    private long size = 0;
    private long lastPageSize;
    private long maxPageSize;

    public ReadOnlyMemory(FilesFacade ff, LPSZ name, long maxPageSize) {
        of(ff, name, maxPageSize);
    }

    public ReadOnlyMemory() {
    }

    @Override
    public void close() {
        super.close();
        if (fd != -1) {
            ff.close(fd);
            LOG.info().$("Closed [").$(fd).$(']').$();
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
        ff.munmap(address, pageSize);
    }

    @Override
    protected void releaseLast(long address) {
        if (address != 0) {
            ff.munmap(address, lastPageSize);
            lastPageSize = pageSize;
        }
    }

    public void of(FilesFacade ff, LPSZ name, long maxPageSize) {
        close();
        this.ff = ff;
        boolean exists = ff.exists(name);
        if (!exists) {
            throw CairoException.instance(0).put("File not found: ").put(name);
        }
        fd = ff.openRO(name);
        if (fd == -1) {
            throw CairoException.instance(ff.errno()).put("Cannot open file: ").put(name);
        }

        this.maxPageSize = maxPageSize;
        setInitialSize(ff.length(fd));
        LOG.info().$("Open ").$(name).$(" [").$(fd).$(']').$();
    }

    @Override
    public void trackFileSize() {
        long size = ff.length(fd);
        if (size < this.size) {
            setInitialSize(size);
        } else if (size > this.size && lastPageSize < pageSize) {
            pages.ensureCapacity((int) (size / this.pageSize + 1));
            int lastIndex = pages.size() - 1;
            if (lastIndex > -1) {
                long address = pages.getQuick(lastIndex);
                if (address != 0) {
                    releaseLast(address);
                    pages.setQuick(lastIndex, 0);
                }
                clearHotPage();
            }
            this.size = size;
        }
    }

    @Override
    public long getFd() {
        return fd;
    }

    private long mapPage(int page) {
        long address;
        long offset = pageOffset(page);
        long sz = size - offset;

        if (sz > 0) {
            if (sz > pageSize) {
                sz = pageSize;
            } else {
                this.lastPageSize = sz;
            }

            address = ff.mmap(fd, sz, offset, Files.MAP_RO);
        } else {
            address = -1L;
        }

        if (address == -1L) {
            throw CairoException.instance(ff.errno()).put("Cannot mmap(read) fd=").put(fd).put(", offset=").put(offset).put(", size=").put(sz);
        }
        cachePageAddress(page, address);
        return address;
    }

    private void setInitialSize(long size) {
        long targetPageSize;
        if (size > maxPageSize) {
            targetPageSize = maxPageSize;
        } else {
            targetPageSize = Math.max(ff.getPageSize(), (size / ff.getPageSize()) * ff.getPageSize());
        }

        if (targetPageSize != pageSize) {
            setPageSize(targetPageSize);
            pages.ensureCapacity((int) (size / this.pageSize + 1));
            this.lastPageSize = targetPageSize;
        }

        this.size = size;
    }
}
