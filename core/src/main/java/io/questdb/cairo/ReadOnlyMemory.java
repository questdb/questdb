/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;

public class ReadOnlyMemory extends VirtualMemory implements ReadOnlyColumn {
    private static final Log LOG = LogFactory.getLog(ReadOnlyMemory.class);
    private FilesFacade ff;
    private long fd = -1;
    private long size = 0;
    private long lastPageSize;
    private int lastPageIndex;
    private long pageSize;
    private long userSize = 0;

    public ReadOnlyMemory(FilesFacade ff, LPSZ name, long pageSize, long size) {
        of(ff, name, pageSize, size);
    }

    public ReadOnlyMemory() {
    }

    @Override
    public void close() {
        super.close();
        if (fd != -1) {
            ff.close(fd);
            LOG.info().$("closed [fd=").$(fd).$(']').$();
            fd = -1;
            this.size = 0;
            this.userSize = 0;
        }
    }

    @Override
    public long getPageAddress(int page) {
        long address = super.getPageAddress(page);
        if (address != 0) {
            return address;
        }
        return mapPage(page);
    }

    @Override
    public long getPageSize(int page) {
        // in some cases VirtualMemory.getPageSize() is called
        // before page is mapped, where lastPageIndex is set
        // if this is the case I need to test better
        if (page < lastPageIndex) {
            return super.getPageSize(page);
        }
        return lastPageSize;
    }

    @Override
    protected long mapWritePage(int page) {
        throw new UnsupportedOperationException("Cannot jump() read-only memory. Use grow() instead.");
    }

    @Override
    protected void release(int page, long address) {
        if (address != 0) {
            ff.munmap(address, getPageSize(page));
            if (page == lastPageIndex) {
                lastPageSize = getMapPageSize();
            }
        }
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long pageSize, long size) {
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

        this.pageSize = pageSize;
        grow(size);
        LOG.info().$("open ").$(name).$(" [fd=").$(fd).$(", pageSize=").$(pageSize).$(", size=").$(this.size).$(']').$();
    }

    @Override
    public long getFd() {
        return fd;
    }

    @Override
    public void grow(long size) {
        if (size > userSize) {
            userSize = size;
        }

        if (size > this.size) {
            final long fileSize = ff.length(fd);
            grow0(Math.max(size, fileSize));
        }
    }

    @Override
    public boolean isDeleted() {
        return !ff.exists(fd);
    }

    @Override
    public int getPageCount() {
        return pageIndex(userSize) + 1;
    }

    public long size() {
        return size;
    }

    private long computePageSize(long memorySize) {
        if (memorySize < pageSize) {
            return Math.max(ff.getPageSize(), (memorySize / ff.getPageSize()) * ff.getPageSize());
        }
        return pageSize;
    }

    private void grow0(final long size) {
        long targetPageSize = computePageSize(size);
        if (targetPageSize != getMapPageSize()) {
            setPageSize(targetPageSize);
            ensurePagesListCapacity(size);
            this.lastPageSize = Math.min(targetPageSize, size);
        } else {
            ensurePagesListCapacity(size);
            if (lastPageSize < getMapPageSize()) {
                int lastIndex = pages.size() - 1;
                if (lastIndex > -1) {
                    long address = pages.getQuick(lastIndex);
                    if (address != 0) {
                        release(lastIndex, address);
                        pages.setQuick(lastIndex, 0);
                    }
                    clearHotPage();
                }
                this.lastPageIndex = 0;
                this.lastPageSize = getMapPageSize();
            }
        }
        this.size = size;
    }

    private long mapPage(int page) {
        long address;
        long offset = pageOffset(page);
        long sz = size - offset;

        if (sz > 0) {
            if (sz < getMapPageSize()) {
                this.lastPageSize = sz;
                this.lastPageIndex = page;
            } else {
                sz = getMapPageSize();
            }

            address = ff.mmap(fd, sz, offset, Files.MAP_RO);
            return address == -1L ? recoverPageMapOrFail(page, offset, sz) : cachePageAddress(page, address);
        }
        throw CairoException.instance(ff.errno()).put("Trying to map read-only page outside of file boundary. fd=").put(fd).put(", offset=").put(offset).put(", size=").put(this.size).put(", page=").put(sz);
    }

    private long recoverPageMapOrFail(int page, long offset, long sz) {
        long address;
        // There is race condition where file may get truncated by writer between "size"
        // being set and call to mmap(). Linux and OSX will allow mapping of file beyond
        // its actual size, but on Windows we get out-of-memory error (errno=8). This
        // condition is caused by "optimistic" page sizing. See grow() implementation on
        // how size is set. To summarize - we may try to over-map the file but we will
        // never read more than actual data in the file - even if data size is smaller
        // than area we map.
        //
        // Ok, when this does happen we will try to adjust "size" down according to file
        // length and re-map the page. This is safe because page size is always "optimistic".

        if (ff.isRestrictedFileSystem() && ff.errno() == 8) {
            // re-read file size in case it has been truncated while we are reading it.
            this.size = Math.min(userSize, ff.length(fd));
            sz = this.size - offset;
            this.lastPageSize = sz;
            this.lastPageIndex = page;

            address = ff.mmap(fd, sz, offset, Files.MAP_RO);
            if (address != -1L) {
                return cachePageAddress(page, address);
            }
        }
        throw CairoException.instance(ff.errno()).put("Cannot mmap read-only fd=").put(fd).put(", offset=").put(offset).put(", size=").put(this.size).put(", page=").put(sz);
    }
}
