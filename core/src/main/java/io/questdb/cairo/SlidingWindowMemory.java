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

public class SlidingWindowMemory extends VirtualMemory {
    private static final Log LOG = LogFactory.getLog(SlidingWindowMemory.class);
    private FilesFacade ff;
    private long fd = -1;
    private long size = 0;
    private long pageAddress;
    private int pageIndex;
    private AppendMemory parent;

    @Override
    public void close() {
        super.close();
        closeFile();
        releasePage();
    }

    @Override
    public long getPageAddress(int page) {
        return page == pageIndex ? pageAddress : mapPage(page);
    }

    @Override
    protected long getPageSize(int page) {
        return getMapPageSize();
    }

    @Override
    protected long mapWritePage(int page) {
        throw new UnsupportedOperationException("Cannot jump() read-only memory. Use grow() instead.");
    }

    public long getFd() {
        return fd;
    }

    public void of(AppendMemory parent) {
        close();
        this.ff = parent.getFilesFacade();
        this.fd = parent.getFd();
        this.parent = parent;
        this.setPageSize(parent.getMapPageSize());
        updateSize();
        this.pageIndex = -1;
        LOG.info().$("open [fd=").$(fd).$(", size=").$(this.size).$(']').$();
    }

    public void updateSize() {
        if (parent != null) {
            this.size = pageOffset(pageIndex(parent.getAppendOffset())) + getMapPageSize();
        }
    }

    private void closeFile() {
        if (fd != -1) {
            LOG.info().$("closed [fd=").$(fd).$(']').$();
            fd = -1;
            this.size = 0;
            this.parent = null;
        }
    }

    private void invalidateCurrentPage() {
        this.pageAddress = 0;
        this.pageIndex = -1;
        clearHotPage();
    }

    private long mapPage(int page) {
        releaseCurrentPage();

        long offset = pageOffset(page);
        long sz = size - offset;

        if (sz > 0) {
            sz = getMapPageSize();

            long address = ff.mmap(fd, sz, offset, Files.MAP_RO);
            if (address == -1L) {
                invalidateCurrentPage();
                throw CairoException.instance(ff.errno()).put("Cannot map read-only page. fd=").put(fd).put(", offset=").put(offset).put(", size=").put(this.size).put(", page=").put(sz);
            }
            this.pageIndex = page;
            this.pageAddress = address;
            return address;
        }
        invalidateCurrentPage();
        throw CairoException.instance(ff.errno()).put("Trying to map read-only page outside of file boundary. fd=").put(fd).put(", offset=").put(offset).put(", size=").put(this.size).put(", page=").put(sz);
    }

    private void releaseCurrentPage() {
        if (pageAddress != 0) {
            ff.munmap(pageAddress, getMapPageSize());
        }
    }

    private void releasePage() {
        releaseCurrentPage();
        invalidateCurrentPage();
    }
}
