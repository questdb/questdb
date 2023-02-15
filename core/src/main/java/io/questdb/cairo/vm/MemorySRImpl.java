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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;

// "sliding" read only memory that reads file descriptor used for append
public class MemorySRImpl extends MemoryPARWImpl {
    private static final Log LOG = LogFactory.getLog(MemorySRImpl.class);
    private int fd = -1;
    private FilesFacade ff;
    private long pageAddress;
    private int pageIndex;
    private MemoryMA parent;
    private long size = 0;

    @Override
    public void close() {
        super.close();
        closeFile();
        releasePage();
    }

    public int getFd() {
        return fd;
    }

    @Override
    public long getPageAddress(int page) {
        return page == pageIndex ? pageAddress : mapPage(page);
    }

    @Override
    public long getPageSize() {
        return getExtendSegmentSize();
    }

    public void of(MemoryMA parent, int memoryTag) {
        close();
        this.memoryTag = memoryTag;
        this.ff = parent.getFilesFacade();
        this.fd = parent.getFd();
        this.parent = parent;
        this.setExtendSegmentSize(parent.getExtendSegmentSize());
        updateSize();
        this.pageIndex = -1;
        LOG.debug().$("open [fd=").$(fd).$(", size=").$(this.size).$(']').$();
    }

    public void updateSize() {
        if (parent != null) {
            this.size = pageOffset(pageIndex(parent.getAppendOffset())) + getExtendSegmentSize();
        }
    }

    private void closeFile() {
        if (fd != -1) {
            LOG.debug().$("closed [fd=").$(fd).$(']').$();
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
            try {
                long address = TableUtils.mapRO(ff, fd, getExtendSegmentSize(), offset, memoryTag);
                this.pageIndex = page;
                this.pageAddress = address;
                return address;
            } catch (Throwable e) {
                invalidateCurrentPage();
                throw e;
            }
        }
        invalidateCurrentPage();
        throw CairoException.critical(ff.errno()).put("Trying to map read-only page outside of file boundary. fd=").put(fd).put(", offset=").put(offset).put(", size=").put(this.size).put(", page=").put(sz);
    }

    private void releaseCurrentPage() {
        if (pageAddress != 0) {
            ff.munmap(pageAddress, getExtendSegmentSize(), memoryTag);
            pageAddress = 0;
        }
    }

    private void releasePage() {
        releaseCurrentPage();
        invalidateCurrentPage();
    }

    @Override
    protected long mapWritePage(int page, long offset) {
        throw new UnsupportedOperationException("Cannot jump() read-only memory. Use extend() instead.");
    }
}
