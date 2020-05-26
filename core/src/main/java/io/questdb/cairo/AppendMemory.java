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

public class AppendMemory extends VirtualMemory {
    private static final Log LOG = LogFactory.getLog(AppendMemory.class);
    private FilesFacade ff;
    private long fd = -1;
    private long pageAddress = 0;

    public AppendMemory(FilesFacade ff, LPSZ name, long pageSize) {
        of(ff, name, pageSize);
    }

    public AppendMemory() {
    }

    static void bestEffortClose(FilesFacade ff, Log log, long fd, boolean truncate, long size, long mapPageSize) {
        try {
            if (truncate) {
                if (ff.truncate(fd, size)) {
                    log.info().$("truncated and closed [fd=").$(fd).$(']').$();
                } else {
                    if (ff.isRestrictedFileSystem()) {
                        // Windows does truncate file if it has a mapped page somewhere, could be another handle and process.
                        // To make it work size needs to be rounded up to nearest page.
                        long n = size / mapPageSize;
                        if (ff.truncate(fd, (n + 1) * mapPageSize)) {
                            log.info().$("truncated and closed, second attempt [fd=").$(fd).$(']').$();
                            return;
                        }
                    }
                    log.info().$("closed without truncate [fd=").$(fd).$(", errno=").$(ff.errno()).$(']').$();
                }
            } else {
                log.info().$("closed [fd=").$(fd).$(']').$();
            }
        } finally {
            ff.close(fd);
        }
    }

    @Override
    public void close() {
        close(true);
    }

    public final void setSize(long size) {
        jumpTo(size);
    }

    public void truncate() {
        if (fd == -1) {
            // are we closed ?
            return;
        }

        releaseCurrentPage();
        if (!ff.truncate(fd, getMapPageSize())) {
            throw CairoException.instance(ff.errno()).put("Cannot truncate fd=").put(fd).put(" to ").put(getMapPageSize()).put(" bytes");
        }
        updateLimits(0, pageAddress = mapPage(0));
        LOG.info().$("truncated [fd=").$(fd).$(']').$();
    }

    public final void close(boolean truncate) {
        long sz = getAppendOffset();
        releaseCurrentPage();
        super.close();
        if (fd != -1) {
            try {
                bestEffortClose(ff, LOG, fd, truncate, sz, getMapPageSize());
            } finally {
                fd = -1;
            }
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
        LOG.info().$("open ").$(name).$(" [fd=").$(fd).$(", pageSize=").$(pageSize).$(']').$();
    }

    FilesFacade getFilesFacade() {
        return ff;
    }

    @Override
    protected long mapWritePage(int page) {
        releaseCurrentPage();
        return pageAddress = mapPage(page);
    }

    @Override
    protected void release(int page, long address) {
        ff.munmap(address, getPageSize(page));
    }

    public void sync(boolean async) {
        if (pageAddress != 0) {
            if (ff.msync(pageAddress, getMapPageSize(), async) == 0) {
                return;
            }
            LOG.error().$("could not msync [fd=").$(fd).$(", errno=").$(ff.errno()).$(']').$();
        }
    }

    private void releaseCurrentPage() {
        if (pageAddress != 0) {
            release(0, pageAddress);
            pageAddress = 0;
        }
    }

    private long mapPage(int page) {
        long target = pageOffset(page + 1);
        if (ff.length(fd) < target && !ff.truncate(fd, target)) {
            throw CairoException.instance(ff.errno()).put("Appender resize failed fd=").put(fd).put(", size=").put(target);
        }
        long offset = pageOffset(page);
        long address = ff.mmap(fd, getMapPageSize(), offset, Files.MAP_RW);
        if (address != -1) {
            return address;
        }
        throw CairoException.instance(ff.errno()).put("Could not mmap append fd=").put(fd).put(", offset=").put(offset).put(", size=").put(getMapPageSize());
    }
}
