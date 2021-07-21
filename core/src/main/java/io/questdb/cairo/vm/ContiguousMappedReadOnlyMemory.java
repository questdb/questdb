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

package io.questdb.cairo.vm;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;

public class ContiguousMappedReadOnlyMemory extends AbstractContiguousMemory
        implements MappedReadOnlyMemory, ContiguousReadOnlyMemory {

    private static final Log LOG = LogFactory.getLog(ContiguousMappedReadOnlyMemory.class);
    protected long page = 0;
    protected FilesFacade ff;
    protected long fd = -1;
    protected long size = 0;
    private long grownLength;

    public ContiguousMappedReadOnlyMemory(FilesFacade ff, LPSZ name, long pageSize, long size) {
        of(ff, name, pageSize, size);
    }

    public ContiguousMappedReadOnlyMemory(FilesFacade ff, LPSZ name, long size) {
        of(ff, name, 0, size);
    }

    public ContiguousMappedReadOnlyMemory() {
    }

    @Override
    public void close() {
        if (page != 0) {
            ff.munmap(page, size);
            this.size = 0;
            this.page = 0;
        }
        if (fd != -1) {
            ff.close(fd);
            LOG.debug().$("closed [fd=").$(fd).$(']').$();
            fd = -1;
        }
        grownLength = 0;
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long pageSize, long size) {
        openFile(ff, name);
        map(ff, name, size);
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long pageSize) {
        openFile(ff, name);
        map(ff, name, ff.length(fd));
    }

    @Override
    public boolean isDeleted() {
        return !ff.exists(fd);
    }

    @Override
    public long getFd() {
        return fd;
    }

    public long getGrownLength() {
        return grownLength;
    }

    @Override
    public long getPageAddress(int pageIndex) {
        return page;
    }

    @Override
    public void extend(long newSize) {
        grownLength = Math.max(newSize, grownLength);
        if (newSize > size) {
            setSize0(newSize);
        }
    }

    @Override
    public int getPageCount() {
        return page != 0 ? 1 : 0;
    }

    public long size() {
        return size;
    }

    public long addressOf(long offset) {
        assert offset <= size : "offset=" + offset + ", size=" + size + ", fd=" + fd;
        return page + offset;
    }

    @Override
    public void growToFileSize() {
        extend(ff.length(fd));
    }

    protected void map(FilesFacade ff, LPSZ name, long size) {
        size = Math.min(ff.length(fd), size);
        this.size = size;
        if (size > 0) {
            try {
                this.page = TableUtils.mapRO(ff, fd, size);
            } catch (Throwable e) {
                close();
                throw e;
            }
        } else {
            this.page = 0;
        }
        LOG.debug().$("open ").$(name).$(" [fd=").$(fd).$(", pageSize=").$(size).$(", size=").$(this.size).$(']').$();
    }

    private void openFile(FilesFacade ff, LPSZ name) {
        close();
        this.ff = ff;
        boolean exists = ff.exists(name);
        if (!exists) {
            throw CairoException.instance(0).put("File not found: ").put(name);
        }
        fd = TableUtils.openRO(ff, name, LOG);
    }

    private void setSize0(long newSize) {
        newSize = Math.max(newSize, ff.length(fd));
        try {
            if (size > 0) {
                page = TableUtils.mremap(ff, fd, page, size, newSize, Files.MAP_RO);
            } else {
                assert page == 0;
                page = TableUtils.mapRO(ff, fd, newSize);
            }
            size = newSize;
        } catch (Throwable e) {
            close();
            throw e;
        }
    }
}
