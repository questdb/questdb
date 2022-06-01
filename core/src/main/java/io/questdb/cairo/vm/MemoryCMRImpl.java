/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryOM;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.LPSZ;

//contiguous mapped readable 
public class MemoryCMRImpl extends AbstractMemoryCR implements MemoryCMR, MemoryOM {
    private static final Log LOG = LogFactory.getLog(MemoryCMRImpl.class);
    private int memoryTag = MemoryTag.MMAP_DEFAULT;
    private long mapFileOffset;

    public MemoryCMRImpl(FilesFacade ff, LPSZ name, long size, int memoryTag) {
        of(ff, name, 0, size, memoryTag);
    }

    public MemoryCMRImpl() {
    }

    @Override
    public void close() {
        if (pageAddress != 0) {
            ff.munmap(pageAddress, size, memoryTag);
            this.size = 0;
            this.pageAddress = 0;
        }
        if (fd != -1) {
            ff.close(fd);
            LOG.debug().$("closed [fd=").$(fd).$(']').$();
            fd = -1;
        }
        grownLength = 0;
    }

    @Override
    public boolean isMapped(long offset, long len) {
        return offset + len <= size();
    }

    @Override
    public void extend(long newSize) {
        grownLength = Math.max(newSize, grownLength);
        if (newSize > size) {
            setSize0(newSize);
        }
    }

    @Override
    public void ofShift(FilesFacade ff, LPSZ name, long lo, long hi, int memoryTag, long opts) {
        this.memoryTag = memoryTag;
        openFile(ff, name);
        if (hi < 0) {
            hi = ff.length(fd);
            if (hi < 0) {
                throw CairoException.instance(ff.errno()).put("Could not get length: ").put(name);
            }
        }

        this.mapFileOffset = Files.PAGE_SIZE * (lo / Files.PAGE_SIZE);
        this.size = hi - mapFileOffset;
        map(ff, name, this.size, this.mapFileOffset);
    }

    @Override
    public long addressOf(long offset) {
        assert offset - mapFileOffset <= size : "offset=" + offset + ", size=" + size + ", fd=" + fd;
        return pageAddress + offset - mapFileOffset;
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, long opts) {
        this.memoryTag = memoryTag;
        openFile(ff, name);
        if (size < 0) {
            size = ff.length(fd);
            if (size < 0) {
                throw CairoException.instance(ff.errno()).put("Could not get length: ").put(name);
            }
        }
        this.mapFileOffset = 0L;
        this.map(ff, name, size, mapFileOffset);
    }

    @Override
    public long getOffset() {
        return mapFileOffset;
    }

    protected void map(FilesFacade ff, LPSZ name, final long size, final long mapOffset) {
        this.size = size;
        if (size > 0) {
            try {
                this.pageAddress = TableUtils.mapRO(ff, fd, size, mapOffset, memoryTag);
            } catch (Throwable e) {
                close();
                throw e;
            }
        } else {
            assert size > -1;
            this.pageAddress = 0;
        }

        // ---------------V leave a space here for alignment with open log message
        LOG.debug().$("map  [file=").$(name).$(", fd=").$(fd).$(", pageSize=").$(size).$(", size=").$(this.size).$(']').$();
    }

    private void openFile(FilesFacade ff, LPSZ name) {
        close();
        this.ff = ff;
        fd = TableUtils.openRO(ff, name, LOG);
    }

    private void setSize0(long newSize) {
        try {
            if (size > 0) {
                pageAddress = TableUtils.mremap(ff, fd, pageAddress, size, newSize, Files.MAP_RO, memoryTag);
            } else {
                assert pageAddress == 0;
                pageAddress = TableUtils.mapRO(ff, fd, newSize, memoryTag);
            }
            size = newSize;
        } catch (Throwable e) {
            close();
            throw e;
        }
    }
}
