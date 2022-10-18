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
import io.questdb.cairo.vm.api.MemoryCMOR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;

// Contiguous mapped with offset readable memory
// todo: investigate if we can map file from 0 offset and have the logc in this class done by the OS
public class MemoryCMORImpl extends MemoryCMRImpl implements MemoryCMOR {
    private static final Log LOG = LogFactory.getLog(MemoryCMORImpl.class);
    private long mapFileOffset;
    private long offset;

    public MemoryCMORImpl() {
    }

    @Override
    public void close() {
        super.close();
        mapFileOffset = 0;
        offset = 0;
    }

    @Override
    public void growToFileSize() {
        long length = getFilesFacade().length(getFd());
        if (length < 0) {
            throw CairoException.critical(ff.errno()).put("could not get length fd: ").put(fd);
        }

        extend(length - mapFileOffset);
    }

    @Override
    public void extend(long newSize) {
        if (newSize > size) {
            setSize0(newSize + offset - mapFileOffset);
        }
    }

    @Override
    public long addressOf(long offset) {
        assert offset - mapFileOffset <= size : "offset=" + offset + ", size=" + size + ", fd=" + fd;
        return pageAddress + offset - mapFileOffset;
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, long opts) {
        ofOffset(ff, name, 0L, size, memoryTag, opts);
    }

    @Override
    public void ofOffset(FilesFacade ff, LPSZ name, long lo, long hi, int memoryTag, long opts) {
        this.memoryTag = memoryTag;
        openFile(ff, name);
        if (hi < 0) {
            hi = ff.length(fd);
            if (hi < 0) {
                close();
                throw CairoException.critical(ff.errno()).put("could not get length: ").put(name);
            }
        }

        assert hi >= lo : "hi : " + hi + " lo : " + lo;

        if (hi > lo) {
            this.offset = lo;
            this.mapFileOffset = Files.PAGE_SIZE * (lo / Files.PAGE_SIZE);
            this.size = hi - mapFileOffset;
            map(ff, name, this.size, this.mapFileOffset);
        } else {
            this.size = 0;
        }
    }

    @Override
    public long getOffset() {
        return offset;
    }

    @Override
    public long size() {
        return size + mapFileOffset - offset;
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
                pageAddress = TableUtils.mremap(ff, fd, pageAddress, size, newSize, mapFileOffset, Files.MAP_RO, memoryTag);
            } else {
                assert pageAddress == 0;
                pageAddress = TableUtils.mapRO(ff, fd, newSize, mapFileOffset, memoryTag);
            }
            size = newSize;
        } catch (Throwable e) {
            close();
            throw e;
        }
    }
}
