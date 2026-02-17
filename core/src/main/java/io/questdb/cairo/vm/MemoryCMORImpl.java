/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
public class MemoryCMORImpl extends MemoryCMRImpl implements MemoryCMOR {
    private static final Log LOG = LogFactory.getLog(MemoryCMORImpl.class);
    private final boolean bypassFdCache;
    private boolean closeFdOnClose = true;
    private long mapFileOffset;
    private long offset;

    public MemoryCMORImpl() {
        super(false);
        bypassFdCache = false;
    }

    public MemoryCMORImpl(boolean bypassFdCache) {
        super(bypassFdCache);
        this.bypassFdCache = bypassFdCache;
    }

    /**
     * Get the address of a file-based offset. This ignores the `lo` and `hi` range specifies during construction.
     * The offset is relative to the start of the file, not the virtual area of interest.
     */
    @Override
    public long addressOf(long fileOffset) {
        assert checkOffsetMapped(fileOffset) : "offset=" + offset + ", size=" + size + ", fd=" + fd;
        if (pageAddress == 0) {
            // Lazy mapping
            map(ff, size, mapFileOffset);
        }
        return pageAddress + fileOffset - mapFileOffset;
    }

    @Override
    public boolean checkOffsetMapped(long fileOffset) {
        return fileOffset - mapFileOffset <= size;
    }

    @Override
    public void close() {
        if (!closeFdOnClose) {
            fd = -1;
        }
        super.close();
        mapFileOffset = 0;
        offset = 0;
        closeFdOnClose = true;
    }

    @Override
    public long detachFdClose() {
        long lfd = fd;
        fd = -1;
        close();
        return lfd;
    }

    @Override
    public void extend(long newSize) {
        if (newSize > size) {
            setSize0(newSize + offset - mapFileOffset);
        }
    }

    @Override
    public long getOffset() {
        return offset;
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
    public void map() {
        if (pageAddress == 0) {
            map(ff, size, mapFileOffset);
        }
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, int opts) {
        ofOffset(ff, name, 0L, size, memoryTag, opts);
    }

    @Override
    public void ofOffset(FilesFacade ff, long fd, boolean keepFdOpen, LPSZ name, long lo, long hi, int memoryTag, int opts) {
        this.memoryTag = memoryTag;
        if (fd > -1) {
            close();
            this.closeFdOnClose = !keepFdOpen;
            this.ff = ff;
            this.fd = fd;
        } else {
            this.closeFdOnClose = !keepFdOpen;
            openFile(ff, name);
        }
        mapLazy(lo, hi);
    }

    /**
     * Size of the "virtual" mapped area, accounting for the offset (hi - lo) during the construction.
     * Careful not to use this in conjunction with `addressOf` which uses a file-based offset.
     */
    @Override
    public long size() {
        return size + mapFileOffset - offset;
    }

    private void mapLazy(long lo, long hi) {
        assert hi >= 0 && hi >= lo : "hi : " + hi + " lo : " + lo;
        if (hi > lo) {
            this.offset = lo;
            this.mapFileOffset = Files.PAGE_SIZE * (lo / Files.PAGE_SIZE);
            this.size = hi - mapFileOffset;
        } else {
            this.size = 0;
        }
    }

    private void openFile(FilesFacade ff, LPSZ name) {
        close();
        this.ff = ff;
        fd = bypassFdCache
                ? TableUtils.openRONoCache(ff, name, LOG)
                : TableUtils.openRO(ff, name, LOG);
    }

    private void setSize0(long newSize) {
        try {
            if (size > 0 && pageAddress != 0) {
                pageAddress = TableUtils.mremap(ff, fd, pageAddress, size, newSize, mapFileOffset, Files.MAP_RO, memoryTag);
            } else {
                pageAddress = TableUtils.mapRO(ff, fd, newSize, mapFileOffset, memoryTag);
            }
            size = newSize;
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    protected void map(FilesFacade ff, final long size, final long mapOffset) {
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
        LOG.debug().$("map  [fd=").$(fd).$(", size=").$(this.size).$(']').$();
    }
}
