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
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.LPSZ;

public class MemoryCMRImpl extends AbstractMemoryCR implements MemoryCMR {
    private static final Log LOG = LogFactory.getLog(MemoryCMRImpl.class);
    private int memoryTag = MemoryTag.MMAP_DEFAULT;

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
    public void extend(long newSize) {
        grownLength = Math.max(newSize, grownLength);
        if (newSize > size) {
            setSize0(newSize);
        }
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag) {
        this.memoryTag = memoryTag;
        openFile(ff, name);
        map(ff, name, size);
    }

    protected void map(FilesFacade ff, LPSZ name, final long size) {
        this.size = size;
        if (size > 0) {
            try {
                this.pageAddress = TableUtils.mapRO(ff, fd, size, memoryTag);
            } catch (Throwable e) {
                close();
                throw e;
            }
        } else {
            assert size > -1;
            this.pageAddress = 0;
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
