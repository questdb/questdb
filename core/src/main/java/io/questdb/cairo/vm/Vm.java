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

import io.questdb.cairo.vm.api.*;
import io.questdb.log.Log;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;

public class Vm {
    public static final int STRING_LENGTH_BYTES = 4;

    public static void bestEffortClose(FilesFacade ff, Log log, long fd, boolean truncate, long size, long mapPageSize) {
        try {
            if (truncate) {
                bestEffortTruncate(ff, log, fd, size, mapPageSize);
            } else {
                log.debug().$("closed [fd=").$(fd).$(']').$();
            }
        } finally {
            if (fd > 0) {
                ff.close(fd);
            }
        }
    }

    public static long bestEffortTruncate(FilesFacade ff, Log log, long fd, long size, long mapPageSize) {
        if (ff.truncate(Math.abs(fd), size)) {
            log.debug()
                    .$("truncated and closed [fd=").$(fd)
                    .$(", size=").$(size)
                    .$(']').$();
            return size;
        }
        if (ff.isRestrictedFileSystem()) {
            // Windows does truncate file if it has a mapped page somewhere, could be another handle and process.
            // To make it work size needs to be rounded up to the nearest page.
            long n = (size - 1) / mapPageSize;
            long sz = (n + 1) * mapPageSize;
            if (ff.truncate(Math.abs(fd), sz)) {
                log.debug()
                        .$("truncated and closed, second attempt [fd=").$(fd)
                        .$(", size=").$(sz)
                        .$(']').$();
                return sz;
            }
        }
        log.debug().$("closed without truncate [fd=").$(fd).$(", errno=").$(ff.errno()).$(']').$();
        return -1;
    }

    public static MemoryAR getARInstance(long pageSize, int maxPages) {
        return new MemoryCARWImpl(pageSize, maxPages);
    }

    public static MemoryARW getARWInstance(long pageSize, int maxPages) {
        return new MemoryCARWImpl(pageSize, maxPages);
    }

    public static MemoryCARW getCARWInstance(long pageSize, int maxPages) {
        return new MemoryCARWImpl(pageSize, maxPages);
    }

    public static MemoryCMARW getCMARWInstance(FilesFacade ff, LPSZ name, long pageSize, long maxPages, int memoryTag) {
        return new MemoryCMARWImpl(ff, name, pageSize, maxPages, memoryTag);
    }

    public static MemoryCMARW getCMARWInstance() {
        return new MemoryCMARWImpl();
    }

    public static MemoryMA getMAInstance() {
        return new MemoryPMAImpl();
    }

    public static MemoryMAR getMARInstance() {
        return new MemoryPMAImpl();
    }

    public static MemoryMARW getMARWInstance() {
        return new MemoryCMARWImpl();
    }

    public static MemoryMARW getMARWInstance(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag) {
        return new MemoryCMARWImpl(ff, name, extendSegmentSize, size, memoryTag);
    }

    public static MemoryMR getMRInstance() {
        return new MemoryCMRImpl();
    }

    public static MemoryMR getMRInstance(FilesFacade ff, LPSZ name, long size, int memoryTag) {
        return new MemoryCMRImpl(ff, name, size, memoryTag);
    }

    public static MemoryA getSmallAInstance(FilesFacade ff, LPSZ name, int memoryTag) {
        return new MemoryCMARWImpl(ff, name, ff.getPageSize(), Long.MAX_VALUE, memoryTag);
    }

    public static MemoryARW getSmallARWInstance(FilesFacade ff, LPSZ name, int memoryTag) {
        return new MemoryCMARWImpl(ff, name, ff.getPageSize(), Long.MAX_VALUE, memoryTag);
    }

    public static MemoryCMARW getSmallCMARWInstance(FilesFacade ff, LPSZ name, int memoryTag) {
        return new MemoryCMARWImpl(ff, name, ff.getPageSize(), Long.MAX_VALUE, memoryTag);
    }

    public static long getStorageLength(int len) {
        return STRING_LENGTH_BYTES + len * 2L;
    }

    public static int getStorageLength(CharSequence s) {
        if (s == null) {
            return STRING_LENGTH_BYTES;
        }

        return STRING_LENGTH_BYTES + s.length() * 2;
    }

    public static MemoryMARW getWholeMARWInstance(FilesFacade ff, LPSZ name, long extendSegmentSize, int memoryTag) {
        return new MemoryCMARWImpl(ff, name, extendSegmentSize, Long.MAX_VALUE, memoryTag);
    }
}
