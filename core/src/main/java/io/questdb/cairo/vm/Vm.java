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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryCMOR;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;

public class Vm {
    // Set to true to enable the assertion of pointers and buffer sizes which are too expensive for production.
    public static final int STRING_LENGTH_BYTES = 4;
    public static final byte TRUNCATE_TO_PAGE = 0;
    public static final byte TRUNCATE_TO_POINTER = 1;

    public static void bestEffortClose(FilesFacade ff, Log log, long fd, long size, byte truncateMode) {
        try {
            if (size > -1L) {
                bestEffortTruncate(ff, log, fd, size, truncateMode);
            } else {
                log.debug().$("closed [fd=").$(fd).$(']').$();
            }
        } finally {
            ff.close(fd);
        }
    }

    public static void bestEffortClose(FilesFacade ff, Log log, long fd, long size) {
        bestEffortClose(ff, log, fd, size, TRUNCATE_TO_PAGE);
    }

    public static long bestEffortTruncate(FilesFacade ff, Log log, long fd, long size, byte truncateMode) {
        long sz = (truncateMode == TRUNCATE_TO_PAGE) ? Files.ceilPageSize(size) : size;
        if (ff.truncate(Math.abs(fd), sz)) {
            log.debug()
                    .$("truncated and closed [fd=").$(fd)
                    .$(", size=").$(sz)
                    .$(']').$();
            return sz;
        }
        log.debug().$("closed without truncate [fd=").$(fd).$(", errno=").$(ff.errno()).$(']').$();
        return -1;
    }

    public static long bestEffortTruncate(FilesFacade ff, Log log, long fd, long size) {
        return bestEffortTruncate(ff, log, fd, size, TRUNCATE_TO_PAGE);
    }

    public static MemoryCARW getCARWInstance(long pageSize, int maxPages, int memoryTag) {
        return new MemoryCARWImpl(pageSize, maxPages, memoryTag);
    }

    public static MemoryCMARW getCMARWInstance(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, int opts) {
        return new MemoryCMARWImpl(ff, name, extendSegmentSize, size, memoryTag, opts);
    }

    public static MemoryCMARW getCMARWInstance() {
        return new MemoryCMARWImpl();
    }

    public static MemoryCMR getCMRInstance() {
        return new MemoryCMRImpl();
    }

    public static MemoryCMR getCMRInstance(boolean bypassFdCache) {
        return new MemoryCMRImpl(bypassFdCache);
    }

    public static MemoryCMR getCMRInstance(FilesFacade ff, LPSZ name, long size, int memoryTag) {
        return new MemoryCMRImpl(ff, name, size, memoryTag);
    }

    public static MemoryCMOR getMemoryCMOR(boolean bypassFdCache) {
        return new MemoryCMORImpl(bypassFdCache);
    }

    public static MemoryMAR getPMARInstance(CairoConfiguration configuration) {
        return new MemoryPMARImpl(configuration);
    }

    public static MemoryMAR getPURInstance(io.questdb.cairo.wal.WalWriterRingManager ringManager) {
        return new MemoryPURImpl(ringManager);
    }

    public static MemoryCMARW getSmallCMARWInstance(FilesFacade ff, LPSZ name, int memoryTag, int opts) {
        return new MemoryCMARWImpl(ff, name, ff.getPageSize(), -1, memoryTag, opts);
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

    public static MemoryMARW getWholeMARWInstance(FilesFacade ff, LPSZ name, long extendSegmentSizePow2, int memoryTag, int opts) {
        return new MemoryCMARWImpl(ff, name, extendSegmentSizePow2, -1, memoryTag, opts);
    }
}
