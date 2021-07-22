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

import io.questdb.log.Log;
import io.questdb.std.FilesFacade;

public class VmUtils {
    public static final int STRING_LENGTH_BYTES = 4;

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
                // To make it work size needs to be rounded up to nearest page.
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

    public static int getStorageLength(CharSequence s) {
        if (s == null) {
            return STRING_LENGTH_BYTES;
        }

        return STRING_LENGTH_BYTES + s.length() * 2;
    }

    public static long getStorageLength(int len) {
        return STRING_LENGTH_BYTES + len * 2L;
    }

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
}
