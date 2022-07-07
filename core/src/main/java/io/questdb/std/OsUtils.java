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

package io.questdb.std;

import io.questdb.log.Log;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

public class OsUtils {
    public static long getMaxMapCount(Log log, FilesFacade ff) {
        if (Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64) {
            return getMaxMapCountLinux(log, ff);
        }

        return -1;
    }

    private static CharSequence readFileAsText(Path path, Log log, FilesFacade ff, StringSink sink) {
        long addr = 0L;

        try {
            long fd = ff.openRO(path);
            if (fd < 0) {
                log.error().$("failed to open file [path=").$(path)
                        .$(", errno=").$(ff.errno())
                        .I$();
                return null;
            }

            addr = Unsafe.malloc(ff.getPageSize(), MemoryTag.NATIVE_DEFAULT);
            long length = ff.read(fd, addr, ff.getPageSize(), 0L);
            for (long i = 0; i < length; i++) {
                sink.put((char) Unsafe.getUnsafe().getByte(addr + i));
            }

            ff.close(fd);
            return sink.subSequence(0, sink.length() - 1);
        } finally {
            if (addr != 0L) {
                Unsafe.free(addr, ff.getPageSize(), MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private static long getMaxMapCountLinux(Log log, FilesFacade ff) {
        long addr = 0L;
        long length = 0L;
        try (Path path = new Path().of("/proc/sys/vm/max_map_count").$()) {
            StringSink sink = new StringSink();
            sink.clear();

            CharSequence fileText = readFileAsText(path, log, ff, sink);
            if (fileText != null) {
                return Numbers.parseLong(fileText);
            }
        } catch (NumericException e) {
            return -1L;
        } finally {
            Unsafe.free(addr, length, MemoryTag.NATIVE_DEFAULT);
        }
        return -1;
    }
}
