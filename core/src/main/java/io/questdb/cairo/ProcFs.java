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

package io.questdb.cairo;

import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

/**
 * Reader for {@code /proc} and {@code /sys} pseudo-files.
 *
 * <p>Exists because pseudo-files cannot be read like regular files: <b>they do not report a usable
 * size</b>. Measured on Linux 6.8:
 * <pre>
 *   /proc/mounts                 fstat=0     readable=2007
 *   /proc/fs/ext4/&lt;dev&gt;/options  fstat=0     readable=305
 *   /sys/... (sysfs)             fstat=4096  readable=11
 * </pre>
 * Sizing a read from {@code ff.length(fd)} therefore reads ZERO bytes from procfs and yields an empty
 * string, or over-reads sysfs and rejects the short read. Both failures are silent: the caller sees
 * "empty file" rather than an error, and a detector built on it reports "nothing detected" forever.
 *
 * <p>That is not hypothetical. {@link FastCommitCheck} and {@link WriteBarrierCheck} each carried their own
 * copy of a length-sized reader, so both were permanently inert on Linux — the ext4 {@code fast_commit}
 * guard never fired (leaving the unsafe batched flush enabled) and the {@code nobarrier} startup warning
 * never printed. One shared, tested reader replaces both copies so a third consumer cannot reintroduce it.
 *
 * <p>The whole content is fetched with a single {@code pread} of up to {@code maxBytes}, which is correct
 * for every pseudo-file of interest and avoids the torn-snapshot risk of reading procfs incrementally.
 */
public final class ProcFs {

    private ProcFs() {
    }

    /**
     * Read up to {@code maxBytes} of a pseudo-file and return it as a string.
     *
     * @param ff       files facade (injectable, so callers are testable without a real {@code /proc})
     * @param path     absolute path of the pseudo-file
     * @param maxBytes cap on the read; sized generously by the caller, NOT taken from the file's stat size
     * @return the content, or {@code null} if the file could not be opened or read
     */
    public static String read(FilesFacade ff, String path, int maxBytes) {
        long fd = -1;
        long mem = 0;
        long allocSize = 0;
        try (Path p = new Path()) {
            p.of(path);
            fd = ff.openRONoCache(p.$());
            if (fd < 0) {
                return null;
            }
            allocSize = maxBytes + 1L;
            mem = Unsafe.malloc(allocSize, MemoryTag.NATIVE_DEFAULT);
            final long bytesRead = ff.read(fd, mem, maxBytes, 0);
            if (bytesRead < 0) {
                return null;
            }
            Unsafe.getUnsafe().putByte(mem + bytesRead, (byte) 0);
            final Utf8StringSink sink = new Utf8StringSink();
            Utf8s.strCpy(mem, mem + bytesRead, sink);
            return sink.toString();
        } finally {
            if (fd >= 0) {
                ff.close(fd);
            }
            if (mem != 0) {
                Unsafe.free(mem, allocSize, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }
}
