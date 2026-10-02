/*+*****************************************************************************
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

package io.questdb.cairo.frm.file;

import io.questdb.cairo.CairoException;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

/**
 * A frame column's native scratch for the appends it writes with {@link FilesFacade#write} rather than through a
 * mapping, when the rows have to change on the way to the file: the designated timestamp, which an O3 frame holds
 * interleaved in its sort index, and a var-size aux vector, whose data offsets shift to where the target's data ends.
 * Such a write goes out in chunks of at most {@link #MAX_SIZE} bytes. The buffer grows on first use and is freed when
 * the column closes.
 */
final class ColumnWriteBuffer implements QuietCloseable {
    static final long MAX_SIZE = 1024 * 1024;
    private long address;
    private long size;

    /**
     * Writes {@code len} bytes from {@code address} at {@code offset} of the file, which grows to hold them: an append
     * this way needs no allocation ahead of it.
     */
    static void write(FilesFacade ff, long fd, long address, long len, long offset) {
        if (ff.write(fd, address, len, offset) != len) {
            throw CairoException.critical(ff.errno()).put("could not append column data [fd=").put(fd)
                    .put(", offset=").put(offset)
                    .put(", len=").put(len)
                    .put(']');
        }
    }

    @Override
    public void close() {
        if (address != 0) {
            address = Unsafe.free(address, size, MemoryTag.NATIVE_O3);
            size = 0;
        }
    }

    /**
     * The buffer's address, grown to hold {@code min(bytes, MAX_SIZE)} bytes.
     */
    long reserve(long bytes) {
        bytes = Math.min(bytes, MAX_SIZE);
        if (bytes > size) {
            address = Unsafe.realloc(address, size, bytes, MemoryTag.NATIVE_O3);
            size = bytes;
        }
        return address;
    }
}
