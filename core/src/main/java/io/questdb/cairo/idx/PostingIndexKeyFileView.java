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

package io.questdb.cairo.idx;

import io.questdb.cairo.vm.AbstractMemoryCR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

/**
 * A posting index key file (.pk) read with pread, for a reader that holds no lock against the writer:
 * {@link PostingIndexWriter#truncate()} shrinks the file in place, and a read of a mapping past the new end of the
 * file raises SIGBUS. Here a value that the file ends before reads as -1, as from
 * {@link FilesFacade#readNonNegativeLong}, and every other value as the file holds it, as through a mapping.
 * <p>
 * Values in the header pages and from the head entry up come from one pread each. A writer changes exactly these
 * bytes in place, and the seqlock of {@link PostingIndexChainHeader} and the GEN_COUNT protocol of
 * {@link PostingIndexChainEntry#read} hold only for a reader that sees values in the order it reads them. Entries
 * below the head entry do not change once published, so their values come from a window that one pread fills, and
 * a walk down a long chain costs about as much as one over a mapping.
 * <p>
 * Only {@link #getInt}, {@link #getLong} and {@link #size} work, which is all that the chain readers use; the other
 * accessors throw.
 */
public final class PostingIndexKeyFileView extends AbstractMemoryCR {
    /**
     * Bytes that one pread copies into the window.
     */
    public static final long WINDOW_SIZE = 64 * 1024;
    /**
     * Bytes that a refill keeps above the value it was made for, room for the other fields of the entry the walk
     * is reading. The walk moves down the chain, so the rest of the window lies below that value.
     */
    public static final long WINDOW_TAIL = 4096;
    private long fd = -1;
    private FilesFacade ff;
    // [immutableLo, immutableHi): the entries below the head entry of the header that setHeader() received
    private long immutableHi;
    private long immutableLo;
    private long window;
    private long windowHi;
    private long windowLo;

    @Override
    public long addressHi() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long addressOf(long offset) {
        throw new UnsupportedOperationException();
    }

    /**
     * Forgets the file and frees the window. The view can read a file again after close.
     */
    @Override
    public void close() {
        detach();
        window = Unsafe.free(window, WINDOW_SIZE, MemoryTag.NATIVE_INDEX_READER);
    }

    /**
     * Forgets the file and keeps the window for the next one. The caller owns the fd and closes it.
     */
    public void detach() {
        ff = null;
        fd = -1;
        size = 0;
        immutableLo = 0;
        immutableHi = 0;
        windowLo = 0;
        windowHi = 0;
    }

    @Override
    public void extend(long size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getFd() {
        return fd;
    }

    @Override
    public int getInt(long offset) {
        if (isImmutable(offset, Integer.BYTES)) {
            final long address = windowAddress(offset, Integer.BYTES);
            return address != 0 ? Unsafe.getInt(address) : -1;
        }
        return ff.readNonNegativeInt(fd, offset);
    }

    @Override
    public long getLong(long offset) {
        if (isImmutable(offset, Long.BYTES)) {
            final long address = windowAddress(offset, Long.BYTES);
            return address != 0 ? Unsafe.getLong(address) : -1;
        }
        return ff.readNonNegativeLong(fd, offset);
    }

    /**
     * Starts reading a file. Until {@link #setHeader} runs, every value comes from a pread of its own.
     *
     * @param fd   a read-only fd of the .pk, which the caller owns and closes
     * @param size the length of the file, which bounds the walk as the length of a mapping would
     */
    public void of(FilesFacade ff, long fd, long size) {
        detach();
        this.ff = ff;
        this.fd = fd;
        this.size = size;
    }

    /**
     * Lets the values of the entries below the head entry that {@code header} names come from the window. The header
     * must come from {@link PostingIndexChainHeader#readUnderSeqlock} through this view.
     */
    public void setHeader(PostingIndexChainHeader.Snapshot header) {
        immutableLo = header.regionBase;
        immutableHi = header.isEmpty() ? header.regionBase : header.headEntryOffset;
        windowLo = 0;
        windowHi = 0;
    }

    private boolean isImmutable(long offset, int len) {
        return offset >= immutableLo && offset + len <= immutableHi;
    }

    // The address of the value at offset in the window, or 0 when the file ends before the value does
    private long windowAddress(long offset, int len) {
        if (offset < windowLo || offset + len > windowHi) {
            if (window == 0) {
                window = Unsafe.malloc(WINDOW_SIZE, MemoryTag.NATIVE_INDEX_READER);
            }
            final long hi = Math.min(immutableHi, offset + len + WINDOW_TAIL);
            final long lo = Math.max(immutableLo, hi - WINDOW_SIZE);
            // isImmutable() holds for the value, and the window is wider than any value
            assert lo <= offset && offset + len <= hi;
            final long bytesRead = ff.read(fd, window, hi - lo, lo);
            windowLo = lo;
            windowHi = lo + Math.max(bytesRead, 0);
            if (offset + len > windowHi) {
                // the file ends before the value does
                return 0;
            }
        }
        return window + offset - windowLo;
    }
}
