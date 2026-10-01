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

package io.questdb.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.api.MemoryA;
import org.jetbrains.annotations.NotNull;

/**
 * Append-only cursor for one live-view function-state page. Window functions
 * receive this restricted surface instead of the checkpoint file's mutable
 * {@link MemoryA}; they cannot seek into framing or another function's state.
 * The owner turns {@link #size()} into the page reference's exact decoded
 * length after {@link io.questdb.griffin.engine.window.WindowFunction#freezeCheckpointState}
 * returns.
 */
public final class LiveViewStatePageWriter {

    public static final long MAX_PAGE_SIZE = Integer.MAX_VALUE;
    private long maxPageSize;
    private long pageSize;
    private long pageStart;
    private MemoryA sink;

    public long getPageStart() {
        ensureOpen();
        return pageStart;
    }

    /**
     * Opens a page at the sink's current append offset.
     */
    public LiveViewStatePageWriter of(@NotNull MemoryA sink) {
        return of(sink, MAX_PAGE_SIZE);
    }

    /**
     * Opens a page with an explicit byte limit. This is used by tests and by
     * page stores with a narrower format-specific maximum.
     */
    public LiveViewStatePageWriter of(@NotNull MemoryA sink, long maxPageSize) {
        if (maxPageSize < 0 || maxPageSize > MAX_PAGE_SIZE) {
            throw CairoException.critical(0)
                    .put("live view checkpoint state page size limit invalid, limit=")
                    .put(maxPageSize);
        }
        this.sink = sink;
        this.pageStart = sink.getAppendOffset();
        this.pageSize = 0;
        this.maxPageSize = maxPageSize;
        return this;
    }

    public void putBool(boolean value) {
        reserve(Byte.BYTES);
        sink.putBool(value);
    }

    public void putByte(byte value) {
        reserve(Byte.BYTES);
        sink.putByte(value);
    }

    public void putDecimal128(long hi, long lo) {
        reserve(2L * Long.BYTES);
        sink.putDecimal128(hi, lo);
    }

    public void putDecimal256(long hh, long hl, long lh, long ll) {
        reserve(4L * Long.BYTES);
        sink.putDecimal256(hh, hl, lh, ll);
    }

    public void putDouble(double value) {
        reserve(Double.BYTES);
        sink.putDouble(value);
    }

    public void putInt(int value) {
        reserve(Integer.BYTES);
        sink.putInt(value);
    }

    public void putLong(long value) {
        reserve(Long.BYTES);
        sink.putLong(value);
    }

    public void putShort(short value) {
        reserve(Short.BYTES);
        sink.putShort(value);
    }

    /**
     * Returns the exact number of bytes appended to this page.
     */
    public long size() {
        ensureOpen();
        final long appendOffset = sink.getAppendOffset();
        if (appendOffset < pageStart || appendOffset - pageStart != pageSize) {
            throw CairoException.critical(0)
                    .put("live view checkpoint state page sink moved outside page writer")
                    .put(" [start=").put(pageStart)
                    .put(", append=").put(appendOffset)
                    .put(", expectedSize=").put(pageSize).put(']');
        }
        return pageSize;
    }

    private void ensureOpen() {
        if (sink == null) {
            throw CairoException.critical(0).put("live view checkpoint state page writer is not open");
        }
    }

    private void reserve(long bytes) {
        ensureOpen();
        if (bytes < 0 || pageSize > maxPageSize - bytes) {
            throw CairoException.critical(0)
                    .put("live view checkpoint state page exceeds size limit")
                    .put(" [size=").put(pageSize)
                    .put(", write=").put(bytes)
                    .put(", limit=").put(maxPageSize).put(']');
        }
        pageSize += bytes;
    }
}
