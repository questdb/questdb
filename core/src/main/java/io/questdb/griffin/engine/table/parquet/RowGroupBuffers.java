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

package io.questdb.griffin.engine.table.parquet;

import io.questdb.cairo.Reopenable;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Native output buffers for a decoded parquet row group.
 * <p>
 * For `_pm`-backed decode, a column chunk that is known to be all-null may be
 * represented without materialized data by setting both data and aux pointers
 * and sizes to {@code 0}. Consumers must treat that as a logical all-null chunk,
 * not as an invalid buffer.
 * <p>
 * The current {@code read_parquet()} direct cursor still uses {@link ParquetFileDecoder},
 * which materializes all requested chunks. If it ever switches to `_pm`, it must
 * honor the same zero-pointer convention before dereferencing these buffers.
 */
public class RowGroupBuffers implements QuietCloseable, Reopenable {
    private static final long CHUNKS_PTR_OFFSET;
    private static final long CHUNK_AUX_PTR_OFFSET;
    private static final long CHUNK_AUX_SIZE_OFFSET;
    private static final long CHUNK_COLUMN_TOP_OFFSET;
    private static final long CHUNK_DATA_PTR_OFFSET;
    private static final long CHUNK_DATA_SIZE_OFFSET;
    private static final long CHUNK_PAGE_BUFFERS_SIZE_OFFSET;
    private static final long CHUNK_STRUCT_SIZE;
    private final int memoryTag;
    // Per-query tracker, when set, charges the decoded column data against the
    // owning workload's limit in addition to the global RSS counter. The native
    // allocator is captured into the Rust RowGroupBuffers struct at create()
    // time, so the tracker must be set before the (lazy) reopen() of a
    // keepClosed instance.
    private @Nullable MemoryTracker memoryTracker;
    private long ptr;

    public RowGroupBuffers(int memoryTag) {
        this.memoryTag = memoryTag;
        this.ptr = create(Unsafe.getNativeAllocator(memoryTag, memoryTracker));
    }

    public RowGroupBuffers(int memoryTag, boolean keepClosed) {
        this.memoryTag = memoryTag;
        if (!keepClosed) {
            this.ptr = create(Unsafe.getNativeAllocator(memoryTag, memoryTracker));
        }
    }

    @Override
    public void close() {
        if (ptr != 0) {
            destroy(ptr);
            ptr = 0;
        }
    }

    public long getChunkAuxPtr(int columnIndex) {
        final long chunksPtr = Unsafe.getLong(ptr + CHUNKS_PTR_OFFSET);
        assert chunksPtr != 0;
        return Unsafe.getLong(chunksPtr + columnIndex * CHUNK_STRUCT_SIZE + CHUNK_AUX_PTR_OFFSET);
    }

    public long getChunkAuxSize(int columnIndex) {
        final long chunksPtr = Unsafe.getLong(ptr + CHUNKS_PTR_OFFSET);
        assert chunksPtr != 0;
        return Unsafe.getLong(chunksPtr + columnIndex * CHUNK_STRUCT_SIZE + CHUNK_AUX_SIZE_OFFSET);
    }

    /**
     * Number of leading column-top rows in the decoded chunk for the given column. For a
     * source type with no in-band null sentinel (BYTE/SHORT/CHAR) these are its only nulls,
     * so a lazy fixed-&gt;var conversion must surface NULL for rows below this count rather
     * than format the in-band 0. Returns 0 when the column has no column top in this chunk.
     */
    public long getChunkColumnTop(int columnIndex) {
        final long chunksPtr = Unsafe.getLong(ptr + CHUNKS_PTR_OFFSET);
        assert chunksPtr != 0;
        return Unsafe.getLong(chunksPtr + columnIndex * CHUNK_STRUCT_SIZE + CHUNK_COLUMN_TOP_OFFSET);
    }

    public long getChunkDataPtr(int columnIndex) {
        final long chunksPtr = Unsafe.getLong(ptr + CHUNKS_PTR_OFFSET);
        assert chunksPtr != 0;
        return Unsafe.getLong(chunksPtr + columnIndex * CHUNK_STRUCT_SIZE + CHUNK_DATA_PTR_OFFSET);
    }

    public long getChunkDataSize(int columnIndex) {
        final long chunksPtr = Unsafe.getLong(ptr + CHUNKS_PTR_OFFSET);
        assert chunksPtr != 0;
        return Unsafe.getLong(chunksPtr + columnIndex * CHUNK_STRUCT_SIZE + CHUNK_DATA_SIZE_OFFSET);
    }

    /**
     * Total bytes retained in the chunk's {@code page_buffers} (decompressed page/dict
     * buffers that VARCHAR_SLICE aux pointers reference). Zero for non-VARCHAR_SLICE
     * columns and for VARCHAR_SLICE chunks whose bytes are borrowed from the mmap or
     * already counted by {@link #getChunkDataSize(int)} (the DeltaByteArray spill path).
     * The decode-cache byte budget adds this so VARCHAR_SLICE frames are not undercounted.
     */
    @TestOnly
    public long getChunkPageBuffersSize(int columnIndex) {
        final long chunksPtr = Unsafe.getLong(ptr + CHUNKS_PTR_OFFSET);
        assert chunksPtr != 0;
        return Unsafe.getLong(chunksPtr + columnIndex * CHUNK_STRUCT_SIZE + CHUNK_PAGE_BUFFERS_SIZE_OFFSET);
    }

    public long ptr() {
        return ptr;
    }

    @Override
    public void reopen() {
        if (ptr == 0) {
            ptr = create(Unsafe.getNativeAllocator(memoryTag, memoryTracker));
        }
    }

    /**
     * Binds the per-query memory tracker. Takes effect on the next (re)allocation
     * of the native buffers: for a {@code keepClosed} instance the matching
     * {@link #reopen()} captures it, so callers must set the tracker before
     * reopening. A {@code null} tracker degrades to global-only accounting.
     */
    public void setMemoryTracker(@Nullable MemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;
    }

    public long sumChunkBytes(int startSlot, int slotCount) {
        final long chunksPtr = Unsafe.getLong(ptr + CHUNKS_PTR_OFFSET);
        assert chunksPtr != 0;
        long total = 0;
        for (int s = 0; s < slotCount; s++) {
            final long base = chunksPtr + (startSlot + s) * CHUNK_STRUCT_SIZE;
            total += Unsafe.getLong(base + CHUNK_DATA_SIZE_OFFSET)
                    + Unsafe.getLong(base + CHUNK_AUX_SIZE_OFFSET)
                    + Unsafe.getLong(base + CHUNK_PAGE_BUFFERS_SIZE_OFFSET);
        }
        return total;
    }

    private static native long chunkAuxPtrOffset();

    private static native long chunkAuxSizeOffset();

    private static native long chunkColumnTopOffset();

    private static native long chunkDataPtrOffset();

    private static native long chunkDataSizeOffset();

    private static native long chunkPageBuffersSizeOffset();

    private static native long columnBuffersPtrOffset();

    private static native long columnChunkBuffersSize();

    private static native long create(long allocator);

    private static native void destroy(long impl);

    static {
        Os.init();

        CHUNKS_PTR_OFFSET = columnBuffersPtrOffset();
        CHUNK_STRUCT_SIZE = columnChunkBuffersSize();
        CHUNK_DATA_PTR_OFFSET = chunkDataPtrOffset();
        CHUNK_DATA_SIZE_OFFSET = chunkDataSizeOffset();
        CHUNK_PAGE_BUFFERS_SIZE_OFFSET = chunkPageBuffersSizeOffset();
        CHUNK_AUX_PTR_OFFSET = chunkAuxPtrOffset();
        CHUNK_AUX_SIZE_OFFSET = chunkAuxSizeOffset();
        CHUNK_COLUMN_TOP_OFFSET = chunkColumnTopOffset();
    }
}
