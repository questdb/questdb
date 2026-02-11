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

package io.questdb.griffin.engine.table.parquet;

import io.questdb.cairo.Reopenable;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

public class RowGroupBuffers implements QuietCloseable, Reopenable {
    private static final long CHUNKS_PTR_OFFSET;
    private static final long CHUNK_AUX_PTR_OFFSET;
    private static final long CHUNK_AUX_SIZE_OFFSET;
    private static final long CHUNK_DATA_PTR_OFFSET;
    private static final long CHUNK_DATA_SIZE_OFFSET;
    private static final long CHUNK_STRUCT_SIZE;
    private final int memoryTag;
    private long ptr;

    public RowGroupBuffers(int memoryTag) {
        this.memoryTag = memoryTag;
        this.ptr = create(Unsafe.getNativeAllocator(memoryTag));
    }

    public RowGroupBuffers(int memoryTag, boolean keepClosed) {
        this.memoryTag = memoryTag;
        if (!keepClosed) {
            this.ptr = create(Unsafe.getNativeAllocator(memoryTag));
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
        final long chunksPtr = Unsafe.getUnsafe().getLong(ptr + CHUNKS_PTR_OFFSET);
        assert chunksPtr != 0;
        return Unsafe.getUnsafe().getLong(chunksPtr + columnIndex * CHUNK_STRUCT_SIZE + CHUNK_AUX_PTR_OFFSET);
    }

    public long getChunkAuxSize(int columnIndex) {
        final long chunksPtr = Unsafe.getUnsafe().getLong(ptr + CHUNKS_PTR_OFFSET);
        assert chunksPtr != 0;
        return Unsafe.getUnsafe().getLong(chunksPtr + columnIndex * CHUNK_STRUCT_SIZE + CHUNK_AUX_SIZE_OFFSET);
    }

    public long getChunkDataPtr(int columnIndex) {
        final long chunksPtr = Unsafe.getUnsafe().getLong(ptr + CHUNKS_PTR_OFFSET);
        assert chunksPtr != 0;
        return Unsafe.getUnsafe().getLong(chunksPtr + columnIndex * CHUNK_STRUCT_SIZE + CHUNK_DATA_PTR_OFFSET);
    }

    public long getChunkDataSize(int columnIndex) {
        final long chunksPtr = Unsafe.getUnsafe().getLong(ptr + CHUNKS_PTR_OFFSET);
        assert chunksPtr != 0;
        return Unsafe.getUnsafe().getLong(chunksPtr + columnIndex * CHUNK_STRUCT_SIZE + CHUNK_DATA_SIZE_OFFSET);
    }

    public long ptr() {
        return ptr;
    }

    @Override
    public void reopen() {
        if (ptr == 0) {
            ptr = create(Unsafe.getNativeAllocator(memoryTag));
        }
    }

    private static native long chunkAuxPtrOffset();

    private static native long chunkAuxSizeOffset();

    private static native long chunkDataPtrOffset();

    private static native long chunkDataSizeOffset();

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
        CHUNK_AUX_PTR_OFFSET = chunkAuxPtrOffset();
        CHUNK_AUX_SIZE_OFFSET = chunkAuxSizeOffset();
    }
}
