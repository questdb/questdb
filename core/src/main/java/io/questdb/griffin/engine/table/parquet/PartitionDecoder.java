/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.CairoException;
import io.questdb.std.*;
import io.questdb.std.str.Path;

public class PartitionDecoder implements QuietCloseable {
    private static final long CHUNK_BUFFERS_SIZE = 2 * Long.BYTES;
    private static final long COLUMN_COUNT_OFFSET = 0;
    private static final long ROW_COUNT_OFFSET = COLUMN_COUNT_OFFSET + Long.BYTES;
    private static final long ROW_GROUP_COUNT_OFFSET = ROW_COUNT_OFFSET + Long.BYTES;
    private final Path path;
    private long chunkAddr;
    private long impl;

    public PartitionDecoder() {
        try {
            this.chunkAddr = Unsafe.malloc(CHUNK_BUFFERS_SIZE, MemoryTag.NATIVE_DEFAULT);
            this.path = new Path();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        destroy();
        chunkAddr = Unsafe.free(chunkAddr, CHUNK_BUFFERS_SIZE, MemoryTag.NATIVE_DEFAULT);
        Misc.free(path);
    }

    public int columnCount() {
        assert impl != 0;
        return Unsafe.getUnsafe().getInt(impl + COLUMN_COUNT_OFFSET);
    }

    public void decodeColumnChunk(long rowGroup, long column, int columnType) {
        assert impl != 0;
        try {
            decodeColumnChunk(
                    impl,
                    rowGroup,
                    column,
                    columnType,
                    chunkAddr
            );
        } catch (Throwable th) {
            throw CairoException.critical(0).put("Could not decode partition: [path=").put(path)
                    .put(", exception=").put(th.getClass().getSimpleName())
                    .put(", msg=").put(th.getMessage())
                    .put(']');
        }
    }

    public void of(@Transient Path srcPath) {
        destroy();

        path.of(srcPath);
        try {
            impl = create(path.ptr(), path.size());
        } catch (Throwable th) {
            throw CairoException.critical(0).put("Could not describe partition: [path=").put(path)
                    .put(", exception=").put(th.getClass().getSimpleName())
                    .put(", msg=").put(th.getMessage())
                    .put(']');
        }
    }

    public int rowCount() {
        assert impl != 0;
        return Unsafe.getUnsafe().getInt(impl + ROW_COUNT_OFFSET);
    }

    public int rowGroupCount() {
        assert impl != 0;
        return Unsafe.getUnsafe().getInt(impl + ROW_GROUP_COUNT_OFFSET);
    }

    private static native long create(long srcPathPtr, int srcPathLength);

    private static native void decodeColumnChunk(
            long impl,
            long rowGroup,
            long column,
            int columnType,
            long buffersPtr
    );

    private static native void destroy(long impl);

    private void destroy() {
        if (impl != 0) {
            destroy(impl);
            impl = 0;
        }
    }

    static {
        Os.init();
    }
}
