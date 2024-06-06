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
import io.questdb.std.str.DirectString;
import io.questdb.std.str.Path;

public class PartitionDecoder implements QuietCloseable {
    public static final int BOOLEAN_PHYSICAL_TYPE = 0;
    public static final int BYTE_ARRAY_PHYSICAL_TYPE = 6;
    public static final int DOUBLE_PHYSICAL_TYPE = 5;
    public static final int FIXED_LEN_BYTE_ARRAY_PHYSICAL_TYPE = 7;
    public static final int FLOAT_PHYSICAL_TYPE = 4;
    public static final int INT32_PHYSICAL_TYPE = 1;
    public static final int INT64_PHYSICAL_TYPE = 2;
    public static final int INT96_PHYSICAL_TYPE = 3;
    private static final long COLUMN_COUNT_OFFSET = 0;
    private static final long ROW_COUNT_OFFSET = COLUMN_COUNT_OFFSET + Long.BYTES;
    private static final long ROW_GROUP_COUNT_OFFSET = ROW_COUNT_OFFSET + Long.BYTES;
    private static final long COLUMN_NAMES_OFFSET = ROW_GROUP_COUNT_OFFSET + Long.BYTES;
    private static final long COLUMN_NAME_LENGTHS_OFFSET = COLUMN_NAMES_OFFSET + Long.BYTES;
    private static final long COLUMN_IDS_OFFSET = COLUMN_NAME_LENGTHS_OFFSET + Long.BYTES;
    private static final long COLUMN_PHYSICAL_TYPES_OFFSET = COLUMN_IDS_OFFSET + Long.BYTES;
    private final ColumnChunkBuffers chunkBuffers;
    private final ObjectPool<DirectString> directStringPool = new ObjectPool<>(DirectString::new, 16);
    private final Metadata metadata = new Metadata();
    private final Path path;
    private long ptr;

    public PartitionDecoder() {
        try {
            this.chunkBuffers = new ColumnChunkBuffers();
            this.path = new Path();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        destroy();
        Misc.free(path);
        chunkBuffers.free();
    }

    public ColumnChunkBuffers decodeColumnChunk(
            long rowGroup,
            long column,
            int columnType,
            long dataPtr,
            long dataSize,
            long auxPtr,
            long auxSize
    ) {
        assert ptr != 0;
        try {
            chunkBuffers.init();
            decodeColumnChunk(
                    ptr,
                    rowGroup,
                    column,
                    columnType,
                    chunkBuffers.ptr()
            );
        } catch (Throwable th) {
            throw CairoException.critical(0).put("Could not decode partition: [path=").put(path)
                    .put(", exception=").put(th.getClass().getSimpleName())
                    .put(", msg=").put(th.getMessage())
                    .put(']');
        }
        return chunkBuffers;
    }

    public Metadata metadata() {
        assert ptr != 0;
        return metadata;
    }

    public void of(@Transient Path srcPath) {
        destroy();

        path.of(srcPath);
        try {
            ptr = create(path.ptr(), path.size());
            metadata.init();
        } catch (Throwable th) {
            throw CairoException.critical(0).put("Could not describe partition: [path=").put(path)
                    .put(", exception=").put(th.getClass().getSimpleName())
                    .put(", msg=").put(th.getMessage())
                    .put(']');
        }
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
        if (ptr != 0) {
            destroy(ptr);
            ptr = 0;
        }
    }

    public static class ColumnChunkBuffers implements QuietCloseable {
        private static final long DATA_PTR_OFFSET = 0;
        private static final long DATA_SIZE_OFFSET = DATA_PTR_OFFSET + Long.BYTES;
        private static final long AUX_PTR_OFFSET = DATA_SIZE_OFFSET + Long.BYTES;
        private static final long AUX_SIZE_OFFSET = AUX_PTR_OFFSET + Long.BYTES;
        private static final long SIZE = AUX_SIZE_OFFSET + Long.BYTES;

        private long ptr;

        public long auxPtr() {
            assert ptr != 0;
            return Unsafe.getUnsafe().getLong(ptr + AUX_PTR_OFFSET);
        }

        public long auxSize() {
            assert ptr != 0;
            return Unsafe.getUnsafe().getLong(ptr + AUX_SIZE_OFFSET);
        }

        @Override
        public void close() {
            setDataPtr(0);
            setDataSize(0);
            setAuxPtr(0);
            setAuxSize(0);
        }

        public long dataPtr() {
            assert ptr != 0;
            return Unsafe.getUnsafe().getLong(ptr + DATA_PTR_OFFSET);
        }

        public long dataSize() {
            assert ptr != 0;
            return Unsafe.getUnsafe().getLong(ptr + DATA_SIZE_OFFSET);
        }

        private void free() {
            if (ptr != 0) {
                ptr = Unsafe.free(ptr, SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        }

        private void init() {
            if (ptr == 0) {
                ptr = Unsafe.malloc(SIZE, MemoryTag.NATIVE_DEFAULT);
                Vect.memset(ptr, SIZE, 0);
            }
        }

        private long ptr() {
            return ptr;
        }

        private void setAuxPtr(long auxPtr) {
            assert ptr != 0;
            Unsafe.getUnsafe().putLong(ptr + AUX_PTR_OFFSET, auxPtr);
        }

        private void setAuxSize(long auxSize) {
            assert ptr != 0;
            Unsafe.getUnsafe().putLong(ptr + AUX_SIZE_OFFSET, auxSize);
        }

        private void setDataPtr(long dataPtr) {
            assert ptr != 0;
            Unsafe.getUnsafe().putLong(ptr + DATA_PTR_OFFSET, dataPtr);
        }

        private void setDataSize(long dataSize) {
            assert ptr != 0;
            Unsafe.getUnsafe().putLong(ptr + DATA_SIZE_OFFSET, dataSize);
        }
    }

    public class Metadata {
        private final ObjList<DirectString> columnNames = new ObjList<>();

        public int columnCount() {
            return Unsafe.getUnsafe().getInt(ptr + COLUMN_COUNT_OFFSET);
        }

        public int columnId(int index) {
            long p = Unsafe.getUnsafe().getLong(ptr + COLUMN_IDS_OFFSET);
            return Unsafe.getUnsafe().getInt(p + (long) Integer.BYTES * index);
        }

        public CharSequence columnName(int index) {
            return columnNames.getQuick(index);
        }

        public long columnPhysicalType(int index) {
            long p = Unsafe.getUnsafe().getLong(ptr + COLUMN_PHYSICAL_TYPES_OFFSET);
            return Unsafe.getUnsafe().getLong(p + (long) Long.BYTES * index);
        }

        public long rowCount() {
            return Unsafe.getUnsafe().getLong(ptr + ROW_COUNT_OFFSET);
        }

        public int rowGroupCount() {
            return Unsafe.getUnsafe().getInt(ptr + ROW_GROUP_COUNT_OFFSET);
        }

        private void init() {
            columnNames.clear();
            directStringPool.clear();

            final long columnCount = columnCount();
            final long namesPtr = Unsafe.getUnsafe().getLong(ptr + COLUMN_NAMES_OFFSET);
            final long nameLengthsPtr = Unsafe.getUnsafe().getLong(ptr + COLUMN_NAME_LENGTHS_OFFSET);
            long namesOffset = 0;
            for (long i = 0; i < columnCount; i++) {
                DirectString str = directStringPool.next();
                int len = Unsafe.getUnsafe().getInt(nameLengthsPtr + Integer.BYTES * i);
                str.of(namesPtr + namesOffset, len);
                columnNames.add(str);
                namesOffset += 2L * len;
            }
        }
    }

    static {
        Os.init();
    }
}
