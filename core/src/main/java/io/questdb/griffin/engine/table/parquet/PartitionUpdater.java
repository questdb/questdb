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

import io.questdb.cairo.CairoException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;

public class PartitionUpdater implements QuietCloseable {
    private long ptr;

    public PartitionUpdater() {
    }

    @Override
    public void close() {
        destroy();
    }

    public void copyRowGroup(short rowGroupIndex) {
        assert ptr != 0;
        copyRowGroup(ptr, rowGroupIndex);
    }

    public long getResultUnusedBytes() {
        assert ptr != 0;
        return getResultUnusedBytes(ptr);
    }

    // call to this method will update file metadata
    // MUST be called after all row groups have been updated
    // returns the final file size
    public long updateFileMetadata() {
        assert ptr != 0;
        return updateFileMetadata(ptr);
    }

    public void addRowGroup(short position, PartitionDescriptor descriptor) {
        final int columnCount = descriptor.getColumnCount();
        final long rowCount = descriptor.getPartitionRowCount();
        final int timestampIndex = descriptor.getTimestampIndex();
        try {
            assert ptr != 0;
            insertRowGroup(
                    ptr,
                    descriptor.tableName.size(),
                    descriptor.tableName.ptr(),
                    position,
                    columnCount,
                    descriptor.getColumnNamesPtr(),
                    descriptor.getColumnNamesLen(),
                    descriptor.getColumnDataPtr(),
                    descriptor.getColumnDataLen(),
                    timestampIndex,
                    rowCount
            );
        } finally {
            descriptor.clear();
        }
    }

    public void of(
            @Transient LPSZ srcPath,
            int readerFd,
            long readFileSize,
            int writerFd,
            long writeFileSize,
            int timestampIndex,
            long compressionCodec,
            boolean statisticsEnabled,
            boolean rawArrayEncoding,
            long rowGroupSize,
            long dataPageSize
    ) {
        final long allocator = Unsafe.getNativeAllocator(MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER);
        destroy();
        ptr = create(  // throws CairoException on error
                allocator,
                srcPath.size(),
                srcPath.ptr(),
                readerFd,
                readFileSize,
                writerFd,
                writeFileSize,
                timestampIndex,
                compressionCodec,
                statisticsEnabled,
                rawArrayEncoding,
                rowGroupSize,
                dataPageSize
        );
    }

    public void sliceRowGroup(short rowGroupIndex, int rowLo, int rowHi) {
        assert ptr != 0;
        assert rowGroupIndex >= 0 : "rowGroupIndex must be >= 0, got " + rowGroupIndex;
        assert rowLo >= 0 : "rowLo must be >= 0, got " + rowLo;
        assert rowHi >= rowLo : "rowHi must be >= rowLo, got rowLo=" + rowLo + " rowHi=" + rowHi;
        sliceRowGroup(ptr, rowGroupIndex, rowLo, rowHi);
    }

    public void updateRowGroup(short rowGroupId, PartitionDescriptor descriptor) {
        final int columnCount = descriptor.getColumnCount();
        final long rowCount = descriptor.getPartitionRowCount();
        final int timestampIndex = descriptor.getTimestampIndex();
        try {
            assert ptr != 0;
            updateRowGroup(  // throws CairoException on error
                    ptr,
                    descriptor.tableName.size(),
                    descriptor.tableName.ptr(),
                    rowGroupId,
                    columnCount,
                    descriptor.getColumnNamesPtr(),
                    descriptor.getColumnNamesLen(),
                    descriptor.getColumnDataPtr(),
                    descriptor.getColumnDataLen(),
                    timestampIndex,
                    rowCount
            );
        } finally {
            descriptor.clear();
        }
    }

    private static native void copyRowGroup(
            long impl,
            short rowGroupIndex
    ) throws CairoException;

    private static native long create(
            long allocator,
            int srcPathLen,
            long srcPathPtr,
            int readerFd,
            long readFileSize,
            int writerFd,
            long writeFileSize,
            int timestampIndex,
            long compressionCodec,
            boolean statisticsEnabled,
            boolean rawArrayEncoding,
            long rowGroupSize,
            long dataPageSize
    ) throws CairoException;

    private static native void destroy(long impl);

    private static native long getResultUnusedBytes(long impl);

    private static native void insertRowGroup(
            long impl,
            int tableNameLen,
            long tableNamePtr,
            short position,
            int columnCount,
            long columnNamesPtr,
            int columnNamesSize,
            long columnDataPtr,
            long columnDataSize,
            int timestampIndex,
            long rowCount
    ) throws CairoException;

    private static native void sliceRowGroup(
            long impl,
            short rowGroupIndex,
            int rowLo,
            int rowHi
    ) throws CairoException;

    // throws CairoException on error, returns file size
    private static native long updateFileMetadata(long impl);

    private static native void updateRowGroup(
            long impl,
            int tableNameLen,
            long tableNamePtr,
            short rowGroupId,
            int columnCount,
            long columnNamesPtr,
            int columnNamesSize,
            long columnDataPtr,
            long columnDataSize,
            int timestampIndex,
            long rowCount
    ) throws CairoException;

    private void destroy() {
        if (ptr != 0) {
            try {
                destroy(ptr);
            } finally {
                ptr = 0;
            }
        }
    }

    static {
        Os.init();
    }
}
