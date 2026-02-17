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
import io.questdb.cairo.TableUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;

public class PartitionUpdater implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(PartitionUpdater.class);
    private final FilesFacade ff;
    private long ptr;

    public PartitionUpdater(FilesFacade ff) {
        this.ff = ff;
    }

    @Override
    public void close() {
        destroy();
    }

    public void of(
            @Transient LPSZ srcPath,
            int fileOpenOpts,
            long fileSize,
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
                Files.detach(TableUtils.openRW(ff, srcPath, LOG, fileOpenOpts)),
                fileSize,
                timestampIndex,
                compressionCodec,
                statisticsEnabled,
                rawArrayEncoding,
                rowGroupSize,
                dataPageSize
        );
    }

    // call to this method will update file metadata
    // MUST be called after all row groups have been updated
    public void updateFileMetadata() {
        assert ptr != 0;
        updateFileMetadata(ptr);
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

    private static native long create(
            long allocator,
            int srcPathLen,
            long srcPathPtr,
            int fd,
            long fileSize,
            int timestampIndex,
            long compressionCodec,
            boolean statisticsEnabled,
            boolean rawArrayEncoding,
            long rowGroupSize,
            long dataPageSize
    ) throws CairoException;

    private static native void destroy(long impl);

    // throws CairoException on error
    private static native void updateFileMetadata(long impl);

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
