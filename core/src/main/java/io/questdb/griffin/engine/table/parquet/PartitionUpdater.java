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
import io.questdb.cairo.TableUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
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
            long fileOpenOpts,
            long fileSize,
            int timestampIndex,
            long compressionCodec,
            boolean statisticsEnabled,
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
                rowGroupSize,
                dataPageSize
        );
    }

    public void updateRowGroup(short rowGroupId, PartitionDescriptor descriptor) {
        final int columnCount = descriptor.getColumnCount();
        final long rowCount = descriptor.getPartitionRowCount();
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
            long rowGroupSize,
            long dataPageSize
    ) throws CairoException;

    private static native void destroy(long impl);

    private static native void finish(long impl) throws CairoException;

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
            long rowCount
    ) throws CairoException;

    private void destroy() {
        // TODO(eugenels): Extract `finish` to a separate public API method.
        //                 Currently it gets called as part of `close()`, which isn't ideal
        //                 from an exception handling point of view.
        //                 This is because `close()` should not throw exceptions or
        //                 the exception will prevent `destroy()` from being called.
        //                 In other words, we also have a memory leak here :-)
        if (ptr != 0) {
            finish(ptr); // write out metadata
            destroy(ptr);
            ptr = 0;
        }
    }

    static {
        Os.init();
    }
}
