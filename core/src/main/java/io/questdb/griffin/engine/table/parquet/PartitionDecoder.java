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
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.LPSZ;

public class PartitionDecoder implements QuietCloseable {
    public static final int BOOLEAN_PHYSICAL_TYPE = 0;
    public static final int BYTE_ARRAY_PHYSICAL_TYPE = 6;
    public static final int DOUBLE_PHYSICAL_TYPE = 5;
    public static final int FIXED_LEN_BYTE_ARRAY_PHYSICAL_TYPE = 7;
    public static final int FLOAT_PHYSICAL_TYPE = 4;
    public static final int INT32_PHYSICAL_TYPE = 1;
    public static final int INT64_PHYSICAL_TYPE = 2;
    private static final long CHUNK_AUX_PTR_OFFSET;
    private static final long CHUNK_DATA_PTR_OFFSET;
    private static final long CHUNK_ROW_GROUP_COUNT_PTR_OFFSET;
    private static final long COLUMNS_PTR_OFFSET;
    private static final long COLUMN_COUNT_OFFSET;
    private final static long COLUMN_IDS_OFFSET;
    private static final long COLUMN_RECORD_NAME_PTR_OFFSET;
    private static final long COLUMN_RECORD_NAME_SIZE_OFFSET;
    private static final long COLUMN_RECORD_SIZE;
    private static final long COLUMN_RECORD_TYPE_OFFSET;
    private static final Log LOG = LogFactory.getLog(PartitionDecoder.class);
    private static final long ROW_COUNT_OFFSET;
    private static final long ROW_GROUP_COUNT_OFFSET;
    private final ObjectPool<DirectString> directStringPool = new ObjectPool<>(DirectString::new, 16);
    private final FilesFacade ff;
    private final Metadata metadata = new Metadata();
    private long columnsPtr;
    private int fd;
    private long ptr;

    public PartitionDecoder(FilesFacade ff) {
        this.ff = ff;
    }

    public static long getChunkAuxPtr(long chunkPtr) {
        return Unsafe.getUnsafe().getLong(chunkPtr + CHUNK_AUX_PTR_OFFSET);
    }

    public static long getChunkDataPtr(long chunkPtr) {
        return Unsafe.getUnsafe().getLong(chunkPtr + CHUNK_DATA_PTR_OFFSET);
    }

    public static long getRowGroupRowCount(long chunkPtr) {
        return Unsafe.getUnsafe().getLong(chunkPtr + CHUNK_ROW_GROUP_COUNT_PTR_OFFSET);
    }

    @Override
    public void close() {
        destroy();
        // parquet decoder will close the FD
        fd = -1;
    }

    public long decodeColumnChunk(
            long rowGroup,
            long columnId,
            int columnType
    ) {
        assert ptr != 0;
        try {
            return decodeColumnChunk(
                    ptr,
                    rowGroup,
                    columnId,
                    columnType
            );
        } catch (Throwable th) {
            LOG.error().$("could not decode [fd=").$(fd)
                    .$(", columnId=").$(columnId)
                    .$(", rowGroup=").$(rowGroup)
                    .$(", msg=").$(th.getMessage())
                    .$(']').$();

            throw CairoException.nonCritical().put(th.getMessage());
        }
    }

    public Metadata getMetadata() {
        assert ptr != 0;
        return metadata;
    }

    public void of(@Transient LPSZ srcPath) {
        destroy();
        this.fd = TableUtils.openRO(ff, srcPath, LOG);
        try {
            ptr = create(Files.detach(fd));
            columnsPtr = Unsafe.getUnsafe().getLong(ptr + COLUMNS_PTR_OFFSET);
            metadata.init();
        } catch (Throwable th) {
            throw CairoException.nonCritical().put("could not read parquet file: [path=").put(srcPath)
                    .put(", msg=").put(th.getMessage())
                    .put(']');
        }
    }

    private static native long chunkAuxPtrOffset();

    private static native long chunkDataPtrOffset();

    private static native long chunkRowGroupCountPtrOffset();

    private static native long columnCountOffset();

    private static native long columnIdsOffset();

    private static native long columnRecordNamePtrOffset();

    private static native long columnRecordNameSizeOffset();

    private static native long columnRecordPhysicalTypeOffset();

    private static native long columnRecordSize();

    private static native long columnRecordTypeOffset();

    private static native long columnsPtrOffset();

    private static native long create(int fd);

    private static native long decodeColumnChunk(
            long decoderPtr,
            long columnId,
            long rowGroup,
            int columnType
    );

    private static native void destroy(long impl);

    private static native long rowCountOffset();

    private static native long rowGroupCountOffset();

    private void destroy() {
        if (ptr != 0) {
            destroy(ptr);
            ptr = 0;
        }
    }

    public class Metadata {
        private final ObjList<DirectString> columnNames = new ObjList<>();

        public int columnCount() {
            return Unsafe.getUnsafe().getInt(ptr + COLUMN_COUNT_OFFSET);
        }

        public int columnId(int index) {
            return Unsafe.getUnsafe().getInt(columnsPtr + index * COLUMN_RECORD_SIZE + COLUMN_IDS_OFFSET);
        }

        public CharSequence columnName(int index) {
            return columnNames.getQuick(index);
        }

        public void copyTo(GenericRecordMetadata metadata) {
            metadata.clear();
            final int columnCount = columnCount();
            for (int i = 0; i < columnCount; i++) {
                metadata.add(new TableColumnMetadata(Chars.toString(columnName(i)), getColumnType(i)));
            }
        }

        public int getColumnType(int index) {
            return Unsafe.getUnsafe().getInt(columnsPtr + index * COLUMN_RECORD_SIZE + COLUMN_RECORD_TYPE_OFFSET);
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
            long currentColumnPtr = columnsPtr;
            for (long i = 0; i < columnCount; i++) {
                DirectString str = directStringPool.next();
                int len = Unsafe.getUnsafe().getInt(currentColumnPtr + COLUMN_RECORD_NAME_SIZE_OFFSET);
                long colNamePtr = Unsafe.getUnsafe().getLong(currentColumnPtr + COLUMN_RECORD_NAME_PTR_OFFSET);
                str.of(colNamePtr, len);
                columnNames.add(str);
                currentColumnPtr += COLUMN_RECORD_SIZE;
            }
        }
    }

    static {
        Os.init();

        COLUMN_COUNT_OFFSET = columnCountOffset();
        COLUMNS_PTR_OFFSET = columnsPtrOffset();
        ROW_COUNT_OFFSET = rowCountOffset();
        COLUMN_RECORD_SIZE = columnRecordSize();
        COLUMN_RECORD_TYPE_OFFSET = columnRecordTypeOffset();
        COLUMN_RECORD_NAME_SIZE_OFFSET = columnRecordNameSizeOffset();
        COLUMN_RECORD_NAME_PTR_OFFSET = columnRecordNamePtrOffset();
        ROW_GROUP_COUNT_OFFSET = rowGroupCountOffset();
        COLUMN_IDS_OFFSET = columnIdsOffset();
        CHUNK_DATA_PTR_OFFSET = chunkDataPtrOffset();
        CHUNK_AUX_PTR_OFFSET = chunkAuxPtrOffset();
        CHUNK_ROW_GROUP_COUNT_PTR_OFFSET = chunkRowGroupCountPtrOffset();
    }
}
