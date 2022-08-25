/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo.wal;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.ObjList;

import static io.questdb.cairo.TableUtils.*;

public class WalUtils {
    public static final int WAL_FORMAT_VERSION = 0;
    public static final String WAL_NAME_BASE = "wal";
    public static final String WAL_INDEX_FILE_NAME = "_wal_index.d";
    public static final long SEQ_META_OFFSET_WAL_LENGTH = 0;
    public static final long SEQ_META_OFFSET_WAL_VERSION = SEQ_META_OFFSET_WAL_LENGTH + Integer.BYTES;
    public static final long SEQ_META_OFFSET_STRUCTURE_VERSION = SEQ_META_OFFSET_WAL_VERSION + Integer.BYTES;
    public static final long SEQ_META_OFFSET_COLUMN_COUNT = SEQ_META_OFFSET_STRUCTURE_VERSION + Long.BYTES;
    public static final long SEQ_META_OFFSET_TIMESTAMP_INDEX = SEQ_META_OFFSET_COLUMN_COUNT + Integer.BYTES;

    public static final long SEQ_META_TABLE_ID = SEQ_META_OFFSET_TIMESTAMP_INDEX + Integer.BYTES;
    public static final long SEQ_META_OFFSET_COLUMNS = SEQ_META_TABLE_ID + Integer.BYTES;

    static void loadSequencerMetadata(
            MemoryMR metaMem,
            ObjList<TableColumnMetadata> columnMetadata,
            LowerCaseCharSequenceIntHashMap nameIndex
    ) {
        try {
            final long memSize = checkMemSize(metaMem, SEQ_META_OFFSET_COLUMNS);
            validateMetaVersion(metaMem, SEQ_META_OFFSET_WAL_VERSION, WAL_FORMAT_VERSION);
            final int columnCount = getColumnCount(metaMem, SEQ_META_OFFSET_COLUMN_COUNT);
            final int timestampIndex = getTimestampIndex(metaMem, SEQ_META_OFFSET_TIMESTAMP_INDEX, columnCount);

            // load column types and names
            long offset = SEQ_META_OFFSET_COLUMNS;
            for (int i = 0; i < columnCount; i++) {
                final int type = getColumnType(metaMem, memSize, offset, i);
                offset += Integer.BYTES;

                final String name = getColumnName(metaMem, memSize, offset, i).toString();
                offset += Vm.getStorageLength(name);

                // Negative type means deleted column, but it is still loaded included
                nameIndex.put(name, i);

                if (ColumnType.isSymbol(Math.abs(type))) {
                    columnMetadata.add(new TableColumnMetadata(name, -1L, type, true, 1024, true, null));
                } else {
                    columnMetadata.add(new TableColumnMetadata(name, -1L, type));
                }
            }

            // validate designated timestamp column
            if (timestampIndex != -1) {
                final int timestampType = columnMetadata.getQuick(timestampIndex).getType();
                if (!ColumnType.isTimestamp(timestampType)) {
                    throw validationException(metaMem).put("Timestamp column must be TIMESTAMP, but found ").put(ColumnType.nameOf(timestampType));
                }
            }
        } catch (Throwable e) {
            nameIndex.clear();
            columnMetadata.clear();
            throw e;
        }
    }
}
