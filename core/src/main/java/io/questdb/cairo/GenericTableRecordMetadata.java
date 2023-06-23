/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.cairo;

import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.wal.seq.TableRecordMetadataSink;

public class GenericTableRecordMetadata extends GenericRecordMetadata implements TableRecordMetadata, TableRecordMetadataSink {
    private long metadataVersion;
    private int tableId;
    private TableToken tableToken;

    @Override
    public void addColumn(
            String columnName,
            int columnType,
            boolean columnIndexed,
            int indexValueBlockCapacity,
            boolean symbolTableStatic,
            int writerIndex,
            boolean isDedupKey
    ) {
        if (columnType > -1L) {
            add(
                    new TableColumnMetadata(
                            columnName,
                            columnType,
                            columnIndexed,
                            indexValueBlockCapacity,
                            symbolTableStatic,
                            null,
                            writerIndex,
                            isDedupKey
                    )
            );
        }
    }

    @Override
    public void close() {
    }

    @Override
    public long getMetadataVersion() {
        return metadataVersion;
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public TableToken getTableToken() {
        return tableToken;
    }

    @Override
    public boolean isWalEnabled() {
        // this class is only used for WAL-enabled tables
        return true;
    }

    @Override
    public void of(TableToken tableToken, int tableId, int timestampIndex, int compressedTimestampIndex, boolean suspended, long structureVersion, int columnCount) {
        this.tableToken = tableToken;
        this.tableId = tableId;
        this.timestampIndex = compressedTimestampIndex;
        // todo: suspended
        this.metadataVersion = structureVersion;
        // todo: maxUncommittedRows where from ?
    }

    public void updateTableToken(TableToken tableToken) {
        this.tableToken = tableToken;
    }
}
