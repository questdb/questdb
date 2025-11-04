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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.std.LongList;
import org.jetbrains.annotations.NotNull;

public class SequencerMetadataService implements MetadataServiceStub {
    private final SequencerMetadata metadata;
    private TableToken tableToken;

    public SequencerMetadataService(SequencerMetadata metadata, TableToken tableToken) {
        this.metadata = metadata;
        this.tableToken = tableToken;
    }

    @Override
    public void addColumn(
            CharSequence columnName,
            int columnType,
            int symbolCapacity,
            boolean symbolCacheFlag,
            boolean isIndexed,
            int indexValueBlockCapacity,
            boolean isSequential,
            boolean isDedupKey,
            SecurityContext securityContext
    ) {
        metadata.addColumn(
                columnName,
                columnType,
                symbolCapacity,
                symbolCacheFlag,
                isIndexed,
                indexValueBlockCapacity,
                isDedupKey
        );
    }

    @Override
    public void changeColumnType(
            CharSequence columnName,
            int columnType,
            int symbolCapacity,
            boolean symbolCacheFlag,
            boolean isIndexed,
            int indexValueBlockCapacity,
            boolean isSequential,
            SecurityContext securityContext
    ) {
        metadata.changeColumnType(
                columnName,
                columnType,
                symbolCapacity,
                symbolCacheFlag,
                isIndexed,
                indexValueBlockCapacity
        );
    }

    @Override
    public boolean convertPartitionNativeToParquet(long partitionTimestamp) {
        return false;
    }

    @Override
    public boolean convertPartitionParquetToNative(long partitionTimestamp) {
        return false;
    }

    @Override
    public void disableDeduplication() {
        metadata.disableDeduplication();
    }

    @Override
    public boolean enableDeduplicationWithUpsertKeys(LongList columnsIndexes) {
        return metadata.enableDeduplicationWithUpsertKeys();
    }

    public TableRecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public TableToken getTableToken() {
        return tableToken;
    }

    @Override
    public int getTimestampType() {
        return metadata.getTimestampType();
    }

    @Override
    public void removeColumn(@NotNull CharSequence columnName) {
        metadata.removeColumn(columnName);
    }

    @Override
    public void renameColumn(@NotNull CharSequence columnName, @NotNull CharSequence newName, SecurityContext securityContext) {
        metadata.renameColumn(columnName, newName);
    }

    @Override
    public void renameTable(@NotNull CharSequence fromNameTable, @NotNull CharSequence toTableName) {
        metadata.renameTable(toTableName);
        tableToken = metadata.getTableToken();
    }
}
