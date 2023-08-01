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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
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
            CharSequence name,
            int type,
            int symbolCapacity,
            boolean symbolCacheFlag,
            boolean isIndexed,
            int indexValueBlockCapacity,
            boolean isSequential,
            SqlExecutionContext executionContext
    ) {
        metadata.addColumn(name, type);
    }

    @Override
    public void disableDeduplication() {
        metadata.disableDeduplication();
    }

    @Override
    public void enableDeduplicationWithUpsertKeys(LongList columnsIndexes) {
        metadata.enableDeduplicationWithUpsertKeys(columnsIndexes);
    }

    public TableRecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public TableToken getTableToken() {
        return tableToken;
    }

    @Override
    public void removeColumn(@NotNull CharSequence columnName) {
        metadata.removeColumn(columnName);
    }

    @Override
    public void renameColumn(@NotNull CharSequence columnName, @NotNull CharSequence newName) {
        metadata.renameColumn(columnName, newName);
    }

    @Override
    public void renameTable(@NotNull CharSequence fromNameTable, @NotNull CharSequence toTableName) {
        assert Chars.equalsIgnoreCaseNc(fromNameTable, metadata.getTableToken().getTableName());
        metadata.renameTable(toTableName);
        this.tableToken = metadata.getTableToken();
    }
}
