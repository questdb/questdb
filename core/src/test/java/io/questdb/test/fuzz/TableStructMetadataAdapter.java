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

package io.questdb.test.fuzz;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.sql.RecordMetadata;

public class TableStructMetadataAdapter implements TableStructure {
    private final CairoConfiguration configuration;
    private final boolean isWalEnabled;
    private final RecordMetadata metadata;
    private final int partitionBy;
    private final String tableName;
    private final int timestampIndex;

    public TableStructMetadataAdapter(
            String tableName,
            boolean isWalEnabled,
            RecordMetadata metadata,
            CairoConfiguration configuration,
            int partitionBy,
            int timestampIndex
    ) {
        this.tableName = tableName;
        this.isWalEnabled = isWalEnabled;
        this.metadata = metadata;
        this.configuration = configuration;
        this.partitionBy = partitionBy;
        this.timestampIndex = timestampIndex;
    }

    @Override
    public int getColumnCount() {
        return metadata.getColumnCount();
    }

    @Override
    public CharSequence getColumnName(int columnIndex) {
        return metadata.getColumnName(columnIndex);
    }

    @Override
    public int getColumnType(int columnIndex) {
        int columnType = metadata.getColumnType(columnIndex);
        assert columnType > 0;
        return columnType;
    }

    @Override
    public int getIndexBlockCapacity(int columnIndex) {
        return 256;
    }

    @Override
    public int getMaxUncommittedRows() {
        return configuration.getMaxUncommittedRows();
    }

    @Override
    public long getO3MaxLag() {
        return configuration.getO3MaxLag();
    }

    @Override
    public int getPartitionBy() {
        return partitionBy;
    }

    @Override
    public boolean getSymbolCacheFlag(int columnIndex) {
        // todo: we cache by default by this might not be true for every fuzz test
        return ColumnType.isSymbol(metadata.getColumnType(columnIndex));
    }

    @Override
    public int getSymbolCapacity(int columnIndex) {
        return 4096;
    }

    @Override
    public CharSequence getTableName() {
        return tableName;
    }

    @Override
    public int getTimestampIndex() {
        return timestampIndex;
    }

    @Override
    public boolean isDedupKey(int columnIndex) {
        return metadata.isDedupKey(columnIndex);
    }

    @Override
    public byte getIndexType(int columnIndex) {
        return ColumnType.isSymbol(metadata.getColumnType(columnIndex)) ? IndexType.SYMBOL : IndexType.NONE;
    }

    @Override
    public boolean isWalEnabled() {
        return isWalEnabled;
    }
}
