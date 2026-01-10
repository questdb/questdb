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

package io.questdb.cutlass.line.tcp.v4;

import io.questdb.cairo.*;
import io.questdb.cutlass.line.tcp.IlpV4WalAppender;

/**
 * TableStructure implementation for ILP v4 table creation.
 * <p>
 * This adapter allows creating tables from ILP v4 column definitions.
 */
public class IlpV4TableStructureAdapter implements TableStructure {
    private static final String DEFAULT_TIMESTAMP_FIELD = "timestamp";

    private final CairoConfiguration configuration;
    private final int defaultPartitionBy;
    private final boolean walEnabledDefault;

    private IlpV4ColumnDef[] columns;
    private CharSequence tableName;
    private int timestampIndex = -1;

    public IlpV4TableStructureAdapter(
            CairoConfiguration configuration,
            int defaultPartitionBy,
            boolean walEnabledDefault
    ) {
        this.configuration = configuration;
        this.defaultPartitionBy = defaultPartitionBy;
        this.walEnabledDefault = walEnabledDefault;
    }

    /**
     * Configures this adapter for the given table and columns.
     *
     * @param tableName table name
     * @param columns   column definitions
     * @return this adapter
     */
    public IlpV4TableStructureAdapter of(CharSequence tableName, IlpV4ColumnDef[] columns) {
        this.tableName = tableName;
        this.columns = columns;
        this.timestampIndex = -1;

        // Find the designated timestamp column - prefer "timestamp" name over just type
        int firstTimestampTypeIndex = -1;
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].getName().equals(DEFAULT_TIMESTAMP_FIELD)) {
                // Prefer the column explicitly named "timestamp"
                timestampIndex = i;
                break;
            }
            if (firstTimestampTypeIndex == -1 && (columns[i].getTypeCode() & 0x7F) == IlpV4Constants.TYPE_TIMESTAMP) {
                // Remember the first TIMESTAMP type column as fallback
                firstTimestampTypeIndex = i;
            }
        }

        // If no "timestamp" column found, use the first TIMESTAMP type column
        if (timestampIndex == -1) {
            timestampIndex = firstTimestampTypeIndex;
        }

        return this;
    }

    @Override
    public int getColumnCount() {
        return timestampIndex == -1 ? columns.length + 1 : columns.length;
    }

    @Override
    public CharSequence getColumnName(int columnIndex) {
        if (columnIndex == getTimestampIndex()) {
            return DEFAULT_TIMESTAMP_FIELD;
        }
        if (columnIndex >= columns.length) {
            return DEFAULT_TIMESTAMP_FIELD;
        }
        String name = columns[columnIndex].getName();
        if (TableUtils.isValidColumnName(name, configuration.getMaxFileNameLength())) {
            return name;
        }
        throw CairoException.nonCritical()
                .put("column name contains invalid characters [colName=")
                .put(name).put(']');
    }

    @Override
    public int getColumnType(int columnIndex) {
        if (columnIndex == getTimestampIndex()) {
            return ColumnType.TIMESTAMP;
        }
        if (columnIndex >= columns.length) {
            return ColumnType.TIMESTAMP;
        }
        return IlpV4WalAppender.mapIlpV4TypeToQuestDB(columns[columnIndex].getTypeCode() & 0x7F);
    }

    @Override
    public int getIndexBlockCapacity(int columnIndex) {
        return 0;
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
        return defaultPartitionBy;
    }

    @Override
    public boolean getSymbolCacheFlag(int columnIndex) {
        return configuration.getDefaultSymbolCacheFlag();
    }

    @Override
    public int getSymbolCapacity(int columnIndex) {
        return configuration.getDefaultSymbolCapacity();
    }

    @Override
    public CharSequence getTableName() {
        return tableName;
    }

    @Override
    public int getTimestampIndex() {
        return timestampIndex == -1 ? columns.length : timestampIndex;
    }

    @Override
    public boolean isDedupKey(int columnIndex) {
        return false;
    }

    @Override
    public boolean isIndexed(int columnIndex) {
        return false;
    }

    @Override
    public boolean isWalEnabled() {
        // ILP v4 always creates WAL tables for partitioned tables
        // Non-WAL tables are legacy and not recommended for ILP v4
        return PartitionBy.isPartitioned(getPartitionBy());
    }
}
