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

package io.questdb.tasks;

import io.questdb.cairo.TableToken;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class ColumnPurgeTask implements Mutable {
    public static final int BLOCK_SIZE = 4;
    public static final int OFFSET_COLUMN_VERSION = 0;
    public static final int OFFSET_PARTITION_NAME_TXN = 2;
    public static final int OFFSET_PARTITION_TIMESTAMP = 1;
    public static final int OFFSET_UPDATE_ROW_ID = 3;
    private final LongList updatedColumnInfo = new LongList();
    private CharSequence columnName;
    private int columnType;
    private int partitionBy;
    private int tableId;
    private TableToken tableName;
    private long truncateVersion;
    private long updateTxn;

    public void appendColumnInfo(long columnVersion, long partitionTimestamp, long partitionNameTxn) {
        updatedColumnInfo.add(columnVersion, partitionTimestamp, partitionNameTxn, 0L);
    }

    public void appendColumnInfo(long columnVersion, long partitionTimestamp, long partitionNameTxn, long updateRowId) {
        updatedColumnInfo.add(columnVersion, partitionTimestamp, partitionNameTxn, updateRowId);
    }

    @Override
    public void clear() {
        updatedColumnInfo.clear();
    }

    public void copyFrom(ColumnPurgeTask inTask) {
        this.tableName = inTask.tableName;
        this.columnName = inTask.columnName;
        this.tableId = inTask.tableId;
        this.partitionBy = inTask.partitionBy;
        this.updateTxn = inTask.updateTxn;
        this.columnType = inTask.columnType;
        this.truncateVersion = inTask.truncateVersion;
        this.updatedColumnInfo.clear();
        this.updatedColumnInfo.add(inTask.updatedColumnInfo);
    }

    public CharSequence getColumnName() {
        return columnName;
    }

    public int getColumnType() {
        return columnType;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public int getTableId() {
        return tableId;
    }

    public TableToken getTableName() {
        return tableName;
    }

    public long getTruncateVersion() {
        return truncateVersion;
    }

    public long getUpdateTxn() {
        return updateTxn;
    }

    public LongList getUpdatedColumnInfo() {
        return updatedColumnInfo;
    }

    public boolean isEmpty() {
        return tableName == null;
    }

    public void of(
            @NotNull
            TableToken tableName,
            String columnName,
            int tableId,
            long truncateVersion,
            int columnType,
            int partitionBy,
            long updateTxn
    ) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.tableId = tableId;
        this.columnType = columnType;
        this.partitionBy = partitionBy;
        this.updateTxn = updateTxn;
        this.truncateVersion = truncateVersion;
        this.updatedColumnInfo.clear();
    }

    public void of(
            @NotNull
            TableToken tableName,
            String columnName,
            int tableId,
            int truncateVersion,
            int columnType,
            int partitionBy,
            long updateTxn,
            @Transient LongList columnVersions
    ) {
        of(tableName, columnName, tableId, truncateVersion, columnType, partitionBy, updateTxn);
        this.updatedColumnInfo.add(columnVersions);
    }

    public void of(
            @NotNull
            TableToken tableName,
            String columnName,
            int tableId,
            int truncateVersion,
            int columnType,
            int partitionBy,
            long updateTxn,
            @Transient LongList columnVersions,
            int columnVersionsLo,
            int columnVersionsHi
    ) {
        of(tableName, columnName, tableId, truncateVersion, columnType, partitionBy, updateTxn);
        this.updatedColumnInfo.add(columnVersions, columnVersionsLo, columnVersionsHi);
    }
}
