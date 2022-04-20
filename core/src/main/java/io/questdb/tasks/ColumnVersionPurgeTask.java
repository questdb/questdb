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

package io.questdb.tasks;

import io.questdb.std.LongList;
import io.questdb.std.Mutable;

public class ColumnVersionPurgeTask implements Mutable {
    public static final int BLOCK_SIZE = 4;
    public static final int OFFSET_COLUMN_VERSION = 0;
    public static final int OFFSET_PARTITION_TIMESTAMP = 1;
    public static final int OFFSET_PARTITION_NAME_TXN = 2;
    private final LongList updatedColumnVersions = new LongList();
    private CharSequence columnName;
    private String tableName;
    private int tableId;
    private int partitionBy;
    private long updatedTxn;
    private int columnType;
    private int truncateVersion;

    public void appendColumnVersion(long columnVersion, long partitionTimestamp, long partitionNameTxn) {
        updatedColumnVersions.add(columnVersion, partitionTimestamp, partitionNameTxn, 0L);
    }

    public void appendColumnVersion(long columnVersion, long partitionTimestamp, long partitionNameTxn, long updateRowId) {
        updatedColumnVersions.add(columnVersion, partitionTimestamp, partitionNameTxn, updateRowId);
    }

    @Override
    public void clear() {
        updatedColumnVersions.clear();
    }

    public void copyFrom(ColumnVersionPurgeTask inTask) {
        this.tableName = inTask.tableName;
        this.columnName = inTask.columnName;
        this.tableId = inTask.tableId;
        this.partitionBy = inTask.partitionBy;
        this.updatedTxn = inTask.updatedTxn;
        this.columnType = inTask.columnType;
        this.truncateVersion = inTask.truncateVersion;
        this.updatedColumnVersions.clear();
        this.updatedColumnVersions.add(inTask.updatedColumnVersions);
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

    public String getTableName() {
        return tableName;
    }

    public int getTruncateVersion() {
        return truncateVersion;
    }

    public LongList getUpdatedColumnVersions() {
        return updatedColumnVersions;
    }

    public long getUpdatedTxn() {
        return updatedTxn;
    }

    public void of(
            String tableName,
            CharSequence columnName,
            int tableId,
            int truncateVersion,
            int columnType,
            int partitionBy,
            long lastTxn
    ) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.tableId = tableId;
        this.columnType = columnType;
        this.partitionBy = partitionBy;
        this.updatedTxn = lastTxn;
        this.truncateVersion = truncateVersion;
        this.updatedColumnVersions.clear();
    }

    public void of(
            String tableName,
            CharSequence columnName,
            int tableId,
            int truncateVersion,
            int columnType,
            int partitionBy,
            long lastTxn,
            LongList columnVersions
    ) {
        of(tableName, columnName, tableId, truncateVersion, columnType, partitionBy, lastTxn);
        this.updatedColumnVersions.add(columnVersions);
    }
}
