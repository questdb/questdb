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

package io.questdb.griffin.engine.ops;

import io.questdb.cairo.TableToken;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.ops.AlterOperation.*;

public class AlterOperationBuilder implements Mutable {
    private final LongList extraInfo = new LongList();
    private final ObjList<CharSequence> extraStrInfo = new ObjList<>();
    private final AlterOperation op;
    private short command;
    private int tableId = -1;
    private int tableNamePosition = -1;
    private TableToken tableToken;

    // the builder and the operation it builds share the extraInfo list
    public AlterOperationBuilder() {
        this.op = new AlterOperation(extraInfo, extraStrInfo);
    }

    public void addColumnToList(
            CharSequence columnName,
            int columnNamePosition,
            int type,
            int symbolCapacity,
            boolean cache,
            boolean indexed,
            int indexValueBlockCapacity,
            boolean dedupKey
    ) {
        assert columnName != null && columnName.length() > 0;
        extraStrInfo.add(columnName);
        extraInfo.add(type);
        extraInfo.add(symbolCapacity);
        extraInfo.add(cache ? 1 : -1);
        extraInfo.add(getFlags(indexed, dedupKey));
        extraInfo.add(indexValueBlockCapacity);
        extraInfo.add(columnNamePosition);
    }

    public void addPartitionToList(long timestamp, int timestampPosition) {
        extraInfo.add(timestamp);
        if (command != FORCE_DROP_PARTITION) {
            extraInfo.add(timestampPosition);
        }
    }

    public AlterOperation build() {
        return op.of(command, tableToken, tableId, tableNamePosition);
    }

    @Override
    public void clear() {
        op.clear();
        extraStrInfo.clear();
        extraInfo.clear();
        command = DO_NOTHING;
        tableToken = null;
        tableId = -1;
        tableNamePosition = -1;
    }

    public ObjList<CharSequence> getExtraStrInfo() {
        return extraStrInfo;
    }

    public AlterOperationBuilder ofAddColumn(int tableNamePosition, TableToken tableToken, int tableId) {
        this.command = ADD_COLUMN;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        return this;
    }

    public void ofAddColumn(CharSequence columnName, int columnNamePosition, int type, int symbolCapacity, boolean cache, boolean indexed, int indexValueBlockCapacity) {
        assert columnName != null && columnName.length() > 0;
        extraStrInfo.add(columnName);
        extraInfo.add(type);
        extraInfo.add(symbolCapacity);
        extraInfo.add(cache ? 1 : -1);
        extraInfo.add(getFlags(indexed, false));
        extraInfo.add(indexValueBlockCapacity);
        extraInfo.add(columnNamePosition);
    }

    public void ofAddIndex(int tableNamePosition, TableToken tableToken, int tableId, CharSequence columnName, int indexValueBlockSize) {
        this.command = ADD_INDEX;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        this.extraStrInfo.add(columnName);
        this.extraInfo.add(indexValueBlockSize);
    }

    public AlterOperationBuilder ofAttachPartition(int tableNamePosition, TableToken tableToken, int tableId) {
        this.command = ATTACH_PARTITION;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        return this;
    }

    public void ofCacheSymbol(int tableNamePosition, TableToken tableToken, int tableId, CharSequence columnName) {
        this.command = ADD_SYMBOL_CACHE;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        this.extraStrInfo.add(columnName);
    }

    public AlterOperationBuilder ofColumnChangeType(int tableNamePosition, TableToken tableToken, int tableId) {
        this.command = CHANGE_COLUMN_TYPE;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        return this;
    }

    public AlterOperationBuilder ofConvertPartition(int tableNamePosition, TableToken tableToken, int tableId, boolean toParquet) {
        this.command = toParquet ? CONVERT_PARTITION_TO_PARQUET : CONVERT_PARTITION_TO_NATIVE;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        return this;
    }

    public AlterOperationBuilder ofDedupDisable(int tableNamePosition, TableToken tableToken) {
        this.command = SET_DEDUP_DISABLE;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableToken.getTableId();
        return this;
    }

    public AlterOperationBuilder ofDedupEnable(int tableNamePosition, TableToken tableToken) {
        this.command = SET_DEDUP_ENABLE;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableToken.getTableId();
        return this;
    }

    public AlterOperationBuilder ofDetachPartition(int tableNamePosition, TableToken tableToken, int tableId) {
        this.command = DETACH_PARTITION;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        return this;
    }

    public AlterOperationBuilder ofDropColumn(CharSequence columnName) {
        assert columnName != null && columnName.length() > 0;
        this.extraStrInfo.add(columnName);
        return this;
    }

    public AlterOperationBuilder ofDropColumn(int tableNamePosition, TableToken tableToken, int tableId) {
        this.command = DROP_COLUMN;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        return this;
    }

    public void ofDropIndex(int tableNamePosition, TableToken tableToken, int tableId, CharSequence columnName, int columnNamePosition) {
        this.command = DROP_INDEX;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        this.extraStrInfo.add(columnName);
        this.extraInfo.add(columnNamePosition);
    }

    public AlterOperationBuilder ofDropPartition(int tableNamePosition, TableToken tableToken, int tableId) {
        this.command = DROP_PARTITION;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        return this;
    }

    public AlterOperationBuilder ofForceDropPartition(int tableNamePosition, TableToken tableToken, int tableId) {
        this.command = FORCE_DROP_PARTITION;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        return this;
    }

    public void ofRemoveCacheSymbol(int tableNamePosition, TableToken tableToken, int tableId, CharSequence columnName) {
        assert columnName != null && columnName.length() > 0;
        this.command = REMOVE_SYMBOL_CACHE;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        this.extraStrInfo.add(columnName);
    }

    public AlterOperationBuilder ofRenameColumn(int tableNamePosition, TableToken tableToken, int tableId) {
        this.command = RENAME_COLUMN;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        return this;
    }

    public void ofRenameColumn(CharSequence columnName, CharSequence newName) {
        extraStrInfo.add(columnName);
        extraStrInfo.add(newName);
    }

    public AlterOperationBuilder ofSetMatViewRefresh(
            int matViewNamePosition,
            @NotNull TableToken matViewToken,
            int tableId,
            int refreshType,
            int timerInterval,
            char timerUnit,
            long timerStartUs,
            @Nullable CharSequence timerTimeZone,
            int periodLength,
            char periodLengthUnit,
            int periodDelay,
            char periodDelayUnit
    ) {
        this.command = SET_MAT_VIEW_REFRESH;
        this.tableNamePosition = matViewNamePosition;
        this.tableToken = matViewToken;
        this.extraInfo.add(refreshType);
        this.extraInfo.add(timerInterval);
        this.extraInfo.add(timerUnit);
        this.extraInfo.add(timerStartUs);
        this.extraInfo.add(periodLength);
        this.extraInfo.add(periodLengthUnit);
        this.extraInfo.add(periodDelay);
        this.extraInfo.add(periodDelayUnit);
        this.extraStrInfo.add(timerTimeZone);
        this.tableId = tableId;
        return this;
    }

    public AlterOperationBuilder ofSetMatViewRefreshLimit(int matViewNamePosition, TableToken matViewToken, int tableId, int limitHoursOrMonths) {
        this.command = SET_MAT_VIEW_REFRESH_LIMIT;
        this.tableNamePosition = matViewNamePosition;
        this.tableToken = matViewToken;
        this.extraInfo.add(limitHoursOrMonths);
        this.tableId = tableId;
        return this;
    }

    public AlterOperationBuilder ofSetO3MaxLag(int tableNamePosition, TableToken tableToken, int tableId, long o3MaxLag) {
        this.command = SET_PARAM_COMMIT_LAG;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.extraInfo.add(o3MaxLag);
        this.tableId = tableId;
        return this;
    }

    public AlterOperationBuilder ofSetParamUncommittedRows(int tableNamePosition, TableToken tableToken, int tableId, int maxUncommittedRows) {
        this.command = SET_PARAM_MAX_UNCOMMITTED_ROWS;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.extraInfo.add(maxUncommittedRows);
        this.tableId = tableId;
        return this;
    }

    public AlterOperationBuilder ofSetTtl(int tableNamePosition, TableToken tableToken, int tableId, int ttlHoursOrMonths) {
        this.command = SET_TTL;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.extraInfo.add(ttlHoursOrMonths);
        this.tableId = tableId;
        return this;
    }

    public AlterOperationBuilder ofSquashPartitions(int tableNamePosition, TableToken tableToken) {
        this.command = SQUASH_PARTITIONS;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableToken.getTableId();
        return this;
    }

    public AlterOperationBuilder ofSymbolCapacityChange(int tableNamePosition, TableToken tableToken, int tableId) {
        this.command = CHANGE_SYMBOL_CAPACITY;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        return this;
    }

    public void setDedupKeyFlag(int writerColumnIndex) {
        extraInfo.add(writerColumnIndex);
    }
}
