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

package io.questdb.griffin.engine.ops;

import io.questdb.std.LongList;
import io.questdb.std.ObjList;

import static io.questdb.griffin.engine.ops.AlterOperation.*;

public class AlterOperationBuilder {
    private final ObjList<CharSequence> objCharList = new ObjList<>();
    private final LongList longList = new LongList();
    private final AlterOperation resultInstance;
    // This is only used to serialize Partition name in form 2020-02-12 or 2020-02 or 2020
    // to exception message using TableUtils.setSinkForPartition
    private short command;
    private String tableName;
    private int tableId = -1;
    private int tableNamePosition = -1;

    public AlterOperationBuilder() {
        this.resultInstance = new AlterOperation(longList, objCharList);
    }

    public void clear() {
        resultInstance.clear();
        objCharList.clear();
        longList.clear();
        command = DO_NOTHING;
        tableName = null;
        tableId = -1;
        tableNamePosition = -1;
    }

    public AlterOperation build() {
        return resultInstance.of(command, tableName, tableId, tableNamePosition);
    }

    public AlterOperationBuilder ofAddColumn(int tableNamePosition, String tableName, int tableId) {
        this.command = ADD_COLUMN;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        this.tableId = tableId;
        return this;
    }

    public AlterOperationBuilder ofAddIndex(int tableNamePosition, String tableName, int tableId, CharSequence columnName, int indexValueBlockSize) {
        this.command = ADD_INDEX;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        this.tableId = tableId;
        this.objCharList.add(columnName);
        this.longList.add(indexValueBlockSize);
        return this;
    }

    public AlterOperationBuilder ofDropIndex(int tableNamePosition, String tableName, int tableId, CharSequence columnName) {
        this.command = DROP_INDEX;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        this.tableId = tableId;
        this.objCharList.add(columnName);
        return this;
    }

    public AlterOperationBuilder ofAttachPartition(int tableNamePosition, String tableName, int tableId) {
        this.command = ATTACH_PARTITION;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        this.tableId = tableId;
        return this;
    }

    public AlterOperationBuilder ofCacheSymbol(int tableNamePosition, String tableName, int tableId, CharSequence columnName) {
        this.command = ADD_SYMBOL_CACHE;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        this.tableId = tableId;
        this.objCharList.add(columnName);
        return this;
    }

    public AlterOperationBuilder ofRemoveCacheSymbol(int tableNamePosition, String tableName, int tableId, CharSequence columnName) {
        assert columnName != null && columnName.length() > 0;
        this.command = REMOVE_SYMBOL_CACHE;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        this.tableId = tableId;
        this.objCharList.add(columnName);
        return this;
    }

    public AlterOperationBuilder ofDropColumn(CharSequence columnName) {
        assert columnName != null && columnName.length() > 0;
        this.objCharList.add(columnName);
        return this;
    }

    public AlterOperationBuilder ofDropColumn(int tableNamePosition, String tableName, int tableId) {
        this.command = DROP_COLUMN;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        this.tableId = tableId;
        return this;
    }

    public AlterOperationBuilder ofDropPartition(int tableNamePosition, String tableName, int tableId) {
        this.command = DROP_PARTITION;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        this.tableId = tableId;
        return this;
    }

    public AlterOperationBuilder ofRenameColumn(int tableNamePosition, String tableName, int tableId) {
        this.command = RENAME_COLUMN;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        this.tableId = tableId;
        return this;
    }

    public AlterOperationBuilder ofSetParamCommitLag(String tableName, int tableId, long commitLag) {
        this.command = SET_PARAM_COMMIT_LAG;
        this.tableName = tableName;
        this.longList.add(commitLag);
        this.tableId = tableId;
        return this;
    }

    public AlterOperationBuilder ofSetParamUncommittedRows(String tableName, int tableId, int maxUncommittedRows) {
        this.command = SET_PARAM_MAX_UNCOMMITTED_ROWS;
        this.tableName = tableName;
        this.longList.add(maxUncommittedRows);
        this.tableId = tableId;
        return this;
    }

    public void ofRenameColumn(CharSequence columnName, CharSequence newName) {
        objCharList.add(columnName);
        objCharList.add(newName);
    }

    public void ofPartition(long timestamp) {
        longList.add(timestamp);
    }

    public void ofAddColumn(
            CharSequence columnName,
            int type,
            int symbolCapacity,
            boolean cache,
            boolean indexed,
            int indexValueBlockCapacity
    ) {
        assert columnName != null && columnName.length() > 0;
        objCharList.add(columnName);
        longList.add(type);
        longList.add(symbolCapacity);
        longList.add(cache ? 1 : -1);
        longList.add(indexed ? 1 : -1);
        longList.add(indexValueBlockCapacity);
    }
}
