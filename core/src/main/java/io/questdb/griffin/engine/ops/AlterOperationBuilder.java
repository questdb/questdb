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

import io.questdb.cairo.TableToken;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;

import static io.questdb.griffin.engine.ops.AlterOperation.*;

public class AlterOperationBuilder {
    private final LongList longList = new LongList();
    private final ObjList<CharSequence> objCharList = new ObjList<>();
    private final AlterOperation resultInstance;
    private short command;
    private int tableId = -1;
    private int tableNamePosition = -1;
    private TableToken tableToken;

    // builder and the operation it is building share two lists.
    public AlterOperationBuilder() {
        this.resultInstance = new AlterOperation(longList, objCharList);
    }

    public void addColumnToList(
            CharSequence columnName,
            int columnNamePosition,
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
        longList.add(columnNamePosition);
    }

    public void addPartitionToList(long timestamp, int timestampPosition) {
        longList.add(timestamp);
        longList.add(timestampPosition);
    }

    public AlterOperation build() {
        return resultInstance.of(command, tableToken, tableId, tableNamePosition);
    }

    public void clear() {
        resultInstance.clear();
        objCharList.clear();
        longList.clear();
        command = DO_NOTHING;
        tableToken = null;
        tableId = -1;
        tableNamePosition = -1;
    }

    public AlterOperationBuilder ofAddColumn(int tableNamePosition, TableToken tableToken, int tableId) {
        this.command = ADD_COLUMN;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        return this;
    }

    public void ofAddColumn(
            CharSequence columnName,
            int columnNamePosition,
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
        longList.add(columnNamePosition);
    }

    public AlterOperationBuilder ofAddIndex(int tableNamePosition, TableToken tableToken, int tableId, CharSequence columnName, int indexValueBlockSize) {
        this.command = ADD_INDEX;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        this.objCharList.add(columnName);
        this.longList.add(indexValueBlockSize);
        return this;
    }

    public AlterOperationBuilder ofAttachPartition(int tableNamePosition, TableToken tableToken, int tableId) {
        this.command = ATTACH_PARTITION;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        return this;
    }

    public AlterOperationBuilder ofCacheSymbol(int tableNamePosition, TableToken tableToken, int tableId, CharSequence columnName) {
        this.command = ADD_SYMBOL_CACHE;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        this.objCharList.add(columnName);
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
        this.objCharList.add(columnName);
        return this;
    }

    public AlterOperationBuilder ofDropColumn(int tableNamePosition, TableToken tableToken, int tableId) {
        this.command = DROP_COLUMN;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        return this;
    }

    public AlterOperationBuilder ofDropIndex(int tableNamePosition, TableToken tableToken, int tableId, CharSequence columnName, int columnNamePosition) {
        this.command = DROP_INDEX;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        this.objCharList.add(columnName);
        this.longList.add(columnNamePosition);
        return this;
    }

    public AlterOperationBuilder ofDropPartition(int tableNamePosition, TableToken tableToken, int tableId) {
        this.command = DROP_PARTITION;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        return this;
    }

    public AlterOperationBuilder ofRemoveCacheSymbol(int tableNamePosition, TableToken tableToken, int tableId, CharSequence columnName) {
        assert columnName != null && columnName.length() > 0;
        this.command = REMOVE_SYMBOL_CACHE;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        this.objCharList.add(columnName);
        return this;
    }

    public AlterOperationBuilder ofRenameColumn(int tableNamePosition, TableToken tableToken, int tableId) {
        this.command = RENAME_COLUMN;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.tableId = tableId;
        return this;
    }

    public void ofRenameColumn(CharSequence columnName, CharSequence newName) {
        objCharList.add(columnName);
        objCharList.add(newName);
    }

    public AlterOperationBuilder ofSetO3MaxLag(int tableNamePosition, TableToken tableToken, int tableId, long o3MaxLag) {
        this.command = SET_PARAM_COMMIT_LAG;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.longList.add(o3MaxLag);
        this.tableId = tableId;
        return this;
    }

    public AlterOperationBuilder ofSetParamUncommittedRows(int tableNamePosition, TableToken tableToken, int tableId, int maxUncommittedRows) {
        this.command = SET_PARAM_MAX_UNCOMMITTED_ROWS;
        this.tableNamePosition = tableNamePosition;
        this.tableToken = tableToken;
        this.longList.add(maxUncommittedRows);
        this.tableId = tableId;
        return this;
    }
}
