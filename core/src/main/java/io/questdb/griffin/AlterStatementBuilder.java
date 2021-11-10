///*******************************************************************************
// *     ___                  _   ____  ____
// *    / _ \ _   _  ___  ___| |_|  _ \| __ )
// *   | | | | | | |/ _ \/ __| __| | | |  _ \
// *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
// *    \__\_\\__,_|\___||___/\__|____/|____/
// *
// *  Copyright (c) 2014-2019 Appsicle
// *  Copyright (c) 2019-2022 QuestDB
// *
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *
// *  http://www.apache.org/licenses/LICENSE-2.0
// *
// *  Unless required by applicable law or agreed to in writing, software
// *  distributed under the License is distributed on an "AS IS" BASIS,
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  See the License for the specific language governing permissions and
// *  limitations under the License.
// *
// ******************************************************************************/
//
//package io.questdb.griffin;
//
//import io.questdb.std.LongList;
//import io.questdb.tasks.TableWriterTask;
//
//import static io.questdb.griffin.AlterStatement.*;
//
//public class AlterStatementBuilder<ObjCharSequenceList> {
//    private final ObjCharSequenceList objCharList = new ObjCharSequenceList();
//    private final DirectCharSequenceList directCharList = new DirectCharSequenceList();
//    private final LongList longList = new LongList();
//    // This is only used to serialize Partition name in form 2020-02-12 or 2020-02 or 2020
//    // to exception message using TableUtils.setSinkForPartition
//    private final AlterStatementBuilder.AlterStatementAddColumn alterStatementAddColumnStatement = new AlterStatementBuilder.AlterStatementAddColumn();
//    private final AlterStatementBuilder.AlterStatementChangePartition alterStatementChangePartition = new AlterStatementBuilder.AlterStatementChangePartition();
//    private final AlterStatementBuilder.AlterStatementDropColumn alterStatementDropColumn = new AlterStatementBuilder.AlterStatementDropColumn();
//    private final AlterStatementBuilder.AlterStatementRenameColumn alterStatementRenameColumn = new AlterStatementBuilder.AlterStatementRenameColumn();
//    private short command;
//    private String tableName;
//    private int tableId;
//    private int tableNamePosition;
//    private CharSequenceList charSequenceList;
//
//    public AlterStatementBuilder.AlterStatementAddColumn ofAddColumn(
//            int tableNamePosition,
//            String tableName,
//            int tableId
//    ) {
//        this.command = ADD_COLUMN;
//        this.tableNamePosition = tableNamePosition;
//        this.tableName = tableName;
//        this.tableId = tableId;
//        return alterStatementAddColumnStatement;
//    }
//
//    public AlterStatementBuilder ofAddIndex(int tableNamePosition, String tableName, int tableId, CharSequence columnName, int indexValueBlockSize) {
//        this.command = ADD_INDEX;
//        this.tableNamePosition = tableNamePosition;
//        this.tableName = tableName;
//        this.tableId = tableId;
//        this.objCharList.add(columnName);
//        this.longList.add(indexValueBlockSize);
//        return this;
//    }
//
//    public AlterStatementBuilder.AlterStatementChangePartition ofAttachPartition(int tableNamePosition, String tableName, int tableId) {
//        this.command = ATTACH_PARTITION;
//        this.tableNamePosition = tableNamePosition;
//        this.tableName = tableName;
//        this.tableId = tableId;
//        return alterStatementChangePartition;
//    }
//
//    public AlterStatementBuilder ofCacheSymbol(int tableNamePosition, String tableName, int tableId, CharSequence columnName) {
//        this.command = ADD_SYMBOL_CACHE;
//        this.tableNamePosition = tableNamePosition;
//        this.tableName = tableName;
//        this.tableId = tableId;
//        this.objCharList.add(columnName);
//        return this;
//    }
//
//    public AlterStatementBuilder.AlterStatementDropColumn ofDropColumn(CharSequence columnName) {
//        assert columnName != null && columnName.length() > 0;
//        this.objCharList.add(columnName);
//        return alterStatementDropColumn;
//    }
//
//    public AlterStatementBuilder.AlterStatementDropColumn ofDropColumn(int tableNamePosition, String tableName, int tableId) {
//        this.command = DROP_COLUMN;
//        this.tableNamePosition = tableNamePosition;
//        this.tableName = tableName;
//        this.tableId = tableId;
//        return alterStatementDropColumn;
//    }
//
//    public AlterStatementBuilder.AlterStatementChangePartition ofDropPartition(int tableNamePosition, String tableName, int tableId) {
//        this.command = DROP_PARTITION;
//        this.tableNamePosition = tableNamePosition;
//        this.tableName = tableName;
//        this.tableId = tableId;
//        return alterStatementChangePartition;
//    }
//
//    public AlterStatementBuilder ofRemoveCacheSymbol(int tableNamePosition, String tableName, int tableId, CharSequence columnName) {
//        assert columnName != null && columnName.length() > 0;
//        this.command = REMOVE_SYMBOL_CACHE;
//        this.tableNamePosition = tableNamePosition;
//        this.tableName = tableName;
//        this.tableId = tableId;
//        this.objCharList.add(columnName);
//        return this;
//    }
//
//    public AlterStatementBuilder.AlterStatementRenameColumn ofRenameColumn(int tableNamePosition, String tableName, int tableId) {
//        this.command = RENAME_COLUMN;
//        this.tableNamePosition = tableNamePosition;
//        this.tableName = tableName;
//        this.tableId = tableId;
//        return alterStatementRenameColumn;
//    }
//
//    public AlterStatementBuilder ofSetParamCommitLag(String tableName, int tableId, long commitLag) {
//        this.command = SET_PARAM_COMMIT_LAG;
//        this.tableName = tableName;
//        this.longList.add(commitLag);
//        this.tableId = tableId;
//        return this;
//    }
//
//    public AlterStatementBuilder ofSetParamUncommittedRows(String tableName, int tableId, int maxUncommittedRows) {
//        this.command = SET_PARAM_MAX_UNCOMMITTED_ROWS;
//        this.tableName = tableName;
//        this.longList.add(maxUncommittedRows);
//        this.tableId = tableId;
//        return this;
//    }
//
//    public void serialize(TableWriterTask event) {
//        event.of(TableWriterTask.TSK_ALTER_TABLE, tableId, tableName);
//        event.putShort(command);
//        event.putInt(tableNamePosition);
//        event.putInt(longList.size());
//        for (int i = 0, n = longList.size(); i < n; i++) {
//            event.put(longList.getQuick(i));
//        }
//
//        event.putInt(objCharList.size());
//        for (int i = 0, n = objCharList.size(); i < n; i++) {
//            event.putStr(objCharList.getStrA(i));
//        }
//    }
//
//}
