/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo;

import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;

public class AlterTableImpl implements AlterStatement, AlterStatementAddColumnStatement, AlterStatementRenameColumnStatement, AlterStatementDropColumnStatement, Mutable {
    private final static Log LOG = LogFactory.getLog(AlterTableImpl.class);
    private final int indexValueBlockSize;
    private short command;
    private CharSequence tableName;
    private int tableNamePosition;
    private final LongList partitionList = new LongList();
    private final ObjList<CharSequence> nameList = new ObjList<>();
    private final ExceptionSinkAdapter exceptionSinkAdapter = new ExceptionSinkAdapter();

    public AlterTableImpl(final CairoConfiguration configuration) {
        indexValueBlockSize = configuration.getIndexValueBlockSize();
    }

    @Override
    public void apply(TableWriter tableWriter) throws SqlException {
        try {
            switch (command) {
                case DO_NOTHING:
                    // Lock cannot be applied on another thread.
                    // it is applied at the SQL compilation time
                    break;
                case ADD_COLUMN:
                    applyAddColumn(tableWriter);
                    break;
                case DROP_PARTITION:
                    applyDropPartition(tableWriter);
                    break;
                case ATTACH_PARTITION:
                    applyAttachPartition(tableWriter);
                    break;
                case ADD_INDEX:
                    applyAddIndex(tableWriter);
                    break;
                case ADD_SYMBOL_CACHE:
                    applySetSymbolCache(tableWriter, true);
                    break;
                case REMOVE_SYMBOL_CACHE:
                    applySetSymbolCache(tableWriter, false);
                    break;
                case DROP_COLUMN:
                    applyDropColumn(tableWriter);
                    break;
                case RENAME_COLUMN:
                    applyRenameColumn(tableWriter);
                    break;
                case SET_PARAM_MAX_UNCOMMITTED_ROWS:
                    applyParamUncommittedRows(tableWriter);
                    break;
                case SET_PARAM_COMMIT_LAG:
                    applyParamCommitLag(tableWriter);
                    break;
                default:
                    throw CairoException.instance(0).put("Invalid alter table command [code=").put(command).put(']');
            }
        } catch (EntryUnavailableException | SqlException ex) {
            throw ex;
        } catch (CairoException e2) {
            LOG.error().$("table '")
                    .$(tableName)
                    .$("' could not be altered [")
                    .$(e2.getErrno())
                    .$("] ")
                    .$(e2.getFlyweightMessage());

            throw SqlException.$(tableNamePosition, "table '")
                    .put(tableName)
                    .put("' could not be altered: [")
                    .put(e2.getErrno())
                    .put("] ")
                    .put(e2.getFlyweightMessage());
        }
    }

    @Override
    public void clear() {
        command = DO_NOTHING;
        nameList.clear();
        partitionList.clear();
    }

    @Override
    public CharSequence getTableName() {
        return tableName;
    }

    public AlterStatement doNothing() {
        this.command = DO_NOTHING;
        this.tableName = null;
        return this;
    }

    public AlterStatement doNoting(CharSequence tableName) {
        this.command = DO_NOTHING;
        this.tableName = tableName;
        return this;
    }

    public LongList getPartitionList() {
        return partitionList;
    }

    public AlterStatementAddColumnStatement ofAddColumn(
            int tableNamePosition,
            CharSequence tableName
    ) {
        this.command = ADD_COLUMN;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        return this;
    }

    @Override
    public AlterStatementAddColumnStatement ofAddColumn(CharSequence columnName, int type, int symbolCapacity, boolean cache, boolean indexed, int indexValueBlockCapacity) {
        this.nameList.add(columnName);
        this.partitionList.add(type);
        this.partitionList.add(symbolCapacity);
        this.partitionList.add(cache ? 1 : -1);
        this.partitionList.add(indexed ? 1 : -1);
        this.partitionList.add(indexValueBlockCapacity);
        return this;
    }

    public AlterStatement ofAddIndex(int tableNamePosition, CharSequence tableName, CharSequence columnName) {
        this.command = ADD_INDEX;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        this.nameList.add(columnName);
        return this;
    }

    public AlterStatement ofAttachPartition(int tableNamePosition, CharSequence tableName) {
        this.command = ATTACH_PARTITION;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        return this;
    }

    public AlterStatement ofCacheSymbol(int tableNamePosition, CharSequence tableName, CharSequence columnName) {
        this.command = ADD_SYMBOL_CACHE;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        this.nameList.add(columnName);
        return this;
    }

    @Override
    public AlterStatementRenameColumnStatement ofRenameColumn(CharSequence columnName, CharSequence newName) {
        this.nameList.add(columnName);
        this.nameList.add(newName);
        return null;
    }

    @Override
    public AlterStatementDropColumnStatement ofDropColumn(CharSequence columnName) {
        this.nameList.add(columnName);
        return this;
    }

    public AlterStatementDropColumnStatement ofDropColumn(int tableNamePosition, CharSequence tableName) {
        this.command = DROP_COLUMN;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        return this;
    }

    public AlterStatement ofDropPartition(int tableNamePosition, CharSequence tableName) {
        this.command = DROP_PARTITION;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        return this;
    }

    public AlterStatement ofRemoveCacheSymbol(int tableNamePosition, CharSequence tableName, CharSequence columnName) {
        this.command = REMOVE_SYMBOL_CACHE;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        this.nameList.add(columnName);
        return this;
    }

    public AlterStatementRenameColumnStatement ofRenameColumn(int tableNamePosition, CharSequence tableName) {
        this.command = RENAME_COLUMN;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        return this;
    }

    public AlterStatement ofSetParamCommitLag(CharSequence tableName, long commitLag) {
        this.command = SET_PARAM_COMMIT_LAG;
        this.tableName = tableName;
        this.partitionList.add(commitLag);
        return this;
    }

    public AlterStatement ofSetParamUncommittedRows(CharSequence tableName, int maxUncommittedRows) {
        this.command = SET_PARAM_MAX_UNCOMMITTED_ROWS;
        this.tableName = tableName;
        this.partitionList.add(maxUncommittedRows);
        return this;
    }

    private void applyAddColumn(TableWriter tableWriter) throws SqlException {
        int lParam = 0;
        for(int i = 0, n = nameList.size(); i < n; i++) {
            CharSequence columnName = nameList.get(i);
            int type = (int) partitionList.get(lParam++);
            int symbolCapacity = (int) partitionList.get(lParam++);
            boolean symbolCacheFlag = partitionList.get(lParam++) > 0;
            boolean isIndexed = partitionList.get(lParam++) > 0;
            int indexValueBlockCapacity = (int) partitionList.get(lParam++);
            try {
                tableWriter.addColumn(
                        columnName,
                        type,
                        symbolCapacity,
                        symbolCacheFlag,
                        isIndexed,
                        indexValueBlockCapacity,
                        false
                );
            } catch (CairoException e) {
                LOG.error().$("Cannot add column '").$(tableWriter.getTableName()).$('.').$(columnName).$("'. Exception: ").$((Sinkable) e).$();
                throw SqlException.$(tableNamePosition, "could add column [error=").put(e.getFlyweightMessage())
                        .put(", errno=").put(e.getErrno())
                        .put(']');
            }
        }
    }

    private void applyAddIndex(TableWriter tableWriter) throws SqlException {
        CharSequence columnName = nameList.get(0);
        try {
            tableWriter.addIndex(columnName, indexValueBlockSize);
        } catch (CairoException e) {
            throw SqlException.position(tableNamePosition).put(e.getFlyweightMessage())
                    .put("[errno=").put(e.getErrno()).put(']');
        }
    }

    private void applyAttachPartition(TableWriter tableWriter) throws SqlException {
        for (int i = 0, n = partitionList.size(); i < n; i++) {
            long partitionTimestamp = partitionList.getQuick(i);
            try {
                int statusCode = tableWriter.attachPartition(partitionTimestamp);
                switch (statusCode) {
                    case StatusCode.OK:
                        break;
                    case StatusCode.CANNOT_ATTACH_MISSING_PARTITION:
                        throw putPartitionName(
                                SqlException.$(tableNamePosition, "attach partition failed, folder '"),
                                tableWriter.getPartitionBy(),
                                partitionTimestamp)
                                .put("' does not exist");
                    case StatusCode.TABLE_HAS_SYMBOLS:
                        throw SqlException.$(tableNamePosition, "attaching partitions to tables with symbol columns not supported");
                    case StatusCode.PARTITION_EMPTY:
                        throw putPartitionName(
                                SqlException.$(tableNamePosition, "failed to attach partition '"),
                                tableWriter.getPartitionBy(),
                                partitionTimestamp)
                                .put("', data does not correspond to the partition folder or partition is empty");
                    case StatusCode.PARTITION_ALREADY_ATTACHED:
                        throw putPartitionName(
                                SqlException.$(tableNamePosition, "failed to attach partition '"),
                                tableWriter.getPartitionBy(),
                                partitionTimestamp)
                                .put("', partition already attached to the table");
                    default:
                        throw putPartitionName(
                                SqlException.$(tableNamePosition, "attach partition  '"),
                                tableWriter.getPartitionBy(),
                                partitionTimestamp)
                                .put(statusCode);
                }
            } catch (CairoException e) {
                LOG.error().$("failed to drop partition [table=").$(tableName)
                        .$(",ts=").$ts(partitionTimestamp)
                        .$(",errno=").$(e.getErrno())
                        .$(",error=").$(e.getFlyweightMessage())
                        .I$();

                throw e;
            }
        }
    }

    private SqlException putPartitionName(SqlException ex, int partitionBy, long timestamp) {
        TableUtils.setSinkForPartition(exceptionSinkAdapter.of(ex), partitionBy, timestamp, false);
        return ex;
    }

    private void applyDropColumn(TableWriter writer) throws SqlException {
        for(int i = 0, n = nameList.size(); i < n; i++) {
            CharSequence columnName = nameList.get(i);
            RecordMetadata metadata = writer.getMetadata();
            if (metadata.getColumnIndexQuiet(columnName) == -1) {
                throw SqlException.invalidColumn(tableNamePosition, columnName);
            }
            try {
                writer.removeColumn(columnName);
            } catch (CairoException e) {
                LOG.error().$("cannot drop column '").$(writer.getTableName()).$('.').$(columnName).$("'. Exception: ").$((Sinkable) e).$();
                throw SqlException.$(tableNamePosition, "cannot drop column. Try again later [errno=").put(e.getErrno()).put(']');
            }
        }
    }

    private void applyDropPartition(TableWriter tableWriter) throws SqlException {
        for (int i = 0, n = partitionList.size(); i < n; i++) {
            long partitionTimestamp = partitionList.getQuick(i);
            try {
                if (!tableWriter.removePartition(partitionTimestamp)) {
                    throw putPartitionName(SqlException.$(tableNamePosition, "could not remove partition '"),
                                    tableWriter.getPartitionBy(),
                                    partitionTimestamp).put('\'');
                }
            } catch (CairoException e) {
                LOG.error().$("failed to drop partition [table=").$(tableName)
                        .$(",ts=").$ts(partitionTimestamp)
                        .$(",errno=").$(e.getErrno())
                        .$(",error=").$(e.getFlyweightMessage())
                        .I$();

                throw SqlException.$(tableNamePosition, e.getFlyweightMessage());
            }
        }
    }

    private void applyParamCommitLag(TableWriter tableWriter) {
        long commitLag = partitionList.get(0);
        tableWriter.setMetaCommitLag(commitLag);
    }

    private void applyParamUncommittedRows(TableWriter tableWriter) {
        int maxUncommittedRows = (int) partitionList.get(0);
        tableWriter.setMetaMaxUncommittedRows(maxUncommittedRows);
    }

    private void applyRenameColumn(TableWriter writer) throws SqlException {
        // To not store 2 var len fields, store only new name as CharSequence
        // and index of existing column store as
        int i = 0, n = nameList.size();
        while (i < n) {
            CharSequence columnName = nameList.get(i++);
            CharSequence newName = nameList.get(i++);
            try {
                writer.renameColumn(columnName, newName);
            } catch (CairoException e) {
                LOG.error().$("cannot rename column '").$(writer.getTableName()).$('.').$(columnName).$("'. Exception: ").$((Sinkable) e).$();
                throw SqlException.$(tableNamePosition, "cannot rename column. Try again later [errno=").put(e.getErrno()).put(']');
            }
        }
    }

    private void applySetSymbolCache(TableWriter tableWriter, boolean isCacheOn) throws SqlException {
        CharSequence columnName = nameList.get(0);
        int columnIndex = tableWriter.getMetadata().getColumnIndexQuiet(columnName);
        if (columnIndex == -1) {
            throw SqlException.invalidColumn(tableNamePosition, columnName);
        }
        tableWriter.changeCacheFlag(columnIndex, isCacheOn);
    }

    private static class ExceptionSinkAdapter implements CharSink {
        private SqlException ex;

        ExceptionSinkAdapter of(SqlException ex) {
            this.ex = ex;
            return this;
        }

        @Override
        public char[] getDoubleDigitsBuffer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSink put(char c) {
            ex.put(c);
            return this;
        }

        @Override
        public CharSink put(long c) {
            ex.put(c);
            return this;
        }

        @Override
        public CharSink put(int c) {
            ex.put(c);
            return this;
        }
    }
}
