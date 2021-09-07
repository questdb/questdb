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

import io.questdb.cairo.sql.AlterStatement;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.Sinkable;
import io.questdb.std.str.CharSink;

public class AlterTableImpl implements AlterStatement {
    private final static Log LOG = LogFactory.getLog(AlterTableImpl.class);
    private final int indexValueBlockSize;
    private short command;
    private CharSequence tableName;
    private CharSequence columnName;
    private int intParam1;
    private int intParam2;
    private int intParam3;
    private int tableNamePosition;
    private LongList partitionList;
    private CharSequence newName;
    private ExceptionSinkAdapter exceptionSinkAdapter = new ExceptionSinkAdapter();

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
//        catch (Throwable th) {
//            throw SqlException.$(tableNamePosition, "table '")
//                    .put(tableName)
//                    .put("' could not be altered: ")
//                    .put(th.getMessage());
//        }
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

    public void setPartitionList(LongList partitionList) {
        this.partitionList = partitionList;
    }

    public void ofAddColumn(
            int tableNamePosition,
            CharSequence tableName,
            CharSequence columnName,
            int type,
            int symbolCapacity,
            boolean cache,
            boolean indexed,
            int indexValueBlockCapacity) {
        this.command = ADD_COLUMN;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        this.columnName = columnName;
        this.intParam1 = type;
        this.intParam2 = cache ? symbolCapacity : - symbolCapacity;
        this.intParam3 = indexed ? indexValueBlockCapacity : -indexValueBlockCapacity;
    }

    public AlterStatement ofAddIndex(int tableNamePosition, CharSequence tableName, CharSequence columnName) {
        this.command = ADD_INDEX;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        this.columnName = columnName;
        return this;
    }

    public AlterStatement ofAttachPartition(int tableNamePosition, CharSequence tableName, LongList partitionList) {
        this.command = ATTACH_PARTITION;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        this.partitionList = partitionList;
        return this;
    }

    public AlterStatement ofCacheSymbol(int tableNamePosition, CharSequence tableName, CharSequence columnName) {
        this.command = ADD_SYMBOL_CACHE;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        this.columnName = columnName;
        return this;
    }

    public AlterStatement ofDropColumn(int tableNamePosition, CharSequence tableName, CharSequence columnName) {
        this.command = DROP_COLUMN;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        this.columnName = columnName;
        return this;
    }

    public AlterStatement ofDropPartition(int tableNamePosition, CharSequence tableName, LongList partitionList) {
        this.command = DROP_PARTITION;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        this.partitionList = partitionList;
        return this;
    }

    public AlterStatement ofRemoveCacheSymbol(int tableNamePosition, CharSequence tableName, CharSequence columnName) {
        this.command = REMOVE_SYMBOL_CACHE;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        this.columnName = columnName;
        return this;
    }

    public AlterTableImpl ofRenameColumn(int tableNamePosition, CharSequence tableName, CharSequence oldName, CharSequence newName) {
        this.command = DROP_COLUMN;
        this.tableNamePosition = tableNamePosition;
        this.tableName = tableName;
        this.columnName = oldName;
        this.newName = newName;
        return this;
    }

    public AlterStatement ofSetParamCommitLag(CharSequence tableName, long commitLag) {
        this.command = SET_PARAM_COMMIT_LAG;
        this.tableName = tableName;
        this.intParam1 = Numbers.decodeHighInt(commitLag);
        this.intParam2 = Numbers.decodeLowInt(commitLag);
        return this;
    }

    public AlterStatement ofSetParamUncommittedRows(CharSequence tableName, int maxUncommittedRows) {
        this.command = SET_PARAM_MAX_UNCOMMITTED_ROWS;
        this.tableName = tableName;
        this.intParam1 = maxUncommittedRows;
        return this;
    }

    private void applyAddColumn(TableWriter tableWriter) throws SqlException {
        try {
            tableWriter.addColumn(
                    columnName,
                    intParam1,
                    Math.abs(intParam2),
                    intParam2 >= 0,
                    intParam3 >= 0,
                    Math.abs(intParam3),
                    false
            );
        } catch (CairoException e) {
            LOG.error().$("Cannot add column '").$(tableWriter.getTableName()).$('.').$(columnName).$("'. Exception: ").$((Sinkable) e).$();
            throw SqlException.$(tableNamePosition, "could add column [error=").put(e.getFlyweightMessage())
                    .put(", errno=").put(e.getErrno())
                    .put(']');
        }
    }

    private void applyAddIndex(TableWriter tableWriter) throws SqlException {
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

    private void applyDropPartition(TableWriter tableWriter) throws SqlException {
        for (int i = 0, n = partitionList.size(); i < n; i++) {
            long partitionTimestamp = partitionList.getQuick(i);
            try {
                tableWriter.removePartition(partitionTimestamp);
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
        int hi = Numbers.decodeLowInt(this.intParam1);
        int low = Numbers.decodeLowInt(this.intParam2);
        long commitLag = Numbers.encodeLowHighInts(low, hi);
        tableWriter.setMetaCommitLag(commitLag);
    }

    private void applyParamUncommittedRows(TableWriter tableWriter) {
        tableWriter.setMetaMaxUncommittedRows(this.intParam1);
    }

    private void applyRenameColumn(TableWriter writer) throws SqlException {
        // To not store 2 var len fields, store only new name as CharSequence
        // and index of existing column store as
        try {
            writer.renameColumn(columnName, newName);
        } catch (CairoException e) {
            LOG.error().$("cannot rename column '").$(writer.getTableName()).$('.').$(columnName).$("'. Exception: ").$((Sinkable) e).$();
            throw SqlException.$(tableNamePosition, "cannot rename column. Try again later [errno=").put(e.getErrno()).put(']');
        }
    }

    private void applySetSymbolCache(TableWriter tableWriter, boolean isCacheOn) throws SqlException {
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
