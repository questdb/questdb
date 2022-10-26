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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectCharSequence;
import io.questdb.tasks.TableWriterTask;

public class AlterOperation extends AbstractOperation implements Mutable {
    public final static String CMD_NAME = "ALTER TABLE";

    public final static short DO_NOTHING = 0;
    public final static short ADD_COLUMN = 1;
    public final static short DROP_PARTITION = 2;
    public final static short ATTACH_PARTITION = 3;
    public final static short ADD_INDEX = 4;
    public final static short DROP_INDEX = 5;
    public final static short ADD_SYMBOL_CACHE = 6;
    public final static short REMOVE_SYMBOL_CACHE = 7;
    public final static short DROP_COLUMN = 8;
    public final static short RENAME_COLUMN = 9;
    public final static short SET_PARAM_MAX_UNCOMMITTED_ROWS = 10;
    public final static short SET_PARAM_COMMIT_LAG = 11;
    public final static short DETACH_PARTITION = 12;

    private final static Log LOG = LogFactory.getLog(AlterOperation.class);

    private final ObjCharSequenceList objCharList;
    private final DirectCharSequenceList directCharList = new DirectCharSequenceList();
    private final LongList longList;
    // This is only used to serialize Partition name in form 2020-02-12 or 2020-02 or 2020
    // to exception message using TableUtils.setSinkForPartition
    private final ExceptionSinkAdapter exceptionSinkAdapter = new ExceptionSinkAdapter();
    private short command;
    private CharSequenceList charSequenceList;

    public AlterOperation() {
        this(new LongList(), new ObjList<>());
    }

    public AlterOperation(LongList longList, ObjList<CharSequence> charSequenceObjList) {
        this.longList = longList;
        this.objCharList = new ObjCharSequenceList(charSequenceObjList);
        this.command = DO_NOTHING;
    }

    @Override
    public long apply(TableWriter tableWriter, boolean contextAllowsAnyStructureChanges) throws SqlException, AlterTableContextException {
        try {
            switch (command) {
                case ADD_COLUMN:
                    applyAddColumn(tableWriter);
                    break;
                case DROP_COLUMN:
                    if (!contextAllowsAnyStructureChanges) {
                        throw AlterTableContextException.INSTANCE;
                    }
                    applyDropColumn(tableWriter);
                    break;
                case RENAME_COLUMN:
                    if (!contextAllowsAnyStructureChanges) {
                        throw AlterTableContextException.INSTANCE;
                    }
                    applyRenameColumn(tableWriter);
                    break;
                case DROP_PARTITION:
                    applyDropPartition(tableWriter);
                    break;
                case DETACH_PARTITION:
                    applyDetachPartition(tableWriter);
                    break;
                case ATTACH_PARTITION:
                    applyAttachPartition(tableWriter);
                    break;
                case ADD_INDEX:
                    applyAddIndex(tableWriter);
                    break;
                case DROP_INDEX:
                    applyDropIndex(tableWriter);
                    break;
                case ADD_SYMBOL_CACHE:
                    applySetSymbolCache(tableWriter, true);
                    break;
                case REMOVE_SYMBOL_CACHE:
                    applySetSymbolCache(tableWriter, false);
                    break;
                case SET_PARAM_MAX_UNCOMMITTED_ROWS:
                    applyParamUncommittedRows(tableWriter);
                    break;
                case SET_PARAM_COMMIT_LAG:
                    applyParamCommitLag(tableWriter);
                    break;
                default:
                    LOG.error().$("Invalid alter table command [code=").$(command).$(" ,table=").$(tableName).I$();
                    throw SqlException.$(tableNamePosition, "Invalid alter table command [code=").put(command).put(']');
            }
        } catch (EntryUnavailableException | SqlException ex) {
            throw ex;
        } catch (CairoException e2) {
            LOG.error().$("table '")
                    .$(tableName)
                    .$("' could not be altered [")
                    .$(e2.getErrno())
                    .$("] ")
                    .$(e2.getFlyweightMessage())
                    .$();

            throw SqlException.$(tableNamePosition, "table '")
                    .put(tableName)
                    .put("' could not be altered: [")
                    .put(e2.getErrno())
                    .put("] ")
                    .put(e2.getFlyweightMessage());
        }
        return 0;
    }

    @Override
    public AlterOperation deserialize(TableWriterTask event) {
        clear();

        tableName = event.getTableName();
        long readPtr = event.getData();
        final long hi = readPtr + event.getDataSize();

        // This is not hot path, do safe deserialization
        if (readPtr + 10 >= hi) {
            throw CairoException.critical(0).put("invalid alter statement serialized to writer queue [1]");
        }
        command = Unsafe.getUnsafe().getShort(readPtr);
        readPtr += 2;
        tableNamePosition = Unsafe.getUnsafe().getInt(readPtr);
        readPtr += 4;
        int longSize = Unsafe.getUnsafe().getInt(readPtr);
        readPtr += 4;
        if (longSize < 0 || readPtr + longSize * 8L >= hi) {
            throw CairoException.critical(0).put("invalid alter statement serialized to writer queue [2]");
        }
        for (int i = 0; i < longSize; i++) {
            longList.add(Unsafe.getUnsafe().getLong(readPtr));
            readPtr += 8;
        }
        directCharList.of(readPtr, hi);
        charSequenceList = directCharList;
        return this;
    }

    @Override
    public void startAsync() {
    }

    @Override
    public void clear() {
        command = DO_NOTHING;
        objCharList.clear();
        directCharList.clear();
        charSequenceList = objCharList;
        longList.clear();
        clearCommandCorrelationId();
    }

    public AlterOperation of(
            short command,
            String tableName,
            int tableId,
            int tableNamePosition
    ) {
        init(TableWriterTask.CMD_ALTER_TABLE, CMD_NAME, tableName, tableId, -1, tableNamePosition);
        this.command = command;
        return this;
    }

    @Override
    public void serialize(TableWriterTask event) {
        super.serialize(event);
        event.putShort(command);
        event.putInt(tableNamePosition);
        event.putInt(longList.size());
        for (int i = 0, n = longList.size(); i < n; i++) {
            event.putLong(longList.getQuick(i));
        }

        event.putInt(objCharList.size());
        for (int i = 0, n = objCharList.size(); i < n; i++) {
            event.putStr(objCharList.getStrA(i));
        }
    }

    private void applyAddColumn(TableWriter tableWriter) throws SqlException {
        int lParam = 0;
        for (int i = 0, n = charSequenceList.size(); i < n; i++) {
            CharSequence columnName = charSequenceList.getStrA(i);
            int type = (int) longList.get(lParam++);
            int symbolCapacity = (int) longList.get(lParam++);
            boolean symbolCacheFlag = longList.get(lParam++) > 0;
            boolean isIndexed = longList.get(lParam++) > 0;
            int indexValueBlockCapacity = (int) longList.get(lParam++);
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
                throw SqlException.$(tableNamePosition, "could not add column [error=").put(e.getFlyweightMessage())
                        .put(", errno=").put(e.getErrno())
                        .put(']');
            }
        }
    }

    private void applyAddIndex(TableWriter tableWriter) throws SqlException {
        CharSequence columnName = charSequenceList.getStrA(0);
        try {
            int indexValueBlockSize = (int) longList.get(0);
            tableWriter.addIndex(columnName, indexValueBlockSize);
        } catch (CairoException e) {
            throw SqlException.position(tableNamePosition).put(e.getFlyweightMessage())
                    .put("[errno=").put(e.getErrno()).put(']');
        }
    }

    private void applyDropIndex(TableWriter tableWriter) throws SqlException {
        CharSequence columnName = charSequenceList.getStrA(0);
        try {
            tableWriter.dropIndex(columnName);
        } catch (CairoException e) {
            throw SqlException.position(tableNamePosition)
                    .put(e.getFlyweightMessage())
                    .put("[errno=")
                    .put(e.getErrno())
                    .put(']');
        }
    }

    private void applyAttachPartition(TableWriter tableWriter) throws SqlException {
        for (int i = 0, n = longList.size(); i < n; i++) {
            long partitionTimestamp = longList.getQuick(i);
            try {
                AttachDetachStatus attachDetachStatus = tableWriter.attachPartition(partitionTimestamp);
                if (attachDetachStatus != AttachDetachStatus.OK) {
                    throw putPartitionName(
                            SqlException.$(tableNamePosition, "failed to attach partition '"),
                            tableWriter.getPartitionBy(),
                            partitionTimestamp
                    ).put("': ").put(attachDetachStatus.name());
                }
            } catch (CairoException e) {
                LOG.error().$("failed to attach partition [table=").$(tableName)
                        .$(", ts=").$ts(partitionTimestamp)
                        .$(", errno=").$(e.getErrno())
                        .$(", error=").$(e.getFlyweightMessage())
                        .I$();
                throw e;
            }
        }
    }

    private void applyDetachPartition(TableWriter tableWriter) throws SqlException {
        for (int i = 0, n = longList.size(); i < n; i++) {
            long partitionTimestamp = longList.getQuick(i);
            try {
                AttachDetachStatus attachDetachStatus = tableWriter.detachPartition(partitionTimestamp);
                if (AttachDetachStatus.OK != attachDetachStatus) {
                    throw putPartitionName(
                            SqlException.$(tableNamePosition, "could not detach [statusCode=").put(attachDetachStatus.name())
                                    .put(", table=").put(tableName)
                                    .put(", partition='"),
                            tableWriter.getPartitionBy(),
                            partitionTimestamp
                    ).put("']");
                }
            } catch (CairoException e) {
                LOG.error().$("failed to detach partition [table=").$(tableName)
                        .$(", ts=").$ts(partitionTimestamp)
                        .$(", errno=").$(e.getErrno())
                        .$(", error=").$(e.getFlyweightMessage())
                        .I$();
                throw e;
            }
        }
    }

    private void applyDropColumn(TableWriter writer) throws SqlException {
        for (int i = 0, n = charSequenceList.size(); i < n; i++) {
            CharSequence columnName = charSequenceList.getStrA(i);
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
        for (int i = 0, n = longList.size(); i < n; i++) {
            long partitionTimestamp = longList.getQuick(i);
            try {
                if (!tableWriter.removePartition(partitionTimestamp)) {
                    throw putPartitionName(SqlException.$(tableNamePosition, "could not remove partition '"),
                            tableWriter.getPartitionBy(),
                            partitionTimestamp).put('\'');
                }
            } catch (CairoException e) {
                LOG.error().$("failed to drop partition [table=").$(tableName)
                        .$(", ts=").$ts(partitionTimestamp)
                        .$(", errno=").$(e.getErrno())
                        .$(", error=").$(e.getFlyweightMessage())
                        .I$();

                throw putPartitionName(SqlException.$(tableNamePosition, "could not remove partition '"),
                        tableWriter.getPartitionBy(),
                        partitionTimestamp).put("'. ")
                        .put(e.getFlyweightMessage());
            }
        }
    }

    private void applyParamCommitLag(TableWriter tableWriter) {
        long commitLag = longList.get(0);
        tableWriter.setMetaCommitLag(commitLag);
    }

    private void applyParamUncommittedRows(TableWriter tableWriter) {
        int maxUncommittedRows = (int) longList.get(0);
        tableWriter.setMetaMaxUncommittedRows(maxUncommittedRows);
    }

    private void applyRenameColumn(TableWriter writer) throws SqlException {
        // To not store 2 var len fields, store only new name as CharSequence
        // and index of existing column store as
        int i = 0, n = charSequenceList.size();
        while (i < n) {
            CharSequence columnName = charSequenceList.getStrA(i++);
            CharSequence newName = charSequenceList.getStrB(i++);
            try {
                writer.renameColumn(columnName, newName);
            } catch (CairoException e) {
                LOG.error().$("cannot rename column '").$(writer.getTableName()).$('.').$(columnName).$("'. Exception: ").$((Sinkable) e).$();
                throw SqlException.$(tableNamePosition, "cannot rename column \"").put(columnName).put("\"; ").put(e.getFlyweightMessage());
            }
        }
    }

    private void applySetSymbolCache(TableWriter tableWriter, boolean isCacheOn) throws SqlException {
        CharSequence columnName = charSequenceList.getStrA(0);
        int columnIndex = tableWriter.getMetadata().getColumnIndexQuiet(columnName);
        if (columnIndex == -1) {
            throw SqlException.invalidColumn(tableNamePosition, columnName);
        }
        tableWriter.changeCacheFlag(columnIndex, isCacheOn);
    }

    private SqlException putPartitionName(SqlException ex, int partitionBy, long timestamp) {
        PartitionBy.setSinkForPartition(exceptionSinkAdapter.of(ex), partitionBy, timestamp, false);
        return ex;
    }

    interface CharSequenceList extends Mutable {
        CharSequence getStrA(int i);

        CharSequence getStrB(int i);

        int size();
    }

    // This is only used to serialize Partition name in form 2020-02-12 or 2020-02 or 2020
    // to exception message using TableUtils.setSinkForPartition
    private static class ExceptionSinkAdapter implements CharSink {
        private SqlException ex;

        @Override
        public int encodeSurrogate(char c, CharSequence in, int pos, int hi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public char[] getDoubleDigitsBuffer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSink put(CharSequence cs) {
            ex.put(cs);
            return this;
        }

        @Override
        public CharSink put(char c) {
            ex.put(c);
            return this;
        }

        @Override
        public CharSink put(int c) {
            ex.put(c);
            return this;
        }

        @Override
        public CharSink put(long c) {
            ex.put(c);
            return this;
        }

        ExceptionSinkAdapter of(SqlException ex) {
            this.ex = ex;
            return this;
        }
    }

    private static class ObjCharSequenceList implements CharSequenceList {
        private final ObjList<CharSequence> strings;

        public ObjCharSequenceList(ObjList<CharSequence> strings) {
            this.strings = strings;
        }

        @Override
        public void clear() {
            strings.clear();
        }

        public CharSequence getStrA(int i) {
            return strings.get(i);
        }

        @Override
        public CharSequence getStrB(int i) {
            return strings.get(i);
        }

        @Override
        public int size() {
            return strings.size();
        }
    }

    private static class DirectCharSequenceList implements CharSequenceList {
        private final LongList offsets = new LongList();
        private final DirectCharSequence strA = new DirectCharSequence();
        private final DirectCharSequence strB = new DirectCharSequence();

        @Override
        public void clear() {
            offsets.clear();
        }

        public CharSequence getStrA(int i) {
            long lo = offsets.get(i * 2);
            long hi = offsets.get(i * 2 + 1);
            strA.of(lo, hi);
            return strA;
        }

        @Override
        public CharSequence getStrB(int i) {
            long lo = offsets.get(i * 2);
            long hi = offsets.get(i * 2 + 1);
            strB.of(lo, hi);
            return strB;
        }

        @Override
        public int size() {
            return offsets.size() / 2;
        }

        public long of(long lo, long hi) {
            long initialAddress = lo;
            if (lo + Integer.BYTES >= hi) {
                throw CairoException.critical(0).put("invalid alter statement serialized to writer queue [11]");
            }
            int size = Unsafe.getUnsafe().getInt(lo);
            lo += 4;
            for (int i = 0; i < size; i++) {
                if (lo + Integer.BYTES >= hi) {
                    throw CairoException.critical(0).put("invalid alter statement serialized to writer queue [12]");
                }
                int stringSize = 2 * Unsafe.getUnsafe().getInt(lo);
                lo += 4;
                if (lo + stringSize >= hi) {
                    throw CairoException.critical(0).put("invalid alter statement serialized to writer queue [13]");
                }
                offsets.add(lo, lo + stringSize);
                lo += stringSize;
            }
            return lo - initialAddress;
        }
    }
}
