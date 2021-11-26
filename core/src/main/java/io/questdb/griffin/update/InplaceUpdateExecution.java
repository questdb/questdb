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

package io.questdb.griffin.update;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;
import java.io.IOException;

import static io.questdb.cairo.TableUtils.dFile;

public class InplaceUpdateExecution implements Closeable {
    private static final Log LOG = LogFactory.getLog(InplaceUpdateExecution.class);
    private final FilesFacade ff;
    private final int rootLen;
    private final IntList updateToColumnMap = new IntList();
    private final ObjList<MemoryCMARW> updateMemory = new ObjList<>();
    private final long dataAppendPageSize;
    private Path path;

    public InplaceUpdateExecution(CairoConfiguration configuration) {
        ff = configuration.getFilesFacade();
        path = new Path().of(configuration.getRoot());
        rootLen = path.length();
        dataAppendPageSize = configuration.getDataAppendPageSize();
    }

    @Override
    public void close() {
        path = Misc.free(path);
    }

    // This is very hacky way to update in place for testing only
    // TODO: rewrite to production grade
    public void executeUpdate(TableWriter tableWriter, UpdateStatement updateStatement, SqlExecutionContext executionContext) throws SqlException {

        if (tableWriter.inTransaction()) {
            LOG.info().$("committing current transaction before UPDATE execution [table=").$(tableWriter.getTableName()).I$();
            tableWriter.commit();
        }

        TableWriterMetadata writerMetadata = tableWriter.getMetadata();
        RecordMetadata updateMetadata = updateStatement.getValuesMetadata();
        int updateStatementColumnCount = updateMetadata.getColumnCount();

        // Build index column map from table to update to values returned in the update statement row cursors
        updateToColumnMap.clear();
        for (int i = 0; i < updateStatementColumnCount; i++) {
            CharSequence columnName = updateMetadata.getColumnName(i);
            int tableColumnIndex = writerMetadata.getColumnIndex(columnName);
            assert tableColumnIndex >= 0;
            updateToColumnMap.add(tableColumnIndex);
        }

        // Create update memory list of all columns to be udpated
        initUpdateMemory(updateStatementColumnCount);

        // Start execution frame by frame
        RecordCursorFactory rowIdFactory = updateStatement.getRowIdFactory();
        if (!rowIdFactory.supportPageFrameCursor()) {
            throw SqlException.$(updateStatement.getPosition(), "Only simple UPDATE statements without joins are supported");
        }

        // Track how many records updated
        long rowsUpdated = 0;

        UpdateStatementMasterCursor joinCursor = null;
        // Start row by row updates
        try (RecordCursor recordCursor = rowIdFactory.getCursor(executionContext)) {
            updateStatement.init(recordCursor, executionContext);

            // If this update has a join (FROM clause), execute the join
            if (updateStatement.getJoinRecordCursorFactory() != null) {
                joinCursor = updateStatement.getJoinRecordCursorFactory().getCursor(executionContext);
            }
            Record masterRecord = recordCursor.getRecord();
            long currentPartitionIndex = -1L;
            Function filter = updateStatement.getRowIdFilter();
            Function postJoinFilter = updateStatement.getPostJoinFilter();

            while (recordCursor.hasNext()) {
                if (filter != null && !filter.getBool(masterRecord)) {
                    continue;
                }

                Record record = masterRecord;
                if (joinCursor != null) {
                    joinCursor.setMaster(record);
                    record = joinCursor.getRecord();
                    boolean found = false;

                    // Scroll through joint records
                    while(joinCursor.hasNext()) {
                        if (postJoinFilter == null || postJoinFilter.getBool(record)) {
                            // Found joint record which satisfies post join filter
                            found = true;
                            break;
                        }
                    }

                    if (!found) {
                        continue;
                    }

                } else if (postJoinFilter != null && !postJoinFilter.getBool(record)) {
                    continue;
                }

                long rowId = masterRecord.getRowId();
                int partitionIndex = Rows.toPartitionIndex(rowId);
                long partitionRowId = Rows.toLocalRowID(rowId);
                if (partitionIndex != currentPartitionIndex) {
                    // Map columns to be update for RW
                    openPartitionColumnsForUpdate(tableWriter, updateMemory, partitionIndex, updateToColumnMap);
                    currentPartitionIndex = partitionIndex;
                }

                // Update values inplace
                updateColumnValues(
                        writerMetadata,
                        updateToColumnMap,
                        updateStatementColumnCount,
                        updateMemory,
                        partitionRowId,
                        updateStatement.getColumnMapper(),
                        record);
                rowsUpdated++;
            }
        } finally {
            for(int i = 0; i < updateMemory.size(); i++) {
                updateMemory.getQuick(i).close(false);
            }
            if (joinCursor != null) {
                joinCursor.close();
            }
        }
        if (rowsUpdated > 0) {
            tableWriter.commit();
        }
    }

    private void updateColumnValues(
            RecordMetadata metadata,
            IntList updateToColumnMap,
            int columnCount,
            ObjList<MemoryCMARW> updateMemory,
            long rowId,
            RecordColumnMapper columnMapper,
            Record record
    ) throws SqlException {
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            int columnType = metadata.getColumnType(updateToColumnMap.get(columnIndex));
            MemoryCMARW primaryColMem = updateMemory.get(columnIndex);

            switch (columnType) {
                case ColumnType.INT:
                    primaryColMem.putInt(rowId << 2, columnMapper.getInt(record, columnIndex));
                    break;
                case ColumnType.FLOAT:
                    primaryColMem.putFloat(rowId << 2, columnMapper.getFloat(record, columnIndex));
                    break;
                case ColumnType.LONG:
                    primaryColMem.putLong(rowId << 3, columnMapper.getLong(record, columnIndex));
                    break;
                case ColumnType.TIMESTAMP:
                    primaryColMem.putLong(rowId << 3, columnMapper.getTimestamp(record, columnIndex));
                    break;
                case ColumnType.DATE:
                    primaryColMem.putLong(rowId << 3, columnMapper.getDate(record, columnIndex));
                    break;
                case ColumnType.DOUBLE:
                    primaryColMem.putDouble(rowId << 3, columnMapper.getDouble(record, columnIndex));
                    break;
                case ColumnType.SHORT:
                    primaryColMem.putShort(rowId << 1, columnMapper.getShort(record,columnIndex));
                    break;
                case ColumnType.CHAR:
                    primaryColMem.putChar(rowId << 1, columnMapper.getChar(record, columnIndex));
                    break;
                case ColumnType.BYTE:
                case ColumnType.BOOLEAN:
                    primaryColMem.putLong(rowId, columnMapper.getByte(record, columnIndex));
                    break;
                default:
                    throw SqlException.$(0, "Column type ")
                            .put(ColumnType.nameOf(columnType))
                            .put(" not supported for updates");
            }
        }
    }

    private void initUpdateMemory(int columnCount) {
        for(int i = updateMemory.size(); i < columnCount; i++) {
            updateMemory.add(Vm.getCMARWInstance());
        }
    }

    private void openPartitionColumnsForUpdate(TableWriter tableWriter, ObjList<MemoryCMARW> updateMemory, int partitionIndex, IntList columnMap) {
        long partitionTimestamp = tableWriter.getPartitionTimestamp(partitionIndex);
        RecordMetadata metadata = tableWriter.getMetadata();
        try {
            path.concat(tableWriter.getTableName());
            TableUtils.setPathForPartition(path, tableWriter.getPartitionBy(), partitionTimestamp, false);
            int pathTrimToLen = path.length();

            for (int i = 0, n = columnMap.size(); i < n; i++) {
                CharSequence name = metadata.getColumnName(columnMap.get(i));
                MemoryCMARW colMem = updateMemory.get(i);
                colMem.of(ff, dFile(path.trimTo(pathTrimToLen), name), dataAppendPageSize, -1, MemoryTag.MMAP_TABLE_WRITER);
            }
        } finally {
            path.trimTo(rootLen);
        }
    }
}
