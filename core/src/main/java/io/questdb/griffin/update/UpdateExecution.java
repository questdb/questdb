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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCM;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.dFile;
import static io.questdb.cairo.TableUtils.iFile;

public class UpdateExecution implements Closeable {
    private static final Log LOG = LogFactory.getLog(UpdateExecution.class);
    private final FilesFacade ff;
    private final int rootLen;
    private final IntList updateColumnIndexes = new IntList();
    private final ObjList<MemoryCMR> baseMemory = new ObjList<>();
    private final ObjList<MemoryCMARW> updateMemory = new ObjList<>();
    private final long dataAppendPageSize;
    private final long fileOpenOpts;
    private Path path;

    public UpdateExecution(CairoConfiguration configuration) {
        ff = configuration.getFilesFacade();
        path = new Path().of(configuration.getRoot());
        rootLen = path.length();
        dataAppendPageSize = configuration.getDataAppendPageSize();
        fileOpenOpts = configuration.getWriterFileOpenOpts();
    }

    @Override
    public void close() {
        path = Misc.free(path);
    }

    public void executeUpdate(TableWriter tableWriter, UpdateStatement updateStatement, SqlExecutionContext executionContext) throws SqlException {
        if (tableWriter.inTransaction()) {
            LOG.info().$("committing current transaction before UPDATE execution [table=").$(tableWriter.getTableName()).I$();
            tableWriter.commit();
        }

        TableWriterMetadata writerMetadata = tableWriter.getMetadata();

        // Check that table structure hasn't changed between planning and executing the UPDATE
        if (writerMetadata.getId() != updateStatement.getTableId() || tableWriter.getStructureVersion() != updateStatement.getTableVersion()) {
            throw ReaderOutOfDateException.of(tableWriter.getTableName());
        }

        RecordMetadata updateMetadata = updateStatement.getUpdateToDataCursorFactory().getMetadata();
        int updateColumnCount = updateMetadata.getColumnCount();

        // Build index column map from table to update to values returned from the update statement row cursors
        updateColumnIndexes.clear();
        for (int i = 0; i < updateColumnCount; i++) {
            CharSequence columnName = updateMetadata.getColumnName(i);
            int tableColumnIndex = writerMetadata.getColumnIndex(columnName);
            assert tableColumnIndex >= 0;
            updateColumnIndexes.add(tableColumnIndex);
        }

        // Create update memory list of all columns to be updated
        initUpdateMemory(writerMetadata, updateColumnCount);

        // Start execution frame by frame
        RecordCursorFactory updateStatementDataCursorFactory = updateStatement.getUpdateToDataCursorFactory();

        // Partition to update
        long currentPartitionIndex = -1L;

        // Row by row updates
        // This should happen parallel per file (partition and column)
        try (RecordCursor recordCursor = updateStatementDataCursorFactory.getCursor(executionContext)) {
            Record masterRecord = recordCursor.getRecord();

            long startPartitionRowId = 0;
            long lastRowId = -1;
            while (recordCursor.hasNext()) {
                long rowId = masterRecord.getUpdateRowId();

                // Some joins expand results set and returns same row multiple times
                if (rowId == lastRowId) {
                    continue;
                }
                lastRowId = rowId;

                int partitionIndex = Rows.toPartitionIndex(rowId);
                if (partitionIndex != currentPartitionIndex) {
                    if (currentPartitionIndex > -1) {
                        long partitionSize = tableWriter.getPartitionSize((int) currentPartitionIndex);
                        copyColumnValues(
                                writerMetadata,
                                updateColumnIndexes,
                                updateColumnCount,
                                baseMemory,
                                updateMemory,
                                startPartitionRowId,
                                partitionSize);
                    }

                    openPartitionColumnsForRead(tableWriter, baseMemory, partitionIndex, updateColumnIndexes);
                    openPartitionColumnsForWrite(tableWriter, updateMemory, partitionIndex, updateColumnIndexes);
                    currentPartitionIndex = partitionIndex;
                    startPartitionRowId = 0;
                }

                long partitionRowId = Rows.toLocalRowID(rowId);
                updateColumnValues(
                        writerMetadata,
                        updateColumnIndexes,
                        updateColumnCount,
                        baseMemory,
                        updateMemory,
                        startPartitionRowId,
                        partitionRowId,
                        masterRecord);

                startPartitionRowId = ++partitionRowId;
            }

            if (currentPartitionIndex > -1) {
                long partitionSize = tableWriter.getPartitionSize((int) currentPartitionIndex);
                copyColumnValues(
                        writerMetadata,
                        updateColumnIndexes,
                        updateColumnCount,
                        baseMemory,
                        updateMemory,
                        startPartitionRowId,
                        partitionSize);
            }
        } finally {
            Misc.freeObjList(baseMemory);
            Misc.freeObjList(updateMemory);
            baseMemory.clear();
            updateMemory.clear();
        }
        if (currentPartitionIndex > -1) {
            tableWriter.commit();
            tableWriter.openLastPartition();
        }
    }

    private void copyColumnValues(
            TableWriterMetadata metadata,
            IntList updateColumnIndexes,
            int columnCount,
            ObjList<MemoryCMR> baseMemory,
            ObjList<MemoryCMARW> updateMemory,
            long startPartitionRowId,
            long partitionSize
    ) {
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            MemoryCMR basePrimaryColumnFile = baseMemory.get(2 * columnIndex);
            MemoryCMARW updatedFixedColumnFile = updateMemory.get(2 * columnIndex);
            MemoryCMR baseVariableColumnFile = baseMemory.get(2 * columnIndex + 1);
            MemoryCMARW updatedVariableColumnFile = updateMemory.get(2 * columnIndex + 1);

            int columnType = metadata.getColumnType(updateColumnIndexes.get(columnIndex));
            int typeSize = getFixedColumnSize(columnType);
            copyRecords(startPartitionRowId, partitionSize, basePrimaryColumnFile, updatedFixedColumnFile, baseVariableColumnFile, updatedVariableColumnFile, columnType, typeSize);
        }
    }

    private void initUpdateMemory(RecordMetadata metadata, int columnCount) {
        for (int i = updateMemory.size(); i < columnCount; i++) {
            int columnType = metadata.getColumnType(updateColumnIndexes.get(i));
            switch (columnType) {
                case ColumnType.STRING:
                case ColumnType.BINARY:
                    // Primary and secondary
                    baseMemory.add(Vm.getCMRInstance());
                    baseMemory.add(Vm.getCMRInstance());
                    updateMemory.add(Vm.getCMARWInstance());
                    updateMemory.add(Vm.getCMARWInstance());
                    break;
                default:
                    baseMemory.add(Vm.getCMRInstance());
                    baseMemory.add(null);
                    updateMemory.add(Vm.getCMARWInstance());
                    updateMemory.add(null);
                    break;

            }
        }
    }

    private void copyRecords(
            long fromRowId,
            long toRowId,
            MemoryCMR baseFixedColumnFile,
            MemoryCMARW updatedFixedColumnFile,
            MemoryCMR baseVariableColumnFile,
            MemoryCMARW updatedVariableColumnFile,
            int columnType,
            int typeSize) {

        long addr = baseFixedColumnFile.addressOf(fromRowId * typeSize);
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.STRING:
            case ColumnType.BINARY:
                long varStartOffset = baseFixedColumnFile.getLong(fromRowId * Long.BYTES);
                long varEndOffset = baseFixedColumnFile.getLong((toRowId + 1) * Long.BYTES);
                long varAddr = baseVariableColumnFile.addressOf(varStartOffset);
                long copyToOffset = updatedVariableColumnFile.getAppendOffset();
                updatedVariableColumnFile.putBlockOfBytes(varAddr, varEndOffset - varStartOffset);
                updatedFixedColumnFile.extend(updatedFixedColumnFile.getAppendOffset() + (toRowId - fromRowId) * typeSize);
                Vect.shiftCopyFixedSizeColumnData(copyToOffset - varStartOffset, addr + Long.BYTES, fromRowId, toRowId - 1, updatedFixedColumnFile.getAppendAddress());
                updatedFixedColumnFile.jumpTo(updatedFixedColumnFile.getAppendOffset() + (toRowId - fromRowId) * typeSize);
                break;
            default:
                updatedFixedColumnFile.putBlockOfBytes(addr, (toRowId - fromRowId) * typeSize);
                break;
        }
    }

    private void openPartitionColumnsForRead(TableWriter tableWriter, ObjList<? extends MemoryCM> baseMemory, int partitionIndex, IntList updateColumnIndexes) {
        openPartitionColumns(tableWriter, baseMemory, partitionIndex, updateColumnIndexes, false);
    }

    private void openPartitionColumnsForWrite(TableWriter tableWriter, ObjList<MemoryCMARW> updateMemory, int partitionIndex, IntList updateColumnIndexes) {
        openPartitionColumns(tableWriter, updateMemory, partitionIndex, updateColumnIndexes, true);
    }

    private int getFixedColumnSize(int columnType) {
        int typeSize = ColumnType.sizeOf(columnType);
        if (columnType == ColumnType.STRING || columnType == ColumnType.BINARY) {
            typeSize = Long.BYTES;
        }
        return typeSize;
    }

    private void openPartitionColumns(TableWriter tableWriter, ObjList<? extends MemoryCM> memList, int partitionIndex, IntList updateColumnIndexes, boolean forWrite) {
        long partitionTimestamp = tableWriter.getPartitionTimestamp(partitionIndex);
        RecordMetadata metadata = tableWriter.getMetadata();
        try {
            path.concat(tableWriter.getTableName());
            TableUtils.setPathForPartition(path, tableWriter.getPartitionBy(), partitionTimestamp, false);
            int pathTrimToLen = path.length();

            for (int i = 0, n = updateColumnIndexes.size(); i < n; i++) {
                int columnIndex = updateColumnIndexes.get(i);
                CharSequence name = metadata.getColumnName(columnIndex);
                int columnType = metadata.getColumnType(columnIndex);

                if (forWrite) {
                    tableWriter.upsertColumnVersion(partitionTimestamp, columnIndex);
                }

                long columnNameTxn = tableWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
                if (columnType == ColumnType.STRING) {
                    MemoryCMR colMemIndex = (MemoryCMR) memList.get(2 * i);
                    colMemIndex.close();
                    colMemIndex.of(
                            ff,
                            iFile(path.trimTo(pathTrimToLen), name, columnNameTxn),
                            dataAppendPageSize,
                            -1,
                            MemoryTag.MMAP_UPDATE,
                            fileOpenOpts
                    );
                    MemoryCMR colMemVar = (MemoryCMR) memList.get(2 * i + 1);
                    colMemVar.close();
                    colMemVar.of(
                            ff,
                            dFile(path.trimTo(pathTrimToLen), name, columnNameTxn),
                            dataAppendPageSize,
                            -1,
                            MemoryTag.MMAP_UPDATE,
                            fileOpenOpts
                    );
                } else {
                    MemoryCMR colMem = (MemoryCMR) memList.get(2 * i);
                    colMem.close();
                    colMem.of(
                            ff,
                            dFile(path.trimTo(pathTrimToLen), name, columnNameTxn),
                            dataAppendPageSize,
                            -1,
                            MemoryTag.MMAP_UPDATE,
                            fileOpenOpts
                    );
                }
                if (forWrite) {
                    if (columnType == ColumnType.STRING) {
                        ((MemoryCMARW) memList.get(2 * i)).putLong(0);
                    }
                }
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void updateColumnValues(
            RecordMetadata metadata,
            IntList updateColumnIndexes,
            int columnCount,
            ObjList<MemoryCMR> baseMemory,
            ObjList<MemoryCMARW> updateMemory,
            long startPartitionRowId,
            long partitionRowId,
            Record masterRecord
    ) throws SqlException {
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            MemoryCMR basePrimaryColumnFile = baseMemory.get(2 * columnIndex);
            MemoryCMARW updatedFixedColumnFile = updateMemory.get(2 * columnIndex);
            MemoryCMR baseVariableColumnFile = baseMemory.get(2 * columnIndex + 1);
            MemoryCMARW updatedVariableColumnFile = updateMemory.get(2 * columnIndex + 1);

            int columnType = metadata.getColumnType(updateColumnIndexes.get(columnIndex));
            int typeSize = getFixedColumnSize(columnType);
            if (partitionRowId > startPartitionRowId) {
                copyRecords(startPartitionRowId, partitionRowId, basePrimaryColumnFile, updatedFixedColumnFile, baseVariableColumnFile, updatedVariableColumnFile, columnType, typeSize);
            }
            switch (ColumnType.tagOf(columnType)) {
                case ColumnType.INT:
                    updatedFixedColumnFile.putInt(masterRecord.getInt(columnIndex));
                    break;
                case ColumnType.FLOAT:
                    updatedFixedColumnFile.putFloat(masterRecord.getFloat(columnIndex));
                    break;
                case ColumnType.LONG:
                    updatedFixedColumnFile.putLong(masterRecord.getLong(columnIndex));
                    break;
                case ColumnType.TIMESTAMP:
                    updatedFixedColumnFile.putLong(masterRecord.getTimestamp(columnIndex));
                    break;
                case ColumnType.DATE:
                    updatedFixedColumnFile.putLong(masterRecord.getDate(columnIndex));
                    break;
                case ColumnType.DOUBLE:
                    updatedFixedColumnFile.putDouble(masterRecord.getDouble(columnIndex));
                    break;
                case ColumnType.SHORT:
                    updatedFixedColumnFile.putShort(masterRecord.getShort(columnIndex));
                    break;
                case ColumnType.CHAR:
                    updatedFixedColumnFile.putChar(masterRecord.getChar(columnIndex));
                    break;
                case ColumnType.BYTE:
                case ColumnType.BOOLEAN:
                    updatedFixedColumnFile.putByte(masterRecord.getByte(columnIndex));
                    break;
                case ColumnType.GEOBYTE:
                    updatedFixedColumnFile.putByte(masterRecord.getGeoByte(columnIndex));
                    break;
                case ColumnType.GEOSHORT:
                    updatedFixedColumnFile.putShort(masterRecord.getGeoShort(columnIndex));
                    break;
                case ColumnType.GEOINT:
                    updatedFixedColumnFile.putInt(masterRecord.getGeoInt(columnIndex));
                    break;
                case ColumnType.GEOLONG:
                    updatedFixedColumnFile.putLong(masterRecord.getGeoLong(columnIndex));
                    break;
                case ColumnType.STRING:
                    CharSequence value = masterRecord.getStr(columnIndex);
                    long offset = updatedVariableColumnFile.putStr(value);
                    updatedFixedColumnFile.putLong(offset);
                    break;
                default:
                    throw SqlException.$(0, "Column type ")
                            .put(ColumnType.nameOf(columnType))
                            .put(" not supported for updates");
            }
        }
    }
}
