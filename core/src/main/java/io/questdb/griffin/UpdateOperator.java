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

package io.questdb.griffin;

import io.questdb.MessageBus;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCM;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Sequence;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.tasks.ColumnVersionPurgeTask;

import java.io.Closeable;

import static io.questdb.cairo.ColumnType.isVariableLength;
import static io.questdb.cairo.TableUtils.dFile;
import static io.questdb.cairo.TableUtils.iFile;

public class UpdateOperator implements Closeable {
    private static final Log LOG = LogFactory.getLog(UpdateOperator.class);
    private final FilesFacade ff;
    private final int rootLen;
    private final IntList updateColumnIndexes = new IntList();
    private final ObjList<MemoryCMR> baseMemory = new ObjList<>();
    private final ObjList<MemoryCMARW> updateMemory = new ObjList<>();
    private final long dataAppendPageSize;
    private final long fileOpenOpts;
    private final StringSink charSink = new StringSink();
    private final LongList cleanupColumnVersions = new LongList();
    private final LongList cleanupColumnVersionsAsync = new LongList();
    private final MessageBus messageBus;
    private final CairoConfiguration configuration;
    private final TableWriter tableWriter;
    private IndexBuilder indexBuilder;
    private Path path;

    public UpdateOperator(CairoConfiguration configuration, MessageBus messageBus, TableWriter tableWriter) {
        this.messageBus = messageBus;
        this.configuration = configuration;
        this.indexBuilder = new IndexBuilder();
        this.ff = configuration.getFilesFacade();
        this.path = new Path().of(configuration.getRoot());
        this.rootLen = path.length();
        this.dataAppendPageSize = configuration.getDataAppendPageSize();
        this.fileOpenOpts = configuration.getWriterFileOpenOpts();
        this.tableWriter = tableWriter;
    }

    @Override
    public void close() {
        path = Misc.free(path);
        indexBuilder = Misc.free(indexBuilder);
    }

    public long executeUpdate(SqlExecutionContext sqlExecutionContext, UpdateOperation op) throws SqlException, ReaderOutOfDateException {

        LOG.debug().$("executing UPDATE [table=").$(tableWriter.getTableName()).I$();

        final int tableId = op.getTableId();
        final long tableVersion = op.getTableVersion();
        final RecordCursorFactory factory = op.getFactory();

        cleanupColumnVersions.clear();

        final String tableName = tableWriter.getTableName();
        if (tableWriter.inTransaction()) {
            LOG.info().$("committing current transaction before UPDATE execution [table=").$(tableName).I$();
            tableWriter.commit();
        }

        TableWriterMetadata writerMetadata = tableWriter.getMetadata();

        // Check that table structure hasn't changed between planning and executing the UPDATE
        if (writerMetadata.getId() != tableId || tableWriter.getStructureVersion() != tableVersion) {
            throw ReaderOutOfDateException.of(tableName);
        }

        // Select the rows to be updated
        final RecordMetadata updateMetadata = factory.getMetadata();
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
        // Partition to update
        int currentPartitionIndex = -1;
        long rowsUpdated = 0;

        // Row by row updates for now
        // This should happen parallel per file (partition and column)
        try (RecordCursor recordCursor = factory.getCursor(sqlExecutionContext)) {
            Record masterRecord = recordCursor.getRecord();

            long startPartitionRowId = 0;
            long firstUpdatedPartitionRowId = -1L;
            long lastRowId = Long.MAX_VALUE;
            while (recordCursor.hasNext()) {
                long rowId = masterRecord.getUpdateRowId();

                // Some joins expand results set and returns same row multiple times
                if (rowId == lastRowId) {
                    continue;
                }
                lastRowId = rowId;

                int partitionIndex = Rows.toPartitionIndex(rowId);
                long partitionRowId = Rows.toLocalRowID(rowId);
                if (partitionIndex != currentPartitionIndex) {
                    if (currentPartitionIndex > -1) {
                        copyColumnValues(
                                tableWriter,
                                currentPartitionIndex,
                                updateColumnIndexes,
                                updateColumnCount,
                                baseMemory,
                                updateMemory,
                                startPartitionRowId,
                                firstUpdatedPartitionRowId);

                        updateEffectiveColumnTops(
                                tableWriter,
                                currentPartitionIndex,
                                updateColumnIndexes,
                                updateColumnCount,
                                firstUpdatedPartitionRowId
                        );
                    }

                    openPartitionColumnsForRead(tableWriter, baseMemory, partitionIndex, updateColumnIndexes);
                    openPartitionColumnsForWrite(tableWriter, updateMemory, partitionIndex, updateColumnIndexes);
                    currentPartitionIndex = partitionIndex;
                    startPartitionRowId = 0;
                    firstUpdatedPartitionRowId = partitionRowId;
                }

                updateColumnValues(
                        tableWriter,
                        partitionIndex,
                        updateColumnIndexes,
                        updateColumnCount,
                        baseMemory,
                        updateMemory,
                        startPartitionRowId,
                        partitionRowId,
                        masterRecord,
                        firstUpdatedPartitionRowId);

                startPartitionRowId = ++partitionRowId;
                rowsUpdated++;
            }

            if (currentPartitionIndex > -1) {
                copyColumnValues(
                        tableWriter,
                        currentPartitionIndex,
                        updateColumnIndexes,
                        updateColumnCount,
                        baseMemory,
                        updateMemory,
                        startPartitionRowId,
                        firstUpdatedPartitionRowId);

                updateEffectiveColumnTops(
                        tableWriter,
                        currentPartitionIndex,
                        updateColumnIndexes,
                        updateColumnCount,
                        firstUpdatedPartitionRowId
                );
            }
        } finally {
            Misc.freeObjList(baseMemory);
            Misc.freeObjList(updateMemory);
            baseMemory.clear();
            updateMemory.clear();
        }
        if (currentPartitionIndex > -1) {
            rebuildIndexes(tableName, writerMetadata, tableWriter);
            tableWriter.commit();
            tableWriter.openLastPartition();
            purgeOldColumnVersions(tableWriter, updateColumnIndexes, ff);
        }

        LOG.info().$("update finished [table=").$(tableName)
                .$(", updated=").$(rowsUpdated)
                .$(", txn=").$(tableWriter.getTxn())
                .I$();

        return rowsUpdated;
    }

    private static void updateEffectiveColumnTops(
            TableWriter tableWriter,
            int partitionIndex,
            IntList updateColumnIndexes,
            int columnCount,
            long firstUpdatedPartitionRowId
    ) {
        final long partitionTimestamp = tableWriter.getPartitionTimestamp(partitionIndex);

        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            final int updateColumnIndex = updateColumnIndexes.get(columnIndex);
            final long columnTop = tableWriter.getColumnTop(partitionTimestamp, updateColumnIndex, -1);
            long effectiveColumnTop = calculatedEffectiveColumnTop(firstUpdatedPartitionRowId, columnTop);
            if (effectiveColumnTop > -1L) {
                tableWriter.upsertColumnVersion(partitionTimestamp, updateColumnIndex, effectiveColumnTop);
            }
        }
    }

    private static long calculatedEffectiveColumnTop(long firstUpdatedPartitionRowId, long columnTop) {
        if (columnTop > -1L) {
            return Math.min(firstUpdatedPartitionRowId, columnTop);
        }
        return firstUpdatedPartitionRowId;
    }

    private void copyColumnValues(
            TableWriter tableWriter,
            int partitionIndex,
            IntList updateColumnIndexes,
            int columnCount,
            ObjList<MemoryCMR> baseMemory,
            ObjList<MemoryCMARW> updateMemory,
            long startPartitionRowId,
            long firstUpdatedPartitionRowId
    ) {
        final TableWriterMetadata metadata = tableWriter.getMetadata();
        final long partitionTimestamp = tableWriter.getPartitionTimestamp(partitionIndex);
        final long partitionSize = tableWriter.getPartitionSize(partitionIndex);
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            MemoryCMR baseFixedColumnFile = baseMemory.get(2 * columnIndex);
            MemoryCMARW updatedFixedColumnFile = updateMemory.get(2 * columnIndex);
            MemoryCMR baseVariableColumnFile = baseMemory.get(2 * columnIndex + 1);
            MemoryCMARW updatedVariableColumnFile = updateMemory.get(2 * columnIndex + 1);

            final int updateColumnIndex = updateColumnIndexes.get(columnIndex);
            final long existingColumnTop = tableWriter.getColumnTop(partitionTimestamp, updateColumnIndex, -1);
            final long columnTop = calculatedEffectiveColumnTop(firstUpdatedPartitionRowId, existingColumnTop);
            int columnType = metadata.getColumnType(updateColumnIndex);

            if (partitionSize > startPartitionRowId) {
                int typeSize = getFixedColumnSize(columnType);
                fillUpdatesGap(
                        startPartitionRowId,
                        partitionSize,
                        baseFixedColumnFile,
                        updatedFixedColumnFile,
                        baseVariableColumnFile,
                        updatedVariableColumnFile,
                        columnTop,
                        existingColumnTop,
                        columnType,
                        typeSize
                );
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

        long address = baseFixedColumnFile.addressOf(fromRowId * typeSize);
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.STRING:
            case ColumnType.BINARY:
                long varStartOffset = baseFixedColumnFile.getLong(fromRowId * Long.BYTES);
                long varEndOffset = baseFixedColumnFile.getLong((toRowId) * Long.BYTES);
                long varAddress = baseVariableColumnFile.addressOf(varStartOffset);
                long copyToOffset = updatedVariableColumnFile.getAppendOffset();
                updatedVariableColumnFile.putBlockOfBytes(varAddress, varEndOffset - varStartOffset);
                updatedFixedColumnFile.extend((toRowId + 1) * typeSize);
                Vect.shiftCopyFixedSizeColumnData(varStartOffset - copyToOffset, address + Long.BYTES, 0, toRowId - fromRowId - 1, updatedFixedColumnFile.getAppendAddress());
                updatedFixedColumnFile.jumpTo((toRowId + 1) * typeSize);
                break;
            default:
                updatedFixedColumnFile.putBlockOfBytes(address, (toRowId - fromRowId) * typeSize);
                break;
        }
    }

    private void fillUpdatesGap(
            long beginRowId,
            long endRowId,
            MemoryCMR basePrimaryColumnFile,
            MemoryCMARW updatedFixedColumnFile,
            MemoryCMR baseVariableColumnFile,
            MemoryCMARW updatedVariableColumnFile,
            long newColumnTop,
            long existingColumnTop,
            int columnType,
            int typeSize
    ) {
        assert newColumnTop <= existingColumnTop || existingColumnTop < 0;

        if (existingColumnTop == -1) {
            if (beginRowId > 0) {
                // Column did not exist at the partition
                fillUpdatesGapWithNull(
                        beginRowId,
                        endRowId,
                        updatedFixedColumnFile,
                        updatedVariableColumnFile,
                        columnType,
                        typeSize
                );
            }
        }

        if (existingColumnTop == 0) {
            // Column fully exists in the partition
            copyRecords(
                    beginRowId,
                    endRowId,
                    basePrimaryColumnFile,
                    updatedFixedColumnFile,
                    baseVariableColumnFile,
                    updatedVariableColumnFile,
                    columnType,
                    typeSize
            );
        }

        if (existingColumnTop > 0) {
            if (beginRowId >= existingColumnTop) {
                copyRecords(
                        beginRowId - existingColumnTop,
                        endRowId - existingColumnTop,
                        basePrimaryColumnFile,
                        updatedFixedColumnFile,
                        baseVariableColumnFile,
                        updatedVariableColumnFile,
                        columnType,
                        typeSize
                );
            } else {
                // beginRowId < existingColumnTop
                if (endRowId <= existingColumnTop) {
                    if (beginRowId > 0) {
                        fillUpdatesGapWithNull(
                                beginRowId,
                                endRowId,
                                updatedFixedColumnFile,
                                updatedVariableColumnFile,
                                columnType,
                                typeSize
                        );
                    }
                } else {
                    // beginRowId < existingColumnTop &&  existingColumnTop < endRowId
                    if (beginRowId > newColumnTop) {
                        fillUpdatesGapWithNull(
                                beginRowId,
                                existingColumnTop,
                                updatedFixedColumnFile,
                                updatedVariableColumnFile,
                                columnType,
                                typeSize
                        );
                    }
                    copyRecords(
                            0,
                            endRowId - existingColumnTop,
                            basePrimaryColumnFile,
                            updatedFixedColumnFile,
                            baseVariableColumnFile,
                            updatedVariableColumnFile,
                            columnType,
                            typeSize
                    );
                }
            }
        }
    }

    private void fillUpdatesGapWithNull(
            long fromRowId,
            long toRowId,
            MemoryCMARW updatedFixedColumnFile,
            MemoryCMARW updatedVariableColumnFile,
            int columnType,
            int typeSize) {

        final short columnTag = ColumnType.tagOf(columnType);
        switch (columnTag) {
            case ColumnType.STRING:
                for (long id = fromRowId; id < toRowId; id++) {
                    updatedFixedColumnFile.putLong(updatedVariableColumnFile.putNullStr());
                }
                break;
            case ColumnType.BINARY:
                for (long id = fromRowId; id < toRowId; id++) {
                    updatedFixedColumnFile.putLong(updatedVariableColumnFile.putNullBin());
                }
                break;
            default:
                final long len = toRowId - fromRowId;
                TableUtils.setNull(columnType, updatedFixedColumnFile.appendAddressFor(len * typeSize), len);
        }
    }

    private int getFixedColumnSize(int columnType) {
        int typeSize = ColumnType.sizeOf(columnType);
        if (isVariableLength(columnType)) {
            typeSize = Long.BYTES;
        }
        return typeSize;
    }

    private void initUpdateMemory(RecordMetadata metadata, int columnCount) {
        for (int i = updateMemory.size(); i < columnCount; i++) {
            int columnType = metadata.getColumnType(updateColumnIndexes.get(i));
            switch (columnType) {
                default:
                    baseMemory.add(Vm.getCMRInstance());
                    baseMemory.add(null);
                    updateMemory.add(Vm.getCMARWInstance());
                    updateMemory.add(null);
                    break;
                case ColumnType.STRING:
                case ColumnType.BINARY:
                    // Primary and secondary
                    baseMemory.add(Vm.getCMRInstance());
                    baseMemory.add(Vm.getCMRInstance());
                    updateMemory.add(Vm.getCMARWInstance());
                    updateMemory.add(Vm.getCMARWInstance());
                    break;
            }
        }
    }

    private void openPartitionColumns(TableWriter tableWriter, ObjList<? extends MemoryCM> memList, int partitionIndex, IntList updateColumnIndexes, boolean forWrite) {
        long partitionTimestamp = tableWriter.getPartitionTimestamp(partitionIndex);
        long partitionNameTxn = tableWriter.getPartitionNameTxn(partitionIndex);
        RecordMetadata metadata = tableWriter.getMetadata();
        try {
            path.concat(tableWriter.getTableName());
            TableUtils.setPathForPartition(path, tableWriter.getPartitionBy(), partitionTimestamp, false);
            TableUtils.txnPartitionConditionally(path, partitionNameTxn);
            int pathTrimToLen = path.length();
            for (int i = 0, n = updateColumnIndexes.size(); i < n; i++) {
                int columnIndex = updateColumnIndexes.get(i);
                CharSequence name = metadata.getColumnName(columnIndex);
                int columnType = metadata.getColumnType(columnIndex);

                final long columnTop = tableWriter.getColumnTop(partitionTimestamp, columnIndex, -1L);

                if (forWrite) {
                    long existingVersion = tableWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
                    tableWriter.upsertColumnVersion(partitionTimestamp, columnIndex, columnTop);
                    if (columnTop > -1) {
                        // columnTop == -1 means column did not exist at the partition
                        cleanupColumnVersions.add(columnIndex, existingVersion, partitionTimestamp, partitionNameTxn);
                    }
                }

                long columnNameTxn = tableWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
                if (isVariableLength(columnType)) {
                    MemoryCMR colMemIndex = (MemoryCMR) memList.get(2 * i);
                    colMemIndex.close();
                    assert !colMemIndex.isOpen();
                    MemoryCMR colMemVar = (MemoryCMR) memList.get(2 * i + 1);
                    colMemVar.close();
                    assert !colMemVar.isOpen();

                    if (forWrite || columnTop != -1) {
                        colMemIndex.of(
                                ff,
                                iFile(path.trimTo(pathTrimToLen), name, columnNameTxn),
                                dataAppendPageSize,
                                -1,
                                MemoryTag.MMAP_UPDATE,
                                fileOpenOpts
                        );
                        colMemVar.of(
                                ff,
                                dFile(path.trimTo(pathTrimToLen), name, columnNameTxn),
                                dataAppendPageSize,
                                -1,
                                MemoryTag.MMAP_UPDATE,
                                fileOpenOpts
                        );
                    }
                } else {
                    MemoryCMR colMem = (MemoryCMR) memList.get(2 * i);
                    colMem.close();
                    assert !colMem.isOpen();

                    if (forWrite || columnTop != -1) {
                        colMem.of(
                                ff,
                                dFile(path.trimTo(pathTrimToLen), name, columnNameTxn),
                                dataAppendPageSize,
                                -1,
                                MemoryTag.MMAP_UPDATE,
                                fileOpenOpts
                        );
                    }
                }
                if (forWrite) {
                    if (isVariableLength(columnType)) {
                        ((MemoryCMARW) memList.get(2 * i)).putLong(0);
                    }
                }
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void openPartitionColumnsForRead(TableWriter tableWriter, ObjList<? extends MemoryCM> baseMemory, int partitionIndex, IntList updateColumnIndexes) {
        openPartitionColumns(tableWriter, baseMemory, partitionIndex, updateColumnIndexes, false);
    }

    private void openPartitionColumnsForWrite(TableWriter tableWriter, ObjList<MemoryCMARW> updateMemory, int partitionIndex, IntList updateColumnIndexes) {
        openPartitionColumns(tableWriter, updateMemory, partitionIndex, updateColumnIndexes, true);
    }

    private void purgeColumnVersionAsync(
            String tableName,
            CharSequence columnName,
            int tableId,
            int tableTruncateVersion,
            int columnType,
            int partitionBy,
            long updatedTxn,
            LongList columnVersions
    ) {
        Sequence pubSeq = messageBus.getColumnVersionPurgePubSeq();
        while (true) {
            long cursor = pubSeq.next();
            if (cursor > -1L) {
                ColumnVersionPurgeTask task = messageBus.getColumnVersionPurgeQueue().get(cursor);
                task.of(tableName, columnName, tableId, tableTruncateVersion, columnType, partitionBy, updatedTxn, columnVersions);
                pubSeq.done(cursor);
                return;
            } else if (cursor == -1L) {
                // Queue overflow
                LOG.error().$("cannot schedule to purge updated column version async [table=").$(tableName)
                        .$(", column=").$(columnName)
                        .$(", lastTxn=").$(updatedTxn)
                        .I$();
                return;
            }
        }
    }

    private void purgeOldColumnVersions(TableWriter tableWriter, IntList updateColumnIndexes, FilesFacade ff) {
        boolean anyReadersBeforeCommittedTxn = tableWriter.checkScoreboardHasReadersBeforeLastCommittedTxn();
        TableWriterMetadata writerMetadata = tableWriter.getMetadata();

        int pathTrimToLen = path.length();
        path.concat(tableWriter.getTableName());
        int pathTableLen = path.length();
        long updatedTxn = tableWriter.getTxn();

        // Process updated column by column, one at the time
        for (int updatedCol = 0, nn = updateColumnIndexes.size(); updatedCol < nn; updatedCol++) {
            int processColumnIndex = updateColumnIndexes.getQuick(updatedCol);
            CharSequence columnName = writerMetadata.getColumnName(processColumnIndex);
            int columnType = writerMetadata.getColumnType(processColumnIndex);
            cleanupColumnVersionsAsync.clear();

            for (int i = 0, n = cleanupColumnVersions.size(); i < n; i += 4) {
                int columnIndex = (int) cleanupColumnVersions.getQuick(i);
                long columnVersion = cleanupColumnVersions.getQuick(i + 1);
                long partitionTimestamp = cleanupColumnVersions.getQuick(i + 2);
                long partitionNameTxn = cleanupColumnVersions.getQuick(i + 3);

                // Process updated column by column, one at a time
                if (columnIndex == processColumnIndex) {
                    boolean columnPurged = !anyReadersBeforeCommittedTxn;
                    if (!anyReadersBeforeCommittedTxn) {
                        path.trimTo(pathTableLen);
                        TableUtils.setPathForPartition(path, tableWriter.getPartitionBy(), partitionTimestamp, false);
                        TableUtils.txnPartitionConditionally(path, partitionNameTxn);
                        int pathPartitionLen = path.length();
                        TableUtils.dFile(path, columnName, columnVersion);
                        if (!ff.remove(path.$())) {
                            columnPurged = false;
                        }
                        if (columnPurged && ColumnType.isVariableLength(columnType)) {
                            path.trimTo(pathPartitionLen);
                            TableUtils.iFile(path, columnName, columnVersion);
                            TableUtils.iFile(path.$(), columnName, columnVersion);
                            if (!ff.remove(path.$()) && ff.exists(path)) {
                                columnPurged = false;
                            }
                        }
                        if (columnPurged && writerMetadata.isColumnIndexed(columnIndex)) {
                            path.trimTo(pathPartitionLen);
                            BitmapIndexUtils.valueFileName(path, columnName, columnVersion);
                            if (!ff.remove(path.$()) && ff.exists(path)) {
                                columnPurged = false;
                            }

                            path.trimTo(pathPartitionLen);
                            BitmapIndexUtils.keyFileName(path, columnName, columnVersion);
                            if (!ff.remove(path.$()) && ff.exists(path)) {
                                columnPurged = false;
                            }
                        }
                    }

                    if (!columnPurged) {
                        cleanupColumnVersionsAsync.add(columnVersion, partitionTimestamp, partitionNameTxn, 0L);
                    }
                }
            }

            // if anything not purged, schedule async purge
            if (cleanupColumnVersionsAsync.size() > 0) {
                purgeColumnVersionAsync(
                        tableWriter.getTableName(),
                        columnName,
                        writerMetadata.getId(),
                        (int) tableWriter.getTruncateVersion(),
                        columnType,
                        tableWriter.getPartitionBy(),
                        updatedTxn,
                        cleanupColumnVersionsAsync
                );
                LOG.info().$("updated column cleanup scheduled [table=").$(tableWriter.getTableName())
                        .$(", column=").$(columnName)
                        .$(", updateTxn=").$(updatedTxn).I$();
            } else {
                LOG.info().$("updated column version cleaned [table=").$(tableWriter.getTableName())
                        .$(", column=").$(columnName)
                        .$(", newColumnVersion=").$(updatedTxn - 1).I$();
            }
        }

        path.trimTo(pathTrimToLen);
    }

    private void rebuildIndexes(String tableName, TableWriterMetadata writerMetadata, TableWriter tableWriter) {
        int pathTrimToLen = path.length();
        indexBuilder.of(path.concat(tableName), configuration);
        for (int i = 0, n = updateColumnIndexes.size(); i < n; i++) {
            int columnIndex = updateColumnIndexes.get(i);
            if (writerMetadata.isColumnIndexed(columnIndex)) {
                CharSequence colName = writerMetadata.getColumnName(columnIndex);
                indexBuilder.rebuildColumn(colName, tableWriter);
            }
        }
        indexBuilder.clear();
        path.trimTo(pathTrimToLen);
    }

    private void updateColumnValues(
            TableWriter tableWriter,
            int partitionIndex,
            IntList updateColumnIndexes,
            int columnCount,
            ObjList<MemoryCMR> baseMemory,
            ObjList<MemoryCMARW> updateMemory,
            long startPartitionRowId,
            long partitionRowId,
            Record masterRecord,
            long firstUpdatedRowId
    ) throws SqlException {
        final TableWriterMetadata metadata = tableWriter.getMetadata();
        final long partitionTimestamp = tableWriter.getPartitionTimestamp(partitionIndex);
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            MemoryCMR baseFixedColumnFile = baseMemory.get(2 * columnIndex);
            MemoryCMARW updatedFixedColumnFile = updateMemory.get(2 * columnIndex);
            MemoryCMR baseVariableColumnFile = baseMemory.get(2 * columnIndex + 1);
            MemoryCMARW updatedVariableColumnFile = updateMemory.get(2 * columnIndex + 1);

            final int updateColumnIndex = updateColumnIndexes.get(columnIndex);
            final long existingColumnTop = tableWriter.getColumnTop(partitionTimestamp, updateColumnIndex, -1);
            final long columnTop = calculatedEffectiveColumnTop(firstUpdatedRowId, existingColumnTop);
            int columnType = metadata.getColumnType(updateColumnIndex);
            int typeSize = getFixedColumnSize(columnType);

            if (partitionRowId > startPartitionRowId) {
                fillUpdatesGap(
                        startPartitionRowId,
                        partitionRowId,
                        baseFixedColumnFile,
                        updatedFixedColumnFile,
                        baseVariableColumnFile,
                        updatedVariableColumnFile,
                        columnTop,
                        existingColumnTop,
                        columnType,
                        typeSize
                );
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
                case ColumnType.SYMBOL:
                    CharSequence symValue = masterRecord.getSym(columnIndex);
                    int symbolIndex = tableWriter.getSymbolIndex(updateColumnIndexes.get(columnIndex), symValue);
                    updatedFixedColumnFile.putInt(symbolIndex);
                    break;
                case ColumnType.STRING:
                    charSink.clear();
                    masterRecord.getStr(columnIndex, charSink);
                    long strOffset = updatedVariableColumnFile.putStr(charSink);
                    updatedFixedColumnFile.putLong(strOffset);
                    break;
                case ColumnType.BINARY:
                    BinarySequence binValue = masterRecord.getBin(columnIndex);
                    long binOffset = updatedVariableColumnFile.putBin(binValue);
                    updatedFixedColumnFile.putLong(binOffset);
                    break;
                default:
                    throw SqlException.$(0, "Column type ")
                            .put(ColumnType.nameOf(columnType))
                            .put(" not supported for updates");
            }
        }
    }
}
