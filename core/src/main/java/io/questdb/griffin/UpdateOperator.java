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
import io.questdb.griffin.engine.ops.UpdateOperation;
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
    private final ObjList<MemoryCMR> srcColumns = new ObjList<>();
    private final ObjList<MemoryCMARW> dstColumns = new ObjList<>();
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

        LOG.info().$("updating [table=").$(tableWriter.getTableName()).I$();

        try {
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
            final int affectedColumnCount = updateMetadata.getColumnCount();

            // Build index column map from table to update to values returned from the update statement row cursors
            updateColumnIndexes.clear();
            for (int i = 0; i < affectedColumnCount; i++) {
                CharSequence columnName = updateMetadata.getColumnName(i);
                int tableColumnIndex = writerMetadata.getColumnIndex(columnName);
                assert tableColumnIndex >= 0;
                updateColumnIndexes.add(tableColumnIndex);
            }

            // Create update memory list of all columns to be updated
            configureColumns(writerMetadata, affectedColumnCount);

            // Start execution frame by frame
            // Partition to update
            int partitionIndex = -1;
            long rowsUpdated = 0;

            // Update may be queued and requester already disconnected, force check someone still waits for it
            op.forceTestTimeout();
            // Row by row updates for now
            // This should happen parallel per file (partition and column)
            try (RecordCursor recordCursor = factory.getCursor(sqlExecutionContext)) {
                Record masterRecord = recordCursor.getRecord();

                long prevRow = 0;
                long minRow = -1L;
                long lastRowId = Long.MIN_VALUE;
                while (recordCursor.hasNext()) {
                    long rowId = masterRecord.getUpdateRowId();

                    // Some joins expand results set and returns same row multiple times
                    if (rowId == lastRowId) {
                        continue;
                    }
                    if (rowId < lastRowId) {
                        // We're assuming, but not enforcing the fact that
                        // factory produces rows in incrementing order.
                        throw CairoException.instance(0).put(
                                "Update statement generated invalid plan. " +
                                        "Rows are not returned in order.");
                    }
                    lastRowId = rowId;

                    final int rowPartitionIndex = Rows.toPartitionIndex(rowId);
                    final long currentRow = Rows.toLocalRowID(rowId);

                    if (rowPartitionIndex != partitionIndex) {
                        LOG.info()
                                .$("updating partition [partitionIndex=").$(partitionIndex)
                                .$(", rowPartitionIndex=").$(rowPartitionIndex)
                                .$(", rowPartitionTs=").$ts(tableWriter.getPartitionTimestamp(rowPartitionIndex))
                                .$(", affectedColumnCount=").$(affectedColumnCount)
                                .$(", prevRow=").$(prevRow)
                                .$(", minRow=").$(minRow)
                                .I$();
                        if (partitionIndex > -1) {
                            copyColumns(
                                    partitionIndex,
                                    affectedColumnCount,
                                    prevRow,
                                    minRow
                            );

                            updateEffectiveColumnTops(
                                    tableWriter,
                                    partitionIndex,
                                    updateColumnIndexes,
                                    affectedColumnCount,
                                    minRow
                            );
                        }

                        openColumns(srcColumns, rowPartitionIndex, false);
                        openColumns(dstColumns, rowPartitionIndex, true);

                        partitionIndex = rowPartitionIndex;
                        prevRow = 0;
                        minRow = currentRow;
                    }

                    appendRowUpdate(
                            rowPartitionIndex,
                            affectedColumnCount,
                            prevRow,
                            currentRow,
                            masterRecord,
                            minRow
                    );

                    prevRow = currentRow + 1;
                    rowsUpdated++;

                    op.testTimeout();
                }

                if (partitionIndex > -1) {
                    copyColumns(partitionIndex, affectedColumnCount, prevRow, minRow);

                    updateEffectiveColumnTops(
                            tableWriter,
                            partitionIndex,
                            updateColumnIndexes,
                            affectedColumnCount,
                            minRow
                    );
                }

            } finally {
                Misc.freeObjList(srcColumns);
                Misc.freeObjList(dstColumns);
                // todo: we are opening columns incrementally, e.g. if we don't have enough objects
                //   but here we're always clearing columns, making incremental "open" pointless
                //   perhaps we should keep N column object max to kick around ?
                srcColumns.clear();
                dstColumns.clear();
            }

            if (partitionIndex > -1) {
                rebuildIndexes(tableName, writerMetadata, tableWriter);
                op.forceTestTimeout();
                tableWriter.commit();
                tableWriter.openLastPartition();
                purgeOldColumnVersions(tableWriter, updateColumnIndexes, ff);
            }

            LOG.info().$("update finished [table=").$(tableName)
                    .$(", updated=").$(rowsUpdated)
                    .$(", txn=").$(tableWriter.getTxn())
                    .I$();

            return rowsUpdated;
        } catch (ReaderOutOfDateException e) {
            tableWriter.rollbackColumnVersions();
            throw e;
        } catch (Throwable th) {
            LOG.error().$("UPDATE failed: ").$(th).$();
            tableWriter.rollbackColumnVersions();
            throw th;
        } finally {
            op.closeWriter();
        }
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

    private static int getFixedColumnSize(int columnType) {
        if (isVariableLength(columnType)) {
            return 3;
        }
        return ColumnType.pow2SizeOf(columnType);
    }

    private static void fillUpdatesGapWithNull(
            int columnType,
            long fromRow, // inclusive
            long toRow, // exclusive
            MemoryCMARW dstFixMem,
            MemoryCMARW dstVarMem,
            int shl
    ) {
        final short columnTag = ColumnType.tagOf(columnType);
        switch (columnTag) {
            case ColumnType.STRING:
                for (long row = fromRow; row < toRow; row++) {
                    dstFixMem.putLong(dstVarMem.putNullStr());
                }
                break;
            case ColumnType.BINARY:
                for (long row = fromRow; row < toRow; row++) {
                    dstFixMem.putLong(dstVarMem.putNullBin());
                }
                break;
            default:
                final long rowCount = toRow - fromRow;
                TableUtils.setNull(
                        columnType,
                        dstFixMem.appendAddressFor(rowCount << shl),
                        rowCount
                );
                break;
        }
    }

    private void appendRowUpdate(
            int rowPartitionIndex,
            int affectedColumnCount,
            long prevRow,
            long currentRow,
            Record masterRecord,
            long firstUpdatedRowId
    ) throws SqlException {
        final TableWriterMetadata metadata = tableWriter.getMetadata();
        final long partitionTimestamp = tableWriter.getPartitionTimestamp(rowPartitionIndex);
        for (int i = 0; i < affectedColumnCount; i++) {
            MemoryCMR srcFixMem = srcColumns.get(2 * i);
            MemoryCMARW dstFixMem = dstColumns.get(2 * i);
            MemoryCMR srcVarMem = srcColumns.get(2 * i + 1);
            MemoryCMARW dstVarMem = dstColumns.get(2 * i + 1);

            final int columnIndex = updateColumnIndexes.get(i);
            final long oldColumnTop = tableWriter.getColumnTop(partitionTimestamp, columnIndex, -1);
            final long newColumnTop = calculatedEffectiveColumnTop(firstUpdatedRowId, oldColumnTop);
            final int columnType = metadata.getColumnType(columnIndex);

            if (currentRow > prevRow) {
                copyColumn(
                        prevRow,
                        currentRow,
                        srcFixMem,
                        srcVarMem,
                        dstFixMem,
                        dstVarMem,
                        newColumnTop,
                        oldColumnTop,
                        columnType
                );
            }
            switch (ColumnType.tagOf(columnType)) {
                case ColumnType.INT:
                    dstFixMem.putInt(masterRecord.getInt(i));
                    break;
                case ColumnType.FLOAT:
                    dstFixMem.putFloat(masterRecord.getFloat(i));
                    break;
                case ColumnType.LONG:
                    dstFixMem.putLong(masterRecord.getLong(i));
                    break;
                case ColumnType.TIMESTAMP:
                    dstFixMem.putLong(masterRecord.getTimestamp(i));
                    break;
                case ColumnType.DATE:
                    dstFixMem.putLong(masterRecord.getDate(i));
                    break;
                case ColumnType.DOUBLE:
                    dstFixMem.putDouble(masterRecord.getDouble(i));
                    break;
                case ColumnType.SHORT:
                    dstFixMem.putShort(masterRecord.getShort(i));
                    break;
                case ColumnType.CHAR:
                    dstFixMem.putChar(masterRecord.getChar(i));
                    break;
                case ColumnType.BYTE:
                case ColumnType.BOOLEAN:
                    dstFixMem.putByte(masterRecord.getByte(i));
                    break;
                case ColumnType.GEOBYTE:
                    dstFixMem.putByte(masterRecord.getGeoByte(i));
                    break;
                case ColumnType.GEOSHORT:
                    dstFixMem.putShort(masterRecord.getGeoShort(i));
                    break;
                case ColumnType.GEOINT:
                    dstFixMem.putInt(masterRecord.getGeoInt(i));
                    break;
                case ColumnType.GEOLONG:
                    dstFixMem.putLong(masterRecord.getGeoLong(i));
                    break;
                case ColumnType.SYMBOL:
                    dstFixMem.putInt(tableWriter.getSymbolIndex(updateColumnIndexes.get(i), masterRecord.getSym(i)));
                    break;
                case ColumnType.STRING:

                    // todo: was ist das ?
                    charSink.clear();
                    masterRecord.getStr(i, charSink);
                    dstFixMem.putLong(dstVarMem.putStr(charSink));
                    break;
                case ColumnType.BINARY:
                    BinarySequence binValue = masterRecord.getBin(i);
                    long binOffset = dstVarMem.putBin(binValue);
                    dstFixMem.putLong(binOffset);
                    break;
                default:
                    throw SqlException.$(0, "Column type ")
                            .put(ColumnType.nameOf(columnType))
                            .put(" not supported for updates");
            }
        }
    }

    private void configureColumns(RecordMetadata metadata, int columnCount) {
        for (int i = dstColumns.size(); i < columnCount; i++) {
            int columnType = metadata.getColumnType(updateColumnIndexes.get(i));
            switch (columnType) {
                default:
                    srcColumns.add(Vm.getCMRInstance());
                    srcColumns.add(null);
                    dstColumns.add(Vm.getCMARWInstance());
                    dstColumns.add(null);
                    break;
                case ColumnType.STRING:
                case ColumnType.BINARY:
                    // Primary and secondary
                    srcColumns.add(Vm.getCMRInstance());
                    srcColumns.add(Vm.getCMRInstance());
                    dstColumns.add(Vm.getCMARWInstance());
                    dstColumns.add(Vm.getCMARWInstance());
                    break;
            }
        }
    }

    private void copyColumn(
            long prevRow,
            long maxRow,
            MemoryCMR srcFixMem,
            MemoryCMR srcVarMem,
            MemoryCMARW dstFixMem,
            MemoryCMARW dstVarMem,
            long newColumnTop,
            long oldColumnTop,
            int columnType
    ) {
        assert newColumnTop <= oldColumnTop || oldColumnTop < 0;

        final int shl = getFixedColumnSize(columnType);

        if (oldColumnTop == -1 && prevRow > 0) {
            // Column did not exist at the partition
            fillUpdatesGapWithNull(columnType, prevRow, maxRow, dstFixMem, dstVarMem, shl);
        }

        if (oldColumnTop == 0) {
            // Column fully exists in the partition
            copyValues(
                    prevRow,
                    maxRow,
                    srcFixMem,
                    srcVarMem,
                    dstFixMem,
                    dstVarMem,
                    columnType,
                    shl
            );
        }

        if (oldColumnTop > 0) {
            if (prevRow >= oldColumnTop) {
                copyValues(
                        prevRow - oldColumnTop,
                        maxRow - oldColumnTop,
                        srcFixMem,
                        srcVarMem, dstFixMem,
                        dstVarMem,
                        columnType,
                        shl
                );
            } else {
                // prevRow < oldColumnTop
                if (maxRow <= oldColumnTop) {
                    if (prevRow > 0) {
                        fillUpdatesGapWithNull(
                                columnType,
                                prevRow,
                                maxRow,
                                dstFixMem,
                                dstVarMem,
                                shl
                        );
                    }
                } else {
                    // prevRow < oldColumnTop &&  oldColumnTop < maxRow
                    if (prevRow > newColumnTop) {
                        fillUpdatesGapWithNull(
                                columnType,
                                prevRow,
                                oldColumnTop,
                                dstFixMem,
                                dstVarMem,
                                shl
                        );
                    }
                    copyValues(
                            0,
                            maxRow - oldColumnTop,
                            srcFixMem,
                            srcVarMem,
                            dstFixMem,
                            dstVarMem,
                            columnType,
                            shl
                    );
                }
            }
        }
    }

    private void copyColumns(int partitionIndex, int affectedColumnCount, long prevRow, long minRow) {
        final TableWriterMetadata metadata = tableWriter.getMetadata();
        final long partitionTimestamp = tableWriter.getPartitionTimestamp(partitionIndex);
        final long maxRow = tableWriter.getPartitionSize(partitionIndex);
        for (int i = 0; i < affectedColumnCount; i++) {
            MemoryCMR srcFixMem = srcColumns.get(2 * i);
            MemoryCMR srcVarMem = srcColumns.get(2 * i + 1);
            MemoryCMARW dstFixMem = dstColumns.get(2 * i);
            MemoryCMARW dstVarMem = dstColumns.get(2 * i + 1);

            final int columnIndex = updateColumnIndexes.getQuick(i);
            final long oldColumnTop = tableWriter.getColumnTop(partitionTimestamp, columnIndex, -1);
            final long newColumnTop = calculatedEffectiveColumnTop(minRow, oldColumnTop);
            final int columnType = metadata.getColumnType(columnIndex);

            if (maxRow > prevRow) {
                copyColumn(
                        prevRow,
                        maxRow,
                        srcFixMem,
                        srcVarMem,
                        dstFixMem,
                        dstVarMem,
                        newColumnTop,
                        oldColumnTop,
                        columnType
                );
            }
        }
    }

    private void copyValues(
            long fromRowId,
            long toRowId,
            MemoryCMR srcFixMem,
            MemoryCMR srcVarMem,
            MemoryCMARW dstFixMem,
            MemoryCMARW dstVarMem,
            int columnType,
            int shl
    ) {
        long address = srcFixMem.addressOf(fromRowId << shl);
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.STRING:
            case ColumnType.BINARY:
                long varStartOffset = srcFixMem.getLong(fromRowId * Long.BYTES);
                long varEndOffset = srcFixMem.getLong((toRowId) * Long.BYTES);
                long varAddress = srcVarMem.addressOf(varStartOffset);
                long copyToOffset = dstVarMem.getAppendOffset();
                dstVarMem.putBlockOfBytes(varAddress, varEndOffset - varStartOffset);
                dstFixMem.extend((toRowId + 1) << shl);
                Vect.shiftCopyFixedSizeColumnData(
                        varStartOffset - copyToOffset,
                        address + Long.BYTES,
                        0,
                        toRowId - fromRowId - 1,
                        dstFixMem.getAppendAddress()
                );
                dstFixMem.jumpTo((toRowId + 1) << shl);
                break;
            default:
                dstFixMem.putBlockOfBytes(address, (toRowId - fromRowId) << shl);
                break;
        }
    }

    private void openColumns(ObjList<? extends MemoryCM> columns, int partitionIndex, boolean forWrite) {
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
                    MemoryCMR colMemIndex = (MemoryCMR) columns.get(2 * i);
                    colMemIndex.close();
                    assert !colMemIndex.isOpen();
                    MemoryCMR colMemVar = (MemoryCMR) columns.get(2 * i + 1);
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
                    MemoryCMR colMem = (MemoryCMR) columns.get(2 * i);
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
                        ((MemoryCMARW) columns.get(2 * i)).putLong(0);
                    }
                }
            }
        } finally {
            path.trimTo(rootLen);
        }
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
                LOG.error().$("failed to schedule column version purge, queue is full. Please run 'VACUUM TABLE \"").$(tableName)
                        .$("\"' [columnName=").$(columnName)
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
}
