/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.std.*;
import io.questdb.std.str.Path;

import static io.questdb.cairo.ColumnType.isVariableLength;
import static io.questdb.cairo.TableUtils.dFile;
import static io.questdb.cairo.TableUtils.iFile;

public class UpdateOperatorImpl implements QuietCloseable, UpdateOperator {
    private static final Log LOG = LogFactory.getLog(UpdateOperatorImpl.class);
    private final long dataAppendPageSize;
    private final ObjList<MemoryCMARW> dstColumns = new ObjList<>();
    private final FilesFacade ff;
    private final long fileOpenOpts;
    private final Path path;
    private final PurgingOperator purgingOperator;
    private final int rootLen;
    private final ObjList<MemoryCMR> srcColumns = new ObjList<>();
    private final TableWriter tableWriter;
    private final IntList updateColumnIndexes = new IntList();
    private IndexBuilder indexBuilder;

    public UpdateOperatorImpl(
            CairoConfiguration configuration,
            TableWriter tableWriter,
            Path path,
            int rootLen,
            PurgingOperator purgingOperator
    ) {
        this.tableWriter = tableWriter;
        this.rootLen = rootLen;
        this.purgingOperator = purgingOperator;
        this.indexBuilder = new IndexBuilder(configuration);
        this.dataAppendPageSize = configuration.getDataAppendPageSize();
        this.fileOpenOpts = configuration.getWriterFileOpenOpts();
        this.ff = configuration.getFilesFacade();
        this.path = path;
    }

    @Override
    public void close() {
        indexBuilder = Misc.free(indexBuilder);
    }

    public long executeUpdate(SqlExecutionContext sqlExecutionContext, UpdateOperation op) throws TableReferenceOutOfDateException {

        TableToken tableToken = tableWriter.getTableToken();
        LOG.info().$("updating [table=").$(tableToken).$(" instance=").$(op.getCorrelationId()).I$();

        try {
            final int tableId = op.getTableId();
            final long tableVersion = op.getTableVersion();
            final RecordCursorFactory factory = op.getFactory();

            purgingOperator.clear();

            if (tableWriter.inTransaction()) {
                LOG.info().$("committing current transaction before UPDATE execution [table=").$(tableToken).$(" instance=").$(op.getCorrelationId()).I$();
                tableWriter.commit();
            }

            final TableRecordMetadata tableMetadata = tableWriter.getMetadata();

            // Check that table structure hasn't changed between planning and executing the UPDATE
            if (tableMetadata.getTableId() != tableId || tableWriter.getMetadataVersion() != tableVersion) {
                throw TableReferenceOutOfDateException.of(tableToken, tableId, tableMetadata.getTableId(),
                        tableVersion, tableWriter.getMetadataVersion());
            }

            // Select the rows to be updated
            final RecordMetadata updateMetadata = factory.getMetadata();
            final int affectedColumnCount = updateMetadata.getColumnCount();

            // Build index column map from table to update to values returned from the update statement row cursors
            updateColumnIndexes.clear();
            for (int i = 0; i < affectedColumnCount; i++) {
                CharSequence columnName = updateMetadata.getColumnName(i);
                int tableColumnIndex = tableMetadata.getColumnIndex(columnName);
                assert tableColumnIndex >= 0;
                updateColumnIndexes.add(tableColumnIndex);
            }

            // Create update memory list of all columns to be updated
            configureColumns(tableMetadata, affectedColumnCount);

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
                        throw CairoException.critical(0).put("Update statement generated invalid query plan. Rows are not returned in order.");
                    }
                    lastRowId = rowId;

                    final int rowPartitionIndex = Rows.toPartitionIndex(rowId);
                    final long currentRow = Rows.toLocalRowID(rowId);

                    if (rowPartitionIndex != partitionIndex) {
                        if (tableWriter.isPartitionReadOnly(rowPartitionIndex)) {
                            throw CairoException.critical(0)
                                    .put("cannot update read-only partition [table=").put(tableToken.getTableName())
                                    .put(", partitionTimestamp=").ts(tableWriter.getPartitionTimestamp(rowPartitionIndex))
                                    .put(']');
                        }
                        if (partitionIndex > -1) {
                            LOG.info()
                                    .$("updating partition [partitionIndex=").$(partitionIndex)
                                    .$(", rowPartitionIndex=").$(rowPartitionIndex)
                                    .$(", rowPartitionTs=").$ts(tableWriter.getPartitionTimestamp(rowPartitionIndex))
                                    .$(", affectedColumnCount=").$(affectedColumnCount)
                                    .$(", prevRow=").$(prevRow)
                                    .$(", minRow=").$(minRow)
                                    .I$();

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

                            rebuildIndexes(tableWriter.getPartitionTimestamp(partitionIndex), tableMetadata, tableWriter);
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

                    rebuildIndexes(tableWriter.getPartitionTimestamp(partitionIndex), tableMetadata, tableWriter);
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
                op.forceTestTimeout();
                tableWriter.commit();
                tableWriter.openLastPartition();
                purgingOperator.purge(
                        path.trimTo(rootLen),
                        tableWriter.getTableToken(),
                        tableWriter.getPartitionBy(),
                        tableWriter.checkScoreboardHasReadersBeforeLastCommittedTxn(),
                        tableWriter.getMetadata(),
                        tableWriter.getTruncateVersion(),
                        tableWriter.getTxn()
                );
            }

            LOG.info().$("update finished [table=").$(tableToken)
                    .$(", instance=").$(op.getCorrelationId())
                    .$(", updated=").$(rowsUpdated)
                    .$(", txn=").$(tableWriter.getTxn())
                    .I$();

            return rowsUpdated;
        } catch (TableReferenceOutOfDateException e) {
            throw e;
        } catch (SqlException e) {
            throw CairoException.critical(0).put("could not apply update on SPI side [e=").put((CharSequence) e).put(']');
        } catch (Throwable th) {
            LOG.error().$("could not update").$(th).$();
            throw th;
        } finally {
            op.closeWriter();
        }
    }

    private static long calculatedEffectiveColumnTop(long firstUpdatedPartitionRowId, long columnTop) {
        if (columnTop > -1L) {
            return Math.min(firstUpdatedPartitionRowId, columnTop);
        }
        return firstUpdatedPartitionRowId;
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

    private static int getFixedColumnSize(int columnType) {
        if (isVariableLength(columnType)) {
            return 3;
        }
        return ColumnType.pow2SizeOf(columnType);
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

    private void appendRowUpdate(
            int rowPartitionIndex,
            int affectedColumnCount,
            long prevRow,
            long currentRow,
            Record masterRecord,
            long firstUpdatedRowId
    ) {
        final TableRecordMetadata tableMetadata = tableWriter.getMetadata();
        final long partitionTimestamp = tableWriter.getPartitionTimestamp(rowPartitionIndex);
        for (int i = 0; i < affectedColumnCount; i++) {
            MemoryCMR srcFixMem = srcColumns.get(2 * i);
            MemoryCMARW dstFixMem = dstColumns.get(2 * i);
            MemoryCMR srcVarMem = srcColumns.get(2 * i + 1);
            MemoryCMARW dstVarMem = dstColumns.get(2 * i + 1);

            final int columnIndex = updateColumnIndexes.get(i);
            final long oldColumnTop = tableWriter.getColumnTop(partitionTimestamp, columnIndex, -1);
            final long newColumnTop = calculatedEffectiveColumnTop(firstUpdatedRowId, oldColumnTop);
            final int toType = tableMetadata.getColumnType(columnIndex);

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
                        toType
                );
            }
            switch (ColumnType.tagOf(toType)) {
                case ColumnType.INT:
                    dstFixMem.putInt(masterRecord.getInt(i));
                    break;
                case ColumnType.IPv4:
                    dstFixMem.putInt(masterRecord.getIPv4(i));
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
                    dstFixMem.putByte(masterRecord.getByte(i));
                    break;
                case ColumnType.BOOLEAN:
                    dstFixMem.putBool(masterRecord.getBool(i));
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
                    // Use special method to write new symbol values
                    // which does not update _txn file transient symbol counts
                    // so that if update fails and rolled back ILP will not use "dirty" symbol indexes
                    // pre-looked up during update run to insert rows
                    dstFixMem.putInt(
                            tableWriter.getSymbolIndexNoTransientCountUpdate(updateColumnIndexes.get(i), masterRecord.getSym(i))
                    );
                    break;
                case ColumnType.STRING:
                    dstFixMem.putLong(dstVarMem.putStr(masterRecord.getStr(i)));
                    break;
                case ColumnType.BINARY:
                    dstFixMem.putLong(dstVarMem.putBin(masterRecord.getBin(i)));
                    break;
                case ColumnType.LONG128:
                    // fall-through
                case ColumnType.UUID:
                    dstFixMem.putLong(masterRecord.getLong128Lo(i));
                    dstFixMem.putLong(masterRecord.getLong128Hi(i));
                    break;
                default:
                    throw CairoException.nonCritical()
                            .put("Column type ").put(ColumnType.nameOf(toType))
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
        final TableRecordMetadata tableMetadata = tableWriter.getMetadata();
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
            final int columnType = tableMetadata.getColumnType(columnIndex);

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
            path.trimTo(rootLen);
            TableUtils.setPathForPartition(path, tableWriter.getPartitionBy(), partitionTimestamp, partitionNameTxn);
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
                        purgingOperator.add(columnIndex, existingVersion, partitionTimestamp, partitionNameTxn);
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

    private void rebuildIndexes(
            long partitionTimestamp,
            TableRecordMetadata tableMetadata,
            TableWriter tableWriter
    ) {
        int pathTrimToLen = path.length();
        indexBuilder.of(path.trimTo(rootLen));
        for (int i = 0, n = updateColumnIndexes.size(); i < n; i++) {
            int columnIndex = updateColumnIndexes.get(i);
            if (tableMetadata.isColumnIndexed(columnIndex)) {
                CharSequence colName = tableMetadata.getColumnName(columnIndex);
                indexBuilder.reindexAfterUpdate(ff, partitionTimestamp, colName, tableWriter);
            }
        }
        indexBuilder.clear();
        path.trimTo(pathTrimToLen);
    }
}
