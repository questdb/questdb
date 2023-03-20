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

package io.questdb.griffin.wal.fuzz;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class FuzzTransactionGenerator {
    private static final int MAX_COLUMNS = 200;

    public static ObjList<FuzzTransaction> generateSet(
            RecordMetadata metadata,
            Rnd rnd,
            long minTimestamp,
            long maxTimestamp,
            int rowCount,
            int transactionCount,
            boolean o3,
            double probabilityOfCancelRow,
            double probabilityOfUnassignedColumnValue,
            double probabilityOfAssigningNull,
            double probabilityOfTransactionRollback,
            double probabilityOfAddingNewColumn,
            double probabilityOfRemovingColumn,
            double probabilityOfRenamingColumn,
            double probabilityOfDataInsert,
            double probabilityOfTruncate,
            int maxStrLenForStrColumns,
            String[] symbols
    ) {
        ObjList<FuzzTransaction> transactionList = new ObjList<>();
        int metaVersion = 0;
        int waitBarrierVersion = 0;
        RecordMetadata meta = GenericRecordMetadata.deepCopyOf(metadata);

        long lastTimestamp = minTimestamp;
        double sumOfProbabilities = probabilityOfAddingNewColumn + probabilityOfRemovingColumn + probabilityOfRemovingColumn + probabilityOfDataInsert + probabilityOfTruncate;
        probabilityOfAddingNewColumn = probabilityOfAddingNewColumn / sumOfProbabilities;
        probabilityOfRemovingColumn = probabilityOfRemovingColumn / sumOfProbabilities;
        probabilityOfRenamingColumn = probabilityOfRenamingColumn / sumOfProbabilities;
        probabilityOfTruncate = probabilityOfTruncate / sumOfProbabilities;

        // Reduce some random parameters if there is too much data so test can finish in reasonable time
        transactionCount = Math.min(transactionCount, 10 * 1_000_000 / rowCount);

        for (int i = 0; i < transactionCount; i++) {
            double transactionType = rnd.nextDouble();
            if (transactionType < probabilityOfRemovingColumn) {
                // generate column remove
                RecordMetadata newTableMetadata = generateDropColumn(transactionList, metaVersion, waitBarrierVersion, rnd, meta);
                if (newTableMetadata != null) {
                    // Sometimes there can be nothing to remove
                    metaVersion++;
                    waitBarrierVersion++;
                    meta = newTableMetadata;
                }
            } else if (transactionType < probabilityOfRemovingColumn + probabilityOfRenamingColumn) {
                // generate column rename
                RecordMetadata newTableMetadata = generateRenameColumn(transactionList, metaVersion, waitBarrierVersion, rnd, meta);
                if (newTableMetadata != null) {
                    // Sometimes there can be nothing to rename
                    metaVersion++;
                    waitBarrierVersion++;
                    meta = newTableMetadata;
                }
            } else if (transactionType < probabilityOfRemovingColumn + probabilityOfRenamingColumn + probabilityOfTruncate) {
                // generate truncate table
                meta = generateTruncateTable(transactionList, metaVersion, waitBarrierVersion++, meta);
            } else if (transactionType < probabilityOfAddingNewColumn + probabilityOfRemovingColumn + probabilityOfRenamingColumn + probabilityOfTruncate && getNonDeletedColumnCount(meta) < MAX_COLUMNS) {
                // generate column add
                meta = generateAddColumn(transactionList, metaVersion++, waitBarrierVersion++, rnd, meta);
            } else {
                // generate row set
                int blockRows = rowCount / (transactionCount - i);
                if (i < transactionCount - 1) {
                    blockRows = (int) ((rnd.nextDouble() * 0.5 + 0.5) * blockRows);
                }

                final long startTs, stopTs;
                if (o3) {
                    long offset = (long) ((maxTimestamp - minTimestamp + 1.0) / transactionCount * i);
                    long jitter = (long) ((maxTimestamp - minTimestamp + 1.0) / transactionCount * rnd.nextDouble());
                    if (rnd.nextBoolean()) {
                        startTs = (minTimestamp + offset + jitter);
                    } else {
                        startTs = (minTimestamp + offset - jitter);
                    }
                } else {
                    long writeInterval = rnd.nextLong((maxTimestamp - minTimestamp) / transactionCount);
                    startTs = lastTimestamp - writeInterval;
                }
                long size = (maxTimestamp - minTimestamp) / transactionCount;
                if (o3) {
                    size *= rnd.nextDouble();
                }
                stopTs = Math.min(startTs + size, maxTimestamp);

                generateDataBlock(
                        transactionList,
                        rnd,
                        metaVersion,
                        waitBarrierVersion,
                        startTs,
                        stopTs,
                        blockRows,
                        o3,
                        probabilityOfCancelRow,
                        probabilityOfUnassignedColumnValue,
                        probabilityOfAssigningNull,
                        probabilityOfTransactionRollback,
                        maxStrLenForStrColumns,
                        symbols,
                        rnd.nextLong(),
                        transactionCount
                );
                rowCount -= blockRows;
                lastTimestamp = stopTs;
            }
        }

        return transactionList;
    }

    private static void copyColumnsExcept(RecordMetadata from, FuzzTestColumnMeta to, int columnIndex) {
        for (int i = 0, n = from.getColumnCount(); i < n; i++) {
            int columnType = from.getColumnType(i);
            if (i == columnIndex) {
                columnType = -columnType;
            }
            to.add(new TableColumnMetadata(
                    from.getColumnName(i),
                    columnType,
                    from.isColumnIndexed(i),
                    from.getIndexValueBlockCapacity(i),
                    from.isSymbolTableStatic(i),
                    GenericRecordMetadata.copyOf(from.getMetadata(i))
            ));
        }
        to.setTimestampIndex(from.getTimestampIndex());
    }

    private static int generateNewColumnType(Rnd rnd) {
        int columnType = FuzzInsertOperation.SUPPORTED_COLUMN_TYPES[rnd.nextInt(FuzzInsertOperation.SUPPORTED_COLUMN_TYPES.length)];
        switch (columnType) {
            default:
                return columnType;
            case ColumnType.GEOBYTE:
                return ColumnType.getGeoHashTypeWithBits(5);
            case ColumnType.GEOSHORT:
                return ColumnType.getGeoHashTypeWithBits(10);
            case ColumnType.GEOINT:
                return ColumnType.getGeoHashTypeWithBits(25);
            case ColumnType.GEOLONG:
                return ColumnType.getGeoHashTypeWithBits(35);
        }
    }

    private static RecordMetadata generateRenameColumn(ObjList<FuzzTransaction> transactionList, int metadataVersion, int waitBarrierVersion, Rnd rnd, RecordMetadata tableMetadata) {
        FuzzTransaction transaction = new FuzzTransaction();
        int startColumnIndex = rnd.nextInt(tableMetadata.getColumnCount());
        for (int i = 0; i < tableMetadata.getColumnCount(); i++) {
            int columnIndex = (startColumnIndex + i) % tableMetadata.getColumnCount();

            int type = tableMetadata.getColumnType(columnIndex);
            if (type > 0 && columnIndex != tableMetadata.getTimestampIndex()) {
                String columnName = tableMetadata.getColumnName(columnIndex);
                String newColName;
                for (int col = 0; ; col++) {
                    newColName = "new_col_" + col;
                    int colIndex = tableMetadata.getColumnIndexQuiet(newColName);
                    if (colIndex == -1) {
                        break;
                    }
                }

                transaction.operationList.add(new FuzzRenameColumnOperation(columnName, newColName));
                transaction.structureVersion = metadataVersion;
                transaction.waitBarrierVersion = waitBarrierVersion;
                transactionList.add(transaction);

                FuzzTestColumnMeta newMeta = new FuzzTestColumnMeta();
                GenericRecordMetadata.copyColumns(tableMetadata, newMeta);
                newMeta.rename(columnIndex, columnName, newColName);
                newMeta.setTimestampIndex(tableMetadata.getTimestampIndex());

                return newMeta;
            }
        }

        // nothing to drop, only timestamp column left
        return null;
    }

    private static RecordMetadata generateTruncateTable(ObjList<FuzzTransaction> transactionList, int metadataVersion, int waitBarrierVersion, RecordMetadata meta) {
        FuzzTransaction transaction = new FuzzTransaction();
        transaction.operationList.add(new TruncateTableOperation());
        transactionList.add(transaction);
        transaction.structureVersion = metadataVersion;
        transaction.waitBarrierVersion = waitBarrierVersion;
        transaction.forceWait();
        return meta;
    }

    private static int getNonDeletedColumnCount(RecordMetadata tableMetadata) {
        if (tableMetadata instanceof FuzzTestColumnMeta) {
            return ((FuzzTestColumnMeta) tableMetadata).getLiveColumnCount();
        } else {
            return tableMetadata.getColumnCount();
        }
    }

    static RecordMetadata generateAddColumn(
            ObjList<FuzzTransaction> transactionList,
            int metadataVersion,
            int waitBarrierVersion,
            Rnd rnd,
            RecordMetadata tableMetadata
    ) {
        FuzzTransaction transaction = new FuzzTransaction();
        int newType = generateNewColumnType(rnd);
        boolean indexFlag = newType == ColumnType.SYMBOL && rnd.nextBoolean();
        int indexValueBlockCapacity = 256;
        boolean symbolTableStatic = rnd.nextBoolean();

        String newColName;
        for (int col = 0; ; col++) {
            newColName = "new_col_" + col;
            int colIndex = tableMetadata.getColumnIndexQuiet(newColName);
            if (colIndex == -1) {
                break;
            }
        }
        transaction.operationList.add(new FuzzAddColumnOperation(newColName, newType, indexFlag, indexValueBlockCapacity, symbolTableStatic));
        transaction.structureVersion = metadataVersion;
        transaction.waitBarrierVersion = waitBarrierVersion;
        transactionList.add(transaction);

        FuzzTestColumnMeta newMeta = new FuzzTestColumnMeta();
        GenericRecordMetadata.copyColumns(tableMetadata, newMeta);
        newMeta.add(new TableColumnMetadata(
                newColName,
                newType,
                indexFlag,
                indexValueBlockCapacity,
                symbolTableStatic,
                null,
                newMeta.getColumnCount()
        ));
        newMeta.setTimestampIndex(tableMetadata.getTimestampIndex());
        return newMeta;
    }

    static void generateDataBlock(
            ObjList<FuzzTransaction> transactionList,
            Rnd rnd,
            int metadataVersion,
            int waitBarrierVersion,
            long startTs,
            long stopTs,
            int rowCount,
            boolean o3,
            double cancelRows,
            double notSet,
            double nullSet,
            double rollback,
            int strLen,
            String[] symbols,
            long seed,
            long transactionCount
    ) {
        FuzzTransaction transaction = new FuzzTransaction();
        long timestamp = startTs;
        final long delta = stopTs - startTs;
        for (int i = 0; i < rowCount; i++) {
            if (o3) {
                timestamp = ((startTs + rnd.nextLong(delta)) / transactionCount) * transactionCount + i;
            } else {
                timestamp = timestamp + delta / rowCount;
            }
            // Use stable random seeds which depends on the transaction index and timestamp
            // This will generate same row for same timestamp so that tests will not fail on reordering within same timestamp
            long seed1 = seed + timestamp;
            long seed2 = timestamp;
            transaction.operationList.add(new FuzzInsertOperation(seed1, seed2, timestamp, notSet, nullSet, cancelRows, strLen, symbols));
        }

        transaction.rollback = rnd.nextDouble() < rollback;
        transaction.structureVersion = metadataVersion;
        transaction.waitBarrierVersion = waitBarrierVersion;
        transactionList.add(transaction);
    }

    static RecordMetadata generateDropColumn(
            ObjList<FuzzTransaction> transactionList,
            int metadataVersion,
            int waitBarrierVersion,
            Rnd rnd,
            RecordMetadata tableMetadata
    ) {
        FuzzTransaction transaction = new FuzzTransaction();
        int startColumnIndex = rnd.nextInt(tableMetadata.getColumnCount());
        for (int i = 0; i < tableMetadata.getColumnCount(); i++) {
            int columnIndex = (startColumnIndex + i) % tableMetadata.getColumnCount();

            int type = tableMetadata.getColumnType(columnIndex);
            if (type > 0 && columnIndex != tableMetadata.getTimestampIndex()) {
                String columnName = tableMetadata.getColumnName(columnIndex);
                transaction.operationList.add(new FuzzDropColumnOperation(columnName));
                transaction.structureVersion = metadataVersion;
                transaction.waitBarrierVersion = waitBarrierVersion;
                transactionList.add(transaction);
                FuzzTestColumnMeta newMeta = new FuzzTestColumnMeta();
                copyColumnsExcept(tableMetadata, newMeta, columnIndex);
                return newMeta;
            }
        }

        // nothing to drop, only timestamp column left
        return null;
    }
}
