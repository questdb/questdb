/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.fuzz;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import org.junit.Assert;

import static io.questdb.std.datetime.microtime.Micros.DAY_MICROS;

public class FuzzTransactionGenerator {
    private static final int MAX_COLUMNS = 200;

    public static ObjList<FuzzTransaction> generateSet(
            long initialRowCount,
            TableRecordMetadata sequencerMetadata,
            TableMetadata tableMetadata,
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
            double probabilityOfColumnTypeChange,
            double probabilityOfDataInsert,
            double probabilityOfSameTimestamp,
            double probabilityOfDropPartition,
            double probabilityOfTruncate,
            double probabilityOfDropTable,
            double probabilityOfSetTtl,
            double replaceInsertProb,
            double probabilityOfSymbolAccessValidation,
            int maxStrLenForStrColumns,
            String[] symbols,
            int metaVersion
    ) {
        ObjList<FuzzTransaction> transactionList = new ObjList<>();
        int waitBarrierVersion = 0;
        RecordMetadata meta = deepMetadataCopyOf(sequencerMetadata, tableMetadata);

        long lastTimestamp = minTimestamp;

        double sumOfProbabilities = probabilityOfAddingNewColumn
                + probabilityOfRemovingColumn
                + probabilityOfRenamingColumn
                + probabilityOfColumnTypeChange
                + probabilityOfTruncate
                + probabilityOfDropPartition
                + probabilityOfDataInsert
                + probabilityOfSymbolAccessValidation;
        probabilityOfAddingNewColumn = probabilityOfAddingNewColumn / sumOfProbabilities;
        probabilityOfRemovingColumn = probabilityOfRemovingColumn / sumOfProbabilities;
        probabilityOfRenamingColumn = probabilityOfRenamingColumn / sumOfProbabilities;
        probabilityOfColumnTypeChange = probabilityOfColumnTypeChange / sumOfProbabilities;
        probabilityOfTruncate = probabilityOfTruncate / sumOfProbabilities;
        probabilityOfDropPartition = probabilityOfDropPartition / sumOfProbabilities;
        probabilityOfSymbolAccessValidation = probabilityOfSymbolAccessValidation / sumOfProbabilities;
        // effectively, probabilityOfDataInsert is as follows, but we don't need this value:
        // probabilityOfDataInsert = probabilityOfDataInsert / sumOfProbabilities;

        // To prevent long loops of cancelling rows, limit max probability of cancelling rows
        probabilityOfCancelRow = Math.min(probabilityOfCancelRow, 0.3);

        // Reduce some random parameters if there is too much data so test can finish in reasonable time
        transactionCount = Math.max(Math.min(transactionCount, 1_500_000 / rowCount), 3);

        // Decide if drop will be generated
        boolean generateTableDrop = rnd.nextDouble() < probabilityOfDropTable;
        int tableDropIteration = generateTableDrop ? rnd.nextInt(transactionCount) : -1;
        if (generateTableDrop) {
            transactionCount++;
        }

        // Decide if TTL will be set
        boolean generateSetTtl = rnd.nextDouble() < probabilityOfSetTtl;
        int setTtlIteration = generateSetTtl ? rnd.nextInt(transactionCount) : -1;
        if (generateSetTtl) {
            transactionCount++;
        }

        long estimatedTotalRows = rowCount + initialRowCount;

        for (int i = 0; i < transactionCount; i++) {
            if (i == tableDropIteration) {
                generateTableDropCreate(transactionList, metaVersion, waitBarrierVersion++);
                metaVersion = 0;
                continue;
            }
            if (i == setTtlIteration) {
                generateSetTtl(transactionList, metaVersion, waitBarrierVersion++, rnd);
                continue;
            }

            final double rndDouble = rnd.nextDouble();
            double aggregateProbability = 0;
            boolean wantSomething = false;

            aggregateProbability += probabilityOfAddingNewColumn;
            boolean wantToAddNewColumn = rndDouble < aggregateProbability;
            wantSomething |= wantToAddNewColumn;

            aggregateProbability += probabilityOfRemovingColumn;
            boolean wantToRemoveColumn = !wantSomething && rndDouble < aggregateProbability;
            wantSomething |= wantToRemoveColumn;

            aggregateProbability += probabilityOfRenamingColumn;
            boolean wantToRenameColumn = !wantSomething && rndDouble < aggregateProbability;
            wantSomething |= wantToRenameColumn;

            aggregateProbability += probabilityOfColumnTypeChange;
            boolean wantToChangeColumnType = !wantSomething && rndDouble < aggregateProbability;
            wantSomething |= wantToChangeColumnType;

            aggregateProbability += probabilityOfTruncate;
            boolean wantToTruncateTable = !wantSomething && rndDouble < aggregateProbability;
            wantSomething |= wantToTruncateTable;

            aggregateProbability += probabilityOfSymbolAccessValidation;
            boolean wantToValidateSymbolAccess = !wantSomething && rndDouble < aggregateProbability;
            wantSomething |= wantToValidateSymbolAccess;

            aggregateProbability += probabilityOfDropPartition;
            boolean wantToDropPartition = !wantSomething && rndDouble < aggregateProbability;

            Assert.assertNotNull(meta);

            if (wantToRemoveColumn) {
                RecordMetadata newTableMetadata = generateDropColumn(transactionList, metaVersion, waitBarrierVersion, rnd, meta);
                if (newTableMetadata != null) {
                    // Sometimes there is nothing to remove
                    metaVersion++;
                    waitBarrierVersion++;
                    meta = newTableMetadata;
                }
            } else if (wantToRenameColumn) {
                RecordMetadata newTableMetadata = generateRenameColumn(transactionList, metaVersion, waitBarrierVersion, rnd, meta);
                if (newTableMetadata != null) {
                    // Sometimes there is nothing to rename
                    metaVersion++;
                    waitBarrierVersion++;
                    meta = newTableMetadata;
                }
            } else if (wantToTruncateTable) {
                generateTruncateTable(transactionList, metaVersion, waitBarrierVersion++);
            } else if (wantToDropPartition) {
                generateDropPartition(transactionList, metaVersion, waitBarrierVersion++, lastTimestamp, rnd);
            } else if (wantToAddNewColumn && getNonDeletedColumnCount(meta) < MAX_COLUMNS) {
                meta = generateAddColumn(transactionList, metaVersion++, waitBarrierVersion++, rnd, meta);
            } else if (wantToChangeColumnType && FuzzChangeColumnTypeOperation.canChangeColumnType(meta)) {
                // 20% chance to change symbol capacity
                if (rnd.nextInt(5) != 0 ||
                        !FuzzChangeSymbolCapacityOperation.generateSymbolCapacityChange(
                                transactionList,
                                metaVersion,
                                waitBarrierVersion,
                                rnd,
                                meta
                        )) {
                    // If symbol capacity change was not generated, generate column type change
                    meta = FuzzChangeColumnTypeOperation.generateColumnTypeChange(transactionList, estimatedTotalRows, metaVersion++, waitBarrierVersion++, rnd, meta);
                }
            } else if (wantToValidateSymbolAccess) {
                FuzzTransaction transaction = new FuzzTransaction();
                transaction.operationList.add(new FuzzValidateSymbolFilterOperation(symbols));
                transaction.structureVersion = metaVersion;
                transaction.waitBarrierVersion = waitBarrierVersion;
                transactionList.add(transaction);
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
                    //noinspection lossy-conversions
                    size *= rnd.nextDouble();
                }
                stopTs = Math.min(startTs + size, maxTimestamp);

                // Replace commits with TTL may result in partition drop, if the data is deleted from WAL table at the end
                // it'll be the reason to drop prior partitions because of the TTL.
                // Non-wal tables don't have the replaced data inserted, and they will not drop partitions
                // because of TTL, and it will generate expected vs actual difference.
                // The workaround is to not generate replace inserts on the tables with TTL set.
                boolean replaceInsert = rnd.nextDouble() < replaceInsertProb && (setTtlIteration < 0 || i < setTtlIteration);
                waitBarrierVersion = generateDataBlock(
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
                        probabilityOfSameTimestamp,
                        replaceInsert
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

    private static GenericRecordMetadata deepMetadataCopyOf(TableRecordMetadata sequencerMetadata, TableMetadata tableMetadata) {
        if (sequencerMetadata != null && tableMetadata != null) {
            GenericRecordMetadata metadata = new GenericRecordMetadata();
            for (int i = 0, n = sequencerMetadata.getColumnCount(); i < n; i++) {
                metadata.add(
                        new TableColumnMetadata(
                                sequencerMetadata.getColumnName(i),
                                sequencerMetadata.getColumnType(i),
                                sequencerMetadata.isColumnIndexed(i),
                                sequencerMetadata.getIndexValueBlockCapacity(i),
                                sequencerMetadata.isSymbolTableStatic(i),
                                sequencerMetadata.getMetadata(i),
                                sequencerMetadata.getWriterIndex(i),
                                tableMetadata.getColumnCount() > i && tableMetadata.isDedupKey(i)
                        )
                );
            }
            metadata.setTimestampIndex(sequencerMetadata.getTimestampIndex());
            return metadata;
        }
        return null;
    }

    private static void generateDropPartition(
            ObjList<FuzzTransaction> transactionList, int metadataVersion, int waitBarrierVersion,
            long lastTimestamp, Rnd rnd
    ) {
        long cutoffTimestamp = lastTimestamp;
        if (rnd.nextInt(100) <= 20) {
            cutoffTimestamp -= DAY_MICROS;
        }
        FuzzTransaction transaction = new FuzzTransaction();
        transaction.operationList.add(new FuzzDropPartitionOperation(cutoffTimestamp));
        transaction.waitBarrierVersion = waitBarrierVersion;
        transaction.structureVersion = metadataVersion;
        transaction.waitAllDone = true;
        transactionList.add(transaction);
    }

    private static int generateNewColumnType(Rnd rnd) {
        int columnType = FuzzInsertOperation.SUPPORTED_COLUMN_TYPES[rnd.nextInt(FuzzInsertOperation.SUPPORTED_COLUMN_TYPES.length)];
        switch (columnType) {
            case ColumnType.GEOBYTE:
                return ColumnType.getGeoHashTypeWithBits(5);
            case ColumnType.GEOSHORT:
                return ColumnType.getGeoHashTypeWithBits(10);
            case ColumnType.GEOINT:
                return ColumnType.getGeoHashTypeWithBits(25);
            case ColumnType.GEOLONG:
                return ColumnType.getGeoHashTypeWithBits(35);
            case ColumnType.ARRAY:
                return ColumnType.encodeArrayType(ColumnType.DOUBLE, 2);
            default:
                return columnType;
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

                transaction.operationList.add(new FuzzRenameColumnOperation(rnd, columnName, newColName));
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

    private static void generateSetTtl(ObjList<FuzzTransaction> transactionList, int metadataVersion, int waitBarrierVersion, Rnd rnd) {
        int ttlDays = rnd.nextInt(2) + 1;
        FuzzTransaction transaction = new FuzzTransaction();
        transaction.waitBarrierVersion = waitBarrierVersion;
        transaction.structureVersion = metadataVersion;
        transaction.waitAllDone = true;
        transaction.reopenTable = true;
        transaction.operationList.add(new FuzzSetTtlOperation(ttlDays));
        transactionList.add(transaction);
    }

    private static void generateTableDropCreate(ObjList<FuzzTransaction> transactionList, int metadataVersion, int waitBarrierVersion) {
        FuzzTransaction transaction = new FuzzTransaction();
        transaction.waitBarrierVersion = waitBarrierVersion;
        transaction.structureVersion = metadataVersion;
        transaction.waitAllDone = true;
        transaction.reopenTable = true;
        transaction.operationList.add(new FuzzDropCreateTableOperation());
        transactionList.add(transaction);
    }

    private static void generateTruncateTable(ObjList<FuzzTransaction> transactionList, int metadataVersion, int waitBarrierVersion) {
        FuzzTransaction transaction = new FuzzTransaction();
        transaction.operationList.add(new FuzzTruncateTableOperation());
        transactionList.add(transaction);
        transaction.structureVersion = metadataVersion;
        transaction.waitBarrierVersion = waitBarrierVersion;
        transaction.waitAllDone = true;
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
        boolean indexFlag = newType == ColumnType.SYMBOL && rnd.nextDouble() < 0.9;
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
                newMeta.getColumnCount(),
                false
        ));
        newMeta.setTimestampIndex(tableMetadata.getTimestampIndex());
        return newMeta;
    }

    static int generateDataBlock(
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
            double probabilityOfRowsSameTimestamp,
            boolean replaceInsert
    ) {
        FuzzTransaction transaction = new FuzzTransaction();
        long timestamp = startTs;
        final long delta = stopTs - startTs;
        long minTs = Long.MAX_VALUE;
        long maxTs = Long.MIN_VALUE;

        for (int i = 0; i < rowCount; i++) {
            // Don't change timestamp sometimes with probabilityOfRowsSameTimestamp
            if (rnd.nextDouble() >= probabilityOfRowsSameTimestamp) {
                if (o3) {
                    timestamp = startTs + rnd.nextLong(delta) + i;
                } else {
                    timestamp = timestamp + delta / rowCount;
                }
            }
            long seed1 = rnd.nextLong();
            long seed2 = rnd.nextLong();
            transaction.operationList.add(new FuzzInsertOperation(seed1, seed2, timestamp, notSet, nullSet, cancelRows, strLen, symbols));
            minTs = Math.min(minTs, timestamp);
            maxTs = Math.max(maxTs, timestamp);
        }

        transaction.rollback = rnd.nextDouble() < rollback;
        transaction.structureVersion = metadataVersion;
        transaction.waitBarrierVersion = waitBarrierVersion;
        transactionList.add(transaction);

        if (replaceInsert) {
            minTs = minTs - (long) Math.exp(24);
            maxTs = maxTs + (long) Math.exp(24);
            if (!transaction.rollback) {
                // Add up to 2 partition to the range from each side to make things more interesting
                transaction.setReplaceRange(minTs, maxTs);

                return waitBarrierVersion + 1;
            } else if (rnd.nextBoolean()) {
                // Instead of rollback, insert empty replace range
                transaction.operationList.clear();
                transaction.rollback = false;
                transaction.setReplaceRange(minTs, maxTs);

                return waitBarrierVersion + 1;
            }
        }

        return waitBarrierVersion;
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
                transaction.operationList.add(new FuzzDropColumnOperation(rnd, columnName));
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
