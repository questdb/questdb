/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetEncoding;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import org.junit.Assert;

public class FuzzTransactionGenerator {
    private static final int MAX_COLUMNS = 200;

    /**
     * Generates a transaction list. {@code timestampDriver} is the unit of {@code minTimestamp},
     * {@code maxTimestamp} and every timestamp the list holds: a day, a replace window's
     * padding and the partition cutoffs scale with it.
     */
    public static ObjList<FuzzTransaction> generateSet(
            TimestampDriver timestampDriver,
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
            double probabilityOfConvertPartitionToParquet,
            double probabilityOfConvertPartitionToNative,
            double probabilityOfTruncate,
            double probabilityOfDropTable,
            double probabilityOfSetTtl,
            double replaceInsertProb,
            double probabilityOfSymbolAccessValidation,
            double probabilityOfQuery,
            int maxStrLenForStrColumns,
            String[] symbols,
            int metaVersion,
            double probabilityOfSetParquetEncoding,
            double probabilityOfAddCoveringIndex,
            double probabilityOfSetTableFormat
    ) {
        ObjList<FuzzTransaction> transactionList = new ObjList<>();
        int waitBarrierVersion = 0;
        RecordMetadata meta = deepMetadataCopyOf(sequencerMetadata, tableMetadata);
        final ReplaceWindowPicker replaceWindowPicker = new ReplaceWindowPicker(timestampDriver);

        long lastTimestamp = minTimestamp;

        double sumOfProbabilities = probabilityOfAddingNewColumn
                + probabilityOfRemovingColumn
                + probabilityOfRenamingColumn
                + probabilityOfColumnTypeChange
                + probabilityOfTruncate
                + probabilityOfDropPartition
                + probabilityOfConvertPartitionToParquet
                + probabilityOfConvertPartitionToNative
                + probabilityOfAddCoveringIndex
                + probabilityOfDataInsert
                + probabilityOfSymbolAccessValidation
                + probabilityOfQuery
                + probabilityOfSetTableFormat
                + probabilityOfSetParquetEncoding;
        probabilityOfAddingNewColumn = probabilityOfAddingNewColumn / sumOfProbabilities;
        probabilityOfRemovingColumn = probabilityOfRemovingColumn / sumOfProbabilities;
        probabilityOfRenamingColumn = probabilityOfRenamingColumn / sumOfProbabilities;
        probabilityOfColumnTypeChange = probabilityOfColumnTypeChange / sumOfProbabilities;
        probabilityOfTruncate = probabilityOfTruncate / sumOfProbabilities;
        probabilityOfDropPartition = probabilityOfDropPartition / sumOfProbabilities;
        probabilityOfConvertPartitionToParquet = probabilityOfConvertPartitionToParquet / sumOfProbabilities;
        probabilityOfConvertPartitionToNative = probabilityOfConvertPartitionToNative / sumOfProbabilities;
        probabilityOfAddCoveringIndex = probabilityOfAddCoveringIndex / sumOfProbabilities;
        probabilityOfSymbolAccessValidation = probabilityOfSymbolAccessValidation / sumOfProbabilities;
        probabilityOfQuery = probabilityOfQuery / sumOfProbabilities;
        probabilityOfSetParquetEncoding = probabilityOfSetParquetEncoding / sumOfProbabilities;
        probabilityOfSetTableFormat = probabilityOfSetTableFormat / sumOfProbabilities;
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

        // Decide if SET PARQUET ENCODING will be generated
        boolean generateSetParquetEncoding = rnd.nextDouble() < probabilityOfSetParquetEncoding;
        int setParquetEncodingIteration = generateSetParquetEncoding ? rnd.nextInt(transactionCount) : -1;
        if (generateSetParquetEncoding) {
            transactionCount++;
        }

        // Decide if SET FORMAT PARQUET|NATIVE will be generated
        boolean generateSetTableFormat = rnd.nextDouble() < probabilityOfSetTableFormat;
        int setTableFormatIteration = generateSetTableFormat ? rnd.nextInt(transactionCount) : -1;
        if (generateSetTableFormat) {
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

            Assert.assertNotNull(meta);

            if (i == setParquetEncodingIteration) {
                if (generateSetParquetEncoding(transactionList, metaVersion, waitBarrierVersion, rnd, meta)) {
                    waitBarrierVersion++;
                }
                continue;
            }
            if (i == setTableFormatIteration) {
                generateSetTableFormat(transactionList, metaVersion, waitBarrierVersion++, rnd);
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

            aggregateProbability += probabilityOfQuery;
            boolean wantToQuery = !wantSomething && rndDouble < aggregateProbability;
            wantSomething |= wantToQuery;

            aggregateProbability += probabilityOfDropPartition;
            boolean wantToDropPartition = !wantSomething && rndDouble < aggregateProbability;
            wantSomething |= wantToDropPartition;

            aggregateProbability += probabilityOfConvertPartitionToParquet;
            boolean wantToConvertPartitionToParquet = !wantSomething && rndDouble < aggregateProbability;
            wantSomething |= wantToConvertPartitionToParquet;

            aggregateProbability += probabilityOfConvertPartitionToNative;
            boolean wantToConvertPartitionToNative = !wantSomething && rndDouble < aggregateProbability;
            wantSomething |= wantToConvertPartitionToNative;

            aggregateProbability += probabilityOfAddCoveringIndex;
            boolean wantToAddCoveringIndex = !wantSomething && rndDouble < aggregateProbability;

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
                generateDropPartition(transactionList, metaVersion, waitBarrierVersion++, lastTimestamp, timestampDriver, rnd);
            } else if (wantToConvertPartitionToParquet) {
                generateConvertPartitionToParquet(transactionList, metaVersion, waitBarrierVersion++, lastTimestamp, timestampDriver, rnd);
            } else if (wantToConvertPartitionToNative) {
                generateConvertPartitionToNative(transactionList, metaVersion, waitBarrierVersion++, lastTimestamp, timestampDriver, rnd);
            } else if (wantToAddCoveringIndex) {
                generateAddCoveringIndex(transactionList, metaVersion, waitBarrierVersion, rnd, meta);
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
                // Indicate that this operation will not add a seqTxn
                transaction.rollback = true;
                transactionList.add(transaction);
            } else if (wantToQuery) {
                FuzzTransaction transaction = new FuzzTransaction();
                final int limit = (rnd.nextBoolean() ? 1 : -1) * (1 + rnd.nextInt(1000));
                transaction.operationList.add(new FuzzQueryOperation(limit));
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
                long step = (maxTimestamp - minTimestamp) / transactionCount;
                if (o3) {
                    //noinspection lossy-conversions
                    step *= rnd.nextDouble();
                }
                stopTs = Math.min(startTs + step, maxTimestamp);

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
                        replaceInsert,
                        replaceWindowPicker
                );
                rowCount -= blockRows;
                lastTimestamp = stopTs;
            }
        }

        return transactionList;
    }

    /**
     * A timestamp in the driver's unit as a literal at the unit's full precision.
     */
    public static String toTimestampLiteral(TimestampDriver timestampDriver, long timestamp) {
        final StringSink sink = new StringSink();
        timestampDriver.append(sink, timestamp);
        return sink.toString();
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
                    from.getColumnIndexType(i),
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
                                sequencerMetadata.getColumnIndexType(i),
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

    private static void generateAddCoveringIndex(
            ObjList<FuzzTransaction> transactionList, int metadataVersion, int waitBarrierVersion,
            Rnd rnd, RecordMetadata meta
    ) {
        IntList symbolCols = new IntList();
        for (int i = 0, n = meta.getColumnCount(); i < n; i++) {
            int colType = meta.getColumnType(i);
            if (colType > 0 && ColumnType.isSymbol(colType) && !IndexType.isIndexed(meta.getColumnIndexType(i))) {
                symbolCols.add(i);
            }
        }
        if (symbolCols.size() == 0) {
            return;
        }
        int symCol = symbolCols.getQuick(rnd.nextInt(symbolCols.size()));

        IntList candidates = new IntList();
        int tsIdx = meta.getTimestampIndex();
        for (int i = 0, n = meta.getColumnCount(); i < n; i++) {
            int colType = meta.getColumnType(i);
            if (colType > 0 && i != symCol && i != tsIdx && ColumnType.tagOf(colType) != ColumnType.LONG128) {
                candidates.add(i);
            }
        }
        if (candidates.size() == 0) {
            return;
        }
        int includeCount = Math.min(1 + rnd.nextInt(3), candidates.size());
        IntList includeIndices = new IntList();
        for (int i = 0; i < includeCount; i++) {
            int pick = rnd.nextInt(candidates.size());
            includeIndices.add(candidates.getQuick(pick));
            candidates.removeIndex(pick);
        }

        // Spread the covering index across all three posting kinds. The row-id encoding rides on
        // the index type, and the covering decoder reads it back per kind, so indexing only with
        // plain POSTING left the delta and EF layouts unexercised by every covering fuzz check.
        final double indexKindPick = rnd.nextDouble();
        final String indexTypeName = indexKindPick < 0.6
                ? "POSTING"
                : (indexKindPick < 0.8 ? "POSTING DELTA" : "POSTING EF");

        FuzzTransaction transaction = new FuzzTransaction();
        transaction.operationList.add(new FuzzAddCoveringIndexOperation(symCol, includeIndices, indexTypeName));
        transaction.structureVersion = metadataVersion;
        transaction.waitBarrierVersion = waitBarrierVersion;
        transaction.rollback = true;
        transactionList.add(transaction);
    }

    private static void generateConvertPartitionToNative(
            ObjList<FuzzTransaction> transactionList, int metadataVersion, int waitBarrierVersion,
            long lastTimestamp, TimestampDriver timestampDriver, Rnd rnd
    ) {
        long cutoffTimestamp = lastTimestamp;
        if (rnd.nextInt(100) <= 20) {
            cutoffTimestamp -= timestampDriver.fromDays(1);
        }
        FuzzTransaction transaction = new FuzzTransaction();
        transaction.operationList.add(new FuzzConvertPartitionToNativeOperation(cutoffTimestamp, timestampDriver));
        transaction.waitBarrierVersion = waitBarrierVersion;
        transaction.structureVersion = metadataVersion;
        transaction.waitAllDone = true;
        transactionList.add(transaction);
    }

    private static void generateConvertPartitionToParquet(
            ObjList<FuzzTransaction> transactionList, int metadataVersion, int waitBarrierVersion,
            long lastTimestamp, TimestampDriver timestampDriver, Rnd rnd
    ) {
        long cutoffTimestamp = lastTimestamp;
        if (rnd.nextInt(100) <= 20) {
            cutoffTimestamp -= timestampDriver.fromDays(1);
        }
        FuzzTransaction transaction = new FuzzTransaction();
        transaction.operationList.add(new FuzzConvertPartitionToParquetOperation(cutoffTimestamp, timestampDriver));
        transaction.waitBarrierVersion = waitBarrierVersion;
        transaction.structureVersion = metadataVersion;
        transaction.waitAllDone = true;
        transactionList.add(transaction);
    }


    private static void generateDropPartition(
            ObjList<FuzzTransaction> transactionList, int metadataVersion, int waitBarrierVersion,
            long lastTimestamp, TimestampDriver timestampDriver, Rnd rnd
    ) {
        long cutoffTimestamp = lastTimestamp;
        if (rnd.nextInt(100) <= 20) {
            cutoffTimestamp -= timestampDriver.fromDays(1);
        }
        FuzzTransaction transaction = new FuzzTransaction();
        transaction.operationList.add(new FuzzDropPartitionOperation(cutoffTimestamp, timestampDriver));
        transaction.waitBarrierVersion = waitBarrierVersion;
        transaction.structureVersion = metadataVersion;
        transaction.waitAllDone = true;
        transactionList.add(transaction);
    }

    private static int generateNewColumnType(Rnd rnd) {
        int columnType = FuzzInsertOperation.SUPPORTED_COLUMN_TYPES[rnd.nextInt(FuzzInsertOperation.SUPPORTED_COLUMN_TYPES.length)];
        return switch (columnType) {
            case ColumnType.GEOBYTE -> ColumnType.getGeoHashTypeWithBits(5);
            case ColumnType.GEOSHORT -> ColumnType.getGeoHashTypeWithBits(10);
            case ColumnType.GEOINT -> ColumnType.getGeoHashTypeWithBits(25);
            case ColumnType.GEOLONG -> ColumnType.getGeoHashTypeWithBits(35);
            case ColumnType.ARRAY -> ColumnType.encodeArrayType(ColumnType.DOUBLE, 2);
            default -> columnType;
        };
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

    private static boolean generateSetParquetEncoding(
            ObjList<FuzzTransaction> transactionList,
            int metadataVersion,
            int waitBarrierVersion,
            Rnd rnd,
            RecordMetadata meta
    ) {
        // Pick a random non-timestamp column
        int colCount = meta.getColumnCount();
        int tsIndex = meta.getTimestampIndex();
        int startIndex = rnd.nextInt(colCount);
        for (int i = 0; i < colCount; i++) {
            int colIndex = (startIndex + i) % colCount;
            if (colIndex == tsIndex) {
                continue;
            }
            int colType = meta.getColumnType(colIndex);
            if (colType < 0) {
                continue;
            }
            String colName = meta.getColumnName(colIndex);

            // Pick a random valid encoding for this column type
            int encoding = 0;
            if (rnd.nextBoolean()) {
                int validCount = 0;
                for (int e = 1; e < ParquetEncoding.MAX_ENUM_INT; e++) {
                    if (ParquetEncoding.isValidForColumnType(e, colType)) {
                        validCount++;
                    }
                }
                if (validCount > 0) {
                    int pick = rnd.nextInt(validCount);
                    for (int e = 1; e < ParquetEncoding.MAX_ENUM_INT; e++) {
                        if (ParquetEncoding.isValidForColumnType(e, colType)) {
                            if (pick == 0) {
                                encoding = e;
                                break;
                            }
                            pick--;
                        }
                    }
                }
            }

            // Pick random compression
            int compression = -1;
            int level = 0;
            if (rnd.nextBoolean()) {
                int[] codecs = {
                        ParquetCompression.COMPRESSION_UNCOMPRESSED,
                        ParquetCompression.COMPRESSION_SNAPPY,
                        ParquetCompression.COMPRESSION_ZSTD,
                        ParquetCompression.COMPRESSION_LZ4_RAW
                };
                compression = codecs[rnd.nextInt(codecs.length)];
                if (compression == ParquetCompression.COMPRESSION_ZSTD) {
                    level = 1 + rnd.nextInt(ParquetCompression.ZSTD_MAX_COMPRESSION_LEVEL);
                }
            }

            FuzzTransaction transaction = new FuzzTransaction();
            transaction.waitBarrierVersion = waitBarrierVersion;
            transaction.structureVersion = metadataVersion;
            transaction.operationList.add(new FuzzSetParquetEncodingOperation(colName, encoding, compression, level));
            transactionList.add(transaction);
            return true;
        }
        return false;
    }

    private static void generateSetTableFormat(ObjList<FuzzTransaction> transactionList, int metadataVersion, int waitBarrierVersion, Rnd rnd) {
        boolean parquet = rnd.nextBoolean();
        FuzzTransaction transaction = new FuzzTransaction();
        transaction.waitBarrierVersion = waitBarrierVersion;
        transaction.structureVersion = metadataVersion;
        transaction.waitAllDone = true;
        transaction.reopenTable = true;
        transaction.operationList.add(new FuzzSetTableFormatOperation(parquet));
        transactionList.add(transaction);
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
        byte indexType;
        if (newType == ColumnType.SYMBOL && rnd.nextDouble() < 0.9) {
            // 50/50 split between BITMAP and POSTING variants so the fuzz
            // exercises both index families. POSTING further fans out across
            // the three encoding modes (adaptive, delta, EF) to cover all
            // bitpack code paths.
            double pick = rnd.nextDouble();
            if (pick < 0.5) {
                indexType = IndexType.BITMAP;
            } else if (pick < 0.7) {
                indexType = IndexType.POSTING;
            } else if (pick < 0.85) {
                indexType = IndexType.POSTING_DELTA;
            } else {
                indexType = IndexType.POSTING_EF;
            }
        } else {
            indexType = IndexType.NONE;
        }
        // CAPACITY is parser-rejected for POSTING but the metadata field
        // still must be >= 2 to pass _meta validation, so always pass 256.
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
        transaction.operationList.add(new FuzzAddColumnOperation(newColName, newType, indexType, indexValueBlockCapacity, symbolTableStatic));
        transaction.structureVersion = metadataVersion;
        transaction.waitBarrierVersion = waitBarrierVersion;
        transactionList.add(transaction);

        FuzzTestColumnMeta newMeta = new FuzzTestColumnMeta();
        GenericRecordMetadata.copyColumns(tableMetadata, newMeta);
        newMeta.add(new TableColumnMetadata(
                newColName,
                newType,
                indexType,
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
            boolean replaceInsert,
            ReplaceWindowPicker replaceWindowPicker
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
                    timestamp = delta > 0 ? startTs + rnd.nextLong(delta) + i : startTs;
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

        int nextWaitBarrierVersion = waitBarrierVersion;
        if (replaceInsert) {
            if (minTs > maxTs) {
                // no rows: anchor the window on the block's range
                minTs = startTs;
                maxTs = Math.max(startTs, stopTs);
            }
            if (!transaction.rollback) {
                replaceWindowPicker.pick(rnd, transaction, minTs, maxTs);
                nextWaitBarrierVersion++;
            } else if (rnd.nextBoolean()) {
                // Instead of rollback, insert empty replace range
                transaction.operationList.clear();
                transaction.rollback = false;
                replaceWindowPicker.pickWide(transaction, minTs, maxTs);
                nextWaitBarrierVersion++;
            }
        }
        if (!transaction.rollback) {
            replaceWindowPicker.addCommittedRows(transaction);
        }
        return nextWaitBarrierVersion;
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

    /**
     * Picks the range of a replace commit, in table timestamp units, one of:
     * <ul>
     * <li>a wide window, padded by about 7.4 hours on each side of the commit's rows;</li>
     * <li>edges on, or one unit next to, the nearest committed rows: {@code lo} on a row,
     * {@code hi - 1} on a row (replaced) or {@code hi} on a row (kept);</li>
     * <li>a narrow window around the commit's rows;</li>
     * <li>whole days, with or without the commit's rows;</li>
     * <li>no rows: a pure delete, or a narrow window on or next to one committed row.</li>
     * </ul>
     * The non-wide shapes reach the edge cases, partition removal and the no-op path. Every
     * window holds the rows the commit brings, as the WAL requires.
     */
    static final class ReplaceWindowPicker {
        private static final int MAX_TIMESTAMPS = 100_000;
        private final long day;
        private final long padding;
        // a sample of the timestamps committed so far
        private final LongList timestamps = new LongList();
        private boolean isSorted = true;
        private int nextOverwrite;

        ReplaceWindowPicker(TimestampDriver timestampDriver) {
            day = timestampDriver.fromDays(1);
            padding = timestampDriver.fromMicros((long) Math.exp(24));
        }

        void addCommittedRows(FuzzTransaction transaction) {
            for (int i = 0, n = transaction.operationList.size(); i < n; i++) {
                if (transaction.operationList.getQuick(i) instanceof FuzzInsertOperation insert) {
                    if (timestamps.size() < MAX_TIMESTAMPS) {
                        timestamps.add(insert.getTimestamp());
                    } else {
                        timestamps.setQuick(nextOverwrite, insert.getTimestamp());
                        nextOverwrite = (nextOverwrite + 1) % MAX_TIMESTAMPS;
                    }
                    isSorted = false;
                }
            }
        }

        void pick(Rnd rnd, FuzzTransaction transaction, long minTs, long maxTs) {
            final long lo;
            final long hi;
            boolean isDroppingRows = false;
            switch (rnd.nextInt(10)) {
                case 0, 1, 2, 3 -> {
                    pickWide(transaction, minTs, maxTs);
                    return;
                }
                case 4, 5 -> {
                    lo = snapLo(rnd, minTs);
                    hi = snapHi(rnd, maxTs);
                }
                case 6 -> {
                    final long margin = padding / 1024 + 1;
                    lo = minTs - rnd.nextLong(margin);
                    hi = maxTs + 1 + rnd.nextLong(margin);
                }
                case 7 -> {
                    lo = floorDay(minTs) - rnd.nextInt(2) * day;
                    hi = floorDay(maxTs) + (1 + rnd.nextInt(2)) * day;
                    isDroppingRows = rnd.nextBoolean();
                }
                case 8 -> {
                    lo = snapLo(rnd, minTs);
                    hi = snapHi(rnd, maxTs);
                    isDroppingRows = true;
                }
                default -> {
                    isDroppingRows = true;
                    final long anchor = timestamps.size() > 0 ? timestamps.getQuick(rnd.nextInt(timestamps.size())) : minTs;
                    if (rnd.nextInt(3) == 0) {
                        // next to the row, not on it
                        lo = anchor + 1;
                        hi = lo + 1 + rnd.nextLong(padding / 1024 + 1);
                    } else {
                        lo = anchor - rnd.nextLong(3);
                        hi = anchor + 1 + rnd.nextLong(3);
                    }
                }
            }
            if (isDroppingRows) {
                transaction.operationList.clear();
            }
            transaction.setReplaceRange(lo, hi);
        }

        void pickWide(FuzzTransaction transaction, long minTs, long maxTs) {
            transaction.setReplaceRange(minTs - padding, maxTs + padding);
        }

        private long floorDay(long ts) {
            return ts - Math.floorMod(ts, day);
        }

        private void ensureSorted() {
            if (!isSorted) {
                timestamps.sort();
                isSorted = true;
            }
        }

        // hi (exclusive) on the committed row at or after maxTs (the row stays), one past it
        // (the row is replaced) or two past it; always above maxTs
        private long snapHi(Rnd rnd, long maxTs) {
            ensureSorted();
            int low = 0;
            int high = timestamps.size();
            while (low < high) {
                final int mid = (low + high) >>> 1;
                if (timestamps.getQuick(mid) < maxTs) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            if (low == timestamps.size() || timestamps.getQuick(low) - maxTs > day) {
                return maxTs + 1 + rnd.nextLong(3);
            }
            return Math.max(timestamps.getQuick(low) + rnd.nextLong(3), maxTs + 1);
        }

        // lo on the committed row at or before minTs, or one unit either side of it; never
        // above minTs
        private long snapLo(Rnd rnd, long minTs) {
            ensureSorted();
            int low = 0;
            int high = timestamps.size();
            while (low < high) {
                final int mid = (low + high) >>> 1;
                if (timestamps.getQuick(mid) <= minTs) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            if (low == 0 || minTs - timestamps.getQuick(low - 1) > day) {
                return minTs - rnd.nextLong(3);
            }
            return Math.min(timestamps.getQuick(low - 1) + rnd.nextLong(3) - 1, minTs);
        }
    }
}
