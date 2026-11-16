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
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

public class FuzzTransactionGenerator {
    private static final int MAX_ANCHORS = 32;
    private static final int MAX_COLUMNS = 200;

    /**
     * Generates fuzz transactions. <code>minTimestamp</code> and <code>maxTimestamp</code>
     * are in the native unit of the table's designated timestamp (micros or nanos).
     */
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
            double probabilityOfSetTableFormat,
            double probabilityOfVacuum
    ) {
        return generateSet(
                initialRowCount,
                sequencerMetadata,
                tableMetadata,
                rnd,
                minTimestamp,
                maxTimestamp,
                rowCount,
                transactionCount,
                o3,
                probabilityOfCancelRow,
                probabilityOfUnassignedColumnValue,
                probabilityOfAssigningNull,
                probabilityOfTransactionRollback,
                probabilityOfAddingNewColumn,
                probabilityOfRemovingColumn,
                probabilityOfRenamingColumn,
                probabilityOfColumnTypeChange,
                probabilityOfDataInsert,
                probabilityOfSameTimestamp,
                probabilityOfDropPartition,
                probabilityOfConvertPartitionToParquet,
                probabilityOfConvertPartitionToNative,
                probabilityOfTruncate,
                probabilityOfDropTable,
                probabilityOfSetTtl,
                replaceInsertProb,
                probabilityOfSymbolAccessValidation,
                probabilityOfQuery,
                maxStrLenForStrColumns,
                symbols,
                metaVersion,
                probabilityOfSetParquetEncoding,
                probabilityOfAddCoveringIndex,
                0.0,
                probabilityOfSetTableFormat,
                probabilityOfVacuum,
                0.0,
                false
        );
    }

    /**
     * {@code isNoSentinelTypeChangeEnabled} makes the first column type change after the first CONVERT TO
     * PARQUET change a STRING, SYMBOL or VARCHAR column to BOOLEAN, BYTE or SHORT, lazily on the parquet
     * partition, and lets BOOLEAN, BYTE and SHORT columns change to text. Those types read their column
     * top as 0 or false; the change back to text shows whether CONVERT TO NATIVE and the ALTER COLUMN TYPE
     * pre-pass kept the top, as the non-WAL twin does.
     * <p>
     * That change back also shows every other place where the WAL tables and the twin hold the top rows
     * differently, so the generator keeps those out. A native O3 merge and an in-place parquet update
     * write the top rows of these types as 0 or false at points that differ between the tables: the
     * generator writes every row in order and converts to parquet only the partitions before the one taking
     * writes, and the caller applies the WAL with one writer, which keeps that order. A parquet to native
     * conversion stores the top of a text column from the start of its first row group with data, the
     * rows after it as NULL values, which a change to BOOLEAN, BYTE or SHORT turns into 0 or false: the
     * text to BOOLEAN, BYTE or SHORT change comes once, before any partition goes back to native.
     * <p>
     * A backfill is an O3 block clustered at a random point behind the run's front, cutting older days into splits
     * and pieces. A zero {@code probabilityOfBackfill} draws nothing from {@code rnd}, keeping old runs reproducible.
     */
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
            double probabilityOfDropIndex,
            double probabilityOfSetTableFormat,
            double probabilityOfVacuum,
            double probabilityOfBackfill,
            boolean isNoSentinelTypeChangeEnabled
    ) {
        return generateSet(
                initialRowCount,
                sequencerMetadata,
                tableMetadata,
                rnd,
                minTimestamp,
                maxTimestamp,
                rowCount,
                transactionCount,
                o3,
                probabilityOfCancelRow,
                probabilityOfUnassignedColumnValue,
                probabilityOfAssigningNull,
                probabilityOfTransactionRollback,
                probabilityOfAddingNewColumn,
                probabilityOfRemovingColumn,
                probabilityOfRenamingColumn,
                probabilityOfColumnTypeChange,
                probabilityOfDataInsert,
                probabilityOfSameTimestamp,
                probabilityOfDropPartition,
                probabilityOfConvertPartitionToParquet,
                probabilityOfConvertPartitionToNative,
                probabilityOfTruncate,
                probabilityOfDropTable,
                probabilityOfSetTtl,
                replaceInsertProb,
                probabilityOfSymbolAccessValidation,
                probabilityOfQuery,
                maxStrLenForStrColumns,
                symbols,
                metaVersion,
                probabilityOfSetParquetEncoding,
                probabilityOfAddCoveringIndex,
                probabilityOfDropIndex,
                probabilityOfSetTableFormat,
                probabilityOfVacuum,
                probabilityOfBackfill,
                isNoSentinelTypeChangeEnabled,
                false,
                null
        );
    }

    /**
     * {@link #generateSet} that, with {@code isNoSentinelWideningEnabled}, changes BYTE and SHORT columns to types with
     * NULL, see {@link FuzzChangeColumnTypeOperation#generateNoSentinelWidening}, and adds their names to
     * {@code widenedColumns}. A quarter into the run it converts every day before the one taking writes to native and
     * adds a BYTE or SHORT column, whose column top covers the rows written so far. The rows written later hold values,
     * the late rows that cut older days into splits and pieces included, and no CONVERT PARTITION runs until, three
     * quarters into the run, it converts every day before the one taking writes to parquet and widens a BYTE or SHORT
     * column: a converted day can hold the values of one folder or piece ahead of the column top of the next in one
     * row group, and the WAL tables apply the change lazily on it. After that half of the column type changes widen a
     * BYTE or SHORT column, and no other change alters a column of these types.
     * <p>
     * Only the WAL tables convert partitions, and their merges cover other rows than the twin's commits, so the
     * non-WAL twin and the WAL tables can disagree on which rows of these columns are a column top and which hold a
     * stored 0: the caller compares NULL and the converted 0 of a widened column as one value, see
     * FuzzRunner.setNoSentinelWideningChecked().
     */
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
            double probabilityOfDropIndex,
            double probabilityOfSetTableFormat,
            double probabilityOfVacuum,
            double probabilityOfBackfill,
            boolean isNoSentinelTypeChangeEnabled,
            boolean isNoSentinelWideningEnabled,
            @Nullable ObjHashSet<String> widenedColumns
    ) {
        Assert.assertFalse("no-sentinel type changes need in-order writes", isNoSentinelTypeChangeEnabled && o3);
        ObjList<FuzzTransaction> transactionList = new ObjList<>();
        int waitBarrierVersion = 0;
        RecordMetadata meta = deepMetadataCopyOf(sequencerMetadata, tableMetadata);
        final TimestampDriver timestampDriver = ColumnType.getTimestampDriver(
                sequencerMetadata != null ? sequencerMetadata.getTimestampType() : ColumnType.TIMESTAMP_MICRO
        );
        final long oneDay = timestampDriver.fromDays(1);
        // Replace range is widened on both sides to cover up to 2 partitions more
        final long replaceRangePadding = timestampDriver.fromMicros((long) Math.exp(24));

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
                + probabilityOfDropIndex
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
        probabilityOfDropIndex = probabilityOfDropIndex / sumOfProbabilities;
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
        // see the javadoc: the first type change after the first CONVERT TO PARQUET changes text to BOOLEAN,
        // BYTE or SHORT, unless a CONVERT TO NATIVE comes first
        boolean isTextToNoSentinelChangeDue = false;
        boolean isTextToNoSentinelChangeSpent = !isNoSentinelTypeChangeEnabled;
        // see the javadoc: a quarter into the run the generator adds a BYTE or SHORT column, and three quarters into the
        // run it widens it, with no CONVERT PARTITION between the two
        Assert.assertTrue("widening needs a sink for the widened columns", !isNoSentinelWideningEnabled || widenedColumns != null);
        if (isNoSentinelWideningEnabled) {
            // the two take iterations of their own
            transactionCount += 2;
        }
        final int noSentinelColumnIteration = isNoSentinelWideningEnabled ? transactionCount / 4 : Integer.MAX_VALUE;
        final int noSentinelWideningIteration = isNoSentinelWideningEnabled ? transactionCount * 3 / 4 : Integer.MAX_VALUE;
        boolean isNoSentinelColumnAdded = !isNoSentinelWideningEnabled;
        boolean isNoSentinelWideningSpent = !isNoSentinelWideningEnabled;
        // the column added a quarter into the run, which the widening three quarters into the run changes
        String noSentinelColumnName = null;

        // Anchor some O3 blocks near earlier timestamps to exercise collisions and adjacent writes.
        final double probabilityOfAnchoredBlock = o3 && rnd.nextBoolean() ? 0.3 * rnd.nextDouble() : 0;
        final LongList anchors = new LongList();
        long lastAnchor = Long.MIN_VALUE;
        long pendingCollisionTs = Long.MIN_VALUE;

        for (int i = 0; i < transactionCount; i++) {
            if (probabilityOfVacuum > 0 && rnd.nextDouble() < probabilityOfVacuum) {
                FuzzTransaction transaction = new FuzzTransaction();
                transaction.operationList.add(new FuzzVacuumOperation());
                transaction.structureVersion = metaVersion;
                transaction.waitBarrierVersion = waitBarrierVersion;
                // VACUUM changes no rows or metadata and must not add a sequencer transaction.
                transaction.rollback = true;
                transactionList.add(transaction);
            }
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
            if (!isNoSentinelColumnAdded && i >= noSentinelColumnIteration && getNonDeletedColumnCount(meta) < MAX_COLUMNS) {
                // the days go back to native, so that the late rows after the column cut them into splits and
                // pieces; its column top covers the rows written so far, and later rows hold values
                isNoSentinelColumnAdded = true;
                waitBarrierVersion = generateConvertEveryDay(transactionList, metaVersion, waitBarrierVersion, minTimestamp, lastTimestamp, oneDay, false);
                meta = generateAddColumn(transactionList, metaVersion++, waitBarrierVersion++, rnd, meta, rnd.nextBoolean() ? ColumnType.BYTE : ColumnType.SHORT);
                noSentinelColumnName = meta.getColumnName(meta.getColumnCount() - 1);
                continue;
            }
            if (!isNoSentinelWideningSpent && isNoSentinelColumnAdded && i >= noSentinelWideningIteration) {
                isNoSentinelWideningSpent = true;
                // the days the late rows cut into splits and pieces go to parquet, which the widening then reads
                waitBarrierVersion = generateConvertEveryDay(transactionList, metaVersion, waitBarrierVersion, minTimestamp, lastTimestamp, oneDay, true);
                final RecordMetadata wideningMeta = FuzzChangeColumnTypeOperation.generateNoSentinelWidening(
                        transactionList,
                        estimatedTotalRows,
                        metaVersion,
                        waitBarrierVersion,
                        rnd,
                        meta,
                        widenedColumns,
                        noSentinelColumnName
                );
                if (wideningMeta != null) {
                    meta = wideningMeta;
                    metaVersion++;
                    waitBarrierVersion++;
                    continue;
                }
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
            wantSomething |= wantToAddCoveringIndex;

            aggregateProbability += probabilityOfDropIndex;
            boolean wantToDropIndex = !wantSomething && rndDouble < aggregateProbability;

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
                generateDropPartition(transactionList, metaVersion, waitBarrierVersion++, lastTimestamp, oneDay, rnd);
            } else if (wantToConvertPartitionToParquet && (!isNoSentinelColumnAdded || isNoSentinelWideningSpent)) {
                // between the added column and the widening the days stay native, see the javadoc
                final long cutoffTimestamp = generateConvertPartitionToParquet(
                        transactionList,
                        metaVersion,
                        waitBarrierVersion++,
                        minTimestamp,
                        lastTimestamp,
                        oneDay,
                        rnd,
                        isNoSentinelTypeChangeEnabled
                );
                // the partition of the cutoff has rows when its day is not before the first day of data
                isTextToNoSentinelChangeDue |= !isTextToNoSentinelChangeSpent
                        && cutoffTimestamp - cutoffTimestamp % oneDay >= minTimestamp - minTimestamp % oneDay;
            } else if (wantToConvertPartitionToNative && !isTextToNoSentinelChangeDue && (!isNoSentinelColumnAdded || isNoSentinelWideningSpent)) {
                // waits for the due text to BOOLEAN, BYTE or SHORT change, and for the widening after the added column
                generateConvertPartitionToNative(transactionList, metaVersion, waitBarrierVersion++, minTimestamp, lastTimestamp, oneDay, rnd);
            } else if (wantToAddCoveringIndex) {
                if (generateAddCoveringIndex(transactionList, metaVersion, waitBarrierVersion, rnd, meta)) {
                    waitBarrierVersion++;
                }
            } else if (wantToDropIndex) {
                generateDropIndex(transactionList, metaVersion, waitBarrierVersion, rnd, meta);
            } else if (wantToAddNewColumn && getNonDeletedColumnCount(meta) < MAX_COLUMNS) {
                meta = generateAddColumn(transactionList, metaVersion++, waitBarrierVersion++, rnd, meta);
            } else if (wantToChangeColumnType && FuzzChangeColumnTypeOperation.canChangeColumnType(meta)) {
                RecordMetadata textToNoSentinelMeta = null;
                if (isTextToNoSentinelChangeDue) {
                    isTextToNoSentinelChangeDue = false;
                    isTextToNoSentinelChangeSpent = true;
                    textToNoSentinelMeta = FuzzChangeColumnTypeOperation.generateTextToNoSentinelChange(transactionList, metaVersion, waitBarrierVersion, rnd, meta);
                }
                // after the due widening, half of the type changes widen a BYTE or SHORT column, if there is one
                final RecordMetadata wideningMeta = textToNoSentinelMeta == null && isNoSentinelWideningSpent && isNoSentinelWideningEnabled && rnd.nextBoolean()
                        ? FuzzChangeColumnTypeOperation.generateNoSentinelWidening(transactionList, estimatedTotalRows, metaVersion, waitBarrierVersion, rnd, meta, widenedColumns)
                        : null;
                if (textToNoSentinelMeta != null || wideningMeta != null) {
                    meta = textToNoSentinelMeta != null ? textToNoSentinelMeta : wideningMeta;
                    metaVersion++;
                    waitBarrierVersion++;
                } else if (rnd.nextInt(5) != 0 ||
                        !FuzzChangeSymbolCapacityOperation.generateSymbolCapacityChange(
                                transactionList,
                                metaVersion,
                                waitBarrierVersion,
                                rnd,
                                meta
                        )) {
                    // a symbol capacity change takes 20% of the others; if it was not generated, generate a
                    // column type change
                    if (isNoSentinelWideningEnabled) {
                        // only a widening changes a column without a NULL sentinel, see the javadoc
                        final RecordMetadata changedMeta = FuzzChangeColumnTypeOperation.generateColumnTypeChange(transactionList, estimatedTotalRows, metaVersion, waitBarrierVersion, rnd, meta, true, false);
                        if (changedMeta != null) {
                            meta = changedMeta;
                            metaVersion++;
                            waitBarrierVersion++;
                        }
                    } else {
                        meta = FuzzChangeColumnTypeOperation.generateColumnTypeChange(transactionList, estimatedTotalRows, metaVersion++, waitBarrierVersion++, rnd, meta, false, isNoSentinelTypeChangeEnabled);
                    }
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
                final boolean isCollision = pendingCollisionTs != Long.MIN_VALUE;
                final boolean isAnchored = isCollision || (probabilityOfAnchoredBlock > 0 && anchors.size() > 0 && rnd.nextDouble() < probabilityOfAnchoredBlock);
                final boolean isBackfill = !isAnchored
                        && o3
                        && probabilityOfBackfill > 0
                        && lastTimestamp > minTimestamp
                        && rnd.nextDouble() < probabilityOfBackfill;
                if (isCollision) {
                    // generate new rows at the previous anchor's timestamp
                    startTs = pendingCollisionTs;
                    stopTs = pendingCollisionTs;
                    pendingCollisionTs = Long.MIN_VALUE;
                    blockRows = Math.min(blockRows, 1 + rnd.nextInt(16));
                } else if (isAnchored) {
                    // revisiting the previous anchor clusters late writes around one timestamp
                    if (lastAnchor == Long.MIN_VALUE || rnd.nextBoolean()) {
                        lastAnchor = anchors.getQuick(rnd.nextInt(anchors.size()));
                    }
                    if (rnd.nextBoolean()) {
                        // write just above the anchor, then revisit its timestamp in the next data block
                        startTs = lastAnchor + 1;
                        pendingCollisionTs = lastAnchor;
                    } else {
                        // just below, exactly at, or just above the anchor
                        startTs = lastAnchor + rnd.nextInt(3) - 1;
                    }
                    final long width = switch (rnd.nextInt(3)) {
                        case 0 -> 0; // every row at the same timestamp
                        case 1 -> rnd.nextLong(1000);
                        default -> (long) ((maxTimestamp - minTimestamp) / transactionCount * rnd.nextDouble());
                    };
                    stopTs = Math.max(startTs, Math.min(startTs + width, maxTimestamp));
                    // a small block keeps the partition's existing prefix large enough to split
                    blockRows = Math.min(blockRows, 1 + rnd.nextInt(16));
                } else {
                    if (isBackfill) {
                        startTs = minTimestamp + rnd.nextLong(lastTimestamp - minTimestamp);
                    } else if (o3) {
                        long offset = (long) ((maxTimestamp - minTimestamp + 1.0) / transactionCount * i);
                        long jitter = (long) ((maxTimestamp - minTimestamp + 1.0) / transactionCount * rnd.nextDouble());
                        if (rnd.nextBoolean()) {
                            startTs = (minTimestamp + offset + jitter);
                        } else {
                            startTs = (minTimestamp + offset - jitter);
                        }
                    } else {
                        long writeInterval = rnd.nextLong((maxTimestamp - minTimestamp) / transactionCount);
                        startTs = isNoSentinelTypeChangeEnabled ? lastTimestamp : lastTimestamp - writeInterval;
                    }
                    long step = (maxTimestamp - minTimestamp) / transactionCount;
                    if (o3) {
                        //noinspection lossy-conversions
                        step *= rnd.nextDouble();
                    }
                    if (isBackfill) {
                        // A tenth of an ordinary block's width at most, so the cluster has existing rows on both sides.
                        step = Math.max(1, step / 10);
                    }
                    stopTs = Math.min(startTs + step, maxTimestamp);
                }

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
                        replaceRangePadding
                );
                rowCount -= blockRows;
                // Drop and convert partition operations use lastTimestamp as their cutoff. An anchored block
                // revisits older data, so it only moves lastTimestamp forward; a backfill does not move the front
                // the next blocks are placed against.
                if (!isBackfill) {
                    lastTimestamp = isAnchored ? Math.max(lastTimestamp, stopTs) : stopTs;
                }
                if (probabilityOfAnchoredBlock > 0) {
                    addAnchors(transactionList.getLast(), anchors);
                }
            }
        }

        return transactionList;
    }

    // keeps the min and max row timestamps of the most recent committed data blocks
    private static void addAnchors(FuzzTransaction transaction, LongList anchors) {
        if (transaction.rollback) {
            return;
        }
        long minTs = Long.MAX_VALUE;
        long maxTs = Long.MIN_VALUE;
        for (int i = 0, n = transaction.operationList.size(); i < n; i++) {
            if (transaction.operationList.getQuick(i) instanceof FuzzInsertOperation insert) {
                minTs = Math.min(minTs, insert.getTimestamp());
                maxTs = Math.max(maxTs, insert.getTimestamp());
            }
        }
        if (minTs > maxTs) {
            return;
        }
        if (anchors.size() >= MAX_ANCHORS) {
            anchors.removeIndexBlock(0, 2);
        }
        anchors.add(minTs);
        anchors.add(maxTs);
    }

    /**
     * Picks the active day 4 in 10, the day before it 3 in 10, else a random older day, so conversions keep
     * reaching days the O3 commits cut into splits and pieces.
     */
    private static long convertCutoffTimestamp(long minTimestamp, long lastTimestamp, long oneDay, Rnd rnd) {
        final int target = rnd.nextInt(100);
        if (target < 30) {
            return lastTimestamp - oneDay;
        }
        if (target < 60 && lastTimestamp - minTimestamp > oneDay) {
            return minTimestamp + oneDay + rnd.nextLong(lastTimestamp - minTimestamp - oneDay);
        }
        return lastTimestamp;
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

    private static boolean generateAddCoveringIndex(
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
            return false;
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
            return false;
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
        transaction.waitAllDone = true;
        transactionList.add(transaction);
        return true;
    }

    /**
     * Drops the index of a symbol column and, five times in eight, indexes it again with BITMAP or a POSTING
     * kind.
     * The operation checks the index of the column when it runs, as the generator metadata does not
     * follow the index changes.
     */
    private static void generateDropIndex(
            ObjList<FuzzTransaction> transactionList, int metadataVersion, int waitBarrierVersion,
            Rnd rnd, RecordMetadata meta
    ) {
        IntList symbolCols = new IntList();
        for (int i = 0, n = meta.getColumnCount(); i < n; i++) {
            int colType = meta.getColumnType(i);
            if (colType > 0 && ColumnType.isSymbol(colType)) {
                symbolCols.add(i);
            }
        }
        if (symbolCols.size() == 0) {
            return;
        }
        final int symCol = symbolCols.getQuick(rnd.nextInt(symbolCols.size()));
        final String newIndexTypeName = switch (rnd.nextInt(8)) {
            case 0, 1 -> "BITMAP";
            case 2 -> "POSTING";
            case 3 -> "POSTING DELTA";
            case 4 -> "POSTING EF";
            default -> null;
        };

        FuzzTransaction transaction = new FuzzTransaction();
        transaction.operationList.add(new FuzzDropIndexOperation(symCol, newIndexTypeName));
        transaction.structureVersion = metadataVersion;
        transaction.waitBarrierVersion = waitBarrierVersion;
        transaction.rollback = true;
        transactionList.add(transaction);
    }

    private static void generateConvertPartitionToNative(
            ObjList<FuzzTransaction> transactionList, int metadataVersion, int waitBarrierVersion,
            long minTimestamp, long lastTimestamp, long oneDay, Rnd rnd
    ) {
        FuzzTransaction transaction = new FuzzTransaction();
        transaction.operationList.add(new FuzzConvertPartitionToNativeOperation(convertCutoffTimestamp(minTimestamp, lastTimestamp, oneDay, rnd)));
        transaction.waitBarrierVersion = waitBarrierVersion;
        transaction.structureVersion = metadataVersion;
        transaction.waitAllDone = true;
        transactionList.add(transaction);
    }

    /**
     * Converts the day {@link #convertCutoffTimestamp} picks, or with {@code isBeforeLastPartition} the day before
     * the one taking writes, two days before it one time in five, and returns the cutoff timestamp.
     */
    private static long generateConvertPartitionToParquet(
            ObjList<FuzzTransaction> transactionList, int metadataVersion, int waitBarrierVersion,
            long minTimestamp, long lastTimestamp, long oneDay, Rnd rnd, boolean isBeforeLastPartition
    ) {
        final long cutoffTimestamp;
        if (isBeforeLastPartition) {
            // The WHERE clause matches partition timestamps in (cutoff - 1 day, cutoff), and the tables partition
            // by DAY: a cutoff just before the day of lastTimestamp selects the partition before the one taking writes
            final long dayBeforeLastPartition = lastTimestamp - lastTimestamp % oneDay - 1;
            cutoffTimestamp = rnd.nextInt(100) <= 20 ? dayBeforeLastPartition - oneDay : dayBeforeLastPartition;
        } else {
            cutoffTimestamp = convertCutoffTimestamp(minTimestamp, lastTimestamp, oneDay, rnd);
        }
        FuzzTransaction transaction = new FuzzTransaction();
        transaction.operationList.add(new FuzzConvertPartitionToParquetOperation(cutoffTimestamp));
        transaction.waitBarrierVersion = waitBarrierVersion;
        transaction.structureVersion = metadataVersion;
        transaction.waitAllDone = true;
        transactionList.add(transaction);
        return cutoffTimestamp;
    }


    /**
     * Converts every day before the one that {@code lastTimestamp} falls in, to parquet or to native, one transaction a
     * day, and returns the next wait barrier version.
     */
    private static int generateConvertEveryDay(
            ObjList<FuzzTransaction> transactionList,
            int metadataVersion,
            int waitBarrierVersion,
            long minTimestamp,
            long lastTimestamp,
            long oneDay,
            boolean isToParquet
    ) {
        for (long day = minTimestamp - minTimestamp % oneDay, frontDay = lastTimestamp - lastTimestamp % oneDay; day < frontDay; day += oneDay) {
            final FuzzTransaction transaction = new FuzzTransaction();
            // the operations convert the day that starts in (cutoff - 1 day, cutoff)
            final long cutoffTimestamp = day + oneDay - 1;
            transaction.operationList.add(isToParquet
                    ? new FuzzConvertPartitionToParquetOperation(cutoffTimestamp)
                    : new FuzzConvertPartitionToNativeOperation(cutoffTimestamp));
            transaction.waitBarrierVersion = waitBarrierVersion++;
            transaction.structureVersion = metadataVersion;
            transaction.waitAllDone = true;
            transactionList.add(transaction);
        }
        return waitBarrierVersion;
    }

    private static void generateDropPartition(
            ObjList<FuzzTransaction> transactionList, int metadataVersion, int waitBarrierVersion,
            long lastTimestamp, long oneDay, Rnd rnd
    ) {
        long cutoffTimestamp = lastTimestamp;
        if (rnd.nextInt(100) <= 20) {
            cutoffTimestamp -= oneDay;
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
        return generateAddColumn(transactionList, metadataVersion, waitBarrierVersion, rnd, tableMetadata, generateNewColumnType(rnd));
    }

    static RecordMetadata generateAddColumn(
            ObjList<FuzzTransaction> transactionList,
            int metadataVersion,
            int waitBarrierVersion,
            Rnd rnd,
            RecordMetadata tableMetadata,
            int newType
    ) {
        FuzzTransaction transaction = new FuzzTransaction();
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
            long replaceRangePadding
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

        if (replaceInsert) {
            minTs = minTs - replaceRangePadding;
            maxTs = maxTs + replaceRangePadding;
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
