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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.std.LongList;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

public class FuzzChangeColumnTypeOperation implements FuzzTransactionOperation {
    private static final long MAX_TABLE_ROWS_TO_CONVERT_TO_SYMBOL = 60_000;
    // the targets with a NULL that generateNoSentinelWidening() changes a BOOLEAN, BYTE or SHORT column to. A lazy
    // change on a parquet partition decodes a BYTE or SHORT column to INT, LONG and TIMESTAMP straight to the
    // target, through the definition levels, and every other change at the source width, through the column-top
    // rows that the decode records, see isDecodedAtSourceWidth()
    private static final int[] NO_SENTINEL_WIDENING_TARGETS = {
            ColumnType.INT, ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP_NANO,
            ColumnType.DATE, ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL
    };
    // CHAR stays out: random CHAR values include lone surrogates, and CONVERT TO PARQUET suspends a table
    // whose STRING holds one, as after a change from CHAR to STRING
    private static final int[] noSentinelColumnTypes = {
            ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.SHORT
    };
    // the changes generateNoSentinelWidening() made
    private static final AtomicInteger noSentinelWidenings = new AtomicInteger();
    private static final int[] numericConvertableColumnTypes = {
            ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG,
            ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.BOOLEAN,
            ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP_NANO,
            ColumnType.STRING, ColumnType.VARCHAR
    };
    private static final int[] textColumnTypes = {
            ColumnType.STRING, ColumnType.SYMBOL, ColumnType.VARCHAR
    };
    // the changes generateTextToNoSentinelChange() made
    private static final AtomicInteger textToNoSentinelChanges = new AtomicInteger();
    private static final int[] varSizeConvertableColumnTypes = {
            ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG,
            ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.BOOLEAN,
            ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP_NANO,
            ColumnType.UUID, ColumnType.IPv4,
            ColumnType.STRING, ColumnType.SYMBOL, ColumnType.VARCHAR
    };
    private final boolean cacheSymbolMap;
    private final String columName;
    private final byte indexType;
    private final int indexValueBlockCapacity;
    private final int newColumnType;
    private final int symbolCapacity;

    public FuzzChangeColumnTypeOperation(Rnd rnd, String columName, int newColumnType, int symbolCapacity, byte indexType, int indexValueBlockCapacity, boolean cacheSymbolMap) {
        this.columName = TestUtils.randomiseCase(rnd, columName);
        this.newColumnType = newColumnType;
        this.indexType = indexType;
        this.indexValueBlockCapacity = indexValueBlockCapacity;
        this.cacheSymbolMap = cacheSymbolMap;
        this.symbolCapacity = symbolCapacity;
    }

    public static boolean canChangeColumnType(RecordMetadata meta) {
        for (int i = 0, n = meta.getColumnCount(); i < n; i++) {
            if (FuzzChangeColumnTypeOperation.canGenerateColumnTypeChange(meta, i)) {
                return true;
            }
        }
        return false;
    }

    public static boolean canGenerateColumnTypeChange(RecordMetadata meta, int columnIndex) {
        if (columnIndex == meta.getTimestampIndex()) {
            return false;
        }

        int columnType = meta.getColumnType(columnIndex);
        return switch (columnType) {
            case ColumnType.STRING, ColumnType.SYMBOL, ColumnType.VARCHAR, ColumnType.BYTE, ColumnType.BOOLEAN,
                 ColumnType.SHORT, ColumnType.INT, ColumnType.LONG, ColumnType.FLOAT, ColumnType.DOUBLE,
                 ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP_NANO -> true;
            default -> false;
        };
    }

    /**
     * {@code isNoSentinelToTextAllowed} lets BOOLEAN, BYTE and SHORT change to STRING, SYMBOL or VARCHAR, half
     * of the time; without it the generator never changes a type without a NULL sentinel to one with it.
     */
    public static int changeColumnTypeTo(Rnd rnd, int columnType, long estimatedTotalRowCount, boolean isNoSentinelToTextAllowed) {
        final boolean isSymbolAllowed = estimatedTotalRowCount < MAX_TABLE_ROWS_TO_CONVERT_TO_SYMBOL;
        if (isNoSentinelToTextAllowed && ColumnType.isNoNullSentinelFixedType(columnType) && rnd.nextBoolean()) {
            return generateNextType(columnType, textColumnTypes, rnd, isSymbolAllowed, true);
        }
        return switch (columnType) {
            case ColumnType.STRING, ColumnType.SYMBOL, ColumnType.VARCHAR ->
                    generateNextType(columnType, varSizeConvertableColumnTypes, rnd, isSymbolAllowed, isNoSentinelToTextAllowed);
            case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG,
                 ColumnType.FLOAT, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP_NANO,
                 ColumnType.DOUBLE ->
                    generateNextType(columnType, numericConvertableColumnTypes, rnd, isSymbolAllowed, isNoSentinelToTextAllowed);
            default ->
                    throw new UnsupportedOperationException("Unsupported column type to generate type change: " + columnType);
        };
    }

    public static RecordMetadata generateColumnTypeChange(
            ObjList<FuzzTransaction> transactionList,
            long estimatedTotalRowCount,
            int metadataVersion,
            int waitBarrierVersion,
            Rnd rnd,
            RecordMetadata tableMetadata
    ) {
        return generateColumnTypeChange(transactionList, estimatedTotalRowCount, metadataVersion, waitBarrierVersion, rnd, tableMetadata, false, false);
    }

    /**
     * Changes the type of a column, with {@code isNoSentinelSkipped} none of type BOOLEAN, BYTE or SHORT, which
     * {@code isNoSentinelToTextAllowed} lets change to text, see {@link #changeColumnTypeTo}. Returns null when no
     * column can change.
     */
    public static RecordMetadata generateColumnTypeChange(
            ObjList<FuzzTransaction> transactionList,
            long estimatedTotalRowCount,
            int metadataVersion,
            int waitBarrierVersion,
            Rnd rnd,
            RecordMetadata tableMetadata,
            boolean isNoSentinelSkipped,
            boolean isNoSentinelToTextAllowed
    ) {
        int startColumnIndex = rnd.nextInt(tableMetadata.getColumnCount());

        // guard against converting designated timestamp
        while (startColumnIndex == tableMetadata.getTimestampIndex()) {
            startColumnIndex = rnd.nextInt(tableMetadata.getColumnCount());
        }

        for (int i = 0; i < tableMetadata.getColumnCount(); i++) {
            int columnIndex = (startColumnIndex + i) % tableMetadata.getColumnCount();
            if (columnIndex != tableMetadata.getTimestampIndex()
                    && canGenerateColumnTypeChange(tableMetadata, columnIndex)
                    && !(isNoSentinelSkipped && isNoSentinelSource(tableMetadata.getColumnType(columnIndex)))) {
                int columnType = tableMetadata.getColumnType(columnIndex);
                int newColType = changeColumnTypeTo(rnd, columnType, estimatedTotalRowCount, isNoSentinelToTextAllowed);
                return addColumnTypeChange(transactionList, metadataVersion, waitBarrierVersion, rnd, tableMetadata, columnIndex, newColType);
            }
        }

        // nothing to drop, only timestamp column left
        return null;
    }

    /**
     * Changes a STRING, SYMBOL or VARCHAR column, if the table has one, to BOOLEAN, BYTE or SHORT: a type
     * that reads the column top as 0 or false, until a later change back to text shows whether the top
     * was kept. {@link #getTextToNoSentinelChanges()} counts these changes.
     */
    public static RecordMetadata generateTextToNoSentinelChange(
            ObjList<FuzzTransaction> transactionList,
            int metadataVersion,
            int waitBarrierVersion,
            Rnd rnd,
            RecordMetadata tableMetadata
    ) {
        final int startColumnIndex = rnd.nextInt(tableMetadata.getColumnCount());
        for (int i = 0, n = tableMetadata.getColumnCount(); i < n; i++) {
            final int columnIndex = (startColumnIndex + i) % n;
            if (isText(tableMetadata.getColumnType(columnIndex))) {
                final int newColumnType = noSentinelColumnTypes[rnd.nextInt(noSentinelColumnTypes.length)];
                textToNoSentinelChanges.incrementAndGet();
                return addColumnTypeChange(transactionList, metadataVersion, waitBarrierVersion, rnd, tableMetadata, columnIndex, newColumnType);
            }
        }
        return null;
    }

    /**
     * Changes a column without a NULL sentinel, if the table has one, to a type with NULL: a BOOLEAN, BYTE or SHORT
     * column to one of {@link #NO_SENTINEL_WIDENING_TARGETS}. Native ALTER COLUMN TYPE
     * reads the column top as NULL and every stored value, 0 and false included, as itself. Adds the name of the
     * column to {@code widenedColumns}; {@link #getNoSentinelWidenings()} counts these changes.
     */
    public static RecordMetadata generateNoSentinelWidening(
            ObjList<FuzzTransaction> transactionList,
            long estimatedTotalRowCount,
            int metadataVersion,
            int waitBarrierVersion,
            Rnd rnd,
            RecordMetadata tableMetadata,
            ObjHashSet<String> widenedColumns
    ) {
        return generateNoSentinelWidening(transactionList, estimatedTotalRowCount, metadataVersion, waitBarrierVersion, rnd, tableMetadata, widenedColumns, null);
    }

    /**
     * {@link #generateNoSentinelWidening} of the column {@code preferredColumnName}, when it can widen, or else of a
     * random one.
     */
    public static RecordMetadata generateNoSentinelWidening(
            ObjList<FuzzTransaction> transactionList,
            long estimatedTotalRowCount,
            int metadataVersion,
            int waitBarrierVersion,
            Rnd rnd,
            RecordMetadata tableMetadata,
            ObjHashSet<String> widenedColumns,
            @Nullable String preferredColumnName
    ) {
        final int preferredColumnIndex = preferredColumnName != null ? tableMetadata.getColumnIndexQuiet(preferredColumnName) : -1;
        final int startColumnIndex = preferredColumnIndex >= 0 && isNoSentinelWideningSource(tableMetadata.getColumnType(preferredColumnIndex))
                ? preferredColumnIndex
                : rnd.nextInt(tableMetadata.getColumnCount());
        for (int i = 0, n = tableMetadata.getColumnCount(); i < n; i++) {
            final int columnIndex = (startColumnIndex + i) % n;
            if (columnIndex != tableMetadata.getTimestampIndex() && isNoSentinelWideningSource(tableMetadata.getColumnType(columnIndex))) {
                int newColumnType = NO_SENTINEL_WIDENING_TARGETS[rnd.nextInt(NO_SENTINEL_WIDENING_TARGETS.length)];
                if (ColumnType.isSymbol(newColumnType) && estimatedTotalRowCount >= MAX_TABLE_ROWS_TO_CONVERT_TO_SYMBOL) {
                    newColumnType = ColumnType.VARCHAR;
                }
                widenedColumns.add(tableMetadata.getColumnName(columnIndex));
                noSentinelWidenings.incrementAndGet();
                return addColumnTypeChange(transactionList, metadataVersion, waitBarrierVersion, rnd, tableMetadata, columnIndex, newColumnType);
            }
        }
        return null;
    }

    public static int getNoSentinelWidenings() {
        return noSentinelWidenings.get();
    }

    public static int getTextToNoSentinelChanges() {
        return textToNoSentinelChanges.get();
    }

    /**
     * Whether a change from {@code columnType} to {@code newColumnType} turns a column without a NULL sentinel into
     * a column with one: its column top becomes the NULL of the target, and a stored 0 or false the converted 0.
     */
    /**
     * Whether a lazy change of a column without a NULL sentinel to {@code newColumnType} decodes the parquet
     * partition at the width of the source type, which reads a column top as 0 or false, so that only the column-top
     * rows the decode records read NULL. A BYTE or SHORT column changed to INT, LONG or TIMESTAMP decodes straight to
     * the target, which reads the definition levels into its NULL.
     */
    public static boolean isDecodedAtSourceWidth(int columnType, int newColumnType) {
        return !((columnType == ColumnType.BYTE || columnType == ColumnType.SHORT)
                && (newColumnType == ColumnType.INT
                || newColumnType == ColumnType.LONG
                || ColumnType.isTimestamp(newColumnType)));
    }

    public static boolean isNoSentinelWidening(int columnType, int newColumnType) {
        return isNoSentinelSource(columnType) && isNullable(newColumnType);
    }

    public static void resetNoSentinelWidenings() {
        noSentinelWidenings.set(0);
    }

    public static void resetTextToNoSentinelChanges() {
        textToNoSentinelChanges.set(0);
    }

    @Override
    public boolean apply(Rnd tempRnd, CairoEngine engine, TableWriterAPI wApi, int virtualTimestampIndex, LongList excludedTsIntervals) {
        AlterOperationBuilder builder = new AlterOperationBuilder().ofColumnChangeType(
                0,
                wApi.getTableToken(),
                wApi.getMetadata().getTableId()
        );
        builder.addColumnToList(columName, 0, newColumnType, symbolCapacity, cacheSymbolMap,
                indexType, indexValueBlockCapacity, false);
        AlterOperation alterOp = builder.build();
        try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)
        ) {
            alterOp.withContext(context);
            wApi.apply(alterOp, true);
        }
        return true;
    }

    // in the letter case that the SQL of the change uses
    public String getColumnName() {
        return columName;
    }

    public int getNewColumnType() {
        return newColumnType;
    }

    private static RecordMetadata addColumnTypeChange(
            ObjList<FuzzTransaction> transactionList,
            int metadataVersion,
            int waitBarrierVersion,
            Rnd rnd,
            RecordMetadata tableMetadata,
            int columnIndex,
            int newColType
    ) {
        FuzzTransaction transaction = new FuzzTransaction();
        String columnName = tableMetadata.getColumnName(columnIndex);
        int columnType = tableMetadata.getColumnType(columnIndex);
        int capacity = 1 << (5 + rnd.nextInt(3));
        byte indexType;
        if (ColumnType.isSymbol(newColType) && (columnType == ColumnType.BOOLEAN || columnType == ColumnType.BYTE)) {
            // Spread index choice across BITMAP and the three POSTING
            // encoding variants so column-type changes exercise both
            // index families.
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
        // CAPACITY is parser-rejected for POSTING but the metadata
        // field still requires a value >= 2; reuse the BITMAP default.
        int indexValueBlockCapacity = (columnType == ColumnType.BOOLEAN) ? 4 : 128;
        boolean cacheSymbolMap = ColumnType.isSymbol(newColType) && rnd.nextBoolean();
        FuzzChangeColumnTypeOperation operation = new FuzzChangeColumnTypeOperation(rnd, columnName, newColType, capacity, indexType, indexValueBlockCapacity, cacheSymbolMap);
        transaction.operationList.add(operation);
        transaction.structureVersion = metadataVersion;
        transaction.waitBarrierVersion = waitBarrierVersion;
        transactionList.add(transaction);

        FuzzTestColumnMeta newMeta = new FuzzTestColumnMeta();
        for (int c = 0, n = tableMetadata.getColumnCount(); c < n; c++) {
            if (c != columnIndex) {
                newMeta.add(tableMetadata.getColumnMetadata(c));
            } else {
                newMeta.add(new TableColumnMetadata(
                        columnName,
                        newColType,
                        indexType,
                        indexValueBlockCapacity,
                        cacheSymbolMap,
                        null,
                        newMeta.getColumnCount(),
                        false
                ));
            }
        }
        newMeta.setTimestampIndex(tableMetadata.getTimestampIndex());

        return newMeta;
    }

    private static int generateNextType(int columnType, int[] convertableColumnTypes, Rnd rnd, boolean symbolsAllowed, boolean isNoSentinelToTextAllowed) {
        int nextColType = columnType;
        // disallow noop conversion
        // disallow conversions from non-nullable to nullable, but for BOOLEAN, BYTE and SHORT to text when allowed
        while (nextColType == columnType
                || (isNullable(columnType) != isNullable(nextColType)
                && !(isNoSentinelToTextAllowed && ColumnType.isNoNullSentinelFixedType(columnType) && isText(nextColType)))
                || (!symbolsAllowed && ColumnType.isSymbol(nextColType))) {
            nextColType = convertableColumnTypes[rnd.nextInt(convertableColumnTypes.length)];
        }
        return nextColType;
    }

    private static boolean isNoSentinelSource(int columnType) {
        return columnType == ColumnType.BOOLEAN || columnType == ColumnType.BYTE || columnType == ColumnType.SHORT;
    }

    private static boolean isNoSentinelWideningSource(int columnType) {
        return columnType == ColumnType.BOOLEAN
                || columnType == ColumnType.BYTE
                || columnType == ColumnType.SHORT;
    }

    private static boolean isText(int columnType) {
        return columnType == ColumnType.STRING || columnType == ColumnType.SYMBOL || columnType == ColumnType.VARCHAR;
    }

    private static boolean isNullable(int columnType) {
        return switch (columnType) {
            case ColumnType.BYTE, ColumnType.SHORT, ColumnType.UUID, ColumnType.IPv4, ColumnType.BOOLEAN -> false;
            default -> true;
        };
    }
}
