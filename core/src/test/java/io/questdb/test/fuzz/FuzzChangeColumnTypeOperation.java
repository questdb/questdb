/*******************************************************************************
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
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;

public class FuzzChangeColumnTypeOperation implements FuzzTransactionOperation {
    private static final long MAX_TABLE_ROWS_TO_CONVERT_TO_SYMBOL = 60_000;
    private static final int[] numericConvertableColumnTypes = {
            ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG,
            ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.BOOLEAN,
            ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP_NANO,
            ColumnType.STRING, ColumnType.VARCHAR
    };
    private static final int[] varSizeConvertableColumnTypes = {
            ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG,
            ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.BOOLEAN,
            ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP_NANO,
            ColumnType.UUID, ColumnType.IPv4,
            ColumnType.STRING, ColumnType.SYMBOL, ColumnType.VARCHAR
    };
    private final boolean cacheSymbolMap;
    private final String columName;
    private final boolean indexFlag;
    private final int indexValueBlockCapacity;
    private final int newColumnType;
    private final int symbolCapacity;

    public FuzzChangeColumnTypeOperation(Rnd rnd, String columName, int newColumnType, int symbolCapacity, boolean indexFlag, int indexValueBlockCapacity, boolean cacheSymbolMap) {
        this.columName = TestUtils.randomiseCase(rnd, columName);
        this.newColumnType = newColumnType;
        this.indexFlag = indexFlag;
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
        switch (columnType) {
            case ColumnType.STRING:
            case ColumnType.SYMBOL:
            case ColumnType.VARCHAR:
            case ColumnType.BYTE:
            case ColumnType.BOOLEAN:
            case ColumnType.SHORT:
            case ColumnType.INT:
            case ColumnType.LONG:
            case ColumnType.FLOAT:
            case ColumnType.DOUBLE:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                return true;
        }
        return false;
    }

    public static int changeColumnTypeTo(Rnd rnd, int columnType, long estimatedTotalRowCount) {
        switch (columnType) {
            case ColumnType.STRING:
            case ColumnType.SYMBOL:
            case ColumnType.VARCHAR:
                return generateNextType(columnType, varSizeConvertableColumnTypes, rnd, estimatedTotalRowCount < MAX_TABLE_ROWS_TO_CONVERT_TO_SYMBOL);
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
            case ColumnType.SHORT:
            case ColumnType.INT:
            case ColumnType.LONG:
            case ColumnType.FLOAT:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
            case ColumnType.TIMESTAMP_NANO:
            case ColumnType.DOUBLE:
                return generateNextType(columnType, numericConvertableColumnTypes, rnd, estimatedTotalRowCount < MAX_TABLE_ROWS_TO_CONVERT_TO_SYMBOL);
            default:
                throw new UnsupportedOperationException("Unsupported column type to generate type change: " + columnType);
        }
    }


    public static RecordMetadata generateColumnTypeChange(
            ObjList<FuzzTransaction> transactionList,
            long estimatedTotalRowCount,
            int metadataVersion,
            int waitBarrierVersion,
            Rnd rnd,
            RecordMetadata tableMetadata
    ) {
        FuzzTransaction transaction = new FuzzTransaction();
        int startColumnIndex = rnd.nextInt(tableMetadata.getColumnCount());

        // guard against converting designated timestamp
        while (startColumnIndex == tableMetadata.getTimestampIndex()) {
            startColumnIndex = rnd.nextInt(tableMetadata.getColumnCount());
        }

        for (int i = 0; i < tableMetadata.getColumnCount(); i++) {
            int columnIndex = (startColumnIndex + i) % tableMetadata.getColumnCount();
            if (columnIndex != tableMetadata.getTimestampIndex() && canGenerateColumnTypeChange(tableMetadata, columnIndex)) {
                String columnName = tableMetadata.getColumnName(columnIndex);
                int columnType = tableMetadata.getColumnType(columnIndex);
                int newColType = changeColumnTypeTo(rnd, columnType, estimatedTotalRowCount);

                int capacity = 1 << (5 + rnd.nextInt(3));
                boolean indexFlag = ColumnType.isSymbol(newColType) && (columnType == ColumnType.BOOLEAN || columnType == ColumnType.BYTE);
                int indexValueBlockCapacity = (columnType == ColumnType.BOOLEAN) ? 4 : 128;
                boolean cacheSymbolMap = ColumnType.isSymbol(newColType) && rnd.nextBoolean();
                FuzzChangeColumnTypeOperation operation = new FuzzChangeColumnTypeOperation(rnd, columnName, newColType, capacity, indexFlag, indexValueBlockCapacity, cacheSymbolMap);
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
                                indexFlag,
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
        }

        // nothing to drop, only timestamp column left
        return null;
    }

    @Override
    public boolean apply(Rnd tempRnd, CairoEngine engine, TableWriterAPI wApi, int virtualTimestampIndex, LongList excludedTsIntervals) {
        AlterOperationBuilder builder = new AlterOperationBuilder().ofColumnChangeType(
                0,
                wApi.getTableToken(),
                wApi.getMetadata().getTableId()
        );
        builder.addColumnToList(columName, 0, newColumnType, symbolCapacity, cacheSymbolMap,
                indexFlag, indexValueBlockCapacity, false);
        AlterOperation alterOp = builder.build();
        try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1)
        ) {
            alterOp.withContext(context);
            wApi.apply(alterOp, true);
        }
        return true;
    }

    private static int generateNextType(int columnType, int[] numericConvertableColumnTypes, Rnd rnd, boolean symbolsAllowed) {
        int nextColType = columnType;
        // disallow noop conversion
        // disallow conversions from non-nullable to nullable
        while (nextColType == columnType || (isNullable(columnType) != isNullable(nextColType) || (!symbolsAllowed && ColumnType.isSymbol(nextColType)))) {
            nextColType = numericConvertableColumnTypes[rnd.nextInt(numericConvertableColumnTypes.length)];
        }
        return nextColType;
    }

    private static boolean isNullable(int columnType) {
        switch (columnType) {
            case ColumnType.BYTE:
            case ColumnType.SHORT:
            case ColumnType.UUID:
            case ColumnType.IPv4:
            case ColumnType.BOOLEAN:
                return false;
            default:
                return true;
        }
    }
}
