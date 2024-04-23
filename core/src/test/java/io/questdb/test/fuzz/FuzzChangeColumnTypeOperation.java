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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class FuzzChangeColumnTypeOperation implements FuzzTransactionOperation {
    private final boolean cacheSymbolMap;
    private final String columName;
    private final boolean indexFlag;
    private final int indexValueBlockCapacity;
    private final int newColumnType;
    private final int symbolCapacity;

    public FuzzChangeColumnTypeOperation(String columName, int newColumnType, int symbolCapacity, boolean indexFlag, int indexValueBlockCapacity, boolean cacheSymbolMap) {
        this.columName = columName;
        this.newColumnType = newColumnType;
        this.indexFlag = indexFlag;
        this.indexValueBlockCapacity = indexValueBlockCapacity;
        this.cacheSymbolMap = cacheSymbolMap;
        this.symbolCapacity = symbolCapacity;
    }

    public static boolean canChangeColumnType(RecordMetadata meta) {
        for (int i = 0, n = meta.getColumnCount(); i < n; i++) {
            if (FuzzChangeColumnTypeOperation.canGenerateColumnTypeChange(meta.getColumnType(i))) {
                return true;
            }
        }
        return false;
    }

    public static boolean canGenerateColumnTypeChange(int columnType) {
        switch (columnType) {
            case ColumnType.STRING:
            case ColumnType.SYMBOL:
            case ColumnType.VARCHAR:
                return true;
        }
        return false;
    }

    public static RecordMetadata generateColumnTypeChange(ObjList<FuzzTransaction> transactionList, int metadataVersion, int waitBarrierVersion, Rnd rnd, RecordMetadata tableMetadata) {
        FuzzTransaction transaction = new FuzzTransaction();
        int startColumnIndex = rnd.nextInt(tableMetadata.getColumnCount());
        for (int i = 0; i < tableMetadata.getColumnCount(); i++) {
            int columnIndex = (startColumnIndex + i) % tableMetadata.getColumnCount();

            int type = tableMetadata.getColumnType(columnIndex);
            if (canGenerateColumnTypeChange(type)) {
                String columnName = tableMetadata.getColumnName(columnIndex);
                int columnType = tableMetadata.getColumnType(columnIndex);
                int newColType = changeColumnTypeTo(rnd, columnType);

                int capacity = rnd.nextInt(10000);
                boolean indexFlag = ColumnType.isSymbol(newColType) && rnd.nextBoolean();
                int indexValueBlockCapacity = rnd.nextInt(10000);
                boolean cacheSymbolMap = ColumnType.isSymbol(newColType) && rnd.nextBoolean();
                transaction.operationList.add(new FuzzChangeColumnTypeOperation(columnName, newColType, capacity, indexFlag, indexValueBlockCapacity, cacheSymbolMap));
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
    public boolean apply(Rnd rnd, CairoEngine engine, TableWriterAPI wApi, int virtualTimestampIndex) {
        AlterOperationBuilder builder = new AlterOperationBuilder().ofColumnChangeType(
                0,
                wApi.getTableToken(),
                wApi.getMetadata().getTableId()
        );
        builder.addColumnToList(columName, 0, newColumnType, symbolCapacity, cacheSymbolMap,
                indexFlag, indexValueBlockCapacity, false);
        AlterOperation alterOp = builder.build();
        wApi.apply(alterOp, true);
        return true;
    }

    private static int changeColumnTypeTo(Rnd rnd, int columnType) {
        switch (columnType) {
            case ColumnType.STRING:
                return rnd.nextBoolean() ? ColumnType.SYMBOL : ColumnType.VARCHAR;
            case ColumnType.SYMBOL:
                return rnd.nextBoolean() ? ColumnType.STRING : ColumnType.VARCHAR;
            case ColumnType.VARCHAR:
                return rnd.nextBoolean() ? ColumnType.STRING : ColumnType.SYMBOL;
        }
        return columnType;
    }
}
