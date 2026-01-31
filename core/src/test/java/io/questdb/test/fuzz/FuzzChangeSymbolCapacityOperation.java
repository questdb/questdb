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
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;

public class FuzzChangeSymbolCapacityOperation implements FuzzTransactionOperation {
    private final String columName;
    private final int symbolCapacity;

    public FuzzChangeSymbolCapacityOperation(Rnd rnd, String columName, int symbolCapacity) {
        this.columName = TestUtils.randomiseCase(rnd, columName);
        this.symbolCapacity = symbolCapacity;
    }

    public static boolean generateSymbolCapacityChange(
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

            if (tableMetadata.getColumnType(columnIndex) == ColumnType.SYMBOL) {
                String columnName = tableMetadata.getColumnName(columnIndex);
                int capacity = 1 << (1 + rnd.nextInt(9));
                FuzzChangeSymbolCapacityOperation operation =
                        new FuzzChangeSymbolCapacityOperation(rnd, columnName, capacity);
                transaction.operationList.add(operation);
                transaction.structureVersion = metadataVersion;
                transaction.waitBarrierVersion = waitBarrierVersion;
                transactionList.add(transaction);

                return true;
            }
        }

        // nothing to drop, only timestamp column left
        return false;
    }

    @Override
    public boolean apply(Rnd tempRnd, CairoEngine engine, TableWriterAPI wApi, int virtualTimestampIndex, LongList excludedTsIntervals) {
        AlterOperationBuilder builder = new AlterOperationBuilder().ofSymbolCapacityChange(
                0,
                wApi.getTableToken(),
                wApi.getMetadata().getTableId()
        );
        builder.addColumnToList(columName, 0, ColumnType.SYMBOL, symbolCapacity, tempRnd.nextBoolean(),
                tempRnd.nextBoolean() ? IndexType.SYMBOL : IndexType.NONE, tempRnd.nextInt(), false);
        AlterOperation alterOp = builder.build();
        try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1)) {
            alterOp.withSqlStatement(
                    "ALTER TABLE " + wApi.getTableToken().getTableName() + " ALTER COLUMN "
                            + columName + " SYMBOL CAPACITY " + symbolCapacity);
            alterOp.withContext(context);
            wApi.apply(alterOp, true);
        }
        return false;
    }
}
