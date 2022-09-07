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

package io.questdb.griffin.wal.fuzz;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

import static io.questdb.test.tools.TestUtils.getZeroToOneDouble;

public class FuzzTransactionGenerator {

    public static ObjList<FuzzTransaction> generateSet(
            RecordMetadata tableModel,
            Rnd rnd,
            long minTimestamp,
            long maxTimestamp,
            int rowCount,
            boolean o3,
            double cancelRows,
            double notSet,
            double nullSet,
            double rollback,
            double collAdd,
            double collRemove,
            int transactionCount,
            int strLen,
            int totalSymbols) {
        ObjList<FuzzTransaction> transactionList = new ObjList<>();
        int metaVersion = 0;

        long lastTimestamp = minTimestamp;
        String[] symbols = new String[totalSymbols];
        for (int i = 0; i < totalSymbols; i++) {
            symbols[i] = Chars.toString(rnd.nextChars(strLen));
        }

        for (int i = 0; i < transactionCount; i++) {
            double transactionType = getZeroToOneDouble(rnd);
            if (transactionType < collAdd) {
                // generate column add
                tableModel = generateAddColumn(transactionList, metaVersion++, rnd, tableModel);
            } else if (transactionType < collAdd + collRemove) {
                // generate column remove
                tableModel = generateDropColumn(transactionList, metaVersion++, rnd, tableModel);
            } else {
                // generate row set
                int blockRows = rowCount / (transactionCount - i);
                if (i < transactionCount - 1) {
                    blockRows = (int) ((getZeroToOneDouble(rnd) * 0.5 + 0.5) * blockRows);
                }

                final long startTs, stopTs;
                if (o3) {
                    long offset = (long) ((maxTimestamp - minTimestamp) / (transactionCount - 1.0 * i) * i);
                    long jitter = (long) ((maxTimestamp - minTimestamp) / transactionCount * 1.0 * getZeroToOneDouble(rnd));
                    if (rnd.nextBoolean()) {
                        startTs = (minTimestamp + offset + jitter);
                    } else {
                        startTs = (minTimestamp + offset - jitter);
                    }
                } else {
                    startTs = lastTimestamp;
                }
                stopTs = (long) (startTs + (maxTimestamp - minTimestamp) / (transactionCount - i) * (i + 1.0) * getZeroToOneDouble(rnd));


                generateDataBlock(transactionList, rnd, tableModel, metaVersion, startTs, stopTs, blockRows, o3, cancelRows, notSet, nullSet, rollback, strLen, symbols);
                rowCount -= blockRows;
                lastTimestamp = stopTs;
            }
        }

        return transactionList;
    }

    static void generateDataBlock(
            ObjList<FuzzTransaction> transactionList,
            Rnd rnd,
            RecordMetadata tableModel,
            int metadataVersion,
            long minTimestamp,
            long maxTimestamp,
            int rowCount,
            boolean o3,
            double cancelRows,
            double notSet,
            double nullSet,
            double rollback,
            int strLen,
            String[] symbols
    ) {
        FuzzTransaction transaction = new FuzzTransaction();
        long timestamp = minTimestamp;
        for (int i = 0; i < rowCount; i++) {
            timestamp = o3 ? minTimestamp + rnd.nextLong(maxTimestamp - minTimestamp) : timestamp + (maxTimestamp - minTimestamp) / rowCount;
            transaction.operationList.add(new FuzzInsertOperation(rnd, tableModel, timestamp, notSet, nullSet, cancelRows, strLen, symbols));
        }

        transaction.rollback = getZeroToOneDouble(rnd) < rollback;
        transaction.metadataVersion = metadataVersion;
        transactionList.add(transaction);
    }

    static RecordMetadata generateDropColumn(
            ObjList<FuzzTransaction> transactionList,
            int metadataVersion,
            Rnd rnd,
            RecordMetadata tableModel
    ) {
        FuzzTransaction transaction = new FuzzTransaction();
        for (int i = 0; i < tableModel.getColumnCount(); i++) {
            int columnIndex = rnd.nextInt(tableModel.getColumnCount());

            int type = tableModel.getColumnType(columnIndex);
            if (type > 0 && columnIndex != tableModel.getTimestampIndex()) {
                String columnName = tableModel.getColumnName(columnIndex);
                transaction.operationList.add(new FuzzDropColumnOperation(tableModel, columnName));
                transaction.metadataVersion = metadataVersion;
                transactionList.add(transaction);
                FuzzTestColumnMeta newMeta = new FuzzTestColumnMeta();
                copyColumnsExcept(tableModel, newMeta, columnIndex);
                return newMeta;
            }
        }

        // nothing to drop
        return tableModel;
    }

    private static void copyColumnsExcept(RecordMetadata from, FuzzTestColumnMeta to, int columnIndex) {
        for (int i = 0, n = from.getColumnCount(); i < n; i++) {
            int columnType = from.getColumnType(i);
            if (i == columnIndex) {
                columnType = -columnType;
            }
            to.add(new TableColumnMetadata(
                    from.getColumnName(i),
                    from.getColumnHash(i),
                    columnType,
                    from.isColumnIndexed(i),
                    from.getIndexValueBlockCapacity(i),
                    from.isSymbolTableStatic(i),
                    GenericRecordMetadata.copyOf(from.getMetadata(i))
            ));
        }
        to.setTimestampIndex(from.getTimestampIndex());
    }

    static RecordMetadata generateAddColumn(
            ObjList<FuzzTransaction> transactionList,
            int metadataVersion,
            Rnd rnd,
            RecordMetadata tableModel
    ) {
        FuzzTransaction transaction = new FuzzTransaction();
        int newType = generateNewColumnType(rnd);
        boolean indexFlag = newType == ColumnType.SYMBOL && rnd.nextBoolean();
        int indexValueBlockCapacity = 256;
        boolean symbolTableStatic = rnd.nextBoolean();

        String newColName;
        for (int col = 0; ; col++) {
            newColName = "new_col_" + col;
            int colIndex = tableModel.getColumnIndexQuiet(newColName);
            if (colIndex == -1 || tableModel.getColumnType(colIndex) < 0) {
                break;
            }
        }
        transaction.operationList.add(new FuzzAddColumnOperation(tableModel, newColName, newType, indexFlag, indexValueBlockCapacity, symbolTableStatic));
        transaction.metadataVersion = metadataVersion;
        transactionList.add(transaction);

        FuzzTestColumnMeta newMeta = new FuzzTestColumnMeta();
        GenericRecordMetadata.copyColumns(tableModel, newMeta);
        newMeta.add(new TableColumnMetadata(
                newColName,
                -1,
                newType,
                indexFlag,
                indexValueBlockCapacity,
                symbolTableStatic,
                null,
                newMeta.getColumnCount()
        ));
        newMeta.setTimestampIndex(tableModel.getTimestampIndex());
        return newMeta;
    }

    private static int generateNewColumnType(Rnd rnd) {
        return FuzzInsertOperation.SUPPORTED_COLUM_TYPES[rnd.nextInt(FuzzInsertOperation.SUPPORTED_COLUM_TYPES.length)];
    }
}
