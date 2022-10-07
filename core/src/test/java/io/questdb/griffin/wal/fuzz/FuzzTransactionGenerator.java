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
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

import static io.questdb.test.tools.TestUtils.getZeroToOneDouble;

public class FuzzTransactionGenerator {
    private static final int MAX_COLUMNS = 200;

    public static ObjList<FuzzTransaction> generateSet(
            RecordMetadata tableMetadata,
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
            double colRename,
            double dataAdd,
            int transactionCount,
            int strLen,
            String[] symbols) {
        ObjList<FuzzTransaction> transactionList = new ObjList<>();
        int metaVersion = 0;

        long lastTimestamp = minTimestamp;
        double totalProbs = collAdd + collRemove + collRemove + dataAdd;
        collAdd = collAdd / totalProbs;
        collRemove = collRemove / totalProbs;
        colRename = colRename / totalProbs;

        // Reduce some random parameters if there is too much data so test can finish in reasonable time
        transactionCount = Math.min(transactionCount, 10 * 1_000_000 / rowCount);

        for (int i = 0; i < transactionCount; i++) {
            double transactionType = getZeroToOneDouble(rnd);
           if (transactionType < collRemove) {
                // generate column remove
                RecordMetadata newTableMetadata = generateDropColumn(transactionList, metaVersion, rnd, tableMetadata);
                if (newTableMetadata != null) {
                    // Sometimes there can be nothing to remove
                    metaVersion++;
                    tableMetadata = newTableMetadata;
                }
            } else if (transactionType < collRemove + colRename) {
                // generate column rename
                RecordMetadata newTableMetadata = generateRenameColumn(transactionList, metaVersion, rnd, tableMetadata);
                if (newTableMetadata != null) {
                    // Sometimes there can be nothing to remove
                    metaVersion++;
                    tableMetadata = newTableMetadata;
                }
            } else  if (transactionType < collAdd + collRemove + colRename && getNonDeletedColumnCount(tableMetadata) < MAX_COLUMNS) {
               // generate column add
               tableMetadata = generateAddColumn(transactionList, metaVersion++, rnd, tableMetadata);
           } else {
                // generate row set
                int blockRows = rowCount / (transactionCount - i);
                if (i < transactionCount - 1) {
                    blockRows = (int) ((getZeroToOneDouble(rnd) * 0.5 + 0.5) * blockRows);
                }

                final long startTs, stopTs;
                if (o3) {
                    long offset = (long) ((maxTimestamp - minTimestamp + 1.0) / transactionCount * i);
                    long jitter = (long) ((maxTimestamp - minTimestamp + 1.0) / transactionCount * getZeroToOneDouble(rnd));
                    if (rnd.nextBoolean()) {
                        startTs = (minTimestamp + offset + jitter);
                    } else {
                        startTs = (minTimestamp + offset - jitter);
                    }
                } else {
                    startTs = lastTimestamp;
                }
                long size = (maxTimestamp - minTimestamp) / transactionCount;
                if (o3) {
                    size *= getZeroToOneDouble(rnd);
                }
                stopTs = Math.min(startTs + size, maxTimestamp);

                generateDataBlock(transactionList, rnd, tableMetadata, metaVersion, startTs, stopTs, blockRows, o3, cancelRows, notSet, nullSet, rollback, strLen, symbols);
                rowCount -= blockRows;
                lastTimestamp = stopTs;
            }
        }

        return transactionList;
    }

    private static int getNonDeletedColumnCount(RecordMetadata tableMetadata) {
        if (tableMetadata instanceof FuzzTestColumnMeta) {
            return ((FuzzTestColumnMeta)tableMetadata).getLiveColumnCount();
        } else {
            return tableMetadata.getColumnCount();
        }
    }

    private static RecordMetadata generateRenameColumn(ObjList<FuzzTransaction> transactionList, int metadataVersion, Rnd rnd, RecordMetadata tableMetadata) {
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

                transaction.operationList.add(new FuzzRenameColumnOperation(tableMetadata, columnName, newColName));
                transaction.structureVersion = metadataVersion;
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
        transaction.structureVersion = metadataVersion;
        transactionList.add(transaction);
    }

    static RecordMetadata generateDropColumn(
            ObjList<FuzzTransaction> transactionList,
            int metadataVersion,
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
                transaction.operationList.add(new FuzzDropColumnOperation(tableMetadata, columnName));
                transaction.structureVersion = metadataVersion;
                transactionList.add(transaction);
                FuzzTestColumnMeta newMeta = new FuzzTestColumnMeta();
                copyColumnsExcept(tableMetadata, newMeta, columnIndex);
                return newMeta;
            }
        }

        // nothing to drop, only timestamp column left
        return null;
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
        transaction.operationList.add(new FuzzAddColumnOperation(tableMetadata, newColName, newType, indexFlag, indexValueBlockCapacity, symbolTableStatic));
        transaction.structureVersion = metadataVersion;
        transactionList.add(transaction);

        FuzzTestColumnMeta newMeta = new FuzzTestColumnMeta();
        GenericRecordMetadata.copyColumns(tableMetadata, newMeta);
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
        newMeta.setTimestampIndex(tableMetadata.getTimestampIndex());
        return newMeta;
    }

    private static int generateNewColumnType(Rnd rnd) {
        return FuzzInsertOperation.SUPPORTED_COLUM_TYPES[rnd.nextInt(FuzzInsertOperation.SUPPORTED_COLUM_TYPES.length)];
    }
}
