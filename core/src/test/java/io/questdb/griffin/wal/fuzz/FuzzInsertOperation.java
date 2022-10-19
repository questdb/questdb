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
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TestRecord;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.wal.TableWriterFrontend;
import io.questdb.griffin.engine.functions.constants.Long128Constant;
import io.questdb.std.IntList;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;

import static io.questdb.test.tools.TestUtils.getZeroToOneDouble;

public class FuzzInsertOperation implements FuzzTransactionOperation {
    public final static int[] SUPPORTED_COLUM_TYPES = new int[]{
            ColumnType.INT,
            ColumnType.LONG,
            ColumnType.TIMESTAMP,
            ColumnType.DATE,
            ColumnType.SYMBOL,
            ColumnType.FLOAT,
            ColumnType.DOUBLE,
            ColumnType.STRING,
            ColumnType.BINARY,
            ColumnType.SHORT,
            ColumnType.BYTE,
            ColumnType.LONG128,
            ColumnType.LONG256,
            ColumnType.GEOBYTE,
            ColumnType.GEOSHORT,
            ColumnType.GEOINT,
            ColumnType.GEOLONG,
            ColumnType.BOOLEAN
    };

    private final double nullSet;
    private final long timestamp;
    private final double notSet;
    private final RecordMetadata metadata;
    private final double cancelRows;
    private final int strLen;
    private final String[] symbols;
    private final long s0;
    private final long s1;

    public FuzzInsertOperation(long seed1, long seed2, RecordMetadata metadata, long timestamp, double notSet, double nullSet, double cancelRows, int strLen, String[] symbols) {
        this.cancelRows = cancelRows;
        this.strLen = strLen;
        this.symbols = symbols;
        this.s0 = seed1;
        this.s1 = seed2;
        this.metadata = metadata;
        this.timestamp = timestamp;
        this.notSet = notSet;
        this.nullSet = nullSet;
    }

    @Override
    public boolean apply(Rnd rnd, TableWriterFrontend tableWriter, String tableName, int tableId, IntList tempList, TestRecord.ArrayBinarySequence binarySequence) {
        rnd.reset(this.s0, this.s1);
        TableWriter.Row row = tableWriter.newRow(timestamp);

        int columnCount = metadata.getColumnCount();
        if (getZeroToOneDouble(rnd) < cancelRows) {
            columnCount = rnd.nextInt(metadata.getColumnCount());
        }

        int tableColumnCount = metadata.getColumnCount();
        tempList.setPos(tableColumnCount);
        tempList.setAll(tableColumnCount, 0);

        for (int i = 0; i < columnCount; i++) {
            int index = rnd.nextInt(tableColumnCount);
            while (tempList.getQuick(index % tableColumnCount) != 0) {
                index++;
            }
            index = index % tableColumnCount;
            tempList.setQuick(index, 1);

            if (index != metadata.getTimestampIndex()) {
                int type = metadata.getColumnType(index);
                if (type > 0) {
                    if (getZeroToOneDouble(rnd) > notSet) {
                        boolean isNull = getZeroToOneDouble(rnd) < nullSet;

                        switch (type) {
                            case ColumnType.INT:
                                row.putInt(index, isNull ? Numbers.INT_NaN : rnd.nextInt());
                                break;

                            case ColumnType.LONG:
                                row.putLong(index, isNull ? Numbers.LONG_NaN : rnd.nextLong());
                                break;

                            case ColumnType.TIMESTAMP:
                                row.putTimestamp(index, isNull ? Numbers.LONG_NaN : rnd.nextLong());
                                break;

                            case ColumnType.DATE:
                                row.putDate(index, isNull ? Numbers.LONG_NaN : rnd.nextLong());
                                break;

                            case ColumnType.SYMBOL:
                                row.putSym(index, isNull || symbols.length == 0 ? null : symbols[rnd.nextInt(symbols.length)]);
                                break;

                            case ColumnType.FLOAT:
                                row.putFloat(index, isNull ? Float.NaN : rnd.nextFloat());
                                break;

                            case ColumnType.SHORT:
                                row.putShort(index, isNull ? 0 : rnd.nextShort());
                                break;

                            case ColumnType.BYTE:
                                row.putByte(index, isNull ? 0 : rnd.nextByte());
                                break;

                            case ColumnType.BOOLEAN:
                                row.putBool(index, rnd.nextBoolean());
                                break;

                            case ColumnType.LONG128:
                                if (!isNull) {
                                    row.putLong128LittleEndian(index, rnd.nextLong(), rnd.nextLong());
                                } else {
                                    row.putLong128LittleEndian(index, Long128Constant.NULL_HI, Long128Constant.NULL_LO);
                                }
                                break;

                            case ColumnType.LONG256:
                                if (!isNull) {
                                    row.putLong256(index, Long256Impl.NULL_LONG256);
                                } else {
                                    row.putLong256(index, rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
                                }
                                break;

                            case ColumnType.DOUBLE:
                                row.putDouble(index, isNull ? Double.NaN : rnd.nextDouble());
                                break;

                            case ColumnType.STRING:
                                row.putStr(index, isNull ? null : strLen == 0 ? "" : rnd.nextString(rnd.nextInt(strLen)));
                                break;

                            case ColumnType.BINARY:
                                int len = strLen > 0 ? rnd.nextInt(strLen) : 0;
                                row.putBin(index, isNull ? null : binarySequence.of(len == 0 ? new byte[0] : rnd.nextBytes(len)));
                                break;

                            case ColumnType.GEOBYTE:
                            case ColumnType.GEOSHORT:
                            case ColumnType.GEOINT:
                            case ColumnType.GEOLONG:
                                row.putGeoHash(index, rnd.nextLong());
                                break;

                            default:
                                throw new UnsupportedOperationException();
                        }
                    }
                }
            }
        }

        if (columnCount < metadata.getColumnCount()) {
            row.cancel();
        } else {
            row.append();
        }
        return false;
    }
}
