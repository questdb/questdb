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
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.IntList;
import io.questdb.std.Long256Impl;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rnd;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.TestRecord;

public class FuzzInsertOperation implements FuzzTransactionOperation, QuietCloseable {
    public final static int[] SUPPORTED_COLUMN_TYPES = new int[]{
            ColumnType.INT,
            ColumnType.LONG,
            ColumnType.TIMESTAMP,
            ColumnType.TIMESTAMP_NANO,
            ColumnType.DATE,
            ColumnType.SYMBOL,
            ColumnType.FLOAT,
            ColumnType.DOUBLE,
            ColumnType.STRING,
            ColumnType.VARCHAR,
            ColumnType.BINARY,
            ColumnType.SHORT,
            ColumnType.BYTE,
            ColumnType.LONG128,
            ColumnType.LONG256,
            ColumnType.GEOBYTE,
            ColumnType.GEOSHORT,
            ColumnType.GEOINT,
            ColumnType.GEOLONG,
            ColumnType.BOOLEAN,
            ColumnType.UUID,
            ColumnType.IPv4,
            ColumnType.ARRAY
    };
    private static final ThreadLocal<DirectArray> tlArray = new ThreadLocal<>(() -> {
        DirectArray array = new DirectArray();
        AbstractTest.CLOSEABLE.add(array);
        return array;
    });
    private static final ThreadLocal<TestRecord.ArrayBinarySequence> tlBinSeq = new ThreadLocal<>(TestRecord.ArrayBinarySequence::new);
    private static final ThreadLocal<IntList> tlIntList = new ThreadLocal<>(IntList::new);
    private static final ThreadLocal<Utf8StringSink> tlUtf8 = new ThreadLocal<>(Utf8StringSink::new);
    private final double cancelRows;
    private final double notSet;
    private final double nullSet;
    private final long s0;
    private final long s1;
    private final double strLen;
    private final String[] symbols;
    private final long timestamp;

    public FuzzInsertOperation(
            long seed1,
            long seed2,
            long timestamp,
            double notSet,
            double nullSet,
            double cancelRows,
            int strLen,
            String[] symbols
    ) {
        this.cancelRows = cancelRows;
        this.strLen = strLen;
        this.symbols = symbols;
        this.s0 = seed1;
        this.s1 = seed2;
        this.timestamp = timestamp;
        this.notSet = notSet;
        this.nullSet = nullSet;
    }

    public FuzzInsertOperation(FuzzInsertOperation operation) {
        this.s0 = operation.s0;
        this.s1 = operation.s1;
        this.timestamp = operation.timestamp;
        this.notSet = operation.notSet;
        this.nullSet = operation.nullSet;
        this.cancelRows = operation.cancelRows;
        this.strLen = operation.strLen;
        this.symbols = operation.symbols != null ? operation.symbols.clone() : null;
    }

    @Override
    public boolean apply(Rnd rnd, CairoEngine engine, TableWriterAPI tableWriter, int virtualTimestampIndex, LongList excludedTsIntervals) {
        if (excludedTsIntervals != null && IntervalUtils.isInIntervals(excludedTsIntervals, timestamp)) {
            return false;
        }

        rnd.reset(this.s1, this.s0);
        rnd.nextLong();
        rnd.nextLong();
        RecordMetadata metadata = tableWriter.getMetadata();

        final IntList tempList = tlIntList.get();
        final TestRecord.ArrayBinarySequence binarySequence = tlBinSeq.get();
        final Utf8StringSink utf8StringSink = tlUtf8.get();

        TableWriter.Row row = tableWriter.newRow(timestamp);
        // this is hack for populating a table without designated timestamp
        if (virtualTimestampIndex != -1) {
            row.putTimestamp(virtualTimestampIndex, timestamp);
        }

        int columnCount = metadata.getColumnCount();
        if (rnd.nextDouble() < cancelRows) {
            columnCount = rnd.nextInt(metadata.getColumnCount());
        }

        int tableColumnCount = metadata.getColumnCount();
        tempList.setPos(tableColumnCount);
        tempList.setAll(tableColumnCount, 0);

        for (int i = 0; i < columnCount; i++) {
            int columnIndex = rnd.nextInt(tableColumnCount);
            while (tempList.getQuick(columnIndex % tableColumnCount) != 0) {
                columnIndex++;
            }
            columnIndex = columnIndex % tableColumnCount;
            tempList.setQuick(columnIndex, 1);

            if (columnIndex != metadata.getTimestampIndex() && columnIndex != virtualTimestampIndex) {
                int type = metadata.getColumnType(columnIndex);
                if (type > 0) {
                    if (rnd.nextDouble() > notSet) {
                        boolean isNull = rnd.nextDouble() < nullSet;
                        appendColumnValue(rnd, type, row, columnIndex, isNull, utf8StringSink, binarySequence);
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

    @Override
    public void close() {
    }

    public int getStrLen() {
        return (int) strLen;
    }

    public String[] getSymbols() {
        return symbols;
    }

    public long getTimestamp() {
        return timestamp;
    }

    private int nextVariableColumnLen(Rnd rnd) {
        return (int) Math.pow(strLen, rnd.nextDouble());
    }

    protected void appendColumnValue(Rnd rnd, int type, TableWriter.Row row, int columnIndex, boolean isNull, Utf8StringSink utf8StringSink, TestRecord.ArrayBinarySequence binarySequence) {
        switch (ColumnType.tagOf(type)) {
            case ColumnType.CHAR:
                row.putChar(columnIndex, rnd.nextChar());
                break;

            case ColumnType.INT:
                row.putInt(columnIndex, isNull ? Numbers.INT_NULL : rnd.nextInt());
                break;

            case ColumnType.IPv4:
                row.putInt(columnIndex, isNull ? Numbers.IPv4_NULL : rnd.nextInt());
                break;

            case ColumnType.LONG:
                row.putLong(columnIndex, isNull ? Numbers.LONG_NULL : rnd.nextLong());
                break;

            case ColumnType.TIMESTAMP:
                row.putTimestamp(columnIndex, isNull ? Numbers.LONG_NULL : rnd.nextLong());
                break;

            case ColumnType.DATE:
                row.putDate(columnIndex, isNull ? Numbers.LONG_NULL : rnd.nextLong());
                break;

            case ColumnType.SYMBOL:
                row.putSym(columnIndex, isNull || symbols.length == 0 ? null : symbols[rnd.nextInt(symbols.length)]);
                break;

            case ColumnType.FLOAT:
                row.putFloat(columnIndex, isNull ? Float.NaN : rnd.nextFloat());
                break;

            case ColumnType.SHORT:
                row.putShort(columnIndex, isNull ? 0 : rnd.nextShort());
                break;

            case ColumnType.BYTE:
                row.putByte(columnIndex, isNull ? 0 : rnd.nextByte());
                break;

            case ColumnType.BOOLEAN:
                row.putBool(columnIndex, rnd.nextBoolean());
                break;

            case ColumnType.LONG128:
                if (!isNull) {
                    row.putLong128(columnIndex, rnd.nextLong(), rnd.nextLong());
                } else {
                    row.putLong128(columnIndex, Numbers.LONG_NULL, Numbers.LONG_NULL);
                }
                break;

            case ColumnType.LONG256:
                if (!isNull) {
                    row.putLong256(columnIndex, Long256Impl.NULL_LONG256);
                } else {
                    row.putLong256(columnIndex, rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
                }
                break;

            case ColumnType.DOUBLE:
                row.putDouble(columnIndex, isNull ? Double.NaN : rnd.nextDouble());
                break;

            case ColumnType.VARCHAR:
                if (isNull) {
                    row.putVarchar(columnIndex, null);
                    break;
                }
                utf8StringSink.clear();
                int varcharLen = strLen > 0 ? nextVariableColumnLen(rnd) : 0;
                rnd.nextUtf8Str(varcharLen, utf8StringSink);
                row.putVarchar(columnIndex, utf8StringSink);
                break;

            case ColumnType.STRING:
                row.putStr(columnIndex, isNull ? null : strLen == 0 ? "" : rnd.nextString(nextVariableColumnLen(rnd)));
                break;

            case ColumnType.BINARY:
                int len = strLen > 0 ? nextVariableColumnLen(rnd) : 0;
                row.putBin(columnIndex, isNull ? null : binarySequence.of(len == 0 ? new byte[0] : rnd.nextBytes(len)));
                break;

            case ColumnType.GEOBYTE:
            case ColumnType.GEOSHORT:
            case ColumnType.GEOINT:
            case ColumnType.GEOLONG:
                row.putGeoHash(columnIndex, rnd.nextLong());
                break;
            case ColumnType.UUID:
                row.putLong128(columnIndex, rnd.nextLong(), rnd.nextLong());
                break;
            case ColumnType.ARRAY:
                DirectArray array = tlArray.get();
                if (rnd.nextPositiveInt() % 4 == 0) {
                    array.ofNull();
                    row.putArray(columnIndex, array);
                } else {
                    rnd.nextDoubleArray(ColumnType.decodeArrayDimensionality(type), array, 1, 8, 0);
                    row.putArray(columnIndex, array);
                }
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }
}
