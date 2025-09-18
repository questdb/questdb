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

package io.questdb.cairo;

import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.NoopArrayWriteState;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogRecord;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.Interval;
import io.questdb.std.Numbers;
import io.questdb.std.Uuid;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.MutableCharSink;
import io.questdb.std.str.Utf8Sequence;

import static io.questdb.std.Numbers.IPv4_NULL;

public class CursorPrinter {
    private static final char COLUMN_DELIMITER = '\t';

    public static void printColumn(Record r, RecordMetadata m, int columnIndex, CharSink<?> sink, boolean printTypes) {
        printColumn(r, m, columnIndex, sink, false, printTypes);
    }

    public static void printColumn(Record record, RecordMetadata metadata, int columnIndex, CharSink<?> sink, boolean symbolAsString, boolean printTypes) {
        printColumn(record, metadata, columnIndex, sink, symbolAsString, printTypes, null);
    }

    public static void printColumn(Record record, RecordMetadata metadata, int columnIndex, CharSink<?> sink, boolean symbolAsString, boolean printTypes, String nullStringValue) {
        final int columnType = metadata.getColumnType(columnIndex);
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.DATE:
                DateFormatUtils.appendDateTime(sink, record.getDate(columnIndex));
                break;
            case ColumnType.TIMESTAMP:
                ColumnType.getTimestampDriver(columnType).append(sink, record.getTimestamp(columnIndex));
                break;
            case ColumnType.DOUBLE:
                double v = record.getDouble(columnIndex);
                if (Numbers.isFinite(v)) {
                    sink.put(v);
                } else {
                    sink.put("null");
                }
                break;
            case ColumnType.FLOAT:
                float f = record.getFloat(columnIndex);
                if (Numbers.isFinite(f)) {
                    sink.put(f);
                } else {
                    sink.put("null");
                }
                break;
            case ColumnType.INT:
                sink.put(record.getInt(columnIndex));
                break;
            case ColumnType.NULL:
                sink.put("null");
                break;
            case ColumnType.STRING:
                if (!symbolAsString | metadata.getColumnType(columnIndex) != ColumnType.SYMBOL) {
                    CharSequence val = record.getStrA(columnIndex);
                    sink.put(val != null ? val : nullStringValue);
                    break;
                } // Fall down to SYMBOL
            case ColumnType.SYMBOL:
                CharSequence sym = record.getSymA(columnIndex);
                sink.put(sym != null ? sym : nullStringValue);
                break;
            case ColumnType.SHORT:
                sink.put(record.getShort(columnIndex));
                break;
            case ColumnType.CHAR:
                char c = record.getChar(columnIndex);
                if (c > 0) {
                    sink.put(c);
                }
                break;
            case ColumnType.LONG:
                sink.put(record.getLong(columnIndex));
                break;
            case ColumnType.GEOBYTE:
                putGeoHash(record.getGeoByte(columnIndex), ColumnType.getGeoHashBits(columnType), sink);
                break;
            case ColumnType.GEOSHORT:
                putGeoHash(record.getGeoShort(columnIndex), ColumnType.getGeoHashBits(columnType), sink);
                break;
            case ColumnType.GEOINT:
                putGeoHash(record.getGeoInt(columnIndex), ColumnType.getGeoHashBits(columnType), sink);
                break;
            case ColumnType.GEOLONG:
                putGeoHash(record.getGeoLong(columnIndex), ColumnType.getGeoHashBits(columnType), sink);
                break;
            case ColumnType.BYTE:
                // as int
                sink.put(record.getByte(columnIndex));
                break;
            case ColumnType.BOOLEAN:
                sink.put(record.getBool(columnIndex));
                break;
            case ColumnType.BINARY:
                BinarySequence bin = record.getBin(columnIndex);
                if (bin != null) {
                    Chars.toSink(bin, sink);
                } else {
                    sink.put(nullStringValue);
                }
                break;
            case ColumnType.LONG256:
                record.getLong256(columnIndex, sink);
                break;
            case ColumnType.LONG128:
                // fall through
            case ColumnType.UUID:
                long hi = record.getLong128Hi(columnIndex);
                long lo = record.getLong128Lo(columnIndex);
                if (!Uuid.isNull(lo, hi)) {
                    Uuid uuid = new Uuid(lo, hi);
                    uuid.toSink(sink);
                }
                break;
            case ColumnType.IPv4: {
                final int val = record.getIPv4(columnIndex);
                if (val != IPv4_NULL) {
                    Numbers.intToIPv4Sink(sink, val);
                }
                break;
            }
            case ColumnType.VARCHAR:
                Utf8Sequence varchar = record.getVarcharA(columnIndex);
                if (varchar != null) {
                    sink.put(varchar);
                } else {
                    sink.put(nullStringValue);
                }
                break;
            case ColumnType.INTERVAL:
                Interval interval = record.getInterval(columnIndex);
                if (!Interval.NULL.equals(interval)) {
                    interval.toSink(sink, columnType);
                }
                break;
            case ColumnType.ARRAY:
                ArrayTypeDriver.arrayToJson(
                        record.getArray(columnIndex, columnType),
                        sink,
                        NoopArrayWriteState.INSTANCE
                );
                break;
            case ColumnType.ARRAY_STRING:
                sink.put(record.getStrA(columnIndex));
            default:
                break;
        }
        if (printTypes) {
            int printColType = symbolAsString && ColumnType.isSymbol(columnType) ? ColumnType.STRING : columnType;
            sink.put(':').put(ColumnType.nameOf(printColType));
        }
    }

    public static void printColumn(Record record, RecordMetadata metadata, int columnIndex, CharSink<?> sink) {
        printColumn(record, metadata, columnIndex, sink, false, false);
    }

    public static void printHeader(RecordMetadata metadata, CharSink<?> sink) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (i > 0) {
                sink.put(COLUMN_DELIMITER);
            }
            sink.put(metadata.getColumnName(i));
        }
    }

    public static void println(RecordCursor cursor, RecordMetadata metadata, boolean printHeader, Log sink) {
        LogRecordSinkAdapter logRecSink = new LogRecordSinkAdapter();
        if (printHeader) {
            LogRecord line = sink.xDebugW();
            printHeader(metadata, logRecSink.of(line));
            line.$();
        }

        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            LogRecord line = sink.xDebugW();
            print(record, metadata, logRecSink.of(line), false);
            line.$();
        }
    }

    public static void println(Record record, RecordMetadata metadata, CharSink<?> sink) {
        println(record, metadata, sink, false);
    }

    public static void println(Record r, RecordMetadata m, CharSink<?> sink, boolean printTypes) {
        print(r, m, sink, printTypes);
        sink.putAscii('\n');
    }

    public static void println(RecordMetadata metadata, CharSink<?> sink) {
        printHeader(metadata, sink);
        sink.putAscii('\n');
    }

    public static void println(RecordCursor cursor, RecordMetadata metadata, MutableCharSink<?> sink) {
        println(cursor, metadata, sink, true, false);
    }

    public static void println(RecordCursor cursor, RecordMetadata metadata, MutableCharSink<?> sink, boolean printHeader, boolean printTypes) {
        sink.clear();
        if (printHeader) {
            println(metadata, sink);
        }

        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            println(record, metadata, sink, printTypes);
        }
    }

    private static void print(Record record, RecordMetadata metadata, CharSink<?> sink, boolean printTypes) {
        for (int i = 0, sz = metadata.getColumnCount(); i < sz; i++) {
            if (i > 0) {
                sink.put(COLUMN_DELIMITER);
            }
            printColumn(record, metadata, i, sink, printTypes);
        }
    }

    private static void putGeoHash(long hash, int bits, CharSink<?> sink) {
        if (hash == GeoHashes.NULL) {
            return;
        }
        if (bits % 5 == 0) {
            GeoHashes.appendCharsUnsafe(hash, bits / 5, sink);
        } else {
            GeoHashes.appendBinaryStringUnsafe(hash, bits, sink);
        }
    }
}
