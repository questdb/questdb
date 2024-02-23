/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogRecord;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.Uuid;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.MutableUtf16Sink;
import io.questdb.std.str.Utf16Sink;

import static io.questdb.std.Numbers.IPv4_NULL;

public class CursorPrinter {
    private static final char COLUMN_DELIMITER = '\t';

    public static void printColumn(Record r, RecordMetadata m, int columnIndex, Utf16Sink sink, boolean printTypes) {
        printColumn(r, m, columnIndex, sink, false, printTypes);
    }

    public static void printColumn(Record r, RecordMetadata m, int columnIndex, Utf16Sink sink, boolean symbolAsString, boolean printTypes) {
        printColumn(r, m, columnIndex, sink, symbolAsString, printTypes, null);
    }

    public static void printColumn(Record r, RecordMetadata m, int columnIndex, Utf16Sink sink, boolean symbolAsString, boolean printTypes, String nullStringValue) {
        final int columnType = m.getColumnType(columnIndex);
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.DATE:
                DateFormatUtils.appendDateTime(sink, r.getDate(columnIndex));
                break;
            case ColumnType.TIMESTAMP:
                TimestampFormatUtils.appendDateTimeUSec(sink, r.getTimestamp(columnIndex));
                break;
            case ColumnType.DOUBLE:
                sink.put(r.getDouble(columnIndex), Numbers.MAX_SCALE);
                break;
            case ColumnType.FLOAT:
                sink.put(r.getFloat(columnIndex), 4);
                break;
            case ColumnType.INT:
                sink.put(r.getInt(columnIndex));
                break;
            case ColumnType.NULL:
                sink.put("null");
                break;
            case ColumnType.STRING:
                if (!symbolAsString | m.getColumnType(columnIndex) != ColumnType.SYMBOL) {
                    sink.put(r.getStr(columnIndex));
                    break;
                } // Fall down to SYMBOL
            case ColumnType.SYMBOL:
                CharSequence sym = r.getSym(columnIndex);
                sink.put(sym != null ? sym : nullStringValue);
                break;
            case ColumnType.SHORT:
                sink.put(r.getShort(columnIndex));
                break;
            case ColumnType.CHAR:
                char c = r.getChar(columnIndex);
                if (c > 0) {
                    sink.put(c);
                }
                break;
            case ColumnType.LONG:
                sink.put(r.getLong(columnIndex));
                break;
            case ColumnType.GEOBYTE:
                putGeoHash(r.getGeoByte(columnIndex), ColumnType.getGeoHashBits(columnType), sink);
                break;
            case ColumnType.GEOSHORT:
                putGeoHash(r.getGeoShort(columnIndex), ColumnType.getGeoHashBits(columnType), sink);
                break;
            case ColumnType.GEOINT:
                putGeoHash(r.getGeoInt(columnIndex), ColumnType.getGeoHashBits(columnType), sink);
                break;
            case ColumnType.GEOLONG:
                putGeoHash(r.getGeoLong(columnIndex), ColumnType.getGeoHashBits(columnType), sink);
                break;
            case ColumnType.BYTE:
                // as int
                sink.put(r.getByte(columnIndex));
                break;
            case ColumnType.BOOLEAN:
                sink.put(r.getBool(columnIndex));
                break;
            case ColumnType.BINARY:
                Chars.toSink(r.getBin(columnIndex), sink);
                break;
            case ColumnType.LONG256:
                r.getLong256(columnIndex, sink);
                break;
            case ColumnType.LONG128:
                // fall through
            case ColumnType.UUID:
                long hi = r.getLong128Hi(columnIndex);
                long lo = r.getLong128Lo(columnIndex);
                if (!Uuid.isNull(lo, hi)) {
                    Uuid uuid = new Uuid(lo, hi);
                    uuid.toSink(sink);
                }
                break;
            case ColumnType.IPv4: {
                final int val = r.getIPv4(columnIndex);
                if (val != IPv4_NULL) {
                    Numbers.intToIPv4Sink(sink, val);
                }
                break;
            }
            default:
                break;
        }
        if (printTypes) {
            int printColType = symbolAsString && ColumnType.isSymbol(columnType) ? ColumnType.STRING : columnType;
            sink.put(':').put(ColumnType.nameOf(printColType));
        }
    }

    public static void printColumn(Record r, RecordMetadata m, int i, Utf16Sink sink) {
        printColumn(r, m, i, sink, false, false);
    }

    public static void printHeader(RecordMetadata metadata, Utf16Sink sink) {
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

    public static void println(Record r, RecordMetadata m, Utf16Sink sink) {
        println(r, m, sink, false);
    }

    public static void println(Record r, RecordMetadata m, Utf16Sink sink, boolean printTypes) {
        print(r, m, sink, printTypes);
        sink.putAscii("\n");
    }

    public static void println(RecordMetadata metadata, Utf16Sink sink) {
        printHeader(metadata, sink);
        sink.putAscii('\n');
    }

    public static void println(RecordCursor cursor, RecordMetadata metadata, MutableUtf16Sink sink) {
        println(cursor, metadata, sink, true, false);
    }

    public static void println(RecordCursor cursor, RecordMetadata metadata, MutableUtf16Sink sink, boolean printHeader, boolean printTypes) {
        sink.clear();
        if (printHeader) {
            println(metadata, sink);
        }

        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            println(record, metadata, sink, printTypes);
        }
    }

    private static void print(Record r, RecordMetadata m, Utf16Sink sink, boolean printTypes) {
        for (int i = 0, sz = m.getColumnCount(); i < sz; i++) {
            if (i > 0) {
                sink.put(COLUMN_DELIMITER);
            }
            printColumn(r, m, i, sink, printTypes);
        }
    }

    private static void putGeoHash(long hash, int bits, Utf16Sink sink) {
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
