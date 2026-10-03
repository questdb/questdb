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

package io.questdb.cairo;

import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.NoopArrayWriteState;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogError;
import io.questdb.log.LogRecord;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.Decimals;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.Misc;
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
        final boolean notNull = metadata.isNotNull(columnIndex);
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.DATE: {
                long date = record.getDate(columnIndex);
                if (notNull && date == Long.MIN_VALUE) {
                    Numbers.append(sink, date, false);
                } else {
                    DateFormatUtils.appendDateTime(sink, date);
                }
                break;
            }
            case ColumnType.TIMESTAMP: {
                long ts = record.getTimestamp(columnIndex);
                if (notNull && ts == Long.MIN_VALUE) {
                    Numbers.append(sink, ts, false);
                } else {
                    ColumnType.getTimestampDriver(columnType).append(sink, ts);
                }
                break;
            }
            case ColumnType.DOUBLE:
                double v = record.getDouble(columnIndex);
                if (notNull || Numbers.isFinite(v)) {
                    sink.put(v);
                } else {
                    sink.put("null");
                }
                break;
            case ColumnType.FLOAT:
                float f = record.getFloat(columnIndex);
                if (notNull || Numbers.isFinite(f)) {
                    sink.put(f);
                } else {
                    sink.put("null");
                }
                break;
            case ColumnType.INT:
                if (notNull) {
                    Numbers.append(sink, (long) record.getInt(columnIndex), false);
                } else {
                    sink.put(record.getInt(columnIndex));
                }
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
                if (notNull || c > 0) {
                    sink.put(c);
                }
                break;
            case ColumnType.LONG:
                if (notNull) {
                    Numbers.append(sink, record.getLong(columnIndex), false);
                } else {
                    sink.put(record.getLong(columnIndex));
                }
                break;
            case ColumnType.GEOBYTE:
                putGeoHash(record.getGeoByte(columnIndex), ColumnType.getGeoHashBits(columnType), sink, notNull);
                break;
            case ColumnType.GEOSHORT:
                putGeoHash(record.getGeoShort(columnIndex), ColumnType.getGeoHashBits(columnType), sink, notNull);
                break;
            case ColumnType.GEOINT:
                putGeoHash(record.getGeoInt(columnIndex), ColumnType.getGeoHashBits(columnType), sink, notNull);
                break;
            case ColumnType.GEOLONG:
                putGeoHash(record.getGeoLong(columnIndex), ColumnType.getGeoHashBits(columnType), sink, notNull);
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
                if (notNull) {
                    Long256 l256 = record.getLong256A(columnIndex);
                    Numbers.appendLong256(l256.getLong0(), l256.getLong1(), l256.getLong2(), l256.getLong3(), sink, false);
                } else {
                    record.getLong256(columnIndex, sink);
                }
                break;
            case ColumnType.LONG128:
                // fall through
            case ColumnType.UUID: {
                long hi = record.getLong128Hi(columnIndex);
                long lo = record.getLong128Lo(columnIndex);
                if (notNull || !Uuid.isNull(lo, hi)) {
                    Numbers.appendUuid(lo, hi, sink);
                }
                break;
            }
            case ColumnType.IPv4: {
                final int val = record.getIPv4(columnIndex);
                if (notNull || val != IPv4_NULL) {
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
                if (notNull || !Interval.NULL.equals(interval)) {
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
                break;
            case ColumnType.DECIMAL8:
                putDecimal8Value(sink, record, columnIndex, columnType, notNull);
                break;
            case ColumnType.DECIMAL16:
                putDecimal16Value(sink, record, columnIndex, columnType, notNull);
                break;
            case ColumnType.DECIMAL32:
                putDecimal32Value(sink, record, columnIndex, columnType, notNull);
                break;
            case ColumnType.DECIMAL64:
                putDecimal64Value(sink, record, columnIndex, columnType, notNull);
                break;
            case ColumnType.DECIMAL128:
                putDecimal128Value(sink, record, columnIndex, columnType, notNull);
                break;
            case ColumnType.DECIMAL256:
                putDecimal256Value(sink, record, columnIndex, columnType, notNull);
                break;
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
            try {
                line.$();
            } catch (LogError e) {
                // We're logging data we don't control, it could be invalid UTF-8.
                // Let's not break the test when this happens.
                sink.xDebugW().$("LogError: ").$(e.getMessage()).$();
            }
        }

        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            LogRecord line = sink.xDebugW();
            print(record, metadata, logRecSink.of(line), false);
            try {
                line.$();
            } catch (LogError e) {
                // We're logging data we don't control, it could be invalid UTF-8.
                // Let's not break the test when this happens.
                sink.xDebugW().$("LogError: ").$(e.getMessage()).$();
            }
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

    private static void putDecimal128Value(CharSink<?> sink, Record rec, int col, int type, boolean notNull) {
        var decimal = Misc.getThreadLocalDecimal128();
        rec.getDecimal128(col, decimal);
        if (notNull) {
            Decimals.appendNonNull(decimal, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), sink);
        } else if (!decimal.isNull()) {
            Decimals.append(decimal, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), sink);
        }
    }

    private static void putDecimal16Value(CharSink<?> sink, Record rec, int col, int type, boolean notNull) {
        short l = rec.getDecimal16(col);
        if (notNull || l != Decimals.DECIMAL16_NULL) {
            Decimals.append(l, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), sink);
        }
    }

    private static void putDecimal256Value(CharSink<?> sink, Record rec, int col, int type, boolean notNull) {
        var decimal = Misc.getThreadLocalDecimal256();
        rec.getDecimal256(col, decimal);
        if (notNull) {
            Decimals.appendNonNull(decimal, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), sink);
        } else if (!decimal.isNull()) {
            Decimals.append(decimal, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), sink);
        }
    }

    private static void putDecimal32Value(CharSink<?> sink, Record rec, int col, int type, boolean notNull) {
        int l = rec.getDecimal32(col);
        if (notNull || l != Decimals.DECIMAL32_NULL) {
            Decimals.append(l, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), sink);
        }
    }

    private static void putDecimal64Value(CharSink<?> sink, Record rec, int col, int type, boolean notNull) {
        long l = rec.getDecimal64(col);
        if (notNull) {
            Decimals.appendNonNull(l, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), sink);
        } else if (l != Decimals.DECIMAL64_NULL) {
            Decimals.append(l, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), sink);
        }
    }

    private static void putDecimal8Value(CharSink<?> sink, Record rec, int col, int type, boolean notNull) {
        byte l = rec.getDecimal8(col);
        if (notNull || l != Decimals.DECIMAL8_NULL) {
            Decimals.append(l, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), sink);
        }
    }

    private static void putGeoHash(long hash, int bits, CharSink<?> sink, boolean notNull) {
        if (!notNull && hash == GeoHashes.NULL) {
            return;
        }
        if (bits % 5 == 0) {
            GeoHashes.appendCharsUnsafe(hash, bits / 5, sink);
        } else {
            GeoHashes.appendBinaryStringUnsafe(hash, bits, sink);
        }
    }
}
