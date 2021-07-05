/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.CharSink;

public class RecordCursorPrinter {
    protected final char delimiter;
    private boolean printTypes;

    public RecordCursorPrinter() {
        this.delimiter = '\t';
    }

    public RecordCursorPrinter withTypes(boolean enabled) {
        this.printTypes = enabled;
        return this;
    }

    public void print(RecordCursor cursor, RecordMetadata metadata, boolean header, CharSink sink) {
        if (header) {
            printHeader(metadata, sink);
        }

        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            print(record, metadata, sink);
        }
    }

    public void print(RecordCursor cursor, RecordMetadata metadata, boolean header, Log sink) {
        LogRecordSinkAdapter logRecSink = new LogRecordSinkAdapter();
        if (header) {
            LogRecord line = sink.xDebugW();
            printHeaderNoNl(metadata, logRecSink.of(line));
            line.$();
        }

        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            LogRecord line = sink.xDebugW();
            printRecordNoNl(record, metadata, logRecSink.of(line));
            line.$();
        }
    }

    public void print(Record r, RecordMetadata m, CharSink sink) {
        printRecordNoNl(r, m, sink);
        sink.put("\n");
        sink.flush();
    }

    public void printRecordNoNl(Record r, RecordMetadata m, CharSink sink) {
        for (int i = 0, sz = m.getColumnCount(); i < sz; i++) {
            if (i > 0) {
                sink.put(delimiter);
            }
            printColumn(r, m, i, sink);
        }
    }

    public void printHeader(RecordMetadata metadata, CharSink sink) {
        printHeaderNoNl(metadata, sink);
        sink.put('\n');
    }

    public void printHeaderNoNl(RecordMetadata metadata, CharSink sink) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (i > 0) {
                sink.put(delimiter);
            }
            sink.put(metadata.getColumnName(i));
        }
    }

    public void printFullColumn(RecordCursor cursor, RecordMetadata metadata, int i, boolean header, CharSink sink) {
        if (header) {
            printHeader(metadata, sink);
        }

        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            printColumn(record, metadata, i, sink);
            sink.put('\n');
        }
    }

    protected void printColumn(Record r, RecordMetadata m, int i, CharSink sink) {
        switch (m.getColumnType(i)) {
            case ColumnType.DATE:
                DateFormatUtils.appendDateTime(sink, r.getDate(i));
                break;
            case ColumnType.TIMESTAMP:
                TimestampFormatUtils.appendDateTimeUSec(sink, r.getTimestamp(i));
                break;
            case ColumnType.DOUBLE:
                sink.put(r.getDouble(i), Numbers.MAX_SCALE);
                break;
            case ColumnType.FLOAT:
                sink.put(r.getFloat(i), 4);
                break;
            case ColumnType.INT:
                sink.put(r.getInt(i));
                break;
            case ColumnType.NULL:
                sink.put("null");
                break;
            case ColumnType.STRING:
                r.getStr(i, sink);
                break;
            case ColumnType.SYMBOL:
                sink.put(r.getSym(i));
                break;
            case ColumnType.SHORT:
                sink.put(r.getShort(i));
                break;
            case ColumnType.CHAR:
                char c = r.getChar(i);
                if (c > 0) {
                    sink.put(c);
                }
                break;
            case ColumnType.LONG:
                sink.put(r.getLong(i));
                break;
            case ColumnType.BYTE:
                sink.put(r.getByte(i));
                break;
            case ColumnType.BOOLEAN:
                sink.put(r.getBool(i));
                break;
            case ColumnType.BINARY:
                Chars.toSink(r.getBin(i), sink);
                break;
            case ColumnType.LONG256:
                r.getLong256(i, sink);
                break;
            default:
                break;
        }
        if (printTypes) {
            sink.put(':').put(ColumnType.nameOf(m.getColumnType(i)));
        }
    }
}
