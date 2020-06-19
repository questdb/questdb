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
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.microtime.TimestampFormatUtils;
import io.questdb.std.str.CharSink;
import io.questdb.std.time.DateFormatUtils;

public class RecordCursorPrinter {
    private final CharSink sink;
    protected final char delimiter;

    public RecordCursorPrinter(CharSink sink) {
        this.sink = sink;
        this.delimiter = '\t';
    }

    public void print(RecordCursor cursor, RecordMetadata metadata, boolean header) {
        if (header) {
            printHeader(metadata);
        }

        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            print(record, metadata);
        }
    }

    public void print(Record r, RecordMetadata m) {
        for (int i = 0, sz = m.getColumnCount(); i < sz; i++) {
            if (i > 0) {
                sink.put(delimiter);
            }
            printColumn(r, m, i);
        }
        sink.put("\n");
        sink.flush();
    }

    public void printHeader(RecordMetadata metadata) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (i > 0) {
                sink.put(delimiter);
            }
            sink.put(metadata.getColumnName(i));
        }
        sink.put('\n');
    }

    protected void printColumn(Record r, RecordMetadata m, int i) {
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
    }
}
