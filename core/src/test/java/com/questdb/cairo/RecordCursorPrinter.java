/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.common.ColumnType;
import com.questdb.std.Chars;
import com.questdb.std.Numbers;
import com.questdb.std.str.CharSink;
import com.questdb.std.time.DateFormatUtils;

import java.io.IOException;

public class RecordCursorPrinter {
    private final CharSink sink;
    private final char delimiter;

    public RecordCursorPrinter(CharSink sink) {
        this.sink = sink;
        this.delimiter = '\t';
    }

    public void print(RecordCursor cursor, RecordMetadata metadata, boolean header) throws IOException {
        if (header) {
            printHeader(metadata);
        }

        while (cursor.hasNext()) {
            print(cursor.next(), metadata);
        }
    }

    public void print(Record r, RecordMetadata m) throws IOException {
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

    private void printColumn(Record r, RecordMetadata m, int i) {
        switch (m.getColumnType(i)) {
            case ColumnType.DATE:
                DateFormatUtils.appendDateTime(sink, r.getDate(i));
                break;
            case ColumnType.TIMESTAMP:
                com.questdb.std.microtime.DateFormatUtils.appendDateTimeUSec(sink, r.getTimestamp(i));
                break;
            case ColumnType.DOUBLE:
                Numbers.append(sink, r.getDouble(i), 12);
                break;
            case ColumnType.FLOAT:
                Numbers.append(sink, r.getFloat(i), 4);
                break;
            case ColumnType.INT:
                Numbers.append(sink, r.getInt(i));
                break;
            case ColumnType.STRING:
                r.getStr(i, sink);
                break;
            case ColumnType.SYMBOL:
                sink.put(r.getSym(i));
                break;
            case ColumnType.SHORT:
                Numbers.append(sink, r.getShort(i));
                break;
            case ColumnType.LONG:
                Numbers.append(sink, r.getLong(i));
                break;
            case ColumnType.BYTE:
                Numbers.append(sink, r.getByte(i));
                break;
            case ColumnType.BOOLEAN:
                sink.put(r.getBool(i) ? "true" : "false");
                break;
            case ColumnType.BINARY:
                Chars.toSink(r.getBin(i), sink);
                break;
            default:
                break;
        }
    }
}
