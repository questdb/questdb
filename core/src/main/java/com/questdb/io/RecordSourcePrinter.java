/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.io;

import com.questdb.ex.JournalException;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Dates;
import com.questdb.misc.Numbers;
import com.questdb.ql.Record;
import com.questdb.ql.RecordSource;
import com.questdb.std.CharSink;
import com.questdb.std.ImmutableIterator;

import java.io.IOException;

public class RecordSourcePrinter {
    private final CharSink sink;
    private final char delimiter;

    public RecordSourcePrinter(CharSink sink) {
        this.sink = sink;
        this.delimiter = '\t';
    }

    public RecordSourcePrinter(CharSink sink, char delimiter) {
        this.sink = sink;
        this.delimiter = delimiter;
    }

    public void printCursor(RecordSource src, JournalReaderFactory factory) throws IOException, JournalException {
        printCursor(src.prepareCursor(factory), false, src.getMetadata());
    }

    public void printCursor(RecordSource src, JournalReaderFactory factory, boolean header) throws IOException, JournalException {
        printCursor(src.prepareCursor(factory), header, src.getMetadata());
    }

    public void printCursor(ImmutableIterator<Record> src, boolean header, RecordMetadata metadata) throws IOException {
        if (header) {
            printHeader(metadata);
        }

        while (src.hasNext()) {
            print(src.next(), metadata);
        }
    }

    private void print(Record r, RecordMetadata m) throws IOException {
        for (int i = 0, sz = m.getColumnCount(); i < sz; i++) {
            if (i > 0) {
                sink.put(delimiter);
            }
            printColumn(r, m, i);
        }
        sink.put("\n");
        sink.flush();
    }

    private void printColumn(Record r, RecordMetadata m, int i) {
        switch (m.getColumnQuick(i).getType()) {
            case DATE:
                Dates.appendDateTime(sink, r.getDate(i));
                break;
            case DOUBLE:
                Numbers.append(sink, r.getDouble(i), 12);
                break;
            case FLOAT:
                Numbers.append(sink, r.getFloat(i), 4);
                break;
            case INT:
                Numbers.append(sink, r.getInt(i));
                break;
            case STRING:
                r.getStr(i, sink);
                break;
            case SYMBOL:
                sink.put(r.getSym(i));
                break;
            case SHORT:
                Numbers.append(sink, r.getShort(i));
                break;
            case LONG:
                Numbers.append(sink, r.getLong(i));
                break;
            case BYTE:
                Numbers.append(sink, r.get(i));
                break;
            case BOOLEAN:
                sink.put(r.getBool(i) ? "true" : "false");
                break;
            default:
                break;
        }
    }

    private void printHeader(RecordMetadata metadata) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (i > 0) {
                sink.put(delimiter);
            }
            sink.put(metadata.getColumnName(i));
        }
        sink.put('\n');
    }
}
