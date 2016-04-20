/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 ******************************************************************************/

package com.questdb.io;

import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.io.sink.CharSink;
import com.questdb.misc.Dates;
import com.questdb.misc.Numbers;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;

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

    public void printCursor(RecordCursor src) throws IOException {
        printCursor(src, false);
    }

    public void printCursor(RecordCursor src, boolean header) throws IOException {
        RecordMetadata metadata = src.getMetadata();
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
            printRecord(r, m, i);
        }
        sink.put("\n");
        sink.flush();
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

    private void printRecord(Record r, RecordMetadata m, int i) {
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
}
