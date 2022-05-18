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

package io.questdb.cairo;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogRecord;
import io.questdb.std.str.CharSink;
import io.questdb.test.tools.TestUtils;

public class RecordCursorPrinter {
    protected final char delimiter;
    private boolean printTypes;

    public RecordCursorPrinter() {
        this.delimiter = '\t';
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

    public void printFullColumn(RecordCursor cursor, RecordMetadata metadata, int i, boolean header, CharSink sink) {
        if (header) {
            printHeader(metadata, sink);
        }

        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            TestUtils.printColumn(record, metadata, i, sink, printTypes);
            sink.put('\n');
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

    public void printRecordNoNl(Record r, RecordMetadata m, CharSink sink) {
        for (int i = 0, sz = m.getColumnCount(); i < sz; i++) {
            if (i > 0) {
                sink.put(delimiter);
            }
            TestUtils.printColumn(r, m, i, sink, printTypes);
        }
    }

    public RecordCursorPrinter withTypes(boolean enabled) {
        this.printTypes = enabled;
        return this;
    }
}
