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

package io.questdb.guiutil;

import io.questdb.cairo.ColumnType;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import java.util.concurrent.TimeUnit;

class MessageSink {
    private static final DateFormat DATE_FORMATTER = new TimestampFormatCompiler().compile(
            "yyyy-MM-ddTHH:mm:ss.SSSz"
    );

    static {
        // preload, which compiles the pattern and is costly, penalising startup time
        DATE_FORMATTER.format(0, null, "Z", new StringSink());
    }

    private final StringSink sink = new StringSink();

    void failedToOpenFile(Path file, Throwable error) {
        sink.clear();
        sink.put("Failed to open [").put(file)
                .put("]: ").put(error.getMessage())
                .put(System.lineSeparator());
    }

    void clear() {
        sink.clear();
    }

    @Override
    public String toString() {
        return sink.toString();
    }

    void addLn() {
        sink.put(System.lineSeparator());
    }

    void addLn(String name, int value) {
        sink.put(name).put(value).put(System.lineSeparator());
    }

    void addLn(String name, long value) {
        sink.put(name).put(value).put(System.lineSeparator());
    }

    void addLn(String name, boolean value) {
        sink.put(name).put(value).put(System.lineSeparator());
    }

    void addLn(String name, String value) {
        sink.put(name).put(value).put(System.lineSeparator());
    }

    void addIndexedSymbolLn(int index, CharSequence value, boolean indented) {
        if (indented) {
            sink.put(" - ");
        }
        sink.put(index).put(": ").put(value).put(System.lineSeparator());
    }

    void addTimeLn(String name, long value) {
        sink.put(name).put(value).put(" micros (")
                .put(TimeUnit.MICROSECONDS.toSeconds(value))
                .put(" sec, or ")
                .put(TimeUnit.MICROSECONDS.toMinutes(value))
                .put(" min)")
                .put(System.lineSeparator());
    }

    void addTimestampLn(String name, long timestamp) {
        sink.put(name).put(timestamp).put(" (");
        DATE_FORMATTER.format(timestamp, null, "Z", sink);
        sink.put(')').put(System.lineSeparator());
    }

    void addPartitionLn(
            int partitionIndex,
            long partitionTimestamp,
            long partitionNameTxn,
            long partitionSize,
            long partitionColumnVersion,
            long partitionSymbolValueCount
    ) {
        sink.put(" - partition ").put(partitionIndex)
                .put(" -> txn: ").put(partitionNameTxn)
                .put(", column version: ").put(partitionColumnVersion)
                .put(", size: ").put(partitionSize)
                .put(", symbol value count: ").put(partitionSymbolValueCount)
                .put(", timestamp: ").put(partitionTimestamp).put(" (");
        DATE_FORMATTER.format(partitionTimestamp, null, "Z", sink);
        sink.put(')').put(System.lineSeparator());
    }


    void addColumnLn(
            int columnIndex,
            CharSequence columnName,
            long columnHash,
            int columnType,
            boolean columnIsIndexed,
            int columnIndexBlockCapacity,
            boolean indented
    ) {
        if (indented) {
            sink.put(" - ");
        }
        sink.put("column ").put(columnIndex)
                .put(" -> name: ").put(columnName)
                .put(", type: ").put(ColumnType.nameOf(columnType))
                .put(", hash: ").put(columnHash)
                .put(", indexed: ").put(columnIsIndexed)
                .put(", indexBlockCapacity: ").put(columnIndexBlockCapacity)
                .put(System.lineSeparator());
    }
}
