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

package io.questdb.cutlass.line.tcp.fuzzer;

import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.StringSink;

public class LineData {
    // colName/value pairs for each line
    // colName can be different to real colName (dupes, uppercase, etc...)
    private final long timestampNanos;
    private final ObjList<CharSequence> colNames = new ObjList<>();
    private final CharSequenceObjHashMap<CharSequence> colValues = new CharSequenceObjHashMap<>();

    public LineData(long timestampMicros) {
        timestampNanos = timestampMicros * 1000;
        final StringSink timestampSink = new StringSink();
        TimestampFormatUtils.appendDateTimeUSec(timestampSink, timestampMicros);
        add("timestamp", timestampSink);
    }

    public long getTimestamp() {
        return timestampNanos;
    }

    public void add(CharSequence colName, CharSequence colValue) {
        colNames.add(colName);
        colValues.put(colName, colValue);
    }

    public String toLine(final CharSequence tableName) {
        final StringBuilder sb = new StringBuilder();
        sb.append(tableName).append(" ");
        return toString(sb).append("\n").toString();
    }

    StringBuilder toString(final StringBuilder sb) {
        for (int i = 0, n = colNames.size(); i < n; i++) {
            final CharSequence colName = colNames.get(i);
            if (colName.equals("timestamp")) {
                continue;
            }
            sb.append(colName).append("=").append(colValues.get(colName)).append(i == n - 1 ? "" : ",");
        }
        return sb.append(" ").append(timestampNanos);
    }

    CharSequence getRow(ObjList<CharSequence> columns) {
        // work out duplicated/missing/extra columns while building the row
        final StringBuilder sb = new StringBuilder();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final CharSequence colName = columns.get(i);
            sb.append(colValues.get(colName)).append(i == n - 1 ? "\n" : "\t");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }
}
