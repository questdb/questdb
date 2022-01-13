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

package io.questdb.cutlass.line.tcp.load;

import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.StringSink;

public class LineData {
    private final long timestampNanos;

    // colName/value pairs for each line
    // colName can be different to real colName (dupes, uppercase...)
    private final ObjList<CharSequence> colNames = new ObjList<>();
    private final ObjList<CharSequence> colValues = new ObjList<>();

    private final LowerCaseCharSequenceIntHashMap colNameToIndex = new LowerCaseCharSequenceIntHashMap();

    public LineData(long timestampMicros) {
        timestampNanos = timestampMicros * 1000;
        final StringSink timestampSink = new StringSink();
        TimestampFormatUtils.appendDateTimeUSec(timestampSink, timestampMicros);
        add("timestamp", timestampSink);
    }

    public void add(CharSequence colName, CharSequence colValue) {
        colNames.add(colName);
        colValues.add(colValue);
        colNameToIndex.putIfAbsent(colName, colNames.size() - 1);
    }

    public long getTimestamp() {
        return timestampNanos;
    }

    public String toLine(final CharSequence tableName) {
        final StringBuilder sb = new StringBuilder();
        sb.append(tableName).append(" ");
        return toString(sb).append("\n").toString();
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    CharSequence getRow(ObjList<CharSequence> columns, ObjList<CharSequence> defaults) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final CharSequence colName = columns.get(i);
            final int index = colNameToIndex.get(colName);
            sb.append(index < 0 ? defaults.get(i) : colValues.get(index)).append(i == n - 1 ? "\n" : "\t");
        }
        return sb.toString();
    }

    StringBuilder toString(final StringBuilder sb) {
        for (int i = 0, n = colNames.size(); i < n; i++) {
            final CharSequence colName = colNames.get(i);
            if (colName.equals("timestamp")) {
                continue;
            }
            sb.append(colName).append("=").append(colValues.get(i)).append(i == n - 1 ? "" : ",");
        }
        return sb.append(" ").append(timestampNanos);
    }
}
