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

import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.line.tcp.ColumnNameType;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.StringSink;

import java.util.List;

public class LineData {
    private final long timestampNanos;

    // column/tag name and value pairs for each line
    // column/tag name can be different to real name (dupes, uppercase...)
    private final ObjList<CharSequence> names = new ObjList<>();
    private final ObjList<CharSequence> values = new ObjList<>();
    private final BoolList tagFlags = new BoolList();

    private final LowerCaseCharSequenceIntHashMap nameToIndex = new LowerCaseCharSequenceIntHashMap();
    private int tagsCount;

    public LineData(long timestampMicros) {
        timestampNanos = timestampMicros * 1000;
        final StringSink timestampSink = new StringSink();
        TimestampFormatUtils.appendDateTimeUSec(timestampSink, timestampMicros);
        addColumn("timestamp", timestampSink);
    }

    public void addColumn(CharSequence colName, CharSequence colValue) {
        if (tagsCount == 0) {
            tagsCount = names.size();
        }
        add(colName, colValue, false);
    }

    public long getTimestamp() {
        return timestampNanos;
    }

    public void addTag(CharSequence tagName, CharSequence tagValue) {
        add(tagName, tagValue, true);
    }

    public String generateRandomUpdate(CharSequence tableName, List<ColumnNameType> metadata, Rnd rnd) {
        int columnType;
        CharSequence name;
        int fieldIndex;

        OUT:
        while (true) {
            for (int i = 0; i < metadata.size(); i++) {
                name = metadata.get(i).columnName;
                columnType = metadata.get(i).columnType;

                for (int j = 0; j < names.size(); j++) {
                    fieldIndex = j;
                    if (Chars.equals(name, names.getQuick(j))) {
                        break OUT;
                    }
                }
            }
        }

        double value = 500.0 + rnd.nextInt(1000);
        String valueStr = String.valueOf(value);
        values.set(fieldIndex, valueStr);

        boolean quote = columnType == ColumnType.STRING || columnType == ColumnType.SYMBOL;
        if (quote) {
            return String.format("update \"%s\" set %s='%s' where timestamp='%s'", tableName, names.getQuick(fieldIndex), valueStr, TimestampsToString(timestampNanos / 1000));
        } else {
            return String.format("update \"%s\" set %s=%s where timestamp='%s'", tableName, names.getQuick(fieldIndex), valueStr, TimestampsToString(timestampNanos / 1000));
        }
    }

    private String TimestampsToString(long uSecs) {
        StringSink sink = Misc.getThreadLocalBuilder();
        TimestampFormatUtils.USEC_UTC_FORMAT.format(uSecs, null, null, sink);
        return sink.toString();
    }

    private void add(CharSequence name, CharSequence value, boolean isTag) {
        names.add(name);
        values.add(value);
        tagFlags.add(isTag);
        nameToIndex.putIfAbsent(name, names.size() - 1);
    }

    public String toLine(final CharSequence tableName) {
        final StringBuilder sb = new StringBuilder();
        sb.append(tableName);
        return toString(sb).append("\n").toString();
    }

    StringBuilder toString(final StringBuilder sb) {
        for (int i = 0, n = names.size(); i < n; i++) {
            if (tagFlags.get(i)) {
                sb.append(",").append(names.get(i)).append("=").append(values.get(i));
            }
        }

        boolean firstColumn = true;
        for (int i = 0, n = names.size(); i < n; i++) {
            if (!tagFlags.get(i)) {
                final CharSequence colName = names.get(i);
                if (colName.equals("timestamp")) {
                    continue;
                }
                if (firstColumn) {
                    sb.append(" ");
                    firstColumn = false;
                }
                sb.append(colName).append("=").append(values.get(i)).append(",");
            }
        }
        if (!firstColumn) {
            sb.setLength(sb.length() - 1);
        }
        return sb.append(" ").append(timestampNanos);
    }

    CharSequence getRow(ObjList<CharSequence> columns, ObjList<CharSequence> defaults) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final CharSequence colName = columns.get(i);
            final int index = nameToIndex.get(colName);
            sb.append(index < 0 ? defaults.get(i) : getValue(values.get(index))).append(i == n - 1 ? "\n" : "\t");
        }
        return sb.toString();
    }

    private CharSequence getValue(CharSequence original) {
        if (original.charAt(0) != '"') {
            return original;
        }
        return original.toString().substring(1, original.length() - 1);
    }

    boolean isValid() {
        for (int i = 0, n = values.size(); i < n; i++) {
            if (tagFlags.get(i) && values.get(i).toString().contains(" ")) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }
}
