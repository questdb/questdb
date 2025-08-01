/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.std.ThreadLocal;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

public class LineProtocolException extends CairoException {
    private static final ThreadLocal<LineProtocolException> tlException = new ThreadLocal<>(LineProtocolException::new);

    public static LineProtocolException boundsError(long entityValue, int colType, CharSequence tableNameUtf16, CharSequence columnName) {
        return instance()
                .put("table: ").put(tableNameUtf16)
                .put(", column: ").put(columnName)
                .put("; line protocol value: ").put(entityValue)
                .put(" is out bounds of column type: ").put(ColumnType.nameOf(colType));
    }

    public static LineProtocolException boundsError(long entityValue, int colType, CharSequence tableNameUtf16, int columnIndex) {
        return instance()
                .put("table: ").put(tableNameUtf16)
                .put(", column: ").put(columnIndex)
                .put("; line protocol value: ").put(entityValue)
                .put(" is out bounds of column type: ").put(ColumnType.nameOf(colType));
    }

    public static LineProtocolException castError(String tableNameUtf16, String ilpType, int colType, DirectUtf8Sequence columnName) {
        LineProtocolException instance = instance();
        instance
                .put("table: ").put(tableNameUtf16)
                .put(", column: ").put(columnName)
                .put("; cast error from protocol type: ").put(ilpType)
                .put(" to column type: ").put(ColumnType.nameOf(colType));
        if (colType <= 0) {
            instance.put('(').put(colType).put(')');
        }
        return instance;
    }

    public static LineProtocolException designatedTimestampMustBePositive(String tableNameUtf16, long timestamp) {
        return instance()
                .put("table: ").put(tableNameUtf16)
                .put(", timestamp: ").put(timestamp)
                .put("; designated timestamp before 1970-01-01 is not allowed");
    }

    public static LineProtocolException designatedTimestampValueOverflow(String tableNameUtf16, long timestamp) {
        return instance()
                .put("table: ").put(tableNameUtf16)
                .put(", timestamp: ").put(timestamp)
                .put("; designated timestamp overflow, max[")
                .put(CommonUtils.MAX_TIMESTAMP)
                .put("]");
    }

    public static LineProtocolException invalidColNameError(CharSequence columnName, String tableNameUtf16) {
        return instance()
                .put("table: ").put(tableNameUtf16)
                .put("; invalid column name: ").put(columnName);
    }

    public static LineProtocolException newColumnsNotAllowed(String columnName, String tableNameUtf16) {
        return instance()
                .put("table: ").put(tableNameUtf16)
                .put(", column: ").put(columnName)
                .put(" does not exist, creating new columns is disabled");
    }

    public LineProtocolException put(@Nullable Utf8Sequence us) {
        message.put(us);
        return this;
    }

    public LineProtocolException put(@Nullable CharSequence cs) {
        message.put(cs);
        return this;
    }

    public LineProtocolException put(long value) {
        message.put(value);
        return this;
    }

    public LineProtocolException put(char c) {
        message.put(c);
        return this;
    }

    private static LineProtocolException instance() {
        LineProtocolException ex = tlException.get();
        // This is to have correct stack trace in local debugging with -ea option
        assert (ex = new LineProtocolException()) != null;
        ex.clear(NON_CRITICAL);
        return ex;
    }
}
