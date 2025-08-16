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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SqlException extends Exception implements Sinkable, FlyweightMessageContainer {
    private static final StackTraceElement[] EMPTY_STACK_TRACE = {};
    private static final int EXCEPTION_TABLE_DOES_NOT_EXIST = -105;
    private static final int EXCEPTION_MAT_VIEW_DOES_NOT_EXIST = EXCEPTION_TABLE_DOES_NOT_EXIST - 1;
    private static final int EXCEPTION_WAL_RECOVERABLE = EXCEPTION_MAT_VIEW_DOES_NOT_EXIST - 1;
    private static final ThreadLocal<SqlException> tlException = new ThreadLocal<>(SqlException::new);
    private final StringSink message = new StringSink();
    private int error;
    private int position;

    protected SqlException() {
    }

    public static SqlException $(int position, CharSequence message) {
        return position(position).put(message);
    }

    public static SqlException $(int position, long addrLo, long addrHi) {
        return position(position).put(addrLo, addrHi);
    }

    public static SqlException ambiguousColumn(int position, CharSequence columnName) {
        return position(position).put("Ambiguous column [name=").put(columnName).put(']');
    }

    public static SqlException duplicateColumn(int position, CharSequence colName) {
        return duplicateColumn(position, colName, null);
    }

    public static SqlException duplicateColumn(int position, CharSequence colName, CharSequence additionalMessage) {
        SqlException exception = SqlException.$(position, "Duplicate column [name=").put(colName).put(']');
        if (additionalMessage != null) {
            exception.put(' ').put(additionalMessage);
        }
        return exception;
    }

    public static SqlException emptyWindowContext(int position) {
        return SqlException.$(position, "window function called in non-window context, make sure to add OVER clause");
    }

    public static SqlException inconvertibleTypes(
            int position,
            int fromType,
            CharSequence fromName,
            int toType,
            CharSequence toName
    ) {
        return $(position, "inconvertible types: ")
                .put(ColumnType.nameOf(fromType))
                .put(" -> ")
                .put(ColumnType.nameOf(toType))
                .put(" [from=").put(fromName)
                .put(", to=").put(toName).put(']');
    }

    public static SqlException invalidColumn(int position, CharSequence column) {
        return position(position).put("Invalid column: ").put(column);
    }

    public static SqlException invalidDate(CharSequence str, int position) {
        return position(position).put("Invalid date [str=").put(str).put(']');
    }

    public static SqlException invalidDate(Utf8Sequence str, int position) {
        return position(position).put("Invalid date [str=").put(str).put(']');
    }

    public static SqlException invalidDate(int position) {
        return position(position).put("Invalid date");
    }

    public static SqlException matViewDoesNotExist(int position, CharSequence tableName) {
        return position(position).errorCode(EXCEPTION_MAT_VIEW_DOES_NOT_EXIST).put("materialized view does not exist [view=").put(tableName).put(']');
    }

    public static SqlException nonDeterministicColumn(int position, CharSequence column) {
        return position(position).put("non-deterministic function cannot be used in materialized view: ").put(column);
    }

    public static SqlException parserErr(int position, @Nullable CharSequence tok, @NotNull CharSequence msg) {
        return tok == null ?
                SqlException.$(position, msg)
                :
                SqlException.$(position, "found [tok='")
                        .put(tok)
                        .put("', len=")
                        .put(tok.length())
                        .put("] ")
                        .put(msg);
    }

    public static SqlException position(int position) {
        SqlException ex = tlException.get();
        // This is to have correct stack trace in local debugging with -ea option
        assert (ex = new SqlException()) != null;
        ex.message.clear();
        ex.position = position;
        ex.error = 0;
        return ex;
    }

    public static SqlException tableDoesNotExist(int position, CharSequence tableName) {
        return position(position).errorCode(EXCEPTION_TABLE_DOES_NOT_EXIST).put("table does not exist [table=").put(tableName).put(']');
    }

    public static SqlException unexpectedToken(int position, CharSequence token) {
        return position(position).put("unexpected token [").put(token).put(']');
    }

    public static SqlException unexpectedToken(int position, CharSequence token, @NotNull CharSequence extraMessage) {
        return position(position).put("unexpected token [").put(token).put("] - ").put(extraMessage);
    }

    public static SqlException unsupportedCast(int position, CharSequence columnName, int fromType, int toType) {
        return SqlException.$(position, "unsupported cast [column=").put(columnName)
                .put(", from=").put(ColumnType.nameOf(fromType))
                .put(", to=").put(ColumnType.nameOf(toType))
                .put(']');
    }

    public static SqlException walRecoverable(int position) {
        return position(position).errorCode(EXCEPTION_WAL_RECOVERABLE);
    }

    @Override
    public CharSequence getFlyweightMessage() {
        return message;
    }

    @Override
    public String getMessage() {
        return "[" + position + "] " + message;
    }

    public int getPosition() {
        return position;
    }

    @Override
    public StackTraceElement[] getStackTrace() {
        StackTraceElement[] result = EMPTY_STACK_TRACE;
        // This is to have correct stack trace reported in CI
        assert (result = super.getStackTrace()) != null;
        return result;
    }

    public boolean isTableDoesNotExist() {
        return error == EXCEPTION_TABLE_DOES_NOT_EXIST;
    }

    public boolean isWalRecoverable() {
        return error == EXCEPTION_WAL_RECOVERABLE;
    }

    public SqlException put(@Nullable CharSequence cs) {
        if (cs != null) {
            message.put(cs);
        }
        return this;
    }

    public SqlException put(@Nullable CharSequence cs, int lo, int hi) {
        if (cs != null) {
            message.put(cs, lo, hi);
        }
        return this;
    }

    public SqlException put(@Nullable Utf8Sequence cs) {
        if (cs != null) {
            message.put(cs);
        }
        return this;
    }

    public SqlException put(char c) {
        message.put(c);
        return this;
    }

    public SqlException put(int value) {
        message.put(value);
        return this;
    }

    public SqlException put(long value) {
        message.put(value);
        return this;
    }

    public SqlException put(double value) {
        message.put(value);
        return this;
    }

    public SqlException put(Sinkable sinkable) {
        message.put(sinkable);
        return this;
    }

    public SqlException put(long addrLo, long addrHi) {
        message.put(addrLo, addrHi);
        return this;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii('[').put(position).putAscii("]: ").put(message);
    }

    private SqlException errorCode(int errorCode) {
        this.error = errorCode;
        return this;
    }
}
