/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.CharSinkBase;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SqlException extends Exception implements Sinkable, FlyweightMessageContainer {

    private static final StackTraceElement[] EMPTY_STACK_TRACE = {};

    private static final ThreadLocal<SqlException> tlException = new ThreadLocal<>(SqlException::new);
    private final StringSink message = new StringSink();
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
        return position(position).put("ambiguous column [name=").put(columnName).put(']');
    }

    public static SqlException duplicateColumn(int position, CharSequence colName) {
        return duplicateColumn(position, colName, null);
    }

    public static SqlException duplicateColumn(int position, CharSequence colName, CharSequence additionalMessage) {
        SqlException exception = SqlException.$(position, "duplicate column [name=").put(colName).put(']');
        if (additionalMessage != null) {
            exception.put(' ').put(additionalMessage);
        }
        return exception;
    }

    public static SqlException emptyWindowContext(int position) {
        return SqlException.$(position, "window function called in non-window context, make sure to add OVER clause");
    }

    public static SqlException inconvertibleTypes(int position, int fromType, int toType) {
        return inconvertibleTypes(position, fromType, null, toType, null, -1);
    }

    public static SqlException inconvertibleTypes(int position, int fromType, int toType, CharSequence toName) {
        return inconvertibleTypes(position, fromType, null, toType, toName, -1);
    }

    public static SqlException inconvertibleTypes(int position, int fromType, int toType, int toTypeIndex) {
        return inconvertibleTypes(position, fromType, null, toType, null, toTypeIndex);
    }

    public static SqlException inconvertibleTypes(int position, int fromType, CharSequence fromName, int toType, CharSequence toName) {
        return inconvertibleTypes(position, fromType, fromName, toType, toName, -1);
    }

    public static SqlException inconvertibleValue(double value, int fromType, int toType) {
        return addCastDirection(tlInconvertibleValueInstance().put(value), fromType, toType);
    }

    public static SqlException inconvertibleValue(char value, int fromType, int toType) {
        return addCastDirection(tlInconvertibleValueInstance().put(value), fromType, toType);
    }

    public static SqlException inconvertibleValue(int tupleIndex, CharSequence value, int fromType, int toType) {
        SqlException ice = inconvertibleValue(value, fromType, toType);
        return tupleIndex == -1 ? ice : ice.put(" tuple: ").put(tupleIndex);
    }

    public static SqlException inconvertibleValue(CharSequence value, int fromType, int toType) {
        SqlException ice = tlInconvertibleValueInstance();
        if (value != null) {
            ice.put('`').put(value).put('`');
        } else {
            ice.put("null");
        }
        return addCastDirection(ice, fromType, toType);
    }

    public static SqlException inconvertibleValue(Utf8Sequence value, int fromType, int toType) {
        SqlException ice = tlInconvertibleValueInstance();
        if (value != null) {
            ice.put('`').put(value.asAsciiCharSequence()).put('`');
        } else {
            ice.put("null");
        }
        return addCastDirection(ice, fromType, toType);
    }

    public static SqlException inconvertibleValue(long value, int fromType, int toType) {
        return addCastDirection(tlInconvertibleValueInstance().put(value), fromType, toType);
    }

    public static SqlException invalidArgument(ExpressionNode node, ObjList<Function> args, FunctionFactoryDescriptor descriptor) {
        SqlException ex = SqlException.position(node.position);
        ex.put("unexpected argument for function [name=");
        ex.put(node.token);
        ex.put(", signature=");
        ex.put('(');
        if (descriptor != null) {
            for (int i = 0, n = descriptor.getSigArgCount(); i < n; i++) {
                if (i > 0) {
                    ex.put(", ");
                }
                final int mask = descriptor.getArgTypeMask(i);
                ex.put(ColumnType.nameOf(FunctionFactoryDescriptor.toTypeTag(mask)));
                if (FunctionFactoryDescriptor.isArray(mask)) {
                    ex.put("[]");
                }
                if (FunctionFactoryDescriptor.isConstant(mask)) {
                    ex.put(" constant");
                }
            }
        }
        ex.put("), args=");
        ex.put('(');
        if (args != null) {
            for (int i = 0, n = args.size(); i < n; i++) {
                if (i > 0) {
                    ex.put(", ");
                }
                Function arg = args.getQuick(i);
                ex.put(ColumnType.nameOf(arg.getType()));
                if (arg.isConstant()) {
                    ex.put(" constant");
                }
            }
        }
        ex.put(")]");
        Misc.freeObjList(args);
        return ex;
    }

    public static SqlException invalidColumn(int position, CharSequence column) {
        return position(position).put("invalid column [name=").put(column).put(']');
    }

    public static SqlException invalidColumnType(int position, int columnType) {
        return SqlException.$(position, "invalid column type [name=").put(ColumnType.nameOf(columnType)).put(", value=").put(columnType).put(']');
    }

    public static SqlException invalidConstant(int position, CharSequence constant) {
        return SqlException.position(position).put("invalid constant [value=").put(constant).put(']');
    }

    public static SqlException invalidDate(CharSequence str, int position) {
        return position(position).put("invalid date [value=").put(str).put(']');
    }

    public static SqlException invalidFunction(ExpressionNode node, ObjList<Function> args) {
        SqlException ex = SqlException.position(node.position);
        ex.put("unknown function name: ");
        ex.put(node.token);
        ex.put('(');
        if (args != null) {
            for (int i = 0, n = args.size(); i < n; i++) {
                if (i > 0) {
                    ex.put(',');
                }
                ex.put(ColumnType.nameOf(args.getQuick(i).getType()));
            }
        }
        ex.put(')');
        Misc.freeObjList(args);
        return ex;
    }

    public static SqlException invalidSignature(int position, CharSequence signature, String message) {
        return position(position).put("invalid signature: ").put(signature).put(" [reason=").put(message).put(']');
    }

    public static SqlException invalidSignature(int position, CharSequence signature, char chr) {
        return position(position).put("invalid signature: ").put(signature).put(" [reason=invalid character ").put(chr).put(']');
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
        return ex;
    }

    public static SqlException tableDoesNotExist(int position, CharSequence tableName) {
        return position(position).put("table does not exist [table=").put(tableName).put(']');
    }

    public static SqlException unexpectedToken(int position, CharSequence token) {
        return position(position).put("unexpected token [").put(token).put(']');
    }

    public static SqlException unsupportedCast(int position, CharSequence columnName, int fromType, int toType) {
        return SqlException.$(position, "unsupported cast [column=").put(columnName)
                .put(", from=").put(ColumnType.nameOf(fromType))
                .put(", to=").put(ColumnType.nameOf(toType))
                .put(']');
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

    public SqlException put(@Nullable CharSequence cs) {
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
    public void toSink(@NotNull CharSinkBase<?> sink) {
        sink.putAscii('[').put(position).putAscii("]: ").put(message);
    }

    private static SqlException addCastDirection(SqlException ice, int fromType, int toType) {
        return ice.put(" [").put(ColumnType.nameOf(fromType)).put(" -> ").put(ColumnType.nameOf(toType)).put(']');
    }

    private static SqlException inconvertibleTypes(
            int position,
            int fromType,
            @Nullable CharSequence fromName,
            int toType,
            @Nullable CharSequence toName,
            int toTypeIndex
    ) {
        SqlException ex = SqlException.$(position, "inconvertible types: ")
                .put(ColumnType.nameOf(fromType)).put(" -> ").put(ColumnType.nameOf(toType));
        if (fromName != null) {
            if (toName != null) {
                ex.put(" [from=").put(fromName).put(", to=").put(toName);
            } else {
                ex.put(" [varName=").put(fromName);
            }
            ex.put(']');
        } else if (toTypeIndex > -1) {
            ex.put(" [varIndex=").put(toTypeIndex).put(']');
        }
        return ex;
    }

    private static SqlException tlInconvertibleValueInstance() {
        SqlException ex = tlException.get();
        ex.message.clear();
        ex.message.put("inconvertible value: ");
        return ex;
    }
}
