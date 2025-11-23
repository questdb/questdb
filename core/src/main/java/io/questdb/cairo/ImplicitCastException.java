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

package io.questdb.cairo;

import io.questdb.std.Chars;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

public class ImplicitCastException extends RuntimeException implements FlyweightMessageContainer {
    private static final StackTraceElement[] EMPTY_STACK_TRACE = {};
    private static final ThreadLocal<ImplicitCastException> tlException = new ThreadLocal<>(ImplicitCastException::new);
    private final StringSink message = new StringSink();
    private int position = 0;

    public static ImplicitCastException inconvertibleValue(double value, int fromType, int toType) {
        return instance().put("inconvertible value: ")
                .put(value)
                .put(" [")
                .put(ColumnType.nameOf(fromType))
                .put(" -> ")
                .put(ColumnType.nameOf(toType))
                .put(']');
    }

    public static ImplicitCastException inconvertibleValue(char value, int fromType, int toType) {
        return instance().put("inconvertible value: ")
                .put(value)
                .put(" [")
                .put(ColumnType.nameOf(fromType))
                .put(" -> ")
                .put(ColumnType.nameOf(toType))
                .put(']');
    }

    public static ImplicitCastException inconvertibleValue(int tupleIndex, CharSequence value, int fromType, int toType) {
        if (tupleIndex == -1) {
            return inconvertibleValue(value, fromType, toType);
        }

        ImplicitCastException ice = instance();
        ice.put("inconvertible value: ");
        if (value != null) {
            ice.put('`').put(value).put('`');
        } else {
            ice.put("null");
        }

        return ice.put(" [")
                .put(ColumnType.nameOf(fromType))
                .put(" -> ")
                .put(ColumnType.nameOf(toType))
                .put(']')
                .put(" tuple: ")
                .put(tupleIndex);
    }

    public static ImplicitCastException inconvertibleValue(CharSequence value, int fromType, int toType) {
        ImplicitCastException ice = instance();
        ice.put("inconvertible value: ");
        if (value != null) {
            ice.put('`').put(value).put('`');
        } else {
            ice.put("null");
        }

        return ice.put(" [")
                .put(ColumnType.nameOf(fromType))
                .put(" -> ")
                .put(ColumnType.nameOf(toType))
                .put(']');
    }

    public static ImplicitCastException inconvertibleValue(Sinkable sinkable, int fromType, int toType) {
        ImplicitCastException ice = instance();
        ice.put("inconvertible value: ");
        sinkable.toSink(ice.message);

        return ice.put(" [")
                .put(ColumnType.nameOf(fromType))
                .put(" -> ")
                .put(ColumnType.nameOf(toType))
                .put(']');
    }

    public static ImplicitCastException inconvertibleValue(Utf8Sequence value, int fromType, int toType) {
        ImplicitCastException ice = instance();
        ice.put("inconvertible value: ");
        if (value != null) {
            ice.put('`').put(value).put('`');
        } else {
            ice.put("null");
        }

        return ice.put(" [")
                .put(ColumnType.nameOf(fromType))
                .put(" -> ")
                .put(ColumnType.nameOf(toType))
                .put(']');
    }

    public static ImplicitCastException inconvertibleValue(long value, int fromType, int toType) {
        return instance().put("inconvertible value: ")
                .put(value)
                .put(" [")
                .put(ColumnType.nameOf(fromType))
                .put(" -> ")
                .put(ColumnType.nameOf(toType))
                .put(']');
    }

    public static ImplicitCastException instance() {
        ImplicitCastException ex = tlException.get();
        // This is to have correct stack trace in local debugging with -ea option
        assert (ex = new ImplicitCastException()) != null;
        ex.message.clear();
        ex.position = 0;
        return ex;
    }

    @Override
    public CharSequence getFlyweightMessage() {
        return message;
    }

    @Override
    public String getMessage() {
        return Chars.toString(message);
    }

    @Override
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

    public ImplicitCastException position(int position) {
        this.position = position;
        return this;
    }

    public ImplicitCastException put(long value) {
        message.put(value);
        return this;
    }

    public ImplicitCastException put(double value) {
        message.put(value);
        return this;
    }

    public ImplicitCastException put(@Nullable CharSequence cs) {
        message.put(cs);
        return this;
    }

    public ImplicitCastException put(@Nullable Utf8Sequence us) {
        message.put(us);
        return this;
    }

    public ImplicitCastException put(char c) {
        message.put(c);
        return this;
    }
}
