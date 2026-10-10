/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.griffin.engine.functions.lt;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.ScalarSubQueryUtils;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

/**
 * Shared cold-path implementation of the {@code string (= | < | >) (sub-query)} factories. A SYMBOL,
 * STRING, VARCHAR or CHAR left operand resolves to these factories; the factory validates that the scalar
 * sub-query selects one text or CHAR column and delegates the operator-specific function construction to the
 * concrete factory. A bare NULL left operand prefers these factories in overload resolution, so it
 * delegates a non-text sub-query to the numeric factory of the same operator.
 */
public abstract class AbstractStrCursorFunctionFactory implements FunctionFactory {
    private final FunctionFactory nullOperandFactory;

    protected AbstractStrCursorFunctionFactory(FunctionFactory nullOperandFactory) {
        this.nullOperandFactory = nullOperandFactory;
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final RecordCursorFactory factory = args.getQuick(1).getRecordCursorFactory();
        final RecordMetadata metadata = ScalarSubQueryUtils.assertSingleColumn(factory, argPositions.getQuick(1));
        final int cursorType = metadata.getColumnType(0);
        return switch (ColumnType.tagOf(cursorType)) {
            case ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL, ColumnType.CHAR, ColumnType.NULL ->
                    newFunc(factory, args.getQuick(0), args.getQuick(1), ColumnType.tagOf(cursorType), argPositions.getQuick(1));
            default -> {
                if (ColumnType.isNull(args.getQuick(0).getType())) {
                    yield nullOperandFactory.newInstance(position, args, argPositions, configuration, sqlExecutionContext);
                }
                throw SqlException.$(argPositions.getQuick(1), "cannot compare ")
                        .put(ColumnType.nameOf(args.getQuick(0).getType()))
                        .put(" and ")
                        .put(ColumnType.nameOf(cursorType));
            }
        };
    }

    protected abstract Function newFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int cursorTag, int rightPos);

    /**
     * Caches the text cursor scalar as UTF-16, or {@code null} for a null scalar or an empty sub-query.
     */
    protected abstract static class StrCursorFunction extends AbstractScalarCursorFunction {
        private final int cursorTag;
        private final StringSink sink = new StringSink();
        protected CharSequence value;

        protected StrCursorFunction(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int cursorTag, int rightPos) {
            super(factory, leftFunc, rightFunc, rightPos);
            this.cursorTag = cursorTag;
        }

        @Override
        protected void donateValueTo(AbstractScalarCursorFunction that) {
            final StrCursorFunction thatF = (StrCursorFunction) that;
            thatF.sink.clear();
            thatF.sink.put(sink);
            thatF.value = value != null ? thatF.sink : null;
        }

        @Override
        protected void readValue(Record record) {
            sink.clear();
            if (cursorTag == ColumnType.VARCHAR) {
                final Utf8Sequence us = record.getVarcharA(0);
                value = us != null && Utf8s.utf8ToUtf16(us, sink) ? sink : null;
                return;
            }
            if (cursorTag == ColumnType.CHAR) {
                final char c = record.getChar(0);
                if (c != 0) {
                    sink.put(c);
                    value = sink;
                } else {
                    value = null;
                }
                return;
            }
            final CharSequence cs = switch (cursorTag) {
                case ColumnType.STRING -> record.getStrA(0);
                case ColumnType.SYMBOL -> record.getSymA(0);
                default -> null;
            };
            if (cs != null) {
                sink.put(cs);
                value = sink;
            } else {
                value = null;
            }
        }

        @Override
        protected void setNullValue() {
            value = null;
        }
    }
}
