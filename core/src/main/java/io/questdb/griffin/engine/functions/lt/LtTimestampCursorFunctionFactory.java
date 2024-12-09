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

package io.questdb.griffin.engine.functions.lt;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;

public class LtTimestampCursorFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "<(NC)";
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

        // verify that the factory has metadata we can support:
        // 1. the factory must provide only one field
        // 2. the following data types are supported
        //    a. timestamp
        //    b. string - will be parsing this
        //    c. varchar - will be parsing this

        final RecordMetadata metadata = factory.getMetadata();
        if (metadata.getColumnCount() != 1) {
            throw SqlException.$(argPositions.getQuick(1), "select must provide exactly one column");
        }

        switch (metadata.getColumnType(0)) {
            case ColumnType.TIMESTAMP:
            case ColumnType.NULL:
                return new TimestampCursorFunc(factory, args.getQuick(0), args.getQuick(1));
            case ColumnType.STRING:
                return new StrCursorFunc(factory, args.getQuick(0), args.getQuick(1), argPositions.getQuick(1));
            case ColumnType.VARCHAR:
                return new VarcharCursorFunc(factory, args.getQuick(0), args.getQuick(1), argPositions.getQuick(1));
            default:
                throw SqlException.$(argPositions.getQuick(1), "cannot compare TIMESTAMP and ").put(ColumnType.nameOf(metadata.getColumnType(0)));
        }
    }

    private static class StrCursorFunc extends NegatableBooleanFunction implements BinaryFunction {
        private final RecordCursorFactory factory;
        private final Function leftFunc;
        private final Function rightFunc;
        private final int rightPos;
        private long epoch;

        public StrCursorFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int rightPos) {
            this.factory = factory;
            this.leftFunc = leftFunc;
            this.rightFunc = rightFunc;
            this.rightPos = rightPos;
        }

        @Override
        public boolean getBool(Record rec) {
            return Numbers.lessThan(
                    leftFunc.getTimestamp(rec),
                    epoch,
                    negated
            );
        }

        @Override
        public Function getLeft() {
            return leftFunc;
        }

        @Override
        public Function getRight() {
            return rightFunc;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                if (cursor.hasNext()) {
                    final CharSequence value = cursor.getRecord().getStrA(0);
                    try {
                        epoch = value != null ? IntervalUtils.parseFloorPartialTimestamp(value) : Numbers.LONG_NULL;
                    } catch (NumericException e) {
                        throw SqlException.$(rightPos, "the cursor selected invalid timestamp value: ").put(value);
                    }
                } else {
                    epoch = Numbers.LONG_NULL;
                }
            }
        }

        @Override
        public boolean isThreadSafe() {
            // the function is thread safe because its state is epoch, which does not mutate
            // between frame executions. For non-thread-safe function, which operates a cursor,
            // the cursor will be re-executed as many times as there are threads. Which is suboptimal.
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(leftFunc);
            if (negated) {
                sink.val(">=");
            } else {
                sink.val('<');
            }
            sink.val(rightFunc);
        }
    }

    private static class TimestampCursorFunc extends NegatableBooleanFunction implements BinaryFunction {
        private final RecordCursorFactory factory;
        private final Function leftFunc;
        private final Function rightFunc;
        private long epoch;

        public TimestampCursorFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc) {
            this.factory = factory;
            this.leftFunc = leftFunc;
            this.rightFunc = rightFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            return Numbers.lessThan(
                    leftFunc.getTimestamp(rec),
                    epoch,
                    negated
            );
        }

        @Override
        public Function getLeft() {
            return leftFunc;
        }

        @Override
        public Function getRight() {
            return rightFunc;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                if (cursor.hasNext()) {
                    epoch = cursor.getRecord().getTimestamp(0);
                } else {
                    epoch = Numbers.LONG_NULL;
                }
            }
        }

        @Override
        public boolean isThreadSafe() {
            // the function is thread safe because its state is epoch, which does not mutate
            // between frame executions. For non-thread-safe function, which operates a cursor,
            // the cursor will be re-executed as many times as there are threads. Which is suboptimal.
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(leftFunc);
            if (negated) {
                sink.val(">=");
            } else {
                sink.val('<');
            }
            sink.val(rightFunc);
        }
    }

    private static class VarcharCursorFunc extends NegatableBooleanFunction implements BinaryFunction {
        private final RecordCursorFactory factory;
        private final Function leftFunc;
        private final Function rightFunc;
        private final int rightPos;
        private long epoch;

        public VarcharCursorFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int rightPos) {
            this.factory = factory;
            this.leftFunc = leftFunc;
            this.rightFunc = rightFunc;
            this.rightPos = rightPos;
        }

        @Override
        public boolean getBool(Record rec) {
            return Numbers.lessThan(
                    leftFunc.getTimestamp(rec),
                    epoch,
                    negated
            );
        }

        @Override
        public Function getLeft() {
            return leftFunc;
        }

        @Override
        public Function getRight() {
            return rightFunc;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                if (cursor.hasNext()) {
                    final Utf8Sequence value = cursor.getRecord().getVarcharA(0);
                    try {
                        epoch = value != null ? IntervalUtils.parseFloorPartialTimestamp(value) : Numbers.LONG_NULL;
                    } catch (NumericException e) {
                        throw SqlException.$(rightPos, "the cursor selected invalid timestamp value: ").put(value);
                    }
                } else {
                    epoch = Numbers.LONG_NULL;
                }
            }
        }

        @Override
        public boolean isThreadSafe() {
            // the function is thread safe because its state is epoch, which does not mutate
            // between frame executions. For non-thread-safe function, which operates a cursor,
            // the cursor will be re-executed as many times as there are threads. Which is suboptimal.
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(leftFunc);
            if (negated) {
                sink.val(">=");
            } else {
                sink.val('<');
            }
            sink.val(rightFunc);
        }
    }
}
