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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;

public class EqTimestampCursorFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(NC)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        RecordCursorFactory factory = args.getQuick(1).getRecordCursorFactory();

        // verify that the factory has metadata we can support:
        // 1. the factory must provide only one field
        // 2. the following data types are supported
        //    a. timestamp
        //    b. string - will be parsing this
        //    c. varchar - will be parsing this

        RecordMetadata metadata = factory.getMetadata();
        if (metadata.getColumnCount() != 1) {
            throw SqlException.$(argPositions.getQuick(1), "select must provide exactly one column");
        }

        switch (metadata.getColumnType(0)) {
            case ColumnType.TIMESTAMP:
            case ColumnType.NULL:
                return new EqTimestampTimestampFromCursorFunction(factory, args.getQuick(0), args.getQuick(1));
            case ColumnType.STRING:
                return new EqTimestampStringFromCursorFunction(factory, args.getQuick(0), args.getQuick(1), argPositions.getQuick(1));
            case ColumnType.VARCHAR:
                return new EqTimestampVarcharFromCursorFunction(factory, args.getQuick(0), args.getQuick(1), argPositions.getQuick(1));
            default:
                throw SqlException.$(argPositions.getQuick(1), "cannot compare TIMESTAMP and ").put(ColumnType.nameOf(metadata.getColumnType(0)));
        }
    }

    public static class EqTimestampStringFromCursorFunction extends BooleanFunction implements BinaryFunction {
        private final RecordCursorFactory factory;
        private final Function leftFn;
        private final Function rightFn;
        private final int rightFnPos;
        private long epoch;

        public EqTimestampStringFromCursorFunction(RecordCursorFactory factory, Function leftFn, Function rightFn, int rightFnPos) {
            this.factory = factory;
            this.leftFn = leftFn;
            this.rightFn = rightFn;
            this.rightFnPos = rightFnPos;
        }

        @Override
        public boolean getBool(Record rec) {
            return leftFn.getTimestamp(rec) == epoch;
        }

        @Override
        public Function getLeft() {
            return leftFn;
        }

        @Override
        public Function getRight() {
            return rightFn;
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
                        throw SqlException.$(rightFnPos, "the cursor selected invalid timestamp value: ").put(value);
                    }
                } else {
                    epoch = Numbers.LONG_NULL;
                }
            }
        }

        @Override
        public boolean isReadThreadSafe() {
            // the function is thread safe because its state is epoch, which does not mutate
            // between frame executions. For non-thread-safe function, which operates a cursor,
            // the cursor will be re-executed as many times as there are threads. Which is suboptimal.
            return true;
        }
    }

    public static class EqTimestampTimestampFromCursorFunction extends BooleanFunction implements BinaryFunction {
        private final RecordCursorFactory factory;
        private final Function leftFn;
        private final Function rightFn;
        private long epoch;

        public EqTimestampTimestampFromCursorFunction(RecordCursorFactory factory, Function leftFn, Function rightFn) {
            this.factory = factory;
            this.leftFn = leftFn;
            this.rightFn = rightFn;
        }

        @Override
        public boolean getBool(Record rec) {
            return leftFn.getTimestamp(rec) == epoch;
        }

        @Override
        public Function getLeft() {
            return leftFn;
        }

        @Override
        public Function getRight() {
            return rightFn;
        }

        @Override
        public boolean isReadThreadSafe() {
            // the function is thread safe because its state is epoch, which does not mutate
            // between frame executions. For non-thread-safe function, which operates a cursor,
            // the cursor will be re-executed as many times as there are threads. Which is suboptimal.
            return true;
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
    }

    public static class EqTimestampVarcharFromCursorFunction extends BooleanFunction implements BinaryFunction {
        private final RecordCursorFactory factory;
        private final Function leftFn;
        private final Function rightFn;
        private final int rightFnPos;
        private long epoch;

        public EqTimestampVarcharFromCursorFunction(RecordCursorFactory factory, Function leftFn, Function rightFn, int rightFnPos) {
            this.factory = factory;
            this.leftFn = leftFn;
            this.rightFn = rightFn;
            this.rightFnPos = rightFnPos;
        }

        @Override
        public boolean getBool(Record rec) {
            return leftFn.getTimestamp(rec) == epoch;
        }

        @Override
        public Function getLeft() {
            return leftFn;
        }

        @Override
        public Function getRight() {
            return rightFn;
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
                        throw SqlException.$(rightFnPos, "the cursor selected invalid timestamp value: ").put(value);
                    }
                } else {
                    epoch = Numbers.LONG_NULL;
                }
            }
        }

        @Override
        public boolean isReadThreadSafe() {
            // the function is thread safe because its state is epoch, which does not mutate
            // between frame executions. For non-thread-safe function, which operates a cursor,
            // the cursor will be re-executed as many times as there are threads. Which is suboptimal.
            return true;
        }
    }
}
