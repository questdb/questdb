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
import io.questdb.cairo.TimestampDriver;
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
        Function arg0 = args.getQuick(0);
        int arg0ColType = arg0.getType();
        if (ColumnType.tagOf(arg0ColType) != ColumnType.TIMESTAMP) {
            throw SqlException.$(argPositions.getQuick(0), "left operand must be a TIMESTAMP, found: ").put(ColumnType.nameOf(args.getQuick(0).getType()));
        }
        int arg0Type = ColumnType.getTimestampType(arg0ColType);
        int metadataType = metadata.getColumnType(0);
        switch (ColumnType.tagOf(metadataType)) {
            case ColumnType.TIMESTAMP:
            case ColumnType.NULL:
                int timestampType = arg0Type;
                if (ColumnType.isTimestamp(metadataType)) {
                    timestampType = ColumnType.getHigherPrecisionTimestampType(arg0Type, metadataType);
                }
                boolean leftNeedsConvert = arg0Type != timestampType;
                if (leftNeedsConvert) {
                    return new LeftConvertTimestampCursorFunc(factory, arg0, args.getQuick(1), ColumnType.getTimestampDriver(timestampType), arg0Type);
                } else {
                    return new TimestampCursorFunc(factory, arg0, args.getQuick(1), ColumnType.getTimestampDriver(timestampType));
                }
            case ColumnType.STRING:
                return new StrCursorFunc(factory, arg0, args.getQuick(1), ColumnType.getTimestampDriver(arg0Type), argPositions.getQuick(1));
            case ColumnType.VARCHAR:
                return new VarcharCursorFunc(factory, arg0, args.getQuick(1), ColumnType.getTimestampDriver(arg0Type), argPositions.getQuick(1));
            default:
                throw SqlException.$(argPositions.getQuick(1), "cannot compare TIMESTAMP and ").put(ColumnType.nameOf(metadataType));
        }
    }

    private static class LeftConvertTimestampCursorFunc extends TimestampCursorFunc {
        private final int leftTimestampType;

        public LeftConvertTimestampCursorFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, TimestampDriver driver, int leftTimestampType) {
            super(factory, leftFunc, rightFunc, driver);
            this.leftTimestampType = leftTimestampType;
        }

        @Override
        public boolean getBool(Record rec) {
            return Numbers.lessThan(
                    driver.from(leftFunc.getTimestamp(rec), leftTimestampType),
                    epoch,
                    negated
            );
        }
    }

    private static class StrCursorFunc extends NegatableBooleanFunction implements BinaryFunction {
        private final TimestampDriver driver;
        private final RecordCursorFactory factory;
        private final Function leftFunc;
        private final Function rightFunc;
        private final int rightPos;
        private long epoch;
        private boolean stateInherited = false;
        private boolean stateShared = false;

        public StrCursorFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, TimestampDriver driver, int rightPos) {
            this.factory = factory;
            this.leftFunc = leftFunc;
            this.rightFunc = rightFunc;
            this.rightPos = rightPos;
            this.driver = driver;
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
            BinaryFunction.super.init(symbolTableSource, executionContext);
            if (stateInherited) {
                return;
            }
            this.stateShared = false;
            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                if (cursor.hasNext()) {
                    final CharSequence value = cursor.getRecord().getStrA(0);
                    try {
                        epoch = driver.parseFloorLiteral(value);
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
            return leftFunc.isThreadSafe();
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof StrCursorFunc) {
                StrCursorFunc thatF = (StrCursorFunc) that;
                thatF.epoch = epoch;
                thatF.stateInherited = this.stateShared = true;
            }
            BinaryFunction.super.offerStateTo(that);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(leftFunc);
            if (isThreadSafe()) {
                sink.val(" [thread-safe]");
            }
            if (negated) {
                sink.val(" >= ");
            } else {
                sink.val(" < ");
            }
            sink.val(rightFunc);
            if (stateShared) {
                sink.val(" [state-shared]");
            }
        }
    }

    private static class TimestampCursorFunc extends NegatableBooleanFunction implements BinaryFunction {
        protected final TimestampDriver driver;
        protected final Function leftFunc;
        private final RecordCursorFactory factory;
        private final Function rightFunc;
        protected long epoch;
        private boolean stateInherited = false;
        private boolean stateShared = false;

        public TimestampCursorFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, TimestampDriver driver) {
            this.factory = factory;
            this.leftFunc = leftFunc;
            this.rightFunc = rightFunc;
            this.driver = driver;
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
            BinaryFunction.super.init(symbolTableSource, executionContext);
            if (stateInherited) {
                return;
            }
            this.stateShared = false;
            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                if (cursor.hasNext()) {
                    epoch = driver.from(cursor.getRecord().getTimestamp(0), ColumnType.getTimestampType(factory.getMetadata().getColumnType(0)));
                } else {
                    epoch = Numbers.LONG_NULL;
                }
            }
        }

        @Override
        public boolean isThreadSafe() {
            return leftFunc.isThreadSafe();
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof TimestampCursorFunc) {
                TimestampCursorFunc thatF = (TimestampCursorFunc) that;
                thatF.epoch = epoch;
                thatF.stateInherited = this.stateShared = true;
            }
            BinaryFunction.super.offerStateTo(that);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(leftFunc);
            if (leftFunc.isThreadSafe()) {
                sink.val(" [thread-safe]");
            }
            if (negated) {
                sink.val(" >= ");
            } else {
                sink.val(" < ");
            }
            sink.val(rightFunc);
            if (stateShared) {
                sink.val(" [state-shared]");
            }
        }
    }

    private static class VarcharCursorFunc extends NegatableBooleanFunction implements BinaryFunction {
        private final TimestampDriver driver;
        private final RecordCursorFactory factory;
        private final Function leftFunc;
        private final Function rightFunc;
        private final int rightPos;
        private long epoch;
        private boolean stateInherited = false;
        private boolean stateShared = false;

        public VarcharCursorFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, TimestampDriver driver, int rightPos) {
            this.factory = factory;
            this.leftFunc = leftFunc;
            this.rightFunc = rightFunc;
            this.rightPos = rightPos;
            this.driver = driver;
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
            BinaryFunction.super.init(symbolTableSource, executionContext);
            if (stateInherited) {
                return;
            }
            this.stateShared = false;
            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                if (cursor.hasNext()) {
                    final Utf8Sequence value = cursor.getRecord().getVarcharA(0);
                    try {
                        epoch = driver.parseFloorLiteral(value);
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
            return leftFunc.isThreadSafe();
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof VarcharCursorFunc) {
                VarcharCursorFunc thatF = (VarcharCursorFunc) that;
                thatF.epoch = epoch;
                thatF.stateInherited = this.stateShared = true;
            }
            BinaryFunction.super.offerStateTo(that);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(leftFunc);
            if (leftFunc.isThreadSafe()) {
                sink.val(" [thread-safe]");
            }
            if (negated) {
                sink.val(" >= ");
            } else {
                sink.val(" < ");
            }
            sink.val(rightFunc);
            if (stateShared) {
                sink.val(" [state-shared]");
            }
        }
    }
}
