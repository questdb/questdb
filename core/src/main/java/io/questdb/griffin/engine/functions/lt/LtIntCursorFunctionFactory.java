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
import io.questdb.griffin.engine.functions.ScalarSubQueryUtils;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

/**
 * Implements {@code int < (sub-query)} where the right-hand operand is a cursor (scalar sub-query)
 * providing exactly one column and (conceptually) one row.
 * <p>
 * Unlike {@link LtDoubleCursorFunctionFactory}, the comparison is performed at the higher precision of the
 * {@code int} left operand and the cursor scalar so that no precision is lost: comparing against a
 * {@code long} sub-query is performed as a {@code long} comparison, and comparing against a
 * {@code float}/{@code double} sub-query is performed as a {@code double} comparison. A
 * {@code byte}/{@code short}/{@code int} (or {@code null}) cursor scalar already fits into an {@code int}
 * comparison, so no widening is applied in that case.
 * <p>
 * The sub-query is executed once per query execution - not per row - in {@link Function#init} and its
 * value is cached as a scalar. An empty cursor or a {@code null} value matches no rows (SQL
 * {@code null}-comparison semantics).
 */
public class LtIntCursorFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "<(IC)";
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

        final RecordMetadata metadata = factory.getMetadata();
        if (metadata.getColumnCount() != 1) {
            throw SqlException.$(argPositions.getQuick(1), "select must provide exactly one column");
        }
        final Function arg0 = args.getQuick(0);
        switch (ColumnType.tagOf(arg0.getType())) {
            case ColumnType.BYTE:
            case ColumnType.SHORT:
            case ColumnType.INT:
                break;
            default:
                throw SqlException.$(argPositions.getQuick(0), "left operand must be BYTE, SHORT or INT, found: ")
                        .put(ColumnType.nameOf(arg0.getType()));
        }
        final int cursorTag = ColumnType.tagOf(metadata.getColumnType(0));
        return switch (cursorTag) {
            case ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.NULL ->
                // left operand is BYTE/SHORT/INT, so a narrow cursor scalar fits into an int comparison
                    new IntCursorFunc(factory, arg0, args.getQuick(1), cursorTag, argPositions.getQuick(1));
            case ColumnType.LONG ->
                // widen the comparison to long so a wide cursor scalar is not narrowed to int
                    new LongCursorFunc(factory, arg0, args.getQuick(1), argPositions.getQuick(1));
            case ColumnType.FLOAT, ColumnType.DOUBLE ->
                    new DoubleCursorFunc(factory, arg0, args.getQuick(1), cursorTag, argPositions.getQuick(1));
            default ->
                    throw SqlException.$(argPositions.getQuick(1), "cannot compare INT and ").put(ColumnType.nameOf(metadata.getColumnType(0)));
        };
    }

    private static int readScalarInt(Record record, int cursorTag) {
        return switch (cursorTag) {
            case ColumnType.BYTE -> record.getByte(0);
            case ColumnType.SHORT -> record.getShort(0);
            case ColumnType.INT -> record.getInt(0);
            default -> Numbers.INT_NULL;
        };
    }

    private static class DoubleCursorFunc extends NegatableBooleanFunction implements BinaryFunction {
        private final int cursorTag;
        private final RecordCursorFactory factory;
        private final Function leftFunc;
        private final Function rightFunc;
        private final int rightPos;
        private boolean stateInherited = false;
        private boolean stateShared = false;
        private double value;

        public DoubleCursorFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int cursorTag, int rightPos) {
            this.factory = factory;
            this.leftFunc = leftFunc;
            this.rightFunc = rightFunc;
            this.rightPos = rightPos;
            this.cursorTag = cursorTag;
        }

        @Override
        public boolean getBool(Record rec) {
            final double l = leftFunc.getDouble(rec);
            final boolean eq = Numbers.equals(l, value);
            return negated ? (eq || l > value) : (!eq && l < value);
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
                    value = cursorTag == ColumnType.FLOAT ? cursor.getRecord().getFloat(0) : cursor.getRecord().getDouble(0);
                    ScalarSubQueryUtils.assertNoMoreRows(cursor, rightPos);
                } else {
                    value = Double.NaN;
                }
            }
        }

        @Override
        public boolean isThreadSafe() {
            return leftFunc.isThreadSafe();
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof DoubleCursorFunc thatF) {
                thatF.value = value;
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

    private static class IntCursorFunc extends NegatableBooleanFunction implements BinaryFunction {
        private final int cursorTag;
        private final RecordCursorFactory factory;
        private final Function leftFunc;
        private final Function rightFunc;
        private final int rightPos;
        private boolean stateInherited = false;
        private boolean stateShared = false;
        private int value;

        public IntCursorFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int cursorTag, int rightPos) {
            this.factory = factory;
            this.leftFunc = leftFunc;
            this.rightFunc = rightFunc;
            this.rightPos = rightPos;
            this.cursorTag = cursorTag;
        }

        @Override
        public boolean getBool(Record rec) {
            return Numbers.lessThan(leftFunc.getInt(rec), value, negated);
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
                    value = readScalarInt(cursor.getRecord(), cursorTag);
                    ScalarSubQueryUtils.assertNoMoreRows(cursor, rightPos);
                } else {
                    value = Numbers.INT_NULL;
                }
            }
        }

        @Override
        public boolean isThreadSafe() {
            return leftFunc.isThreadSafe();
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof IntCursorFunc thatF) {
                thatF.value = value;
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

    private static class LongCursorFunc extends NegatableBooleanFunction implements BinaryFunction {
        private final RecordCursorFactory factory;
        private final Function leftFunc;
        private final Function rightFunc;
        private final int rightPos;
        private boolean stateInherited = false;
        private boolean stateShared = false;
        private long value;

        public LongCursorFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int rightPos) {
            this.factory = factory;
            this.leftFunc = leftFunc;
            this.rightFunc = rightFunc;
            this.rightPos = rightPos;
        }

        @Override
        public boolean getBool(Record rec) {
            return Numbers.lessThan(leftFunc.getLong(rec), value, negated);
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
                    value = cursor.getRecord().getLong(0);
                    ScalarSubQueryUtils.assertNoMoreRows(cursor, rightPos);
                } else {
                    value = Numbers.LONG_NULL;
                }
            }
        }

        @Override
        public boolean isThreadSafe() {
            return leftFunc.isThreadSafe();
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof LongCursorFunc thatF) {
                thatF.value = value;
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
