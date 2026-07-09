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
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

/**
 * Implements {@code double > (sub-query)} where the right-hand operand is a cursor
 * (scalar sub-query) providing exactly one column and (conceptually) one row.
 * <p>
 * The sub-query is executed once per query execution - not per row - in {@link Function#init}
 * and the resulting value is cached as a scalar {@code double}. If the cursor selects no rows,
 * or the value is {@code null}, the cached value is {@link Double#NaN} and the predicate matches
 * no rows (SQL {@code null}-comparison semantics).
 */
public class GtDoubleCursorFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return ">(DC)";
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
        // 1. the factory must provide only one column
        // 2. the column must be a numeric type we can widen to double (or null)
        final RecordMetadata metadata = factory.getMetadata();
        if (metadata.getColumnCount() != 1) {
            throw SqlException.$(argPositions.getQuick(1), "select must provide exactly one column");
        }
        final Function arg0 = args.getQuick(0);
        if (ColumnType.tagOf(arg0.getType()) != ColumnType.DOUBLE) {
            throw SqlException.$(argPositions.getQuick(0), "left operand must be a DOUBLE, found: ")
                    .put(ColumnType.nameOf(arg0.getType()));
        }
        final int metadataType = metadata.getColumnType(0);
        switch (ColumnType.tagOf(metadataType)) {
            case ColumnType.DOUBLE:
            case ColumnType.FLOAT:
            case ColumnType.LONG:
            case ColumnType.INT:
            case ColumnType.SHORT:
            case ColumnType.BYTE:
            case ColumnType.NULL:
                return new GtDoubleCursorFunc(factory, arg0, args.getQuick(1), ColumnType.tagOf(metadataType));
            default:
                throw SqlException.$(argPositions.getQuick(1), "cannot compare DOUBLE and ").put(ColumnType.nameOf(metadataType));
        }
    }

    private static class GtDoubleCursorFunc extends NegatableBooleanFunction implements BinaryFunction {
        private final int cursorColumnTypeTag;
        private final RecordCursorFactory factory;
        private final Function leftFunc;
        private final Function rightFunc;
        private boolean stateInherited = false;
        private boolean stateShared = false;
        private double value;

        public GtDoubleCursorFunc(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int cursorColumnTypeTag) {
            this.factory = factory;
            this.leftFunc = leftFunc;
            this.rightFunc = rightFunc;
            this.cursorColumnTypeTag = cursorColumnTypeTag;
        }

        @Override
        public boolean getBool(Record rec) {
            final double l = leftFunc.getDouble(rec);
            final boolean eq = Numbers.equals(l, value);
            return negated ? (eq || l < value) : (!eq && l > value);
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
                    value = readValue(cursor.getRecord(), cursorColumnTypeTag);
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
            if (that instanceof GtDoubleCursorFunc thatF) {
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
                sink.val(" <= ");
            } else {
                sink.val(" > ");
            }
            sink.val(rightFunc);
            if (stateShared) {
                sink.val(" [state-shared]");
            }
        }

        private static double readValue(Record record, int columnTypeTag) {
            switch (columnTypeTag) {
                case ColumnType.DOUBLE:
                    return record.getDouble(0);
                case ColumnType.FLOAT:
                    return record.getFloat(0);
                case ColumnType.LONG:
                    final long l = record.getLong(0);
                    return l == Numbers.LONG_NULL ? Double.NaN : l;
                case ColumnType.INT:
                    final int i = record.getInt(0);
                    return i == Numbers.INT_NULL ? Double.NaN : i;
                case ColumnType.SHORT:
                    return record.getShort(0);
                case ColumnType.BYTE:
                    return record.getByte(0);
                default:
                    return Double.NaN;
            }
        }
    }
}
