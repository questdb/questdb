/*+*****************************************************************************
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

package io.questdb.griffin.engine.functions.bool;

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
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.ScalarSubQueryUtils;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;

/**
 * Implements {@code between(NCC)}: a TIMESTAMP BETWEEN two scalar sub-query bounds. Also hosts
 * the shared implementation for the mixed signatures {@code between(NCN)}
 * ({@link BetweenTimestampCursorLoFunctionFactory}) and {@code between(NNC)}
 * ({@link BetweenTimestampCursorHiFunctionFactory}).
 * <p>
 * The semantics mirror both {@code between(NNN)} ({@link BetweenTimestampFunctionFactory}) and
 * the designated-timestamp interval intrinsic ({@code RuntimeIntervalModel}): the sub-query
 * evaluates once per execution during {@code init()}, must yield at most one row of a single
 * TIMESTAMP, STRING, VARCHAR or NULL column, bounds convert to the left operand's timestamp
 * precision, a NULL bound (or empty sub-query) makes the predicate false, and reversed bounds
 * normalize via min/max.
 */
public class BetweenTimestampCursorFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "between(NCC)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return newDualCursorInstance(args, argPositions);
    }

    static Function newCursorHiInstance(ObjList<Function> args, IntList argPositions) throws SqlException {
        final Function arg = args.getQuick(0);
        final Function loFunc = args.getQuick(1);
        final Function hiFunc = args.getQuick(2);
        final int hiPos = argPositions.getQuick(2);
        final RecordCursorFactory hiFactory = hiFunc.getRecordCursorFactory();
        final int hiColumnType = assertComparableCursorColumn(hiFactory, hiPos);
        final int argType = resolveLeftTimestampType(arg, argPositions.getQuick(0), hiColumnType, ColumnType.UNDEFINED);
        return new CursorHiFunc(
                arg,
                loFunc,
                hiFunc,
                hiFactory,
                ColumnType.getTimestampDriver(argType),
                hiColumnType,
                ColumnType.getTimestampType(loFunc.getType()),
                hiPos
        );
    }

    static Function newCursorLoInstance(ObjList<Function> args, IntList argPositions) throws SqlException {
        final Function arg = args.getQuick(0);
        final Function loFunc = args.getQuick(1);
        final Function hiFunc = args.getQuick(2);
        final int loPos = argPositions.getQuick(1);
        final RecordCursorFactory loFactory = loFunc.getRecordCursorFactory();
        final int loColumnType = assertComparableCursorColumn(loFactory, loPos);
        final int argType = resolveLeftTimestampType(arg, argPositions.getQuick(0), loColumnType, ColumnType.UNDEFINED);
        return new CursorLoFunc(
                arg,
                loFunc,
                hiFunc,
                loFactory,
                ColumnType.getTimestampDriver(argType),
                loColumnType,
                ColumnType.getTimestampType(hiFunc.getType()),
                loPos
        );
    }

    static Function newDualCursorInstance(ObjList<Function> args, IntList argPositions) throws SqlException {
        final Function arg = args.getQuick(0);
        final Function loFunc = args.getQuick(1);
        final Function hiFunc = args.getQuick(2);
        final int loPos = argPositions.getQuick(1);
        final int hiPos = argPositions.getQuick(2);
        final RecordCursorFactory loFactory = loFunc.getRecordCursorFactory();
        final int loColumnType = assertComparableCursorColumn(loFactory, loPos);
        final RecordCursorFactory hiFactory = hiFunc.getRecordCursorFactory();
        final int hiColumnType = assertComparableCursorColumn(hiFactory, hiPos);
        final int argType = resolveLeftTimestampType(arg, argPositions.getQuick(0), loColumnType, hiColumnType);
        return new DualCursorFunc(
                arg,
                loFunc,
                hiFunc,
                loFactory,
                hiFactory,
                ColumnType.getTimestampDriver(argType),
                loColumnType,
                hiColumnType,
                loPos,
                hiPos
        );
    }

    private static int assertComparableCursorColumn(RecordCursorFactory factory, int position) throws SqlException {
        final RecordMetadata metadata = ScalarSubQueryUtils.assertSingleColumn(factory, position);
        final int columnType = metadata.getColumnType(0);
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.TIMESTAMP, ColumnType.NULL, ColumnType.STRING, ColumnType.VARCHAR -> columnType;
            default ->
                    throw SqlException.$(position, "cannot compare TIMESTAMP and ").put(ColumnType.nameOf(columnType));
        };
    }

    /**
     * Reads the single scalar value of a sub-query bound during {@code init()} and converts it
     * to the left operand's timestamp precision, the same conversion {@code between(NNN)} and
     * the interval intrinsic apply to their bounds. An empty sub-query yields
     * {@link Numbers#LONG_NULL}, which makes the predicate false.
     */
    private static long readCursorBound(
            RecordCursorFactory factory,
            int columnType,
            TimestampDriver driver,
            SqlExecutionContext executionContext,
            int position
    ) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(executionContext)) {
            if (!cursor.hasNext()) {
                return Numbers.LONG_NULL;
            }
            final Record record = cursor.getRecord();
            final long value;
            switch (ColumnType.tagOf(columnType)) {
                case ColumnType.STRING -> {
                    final CharSequence str = record.getStrA(0);
                    try {
                        value = driver.parseFloorLiteral(str);
                    } catch (NumericException e) {
                        throw SqlException.$(position, "the cursor selected invalid timestamp value: ").put(str);
                    }
                }
                case ColumnType.VARCHAR -> {
                    final Utf8Sequence str = record.getVarcharA(0);
                    try {
                        value = driver.parseFloorLiteral(str);
                    } catch (NumericException e) {
                        throw SqlException.$(position, "the cursor selected invalid timestamp value: ").put(str);
                    }
                }
                default -> value = driver.from(record.getTimestamp(0), ColumnType.getTimestampType(columnType));
            }
            ScalarSubQueryUtils.assertNoMoreRows(cursor, position);
            return value;
        }
    }

    private static int resolveLeftTimestampType(
            Function arg,
            int argPosition,
            int loCursorColumnType,
            int hiCursorColumnType
    ) throws SqlException {
        final int argColType = arg.getType();
        switch (ColumnType.tagOf(argColType)) {
            case ColumnType.TIMESTAMP:
                return ColumnType.getTimestampType(argColType);
            case ColumnType.NULL:
                // a NULL left operand always evaluates to false; borrow the precision of a
                // timestamp-typed cursor bound, same as the =(NC) factory does
                final boolean isLoTimestamp = ColumnType.isTimestamp(loCursorColumnType);
                final boolean isHiTimestamp = ColumnType.isTimestamp(hiCursorColumnType);
                if (isLoTimestamp && isHiTimestamp) {
                    return ColumnType.getHigherPrecisionTimestampType(loCursorColumnType, hiCursorColumnType);
                }
                if (isLoTimestamp) {
                    return loCursorColumnType;
                }
                if (isHiTimestamp) {
                    return hiCursorColumnType;
                }
                // fall through to the error
            default:
                throw SqlException.$(argPosition, "left operand must be a TIMESTAMP, found: ").put(ColumnType.nameOf(argColType));
        }
    }

    private static class CursorHiFunc extends BooleanFunction implements TernaryFunction {
        private final Function arg;
        private final TimestampDriver driver;
        private final int hiColumnType;
        private final RecordCursorFactory hiFactory;
        private final Function hiFunc;
        private final int hiPos;
        private final Function loFunc;
        private final int loValueType;
        private long hiEpoch;
        private boolean stateInherited = false;
        private boolean stateShared = false;

        public CursorHiFunc(
                Function arg,
                Function loFunc,
                Function hiFunc,
                RecordCursorFactory hiFactory,
                TimestampDriver driver,
                int hiColumnType,
                int loValueType,
                int hiPos
        ) {
            this.arg = arg;
            this.loFunc = loFunc;
            this.hiFunc = hiFunc;
            this.hiFactory = hiFactory;
            this.driver = driver;
            this.hiColumnType = hiColumnType;
            this.loValueType = loValueType;
            this.hiPos = hiPos;
        }

        @Override
        public boolean getBool(Record rec) {
            if (hiEpoch == Numbers.LONG_NULL) {
                return false;
            }
            final long value = arg.getTimestamp(rec);
            if (value == Numbers.LONG_NULL) {
                return false;
            }
            final long loTs = driver.from(loFunc.getTimestamp(rec), loValueType);
            if (loTs == Numbers.LONG_NULL) {
                return false;
            }
            return Math.min(loTs, hiEpoch) <= value && value <= Math.max(loTs, hiEpoch);
        }

        @Override
        public Function getCenter() {
            return arg;
        }

        @Override
        public Function getLeft() {
            return loFunc;
        }

        @Override
        public Function getRight() {
            return hiFunc;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            TernaryFunction.super.init(symbolTableSource, executionContext);
            if (stateInherited) {
                return;
            }
            this.stateShared = false;
            hiEpoch = readCursorBound(hiFactory, hiColumnType, driver, executionContext, hiPos);
        }

        @Override
        public boolean isThreadSafe() {
            return arg.isThreadSafe() && loFunc.isThreadSafe();
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof CursorHiFunc thatF) {
                thatF.hiEpoch = hiEpoch;
                thatF.stateInherited = this.stateShared = true;
            }
            TernaryFunction.super.offerStateTo(that);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val(" between ").val(loFunc).val(" and ").val(hiFunc);
            if (stateShared) {
                sink.val(" [state-shared]");
            }
        }
    }

    private static class CursorLoFunc extends BooleanFunction implements TernaryFunction {
        private final Function arg;
        private final TimestampDriver driver;
        private final Function hiFunc;
        private final int hiValueType;
        private final int loColumnType;
        private final RecordCursorFactory loFactory;
        private final Function loFunc;
        private final int loPos;
        private long loEpoch;
        private boolean stateInherited = false;
        private boolean stateShared = false;

        public CursorLoFunc(
                Function arg,
                Function loFunc,
                Function hiFunc,
                RecordCursorFactory loFactory,
                TimestampDriver driver,
                int loColumnType,
                int hiValueType,
                int loPos
        ) {
            this.arg = arg;
            this.loFunc = loFunc;
            this.hiFunc = hiFunc;
            this.loFactory = loFactory;
            this.driver = driver;
            this.loColumnType = loColumnType;
            this.hiValueType = hiValueType;
            this.loPos = loPos;
        }

        @Override
        public boolean getBool(Record rec) {
            if (loEpoch == Numbers.LONG_NULL) {
                return false;
            }
            final long value = arg.getTimestamp(rec);
            if (value == Numbers.LONG_NULL) {
                return false;
            }
            final long hiTs = driver.from(hiFunc.getTimestamp(rec), hiValueType);
            if (hiTs == Numbers.LONG_NULL) {
                return false;
            }
            return Math.min(loEpoch, hiTs) <= value && value <= Math.max(loEpoch, hiTs);
        }

        @Override
        public Function getCenter() {
            return arg;
        }

        @Override
        public Function getLeft() {
            return loFunc;
        }

        @Override
        public Function getRight() {
            return hiFunc;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            TernaryFunction.super.init(symbolTableSource, executionContext);
            if (stateInherited) {
                return;
            }
            this.stateShared = false;
            loEpoch = readCursorBound(loFactory, loColumnType, driver, executionContext, loPos);
        }

        @Override
        public boolean isThreadSafe() {
            return arg.isThreadSafe() && hiFunc.isThreadSafe();
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof CursorLoFunc thatF) {
                thatF.loEpoch = loEpoch;
                thatF.stateInherited = this.stateShared = true;
            }
            TernaryFunction.super.offerStateTo(that);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val(" between ").val(loFunc).val(" and ").val(hiFunc);
            if (stateShared) {
                sink.val(" [state-shared]");
            }
        }
    }

    private static class DualCursorFunc extends BooleanFunction implements TernaryFunction {
        private final Function arg;
        private final TimestampDriver driver;
        private final int hiColumnType;
        private final RecordCursorFactory hiFactory;
        private final Function hiFunc;
        private final int hiPos;
        private final int loColumnType;
        private final RecordCursorFactory loFactory;
        private final Function loFunc;
        private final int loPos;
        private long hiEpoch;
        private long loEpoch;
        private boolean stateInherited = false;
        private boolean stateShared = false;

        public DualCursorFunc(
                Function arg,
                Function loFunc,
                Function hiFunc,
                RecordCursorFactory loFactory,
                RecordCursorFactory hiFactory,
                TimestampDriver driver,
                int loColumnType,
                int hiColumnType,
                int loPos,
                int hiPos
        ) {
            this.arg = arg;
            this.loFunc = loFunc;
            this.hiFunc = hiFunc;
            this.loFactory = loFactory;
            this.hiFactory = hiFactory;
            this.driver = driver;
            this.loColumnType = loColumnType;
            this.hiColumnType = hiColumnType;
            this.loPos = loPos;
            this.hiPos = hiPos;
        }

        @Override
        public boolean getBool(Record rec) {
            if (loEpoch == Numbers.LONG_NULL || hiEpoch == Numbers.LONG_NULL) {
                return false;
            }
            final long value = arg.getTimestamp(rec);
            if (value == Numbers.LONG_NULL) {
                return false;
            }
            return loEpoch <= value && value <= hiEpoch;
        }

        @Override
        public Function getCenter() {
            return arg;
        }

        @Override
        public Function getLeft() {
            return loFunc;
        }

        @Override
        public Function getRight() {
            return hiFunc;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            TernaryFunction.super.init(symbolTableSource, executionContext);
            if (stateInherited) {
                return;
            }
            this.stateShared = false;
            loEpoch = readCursorBound(loFactory, loColumnType, driver, executionContext, loPos);
            hiEpoch = readCursorBound(hiFactory, hiColumnType, driver, executionContext, hiPos);
            // normalize reversed bounds once per execution, matching between(NNN)
            if (loEpoch != Numbers.LONG_NULL && hiEpoch != Numbers.LONG_NULL && loEpoch > hiEpoch) {
                final long tmp = loEpoch;
                loEpoch = hiEpoch;
                hiEpoch = tmp;
            }
        }

        @Override
        public boolean isThreadSafe() {
            return arg.isThreadSafe();
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof DualCursorFunc thatF) {
                thatF.loEpoch = loEpoch;
                thatF.hiEpoch = hiEpoch;
                thatF.stateInherited = this.stateShared = true;
            }
            TernaryFunction.super.offerStateTo(that);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg).val(" between ").val(loFunc).val(" and ").val(hiFunc);
            if (stateShared) {
                sink.val(" [state-shared]");
            }
        }
    }
}
