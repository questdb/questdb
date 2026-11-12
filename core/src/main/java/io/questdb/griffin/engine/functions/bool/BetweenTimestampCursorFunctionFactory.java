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
import io.questdb.griffin.engine.functions.TimestampBounds;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
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
        final TimestampDriver driver = ColumnType.getTimestampDriver(argType);
        final int loValueType = ColumnType.getTimestampType(loFunc.getType());
        // a constant or runtime-constant lower bound is invariant across rows: read it once per
        // execution during init() instead of re-evaluating it (getter + precision conversion +
        // min/max) per row, mirroring the ConstFunc specialization in between(NNN)
        if (loFunc.isConstant() || loFunc.isRuntimeConstant()) {
            return new CursorScalarFunc(arg, loFunc, hiFunc, hiFactory, driver, hiColumnType, hiPos, true);
        }
        return new CursorHiFunc(
                arg,
                loFunc,
                hiFunc,
                hiFactory,
                driver,
                hiColumnType,
                loValueType,
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
        final TimestampDriver driver = ColumnType.getTimestampDriver(argType);
        final int hiValueType = ColumnType.getTimestampType(hiFunc.getType());
        // a constant or runtime-constant upper bound is invariant across rows: read it once per
        // execution during init() instead of re-evaluating it (getter + precision conversion +
        // min/max) per row, mirroring the ConstFunc specialization in between(NNN)
        if (hiFunc.isConstant() || hiFunc.isRuntimeConstant()) {
            return new CursorScalarFunc(arg, loFunc, hiFunc, loFactory, driver, loColumnType, loPos, false);
        }
        return new CursorLoFunc(
                arg,
                loFunc,
                hiFunc,
                loFactory,
                driver,
                loColumnType,
                hiValueType,
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
     * Reads the single scalar value of a sub-query bound during {@code init()} into the left operand's timestamp
     * type: out holds the first value of the type at or after the bound, then the last value at or before it.
     * The two are equal unless the bound lies between two values of the type, as a finer one can. An empty
     * sub-query yields {@link Numbers#LONG_NULL} twice, which makes the predicate false.
     */
    private static void readCursorBound(
            RecordCursorFactory factory,
            int columnType,
            TimestampDriver driver,
            SqlExecutionContext executionContext,
            int position,
            LongList out
    ) throws SqlException {
        out.clear();
        try (RecordCursor cursor = factory.getCursor(executionContext)) {
            if (!cursor.hasNext()) {
                out.add(Numbers.LONG_NULL, Numbers.LONG_NULL);
                return;
            }
            final Record record = cursor.getRecord();
            final long lower;
            final long upper;
            switch (ColumnType.tagOf(columnType)) {
                case ColumnType.STRING -> {
                    final CharSequence str = record.getStrA(0);
                    try {
                        upper = driver.parseFloorLiteral(str);
                    } catch (NumericException e) {
                        throw SqlException.$(position, "the cursor selected invalid timestamp value: ").put(str);
                    }
                    lower = upper != Numbers.LONG_NULL && !TimestampBounds.isExactText(driver, str) ? upper + 1 : upper;
                }
                case ColumnType.VARCHAR -> {
                    final Utf8Sequence str = record.getVarcharA(0);
                    try {
                        upper = driver.parseFloorLiteral(str);
                    } catch (NumericException e) {
                        throw SqlException.$(position, "the cursor selected invalid timestamp value: ").put(str);
                    }
                    lower = upper != Numbers.LONG_NULL && !TimestampBounds.isExactText(driver, str) ? upper + 1 : upper;
                }
                default -> {
                    final long value = record.getTimestamp(0);
                    final int valueType = ColumnType.getTimestampType(columnType);
                    if (value == Numbers.LONG_NULL || driver.compareToRange(value, valueType) != 0) {
                        // a value past the range of the left operand's type fails to convert
                        lower = upper = driver.from(value, valueType);
                    } else {
                        lower = driver.lowerBound(value, valueType, false);
                        upper = driver.upperBound(value, valueType, false);
                    }
                }
            }
            ScalarSubQueryUtils.assertNoMoreRows(cursor, position);
            out.add(lower, upper);
        }
    }

    // where a cursor bound [lower, upper] lies against a value: after it, at it or before it
    private static int compareBound(long lower, long upper, long value) {
        return value < lower ? 1 : (value > upper ? -1 : 0);
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
        private final LongList bound = new LongList(2);
        // the cursor bound lies at or between hiEpoch and hiEpochHi
        private long hiEpoch;
        private long hiEpochHi;
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
            final long loTs = loFunc.getTimestamp(rec);
            if (loTs == Numbers.LONG_NULL) {
                return false;
            }
            // the value lies at or between the bounds, in either order; nothing converts
            return driver.compareFrom(loTs, loValueType, value) * compareBound(hiEpoch, hiEpochHi, value) <= 0;
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
            readCursorBound(hiFactory, hiColumnType, driver, executionContext, hiPos, bound);
            hiEpoch = bound.getQuick(0);
            hiEpochHi = bound.getQuick(1);
        }

        @Override
        public boolean isThreadSafe() {
            return arg.isThreadSafe() && loFunc.isThreadSafe();
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof CursorHiFunc thatF) {
                thatF.hiEpoch = hiEpoch;
                thatF.hiEpochHi = hiEpochHi;
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
        private final LongList bound = new LongList(2);
        // the cursor bound lies at or between loEpoch and loEpochHi
        private long loEpoch;
        private long loEpochHi;
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
            final long hiTs = hiFunc.getTimestamp(rec);
            if (hiTs == Numbers.LONG_NULL) {
                return false;
            }
            // the value lies at or between the bounds, in either order; nothing converts
            return driver.compareFrom(hiTs, hiValueType, value) * compareBound(loEpoch, loEpochHi, value) <= 0;
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
            readCursorBound(loFactory, loColumnType, driver, executionContext, loPos, bound);
            loEpoch = bound.getQuick(0);
            loEpochHi = bound.getQuick(1);
        }

        @Override
        public boolean isThreadSafe() {
            return arg.isThreadSafe() && hiFunc.isThreadSafe();
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof CursorLoFunc thatF) {
                thatF.loEpoch = loEpoch;
                thatF.loEpochHi = loEpochHi;
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

    /**
     * Handles the mixed signatures {@code between(NNC)} / {@code between(NCN)} when the non-cursor
     * bound is a constant or runtime-constant. Both bounds resolve to fixed epochs during
     * {@code init()} - the cursor bound from its sub-query, the non-cursor bound from a single
     * {@code getTimestamp(null)} plus one precision conversion - so {@code getBool} reduces to the
     * same O(1)/row range test as {@link DualCursorFunc}, and reversed bounds normalize once per
     * execution instead of per row. Row-dependent non-cursor bounds keep using the per-row
     * {@link CursorHiFunc} / {@link CursorLoFunc}.
     */
    private static class CursorScalarFunc extends BooleanFunction implements TernaryFunction {
        private final Function arg;
        private final int cursorColumnType;
        private final RecordCursorFactory cursorFactory;
        private final boolean cursorIsHi;
        private final int cursorPos;
        private final TimestampDriver driver;
        private final Function hiFunc;
        private final Function loFunc;
        private final Function scalarFunc;
        private final LongList bound = new LongList(2);
        private long hiEpoch;
        private long loEpoch;
        private boolean stateInherited = false;
        private boolean stateShared = false;

        public CursorScalarFunc(
                Function arg,
                Function loFunc,
                Function hiFunc,
                RecordCursorFactory cursorFactory,
                TimestampDriver driver,
                int cursorColumnType,
                int cursorPos,
                boolean cursorIsHi
        ) {
            this.arg = arg;
            this.loFunc = loFunc;
            this.hiFunc = hiFunc;
            this.cursorFactory = cursorFactory;
            this.driver = driver;
            this.cursorColumnType = cursorColumnType;
            this.cursorPos = cursorPos;
            this.cursorIsHi = cursorIsHi;
            this.scalarFunc = cursorIsHi ? loFunc : hiFunc;
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
            readCursorBound(cursorFactory, cursorColumnType, driver, executionContext, cursorPos, bound);
            final long cursorLo = bound.getQuick(0);
            final long cursorHi = bound.getQuick(1);
            // the non-cursor bound is (runtime-)constant, so a null record yields the per-execution value
            if (cursorLo == Numbers.LONG_NULL || !TimestampBounds.bounds(driver, scalarFunc, bound)) {
                loEpoch = hiEpoch = Numbers.LONG_NULL;
                return;
            }
            // the bounds come in either order; a scalar bound with no value of the type on its side, past the
            // range of the type, gives way to the cursor bound
            final long scalarLo = bound.getQuick(0);
            final long scalarHi = bound.getQuick(1);
            loEpoch = scalarLo == Numbers.LONG_NULL ? cursorLo : Math.min(scalarLo, cursorLo);
            hiEpoch = scalarHi == Numbers.LONG_NULL ? cursorHi : Math.max(scalarHi, cursorHi);
        }

        @Override
        public boolean isThreadSafe() {
            // the non-cursor bound is folded to a cached epoch, so per-row work only touches arg
            return arg.isThreadSafe();
        }

        @Override
        public void offerStateTo(Function that) {
            if (that instanceof CursorScalarFunc thatF) {
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
        private final LongList bound = new LongList(2);
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
            readCursorBound(loFactory, loColumnType, driver, executionContext, loPos, bound);
            final long fromLo = bound.getQuick(0);
            final long fromHi = bound.getQuick(1);
            readCursorBound(hiFactory, hiColumnType, driver, executionContext, hiPos, bound);
            final long toLo = bound.getQuick(0);
            final long toHi = bound.getQuick(1);
            if (fromLo == Numbers.LONG_NULL || toLo == Numbers.LONG_NULL) {
                loEpoch = hiEpoch = Numbers.LONG_NULL;
            } else {
                // the bounds come in either order, as in between(NNN)
                loEpoch = Math.min(fromLo, toLo);
                hiEpoch = Math.max(fromHi, toHi);
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
