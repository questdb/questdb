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


package io.questdb.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunction;
import io.questdb.griffin.engine.functions.ScalarSubQueryTimestampFunction;
import io.questdb.griffin.engine.functions.columns.BindableColumn;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.griffin.model.ScalarTimestampBoundHolder;
import io.questdb.griffin.model.TimestampMonotonicInverter;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Interval;
import io.questdb.std.Misc;

/**
 * Builds the runtime bounds of interval extraction with a {@link FunctionInstantiator} and inverts monotonic
 * operands through the functions it builds.
 */
public final class InstantiatedIntervalBounds implements IntervalBoundSource {
    private FunctionInstantiator instantiator;

    @Override
    public Function compileTickExpr(TimestampDriver timestampDriver, CairoConfiguration configuration, CharSequence seq, int lo, int lim, int position)
            throws SqlException {
        return IntervalUtils.compileTickExpr(timestampDriver, configuration, seq, lo, lim, position);
    }

    @Override
    public Function instantiate(BoundExpression expression, OutputSchema input, SqlExecutionContext executionContext) throws SqlException {
        return instantiator.instantiate(expression, input, executionContext);
    }

    @Override
    public Function instantiateTimestampCursor(CursorExpression cursor, SqlExecutionContext executionContext) throws SqlException {
        final Function function = instantiator.instantiateSubquery(cursor, executionContext);
        try {
            return ColumnType.isTimestamp(cursor.getDataType())
                    ? new ScalarSubQueryTimestampFunction(function, cursor.getPosition(), cursor.getDataType())
                    : new ScalarSubQueryTimestampFunction(function, cursor.getPosition());
        } catch (Throwable th) {
            Misc.free(function, th);
            throw th;
        }
    }

    @Override
    public int invertMonotonic(BoundExpression operand, Function head, Interval io) {
        int soundness = MonotonicTimestampFunction.EXACT;
        while (head instanceof MonotonicTimestampFunction function) {
            final int grade = function.invertTimestampInterval(io);
            if (grade == MonotonicTimestampFunction.NONE) {
                return grade;
            }
            soundness = Math.min(soundness, grade);
            if (io.getLo() > io.getHi()) {
                break;
            }
            head = function.getTimestampArg();
        }
        return soundness;
    }

    @Override
    public int monotonicColumnId(BoundExpression operand, Function head) {
        while (head instanceof MonotonicTimestampFunction monotonic) {
            head = monotonic.getTimestampArg();
        }
        return head instanceof BindableColumn column && ColumnType.isTimestamp(head.getType()) ? column.getColumnId() : -1;
    }

    @Override
    public Function newMonotonicInverter(Function head, Function lo, short loAdjustment, long loValue, Function hi, short hiAdjustment,
                                         long hiValue, boolean isBetween, TimestampDriver timestampDriver) {
        return new TimestampMonotonicInverter(head, lo, loAdjustment, loValue, hi, hiAdjustment, hiValue, isBetween, timestampDriver);
    }

    public void of(FunctionInstantiator instantiator) {
        this.instantiator = instantiator;
    }

    @Override
    public Function parkDeclinedBound(BoundExpression bound, Function function) {
        if (bound instanceof CursorExpression cursor && function instanceof ScalarSubQueryTimestampFunction owner) {
            instantiator.parkSubquery(cursor, owner.releaseCursorFunction());
            owner.close();
            return null;
        }
        return function;
    }

    @Override
    public void publishScalarBound(BoundExpression bound, Function function) {
        if (bound instanceof CursorExpression cursor && function instanceof ScalarSubQueryTimestampFunction owner) {
            final ScalarTimestampBoundHolder holder = new ScalarTimestampBoundHolder(owner.getType());
            owner.setPublishHolder(holder);
            instantiator.shareScalarBound(cursor, holder);
        }
    }
}
