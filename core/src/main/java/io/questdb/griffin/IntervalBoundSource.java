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
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Interval;

/**
 * Supplies interval extraction with the functions of a predicate's runtime bounds and resolves its monotonic
 * timestamp operands: codegen builds them, analysis stands in for them.
 */
public interface IntervalBoundSource {

    Function compileTickExpr(TimestampDriver timestampDriver, CairoConfiguration configuration, CharSequence seq, int lo, int lim, int position)
            throws SqlException;

    Function instantiate(BoundExpression expression, OutputSchema input, SqlExecutionContext executionContext) throws SqlException;

    Function instantiateTimestampCursor(CursorExpression cursor, SqlExecutionContext executionContext) throws SqlException;

    /**
     * Inverts {@code io} through the monotonic chain of the operand, whose function is {@code head}, onto the
     * timestamp; returns the {@link io.questdb.griffin.engine.functions.MonotonicTimestampFunction} grade.
     */
    int invertMonotonic(BoundExpression operand, Function head, Interval io) throws SqlException;

    /**
     * The id of the timestamp column at the end of the operand's monotonic chain, or -1 when there is none.
     */
    int monotonicColumnId(BoundExpression operand, Function head) throws SqlException;

    Function newMonotonicInverter(Function head, Function lo, short loAdjustment, long loValue, Function hi, short hiAdjustment,
                                  long hiValue, boolean isBetween, TimestampDriver timestampDriver);

    /**
     * Hands a sub-query bound the extraction declined to the residual, which adopts it; returns what is left to free.
     */
    Function parkDeclinedBound(BoundExpression bound, Function function);

    /**
     * Lets later consumers of a sub-query bound read the value its pruning bound publishes.
     */
    void publishScalarBound(BoundExpression bound, Function function);
}
