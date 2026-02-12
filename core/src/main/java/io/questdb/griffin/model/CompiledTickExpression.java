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

package io.questdb.griffin.model;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UntypedFunction;
import io.questdb.std.LongList;
import io.questdb.std.str.StringSink;

/**
 * A Function that represents a pre-parsed tick expression containing date variables
 * ($now, $today, $yesterday, $tomorrow). Re-evaluates the expression on each query
 * execution to produce dynamic intervals, ensuring that cached queries always use
 * the current time.
 * <p>
 * At compile time, the expression string is stored. At runtime, {@link #init} captures
 * the current "now" from the execution context, and {@link #evaluate} calls
 * {@link IntervalUtils#parseTickExpr} with that timestamp to produce [lo, hi] pairs.
 */
public class CompiledTickExpression extends UntypedFunction {
    private final CairoConfiguration configuration;
    private final String expression;
    private final StringSink sink = new StringSink();
    private final TimestampDriver timestampDriver;
    private long now;

    public CompiledTickExpression(TimestampDriver timestampDriver, CairoConfiguration configuration, CharSequence expression) {
        this.timestampDriver = timestampDriver;
        this.configuration = configuration;
        this.expression = expression.toString();
    }

    /**
     * Evaluates the tick expression with the given "now" timestamp and appends
     * the resulting [lo, hi] interval pairs to outIntervals.
     * This overload is intended for testing without a SqlExecutionContext.
     */
    public void evaluate(LongList outIntervals, long now) throws SqlException {
        this.now = now;
        evaluate(outIntervals);
    }

    /**
     * Evaluates the tick expression using the captured "now" timestamp and appends
     * the resulting [lo, hi] interval pairs to outIntervals.
     */
    public void evaluate(LongList outIntervals) throws SqlException {
        sink.clear();
        IntervalUtils.parseTickExpr(
                timestampDriver,
                configuration,
                expression,
                0,
                expression.length(),
                0,
                outIntervals,
                IntervalOperation.INTERSECT,
                sink,
                true,
                now
        );
    }

    @Override
    public int getType() {
        return ColumnType.UNDEFINED;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
        now = executionContext.getNow(timestampDriver.getTimestampType());
    }

    @Override
    public boolean isNonDeterministic() {
        return true;
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("tick('").val(expression).val("')");
    }
}
