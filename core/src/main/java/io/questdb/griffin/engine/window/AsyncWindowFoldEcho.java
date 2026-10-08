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


package io.questdb.griffin.engine.window;

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.functions.window.SumDoubleWindowFunctionFactory;
import io.questdb.std.Misc;

/**
 * A worker's stand-in for a window function the query's thread computes itself over the worker's
 * rows, in order: {@code sum(x) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)} of a
 * single key, see {@link AsyncWindowSplitPlan#OP_FOLD}, or a bounded frame's DOUBLE {@code avg} or
 * {@code sum}, see {@link AsyncWindowSplitPlan#OP_REPLAY}. It outputs each row's argument as it
 * is, and the query's thread computes the function over them, from the state the rows before them
 * left, with the serial function's own arithmetic. A carry added to sums computed from scratch,
 * or a frame rebuilt from warm-up rows, would add the same values in another order; the fold and
 * the replay add them in the serial order, so the values are the serial ones, bit for bit.
 */
public class AsyncWindowFoldEcho extends SumDoubleWindowFunctionFactory.SumOverUnboundedRowsFrameFunction {
    private final BaseWindowFunction function;
    private double value;

    /**
     * @param function the worker's copy of the function this one stands in for, whose argument
     *                 it reads; it owns the copy, and frees it, argument included, on close
     */
    public AsyncWindowFoldEcho(BaseWindowFunction function) {
        super(function.getWindowArgument());
        this.function = function;
    }

    @Override
    public void close() {
        // the argument is the function's, which frees it
        Misc.free(function);
    }

    @Override
    public void computeNext(Record record) {
        value = arg.getDouble(record);
    }

    @Override
    public double getDouble(Record rec) {
        return value;
    }
}
