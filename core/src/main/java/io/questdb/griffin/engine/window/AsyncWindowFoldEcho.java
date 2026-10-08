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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.window.SumDoubleWindowFunctionFactory;

/**
 * A worker's stand-in for {@code sum(x) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)}
 * of a single key, see {@link AsyncWindowSplitPlan#OP_FOLD}: it outputs each row's argument as it
 * is, and the query's thread folds the running sum over the task's rows, from the value the sum had
 * before them, with the serial function's own arithmetic. A carry added to sums computed from
 * scratch would add the same values in another order; the fold adds them in the serial order, so
 * the sums are the serial ones, bit for bit.
 */
public class AsyncWindowFoldEcho extends SumDoubleWindowFunctionFactory.SumOverUnboundedRowsFrameFunction {
    private double value;

    /**
     * @param arg the argument of the sum this function stands in for, which it takes over
     */
    public AsyncWindowFoldEcho(Function arg) {
        super(arg);
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
