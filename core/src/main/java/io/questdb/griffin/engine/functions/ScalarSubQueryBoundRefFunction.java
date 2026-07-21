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

package io.questdb.griffin.engine.functions;

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.model.ScalarTimestampBoundHolder;

/**
 * Residual-side reader for a scalar sub-query designated-timestamp bound that is also used for
 * interval pruning. Instead of opening the sub-query a second time (which could observe a different
 * commit than the pruning open and drop qualifying rows), it returns the single value published by
 * the pruning bound into the shared {@link ScalarTimestampBoundHolder}.
 * <p>
 * Owns nothing: the wrapped sub-query factory is owned and closed by the pruning bound
 * ({@link ScalarSubQueryTimestampFunction}). Per-worker filter clones reference the same holder, so
 * every worker reads the identical frozen bound.
 *
 * @see io.questdb.griffin.WhereClauseParser
 * @see io.questdb.griffin.FunctionParser
 */
public final class ScalarSubQueryBoundRefFunction extends TimestampFunction {
    private final ScalarTimestampBoundHolder holder;

    public ScalarSubQueryBoundRefFunction(ScalarTimestampBoundHolder holder) {
        super(holder.getTimestampType());
        this.holder = holder;
    }

    @Override
    public long getTimestamp(Record rec) {
        return holder.value();
    }

    @Override
    public boolean isNonDeterministic() {
        return holder.isNonDeterministic();
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    // The value is a single frozen bound shared with the pruning inverter, so it is - by
    // construction - identical across every open within one execution.
    @Override
    public boolean isStableWithinExecution() {
        return true;
    }

    // Stateless reader of a volatile value: safe to share across worker threads. Also spares
    // per-worker filter re-compiles when this is the only non-thread-safe filter component.
    @Override
    public boolean isThreadSafe() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("scalar_subquery_bound");
    }
}
