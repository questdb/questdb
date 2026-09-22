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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ScalarTimestampBoundHolder;
import io.questdb.std.Numbers;

/**
 * Residual-side reader for a scalar sub-query designated-timestamp bound that is also used for
 * interval pruning. Instead of opening the sub-query a second time (which could observe a different
 * commit than the pruning open and drop qualifying rows), it returns the single value published by
 * the pruning bound into the shared {@link ScalarTimestampBoundHolder}.
 * <p>
 * Owns nothing: the wrapped sub-query factory is owned and closed by the pruning bound
 * ({@link ScalarSubQueryTimestampFunction}). Per-worker filter clones reference the same holder, so
 * every worker reads the identical frozen bound.
 * <p>
 * The holder read is hoisted into {@link #init}: the row loop must not repeat a volatile
 * acquire-load (an {@code ldar} on ARM64, non-hoistable by the JIT) once per filtered row when the
 * value is frozen for the whole execution.
 *
 * @see io.questdb.griffin.WhereClauseParser
 * @see io.questdb.griffin.FunctionParser
 */
public final class ScalarSubQueryBoundRefFunction extends TimestampFunction {
    private final ScalarTimestampBoundHolder holder;
    // Execution-scoped snapshot of holder.value(), taken in init(). Plain field on purpose: it is
    // written on the frame-open thread before any row is filtered and before the async filter
    // dispatches to workers, so the existing dispatch edge publishes it.
    private long value = Numbers.LONG_NULL;

    public ScalarSubQueryBoundRefFunction(ScalarTimestampBoundHolder holder) {
        super(holder.getTimestampType());
        this.holder = holder;
    }

    @Override
    public long getTimestamp(Record rec) {
        return value;
    }

    /**
     * Snapshots the frozen bound once per execution. The owning pruning bound publishes it from its
     * own {@code init()} at partition-frame open, which happens-before this one on both the serial
     * path ({@code AbstractPageFrameRecordCursorFactory.getCursor()} opens the partition-frame
     * cursor, and with it the runtime interval model, before it builds the record cursor) and the
     * async path ({@code PageFrameSequence.of()} opens the page-frame cursor before
     * {@code atom.init()}). Per-worker filter clones are init()ed from that same frame-open thread
     * ({@code AsyncFilterAtom.init()}), so each clone snapshots the identical published value.
     */
    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        // Fail fast under -ea if that publish-before-init ordering is ever violated. The owner
        // disarms the flag at the top of the same init(), so this stays armed on every execution of
        // a reused/cached factory, not just the first one.
        assert holder.isPublished() : "scalar sub-query bound read before it was published";
        value = holder.value();
    }

    // Deliberately does NOT override isNonDeterministic(). The holder mirrors the wrapped sub-query
    // factory's RecordCursorFactory#isNonDeterministic(), a fail-safe optimizer hint defaulting to
    // true (an index-driven sub-query scan reports true even for a fixed-literal key), while
    // Function#isNonDeterministic() is the opposite polarity: a fail-open legality flag read by the
    // materialized-view guard in FunctionParser. Bridging the two makes the enclosing comparison
    // operator inherit the fail-safe true, so the guard rejects DDL that compiled on master - and
    // ADD INDEX on the sub-query's table flips the hint under a live view, failing its refresh
    // recompile and invalidating it cluster-wide. Genuinely non-deterministic bounds never reach
    // this reader: they are rejected while the sub-query body is generated (FunctionParser guard)
    // and they fail isStableWithinExecution(), so no holder is installed (WhereClauseParser). See
    // CursorFunction for the same polarity note on the direct path.

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
