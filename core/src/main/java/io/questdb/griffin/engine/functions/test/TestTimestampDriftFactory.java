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

package io.questdb.griffin.engine.functions.test;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Dev-mode test function {@code test_timestamp_drift(N)}: returns its TIMESTAMP argument shifted
 * forward by {@code STEP} for every <em>previous</em> evaluation, i.e. the value it yields
 * <em>advances on every open</em> ({@code arg}, {@code arg + STEP}, {@code arg + 2*STEP}, ...).
 * <p>
 * This is the deterministic, single-threaded stand-in for the production hazard that
 * {@link io.questdb.griffin.model.ScalarTimestampBoundHolder} exists to eliminate: a commit landing
 * on the bound table between the interval-pruning open and the residual filter's open, which makes
 * the two sides read <em>different</em> values and silently drops qualifying rows. Wrapping a
 * scalar sub-query bound in this function turns that timing-dependent race into a fixed outcome -
 * if the bound is evaluated once and shared, pruning and residual necessarily agree and the query
 * matches a literal-bound oracle; if either side re-opens the sub-query, the second open returns a
 * strictly later bound and the result set diverges from the oracle.
 * <p>
 * Deliberately does <em>not</em> report itself as non-deterministic: pruning must stay engaged so
 * the shared-value path is what is under test (the declined-pruning path has its own coverage in
 * {@code ScalarSubqueryDeclinedPruningTest}). {@link #STEP} is expressed in the argument's native
 * timestamp units and must be set by the test; {@link #OPENS} counts evaluations and should be
 * reset after compilation, since a JIT serialization attempt can evaluate the bound once outside
 * any execution. Outside dev mode the function folds to its argument, so it is inert in production.
 */
public class TestTimestampDriftFactory implements FunctionFactory {
    /**
     * Number of evaluations since the last reset. For a scalar sub-query (exactly one row per open)
     * this equals the number of times the sub-query was opened.
     */
    public static final AtomicLong OPENS = new AtomicLong();
    /**
     * Units added to the argument per preceding evaluation, in the argument's native timestamp
     * units. Must be large enough to move the bound across a row boundary in the test data.
     */
    public static final AtomicLong STEP = new AtomicLong();
    private static final String NAME = "test_timestamp_drift";

    @Override
    public String getSignature() {
        return NAME + "(N)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final Function tsFunc = args.getQuick(0);
        if (configuration.isDevModeEnabled()) {
            return new Func(tsFunc, ColumnType.getTimestampType(tsFunc.getType()));
        }
        return tsFunc;
    }

    private static class Func extends TimestampFunction implements UnaryFunction {
        private final Function tsFunc;

        private Func(Function tsFunc, int timestampType) {
            super(timestampType);
            this.tsFunc = tsFunc;
        }

        @Override
        public Function getArg() {
            return tsFunc;
        }

        @Override
        public long getTimestamp(Record rec) {
            final long base = tsFunc.getTimestamp(rec);
            // NULL is the "no bound" sentinel; drifting it would change the predicate's shape
            // rather than its value, which is not what this fixture is for.
            if (base == Numbers.LONG_NULL) {
                return base;
            }
            return base + STEP.get() * OPENS.getAndIncrement();
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
        }
    }
}
