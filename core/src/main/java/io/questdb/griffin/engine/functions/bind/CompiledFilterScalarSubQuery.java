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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.lt.AbstractScalarCursorFunction;

/**
 * A compiled filter's bind variable that reads the value of an uncorrelated scalar sub-query, so
 * that {@code col > (SELECT ...)} stays in the JIT filter: the sub-query is a constant of the
 * execution. The value is the one the Java filter's {@link AbstractScalarCursorFunction} computed
 * for the execution, at the width and with the NULL the Java comparison uses (a DOUBLE NaN, a
 * {@code LONG_NULL}, an {@code INT_NULL} for an empty sub-query), so both filters compare the same
 * numbers. The sub-query runs once per execution, by whichever of the source's {@code init()} and
 * this link's {@code init()} a consumer calls first (see
 * {@link AbstractScalarCursorFunction#initFromLink}), so the link binds the value of the execution
 * whatever the order in which a consumer initialises its Java filter and its bind variables. The
 * link does not own the source: the Java filter does.
 */
public final class CompiledFilterScalarSubQuery {

    private CompiledFilterScalarSubQuery() {
    }

    /**
     * @return a new bind variable function for the source's value; DOUBLE, LONG or INT
     */
    public static Function newLink(AbstractScalarCursorFunction source) {
        return switch (source.getScalarType()) {
            case ColumnType.DOUBLE -> new DoubleLink(source);
            case ColumnType.LONG -> new LongLink(source);
            case ColumnType.INT -> new IntLink(source);
            default -> throw new UnsupportedOperationException();
        };
    }

    private static void toPlan(PlanSink sink, AbstractScalarCursorFunction source) {
        sink.val("scalar sub-query of ").val(source);
    }

    private static final class DoubleLink extends DoubleFunction {
        private final AbstractScalarCursorFunction source;

        private DoubleLink(AbstractScalarCursorFunction source) {
            this.source = source;
        }

        @Override
        public double getDouble(Record rec) {
            return source.getScalarDouble();
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            source.initFromLink(executionContext);
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            CompiledFilterScalarSubQuery.toPlan(sink, source);
        }
    }

    private static final class IntLink extends IntFunction {
        private final AbstractScalarCursorFunction source;

        private IntLink(AbstractScalarCursorFunction source) {
            this.source = source;
        }

        @Override
        public int getInt(Record rec) {
            return source.getScalarInt();
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            source.initFromLink(executionContext);
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            CompiledFilterScalarSubQuery.toPlan(sink, source);
        }
    }

    private static final class LongLink extends LongFunction {
        private final AbstractScalarCursorFunction source;

        private LongLink(AbstractScalarCursorFunction source) {
            this.source = source;
        }

        @Override
        public long getLong(Record rec) {
            return source.getScalarLong();
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            source.initFromLink(executionContext);
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            CompiledFilterScalarSubQuery.toPlan(sink, source);
        }
    }
}
