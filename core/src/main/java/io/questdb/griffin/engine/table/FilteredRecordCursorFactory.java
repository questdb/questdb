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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.std.Misc;

public class FilteredRecordCursorFactory extends AbstractRecordCursorFactory {
    private RecordCursorFactory base;
    private FilteredRecordCursor cursor;
    private Function filter;

    public FilteredRecordCursorFactory(RecordCursorFactory base, Function filter) {
        super(base.getMetadata());
        // Some optimiser paths split a single WHERE into multiple model
        // filters (e.g. a predicate that does not reference any inner-model
        // column gets pushed past an aggregate while a predicate on the
        // aggregate stays at the outer level), and codegen then wraps each
        // model's filter independently. Collapse those into one factory
        // here so we evaluate the combined boolean once per row instead of
        // chaining two RecordCursor wrappers.
        if (base instanceof FilteredRecordCursorFactory existing) {
            filter = new ChainedAndFilter(existing.filter, filter);
            base = existing.base;
        }
        this.base = base;
        this.cursor = new FilteredRecordCursor(filter);
        this.filter = filter;
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public Function getFilter() {
        return filter;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor cursor = base.getCursor(executionContext);
        try {
            this.cursor.of(cursor, executionContext);
            return this.cursor;
        } catch (Throwable e) {
            Misc.free(cursor);
            throw e;
        }
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public void halfClose() {
        Misc.free(cursor);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return base.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Filter");
        sink.meta("filter").val(filter);
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    @Override
    protected void _close() {
        final RecordCursorFactory base = this.base;
        this.base = null;
        this.cursor = null;
        final Function filter = this.filter;
        this.filter = null;

        Throwable failure = Misc.freeBestEffort(null, base);
        failure = Misc.freeBestEffort(failure, filter);
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Boolean AND of two filter functions. Used when a
     * {@link FilteredRecordCursorFactory} is constructed over another
     * {@code FilteredRecordCursorFactory} so the two row-time predicates
     * collapse into a single short-circuit conjunction. Mirrors the
     * private AndBooleanFunction in
     * {@link io.questdb.griffin.engine.functions.bool.AndFunctionFactory}
     * but stays local here to avoid widening that factory's public API
     * surface for one consumer.
     */
    private static final class ChainedAndFilter extends BooleanFunction implements BinaryFunction {
        private final Function left;
        private final Function right;

        ChainedAndFilter(Function left, Function right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean getBool(Record rec) {
            return left.getBool(rec) && right.getBool(rec);
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public Function getRight() {
            return right;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val('(');
            sink.val(left);
            sink.val(" and ");
            sink.val(right);
            sink.val(')');
        }
    }
}
