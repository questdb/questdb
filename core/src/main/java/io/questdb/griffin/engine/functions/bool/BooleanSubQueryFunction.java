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

package io.questdb.griffin.engine.functions.bool;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.ScalarSubQueryUtils;
import io.questdb.griffin.engine.functions.UnaryFunction;
import org.jetbrains.annotations.Nullable;

/**
 * Adapts a scalar boolean sub-query used directly as a predicate, for example
 * {@code where (select b from x limit 1)} or {@code where a and not (select b from x limit 1)}.
 * Follows the scalar-cursor lifecycle established by
 * {@link io.questdb.griffin.engine.functions.lt.AbstractScalarCursorFunction}:
 * <ul>
 *     <li>the sub-query executes once per query execution - not per row - in {@link #init},
 *     and its single value is cached;</li>
 *     <li>single-row cardinality is enforced
 *     ({@link ScalarSubQueryUtils#assertNoMoreRows});</li>
 *     <li>an empty sub-query yields NULL, which QuestDB's two-valued BOOLEAN represents as
 *     {@code false}, so the predicate matches no rows;</li>
 *     <li>the cached value is donated to per-worker clones ({@link #offerStateTo}) so the
 *     sub-query never re-executes per worker.</li>
 * </ul>
 */
public class BooleanSubQueryFunction extends BooleanFunction implements UnaryFunction {
    private final Function cursorFunc;
    private final RecordCursorFactory factory;
    private final int position;
    private boolean stateInherited = false;
    private boolean stateShared = false;
    private boolean value = false;

    public BooleanSubQueryFunction(RecordCursorFactory factory, Function cursorFunc, int position) {
        this.factory = factory;
        this.cursorFunc = cursorFunc;
        this.position = position;
    }

    /**
     * Wraps a CURSOR-typed function in a {@link BooleanSubQueryFunction} when the underlying
     * sub-query yields exactly one BOOLEAN column, making it usable in boolean expression
     * context. Returns {@code null} when the function is not coercible; the caller then reports
     * its usual type error.
     *
     * @param function a candidate function, typically CURSOR-typed
     * @param position the parse position of the sub-query, used for error markers
     * @return the boolean adapter, or {@code null} if the function is not a scalar boolean cursor
     */
    public static @Nullable Function maybeWrap(Function function, int position) {
        if (!ColumnType.isCursor(function.getType())) {
            return null;
        }
        final RecordCursorFactory factory = function.getRecordCursorFactory();
        if (factory == null) {
            return null;
        }
        final RecordMetadata metadata = factory.getMetadata();
        if (metadata.getColumnCount() == 1 && ColumnType.tagOf(metadata.getColumnType(0)) == ColumnType.BOOLEAN) {
            return new BooleanSubQueryFunction(factory, function, position);
        }
        return null;
    }

    @Override
    public Function getArg() {
        return cursorFunc;
    }

    @Override
    public boolean getBool(Record rec) {
        return value;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        UnaryFunction.super.init(symbolTableSource, executionContext);
        if (stateInherited) {
            return;
        }
        this.stateShared = false;
        try (RecordCursor cursor = factory.getCursor(executionContext)) {
            if (cursor.hasNext()) {
                value = cursor.getRecord().getBool(0);
                ScalarSubQueryUtils.assertNoMoreRows(cursor, position);
            } else {
                // empty scalar sub-query yields NULL; two-valued BOOLEAN renders it as false
                value = false;
            }
        }
    }

    @Override
    public boolean isRuntimeConstant() {
        // The sub-query executes once per query execution in init() and its single value is
        // cached, so the predicate is constant across every row of one execution - yet its
        // value is unknown at compile time (it may differ between executions), so it is a
        // runtime constant, not a compile-time constant.
        return true;
    }

    @Override
    public void offerStateTo(Function that) {
        // state moves only between clones compiled from the same expression
        if (that instanceof BooleanSubQueryFunction thatF) {
            thatF.value = value;
            thatF.stateInherited = this.stateShared = true;
        }
        UnaryFunction.super.offerStateTo(that);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(cursorFunc);
        if (stateShared) {
            sink.val(" [state-shared]");
        }
    }
}
