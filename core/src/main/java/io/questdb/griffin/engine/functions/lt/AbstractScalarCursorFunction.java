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

package io.questdb.griffin.engine.functions.lt;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.ScalarSubQueryUtils;

/**
 * Base of the numeric cursor-comparison functions ({@code col < (select ...)},
 * {@code col > (select ...)} and their negations). Owns the cold lifecycle shared by every typed
 * variant:
 * <ul>
 *     <li>executing the scalar sub-query once per query execution - not per row - in
 *     {@link #init} and caching its value as a typed scalar (via {@link #readValue});</li>
 *     <li>enforcing the single-row cardinality of the sub-query
 *     ({@link ScalarSubQueryUtils#assertNoMoreRows});</li>
 *     <li>donating the cached scalar to per-worker clones ({@link #offerStateTo}) so the sub-query
 *     never re-executes per worker;</li>
 *     <li>rendering the execution plan, including the {@code [thread-safe]} and
 *     {@code [state-shared]} markers.</li>
 * </ul>
 * The per-row {@link #getBool(Record)} stays specialized in the concrete subclasses, so the hot
 * path is unaffected by this sharing.
 */
public abstract class AbstractScalarCursorFunction extends NegatableBooleanFunction implements BinaryFunction {
    // the link read the value init() computed for the execution
    private static final int RUN_BY_LINK_AFTER_OWNER = 2;
    // the link computed the value, before init(); init() reuses it
    private static final int RUN_BY_LINK_BEFORE_OWNER = 3;
    // init() computed the value; the link has not read it yet
    private static final int RUN_BY_OWNER = 1;
    // no run that the other side has to account for
    private static final int RUN_NONE = 0;
    protected final Function leftFunc;
    private final RecordCursorFactory factory;
    private final Function rightFunc;
    private final int rightPos;
    // which of this function and the compiled filter's link to its value ran the sub-query for the
    // execution, see initFromLink()
    private int runState = RUN_NONE;
    private boolean stateInherited = false;
    private boolean stateShared = false;

    protected AbstractScalarCursorFunction(RecordCursorFactory factory, Function leftFunc, Function rightFunc, int rightPos) {
        this.factory = factory;
        this.leftFunc = leftFunc;
        this.rightFunc = rightFunc;
        this.rightPos = rightPos;
    }

    @Override
    public Function getLeft() {
        return leftFunc;
    }

    @Override
    public Function getRight() {
        return rightFunc;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        BinaryFunction.super.init(symbolTableSource, executionContext);
        if (stateInherited) {
            return;
        }
        this.stateShared = false;
        if (runState == RUN_BY_LINK_BEFORE_OWNER) {
            // the compiled filter's link ran the sub-query for this execution: both filters compare
            // that one value
            runState = RUN_NONE;
            return;
        }
        runSubQuery(executionContext);
        runState = RUN_BY_OWNER;
    }

    /**
     * @return the cached sub-query value, when {@link #getScalarType()} is DOUBLE
     */
    public double getScalarDouble() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return the cached sub-query value, when {@link #getScalarType()} is INT
     */
    public int getScalarInt() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return the cached sub-query value, when {@link #getScalarType()} is LONG
     */
    public long getScalarLong() {
        throw new UnsupportedOperationException();
    }

    /**
     * The type the comparison reads the sub-query value at, its NULL included: DOUBLE (NaN), LONG
     * ({@code LONG_NULL}) or INT ({@code INT_NULL}).
     */
    public abstract int getScalarType();

    /**
     * Called by the compiled filter's link to this function's value ({@code
     * CompiledFilterScalarSubQuery}) as it initialises for an execution, before it reads the value.
     * The consumers of a compiled filter initialise this function (the Java filter) and the link once
     * per execution each, in an order of their own; whichever of the two comes first runs the
     * sub-query, and the other reads that value, so the sub-query runs once per execution in either
     * order and the link never binds the value of an earlier execution:
     * <ul>
     *     <li>this function first: {@link #init} runs it, the link reads it;</li>
     *     <li>the link first: the link runs it, {@link #init} reuses it.</li>
     * </ul>
     * A link initialised again within an execution after it read this function's value runs the
     * sub-query afresh, and leaves the next execution's {@link #init} to run it too.
     */
    public void initFromLink(SqlExecutionContext executionContext) throws SqlException {
        switch (runState) {
            case RUN_BY_OWNER:
                runState = RUN_BY_LINK_AFTER_OWNER;
                break;
            case RUN_BY_LINK_AFTER_OWNER:
                runSubQuery(executionContext);
                break;
            default:
                runSubQuery(executionContext);
                runState = RUN_BY_LINK_BEFORE_OWNER;
                break;
        }
    }

    @Override
    public boolean isThreadSafe() {
        return leftFunc.isThreadSafe();
    }

    @Override
    public void offerStateTo(Function that) {
        // state moves only between clones compiled from the same expression, which are always
        // instances of the same concrete class
        if (getClass() == that.getClass()) {
            final AbstractScalarCursorFunction thatF = (AbstractScalarCursorFunction) that;
            donateValueTo(thatF);
            thatF.stateInherited = this.stateShared = true;
        }
        BinaryFunction.super.offerStateTo(that);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(leftFunc);
        if (leftFunc.isThreadSafe()) {
            sink.val(" [thread-safe]");
        }
        if (negated) {
            sink.val(negatedOperator());
        } else {
            sink.val(operator());
        }
        sink.val(rightFunc);
        if (stateShared) {
            sink.val(" [state-shared]");
        }
    }

    /**
     * Copies the cached typed scalar into {@code that}, which is guaranteed to be an instance of
     * the same concrete class.
     */
    protected abstract void donateValueTo(AbstractScalarCursorFunction that);

    /**
     * Plan token of the negated operator, for example {@code " >= "} for a {@code <} function.
     */
    protected abstract String negatedOperator();

    /**
     * Plan token of the operator, for example {@code " < "}.
     */
    protected abstract String operator();

    /**
     * Reads the sub-query scalar from the first cursor row into the typed cache field.
     */
    protected abstract void readValue(Record record);

    /**
     * Caches the typed null value (empty sub-query); the predicate then follows QuestDB's
     * sentinel-null convention: strict comparisons match no rows, while the negated inclusive
     * forms ({@code >=}, {@code <=}) match rows whose left operand is also null (null equals
     * null).
     */
    protected abstract void setNullValue();

    private void runSubQuery(SqlExecutionContext executionContext) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(executionContext)) {
            if (cursor.hasNext()) {
                readValue(cursor.getRecord());
                ScalarSubQueryUtils.assertNoMoreRows(cursor, rightPos);
            } else {
                setNullValue();
            }
        }
    }
}
