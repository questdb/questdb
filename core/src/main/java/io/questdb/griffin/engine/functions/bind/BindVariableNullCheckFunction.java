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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.std.Misc;

/**
 * {@code $n IS [NOT] NULL} (or {@code $n = NULL}) where {@code $n} has no type yet. Like
 * PostgreSQL, the null test does not type the bind variable: {@code FunctionParser} creates
 * this placeholder, finishes parsing the expression so other uses of {@code $n} can type it,
 * then gives it the ordinary {@code =}/{@code !=} function for the resolved type (STRING
 * when nothing else typed it). Every call forwards to that function.
 */
public class BindVariableNullCheckFunction extends BooleanFunction {
    private final int bindVariablePosition;
    private final boolean isBindVariableLeft;
    private final int nullPosition;
    private final int variableIndex;
    private IndexedParameterLinkFunction bindVariable;
    private Function delegate;
    private boolean isClosed;

    public BindVariableNullCheckFunction(
            IndexedParameterLinkFunction bindVariable,
            int bindVariablePosition,
            int nullPosition,
            boolean isBindVariableLeft
    ) {
        this.bindVariable = bindVariable;
        this.bindVariablePosition = bindVariablePosition;
        this.nullPosition = nullPosition;
        this.isBindVariableLeft = isBindVariableLeft;
        this.variableIndex = bindVariable.getVariableIndex();
    }

    @Override
    public void close() {
        isClosed = true;
        if (delegate != null) {
            delegate = Misc.free(delegate);
        } else {
            bindVariable = Misc.free(bindVariable);
        }
    }

    @Override
    public void cursorClosed() {
        delegate.cursorClosed();
    }

    /**
     * Hands the bind variable to the caller, which passes it to the resolved function.
     */
    public IndexedParameterLinkFunction detachBindVariable() {
        final IndexedParameterLinkFunction result = bindVariable;
        bindVariable = null;
        return result;
    }

    public int getBindVariablePosition() {
        return bindVariablePosition;
    }

    @Override
    public boolean getBool(Record rec) {
        return delegate.getBool(rec);
    }

    @Override
    public int getComplexity() {
        return delegate != null ? delegate.getComplexity() : super.getComplexity();
    }

    public int getNullPosition() {
        return nullPosition;
    }

    public int getVariableIndex() {
        return variableIndex;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        delegate.init(symbolTableSource, executionContext);
    }

    public boolean isBindVariableLeft() {
        return isBindVariableLeft;
    }

    /**
     * True when the test was closed before {@code FunctionParser} resolved it, e.g. because
     * {@code false AND $1 IS NULL} folded it away. A closed test no longer owns its bind variable.
     */
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public boolean isNonDeterministic() {
        return delegate == null || delegate.isNonDeterministic();
    }

    @Override
    public boolean isRuntimeConstant() {
        // a bind variable is runtime constant, and so is its null test
        return delegate == null || delegate.isRuntimeConstant();
    }

    @Override
    public boolean isStableWithinExecution() {
        return delegate == null || delegate.isStableWithinExecution();
    }

    @Override
    public boolean isThreadSafe() {
        return delegate == null || delegate.isThreadSafe();
    }

    public void of(Function delegate) {
        this.delegate = delegate;
    }

    @Override
    public void offerStateTo(Function that) {
        if (that instanceof BindVariableNullCheckFunction other && other.delegate != null) {
            delegate.offerStateTo(other.delegate);
        }
    }

    @Override
    public boolean shouldMemoize() {
        return delegate != null && delegate.shouldMemoize();
    }

    @Override
    public boolean supportsParallelism() {
        return delegate == null || delegate.supportsParallelism();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(delegate);
    }

    @Override
    public void toTop() {
        delegate.toTop();
    }
}
