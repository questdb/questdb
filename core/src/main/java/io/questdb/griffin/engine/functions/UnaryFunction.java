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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

public interface UnaryFunction extends Function {

    @Override
    default void close() {
        getArg().close();
    }

    @Override
    default void cursorClosed() {
        getArg().cursorClosed();
    }

    /**
     * Returns the single argument of this unary function.
     *
     * @return the function argument
     */
    Function getArg();

    @Override
    default void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        getArg().init(symbolTableSource, executionContext);
    }

    @Override
    default boolean isConstant() {
        return getArg().isConstant();
    }

    @Override
    default boolean isEquivalentTo(Function other) {
        if (other == this) {
            return true;
        }
        if (other instanceof UnaryFunction that) {
            return getArg().isEquivalentTo(that.getArg());
        }
        return false;
    }

    @Override
    default boolean isNonDeterministic() {
        return getArg().isNonDeterministic();
    }

    @Override
    default boolean isRandom() {
        return getArg().isRandom();
    }

    @Override
    default boolean isRuntimeConstant() {
        return getArg().isRuntimeConstant();
    }

    @Override
    default boolean isThreadSafe() {
        return getArg().isThreadSafe();
    }

    @Override
    default void offerStateTo(Function that) {
        if (that instanceof UnaryFunction other) {
            getArg().offerStateTo(other.getArg());
        }
    }

    @Override
    default boolean shouldMemoize() {
        return getArg().shouldMemoize();
    }

    @Override
    default boolean supportsParallelism() {
        return getArg().supportsParallelism();
    }

    @Override
    default boolean supportsRandomAccess() {
        return getArg().supportsRandomAccess();
    }

    @Override
    default void toPlan(PlanSink sink) {
        if (isOperator()) {
            sink.val(getName()).val(getArg());
        } else {
            sink.val(getName()).val('(').val(getArg()).val(')');
        }
    }

    @Override
    default void toTop() {
        getArg().toTop();
    }
}
