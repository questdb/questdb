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
import io.questdb.std.Misc;

/**
 * Interface for functions that take two arguments.
 */
public interface BinaryFunction extends Function {

    @Override
    default void close() {
        Misc.free(getLeft());
        Misc.free(getRight());
    }

    @Override
    default void cursorClosed() {
        getLeft().cursorClosed();
        getRight().cursorClosed();
    }

    /**
     * Returns the left (first) argument of this binary function.
     *
     * @return the left function argument
     */
    Function getLeft();

    /**
     * Returns the right (second) argument of this binary function.
     *
     * @return the right function argument
     */
    Function getRight();

    @Override
    default void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        getLeft().init(symbolTableSource, executionContext);
        getRight().init(symbolTableSource, executionContext);
    }

    @Override
    default boolean isConstant() {
        return getLeft().isConstant() && getRight().isConstant();
    }

    @Override
    default boolean isEquivalentTo(Function other) {
        if (other == this) {
            return true;
        }
        if (other instanceof BinaryFunction that) {
            return getLeft().isEquivalentTo(that.getLeft()) && getRight().isEquivalentTo(that.getRight());
        }
        return false;
    }

    @Override
    default boolean isNonDeterministic() {
        return getLeft().isNonDeterministic() || getRight().isNonDeterministic();
    }

    // used in generic toSink implementation
    default boolean isOperator() {
        return false;
    }

    @Override
    default boolean isRandom() {
        return getLeft().isRandom() || getRight().isRandom();
    }

    @Override
    default boolean isRuntimeConstant() {
        final Function l = getLeft();
        final Function r = getRight();
        return (l.isConstant() && r.isRuntimeConstant()) || (r.isConstant() && l.isRuntimeConstant()) || (l.isRuntimeConstant() && r.isRuntimeConstant());
    }

    @Override
    default boolean isThreadSafe() {
        return getLeft().isThreadSafe() && getRight().isThreadSafe();
    }

    @Override
    default void offerStateTo(Function that) {
        if (that instanceof BinaryFunction other) {
            getLeft().offerStateTo(other.getLeft());
            getRight().offerStateTo(other.getRight());
        }
    }

    @Override
    default boolean shouldMemoize() {
        return getLeft().shouldMemoize() || getRight().shouldMemoize();
    }

    @Override
    default boolean supportsParallelism() {
        return getLeft().supportsParallelism() && getRight().supportsParallelism();
    }

    @Override
    default boolean supportsRandomAccess() {
        return getLeft().supportsRandomAccess() && getRight().supportsRandomAccess();
    }

    @Override
    default void toPlan(PlanSink sink) {
        if (isOperator()) {
            sink.val(getLeft()).val(getName()).val(getRight());
        } else {
            sink.val(getName()).val('(').val(getLeft()).val(',').val(getRight()).val(')');
        }
    }

    @Override
    default void toTop() {
        getLeft().toTop();
        getRight().toTop();
    }
}
