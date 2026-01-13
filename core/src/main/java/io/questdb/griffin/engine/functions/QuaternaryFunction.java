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

public interface QuaternaryFunction extends Function {

    @Override
    default void close() {
        getFunc0().close();
        getFunc1().close();
        getFunc2().close();
        getFunc3().close();
    }

    @Override
    default void cursorClosed() {
        getFunc0().cursorClosed();
        getFunc1().cursorClosed();
        getFunc2().cursorClosed();
        getFunc3().cursorClosed();
    }

    Function getFunc0();

    Function getFunc1();

    Function getFunc2();

    Function getFunc3();

    @Override
    default void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        getFunc0().init(symbolTableSource, executionContext);
        getFunc1().init(symbolTableSource, executionContext);
        getFunc2().init(symbolTableSource, executionContext);
        getFunc3().init(symbolTableSource, executionContext);
    }

    @Override
    default boolean isConstant() {
        return getFunc0().isConstant()
                && getFunc1().isConstant()
                && getFunc2().isConstant()
                && getFunc3().isConstant();
    }

    @Override
    default boolean isNonDeterministic() {
        return getFunc0().isNonDeterministic()
                || getFunc1().isNonDeterministic()
                || getFunc2().isNonDeterministic()
                || getFunc3().isNonDeterministic();
    }

    @Override
    default boolean isRandom() {
        return getFunc0().isRandom() || getFunc1().isRandom() || getFunc2().isRandom() || getFunc3().isRandom();
    }

    @Override
    default boolean isRuntimeConstant() {
        final boolean arc = getFunc0().isRuntimeConstant();
        final boolean brc = getFunc1().isRuntimeConstant();
        final boolean crc = getFunc2().isRuntimeConstant();
        final boolean drc = getFunc3().isRuntimeConstant();

        final boolean ac = getFunc0().isConstant();
        final boolean bc = getFunc1().isConstant();
        final boolean cc = getFunc2().isConstant();
        final boolean dc = getFunc3().isConstant();

        return (ac || arc) && (bc || brc) && (cc || crc) && (dc || drc) && (arc || brc || crc || drc);
    }

    @Override
    default boolean isThreadSafe() {
        return getFunc0().isThreadSafe()
                && getFunc1().isThreadSafe()
                && getFunc2().isThreadSafe()
                && getFunc3().isThreadSafe();
    }

    @Override
    default void offerStateTo(Function that) {
        if (that instanceof QuaternaryFunction other) {
            getFunc0().offerStateTo(other.getFunc0());
            getFunc1().offerStateTo(other.getFunc1());
            getFunc2().offerStateTo(other.getFunc2());
            getFunc3().offerStateTo(other.getFunc3());
        }
    }

    @Override
    default boolean shouldMemoize() {
        return getFunc0().shouldMemoize()
                || getFunc1().shouldMemoize()
                || getFunc2().shouldMemoize()
                || getFunc3().shouldMemoize();
    }

    @Override
    default boolean supportsParallelism() {
        return getFunc0().supportsParallelism()
                && getFunc1().supportsParallelism()
                && getFunc2().supportsParallelism()
                && getFunc3().supportsParallelism();
    }

    @Override
    default boolean supportsRandomAccess() {
        return getFunc0().supportsRandomAccess()
                && getFunc1().supportsRandomAccess()
                && getFunc2().supportsRandomAccess()
                && getFunc3().supportsRandomAccess();
    }

    @Override
    default void toPlan(PlanSink sink) {
        sink.val(getName()).val('(')
                .val(getFunc0()).val(',')
                .val(getFunc1()).val(',')
                .val(getFunc2()).val(',')
                .val(getFunc3())
                .val(')');
    }

    @Override
    default void toTop() {
        getFunc0().toTop();
        getFunc1().toTop();
        getFunc2().toTop();
        getFunc3().toTop();
    }
}
