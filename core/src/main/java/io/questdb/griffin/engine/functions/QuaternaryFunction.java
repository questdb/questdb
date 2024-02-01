/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
    default void initCursor() {
        getFunc0().initCursor();
        getFunc1().initCursor();
        getFunc2().initCursor();
        getFunc3().initCursor();
    }

    @Override
    default boolean isConstant() {
        return getFunc0().isConstant()
                && getFunc1().isConstant()
                && getFunc2().isConstant()
                && getFunc3().isConstant();
    }

    @Override
    default boolean isParallelismSupported() {
        return getFunc0().isParallelismSupported()
                && getFunc1().isParallelismSupported()
                && getFunc2().isParallelismSupported()
                && getFunc3().isParallelismSupported();
    }

    @Override
    default boolean isReadThreadSafe() {
        return getFunc0().isReadThreadSafe()
                && getFunc1().isReadThreadSafe()
                && getFunc2().isReadThreadSafe()
                && getFunc3().isReadThreadSafe();
    }

    @Override
    default boolean isRuntimeConstant() {
        boolean arc = getFunc0().isRuntimeConstant();
        boolean brc = getFunc1().isRuntimeConstant();
        boolean crc = getFunc2().isRuntimeConstant();
        boolean drc = getFunc3().isRuntimeConstant();

        boolean ac = getFunc0().isConstant();
        boolean bc = getFunc1().isConstant();
        boolean cc = getFunc2().isConstant();
        boolean dc = getFunc3().isConstant();

        return (ac || arc) && (bc || brc) && (cc || crc) && (dc || drc) && (arc || brc || crc || drc);
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
