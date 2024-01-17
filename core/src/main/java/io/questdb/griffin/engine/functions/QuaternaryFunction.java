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
        getZero().close();
        getOne().close();
        getTwo().close();
        getThree().close();
    }

    Function getZero();
    Function getOne();
    Function getTwo();
    Function getThree();

    @Override
    default void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        getZero().init(symbolTableSource, executionContext);
        getOne().init(symbolTableSource, executionContext);
        getTwo().init(symbolTableSource, executionContext);
        getThree().init(symbolTableSource, executionContext);
    }

    @Override
    default void initCursor() {
        getZero().initCursor();
        getOne().initCursor();
        getTwo().initCursor();
        getThree().initCursor();
    }

    @Override
    default boolean isConstant() {
        return
            getZero().isConstant() &&
            getOne().isConstant() &&
            getTwo().isConstant() &&
            getThree().isConstant();
    }

    @Override
    default boolean isParallelismSupported() {
        return getZero().isParallelismSupported() && getOne().isParallelismSupported() && getTwo().isParallelismSupported() && getThree().isParallelismSupported();
    }

    @Override
    default boolean isReadThreadSafe() {
        return getZero().isReadThreadSafe() && getOne().isReadThreadSafe() && getTwo().isReadThreadSafe() && getThree().isReadThreadSafe();
    }

    @Override
    default boolean isRuntimeConstant() {
        boolean arc = getZero().isRuntimeConstant();
        boolean brc = getOne().isRuntimeConstant();
        boolean crc = getTwo().isRuntimeConstant();
        boolean drc = getThree().isRuntimeConstant();

        boolean ac = getZero().isConstant();
        boolean bc = getOne().isConstant();
        boolean cc = getTwo().isConstant();
        boolean dc = getThree().isConstant();

        return (ac || arc) && (bc || brc) && (cc || crc) && (dc || drc) &&  (arc || brc || crc || drc);
    }

    @Override
    default void toPlan(PlanSink sink) {
        sink.val(getName()).val('(').val(getZero()).val(',').val(getOne()).val(',').val(getTwo()).val(',').val(getThree()).val(')');
    }

    @Override
    default void toTop() {
        getZero().toTop();
        getOne().toTop();
        getTwo().toTop();
        getThree().toTop();
    }
}
