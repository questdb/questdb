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

public interface BinaryFunction extends Function {

    @Override
    default void close() {
        getLeft().close();
        getRight().close();
    }

    Function getLeft();

    default String getName() {
        return getClass().getName();
    }

    Function getRight();

    @Override
    default void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        getLeft().init(symbolTableSource, executionContext);
        getRight().init(symbolTableSource, executionContext);
    }

    @Override
    default void initCursor() {
        getLeft().initCursor();
        getRight().initCursor();
    }

    @Override
    default boolean isConstant() {
        return getLeft().isConstant() && getRight().isConstant();
    }

    //used in generic toSink implementation
    default boolean isOperator() {
        return false;
    }

    @Override
    default boolean isReadThreadSafe() {
        return getLeft().isReadThreadSafe() && getRight().isReadThreadSafe();
    }

    default boolean isRuntimeConstant() {
        final Function l = getLeft();
        final Function r = getRight();
        return (l.isConstant() && r.isRuntimeConstant()) || (r.isConstant() && l.isRuntimeConstant()) || (l.isRuntimeConstant() && r.isRuntimeConstant());
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
