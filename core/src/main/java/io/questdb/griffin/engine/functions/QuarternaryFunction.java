/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

public interface QuarternaryFunction extends Function {

    @Override
    default void close() {
        getLeftEnd().close();
        getCenterLeft().close();
        getCenterRight().close();
        getRightEnd().close();
    }

    @Override
    default void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        getLeftEnd().init(symbolTableSource, executionContext);
        getCenterLeft().init(symbolTableSource, executionContext);
        getCenterRight().init(symbolTableSource, executionContext);
        getRightEnd().init(symbolTableSource, executionContext);
    }

    @Override
    default boolean isConstant() {
        return getLeftEnd().isConstant() && getCenterLeft().isConstant() && getCenterRight().isConstant() && getRightEnd().isConstant();
    }

    @Override
    default boolean isRuntimeConstant() {
        boolean arc = getLeftEnd().isRuntimeConstant();
        boolean brc = getCenterLeft().isRuntimeConstant();
        boolean crc = getCenterRight().isRuntimeConstant();
        boolean drc = getRightEnd().isRuntimeConstant();

        boolean ac = getLeftEnd().isConstant();
        boolean bc = getCenterLeft().isConstant();
        boolean cc = getCenterRight().isConstant();
        boolean dc = getRightEnd().isConstant();

        return (ac || arc) && (bc || brc) && (cc || crc) && (dc || drc) && (arc || brc || crc || drc);
    }

    @Override
    default boolean isReadThreadSafe() {
        return getLeftEnd().isReadThreadSafe() && getCenterLeft().isReadThreadSafe() && getCenterRight().isReadThreadSafe() && getRightEnd().isReadThreadSafe();
    }

    @Override
    default void toTop() {
        getLeftEnd().toTop();
        getCenterLeft().toTop();
        getCenterRight().toTop();
        getRightEnd().toTop();
    }

    Function getLeftEnd();

    Function getCenterLeft();

    Function getCenterRight();

    Function getRightEnd();
}
