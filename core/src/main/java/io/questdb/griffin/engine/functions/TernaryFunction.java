/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

public interface TernaryFunction extends Function {

    @Override
    default void close() {
        getLeft().close();
        getCenter().close();
        getRight().close();
    }

    @Override
    default void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        getLeft().init(symbolTableSource, executionContext);
        getCenter().init(symbolTableSource, executionContext);
        getRight().init(symbolTableSource, executionContext);
    }

    @Override
    default boolean isConstant() {
        return getLeft().isConstant() && getCenter().isConstant() && getRight().isConstant();
    }

    @Override
    default boolean isRuntimeConstant() {
        boolean arc = getLeft().isRuntimeConstant();
        boolean brc = getCenter().isRuntimeConstant();
        boolean crc = getRight().isRuntimeConstant();

        boolean ac = getLeft().isConstant();
        boolean bc = getCenter().isConstant();
        boolean cc = getRight().isConstant();

        return (ac || arc) && (bc || brc) && (cc || crc) && (arc || brc || crc);
    }

    @Override
    default void toTop() {
        getLeft().toTop();
        getCenter().toTop();
        getRight().toTop();
    }

    Function getLeft();

    Function getCenter();

    Function getRight();
}
