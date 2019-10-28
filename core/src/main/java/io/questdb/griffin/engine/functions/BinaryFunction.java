/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.functions;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlExecutionContext;

public interface BinaryFunction extends Function {

    @Override
    default void close() {
        getLeft().close();
        getRight().close();
    }

    @Override
    default void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
        getLeft().init(symbolTableSource, executionContext);
        getRight().init(symbolTableSource, executionContext);
    }

    @Override
    default boolean isConstant() {
        return getLeft().isConstant() && getRight().isConstant();
    }

    @Override
    default void toTop() {
        getLeft().toTop();
        getRight().toTop();
    }

    Function getLeft();

    Function getRight();
}
