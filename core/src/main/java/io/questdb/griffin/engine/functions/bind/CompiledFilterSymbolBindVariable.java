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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.std.Misc;

/**
 * String bind variable function wrapper used in SQL JIT. Also used to handle deferred
 * (unknown at compile time) symbol literals.
 */
public class CompiledFilterSymbolBindVariable extends SymbolFunction implements ScalarFunction {

    private final Function symbolFunction;
    private final int columnIndex;
    private StaticSymbolTable symbolTable;
    private final boolean cloneSymbolTable;

    public CompiledFilterSymbolBindVariable(Function symbolFunction, int columnIndex, boolean cloneSymbolTable) {
        assert symbolFunction.getType() == ColumnType.STRING || symbolFunction.getType() == ColumnType.SYMBOL;
        this.symbolFunction = symbolFunction;
        this.columnIndex = columnIndex;
        this.cloneSymbolTable = cloneSymbolTable;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        if (symbolTable == null) {
            if (cloneSymbolTable) {
                this.symbolTable = (StaticSymbolTable) symbolTableSource.newSymbolTable(columnIndex);
            } else {
                this.symbolTable = (StaticSymbolTable) symbolTableSource.getSymbolTable(columnIndex);
            }
            this.symbolFunction.init(symbolTableSource, executionContext);
        }
    }

    @Override
    public int getInt(Record rec) {
        final CharSequence symbolStr = symbolFunction.getStr(null);
        return symbolTable.keyOf(symbolStr);
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return symbolFunction.getStr(null);
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        return symbolFunction.getStrB(null);
    }

    @Override
    public boolean isSymbolTableStatic() {
        return true;
    }

    @Override
    public CharSequence valueOf(int symbolKey) {
        return symbolTable.valueOf(symbolKey);
    }

    @Override
    public CharSequence valueBOf(int symbolKey) {
        return symbolTable.valueBOf(symbolKey);
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public void close() {
        if (cloneSymbolTable) {
            symbolTable = Misc.free(symbolTable);
        }
    }
}
