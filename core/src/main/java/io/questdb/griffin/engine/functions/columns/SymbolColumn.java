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

package io.questdb.griffin.engine.functions.columns;

import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import org.jetbrains.annotations.Nullable;

public class SymbolColumn extends SymbolFunction implements ScalarFunction {
    private final int columnIndex;
    private final boolean symbolTableStatic;
    private SymbolTable symbolTable;

    public SymbolColumn(int position, int columnIndex, boolean symbolTableStatic) {
        super(position);
        this.columnIndex = columnIndex;
        this.symbolTableStatic = symbolTableStatic;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public int getInt(Record rec) {
        return rec.getInt(columnIndex);
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return rec.getSym(columnIndex);
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        return rec.getSymB(columnIndex);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
        this.symbolTable = symbolTableSource.getSymbolTable(columnIndex);
        assert !symbolTableStatic || symbolTable != null;
    }

    @Override
    public @Nullable StaticSymbolTable getStaticSymbolTable() {
        return symbolTable instanceof StaticSymbolTable ? (StaticSymbolTable) symbolTable : null;
    }

    @Override
    public boolean isSymbolTableStatic() {
        return symbolTableStatic;
    }

    @Override
    public CharSequence valueOf(int symbolKey) {
        return symbolTable.valueOf(symbolKey);
    }

    @Override
    public CharSequence valueBOf(int symbolKey) {
        return symbolTable.valueBOf(symbolKey);
    }
}
