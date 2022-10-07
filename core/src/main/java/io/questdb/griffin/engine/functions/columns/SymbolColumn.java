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

package io.questdb.griffin.engine.functions.columns;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.std.Misc;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.Nullable;

public class SymbolColumn extends SymbolFunction implements ScalarFunction {
    private final int columnIndex;
    private final boolean symbolTableStatic;
    private SymbolTable symbolTable;
    private SymbolTableSource symbolTableSource;
    private boolean ownSymbolTable;

    public SymbolColumn(int columnIndex, boolean symbolTableStatic) {
        this.columnIndex = columnIndex;
        this.symbolTableStatic = symbolTableStatic;
    }

    @Override
    public int getInt(Record rec) {
        return rec.getInt(columnIndex);
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return symbolTable.valueOf(rec.getInt(columnIndex));
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        return symbolTable.valueBOf(rec.getInt(columnIndex));
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
        this.symbolTableSource = symbolTableSource;
        if (executionContext.getCloneSymbolTables()) {
            if (symbolTable != null) {
                assert ownSymbolTable;
                symbolTable = Misc.freeIfCloseable(symbolTable);
            }
            symbolTable = symbolTableSource.newSymbolTable(columnIndex);
            ownSymbolTable = true;
        } else {
            symbolTable = symbolTableSource.getSymbolTable(columnIndex);
        }
        // static symbol table must be non-null
        assert !symbolTableStatic || getStaticSymbolTable() != null;
    }
    
    @Override
    public @Nullable StaticSymbolTable getStaticSymbolTable() {
        if (symbolTable instanceof StaticSymbolTable) {
            return (StaticSymbolTable) symbolTable;
        }
        if (symbolTable instanceof SymbolFunction) {
            return ((SymbolFunction) symbolTable).getStaticSymbolTable();
        }
        return null;
    }

    @Override
    public boolean isReadThreadSafe() {
        return false;
    }

    @Override
    public @Nullable SymbolTable newSymbolTable() {
        return symbolTableSource.newSymbolTable(columnIndex);
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

    @Override
    public void close() {
        if (ownSymbolTable) {
            symbolTable = Misc.freeIfCloseable(symbolTable);
        }
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put("SymbolColumn(").put(columnIndex).put(')');
    }

}
