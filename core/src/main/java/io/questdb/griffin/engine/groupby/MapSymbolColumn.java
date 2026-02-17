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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.std.Misc;
import org.jetbrains.annotations.Nullable;

public class MapSymbolColumn extends SymbolFunction {
    private final int cursorColumnIndex;
    private final int mapColumnIndex;
    private final boolean symbolTableStatic;
    private boolean ownSymbolTable;
    private SymbolTable symbolTable;
    private SymbolTableSource symbolTableSource;

    public MapSymbolColumn(int mapColumnIndex, int cursorColumnIndex, boolean symbolTableStatic) {
        this.mapColumnIndex = mapColumnIndex;
        this.cursorColumnIndex = cursorColumnIndex;
        this.symbolTableStatic = symbolTableStatic;
    }

    @Override
    public void close() {
        if (ownSymbolTable) {
            symbolTable = Misc.freeIfCloseable(symbolTable);
        }
    }

    @Override
    public int getInt(Record rec) {
        return rec.getInt(mapColumnIndex);
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
    public CharSequence getSymbol(Record rec) {
        return symbolTable.valueOf(getInt(rec));
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        return symbolTable.valueBOf(getInt(rec));
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
        this.symbolTableSource = symbolTableSource;
        if (executionContext.getCloneSymbolTables()) {
            if (symbolTable != null) {
                assert ownSymbolTable;
                symbolTable = Misc.freeIfCloseable(symbolTable);
            }
            symbolTable = symbolTableSource.newSymbolTable(cursorColumnIndex);
            ownSymbolTable = true;
        } else {
            symbolTable = symbolTableSource.getSymbolTable(cursorColumnIndex);
            ownSymbolTable = false;
        }
        assert this.symbolTable != this;
        assert this.symbolTable != null;
        // static symbol table must be non-null
        assert !symbolTableStatic || getStaticSymbolTable() != null;
    }

    @Override
    public boolean isSymbolTableStatic() {
        return symbolTableStatic;
    }

    @Override
    public @Nullable SymbolTable newSymbolTable() {
        return symbolTableSource.newSymbolTable(cursorColumnIndex);
    }

    @Override
    public boolean supportsParallelism() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.putColumnName(mapColumnIndex);
    }

    @Override
    public CharSequence valueBOf(int symbolKey) {
        return symbolTable.valueBOf(symbolKey);
    }

    @Override
    public CharSequence valueOf(int symbolKey) {
        return symbolTable.valueOf(symbolKey);
    }
}
