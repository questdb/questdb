/*+*****************************************************************************
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

package io.questdb.griffin.engine.functions.columns;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;

final class BindableSymbolColumn extends SymbolFunction implements BindableColumn {
    private final boolean isSymbolTableStatic;
    private SymbolColumn column;
    private int columnId;
    private boolean isOpen = true;

    BindableSymbolColumn(int columnId, boolean isSymbolTableStatic) {
        this.columnId = columnId;
        this.isSymbolTableStatic = isSymbolTableStatic;
    }

    @Override
    public void close() {
        isOpen = false;
        if (column != null) {
            column.close();
        }
    }

    @Override
    public int getColumnId() {
        return columnId;
    }

    @Override
    public int getInt(Record rec) {
        return column.getInt(rec);
    }

    @Override
    public StaticSymbolTable getStaticSymbolTable() {
        return column == null ? null : column.getStaticSymbolTable();
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return column.getSymbol(rec);
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        return column.getSymbolB(rec);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
        column.init(symbolTableSource, executionContext);
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public boolean isSymbolTableStatic() {
        return isSymbolTableStatic;
    }

    @Override
    public SymbolTable newSymbolTable() {
        return column.newSymbolTable();
    }

    @Override
    public void setColumnId(int columnId) {
        assert isOpen && column == null;
        this.columnId = columnId;
    }

    @Override
    public void setColumnIndex(int columnIndex) {
        assert column == null && columnIndex >= 0;
        column = new SymbolColumn(columnIndex, isSymbolTableStatic);
    }

    @Override
    public boolean supportsKeyValueAccess() {
        return column != null && column.supportsKeyValueAccess();
    }

    @Override
    public boolean supportsParallelism() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        column.toPlan(sink);
    }

    @Override
    public CharSequence valueBOf(int symbolKey) {
        return column.valueBOf(symbolKey);
    }

    @Override
    public CharSequence valueOf(int symbolKey) {
        return column.valueOf(symbolKey);
    }
}
