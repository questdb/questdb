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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

public class LastSymbolGroupByFunction extends SymbolFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private int valueIndex;
    private SymbolTable symbolTable;

    public LastSymbolGroupByFunction(int position, @NotNull Function arg) {
        super(position);
        this.arg = arg;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
        this.symbolTable = symbolTableSource.getSymbolTable(this.valueIndex);
        assert this.symbolTable != this;
        assert this.symbolTable != null;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        mapValue.putInt(this.valueIndex, this.arg.getInt(record));
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        computeFirst(mapValue, record);
    }

    @Override
    public Function getArg() {
        return this.arg;
    }

    @Override
    public int getInt(Record rec) {
        return rec.getInt(this.valueIndex);
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return symbolTable.valueOf(getInt(rec));
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.SYMBOL);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putInt(this.valueIndex, Numbers.INT_NaN);
    }

    @Override
    public boolean isSymbolTableStatic() {
        return true;
    }

    @Override
    public CharSequence valueOf(int key) {
        return "";
    }
}
