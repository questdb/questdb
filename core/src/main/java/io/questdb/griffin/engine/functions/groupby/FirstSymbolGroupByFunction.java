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
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import org.jetbrains.annotations.NotNull;

public class FirstSymbolGroupByFunction extends SymbolFunction implements GroupByFunction, UnaryFunction {
    private final SymbolFunction arg;
    private int valueIndex;

    public FirstSymbolGroupByFunction(int position, @NotNull SymbolFunction arg) {
        super(position);
        this.arg = arg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        mapValue.putInt(this.valueIndex, this.arg.getInt(record));
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.INT);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putInt(this.valueIndex, SymbolTable.VALUE_IS_NULL);
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
        return arg.valueOf(getInt(rec));
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        return arg.valueBOf(getInt(rec));
    }

    @Override
    public boolean isSymbolTableStatic() {
        return arg.isSymbolTableStatic();
    }

    @Override
    public CharSequence valueOf(int key) {
        return arg.valueOf(key);
    }

    @Override
    public CharSequence valueBOf(int key) {
        return arg.valueBOf(key);
    }
}