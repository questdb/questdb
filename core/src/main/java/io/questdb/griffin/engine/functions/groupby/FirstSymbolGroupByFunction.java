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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class FirstSymbolGroupByFunction extends SymbolFunction implements GroupByFunction, UnaryFunction {
    protected final SymbolFunction arg;
    protected int valueIndex;

    public FirstSymbolGroupByFunction(@NotNull SymbolFunction arg) {
        this.arg = arg;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        mapValue.putLong(valueIndex, rowId);
        mapValue.putInt(valueIndex + 1, arg.getInt(record));
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        // empty
    }

    @Override
    public Function getArg() {
        return this.arg;
    }

    @Override
    public int getInt(Record rec) {
        return rec.getInt(valueIndex + 1);
    }

    @Override
    public String getName() {
        return "first";
    }

    @Override
    public @Nullable StaticSymbolTable getStaticSymbolTable() {
        return arg.getStaticSymbolTable();
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
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG); // row id
        columnTypes.add(ColumnType.INT);  // value
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isSymbolTableStatic() {
        return arg.isSymbolTableStatic();
    }

    @Override
    public boolean isThreadSafe() {
        return UnaryFunction.super.isThreadSafe();
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        long srcRowId = srcValue.getLong(valueIndex);
        long destRowId = destValue.getLong(valueIndex);
        if (srcRowId != Numbers.LONG_NULL && (srcRowId < destRowId || destRowId == Numbers.LONG_NULL)) {
            destValue.putLong(valueIndex, srcRowId);
            destValue.putInt(valueIndex + 1, srcValue.getInt(valueIndex + 1));
        }
    }

    @Override
    public @Nullable SymbolTable newSymbolTable() {
        // this implementation does not have its own symbol table
        // it fully relies on the argument
        return arg.newSymbolTable();
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, Numbers.LONG_NULL);
        mapValue.putInt(valueIndex + 1, SymbolTable.VALUE_IS_NULL);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }

    @Override
    public CharSequence valueBOf(int key) {
        return arg.valueBOf(key);
    }

    @Override
    public CharSequence valueOf(int key) {
        return arg.valueOf(key);
    }
}
