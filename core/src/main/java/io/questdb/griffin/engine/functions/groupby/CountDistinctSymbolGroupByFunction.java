/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.std.BitSet;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

import static io.questdb.cairo.sql.SymbolTable.VALUE_IS_NULL;

public class CountDistinctSymbolGroupByFunction extends LongFunction implements UnaryFunction, GroupByFunction {
    private final Function arg;
    private final int setInitialCapacity;
    private final ObjList<BitSet> sets = new ObjList<>();
    private int knownSymbolCount = -1;
    private int setIndex;
    private int valueIndex;

    public CountDistinctSymbolGroupByFunction(Function arg, int setInitialCapacity) {
        this.arg = arg;
        this.setInitialCapacity = setInitialCapacity * BitSet.BITS_PER_WORD;
    }

    @Override
    public void clear() {
        sets.clear();
        setIndex = 0;
        knownSymbolCount = -1;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final BitSet set;
        if (sets.size() <= setIndex) {
            sets.extendAndSet(setIndex, set = new BitSet(setInitialCapacity));
        } else {
            set = sets.getQuick(setIndex);
            set.clear();
        }

        final int val = arg.getInt(record);
        if (val != VALUE_IS_NULL) {
            set.set(val);
            mapValue.putLong(valueIndex, 1L);
        } else {
            mapValue.putLong(valueIndex, 0L);
        }
        mapValue.putInt(valueIndex + 1, setIndex++);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final BitSet set = sets.getQuick(mapValue.getInt(valueIndex + 1));
        final int val = arg.getInt(record);
        if (val != VALUE_IS_NULL) {
            if (set.get(val)) {
                return;
            }
            set.set(val);
            mapValue.addLong(valueIndex, 1);
        }
    }

    @Override
    public boolean earlyExit(MapValue mapValue) {
        // Fast path for the case when we've reached total number of symbols.
        return knownSymbolCount != -1 && mapValue.getLong(valueIndex) == knownSymbolCount;
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public long getLong(Record rec) {
        return rec.getLong(valueIndex);
    }

    @Override
    public String getName() {
        return "count_distinct";
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        arg.init(symbolTableSource, executionContext);
        knownSymbolCount = -1;
        if (arg instanceof SymbolColumn) {
            final SymbolColumn argCol = (SymbolColumn) arg;
            final StaticSymbolTable symbolTable = argCol.getStaticSymbolTable();
            if (symbolTable != null) {
                knownSymbolCount = symbolTable.getSymbolCount();
            }
        }
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG);
        columnTypes.add(ColumnType.INT);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isEarlyExitSupported() {
        return true;
    }

    @Override
    public boolean isReadThreadSafe() {
        return false;
    }

    @Override
    public void setEmpty(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0L);
    }

    @Override
    public void setLong(MapValue mapValue, long value) {
        mapValue.putLong(valueIndex, value);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, Numbers.LONG_NaN);
    }

    @Override
    public boolean supportsParallelism() {
        return false;
    }

    @Override
    public void toTop() {
        UnaryFunction.super.toTop();
        setIndex = 0;
    }
}
