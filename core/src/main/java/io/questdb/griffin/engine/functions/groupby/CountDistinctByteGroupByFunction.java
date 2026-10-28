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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;

class CountDistinctByteGroupByFunction extends LongFunction implements UnaryFunction, GroupByFunction {
    private final Function arg;
    private final String name;
    private final int wordCount;
    private int valueIndex;

    CountDistinctByteGroupByFunction(Function arg, String name) {
        this.arg = arg;
        this.name = name;
        this.wordCount = arg.getType() == ColumnType.BOOLEAN ? 1 : 4;
        assert arg.getType() == ColumnType.BYTE || arg.getType() == ColumnType.BOOLEAN;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        setEmpty(mapValue);
        computeNext(mapValue, record, rowId);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final int value = arg.getByte(record) & 0xff;
        final int index = valueIndex + 1 + (value >>> 6);
        final long word = mapValue.getLong(index);
        final long bit = 1L << value;
        if ((word & bit) == 0) {
            mapValue.putLong(index, word | bit);
            mapValue.addLong(valueIndex, 1);
        }
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
        return name;
    }

    @Override
    public int getSampleByFlags() {
        return GroupByFunction.SAMPLE_BY_FILL_ALL;
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
        valueIndex = columnTypes.getColumnCount();
        for (int i = 0; i <= wordCount; i++) {
            columnTypes.add(ColumnType.LONG);
        }
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isStateSelfContained() {
        return true;
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        if (srcValue.getLong(valueIndex) <= 0) {
            return;
        }
        long count = 0;
        for (int i = 1; i <= wordCount; i++) {
            final long word = destValue.getLong(valueIndex + i) | srcValue.getLong(valueIndex + i);
            destValue.putLong(valueIndex + i, word);
            count += Long.bitCount(word);
        }
        destValue.putLong(valueIndex, count);
    }

    @Override
    public void setEmpty(MapValue mapValue) {
        setLong(mapValue, 0);
    }

    @Override
    public void setLong(MapValue mapValue, long value) {
        mapValue.putLong(valueIndex, value);
        for (int i = 1; i <= wordCount; i++) {
            mapValue.putLong(valueIndex + i, 0);
        }
    }

    @Override
    public void setNull(MapValue mapValue) {
        setLong(mapValue, Numbers.LONG_NULL);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }
}
