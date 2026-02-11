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
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByLong128HashSet;
import io.questdb.std.Numbers;
import io.questdb.std.Uuid;

public class CountDistinctUuidGroupByFunction extends LongFunction implements UnaryFunction, GroupByFunction {
    private final Function arg;
    private final GroupByLong128HashSet setA;
    private final GroupByLong128HashSet setB;
    private long cardinality;
    private int valueIndex;

    public CountDistinctUuidGroupByFunction(Function arg, int setInitialCapacity, double setLoadFactor) {
        this.arg = arg;
        setA = new GroupByLong128HashSet(setInitialCapacity, setLoadFactor, Numbers.LONG_NULL);
        setB = new GroupByLong128HashSet(setInitialCapacity, setLoadFactor, Numbers.LONG_NULL);
    }

    @Override
    public void clear() {
        setA.resetPtr();
        setB.resetPtr();
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final long lo = arg.getLong128Lo(record);
        final long hi = arg.getLong128Hi(record);
        if (!Uuid.isNull(lo, hi)) {
            mapValue.putLong(valueIndex, 1);
            setA.of(0).add(lo, hi);
            mapValue.putLong(valueIndex + 1, setA.ptr());
            cardinality++;
        } else {
            mapValue.putLong(valueIndex, 0);
            mapValue.putLong(valueIndex + 1, 0);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final long lo = arg.getLong128Lo(record);
        final long hi = arg.getLong128Hi(record);
        if (!Uuid.isNull(lo, hi)) {
            final long ptr = mapValue.getLong(valueIndex + 1);
            final long index = setA.of(ptr).keyIndex(lo, hi);
            if (index >= 0) {
                setA.addAt(index, lo, hi);
                mapValue.addLong(valueIndex, 1);
                mapValue.putLong(valueIndex + 1, setA.ptr());
                cardinality++;
            }
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public long getCardinalityStat() {
        return cardinality;
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
        // count
        columnTypes.add(ColumnType.LONG);
        // GroupByLong128HashSet pointer (count>1)
        columnTypes.add(ColumnType.LONG);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        final long srcCount = srcValue.getLong(valueIndex);
        if (srcCount == 0 || srcCount == Numbers.LONG_NULL) {
            return;
        }
        final long srcPtr = srcValue.getLong(valueIndex + 1);

        final long destCount = destValue.getLong(valueIndex);
        if (destCount == 0 || destCount == Numbers.LONG_NULL) {
            destValue.putLong(valueIndex, srcCount);
            destValue.putLong(valueIndex + 1, srcPtr);
            return;
        }
        final long destPtr = destValue.getLong(valueIndex + 1);

        setA.of(destPtr);
        setB.of(srcPtr);

        if (setA.size() > (setB.size() >>> 1)) {
            setA.merge(setB);
            destValue.putLong(valueIndex, setA.size());
            destValue.putLong(valueIndex + 1, setA.ptr());
        } else {
            // Set A is significantly smaller than set B, so we merge it into set B.
            setB.merge(setA);
            destValue.putLong(valueIndex, setB.size());
            destValue.putLong(valueIndex + 1, setB.ptr());
        }
    }

    @Override
    public void resetStats() {
        this.cardinality = 0;
    }

    @Override
    public void setAllocator(GroupByAllocator allocator) {
        setA.setAllocator(allocator);
        setB.setAllocator(allocator);
    }

    @Override
    public void setEmpty(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0);
        mapValue.putLong(valueIndex + 1, 0);
    }

    @Override
    public void setLong(MapValue mapValue, long value) {
        mapValue.putLong(valueIndex, value);
        mapValue.putLong(valueIndex + 1, 0);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, Numbers.LONG_NULL);
        mapValue.putLong(valueIndex + 1, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }
}
