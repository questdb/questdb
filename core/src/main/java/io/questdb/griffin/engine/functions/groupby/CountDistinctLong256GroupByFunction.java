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
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByLong256HashSet;
import io.questdb.griffin.engine.groupby.GroupByLongList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;

public class CountDistinctLong256GroupByFunction extends LongFunction implements UnaryFunction, GroupByFunction {
    private final Function arg;
    private final GroupByLongList list;
    private final GroupByLong256HashSet setA;
    private final GroupByLong256HashSet setB;
    private int valueIndex;

    public CountDistinctLong256GroupByFunction(Function arg, int setInitialCapacity, double setLoadFactor, int workerCount) {
        this.arg = arg;
        setA = new GroupByLong256HashSet(setInitialCapacity, setLoadFactor, Numbers.LONG_NULL);
        setB = new GroupByLong256HashSet(setInitialCapacity, setLoadFactor, Numbers.LONG_NULL);
        list = new GroupByLongList(Math.max(workerCount, 4));
    }

    @Override
    public void clear() {
        setA.resetPtr();
        setB.resetPtr();
        list.resetPtr();
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final Long256 l256 = arg.getLong256A(record);
        if (isNotNull(l256)) {
            mapValue.putLong(valueIndex, 1);
            setA.of(0).add(l256.getLong0(), l256.getLong1(), l256.getLong2(), l256.getLong3());
            mapValue.putLong(valueIndex + 1, setA.ptr());
        } else {
            mapValue.putLong(valueIndex, 0);
            mapValue.putLong(valueIndex + 1, 0);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final Long256 l256 = arg.getLong256A(record);
        if (isNotNull(l256)) {
            final long l0 = l256.getLong0();
            final long l1 = l256.getLong1();
            final long l2 = l256.getLong2();
            final long l3 = l256.getLong3();
            final long ptr = mapValue.getLong(valueIndex + 1);
            final long index = setA.of(ptr).keyIndex(l0, l1, l2, l3);
            if (index >= 0) {
                setA.addAt(index, l0, l1, l2, l3);
                mapValue.addLong(valueIndex, 1);
                mapValue.putLong(valueIndex + 1, setA.ptr());
            }
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public int getComputeBatchArgType() {
        return ColumnType.LONG256;
    }

    @Override
    public long getLong(Record rec) {
        final long cnt = rec.getLong(valueIndex);
        if (cnt != -1) {
            return cnt;
        }
        final MapValue value;
        if (rec instanceof MapRecord) {
            value = ((MapRecord) rec).getValue();
        } else {
            value = (MapValue) rec;
        }
        mergeAccumulatedSets(value);
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
        // GroupByLong256HashSet (count>1) or GroupByLongList pointer (count=-1)
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
        if (srcCount == 0) {
            return;
        }

        final long destCount = destValue.getLong(valueIndex);
        if (destCount == 0) {
            destValue.putLong(valueIndex, srcCount);
            destValue.putLong(valueIndex + 1, srcValue.getLong(valueIndex + 1));
            return;
        }

        // src holds a set
        final long srcPtr = srcValue.getLong(valueIndex + 1);
        final long destPtr = destValue.getLong(valueIndex + 1);
        if (destCount == -1) { // dest holds accumulated sets
            list.of(destPtr).add(srcPtr);
            destValue.putLong(valueIndex + 1, list.ptr());
        } else { // dest holds a set
            list.of(0).add(destPtr);
            list.add(srcPtr);
            destValue.putLong(valueIndex, -1);
            destValue.putLong(valueIndex + 1, list.ptr());
        }
    }

    @Override
    public void setAllocator(GroupByAllocator allocator) {
        setA.setAllocator(allocator);
        setB.setAllocator(allocator);
        list.setAllocator(allocator);
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

    private static boolean isNotNull(Long256 value) {
        return value != null && value != Long256Impl.NULL_LONG256
                && (value.getLong0() != Numbers.LONG_NULL || value.getLong1() != Numbers.LONG_NULL
                || value.getLong2() != Numbers.LONG_NULL || value.getLong3() != Numbers.LONG_NULL);
    }

    private void mergeAccumulatedSets(MapValue value) {
        final long listPtr = value.getLong(valueIndex + 1);
        assert listPtr != 0;
        list.of(listPtr);
        assert list.size() > 0;

        // select set with max size and use it as dest set
        int maxSize = -1;
        int maxIndex = -1;
        for (int i = 0, n = list.size(); i < n; i++) {
            setA.of(list.get(i));
            final int size = setA.size();
            if (size > maxSize) {
                maxSize = setA.size();
                maxIndex = i;
            }
        }
        // init the dest set and erase the pointer in the list
        setA.of(list.get(maxIndex));
        list.set(maxIndex, 0);

        for (int i = 0, n = list.size(); i < n; i++) {
            final long srcPtr = list.get(i);
            if (srcPtr == 0) {
                continue;
            }
            setB.of(srcPtr);
            setA.merge(setB);
        }

        value.putLong(valueIndex, setA.size());
        value.putLong(valueIndex + 1, setA.ptr());
    }
}
