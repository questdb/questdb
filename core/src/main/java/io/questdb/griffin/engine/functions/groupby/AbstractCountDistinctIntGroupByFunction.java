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
import io.questdb.griffin.engine.groupby.GroupByIntHashSet;
import io.questdb.griffin.engine.groupby.GroupByLongList;
import io.questdb.std.Numbers;

public abstract class AbstractCountDistinctIntGroupByFunction extends LongFunction implements UnaryFunction, GroupByFunction {
    protected final Function arg;
    protected final GroupByLongList list;
    protected final GroupByIntHashSet setA;
    protected final GroupByIntHashSet setB;
    protected int valueIndex;

    public AbstractCountDistinctIntGroupByFunction(Function arg, GroupByIntHashSet setA, GroupByIntHashSet setB, GroupByLongList list) {
        this.arg = arg;
        this.setA = setA;
        this.setB = setB;
        this.list = list;
    }

    @Override
    public void clear() {
        setA.resetPtr();
        setB.resetPtr();
        list.resetPtr();
    }

    @Override
    public Function getArg() {
        return arg;
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
        // inlined single value (count=1) or GroupByIntHashSet (count>1) or GroupByLongList pointer (count=-1)
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

        if (srcCount == 1) { // inlined src value
            final int srcVal = (int) srcValue.getLong(valueIndex + 1);
            if (destCount == -1) { // dest holds accumulated sets
                // pick up the first set and add the value there
                final long destPtr = destValue.getLong(valueIndex + 1);
                list.of(destPtr);
                setA.of(list.get(0)).add(srcVal);
                list.set(0, setA.ptr());
            } else if (destCount == 1) { // dest holds inlined value
                final int destVal = (int) destValue.getLong(valueIndex + 1);
                if (destVal == srcVal) {
                    return;
                }
                setA.of(0).add(srcVal);
                setA.add(destVal);
                destValue.putLong(valueIndex, 2);
                destValue.putLong(valueIndex + 1, setA.ptr());
            } else { // dest holds a set
                final long destPtr = destValue.getLong(valueIndex + 1);
                setA.of(destPtr).add(srcVal);
                destValue.putLong(valueIndex, setA.size());
                destValue.putLong(valueIndex + 1, setA.ptr());
            }
            return;
        }

        // src holds a set
        final long srcPtr = srcValue.getLong(valueIndex + 1);
        if (destCount == -1) { // dest holds accumulated sets
            final long destPtr = destValue.getLong(valueIndex + 1);
            list.of(destPtr).add(srcPtr);
            destValue.putLong(valueIndex + 1, list.ptr());
        } else if (destCount == 1) { // dest holds inlined value
            final int destVal = (int) destValue.getLong(valueIndex + 1);
            setA.of(srcPtr).add(destVal);
            destValue.putLong(valueIndex, setA.size());
            destValue.putLong(valueIndex + 1, setA.ptr());
        } else { // dest holds a set
            final long destPtr = destValue.getLong(valueIndex + 1);
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

    private void mergeAccumulatedSets(MapValue value) {
        final long listPtr = value.getLong(valueIndex + 1);
        assert listPtr != 0;
        list.of(listPtr);
        assert list.size() > 0;

        // select set with max size and use it as dest set
        int maxSize = -1;
        int maxIndex = -1;
        for (int i = 0, n = list.size(); i < n; i++) {
            if (setA.of(list.get(i)).size() > maxSize) {
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
