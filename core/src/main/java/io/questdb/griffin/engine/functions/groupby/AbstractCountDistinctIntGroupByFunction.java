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
import io.questdb.griffin.engine.groupby.GroupByIntHashSet;
import io.questdb.std.Numbers;

/**
 * Abstract base class for count distinct group by functions on int values.
 */
public abstract class AbstractCountDistinctIntGroupByFunction extends LongFunction implements UnaryFunction, GroupByFunction {
    /**
     * The function argument.
     */
    protected final Function arg;
    /**
     * Primary hash set for counting distinct values.
     */
    protected final GroupByIntHashSet setA;
    /**
     * Secondary hash set for counting distinct values.
     */
    protected final GroupByIntHashSet setB;
    /**
     * The cardinality counter.
     */
    protected long cardinality;
    /**
     * The value index in the map.
     */
    protected int valueIndex;

    /**
     * Constructs a new count distinct int group by function.
     *
     * @param arg  the function argument
     * @param setA the primary hash set
     * @param setB the secondary hash set
     */
    public AbstractCountDistinctIntGroupByFunction(Function arg, GroupByIntHashSet setA, GroupByIntHashSet setB) {
        this.arg = arg;
        this.setA = setA;
        this.setB = setB;
    }

    @Override
    public void clear() {
        setA.resetPtr();
        setB.resetPtr();
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
        // inlined single value (count=1) or GroupByIntHashSet pointer (count>1)
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

        final long destCount = destValue.getLong(valueIndex);
        if (destCount == 0 || destCount == Numbers.LONG_NULL) {
            destValue.putLong(valueIndex, srcCount);
            destValue.putLong(valueIndex + 1, srcValue.getLong(valueIndex + 1));
            return;
        }

        if (srcCount == 1) { // inlined src value
            final int srcVal = (int) srcValue.getLong(valueIndex + 1);
            if (destCount == 1) { // dest also holds inlined value
                final int destVal = (int) destValue.getLong(valueIndex + 1);
                if (destVal != srcVal) {
                    setA.of(0).add(srcVal);
                    setA.add(destVal);
                    destValue.putLong(valueIndex, 2);
                    destValue.putLong(valueIndex + 1, setA.ptr());
                }
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
        if (destCount == 1) { // dest holds inlined value
            final int destVal = (int) destValue.getLong(valueIndex + 1);
            setA.of(srcPtr).add(destVal);
            destValue.putLong(valueIndex, setA.size());
            destValue.putLong(valueIndex + 1, setA.ptr());
        } else { // dest holds a set
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
