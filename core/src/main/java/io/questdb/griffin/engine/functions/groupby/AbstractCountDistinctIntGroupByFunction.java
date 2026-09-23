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
     * Whether the argument is known NOT NULL at plan time. Subclasses gate
     * their sentinel-skip on this so that NOT NULL columns still accept
     * the bit pattern that would otherwise read as NULL.
     */
    protected final boolean isArgNotNull;
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
        this.isArgNotNull = arg != null && arg.isNotNull();
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
        // count, including the sentinel when the flag below is set
        columnTypes.add(ColumnType.LONG);
        // inlined single non-sentinel value or GroupByIntHashSet pointer;
        // which one is decided by the stored count: count minus the sentinel
        // flag is 1 for an inlined value and greater for a set pointer
        columnTypes.add(ColumnType.LONG);
        // sentinel membership flag: the hash sets reserve the type's null
        // bit pattern as their empty marker, so a NOT NULL argument's
        // sentinel value cannot live in the set and is tracked here instead
        columnTypes.add(ColumnType.BOOLEAN);
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
        final boolean srcSentinel = srcValue.getBool(valueIndex + 2);
        final long srcStored = srcSentinel ? srcCount - 1 : srcCount;

        final long destCount = destValue.getLong(valueIndex);
        if (destCount == 0 || destCount == Numbers.LONG_NULL) {
            destValue.putLong(valueIndex, srcCount);
            destValue.putLong(valueIndex + 1, srcValue.getLong(valueIndex + 1));
            destValue.putBool(valueIndex + 2, srcSentinel);
            return;
        }
        final boolean destSentinel = destValue.getBool(valueIndex + 2);
        final long destStored = destSentinel ? destCount - 1 : destCount;
        final boolean sentinel = srcSentinel || destSentinel;
        final long sentinelInc = sentinel ? 1 : 0;

        if (srcStored == 0) { // src holds only the sentinel
            destValue.putLong(valueIndex, destStored + sentinelInc);
            destValue.putBool(valueIndex + 2, sentinel);
            return;
        }

        if (destStored == 0) { // dest holds at most the sentinel, adopt src's values
            destValue.putLong(valueIndex, srcStored + sentinelInc);
            destValue.putLong(valueIndex + 1, srcValue.getLong(valueIndex + 1));
            destValue.putBool(valueIndex + 2, sentinel);
            return;
        }

        if (srcStored == 1) { // inlined src value
            final int srcVal = (int) srcValue.getLong(valueIndex + 1);
            if (destStored == 1) { // dest also holds inlined value
                final int destVal = (int) destValue.getLong(valueIndex + 1);
                if (destVal != srcVal) {
                    setA.of(0).add(srcVal);
                    setA.add(destVal);
                    destValue.putLong(valueIndex, 2 + sentinelInc);
                    destValue.putLong(valueIndex + 1, setA.ptr());
                } else {
                    destValue.putLong(valueIndex, 1 + sentinelInc);
                }
            } else { // dest holds a set
                final long destPtr = destValue.getLong(valueIndex + 1);
                setA.of(destPtr).add(srcVal);
                destValue.putLong(valueIndex, setA.size() + sentinelInc);
                destValue.putLong(valueIndex + 1, setA.ptr());
            }
            destValue.putBool(valueIndex + 2, sentinel);
            return;
        }

        // src holds a set
        final long srcPtr = srcValue.getLong(valueIndex + 1);
        if (destStored == 1) { // dest holds inlined value
            final int destVal = (int) destValue.getLong(valueIndex + 1);
            setA.of(srcPtr).add(destVal);
            destValue.putLong(valueIndex, setA.size() + sentinelInc);
            destValue.putLong(valueIndex + 1, setA.ptr());
        } else { // dest holds a set
            final long destPtr = destValue.getLong(valueIndex + 1);
            setA.of(destPtr);
            setB.of(srcPtr);

            if (setA.size() > (setB.size() >>> 1)) {
                setA.merge(setB);
                destValue.putLong(valueIndex, setA.size() + sentinelInc);
                destValue.putLong(valueIndex + 1, setA.ptr());
            } else {
                // Set A is significantly smaller than set B, so we merge it into set B.
                setB.merge(setA);
                destValue.putLong(valueIndex, setB.size() + sentinelInc);
                destValue.putLong(valueIndex + 1, setB.ptr());
            }
        }
        destValue.putBool(valueIndex + 2, sentinel);
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
        mapValue.putBool(valueIndex + 2, false);
    }

    @Override
    public void setLong(MapValue mapValue, long value) {
        mapValue.putLong(valueIndex, value);
        mapValue.putLong(valueIndex + 1, 0);
        mapValue.putBool(valueIndex + 2, false);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, Numbers.LONG_NULL);
        mapValue.putLong(valueIndex + 1, 0);
        mapValue.putBool(valueIndex + 2, false);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }
}
