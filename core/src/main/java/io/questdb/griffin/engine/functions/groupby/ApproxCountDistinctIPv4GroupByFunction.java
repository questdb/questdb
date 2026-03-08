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
import io.questdb.griffin.engine.groupby.hyperloglog.HyperLogLog;
import io.questdb.std.Hash;
import io.questdb.std.Numbers;

public class ApproxCountDistinctIPv4GroupByFunction extends LongFunction implements UnaryFunction, GroupByFunction {
    private static final long NULL_VALUE = -1;

    private final Function arg;
    private final HyperLogLog hllA;
    private final HyperLogLog hllB;
    private int hllPtrIndex;
    private int overwrittenFlagIndex;
    private int valueIndex;

    public ApproxCountDistinctIPv4GroupByFunction(Function arg, int precision) {
        this.arg = arg;
        this.hllA = new HyperLogLog(precision);
        this.hllB = new HyperLogLog(precision);
    }

    public ApproxCountDistinctIPv4GroupByFunction(Function arg) {
        this(arg, HyperLogLog.DEFAULT_PRECISION);
    }

    @Override
    public void clear() {
        hllA.resetPtr();
        hllB.resetPtr();
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final int val = arg.getIPv4(record);
        if (val != Numbers.IPv4_NULL) {
            final long hash = Hash.murmur3ToLong(val);
            long cardinality = hllA.of(0).addAndComputeCardinalityFast(hash);
            mapValue.putLong(hllPtrIndex, hllA.ptr());
            mapValue.putLong(valueIndex, cardinality);
        } else {
            mapValue.putLong(hllPtrIndex, 0);
            mapValue.putLong(valueIndex, NULL_VALUE);
        }
        mapValue.putBool(overwrittenFlagIndex, false);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final int val = arg.getIPv4(record);
        if (val != Numbers.IPv4_NULL) {
            final long hash = Hash.murmur3ToLong(val);
            long ptr = mapValue.getLong(hllPtrIndex);
            long cardinality = hllA.of(ptr).addAndComputeCardinalityFast(hash);
            mapValue.putLong(hllPtrIndex, hllA.ptr());
            mapValue.putLong(valueIndex, cardinality);
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public long getLong(Record rec) {
        if (rec.getBool(overwrittenFlagIndex)) {
            return rec.getLong(valueIndex);
        }

        long ptr = rec.getLong(hllPtrIndex);
        if (ptr == 0) {
            return 0;
        }
        long val = rec.getLong(valueIndex);
        if (val != NULL_VALUE) {
            return val;
        }
        hllA.of(ptr);
        return hllA.computeCardinality();
    }

    @Override
    public String getName() {
        return "approx_count_distinct";
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
        this.hllPtrIndex = valueIndex + 1;
        this.overwrittenFlagIndex = valueIndex + 2;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        initValueIndex(columnTypes.getColumnCount());
        columnTypes.add(ColumnType.LONG); // overwritten value
        columnTypes.add(ColumnType.LONG); // pointer to HyperLogLog
        columnTypes.add(ColumnType.BOOLEAN); // flag denoting whether the value has been overwritten
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
        if (srcValue.getBool(overwrittenFlagIndex)) {
            long srcCount = srcValue.getLong(valueIndex);
            if (srcCount == 0 || srcCount == Numbers.LONG_NULL) {
                return;
            }
            // If reached here, it would mean that the value has been overwritten by interpolation
            // associated with SAMPLE BY. However, since merge() is called only when the execution
            // is parallel, this cannot happen. To produce the correct result, interpolation can
            // only run on merged data (yielded by the merge phase), not on individual partitions.
            assert false : "merging overwritten values with HyperLogLog is unsupported";
        }

        long srcPtr = srcValue.getLong(hllPtrIndex);
        if (srcPtr == 0) {
            return;
        }

        if (destValue.getBool(overwrittenFlagIndex)) {
            long dstCount = destValue.getLong(valueIndex);
            if (dstCount == 0 || dstCount == Numbers.LONG_NULL) {
                destValue.putBool(overwrittenFlagIndex, false);
                destValue.putLong(hllPtrIndex, srcPtr);
                destValue.putLong(valueIndex, NULL_VALUE);
                return;
            }
            // See the comment above. The same applies here.
            assert false : "merging overwritten values with HyperLogLog is unsupported";
        }

        long destPtr = destValue.getLong(hllPtrIndex);
        if (destPtr == 0) {
            destValue.putBool(overwrittenFlagIndex, false);
            destValue.putLong(hllPtrIndex, srcPtr);
            destValue.putLong(valueIndex, NULL_VALUE);
            return;
        }

        hllA.of(destPtr);
        hllB.of(srcPtr);
        long mergedPtr = HyperLogLog.merge(hllA, hllB);

        destValue.putBool(overwrittenFlagIndex, false);
        destValue.putLong(hllPtrIndex, mergedPtr);
        destValue.putLong(valueIndex, NULL_VALUE);
    }

    @Override
    public void setAllocator(GroupByAllocator allocator) {
        hllA.setAllocator(allocator);
        hllB.setAllocator(allocator);
    }

    @Override
    public void setEmpty(MapValue mapValue) {
        overwrite(mapValue, 0L);
    }

    @Override
    public void setLong(MapValue mapValue, long value) {
        overwrite(mapValue, value);
    }

    @Override
    public void setNull(MapValue mapValue) {
        overwrite(mapValue, Numbers.LONG_NULL);
    }

    @Override
    public boolean supportsParallelism() {
        return true;
    }

    private void overwrite(MapValue mapValue, long value) {
        mapValue.putLong(valueIndex, value);
        mapValue.putLong(hllPtrIndex, 0);
        mapValue.putBool(overwrittenFlagIndex, true);
    }
}
