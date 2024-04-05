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

package io.questdb.griffin.engine.functions;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.std.Mutable;

public interface GroupByFunction extends Function, Mutable {

    @Override
    default void clear() {
    }

    /**
     * Performs the first aggregation within a group.
     * <p>
     * Row id is provided for aggregation functions that consider row order, such as first/last.
     * The value is guaranteed to be growing between subsequent calls. In case of parallel GROUP BY,
     * this means that all row ids of a later page frame are guaranteed to be greater than row ids
     * of all previous page frames. {@link Record#getRowId()} shouldn't be used for this purpose
     * since not all records implement it, and it's not guaranteed to be growing.
     *
     * @param mapValue map value holding the group
     * @param record   record holding the aggregated row
     * @param rowId    row id; the value may be different from record.getRowId()
     */
    void computeFirst(MapValue mapValue, Record record, long rowId);

    /**
     * Performs a subsequent aggregation within a group.
     * <p>
     * Row id is provided for aggregation functions that consider row order, such as first/last.
     * The value is guaranteed to be growing between subsequent calls. In case of parallel GROUP BY,
     * this means that all row ids of a later page frame are guaranteed to be greater than row ids
     * of all previous page frames. {@link Record#getRowId()} shouldn't be used for this purpose
     * since not all records implement it, and it's not guaranteed to be growing.
     *
     * @param mapValue map value holding the group
     * @param record   record holding the aggregated row
     * @param rowId    row id; the value may be different from record.getRowId()
     */
    void computeNext(MapValue mapValue, Record record, long rowId);

    // only makes sense for non-keyed group by
    default boolean earlyExit(MapValue mapValue) {
        return false;
    }

    int getValueIndex();

    /**
     * Called for group by function cloned to be used in different threads of parallel execution.
     * Guaranteed to be called before any other call accessing the map or map values.
     * {@link #initValueTypes(ArrayColumnTypes)} is not called on such functions.
     *
     * @param valueIndex index of the first value of the original function in the type array
     */
    void initValueIndex(int valueIndex);

    /**
     * Called for group by function to register its values to be used in the map.
     * Guaranteed to be called before any other call accessing the map or map values.
     * {@link #initValueIndex(int)} is not called on such functions.
     *
     * @param columnTypes value type array
     */
    void initValueTypes(ArrayColumnTypes columnTypes);

    default void interpolateBoundary(
            MapValue mapValue1,
            MapValue mapValue2,
            long boundaryTimestamp,
            boolean isEndOfBoundary
    ) {
        throw new UnsupportedOperationException();
    }

    default void interpolateGap(
            MapValue mapValue,
            MapValue mapValue1,
            MapValue mapValue2,
            long x
    ) {
        throw new UnsupportedOperationException();
    }

    // only makes sense for non-keyed group by
    default boolean isEarlyExitSupported() {
        return false;
    }

    default boolean isInterpolationSupported() {
        return false;
    }

    default boolean isScalar() {
        return true;
    }

    /**
     * Used in parallel GROUP BY to merge partial results. Both values are guaranteed to be not new
     * when this method is called, i.e. {@code !destValue.isNew() && !srcValue.isNew()} is true.
     */
    default void merge(MapValue destValue, MapValue srcValue) {
        throw new UnsupportedOperationException();
    }

    default void setAllocator(GroupByAllocator allocator) {
        // no-op
    }

    // used when doing interpolation
    default void setByte(MapValue mapValue, byte value) {
        throw new UnsupportedOperationException();
    }

    // used when doing interpolation
    default void setDouble(MapValue mapValue, double value) {
        throw new UnsupportedOperationException();
    }

    default void setEmpty(MapValue value) {
        setNull(value);
    }

    // used when doing interpolation
    default void setFloat(MapValue mapValue, float value) {
        throw new UnsupportedOperationException();
    }

    // used when doing interpolation
    default void setInt(MapValue mapValue, int value) {
        throw new UnsupportedOperationException();
    }

    // used when doing interpolation
    default void setLong(MapValue mapValue, long value) {
        throw new UnsupportedOperationException();
    }

    void setNull(MapValue mapValue);

    // used when doing interpolation
    default void setShort(MapValue mapValue, short value) {
        throw new UnsupportedOperationException();
    }

    @Override
    default boolean supportsParallelism() {
        return false;
    }
}
