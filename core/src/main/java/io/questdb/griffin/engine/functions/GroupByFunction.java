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

package io.questdb.griffin.engine.functions;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Mutable;

public interface GroupByFunction extends Function, Mutable {
    int SAMPLE_BY_FILL_LINEAR = 4;
    int SAMPLE_BY_FILL_NONE = 8;
    int SAMPLE_BY_FILL_NULL = 16;
    int SAMPLE_BY_FILL_PREVIOUS = 2;
    int SAMPLE_BY_FILL_VALUE = 1;
    int SAMPLE_BY_FILL_ALL = SAMPLE_BY_FILL_LINEAR | SAMPLE_BY_FILL_NONE | SAMPLE_BY_FILL_PREVIOUS | SAMPLE_BY_FILL_VALUE | SAMPLE_BY_FILL_NULL;

    @Override
    default void clear() {
    }

    /**
     * Aggregates the buffered argument values for the current group in one go.
     * <p>
     * The engine materialises the argument column into a
     * {@link io.questdb.griffin.engine.groupby.GroupByColumnSink}, exposing the values in native
     * memory starting at {@code ptr}. Each entry has the fixed size implied by the function's
     * argument type. Implementations can use vectorised routines to consume the {@code count}
     * consecutive values and must write the resulting aggregate into {@code mapValue}.
     * <p>
     * This method:
     * <ul>
     *     <li>runs at most once per group {@link MapValue}, immediately after {@link #setEmpty(MapValue)};</li>
     *     <li>runs without a preceding {@link #computeFirst(MapValue, Record, long)} invocation;</li>
     *     <li>is not followed by {@link #merge(MapValue, MapValue)};</li>
     *     <li>always receives a non-zero {@code ptr} pointing to readable memory;</li>
     *     <li>is used only when {@link #supportsBatchComputation()} returns {@code true}.</li>
     * </ul>
     *
     * @param mapValue group state that must be updated with the aggregated result
     * @param ptr      native memory address of the first buffered value for the group
     * @param count    number of buffered values that can be read starting from {@code ptr}
     */
    default void computeBatch(MapValue mapValue, long ptr, int count) {
        throw new UnsupportedOperationException();
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

    /**
     * Returns true if the aggregate function's value is already calculation
     * and further row scan is not necessary. Only makes sense for non-keyed,
     * single-threaded group by.
     *
     * @param mapValue the map value to check
     * @return true if early exit is possible
     */
    default boolean earlyExit(MapValue mapValue) {
        return false;
    }

    /**
     * Returns recorded cardinality for hash set based functions such as count_distinct().
     * <p>
     * A prior {@link #resetStats()} call should be made to reset the counter before computing any values.
     *
     * @return the cardinality statistic
     */
    default long getCardinalityStat() {
        return 0;
    }

    /**
     * Returns the compute batch argument function for this group by function.
     *
     * @return the compute batch argument function, or null if not applicable
     */
    default Function getComputeBatchArg() {
        if (this instanceof UnaryFunction thisUnary) {
            // for unary functions, default to the function's argument.
            return thisUnary.getArg();
        }
        return null;
    }

    /**
     * Returns the compute batch argument type for this group by function.
     * <p>
     * Note: the returned type may not match the type of the function returned by
     * {@link #getComputeBatchArg()}. Example: in case of avg(long_col) the type of
     * the argument function is LONG, but the aggregate function's argument type is
     * DOUBLE. This means that the input values need to be materialized in
     * an intermediate buffer via getDouble calls before to calling
     * {@link #computeBatch(MapValue, long, int)}.
     *
     * @return the column type of the batch argument
     */
    default int getComputeBatchArgType() {
        if (this instanceof UnaryFunction) {
            // for unary functions, default to the function's output type.
            return getType();
        }
        return ColumnType.UNDEFINED;
    }

    /**
     * Returns the sample by flags supported by this function.
     *
     * @return the sample by flags
     */
    default int getSampleByFlags() {
        return SAMPLE_BY_FILL_VALUE | SAMPLE_BY_FILL_NONE | SAMPLE_BY_FILL_NULL | SAMPLE_BY_FILL_PREVIOUS;
    }

    /**
     * Returns the value index for this function in the map.
     *
     * @return the value index
     */
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

    /**
     * Returns true if {@link #earlyExit(MapValue)} method can be used.
     * Only makes sense for non-keyed, single-threaded group by.
     *
     * @return true if early exit is supported
     */
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
     *
     * @param destValue the destination map value to merge into
     * @param srcValue  the source map value to merge from
     */
    default void merge(MapValue destValue, MapValue srcValue) {
        throw new UnsupportedOperationException();
    }

    /**
     * Reset statistics for functions that support cardinality counter. After calling this method and prior
     * to any compute calls, {@link #getCardinalityStat()} will return 0.
     */
    default void resetStats() {
    }

    /**
     * Sets the allocator for this group by function.
     *
     * @param allocator the group by allocator
     */
    default void setAllocator(GroupByAllocator allocator) {
        // no-op
    }

    /**
     * Sets a byte value in the map value, used for interpolation.
     *
     * @param mapValue the map value to set
     * @param value    the byte value
     */
    default void setByte(MapValue mapValue, byte value) {
        throw new UnsupportedOperationException();
    }

    // TODO(RaphDal): to be used when doing interpolation
    default void setDecimal128(MapValue mapValue, Decimal128 value) {
        throw new UnsupportedOperationException();
    }

    // TODO(RaphDal): to be used when doing interpolation
    default void setDecimal256(MapValue mapValue, Decimal256 value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets a double value in the map value, used for interpolation.
     *
     * @param mapValue the map value to set
     * @param value    the double value
     */
    default void setDouble(MapValue mapValue, double value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets the map value to empty state, used by generated code.
     *
     * @param value the map value to set
     */
    default void setEmpty(MapValue value) {
        setNull(value);
    }

    /**
     * Sets a float value in the map value, used for interpolation.
     *
     * @param mapValue the map value to set
     * @param value    the float value
     */
    default void setFloat(MapValue mapValue, float value) {
        throw new UnsupportedOperationException();
    }

    // used when doing interpolation
    default void setInt(MapValue mapValue, int value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets a long value in the map value, used for interpolation.
     *
     * @param mapValue the map value to set
     * @param value    the long value
     */
    default void setLong(MapValue mapValue, long value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets the map value to null.
     *
     * @param mapValue the map value to set
     */
    void setNull(MapValue mapValue);

    // used when doing interpolation
    default void setShort(MapValue mapValue, short value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Indicates whether {@link #computeBatch(MapValue, long, int)}, {@link #getComputeBatchArg()},
     * and {@link #getComputeBatchArgType()} are implemented for this function. When {@code true},
     * the engine may materialise the argument column into native memory buffers and invoke
     * {@code computeBatch} instead of per-row aggregation for compatible execution paths.
     *
     * @return {@code true} if the function can consume batches via {@code computeBatch}, {@code false} otherwise
     */
    default boolean supportsBatchComputation() {
        return false;
    }

    @Override
    default boolean supportsParallelism() {
        return false;
    }
}
