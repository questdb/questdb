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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.groupby.AvgDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastNotNullDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumDoubleGroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class DoubleGroupByFunctionBatchTest {
    private static final int COLUMN_INDEX = 123;
    private long lastAllocated;
    private long lastSize;

    @After
    public void tearDown() {
        if (lastAllocated != 0) {
            Unsafe.free(lastAllocated, lastSize, MemoryTag.NATIVE_DEFAULT);
            lastAllocated = 0;
            lastSize = 0;
        }
    }

    // Verify that computeBatch is consistent with computeNext: when the running sum
    // overflows to +Infinity, it is preserved. AvgDouble's computeNext uses addDouble
    // with no inner guard, so Infinity + finite = Infinity naturally.
    @Test
    public void testAvgDoubleBatchAccumulatedInfinityIsPreserved() {
        AvgDoubleGroupByFunction function = new AvgDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            // Batch 1: running sum = MAX_VALUE (finite)
            long ptr = allocateDoubles(Double.MAX_VALUE);
            function.computeBatch(value, ptr, 1, 0);

            // Batch 2: running sum = MAX_VALUE + MAX_VALUE = +Infinity
            ptr = allocateDoubles(Double.MAX_VALUE);
            function.computeBatch(value, ptr, 1, 0);

            // Batch 3: Infinity is preserved, not overwritten
            ptr = allocateDoubles(1.0);
            function.computeBatch(value, ptr, 1, 0);

            // avg = Infinity / 3 = Infinity
            Assert.assertTrue(Double.isInfinite(function.getDouble(value)));
        }
    }

    @Test
    public void testCountDoubleBatch() {
        CountDoubleGroupByFunction function = new CountDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(1.0, Double.NaN, 2.0, Double.NaN, 3.0);
            function.computeBatch(value, ptr, 5, 0);

            Assert.assertEquals(3L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testCountDoubleBatchAccumulates() {
        CountDoubleGroupByFunction function = new CountDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(1.0, Double.NaN);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateDoubles(2.0, 3.0);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(3L, function.getLong(value));
        }
    }

    @Test
    public void testCountDoubleBatchAllNaN() {
        CountDoubleGroupByFunction function = new CountDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(Double.NaN, Double.NaN, Double.NaN);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testCountDoubleBatchZeroCountKeepsZero() {
        CountDoubleGroupByFunction function = new CountDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.computeBatch(value, 0, 0, 0);

            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testCountDoubleSetEmpty() {
        CountDoubleGroupByFunction function = new CountDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testFirstDoubleBatch() {
        FirstDoubleGroupByFunction function = new FirstDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(5.5, 6.6, 7.7);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(5.5, function.getDouble(value), 0.0);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstDoubleBatchAccumulates() {
        FirstDoubleGroupByFunction function = new FirstDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(5.5, 6.6);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateDoubles(7.7, 8.8);
            function.computeBatch(value, ptr, 2, 2);

            Assert.assertEquals(5.5, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testFirstDoubleBatchAllNaN() {
        FirstDoubleGroupByFunction function = new FirstDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(Double.NaN, 1.0);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testFirstDoubleBatchEmpty() {
        FirstDoubleGroupByFunction function = new FirstDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateDoubles();
            function.computeBatch(value, ptr, 0, 0);

            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testFirstDoubleBatchNotCalled() {
        FirstDoubleGroupByFunction function = new FirstDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testFirstDoubleSetEmpty() {
        FirstDoubleGroupByFunction function = new FirstDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testFirstNotNullDoubleBatch() {
        FirstNotNullDoubleGroupByFunction function = new FirstNotNullDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(Double.NaN, 7.7, Double.NaN);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(7.7, function.getDouble(value), 0.0);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstNotNullDoubleBatchAccumulates() {
        FirstNotNullDoubleGroupByFunction function = new FirstNotNullDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(Double.NaN, 7.7);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateDoubles(8.8, Double.NaN);
            function.computeBatch(value, ptr, 2, 2);

            Assert.assertEquals(7.7, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testLastDoubleBatch() {
        LastDoubleGroupByFunction function = new LastDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateDoubles(11.0, 22.0, 33.0);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(2, value.getLong(0));
            Assert.assertEquals(33.0, function.getDouble(value), 0.0);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastDoubleBatchAccumulates() {
        LastDoubleGroupByFunction function = new LastDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateDoubles(11.0, 22.0);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateDoubles(33.0, 44.0);
            function.computeBatch(value, ptr, 2, 2);

            Assert.assertEquals(44.0, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testLastDoubleBatchAllNaN() {
        LastDoubleGroupByFunction function = new LastDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateDoubles(11.0, Double.NaN);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(1, value.getLong(0));
            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testLastDoubleSetEmpty() {
        LastDoubleGroupByFunction function = new LastDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testLastNotNullDoubleBatch() {
        LastNotNullDoubleGroupByFunction function = new LastNotNullDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateDoubles(Double.NaN, 5.5, Double.NaN, 6.6);
            function.computeBatch(value, ptr, 4, 0);

            Assert.assertEquals(6.6, function.getDouble(value), 0.0);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastNotNullDoubleBatchAccumulates() {
        LastNotNullDoubleGroupByFunction function = new LastNotNullDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateDoubles(5.5, Double.NaN);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateDoubles(Double.NaN, 6.6);
            function.computeBatch(value, ptr, 2, 2);

            Assert.assertEquals(6.6, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testMaxDoubleBatch() {
        MaxDoubleGroupByFunction function = new MaxDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putDouble(0, -999.0);

            long ptr = allocateDoubles(-10.0, Double.NaN, 15.5, 7.0);
            function.computeBatch(value, ptr, 4, 0);

            Assert.assertEquals(15.5, function.getDouble(value), 0.0);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMaxDoubleBatchAccumulates() {
        MaxDoubleGroupByFunction function = new MaxDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(1.0, 5.0);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateDoubles(3.0, 2.0);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(5.0, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testMaxDoubleBatchAllNaN() {
        MaxDoubleGroupByFunction function = new MaxDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(Double.NaN, Double.NaN, Double.NaN);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testMaxDoubleSetEmpty() {
        MaxDoubleGroupByFunction function = new MaxDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testMinDoubleBatch() {
        MinDoubleGroupByFunction function = new MinDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putDouble(0, 999.0);

            long ptr = allocateDoubles(Double.NaN, 4.0, 2.5, 3.0);
            function.computeBatch(value, ptr, 4, 0);

            Assert.assertEquals(2.5, function.getDouble(value), 0.0);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMinDoubleBatchAccumulates() {
        MinDoubleGroupByFunction function = new MinDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(5.0, 3.0);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateDoubles(4.0, 1.0);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(1.0, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testMinDoubleBatchAllNaN() {
        MinDoubleGroupByFunction function = new MinDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(Double.NaN, Double.NaN);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testMinDoubleSetEmpty() {
        MinDoubleGroupByFunction function = new MinDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testSumDoubleBatch() {
        SumDoubleGroupByFunction function = new SumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(1.0, Double.NaN, 2.5, 3.5);
            function.computeBatch(value, ptr, 4, 0);

            Assert.assertEquals(7.0, function.getDouble(value), 0.0);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    // Verify that computeBatch preserves Infinity: when the running sum overflows
    // to +Infinity, subsequent finite batches add to it (Infinity + finite = Infinity).
    @Test
    public void testSumDoubleBatchAccumulatedInfinityIsPreserved() {
        SumDoubleGroupByFunction function = new SumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            // Batch 1: running sum = MAX_VALUE (finite)
            long ptr = allocateDoubles(Double.MAX_VALUE);
            function.computeBatch(value, ptr, 1, 0);
            Assert.assertEquals(Double.MAX_VALUE, function.getDouble(value), 0.0);

            // Batch 2: running sum = MAX_VALUE + MAX_VALUE = +Infinity
            ptr = allocateDoubles(Double.MAX_VALUE);
            function.computeBatch(value, ptr, 1, 0);
            Assert.assertEquals(Double.POSITIVE_INFINITY, function.getDouble(value), 0.0);

            // Batch 3: Infinity + 1.0 = Infinity (preserved, not reset)
            ptr = allocateDoubles(1.0);
            function.computeBatch(value, ptr, 1, 0);
            Assert.assertEquals(Double.POSITIVE_INFINITY, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testSumDoubleBatchAccumulates() {
        SumDoubleGroupByFunction function = new SumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(1.0, 2.0);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateDoubles(3.0, 4.0);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(10.0, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testSumDoubleBatchAllNaN() {
        SumDoubleGroupByFunction function = new SumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(Double.NaN, Double.NaN);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testSumDoubleBatchZeroCountKeepsExistingValue() {
        SumDoubleGroupByFunction function = new SumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putDouble(0, 55.0);

            function.computeBatch(value, 0, 0, 0);

            Assert.assertEquals(55.0, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testSumDoubleBatchInfinityInput() {
        SumDoubleGroupByFunction function = new SumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(1.0, Double.POSITIVE_INFINITY, 2.0);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(Double.POSITIVE_INFINITY, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testSumDoubleBatchNegativeInfinityInput() {
        SumDoubleGroupByFunction function = new SumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(1.0, Double.NEGATIVE_INFINITY, 2.0);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(Double.NEGATIVE_INFINITY, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testSumDoubleComputeNextInfinityInput() {
        SumDoubleGroupByFunction function = new SumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.computeFirst(value, recordOf(1.0), 0);
            function.computeNext(value, recordOf(Double.POSITIVE_INFINITY), 1);

            Assert.assertEquals(Double.POSITIVE_INFINITY, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testSumDoubleComputeNextInfinityAccumulator() {
        SumDoubleGroupByFunction function = new SumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.computeFirst(value, recordOf(Double.POSITIVE_INFINITY), 0);
            function.computeNext(value, recordOf(1.0), 1);

            Assert.assertEquals(Double.POSITIVE_INFINITY, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testSumDoubleComputeNextNaNSkipped() {
        SumDoubleGroupByFunction function = new SumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.computeFirst(value, recordOf(5.0), 0);
            function.computeNext(value, recordOf(Double.NaN), 1);

            Assert.assertEquals(5.0, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testSumDoubleMergeInfinitySrc() {
        SumDoubleGroupByFunction function = new SumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue dest = prepare(function)) {
            dest.putDouble(0, 5.0);
            try (SimpleMapValue src = new SimpleMapValue(1)) {
                src.putDouble(0, Double.POSITIVE_INFINITY);
                function.merge(dest, src);
            }
            Assert.assertEquals(Double.POSITIVE_INFINITY, function.getDouble(dest), 0.0);
        }
    }

    @Test
    public void testSumDoubleMergeInfinityDest() {
        SumDoubleGroupByFunction function = new SumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue dest = prepare(function)) {
            dest.putDouble(0, Double.POSITIVE_INFINITY);
            try (SimpleMapValue src = new SimpleMapValue(1)) {
                src.putDouble(0, 5.0);
                function.merge(dest, src);
            }
            Assert.assertEquals(Double.POSITIVE_INFINITY, function.getDouble(dest), 0.0);
        }
    }

    @Test
    public void testSumDoubleMergeNaNSrcSkipped() {
        SumDoubleGroupByFunction function = new SumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue dest = prepare(function)) {
            dest.putDouble(0, 5.0);
            try (SimpleMapValue src = new SimpleMapValue(1)) {
                src.putDouble(0, Double.NaN);
                function.merge(dest, src);
            }
            Assert.assertEquals(5.0, function.getDouble(dest), 0.0);
        }
    }

    @Test
    public void testAvgDoubleComputeFirstInfinity() {
        AvgDoubleGroupByFunction function = new AvgDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.computeFirst(value, recordOf(Double.POSITIVE_INFINITY), 0);

            Assert.assertEquals(Double.POSITIVE_INFINITY, value.getDouble(0), 0.0);
            Assert.assertEquals(1L, value.getLong(1));
        }
    }

    @Test
    public void testAvgDoubleComputeFirstNaN() {
        AvgDoubleGroupByFunction function = new AvgDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.computeFirst(value, recordOf(Double.NaN), 0);

            Assert.assertEquals(0.0, value.getDouble(0), 0.0);
            Assert.assertEquals(0L, value.getLong(1));
        }
    }

    @Test
    public void testAvgDoubleComputeNextInfinity() {
        AvgDoubleGroupByFunction function = new AvgDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.computeFirst(value, recordOf(1.0), 0);
            function.computeNext(value, recordOf(Double.POSITIVE_INFINITY), 1);

            Assert.assertEquals(2L, value.getLong(1));
            Assert.assertTrue(Double.isInfinite(function.getDouble(value)));
        }
    }

    @Test
    public void testAvgDoubleComputeNextNaNSkipped() {
        AvgDoubleGroupByFunction function = new AvgDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.computeFirst(value, recordOf(4.0), 0);
            function.computeNext(value, recordOf(Double.NaN), 1);

            Assert.assertEquals(1L, value.getLong(1));
            Assert.assertEquals(4.0, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testSumDoubleSetEmpty() {
        SumDoubleGroupByFunction function = new SumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    private long allocateDoubles(double... values) {
        if (lastAllocated != 0) {
            Unsafe.free(lastAllocated, lastSize, MemoryTag.NATIVE_DEFAULT);
        }
        lastSize = (long) values.length * Double.BYTES;
        lastAllocated = Unsafe.malloc(lastSize, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < values.length; i++) {
            Unsafe.putDouble(lastAllocated + (long) i * Double.BYTES, values[i]);
        }
        return lastAllocated;
    }

    private SimpleMapValue prepare(GroupByFunction function) {
        var columnTypes = new ArrayColumnTypes();
        function.initValueTypes(columnTypes);
        SimpleMapValue value = new SimpleMapValue(columnTypes.getColumnCount());
        function.initValueIndex(0);
        function.setEmpty(value);
        return value;
    }

    private Record recordOf(double value) {
        return new Record() {
            @Override
            public double getDouble(int col) {
                assert col == COLUMN_INDEX;
                return value;
            }
        };
    }
}
