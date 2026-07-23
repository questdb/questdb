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

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.groupby.AvgDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.KSumDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastNotNullDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.NSumDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumDoubleGroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import org.junit.Assert;
import org.junit.Test;

public class DoubleGroupByFunctionBatchTest extends AbstractGroupByFunctionBatchTest {
    // Stands in for a row whose column is NULL, as a column-top row reads.
    private static final Record NULL_RECORD = new Record() {
        @Override
        public double getDouble(int col) {
            return Double.NaN;
        }
    };

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
    public void testKSumDoubleBatch() {
        KSumDoubleGroupByFunction function = new KSumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(1.0, Double.NaN, 2.5, 3.5);
            function.computeBatch(value, ptr, 4, 0);

            Assert.assertEquals(7.0, function.getDouble(value), 0.0);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testKSumDoubleBatchAccumulates() {
        KSumDoubleGroupByFunction function = new KSumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(1.0, 2.0);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateDoubles(3.0, 4.0);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(10.0, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testKSumDoubleBatchAllNaN() {
        KSumDoubleGroupByFunction function = new KSumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(Double.NaN, Double.NaN);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    // computeNext skips +/-Inf via Numbers.isFinite, so the batched path must do the same.
    // A single +Inf row must not poison the rest of the batch. Use 12 rows so the SIMD
    // body (8-wide on x86) runs and the Inf falls inside it, exercising the path that
    // would otherwise only filter NaN.
    @Test
    public void testKSumDoubleBatchInfinityIsSkipped() {
        KSumDoubleGroupByFunction function = new KSumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(
                    1.0, 2.0, 3.0, Double.POSITIVE_INFINITY, 4.0, 5.0, 6.0, 7.0,
                    8.0, 9.0, 10.0, 11.0
            );
            function.computeBatch(value, ptr, 12, 0);

            // 1+2+...+11 = 66, Inf skipped.
            Assert.assertEquals(66.0, function.getDouble(value), 0.0);
        }
    }

    // +Inf and -Inf in the same batch collapse to NaN inside the native sum. The
    // batched path must still produce the sum of the finite rows.
    @Test
    public void testKSumDoubleBatchInfinityPairCollapsesToFinite() {
        KSumDoubleGroupByFunction function = new KSumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(
                    1.0, Double.POSITIVE_INFINITY, 2.0, 3.0, 4.0, Double.NEGATIVE_INFINITY, 5.0, 6.0,
                    7.0, 8.0
            );
            function.computeBatch(value, ptr, 10, 0);

            // 1+2+...+8 = 36, both Inf skipped.
            Assert.assertEquals(36.0, function.getDouble(value), 0.0);
        }
    }

    // A batch with one Inf must not poison the running sum produced by subsequent
    // all-finite batches. 9 rows so the SIMD body processes the +Inf row.
    @Test
    public void testKSumDoubleBatchInfinityPoisoningDoesNotPersist() {
        KSumDoubleGroupByFunction function = new KSumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(1.0, 2.0, 3.0, 4.0, Double.POSITIVE_INFINITY, 5.0, 6.0, 7.0, 8.0);
            function.computeBatch(value, ptr, 9, 0);

            ptr = allocateDoubles(10.0, 20.0);
            function.computeBatch(value, ptr, 2, 0);

            // 1+2+...+8 = 36, plus 10+20 = 66.
            Assert.assertEquals(66.0, function.getDouble(value), 0.0);
        }
    }

    // Kahan compensation should be applied across batch boundaries: many small values
    // added one big value should produce a more accurate result than naive sum.
    @Test
    public void testKSumDoubleBatchKahanCompensationAcrossBatches() {
        KSumDoubleGroupByFunction function = new KSumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            // Batch 1: one large value.
            long ptr = allocateDoubles(1e16);
            function.computeBatch(value, ptr, 1, 0);

            // Batch 2: many small values that would lose precision in naive accumulation.
            double[] smalls = new double[100];
            for (int i = 0; i < smalls.length; i++) {
                smalls[i] = 1.0;
            }
            ptr = allocateDoubles(smalls);
            function.computeBatch(value, ptr, smalls.length, 0);

            // Kahan should preserve the smalls. Naive double-add would lose them.
            Assert.assertEquals(1e16 + 100.0, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testKSumDoubleBatchMixedNaN() {
        KSumDoubleGroupByFunction function = new KSumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(1.0, Double.NaN, 2.0);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(3.0, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testKSumDoubleBatchZeroCountKeepsNaN() {
        KSumDoubleGroupByFunction function = new KSumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.computeBatch(value, 0, 0, 0);

            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testKSumDoubleSetEmpty() {
        KSumDoubleGroupByFunction function = new KSumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
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
    public void testLastNotNullDoubleBatchKeepsHigherRowIdNonNull() {
        LastNotNullDoubleGroupByFunction function = new LastNotNullDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            // A stored non-null must survive a batch that arrives at a lower rowId. See the class javadoc.
            long ptr = allocateDoubles(9.5);
            function.computeBatch(value, ptr, 1, 100);
            Assert.assertEquals(9.5, function.getDouble(value), 0.0);

            ptr = allocateDoubles(4.25);
            function.computeBatch(value, ptr, 1, 10);

            Assert.assertEquals(9.5, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testLastNotNullDoubleBatchReplacesStoredNull() {
        LastNotNullDoubleGroupByFunction function = new LastNotNullDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            // computeFirst writes NULL through with a real rowId; a non-null at a lower rowId must still
            // replace it. See the class javadoc.
            function.computeFirst(value, NULL_RECORD, 100);

            long ptr = allocateDoubles(4.25);
            function.computeBatch(value, ptr, 1, 10);

            Assert.assertEquals(4.25, function.getDouble(value), 0.0);
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
    public void testNSumDoubleBatch() {
        NSumDoubleGroupByFunction function = new NSumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(1.0, Double.NaN, 2.5, 3.5);
            function.computeBatch(value, ptr, 4, 0);

            Assert.assertEquals(7.0, function.getDouble(value), 0.0);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testNSumDoubleBatchAccumulates() {
        NSumDoubleGroupByFunction function = new NSumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(1.0, 2.0);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateDoubles(3.0, 4.0);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(10.0, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testNSumDoubleBatchAllNaN() {
        NSumDoubleGroupByFunction function = new NSumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(Double.NaN, Double.NaN);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    // computeNext skips +/-Inf via Numbers.isFinite, so the batched path must do the same.
    // A single +Inf row must not poison the rest of the batch.
    @Test
    public void testNSumDoubleBatchInfinityIsSkipped() {
        NSumDoubleGroupByFunction function = new NSumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(1.0, 2.0, Double.POSITIVE_INFINITY, 3.0);
            function.computeBatch(value, ptr, 4, 0);

            Assert.assertEquals(6.0, function.getDouble(value), 0.0);
        }
    }

    // +Inf and -Inf in the same batch would collapse to NaN inside the native sum.
    // The batched path must still produce the sum of the finite rows.
    @Test
    public void testNSumDoubleBatchInfinityPairCollapsesToFinite() {
        NSumDoubleGroupByFunction function = new NSumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(1.0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 2.0);
            function.computeBatch(value, ptr, 4, 0);

            Assert.assertEquals(3.0, function.getDouble(value), 0.0);
        }
    }

    // A batch with one Inf must not poison the running sum produced by subsequent
    // all-finite batches.
    @Test
    public void testNSumDoubleBatchInfinityPoisoningDoesNotPersist() {
        NSumDoubleGroupByFunction function = new NSumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(1.0, Double.POSITIVE_INFINITY);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateDoubles(2.0, 3.0);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(6.0, function.getDouble(value), 0.0);
        }
    }

    // Neumaier compensation should be applied across batch boundaries. Unlike Kahan,
    // Neumaier handles the case where the running sum is smaller than the increment.
    @Test
    public void testNSumDoubleBatchNeumaierCompensationAcrossBatches() {
        NSumDoubleGroupByFunction function = new NSumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            // Batch 1: many small values, running sum starts small.
            double[] smalls = new double[100];
            for (int i = 0; i < smalls.length; i++) {
                smalls[i] = 1.0;
            }
            long ptr = allocateDoubles(smalls);
            function.computeBatch(value, ptr, smalls.length, 0);

            // Batch 2: one large value. Without Neumaier, the small running sum
            // would be lost when adding to the much larger batch sum.
            ptr = allocateDoubles(1e16);
            function.computeBatch(value, ptr, 1, 0);

            Assert.assertEquals(100.0 + 1e16, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testNSumDoubleBatchMixedNaN() {
        NSumDoubleGroupByFunction function = new NSumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(1.0, Double.NaN, 2.0);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(3.0, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testNSumDoubleBatchZeroCountKeepsNaN() {
        NSumDoubleGroupByFunction function = new NSumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.computeBatch(value, 0, 0, 0);

            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testNSumDoubleSetEmpty() {
        NSumDoubleGroupByFunction function = new NSumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
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
