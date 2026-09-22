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

import io.questdb.griffin.engine.functions.columns.Long256Column;
import io.questdb.griffin.engine.functions.groupby.SumLong256GroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class Long256GroupByFunctionBatchTest extends AbstractGroupByFunctionBatchTest {
    @Test
    public void testSumLong256Batch() {
        SumLong256GroupByFunction function = new SumLong256GroupByFunction(Long256Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            // 1 + 2 + 3 = 6
            long ptr = allocateLongs(
                    1L, 0L, 0L, 0L,
                    2L, 0L, 0L, 0L,
                    3L, 0L, 0L, 0L
            );
            function.computeBatch(value, ptr, 3, 0);

            Long256Impl result = (Long256Impl) function.getLong256A(value);
            Assert.assertEquals(6L, result.getLong0());
            Assert.assertEquals(0L, result.getLong1());
            Assert.assertEquals(0L, result.getLong2());
            Assert.assertEquals(0L, result.getLong3());
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testSumLong256BatchAccumulates() {
        SumLong256GroupByFunction function = new SumLong256GroupByFunction(Long256Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            // Batch 1: 1 + 2 = 3
            long ptr = allocateLongs(
                    1L, 0L, 0L, 0L,
                    2L, 0L, 0L, 0L
            );
            function.computeBatch(value, ptr, 2, 0);

            // Batch 2: 3 + 4 = 7. Running total = 3 + 7 = 10.
            ptr = allocateLongs(
                    3L, 0L, 0L, 0L,
                    4L, 0L, 0L, 0L
            );
            function.computeBatch(value, ptr, 2, 0);

            Long256Impl result = (Long256Impl) function.getLong256A(value);
            Assert.assertEquals(10L, result.getLong0());
            Assert.assertEquals(0L, result.getLong1());
            Assert.assertEquals(0L, result.getLong2());
            Assert.assertEquals(0L, result.getLong3());
        }
    }

    @Test
    public void testSumLong256BatchAllNull() {
        SumLong256GroupByFunction function = new SumLong256GroupByFunction(Long256Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(
                    Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL,
                    Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL
            );
            function.computeBatch(value, ptr, 2, 0);

            Long256Impl result = (Long256Impl) function.getLong256A(value);
            Assert.assertTrue(Long256Impl.isNull(result));
        }
    }

    // Verify carry propagates from limb 0 into limb 1.
    @Test
    public void testSumLong256BatchCarryAcrossLimbs() {
        SumLong256GroupByFunction function = new SumLong256GroupByFunction(Long256Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            // 0xFFFFFFFF_FFFFFFFF + 1 = 0x1_00000000_00000000 across the first two limbs.
            long ptr = allocateLongs(
                    -1L, 0L, 0L, 0L,
                    1L, 0L, 0L, 0L
            );
            function.computeBatch(value, ptr, 2, 0);

            Long256Impl result = (Long256Impl) function.getLong256A(value);
            Assert.assertEquals(0L, result.getLong0());
            Assert.assertEquals(1L, result.getLong1());
            Assert.assertEquals(0L, result.getLong2());
            Assert.assertEquals(0L, result.getLong3());
        }
    }

    // Verify carry propagates from limb 1 into limb 2.
    @Test
    public void testSumLong256BatchCarryFromLimb1ToLimb2() {
        SumLong256GroupByFunction function = new SumLong256GroupByFunction(Long256Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(
                    0L, -1L, 0L, 0L,
                    0L, 1L, 0L, 0L
            );
            function.computeBatch(value, ptr, 2, 0);

            Long256Impl result = (Long256Impl) function.getLong256A(value);
            Assert.assertEquals(0L, result.getLong0());
            Assert.assertEquals(0L, result.getLong1());
            Assert.assertEquals(1L, result.getLong2());
            Assert.assertEquals(0L, result.getLong3());
        }
    }

    // Verify carry propagates from limb 2 into limb 3.
    @Test
    public void testSumLong256BatchCarryFromLimb2ToLimb3() {
        SumLong256GroupByFunction function = new SumLong256GroupByFunction(Long256Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(
                    0L, 0L, -1L, 0L,
                    0L, 0L, 1L, 0L
            );
            function.computeBatch(value, ptr, 2, 0);

            Long256Impl result = (Long256Impl) function.getLong256A(value);
            Assert.assertEquals(0L, result.getLong0());
            Assert.assertEquals(0L, result.getLong1());
            Assert.assertEquals(0L, result.getLong2());
            Assert.assertEquals(1L, result.getLong3());
        }
    }

    @Test
    public void testSumLong256BatchMixedNull() {
        SumLong256GroupByFunction function = new SumLong256GroupByFunction(Long256Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(
                    5L, 0L, 0L, 0L,
                    Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL,
                    7L, 0L, 0L, 0L
            );
            function.computeBatch(value, ptr, 3, 0);

            Long256Impl result = (Long256Impl) function.getLong256A(value);
            Assert.assertEquals(12L, result.getLong0());
            Assert.assertEquals(0L, result.getLong1());
            Assert.assertEquals(0L, result.getLong2());
            Assert.assertEquals(0L, result.getLong3());
        }
    }

    @Test
    public void testSumLong256BatchZeroCountKeepsNull() {
        SumLong256GroupByFunction function = new SumLong256GroupByFunction(Long256Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.computeBatch(value, 0, 0, 0);

            Long256Impl result = (Long256Impl) function.getLong256A(value);
            Assert.assertTrue(Long256Impl.isNull(result));
        }
    }

    @Test
    public void testSumLong256Merge() {
        SumLong256GroupByFunction function = new SumLong256GroupByFunction(Long256Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue dest = prepare(function)) {
            // dest = 10
            long ptr = allocateLongs(10L, 0L, 0L, 0L);
            function.computeBatch(dest, ptr, 1, 0);

            try (SimpleMapValue src = new SimpleMapValue(1)) {
                function.initValueIndex(0);
                function.setNull(src);
                ptr = allocateLongs(20L, 0L, 0L, 0L);
                function.computeBatch(src, ptr, 1, 0);

                function.merge(dest, src);
            }

            Long256Impl result = (Long256Impl) function.getLong256A(dest);
            Assert.assertEquals(30L, result.getLong0());
            Assert.assertEquals(0L, result.getLong1());
            Assert.assertEquals(0L, result.getLong2());
            Assert.assertEquals(0L, result.getLong3());
        }
    }

    @Test
    public void testSumLong256MergeDestNull() {
        SumLong256GroupByFunction function = new SumLong256GroupByFunction(Long256Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue dest = prepare(function)) {
            try (SimpleMapValue src = new SimpleMapValue(1)) {
                function.initValueIndex(0);
                function.setNull(src);
                long ptr = allocateLongs(42L, 0L, 0L, 0L);
                function.computeBatch(src, ptr, 1, 0);

                function.merge(dest, src);
            }

            Long256Impl result = (Long256Impl) function.getLong256A(dest);
            Assert.assertEquals(42L, result.getLong0());
        }
    }

    @Test
    public void testSumLong256MergeSrcNullIsNoOp() {
        SumLong256GroupByFunction function = new SumLong256GroupByFunction(Long256Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue dest = prepare(function)) {
            long ptr = allocateLongs(7L, 0L, 0L, 0L);
            function.computeBatch(dest, ptr, 1, 0);

            try (SimpleMapValue src = new SimpleMapValue(1)) {
                function.initValueIndex(0);
                function.setNull(src);
                function.merge(dest, src);
            }

            Long256Impl result = (Long256Impl) function.getLong256A(dest);
            Assert.assertEquals(7L, result.getLong0());
        }
    }

    @Test
    public void testSumLong256SetEmpty() {
        SumLong256GroupByFunction function = new SumLong256GroupByFunction(Long256Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Long256Impl result = (Long256Impl) function.getLong256A(value);
            Assert.assertTrue(Long256Impl.isNull(result));
        }
    }

    @Test
    public void testSumLong256SupportsParallelism() {
        SumLong256GroupByFunction function = new SumLong256GroupByFunction(Long256Column.newInstance(COLUMN_INDEX));
        Assert.assertTrue(function.supportsParallelism());
    }
}
