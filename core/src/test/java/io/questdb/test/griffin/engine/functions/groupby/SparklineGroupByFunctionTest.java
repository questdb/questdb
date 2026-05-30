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

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.groupby.SparklineGroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * White-box tests for {@link SparklineGroupByFunction#computeNext} - its
 * per-frame batch tracking and pair-buffer growth. The MapValue layout is
 * {@code [ptr, count, capacity, descPtr, descCount, descCapacity, lastFrameId]}.
 * The descriptor buffer ({@code descPtr}, slot 3) stays unallocated while frames
 * arrive as a single consecutive, in-order run; a batch boundary is recorded
 * only at a gap or an out-of-order frame. {@code merge} stamps {@code lastFrameId}
 * with a negative sentinel so a later {@code computeNext} on the merged value
 * trips an assertion (CI runs with -ea) instead of silently reallocating the
 * exactly-sized descriptor buffer.
 */
public class SparklineGroupByFunctionTest extends AbstractCairoTest {

    @Test
    public void testBufferGrowthPreservesEntries() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration);
                    SimpleMapValue mapValue = new SimpleMapValue(7)
            ) {
                SparklineGroupByFunction func = sparklineFunction(allocator);
                // The pair buffer starts at INITIAL_CAPACITY entries; appending
                // one beyond it forces a grow. Sparkline grows via
                // allocator.realloc (twap uses malloc + memcpy), so preserving
                // the existing entries is the allocator's job - this pins that
                // they survive the move. All entries share frame 0, so the
                // buffer stays a single batch and the grow path is exercised in
                // isolation from the batch-boundary logic.
                func.computeFirst(mapValue, null, rowId(0, 0));
                final long initialCapacity = mapValue.getLong(2);
                for (int i = 1; i <= initialCapacity; i++) {
                    func.computeNext(mapValue, null, rowId(0, i));
                }
                final long count = initialCapacity + 1;
                Assert.assertEquals("entry count", count, mapValue.getLong(1));
                Assert.assertEquals("capacity doubled once", initialCapacity * 2, mapValue.getLong(2));
                Assert.assertEquals("descriptor pointer", 0, mapValue.getLong(3));
                // Every entry survived the realloc move: entry i holds rowId i
                // (frame 0, local id i) and the constant value 1.0. Entries are
                // 16 bytes - rowId at +0, value at +8.
                final long ptr = mapValue.getLong(0);
                for (int i = 0; i < count; i++) {
                    Assert.assertEquals("rowId at entry " + i, i, Unsafe.getLong(ptr + i * 16L));
                    Assert.assertEquals("value at entry " + i, 1.0, Unsafe.getDouble(ptr + i * 16L + 8), 0.0);
                }
            }
        });
    }

    @Test
    public void testComputeNextAfterMergeTripsAssertion() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration);
                    SimpleMapValue destValue = new SimpleMapValue(7);
                    SimpleMapValue srcValue = new SimpleMapValue(7)
            ) {
                SparklineGroupByFunction func = sparklineFunction(allocator);
                // Two partials merged: merge() stamps the terminal -1 sentinel
                // into the destination's lastFrameId slot.
                func.computeFirst(destValue, null, rowId(0, 0));
                func.computeFirst(srcValue, null, rowId(1, 0));
                func.merge(destValue, srcValue);
                Assert.assertEquals("lastFrameId sentinel", -1, destValue.getLong(6));
                // Appending after a merge violates the no-append-after-merge
                // contract; computeNext asserts against it (CI runs with -ea)
                // rather than silently reallocating the tight descriptor buffer.
                AssertionError e = Assert.assertThrows(
                        AssertionError.class,
                        () -> func.computeNext(destValue, null, rowId(2, 0))
                );
                TestUtils.assertContains(e.getMessage(), "post-merge MapValue");
            }
        });
    }

    @Test
    public void testConsecutiveFramesStaySingleBatch() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration);
                    SimpleMapValue mapValue = new SimpleMapValue(7)
            ) {
                SparklineGroupByFunction func = sparklineFunction(allocator);
                func.computeFirst(mapValue, null, rowId(0, 0));
                func.computeNext(mapValue, null, rowId(0, 1));
                func.computeNext(mapValue, null, rowId(1, 0));
                func.computeNext(mapValue, null, rowId(2, 0));
                func.computeNext(mapValue, null, rowId(3, 0));
                // Frames 0..3 arrive in order: one run, descriptor buffer never allocated.
                Assert.assertEquals("entry count", 5, mapValue.getLong(1));
                Assert.assertEquals("descriptor pointer", 0, mapValue.getLong(3));
            }
        });
    }

    @Test
    public void testFrameGapRecordsDescriptor() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration);
                    SimpleMapValue mapValue = new SimpleMapValue(7)
            ) {
                SparklineGroupByFunction func = sparklineFunction(allocator);
                func.computeFirst(mapValue, null, rowId(0, 0));
                func.computeNext(mapValue, null, rowId(1, 0)); // consecutive: stays in the batch
                func.computeNext(mapValue, null, rowId(3, 0)); // frame 2 skipped: new batch
                Assert.assertNotEquals("descriptor pointer", 0, mapValue.getLong(3));
                Assert.assertEquals("descriptor count", 2, mapValue.getLong(4));
            }
        });
    }

    @Test
    public void testNullPrefixRecoveryReinitializesLastFrameId() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration);
                    SimpleMapValue mapValue = new SimpleMapValue(7)
            ) {
                // computeFirst sees NULL and calls setNull(), which zeroes lastFrameId.
                SparklineGroupByFunction nullFunc = sparklineFunction(allocator, DoubleConstant.NULL);
                nullFunc.computeFirst(mapValue, null, rowId(5, 0));

                // The first valid observation arrives via computeNext on a non-zero
                // frame: the recovery branch must initialise lastFrameId to the
                // current frame, not leave it at setNull's starting value of 0.
                SparklineGroupByFunction func = sparklineFunction(allocator);
                func.computeNext(mapValue, null, rowId(5, 1));
                Assert.assertEquals("entry count", 1, mapValue.getLong(1));
                Assert.assertEquals("lastFrameId", 5, mapValue.getLong(6));
            }
        });
    }

    @Test
    public void testOutOfOrderFrameRecordsDescriptor() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration);
                    SimpleMapValue mapValue = new SimpleMapValue(7)
            ) {
                SparklineGroupByFunction func = sparklineFunction(allocator);
                func.computeFirst(mapValue, null, rowId(5, 0));
                func.computeNext(mapValue, null, rowId(2, 0)); // earlier frame: new batch
                Assert.assertNotEquals("descriptor pointer", 0, mapValue.getLong(3));
                Assert.assertEquals("descriptor count", 2, mapValue.getLong(4));
            }
        });
    }

    // Builds a rowId the way the page-frame cursor does: the frame index in the
    // high bits, the frame-local row id in the low 44 bits.
    private static long rowId(int frameIndex, long localRowId) {
        return ((long) frameIndex << 44) + localRowId;
    }

    private static SparklineGroupByFunction sparklineFunction(GroupByAllocator allocator) {
        return sparklineFunction(allocator, new DoubleConstant(1.0));
    }

    private static SparklineGroupByFunction sparklineFunction(GroupByAllocator allocator, Function arg) {
        // The chars array is only consulted at render time, which this test
        // never reaches, so a dummy palette is fine.
        SparklineGroupByFunction func = new SparklineGroupByFunction(
                "sparkline", new char[]{'.', '#'}, arg,
                null, null, null, 0, 0, 4096
        );
        func.initValueIndex(0);
        func.setAllocator(allocator);
        return func;
    }
}
