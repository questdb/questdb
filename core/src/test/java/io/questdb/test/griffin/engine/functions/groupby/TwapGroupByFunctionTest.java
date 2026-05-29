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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.groupby.TwapGroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * White-box tests for the per-frame batch tracking in
 * {@link TwapGroupByFunction#computeNext}. The MapValue layout is
 * {@code [ptr, count, capacity, descPtr, descCount, descCapacity, lastFrameId]}.
 * The descriptor buffer ({@code descPtr}, slot 3) stays unallocated while frames
 * arrive as a single consecutive, in-order run; a batch boundary is recorded
 * only at a gap or an out-of-order frame.
 */
public class TwapGroupByFunctionTest extends AbstractCairoTest {

    @Test
    public void testComputeNextAfterMergeTripsAssertion() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration);
                    SimpleMapValue destValue = new SimpleMapValue(7);
                    SimpleMapValue srcValue = new SimpleMapValue(7)
            ) {
                TwapGroupByFunction func = twapFunction(allocator);
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
                TwapGroupByFunction func = twapFunction(allocator);
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
                TwapGroupByFunction func = twapFunction(allocator);
                func.computeFirst(mapValue, null, rowId(0, 0));
                func.computeNext(mapValue, null, rowId(1, 0)); // consecutive: stays in the batch
                func.computeNext(mapValue, null, rowId(3, 0)); // frame 2 skipped: new batch
                Assert.assertNotEquals("descriptor pointer", 0, mapValue.getLong(3));
                Assert.assertEquals("descriptor count", 2, mapValue.getLong(4));
            }
        });
    }

    @Test
    public void testGetDoubleRejectsOutOfOrderTimestampsInBatch() throws Exception {
        // Runtime backstop. The compile-time guards should keep out-of-order bases away from
        // twap, but if a factory ever slips descending timestamps into a single batch (same
        // frame, so compactInPlace cannot reorder them), getDouble must refuse to integrate
        // garbage and surface the contract violation instead of silently returning the plain
        // average via the totalDuration <= 0 fallback.
        assertMemoryLeak(() -> {
            try (
                    GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration);
                    SimpleMapValue mapValue = new SimpleMapValue(7)
            ) {
                // First observation ts = 100, second ts = 50: descending within one frame.
                LongFunction descendingTs = new LongFunction() {
                    private int i = 0;

                    @Override
                    public long getLong(Record rec) {
                        return i++ == 0 ? 100L : 50L;
                    }
                };
                TwapGroupByFunction func = new TwapGroupByFunction(new DoubleConstant(1.0), descendingTs);
                func.initValueIndex(0);
                func.setAllocator(allocator);
                func.computeFirst(mapValue, null, rowId(0, 0));
                func.computeNext(mapValue, null, rowId(0, 1)); // same frame: single batch, not reorderable
                Assert.assertEquals("single batch", 0, mapValue.getLong(3));

                // getDouble reads the MapValue slots through a Record; only getLong is needed.
                Record view = new Record() {
                    @Override
                    public long getLong(int col) {
                        return mapValue.getLong(col);
                    }
                };
                CairoException e = Assert.assertThrows(CairoException.class, () -> func.getDouble(view));
                TestUtils.assertContains(e.getFlyweightMessage(), "twap() requires the base query to provide ascending designated timestamp order");
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
                TwapGroupByFunction nullFunc = new TwapGroupByFunction(DoubleConstant.NULL, LongConstant.NULL);
                nullFunc.initValueIndex(0);
                nullFunc.setAllocator(allocator);
                nullFunc.computeFirst(mapValue, null, rowId(5, 0));

                // The first valid observation arrives via computeNext on a non-zero
                // frame: the recovery branch must initialise lastFrameId to the
                // current frame, not leave it at setNull's starting value of 0.
                TwapGroupByFunction func = twapFunction(allocator);
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
                TwapGroupByFunction func = twapFunction(allocator);
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

    private static TwapGroupByFunction twapFunction(GroupByAllocator allocator) {
        TwapGroupByFunction func = new TwapGroupByFunction(new DoubleConstant(1.0), new LongConstant(1_000));
        func.initValueIndex(0);
        func.setAllocator(allocator);
        return func;
    }
}
