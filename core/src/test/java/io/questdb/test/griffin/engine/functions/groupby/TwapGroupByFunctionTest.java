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

import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.groupby.TwapGroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.test.AbstractCairoTest;
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
