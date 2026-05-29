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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.groupby.SparklineGroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * White-box tests for {@link SparklineGroupByFunction}'s per-frame batch
 * tracking. The MapValue layout is
 * {@code [ptr, count, capacity, descPtr, descCount, descCapacity, lastFrameId]}.
 * {@code merge} stamps {@code lastFrameId} with a negative sentinel so a later
 * {@code computeNext} on the merged value trips an assertion (CI runs with -ea)
 * instead of silently reallocating the exactly-sized descriptor buffer.
 */
public class SparklineGroupByFunctionTest extends AbstractCairoTest {

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

    // Builds a rowId the way the page-frame cursor does: the frame index in the
    // high bits, the frame-local row id in the low 44 bits.
    private static long rowId(int frameIndex, long localRowId) {
        return ((long) frameIndex << 44) + localRowId;
    }

    private static SparklineGroupByFunction sparklineFunction(GroupByAllocator allocator) {
        // The chars array is only consulted at render time, which this test
        // never reaches, so a dummy palette is fine.
        SparklineGroupByFunction func = new SparklineGroupByFunction(
                "sparkline", new char[]{'.', '#'}, new DoubleConstant(1.0),
                null, null, null, 0, 0, 4096
        );
        func.initValueIndex(0);
        func.setAllocator(allocator);
        return func;
    }
}
