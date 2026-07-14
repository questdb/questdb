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

package io.questdb.test.griffin.model;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.model.RuntimeIntervalModelBuilder;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Covers the ownership contract of the builder's Function-taking operations: the caller
 * (WhereClauseParser) hands over the Function and keeps no reference to it, so every path -
 * including the early returns that apply nothing - must either store the Function or close it.
 */
public class RuntimeIntervalModelBuilderTest extends AbstractCairoTest {
    private static final long ALLOC_SIZE = 32;

    @Test
    public void testBetweenBoundariesFreedOnEmptySet() throws Exception {
        assertMemoryLeak(() -> {
            // both boundaries dynamic
            RuntimeIntervalModelBuilder builder = newEmptyBuilder();
            AllocatingFunction lo = new AllocatingFunction();
            AllocatingFunction hi = new AllocatingFunction();
            builder.setBetweenBoundary(lo);
            builder.setBetweenBoundary(hi);
            Assert.assertEquals(1, lo.closeCount);
            Assert.assertEquals(1, hi.closeCount);
            builder.clear();
            Assert.assertEquals(1, lo.closeCount);
            Assert.assertEquals(1, hi.closeCount);

            // dynamic lo, constant hi
            builder = newEmptyBuilder();
            AllocatingFunction dynamicLo = new AllocatingFunction();
            builder.setBetweenBoundary(dynamicLo);
            builder.setBetweenBoundary(1_000L);
            Assert.assertEquals(1, dynamicLo.closeCount);
            builder.clear();
            Assert.assertEquals(1, dynamicLo.closeCount);

            // constant lo, dynamic hi
            builder = newEmptyBuilder();
            AllocatingFunction dynamicHi = new AllocatingFunction();
            builder.setBetweenBoundary(1_000L);
            builder.setBetweenBoundary(dynamicHi);
            Assert.assertEquals(1, dynamicHi.closeCount);
            builder.clear();
            Assert.assertEquals(1, dynamicHi.closeCount);
        });
    }

    @Test
    public void testBetweenBoundaryFreedOnNullConstantBoundary() throws Exception {
        // BETWEEN NULL matches nothing and NOT BETWEEN NULL filters nothing: neither keeps the
        // dynamic boundary, and the model is not an empty set when the NULL boundary arrives.
        assertMemoryLeak(() -> {
            for (boolean negated : new boolean[]{false, true}) {
                RuntimeIntervalModelBuilder builder = newBuilder();
                builder.setBetweenNegated(negated);
                AllocatingFunction lo = new AllocatingFunction();
                builder.setBetweenBoundary(lo);
                builder.setBetweenBoundary(Numbers.LONG_NULL);
                Assert.assertEquals(1, lo.closeCount);
                builder.clear();
                Assert.assertEquals(1, lo.closeCount);
            }
        });
    }

    @Test
    public void testBetweenBoundaryFreedWhenUnpaired() throws Exception {
        // The hi boundary never arrives (it threw, or BETWEEN stayed a residual filter), so the
        // parked lo boundary is still the builder's to free.
        assertMemoryLeak(() -> {
            RuntimeIntervalModelBuilder builder = newBuilder();
            AllocatingFunction lo = new AllocatingFunction();
            builder.setBetweenBoundary(lo);
            builder.clearBetweenParsing();
            Assert.assertEquals(1, lo.closeCount);
            builder.clear();
            Assert.assertEquals(1, lo.closeCount);
        });
    }

    @Test
    public void testBetweenBoundaryNotFreedTwiceWhenPaired() throws Exception {
        // The paired boundaries move into the dynamic range list; clear() must free them through it
        // exactly once, and not a second time as a parked boundary.
        assertMemoryLeak(() -> {
            RuntimeIntervalModelBuilder builder = newBuilder();
            AllocatingFunction lo = new AllocatingFunction();
            AllocatingFunction hi = new AllocatingFunction();
            builder.setBetweenBoundary(lo);
            builder.setBetweenBoundary(hi);
            Assert.assertEquals(0, lo.closeCount);
            Assert.assertEquals(0, hi.closeCount);
            builder.clear();
            Assert.assertEquals(1, lo.closeCount);
            Assert.assertEquals(1, hi.closeCount);
        });
    }

    @Test
    public void testFunctionsFreedOnEmptySet() throws Exception {
        // Every Function-taking operation, applied to a model an earlier conjunct already emptied
        // (e.g. "ts > '2021' AND ts < '2020' AND ts = now()"). The operation applies nothing, and
        // the caller has already let go of the Function.
        assertMemoryLeak(() -> {
            assertFreedOnEmptySet((builder, func) -> builder.intersect(0, func, (short) 0));
            assertFreedOnEmptySet((builder, func) -> builder.intersect(func, 0, (short) 0));
            assertFreedOnEmptySet(RuntimeIntervalModelBuilder::intersectRuntimeIntervals);
            assertFreedOnEmptySet(RuntimeIntervalModelBuilder::intersectRuntimeTimestamp);
            assertFreedOnEmptySet(RuntimeIntervalModelBuilder::subtractEquals);
            assertFreedOnEmptySet(RuntimeIntervalModelBuilder::subtractRuntimeIntervals);
            assertFreedOnEmptySet(RuntimeIntervalModelBuilder::unionRuntimeTimestamp);
        });
    }

    @Test
    public void testFunctionsNotFreedTwiceWhenApplied() throws Exception {
        // The mirror of testFunctionsFreedOnEmptySet: a non-empty model stores the Function, and
        // the built model - not the builder - owns it from then on.
        assertMemoryLeak(() -> {
            RuntimeIntervalModelBuilder builder = newBuilder();
            AllocatingFunction func = new AllocatingFunction();
            builder.subtractEquals(func);
            Assert.assertEquals(0, func.closeCount);
            RuntimeIntrinsicIntervalModel model = builder.build();
            // build() transferred ownership: clear() must leave the Function alone
            builder.clear();
            Assert.assertEquals(0, func.closeCount);
            model.close();
            Assert.assertEquals(1, func.closeCount);
        });
    }

    private static void assertFreedOnEmptySet(EmptySetOperation operation) {
        RuntimeIntervalModelBuilder builder = newEmptyBuilder();
        AllocatingFunction func = new AllocatingFunction();
        operation.apply(builder, func);
        Assert.assertEquals(1, func.closeCount);
        // the builder must not close it a second time on clear()
        builder.clear();
        Assert.assertEquals(1, func.closeCount);
    }

    private static RuntimeIntervalModelBuilder newBuilder() {
        RuntimeIntervalModelBuilder builder = new RuntimeIntervalModelBuilder();
        builder.of(ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY, configuration);
        return builder;
    }

    private static RuntimeIntervalModelBuilder newEmptyBuilder() {
        RuntimeIntervalModelBuilder builder = newBuilder();
        builder.intersectEmpty();
        Assert.assertTrue(builder.isEmptySet());
        return builder;
    }

    @FunctionalInterface
    private interface EmptySetOperation {
        void apply(RuntimeIntervalModelBuilder builder, AllocatingFunction func);
    }

    /**
     * Tracks its own native allocation, so a Function the builder orphans fails assertMemoryLeak,
     * and counts close() calls, so one it frees twice fails on the count.
     */
    private static class AllocatingFunction extends TimestampFunction {
        private int closeCount;
        private long ptr;

        private AllocatingFunction() {
            super(ColumnType.TIMESTAMP_MICRO);
            this.ptr = Unsafe.malloc(ALLOC_SIZE, MemoryTag.NATIVE_DEFAULT);
        }

        @Override
        public void close() {
            closeCount++;
            if (ptr != 0) {
                ptr = Unsafe.free(ptr, ALLOC_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        }

        @Override
        public long getTimestamp(Record rec) {
            return 0;
        }

        @Override
        public boolean isRuntimeConstant() {
            return true;
        }
    }
}
