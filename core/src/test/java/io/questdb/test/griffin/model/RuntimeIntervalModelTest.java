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

package io.questdb.test.griffin.model;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.bind.IndexedParameterLinkFunction;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.model.IntervalDynamicIndicator;
import io.questdb.griffin.model.IntervalOperation;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.griffin.model.RuntimeIntervalModel;
import io.questdb.griffin.model.RuntimeIntervalModelBuilder;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Covers sparse cursor-position handling in {@link RuntimeIntervalModel}. Models without a cursor
 * position list (or with a shorter one) must evaluate dynamic intervals normally and fall back to
 * position 0. Cursor entries carry their positions in cursor encounter order.
 */
public class RuntimeIntervalModelTest extends AbstractCairoTest {
    private static final long DAY_MICROS = 86_400_000_000L;
    private static final long DAY_NANOS = 86_400_000_000_000L;

    @Test
    public void testAllIntervalsHitOnePartitionCorrectnessMatrix() {
        assertOnePartition(ColumnType.TIMESTAMP, new LongList(), true);
        assertOnePartition(ColumnType.TIMESTAMP, new LongList(new long[]{1_000, 1_000}), true);
        assertOnePartition(ColumnType.TIMESTAMP, new LongList(new long[]{1_000, 2_000, 4_000, 5_000}), true);
        assertOnePartition(ColumnType.TIMESTAMP, new LongList(new long[]{1_000, 2_000, DAY_MICROS + 1, DAY_MICROS + 2}), false);
        assertOnePartition(ColumnType.TIMESTAMP, new LongList(new long[]{DAY_MICROS - 1, DAY_MICROS - 1}), true);
        assertOnePartition(ColumnType.TIMESTAMP, new LongList(new long[]{DAY_MICROS - 1, DAY_MICROS}), false);
        assertOnePartition(ColumnType.TIMESTAMP, new LongList(new long[]{-2_000, -1_000, 0, 1_000}), true);
        assertOnePartition(ColumnType.TIMESTAMP, new LongList(new long[]{Long.MIN_VALUE, Long.MAX_VALUE}), false);
        assertOnePartition(ColumnType.TIMESTAMP_NANO, new LongList(new long[]{DAY_NANOS - 1, DAY_NANOS}), false);
        assertOnePartition(
                ColumnType.TIMESTAMP,
                new LongList(new long[]{Long.MAX_VALUE - 3 * DAY_MICROS, Long.MAX_VALUE - 3 * DAY_MICROS + 1}),
                false,
                PartitionBy.WEEK
        );
        assertOnePartition(
                ColumnType.TIMESTAMP_NANO,
                new LongList(new long[]{Long.MAX_VALUE - 3 * DAY_NANOS, Long.MAX_VALUE - 3 * DAY_NANOS + 1}),
                false,
                PartitionBy.WEEK
        );
    }

    @Test
    public void testAllIntervalsHitOnePartitionDynamicModelsStayConservative() throws Exception {
        assertMemoryLeak(() -> {
            assertDynamicModel(ColumnType.TIMESTAMP, 1_000, 2_000);
            assertDynamicModel(ColumnType.TIMESTAMP, 1_000, DAY_MICROS + 1);
            assertDynamicModel(ColumnType.TIMESTAMP_NANO, 1_000, 2_000);
            assertDynamicModel(ColumnType.TIMESTAMP_NANO, 1_000, DAY_NANOS + 1);
        });
    }

    @Test
    public void testAllIntervalsHitOnePartitionUsesConstantFloorCalls() {
        final CountingTimestampDriver driver = new CountingTimestampDriver();
        final RuntimeIntervalModel model = new RuntimeIntervalModel(
                driver.driver,
                PartitionBy.DAY,
                new LongList(new long[]{1_000, 2_000, 4_000, 5_000, 7_000, 8_000, 10_000, 11_000})
        );
        Assert.assertTrue(model.allIntervalsHitOnePartition());
        Assert.assertEquals(2, driver.floorCallCount);
    }

    @Test
    public void testCloseAttemptsEveryFunctionAndPreservesFirstFailure() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException firstFailure = new RuntimeException("first");
            final RuntimeException laterFailure = new RuntimeException("later");
            final CloseCountingFunction first = new CloseCountingFunction(firstFailure);
            final CloseCountingFunction second = new CloseCountingFunction(null);
            final CloseCountingFunction third = new CloseCountingFunction(laterFailure);
            final CloseCountingFunction fourth = new CloseCountingFunction(null);
            final ObjList<Function> dynamicFunctions = new ObjList<>();
            dynamicFunctions.add(first);
            dynamicFunctions.add(second);
            dynamicFunctions.add(null);
            dynamicFunctions.add(third);
            dynamicFunctions.add(fourth);
            final RuntimeIntervalModel model = new RuntimeIntervalModel(
                    ColumnType.getTimestampDriver(ColumnType.TIMESTAMP),
                    PartitionBy.DAY,
                    new LongList(),
                    dynamicFunctions
            );

            try {
                model.close();
                Assert.fail("close must propagate the first failure");
            } catch (RuntimeException e) {
                Assert.assertSame(firstFailure, e);
                Assert.assertArrayEquals(new Throwable[]{laterFailure}, e.getSuppressed());
            }

            Assert.assertEquals(1, first.closeCount);
            Assert.assertEquals(1, second.closeCount);
            Assert.assertEquals(1, third.closeCount);
            Assert.assertEquals(1, fourth.closeCount);
            for (int i = 0, n = dynamicFunctions.size(); i < n; i++) {
                Assert.assertNull(dynamicFunctions.getQuick(i));
            }

            model.close();
            Assert.assertEquals(1, first.closeCount);
            Assert.assertEquals(1, second.closeCount);
            Assert.assertEquals(1, third.closeCount);
            Assert.assertEquals(1, fourth.closeCount);
            Assert.assertArrayEquals(new Throwable[]{laterFailure}, firstFailure.getSuppressed());
        });
    }

    @Test
    public void testDeterminismClassificationSkipsStaticPlaceholder() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeIntervalModelBuilder builder = new RuntimeIntervalModelBuilder();
            builder.of(ColumnType.TIMESTAMP, PartitionBy.DAY, configuration);
            builder.intersectRuntimeTimestamp(
                    TimestampConstant.newInstance(5_000L, ColumnType.TIMESTAMP),
                    0
            );
            builder.intersect(1_000L, 10_000L);
            try (RuntimeIntervalModel model = (RuntimeIntervalModel) builder.build()) {
                builder.clear();
                Assert.assertEquals(2, model.getDynamicRangeList().size());
                Assert.assertNull(model.getDynamicRangeList().getQuick(1));
                Assert.assertFalse(model.isNonDeterministic());
                Assert.assertTrue(model.isStableWithinExecution());
            }

            assertRuntimeStabilityClassification(true);
            assertRuntimeStabilityClassification(false);
        });
    }

    @Test
    public void testDynamicIntervalWithNullPositionList() throws Exception {
        assertMemoryLeak(() -> {
            final LongList intervals = new LongList();
            IntervalUtils.encodeInterval(
                    1_000L,
                    0,
                    (short) 0,
                    IntervalDynamicIndicator.IS_HI_DYNAMIC,
                    IntervalOperation.INTERSECT,
                    intervals
            );
            final ObjList<Function> dynamicFunctions = new ObjList<>();
            dynamicFunctions.add(TimestampConstant.newInstance(5_000L, ColumnType.TIMESTAMP));
            // legacy constructor: no position list at all
            try (RuntimeIntervalModel model = new RuntimeIntervalModel(
                    ColumnType.getTimestampDriver(ColumnType.TIMESTAMP),
                    PartitionBy.DAY,
                    intervals,
                    dynamicFunctions
            )) {
                final LongList out = model.calculateIntervals(sqlExecutionContext);
                Assert.assertEquals(2, out.size());
                Assert.assertEquals(1_000L, out.getQuick(0));
                Assert.assertEquals(5_000L, out.getQuick(1));
            }
        });
    }

    @Test
    public void testDynamicIntervalWithShortPositionList() throws Exception {
        assertMemoryLeak(() -> {
            final LongList intervals = new LongList();
            IntervalUtils.encodeInterval(
                    2_000L,
                    0,
                    (short) 0,
                    IntervalDynamicIndicator.IS_HI_DYNAMIC,
                    IntervalOperation.INTERSECT,
                    intervals
            );
            final ObjList<Function> dynamicFunctions = new ObjList<>();
            dynamicFunctions.add(TimestampConstant.newInstance(7_000L, ColumnType.TIMESTAMP));
            // No cursor positions are needed for a timestamp function.
            try (RuntimeIntervalModel model = new RuntimeIntervalModel(
                    ColumnType.getTimestampDriver(ColumnType.TIMESTAMP),
                    PartitionBy.DAY,
                    intervals,
                    dynamicFunctions,
                    new IntList()
            )) {
                final LongList out = model.calculateIntervals(sqlExecutionContext);
                Assert.assertEquals(2, out.size());
                Assert.assertEquals(2_000L, out.getQuick(0));
                Assert.assertEquals(7_000L, out.getQuick(1));
            }
        });
    }

    @Test
    public void testDynamicTypeLookedUpOncePerEndpointAndExecution() throws Exception {
        assertMemoryLeak(() -> {
            final CountingTypeFunction timestampFunction = new CountingTypeFunction(ColumnType.TIMESTAMP, 5_000L, null);
            assertTypeLookups(timestampFunction, 2);
            Assert.assertEquals(1, timestampFunction.typeCallCount);

            final CountingTypeFunction nullStringFunction = new CountingTypeFunction(ColumnType.STRING, 0, null);
            assertTypeLookups(nullStringFunction, 0);
            Assert.assertEquals(1, nullStringFunction.typeCallCount);
        });
    }

    @Test
    public void testMultiRowSubQueryErrorFallsBackToPositionZeroWithNullPositionList() throws Exception {
        assertMemoryLeak(() -> {
            createTwoRowTable();
            assertMultiRowSubQueryError(null, 0);
        });
    }

    @Test
    public void testMultiRowSubQueryErrorFallsBackToPositionZeroWithShortPositionList() throws Exception {
        assertMemoryLeak(() -> {
            createTwoRowTable();
            assertMultiRowSubQueryError(new IntList(), 0);
        });
    }

    @Test
    public void testMultiRowSubQueryErrorUsesCursorPositionAfterNonCursorFunction() throws Exception {
        assertMemoryLeak(() -> {
            createTwoRowTable();
            final ObjList<Function> dynamicFunctions = new ObjList<>();
            dynamicFunctions.add(TimestampConstant.newInstance(2_000L, ColumnType.TIMESTAMP));
            dynamicFunctions.add(new CursorFunction(select("SELECT ts FROM tab")));
            final IntList positions = new IntList();
            positions.add(42);
            assertMultiRowSubQueryError(dynamicFunctions, positions, 42);
        });
    }

    @Test
    public void testMultiRowSubQueryErrorUsesSecondCursorPosition() throws Exception {
        assertMemoryLeak(() -> {
            createTwoRowTable();
            final RuntimeIntervalModelBuilder builder = new RuntimeIntervalModelBuilder();
            builder.of(ColumnType.TIMESTAMP, PartitionBy.DAY, configuration);
            // The builder stores the incoming BETWEEN boundary before the pending one. Supplying
            // the multi-row cursor first therefore makes it the second cursor evaluated by the
            // runtime model, while preserving its distinct parse position.
            builder.setBetweenBoundary(new CursorFunction(select("SELECT ts FROM tab")), 42);
            builder.setBetweenBoundary(new CursorFunction(select("SELECT 2_000::timestamp")), 17);
            builder.clearBetweenParsing();
            final RuntimeIntrinsicIntervalModel model = builder.build();
            builder.clear();
            assertModelMultiRowSubQueryError(model, 42);
        });
    }

    @Test
    public void testMultiRowSubQueryErrorUsesSparseCursorPosition() throws Exception {
        assertMemoryLeak(() -> {
            createTwoRowTable();
            final IntList positions = new IntList();
            positions.add(42);
            assertMultiRowSubQueryError(positions, 42);
        });
    }

    @Test
    public void testUnionLeafRunAllNullLeavesFollowingIntersectFirstApplied() throws Exception {
        assertMemoryLeak(() -> {
            final LongList intervals = new LongList();
            final ObjList<Function> dynamicFunctions = new ObjList<>();
            // every union leaf is NULL: the run contributes nothing and the intersect leaf
            // must become the first applied interval, exactly as with per-leaf merging
            for (int i = 0; i < 3; i++) {
                encodeUnionPointLeaf(intervals);
                dynamicFunctions.add(TimestampConstant.TIMESTAMP_MICRO_NULL);
            }
            IntervalUtils.encodeInterval(
                    1_500L,
                    0,
                    (short) 0,
                    IntervalDynamicIndicator.IS_HI_DYNAMIC,
                    IntervalOperation.INTERSECT,
                    intervals
            );
            dynamicFunctions.add(TimestampConstant.newInstance(6_000L, ColumnType.TIMESTAMP));
            try (RuntimeIntervalModel model = new RuntimeIntervalModel(
                    ColumnType.getTimestampDriver(ColumnType.TIMESTAMP),
                    PartitionBy.DAY,
                    intervals,
                    dynamicFunctions
            )) {
                assertIntervals(model.calculateIntervals(sqlExecutionContext), 1_500L, 6_000L);
            }
        });
    }

    @Test
    public void testUnionLeafRunFoldsBeforeFollowingIntersect() throws Exception {
        assertMemoryLeak(() -> {
            final LongList intervals = new LongList();
            final ObjList<Function> dynamicFunctions = new ObjList<>();
            for (long value : new long[]{8_000L, 2_000L, 5_000L, 2_000L}) {
                encodeUnionPointLeaf(intervals);
                dynamicFunctions.add(TimestampConstant.newInstance(value, ColumnType.TIMESTAMP));
            }
            // the trailing intersect leaf must see the union run already folded into one
            // sorted, coalesced accumulator
            IntervalUtils.encodeInterval(
                    1_500L,
                    0,
                    (short) 0,
                    IntervalDynamicIndicator.IS_HI_DYNAMIC,
                    IntervalOperation.INTERSECT,
                    intervals
            );
            dynamicFunctions.add(TimestampConstant.newInstance(6_000L, ColumnType.TIMESTAMP));
            try (RuntimeIntervalModel model = new RuntimeIntervalModel(
                    ColumnType.getTimestampDriver(ColumnType.TIMESTAMP),
                    PartitionBy.DAY,
                    intervals,
                    dynamicFunctions
            )) {
                assertIntervals(
                        model.calculateIntervals(sqlExecutionContext),
                        2_000L, 2_000L, 5_000L, 5_000L
                );
            }
        });
    }

    @Test
    public void testUnionLeafRunsMergeOnceAndMatchIncrementalSemantics() throws Exception {
        assertMemoryLeak(() -> {
            // shuffled OR-ed point leaves with duplicates, adjacent values and a NULL: the NULL
            // is the empty-set identity, duplicates collapse, and adjacent points stay separate
            // intervals - exactly what per-leaf incremental unionInPlace used to produce
            final long[] values = {5_000L, 1_000L, 5_000L, 3_000L, 1_001L, 9_000L};
            final LongList intervals = new LongList();
            final ObjList<Function> dynamicFunctions = new ObjList<>();
            for (long value : values) {
                encodeUnionPointLeaf(intervals);
                dynamicFunctions.add(TimestampConstant.newInstance(value, ColumnType.TIMESTAMP));
            }
            encodeUnionPointLeaf(intervals);
            dynamicFunctions.add(TimestampConstant.TIMESTAMP_MICRO_NULL);
            try (RuntimeIntervalModel model = new RuntimeIntervalModel(
                    ColumnType.getTimestampDriver(ColumnType.TIMESTAMP),
                    PartitionBy.DAY,
                    intervals,
                    dynamicFunctions
            )) {
                assertIntervals(
                        model.calculateIntervals(sqlExecutionContext),
                        1_000L, 1_000L, 1_001L, 1_001L, 3_000L, 3_000L, 5_000L, 5_000L, 9_000L, 9_000L
                );
            }
        });
    }

    private static void assertIntervals(LongList actual, long... expectedLoHiPairs) {
        Assert.assertEquals("interval count", expectedLoHiPairs.length, actual.size());
        for (int i = 0; i < expectedLoHiPairs.length; i++) {
            Assert.assertEquals("value at index " + i, expectedLoHiPairs[i], actual.getQuick(i));
        }
    }

    private static void encodeUnionPointLeaf(LongList intervals) {
        // mirrors RuntimeIntervalModelBuilder.unionRuntimeTimestamp encoding of an OR-ed
        // scalar timestamp disjunct
        IntervalUtils.encodeInterval(
                0,
                0,
                (short) 0,
                IntervalDynamicIndicator.IS_LO_HI_DYNAMIC,
                IntervalOperation.UNION,
                intervals
        );
    }

    private static void assertDynamicModel(int timestampType, long lo, long hi) throws SqlException {
        final LongList intervals = new LongList();
        IntervalUtils.encodeInterval(
                lo,
                0,
                (short) 0,
                IntervalDynamicIndicator.IS_HI_DYNAMIC,
                IntervalOperation.INTERSECT,
                intervals
        );
        final ObjList<Function> dynamicFunctions = new ObjList<>();
        dynamicFunctions.add(TimestampConstant.newInstance(hi, timestampType));
        try (RuntimeIntervalModel model = new RuntimeIntervalModel(
                ColumnType.getTimestampDriver(timestampType),
                PartitionBy.DAY,
                intervals,
                dynamicFunctions
        )) {
            final LongList calculated = model.calculateIntervals(sqlExecutionContext);
            Assert.assertEquals(lo, calculated.getQuick(0));
            Assert.assertEquals(hi, calculated.getQuick(1));
            Assert.assertFalse(model.allIntervalsHitOnePartition());
        }
    }

    private static void assertRuntimeStabilityClassification(boolean isStableWithinExecution) {
        final ObjList<Function> dynamicFunctions = new ObjList<>();
        dynamicFunctions.add(new StabilityFunction(isStableWithinExecution));
        dynamicFunctions.add(null);
        try (RuntimeIntervalModel model = new RuntimeIntervalModel(
                ColumnType.getTimestampDriver(ColumnType.TIMESTAMP),
                PartitionBy.DAY,
                new LongList(),
                dynamicFunctions
        )) {
            Assert.assertTrue(model.isNonDeterministic());
            Assert.assertEquals(isStableWithinExecution, model.isStableWithinExecution());
        }
    }

    private static void assertOnePartition(int timestampType, LongList intervals, boolean expected) {
        assertOnePartition(timestampType, intervals, expected, PartitionBy.DAY);
    }

    private static void assertOnePartition(int timestampType, LongList intervals, boolean expected, int partitionBy) {
        final RuntimeIntervalModel model = new RuntimeIntervalModel(
                ColumnType.getTimestampDriver(timestampType),
                partitionBy,
                intervals
        );
        Assert.assertEquals(expected, model.allIntervalsHitOnePartition());
    }

    private void assertModelMultiRowSubQueryError(RuntimeIntrinsicIntervalModel model, int expectedPosition) {
        try (model) {
            model.calculateIntervals(sqlExecutionContext);
            Assert.fail("multi-row scalar sub-query must be rejected");
        } catch (SqlException e) {
            Assert.assertEquals(
                    "error must use the sparse cursor position, or 0 when it is unavailable",
                    expectedPosition,
                    e.getPosition()
            );
            TestUtils.assertContains(e.getFlyweightMessage(), "scalar sub-query returned more than one row");
        }
    }

    private void assertMultiRowSubQueryError(IntList positions, int expectedPosition) throws Exception {
        final ObjList<Function> dynamicFunctions = new ObjList<>();
        dynamicFunctions.add(new CursorFunction(select("SELECT ts FROM tab")));
        assertMultiRowSubQueryError(dynamicFunctions, positions, expectedPosition);
    }

    private void assertMultiRowSubQueryError(
            ObjList<Function> dynamicFunctions,
            IntList positions,
            int expectedPosition
    ) {
        final LongList intervals = new LongList();
        for (int i = 0, n = dynamicFunctions.size(); i < n; i++) {
            IntervalUtils.encodeInterval(
                    1_000L,
                    0,
                    (short) 0,
                    IntervalDynamicIndicator.IS_HI_DYNAMIC,
                    IntervalOperation.INTERSECT,
                    intervals
            );
        }
        assertModelMultiRowSubQueryError(new RuntimeIntervalModel(
                ColumnType.getTimestampDriver(ColumnType.TIMESTAMP),
                PartitionBy.DAY,
                intervals,
                dynamicFunctions,
                positions
        ), expectedPosition);
    }

    private void assertTypeLookups(CountingTypeFunction function, int expectedIntervalCount) throws SqlException {
        final LongList intervals = new LongList();
        IntervalUtils.encodeInterval(
                1_000L,
                0,
                (short) 0,
                IntervalDynamicIndicator.IS_HI_DYNAMIC,
                IntervalOperation.INTERSECT,
                intervals
        );
        final ObjList<Function> dynamicFunctions = new ObjList<>();
        dynamicFunctions.add(function);
        try (RuntimeIntervalModel model = new RuntimeIntervalModel(
                ColumnType.getTimestampDriver(ColumnType.TIMESTAMP),
                PartitionBy.DAY,
                intervals,
                dynamicFunctions
        )) {
            Assert.assertEquals(expectedIntervalCount, model.calculateIntervals(sqlExecutionContext).size());
        }
    }

    private void createTwoRowTable() throws SqlException {
        execute("create table tab as (" +
                "select timestamp_sequence(0, 1000) ts from long_sequence(2)" +
                ") timestamp(ts)");
    }

    private static class CloseCountingFunction extends TimestampFunction {
        private final RuntimeException failure;
        private int closeCount;

        private CloseCountingFunction(RuntimeException failure) {
            super(ColumnType.TIMESTAMP);
            this.failure = failure;
        }

        @Override
        public void close() {
            closeCount++;
            if (failure != null) {
                throw failure;
            }
        }

        @Override
        public long getTimestamp(Record rec) {
            return 0;
        }
    }

    private static class CountingTimestampDriver {
        private final TimestampDriver driver;
        private int floorCallCount;

        private CountingTimestampDriver() {
            final TimestampDriver delegate = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
            driver = (TimestampDriver) java.lang.reflect.Proxy.newProxyInstance(
                    TimestampDriver.class.getClassLoader(),
                    new Class[]{TimestampDriver.class},
                    (_, method, args) -> {
                        final Object result = method.invoke(delegate, args);
                        if (method.getName().equals("getPartitionFloorMethod")) {
                            final TimestampDriver.TimestampFloorMethod floorMethod = (TimestampDriver.TimestampFloorMethod) result;
                            return (TimestampDriver.TimestampFloorMethod) timestamp -> {
                                floorCallCount++;
                                return floorMethod.floor(timestamp);
                            };
                        }
                        return result;
                    }
            );
        }
    }

    private static class CountingTypeFunction extends IndexedParameterLinkFunction {
        private final CharSequence stringValue;
        private final long timestampValue;
        private final int type;
        private int typeCallCount;

        private CountingTypeFunction(int type, long timestampValue, CharSequence stringValue) {
            super(0, type, 0);
            this.type = type;
            this.timestampValue = timestampValue;
            this.stringValue = stringValue;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return stringValue;
        }

        @Override
        public long getTimestamp(Record rec) {
            return timestampValue;
        }

        @Override
        public int getType() {
            typeCallCount++;
            return type;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
        }
    }

    private static class StabilityFunction extends TimestampFunction {
        private final boolean isStableWithinExecution;

        private StabilityFunction(boolean isStableWithinExecution) {
            super(ColumnType.TIMESTAMP);
            this.isStableWithinExecution = isStableWithinExecution;
        }

        @Override
        public long getTimestamp(Record rec) {
            return 0;
        }

        @Override
        public boolean isNonDeterministic() {
            return true;
        }

        @Override
        public boolean isStableWithinExecution() {
            return isStableWithinExecution;
        }
    }
}
