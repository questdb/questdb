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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
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
            final RuntimeIntervalModel model = new RuntimeIntervalModel(
                    ColumnType.getTimestampDriver(ColumnType.TIMESTAMP),
                    PartitionBy.DAY,
                    intervals,
                    dynamicFunctions
            );
            try {
                final LongList out = model.calculateIntervals(sqlExecutionContext);
                Assert.assertEquals(2, out.size());
                Assert.assertEquals(1_000L, out.getQuick(0));
                Assert.assertEquals(5_000L, out.getQuick(1));
            } finally {
                model.close();
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
            final RuntimeIntervalModel model = new RuntimeIntervalModel(
                    ColumnType.getTimestampDriver(ColumnType.TIMESTAMP),
                    PartitionBy.DAY,
                    intervals,
                    dynamicFunctions,
                    new IntList()
            );
            try {
                final LongList out = model.calculateIntervals(sqlExecutionContext);
                Assert.assertEquals(2, out.size());
                Assert.assertEquals(2_000L, out.getQuick(0));
                Assert.assertEquals(7_000L, out.getQuick(1));
            } finally {
                model.close();
            }
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

    private void assertModelMultiRowSubQueryError(RuntimeIntrinsicIntervalModel model, int expectedPosition) throws SqlException {
        try {
            model.calculateIntervals(sqlExecutionContext);
            Assert.fail("multi-row scalar sub-query must be rejected");
        } catch (SqlException e) {
            Assert.assertEquals(
                    "error must use the sparse cursor position, or 0 when it is unavailable",
                    expectedPosition,
                    e.getPosition()
            );
            TestUtils.assertContains(e.getFlyweightMessage(), "scalar sub-query returned more than one row");
        } finally {
            model.close();
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
    ) throws Exception {
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
}
