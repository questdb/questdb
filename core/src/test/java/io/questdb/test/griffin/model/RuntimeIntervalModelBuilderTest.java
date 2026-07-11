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
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.model.RuntimeIntervalModelBuilder;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.griffin.model.TimestampMonotonicInverter;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

/**
 * Function-ownership tests for {@link RuntimeIntervalModelBuilder}. Every Function handed to the
 * builder must end up closed exactly once: either by the {@link RuntimeIntrinsicIntervalModel}
 * that {@link RuntimeIntervalModelBuilder#build()} transfers ownership to, or by the builder
 * itself on rollback/no-op paths that never adopt the function.
 */
public class RuntimeIntervalModelBuilderTest {

    @Test
    public void testBetweenDynamicAdoptionIsAtomicUnderAllocationFailure() {
        // Injects an allocation failure into the capacity reservation of a dynamic/dynamic
        // BETWEEN and emulates the WhereClauseParser error handling: the catch in
        // translateBetweenToTimestampModel frees the incoming endpoint, the finally in
        // analyzeBetween0 rolls back the pending endpoint via clearBetweenParsing(). Every
        // endpoint must be closed exactly once and the builder must stay consistent.
        ReservationFailingBuilder builder = newFailingBuilder();
        CloseCountingFunction lo = new CloseCountingFunction();
        CloseCountingFunction hi = new CloseCountingFunction();
        builder.setBetweenBoundary(lo, 0);
        builder.failNextReservation = true;
        try {
            builder.setBetweenBoundary(hi, 0);
            Assert.fail("injected failure expected");
        } catch (RuntimeException e) {
            // WhereClauseParser.translateBetweenToTimestampModel catch: frees the incoming func
            Misc.free(hi);
        }
        // WhereClauseParser.analyzeBetween0 finally: rollback of the pending endpoint
        builder.clearBetweenParsing();
        Assert.assertEquals("pending first endpoint must be closed exactly once", 1, lo.closeCount);
        Assert.assertEquals("incoming second endpoint must be closed exactly once", 1, hi.closeCount);
        Assert.assertFalse("failed append must not mark intervals applied", builder.hasIntervalFilters());

        // the failed append must leave the lists untouched and aligned: a subsequent BETWEEN
        // must adopt and transfer its endpoints normally
        CloseCountingFunction lo2 = new CloseCountingFunction();
        CloseCountingFunction hi2 = new CloseCountingFunction();
        builder.setBetweenBoundary(lo2, 0);
        builder.setBetweenBoundary(hi2, 0);
        RuntimeIntrinsicIntervalModel model = builder.build();
        builder.clear();
        Misc.free(model);
        Assert.assertEquals(1, lo2.closeCount);
        Assert.assertEquals(1, hi2.closeCount);
        Assert.assertEquals("retry must not close the rolled-back endpoint again", 1, lo.closeCount);
        Assert.assertEquals("retry must not close the freed endpoint again", 1, hi.closeCount);
    }

    @Test
    public void testBetweenNullBoundaryConsumesDynamicEndpoint() {
        // `ts BETWEEN <runtime func> AND NULL`: the semi-dynamic translation empties the model
        // (or no-ops when negated) instead of adopting the dynamic endpoint; the endpoint must
        // still be closed exactly once by the roll-back path.
        RuntimeIntervalModelBuilder builder = newBuilder();
        CloseCountingFunction lo = new CloseCountingFunction();
        builder.setBetweenBoundary(lo, 0);
        builder.setBetweenBoundary(Numbers.LONG_NULL);
        builder.clearBetweenParsing();
        Assert.assertEquals(1, lo.closeCount);

        builder = newBuilder();
        builder.setBetweenNegated(true);
        lo = new CloseCountingFunction();
        builder.setBetweenBoundary(lo, 0);
        builder.setBetweenBoundary(Numbers.LONG_NULL);
        builder.clearBetweenParsing();
        Assert.assertEquals(1, lo.closeCount);

        // constant NULL first, dynamic endpoint second
        builder = newBuilder();
        lo = new CloseCountingFunction();
        builder.setBetweenBoundary(Numbers.LONG_NULL);
        builder.setBetweenBoundary(lo, 0);
        builder.clearBetweenParsing();
        Assert.assertEquals(1, lo.closeCount);
    }

    @Test
    public void testBetweenRollbackClosesPendingFunction() {
        // WhereClauseParser stores the first dynamic BETWEEN endpoint in the builder and rolls
        // back via clearBetweenParsing() when the second endpoint cannot become an intrinsic.
        // The rollback must close the pending, not-yet-adopted function.
        RuntimeIntervalModelBuilder builder = newBuilder();
        CloseCountingFunction lo = new CloseCountingFunction();
        builder.setBetweenBoundary(lo, 0);
        builder.clearBetweenParsing();
        Assert.assertEquals(1, lo.closeCount);
    }

    @Test
    public void testBetweenSemiDynamicIncomingAdoptionIsAtomicUnderAllocationFailure() {
        // Constant first endpoint, dynamic second endpoint: an allocation failure in the
        // capacity reservation must leave the incoming function owned by the caller
        // (WhereClauseParser frees it in its catch) and must not adopt it into the builder,
        // otherwise the parser catch and the builder rollback double-close it.
        ReservationFailingBuilder builder = newFailingBuilder();
        CloseCountingFunction hi = new CloseCountingFunction();
        builder.setBetweenBoundary(1_000_000L);
        builder.failNextReservation = true;
        try {
            builder.setBetweenBoundary(hi, 0);
            Assert.fail("injected failure expected");
        } catch (RuntimeException e) {
            // WhereClauseParser.translateBetweenToTimestampModel catch: frees the incoming func
            Misc.free(hi);
        }
        builder.clearBetweenParsing();
        builder.freeAndClear();
        Assert.assertEquals("incoming endpoint must be closed exactly once", 1, hi.closeCount);
    }

    @Test
    public void testBetweenSemiDynamicPendingAdoptionIsAtomicUnderAllocationFailure() {
        // Dynamic first endpoint, constant second endpoint: an allocation failure in the
        // capacity reservation must leave the pending endpoint reachable through
        // betweenBoundaryFunc so the clearBetweenParsing() rollback closes it exactly once.
        ReservationFailingBuilder builder = newFailingBuilder();
        CloseCountingFunction lo = new CloseCountingFunction();
        builder.setBetweenBoundary(lo, 0);
        builder.failNextReservation = true;
        try {
            builder.setBetweenBoundary(2_000_000L);
            Assert.fail("injected failure expected");
        } catch (RuntimeException e) {
            // no incoming function on this path; nothing for the parser to free
        }
        builder.clearBetweenParsing();
        Assert.assertEquals("pending endpoint must be closed exactly once", 1, lo.closeCount);
        Assert.assertFalse("failed append must not mark intervals applied", builder.hasIntervalFilters());
        builder.freeAndClear();
        Assert.assertEquals("rollback must not close the pending endpoint again", 1, lo.closeCount);
    }

    @Test
    public void testBetweenTransferClosesFunctionsExactlyOnce() {
        // Both endpoints translate: the builder adopts them into its dynamic range list, and
        // build() transfers ownership to the model. The rollback in clearBetweenParsing() and the
        // builder's clear() must not close them; only the model's close() does, exactly once.
        RuntimeIntervalModelBuilder builder = newBuilder();
        CloseCountingFunction lo = new CloseCountingFunction();
        CloseCountingFunction hi = new CloseCountingFunction();
        builder.setBetweenBoundary(lo, 0);
        builder.setBetweenBoundary(hi, 0);
        builder.clearBetweenParsing();
        Assert.assertEquals(0, lo.closeCount);
        Assert.assertEquals(0, hi.closeCount);

        RuntimeIntrinsicIntervalModel model = builder.build();
        builder.clear();
        Assert.assertEquals(0, lo.closeCount);
        Assert.assertEquals(0, hi.closeCount);

        Misc.free(model);
        Assert.assertEquals(1, lo.closeCount);
        Assert.assertEquals(1, hi.closeCount);
    }

    @Test
    public void testBuildFailureLeavesFunctionsOwnedByBuilderAtEveryStage() {
        for (BuildFailureStage stage : BuildFailureStage.values()) {
            BuildFailingBuilder builder = newBuildFailingBuilder();
            CloseCountingFunction timestampFunction = new CloseCountingFunction();
            CloseCountingCursorFunction cursorFunction = new CloseCountingCursorFunction();
            builder.intersectRuntimeTimestamp(timestampFunction, 11);
            builder.intersectRuntimeTimestamp(cursorFunction, 22);
            // Once dynamic mode starts, this static interval adds a null dynamic-function slot.
            builder.intersect(30, 40);

            assertBuildFailure(builder, stage);
            Assert.assertEquals("failed build must not close the timestamp function at " + stage, 0, timestampFunction.closeCount);
            Assert.assertEquals("failed build must not close the cursor function at " + stage, 0, cursorFunction.closeCount);

            // No model exists, so the transfer never committed and builder cleanup owns both
            // non-null functions. The null slot needs no cleanup.
            builder.clear();
            Assert.assertEquals("builder must close the timestamp function exactly once at " + stage, 1, timestampFunction.closeCount);
            Assert.assertEquals("builder must close the cursor function exactly once at " + stage, 1, cursorFunction.closeCount);
            builder.clear();
            Assert.assertEquals("repeated cleanup must not close the timestamp function again at " + stage, 1, timestampFunction.closeCount);
            Assert.assertEquals("repeated cleanup must not close the cursor function again at " + stage, 1, cursorFunction.closeCount);

            // A builder remains reusable after rolling back any build stage.
            CloseCountingFunction reusedFunction = new CloseCountingFunction();
            builder.intersectRuntimeTimestamp(reusedFunction, 33);
            RuntimeIntrinsicIntervalModel model = builder.build();
            builder.clear();
            Assert.assertEquals(0, reusedFunction.closeCount);
            Misc.free(model);
            Assert.assertEquals(1, reusedFunction.closeCount);
        }
    }

    @Test
    public void testBuildRetryAfterEveryFailureStageTransfersOwnership() {
        for (BuildFailureStage stage : BuildFailureStage.values()) {
            BuildFailingBuilder builder = newBuildFailingBuilder();
            CloseCountingFunction timestampFunction = new CloseCountingFunction();
            CloseCountingCursorFunction cursorFunction = new CloseCountingCursorFunction();
            builder.intersectRuntimeTimestamp(timestampFunction, 11);
            builder.intersectRuntimeTimestamp(cursorFunction, 22);
            builder.intersect(30, 40);

            assertBuildFailure(builder, stage);
            RuntimeIntrinsicIntervalModel model = builder.build();
            builder.clear();
            Assert.assertEquals("builder must not close the transferred timestamp function after " + stage, 0, timestampFunction.closeCount);
            Assert.assertEquals("builder must not close the transferred cursor function after " + stage, 0, cursorFunction.closeCount);
            Misc.free(model);
            Assert.assertEquals("model must close the timestamp function exactly once after " + stage, 1, timestampFunction.closeCount);
            Assert.assertEquals("model must close the cursor function exactly once after " + stage, 1, cursorFunction.closeCount);
        }
    }

    @Test
    public void testClearAfterBuildLeavesTransferredFunctionsToModel() {
        // clear() after build() with no intermediate clearBetweenParsing(): the transferred
        // functions belong to the model; clear() must not close them (and the model closes them
        // exactly once).
        RuntimeIntervalModelBuilder builder = newBuilder();
        CloseCountingFunction lo = new CloseCountingFunction();
        CloseCountingFunction hi = new CloseCountingFunction();
        builder.setBetweenBoundary(lo, 0);
        builder.setBetweenBoundary(hi, 0);
        RuntimeIntrinsicIntervalModel model = builder.build();
        builder.clear();
        Assert.assertEquals(0, lo.closeCount);
        Assert.assertEquals(0, hi.closeCount);
        Misc.free(model);
        Assert.assertEquals(1, lo.closeCount);
        Assert.assertEquals(1, hi.closeCount);
    }

    @Test
    public void testClearClosesPendingFunctionOnce() {
        // clear() without build() is a rollback: the pending BETWEEN endpoint must be closed
        // exactly once even though freeAndClear() also runs clearBetweenParsing() internally.
        RuntimeIntervalModelBuilder builder = newBuilder();
        CloseCountingFunction lo = new CloseCountingFunction();
        builder.setBetweenBoundary(lo, 0);
        builder.clear();
        Assert.assertEquals(1, lo.closeCount);
    }

    @Test
    public void testCursorFunctionsProducePositionsList() {
        // The non-empty path stays intact: each cursor function records its parse position.
        CursorPositionsExposingBuilder builder = newCursorPositionsExposingBuilder();
        builder.intersectRuntimeTimestamp(new CloseCountingFunction(), 11);
        builder.intersectRuntimeTimestamp(new CloseCountingCursorFunction(), 22);

        IntList positions = builder.cursorFunctionPositionsCopy();
        Assert.assertNotNull(positions);
        Assert.assertEquals(1, positions.size());
        Assert.assertEquals(22, positions.getQuick(0));
        builder.freeAndClear();
    }

    @Test
    public void testDynamicIntervalsDoNotAllocateErrorPositions() {
        // With no cursor functions the copy must be null, not an empty IntList: null is the
        // "no cursor bounds" sentinel RuntimeIntervalModel.getCursorFunctionPosition() handles,
        // and returning it avoids one dead object per interval-model compilation.
        CursorPositionsExposingBuilder builder = newCursorPositionsExposingBuilder();
        for (int i = 0; i < 32; i++) {
            builder.intersectRuntimeTimestamp(new CloseCountingFunction(), i);
        }

        Assert.assertNull(builder.cursorFunctionPositionsCopy());
        builder.freeAndClear();
    }

    @Test
    public void testEmptySetConsumesBetweenFunctions() {
        // dynamic + dynamic endpoints against an already-empty model
        RuntimeIntervalModelBuilder builder = newBuilder();
        builder.intersectEmpty();
        CloseCountingFunction lo = new CloseCountingFunction();
        CloseCountingFunction hi = new CloseCountingFunction();
        builder.setBetweenBoundary(lo, 0);
        builder.setBetweenBoundary(hi, 0);
        builder.clearBetweenParsing();
        Assert.assertEquals(1, lo.closeCount);
        Assert.assertEquals(1, hi.closeCount);

        // dynamic + constant
        builder = newBuilder();
        builder.intersectEmpty();
        lo = new CloseCountingFunction();
        builder.setBetweenBoundary(lo, 0);
        builder.setBetweenBoundary(1_000_000L);
        builder.clearBetweenParsing();
        Assert.assertEquals(1, lo.closeCount);

        // constant + dynamic
        builder = newBuilder();
        builder.intersectEmpty();
        hi = new CloseCountingFunction();
        builder.setBetweenBoundary(1_000_000L);
        builder.setBetweenBoundary(hi, 0);
        builder.clearBetweenParsing();
        Assert.assertEquals(1, hi.closeCount);
    }

    @Test
    public void testEmptySetConsumesIncomingFunctions() {
        // WhereClauseParser traverses AND predicates right-to-left, so `ts = NULL::TIMESTAMP` can
        // empty the model before an earlier predicate parses its runtime function. Every
        // Function-accepting builder method must consume ownership even on the empty-set no-op
        // path; otherwise the function leaks with no owner.
        RuntimeIntervalModelBuilder builder = newBuilder();
        builder.intersectEmpty();
        Assert.assertTrue(builder.isEmptySet());

        CloseCountingFunction f = new CloseCountingFunction();
        builder.intersect(0L, f, (short) -1, 0);
        Assert.assertEquals(1, f.closeCount);

        f = new CloseCountingFunction();
        builder.intersect(f, 0L, (short) 1, 0);
        Assert.assertEquals(1, f.closeCount);

        f = new CloseCountingFunction();
        builder.intersectRuntimeIntervals(f, 0);
        Assert.assertEquals(1, f.closeCount);

        f = new CloseCountingFunction();
        builder.intersectRuntimeTimestamp(f, 0);
        Assert.assertEquals(1, f.closeCount);

        f = new CloseCountingFunction();
        builder.subtractEquals(f, 0);
        Assert.assertEquals(1, f.closeCount);

        f = new CloseCountingFunction();
        builder.subtractRuntimeIntervals(f, 0);
        Assert.assertEquals(1, f.closeCount);

        f = new CloseCountingFunction();
        builder.unionRuntimeTimestamp(f, 0);
        Assert.assertEquals(1, f.closeCount);
    }

    @Test
    public void testEmptySetConsumesMonotonicInverter() {
        // intersectMonotonicTimestamp() on an already-empty model must consume ownership of the
        // inverter: its close() must reach the head and bound functions exactly once.
        RuntimeIntervalModelBuilder builder = newBuilder();
        builder.intersectEmpty();
        CloseCountingFunction head = new CloseCountingFunction();
        CloseCountingFunction lo = new CloseCountingFunction();
        CloseCountingFunction hi = new CloseCountingFunction();
        builder.intersectMonotonicTimestamp(new TimestampMonotonicInverter(
                head,
                new ObjList<>(),
                lo,
                (short) 0,
                0L,
                hi,
                (short) 0,
                0L,
                true,
                ColumnType.getTimestampDriver(ColumnType.TIMESTAMP)
        ));
        Assert.assertEquals(1, head.closeCount);
        Assert.assertEquals(1, lo.closeCount);
        Assert.assertEquals(1, hi.closeCount);
    }

    @Test
    public void testFreeAndClearClosesPendingFunctionOnce() {
        RuntimeIntervalModelBuilder builder = newBuilder();
        CloseCountingFunction lo = new CloseCountingFunction();
        builder.setBetweenBoundary(lo, 0);
        builder.freeAndClear();
        Assert.assertEquals(1, lo.closeCount);
    }

    @Test
    public void testFreeAndClearClosesSemiDynamicFunctionsOnce() {
        // One dynamic endpoint plus one constant endpoint: the dynamic function is adopted into
        // the range list; freeAndClear() must close it exactly once (not once via the pending
        // reference and again via the list).
        RuntimeIntervalModelBuilder builder = newBuilder();
        CloseCountingFunction lo = new CloseCountingFunction();
        builder.setBetweenBoundary(lo, 0);
        builder.setBetweenBoundary(1_000_000L);
        builder.freeAndClear();
        Assert.assertEquals(1, lo.closeCount);
    }

    private static void assertBuildFailure(BuildFailingBuilder builder, BuildFailureStage stage) {
        builder.failAt = stage;
        try {
            builder.build();
            Assert.fail("injected failure expected at " + stage);
        } catch (RuntimeException e) {
            Assert.assertEquals("injected build failure at " + stage, e.getMessage());
        }
    }

    private static BuildFailingBuilder newBuildFailingBuilder() {
        BuildFailingBuilder builder = new BuildFailingBuilder();
        builder.of(ColumnType.TIMESTAMP, PartitionBy.DAY, null);
        return builder;
    }

    private static RuntimeIntervalModelBuilder newBuilder() {
        RuntimeIntervalModelBuilder builder = new RuntimeIntervalModelBuilder();
        builder.of(ColumnType.TIMESTAMP, PartitionBy.DAY, null);
        return builder;
    }

    private static CursorPositionsExposingBuilder newCursorPositionsExposingBuilder() {
        CursorPositionsExposingBuilder builder = new CursorPositionsExposingBuilder();
        builder.of(ColumnType.TIMESTAMP, PartitionBy.DAY, null);
        return builder;
    }

    private static ReservationFailingBuilder newFailingBuilder() {
        ReservationFailingBuilder builder = new ReservationFailingBuilder();
        builder.of(ColumnType.TIMESTAMP, PartitionBy.DAY, null);
        return builder;
    }

    private static class BuildFailingBuilder extends RuntimeIntervalModelBuilder {
        BuildFailureStage failAt;

        @Override
        protected IntList copyCursorFunctionPositions() {
            fail(BuildFailureStage.CURSOR_POSITIONS_COPY);
            return super.copyCursorFunctionPositions();
        }

        @Override
        protected ObjList<Function> copyDynamicRangeList() {
            fail(BuildFailureStage.DYNAMIC_RANGE_COPY);
            return super.copyDynamicRangeList();
        }

        @Override
        protected LongList copyStaticIntervals() {
            fail(BuildFailureStage.STATIC_INTERVALS_COPY);
            return super.copyStaticIntervals();
        }

        @Override
        protected RuntimeIntrinsicIntervalModel createModel(
                LongList staticIntervals,
                ObjList<Function> dynamicRangeList,
                IntList cursorFunctionPositions
        ) {
            fail(BuildFailureStage.MODEL_CREATION);
            return super.createModel(staticIntervals, dynamicRangeList, cursorFunctionPositions);
        }

        private void fail(BuildFailureStage stage) {
            if (failAt == stage) {
                failAt = null;
                throw new RuntimeException("injected build failure at " + stage);
            }
        }
    }

    private enum BuildFailureStage {
        STATIC_INTERVALS_COPY,
        DYNAMIC_RANGE_COPY,
        CURSOR_POSITIONS_COPY,
        MODEL_CREATION
    }

    private static class CloseCountingCursorFunction extends CursorFunction {
        int closeCount;

        CloseCountingCursorFunction() {
            super(new EmptyTableRecordCursorFactory(new GenericRecordMetadata()));
        }

        @Override
        public void close() {
            closeCount++;
            super.close();
        }
    }

    private static class CloseCountingFunction extends TimestampFunction {
        int closeCount;

        CloseCountingFunction() {
            super(ColumnType.TIMESTAMP);
        }

        @Override
        public void close() {
            closeCount++;
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

    /**
     * Exposes the protected {@link RuntimeIntervalModelBuilder#copyCursorFunctionPositions()}
     * accessor so the test can assert on the sparse positions list without reflection.
     */
    private static class CursorPositionsExposingBuilder extends RuntimeIntervalModelBuilder {
        IntList cursorFunctionPositionsCopy() {
            return copyCursorFunctionPositions();
        }
    }

    /**
     * Simulates an allocation failure at the single fallible point of a BETWEEN append: the
     * up-front capacity reservation that precedes every list mutation and ownership transfer.
     */
    private static class ReservationFailingBuilder extends RuntimeIntervalModelBuilder {
        boolean failNextReservation;

        @Override
        protected void reserveEncodedIntervals(int intervalCount, int cursorFunctionCount) {
            if (failNextReservation) {
                failNextReservation = false;
                throw new RuntimeException("injected allocation failure");
            }
            super.reserveEncodedIntervals(intervalCount, cursorFunctionCount);
        }
    }
}
