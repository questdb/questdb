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
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.model.RuntimeIntervalModelBuilder;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
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

    private static RuntimeIntervalModelBuilder newBuilder() {
        RuntimeIntervalModelBuilder builder = new RuntimeIntervalModelBuilder();
        builder.of(ColumnType.TIMESTAMP, PartitionBy.DAY, null);
        return builder;
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
}
