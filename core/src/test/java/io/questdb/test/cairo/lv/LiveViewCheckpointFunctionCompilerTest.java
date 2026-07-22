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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointContracts.DependencyKind;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointDependency.NumericConvergence;
import io.questdb.cairo.lv.LiveViewCheckpointDependency.StructuralConvergence;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
import io.questdb.cairo.lv.LiveViewCheckpointRangePlan;
import io.questdb.cairo.lv.LiveViewCheckpointRowsPlan;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.LiveViewCheckpointFunctionCompiler;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class LiveViewCheckpointFunctionCompilerTest extends AbstractCairoTest {

    @Test
    public void testCompilerIdentityIsStableAndSeparatesLogicalFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, sym symbol, x double) timestamp(ts) partition by day wal");
            final String functionAtPositionTwo = "select ts, sym, "
                    + "avg(x) over w as a from base "
                    + "window w as (partition by sym order by ts range between 10 preceding and current row)";

            final Metadata first = compileMetadata(functionAtPositionTwo, 0);
            final Metadata repeated = compileMetadata(functionAtPositionTwo, 0);
            final Metadata secondOutput = compileMetadata(
                    "select ts, sym, x, avg(x) over w as a from base "
                            + "window w as (partition by sym order by ts range between 10 preceding and current row)",
                    0
            );
            Assert.assertArrayEquals(first.identity.getEncoded(), repeated.identity.getEncoded());
            Assert.assertFalse(Arrays.equals(first.identity.getEncoded(), secondOutput.identity.getEncoded()));
            Assert.assertEquals(2, first.identity.getOutputPosition());
            Assert.assertEquals(3, secondOutput.identity.getOutputPosition());
            Assert.assertEquals("w", first.identity.getCanonicalWindowName());
            Assert.assertEquals("avg(D)", first.identity.getFactorySignature());
            Assert.assertTrue(first.identity.getPartitionSignature().contains("sym"));
            Assert.assertTrue(first.identity.getOrderSignature().contains("ts"));
            Assert.assertTrue(first.identity.getStateCodecIdentity().contains("/avg(D)/v2"));

            final Metadata renamedWindow = compileMetadata(
                    "select ts, sym, avg(x) over other as a from base "
                            + "window other as (partition by sym order by ts range between 10 preceding and current row)",
                    0
            );
            Assert.assertFalse(Arrays.equals(first.identity.getEncoded(), renamedWindow.identity.getEncoded()));

            final Metadata otherFactory = compileMetadata(
                    "select ts, sym, sum(x) over w as a from base "
                            + "window w as (partition by sym order by ts range between 10 preceding and current row)",
                    0
            );
            Assert.assertFalse(Arrays.equals(first.identity.getEncoded(), otherFactory.identity.getEncoded()));
            Assert.assertNotEquals(first.identity.getFactorySignature(), otherFactory.identity.getFactorySignature());
        });
    }

    @Test
    public void testIdentityEncodingIsLengthDelimited() {
        final LiveViewCheckpointFunctionIdentity left = new LiveViewCheckpointFunctionIdentity(
                "w", "f(D)", 1, "1:a;2:bc", "", "codec/v1"
        );
        final LiveViewCheckpointFunctionIdentity right = new LiveViewCheckpointFunctionIdentity(
                "w", "f(D)", 1, "2:ab;1:c", "", "codec/v1"
        );
        Assert.assertFalse(Arrays.equals(left.getEncoded(), right.getEncoded()));
        final byte[] owned = left.getEncoded();
        owned[0] ^= 1;
        Assert.assertFalse(Arrays.equals(owned, left.getEncoded()));
    }

    @Test
    public void testRangeAndRowsDependencyDescriptors() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, sym symbol, x double) timestamp(ts) partition by day wal");
            final Metadata range = compileMetadata(
                    "select ts, sym, avg(x) over (partition by sym order by ts "
                            + "range between 10 preceding and current row) as a from base",
                    0
            );
            Assert.assertEquals(DependencyKind.RANGE_W_PRECEDING_BOUNDED_HI, range.dependency.getKind());
            Assert.assertEquals(-10, range.dependency.getFrameLo());
            Assert.assertEquals(0, range.dependency.getFrameHi());
            Assert.assertEquals(10, range.dependency.getRangeFrameWidth());
            Assert.assertNotNull(range.rangePlan);
            Assert.assertTrue(range.dependency.supportsKeyRestore());
            Assert.assertFalse(range.dependency.supportsKeyReset());
            Assert.assertEquals(StructuralConvergence.EXACT, range.dependency.getStructuralConvergence());
            Assert.assertEquals(NumericConvergence.FLOATING_TOLERANCE, range.dependency.getNumericConvergence());
            Assert.assertTrue(range.dependency.hasFrameLocalState());
            Assert.assertEquals(range.dependency.getKind().getLowBoundStrategy(), range.dependency.getLowBoundStrategy());
            Assert.assertEquals(range.dependency.getKind().getHighBoundStrategy(), range.dependency.getHighBoundStrategy());

            final Metadata rows = compileMetadata(
                    "select ts, sym, count(x) over (partition by sym order by ts "
                            + "rows between 3 preceding and current row) as c from base",
                    0
            );
            Assert.assertEquals(DependencyKind.ROWS_N_PRECEDING_BOUNDED_HI, rows.dependency.getKind());
            Assert.assertEquals(-3, rows.dependency.getFrameLo());
            Assert.assertEquals(0, rows.dependency.getFrameHi());
            Assert.assertEquals(NumericConvergence.EXACT, rows.dependency.getNumericConvergence());
            Assert.assertTrue(rows.dependency.hasFrameLocalState());
            Assert.assertEquals(3, rows.dependency.getRowsPrecedingCount());
            Assert.assertTrue(rows.dependency.isFiniteRows());
            Assert.assertFalse(rows.dependency.isFiniteRange());
            Assert.assertFalse(range.dependency.isFiniteRows());
            // Each plan describes the functions of its own kind and no others, so a
            // single-function factory carries exactly one of them.
            Assert.assertNull(rows.rangePlan);
            Assert.assertNotNull(rows.rowsPlan);
            Assert.assertNull(range.rowsPlan);
            Assert.assertTrue(rows.isDependencyComplete);
            Assert.assertTrue(range.isDependencyComplete);
        });
    }

    @Test
    public void testRowsDependencyBuildsAKeyedUnionPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, sym symbol, x double) timestamp(ts) partition by day wal");
            // Two ROWS functions over the same domain: the plan unions them and keeps the
            // widest look-behind, because the dependency floor has to satisfy both.
            final Metadata rows = compileMetadata(
                    "select ts, sym, "
                            + "sum(x) over (partition by sym order by ts rows between 3 preceding and current row) s, "
                            + "count(x) over (partition by sym order by ts rows between 5 preceding and current row) c "
                            + "from base",
                    0
            );
            Assert.assertNotNull(rows.rowsPlan);
            Assert.assertEquals(2, rows.rowsPlan.getFunctionCount());
            Assert.assertEquals(5, rows.rowsPlan.getMaxPrecedingRows());
            Assert.assertEquals(rows.dependency.getPartitionSignature(), rows.rowsPlan.getPartitionSignature());
            Assert.assertEquals(rows.dependency.getOrderSignature(), rows.rowsPlan.getOrderSignature());
            Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, rows.rowsPlan.getTimestampType());
            // The key projector is resolved against the base factory's own metadata, so
            // its indexes are the ones a page-frame record answers to.
            Assert.assertEquals(0, rows.rowsPlan.getTimestampIndex());
            Assert.assertEquals(1, rows.rowsPlan.getPartitionByColumnCount());
            Assert.assertEquals(1, rows.rowsPlan.getPartitionByColumnIndex(0));
            Assert.assertNotNull(rows.rowsPlan.getKeySink());
            Assert.assertEquals(1, rows.rowsPlan.getKeyColumnTypes().getColumnCount());
            Assert.assertEquals(ColumnType.SYMBOL, rows.rowsPlan.getKeyColumnTypes().getColumnType(0));
        });
    }

    /**
     * A PARTITION BY term the sink cannot read off a page-frame record is projected through
     * a compiled key function instead of declining the plan. What such a view loses is the
     * index seek and nothing else: the plan names no key column, so the dependency floor is
     * found by the unrestricted backward walk.
     * <p>
     * One expression puts <b>every</b> term on a key function, mixed list or not. The two
     * halves would otherwise write a SYMBOL key in two different spaces - a column as the
     * reader's integer, a function as its resolved string - and a projector whose key
     * identity depends on which half a term landed in is one bug away from counting two
     * keys as one.
     */
    @Test
    public void testRowsDependencyProjectsAnExpressionKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, sym symbol, x double) timestamp(ts) partition by day wal");
            final Metadata expression = compileMetadata(
                    "select ts, sym, "
                            + "sum(x) over (partition by upper(sym) order by ts rows between 3 preceding and current row) s "
                            + "from base",
                    0
            );
            Assert.assertNotNull(expression.rowsPlan);
            Assert.assertTrue(expression.isDependencyComplete);
            Assert.assertEquals(3, expression.rowsPlan.getMaxPrecedingRows());
            Assert.assertEquals(0, expression.rowsPlan.getPartitionByColumnCount());
            Assert.assertNotNull(expression.rowsPlan.getKeySink());
            Assert.assertEquals(1, expression.rowsPlan.getKeyColumnTypes().getColumnCount());
            Assert.assertEquals(ColumnType.STRING, expression.rowsPlan.getKeyColumnTypes().getColumnType(0));

            // A plain column beside an expression: both become key functions, and the
            // column's SYMBOL is written through its resolved string like any other.
            final Metadata mixedKeys = compileMetadata(
                    "select ts, sym, "
                            + "sum(x) over (partition by sym, x % 10 order by ts "
                            + "rows between 3 preceding and current row) s from base",
                    0
            );
            Assert.assertNotNull(mixedKeys.rowsPlan);
            Assert.assertEquals(0, mixedKeys.rowsPlan.getPartitionByColumnCount());
            Assert.assertEquals(2, mixedKeys.rowsPlan.getKeyColumnTypes().getColumnCount());
            Assert.assertEquals(ColumnType.STRING, mixedKeys.rowsPlan.getKeyColumnTypes().getColumnType(0));
            Assert.assertEquals(ColumnType.DOUBLE, mixedKeys.rowsPlan.getKeyColumnTypes().getColumnType(1));

            // A non-deterministic key is the one expression that still declines. The forward
            // pass and the backward search read the same base row from two cursors, and a
            // key that answers differently each time would count one row's predecessors
            // against another row's key.
            assertNoRowsPlan(
                    "select ts, sym, "
                            + "sum(x) over (partition by now() order by ts rows between 3 preceding and current row) s "
                            + "from base"
            );
        });
    }

    /**
     * A factory mixing a bounded RANGE window with a bounded ROWS one carries both plans.
     * Each describes the functions of its own kind and stays silent about the rest, and a
     * repair bounds them together by taking the earliest {@code L} and the latest {@code H}
     * the two prove.
     * <p>
     * What the compiler owes the repair is therefore not one plan but a <b>complete set</b>:
     * every window function inside one of them. The replacement over {@code [R, H)} is
     * timestamp-global, so it re-emits every function's output from the same replay, and one
     * function the replay cannot reconstruct is one wrong column - which is why a
     * half-covered factory declines rather than localizing on the half it can prove.
     */
    @Test
    public void testMixedFrameFactoryCarriesBothPlans() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, sym symbol, x double) timestamp(ts) partition by day wal");
            final Metadata mixed = compileMetadata(
                    "select ts, sym, "
                            + "avg(x) over (partition by sym order by ts range between 2 seconds preceding and current row) a, "
                            + "sum(x) over (partition by sym order by ts rows between 3 preceding and current row) s "
                            + "from base",
                    0
            );
            Assert.assertTrue(mixed.dependency.isFiniteRange());
            Assert.assertNotNull(mixed.rangePlan);
            Assert.assertEquals(1, mixed.rangePlan.getFunctionCount());
            Assert.assertEquals(2_000_000L, mixed.rangePlan.getMaxFrameWidth());
            Assert.assertNotNull(mixed.rowsPlan);
            Assert.assertEquals(1, mixed.rowsPlan.getFunctionCount());
            Assert.assertEquals(3, mixed.rowsPlan.getMaxPrecedingRows());
            Assert.assertTrue(mixed.isDependencyComplete);

            // lag() reads outside its declared frame, so the ROWS plan declines - and the
            // RANGE plan, correct for the function it does describe, covers only half the
            // factory.
            final Metadata halfCovered = compileMetadata(
                    "select ts, sym, "
                            + "avg(x) over (partition by sym order by ts range between 2 seconds preceding and current row) a, "
                            + "lag(x, 5) over (partition by sym order by ts rows between 3 preceding and current row) l "
                            + "from base",
                    0
            );
            Assert.assertNotNull(halfCovered.rangePlan);
            Assert.assertNull(halfCovered.rowsPlan);
            Assert.assertFalse(halfCovered.isDependencyComplete);

            // A function of a kind no plan bounds - an unbounded cumulative window with no
            // anchor to reset it - leaves the set incomplete the same way, even though the
            // ROWS plan beside it is correct for its own half.
            final Metadata uncoveredKind = compileMetadata(
                    "select ts, sym, "
                            + "sum(x) over (partition by sym order by ts rows between 3 preceding and current row) s, "
                            + "count(x) over (partition by sym order by ts rows between unbounded preceding and current row) c "
                            + "from base",
                    0
            );
            Assert.assertNotNull(uncoveredKind.rowsPlan);
            Assert.assertEquals(1, uncoveredKind.rowsPlan.getFunctionCount());
            Assert.assertFalse(uncoveredKind.isDependencyComplete);
        });
    }

    /**
     * A frame shape alone does not license a localized repair. The repair reconstructs
     * state by replaying {@code [L, R)} - the frame's own extent below the output floor -
     * so a function that reads rows the frame does not admit would be replayed against a
     * warm-up that never feeds them. Such a function declines the plan for the whole
     * factory: the replacement is timestamp-global, so every function's output inside
     * {@code [R, H)} is re-emitted from the same replay.
     */
    @Test
    public void testFunctionsWithoutFrameLocalStateDeclineThePlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, sym symbol, x double) timestamp(ts) partition by day wal");
            // lag() counts predecessors by its own offset and ignores the frame entirely:
            // the frame promises three rows of look-behind and the function reads five.
            final Metadata rows = compileMetadata(
                    "select ts, sym, lag(x, 5) over (partition by sym order by ts "
                            + "rows between 3 preceding and current row) l from base",
                    0
            );
            Assert.assertTrue(rows.dependency.isFiniteRows());
            Assert.assertFalse(rows.dependency.hasFrameLocalState());
            Assert.assertNull(rows.rowsPlan);

            // The same hole on the RANGE side, where the width bounds the frame and not
            // the offset either.
            final Metadata range = compileMetadata(
                    "select ts, sym, lag(x, 5) over (partition by sym order by ts "
                            + "range between 2 seconds preceding and current row) l from base",
                    0
            );
            Assert.assertTrue(range.dependency.isFiniteRange());
            Assert.assertFalse(range.dependency.hasFrameLocalState());
            Assert.assertNull(range.rangePlan);

            // first_value() holds the frame and nothing else, so it is a candidate - but its
            // state is not proven to converge yet, and the default fails closed until it is.
            final Metadata notEnabledYet = compileMetadata(
                    "select ts, sym, first_value(x) over (partition by sym order by ts "
                            + "rows between 3 preceding and current row) f from base",
                    0
            );
            Assert.assertTrue(notEnabledYet.dependency.isFiniteRows());
            Assert.assertFalse(notEnabledYet.dependency.hasFrameLocalState());
            Assert.assertNull(notEnabledYet.rowsPlan);

            // One function short of the whole factory is enough to decline it.
            assertNoRowsPlan("select ts, sym, "
                    + "sum(x) over (partition by sym order by ts rows between 3 preceding and current row) s, "
                    + "lag(x, 5) over (partition by sym order by ts rows between 3 preceding and current row) l "
                    + "from base");

            // The RANGE domain check runs ahead of the gate, so an incompatible pair is
            // still named at CREATE rather than disappearing into a declined plan.
            execute("create table two_keys (ts timestamp, sym symbol, sym2 symbol, x double) "
                    + "timestamp(ts) partition by day wal");
            try {
                compileMetadata("select ts, sym, "
                        + "avg(x) over (partition by sym order by ts range between 2 seconds preceding and current row) a, "
                        + "lag(x, 5) over (partition by sym2 order by ts range between 2 seconds preceding and current row) l "
                        + "from two_keys", 0);
                Assert.fail("expected incompatible RANGE domain rejection");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "must use the same PARTITION BY and ORDER BY domain");
            }
        });
    }

    /**
     * The max/min family, admitted one type at a time. Its state is a ring of the frame's
     * own rows plus a monotonic deque over exactly those rows, so the frame's extent
     * determines every value it emits - the same claim the count and sum/avg families
     * carry, over a different buffer. The value it emits is one of the frame's rows rather
     * than an accumulator, so it converges exactly even over DOUBLE, where the sum carries
     * the documented floating tolerance instead.
     * <p>
     * Every value type that reaches a partitioned bounded frame is covered here, because
     * they are separate implementations rather than one parameterized class: the
     * long-valued family (LONG directly, DATE and TIMESTAMP through the shared helper),
     * DOUBLE, and each of the six DECIMAL widths.
     */
    @Test
    public void testMaxAndMinDeclareFrameLocalStateForEveryTypeAndFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table typed (ts timestamp, sym symbol, l long, d double, other timestamp, dt date, "
                    + "d8 decimal(2, 0), d16 decimal(4, 1), d32 decimal(9, 2), d64 decimal(18, 3), "
                    + "d128 decimal(38, 6), d256 decimal(76, 10)) timestamp(ts) partition by day wal");
            final String[] columns = {"l", "d", "other", "dt", "d8", "d16", "d32", "d64", "d128", "d256"};
            for (int i = 0; i < columns.length; i++) {
                assertFrameLocalOverBothFrames("max(" + columns[i] + ")");
                assertFrameLocalOverBothFrames("min(" + columns[i] + ")");
            }
        });
    }

    /**
     * The sum/count/avg families over every value type that reaches a partitioned bounded
     * frame. All three hold a ring of the frame's own rows and an accumulator over exactly
     * that ring, so the claim is one claim - but the accumulator's arithmetic is not, and
     * that is what this pins per type. A DECIMAL accumulator adds and subtracts fixed-point
     * values, which is exact, so a frame re-accumulated from the dependency floor holds the
     * same bits and the value read off it converges exactly. The DOUBLE arm re-accumulates
     * a floating sum instead and keeps the documented tolerance.
     * <p>
     * The rescaled two-argument {@code avg} is a fourth implementation rather than a
     * projection of the plain one: it carries its own accumulator per argument width and
     * divides at the target scale, so it declares the contract for itself.
     */
    @Test
    public void testSumCountAndAvgDeclareFrameLocalStateForEveryTypeAndFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table typed (ts timestamp, sym symbol, l long, d double, v varchar, "
                    + "d8 decimal(2, 0), d16 decimal(4, 1), d32 decimal(9, 2), d64 decimal(18, 3), "
                    + "d128 decimal(38, 6), d256 decimal(76, 10)) timestamp(ts) partition by day wal");
            final String[] decimals = {"d8", "d16", "d32", "d64", "d128", "d256"};
            for (int i = 0; i < decimals.length; i++) {
                assertFrameLocalOverBothFrames("sum(" + decimals[i] + ")");
                assertFrameLocalOverBothFrames("avg(" + decimals[i] + ")");
                // The rescale form. A target scale of 4 keeps every argument width inside the
                // precision limit, so all six reach their own rescaling implementation.
                assertFrameLocalOverBothFrames("avg(" + decimals[i] + ", 4)");
            }
            // A count is a counter over the frame's rows whatever it counts, so it is exact
            // for every argument type - including the ones no sum accepts.
            final String[] counted = {"*", "l", "d", "v", "sym", "d64"};
            for (int i = 0; i < counted.length; i++) {
                assertFrameLocalOverBothFrames("count(" + counted[i] + ")");
            }
            // The floating arm of the same buffer. LONG has no sum of its own and widens into
            // it, so it carries the tolerance too.
            assertFrameLocalOverBothFrames("sum(d)", NumericConvergence.FLOATING_TOLERANCE);
            assertFrameLocalOverBothFrames("avg(d)", NumericConvergence.FLOATING_TOLERANCE);
            assertFrameLocalOverBothFrames("sum(l)", NumericConvergence.FLOATING_TOLERANCE);
            assertFrameLocalOverBothFrames("avg(l)", NumericConvergence.FLOATING_TOLERANCE);
        });
    }

    /**
     * Every ROWS shape below compiles and stays a valid live view. Declining the repair
     * plan costs such a view only the localized path it does not have today - which is
     * why these are silent refusals rather than the CREATE-time rejections the RANGE
     * side uses.
     */
    @Test
    public void testRowsShapesOutsideTheDiscoverableContractDeclineThePlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, sym symbol, x double) timestamp(ts) partition by day wal");
            // An unbounded look-behind has no floor to discover at all.
            assertNoRowsPlan("select ts, sym, sum(x) over (partition by sym order by ts "
                    + "rows between unbounded preceding and current row) s from base");
            // A frame with no ORDER BY counts row positions in an order nothing pins, so
            // the replay's ts-ordered cursor need not reproduce them.
            assertNoRowsPlan("select ts, sym, sum(x) over (partition by sym "
                    + "rows between 3 preceding and current row) s from base");

            // Two bounded ROWS functions on different key domains would have to be
            // planned as two key domains and their timestamp ranges unioned, which the
            // first rollout does not do.
            execute("create table two_keys (ts timestamp, sym symbol, sym2 symbol, x double) "
                    + "timestamp(ts) partition by day wal");
            assertNoRowsPlan("select ts, sym, "
                    + "sum(x) over (partition by sym order by ts rows between 3 preceding and current row) s, "
                    + "count(x) over (partition by sym2 order by ts rows between 3 preceding and current row) c "
                    + "from two_keys");
        });
    }

    @Test
    public void testRangeDependencyNormalizesTimestampUnitsAndBuildsUnionPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, sym symbol, x double) timestamp(ts) partition by day wal");
            // The parser leaves the width in the units the user wrote, so the descriptor has
            // to normalize it the same way the runtime frame does - here 2s/3s into micros.
            final Metadata micros = compileMetadata(
                    "select ts, sym, "
                            + "avg(x) over (partition by sym order by ts range between 2 seconds preceding and current row) a, "
                            + "count(x) over (partition by sym order by ts range between 3 seconds preceding and current row) c "
                            + "from base",
                    0
            );
            Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, micros.dependency.getTimestampType());
            Assert.assertTrue(micros.dependency.isFiniteRange());
            Assert.assertEquals(2_000_000L, micros.dependency.getRangeFrameWidth());
            Assert.assertEquals(-2_000_000L, micros.dependency.getFrameLo());
            // The plan unions both functions and keeps the widest look-behind.
            Assert.assertNotNull(micros.rangePlan);
            Assert.assertEquals(2, micros.rangePlan.getFunctionCount());
            Assert.assertEquals(3_000_000L, micros.rangePlan.getMaxFrameWidth());
            Assert.assertEquals(micros.dependency.getPartitionSignature(), micros.rangePlan.getPartitionSignature());
            Assert.assertEquals(micros.dependency.getOrderSignature(), micros.rangePlan.getOrderSignature());

            // The same width normalizes against the base's own timestamp resolution.
            execute("create table base_ns (ts timestamp_ns, sym symbol, x double) timestamp(ts) partition by day wal");
            final Metadata nanos = compileMetadata(
                    "select ts, sym, avg(x) over (partition by sym order by ts "
                            + "range between 2 milliseconds preceding and current row) a from base_ns",
                    0
            );
            Assert.assertEquals(ColumnType.TIMESTAMP_NANO, nanos.dependency.getTimestampType());
            Assert.assertEquals(2_000_000L, nanos.dependency.getRangeFrameWidth());
            Assert.assertEquals(-2_000_000L, nanos.dependency.getFrameLo());
            Assert.assertNotNull(nanos.rangePlan);
            Assert.assertEquals(ColumnType.TIMESTAMP_NANO, nanos.rangePlan.getTimestampType());
        });
    }

    /**
     * The RANGE shapes outside {@code W PRECEDING} ending at or below the current row stay
     * compilable, but must not present themselves as a finite RANGE dependency - a repair
     * planner that claimed them would derive a bound the frame does not obey.
     * <p>
     * What separates them from the lagging high bounds, which
     * {@link #testLaggingHighBoundIsAFiniteDependency} owns, is the look-behind: it is what
     * both repair bounds are functions of, so a frame that names none has neither.
     */
    @Test
    public void testRangeShapesOutsideTheSupportedFrameAreNotFiniteRange() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, sym symbol, x double) timestamp(ts) partition by day wal");

            // Unbounded look-behind: no dependency floor below the correction.
            assertNotFiniteRange("select ts, sym, first_value(x) ignore nulls over (partition by sym order by ts "
                    + "range between unbounded preceding and '2' second preceding) a from base");
        });
    }

    /**
     * A frame ending below its own row reads a subset of what the same-width frame ending at
     * that row reads: the RANGE floor {@code R - W} still feeds every base row the frame
     * admits, the RANGE ceiling {@code changeMaxTs + W + 1} still sits above every output a
     * changed row reaches, and the ROWS discovery still converges from a key's
     * {@code (Nmax + 1)}-th row above the change. So the look-behind alone keeps bounding the
     * repair and the classifier hands out the eligible kind.
     * <p>
     * The descriptor's own finite-frame gates read the same subset argument, so the plan
     * follows from the kind: the width the plan carries is the look-behind, unchanged by
     * where the frame ends.
     */
    @Test
    public void testLaggingHighBoundIsAFiniteDependency() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, sym symbol, x double) timestamp(ts) partition by day wal");

            // RANGE, ending an hour below its own row. Both bounds carry the parser's unit
            // conversion, so both are tick offsets of the designated timestamp and the pair is
            // commensurable. The plan reads the look-behind, which the lag leaves alone.
            final Metadata range = compileMetadata(
                    "select ts, sym, avg(x) over (partition by sym order by ts "
                            + "range between '3' hour preceding and '1' hour preceding) a from base",
                    0
            );
            Assert.assertEquals(DependencyKind.RANGE_W_PRECEDING_BOUNDED_HI, range.dependency.getKind());
            Assert.assertEquals(-10_800_000_000L, range.dependency.getFrameLo());
            Assert.assertEquals(-3_600_000_000L, range.dependency.getFrameHi());
            Assert.assertTrue(range.dependency.isFiniteRange());
            Assert.assertEquals(10_800_000_000L, range.dependency.getRangeFrameWidth());
            Assert.assertNotNull(range.rangePlan);
            Assert.assertEquals(10_800_000_000L, range.rangePlan.getMaxFrameWidth());
            Assert.assertTrue(range.isDependencyComplete);

            // ROWS, ending two rows below its own row. Both bounds are row counts and carry
            // no unit, so the descriptor holds what the model does.
            final Metadata rows = compileMetadata(
                    "select ts, sym, sum(x) over (partition by sym order by ts "
                            + "rows between 10 preceding and 2 preceding) s from base",
                    0
            );
            Assert.assertEquals(DependencyKind.ROWS_N_PRECEDING_BOUNDED_HI, rows.dependency.getKind());
            Assert.assertEquals(-10, rows.dependency.getFrameLo());
            Assert.assertEquals(-2, rows.dependency.getFrameHi());
            Assert.assertTrue(rows.dependency.isFiniteRows());
            Assert.assertEquals(10, rows.dependency.getRowsPrecedingCount());
            Assert.assertNotNull(rows.rowsPlan);
            Assert.assertEquals(10, rows.rowsPlan.getMaxPrecedingRows());
            Assert.assertTrue(rows.isDependencyComplete);

            // The plan is the look-behind's, so the same frame written without the lag
            // reports the same bounds - the lag only removes rows from the affected set.
            final Metadata unlagged = compileMetadata(
                    "select ts, sym, sum(x) over (partition by sym order by ts "
                            + "rows between 10 preceding and current row) s from base",
                    0
            );
            Assert.assertEquals(0, unlagged.dependency.getFrameHi());
            Assert.assertEquals(
                    rows.dependency.getRowsPrecedingCount(),
                    unlagged.dependency.getRowsPrecedingCount()
            );

            // A FOLLOWING high bound is what must keep falling through: a base row then joins
            // the frame of output below itself and neither bound holds. No descriptor for one
            // is reachable from here - WindowContextImpl.validate() turns every finite
            // FOLLOWING bound away outright, and the UNBOUNDED FOLLOWING spelling compiles to
            // the two-pass factory this harness does not build - so what is assertable is the
            // rejection, and the classifier's own fall-through stands behind it.
            try {
                compileMetadata(
                        "select ts, sym, sum(x) over (partition by sym order by ts "
                                + "rows between 3 preceding and 2 following) s from base",
                        0
                );
                Assert.fail("expected FOLLOWING frame end rejection");
            } catch (SqlException e) {
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "frame end supports _number_ PRECEDING and CURRENT ROW only"
                );
            }

            // SqlOptimiser.normalizeWindowFrame() negates a Long.MAX_VALUE PRECEDING bound
            // into Long.MIN_VALUE, the encoding an unbounded look-behind uses, which leaves
            // the frame ending below its own start. The window layer answers that with a
            // constant null function carrying no descriptor at all, so the shape never
            // reaches the eligible kinds - and the classifier's own Long.MIN_VALUE test is
            // what keeps it out if that empty-frame handling ever changes.
            assertNoCheckpointDependency("select ts, sym, sum(x) over (partition by sym order by ts "
                    + "rows between 10 preceding and 9223372036854775807 preceding) s from base");
        });
    }

    /**
     * The compiler must never claim a frame the window factories cannot evaluate.
     * {@code WindowContextImpl.validate()} implements only {@code EXCLUDE NO OTHERS} and
     * {@code EXCLUDE CURRENT ROW}, and it is what turns the other two away - before the
     * descriptor is ever built, which is why these assert the compile failure rather than a
     * kind. The classifier's own exclusion test is the guard that keeps the two in agreement
     * if that ever stops being true.
     */
    @Test
    public void testUnsupportedExclusionModesFailCompilation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, sym symbol, x double) timestamp(ts) partition by day wal");
            assertExclusionRejected("select ts, sym, avg(x) over (partition by sym order by ts "
                    + "range between 2 preceding and current row exclude group) a from base");
            assertExclusionRejected("select ts, sym, avg(x) over (partition by sym order by ts "
                    + "range between 2 preceding and current row exclude ties) a from base");
            assertExclusionRejected("select ts, sym, sum(x) over (partition by sym order by ts "
                    + "rows between 3 preceding and current row exclude group) s from base");
            assertExclusionRejected("select ts, sym, sum(x) over (partition by sym order by ts "
                    + "rows between 3 preceding and current row exclude ties) s from base");
        });
    }

    /**
     * The descriptor records the high bound the runtime evaluates, not the one the model
     * carries. {@code WindowContextImpl.getRowsHi()} rewrites an {@code EXCLUDE CURRENT ROW}
     * frame's {@code 0} to {@code -1} before any factory sees it, so a descriptor reading the
     * model would claim a frame ending at the current row while the factory evaluates one
     * ending below it.
     * <p>
     * That {@code -1} is a lagging high bound with the smallest possible lag, so both
     * spellings reach the same eligible kind through the same test rather than the RANGE one
     * being turned away by an exclusion check of its own, and both are finite descriptors on
     * the look-behind the exclusion leaves untouched.
     */
    @Test
    public void testExcludeCurrentRowDescribesTheRuntimeFrameHighBound() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, sym symbol, x double) timestamp(ts) partition by day wal");
            final Metadata rows = compileMetadata(
                    "select ts, sym, sum(x) over (partition by sym order by ts "
                            + "rows between 3 preceding and current row exclude current row) s from base",
                    0
            );
            Assert.assertEquals(-3, rows.dependency.getFrameLo());
            Assert.assertEquals(-1, rows.dependency.getFrameHi());
            Assert.assertEquals(DependencyKind.ROWS_N_PRECEDING_BOUNDED_HI, rows.dependency.getKind());
            Assert.assertTrue(rows.dependency.isFiniteRows());
            Assert.assertEquals(3, rows.dependency.getRowsPrecedingCount());
            Assert.assertNotNull(rows.rowsPlan);
            Assert.assertTrue(rows.isDependencyComplete);

            // The RANGE spelling reaches the same -1, and that -1 is already normalized -
            // one tick of the designated timestamp, not one unit of whatever the frame
            // start was written in.
            final Metadata range = compileMetadata(
                    "select ts, sym, avg(x) over (partition by sym order by ts "
                            + "range between 2 seconds preceding and current row exclude current row) a from base",
                    0
            );
            Assert.assertEquals(-1, range.dependency.getFrameHi());
            Assert.assertEquals(DependencyKind.RANGE_W_PRECEDING_BOUNDED_HI, range.dependency.getKind());
            Assert.assertTrue(range.dependency.isFiniteRange());
            Assert.assertEquals(2_000_000L, range.dependency.getRangeFrameWidth());
            Assert.assertNotNull(range.rangePlan);
            Assert.assertTrue(range.isDependencyComplete);

            // EXCLUDE NO OTHERS is the same frame written without the exclusion, and it is
            // the one the model and the runtime already agreed on.
            final Metadata unexcluded = compileMetadata(
                    "select ts, sym, sum(x) over (partition by sym order by ts "
                            + "rows between 3 preceding and current row exclude no others) s from base",
                    0
            );
            Assert.assertEquals(0, unexcluded.dependency.getFrameHi());
            Assert.assertEquals(DependencyKind.ROWS_N_PRECEDING_BOUNDED_HI, unexcluded.dependency.getKind());
            Assert.assertTrue(unexcluded.dependency.isFiniteRows());
            Assert.assertNotNull(unexcluded.rowsPlan);
        });
    }

    /**
     * A RANGE bound the designated timestamp's units cannot carry produces a runtime frame
     * that is not the one the user wrote, so the compiler names it at its own position rather
     * than checkpointing a view against it. Both bounds run the same check, and each names
     * itself, so an error over a two-unit frame says which end failed.
     */
    @Test
    public void testRangeFrameBoundOutOfRangeFailsCompilation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, sym symbol, x double) timestamp(ts) partition by day wal");
            execute("create table base_ns (ts timestamp_ns, sym symbol, x double) timestamp(ts) partition by day wal");

            // TimestampDriver.from(long, char) narrows days to int, so this width collapses to
            // a zero-wide runtime frame.
            assertRangeBoundRejected(
                    "select ts, sym, avg(x) over (partition by sym order by ts "
                            + "range between 4294967296 days preceding and current row) a from base",
                    "RANGE frame width is out of range for the designated timestamp"
            );
            // The same narrowing on the high bound, which reaches the descriptor through the
            // same conversion and is named as the frame's end.
            assertRangeBoundRejected(
                    "select ts, sym, avg(x) over (partition by sym order by ts "
                            + "range between 5000 days preceding and 4294967296 days preceding) a from base",
                    "RANGE frame end lag is out of range for the designated timestamp"
            );
            // A bound finer than the timestamp's resolution divides down to zero, which is the
            // same failure seen from the other side: the frame the runtime evaluates ends at
            // the current row, not one nanosecond below it.
            assertRangeBoundRejected(
                    "select ts, sym, avg(x) over (partition by sym order by ts "
                            + "range between 2 seconds preceding and 1 nanosecond preceding) a from base",
                    "RANGE frame end lag is out of range for the designated timestamp"
            );
            // The unchecked multiply inside from() wraps 200000 days of nanoseconds onto a
            // positive value. The runtime's own frame validation runs first and turns it away
            // as a FOLLOWING bound, which is the right refusal for the wrong reason - the
            // frame is not one the user wrote at all. The live view is rejected either way,
            // and the compiler's sign test stands behind the runtime's.
            assertRangeBoundRejected(
                    "select ts, sym, avg(x) over (partition by sym order by ts "
                            + "range between 100000 days preceding and 200000 days preceding) a from base_ns",
                    "frame end supports _number_ PRECEDING and CURRENT ROW only"
            );

            // What the sign test cannot catch: 300000 days of nanoseconds wraps onto a
            // negative value, so it reads here as a legal lag of the wrong magnitude - about
            // 236 years rather than the 821 asked for. The descriptor still agrees with the
            // frame the runtime evaluates, since WindowContextImpl.of() calls the same
            // conversion, so the repair bounds stay sound and what is lost is the user's
            // frame, in a plain window query as much as in a live view. Closing it belongs in
            // the conversion rather than in either caller's guard, which is why both bounds
            // share one test here.
            final Metadata wrapped = compileMetadata(
                    "select ts, sym, avg(x) over (partition by sym order by ts "
                            + "range between 100000 days preceding and 300000 days preceding) a from base_ns",
                    0
            );
            Assert.assertEquals(-8_640_000_000_000_000_000L, wrapped.dependency.getFrameLo());
            Assert.assertEquals(-7_473_255_926_290_448_384L, wrapped.dependency.getFrameHi());
        });
    }

    /**
     * Both RANGE bounds reach the descriptor in the designated timestamp column's native
     * units, because both carry whatever unit the user wrote and the runtime converts both
     * before building the frame. A descriptor holding a converted low bound beside a raw high
     * one would disagree with that frame and would hold two numbers that cannot be compared to
     * each other, which is what a bound read off the high end needs them to be.
     */
    @Test
    public void testRangeFrameHighBoundIsNormalizedIntoTimestampUnits() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, sym symbol, x double) timestamp(ts) partition by day wal");
            final String[] highBounds = {
                    "1 day", "1 hour", "1 minute", "1 second", "1 millisecond", "1 microsecond", "1000 nanoseconds"
            };
            final long[] expectedMicros = {-86_400_000_000L, -3_600_000_000L, -60_000_000L, -1_000_000L, -1_000L, -1L, -1L};
            for (int i = 0; i < highBounds.length; i++) {
                final Metadata metadata = compileMetadata(
                        "select ts, sym, avg(x) over (partition by sym order by ts "
                                + "range between 2 days preceding and " + highBounds[i] + " preceding) a from base",
                        0
                );
                Assert.assertEquals(highBounds[i], expectedMicros[i], metadata.dependency.getFrameHi());
                // The pair is commensurable: one width, one lag, both micros, and the frame
                // they describe is non-empty.
                Assert.assertEquals(highBounds[i], -172_800_000_000L, metadata.dependency.getFrameLo());
                Assert.assertTrue(highBounds[i], metadata.dependency.getFrameLo() < metadata.dependency.getFrameHi());
                Assert.assertTrue(highBounds[i], metadata.dependency.isFiniteRange());
                Assert.assertEquals(highBounds[i], 172_800_000_000L, metadata.dependency.getRangeFrameWidth());
            }

            // The conversion follows the base's own resolution rather than a fixed unit.
            execute("create table base_ns (ts timestamp_ns, sym symbol, x double) timestamp(ts) partition by day wal");
            final Metadata nanos = compileMetadata(
                    "select ts, sym, avg(x) over (partition by sym order by ts "
                            + "range between '24' hour preceding and '2' second preceding) a from base_ns",
                    0
            );
            Assert.assertEquals(ColumnType.TIMESTAMP_NANO, nanos.dependency.getTimestampType());
            Assert.assertEquals(-86_400_000_000_000L, nanos.dependency.getFrameLo());
            Assert.assertEquals(-2_000_000_000L, nanos.dependency.getFrameHi());

            // A high bound at the current row carries no unit and reaches the descriptor
            // unchanged, so the shape admitted before the lagging ones were is unaffected.
            final Metadata currentRow = compileMetadata(
                    "select ts, sym, avg(x) over (partition by sym order by ts "
                            + "range between '24' hour preceding and current row) a from base",
                    0
            );
            Assert.assertEquals(0, currentRow.dependency.getFrameHi());
        });
    }

    @Test
    public void testAnchoredNamedWindowSurvivesResolution() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, sym symbol, x double) timestamp(ts) partition by day wal");
            final Metadata metadata = compileMetadata(
                    "select ts, sym, avg(x) over DailyWindow as a from base "
                            + "window dailywindow as (partition by sym order by ts "
                            + "anchor expression timestamp_floor('1d', ts))",
                    0
            );
            Assert.assertEquals("dailywindow", metadata.identity.getCanonicalWindowName());
            Assert.assertEquals(DependencyKind.FIXED_ANCHOR_SEGMENT, metadata.dependency.getKind());
            Assert.assertTrue(metadata.dependency.supportsKeyRestore());
            Assert.assertTrue(metadata.dependency.supportsKeyReset());

            final Metadata inlineAnchor = compileMetadata(
                    "select ts, sym, avg(x) over (partition by sym order by ts "
                            + "anchor expression timestamp_floor('1d', ts)) as a from base",
                    0
            );
            Assert.assertEquals("", inlineAnchor.identity.getCanonicalWindowName());
            Assert.assertEquals(DependencyKind.FIXED_ANCHOR_SEGMENT, inlineAnchor.dependency.getKind());
            Assert.assertTrue(inlineAnchor.dependency.supportsKeyReset());

            final String[] rankingFunctions = {"row_number", "rank", "dense_rank"};
            for (int i = 0; i < rankingFunctions.length; i++) {
                final String functionName = rankingFunctions[i];
                final Metadata ranking = compileMetadata(
                        "select ts, sym, " + functionName + "() over DailyWindow as r from base "
                                + "window dailywindow as (partition by sym order by ts "
                                + "anchor expression timestamp_floor('1d', ts))",
                        0
                );
                Assert.assertNotNull(functionName, ranking.identity);
                Assert.assertEquals(functionName + "()", ranking.identity.getFactorySignature());
                Assert.assertEquals(DependencyKind.FIXED_ANCHOR_SEGMENT, ranking.dependency.getKind());
            }
        });
    }

    private static void assertExclusionRejected(String sql) throws Exception {
        try {
            compileMetadata(sql, 0);
            Assert.fail(sql);
        } catch (SqlException e) {
            TestUtils.assertContains(
                    e.getFlyweightMessage(),
                    "only EXCLUDE NO OTHERS and EXCLUDE CURRENT ROW exclusion modes are supported"
            );
        }
    }

    private static void assertFrameLocalOverBothFrames(String projection) throws Exception {
        assertFrameLocalOverBothFrames(projection, NumericConvergence.EXACT);
    }

    /**
     * Asserts that {@code projection} carries frame-local state, and therefore a repair
     * plan, over both bounded frame shapes on the {@code typed} fixture, and that it
     * converges the way {@code convergence} claims. The two shapes are separate
     * implementations of the same buffer, so neither implies the other.
     */
    private static void assertFrameLocalOverBothFrames(String projection, NumericConvergence convergence) throws Exception {
        final Metadata rows = compileMetadata(
                "select ts, sym, " + projection + " over (partition by sym order by ts "
                        + "rows between 3 preceding and current row) w from typed",
                0
        );
        Assert.assertTrue(projection, rows.dependency.isFiniteRows());
        Assert.assertTrue(projection, rows.dependency.hasFrameLocalState());
        Assert.assertEquals(projection, convergence, rows.dependency.getNumericConvergence());
        Assert.assertNotNull(projection, rows.rowsPlan);

        final Metadata range = compileMetadata(
                "select ts, sym, " + projection + " over (partition by sym order by ts "
                        + "range between 2 seconds preceding and current row) w from typed",
                0
        );
        Assert.assertTrue(projection, range.dependency.isFiniteRange());
        Assert.assertTrue(projection, range.dependency.hasFrameLocalState());
        Assert.assertEquals(projection, convergence, range.dependency.getNumericConvergence());
        Assert.assertNotNull(projection, range.rangePlan);
    }

    /**
     * Asserts that {@code sql} compiles to a window function carrying no checkpoint
     * descriptor at all. That is a stronger refusal than an ineligible kind: with no
     * dependency to read, {@code isDependencyComplete} declines the repair however many
     * plans the factory holds.
     */
    private static void assertNoCheckpointDependency(String sql) throws Exception {
        sqlExecutionContext.setLiveViewCompile(true);
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
            RecordCursorFactory root = factory;
            while (root instanceof QueryProgress) {
                root = root.getBaseFactory();
            }
            Assert.assertTrue(sql, root instanceof WindowRecordCursorFactory);
            final ObjList<WindowFunction> functions = ((WindowRecordCursorFactory) root).getWindowFunctions();
            Assert.assertNull(sql, functions.getQuick(0).checkpointDependency());
            Assert.assertFalse(sql, LiveViewCheckpointFunctionCompiler.isDependencyComplete(functions, true, true, true));
        } finally {
            sqlExecutionContext.setLiveViewCompile(false);
        }
    }

    private static void assertNoRowsPlan(String sql) throws Exception {
        Assert.assertNull(sql, compileMetadata(sql, 0).rowsPlan);
    }

    private static void assertNotFiniteRange(String sql) throws Exception {
        final Metadata metadata = compileMetadata(sql, 0);
        Assert.assertFalse(sql, metadata.dependency.isFiniteRange());
        Assert.assertNull(sql, metadata.rangePlan);
    }

    private static void assertRangeBoundRejected(String sql, String expectedMessage) throws Exception {
        try {
            compileMetadata(sql, 0);
            Assert.fail(sql);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
        }
    }

    private static Metadata compileMetadata(String sql, int functionIndex) throws Exception {
        sqlExecutionContext.setLiveViewCompile(true);
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
            RecordCursorFactory root = factory;
            while (root instanceof QueryProgress) {
                root = root.getBaseFactory();
            }
            Assert.assertTrue(root instanceof WindowRecordCursorFactory);
            final WindowRecordCursorFactory windowFactory = (WindowRecordCursorFactory) root;
            final ObjList<WindowFunction> functions = windowFactory.getWindowFunctions();
            final WindowFunction function = functions.getQuick(functionIndex);
            Assert.assertNotNull(function.getClass().getName(), function.checkpointFunctionIdentity());
            Assert.assertNotNull(function.getClass().getName(), function.checkpointDependency());
            final LiveViewCheckpointRangePlan rangePlan = windowFactory.getCheckpointRangePlan();
            final LiveViewCheckpointRowsPlan rowsPlan = windowFactory.getCheckpointRowsPlan();
            return new Metadata(
                    function.checkpointFunctionIdentity(),
                    function.checkpointDependency(),
                    rangePlan,
                    rowsPlan,
                    LiveViewCheckpointFunctionCompiler.isDependencyComplete(
                            functions,
                            rangePlan != null,
                            rowsPlan != null,
                            false
                    )
            );
        } finally {
            sqlExecutionContext.setLiveViewCompile(false);
        }
    }

    private static class Metadata {
        private final LiveViewCheckpointDependency dependency;
        private final LiveViewCheckpointFunctionIdentity identity;
        // Whether the frame plans between them cover every window function, which is what
        // a repair checks before it bounds any of them. Computed without an anchor plan,
        // since this harness compiles the factory rather than the anchor window.
        private final boolean isDependencyComplete;
        private final LiveViewCheckpointRangePlan rangePlan;
        private final LiveViewCheckpointRowsPlan rowsPlan;

        private Metadata(
                LiveViewCheckpointFunctionIdentity identity,
                LiveViewCheckpointDependency dependency,
                LiveViewCheckpointRangePlan rangePlan,
                LiveViewCheckpointRowsPlan rowsPlan,
                boolean isDependencyComplete
        ) {
            this.identity = identity;
            this.dependency = dependency;
            this.rangePlan = rangePlan;
            this.rowsPlan = rowsPlan;
            this.isDependencyComplete = isDependencyComplete;
        }
    }
}
