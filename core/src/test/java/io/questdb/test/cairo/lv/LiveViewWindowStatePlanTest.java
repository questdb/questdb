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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewAccumulatorDescriptor;
import io.questdb.cairo.lv.LiveViewAccumulatorProjection;
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.lv.LiveViewWindowStatePlan;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * Coverage for the compiled fused window-state plan: which accumulator components a
 * live view's durable state is made of, which outputs project them, and which
 * functions the group leaves on their legacy roots.
 * <p>
 * The plan is compiled but not yet persisted - the seal still writes one root per
 * function - so these cases are the whole contract it has today. Two properties matter
 * most, because everything the window-state root will do rests on them:
 * <ul>
 *     <li><b>sharing is proved, not guessed.</b> Two calls collapse onto one component
 *     when their identities match outright, and a third folds onto that component when
 *     its whole image is provably a slice of it - but never across arguments or
 *     contribution predicates, whichever of the two relations is in play. The HDFC
 *     shape is the required negative control: {@code count(cod_acct_no)} must not read
 *     the counter inside {@code sum(amt_txn)}, because the two disagree on every row
 *     where exactly one column is null;</li>
 *     <li><b>the layout is deterministic.</b> Components are ordered by encoded
 *     identity, never by SELECT-list order, so reordering the projections of one view
 *     must produce a byte-identical manifest. A manifest that moved would silently
 *     reinterpret leaves an earlier manifest wrote, since a fused entry carries no
 *     component tags of its own.</li>
 * </ul>
 */
public class LiveViewWindowStatePlanTest extends AbstractLiveViewTest {

    private static final int ANCHOR_BYTES = Long.BYTES;
    private static final int COUNT_STATE_BYTES = Long.BYTES;
    private static final int SUM_STATE_BYTES = Double.BYTES + Long.BYTES;
    private static final int WELFORD_STATE_BYTES = 2 * Double.BYTES + Long.BYTES;

    @Test
    public void testBoundedWindowBesideTheAnchoredOneStaysResidual() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // The bounded ROWS window keeps sliding across bucket crossings, so the anchor
            // does not reset it and the runtime never collects it. It keeps its own root.
            assertPlan(
                    "select ts, sym, sum(x) over w as s, "
                            + "sum(x) over (partition by sym order by ts rows between 3 preceding and current row) as b "
                            + "from base window w as (partition by sym order by ts anchor daily '00:00')",
                    plan -> {
                        Assert.assertNotNull(plan);
                        Assert.assertEquals(1, plan.getComponentCount());
                        Assert.assertEquals(1, plan.getProjectionCount());
                        Assert.assertEquals(1, plan.getResidualFunctions().size());
                    }
            );
        });
    }

    @Test
    public void testContributionSemanticsSeparateTwoCounters() {
        // count over a DOUBLE counts finite values, exactly as a DOUBLE sum's own counter
        // does, so the two agree on infinities as well as on NULL.
        Assert.assertEquals(
                LiveViewAccumulatorDescriptor.CONTRIBUTION_FINITE_DOUBLE,
                LiveViewAccumulatorDescriptor.contributionKindFor(
                        LiveViewAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                        ColumnType.DOUBLE
                )
        );
        Assert.assertEquals(
                LiveViewAccumulatorDescriptor.CONTRIBUTION_TYPED_NOT_NULL,
                LiveViewAccumulatorDescriptor.contributionKindFor(
                        LiveViewAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                        ColumnType.SYMBOL
                )
        );
        // A DOUBLE sum has no meaning over a non-DOUBLE argument, and no count family
        // predicate is named for the types the first release does not carry.
        Assert.assertEquals(
                LiveViewAccumulatorDescriptor.CONTRIBUTION_NONE,
                LiveViewAccumulatorDescriptor.contributionKindFor(
                        LiveViewAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT,
                        ColumnType.LONG
                )
        );
        Assert.assertNull(LiveViewAccumulatorDescriptor.of(
                LiveViewAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                0,
                ColumnType.LONG
        ));

        // The same argument under two families is two components: one counts finite
        // doubles alongside their sum, the other only counts.
        final LiveViewAccumulatorDescriptor sumCount = component(
                LiveViewAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT,
                2,
                ColumnType.DOUBLE
        );
        final LiveViewAccumulatorDescriptor count = component(
                LiveViewAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                2,
                ColumnType.DOUBLE
        );
        Assert.assertFalse(sumCount.isSameIdentity(count));
        // And so is the same family over two different columns - the HDFC negative
        // control, in its smallest form.
        Assert.assertFalse(count.isSameIdentity(component(
                LiveViewAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                1,
                ColumnType.DOUBLE
        )));
    }

    @Test
    public void testCountOverTheSummedColumnDerivesFromTheSumsCounter() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // The acceptance shape: three calls, one 16-byte accumulator, a 24-byte fused
            // entry. sum and avg merge on identical identities; count folds onto the
            // counter beside their sum, which it may because both count the finite values
            // of the same column.
            assertPlan(
                    "select ts, sym, sum(x) over w as s, avg(x) over w as a, count(x) over w as c "
                            + "from base window w as (partition by sym order by ts anchor daily '00:00')",
                    plan -> {
                        Assert.assertNotNull(plan);
                        Assert.assertEquals(1, plan.getComponentCount());
                        Assert.assertEquals(3, plan.getProjectionCount());
                        Assert.assertEquals(ANCHOR_BYTES + SUM_STATE_BYTES, plan.getTotalInlineStateBytes());
                        Assert.assertEquals(
                                LiveViewAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT,
                                plan.getComponent(0).getFamily()
                        );
                        for (int i = 0; i < 3; i++) {
                            Assert.assertEquals(
                                    "every projection reads the one accumulator",
                                    0,
                                    plan.getProjection(i).getComponentIndex()
                            );
                        }

                        // The two that persist the component read the whole of it; the
                        // derived one reads the counter inside it and stops there.
                        final LiveViewAccumulatorProjection sum = plan.getProjection(0);
                        final LiveViewAccumulatorProjection count = plan.getProjection(2);
                        Assert.assertFalse(sum.isDerived());
                        Assert.assertFalse(plan.getProjection(1).isDerived());
                        Assert.assertTrue(count.isDerived());
                        Assert.assertEquals(sum.getComponentStateOffset(), sum.getFunctionStateOffset());
                        Assert.assertEquals(SUM_STATE_BYTES, sum.getFunctionStateLength());
                        Assert.assertEquals(count.getNonNullCountFieldOffset(), count.getFunctionStateOffset());
                        Assert.assertEquals(COUNT_STATE_BYTES, count.getFunctionStateLength());
                        Assert.assertEquals(
                                "the derived slice sits inside the host, not beside it",
                                count.getComponentStateOffset() + Double.BYTES,
                                count.getFunctionStateOffset()
                        );

                        // The count freezes eight bytes and the component is sixteen, so it
                        // could never write the image it reads. The contributor stays a
                        // function that persists the whole component.
                        Assert.assertSame(plan.getProjectionFunction(0), plan.getContributor(0));
                        Assert.assertNotSame(plan.getProjectionFunction(2), plan.getContributor(0));
                    }
            );

            // Which call comes first must not decide any of it: the fold reads the
            // component set, and a manifest that moved with SELECT-list order would make
            // reordering two outputs force a conversion seal.
            Assert.assertArrayEquals(
                    manifestOf("select ts, sym, sum(x) over w as s, count(x) over w as c "
                            + "from base window w as (partition by sym order by ts anchor daily '00:00')"),
                    manifestOf("select ts, sym, count(x) over w as c, sum(x) over w as s "
                            + "from base window w as (partition by sym order by ts anchor daily '00:00')")
            );
        });
    }

    @Test
    public void testCountWithNoSumBesideItKeepsItsOwnComponent() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // Nothing to fold onto: the counter is the whole durable state, and its own
            // family is what persists it. Eight inline bytes, not sixteen.
            assertPlan(
                    "select ts, sym, count(x) over w as c "
                            + "from base window w as (partition by sym order by ts anchor daily '00:00')",
                    plan -> {
                        Assert.assertNotNull(plan);
                        Assert.assertEquals(1, plan.getComponentCount());
                        Assert.assertEquals(
                                LiveViewAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                                plan.getComponent(0).getFamily()
                        );
                        Assert.assertEquals(ANCHOR_BYTES + COUNT_STATE_BYTES, plan.getTotalInlineStateBytes());
                        Assert.assertFalse(plan.getProjection(0).isDerived());
                        Assert.assertSame(plan.getProjectionFunction(0), plan.getContributor(0));
                    }
            );
            // And a sum over a different column is not something to fold onto either.
            assertPlan(
                    "select ts, sym, sum(y) over w as s, count(x) over w as c "
                            + "from base window w as (partition by sym order by ts anchor daily '00:00')",
                    plan -> {
                        Assert.assertNotNull(plan);
                        Assert.assertEquals(2, plan.getComponentCount());
                        Assert.assertEquals(
                                ANCHOR_BYTES + SUM_STATE_BYTES + COUNT_STATE_BYTES,
                                plan.getTotalInlineStateBytes()
                        );
                    }
            );
        });
    }

    @Test
    public void testDerivationIsProvedFromTheComponentIdentityAndNotTheArithmetic() {
        final LiveViewAccumulatorDescriptor sumCount = component(
                LiveViewAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT,
                2,
                ColumnType.DOUBLE
        );
        final LiveViewAccumulatorDescriptor count = component(
                LiveViewAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                2,
                ColumnType.DOUBLE
        );
        // A component always contains itself, at offset zero.
        Assert.assertEquals(0, sumCount.derivedStateOffset(sumCount));
        // And it contains the bare counter over its own argument, at the counter's own
        // field offset - which is the whole of what makes count(x) free beside sum(x).
        Assert.assertEquals(
                sumCount.getFieldOffset(LiveViewAccumulatorDescriptor.FIELD_NON_NULL_COUNT),
                sumCount.derivedStateOffset(count)
        );
        // Not the other way round: eight bytes do not contain sixteen.
        Assert.assertEquals(-1, count.derivedStateOffset(sumCount));
        // A different column is a different accumulator however alike the families read.
        Assert.assertEquals(-1, sumCount.derivedStateOffset(component(
                LiveViewAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                1,
                ColumnType.DOUBLE
        )));
        // And so is a counter over a differently typed argument: a SYMBOL count tests for
        // null where the DOUBLE accumulators test for finiteness, so the two disagree on
        // any infinity even before their columns do.
        Assert.assertEquals(-1, sumCount.derivedStateOffset(component(
                LiveViewAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                2,
                ColumnType.SYMBOL
        )));
    }

    @Test
    public void testCountStarJoinsAsARowCountAndNeverMergesWithACountOverAColumn() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // count(*) counts rows rather than an argument's non-null values. It has its
            // own family and joins the group under it, but the two counters stay apart:
            // they disagree on every row where the counted column is null, and an identity
            // that merged them would depend on the data rather than on the query.
            assertPlan(
                    "select ts, sym, count(*) over w as r, count(x) over w as c "
                            + "from base window w as (partition by sym order by ts anchor daily '00:00')",
                    plan -> {
                        Assert.assertNotNull(plan);
                        Assert.assertEquals(2, plan.getComponentCount());
                        Assert.assertEquals(2, plan.getProjectionCount());
                        Assert.assertEquals(0, plan.getResidualFunctions().size());
                        Assert.assertEquals(
                                ANCHOR_BYTES + 2 * COUNT_STATE_BYTES,
                                plan.getTotalInlineStateBytes()
                        );
                        Assert.assertFalse(plan.getProjection(0).isDerived());
                        Assert.assertFalse(plan.getProjection(1).isDerived());
                        Assert.assertNotEquals(
                                "a row count must not read a non-null count's counter",
                                plan.getProjection(0).getComponentIndex(),
                                plan.getProjection(1).getComponentIndex()
                        );
                    }
            );
            // Nor with the counter inside a sum, for the same reason and at the same
            // eight bytes: the fold needs containment, and a row count is not contained
            // in a counter that skips rows.
            assertPlan(
                    "select ts, sym, sum(x) over w as s, count(*) over w as c "
                            + "from base window w as (partition by sym order by ts anchor daily '00:00')",
                    plan -> {
                        Assert.assertNotNull(plan);
                        Assert.assertEquals(2, plan.getComponentCount());
                        Assert.assertEquals(
                                ANCHOR_BYTES + SUM_STATE_BYTES + COUNT_STATE_BYTES,
                                plan.getTotalInlineStateBytes()
                        );
                    }
            );
            // An expression argument is not a direct column reference, and SQL text equality
            // is not a proof that two expressions are the same accumulator. With nothing else
            // to group, the whole plan declines.
            assertPlan(
                    "select ts, sym, sum(x + 1) over w as s "
                            + "from base window w as (partition by sym order by ts anchor daily '00:00')",
                    Assert::assertNull
            );
        });
    }

    @Test
    public void testCountStarAndRowNumberShareOneRowCounter() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // Both keep the count of rows since the partition's last anchor crossing, and
            // after n rows both read n off it. One component, eight inline bytes, and the
            // second call costs nothing.
            assertPlan(
                    "select ts, sym, count(*) over w as c, row_number() over w as rn "
                            + "from base window w as (partition by sym order by ts anchor daily '00:00')",
                    plan -> {
                        Assert.assertNotNull(plan);
                        Assert.assertEquals(1, plan.getComponentCount());
                        Assert.assertEquals(2, plan.getProjectionCount());
                        Assert.assertEquals(0, plan.getResidualFunctions().size());
                        Assert.assertEquals(
                                LiveViewAccumulatorDescriptor.FAMILY_ROW_COUNT,
                                plan.getComponent(0).getFamily()
                        );
                        Assert.assertEquals(ANCHOR_BYTES + COUNT_STATE_BYTES, plan.getTotalInlineStateBytes());
                        // Neither is derived - each persists the whole component - so the
                        // contributor is decided by output position, and only it updates.
                        Assert.assertFalse(plan.getProjection(0).isDerived());
                        Assert.assertFalse(plan.getProjection(1).isDerived());
                        Assert.assertSame(plan.getProjectionFunction(0), plan.getContributor(0));
                    }
            );
            // A row_number() beside a count(x) is two components: one counts rows and the
            // other counts a column's non-null values.
            assertPlan(
                    "select ts, sym, row_number() over w as rn, count(x) over w as c "
                            + "from base window w as (partition by sym order by ts anchor daily '00:00')",
                    plan -> {
                        Assert.assertNotNull(plan);
                        Assert.assertEquals(2, plan.getComponentCount());
                        Assert.assertEquals(
                                ANCHOR_BYTES + 2 * COUNT_STATE_BYTES,
                                plan.getTotalInlineStateBytes()
                        );
                    }
            );
        });
    }

    @Test
    public void testTheFourWelfordCallsShareOneAccumulatorAndACountFoldsOntoIt() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // stddev_samp, stddev_pop, var_samp and var_pop are one implementation with two
            // flags flipped, and the flags decide only what is read off the state. Five
            // calls, one 24-byte accumulator, a 32-byte fused entry: the count folds onto
            // Welford's counter, which increments under the same isFinite test it would
            // have counted with on its own.
            assertPlan(
                    "select ts, sym, stddev_samp(x) over w as ss, stddev_pop(x) over w as sp, "
                            + "var_samp(x) over w as vs, var_pop(x) over w as vp, count(x) over w as c "
                            + "from base window w as (partition by sym order by ts anchor daily '00:00')",
                    plan -> {
                        Assert.assertNotNull(plan);
                        Assert.assertEquals(1, plan.getComponentCount());
                        Assert.assertEquals(5, plan.getProjectionCount());
                        Assert.assertEquals(0, plan.getResidualFunctions().size());
                        Assert.assertEquals(
                                LiveViewAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD,
                                plan.getComponent(0).getFamily()
                        );
                        Assert.assertEquals(
                                ANCHOR_BYTES + WELFORD_STATE_BYTES,
                                plan.getTotalInlineStateBytes()
                        );
                        for (int i = 0; i < 5; i++) {
                            Assert.assertEquals(
                                    "every projection reads the one accumulator",
                                    0,
                                    plan.getProjection(i).getComponentIndex()
                            );
                        }
                        Assert.assertEquals(
                                LiveViewAccumulatorProjection.PROJECTION_STDDEV_SAMP,
                                plan.getProjection(0).getKind()
                        );
                        Assert.assertEquals(
                                LiveViewAccumulatorProjection.PROJECTION_VAR_POP,
                                plan.getProjection(3).getKind()
                        );
                        // Only the count is derived, and its slice is the counter at the
                        // end of Welford's image rather than the whole of it.
                        final LiveViewAccumulatorProjection count = plan.getProjection(4);
                        Assert.assertFalse(plan.getProjection(0).isDerived());
                        Assert.assertTrue(count.isDerived());
                        Assert.assertEquals(COUNT_STATE_BYTES, count.getFunctionStateLength());
                        Assert.assertEquals(count.getNonNullCountFieldOffset(), count.getFunctionStateOffset());
                        Assert.assertEquals(
                                count.getComponentStateOffset() + 2 * Double.BYTES,
                                count.getFunctionStateOffset()
                        );
                        // The count freezes eight bytes and the component is twenty-four,
                        // so it could never write the image it reads.
                        Assert.assertSame(plan.getProjectionFunction(0), plan.getContributor(0));
                    }
            );
            // A Welford accumulator over one column and a sum over the same column stay two
            // components: a mean is not a sum, so neither contains the other.
            assertPlan(
                    "select ts, sym, stddev_samp(x) over w as ss, sum(x) over w as s "
                            + "from base window w as (partition by sym order by ts anchor daily '00:00')",
                    plan -> {
                        Assert.assertNotNull(plan);
                        Assert.assertEquals(2, plan.getComponentCount());
                        Assert.assertEquals(
                                ANCHOR_BYTES + WELFORD_STATE_BYTES + SUM_STATE_BYTES,
                                plan.getTotalInlineStateBytes()
                        );
                    }
            );
        });
    }

    @Test
    public void testAnArgumentlessFamilyIsIdentifiedByNothingElse() {
        Assert.assertFalse(LiveViewAccumulatorDescriptor.familyTakesArgument(
                LiveViewAccumulatorDescriptor.FAMILY_ROW_COUNT
        ));
        Assert.assertTrue(LiveViewAccumulatorDescriptor.familyTakesArgument(
                LiveViewAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD
        ));
        // A row count declared with an argument, and an argument-taking family declared
        // without one, are both incoherent rather than merely unusual.
        Assert.assertNull(LiveViewAccumulatorDescriptor.of(
                LiveViewAccumulatorDescriptor.FAMILY_ROW_COUNT,
                2,
                ColumnType.DOUBLE
        ));
        Assert.assertNull(LiveViewAccumulatorDescriptor.of(
                LiveViewAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                LiveViewAccumulatorDescriptor.NO_ARGUMENT_COLUMN_INDEX,
                ColumnType.UNDEFINED
        ));

        final LiveViewAccumulatorDescriptor rowCount = rowCountComponent();
        Assert.assertEquals(
                LiveViewAccumulatorDescriptor.CONTRIBUTION_EVERY_ROW,
                rowCount.getContributionKind()
        );
        Assert.assertEquals(COUNT_STATE_BYTES, rowCount.getStateLength());
        // Two row counts under one window are the same component whatever named them.
        Assert.assertTrue(rowCount.isSameIdentity(rowCountComponent()));
        // And a row count neither is, nor contains, nor sits inside a counter over a
        // column - in either direction, at the same eight bytes.
        final LiveViewAccumulatorDescriptor count = component(
                LiveViewAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                2,
                ColumnType.DOUBLE
        );
        Assert.assertFalse(rowCount.isSameIdentity(count));
        Assert.assertEquals(-1, rowCount.derivedStateOffset(count));
        Assert.assertEquals(-1, count.derivedStateOffset(rowCount));

        // Welford ends with the counter a count(x) over the same argument would keep, and
        // increments it under the same predicate, so the count's whole image is that field.
        final LiveViewAccumulatorDescriptor welford = component(
                LiveViewAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD,
                2,
                ColumnType.DOUBLE
        );
        Assert.assertEquals(WELFORD_STATE_BYTES, welford.getStateLength());
        Assert.assertEquals(
                welford.getFieldOffset(LiveViewAccumulatorDescriptor.FIELD_NON_NULL_COUNT),
                welford.derivedStateOffset(count)
        );
        Assert.assertEquals(2, welford.derivedSlotOffset(count));
        // A sum's image is not a run inside Welford's, however alike the two families
        // read: the second holds a running mean where the first holds a running sum.
        Assert.assertEquals(-1, welford.derivedStateOffset(component(
                LiveViewAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT,
                2,
                ColumnType.DOUBLE
        )));
        Assert.assertEquals(-1, rowCount.derivedStateOffset(welford));
    }

    @Test
    public void testDuplicateCallsMergeAndReorderingDoesNotMoveTheLayout() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // Two textually identical calls never reach the plan as two functions: the
            // optimiser computes the window once and selects it twice, so the plan sees one
            // projection. Its own merge is therefore not what handles this shape - it is
            // what handles sum(x) beside avg(x), which no rewrite above it collapses.
            assertPlan(
                    "select ts, sym, sum(x) over w as s1, sum(x) over w as s2 "
                            + "from base window w as (partition by sym order by ts anchor daily '00:00')",
                    plan -> {
                        Assert.assertNotNull(plan);
                        Assert.assertEquals(1, plan.getComponentCount());
                        Assert.assertEquals(1, plan.getProjectionCount());
                        Assert.assertEquals(ANCHOR_BYTES + SUM_STATE_BYTES, plan.getTotalInlineStateBytes());
                        // The contributor is chosen off the compiled view - the lowest output
                        // position - rather than off traversal order, and never reaches disk.
                        Assert.assertSame(plan.getProjectionFunction(0), plan.getContributor(0));
                    }
            );

            final byte[] declared = manifestOf(
                    "select ts, sym, sum(x) over w as s, count(sym) over w as c "
                            + "from base window w as (partition by sym order by ts anchor daily '00:00')"
            );
            final byte[] reordered = manifestOf(
                    "select ts, sym, count(sym) over w as c, sum(x) over w as s "
                            + "from base window w as (partition by sym order by ts anchor daily '00:00')"
            );
            Assert.assertArrayEquals(
                    "the manifest must not depend on SELECT-list order",
                    declared,
                    reordered
            );
            Assert.assertArrayEquals(
                    "recompiling one view must reproduce the manifest byte for byte",
                    declared,
                    manifestOf(
                            "select ts, sym, sum(x) over w as s, count(sym) over w as c "
                                    + "from base window w as (partition by sym order by ts anchor daily '00:00')"
                    )
            );
        });
    }

    @Test
    public void testHdfcShapeFusesTwoComponentsWithoutSharingACounter() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            assertPlan(hdfcSelect(), plan -> {
                Assert.assertNotNull(plan);
                Assert.assertEquals(2, plan.getComponentCount());
                Assert.assertEquals(2, plan.getProjectionCount());
                Assert.assertEquals(0, plan.getResidualFunctions().size());
                // 8-byte anchor + (sum, nonNullCount) + one counter, all inline, no refs.
                Assert.assertEquals(
                        ANCHOR_BYTES + SUM_STATE_BYTES + COUNT_STATE_BYTES,
                        plan.getTotalInlineStateBytes()
                );
                Assert.assertTrue(
                        plan.getTotalInlineStateBytes() <= LiveViewCheckpointContracts.MAX_INLINE_LEAF_STATE_BYTES
                );
                Assert.assertNotEquals(
                        "count(cod_acct_no) must not bind to the counter in sum(amt_txn)",
                        plan.getProjection(0).getComponentIndex(),
                        plan.getProjection(1).getComponentIndex()
                );
                // The anchor leads the payload and every component sits above it, in
                // ascending order, exactly covering the declared total.
                int expectedOffset = LiveViewWindowStatePlan.ANCHOR_STATE_OFFSET
                        + LiveViewWindowStatePlan.ANCHOR_STATE_BYTES;
                for (int i = 0, n = plan.getComponentCount(); i < n; i++) {
                    final int componentIndex = i;
                    final LiveViewAccumulatorProjection projection = projectionOn(plan, componentIndex);
                    Assert.assertEquals(expectedOffset, projection.getComponentStateOffset());
                    Assert.assertEquals(
                            expectedOffset + plan.getComponent(componentIndex).getFieldOffset(
                                    LiveViewAccumulatorDescriptor.FIELD_NON_NULL_COUNT
                            ),
                            projection.getNonNullCountFieldOffset()
                    );
                    expectedOffset += plan.getComponent(componentIndex).getStateLength();
                }
                Assert.assertEquals(plan.getTotalInlineStateBytes(), expectedOffset);
                Assert.assertEquals(plan.getTotalInlineStateBytes(), plan.getManifest().getTotalInlineStateBytes());
                Assert.assertEquals(2, plan.getManifest().getComponentCount());
            });
        });
    }

    @Test
    public void testLeafBudgetDeclinesAGroupThatDoesNotFit() {
        // Three (sum, nonNullCount) components plus the anchor is 56 bytes and fits; a
        // fourth takes it to 72 and the whole group falls back, because a window root is
        // complete or absent and the format has no combined overflow page yet.
        Assert.assertNotNull(buildSumGroup(3));
        Assert.assertEquals(
                ANCHOR_BYTES + 3 * SUM_STATE_BYTES,
                buildSumGroup(3).getTotalInlineStateBytes()
        );
        Assert.assertNull(buildSumGroup(4));
    }

    @Test
    public void testProjectionMustFitItsComponentFamily() {
        Assert.assertTrue(LiveViewAccumulatorProjection.isCompatible(
                LiveViewAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT,
                LiveViewAccumulatorProjection.PROJECTION_AVG
        ));
        // Arithmetically a count reads either family's counter; whether it may is decided
        // by the component identity the plan checks first, not here.
        Assert.assertTrue(LiveViewAccumulatorProjection.isCompatible(
                LiveViewAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT,
                LiveViewAccumulatorProjection.PROJECTION_COUNT
        ));
        // A bare counter carries no sum, so nothing can project one out of it.
        Assert.assertFalse(LiveViewAccumulatorProjection.isCompatible(
                LiveViewAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                LiveViewAccumulatorProjection.PROJECTION_SUM
        ));
        Assert.assertFalse(LiveViewAccumulatorProjection.isCompatible(
                LiveViewAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                LiveViewAccumulatorProjection.PROJECTION_AVG
        ));
        // Every family carries a counter, so a count reads any of them - subject, again,
        // to the identity test the plan applies first.
        Assert.assertTrue(LiveViewAccumulatorProjection.isCompatible(
                LiveViewAccumulatorDescriptor.FAMILY_ROW_COUNT,
                LiveViewAccumulatorProjection.PROJECTION_COUNT
        ));
        Assert.assertTrue(LiveViewAccumulatorProjection.isCompatible(
                LiveViewAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD,
                LiveViewAccumulatorProjection.PROJECTION_COUNT
        ));
        // A dispersion needs the squared deviations only Welford's state holds.
        Assert.assertTrue(LiveViewAccumulatorProjection.isCompatible(
                LiveViewAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD,
                LiveViewAccumulatorProjection.PROJECTION_VAR_SAMP
        ));
        Assert.assertFalse(LiveViewAccumulatorProjection.isCompatible(
                LiveViewAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT,
                LiveViewAccumulatorProjection.PROJECTION_STDDEV_POP
        ));
        Assert.assertFalse(LiveViewAccumulatorProjection.isCompatible(
                LiveViewAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD,
                LiveViewAccumulatorProjection.PROJECTION_SUM
        ));

        // A contributor whose declared image is not its family's width is declined
        // outright: the manifest would name a slice the runtime image does not fill, and
        // the leaf carries no length of its own to catch it.
        final LiveViewWindowStatePlan.Builder builder = new LiveViewWindowStatePlan.Builder();
        Assert.assertFalse(builder.addProjection(
                new WidthStub(COUNT_STATE_BYTES),
                component(LiveViewAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT, 0, ColumnType.DOUBLE),
                LiveViewAccumulatorProjection.PROJECTION_SUM,
                0,
                windowIdentity(),
                keyTypes()
        ));
        Assert.assertNull(builder.build());
    }

    @Test
    public void testSumAndAvgOverOneArgumentShareOneComponent() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            assertPlan(
                    "select ts, sym, sum(x) over w as s, avg(x) over w as a "
                            + "from base window w as (partition by sym order by ts anchor daily '00:00')",
                    plan -> {
                        Assert.assertNotNull(plan);
                        Assert.assertEquals(1, plan.getComponentCount());
                        Assert.assertEquals(2, plan.getProjectionCount());
                        Assert.assertEquals(
                                LiveViewAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT,
                                plan.getComponent(0).getFamily()
                        );
                        Assert.assertEquals(ANCHOR_BYTES + SUM_STATE_BYTES, plan.getTotalInlineStateBytes());
                        Assert.assertEquals(
                                LiveViewAccumulatorProjection.PROJECTION_SUM,
                                plan.getProjection(0).getKind()
                        );
                        Assert.assertEquals(
                                LiveViewAccumulatorProjection.PROJECTION_AVG,
                                plan.getProjection(1).getKind()
                        );
                        // Both read the same slice; only the arithmetic on top differs.
                        Assert.assertEquals(
                                plan.getProjection(0).getSumFieldOffset(),
                                plan.getProjection(1).getSumFieldOffset()
                        );
                        Assert.assertEquals(
                                plan.getProjection(0).getNonNullCountFieldOffset(),
                                plan.getProjection(1).getNonNullCountFieldOffset()
                        );
                    }
            );
        });
    }

    @Test
    public void testTheAnchorWindowAdoptsThePlanForTheHdfcShape() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tx (created_at timestamp, cod_acct_no symbol nocache index capacity 4, "
                    + "amt_txn double) timestamp(created_at) partition by hour wal");
            execute("insert into tx values ('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0)");
            drainWalQueue();
            execute("create live view lv flush every 100ms start from beginning as "
                    + "select created_at, cod_acct_no, sum(amt_txn) over w as cumulative_sum, "
                    + "count(cod_acct_no) over w as cumulative_count "
                    + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                execute("insert into tx values ('2026-01-01T11:00:10.000000Z', 'acct-2', 11.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull("live view 'lv' must be registered", instance);
                final LiveViewWindow window = instance.getAnchorWindow();
                Assert.assertNotNull("the anchored view must have built its window", window);
                final LiveViewWindowStatePlan plan = window.getCheckpointWindowStatePlan();
                Assert.assertNotNull("the window must adopt the compiled plan", plan);
                Assert.assertEquals(2, plan.getComponentCount());
                Assert.assertEquals(
                        ANCHOR_BYTES + SUM_STATE_BYTES + COUNT_STATE_BYTES,
                        plan.getTotalInlineStateBytes()
                );
                // The fused entry is keyed by the anchor map, so the plan is adopted only
                // while its components are keyed the same way.
                Assert.assertTrue(plan.isKeyLayoutCompatible(window.getPartitionKeyTypes()));

                // Adoption is a decision the window can reverse, and declining costs the view
                // only the fused root - every function is still on its legacy one today.
                Assert.assertFalse(window.bindCheckpointWindowStatePlan(null));
                Assert.assertNull(window.getCheckpointWindowStatePlan());
                Assert.assertTrue(window.bindCheckpointWindowStatePlan(plan));
            }
        });
    }

    /**
     * Builds a group of {@code componentCount} distinct 16-byte components, one per
     * argument column, so a case can push the fused layout past the leaf budget without
     * needing a view that declares that many accumulators.
     */
    private static LiveViewWindowStatePlan buildSumGroup(int componentCount) {
        final LiveViewWindowStatePlan.Builder builder = new LiveViewWindowStatePlan.Builder();
        for (int i = 0; i < componentCount; i++) {
            Assert.assertTrue(builder.addProjection(
                    new WidthStub(SUM_STATE_BYTES),
                    component(LiveViewAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT, i, ColumnType.DOUBLE),
                    LiveViewAccumulatorProjection.PROJECTION_SUM,
                    i,
                    windowIdentity(),
                    keyTypes()
            ));
        }
        return builder.build();
    }

    private static LiveViewAccumulatorDescriptor rowCountComponent() {
        final LiveViewAccumulatorDescriptor component = LiveViewAccumulatorDescriptor.of(
                LiveViewAccumulatorDescriptor.FAMILY_ROW_COUNT,
                LiveViewAccumulatorDescriptor.NO_ARGUMENT_COLUMN_INDEX,
                ColumnType.UNDEFINED
        );
        Assert.assertNotNull(component);
        return component;
    }

    private static LiveViewAccumulatorDescriptor component(int family, int argumentColumnIndex, int argumentColumnType) {
        final LiveViewAccumulatorDescriptor component = LiveViewAccumulatorDescriptor.of(
                family,
                argumentColumnIndex,
                argumentColumnType
        );
        Assert.assertNotNull(component);
        return component;
    }

    /**
     * Compiles {@code sql} the way a live view compiles it and hands the resulting plan
     * to {@code check}, with the factory still open so the plan's non-owning references
     * are live.
     */
    private static void assertPlan(String sql, PlanCheck check) throws Exception {
        sqlExecutionContext.setLiveViewCompile(true);
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
            // A projection wrapper can sit above the window factory - two calls sharing a
            // rendered expression are computed once and selected twice - so the search walks
            // the whole chain rather than unwrapping only QueryProgress.
            RecordCursorFactory root = factory;
            while (root != null && !(root instanceof WindowRecordCursorFactory)) {
                root = root.getBaseFactory();
            }
            Assert.assertNotNull(sql, root);
            check.run(((WindowRecordCursorFactory) root).getCheckpointWindowStatePlan());
        } finally {
            sqlExecutionContext.setLiveViewCompile(false);
        }
    }

    private static String hdfcSelect() {
        return "select ts, sym, sum(x) over w as cumulative_sum, count(sym) over w as cumulative_count "
                + "from base window w as (partition by sym order by ts anchor daily '00:00')";
    }

    private static ArrayColumnTypes keyTypes() {
        final ArrayColumnTypes types = new ArrayColumnTypes();
        types.add(ColumnType.STRING);
        return types;
    }

    private static byte[] manifestOf(String sql) throws Exception {
        final byte[][] out = new byte[1][];
        assertPlan(sql, plan -> {
            Assert.assertNotNull(plan);
            out[0] = plan.getManifest().getEncoded();
        });
        return out[0];
    }

    private static LiveViewAccumulatorProjection projectionOn(LiveViewWindowStatePlan plan, int componentIndex) {
        for (int i = 0, n = plan.getProjectionCount(); i < n; i++) {
            if (plan.getProjection(i).getComponentIndex() == componentIndex) {
                return plan.getProjection(i);
            }
        }
        throw new AssertionError("no projection reads component " + componentIndex);
    }

    private static byte[] windowIdentity() {
        return LiveViewWindowStatePlan.encodeWindowIdentity("w", "1:3:sym;", "1:2:ts:1;");
    }

    private void createBaseTable() throws Exception {
        execute("create table base (ts timestamp, sym symbol, x double, y double) "
                + "timestamp(ts) partition by day wal");
    }

    @FunctionalInterface
    private interface PlanCheck {
        void run(LiveViewWindowStatePlan plan);
    }

    /**
     * A map-less window function that declares one fixed width and nothing else, so a
     * case can drive the plan builder without a compiled view behind it.
     */
    private static final class WidthStub extends BaseWindowFunction {
        private final int declaredLength;

        private WidthStub(int declaredLength) {
            super(null);
            this.declaredLength = declaredLength;
            setCheckpointCompilerMetadata(
                    new LiveViewCheckpointFunctionIdentity("w", "width_stub()", 0, "", "ts asc", "width-stub-v1"),
                    new LiveViewCheckpointDependency(
                            LiveViewCheckpointContracts.DependencyKind.FIXED_ANCHOR_SEGMENT,
                            "",
                            "ts asc",
                            Long.MIN_VALUE,
                            0,
                            Long.MIN_VALUE,
                            ColumnType.TIMESTAMP,
                            false,
                            true,
                            true,
                            LiveViewCheckpointDependency.StructuralConvergence.EXACT,
                            LiveViewCheckpointDependency.NumericConvergence.EXACT
                    )
            );
        }

        @Override
        public int checkpointStateFixedLength() {
            return declaredLength;
        }

        @Override
        public String getName() {
            return "width_stub";
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public int getType() {
            return ColumnType.DOUBLE;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }
    }
}
