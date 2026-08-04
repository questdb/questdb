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

package io.questdb.test.griffin.engine.window;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewWindowStatePlan;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.window.WindowAccumulatorDescriptor;
import io.questdb.griffin.engine.window.WindowAccumulatorPlan;
import io.questdb.griffin.engine.window.WindowAccumulatorProjection;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * The window Map groups an ordinary streaming query compiles, end to end from SQL.
 * <p>
 * Nothing binds them yet - the cursor still drives every function through the private map it
 * owns - so these cases are the whole of what the groups promise today, and they are the
 * promises everything built on them later rests on:
 * <ul>
 *     <li><b>sharing is proved, not guessed.</b> Two calls collapse onto one component when
 *     their identities match outright, and a third folds onto that component when its whole
 *     state is provably a slice of it - but never across arguments or contribution
 *     predicates. {@code sum(x)} beside {@code count(y)} is the required negative control:
 *     one map, two counters, because the two disagree on every row where exactly one column
 *     is null;</li>
 *     <li><b>the layout is deterministic.</b> Components are ordered by identity and never by
 *     SELECT-list order, so reordering the outputs of one query must not move a slot;</li>
 *     <li><b>the group is the exact window.</b> Two spellings of one window are one group and
 *     two windows differing anywhere are two, whatever they are named.</li>
 * </ul>
 * The identity's own discriminations - including the ones a streaming query cannot reach,
 * since the fast path admits only ZERO_PASS functions - are {@code WindowMapSpecTest}'s.
 */
public class WindowAccumulatorPlanTest extends AbstractCairoTest {

    @Test
    public void testAGuardedCountNeedsAnUnguardedRowCountToJoin() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // No row count in the group, so count(k) is an ordinary non-null count over its
            // own argument and keeps a component of its own. The guard exists to correct a
            // row count somebody else maintains; with nobody maintaining one there is
            // nothing to correct and no counter to share.
            assertPlans("select ts, sum(x) over w, count(k) over w from base " + window(), plans -> {
                final WindowAccumulatorPlan plan = onlyPlan(plans);
                Assert.assertEquals(2, plan.getComponentCount());
                Assert.assertEquals(2, plan.getProjectionCount());
                for (int i = 0; i < plan.getProjectionCount(); i++) {
                    Assert.assertFalse(plan.getProjection(i).isPartitionKeyGuarded());
                }
            });
        });
    }

    @Test
    public void testALiveViewCompileProducesNoGroup() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // A compatible live-view function is already owned by LiveViewWindow through the
            // fused window-state plan. Compiling a second owner over the same accumulators
            // would be two sources of truth for one piece of state, so the generic compiler
            // stays out of a live-view compile entirely.
            sqlExecutionContext.setLiveViewCompile(true);
            try {
                assertPlans(
                        "select ts, k, sum(x) over w, avg(x) over w, count(x) over w from base "
                                + "window w as (partition by k order by ts anchor daily '00:00')",
                        Assert::assertNull
                );
            } finally {
                sqlExecutionContext.setLiveViewCompile(false);
            }
        });
    }

    @Test
    public void testAnExpressionPartitionKeyProducesNoGroup() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // No canonical, type-resolved fingerprint proves two compiled expressions
            // equivalent, and the rendered SQL is not that proof - so an expression key
            // declines in this release and the functions keep the maps they have today.
            assertPlans(
                    "select ts, sum(x) over w, avg(x) over w from base "
                            + "window w as (partition by concat(k, 'z') order by ts "
                            + "rows between unbounded preceding and current row)",
                    Assert::assertNull
            );
        });
    }

    @Test
    public void testAnUnpartitionedWindowProducesNoGroup() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // Such a function keeps its state in scalar fields and owns no map at all, so a
            // group would remove nothing and add a probe.
            assertPlans(
                    "select ts, sum(x) over w, avg(x) over w from base "
                            + "window w as (order by ts rows between unbounded preceding and current row)",
                    Assert::assertNull
            );
        });
    }

    @Test
    public void testCountOverThePartitionKeyReadsTheRowCount() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // Every row of a partition carries the same k, so count(k) is the partition's
            // row count wherever k is present and zero where it is not: a guarded reading of
            // the component count(*) and row_number() already maintain, rather than a
            // counter of its own. The guarded projection deliberately comes first in the
            // SELECT list, because whether it fuses must not depend on that.
            assertPlans(
                    "select ts, count(k) over w, count(*) over w, row_number() over w from base " + window(),
                    plans -> {
                        final WindowAccumulatorPlan plan = onlyPlan(plans);
                        Assert.assertEquals(1, plan.getComponentCount());
                        Assert.assertEquals(3, plan.getProjectionCount());
                        Assert.assertEquals(
                                WindowAccumulatorDescriptor.FAMILY_ROW_COUNT,
                                plan.getComponent(0).getFamily()
                        );
                        Assert.assertEquals(1, plan.getSlotCount());
                        // The guarded one is the count(k) at output position 1, and the
                        // contributor is the count(*) that follows it - the lowest output
                        // position among the projections that keep a true row count.
                        Assert.assertTrue(projectionAt(plan, 1).isPartitionKeyGuarded());
                        Assert.assertEquals(2, plan.getProjection(plan.getContributorIndex(0)).getOutputPosition());
                    }
            );
        });
    }

    @Test
    public void testDifferentWindowsDoNotJoin() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // A different partition column is a different key domain.
            assertPlans(
                    "select ts, sum(x) over w1, avg(x) over w1, sum(x) over w2, avg(x) over w2 from base "
                            + "window w1 as (partition by k order by ts rows between unbounded preceding and current row), "
                            + "w2 as (partition by k2 order by ts rows between unbounded preceding and current row)",
                    plans -> {
                        Assert.assertNotNull(plans);
                        Assert.assertEquals(2, plans.size());
                        Assert.assertEquals(1, plans.getQuick(0).getComponentCount());
                        Assert.assertEquals(2, plans.getQuick(0).getProjectionCount());
                        Assert.assertEquals(1, plans.getQuick(1).getComponentCount());
                        Assert.assertEquals(2, plans.getQuick(1).getProjectionCount());
                        Assert.assertFalse(
                                plans.getQuick(0).getSpec().isSameSpec(plans.getQuick(1).getSpec())
                        );
                    }
            );
            // A different frame over one key domain is a different traversal, so the two
            // still keep separate maps even though the partitions coincide.
            assertPlans(
                    "select ts, sum(x) over w1, avg(x) over w1, sum(x) over w2, avg(x) over w2 from base "
                            + "window w1 as (partition by k order by ts rows between unbounded preceding and current row), "
                            + "w2 as (partition by k order by ts rows between 3 preceding and current row)",
                    plans -> {
                        Assert.assertNotNull(plans);
                        // Both windows form a group and neither pair reaches the other's: a
                        // cumulative (sum, count) and a bounded frame's are two state shapes, and
                        // the group they belong to is what says so - the components never meet to
                        // be compared.
                        Assert.assertEquals(2, plans.size());
                        Assert.assertEquals(2, plans.getQuick(0).getProjectionCount());
                        Assert.assertEquals(2, plans.getQuick(1).getProjectionCount());
                        Assert.assertNotEquals(
                                "one group is the bounded one",
                                plans.getQuick(0).getComponent(0).isRingBacked(),
                                plans.getQuick(1).getComponent(0).isRingBacked()
                        );
                        Assert.assertFalse(
                                plans.getQuick(0).getSpec().isSameSpec(plans.getQuick(1).getSpec())
                        );
                    }
            );
        });
    }

    @Test
    public void testOneFusibleFunctionIsNotAGroup() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // A single-function group moves a map rather than removing one, so binding a
            // runtime through it would cost the query an abstraction and buy it nothing.
            assertPlans("select ts, sum(x) over w from base " + window(), Assert::assertNull);
            // ... and neither does a second function that cannot join it.
            assertPlans(
                    "select ts, sum(x) over w, first_value(x) over w from base " + window(),
                    Assert::assertNull
            );
        });
    }

    @Test
    public void testSelectListOrderMovesNothing() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            final String[] layouts = new String[2];
            assertPlans(
                    "select ts, sum(x) over w, count(y) over w, avg(x) over w, count(x) over w from base " + window(),
                    plans -> layouts[0] = componentLayout(onlyPlan(plans))
            );
            assertPlans(
                    "select ts, count(x) over w, avg(x) over w, count(y) over w, sum(x) over w from base " + window(),
                    plans -> layouts[1] = componentLayout(onlyPlan(plans))
            );
            // Same components, same order, same slot bases. A layout that followed the
            // SELECT list would make two spellings of one query two different maps, and
            // would move a slot under a live view's persisted manifest once step 5 has the
            // two plans sharing this builder.
            //
            // Which of two equivalent functions maintains a component does move with the
            // SELECT list, and is meant to: the rule is the lowest output position among the
            // candidates that keep the whole component, so reordering sum and avg swaps
            // them. What the reorder must not touch is the layout, and nothing persists the
            // contributor. That it is always an honest one is assertContributorsAreHonest's,
            // which every case here runs through.
            Assert.assertEquals(layouts[0], layouts[1]);
        });
    }

    @Test
    public void testSumAndCountOverDifferentArgumentsShareAMapButNotACounter() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // The required negative control. sum(x) counts finite x values and count(y)
            // counts non-null y values, and those disagree on every row where exactly one is
            // absent - so the key and the lookup fuse and the counters do not.
            assertPlans("select ts, sum(x) over w, count(y) over w from base " + window(), plans -> {
                final WindowAccumulatorPlan plan = onlyPlan(plans);
                Assert.assertEquals(2, plan.getComponentCount());
                Assert.assertEquals(2, plan.getProjectionCount());
                Assert.assertEquals(3, plan.getSlotCount());
                Assert.assertEquals(0, plan.getSlotPrefix());
                for (int i = 0; i < plan.getProjectionCount(); i++) {
                    Assert.assertFalse(plan.getProjection(i).isDerived());
                }
            });
        });
    }

    @Test
    public void testSumAvgCountOverOneArgumentIsOneComponent() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            assertPlans(
                    "select ts, sum(x) over w, avg(x) over w, count(x) over w from base " + window(),
                    plans -> {
                        final WindowAccumulatorPlan plan = onlyPlan(plans);
                        Assert.assertEquals(1, plan.getComponentCount());
                        Assert.assertEquals(3, plan.getProjectionCount());
                        Assert.assertEquals(
                                WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT,
                                plan.getComponent(0).getFamily()
                        );
                        // Two slots for three outputs: sum and avg merge outright, and the
                        // count folds onto the counter the pair already keeps.
                        Assert.assertEquals(2, plan.getSlotCount());
                        Assert.assertEquals(0, plan.getComponentSlotBase(0));
                        final ArrayColumnTypes types = new ArrayColumnTypes();
                        plan.buildMapValueTypes(types);
                        Assert.assertEquals(2, types.getColumnCount());
                        Assert.assertEquals(ColumnType.DOUBLE, types.getColumnType(0));
                        Assert.assertEquals(ColumnType.LONG, types.getColumnType(1));
                        // The sum is the lowest output position among the two that keep the
                        // whole component, so it is the one that updates it.
                        Assert.assertEquals(1, plan.getProjection(plan.getContributorIndex(0)).getOutputPosition());
                        Assert.assertFalse(projectionAt(plan, 1).isDerived());
                        Assert.assertFalse(projectionAt(plan, 2).isDerived());
                        Assert.assertTrue(projectionAt(plan, 3).isDerived());
                    }
            );
        });
    }

    @Test
    public void testTheDecimalExtremumFamiliesKeepAWidthEach() throws Exception {
        assertMemoryLeak(() -> {
            createDecimalBaseTable();
            // A DECIMAL extremum keeps its argument's own payload, so this group is where the
            // fused value stops being a list of 64-bit words: the two narrow calls take a LONG
            // slot each and the wide one takes a DECIMAL128, in the middle of the value rather
            // than at either end of it.
            //
            // Nothing merges here either. max and min over one column are two directions of
            // one reading, two widths of one direction are two states, and a count over the
            // same column keeps a counter no extremum carries - so four calls are four
            // components and the count is not derived.
            assertPlans(
                    "select ts, max(d64) over w, min(d64) over w, max(d128) over w, count(d64) over w "
                            + "from dbase " + window(),
                    plans -> {
                        final WindowAccumulatorPlan plan = onlyPlan(plans);
                        Assert.assertEquals(4, plan.getComponentCount());
                        Assert.assertEquals(4, plan.getProjectionCount());
                        Assert.assertEquals(4, plan.getSlotCount());
                        // Canonical order is by family id, so the counter leads and the two
                        // DECIMAL_MAX components follow in argument order, the DECIMAL_MIN last.
                        Assert.assertEquals(
                                WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                                plan.getComponent(0).getFamily()
                        );
                        Assert.assertEquals(
                                WindowAccumulatorDescriptor.FAMILY_DECIMAL_MAX,
                                plan.getComponent(1).getFamily()
                        );
                        Assert.assertEquals(
                                WindowAccumulatorDescriptor.FAMILY_DECIMAL_MAX,
                                plan.getComponent(2).getFamily()
                        );
                        Assert.assertEquals(
                                WindowAccumulatorDescriptor.FAMILY_DECIMAL_MIN,
                                plan.getComponent(3).getFamily()
                        );
                        Assert.assertTrue(
                                "the two DECIMAL_MAX components must be ordered by argument",
                                plan.getComponent(2).getArgumentColumnIndex()
                                        > plan.getComponent(1).getArgumentColumnIndex()
                        );
                        final ArrayColumnTypes types = new ArrayColumnTypes();
                        plan.buildMapValueTypes(types);
                        Assert.assertEquals(4, types.getColumnCount());
                        Assert.assertEquals(ColumnType.LONG, types.getColumnType(0));
                        Assert.assertEquals(ColumnType.LONG, types.getColumnType(1));
                        Assert.assertEquals(ColumnType.DECIMAL128, types.getColumnType(2));
                        Assert.assertEquals(ColumnType.LONG, types.getColumnType(3));
                        for (int output = 1; output <= 4; output++) {
                            Assert.assertFalse(
                                    "output " + output + " should keep its own component",
                                    projectionAt(plan, output).isDerived()
                            );
                        }
                    }
            );
        });
    }

    @Test
    public void testTheExtremumFamiliesNeitherMergeNorFold() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // Five extremum calls and an accumulating pair over one window. Nothing about an
            // extremum can be read out of anything else, and nothing out of it, so every one
            // of them keeps a slot: max and min over one column are two directions of one
            // reading and merge no more than max over two columns do, and a sum's first slot
            // is a running total rather than the largest thing ever added to it. What does
            // still fold is the count onto the sum's counter, which is what says admitting a
            // family left the proved relations alone.
            //
            // ts is here for the other half of the split: the extremum families are separated
            // by the state's type as well as by direction, and a timestamp argument keeps a
            // raw 64-bit word where x keeps a DOUBLE.
            assertPlans(
                    "select ts, max(x) over w, min(x) over w, max(y) over w, max(ts) over w, "
                            + "sum(x) over w, count(x) over w from base " + window(),
                    plans -> {
                        final WindowAccumulatorPlan plan = onlyPlan(plans);
                        Assert.assertEquals(5, plan.getComponentCount());
                        Assert.assertEquals(6, plan.getProjectionCount());
                        Assert.assertEquals(6, plan.getSlotCount());
                        // Canonical order is by family id first, so the (sum, count) pair leads
                        // and the four extrema follow it in family order, ties broken by the
                        // argument column. The two DOUBLE_MAX components are the tie: they sit
                        // adjacent, in argument order, and the x one is also the argument the
                        // sum and the DOUBLE_MIN carry.
                        Assert.assertEquals(
                                WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT,
                                plan.getComponent(0).getFamily()
                        );
                        Assert.assertEquals(
                                WindowAccumulatorDescriptor.FAMILY_DOUBLE_MAX,
                                plan.getComponent(1).getFamily()
                        );
                        Assert.assertEquals(
                                WindowAccumulatorDescriptor.FAMILY_DOUBLE_MAX,
                                plan.getComponent(2).getFamily()
                        );
                        Assert.assertEquals(
                                WindowAccumulatorDescriptor.FAMILY_DOUBLE_MIN,
                                plan.getComponent(3).getFamily()
                        );
                        Assert.assertEquals(
                                WindowAccumulatorDescriptor.FAMILY_LONG_MAX,
                                plan.getComponent(4).getFamily()
                        );
                        final int xColumn = plan.getComponent(0).getArgumentColumnIndex();
                        Assert.assertEquals(xColumn, plan.getComponent(1).getArgumentColumnIndex());
                        Assert.assertEquals(xColumn, plan.getComponent(3).getArgumentColumnIndex());
                        Assert.assertTrue(
                                "the two DOUBLE_MAX components must be ordered by argument",
                                plan.getComponent(2).getArgumentColumnIndex() > xColumn
                        );
                        final ArrayColumnTypes types = new ArrayColumnTypes();
                        plan.buildMapValueTypes(types);
                        Assert.assertEquals(6, types.getColumnCount());
                        Assert.assertEquals(ColumnType.DOUBLE, types.getColumnType(0));
                        Assert.assertEquals(ColumnType.LONG, types.getColumnType(1));
                        Assert.assertEquals(ColumnType.DOUBLE, types.getColumnType(2));
                        Assert.assertEquals(ColumnType.DOUBLE, types.getColumnType(3));
                        Assert.assertEquals(ColumnType.DOUBLE, types.getColumnType(4));
                        // The 64-bit extremum's slot is a LONG whatever unit the timestamp
                        // subclass hands its own answer back in.
                        Assert.assertEquals(ColumnType.LONG, types.getColumnType(5));
                        for (int output = 1; output <= 5; output++) {
                            Assert.assertFalse(
                                    "output " + output + " should keep its own component",
                                    projectionAt(plan, output).isDerived()
                            );
                        }
                        Assert.assertTrue(projectionAt(plan, 6).isDerived());
                        Assert.assertEquals(1, projectionAt(plan, 6).getNonNullCountSlot());
                    }
            );
        });
    }

    @Test
    public void testTheRingBackedFamiliesMergeWithinAFrameAndNeverAcrossOne() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // A bounded ROWS window carrying both ring-backed families twice over. The merge is
            // by identity as everywhere else - sum(x) and avg(x) are one component, and so are
            // the two count(x) calls if a query writes them - and what does not happen is a fold:
            // count(x)'s answer is the counter sum(x) keeps, and it still gets a component of its
            // own, because its state continues into a ring of its own shape.
            assertPlans(
                    "select ts, sum(x) over w, avg(x) over w, count(x) over w, count(y) over w "
                            + "from base " + rowsFrameWindow(),
                    plans -> {
                        final WindowAccumulatorPlan plan = onlyPlan(plans);
                        Assert.assertEquals(3, plan.getComponentCount());
                        Assert.assertEquals(4, plan.getProjectionCount());
                        // [sum, count, ringIndex, ringOffset] then a three-slot counter per
                        // argument: the layout is canonical by family id, and the sum family's is
                        // the lower.
                        Assert.assertEquals(10, plan.getSlotCount());
                        Assert.assertEquals(
                                WindowAccumulatorDescriptor.FAMILY_DOUBLE_ROWS_SUM_COUNT,
                                plan.getComponent(0).getFamily()
                        );
                        Assert.assertEquals(
                                WindowAccumulatorDescriptor.FAMILY_ROWS_NON_NULL_COUNT,
                                plan.getComponent(1).getFamily()
                        );
                        Assert.assertEquals(
                                WindowAccumulatorDescriptor.FAMILY_ROWS_NON_NULL_COUNT,
                                plan.getComponent(2).getFamily()
                        );
                        final int xColumn = plan.getComponent(0).getArgumentColumnIndex();
                        Assert.assertEquals(xColumn, plan.getComponent(1).getArgumentColumnIndex());
                        Assert.assertTrue(
                                "the two counters must be ordered by argument",
                                plan.getComponent(2).getArgumentColumnIndex() > xColumn
                        );
                        Assert.assertEquals(0, plan.getComponentSlotBase(0));
                        Assert.assertEquals(4, plan.getComponentSlotBase(1));
                        Assert.assertEquals(7, plan.getComponentSlotBase(2));
                        final ArrayColumnTypes types = new ArrayColumnTypes();
                        plan.buildMapValueTypes(types);
                        Assert.assertEquals(10, types.getColumnCount());
                        Assert.assertEquals(ColumnType.DOUBLE, types.getColumnType(0));
                        for (int slot = 1; slot < 10; slot++) {
                            // Every other slot is a 64-bit word, the two ring addresses included.
                            Assert.assertEquals("slot " + slot, ColumnType.LONG, types.getColumnType(slot));
                        }
                        // No output reads a component wider than its own function's.
                        for (int output = 1; output <= 4; output++) {
                            Assert.assertFalse(
                                    "output " + output + " should keep its own component",
                                    projectionAt(plan, output).isDerived()
                            );
                        }
                        Assert.assertTrue(plan.getComponent(0).isRingBacked());
                        Assert.assertTrue(plan.getComponent(1).isRingBacked());
                    }
            );
            // The same calls over a cumulative frame and a bounded one. Two groups: a component
            // only ever merges inside a group and the frame is part of the group's identity, so
            // the ring-backed state and the cumulative one cannot meet however alike the calls
            // look - and the cumulative pair still folds its count where the bounded pair does
            // not.
            assertPlans(
                    "select ts, sum(x) over w, avg(x) over w, count(x) over w, "
                            + "sum(x) over c, avg(x) over c, count(x) over c from base "
                            + "window w as (partition by k order by ts rows between 3 preceding and current row), "
                            + "c as (partition by k order by ts rows between unbounded preceding and current row)",
                    plans -> {
                        Assert.assertNotNull(plans);
                        Assert.assertEquals(2, plans.size());
                        int bounded = 0;
                        int cumulative = 0;
                        for (int i = 0; i < 2; i++) {
                            final WindowAccumulatorPlan plan = plans.getQuick(i);
                            Assert.assertEquals(3, plan.getProjectionCount());
                            if (plan.getComponent(0).isRingBacked()) {
                                bounded++;
                                Assert.assertEquals(2, plan.getComponentCount());
                                Assert.assertEquals(7, plan.getSlotCount());
                            } else {
                                cumulative++;
                                Assert.assertEquals(1, plan.getComponentCount());
                                Assert.assertEquals(2, plan.getSlotCount());
                            }
                        }
                        Assert.assertEquals(1, bounded);
                        Assert.assertEquals(1, cumulative);
                    }
            );
        });
    }

    @Test
    public void testTheGenericAndLiveViewPlansAgreeOnTheAnchoredShapes() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // The two builders decide the same things from the same descriptor tables, and
            // step 5 makes the live-view one compose this plan rather than repeat it. Until
            // then, the drift these compare is what a persisted manifest would move: same
            // components, same canonical order, same folds, same contributor - with the
            // live-view layout shifted by the anchor slots its own value carries.
            //
            // The anchor clause is what makes the shape a live view's and contributes
            // nothing to the component identities, so the two SELECTs below differ only in
            // the frame's spelling.
            assertPlansAgree("sum(x) over w, count(k) over w");
            assertPlansAgree("sum(x) over w, avg(x) over w, count(x) over w");
            assertPlansAgree("count(*) over w, row_number() over w, count(k) over w");
        });
    }

    @Test
    public void testTheWelfordFamilyIsOneComponent() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // Four dispersion projections differ only in the arithmetic they read off
            // (m2, nonNullCount), and a count over the same argument reads the counter
            // Welford's accumulator keeps behind its running mean.
            assertPlans(
                    "select ts, stddev_samp(x) over w, stddev_pop(x) over w, var_samp(x) over w, "
                            + "var_pop(x) over w, count(x) over w from base " + window(),
                    plans -> {
                        final WindowAccumulatorPlan plan = onlyPlan(plans);
                        Assert.assertEquals(1, plan.getComponentCount());
                        Assert.assertEquals(5, plan.getProjectionCount());
                        Assert.assertEquals(
                                WindowAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD,
                                plan.getComponent(0).getFamily()
                        );
                        Assert.assertEquals(3, plan.getSlotCount());
                        Assert.assertTrue(projectionAt(plan, 5).isDerived());
                    }
            );
        });
    }

    @Test
    public void testTwoSpellingsOfOneWindowAreOneGroup() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // A named window and an inline one that resolves to the same specification, plus
            // a second reference to the name. Names and SQL rendering are not semantic
            // identity, and neither is the absence of a name.
            assertPlans(
                    "select ts, sum(x) over w, "
                            + "count(y) over (partition by k order by ts rows between unbounded preceding and current row), "
                            + "avg(x) over w from base " + window(),
                    plans -> {
                        final WindowAccumulatorPlan plan = onlyPlan(plans);
                        Assert.assertEquals(2, plan.getComponentCount());
                        Assert.assertEquals(3, plan.getProjectionCount());
                    }
            );
        });
    }

    /**
     * Compiles {@code sql} and hands the window Map groups it produced to {@code check},
     * with the factory still open so the groups' non-owning references are live.
     */
    private static void assertPlans(String sql, PlanCheck check) throws Exception {
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
            final ObjList<WindowAccumulatorPlan> plans =
                    ((WindowRecordCursorFactory) root).getWindowAccumulatorPlans();
            if (plans != null) {
                for (int i = 0, n = plans.size(); i < n; i++) {
                    assertContributorsAreHonest(plans.getQuick(i));
                }
            }
            check.run(plans);
        }
    }

    /**
     * No component may be left maintained by a projection that does not keep the whole of
     * it: a derived one keeps a narrower state, and a guarded one keeps a different number
     * on the NULL-key partition. Asserted for every group every case compiles, because it is
     * the one property whose failure is silent - the group would still have a contributor
     * and would still update something.
     */
    private static void assertContributorsAreHonest(WindowAccumulatorPlan plan) {
        for (int i = 0, n = plan.getComponentCount(); i < n; i++) {
            final WindowAccumulatorProjection contributor = plan.getProjection(plan.getContributorIndex(i));
            Assert.assertEquals(i, contributor.getComponentIndex());
            Assert.assertFalse("component " + i + " has a derived contributor", contributor.isDerived());
            Assert.assertFalse("component " + i + " has a guarded contributor", contributor.isPartitionKeyGuarded());
            Assert.assertSame(plan.getContributor(i), plan.getProjectionFunction(plan.getContributorIndex(i)));
        }
    }

    /**
     * Compiles {@code outputs} twice - once as an ordinary streaming query and once as the
     * live view whose fused state plan is the reference implementation - and requires the
     * two to describe the same components in the same order, with the same folds and the
     * same contributors.
     */
    private static void assertPlansAgree(String outputs) throws Exception {
        final String[] generic = new String[1];
        assertPlans(
                "select ts, " + outputs + " from base " + window(),
                plans -> generic[0] = layout(onlyPlan(plans))
        );
        sqlExecutionContext.setLiveViewCompile(true);
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = select(
                     compiler,
                     "select ts, " + outputs + " from base "
                             + "window w as (partition by k order by ts anchor daily '00:00')",
                     sqlExecutionContext
             )) {
            RecordCursorFactory root = factory;
            while (root != null && !(root instanceof WindowRecordCursorFactory)) {
                root = root.getBaseFactory();
            }
            Assert.assertNotNull(outputs, root);
            final WindowRecordCursorFactory windowFactory = (WindowRecordCursorFactory) root;
            // Belt and braces on the exclusion: a live-view compile produces the fused plan
            // and no generic group, which is also what testALiveViewCompileProducesNoGroup
            // asserts on its own.
            Assert.assertNull(windowFactory.getWindowAccumulatorPlans());
            final LiveViewWindowStatePlan plan = windowFactory.getCheckpointWindowStatePlan();
            Assert.assertNotNull(outputs, plan);
            Assert.assertEquals(outputs, generic[0], liveViewLayout(plan));
        } finally {
            sqlExecutionContext.setLiveViewCompile(false);
        }
    }

    /**
     * Renders the layout alone - the components in canonical order with their slot bases -
     * which is the part that must not depend on the SELECT list. Deliberately a rendering
     * rather than a field-by-field walk: a part of the decision added later and not
     * asserted is the way this stops testing what it says it tests.
     */
    private static String componentLayout(WindowAccumulatorPlan plan) {
        final StringBuilder sink = new StringBuilder();
        for (int i = 0, n = plan.getComponentCount(); i < n; i++) {
            final WindowAccumulatorDescriptor component = plan.getComponent(i);
            sink.append("component ").append(i)
                    .append(" family=").append(component.getFamily())
                    .append(" contribution=").append(component.getContributionKind())
                    .append(" arg=").append(component.getArgumentColumnIndex())
                    .append(':').append(component.getArgumentColumnType())
                    .append(" slotBase=").append(plan.getComponentSlotBase(i) - plan.getSlotPrefix())
                    .append(" slots=").append(component.getSlotCount())
                    .append('\n');
        }
        return sink.toString();
    }

    /**
     * The whole of a group's decision: {@link #componentLayout} plus the contributor choice
     * and every projection's binding. What two compilers of one shape must agree on.
     */
    private static String layout(WindowAccumulatorPlan plan) {
        final StringBuilder sink = new StringBuilder(componentLayout(plan));
        for (int i = 0, n = plan.getComponentCount(); i < n; i++) {
            sink.append("contributor on ").append(i)
                    .append(" out=").append(plan.getProjection(plan.getContributorIndex(i)).getOutputPosition())
                    .append('\n');
        }
        for (int i = 0, n = plan.getProjectionCount(); i < n; i++) {
            final WindowAccumulatorProjection projection = plan.getProjection(i);
            sink.append("projection out=").append(projection.getOutputPosition())
                    .append(" kind=").append(projection.getKind())
                    .append(" component=").append(projection.getComponentIndex())
                    .append(" derived=").append(projection.isDerived())
                    .append(" guarded=").append(projection.isPartitionKeyGuarded())
                    .append(" functionSlot=").append(projection.getFunctionSlotBase() - plan.getSlotPrefix())
                    .append('\n');
        }
        return sink.toString();
    }

    /**
     * The same rendering off the live-view plan, whose projections wrap the runtime ones and
     * whose slot bases carry the anchor prefix this subtracts back out.
     */
    private static String liveViewLayout(LiveViewWindowStatePlan plan) {
        final StringBuilder sink = new StringBuilder();
        final int prefix = LiveViewWindowStatePlan.WINDOW_VALUE_SLOT_COUNT;
        for (int i = 0, n = plan.getComponentCount(); i < n; i++) {
            sink.append("component ").append(i)
                    .append(" family=").append(plan.getComponent(i).getFamily())
                    .append(" contribution=").append(plan.getComponent(i).getContributionKind())
                    .append(" arg=").append(plan.getComponent(i).getArgumentColumnIndex())
                    .append(':').append(plan.getComponent(i).getArgumentColumnType())
                    .append(" slotBase=").append(plan.getComponentSlotBase(i) - prefix)
                    .append(" slots=").append(plan.getComponent(i).getSlotCount())
                    .append('\n');
        }
        for (int i = 0, n = plan.getComponentCount(); i < n; i++) {
            sink.append("contributor on ").append(i)
                    .append(" out=").append(contributorOutputPosition(plan, i))
                    .append('\n');
        }
        for (int i = 0, n = plan.getProjectionCount(); i < n; i++) {
            final WindowAccumulatorProjection projection = plan.getProjection(i).getRuntime();
            sink.append("projection out=").append(projection.getOutputPosition())
                    .append(" kind=").append(projection.getKind())
                    .append(" component=").append(projection.getComponentIndex())
                    .append(" derived=").append(projection.isDerived())
                    .append(" guarded=").append(projection.isPartitionKeyGuarded())
                    .append(" functionSlot=").append(projection.getFunctionSlotBase() - prefix)
                    .append('\n');
        }
        return sink.toString();
    }

    private static int contributorOutputPosition(LiveViewWindowStatePlan plan, int componentIndex) {
        for (int i = 0, n = plan.getProjectionCount(); i < n; i++) {
            if (plan.getProjection(i).getComponentIndex() == componentIndex
                    && plan.getProjectionFunction(i) == plan.getContributor(componentIndex)) {
                return plan.getProjection(i).getRuntime().getOutputPosition();
            }
        }
        throw new AssertionError("no contributing projection on component " + componentIndex);
    }

    private static WindowAccumulatorPlan onlyPlan(ObjList<WindowAccumulatorPlan> plans) {
        Assert.assertNotNull(plans);
        Assert.assertEquals(1, plans.size());
        return plans.getQuick(0);
    }

    private static WindowAccumulatorProjection projectionAt(WindowAccumulatorPlan plan, int outputPosition) {
        for (int i = 0, n = plan.getProjectionCount(); i < n; i++) {
            if (plan.getProjection(i).getOutputPosition() == outputPosition) {
                return plan.getProjection(i);
            }
        }
        throw new AssertionError("no projection at output position " + outputPosition);
    }

    /**
     * The reference window every case above shares: partitioned, cumulative, and ordered by
     * the designated timestamp the base is already scanned in, so the query stays on the
     * streaming path the group compiler runs under.
     */
    private static String window() {
        return "window w as (partition by k order by ts rows between unbounded preceding and current row)";
    }

    /**
     * The same window with a bounded low bound, which is what the ring-backed families need: their
     * state is the frame's own values, so a frame that never gives one back reaches a different
     * set of implementations entirely.
     */
    private static String rowsFrameWindow() {
        return "window w as (partition by k order by ts rows between 3 preceding and current row)";
    }

    private void createBaseTable() throws Exception {
        execute("create table base (ts timestamp, k symbol, k2 symbol, x double, y double) "
                + "timestamp(ts) partition by day wal");
    }

    /**
     * The same shape with two DECIMAL columns, one of each state width a DECIMAL extremum can
     * keep: {@code d64} lands in a LONG slot and {@code d128} in a {@code DECIMAL128} one.
     */
    private void createDecimalBaseTable() throws Exception {
        execute("create table dbase (ts timestamp, k symbol, d64 decimal(18, 2), d128 decimal(38, 6)) "
                + "timestamp(ts) partition by day wal");
    }

    @FunctionalInterface
    private interface PlanCheck {
        void run(ObjList<WindowAccumulatorPlan> plans);
    }
}
