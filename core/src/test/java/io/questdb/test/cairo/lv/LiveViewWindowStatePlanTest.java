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
 *     only when their family, argument and contribution predicate all match. The HDFC
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
    public void testCountOverTheSummedColumnKeepsItsOwnComponentForNow() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // sum and avg collapse; count(x) does not yet, because deriving a count from
            // another family's counter is the next step's relation rather than a merge of
            // identical descriptors. The fused entry is therefore 32 bytes here and becomes
            // 24 once the derivation lands.
            assertPlan(
                    "select ts, sym, sum(x) over w as s, avg(x) over w as a, count(x) over w as c "
                            + "from base window w as (partition by sym order by ts anchor daily '00:00')",
                    plan -> {
                        Assert.assertNotNull(plan);
                        Assert.assertEquals(2, plan.getComponentCount());
                        Assert.assertEquals(3, plan.getProjectionCount());
                        Assert.assertEquals(
                                ANCHOR_BYTES + SUM_STATE_BYTES + COUNT_STATE_BYTES,
                                plan.getTotalInlineStateBytes()
                        );
                        Assert.assertEquals(
                                "sum and avg read one component",
                                plan.getProjection(0).getComponentIndex(),
                                plan.getProjection(1).getComponentIndex()
                        );
                        Assert.assertNotEquals(
                                "count(x) may not read the sum's counter yet",
                                plan.getProjection(0).getComponentIndex(),
                                plan.getProjection(2).getComponentIndex()
                        );
                    }
            );
        });
    }

    @Test
    public void testCountStarAndExpressionArgumentsStayResidual() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            // count(*) counts rows rather than an argument's non-null values, so it has no
            // argument key and can never join a count(x) component.
            assertPlan(
                    "select ts, sym, sum(x) over w as s, count(*) over w as c "
                            + "from base window w as (partition by sym order by ts anchor daily '00:00')",
                    plan -> {
                        Assert.assertNotNull(plan);
                        Assert.assertEquals(1, plan.getComponentCount());
                        Assert.assertEquals(1, plan.getProjectionCount());
                        Assert.assertEquals(1, plan.getResidualFunctions().size());
                        Assert.assertEquals("count", plan.getResidualFunctions().getQuick(0).getName());
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
        execute("create table base (ts timestamp, sym symbol, x double) timestamp(ts) partition by day wal");
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
