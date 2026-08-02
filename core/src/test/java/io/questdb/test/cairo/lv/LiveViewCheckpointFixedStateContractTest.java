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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewAccumulatorProjection;
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionRoot;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRoot;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewStatePageWriter;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.lv.LiveViewWindowStatePlan;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Coverage for the fixed-width checkpoint state contract: the width a window
 * function declares through
 * {@link WindowFunction#checkpointStateFixedLength()}, the budget that decides
 * whether such a width may be inlined into a partition-map leaf, and the
 * framework check that every frozen image really is that wide.
 * <p>
 * The declaration is what a later inline entry gets sized by. The leaf carries no
 * length of its own for an inlined image, so a decoder has nothing but the
 * declaration to slice by: an image one byte off it reads a neighbouring field
 * rather than failing, and the wrong value only surfaces on a restart, well away
 * from the seal that wrote it. That is why the width is verified on every freeze
 * and why a malformed declaration is turned away at CREATE.
 * <p>
 * The cases pin the declarations against the images the seal actually produces.
 * They began doing that while the writer was still page-backed, which is what
 * made the width a contract the inline entry could be built on rather than one
 * asserted for the first time by the change that depends on it; now that the seal
 * inlines a declared width, the same walk reads the image out of the leaf.
 * {@code LiveViewCheckpointInlineStateTest} owns the entry shape itself.
 */
public class LiveViewCheckpointFixedStateContractTest extends AbstractLiveViewTest {

    private static final int COUNT_STATE_BYTES = Long.BYTES;
    private static final int SUM_STATE_BYTES = Double.BYTES + Long.BYTES;
    private static final int WELFORD_STATE_BYTES = 2 * Double.BYTES + Long.BYTES;

    /**
     * The customer shape the fused root is aimed at - an anchored window carrying an
     * unbounded cumulative sum and count per account - declares its widths, and every
     * state image the seal writes for it is exactly that wide, across the first seal
     * and every incremental one after it.
     */
    @Test
    public void testAnchoredAccumulatorsDeclareTheWidthTheirImagesActuallyHave() throws Exception {
        // One logical boundary per commit, so each commit below is its own seal and
        // the assertion covers a fresh image and a reused one alike.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                commit("('2026-01-01T11:00:10.000000Z', 'acct-1', 11.0)", job);
                commit("('2026-01-01T11:00:11.000000Z', 'acct-2', 12.0)", job);
                // A null argument contributes to neither accumulator, and must not change
                // the width of either image.
                commit("('2026-01-01T11:00:12.000000Z', 'acct-1', null)", job);
                assertViewMatchesRecompute();

                final ObjList<WindowFunction> functions = windowFunctions(viewInstance());
                int declaring = 0;
                for (int i = 0, n = functions.size(); i < n; i++) {
                    final WindowFunction function = functions.getQuick(i);
                    final int declared = function.checkpointStateFixedLength();
                    if (declared < 0) {
                        continue;
                    }
                    declaring++;
                    Assert.assertTrue(
                            "a declared width must be inlineable for the shapes this step covers",
                            LiveViewCheckpointContracts.isInlineableStateLength(declared)
                    );
                    assertEveryStateImageIsExactly(function, declared);
                }
                Assert.assertEquals(
                        "both the sum and the count accumulator must declare a fixed width",
                        2,
                        declaring
                );
            }
        });
    }

    @Test
    public void testBudgetAdmitsTheDeclaredWidthsAndRejectsTheDegenerateOnes() {
        Assert.assertTrue(LiveViewCheckpointContracts.isInlineableStateLength(SUM_STATE_BYTES));
        Assert.assertTrue(LiveViewCheckpointContracts.isInlineableStateLength(COUNT_STATE_BYTES));
        Assert.assertTrue(LiveViewCheckpointContracts.isInlineableStateLength(
                LiveViewCheckpointContracts.MAX_INLINE_COMPONENT_STATE_BYTES
        ));
        // A declining function and a state too wide for a leaf are both simply not
        // inlineable; neither is an error, and both keep the page-backed shape.
        Assert.assertFalse(LiveViewCheckpointContracts.isInlineableStateLength(-1));
        Assert.assertFalse(LiveViewCheckpointContracts.isInlineableStateLength(
                LiveViewCheckpointContracts.MAX_INLINE_COMPONENT_STATE_BYTES + 1
        ));
        // Zero is excluded on purpose: an empty scalar beside no page reference is
        // the one leaf shape that cannot be told apart from a corrupt entry.
        Assert.assertFalse(LiveViewCheckpointContracts.isInlineableStateLength(0));

        // The leaf the fused root is aimed at - an 8-byte anchor plus these two
        // accumulators - has to fit the leaf budget, or the whole group falls back to
        // legacy roots.
        Assert.assertTrue(
                Long.BYTES + SUM_STATE_BYTES + COUNT_STATE_BYTES
                        <= LiveViewCheckpointContracts.MAX_INLINE_LEAF_STATE_BYTES
        );
    }

    @Test
    public void testBufferedAndRingVariantsDeclineAFixedWidth() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, sym symbol, x double) timestamp(ts) partition by day wal");
            // A bounded ROWS frame keeps the rows behind its scalar tail in the image, and a
            // bounded RANGE frame is ring-shaped outright. Neither may declare a width just
            // because the tail in front of those rows is fixed.
            assertDeclinesFixedWidth("select ts, sym, sum(x) over (partition by sym order by ts "
                    + "rows between 3 preceding and current row) s from base");
            assertDeclinesFixedWidth("select ts, sym, avg(x) over (partition by sym order by ts "
                    + "range between 10 preceding and current row) a from base");
            assertDeclinesFixedWidth("select ts, sym, count(x) over (partition by sym order by ts "
                    + "rows between 3 preceding and current row) c from base");
            // An unpartitioned cumulative sum holds one scalar field and no map at all. It is
            // fixed width in principle, but it is not one of the implementations that have
            // opted in, so it declines like everything else that has not.
            assertDeclinesFixedWidth("select ts, sum(x) over (order by ts "
                    + "rows between unbounded preceding and current row) s from base");
        });
    }

    @Test
    public void testCreateRejectsAMalformedFixedWidthDeclaration() {
        // A width is only meaningful as the size of a whole-state image, so the three
        // shapes that cannot carry one are turned away rather than left to a decoder.
        assertDeclarationRejected(new FixedWidthStub(0, 0, false, true));
        assertDeclarationRejected(new FixedWidthStub(-2, 0, false, true));
        assertDeclarationRejected(new FixedWidthStub(SUM_STATE_BYTES, SUM_STATE_BYTES, true, true));
        assertDeclarationRejected(new FixedWidthStub(SUM_STATE_BYTES, SUM_STATE_BYTES, false, false));
        // Declining, and declaring a truthful width, are both accepted.
        assertDeclarationAccepted(new FixedWidthStub(-1, 0, false, true));
        assertDeclarationAccepted(new FixedWidthStub(SUM_STATE_BYTES, SUM_STATE_BYTES, false, true));
        // A width past the inline budget is a storage decision, not a malformed
        // declaration: the function keeps its page-backed entry and is still valid.
        final int tooWide = LiveViewCheckpointContracts.MAX_INLINE_COMPONENT_STATE_BYTES + 8;
        assertDeclarationAccepted(new FixedWidthStub(tooWide, tooWide, false, true));
    }

    @Test
    public void testFreezeRejectsAnImageThatMissesTheDeclaredWidth() {
        try (MemoryCARWImpl mem = new MemoryCARWImpl(64, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            final LiveViewStatePageWriter writer = new LiveViewStatePageWriter();
            // Truthful: the freeze returns the width it declared, and nothing else changes.
            Assert.assertEquals(
                    SUM_STATE_BYTES,
                    writer.of(mem).freeze(new FixedWidthStub(SUM_STATE_BYTES, SUM_STATE_BYTES, false, true), null)
            );
            // A declining function is not measured at all, so an image of any width passes.
            Assert.assertEquals(3, writer.of(mem).freeze(new FixedWidthStub(-1, 3, false, true), null));

            assertFreezeRejected(writer.of(mem), new FixedWidthStub(SUM_STATE_BYTES, SUM_STATE_BYTES - 1, false, true));
            assertFreezeRejected(writer.of(mem), new FixedWidthStub(SUM_STATE_BYTES, SUM_STATE_BYTES + 1, false, true));
            assertFreezeRejected(writer.of(mem), new FixedWidthStub(COUNT_STATE_BYTES, 0, false, true));
        }
    }

    @Test
    public void testUnboundedPartitionedAccumulatorsDeclareTheirWidth() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, sym symbol, x double) timestamp(ts) partition by day wal");
            // (sum, nonNullCount) for the two DOUBLE accumulators, one counter for count.
            assertDeclaredWidth("select ts, sym, sum(x) over (partition by sym order by ts "
                    + "rows between unbounded preceding and current row) s from base", SUM_STATE_BYTES);
            assertDeclaredWidth("select ts, sym, avg(x) over (partition by sym order by ts "
                    + "rows between unbounded preceding and current row) a from base", SUM_STATE_BYTES);
            assertDeclaredWidth("select ts, sym, count(x) over (partition by sym order by ts "
                    + "rows between unbounded preceding and current row) c from base", COUNT_STATE_BYTES);
            // One counter for the row-count pair, and Welford's (mean, m2, count) for
            // every dispersion call, whichever of the four is written.
            assertDeclaredWidth("select ts, sym, count(*) over (partition by sym order by ts "
                    + "rows between unbounded preceding and current row) c from base", COUNT_STATE_BYTES);
            assertDeclaredWidth("select ts, sym, row_number() over (partition by sym order by ts) rn "
                    + "from base", COUNT_STATE_BYTES);
            assertDeclaredWidth("select ts, sym, stddev_samp(x) over (partition by sym order by ts "
                    + "rows between unbounded preceding and current row) s from base", WELFORD_STATE_BYTES);
            assertDeclaredWidth("select ts, sym, var_pop(x) over (partition by sym order by ts "
                    + "rows between unbounded preceding and current row) v from base", WELFORD_STATE_BYTES);
            // The RANGE spelling of the same unbounded frame compiles to the same
            // implementations, so it must declare the same widths.
            assertDeclaredWidth("select ts, sym, sum(x) over (partition by sym order by ts "
                    + "range between unbounded preceding and current row) s from base", SUM_STATE_BYTES);
            assertDeclaredWidth("select ts, sym, count(x) over (partition by sym order by ts "
                    + "range between unbounded preceding and current row) c from base", COUNT_STATE_BYTES);
        });
    }

    private static void assertDeclarationAccepted(WindowFunction function) {
        try {
            CairoEngine.validateLiveViewWindowFunction(function, 0);
        } catch (SqlException e) {
            Assert.fail("declaration must be accepted: " + e.getFlyweightMessage());
        }
    }

    private static void assertDeclarationRejected(WindowFunction function) {
        try {
            CairoEngine.validateLiveViewWindowFunction(function, 0);
            Assert.fail("expected the malformed fixed-width declaration to be rejected");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "declares an invalid fixed checkpoint state width");
        }
    }

    private static void assertDeclaredWidth(String sql, int expected) throws Exception {
        Assert.assertEquals(sql, expected, compiledFunction(sql).checkpointStateFixedLength());
    }

    private static void assertDeclinesFixedWidth(String sql) throws Exception {
        Assert.assertEquals(sql, -1, compiledFunction(sql).checkpointStateFixedLength());
    }

    private static void assertFreezeRejected(LiveViewStatePageWriter writer, WindowFunction function) {
        try {
            writer.freeze(function, null);
            Assert.fail("expected the declared-width mismatch to be rejected");
        } catch (CairoException e) {
            TestUtils.assertContains(
                    e.getFlyweightMessage(),
                    "function state length does not match the declared fixed width"
            );
            Assert.assertEquals(
                    "an implementation defect must not be classified as recoverable checkpoint corruption",
                    0,
                    e.getErrno()
            );
        }
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    /**
     * Compiles {@code sql} the way a live view compiles it and returns its single
     * window function, so a case reads the declaration off the implementation the
     * view would actually run.
     */
    private static WindowFunction compiledFunction(String sql) throws Exception {
        sqlExecutionContext.setLiveViewCompile(true);
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
            RecordCursorFactory root = factory;
            while (root instanceof QueryProgress) {
                root = root.getBaseFactory();
            }
            Assert.assertTrue(sql, root instanceof WindowRecordCursorFactory);
            final ObjList<WindowFunction> functions = ((WindowRecordCursorFactory) root).getWindowFunctions();
            Assert.assertEquals(sql, 1, functions.size());
            return functions.getQuick(0);
        } finally {
            sqlExecutionContext.setLiveViewCompile(false);
        }
    }

    private static ObjList<WindowFunction> windowFunctions(LiveViewInstance instance) {
        RecordCursorFactory factory = instance.getCompiledFactory();
        while (factory != null) {
            if (factory instanceof WindowRecordCursorFactory windowFactory) {
                return windowFactory.getWindowFunctions();
            }
            if (factory instanceof QueryProgress) {
                factory = factory.getBaseFactory();
                continue;
            }
            break;
        }
        throw new IllegalStateException("compiled factory does not contain a WindowRecordCursorFactory");
    }

    /**
     * Walks every logical boundary the view has published and asserts that every
     * partition carries a whole-state image for {@code function} of exactly
     * {@code expected} bytes, inlined in the leaf and naming no state page.
     * <p>
     * Where that image lives is the shape of the boundary rather than of the
     * declaration: a function the fused plan groups has its image as a slice of the
     * window root's payload, at the width the manifest lays out for it, and one it
     * leaves residual has it as the whole scalar of its own function-root entry. The
     * declared width has to be the same number either way, which is the point of
     * checking both arms with one expectation.
     */
    private void assertEveryStateImageIsExactly(WindowFunction function, int expected) {
        final LiveViewCheckpointFunctionIdentity identity = function.checkpointFunctionIdentity();
        Assert.assertNotNull(identity);
        final byte[] encodedIdentity = identity.getEncoded();
        final LiveViewWindow anchorWindow = viewInstance().getAnchorWindow();
        final LiveViewWindowStatePlan plan = anchorWindow == null
                ? null
                : anchorWindow.getCheckpointWindowStatePlan();
        final LiveViewAccumulatorProjection projection = projectionOf(plan, function);
        int entries = 0;
        try (
                Path dir = checkpointsDir(viewInstance());
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration);
                    LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                    LiveViewCheckpointFunctionDirectory directory = new LiveViewCheckpointFunctionDirectory(configuration);
                    LiveViewCheckpointFunctionRoot functionRoot = new LiveViewCheckpointFunctionRoot(configuration);
                    LiveViewCheckpointWindowRoot windowRoot = new LiveViewCheckpointWindowRoot(configuration);
                    LiveViewCheckpointPartitionMapReader partitions = new LiveViewCheckpointPartitionMapReader(configuration)
            ) {
                timeline.of(dir);
                partitions.of(dir);
                final LiveViewCheckpointPageRef directoryRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef partitionMapRoot = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef stateRootRef = new LiveViewCheckpointPageRef();
                final int[] seen = {0};
                timeline.iterateAll(pin.getTimelineRootRef(), entry -> {
                    root.of(dir, entry.rootRef);
                    root.getStateRootRef(stateRootRef);
                    if (projection != null && !stateRootRef.isNull()
                            && windowRoot.ofIfWindowRoot(dir, stateRootRef)) {
                        // The projection's own slice, not the component's: a derived one
                        // reads a field of a wider host, and it is still its declared
                        // width that has to describe what its decoder consumes.
                        Assert.assertEquals(
                                "the manifest must lay out " + function.getName() + " at its declared width",
                                expected,
                                projection.getFunctionStateLength()
                        );
                        final int totalInlineStateBytes = windowRoot.getTotalInlineStateBytes();
                        windowRoot.getPartitionMapRootRef(partitionMapRoot);
                        partitions.iterateAll(partitionMapRoot, partition -> {
                            LiveViewCheckpointWindowRoot.readWindowState(partition, totalInlineStateBytes);
                            seen[0]++;
                        });
                        return;
                    }
                    root.getFunctionDirectoryRef(directoryRef);
                    directory.of(dir, directoryRef);
                    Assert.assertTrue(
                            "the boundary must name a root for " + function.getName(),
                            directory.find(encodedIdentity, functionRootRef)
                    );
                    functionRoot.of(dir, functionRootRef);
                    functionRoot.getPartitionMapRootRef(partitionMapRoot);
                    partitions.iterateAll(partitionMapRoot, partition -> {
                        Assert.assertEquals(
                                "an inlined image names no state page",
                                0,
                                partition.getStatePageCount()
                        );
                        Assert.assertEquals(
                                "the image of " + function.getName() + " must be its declared width",
                                expected,
                                partition.getScalarState().length
                        );
                        seen[0]++;
                    });
                });
                entries = seen[0];
            }
        }
        Assert.assertTrue("the view must have sealed partition state for " + function.getName(), entries > 0);
    }

    /**
     * Returns the plan's binding for {@code function}, or null when the plan does not
     * group it and it therefore keeps a root of its own.
     */
    private static LiveViewAccumulatorProjection projectionOf(LiveViewWindowStatePlan plan, WindowFunction function) {
        if (plan == null) {
            return null;
        }
        for (int i = 0, n = plan.getProjectionCount(); i < n; i++) {
            if (plan.getProjectionFunction(i) == function) {
                return plan.getProjection(i);
            }
        }
        return null;
    }

    private void assertViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + recompute() + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    private void commit(String values, LiveViewRefreshJob job) throws Exception {
        execute("insert into tx values " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private void createView() throws Exception {
        execute("create table tx (created_at timestamp, cod_acct_no symbol nocache index capacity 4, "
                + "amt_txn double) timestamp(created_at) partition by hour wal");
        execute("insert into tx values "
                + "('2026-01-01T11:00:00.000000Z', 'acct-1', 10.0), "
                + "('2026-01-01T11:00:01.000000Z', 'acct-2', 20.0), "
                + "('2026-01-01T11:00:02.000000Z', 'acct-3', 30.0)");
        drainWalQueue();
        execute("create live view lv flush every 100ms start from beginning as " + viewSelect());
    }

    /**
     * The view's own SELECT with the anchor desugared into the bucket it floors to,
     * so the oracle is a plain query the compiler accepts outside a live view.
     */
    private String recompute() {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        return "select created_at, cod_acct_no, "
                + "sum(amt_txn) over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_sum, "
                + "count(cod_acct_no) over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_count "
                + "from (select created_at, cod_acct_no, amt_txn, " + bucket + " as bucket from tx)";
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    private String viewSelect() {
        return "select created_at, cod_acct_no, "
                + "sum(amt_txn) over w as cumulative_sum, "
                + "count(cod_acct_no) over w as cumulative_count "
                + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')";
    }

    /**
     * A scalar (map-less) whole-state function that declares one width and emits
     * another, so a case controls the two independently.
     */
    private static final class FixedWidthStub extends BaseWindowFunction {
        private final int declaredLength;
        private final int emittedLength;
        private final boolean ringShaped;
        private final boolean stateful;

        private FixedWidthStub(int declaredLength, int emittedLength, boolean ringShaped, boolean stateful) {
            super(null);
            this.declaredLength = declaredLength;
            this.emittedLength = emittedLength;
            this.ringShaped = ringShaped;
            this.stateful = stateful;
            setCheckpointCompilerMetadata(
                    new LiveViewCheckpointFunctionIdentity(
                            "w0",
                            "fixed_width_stub()",
                            0,
                            "",
                            "ts asc",
                            "fixed-width-stub-v1"
                    ),
                    new LiveViewCheckpointDependency(
                            LiveViewCheckpointContracts.DependencyKind.STATELESS_CURRENT_ROW,
                            "",
                            "ts asc",
                            0,
                            0,
                            0,
                            ColumnType.TIMESTAMP,
                            false,
                            false,
                            false,
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
        public int checkpointStateFormatVersion() {
            return 1;
        }

        @Override
        public void freezeCheckpointState(LiveViewStatePageWriter sink, MapValue value) {
            for (int i = 0; i < emittedLength; i++) {
                sink.putByte((byte) i);
            }
        }

        @Override
        public String getName() {
            return "fixed_width_stub";
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public int getType() {
            return ColumnType.LONG;
        }

        @Override
        public boolean isCheckpointStateless() {
            return !stateful;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }

        @Override
        public boolean supportsCheckpointRingState() {
            return ringShaped;
        }

        @Override
        public boolean supportsCheckpointState() {
            return stateful;
        }
    }
}
