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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.QueryProgress;
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
            Assert.assertTrue(first.identity.getStateCodecIdentity().contains("/avg(D)/v1"));

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
            Assert.assertEquals(DependencyKind.RANGE_W_PRECEDING_CURRENT_ROW, range.dependency.getKind());
            Assert.assertEquals(-10, range.dependency.getFrameLo());
            Assert.assertEquals(0, range.dependency.getFrameHi());
            Assert.assertEquals(10, range.dependency.getRangeFrameWidth());
            Assert.assertNotNull(range.rangePlan);
            Assert.assertTrue(range.dependency.supportsKeyRestore());
            Assert.assertFalse(range.dependency.supportsKeyReset());
            Assert.assertEquals(StructuralConvergence.EXACT, range.dependency.getStructuralConvergence());
            Assert.assertEquals(NumericConvergence.FLOATING_TOLERANCE, range.dependency.getNumericConvergence());
            Assert.assertEquals(range.dependency.getKind().getLowBoundStrategy(), range.dependency.getLowBoundStrategy());
            Assert.assertEquals(range.dependency.getKind().getHighBoundStrategy(), range.dependency.getHighBoundStrategy());

            final Metadata rows = compileMetadata(
                    "select ts, sym, count(x) over (partition by sym order by ts "
                            + "rows between 3 preceding and current row) as c from base",
                    0
            );
            Assert.assertEquals(DependencyKind.ROWS_N_PRECEDING_CURRENT_ROW, rows.dependency.getKind());
            Assert.assertEquals(-3, rows.dependency.getFrameLo());
            Assert.assertEquals(0, rows.dependency.getFrameHi());
            Assert.assertEquals(NumericConvergence.EXACT, rows.dependency.getNumericConvergence());
            Assert.assertNull(rows.rangePlan);
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

            // A view mixing a bounded RANGE with a bounded ROWS frame stays
            // valid, but has no RANGE repair plan - the ROWS half is not
            // bounded by a timestamp width.
            final Metadata mixed = compileMetadata(
                    "select ts, sym, "
                            + "avg(x) over (partition by sym order by ts range between 2 seconds preceding and current row) a, "
                            + "sum(x) over (partition by sym order by ts rows between 3 preceding and current row) s "
                            + "from base",
                    0
            );
            Assert.assertTrue(mixed.dependency.isFiniteRange());
            Assert.assertNull(mixed.rangePlan);

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
     * The RANGE shapes outside {@code W PRECEDING ... CURRENT ROW} stay compilable, but must
     * not present themselves as a finite RANGE dependency - a repair planner that claimed
     * them would derive a bound the frame does not obey.
     */
    @Test
    public void testRangeShapesOutsideTheSupportedFrameAreNotFiniteRange() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, sym symbol, x double) timestamp(ts) partition by day wal");

            // Ends before the current row: finite, but its influence boundary is not the
            // one RANGE W PRECEDING ... CURRENT ROW arithmetic derives.
            assertNotFiniteRange("select ts, sym, last_value(x) over (partition by sym order by ts "
                    + "range between '3' hour preceding and '1' hour preceding) a from base");
            // Unbounded look-behind: no dependency floor below the correction.
            assertNotFiniteRange("select ts, sym, first_value(x) ignore nulls over (partition by sym order by ts "
                    + "range between unbounded preceding and '2' second preceding) a from base");
            // A frame exclusion changes membership inside the window.
            assertNotFiniteRange("select ts, sym, avg(x) over (partition by sym order by ts "
                    + "range between 2 preceding and current row exclude current row) a from base");
        });
    }

    @Test
    public void testRangeWidthOutOfRangeFailsCompilation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp, sym symbol, x double) timestamp(ts) partition by day wal");
            // TimestampDriver.from(long, char) narrows days to int, so this width collapses to
            // a zero-wide runtime frame. The descriptor cannot describe the frame the user
            // asked for, so compilation fails instead of checkpointing a mismatched bound.
            try {
                compileMetadata(
                        "select ts, sym, avg(x) over (partition by sym order by ts "
                                + "range between 4294967296 days preceding and current row) a from base",
                        0
                );
                Assert.fail("expected RANGE width rejection");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "RANGE width is out of range for the designated timestamp");
            }
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

    private static void assertNotFiniteRange(String sql) throws Exception {
        final Metadata metadata = compileMetadata(sql, 0);
        Assert.assertFalse(sql, metadata.dependency.isFiniteRange());
        Assert.assertNull(sql, metadata.rangePlan);
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
            return new Metadata(
                    function.checkpointFunctionIdentity(),
                    function.checkpointDependency(),
                    windowFactory.getCheckpointRangePlan()
            );
        } finally {
            sqlExecutionContext.setLiveViewCompile(false);
        }
    }

    private static class Metadata {
        private final LiveViewCheckpointDependency dependency;
        private final LiveViewCheckpointFunctionIdentity identity;
        private final LiveViewCheckpointRangePlan rangePlan;

        private Metadata(
                LiveViewCheckpointFunctionIdentity identity,
                LiveViewCheckpointDependency dependency,
                LiveViewCheckpointRangePlan rangePlan
        ) {
            this.identity = identity;
            this.dependency = dependency;
            this.rangePlan = rangePlan;
        }
    }
}
