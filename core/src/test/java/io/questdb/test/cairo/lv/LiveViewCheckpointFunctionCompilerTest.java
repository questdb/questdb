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

import io.questdb.cairo.lv.LiveViewCheckpointContracts.DependencyKind;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointDependency.NumericConvergence;
import io.questdb.cairo.lv.LiveViewCheckpointDependency.StructuralConvergence;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
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

    private static Metadata compileMetadata(String sql, int functionIndex) throws Exception {
        sqlExecutionContext.setLiveViewCompile(true);
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
            RecordCursorFactory root = factory;
            while (root instanceof QueryProgress) {
                root = root.getBaseFactory();
            }
            Assert.assertTrue(root instanceof WindowRecordCursorFactory);
            final ObjList<WindowFunction> functions = ((WindowRecordCursorFactory) root).getWindowFunctions();
            final WindowFunction function = functions.getQuick(functionIndex);
            Assert.assertNotNull(function.getClass().getName(), function.checkpointFunctionIdentity());
            Assert.assertNotNull(function.getClass().getName(), function.checkpointDependency());
            return new Metadata(function.checkpointFunctionIdentity(), function.checkpointDependency());
        } finally {
            sqlExecutionContext.setLiveViewCompile(false);
        }
    }

    private static class Metadata {
        private final LiveViewCheckpointDependency dependency;
        private final LiveViewCheckpointFunctionIdentity identity;

        private Metadata(
                LiveViewCheckpointFunctionIdentity identity,
                LiveViewCheckpointDependency dependency
        ) {
            this.identity = identity;
            this.dependency = dependency;
        }
    }
}
