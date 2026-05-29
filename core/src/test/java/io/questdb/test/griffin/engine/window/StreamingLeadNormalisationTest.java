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

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for Phase 4 LAG <-> LEAD normalisation in the planner. Verifies that the planner rewrites
 * direction-mismatched LAG/LEAD calls against the designated timestamp so the resulting window can
 * stream, and that the rewritten function returns the same values as the original.
 */
public class StreamingLeadNormalisationTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_STREAMING_LEAD_ENABLED, "true");
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void reapplyStreamingLeadFlag() {
        // The standard test tearDown (Cairo.tearDown -> overrides.reset()) wipes property overrides
        // after every test, so the flag set in @BeforeClass survives only the first test. Re-apply
        // here so each test executes with the flag enabled. The CairoConfigurationWrapper reads
        // overrides dynamically per call, so this takes effect for queries compiled in the test body.
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_STREAMING_LEAD_ENABLED, "true");
    }

    @Test
    public void testLagDescNormalisesToStreamingLead() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000), (30, 2000)");

            // LAG-DESC on forward-scanned table normalises to LEAD-ASC, which streams. The OVER ORDER
            // BY is dismissed (matches scan direction after normalisation) so the plan prints
            // `over ()` rather than the original `order by ts desc`.
            assertPlanNoLeakCheck(
                    "select x, lag(x, 1) over (order by ts desc) as lx from t",
                    """
                            DeferredEmitWindow
                              functions: [lead(x, 1, NULL) over ()]
                              maxLookahead: 1
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testLagDescNormalisedProducesCorrectValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000), (30, 2000)");

            // LAG(x, 1) OVER (ORDER BY ts DESC) equals LEAD(x, 1) OVER (ORDER BY ts ASC) value-wise:
            // for each row, the "next row at higher timestamp" — which is what LAG-DESC and LEAD-ASC
            // both compute. Row 30 has no higher ts so default (NULL) applies.
            assertQueryNoLeakCheck(
                    """
                            x\tlx
                            10\t20
                            20\t30
                            30\tnull
                            """,
                    "select x, lag(x, 1) over (order by ts desc) as lx from t",
                    null, false, true
            );
        });
    }

    @Test
    public void testLagDescOnNonTimestampNotNormalised() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, y long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 100, 0), (20, 200, 1000)");

            // OVER ORDER BY a non-timestamp column — normalisation does not fire, falls to cached.
            assertPlanNoLeakCheck(
                    "select x, lag(x, 1) over (order by y desc) as lx from t",
                    """
                            CachedWindow
                              orderedFunctions: [[y desc] => [lag(x, 1, NULL) over ()]]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testLagDescWithPartitionByStreamsViaNormalisation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, sym symbol, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 'A', 0), (20, 'B', 1000), (30, 'A', 2000), (40, 'B', 3000), (50, 'A', 4000)");

            // Phase 5: PARTITION BY no longer blocks normalisation. LAG-DESC normalises to LEAD-ASC
            // and streams via per-partition deferred emit. Output is partition-major-resolution order:
            // in-stream emissions when the next row of a partition arrives; remaining entries flushed
            // per partition at end-of-cursor in map iteration order.
            assertQueryNoLeakCheck(
                    """
                            x\tsym\tlx
                            10\tA\t30
                            20\tB\t40
                            30\tA\t50
                            50\tA\tnull
                            40\tB\tnull
                            """,
                    "select x, sym, lag(x, 1) over (partition by sym order by ts desc) as lx from t",
                    null, false, true
            );
        });
    }

    @Test
    public void testLagDescWithoutOverOrderNotNormalised() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000)");

            // No OVER ORDER BY — normalisation only fires when there is exactly one OVER ORDER BY
            // column. Plain LAG over () is already ZERO_PASS with lookahead 0, so it uses the
            // existing Window streaming factory directly without going through DeferredEmit.
            assertPlanNoLeakCheck(
                    "select x, lag(x, 1) over () as lx from t",
                    """
                            Window
                              functions: [lag(x, 1, NULL) over ()]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testLeadAscOnBackwardScanNormalises() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000), (30, 2000)");

            // Outer ORDER BY ts DESC triggers backward base scan. LEAD-ASC mismatches the
            // backward scan and normalises to LAG-DESC, which then streams via the existing
            // ZERO_PASS + lookahead==0 LAG path (Window, not DeferredEmit). The outer
            // ORDER BY ts DESC is encoded by the backward base scan plus a SelectedRecord wrapper;
            // the OVER ORDER BY is dismissed because it now matches the scan direction.
            assertPlanNoLeakCheck(
                    "select x, lead(x, 1) over (order by ts asc) as lx from t order by ts desc",
                    """
                            SelectedRecord
                                Window
                                  functions: [lag(x, 1, NULL) over ()]
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: t
                            """
            );
        });
    }

    @Test
    public void testReexecutionAfterNormalisationStillCorrect() throws Exception {
        // Verifies that the AST normalisation correctly clones the AST before mutation, so a second
        // execution of the same prepared statement still produces correct results. Without cloning,
        // the second execution would see a mutated AST where the function name no longer matches the
        // original SQL and could either silently produce wrong values or throw.
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000), (30, 2000)");

            String sql = "select x, lag(x, 1) over (order by ts desc) as lx from t";
            String expected = "x\tlx\n10\t20\n20\t30\n30\tnull\n";

            // Run twice — second run must compile and execute identically.
            assertQueryNoLeakCheck(expected, sql, null, false, true);
            assertQueryNoLeakCheck(expected, sql, null, false, true);
            assertQueryNoLeakCheck(expected, sql, null, false, true);
        });
    }

    @Test
    public void testLagDescNormalisedWithLargerOffset() throws Exception {
        // Larger offset (k=3) exercises the same Phase 4 AST swap on a non-trivial lookahead. LAG(x, 3)
        // OVER (ORDER BY ts DESC) equals LEAD(x, 3) OVER (ORDER BY ts ASC) value-wise.
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute(
                    "insert into t values " +
                            "(10, 0), (20, 1000), (30, 2000), (40, 3000), (50, 4000), (60, 5000)"
            );

            assertQueryNoLeakCheck(
                    """
                            x\tlx
                            10\t40
                            20\t50
                            30\t60
                            40\tnull
                            50\tnull
                            60\tnull
                            """,
                    "select x, lag(x, 3) over (order by ts desc) as lx from t",
                    null, false, true
            );
        });
    }

    @Test
    public void testLagDescNormalisedWithNonNullDefault() throws Exception {
        // Non-NULL default value flows through the Phase 4 swap. The default applies to the rows whose
        // lookahead position falls past EOF (here: top three rows after the swap to LEAD ASC).
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000), (30, 2000)");

            assertQueryNoLeakCheck(
                    """
                            x\tlx
                            10\t20
                            20\t30
                            30\t99
                            """,
                    "select x, lag(x, 1, 99::long) over (order by ts desc) as lx from t",
                    null, false, true
            );
        });
    }

    @Test
    public void testLagDescNormalisedWithCompoundArgument() throws Exception {
        // Compound argument expression (x + 1) forces the AST deep-clone to walk an args subtree, not
        // just a leaf. The swap must rebuild the function on the cloned AST without mutating the
        // original subtree.
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000), (30, 2000)");

            assertQueryNoLeakCheck(
                    """
                            x\tlx
                            10\t21
                            20\t31
                            30\tnull
                            """,
                    "select x, lag(x + 1, 1) over (order by ts desc) as lx from t",
                    null, false, true
            );
        });
    }
}
