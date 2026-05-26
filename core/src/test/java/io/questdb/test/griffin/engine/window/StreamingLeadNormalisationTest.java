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

    @org.junit.Before
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
                    "DeferredEmitWindow\n" +
                            "  functions: [lead(x, 1, NULL) over ()]\n" +
                            "  maxLookahead: 1\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
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
            assertSql(
                    "x\tlx\n" +
                            "10\t20\n" +
                            "20\t30\n" +
                            "30\tnull\n",
                    "select x, lag(x, 1) over (order by ts desc) as lx from t"
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
                    "CachedWindow\n" +
                            "  orderedFunctions: [[y desc] => [lag(x, 1, NULL) over ()]]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testLagDescWithPartitionByNotNormalised() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, sym symbol, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 'A', 0), (20, 'B', 1000)");

            // PARTITION BY blocks Phase 4 normalisation (the deferred-emit cursor doesn't yet support
            // PARTITION BY); query falls through to CachedWindow.
            assertPlanNoLeakCheck(
                    "select x, lag(x, 1) over (partition by sym order by ts desc) as lx from t",
                    "CachedWindow\n" +
                            "  orderedFunctions: [[ts desc] => [lag(x, 1, NULL) over (partition by [sym])]]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
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
                    "Window\n" +
                            "  functions: [lag(x, 1, NULL) over ()]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
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
                    "SelectedRecord\n" +
                            "    Window\n" +
                            "      functions: [lag(x, 1, NULL) over ()]\n" +
                            "        PageFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: t\n"
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
            assertSql(expected, sql);
            assertSql(expected, sql);
            assertSql(expected, sql);
        });
    }
}
