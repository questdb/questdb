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
 * SQL-level integration tests for the streaming LEAD path with the session flag enabled. Verifies
 * that the planner dispatches to {@code DeferredEmitWindowRecordCursorFactory} for eligible
 * queries, falls back to cached for unsupported shapes, and that the deferred-emit cursor returns
 * correct rows including default values for unfilled flush entries.
 */
public class StreamingLeadIntegrationTest extends AbstractCairoTest {

    @org.junit.Before
    public void reapplyStreamingLeadFlag() {
        // The standard test tearDown (Cairo.tearDown -> overrides.reset()) wipes property overrides
        // after every test, so the flag set in @BeforeClass survives only the first test. Re-apply
        // here so each test executes with the flag enabled. The CairoConfigurationWrapper reads
        // overrides dynamically per call, so this takes effect for queries compiled in the test body.
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_STREAMING_LEAD_ENABLED, "true");
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Enable the streaming-lead flag before the engine is built so the captured CairoConfiguration
        // reflects it. The engine is constructed once per test class in AbstractCairoTest.setUpStatic
        // and holds a final reference to the configuration thereafter; per-test @Before overrides do
        // not propagate to the engine.
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_STREAMING_LEAD_ENABLED, "true");
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testIgnoreNullsFallsBackToCached() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000)");

            // Streaming LEAD does not support IGNORE NULLS in Phase 3; planner uses cached.
            assertPlanNoLeakCheck(
                    "select x, lead(x, 1) ignore nulls over () as lx from t",
                    "CachedWindow\n" +
                            "  unorderedFunctions: [lead(x, 1, NULL) ignore nulls over ()]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testLeadOneStreamsAndProducesExpectedRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000), (30, 2000), (40, 3000), (50, 4000)");

            assertSql(
                    "x\tlx\n" +
                            "10\t20\n" +
                            "20\t30\n" +
                            "30\t40\n" +
                            "40\t50\n" +
                            "50\tnull\n",
                    "select x, lead(x, 1) over () as lx from t"
            );
        });
    }

    @Test
    public void testLeadOneStreamsWithDefaultLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000), (30, 2000)");

            // Default value -1 supplied; flushed entries should carry it instead of NULL.
            assertSql(
                    "x\tlx\n" +
                            "10\t20\n" +
                            "20\t30\n" +
                            "30\t-1\n",
                    "select x, lead(x, 1, -1) over () as lx from t"
            );
        });
    }

    @Test
    public void testLeadOneStreamsWithFlagOn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000), (30, 2000)");

            // Plan should route through DeferredEmitWindow.
            assertPlanNoLeakCheck(
                    "select x, lead(x, 1) over () as lx from t",
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
    public void testLeadThreeStreamsWithFlushedTail() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 0), (20, 1000), (30, 2000), (40, 3000), (50, 4000)");

            // Lookahead 3 over 5 rows: rows 0,1 in-stream (paired with rows 3,4); rows 2,3,4 flushed.
            assertSql(
                    "x\tlx\n" +
                            "10\t40\n" +
                            "20\t50\n" +
                            "30\tnull\n" +
                            "40\tnull\n" +
                            "50\tnull\n",
                    "select x, lead(x, 3) over () as lx from t"
            );
        });
    }

    @Test
    public void testNonConstantDefaultFallsBackToCached() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, y long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 100, 0), (20, 200, 1000)");

            // Non-constant default (y is a column) blocks streaming; planner uses cached.
            assertPlanNoLeakCheck(
                    "select x, lead(x, 1, y) over () as lx from t",
                    "CachedWindow\n" +
                            "  unorderedFunctions: [lead(x, 1, y) over ()]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testPartitionByFallsBackToCached() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, sym symbol, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (10, 'A', 0), (20, 'B', 1000)");

            // PARTITION BY is a Phase 5+ feature; planner uses cached.
            assertPlanNoLeakCheck(
                    "select x, lead(x, 1) over (partition by sym) as lx from t",
                    "CachedWindow\n" +
                            "  unorderedFunctions: [lead(x, 1, NULL) over (partition by [sym])]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testSingleRowFlushedAsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into t values (42, 0)");

            assertSql(
                    "x\tlx\n" +
                            "42\tnull\n",
                    "select x, lead(x, 1) over () as lx from t"
            );
        });
    }
}
