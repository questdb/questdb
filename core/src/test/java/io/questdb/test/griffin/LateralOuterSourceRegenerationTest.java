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

package io.questdb.test.griffin;

import io.questdb.cairo.SqlJitMode;
import io.questdb.griffin.PlanSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class LateralOuterSourceRegenerationTest extends AbstractCairoTest {
    private static final String LIMITED_RESULT = """
            ts\tx\tc
            1970-01-01T00:00:00.000101Z\t101\t50
            """;
    private static final String SELECTED_RESULT = """
            ts\tx\tc
            1970-01-01T00:00:00.000001Z\t1\t1
            1970-01-01T00:00:00.000101Z\t101\t50
            1970-01-01T00:00:00.000200Z\t200\t50
            """;

    @Test
    public void testEqualityEliminatesOuterReferenceControl() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String query = lateral("(SELECT ts, x FROM t WHERE x IN (1, 101, 200))")
                    .replace("u.ts <= o.ts", "u.ts = o.ts");
            assertQuery(query + " ORDER BY o.x").returns("""
                    ts\tx\tc
                    1970-01-01T00:00:00.000001Z\t1\t1
                    1970-01-01T00:00:00.000101Z\t101\t0
                    1970-01-01T00:00:00.000200Z\t200\t0
                    """);
            assertPlanOccurrences(query, "Frame forward scan on: t", 1);
            assertPlanOccurrences(query, "Cross Join", 0);
        });
    }

    @Test
    public void testFilteredLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String query = lateral("(SELECT ts, x FROM t WHERE x IN (101, 200) LIMIT 1)");
            assertQuery(query + " ORDER BY o.x").returns(LIMITED_RESULT);
        });
    }

    @Test
    public void testFilteredLimitInterpreted() throws Exception {
        int jitMode = sqlExecutionContext.getJitMode();
        try {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            testFilteredLimit();
        } finally {
            sqlExecutionContext.setJitMode(jitMode);
        }
    }

    @Test
    public void testFilteredSelectionInEveryBranch() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String query = lateral("(SELECT ts, x FROM t WHERE x IN (1, 101, 200))");
            assertQuery(query + " ORDER BY o.x").returns(SELECTED_RESULT);
            // Both the primary source and the DISTINCT correlation-key source must select three keys.
            assertSourceSelection(query, "x in [1,101,200]", 2);
        });
    }

    @Test
    public void testIntrinsicSelectionInEveryBranch() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String query = lateral("(SELECT ts, x FROM t WHERE ts BETWEEN "
                    + "'1970-01-01T00:00:00.000101Z' AND '1970-01-01T00:00:00.000103Z')");
            assertQuery(query + " ORDER BY o.x").returns("""
                    ts\tx\tc
                    1970-01-01T00:00:00.000101Z\t101\t50
                    1970-01-01T00:00:00.000102Z\t102\t50
                    1970-01-01T00:00:00.000103Z\t103\t50
                    """);
            assertSourceSelection(query, "Interval forward scan on: t", 2);
        });
    }

    @Test
    public void testJoinedOuterLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String query = lateral("(SELECT t.ts, t.x FROM t JOIN "
                    + "(SELECT x FROM long_sequence(200)) b ON t.x = b.x "
                    + "WHERE t.x IN (101, 200) LIMIT 1)");
            assertQuery(query + " ORDER BY o.x").returns(LIMITED_RESULT);
        });
    }

    @Test
    public void testLatestByOuterLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE latest_source AS (SELECT x::TIMESTAMP ts, x, x % 2 k FROM long_sequence(200)) TIMESTAMP(ts)");
            String source = "(SELECT ts, x FROM latest_source LATEST ON ts PARTITION BY k LIMIT 1)";
            execute("CREATE TABLE selected AS " + source + " TIMESTAMP(ts)");
            String expected = """
                    ts\tx\tc
                    1970-01-01T00:00:00.000199Z\t199\t50
                    """;
            assertQuery(lateral("selected") + " ORDER BY o.x").returns(expected);
            String query = lateral(source);
            assertQuery(query + " ORDER BY o.x").returns(expected);
        });
    }

    @Test
    public void testMaterializedFilteredLimitControl() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE selected AS (SELECT ts, x FROM t WHERE x IN (101, 200) LIMIT 1) TIMESTAMP(ts)");
            assertQuery(lateral("selected") + " ORDER BY o.x").returns(LIMITED_RESULT);
        });
    }

    @Test
    public void testMaterializedSubsampleLimitControl() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE selected AS (SELECT ts, x FROM t SUBSAMPLE uniform(3) LIMIT 1, 2) TIMESTAMP(ts)");
            assertQuery(lateral("selected") + " ORDER BY o.x").returns(LIMITED_RESULT);
        });
    }

    @Test
    public void testRepeatedOuterReferenceSelection() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String query = """
                    SELECT o.ts, o.x, l.c, r.c AS d
                    FROM (SELECT ts, x FROM t WHERE x IN (1, 101, 200)) o
                    JOIN LATERAL (SELECT count() c FROM u WHERE u.ts <= o.ts) l ON true
                    JOIN LATERAL (SELECT count() c FROM u WHERE u.ts < o.ts) r ON true
                    """;
            assertQuery(query + " ORDER BY o.x").returns("""
                    ts\tx\tc\td
                    1970-01-01T00:00:00.000001Z\t1\t1\t0
                    1970-01-01T00:00:00.000101Z\t101\t50\t50
                    1970-01-01T00:00:00.000200Z\t200\t50\t50
                    """);
            assertSourceSelection(query, "x in [1,101,200]", 3);
        });
    }

    @Test
    public void testRepeatedSubsampleSelection() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String query = """
                    SELECT o.ts, o.x, l.c, r.c AS d
                    FROM (SELECT ts, x FROM t SUBSAMPLE uniform(3)) o
                    JOIN LATERAL (SELECT count() c FROM u WHERE u.ts <= o.ts) l ON true
                    JOIN LATERAL (SELECT count() c FROM u WHERE u.ts < o.ts) r ON true
                    """;
            assertQuery(query + " ORDER BY o.x").returns("""
                    ts\tx\tc\td
                    1970-01-01T00:00:00.000001Z\t1\t1\t0
                    1970-01-01T00:00:00.000101Z\t101\t50\t50
                    1970-01-01T00:00:00.000200Z\t200\t50\t50
                    """);
            assertSourceSelection(query, "CachedWindowLightSelect", 3);
        });
    }

    @Test
    public void testSharedGroupedOuterControl() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String query = lateral("(SELECT ts, max(x) AS x FROM t WHERE x IN (1, 101, 200) GROUP BY ts)");
            assertQuery(query + " ORDER BY o.x").returns(SELECTED_RESULT);
            assertQuery(query).assertsPlanContaining("(Shared)");
            assertSourceSelection(query, "x in [1,101,200]", 2);
        });
    }

    @Test
    public void testSubsampleLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String query = lateral("(SELECT ts, x FROM t SUBSAMPLE uniform(3) LIMIT 1, 2)");
            assertQuery(query + " ORDER BY o.x").returns(LIMITED_RESULT);
        });
    }

    @Test
    public void testSubsampleSelectionInEveryBranch() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String query = lateral("(SELECT ts, x FROM t SUBSAMPLE uniform(3))");
            assertQuery(query + " ORDER BY o.x").returns(SELECTED_RESULT);
            assertSourceSelection(query, "CachedWindowLightSelect", 2);
        });
    }

    @Test
    public void testUnionSortedOuterLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String query = lateral("(SELECT ts, x FROM ("
                    + "SELECT ts, x FROM t WHERE x IN (101, 200) UNION ALL "
                    + "SELECT ts, x FROM t WHERE x = 150) ORDER BY ts LIMIT 1)");
            assertQuery(query + " ORDER BY o.x").returns(LIMITED_RESULT);
        });
    }

    private static String lateral(String source) {
        return "SELECT o.ts, o.x, l.c FROM " + source
                + " o JOIN LATERAL (SELECT count() c FROM u WHERE u.ts <= o.ts) l ON true";
    }

    private void assertPlanOccurrences(String query, String fragment, int expected) throws Exception {
        String plan = planText(getPlanSink(query));
        int count = 0;
        for (int offset = plan.indexOf(fragment); offset >= 0; offset = plan.indexOf(fragment, offset + fragment.length())) {
            count++;
        }
        Assert.assertEquals(fragment + " in plan:\n" + plan, expected, count);
    }

    private void assertSourceSelection(String query, String selection, int expectedSources) throws Exception {
        PlanSink plan = getPlanSink(query);
        String planText = planText(plan);
        int sources = 0;
        for (int i = 1; i <= plan.getLineCount(); i++) {
            String line = plan.getLine(i).toString();
            if (!line.contains("scan on: t")) {
                continue;
            }
            sources++;
            boolean hasSelection = line.contains(selection);
            int ancestorIndent = line.length() - line.stripLeading().length();
            // Walk only ancestors of this scan, not a selection in a previous sibling branch.
            for (int j = i - 1; j > 0 && !hasSelection; j--) {
                String ancestor = plan.getLine(j).toString();
                int indent = ancestor.length() - ancestor.stripLeading().length();
                if (indent < ancestorIndent) {
                    hasSelection = ancestor.contains(selection);
                    ancestorIndent = indent;
                }
            }
            Assert.assertTrue("Missing " + selection + " above source " + sources + ":\n" + planText, hasSelection);
        }
        Assert.assertEquals(planText, expectedSources, sources);
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE t AS (SELECT x::TIMESTAMP ts, x FROM long_sequence(200)) TIMESTAMP(ts)");
        execute("CREATE TABLE u AS (SELECT x::TIMESTAMP ts FROM long_sequence(50)) TIMESTAMP(ts)");
    }

    private static String planText(PlanSink sink) {
        StringBuilder plan = new StringBuilder();
        for (int i = 1; i <= sink.getLineCount(); i++) {
            plan.append(sink.getLine(i)).append('\n');
        }
        return plan.toString();
    }
}
