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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;

import org.junit.Assert;
import org.junit.Test;

public class LateralOuterSourceBoundaryTest extends AbstractCairoTest {
    private static final String SELECTED_RESULT = """
            ts\tx\tc
            1970-01-01T00:00:00.000001Z\t1\t1
            1970-01-01T00:00:00.000101Z\t101\t50
            1970-01-01T00:00:00.000200Z\t200\t50
            """;

    @Test
    public void testBoundLimitsAndFactoryReuseWithChangedInput() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            bindVariableService.setLong("lo", 1);
            bindVariableService.setLong("hi", 2);
            assertQuery(repeated("(SELECT ts, x FROM t WHERE x IN (1, 101, 200) LIMIT :lo, :hi)"))
                    .returns("ts\tx\tc\td\n1970-01-01T00:00:00.000101Z\t101\t50\t50\n");
            assertQuery(repeated("(SELECT ts, x FROM t WHERE x >= 200 LIMIT 1)"))
                    .mutateWith("INSERT INTO u VALUES (150)")
                    .returns("ts\tx\tc\td\n1970-01-01T00:00:00.000200Z\t200\t50\t50\n",
                            "ts\tx\tc\td\n1970-01-01T00:00:00.000200Z\t200\t51\t51\n");
        });
    }

    @Test
    public void testGroupedHeadFilteredUnionAllTailRepeated() throws Exception {
        assertGroupedHeadFilteredTail("UNION ALL");
    }

    @Test
    public void testGroupedHeadFilteredUnionTailRepeated() throws Exception {
        assertGroupedHeadFilteredTail("UNION");
    }

    @Test
    public void testGroupedHeadNestedUnionAllTailRepeated() throws Exception {
        assertGroupedHeadNestedTail("UNION ALL");
    }

    @Test
    public void testGroupedHeadNestedUnionTailRepeated() throws Exception {
        assertGroupedHeadNestedTail("UNION");
    }

    @Test
    public void testInternalRetryAfterPrimaryBindingControl() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (RetryContext context = new RetryContext(); SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                String query = "SELECT o.ts, o.x, l.c FROM "
                        + "(SELECT ts, x FROM t WHERE x IN (1, 101, 200)) o "
                        + "JOIN LATERAL (SELECT count() c FROM u WHERE u.ts <= o.ts) l ON true ORDER BY o.x";
                assertQuery(query).withCompiler(compiler).returns(SELECTED_RESULT);
                final int modelPoolCapacity = compiler.getQueryModelPoolCapacity();
                assertQuery(query).withCompiler(compiler).withContext(context).returns(SELECTED_RESULT);
                Assert.assertTrue("must inject after binding the primary t source", context.hasInjected);
                Assert.assertTrue("must bind again inside one compile", context.primaryReadsAfterInjection > 0);
                Assert.assertEquals("retry must reuse pooled models", modelPoolCapacity, compiler.getQueryModelPoolCapacity());
                assertQuery("SELECT x FROM t WHERE x = 200").withCompiler(compiler).withContext(context)
                        .returns("x\n200\n");
            }
        });
    }

    @Test
    public void testMaterializedSelectionMatrix() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE bounds AS (SELECT ts, x FROM t WHERE x IN (101, 103)) TIMESTAMP(ts)");
            execute("CREATE TABLE latest_source AS (SELECT ts, x, x % 2 k FROM t) TIMESTAMP(ts)");
            ObjList<String> sources = new ObjList<>();
            sources.add("SELECT ts, x FROM t WHERE x IN (101, 200) LIMIT -1");
            sources.add("SELECT ts, x FROM t WHERE x IN (101, 200) ORDER BY ts DESC LIMIT 1");
            sources.add("SELECT ts, x FROM t WHERE x IN (1, 101, 200) LIMIT 1, 2");
            sources.add("SELECT ts, x FROM t WHERE ts BETWEEN 101::TIMESTAMP AND 103::TIMESTAMP LIMIT 1");
            sources.add("SELECT ts, x FROM t WHERE ts BETWEEN (SELECT min(ts) FROM bounds) AND (SELECT max(ts) FROM bounds) AND x > 100 LIMIT 1");
            sources.add("SELECT ts, x FROM latest_source WHERE x > 100 LATEST ON ts PARTITION BY k LIMIT 1");
            sources.add("SELECT ts, x FROM latest_source WHERE false LATEST ON ts PARTITION BY k LIMIT 1");
            sources.add("SELECT ts, x FROM t WHERE false LIMIT 1");
            sources.add("SELECT ts, x FROM (SELECT ts, x FROM t WHERE false) SUBSAMPLE uniform(3)");
            sources.add("SELECT ts, x FROM (SELECT ts, x FROM t WHERE x = 101 UNION SELECT ts, x FROM t WHERE x = 200) ORDER BY ts LIMIT 1");
            for (int i = 0; i < sources.size(); i++) {
                String source = sources.getQuick(i);
                execute("CREATE TABLE selected_" + i + " AS (" + source + ")");
                String control = repeated("selected_" + i);
                printSql(control);
                String expected = sink.toString();
                assertQuery(control).returns(expected);
                assertQuery(repeated("(" + source + ")")).returns(expected);
            }
        });
    }

    @Test
    public void testMultiworkerJitAndParallelModes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            TestWorkerPool pool = new TestWorkerPool(4);
            TestUtils.setupWorkerPool(pool, engine);
            pool.start(LOG);
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 4)) {
                context.with(AllowAllSecurityContext.INSTANCE);
                context.changePageFrameSizes(10, 20);
                // Per-query leak checks cannot inspect pools while worker jobs are active.
                // The enclosing scope checks native memory and busy resources after halt.
                for (int mode = 0; mode < 4; mode++) {
                    boolean isParallel = (mode & 1) != 0;
                    context.setParallelFilterEnabled(isParallel);
                    context.setParallelGroupByEnabled(isParallel);
                    context.setJitMode((mode & 2) != 0 ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
                    String query = repeated("(SELECT ts, x FROM t WHERE x IN (101, 200) LIMIT 1)");
                    assertQuery(query).withContext(context).noLeakCheck()
                            .returns("ts\tx\tc\td\n1970-01-01T00:00:00.000101Z\t101\t50\t50\n");
                    assertQuery(repeated("(SELECT ts, x FROM t SUBSAMPLE uniform(3) LIMIT 1, 2)"))
                            .withContext(context).noLeakCheck()
                            .returns("ts\tx\tc\td\n1970-01-01T00:00:00.000101Z\t101\t50\t50\n");
                }
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testNullableDuplicateKeysAndEmptyInner() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE nullable (x LONG)");
            execute("INSERT INTO nullable VALUES (NULL), (101), (101), (200)");
            execute("CREATE TABLE inner_values (x LONG)");
            String query = "SELECT o.x, l.c FROM (SELECT x FROM nullable WHERE x IS NULL OR x = 101) o "
                    + "JOIN LATERAL (SELECT count() c FROM inner_values i WHERE i.x <= o.x) l ON true ORDER BY o.x";
            assertQuery(query).mutateWith("INSERT INTO inner_values VALUES (1), (NULL), (100)")
                    .returns("x\tc\nnull\t0\n101\t0\n101\t0\n", "x\tc\nnull\t1\n101\t2\n101\t2\n");
            // QuestDB compares two LONG null sentinels as equal, including <=.
            assertQuery("SELECT count() c FROM inner_values WHERE x <= NULL::LONG").noRandomAccess().expectSize().returns("c\n1\n");
            execute("CREATE TABLE selected_nullable AS (SELECT x FROM nullable WHERE x IS NULL OR x = 101)");
            assertQuery(query.replace("(SELECT x FROM nullable WHERE x IS NULL OR x = 101)", "selected_nullable"))
                    .returns("x\tc\nnull\t1\n101\t2\n101\t2\n");
        });
    }

    @Test
    public void testPostingDistinctAndRejectedFastPath() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE symbols (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, extra DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO symbols VALUES (1, 'A', 10), (2, 'BB', 20), (3, 'A', 30), (4, 'CCC', 40)");
            execute("CREATE TABLE inner_symbols AS (SELECT * FROM symbols)");
            for (int i = 0; i < 2; i++) {
                String predicate = i == 0 ? "ts BETWEEN 1::TIMESTAMP AND 3::TIMESTAMP" : "sym IN ('A','BB') AND extra > 5";
                String source = "SELECT sym::STRING sym FROM (SELECT DISTINCT sym FROM symbols WHERE " + predicate + ")";
                String query = "SELECT o.sym, l.c FROM (" + source + ") o "
                        + "JOIN LATERAL (SELECT count() c FROM inner_symbols i WHERE length(i.sym) <= length(o.sym)) l ON true ORDER BY o.sym";
                execute("CREATE TABLE selected_symbols_" + i + " AS (" + source + ")");
                assertQuery(query.replace("(" + source + ")", "selected_symbols_" + i))
                        .returns("sym\tc\nA\t2\nBB\t3\n");
                assertQuery(query).returns("sym\tc\nA\t2\nBB\t3\n");
                if (i == 0) {
                    assertQuery(query).assertsPlanContaining("PostingIndex op: distinct", "Interval forward scan on: symbols");
                } else {
                    assertQuery(query).assertsPlanNotContaining("PostingIndex op: distinct");
                }
                assertPostingSelection(query, i == 0);
            }
        });
    }

    @Test
    public void testRepeatedDeclaredLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("DECLARE @n := 101 SELECT o.ts, o.x, l.c, r.c AS d FROM "
                    + "(SELECT ts, x FROM t WHERE x >= @n LIMIT @n - 100) o "
                    + "JOIN LATERAL (SELECT count() c FROM u WHERE u.ts <= o.ts) l ON true "
                    + "JOIN LATERAL (SELECT count() c FROM u WHERE u.ts < o.ts) r ON true ORDER BY o.x")
                    .returns("""
                            ts\tx\tc\td
                            1970-01-01T00:00:00.000101Z\t101\t50\t50
                            """);
        });
    }

    @Test
    public void testRepeatedFilteredLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery(repeated("(SELECT ts, x FROM t WHERE x IN (101, 200) LIMIT 1)"))
                    .returns("""
                            ts\tx\tc\td
                            1970-01-01T00:00:00.000101Z\t101\t50\t50
                            """);
        });
    }

    @Test
    public void testRepeatedGroupedLimitControl() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            String query = repeated("(SELECT ts, x FROM "
                    + "(SELECT ts, max(x) AS x FROM t WHERE x IN (1, 101, 200) GROUP BY ts) ORDER BY ts LIMIT 1, 2)");
            // The sort-limit wrapper regenerates this grouped source; it is not a cache-hit control.
            assertQuery(query).returns("""
                    ts\tx\tc\td
                    1970-01-01T00:00:00.000101Z\t101\t50\t50
                    """);
        });
    }

    @Test
    public void testRepeatedSubsampleLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery(repeated("(SELECT ts, x FROM t SUBSAMPLE uniform(3) LIMIT 1, 2)"))
                    .withPlanContaining("CachedWindowLightSelect")
                    .returns("ts\tx\tc\td\n1970-01-01T00:00:00.000101Z\t101\t50\t50\n");
        });
    }

    @Test
    public void testScalarBoundFactoryReopenUsesChangedInput() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE bounds (ts TIMESTAMP)");
            execute("INSERT INTO bounds VALUES (101)");
            assertQuery(repeated("(SELECT ts, x FROM t WHERE ts >= (SELECT min(ts) FROM bounds) AND x > 0 LIMIT 1)"))
                    .mutateWith("INSERT INTO bounds VALUES (2)")
                    .returns("ts\tx\tc\td\n1970-01-01T00:00:00.000101Z\t101\t50\t50\n",
                            "ts\tx\tc\td\n1970-01-01T00:00:00.000002Z\t2\t2\t1\n");
        });
    }

    @Test
    public void testWindowJoinFilteredOuterMatchesMaterialized() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE trades AS (SELECT ts, x, (x % 2)::STRING::SYMBOL sym FROM t) TIMESTAMP(ts)");
            execute("CREATE TABLE prices AS (SELECT ts, x, (x % 2)::STRING::SYMBOL sym FROM t) TIMESTAMP(ts)");
            String source = "SELECT t.ts, sum(p.x) x FROM (SELECT * FROM trades WHERE x > 100) t "
                    + "WINDOW JOIN prices p ON (t.sym = p.sym) "
                    + "RANGE BETWEEN 0 MICROSECONDS PRECEDING AND 0 MICROSECONDS FOLLOWING EXCLUDE PREVAILING LIMIT 1";
            execute("CREATE TABLE selected_window AS (" + source + ")");
            printSql(repeated("selected_window"));
            String expected = sink.toString();
            assertQuery(repeated("selected_window")).returns(expected);
            assertQuery(repeated("(" + source + ")")).returns(expected);
        });
    }

    private static String repeated(String source) {
        return "SELECT o.ts, o.x, l.c, r.c AS d FROM " + source + " o "
                + "JOIN LATERAL (SELECT count() c FROM u WHERE u.ts <= o.ts) l ON true "
                + "JOIN LATERAL (SELECT count() c FROM u WHERE u.ts < o.ts) r ON true ORDER BY o.x";
    }

    private void assertGroupedHeadFilteredTail(String setOperation) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE tail AS (SELECT x::TIMESTAMP ts, x FROM long_sequence(200)) TIMESTAMP(ts)");
            String query = repeated("(SELECT ts, max(x) AS x FROM t WHERE x IN (1, 101) GROUP BY ts "
                    + setOperation + " SELECT ts, x FROM tail WHERE x = 200)");
            assertQuery(query).assertsPlanContaining("(Shared)");
            assertQuery(query).returns("""
                    ts\tx\tc\td
                    1970-01-01T00:00:00.000001Z\t1\t1\t0
                    1970-01-01T00:00:00.000101Z\t101\t50\t50
                    1970-01-01T00:00:00.000200Z\t200\t50\t50
                    """);
            assertTailSelection(query);
        });
    }

    private void assertGroupedHeadNestedTail(String setOperation) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE tail AS (SELECT x::TIMESTAMP ts, x FROM long_sequence(200)) TIMESTAMP(ts)");
            String nestedUnion = """
                    (SELECT ts, x FROM t WHERE x = 101
                     UNION ALL
                     SELECT ts, x FROM t WHERE x = 150)
                    """;
            for (int shape = 0; shape < 2; shape++) {
                String nestedBranch = shape == 0 ? nestedUnion : "SELECT * FROM " + nestedUnion;
                String query = repeated("(SELECT ts, max(x) AS x FROM t WHERE x = 1 GROUP BY ts "
                        + setOperation + " " + nestedBranch + " "
                        + setOperation + " SELECT ts, x FROM tail WHERE x = 200)");
                assertQuery(query).withPlanContaining("(Shared)").returns("""
                        ts\tx\tc\td
                        1970-01-01T00:00:00.000001Z\t1\t1\t0
                        1970-01-01T00:00:00.000101Z\t101\t50\t50
                        1970-01-01T00:00:00.000150Z\t150\t50\t50
                        1970-01-01T00:00:00.000200Z\t200\t50\t50
                        """);
                assertTailSelection(query);
            }
        });
    }

    private void assertTailSelection(String query) throws Exception {
        PlanSink plan = getPlanSink(query);
        StringBuilder text = new StringBuilder();
        for (int i = 1; i <= plan.getLineCount(); i++) {
            text.append(plan.getLine(i)).append('\n');
        }
        int sources = 0;
        for (int i = 1; i <= plan.getLineCount(); i++) {
            String line = plan.getLine(i).toString();
            if (!line.contains("scan on: tail")) {
                continue;
            }
            sources++;
            boolean hasSelection = false;
            int ancestorIndent = line.length() - line.stripLeading().length();
            for (int j = i - 1; j > 0 && !hasSelection; j--) {
                String ancestor = plan.getLine(j).toString();
                int indent = ancestor.length() - ancestor.stripLeading().length();
                if (indent < ancestorIndent) {
                    hasSelection = ancestor.contains("x=200");
                    ancestorIndent = indent;
                }
            }
            Assert.assertTrue("Missing tail selection above source " + sources + ":\n" + text, hasSelection);
        }
        Assert.assertEquals(text.toString(), 3, sources);
    }

    private void assertPostingSelection(String query, boolean isFastPath) throws Exception {
        PlanSink plan = getPlanSink(query);
        StringBuilder text = new StringBuilder();
        for (int i = 1; i <= plan.getLineCount(); i++) {
            text.append(plan.getLine(i)).append('\n');
        }
        int sources = 0;
        int distinctSources = 0;
        for (int i = 1; i <= plan.getLineCount(); i++) {
            String line = plan.getLine(i).toString();
            if (line.contains("PostingIndex op: distinct")) {
                distinctSources++;
            }
            if (!line.contains("scan on: symbols")) {
                continue;
            }
            sources++;
            boolean hasSelection = isFastPath && line.contains("Interval forward scan");
            int ancestorIndent = line.length() - line.stripLeading().length();
            for (int j = i - 1; j > 0 && !hasSelection; j--) {
                String ancestor = plan.getLine(j).toString();
                int indent = ancestor.length() - ancestor.stripLeading().length();
                if (indent < ancestorIndent) {
                    if (!isFastPath && ancestor.contains("FilterOnValues")) {
                        int filteredKeys = 0;
                        // The row selectors and the partition scan are siblings within this
                        // FilterOnValues, so inspect only this source's selector subtree.
                        for (int k = j + 1; k < i; k++) {
                            if (plan.getLine(k).toString().contains("and 5<extra")) {
                                filteredKeys++;
                            }
                        }
                        hasSelection = filteredKeys == 2;
                    }
                    ancestorIndent = indent;
                }
            }
            Assert.assertTrue(text.toString(), hasSelection);
        }
        Assert.assertEquals(text.toString(), 2, sources);
        Assert.assertEquals(text.toString(), isFastPath ? 2 : 0, distinctSources);
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE t AS (SELECT x::TIMESTAMP ts, x FROM long_sequence(200)) TIMESTAMP(ts)");
        execute("CREATE TABLE u AS (SELECT x::TIMESTAMP ts FROM long_sequence(50)) TIMESTAMP(ts)");
    }

    private static class RetryContext extends SqlExecutionContextImpl {
        private boolean hasInjected;
        private boolean hasReadPrimary;
        private int primaryReadsAfterInjection;

        private RetryContext() {
            super(AbstractCairoTest.engine, 1);
            with(AllowAllSecurityContext.INSTANCE);
        }

        @Override
        public TableReader getReader(TableToken token, long version) {
            inject(token);
            return super.getReader(token, version);
        }

        @Override
        public TableReader getReader(TableToken token) {
            inject(token);
            return super.getReader(token);
        }

        private void inject(TableToken token) {
            if (token.getTableName().equals("t")) {
                hasReadPrimary = true;
                if (hasInjected) {
                    primaryReadsAfterInjection++;
                }
            } else if (token.getTableName().equals("u") && hasReadPrimary && !hasInjected) {
                hasInjected = true;
                throw TableReferenceOutOfDateException.of(token);
            }
        }
    }
}
