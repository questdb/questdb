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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
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
    public void testInternalRetryAfterPrimaryGenerationControl() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (RetryContext context = new RetryContext(); RetryCompiler compiler = new RetryCompiler()) {
                String query = "SELECT o.ts, o.x, l.c FROM "
                        + "(SELECT ts, x FROM t WHERE x IN (1, 101, 200)) o "
                        + "JOIN LATERAL (SELECT count() c FROM u WHERE u.ts <= o.ts) l ON true ORDER BY o.x";
                assertQuery(query).withCompiler(compiler).withContext(context).returns(SELECTED_RESULT);
                Assert.assertTrue("must inject after compiling the primary t source", context.hasInjected);
                Assert.assertTrue("must retry inside one compile", compiler.attempts >= 2);
                Assert.assertTrue("retry must reuse pooled root identity", compiler.hasReusedRoot);
                // Reuse the same compiler with a different logical predicate after the retry.
                assertQuery("SELECT x FROM t WHERE x = 200").withCompiler(compiler).withContext(context)
                        .returns("x\n200\n");
            }
        });
    }

    @Test
    public void testLimitAdviceMarkerProtocolWithDetachedPredicate() {
        // Design probe, not a production restoration test. DECLARE substitution can put
        // the same node in a predicate and LIMIT. Detach predicates, not LIMIT/advice.
        ObjectPool<ExpressionNode> pool = new ObjectPool<>(ExpressionNode.FACTORY, 16);
        ExpressionNode limit = pool.next().of(ExpressionNode.CONSTANT, "1", 0, 0);
        QueryModel parent = QueryModel.FACTORY.newInstance();
        QueryModel nested = QueryModel.FACTORY.newInstance();
        parent.setNestedModel(nested);
        parent.setLimit(limit, null);
        nested.setLimitAdvice(limit, null);
        nested.setWhereClause(limit);
        ExpressionNode pristinePredicate = ExpressionNode.deepClone(pool, nested.getWhereClause());
        ExpressionNode retainedFactoryPredicate = null;
        for (int generation = 0; generation < 3; generation++) {
            // Region preparation creates fresh predicate nodes but resets only LIMIT markers.
            nested.setWhereClause(ExpressionNode.deepClone(pool, pristinePredicate));
            parent.getLimitLo().implemented = false;
            Assert.assertSame(parent.getLimitLo(), nested.getLimitAdviceLo());
            Assert.assertFalse(nested.getLimitAdviceLo().implemented);
            Assert.assertNotSame(limit, nested.getWhereClause());
            Assert.assertNotSame(retainedFactoryPredicate, nested.getWhereClause());
            if (retainedFactoryPredicate != null) {
                Assert.assertEquals("consumed", retainedFactoryPredicate.token);
            }
            // A nested regeneration must not sever its still-active parent's handoff.
            nested.getLimitAdviceLo().implemented = false;
            nested.getLimitAdviceLo().implemented = true;
            Assert.assertTrue(parent.getLimitLo().implemented);
            nested.getWhereClause().token = "consumed";
            retainedFactoryPredicate = nested.getWhereClause();
            nested.setWhereClause(null);
            Assert.assertEquals("1", limit.token);
        }
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

    private static class RetryCompiler extends SqlCompilerImpl {
        private int attempts;
        private IQueryModel firstRoot;
        private boolean hasReusedRoot;

        private RetryCompiler() {
            super(AbstractCairoTest.engine);
        }

        @Override
        protected RecordCursorFactory generateSelectOneShot(IQueryModel model, SqlExecutionContext context, boolean isProgressLogger) throws SqlException {
            attempts++;
            if (firstRoot == null) {
                firstRoot = model;
            } else if (attempts == 2) {
                hasReusedRoot = firstRoot == model;
            }
            Assert.assertEquals("previous attempt released its archive", 0,
                    codeGenerator.getGenerationStateForTesting().getRetainedNodeCount());
            try {
                return super.generateSelectOneShot(model, context, isProgressLogger);
            } finally {
                Assert.assertEquals("success and failure release the archive", 0,
                        codeGenerator.getGenerationStateForTesting().getRetainedNodeCount());
            }
        }
    }

    private static class RetryContext extends SqlExecutionContextImpl {
        private boolean hasInjected;
        private boolean hasReadPrimary;

        private RetryContext() {
            super(AbstractCairoTest.engine, 1);
            with(AllowAllSecurityContext.INSTANCE);
        }

        @Override
        public TableReader getReader(TableToken token, long version) {
            if (token.getTableName().equals("t")) {
                hasReadPrimary = true;
            } else if (token.getTableName().equals("u") && hasReadPrimary && !hasInjected) {
                hasInjected = true;
                throw TableReferenceOutOfDateException.of(token);
            }
            return super.getReader(token, version);
        }
    }
}
