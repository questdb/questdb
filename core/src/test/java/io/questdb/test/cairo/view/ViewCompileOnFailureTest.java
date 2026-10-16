/*******************************************************************************
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

package io.questdb.test.cairo.view;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.cairo.view.ViewCompilerTask;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.view.ViewGraph;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

import java.util.TreeMap;
import java.util.function.UnaryOperator;

/**
 * A statement that fails to compile signals a recompile for every view it reads, once per view,
 * however deep in nested sub-queries the statement reads it. Each test derives the expected views
 * from its own DDL and statement text: the views the statement names, plus the views those views
 * read.
 * <p>
 * The same nesting shapes also drive {@link SqlUtil#collectAllTableAndViewNames}, the walk that
 * SHOW CREATE DATABASE uses to emit a view before a materialized view that reads it. Each shape
 * reaches the view through a different route of that walk.
 */
public class ViewCompileOnFailureTest extends AbstractViewTest {
    private static final int DEPTH = 16;
    private static final String FAILING_SELECT_PREFIX = "SELECT no_such_col FROM (";
    private static final String NO_SUCH_COL = "Invalid column: no_such_col";
    // Every shape gets through the optimiser, which is as far as these tests compile. Code
    // generation rejects EXISTS and a sub-query LIMIT, so no materialized view carries those two
    // shapes; a failing statement does.
    private static final Shape[] SHAPES = {
            new Shape("IN", "", ViewCompileOnFailureTest::wrapIn, "SELECT s FROM v"),
            new Shape("NOT IN", "", q -> "SELECT s FROM t WHERE s NOT IN (" + q + ")", "SELECT s FROM v"),
            new Shape("scalar", "", q -> "SELECT max(x) x FROM t WHERE x > (" + q + ")", "SELECT max(x) x FROM v"),
            new Shape("select-list scalar", "", q -> "SELECT CASE WHEN x = (" + q + ") THEN x END x FROM t LIMIT 1", "SELECT max(x) x FROM v"),
            new Shape("EXISTS", "", q -> "SELECT s FROM t WHERE EXISTS (" + q + ")", "SELECT s FROM v"),
            new Shape("LIMIT", "", q -> "SELECT count() c FROM t LIMIT (" + q + ")", "SELECT count() c FROM v"),
            new Shape("FROM", "", q -> "SELECT s FROM (" + q + ") WHERE s != 'z'", "SELECT s FROM v"),
            new Shape("JOIN", "", q -> "SELECT s FROM t WHERE s IN (SELECT a.s FROM t a JOIN (" + q + ") b ON a.s = b.s)", "SELECT s FROM v"),
            new Shape("UNION ALL", "", q -> "SELECT s FROM t WHERE s IN (SELECT s FROM t UNION ALL " + q + ")", "SELECT s FROM v"),
            new Shape("EXCEPT", "", q -> "SELECT s FROM t WHERE s IN (SELECT s FROM t EXCEPT " + q + ")", "SELECT s FROM v"),
            new Shape("INTERSECT", "", q -> "SELECT s FROM t WHERE s IN (SELECT s FROM t INTERSECT " + q + ")", "SELECT s FROM v"),
            new Shape("LATERAL", "", ViewCompileOnFailureTest::wrapIn, "SELECT a.s FROM t a JOIN LATERAL (SELECT s FROM v WHERE v.x = a.x) l ON true"),
            // the CTE body reads v, and the innermost level reads the CTE
            new Shape("CTE", "WITH c AS (SELECT s FROM v) ", ViewCompileOnFailureTest::wrapIn, "SELECT s FROM c"),
            // the declared sub-query reads v, and the innermost level reads the declaration
            new Shape("DECLARE", "DECLARE @q := (SELECT s FROM v) ", ViewCompileOnFailureTest::wrapIn, "SELECT s FROM t WHERE s IN @q"),
    };
    private static final String V_ROWS = """
            s\tx
            a\t1
            b\t2
            """;

    @Test
    public void testDistinctViewsAtEveryLevelQueuedOnceEach() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            final int depth = 20;
            final StringBuilder expected = new StringBuilder();
            for (int i = 0; i < depth; i++) {
                execute("CREATE VIEW v" + i + " AS (SELECT s, x, ts FROM t)");
                expected.append("v").append(i).append("=1\n");
            }
            drainWalAndViewQueues();
            // v0 sits at the outermost level and v19 at the innermost one
            String q = "SELECT s FROM v" + (depth - 1);
            for (int i = depth - 2; i >= 0; i--) {
                q = "SELECT s FROM v" + i + " WHERE s IN (" + q + ")";
            }
            assertFailingSelect(q);
            // drained tokens come back sorted by name, so v10..v19 come between v1 and v2
            Assert.assertEquals(sortedCounts(expected.toString()), drainCompileTasks());
            assertViewServesRows();
        });
    }

    @Test
    public void testFailedExplainQueuesNothing() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            final String nest = nest(ViewCompileOnFailureTest::wrapIn, "SELECT s FROM v");
            assertExceptionNoLeakCheck("EXPLAIN " + FAILING_SELECT_PREFIX + nest + ")", 15, NO_SUCH_COL);
            Assert.assertEquals("", drainCompileTasks());
            assertViewServesRows();
        });
    }

    @Test
    public void testFailedInsertAfterOptimiseQueuesViewOnce() throws Exception {
        // authorizeInsert() runs after the INSERT has swapped in the optimised SELECT model, so the
        // failure path reads the views off the optimiser's root rather than the parser's
        assertMemoryLeak(() -> {
            createFixture();
            createTable2();
            final String nest = nest(ViewCompileOnFailureTest::wrapIn, "SELECT s FROM v");
            assertExceptionNoLeakCheck(
                    "INSERT INTO t2 SELECT * FROM t WHERE s IN (" + nest + ")",
                    0,
                    "Write permission denied",
                    readOnlyContext()
            );
            Assert.assertEquals("v=1\n", drainCompileTasks());
            assertViewServesRows();
            assertQuery("SELECT count() FROM t2").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    @Test
    public void testFailedInsertReadingLateralNestQueuesViewOnce() throws Exception {
        // a LATERAL join over a sub-query shares that sub-query with the outer reference the
        // decorrelation builds, so the optimised model reaches the innermost level 3^12 ways
        assertMemoryLeak(() -> {
            createFixture();
            createTable2();
            String q = "SELECT s, x, ts FROM v";
            for (int i = 0; i < 12; i++) {
                q = "SELECT o.s, o.x, o.ts FROM (" + q + ") o JOIN LATERAL (SELECT count() c FROM t WHERE t.ts <= o.ts) l ON true";
            }
            assertExceptionNoLeakCheck("INSERT INTO t2 SELECT * FROM (" + q + ")", 0, "Write permission denied", readOnlyContext());
            Assert.assertEquals("v=1\n", drainCompileTasks());
            assertViewServesRows();
            assertQuery("SELECT count() FROM t2").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    @Test
    public void testFailedInsertAsSelectQueuesViewOnce() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            createTable2();
            final String nest = nest(ViewCompileOnFailureTest::wrapIn, "SELECT s FROM v");
            // two target columns, one selected column
            assertExceptionNoLeakCheck(
                    "INSERT INTO t2 (s, x) SELECT s FROM t WHERE s IN (" + nest + ")",
                    12,
                    "column count mismatch"
            );
            Assert.assertEquals("v=1\n", drainCompileTasks());
            assertViewServesRows();
            assertQuery("SELECT count() FROM t2").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    @Test
    public void testFailedReadOfObjectKindsFromSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            execute("CREATE VIEW av AS (SELECT s, x, ts FROM t)");
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT ts, s, count() c FROM t SAMPLE BY 1d) PARTITION BY DAY");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, s, count(*) OVER (PARTITION BY s ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM t");
            drainWalAndViewQueues();
            drainWalAndMatViewQueues();
            markViewAudited("av");
            Assert.assertEquals("", drainCompileTasks());

            // only views expand into the statement, so only the plain and the audited view are
            // compile targets; the table, the materialized view and the live view are not views
            final String sql = "SELECT no_such_col FROM t WHERE "
                    + "s IN (" + nest(ViewCompileOnFailureTest::wrapIn, "SELECT s FROM t") + ") AND "
                    + "s IN (" + nest(ViewCompileOnFailureTest::wrapIn, "SELECT s FROM v") + ") AND "
                    + "s IN (" + nest(ViewCompileOnFailureTest::wrapIn, "SELECT s FROM av") + ") AND "
                    + "s IN (" + nest(ViewCompileOnFailureTest::wrapIn, "SELECT s FROM mv") + ") AND "
                    + "s IN (" + nest(ViewCompileOnFailureTest::wrapIn, "SELECT s FROM lv") + ")";
            assertExceptionNoLeakCheck(sql, 7, NO_SUCH_COL);
            Assert.assertEquals("av=1\nv=1\n", drainCompileTasks());
            assertViewServesRows();
        });
    }

    @Test
    public void testFailedReadOfUnusedCteQueuesView() throws Exception {
        // the parser expands every CTE body, a body no reference reads included, so the statement
        // reads v even though no row of v reaches its result
        assertMemoryLeak(() -> {
            createFixture();
            assertExceptionNoLeakCheck("WITH u AS (SELECT s FROM v) SELECT no_such_col FROM t", 35, NO_SUCH_COL);
            Assert.assertEquals("v=1\n", drainCompileTasks());
            assertViewServesRows();
        });
    }

    @Test
    public void testFailedReadQueuesViewOncePerShape() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            for (Shape shape : SHAPES) {
                final String sql = shape.prefix() + FAILING_SELECT_PREFIX + nest(shape.wrap(), shape.innermost(), DEPTH) + ")";
                assertExceptionNoLeakCheck(sql, shape.prefix().length() + 7, NO_SUCH_COL);
                Assert.assertEquals("shape " + shape.name(), "v=1\n", drainCompileTasks());
            }
            assertViewServesRows();
        });
    }

    @Test
    public void testFailedSelectQueuesViewOnce() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            assertFailingSelect(nest(ViewCompileOnFailureTest::wrapIn, "SELECT s FROM v"));
            Assert.assertEquals("v=1\n", drainCompileTasks());
            assertViewServesRows();

            // the same statement at depth 1
            assertFailingSelect(wrapIn("SELECT s FROM v"));
            Assert.assertEquals("v=1\n", drainCompileTasks());
            assertViewServesRows();
        });
    }

    @Test
    public void testFailedUpdateQueuesViewOnce() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            createTable2();
            execute("INSERT INTO t2 VALUES ('a', 1, '2024-01-01T00:00:00.000000Z')");
            final String nest = nest(ViewCompileOnFailureTest::wrapIn, "SELECT s FROM v");
            assertExceptionNoLeakCheck(
                    "UPDATE t2 SET x = no_such_col WHERE s IN (" + nest + ")",
                    18,
                    NO_SUCH_COL
            );
            Assert.assertEquals("v=1\n", drainCompileTasks());
            assertViewServesRows();
            assertQuery("SELECT s, x FROM t2").noLeakCheck().expectSize().returns("s\tx\na\t1\n");
        });
    }

    @Test
    public void testNoViewQueuesNothing() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            assertFailingSelect(nest(ViewCompileOnFailureTest::wrapIn, "SELECT s FROM t"));
            Assert.assertEquals("", drainCompileTasks());
        });
    }

    @Test
    public void testNonAsciiViewNameQueuedOnce() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            execute("CREATE VIEW \"vüé視\" AS (SELECT s, x, ts FROM t)");
            drainWalAndViewQueues();
            assertFailingSelect(nest(ViewCompileOnFailureTest::wrapIn, "SELECT s FROM \"vüé視\""));
            Assert.assertEquals("vüé視=1\n", drainCompileTasks());
        });
    }

    @Test
    public void testParseFailureQueuesNothing() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            final String nest = nest(ViewCompileOnFailureTest::wrapIn, "SELECT s FROM v");
            final String sql = "SELECT s FROM (" + nest + ") LIMIT";
            assertExceptionNoLeakCheck(sql, sql.lastIndexOf("LIMIT"), "limit expression expected");
            Assert.assertEquals("", drainCompileTasks());
            assertViewServesRows();
        });
    }

    @Test
    public void testReferencedViewsDistinctAtDepth() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final ExecutionModel model = compiler.generateExecutionModel(
                        nest(ViewCompileOnFailureTest::wrapIn, "SELECT s FROM v"),
                        sqlExecutionContext
                );
                final ObjList<ViewDefinition> referencedViews = model.getQueryModel().getReferencedViews();
                Assert.assertEquals(1, referencedViews.size());
                Assert.assertEquals("v", referencedViews.getQuick(0).getViewToken().getTableName());
            }
        });
    }

    @Test
    public void testSameViewAtEveryLevelQueuedOnce() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            assertFailingSelect(nest(q -> "SELECT s FROM v WHERE s IN (" + q + ")", "SELECT s FROM v"));
            Assert.assertEquals("v=1\n", drainCompileTasks());
            assertViewServesRows();
        });
    }

    @Test
    public void testViewChainReadFromSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            execute("CREATE VIEW w AS (SELECT s, x, ts FROM v)");
            drainWalAndViewQueues();
            assertFailingSelect(nest(ViewCompileOnFailureTest::wrapIn, "SELECT s FROM w"));
            Assert.assertEquals("v=1\nw=1\n", drainCompileTasks());
            assertViewServesRows();
        });
    }

    @Test
    public void testViewNamedInDifferentCaseAndQuotesQueuedOnce() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            final String nest = nest(ViewCompileOnFailureTest::wrapIn, "SELECT s FROM v");
            assertFailingSelect("SELECT s FROM \"v\" WHERE s IN (SELECT s FROM V WHERE s IN (" + nest + "))");
            Assert.assertEquals("v=1\n", drainCompileTasks());
            assertViewServesRows();
        });
    }

    @Test
    public void testWalkReadsViewOnceAsLateralOuterSource() throws Exception {
        // the LATERAL rewrite shares the outer source, here the view's own model, with the outer
        // reference it builds, through a QueryModelWrapper that reports the view's name
        assertMemoryLeak(() -> {
            createFixture();
            final String lateral = "SELECT o.s FROM v o JOIN LATERAL (SELECT count() c FROM t WHERE t.ts <= o.ts) l ON true";
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final ObjList<CharSequence> names = new ObjList<>();
                for (int depth : new int[]{0, DEPTH}) {
                    final String sql = nest(ViewCompileOnFailureTest::wrapIn, lateral, depth);
                    final ExecutionModel model = compiler.generateExecutionModel(sql, sqlExecutionContext);
                    names.clear();
                    SqlUtil.collectAllTableAndViewNames(model.getQueryModel(), names, true);
                    Assert.assertEquals("depth " + depth, "[v]", names.toString());
                }
            }
            Assert.assertEquals("", drainCompileTasks());
        });
    }

    @Test
    public void testWalkReadsViewOncePerShape() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final ObjList<CharSequence> names = new ObjList<>();
                for (Shape shape : SHAPES) {
                    // the statement text names v once, so the walk reads it once at any depth
                    for (int depth : new int[]{1, DEPTH}) {
                        final String sql = shape.prefix() + nest(shape.wrap(), shape.innermost(), depth);
                        final ExecutionModel model = compiler.generateExecutionModel(sql, sqlExecutionContext);
                        names.clear();
                        SqlUtil.collectAllTableAndViewNames(model.getQueryModel(), names, true);
                        Assert.assertEquals("shape " + shape.name() + ", depth " + depth, "[v]", names.toString());
                    }
                }
            }
            Assert.assertEquals("", drainCompileTasks());
        });
    }

    private static void assertFailingSelect(String nest) throws Exception {
        assertExceptionNoLeakCheck(FAILING_SELECT_PREFIX + nest + ")", 7, NO_SUCH_COL);
    }

    private void assertViewServesRows() throws Exception {
        drainViewQueue();
        assertQuery("SELECT s, x FROM v").noLeakCheck().expectSize().returns(V_ROWS);
        assertViewState("v");
    }

    private static void createTable2() throws SqlException {
        execute("CREATE TABLE t2 (s SYMBOL, x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        drainViewQueue();
        Assert.assertEquals("", drainCompileTasks());
    }

    private static void createFixture() throws SqlException {
        execute("CREATE TABLE t (s SYMBOL, x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO t VALUES ('a', 1, '2024-01-01T00:00:00.000000Z'), ('b', 2, '2024-01-02T00:00:00.000000Z')");
        drainWalQueue();
        execute("CREATE VIEW v AS (SELECT s, x, ts FROM t)");
        drainWalAndViewQueues();
        Assert.assertEquals("", drainCompileTasks());
    }

    /**
     * Drains the view compile queue and renders each queued view with the number of times it was
     * queued, one {@code name=count} line per view, sorted by name.
     */
    private static String drainCompileTasks() {
        final TreeMap<String, Integer> counts = new TreeMap<>();
        final ViewCompilerTask task = new ViewCompilerTask();
        while (engine.getViewStateStore().tryDequeueCompilerTask(task)) {
            counts.merge(task.tableToken.getTableName(), 1, Integer::sum);
        }
        final StringBuilder sb = new StringBuilder();
        counts.forEach((name, count) -> sb.append(name).append('=').append(count).append('\n'));
        return sb.toString();
    }

    private static void markViewAudited(String viewName) {
        final TableToken viewToken = engine.verifyTableName(viewName);
        final ViewGraph viewGraph = engine.getViewGraph();
        final ViewDefinition current = viewGraph.getViewDefinition(viewToken);
        final ViewDefinition audited = new ViewDefinition();
        audited.init(viewToken, current.getViewSql(), current.getDependencies(), current.getSeqTxn(), true);
        viewGraph.removeView(viewToken);
        Assert.assertTrue(viewGraph.addView(audited));
    }

    private static String nest(UnaryOperator<String> wrap, String innermost) {
        return nest(wrap, innermost, DEPTH);
    }

    private static String nest(UnaryOperator<String> wrap, String innermost, int depth) {
        String q = innermost;
        for (int i = 0; i < depth; i++) {
            q = wrap.apply(q);
        }
        return q;
    }

    private static SqlExecutionContext readOnlyContext() {
        return new SqlExecutionContextImpl(engine, 1).with(
                ReadOnlySecurityContext.INSTANCE,
                bindVariableService,
                null,
                -1,
                null
        );
    }

    private static String sortedCounts(String lines) {
        final TreeMap<String, String> sorted = new TreeMap<>();
        for (String line : lines.split("\n")) {
            sorted.put(line.substring(0, line.indexOf('=')), line);
        }
        final StringBuilder sb = new StringBuilder();
        sorted.forEach((name, line) -> sb.append(line).append('\n'));
        return sb.toString();
    }

    private static String wrapIn(String q) {
        return "SELECT s FROM t WHERE s IN (" + q + ")";
    }

    /**
     * One way a statement reads v from deep inside nested sub-queries: an optional prefix (a CTE or
     * a declared sub-query that reads v), how one level wraps the next, and the innermost query.
     */
    private record Shape(String name, String prefix, UnaryOperator<String> wrap, String innermost) {
    }
}
