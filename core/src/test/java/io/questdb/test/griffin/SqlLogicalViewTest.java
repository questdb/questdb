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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalViewTest extends AbstractCairoTest {
    @Test
    public void testExpandedQueriesAndNestedViews() throws Exception {
        assertMemoryLeak(() -> {
            createViews();
            assertView("SELECT * FROM lp_view_all", sqlExecutionContext, null, """
                    id	k	sym	active	ts
                    1	1	A	true	2020-01-01T00:00:01.000000Z
                    2	1	A	false	2020-01-01T00:00:02.000000Z
                    3	2	B	true	2020-01-02T00:00:01.000000Z
                    4	3	C	true	2020-01-02T00:00:02.000000Z
                    """);
            assertView("SELECT v.id,v.sym FROM lp_view_all v WHERE v.id>1 ORDER BY v.id DESC LIMIT 2", sqlExecutionContext, null, """
                    id	sym
                    4	C
                    3	B
                    """);
            assertView("SELECT id FROM lp_view_nested ORDER BY id", sqlExecutionContext, null, """
                    id
                    1
                    3
                    4
                    """);
            assertView("SELECT a.id aid,b.id bid FROM lp_view_keys a JOIN lp_view_keys b ON(id) ORDER BY aid", sqlExecutionContext, null, """
                    aid	bid
                    1	1
                    2	2
                    3	3
                    4	4
                    """);
            assertView("WITH q AS (SELECT id FROM lp_view_all WHERE id<3) SELECT * FROM q UNION ALL SELECT id FROM lp_view_all WHERE id>=3", sqlExecutionContext, null, """
                    id
                    1
                    2
                    3
                    4
                    """);
            assertView("SELECT k,sum(id) total FROM lp_view_all GROUP BY k ORDER BY k", sqlExecutionContext, null, """
                    k	total
                    1	3
                    2	3
                    3	4
                    """);
            assertView("EXPLAIN SELECT id FROM lp_view_nested WHERE id>1", sqlExecutionContext, null, """
                    QUERY PLAN
                    SelectedRecord
                        Async JIT Filter workers: 1
                          filter: (active and 1<id)
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_view_base
                    """);
        });
    }

    @Test
    public void testOuterViewAuthorizationAndHiddenTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createViews();
            final ViewSecurityContext nestedSecurity = new ViewSecurityContext("lp_view_nested", false);
            try (SqlExecutionContext context = newContext(nestedSecurity)) {
                assertView("SELECT id FROM lp_view_nested ORDER BY id", context, null, """
                        id
                        1
                        3
                        4
                        """);
                assertView("SELECT count() FROM lp_view_nested", context, null, """
                        count
                        3
                        """);
                Assert.assertTrue(nestedSecurity.viewAuthorizations > 0);
                Assert.assertEquals(0, nestedSecurity.baseColumns.size());
            }
            final ViewSecurityContext hiddenSecurity = new ViewSecurityContext("lp_view_keys", true);
            try (SqlExecutionContext context = newContext(hiddenSecurity)) {
                assertView("SELECT count() FROM lp_view_keys", context, null, """
                        count
                        4
                        """);
                Assert.assertEquals(0, hiddenSecurity.baseColumns.size());
                assertView("SELECT l.id lid,r.id rid FROM lp_view_keys l ASOF JOIN lp_view_keys r ON(sym)", context, "AsOf Join Fast", """
                        lid	rid
                        1	1
                        2	2
                        3	3
                        4	4
                        """);
                Assert.assertTrue(hiddenSecurity.viewAuthorizations > 0);
                Assert.assertEquals(1, hiddenSecurity.baseColumns.size());
                Assert.assertTrue(hiddenSecurity.baseColumns.contains("ts"));
            }
        });
    }

    @Test
    public void testPostingAndIntervalScansAuthorizeTheView() throws Exception {
        assertMemoryLeak(() -> {
            createViews();
            final ViewSecurityContext security = new ViewSecurityContext("lp_view_all", false);
            try (SqlExecutionContext context = newContext(security)) {
                assertView("SELECT DISTINCT sym FROM lp_view_all ORDER BY sym", context, "GroupBy vectorized", """
                        sym
                        A
                        B
                        C
                        """);
                assertView("SELECT DISTINCT sym FROM lp_view_all WHERE ts>='2020-01-01T00:00:02Z' ORDER BY sym", context, "GroupBy vectorized", """
                        sym
                        A
                        B
                        C
                        """);
                assertView("SELECT DISTINCT sym FROM (SELECT sym FROM lp_view_all LIMIT 2) ORDER BY sym", context, null, """
                        sym
                        A
                        """);
                assertView("SELECT id FROM lp_view_all WHERE ts>='2020-01-01T00:00:02Z' ORDER BY ts DESC", context, "Interval backward scan", """
                        id
                        4
                        3
                        2
                        """);
                Assert.assertTrue(security.viewAuthorizations > 0);
                Assert.assertEquals(0, security.baseColumns.size());
            }
            assertView("SELECT DISTINCT sym FROM lp_view_posting ORDER BY sym", sqlExecutionContext, "PostingIndex", """
                    sym
                    A
                    B
                    C
                    """);
            final ViewSecurityContext projectedSecurity = new ViewSecurityContext("lp_view_posting_all", false);
            try (SqlExecutionContext context = newContext(projectedSecurity)) {
                assertView("SELECT DISTINCT sym FROM lp_view_posting_all ORDER BY sym", context, "GroupBy vectorized", """
                        sym
                        A
                        B
                        C
                        """);
                assertView("SELECT DISTINCT sym FROM lp_view_posting_all WHERE ts>='2020-01-01T00:00:02Z' ORDER BY sym", context, "GroupBy vectorized", """
                        sym
                        A
                        B
                        C
                        """);
                Assert.assertTrue(projectedSecurity.viewAuthorizations > 0);
                Assert.assertEquals(0, projectedSecurity.baseColumns.size());
            }
            final ViewSecurityContext distinctSecurity = new ViewSecurityContext("lp_view_distinct", false);
            try (SqlExecutionContext context = newContext(distinctSecurity)) {
                assertView("SELECT sym FROM lp_view_distinct ORDER BY sym", context, "PostingIndex", """
                        sym
                        A
                        B
                        C
                        """);
                Assert.assertTrue(distinctSecurity.viewAuthorizations > 0);
                Assert.assertEquals(0, distinctSecurity.baseColumns.size());
            }
            final ViewSecurityContext intervalSecurity = new ViewSecurityContext("lp_view_distinct_interval", false);
            try (SqlExecutionContext context = newContext(intervalSecurity)) {
                assertView("SELECT sym FROM lp_view_distinct_interval ORDER BY sym", context, "PostingIndex", """
                        sym
                        A
                        B
                        C
                        """);
                Assert.assertTrue(intervalSecurity.viewAuthorizations > 0);
                Assert.assertEquals(0, intervalSecurity.baseColumns.size());
            }
        });
    }

    @Test
    public void testPrunedColumnsAndMissingDependencyFallback() throws Exception {
        assertMemoryLeak(() -> {
            createViews();
            final ViewDefinition view = engine.getViewGraph().getViewDefinition(engine.verifyTableName("lp_view_keys"));
            Assert.assertTrue(view.getDependencies().remove("lp_view_base") > -1);
            final ViewSecurityContext security = new ViewSecurityContext("lp_view_keys", true);
            try (SqlExecutionContext context = newContext(security)) {
                assertView("SELECT id FROM lp_view_keys ORDER BY id", context, null, """
                        id
                        1
                        2
                        3
                        4
                        """);
                Assert.assertTrue(security.viewAuthorizations > 0);
                Assert.assertEquals(1, security.baseColumns.size());
                Assert.assertTrue(security.baseColumns.contains("id"));
            }
            final ViewSecurityContext denied = new ViewSecurityContext("lp_view_keys", false);
            try (SqlExecutionContext context = newContext(denied); SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile("SELECT id FROM lp_view_keys", context).getRecordCursorFactory()) {
                    try (RecordCursor ignored = factory.getCursor(context)) {
                        Assert.fail("missing dependency coverage requires base-table permission");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "base-table permission required");
                    }
                }
            }
        });
    }

    @Test
    public void testRetainedFactoriesCheckChangedViewDefinitions() throws Exception {
        assertMemoryLeak(() -> {
            createViews();
            RecordCursorFactory retained = null;
            RecordCursorFactory explanation = null;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try {
                    retained = compiler.compile("SELECT id FROM lp_view_nested ORDER BY id", sqlExecutionContext).getRecordCursorFactory();
                    explanation = compiler.compile("EXPLAIN SELECT id FROM lp_view_nested", sqlExecutionContext).getRecordCursorFactory();
                    try (RecordCursorFactory ignored = compiler.compile("SELECT missing FROM lp_view_filtered", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.fail("invalid outer column must fail");
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column: missing");
                    }
                    try (RecordCursorFactory factory = compiler.compile("SELECT count() FROM lp_view_base", sqlExecutionContext).getRecordCursorFactory()) {
                        assertResult(factory, "count\n4\n", sqlExecutionContext);
                    }
                    compiler.clear();
                } catch (Throwable th) {
                    Misc.free(retained, th);
                    Misc.free(explanation, th);
                    throw th;
                }
            }
            try (RecordCursorFactory factory = retained; RecordCursorFactory plan = explanation) {
                assertResult(factory, "id\n1\n3\n4\n", sqlExecutionContext);
                TestUtils.assertEquals("""
                        QUERY PLAN
                        SelectedRecord
                            Async JIT Filter workers: 1
                              filter: active
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: lp_view_base
                        """, print(plan, sqlExecutionContext));
                execute("ALTER VIEW lp_view_all AS SELECT id,k,sym,active,ts FROM lp_view_base WHERE id>1");
                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("a retained nested-view factory must observe a changed dependency");
                } catch (TableReferenceOutOfDateException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "cached query plan cannot be used");
                }
            }
            drainWalAndViewQueues();
            assertView("SELECT id FROM lp_view_nested ORDER BY id", sqlExecutionContext, null, """
                    id
                    3
                    4
                    """);
        });
    }

    @Test
    public void testViewHintsAndAliasesPreserveScope() throws Exception {
        assertMemoryLeak(() -> {
            createViews();
            execute("CREATE VIEW lp_view_join AS (SELECT /*+ asof_dense(l r) */ l.id lid,r.id rid FROM lp_view_base l ASOF JOIN lp_view_base r ON(sym))");
            drainWalAndViewQueues();
            assertView("SELECT * FROM lp_view_join", sqlExecutionContext, "AsOf Join Dense", """
                    lid	rid
                    1	1
                    2	2
                    3	3
                    4	4
                    """);
            assertView("SELECT /*+ asof_linear(l r) */ * FROM lp_view_join v", sqlExecutionContext, "AsOf Join Light", """
                    lid	rid
                    1	1
                    2	2
                    3	3
                    4	4
                    """);
            assertView("SELECT /*+ asof_dense(a b) */ * FROM lp_view_join", sqlExecutionContext, "AsOf Join Fast", """
                    lid	rid
                    1	1
                    2	2
                    3	3
                    4	4
                    """);
        });
    }

    @Test
    public void testViewLocalAndCallerErrorsKeepTheirPositions() throws Exception {
        assertMemoryLeak(() -> {
            createViews();
            assertQuery("SELECT missing FROM lp_view_filtered").noLeakCheck().fails(7, "Invalid column: missing");
            assertQuery("SELECT v.missing FROM lp_view_all v").noLeakCheck().fails(7, "Invalid column: v.missing");
            assertQuery("SELECT ts FROM lp_view_keys").noLeakCheck().fails(7, "Invalid column: ts");
            execute("ALTER TABLE lp_view_base DROP COLUMN k");
            assertQuery("SELECT id FROM lp_view_all").noLeakCheck().fails(10, "Invalid column: k");
            assertView("SELECT id FROM lp_view_keys ORDER BY id", sqlExecutionContext, null, """
                    id
                    1
                    2
                    3
                    4
                    """);
        });
    }

    private static SqlExecutionContext newContext(ViewSecurityContext security) {
        return new SqlExecutionContextImpl(engine, 1).with(security, bindVariableService, null, -1, null);
    }

    private void assertView(String sql, SqlExecutionContext context, String algorithm, String expected) throws Exception {
        try (RecordCursorFactory factory = select(sql, context)) {
            if (algorithm != null) {
                TestUtils.assertContains(plan(factory, context), algorithm);
            }
            assertResult(factory, expected, context);
        }
    }

    private void assertResult(RecordCursorFactory factory, String expected, SqlExecutionContext context) throws Exception {
        assertFactory(factory).withContext(context).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
    }

    private void createViews() throws Exception {
        execute("CREATE TABLE lp_view_base(id INT,k INT,sym SYMBOL INDEX,active BOOLEAN,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO lp_view_base VALUES(1,1,'A',true,'2020-01-01T00:00:01Z'),(2,1,'A',false,'2020-01-01T00:00:02Z'),"
                + "(3,2,'B',true,'2020-01-02T00:00:01Z'),(4,3,'C',true,'2020-01-02T00:00:02Z')");
        execute("CREATE VIEW lp_view_all AS (SELECT id,k,sym,active,ts FROM lp_view_base)");
        execute("CREATE VIEW lp_view_keys AS (SELECT id,sym FROM lp_view_base)");
        execute("CREATE VIEW lp_view_nested AS (SELECT id,ts FROM lp_view_all WHERE active)");
        execute("CREATE VIEW lp_view_filtered AS (SELECT id FROM lp_view_base WHERE id IN(1,2))");
        execute("CREATE TABLE lp_view_posting(id INT,sym SYMBOL INDEX TYPE POSTING,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO lp_view_posting SELECT id,sym,ts FROM lp_view_base");
        execute("CREATE VIEW lp_view_posting_all AS (SELECT id,sym,ts FROM lp_view_posting)");
        execute("CREATE VIEW lp_view_distinct AS (SELECT DISTINCT sym FROM lp_view_posting)");
        execute("CREATE VIEW lp_view_distinct_interval AS (SELECT DISTINCT sym FROM lp_view_posting WHERE ts>='2020-01-01T00:00:02Z')");
        drainWalAndViewQueues();
    }

    private String plan(RecordCursorFactory factory, SqlExecutionContext context) {
        final TextPlanSink sink = new TextPlanSink();
        sink.of(factory, context);
        return sink.getSink().toString();
    }

    private String print(RecordCursorFactory factory, SqlExecutionContext context) throws Exception {
        final StringSink sink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(context)) {
            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
        }
        return sink.toString();
    }

    private static class ViewSecurityContext extends AllowAllSecurityContext {
        private final LowerCaseCharSequenceHashSet baseColumns = new LowerCaseCharSequenceHashSet();
        private final String expectedView;
        private final boolean isBaseAllowed;
        private int viewAuthorizations;

        private ViewSecurityContext(String expectedView, boolean isBaseAllowed) {
            this.expectedView = expectedView;
            this.isBaseAllowed = isBaseAllowed;
        }

        @Override
        public void authorizeSelect(ViewDefinition view) {
            Assert.assertEquals(expectedView, view.getViewToken().getTableName());
            viewAuthorizations++;
        }

        @Override
        public void authorizeSelect(TableToken table, @NotNull ObjList<CharSequence> columns) {
            if (!isBaseAllowed) {
                throw CairoException.nonCritical().put("base-table permission required");
            }
            Assert.assertEquals("lp_view_base", table.getTableName());
            for (int i = 0, n = columns.size(); i < n; i++) {
                baseColumns.add(columns.getQuick(i).toString());
            }
        }

        @Override
        public void authorizeSelectOnAnyColumn(TableToken table) {
            Assert.fail("view authorization must survive an empty required-column set");
        }
    }
}
