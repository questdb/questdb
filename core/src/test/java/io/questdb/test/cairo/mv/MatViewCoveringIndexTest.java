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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.mv.MatViewRefreshSqlExecutionContext;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class MatViewCoveringIndexTest extends AbstractCairoTest {

    // Plan signature emitted by CoveringIndexRecordCursorFactory.
    private static final String COVERING_INDEX_MARKER = "CoveringIndex";
    // Plan signature emitted by PostingIndexDistinctRecordCursorFactory.
    private static final String POSTING_DISTINCT_MARKER = "PostingIndex op: distinct on:";

    @Test
    public void testEnabledOverrideRestoresCoveringInRefresh() throws Exception {
        // With the config flipped to true, the mat-view refresh context must use
        // covering again for at least the simplest covering-eligible query.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_COVERING_INDEX_ENABLED, "true");
        assertMemoryLeak(() -> {
            createCoveringTable();
            try (MatViewRefreshSqlExecutionContext refreshCtx = new MatViewRefreshSqlExecutionContext(engine, 0);
                 TableReader baseReader = engine.getReader("t_cov")) {
                refreshCtx.of(baseReader);
                Assert.assertTrue(refreshCtx.isCoveringIndexEnabled());
                assertPlanContains(refreshCtx, "SELECT sym, price FROM t_cov WHERE sym = 'A'", COVERING_INDEX_MARKER);
            }
        });
    }

    @Test
    public void testGateAtAllCoveringSites() throws Exception {
        // Each query shape exercises a distinct covering-index gate site in
        // SqlCodeGenerator. Under the default config (covering disabled for
        // mat view refresh) the refresh context must NOT pick the covering
        // factory at any of them, while the regular context still does.
        assertMemoryLeak(() -> {
            createCoveringTable();
            assertGated("SELECT sym, price FROM t_cov WHERE sym = 'A'", COVERING_INDEX_MARKER);
            assertGated("SELECT sym, price FROM t_cov WHERE sym IN ('A', 'B')", COVERING_INDEX_MARKER);
            assertGated("SELECT sym, price FROM t_cov WHERE sym = 'A' LATEST ON ts PARTITION BY sym", COVERING_INDEX_MARKER);
            assertGated("SELECT sym, price FROM t_cov WHERE sym IN ('A', 'B') LATEST ON ts PARTITION BY sym", COVERING_INDEX_MARKER);
            assertGated("SELECT DISTINCT sym FROM t_cov", POSTING_DISTINCT_MARKER);
        });
    }

    @Test
    public void testRegularContextUnaffectedByDefault() throws Exception {
        // The flag is mat-view-only. Regular ad-hoc queries must still pick
        // covering even with the default-off config.
        assertMemoryLeak(() -> {
            createCoveringTable();
            Assert.assertTrue(sqlExecutionContext.isCoveringIndexEnabled());
            assertPlanContains(sqlExecutionContext, "SELECT sym, price FROM t_cov WHERE sym = 'A'", COVERING_INDEX_MARKER);
            assertPlanContains(sqlExecutionContext, "SELECT sym, price FROM t_cov WHERE sym IN ('A', 'B')", COVERING_INDEX_MARKER);
            assertPlanContains(sqlExecutionContext, "SELECT sym, price FROM t_cov WHERE sym = 'A' LATEST ON ts PARTITION BY sym", COVERING_INDEX_MARKER);
            assertPlanContains(sqlExecutionContext, "SELECT sym, price FROM t_cov WHERE sym IN ('A', 'B') LATEST ON ts PARTITION BY sym", COVERING_INDEX_MARKER);
            assertPlanContains(sqlExecutionContext, "SELECT DISTINCT sym FROM t_cov", POSTING_DISTINCT_MARKER);
        });
    }

    private void assertGated(String query, String marker) throws Exception {
        // Sanity: regular context picks the covering/posting plan.
        assertPlanContains(sqlExecutionContext, query, marker);
        // Refresh context (default config: disabled) must avoid it.
        try (MatViewRefreshSqlExecutionContext refreshCtx = new MatViewRefreshSqlExecutionContext(engine, 0);
             TableReader baseReader = engine.getReader("t_cov")) {
            refreshCtx.of(baseReader);
            Assert.assertFalse(refreshCtx.isCoveringIndexEnabled());
            assertPlanDoesNotContain(refreshCtx, query, marker);
        }
    }

    private void assertPlanContains(SqlExecutionContext ctx, String query, String marker) throws Exception {
        String plan = compilePlan(ctx, query);
        Assert.assertTrue(
                "expected plan for [" + query + "] to contain '" + marker + "', got:\n" + plan,
                plan.contains(marker)
        );
    }

    private void assertPlanDoesNotContain(SqlExecutionContext ctx, String query, String marker) throws Exception {
        String plan = compilePlan(ctx, query);
        Assert.assertFalse(
                "expected plan for [" + query + "] to not contain '" + marker + "', got:\n" + plan,
                plan.contains(marker)
        );
    }

    private String compilePlan(SqlExecutionContext ctx, String query) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory()) {
            planSink.clear();
            planSink.of(factory, ctx);
            StringBuilder sb = new StringBuilder();
            for (int i = 1, n = planSink.getLineCount(); i <= n; i++) {
                if (i > 1) {
                    sb.append('\n');
                }
                sb.append(Chars.toString(planSink.getLine(i)));
            }
            return sb.toString();
        }
    }

    private void createCoveringTable() throws Exception {
        execute("""
                CREATE TABLE t_cov (
                    ts TIMESTAMP,
                    sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                    price DOUBLE
                ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                """);
        execute("""
                INSERT INTO t_cov
                SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                       rnd_symbol('A','B','C'),
                       rnd_double() * 100
                FROM long_sequence(100)
                """);
        engine.releaseAllWriters();
    }
}
