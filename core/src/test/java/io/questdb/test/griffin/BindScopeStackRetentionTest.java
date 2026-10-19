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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.std.RetainedCapacity;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A compiler keeps one bind scope per nesting depth whatever the width of its queries and whichever clauses nest
 * them, and the statement's sub-queries come from a pool that keeps what its recent uses took between statements and
 * returns to the configured model pool capacity once they stop taking more, after success and after a failure at
 * binding or generation alike.
 */
public class BindScopeStackRetentionTest extends AbstractCairoTest {
    private static final String GENERATION_ERROR_SUBQUERY = "(SELECT p.price FROM (SELECT * FROM prices ORDER BY ts DESC) p ASOF JOIN trades t)";

    @Test
    public void testBindFailureKeepsScopePerDepth() throws Exception {
        assertMemoryLeak(() -> assertScopeCountAfterFailure(
                "SELECT x = (" + nestedQuery(2) + ") a, x = (SELECT missing FROM long_sequence(1)) c FROM long_sequence(1)",
                "Invalid column: missing",
                145,
                4
        ));
    }

    @Test
    public void testScopesFollowNestingDepth() throws Exception {
        assertMemoryLeak(() -> {
            assertScopeCount(wideQuery(1000), 2);
            assertScopeCount(nestedQuery(5), 6);
            final StringSink sql = new StringSink();
            sql.put("SELECT ");
            for (int i = 0; i < 10; i++) {
                if (i > 0) {
                    sql.put(", ");
                }
                sql.put("x = (").put(nestedQuery(5)).put(") c").put(i);
            }
            sql.put(" FROM long_sequence(1)");
            assertScopeCount(sql.toString(), 7);
        });
    }

    @Test
    public void testScopesFollowNestingDepthAcrossClauses() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE u (k SYMBOL, w INT)");
            execute("INSERT INTO u VALUES ('a', 10)");
            final StringSink sql = new StringSink();
            sql.put("SELECT t.k, sum(CASE WHEN v > (").put(nestedQuery(3)).put(") THEN v ELSE 0 END) s FROM t ");
            sql.put("JOIN u ON t.k = u.k WHERE v > (").put(nestedQuery(4)).put(") ");
            sql.put("ORDER BY CASE WHEN sum(v) > (").put(nestedQuery(1)).put(") THEN 0 ELSE 1 END");
            assertScopeCount(sql.toString(), 6);
            sql.clear();
            sql.put("SELECT v, row_number() OVER (ORDER BY ts) rn, v = (SELECT max(r) FROM (SELECT row_number() OVER () r FROM u WHERE w > (");
            sql.put(nestedQuery(2)).put("))) m FROM t ORDER BY ts");
            assertScopeCount(sql.toString(), 5);
            sql.clear();
            sql.put("SELECT * FROM (SELECT k, v FROM t WHERE v > (").put(nestedQuery(3)).put(")) ");
            sql.put("PIVOT (sum(v) FOR k IN (SELECT DISTINCT k FROM u WHERE w > (").put(nestedQuery(1)).put(") ORDER BY k))");
            assertScopeCount(sql.toString(), 5);
        });
    }

    @Test
    public void testGenerationFailureKeepsScopePerDepth() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE prices (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertScopeCountAfterFailure(
                    "SELECT price FROM trades WHERE price = (SELECT price FROM prices WHERE price = " + GENERATION_ERROR_SUBQUERY + ")",
                    "left side of time series join doesn't have ASC timestamp order",
                    142,
                    3
            );
        });
    }

    @Test
    public void testWideQuerySubqueriesReturnToFloor() throws Exception {
        assertMemoryLeak(() -> {
            final int ceiling = configuration.getSqlModelPoolCapacity();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compileAndClose(compiler, wideQuery(3));
                compiler.clear();
                final int capacity = compiler.getSubqueryPoolCapacity();
                compileAndClose(compiler, wideQuery(3));
                compiler.clear();
                Assert.assertEquals(capacity, compiler.getSubqueryPoolCapacity());

                compileAndClose(compiler, wideQuery(ceiling + 8));
                Assert.assertTrue(compiler.getSubqueryPoolCapacity() > ceiling);
                compiler.clear();
                Assert.assertTrue(compiler.getSubqueryPoolCapacity() > ceiling);
                for (int i = 0, n = 2 * RetainedCapacity.WINDOW_USES; i < n; i++) {
                    compiler.clear();
                }
                Assert.assertEquals(ceiling, compiler.getSubqueryPoolCapacity());
            }
        });
    }

    private static void assertScopeCount(String sql, int expected) throws SqlException {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            for (int i = 0; i < 2; i++) {
                compileAndClose(compiler, sql);
                Assert.assertEquals(expected, compiler.getBindScopeCount());
            }
        }
    }

    private void assertScopeCountAfterFailure(String sql, String message, int position, int expected) throws Exception {
        final WindowContext windowContext = sqlExecutionContext.getWindowContext();
        final boolean isTimestampRequired = sqlExecutionContext.isTimestampRequired();
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            for (int i = 0; i < 2; i++) {
                try (RecordCursorFactory ignore = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.fail("compilation must fail");
                } catch (SqlException e) {
                    TestUtils.assertEquals(message, e.getFlyweightMessage());
                    Assert.assertEquals(position, e.getPosition());
                }
                Assert.assertEquals(expected, compiler.getBindScopeCount());
                Assert.assertSame(windowContext, sqlExecutionContext.getWindowContext());
                Assert.assertTrue(windowContext.isEmpty());
                Assert.assertEquals(isTimestampRequired, sqlExecutionContext.isTimestampRequired());
                assertQuery("SELECT x FROM long_sequence(1) WHERE x = (SELECT x FROM long_sequence(1))")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .returns("x\n1\n");
            }
        }
    }

    private static void compileAndClose(SqlCompilerImpl compiler, CharSequence sql) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            Assert.assertNotNull(factory);
        }
    }

    private static String nestedQuery(int depth) {
        String sql = "SELECT x FROM long_sequence(1)";
        for (int i = 0; i < depth; i++) {
            sql = "SELECT x FROM long_sequence(1) WHERE x = (" + sql + ")";
        }
        return sql;
    }

    private static String wideQuery(int width) {
        final StringSink sql = new StringSink();
        sql.put("SELECT ");
        for (int i = 0; i < width; i++) {
            if (i > 0) {
                sql.put(", ");
            }
            sql.put("x = (SELECT x FROM long_sequence(1)) c").put(i);
        }
        sql.put(" FROM long_sequence(1)");
        return sql.toString();
    }
}
