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

package io.questdb.test.cairo.view;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A view's dependency map lists, for every table the view reads, the columns the view reads from it.
 * A reader of the view needs no permission on those columns, see
 * {@code AbstractPartitionFrameCursorFactory.authorizeSelect()}. These tests check that CREATE VIEW and
 * ALTER VIEW record exactly the columns the plan of the view reads, whichever alias, join, CTE or
 * nested view the SELECT reads them through, and that a principal with SELECT on the view only reads it.
 */
public class ViewColumnDependencyTest extends AbstractViewTest {

    @Test
    public void testAliasNamedLikeAnotherTableIsNotADependency() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // a table named like the alias the view reads x through
            execute("CREATE TABLE t (ts TIMESTAMP, a LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            assertDependencies("SELECT t.a FROM x t", "{x=[a]}");
            assertDependencies("SELECT t.a, u.c FROM x t JOIN y u ON t.k = u.k", "{x=[a, k], y=[c, k]}");
        });
    }

    @Test
    public void testAliasedColumns() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertDependencies("SELECT t.ts, t.a FROM x t", "{x=[a, ts]}");
            assertDependencies("SELECT x.a FROM x", "{x=[a]}");
            assertDependencies("SELECT t.a FROM x t WHERE t.b > 0 ORDER BY t.ts", "{x=[a, b, ts]}");
            assertDependencies("SELECT t.k, count() FROM x t GROUP BY t.k", "{x=[k]}");
            assertDependencies("SELECT DISTINCT t.k FROM x t", "{x=[k]}");
            assertDependencies("SELECT t.a FROM x t WHERE t.ts IN '1970-01-01'", "{x=[a, ts]}");
        });
    }

    @Test
    public void testColumnsOfJoinedTables() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertDependencies("SELECT a, c FROM x JOIN y ON (k)", "{x=[a, k], y=[c, k]}");
            assertDependencies("SELECT x.a, y.c FROM x JOIN y ON (k)", "{x=[a, k], y=[c, k]}");
            assertDependencies("SELECT t.a FROM x t JOIN y u ON t.k = u.k AND u.d > t.b", "{x=[a, b, k], y=[d, k]}");
            assertDependencies("SELECT e.ts, e.a, l.c FROM x e JOIN y l ON (k, ts)", "{x=[a, k, ts], y=[c, k, ts]}");
            assertDependencies("SELECT t.a, u.b FROM x t JOIN x u ON (k)", "{x=[a, b, k]}");
        });
    }

    @Test
    public void testCte() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertDependencies("WITH c AS (SELECT a, k FROM x) SELECT c.a FROM c JOIN y ON (k)", "{x=[a, k], y=[k]}");
            assertDependencies("WITH c AS (SELECT t.a, t.k FROM x t) SELECT c.a FROM c", "{x=[a]}");
        });
    }

    @Test
    public void testLateralJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertDependencies(
                    "SELECT t.a, l.c FROM x t CROSS JOIN LATERAL (SELECT c FROM y u WHERE u.k = t.k LIMIT 1) l",
                    "{x=[a, k], y=[c, k]}"
            );
        });
    }

    @Test
    public void testLatestOn() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertDependencies("SELECT a FROM x LATEST ON ts PARTITION BY k", "{x=[a, k]}");
        });
    }

    @Test
    public void testNestedView() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v2 AS (SELECT u.a, u.k, u.ts FROM x u)");
            drainWalAndViewQueues();
            Assert.assertEquals("{x=[a, k, ts]}", dependenciesOf("v2"));
            // a read of the outer view reads the tables of the inner one through the outer one
            assertDependencies("SELECT w.a FROM v2 w", "{v2=[], x=[a]}");
            assertDependencies("SELECT * FROM v2", "{v2=[], x=[a, k, ts]}");
        });
    }

    @Test
    public void testOverridableVariableReadsNoColumnTheViewDoesNot() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertDependencies("DECLARE OVERRIDABLE @c := t.a SELECT t.ts, @c FROM x t", "{x=[a, ts]}");
            execute("CREATE VIEW v AS (DECLARE OVERRIDABLE @c := t.a SELECT t.ts, @c FROM x t)");
            drainWalAndViewQueues();
            try (SqlExecutionContext context = denyBaseTableSelectContext()) {
                TestUtils.assertContains(
                        failureOf(context, "DECLARE @c := t.b SELECT * FROM v"),
                        "permission denied [table=x, columns=b]"
                );
            }
        });
    }

    @Test
    public void testSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertDependencies("SELECT count() FROM x SAMPLE BY 1h", "{x=[ts]}");
            assertDependencies("SELECT t.k, sum(t.a) FROM x t SAMPLE BY 1h", "{x=[a, k, ts]}");
            // SAMPLE BY over a sub-query requires the timestamp the sub-query does not select
            assertDependencies("SELECT k, count() FROM (SELECT k, a FROM x) SAMPLE BY 1h", "{x=[k, ts]}");
        });
    }

    @Test
    public void testSelectStarViewReadsColumnAddedLater() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v_created AS (SELECT * FROM x)");
            execute("CREATE VIEW v_altered AS (SELECT a FROM x)");
            drainWalAndViewQueues();
            execute("ALTER VIEW v_altered AS (SELECT * FROM x)");
            drainWalAndViewQueues();

            execute("ALTER TABLE x ADD COLUMN e INT");
            execute("INSERT INTO x VALUES (7_200_000_000, 'k2', 5, 6, 42)");
            drainWalAndViewQueues();
            final String expected = """
                    ts\tk\ta\tb\te
                    1970-01-01T00:00:00.000000Z\tk1\t1\t2\tnull
                    1970-01-01T01:00:00.000000Z\tk1\t3\t4\tnull
                    1970-01-01T02:00:00.000000Z\tk2\t5\t6\t42
                    """;
            try (SqlExecutionContext context = denyBaseTableSelectContext()) {
                TestUtils.assertEquals(expected, print(context, "SELECT * FROM v_created"));
                TestUtils.assertEquals(expected, print(context, "SELECT * FROM v_altered"));
            }
        });
    }

    @Test
    public void testSetOperation() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertDependencies("SELECT t.a FROM x t UNION ALL SELECT u.c FROM y u", "{x=[a], y=[c]}");
            assertDependencies("SELECT t.k FROM x t EXCEPT SELECT u.k FROM y u WHERE u.d > 6", "{x=[k], y=[d, k]}");
        });
    }

    @Test
    public void testStoragePolicyShapedView() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE sp_entries (
                        ts TIMESTAMP, policy_name VARCHAR, to_parquet INT, drop_local INT, status CHAR
                    ) TIMESTAMP(ts) PARTITION BY MONTH WAL
                    """);
            execute("""
                    CREATE TABLE sp_links (
                        ts TIMESTAMP, table_id INT, table_dir_name VARCHAR, policy_name VARCHAR, status CHAR
                    ) TIMESTAMP(ts) PARTITION BY MONTH WAL
                    """);
            execute("""
                    INSERT INTO sp_entries VALUES
                        (1, 'p1', 24, 48, 'A'),
                        (2, 'p2', 12, 0, 'A'),
                        (3, 'p2', 0, 0, 'R')
                    """);
            execute("""
                    INSERT INTO sp_links VALUES
                        (1, 1, 't1~1', 'p1', 'A'),
                        (2, 2, 't2~2', 'p2', 'A'),
                        (3, 2, 't2~2', 'p2', 'R')
                    """);
            drainWalQueue();
            // the entries are joined and read only through table aliases, the status of an entry as e.status
            assertDependencies(
                    """
                            WITH
                              policies AS (
                                SELECT ts, table_dir_name, status, to_parquet, drop_local
                                FROM (
                                  SELECT e.ts, l.table_dir_name, e.status, to_parquet, drop_local
                                  FROM sp_entries e
                                  JOIN sp_links l ON (policy_name, ts)
                                ) LATEST ON ts PARTITION BY table_dir_name
                              ),
                              links AS (
                                SELECT ts, table_id, table_dir_name, policy_name, status
                                FROM sp_links
                                LATEST ON ts PARTITION BY table_dir_name, policy_name
                              )
                            SELECT
                              l.table_dir_name,
                              CASE WHEN p.to_parquet < 0 THEN (-(p.to_parquet))::VARCHAR || 'm' ELSE p.to_parquet::VARCHAR || 'h' END AS to_parquet,
                              CASE WHEN (l.status = 'A' AND p.status = 'A') THEN 'A' ELSE 'D' END AS status,
                              CASE WHEN l.ts > p.ts THEN l.ts ELSE p.ts END AS last_updated
                            FROM policies p
                            JOIN links l ON table_dir_name
                            WHERE p.status != 'R' AND l.status != 'R'
                            """,
                    // drop_local is selected by the CTE, but nothing reads it from there
                    "{sp_entries=[policy_name, status, to_parquet, ts], sp_links=[policy_name, status, table_dir_name, ts]}"
            );
        });
    }

    @Test
    public void testSubQueryInExpression() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The view records the columns its sub-query reads. The scan of y still authorizes as the
            // reader, a sub-query in an expression does not read through the view, so this does not
            // read the view as a principal without permission on y.
            assertDependencies(
                    "SELECT t.a FROM x t WHERE t.k IN (SELECT u.k FROM y u WHERE u.c > 0)",
                    "{x=[a, k], y=[c, k]}",
                    false
            );
        });
    }

    @Test
    public void testTemporalJoins() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // a temporal join reads the timestamps of both sides, whether the SELECT names them or not
            assertDependencies("SELECT t.a, u.c FROM x t ASOF JOIN y u ON (k)", "{x=[a, k, ts], y=[c, k, ts]}");
            assertDependencies("SELECT t.a, u.c FROM x t LT JOIN y u ON (k)", "{x=[a, k, ts], y=[c, k, ts]}");
            assertDependencies("SELECT t.a, u.c FROM x t SPLICE JOIN y u ON (k)", "{x=[a, k, ts], y=[c, k, ts]}");
            // the first table in join order provides the timestamp of a temporal join after an inner one
            assertDependencies(
                    "SELECT t.a, u.c, w.b FROM x t JOIN y u ON (k) ASOF JOIN x w ON (k)",
                    "{x=[a, b, k, ts], y=[c, k]}"
            );
        });
    }

    @Test
    public void testWildcards() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // a wildcard selects every column of its tables, including one they gain later
            assertDependencies("SELECT * FROM x", "{x=[*, a, b, k, ts]}");
            assertDependencies("x WHERE a > 1", "{x=[*, a, b, k, ts]}");
            assertDependencies("SELECT * FROM x JOIN y ON (k)", "{x=[*, a, b, k, ts], y=[*, c, d, k, ts]}");
            assertDependencies("SELECT t.* FROM x t JOIN y u ON (k)", "{x=[*, a, b, k, ts], y=[k]}");
            // the columns of a sub-query are the ones it selects
            assertDependencies("SELECT * FROM (SELECT a FROM x)", "{x=[a]}");
            assertDependencies("SELECT s.* FROM (SELECT t.a, t.k FROM x t) s JOIN y u ON (k)", "{x=[a, k], y=[k]}");
        });
    }

    @Test
    public void testWindowFunction() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertDependencies("SELECT a, row_number() OVER (PARTITION BY k ORDER BY b) FROM x", "{x=[a, b, k]}");
            assertDependencies(
                    "SELECT t.a, row_number() OVER (PARTITION BY t.k ORDER BY t.b) FROM x t",
                    "{x=[a, b, k]}"
            );
        });
    }

    private static void assertDependencies(String viewSql, String expectedDependencies) throws Exception {
        assertDependencies(viewSql, expectedDependencies, true);
    }

    // Asserts the dependency map CREATE VIEW and ALTER VIEW give the SELECT of a view, and, unless told
    // otherwise, that a principal with no permission on the columns of the base tables reads the view
    // exactly as a principal with every permission does.
    private static void assertDependencies(String viewSql, String expectedDependencies, boolean isReadableWithViewPermissionOnly) throws Exception {
        execute("CREATE VIEW v AS (" + viewSql + ")");
        drainWalAndViewQueues();
        assertViewDependencies(viewSql, expectedDependencies, isReadableWithViewPermissionOnly, "CREATE VIEW");

        // a definition that reads no table first, so that ALTER VIEW records every dependency itself
        execute("ALTER VIEW v AS (SELECT x FROM long_sequence(1))");
        drainWalAndViewQueues();
        execute("ALTER VIEW v AS (" + viewSql + ")");
        drainWalAndViewQueues();
        assertViewDependencies(viewSql, expectedDependencies, isReadableWithViewPermissionOnly, "ALTER VIEW");

        execute("DROP VIEW v");
        drainWalAndViewQueues();
    }

    private static void assertViewDependencies(String viewSql, String expectedDependencies, boolean isReadableWithViewPermissionOnly, String statement) throws Exception {
        Assert.assertEquals(statement + ": " + viewSql, expectedDependencies, dependenciesOf("v"));
        if (isReadableWithViewPermissionOnly) {
            final String expected = print(sqlExecutionContext, "SELECT * FROM v");
            try (SqlExecutionContext context = denyBaseTableSelectContext()) {
                TestUtils.assertEquals(statement + ": " + viewSql, expected, print(context, "SELECT * FROM v"));
            }
        }
    }

    private static void createTables() throws Exception {
        execute("CREATE TABLE x (ts TIMESTAMP, k SYMBOL, a LONG, b LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE TABLE y (ts TIMESTAMP, k SYMBOL, c LONG, d LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO x VALUES (0, 'k1', 1, 2), (3_600_000_000, 'k1', 3, 4)");
        execute("INSERT INTO y VALUES (0, 'k1', 5, 6), (3_600_000_000, 'k1', 7, 8)");
        drainWalQueue();
    }

    private static SqlExecutionContext denyBaseTableSelectContext() {
        return new SqlExecutionContextImpl(engine, 1).with(
                new DenyBaseTableSelectSecurityContext(),
                bindVariableService,
                null,
                -1,
                null
        );
    }

    private static String dependenciesOf(String viewName) {
        final ViewDefinition viewDefinition = getViewDefinition(viewName);
        Assert.assertNotNull(viewDefinition);
        final LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = viewDefinition.getDependencies();
        final TreeMap<String, TreeSet<String>> sorted = new TreeMap<>();
        final ObjList<CharSequence> tableNames = dependencies.keys();
        for (int i = 0, n = tableNames.size(); i < n; i++) {
            final LowerCaseCharSequenceHashSet columnNames = dependencies.get(tableNames.getQuick(i));
            final TreeSet<String> columns = new TreeSet<>();
            for (int j = 0, m = columnNames.getKeyCount(); j < m; j++) {
                final CharSequence columnName = columnNames.getKey(j);
                if (columnName != null) {
                    columns.add(columnName.toString());
                }
            }
            sorted.put(tableNames.getQuick(i).toString(), columns);
        }
        return sorted.toString();
    }

    private static String failureOf(SqlExecutionContext context, String sql) throws Exception {
        try {
            print(context, sql);
        } catch (CairoException e) {
            return e.getFlyweightMessage().toString();
        }
        Assert.fail("expected the read to be denied: " + sql);
        return null;
    }

    private static String print(SqlExecutionContext context, String sql) throws Exception {
        return TestUtils.printSqlToString(engine, context, sql, new StringSink());
    }

    /**
     * Grants everything but SELECT on the columns of a table, so that a principal reads a view, and a
     * base table through it, only on the authority of the view.
     */
    private static final class DenyBaseTableSelectSecurityContext extends AllowAllSecurityContext {
        @Override
        public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
            final CairoException e = CairoException.nonCritical()
                    .put("permission denied [table=").put(tableToken.getTableName())
                    .put(", columns=");
            for (int i = 0, n = columnNames.size(); i < n; i++) {
                if (i > 0) {
                    e.put(',');
                }
                e.put(columnNames.getQuick(i));
            }
            throw e.put(']');
        }
    }
}
