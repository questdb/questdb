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
 * {@code AbstractPartitionFrameCursorFactory.authorizeSelect()}. These tests check that CREATE VIEW,
 * ALTER VIEW and CREATE OR REPLACE VIEW record exactly the columns the plan of the view reads, whichever
 * alias, join, CTE or nested view the SELECT reads them through, and that a principal with SELECT on the
 * view only reads it.
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
    public void testNarrowedNestedWildcard() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A SELECT list that names its columns keeps only those of the wildcard under it, so the view
            // selects no other column of the tables the wildcard selects from, and none of them gets "*".
            assertDependencies("SELECT a FROM (SELECT * FROM x)", "{x=[a]}");
            assertDependencies("SELECT a FROM (x WHERE a > 1)", "{x=[a]}");
            assertDependencies("SELECT * FROM (SELECT a FROM (SELECT * FROM x))", "{x=[a]}");
            assertDependencies("SELECT k, a, c FROM (SELECT * FROM x JOIN y ON (k))", "{x=[a, k], y=[c, k]}");
            assertDependencies("SELECT k, a FROM (SELECT * FROM x JOIN y ON (k)) WHERE k = 'k1'", "{x=[a, k], y=[k]}");
            assertDependencies("WITH j AS (SELECT * FROM x JOIN y ON (k)) SELECT k, a FROM j", "{x=[a, k], y=[k]}");
            assertDependencies("DECLARE @q := (SELECT * FROM x) SELECT a FROM @q", "{x=[a]}");
            assertDependencies("SELECT k FROM (SELECT * FROM x UNION ALL SELECT * FROM y)", "{x=[k], y=[k]}");
            // a qualified wildcard selects every column of the source it names only
            assertDependencies("SELECT u.* FROM (SELECT * FROM x) s JOIN y u ON (k)", "{x=[k], y=[*, c, d, k, ts]}");
            // PIVOT selects the columns it groups and aggregates, not the ones of its source
            assertDependencies("SELECT * FROM (SELECT * FROM x) PIVOT (sum(a) FOR k IN ('k1'))", "{x=[a, k]}");
            // A sub-query in an expression selects no column into the view. The scan of y authorizes as
            // the reader, so this does not read the view as a principal without permission on y.
            assertDependencies(
                    "SELECT a FROM x WHERE k IN (SELECT k FROM (SELECT * FROM y))",
                    "{x=[a, k], y=[k]}",
                    false
            );

            // the view reads no column the base table gains later
            execute("CREATE VIEW v_narrowed AS (SELECT k, a FROM (SELECT * FROM x))");
            drainWalAndViewQueues();
            execute("ALTER TABLE x ADD COLUMN e INT");
            execute("INSERT INTO x VALUES (7_200_000_000, 'k2', 5, 6, 42)");
            drainWalAndViewQueues();
            Assert.assertEquals("{x=[a, k]}", dependenciesOf("v_narrowed"));
            try (SqlExecutionContext context = denyBaseTableSelectContext()) {
                assertQuery("SELECT * FROM v_narrowed")
                        .withContext(context)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                k\ta
                                k1\t1
                                k1\t3
                                k2\t5
                                """);
            }
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
    public void testNestedViewWildcard() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v_join AS (SELECT * FROM x JOIN y ON (k))");
            drainWalAndViewQueues();
            // A read of the outer view reads the tables of the inner one on the authority of the outer one,
            // which selects every column of them only through a wildcard of its own.
            assertDependencies("SELECT * FROM v_join", "{v_join=[], x=[*, a, b, k, ts], y=[*, c, d, k, ts]}");
            assertDependencies("SELECT w.* FROM v_join w", "{v_join=[], x=[*, a, b, k, ts], y=[*, c, d, k, ts]}");
            assertDependencies("SELECT a FROM v_join", "{v_join=[], x=[a, k], y=[k]}");
            assertDependencies("SELECT * FROM (SELECT k, c FROM v_join)", "{v_join=[], x=[k], y=[c, k]}");
        });
    }

    @Test
    public void testNestedWildcard() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // a wildcard over a sub-query, a CTE or a view that selects every column of a table selects
            // every column of it too
            assertDependencies("SELECT * FROM (SELECT * FROM x)", "{x=[*, a, b, k, ts]}");
            assertDependencies("SELECT * FROM (x WHERE a > 1)", "{x=[*, a, b, k, ts]}");
            assertDependencies("WITH c AS (SELECT * FROM x) SELECT * FROM c WHERE a > 1", "{x=[*, a, b, k, ts]}");
            assertDependencies("DECLARE @q := (SELECT * FROM x) SELECT * FROM @q", "{x=[*, a, b, k, ts]}");
            assertDependencies("SELECT * FROM x JOIN (SELECT * FROM y) s ON (k)", "{x=[*, a, b, k, ts], y=[*, c, d, k, ts]}");
            assertDependencies("SELECT s.* FROM (SELECT * FROM x) s JOIN y u ON (k)", "{x=[*, a, b, k, ts], y=[k]}");
            assertDependencies(
                    "SELECT * FROM (SELECT * FROM x UNION ALL SELECT * FROM y)",
                    "{x=[*, a, b, k, ts], y=[*, c, d, k, ts]}"
            );
            assertDependencies(
                    "SELECT t.* FROM x t CROSS JOIN LATERAL (SELECT * FROM y u WHERE u.k = t.k) l",
                    "{x=[*, a, b, k, ts], y=[k]}"
            );
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
                assertQuery("DECLARE @c := t.b SELECT * FROM v")
                        .withContext(context)
                        .noLeakCheck()
                        .failsWith("permission denied [table=x, columns=b]");
            }
        });
    }

    @Test
    public void testOverridableVariableReadsNoColumnTheViewDropsFromAWildcard() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v_join AS (SELECT * FROM x JOIN y ON (k))");
            drainWalAndViewQueues();
            // the first table of the wildcard's FROM clause
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @c := a SELECT ts, @c AS v FROM (SELECT * FROM x)",
                    "DECLARE @c := b SELECT * FROM v",
                    "permission denied [table=x, columns=b]"
            );
            // a table joined to the first one, in a sub-query, a CTE and a nested view
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @c := c SELECT k, @c AS v FROM (SELECT * FROM x JOIN y ON (k))",
                    "DECLARE @c := d SELECT * FROM v",
                    "permission denied [table=y, columns=d]"
            );
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @c := c WITH j AS (SELECT * FROM x JOIN y ON (k)) SELECT k, @c AS v FROM j",
                    "DECLARE @c := d SELECT * FROM v",
                    "permission denied [table=y, columns=d]"
            );
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @c := c SELECT k, @c AS v FROM v_join",
                    "DECLARE @c := d SELECT * FROM v",
                    "permission denied [table=y, columns=d]"
            );
            // a filter turned into a test of a column the view does not select
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @s := 'k1' SELECT k, a FROM (SELECT * FROM x JOIN y ON (k)) WHERE k = @s",
                    "DECLARE @s := CASE WHEN d = 6 THEN 'k1' ELSE 'k2' END SELECT * FROM v",
                    "permission denied [table=y, columns=d]"
            );
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
            execute("CREATE VIEW v_nested AS (SELECT * FROM (SELECT * FROM x))");
            drainWalAndViewQueues();
            execute("ALTER VIEW v_altered AS (SELECT * FROM x)");
            execute("CREATE VIEW v_outer AS (SELECT * FROM v_created)");
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
                for (String viewName : new String[]{"v_created", "v_altered", "v_nested", "v_outer"}) {
                    assertQuery("SELECT * FROM " + viewName)
                            .withContext(context)
                            .noLeakCheck()
                            .timestamp("ts")
                            .expectSize()
                            .returns(expected);
                }
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
                    "SELECT t.a, u.c, w.d FROM x t JOIN y u ON (k) ASOF JOIN y w ON (k)",
                    "{x=[a, k, ts], y=[c, d, k, ts]}"
            );
        });
    }

    @Test
    public void testViewReadingWholeRowsReadsColumnAddedLater() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v_distinct AS (SELECT a FROM (SELECT DISTINCT * FROM x))");
            execute("CREATE VIEW v_grouped AS (SELECT a FROM (SELECT *, count() AS n FROM x))");
            execute("CREATE VIEW v_union AS (SELECT a FROM (SELECT * FROM x UNION SELECT * FROM x))");
            execute("CREATE VIEW v_altered AS (SELECT a FROM x)");
            execute("CREATE VIEW v_rows AS (SELECT DISTINCT * FROM x)");
            drainWalAndViewQueues();
            execute("ALTER VIEW v_altered AS (SELECT a FROM (SELECT DISTINCT * FROM x))");
            execute("CREATE VIEW v_outer AS (SELECT a FROM v_rows)");
            drainWalAndViewQueues();

            // each view reads the column x gains, and a principal with SELECT on the view only still reads it
            execute("ALTER TABLE x ADD COLUMN e INT");
            execute("INSERT INTO x VALUES (7_200_000_000, 'k2', 5, 6, 42)");
            drainWalAndViewQueues();
            final String expected = """
                    a
                    1
                    3
                    5
                    """;
            try (SqlExecutionContext context = denyBaseTableSelectContext()) {
                for (String viewName : new String[]{"v_distinct", "v_grouped", "v_altered", "v_outer"}) {
                    assertQuery("SELECT * FROM " + viewName + " ORDER BY a")
                            .withContext(context)
                            .noLeakCheck()
                            .expectSize()
                            .returns(expected);
                }
                assertQuery("SELECT * FROM v_union ORDER BY a")
                        .withContext(context)
                        .noLeakCheck()
                        .returns(expected);
            }
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
    public void testWildcardsOfSelectsReadingWholeRows() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A SELECT DISTINCT, a SELECT that groups and the branches of a set operation other than UNION ALL
            // read every column their wildcards select, whichever columns the query above them keeps.
            assertDependencies("SELECT a FROM (SELECT DISTINCT * FROM x)", "{x=[*, a, b, k, ts]}");
            assertDependencies("SELECT count() FROM (SELECT DISTINCT * FROM x)", "{x=[*, a, b, k, ts]}");
            assertDependencies("SELECT a FROM (SELECT DISTINCT * FROM (SELECT * FROM x))", "{x=[*, a, b, k, ts]}");
            assertDependencies("SELECT a FROM (SELECT DISTINCT t.* FROM x t JOIN y u ON (k))", "{x=[*, a, b, k, ts], y=[k]}");
            assertDependencies("SELECT a FROM (SELECT *, count() AS n FROM x)", "{x=[*, a, b, k, ts]}");
            assertDependencies(
                    "SELECT a FROM (SELECT * FROM x UNION SELECT * FROM y)",
                    "{x=[*, a, b, k, ts], y=[*, c, d, k, ts]}"
            );
            assertDependencies(
                    "SELECT a FROM (SELECT * FROM x EXCEPT SELECT * FROM y)",
                    "{x=[*, a, b, k, ts], y=[*, c, d, k, ts]}"
            );
            assertDependencies(
                    "SELECT a FROM (SELECT * FROM x INTERSECT ALL SELECT * FROM y)",
                    "{x=[*, a, b, k, ts], y=[*, c, d, k, ts]}"
            );
            // an operation other than UNION ALL compares the rows of every branch
            assertDependencies(
                    "SELECT a FROM (SELECT * FROM x UNION ALL SELECT * FROM y UNION SELECT * FROM x)",
                    "{x=[*, a, b, k, ts], y=[*, c, d, k, ts]}"
            );
            execute("CREATE VIEW v_rows AS (SELECT DISTINCT * FROM x)");
            drainWalAndViewQueues();
            assertDependencies("SELECT a FROM v_rows", "{v_rows=[], x=[*, a, b, k, ts]}");

            // such a SELECT reads only the columns a SELECT list names, its own or one under it
            assertDependencies("SELECT a FROM (SELECT DISTINCT * FROM (SELECT a, b FROM x))", "{x=[a, b]}");
            assertDependencies("SELECT a FROM (SELECT DISTINCT a, b FROM (SELECT * FROM x))", "{x=[a, b]}");
            // a window function groups no rows
            assertDependencies("SELECT a FROM (SELECT *, sum(b) OVER () AS s FROM x)", "{x=[a]}");
            // a sub-query in an expression reads no column through the view
            assertDependencies(
                    "SELECT a FROM x WHERE k IN (SELECT k FROM (SELECT DISTINCT * FROM y))",
                    "{x=[a, k], y=[c, d, k, ts]}",
                    false
            );
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

    // Records the SELECT as the definition of the view v with CREATE VIEW, ALTER VIEW and CREATE OR REPLACE
    // VIEW in turn, and runs the assertion after each.
    private static void forEachViewStatement(String viewSql, ViewStatementAssertion assertion) throws Exception {
        execute("CREATE VIEW v AS (" + viewSql + ")");
        drainWalAndViewQueues();
        assertion.assertView("CREATE VIEW");

        // a definition that reads no table first, so that the statement records every dependency itself
        execute("ALTER VIEW v AS (SELECT x FROM long_sequence(1))");
        drainWalAndViewQueues();
        execute("ALTER VIEW v AS (" + viewSql + ")");
        drainWalAndViewQueues();
        assertion.assertView("ALTER VIEW");

        execute("ALTER VIEW v AS (SELECT x FROM long_sequence(1))");
        drainWalAndViewQueues();
        execute("CREATE OR REPLACE VIEW v AS (" + viewSql + ")");
        drainWalAndViewQueues();
        assertion.assertView("CREATE OR REPLACE VIEW");

        execute("DROP VIEW v");
        drainWalAndViewQueues();
    }

    private static String print(SqlExecutionContext context, String sql) throws Exception {
        return TestUtils.printSqlToString(engine, context, sql, new StringSink());
    }

    private void assertDependencies(String viewSql, String expectedDependencies) throws Exception {
        assertDependencies(viewSql, expectedDependencies, true);
    }

    // Asserts the dependency map CREATE VIEW, ALTER VIEW and CREATE OR REPLACE VIEW give the SELECT of a
    // view, and, unless told otherwise, that a principal with no permission on the columns of the base
    // tables reads the view exactly as a principal with every permission does.
    private void assertDependencies(String viewSql, String expectedDependencies, boolean isReadableWithViewPermissionOnly) throws Exception {
        forEachViewStatement(
                viewSql,
                statement -> assertViewDependencies(viewSql, expectedDependencies, isReadableWithViewPermissionOnly, statement)
        );
    }

    // Asserts that a principal with no permission on the columns of the base tables cannot read through
    // the view, by overriding a variable the view declares OVERRIDABLE, a column the view does not read.
    private void assertOverrideDenied(String viewSql, String readSql, String expectedFailure) throws Exception {
        forEachViewStatement(viewSql, statement -> {
            try (SqlExecutionContext context = denyBaseTableSelectContext()) {
                assertQuery(readSql).withContext(context).noLeakCheck().failsWith(expectedFailure);
            } catch (AssertionError e) {
                throw new AssertionError(statement + ": " + viewSql, e);
            }
        });
    }

    private void assertViewDependencies(String viewSql, String expectedDependencies, boolean isReadableWithViewPermissionOnly, String statement) throws Exception {
        Assert.assertEquals(statement + ": " + viewSql, expectedDependencies, dependenciesOf("v"));
        if (isReadableWithViewPermissionOnly) {
            // The views differ in timestamp, size and random access, which the read infers. It returns the
            // rows a principal with every permission reads.
            final String expected = print(sqlExecutionContext, "SELECT * FROM v");
            try (SqlExecutionContext context = denyBaseTableSelectContext()) {
                assertQuery("SELECT * FROM v")
                        .withContext(context)
                        .noLeakCheck()
                        .inferTimestamp()
                        .inferRandomAccess()
                        .sizeMayVary()
                        .returns(expected);
            } catch (AssertionError e) {
                throw new AssertionError(statement + ": " + viewSql, e);
            }
        }
    }

    @FunctionalInterface
    private interface ViewStatementAssertion {
        void assertView(String statement) throws Exception;
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
