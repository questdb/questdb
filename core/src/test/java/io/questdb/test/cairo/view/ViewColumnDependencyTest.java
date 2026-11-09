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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.SqlOptimiser;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.IntFunction;

/**
 * A view's dependency map lists, for every table the view reads, the columns the view reads from it.
 * A reader of the view needs no permission on those columns, see
 * {@code AbstractPartitionFrameCursorFactory.authorizeSelect()}. These tests check that CREATE VIEW,
 * ALTER VIEW and CREATE OR REPLACE VIEW record exactly the columns the plan of the view reads through
 * the view, whichever alias, join, CTE or nested view the SELECT reads them through, and that a principal
 * with SELECT on the view only reads it.
 */
public class ViewColumnDependencyTest extends AbstractViewTest {

    @Test
    public void testAggregateOverOrderedSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Standalone queries retain the optimisation; only view definitions inherit a reader's order.
            assertQuery("SELECT k, avg(a) av FROM (SELECT * FROM x ORDER BY ts DESC)")
                    .noLeakCheck()
                    .assertsPlanContaining("Row forward scan");
            assertViewResult(
                    "SELECT k, avg(a) av FROM (SELECT * FROM x ORDER BY ts DESC)",
                    "{x=[a, k, ts]}",
                    "k\tav\nk1\t2.0\n"
            );
            assertViewResult(
                    "SELECT sum(a) s FROM (SELECT * FROM x ORDER BY b)",
                    "{x=[a, b]}",
                    "s\n4\n"
            );
            assertViewResult(
                    "WITH ordered AS (SELECT * FROM x ORDER BY b DESC) SELECT k, avg(a) av FROM ordered",
                    "{x=[a, b, k]}",
                    "k\tav\nk1\t2.0\n"
            );
            assertViewResult(
                    "SELECT avg(a) av FROM (SELECT * FROM x ORDER BY b LIMIT 1)",
                    "{x=[a, b]}",
                    "av\n1.0\n"
            );
            assertViewResult(
                    "SELECT avg(a) av FROM (SELECT * FROM x ORDER BY b) UNION ALL SELECT avg(c) FROM (SELECT * FROM y ORDER BY d)",
                    "{x=[a, b], y=[c, d]}",
                    "av\n2.0\n6.0\n"
            );
            assertReads(
                    "SELECT k, avg(a) av FROM (SELECT * FROM x ORDER BY ts DESC)",
                    "{x=[a, k, ts]}",
                    "SELECT av FROM v", "SELECT * FROM v WHERE av > 1", "SELECT count() FROM v"
            );
            execute("CREATE VIEW ordered_x AS (SELECT * FROM x ORDER BY b DESC)");
            drainWalAndViewQueues();
            assertViewResult(
                    "SELECT avg(a) av FROM ordered_x",
                    "{ordered_x=[], x=[a, b]}",
                    "av\n2.0\n"
            );
        });
    }

    @Test
    public void testAggregateOverOrderedSubQueryWalModes() throws Exception {
        assertMemoryLeak(() -> {
            for (boolean isWal : new boolean[]{false, true}) {
                execute("CREATE TABLE w (ts TIMESTAMP, a DOUBLE, b LONG) TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
                for (int stage = 0; stage < 3; stage++) {
                    if (stage == 1) {
                        execute("INSERT INTO w VALUES (0, NULL, NULL)");
                    } else if (stage == 2) {
                        execute("INSERT INTO w VALUES (1, -2.0, 3), (2, 0.0, 2), (3, 2.0, 1)");
                    }
                    drainWalQueue();
                    assertViewResult(
                            "SELECT avg(a) av FROM (SELECT * FROM w ORDER BY b DESC)",
                            "{w=[a, b]}",
                            stage == 2 ? "av\n0.0\n" : "av\nnull\n"
                    );
                }
                execute("DROP TABLE w");
                drainWalAndViewQueues();
            }
        });
    }

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
    public void testExplicitTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE x2 (ts TIMESTAMP, ts2 TIMESTAMP, k SYMBOL, a LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x2 VALUES (0, 0, 'k1', 1), (3_600_000_000, 3_600_000_000, 'k1', 3)");
            drainWalQueue();
            // A TIMESTAMP() clause names the timestamp that a temporal join or SAMPLE BY reads from the query
            // it follows, so the scan of x2 reads ts2, and not the designated timestamp ts.
            assertDependencies(
                    "SELECT t.a, u.c FROM (SELECT k, a, ts2 FROM x2) t TIMESTAMP(ts2) ASOF JOIN y u ON (k)",
                    "{x2=[a, k, ts2], y=[c, k, ts]}"
            );
            assertDependencies(
                    "SELECT t.k, u.c FROM (SELECT k, ts2 FROM x2) t TIMESTAMP(ts2) SPLICE JOIN y u ON (k)",
                    "{x2=[k, ts2], y=[c, k, ts]}"
            );
            assertDependencies(
                    "SELECT count() FROM (SELECT * FROM x2) t TIMESTAMP(ts2) ASOF JOIN y u ON (k)",
                    "{x2=[k, ts2], y=[k, ts]}"
            );
            assertDependencies(
                    "WITH c AS (SELECT k, a, ts2 FROM x2) SELECT t.a, u.c FROM c t TIMESTAMP(ts2) ASOF JOIN y u ON (k)",
                    "{x2=[a, k, ts2], y=[c, k, ts]}"
            );
            assertDependencies("SELECT count() FROM (SELECT k, ts2 FROM x2) TIMESTAMP(ts2) SAMPLE BY 1h", "{x2=[ts2]}");
            // the view reads no ts, so a reader cannot read it by overriding @c
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @c := a SELECT t.v, u.c FROM (SELECT k, @c AS v, ts2 FROM x2) t TIMESTAMP(ts2) ASOF JOIN y u ON (k)",
                    "DECLARE @c := ts SELECT * FROM v",
                    "permission denied [table=x2, columns=ts]"
            );
            // A SAMPLE BY that fills keeps a model of its own, whose TIMESTAMP() clause names the timestamp it
            // reads from the sub-query, so the scan of x2 reads ts2 and not ts.
            assertDependencies(
                    "SELECT ts2, count() c, first(v) f FROM (SELECT k v, ts2 FROM x2) TIMESTAMP(ts2) SAMPLE BY 1h FILL(PREV)",
                    "{x2=[k, ts2]}"
            );
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @c := k SELECT ts2, count() c, first(v) f FROM (SELECT @c v, ts2 FROM x2) TIMESTAMP(ts2) SAMPLE BY 1h FILL(PREV)",
                    "DECLARE @c := ts SELECT * FROM v",
                    "permission denied [table=x2, columns=ts]"
            );
            // A sub-query that orders its rows takes its timestamp from its ORDER BY, so the temporal join needs
            // no timestamp of x2 the sub-query does not select, as long as the plan keeps the ORDER BY. A read that
            // counts the rows of the view drops it, and the temporal join reads ts, the designated timestamp, then,
            // unless a LIMIT needs the order of the rows.
            assertDependencies(
                    "SELECT t.a, u.c FROM (SELECT k, a, ts2 FROM x2 ORDER BY ts2) t ASOF JOIN y u ON (k)",
                    "{x2=[?ts, a, k, ts2], y=[c, k, ts]}"
            );
            assertDependencies(
                    "SELECT t.a, u.c FROM (SELECT k, a, ts2 FROM x2 ORDER BY ts2 LIMIT 10) t ASOF JOIN y u ON (k)",
                    "{x2=[a, k, ts2], y=[c, k, ts]}"
            );
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @c := a SELECT t.v, u.c FROM (SELECT k, @c AS v, ts2 FROM x2 ORDER BY ts2 LIMIT 10) t ASOF JOIN y u ON (k)",
                    "DECLARE @c := ts SELECT * FROM v",
                    "permission denied [table=x2, columns=ts]"
            );
        });
    }

    @Test
    public void testIndependentWildcardHolders() throws Exception {
        assertMemoryLeak(() -> assertIndependentWildcardHolders(false));
    }

    @Test
    public void testIndependentWildcardHoldersOfSameTable() throws Exception {
        assertMemoryLeak(() -> assertIndependentWildcardHolders(true));
    }

    @Test
    public void testLateralJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertDependencies(
                    "SELECT t.a, l.c FROM x t CROSS JOIN LATERAL (SELECT c FROM y u WHERE u.k = t.k LIMIT 1) l",
                    "{x=[a, k], y=[c, k]}"
            );
            // A LATERAL join that no condition keys joins without a key, so the statement compiles the SELECT again.
            // Its decorrelation scans x twice, once with a copy of the model of x that knows no table id, which
            // neither compile tells from the table it scans.
            assertReads(
                    "SELECT o.a, l.n FROM x o LEFT JOIN LATERAL (SELECT count(*) n FROM y WHERE y.c >= o.a AND y.c < o.a + 5) l ON true",
                    "{x=[a], y=[c]}",
                    "SELECT a FROM v", "SELECT n FROM v"
            );
            assertReads(
                    "SELECT o.a, l.n, p.mx FROM x o LEFT JOIN LATERAL (SELECT count(*) n FROM y WHERE y.c >= o.a AND y.c < o.a + 5) l ON true CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y) p",
                    "{x=[a], y=[?d, c]}",
                    "SELECT a FROM v", "SELECT mx FROM v"
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
            assertDependencies("SELECT u.* FROM (SELECT * FROM x) s JOIN y u ON (k)", "{x=[k], y=[*]}");
            // PIVOT selects the columns it groups and aggregates, not the ones of its source
            assertDependencies("SELECT * FROM (SELECT * FROM x) PIVOT (sum(a) FOR k IN ('k1'))", "{x=[a, k]}");
            // A sub-query in an expression selects no column into the view, and its scan of y authorizes
            // as the reader, so the view lists no column of y, and a principal without permission on y
            // does not read the view.
            assertDependencies(
                    "SELECT a FROM x WHERE k IN (SELECT k FROM (SELECT * FROM y))",
                    "{x=[a, k], y=[]}",
                    "permission denied [table=y, columns=k]"
            );

            // The view reads no column the base table gains later, even when its SELECT list names every
            // column the wildcard under it selects, so a reader cannot reach such a column through it.
            execute("CREATE VIEW v_narrowed AS (SELECT k, a FROM (SELECT * FROM x))");
            execute("CREATE VIEW v_named AS (DECLARE OVERRIDABLE @c := a SELECT ts, k, @c AS a, b FROM (SELECT * FROM x))");
            drainWalAndViewQueues();
            execute("ALTER TABLE x ADD COLUMN e INT");
            execute("INSERT INTO x VALUES (7_200_000_000, 'k2', 5, 6, 42)");
            drainWalAndViewQueues();
            Assert.assertEquals("{x=[a, k]}", dependenciesOf("v_narrowed"));
            Assert.assertEquals("{x=[a, b, k, ts]}", dependenciesOf("v_named"));
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
                assertQuery("DECLARE @c := e SELECT * FROM v_named")
                        .withContext(context)
                        .noLeakCheck()
                        .failsWith("permission denied [table=x, columns=e]");
            }
        });
    }

    @Test
    public void testNestedLateralJoins() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                // The optimiser shares the left side of a LATERAL join with the sub-query that decorrelates its
                // right side, so the walk of the columns the plan reads reaches the models of the inner joins
                // along a number of paths that doubles with each join. Nothing reads the column the right sides
                // select, so the plan reads no column of y.
                assertLateralJoinDependencies(compiler, count -> lateralJoins("SELECT k, a FROM x", count), "{x=[a, k], y=[]}");
                // the optimiser keeps every column of the left side of a LATERAL join
                assertLateralJoinDependencies(compiler, count -> lateralJoins("SELECT * FROM x", count), "{x=[*], y=[]}");
                // It also keeps every column of a sub-query whose columns the query above it names none of. The
                // search for the scan of x under the sub-query of the wildcard goes through the LATERAL joins
                // before it reaches the scan.
                assertLateralJoinDependencies(
                        compiler,
                        count -> "SELECT count() FROM (SELECT * FROM (SELECT * FROM x) w CROSS JOIN (" + lateralJoins("SELECT k, a FROM x", count) + ") c)",
                        "{x=[*], y=[]}"
                );
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
            assertDependencies("SELECT * FROM v_join", "{v_join=[], x=[*], y=[*]}");
            assertDependencies("SELECT w.* FROM v_join w", "{v_join=[], x=[*], y=[*]}");
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
            assertDependencies("SELECT * FROM (SELECT * FROM x)", "{x=[*]}");
            assertDependencies("SELECT * FROM (x WHERE a > 1)", "{x=[*]}");
            assertDependencies("WITH c AS (SELECT * FROM x) SELECT * FROM c WHERE a > 1", "{x=[*]}");
            assertDependencies("DECLARE @q := (SELECT * FROM x) SELECT * FROM @q", "{x=[*]}");
            assertDependencies("SELECT * FROM x JOIN (SELECT * FROM y) s ON (k)", "{x=[*], y=[*]}");
            assertDependencies("SELECT s.* FROM (SELECT * FROM x) s JOIN y u ON (k)", "{x=[*], y=[k]}");
            assertDependencies(
                    "SELECT * FROM (SELECT * FROM x UNION ALL SELECT * FROM y)",
                    "{x=[*], y=[*]}"
            );
            assertDependencies(
                    "SELECT t.* FROM x t CROSS JOIN LATERAL (SELECT * FROM y u WHERE u.k = t.k) l",
                    "{x=[*], y=[k]}"
            );
        });
    }

    @Test
    public void testOrderSensitiveAggregatesOverOrderedSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertViewResult(
                    "SELECT string_agg(a::STRING, ',') s FROM (SELECT * FROM x ORDER BY b DESC)",
                    "{x=[a, b]}",
                    "s\n3,1\n"
            );
            assertViewResult(
                    "SELECT array_agg(a::DOUBLE) arr FROM (SELECT * FROM x ORDER BY b DESC)",
                    "{x=[a, b]}",
                    "arr\n[3.0,1.0]\n"
            );
            assertViewResult(
                    "SELECT first(a) f, last(a) l FROM (SELECT * FROM x ORDER BY b DESC)",
                    "{x=[a, b]}",
                    "f\tl\n3\t1\n"
            );
        });
    }

    @Test
    public void testOverridableVariableInOrderedAggregate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String viewSql = "DECLARE OVERRIDABLE @c := a SELECT k, avg(@c) av FROM (SELECT * FROM x ORDER BY ts DESC)";
            assertDependencies(viewSql, "{x=[a, k, ts]}");
            assertOverrideDenied(viewSql, "DECLARE @c := b SELECT * FROM v", "permission denied [table=x, columns=b]");
        });
    }

    @Test
    public void testOverridableVariableReadsNoColumnOfSubQueryEveryOutputNeeds() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Every output of these views reads the sub-query their FROM clause joins without a key, so every read
            // gives it the columns it names, and none keeps every column of it. The views list no column that only an
            // unread column of the SELECT list of the sub-query reads, and a reader that overrides @m does not read d.
            final String[] viewSqls = {
                    "DECLARE OVERRIDABLE @m := c SELECT sum(x.a * p.mx) s FROM x CROSS JOIN (SELECT max(@m) mx, min(d) mn FROM y) p",
                    "DECLARE OVERRIDABLE @m := c SELECT x.a * p.f adj FROM x CROSS JOIN (SELECT last(@m) f, last(d) o FROM y) p",
                    "DECLARE OVERRIDABLE @m := c WITH p AS (SELECT max(@m) mx, min(d) mn FROM y) SELECT sum(x.a * p.mx) s FROM x CROSS JOIN p",
                    "DECLARE OVERRIDABLE @m := c SELECT a + mx s FROM (SELECT x.a, p.mx FROM x CROSS JOIN (SELECT max(@m) mx, min(d) mn FROM y) p)",
                    "DECLARE OVERRIDABLE @m := c SELECT x.a + q.qv s FROM x ASOF JOIN (SELECT ts, @m AS qv, d AS qw FROM y) q",
                    "DECLARE OVERRIDABLE @m := c SELECT q.ts, q.qv + x.a s FROM (SELECT ts, @m AS qv, d AS qw FROM y) q ASOF JOIN x",
            };
            final String[] expectedDependencies = {
                    "{x=[a], y=[c]}",
                    "{x=[a], y=[c]}",
                    "{x=[a], y=[c]}",
                    "{x=[a], y=[c]}",
                    "{x=[a, ts], y=[c, ts]}",
                    "{x=[a, ts], y=[c, ts]}",
            };
            for (int i = 0; i < viewSqls.length; i++) {
                assertDependencies(viewSqls[i], expectedDependencies[i]);
                assertOverrideDenied(viewSqls[i], "DECLARE @m := d SELECT * FROM v", "permission denied [table=y, columns=d]");
            }
            // through a table alias
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @m := u.c SELECT a + mx s FROM (SELECT x.a, p.mx FROM x CROSS JOIN (SELECT max(@m) mx, min(u.d) mn FROM y u) p)",
                    "DECLARE @m := u.d SELECT * FROM v",
                    "permission denied [table=y, columns=d]"
            );
            // An output reads the sub-query through another column of the same SELECT.
            assertReads(
                    "SELECT m1, m2 FROM (SELECT p.mx + 1 AS m1, m1 * 2 AS m2 FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y) p)",
                    "{x=[], y=[c]}",
                    "SELECT m1 FROM v", "SELECT m2 FROM v", "SELECT count() FROM v"
            );
            // A read that names only the key of a SELECT that groups prunes the aggregates, and keeps the sub-query
            // whole when the key reads none of it, so this view reads d.
            assertReads(
                    "SELECT x.k, sum(p.mx) s FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y) p",
                    "{x=[k], y=[?d, c]}",
                    "SELECT k FROM v", "SELECT s FROM v"
            );
            // The SELECT that only aggregates reads the sub-query through a SELECT of mx, which a read that names
            // only count() keeps whole, so that every read gives the sub-query mx, and none reads d.
            assertReads(
                    "SELECT sum(p.mx) s, count() n FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y) p",
                    "{x=[], y=[c]}",
                    "SELECT n FROM v", "SELECT s FROM v"
            );
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @m := c SELECT sum(p.mx) s, count() n FROM x CROSS JOIN (SELECT max(@m) mx, min(d) mn FROM y) p",
                    "DECLARE @m := d SELECT * FROM v",
                    "permission denied [table=y, columns=d]"
            );
            // a branch of a set operation, which prunes its columns as the first branch does
            assertReads(
                    "SELECT x.a, p.mx FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y) p UNION ALL SELECT x.a, q.mc FROM x CROSS JOIN (SELECT min(c) mc, max(d) md FROM y) q",
                    "{x=[a], y=[?d, c]}",
                    "SELECT a FROM v", "SELECT mx FROM v"
            );
        });
    }

    @Test
    public void testOverridableVariableReadsNoColumnOnlyASubQueryReads() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The sub-query reads b as the reader, and a read of the view with @m at 1 skips it. The
            // scan of x that goes through the view reads no b, so a reader cannot read it by overriding @c.
            final String viewSql = """
                    DECLARE OVERRIDABLE @c := a, OVERRIDABLE @m := 1
                    SELECT k, @c AS v FROM x WHERE @m = 1 OR a < (SELECT max(u.b) FROM x u)
                    """;
            assertDependencies(viewSql, "{x=[a, k]}");
            assertOverrideDenied(viewSql, "DECLARE @c := b SELECT * FROM v", "permission denied [table=x, columns=b]");
        });
    }

    @Test
    public void testOverridableVariableReadsNoColumnOnlyOtherReadsScan() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE x2 (ts TIMESTAMP, ts2 TIMESTAMP, k SYMBOL, a LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x2 VALUES (0, 0, 'k1', 1), (3_600_000_000, 3_600_000_000, 'k1', 3)");
            execute("CREATE VIEW v_join AS (SELECT * FROM x JOIN y ON (k))");
            drainWalAndViewQueues();
            // A read that names only an output that reads no column, such as 'src' or @c, keeps the sub-query whole,
            // and its plan scans every column of x and y, so the view lends them to such a read. A reader that sets a
            // variable of the view could read any of them through it, so it reads through the view only the columns
            // the plan of the view reads.
            assertReads(
                    "DECLARE OVERRIDABLE @s := 'k1' SELECT 'src' src, k FROM (SELECT * FROM x JOIN y ON (k)) WHERE k = @s",
                    "{x=[?*, k], y=[?*, k]}",
                    "SELECT src FROM v",
                    // a variable of the reader that the view does not declare sets nothing in the view
                    "DECLARE @t := 'k2' SELECT src FROM v"
            );
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @s := 'k1' SELECT 'src' src, k FROM (SELECT * FROM x JOIN y ON (k)) WHERE k = @s",
                    "DECLARE @s := CASE WHEN d = 6 THEN 'k1' ELSE 'k2' END SELECT * FROM v",
                    "permission denied [table=y, columns=d]"
            );
            // also when the value the reader sets reads no column: the read keeps the sub-query whole, and its scan of x
            // reads the columns the plan of the view does not
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @s := 'k1' SELECT 'src' src, k FROM (SELECT * FROM x) WHERE k = @s",
                    "DECLARE @s := 'k1' SELECT src FROM v",
                    "permission denied [table=x, columns=ts,a,b]"
            );
            // the variable as an output, the wildcard in a CTE and in a view the view reads, and the table the
            // wildcard selects from alone
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @c := 0 SELECT k, @c AS v FROM (SELECT * FROM x JOIN y ON (k))",
                    "DECLARE @c := d SELECT * FROM v",
                    "permission denied [table=y, columns=d]"
            );
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @c := 0 WITH j AS (SELECT * FROM x JOIN y ON (k)) SELECT k, @c AS v FROM j",
                    "DECLARE @c := d SELECT * FROM v",
                    "permission denied [table=y, columns=d]"
            );
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @c := 0 SELECT k, @c AS v FROM v_join",
                    "DECLARE @c := d SELECT * FROM v",
                    "permission denied [table=y, columns=d]"
            );
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @c := 0 SELECT k, a, @c AS v FROM (SELECT * FROM x)",
                    "DECLARE @c := b SELECT * FROM v",
                    "permission denied [table=x, columns=b]"
            );
            // a sub-query the FROM clause joins without a key, which a read that names only a can leave whole
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @c := 0 SELECT x.a, p.c, @c AS v FROM x CROSS JOIN (SELECT * FROM y) p",
                    "DECLARE @c := d SELECT * FROM v",
                    "permission denied [table=y, columns=d]"
            );
            // the designated timestamp a read that counts the rows of the view takes in place of the ORDER BY
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @c := a SELECT t.v, u.c FROM (SELECT k, @c AS v, ts2 FROM x2 ORDER BY ts2) t ASOF JOIN y u ON (k)",
                    "DECLARE @c := ts SELECT * FROM v",
                    "permission denied [table=x2, columns=ts]"
            );

            // A reader sets the variable of a view the view reads through the view, so it reads only what the plan of
            // the view reads too. The view's own value of that variable is not a reader's.
            execute("CREATE VIEW v_in AS (DECLARE OVERRIDABLE @c := 0 SELECT k, @c AS v FROM (SELECT * FROM x JOIN y ON (k)))");
            drainWalAndViewQueues();
            assertOverrideDenied("SELECT 'o' o, k, v FROM v_in", "DECLARE @c := d SELECT * FROM v", "permission denied [table=y, columns=d]");
            assertReads("DECLARE @c := 1 SELECT 'o' o, k, v FROM v_in", "{v_in=[], x=[?*, k], y=[?*, k]}", "SELECT o FROM v");
        });
    }

    @Test
    public void testOverridableVariableReadsNoColumnOnlyOtherReadsScanInAnyBranch() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Every branch of a set operation takes the reader's value of a variable its own DECLARE block marks
            // OVERRIDABLE, so a reader that sets a variable of a later branch reads through the view only the columns
            // the plan of the view reads, like one that sets a variable of the first branch.
            final String laterBranch = "DECLARE OVERRIDABLE @s := 'k1' SELECT 'src' src, k FROM (SELECT * FROM x JOIN y ON (k)) WHERE k = @s";
            final String overrideReadSql = "DECLARE @s := CASE WHEN d = 6 THEN 'k1' ELSE 'k2' END SELECT * FROM v";
            assertReads("SELECT 'a' src, k FROM x WHERE k = 'none' UNION ALL " + laterBranch, "{x=[?*, k], y=[?*, k]}", "SELECT src FROM v");
            for (String setOperation : new String[]{"UNION ALL", "UNION", "EXCEPT", "INTERSECT"}) {
                assertOverrideDenied(
                        "SELECT 'a' src, k FROM x WHERE k = 'none' " + setOperation + ' ' + laterBranch,
                        overrideReadSql,
                        "permission denied [table=y, columns=d]"
                );
            }
            // the variable as an output of the third branch
            assertOverrideDenied(
                    """
                            SELECT 'a' v, k FROM x WHERE k = 'none'
                            UNION ALL SELECT 'b' v, k FROM y WHERE k = 'none'
                            UNION ALL DECLARE OVERRIDABLE @c := 'c' SELECT @c AS v, k FROM (SELECT * FROM x JOIN y ON (k))
                            """,
                    "DECLARE @c := d::STRING SELECT * FROM v",
                    "permission denied [table=y, columns=d]"
            );
            // a later branch of a view the view reads
            execute("CREATE VIEW v_in AS (SELECT 'a' src, k FROM x WHERE k = 'none' UNION ALL " + laterBranch + ")");
            drainWalAndViewQueues();
            assertOverrideDenied("SELECT 'o' o, src, k FROM v_in", overrideReadSql, "permission denied [table=y, columns=d]");
        });
    }

    @Test
    public void testOverridableVariableReadsNoColumnOnlyOtherReadsScanInAnyScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE zi (ts TIMESTAMP, k SYMBOL INDEX, k2 SYMBOL, v LONG, w LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE zp (ts TIMESTAMP, k SYMBOL INDEX TYPE POSTING, v LONG, w LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO zi VALUES (0, 'k1', 'q1', 1, 2), (3_600_000_000, 'k1', 'q2', 3, 4)");
            execute("INSERT INTO zp VALUES (0, 'k1', 1, 2), (3_600_000_000, 'k2', 3, 4)");
            drainWalQueue();
            // Every kind of scan lends the columns only the other reads scan, here the reads that name o alone, to a
            // read that sets no variable of the view, and not to one that sets it: LATEST ON a column with an index,
            // ...
            assertOverrideDeniedInScan(
                    "DECLARE OVERRIDABLE @c := 0 SELECT k, @c AS o FROM (SELECT * FROM zi LATEST ON ts PARTITION BY k)",
                    "{zi=[?*, k]}",
                    "DECLARE @c := w SELECT * FROM v",
                    "permission denied [table=zi, columns=w]",
                    "LatestByAllIndexed"
            );
            // ... on several symbols, ...
            assertOverrideDeniedInScan(
                    "DECLARE OVERRIDABLE @c := 0 SELECT k, k2, @c AS o FROM (SELECT * FROM zi LATEST ON ts PARTITION BY k, k2)",
                    "{zi=[?*, k, k2]}",
                    "DECLARE @c := w SELECT * FROM v",
                    "permission denied [table=zi, columns=w]",
                    "LatestByAllSymbolsFiltered"
            );
            // ... after a filter on the designated timestamp, which the plan of the view reads, and after another
            // filter, ...
            assertOverrideDeniedInScan(
                    "DECLARE OVERRIDABLE @c := 0 SELECT k2, @c AS o FROM (SELECT * FROM zi WHERE ts >= '1970-01-01' LATEST ON ts PARTITION BY k2)",
                    "{zi=[?*, k2, ts]}",
                    "DECLARE @c := w SELECT * FROM v",
                    "permission denied [table=zi, columns=w]",
                    "Interval backward scan on: zi"
            );
            assertOverrideDeniedInScan(
                    "DECLARE OVERRIDABLE @c := 0 SELECT k2, @c AS o FROM (SELECT * FROM zi WHERE v >= 0 LATEST ON ts PARTITION BY k2)",
                    "{zi=[?*, k2, v]}",
                    "DECLARE @c := w SELECT * FROM v",
                    "permission denied [table=zi, columns=w]",
                    "LatestByDeferredListValuesFiltered",
                    "filter: v>=0",
                    "Frame backward scan on: zi"
            );
            // ... and the distinct values of a column with a posting index, without and with a filter on the
            // designated timestamp, beside a sub-query that the read that names o alone keeps whole
            assertOverrideDeniedInScan(
                    "DECLARE OVERRIDABLE @c := v SELECT t.o, p.v FROM (SELECT DISTINCT @c AS o FROM zp) t CROSS JOIN (SELECT * FROM zp) p",
                    "{zp=[?*, v]}",
                    "DECLARE @c := k SELECT * FROM v",
                    "permission denied [table=zp, columns=k]",
                    "PostingIndex op: distinct on: o",
                    "Frame forward scan on: zp"
            );
            assertOverrideDeniedInScan(
                    "DECLARE OVERRIDABLE @c := v SELECT t.o, p.v FROM (SELECT DISTINCT @c AS o FROM zp WHERE ts >= '1970-01-01') t CROSS JOIN (SELECT * FROM zp) p",
                    "{zp=[?*, ts, v]}",
                    "DECLARE @c := k SELECT * FROM v",
                    "permission denied [table=zp, columns=k]",
                    "PostingIndex op: distinct on: o",
                    "Interval forward scan on: zp"
            );
        });
    }

    @Test
    public void testOverridableVariableReadsNoColumnOnlyOtherReadsScanThroughOuterView() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The views lend the columns their sub-queries select to a read that names only v, which keeps the
            // sub-query whole, and not to a read that sets @c.
            execute("CREATE VIEW v_alias AS (DECLARE OVERRIDABLE @c := 0 SELECT l.k, @c AS v FROM (SELECT u.ts, u.k, u.a, u.b FROM x u) l)");
            execute("CREATE VIEW v_join AS (DECLARE OVERRIDABLE @c := 0 SELECT k, @c AS v FROM (SELECT x.k, a, b, c, d FROM x JOIN y ON (k)))");
            execute("CREATE VIEW v_wildcard AS (DECLARE OVERRIDABLE @c := 0 SELECT k, @c AS v FROM (SELECT * FROM x))");
            drainWalAndViewQueues();
            Assert.assertEquals("{x=[?a, ?b, ?ts, k]}", dependenciesOf("v_alias"));
            Assert.assertEquals("{x=[?a, ?b, k], y=[?c, ?d, k]}", dependenciesOf("v_join"));
            Assert.assertEquals("{x=[?*, k]}", dependenciesOf("v_wildcard"));

            // A view whose own plan names only v of such a view is such a read: it scans the columns through that view,
            // and lends each of them to the reads that view lends it to. A reader that sets @c of that view through it
            // reads only the columns that view lends to every read.
            assertReads("SELECT v FROM v_alias", "{v_alias=[], x=[?a, ?b, ?ts, k]}", "SELECT v FROM v", "DECLARE @c := k SELECT * FROM v");
            assertOverrideDenied("SELECT v FROM v_alias", "DECLARE @c := b SELECT * FROM v", "permission denied [table=x, columns=b]");
            assertOverrideDenied("SELECT v FROM v_alias", "DECLARE @c := ts SELECT * FROM v", "permission denied [table=x, columns=ts]");
            assertReads("SELECT v FROM v_join", "{v_join=[], x=[?a, ?b, k], y=[?c, ?d, k]}", "SELECT v FROM v");
            assertOverrideDenied("SELECT v FROM v_join", "DECLARE @c := d SELECT * FROM v", "permission denied [table=y, columns=d]");
            assertReads("SELECT v FROM v_wildcard", "{v_wildcard=[], x=[?*, k]}", "SELECT v FROM v");
            assertOverrideDenied("SELECT v FROM v_wildcard", "DECLARE @c := b SELECT * FROM v", "permission denied [table=x, columns=b]");
        });
    }

    @Test
    public void testOverridableVariableReadsNoColumnTheViewDoesNot() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String viewSql = "DECLARE OVERRIDABLE @c := t.a SELECT t.ts, @c FROM x t";
            assertDependencies(viewSql, "{x=[a, ts]}");
            assertOverrideDenied(viewSql, "DECLARE @c := t.b SELECT * FROM v", "permission denied [table=x, columns=b]");
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
            // A sub-query in an expression reads y as the reader, not through the view, so the view lists
            // y with none of its columns, and a principal without permission on y does not read the view.
            assertDependencies(
                    "SELECT t.a FROM x t WHERE t.k IN (SELECT u.k FROM y u WHERE u.c > 0)",
                    "{x=[a, k], y=[]}",
                    "permission denied [table=y, columns=k,c]"
            );
        });
    }

    @Test
    public void testSubQueryJoinedWithoutKey() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE z (ts TIMESTAMP, k SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO z VALUES (0, 'k1', 9), (3_600_000_000, 'k2', 10)");
            drainWalQueue();
            // The plan of the view keeps only the columns of the sub-query the view selects, but a read that names
            // none of them keeps every column of the sub-query, so the view reads every column the SELECT list of
            // the sub-query reads, and every column of a table a wildcard it keeps whole selects.
            assertReads(
                    "SELECT x.ts, x.a, p.mx FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y) p",
                    "{x=[a, ts], y=[?d, c]}",
                    "SELECT ts, a FROM v", "SELECT a FROM v WHERE a > 1", "SELECT sum(a) FROM v"
            );
            assertReads("SELECT x.ts, x.a, p.mx FROM x, (SELECT max(c) mx, min(d) mn FROM y) p", "{x=[a, ts], y=[?d, c]}", "SELECT ts, a FROM v");
            assertReads("SELECT ts, a, mx FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y)", "{x=[a, ts], y=[?d, c]}", "SELECT ts, a FROM v");
            assertReads("SELECT p.mx, y.c FROM (SELECT max(a) mx, min(b) mn FROM x) p CROSS JOIN y", "{x=[?b, a], y=[c]}", "SELECT c FROM v");
            // The view needs the timestamp of the sub-query, which a read that needs no timestamp of the view
            // does not, and then the read keeps every column of the sub-query.
            assertReads(
                    "SELECT x.a FROM (SELECT ts, k, first(c) mx, last(d) mn FROM y SAMPLE BY 1h) p CROSS JOIN x",
                    "{x=[a], y=[?c, ?d, k, ts]}",
                    "SELECT count() FROM v", "SELECT a FROM v"
            );
            assertReads(
                    "SELECT x.a FROM (SELECT * FROM (SELECT ts, k, c mx, d mn FROM y) ORDER BY ts) p CROSS JOIN x",
                    "{x=[a], y=[?c, ?d, ?k, ts]}",
                    "SELECT count() FROM v", "SELECT a FROM v"
            );
            assertReads(
                    "SELECT x.ts, x.a, p.c FROM x CROSS JOIN (SELECT * FROM y LIMIT 1) p",
                    "{x=[a, ts], y=[?*, c]}",
                    "SELECT ts, a FROM v", "SELECT ts, a FROM v LIMIT 1"
            );
            assertReads("SELECT x.ts, x.k, x.a, p.v threshold FROM x CROSS JOIN (z LATEST ON ts PARTITION BY k) p", "{x=[a, k, ts], z=[?*, k, v]}", "SELECT ts, k, a FROM v");
            assertReads("SELECT a, v FROM (SELECT * FROM x) CROSS JOIN (SELECT v FROM z)", "{x=[?*, a], z=[v]}", "SELECT v FROM v");
            assertReads("SELECT x.a, z.v FROM x CROSS JOIN (SELECT * FROM z) z", "{x=[a], z=[?*, v]}", "SELECT a FROM v");
            assertReads(
                    "SELECT x.a, p.c, q.v FROM x CROSS JOIN (SELECT c, d FROM y) p CROSS JOIN (SELECT v, k FROM z) q",
                    "{x=[a], y=[?d, c], z=[?k, v]}",
                    "SELECT a FROM v", "SELECT c FROM v", "SELECT v FROM v"
            );
            // a temporal join without ON joins without a key
            assertReads(
                    "WITH q AS (SELECT * FROM y WHERE k = 'k1') SELECT t.ts, t.a, q.c FROM (SELECT * FROM x WHERE k = 'k1') t ASOF JOIN q",
                    "{x=[?*, a, k, ts], y=[?*, c, k, ts]}",
                    "SELECT ts, a FROM v"
            );
            assertReads("SELECT x.ts, x.a, q.c FROM x LT JOIN (SELECT * FROM y) q", "{x=[a, ts], y=[?*, c, ts]}", "SELECT ts, a FROM v");
            assertReads("SELECT x.ts, x.a, q.c FROM x SPLICE JOIN (SELECT * FROM y) q", "{x=[a, ts], y=[?*, c, ts]}", "SELECT ts, a FROM v");
            // A read names the key of a sub-query a join condition names, and a table keeps only the columns a
            // read names, so neither makes a read of the view read more than the plan of the view.
            assertReads("SELECT x.a, p.mx FROM x JOIN (SELECT k, max(c) mx, min(d) mn FROM y GROUP BY k) p ON (k)", "{x=[a, k], y=[c, k]}", "SELECT a FROM v");
            assertReads(
                    "SELECT t.a, p.mx FROM (SELECT k, a, b FROM x) t JOIN (SELECT k, max(c) mx, min(d) mn FROM y GROUP BY k) p ON (k)",
                    "{x=[a, k], y=[c, k]}",
                    "SELECT a FROM v", "SELECT mx FROM v"
            );
            assertReads(
                    "SELECT t.mb, p.mx FROM (SELECT k, max(a) ma, max(b) mb FROM x GROUP BY k) t JOIN (SELECT k, max(c) mx, min(d) mn FROM y GROUP BY k) p ON (k)",
                    "{x=[b, k], y=[c, k]}",
                    "SELECT mb FROM v", "SELECT mx FROM v"
            );
            assertReads("SELECT x.ts, x.a, y.c FROM x CROSS JOIN y", "{x=[a, ts], y=[c]}", "SELECT ts, a FROM v");
            // the second compile opens the cursor function of long_sequence(), and closes it
            assertReads("SELECT x.a, p.v FROM x CROSS JOIN (SELECT x v, x * 2 w FROM long_sequence(2)) p", "{x=[a]}", "SELECT a FROM v");
            // The sub-query the FROM clause joins without a key reads mx alone of the one it selects from.
            assertReads("SELECT x.a, p.mx FROM x CROSS JOIN (SELECT mx FROM (SELECT max(c) mx, min(d) mn FROM y)) p", "{x=[a], y=[c]}", "SELECT a FROM v");
            // Every column of this view reads the sub-query, so no read of it keeps every column of the
            // sub-query, and none reads d.
            assertReads("SELECT a + mx s FROM (SELECT x.a, p.mx, p.mn FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y) p)", "{x=[a], y=[c]}", "SELECT s FROM v");
            // A clause of the view that names only columns of other sources leaves the sub-query to the reads.
            assertReads(
                    "SELECT x.a, p.mx FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y) p ORDER BY a LIMIT 1",
                    "{x=[a], y=[?d, c]}",
                    "SELECT a FROM v", "SELECT mx FROM v"
            );
            // The view reads no column the SELECT list of the sub-query does not, whatever a read names. A reader that
            // sets @c reads only the columns the plan of the view reads, c of y, and the read that keeps the sub-query
            // whole reads k too.
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @c := c SELECT x.a, p.v FROM x CROSS JOIN (SELECT @c AS v, k FROM y) p",
                    "DECLARE @c := d SELECT a FROM v",
                    "permission denied [table=y, columns=d,k]"
            );
        });
    }

    @Test
    public void testSubQueryJoinedWithoutKeyGivenColumnsByEveryRead() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Every read of these views gives the sub-query they join without a key the column mx, which an output
            // that some read does not name selects, so no read keeps every column of the sub-query, and none reads
            // d. A read keeps every column of a SELECT DISTINCT and of a set operation other than UNION ALL, those
            // a clause of the SELECT names, and the keys of a branch of a set operation that groups.
            assertReads(
                    "SELECT DISTINCT x.a, p.mx FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y) p",
                    "{x=[a], y=[c]}",
                    "SELECT a FROM v", "SELECT count() FROM v"
            );
            assertReads(
                    "SELECT x.a, p.mx FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y) p UNION SELECT a, b FROM x",
                    "{x=[a, b], y=[c]}",
                    "SELECT a FROM v"
            );
            assertReads(
                    "SELECT x.a, p.mx FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y) p ORDER BY mx, a LIMIT 1",
                    "{x=[a], y=[c]}",
                    "SELECT a FROM v"
            );
            assertReads(
                    "SELECT mx, s FROM (SELECT p.mx, x.a s FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y) p UNION ALL SELECT a, sum(b) FROM x)",
                    "{x=[a, b], y=[c]}",
                    "SELECT s FROM v", "SELECT mx FROM v"
            );
            // a key that a function computes
            assertReads(
                    "SELECT upper(p.k) uk, sum(x.a) s FROM x CROSS JOIN (SELECT k, c, d FROM y) p",
                    "{x=[a], y=[k]}",
                    "SELECT s FROM v", "SELECT uk FROM v"
            );
            // the key of a SAMPLE BY, which groups the rows by a function of the timestamp
            assertReads(
                    "SELECT p.ts, sum(x.a) s FROM (SELECT ts, c, d FROM y) p CROSS JOIN x SAMPLE BY 1h",
                    "{x=[a], y=[ts]}",
                    "SELECT s FROM v", "SELECT ts FROM v"
            );
            // A HORIZON JOIN that aggregates groups its rows by every other column it selects. One without ON joins
            // its right side without a key.
            final String horizonJoin = "SELECT h.offset o, u.mx, sum(t.a) s FROM x t HORIZON JOIN (SELECT ts, c mx, d mn FROM y) u RANGE FROM 0s TO 1s STEP 1s AS h";
            assertReads(horizonJoin, "{x=[a, ts], y=[c, ts]}", "SELECT s FROM v", "SELECT o FROM v");
            assertReads("SELECT o, mx, s FROM (" + horizonJoin + ")", "{x=[a, ts], y=[c, ts]}", "SELECT s FROM v");
            assertReads(
                    """
                            SELECT o, s FROM (
                                SELECT p.mx o, x.a s FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y) p
                                UNION ALL
                                SELECT h.offset o, sum(u.c) s FROM x t HORIZON JOIN y u ON (t.k = u.k) RANGE FROM 0s TO 1s STEP 1s AS h
                            )
                            """,
                    "{x=[a, k, ts], y=[c, k, ts]}",
                    "SELECT s FROM v"
            );
        });
    }

    @Test
    public void testSubQueryJoinedWithoutKeyInAuditedView() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The second compile parses the SELECT as the first one does, so the top-level DECLARE block of the view
            // may mark a variable AUDITED. A sub-query cannot declare one, so no read selects from these SELECTs
            // inline, see assertDependenciesScanned().
            assertReads(
                    false,
                    "DECLARE AUDITED @z := 0 SELECT x.ts, x.a, p.mx FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y) p WHERE x.a > @z",
                    "{x=[a, ts], y=[?d, c]}",
                    "SELECT ts, a FROM v"
            );
            assertReads(
                    false,
                    "DECLARE OVERRIDABLE AUDITED @z := 0 SELECT x.ts, x.a, q.c FROM x ASOF JOIN (SELECT * FROM y WHERE c > @z) q",
                    "{x=[a, ts], y=[?*, c, ts]}",
                    "SELECT ts, a FROM v"
            );
        });
    }

    @Test
    public void testSubQueryJoinedWithoutKeyInBareStatement() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Without brackets, the SELECT of a view runs to the end of the statement, a semicolon and an unclosed
            // comment included, and the second compile parses it as the first one does.
            final String viewSql = "SELECT x.ts, x.a, p.mx FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y) p";
            final String expected = "{x=[a, ts], y=[?d, c]}";
            for (String ending : new String[]{";", "; -- the largest c", " /* the largest c"}) {
                execute("CREATE VIEW v AS " + viewSql + ending);
                drainWalAndViewQueues();
                assertViewDependencies(viewSql, expected, null, "CREATE VIEW" + ending);

                execute("ALTER VIEW v AS SELECT x FROM long_sequence(1)");
                drainWalAndViewQueues();
                execute("ALTER VIEW v AS " + viewSql + ending);
                drainWalAndViewQueues();
                assertViewDependencies(viewSql, expected, null, "ALTER VIEW" + ending);

                execute("ALTER VIEW v AS SELECT x FROM long_sequence(1)");
                drainWalAndViewQueues();
                execute("CREATE OR REPLACE VIEW v AS " + viewSql + ending);
                drainWalAndViewQueues();
                assertViewDependencies(viewSql, expected, null, "CREATE OR REPLACE VIEW" + ending);

                execute("DROP VIEW v");
                drainWalAndViewQueues();
            }
        });
    }

    @Test
    public void testSubQueryJoinedWithoutKeyInSelectEndingWithComment() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A SELECT may end with a line comment. CREATE VIEW and ALTER VIEW keep the line break their closing
            // parenthesis follows, a caller that compiles the SELECT of a view on its own may not.
            final String viewSql = "SELECT x.ts, x.a, p.mx FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y) p -- the largest c";
            assertReads(viewSql + "\n", "{x=[a, ts], y=[?d, c]}", "SELECT ts, a FROM v");
            final LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = new LowerCaseCharSequenceObjHashMap<>();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.generateViewExecutionModel(viewSql, sqlExecutionContext, dependencies);
            }
            Assert.assertEquals("{x=[a, ts], y=[?d, c]}", dependenciesOf(dependencies));
        });
    }

    @Test
    public void testSubQueryJoinedWithoutKeyLosesColumnWhileViewCompiles() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v_altered AS (SELECT a FROM x)");
            drainWalAndViewQueues();
            // y loses d as the statement compiles the SELECT again with every sub-query it joins without a key
            // kept whole, after the first compile read d. The statement fails as it does when it runs after y
            // lost d, pointing at d in the statement.
            final String viewSql = "SELECT x.ts, x.a, p.mx FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y) p";
            final AlteringSecurityContext securityContext = new AlteringSecurityContext();
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(securityContext, bindVariableService, null, -1, null)) {
                for (String statement : new String[]{"CREATE VIEW v", "ALTER VIEW v_altered"}) {
                    final String sql = statement + " AS (" + viewSql + ")";
                    // the second check of y is the one of the second compile, see testViewCompiledAgainForReadsPlannedOtherwise()
                    securityContext.alterAtCheck(1, "ALTER TABLE y DROP COLUMN d");
                    final String failure = failureOf(sql, context);
                    Assert.assertFalse(statement, securityContext.isAlterPending());
                    Assert.assertEquals(statement, failureOf(sql, sqlExecutionContext), failure);
                    if (statement.startsWith("CREATE")) {
                        Assert.assertEquals(sql.indexOf("min(d)") + 4 + ": Invalid column: d", failure);
                    }
                    Assert.assertNull(getViewDefinition("v"));
                    Assert.assertEquals("{x=[a]}", dependenciesOf("v_altered"));
                    execute("ALTER TABLE y ADD COLUMN d LONG");
                    drainWalAndViewQueues();
                }
            }
        });
    }

    @Test
    public void testSubQueryJoinedWithoutKeyOfTableReplacedByOneOfSameVersion() throws Exception {
        assertMemoryLeak(() -> {
            createTablesWithTwoTimestamps();
            // x2 takes the name of x as the second compile checks whether y is visible, after both compiles read the
            // columns of x. x2 has the metadata version of x, so only its id tells the plan of the first compile, which
            // reads ts as the timestamp the ASOF JOIN requires, that x changed under it: the statement compiles the
            // SELECT again, and the view lists no column only the plan before the replacement read.
            final String viewSql = "DECLARE OVERRIDABLE @c := a SELECT t.v, p.c FROM (SELECT k, @c AS v FROM x) t ASOF JOIN (SELECT * FROM y) p";
            final AlteringSecurityContext securityContext = new AlteringSecurityContext();
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(securityContext, bindVariableService, null, -1, null)) {
                for (String statement : new String[]{"CREATE VIEW", "ALTER VIEW"}) {
                    if (statement.equals("ALTER VIEW")) {
                        execute("CREATE VIEW v AS (SELECT x FROM long_sequence(1))");
                        drainWalAndViewQueues();
                    }
                    Assert.assertEquals(statement, metadataVersionOf("x"), metadataVersionOf("x2"));
                    securityContext.alterAtCheck(1, "RENAME TABLE x TO x_replaced", "RENAME TABLE x2 TO x");
                    execute(statement + " v AS (" + viewSql + ")", context);
                    drainWalAndViewQueues();
                    Assert.assertFalse(statement, securityContext.isAlterPending());
                    // two compiles, twice
                    Assert.assertEquals(statement, 4, securityContext.takeCheckCount());
                    Assert.assertEquals(statement, "{x=[?k, a, ts2], y=[?*, c, ts]}", dependenciesOf("v"));
                    try (SqlExecutionContext denyContext = denyBaseTableSelectContext()) {
                        assertQuery("DECLARE @c := ts SELECT * FROM v")
                                .withContext(denyContext)
                                .noLeakCheck()
                                .failsWith("permission denied [table=x, columns=ts]");
                    }
                    // the next statement starts with x as it was, and x2 of the same metadata version
                    execute("DROP VIEW v");
                    execute("RENAME TABLE x TO x2");
                    execute("RENAME TABLE x_replaced TO x");
                    drainWalAndViewQueues();
                }
            }
        });
    }

    @Test
    public void testSubQueryJoinedWithoutKeyOfTableReplacedWhileViewCompiles() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The statement compiles the SELECT again with every sub-query it joins without a key kept whole, and
            // x2 takes the name of x as that compile checks whether y is visible, after both compiles read the
            // columns of x, and the first one planned a read of ts, the timestamp of x the ASOF JOIN requires.
            // Generating the first plan then finds x out of date, and the statement compiles both again, so the
            // view lists no column of x only the plans before the replacement read, and a reader that overrides
            // @c does not read ts.
            final String viewSql = "DECLARE OVERRIDABLE @c := a SELECT t.v, p.c FROM (SELECT k, @c AS v FROM x) t ASOF JOIN (SELECT * FROM y) p";
            final AlteringSecurityContext securityContext = new AlteringSecurityContext();
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(securityContext, bindVariableService, null, -1, null)) {
                for (String statement : new String[]{"CREATE VIEW", "ALTER VIEW"}) {
                    // The table that takes the name of x: its designated timestamp is ts2, and ts is another
                    // column. It has a column x does not, so its metadata version differs from that of x, and
                    // a plan of x finds it out of date.
                    execute("CREATE TABLE x2 (ts TIMESTAMP, ts2 TIMESTAMP, k SYMBOL, a LONG, b LONG) TIMESTAMP(ts2) PARTITION BY DAY WAL");
                    execute("ALTER TABLE x2 ADD COLUMN e INT");
                    execute("INSERT INTO x2 VALUES (10, 0, 'k1', 1, 2, 3), (3_600_000_010, 3_600_000_000, 'k1', 3, 4, 5)");
                    drainWalQueue();
                    if (statement.equals("ALTER VIEW")) {
                        execute("CREATE VIEW v AS (SELECT x FROM long_sequence(1))");
                        drainWalAndViewQueues();
                    }
                    // the second check of y is the one of the second compile, see testViewCompiledAgainForReadsPlannedOtherwise()
                    securityContext.alterAtCheck(1, "RENAME TABLE x TO x_replaced", "RENAME TABLE x2 TO x");
                    execute(statement + " v AS (" + viewSql + ")", context);
                    drainWalAndViewQueues();
                    Assert.assertFalse(statement, securityContext.isAlterPending());
                    Assert.assertEquals(statement, "{x=[?k, a, ts2], y=[?*, c, ts]}", dependenciesOf("v"));
                    try (SqlExecutionContext denyContext = denyBaseTableSelectContext()) {
                        assertQuery("DECLARE @c := ts SELECT * FROM v")
                                .withContext(denyContext)
                                .noLeakCheck()
                                .failsWith("permission denied [table=x, columns=ts]");
                    }
                    // the next statement starts with x as it was
                    execute("DROP VIEW v");
                    execute("DROP TABLE x");
                    execute("RENAME TABLE x_replaced TO x");
                    drainWalAndViewQueues();
                }
            }
        });
    }

    @Test
    public void testSubQueryJoinedWithoutKeyOfTableSwappedBackBeforeThePlan() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // the table that takes the name of x for a while: its designated timestamp is b, a column of x the view
            // does not read
            execute("CREATE TABLE x2 (b TIMESTAMP, k SYMBOL, a LONG) TIMESTAMP(b) PARTITION BY DAY WAL");
            execute("INSERT INTO x2 VALUES (0, 'k1', 1), (3_600_000_000, 'k1', 3)");
            drainWalQueue();
            // x2 takes the name of x as the first compile checks whether y is visible, after it read the columns of
            // x, and gives the name back as the second compile checks y, after it read the columns of x2, with b as
            // the timestamp the ASOF JOIN requires. The plan of the first compile scans the x it was optimised on,
            // so only the id of the table the second compile scanned tells that the compiles saw different tables:
            // the statement compiles both again, the view lists no column only x2 read, and a reader that
            // overrides @c does not read b.
            final String viewSql = "DECLARE OVERRIDABLE @c := a SELECT t.v, p.c FROM (SELECT k, @c AS v FROM x) t ASOF JOIN (SELECT * FROM y) p";
            final AlteringSecurityContext securityContext = new AlteringSecurityContext();
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(securityContext, bindVariableService, null, -1, null)) {
                for (String statement : new String[]{"CREATE VIEW", "ALTER VIEW"}) {
                    if (statement.equals("ALTER VIEW")) {
                        execute("CREATE VIEW v AS (SELECT x FROM long_sequence(1))");
                        drainWalAndViewQueues();
                    }
                    securityContext.alterAtCheck(0, "RENAME TABLE x TO x_held", "RENAME TABLE x2 TO x");
                    securityContext.alterAtCheck(1, "RENAME TABLE x TO x2", "RENAME TABLE x_held TO x");
                    execute(statement + " v AS (" + viewSql + ")", context);
                    drainWalAndViewQueues();
                    Assert.assertFalse(statement, securityContext.isAlterPending());
                    // two compiles, twice
                    Assert.assertEquals(statement, 4, securityContext.takeCheckCount());
                    Assert.assertEquals(statement, "{x=[?k, a, ts], y=[?*, c, ts]}", dependenciesOf("v"));
                    try (SqlExecutionContext denyContext = denyBaseTableSelectContext()) {
                        assertQuery("DECLARE @c := b SELECT * FROM v")
                                .withContext(denyContext)
                                .noLeakCheck()
                                .failsWith("permission denied [table=x, columns=b]");
                    }
                    execute("DROP VIEW v");
                    drainWalAndViewQueues();
                }
            }
        });
    }

    @Test
    public void testSubQueryJoinedWithoutKeyOfViewAlteredWhileViewCompiles() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW vv AS (SELECT ts, k, c, d FROM y)");
            drainWalAndViewQueues();
            // vv changes from the time the first compile checks whether x is visible, after it parsed the old
            // definition of vv, and before the second compile parses the SELECT. The second compile finds vv out of
            // date as it parses the SELECT, so the statement compiles both again: the view lists what vv reads now.
            // In the first change vv reads c of y as d too, so the view no longer lists d. In the second one vv
            // gains a PIVOT IN sub-query, whose values the second compile would otherwise take from a first compile
            // that never ran it.
            final String viewSql = "SELECT x.a, p.mx FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM vv) p";
            final String[][] changes = {
                    {"SELECT ts, k, c, c d FROM y", "{vv=[], x=[a], y=[c]}"},
                    {
                            "SELECT y.ts, y.k, y.c, y.d FROM y CROSS JOIN ((SELECT k, c FROM y) PIVOT (sum(c) FOR k IN (SELECT DISTINCT k FROM y))) pv",
                            "{vv=[], x=[a], y=[?d, c, k]}"
                    }
            };
            final AlteringSecurityContext securityContext = new AlteringSecurityContext("x");
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(securityContext, bindVariableService, null, -1, null)) {
                for (String[] change : changes) {
                    for (String statement : new String[]{"CREATE VIEW", "ALTER VIEW"}) {
                        final String message = statement + ", vv: " + change[0];
                        if (statement.equals("ALTER VIEW")) {
                            execute("CREATE VIEW v AS (SELECT x FROM long_sequence(1))");
                            drainWalAndViewQueues();
                        }
                        securityContext.alterAtCheck(0, "ALTER VIEW vv AS (" + change[0] + ")");
                        execute(statement + " v AS (" + viewSql + ")", context);
                        drainWalAndViewQueues();
                        Assert.assertFalse(message, securityContext.isAlterPending());
                        // the first compile, then both compiles again
                        Assert.assertEquals(message, 3, securityContext.takeCheckCount());
                        Assert.assertEquals(message, change[1], dependenciesOf("v"));
                        assertQuery("SELECT * FROM v")
                                .noLeakCheck()
                                .inferTimestamp()
                                .inferRandomAccess()
                                .sizeMayVary()
                                .returns("""
                                        a\tmx
                                        1\t7
                                        3\t7
                                        """);
                        // the next statement starts with vv as it was
                        execute("DROP VIEW v");
                        execute("ALTER VIEW vv AS (SELECT ts, k, c, d FROM y)");
                        drainWalAndViewQueues();
                    }
                }
            }
        });
    }

    @Test
    public void testSubQueryJoinedWithoutKeyReadsColumnAddedLater() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v_cross AS (SELECT x.ts, x.a, p.c FROM x CROSS JOIN (SELECT * FROM y LIMIT 1) p)");
            execute("CREATE VIEW v_asof AS (SELECT x.ts, x.a, q.c FROM x ASOF JOIN (SELECT * FROM y) q)");
            execute("CREATE VIEW v_altered AS (SELECT a FROM x)");
            drainWalAndViewQueues();
            execute("ALTER VIEW v_altered AS (SELECT x.ts, x.a, p.c FROM x CROSS JOIN (SELECT * FROM y LIMIT 1) p)");
            drainWalAndViewQueues();

            // A read that names none of the columns of the sub-query reads the column y gains. The views differ
            // in size and random access, which the read infers.
            execute("ALTER TABLE y ADD COLUMN e INT");
            execute("INSERT INTO y VALUES (7_200_000_000, 'k2', 9, 10, 42)");
            drainWalAndViewQueues();
            try (SqlExecutionContext context = denyBaseTableSelectContext()) {
                for (String viewName : new String[]{"v_cross", "v_asof", "v_altered"}) {
                    assertQuery("SELECT ts, a FROM " + viewName)
                            .withContext(context)
                            .noLeakCheck()
                            .timestamp("ts")
                            .inferRandomAccess()
                            .sizeMayVary()
                            .returns("""
                                    ts\ta
                                    1970-01-01T00:00:00.000000Z\t1
                                    1970-01-01T01:00:00.000000Z\t3
                                    """);
                }
            }
        });
    }

    @Test
    public void testSubQueryJoinedWithoutKeyWithPivotInSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE z (ts TIMESTAMP, zk SYMBOL, zd LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO z VALUES (0, 'k1', 6), (3_600_000_000, 'k1', 8)");
            drainWalQueue();
            final String expectedRows = """
                    a\tk1
                    1\t12
                    3\t12
                    """;
            // The second compile takes the values the PIVOT IN sub-query returned in the first one rather than
            // running it again, so each statement reads z once.
            final String viewSql = "SELECT x.a, pv.* FROM x CROSS JOIN ((SELECT k, c FROM y) PIVOT (sum(c) FOR k IN (SELECT zk FROM z))) pv";
            final ReadCountingSecurityContext securityContext = new ReadCountingSecurityContext("z");
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(securityContext, bindVariableService, null, -1, null)) {
                forEachViewStatement(viewSql, context, statement -> {
                    // the plan of the view reads the values, not z
                    Assert.assertEquals(statement, "{x=[a], y=[c, k]}", dependenciesOf("v"));
                    Assert.assertEquals(statement, 1, securityContext.takeReadCount());
                    assertQuery("SELECT * FROM v")
                            .noLeakCheck()
                            .inferTimestamp()
                            .inferRandomAccess()
                            .sizeMayVary()
                            .returns(expectedRows);
                });
            }
            // With a PIVOT IN sub-query for each FOR column, and a SELECT that names the columns their values make,
            // which the second compile has to make too. A filter the plan of a sub-query compiles to native code
            // reads the table as the plan compiles, see SqlCodeGenerator.generateFilter0(), so these have none.
            final String viewSql2 = "SELECT x.a, pv.k1_6, pv.k1_8 FROM x CROSS JOIN ((SELECT k, c, d FROM y) PIVOT (sum(c) FOR k IN (SELECT zk FROM z) d IN (SELECT zd FROM z))) pv";
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(securityContext, bindVariableService, null, -1, null)) {
                forEachViewStatement(viewSql2, context, statement -> {
                    Assert.assertEquals(statement, "{x=[a], y=[c, d, k]}", dependenciesOf("v"));
                    Assert.assertEquals(statement, 2, securityContext.takeReadCount());
                    assertQuery("SELECT * FROM v")
                            .noLeakCheck()
                            .inferTimestamp()
                            .inferRandomAccess()
                            .sizeMayVary()
                            .returns("""
                                    a\tk1_6\tk1_8
                                    1\t5\t7
                                    3\t5\t7
                                    """);
                });
            }
            // A PIVOT IN sub-query that joins a sub-query without a key compiles as any query does.
            forEachViewStatement(
                    "SELECT x.a, pv.* FROM x CROSS JOIN ((SELECT k, c FROM y) PIVOT (sum(c) FOR k IN (SELECT p.k FROM x xx CROSS JOIN (SELECT k, c FROM y) p WHERE p.c > xx.a))) pv",
                    statement -> {
                        Assert.assertEquals(statement, "{x=[a], y=[c, k]}", dependenciesOf("v"));
                        assertQuery("SELECT * FROM v")
                                .noLeakCheck()
                                .inferTimestamp()
                                .inferRandomAccess()
                                .sizeMayVary()
                                .returns(expectedRows);
                    }
            );
        });
    }

    @Test
    public void testSubQueryKeptWholeByARead() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW vx AS (SELECT * FROM x)");
            drainWalAndViewQueues();
            // A read that names only outputs that read no column of the sub-query the FROM clause selects from alone,
            // such as count(), gives the sub-query no column, and its plan computes every column of its SELECT list:
            // the view reads those, and every column of a table a wildcard it keeps whole selects.
            assertReads("SELECT count() n, sum(a) s FROM (SELECT * FROM x)", "{x=[?*, a]}", "SELECT n FROM v", "SELECT s FROM v");
            assertReads("SELECT count(*) n, sum(a) s FROM (SELECT * FROM x)", "{x=[?*, a]}", "SELECT n FROM v");
            assertReads("SELECT count() n, sum(a) s FROM (SELECT a, b FROM x)", "{x=[?b, a]}", "SELECT n FROM v", "SELECT s FROM v");
            assertReads("SELECT 1 one, a FROM (SELECT * FROM x)", "{x=[?*, a]}", "SELECT one FROM v", "SELECT a FROM v");
            assertReads("SELECT count() n, sum(a) s FROM vx", "{vx=[], x=[?*, a]}", "SELECT n FROM v");
            assertReads("WITH c AS (SELECT a, b FROM x) SELECT count() n, sum(a) s FROM c", "{x=[?b, a]}", "SELECT n FROM v");
            // through a SELECT whose every column a read of the view can name
            assertReads("SELECT n, s FROM (SELECT count() n, sum(a) s FROM (SELECT * FROM x))", "{x=[?*, a]}", "SELECT n FROM v");
            // The optimiser puts a SELECT of the columns the projection reads under a projection that computes an
            // expression. A read that names only the constant keeps that SELECT whole, and so gives the sub-query mx.
            assertReads("SELECT 1 one, p.mx FROM (SELECT max(c) mx, min(d) mn FROM y) p CROSS JOIN x", "{x=[], y=[c]}", "SELECT one FROM v", "SELECT mx FROM v");
            // a window function that reads no column, and a constant next to the timestamp of the view, which a read
            // that names neither needs
            assertReads("SELECT a, count() OVER () c FROM (SELECT * FROM x)", "{x=[?*, a]}", "SELECT c FROM v", "SELECT a FROM v");
            assertReads("SELECT ts, 1 one, a FROM (SELECT * FROM x)", "{x=[?*, a, ts]}", "SELECT one FROM v");
            // A key, a column every output reads, and the window of a window function, read the sub-query in every read.
            assertReads("SELECT k, count() n, sum(a) s FROM (SELECT * FROM x)", "{x=[a, k]}", "SELECT n FROM v");
            assertReads("SELECT a, b FROM (SELECT * FROM x)", "{x=[a, b]}", "SELECT a FROM v");
            assertReads(
                    "SELECT k, row_number() OVER (PARTITION BY k) rn, rank() OVER (ORDER BY a) r FROM (SELECT * FROM x)",
                    "{x=[a, k]}",
                    "SELECT rn FROM v", "SELECT r FROM v"
            );
            // A read that names only an output that reads a SELECT of the view and not the sub-query the SELECT joins
            // without a key leaves the sub-query without columns.
            assertReads(
                    "SELECT s.c1 o1, s.c2 o2, 1 o3 FROM (SELECT x.a + p.mx c1, x.b c2 FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y) p) s",
                    "{x=[a, b], y=[?d, c]}",
                    "SELECT o2 FROM v", "SELECT o3 FROM v"
            );
            // A read that names only n keeps the SELECT whole, u included, which gives the sub-query mn, and one that
            // names only t2 gives the SELECT c2 alone, and keeps the sub-query whole, fk included.
            assertReads(
                    "SELECT count() n, sum(s.c1) t1, sum(s.c2) t2 FROM (SELECT x.a + p.mx c1, x.b c2, p.mn u FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn, first(k) fk FROM y) p) s",
                    "{x=[a, b], y=[?d, ?k, c]}",
                    "SELECT n FROM v", "SELECT t1 FROM v", "SELECT t2 FROM v"
            );
            // A read that names only outputs that read no column of that SELECT keeps the SELECT whole, and so gives
            // the sub-query the columns its SELECT list reads: no read keeps the sub-query whole, and none reads d.
            assertReads(
                    "SELECT s.o1, 1 o2 FROM (SELECT x.a + p.mx o1 FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y) p) s",
                    "{x=[a], y=[c]}",
                    "SELECT o2 FROM v", "SELECT o1 FROM v"
            );
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @m := c SELECT s.o1, 1 o2 FROM (SELECT x.a + p.mx o1 FROM x CROSS JOIN (SELECT max(@m) mx, min(d) mn FROM y) p) s",
                    "DECLARE @m := d SELECT * FROM v",
                    "permission denied [table=y, columns=d]"
            );
            // A read that names only t computes m1, which m2 reads, so every read gives the SELECT * the column v, and
            // no read keeps the SELECT * whole: the view reads a of x, and b for u, which a read that names only n
            // computes as it keeps the SELECT of m1 whole.
            assertReads(
                    "SELECT count() n, sum(m2) t FROM (SELECT v + 1 m1, m1 * 2 m2, w u FROM (SELECT * FROM (SELECT a v, b w, ts, k FROM x) CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y)))",
                    "{x=[?b, a], y=[c, d]}",
                    "SELECT n FROM v", "SELECT t FROM v"
            );
            // and a read that names only t gives no column to the sub-query only u reads
            assertReads(
                    "SELECT count() n, sum(m2) t, sum(u) tu FROM (SELECT x.a + 1 m1, m1 * 2 m2, p.mx u FROM x CROSS JOIN (SELECT max(c) mx, min(d) mn FROM y) p)",
                    "{x=[a], y=[?d, c]}",
                    "SELECT n FROM v", "SELECT t FROM v", "SELECT tu FROM v"
            );
        });
    }

    @Test
    public void testSubQueryKeptWholeByReadOfOutputThatSharesColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A read that names only s gives the SELECT under the projection a alone, which leaves q without a column
            // and keeps it whole. The outputs t and s both read a, and t reads b too, which only q gives: s does not
            // need b, whatever the column a that t and s share serves.
            assertReads(
                    "SELECT t, s, 1 one FROM (SELECT a + b t, a + a s FROM (SELECT p.a, q.b FROM (SELECT * FROM x) p CROSS JOIN (SELECT * FROM x) q))",
                    "{x=[?*, a, b]}",
                    "SELECT s FROM v", "SELECT t FROM v", "SELECT one FROM v"
            );
        });
    }

    @Test
    public void testSubQueryKeptWholeByReadsOfManyOutputs() throws Exception {
        assertMemoryLeak(() -> {
            for (int width : new int[]{100, 400}) {
                final StringSink ddl = new StringSink();
                ddl.put("CREATE TABLE w").put(width).put(" (ts TIMESTAMP");
                for (int i = 1; i <= width; i++) {
                    ddl.put(", c").put(i).put(" LONG");
                }
                ddl.put(") TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute(ddl);
            }
            drainWalQueue();
            // A read that names one of the constant outputs alone keeps the sub-query whole, so the second compile gives
            // every column of the sub-query, at each level of it, the outputs such reads name. The columns share those
            // outputs, so its work grows with the columns plus the outputs, not with their product.
            for (String subQuery : new String[]{"(SELECT * FROM %s)", "(SELECT * FROM (SELECT * FROM (SELECT * FROM %s)))"}) {
                final StringSink viewSql = new StringSink();
                for (int i = 1; i <= 200; i++) {
                    viewSql.put(i == 1 ? "SELECT " : ", ").put(i).put(" o").put(i);
                }
                viewSql.put(", c1 FROM ").put(subQuery);
                final long work100 = keptWholeWork(String.format(viewSql.toString(), "w100"), "{w100=[?*, c1]}");
                final long work400 = keptWholeWork(String.format(viewSql.toString(), "w400"), "{w400=[?*, c1]}");
                Assert.assertTrue(subQuery + ": " + work400 + " outputs added with 400 columns, " + work100 + " with 100", work400 < 3 * work100);
            }
            // Each output that reads the same column adds itself to the outputs of the column, so the work grows with
            // the outputs, not with their square.
            final long work100 = keptWholeWork(sameColumnOutputs(100), "{w100=[?*, c1]}");
            final long work400 = keptWholeWork(sameColumnOutputs(400), "{w100=[?*, c1]}");
            Assert.assertTrue(work400 + " outputs added with 400 outputs, " + work100 + " with 100", work400 < 8 * work100);
        });
    }

    @Test
    public void testTableReplacedByOneOfSameVersionWhileViewCompiles() throws Exception {
        assertMemoryLeak(() -> {
            createTablesWithTwoTimestamps();
            // The SELECT scans x twice, and x2, of the metadata version of x, takes the name of x as the compile checks
            // whether y is visible, between the two scans. The first scan reads ts, the timestamp of x the ASOF JOIN
            // requires, and its id tells the plan that x changed under it, so the statement compiles the SELECT again,
            // where both scans read x2: the view lists no column only the old x had as its timestamp.
            final String viewSql = """
                    DECLARE OVERRIDABLE @c := a
                    SELECT t.v, u.c, w.b FROM (SELECT k, @c AS v FROM x) t ASOF JOIN y u ON (k) ASOF JOIN (SELECT k, b FROM x) w ON (k)
                    """;
            final AlteringSecurityContext securityContext = new AlteringSecurityContext();
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(securityContext, bindVariableService, null, -1, null)) {
                for (String statement : new String[]{"CREATE VIEW", "ALTER VIEW"}) {
                    if (statement.equals("ALTER VIEW")) {
                        execute("CREATE VIEW v AS (SELECT x FROM long_sequence(1))");
                        drainWalAndViewQueues();
                    }
                    Assert.assertEquals(statement, metadataVersionOf("x"), metadataVersionOf("x2"));
                    securityContext.alterAtCheck(0, "RENAME TABLE x TO x_replaced", "RENAME TABLE x2 TO x");
                    execute(statement + " v AS (" + viewSql + ")", context);
                    drainWalAndViewQueues();
                    Assert.assertFalse(statement, securityContext.isAlterPending());
                    // one compile, twice
                    Assert.assertEquals(statement, 2, securityContext.takeCheckCount());
                    Assert.assertEquals(statement, "{x=[a, b, k, ts2], y=[c, k, ts]}", dependenciesOf("v"));
                    try (SqlExecutionContext denyContext = denyBaseTableSelectContext()) {
                        assertQuery("DECLARE @c := ts SELECT * FROM v")
                                .withContext(denyContext)
                                .noLeakCheck()
                                .failsWith("permission denied [table=x, columns=ts]");
                    }
                    execute("DROP VIEW v");
                    execute("RENAME TABLE x TO x2");
                    execute("RENAME TABLE x_replaced TO x");
                    drainWalAndViewQueues();
                }
            }
        });
    }

    @Test
    public void testTableReplacedWhileViewCompiles() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // the table that takes the name of x: its designated timestamp is ts2, and ts is a plain column
            execute("CREATE TABLE x2 (ts LONG, ts2 TIMESTAMP, k SYMBOL, a LONG, b LONG) TIMESTAMP(ts2) PARTITION BY DAY WAL");
            execute("INSERT INTO x2 VALUES (1, 0, 'k1', 1, 2), (2, 3_600_000_000, 'k1', 3, 4)");
            drainWalQueue();
            // CREATE VIEW plans a read of ts, the timestamp of x the ASOF JOIN requires, and x2 takes the name
            // of x before the plan opens it. The statement compiles the SELECT again, into a plan that reads
            // ts2 instead, and the view lists no column only the first plan reads, so a reader that overrides
            // @c does not read ts.
            final String viewSql = "DECLARE OVERRIDABLE @c := a SELECT t.v, u.c FROM (SELECT k, @c AS v FROM x) t ASOF JOIN y u ON (k)";
            final TableReplacingSecurityContext securityContext = new TableReplacingSecurityContext();
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(securityContext, bindVariableService, null, -1, null)) {
                securityContext.replaceTableOnce();
                execute("CREATE VIEW v AS (" + viewSql + ")", context);
                drainWalAndViewQueues();
                Assert.assertFalse(securityContext.isReplacementPending());
            }
            assertViewDependencies(viewSql, "{x=[a, k, ts2], y=[c, k, ts]}", null, "CREATE VIEW");
            try (SqlExecutionContext context = denyBaseTableSelectContext()) {
                assertQuery("DECLARE @c := ts SELECT * FROM v")
                        .withContext(context)
                        .noLeakCheck()
                        .failsWith("permission denied [table=x, columns=ts]");
            }
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
            // a sub-query on either side of a temporal join reads the timestamp its SELECT does not name
            assertDependencies("SELECT t.a, u.c FROM x t ASOF JOIN (SELECT k, c FROM y) u ON (k)", "{x=[a, k, ts], y=[c, k, ts]}");
            assertDependencies("SELECT t.k, t.a FROM x t SPLICE JOIN (SELECT k, c FROM y) u ON (k)", "{x=[a, k, ts], y=[k, ts]}");
            assertDependencies("SELECT t.a, u.c FROM (SELECT k, a FROM x) t ASOF JOIN y u ON (k)", "{x=[a, k, ts], y=[c, k, ts]}");
            // The first table in join order provides the timestamp of a temporal join after an inner one, and
            // the table joined to it by the inner join reads none, whether the temporal join comes after it
            // or before it.
            execute("CREATE TABLE z (ts TIMESTAMP, k SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO z VALUES (0, 'k1', 9), (3_600_000_000, 'k1', 10)");
            drainWalQueue();
            assertDependencies(
                    "SELECT t.a, u.c, w.v FROM x t JOIN y u ON (k) ASOF JOIN z w ON (k)",
                    "{x=[a, k, ts], y=[c, k], z=[k, ts, v]}"
            );
            assertDependencies(
                    "SELECT t.a, w.v, u.c FROM x t ASOF JOIN z w ON (k) JOIN y u ON (k)",
                    "{x=[a, k, ts], y=[c, k], z=[k, ts, v]}"
            );
        });
    }

    @Test
    public void testUnionWithLimitReadsColumnAddedLater() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, a LONG, b LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE y (ts TIMESTAMP, a LONG, b LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES (0, 7, 11)");
            execute("INSERT INTO y VALUES (0, 7, 13)");
            drainWalQueue();
            execute("CREATE VIEW v_named AS (SELECT a FROM (SELECT ts, a, b FROM x UNION SELECT ts, a, b FROM y LIMIT 1))");
            drainWalAndViewQueues();
            // Every input has a=7, so the single result does not depend on UNION's iteration order.
            final String expected = "a\n7\n";
            forEachViewStatement("SELECT a FROM (SELECT * FROM x UNION SELECT * FROM y LIMIT 1)", statement -> {
                try (SqlExecutionContext context = denyBaseTableSelectContext()) {
                    assertQuery("SELECT a FROM x").withContext(context).noLeakCheck()
                            .failsWith("permission denied [table=x, columns=a]");
                    assertQuery("SELECT * FROM v").withContext(context).noLeakCheck().noRandomAccess().returns(expected);
                    final int columnCount;
                    try (TableReader reader = engine.getReader("x")) {
                        columnCount = reader.getMetadata().getColumnCount();
                    }
                    final String column = "extra" + columnCount;
                    // Both UNION branches must grow compatibly. Existing rows get NULL in the new column.
                    execute("ALTER TABLE x ADD COLUMN " + column + " INT");
                    execute("ALTER TABLE y ADD COLUMN " + column + " INT");
                    execute("INSERT INTO x (ts, a, b, " + column + ") VALUES (" + columnCount + ", 7, 17, 19)");
                    execute("INSERT INTO y (ts, a, b, " + column + ") VALUES (" + columnCount + ", 7, 23, 29)");
                    drainWalAndViewQueues();
                    assertQuery("SELECT * FROM v").noLeakCheck().noRandomAccess().returns(expected);
                    assertQuery("SELECT * FROM v_named").withContext(context).noLeakCheck().noRandomAccess().returns(expected);
                    assertQuery("SELECT * FROM v").withContext(context).noLeakCheck().noRandomAccess().returns(expected);
                    Assert.assertTrue(statement, getViewDefinition("v").getDependencies().get("x").contains("*"));
                    Assert.assertTrue(statement, getViewDefinition("v").getDependencies().get("y").contains("*"));
                    Assert.assertFalse(getViewDefinition("v_named").getDependencies().get("x").contains("*"));
                    Assert.assertFalse(getViewDefinition("v_named").getDependencies().get("y").contains("*"));
                }
            });
        });
    }

    @Test
    public void testUnnarrowedNestedWildcard() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The optimiser keeps every column of a sub-query whose columns the query above it names none of,
            // and of the left side of a LATERAL join, so the view reads every column the wildcard in such a
            // sub-query selects, including one the table gains later.
            assertDependencies("SELECT count() FROM (SELECT * FROM x)", "{x=[*]}");
            assertDependencies("SELECT count() FROM (SELECT * FROM (SELECT * FROM x))", "{x=[*]}");
            assertDependencies("SELECT count() FROM (SELECT * FROM x) WHERE a > 0", "{x=[*]}");
            assertDependencies("SELECT 1 AS one FROM (SELECT * FROM x)", "{x=[*]}");
            assertDependencies("SELECT count() FROM (SELECT *, a + 1 AS a1 FROM x)", "{x=[*]}");
            assertDependencies("SELECT count() FROM (SELECT x.* FROM x JOIN y ON (k))", "{x=[*], y=[k]}");
            assertDependencies("WITH c AS (SELECT * FROM x) SELECT count() FROM c", "{x=[*]}");
            assertDependencies(
                    "SELECT count() FROM (SELECT * FROM x UNION ALL SELECT * FROM y)",
                    "{x=[*], y=[*]}"
            );
            assertDependencies("SELECT y.c FROM y CROSS JOIN (SELECT * FROM x)", "{x=[*], y=[c]}");
            assertDependencies(
                    "SELECT * FROM (SELECT count() FROM (SELECT * FROM x)) PIVOT (sum(count) FOR count IN (1))",
                    "{x=[*]}"
            );
            assertDependencies(
                    "SELECT t.a, l.c FROM (SELECT * FROM x) t CROSS JOIN LATERAL (SELECT c, k FROM y u WHERE u.k = t.k LIMIT 1) l",
                    "{x=[*], y=[c, k]}"
            );
            execute("CREATE VIEW v_star AS (SELECT * FROM x)");
            drainWalAndViewQueues();
            assertDependencies("SELECT count() FROM v_star", "{v_star=[], x=[*]}");

            // a SELECT list that names its columns, every one of them included, reads no column added later
            assertDependencies("SELECT count() FROM (SELECT ts, k, a, b FROM (SELECT * FROM x))", "{x=[a, b, k, ts]}");
            assertDependencies("SELECT count() FROM (SELECT a, b FROM x)", "{x=[a, b]}");
        });
    }

    @Test
    public void testUnnarrowedNestedWildcardOfSelfJoinedTableAlteredBetweenItsScans() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The sub-query scans x twice, and its wildcard selects the first scan only. x gains a column after
            // the optimiser has read the columns of that scan and before it reads those of x2. The attempt checks
            // its "*" against the columns of the scan the wildcard selects, which the plan reads, and not against
            // the columns x has by then; code generation then finds the plan out of date, and the statement
            // compiles the SELECT again on the new metadata.
            assertWildcardKeptWhileTableAltered(
                    "SELECT count() FROM (SELECT x.* FROM x CROSS JOIN y CROSS JOIN x x2)",
                    """
                            count
                            8
                            """
            );
        });
    }

    @Test
    public void testUnnarrowedNestedWildcardOfTableAlteredBetweenItsScans() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The view scans x twice. x gains a column after the optimiser has read the columns of the scan
            // under the wildcard and before it reads those of the other scan, which reads none of the added
            // column. The attempt checks its "*" against the columns of the scan under the wildcard, which the
            // plan reads, and not against the columns x has by then; code generation then finds the plan out of
            // date, and the statement compiles the SELECT again on the new metadata.
            assertWildcardKeptWhileTableAltered(
                    "SELECT count() FROM (SELECT * FROM x) CROSS JOIN y CROSS JOIN (SELECT a FROM x)",
                    """
                            count
                            8
                            """
            );
        });
    }

    @Test
    public void testUnnarrowedNestedWildcardOfTableAlteredWhileViewCompiles() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // x gains a column after the optimiser has read its columns and before the statement records the
            // dependencies, as it does when an ALTER TABLE commits while the view compiles. The plan, built
            // again on the new metadata, reads the added column, and the "*" the view records for x covers it.
            assertWildcardKeptWhileTableAltered(
                    "SELECT count() FROM (SELECT * FROM x) CROSS JOIN y",
                    """
                            count
                            4
                            """
            );
        });
    }

    @Test
    public void testUnnarrowedNestedWildcardReadsColumnAddedLater() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v_star AS (SELECT * FROM x)");
            execute("CREATE VIEW v_count AS (SELECT count() FROM (SELECT * FROM x) WHERE a > 1)");
            execute("CREATE VIEW v_cross AS (SELECT y.c FROM y CROSS JOIN (SELECT * FROM x))");
            execute("""
                    CREATE VIEW v_lateral AS (
                        SELECT t.a, l.c
                        FROM (SELECT * FROM x) t
                        CROSS JOIN LATERAL (SELECT c, k FROM y u WHERE u.k = t.k LIMIT 1) l
                    )
                    """);
            execute("CREATE VIEW v_altered AS (SELECT a FROM x)");
            drainWalAndViewQueues();
            execute("ALTER VIEW v_altered AS (SELECT count() FROM (SELECT * FROM x) WHERE a > 1)");
            execute("CREATE VIEW v_outer AS (SELECT count() FROM v_star)");
            drainWalAndViewQueues();

            // each view reads the column x gains, and a principal with SELECT on the view only still reads it
            execute("ALTER TABLE x ADD COLUMN e INT");
            execute("INSERT INTO x VALUES (7_200_000_000, 'k2', 5, 6, 42)");
            drainWalAndViewQueues();
            try (SqlExecutionContext context = denyBaseTableSelectContext()) {
                for (String viewName : new String[]{"v_count", "v_altered"}) {
                    assertQuery("SELECT * FROM " + viewName)
                            .withContext(context)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("""
                                    count
                                    2
                                    """);
                }
                assertQuery("SELECT * FROM v_outer")
                        .withContext(context)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count
                                3
                                """);
                assertQuery("SELECT * FROM v_cross ORDER BY c")
                        .withContext(context)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                c
                                5
                                5
                                5
                                7
                                7
                                7
                                """);
                assertQuery("SELECT * FROM v_lateral ORDER BY a")
                        .withContext(context)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                a\tc
                                1\t5
                                3\t5
                                """);
            }
        });
    }

    @Test
    public void testUnreachableWildcardHolder() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a LONG, b LONG)");
            try (SqlCompiler parsedCompiler = engine.getSqlCompiler(); SqlCompiler optimisedCompiler = engine.getSqlCompiler()) {
                final IQueryModel parsed = parsedCompiler.testParseExpression(
                        "(SELECT count() FROM (SELECT * FROM x))",
                        QueryModel.FACTORY.newInstance()
                ).queryModel;
                final LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = new LowerCaseCharSequenceObjHashMap<>();
                final SqlUtil.ViewWildcardSources wildcardSources = new SqlUtil.ViewWildcardSources();
                SqlUtil.collectTableAndColumnReferences(engine, parsed, dependencies, wildcardSources);
                Assert.assertEquals("{x=[]}", dependenciesOf(dependencies));

                // The recorded holder is outside this graph: it must not lend '*' just because it has no top-down columns.
                final IQueryModel optimised = optimisedCompiler.generateExecutionModel("SELECT a FROM x", sqlExecutionContext).getQueryModel();
                SqlUtil.collectTableColumnReads(engine, optimised, dependencies, wildcardSources, new SqlUtil.ViewCompileRecord(), true);
                Assert.assertEquals("{x=[a]}", dependenciesOf(dependencies));
            }
        });
    }

    @Test
    public void testViewCompiledAgainForReadsPlannedOtherwise() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v_altered AS (SELECT a FROM x)");
            drainWalAndViewQueues();
            // Each compile of the SELECT checks once whether y is visible. CREATE VIEW and ALTER VIEW compile it
            // again when a read of the view may plan it otherwise, and only then: when it joins a sub-query without a
            // key, but not when a join condition names a column of the sub-queries on both of its sides; when it
            // selects from a sub-query alone, the plan leaves a column of the sub-query out, and the SELECT has a
            // column that reads no column of the sub-query; and when a temporal join takes its timestamp from the
            // ORDER BY of a sub-query, which a read that counts the rows of the view drops.
            final AlteringSecurityContext securityContext = new AlteringSecurityContext();
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(securityContext, bindVariableService, null, -1, null)) {
                final String[] viewSqls = {
                        "SELECT c FROM (SELECT c, d FROM y)",
                        "SELECT x.a, y.c FROM x CROSS JOIN y",
                        "SELECT x.a, p.mx FROM x JOIN (SELECT k, max(c) mx FROM y GROUP BY k) p ON (k)",
                        "SELECT t.mb, p.mx FROM (SELECT k, max(b) mb FROM x GROUP BY k) t JOIN (SELECT k, max(c) mx FROM y GROUP BY k) p ON (k)",
                        "SELECT x.a, p.mx FROM x CROSS JOIN (SELECT max(c) mx FROM y) p",
                        "SELECT x.a, p.c FROM x ASOF JOIN (SELECT * FROM y) p",
                        "SELECT count() n, max(c) m FROM (SELECT c, d FROM y)",
                        "SELECT 1 one, c FROM (SELECT * FROM y)",
                        "SELECT max(c) m FROM (SELECT c, d FROM y)",
                        "SELECT count() n FROM (SELECT c, d FROM y)",
                        "SELECT count() n, max(c) m FROM y",
                        "SELECT t.a, u.c FROM x t ASOF JOIN (SELECT k, c, ts FROM y ORDER BY ts) u ON (k)",
                        "SELECT t.a, u.c FROM x t ASOF JOIN (SELECT k, c, ts FROM y) u ON (k)",
                };
                final int[] expectedCompiles = {1, 1, 1, 1, 2, 2, 2, 2, 1, 1, 1, 2, 1};
                for (int i = 0; i < viewSqls.length; i++) {
                    execute("CREATE VIEW v AS (" + viewSqls[i] + ")", context);
                    Assert.assertEquals(viewSqls[i], expectedCompiles[i], securityContext.takeCheckCount());
                    execute("ALTER VIEW v_altered AS (" + viewSqls[i] + ")", context);
                    Assert.assertEquals(viewSqls[i], expectedCompiles[i], securityContext.takeCheckCount());
                    execute("DROP VIEW v");
                    drainWalAndViewQueues();
                }
            }
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
    public void testWildcardHolderReachabilityAcrossCompilations() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                recordViewDependencies(
                        compiler,
                        "SELECT count() FROM (SELECT * FROM x) s CROSS JOIN (SELECT * FROM y) t",
                        "{x=[*], y=[*]}"
                );
                recordViewDependencies(compiler, "SELECT a FROM (SELECT * FROM x)", "{x=[a]}");
                recordViewDependencies(
                        compiler,
                        "SELECT count() FROM (SELECT y.* FROM x JOIN y ON (k))",
                        "{x=[k], y=[*]}"
                );
                recordViewDependencies(
                        compiler,
                        "SELECT count() FROM (SELECT * FROM x UNION ALL SELECT * FROM y)",
                        "{x=[*], y=[*]}"
                );
                recordViewDependencies(compiler, lateralJoins("SELECT * FROM x", 2), "{x=[*], y=[]}");
                recordViewDependencies(
                        compiler,
                        "SELECT count() FROM (SELECT * FROM (SELECT * FROM x) w CROSS JOIN ("
                                + lateralJoins("SELECT k, a FROM x", 2) + ") c)",
                        "{x=[*], y=[]}"
                );
                recordViewDependencies(
                        compiler,
                        "SELECT count() FROM (SELECT * FROM y) s CROSS JOIN (SELECT * FROM x) t",
                        "{x=[*], y=[*]}"
                );
            }
        });
    }

    @Test
    public void testWildcards() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // a wildcard selects every column of its tables, including one they gain later
            assertDependencies("SELECT * FROM x", "{x=[*]}");
            assertDependencies("x WHERE a > 1", "{x=[*]}");
            assertDependencies("SELECT * FROM x JOIN y ON (k)", "{x=[*], y=[*]}");
            assertDependencies("SELECT t.* FROM x t JOIN y u ON (k)", "{x=[*], y=[k]}");
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
            assertDependencies("SELECT a FROM (SELECT DISTINCT * FROM x)", "{x=[*]}");
            assertDependencies("SELECT count() FROM (SELECT DISTINCT * FROM x)", "{x=[*]}");
            assertDependencies("SELECT a FROM (SELECT DISTINCT * FROM (SELECT * FROM x))", "{x=[*]}");
            assertDependencies("SELECT a FROM (SELECT DISTINCT t.* FROM x t JOIN y u ON (k))", "{x=[*], y=[k]}");
            assertDependencies("SELECT a FROM (SELECT *, count() AS n FROM x)", "{x=[*]}");
            // an aggregate groups the rows of its SELECT wherever it sits in an expression
            assertDependencies("SELECT a FROM (SELECT *, CASE WHEN count() > 1 THEN 1 ELSE 0 END AS n FROM x)", "{x=[*]}");
            assertDependencies("SELECT a FROM (SELECT *, coalesce(a, b, count()) AS n FROM x)", "{x=[*]}");
            assertDependencies(
                    "SELECT a FROM (SELECT * FROM x UNION SELECT * FROM y)",
                    "{x=[*], y=[*]}"
            );
            assertDependencies(
                    "SELECT a FROM (SELECT * FROM x EXCEPT SELECT * FROM y)",
                    "{x=[*], y=[*]}"
            );
            assertDependencies(
                    "SELECT a FROM (SELECT * FROM x INTERSECT ALL SELECT * FROM y)",
                    "{x=[*], y=[*]}"
            );
            // an operation other than UNION ALL compares the rows of every branch
            assertDependencies(
                    "SELECT a FROM (SELECT * FROM x UNION ALL SELECT * FROM y UNION SELECT * FROM x)",
                    "{x=[*], y=[*]}"
            );
            execute("CREATE VIEW v_rows AS (SELECT DISTINCT * FROM x)");
            drainWalAndViewQueues();
            assertDependencies("SELECT a FROM v_rows", "{v_rows=[], x=[*]}");

            // such a SELECT reads only the columns a SELECT list names, its own or one under it
            assertDependencies("SELECT a FROM (SELECT DISTINCT * FROM (SELECT a, b FROM x))", "{x=[a, b]}");
            assertDependencies("SELECT a FROM (SELECT DISTINCT a, b FROM (SELECT * FROM x))", "{x=[a, b]}");
            // a window function groups no rows
            assertDependencies("SELECT a FROM (SELECT *, sum(b) OVER () AS s FROM x)", "{x=[a]}");
            // a sub-query in an expression reads no column through the view
            assertDependencies(
                    "SELECT a FROM x WHERE k IN (SELECT k FROM (SELECT DISTINCT * FROM y))",
                    "{x=[a, k], y=[]}",
                    "permission denied [table=y, columns=k,ts,c,d]"
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

    @Test
    public void testWindowFunctionOverOrderedSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertViewResult(
                    "SELECT a, avg(a) OVER () av FROM (SELECT * FROM x ORDER BY b DESC)",
                    "{x=[a, b]}",
                    "a\tav\n3\t2.0\n1\t2.0\n"
            );
            assertViewResult(
                    "SELECT a, avg(a) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) av FROM (SELECT * FROM x ORDER BY b DESC)",
                    "{x=[a, b]}",
                    "a\tav\n3\t3.0\n1\t2.0\n"
            );
        });
    }

    @Test
    public void testWindowJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String windowJoin = " FROM x t WINDOW JOIN y w ON (t.k = w.k) RANGE BETWEEN 1 hour PRECEDING AND 1 hour FOLLOWING";
            // The aggregates of a WINDOW JOIN group no rows of their SELECT, so a SELECT list above it that
            // names its columns keeps only those of the wildcard, and the view reads no other column of x.
            assertDependencies("SELECT ts, s FROM (SELECT t.*, sum(w.c) s" + windowJoin + ')', "{x=[k, ts], y=[c, k, ts]}");
            assertDependencies("SELECT ts, s FROM (SELECT *, sum(w.c) s" + windowJoin + ')', "{x=[k, ts], y=[c, k, ts]}");
            assertOverrideDenied(
                    "DECLARE OVERRIDABLE @c := ts SELECT @c AS v, s FROM (SELECT t.*, sum(w.c) s" + windowJoin + ')',
                    "DECLARE @c := b SELECT * FROM v",
                    "permission denied [table=x, columns=b]"
            );
            // the SELECT list of the view, and one that names none of the columns under it, keep them all
            assertDependencies("SELECT t.*, sum(w.c) s" + windowJoin, "{x=[*], y=[c, k, ts]}");
            assertDependencies("SELECT count() FROM (SELECT t.*, sum(w.c) s" + windowJoin + ')', "{x=[*], y=[c, k, ts]}");
        });
    }

    // Asserts the dependencies CREATE VIEW records for the SELECT of a view with 8 and with 16 LATERAL joins,
    // and the ones ALTER VIEW and CREATE OR REPLACE VIEW record, and that the walks of the optimised SELECT
    // reach about twice as many models with twice as many joins. A walk of every path to the models reaches
    // 2^8 times as many with 16 joins as with 8.
    private static void assertLateralJoinDependencies(
            SqlCompilerImpl compiler,
            IntFunction<String> viewSql,
            String expectedDependencies
    ) throws Exception {
        final long visits8 = recordViewDependencies(compiler, viewSql.apply(8), expectedDependencies);
        final long visits16 = recordViewDependencies(compiler, viewSql.apply(16), expectedDependencies);
        Assert.assertTrue(visits16 + " visits with 16 LATERAL joins, " + visits8 + " with 8", visits16 < 3 * visits8);
    }

    private static void createTables() throws Exception {
        execute("CREATE TABLE x (ts TIMESTAMP, k SYMBOL, a LONG, b LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE TABLE y (ts TIMESTAMP, k SYMBOL, c LONG, d LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO x VALUES (0, 'k1', 1, 2), (3_600_000_000, 'k1', 3, 4)");
        execute("INSERT INTO y VALUES (0, 'k1', 5, 6), (3_600_000_000, 'k1', 7, 8)");
        drainWalQueue();
    }

    // x, y, and x2, which can take the name of x: its designated timestamp is ts2, and ts is another column. It
    // has the metadata version of x, so a plan of x tells x2 from x only by its id.
    private static void createTablesWithTwoTimestamps() throws Exception {
        createTables();
        execute("CREATE TABLE x2 (ts TIMESTAMP, ts2 TIMESTAMP, k SYMBOL, a LONG, b LONG) TIMESTAMP(ts2) PARTITION BY DAY WAL");
        execute("INSERT INTO x2 VALUES (10, 0, 'k1', 1, 2), (3_600_000_010, 3_600_000_000, 'k1', 3, 4)");
        drainWalQueue();
    }

    private static String dependenciesOf(String viewName) {
        final ViewDefinition viewDefinition = getViewDefinition(viewName);
        Assert.assertNotNull(viewDefinition);
        return dependenciesOf(viewDefinition.getDependencies());
    }

    private static String dependenciesOf(LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies) {
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

    // Runs the statement and returns the position and the message of the error it fails with.
    private static String failureOf(String sql, SqlExecutionContext context) {
        try {
            execute(sql, context);
        } catch (SqlException e) {
            return e.getPosition() + ": " + e.getFlyweightMessage();
        }
        throw new AssertionError("the statement did not fail: " + sql);
    }

    // Records the SELECT as the definition of the view v with CREATE VIEW, ALTER VIEW and CREATE OR REPLACE
    // VIEW in turn, and runs the assertion after each.
    private static void forEachViewStatement(String viewSql, ViewStatementAssertion assertion) throws Exception {
        forEachViewStatement(viewSql, sqlExecutionContext, assertion);
    }

    // runs the statements in the context
    private static void forEachViewStatement(String viewSql, SqlExecutionContext context, ViewStatementAssertion assertion) throws Exception {
        execute("CREATE VIEW v AS (" + viewSql + ")", context);
        drainWalAndViewQueues();
        assertion.assertView("CREATE VIEW");

        // a definition that reads no table first, so that the statement records every dependency itself
        execute("ALTER VIEW v AS (SELECT x FROM long_sequence(1))", context);
        drainWalAndViewQueues();
        execute("ALTER VIEW v AS (" + viewSql + ")", context);
        drainWalAndViewQueues();
        assertion.assertView("ALTER VIEW");

        execute("ALTER VIEW v AS (SELECT x FROM long_sequence(1))", context);
        drainWalAndViewQueues();
        execute("CREATE OR REPLACE VIEW v AS (" + viewSql + ")", context);
        drainWalAndViewQueues();
        assertion.assertView("CREATE OR REPLACE VIEW");

        execute("DROP VIEW v", context);
        drainWalAndViewQueues();
    }

    // A balanced UNION stays below the model-depth limit while giving each leaf its own wildcard holder.
    private static String independentWildcardHolders(int lo, int hi, boolean isSameTable) {
        if (hi - lo == 1) {
            return "SELECT count() FROM (SELECT * FROM w" + (isSameTable ? 0 : lo) + ")";
        }
        final int mid = (lo + hi) / 2;
        return "SELECT * FROM (" + independentWildcardHolders(lo, mid, isSameTable)
                + ") UNION ALL SELECT * FROM (" + independentWildcardHolders(mid, hi, isSameTable) + ")";
    }

    // Records the SELECT as the definition of the view v, asserts its dependencies, and returns the outputs the second
    // compile, which keeps the sub-query whole, adds to the sets of outputs of the columns, see
    // SqlOptimiser.recordKeptWholeOrigins().
    private static long keptWholeWork(String viewSql, String expectedDependencies) throws Exception {
        final long addCount = SqlOptimiser.getKeptWholeOriginAddCountForTesting();
        execute("CREATE VIEW v AS (" + viewSql + ")");
        drainWalAndViewQueues();
        final long work = SqlOptimiser.getKeptWholeOriginAddCountForTesting() - addCount;
        Assert.assertEquals(expectedDependencies, dependenciesOf("v"));
        execute("DROP VIEW v");
        drainWalAndViewQueues();
        return work;
    }

    // The SELECT of count LATERAL joins, each in the left side of the next, the innermost over the SELECT
    // innermost. The number of paths to the models of a LATERAL join doubles with each one around it.
    private static String lateralJoins(String innermost, int count) {
        String sql = innermost;
        for (int i = 0; i < count; i++) {
            sql = "SELECT t.k, t.a, count() n FROM (" + sql + ") t CROSS JOIN LATERAL (SELECT c + t.a v FROM y u LIMIT 1) l";
        }
        return sql;
    }

    private static long metadataVersionOf(String tableName) {
        try (TableReader reader = engine.getReader(tableName)) {
            return reader.getMetadataVersion();
        }
    }

    private static String print(SqlExecutionContext context, String sql) throws Exception {
        return TestUtils.printSqlToString(engine, context, sql, new StringSink());
    }

    // Records the SELECT as the definition of the view v with CREATE VIEW, and as the definition ALTER VIEW and
    // CREATE OR REPLACE VIEW give a view, both on the compiler, asserts the dependencies they record, and
    // returns the number of times the walks of the optimised SELECT reached a model.
    private static long recordViewDependencies(SqlCompilerImpl compiler, String viewSql, String expectedDependencies) throws Exception {
        final long visitCount = compiler.getViewDependencyVisitCountForTesting();
        try (Operation operation = compiler.compile("CREATE VIEW v AS (" + viewSql + ")", sqlExecutionContext).getOperation()) {
            compiler.execute(operation, sqlExecutionContext);
        }
        drainWalAndViewQueues();
        Assert.assertEquals("CREATE VIEW: " + viewSql, expectedDependencies, dependenciesOf("v"));
        execute("DROP VIEW v");
        drainWalAndViewQueues();

        // the compilation ALTER VIEW and CREATE OR REPLACE VIEW run, see SqlCompilerImpl.alterViewExecution()
        final LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = new LowerCaseCharSequenceObjHashMap<>();
        compiler.generateViewExecutionModel(viewSql, sqlExecutionContext, dependencies);
        Assert.assertEquals("ALTER VIEW: " + viewSql, expectedDependencies, dependenciesOf(dependencies));
        return compiler.getViewDependencyVisitCountForTesting() - visitCount;
    }

    // the SELECT of outputs that each read c1 of w100, and a constant
    private static String sameColumnOutputs(int outputCount) {
        final StringSink viewSql = new StringSink();
        for (int i = 1; i <= outputCount; i++) {
            viewSql.put(i == 1 ? "SELECT " : ", ").put("c1 + ").put(i).put(" o").put(i);
        }
        return viewSql.put(", 1 one FROM (SELECT * FROM w100)").toString();
    }

    // The columns the dependencies of the view lend, with the columns of a table a "*" lends listed in its place, and
    // null for a view that reads another view: those the view lends to every read, and, with isOtherReadLent, those it
    // lends only to the reads that set none of its variables, without their mark.
    private static @Nullable String scannedDependenciesOf(String viewName, boolean isOtherReadLent) {
        final ViewDefinition viewDefinition = getViewDefinition(viewName);
        Assert.assertNotNull(viewDefinition);
        final LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = viewDefinition.getDependencies();
        final TreeMap<String, TreeSet<String>> sorted = new TreeMap<>();
        final ObjList<CharSequence> tableNames = dependencies.keys();
        for (int i = 0, n = tableNames.size(); i < n; i++) {
            final String tableName = tableNames.getQuick(i).toString();
            if (engine.verifyTableName(tableName).isView()) {
                return null;
            }
            final LowerCaseCharSequenceHashSet columnNames = dependencies.get(tableName);
            final TreeSet<String> columns = new TreeSet<>();
            if (columnNames.contains("*") || (isOtherReadLent && columnNames.contains(ViewDefinition.OTHER_READ_WILDCARD))) {
                try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName(tableName))) {
                    for (int j = 0, m = metadata.getColumnCount(); j < m; j++) {
                        columns.add(metadata.getColumnName(j));
                    }
                }
            } else {
                for (int j = 0, m = columnNames.getKeyCount(); j < m; j++) {
                    final CharSequence columnName = columnNames.getKey(j);
                    if (columnName != null) {
                        if (columnName.charAt(0) != ViewDefinition.OTHER_READ_COLUMN_PREFIX) {
                            columns.add(columnName.toString());
                        } else if (isOtherReadLent) {
                            columns.add(columnName.subSequence(1, columnName.length()).toString());
                        }
                    }
                }
            }
            sorted.put(tableName, columns);
        }
        return sorted.toString();
    }

    // the dependencies in the definition file of the view, which a restart reads back
    private static String storedDependenciesOf(String viewName) {
        try (BlockFileReader reader = new BlockFileReader(configuration); Path path = new Path()) {
            path.of(configuration.getDbRoot());
            final ViewDefinition viewDefinition = new ViewDefinition();
            ViewDefinition.readFrom(viewDefinition, reader, path, path.size(), engine.verifyTableName(viewName));
            return dependenciesOf(viewDefinition.getDependencies());
        }
    }

    private void assertDependencies(String viewSql, String expectedDependencies) throws Exception {
        assertDependencies(viewSql, expectedDependencies, null);
    }

    // Asserts the dependency map CREATE VIEW, ALTER VIEW and CREATE OR REPLACE VIEW give the SELECT of a view, that
    // it lists the columns the reads of the view scan, see assertDependenciesScanned(), and that a principal with no
    // permission on the columns of the base tables reads the view exactly as a principal with every permission does
    // or, given expectedReadFailure, fails to read it.
    private void assertDependencies(String viewSql, String expectedDependencies, @Nullable String expectedReadFailure) throws Exception {
        // a view the principal fails to read reads a table as the principal, outside its dependencies
        if (expectedReadFailure == null) {
            assertDependenciesScanned(viewSql);
        }
        forEachViewStatement(
                viewSql,
                statement -> assertViewDependencies(viewSql, expectedDependencies, expectedReadFailure, statement)
        );
    }

    // Asserts that the dependencies CREATE VIEW records for the SELECT of a view list, for each table, the columns
    // the reads of the view scan, with every column of a table a "*" lends: the read that counts the rows of the
    // view, and each read that names one output of it alone, which between them scan every column a read of the view
    // scans, as a read that names several outputs scans what the reads of each of them scan, or less. The reads
    // select from the SELECT of the view inline, so that each authorizes every column it scans as the reader. A view
    // that reads another one authorizes the columns of the tables that view reads through it, and passes.
    // The columns the view lends to every read, a reader that sets a variable of the view included, are those the read
    // of every output scans, as the plan of the view's own SELECT does.
    private void assertDependenciesScanned(String viewSql) throws Exception {
        execute("CREATE VIEW v_scanned AS (" + viewSql + ")");
        drainWalAndViewQueues();
        final String expectedScans = scannedDependenciesOf("v_scanned", true);
        final String expectedPlanScans = scannedDependenciesOf("v_scanned", false);
        execute("DROP VIEW v_scanned");
        drainWalAndViewQueues();
        if (expectedScans == null) {
            return;
        }
        final ScanRecordingSecurityContext planSecurityContext = new ScanRecordingSecurityContext();
        try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(planSecurityContext, bindVariableService, null, -1, null)) {
            print(context, "SELECT * FROM (" + viewSql + ")");
        }
        Assert.assertEquals(viewSql, expectedPlanScans, planSecurityContext.toString());
        final ObjList<String> reads = new ObjList<>();
        reads.add("SELECT count() FROM (" + viewSql + ")");
        try (RecordCursorFactory factory = select(viewSql)) {
            final RecordMetadata metadata = factory.getMetadata();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                reads.add("SELECT \"" + metadata.getColumnName(i) + "\" FROM (" + viewSql + ")");
            }
        }
        final ScanRecordingSecurityContext securityContext = new ScanRecordingSecurityContext();
        try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(securityContext, bindVariableService, null, -1, null)) {
            for (int i = 0, n = reads.size(); i < n; i++) {
                print(context, reads.getQuick(i));
            }
        }
        Assert.assertEquals(viewSql, expectedScans, securityContext.toString());
    }

    private void assertIndependentWildcardHolders(boolean isSameTable) throws Exception {
        final int maxHolders = 64;
        for (int i = 0, n = isSameTable ? 1 : maxHolders; i < n; i++) {
            execute("CREATE TABLE w" + i + " (a LONG)");
        }
        final TreeMap<String, String> expectedDependencies = new TreeMap<>();
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            long previousVisits = 0;
            for (int holderCount : new int[]{1, maxHolders / 2, maxHolders}) {
                expectedDependencies.clear();
                for (int i = 0, n = isSameTable ? 1 : holderCount; i < n; i++) {
                    expectedDependencies.put("w" + i, "[*]");
                }
                final long visits = recordViewDependencies(
                        compiler,
                        independentWildcardHolders(0, holderCount, isSameTable),
                        expectedDependencies.toString()
                );
                if (holderCount == maxHolders) {
                    Assert.assertTrue(
                            visits + " visits with " + maxHolders + " holders, " + previousVisits + " with " + maxHolders / 2,
                            visits < 3 * previousVisits
                    );
                }
                previousVisits = visits;
            }
        }
        forEachViewStatement(independentWildcardHolders(0, maxHolders, isSameTable), statement -> {
            Assert.assertEquals(statement, expectedDependencies.toString(), dependenciesOf("v"));
            assertQuery("SELECT * FROM v")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("count\n" + "0\n".repeat(maxHolders));
        });
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

    // Asserts the dependencies of the view, that a principal with no permission on the columns of the base tables
    // reads o through it, and that the read that sets a variable of the view, whose plan scans the table as the
    // fragments of expectedPlanScan name it, fails.
    private void assertOverrideDeniedInScan(
            String viewSql,
            String expectedDependencies,
            String overrideReadSql,
            String expectedFailure,
            String... expectedPlanScan
    ) throws Exception {
        assertReads(viewSql, expectedDependencies, "SELECT o FROM v");
        assertOverrideDenied(viewSql, overrideReadSql, expectedFailure);
        execute("CREATE VIEW v AS (" + viewSql + ")");
        drainWalAndViewQueues();
        assertQuery(overrideReadSql).noLeakCheck().assertsPlanContaining(expectedPlanScan);
        execute("DROP VIEW v");
        drainWalAndViewQueues();
    }

    private void assertReads(String viewSql, String expectedDependencies, String... reads) throws Exception {
        assertReads(true, viewSql, expectedDependencies, reads);
    }

    // Asserts the dependency map CREATE VIEW, ALTER VIEW and CREATE OR REPLACE VIEW give the SELECT of a view, that,
    // when isScanned holds, it lists the columns the reads of the view scan, see assertDependenciesScanned(), and that
    // a principal with no permission on the columns of the base tables runs each of the reads of the view as a
    // principal with every permission does.
    private void assertReads(boolean isScanned, String viewSql, String expectedDependencies, String... reads) throws Exception {
        if (isScanned) {
            assertDependenciesScanned(viewSql);
        }
        forEachViewStatement(viewSql, statement -> {
            assertViewDependencies(viewSql, expectedDependencies, null, statement);
            try (SqlExecutionContext context = denyBaseTableSelectContext()) {
                for (String read : reads) {
                    final String expected = print(sqlExecutionContext, read);
                    assertQuery(read)
                            .withContext(context)
                            .noLeakCheck()
                            .inferTimestamp()
                            .inferRandomAccess()
                            .sizeMayVary()
                            .returns(expected);
                }
            } catch (AssertionError e) {
                throw new AssertionError(statement + ": " + viewSql, e);
            }
        });
    }

    private void assertViewDependencies(String viewSql, String expectedDependencies, @Nullable String expectedReadFailure, String statement) throws Exception {
        Assert.assertEquals(statement + ": " + viewSql, expectedDependencies, dependenciesOf("v"));
        Assert.assertEquals(statement + ": " + viewSql, expectedDependencies, storedDependenciesOf("v"));
        try (SqlExecutionContext context = denyBaseTableSelectContext()) {
            if (expectedReadFailure == null) {
                // The views differ in timestamp, size and random access, which the read infers. It returns the
                // rows a principal with every permission reads.
                final String expected = print(sqlExecutionContext, "SELECT * FROM v");
                assertQuery("SELECT * FROM v")
                        .withContext(context)
                        .noLeakCheck()
                        .inferTimestamp()
                        .inferRandomAccess()
                        .sizeMayVary()
                        .returns(expected);
            } else {
                assertQuery("SELECT * FROM v").withContext(context).noLeakCheck().failsWith(expectedReadFailure);
            }
        } catch (AssertionError e) {
            throw new AssertionError(statement + ": " + viewSql, e);
        }
    }

    private void assertViewResult(String viewSql, String expectedDependencies, String expectedResult) throws Exception {
        assertDependenciesScanned(viewSql);
        forEachViewStatement(viewSql, statement -> {
            assertViewDependencies(viewSql, expectedDependencies, null, statement);
            try (SqlExecutionContext context = denyBaseTableSelectContext()) {
                assertQuery("SELECT * FROM v")
                        .withContext(context)
                        .noLeakCheck()
                        .inferTimestamp()
                        .inferRandomAccess()
                        .sizeMayVary()
                        .returns(expectedResult);
            }
        });
    }

    // Records the SELECT as the definition of the view v with CREATE VIEW, then with ALTER VIEW, while x gains
    // a column after the optimiser has read the columns of x, and asserts that the view records "*" for x and
    // that a principal with no permission on the columns of the base tables reads it.
    private void assertWildcardKeptWhileTableAltered(String viewSql, String expectedResult) throws Exception {
        final ColumnAddingSecurityContext securityContext = new ColumnAddingSecurityContext();
        try (
                SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(securityContext, bindVariableService, null, -1, null);
                SqlExecutionContext denyContext = denyBaseTableSelectContext()
        ) {
            final ObjList<String> statements = new ObjList<>();
            statements.add("CREATE VIEW");
            statements.add("ALTER VIEW");
            for (int i = 0; i < statements.size(); i++) {
                final String statement = statements.getQuick(i);
                securityContext.addColumnOnce("e" + (i + 1));
                execute(statement + " v AS (" + viewSql + ")", context);
                drainWalAndViewQueues();
                Assert.assertFalse(statement, securityContext.isColumnPending());
                Assert.assertEquals(statement, "{x=[*], y=[]}", dependenciesOf("v"));
                assertQuery("SELECT * FROM v")
                        .withContext(denyContext)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns(expectedResult);
            }
        }
    }

    @FunctionalInterface
    private interface ViewStatementAssertion {
        void assertView(String statement) throws Exception;
    }

    /**
     * Grants everything, counts the checks of whether a table, y unless named otherwise, is visible, and runs the
     * statements each {@link #alterAtCheck(int, String...)} names at the check it names, counted from that call.
     * A compile checks a table its SELECT names before it reads the columns of the table, and after it parsed the
     * SELECT and read the columns of the tables its FROM clause reads before it.
     */
    private static final class AlteringSecurityContext extends AllowAllSecurityContext {
        // the number of checks each pending alter waits for, and its statements, at the same index
        private final IntList checksToSkip = new IntList();
        private final String tableName;
        private final ObjList<String[]> statements = new ObjList<>();
        private int checkCount;

        private AlteringSecurityContext() {
            this("y");
        }

        private AlteringSecurityContext(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public boolean isTableVisible(TableToken tableToken) {
            if (tableName.equals(tableToken.getTableName())) {
                checkCount++;
                for (int i = 0; i < checksToSkip.size(); i++) {
                    final int checks = checksToSkip.getQuick(i);
                    if (checks > 0) {
                        checksToSkip.setQuick(i, checks - 1);
                        continue;
                    }
                    final String[] alters = statements.getQuick(i);
                    checksToSkip.removeIndex(i);
                    statements.remove(i);
                    i--;
                    try {
                        for (String statement : alters) {
                            execute(statement);
                        }
                    } catch (SqlException e) {
                        throw new AssertionError(e);
                    }
                    drainWalQueue();
                }
            }
            return true;
        }

        private void alterAtCheck(int checksToSkip, String... statements) {
            this.checksToSkip.add(checksToSkip);
            this.statements.add(statements);
        }

        private boolean isAlterPending() {
            return checksToSkip.size() > 0;
        }

        private int takeCheckCount() {
            final int count = checkCount;
            checkCount = 0;
            return count;
        }
    }

    /**
     * Grants everything, and adds a column to x the first time a statement checks whether y is visible
     * after {@link #addColumnOnce(String)}. The optimiser checks that once it has read the columns of x
     * when the FROM clause reads x before y, so x gains the column while the statement compiles.
     */
    private static final class ColumnAddingSecurityContext extends AllowAllSecurityContext {
        private String pendingColumnName;

        @Override
        public boolean isTableVisible(TableToken tableToken) {
            if (pendingColumnName != null && "y".equals(tableToken.getTableName())) {
                final String columnName = pendingColumnName;
                pendingColumnName = null;
                try {
                    execute("ALTER TABLE x ADD COLUMN " + columnName + " INT");
                } catch (SqlException e) {
                    throw new AssertionError(e);
                }
                drainWalQueue();
            }
            return true;
        }

        private void addColumnOnce(String columnName) {
            pendingColumnName = columnName;
        }

        private boolean isColumnPending() {
            return pendingColumnName != null;
        }
    }

    /**
     * Grants everything, and counts the reads of a table: a plan authorizes the read of the columns of a table as
     * it opens the cursor of a scan of the table.
     */
    private static final class ReadCountingSecurityContext extends AllowAllSecurityContext {
        private final String tableName;
        private int readCount;

        private ReadCountingSecurityContext(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
            if (tableName.equals(tableToken.getTableName())) {
                readCount++;
            }
        }

        private int takeReadCount() {
            final int count = readCount;
            readCount = 0;
            return count;
        }
    }

    /**
     * Grants everything, and records the columns of each table a read authorizes: a read that selects from a table
     * itself authorizes every column its scan reads, see {@code AbstractPartitionFrameCursorFactory.authorizeSelect()}.
     */
    private static final class ScanRecordingSecurityContext extends AllowAllSecurityContext {
        private final TreeMap<String, TreeSet<String>> scannedColumns = new TreeMap<>();

        @Override
        public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
            final TreeSet<String> columns = scannedColumns.computeIfAbsent(tableToken.getTableName(), tableName -> new TreeSet<>());
            for (int i = 0, n = columnNames.size(); i < n; i++) {
                columns.add(columnNames.getQuick(i).toString());
            }
        }

        @Override
        public void authorizeSelectOnAnyColumn(TableToken tableToken) {
            scannedColumns.computeIfAbsent(tableToken.getTableName(), tableName -> new TreeSet<>());
        }

        @Override
        public String toString() {
            return scannedColumns.toString();
        }
    }

    /**
     * Grants everything, and gives the name of x to x2 the first time a statement authorizes a read of x
     * after {@link #replaceTableOnce()}. A statement authorizes the read as it opens the cursor of its plan,
     * which then finds that x is no longer the table it planned to read.
     */
    private static final class TableReplacingSecurityContext extends AllowAllSecurityContext {
        private boolean isReplacementPending;

        @Override
        public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
            if (isReplacementPending && "x".equals(tableToken.getTableName())) {
                isReplacementPending = false;
                try {
                    execute("RENAME TABLE x TO x_replaced");
                    execute("RENAME TABLE x2 TO x");
                } catch (SqlException e) {
                    throw new AssertionError(e);
                }
            }
        }

        private boolean isReplacementPending() {
            return isReplacementPending;
        }

        private void replaceTableOnce() {
            isReplacementPending = true;
        }
    }
}
