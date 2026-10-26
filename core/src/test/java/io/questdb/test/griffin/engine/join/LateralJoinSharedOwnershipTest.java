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

package io.questdb.test.griffin.engine.join;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * LATERAL decorrelation shares one sub-query between several places of the rewritten query: the
 * distinct outer values that the outer-ref join reads, and the copies of that join in the correlated
 * sub-queries of the body. The code generator builds the sub-query once and hands it to every place
 * that reads it. The tests run bodies in which the join planner orders a copy ahead of the join that
 * holds the sub-query itself, and bodies in which the code generator drops the sub-tree that holds it.
 * <p>
 * Every expected result comes from the body run once per outer row, with the outer columns substituted
 * as literals, see {@link #expectedRows}. The reference never decorrelates and never shares a sub-query.
 */
@RunWith(Parameterized.class)
public class LateralJoinSharedOwnershipTest extends AbstractCairoTest {
    // a body whose outer-ref join the join planner orders after the copy in its correlated sub-query
    private static final String FILTERED_BODY = "SELECT a.id aid, c.id cid FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x WHERE a.y > {K}";
    private static final String[] LATERAL_JOINS = {"JOIN", "LEFT JOIN", "CROSS JOIN"};
    private static final String OUTER = "o";
    private static final String OUTER_ROWS = "SELECT id, k, k2 FROM o";
    // a body whose outer-ref join comes after a RIGHT join, and which the join planner orders after the copy
    private static final String RIGHT_JOIN_BODY = "SELECT a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x";
    private final boolean isParallelGroupByEnabled;

    public LateralJoinSharedOwnershipTest(boolean isParallelGroupByEnabled) {
        this.isParallelGroupByEnabled = isParallelGroupByEnabled;
    }

    @Parameterized.Parameters(name = "parallel={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {false},
                {true},
        });
    }

    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, String.valueOf(isParallelGroupByEnabled));
        super.setUp();
    }

    @Test
    public void testAggregateAndDistinctBodies() throws Exception {
        assertBodies(
                "SELECT count(*) n, max(a.id) aid FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x WHERE a.y > {K}",
                "SELECT count(*) n, max(c.id) cid FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x WHERE a.y + c.y > {K}",
                "SELECT a.id aid, count(*) n FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x WHERE a.y > {K}",
                "SELECT DISTINCT a.id aid, c.id cid FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x WHERE a.y > {K}",
                "SELECT DISTINCT a.x ax FROM a JOIN (SELECT * FROM b WHERE y < {K2}) b ON b.x = a.x WHERE a.y > {K}"
        );
    }

    @Test
    public void testCompiledFactoryReadsChangedData() throws Exception {
        // the shared sub-query is a distinct set of outer values: a second execution of the same factory
        // reads the rows inserted after the first one
        assertMemoryLeak(() -> {
            createTables();
            final String body = FILTERED_BODY;
            final String sql = lateralSql(OUTER, "JOIN", body);
            try (RecordCursorFactory factory = select(sql)) {
                assertFactoryRows(factory, expectedRows(OUTER_ROWS, "JOIN", body));
                execute("INSERT INTO o VALUES (8, -1, -1), (9, 0, 5)");
                execute("INSERT INTO c VALUES (35, 1, 9), (36, 2, 0)");
                assertFactoryRows(factory, expectedRows(OUTER_ROWS, "JOIN", body));
            }
        });
    }

    @Test
    public void testCompileAndExplainWithoutExecution() throws Exception {
        // a factory that is built and closed without a cursor, and EXPLAIN, release every sub-query they build
        assertMemoryLeak(() -> {
            createTables();
            final String[] bodies = {
                    FILTERED_BODY,
                    "SELECT a.id aid, c.id cid FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x WHERE a.y < {K2}",
                    RIGHT_JOIN_BODY
            };
            for (String body : bodies) {
                for (String join : LATERAL_JOINS) {
                    final String sql = lateralSql(OUTER, join, body);
                    try {
                        assertMemoryLeak(() -> {
                            for (int i = 0; i < 3; i++) {
                                try (RecordCursorFactory ignore = select(sql)) {
                                    // no cursor
                                }
                            }
                            for (int i = 0; i < 3; i++) {
                                printSql("EXPLAIN " + sql, new StringSink());
                            }
                        });
                    } catch (Throwable th) {
                        throw new AssertionError(sql, th);
                    }
                }
            }
        });
    }

    @Test
    public void testConstantFalseJoinBeforeCopy() throws Exception {
        // A constant false ON conjunct of an outer join frees the joins it drops. The sub-query they hold
        // stays readable to the copies that the code generator built before it dropped them.
        assertBodies(
                "SELECT a.id aid, b.id bid, d.id did FROM a RIGHT JOIN b ON a.x = b.x AND b.x = {K} RIGHT JOIN d ON d.x = b.x AND d.x = {K} AND 1 = 0",
                "SELECT a.id aid, b.id bid, d.id did FROM a FULL JOIN b ON a.x = b.x AND b.x = {K} RIGHT JOIN d ON d.x = b.x AND d.x = {K} AND false",
                "SELECT a.id aid, b.id bid, d.id did FROM a RIGHT JOIN b ON a.x = b.x AND b.y > {K} RIGHT JOIN d ON d.x = b.x AND d.y > {K} AND 1 = 0",
                "SELECT a.id aid, c.id cid, d.id did FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x LEFT JOIN d ON d.x = a.x AND 1 = 0 WHERE a.y > {K}",
                "SELECT a.id aid, c.id cid, d.id did FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x RIGHT JOIN d ON d.x = a.x AND d.y > {K} AND 1 = 0"
        );
    }

    @Test
    public void testCopiesInSeveralBranches() throws Exception {
        assertBodies(
                "SELECT a.id aid, c.id cid, g.id gid FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x JOIN (SELECT x, max(y) y, min(id) id FROM c WHERE y > {K} GROUP BY x) g ON g.x = a.x WHERE a.y > {K}",
                "SELECT a.id aid, b.id bid, d.id did FROM a JOIN (SELECT * FROM b WHERE y < {K2}) b ON b.x = a.x JOIN (SELECT DISTINCT x, y, id FROM d WHERE x <> {K}) d ON d.x = a.x WHERE d.y > {K}",
                "SELECT a.id aid, c.id cid, g.id gid FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x LEFT JOIN (SELECT x, max(y) y, min(id) id FROM c WHERE y > {K} GROUP BY x) g ON g.x = c.x WHERE a.y < {K2}",
                "SELECT a.id aid, c.id cid, d.id did FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x JOIN (SELECT DISTINCT x, y, id FROM d WHERE x <> {K}) d ON d.x = a.x AND d.y > {K} WHERE d.y > {K}",
                "SELECT a.id aid, g.id gid, h.id hid FROM a JOIN (SELECT x, max(y) y, min(id) id FROM c WHERE y > {K} GROUP BY x) g ON g.x = a.x JOIN (SELECT x, max(y) y, min(id) id FROM c WHERE y > {K} GROUP BY x) h ON h.x = g.x WHERE h.y > {K}",
                "SELECT a.id aid, c.id cid, d.id did FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x JOIN (SELECT DISTINCT x, y, id FROM d WHERE x <> {K}) d ON d.x > c.x WHERE (a.y > {K} OR d.x = {K2})"
        );
    }

    @Test
    public void testCorrelatedDerivedTableKinds() throws Exception {
        assertBodies(
                "SELECT a.id aid, g.id gid FROM a JOIN (SELECT x, max(y) y, min(id) id FROM c WHERE y > {K} GROUP BY x) g ON g.x = a.x WHERE a.y > {K}",
                "SELECT a.id aid, g.id gid FROM a JOIN (SELECT x, max(y) y, min(id) id FROM c WHERE y > {K} GROUP BY x) g ON g.x = a.x AND g.y > {K} WHERE a.y > {K}",
                "SELECT a.id aid, d.id did FROM a JOIN (SELECT DISTINCT x, y, id FROM d WHERE x <> {K}) d ON d.x = a.x WHERE a.y > {K}",
                "SELECT a.id aid, d.id did FROM a JOIN (SELECT DISTINCT x, y, id FROM d WHERE x <> {K}) d ON d.x = a.x WHERE d.y > {K}",
                "SELECT a.id aid, c.id cid FROM a JOIN (SELECT * FROM c WHERE y > {K} AND x <> {K2}) c ON c.x = a.x WHERE a.y < {K2}",
                "SELECT a.id aid, c.id cid FROM a JOIN (SELECT * FROM c WHERE y > {K} AND x <> {K2}) c ON c.x = a.x AND c.y > {K} WHERE a.y > {K} AND c.y < {K2}",
                "SELECT a.id aid, b.id bid FROM a JOIN (SELECT * FROM b WHERE y < {K2}) b ON b.x = a.x WHERE a.y > {K}",
                "SELECT a.id aid, b.id bid FROM a JOIN (SELECT * FROM b WHERE y < {K2}) b ON b.x = a.x WHERE a.y < {K2}"
        );
    }

    @Test
    public void testCorrelatedFilterOnOuterRefJoinLevel() throws Exception {
        // A conjunct that compares a column of the body with an outer column keeps the outer-ref join in the
        // body next to a copy of it in the correlated sub-query. The join planner orders the outer-ref join,
        // whose only key is the copy's column, after the sub-query.
        assertBodies(
                FILTERED_BODY,
                "SELECT a.id aid, c.id cid FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x WHERE a.y < {K2}",
                "SELECT a.id aid, c.id cid FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x WHERE a.y + c.y > {K}",
                "SELECT a.id aid, c.id cid FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x WHERE c.y > {K}",
                "SELECT a.id aid, c.id cid FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x WHERE (a.y > {K} OR c.x = {K2})",
                "SELECT a.id aid, c.id cid FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x WHERE a.y > {K} AND c.y < {K2}",
                "SELECT a.id aid, c.id cid FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x AND c.y > {K} WHERE a.y > {K}",
                "SELECT a.id aid, c.id cid FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x AND c.y > {K} WHERE (a.y > {K} OR c.x = {K2})",
                "SELECT a.id aid, d.id did, c.id cid FROM a JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x WHERE d.y > {K}",
                "SELECT a.id aid, d.id did, c.id cid FROM a JOIN d ON d.x = a.x AND d.y > {K} JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = d.x WHERE c.y > {K}",
                "SELECT a.id aid, c.id cid, d.id did FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x JOIN d ON d.x > c.x WHERE d.y > {K}",
                "SELECT a.id aid, c.id cid, d.id did FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x CROSS JOIN d WHERE a.y + d.y > {K}",
                "SELECT a.id aid, d.id did, c.id cid FROM a CROSS JOIN d JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = d.x WHERE c.y > {K}",
                "SELECT a.id aid, c.id cid, d.id did FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x LEFT JOIN d ON d.x = c.x WHERE (a.y > {K} OR d.x = {K2})"
        );
    }

    @Test
    public void testEmptyTables() throws Exception {
        // An empty outer table leaves the shared sub-query empty, and empty inner tables leave its places unread.
        final String[] bodies = {
                FILTERED_BODY,
                "SELECT a.id aid, c.id cid FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x WHERE a.y < {K2}",
                "SELECT a.id aid, g.id gid FROM a JOIN (SELECT x, max(y) y, min(id) id FROM c WHERE y > {K} GROUP BY x) g ON g.x = a.x WHERE a.y > {K}",
                RIGHT_JOIN_BODY
        };
        assertMemoryLeak(this::createTables);
        for (String table : new String[]{"c", "a", "d", "o"}) {
            execute("TRUNCATE TABLE " + table);
            assertShapes(OUTER, OUTER_ROWS, bodies);
        }
    }

    @Test
    public void testFailedQueryReleasesSharedSubQuery() throws Exception {
        // The code generator builds the shared sub-query before it reaches the error. After each failure the query
        // without the error returns its rows.
        assertMemoryLeak(this::createTables);
        for (String body : new String[]{FILTERED_BODY, RIGHT_JOIN_BODY}) {
            final String from = "FROM o JOIN LATERAL (" + body.replace("{K2}", "o.k2").replace("{K}", "o.k") + ") l";
            assertException("SELECT o.id oid, l.*, not_a_function(l.aid) f " + from, 22, "unknown function name: not_a_function(INT)");
            assertException("SELECT o.id oid, l.*, cast(l.aid as UUID) f " + from, 22, "there is no matching function `cast` with the argument types: (INT, UUID)");
            assertException("SELECT o.id oid, sum(l.aid) s " + from + " SAMPLE BY 1h", 0, "TIMESTAMP column is required but not provided");
            // the cast fails on the first row, while the cursor reads the shared sub-query
            assertException("SELECT o.id oid, l.*, l.aid + 'abc' f " + from, 0, "inconvertible value: `abc` [STRING -> INT]");
            assertException("SELECT o.id oid, l.* " + from + " ORDER BY l.aid + 'abc'", 0, "inconvertible value: `abc` [STRING -> INT]");
            final String sql = lateralSql(OUTER, "JOIN", body);
            assertMemoryLeak(() -> assertQuery(sql).noLeakCheck().sizeMayVary().returns(expectedRows(OUTER_ROWS, "JOIN", body)));
        }
    }

    @Test
    public void testFourWorkers() throws Exception {
        // a vectorized GROUP BY builds one hash table per worker
        assertMemoryLeak(() -> {
            createTables();
            final String body = FILTERED_BODY;
            try (SqlExecutionContext context = TestUtils.createSqlExecutionCtx(engine, 4)) {
                context.setParallelGroupByEnabled(isParallelGroupByEnabled);
                for (String join : LATERAL_JOINS) {
                    final String sql = lateralSql(OUTER, join, body);
                    try {
                        assertMemoryLeak(() -> assertQuery(sql)
                                .withContext(context)
                                .noLeakCheck()
                                .sizeMayVary()
                                .returns(expectedRows(OUTER_ROWS, join, body)));
                    } catch (Throwable th) {
                        throw new AssertionError(sql, th);
                    }
                }
            }
        });
    }

    @Test
    public void testNestedLateral() throws Exception {
        // A LATERAL in the body reads the outer row and a column of the body. DuckDB returns the same rows for the
        // same SQL, with ON true after each LATERAL.
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT o.id oid, l.* FROM o JOIN LATERAL (
                        SELECT a.id aid, z.cid FROM a
                        JOIN LATERAL (SELECT c.id cid FROM c WHERE c.y > o.k AND c.x = a.x) z
                        WHERE a.y > o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            oid\taid\tcid
                            7\t10\t33
                            7\t11\t30
                            7\t15\t30
                            """);
            assertQuery("""
                    SELECT o.id oid, l.* FROM o LEFT JOIN LATERAL (
                        SELECT a.id aid, z.cid FROM a
                        JOIN LATERAL (SELECT c.id cid FROM c WHERE c.y > o.k AND c.x = a.x) z
                        WHERE a.y > o.k
                    ) l ORDER BY 1, 2, 3
                    """)
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            oid\taid\tcid
                            1\tnull\tnull
                            2\tnull\tnull
                            3\tnull\tnull
                            4\tnull\tnull
                            5\tnull\tnull
                            6\tnull\tnull
                            7\t10\t33
                            7\t11\t30
                            7\t15\t30
                            """);
            assertQuery("""
                    SELECT o.id oid, l.* FROM o JOIN LATERAL (
                        SELECT a.id aid, z.cid, z.did FROM a
                        JOIN LATERAL (
                            SELECT c.id cid, d.id did FROM (SELECT * FROM c WHERE y > o.k) c
                            JOIN d ON d.x = c.x
                            WHERE c.x = a.x AND d.y > o.k2
                        ) z
                        WHERE a.y > o.k
                    ) l ORDER BY 1, 2, 3, 4
                    """)
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            oid\taid\tcid\tdid
                            7\t10\t33\t40
                            7\t11\t30\t43
                            7\t15\t30\t43
                            """);
        });
    }

    @Test
    public void testOuterColumnTypes() throws Exception {
        // the type of the outer column picks the GROUP BY that computes the distinct outer values
        final String[][] types = {
                // type, values of k by outer row (null is NULL), comparison
                {"LONG", "1", "2", "null", "1", "3", "4", "0"},
                {"SHORT", "1", "2", "0", "1", "3", "4", "0"},
                {"DOUBLE", "1.5", "2.5", "null", "1.5", "3.5", "4.5", "0.5"},
                {"TIMESTAMP", "2024-01-02T00:00:00.000000Z", "2024-01-03T00:00:00.000000Z", "null", "2024-01-02T00:00:00.000000Z", "2024-01-04T00:00:00.000000Z", "2024-01-05T00:00:00.000000Z", "2024-01-01T00:00:00.000000Z"},
                {"VARCHAR", "b", "c", "null", "b", "d", "e", "a"},
                {"STRING", "b", "c", "null", "b", "d", "e", "a"},
                {"DATE", "2024-01-02T00:00:00.000Z", "2024-01-03T00:00:00.000Z", "null", "2024-01-02T00:00:00.000Z", "2024-01-04T00:00:00.000Z", "2024-01-05T00:00:00.000Z", "2024-01-01T00:00:00.000Z"},
        };
        for (String[] t : types) {
            final String type = t[0];
            assertMemoryLeak(() -> {
                execute("CREATE TABLE ot (id INT, k " + type + ")");
                execute("CREATE TABLE at (id INT, x INT, y " + type + ")");
                execute("CREATE TABLE ct (id INT, x INT, y " + type + ")");
                final StringSink sink = new StringSink();
                sink.put("INSERT INTO ot VALUES ");
                for (int i = 1; i < t.length; i++) {
                    if (i > 1) {
                        sink.put(", ");
                    }
                    sink.put('(').put(i).put(", ").put(literal(type, t[i])).put(')');
                }
                execute(sink);
                // a.y and c.y take the values of the outer column, by row
                execute("INSERT INTO at VALUES (10, 1, " + literal(type, t[1]) + "), (11, 2, " + literal(type, t[2]) + "), (12, null, " + literal(type, t[5]) + "), (13, 3, null), (14, 1, " + literal(type, t[6]) + "), (15, 2, " + literal(type, t[7]) + ")");
                execute("INSERT INTO ct VALUES (30, 2, " + literal(type, t[1]) + "), (31, 3, " + literal(type, t[2]) + "), (32, null, null), (33, 1, " + literal(type, t[6]) + "), (34, 5, " + literal(type, t[7]) + ")");
                final String outerRows = "SELECT id, k, k FROM ot";
                final String[] bodies = {
                        "SELECT a.id aid, c.id cid FROM at a JOIN (SELECT * FROM ct WHERE y > {K}) c ON c.x = a.x WHERE a.y > {K}",
                        "SELECT a.id aid, c.id cid FROM at a JOIN (SELECT * FROM ct WHERE y <> {K}) c ON c.x = a.x WHERE a.y < {K}",
                };
                for (String body : bodies) {
                    for (String join : LATERAL_JOINS) {
                        final String sql = lateralSql("ot o", join, body);
                        try {
                            assertMemoryLeak(() -> assertQuery(sql).noLeakCheck().sizeMayVary().returns(expectedRows(outerRows, type, join, body)));
                        } catch (Throwable th) {
                            throw new AssertionError(type + ": " + sql, th);
                        }
                    }
                }
                execute("DROP TABLE ot");
                execute("DROP TABLE at");
                execute("DROP TABLE ct");
            });
        }
    }

    @Test
    public void testOuterSources() throws Exception {
        final String[] bodies = {
                FILTERED_BODY,
                "SELECT a.id aid, c.id cid FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x WHERE a.y < {K2}",
                RIGHT_JOIN_BODY
        };
        final String[][] outers = {
                {"(SELECT id, k, k2 FROM o WHERE id > 0) o", "SELECT id, k, k2 FROM o WHERE id > 0"},
                {"(SELECT min(id) id, k, max(k2) k2 FROM o GROUP BY k) o", "SELECT min(id) id, k, max(k2) k2 FROM o GROUP BY k"},
                {"(SELECT k, min(id) id, max(k2) k2 FROM o GROUP BY k) o", "SELECT min(id) id, k, max(k2) k2 FROM o GROUP BY k"},
                {"o JOIN t ON t.k = o.k", "SELECT o.id, o.k, o.k2 FROM o JOIN t ON t.k = o.k"},
                {"(SELECT id, k, k2 FROM o UNION ALL SELECT id + 100, k2, k FROM o) o", "SELECT id, k, k2 FROM o UNION ALL SELECT id + 100, k2, k FROM o"},
        };
        assertMemoryLeak(this::createTables);
        for (String[] outer : outers) {
            assertShapes(outer[0], outer[1], bodies);
        }
    }

    @Test
    public void testQueryContexts() throws Exception {
        // the LATERAL query under a CTE, a view, an aggregate, UNION ALL, LIMIT, CREATE TABLE AS and INSERT AS SELECT
        assertMemoryLeak(() -> {
            createTables();
            for (String body : new String[]{FILTERED_BODY, RIGHT_JOIN_BODY}) {
                final String from = lateralFrom(OUTER, "JOIN", body);
                final String orderBy = orderBy(body);
                final String expected = expectedRows(OUTER_ROWS, "JOIN", body);
                final String[] lines = expected.split("\n");
                final StringSink doubled = new StringSink();
                doubled.put(lines[0]).put('\n');
                for (int i = 1; i < lines.length; i++) {
                    doubled.put(lines[i]).put('\n').put(lines[i]).put('\n');
                }
                final StringSink limited = new StringSink();
                for (int i = 0; i < Math.min(lines.length, 3); i++) {
                    limited.put(lines[i]).put('\n');
                }
                final StringSink counts = new StringSink();
                counts.put("oid\tn\n");
                for (int i = 1; i < lines.length; ) {
                    final String oid = lines[i].substring(0, lines[i].indexOf('\t'));
                    int n = 0;
                    while (i < lines.length && lines[i].startsWith(oid + '\t')) {
                        n++;
                        i++;
                    }
                    counts.put(oid).put('\t').put(n).put('\n');
                }
                assertContext("WITH lat AS (" + from + ") SELECT * FROM lat" + orderBy, expected);
                assertContext("SELECT * FROM (" + from + ")" + orderBy, expected);
                assertContext("SELECT oid, count(*) n FROM (" + from + ") ORDER BY oid", counts.toString());
                assertContext("SELECT * FROM (" + from + " UNION ALL " + from + ")" + orderBy, doubled.toString());
                assertContext(from + orderBy + " LIMIT 2", limited.toString());
                execute("CREATE VIEW lat_v AS (" + from + ")");
                drainWalAndViewQueues();
                assertContext("SELECT * FROM lat_v" + orderBy, expected);
                execute("DROP VIEW lat_v");
                drainWalAndViewQueues();
                execute("CREATE TABLE lat_t AS (" + from + ")");
                assertContext("SELECT * FROM lat_t" + orderBy, expected);
                execute("INSERT INTO lat_t SELECT * FROM (" + from + ")");
                assertContext("SELECT * FROM lat_t" + orderBy, doubled.toString());
                execute("DROP TABLE lat_t");
            }
            // a bind variable in the body
            final String body = FILTERED_BODY + " AND a.id <> $1";
            bindVariableService.clear();
            bindVariableService.setInt(0, 11);
            assertContext(lateralSql(OUTER, "JOIN", body), expectedRows(OUTER_ROWS, "JOIN", body));
            bindVariableService.setInt(0, 15);
            assertContext(lateralSql(OUTER, "LEFT JOIN", body), expectedRows(OUTER_ROWS, "LEFT JOIN", body));
        });
    }

    @Test
    public void testRightAndFullJoinBeforeCopy() throws Exception {
        // The outer-ref join of a body with a RIGHT or FULL join comes right after that join, and the join
        // planner orders it after a correlated sub-query joined to the rows of that join.
        assertBodies(
                RIGHT_JOIN_BODY,
                "SELECT a.id aid, d.id did, c.id cid FROM a FULL JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x",
                "SELECT a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = d.x",
                "SELECT a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE y > {K2}) c ON c.x = a.x",
                "SELECT a.id aid, d.id did, c.id cid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE y > {K} AND x <> {K2}) c ON c.x = a.x",
                "SELECT a.id aid, d.id did, g.id gid FROM a RIGHT JOIN d ON d.x = a.x JOIN (SELECT x, max(y) y, min(id) id FROM c WHERE y > {K} GROUP BY x) g ON g.x = a.x",
                "SELECT a.id aid, d.id did, c.id cid FROM a FULL JOIN d ON d.x = a.x JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = d.x WHERE c.id > 0"
        );
    }

    @Test
    public void testRuntimeConstantGates() throws Exception {
        // a runtime constant conjunct of an ON clause gates the joined table: false empties it in every execution
        assertBodies(
                "SELECT a.id aid, c.id cid FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x AND now() > '2000-01-01' WHERE a.y > {K}",
                "SELECT a.id aid, c.id cid FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x AND now() < '2000-01-01' WHERE a.y > {K}",
                "SELECT a.id aid, c.id cid, d.id did FROM a JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x LEFT JOIN d ON d.x = a.x AND now() < '2000-01-01' WHERE a.y > {K}",
                "SELECT a.id aid, d.id did, c.id cid FROM a JOIN d ON d.x = a.x AND now() < '2000-01-01' JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = a.x WHERE d.y > {K}"
        );
    }

    @Test
    public void testTwoLateralJoins() throws Exception {
        // each LATERAL join has an outer-ref sub-query of its own over the same outer table
        assertMemoryLeak(() -> {
            createTables();
            final String body1 = FILTERED_BODY;
            final String body2 = "SELECT d.id did FROM d JOIN (SELECT * FROM c WHERE y > {K}) c ON c.x = d.x WHERE d.y > {K2}";
            final String[] joins = {"JOIN", "LEFT JOIN"};
            for (String join1 : joins) {
                for (String join2 : joins) {
                    final String sql = "SELECT o.id oid, l1.*, l2.* FROM o " + join1 + " LATERAL (" + outerColumns(body1) + ") l1 "
                            + join2 + " LATERAL (" + outerColumns(body2) + ") l2 ORDER BY 1, 2, 3, 4";
                    final String expected1 = expectedRows(OUTER_ROWS, join1, body1);
                    final String expected2 = expectedRows(OUTER_ROWS, join2, body2);
                    try {
                        assertMemoryLeak(() -> assertQuery(sql).noLeakCheck().sizeMayVary().returns(product(expected1, expected2)));
                    } catch (Throwable th) {
                        throw new AssertionError(sql, th);
                    }
                }
            }
        });
    }

    private static String lateralFrom(String outer, String join, String body) {
        return "SELECT o.id oid, l.* FROM " + outer + ' ' + join + " LATERAL (" + outerColumns(body) + ") l";
    }

    private static String lateralSql(String outer, String join, String body) {
        return lateralFrom(outer, join, body) + orderBy(body);
    }

    // the text that printSql() prints for a value of the type, as a literal: printSql() prints NULL as null, or, for
    // the types that print as text, as an empty string, which the test data does not hold otherwise
    private static String literal(String type, String value) {
        if ("null".equals(value) || value.isEmpty()) {
            return "NULL::" + type;
        }
        return "'" + value + "'::" + type;
    }

    // the ORDER BY of every column of the LATERAL query: the outer id and the columns of the body
    private static String orderBy(String body) {
        final int columnCount = body.substring(0, body.indexOf(" FROM ")).split(",").length + 1;
        final StringSink sink = new StringSink();
        sink.put(" ORDER BY 1");
        for (int i = 2; i <= columnCount; i++) {
            sink.put(", ").put(i);
        }
        return sink.toString();
    }

    private static String outerColumns(String body) {
        return body.replace("{K2}", "o.k2").replace("{K}", "o.k");
    }

    // the rows of two LATERAL joins of the same outer rows: each row of the first with each row of the second that
    // has the same outer id
    private static String product(String expected1, String expected2) {
        final String[] lines1 = expected1.split("\n");
        final String[] lines2 = expected2.split("\n");
        final List<String[]> rows = new ArrayList<>();
        for (int i = 1; i < lines1.length; i++) {
            final String[] row1 = lines1[i].split("\t", -1);
            for (int j = 1; j < lines2.length; j++) {
                final String[] row2 = lines2[j].split("\t", -1);
                if (row1[0].equals(row2[0])) {
                    final String[] row = Arrays.copyOf(row1, row1.length + row2.length - 1);
                    System.arraycopy(row2, 1, row, row1.length, row2.length - 1);
                    rows.add(row);
                }
            }
        }
        final String header2 = lines2[0].substring(lines2[0].indexOf('\t'));
        return toText(lines1[0] + header2, rows);
    }

    // INT and LONG columns sort NULL first, as their NULL is the smallest value of the type
    private static long sortKey(String value) {
        return "null".equals(value) ? Long.MIN_VALUE : Long.parseLong(value);
    }

    // the header and the rows, sorted as the ORDER BY of every column sorts them
    private static String toText(String header, List<String[]> rows) {
        rows.sort((l, r) -> {
            for (int i = 0; i < l.length; i++) {
                final int cmp = Long.compare(sortKey(l[i]), sortKey(r[i]));
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        });
        final StringSink sink = new StringSink();
        sink.put(header).put('\n');
        for (String[] row : rows) {
            sink.put(String.join("\t", row)).put('\n');
        }
        return sink.toString();
    }

    private void assertBodies(String... bodies) throws Exception {
        assertMemoryLeak(this::createTables);
        assertShapes(OUTER, OUTER_ROWS, bodies);
    }

    private void assertContext(String sql, String expected) {
        try {
            assertQuery(sql).noLeakCheck().sizeMayVary().returns(expected);
        } catch (Throwable th) {
            throw new AssertionError(sql, th);
        }
    }

    private void assertFactoryRows(RecordCursorFactory factory, String expected) throws SqlException {
        final StringSink sink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            println(factory.getMetadata(), cursor, sink);
        }
        TestUtils.assertEquals(expected, sink);
    }

    private void assertLateral(String outer, String outerRows, String join, String body, StringSink failures) {
        final String sql = lateralSql(outer, join, body);
        try {
            assertMemoryLeak(() -> assertQuery(sql).noLeakCheck().sizeMayVary().returns(expectedRows(outerRows, join, body)));
        } catch (Throwable th) {
            failures.put(sql).put("\n    ").put(th.toString().replace('\n', ' ')).put('\n');
        }
    }

    private void assertNoFailures(StringSink failures) {
        if (failures.length() > 0) {
            Assert.fail("queries that failed:\n" + failures);
        }
    }

    private void assertShapes(String outer, String outerRows, String... bodies) {
        final StringSink failures = new StringSink();
        for (String body : bodies) {
            for (String join : LATERAL_JOINS) {
                assertLateral(outer, outerRows, join, body, failures);
            }
        }
        assertNoFailures(failures);
    }

    private void createTables() throws SqlException {
        execute("CREATE TABLE o (id INT, k INT, k2 INT)");
        execute("INSERT INTO o VALUES (1, 1, 1), (2, 2, null), (3, null, 2), (4, 1, 2), (5, 3, 3), (6, 4, 1), (7, 0, 0)");
        execute("CREATE TABLE t (k INT)");
        execute("INSERT INTO t VALUES (1), (3), (0), (null)");
        execute("CREATE TABLE a (id INT, x INT, y INT)");
        execute("INSERT INTO a VALUES (10, 1, 1), (11, 2, 2), (12, null, 3), (13, 3, null), (14, 1, 0), (15, 2, 5)");
        execute("CREATE TABLE b (id INT, x INT, y INT)");
        execute("INSERT INTO b VALUES (20, 1, 2), (21, 2, null), (22, 3, 1), (23, null, 4), (24, 1, 3)");
        execute("CREATE TABLE c (id INT, x INT, y INT)");
        execute("INSERT INTO c VALUES (30, 2, 1), (31, 3, 2), (32, null, null), (33, 1, 4), (34, 5, 0)");
        execute("CREATE TABLE d (id INT, x INT, y INT)");
        execute("INSERT INTO d VALUES (40, 1, 1), (41, 4, 2), (42, 3, null), (43, 2, 3)");
    }

    private String expectedRows(String outerRows, String join, String body) throws SqlException {
        return expectedRows(outerRows, "INT", join, body);
    }

    // The rows of the body run once per row of outerRows, whose columns are the outer id, k and k2, with
    // {K} and {K2} replaced by the literals of k and k2, sorted as the ORDER BY of lateralSql() sorts them.
    private String expectedRows(String outerRows, String type, String join, String body) throws SqlException {
        final List<String[]> rows = new ArrayList<>();
        String header = null;
        final StringSink outerSink = new StringSink();
        printSql(outerRows, outerSink);
        final String[] outerLines = outerSink.toString().split("\n");
        final StringSink bodySink = new StringSink();
        for (int i = 1; i < outerLines.length; i++) {
            final String[] outer = outerLines[i].split("\t", -1);
            bodySink.clear();
            printSql(body.replace("{K2}", literal(type, outer[2])).replace("{K}", literal(type, outer[1])), bodySink);
            final String[] lines = bodySink.toString().split("\n");
            header = lines[0];
            final int columnCount = header.split("\t", -1).length;
            for (int j = 1; j < lines.length; j++) {
                rows.add((outer[0] + '\t' + lines[j]).split("\t", -1));
            }
            if (lines.length == 1 && "LEFT JOIN".equals(join)) {
                final String[] row = new String[columnCount + 1];
                Arrays.fill(row, "null");
                row[0] = outer[0];
                rows.add(row);
            }
        }
        if (header == null) {
            // no outer row: the header is that of the body
            bodySink.clear();
            printSql(body.replace("{K2}", literal(type, "null")).replace("{K}", literal(type, "null")), bodySink);
            header = bodySink.toString().split("\n")[0];
        }
        return toText("oid\t" + header, rows);
    }
}
