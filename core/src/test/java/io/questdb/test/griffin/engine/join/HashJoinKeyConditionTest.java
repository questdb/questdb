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
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.functions.test.TestThrowingFilterFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestTimestampCounterFactory;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVarTuple;
import io.questdb.test.tools.CountingSqlExecutionCircuitBreaker;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.LongFunction;

// A condition after an INNER hash join that reads slave columns and master key columns, where each such
// master column has a slave key column of its type that the join equates with it, moves into the join,
// which reads the slave key column instead. When the join hashes its master, it evaluates the condition
// once per slave row whose key the master has; otherwise, once per row of the join, as the filter after
// the join does. The tables of createTables() give the expected rows by hand: m has 5 rows, s 8, so
// "FROM m JOIN s" hashes m, and "FROM s JOIN m" hashes m as well, as its slave.
public class HashJoinKeyConditionTest extends AbstractCairoTest {
    private static final String NULL_KEY_TYPE_ROWS = """
            id\tid1
            1\t11
            1\t16
            2\t13
            3\t14
            """;

    // BETWEEN, a SET_OPERATION of three arguments, and CASE move into the join when each of their arguments
    // reads slave or master key columns, and stay after it when one reads a master column that is no key. Per m
    // row of k = 1, s rows 10 (v 1) and 11 (v 4) lie between 1 and 4; for m row 3 (k = 2), s row 14 (v 3) lies
    // between 2 and 5. With m.y + 4 as the upper bound, m row 2 (y 5) takes s rows 12 (v 9) and 13 (v 6) as
    // well. The CASE keeps m.k < s.v < 7, s rows 11 and 13 for k = 1 and s row 14 for k = 2; on m.y it keeps
    // m.y < s.v < 7, which m row 2 (y 5) narrows to s row 13.
    @Test
    public void testBetweenAndCase() throws Exception {
        assertMemoryLeak(() -> {
            createConjunctTables();
            assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE s.v BETWEEN m.k AND m.k + 3 ORDER BY 1, 2")
                    .noLeakCheck()
                    .withPlanContaining("filter: s.v between s.k and s.k+3")
                    .withPlanNotContaining("Filter filter")
                    .returns("""
                            id\tid1
                            1\t10
                            1\t11
                            2\t10
                            2\t11
                            3\t14
                            """);
            assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE s.v BETWEEN m.k AND m.y + 4 ORDER BY 1, 2")
                    .noLeakCheck()
                    .withPlanContaining("Filter filter: s.v between m.k and m.y+4")
                    .returns("""
                            id\tid1
                            1\t10
                            1\t11
                            2\t10
                            2\t11
                            2\t12
                            2\t13
                            3\t14
                            """);
            assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE CASE WHEN s.v > m.k THEN s.v < 7 ELSE false END ORDER BY 1, 2")
                    .noLeakCheck()
                    .withPlanContaining("filter: case([s.k<s.v,s.v<7,false])")
                    .withPlanNotContaining("Filter filter")
                    .returns("""
                            id\tid1
                            1\t11
                            1\t13
                            2\t11
                            2\t13
                            3\t14
                            """);
            assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE CASE WHEN s.v > m.y THEN s.v < 7 ELSE false END ORDER BY 1, 2")
                    .noLeakCheck()
                    .withPlanContaining("Filter filter: case([m.y<s.v,s.v<7,false])")
                    .returns("""
                            id\tid1
                            1\t10
                            1\t11
                            1\t13
                            2\t13
                            3\t14
                            """);
        });
    }

    // $1 = 0 keeps s rows 11 (5 > 1) and 13 (3 > 2); $1 = 3 keeps s row 11 (5 > 4) only; a NULL $1 makes
    // m.k + $1 NULL, and no comparison with NULL holds.
    @Test
    public void testBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE s.v > m.k + $1 ORDER BY 1, 2";
            final ObjList<BindVarTuple> cases = new ObjList<>();
            cases.add(BindVarTuple.ok(
                    "zero",
                    """
                            id\tid1
                            1\t11
                            2\t11
                            3\t11
                            4\t13
                            """,
                    bindVariableService -> bindVariableService.setInt(0, 0)
            ));
            cases.add(BindVarTuple.ok(
                    "three",
                    """
                            id\tid1
                            1\t11
                            2\t11
                            3\t11
                            """,
                    bindVariableService -> bindVariableService.setInt(0, 3)
            ));
            cases.add(BindVarTuple.ok(
                    "NULL",
                    "id\tid1\n",
                    bindVariableService -> bindVariableService.setInt(0, Numbers.INT_NULL)
            ));
            assertQuery(sql).noLeakCheck().assertBinds(cases);
            bindVariableService.setInt(0, 0);
            assertQuery(sql).noLeakCheck().assertsPlanContaining("filter: s.k+$0::int<s.v");
        });
    }

    // calculateSize() counts the rows left after each number of rows read, the rows of the current hash
    // chain included: a swapped cursor counts every entry of the chain of a slave row that the condition
    // let through, and the other one checks each entry. A join without a condition counts the chains.
    @Test
    public void testCalculateSizeAfterEachRow() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertCalculateSizeAfterEachRow("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE s.v > m.k", 4);
            assertCalculateSizeAfterEachRow("SELECT s.id, m.id FROM s JOIN m ON m.k = s.k WHERE m.y > s.k * 10", 6);
            // k = 1 joins 3 m rows with 2 s rows, k = 2 one m row with 2, and the NULL key one m row with 2
            assertCalculateSizeAfterEachRow("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k", 10);
        });
    }

    // calculateSize() of a join that hashes its slave evaluates the condition for each entry of the hash chain of
    // a master row, and checks the circuit breaker for each, as counting the rows of a filter after the join
    // does. m has 4 rows of k = 1 and s 3, so the join hashes s: the 4 master rows and their 12 chain entries
    // check the breaker, and s rows 11 and 12 pass s.v > s.k for each m row.
    @Test
    public void testCalculateSizeChecksCircuitBreakerPerChainEntry() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (id INT, k INT)");
            execute("CREATE TABLE s (id INT, k INT, v INT)");
            execute("INSERT INTO m VALUES (1, 1), (2, 1), (3, 1), (4, 1)");
            execute("INSERT INTO s VALUES (10, 1, 0), (11, 1, 5), (12, 1, 7)");
            final String sql = "SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE s.v > m.k";
            assertQuery(sql).noLeakCheck().assertsPlanContaining("filter: s.k<s.v");
            try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                // the first row builds the hash table, which calculateSize() then reads
                Assert.assertTrue(cursor.hasNext());
                cursor.toTop();
                final CountingSqlExecutionCircuitBreaker circuitBreaker = new CountingSqlExecutionCircuitBreaker(sqlExecutionContext.getCircuitBreaker());
                final RecordCursor.Counter counter = new RecordCursor.Counter();
                cursor.calculateSize(circuitBreaker, counter);
                Assert.assertEquals(8, counter.get());
                Assert.assertEquals(16, circuitBreaker.getCheckCount());
            }
        });
    }

    // Every key type whose equal values are the same value. Per m row: v1 meets s rows 10, 11 and 16,
    // of which k2 differs from v1 for rows 11 and 16; v2 meets s rows 12 and 13, of which row 13 differs;
    // NULL meets s rows 14 and 15, as = matches NULL to NULL, and row 14 differs. The join evaluates the
    // condition on s.k, so no filter follows it.
    @Test
    public void testColumnTypesWithNull() throws Exception {
        assertMemoryLeak(() -> {
            final String[][] types = {
                    {"INT", "1", "2"},
                    {"LONG", "1", "2"},
                    {"DATE", "'2024-01-01T00:00:00.000Z'", "'2024-01-02T00:00:00.000Z'"},
                    {"TIMESTAMP", "'2024-01-01T00:00:00.000000Z'", "'2024-01-02T00:00:00.000000Z'"},
                    {"TIMESTAMP_NS", "'2024-01-01T00:00:00.000000000Z'", "'2024-01-02T00:00:00.000000000Z'"},
                    {"IPv4", "'1.1.1.1'", "'2.2.2.2'"},
                    {"UUID", "'11111111-1111-1111-1111-111111111111'", "'22222222-2222-2222-2222-222222222222'"},
                    {"LONG256", "'0x01'", "'0x02'"},
                    {"GEOHASH(4c)", "#u33d", "#u33e"},
                    {"DECIMAL(10,2)", "'1.50'", "'2.50'"},
                    {"CHAR", "'p'", "'q'"},
                    {"SYMBOL", "'p'", "'q'"},
                    {"STRING", "'p'", "'q'"},
                    {"VARCHAR", "'p'", "'q'"},
            };
            for (String[] type : types) {
                createKeyTypeTables(type[0], type[0], type[1], type[2]);
                assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE s.k2 != m.k ORDER BY 1, 2")
                        .noLeakCheck()
                        .withPlanContaining("Hash Join Light", "filter: s.k2!=s.k")
                        .withPlanNotContaining("Filter filter")
                        .returns(NULL_KEY_TYPE_ROWS);
                dropKeyTypeTables();
            }
        });
    }

    // testColumnTypesWithNull() for the types without NULL: v1 meets s rows 10 and 11, of which k2 differs
    // for row 11; v2 meets s rows 12 and 13, of which row 13 differs.
    @Test
    public void testColumnTypesWithoutNull() throws Exception {
        assertMemoryLeak(() -> {
            final String[][] types = {
                    {"BOOLEAN", "true", "false"},
                    {"BYTE", "1::BYTE", "2::BYTE"},
                    {"SHORT", "1::SHORT", "2::SHORT"},
            };
            for (String[] type : types) {
                final String v1 = type[1];
                final String v2 = type[2];
                execute("CREATE TABLE m (id INT, k " + type[0] + ")");
                execute("CREATE TABLE s (id INT, k " + type[0] + ", k2 " + type[0] + ")");
                execute("INSERT INTO m VALUES (1, " + v1 + "), (2, " + v2 + ")");
                execute("INSERT INTO s VALUES (10, " + v1 + ", " + v1 + "), (11, " + v1 + ", " + v2 + "), "
                        + "(12, " + v2 + ", " + v2 + "), (13, " + v2 + ", " + v1 + ")");
                assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE s.k2 != m.k ORDER BY 1, 2")
                        .noLeakCheck()
                        .withPlanContaining("Hash Join Light", "filter: s.k2!=s.k")
                        .withPlanNotContaining("Filter filter")
                        .returns("""
                                id\tid1
                                1\t11
                                2\t13
                                """);
                dropKeyTypeTables();
            }
        });
    }

    // The code generator compiles a condition that moves into the join twice: once to check that it has one
    // value per row however often it runs, and once for the join. A failure in either fails the query and
    // closes what it built.
    @Test
    public void testCompileFailure() throws Exception {
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE test_throwing_filter() OR s.v > m.k ORDER BY 1, 2";
            for (int call = 1; call <= 2; call++) {
                TestThrowingFilterFunctionFactory.reset(call);
                assertExceptionNoLeakCheck(sql, sql.indexOf("test_throwing_filter"), "test_throwing_filter: configured to throw on call " + call);
                Assert.assertEquals(call - 1, TestThrowingFilterFunctionFactory.CLOSE_COUNT.get());
            }
            TestThrowingFilterFunctionFactory.reset(-1);
            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertEquals(2, TestThrowingFilterFunctionFactory.CONSTRUCT_COUNT.get());
                Assert.assertEquals(1, TestThrowingFilterFunctionFactory.CLOSE_COUNT.get());
                Assert.assertNotNull(factory);
            }
            Assert.assertEquals(2, TestThrowingFilterFunctionFactory.CLOSE_COUNT.get());
            // test_throwing_filter() is true, so every row of the join passes
            assertQuery(sql)
                    .noLeakCheck()
                    .withPlanContaining("filter: (test_throwing_filter() or s.k<s.v)")
                    .returns("""
                            id\tid1
                            1\t10
                            1\t11
                            2\t10
                            2\t11
                            3\t10
                            3\t11
                            4\t12
                            4\t13
                            5\t14
                            5\t15
                            """);
        });
    }

    // Conditions that keep their place after the join: a master column that is no key, a random value, a value
    // that changes during the execution, a sub-query, a word that is no column, as the unit of extract(), and a
    // condition of the ON clause that reads no column, which the filter after the join evaluates once per
    // execution. A conjunct of the same filter that reads key columns only moves into the join on its own.
    @Test
    public void testConditionsThatStayAfterJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // m.y is no key: s.v * 10 > m.y keeps s row 11 (50) for m rows 1, 2 and 3, and s row 14 (70) for m
            // row 5, which = matches by its NULL key
            assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE s.v * 10 > m.y ORDER BY 1, 2")
                    .noLeakCheck()
                    .withPlanContaining("Filter filter: m.y<s.v*10")
                    .withPlanNotContaining("Hash Join Light\n          condition: s.k=m.k\n          filter:")
                    .returns("""
                            id\tid1
                            1\t11
                            2\t11
                            3\t11
                            5\t14
                            """);
            assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE s.v > m.k + rnd_int(0, 1, 0)")
                    .noLeakCheck()
                    .assertsPlanContaining("Filter filter: m.k+rnd_int(0,1,0)<s.v");
            // rnd_interval() reports a random value without being non-deterministic
            assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE s.ts + m.k < interval_start(rnd_interval())")
                    .noLeakCheck()
                    .assertsPlanContaining("Filter filter: s.ts+m.k<interval_start(rnd_interval())");
            assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE s.ts < m.k::TIMESTAMP + systimestamp()")
                    .noLeakCheck()
                    .assertsPlanContaining("Filter filter: s.ts<m.k::timestamp+systimestamp()");
            // max(v) is 9, and s.v * 2 + m.k exceeds it for s row 11 (11) only
            assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE s.v * 2 + m.k > (SELECT max(v) FROM s) ORDER BY 1, 2")
                    .noLeakCheck()
                    .withPlanContaining("Filter filter: s.v*2+m.k")
                    .returns("""
                            id\tid1
                            1\t11
                            2\t11
                            3\t11
                            """);
            // the hours of s.ts are all 0
            assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE extract(hour from s.ts) > m.k ORDER BY 1, 2")
                    .noLeakCheck()
                    .withPlanContaining("Filter filter: m.k<hour(s.ts)")
                    .returns("id\tid1\n");
            assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k AND now() > '2000-01-01' WHERE s.v > m.k ORDER BY 1, 2")
                    .noLeakCheck()
                    .withPlanContaining("filter: s.k<s.v", "Filter filter: 2000-01-01T00:00:00.000000Z<now()")
                    .returns("""
                            id\tid1
                            1\t11
                            2\t11
                            3\t11
                            4\t13
                            """);
            // s row 10 passes s.v > m.k OR s.v = 0 in the join, but 0 > m.y drops it; s row 11 passes both for
            // m rows 1, 2 and 3; s row 13 passes the first only, as 30 > 40 fails
            assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE (s.v > m.k OR s.v = 0) AND s.v * 10 > m.y ORDER BY 1, 2")
                    .noLeakCheck()
                    .withPlanContaining("filter: (s.k<s.v or s.v=0)", "Filter filter: m.y<s.v*10")
                    .returns("""
                            id\tid1
                            1\t11
                            2\t11
                            3\t11
                            """);
        });
    }

    // The rows of a condition that moves into the join match the rows of the same condition with a master
    // column that is no key, m.y * 0, which keeps it after the join, under GROUP BY, in a CTE, in UNION ALL,
    // under ORDER BY with LIMIT, under a negative LIMIT, which counts the rows and reads them again from the
    // top, on the slave side of a CROSS join, which reads it from the top for each of its rows, as the master
    // of another hash join, and under a window function.
    @Test
    public void testContexts() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String[] contexts = {
                    "SELECT m.id, count(*) c FROM m JOIN s ON s.k = m.k WHERE %s GROUP BY m.id ORDER BY 1",
                    "WITH j AS (SELECT m.id mid, s.id sid FROM m JOIN s ON s.k = m.k WHERE %s) SELECT * FROM j ORDER BY 1, 2",
                    "SELECT * FROM (SELECT m.id mid, s.id sid FROM m JOIN s ON s.k = m.k WHERE %s UNION ALL SELECT id, id FROM m) ORDER BY 1, 2",
                    "SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE %s ORDER BY 2 DESC, 1 LIMIT 3",
                    "SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE %s LIMIT -2",
                    "SELECT x.x, j.mid, j.sid FROM long_sequence(2) x CROSS JOIN (SELECT m.id mid, s.id sid FROM m JOIN s ON s.k = m.k WHERE %s) j",
                    "SELECT m.id, s.id, t.id FROM m JOIN s ON s.k = m.k JOIN s t ON t.v = s.v WHERE %s",
                    "SELECT m.id, s.id, row_number() OVER (PARTITION BY m.id ORDER BY s.id) rn FROM m JOIN s ON s.k = m.k WHERE %s",
            };
            for (String context : contexts) {
                final String sql = context.formatted("s.v > m.k");
                assertQuery(sql).noLeakCheck().assertsPlanContaining("filter: s.k<s.v");
                assertSqlCursors(context.formatted("s.v > m.k + m.y * 0"), sql);
            }
        });
    }

    // s.k = m.k AND s.k2 = m.k: the optimiser keeps s.k2 = m.k as the key and filters s by k = k2, and the
    // condition reads s.k2, the key column that the join has. m row 1 meets s rows 10 and 14, of which row 10
    // has v 5 > 1; m row 2 meets s rows 12 and 13, of which row 13 has v 3 > 2. The filter on s leaves its size
    // unknown, so the join hashes s and checks the condition for each row of the join.
    @Test
    public void testDerivedKeyColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (id INT, k INT)");
            execute("CREATE TABLE s (id INT, k INT, k2 INT, v INT)");
            execute("INSERT INTO m VALUES (1, 1), (2, 2)");
            execute("INSERT INTO s VALUES (10, 1, 1, 5), (11, 1, 2, 5), (12, 2, 2, 1), (13, 2, 2, 3), (14, 1, 1, 0)");
            assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k AND s.k2 = m.k WHERE s.v > m.k ORDER BY 1, 2")
                    .noLeakCheck()
                    .withPlanContaining("condition: s.k2=m.k", "filter: s.k2<s.v", "filter: k=k2")
                    .returns("""
                            id\tid1
                            1\t10
                            2\t13
                            """);
        });
    }

    // A table alias with a dot: the join metadata names the slave key column my.s.k, a name that resolves to that
    // column, so v > m.k moves as my.s.k < my.s.v. Per m row of k = 1, s rows 11, 12 and 13 have v above 1; for m
    // row 3 (k = 2), s row 14 (v 3) has. The quoted alias resolves to no column of the join, so a condition on
    // "my.s".v stays, and the filter after the join fails on the name, as it did before conditions moved.
    @Test
    public void testDottedAlias() throws Exception {
        assertMemoryLeak(() -> {
            createConjunctTables();
            assertQuery("SELECT m.id, v FROM m JOIN s \"my.s\" ON \"my.s\".k = m.k WHERE v > m.k ORDER BY 1, 2")
                    .noLeakCheck()
                    .withPlanContaining("filter: my.s.k<my.s.v")
                    .withPlanNotContaining("Filter filter")
                    .returns("""
                            id\tv
                            1\t4
                            1\t6
                            1\t9
                            2\t4
                            2\t6
                            2\t9
                            3\t3
                            """);
            final String sql = "SELECT m.id FROM m JOIN s \"my.s\" ON \"my.s\".k = m.k WHERE \"my.s\".v > m.k";
            assertExceptionNoLeakCheck(sql, sql.indexOf("\"my.s\".v"), "Invalid column: \"my.s\".v");
        });
    }

    // An empty side leaves the join empty, and the cursor counts no row.
    @Test
    public void testEmptySide() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE e (id INT, k INT, v INT, y INT)");
            assertQuery("SELECT m.id, e.id FROM m JOIN e ON e.k = m.k WHERE e.v > m.k")
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("filter: e.k<e.v")
                    .returns("id\tid1\n");
            assertCalculateSizeAfterEachRow("SELECT m.id, e.id FROM m JOIN e ON e.k = m.k WHERE e.v > m.k", 0);
            assertQuery("SELECT e.id, s.id FROM e JOIN s ON s.k = e.k WHERE s.v > e.k")
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("filter: s.k<s.v")
                    .returns("id\tid1\n");
            assertCalculateSizeAfterEachRow("SELECT e.id, s.id FROM e JOIN s ON s.k = e.k WHERE s.v > e.k", 0);
        });
    }

    // The performance property: when the join hashes the master, it evaluates the condition once per slave row
    // whose key the master has, s rows 10 to 15, not once per row of the join, 10, as it does when the condition
    // stays after the join, or when the join hashes the slave.
    @Test
    public void testEvaluatedOncePerSlaveRow() throws Exception {
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);
        assertMemoryLeak(() -> {
            createTables();
            // s.ts is 10us to 17us, above every m.k: the comparison drops only the NULL key of m row 5
            assertEvaluations(6, 8, "SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE test_timestamp_counter(s.ts) > m.k::TIMESTAMP");
            assertEvaluations(10, 8, "SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE test_timestamp_counter(s.ts) > m.k::TIMESTAMP + m.y * 0");
            // m.ts is 1us to 5us: m rows 2 and 3 pass for s.k = 1, m row 4 for s.k = 2
            assertEvaluations(10, 6, "SELECT s.id, m.id FROM s JOIN m ON m.k = s.k WHERE test_timestamp_counter(m.ts) > s.k::TIMESTAMP");
            assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE test_timestamp_counter(s.ts) > m.k::TIMESTAMP")
                    .noLeakCheck()
                    .assertsPlanContaining("filter: s.k::timestamp<test_timestamp_counter");
        });
    }

    // DOUBLE, FLOAT and array keys: the hash join compares their bits, which the condition may not rely on, so
    // it stays after the join. Their rows are those of testColumnTypesWithNull(), NULL being NaN.
    @Test
    public void testFloatingPointKeysStayAfterJoin() throws Exception {
        assertMemoryLeak(() -> {
            final String[][] types = {
                    {"DOUBLE", "1.5", "2.5"},
                    {"FLOAT", "1.5", "2.5"},
            };
            for (String[] type : types) {
                createKeyTypeTables(type[0], type[0], type[1], type[2]);
                assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE s.k2 != m.k ORDER BY 1, 2")
                        .noLeakCheck()
                        .withPlanContaining("Filter filter: s.k2!=m.k")
                        .returns(NULL_KEY_TYPE_ROWS);
                dropKeyTypeTables();
            }
            execute("CREATE TABLE m (id INT, k DOUBLE[])");
            execute("CREATE TABLE s (id INT, k DOUBLE[], k2 DOUBLE[])");
            assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE s.k2 != m.k")
                    .noLeakCheck()
                    .assertsPlanContaining("Filter filter: s.k2!=m.k");
        });
    }

    // A full-fat hash join, which copies its slave, keeps the condition after the join.
    @Test
    public void testFullFatJoinKeepsConditionAfterJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE s.v > m.k ORDER BY 1, 2";
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                assertQuery(sql)
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .assertsPlanContaining("Filter filter: m.k<s.v", "Hash Join\n");
            }
            assertQuery(sql)
                    .noLeakCheck()
                    .fullFatJoins(true)
                    .returns("""
                            id\tid1
                            1\t11
                            2\t11
                            3\t11
                            4\t13
                            """);
        });
    }

    // Tables of several page frames and partitions, with keys enough to grow the hash table and NULL keys among
    // them. "FROM m JOIN s" hashes m, the smaller side, and reads s row by row, and "FROM s JOIN m" hashes m as
    // its slave, for INT and SYMBOL keys. The rows match those of the same condition kept after the join, as
    // m.y * 0 or s.v * 0 reads a master column that is no key, and their count matches a count in Java.
    @Test
    public void testManyRows() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE m AS (
                        SELECT x::INT id, CASE WHEN x % 997 = 0 THEN NULL ELSE (x % 2_000)::INT END k, (x % 13)::INT y,
                            CASE WHEN x % 997 = 0 THEN NULL ELSE (x % 2_000)::SYMBOL END sk,
                            timestamp_sequence('2024-01-01'::TIMESTAMP, 2_000_000) ts
                        FROM long_sequence(5_000)
                    ) TIMESTAMP(ts) PARTITION BY HOUR
                    """);
            execute("""
                    CREATE TABLE s AS (
                        SELECT x::INT id, CASE WHEN x % 991 = 0 THEN NULL ELSE (x * 7 % 2_500)::INT END k, (x % 101)::INT v,
                            CASE WHEN x % 991 = 0 THEN NULL ELSE (x * 7 % 2_500)::SYMBOL END sk,
                            timestamp_sequence('2024-01-01'::TIMESTAMP, 500_000) ts
                        FROM long_sequence(20_000)
                    ) TIMESTAMP(ts) PARTITION BY HOUR
                    """);
            // the rows of m and s, as the CREATE TABLE statements above compute them, by x
            final LongFunction<Integer> masterKey = x -> x % 997 == 0 ? null : (int) (x % 2_000);
            final LongFunction<Integer> slaveKey = x -> x % 991 == 0 ? null : (int) (x * 7 % 2_500);
            assertManyRows(
                    "SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE s.v > m.k % 101",
                    "SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE s.v > m.k % 101 + m.y * 0",
                    "filter: s.k%101<s.v",
                    countPairs(masterKey, slaveKey, (mx, sx) -> masterKey.apply(mx) != null && sx % 101 > masterKey.apply(mx) % 101)
            );
            assertManyRows(
                    "SELECT s.id, m.id FROM s JOIN m ON m.k = s.k WHERE m.y > s.k % 13",
                    "SELECT s.id, m.id FROM s JOIN m ON m.k = s.k WHERE m.y > s.k % 13 + s.v * 0",
                    "filter: m.k%13<m.y",
                    countPairs(masterKey, slaveKey, (mx, sx) -> slaveKey.apply(sx) != null && mx % 13 > slaveKey.apply(sx) % 13)
            );
            // length() of a NULL symbol is -1, so the NULL keys, which = matches, pass s.v > -20
            assertManyRows(
                    "SELECT m.id, s.id FROM m JOIN s ON s.sk = m.sk WHERE s.v > length(m.sk) * 20",
                    "SELECT m.id, s.id FROM m JOIN s ON s.sk = m.sk WHERE s.v > length(m.sk) * 20 + m.y * 0",
                    "filter: length(s.sk)*20<s.v",
                    countPairs(masterKey, slaveKey, (mx, sx) -> sx % 101 > symbolLength(masterKey.apply(mx)) * 20)
            );
            assertManyRows(
                    "SELECT s.id, m.id FROM s JOIN m ON m.sk = s.sk WHERE m.y < length(s.sk) * 3",
                    "SELECT s.id, m.id FROM s JOIN m ON m.sk = s.sk WHERE m.y < length(s.sk) * 3 + s.v * 0",
                    "filter: m.y<length(m.sk)*3",
                    countPairs(masterKey, slaveKey, (mx, sx) -> mx % 13 < symbolLength(slaveKey.apply(sx)) * 3)
            );
        });
    }

    // Key columns of two types: equal values of TIMESTAMP and TIMESTAMP_NS, or of a SYMBOL and a STRING, can
    // compute differently, so the condition stays after the join and reads the master column. The rows are
    // those of testColumnTypesWithNull().
    @Test
    public void testMixedKeyTypesStayAfterJoin() throws Exception {
        assertMemoryLeak(() -> {
            final String[][] types = {
                    {"TIMESTAMP", "TIMESTAMP_NS", "'2024-01-01'", "'2024-01-02'"},
                    {"TIMESTAMP_NS", "TIMESTAMP", "'2024-01-01'", "'2024-01-02'"},
                    {"SYMBOL", "STRING", "'p'", "'q'"},
                    {"SYMBOL", "VARCHAR", "'p'", "'q'"},
                    {"STRING", "VARCHAR", "'p'", "'q'"},
                    {"VARCHAR", "SYMBOL", "'p'", "'q'"},
            };
            for (String[] type : types) {
                createKeyTypeTables(type[0], type[1], type[2], type[3]);
                // the condition is the only one, and the filter after the join holds it
                assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE s.k2 != m.k ORDER BY 1, 2")
                        .noLeakCheck()
                        .withPlanContaining("Filter filter: ")
                        .returns(NULL_KEY_TYPE_ROWS);
                dropKeyTypeTables();
            }
        });
    }

    // Two conjuncts move into the join and two stay after it, each pair as one AND. Every conjunct drops a row of
    // the join that the other three keep: s.v > m.k drops (1, 10), s.v < m.k + 8 drops (1, 12), s.v > m.y drops
    // (2, 11), and s.v + m.y < 10 drops (2, 13).
    @Test
    public void testSeveralConjuncts() throws Exception {
        assertMemoryLeak(() -> {
            createConjunctTables();
            assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE s.v > m.k AND s.v < m.k + 8 AND s.v > m.y AND s.v + m.y < 10 ORDER BY 1, 2")
                    .noLeakCheck()
                    .withPlanContaining("filter: (s.k<s.v and s.v<s.k+8)", "Filter filter: (m.y<s.v and s.v+m.y<10)")
                    .returns("""
                            id\tid1
                            1\t11
                            1\t13
                            3\t14
                            """);
        });
    }

    // s has more rows than m, so "FROM s JOIN m" hashes m, its slave, and checks the condition for each m row
    // of the hash chain of an s row: m.y > m.k * 10 keeps m rows 2 and 3 for s rows 10 and 11 (k = 1), and m
    // row 4 for s rows 12 and 13 (k = 2). The rows come in the order of s.
    @Test
    public void testSlaveHashed() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT s.id, m.id FROM s JOIN m ON m.k = s.k WHERE m.y > s.k * 10")
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("Hash Join Light", "condition: m.k=s.k", "filter: m.k*10<m.y")
                    .withPlanNotContaining("Filter filter")
                    .returns("""
                            id\tid1
                            10\t3
                            10\t2
                            11\t3
                            11\t2
                            12\t4
                            13\t4
                            """);
        });
    }

    // s has more rows than m, so the join hashes m and reads s row by row: s rows 11 and 13 pass s.v > s.k, and
    // the rows come in the order of s, each with the m rows of its key, newest first.
    @Test
    public void testSlaveReadRowByRow() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT m.id, s.id FROM m JOIN s ON s.k = m.k WHERE s.v > m.k")
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("Hash Join Light", "condition: s.k=m.k", "filter: s.k<s.v")
                    .withPlanNotContaining("Filter filter")
                    .returns("""
                            id\tid1
                            3\t11
                            2\t11
                            1\t11
                            4\t13
                            """);
        });
    }

    private static void assertCalculateSizeAfterEachRow(String sql, long rowCount) throws Exception {
        try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            for (long skip = 0; skip <= rowCount; skip++) {
                cursor.toTop();
                for (long i = 0; i < skip; i++) {
                    Assert.assertTrue(cursor.hasNext());
                }
                final RecordCursor.Counter counter = new RecordCursor.Counter();
                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                Assert.assertEquals(sql + ", skip " + skip, rowCount - skip, counter.get());
            }
        }
    }

    // Reads the rows of the query once, and checks the number of evaluations of test_timestamp_counter() and of rows
    private static void assertEvaluations(long expectedEvaluations, long expectedRows, String sql) throws Exception {
        TestTimestampCounterFactory.COUNTER.set(0);
        long rows = 0;
        try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            while (cursor.hasNext()) {
                rows++;
            }
        }
        Assert.assertEquals(sql, expectedRows, rows);
        Assert.assertEquals(sql, expectedEvaluations, TestTimestampCounterFactory.COUNTER.get());
    }

    // Counts the pairs of rows of m (x from 1 to 5,000) and s (x from 1 to 20,000) that the join matches, = matching
    // NULL to NULL, and that the condition keeps
    private static long countPairs(LongFunction<Integer> masterKey, LongFunction<Integer> slaveKey, BiPredicate<Long, Long> condition) {
        final Map<Integer, List<Long>> slaveRowsByKey = new HashMap<>();
        for (long x = 1; x <= 20_000; x++) {
            slaveRowsByKey.computeIfAbsent(slaveKey.apply(x), key -> new ArrayList<>()).add(x);
        }
        long count = 0;
        for (long x = 1; x <= 5_000; x++) {
            for (long slaveX : slaveRowsByKey.getOrDefault(masterKey.apply(x), List.of())) {
                if (condition.test(x, slaveX)) {
                    count++;
                }
            }
        }
        return count;
    }

    // m rows 1 and 2 have k = 1 and y 0 and 5, m row 3 has k = 2 and y 0. s rows 10 to 13 have k = 1 and v 1, 4, 9
    // and 6, s row 14 has k = 2 and v 3.
    private static void createConjunctTables() throws Exception {
        execute("CREATE TABLE m (id INT, k INT, y INT)");
        execute("CREATE TABLE s (id INT, k INT, v INT)");
        execute("INSERT INTO m VALUES (1, 1, 0), (2, 1, 5), (3, 2, 0)");
        execute("INSERT INTO s VALUES (10, 1, 1), (11, 1, 4), (12, 1, 9), (13, 1, 6), (14, 2, 3)");
    }

    // m.k has the master type, s.k and s.k2 the slave type. m rows 1, 2 and 3 have v1, v2 and NULL; s rows 10 to
    // 16 have (k, k2) of (v1, v1), (v1, v2), (v2, v2), (v2, NULL), (NULL, v1), (NULL, NULL) and (v1, NULL).
    private static void createKeyTypeTables(String masterType, String slaveType, String v1, String v2) throws Exception {
        execute("CREATE TABLE m (id INT, k " + masterType + ")");
        execute("CREATE TABLE s (id INT, k " + slaveType + ", k2 " + slaveType + ")");
        execute("INSERT INTO m VALUES (1, " + v1 + "), (2, " + v2 + "), (3, NULL)");
        execute("INSERT INTO s VALUES (10, " + v1 + ", " + v1 + "), (11, " + v1 + ", " + v2 + "), (12, " + v2 + ", " + v2 + "), "
                + "(13, " + v2 + ", NULL), (14, NULL, " + v1 + "), (15, NULL, NULL), (16, " + v1 + ", NULL)");
    }

    // m rows 1, 2 and 3 share k = 1, m row 4 has k = 2 and m row 5 a NULL k. s rows 10 and 11 have k = 1, s rows
    // 12 and 13 k = 2, s rows 14 and 15 a NULL k, and s rows 16 and 17 k = 3, which m does not have.
    private static void createTables() throws Exception {
        execute("CREATE TABLE m (id INT, k INT, y INT, ts TIMESTAMP)");
        execute("CREATE TABLE s (id INT, k INT, v INT, ts TIMESTAMP)");
        execute("INSERT INTO m VALUES (1, 1, 10, 1::TIMESTAMP), (2, 1, 20, 2::TIMESTAMP), (3, 1, 30, 3::TIMESTAMP), "
                + "(4, 2, 40, 4::TIMESTAMP), (5, NULL, 50, 5::TIMESTAMP)");
        execute("INSERT INTO s VALUES (10, 1, 0, 10::TIMESTAMP), (11, 1, 5, 11::TIMESTAMP), (12, 2, 1, 12::TIMESTAMP), "
                + "(13, 2, 3, 13::TIMESTAMP), (14, NULL, 7, 14::TIMESTAMP), (15, NULL, NULL, 15::TIMESTAMP), "
                + "(16, 3, 9, 16::TIMESTAMP), (17, 3, 2, 17::TIMESTAMP)");
    }

    private static void dropKeyTypeTables() throws Exception {
        execute("DROP TABLE m");
        execute("DROP TABLE s");
    }

    // length() of the SYMBOL that the key makes, -1 for NULL
    private static int symbolLength(Integer key) {
        return key == null ? -1 : String.valueOf(key).length();
    }

    // The join moves the condition into itself, returns the rows of the reference, which keeps the condition after
    // the join, in the same order, and as many as expected
    private void assertManyRows(String sql, String reference, String movedFilter, long expectedRowCount) throws Exception {
        assertQuery(sql).noLeakCheck().assertsPlanContaining(movedFilter);
        assertQuery(sql).noLeakCheck().assertsPlanNotContaining("Filter filter");
        assertQuery(reference).noLeakCheck().assertsPlanContaining("Filter filter");
        assertSqlCursors(reference, sql);
        long rowCount = 0;
        try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            while (cursor.hasNext()) {
                rowCount++;
            }
        }
        Assert.assertEquals(sql, expectedRowCount, rowCount);
    }
}
