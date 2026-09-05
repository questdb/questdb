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

import io.questdb.cairo.ColumnType;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class JoinCastTest extends AbstractCairoTest {

    @Test
    public void testCastsOverExpressions() throws Exception {
        assertMemoryLeak(() -> {
            createTables("SYMBOL", "STRING");
            assertQuery("SELECT l.id, r.id FROM l JOIN r ON l.k = lower(r.k)::symbol ORDER BY l.id, r.id")
                    .withPlanContaining("Cross Join")
                    .returns(innerJoinExpected());
        });
    }

    @Test
    public void testMixedEncodingCastsPreserveReplacementCharacters() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE l (k STRING)");
            execute("CREATE TABLE r (k VARCHAR)");
            execute("INSERT INTO l VALUES ('\uD800')");
            execute("INSERT INTO r VALUES ('?')");
            assertQuery("SELECT count(*) FROM l JOIN r ON l.k::string = r.k::varchar")
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Cross Join")
                    .returns("count\n0\n");
        });
    }

    @Test
    public void testStringCasts() throws Exception {
        assertMemoryLeak(() -> {
            final int[] types = {ColumnType.SYMBOL, ColumnType.STRING, ColumnType.VARCHAR};
            for (int leftType : types) {
                for (int rightType : types) {
                    createTables(ColumnType.nameOf(leftType), ColumnType.nameOf(rightType));
                    boolean isSameEncoding = (leftType == ColumnType.VARCHAR) == (rightType == ColumnType.VARCHAR);
                    for (int castType : types) {
                        String type = ColumnType.nameOf(castType);
                        boolean isLeftCastLossless = (leftType == ColumnType.VARCHAR) == (castType == ColumnType.VARCHAR);
                        boolean isRightCastLossless = (rightType == ColumnType.VARCHAR) == (castType == ColumnType.VARCHAR);
                        assertInnerJoin("l.k = r.k::" + type, isSameEncoding && isRightCastLossless);
                        assertInnerJoin("l.k::" + type + " = r.k", isSameEncoding && isLeftCastLossless);
                        assertInnerJoin("l.k::" + type + " = r.k::" + type, isSameEncoding && isLeftCastLossless && isRightCastLossless);
                    }
                    assertInnerJoin("l.k::symbol::string = r.k::symbol::string", leftType != ColumnType.VARCHAR && rightType != ColumnType.VARCHAR);
                    execute("DROP TABLE l");
                    execute("DROP TABLE r");
                }
            }
        });
    }

    @Test
    public void testStringCastsInAsOfJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE l (k SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE r (k SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO l VALUES ('a', 2), ('b', 3), (null, 4), ('c', 5)");
            execute("INSERT INTO r VALUES ('a', 1), ('b', 2), (null, 3)");
            assertQuery("SELECT l.k lk, r.k rk FROM l ASOF JOIN r ON l.k = r.k::string")
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("AsOf Join")
                    .returns("""
                            lk\trk
                            a\ta
                            b\tb
                            \t
                            c\t
                            """);
        });
    }

    @Test
    public void testStringCastsInCrossJoinWhere() throws Exception {
        assertMemoryLeak(() -> {
            createTables("SYMBOL", "STRING");
            assertQuery("SELECT l.id, r.id FROM l CROSS JOIN r WHERE l.k = r.k::symbol ORDER BY l.id, r.id")
                    .withPlanContaining("Hash Join")
                    .returns(innerJoinExpected());
        });
    }

    @Test
    public void testStringCastsInFullFatJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables("SYMBOL", "STRING");
            assertQuery("SELECT l.id, r.id FROM l JOIN r ON l.k::string = r.k::string ORDER BY l.id, r.id")
                    .fullFatJoins()
                    .expectSize()
                    .returns(innerJoinExpected());
        });
    }

    @Test
    public void testStringCastsInOuterJoins() throws Exception {
        assertMemoryLeak(() -> {
            createTables("SYMBOL", "STRING");
            assertQuery("""
                    SELECT coalesce(l.id, -1) lid, coalesce(r.id, -1) rid
                    FROM l LEFT JOIN r ON l.k::string = r.k::string ORDER BY lid, rid
                    """)
                    .withPlanContaining("Hash Left Outer Join")
                    .returns("""
                            lid\trid
                            1\t10
                            1\t11
                            2\t10
                            2\t11
                            3\t12
                            4\t13
                            5\t14
                            6\t-1
                            """);
            assertQuery("""
                    SELECT coalesce(l.id, -1) lid, coalesce(r.id, -1) rid
                    FROM l RIGHT JOIN r ON l.k::string = r.k::string ORDER BY lid, rid
                    """)
                    .withPlanContaining("Hash Right Outer Join")
                    .returns("""
                            lid\trid
                            -1\t15
                            1\t10
                            1\t11
                            2\t10
                            2\t11
                            3\t12
                            4\t13
                            5\t14
                            """);
            assertQuery("""
                    SELECT coalesce(l.id, -1) lid, coalesce(r.id, -1) rid
                    FROM l FULL JOIN r ON l.k::string = r.k::string ORDER BY lid, rid
                    """)
                    .withPlanContaining("Hash Full Outer Join")
                    .returns("""
                            lid\trid
                            -1\t15
                            1\t10
                            1\t11
                            2\t10
                            2\t11
                            3\t12
                            4\t13
                            5\t14
                            6\t-1
                            """);
        });
    }

    @Test
    public void testStringCastsInSubqueries() throws Exception {
        assertMemoryLeak(() -> {
            createTables("SYMBOL", "STRING");
            assertQuery("""
                    WITH a AS (SELECT k renamed, id FROM l), b AS (SELECT k renamed, id FROM r)
                    SELECT a.id, b.id FROM a JOIN b ON a.renamed::string = b.renamed::string
                    ORDER BY a.id, b.id
                    """)
                    .withPlanContaining("Hash Join")
                    .returns(innerJoinExpected());
        });
    }

    @Test
    public void testStringCastsInThreeWayJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables("SYMBOL", "STRING");
            execute("CREATE TABLE s (k STRING, id INT)");
            execute("INSERT INTO s VALUES ('a', 20), (null, 21), ('', 22), ('é💹', 23)");
            assertQuery("""
                    SELECT l.id, r.id, s.id FROM l
                    JOIN r ON l.k::string = r.k
                    JOIN s ON s.k::symbol = r.k::string
                    ORDER BY l.id, r.id, s.id
                    """)
                    .withPlanContaining("Hash Join")
                    .returns("""
                            id\tid1\tid2
                            1\t10\t20
                            1\t11\t20
                            2\t10\t20
                            2\t11\t20
                            3\t12\t21
                            4\t13\t22
                            5\t14\t23
                            """);
        });
    }

    @Test
    public void testStringCastsPreserveOtherJoinConditions() throws Exception {
        assertMemoryLeak(() -> {
            createTables("SYMBOL", "STRING");
            assertQuery("""
                    SELECT l.id, r.id FROM l JOIN r ON l.k = r.k::string AND l.id < 2 AND r.id < 11
                    ORDER BY l.id, r.id
                    """)
                    .withPlanContaining("Hash Join")
                    .returns("""
                            id\tid1
                            1\t10
                            """);
        });
    }

    @Test
    public void testStringCastsPreservePostJoinFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables("SYMBOL", "STRING");
            assertQuery("""
                    SELECT l.id, r.id FROM l JOIN r ON l.k = r.k::string AND l.id % 2 = r.id % 2
                    ORDER BY l.id, r.id
                    """)
                    .withPlanContaining("Hash Join", "Filter")
                    .returns("""
                            id\tid1
                            1\t11
                            2\t10
                            """);
        });
    }

    @Test
    public void testStringCastsPreserveWhereAfterOuterJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables("SYMBOL", "STRING");
            assertQuery("""
                    SELECT l.id, r.id FROM l FULL JOIN r ON l.k::string = r.k::string
                    WHERE l.id = 1 ORDER BY l.id, r.id
                    """)
                    .withPlanContaining("Hash Full Outer Join")
                    .returns("""
                            id\tid1
                            1\t10
                            1\t11
                            """);
        });
    }

    @Test
    public void testSymbolCastInLimitedSelfJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (symbol SYMBOL, id INT)");
            execute("INSERT INTO trades VALUES ('a', 1), ('a', 2), ('b', 3), (null, 4)");
            assertQuery("""
                    SELECT t1.id, t2.id
                    FROM (trades LIMIT 10_000) t1
                    INNER JOIN (trades LIMIT 10_000) t2 ON t1.symbol = t2.symbol::symbol
                    ORDER BY t1.id, t2.id
                    """)
                    .withPlanContaining("Hash Join Light")
                    .returns("""
                            id\tid1
                            1\t1
                            1\t2
                            2\t1
                            2\t2
                            3\t3
                            4\t4
                            """);
        });
    }

    @Test
    public void testSymbolToStringCastInLimitedSelfJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (symbol SYMBOL, id INT)");
            execute("INSERT INTO trades VALUES ('a', 1), ('a', 2), ('b', 3), (null, 4)");
            assertQuery("""
                    SELECT t1.id, t2.id
                    FROM (trades LIMIT 10_000) t1
                    INNER JOIN (trades LIMIT 10_000) t2 ON t1.symbol = t2.symbol::string
                    ORDER BY t1.id, t2.id
                    """)
                    .withPlanContaining("Hash Join Light")
                    .returns("""
                            id\tid1
                            1\t1
                            1\t2
                            2\t1
                            2\t2
                            3\t3
                            4\t4
                            """);
        });
    }

    @Test
    public void testUtf8CastsPreserveReplacementCharacters() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE l (k STRING)");
            execute("CREATE TABLE r (k STRING)");
            execute("INSERT INTO l VALUES ('\uD800')");
            execute("INSERT INTO r VALUES ('\uD801')");
            assertQuery("SELECT count(*) FROM l JOIN r ON l.k::varchar = r.k::varchar")
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Cross Join")
                    .returns("count\n1\n");
            assertQuery("SELECT count(*) FROM l JOIN r ON l.k::varchar::string = r.k::varchar::string")
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Cross Join")
                    .returns("count\n1\n");
        });
    }

    @Test
    public void testValueChangingCasts() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE l (k INT, id INT)");
            execute("CREATE TABLE r (k STRING, id INT)");
            execute("INSERT INTO l VALUES (1, 1), (2, 2), (null, 3)");
            execute("INSERT INTO r VALUES ('01', 10), ('1', 11), ('2', 12), (null, 13)");
            assertQuery("SELECT l.id, r.id FROM l JOIN r ON l.k::string = r.k ORDER BY l.id, r.id")
                    .withPlanContaining("Cross Join")
                    .returns("""
                            id\tid1
                            1\t11
                            2\t12
                            3\t13
                            """);
            assertQuery("SELECT l.id, r.id FROM l JOIN r ON l.k = r.k::int ORDER BY l.id, r.id")
                    .withPlanContaining("Cross Join")
                    .returns("""
                            id\tid1
                            1\t10
                            1\t11
                            2\t12
                            3\t13
                            """);
        });
    }

    private static String innerJoinExpected() {
        return """
                id\tid1
                1\t10
                1\t11
                2\t10
                2\t11
                3\t12
                4\t13
                5\t14
                """;
    }

    private void assertInnerJoin(String condition, boolean isHashJoinExpected) throws Exception {
        assertQuery("SELECT l.id, r.id FROM l JOIN r ON " + condition + " ORDER BY l.id, r.id")
                .withPlanContaining(isHashJoinExpected ? "Hash Join" : "Cross Join")
                .returns(innerJoinExpected());
    }

    private void createTables(String leftType, String rightType) throws Exception {
        execute("CREATE TABLE l (k " + leftType + ", id INT)");
        execute("CREATE TABLE r (k " + rightType + ", id INT)");
        execute("INSERT INTO l VALUES ('a', 1), ('a', 2), (null, 3), ('', 4), ('é💹', 5), ('left', 6)");
        execute("INSERT INTO r VALUES ('a', 10), ('a', 11), (null, 12), ('', 13), ('é💹', 14), ('right', 15)");
    }
}
