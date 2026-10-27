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

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class OuterJoinTest extends AbstractCairoTest {
    @Test
    public void testBetweenAndInListOverRuntimeValuesStayRuntimeOn() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setInt(0, 1);
            final String matched = """
                    lid\txid
                    1\t10
                    1\t10
                    1\t11
                    1\t11
                    3\t12
                    4\t13
                    """;
            final String[][] predicates = {
                    {"$1 BETWEEN 0 AND 2", "$0::int between 0 and 2", matched},
                    {"$1 IN (2, 3)", "$0::int in [2,3]", "lid\txid\n"},
                    {"now() BETWEEN '2020' AND '2100'", "now() between 1577836800000000 and 4102444800000000", matched},
                    {"now() IN ('2020', '2030')", "now() in [1577836800000000,1893456000000000]", "lid\txid\n"}
            };
            for (boolean isFullFat : new boolean[]{false, true}) {
                final String light = isFullFat ? "" : " Light";
                for (String[] predicate : predicates) {
                    final String query = "SELECT l.id lid, x.id xid FROM lp_outer_l l JOIN lp_outer_r r ON r.k = l.k AND "
                            + predicate[0] + " %s lp_outer_r x ON x.k = r.k ORDER BY lid, xid";
                    assertRows(query.formatted("JOIN"), isFullFat, """
                            SelectedRecord
                                    Filter filter: %s
                                        Hash Join%s
                                          condition: x.k=r.k
                            """.formatted(predicate[1], light), predicate[2]);
                    assertRows(query.formatted("LEFT JOIN"), isFullFat, """
                            Hash Left Outer Join%s
                                      condition: x.k=r.k
                                        Filter filter: %s
                                            Hash Join%s
                            """.formatted(light, predicate[1], light), predicate[2]);
                }
            }
        });
    }

    @Test
    public void testBindVariableInnerOnBeforeRightOrFullJoin() throws Exception {
        assertInnerOnBeforeRightOrFullJoin(":has_match");
    }

    @Test
    public void testConstantFalseOnEmptiesUnmatchableSide() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            for (boolean isFullFat : new boolean[]{false, true}) {
                final String light = isFullFat ? "" : " Light";
                assertRows("SELECT l.id lid,r.id rid FROM lp_outer_l l LEFT JOIN "
                                + "(SELECT * FROM lp_outer_r UNION ALL SELECT * FROM lp_outer_r) r ON l.k=r.k AND false ORDER BY lid,rid",
                        isFullFat, """
                                Hash Left Outer Join%s
                                          condition: r.k=l.k
                                          filter: false
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_outer_l
                                            Hash
                                                Empty table
                                """.formatted(light),
                        """
                                lid	rid
                                1	null
                                2	null
                                3	null
                                4	null
                                5	null
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM (SELECT * FROM lp_outer_l UNION ALL SELECT * FROM lp_outer_l) l "
                                + "RIGHT JOIN lp_outer_r r ON l.k=r.k AND false ORDER BY lid,rid",
                        isFullFat, """
                                Hash Right Outer Join%s
                                          condition: r.k=l.k
                                          filter: false
                                            Empty table
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_outer_r
                                """.formatted(light),
                        """
                                lid	rid
                                null	10
                                null	11
                                null	12
                                null	13
                                null	14
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_outer_l l LEFT JOIN lp_outer_r r ON false ORDER BY lid,rid",
                        isFullFat, """
                                Nested Loop Left Join
                                          filter: false
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_outer_l
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_outer_r
                                """,
                        """
                                lid	rid
                                1	null
                                2	null
                                3	null
                                4	null
                                5	null
                                """);
            }
        });
    }

    @Test
    public void testConstantInnerOnBeforeRightOrFullJoin() throws Exception {
        assertInnerOnBeforeRightOrFullJoin("false");
    }

    @Test
    public void testConstantInnerOnBeforeRightOrFullJoinErrors() throws Exception {
        assertMemoryLeak(() -> {
            createInnerOnRows();
            final String prefix = "SELECT a.id FROM a JOIN c ON ";
            for (String join : new String[]{"RIGHT", "FULL"}) {
                assertQuery(prefix + "NULL " + join + " JOIN b ON a.k = b.k").noLeakCheck()
                        .fails(29, "boolean expression expected");
                assertQuery(prefix + "1 " + join + " JOIN b ON a.k = b.k").noLeakCheck()
                        .fails(29, "boolean expression expected");
                assertQuery(prefix + "true JOIN c c2 ON NULL " + join + " JOIN b ON a.k = b.k").noLeakCheck()
                        .fails(47, "boolean expression expected");
                assertQuery(prefix + "1 " + join + " JOIN b ON a.k = b.k WHERE true").noLeakCheck()
                        .fails(29, "expression type mismatch, expected: BOOLEAN, actual: INT");
                assertQuery(prefix + "NULL JOIN c c2 ON 1 " + join + " JOIN b ON a.k = b.k").noLeakCheck()
                        .fails(29, "expression type mismatch, expected: BOOLEAN, actual: NULL");
                for (String on : new String[]{"1 ", "true "}) {
                    final String sql = prefix + on + join + " JOIN b ON a.k = b.k WHERE 1";
                    assertQuery(sql).noLeakCheck().fails(sql.length() - 1, "expression type mismatch, expected: BOOLEAN, actual: INT");
                }
                final String keyMismatch = prefix + "1 + true " + join + " JOIN b ON a.k = b.l";
                assertQuery(keyMismatch).noLeakCheck().fails(keyMismatch.length() - 3, "join column type mismatch");
                final String twoTypeErrors = prefix + "1 + true " + join + " JOIN b ON a.k = b.k JOIN c c2 ON 2 + true";
                assertQuery(twoTypeErrors).noLeakCheck()
                        .fails(twoTypeErrors.lastIndexOf('+'), "there is no matching operator `+` with the argument types: INT + BOOLEAN");
                assertQuery(prefix + "(SELECT false) " + join + " JOIN b ON a.k = b.k").noLeakCheck()
                        .fails(30, "query is not allowed here");
            }
        });
    }

    @Test
    public void testConstantInnerOnFiltersItsStepLikeRuntimeOnWithLeftJoin() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setInt(0, 1);
            final String query = "SELECT l.id lid, x.id xid FROM lp_outer_l l JOIN lp_outer_r r ON r.k = l.k AND %s = %d "
                    + "LEFT JOIN lp_outer_r x ON x.k = r.k ORDER BY lid, xid";
            for (boolean isFullFat : new boolean[]{false, true}) {
                final String light = isFullFat ? "" : " Light";
                assertRows(query.formatted("1", 0), isFullFat, """
                        Hash Left Outer Join%s
                                  condition: x.k=r.k
                                    Empty table
                        """.formatted(light), "lid\txid\n");
                assertRows(query.formatted("$1", 0), isFullFat, """
                        Hash Left Outer Join%s
                                  condition: x.k=r.k
                                    Filter filter: $0::int=0
                                        Hash Join%s
                        """.formatted(light, light), "lid\txid\n");
                final String matched = """
                        lid\txid
                        1\t10
                        1\t10
                        1\t11
                        1\t11
                        3\t12
                        4\t13
                        """;
                assertRows(query.formatted("1", 1), isFullFat, """
                        Hash Left Outer Join%s
                                  condition: x.k=r.k
                                    Hash Join%s
                        """.formatted(light, light), matched);
                assertRows(query.formatted("$1", 1), isFullFat, """
                        Hash Left Outer Join%s
                                  condition: x.k=r.k
                                    Filter filter: $0::int=1
                                        Hash Join%s
                        """.formatted(light, light), matched);
            }
        });
    }

    @Test
    public void testConstantInnerOnsAroundRightOrFullJoin() throws Exception {
        assertMemoryLeak(() -> {
            createInnerOnRows();
            final String rightRows = """
                    aid\tbid
                    null\t20
                    null\t21
                    """;
            final String select = "SELECT a.id aid, b.id bid FROM a JOIN c ON ";
            for (boolean isFullFat : new boolean[]{false, true}) {
                for (String join : new String[]{"RIGHT", "FULL"}) {
                    final String outerJoin = "Hash " + (join.equals("RIGHT") ? "Right" : "Full") + " Outer Join" + (isFullFat ? "" : " Light");
                    final String on = " " + join + " JOIN b ON a.k = b.k";
                    assertRows(select + "true JOIN c c2 ON 1 = 2" + on + " ORDER BY aid, bid", isFullFat, """
                            %s
                                      condition: b.k=a.k
                                        Empty table
                            """.formatted(outerJoin), rightRows);
                    assertRows(select + "1 = 2 JOIN c c2 ON true" + on + " ORDER BY aid, bid", isFullFat, """
                            %s
                                      condition: b.k=a.k
                                        Cross Join
                                            Empty table
                            """.formatted(outerJoin), rightRows);
                    assertRows(select + "NULL AND true" + on + " ORDER BY aid, bid", isFullFat, """
                            %s
                                      condition: b.k=a.k
                                        Empty table
                            """.formatted(outerJoin), rightRows);
                    assertRows(select + "1 = 1" + on + " WHERE 2 > 1 ORDER BY aid, bid", isFullFat, """
                            %s
                                      condition: b.k=a.k
                                        Cross Join
                                            PageFrame
                            """.formatted(outerJoin), """
                            aid\tbid
                            10\t20
                            11\t21
                            """);
                    assertRows(select + "1 = 2" + on + " JOIN c c2 ON 2 > 1 ORDER BY aid, bid", isFullFat, """
                            Cross Join
                                        %s
                                          condition: b.k=a.k
                                            Empty table
                            """.formatted(outerJoin), rightRows);
                    assertRows(select + "true" + on + " JOIN c c2 ON 1 = 2 ORDER BY aid, bid", isFullFat, """
                            SelectedRecord
                                    Empty table
                            """, "aid\tbid\n");
                    assertRows(select + "false" + on + " WHERE (SELECT true) ORDER BY aid, bid", isFullFat, null, rightRows);
                }
            }
        });
    }

    @Test
    public void testConstantNullInnerOnBeforeRightOrFullJoinWithFalseWhere() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            for (int joinMode = 0; joinMode < 2; joinMode++) {
                final String join = joinMode == 0 ? "RIGHT" : "FULL";
                assertQuery("SELECT l.id FROM lp_outer_l l JOIN lp_outer_r r ON l.k = r.k AND NULL "
                        + join + " JOIN lp_outer_r x ON r.k = x.k WHERE false")
                        .noLeakCheck().returns("id\n");
            }
        });
    }

    @Test
    public void testEmptyInputsAndConstantFalsePreserveOuterRows() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            {
                final String join = " RIGHT JOIN ";
                for (boolean isFullFat : new boolean[]{false, true}) {
                    assertRows("SELECT l.id lid,r.id rid FROM lp_outer_l l" + join
                                    + "lp_outer_r r ON l.k=r.k AND false ORDER BY lid,rid", isFullFat, null,
                            """
                                    lid	rid
                                    null	10
                                    null	11
                                    null	12
                                    null	13
                                    null	14
                                    """);
                    assertRows("SELECT l.id lid,r.id rid FROM (SELECT * FROM lp_outer_l WHERE false) l" + join
                                    + "lp_outer_r r ON l.k=r.k ORDER BY lid,rid", isFullFat, null,
                            """
                                    lid	rid
                                    null	10
                                    null	11
                                    null	12
                                    null	13
                                    null	14
                                    """);
                    assertRows("SELECT l.id lid,r.id rid FROM lp_outer_l l" + join
                                    + "(SELECT * FROM lp_outer_r WHERE false) r ON l.k=r.k ORDER BY lid,rid", isFullFat, null,
                            "lid\trid\n");
                    assertRows("SELECT l.id lid,r.id rid FROM (SELECT * FROM lp_outer_l WHERE false) l" + join
                                    + "(SELECT * FROM lp_outer_r WHERE false) r ON l.k=r.k ORDER BY lid,rid", isFullFat, null,
                            "lid\trid\n");
                }
            }
            {
                final String join = " FULL JOIN ";
                for (boolean isFullFat : new boolean[]{false, true}) {
                    assertRows("SELECT l.id lid,r.id rid FROM lp_outer_l l" + join
                                    + "lp_outer_r r ON l.k=r.k AND false ORDER BY lid,rid", isFullFat, null,
                            """
                                    lid	rid
                                    null	10
                                    null	11
                                    null	12
                                    null	13
                                    null	14
                                    1	null
                                    2	null
                                    3	null
                                    4	null
                                    5	null
                                    """);
                    assertRows("SELECT l.id lid,r.id rid FROM (SELECT * FROM lp_outer_l WHERE false) l" + join
                                    + "lp_outer_r r ON l.k=r.k ORDER BY lid,rid", isFullFat, null,
                            """
                                    lid	rid
                                    null	10
                                    null	11
                                    null	12
                                    null	13
                                    null	14
                                    """);
                    assertRows("SELECT l.id lid,r.id rid FROM lp_outer_l l" + join
                                    + "(SELECT * FROM lp_outer_r WHERE false) r ON l.k=r.k ORDER BY lid,rid", isFullFat, null,
                            """
                                    lid	rid
                                    1	null
                                    2	null
                                    3	null
                                    4	null
                                    5	null
                                    """);
                    assertRows("SELECT l.id lid,r.id rid FROM (SELECT * FROM lp_outer_l WHERE false) l" + join
                                    + "(SELECT * FROM lp_outer_r WHERE false) r ON l.k=r.k ORDER BY lid,rid", isFullFat, null,
                            "lid\trid\n");
                }
            }
        });
    }

    @Test
    public void testImpliedGeoHashKeyNullExtendsByOnClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ga (id INT, w GEOHASH(4c))");
            execute("CREATE TABLE gb (id INT, x INT, k GEOHASH(4c))");
            execute("CREATE TABLE gc (id INT, k GEOHASH(4c))");
            execute("INSERT INTO ga VALUES (1, NULL), (2, #u33d), (3, #u33e)");
            execute("INSERT INTO gb VALUES (10, 2, #u33d), (11, 0, NULL), (12, 3, #zzzz), (13, 3, NULL)");
            execute("INSERT INTO gc VALUES (20, NULL), (21, #u33d), (22, #zzzz)");
            final String query = "SELECT a.id, b.id, c.id FROM ga a %s JOIN gb b ON b.x > 1 JOIN gc c ON a.w = c.k AND b.k = c.k ORDER BY 1, 2, 3";
            assertQuery(query.formatted("LEFT"))
                    .withPlanContaining("Hash Left Outer Join", "condition: b.k=a.w")
                    .returns("""
                            id\tid1\tid2
                            1\t13\t20
                            2\t10\t21
                            """);
            final String nullExtended = """
                    id\tid1\tid2
                    null\t11\t20
                    1\t13\t20
                    2\t10\t21
                    """;
            assertQuery(query.formatted("RIGHT"))
                    .withPlanContaining("Hash Right Outer Join", "condition: b.k=a.w")
                    .returns(nullExtended);
            assertQuery(query.formatted("FULL"))
                    .withPlanContaining("Hash Full Outer Join", "condition: b.k=a.w")
                    .returns(nullExtended);
        });
    }

    @Test
    public void testImpliedKeyFullJoinNullExtendsByOnClause() throws Exception {
        assertMemoryLeak(() -> {
            createImpliedKeyRows();
            assertQuery("SELECT a.id, b.id, c.id FROM ik_a a FULL JOIN ik_b b ON b.x > 1 JOIN ik_c c ON a.w = c.k AND b.k = c.k ORDER BY 1, 2, 3")
                    .withPlanContaining("Hash Full Outer Join", "condition: b.k=a.w")
                    .returns("""
                            id\tid1\tid2
                            null\t11\t20
                            1\t13\t20
                            2\t10\t21
                            4\t13\t20
                            """);
            assertQuery("SELECT a.id, b.id, c.id FROM ik_a a FULL JOIN ik_b2 b ON b.x > 1 JOIN ik_c c ON a.w = c.k AND b.k = c.k ORDER BY 1, 2, 3")
                    .returns("""
                            id\tid1\tid2
                            null\t11\t20
                            2\t10\t21
                            """);
            assertQuery("SELECT a.id, b.id, c.id FROM ik_a a FULL JOIN ik_b b ON a.x = 1 JOIN ik_c c ON a.w = c.k AND b.k = c.k ORDER BY 1, 2, 3")
                    .returns("""
                            id\tid1\tid2
                            1\t11\t20
                            1\t13\t20
                            2\t10\t21
                            4\tnull\t20
                            """);
        });
    }

    @Test
    public void testImpliedKeyKeepsCrossInputBeforeOuterStep() throws Exception {
        assertMemoryLeak(() -> {
            createImpliedCrossKeyRows();
            final String query = "SELECT a.id, m.id, b.id, c.id FROM ia a CROSS JOIN im m %s JOIN ib b ON b.i = a.l "
                    + "JOIN ic c ON c.x = m.k AND c.x = b.k ORDER BY 1, 2, 3, 4";
            final String expected = """
                    id\tid1\tid2\tid3
                    1\t10\t20\t30
                    1\t12\t23\t32
                    2\t11\t21\t31
                    3\t12\t24\t32
                    """;
            assertQuery(query.formatted("RIGHT"))
                    .withPlanContaining("Hash Right Outer Join", "condition: b.k=m.k", "filter: a.l=b.i")
                    .returns(expected);
            assertQuery(query.formatted("FULL"))
                    .withPlanContaining("Hash Full Outer Join", "condition: b.k=m.k", "filter: a.l=b.i")
                    .returns(expected);
            assertQuery(query.formatted("LEFT"))
                    .withPlanContaining("Hash Left Outer Join", "condition: b.k=m.k", "filter: a.l=b.i")
                    .returns(expected);
        });
    }

    @Test
    public void testImpliedKeyKeepsCrossInputBeforeUnkeyedOuterStep() throws Exception {
        assertMemoryLeak(() -> {
            createImpliedCrossKeyRows();
            final String query = "SELECT a.id, m.id, b.id, c.id FROM ia a CROSS JOIN im m %s JOIN ib b ON b.i <= a.l "
                    + "JOIN ic c ON c.x = m.k AND c.x = b.k ORDER BY 1, 2, 3, 4";
            final String expected = """
                    id\tid1\tid2\tid3
                    1\t10\t20\t30
                    1\t12\t23\t32
                    2\t10\t20\t30
                    2\t11\t21\t31
                    2\t12\t23\t32
                    3\t12\t24\t32
                    """;
            assertQuery(query.formatted("RIGHT"))
                    .withPlanContaining("Hash Right Outer Join", "condition: b.k=m.k", "filter: a.l>=b.i")
                    .returns(expected);
            assertQuery(query.formatted("FULL"))
                    .withPlanContaining("Hash Full Outer Join", "condition: b.k=m.k", "filter: a.l>=b.i")
                    .returns(expected);
        });
    }

    @Test
    public void testImpliedKeyLeftJoinNullExtendsByOnClause() throws Exception {
        assertMemoryLeak(() -> {
            createImpliedKeyRows();
            assertQuery("SELECT a.id, b.id, c.id FROM ik_a a LEFT JOIN ik_b b ON b.x > 1 JOIN ik_c c ON a.w = c.k AND b.k = c.k ORDER BY 1, 2, 3")
                    .withPlanContaining("Hash Left Outer Join", "condition: b.k=a.w")
                    .returns("""
                            id\tid1\tid2
                            1\t13\t20
                            2\t10\t21
                            4\t13\t20
                            """);
            assertQuery("SELECT a.id, b.id, c.id FROM ik_a a LEFT JOIN ik_b2 b ON b.x > 1 JOIN ik_c c ON a.w = c.k AND b.k = c.k ORDER BY 1, 2, 3")
                    .returns("""
                            id\tid1\tid2
                            2\t10\t21
                            """);
            assertQuery("SELECT a.id, b.id, c.id FROM ik_a a LEFT JOIN ik_b b ON b.x > 100 JOIN ik_c c ON a.w = c.k AND b.k = c.k ORDER BY 1, 2, 3")
                    .returns("""
                            id\tid1\tid2
                            1\tnull\t20
                            4\tnull\t20
                            """);
            assertQuery("SELECT a.id, b.id, c.id FROM ik_a a LEFT JOIN ik_b b ON a.x = 1 JOIN ik_c c ON a.w = c.k AND b.k = c.k ORDER BY 1, 2, 3")
                    .returns("""
                            id\tid1\tid2
                            1\t11\t20
                            1\t13\t20
                            2\t10\t21
                            4\tnull\t20
                            """);
            assertQuery("SELECT a.id, b.id, c.id FROM ik_a a LEFT JOIN ik_b b ON b.x > 1 JOIN ik_c c ON a.w = c.k AND b.k = c.k WHERE b.u > 2 ORDER BY 1, 2, 3")
                    .returns("""
                            id\tid1\tid2
                            2\t10\t21
                            """);
        });
    }

    @Test
    public void testImpliedKeyNullExtensionInBothHashJoinFactories() throws Exception {
        assertMemoryLeak(() -> {
            createImpliedKeyRows();
            final String query = "SELECT a.id, b.id, c.id FROM %s a %s JOIN %s b ON b.x > %d JOIN ik_c c ON a.w = c.k AND b.k = c.k ORDER BY 1, 2, 3";
            for (boolean isFullFat : new boolean[]{false, true}) {
                final String light = isFullFat ? "" : " Light";
                assertRows(query.formatted("ik_a", "LEFT", "ik_b", 1), isFullFat, "Hash Left Outer Join" + light + "\n", """
                        id\tid1\tid2
                        1\t13\t20
                        2\t10\t21
                        4\t13\t20
                        """);
                assertRows(query.formatted("ik_a", "LEFT", "ik_b2", 1), isFullFat, null, """
                        id\tid1\tid2
                        2\t10\t21
                        """);
                assertRows(query.formatted("ik_a", "LEFT", "ik_b", 100), isFullFat, null, """
                        id\tid1\tid2
                        1\tnull\t20
                        4\tnull\t20
                        """);
                assertRows(query.formatted("ik_a", "RIGHT", "ik_b", 1), isFullFat, "Hash Right Outer Join" + light + "\n", """
                        id\tid1\tid2
                        null\t11\t20
                        1\t13\t20
                        2\t10\t21
                        4\t13\t20
                        """);
                assertRows(query.formatted("ik_a2", "RIGHT", "ik_b", 1), isFullFat, null, """
                        id\tid1\tid2
                        null\t11\t20
                        2\t10\t21
                        """);
                assertRows(query.formatted("ik_a", "FULL", "ik_b", 1), isFullFat, "Hash Full Outer Join" + light + "\n", """
                        id\tid1\tid2
                        null\t11\t20
                        1\t13\t20
                        2\t10\t21
                        4\t13\t20
                        """);
                assertRows(query.formatted("ik_a", "FULL", "ik_b2", 1), isFullFat, null, """
                        id\tid1\tid2
                        null\t11\t20
                        2\t10\t21
                        """);
            }
        });
    }

    @Test
    public void testImpliedKeyReplacesMixedTypeOnKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s0 (id INT, k INT, x INT, j LONG)");
            execute("CREATE TABLE s1 (id INT, k INT, x INT, j LONG)");
            execute("CREATE TABLE s2 (id INT, k INT, x INT, j LONG)");
            execute("INSERT INTO s0 VALUES (1, 1, 1, 1), (2, 2, 2, 1), (3, 2, 1, 2), (4, NULL, 1, NULL), (5, 1, NULL, 1), (6, NULL, 1, 5)");
            execute("INSERT INTO s1 VALUES (10, 1, 1, 1), (11, 2, 1, 2), (12, NULL, 1, NULL), (13, 3, 1, 3)");
            execute("INSERT INTO s2 VALUES (20, 1, 0, 1), (21, 2, 0, 2), (22, NULL, 0, NULL)");
            final String query = "SELECT a0.id, a1.id, a2.id FROM s0 a0 %s JOIN s1 a1 ON a0.j = a1.k JOIN s2 a2 ON a2.k = a1.k AND a2.k = a0.k %s ORDER BY 1, 2, 3";
            assertQuery(query.formatted("RIGHT", ""))
                    .withPlanContaining("Hash Right Outer Join", "condition: a1.k=a0.k", "filter: a0.j=a1.k")
                    .returns("""
                            id\tid1\tid2
                            1\t10\t20
                            3\t11\t21
                            4\t12\t22
                            5\t10\t20
                            """);
            assertQuery(query.formatted("LEFT", ""))
                    .returns("""
                            id\tid1\tid2
                            1\t10\t20
                            3\t11\t21
                            4\t12\t22
                            5\t10\t20
                            6\tnull\t22
                            """);
            assertQuery(query.formatted("FULL", ""))
                    .returns("""
                            id\tid1\tid2
                            1\t10\t20
                            3\t11\t21
                            4\t12\t22
                            5\t10\t20
                            6\tnull\t22
                            """);
            final String rejected = """
                    id\tid1\tid2
                    1\t10\t20
                    3\t11\t21
                    4\t12\t22
                    """;
            assertQuery(query.formatted("RIGHT", "WHERE a0.x + a0.id > 1")).returns(rejected);
            assertQuery(query.formatted("RIGHT", "WHERE a0.x IN (1, 2)")).returns(rejected);
            assertQuery("SELECT a0.id, a1.id FROM s0 a0 RIGHT JOIN s1 a1 ON a0.j = a1.k ORDER BY 1, 2")
                    .fails(58, "join column type mismatch");
        });
    }

    @Test
    public void testImpliedKeyRightJoinNullExtendsByOnClause() throws Exception {
        assertMemoryLeak(() -> {
            createImpliedKeyRows();
            assertQuery("SELECT a.id, b.id, c.id FROM ik_a a RIGHT JOIN ik_b b ON b.x > 1 JOIN ik_c c ON a.w = c.k AND b.k = c.k ORDER BY 1, 2, 3")
                    .withPlanContaining("Hash Right Outer Join", "condition: b.k=a.w")
                    .returns("""
                            id\tid1\tid2
                            null\t11\t20
                            1\t13\t20
                            2\t10\t21
                            4\t13\t20
                            """);
            assertQuery("SELECT a.id, b.id, c.id FROM ik_a2 a RIGHT JOIN ik_b b ON b.x > 1 JOIN ik_c c ON a.w = c.k AND b.k = c.k ORDER BY 1, 2, 3")
                    .returns("""
                            id\tid1\tid2
                            null\t11\t20
                            2\t10\t21
                            """);
            assertQuery("SELECT a.id, b.id, c.id FROM ik_a a RIGHT JOIN ik_b b ON a.x = 1 JOIN ik_c c ON a.w = c.k AND b.k = c.k ORDER BY 1, 2, 3")
                    .returns("""
                            id\tid1\tid2
                            1\t11\t20
                            1\t13\t20
                            2\t10\t21
                            """);
            assertQuery("SELECT a.id, b.id, c.id FROM ik_a a RIGHT JOIN ik_b b ON b.x > 1 JOIN ik_c c ON a.w = c.k AND b.k = c.k WHERE a.x > 0 ORDER BY 1, 2, 3")
                    .returns("""
                            id\tid1\tid2
                            1\t13\t20
                            2\t10\t21
                            """);
        });
    }

    @Test
    public void testImpliedSymbolKeyNullExtendsByOnClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE sa (id INT, w SYMBOL)");
            execute("CREATE TABLE sa2 (id INT, w SYMBOL)");
            execute("CREATE TABLE sb (id INT, x INT, k SYMBOL)");
            execute("CREATE TABLE sb2 (id INT, x INT, k SYMBOL)");
            execute("CREATE TABLE sc (id INT, k SYMBOL)");
            execute("INSERT INTO sa VALUES (1, NULL), (2, 'p'), (3, 'q')");
            execute("INSERT INTO sa2 VALUES (2, 'p'), (3, 'q')");
            execute("INSERT INTO sb VALUES (10, 2, 'p'), (11, 0, NULL), (12, 3, 'z'), (13, 3, NULL)");
            execute("INSERT INTO sb2 VALUES (10, 2, 'p'), (11, 0, NULL), (12, 3, 'z')");
            execute("INSERT INTO sc VALUES (20, NULL), (21, 'p'), (22, 'z')");
            final String query = "SELECT a.id, b.id, c.id FROM %s a %s JOIN %s b ON b.x > 1 JOIN sc c ON a.w = c.k AND b.k = c.k ORDER BY 1, 2, 3";
            assertQuery(query.formatted("sa", "LEFT", "sb"))
                    .withPlanContaining("Hash Left Outer Join", "condition: b.k=a.w")
                    .returns("""
                            id\tid1\tid2
                            1\t13\t20
                            2\t10\t21
                            """);
            assertQuery(query.formatted("sa", "LEFT", "sb2"))
                    .returns("""
                            id\tid1\tid2
                            2\t10\t21
                            """);
            assertQuery(query.formatted("sa", "RIGHT", "sb"))
                    .returns("""
                            id\tid1\tid2
                            null\t11\t20
                            1\t13\t20
                            2\t10\t21
                            """);
            assertQuery(query.formatted("sa2", "RIGHT", "sb"))
                    .returns("""
                            id\tid1\tid2
                            null\t11\t20
                            2\t10\t21
                            """);
            assertQuery(query.formatted("sa2", "FULL", "sb"))
                    .returns("""
                            id\tid1\tid2
                            null\t11\t20
                            2\t10\t21
                            """);
        });
    }

    @Test
    public void testInnerOnReadingNoInputFiltersItsStepOnlyWithOuterJoin() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setBoolean("has_match", true);
            final String expected = """
                    lid\txid
                    1\t10
                    1\t10
                    1\t11
                    1\t11
                    3\t12
                    4\t13
                    """;
            for (boolean isFullFat : new boolean[]{false, true}) {
                final String light = isFullFat ? "" : " Light";
                final String query = "SELECT l.id lid, x.id xid FROM lp_outer_l l JOIN lp_outer_r r ON r.k = l.k AND :has_match %s "
                        + "lp_outer_r x ON x.k = r.k ORDER BY lid, xid";
                assertRows(query.formatted("JOIN"), isFullFat, """
                        SelectedRecord
                                Filter filter: :has_match::boolean
                                    Hash Join%s
                                      condition: x.k=r.k
                        """.formatted(light), expected);
                assertRows(query.formatted("LEFT JOIN"), isFullFat, """
                        Hash Left Outer Join%s
                                  condition: x.k=r.k
                                    Filter filter: :has_match::boolean
                                        Hash Join%s
                        """.formatted(light, light), expected);
            }
        });
    }

    @Test
    public void testKeyedOuterPredicatesAndNativeIn() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            {
                final String join = " RIGHT JOIN ";
                final String label = "Right";
                for (boolean isFullFat : new boolean[]{false, true}) {
                    final String plan = "Hash " + label + " Outer Join" + (isFullFat ? "" : " Light");
                    final String prefix = "SELECT l.id lid,r.id rid FROM lp_outer_l l" + join + "lp_outer_r r ON ";
                    assertRows(prefix + "l.k=r.k ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    null	14
                                    1	10
                                    1	11
                                    3	12
                                    4	13
                                    """);
                    assertRows(prefix + "l.k=r.k AND l.v<r.v ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    null	13
                                    null	14
                                    1	10
                                    1	11
                                    3	12
                                    """);
                    assertRows(prefix + "l.k=r.k AND r.v IN (11,31) ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    null	11
                                    null	13
                                    null	14
                                    1	10
                                    3	12
                                    """);
                    assertRows(prefix + "l.k=r.k WHERE r.v IN (11,31) ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    1	10
                                    3	12
                                    """);
                    assertRows(prefix + "l.k=r.k WHERE l.id=null ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    null	14
                                    """);
                    assertRows(prefix + "l.k=r.k WHERE r.id=null ORDER BY lid,rid", isFullFat, plan, "lid\trid\n");
                    assertRows("SELECT l.id lid,r.id rid,l.s ls,r.s rs FROM lp_outer_l l" + join
                                    + "lp_outer_r r ON l.s=r.s ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid	ls	rs
                                    null	14		f
                                    1	10	a	a
                                    1	11	a	a
                                    3	12	c	c
                                    4	13	\t
                                    """);
                }
            }
            {
                final String join = " FULL JOIN ";
                final String label = "Full";
                for (boolean isFullFat : new boolean[]{false, true}) {
                    final String plan = "Hash " + label + " Outer Join" + (isFullFat ? "" : " Light");
                    final String prefix = "SELECT l.id lid,r.id rid FROM lp_outer_l l" + join + "lp_outer_r r ON ";
                    assertRows(prefix + "l.k=r.k ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    null	14
                                    1	10
                                    1	11
                                    2	null
                                    3	12
                                    4	13
                                    5	null
                                    """);
                    assertRows(prefix + "l.k=r.k AND l.v<r.v ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    null	13
                                    null	14
                                    1	10
                                    1	11
                                    2	null
                                    3	12
                                    4	null
                                    5	null
                                    """);
                    assertRows(prefix + "l.k=r.k AND r.v IN (11,31) ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    null	11
                                    null	13
                                    null	14
                                    1	10
                                    2	null
                                    3	12
                                    4	null
                                    5	null
                                    """);
                    assertRows(prefix + "l.k=r.k WHERE r.v IN (11,31) ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    1	10
                                    3	12
                                    """);
                    assertRows(prefix + "l.k=r.k WHERE l.id=null ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    null	14
                                    """);
                    assertRows(prefix + "l.k=r.k WHERE r.id=null ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    2	null
                                    5	null
                                    """);
                    assertRows("SELECT l.id lid,r.id rid,l.s ls,r.s rs FROM lp_outer_l l" + join
                                    + "lp_outer_r r ON l.s=r.s ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid	ls	rs
                                    null	14		f
                                    1	10	a	a
                                    1	11	a	a
                                    2	null	b\t
                                    3	12	c	c
                                    4	13	\t
                                    5	null	e\t
                                    """);
                }
            }
        });
    }

    @Test
    public void testNestedLoopFullJoinOverSlaveWithoutUniqueRowIds() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            execute("CREATE TABLE lp_outer_u(id INT,v INT,s SYMBOL,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO lp_outer_u VALUES (20,12,'a','2020-01-01'),(21,25,'g','2020-01-02'),(22,5,'h','2020-01-03')");
            final String plan = "Nested Loop Full Join";
            final String unionAll = "SELECT v,s FROM lp_outer_r UNION ALL SELECT v,s FROM lp_outer_u";
            final String unionAllExpected = """
                    lid	rv
                    null	null
                    null	5
                    1	11
                    1	12
                    1	12
                    1	25
                    1	31
                    1	60
                    2	25
                    2	31
                    2	60
                    3	31
                    3	60
                    4	null
                    5	60
                    """;
            assertRows("SELECT count() FROM lp_outer_l l FULL JOIN (" + unionAll + ") r ON true", false, plan, "count\n40\n");
            assertRows("SELECT count() FROM lp_outer_l l FULL JOIN (SELECT s FROM lp_outer_r UNION ALL SELECT s FROM lp_outer_u) r ON true",
                    false, plan, "count\n40\n");
            assertRows("SELECT l.id lid,r.v rv FROM lp_outer_l l FULL JOIN (" + unionAll + ") r ON l.v<r.v ORDER BY lid,rv",
                    false, plan, unionAllExpected);
            assertRows("SELECT l.id lid,r.v rv FROM lp_outer_l l FULL JOIN (SELECT v FROM lp_outer_r UNION ALL SELECT v::LONG v FROM lp_outer_u) r "
                    + "ON l.v<r.v ORDER BY lid,rv", false, plan, unionAllExpected);
            assertRows("SELECT l.id lid,r.s rs FROM lp_outer_l l FULL JOIN (" + unionAll + ") r ON l.s=r.s OR r.s='g' ORDER BY lid,rs",
                    false, plan,
                    """
                            lid	rs
                            null	f
                            null	h
                            1	a
                            1	a
                            1	a
                            1	g
                            2	g
                            3	c
                            3	g
                            4\t
                            4	g
                            5	g
                            """);
            assertRows("SELECT l.id lid,r.v rv FROM (SELECT * FROM lp_outer_l WHERE id>100) l FULL JOIN (" + unionAll + ") r "
                            + "ON l.v<r.v ORDER BY lid,rv", false, plan,
                    """
                            lid	rv
                            null	null
                            null	5
                            null	11
                            null	12
                            null	12
                            null	25
                            null	31
                            null	60
                            """);
            assertRows("SELECT l.id lid,r.v rv FROM lp_outer_l l FULL JOIN "
                            + "(SELECT v FROM lp_outer_r WHERE id>100 UNION ALL SELECT v FROM lp_outer_u WHERE id>100) r ON l.v<r.v ORDER BY lid,rv",
                    false, plan,
                    """
                            lid	rv
                            1	null
                            2	null
                            3	null
                            4	null
                            5	null
                            """);
            assertRows("SELECT l.id lid,r.v rv FROM lp_outer_l l FULL JOIN (SELECT v FROM lp_outer_r UNION SELECT v FROM lp_outer_u) r "
                            + "ON l.v<r.v ORDER BY lid,rv", false, plan,
                    """
                            lid	rv
                            null	null
                            null	5
                            1	11
                            1	12
                            1	25
                            1	31
                            1	60
                            2	25
                            2	31
                            2	60
                            3	31
                            3	60
                            4	null
                            5	60
                            """);
            assertRows("SELECT l.id lid,r.s rs FROM lp_outer_l l FULL JOIN (SELECT s FROM lp_outer_r UNION SELECT s FROM lp_outer_u) r "
                            + "ON l.s=r.s OR r.s='g' ORDER BY lid,rs", false, plan,
                    """
                            lid	rs
                            null	f
                            null	h
                            1	a
                            1	g
                            2	g
                            3	c
                            3	g
                            4\t
                            4	g
                            5	g
                            """);
            assertRows("SELECT l.id lid,r.v rv FROM lp_outer_l l FULL JOIN "
                            + "(SELECT v FROM (" + unionAll + ") EXCEPT SELECT v FROM lp_outer_r WHERE id=11) r ON l.v<r.v ORDER BY lid,rv",
                    false, plan,
                    """
                            lid	rv
                            null	null
                            null	5
                            1	11
                            1	25
                            1	31
                            1	60
                            2	25
                            2	31
                            2	60
                            3	31
                            3	60
                            4	null
                            5	60
                            """);
            assertRows("SELECT l.id lid,r.v rv FROM lp_outer_l l FULL JOIN "
                            + "(SELECT v FROM (" + unionAll + ") INTERSECT SELECT v FROM lp_outer_r) r ON l.v<r.v ORDER BY lid,rv",
                    false, plan,
                    """
                            lid	rv
                            null	null
                            1	11
                            1	12
                            1	31
                            1	60
                            2	31
                            2	60
                            3	31
                            3	60
                            4	null
                            5	60
                            """);
            assertRows("SELECT count() FROM lp_outer_l l FULL JOIN "
                    + "(SELECT s FROM (" + unionAll + ") EXCEPT SELECT s FROM lp_outer_r WHERE id=14) r ON true", false, plan, "count\n25\n");
            assertRows("SELECT count() FROM lp_outer_l l FULL JOIN "
                    + "(SELECT s FROM (" + unionAll + ") INTERSECT SELECT s FROM lp_outer_u) r ON true", false, plan, "count\n15\n");
            assertRows("SELECT l.id lid,r.k rk FROM (SELECT * FROM lp_outer_l WHERE id=1) l FULL JOIN "
                            + "(SELECT a.v+b.v k FROM lp_outer_u a CROSS JOIN lp_outer_u b) r ON l.v+10<r.k ORDER BY lid,rk", false, plan,
                    """
                            lid	rk
                            null	10
                            null	17
                            null	17
                            1	24
                            1	30
                            1	30
                            1	37
                            1	37
                            1	50
                            """);
        });
    }

    @Test
    public void testNestedLoopFullJoinRewindsAfterUnmatchedMasterRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_nl_m(k LONG)");
            execute("INSERT INTO lp_nl_m VALUES (1),(7),(5),(2)");
            execute("CREATE TABLE lp_nl_s(k LONG)");
            execute("INSERT INTO lp_nl_s VALUES (2),(3)");
            assertQuery("SELECT x,mk,sk FROM long_sequence(2) CROSS JOIN "
                    + "(SELECT m.k mk,s.k sk FROM lp_nl_m m FULL JOIN lp_nl_s s ON m.k<s.k LIMIT 3)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            x	mk	sk
                            1	1	2
                            1	1	3
                            1	7	null
                            2	1	2
                            2	1	3
                            2	7	null
                            """);
        });
    }

    @Test
    public void testNestedLoopOuterJoinReopensAfterEarlyClose() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_nl_m(k LONG)");
            execute("INSERT INTO lp_nl_m VALUES (7),(5),(2)");
            execute("CREATE TABLE lp_nl_s(k LONG)");
            execute("INSERT INTO lp_nl_s VALUES (2),(3)");
            assertRowsAfterEarlyClose("SELECT m.k mk,s.k sk FROM lp_nl_m m LEFT JOIN lp_nl_s s ON m.k<s.k", "Nested Loop Left Join",
                    """
                            mk	sk
                            7	null
                            5	null
                            2	3
                            """);
            assertRowsAfterEarlyClose("SELECT s.k sk,m.k mk FROM lp_nl_s s RIGHT JOIN lp_nl_m m ON s.k>m.k", "Nested Loop Right Join",
                    """
                            sk	mk
                            null	7
                            null	5
                            3	2
                            """);
            assertRowsAfterEarlyClose("SELECT m.k mk,s.k sk FROM lp_nl_m m FULL JOIN lp_nl_s s ON m.k<s.k", "Nested Loop Full Join",
                    """
                            mk	sk
                            7	null
                            5	null
                            2	3
                            null	2
                            """);
        });
    }

    @Test
    public void testNestedLoopOuterPredicatesAndConstantConditions() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            {
                final String join = " RIGHT JOIN ";
                final String plan = "Nested Loop Right Join";
                final String prefix = "SELECT l.id lid,r.id rid FROM lp_outer_l l" + join + "lp_outer_r r ON ";
                assertRows(prefix + "l.v<r.v ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                null	13
                                1	10
                                1	11
                                1	12
                                1	14
                                2	12
                                2	14
                                3	12
                                3	14
                                5	14
                                """);
                assertRows(prefix + "false ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                null	10
                                null	11
                                null	12
                                null	13
                                null	14
                                """);
                assertRows(prefix + "true ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                1	10
                                1	11
                                1	12
                                1	13
                                1	14
                                2	10
                                2	11
                                2	12
                                2	13
                                2	14
                                3	10
                                3	11
                                3	12
                                3	13
                                3	14
                                4	10
                                4	11
                                4	12
                                4	13
                                4	14
                                5	10
                                5	11
                                5	12
                                5	13
                                5	14
                                """);
                assertRows(prefix + "l.v<r.v WHERE l.id=null OR r.id=null ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                null	13
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM (SELECT * FROM lp_outer_l WHERE false) l" + join
                                + "lp_outer_r r ON l.v<r.v ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                null	10
                                null	11
                                null	12
                                null	13
                                null	14
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_outer_l l" + join
                                + "(SELECT * FROM lp_outer_r WHERE false) r ON l.v<r.v ORDER BY lid,rid", false, plan,
                        "lid\trid\n");
            }
            {
                final String join = " FULL JOIN ";
                final String plan = "Nested Loop Full Join";
                final String prefix = "SELECT l.id lid,r.id rid FROM lp_outer_l l" + join + "lp_outer_r r ON ";
                assertRows(prefix + "l.v<r.v ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                null	13
                                1	10
                                1	11
                                1	12
                                1	14
                                2	12
                                2	14
                                3	12
                                3	14
                                4	null
                                5	14
                                """);
                assertRows(prefix + "false ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                null	10
                                null	11
                                null	12
                                null	13
                                null	14
                                1	null
                                2	null
                                3	null
                                4	null
                                5	null
                                """);
                assertRows(prefix + "true ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                1	10
                                1	11
                                1	12
                                1	13
                                1	14
                                2	10
                                2	11
                                2	12
                                2	13
                                2	14
                                3	10
                                3	11
                                3	12
                                3	13
                                3	14
                                4	10
                                4	11
                                4	12
                                4	13
                                4	14
                                5	10
                                5	11
                                5	12
                                5	13
                                5	14
                                """);
                assertRows(prefix + "l.v<r.v WHERE l.id=null OR r.id=null ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                null	13
                                4	null
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM (SELECT * FROM lp_outer_l WHERE false) l" + join
                                + "lp_outer_r r ON l.v<r.v ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                null	10
                                null	11
                                null	12
                                null	13
                                null	14
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_outer_l l" + join
                                + "(SELECT * FROM lp_outer_r WHERE false) r ON l.v<r.v ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                1	null
                                2	null
                                3	null
                                4	null
                                5	null
                                """);
            }
        });
    }

    @Test
    public void testOuterFactoryAndExplainSurviveCompilerReuse() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String sql = "SELECT l.id lid,r.id rid FROM lp_outer_l l FULL JOIN lp_outer_r r "
                    + "ON l.k=r.k AND r.v IN (11,31) ORDER BY lid,rid";
            final RecordCursorFactory retained;
            final String expected = """
                    lid	rid
                    null	11
                    null	13
                    null	14
                    1	10
                    2	null
                    3	12
                    4	null
                    5	null
                    """;
            final String expectedPlan;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
                }
                retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                try {
                    expectedPlan = planText(retained);
                    try (RecordCursorFactory ignored = compiler.compile("SELECT count() FROM lp_outer_l", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(compiler.getPlanForTesting());
                    }
                } catch (Throwable th) {
                    retained.close();
                    throw th;
                }
            }
            try (RecordCursorFactory factory = retained) {
                assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
                TestUtils.assertEquals(expectedPlan, planText(factory));
            }
        });
    }

    @Test
    public void testOuterJoinResidualConditionOnDuplicateKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_dup_l (id INT, k INT, v INT)");
            execute("CREATE TABLE lp_dup_r (id INT, k INT, v INT)");
            execute("INSERT INTO lp_dup_l VALUES (1, 1, 20), (2, 1, 10), (3, 0, 30), (4, NULL, 40), (5, NULL, 5)");
            execute("""
                    INSERT INTO lp_dup_r VALUES (10, 1, 5), (11, 1, 25), (12, 0, 35), (13, 0, 1),
                        (14, NULL, 50), (15, NULL, 2), (16, 2, 60), (17, 1, 30)
                    """);
            final String fullNoMatch = """
                    lid	rid
                    null	10
                    null	11
                    null	12
                    null	13
                    null	14
                    null	15
                    null	16
                    null	17
                    1	null
                    2	null
                    3	null
                    4	null
                    5	null
                    """;
            final String fullLeftResidual = """
                    lid	rid
                    null	16
                    1	10
                    1	11
                    1	17
                    2	null
                    3	12
                    3	13
                    4	14
                    4	15
                    5	null
                    """;
            final String crossResidual = """
                    lid	rid
                    null	10
                    null	13
                    null	15
                    null	16
                    1	11
                    1	17
                    2	11
                    2	17
                    3	12
                    4	14
                    5	14
                    """;
            final String rightNoMatch = """
                    lid	rid
                    null	10
                    null	11
                    null	12
                    null	13
                    null	14
                    null	15
                    null	16
                    null	17
                    """;
            final String rightLeftResidual = """
                    lid	rid
                    null	16
                    1	10
                    1	11
                    1	17
                    3	12
                    3	13
                    4	14
                    4	15
                    """;
            for (boolean isFullFat : new boolean[]{false, true}) {
                final String light = isFullFat ? "" : " Light";
                // The left side is the smaller one, so the light FULL join hashes it and streams the right side.
                assertResidualOuterJoin("lp_dup_l l FULL JOIN lp_dup_r r ON l.k = r.k", isFullFat, "Hash Full Outer Join" + light,
                        fullNoMatch, fullLeftResidual, crossResidual);
                assertResidualOuterJoin("lp_dup_r r FULL JOIN lp_dup_l l ON l.k = r.k", isFullFat, "Hash Full Outer Join" + light,
                        fullNoMatch, fullLeftResidual, crossResidual);
                assertResidualOuterJoin("lp_dup_l l RIGHT JOIN lp_dup_r r ON l.k = r.k", isFullFat, "Hash Right Outer Join" + light,
                        rightNoMatch, rightLeftResidual, crossResidual);
            }
            assertResidualOuterJoin("lp_dup_l l FULL JOIN lp_dup_r r ON l.k + 0 = r.k", false, "Nested Loop Full Join",
                    fullNoMatch, fullLeftResidual, crossResidual);
            assertResidualOuterJoin("lp_dup_l l RIGHT JOIN lp_dup_r r ON l.k + 0 = r.k", false, "Nested Loop Right Join",
                    rightNoMatch, rightLeftResidual, crossResidual);
        });
    }

    @Test
    public void testTimestampDesignationClearedForBothOuterSides() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            {
                final String join = " RIGHT JOIN ";
                assertRowsWithoutTimestamp("SELECT l.ts lts,r.ts rts FROM lp_outer_l l" + join + "lp_outer_r r ON l.k=r.k",
                        """
                                lts	rts
                                2020-01-01T00:00:00.000000Z	2020-01-02T00:00:00.000000Z
                                2020-01-01T00:00:00.000000Z	2020-01-01T00:00:00.000000Z
                                2020-01-03T00:00:00.000000Z	2020-01-03T00:00:00.000000Z
                                2020-01-04T00:00:00.000000Z	2020-01-04T00:00:00.000000Z
                                	2020-01-05T00:00:00.000000Z
                                """);
                assertRowsWithoutTimestamp("SELECT l.ts lts,r.ts rts FROM lp_outer_l l" + join + "lp_outer_r r ON l.v<r.v",
                        """
                                lts	rts
                                2020-01-01T00:00:00.000000Z	2020-01-01T00:00:00.000000Z
                                2020-01-01T00:00:00.000000Z	2020-01-02T00:00:00.000000Z
                                2020-01-01T00:00:00.000000Z	2020-01-03T00:00:00.000000Z
                                2020-01-02T00:00:00.000000Z	2020-01-03T00:00:00.000000Z
                                2020-01-03T00:00:00.000000Z	2020-01-03T00:00:00.000000Z
                                	2020-01-04T00:00:00.000000Z
                                2020-01-01T00:00:00.000000Z	2020-01-05T00:00:00.000000Z
                                2020-01-02T00:00:00.000000Z	2020-01-05T00:00:00.000000Z
                                2020-01-03T00:00:00.000000Z	2020-01-05T00:00:00.000000Z
                                2020-01-05T00:00:00.000000Z	2020-01-05T00:00:00.000000Z
                                """);
            }
            {
                final String join = " FULL JOIN ";
                assertRowsWithoutTimestamp("SELECT l.ts lts,r.ts rts FROM lp_outer_l l" + join + "lp_outer_r r ON l.k=r.k",
                        """
                                lts	rts
                                2020-01-01T00:00:00.000000Z	2020-01-02T00:00:00.000000Z
                                2020-01-01T00:00:00.000000Z	2020-01-01T00:00:00.000000Z
                                2020-01-02T00:00:00.000000Z\t
                                2020-01-03T00:00:00.000000Z	2020-01-03T00:00:00.000000Z
                                2020-01-04T00:00:00.000000Z	2020-01-04T00:00:00.000000Z
                                2020-01-05T00:00:00.000000Z\t
                                	2020-01-05T00:00:00.000000Z
                                """);
                assertRowsWithoutTimestamp("SELECT l.ts lts,r.ts rts FROM lp_outer_l l" + join + "lp_outer_r r ON l.v<r.v",
                        """
                                lts	rts
                                2020-01-01T00:00:00.000000Z	2020-01-01T00:00:00.000000Z
                                2020-01-01T00:00:00.000000Z	2020-01-02T00:00:00.000000Z
                                2020-01-01T00:00:00.000000Z	2020-01-03T00:00:00.000000Z
                                2020-01-01T00:00:00.000000Z	2020-01-05T00:00:00.000000Z
                                2020-01-02T00:00:00.000000Z	2020-01-03T00:00:00.000000Z
                                2020-01-02T00:00:00.000000Z	2020-01-05T00:00:00.000000Z
                                2020-01-03T00:00:00.000000Z	2020-01-03T00:00:00.000000Z
                                2020-01-03T00:00:00.000000Z	2020-01-05T00:00:00.000000Z
                                2020-01-04T00:00:00.000000Z\t
                                2020-01-05T00:00:00.000000Z	2020-01-05T00:00:00.000000Z
                                	2020-01-04T00:00:00.000000Z
                                """);
            }
        });
    }

    private void assertResidualOuterJoin(
            String join,
            boolean isFullFat,
            String expectedPlan,
            String expectedNoMatch,
            String expectedLeftResidual,
            String expectedCrossResidual
    ) throws Exception {
        final String prefix = "SELECT l.id lid, r.id rid FROM " + join + " AND ";
        assertRows(prefix + "r.v > 1000 ORDER BY lid, rid", isFullFat, expectedPlan, expectedNoMatch);
        assertRows(prefix + "l.v > 15 ORDER BY lid, rid", isFullFat, expectedPlan, expectedLeftResidual);
        assertRows(prefix + "l.v < r.v ORDER BY lid, rid", isFullFat, expectedPlan, expectedCrossResidual);
    }

    private void assertInnerOnBeforeRightOrFullJoin(String predicate) throws Exception {
        assertMemoryLeak(() -> {
            createInnerOnRows();
            bindVariableService.setBoolean("has_match", false);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (boolean isFullFat : new boolean[]{false, true}) {
                    compiler.setFullFatJoins(isFullFat);
                    for (int joinMode = 0; joinMode < 2; joinMode++) {
                        final String join = joinMode == 0 ? "RIGHT" : "FULL";
                        final String query = "SELECT a.id aid, b.id bid FROM a JOIN c ON " + predicate
                                + " " + join + " JOIN b ON a.k = b.k";
                        assertQuery(query + " ORDER BY aid, bid").withCompiler(compiler).noLeakCheck().returns("""
                                aid\tbid
                                null\t20
                                null\t21
                                """);
                        assertQuery(query + " WHERE " + predicate + " ORDER BY aid, bid")
                                .withCompiler(compiler).noLeakCheck().returns("aid\tbid\n");
                        assertQuery(query + " WHERE b.k IS NULL ORDER BY aid, bid")
                                .withCompiler(compiler).noLeakCheck().returns("""
                                        aid\tbid
                                        null\t21
                                        """);
                    }
                }
            }
        });
    }

    private void assertRows(String sql, boolean isFullFat, String expectedPlan, String expected) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            compiler.setFullFatJoins(isFullFat);
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
                if (expectedPlan != null) {
                    TestUtils.assertContains(planText(factory), expectedPlan);
                }
            }
        }
    }

    private void assertRowsAfterEarlyClose(String sql, String expectedPlan, String expected) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            TestUtils.assertContains(planText(factory), expectedPlan);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
            }
            assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
        }
    }

    private void assertRowsWithoutTimestamp(String sql, String expected) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            Assert.assertEquals(-1, factory.getMetadata().getTimestampIndex());
        }
        assertRows(sql, false, null, expected);
    }

    private void createImpliedCrossKeyRows() throws Exception {
        execute("CREATE TABLE ia (id INT, l LONG)");
        execute("CREATE TABLE im (id INT, k INT)");
        execute("CREATE TABLE ib (id INT, i INT, k INT)");
        execute("CREATE TABLE ic (id INT, x INT)");
        execute("INSERT INTO ia VALUES (1, 1), (2, 2), (3, NULL)");
        execute("INSERT INTO im VALUES (10, 5), (11, 6), (12, NULL)");
        execute("INSERT INTO ib VALUES (20, 1, 5), (21, 2, 6), (22, 3, 5), (23, 1, NULL), (24, NULL, NULL)");
        execute("INSERT INTO ic VALUES (30, 5), (31, 6), (32, NULL)");
    }

    private void createImpliedKeyRows() throws Exception {
        execute("CREATE TABLE ik_a (id INT, x INT, w INT)");
        execute("CREATE TABLE ik_a2 (id INT, x INT, w INT)");
        execute("CREATE TABLE ik_b (id INT, x INT, k INT, u INT)");
        execute("CREATE TABLE ik_b2 (id INT, x INT, k INT, u INT)");
        execute("CREATE TABLE ik_c (id INT, k INT)");
        execute("INSERT INTO ik_a VALUES (1, 1, NULL), (2, 1, 5), (3, 1, 7), (4, NULL, NULL)");
        execute("INSERT INTO ik_a2 VALUES (2, 1, 5), (3, 1, 7)");
        execute("INSERT INTO ik_b VALUES (10, 2, 5, 3), (11, 0, NULL, 3), (12, 3, 9, NULL), (13, 3, NULL, 1)");
        execute("INSERT INTO ik_b2 VALUES (10, 2, 5, 3), (11, 0, NULL, 3), (12, 3, 9, NULL)");
        execute("INSERT INTO ik_c VALUES (20, NULL), (21, 5), (22, 9)");
    }

    private void createInnerOnRows() throws Exception {
        execute("CREATE TABLE a (id INT, k INT)");
        execute("CREATE TABLE b (id INT, k INT, l LONG)");
        execute("CREATE TABLE c (id INT)");
        execute("INSERT INTO a VALUES (10, 1), (11, NULL)");
        execute("INSERT INTO b VALUES (20, 1, 1), (21, NULL, NULL)");
        execute("INSERT INTO c VALUES (30)");
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_outer_l(id INT,k INT,v INT,s SYMBOL,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("CREATE TABLE lp_outer_r(id INT,k INT,v INT,s SYMBOL,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO lp_outer_l VALUES (1,1,10,'a','2020-01-01'),(2,2,20,'b','2020-01-02'),"
                + "(3,3,30,'c','2020-01-03'),(4,null,null,null,'2020-01-04'),(5,5,50,'e','2020-01-05')");
        execute("INSERT INTO lp_outer_r VALUES (10,1,11,'a','2020-01-01'),(11,1,12,'a','2020-01-02'),"
                + "(12,3,31,'c','2020-01-03'),(13,null,null,null,'2020-01-04'),(14,6,60,'f','2020-01-05')");
    }
}
