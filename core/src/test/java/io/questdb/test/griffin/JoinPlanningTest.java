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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class JoinPlanningTest extends AbstractCairoTest {
    private static final String LEADING_CROSS_PLAN = """
            Cross Join
                PageFrame
                    Row forward scan
                    Frame forward scan on: lp_cross_a
                Materialize
                    Hash Join Light
                      condition: c.k=b.k
                        PageFrame
                            Row forward scan
                            Frame forward scan on: lp_cross_b
                        Hash
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_cross_c
            """;
    private static final String TRAILING_CROSS_PLAN = """
            Cross Join
                Hash Join Light
                  condition: c.k=b.k
                    PageFrame
                        Row forward scan
                        Frame forward scan on: lp_cross_b
                    Hash
                        PageFrame
                            Row forward scan
                            Frame forward scan on: lp_cross_c
                PageFrame
                    Row forward scan
                    Frame forward scan on: lp_cross_a
            """;

    @Test
    public void testCompileTimeWhereConjunctsKeepTheirOwnBooleanValidation() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final ObjList<String> joins = new ObjList<>(
                    "JOIN lp_join_r r ON l.k=r.k", "CROSS JOIN lp_join_r r",
                    "LEFT JOIN lp_join_r r ON l.k=r.k", "RIGHT JOIN lp_join_r r ON l.k=r.k",
                    "FULL JOIN lp_join_r r ON l.k=r.k");
            {
                final int i = 0;
                final String prefix = "SELECT l.id FROM lp_join_l l " + joins.getQuick(i) + " WHERE ";
                assertQuery(prefix + "l.id>0 AND null").noLeakCheck().fails(74, "boolean expression expected");
                assertQuery(prefix + "null AND l.id>0").noLeakCheck().fails(63, "boolean expression expected");
                assertQuery(prefix + "true AND null").noLeakCheck().fails(72, "boolean expression expected");
                assertRows(prefix + "null AND true ORDER BY l.id", false, "id\n");
            }
            {
                final int i = 1;
                final String prefix = "SELECT l.id FROM lp_join_l l " + joins.getQuick(i) + " WHERE ";
                assertQuery(prefix + "l.id>0 AND null").noLeakCheck().fails(69, "boolean expression expected");
                assertQuery(prefix + "null AND l.id>0").noLeakCheck().fails(58, "boolean expression expected");
                assertQuery(prefix + "true AND null").noLeakCheck().fails(67, "boolean expression expected");
                assertRows(prefix + "null AND true ORDER BY l.id", false, "id\n");
            }
            {
                final int i = 2;
                final String prefix = "SELECT l.id FROM lp_join_l l " + joins.getQuick(i) + " WHERE ";
                assertQuery(prefix + "l.id>0 AND null").noLeakCheck().fails(79, "boolean expression expected");
                assertQuery(prefix + "null AND l.id>0").noLeakCheck().fails(68, "boolean expression expected");
                assertQuery(prefix + "true AND null").noLeakCheck().fails(77, "boolean expression expected");
                assertRows(prefix + "null AND true ORDER BY l.id", false, "id\n");
            }
            {
                final int i = 3;
                final String prefix = "SELECT l.id FROM lp_join_l l " + joins.getQuick(i) + " WHERE ";
                assertQuery(prefix + "l.id>0 AND null").noLeakCheck().fails(80, "boolean expression expected");
                assertQuery(prefix + "null AND l.id>0").noLeakCheck().fails(69, "boolean expression expected");
                assertQuery(prefix + "true AND null").noLeakCheck().fails(78, "boolean expression expected");
                assertRows(prefix + "null AND true ORDER BY l.id", false, "id\n");
            }
            {
                final int i = 4;
                final String prefix = "SELECT l.id FROM lp_join_l l " + joins.getQuick(i) + " WHERE ";
                assertQuery(prefix + "l.id>0 AND null").noLeakCheck().fails(79, "boolean expression expected");
                assertQuery(prefix + "null AND l.id>0").noLeakCheck().fails(68, "boolean expression expected");
                assertQuery(prefix + "true AND null").noLeakCheck().fails(77, "boolean expression expected");
                assertRows(prefix + "null AND true ORDER BY l.id", false, "id\n");
            }
            // Failed constant-group validation also releases the already bound
            // native IN preparation. The following compilation must still work.
            assertQuery("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k "
                    + "WHERE l.id IN (1,2,3) AND null").noLeakCheck().fails(83, "boolean expression expected");
            assertRows("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k "
                            + "WHERE l.id IN (1,2,3) ORDER BY l.id", false,
                    """
                            id
                            1
                            1
                            2
                            3
                            """);
        });
    }

    @Test
    public void testCrossBeforeKeyedJoinTrailsItInNonEquiNullingPrefix() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_cross_a (k INT, x INT)");
            execute("CREATE TABLE lp_cross_b (k INT)");
            execute("CREATE TABLE lp_cross_c (y INT)");
            execute("CREATE TABLE lp_cross_d (k INT)");
            execute("INSERT INTO lp_cross_a VALUES (1, 1), (2, 2), (3, 3)");
            execute("INSERT INTO lp_cross_b VALUES (1), (2)");
            execute("INSERT INTO lp_cross_c VALUES (10), (20)");
            execute("INSERT INTO lp_cross_d VALUES (2)");
            for (String joinType : new String[]{"RIGHT", "FULL"}) {
                final String nullingJoin = joinType.equals("RIGHT") ? "Right" : "Full";
                assertQuery("SELECT a.k, c.y, d.k dk FROM lp_cross_a a CROSS JOIN lp_cross_c c JOIN lp_cross_b b ON a.k = b.k "
                        + joinType + " JOIN lp_cross_d d ON a.x >= d.k")
                        .noLeakCheck()
                        .noRandomAccess()
                        .withPlan("""
                                SelectedRecord
                                    Nested Loop %s Join
                                      filter: a.x>=d.k
                                        Cross Join
                                            Hash Join Light
                                              condition: b.k=a.k
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_cross_a
                                                Hash
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: lp_cross_b
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_cross_c
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_cross_d
                                """.formatted(nullingJoin))
                        .returns(joinType.equals("RIGHT")
                                ? """
                                k	y	dk
                                2	10	2
                                2	20	2
                                """
                                : """
                                k	y	dk
                                1	10	null
                                1	20	null
                                2	10	2
                                2	20	2
                                """);
                assertQuery("SELECT a.k, c.y, d.k dk, b2.k bk FROM lp_cross_a a CROSS JOIN lp_cross_c c JOIN lp_cross_b b ON a.k = b.k "
                        + joinType + " JOIN lp_cross_d d ON a.x >= d.k JOIN lp_cross_b b2 ON b2.k = d.k")
                        .noLeakCheck()
                        .noRandomAccess()
                        .withPlan("""
                                SelectedRecord
                                    Hash Join Light
                                      condition: b2.k=d.k
                                        Nested Loop %s Join
                                          filter: a.x>=d.k
                                            Cross Join
                                                Hash Join Light
                                                  condition: b.k=a.k
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: lp_cross_a
                                                    Hash
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: lp_cross_b
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_cross_c
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_cross_d
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_cross_b
                                """.formatted(nullingJoin))
                        .returns("""
                                k	y	dk	bk
                                2	10	2	2
                                2	20	2	2
                                """);
            }
        });
    }

    @Test
    public void testCrossInnerExplicitShorthandAndCompositeKeys() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            for (boolean fullFat : new boolean[]{false, true}) {
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l CROSS JOIN lp_join_r r ORDER BY lid,rid", fullFat,
                        """
                                lid	rid
                                1	10
                                1	11
                                1	12
                                1	13
                                2	10
                                2	11
                                2	12
                                2	13
                                3	10
                                3	11
                                3	12
                                3	13
                                4	10
                                4	11
                                4	12
                                4	13
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l CROSS JOIN lp_join_r r WHERE l.k=r.k ORDER BY lid,rid", fullFat,
                        """
                                lid	rid
                                1	10
                                1	11
                                2	12
                                3	13
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k ORDER BY lid,rid", fullFat,
                        """
                                lid	rid
                                1	10
                                1	11
                                2	12
                                3	13
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l JOIN lp_join_r r ON(k) ORDER BY lid,rid", fullFat,
                        """
                                lid	rid
                                1	10
                                1	11
                                2	12
                                3	13
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k AND r.b=l.b ORDER BY lid,rid", fullFat,
                        """
                                lid	rid
                                1	10
                                2	12
                                3	13
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l JOIN lp_join_r r ON(k,b) ORDER BY lid,rid", fullFat,
                        """
                                lid	rid
                                1	10
                                2	12
                                3	13
                                """);
            }
        });
    }

    @Test
    public void testIndependentConjunctBindingPreservesBooleanNullAndParameterTypes() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String from = " FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k WHERE ";
            assertRows("SELECT l.id" + from + "null AND true ORDER BY l.id", false, "id\n");
            assertRows("SELECT l.id" + from + "false AND null ORDER BY l.id", false, "id\n");
            // LEFT ON keeps these terms in one matching predicate.
            assertRows("SELECT l.id FROM lp_join_l l LEFT JOIN lp_join_r r ON l.k=r.k "
                            + "AND l.id IN (1,2,3) AND null ORDER BY l.id", false,
                    """
                            id
                            1
                            2
                            3
                            4
                            """);
            assertRows("SELECT l.id FROM lp_join_l l LEFT JOIN lp_join_r r ON l.k=r.k "
                            + "AND null AND l.id IN (1,2,3) ORDER BY l.id", false,
                    """
                            id
                            1
                            2
                            3
                            4
                            """);
            assertRows("SELECT l.id" + from + "l.id<r.id AND true ORDER BY l.id", false,
                    """
                            id
                            1
                            1
                            2
                            3
                            """);
            assertQuery("SELECT l.id" + from + "l.id<r.id AND 1").noLeakCheck().fails(77, "boolean expression expected");
            assertQuery("SELECT l.id" + from + "null AND 1").noLeakCheck().fails(63, "expression type mismatch, expected: BOOLEAN, actual: NULL");
            assertQuery("SELECT l.id" + from + "1 AND null").noLeakCheck().fails(63, "expression type mismatch, expected: BOOLEAN, actual: INT");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                final String expected = """
                        id
                        1
                        1
                        2
                        3
                        """;
                try (RecordCursorFactory factory = compiler.compile("SELECT l.id" + from
                        + "l.id<r.id ORDER BY l.id", sqlExecutionContext).getRecordCursorFactory()) {
                    assertRowsOnly(factory, expected);
                }
                bindVariableService.clear();
                try (RecordCursorFactory factory = compiler.compile("SELECT l.id" + from
                        + "$1 AND l.id<r.id ORDER BY l.id", sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.assertEquals(ColumnType.BOOLEAN, bindVariableService.getFunction(0).getType());
                    bindVariableService.setBoolean(0, true);
                    assertRowsOnly(factory, expected);
                    bindVariableService.setBoolean(0, false);
                    assertRowsOnly(factory, "id\n");
                }
            }
        });
    }

    @Test
    public void testDisconnectedComponentAfterKeyedComponentIsMaterialized() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_comp_a AS (SELECT x id FROM long_sequence(100))");
            execute("CREATE TABLE lp_comp_d AS (SELECT x id FROM long_sequence(50))");
            execute("CREATE TABLE lp_comp_b AS (SELECT x k FROM long_sequence(1_000))");
            execute("CREATE TABLE lp_comp_c AS (SELECT x * 10 k FROM long_sequence(3))");
            final String sql = "SELECT count(), sum(a.id), sum(c.k) FROM lp_comp_a a JOIN lp_comp_d d ON a.id = d.id "
                    + "CROSS JOIN lp_comp_b b JOIN lp_comp_c c ON b.k = c.k";
            assertQuery(sql).noLeakCheck().noRandomAccess().expectSize().withPlan("""
                    GroupBy vectorized: false
                      values: [count(*),sum(a.id),sum(c.k)]
                        Cross Join
                            Hash Join Light
                              condition: d.id=a.id
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: lp_comp_a
                                Hash
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_comp_d
                            Materialize
                                Hash Join Light
                                  condition: c.k=b.k
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_comp_b
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_comp_c
                    """).returns("count\tsum\tsum1\n150\t3825\t3000\n");
            assertQuery(sql).noLeakCheck().fullFatJoins().noRandomAccess().expectSize().returns("count\tsum\tsum1\n150\t3825\t3000\n");
        });
    }

    @Test
    public void testDisconnectedLeadingCrossPreservesColumnsAndCompilerReuse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_cross_a (id INT)");
            execute("CREATE TABLE lp_cross_b (k INT)");
            execute("CREATE TABLE lp_cross_c (k INT)");
            execute("CREATE TABLE lp_cross_d (d INT)");
            execute("CREATE TABLE lp_cross_ts (id INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO lp_cross_a VALUES (1), (2)");
            execute("INSERT INTO lp_cross_b VALUES (1), (2), (null)");
            execute("INSERT INTO lp_cross_c VALUES (1), (null)");
            execute("INSERT INTO lp_cross_d VALUES (10), (20)");
            execute("INSERT INTO lp_cross_ts VALUES (1, '2020-01-01'), (2, '2020-01-02')");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (boolean isFullFat : new boolean[]{false, true}) {
                    compiler.setFullFatJoins(isFullFat);
                    try (RecordCursorFactory factory = compiler.compile(
                            "SELECT * FROM lp_cross_a a CROSS JOIN lp_cross_b b JOIN lp_cross_c c ON b.k=c.k ORDER BY a.id,b.k",
                            sqlExecutionContext
                    ).getRecordCursorFactory()) {
                        assertFactory(factory).withContext(sqlExecutionContext).sizeMayVary().returns("""
                                id\tk\tk1
                                1\tnull\tnull
                                1\t1\t1
                                2\tnull\tnull
                                2\t1\t1
                                """);
                    }
                    assertQuery("SELECT sum(a.id*d.d) FROM lp_cross_a a CROSS JOIN lp_cross_d d CROSS JOIN lp_cross_b b JOIN lp_cross_c c ON b.k=c.k")
                            .withCompiler(compiler).noLeakCheck().noRandomAccess().expectSize().returns("sum\n180\n");
                    assertQuery("SELECT count() FROM lp_cross_a a CROSS JOIN lp_cross_b b CROSS JOIN lp_cross_c c")
                            .withCompiler(compiler).noLeakCheck().noRandomAccess().expectSize().returns("count\n12\n");
                    assertQuery("SELECT sum(a.id) FROM lp_cross_a a JOIN lp_cross_b b ON a.id=b.k JOIN lp_cross_c c ON b.k=c.k")
                            .withCompiler(compiler).noLeakCheck().noRandomAccess().expectSize().returns("sum\n1\n");
                    assertQuery("SELECT a.id FROM (SELECT rnd_int() id FROM long_sequence(2)) a CROSS JOIN lp_cross_b b JOIN lp_cross_c c ON b.k=c.k")
                            .withCompiler(compiler).noLeakCheck().assertsPlanContaining("Cross Join\n        VirtualRecord");
                    for (String clause : new String[]{" WHERE ", " AND "}) {
                        assertQuery("SELECT count() FROM lp_cross_a a CROSS JOIN lp_cross_b b JOIN lp_cross_c c ON b.k=c.k"
                                + clause + "b.k-c.k<rnd_int(0,1,0)")
                                .withCompiler(compiler).noLeakCheck().assertsPlanContaining("condition: c.k=b.k\n            Cross Join");
                    }
                    assertQuery("SELECT a.ts,a.id FROM lp_cross_ts a CROSS JOIN lp_cross_b b JOIN lp_cross_c c ON b.k=c.k")
                            .withCompiler(compiler).noLeakCheck().noRandomAccess().expectSize(isFullFat).returns("""
                                    ts\tid
                                    2020-01-01T00:00:00.000000Z\t1
                                    2020-01-02T00:00:00.000000Z\t2
                                    2020-01-01T00:00:00.000000Z\t1
                                    2020-01-02T00:00:00.000000Z\t2
                                    """);
                    assertQuery("SELECT sum(a.id) FROM lp_cross_a a CROSS JOIN lp_cross_b b JOIN lp_cross_c c ON b.k=c.k")
                            .withCompiler(compiler).noLeakCheck().assertsPlanContaining("Cross Join\n        Hash Join");
                }
            }
        });
    }

    @Test
    public void testDisconnectedLeadingCrossTrailsKeyedJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_cross_a AS (SELECT x id FROM long_sequence(1000))");
            execute("CREATE TABLE lp_cross_b AS (SELECT x k FROM long_sequence(10_000))");
            execute("CREATE TABLE lp_cross_c AS (SELECT x k FROM long_sequence(1))");
            final String sql = "SELECT sum(a.id) FROM lp_cross_a a CROSS JOIN lp_cross_b b JOIN lp_cross_c c ON b.k=c.k";
            assertQuery(sql).noLeakCheck().noRandomAccess().expectSize().withPlan("""
                    GroupBy vectorized: false
                      values: [sum(a.id)]
                    """ + TRAILING_CROSS_PLAN.indent(4)).returns("sum\n500500\n");
            assertQuery(sql).noLeakCheck().fullFatJoins().noRandomAccess().expectSize().returns("sum\n500500\n");
        });
    }

    @Test
    public void testDisconnectedLeadingCrossTrailsKeyedJoinOverWindowInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_cross_a AS (SELECT x id FROM long_sequence(1000))");
            execute("CREATE TABLE lp_cross_b AS (SELECT x k FROM long_sequence(10_000))");
            execute("CREATE TABLE lp_cross_c AS (SELECT x + 9_999 k FROM long_sequence(1))");
            final String join = "lp_cross_a a CROSS JOIN (SELECT k, row_number() OVER () rn FROM lp_cross_b) b JOIN lp_cross_c c ON b.k=c.k";
            assertQuery("SELECT sum(a.id) FROM " + join).noLeakCheck().noRandomAccess().expectSize().withPlan("""
                    GroupBy vectorized: false
                      values: [sum(a.id)]
                    """ + TRAILING_CROSS_PLAN.indent(4)).returns("sum\n500500\n");
            assertQuery("SELECT sum(a.id), sum(b.rn) FROM " + join).noLeakCheck().noRandomAccess().expectSize().withPlan("""
                    GroupBy vectorized: false
                      values: [sum(a.id),sum(b.rn)]
                        Cross Join
                            Hash Join Light
                              condition: c.k=b.k
                                Window
                                  functions: [row_number()]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_cross_b
                                Hash
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_cross_c
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_cross_a
                    """).returns("sum\tsum1\n500500\t10000000\n");
        });
    }

    @Test
    public void testDisconnectedLeadingCrossTrailsKeyedJoinUnlessReadWindowInputIsUnordered() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_cross_a (id INT)");
            execute("CREATE TABLE lp_cross_b (k INT)");
            execute("CREATE TABLE lp_cross_c (k INT)");
            execute("INSERT INTO lp_cross_a VALUES (1), (2)");
            execute("INSERT INTO lp_cross_b VALUES (1), (2), (3)");
            execute("INSERT INTO lp_cross_c VALUES (2), (3)");
            final String join = "lp_cross_a a CROSS JOIN (SELECT k, row_number() OVER () rn FROM (SELECT k, count() n FROM lp_cross_b)) b "
                    + "JOIN lp_cross_c c ON b.k=c.k";
            assertQuery("SELECT sum(a.id) FROM " + join).noLeakCheck().noRandomAccess().expectSize().withPlan("""
                    GroupBy vectorized: false
                      values: [sum(a.id)]
                        Cross Join
                            Hash Join Light
                              condition: c.k=b.k
                                GroupBy vectorized: true workers: 1
                                  keys: [k]
                                  values: [count(*)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_cross_b
                                Hash
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_cross_c
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_cross_a
                    """).returns("sum\n6\n");
            assertQuery("SELECT sum(a.id) FROM " + join + " WHERE b.rn > 0").noLeakCheck().noRandomAccess().expectSize().withPlan("""
                    GroupBy vectorized: false
                      values: [sum(a.id)]
                        Hash Join Light
                          condition: c.k=b.k
                            Cross Join
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: lp_cross_a
                                SelectedRecord
                                    Filter filter: 0<rn
                                        Window
                                          functions: [row_number()]
                                            GroupBy vectorized: true workers: 1
                                              keys: [k]
                                              values: [count(*)]
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_cross_b
                            Hash
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: lp_cross_c
                    """).returns("sum\n6\n");
        });
    }

    @Test
    public void testDisconnectedLeadingCrossTrailsKeyedOuterJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_outer_a AS (SELECT x id FROM long_sequence(1000))");
            execute("CREATE TABLE lp_outer_b AS (SELECT x id FROM long_sequence(10_000))");
            execute("CREATE TABLE lp_outer_c AS (SELECT x id FROM long_sequence(2))");
            execute("CREATE TABLE lp_outer_d AS (SELECT x id FROM long_sequence(1))");
            final String keyed = """
                    Hash Join Light
                      condition: c.id=b.id
                        PageFrame
                            Row forward scan
                            Frame forward scan on: lp_outer_b
                        Hash
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_outer_c
                    """;
            final String scanA = """
                    PageFrame
                        Row forward scan
                        Frame forward scan on: lp_outer_a
                    """;
            final String scanD = """
                    PageFrame
                        Row forward scan
                        Frame forward scan on: lp_outer_d
                    """;
            final String join = "lp_outer_a a CROSS JOIN lp_outer_b b JOIN lp_outer_c c ON b.id = c.id ";
            final String leftSql = "SELECT count() FROM " + join + "LEFT JOIN lp_outer_d d ON c.id = d.id";
            assertQuery(leftSql).noLeakCheck().noRandomAccess().expectSize().withPlan("""
                    Count
                        Cross Join
                            Hash Left Outer Join Light
                              condition: d.id=c.id
                    """ + keyed.indent(12) + "            Hash\n" + scanD.indent(16) + scanA.indent(8)).returns("count\n2000\n");
            assertQuery(leftSql).noLeakCheck().fullFatJoins().noRandomAccess().expectSize().returns("count\n2000\n");
            assertQuery("SELECT count(), sum(a.id), sum(d.id) FROM " + join + "LEFT JOIN lp_outer_d d ON c.id > d.id")
                    .noLeakCheck().noRandomAccess().expectSize().withPlan("""
                            GroupBy vectorized: false
                              values: [count(*),sum(a.id),sum(d.id)]
                                Cross Join
                                    Nested Loop Left Join
                                      filter: d.id<c.id
                            """ + keyed.indent(12) + scanD.indent(12) + scanA.indent(8))
                    .returns("count\tsum\tsum1\n2000\t1001000\t1000\n");
            assertQuery("SELECT count(), sum(a.id), sum(c.id), sum(d.id) FROM " + join + "RIGHT JOIN lp_outer_d d ON c.id = d.id")
                    .noLeakCheck().noRandomAccess().expectSize().withPlan("""
                            GroupBy vectorized: false
                              values: [count(*),sum(a.id),sum(c.id),sum(d.id)]
                                Hash Right Outer Join Light
                                  condition: d.id=c.id
                                    Cross Join
                            """ + scanA.indent(12) + "            Materialize\n" + keyed.indent(16) + "        Hash\n" + scanD.indent(12))
                    .returns("count\tsum\tsum1\tsum2\n1000\t500500\t1000\t1000\n");
        });
    }

    @Test
    public void testDisconnectedLeadingCrossTrailsKeylessOuterJoinReadByLaterKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_keyless_a AS (SELECT x id FROM long_sequence(1000))");
            execute("CREATE TABLE lp_keyless_b AS (SELECT x id FROM long_sequence(10_000))");
            execute("CREATE TABLE lp_keyless_c AS (SELECT x id FROM long_sequence(2))");
            execute("CREATE TABLE lp_keyless_d AS (SELECT x id FROM long_sequence(1))");
            execute("CREATE TABLE lp_keyless_e AS (SELECT x id FROM long_sequence(3))");
            final String join = "lp_keyless_a a CROSS JOIN lp_keyless_b b JOIN lp_keyless_c c ON b.id = c.id LEFT JOIN lp_keyless_d d ON true ";
            final String sql = "SELECT count(), sum(a.id), sum(c.id), sum(e.id) FROM " + join + "JOIN lp_keyless_e e ON e.id = d.id";
            assertQuery(sql).noLeakCheck().noRandomAccess().expectSize().withPlan("""
                    GroupBy vectorized: false
                      values: [count(*),sum(a.id),sum(c.id),sum(e.id)]
                        Cross Join
                            Hash Join Light
                              condition: e.id=d.id
                                Nested Loop Left Join
                                  filter: true
                                    Hash Join Light
                                      condition: c.id=b.id
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_keyless_b
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_keyless_c
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_keyless_d
                                Hash
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_keyless_e
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_keyless_a
                    """).returns("count\tsum\tsum1\tsum2\n2000\t1001000\t3000\t2000\n");
            assertQuery(sql).noLeakCheck().fullFatJoins().noRandomAccess().expectSize()
                    .returns("count\tsum\tsum1\tsum2\n2000\t1001000\t3000\t2000\n");
            assertQuery("SELECT count(), sum(a.id), sum(e.id) FROM " + join + "LEFT JOIN lp_keyless_d d2 ON true JOIN lp_keyless_e e ON e.id = d2.id")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tsum\tsum1\n2000\t1001000\t2000\n");
        });
    }

    @Test
    public void testDisconnectedTimestampedLeadingCrossCopiesComponentColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_copy_a (id INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_copy_b (k INT, s SYMBOL, v VARCHAR, str STRING, arr DOUBLE[])");
            execute("CREATE TABLE lp_copy_c (k INT, d DOUBLE)");
            execute("INSERT INTO lp_copy_a VALUES (1, '2020-01-01T00:00:00Z'), (2, '2020-01-01T00:00:01Z'), (3, '2020-01-01T00:00:02Z')");
            execute("""
                    INSERT INTO lp_copy_b VALUES
                        (1, 'x', 'vx', 'sx', ARRAY[1.0, 2.0]),
                        (2, 'y', 'vy', 'sy', ARRAY[3.0]),
                        (3, null, null, null, null)
                    """);
            execute("INSERT INTO lp_copy_c VALUES (1, 0.5), (3, 1.5)");
            final String sql = "SELECT * FROM (SELECT a.id, a.ts, b.s, b.v, b.str, b.arr, c.d FROM lp_copy_a a "
                    + "CROSS JOIN lp_copy_b b JOIN lp_copy_c c ON b.k = c.k) TIMESTAMP(ts)";
            final String rows = """
                    id\tts\ts\tv\tstr\tarr\td
                    1\t2020-01-01T00:00:00.000000Z\tx\tvx\tsx\t[1.0,2.0]\t0.5
                    1\t2020-01-01T00:00:00.000000Z\t\t\t\tnull\t1.5
                    2\t2020-01-01T00:00:01.000000Z\tx\tvx\tsx\t[1.0,2.0]\t0.5
                    2\t2020-01-01T00:00:01.000000Z\t\t\t\tnull\t1.5
                    3\t2020-01-01T00:00:02.000000Z\tx\tvx\tsx\t[1.0,2.0]\t0.5
                    3\t2020-01-01T00:00:02.000000Z\t\t\t\tnull\t1.5
                    """;
            assertQuery(sql).noLeakCheck().noRandomAccess().timestamp("ts")
                    .withPlanContaining("""
                            SelectedRecord
                                Cross Join
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_copy_a
                                    Materialize
                                        Hash Join Light
                            """).returns(rows);
            assertQuery(sql).noLeakCheck().fullFatJoins().noRandomAccess().expectSize().timestamp("ts").returns(rows);
            assertQuery(sql + " LIMIT 1, 4").noLeakCheck().inferRandomAccess().sizeMayVary().timestamp("ts").returns("""
                    id\tts\ts\tv\tstr\tarr\td
                    1\t2020-01-01T00:00:00.000000Z\t\t\t\tnull\t1.5
                    2\t2020-01-01T00:00:01.000000Z\tx\tvx\tsx\t[1.0,2.0]\t0.5
                    2\t2020-01-01T00:00:01.000000Z\t\t\t\tnull\t1.5
                    """);
            assertQuery("SELECT a.ts, count_distinct(b.s) n, first(b.v) v, last(c.d) d FROM lp_copy_a a CROSS JOIN lp_copy_b b "
                    + "JOIN lp_copy_c c ON b.k = c.k SAMPLE BY 1s ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().timestamp("ts").returns("""
                            ts\tn\tv\td
                            2020-01-01T00:00:00.000000Z\t1\tvx\t1.5
                            2020-01-01T00:00:01.000000Z\t1\tvx\t1.5
                            2020-01-01T00:00:02.000000Z\t1\tvx\t1.5
                            """);
        });
    }

    @Test
    public void testDisconnectedTimestampedLeadingCrossKeepsUnstableComponentLeftDeep() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_cross_a (id INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_cross_c (k INT)");
            execute("INSERT INTO lp_cross_a VALUES (1, '2020-01-01T00:00:00Z'), (2, '2020-01-01T00:00:01Z'), (3, '2020-01-01T00:00:02Z')");
            execute("INSERT INTO lp_cross_c VALUES (1)");
            assertQuery("SELECT twap(a.id, a.ts) FROM lp_cross_a a CROSS JOIN (SELECT rnd_int(1, 2, 0) * 0 + 1 k FROM long_sequence(2)) b "
                    + "JOIN lp_cross_c c ON b.k = c.k").noLeakCheck().inferRandomAccess().sizeMayVary().withPlan("""
                    GroupBy vectorized: false
                      values: [twap(a.id,a.ts)]
                        Hash Join Light
                          condition: c.k=b.k
                            Cross Join
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: lp_cross_a
                                VirtualRecord
                                  functions: [rnd_int(1,2,0)*0+1]
                                    long_sequence count: 2
                            Hash
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: lp_cross_c
                    """).returns("twap\n1.5\n");
        });
    }

    @Test
    public void testDisconnectedTimestampedLeadingCrossReplaysLargeComponent() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_large_a AS (SELECT x::INT id, timestamp_sequence(0, 1_000_000) ts FROM long_sequence(3)) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_large_b AS (SELECT (x % 100)::INT k, x v FROM long_sequence(100_000))");
            execute("CREATE TABLE lp_large_c AS (SELECT x::INT k FROM long_sequence(10))");
            final String sql = "SELECT count() n, sum(b.v) s, twap(a.id, a.ts) t FROM lp_large_a a CROSS JOIN lp_large_b b "
                    + "JOIN lp_large_c c ON b.k = c.k";
            final String result = "n\ts\tt\n30000\t1498665000\t1.5\n";
            assertQuery(sql).noLeakCheck().noRandomAccess().expectSize().withPlanContaining("""
                        Cross Join
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_large_a
                            Materialize
                    """).returns(result);
            assertQuery(sql).noLeakCheck().fullFatJoins().noRandomAccess().expectSize().returns(result);
        });
    }

    @Test
    public void testDisconnectedTimestampedLeadingCrossTrailsUnderOrderDependentReads() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_cross_a (id INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_cross_b (k INT)");
            execute("CREATE TABLE lp_cross_c (k INT)");
            execute("INSERT INTO lp_cross_a VALUES (1, '2020-01-01T00:00:00Z'), (2, '2020-01-01T00:00:01Z'), (3, '2020-01-01T00:00:02Z')");
            execute("INSERT INTO lp_cross_b VALUES (1), (2), (3)");
            execute("INSERT INTO lp_cross_c VALUES (2), (3)");
            final String join = "lp_cross_a a CROSS JOIN lp_cross_b b JOIN lp_cross_c c ON b.k=c.k";
            assertQuery("SELECT first(a.id), last(a.id), count() FROM " + join).noLeakCheck().inferRandomAccess().sizeMayVary().withPlan("""
                    GroupBy vectorized: false
                      values: [first(a.id),last(a.id),count(*)]
                    """ + TRAILING_CROSS_PLAN.indent(4)).returns("first\tlast\tcount\n1\t3\t6\n");
            assertQuery("SELECT string_agg(a.id::VARCHAR, ',') s FROM " + join).noLeakCheck().inferRandomAccess().sizeMayVary().withPlan("""
                    GroupBy vectorized: false
                      values: [string_agg(a.id::varchar,,)]
                    """ + TRAILING_CROSS_PLAN.indent(4)).returns("s\n1,2,3,1,2,3\n");
            assertQuery("SELECT a.id, row_number() OVER () rn FROM " + join).noLeakCheck().inferRandomAccess().sizeMayVary().withPlan("""
                    Window
                      functions: [row_number()]
                        SelectedRecord
                    """ + TRAILING_CROSS_PLAN.indent(8)).returns("""
                    id\trn
                    1\t1
                    2\t2
                    3\t3
                    1\t4
                    2\t5
                    3\t6
                    """);
            assertQuery("SELECT sum(rn) FROM (SELECT a.id, row_number() OVER (ORDER BY a.id) rn FROM " + join + ")")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().withPlan("""
                            GroupBy vectorized: false
                              values: [sum(rn)]
                                CachedWindow
                                  orderedFunctions: [[id] => [row_number()]]
                                    SelectedRecord
                            """ + TRAILING_CROSS_PLAN.indent(12)).returns("sum\n21\n");
            assertQuery("SELECT a.id, b.k, avg(a.id, 'alpha', 0.5) OVER (ORDER BY a.ts, b.k) e FROM " + join)
                    .noLeakCheck().inferRandomAccess().sizeMayVary().withPlan("""
                            CachedWindow
                              orderedFunctions: [[ts, k] => [avg(id, 'alpha', 0.5) over (rows between unbounded preceding and current row)]]
                                SelectedRecord
                            """ + TRAILING_CROSS_PLAN.indent(8)).returns("""
                            id\tk\te
                            1\t2\t1.0
                            2\t2\t1.5
                            3\t2\t2.375
                            1\t3\t1.0
                            2\t3\t1.75
                            3\t3\t2.6875
                            """);
            assertQuery("SELECT a.id, b.k, avg(a.id, 'alpha', 0.5, a.id) OVER (ORDER BY a.ts, b.k) e FROM " + join)
                    .noLeakCheck().inferRandomAccess().sizeMayVary().withPlan("""
                            CachedWindow
                              orderedFunctions: [[ts, k] => [avg(id, 'alpha', 0.5, id) over (rows between unbounded preceding and current row)]]
                                SelectedRecord
                            """ + TRAILING_CROSS_PLAN.indent(8)).returns("""
                            id\tk\te
                            1\t2\t1.0
                            2\t2\t1.6666666666666667
                            3\t2\t2.5789473684210527
                            1\t3\t1.0
                            2\t3\t1.8571428571428572
                            3\t3\t2.813953488372093
                            """);
            assertQuery("SELECT a.id, b.k, sum(a.id) OVER (ORDER BY a.ts, b.k ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) s FROM " + join)
                    .noLeakCheck().inferRandomAccess().sizeMayVary().withPlan("""
                            CachedWindow
                              orderedFunctions: [[ts, k] => [sum(id) over ( rows between 1 preceding and current row)]]
                                SelectedRecord
                            """ + TRAILING_CROSS_PLAN.indent(8)).returns("""
                            id\tk\ts
                            1\t2\t1.0
                            2\t2\t3.0
                            3\t2\t5.0
                            1\t3\t2.0
                            2\t3\t4.0
                            3\t3\t6.0
                            """);
            assertQuery("SELECT a.id, b.k, lag(a.id) OVER (ORDER BY a.ts, b.k) p FROM " + join)
                    .noLeakCheck().inferRandomAccess().sizeMayVary().withPlan("""
                            CachedWindow
                              orderedFunctions: [[ts, k] => [lag(id, 1, NULL) over ()]]
                                SelectedRecord
                            """ + TRAILING_CROSS_PLAN.indent(8)).returns("""
                            id\tk\tp
                            1\t2\tnull
                            2\t2\t1
                            3\t2\t2
                            1\t3\t1
                            2\t3\t2
                            3\t3\t3
                            """);
        });
    }

    @Test
    public void testDisconnectedTimestampedLeadingCrossTrailsUnderUnreadOrderedColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_cross_a (id INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_cross_b (k INT)");
            execute("CREATE TABLE lp_cross_c (k INT)");
            execute("INSERT INTO lp_cross_a VALUES (1, '2020-01-01T00:00:00Z'), (2, '2020-01-01T00:00:01Z'), (3, '2020-01-01T00:00:02Z')");
            execute("INSERT INTO lp_cross_b VALUES (1), (2), (3)");
            execute("INSERT INTO lp_cross_c VALUES (2)");
            final String join = "lp_cross_a a CROSS JOIN lp_cross_b b JOIN lp_cross_c c ON b.k=c.k";
            assertQuery("SELECT count() FROM (SELECT a.id, row_number() OVER (ORDER BY a.id) rn FROM " + join + ")")
                    .noLeakCheck().noRandomAccess().expectSize().withPlan("""
                            Count
                            """ + TRAILING_CROSS_PLAN.indent(4)).returns("count\n3\n");
            assertQuery("SELECT count() FROM (SELECT a.id + 1 x, sum(a.id) OVER (ORDER BY a.ts RANGE BETWEEN 1 SECOND PRECEDING AND CURRENT ROW) s FROM " + join + ")")
                    .noLeakCheck().noRandomAccess().expectSize().withPlan("""
                            Count
                                VirtualRecord
                            """ + TRAILING_CROSS_PLAN.indent(8)).returns("count\n3\n");
            assertQuery("SELECT count() FROM (SELECT b.k, first(a.id) f FROM " + join + ")")
                    .noLeakCheck().noRandomAccess().expectSize().withPlan("""
                            Count
                                GroupBy vectorized: false
                                  keys: [k]
                            """ + TRAILING_CROSS_PLAN.indent(8)).returns("count\n1\n");
            assertQuery("SELECT sum(id) FROM (SELECT a.id, rn FROM (SELECT a.id, row_number() OVER (ORDER BY a.ts) rn FROM " + join + ") a)")
                    .noLeakCheck().noRandomAccess().expectSize().withPlan("""
                            GroupBy vectorized: false
                              values: [sum(id)]
                                SelectedRecord
                            """ + TRAILING_CROSS_PLAN.indent(8)).returns("sum\n6\n");
            assertQuery("SELECT sum(s) FROM (SELECT a.id, sum(a.id) OVER (ORDER BY a.ts RANGE BETWEEN 1 SECOND PRECEDING AND CURRENT ROW) s FROM " + join + ")")
                    .noLeakCheck().noRandomAccess().expectSize().withPlan("""
                            GroupBy vectorized: false
                              values: [sum(s)]
                                Window
                                  functions: [sum(id) over (range between 1000000 preceding and current row)]
                                    SelectedRecord
                            """ + LEADING_CROSS_PLAN.indent(12)).returns("sum\n9.0\n");
        });
    }

    @Test
    public void testDisconnectedTimestampedLeadingCrossTrailsUnlessTimestampIsRequired() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_cross_a (id INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_cross_b (k INT)");
            execute("CREATE TABLE lp_cross_c (k INT)");
            execute("CREATE TABLE lp_cross_q (v INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO lp_cross_a VALUES (1, '2020-01-01T00:00:00Z'), (2, '2020-01-01T00:00:01Z'), (3, '2020-01-01T00:00:02Z')");
            execute("INSERT INTO lp_cross_b VALUES (1), (2), (3)");
            execute("INSERT INTO lp_cross_c VALUES (2)");
            execute("INSERT INTO lp_cross_q VALUES (10, '2020-01-01T00:00:00Z'), (20, '2020-01-01T00:00:01.500000Z')");
            final String join = "lp_cross_a a CROSS JOIN lp_cross_b b JOIN lp_cross_c c ON b.k=c.k";
            final String rows = """
                    id\tts
                    1\t2020-01-01T00:00:00.000000Z
                    2\t2020-01-01T00:00:01.000000Z
                    3\t2020-01-01T00:00:02.000000Z
                    """;
            assertQuery("SELECT * FROM " + join).noLeakCheck().inferRandomAccess().sizeMayVary().withPlan("""
                    SelectedRecord
                    """ + TRAILING_CROSS_PLAN.indent(4)).returns("""
                    id\tts\tk\tk1
                    1\t2020-01-01T00:00:00.000000Z\t2\t2
                    2\t2020-01-01T00:00:01.000000Z\t2\t2
                    3\t2020-01-01T00:00:02.000000Z\t2\t2
                    """);
            assertQuery("SELECT a.ts, a.id FROM " + join + " ORDER BY a.ts LIMIT 1")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().timestamp("ts").withPlan("""
                            Limit value: 1 skip-rows-max: 0 take-rows-max: 1
                                Encode sort
                                  keys: [ts]
                                    SelectedRecord
                            """ + TRAILING_CROSS_PLAN.indent(12)).returns("ts\tid\n2020-01-01T00:00:00.000000Z\t1\n");
            assertQuery("SELECT DISTINCT a.id FROM " + join).noLeakCheck().assertsPlan("""
                    GroupBy vectorized: false
                      keys: [id]
                    """ + TRAILING_CROSS_PLAN.indent(4));
            assertQuery("SELECT x.id, q.v FROM (SELECT a.id, a.ts FROM " + join + ") x ASOF JOIN lp_cross_q q")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().withPlan("""
                            SelectedRecord
                                AsOf Join Fast
                                    SelectedRecord
                            """ + LEADING_CROSS_PLAN.indent(12) + """
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_cross_q
                            """).returns("id\tv\n1\t10\n2\t10\n3\t20\n");
            assertQuery("SELECT a.ts, count() FROM " + join + " SAMPLE BY 1s ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().timestamp("ts").withPlan("""
                            Sample By
                              fill: none
                              values: [count(*)]
                                SelectedRecord
                            """ + LEADING_CROSS_PLAN.indent(8)).returns("""
                            ts\tcount
                            2020-01-01T00:00:00.000000Z\t1
                            2020-01-01T00:00:01.000000Z\t1
                            2020-01-01T00:00:02.000000Z\t1
                            """);
            assertQuery("SELECT twap(a.id, a.ts) FROM " + join).noLeakCheck().inferRandomAccess().sizeMayVary().withPlan("""
                    GroupBy vectorized: false
                      values: [twap(a.id,a.ts)]
                    """ + LEADING_CROSS_PLAN.indent(4)).returns("twap\n1.5\n");
            assertQuery("SELECT id, ts FROM (SELECT a.id, a.ts, b.k FROM " + join + ") LATEST ON ts PARTITION BY k")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().timestamp("ts").withPlan("""
                            SelectedRecord
                                LatestBy
                                    SelectedRecord
                            """ + LEADING_CROSS_PLAN.indent(12)).returns("id\tts\n3\t2020-01-01T00:00:02.000000Z\n");
            assertQuery("SELECT id, sum(id) OVER (ORDER BY ts RANGE BETWEEN 1 SECOND PRECEDING AND CURRENT ROW) s FROM (SELECT a.id, a.ts FROM " + join + ")")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().withPlan("""
                            Window
                              functions: [sum(id) over (range between 1000000 preceding and current row)]
                                SelectedRecord
                            """ + LEADING_CROSS_PLAN.indent(8)).returns("id\ts\n1\t1.0\n2\t3.0\n3\t5.0\n");
            assertQuery("SELECT avg(a.id, 'second', 1) OVER (ORDER BY a.ts) e FROM " + join)
                    .noLeakCheck().inferRandomAccess().sizeMayVary().withPlan("""
                            Window
                              functions: [avg(id, 'second', 1.0) over (rows between unbounded preceding and current row)]
                                SelectedRecord
                            """ + LEADING_CROSS_PLAN.indent(8)).returns("e\n1.0\n1.6321205588285577\n2.496785275591945\n");
            assertQuery("SELECT avg(a.id, 'second', 1, a.id) OVER (ORDER BY a.ts) e FROM " + join)
                    .noLeakCheck().inferRandomAccess().sizeMayVary().withPlan("""
                            Window
                              functions: [avg(id, 'second', 1.0, id) over (rows between unbounded preceding and current row)]
                                SelectedRecord
                            """ + LEADING_CROSS_PLAN.indent(8)).returns("e\n1.0\n1.7746003264394359\n2.7053175178756814\n");
            assertQuery("SELECT sum(a.id) OVER (ORDER BY a.ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) s FROM " + join)
                    .noLeakCheck().inferRandomAccess().sizeMayVary().withPlan("""
                            Window
                              functions: [sum(id) over (range between unbounded preceding and 1 preceding)]
                                SelectedRecord
                            """ + LEADING_CROSS_PLAN.indent(8)).returns("s\nnull\n1.0\n3.0\n");
            assertQuery("SELECT a.id, a.ts FROM " + join + " SUBSAMPLE uniform(2)").noLeakCheck().inferRandomAccess().sizeMayVary().timestamp("ts").withPlan("""
                    SelectedRecord
                        Filter filter: __keep_subsample
                            CachedWindow
                              unorderedFunctions: [uniform(2) over (order by [ts])]
                                SelectedRecord
                    """ + LEADING_CROSS_PLAN.indent(16)).returns("""
                    id\tts
                    1\t2020-01-01T00:00:00.000000Z
                    3\t2020-01-01T00:00:02.000000Z
                    """);
            assertQuery("SELECT * FROM (SELECT a.id, a.ts FROM " + join + ") TIMESTAMP(ts)")
                    .noLeakCheck().inferRandomAccess().sizeMayVary().timestamp("ts").withPlan("""
                            SelectedRecord
                            """ + LEADING_CROSS_PLAN.indent(4)).returns(rows);
            execute("CREATE TABLE lp_cross_copy AS (SELECT a.id, a.ts FROM " + join + ")");
            assertQuery("SELECT * FROM lp_cross_copy").noLeakCheck().timestamp("ts").expectSize().returns(rows);
        });
    }

    @Test
    public void testDisconnectedTimestampedLeadingCrossUnderLateralRead() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_cross_a (id INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_cross_b (k INT)");
            execute("CREATE TABLE lp_cross_c (k INT)");
            execute("INSERT INTO lp_cross_a VALUES (1, '2020-01-01T00:00:00Z'), (2, '2020-01-01T00:00:01Z'), (3, '2020-01-01T00:00:02Z')");
            execute("INSERT INTO lp_cross_b VALUES (1), (2), (3)");
            execute("INSERT INTO lp_cross_c VALUES (2), (3)");
            assertQuery("""
                    SELECT x.id, l.v
                    FROM (SELECT a.id, sum(a.id) OVER (ORDER BY a.ts RANGE BETWEEN 1 SECOND PRECEDING AND CURRENT ROW) s
                          FROM lp_cross_a a CROSS JOIN lp_cross_b b JOIN lp_cross_c c ON b.k=c.k) x
                    JOIN LATERAL (SELECT x.s v FROM long_sequence(1)) l ON true
                    """)
                    .noLeakCheck().inferRandomAccess().sizeMayVary().withPlanContaining(LEADING_CROSS_PLAN.indent(16)).returns("""
                            id\tv
                            1\t1.0
                            1\t2.0
                            2\t4.0
                            2\t6.0
                            3\t7.0
                            3\t10.0
                            """);
            assertQuery("""
                    SELECT x.id, l.v
                    FROM (SELECT a.id, row_number() OVER () rn FROM lp_cross_a a CROSS JOIN lp_cross_b b JOIN lp_cross_c c ON b.k=c.k) x
                    JOIN LATERAL (SELECT x.rn v FROM long_sequence(1)) l ON true
                    """)
                    .noLeakCheck().inferRandomAccess().sizeMayVary().withPlanContaining(TRAILING_CROSS_PLAN.indent(16)).returns("""
                            id\tv
                            1\t1
                            2\t2
                            3\t3
                            1\t4
                            2\t5
                            3\t6
                            """);
        });
    }

    @Test
    public void testInnerConstantTermsCombineAcrossOriginsButOuterOnStaysSeparate() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k AND null WHERE true").noLeakCheck().fails(61, "boolean expression expected");
            assertRows("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k AND null WHERE false", false, "id\n");
            assertRows("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k AND null "
                            + "JOIN lp_join_l x ON r.k=x.k AND true ORDER BY l.id", false,
                    "id\n");
            assertQuery("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k AND true "
                    + "JOIN lp_join_l x ON r.k=x.k AND null").noLeakCheck().fails(98, "boolean expression expected");
            assertQuery("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k AND null "
                    + "JOIN lp_join_l x ON r.k=x.k WHERE l.id>0").noLeakCheck().fails(61, "boolean expression expected");
            assertRows("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k "
                            + "JOIN lp_join_l x ON r.k=x.k WHERE null AND true ORDER BY l.id", false,
                    "id\n");
            assertRows("SELECT l.id FROM lp_join_l l LEFT JOIN lp_join_r r ON l.k=r.k "
                    + "AND null AND l.id IN (1,2,3) ORDER BY l.id", false, """
                    id
                    1
                    2
                    3
                    4
                    """);
            assertRows("SELECT l.id FROM lp_join_l l RIGHT JOIN lp_join_r r ON l.k=r.k "
                    + "AND null AND l.id IN (1,2,3) ORDER BY l.id", false, """
                    id
                    null
                    null
                    null
                    null
                    """);
            assertRows("SELECT l.id FROM lp_join_l l FULL JOIN lp_join_r r ON l.k=r.k "
                    + "AND null AND l.id IN (1,2,3) ORDER BY l.id", false, """
                    id
                    null
                    null
                    null
                    null
                    1
                    2
                    3
                    4
                    """);
            assertQuery("SELECT l.id FROM lp_join_l l CROSS JOIN lp_join_r r ON l.k=r.k AND null").noLeakCheck().fails(52, "Cross joins cannot have join clauses");
        });
    }

    @Test
    public void testInnerThenLeftJoinUsesAllThreeSources() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            for (boolean isFullFat : new boolean[]{false, true}) {
                assertRows("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k "
                                + "LEFT JOIN lp_join_r x ON l.k=x.k ORDER BY l.id", isFullFat,
                        """
                                id
                                1
                                1
                                1
                                1
                                2
                                3
                                """);
            }
        });
    }

    @Test
    public void testJoinFactoriesAndExplainSurviveCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            RecordCursorFactory retained = null;
            RecordCursorFactory explain = null;
            final String expected = """
                    lid	rid
                    1	10
                    2	null
                    3	13
                    4	null
                    """;
            String expectedPlan;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                final String sql = "SELECT l.id lid,r.id rid FROM lp_join_l l LEFT JOIN lp_join_r r ON l.k=r.k AND r.v IN (11,31) ORDER BY lid,rid";
                try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    assertRowsOnly(factory, expected);
                }
                try {
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    Assert.assertNotNull(compiler.getPlanForTesting());
                    explain = compiler.compile("EXPLAIN " + sql, sqlExecutionContext).getRecordCursorFactory();
                    expectedPlan = printFactory(explain);
                    assertFailure(compiler, "SELECT lp_missing_fn(l.id) FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k", "unknown function name");
                    try (RecordCursorFactory factory = compiler.compile("SELECT count() FROM lp_join_l", sqlExecutionContext).getRecordCursorFactory()) {
                        TestUtils.assertEquals("count\n4\n", printFactory(factory));
                    }
                } catch (Throwable th) {
                    if (retained != null) {
                        retained.close();
                    }
                    if (explain != null) {
                        explain.close();
                    }
                    throw th;
                }
            }
            try (RecordCursorFactory factory = retained; RecordCursorFactory plan = explain) {
                assertRowsOnly(factory, expected);
                TestUtils.assertEquals(expectedPlan, printFactory(plan));
            }
        });
    }

    @Test
    public void testJoinPruningRetainsSurvivingTimestampAliasDesignation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_join_ts_l(id INT,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_join_ts_r(id INT)");
            execute("INSERT INTO lp_join_ts_l VALUES (1,'2020-01-01'),(2,'2020-01-02')");
            execute("INSERT INTO lp_join_ts_r VALUES (1),(2)");
            final String sql = "SELECT q.b FROM (SELECT ts a,ts b,id FROM lp_join_ts_l) q JOIN lp_join_ts_r r ON q.id=r.id";
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (boolean fullFat : new boolean[]{false, true}) {
                    compiler.setFullFatJoins(fullFat);
                    try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertEquals(fullFat ? 0 : -1, factory.getMetadata().getTimestampIndex());
                        assertRowsOnly(factory, "b\n2020-01-01T00:00:00.000000Z\n2020-01-02T00:00:00.000000Z\n");
                    }
                    try (RecordCursorFactory factory = compiler.compile(sql + " ORDER BY q.b", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertEquals(0, factory.getMetadata().getTimestampIndex());
                        assertRowsOnly(factory, "b\n2020-01-01T00:00:00.000000Z\n2020-01-02T00:00:00.000000Z\n");
                    }
                }
            }
        });
    }

    @Test
    public void testJoinValidationAndMissingTimestampRecover() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k").noLeakCheck().fails(7, "Ambiguous column [name=id]");
            assertQuery("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON k=r.k").noLeakCheck().fails(49, "Ambiguous column [name=k]");
            assertQuery("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=missing.k").noLeakCheck().fails(53, "Invalid table name or alias");
            assertQuery("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.missing=r.k").noLeakCheck().fails(49, "Invalid column: l.missing");
            assertQuery("SELECT l.id FROM lp_join_l l JOIN lp_join_r l ON l.k=l.k").noLeakCheck().fails(44, "Duplicate table or alias: l");
            assertQuery("SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.ts").noLeakCheck().fails(53, "join column type mismatch");
            assertQuery("SELECT l.id FROM lp_join_l l SPLICE JOIN lp_join_r r ON l.k=r.k JOIN lp_join_r x ON l.k=x.k").noLeakCheck().fails(29, "left side of time series join has no timestamp");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertFailure(compiler, "SELECT l.id FROM lp_join_l l SPLICE JOIN lp_join_r r ON l.k=r.k JOIN lp_join_r x ON l.k=x.k", "left side of time series join has no timestamp");
                try (RecordCursorFactory factory = compiler.compile("SELECT l.id lid,r.id rid FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k ORDER BY lid,rid", sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.assertNotNull(compiler.getPlanForTesting());
                    assertRowsOnly(factory, "lid\trid\n1\t10\n1\t11\n2\t12\n3\t13\n");
                }
            }
        });
    }

    @Test
    public void testLeftOnResidualAndWhereApplyAtDifferentBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            for (boolean fullFat : new boolean[]{false, true}) {
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l LEFT JOIN lp_join_r r ON l.k=r.k AND l.v<r.v ORDER BY lid,rid", fullFat,
                        """
                                lid	rid
                                1	10
                                2	null
                                3	13
                                4	null
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l LEFT JOIN lp_join_r r ON l.k=r.k WHERE l.v<r.v ORDER BY lid,rid", fullFat,
                        """
                                lid	rid
                                1	10
                                3	13
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l LEFT JOIN lp_join_r r ON l.k=r.k AND r.v IN (11,31) ORDER BY lid,rid", fullFat,
                        """
                                lid	rid
                                1	10
                                2	null
                                3	13
                                4	null
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l LEFT JOIN lp_join_r r ON l.k=r.k WHERE r.id=null ORDER BY lid,rid", fullFat,
                        """
                                lid	rid
                                4	null
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l LEFT JOIN lp_join_r r ON l.k=r.k AND false ORDER BY lid,rid", fullFat,
                        """
                                lid	rid
                                1	null
                                2	null
                                3	null
                                4	null
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l LEFT JOIN lp_join_r r ON l.v<r.v ORDER BY lid,rid", fullFat,
                        """
                                lid	rid
                                1	10
                                1	13
                                2	13
                                3	13
                                4	null
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l LEFT JOIN lp_join_r r ON false ORDER BY lid,rid", fullFat,
                        """
                                lid	rid
                                1	null
                                2	null
                                3	null
                                4	null
                                """);
            }
        });
    }

    @Test
    public void testMixedSourceConjunctsCompareTimestampLiteralsExactly() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_join_ts_l(id INT,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_join_ts_r(id INT)");
            execute("INSERT INTO lp_join_ts_l VALUES (1,'2020-01-01T00:00:00.000000Z'),(2,'2020-01-01T12:00:00.000000Z')");
            execute("INSERT INTO lp_join_ts_r VALUES (1),(2)");
            final String predicate = "l.ts<'2020-01-01T00:00:00.000000001Z' AND r.id>0";
            final String[] queries = {
                    "SELECT l.id lid,r.id rid FROM lp_join_ts_l l JOIN lp_join_ts_r r ON l.id=r.id WHERE " + predicate,
                    "SELECT l.id lid,r.id rid FROM lp_join_ts_l l LEFT JOIN lp_join_ts_r r ON l.id=r.id WHERE " + predicate,
                    "SELECT l.id lid,r.id rid FROM lp_join_ts_l l JOIN lp_join_ts_r r ON l.id=r.id AND " + predicate
            };
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (String sql : queries) {
                    try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                        assertRowsOnly(factory, "lid\trid\n1\t1\n");
                    }
                }
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT l.id lid,r.id rid FROM lp_join_ts_l l LEFT JOIN lp_join_ts_r r ON l.id=r.id AND "
                                + predicate + " ORDER BY lid", sqlExecutionContext).getRecordCursorFactory()) {
                    assertRowsOnly(factory, "lid\trid\n1\t1\n2\tnull\n");
                }
            }
        });
    }

    @Test
    public void testProjectedDerivedSourcesPruningAndQuotedScopes() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRows("SELECT l.id lid,r.id rid FROM (SELECT id,k,v+1 value FROM lp_join_l WHERE id>0) l JOIN (SELECT id,k,v value FROM lp_join_r) r ON l.k=r.k WHERE l.value<r.value ORDER BY lid,rid", false, "lid\trid\n");
            assertRows("SELECT rid FROM (SELECT l.id lid,r.id rid FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k) q WHERE lid>1 ORDER BY rid", false,
                    """
                            rid
                            12
                            13
                            """);
            assertRows("SELECT l.* FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k ORDER BY id,v", false,
                    """
                            id	k	b	v	s	ts
                            1	1	1	10	a	1970-01-01T00:00:00.001000Z
                            1	1	1	10	a	1970-01-01T00:00:00.001000Z
                            2	2	1	20	b	1970-01-01T00:00:00.002000Z
                            3	null	2	30	\t
                            """);
            assertRows("SELECT l.*,r.* FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k ORDER BY 1,7", false,
                    """
                            id	k	b	v	s	ts	id1	k1	b1	v1	s1	ts1
                            1	1	1	10	a	1970-01-01T00:00:00.001000Z	10	1	1	11	a	1970-01-01T00:00:00.001000000Z
                            1	1	1	10	a	1970-01-01T00:00:00.001000Z	11	1	2	5	a	1970-01-01T00:00:00.001000001Z
                            2	2	1	20	b	1970-01-01T00:00:00.002000Z	12	2	1	5	b	1970-01-01T00:00:00.002000000Z
                            3	null	2	30			13	null	2	31	\t
                            """);
            assertRows("SELECT l.*,sum(r.v) total FROM (SELECT id,k,v FROM lp_join_l) l JOIN lp_join_r r ON l.k=r.k ORDER BY l.id", false,
                    """
                            id	k	v	total
                            1	1	10	16
                            2	2	20	5
                            3	null	30	31
                            """);
            assertRows("SELECT DISTINCT l.* FROM (SELECT id,k,v FROM lp_join_l) l JOIN lp_join_r r ON l.k=r.k ORDER BY id", false,
                    """
                            id	k	v
                            1	1	10
                            2	2	20
                            3	null	30
                            """);
            assertRows("SELECT leftalias.id lid,r.id rid FROM lp_join_l \"LeftAlias\" JOIN lp_join_r r ON leftalias.k=r.k ORDER BY lid,rid", false,
                    """
                            lid	rid
                            1	10
                            1	11
                            2	12
                            3	13
                            """);
        });
    }

    @Test
    public void testRuntimeTermsDoNotTypeAnIsolatedCompileTimeNull() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setBoolean(0, true);
            final String prefix = "SELECT l.id FROM lp_join_l l JOIN lp_join_r r ON l.k=r.k WHERE ";
            assertQuery(prefix + "null AND $1").noLeakCheck().fails(63, "boolean expression expected");
            assertQuery(prefix + "null AND abs(1)>0").noLeakCheck().fails(63, "boolean expression expected");
            assertRows(prefix + "null AND true AND $1 ORDER BY l.id", false, "id\n");
        });
    }

    @Test
    public void testSymbolStringAndMixedTimestampPrecisionKeys() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            for (boolean fullFat : new boolean[]{false, true}) {
                assertRows("SELECT l.id lid,r.id rid,l.s ls,r.s rs FROM lp_join_l l JOIN lp_join_r r ON l.s=r.s ORDER BY lid,rid", fullFat,
                        """
                                lid	rid	ls	rs
                                1	10	a	a
                                1	11	a	a
                                2	12	b	b
                                3	13	\t
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_l l LEFT JOIN lp_join_r r ON l.s=r.s AND l.v<r.v ORDER BY lid,rid", fullFat,
                        """
                                lid	rid
                                1	10
                                2	null
                                3	13
                                4	null
                                """);
                assertRows("SELECT l.id lid,r.id rid,l.ts lts,r.ts rts FROM lp_join_l l JOIN lp_join_r r ON l.ts=r.ts ORDER BY lid,rid", fullFat,
                        """
                                lid	rid	lts	rts
                                1	10	1970-01-01T00:00:00.001000Z	1970-01-01T00:00:00.001000000Z
                                2	12	1970-01-01T00:00:00.002000Z	1970-01-01T00:00:00.002000000Z
                                3	13	\t
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_join_r r JOIN lp_join_l l ON r.ts=l.ts ORDER BY lid,rid", fullFat,
                        """
                                lid	rid
                                1	10
                                2	12
                                3	13
                                """);
            }
        });
    }

    private void assertFailure(SqlCompilerImpl compiler, String sql, String message) throws Exception {
        try (RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            Assert.fail(sql);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), message);
        }
    }

    private void assertRows(String sql, boolean isFullFat, String expected) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            compiler.setFullFatJoins(isFullFat);
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                assertRowsOnly(factory, expected);
            }
        }
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_join_l(id INT,k INT,b INT,v INT,s SYMBOL,ts TIMESTAMP)");
        execute("CREATE TABLE lp_join_r(id INT,k INT,b INT,v INT,s STRING,ts TIMESTAMP_NS)");
        execute("""
                INSERT INTO lp_join_l VALUES
                (1,1,1,10,'a','1970-01-01T00:00:00.001000Z'),
                (2,2,1,20,'b','1970-01-01T00:00:00.002000Z'),
                (3,null,2,30,null,null),
                (4,4,2,40,'d','1970-01-01T00:00:00.004000Z')
                """);
        execute("""
                INSERT INTO lp_join_r VALUES
                (10,1,1,11,'a','1970-01-01T00:00:00.001000000Z'),
                (11,1,2,5,'a','1970-01-01T00:00:00.001000001Z'),
                (12,2,1,5,'b','1970-01-01T00:00:00.002000000Z'),
                (13,null,2,31,null,null)
                """);
    }
}
