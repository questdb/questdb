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

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * These tests cover distinct variations.
 */
public class DistinctTest extends AbstractCairoTest {

    @Test
    public void testAliases_columnAliasExprDisabled() throws Exception {
        testColumnPrefixes(false);
    }

    @Test
    public void testColumnPrefixes() throws Exception {
        testColumnPrefixes(true);
    }

    @Test
    public void testDistinctConstAliasOrderByLimitFromFuzzer() throws Exception {
        // Original failing query from the query fuzzer (seed s0=104514844543552, s1=1779785264959).
        assertQuery("SELECT DISTINCT -676 AS e0, t0.x AS e1, -97 AS e2," +
                " '2024-01-23T16:57:00.000000Z'::TIMESTAMP AS e3, t0.x AS e4" +
                " FROM long_sequence(73) t0" +
                " WHERE (t0.x > (t0.x - t0.x) AND t0.x IS NOT NULL)" +
                " ORDER BY e0 DESC LIMIT 2")
                .expectSize()
                .returns("e0\te1\te2\te3\te4\n" +
                        "-676\t36\t-97\t2024-01-23T16:57:00.000000Z\t36\n" +
                        "-676\t26\t-97\t2024-01-23T16:57:00.000000Z\t26\n");
    }

    @Test
    public void testDistinctConstAliasOrderByLimitWithDuplicateCol() throws Exception {
        // Duplicate column refs trigger rewriteTrivialGroupByExpressions which used to push
        // LIMIT past the virtual carrying the constant alias; ORDER BY e0 then resolved at the
        // limited group-by and crashed with AIOOBE.
        assertQuery("SELECT DISTINCT -1 AS e0, t0.x AS e1, t0.x AS e4 FROM long_sequence(3) t0 ORDER BY e0 DESC LIMIT 2")
                .expectSize()
                .returns("e0\te1\te4\n" +
                        "-1\t3\t3\n" +
                        "-1\t2\t2\n");
    }

    @Test
    public void testDistinctImplementsLimitLoPositive() throws Exception {
        execute(
                "create table x as (" +
                        "  select" +
                        "    rnd_symbol('foo') sym," +
                        "    rnd_int() origin," +
                        "    rnd_int() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(100)" +
                        ") timestamp(created);"
        );
        assertQuery("SELECT DISTINCT sym, origin, lag(origin) over() from x limit 5")
                .withPlan("""
                        Limit value: 5 skip-rows: 0 take-rows: 5
                            Distinct
                              keys: sym,origin,lag
                              earlyExit: 5
                                Window
                                  functions: [lag(origin, 1, NULL) over ()]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: x
                        """)
                .noRandomAccess()
                .expectSize()
                .returns("""
                        sym\torigin\tlag
                        foo\t315515118\tnull
                        foo\t73575701\t315515118
                        foo\t592859671\t73575701
                        foo\t-1191262516\t592859671
                        foo\t-1575378703\t-1191262516
                        """);

        assertQuery("SELECT DISTINCT sym, origin, lag(origin) over() from x limit 20, 28")
                .withPlan("""
                        Limit left: 20 right: 28 skip-rows: 20 take-rows: 8
                            Distinct
                              keys: sym,origin,lag
                              earlyExit: 28
                                Window
                                  functions: [lag(origin, 1, NULL) over ()]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: x
                        """)
                .noRandomAccess()
                .expectSize()
                .returns("""
                        sym\torigin\tlag
                        foo\t2137969456\t264240638
                        foo\t68265578\t2137969456
                        foo\t44173540\t68265578
                        foo\t-2144581835\t44173540
                        foo\t-1162267908\t-2144581835
                        foo\t-1575135393\t-1162267908
                        foo\t326010667\t-1575135393
                        foo\t-2034804966\t326010667
                        """);

        // no early exit
        assertQuery("SELECT DISTINCT sym, origin, lag(origin) over() from x limit -5")
                .withPlan("""
                        Limit value: -5 skip-rows: 95 take-rows: 5
                            Distinct
                              keys: sym,origin,lag
                                Window
                                  functions: [lag(origin, 1, NULL) over ()]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: x
                        """)
                .noRandomAccess()
                .expectSize()
                .returns("""
                        sym\torigin\tlag
                        foo\t874367915\t-1775036711
                        foo\t1431775887\t874367915
                        foo\t-1822590290\t1431775887
                        foo\t957075831\t-1822590290
                        foo\t-2043541236\t957075831
                        """);

        // no early exit
        assertQuery("SELECT DISTINCT sym, origin, lag(origin) over() from x order by 1 limit 5")
                .withPlan("""
                        Limit value: 5 skip-rows: 0 take-rows: 5
                            Encode sort
                              keys: [sym]
                                Distinct
                                  keys: sym,origin,lag
                                    Window
                                      functions: [lag(origin, 1, NULL) over ()]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                        """)
                .expectSize()
                .returns("""
                        sym\torigin\tlag
                        foo\t315515118\tnull
                        foo\t73575701\t315515118
                        foo\t592859671\t73575701
                        foo\t-1191262516\t592859671
                        foo\t-1575378703\t-1191262516
                        """);
    }

    @Test
    public void testDistinctOrderByPositionLimitPush() throws Exception {
        // The trivial-group-by rewrite pushes the outer LIMIT onto the nested group by, but the
        // gate runs before ORDER BY tokens are normalized, so it resolves positions/qualifiers
        // itself. A position over a key (2 == e1) must push like the alias form; one over the
        // virtual-only constant (1 == e0) must not, else rewriteOrderBy later crashes (AIOOBE).
        assertMemoryLeak(() -> {
            final String base = "SELECT DISTINCT -1 AS e0, t0.x AS e1, t0.x AS e4 FROM long_sequence(3) t0 ";

            // position 2 == e1 (a key): pushed, same plan as the alias form. Asserting both against
            // one literal proves the positional form normalizes to the same column.
            final String pushedPlan = """
                    VirtualRecord
                      functions: [-1,e1,e4]
                        Long Top K lo: 2
                          keys: [e1 desc]
                            GroupBy vectorized: false
                              keys: [e1,e4]
                                long_sequence count: 3
                    """;
            assertPlanNoLeakCheck(base + "ORDER BY e1 DESC LIMIT 2", pushedPlan);
            assertPlanNoLeakCheck(base + "ORDER BY 2 DESC LIMIT 2", pushedPlan);

            // position 1 == e0 (virtual-only constant): not pushed, same plan as the alias form, no AIOOBE.
            final String notPushedPlan = """
                    Sort light lo: 2
                      keys: [e0 desc]
                        VirtualRecord
                          functions: [-1,e1,e4]
                            GroupBy vectorized: false
                              keys: [e1,e4]
                                long_sequence count: 3
                    """;
            assertPlanNoLeakCheck(base + "ORDER BY e0 DESC LIMIT 2", notPushedPlan);
            assertPlanNoLeakCheck(base + "ORDER BY 1 DESC LIMIT 2", notPushedPlan);

            // e1 is unique, so the key-ordered result is deterministic.
            assertSql("e0\te1\te4\n-1\t3\t3\n-1\t2\t2\n", base + "ORDER BY 2 DESC LIMIT 2");
            // constant order is unspecified (all e0 equal); assert only the row count and no crash.
            assertSql("c\n2\n", "SELECT count() c FROM (" + base + "ORDER BY 1 DESC LIMIT 2)");

            // Qualified ORDER BY t0.x strips to x (single join model). The constant e0 forces the
            // outer virtual, and the unaliased middle column makes x a group-by key, so the
            // qualified form pushes like the bare column.
            final String base2 = "SELECT DISTINCT -1 AS e0, t0.x, t0.x AS e4 FROM long_sequence(3) t0 ";
            final String pushedPlan2 = """
                    VirtualRecord
                      functions: [-1,x,e4]
                        Long Top K lo: 2
                          keys: [x desc]
                            GroupBy vectorized: false
                              keys: [x,e4]
                                long_sequence count: 3
                    """;
            assertPlanNoLeakCheck(base2 + "ORDER BY x DESC LIMIT 2", pushedPlan2);
            assertPlanNoLeakCheck(base2 + "ORDER BY t0.x DESC LIMIT 2", pushedPlan2);
            assertSql("e0\tx\te4\n-1\t3\t3\n-1\t2\t2\n", base2 + "ORDER BY t0.x DESC LIMIT 2");
        });
    }

    @Test
    public void testDistinctQualifiedColumnInExprWithBindVariableFromFuzzer() throws Exception {
        // Bind-form of the prior fuzzer query. The bind cast steals the early aliases
        // ("cast", "cast1") so the bare t0.x projection keeps its user alias "x". The
        // earlier qualified emit for (t0.x)::CHAR had already registered "x" in the
        // translating model under the stripped key, and createSelectColumn needed the
        // same qualified-vs-stripped retry as doReplaceLiteral0 to reuse that entry
        // instead of renaming the bare projection to "x1" and breaking the DISTINCT
        // wrapper's lookup.
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            bindVariableService.setStr("b0", "Y");
            assertSql(
                    "cast\tcast1\tx\n" +
                            "Y\t\t1\n" +
                            "Y\t\t2\n" +
                            "Y\t\t3\n",
                    "SELECT DISTINCT :b0::CHAR, (t0.x)::CHAR, t0.x" +
                            " FROM long_sequence(3) t0"
            );
        });
    }

    @Test
    public void testDistinctQualifiedColumnInExpressionFromFuzzer() throws Exception {
        // Qualified t0.x inside the expression must resolve to the same column as the bare
        // t0.x projection above; otherwise the expression's literal misses the translating
        // entry, overwrites the columnNameToAlias mapping with a duplicate, and the parent
        // virtual model raises Invalid column.
        assertQuery("SELECT DISTINCT -614 AS e0, t0.x AS e1, 786 AS e2, -716 AS e3," +
                " (278444 * t0.x) AS e4" +
                " FROM long_sequence(2) t0" +
                " ORDER BY e1")
                .expectSize()
                .returns("e0\te1\te2\te3\te4\n" +
                        "-614\t1\t786\t-716\t278444\n" +
                        "-614\t2\t786\t-716\t556888\n");
    }

    @Test
    public void testDistinctReusedColumnWithUserAliasRepro() throws Exception {
        // Duplicate aliased refs to the same column (x AS e1, x AS e2) under DISTINCT
        // pointed the outer projection at the translating-model alias "x" while the
        // group-by metadata only exposed "e1"; generateSelectChoose hit "wtf? x".
        assertQuery("SELECT DISTINCT abs(x) AS e0, x AS e1, x AS e2 FROM long_sequence(3)")
                .expectSize()
                .returns("e0\te1\te2\n" +
                        "1\t1\t1\n" +
                        "2\t2\t2\n" +
                        "3\t3\t3\n");
    }

    @Test
    public void testDistinctWithAlias() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
        assertQuery("SELECT distinct sa.created FROM x sa;")
                .ddl("create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(10)" +
                        ") timestamp(created);")
                .expectSize()
                .returns("""
                        created
                        1970-01-01T00:00:00.000000Z
                        """);
    }

    @Test
    public void testDuplicateColumn() throws Exception {
        assertQuery("SELECT DISTINCT event e1, event e2 FROM x order by 1 desc;")
                .ddl("create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(3)" +
                        ") timestamp(created);")
                .expectSize()
                .returns("""
                        e1\te2
                        24814\t24814
                        -13027\t-13027
                        -22955\t-22955
                        """);
    }

    @Test
    public void testDuplicateColumnInWhereClauseSubQuery() throws Exception {
        assertQuery("SELECT * FROM x WHERE event IN (SELECT * FROM (SELECT DISTINCT event, event FROM x));")
                .ddl("create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_symbol('a','b','c') event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(3)" +
                        ") timestamp(created);")
                .timestamp("created")
                .returns("""
                        origin\tevent\tcreated
                        -27056\ta\t1970-01-01T00:00:00.000000Z
                        -11455\tc\t1970-01-01T00:00:00.000000Z
                        -21227\tc\t1970-01-01T00:00:00.000000Z
                        """);
    }

    @Test
    public void testDuplicateColumnWithSubQuery() throws Exception {
        assertQuery("SELECT DISTINCT event e1, event e2 FROM (SELECT origin, (-event) event FROM x) order by 1;")
                .ddl("create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(3)" +
                        ") timestamp(created);")
                .expectSize()
                .returns("""
                        e1\te2
                        -24814\t-24814
                        13027\t13027
                        22955\t22955
                        """);
    }

    @Test
    public void testDuplicateColumnWithUnion() throws Exception {
        assertQuery("(SELECT 42 e1, 42 e2) UNION (SELECT DISTINCT event e1, event e2 FROM x order by 1);")
                .ddl("create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(3)" +
                        ") timestamp(created);")
                .noRandomAccess()
                .returns("""
                        e1\te2
                        42\t42
                        -22955\t-22955
                        -13027\t-13027
                        24814\t24814
                        """);
    }

    @Test
    public void testDuplicateCount() throws Exception {
        assertQuery("SELECT DISTINCT count(*), count(*) FROM x;")
                .ddl("create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(10)" +
                        ") timestamp(created);")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        count\tcount1
                        10\t10
                        """);
    }

    @Test
    public void testDuplicateCount2() throws Exception {
        assertQuery("SELECT DISTINCT sym, count(*), count(*) FROM x;")
                .ddl("create table x as (" +
                        "  select" +
                        "    rnd_symbol('foo') sym," +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(10)" +
                        ") timestamp(created);")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        sym\tcount\tcount1
                        foo\t10\t10
                        """);
    }

    @Test
    public void testDuplicateCountNested() throws Exception {
        assertQuery("SELECT * FROM (SELECT DISTINCT count(*), count(*) FROM x);")
                .ddl("create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(10)" +
                        ") timestamp(created);")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        count\tcount1
                        10\t10
                        """);
    }

    @Test
    public void testInnerJoinAliases1() throws Exception {
        testInnerJoinAliases1(true);
    }

    @Test
    public void testInnerJoinAliases1_columnAliasExprDisabled() throws Exception {
        testInnerJoinAliases1(false);
    }

    @Test
    public void testInnerJoinAliases2() throws Exception {
        testInnerJoinAliases2(true);
    }

    @Test
    public void testInnerJoinAliases2_columnAliasExprDisabled() throws Exception {
        testInnerJoinAliases2(false);
    }

    private void testColumnPrefixes(boolean columnAliasExprEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, String.valueOf(columnAliasExprEnabled));
        assertMemoryLeak(() -> {
            execute(
                    "create table sensors (" +
                            "  sensor_id SYMBOL," +
                            "  apptype INT," +
                            "  ts TIMESTAMP" +
                            ") timestamp (ts) partition by day;"
            );

            execute("insert into sensors values ('air', 1, '1970-02-02T10:10:00');");
            execute("insert into sensors values ('air', 1, '1970-02-02T10:10:01');");

            assertQuery("select distinct sensors.sensor_id, sensors.apptype " +
                    "from sensors")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            sensor_id\tapptype
                            air\t1
                            """);
        });
    }

    private void testInnerJoinAliases1(boolean columnAliasExprEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, String.valueOf(columnAliasExprEnabled));
        assertMemoryLeak(() -> {
            execute(
                    "create table sensors (" +
                            "  sensor_id SYMBOL," +
                            "  apptype INT," +
                            "  ts TIMESTAMP" +
                            ") timestamp (ts) partition by day;"
            );
            execute(
                    "create table samples (" +
                            "  sensor_id SYMBOL," +
                            "  ts TIMESTAMP" +
                            ") timestamp(ts) partition by day;"
            );

            execute("insert into sensors values ('air', 1, '1970-02-02T10:10:00');");
            execute("insert into sensors values ('air', 1, '1970-02-02T10:10:01');");
            execute("insert into samples values ('air', '1970-02-02T10:10:00');");

            assertQuery("select distinct samples.sensor_id, sensors.apptype " +
                    "from samples " +
                    "inner join sensors on sensors.sensor_id = samples.sensor_id")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            sensor_id\tapptype
                            air\t1
                            """);
        });
    }

    private void testInnerJoinAliases2(boolean columnAliasExprEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, String.valueOf(columnAliasExprEnabled));
        assertMemoryLeak(() -> {
            execute(
                    "create table sensors (" +
                            "  sensor_id SYMBOL," +
                            "  apptype INT," +
                            "  ts TIMESTAMP" +
                            ") timestamp (ts) partition by day;"
            );
            execute(
                    "create table samples (" +
                            "  sensor_id SYMBOL," +
                            "  apptype INT," +
                            "  ts TIMESTAMP" +
                            ") timestamp(ts) partition by day;"
            );

            execute("insert into sensors values ('air', 1, '1970-02-02T10:10:00');");
            execute("insert into sensors values ('air', 2, '1970-02-02T10:10:01');");
            execute("insert into samples values ('air', 1, '1970-02-02T10:10:00');");
            execute("insert into samples values ('air', 1, '1970-02-02T10:10:01');");

            final String secondAlias = columnAliasExprEnabled ? "apptype_2" : "apptype1";
            assertQuery("select distinct samples.sensor_id, sensors.apptype, samples.apptype " +
                    "from samples " +
                    "inner join sensors on sensors.sensor_id = samples.sensor_id")
                    .noLeakCheck()
                    .expectSize()
                    .returns("sensor_id\tapptype\t" + secondAlias + "\n" +
                            "air\t2\t1\n" +
                            "air\t1\t1\n");
        });
    }
}
