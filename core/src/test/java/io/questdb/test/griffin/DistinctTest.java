/*******************************************************************************
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
        assertQueryAndPlan(
                """
                        sym\torigin\tlag
                        foo\t315515118\tnull
                        foo\t73575701\t315515118
                        foo\t592859671\t73575701
                        foo\t-1191262516\t592859671
                        foo\t-1575378703\t-1191262516
                        """,
                """
                        Limit value: 5 skip-rows: 0 take-rows: 5
                            Distinct
                              keys: sym,origin,lag
                              earlyExit: 5
                                Window
                                  functions: [lag(origin, 1, NULL) over ()]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: x
                        """,
                "SELECT DISTINCT sym, origin, lag(origin) over() from x limit 5",
                null,
                false,
                true
        );

        assertQueryAndPlan(
                """
                        sym\torigin\tlag
                        foo\t2137969456\t264240638
                        foo\t68265578\t2137969456
                        foo\t44173540\t68265578
                        foo\t-2144581835\t44173540
                        foo\t-1162267908\t-2144581835
                        foo\t-1575135393\t-1162267908
                        foo\t326010667\t-1575135393
                        foo\t-2034804966\t326010667
                        """,
                """
                        Limit left: 20 right: 28 skip-rows: 20 take-rows: 8
                            Distinct
                              keys: sym,origin,lag
                              earlyExit: 28
                                Window
                                  functions: [lag(origin, 1, NULL) over ()]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: x
                        """,
                "SELECT DISTINCT sym, origin, lag(origin) over() from x limit 20, 28",
                null,
                false,
                true
        );

        // no early exit
        assertQueryAndPlan(
                """
                        sym\torigin\tlag
                        foo\t874367915\t-1775036711
                        foo\t1431775887\t874367915
                        foo\t-1822590290\t1431775887
                        foo\t957075831\t-1822590290
                        foo\t-2043541236\t957075831
                        """,
                """
                        Limit value: -5 skip-rows: 95 take-rows: 5
                            Distinct
                              keys: sym,origin,lag
                                Window
                                  functions: [lag(origin, 1, NULL) over ()]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: x
                        """,
                "SELECT DISTINCT sym, origin, lag(origin) over() from x limit -5",
                null,
                false,
                true
        );

        // no early exit
        assertQueryAndPlan(
                """
                        sym\torigin\tlag
                        foo\t315515118\tnull
                        foo\t73575701\t315515118
                        foo\t592859671\t73575701
                        foo\t-1191262516\t592859671
                        foo\t-1575378703\t-1191262516
                        """,
                """
                        Limit value: 5 skip-rows: 0 take-rows: 5
                            Sort
                              keys: [sym]
                                Distinct
                                  keys: sym,origin,lag
                                    Window
                                      functions: [lag(origin, 1, NULL) over ()]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                        """,
                "SELECT DISTINCT sym, origin, lag(origin) over() from x order by 1 limit 5",
                null,
                true,
                true
        );
    }

    @Test
    public void testDistinctWithAlias() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
        assertQuery(
                """
                        created
                        1970-01-01T00:00:00.000000Z
                        """,
                "SELECT distinct sa.created FROM x sa;",
                "create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(10)" +
                        ") timestamp(created);",
                null,
                true,
                true
        );
    }

    @Test
    public void testDuplicateColumn() throws Exception {
        assertQuery(
                """
                        e1\te2
                        24814\t24814
                        -13027\t-13027
                        -22955\t-22955
                        """,
                "SELECT DISTINCT event e1, event e2 FROM x order by 1 desc;",
                "create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(3)" +
                        ") timestamp(created);",
                null,
                true,
                true
        );
    }

    @Test
    public void testDuplicateColumnInWhereClauseSubQuery() throws Exception {
        assertQuery(
                """
                        origin\tevent\tcreated
                        -27056\ta\t1970-01-01T00:00:00.000000Z
                        -11455\tc\t1970-01-01T00:00:00.000000Z
                        -21227\tc\t1970-01-01T00:00:00.000000Z
                        """,
                "SELECT * FROM x WHERE event IN (SELECT * FROM (SELECT DISTINCT event, event FROM x));",
                "create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_symbol('a','b','c') event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(3)" +
                        ") timestamp(created);",
                "created",
                true,
                false
        );
    }

    @Test
    public void testDuplicateColumnWithSubQuery() throws Exception {
        assertQuery(
                """
                        e1\te2
                        -24814\t-24814
                        13027\t13027
                        22955\t22955
                        """,
                "SELECT DISTINCT event e1, event e2 FROM (SELECT origin, (-event) event FROM x) order by 1;",
                "create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(3)" +
                        ") timestamp(created);",
                null,
                true,
                true
        );
    }

    @Test
    public void testDuplicateColumnWithUnion() throws Exception {
        assertQuery(
                """
                        e1\te2
                        42\t42
                        -22955\t-22955
                        -13027\t-13027
                        24814\t24814
                        """,
                "(SELECT 42 e1, 42 e2) UNION (SELECT DISTINCT event e1, event e2 FROM x order by 1);",
                "create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(3)" +
                        ") timestamp(created);",
                null,
                false,
                false
        );
    }

    @Test
    public void testDuplicateCount() throws Exception {
        assertQuery(
                """
                        count\tcount1
                        10\t10
                        """,
                "SELECT DISTINCT count(*), count(*) FROM x;",
                "create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(10)" +
                        ") timestamp(created);",
                null,
                false,
                true
        );
    }

    @Test
    public void testDuplicateCount2() throws Exception {
        assertQuery(
                """
                        sym\tcount\tcount1
                        foo\t10\t10
                        """,
                "SELECT DISTINCT sym, count(*), count(*) FROM x;",
                "create table x as (" +
                        "  select" +
                        "    rnd_symbol('foo') sym," +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(10)" +
                        ") timestamp(created);",
                null,
                false,
                true
        );
    }

    @Test
    public void testDuplicateCountNested() throws Exception {
        assertQuery(
                """
                        count\tcount1
                        10\t10
                        """,
                "SELECT * FROM (SELECT DISTINCT count(*), count(*) FROM x);",
                "create table x as (" +
                        "  select" +
                        "    rnd_short() origin," +
                        "    rnd_short() event," +
                        "    timestamp_sequence(0, 0) created" +
                        "  from long_sequence(10)" +
                        ") timestamp(created);",
                null,
                false,
                true
        );
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

            assertQueryNoLeakCheck(
                    """
                            sensor_id\tapptype
                            air\t1
                            """,
                    "select distinct sensors.sensor_id, sensors.apptype " +
                            "from sensors"
            );
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

            assertQueryNoLeakCheck(
                    """
                            sensor_id\tapptype
                            air\t1
                            """,
                    "select distinct samples.sensor_id, sensors.apptype " +
                            "from samples " +
                            "inner join sensors on sensors.sensor_id = samples.sensor_id"
            );
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
            assertQueryNoLeakCheck(
                    "sensor_id\tapptype\t" + secondAlias + "\n" +
                            "air\t2\t1\n" +
                            "air\t1\t1\n",
                    "select distinct samples.sensor_id, sensors.apptype, samples.apptype " +
                            "from samples " +
                            "inner join sensors on sensors.sensor_id = samples.sensor_id"
            );
        });
    }
}
