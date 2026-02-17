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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CountDistinctSymbolGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstant() throws Exception {
        String expected = """
                a\tcount_distinct
                a\t1
                b\t1
                c\t1
                """;
        assertQuery(
                expected,
                "select a, count_distinct('a'::symbol) from x order by a",
                "create table x as (select * from (select rnd_symbol('a','b','c') a from long_sequence(20)))",
                null,
                true,
                true
        );
        assertSql(expected, "select a, count(distinct 'a'::symbol) from x order by a");
    }

    @Test
    public void testExpression() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    a\tcount_distinct
                    1\t1
                    3\t1
                    4\t2
                    5\t2
                    6\t2
                    7\t3
                    8\t2
                    """;
            assertQueryNoLeakCheck(
                    expected,
                    "select a, count_distinct(concat(s, 'foobar')::symbol) from x order by a",
                    "create table x as (select * from (select rnd_int(1, 8, 0) a, rnd_symbol('a','b','c') s from long_sequence(20)))",
                    null,
                    true,
                    true
            );
            assertSql(expected, "select a, count(distinct concat(s, 'foobar')::symbol) from x order by a");
            // concatenation shouldn't affect the number of distinct values,
            // so the result should stay the same
            assertSql(expected, "select a, count_distinct(s) from x order by a");
            assertSql(expected, "select a, count(distinct s) from x order by a");
        });
    }

    @Test
    public void testGroupKeyed() throws Exception {
        String expected = """
                a\tcount_distinct
                0\t3
                1\t3
                3\t3
                4\t3
                5\t1
                6\t2
                8\t3
                """;
        assertQuery(
                expected,
                "select a, count_distinct(s) from x order by a",
                "create table x as (select * from (select rnd_int(0, 9, 0) a, rnd_symbol('a','b','c','d','e','f') s, timestamp_sequence(0, 100000) ts from long_sequence(20)) timestamp(ts))",
                null,
                true,
                true
        );
        assertSql(expected, "select a, count(distinct s) from x order by a");
    }

    @Test
    public void testGroupNotKeyed() throws Exception {
        String expected = """
                count_distinct
                922
                """;
        assertQuery(
                expected,
                "select count_distinct(s) from x",
                "create table x as (select * from (select rnd_symbol(1000, 1, 10, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(10000)) timestamp(ts))",
                null,
                false,
                true
        );
        assertSql(expected, "select count(distinct s) from x");
    }

    @Test
    public void testGroupNotKeyedMultipleFunctions() throws Exception {
        String expected = """
                count_distinct\tcount_distinct1
                96\t921
                """;
        assertQuery(
                expected,
                "select count_distinct(s1), count_distinct(s2) from x",
                "create table x as (select * from (select rnd_symbol(100, 1, 10, 0) s1, rnd_symbol(1000, 1, 10, 0) s2, timestamp_sequence(0, 100000) ts from long_sequence(10000)) timestamp(ts))",
                null,
                false,
                true
        );
        assertSql(expected, "select count(distinct s1), count(distinct s2) from x");
    }

    @Test
    public void testGroupNotKeyedWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            String expected = """
                    count_distinct
                    62
                    """;
            assertQueryNoLeakCheck(
                    expected,
                    "select count_distinct(s) from x",
                    "create table x as (select * from (select rnd_symbol(100, 10, 10, 0) s, timestamp_sequence(10, 100000) ts from long_sequence(100)) timestamp(ts)) timestamp(ts) PARTITION BY YEAR",
                    null,
                    false,
                    true
            );
            assertSql(expected, "select count(distinct s) from x");

            execute("insert into x values(cast(null as SYMBOL), '2021-05-21')");
            execute("insert into x values(cast(null as SYMBOL), '1970-01-01')");
            assertSql(expected, "select count_distinct(s) from x");
            assertSql(expected, "select count(distinct s) from x");
        });
    }

    @Test
    public void testNullConstant() throws Exception {
        String expected = """
                s\tcount_distinct
                a\t0
                b\t0
                c\t0
                """;
        assertQuery(
                expected,
                "select s, count_distinct(cast(null as SYMBOL)) from x order by s",
                "create table x as (select * from (select rnd_symbol('a','b','c') s from long_sequence(20)))",
                null,
                true,
                true
        );
        assertSql(expected, "select s, count(distinct cast(null as SYMBOL)) from x order by s");
    }

    @Test
    public void testSampleKeyed() throws Exception {
        final String expected = """
                count_distinct\tts
                6\t1970-01-01T00:00:00.000000Z
                6\t1970-01-01T00:00:05.000000Z
                """;
        final String queryA = "select count_distinct(s), ts from x sample by 5s";
        final String queryB = "select count(distinct s), ts from x sample by 5s";
        final String ddl = "create table x as (select * from (select rnd_symbol('a','b','c','d','e','f') s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))";
        assertMemoryLeak(() -> {
            assertQueryNoLeakCheck(expected, queryA, ddl, "ts", true, true);
            assertQueryNoLeakCheck(expected, queryA + " align to first observation", "ts", false);
            assertQueryNoLeakCheck(expected, queryB + " align to first observation", "ts", false);
            assertQueryNoLeakCheck(expected, queryA + " align to calendar", "ts", true, true);
            assertQueryNoLeakCheck(expected, queryB + " align to calendar", "ts", true, true);
        });
    }
}
