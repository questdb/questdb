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

import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CountStringGroupByFunctionFactoryTest extends AbstractCairoTest {

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
                "select a, count_distinct('42') from x order by a",
                "create table x as (select * from (select rnd_symbol('a','b','c') a from long_sequence(20)))",
                null,
                true,
                true
        );
        assertSql(expected, "select a, count(distinct '42') from x order by a");
    }

    @Test
    public void testExpression() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    a\tcount_distinct
                    a\t3
                    b\t3
                    c\t3
                    """;
            assertQueryNoLeakCheck(
                    expected,
                    "select a, count_distinct(concat(s, s)) from x order by a",
                    "create table x as (select * from (select rnd_symbol('a','b','c') a, rnd_str('aaa','bbb','ccc') s from long_sequence(20)))",
                    null,
                    true,
                    true
            );
            assertSql(expected, "select a, count(distinct concat(s, s)) from x order by a");
            // self-concatenation shouldn't affect the number of distinct values,
            // so the result should stay the same
            assertSql(expected, "select a, count_distinct(s) from x order by a");
            assertSql(expected, "select a, count(distinct s) from x order by a");
        });
    }

    @Test
    public void testGroupKeyed() throws Exception {
        String expected = """
                a\tcount_distinct
                a\t4
                b\t4
                c\t3
                d\t1
                e\t2
                f\t3
                """;
        assertQuery(
                expected,
                "select a, count_distinct(s) from x order by a",
                "create table x as (select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_str('344', 'xx2', '00s', '544', 'rraa', '0llp') s,  timestamp_sequence(0, 100000) ts from long_sequence(20)) timestamp(ts))",
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
                6
                """;
        assertQuery(
                expected,
                "select count_distinct(s) from x",
                "create table x as (select * from (select rnd_symbol('344', 'xx2', '00s', '544', 'rraa', '0llp') s,  timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                null,
                false,
                true
        );
        assertSql(expected, "select count(distinct s) from x");
    }

    @Test
    public void testGroupNotKeyedWithNulls() throws Exception {
        String expected = """
                count_distinct
                4
                """;
        assertQuery(
                expected,
                "select count_distinct(s) from x",
                "create table x as (select * from (select rnd_symbol(null, 'xx2', '00s', '544', 'rraa', null) s,  timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                null,
                false,
                true
        );
        assertSql(expected, "select count(distinct s) from x");
    }

    @Test
    public void testNullConstant() throws Exception {
        String expected = """
                a\tcount_distinct
                a\t0
                b\t0
                c\t0
                """;
        assertQuery(
                expected,
                "select a, count_distinct(cast(null as STRING)) from x order by a",
                "create table x as (select * from (select rnd_symbol('a','b','c') a from long_sequence(20)))",
                null,
                true,
                true
        );
        assertSql(expected, "select a, count(distinct cast(null as STRING)) from x order by a");
    }

    @Test
    public void testSampleFillLinear() throws Exception {
        String expected = """
                ts\tcount_distinct
                1970-01-01T00:00:00.000000Z\t5
                1970-01-01T00:00:01.000000Z\t5
                1970-01-01T00:00:02.000000Z\t6
                1970-01-01T00:00:03.000000Z\t5
                1970-01-01T00:00:04.000000Z\t6
                1970-01-01T00:00:05.000000Z\t4
                1970-01-01T00:00:06.000000Z\t4
                1970-01-01T00:00:07.000000Z\t5
                1970-01-01T00:00:08.000000Z\t6
                1970-01-01T00:00:09.000000Z\t5
                """;
        assertQuery(
                expected,
                "select ts, count_distinct(s) from x sample by 1s fill(linear)",
                "create table x as (select * from (select rnd_str('344', 'xx2', '00s', '544', 'rraa', '0llp') s,  timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                true,
                true
        );
        assertSql(expected, "select ts, count(distinct s) from x sample by 1s fill(linear)");
    }

    @Test
    public void testSampleFillNone() throws Exception {
        assertMemoryLeak(() -> {
            String expected = """
                    ts\tcount_distinct
                    1970-01-01T00:06:40.000000Z\t4
                    1970-01-01T00:06:42.000000Z\t4
                    1970-01-01T00:06:44.000000Z\t4
                    1970-01-01T00:06:46.000000Z\t6
                    1970-01-01T00:06:48.000000Z\t5
                    1970-01-01T00:06:50.000000Z\t4
                    1970-01-01T00:06:52.000000Z\t4
                    1970-01-01T00:06:54.000000Z\t4
                    1970-01-01T00:06:56.000000Z\t4
                    1970-01-01T00:06:58.000000Z\t4
                    1970-01-01T00:07:00.000000Z\t5
                    1970-01-01T00:07:02.000000Z\t3
                    1970-01-01T00:07:04.000000Z\t6
                    1970-01-01T00:07:06.000000Z\t5
                    1970-01-01T00:07:08.000000Z\t4
                    """;

            final String sqlA = "with x as (select * from (select rnd_str('344', 'xx2', '00s', '544', 'rraa', '0llp') s,  timestamp_sequence(400000000, 300000) ts from long_sequence(100)) timestamp(ts))\n" +
                    "select ts, count_distinct(s) from x sample by 2s";
            Rnd rnd = sqlExecutionContext.getRandom();
            long s0 = rnd.getSeed0();
            long s1 = rnd.getSeed1();
            assertSql(expected, sqlA);

            rnd.reset(s0, s1);
            final String sqlB = "with x as (select * from (select rnd_str('344', 'xx2', '00s', '544', 'rraa', '0llp') s,  timestamp_sequence(400000000, 300000) ts from long_sequence(100)) timestamp(ts))\n" +
                    "select ts, count(distinct s) from x sample by 2s";
            assertSql(expected, sqlB);
        });
    }

    @Test
    public void testSampleFillValue() throws Exception {
        String expected = """
                ts\tcount_distinct
                1970-01-01T00:00:00.000000Z\t5
                1970-01-01T00:00:01.000000Z\t5
                1970-01-01T00:00:02.000000Z\t6
                1970-01-01T00:00:03.000000Z\t5
                1970-01-01T00:00:04.000000Z\t6
                1970-01-01T00:00:05.000000Z\t4
                1970-01-01T00:00:06.000000Z\t4
                1970-01-01T00:00:07.000000Z\t5
                1970-01-01T00:00:08.000000Z\t6
                1970-01-01T00:00:09.000000Z\t5
                """;
        assertQuery(
                expected,
                "select ts, count_distinct(s) from x sample by 1s fill(99)",
                "create table x as (select * from (select rnd_str('344', 'xx2', '00s', '544', 'rraa', '0llp') s,  timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                true
        );
        assertSql(expected, "select ts, count(distinct s) from x sample by 1s fill(99)");
    }

    @Test
    public void testSampleKeyed() throws Exception {
        String expected = """
                a\tcount_distinct\tts
                a\t3\t1970-01-01T00:00:00.000000Z
                b\t2\t1970-01-01T00:00:00.000000Z
                f\t1\t1970-01-01T00:00:00.000000Z
                c\t1\t1970-01-01T00:00:00.000000Z
                e\t2\t1970-01-01T00:00:00.000000Z
                d\t1\t1970-01-01T00:00:00.000000Z
                b\t3\t1970-01-01T00:00:01.000000Z
                a\t2\t1970-01-01T00:00:01.000000Z
                d\t1\t1970-01-01T00:00:01.000000Z
                c\t2\t1970-01-01T00:00:01.000000Z
                f\t2\t1970-01-01T00:00:01.000000Z
                c\t2\t1970-01-01T00:00:02.000000Z
                b\t3\t1970-01-01T00:00:02.000000Z
                f\t3\t1970-01-01T00:00:02.000000Z
                e\t2\t1970-01-01T00:00:02.000000Z
                f\t3\t1970-01-01T00:00:03.000000Z
                a\t1\t1970-01-01T00:00:03.000000Z
                c\t2\t1970-01-01T00:00:03.000000Z
                e\t1\t1970-01-01T00:00:03.000000Z
                d\t1\t1970-01-01T00:00:03.000000Z
                b\t1\t1970-01-01T00:00:03.000000Z
                b\t3\t1970-01-01T00:00:04.000000Z
                a\t1\t1970-01-01T00:00:04.000000Z
                c\t3\t1970-01-01T00:00:04.000000Z
                f\t3\t1970-01-01T00:00:04.000000Z
                d\t3\t1970-01-01T00:00:05.000000Z
                b\t1\t1970-01-01T00:00:05.000000Z
                a\t2\t1970-01-01T00:00:05.000000Z
                c\t1\t1970-01-01T00:00:05.000000Z
                f\t2\t1970-01-01T00:00:05.000000Z
                c\t2\t1970-01-01T00:00:06.000000Z
                f\t4\t1970-01-01T00:00:06.000000Z
                b\t2\t1970-01-01T00:00:06.000000Z
                d\t1\t1970-01-01T00:00:06.000000Z
                a\t1\t1970-01-01T00:00:06.000000Z
                e\t1\t1970-01-01T00:00:07.000000Z
                c\t3\t1970-01-01T00:00:07.000000Z
                f\t1\t1970-01-01T00:00:07.000000Z
                d\t2\t1970-01-01T00:00:07.000000Z
                b\t2\t1970-01-01T00:00:07.000000Z
                d\t2\t1970-01-01T00:00:08.000000Z
                e\t2\t1970-01-01T00:00:08.000000Z
                a\t2\t1970-01-01T00:00:08.000000Z
                b\t1\t1970-01-01T00:00:08.000000Z
                c\t1\t1970-01-01T00:00:08.000000Z
                f\t1\t1970-01-01T00:00:08.000000Z
                c\t2\t1970-01-01T00:00:09.000000Z
                b\t2\t1970-01-01T00:00:09.000000Z
                d\t2\t1970-01-01T00:00:09.000000Z
                e\t1\t1970-01-01T00:00:09.000000Z
                a\t1\t1970-01-01T00:00:09.000000Z
                f\t1\t1970-01-01T00:00:09.000000Z
                """;
        assertQuery(
                expected,
                "select a, count_distinct(s), ts from x sample by 1s ALIGN TO FIRST OBSERVATION",
                "create table x as (select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_str('344', 'xx2', '00s', '544', 'rraa', '0llp') s,  timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                false
        );
        assertSql(expected, "select a, count(distinct s), ts from x sample by 1s ALIGN TO FIRST OBSERVATION");
    }
}