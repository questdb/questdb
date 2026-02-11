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

public class CountDistinctLongGroupByFunctionFactoryTest extends AbstractCairoTest {

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
                "select a, count_distinct(42L) from x order by a",
                "create table x as (select * from (select rnd_symbol('a','b','c') a from long_sequence(20)))",
                null,
                true,
                true
        );
        assertSql(expected, "select a, count(distinct 42L) from x order by a");
    }

    @Test
    public void testConstantDefaultHashSetNoEntryValue() throws Exception {
        String expected = """
                count_distinct
                1
                """;
        assertQuery(
                expected,
                "select count_distinct(l) from x",
                "create table x as (select -1::long as l from long_sequence(10))",
                null,
                false,
                true
        );
        assertSql(expected, "select count(distinct l) from x");
    }

    @Test
    public void testExpression() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    a\tcount_distinct
                    a\t4
                    b\t4
                    c\t4
                    """;
            assertQueryNoLeakCheck(
                    expected,
                    "select a, count_distinct(s * 42) from x order by a",
                    "create table x as (select * from (select rnd_symbol('a','b','c') a, rnd_long(1, 8, 0) s from long_sequence(20)))",
                    null,
                    true,
                    true
            );
            assertSql(expected, "select a, count(distinct s * 42) from x order by a");

            // multiplication shouldn't affect the number of distinct values,
            // so the result should stay the same
            assertSql(expected, "select a, count_distinct(s) from x order by a");
            assertSql(expected, "select a, count(distinct s) from x order by a");
        });
    }

    @Test
    public void testGroupKeyed() throws Exception {
        String expected = """
                a\tcount_distinct
                a\t2
                b\t1
                c\t1
                d\t4
                e\t4
                f\t3
                """;
        assertQuery(
                expected,
                "select a, count_distinct(s) from x order by a",
                "create table x as (select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_long(0, 16, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(20)) timestamp(ts))",
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
                "create table x as (select * from (select rnd_long(1, 6, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                null,
                false,
                true
        );
        assertSql(expected, "select count(distinct s) from x");
    }

    @Test
    public void testGroupNotKeyedWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            String expected = """
                    count_distinct
                    6
                    """;
            assertQueryNoLeakCheck(
                    expected,
                    "select count_distinct(s) from x",
                    "create table x as (select * from (select rnd_long(1, 6, 0) s, timestamp_sequence(10, 100000) ts from long_sequence(100)) timestamp(ts)) timestamp(ts) PARTITION BY YEAR",
                    null,
                    false,
                    true
            );
            assertSql(expected, "select count(distinct s) from x");

            execute("insert into x values(cast(null as LONG), '2021-05-21')");
            execute("insert into x values(cast(null as LONG), '1970-01-01')");
            assertSql(expected, "select count_distinct(s) from x");
            assertSql(expected, "select count(distinct s) from x");
        });
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
                "select a, count_distinct(cast(null as LONG)) from x order by a",
                "create table x as (select * from (select rnd_symbol('a','b','c') a from long_sequence(20)))",
                null,
                true,
                true
        );
        assertSql(expected, "select a, count(distinct cast(null as LONG)) from x order by a");
    }

    @Test
    public void testSampleFillLinear() throws Exception {
        String expected = """
                ts\tcount_distinct
                1970-01-01T00:00:00.000000Z\t9
                1970-01-01T00:00:01.000000Z\t7
                1970-01-01T00:00:02.000000Z\t7
                1970-01-01T00:00:03.000000Z\t8
                1970-01-01T00:00:04.000000Z\t8
                1970-01-01T00:00:05.000000Z\t8
                1970-01-01T00:00:06.000000Z\t7
                1970-01-01T00:00:07.000000Z\t8
                1970-01-01T00:00:08.000000Z\t7
                1970-01-01T00:00:09.000000Z\t9
                """;
        assertQuery(
                expected,
                "select ts, count_distinct(s) from x sample by 1s fill(linear)",
                "create table x as (select * from (select rnd_long(0, 16, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
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
                    1970-01-01T00:00:00.050000Z\t8
                    1970-01-01T00:00:02.050000Z\t8
                    """;
            assertSql(
                    expected,
                    "with x as (select * from (select rnd_long(1, 8, 0) s, timestamp_sequence(50000, 100000L/4) ts from long_sequence(100)) timestamp(ts))\n" +
                            "select ts, count_distinct(s) from x sample by 2s align to first observation"
            );
            assertSql(expected, "with x as (select * from (select rnd_long(1, 8, 0) s, timestamp_sequence(50000, 100000L/4) ts from long_sequence(100)) timestamp(ts))\n" +
                    "select ts, count(distinct s) from x sample by 2s align to first observation");
        });
    }

    @Test
    public void testSampleFillValue() throws Exception {
        String expected = """
                ts\tcount_distinct
                1970-01-01T00:00:00.000000Z\t5
                1970-01-01T00:00:01.000000Z\t8
                1970-01-01T00:00:02.000000Z\t6
                1970-01-01T00:00:03.000000Z\t7
                1970-01-01T00:00:04.000000Z\t6
                1970-01-01T00:00:05.000000Z\t5
                1970-01-01T00:00:06.000000Z\t6
                1970-01-01T00:00:07.000000Z\t6
                1970-01-01T00:00:08.000000Z\t6
                1970-01-01T00:00:09.000000Z\t7
                """;
        assertQuery(
                expected,
                "select ts, count_distinct(s) from x sample by 1s fill(99)",
                "create table x as (select * from (select rnd_long(0, 8, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                true
        );
        assertSql(expected, "select ts, count(distinct s) from x sample by 1s fill(99)");
    }

    @Test
    public void testSampleKeyed() throws Exception {
        String expected = """
                a\tcount_distinct\tts
                a\t4\t1970-01-01T00:00:00.000000Z
                f\t9\t1970-01-01T00:00:00.000000Z
                c\t8\t1970-01-01T00:00:00.000000Z
                e\t4\t1970-01-01T00:00:00.000000Z
                d\t6\t1970-01-01T00:00:00.000000Z
                b\t6\t1970-01-01T00:00:00.000000Z
                b\t5\t1970-01-01T00:00:05.000000Z
                c\t4\t1970-01-01T00:00:05.000000Z
                f\t7\t1970-01-01T00:00:05.000000Z
                e\t6\t1970-01-01T00:00:05.000000Z
                d\t8\t1970-01-01T00:00:05.000000Z
                a\t5\t1970-01-01T00:00:05.000000Z
                """;
        assertQuery(
                expected,
                "select a, count_distinct(s), ts from x sample by 5s align to first observation",
                "create table x as (select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_long(0, 12, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                false
        );
        assertSql(expected, "select a, count(distinct s), ts from x sample by 5s align to first observation");
    }
}
