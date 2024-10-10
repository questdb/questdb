/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

public class CountLong256GroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstant() throws Exception {
        String expected = "a\tcount_distinct\n" +
                "a\t1\n" +
                "b\t1\n" +
                "c\t1\n";
        assertQuery(
                expected,
                "select a, count_distinct(cast('0x42' AS LONG256)) from x order by a",
                "create table x as (select * from (select rnd_symbol('a','b','c') a from long_sequence(20)))",
                null,
                true,
                true
        );
        assertSql(expected, "select a, count(distinct cast('0x42' AS LONG256)) from x order by a");
    }

    @Test
    public void testExpression() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "a\tcount_distinct\n" +
                    "a\t2\n" +
                    "b\t4\n" +
                    "c\t5\n";
            assertQueryNoLeakCheck(
                    expected,
                    "select a, count_distinct(s + s) from x order by a",
                    "create table x as (select * from (select rnd_symbol('a','b','c') a, rnd_long256(8) s from long_sequence(20)))",
                    null,
                    true,
                    true
            );
            assertSql(expected, "select a, count(distinct s + s) from x order by a");
            // self-addition shouldn't affect the number of distinct values,
            // so the result should stay the same
            assertSql(expected, "select a, count_distinct(s) from x order by a");
            assertSql(expected, "select a, count(distinct s) from x order by a");
        });
    }

    @Test
    public void testGroupKeyed() throws Exception {
        String expected = "a\tcount_distinct\n" +
                "a\t1\n" +
                "b\t4\n" +
                "c\t5\n" +
                "d\t3\n" +
                "e\t1\n" +
                "f\t4\n";
        assertQuery(
                expected,
                "select a, count_distinct(s) from x order by a",
                "create table x as (select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_long256(16) s,  timestamp_sequence(0, 100000) ts from long_sequence(20)) timestamp(ts))",
                null,
                true,
                true
        );
        assertSql(expected, "select a, count(distinct s) from x order by a");
    }

    @Test
    public void testGroupNotKeyed() throws Exception {
        String expected = "count_distinct\n" +
                "6\n";
        assertQuery(
                expected,
                "select count_distinct(s) from x",
                "create table x as (select * from (select rnd_long256(6) s,  timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                null,
                false,
                true
        );
        assertSql(expected, "select count(distinct s) from x");
    }

    @Test
    public void testGroupNotKeyedWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            String expected = "count_distinct\n" +
                    "6\n";
            assertQueryNoLeakCheck(
                    expected,
                    "select count_distinct(s) from x",
                    "create table x as (select * from (select rnd_long256(6) s,  timestamp_sequence(10, 100000) ts from long_sequence(100)) timestamp(ts)) timestamp(ts) PARTITION BY YEAR",
                    null,
                    false,
                    true
            );
            assertSql(expected, "select count(distinct s) from x");

            insert("insert into x values(cast(null as LONG256), '2021-05-21')");
            insert("insert into x values(cast(null as LONG256), '1970-01-01')");
            assertSql(expected, "select count_distinct(s) from x");
            assertSql(expected, "select count(distinct s) from x");
        });
    }

    @Test
    public void testNullConstant() throws Exception {
        String expected = "a\tcount_distinct\n" +
                "a\t0\n" +
                "b\t0\n" +
                "c\t0\n";
        assertQuery(
                expected,
                "select a, count_distinct(cast(null as LONG256)) from x order by a",
                "create table x as (select * from (select rnd_symbol('a','b','c') a from long_sequence(20)))",
                null,
                true,
                true
        );
        assertSql(expected, "select a, count(distinct cast(null as LONG256)) from x order by a");
    }

    @Test
    public void testSampleFillLinear() throws Exception {
        String expected = "ts\tcount_distinct\n" +
                "1970-01-01T00:00:00.000000Z\t6\n" +
                "1970-01-01T00:00:01.000000Z\t6\n" +
                "1970-01-01T00:00:02.000000Z\t6\n" +
                "1970-01-01T00:00:03.000000Z\t6\n" +
                "1970-01-01T00:00:04.000000Z\t6\n" +
                "1970-01-01T00:00:05.000000Z\t7\n" +
                "1970-01-01T00:00:06.000000Z\t6\n" +
                "1970-01-01T00:00:07.000000Z\t7\n" +
                "1970-01-01T00:00:08.000000Z\t5\n" +
                "1970-01-01T00:00:09.000000Z\t7\n";
        assertQuery(
                expected,
                "select ts, count_distinct(s) from x sample by 1s fill(linear)",
                "create table x as (select * from (select rnd_long256(10) s,  timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                true,
                true
        );
        assertSql(expected, "select ts, count(distinct s) from x sample by 1s fill(linear)");
    }

    @Test
    public void testSampleFillNone() throws Exception {
        assertMemoryLeak(() -> {
            String expected = "ts\tcount_distinct\n" +
                    "1970-01-01T00:00:00.050000Z\t8\n" +
                    "1970-01-01T00:00:02.050000Z\t8\n";
            Rnd rnd = sqlExecutionContext.getRandom();
            long so = rnd.getSeed0();
            long s1 = rnd.getSeed1();
            assertSql(expected,
                    "with x as (select * from (select rnd_long256(8) s, timestamp_sequence(50000, 100000L/4) ts from long_sequence(100)) timestamp(ts))\n" +
                            "select ts, count_distinct(s) from x sample by 2s align to first observation"
            );
            rnd.reset(so, s1);
            assertSql(expected, "with x as (select * from (select rnd_long256(8) s, timestamp_sequence(50000, 100000L/4) ts from long_sequence(100)) timestamp(ts))\n" +
                    "select ts, count(distinct s) from x sample by 2s align to first observation");
        });
    }

    @Test
    public void testSampleFillValue() throws Exception {
        String expected = "ts\tcount_distinct\n" +
                "1970-01-01T00:00:00.000000Z\t7\n" +
                "1970-01-01T00:00:01.000000Z\t7\n" +
                "1970-01-01T00:00:02.000000Z\t7\n" +
                "1970-01-01T00:00:03.000000Z\t5\n" +
                "1970-01-01T00:00:04.000000Z\t6\n" +
                "1970-01-01T00:00:05.000000Z\t6\n" +
                "1970-01-01T00:00:06.000000Z\t7\n" +
                "1970-01-01T00:00:07.000000Z\t5\n" +
                "1970-01-01T00:00:08.000000Z\t7\n" +
                "1970-01-01T00:00:09.000000Z\t5\n";
        assertQuery(
                expected,
                "select ts, count_distinct(s) from x sample by 1s fill(99)",
                "create table x as (select * from (select rnd_long256(8) s,  timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                true
        );
        assertSql(expected, "select ts, count(distinct s) from x sample by 1s fill(99)");
    }

    @Test
    public void testSampleKeyed() throws Exception {
        String expected = "a\tcount_distinct\tts\n" +
                "f\t8\t1970-01-01T00:00:00.000000Z\n" +
                "e\t4\t1970-01-01T00:00:00.000000Z\n" +
                "c\t8\t1970-01-01T00:00:00.000000Z\n" +
                "a\t4\t1970-01-01T00:00:00.000000Z\n" +
                "d\t5\t1970-01-01T00:00:00.000000Z\n" +
                "b\t6\t1970-01-01T00:00:00.000000Z\n" +
                "b\t8\t1970-01-01T00:00:05.000000Z\n" +
                "c\t4\t1970-01-01T00:00:05.000000Z\n" +
                "d\t8\t1970-01-01T00:00:05.000000Z\n" +
                "e\t6\t1970-01-01T00:00:05.000000Z\n" +
                "a\t4\t1970-01-01T00:00:05.000000Z\n" +
                "f\t6\t1970-01-01T00:00:05.000000Z\n";
        assertQuery(
                expected,
                "select a, count_distinct(s), ts from x sample by 5s align to first observation",
                "create table x as (select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_long256(12) s,  timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                false
        );
        assertSql(expected, "select a, count(distinct s), ts from x sample by 5s align to first observation");
    }
}