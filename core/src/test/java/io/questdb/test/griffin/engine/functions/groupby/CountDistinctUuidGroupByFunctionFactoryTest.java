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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CountDistinctUuidGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstant() throws Exception {
        String expected = """
                a\tcount_distinct
                a\t1
                b\t1
                c\t1
                """;
        assertQuery("select a, count_distinct(to_uuid(42L, 42L)) from x order by a")
                .ddl("create table x as (select * from (select rnd_symbol('a','b','c') a from long_sequence(20)))")
                .expectSize()
                .returns(expected);
        assertQuery("select a, count(distinct to_uuid(42L, 42L)) from x order by a")
                .expectSize()
                .returns(expected);
    }

    @Test
    public void testConstantDefaultHashSetNoEntryValue() throws Exception {
        String expected = """
                count_distinct
                1
                """;
        assertQuery("select count_distinct(to_uuid(l, l)) from x")
                .ddl("create table x as (select -1::long as l from long_sequence(10))")
                .noRandomAccess()
                .expectSize()
                .returns(expected);
        assertQuery("select count(distinct to_uuid(l, l)) from x")
                .noRandomAccess()
                .expectSize()
                .returns(expected);
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
            assertQuery("select a, count_distinct(to_uuid(s * 42, s * 42)) from x order by a")
                    .noLeakCheck()
                    .ddl("create table x as (select * from (select rnd_symbol('a','b','c') a, rnd_long(1, 8, 0) s from long_sequence(20)))")
                    .expectSize()
                    .returns(expected);
            assertQuery("select a, count(distinct to_uuid(s * 42, s * 42)) from x order by a")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            // multiplication shouldn't affect the number of distinct values,
            // so the result should stay the same
            assertQuery("select a, count_distinct(s) from x order by a")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("select a, count(distinct s) from x order by a")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
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
        assertQuery("select a, count_distinct(s) from x order by a")
                .ddl("create table x as (select * from (select rnd_symbol('a','b','c','d','e','f') a, to_uuid(rnd_long(0, 16, 0), 0) s, timestamp_sequence(0, 100000) ts from long_sequence(20)) timestamp(ts))")
                .expectSize()
                .returns(expected);
        assertQuery("select a, count(distinct s) from x order by a")
                .expectSize()
                .returns(expected);
    }

    @Test
    public void testGroupNotKeyed() throws Exception {
        String expected = """
                count_distinct
                6
                """;
        assertQuery("select count_distinct(s) from x")
                .ddl("create table x as (select * from (select to_uuid(rnd_long(1, 6, 0), 0) s, timestamp_sequence(0, 1000) ts from long_sequence(1000)) timestamp(ts))")
                .noRandomAccess()
                .expectSize()
                .returns(expected);
        assertQuery("select count(distinct s) from x")
                .noRandomAccess()
                .expectSize()
                .returns(expected);
    }

    @Test
    public void testGroupNotKeyedWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            String expected = """
                    count_distinct
                    6
                    """;
            assertQuery("select count_distinct(s) from x")
                    .noLeakCheck()
                    .ddl("create table x as (select * from (select to_uuid(rnd_long(1, 6, 0), 0) s, timestamp_sequence(10, 100000) ts from long_sequence(1000)) timestamp(ts)) timestamp(ts) PARTITION BY YEAR")
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            assertQuery("select count(distinct s) from x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);

            execute("insert into x values(cast(null as UUID), '2021-05-21')");
            execute("insert into x values(cast(null as UUID), '1970-01-01')");
            assertQuery("select count_distinct(s) from x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            assertQuery("select count(distinct s) from x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testMappingZeroToNulls() throws Exception {
        assertMemoryLeak(() -> {
            // this is to ensure that uuids wth nulls and zeros don't map to the same values
            assertQuery("select * from x")
                    .ddl("create table x ( a SYMBOL, s UUID, ts TIMESTAMP ) timestamp(ts)")
                    .timestamp("ts")
                    .returns("a\ts\tts\n");

            execute("insert into x values ('a', to_uuid(5, 0), '2021-05-21'), ('a', to_uuid(5, 0), '2021-05-21'), ('a', to_uuid(5, null), '2021-05-21'), ('a', to_uuid(10, 0), '2021-05-21'), ('a', to_uuid(10, null), '2021-05-21')" +
                    ", ('a', to_uuid(0, 5), '2021-05-21'), ('a', to_uuid(0, 5), '2021-05-21'), ('a', to_uuid(null, 5), '2021-05-21'), ('a', to_uuid(0, 10), '2021-05-21'), ('a', to_uuid(null, 10), '2021-05-21'), ('a', to_uuid(0, 0), '2021-05-21'), ('a', to_uuid(null, null), '2021-05-21')");
            String expected = """
                    a\ts
                    a\t9
                    """;
            assertQuery("select a, count_distinct(s) as s from x order by a")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("select a, count(distinct s) as s from x order by a")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
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
        assertQuery("select a, count_distinct(to_uuid(null, null)) from x order by a")
                .ddl("create table x as (select * from (select rnd_symbol('a','b','c') a from long_sequence(20)))")
                .expectSize()
                .returns(expected);
        assertQuery("select a, count(distinct to_uuid(null, null)) from x order by a")
                .expectSize()
                .returns(expected);
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
        assertQuery("select ts, count_distinct(s) from x sample by 1s fill(linear)")
                .ddl("create table x as (select * from (select to_uuid(rnd_long(0, 16, 0), 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))")
                .timestamp("ts")
                .expectSize()
                .returns(expected);
        assertQuery("select ts, count(distinct s) from x sample by 1s fill(linear)")
                .timestamp("ts")
                .expectSize()
                .returns(expected);
    }

    //
    @Test
    public void testSampleFillNone() throws Exception {
        String expected = """
                ts\tcount_distinct
                1970-01-01T00:00:00.050000Z\t8
                1970-01-01T00:00:02.050000Z\t8
                """;
        // returnsOnce(): the query evaluates rnd_*() inline, so its values differ across the
        // re-reads returns() performs; the single cursor pass keeps the result stable.
        assertQuery("with x as (select * from (select to_uuid(rnd_long(1, 8, 0), 0) s, timestamp_sequence(50000, 100000L/4) ts from long_sequence(100)) timestamp(ts))\n" +
                "select ts, count_distinct(s) from x sample by 2s align to first observation")
                .returnsOnce(expected);
        // returnsOnce(): the query evaluates rnd_*() inline, so its values differ across the
        // re-reads returns() performs; the single cursor pass keeps the result stable.
        assertQuery("with x as (select * from (select to_uuid(rnd_long(1, 8, 0), 0) s, timestamp_sequence(50000, 100000L/4) ts from long_sequence(100)) timestamp(ts))\n" +
                "select ts, count(distinct s) from x sample by 2s align to first observation")
                .noLeakCheck()
                .returnsOnce(expected);
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
        assertQuery("select ts, count_distinct(s) from x sample by 1s fill(99)")
                .ddl("create table x as (select * from (select to_uuid(rnd_long(0, 8, 0), 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))")
                .timestamp("ts")
                .noRandomAccess()
                .returns(expected);
        assertQuery("select ts, count(distinct s) from x sample by 1s fill(99)")
                .timestamp("ts")
                .noRandomAccess()
                .returns(expected);
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
        assertQuery("select a, count_distinct(s), ts from x sample by 5s align to first observation")
                .ddl("create table x as (select * from (select rnd_symbol('a','b','c','d','e','f') a, to_uuid(rnd_long(0, 12, 0), 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))")
                .timestamp("ts")
                .noRandomAccess()
                .returns(expected);
        assertQuery("select a, count(distinct s), ts from x sample by 5s align to first observation")
                .timestamp("ts")
                .noRandomAccess()
                .returns(expected);
    }
}