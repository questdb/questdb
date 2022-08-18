/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Test;

public class CountDistinctIntGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testGroupKeyed() throws Exception {
        assertQuery(
                "a\tcount_distinct\n" +
                        "a\t2\n" +
                        "f\t4\n" +
                        "c\t2\n" +
                        "e\t4\n" +
                        "d\t4\n" +
                        "b\t1\n",
                "select a, count_distinct(s) from x",
                "create table x as (select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_int(0, 16, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(20)) timestamp(ts))",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testGroupNotKeyed() throws Exception {
        assertQuery(
                "count_distinct\n" +
                        "6\n",
                "select count_distinct(s) from x",
                "create table x as (select * from (select rnd_int(1, 6, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testGroupNotKeyedWithNulls() throws Exception {
        String expected = "count_distinct\n" +
                "6\n";
        assertQuery(
                expected,
                "select count_distinct(s) from x",
                "create table x as (select * from (select rnd_int(1, 6, 0) s, timestamp_sequence(10, 100000) ts from long_sequence(100)) timestamp(ts)) timestamp(ts) PARTITION BY YEAR",
                null,
                false,
                true,
                true
        );

        executeInsert("insert into x values(cast(null as INT), '2021-05-21')");
        executeInsert("insert into x values(cast(null as INT), '1970-01-01')");
        assertSql("select count_distinct(s) from x", expected);
    }

    @Test
    public void testSampleFillLinear() throws Exception {
        assertQuery(
                "ts\tcount_distinct\n" +
                        "1970-01-01T00:00:00.000000Z\t8\n" +
                        "1970-01-01T00:00:01.000000Z\t9\n" +
                        "1970-01-01T00:00:02.000000Z\t9\n" +
                        "1970-01-01T00:00:03.000000Z\t9\n" +
                        "1970-01-01T00:00:04.000000Z\t8\n" +
                        "1970-01-01T00:00:05.000000Z\t9\n" +
                        "1970-01-01T00:00:06.000000Z\t10\n" +
                        "1970-01-01T00:00:07.000000Z\t7\n" +
                        "1970-01-01T00:00:08.000000Z\t8\n" +
                        "1970-01-01T00:00:09.000000Z\t7\n",
                "select ts, count_distinct(s) from x sample by 1s fill(linear)",
                "create table x as (select * from (select rnd_int(0, 16, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                true,
                true,
                true
        );
    }

    @Test
    public void testSampleFillNone() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "with x as (select * from (select rnd_int(1, 8, 0) s, timestamp_sequence(50000, 100000L/4) ts from long_sequence(150)) timestamp(ts))\n" +
                        "select ts, count_distinct(s) from x sample by 2s",
                "ts\tcount_distinct\n" +
                        "1970-01-01T00:00:00.050000Z\t8\n" +
                        "1970-01-01T00:00:02.050000Z\t8\n"
        ));
    }

    @Test
    public void testSampleFillValue() throws Exception {
        assertQuery(
                "ts\tcount_distinct\n" +
                        "1970-01-01T00:00:00.000000Z\t8\n" +
                        "1970-01-01T00:00:01.000000Z\t6\n" +
                        "1970-01-01T00:00:02.000000Z\t7\n" +
                        "1970-01-01T00:00:03.000000Z\t8\n" +
                        "1970-01-01T00:00:04.000000Z\t6\n" +
                        "1970-01-01T00:00:05.000000Z\t7\n" +
                        "1970-01-01T00:00:06.000000Z\t7\n" +
                        "1970-01-01T00:00:07.000000Z\t6\n" +
                        "1970-01-01T00:00:08.000000Z\t7\n" +
                        "1970-01-01T00:00:09.000000Z\t5\n",
                "select ts, count_distinct(s) from x sample by 1s fill(99)",
                "create table x as (select * from (select rnd_int(0, 8, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                false
        );
    }

    @Test
    public void testSampleKeyed() throws Exception {
        assertQuery(
                "a\tcount_distinct\tts\n" +
                        "a\t5\t1970-01-01T00:00:00.000000Z\n" +
                        "f\t8\t1970-01-01T00:00:00.000000Z\n" +
                        "c\t9\t1970-01-01T00:00:00.000000Z\n" +
                        "e\t6\t1970-01-01T00:00:00.000000Z\n" +
                        "d\t7\t1970-01-01T00:00:00.000000Z\n" +
                        "b\t5\t1970-01-01T00:00:00.000000Z\n" +
                        "b\t6\t1970-01-01T00:00:05.000000Z\n" +
                        "c\t5\t1970-01-01T00:00:05.000000Z\n" +
                        "f\t6\t1970-01-01T00:00:05.000000Z\n" +
                        "e\t8\t1970-01-01T00:00:05.000000Z\n" +
                        "d\t7\t1970-01-01T00:00:05.000000Z\n" +
                        "a\t4\t1970-01-01T00:00:05.000000Z\n",
                "select a, count_distinct(s), ts from x sample by 5s",
                "create table x as (select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_int(0, 12, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                false
        );
    }
}
