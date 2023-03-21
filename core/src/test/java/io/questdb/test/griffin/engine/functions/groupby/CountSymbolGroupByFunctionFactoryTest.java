/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.test.AbstractGriffinTest;
import org.junit.Test;

public class CountSymbolGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testConstant() throws Exception {
        assertQuery(
                "a\tcount_distinct\n" +
                        "a\t1\n" +
                        "b\t1\n" +
                        "c\t1\n",
                "select a, count_distinct(cast('foobar' as SYMBOL)) from x",
                "create table x as (select * from (select rnd_symbol('a','b','c') a from long_sequence(20)))",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupKeyed() throws Exception {
        assertQuery(
                "a\tcount_distinct\n" +
                        "a\t4\n" +
                        "b\t4\n" +
                        "f\t3\n" +
                        "c\t3\n" +
                        "e\t2\n" +
                        "d\t1\n",
                "select a, count_distinct(s) from x",
                "create table x as (select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_symbol('344', 'xx2', '00s', '544', 'rraa', '0llp') s,  timestamp_sequence(0, 100000) ts from long_sequence(20)) timestamp(ts))",
                null,
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
                "create table x as (select * from (select rnd_symbol('344', 'xx2', '00s', '544', 'rraa', '0llp') s,  timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                null,
                false,
                true
        );
    }

    @Test
    public void testGroupNotKeyedWithNull() throws Exception {
        assertQuery(
                "count_distinct\n" +
                        "2\n",
                "select count_distinct(s) from x",
                "create table x as (select * from (select rnd_symbol(null, '344', 'xx2', null) s,  timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                null,
                false,
                true
        );
    }

    @Test
    public void testNullConstant() throws Exception {
        assertQuery(
                "a\tcount_distinct\n" +
                        "a\t0\n" +
                        "b\t0\n" +
                        "c\t0\n",
                "select a, count_distinct(cast(null as SYMBOL)) from x",
                "create table x as (select * from (select rnd_symbol('a','b','c') a from long_sequence(20)))",
                null,
                true,
                true
        );
    }

    @Test
    public void testSampleFillLinear() throws Exception {
        assertQuery(
                "ts\tcount_distinct\n" +
                        "1970-01-01T00:00:00.000000Z\t5\n" +
                        "1970-01-01T00:00:01.000000Z\t5\n" +
                        "1970-01-01T00:00:02.000000Z\t6\n" +
                        "1970-01-01T00:00:03.000000Z\t5\n" +
                        "1970-01-01T00:00:04.000000Z\t6\n" +
                        "1970-01-01T00:00:05.000000Z\t4\n" +
                        "1970-01-01T00:00:06.000000Z\t4\n" +
                        "1970-01-01T00:00:07.000000Z\t5\n" +
                        "1970-01-01T00:00:08.000000Z\t6\n" +
                        "1970-01-01T00:00:09.000000Z\t5\n",
                "select ts, count_distinct(s) from x sample by 1s fill(linear)",
                "create table x as (select * from (select rnd_symbol('344', 'xx2', '00s', '544', 'rraa', '0llp') s,  timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                true,
                true
        );
    }

    @Test
    public void testSampleFillNone() throws Exception {
        assertMemoryLeak(() -> {
            final String sql = "with x as (select * from (select rnd_symbol('344', 'xx2', '00s', '544', 'rraa', '0llp') s,  timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))\n" +
                    "select ts, count_distinct(s) from x sample by 1s";
            assertSql(
                    sql,
                    "ts\tcount_distinct\n" +
                            "1970-01-01T00:00:00.000000Z\t5\n" +
                            "1970-01-01T00:00:01.000000Z\t5\n" +
                            "1970-01-01T00:00:02.000000Z\t6\n" +
                            "1970-01-01T00:00:03.000000Z\t5\n" +
                            "1970-01-01T00:00:04.000000Z\t6\n" +
                            "1970-01-01T00:00:05.000000Z\t4\n" +
                            "1970-01-01T00:00:06.000000Z\t4\n" +
                            "1970-01-01T00:00:07.000000Z\t5\n" +
                            "1970-01-01T00:00:08.000000Z\t6\n" +
                            "1970-01-01T00:00:09.000000Z\t5\n"
            );
        });
    }

    @Test
    public void testSampleFillValue() throws Exception {
        assertQuery(
                "ts\tcount_distinct\n" +
                        "1970-01-01T00:00:00.000000Z\t5\n" +
                        "1970-01-01T00:00:01.000000Z\t5\n" +
                        "1970-01-01T00:00:02.000000Z\t6\n" +
                        "1970-01-01T00:00:03.000000Z\t5\n" +
                        "1970-01-01T00:00:04.000000Z\t6\n" +
                        "1970-01-01T00:00:05.000000Z\t4\n" +
                        "1970-01-01T00:00:06.000000Z\t4\n" +
                        "1970-01-01T00:00:07.000000Z\t5\n" +
                        "1970-01-01T00:00:08.000000Z\t6\n" +
                        "1970-01-01T00:00:09.000000Z\t5\n",
                "select ts, count_distinct(s) from x sample by 1s fill(99)",
                "create table x as (select * from (select rnd_symbol('344', 'xx2', '00s', '544', 'rraa', '0llp') s,  timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                false
        );
    }

    @Test
    public void testSampleKeyed() throws Exception {
        assertQuery(
                "a\tcount_distinct\tts\n" +
                        "a\t3\t1970-01-01T00:00:00.000000Z\n" +
                        "b\t2\t1970-01-01T00:00:00.000000Z\n" +
                        "f\t1\t1970-01-01T00:00:00.000000Z\n" +
                        "c\t1\t1970-01-01T00:00:00.000000Z\n" +
                        "e\t2\t1970-01-01T00:00:00.000000Z\n" +
                        "d\t1\t1970-01-01T00:00:00.000000Z\n" +
                        "b\t3\t1970-01-01T00:00:01.000000Z\n" +
                        "a\t2\t1970-01-01T00:00:01.000000Z\n" +
                        "d\t1\t1970-01-01T00:00:01.000000Z\n" +
                        "c\t2\t1970-01-01T00:00:01.000000Z\n" +
                        "f\t2\t1970-01-01T00:00:01.000000Z\n" +
                        "c\t2\t1970-01-01T00:00:02.000000Z\n" +
                        "b\t3\t1970-01-01T00:00:02.000000Z\n" +
                        "f\t3\t1970-01-01T00:00:02.000000Z\n" +
                        "e\t2\t1970-01-01T00:00:02.000000Z\n" +
                        "f\t3\t1970-01-01T00:00:03.000000Z\n" +
                        "a\t1\t1970-01-01T00:00:03.000000Z\n" +
                        "c\t2\t1970-01-01T00:00:03.000000Z\n" +
                        "e\t1\t1970-01-01T00:00:03.000000Z\n" +
                        "d\t1\t1970-01-01T00:00:03.000000Z\n" +
                        "b\t1\t1970-01-01T00:00:03.000000Z\n" +
                        "b\t3\t1970-01-01T00:00:04.000000Z\n" +
                        "a\t1\t1970-01-01T00:00:04.000000Z\n" +
                        "c\t3\t1970-01-01T00:00:04.000000Z\n" +
                        "f\t3\t1970-01-01T00:00:04.000000Z\n" +
                        "d\t3\t1970-01-01T00:00:05.000000Z\n" +
                        "b\t1\t1970-01-01T00:00:05.000000Z\n" +
                        "a\t2\t1970-01-01T00:00:05.000000Z\n" +
                        "c\t1\t1970-01-01T00:00:05.000000Z\n" +
                        "f\t2\t1970-01-01T00:00:05.000000Z\n" +
                        "c\t2\t1970-01-01T00:00:06.000000Z\n" +
                        "f\t4\t1970-01-01T00:00:06.000000Z\n" +
                        "b\t2\t1970-01-01T00:00:06.000000Z\n" +
                        "d\t1\t1970-01-01T00:00:06.000000Z\n" +
                        "a\t1\t1970-01-01T00:00:06.000000Z\n" +
                        "e\t1\t1970-01-01T00:00:07.000000Z\n" +
                        "c\t3\t1970-01-01T00:00:07.000000Z\n" +
                        "f\t1\t1970-01-01T00:00:07.000000Z\n" +
                        "d\t2\t1970-01-01T00:00:07.000000Z\n" +
                        "b\t2\t1970-01-01T00:00:07.000000Z\n" +
                        "d\t2\t1970-01-01T00:00:08.000000Z\n" +
                        "e\t2\t1970-01-01T00:00:08.000000Z\n" +
                        "a\t2\t1970-01-01T00:00:08.000000Z\n" +
                        "b\t1\t1970-01-01T00:00:08.000000Z\n" +
                        "c\t1\t1970-01-01T00:00:08.000000Z\n" +
                        "f\t1\t1970-01-01T00:00:08.000000Z\n" +
                        "c\t2\t1970-01-01T00:00:09.000000Z\n" +
                        "b\t2\t1970-01-01T00:00:09.000000Z\n" +
                        "d\t2\t1970-01-01T00:00:09.000000Z\n" +
                        "e\t1\t1970-01-01T00:00:09.000000Z\n" +
                        "a\t1\t1970-01-01T00:00:09.000000Z\n" +
                        "f\t1\t1970-01-01T00:00:09.000000Z\n",
                "select a, count_distinct(s), ts from x sample by 1s",
                "create table x as (select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_symbol('344', 'xx2', '00s', '544', 'rraa', '0llp') s,  timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                false
        );
    }
}