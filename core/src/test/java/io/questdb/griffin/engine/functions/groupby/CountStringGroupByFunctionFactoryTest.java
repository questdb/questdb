/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class CountStringGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testGroupKeyed() throws Exception {
        assertQuery(
                "a\tcount\n" +
                        "a\t4\n" +
                        "b\t4\n" +
                        "f\t3\n" +
                        "c\t3\n" +
                        "e\t2\n" +
                        "d\t1\n",
                "select a, count(s) from x",
                "create table x as (select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_str('344', 'xx2', '00s', '544', 'rraa', '0llp') s,  timestamp_sequence(0, 100000) ts from long_sequence(20)) timestamp(ts))",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testGroupNotKeyed() throws Exception {
        assertQuery(
                "count\n" +
                        "6\n",
                "select count(s) from x",
                "create table x as (select * from (select rnd_symbol('344', 'xx2', '00s', '544', 'rraa', '0llp') s,  timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testSampleFillLinear() throws Exception {
        assertQuery(
                "ts\tcount\n" +
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
                "select ts, count(s) from x sample by 1s fill(linear)",
                "create table x as (select * from (select rnd_str('344', 'xx2', '00s', '544', 'rraa', '0llp') s,  timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                true,
                true,
                true
        );
    }

    @Test
    public void testSampleFillNone() throws Exception {
        assertMemoryLeak(() -> {
            final String sql = "with x as (select * from (select rnd_str('344', 'xx2', '00s', '544', 'rraa', '0llp') s,  timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))\n" +
                    "select ts, count(s) from x sample by 1s";
            CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);

            try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                sink.clear();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    printer.print(cursor, factory.getMetadata(), true);
                }
            }

            TestUtils.assertEquals("ts\tcount\n" +
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
                    sink);
        });
    }

    @Test
    public void testSampleFillValue() throws Exception {
        assertQuery(
                "ts\tcount\n" +
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
                "select ts, count(s) from x sample by 1s fill(99)",
                "create table x as (select * from (select rnd_str('344', 'xx2', '00s', '544', 'rraa', '0llp') s,  timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                false
        );
    }

    @Test
    public void testSampleKeyed() throws Exception {
        assertQuery(
                "a\tcount\tts\n" +
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
                "select a, count(s), ts from x sample by 1s",
                "create table x as (select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_str('344', 'xx2', '00s', '544', 'rraa', '0llp') s,  timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                false
        );
    }
}