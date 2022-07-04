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

package io.questdb.griffin;

import org.junit.Test;

public class Long128Tests extends AbstractGriffinTest {
    @Test
    public void testReadLong128Column() throws SqlException {
        assertQuery(
                "ts\tts1\ti\n" +
                        "1-1645660800000000\t2022-02-24T00:00:00.000000Z\t1\n" +
                        "2-1645660801000000\t2022-02-24T00:00:01.000000Z\t2\n" +
                        "3-1645660802000000\t2022-02-24T00:00:02.000000Z\t3\n" +
                        "4-1645660803000000\t2022-02-24T00:00:03.000000Z\t4\n" +
                        "5-1645660804000000\t2022-02-24T00:00:04.000000Z\t5\n" +
                        "6-1645660805000000\t2022-02-24T00:00:05.000000Z\t6\n" +
                        "7-1645660806000000\t2022-02-24T00:00:06.000000Z\t7\n" +
                        "8-1645660807000000\t2022-02-24T00:00:07.000000Z\t8\n" +
                        "9-1645660808000000\t2022-02-24T00:00:08.000000Z\t9\n" +
                        "10-1645660809000000\t2022-02-24T00:00:09.000000Z\t10\n",
                "select" +
                        " to_long128(x, timestamp_sequence('2022-02-24', 1000000L)) ts," +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(10)",
                null,
                false,
                true
        );
    }

    @Test
    public void testJoinWithLong128ColumnOnPrimaryAndSecondary() throws Exception {
        compile(
                "create table tab1 as " +
                "(select" +
                " to_long128(x, x) ts, " +
                " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                " cast(x as int) i" +
                " from long_sequence(10)" +
                ")"
        );
        engine.releaseInactive();

        assertQuery("ts\tts1\tts11\ti\n" +
                        "1-2\t1-1\t2022-02-24T00:00:00.000000Z\t1\n" +
                        "2-4\t2-2\t2022-02-24T00:00:01.000000Z\t2\n" +
                        "3-6\t3-3\t2022-02-24T00:00:02.000000Z\t3\n" +
                        "4-8\t4-4\t2022-02-24T00:00:03.000000Z\t4\n" +
                        "5-10\t5-5\t2022-02-24T00:00:04.000000Z\t5\n" +
                        "6-12\t6-6\t2022-02-24T00:00:05.000000Z\t6\n" +
                        "7-14\t7-7\t2022-02-24T00:00:06.000000Z\t7\n" +
                        "8-16\t8-8\t2022-02-24T00:00:07.000000Z\t8\n" +
                        "9-18\t9-9\t2022-02-24T00:00:08.000000Z\t9\n" +
                        "10-20\t10-10\t2022-02-24T00:00:09.000000Z\t10\n",
                "select tab2.ts, tab1.* from tab1 JOIN tab2 ON tab1.ts1 = tab2.ts1",
                "create table tab2 as " +
                        "(select" +
                        " to_long128(x, 2 * x) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(10)" +
                        ")",
                null,
                false
        );
    }

    @Test
    public void testJoinOnLong128Column() throws Exception {
        compile(
                "create table tab1 as " +
                        "(select" +
                        " to_long128(3 * x, 3 * x) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(20)" +
                        ")"
        );
        engine.releaseInactive();

        assertQuery("ts\tts1\tts11\ti\n" +
                        "6-6\t6-6\t2022-02-24T00:00:01.000000Z\t2\n" +
                        "12-12\t12-12\t2022-02-24T00:00:03.000000Z\t4\n" +
                        "18-18\t18-18\t2022-02-24T00:00:05.000000Z\t6\n" +
                        "24-24\t24-24\t2022-02-24T00:00:07.000000Z\t8\n" +
                        "30-30\t30-30\t2022-02-24T00:00:09.000000Z\t10\n" +
                        "36-36\t36-36\t2022-02-24T00:00:11.000000Z\t12\n",
                "select tab2.ts, tab1.* from tab1 JOIN tab2 ON tab1.ts = tab2.ts",
                "create table tab2 as " +
                        "(select" +
                        " to_long128(2 * x, 2 * x) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(20)" +
                        ")",
                null,
                false
        );
    }

    @Test
    public void testGroupByLong128Column() throws Exception {
        assertQuery("ts\tcount\n" +
                        "0-0\t1\n" +
                        "1-1\t2\n" +
                        "2-2\t2\n" +
                        "3-3\t2\n" +
                        "4-4\t2\n" +
                        "5-5\t2\n" +
                        "6-6\t2\n" +
                        "7-7\t2\n" +
                        "8-8\t2\n" +
                        "9-9\t2\n" +
                        "10-10\t1\n",
                "select ts, count() from tab1",
                "create table tab1 as " +
                        "(select" +
                        " to_long128(x / 2, x / 2) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testOrderByLong128Column() throws Exception {
        assertQuery("ts\tcount\n" +
                        "0-0\t1\n" +
                        "1-1\t2\n" +
                        "2-2\t2\n" +
                        "3-3\t2\n" +
                        "4-4\t2\n" +
                        "5-5\t2\n" +
                        "6-6\t2\n" +
                        "7-7\t2\n" +
                        "8-8\t2\n" +
                        "9-9\t2\n" +
                        "10-10\t1\n",
                "select * from tab1 order by ts desc",
                "create table tab1 as " +
                        "(select" +
                        " to_long128(x, -x) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true,
                true
        );
    }
}
