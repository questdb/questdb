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

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LagLongGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testLongRingBuffer() {
        final long[] sequence = {1, 2, 6, 9, 7, 11, 15, 2, 0};
        long defaultValue = 0;
        for (int offset = 1; offset < 12; offset++) {
            LagLongGroupByFunction f = new LagLongGroupByFunction(NullConstant.NULL, offset, 0);
            f.cache.reset(new SimpleMapValue(1));
            for (int i = 0; i < sequence.length; i++) {
                f.cache.putLong(sequence[i]);
            }
            int resultOffset = sequence.length - offset - 1;
            if (resultOffset < 0) {
                Assert.assertEquals(defaultValue, f.cache.getLong());
            } else {
                Assert.assertEquals(sequence[resultOffset], f.cache.getLong());
            }
        }
    }

    @Test
    public void testBase() throws Exception {
        assertQuery(
                "ts\tname\tvalue\n" +
                        "2021-07-05T22:37:00.000000Z\taaa\t3\n" +
                        "2021-07-05T22:50:00.000000Z\tbbb\tNaN\n" +
                        "2021-07-05T23:03:00.000000Z\t\t12\n" +
                        "2021-07-05T23:16:00.000000Z\tbbb\t8\n" +
                        "2021-07-05T23:29:00.000000Z\tccc\t18\n" +
                        "2021-07-05T23:42:00.000000Z\tbbb\t11\n" +
                        "2021-07-05T23:55:00.000000Z\tccc\t5\n" +
                        "2021-07-06T00:08:00.000000Z\tccc\tNaN\n" +
                        "2021-07-06T00:21:00.000000Z\t\t14\n" +
                        "2021-07-06T00:34:00.000000Z\taaa\t4\n" +
                        "2021-07-06T00:47:00.000000Z\taaa\t4\n" +
                        "2021-07-06T01:00:00.000000Z\tbbb\t8\n" +
                        "2021-07-06T01:13:00.000000Z\tddd\t17\n" +
                        "2021-07-06T01:26:00.000000Z\tddd\t4\n" +
                        "2021-07-06T01:39:00.000000Z\taaa\t17\n" +
                        "2021-07-06T01:52:00.000000Z\taaa\tNaN\n" +
                        "2021-07-06T02:05:00.000000Z\t\tNaN\n" +
                        "2021-07-06T02:18:00.000000Z\tbbb\t20\n" +
                        "2021-07-06T02:31:00.000000Z\tccc\t15\n" +
                        "2021-07-06T02:44:00.000000Z\tccc\t2\n",

                "select * from tab",
                "create table tab as (" +
                        "select " +
                        "  timestamp_sequence(\n" +
                        "            to_timestamp('2021-07-05T22:37:00', 'yyyy-MM-ddTHH:mm:ss'),\n" +
                        "            780000000) as ts, " +
                        "  rnd_str('aaa', 'bbb', 'ccc', 'ddd', null) as name, " +
                        "  rnd_long(0, 20, 1) as value " +
                        "from long_sequence(20)" +
                        ") timestamp(ts) partition by DAY",
                "ts",
                true,
                true,
                true
        );
    }


    @Test
    public void testLagSampleBy1h() throws Exception {
        // TODO: something is broken, the lag function is being called 7 times
        // groups:
        // "ts\tname\tvalue\n"
        // "2021-07-05T22:37:00.000000Z\taaa\t3\n"
        // "2021-07-05T22:50:00.000000Z\tbbb\tNaN\n"
        // "2021-07-05T23:03:00.000000Z\t\t12\n"
        // "2021-07-05T23:16:00.000000Z\tbbb\t8\n"
        // "2021-07-05T23:29:00.000000Z\tccc\t18\n"
        //
        // "2021-07-05T23:42:00.000000Z\tbbb\t11\n"
        // "2021-07-05T23:55:00.000000Z\tccc\t5\n"
        // "2021-07-06T00:08:00.000000Z\tccc\tNaN\n"
        // "2021-07-06T00:21:00.000000Z\t\t14\n"
        // "2021-07-06T00:34:00.000000Z\taaa\t4\n"
        //
        // "2021-07-06T00:47:00.000000Z\taaa\t4\n"
        // "2021-07-06T01:00:00.000000Z\tbbb\t8\n"
        // "2021-07-06T01:13:00.000000Z\tddd\t17\n"
        // "2021-07-06T01:26:00.000000Z\tddd\t4\n"
        //
        // "2021-07-06T01:39:00.000000Z\taaa\t17\n"
        // "2021-07-06T01:52:00.000000Z\taaa\tNaN\n"
        // "2021-07-06T02:05:00.000000Z\t\tNaN\n"
        // "2021-07-06T02:18:00.000000Z\tbbb\t20\n"
        // "2021-07-06T02:31:00.000000Z\tccc\t15\n"
        //
        // "2021-07-06T02:44:00.000000Z\tccc\t2\n"
        assertQuery(
                "lag\n" +
                        "8\n" +
                        "14\n" +
                        "17\n" +
                        "20\n" +
                        "NaN\n",
                "select lag(value) from tab sample by 1h",
                "create table tab as (" +
                        "select " +
                        "  timestamp_sequence(\n" +
                        "            to_timestamp('2021-07-05T22:37:00', 'yyyy-MM-ddTHH:mm:ss'),\n" +
                        "            780000000) as ts, " +
                        "  rnd_str('aaa', 'bbb', 'ccc', 'ddd', null) as name, " +
                        "  rnd_long(0, 20, 1) as value " +
                        "from long_sequence(20)" +
                        ") timestamp(ts) partition by DAY",
                null,
                false,
                true,
                true,
                true
        );
    }

    @Test
    public void testLagSampleBy1d() throws Exception {
        // TODO: something is broken, the lag function is being called 7 times
        assertQuery(
                "lag\n" +
                        "15\n",
                "select lag(value) from tab sample by 1d",
                "create table tab as (" +
                        "select " +
                        "  timestamp_sequence(\n" +
                        "            to_timestamp('2021-07-05T22:37:00', 'yyyy-MM-ddTHH:mm:ss'),\n" +
                        "            780000000) as ts, " +
                        "  rnd_str('aaa', 'bbb', 'ccc', 'ddd', null) as name, " +
                        "  rnd_long(0, 20, 1) as value " +
                        "from long_sequence(20)" +
                        ") timestamp(ts) partition by DAY",
                null,
                false,
                true,
                true,
                true
        );
    }

    @Test
    public void testLagSampleBy20m() throws Exception {
        // TODO: something is broken, the lag function is being called 7 times
        // "ts\tname\tvalue\n"
        // "2021-07-05T22:37:00.000000Z\taaa\t3\n"
        // "2021-07-05T22:50:00.000000Z\tbbb\tNaN\n"
        //
        // "2021-07-05T23:03:00.000000Z\t\t12\n"
        // "2021-07-05T23:16:00.000000Z\tbbb\t8\n"
        //
        // "2021-07-05T23:29:00.000000Z\tccc\t18\n"
        //
        // "2021-07-05T23:42:00.000000Z\tbbb\t11\n"
        // "2021-07-05T23:55:00.000000Z\tccc\t5\n"
        //
        // "2021-07-06T00:08:00.000000Z\tccc\tNaN\n"
        //
        // "2021-07-06T00:21:00.000000Z\t\t14\n"
        // "2021-07-06T00:34:00.000000Z\taaa\t4\n"
        //
        // "2021-07-06T00:47:00.000000Z\taaa\t4\n"
        //
        // "2021-07-06T01:00:00.000000Z\tbbb\t8\n"
        // "2021-07-06T01:13:00.000000Z\tddd\t17\n"
        //
        // "2021-07-06T01:26:00.000000Z\tddd\t4\n"
        //
        // "2021-07-06T01:39:00.000000Z\taaa\t17\n"
        // "2021-07-06T01:52:00.000000Z\taaa\tNaN\n"
        //
        // "2021-07-06T02:05:00.000000Z\t\tNaN\n"
        //
        // "2021-07-06T02:18:00.000000Z\tbbb\t20\n"
        // "2021-07-06T02:31:00.000000Z\tccc\t15\n"
        //
        // "2021-07-06T02:44:00.000000Z\tccc\t2\n"
        assertQuery(
                "lag\n" +
                        "3\n" +
                        "12\n" +
                        "NaN\n" +
                        "11\n" +
                        "NaN\n" +
                        "14\n" +
                        "NaN\n" +
                        "8\n" +
                        "NaN\n" +
                        "17\n" +
                        "NaN\n" +
                        "20\n" +
                        "NaN\n",
                "select lag(value) from tab sample by 20m",
                "create table tab as (" +
                        "select " +
                        "  timestamp_sequence(\n" +
                        "            to_timestamp('2021-07-05T22:37:00', 'yyyy-MM-ddTHH:mm:ss'),\n" +
                        "            780000000) as ts, " +
                        "  rnd_str('aaa', 'bbb', 'ccc', 'ddd', null) as name, " +
                        "  rnd_long(0, 20, 1) as value " +
                        "from long_sequence(20)" +
                        ") timestamp(ts) partition by DAY",
                null,
                false,
                true,
                true,
                true
        );
    }
}
