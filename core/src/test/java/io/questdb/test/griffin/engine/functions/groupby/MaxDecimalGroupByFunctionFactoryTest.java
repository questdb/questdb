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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class MaxDecimalGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testMax() throws Exception {
        assertQuery(
                "m8\tm16\tm32\tm64\tm128\tm256\n" +
                        "1\t2.0\t3.0\t9.00\t99.000\t100.000000\n",
                "select max(d8) m8, max(d16) m16, max(d32) m32, max(d64) m64, max(d128) m128, max(d256) m256 from x",
                "create table x as (" +
                        "select" +
                        " cast(x%2 as decimal(2,0)) d8, " +
                        " cast(x%3 as decimal(4,1)) d16, " +
                        " cast(x%4 as decimal(7,1)) d32, " +
                        " cast(x%10 as decimal(15,2)) d64, " +
                        " cast(x%100 as decimal(32,3)) d128, " +
                        " cast(x%1000 as decimal(76,6)) d256, " +
                        " timestamp_sequence(0, 1000) ts" +
                        " from long_sequence(100)" +
                        ") timestamp(ts) partition by month",
                null,
                false,
                true
        );
    }

    @Test
    public void testMaxAllNull() throws Exception {
        assertQuery(
                "max\n\n",
                "select max(x) from (select cast(null as decimal(10,2)) x from long_sequence(1000))",
                null,
                false,
                true
        );
    }

    @Test
    public void testMaxKeyed() throws Exception {
        assertQuery(
                "key\tm8\tm16\tm32\tm64\tm128\tm256\n" +
                        "4\t98\t999.6\t996519.1\t9996706980009.09\t18438116870105300071299057341.748\t62768662572617782789411637750252540600463143391413031101720390843.02695\n" +
                        "3\t98\t999.4\t999293.4\t9995304091750.47\t18445907049965428836184027650.544\t62695361596829930762746263372106158875223197903912027699958711281.60863\n" +
                        "2\t98\t998.5\t999538.2\t9990581500662.18\t18418677987608525892467469731.068\t62735651907801120481573277943721313295427224023316414377330145029.34803\n" +
                        "1\t98\t999.7\t999740.2\t9999997684267.21\t18432329569787485012589911950.901\t62754505182666656532937752284481109802819894709260977303861282809.57981\n" +
                        "0\t98\t999.2\t999956.5\t9987078655347.41\t18439524792393733125371110516.979\t62769047116838768558379187806699415465311881011361766441286822461.81219\n",
                "select id%5 key, max(d8) m8, max(d16) m16, max(d32) m32, " +
                        "max(d64) m64, max(d128) m128, max(d256) m256 " +
                        "from x " +
                        "order by key desc",
                "create table x as (" +
                        "select" +
                        " x id," +
                        " rnd_decimal(2,0,2) d8," +
                        " rnd_decimal(4,1,2) d16," +
                        " rnd_decimal(7,1,2) d32," +
                        " rnd_decimal(15,2,2) d64," +
                        " rnd_decimal(32,3,2) d128," +
                        " rnd_decimal(70,5,2) d256," +
                        " timestamp_sequence(0, 1000) ts" +
                        " from long_sequence(10000)" +
                        ") timestamp(ts) partition by month",
                null,
                true,
                true
        );
    }
}
