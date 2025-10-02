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

public class MinDecimalGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testMin() throws Exception {
        assertQuery(
                "m8\tm16\tm32\tm64\tm128\tm256\n" +
                        "0\t0.0\t0.0\t0.00\t0.000\t1.000000\n",
                "select min(d8) m8, min(d16) m16, min(d32) m32, min(d64) m64, min(d128) m128, min(d256) m256 from x",
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
    public void testMinAllNull() throws Exception {
        assertQuery(
                "min\n\n",
                "select min(x) from (select cast(null as decimal(10,2)) x from long_sequence(1000))",
                null,
                false,
                true
        );
    }

    @Test
    public void testMinKeyed() throws Exception {
        assertQuery(
                "key\tm8\tm16\tm32\tm64\tm128\tm256\n" +
                        "4\t0\t0.7\t45.6\t9516116412.42\t1249700766257304067881104.715\t26569896652744545227258357538980448808375418037382703146739545.53411\n" +
                        "3\t0\t1.0\t764.3\t14413140006.68\t14009899734713494655243293.977\t18424786054828937641371486332303834735549581809675825882017721.24503\n" +
                        "2\t0\t0.1\t108.0\t2355986723.93\t29350537582167712880732141.975\t98251806332045731296204472304201465326768046788025211797176160.79739\n" +
                        "1\t0\t1.0\t58.5\t4613783414.13\t9334117075839212356219090.391\t57394877098324224183907040745420083885435039013499806304650942.51905\n" +
                        "0\t0\t0.6\t1412.2\t2945918070.21\t13538565735969580352006935.514\t26143183604600090594250684619353778141518714287817295801364560.99782\n",
                "select id%5 key, min(d8) m8, min(d16) m16, min(d32) m32, " +
                        "min(d64) m64, min(d128) m128, min(d256) m256 " +
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
