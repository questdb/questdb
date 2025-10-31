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

public class FirstDecimalGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testFirst() throws Exception {
        assertQuery(
                "f8\tf16\tf32\tf64\tf128\tf256\n" +
                        "1\t1.0\t1.0\t1.00\t1.000\t1.000000\n",
                "select first(d8) f8, first(d16) f16, first(d32) f32, first(d64) f64, first(d128) f128, first(d256) f256 from x",
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
    public void testFirstAllNull() throws Exception {
        assertQuery(
                "first\n\n",
                "select first(x) from (select cast(null as decimal(10,2)) x from long_sequence(1000))",
                null,
                false,
                true
        );
    }

    @Test
    public void testFirstKeyed() throws Exception {
        assertQuery(
                "key\tf8\tf16\tf32\tf64\tf128\tf256\n" +
                        "4\t70\t\t389048.8\t6408249039051.17\t12216318151301496245897961419.405\t31850478184631552508628555493605333879162869053463378487178070639.48857\n" +
                        "3\t4\t827.4\t87232.3\t3390459539777.63\t15645909974661302225752806082.585\t14683348116686869687524623962248136479924840218878560502313730441.12723\n" +
                        "2\t22\t708.0\t\t6399423916571.06\t\t21371473524489255821034919387834122566943409168796430462838175213.11368\n" +
                        "1\t39\t\t357570.8\t8435781410906.09\t\t61772473471096267235896434428639228331572588615230714313721136437.60846\n" +
                        "0\t35\t205.7\t\t5951841157618.99\t17298862804614406683231040975.838\t\n",
                "select id%5 key, first(d8) f8, first(d16) f16, first(d32) f32, " +
                        "first(d64) f64, first(d128) f128, first(d256) f256 " +
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
