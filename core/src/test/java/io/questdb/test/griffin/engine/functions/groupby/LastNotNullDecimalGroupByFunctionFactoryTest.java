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

public class LastNotNullDecimalGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testLastNotNull() throws Exception {
        assertQuery(
                "key\tl8\tl16\tl32\tl64\tl128\tl256\n" +
                        "4\t70\t695.1\t389048.8\t6408249039051.17\t10409682140997973547063160656.333\t31850478184631552508628555493605333879162869053463378487178070639.48857\n" +
                        "3\t4\t649.8\t87232.3\t3390459539777.63\t5212883105641932213650896752.227\t14683348116686869687524623962248136479924840218878560502313730441.12723\n" +
                        "2\t22\t515.9\t\t6399423916571.06\t915705502408806818202095691.275\t21371473524489255821034919387834122566943409168796430462838175213.11368\n" +
                        "1\t39\t687.4\t357570.8\t8435781410906.09\t3745189542191285695514669160.822\t61772473471096267235896434428639228331572588615230714313721136437.60846\n" +
                        "0\t35\t328.0\t\t5951841157618.99\t17933455213324506582633392291.209\t\n",
                "select id%5 key, last_not_null(d8) l8, last_not_null(d16) l16, last_not_null(d32) l32, " +
                        "last_not_null(d64) l64, last_not_null(d128) l128, last_not_null(d256) l256 " +
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

    @Test
    public void testLastNotNullAllNull() throws Exception {
        assertQuery(
                "last_not_null\n\n",
                "select last_not_null(x) from (select cast(null as decimal(10,2)) x from long_sequence(1000))",
                null,
                false,
                true
        );
    }
}
