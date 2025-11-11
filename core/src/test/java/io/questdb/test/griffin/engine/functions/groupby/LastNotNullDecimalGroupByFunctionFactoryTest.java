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
                "l8\tl16\tl32\tl64\tl128\tl256\n" +
                        "1\t0.0\t1.0\t5.00\t5.000\t5.000000\n",
                "select last_not_null(d8) l8, last_not_null(d16) l16, last_not_null(d32) l32, last_not_null(d64) l64, last_not_null(d128) l128, last_not_null(d256) l256 from x",
                "create table x as (" +
                        "select" +
                        " cast(x%2 as decimal(2,0)) d8, " +
                        " cast(x%3 as decimal(4,1)) d16, " +
                        " cast(x%4 as decimal(7,1)) d32, " +
                        " cast(x%10 as decimal(15,2)) d64, " +
                        " cast(x%100 as decimal(32,3)) d128, " +
                        " cast(x%1000 as decimal(76,6)) d256, " +
                        " timestamp_sequence(0, 1000) ts" +
                        " from long_sequence(1005)" +
                        ") timestamp(ts) partition by month",
                null,
                false,
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

    @Test
    public void testLastNotNullKeyed() throws Exception {
        assertQuery(
                "key\tl8\tl16\tl32\tl64\tl128\tl256\n" +
                        "4\t22\t695.1\t467658.0\t6250284030125.65\t10409682140997973547063160656.333\t31755846307251943477637882574691843024105350218826634181201381995.88860\n" +
                        "3\t91\t649.8\t995117.6\t9097895466409.14\t5212883105641932213650896752.227\t21429539433645944066723057138932043253063523632029335824995179403.39307\n" +
                        "2\t31\t515.9\t663556.5\t679948995819.30\t915705502408806818202095691.275\t12716906595555905635760015728326323204413658381366768359700697535.35139\n" +
                        "1\t84\t687.4\t472735.3\t6396112007189.61\t3745189542191285695514669160.822\t43374331755482849753506722442458838882904952417739777018150481166.89444\n" +
                        "0\t78\t328.0\t716136.1\t5982959190202.73\t17933455213324506582633392291.209\t41215786599361552856612910748212057337915136763269251855851967201.70522\n",
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
}
