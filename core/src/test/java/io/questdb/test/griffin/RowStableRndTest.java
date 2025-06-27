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

package io.questdb.test.griffin;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class RowStableRndTest extends AbstractCairoTest {
    @Test
    public void testRndInt() throws SqlException {
        allowFunctionPrefetch();
        // assertQuery does not reset rnd between SQL executions - using assertSql
        assertSql(
                "i\tk\n" +
                        "-1148479920\t-1148479910\n" +
                        "315515118\t315515128\n" +
                        "1548800833\t1548800843\n" +
                        "-727724771\t-727724761\n" +
                        "73575701\t73575711\n" +
                        "-948263339\t-948263329\n" +
                        "1326447242\t1326447252\n" +
                        "592859671\t592859681\n" +
                        "1868723706\t1868723716\n" +
                        "-847531048\t-847531038\n",
                "select rnd_int() i, i + 10 k from long_sequence(10)"
        );
    }

    @Test
    public void testRndIntBinary() throws SqlException {
        allowFunctionPrefetch();
        // assertQuery does not reset rnd between SQL executions - using assertSql
        assertSql(
                "i\tk\n" +
                        "1998007456\t1998007466\n" +
                        "631030236\t631030246\n" +
                        "-1197365630\t-1197365620\n" +
                        "-1455449542\t-1455449532\n" +
                        "147151402\t147151412\n" +
                        "-1896526678\t-1896526668\n" +
                        "-1642072812\t-1642072802\n" +
                        "1185719342\t1185719352\n" +
                        "-557519884\t-557519874\n" +
                        "-1695062096\t-1695062086\n",
                "select rnd_int() * 2 i, i + 10 k from long_sequence(10)"
        );
    }

    @Test
    public void testRndIntUnary() throws SqlException {
        allowFunctionPrefetch();
        // assertQuery does not reset rnd between SQL executions - using assertSql
        assertSql(
                "i\tk\n" +
                        "1148479920\t1148479930\n" +
                        "-315515118\t-315515108\n" +
                        "-1548800833\t-1548800823\n" +
                        "727724771\t727724781\n" +
                        "-73575701\t-73575691\n" +
                        "948263339\t948263349\n" +
                        "-1326447242\t-1326447232\n" +
                        "-592859671\t-592859661\n" +
                        "-1868723706\t-1868723696\n" +
                        "847531048\t847531058\n",
                "select -rnd_int() i, i + 10 k from long_sequence(10)"
        );
    }

    @Test
    public void testRndLong() throws SqlException {
        allowFunctionPrefetch();
        // assertQuery does not reset rnd between SQL executions - using assertSql
        assertSql(
                "i\tk\n" +
                        "4689592037643856\t4689592037643866\n" +
                        "4729996258992366\t4729996258992376\n" +
                        "7746536061816329025\t7746536061816329035\n" +
                        "-6945921502384501475\t-6945921502384501465\n" +
                        "8260188555232587029\t8260188555232587039\n" +
                        "8920866532787660373\t8920866532787660383\n" +
                        "-7611843578141082998\t-7611843578141082988\n" +
                        "-5354193255228091881\t-5354193255228091871\n" +
                        "-2653407051020864006\t-2653407051020863996\n" +
                        "-1675638984090602536\t-1675638984090602526\n",
                "select rnd_long() i, i + 10 k from long_sequence(10)"
        );
    }

    @Test
    public void testRndLongBinary() throws SqlException {
        allowFunctionPrefetch();
        // assertQuery does not reset rnd between SQL executions - using assertSql
        assertSql(
                "i\tk\n" +
                        "9379184075287712\t9379184075287722\n" +
                        "9459992517984732\t9459992517984742\n" +
                        "-2953671950076893566\t-2953671950076893556\n" +
                        "4554901068940548666\t4554901068940548676\n" +
                        "-1926366963244377558\t-1926366963244377548\n" +
                        "-605011008134230870\t-605011008134230860\n" +
                        "3223056917427385620\t3223056917427385630\n" +
                        "7738357563253367854\t7738357563253367864\n" +
                        "-5306814102041728012\t-5306814102041728002\n" +
                        "-3351277968181205072\t-3351277968181205062\n",
                "select rnd_long() * 2 i, i + 10 k from long_sequence(10)"
        );
    }

    @Test
    public void testRndLongUnary() throws SqlException {
        allowFunctionPrefetch();
        // assertQuery does not reset rnd between SQL executions - using assertSql
        assertSql(
                "i\tk\n" +
                        "-4689592037643856\t-4689592037643846\n" +
                        "-4729996258992366\t-4729996258992356\n" +
                        "-7746536061816329025\t-7746536061816329015\n" +
                        "6945921502384501475\t6945921502384501485\n" +
                        "-8260188555232587029\t-8260188555232587019\n" +
                        "-8920866532787660373\t-8920866532787660363\n" +
                        "7611843578141082998\t7611843578141083008\n" +
                        "5354193255228091881\t5354193255228091891\n" +
                        "2653407051020864006\t2653407051020864016\n" +
                        "1675638984090602536\t1675638984090602546\n",
                "select -rnd_long() i, i + 10 k from long_sequence(10)"
        );
    }

}
