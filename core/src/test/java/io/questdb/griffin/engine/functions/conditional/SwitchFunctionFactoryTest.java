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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class SwitchFunctionFactoryTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    @Ignore
    public void testDouble() throws Exception {
        assertQuery(
                "x\tcase\n" +
                        "-920\t0.804322409997\n" +
                        "671\tNaN\n" +
                        "481\tNaN\n" +
                        "147\t0.524372285929\n" +
                        "-55\t0.726113620982\n" +
                        "-769\t0.310054598386\n" +
                        "-831\t0.524932106269\n" +
                        "-914\t0.621732670785\n" +
                        "-463\t0.125030421903\n" +
                        "-194\t0.676193485708\n" +
                        "-835\t0.788306583006\n" +
                        "-933\t0.552249417051\n" +
                        "416\tNaN\n" +
                        "380\tNaN\n" +
                        "-574\t0.799773322997\n" +
                        "-722\t0.404554697479\n" +
                        "-128\t0.882822836670\n" +
                        "-842\t0.956623654944\n" +
                        "-123\t0.926906851955\n" +
                        "535\tNaN\n",
                "select \n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case a\n" +
                        "        when 0.726113620982 then b\n" +
                        "        when 0.621732670785 then 150\n" +
                        "    end \n" +
                        "from tanc",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_double() a," +
                        " rnd_double() b," +
                        " rnd_double() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testInt() throws Exception {
        assertQuery(
                "x\ta\tb\tc\tk\n" +
                        "-920\t315515118\t1548800833\t-727724771\t315515118\n" +
                        "701\t-948263339\t1326447242\t592859671\t592859671\n" +
                        "706\t-847531048\t-1191262516\t-2041844972\tNaN\n" +
                        "-714\t-1575378703\t806715481\t1545253512\t350\n" +
                        "116\t1573662097\t-409854405\t339631474\tNaN\n" +
                        "67\t1904508147\t-1532328444\t-1458132197\tNaN\n" +
                        "207\t-1849627000\t-1432278050\t426455968\tNaN\n" +
                        "-55\t-1792928964\t-1844391305\t-1520872171\tNaN\n" +
                        "-104\t-1153445279\t1404198\t-1715058769\tNaN\n" +
                        "-127\t1631244228\t-1975183723\t-1252906348\tNaN\n" +
                        "790\t-761275053\t-2119387831\t-212807500\tNaN\n" +
                        "881\t1110979454\t1253890363\t-113506296\tNaN\n" +
                        "-535\t-938514914\t-547127752\t-1271909747\tNaN\n" +
                        "-973\t-342047842\t-2132716300\t2006313928\tNaN\n" +
                        "-463\t-27395319\t264240638\t2085282008\tNaN\n" +
                        "-667\t2137969456\t1890602616\t-1272693194\tNaN\n" +
                        "578\t1036510002\t-2002373666\t44173540\tNaN\n" +
                        "940\t410717394\t-2144581835\t1978144263\tNaN\n" +
                        "-54\t-1162267908\t2031014705\t-530317703\tNaN\n" +
                        "-393\t-296610933\t936627841\t326010667\tNaN\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when -920 then a\n" +
                        "        when 701 then c\n" +
                        "        when -714 then 350\n" +
                        "    end k\n" +
                        "from tanc",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testIntOrElse() throws Exception {
        assertQuery(
                "x\ta\tb\tc\tk\n" +
                        "-920\t315515118\t1548800833\t-727724771\t315515118\n" +
                        "701\t-948263339\t1326447242\t592859671\t592859671\n" +
                        "706\t-847531048\t-1191262516\t-2041844972\t-1191262516\n" +
                        "-714\t-1575378703\t806715481\t1545253512\t350\n" +
                        "116\t1573662097\t-409854405\t339631474\t-409854405\n" +
                        "67\t1904508147\t-1532328444\t-1458132197\t-1532328444\n" +
                        "207\t-1849627000\t-1432278050\t426455968\t-1432278050\n" +
                        "-55\t-1792928964\t-1844391305\t-1520872171\t-1844391305\n" +
                        "-104\t-1153445279\t1404198\t-1715058769\t1404198\n" +
                        "-127\t1631244228\t-1975183723\t-1252906348\t-1975183723\n" +
                        "790\t-761275053\t-2119387831\t-212807500\t-2119387831\n" +
                        "881\t1110979454\t1253890363\t-113506296\t1253890363\n" +
                        "-535\t-938514914\t-547127752\t-1271909747\t-547127752\n" +
                        "-973\t-342047842\t-2132716300\t2006313928\t-2132716300\n" +
                        "-463\t-27395319\t264240638\t2085282008\t264240638\n" +
                        "-667\t2137969456\t1890602616\t-1272693194\t1890602616\n" +
                        "578\t1036510002\t-2002373666\t44173540\t-2002373666\n" +
                        "940\t410717394\t-2144581835\t1978144263\t-2144581835\n" +
                        "-54\t-1162267908\t2031014705\t-530317703\t2031014705\n" +
                        "-393\t-296610933\t936627841\t326010667\t936627841\n",
                "select \n" +
                        "    x,\n" +
                        "    a,\n" +
                        "    b,\n" +
                        "    c,\n" +
                        "    case x\n" +
                        "        when -920 then a\n" +
                        "        when 701 then c\n" +
                        "        when -714 then 350\n" +
                        "        else b\n" +
                        "    end k\n" +
                        "from tanc",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true
        );
    }

    @Test
    @Ignore
    public void testShort() throws Exception {
        assertQuery(
                "x\tk\n" +
                        "-920\t0\n" +
                        "701\t0\n" +
                        "706\t0\n" +
                        "-714\t0\n" +
                        "116\t0\n" +
                        "67\t0\n" +
                        "207\t11230\n" +
                        "-55\t0\n" +
                        "-104\t0\n" +
                        "-127\t0\n" +
                        "790\t0\n" +
                        "881\t0\n" +
                        "-535\t0\n" +
                        "-973\t-15458\n" +
                        "-463\t0\n" +
                        "-667\t0\n" +
                        "578\t0\n" +
                        "940\t0\n" +
                        "-54\t0\n" +
                        "-393\t0\n",
                "select \n" +
                        "    a,\n" +
                        "    case a\n" +
                        "        when 120 then a\n" +
                        "        when 207 then b\n" +
                        "    end k \n" +
                        "from tanc",
                "create table tanc as (" +
                        "select " +
                        " rnd_short() a," +
                        " rnd_short() b," +
                        " rnd_short() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true
        );
    }
}