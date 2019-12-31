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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class CastTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testIntToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(rnd_int(2,100, 10) as byte) from long_sequence(10)",
                "a\n" +
                        "41\n" +
                        "28\n" +
                        "0\n" +
                        "100\n" +
                        "5\n" +
                        "72\n" +
                        "72\n" +
                        "24\n" +
                        "53\n" +
                        "50\n",
                true
        );
    }

    @Test
    public void testIntToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(rnd_int(23,56,100) as short) from long_sequence(10)",
                "a\n" +
                        "37\n" +
                        "48\n" +
                        "30\n" +
                        "0\n" +
                        "55\n" +
                        "51\n" +
                        "53\n" +
                        "54\n" +
                        "23\n" +
                        "34\n",
                true
        );
    }

    @Test
    public void testIntToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast(rnd_int(34,66,100) as char) from long_sequence(10)",
                "a\n" +
                        "(\n" +
                        "<\n" +
                        "9\n" +
                        "\n" +
                        "%\n" +
                        "&\n" +
                        "&\n" +
                        "8\n" +
                        "4\n" +
                        "1\n",
                true
        );
    }

    @Test
    public void testIntToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_int(34,66,100) as int) from long_sequence(10)",
                "a\n" +
                        "40\n" +
                        "60\n" +
                        "57\n" +
                        "NaN\n" +
                        "37\n" +
                        "38\n" +
                        "38\n" +
                        "56\n" +
                        "52\n" +
                        "49\n",
                true
        );
    }

    @Test
    public void testIntToStr() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(rnd_int(34,66,100) as string) from long_sequence(10)",
                "a\n" +
                        "40\n" +
                        "60\n" +
                        "57\n" +
                        "\n" +
                        "37\n" +
                        "38\n" +
                        "38\n" +
                        "56\n" +
                        "52\n" +
                        "49\n",
                true
        );
    }

    @Test
    public void testIntToStrSort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "102\n" +
                        "104\n" +
                        "105\n" +
                        "106\n" +
                        "117\n" +
                        "119\n" +
                        "173\n" +
                        "198\n" +
                        "43\n" +
                        "49\n" +
                        "51\n" +
                        "56\n" +
                        "75\n",
                "select cast(a as string) x from tt order by x",
                "create table tt as (select rnd_int(1,200,1) a from long_sequence(20))",
                null,
                true
        );
    }

    @Test
    public void testIntToStrConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(334 as string) from long_sequence(10)",
                "a\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n" +
                        "334\n",
                true
        );
    }

    @Test
    public void testIntToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(rnd_int(1,150,100) as long) from long_sequence(10)",
                "a\n" +
                        "19\n" +
                        "72\n" +
                        "90\n" +
                        "NaN\n" +
                        "7\n" +
                        "17\n" +
                        "65\n" +
                        "32\n" +
                        "67\n" +
                        "106\n",
                true
        );
    }

    @Test
    public void testIntToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(rnd_int(1,150,100) as float) from long_sequence(10)",
                "a\n" +
                        "19.0000\n" +
                        "72.0000\n" +
                        "90.0000\n" +
                        "NaN\n" +
                        "7.0000\n" +
                        "17.0000\n" +
                        "65.0000\n" +
                        "32.0000\n" +
                        "67.0000\n" +
                        "106.0000\n",
                true
        );
    }

    @Test
    public void testIntToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(rnd_int(1,150,100) as double) from long_sequence(10)",
                "a\n" +
                        "19.000000000000\n" +
                        "72.000000000000\n" +
                        "90.000000000000\n" +
                        "NaN\n" +
                        "7.000000000000\n" +
                        "17.000000000000\n" +
                        "65.000000000000\n" +
                        "32.000000000000\n" +
                        "67.000000000000\n" +
                        "106.000000000000\n",
                true
        );
    }

    @Test
    public void testIntToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(rnd_int(1,150,100) as date) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.019Z\n" +
                        "1970-01-01T00:00:00.072Z\n" +
                        "1970-01-01T00:00:00.090Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.007Z\n" +
                        "1970-01-01T00:00:00.017Z\n" +
                        "1970-01-01T00:00:00.065Z\n" +
                        "1970-01-01T00:00:00.032Z\n" +
                        "1970-01-01T00:00:00.067Z\n" +
                        "1970-01-01T00:00:00.106Z\n",
                true
        );
    }

    @Test
    public void testIntToTimestamp() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp)",
                null,
                "insert into tab select cast(rnd_int(1,150,100) as timestamp) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000019Z\n" +
                        "1970-01-01T00:00:00.000072Z\n" +
                        "1970-01-01T00:00:00.000090Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000007Z\n" +
                        "1970-01-01T00:00:00.000017Z\n" +
                        "1970-01-01T00:00:00.000065Z\n" +
                        "1970-01-01T00:00:00.000032Z\n" +
                        "1970-01-01T00:00:00.000067Z\n" +
                        "1970-01-01T00:00:00.000106Z\n",
                true
        );
    }

    @Test
    public void testIntToSymbol() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(rnd_int(1,150,100) as symbol) from long_sequence(10)",
                "a\n" +
                        "19\n" +
                        "72\n" +
                        "90\n" +
                        "\n" +
                        "7\n" +
                        "17\n" +
                        "65\n" +
                        "32\n" +
                        "67\n" +
                        "106\n",
                true
        );
    }

    @Test
    public void testIntToSymbolConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(601 as symbol) from long_sequence(10)",
                "a\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n" +
                        "601\n",
                true
        );
    }

    @Test
    @Ignore
    public void testIntToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "16\t16\n" +
                        "\tNaN\n" +
                        "11\t11\n" +
                        "20\t20\n" +
                        "\tNaN\n" +
                        "11\t11\n" +
                        "12\t12\n" +
                        "18\t18\n" +
                        "\tNaN\n" +
                        "17\t17\n" +
                        "\tNaN\n" +
                        "16\t16\n" +
                        "\tNaN\n" +
                        "19\t19\n" +
                        "15\t15\n" +
                        "15\t15\n" +
                        "\tNaN\n" +
                        "12\t12\n" +
                        "15\t15\n" +
                        "18\t18\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_int(10, 20, 2) a from long_sequence(20))",
                null,
                true
        );
    }

}