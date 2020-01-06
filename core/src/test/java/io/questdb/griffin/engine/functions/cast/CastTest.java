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
    public void testByteToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(rnd_byte() as byte) from long_sequence(10)",
                "a\n" +
                        "76\n" +
                        "102\n" +
                        "27\n" +
                        "87\n" +
                        "79\n" +
                        "79\n" +
                        "122\n" +
                        "83\n" +
                        "90\n" +
                        "76\n",
                true
        );
    }

    @Test
    public void testLongToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(rnd_long(96,100, 10) as byte) from long_sequence(10)",
                "a\n" +
                        "97\n" +
                        "96\n" +
                        "0\n" +
                        "99\n" +
                        "97\n" +
                        "98\n" +
                        "100\n" +
                        "100\n" +
                        "96\n" +
                        "97\n",
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
    public void testByteToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(rnd_byte() as short) from long_sequence(10)",
                "a\n" +
                        "76\n" +
                        "102\n" +
                        "27\n" +
                        "87\n" +
                        "79\n" +
                        "79\n" +
                        "122\n" +
                        "83\n" +
                        "90\n" +
                        "76\n",
                true
        );
    }

    @Test
    public void testByteToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast(rnd_byte() as char) from long_sequence(10)",
                "a\n" +
                        "L\n" +
                        "f\n" +
                        "\u001B\n" +
                        "W\n" +
                        "O\n" +
                        "O\n" +
                        "z\n" +
                        "S\n" +
                        "Z\n" +
                        "L\n",
                true
        );
    }

    @Test
    public void testByteToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_byte() as int) from long_sequence(10)",
                "a\n" +
                        "76\n" +
                        "102\n" +
                        "27\n" +
                        "87\n" +
                        "79\n" +
                        "79\n" +
                        "122\n" +
                        "83\n" +
                        "90\n" +
                        "76\n",
                true
        );
    }

    @Test
    public void testByteToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(rnd_byte() as float) from long_sequence(10)",
                "a\n" +
                        "76.0000\n" +
                        "102.0000\n" +
                        "27.0000\n" +
                        "87.0000\n" +
                        "79.0000\n" +
                        "79.0000\n" +
                        "122.0000\n" +
                        "83.0000\n" +
                        "90.0000\n" +
                        "76.0000\n",
                true
        );
    }

    @Test
    public void testByteToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(rnd_byte() as double) from long_sequence(10)",
                "a\n" +
                        "76.000000000000\n" +
                        "102.000000000000\n" +
                        "27.000000000000\n" +
                        "87.000000000000\n" +
                        "79.000000000000\n" +
                        "79.000000000000\n" +
                        "122.000000000000\n" +
                        "83.000000000000\n" +
                        "90.000000000000\n" +
                        "76.000000000000\n",
                true
        );
    }

    @Test
    public void testByteToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(rnd_byte() as date) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.076Z\n" +
                        "1970-01-01T00:00:00.102Z\n" +
                        "1970-01-01T00:00:00.027Z\n" +
                        "1970-01-01T00:00:00.087Z\n" +
                        "1970-01-01T00:00:00.079Z\n" +
                        "1970-01-01T00:00:00.079Z\n" +
                        "1970-01-01T00:00:00.122Z\n" +
                        "1970-01-01T00:00:00.083Z\n" +
                        "1970-01-01T00:00:00.090Z\n" +
                        "1970-01-01T00:00:00.076Z\n",
                true
        );
    }

    @Test
    public void testByteToBoolean() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(rnd_byte() as boolean) from long_sequence(10)",
                "a\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n",
                true
        );
    }

    @Test
    public void testByteToBooleanConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(cast(0 as byte) as boolean) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true
        );
    }

    @Test
    public void testByteToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(rnd_byte() as long) from long_sequence(10)",
                "a\n" +
                        "76\n" +
                        "102\n" +
                        "27\n" +
                        "87\n" +
                        "79\n" +
                        "79\n" +
                        "122\n" +
                        "83\n" +
                        "90\n" +
                        "76\n",
                true
        );
    }

    @Test
    public void testByteToLong256() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long256)",
                null,
                "insert into tab select cast(rnd_byte() as long256) from long_sequence(10)",
                "a\n" +
                        "0x4c\n" +
                        "0x66\n" +
                        "0x1b\n" +
                        "0x57\n" +
                        "0x4f\n" +
                        "0x4f\n" +
                        "0x7a\n" +
                        "0x53\n" +
                        "0x5a\n" +
                        "0x4c\n",
                true
        );
    }

    @Test
    public void testLongToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(rnd_long(23,56,100) as short) from long_sequence(10)",
                "a\n" +
                        "31\n" +
                        "26\n" +
                        "38\n" +
                        "0\n" +
                        "47\n" +
                        "41\n" +
                        "53\n" +
                        "46\n" +
                        "51\n" +
                        "46\n",
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
    public void testLongToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast(rnd_long(34,66,100) as char) from long_sequence(10)",
                "a\n" +
                        "7\n" +
                        "0\n" +
                        "7\n" +
                        "\n" +
                        "-\n" +
                        "$\n" +
                        "\"\n" +
                        ":\n" +
                        "-\n" +
                        "=\n",
                true
        );
    }

    @Test
    public void testIntToLong256() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long256)",
                null,
                "insert into tab select cast(rnd_int(34,66,100) as long256) from long_sequence(10)",
                "a\n" +
                        "0x28\n" +
                        "0x3c\n" +
                        "0x39\n" +
                        "\n" +
                        "0x25\n" +
                        "0x26\n" +
                        "0x26\n" +
                        "0x38\n" +
                        "0x34\n" +
                        "0x31\n",
                true
        );
    }

    @Test
    public void testLongToLong256() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long256)",
                null,
                "insert into tab select cast(rnd_long(34,66,100) as long256) from long_sequence(10)",
                "a\n" +
                        "0x37\n" +
                        "0x30\n" +
                        "0x37\n" +
                        "\n" +
                        "0x2d\n" +
                        "0x24\n" +
                        "0x22\n" +
                        "0x3a\n" +
                        "0x2d\n" +
                        "0x3d\n",
                true
        );
    }

    @Test
    public void testIntToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0x2b\n" +
                        "0x31\n" +
                        "0x33\n" +
                        "0x38\n" +
                        "0x4b\n" +
                        "0x66\n" +
                        "0x68\n" +
                        "0x69\n" +
                        "0x6a\n" +
                        "0x75\n" +
                        "0x77\n" +
                        "0xad\n" +
                        "0xc6\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_int(1,200,1) a from long_sequence(20))",
                null,
                true
        );
    }

    @Test
    public void testByteToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
                        "0x15\n" +
                        "0x1b\n" +
                        "0x20\n" +
                        "0x37\n" +
                        "0x4a\n" +
                        "0x4a\n" +
                        "0x4c\n" +
                        "0x4c\n" +
                        "0x4f\n" +
                        "0x4f\n" +
                        "0x53\n" +
                        "0x53\n" +
                        "0x54\n" +
                        "0x54\n" +
                        "0x57\n" +
                        "0x58\n" +
                        "0x5a\n" +
                        "0x5b\n" +
                        "0x66\n" +
                        "0x7a\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_byte() a from long_sequence(20))",
                null,
                true
        );
    }

    @Test
    public void testLongToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0x08\n" +
                        "0x11\n" +
                        "0x1e\n" +
                        "0x34\n" +
                        "0x3d\n" +
                        "0x4d\n" +
                        "0x57\n" +
                        "0x63\n" +
                        "0x80\n" +
                        "0x89\n" +
                        "0xa7\n" +
                        "0xc0\n" +
                        "0xc7\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_long(1,200,1) a from long_sequence(20))",
                null,
                true
        );
    }

    @Test
    public void testIntToBoolean() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(rnd_int(0,66,100) as boolean) from long_sequence(10)",
                "a\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n",
                true
        );
    }

    @Test
    public void testLongToBoolean() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(rnd_long(0,66,100) as boolean) from long_sequence(10)",
                "a\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "false\n",
                true
        );
    }

    @Test
    public void testIntToBooleanConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(0 as boolean) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
                true
        );
    }

    @Test
    public void testLongToBooleanConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(0l as boolean) from long_sequence(10)",
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n",
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
    public void testLongToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_long(34,66,100) as int) from long_sequence(10)",
                "a\n" +
                        "55\n" +
                        "48\n" +
                        "55\n" +
                        "NaN\n" +
                        "45\n" +
                        "36\n" +
                        "34\n" +
                        "58\n" +
                        "45\n" +
                        "61\n",
                true
        );
    }

    @Test
    public void testDoubleToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_double()*100 as int) from long_sequence(10)",
                "a\n" +
                        "66\n" +
                        "22\n" +
                        "8\n" +
                        "29\n" +
                        "20\n" +
                        "65\n" +
                        "84\n" +
                        "98\n" +
                        "22\n" +
                        "50\n",
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
    public void testByteToStr() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(rnd_byte() as string) from long_sequence(10)",
                "a\n" +
                        "76\n" +
                        "102\n" +
                        "27\n" +
                        "87\n" +
                        "79\n" +
                        "79\n" +
                        "122\n" +
                        "83\n" +
                        "90\n" +
                        "76\n",
                true
        );
    }

    @Test
    public void testByteToStrConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(cast(34 as byte) as string) from long_sequence(10)",
                "a\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n" +
                        "34\n",
                true
        );
    }

    @Test
    public void testLongToStr() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(rnd_long(34,66,100) as string) from long_sequence(10)",
                "a\n" +
                        "55\n" +
                        "48\n" +
                        "55\n" +
                        "\n" +
                        "45\n" +
                        "36\n" +
                        "34\n" +
                        "58\n" +
                        "45\n" +
                        "61\n",
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
    public void testByteToStrSort() throws Exception {
        assertQuery(
                "x\n" +
                        "102\n" +
                        "122\n" +
                        "21\n" +
                        "27\n" +
                        "32\n" +
                        "55\n" +
                        "74\n" +
                        "74\n" +
                        "76\n" +
                        "76\n" +
                        "79\n" +
                        "79\n" +
                        "83\n" +
                        "83\n" +
                        "84\n" +
                        "84\n" +
                        "87\n" +
                        "88\n" +
                        "90\n" +
                        "91\n",
                "select cast(a as string) x from tt order by x",
                "create table tt as (select rnd_byte() a from long_sequence(20))",
                null,
                true
        );
    }

    @Test
    public void testLongToStrSort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "128\n" +
                        "137\n" +
                        "167\n" +
                        "17\n" +
                        "192\n" +
                        "199\n" +
                        "30\n" +
                        "52\n" +
                        "61\n" +
                        "77\n" +
                        "8\n" +
                        "87\n" +
                        "99\n",
                "select cast(a as string) x from tt order by x",
                "create table tt as (select rnd_long(1,200,1) a from long_sequence(20))",
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
    public void testLongToStrConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(334l as string) from long_sequence(10)",
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
    public void testTimestampToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(cast(rnd_long(1,15000000,100) as timestamp) as long) from long_sequence(10)",
                "a\n" +
                        "13992367\n" +
                        "4501476\n" +
                        "2660374\n" +
                        "NaN\n" +
                        "5864007\n" +
                        "10281933\n" +
                        "6977935\n" +
                        "9100840\n" +
                        "8600061\n" +
                        "478012\n",
                true
        );
    }

    @Test
    public void testLongToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(rnd_long(1,150,100) as long) from long_sequence(10)",
                "a\n" +
                        "67\n" +
                        "126\n" +
                        "124\n" +
                        "NaN\n" +
                        "57\n" +
                        "33\n" +
                        "85\n" +
                        "40\n" +
                        "111\n" +
                        "112\n",
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
    public void testLongToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(rnd_long(1,150,100) as float) from long_sequence(10)",
                "a\n" +
                        "67.0000\n" +
                        "126.0000\n" +
                        "124.0000\n" +
                        "NaN\n" +
                        "57.0000\n" +
                        "33.0000\n" +
                        "85.0000\n" +
                        "40.0000\n" +
                        "111.0000\n" +
                        "112.0000\n",
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
    public void testLongToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(rnd_long(1,150,100) as double) from long_sequence(10)",
                "a\n" +
                        "67.000000000000\n" +
                        "126.000000000000\n" +
                        "124.000000000000\n" +
                        "NaN\n" +
                        "57.000000000000\n" +
                        "33.000000000000\n" +
                        "85.000000000000\n" +
                        "40.000000000000\n" +
                        "111.000000000000\n" +
                        "112.000000000000\n",
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
    public void testLongToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(rnd_long(1,150,100) as date) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.067Z\n" +
                        "1970-01-01T00:00:00.126Z\n" +
                        "1970-01-01T00:00:00.124Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.057Z\n" +
                        "1970-01-01T00:00:00.033Z\n" +
                        "1970-01-01T00:00:00.085Z\n" +
                        "1970-01-01T00:00:00.040Z\n" +
                        "1970-01-01T00:00:00.111Z\n" +
                        "1970-01-01T00:00:00.112Z\n",
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
    public void testByteToTimestamp() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp)",
                null,
                "insert into tab select cast(rnd_byte() as timestamp) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000076Z\n" +
                        "1970-01-01T00:00:00.000102Z\n" +
                        "1970-01-01T00:00:00.000027Z\n" +
                        "1970-01-01T00:00:00.000087Z\n" +
                        "1970-01-01T00:00:00.000079Z\n" +
                        "1970-01-01T00:00:00.000079Z\n" +
                        "1970-01-01T00:00:00.000122Z\n" +
                        "1970-01-01T00:00:00.000083Z\n" +
                        "1970-01-01T00:00:00.000090Z\n" +
                        "1970-01-01T00:00:00.000076Z\n",
                true
        );
    }

    @Test
    public void testLongToTimestamp() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp)",
                null,
                "insert into tab select cast(rnd_long(1,150,100) as timestamp) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000067Z\n" +
                        "1970-01-01T00:00:00.000126Z\n" +
                        "1970-01-01T00:00:00.000124Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.000057Z\n" +
                        "1970-01-01T00:00:00.000033Z\n" +
                        "1970-01-01T00:00:00.000085Z\n" +
                        "1970-01-01T00:00:00.000040Z\n" +
                        "1970-01-01T00:00:00.000111Z\n" +
                        "1970-01-01T00:00:00.000112Z\n",
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
    public void testByteToSymbol() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(rnd_byte() as symbol) from long_sequence(10)",
                "a\n" +
                        "76\n" +
                        "102\n" +
                        "27\n" +
                        "87\n" +
                        "79\n" +
                        "79\n" +
                        "122\n" +
                        "83\n" +
                        "90\n" +
                        "76\n",
                true
        );
    }

    @Test
    public void testLongToSymbol() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(rnd_long(1,150,100) as symbol) from long_sequence(10)",
                "a\n" +
                        "67\n" +
                        "126\n" +
                        "124\n" +
                        "\n" +
                        "57\n" +
                        "33\n" +
                        "85\n" +
                        "40\n" +
                        "111\n" +
                        "112\n",
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
    public void testByteToSymbolConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(cast(14 as byte) as symbol) from long_sequence(10)",
                "a\n" +
                        "14\n" +
                        "14\n" +
                        "14\n" +
                        "14\n" +
                        "14\n" +
                        "14\n" +
                        "14\n" +
                        "14\n" +
                        "14\n" +
                        "14\n",
                true
        );
    }

    @Test
    public void testLongToSymbolConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(601l as symbol) from long_sequence(10)",
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

    @Test
    public void testByteToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "76\t76\n" +
                        "102\t102\n" +
                        "27\t27\n" +
                        "87\t87\n" +
                        "79\t79\n" +
                        "79\t79\n" +
                        "122\t122\n" +
                        "83\t83\n" +
                        "90\t90\n" +
                        "76\t76\n" +
                        "84\t84\n" +
                        "84\t84\n" +
                        "74\t74\n" +
                        "55\t55\n" +
                        "83\t83\n" +
                        "88\t88\n" +
                        "32\t32\n" +
                        "21\t21\n" +
                        "91\t91\n" +
                        "74\t74\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_byte() a from long_sequence(20))",
                null,
                true
        );
    }

    @Test
    public void testLongToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "20\t20\n" +
                        "\tNaN\n" +
                        "11\t11\n" +
                        "13\t13\n" +
                        "\tNaN\n" +
                        "15\t15\n" +
                        "19\t19\n" +
                        "17\t17\n" +
                        "\tNaN\n" +
                        "10\t10\n" +
                        "\tNaN\n" +
                        "17\t17\n" +
                        "\tNaN\n" +
                        "17\t17\n" +
                        "18\t18\n" +
                        "18\t18\n" +
                        "\tNaN\n" +
                        "12\t12\n" +
                        "11\t11\n" +
                        "15\t15\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_long(10, 20, 2) a from long_sequence(20))",
                null,
                true
        );
    }

    @Test
    public void testStrToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "JWCP\tJWCP\n" +
                        "\t\n" +
                        "YRXP\tYRXP\n" +
                        "NRXGZ\tNRXGZ\n" +
                        "UXIBB\tUXIBB\n" +
                        "PGWFF\tPGWFF\n" +
                        "DEYYQ\tDEYYQ\n" +
                        "BHFOW\tBHFOW\n" +
                        "DXYS\tDXYS\n" +
                        "OUOJ\tOUOJ\n" +
                        "RUED\tRUED\n" +
                        "QULO\tQULO\n" +
                        "GETJ\tGETJ\n" +
                        "ZSRYR\tZSRYR\n" +
                        "VTMHG\tVTMHG\n" +
                        "ZZVD\tZZVD\n" +
                        "MYICC\tMYICC\n" +
                        "OUIC\tOUIC\n" +
                        "KGHV\tKGHV\n" +
                        "SDOTS\tSDOTS\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_str(4,5,100) a from long_sequence(20))",
                null,
                true
        );
    }

    @Test
    public void testStrToSymbolConst() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "abc\tJWCP\n" +
                        "abc\t\n" +
                        "abc\tYRXP\n" +
                        "abc\tNRXGZ\n" +
                        "abc\tUXIBB\n" +
                        "abc\tPGWFF\n" +
                        "abc\tDEYYQ\n" +
                        "abc\tBHFOW\n" +
                        "abc\tDXYS\n" +
                        "abc\tOUOJ\n" +
                        "abc\tRUED\n" +
                        "abc\tQULO\n" +
                        "abc\tGETJ\n" +
                        "abc\tZSRYR\n" +
                        "abc\tVTMHG\n" +
                        "abc\tZZVD\n" +
                        "abc\tMYICC\n" +
                        "abc\tOUIC\n" +
                        "abc\tKGHV\n" +
                        "abc\tSDOTS\n",
                "select cast('abc' as symbol) b, a from tab",
                "create table tab as (select rnd_str(4,5,100) a from long_sequence(20))",
                null,
                true,
                false
        );
    }
}