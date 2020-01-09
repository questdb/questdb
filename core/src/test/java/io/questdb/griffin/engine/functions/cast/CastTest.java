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
    public void testCharToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(rnd_char() as byte) from long_sequence(10)",
                "a\n" +
                        "86\n" +
                        "84\n" +
                        "74\n" +
                        "87\n" +
                        "67\n" +
                        "80\n" +
                        "83\n" +
                        "87\n" +
                        "72\n" +
                        "89\n",
                true
        );
    }

    @Test
    public void testShortToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(rnd_short() as byte) from long_sequence(10)",
                "a\n" +
                        "80\n" +
                        "-18\n" +
                        "65\n" +
                        "29\n" +
                        "21\n" +
                        "85\n" +
                        "-118\n" +
                        "23\n" +
                        "-6\n" +
                        "-40\n",
                true
        );
    }

    @Test
    public void testDoubleToByte() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a byte)",
                null,
                "insert into tab select cast(rnd_double(4)*10 as byte) from long_sequence(10)",
                "a\n" +
                        "8\n" +
                        "0\n" +
                        "0\n" +
                        "6\n" +
                        "7\n" +
                        "2\n" +
                        "3\n" +
                        "7\n" +
                        "4\n" +
                        "0\n",
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
    public void testDoubleToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(rnd_double(5)*10 as short) from long_sequence(10)",
                "a\n" +
                        "8\n" +
                        "0\n" +
                        "0\n" +
                        "6\n" +
                        "7\n" +
                        "2\n" +
                        "3\n" +
                        "7\n" +
                        "4\n" +
                        "0\n",
                true
        );
    }

    @Test
    public void testCharToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(rnd_char() as short) from long_sequence(10)",
                "a\n" +
                        "86\n" +
                        "84\n" +
                        "74\n" +
                        "87\n" +
                        "67\n" +
                        "80\n" +
                        "83\n" +
                        "87\n" +
                        "72\n" +
                        "89\n",
                true
        );
    }

    @Test
    public void testShortToShort() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a short)",
                null,
                "insert into tab select cast(rnd_short() as short) from long_sequence(10)",
                "a\n" +
                        "-27056\n" +
                        "24814\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-21227\n" +
                        "-22955\n" +
                        "-1398\n" +
                        "21015\n" +
                        "30202\n" +
                        "-19496\n",
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
    public void testShortToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast(rnd_short(34,66) as char) from long_sequence(10)",
                "a\n" +
                        "?\n" +
                        "A\n" +
                        "&\n" +
                        ";\n" +
                        "*\n" +
                        "6\n" +
                        ".\n" +
                        "=\n" +
                        ")\n" +
                        "<\n",
                true
        );
    }

    @Test
    public void testCharToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast(rnd_char() as char) from long_sequence(10)",
                "a\n" +
                        "V\n" +
                        "T\n" +
                        "J\n" +
                        "W\n" +
                        "C\n" +
                        "P\n" +
                        "S\n" +
                        "W\n" +
                        "H\n" +
                        "Y\n",
                true
        );
    }

    @Test
    public void testDoubleToChar() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a char)",
                null,
                "insert into tab select cast(33 + (rnd_double() * 100) % 25 as char) from long_sequence(10)",
                "a\n" +
                        "1\n" +
                        "7\n" +
                        ")\n" +
                        "%\n" +
                        "5\n" +
                        "0\n" +
                        "*\n" +
                        "8\n" +
                        "7\n" +
                        "!\n",
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
    public void testCharToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_char() as int) from long_sequence(10)",
                "a\n" +
                        "86\n" +
                        "84\n" +
                        "74\n" +
                        "87\n" +
                        "67\n" +
                        "80\n" +
                        "83\n" +
                        "87\n" +
                        "72\n" +
                        "89\n",
                true
        );
    }

    @Test
    public void testShortToInt() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a int)",
                null,
                "insert into tab select cast(rnd_short() as int) from long_sequence(10)",
                "a\n" +
                        "-27056\n" +
                        "24814\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-21227\n" +
                        "-22955\n" +
                        "-1398\n" +
                        "21015\n" +
                        "30202\n" +
                        "-19496\n",
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
    public void testDoubleToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(rnd_double(2) as float) from long_sequence(10)",
                "a\n" +
                        "0.8043\n" +
                        "0.0849\n" +
                        "0.0844\n" +
                        "0.6509\n" +
                        "0.7906\n" +
                        "0.2245\n" +
                        "0.3491\n" +
                        "0.7611\n" +
                        "0.4218\n" +
                        "NaN\n",
                true
        );
    }

    @Test
    public void testCharToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(rnd_char() as float) from long_sequence(10)",
                "a\n" +
                        "86.0000\n" +
                        "84.0000\n" +
                        "74.0000\n" +
                        "87.0000\n" +
                        "67.0000\n" +
                        "80.0000\n" +
                        "83.0000\n" +
                        "87.0000\n" +
                        "72.0000\n" +
                        "89.0000\n",
                true
        );
    }

    @Test
    public void testShortToFloat() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a float)",
                null,
                "insert into tab select cast(rnd_short() as float) from long_sequence(10)",
                "a\n" +
                        "-27056.0000\n" +
                        "24814.0000\n" +
                        "-11455.0000\n" +
                        "-13027.0000\n" +
                        "-21227.0000\n" +
                        "-22955.0000\n" +
                        "-1398.0000\n" +
                        "21015.0000\n" +
                        "30202.0000\n" +
                        "-19496.0000\n",
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
                        "76.0\n" +
                        "102.0\n" +
                        "27.0\n" +
                        "87.0\n" +
                        "79.0\n" +
                        "79.0\n" +
                        "122.0\n" +
                        "83.0\n" +
                        "90.0\n" +
                        "76.0\n",
                true
        );
    }

    @Test
    public void testDoubleToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(rnd_double(2) as double) from long_sequence(10)",
                "a\n" +
                        "0.8043224099968393\n" +
                        "0.08486964232560668\n" +
                        "0.0843832076262595\n" +
                        "0.6508594025855301\n" +
                        "0.7905675319675964\n" +
                        "0.22452340856088226\n" +
                        "0.3491070363730514\n" +
                        "0.7611029514995744\n" +
                        "0.4217768841969397\n" +
                        "NaN\n",
                true
        );
    }

    @Test
    public void testCharToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(rnd_char() as double) from long_sequence(10)",
                "a\n" +
                        "86.0\n" +
                        "84.0\n" +
                        "74.0\n" +
                        "87.0\n" +
                        "67.0\n" +
                        "80.0\n" +
                        "83.0\n" +
                        "87.0\n" +
                        "72.0\n" +
                        "89.0\n",
                true
        );
    }

    @Test
    public void testShortToDouble() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a double)",
                null,
                "insert into tab select cast(rnd_short() as double) from long_sequence(10)",
                "a\n" +
                        "-27056.0\n" +
                        "24814.0\n" +
                        "-11455.0\n" +
                        "-13027.0\n" +
                        "-21227.0\n" +
                        "-22955.0\n" +
                        "-1398.0\n" +
                        "21015.0\n" +
                        "30202.0\n" +
                        "-19496.0\n",
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
    public void testDoubleToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(rnd_double(2)*10000 as date) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:08.043Z\n" +
                        "1970-01-01T00:00:00.848Z\n" +
                        "1970-01-01T00:00:00.843Z\n" +
                        "1970-01-01T00:00:06.508Z\n" +
                        "1970-01-01T00:00:07.905Z\n" +
                        "1970-01-01T00:00:02.245Z\n" +
                        "1970-01-01T00:00:03.491Z\n" +
                        "1970-01-01T00:00:07.611Z\n" +
                        "1970-01-01T00:00:04.217Z\n" +
                        "\n",
                true
        );
    }

    @Test
    public void testCharToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(rnd_char() as date) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.086Z\n" +
                        "1970-01-01T00:00:00.084Z\n" +
                        "1970-01-01T00:00:00.074Z\n" +
                        "1970-01-01T00:00:00.087Z\n" +
                        "1970-01-01T00:00:00.067Z\n" +
                        "1970-01-01T00:00:00.080Z\n" +
                        "1970-01-01T00:00:00.083Z\n" +
                        "1970-01-01T00:00:00.087Z\n" +
                        "1970-01-01T00:00:00.072Z\n" +
                        "1970-01-01T00:00:00.089Z\n",
                true
        );
    }

    @Test
    public void testShortToDate() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a date)",
                null,
                "insert into tab select cast(rnd_short() as date) from long_sequence(10)",
                "a\n" +
                        "1969-12-31T23:59:32.944Z\n" +
                        "1970-01-01T00:00:24.814Z\n" +
                        "1969-12-31T23:59:48.545Z\n" +
                        "1969-12-31T23:59:46.973Z\n" +
                        "1969-12-31T23:59:38.773Z\n" +
                        "1969-12-31T23:59:37.045Z\n" +
                        "1969-12-31T23:59:58.602Z\n" +
                        "1970-01-01T00:00:21.015Z\n" +
                        "1970-01-01T00:00:30.202Z\n" +
                        "1969-12-31T23:59:40.504Z\n",
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
    public void testCharToBoolean() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(rnd_char() as boolean) from long_sequence(10)",
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
    public void testCharToBooleanTrue() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(cast(0 as char) as boolean) from long_sequence(10)",
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
    public void testShortToBoolean() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(rnd_short() as boolean) from long_sequence(10)",
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
    public void testShortToBooleanTrue() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a boolean)",
                null,
                "insert into tab select cast(cast(0 as short) as boolean) from long_sequence(10)",
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
    public void testDoubleToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(rnd_double(2)*1000 as long) from long_sequence(10)",
                "a\n" +
                        "804\n" +
                        "84\n" +
                        "84\n" +
                        "650\n" +
                        "790\n" +
                        "224\n" +
                        "349\n" +
                        "761\n" +
                        "421\n" +
                        "NaN\n",
                true
        );
    }

    @Test
    public void testCharToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(rnd_char() as long) from long_sequence(10)",
                "a\n" +
                        "86\n" +
                        "84\n" +
                        "74\n" +
                        "87\n" +
                        "67\n" +
                        "80\n" +
                        "83\n" +
                        "87\n" +
                        "72\n" +
                        "89\n",
                true
        );
    }

    @Test
    public void testShortToLong() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long)",
                null,
                "insert into tab select cast(rnd_short() as long) from long_sequence(10)",
                "a\n" +
                        "-27056\n" +
                        "24814\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-21227\n" +
                        "-22955\n" +
                        "-1398\n" +
                        "21015\n" +
                        "30202\n" +
                        "-19496\n",
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
    public void testDoubleToLong256() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long256)",
                null,
                "insert into tab select cast(rnd_double(2)*1000000 as long256) from long_sequence(10)",
                "a\n" +
                        "0x0c45e2\n" +
                        "0x014b85\n" +
                        "0x01499f\n" +
                        "0x09ee6b\n" +
                        "0x0c1027\n" +
                        "0x036d0b\n" +
                        "0x0553b3\n" +
                        "0x0b9d0e\n" +
                        "0x066f90\n" +
                        "\n",
                true
        );
    }

    @Test
    public void testShortToLong256() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long256)",
                null,
                "insert into tab select cast(rnd_short() as long256) from long_sequence(10)",
                "a\n" +
                        "0xffffffffffff9650\n" +
                        "0x60ee\n" +
                        "0xffffffffffffd341\n" +
                        "0xffffffffffffcd1d\n" +
                        "0xffffffffffffad15\n" +
                        "0xffffffffffffa655\n" +
                        "0xfffffffffffffa8a\n" +
                        "0x5217\n" +
                        "0x75fa\n" +
                        "0xffffffffffffb3d8\n",
                true
        );
    }

    @Test
    public void testCharToLong256() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a long256)",
                null,
                "insert into tab select cast(rnd_char() as long256) from long_sequence(10)",
                "a\n" +
                        "0x56\n" +
                        "0x54\n" +
                        "0x4a\n" +
                        "0x57\n" +
                        "0x43\n" +
                        "0x50\n" +
                        "0x53\n" +
                        "0x57\n" +
                        "0x48\n" +
                        "0x59\n",
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
    public void testDoubleToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "0x0f52\n" +
                        "0x016749\n" +
                        "0x01695b\n" +
                        "0x03bbfa\n" +
                        "0x042050\n" +
                        "0x05780b\n" +
                        "0x05ce6e\n" +
                        "0x0668f5\n" +
                        "0x0703d8\n" +
                        "0x0706a6\n" +
                        "0x0a9d2b\n" +
                        "0x0ad33c\n" +
                        "0x0bcca5\n" +
                        "0x0c13a7\n" +
                        "0x0ca8a2\n" +
                        "0x0d2616\n" +
                        "0x0d60a7\n" +
                        "0x10405a\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_double(2)*1090000 a from long_sequence(20))",
                null,
                true
        );
    }

    @Test
    public void testCharToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
                        "0x43\n" +
                        "0x45\n" +
                        "0x47\n" +
                        "0x48\n" +
                        "0x48\n" +
                        "0x4a\n" +
                        "0x4e\n" +
                        "0x50\n" +
                        "0x50\n" +
                        "0x52\n" +
                        "0x52\n" +
                        "0x53\n" +
                        "0x54\n" +
                        "0x56\n" +
                        "0x57\n" +
                        "0x57\n" +
                        "0x58\n" +
                        "0x58\n" +
                        "0x59\n" +
                        "0x5a\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_char() a from long_sequence(20))",
                null,
                true
        );
    }

    @Test
    public void testShortToLong256Sort() throws Exception {
        assertQuery(
                "x\n" +
                        "0xffffffffffff8059\n" +
                        "0xffffffffffff84c4\n" +
                        "0xffffffffffff9650\n" +
                        "0xffffffffffffa0f1\n" +
                        "0xffffffffffffa655\n" +
                        "0xffffffffffffad15\n" +
                        "0xffffffffffffb288\n" +
                        "0xffffffffffffb3d8\n" +
                        "0xffffffffffffc6cc\n" +
                        "0xffffffffffffcd1d\n" +
                        "0xffffffffffffd341\n" +
                        "0xffffffffffffeb14\n" +
                        "0xffffffffffffecce\n" +
                        "0xfffffffffffffa8a\n" +
                        "0x1e3b\n" +
                        "0x2d91\n" +
                        "0x5217\n" +
                        "0x5d72\n" +
                        "0x60ee\n" +
                        "0x75fa\n",
                "select cast(a as long256) x from tt order by x",
                "create table tt as (select rnd_short() a from long_sequence(20))",
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
    public void testDoubleToStr() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(rnd_double() as string) from long_sequence(10)",
                "a\n" +
                        "0.6607777894187332\n" +
                        "0.2246301342497259\n" +
                        "0.08486964232560668\n" +
                        "0.299199045961845\n" +
                        "0.20447441837877756\n" +
                        "0.6508594025855301\n" +
                        "0.8423410920883345\n" +
                        "0.9856290845874263\n" +
                        "0.22452340856088226\n" +
                        "0.5093827001617407\n",
                true
        );
    }

    @Test
    public void testCharToStr() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(rnd_char() as string) from long_sequence(10)",
                "a\n" +
                        "V\n" +
                        "T\n" +
                        "J\n" +
                        "W\n" +
                        "C\n" +
                        "P\n" +
                        "S\n" +
                        "W\n" +
                        "H\n" +
                        "Y\n",
                true
        );
    }

    @Test
    public void testShortToStr() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(rnd_short() as string) from long_sequence(10)",
                "a\n" +
                        "-27056\n" +
                        "24814\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-21227\n" +
                        "-22955\n" +
                        "-1398\n" +
                        "21015\n" +
                        "30202\n" +
                        "-19496\n",
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
    public void testCharToStrConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast('A' as string) from long_sequence(10)",
                "a\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n" +
                        "A\n",
                true
        );
    }

    @Test
    public void testShortToStrConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(cast(10 as short) as string) from long_sequence(10)",
                "a\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n" +
                        "10\n",
                true
        );
    }

    @Test
    public void testDoubleToStrConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a string)",
                null,
                "insert into tab select cast(1.34 as string) from long_sequence(10)",
                "a\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n" +
                        "1.34\n",
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
    public void testCharToStrSort() throws Exception {
        assertQuery(
                "x\n" +
                        "C\n" +
                        "E\n" +
                        "G\n" +
                        "H\n" +
                        "H\n" +
                        "J\n" +
                        "N\n" +
                        "P\n" +
                        "P\n" +
                        "R\n" +
                        "R\n" +
                        "S\n" +
                        "T\n" +
                        "V\n" +
                        "W\n" +
                        "W\n" +
                        "X\n" +
                        "X\n" +
                        "Y\n" +
                        "Z\n",
                "select cast(a as string) x from tt order by x",
                "create table tt as (select rnd_char() a from long_sequence(20))",
                null,
                true
        );
    }

    @Test
    public void testShortToStrSort() throws Exception {
        assertQuery(
                "x\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-1398\n" +
                        "-14644\n" +
                        "-19496\n" +
                        "-19832\n" +
                        "-21227\n" +
                        "-22955\n" +
                        "-24335\n" +
                        "-27056\n" +
                        "-31548\n" +
                        "-32679\n" +
                        "-4914\n" +
                        "-5356\n" +
                        "11665\n" +
                        "21015\n" +
                        "23922\n" +
                        "24814\n" +
                        "30202\n" +
                        "7739\n",
                "select cast(a as string) x from tt order by x",
                "create table tt as (select rnd_short() a from long_sequence(20))",
                null,
                true
        );
    }

    @Test
    public void testDoubleToStrSort() throws Exception {
        assertQuery(
                "x\n" +
                        "\n" +
                        "\n" +
                        "0.0035983672154330515\n" +
                        "0.0843832076262595\n" +
                        "0.08486964232560668\n" +
                        "0.22452340856088226\n" +
                        "0.24808812376657652\n" +
                        "0.3288176907679504\n" +
                        "0.3491070363730514\n" +
                        "0.38539947865244994\n" +
                        "0.4217768841969397\n" +
                        "0.4224356661645131\n" +
                        "0.6381607531178513\n" +
                        "0.6508594025855301\n" +
                        "0.7094360487171202\n" +
                        "0.7261136209823622\n" +
                        "0.7611029514995744\n" +
                        "0.7905675319675964\n" +
                        "0.8043224099968393\n" +
                        "0.9771103146051203\n",
                "select cast(a as string) x from tt order by x",
                "create table tt as (select rnd_double(2) a from long_sequence(20))",
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
                        "19.0\n" +
                        "72.0\n" +
                        "90.0\n" +
                        "NaN\n" +
                        "7.0\n" +
                        "17.0\n" +
                        "65.0\n" +
                        "32.0\n" +
                        "67.0\n" +
                        "106.0\n",
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
                        "67.0\n" +
                        "126.0\n" +
                        "124.0\n" +
                        "NaN\n" +
                        "57.0\n" +
                        "33.0\n" +
                        "85.0\n" +
                        "40.0\n" +
                        "111.0\n" +
                        "112.0\n",
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
    public void testDoubleToTimestamp() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp)",
                null,
                "insert into tab select cast(rnd_double(2)*100000000 as timestamp) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:01:20.432240Z\n" +
                        "1970-01-01T00:00:08.486964Z\n" +
                        "1970-01-01T00:00:08.438320Z\n" +
                        "1970-01-01T00:01:05.085940Z\n" +
                        "1970-01-01T00:01:19.056753Z\n" +
                        "1970-01-01T00:00:22.452340Z\n" +
                        "1970-01-01T00:00:34.910703Z\n" +
                        "1970-01-01T00:01:16.110295Z\n" +
                        "1970-01-01T00:00:42.177688Z\n" +
                        "\n",
                true
        );
    }

    @Test
    public void testCharToTimestamp() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp)",
                null,
                "insert into tab select cast(rnd_char() as timestamp) from long_sequence(10)",
                "a\n" +
                        "1970-01-01T00:00:00.000086Z\n" +
                        "1970-01-01T00:00:00.000084Z\n" +
                        "1970-01-01T00:00:00.000074Z\n" +
                        "1970-01-01T00:00:00.000087Z\n" +
                        "1970-01-01T00:00:00.000067Z\n" +
                        "1970-01-01T00:00:00.000080Z\n" +
                        "1970-01-01T00:00:00.000083Z\n" +
                        "1970-01-01T00:00:00.000087Z\n" +
                        "1970-01-01T00:00:00.000072Z\n" +
                        "1970-01-01T00:00:00.000089Z\n",
                true
        );
    }

    @Test
    public void testShortToTimestamp() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a timestamp)",
                null,
                "insert into tab select cast(rnd_short() as timestamp) from long_sequence(10)",
                "a\n" +
                        "1969-12-31T23:59:59.972944Z\n" +
                        "1970-01-01T00:00:00.024814Z\n" +
                        "1969-12-31T23:59:59.988545Z\n" +
                        "1969-12-31T23:59:59.986973Z\n" +
                        "1969-12-31T23:59:59.978773Z\n" +
                        "1969-12-31T23:59:59.977045Z\n" +
                        "1969-12-31T23:59:59.998602Z\n" +
                        "1970-01-01T00:00:00.021015Z\n" +
                        "1970-01-01T00:00:00.030202Z\n" +
                        "1969-12-31T23:59:59.980504Z\n",
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
    public void testCharToSymbol() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(rnd_char() as symbol) from long_sequence(10)",
                "a\n" +
                        "V\n" +
                        "T\n" +
                        "J\n" +
                        "W\n" +
                        "C\n" +
                        "P\n" +
                        "S\n" +
                        "W\n" +
                        "H\n" +
                        "Y\n",
                true
        );
    }

    @Test
    public void testShortToSymbol() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(rnd_short() as symbol) from long_sequence(10)",
                "a\n" +
                        "-27056\n" +
                        "24814\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-21227\n" +
                        "-22955\n" +
                        "-1398\n" +
                        "21015\n" +
                        "30202\n" +
                        "-19496\n",
                true
        );
    }

    @Test
    public void testDoubleToSymbol() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(rnd_double(2) as symbol) from long_sequence(10)",
                "a\n" +
                        "0.8043224099968393\n" +
                        "0.08486964232560668\n" +
                        "0.0843832076262595\n" +
                        "0.6508594025855301\n" +
                        "0.7905675319675964\n" +
                        "0.22452340856088226\n" +
                        "0.3491070363730514\n" +
                        "0.7611029514995744\n" +
                        "0.4217768841969397\n" +
                        "\n",
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
    public void testCharToSymbolConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast('X' as symbol) from long_sequence(10)",
                "a\n" +
                        "X\n" +
                        "X\n" +
                        "X\n" +
                        "X\n" +
                        "X\n" +
                        "X\n" +
                        "X\n" +
                        "X\n" +
                        "X\n" +
                        "X\n",
                true
        );
    }

    @Test
    public void testShortToSymbolConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(cast(99 as short) as symbol) from long_sequence(10)",
                "a\n" +
                        "99\n" +
                        "99\n" +
                        "99\n" +
                        "99\n" +
                        "99\n" +
                        "99\n" +
                        "99\n" +
                        "99\n" +
                        "99\n" +
                        "99\n",
                true
        );
    }

    @Test
    public void testDoubleToSymbolConst() throws Exception {
        assertQuery(
                "a\n",
                "select a from tab",
                "create table tab (a symbol)",
                null,
                "insert into tab select cast(1.5 as symbol) from long_sequence(10)",
                "a\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n" +
                        "1.5\n",
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
    public void testCharToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "V\tV\n" +
                        "T\tT\n" +
                        "J\tJ\n" +
                        "W\tW\n" +
                        "C\tC\n" +
                        "P\tP\n" +
                        "S\tS\n" +
                        "W\tW\n" +
                        "H\tH\n" +
                        "Y\tY\n" +
                        "R\tR\n" +
                        "X\tX\n" +
                        "P\tP\n" +
                        "E\tE\n" +
                        "H\tH\n" +
                        "N\tN\n" +
                        "R\tR\n" +
                        "X\tX\n" +
                        "G\tG\n" +
                        "Z\tZ\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_char() a from long_sequence(20))",
                null,
                true
        );
    }

    @Test
    public void testShortToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "-27056\t-27056\n" +
                        "24814\t24814\n" +
                        "-11455\t-11455\n" +
                        "-13027\t-13027\n" +
                        "-21227\t-21227\n" +
                        "-22955\t-22955\n" +
                        "-1398\t-1398\n" +
                        "21015\t21015\n" +
                        "30202\t30202\n" +
                        "-19496\t-19496\n" +
                        "-14644\t-14644\n" +
                        "-5356\t-5356\n" +
                        "-4914\t-4914\n" +
                        "-24335\t-24335\n" +
                        "-32679\t-32679\n" +
                        "-19832\t-19832\n" +
                        "-31548\t-31548\n" +
                        "11665\t11665\n" +
                        "7739\t7739\n" +
                        "23922\t23922\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_short() a from long_sequence(20))",
                null,
                true
        );
    }

    @Test
    public void testDoubleToSymbolIndexBehaviour() throws Exception {
        assertQuery(
                "b\ta\n" +
                        "0.8043224099968393\t0.8043224099968393\n" +
                        "0.08486964232560668\t0.08486964232560668\n" +
                        "0.0843832076262595\t0.0843832076262595\n" +
                        "0.6508594025855301\t0.6508594025855301\n" +
                        "0.7905675319675964\t0.7905675319675964\n" +
                        "0.22452340856088226\t0.22452340856088226\n" +
                        "0.3491070363730514\t0.3491070363730514\n" +
                        "0.7611029514995744\t0.7611029514995744\n" +
                        "0.4217768841969397\t0.4217768841969397\n" +
                        "\tNaN\n" +
                        "0.7261136209823622\t0.7261136209823622\n" +
                        "0.4224356661645131\t0.4224356661645131\n" +
                        "0.7094360487171202\t0.7094360487171202\n" +
                        "0.38539947865244994\t0.38539947865244994\n" +
                        "0.0035983672154330515\t0.0035983672154330515\n" +
                        "0.3288176907679504\t0.3288176907679504\n" +
                        "\tNaN\n" +
                        "0.9771103146051203\t0.9771103146051203\n" +
                        "0.24808812376657652\t0.24808812376657652\n" +
                        "0.6381607531178513\t0.6381607531178513\n",
                "select cast(a as symbol) b, a from tab",
                "create table tab as (select rnd_double(2) a from long_sequence(20))",
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