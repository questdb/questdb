/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import org.junit.Test;

public class UnionAllCastTest extends AbstractGriffinTest {
    @Test
    public void testDateLong() throws Exception {
        compile("create table y as (select rnd_long() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "1970-01-01T02:07:40.373Z\n" +
                        "1970-01-01T00:18:02.998Z\n" +
                        "1970-01-01T02:14:51.881Z\n" +
                        "1970-01-01T00:14:24.006Z\n" +
                        "1970-01-01T00:10:02.536Z\n" +
                        "150577-04-03T14:54:03.856Z\n" +
                        "151857-08-13T01:43:12.366Z\n" +
                        "245479925-08-06T22:45:29.025Z\n" +
                        "-220105521-10-27T16:44:58.525Z\n" +
                        "261756925-02-22T01:56:27.029Z\n",
                "x union all y",
                "create table x as (select rnd_date() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testDoubleLong() throws Exception {
        compile("create table y as (select rnd_long() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "0.2845577791213847\n" +
                        "0.0843832076262595\n" +
                        "0.9344604857394011\n" +
                        "0.13123360041292131\n" +
                        "0.7905675319675964\n" +
                        "4.689592037643856E15\n" +
                        "4.729996258992366E15\n" +
                        "7.7465360618163292E18\n" +
                        "-6.9459215023845018E18\n" +
                        "8.2601885552325868E18\n",
                "x union all y",
                "create table x as (select rnd_double() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testFloatInt() throws Exception {
        compile("create table y as (select rnd_float() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "-9.4826336E8\n" +
                        "1.32644723E9\n" +
                        "5.9285965E8\n" +
                        "1.86872371E9\n" +
                        "-8.4753107E8\n" +
                        "0.6608\n" +
                        "0.8043\n" +
                        "0.2246\n" +
                        "0.1297\n" +
                        "0.0849\n",
                "x union all y",
                "create table x as (select rnd_int() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testFloatLong() throws Exception {
        compile("create table y as (select rnd_long() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "0.2846\n" +
                        "0.2992\n" +
                        "0.0844\n" +
                        "0.2045\n" +
                        "0.9345\n" +
                        "4.6895921E15\n" +
                        "4.7299965E15\n" +
                        "7.7465361E18\n" +
                        "-6.9459217E18\n" +
                        "8.2601883E18\n",
                "x union all y",
                "create table x as (select rnd_float() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testFloatShort() throws Exception {
        compile("create table y as (select rnd_float() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "-22955.0000\n" +
                        "-1398.0000\n" +
                        "21015.0000\n" +
                        "30202.0000\n" +
                        "-19496.0000\n" +
                        "0.6608\n" +
                        "0.8043\n" +
                        "0.2246\n" +
                        "0.1297\n" +
                        "0.0849\n",
                "x union all y",
                "create table x as (select rnd_short() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testGeoByteStr() throws Exception {
        // long + geohash overlap via string type
        compile("create table y as (select rnd_long() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "2\n" +
                        "c\n" +
                        "c\n" +
                        "f\n" +
                        "5\n" +
                        "4689592037643856\n" +
                        "4729996258992366\n" +
                        "7746536061816329025\n" +
                        "-6945921502384501475\n" +
                        "8260188555232587029\n",
                "x union all y",
                "create table x as (select rnd_geohash(5) a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testGeoIntStr() throws Exception {
        // long + geohash overlap via string type
        compile("create table y as (select rnd_long() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "29je\n" +
                        "cjcs\n" +
                        "c931\n" +
                        "fu3r\n" +
                        "5ewm\n" +
                        "4689592037643856\n" +
                        "4729996258992366\n" +
                        "7746536061816329025\n" +
                        "-6945921502384501475\n" +
                        "8260188555232587029\n",
                "x union all y",
                "create table x as (select rnd_geohash(20) a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testGeoIntStrBits() throws Exception {
        // long + geohash overlap via string type
        compile("create table y as (select rnd_long() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "000100100110001011010\n" +
                        "010111000101011110000\n" +
                        "010110100100011000011\n" +
                        "011101101000011101110\n" +
                        "001010110111100100110\n" +
                        "4689592037643856\n" +
                        "4729996258992366\n" +
                        "7746536061816329025\n" +
                        "-6945921502384501475\n" +
                        "8260188555232587029\n",
                "x union all y",
                "create table x as (select rnd_geohash(21) a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testGeoLongStr() throws Exception {
        // long + geohash overlap via string type
        compile("create table y as (select rnd_long() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "29je7k2s\n" +
                        "cjcsgh6h\n" +
                        "c931t136\n" +
                        "fu3r7chm\n" +
                        "5ewm40wx\n" +
                        "4689592037643856\n" +
                        "4729996258992366\n" +
                        "7746536061816329025\n" +
                        "-6945921502384501475\n" +
                        "8260188555232587029\n",
                "x union all y",
                "create table x as (select rnd_geohash(40) a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testGeoLongStrBits() throws Exception {
        // long + geohash overlap via string type
        compile("create table y as (select rnd_long() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "000100100110001011010011110010000101100000\n" +
                        "010111000101011110000111110000001101000001\n" +
                        "010110100100011000011100100001000110011010\n" +
                        "011101101000011101110011101011100001001101\n" +
                        "001010110111100100110010000000111001110110\n" +
                        "4689592037643856\n" +
                        "4729996258992366\n" +
                        "7746536061816329025\n" +
                        "-6945921502384501475\n" +
                        "8260188555232587029\n",
                "x union all y",
                "create table x as (select rnd_geohash(42) a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testGeoShortStr() throws Exception {
        // long + geohash overlap via string type
        compile("create table y as (select rnd_long() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "29\n" +
                        "cj\n" +
                        "c9\n" +
                        "fu\n" +
                        "5e\n" +
                        "4689592037643856\n" +
                        "4729996258992366\n" +
                        "7746536061816329025\n" +
                        "-6945921502384501475\n" +
                        "8260188555232587029\n",
                "x union all y",
                "create table x as (select rnd_geohash(10) a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testGeoShortStrBits() throws Exception {
        // long + geohash overlap via string type
        compile("create table y as (select rnd_long() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "000100100110\n" +
                        "010111000101\n" +
                        "010110100100\n" +
                        "011101101000\n" +
                        "001010110111\n" +
                        "4689592037643856\n" +
                        "4729996258992366\n" +
                        "7746536061816329025\n" +
                        "-6945921502384501475\n" +
                        "8260188555232587029\n",
                "x union all y",
                "create table x as (select rnd_geohash(12) a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testLongBool() throws Exception {
        // this is cast to STRING, both columns
        compile("create table y as (select rnd_long() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "4689592037643856\n" +
                        "4729996258992366\n" +
                        "7746536061816329025\n" +
                        "-6945921502384501475\n" +
                        "8260188555232587029\n",
                "x union all y",
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testLongByte() throws Exception {
        compile("create table y as (select rnd_long() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "79\n" +
                        "122\n" +
                        "83\n" +
                        "90\n" +
                        "76\n" +
                        "4689592037643856\n" +
                        "4729996258992366\n" +
                        "7746536061816329025\n" +
                        "-6945921502384501475\n" +
                        "8260188555232587029\n",
                "x union all y",
                "create table x as (select rnd_byte() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testLongChar() throws Exception {
        compile("create table y as (select rnd_long() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "80\n" +
                        "83\n" +
                        "87\n" +
                        "72\n" +
                        "89\n" +
                        "4689592037643856\n" +
                        "4729996258992366\n" +
                        "7746536061816329025\n" +
                        "-6945921502384501475\n" +
                        "8260188555232587029\n",
                "x union all y",
                "create table x as (select rnd_char() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testLongInt() throws Exception {
        compile("create table y as (select rnd_long() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "-948263339\n" +
                        "1326447242\n" +
                        "592859671\n" +
                        "1868723706\n" +
                        "-847531048\n" +
                        "4689592037643856\n" +
                        "4729996258992366\n" +
                        "7746536061816329025\n" +
                        "-6945921502384501475\n" +
                        "8260188555232587029\n",
                "x union all y",
                "create table x as (select rnd_int() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testLongLong256() throws Exception {
        compile("create table y as (select rnd_long() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\n" +
                        "0x6846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cce8beef38cd7bb3d8\n" +
                        "0x9fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059980eca62a219a0f1\n" +
                        "0x6e60a01a5b3ea0db4b0f595f143e5d722f1a8266e7921e3b716de3d25dcc2d91\n" +
                        "0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\n" +
                        "4689592037643856\n" +
                        "4729996258992366\n" +
                        "7746536061816329025\n" +
                        "-6945921502384501475\n" +
                        "8260188555232587029\n",
                "x union all y",
                "create table x as (select rnd_long256() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testLongShort() throws Exception {
        compile("create table y as (select rnd_long() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "-22955\n" +
                        "-1398\n" +
                        "21015\n" +
                        "30202\n" +
                        "-19496\n" +
                        "4689592037643856\n" +
                        "4729996258992366\n" +
                        "7746536061816329025\n" +
                        "-6945921502384501475\n" +
                        "8260188555232587029\n",
                "x union all y",
                "create table x as (select rnd_short() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testLongSymbol() throws Exception {
        compile("create table y as (select rnd_long() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "bbb\n" +
                        "aaa\n" +
                        "bbb\n" +
                        "aaa\n" +
                        "aaa\n" +
                        "4689592037643856\n" +
                        "4729996258992366\n" +
                        "7746536061816329025\n" +
                        "-6945921502384501475\n" +
                        "8260188555232587029\n",
                "x union all y",
                "create table x as (select rnd_symbol('aaa', 'bbb') a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testTimestampLong() throws Exception {
        compile("create table y as (select rnd_long() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "284661-01-03T01:19:47.660373Z\n" +
                        "-239240-04-12T14:04:18.917002Z\n" +
                        "-167698-05-16T15:46:11.908119Z\n" +
                        "-82114-11-17T05:49:39.135994Z\n" +
                        "-51129-02-11T06:38:29.397464Z\n" +
                        "2118-08-10T16:27:17.643856Z\n" +
                        "2119-11-21T07:50:58.992366Z\n" +
                        "247447-12-16T04:43:36.329025Z\n" +
                        "-218138-07-06T00:26:55.498525Z\n" +
                        "263724-12-15T04:53:52.587029Z\n",
                "x union all y",
                "create table x as (select cast(rnd_long() as timestamp) a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }
}
