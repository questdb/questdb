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

import org.junit.Ignore;
import org.junit.Test;

public class UnionAllCastTest extends AbstractGriffinTest {
    @Test
    public void testBoolBool() throws Exception {
        // we include byte <-> bool cast to make sure
        // bool <-> bool cast it not thrown away as redundant
        compile("create table y as (select rnd_byte() b, rnd_boolean() c from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\tc\n" +
                        "false\tfalse\n" +
                        "false\ttrue\n" +
                        "true\ttrue\n" +
                        "true\tfalse\n" +
                        "false\tfalse\n" +
                        "76\tfalse\n" +
                        "27\ttrue\n" +
                        "79\tfalse\n" +
                        "122\ttrue\n" +
                        "90\ttrue\n",
                "x union all y",
                "create table x as (select rnd_boolean() a, rnd_boolean() c from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testByteBool() throws Exception {
        // this is cast to STRING, both columns
        compile("create table y as (select rnd_byte() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "76\n" +
                        "102\n" +
                        "27\n" +
                        "87\n" +
                        "79\n",
                "x union all y",
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testByteByte() throws Exception {
        // we include byte <-> bool cast to make sure
        // byte <-> byte cast it not thrown away as redundant
        compile("create table y as (select rnd_byte() b, rnd_byte() c from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\tc\n" +
                        "false\t84\n" +
                        "false\t55\n" +
                        "true\t88\n" +
                        "true\t21\n" +
                        "false\t74\n" +
                        "76\t102\n" +
                        "27\t87\n" +
                        "79\t79\n" +
                        "122\t83\n" +
                        "90\t76\n",
                "x union all y",
                "create table x as (select rnd_boolean() a, rnd_byte() c from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testCharBool() throws Exception {
        // this is cast to STRING, both columns
        compile("create table y as (select rnd_char() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "V\n" +
                        "T\n" +
                        "J\n" +
                        "W\n" +
                        "C\n",
                "x union all y",
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testCharByte() throws Exception {
        compile("create table y as (select rnd_char() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "O\n" +
                        "z\n" +
                        "S\n" +
                        "Z\n" +
                        "L\n" +
                        "V\n" +
                        "T\n" +
                        "J\n" +
                        "W\n" +
                        "C\n",
                "x union all y",
                "create table x as (select rnd_byte() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testCharShort() throws Exception {
        compile("create table y as (select rnd_char() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "-22955\n" +
                        "-1398\n" +
                        "21015\n" +
                        "30202\n" +
                        "-19496\n" +
                        "86\n" +
                        "84\n" +
                        "74\n" +
                        "87\n" +
                        "67\n",
                "x union all y",
                "create table x as (select rnd_short() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testDateBool() throws Exception {
        // this is cast to STRING, both columns
        compile("create table y as (select rnd_date() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "1970-01-01T02:07:23.856Z\n" +
                        "1970-01-01T02:29:52.366Z\n" +
                        "1970-01-01T01:45:29.025Z\n" +
                        "1970-01-01T01:15:01.475Z\n" +
                        "1970-01-01T00:43:07.029Z\n",
                "x union all y",
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testDateByte() throws Exception {
        compile("create table y as (select rnd_byte() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "1970-01-01T02:07:40.373Z\n" +
                        "1970-01-01T00:18:02.998Z\n" +
                        "1970-01-01T02:14:51.881Z\n" +
                        "1970-01-01T00:14:24.006Z\n" +
                        "1970-01-01T00:10:02.536Z\n" +
                        "1970-01-01T00:00:00.076Z\n" +
                        "1970-01-01T00:00:00.102Z\n" +
                        "1970-01-01T00:00:00.027Z\n" +
                        "1970-01-01T00:00:00.087Z\n" +
                        "1970-01-01T00:00:00.079Z\n",
                "x union all y",
                "create table x as (select rnd_date() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testDateChar() throws Exception {
        compile("create table y as (select rnd_char() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "1970-01-01T02:07:40.373Z\n" +
                        "1970-01-01T00:18:02.998Z\n" +
                        "1970-01-01T02:14:51.881Z\n" +
                        "1970-01-01T00:14:24.006Z\n" +
                        "1970-01-01T00:10:02.536Z\n" +
                        "1970-01-01T00:00:00.086Z\n" +
                        "1970-01-01T00:00:00.084Z\n" +
                        "1970-01-01T00:00:00.074Z\n" +
                        "1970-01-01T00:00:00.087Z\n" +
                        "1970-01-01T00:00:00.067Z\n",
                "x union all y",
                "create table x as (select rnd_date() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testDateInt() throws Exception {
        compile("create table y as (select rnd_int() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "1970-01-01T02:07:40.373Z\n" +
                        "1970-01-01T00:18:02.998Z\n" +
                        "1970-01-01T02:14:51.881Z\n" +
                        "1970-01-01T00:14:24.006Z\n" +
                        "1970-01-01T00:10:02.536Z\n" +
                        "1969-12-18T16:58:40.080Z\n" +
                        "1970-01-04T15:38:35.118Z\n" +
                        "1970-01-18T22:13:20.833Z\n" +
                        "1969-12-23T13:51:15.229Z\n" +
                        "1970-01-01T20:26:15.701Z\n",
                "x union all y",
                "create table x as (select rnd_date() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

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
    public void testDateShort() throws Exception {
        compile("create table y as (select rnd_short() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "1970-01-01T02:07:40.373Z\n" +
                        "1970-01-01T00:18:02.998Z\n" +
                        "1970-01-01T02:14:51.881Z\n" +
                        "1970-01-01T00:14:24.006Z\n" +
                        "1970-01-01T00:10:02.536Z\n" +
                        "1969-12-31T23:59:32.944Z\n" +
                        "1970-01-01T00:00:24.814Z\n" +
                        "1969-12-31T23:59:48.545Z\n" +
                        "1969-12-31T23:59:46.973Z\n" +
                        "1969-12-31T23:59:38.773Z\n",
                "x union all y",
                "create table x as (select rnd_date() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testDateTimestamp() throws Exception {
        compile("create table y as (select rnd_timestamp(0, 100000, 1) b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\ttypeOf\n" +
                        "1970-01-01T00:14:24.006000Z\tTIMESTAMP\n" +
                        "1970-01-01T00:10:02.536000Z\tTIMESTAMP\n" +
                        "1970-01-01T00:04:41.932000Z\tTIMESTAMP\n" +
                        "1970-01-01T00:01:52.276000Z\tTIMESTAMP\n" +
                        "1970-01-01T00:32:57.934000Z\tTIMESTAMP\n" +
                        "1970-01-01T00:00:00.002771Z\tTIMESTAMP\n" +
                        "\tTIMESTAMP\n" +
                        "1970-01-01T00:00:00.045299Z\tTIMESTAMP\n" +
                        "1970-01-01T00:00:00.078334Z\tTIMESTAMP\n" +
                        "\tTIMESTAMP\n",
                "select a, typeOf(a) from (x union all y)",
                "create table x as (select rnd_date() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testDoubleBool() throws Exception {
        // this is cast to STRING, both columns
        compile("create table y as (select rnd_double() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "0.6607777894187332\n" +
                        "0.2246301342497259\n" +
                        "0.08486964232560668\n" +
                        "0.299199045961845\n" +
                        "0.20447441837877756\n",
                "x union all y",
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testDoubleByte() throws Exception {
        compile("create table y as (select rnd_byte() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "0.2845577791213847\n" +
                        "0.0843832076262595\n" +
                        "0.9344604857394011\n" +
                        "0.13123360041292131\n" +
                        "0.7905675319675964\n" +
                        "76.0\n" +
                        "102.0\n" +
                        "27.0\n" +
                        "87.0\n" +
                        "79.0\n",
                "x union all y",
                "create table x as (select rnd_double() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testDoubleChar() throws Exception {
        compile("create table y as (select rnd_char() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "0.2845577791213847\n" +
                        "0.0843832076262595\n" +
                        "0.9344604857394011\n" +
                        "0.13123360041292131\n" +
                        "0.7905675319675964\n" +
                        "86.0\n" +
                        "84.0\n" +
                        "74.0\n" +
                        "87.0\n" +
                        "67.0\n",
                "x union all y",
                "create table x as (select rnd_double() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testDoubleDate() throws Exception {
        compile("create table y as (select rnd_date() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "0.2845577791213847\n" +
                        "0.0843832076262595\n" +
                        "0.9344604857394011\n" +
                        "0.13123360041292131\n" +
                        "0.7905675319675964\n" +
                        "7643856.0\n" +
                        "8992366.0\n" +
                        "6329025.0\n" +
                        "4501475.0\n" +
                        "2587029.0\n",
                "x union all y",
                "create table x as (select rnd_double() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testDoubleInt() throws Exception {
        compile("create table y as (select rnd_int() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "0.2845577791213847\n" +
                        "0.0843832076262595\n" +
                        "0.9344604857394011\n" +
                        "0.13123360041292131\n" +
                        "0.7905675319675964\n" +
                        "-1.14847992E9\n" +
                        "3.15515118E8\n" +
                        "1.548800833E9\n" +
                        "-7.27724771E8\n" +
                        "7.3575701E7\n",
                "x union all y",
                "create table x as (select rnd_double() a from long_sequence(5))",
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
    public void testDoubleShort() throws Exception {
        compile("create table y as (select rnd_short() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "0.2845577791213847\n" +
                        "0.0843832076262595\n" +
                        "0.9344604857394011\n" +
                        "0.13123360041292131\n" +
                        "0.7905675319675964\n" +
                        "-27056.0\n" +
                        "24814.0\n" +
                        "-11455.0\n" +
                        "-13027.0\n" +
                        "-21227.0\n",
                "x union all y",
                "create table x as (select rnd_double() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testDoubleTimestamp() throws Exception {
        compile("create table y as (select cast(rnd_date() as timestamp) b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "0.2845577791213847\n" +
                        "0.0843832076262595\n" +
                        "0.9344604857394011\n" +
                        "0.13123360041292131\n" +
                        "0.7905675319675964\n" +
                        "7.643856E9\n" +
                        "8.992366E9\n" +
                        "6.329025E9\n" +
                        "4.501475E9\n" +
                        "2.587029E9\n",
                "x union all y",
                "create table x as (select rnd_double() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testFloatBool() throws Exception {
        // this is cast to STRING, both columns
        compile("create table y as (select rnd_float() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "0.6608\n" +
                        "0.8043\n" +
                        "0.2246\n" +
                        "0.1297\n" +
                        "0.0849\n",
                "x union all y",
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testFloatByte() throws Exception {
        compile("create table y as (select rnd_float() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "79.0000\n" +
                        "122.0000\n" +
                        "83.0000\n" +
                        "90.0000\n" +
                        "76.0000\n" +
                        "0.6608\n" +
                        "0.8043\n" +
                        "0.2246\n" +
                        "0.1297\n" +
                        "0.0849\n",
                "x union all y",
                "create table x as (select rnd_byte() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testFloatChar() throws Exception {
        compile("create table y as (select rnd_float() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "80.0000\n" +
                        "83.0000\n" +
                        "87.0000\n" +
                        "72.0000\n" +
                        "89.0000\n" +
                        "0.6608\n" +
                        "0.8043\n" +
                        "0.2246\n" +
                        "0.1297\n" +
                        "0.0849\n",
                "x union all y",
                "create table x as (select rnd_char() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testFloatDate() throws Exception {
        compile("create table y as (select rnd_float() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "7660373.0\n" +
                        "1082998.0\n" +
                        "8091881.0\n" +
                        "864006.0\n" +
                        "602536.0\n" +
                        "0.6608\n" +
                        "0.8043\n" +
                        "0.2246\n" +
                        "0.1297\n" +
                        "0.0849\n",
                "x union all y",
                "create table x as (select rnd_date() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testFloatDouble() throws Exception {
        compile("create table y as (select rnd_float() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "0.2845577791213847\n" +
                        "0.0843832076262595\n" +
                        "0.9344604857394011\n" +
                        "0.13123360041292131\n" +
                        "0.7905675319675964\n" +
                        "0.660777747631073\n" +
                        "0.804322361946106\n" +
                        "0.22463011741638184\n" +
                        "0.12966656684875488\n" +
                        "0.0848696231842041\n",
                "x union all y",
                "create table x as (select rnd_double() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testFloatGeoHash() throws Exception {
        compile("create table y as (select rnd_float() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "0001001\n" +
                        "0101110\n" +
                        "0101101\n" +
                        "0111011\n" +
                        "0010101\n" +
                        "0.6608\n" +
                        "0.8043\n" +
                        "0.2246\n" +
                        "0.1297\n" +
                        "0.0849\n",
                "x union all y",
                "create table x as (select rnd_geohash(7) a from long_sequence(5))",
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
    public void testGeoLongGeoShort() throws Exception {
        // long + geohash overlap via string type
        compile("create table y as (select rnd_geohash(10) b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "wh4b6vnt\n" +
                        "s2z2fyds\n" +
                        "1cjjwk6r\n" +
                        "mmt89425\n" +
                        "71ftmpy5\n" +
                        "9v\n" +
                        "46\n" +
                        "jn\n" +
                        "zf\n" +
                        "hp\n",
                "x union all y",
                "create table x as (select rnd_geohash(40) a from long_sequence(5))",
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
    public void testGeoShortGeoShort() throws Exception {
        // long + geohash overlap via string type
        compile("create table y as (select rnd_geohash(10) b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "111001000000\n" +
                        "110000001011\n" +
                        "000010101110\n" +
                        "100111001111\n" +
                        "001110000101\n" +
                        "9v\n" +
                        "46\n" +
                        "jn\n" +
                        "zf\n" +
                        "hp\n",
                "x union all y",
                "create table x as (select rnd_geohash(12) a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testGeoShortGeoShortExact() throws Exception {
        // long + geohash overlap via string type
        compile("create table y as (select rnd_geohash(12) b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\ttypeOf\n" +
                        "111001000000\tGEOHASH(12b)\n" +
                        "110000001011\tGEOHASH(12b)\n" +
                        "000010101110\tGEOHASH(12b)\n" +
                        "100111001111\tGEOHASH(12b)\n" +
                        "001110000101\tGEOHASH(12b)\n" +
                        "010011101100\tGEOHASH(12b)\n" +
                        "001000011011\tGEOHASH(12b)\n" +
                        "100011010011\tGEOHASH(12b)\n" +
                        "111110111011\tGEOHASH(12b)\n" +
                        "100001010100\tGEOHASH(12b)\n",
                "select a, typeOf(a) from (x union all y)",
                "create table x as (select rnd_geohash(12) a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testGeoShortGeoByteExact() throws Exception {
        // long + geohash overlap via string type
        compile("create table y as (select rnd_geohash(5) b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\ttypeOf\n" +
                        "w\tGEOHASH(1c)\n" +
                        "s\tGEOHASH(1c)\n" +
                        "1\tGEOHASH(1c)\n" +
                        "m\tGEOHASH(1c)\n" +
                        "7\tGEOHASH(1c)\n" +
                        "9\tGEOHASH(1c)\n" +
                        "4\tGEOHASH(1c)\n" +
                        "j\tGEOHASH(1c)\n" +
                        "z\tGEOHASH(1c)\n" +
                        "h\tGEOHASH(1c)\n",
                "select a, typeOf(a) from (x union all y)",
                "create table x as (select rnd_geohash(5) a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testGeoShortGeoIntExact() throws Exception {
        // long + geohash overlap via string type
        compile("create table y as (select rnd_geohash(30) b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\ttypeOf\n" +
                        "wh4b6v\tGEOHASH(6c)\n" +
                        "s2z2fy\tGEOHASH(6c)\n" +
                        "1cjjwk\tGEOHASH(6c)\n" +
                        "mmt894\tGEOHASH(6c)\n" +
                        "71ftmp\tGEOHASH(6c)\n" +
                        "9v1s8h\tGEOHASH(6c)\n" +
                        "46swgj\tGEOHASH(6c)\n" +
                        "jnw97u\tGEOHASH(6c)\n" +
                        "zfuqd3\tGEOHASH(6c)\n" +
                        "hp4muv\tGEOHASH(6c)\n",
                "select a, typeOf(a) from (x union all y)",
                "create table x as (select rnd_geohash(30) a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testGeoShortGeoLongExact() throws Exception {
        // long + geohash overlap via string type
        compile("create table y as (select rnd_geohash(35) b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\ttypeOf\n" +
                        "wh4b6vn\tGEOHASH(7c)\n" +
                        "s2z2fyd\tGEOHASH(7c)\n" +
                        "1cjjwk6\tGEOHASH(7c)\n" +
                        "mmt8942\tGEOHASH(7c)\n" +
                        "71ftmpy\tGEOHASH(7c)\n" +
                        "9v1s8hm\tGEOHASH(7c)\n" +
                        "46swgj1\tGEOHASH(7c)\n" +
                        "jnw97u4\tGEOHASH(7c)\n" +
                        "zfuqd3b\tGEOHASH(7c)\n" +
                        "hp4muv5\tGEOHASH(7c)\n",
                "select a, typeOf(a) from (x union all y)",
                "create table x as (select rnd_geohash(35) a from long_sequence(5))",
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
    public void testIntBool() throws Exception {
        // this is cast to STRING, both columns
        compile("create table y as (select rnd_int() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "-1148479920\n" +
                        "315515118\n" +
                        "1548800833\n" +
                        "-727724771\n" +
                        "73575701\n",
                "x union all y",
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testIntByte() throws Exception {
        compile("create table y as (select rnd_int() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "79\n" +
                        "122\n" +
                        "83\n" +
                        "90\n" +
                        "76\n" +
                        "-1148479920\n" +
                        "315515118\n" +
                        "1548800833\n" +
                        "-727724771\n" +
                        "73575701\n",
                "x union all y",
                "create table x as (select rnd_byte() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testIntChar() throws Exception {
        compile("create table y as (select rnd_int() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "80\n" +
                        "83\n" +
                        "87\n" +
                        "72\n" +
                        "89\n" +
                        "-1148479920\n" +
                        "315515118\n" +
                        "1548800833\n" +
                        "-727724771\n" +
                        "73575701\n",
                "x union all y",
                "create table x as (select rnd_char() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testIntShort() throws Exception {
        compile("create table y as (select rnd_int() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "-22955\n" +
                        "-1398\n" +
                        "21015\n" +
                        "30202\n" +
                        "-19496\n" +
                        "-1148479920\n" +
                        "315515118\n" +
                        "1548800833\n" +
                        "-727724771\n" +
                        "73575701\n",
                "x union all y",
                "create table x as (select rnd_short() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testBinBin() throws Exception {
        compile("create table y as (select rnd_int() a, rnd_bin(10,10,1) n from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\tn\ttypeOf\n" +
                        "32312\t\tBINARY\n" +
                        "4635\t00000000 f4 c8 39 09 fe d8 9d 30 78 36\tBINARY\n" +
                        "-22934\t00000000 de e4 7c d2 35 07 42 fc 31 79\tBINARY\n" +
                        "22367\t\tBINARY\n" +
                        "-12671\t\tBINARY\n" +
                        "-1148479920\t00000000 41 1d 15 55 8a 17 fa d8 cc 14\tBINARY\n" +
                        "-1436881714\t\tBINARY\n" +
                        "806715481\t00000000 c4 91 3b 72 db f3 04 1b c7 88\tBINARY\n" +
                        "-1432278050\t00000000 79 3c 77 15 68 61 26 af 19 c4\tBINARY\n" +
                        "-1975183723\t00000000 36 53 49 b4 59 7e 3b 08 a1 1e\tBINARY\n",
                "select a, n, typeOf(n) from (x union all y)",
                "create table x as (select rnd_short() a, rnd_bin(10,10,1) n from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testLongBin() throws Exception {
        compile("create table y as (select rnd_long() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertFailure("x union all y",
                "create table x as (select rnd_bin(10, 24, 1) a from long_sequence(5))",
                0,
                "unsupported cast"
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
    public void testShortBool() throws Exception {
        // this is cast to STRING, both columns
        compile("create table y as (select rnd_short() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "-27056\n" +
                        "24814\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-21227\n",
                "x union all y",
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testShortByte() throws Exception {
        compile("create table y as (select rnd_byte() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "-22955\n" +
                        "-1398\n" +
                        "21015\n" +
                        "30202\n" +
                        "-19496\n" +
                        "76\n" +
                        "102\n" +
                        "27\n" +
                        "87\n" +
                        "79\n",
                "x union all y",
                "create table x as (select rnd_short() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testStrStr() throws Exception {
        // we include byte <-> bool cast to make sure
        // bool <-> bool cast it not thrown away as redundant
        compile("create table y as (select rnd_byte() b, rnd_str(4,4,1) c from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\tc\n" +
                        "false\tIBB\n" +
                        "true\tPGW\n" +
                        "true\tYUD\n" +
                        "false\tYQE\n" +
                        "false\tHFO\n" +
                        "76\tJWCP\n" +
                        "122\t\n" +
                        "90\tRXPE\n" +
                        "83\tRXGZ\n" +
                        "77\t\n",
                "x union all y",
                "create table x as (select rnd_boolean() a, rnd_str(3,3,1) c from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testSymBin() throws Exception {
        compile("create table y as (select rnd_bin(10, 24, 1) b from long_sequence(5))");
        engine.releaseAllWriters();
        assertFailure("x union all y",
                "create table x as (select rnd_symbol('aa','bb') a from long_sequence(5))",
                12,
                "unsupported cast"
        );
    }

    @Test
    public void testSymSym() throws Exception {
        // we include byte <-> bool cast to make sure
        // sym <-> sym cast it not thrown away as redundant
        compile("create table y as (select rnd_byte() a, rnd_symbol('x','y') c from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\tc\ttypeOf\n" +
                        "false\ta\tSTRING\n" +
                        "false\tb\tSTRING\n" +
                        "true\ta\tSTRING\n" +
                        "true\tb\tSTRING\n" +
                        "false\ta\tSTRING\n" +
                        "76\tx\tSTRING\n" +
                        "27\ty\tSTRING\n" +
                        "79\ty\tSTRING\n" +
                        "122\ty\tSTRING\n" +
                        "90\tx\tSTRING\n",
                "select a, c, typeOf(c) from (x union all y)",
                "create table x as (select rnd_boolean() a, rnd_symbol('a','b') c from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    @Ignore
    public void testSymSymErr() throws Exception {
        // we include byte <-> bool cast to make sure
        // sym <-> sym cast it not thrown away as redundant
        compile("create table y as (select rnd_byte() b, rnd_symbol('x','y') c from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\tc\n" +
                        "false\tfalse\n" +
                        "false\ttrue\n" +
                        "true\ttrue\n" +
                        "true\tfalse\n" +
                        "false\tfalse\n" +
                        "76\tfalse\n" +
                        "27\ttrue\n" +
                        "79\tfalse\n" +
                        "122\ttrue\n" +
                        "90\ttrue\n",
                "select a, c, typeOf(c) from (x union all y)",
                "create table x as (select rnd_boolean() a, rnd_symbol('a','b') c from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testTimestampBool() throws Exception {
        // this is cast to STRING, both columns
        compile("create table y as (select rnd_timestamp(0, 100000, 2) b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "true\n" +
                        "true\n" +
                        "false\n" +
                        "false\n" +
                        "false\n" +
                        "1970-01-01T00:00:00.002771Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.045299Z\n" +
                        "1970-01-01T00:00:00.078334Z\n" +
                        "\n",
                "x union all y",
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testTimestampByte() throws Exception {
        compile("create table y as (select rnd_byte() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "284661-01-03T01:19:47.660373Z\n" +
                        "-239240-04-12T14:04:18.917002Z\n" +
                        "-167698-05-16T15:46:11.908119Z\n" +
                        "-82114-11-17T05:49:39.135994Z\n" +
                        "-51129-02-11T06:38:29.397464Z\n" +
                        "1970-01-01T00:00:00.000076Z\n" +
                        "1970-01-01T00:00:00.000102Z\n" +
                        "1970-01-01T00:00:00.000027Z\n" +
                        "1970-01-01T00:00:00.000087Z\n" +
                        "1970-01-01T00:00:00.000079Z\n",
                "x union all y",
                "create table x as (select cast(rnd_long() as timestamp) a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testTimestampChar() throws Exception {
        compile("create table y as (select rnd_char() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "284661-01-03T01:19:47.660373Z\n" +
                        "-239240-04-12T14:04:18.917002Z\n" +
                        "-167698-05-16T15:46:11.908119Z\n" +
                        "-82114-11-17T05:49:39.135994Z\n" +
                        "-51129-02-11T06:38:29.397464Z\n" +
                        "1970-01-01T00:00:00.000086Z\n" +
                        "1970-01-01T00:00:00.000084Z\n" +
                        "1970-01-01T00:00:00.000074Z\n" +
                        "1970-01-01T00:00:00.000087Z\n" +
                        "1970-01-01T00:00:00.000067Z\n",
                "x union all y",
                "create table x as (select cast(rnd_long() as timestamp) a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testTimestampFloat() throws Exception {
        compile("create table y as (select rnd_float() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "8.9208667E18\n" +
                        "-7.6118437E18\n" +
                        "-5.3541934E18\n" +
                        "-2.65340716E18\n" +
                        "-1.67563895E18\n" +
                        "0.6608\n" +
                        "0.8043\n" +
                        "0.2246\n" +
                        "0.1297\n" +
                        "0.0849\n",
                "x union all y",
                "create table x as (select cast(rnd_long() as timestamp) a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testTimestampInt() throws Exception {
        compile("create table y as (select rnd_int() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "284661-01-03T01:19:47.660373Z\n" +
                        "-239240-04-12T14:04:18.917002Z\n" +
                        "-167698-05-16T15:46:11.908119Z\n" +
                        "-82114-11-17T05:49:39.135994Z\n" +
                        "-51129-02-11T06:38:29.397464Z\n" +
                        "1969-12-31T23:40:51.520080Z\n" +
                        "1970-01-01T00:05:15.515118Z\n" +
                        "1970-01-01T00:25:48.800833Z\n" +
                        "1969-12-31T23:47:52.275229Z\n" +
                        "1970-01-01T00:01:13.575701Z\n",
                "x union all y",
                "create table x as (select cast(rnd_long() as timestamp) a from long_sequence(5))",
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

    @Test
    public void testTimestampShort() throws Exception {
        compile("create table y as (select rnd_short() b from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "a\n" +
                        "284661-01-03T01:19:47.660373Z\n" +
                        "-239240-04-12T14:04:18.917002Z\n" +
                        "-167698-05-16T15:46:11.908119Z\n" +
                        "-82114-11-17T05:49:39.135994Z\n" +
                        "-51129-02-11T06:38:29.397464Z\n" +
                        "1969-12-31T23:59:59.972944Z\n" +
                        "1970-01-01T00:00:00.024814Z\n" +
                        "1969-12-31T23:59:59.988545Z\n" +
                        "1969-12-31T23:59:59.986973Z\n" +
                        "1969-12-31T23:59:59.978773Z\n",
                "x union all y",
                "create table x as (select cast(rnd_long() as timestamp) a from long_sequence(5))",
                null,
                false,
                true,
                true
        );
    }
}
