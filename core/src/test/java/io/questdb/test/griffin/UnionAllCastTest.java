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

import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class UnionAllCastTest extends AbstractCairoTest {

    @Test
    public void testAllNoCast() throws Exception {
        // we include byte <-> bool cast to make sure
        // sym <-> sym cast it not thrown away as redundant
        testUnionAll(
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tp\tr\ts\n" +
                        "true\t37\tO\t10549\t171760612\t8503557900983561786\t0.7586254\t0.5913874468544745\t1970-01-01T00:05:56.479Z\t1970-01-01T00:00:00.000001Z\t0x434524824ca84f523ed391560ac327544a27205d291d7f124c83d07de0778e77\t00000000 29 26 c5 aa da 18 ce 5f b2 8b\t101000\t11000111111100\t6jq4\tn3ub6zju\tCKFMQ\tᣮաf@ץ\n" +
                        "false\t113\tY\t21020\t-1915752164\t5922689877598858022\t0.43461353\t0.7195457109208119\t1970-01-01T00:40:51.578Z\t1970-01-01T00:00:00.000000Z\t0x20cfa22cd22bf054483c83d88ac674e3894499a1a1680580cfedff23a67d918f\t00000000 07 b1 32 57 ff 9a ef 88 cb 4b\t001011\t01000101000010\txf81\tcbj71euv\tLNYRZ\t9іa\uDA76\uDDD4*\n" +
                        "true\t62\tP\t13143\t-770962341\t-4036499202601723677\t0.84029645\t0.5794665369115236\t1970-01-01T02:26:24.738Z\t1970-01-01T00:00:00.000000Z\t0x7f19777ec13680558a2d082bfad3aa844a20938221fd7f431bd29676f6902e64\t00000000 dd 44 11 e2 a3 24 4e 44 a8 0d\t001110\t10101011111110\tnz3p\tz1rvm419\tGIJYD\t\n" +
                        "false\t79\tN\t-29677\t-8264817\t-9118587154366063429\t0.6397125\t0.798471808479839\t1970-01-01T00:02:12.849Z\t1970-01-01T00:00:00.000001Z\t0xb19ddb7ff5abcafec82c35a389f834dababcd0482f05618f926cdd99e63abb35\t\t001001\t11010000011000\tgj7w\txd3qr0fm\tQEMXD\t\n" +
                        "false\t123\tJ\t-4254\t-735934368\t8384866408379441071\t0.52348924\t0.5778817852306684\t1970-01-01T01:12:18.900Z\t\t0x705380a68d53d93c2692845742d6674742bdf2c301f7f43b9747f18a1ed2ef46\t\t010001\t01000000100001\tz403\trcc9nh2x\tDHHGG\tX\uDA8B\uDFC4︵Ƀ^\n" +
                        "false\t102\tJ\t-13027\t73575701\t8920866532787660373\t0.29919904\t0.0843832076262595\t1970-01-01T00:10:02.536Z\t1970-01-01T00:00:00.000000Z\t0xc1e631285c1ab288c72bfc5230158059980eca62a219a0f16846d7a3aa5aecce\t00000000 91 3b 72 db f3 04 1b c7 88 de\t110110\t11001100100010\txn8n\t0n2gm6r7\tHFOWL\t͛Ԉ龘и\uDA89\uDFA4\n" +
                        "true\t62\tL\t5639\t-1418341054\t3152466304308949756\t0.76642567\t0.4138164748227684\t1970-01-01T02:34:54.347Z\t\t0x6698c6c186b7571a9cba3ef59083484d98c2d832d83de9934a0705e1136e872b\t00000000 78 b5 b9 11 53 d0 fb 64 bb 1a\t011110\t01011100110001\tqytg\t7kfnr23n\tWEKGH\t\uDB8D\uDE4Eᯤ\\篸{\n" +
                        "false\t33\tL\t-20409\t-712702244\t-6190031864817509934\t0.58112466\t0.4971342426836798\t1970-01-01T01:17:56.168Z\t\t0x4b0a72b3339b8c7c1872e79ea10322460cb5f439cbc22e9d1f0481ab7acd1f4a\t\t101010\t10101111000001\tttz9\tnzxtf741\t\t|\\軦۽㒾\n" +
                        "true\t92\tP\t4215\t-889224806\t8889492928577876455\t0.069444776\t0.6697969295620055\t1970-01-01T00:02:10.728Z\t1970-01-01T00:00:00.000001Z\t0x3d9491e7e14eba8e1de93a9cf1483e290ec6c3651b1c029f825c96def9f2fcc2\t\t010111\t10110011101001\tvgyb\tg6mmvcdb\tZRMFM\t\n" +
                        "true\t107\tG\t-5240\t-1121895896\t-2000273984235276379\t0.18336213\t0.2711532808184136\t1970-01-01T00:55:56.811Z\t1970-01-01T00:00:00.000001Z\t0x76ffd1a81bf39767b92d0771d78263eb5479ae0482582ad03c84de8f7bd9235d\t00000000 a7 6a 71 34 e0 b0 e9 98 f7 67\t100011\t00001000010100\tvs9s\tn0vjumxz\tLDGLO\ṱ\uD8F2\uDE8E>\uDAE6\uDEE3g\n",
                // column "u" is not ultimately selected from neither X nor Y
                // we expect this column to be ignored by optimiser, and also
                // we expect optimiser to correctly select column "b" from Y as
                // a match against column "a" in the union
                "create table x as (" +
                        "select" +
                        " rnd_boolean() a," +
                        " rnd_byte() b," +
                        " rnd_char() c," +
                        " rnd_short() d," +
                        " rnd_int() e," +
                        " rnd_long() f," +
                        " rnd_float() g," +
                        " rnd_double() h," +
                        " rnd_date() i," +
                        " rnd_timestamp(0, 1, 2) j," +
                        " rnd_long256() k," +
                        " rnd_bin(10, 10, 1) l," +
                        " rnd_geohash(6) m," +
                        " rnd_geohash(14) n," +
                        " rnd_geohash(20) o," +
                        " rnd_geohash(40) p," +
                        " rnd_str(5,5,1) r, " +
                        " rnd_varchar(5,5,1) s " +
                        "from long_sequence(5))",
                "create table y as (" +
                        "select" +
                        " rnd_boolean() a," +
                        " rnd_byte() b," +
                        " rnd_char() c," +
                        " rnd_short() d," +
                        " rnd_int() e," +
                        " rnd_long() f," +
                        " rnd_float() g," +
                        " rnd_double() h," +
                        " rnd_date() i," +
                        " rnd_timestamp(0, 1, 2) j," +
                        " rnd_long256() k," +
                        " rnd_bin(10, 10, 1) l," +
                        " rnd_geohash(6) m," +
                        " rnd_geohash(14) n," +
                        " rnd_geohash(20) o," +
                        " rnd_geohash(40) p," +
                        " rnd_str(5,5,1) r, " +
                        " rnd_varchar(5,5,1) s " +
                        "from long_sequence(5))"
        );
    }

    @Test
    public void testBinBin() throws Exception {
        testUnionAll(
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
                "create table y as (select rnd_int() a, rnd_bin(10,10,1) n from long_sequence(5))"
        );
    }

    @Test
    public void testBoolBool() throws Exception {
        // we include byte <-> bool cast to make sure
        // bool <-> bool cast it not thrown away as redundant
        testUnionAll(
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
                "create table x as (select rnd_boolean() a, rnd_boolean() c from long_sequence(5))",
                "create table y as (select rnd_byte() b, rnd_boolean() c from long_sequence(5))",
                false
        );

        testUnion(
                "a\tc\n" +
                        "false\tfalse\n" +
                        "false\ttrue\n" +
                        "true\ttrue\n" +
                        "true\tfalse\n" +
                        "76\tfalse\n" +
                        "27\ttrue\n" +
                        "79\tfalse\n" +
                        "122\ttrue\n" +
                        "90\ttrue\n"
        );
    }

    @Test
    public void testBoolNull() throws Exception {
        testUnionAllWithNull(
                "a\tc\n" +
                        "false\tfalse\n" +
                        "true\tfalse\n" +
                        "true\tfalse\n" +
                        "true\tfalse\n" +
                        "true\tfalse\n" +
                        "false\tfalse\n" +
                        "false\tfalse\n" +
                        "false\tfalse\n" +
                        "false\ttrue\n" +
                        "false\tfalse\n",
                "rnd_boolean()",
                false
        );

        testUnionWithNull(
                "a\tc\n" +
                        "false\tfalse\n" +
                        "true\tfalse\n" +
                        "false\ttrue\n"
        );
    }

    @Test
    public void testByteBool() throws Exception {
        // this is cast to STRING, both columns
        testUnionAll(
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
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                "create table y as (select rnd_byte() b from long_sequence(5))",
                false
        );

        testUnion(
                "a\n" +
                        "false\n" +
                        "true\n" +
                        "76\n" +
                        "102\n" +
                        "27\n" +
                        "87\n" +
                        "79\n"
        );
    }

    @Test
    public void testByteByte() throws Exception {
        // we include byte <-> bool cast to make sure
        // byte <-> byte cast it not thrown away as redundant
        testUnionAll(
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
                "create table x as (select rnd_boolean() a, rnd_byte() c from long_sequence(5))",
                "create table y as (select rnd_byte() b, rnd_byte() c from long_sequence(5))"
        );
    }

    @Test
    public void testByteChar() throws Exception {
        testUnionAll(
                "a\n" +
                        "79\n" +
                        "122\n" +
                        "83\n" +
                        "90\n" +
                        "76\n" +
                        "V\n" +
                        "T\n" +
                        "J\n" +
                        "W\n" +
                        "C\n",
                "create table x as (select rnd_byte() a from long_sequence(5))",
                "create table y as (select rnd_char() b from long_sequence(5))"
        );
    }

    @Test
    public void testByteNull() throws Exception {
        testUnionAllWithNull(
                "a\tc\n" +
                        "79\t0\n" +
                        "122\t0\n" +
                        "83\t0\n" +
                        "90\t0\n" +
                        "76\t0\n" +
                        "0\t76\n" +
                        "0\t102\n" +
                        "0\t27\n" +
                        "0\t87\n" +
                        "0\t79\n",
                "rnd_byte()"
        );
    }

    @Test
    public void testCharBool() throws Exception {
        // this is cast to STRING, both columns
        testUnionAll(
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
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                "create table y as (select rnd_char() b from long_sequence(5))",
                false
        );
        testUnion(
                "a\n" +
                        "false\n" +
                        "true\n" +
                        "V\n" +
                        "T\n" +
                        "J\n" +
                        "W\n" +
                        "C\n"
        );
    }

    @Test
    public void testCharByte() throws Exception {
        testUnionAll(
                "a\n" +
                        "P\n" +
                        "S\n" +
                        "W\n" +
                        "H\n" +
                        "Y\n" +
                        "76\n" +
                        "102\n" +
                        "27\n" +
                        "87\n" +
                        "79\n",
                "create table x as (select rnd_char() a from long_sequence(5))",
                "create table y as (select rnd_byte() b from long_sequence(5))"
        );
    }

    @Test
    public void testCharNull() throws Exception {
        testUnionAllWithNull(
                "a\tc\n" +
                        "P\t\n" +
                        "S\t\n" +
                        "W\t\n" +
                        "H\t\n" +
                        "Y\t\n" +
                        "\tV\n" +
                        "\tT\n" +
                        "\tJ\n" +
                        "\tW\n" +
                        "\tC\n",
                "rnd_char()"
        );
    }

    @Test
    public void testCharShort() throws Exception {
        testUnionAll(
                "a\n" +
                        "-27056\n" +
                        "24814\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-21227\n" +
                        "0\n" +
                        "3\n" +
                        "5\n" +
                        "9\n" +
                        "8\n",
                "create table x as (select rnd_short() a from long_sequence(5))",
                "create table y as (select '0'::char a union select '3'::char union select '5'::char union select '9'::char union select '8'::char)"
        );
    }

    @Test
    public void testDateBool() throws Exception {
        // this is cast to STRING, both columns
        testUnionAll(
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
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                "create table y as (select rnd_date() b from long_sequence(5))",
                false
        );
        testUnion(
                "a\n" +
                        "false\n" +
                        "true\n" +
                        "1970-01-01T02:07:23.856Z\n" +
                        "1970-01-01T02:29:52.366Z\n" +
                        "1970-01-01T01:45:29.025Z\n" +
                        "1970-01-01T01:15:01.475Z\n" +
                        "1970-01-01T00:43:07.029Z\n"
        );
    }

    @Test
    public void testDateByte() throws Exception {
        assertFailure(
                "create table x as (select rnd_date() a from long_sequence(5))",
                "create table y as (select rnd_byte() b from long_sequence(5))",
                12
        );
    }

    @Test
    public void testDateChar() throws Exception {
        assertFailure(
                "create table x as (select rnd_date() a from long_sequence(5))",
                "create table y as (select rnd_char() b from long_sequence(5))",
                12
        );
    }

    @Test
    public void testDateInt() throws Exception {
        assertFailure(
                "create table x as (select rnd_date() a from long_sequence(5))",
                "create table y as (select rnd_int() b from long_sequence(5))",
                12
        );
    }

    @Test
    public void testDateLong() throws Exception {
        assertFailure(
                "create table x as (select rnd_date() a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))",
                12
        );
    }

    @Test
    public void testDateNull() throws Exception {
        testUnionAllWithNull(
                "a\tc\n" +
                        "1970-01-01T02:07:40.373Z\t\n" +
                        "1970-01-01T00:18:02.998Z\t\n" +
                        "1970-01-01T02:14:51.881Z\t\n" +
                        "1970-01-01T00:14:24.006Z\t\n" +
                        "1970-01-01T00:10:02.536Z\t\n" +
                        "\t1970-01-01T02:07:23.856Z\n" +
                        "\t1970-01-01T02:29:52.366Z\n" +
                        "\t1970-01-01T01:45:29.025Z\n" +
                        "\t1970-01-01T01:15:01.475Z\n" +
                        "\t1970-01-01T00:43:07.029Z\n",
                "rnd_date()"
        );
    }

    @Test
    public void testDateShort() throws Exception {
        assertFailure(
                "create table x as (select rnd_date() a from long_sequence(5))",
                "create table y as (select rnd_short() b from long_sequence(5))",
                12
        );
    }

    @Test
    public void testDateTimestamp() throws Exception {
        testUnionAll(
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
                "create table y as (select rnd_timestamp(0, 100000, 1) b from long_sequence(5))"
        );
    }

    @Test
    public void testDoubleBool() throws Exception {
        // this is cast to STRING, both columns
        testUnionAll(
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
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                "create table y as (select rnd_double() b from long_sequence(5))",
                false
        );

        testUnion(
                "a\n" +
                        "false\n" +
                        "true\n" +
                        "0.6607777894187332\n" +
                        "0.2246301342497259\n" +
                        "0.08486964232560668\n" +
                        "0.299199045961845\n" +
                        "0.20447441837877756\n"
        );
    }

    @Test
    public void testDoubleByte() throws Exception {
        testUnionAll(
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
                "create table x as (select rnd_double() a from long_sequence(5))",
                "create table y as (select rnd_byte() b from long_sequence(5))"
        );
    }

    @Test
    public void testDoubleChar() throws Exception {
        assertFailure(
                "create table x as (select rnd_double() a from long_sequence(5))",
                "create table y as (select rnd_char() b from long_sequence(5))",
                12
        );
    }

    @Test
    public void testDoubleDate() throws Exception {
        assertFailure(
                "create table x as (select rnd_double() a from long_sequence(5))",
                "create table y as (select rnd_date() b from long_sequence(5))",
                12
        );
    }

    @Test
    public void testDoubleInt() throws Exception {
        testUnionAll(
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
                "create table x as (select rnd_double() a from long_sequence(5))",
                "create table y as (select rnd_int() b from long_sequence(5))"
        );
    }

    @Test
    public void testDoubleLong() throws Exception {
        testUnionAll(
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
                "create table x as (select rnd_double() a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testDoubleNull() throws Exception {
        testUnionAllWithNull(
                "a\tc\n" +
                        "0.6508594025855301\tnull\n" +
                        "0.8423410920883345\tnull\n" +
                        "0.9856290845874263\tnull\n" +
                        "0.22452340856088226\tnull\n" +
                        "0.5093827001617407\tnull\n" +
                        "null\t0.6607777894187332\n" +
                        "null\t0.2246301342497259\n" +
                        "null\t0.08486964232560668\n" +
                        "null\t0.299199045961845\n" +
                        "null\t0.20447441837877756\n",
                "rnd_double()"
        );
    }

    @Test
    public void testDoubleShort() throws Exception {
        testUnionAll(
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
                "create table x as (select rnd_double() a from long_sequence(5))",
                "create table y as (select rnd_short() b from long_sequence(5))"
        );
    }

    @Test
    public void testDoubleTimestamp() throws Exception {
        assertFailure(
                "create table x as (select rnd_double() a from long_sequence(5))",
                "create table y as (select cast(rnd_date() as timestamp) b from long_sequence(5))",
                12
        );
    }

    @Test
    public void testExceptDoubleFloat() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table events1 (contact symbol, groupid float, eventid string)");
            execute("insert into events1 values ('1', 1.5, 'flash')");
            execute("insert into events1 values ('2', 1.5, 'stand')");

            execute("create table events2 (contact symbol, groupid double, eventid string)");
            execute("insert into events2 values ('1', 1.5, 'flash')");
            execute("insert into events2 values ('2', 1.5, 'stand')");

            assertQueryNoLeakCheck(
                    // Empty table expected
                    "contact\tgroupid\teventid\n",
                    "events1\n" +
                            "except\n" +
                            "events2",
                    null,
                    true
            );
        });
    }

    @Test
    public void testExceptDoubleFloatSort() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table events1 (contact symbol, groupid float, eventid string)");
            execute("insert into events1 values ('1', 1.5, 'flash')");
            execute("insert into events1 values ('2', 1.5, 'stand')");
            execute("insert into events1 values ('1', 1.6, 'stand')");
            execute("insert into events1 values ('2', 1.6, 'stand')");

            execute("create table events2 (contact symbol, groupid double, eventid string)");
            execute("insert into events2 values ('1', 1.5, 'flash')");
            execute("insert into events2 values ('2', 1.5, 'stand')");

            assertQueryNoLeakCheck(
                    // Empty table expected
                    "contact\tgroupid\teventid\n" +
                            "2\t1.600000023841858\tstand\n" +
                            "1\t1.600000023841858\tstand\n",
                    "(events1\n" +
                            "except\n" +
                            "events2) order by 1 desc",
                    null,
                    true
            );
        });
    }

    @Test
    public void testExceptFloatDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table events1 (contact symbol, groupid double, eventid string)");
            execute("insert into events1 values ('1', 1.5, 'flash')");
            execute("insert into events1 values ('2', 1.5, 'stand')");

            execute("create table events2 (contact symbol, groupid float, eventid string)");
            execute("insert into events2 values ('1', 1.5, 'flash')");
            execute("insert into events2 values ('2', 1.5, 'stand')");

            assertQueryNoLeakCheck(
                    // Empty table expected
                    "contact\tgroupid\teventid\n",
                    "events1\n" +
                            "except\n" +
                            "events2",
                    null,
                    true
            );
        });
    }

    @Test
    public void testExceptSort() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table events1 (contact symbol, groupid double, eventid string)");
            execute("insert into events1 values ('1', 1.5, 'flash')");
            execute("insert into events1 values ('2', 1.5, 'stand')");
            execute("insert into events1 values ('1', 1.6, 'stand')");
            execute("insert into events1 values ('2', 1.6, 'stand')");

            execute("create table events2 (contact symbol, groupid double, eventid string)");
            execute("insert into events2 values ('1', 1.5, 'flash')");
            execute("insert into events2 values ('2', 1.5, 'stand')");

            assertQueryNoLeakCheck(
                    // Empty table expected
                    "contact\tgroupid\teventid\n" +
                            "2\t1.6\tstand\n" +
                            "1\t1.6\tstand\n",
                    "(events1\n" +
                            "except\n" +
                            "events2) order by 1 desc",
                    null,
                    true
            );
        });
    }

    @Test
    public void testFloatBool() throws Exception {
        // this is cast to STRING, both columns
        testUnionAll(
                "a\n" +
                        "false\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "true\n" +
                        "0.66077775\n" +
                        "0.80432236\n" +
                        "0.22463012\n" +
                        "0.12966657\n" +
                        "0.08486962\n",
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                "create table y as (select rnd_float() b from long_sequence(5))",
                false
        );

        testUnion(
                "a\n" +
                        "false\n" +
                        "true\n" +
                        "0.66077775\n" +
                        "0.80432236\n" +
                        "0.22463012\n" +
                        "0.12966657\n" +
                        "0.08486962\n"
        );
    }

    @Test
    public void testFloatByte() throws Exception {
        testUnionAll(
                "a\n" +
                        "79.0\n" +
                        "122.0\n" +
                        "83.0\n" +
                        "90.0\n" +
                        "76.0\n" +
                        "0.66077775\n" +
                        "0.80432236\n" +
                        "0.22463012\n" +
                        "0.12966657\n" +
                        "0.08486962\n",
                "create table x as (select rnd_byte() a from long_sequence(5))",
                "create table y as (select rnd_float() b from long_sequence(5))"
        );
    }

    @Test
    public void testFloatChar() throws Exception {
        assertFailure(
                "create table x as (select rnd_char() a from long_sequence(5))",
                "create table y as (select rnd_float() b from long_sequence(5))",
                0
        );
    }

    @Test
    public void testFloatDate() throws Exception {
        assertFailure(
                "create table x as (select rnd_date() a from long_sequence(5))",
                "create table y as (select rnd_float() b from long_sequence(5))",
                0
        );
    }

    @Test
    public void testFloatDouble() throws Exception {
        testUnionAll(
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
                "create table x as (select rnd_double() a from long_sequence(5))",
                "create table y as (select rnd_float() b from long_sequence(5))"
        );
    }

    @Test
    public void testFloatGeoHash() throws Exception {
        testUnionAll(
                "a\n" +
                        "0001001\n" +
                        "0101110\n" +
                        "0101101\n" +
                        "0111011\n" +
                        "0010101\n" +
                        "0.66077775\n" +
                        "0.80432236\n" +
                        "0.22463012\n" +
                        "0.12966657\n" +
                        "0.08486962\n",
                "create table x as (select rnd_geohash(7) a from long_sequence(5))",
                "create table y as (select rnd_float() b from long_sequence(5))"
        );
    }

    @Test
    public void testFloatInt() throws Exception {
        testUnionAll(
                "a\n" +
                        "-9.4826336E8\n" +
                        "1.32644723E9\n" +
                        "5.9285965E8\n" +
                        "1.86872371E9\n" +
                        "-8.4753107E8\n" +
                        "0.66077775\n" +
                        "0.80432236\n" +
                        "0.22463012\n" +
                        "0.12966657\n" +
                        "0.08486962\n",
                "create table x as (select rnd_int() a from long_sequence(5))",
                "create table y as (select rnd_float() b from long_sequence(5))"
        );
    }

    @Test
    public void testFloatLong() throws Exception {
        testUnionAll(
                "a\n" +
                        "0.28455776\n" +
                        "0.29919904\n" +
                        "0.08438319\n" +
                        "0.20447439\n" +
                        "0.93446046\n" +
                        "4.6895921E15\n" +
                        "4.7299965E15\n" +
                        "7.7465361E18\n" +
                        "-6.9459217E18\n" +
                        "8.2601883E18\n",
                "create table x as (select rnd_float() a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testFloatNull() throws Exception {
        testUnionAllWithNull(
                "a\tc\n" +
                        "0.28455776\tnull\n" +
                        "0.29919904\tnull\n" +
                        "0.08438319\tnull\n" +
                        "0.20447439\tnull\n" +
                        "0.93446046\tnull\n" +
                        "null\t0.66077775\n" +
                        "null\t0.80432236\n" +
                        "null\t0.22463012\n" +
                        "null\t0.12966657\n" +
                        "null\t0.08486962\n",
                "rnd_float()"
        );
    }

    @Test
    public void testFloatShort() throws Exception {
        testUnionAll(
                "a\n" +
                        "-22955.0\n" +
                        "-1398.0\n" +
                        "21015.0\n" +
                        "30202.0\n" +
                        "-19496.0\n" +
                        "0.66077775\n" +
                        "0.80432236\n" +
                        "0.22463012\n" +
                        "0.12966657\n" +
                        "0.08486962\n",
                "create table x as (select rnd_short() a from long_sequence(5))",
                "create table y as (select rnd_float() b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoByteExact() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
                "a\ttypeOf\tk\n" +
                        "q\tGEOHASH(1c)\t-1792928964\n" +
                        "5\tGEOHASH(1c)\t1404198\n" +
                        "x\tGEOHASH(1c)\t-1252906348\n" +
                        "f\tGEOHASH(1c)\t1699553881\n" +
                        "8\tGEOHASH(1c)\t-938514914\n" +
                        "9\tGEOHASH(1c)\t8260188555232587029\n" +
                        "2\tGEOHASH(1c)\t-1675638984090602536\n" +
                        "w\tGEOHASH(1c)\t-4094902006239100839\n" +
                        "p\tGEOHASH(1c)\t5408639942391651698\n" +
                        "w\tGEOHASH(1c)\t-3985256597569472057\n",
                "select a, typeOf(a), k from (x union all y)",
                "create table x as (select rnd_geohash(5) a, rnd_int() k from long_sequence(5))",
                "create table y as (select rnd_geohash(5) a, rnd_long() k from long_sequence(5))"
        );
    }

    @Test
    public void testGeoByteNull() throws Exception {
        testUnionAllWithNull(
                "a\tc\n" +
                        "1110010\t\n" +
                        "1100000\t\n" +
                        "0000101\t\n" +
                        "1001110\t\n" +
                        "0011100\t\n" +
                        "\t0100111\n" +
                        "\t0010000\n" +
                        "\t1000110\n" +
                        "\t1111101\n" +
                        "\t1000010\n",
                "rnd_geohash(7)"
        );
    }

    @Test
    public void testGeoByteStr() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
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
                "create table x as (select rnd_geohash(5) a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))",
                false
        );

        testUnion(
                "a\n" +
                        "2\n" +
                        "c\n" +
                        "f\n" +
                        "5\n" +
                        "4689592037643856\n" +
                        "4729996258992366\n" +
                        "7746536061816329025\n" +
                        "-6945921502384501475\n" +
                        "8260188555232587029\n"
        );
    }

    @Test
    public void testGeoIntGeoByte() throws Exception {
        testUnionAll(
                "a\n" +
                        "w\n" +
                        "s\n" +
                        "1\n" +
                        "m\n" +
                        "7\n" +
                        "9\n" +
                        "4\n" +
                        "j\n" +
                        "z\n" +
                        "h\n",
                "create table x as (select rnd_geohash(20) a from long_sequence(5))",
                "create table y as (select rnd_geohash(5) b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoIntGeoShort() throws Exception {
        testUnionAll(
                "a\n" +
                        "wh\n" +
                        "s2\n" +
                        "1c\n" +
                        "mm\n" +
                        "71\n" +
                        "9v\n" +
                        "46\n" +
                        "jn\n" +
                        "zf\n" +
                        "hp\n",
                "create table x as (select rnd_geohash(30) a from long_sequence(5))",
                "create table y as (select rnd_geohash(10) b from long_sequence(5))",
                false
        );
    }

    @Test
    public void testGeoIntNull() throws Exception {
        testUnionAllWithNull(
                "a\tc\n" +
                        "wh4b6v\t\n" +
                        "s2z2fy\t\n" +
                        "1cjjwk\t\n" +
                        "mmt894\t\n" +
                        "71ftmp\t\n" +
                        "\t9v1s8h\n" +
                        "\t46swgj\n" +
                        "\tjnw97u\n" +
                        "\tzfuqd3\n" +
                        "\thp4muv\n",
                "rnd_geohash(30)"
        );
    }

    @Test
    public void testGeoIntStr() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
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
                "create table x as (select rnd_geohash(20) a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoIntStrBits() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
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
                "create table x as (select rnd_geohash(21) a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoLongExact() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
                "a\ttypeOf\tk\n" +
                        "qmqxuuu\tGEOHASH(7c)\t-1792928964\n" +
                        "5rshu96\tGEOHASH(7c)\t1404198\n" +
                        "xn8nmwc\tGEOHASH(7c)\t-1252906348\n" +
                        "fsnj14w\tGEOHASH(7c)\t1699553881\n" +
                        "8nje17e\tGEOHASH(7c)\t-938514914\n" +
                        "9v1s8hm\tGEOHASH(7c)\t8260188555232587029\n" +
                        "29je7k2\tGEOHASH(7c)\t-1675638984090602536\n" +
                        "wszdkrq\tGEOHASH(7c)\t-4094902006239100839\n" +
                        "pn5udk1\tGEOHASH(7c)\t5408639942391651698\n" +
                        "wh4b6vn\tGEOHASH(7c)\t-3985256597569472057\n",
                "select a, typeOf(a), k from (x union all y)",
                "create table x as (select rnd_geohash(35) a, rnd_int() k from long_sequence(5))",
                "create table y as (select rnd_geohash(35) a, rnd_long() k from long_sequence(5))"
        );
    }

    @Test
    public void testGeoLongGeoByte() throws Exception {
        testUnionAll(
                "a\n" +
                        "w\n" +
                        "s\n" +
                        "1\n" +
                        "m\n" +
                        "7\n" +
                        "9\n" +
                        "4\n" +
                        "j\n" +
                        "z\n" +
                        "h\n",
                "create table x as (select rnd_geohash(40) a from long_sequence(5))",
                "create table y as (select rnd_geohash(5) b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoLongGeoInt() throws Exception {
        testUnionAll(
                "a\n" +
                        "wh4b6v\n" +
                        "s2z2fy\n" +
                        "1cjjwk\n" +
                        "mmt894\n" +
                        "71ftmp\n" +
                        "9v1s8h\n" +
                        "46swgj\n" +
                        "jnw97u\n" +
                        "zfuqd3\n" +
                        "hp4muv\n",
                "create table x as (select rnd_geohash(40) a from long_sequence(5))",
                "create table y as (select rnd_geohash(30) b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoLongGeoShort() throws Exception {
        testUnionAll(
                "a\n" +
                        "wh\n" +
                        "s2\n" +
                        "1c\n" +
                        "mm\n" +
                        "71\n" +
                        "9v\n" +
                        "46\n" +
                        "jn\n" +
                        "zf\n" +
                        "hp\n",
                "create table x as (select rnd_geohash(40) a from long_sequence(5))",
                "create table y as (select rnd_geohash(10) b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoLongNull() throws Exception {
        testUnionAllWithNull(
                "a\tc\n" +
                        "wh4b6vnt\t\n" +
                        "s2z2fyds\t\n" +
                        "1cjjwk6r\t\n" +
                        "mmt89425\t\n" +
                        "71ftmpy5\t\n" +
                        "\t9v1s8hm7\n" +
                        "\t46swgj10\n" +
                        "\tjnw97u4y\n" +
                        "\tzfuqd3bf\n" +
                        "\thp4muv5t\n",
                "rnd_geohash(40)"
        );
    }

    @Test
    public void testGeoLongStr() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
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
                "create table x as (select rnd_geohash(40) a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoLongStrBits() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
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
                "create table x as (select rnd_geohash(42) a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoShortExact() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
                "a\ttypeOf\tk\n" +
                        "101101001110\tGEOHASH(12b)\t-1792928964\n" +
                        "001011011111\tGEOHASH(12b)\t1404198\n" +
                        "111011010001\tGEOHASH(12b)\t-1252906348\n" +
                        "011101100010\tGEOHASH(12b)\t1699553881\n" +
                        "010001010010\tGEOHASH(12b)\t-938514914\n" +
                        "010011101100\tGEOHASH(12b)\t8260188555232587029\n" +
                        "000100100110\tGEOHASH(12b)\t-1675638984090602536\n" +
                        "111001100011\tGEOHASH(12b)\t-4094902006239100839\n" +
                        "101011010000\tGEOHASH(12b)\t5408639942391651698\n" +
                        "111001000000\tGEOHASH(12b)\t-3985256597569472057\n",
                "select a, typeOf(a), k from (x union all y)",
                "create table x as (select rnd_geohash(12) a, rnd_int() k from long_sequence(5))",
                "create table y as (select rnd_geohash(12) a, rnd_long() k from long_sequence(5))"
        );
    }

    @Test
    public void testGeoShortGeoByte() throws Exception {
        testUnionAll(
                "a\n" +
                        "w\n" +
                        "s\n" +
                        "1\n" +
                        "m\n" +
                        "7\n" +
                        "9\n" +
                        "4\n" +
                        "j\n" +
                        "z\n" +
                        "h\n",
                "create table x as (select rnd_geohash(10) a from long_sequence(5))",
                "create table y as (select rnd_geohash(5) b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoShortGeoShort() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
                "a\n" +
                        "k0\n" +
                        "0c\n" +
                        "5f\n" +
                        "fg\n" +
                        "w5\n" +
                        "9v\n" +
                        "46\n" +
                        "jn\n" +
                        "zf\n" +
                        "hp\n",
                "create table x as (select rnd_geohash(12) a from long_sequence(5))",
                "create table y as (select rnd_geohash(10) b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoShortNull() throws Exception {
        testUnionAllWithNull(
                "a\tc\n" +
                        "wh4\t\n" +
                        "s2z\t\n" +
                        "1cj\t\n" +
                        "mmt\t\n" +
                        "71f\t\n" +
                        "\t9v1\n" +
                        "\t46s\n" +
                        "\tjnw\n" +
                        "\tzfu\n" +
                        "\thp4\n",
                "rnd_geohash(15)"
        );
    }

    @Test
    public void testGeoShortStr() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
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
                "create table x as (select rnd_geohash(10) a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoShortStrBits() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
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
                "create table x as (select rnd_geohash(12) a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testIPv4String() throws Exception {
        testUnionAll(
                "a\n" +
                        "101.77.34.89\n" +
                        "66.56.51.126\n" +
                        "74.188.217.59\n" +
                        "249.60.8.8\n" +
                        "230.202.108.161\n" +
                        "JWCPSWHYR\n" +
                        "EHNRX\n" +
                        "SXUXI\n" +
                        "TGPGW\n" +
                        "YUDEYYQEHB\n",
                "create table x as (select rnd_ipv4() a from long_sequence(5))",
                "create table y as (select rnd_str() b from long_sequence(5))",
                true
        );
    }

    @Test
    public void testIPv4Symbol() throws Exception {
        testUnionAll(
                "a\n" +
                        "199.122.166.85\n" +
                        "79.15.250.138\n" +
                        "35.86.82.23\n" +
                        "111.98.117.250\n" +
                        "205.123.179.216\n" +
                        "aaa\n" +
                        "aaa\n" +
                        "bbb\n" +
                        "bbb\n" +
                        "bbb\n",
                "create table x as (select rnd_ipv4() a from long_sequence(5))",
                "create table y as (select rnd_symbol('aaa', 'bbb') a from long_sequence(5))",
                false
        );

        testUnion(
                "a\n" +
                        "199.122.166.85\n" +
                        "79.15.250.138\n" +
                        "35.86.82.23\n" +
                        "111.98.117.250\n" +
                        "205.123.179.216\n" +
                        "aaa\n" +
                        "bbb\n"
        );
    }

    @Test
    public void testIPv4Varchar() throws Exception {
        testUnionAll(
                "a\n" +
                        "49.254.54.230\n" +
                        "89.207.251.208\n" +
                        "66.9.11.179\n" +
                        "50.89.42.43\n" +
                        "219.41.127.7\n" +
                        "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\n" +
                        "8#3TsZ\n" +
                        "zV衞͛Ԉ龘и\uDA89\uDFA4~\n" +
                        "ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\n" +
                        "\uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46OF\n",
                "create table x as (select rnd_ipv4() a from long_sequence(5))",
                "create table y as (select rnd_varchar() b from long_sequence(5))",
                true
        );
    }

    @Test
    public void testIntBool() throws Exception {
        // this is cast to STRING, both columns
        testUnionAll(
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
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                "create table y as (select rnd_int() b from long_sequence(5))",
                false
        );

        testUnion(
                "a\n" +
                        "false\n" +
                        "true\n" +
                        "-1148479920\n" +
                        "315515118\n" +
                        "1548800833\n" +
                        "-727724771\n" +
                        "73575701\n"
        );
    }

    @Test
    public void testIntByte() throws Exception {
        testUnionAll(
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
                "create table x as (select rnd_byte() a from long_sequence(5))",
                "create table y as (select rnd_int() b from long_sequence(5))"
        );
    }

    @Test
    public void testIntChar() throws Exception {
        testUnionAll(
                "a\n" +
                        "0\n" +
                        "3\n" +
                        "5\n" +
                        "9\n" +
                        "8\n" +
                        "-1148479920\n" +
                        "315515118\n" +
                        "1548800833\n" +
                        "-727724771\n" +
                        "73575701\n",
                "create table x as (select '0'::char a union select '3'::char union select '5'::char union select '9'::char union select '8'::char)",
                "create table y as (select rnd_int() b from long_sequence(5))"
        );
    }

    @Test
    public void testIntExact() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
                "a\ttypeOf\tk\n" +
                        "qmqxuu\tGEOHASH(6c)\t-1792928964\n" +
                        "5rshu9\tGEOHASH(6c)\t1404198\n" +
                        "xn8nmw\tGEOHASH(6c)\t-1252906348\n" +
                        "fsnj14\tGEOHASH(6c)\t1699553881\n" +
                        "8nje17\tGEOHASH(6c)\t-938514914\n" +
                        "9v1s8h\tGEOHASH(6c)\t8260188555232587029\n" +
                        "29je7k\tGEOHASH(6c)\t-1675638984090602536\n" +
                        "wszdkr\tGEOHASH(6c)\t-4094902006239100839\n" +
                        "pn5udk\tGEOHASH(6c)\t5408639942391651698\n" +
                        "wh4b6v\tGEOHASH(6c)\t-3985256597569472057\n",
                "select a, typeOf(a), k from (x union all y)",
                "create table x as (select rnd_geohash(30) a, rnd_int() k from long_sequence(5))",
                "create table y as (select rnd_geohash(30) a, rnd_long() k from long_sequence(5))"
        );
    }

    @Test
    public void testIntNull() throws Exception {
        testUnionAllWithNull(
                "a\tc\n" +
                        "-948263339\tnull\n" +
                        "1326447242\tnull\n" +
                        "592859671\tnull\n" +
                        "1868723706\tnull\n" +
                        "-847531048\tnull\n" +
                        "null\t-1148479920\n" +
                        "null\t315515118\n" +
                        "null\t1548800833\n" +
                        "null\t-727724771\n" +
                        "null\t73575701\n",
                "rnd_int()"
        );
    }

    @Test
    public void testIntShort() throws Exception {
        testUnionAll(
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
                "create table x as (select rnd_short() a from long_sequence(5))",
                "create table y as (select rnd_int() b from long_sequence(5))"
        );
    }

    @Test
    public void testIntersectDoubleFloatSort() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table events1 (contact symbol, groupid float, eventid string)");
            execute("insert into events1 values ('1', 1.5, 'flash')");
            execute("insert into events1 values ('2', 1.5, 'stand')");
            execute("insert into events1 values ('1', 1.6, 'stand')");
            execute("insert into events1 values ('2', 1.6, 'stand')");

            execute("create table events2 (contact symbol, groupid double, eventid string)");
            execute("insert into events2 values ('1', 1.5, 'flash')");
            execute("insert into events2 values ('2', 1.5, 'stand')");

            assertQueryNoLeakCheck(
                    // Empty table expected
                    "contact\tgroupid\teventid\n" +
                            "2\t1.5\tstand\n" +
                            "1\t1.5\tflash\n",
                    "(events1\n" +
                            "intersect\n" +
                            "events2) order by 1 desc",
                    null,
                    true
            );
        });
    }

    @Test
    public void testIntersectSort() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table events1 (contact symbol, groupid double, eventid string)");
            execute("insert into events1 values ('1', 1.5, 'flash')");
            execute("insert into events1 values ('2', 1.5, 'stand')");
            execute("insert into events1 values ('1', 1.6, 'stand')");
            execute("insert into events1 values ('2', 1.6, 'stand')");

            execute("create table events2 (contact symbol, groupid double, eventid string)");
            execute("insert into events2 values ('1', 1.5, 'flash')");
            execute("insert into events2 values ('2', 1.5, 'stand')");

            assertQueryNoLeakCheck(
                    // Empty table expected
                    "contact\tgroupid\teventid\n" +
                            "2\t1.5\tstand\n" +
                            "1\t1.5\tflash\n",
                    "(events1\n" +
                            "intersect\n" +
                            "events2) order by 1 desc",
                    null,
                    true
            );
        });
    }

    @Test
    public void testInterval1() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "i\ttypeOf\n" +
                        "('1970-01-01T00:00:00.100Z', '1970-01-01T00:00:00.200Z')\tINTERVAL\n" +
                        "('1970-01-01T00:00:00.300Z', '1970-01-01T00:00:00.400Z')\tINTERVAL\n" +
                        "\tINTERVAL\n",
                "select i, typeOf(i) from ((select interval(100000,200000) i) union all (select interval(300000,400000) i) union all (select null::interval i))"
        ));
    }

    @Test
    public void testInterval2() throws Exception {
        setCurrentMicros(7 * Micros.DAY_MICROS + Micros.HOUR_MICROS); // 1970-01-07T01:00:00.000Z
        assertMemoryLeak(() -> assertSql(
                "a\tb\n" +
                        "('1970-01-07T00:00:00.000Z', '1970-01-07T23:59:59.999Z')\t('1970-01-07T00:00:00.000Z', '1970-01-07T23:59:59.999Z')\n",
                "select * from (\n" +
                        "  select today() a, yesterday() b\n" +
                        "  union all\n" +
                        "  select yesterday(), yesterday()\n" +
                        "  union all\n" +
                        "  select today() a, null b\n" +
                        ")\n" +
                        "where b = a"
        ));
    }

    @Test
    public void testInterval3() throws Exception {
        setCurrentMicros(7 * Micros.DAY_MICROS + Micros.HOUR_MICROS); // 1970-01-07T01:00:00.000Z
        assertMemoryLeak(() -> assertSql(
                "a\ta1\n" +
                        "('1970-01-08T00:00:00.000Z', '1970-01-08T23:59:59.999Z')\t('1970-01-08T00:00:00.000Z', '1970-01-08T23:59:59.999Z')\n" +
                        "('1970-01-07T00:00:00.000Z', '1970-01-07T23:59:59.999Z')\t('1970-01-07T00:00:00.000Z', '1970-01-07T23:59:59.999Z')\n" +
                        "('1970-01-09T00:00:00.000Z', '1970-01-09T23:59:59.999Z')\t('1970-01-09T00:00:00.000Z', '1970-01-09T23:59:59.999Z')\n",
                "select * from (\n" +
                        "  select today() a\n" +
                        "  union \n" +
                        "  select yesterday()\n" +
                        "  union \n" +
                        "  select tomorrow()\n" +
                        ") a\n" +
                        "join (\n" +
                        "  select today() a\n" +
                        "  union \n" +
                        "  select yesterday()\n" +
                        "  union \n" +
                        "  select tomorrow()\n" +
                        ") b\n" +
                        "on a.a = b.a"
        ));
    }

    @Test
    public void testInterval4() throws Exception {
        setCurrentMicros(7 * Micros.DAY_MICROS + Micros.HOUR_MICROS); // 1970-01-07T01:00:00.000Z
        assertMemoryLeak(() -> assertSql(
                "a\tb\n" +
                        "('1970-01-08T00:00:00.000Z', '1970-01-08T23:59:59.999Z')\t('1970-01-07T00:00:00.000Z', '1970-01-07T23:59:59.999Z')\n" +
                        "foobar\t\n",
                "select * from (\n" +
                        "  select today() a, yesterday() b\n" +
                        "  union all\n" +
                        "  select yesterday(), yesterday()\n" +
                        "  union all\n" +
                        "  select 'foobar' a, null b\n" +
                        ")\n" +
                        "where b != a"
        ));
    }

    @Test
    public void testLong256Long256() throws Exception {
        testUnionAll(
                "a\tn\ttypeOf\n" +
                        "-4472\t0x4cdfb9e29522133c87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde\tLONG256\n" +
                        "-11657\t0x9840ad8800156d26c718ab5cbb3fd261c1bf6c24be53876861b1a0b0a5595515\tLONG256\n" +
                        "18351\t0x5b9832d4b5522a9474ce62a98a4516952705e02c613acfc405374f5fbcef4819\tLONG256\n" +
                        "21558\t0x36ee542d654d22598a538661f350d0b46f06560981acb5496adc00ebd29fdd53\tLONG256\n" +
                        "13182\t0x63eb3740c80f661e9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b\tLONG256\n" +
                        "-1148479920\t0x72a215ba0462ad159f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee\tLONG256\n" +
                        "-948263339\t0xe8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217965d4c984f0ffa8a\tLONG256\n" +
                        "-1191262516\t0xc72bfc5230158059980eca62a219a0f16846d7a3aa5aecce322a2198864beb14\tLONG256\n" +
                        "1545253512\t0x4b0f595f143e5d722f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4\tLONG256\n" +
                        "1530831067\t0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tLONG256\n",
                "select a, n, typeOf(n) from (x union all y)",
                "create table x as (select rnd_short() a, rnd_long256() n from long_sequence(5))",
                "create table y as (select rnd_int() a, rnd_long256() n from long_sequence(5))"
        );
    }

    @Test
    public void testLong256Null() throws Exception {
        testUnionAllWithNull(
                "a\tc\n" +
                        "0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\t\n" +
                        "0xa0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88c8b1863d4316f9c7\t\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\t\n" +
                        "0x523eb59d99c647af9840ad8800156d26c718ab5cbb3fd261c1bf6c24be538768\t\n" +
                        "0x5b9832d4b5522a9474ce62a98a4516952705e02c613acfc405374f5fbcef4819\t\n" +
                        "\t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\n" +
                        "\t0xb5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa65572a215ba0462ad15\n" +
                        "\t0x322a2198864beb14797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fa\n" +
                        "\t0xc1e631285c1ab288c72bfc5230158059980eca62a219a0f16846d7a3aa5aecce\n" +
                        "\t0x4b0f595f143e5d722f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4\n",
                "rnd_long256()"
        );
    }

    @Test
    public void testLongBin() throws Exception {
        assertFailure("create table x as (select rnd_bin(10, 24, 1) a from long_sequence(5))", "create table y as (select rnd_long() b from long_sequence(5))", 0);
    }

    @Test
    public void testLongBool() throws Exception {
        // this is cast to STRING, both columns
        testUnionAll(
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
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))",
                false
        );

        testUnion(
                "a\n" +
                        "false\n" +
                        "true\n" +
                        "4689592037643856\n" +
                        "4729996258992366\n" +
                        "7746536061816329025\n" +
                        "-6945921502384501475\n" +
                        "8260188555232587029\n"
        );
    }

    @Test
    public void testLongByte() throws Exception {
        testUnionAll(
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
                "create table x as (select rnd_byte() a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testLongChar() throws Exception {
        testUnionAll(
                "a\n" +
                        "0\n" +
                        "3\n" +
                        "5\n" +
                        "9\n" +
                        "8\n" +
                        "4689592037643856\n" +
                        "4729996258992366\n" +
                        "7746536061816329025\n" +
                        "-6945921502384501475\n" +
                        "8260188555232587029\n",
                "create table x as (select '0'::char a union select '3'::char union select '5'::char union select '9'::char union select '8'::char)",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testLongInt() throws Exception {
        testUnionAll(
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
                "create table x as (select rnd_int() a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testLongLong256() throws Exception {
        testUnionAll(
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
                "create table x as (select rnd_long256() a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testLongNull() throws Exception {
        testUnionAllWithNull(
                "a\tc\n" +
                        "8920866532787660373\tnull\n" +
                        "-7611843578141082998\tnull\n" +
                        "-5354193255228091881\tnull\n" +
                        "-2653407051020864006\tnull\n" +
                        "-1675638984090602536\tnull\n" +
                        "null\t4689592037643856\n" +
                        "null\t4729996258992366\n" +
                        "null\t7746536061816329025\n" +
                        "null\t-6945921502384501475\n" +
                        "null\t8260188555232587029\n",
                "rnd_long()"
        );
    }

    @Test
    public void testLongShort() throws Exception {
        testUnionAll(
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
                "create table x as (select rnd_short() a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testLongSymbol() throws Exception {
        testUnionAll(
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
                "create table x as (select rnd_symbol('aaa', 'bbb') a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))",
                false
        );

        testUnion(
                "a\n" +
                        "bbb\n" +
                        "aaa\n" +
                        "4689592037643856\n" +
                        "4729996258992366\n" +
                        "7746536061816329025\n" +
                        "-6945921502384501475\n" +
                        "8260188555232587029\n"
        );
    }

    @Test
    public void testLongTimestamp() throws Exception {
        assertFailure(
                "create table x as (select cast(rnd_long() as timestamp) b from long_sequence(5))",
                "create table y as (select rnd_long() a from long_sequence(5))",
                12
        );
    }

    @Test
    public void testShortBool() throws Exception {
        // this is cast to STRING, both columns
        testUnionAll(
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
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                "create table y as (select rnd_short() b from long_sequence(5))",
                false
        );

        testUnion(
                "a\n" +
                        "false\n" +
                        "true\n" +
                        "-27056\n" +
                        "24814\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-21227\n"
        );
    }

    @Test
    public void testShortByte() throws Exception {
        testUnionAll(
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
                "create table x as (select rnd_short() a from long_sequence(5))",
                "create table y as (select rnd_byte() b from long_sequence(5))"
        );
    }

    @Test
    public void testShortChar() throws Exception {
        testUnionAll(
                "a\n" +
                        "0\n" +
                        "3\n" +
                        "5\n" +
                        "9\n" +
                        "8\n" +
                        "-27056\n" +
                        "24814\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-21227\n",
                "create table x as (select '0'::char a union select '3'::char union select '5'::char union select '9'::char union select '8'::char)",
                "create table y as (select rnd_short() b from long_sequence(5))"
        );
    }

    @Test
    public void testShortNull() throws Exception {
        testUnionAllWithNull(
                "a\tc\n" +
                        "-22955\t0\n" +
                        "-1398\t0\n" +
                        "21015\t0\n" +
                        "30202\t0\n" +
                        "-19496\t0\n" +
                        "0\t-27056\n" +
                        "0\t24814\n" +
                        "0\t-11455\n" +
                        "0\t-13027\n" +
                        "0\t-21227\n",
                "rnd_short()"
        );
    }

    @Test
    public void testStrGeoByte() throws Exception {
        testUnionAll(
                "a\n" +
                        "x\n" +
                        "x\n" +
                        "x\n" +
                        "x\n" +
                        "x\n" +
                        "9\n" +
                        "4\n" +
                        "j\n" +
                        "z\n" +
                        "h\n",
                "create table x as (select 'xkl921' a from long_sequence(5))",
                "create table y as (select rnd_geohash(5) b from long_sequence(5))",
                false
        );
    }

    @Test
    public void testStrGeoInt() throws Exception {
        testUnionAll(
                "a\n" +
                        "fgert9\n" +
                        "fgert9\n" +
                        "fgert9\n" +
                        "fgert9\n" +
                        "fgert9\n" +
                        "9v1s8h\n" +
                        "46swgj\n" +
                        "jnw97u\n" +
                        "zfuqd3\n" +
                        "hp4muv\n",
                "create table x as (select 'fgert930' a from long_sequence(5))",
                "create table y as (select rnd_geohash(30) b from long_sequence(5))",
                false
        );
    }

    @Test
    public void testStrGeoLong() throws Exception {
        testUnionAll(
                "a\n" +
                        "kjhgt66s\n" +
                        "kjhgt66s\n" +
                        "kjhgt66s\n" +
                        "kjhgt66s\n" +
                        "kjhgt66s\n" +
                        "9v1s8hm7\n" +
                        "46swgj10\n" +
                        "jnw97u4y\n" +
                        "zfuqd3bf\n" +
                        "hp4muv5t\n",
                "create table x as (select 'kjhgt66srs' a from long_sequence(5))",
                "create table y as (select rnd_geohash(40) b from long_sequence(5))",
                false
        );
    }

    @Test
    public void testStrGeoShort() throws Exception {
        testUnionAll(
                "a\n" +
                        "xk\n" +
                        "xk\n" +
                        "xk\n" +
                        "xk\n" +
                        "xk\n" +
                        "9v\n" +
                        "46\n" +
                        "jn\n" +
                        "zf\n" +
                        "hp\n",
                "create table x as (select 'xkl921' a from long_sequence(5))",
                "create table y as (select rnd_geohash(10) b from long_sequence(5))",
                false
        );
    }

    @Test
    public void testStrStr() throws Exception {
        // we include byte <-> bool cast to make sure
        // bool <-> bool cast it not thrown away as redundant
        testUnionAll(
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
                "create table x as (select rnd_boolean() a, rnd_str(3,3,1) c from long_sequence(5))",
                "create table y as (select rnd_byte() b, rnd_str(4,4,1) c from long_sequence(5))"
        );
    }

    @Test
    public void testStringIPv4() throws Exception {
        testUnionAll(
                "b\n" +
                        "JWCPSWHYR\n" +
                        "EHNRX\n" +
                        "SXUXI\n" +
                        "TGPGW\n" +
                        "YUDEYYQEHB\n" +
                        "101.77.34.89\n" +
                        "66.56.51.126\n" +
                        "74.188.217.59\n" +
                        "249.60.8.8\n" +
                        "230.202.108.161\n",
                "create table y as (select rnd_ipv4() a from long_sequence(5))",
                "create table x as (select rnd_str() b from long_sequence(5))",
                true
        );
    }

    @Test
    public void testStringNull() throws Exception {
        testUnionAllWithNull(
                "a\tc\n" +
                        "\t\n" +
                        "ZSX\t\n" +
                        "XIB\t\n" +
                        "TGP\t\n" +
                        "WFF\t\n" +
                        "\tTJW\n" +
                        "\t\n" +
                        "\tSWH\n" +
                        "\tRXP\n" +
                        "\tHNR\n",
                "rnd_str(3,3,1)",
                false
        );

        testUnionWithNull(
                "a\tc\n" +
                        "\t\n" +
                        "ZSX\t\n" +
                        "XIB\t\n" +
                        "TGP\t\n" +
                        "WFF\t\n" +
                        "\tTJW\n" +
                        "\tSWH\n" +
                        "\tRXP\n" +
                        "\tHNR\n"
        );
    }

    @Test
    public void testSymBin() throws Exception {
        assertFailure("create table x as (select rnd_symbol('aa','bb') a from long_sequence(5))", "create table y as (select rnd_bin(10, 24, 1) b from long_sequence(5))", 12);
    }

    @Test
    public void testSymNull() throws Exception {
        testUnionAllWithNull("a\tc\n" +
                        "bb\t\n" +
                        "aa\t\n" +
                        "bb\t\n" +
                        "aa\t\n" +
                        "aa\t\n" +
                        "\taa\n" +
                        "\taa\n" +
                        "\tbb\n" +
                        "\tbb\n" +
                        "\tbb\n",
                "rnd_symbol('aa','bb')",
                false
        );

        testUnionWithNull(
                "a\tc\n" +
                        "bb\t\n" +
                        "aa\t\n" +
                        "\taa\n" +
                        "\tbb\n"
        );
    }

    @Test
    public void testSymSym() throws Exception {
        // we include byte <-> bool cast to make sure
        // sym <-> sym cast it not thrown away as redundant
        testUnionAll(
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
                "create table y as (select rnd_byte() a, rnd_symbol('x','y') c from long_sequence(5))"
        );
    }

    @Test
    public void testSymSymErr() throws Exception {
        // we include byte <-> bool cast to make sure
        // sym <-> sym cast it not thrown away as redundant
        testUnionAll(
                "a\tc\ttypeOf\n" +
                        "false\tb\tSTRING\n" +
                        "false\ta\tSTRING\n" +
                        "false\tb\tSTRING\n" +
                        "true\tb\tSTRING\n" +
                        "false\ta\tSTRING\n" +
                        "27\ty\tSTRING\n" +
                        "122\ty\tSTRING\n" +
                        "84\tx\tSTRING\n" +
                        "83\tx\tSTRING\n" +
                        "91\tx\tSTRING\n",
                "select a, c, typeOf(c) from (x union all y)",
                // column "u" is not ultimately selected from neither X nor Y
                // we expect this column to be ignored by optimiser, and also
                // we expect optimiser to correctly select column "b" from Y as
                // a match against column "a" in the union
                "create table x as (select rnd_double() u, rnd_boolean() a, rnd_symbol('a','b') c from long_sequence(5))",
                "create table y as (select rnd_double() u, rnd_byte() b, rnd_symbol('x','y') c from long_sequence(5))"
        );
    }

    @Test
    public void testSymSymPickColumnFromWhere() throws Exception {
        // we include byte <-> bool cast to make sure
        // sym <-> sym cast it not thrown away as redundant
        // column "u" is not ultimately selected from neither X nor Y
        // we expect this column to be ignored by optimiser, and also
        // we expect optimiser to correctly select column "b" from Y as
        // a match against column "a" in the union
        execute("create table y as (select rnd_double() u, rnd_byte() b, rnd_symbol('x','y') c from long_sequence(5))");
        engine.releaseAllWriters();
        assertQuery(
                "u\ta\tc\n" +
                        "0.6607777894187332\t27\ty\n",
                "(x union all y) where a = '27'", "create table x as (select rnd_double() u, rnd_boolean() a, rnd_symbol('a','b') c from long_sequence(5))",
                null,
                false,
                false
        );
    }

    @Test
    public void testTimestampBool() throws Exception {
        // this is cast to STRING, both columns
        testUnionAll(
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
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                "create table y as (select rnd_timestamp(0, 100000, 2) b from long_sequence(5))",
                false
        );

        testUnion(
                "a\n" +
                        "true\n" +
                        "false\n" +
                        "1970-01-01T00:00:00.002771Z\n" +
                        "\n" +
                        "1970-01-01T00:00:00.045299Z\n" +
                        "1970-01-01T00:00:00.078334Z\n"
        );
    }

    @Test
    public void testTimestampByte() throws Exception {
        assertFailure(
                "create table x as (select cast(rnd_long() as timestamp) a from long_sequence(5))",
                "create table y as (select rnd_byte() b from long_sequence(5))",
                12
        );
    }

    @Test
    public void testTimestampChar() throws Exception {
        assertFailure(
                "create table x as (select cast(rnd_long() as timestamp) a from long_sequence(5))",
                "create table y as (select rnd_char() b from long_sequence(5))",
                12
        );
    }

    @Test
    public void testTimestampFloat() throws Exception {
        assertFailure(
                "create table x as (select cast(rnd_long() as timestamp) a from long_sequence(5))",
                "create table y as (select rnd_float() b from long_sequence(5))",
                0
        );
    }

    @Test
    public void testTimestampInt() throws Exception {
        assertFailure(
                "create table x as (select cast(rnd_long() as timestamp) a from long_sequence(5))",
                "create table y as (select rnd_int() b from long_sequence(5))",
                12
        );
    }

    @Test
    public void testTimestampLong() throws Exception {
        assertFailure(
                "create table x as (select cast(rnd_long() as timestamp) a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))",
                12
        );
    }

    @Test
    public void testTimestampMicrosTimestampNanos() throws Exception {
        testUnionAll(
                "a\n" +
                        "1970-01-01T00:00:00.000001000Z\n" +
                        "1970-01-01T00:00:00.000002000Z\n" +
                        "1970-01-01T00:00:00.000003000Z\n" +
                        "1970-01-01T00:00:00.000004000Z\n" +
                        "1970-01-01T00:00:00.000005000Z\n" +
                        "1970-01-01T00:00:00.000000001Z\n" +
                        "1970-01-01T00:00:00.000000002Z\n" +
                        "1970-01-01T00:00:00.000000003Z\n" +
                        "1970-01-01T00:00:00.000000004Z\n" +
                        "1970-01-01T00:00:00.000000005Z\n",
                "create table x as (select cast(x as timestamp) a from long_sequence(5))",
                "create table y as (select cast(x as timestamp_ns) a from long_sequence(5))",
                true
        );
    }

    @Test
    public void testTimestampNanosTimestampMicros() throws Exception {
        testUnionAll(
                "a\n" +
                        "1970-01-01T00:00:00.000000001Z\n" +
                        "1970-01-01T00:00:00.000000002Z\n" +
                        "1970-01-01T00:00:00.000000003Z\n" +
                        "1970-01-01T00:00:00.000000004Z\n" +
                        "1970-01-01T00:00:00.000000005Z\n" +
                        "1970-01-01T00:00:00.000001000Z\n" +
                        "1970-01-01T00:00:00.000002000Z\n" +
                        "1970-01-01T00:00:00.000003000Z\n" +
                        "1970-01-01T00:00:00.000004000Z\n" +
                        "1970-01-01T00:00:00.000005000Z\n",
                "create table x as (select cast(x as timestamp_ns) a from long_sequence(5))",
                "create table y as (select cast(x as timestamp) a from long_sequence(5))",
                true
        );
    }

    @Test
    public void testTimestampNull() throws Exception {
        testUnionAllWithNull(
                "a\tc\n" +
                        "1970-01-01T00:00:00.023853Z\t\n" +
                        "1970-01-01T00:00:00.083620Z\t\n" +
                        "1970-01-01T00:00:00.084025Z\t\n" +
                        "\t\n" +
                        "1970-01-01T00:00:00.008228Z\t\n" +
                        "\t1970-01-01T00:00:00.002771Z\n" +
                        "\t\n" +
                        "\t1970-01-01T00:00:00.045299Z\n" +
                        "\t1970-01-01T00:00:00.078334Z\n" +
                        "\t\n",
                "rnd_timestamp(0, 100000, 2)",
                false
        );

        testUnionWithNull(
                "a\tc\n" +
                        "1970-01-01T00:00:00.023853Z\t\n" +
                        "1970-01-01T00:00:00.083620Z\t\n" +
                        "1970-01-01T00:00:00.084025Z\t\n" +
                        "\t\n" +
                        "1970-01-01T00:00:00.008228Z\t\n" +
                        "\t1970-01-01T00:00:00.002771Z\n" +
                        "\t1970-01-01T00:00:00.045299Z\n" +
                        "\t1970-01-01T00:00:00.078334Z\n"
        );
    }

    @Test
    public void testTimestampShort() throws Exception {
        assertFailure(
                "create table x as (select cast(rnd_long() as timestamp) a from long_sequence(5))",
                "create table y as (select rnd_short() b from long_sequence(5))",
                12
        );
    }

    @Test
    public void testUuidNull() throws Exception {
        testUnionAllWithNull(
                "a\tc\n" +
                        "322a2198-864b-4b14-b97f-a69eb8fec6cc\t\n" +
                        "980eca62-a219-40f1-a846-d7a3aa5aecce\t\n" +
                        "c1e63128-5c1a-4288-872b-fc5230158059\t\n" +
                        "716de3d2-5dcc-4d91-9fa2-397a5d8c84c4\t\n" +
                        "4b0f595f-143e-4d72-af1a-8266e7921e3b\t\n" +
                        "\t0010cde8-12ce-40ee-8010-a928bb8b9650\n" +
                        "\t9f9b2131-d49f-4d1d-ab81-39815c50d341\n" +
                        "\t7bcd48d8-c77a-4655-b2a2-15ba0462ad15\n" +
                        "\tb5b2159a-2356-4217-965d-4c984f0ffa8a\n" +
                        "\te8beef38-cd7b-43d8-9b2d-34586f6275fa\n",
                "rnd_uuid4()"
        );
    }

    @Test
    public void testUuidString() throws Exception {
        testUnionAll(
                "a\n" +
                        "7f98b0c7-4238-437e-b6ee-542d654d2259\n" +
                        "7c1b058a-f93c-4808-abaf-c47f4abcd93b\n" +
                        "63eb3740-c80f-461e-9c8a-fa23e6ca6ca1\n" +
                        "c2593f82-b430-428d-84a0-9f29df637e38\n" +
                        "58dfd08e-eb9c-439e-8ec8-2869edec121b\n" +
                        "JWCPSWHYR\n" +
                        "EHNRX\n" +
                        "SXUXI\n" +
                        "TGPGW\n" +
                        "YUDEYYQEHB\n",
                "create table x as (select rnd_uuid4() a from long_sequence(5))",
                "create table y as (select rnd_str() b from long_sequence(5))",
                true
        );
    }

    @Test
    public void testUuidUuid() throws Exception {
        testUnionAll(
                "a\n" +
                        "0010cde8-12ce-40ee-8010-a928bb8b9650\n" +
                        "9f9b2131-d49f-4d1d-ab81-39815c50d341\n" +
                        "7bcd48d8-c77a-4655-b2a2-15ba0462ad15\n" +
                        "b5b2159a-2356-4217-965d-4c984f0ffa8a\n" +
                        "e8beef38-cd7b-43d8-9b2d-34586f6275fa\n" +
                        "322a2198-864b-4b14-b97f-a69eb8fec6cc\n" +
                        "980eca62-a219-40f1-a846-d7a3aa5aecce\n" +
                        "c1e63128-5c1a-4288-872b-fc5230158059\n" +
                        "716de3d2-5dcc-4d91-9fa2-397a5d8c84c4\n" +
                        "4b0f595f-143e-4d72-af1a-8266e7921e3b\n",
                "create table y as (select rnd_uuid4() b from long_sequence(5))",
                "create table x as (select rnd_uuid4() a from long_sequence(5))",
                false
        );

        testUnion(
                "a\n" +
                        "0010cde8-12ce-40ee-8010-a928bb8b9650\n" +
                        "9f9b2131-d49f-4d1d-ab81-39815c50d341\n" +
                        "7bcd48d8-c77a-4655-b2a2-15ba0462ad15\n" +
                        "b5b2159a-2356-4217-965d-4c984f0ffa8a\n" +
                        "e8beef38-cd7b-43d8-9b2d-34586f6275fa\n" +
                        "322a2198-864b-4b14-b97f-a69eb8fec6cc\n" +
                        "980eca62-a219-40f1-a846-d7a3aa5aecce\n" +
                        "c1e63128-5c1a-4288-872b-fc5230158059\n" +
                        "716de3d2-5dcc-4d91-9fa2-397a5d8c84c4\n" +
                        "4b0f595f-143e-4d72-af1a-8266e7921e3b\n"
        );
    }

    @Test
    public void testUuidVarchar() throws Exception {
        testUnionAll(
                "a\n" +
                        "acb025f7-59cf-4bd0-9e9b-e4e331fe36e6\n" +
                        "8fd449ba-3259-4a2b-9beb-329042090bb3\n" +
                        "b482cff5-7e9c-4398-ac09-f1b4db297f07\n" +
                        "dbd7587f-2077-4576-9b4b-ae41862e09cc\n" +
                        "7ee6a03f-4f93-4fa3-9d6c-b7b4fbf1fa48\n" +
                        "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\n" +
                        "8#3TsZ\n" +
                        "zV衞͛Ԉ龘и\uDA89\uDFA4~\n" +
                        "ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\n" +
                        "\uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46OF\n",
                "create table x as (select rnd_uuid4() a from long_sequence(5))",
                "create table y as (select rnd_varchar() b from long_sequence(5))",
                true
        );
    }

    @Test
    public void testVarcharNull() throws Exception {
        testUnionAllWithNull(
                "a\tc\n" +
                        "衞͛Ԉ\t\n" +
                        "\uD93C\uDEC1ӍK\t\n" +
                        "\uD905\uDCD0\\ꔰ\t\n" +
                        "\u008B}ѱ\t\n" +
                        "\uD96C\uDF5FƐ㙎\t\n" +
                        "\t\u1755\uDA1F\uDE98|\n" +
                        "\t鈄۲ӄ\n" +
                        "\tȞ鼷G\n" +
                        "\t\uF644䶓z\n" +
                        "\t\n",
                "rnd_varchar(3,3,1)"
        );
    }

    @Test
    public void testVarcharVarchar() throws Exception {
        // we include byte <-> bool cast to make sure
        // bool <-> bool cast it not thrown away as redundant
        testUnionAll(
                "a\tc\n" +
                        "false\tZ끫\uDB53\uDEDA\n" +
                        "false\t\"\uDB87\uDFA35\n" +
                        "false\t톬F\uD9E6\uDECD\n" +
                        "false\tЃَᯤ\n" +
                        "false\t篸{\uD9D7\uDFE5\n" +
                        "76\t핕\u05FA씎鈄\n" +
                        "21\t\uDB8C\uDD1BȞ鼷G\n" +
                        "35\t\uD8D1\uDD54ZzV\n" +
                        "117\tB͛Ԉ龘\n" +
                        "103\tL➤~2\n",
                "create table x as (select rnd_boolean() a, rnd_varchar(3,3,1) c from long_sequence(5))",
                "create table y as (select rnd_byte() b, rnd_varchar(4,4,1) c from long_sequence(5))"
        );
    }

    private static void testUnionAllWithNull(String expected, String function) throws Exception {
        testUnionAllWithNull(expected, function, true);
    }

    private static void testUnionAllWithNull(String expected, String function, boolean testUnion) throws Exception {
        execute("create table y as (select " + function + " c from long_sequence(5))");
        execute("create table x as (select " + function + " a from long_sequence(5))");
        engine.releaseAllWriters();

        assertQuery(
                expected,
                "(select a, null c from x) union all (select null b, c from y)",
                null,
                null,
                false,
                true
        );

        if (testUnion) {
            testUnionWithNull(expected);
        }
    }

    private static void testUnionWithNull(String expected) throws Exception {
        assertQuery(
                expected,
                "(select a, null c from x) union (select null b, c from y)",
                null,
                null,
                false,
                false
        );
    }

    private void assertFailure(String ddlX, String ddlY, int pos) throws Exception {
        execute(ddlY);
        engine.releaseAllWriters();
        assertException(
                "x union all y",
                ddlX,
                pos,
                "unsupported cast"
        );
    }

    private void testUnion(String expected) throws Exception {
        assertQuery(expected, "x union y", null, null, false, false);
    }

    private void testUnionAll(String expected, String ddlX, String ddlY) throws Exception {
        testUnionAll(expected, ddlX, ddlY, true);
    }

    private void testUnionAll(String expected, String ddlX, String ddlY, boolean testUnion) throws Exception {
        testUnionAll(
                expected,
                "x union all y",
                ddlX,
                ddlY
        );

        if (testUnion) {
            testUnion(expected);
        }
    }

    private void testUnionAll(String expected, String sql, String ddlX, String ddlY) throws Exception {
        execute(ddlY);
        engine.releaseAllWriters();
        assertQuery(expected, sql, ddlX, null, false, true);
    }
}
