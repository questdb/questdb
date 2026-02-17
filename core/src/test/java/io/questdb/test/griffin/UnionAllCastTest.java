/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
                """
                        a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tp\tr\ts
                        true\t37\tO\t10549\t171760612\t8503557900983561786\t0.7586254\t0.5913874468544745\t1970-01-01T00:05:56.479Z\t1970-01-01T00:00:00.000001Z\t0x434524824ca84f523ed391560ac327544a27205d291d7f124c83d07de0778e77\t00000000 29 26 c5 aa da 18 ce 5f b2 8b\t101000\t11000111111100\t6jq4\tn3ub6zju\tCKFMQ\tᣮաf@ץ
                        false\t113\tY\t21020\t-1915752164\t5922689877598858022\t0.43461353\t0.7195457109208119\t1970-01-01T00:40:51.578Z\t1970-01-01T00:00:00.000000Z\t0x20cfa22cd22bf054483c83d88ac674e3894499a1a1680580cfedff23a67d918f\t00000000 07 b1 32 57 ff 9a ef 88 cb 4b\t001011\t01000101000010\txf81\tcbj71euv\tLNYRZ\t9іa\uDA76\uDDD4*
                        true\t62\tP\t13143\t-770962341\t-4036499202601723677\t0.84029645\t0.5794665369115236\t1970-01-01T02:26:24.738Z\t1970-01-01T00:00:00.000000Z\t0x7f19777ec13680558a2d082bfad3aa844a20938221fd7f431bd29676f6902e64\t00000000 dd 44 11 e2 a3 24 4e 44 a8 0d\t001110\t10101011111110\tnz3p\tz1rvm419\tGIJYD\t
                        false\t79\tN\t-29677\t-8264817\t-9118587154366063429\t0.6397125\t0.798471808479839\t1970-01-01T00:02:12.849Z\t1970-01-01T00:00:00.000001Z\t0xb19ddb7ff5abcafec82c35a389f834dababcd0482f05618f926cdd99e63abb35\t\t001001\t11010000011000\tgj7w\txd3qr0fm\tQEMXD\t
                        false\t123\tJ\t-4254\t-735934368\t8384866408379441071\t0.52348924\t0.5778817852306684\t1970-01-01T01:12:18.900Z\t\t0x705380a68d53d93c2692845742d6674742bdf2c301f7f43b9747f18a1ed2ef46\t\t010001\t01000000100001\tz403\trcc9nh2x\tDHHGG\tX\uDA8B\uDFC4︵Ƀ^
                        false\t102\tJ\t-13027\t73575701\t8920866532787660373\t0.29919904\t0.0843832076262595\t1970-01-01T00:10:02.536Z\t1970-01-01T00:00:00.000000Z\t0xc1e631285c1ab288c72bfc5230158059980eca62a219a0f16846d7a3aa5aecce\t00000000 91 3b 72 db f3 04 1b c7 88 de\t110110\t11001100100010\txn8n\t0n2gm6r7\tHFOWL\t͛Ԉ龘и\uDA89\uDFA4
                        true\t62\tL\t5639\t-1418341054\t3152466304308949756\t0.76642567\t0.4138164748227684\t1970-01-01T02:34:54.347Z\t\t0x6698c6c186b7571a9cba3ef59083484d98c2d832d83de9934a0705e1136e872b\t00000000 78 b5 b9 11 53 d0 fb 64 bb 1a\t011110\t01011100110001\tqytg\t7kfnr23n\tWEKGH\t\uDB8D\uDE4Eᯤ\\篸{
                        false\t33\tL\t-20409\t-712702244\t-6190031864817509934\t0.58112466\t0.4971342426836798\t1970-01-01T01:17:56.168Z\t\t0x4b0a72b3339b8c7c1872e79ea10322460cb5f439cbc22e9d1f0481ab7acd1f4a\t\t101010\t10101111000001\tttz9\tnzxtf741\t\t|\\軦۽㒾
                        true\t92\tP\t4215\t-889224806\t8889492928577876455\t0.069444776\t0.6697969295620055\t1970-01-01T00:02:10.728Z\t1970-01-01T00:00:00.000001Z\t0x3d9491e7e14eba8e1de93a9cf1483e290ec6c3651b1c029f825c96def9f2fcc2\t\t010111\t10110011101001\tvgyb\tg6mmvcdb\tZRMFM\t
                        true\t107\tG\t-5240\t-1121895896\t-2000273984235276379\t0.18336213\t0.2711532808184136\t1970-01-01T00:55:56.811Z\t1970-01-01T00:00:00.000001Z\t0x76ffd1a81bf39767b92d0771d78263eb5479ae0482582ad03c84de8f7bd9235d\t00000000 a7 6a 71 34 e0 b0 e9 98 f7 67\t100011\t00001000010100\tvs9s\tn0vjumxz\tLDGLO\ṱ\uD8F2\uDE8E>\uDAE6\uDEE3g
                        """,
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
                """
                        a\tn\ttypeOf
                        32312\t\tBINARY
                        4635\t00000000 f4 c8 39 09 fe d8 9d 30 78 36\tBINARY
                        -22934\t00000000 de e4 7c d2 35 07 42 fc 31 79\tBINARY
                        22367\t\tBINARY
                        -12671\t\tBINARY
                        -1148479920\t00000000 41 1d 15 55 8a 17 fa d8 cc 14\tBINARY
                        -1436881714\t\tBINARY
                        806715481\t00000000 c4 91 3b 72 db f3 04 1b c7 88\tBINARY
                        -1432278050\t00000000 79 3c 77 15 68 61 26 af 19 c4\tBINARY
                        -1975183723\t00000000 36 53 49 b4 59 7e 3b 08 a1 1e\tBINARY
                        """,
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
                """
                        a\tc
                        false\tfalse
                        false\ttrue
                        true\ttrue
                        true\tfalse
                        false\tfalse
                        76\tfalse
                        27\ttrue
                        79\tfalse
                        122\ttrue
                        90\ttrue
                        """,
                "create table x as (select rnd_boolean() a, rnd_boolean() c from long_sequence(5))",
                "create table y as (select rnd_byte() b, rnd_boolean() c from long_sequence(5))",
                false
        );

        testUnion(
                """
                        a\tc
                        false\tfalse
                        false\ttrue
                        true\ttrue
                        true\tfalse
                        76\tfalse
                        27\ttrue
                        79\tfalse
                        122\ttrue
                        90\ttrue
                        """
        );
    }

    @Test
    public void testBoolNull() throws Exception {
        testUnionAllWithNull(
                """
                        a\tc
                        false\tfalse
                        true\tfalse
                        true\tfalse
                        true\tfalse
                        true\tfalse
                        false\tfalse
                        false\tfalse
                        false\tfalse
                        false\ttrue
                        false\tfalse
                        """,
                "rnd_boolean()",
                false
        );

        testUnionWithNull(
                """
                        a\tc
                        false\tfalse
                        true\tfalse
                        false\ttrue
                        """
        );
    }

    @Test
    public void testByteBool() throws Exception {
        // this is cast to STRING, both columns
        testUnionAll(
                """
                        a
                        false
                        true
                        true
                        true
                        true
                        76
                        102
                        27
                        87
                        79
                        """,
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                "create table y as (select rnd_byte() b from long_sequence(5))",
                false
        );

        testUnion(
                """
                        a
                        false
                        true
                        76
                        102
                        27
                        87
                        79
                        """
        );
    }

    @Test
    public void testByteByte() throws Exception {
        // we include byte <-> bool cast to make sure
        // byte <-> byte cast it not thrown away as redundant
        testUnionAll(
                """
                        a\tc
                        false\t84
                        false\t55
                        true\t88
                        true\t21
                        false\t74
                        76\t102
                        27\t87
                        79\t79
                        122\t83
                        90\t76
                        """,
                "create table x as (select rnd_boolean() a, rnd_byte() c from long_sequence(5))",
                "create table y as (select rnd_byte() b, rnd_byte() c from long_sequence(5))"
        );
    }

    @Test
    public void testByteChar() throws Exception {
        testUnionAll(
                """
                        a
                        79
                        122
                        83
                        90
                        76
                        V
                        T
                        J
                        W
                        C
                        """,
                "create table x as (select rnd_byte() a from long_sequence(5))",
                "create table y as (select rnd_char() b from long_sequence(5))"
        );
    }

    @Test
    public void testByteDecimal() throws Exception {
        testUnionAll(
                """
                        a
                        1.000
                        2.000
                        3.000
                        4.000
                        5.000
                        0.000
                        3.000
                        5.000
                        9.000
                        8.000
                        """,
                "create table x as (select x::decimal(19, 3) a from long_sequence(5))",
                "create table y as (select 0::byte a union select 3::byte union select 5::byte union select 9::byte union select 8::byte)",
                false
        );
    }

    @Test
    public void testByteNull() throws Exception {
        testUnionAllWithNull(
                """
                        a\tc
                        79\t0
                        122\t0
                        83\t0
                        90\t0
                        76\t0
                        0\t76
                        0\t102
                        0\t27
                        0\t87
                        0\t79
                        """,
                "rnd_byte()"
        );
    }

    @Test
    public void testCharBool() throws Exception {
        // this is cast to STRING, both columns
        testUnionAll(
                """
                        a
                        false
                        true
                        true
                        true
                        true
                        V
                        T
                        J
                        W
                        C
                        """,
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                "create table y as (select rnd_char() b from long_sequence(5))",
                false
        );
        testUnion(
                """
                        a
                        false
                        true
                        V
                        T
                        J
                        W
                        C
                        """
        );
    }

    @Test
    public void testCharByte() throws Exception {
        testUnionAll(
                """
                        a
                        P
                        S
                        W
                        H
                        Y
                        76
                        102
                        27
                        87
                        79
                        """,
                "create table x as (select rnd_char() a from long_sequence(5))",
                "create table y as (select rnd_byte() b from long_sequence(5))"
        );
    }

    @Test
    public void testCharNull() throws Exception {
        testUnionAllWithNull(
                """
                        a\tc
                        P\t
                        S\t
                        W\t
                        H\t
                        Y\t
                        \tV
                        \tT
                        \tJ
                        \tW
                        \tC
                        """,
                "rnd_char()"
        );
    }

    @Test
    public void testCharShort() throws Exception {
        testUnionAll(
                """
                        a
                        -27056
                        24814
                        -11455
                        -13027
                        -21227
                        0
                        3
                        5
                        9
                        8
                        """,
                "create table x as (select rnd_short() a from long_sequence(5))",
                "create table y as (select '0'::char a union select '3'::char union select '5'::char union select '9'::char union select '8'::char)"
        );
    }

    @Test
    public void testDateBool() throws Exception {
        // this is cast to STRING, both columns
        testUnionAll(
                """
                        a
                        false
                        true
                        true
                        true
                        true
                        1970-01-01T02:07:23.856Z
                        1970-01-01T02:29:52.366Z
                        1970-01-01T01:45:29.025Z
                        1970-01-01T01:15:01.475Z
                        1970-01-01T00:43:07.029Z
                        """,
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                "create table y as (select rnd_date() b from long_sequence(5))",
                false
        );
        testUnion(
                """
                        a
                        false
                        true
                        1970-01-01T02:07:23.856Z
                        1970-01-01T02:29:52.366Z
                        1970-01-01T01:45:29.025Z
                        1970-01-01T01:15:01.475Z
                        1970-01-01T00:43:07.029Z
                        """
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
                """
                        a\tc
                        1970-01-01T02:07:40.373Z\t
                        1970-01-01T00:18:02.998Z\t
                        1970-01-01T02:14:51.881Z\t
                        1970-01-01T00:14:24.006Z\t
                        1970-01-01T00:10:02.536Z\t
                        \t1970-01-01T02:07:23.856Z
                        \t1970-01-01T02:29:52.366Z
                        \t1970-01-01T01:45:29.025Z
                        \t1970-01-01T01:15:01.475Z
                        \t1970-01-01T00:43:07.029Z
                        """,
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
                """
                        a\ttypeOf
                        1970-01-01T00:14:24.006000Z\tTIMESTAMP
                        1970-01-01T00:10:02.536000Z\tTIMESTAMP
                        1970-01-01T00:04:41.932000Z\tTIMESTAMP
                        1970-01-01T00:01:52.276000Z\tTIMESTAMP
                        1970-01-01T00:32:57.934000Z\tTIMESTAMP
                        1970-01-01T00:00:00.002771Z\tTIMESTAMP
                        \tTIMESTAMP
                        1970-01-01T00:00:00.045299Z\tTIMESTAMP
                        1970-01-01T00:00:00.078334Z\tTIMESTAMP
                        \tTIMESTAMP
                        """,
                "select a, typeOf(a) from (x union all y)",
                "create table x as (select rnd_date() a from long_sequence(5))",
                "create table y as (select rnd_timestamp(0, 100000, 1) b from long_sequence(5))"
        );
    }

    @Test
    public void testDecimal128Decimal128() throws Exception {
        testUnionAll(
                """
                        a
                        1.000
                        2.000
                        3.000
                        4.000
                        5.000
                        0.000
                        3.000
                        5.000
                        9.000
                        8.000
                        """,
                "create table x as (select x::decimal(36, 3) a from long_sequence(5))",
                "create table y as (select 0::decimal(33, 0) a union select 3::decimal(33, 0) union select 5::decimal(33, 0) union select 9::decimal(33, 0) union select 8::decimal(33, 0))",
                false
        );
    }

    @Test
    public void testDecimal16Decimal16() throws Exception {
        testUnionAll(
                """
                        a
                        1.0
                        2.0
                        3.0
                        4.0
                        5.0
                        0.0
                        3.0
                        5.0
                        9.0
                        8.0
                        """,
                "create table x as (select x::decimal(4, 1) a from long_sequence(5))",
                "create table y as (select 0::decimal(3, 0) a union select 3::decimal(3, 0) union select 5::decimal(3, 0) union select 9::decimal(3, 0) union select 8::decimal(3, 0))",
                false
        );
    }

    @Test
    public void testDecimal256Decimal256() throws Exception {
        testUnionAll(
                """
                        a
                        1.000000
                        2.000000
                        3.000000
                        4.000000
                        5.000000
                        0.000000
                        3.000000
                        5.000000
                        9.000000
                        8.000000
                        """,
                "create table x as (select x::decimal(56, 6) a from long_sequence(5))",
                "create table y as (select 0::decimal(56, 2) a union select 3::decimal(56, 2) union select 5::decimal(56, 2) union select 9::decimal(56, 2) union select 8::decimal(56, 2))",
                false
        );
    }

    @Test
    public void testDecimal32Decimal32() throws Exception {
        testUnionAll(
                """
                        a
                        1.0
                        2.0
                        3.0
                        4.0
                        5.0
                        0.0
                        3.0
                        5.0
                        9.0
                        8.0
                        """,
                "create table x as (select x::decimal(8, 1) a from long_sequence(5))",
                "create table y as (select 0::decimal(7, 0) a union select 3::decimal(7, 0) union select 5::decimal(7, 0) union select 9::decimal(7, 0) union select 8::decimal(7, 0))",
                false
        );
    }

    @Test
    public void testDecimal64Decimal64() throws Exception {
        testUnionAll(
                """
                        a
                        1.0
                        2.0
                        3.0
                        4.0
                        5.0
                        0.0
                        3.0
                        5.0
                        9.0
                        8.0
                        """,
                "create table x as (select x::decimal(17, 1) a from long_sequence(5))",
                "create table y as (select 0::decimal(16, 0) a union select 3::decimal(16, 0) union select 5::decimal(16, 0) union select 9::decimal(16, 0) union select 8::decimal(16, 0))",
                false
        );
    }

    @Test
    public void testDecimal8Decimal8() throws Exception {
        testUnionAll(
                """
                        a
                        1.0
                        2.0
                        3.0
                        4.0
                        5.0
                        0.0
                        3.0
                        5.0
                        9.0
                        8.0
                        """,
                "create table x as (select x::decimal(2, 1) a from long_sequence(5))",
                "create table y as (select 0::decimal(1, 0) a union select 3::decimal(1, 0) union select 5::decimal(1, 0) union select 9::decimal(1, 0) union select 8::decimal(1, 0))",
                false
        );
    }

    @Test
    public void testDecimalDecimalMixedPrecisions() throws Exception {
        testUnionAll(
                """
                        a
                        1.000
                        2.000
                        3.000
                        4.000
                        5.000
                        0.000
                        3.000
                        5.000
                        9.000
                        8.000
                        """,
                "create table x as (select x::decimal(19, 3) a from long_sequence(5))",
                "create table y as (select 0::decimal(18, 3) a union select 3::decimal(18, 3) union select 5::decimal(18, 3) union select 9::decimal(18, 3) union select 8::decimal(18, 3))",
                false
        );
    }

    @Test
    public void testDecimalDecimalMixedScales() throws Exception {
        testUnionAll(
                """
                        a
                        1.00000
                        2.00000
                        3.00000
                        4.00000
                        5.00000
                        0.00000
                        3.00000
                        5.00000
                        9.00000
                        8.00000
                        """,
                "create table x as (select x::decimal(19, 3) a from long_sequence(5))",
                "create table y as (select 0::decimal(8, 1) a union select 3::decimal(8, 2) union select 5::decimal(8, 3) union select 9::decimal(8, 4) union select 8::decimal(8, 5))",
                false
        );
    }

    @Test
    public void testDecimalNull() throws Exception {
        testUnionAllWithNull(
                """
                        a\tc
                        1.000\t
                        2.000\t
                        3.000\t
                        4.000\t
                        5.000\t
                        \t1.000
                        \t2.000
                        \t3.000
                        \t4.000
                        \t5.000
                        """,
                "x::decimal(18, 3)"
        );
    }

    @Test
    public void testDecimalStr() throws Exception {
        testUnionAll(
                """
                        a
                        1.000
                        2.000
                        3.000
                        4.000
                        5.000
                        0.000
                        3.000
                        5.000
                        9.000
                        8.000
                        """,
                "create table x as (select x::string a from long_sequence(5))",
                "create table y as (select 0::decimal(18, 3) a union select 3::decimal(18, 3) union select 5::decimal(18, 3) union select 9::decimal(18, 3) union select 8::decimal(18, 3))",
                false
        );
    }

    @Test
    public void testDecimalVarchar() throws Exception {
        testUnionAll(
                """
                        a
                        1.000
                        2.000
                        3.000
                        4.000
                        5.000
                        0.000
                        3.000
                        5.000
                        9.000
                        8.000
                        """,
                "create table x as (select x::varchar a from long_sequence(5))",
                "create table y as (select 0::decimal(18, 3) a union select 3::decimal(18, 3) union select 5::decimal(18, 3) union select 9::decimal(18, 3) union select 8::decimal(18, 3))",
                false
        );
    }

    @Test
    public void testDoubleBool() throws Exception {
        // this is cast to STRING, both columns
        testUnionAll(
                """
                        a
                        false
                        false
                        false
                        true
                        true
                        0.6607777894187332
                        0.2246301342497259
                        0.08486964232560668
                        0.299199045961845
                        0.20447441837877756
                        """,
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                "create table y as (select rnd_double() b from long_sequence(5))",
                false
        );

        testUnion(
                """
                        a
                        false
                        true
                        0.6607777894187332
                        0.2246301342497259
                        0.08486964232560668
                        0.299199045961845
                        0.20447441837877756
                        """
        );
    }

    @Test
    public void testDoubleByte() throws Exception {
        testUnionAll(
                """
                        a
                        0.2845577791213847
                        0.0843832076262595
                        0.9344604857394011
                        0.13123360041292131
                        0.7905675319675964
                        76.0
                        102.0
                        27.0
                        87.0
                        79.0
                        """,
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
    public void testDoubleDecimal() throws Exception {
        testUnionAll(
                """
                        a
                        1.000
                        2.000
                        3.000
                        4.000
                        5.000
                        0.0
                        3.0
                        5.0
                        9.0
                        8.0
                        """,
                "create table x as (select x::decimal(19, 3) a from long_sequence(5))",
                "create table y as (select 0d a union select 3d union select 5d union select 9d union select 8d)",
                false
        );
    }

    @Test
    public void testDoubleInt() throws Exception {
        testUnionAll(
                """
                        a
                        0.2845577791213847
                        0.0843832076262595
                        0.9344604857394011
                        0.13123360041292131
                        0.7905675319675964
                        -1.14847992E9
                        3.15515118E8
                        1.548800833E9
                        -7.27724771E8
                        7.3575701E7
                        """,
                "create table x as (select rnd_double() a from long_sequence(5))",
                "create table y as (select rnd_int() b from long_sequence(5))"
        );
    }

    @Test
    public void testDoubleLong() throws Exception {
        testUnionAll(
                """
                        a
                        0.2845577791213847
                        0.0843832076262595
                        0.9344604857394011
                        0.13123360041292131
                        0.7905675319675964
                        4.689592037643856E15
                        4.729996258992366E15
                        7.7465360618163292E18
                        -6.9459215023845018E18
                        8.2601885552325868E18
                        """,
                "create table x as (select rnd_double() a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testDoubleNull() throws Exception {
        testUnionAllWithNull(
                """
                        a\tc
                        0.6508594025855301\tnull
                        0.8423410920883345\tnull
                        0.9856290845874263\tnull
                        0.22452340856088226\tnull
                        0.5093827001617407\tnull
                        null\t0.6607777894187332
                        null\t0.2246301342497259
                        null\t0.08486964232560668
                        null\t0.299199045961845
                        null\t0.20447441837877756
                        """,
                "rnd_double()"
        );
    }

    @Test
    public void testDoubleShort() throws Exception {
        testUnionAll(
                """
                        a
                        0.2845577791213847
                        0.0843832076262595
                        0.9344604857394011
                        0.13123360041292131
                        0.7905675319675964
                        -27056.0
                        24814.0
                        -11455.0
                        -13027.0
                        -21227.0
                        """,
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
                    """
                            events1
                            except
                            events2""",
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
                    """
                            contact\tgroupid\teventid
                            2\t1.600000023841858\tstand
                            1\t1.600000023841858\tstand
                            """,
                    """
                            (events1
                            except
                            events2) order by 1 desc""",
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
                    """
                            events1
                            except
                            events2""",
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
                    """
                            contact\tgroupid\teventid
                            2\t1.6\tstand
                            1\t1.6\tstand
                            """,
                    """
                            (events1
                            except
                            events2) order by 1 desc""",
                    null,
                    true
            );
        });
    }

    @Test
    public void testFloatBool() throws Exception {
        // this is cast to STRING, both columns
        testUnionAll(
                """
                        a
                        false
                        true
                        true
                        true
                        true
                        0.66077775
                        0.80432236
                        0.22463012
                        0.12966657
                        0.08486962
                        """,
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                "create table y as (select rnd_float() b from long_sequence(5))",
                false
        );

        testUnion(
                """
                        a
                        false
                        true
                        0.66077775
                        0.80432236
                        0.22463012
                        0.12966657
                        0.08486962
                        """
        );
    }

    @Test
    public void testFloatByte() throws Exception {
        testUnionAll(
                """
                        a
                        79.0
                        122.0
                        83.0
                        90.0
                        76.0
                        0.66077775
                        0.80432236
                        0.22463012
                        0.12966657
                        0.08486962
                        """,
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
                """
                        a
                        0.2845577791213847
                        0.0843832076262595
                        0.9344604857394011
                        0.13123360041292131
                        0.7905675319675964
                        0.660777747631073
                        0.804322361946106
                        0.22463011741638184
                        0.12966656684875488
                        0.0848696231842041
                        """,
                "create table x as (select rnd_double() a from long_sequence(5))",
                "create table y as (select rnd_float() b from long_sequence(5))"
        );
    }

    @Test
    public void testFloatGeoHash() throws Exception {
        testUnionAll(
                """
                        a
                        0001001
                        0101110
                        0101101
                        0111011
                        0010101
                        0.66077775
                        0.80432236
                        0.22463012
                        0.12966657
                        0.08486962
                        """,
                "create table x as (select rnd_geohash(7) a from long_sequence(5))",
                "create table y as (select rnd_float() b from long_sequence(5))"
        );
    }

    @Test
    public void testFloatInt() throws Exception {
        testUnionAll(
                """
                        a
                        -9.4826336E8
                        1.32644723E9
                        5.9285965E8
                        1.86872371E9
                        -8.4753107E8
                        0.66077775
                        0.80432236
                        0.22463012
                        0.12966657
                        0.08486962
                        """,
                "create table x as (select rnd_int() a from long_sequence(5))",
                "create table y as (select rnd_float() b from long_sequence(5))"
        );
    }

    @Test
    public void testFloatLong() throws Exception {
        testUnionAll(
                """
                        a
                        0.28455776
                        0.29919904
                        0.08438319
                        0.20447439
                        0.93446046
                        4.6895921E15
                        4.7299965E15
                        7.7465361E18
                        -6.9459217E18
                        8.2601883E18
                        """,
                "create table x as (select rnd_float() a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testFloatNull() throws Exception {
        testUnionAllWithNull(
                """
                        a\tc
                        0.28455776\tnull
                        0.29919904\tnull
                        0.08438319\tnull
                        0.20447439\tnull
                        0.93446046\tnull
                        null\t0.66077775
                        null\t0.80432236
                        null\t0.22463012
                        null\t0.12966657
                        null\t0.08486962
                        """,
                "rnd_float()"
        );
    }

    @Test
    public void testFloatShort() throws Exception {
        testUnionAll(
                """
                        a
                        -22955.0
                        -1398.0
                        21015.0
                        30202.0
                        -19496.0
                        0.66077775
                        0.80432236
                        0.22463012
                        0.12966657
                        0.08486962
                        """,
                "create table x as (select rnd_short() a from long_sequence(5))",
                "create table y as (select rnd_float() b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoByteExact() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
                """
                        a\ttypeOf\tk
                        q\tGEOHASH(1c)\t-1792928964
                        5\tGEOHASH(1c)\t1404198
                        x\tGEOHASH(1c)\t-1252906348
                        f\tGEOHASH(1c)\t1699553881
                        8\tGEOHASH(1c)\t-938514914
                        9\tGEOHASH(1c)\t8260188555232587029
                        2\tGEOHASH(1c)\t-1675638984090602536
                        w\tGEOHASH(1c)\t-4094902006239100839
                        p\tGEOHASH(1c)\t5408639942391651698
                        w\tGEOHASH(1c)\t-3985256597569472057
                        """,
                "select a, typeOf(a), k from (x union all y)",
                "create table x as (select rnd_geohash(5) a, rnd_int() k from long_sequence(5))",
                "create table y as (select rnd_geohash(5) a, rnd_long() k from long_sequence(5))"
        );
    }

    @Test
    public void testGeoByteNull() throws Exception {
        testUnionAllWithNull(
                """
                        a\tc
                        1110010\t
                        1100000\t
                        0000101\t
                        1001110\t
                        0011100\t
                        \t0100111
                        \t0010000
                        \t1000110
                        \t1111101
                        \t1000010
                        """,
                "rnd_geohash(7)"
        );
    }

    @Test
    public void testGeoByteStr() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
                """
                        a
                        2
                        c
                        c
                        f
                        5
                        4689592037643856
                        4729996258992366
                        7746536061816329025
                        -6945921502384501475
                        8260188555232587029
                        """,
                "create table x as (select rnd_geohash(5) a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))",
                false
        );

        testUnion(
                """
                        a
                        2
                        c
                        f
                        5
                        4689592037643856
                        4729996258992366
                        7746536061816329025
                        -6945921502384501475
                        8260188555232587029
                        """
        );
    }

    @Test
    public void testGeoIntGeoByte() throws Exception {
        testUnionAll(
                """
                        a
                        w
                        s
                        1
                        m
                        7
                        9
                        4
                        j
                        z
                        h
                        """,
                "create table x as (select rnd_geohash(20) a from long_sequence(5))",
                "create table y as (select rnd_geohash(5) b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoIntGeoShort() throws Exception {
        testUnionAll(
                """
                        a
                        wh
                        s2
                        1c
                        mm
                        71
                        9v
                        46
                        jn
                        zf
                        hp
                        """,
                "create table x as (select rnd_geohash(30) a from long_sequence(5))",
                "create table y as (select rnd_geohash(10) b from long_sequence(5))",
                false
        );
    }

    @Test
    public void testGeoIntNull() throws Exception {
        testUnionAllWithNull(
                """
                        a\tc
                        wh4b6v\t
                        s2z2fy\t
                        1cjjwk\t
                        mmt894\t
                        71ftmp\t
                        \t9v1s8h
                        \t46swgj
                        \tjnw97u
                        \tzfuqd3
                        \thp4muv
                        """,
                "rnd_geohash(30)"
        );
    }

    @Test
    public void testGeoIntStr() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
                """
                        a
                        29je
                        cjcs
                        c931
                        fu3r
                        5ewm
                        4689592037643856
                        4729996258992366
                        7746536061816329025
                        -6945921502384501475
                        8260188555232587029
                        """,
                "create table x as (select rnd_geohash(20) a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoIntStrBits() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
                """
                        a
                        000100100110001011010
                        010111000101011110000
                        010110100100011000011
                        011101101000011101110
                        001010110111100100110
                        4689592037643856
                        4729996258992366
                        7746536061816329025
                        -6945921502384501475
                        8260188555232587029
                        """,
                "create table x as (select rnd_geohash(21) a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoLongExact() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
                """
                        a\ttypeOf\tk
                        qmqxuuu\tGEOHASH(7c)\t-1792928964
                        5rshu96\tGEOHASH(7c)\t1404198
                        xn8nmwc\tGEOHASH(7c)\t-1252906348
                        fsnj14w\tGEOHASH(7c)\t1699553881
                        8nje17e\tGEOHASH(7c)\t-938514914
                        9v1s8hm\tGEOHASH(7c)\t8260188555232587029
                        29je7k2\tGEOHASH(7c)\t-1675638984090602536
                        wszdkrq\tGEOHASH(7c)\t-4094902006239100839
                        pn5udk1\tGEOHASH(7c)\t5408639942391651698
                        wh4b6vn\tGEOHASH(7c)\t-3985256597569472057
                        """,
                "select a, typeOf(a), k from (x union all y)",
                "create table x as (select rnd_geohash(35) a, rnd_int() k from long_sequence(5))",
                "create table y as (select rnd_geohash(35) a, rnd_long() k from long_sequence(5))"
        );
    }

    @Test
    public void testGeoLongGeoByte() throws Exception {
        testUnionAll(
                """
                        a
                        w
                        s
                        1
                        m
                        7
                        9
                        4
                        j
                        z
                        h
                        """,
                "create table x as (select rnd_geohash(40) a from long_sequence(5))",
                "create table y as (select rnd_geohash(5) b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoLongGeoInt() throws Exception {
        testUnionAll(
                """
                        a
                        wh4b6v
                        s2z2fy
                        1cjjwk
                        mmt894
                        71ftmp
                        9v1s8h
                        46swgj
                        jnw97u
                        zfuqd3
                        hp4muv
                        """,
                "create table x as (select rnd_geohash(40) a from long_sequence(5))",
                "create table y as (select rnd_geohash(30) b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoLongGeoShort() throws Exception {
        testUnionAll(
                """
                        a
                        wh
                        s2
                        1c
                        mm
                        71
                        9v
                        46
                        jn
                        zf
                        hp
                        """,
                "create table x as (select rnd_geohash(40) a from long_sequence(5))",
                "create table y as (select rnd_geohash(10) b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoLongNull() throws Exception {
        testUnionAllWithNull(
                """
                        a\tc
                        wh4b6vnt\t
                        s2z2fyds\t
                        1cjjwk6r\t
                        mmt89425\t
                        71ftmpy5\t
                        \t9v1s8hm7
                        \t46swgj10
                        \tjnw97u4y
                        \tzfuqd3bf
                        \thp4muv5t
                        """,
                "rnd_geohash(40)"
        );
    }

    @Test
    public void testGeoLongStr() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
                """
                        a
                        29je7k2s
                        cjcsgh6h
                        c931t136
                        fu3r7chm
                        5ewm40wx
                        4689592037643856
                        4729996258992366
                        7746536061816329025
                        -6945921502384501475
                        8260188555232587029
                        """,
                "create table x as (select rnd_geohash(40) a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoLongStrBits() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
                """
                        a
                        000100100110001011010011110010000101100000
                        010111000101011110000111110000001101000001
                        010110100100011000011100100001000110011010
                        011101101000011101110011101011100001001101
                        001010110111100100110010000000111001110110
                        4689592037643856
                        4729996258992366
                        7746536061816329025
                        -6945921502384501475
                        8260188555232587029
                        """,
                "create table x as (select rnd_geohash(42) a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoShortExact() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
                """
                        a\ttypeOf\tk
                        101101001110\tGEOHASH(12b)\t-1792928964
                        001011011111\tGEOHASH(12b)\t1404198
                        111011010001\tGEOHASH(12b)\t-1252906348
                        011101100010\tGEOHASH(12b)\t1699553881
                        010001010010\tGEOHASH(12b)\t-938514914
                        010011101100\tGEOHASH(12b)\t8260188555232587029
                        000100100110\tGEOHASH(12b)\t-1675638984090602536
                        111001100011\tGEOHASH(12b)\t-4094902006239100839
                        101011010000\tGEOHASH(12b)\t5408639942391651698
                        111001000000\tGEOHASH(12b)\t-3985256597569472057
                        """,
                "select a, typeOf(a), k from (x union all y)",
                "create table x as (select rnd_geohash(12) a, rnd_int() k from long_sequence(5))",
                "create table y as (select rnd_geohash(12) a, rnd_long() k from long_sequence(5))"
        );
    }

    @Test
    public void testGeoShortGeoByte() throws Exception {
        testUnionAll(
                """
                        a
                        w
                        s
                        1
                        m
                        7
                        9
                        4
                        j
                        z
                        h
                        """,
                "create table x as (select rnd_geohash(10) a from long_sequence(5))",
                "create table y as (select rnd_geohash(5) b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoShortGeoShort() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
                """
                        a
                        k0
                        0c
                        5f
                        fg
                        w5
                        9v
                        46
                        jn
                        zf
                        hp
                        """,
                "create table x as (select rnd_geohash(12) a from long_sequence(5))",
                "create table y as (select rnd_geohash(10) b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoShortNull() throws Exception {
        testUnionAllWithNull(
                """
                        a\tc
                        wh4\t
                        s2z\t
                        1cj\t
                        mmt\t
                        71f\t
                        \t9v1
                        \t46s
                        \tjnw
                        \tzfu
                        \thp4
                        """,
                "rnd_geohash(15)"
        );
    }

    @Test
    public void testGeoShortStr() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
                """
                        a
                        29
                        cj
                        c9
                        fu
                        5e
                        4689592037643856
                        4729996258992366
                        7746536061816329025
                        -6945921502384501475
                        8260188555232587029
                        """,
                "create table x as (select rnd_geohash(10) a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testGeoShortStrBits() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
                """
                        a
                        000100100110
                        010111000101
                        010110100100
                        011101101000
                        001010110111
                        4689592037643856
                        4729996258992366
                        7746536061816329025
                        -6945921502384501475
                        8260188555232587029
                        """,
                "create table x as (select rnd_geohash(12) a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testIPv4String() throws Exception {
        testUnionAll(
                """
                        a
                        101.77.34.89
                        66.56.51.126
                        74.188.217.59
                        249.60.8.8
                        230.202.108.161
                        JWCPSWHYR
                        EHNRX
                        SXUXI
                        TGPGW
                        YUDEYYQEHB
                        """,
                "create table x as (select rnd_ipv4() a from long_sequence(5))",
                "create table y as (select rnd_str() b from long_sequence(5))",
                true
        );
    }

    @Test
    public void testIPv4Symbol() throws Exception {
        testUnionAll(
                """
                        a
                        199.122.166.85
                        79.15.250.138
                        35.86.82.23
                        111.98.117.250
                        205.123.179.216
                        aaa
                        aaa
                        bbb
                        bbb
                        bbb
                        """,
                "create table x as (select rnd_ipv4() a from long_sequence(5))",
                "create table y as (select rnd_symbol('aaa', 'bbb') a from long_sequence(5))",
                false
        );

        testUnion(
                """
                        a
                        199.122.166.85
                        79.15.250.138
                        35.86.82.23
                        111.98.117.250
                        205.123.179.216
                        aaa
                        bbb
                        """
        );
    }

    @Test
    public void testIPv4Varchar() throws Exception {
        testUnionAll(
                """
                        a
                        49.254.54.230
                        89.207.251.208
                        66.9.11.179
                        50.89.42.43
                        219.41.127.7
                        &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L
                        8#3TsZ
                        zV衞͛Ԉ龘и\uDA89\uDFA4~
                        ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ
                        \uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46OF
                        """,
                "create table x as (select rnd_ipv4() a from long_sequence(5))",
                "create table y as (select rnd_varchar() b from long_sequence(5))",
                true
        );
    }

    @Test
    public void testIntBool() throws Exception {
        // this is cast to STRING, both columns
        testUnionAll(
                """
                        a
                        false
                        true
                        true
                        true
                        true
                        -1148479920
                        315515118
                        1548800833
                        -727724771
                        73575701
                        """,
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                "create table y as (select rnd_int() b from long_sequence(5))",
                false
        );

        testUnion(
                """
                        a
                        false
                        true
                        -1148479920
                        315515118
                        1548800833
                        -727724771
                        73575701
                        """
        );
    }

    @Test
    public void testIntByte() throws Exception {
        testUnionAll(
                """
                        a
                        79
                        122
                        83
                        90
                        76
                        -1148479920
                        315515118
                        1548800833
                        -727724771
                        73575701
                        """,
                "create table x as (select rnd_byte() a from long_sequence(5))",
                "create table y as (select rnd_int() b from long_sequence(5))"
        );
    }

    @Test
    public void testIntChar() throws Exception {
        testUnionAll(
                """
                        a
                        0
                        3
                        5
                        9
                        8
                        -1148479920
                        315515118
                        1548800833
                        -727724771
                        73575701
                        """,
                "create table x as (select '0'::char a union select '3'::char union select '5'::char union select '9'::char union select '8'::char)",
                "create table y as (select rnd_int() b from long_sequence(5))"
        );
    }

    @Test
    public void testIntDecimal() throws Exception {
        testUnionAll(
                """
                        a
                        1.000
                        2.000
                        3.000
                        4.000
                        5.000
                        0.000
                        3.000
                        5.000
                        9.000
                        8.000
                        """,
                "create table x as (select x::decimal(19, 3) a from long_sequence(5))",
                "create table y as (select 0::int a union select 3::int union select 5::int union select 9::int union select 8::int)",
                false
        );
    }

    @Test
    public void testIntExact() throws Exception {
        // long + geohash overlap via string type
        testUnionAll(
                """
                        a\ttypeOf\tk
                        qmqxuu\tGEOHASH(6c)\t-1792928964
                        5rshu9\tGEOHASH(6c)\t1404198
                        xn8nmw\tGEOHASH(6c)\t-1252906348
                        fsnj14\tGEOHASH(6c)\t1699553881
                        8nje17\tGEOHASH(6c)\t-938514914
                        9v1s8h\tGEOHASH(6c)\t8260188555232587029
                        29je7k\tGEOHASH(6c)\t-1675638984090602536
                        wszdkr\tGEOHASH(6c)\t-4094902006239100839
                        pn5udk\tGEOHASH(6c)\t5408639942391651698
                        wh4b6v\tGEOHASH(6c)\t-3985256597569472057
                        """,
                "select a, typeOf(a), k from (x union all y)",
                "create table x as (select rnd_geohash(30) a, rnd_int() k from long_sequence(5))",
                "create table y as (select rnd_geohash(30) a, rnd_long() k from long_sequence(5))"
        );
    }

    @Test
    public void testIntNull() throws Exception {
        testUnionAllWithNull(
                """
                        a\tc
                        -948263339\tnull
                        1326447242\tnull
                        592859671\tnull
                        1868723706\tnull
                        -847531048\tnull
                        null\t-1148479920
                        null\t315515118
                        null\t1548800833
                        null\t-727724771
                        null\t73575701
                        """,
                "rnd_int()"
        );
    }

    @Test
    public void testIntShort() throws Exception {
        testUnionAll(
                """
                        a
                        -22955
                        -1398
                        21015
                        30202
                        -19496
                        -1148479920
                        315515118
                        1548800833
                        -727724771
                        73575701
                        """,
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
                    """
                            contact\tgroupid\teventid
                            2\t1.5\tstand
                            1\t1.5\tflash
                            """,
                    """
                            (events1
                            intersect
                            events2) order by 1 desc""",
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
                    """
                            contact\tgroupid\teventid
                            2\t1.5\tstand
                            1\t1.5\tflash
                            """,
                    """
                            (events1
                            intersect
                            events2) order by 1 desc""",
                    null,
                    true
            );
        });
    }

    @Test
    public void testInterval1() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        i\ttypeOf
                        ('1970-01-01T00:00:00.100Z', '1970-01-01T00:00:00.200Z')\tINTERVAL
                        ('1970-01-01T00:00:00.300Z', '1970-01-01T00:00:00.400Z')\tINTERVAL
                        \tINTERVAL
                        """,
                "select i, typeOf(i) from ((select interval(100000,200000) i) union all (select interval(300000,400000) i) union all (select null::interval i))"
        ));
    }

    @Test
    public void testInterval2() throws Exception {
        setCurrentMicros(7 * Micros.DAY_MICROS + Micros.HOUR_MICROS); // 1970-01-07T01:00:00.000Z
        assertMemoryLeak(() -> assertSql(
                """
                        a\tb
                        ('1970-01-07T00:00:00.000Z', '1970-01-07T23:59:59.999Z')\t('1970-01-07T00:00:00.000Z', '1970-01-07T23:59:59.999Z')
                        """,
                """
                        select * from (
                          select today() a, yesterday() b
                          union all
                          select yesterday(), yesterday()
                          union all
                          select today() a, null b
                        )
                        where b = a"""
        ));
    }

    @Test
    public void testInterval3() throws Exception {
        setCurrentMicros(7 * Micros.DAY_MICROS + Micros.HOUR_MICROS); // 1970-01-07T01:00:00.000Z
        assertMemoryLeak(() -> assertSql(
                """
                        a\ta1
                        ('1970-01-08T00:00:00.000Z', '1970-01-08T23:59:59.999Z')\t('1970-01-08T00:00:00.000Z', '1970-01-08T23:59:59.999Z')
                        ('1970-01-07T00:00:00.000Z', '1970-01-07T23:59:59.999Z')\t('1970-01-07T00:00:00.000Z', '1970-01-07T23:59:59.999Z')
                        ('1970-01-09T00:00:00.000Z', '1970-01-09T23:59:59.999Z')\t('1970-01-09T00:00:00.000Z', '1970-01-09T23:59:59.999Z')
                        """,
                """
                        select * from (
                          select today() a
                          union\s
                          select yesterday()
                          union\s
                          select tomorrow()
                        ) a
                        join (
                          select today() a
                          union\s
                          select yesterday()
                          union\s
                          select tomorrow()
                        ) b
                        on a.a = b.a"""
        ));
    }

    @Test
    public void testInterval4() throws Exception {
        setCurrentMicros(7 * Micros.DAY_MICROS + Micros.HOUR_MICROS); // 1970-01-07T01:00:00.000Z
        assertMemoryLeak(() -> assertSql(
                """
                        a\tb
                        ('1970-01-08T00:00:00.000Z', '1970-01-08T23:59:59.999Z')\t('1970-01-07T00:00:00.000Z', '1970-01-07T23:59:59.999Z')
                        foobar\t
                        """,
                """
                        select * from (
                          select today() a, yesterday() b
                          union all
                          select yesterday(), yesterday()
                          union all
                          select 'foobar' a, null b
                        )
                        where b != a"""
        ));
    }

    @Test
    public void testLong256Long256() throws Exception {
        testUnionAll(
                """
                        a\tn\ttypeOf
                        -4472\t0x4cdfb9e29522133c87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde\tLONG256
                        -11657\t0x9840ad8800156d26c718ab5cbb3fd261c1bf6c24be53876861b1a0b0a5595515\tLONG256
                        18351\t0x5b9832d4b5522a9474ce62a98a4516952705e02c613acfc405374f5fbcef4819\tLONG256
                        21558\t0x36ee542d654d22598a538661f350d0b46f06560981acb5496adc00ebd29fdd53\tLONG256
                        13182\t0x63eb3740c80f661e9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b\tLONG256
                        -1148479920\t0x72a215ba0462ad159f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee\tLONG256
                        -948263339\t0xe8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217965d4c984f0ffa8a\tLONG256
                        -1191262516\t0xc72bfc5230158059980eca62a219a0f16846d7a3aa5aecce322a2198864beb14\tLONG256
                        1545253512\t0x4b0f595f143e5d722f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4\tLONG256
                        1530831067\t0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tLONG256
                        """,
                "select a, n, typeOf(n) from (x union all y)",
                "create table x as (select rnd_short() a, rnd_long256() n from long_sequence(5))",
                "create table y as (select rnd_int() a, rnd_long256() n from long_sequence(5))"
        );
    }

    @Test
    public void testLong256Null() throws Exception {
        testUnionAllWithNull(
                """
                        a\tc
                        0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\t
                        0xa0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88c8b1863d4316f9c7\t
                        0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\t
                        0x523eb59d99c647af9840ad8800156d26c718ab5cbb3fd261c1bf6c24be538768\t
                        0x5b9832d4b5522a9474ce62a98a4516952705e02c613acfc405374f5fbcef4819\t
                        \t0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650
                        \t0xb5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa65572a215ba0462ad15
                        \t0x322a2198864beb14797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fa
                        \t0xc1e631285c1ab288c72bfc5230158059980eca62a219a0f16846d7a3aa5aecce
                        \t0x4b0f595f143e5d722f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4
                        """,
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
                """
                        a
                        false
                        true
                        true
                        true
                        true
                        4689592037643856
                        4729996258992366
                        7746536061816329025
                        -6945921502384501475
                        8260188555232587029
                        """,
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))",
                false
        );

        testUnion(
                """
                        a
                        false
                        true
                        4689592037643856
                        4729996258992366
                        7746536061816329025
                        -6945921502384501475
                        8260188555232587029
                        """
        );
    }

    @Test
    public void testLongByte() throws Exception {
        testUnionAll(
                """
                        a
                        79
                        122
                        83
                        90
                        76
                        4689592037643856
                        4729996258992366
                        7746536061816329025
                        -6945921502384501475
                        8260188555232587029
                        """,
                "create table x as (select rnd_byte() a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testLongChar() throws Exception {
        testUnionAll(
                """
                        a
                        0
                        3
                        5
                        9
                        8
                        4689592037643856
                        4729996258992366
                        7746536061816329025
                        -6945921502384501475
                        8260188555232587029
                        """,
                "create table x as (select '0'::char a union select '3'::char union select '5'::char union select '9'::char union select '8'::char)",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testLongDecimal() throws Exception {
        testUnionAll(
                """
                        a
                        1.000
                        2.000
                        3.000
                        4.000
                        5.000
                        0.000
                        3.000
                        5.000
                        9.000
                        8.000
                        """,
                "create table x as (select x::decimal(19, 3) a from long_sequence(5))",
                "create table y as (select 0::long a union select 3::long union select 5::long union select 9::long union select 8::long)",
                false
        );
    }

    @Test
    public void testLongInt() throws Exception {
        testUnionAll(
                """
                        a
                        -948263339
                        1326447242
                        592859671
                        1868723706
                        -847531048
                        4689592037643856
                        4729996258992366
                        7746536061816329025
                        -6945921502384501475
                        8260188555232587029
                        """,
                "create table x as (select rnd_int() a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testLongLong256() throws Exception {
        testUnionAll(
                """
                        a
                        0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655
                        0x6846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cce8beef38cd7bb3d8
                        0x9fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059980eca62a219a0f1
                        0x6e60a01a5b3ea0db4b0f595f143e5d722f1a8266e7921e3b716de3d25dcc2d91
                        0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3
                        4689592037643856
                        4729996258992366
                        7746536061816329025
                        -6945921502384501475
                        8260188555232587029
                        """,
                "create table x as (select rnd_long256() a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testLongNull() throws Exception {
        testUnionAllWithNull(
                """
                        a\tc
                        8920866532787660373\tnull
                        -7611843578141082998\tnull
                        -5354193255228091881\tnull
                        -2653407051020864006\tnull
                        -1675638984090602536\tnull
                        null\t4689592037643856
                        null\t4729996258992366
                        null\t7746536061816329025
                        null\t-6945921502384501475
                        null\t8260188555232587029
                        """,
                "rnd_long()"
        );
    }

    @Test
    public void testLongShort() throws Exception {
        testUnionAll(
                """
                        a
                        -22955
                        -1398
                        21015
                        30202
                        -19496
                        4689592037643856
                        4729996258992366
                        7746536061816329025
                        -6945921502384501475
                        8260188555232587029
                        """,
                "create table x as (select rnd_short() a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testLongSymbol() throws Exception {
        testUnionAll(
                """
                        a
                        bbb
                        aaa
                        bbb
                        aaa
                        aaa
                        4689592037643856
                        4729996258992366
                        7746536061816329025
                        -6945921502384501475
                        8260188555232587029
                        """,
                "create table x as (select rnd_symbol('aaa', 'bbb') a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))",
                false
        );

        testUnion(
                """
                        a
                        bbb
                        aaa
                        4689592037643856
                        4729996258992366
                        7746536061816329025
                        -6945921502384501475
                        8260188555232587029
                        """
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
                """
                        a
                        false
                        true
                        true
                        true
                        true
                        -27056
                        24814
                        -11455
                        -13027
                        -21227
                        """,
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                "create table y as (select rnd_short() b from long_sequence(5))",
                false
        );

        testUnion(
                """
                        a
                        false
                        true
                        -27056
                        24814
                        -11455
                        -13027
                        -21227
                        """
        );
    }

    @Test
    public void testShortByte() throws Exception {
        testUnionAll(
                """
                        a
                        -22955
                        -1398
                        21015
                        30202
                        -19496
                        76
                        102
                        27
                        87
                        79
                        """,
                "create table x as (select rnd_short() a from long_sequence(5))",
                "create table y as (select rnd_byte() b from long_sequence(5))"
        );
    }

    @Test
    public void testShortChar() throws Exception {
        testUnionAll(
                """
                        a
                        0
                        3
                        5
                        9
                        8
                        -27056
                        24814
                        -11455
                        -13027
                        -21227
                        """,
                "create table x as (select '0'::char a union select '3'::char union select '5'::char union select '9'::char union select '8'::char)",
                "create table y as (select rnd_short() b from long_sequence(5))"
        );
    }

    @Test
    public void testShortDecimal() throws Exception {
        testUnionAll(
                """
                        a
                        1.000
                        2.000
                        3.000
                        4.000
                        5.000
                        0.000
                        3.000
                        5.000
                        9.000
                        8.000
                        """,
                "create table x as (select x::decimal(19, 3) a from long_sequence(5))",
                "create table y as (select 0::short a union select 3::short union select 5::short union select 9::short union select 8::short)",
                false
        );
    }

    @Test
    public void testShortNull() throws Exception {
        testUnionAllWithNull(
                """
                        a\tc
                        -22955\t0
                        -1398\t0
                        21015\t0
                        30202\t0
                        -19496\t0
                        0\t-27056
                        0\t24814
                        0\t-11455
                        0\t-13027
                        0\t-21227
                        """,
                "rnd_short()"
        );
    }

    @Test
    public void testStrDecimal() throws Exception {
        testUnionAll(
                """
                        a
                        1.000
                        2.000
                        3.000
                        4.000
                        5.000
                        0.000
                        3.000
                        5.000
                        9.000
                        8.000
                        """,
                "create table x as (select x::decimal(19, 3) a from long_sequence(5))",
                "create table y as (select '0'::string a union select '3'::string union select '5'::string union select '9'::string union select '8'::string)",
                false
        );
    }

    @Test
    public void testStrGeoByte() throws Exception {
        testUnionAll(
                """
                        a
                        x
                        x
                        x
                        x
                        x
                        9
                        4
                        j
                        z
                        h
                        """,
                "create table x as (select 'xkl921' a from long_sequence(5))",
                "create table y as (select rnd_geohash(5) b from long_sequence(5))",
                false
        );
    }

    @Test
    public void testStrGeoInt() throws Exception {
        testUnionAll(
                """
                        a
                        fgert9
                        fgert9
                        fgert9
                        fgert9
                        fgert9
                        9v1s8h
                        46swgj
                        jnw97u
                        zfuqd3
                        hp4muv
                        """,
                "create table x as (select 'fgert930' a from long_sequence(5))",
                "create table y as (select rnd_geohash(30) b from long_sequence(5))",
                false
        );
    }

    @Test
    public void testStrGeoLong() throws Exception {
        testUnionAll(
                """
                        a
                        kjhgt66s
                        kjhgt66s
                        kjhgt66s
                        kjhgt66s
                        kjhgt66s
                        9v1s8hm7
                        46swgj10
                        jnw97u4y
                        zfuqd3bf
                        hp4muv5t
                        """,
                "create table x as (select 'kjhgt66srs' a from long_sequence(5))",
                "create table y as (select rnd_geohash(40) b from long_sequence(5))",
                false
        );
    }

    @Test
    public void testStrGeoShort() throws Exception {
        testUnionAll(
                """
                        a
                        xk
                        xk
                        xk
                        xk
                        xk
                        9v
                        46
                        jn
                        zf
                        hp
                        """,
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
                """
                        a\tc
                        false\tIBB
                        true\tPGW
                        true\tYUD
                        false\tYQE
                        false\tHFO
                        76\tJWCP
                        122\t
                        90\tRXPE
                        83\tRXGZ
                        77\t
                        """,
                "create table x as (select rnd_boolean() a, rnd_str(3,3,1) c from long_sequence(5))",
                "create table y as (select rnd_byte() b, rnd_str(4,4,1) c from long_sequence(5))"
        );
    }

    @Test
    public void testStringIPv4() throws Exception {
        testUnionAll(
                """
                        b
                        JWCPSWHYR
                        EHNRX
                        SXUXI
                        TGPGW
                        YUDEYYQEHB
                        101.77.34.89
                        66.56.51.126
                        74.188.217.59
                        249.60.8.8
                        230.202.108.161
                        """,
                "create table y as (select rnd_ipv4() a from long_sequence(5))",
                "create table x as (select rnd_str() b from long_sequence(5))",
                true
        );
    }

    @Test
    public void testStringNull() throws Exception {
        testUnionAllWithNull(
                """
                        a\tc
                        \t
                        ZSX\t
                        XIB\t
                        TGP\t
                        WFF\t
                        \tTJW
                        \t
                        \tSWH
                        \tRXP
                        \tHNR
                        """,
                "rnd_str(3,3,1)",
                false
        );

        testUnionWithNull(
                """
                        a\tc
                        \t
                        ZSX\t
                        XIB\t
                        TGP\t
                        WFF\t
                        \tTJW
                        \tSWH
                        \tRXP
                        \tHNR
                        """
        );
    }

    @Test
    public void testSymBin() throws Exception {
        assertFailure("create table x as (select rnd_symbol('aa','bb') a from long_sequence(5))", "create table y as (select rnd_bin(10, 24, 1) b from long_sequence(5))", 12);
    }

    @Test
    public void testSymNull() throws Exception {
        testUnionAllWithNull("""
                        a\tc
                        bb\t
                        aa\t
                        bb\t
                        aa\t
                        aa\t
                        \taa
                        \taa
                        \tbb
                        \tbb
                        \tbb
                        """,
                "rnd_symbol('aa','bb')",
                false
        );

        testUnionWithNull(
                """
                        a\tc
                        bb\t
                        aa\t
                        \taa
                        \tbb
                        """
        );
    }

    @Test
    public void testSymSym() throws Exception {
        // we include byte <-> bool cast to make sure
        // sym <-> sym cast it not thrown away as redundant
        testUnionAll(
                """
                        a\tc\ttypeOf
                        false\ta\tSTRING
                        false\tb\tSTRING
                        true\ta\tSTRING
                        true\tb\tSTRING
                        false\ta\tSTRING
                        76\tx\tSTRING
                        27\ty\tSTRING
                        79\ty\tSTRING
                        122\ty\tSTRING
                        90\tx\tSTRING
                        """,
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
                """
                        a\tc\ttypeOf
                        false\tb\tSTRING
                        false\ta\tSTRING
                        false\tb\tSTRING
                        true\tb\tSTRING
                        false\ta\tSTRING
                        27\ty\tSTRING
                        122\ty\tSTRING
                        84\tx\tSTRING
                        83\tx\tSTRING
                        91\tx\tSTRING
                        """,
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
                """
                        u\ta\tc
                        0.6607777894187332\t27\ty
                        """,
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
                """
                        a
                        true
                        true
                        false
                        false
                        false
                        1970-01-01T00:00:00.002771Z
                        
                        1970-01-01T00:00:00.045299Z
                        1970-01-01T00:00:00.078334Z
                        
                        """,
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                "create table y as (select rnd_timestamp(0, 100000, 2) b from long_sequence(5))",
                false
        );

        testUnion(
                """
                        a
                        true
                        false
                        1970-01-01T00:00:00.002771Z
                        
                        1970-01-01T00:00:00.045299Z
                        1970-01-01T00:00:00.078334Z
                        """
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
                """
                        a
                        1970-01-01T00:00:00.000001000Z
                        1970-01-01T00:00:00.000002000Z
                        1970-01-01T00:00:00.000003000Z
                        1970-01-01T00:00:00.000004000Z
                        1970-01-01T00:00:00.000005000Z
                        1970-01-01T00:00:00.000000001Z
                        1970-01-01T00:00:00.000000002Z
                        1970-01-01T00:00:00.000000003Z
                        1970-01-01T00:00:00.000000004Z
                        1970-01-01T00:00:00.000000005Z
                        """,
                "create table x as (select cast(x as timestamp) a from long_sequence(5))",
                "create table y as (select cast(x as timestamp_ns) a from long_sequence(5))",
                true
        );
    }

    @Test
    public void testTimestampNanosTimestampMicros() throws Exception {
        testUnionAll(
                """
                        a
                        1970-01-01T00:00:00.000000001Z
                        1970-01-01T00:00:00.000000002Z
                        1970-01-01T00:00:00.000000003Z
                        1970-01-01T00:00:00.000000004Z
                        1970-01-01T00:00:00.000000005Z
                        1970-01-01T00:00:00.000001000Z
                        1970-01-01T00:00:00.000002000Z
                        1970-01-01T00:00:00.000003000Z
                        1970-01-01T00:00:00.000004000Z
                        1970-01-01T00:00:00.000005000Z
                        """,
                "create table x as (select cast(x as timestamp_ns) a from long_sequence(5))",
                "create table y as (select cast(x as timestamp) a from long_sequence(5))",
                true
        );
    }

    @Test
    public void testTimestampNull() throws Exception {
        testUnionAllWithNull(
                """
                        a\tc
                        1970-01-01T00:00:00.023853Z\t
                        1970-01-01T00:00:00.083620Z\t
                        1970-01-01T00:00:00.084025Z\t
                        \t
                        1970-01-01T00:00:00.008228Z\t
                        \t1970-01-01T00:00:00.002771Z
                        \t
                        \t1970-01-01T00:00:00.045299Z
                        \t1970-01-01T00:00:00.078334Z
                        \t
                        """,
                "rnd_timestamp(0, 100000, 2)",
                false
        );

        testUnionWithNull(
                """
                        a\tc
                        1970-01-01T00:00:00.023853Z\t
                        1970-01-01T00:00:00.083620Z\t
                        1970-01-01T00:00:00.084025Z\t
                        \t
                        1970-01-01T00:00:00.008228Z\t
                        \t1970-01-01T00:00:00.002771Z
                        \t1970-01-01T00:00:00.045299Z
                        \t1970-01-01T00:00:00.078334Z
                        """
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
                """
                        a\tc
                        322a2198-864b-4b14-b97f-a69eb8fec6cc\t
                        980eca62-a219-40f1-a846-d7a3aa5aecce\t
                        c1e63128-5c1a-4288-872b-fc5230158059\t
                        716de3d2-5dcc-4d91-9fa2-397a5d8c84c4\t
                        4b0f595f-143e-4d72-af1a-8266e7921e3b\t
                        \t0010cde8-12ce-40ee-8010-a928bb8b9650
                        \t9f9b2131-d49f-4d1d-ab81-39815c50d341
                        \t7bcd48d8-c77a-4655-b2a2-15ba0462ad15
                        \tb5b2159a-2356-4217-965d-4c984f0ffa8a
                        \te8beef38-cd7b-43d8-9b2d-34586f6275fa
                        """,
                "rnd_uuid4()"
        );
    }

    @Test
    public void testUuidString() throws Exception {
        testUnionAll(
                """
                        a
                        7f98b0c7-4238-437e-b6ee-542d654d2259
                        7c1b058a-f93c-4808-abaf-c47f4abcd93b
                        63eb3740-c80f-461e-9c8a-fa23e6ca6ca1
                        c2593f82-b430-428d-84a0-9f29df637e38
                        58dfd08e-eb9c-439e-8ec8-2869edec121b
                        JWCPSWHYR
                        EHNRX
                        SXUXI
                        TGPGW
                        YUDEYYQEHB
                        """,
                "create table x as (select rnd_uuid4() a from long_sequence(5))",
                "create table y as (select rnd_str() b from long_sequence(5))",
                true
        );
    }

    @Test
    public void testUuidUuid() throws Exception {
        testUnionAll(
                """
                        a
                        0010cde8-12ce-40ee-8010-a928bb8b9650
                        9f9b2131-d49f-4d1d-ab81-39815c50d341
                        7bcd48d8-c77a-4655-b2a2-15ba0462ad15
                        b5b2159a-2356-4217-965d-4c984f0ffa8a
                        e8beef38-cd7b-43d8-9b2d-34586f6275fa
                        322a2198-864b-4b14-b97f-a69eb8fec6cc
                        980eca62-a219-40f1-a846-d7a3aa5aecce
                        c1e63128-5c1a-4288-872b-fc5230158059
                        716de3d2-5dcc-4d91-9fa2-397a5d8c84c4
                        4b0f595f-143e-4d72-af1a-8266e7921e3b
                        """,
                "create table y as (select rnd_uuid4() b from long_sequence(5))",
                "create table x as (select rnd_uuid4() a from long_sequence(5))",
                false
        );

        testUnion(
                """
                        a
                        0010cde8-12ce-40ee-8010-a928bb8b9650
                        9f9b2131-d49f-4d1d-ab81-39815c50d341
                        7bcd48d8-c77a-4655-b2a2-15ba0462ad15
                        b5b2159a-2356-4217-965d-4c984f0ffa8a
                        e8beef38-cd7b-43d8-9b2d-34586f6275fa
                        322a2198-864b-4b14-b97f-a69eb8fec6cc
                        980eca62-a219-40f1-a846-d7a3aa5aecce
                        c1e63128-5c1a-4288-872b-fc5230158059
                        716de3d2-5dcc-4d91-9fa2-397a5d8c84c4
                        4b0f595f-143e-4d72-af1a-8266e7921e3b
                        """
        );
    }

    @Test
    public void testUuidVarchar() throws Exception {
        testUnionAll(
                """
                        a
                        acb025f7-59cf-4bd0-9e9b-e4e331fe36e6
                        8fd449ba-3259-4a2b-9beb-329042090bb3
                        b482cff5-7e9c-4398-ac09-f1b4db297f07
                        dbd7587f-2077-4576-9b4b-ae41862e09cc
                        7ee6a03f-4f93-4fa3-9d6c-b7b4fbf1fa48
                        &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L
                        8#3TsZ
                        zV衞͛Ԉ龘и\uDA89\uDFA4~
                        ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ
                        \uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46OF
                        """,
                "create table x as (select rnd_uuid4() a from long_sequence(5))",
                "create table y as (select rnd_varchar() b from long_sequence(5))",
                true
        );
    }

    @Test
    public void testVarcharDecimal() throws Exception {
        testUnionAll(
                """
                        a
                        1.000
                        2.000
                        3.000
                        4.000
                        5.000
                        0.000
                        3.000
                        5.000
                        9.000
                        8.000
                        """,
                "create table x as (select x::decimal(19, 3) a from long_sequence(5))",
                "create table y as (select '0'::varchar a union select '3'::varchar union select '5'::varchar union select '9'::varchar union select '8'::varchar)",
                false
        );
    }

    @Test
    public void testVarcharNull() throws Exception {
        testUnionAllWithNull(
                """
                        a\tc
                        衞͛Ԉ\t
                        \uD93C\uDEC1ӍK\t
                        \uD905\uDCD0\\ꔰ\t
                        \u008B}ѱ\t
                        \uD96C\uDF5FƐ㙎\t
                        \t\u1755\uDA1F\uDE98|
                        \t鈄۲ӄ
                        \tȞ鼷G
                        \t\uF644䶓z
                        \t
                        """,
                "rnd_varchar(3,3,1)"
        );
    }

    @Test
    public void testVarcharVarchar() throws Exception {
        // we include byte <-> bool cast to make sure
        // bool <-> bool cast it not thrown away as redundant
        testUnionAll(
                """
                        a\tc
                        false\tZ끫\uDB53\uDEDA
                        false\t"\uDB87\uDFA35
                        false\t톬F\uD9E6\uDECD
                        false\tЃَᯤ
                        false\t篸{\uD9D7\uDFE5
                        76\t핕\u05FA씎鈄
                        21\t\uDB8C\uDD1BȞ鼷G
                        35\t\uD8D1\uDD54ZzV
                        117\tB͛Ԉ龘
                        103\tL➤~2
                        """,
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
