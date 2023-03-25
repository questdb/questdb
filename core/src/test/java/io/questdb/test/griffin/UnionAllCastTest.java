/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.test.AbstractGriffinTest;
import org.junit.Test;

public class UnionAllCastTest extends AbstractGriffinTest {

    @Test
    public void testAllNoCast() throws Exception {
        // we include byte <-> bool cast to make sure
        // sym <-> sym cast it not thrown away as redundant
        testUnionAll(
                "a\tb\tc\td\te\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tp\tr\n" +
                        "true\t51\tI\t-24455\t-230430837\t-8323443786521150653\t0.0212\t0.33747075654972813\t1970-01-01T00:00:55.172Z\t1970-01-01T00:00:00.000001Z\t0x994c39efcb88eb88810d53a53367e79138e4be9e19321b57832dd27952d949d8\t00000000 a5 18 93 bd 0b 61 f5 5d d0 eb\t010100\t01001111011100\tzyjh\tjzgum6yb\tPGLUO\n" +
                        "true\t42\tZ\t-23702\t-210935524\t4502522085684189707\t0.7668\t0.4416432347777828\t1970-01-01T00:32:23.671Z\t1970-01-01T00:00:00.000001Z\t0x30f5a8b9a8a2672d1fae11870231bf0c54f33e997d4c2e14540bc0127276f42c\t00000000 84 52 d9 6f 04 ab 27 47 8f 23\t010001\t01100011100011\t46ng\tkwezzxdv\tNWIFF\n" +
                        "true\t102\tB\t25721\t-1309308188\t8490886945852172597\t0.0930\t0.706473302224657\t1970-01-01T01:11:40.508Z\t1970-01-01T00:00:00.000001Z\t0x4a27205d291d7f124c83d07de0778e771bd70aaefd486767588c16c272288827\t00000000 52 d0 29 26 c5 aa da 18 ce 5f\t000001\t11011011111101\te7d9\t22y0ef3h\t\n" +
                        "false\t36\tC\t2578\t-799774729\t812677186520066053\t0.0891\t0.7873229912811514\t1970-01-01T00:50:51.249Z\t1970-01-01T00:00:00.000000Z\t0x7cb055c54725b9527a19164d807cee6134570a2bee44673552c395ffb8982d58\t\t111100\t11010010000101\tsgzj\txp0bvd4d\tDTNPH\n" +
                        "false\t33\tP\t-21417\t992057087\t-6482694999745905510\t0.9130\t0.8813290192134411\t1970-01-01T01:55:59.541Z\t1970-01-01T00:00:00.000001Z\t0x7d4dc4398a4592a6561fb054b568947d3e2ed2ca0fea47416fff79101ec5c1cf\t\t110100\t00100001101110\trr8p\tf6fdj7pr\t\n" +
                        "false\t102\tJ\t-13027\t73575701\t8920866532787660373\t0.2992\t0.0843832076262595\t1970-01-01T00:10:02.536Z\t1970-01-01T00:00:00.000000Z\t0xc1e631285c1ab288c72bfc5230158059980eca62a219a0f16846d7a3aa5aecce\t00000000 91 3b 72 db f3 04 1b c7 88 de\t110110\t11001100100010\txn8n\t0n2gm6r7\tHFOWL\n" +
                        "false\t40\tX\t4635\t-342047842\t6854658259142399220\t0.1911\t0.4022810626779558\t1970-01-01T01:24:18.302Z\t1970-01-01T00:00:00.000001Z\t0x3239ad1b0411a66a10bb226eb4243e3683b91ec970b04e788a50f7ff7f6ed330\t00000000 de e4 7c d2 35 07 42 fc 31 79\t110000\t11110001011010\tp1d7\tp2n3hk69\t\n" +
                        "true\t88\tZ\t-13523\t-360860352\t-7266580375914176030\t0.7998\t0.16381374773748514\t1970-01-01T00:22:51.747Z\t1970-01-01T00:00:00.000001Z\t0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\t00000000 ac 37 c8 cd 82 89 2b 4d 5f f6\t010101\t11100011000001\tu7g3\tqjuztt7j\tCKYLS\n" +
                        "true\t42\tD\t27519\t1235206821\t-4116381468144676168\t0.4856\t0.5065228336156442\t1970-01-01T02:04:31.430Z\t1970-01-01T00:00:00.000001Z\t0xacb025f759cffbd0de9be4e331fe36e67dc859770af204938151081b8acafadd\t\t101100\t10011001010000\tsfw9\tvbw85v0q\t\n" +
                        "false\t64\tV\t-31322\t-283321892\t3446015290144635451\t0.1064\t0.49765193229684157\t1970-01-01T02:44:02.834Z\t1970-01-01T00:00:00.000001Z\t0xb20e1900caff819aaec65e34419d1077db217d41156b2ee1a90c04663c808638\t\t000000\t11010111011010\tpp3d\tur9ps7wg\tYPHRI\n",
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
                        " rnd_str(5,5,1) r " +
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
                        " rnd_str(5,5,1) r " +
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
        testUnionAll("a\tc\n" +
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
                "create table x as (select rnd_boolean() a, null c from long_sequence(5))",
                "create table y as (select null b, rnd_boolean() c from long_sequence(5))",
                false
        );

        testUnion(
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
    public void testByteNull() throws Exception {
        testUnionAll(
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
                "create table x as (select rnd_byte() a, null c from long_sequence(5))",
                "create table y as (select null b, rnd_byte() c from long_sequence(5))"
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
                "create table x as (select rnd_byte() a from long_sequence(5))",
                "create table y as (select rnd_char() b from long_sequence(5))"
        );
    }

    @Test
    public void testCharNull() throws Exception {
        testUnionAll(
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
                "create table x as (select rnd_char() a, null c from long_sequence(5))",
                "create table y as (select null b, rnd_char() c from long_sequence(5))"
        );
    }

    @Test
    public void testCharShort() throws Exception {
        testUnionAll(
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
                "create table x as (select rnd_short() a from long_sequence(5))",
                "create table y as (select rnd_char() b from long_sequence(5))"
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
        testUnionAll(
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
                "create table x as (select rnd_date() a, null c from long_sequence(5))",
                "create table y as (select null b, rnd_date() c from long_sequence(5))"
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
        testUnionAll(
                "a\tc\n" +
                        "0.6508594025855301\tNaN\n" +
                        "0.8423410920883345\tNaN\n" +
                        "0.9856290845874263\tNaN\n" +
                        "0.22452340856088226\tNaN\n" +
                        "0.5093827001617407\tNaN\n" +
                        "NaN\t0.6607777894187332\n" +
                        "NaN\t0.2246301342497259\n" +
                        "NaN\t0.08486964232560668\n" +
                        "NaN\t0.299199045961845\n" +
                        "NaN\t0.20447441837877756\n",
                "create table x as (select rnd_double() a, null c from long_sequence(5))",
                "create table y as (select null b, rnd_double() c from long_sequence(5))"
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
            compiler.compile("create table events1 (contact symbol, groupid float, eventid string)", sqlExecutionContext);
            executeInsert("insert into events1 values ('1', 1.5, 'flash')");
            executeInsert("insert into events1 values ('2', 1.5, 'stand')");

            compiler.compile("create table events2 (contact symbol, groupid double, eventid string)", sqlExecutionContext);
            executeInsert("insert into events2 values ('1', 1.5, 'flash')");
            executeInsert("insert into events2 values ('2', 1.5, 'stand')");

            assertQuery(
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
            compiler.compile("create table events1 (contact symbol, groupid float, eventid string)", sqlExecutionContext);
            executeInsert("insert into events1 values ('1', 1.5, 'flash')");
            executeInsert("insert into events1 values ('2', 1.5, 'stand')");
            executeInsert("insert into events1 values ('1', 1.6, 'stand')");
            executeInsert("insert into events1 values ('2', 1.6, 'stand')");

            compiler.compile("create table events2 (contact symbol, groupid double, eventid string)", sqlExecutionContext);
            executeInsert("insert into events2 values ('1', 1.5, 'flash')");
            executeInsert("insert into events2 values ('2', 1.5, 'stand')");

            assertQuery(
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
            compiler.compile("create table events1 (contact symbol, groupid double, eventid string)", sqlExecutionContext);
            executeInsert("insert into events1 values ('1', 1.5, 'flash')");
            executeInsert("insert into events1 values ('2', 1.5, 'stand')");

            compiler.compile("create table events2 (contact symbol, groupid float, eventid string)", sqlExecutionContext);
            executeInsert("insert into events2 values ('1', 1.5, 'flash')");
            executeInsert("insert into events2 values ('2', 1.5, 'stand')");

            assertQuery(
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
            compiler.compile("create table events1 (contact symbol, groupid double, eventid string)", sqlExecutionContext);
            executeInsert("insert into events1 values ('1', 1.5, 'flash')");
            executeInsert("insert into events1 values ('2', 1.5, 'stand')");
            executeInsert("insert into events1 values ('1', 1.6, 'stand')");
            executeInsert("insert into events1 values ('2', 1.6, 'stand')");

            compiler.compile("create table events2 (contact symbol, groupid double, eventid string)", sqlExecutionContext);
            executeInsert("insert into events2 values ('1', 1.5, 'flash')");
            executeInsert("insert into events2 values ('2', 1.5, 'stand')");

            assertQuery(
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
                        "0.6608\n" +
                        "0.8043\n" +
                        "0.2246\n" +
                        "0.1297\n" +
                        "0.0849\n",
                "create table x as (select rnd_boolean() a from long_sequence(5))",
                "create table y as (select rnd_float() b from long_sequence(5))",
                false
        );

        testUnion(
                "a\n" +
                        "false\n" +
                        "true\n" +
                        "0.6608\n" +
                        "0.8043\n" +
                        "0.2246\n" +
                        "0.1297\n" +
                        "0.0849\n"
        );
    }

    @Test
    public void testFloatByte() throws Exception {
        testUnionAll(
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
                        "0.6608\n" +
                        "0.8043\n" +
                        "0.2246\n" +
                        "0.1297\n" +
                        "0.0849\n",
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
                        "0.6608\n" +
                        "0.8043\n" +
                        "0.2246\n" +
                        "0.1297\n" +
                        "0.0849\n",
                "create table x as (select rnd_int() a from long_sequence(5))",
                "create table y as (select rnd_float() b from long_sequence(5))"
        );
    }

    @Test
    public void testFloatLong() throws Exception {
        testUnionAll(
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
                "create table x as (select rnd_float() a from long_sequence(5))",
                "create table y as (select rnd_long() b from long_sequence(5))"
        );
    }

    @Test
    public void testFloatNull() throws Exception {
        testUnionAll(
                "a\tc\n" +
                        "0.2846\tNaN\n" +
                        "0.2992\tNaN\n" +
                        "0.0844\tNaN\n" +
                        "0.2045\tNaN\n" +
                        "0.9345\tNaN\n" +
                        "NaN\t0.6608\n" +
                        "NaN\t0.8043\n" +
                        "NaN\t0.2246\n" +
                        "NaN\t0.1297\n" +
                        "NaN\t0.0849\n",
                "create table x as (select rnd_float() a, null c from long_sequence(5))",
                "create table y as (select null b, rnd_float() c from long_sequence(5))"
        );
    }

    @Test
    public void testFloatShort() throws Exception {
        testUnionAll(
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
        testUnionAll(
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
                "create table x as (select rnd_geohash(7) a, null c from long_sequence(5))",
                "create table y as (select null b, rnd_geohash(7) c from long_sequence(5))"
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
        testUnionAll(
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
                "create table x as (select rnd_geohash(30) a, null c from long_sequence(5))",
                "create table y as (select null b, rnd_geohash(30) c from long_sequence(5))"
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
        testUnionAll(
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
                "create table x as (select rnd_geohash(40) a, null c from long_sequence(5))",
                "create table y as (select null b, rnd_geohash(40) c from long_sequence(5))"
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
        testUnionAll(
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
                "create table x as (select rnd_geohash(15) a, null c from long_sequence(5))",
                "create table y as (select null b, rnd_geohash(15) c from long_sequence(5))"
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
                "create table x as (select rnd_char() a from long_sequence(5))",
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
        testUnionAll(
                "a\tc\n" +
                        "-948263339\tNaN\n" +
                        "1326447242\tNaN\n" +
                        "592859671\tNaN\n" +
                        "1868723706\tNaN\n" +
                        "-847531048\tNaN\n" +
                        "NaN\t-1148479920\n" +
                        "NaN\t315515118\n" +
                        "NaN\t1548800833\n" +
                        "NaN\t-727724771\n" +
                        "NaN\t73575701\n",
                "create table x as (select rnd_int() a, null c from long_sequence(5))",
                "create table y as (select null b, rnd_int() c from long_sequence(5))"
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
            compiler.compile("create table events1 (contact symbol, groupid float, eventid string)", sqlExecutionContext);
            executeInsert("insert into events1 values ('1', 1.5, 'flash')");
            executeInsert("insert into events1 values ('2', 1.5, 'stand')");
            executeInsert("insert into events1 values ('1', 1.6, 'stand')");
            executeInsert("insert into events1 values ('2', 1.6, 'stand')");

            compiler.compile("create table events2 (contact symbol, groupid double, eventid string)", sqlExecutionContext);
            executeInsert("insert into events2 values ('1', 1.5, 'flash')");
            executeInsert("insert into events2 values ('2', 1.5, 'stand')");

            assertQuery(
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
            compiler.compile("create table events1 (contact symbol, groupid double, eventid string)", sqlExecutionContext);
            executeInsert("insert into events1 values ('1', 1.5, 'flash')");
            executeInsert("insert into events1 values ('2', 1.5, 'stand')");
            executeInsert("insert into events1 values ('1', 1.6, 'stand')");
            executeInsert("insert into events1 values ('2', 1.6, 'stand')");

            compiler.compile("create table events2 (contact symbol, groupid double, eventid string)", sqlExecutionContext);
            executeInsert("insert into events2 values ('1', 1.5, 'flash')");
            executeInsert("insert into events2 values ('2', 1.5, 'stand')");

            assertQuery(
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
        testUnionAll(
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
                "create table x as (select rnd_long256() a, null c from long_sequence(5))",
                "create table y as (select null b, rnd_long256() c from long_sequence(5))"
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
                "create table x as (select rnd_char() a from long_sequence(5))",
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
        testUnionAll(
                "a\tc\n" +
                        "8920866532787660373\tNaN\n" +
                        "-7611843578141082998\tNaN\n" +
                        "-5354193255228091881\tNaN\n" +
                        "-2653407051020864006\tNaN\n" +
                        "-1675638984090602536\tNaN\n" +
                        "NaN\t4689592037643856\n" +
                        "NaN\t4729996258992366\n" +
                        "NaN\t7746536061816329025\n" +
                        "NaN\t-6945921502384501475\n" +
                        "NaN\t8260188555232587029\n",
                "create table x as (select rnd_long() a, null c from long_sequence(5))",
                "create table y as (select null b, rnd_long() c from long_sequence(5))"
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
                "create table x as (select rnd_long() a from long_sequence(5))",
                "create table y as (select cast(rnd_long() as timestamp) b from long_sequence(5))",
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
                        "80\n" +
                        "83\n" +
                        "87\n" +
                        "72\n" +
                        "89\n" +
                        "-27056\n" +
                        "24814\n" +
                        "-11455\n" +
                        "-13027\n" +
                        "-21227\n",
                "create table x as (select rnd_char() a from long_sequence(5))",
                "create table y as (select rnd_short() b from long_sequence(5))"
        );
    }

    @Test
    public void testShortNull() throws Exception {
        testUnionAll(
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
                "create table x as (select rnd_short() a, null c from long_sequence(5))",
                "create table y as (select null b, rnd_short() c from long_sequence(5))"
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
    public void testStringNull() throws Exception {
        testUnionAll(
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
                "create table x as (select rnd_str(3,3,1) a, null c from long_sequence(5))",
                "create table y as (select null b, rnd_str(3,3,1) c from long_sequence(5))",
                false
        );

        testUnion(
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
        testUnionAll("a\tc\n" +
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
                "create table x as (select rnd_symbol('aa','bb') a, null c from long_sequence(5))",
                "create table y as (select null b, rnd_symbol('aa','bb') c from long_sequence(5))",
                false
        );

        testUnion(
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
        compile("create table y as (select rnd_double() u, rnd_byte() b, rnd_symbol('x','y') c from long_sequence(5))");
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
    public void testTimestampNull() throws Exception {
        testUnionAll(
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
                "create table x as (select rnd_timestamp(0, 100000, 2) a, null c from long_sequence(5))",
                "create table y as (select null b, rnd_timestamp(0, 100000, 2) c from long_sequence(5))",
                false
        );

        testUnion(
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
        testUnionAll(
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
                "create table x as (select rnd_uuid4() a, null c from long_sequence(5))",
                "create table y as (select null b, rnd_uuid4() c from long_sequence(5))"
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
                false
        );

        testUnion(
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
                        "YUDEYYQEHB\n"
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

    private void assertFailure(String ddlX, String ddlY, int pos) throws Exception {
        compile(ddlY);
        engine.releaseAllWriters();
        assertFailure("x union all y",
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
        compile(ddlY);
        engine.releaseAllWriters();
        assertQuery(expected, sql, ddlX, null, false, true);
    }
}
