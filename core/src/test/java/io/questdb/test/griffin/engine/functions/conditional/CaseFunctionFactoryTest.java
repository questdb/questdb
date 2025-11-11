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

package io.questdb.test.griffin.engine.functions.conditional;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CaseFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBinary() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t00000000 ee 41 1d 15 55 8a 17 fa d8 cc 14 ce f1 59 88 c4
                        00000010 91 3b 72 db f3 04 1b c7 88 de a0 79 3c 77 15 68
                        -352\t00000000 e2 4b b1 3e e3 f1 f1 1e ca 9c 1d 06 ac 37 c8 cd
                        00000010 82 89 2b 4d 5f f6 46 90 c3 b3 59 8e e5 61 2f 64
                        -743\t00000000 14 58 63 b7 c2 9f 29 8e 29 5e 69 c6 eb ea c3 c9
                        00000010 73 93 46 fe c2 d3 68 79 8b 43 1d 57 34 04 23 8d
                        -601\t00000000 ae 7c 9f 77 04 e9 0c ea 4e ea 8b f5 0f 2d b3 14
                        00000010 33 80 c9 eb a3 67 7a 1a 79 e4 35 e4 3a dc 5c 65
                        -398\t00000000 f5 6e 8f 80 e3 54 b8 07 b1 32 57 ff 9a ef 88 cb
                        00000010 4b a1 cf cf 41 7d a6 d1 3e b4 48 d4 41 9d fb 49
                        437\t
                        -231\t00000000 19 ca f2 bf 84 5a 6f 38 35 15 29 83 1f c3 2f ed
                        00000010 b0 ba 08 e0 2c ee 41 de b6 81 df b7 6c 4b fb 2d
                        19\t
                        215\t
                        819\t
                        15\t
                        -307\t00000000 25 07 db 62 44 33 6e 00 8e 93 bd 27 42 f8 25 2a
                        00000010 42 71 a3 7a 58 e5 78 b8 1c d6 fc 7a ac 4c 11 9e
                        -272\t00000000 71 ea 20 7e 43 97 27 1f 5c d9 ee 04 5b 9c 17 f2
                        00000010 8c bf 95 30 57 1d 91 72 30 04 b7 02 cb 03 23 61
                        -559\t00000000 6c 3e 51 d7 eb b1 07 71 32 1f af 40 4e 8c 47 84
                        00000010 e9 c0 55 12 44 dc 4b c0 d9 1c 71 cf 5a 8f 21 06
                        560\t
                        687\t
                        629\t
                        -592\t00000000 7d f4 03 ed c9 2a 4e 91 c5 e4 39 b2 dd 0d a7 bb
                        00000010 d5 71 72 ba 9c ac 89 76 dd e7 1f eb 30 58 15 38
                        -228\t00000000 1c dd fc d2 8e 79 ec 02 b2 31 9c 69 be 74 9a ad
                        00000010 cc cf b8 e4 d1 7a 4f fb 16 fa 19 a2 df 43 81 a2
                        625\t
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_bin() a," +
                        " rnd_bin() b," +
                        " rnd_bin() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testBinaryOrElse() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t00000000 ee 41 1d 15 55 8a 17 fa d8 cc 14 ce f1 59 88 c4
                        00000010 91 3b 72 db f3 04 1b c7 88 de a0 79 3c 77 15 68
                        -352\t00000000 e2 4b b1 3e e3 f1 f1 1e ca 9c 1d 06 ac 37 c8 cd
                        00000010 82 89 2b 4d 5f f6 46 90 c3 b3 59 8e e5 61 2f 64
                        -743\t00000000 14 58 63 b7 c2 9f 29 8e 29 5e 69 c6 eb ea c3 c9
                        00000010 73 93 46 fe c2 d3 68 79 8b 43 1d 57 34 04 23 8d
                        -601\t00000000 ae 7c 9f 77 04 e9 0c ea 4e ea 8b f5 0f 2d b3 14
                        00000010 33 80 c9 eb a3 67 7a 1a 79 e4 35 e4 3a dc 5c 65
                        -398\t00000000 f5 6e 8f 80 e3 54 b8 07 b1 32 57 ff 9a ef 88 cb
                        00000010 4b a1 cf cf 41 7d a6 d1 3e b4 48 d4 41 9d fb 49
                        437\t00000000 3b 47 3c e1 72 3b 9d ef c4 4a c9 cf fb 9d 63 ca
                        00000010 94 00 6b dd 18 fe 71 76 bc 45 24 cd 13 00 7c fb
                        -231\t00000000 19 ca f2 bf 84 5a 6f 38 35 15 29 83 1f c3 2f ed
                        00000010 b0 ba 08 e0 2c ee 41 de b6 81 df b7 6c 4b fb 2d
                        19\t00000000 f5 4b ea 01 c9 63 b4 fc 92 60 1f df 41 ec 2c 38
                        00000010 88 88 e7 59 40 10 20 81 c6 3d bc b5 05 2b 73 51
                        215\t00000000 f7 fe 9a 9e 1b fd a9 d7 0e 39 5a 28 ed 97 99 d8
                        00000010 77 33 3f b2 67 da 98 47 47 bf 4f ea 5f 48 ed f6
                        819\t00000000 01 b1 55 38 ad b2 4a 4e 7d 85 f9 39 25 42 67 78
                        00000010 47 b3 80 69 b9 14 d6 fc ee 03 22 81 b8 06 c4 06
                        15\t00000000 44 54 13 3f ff b6 7e cd 04 27 66 94 89 db 3c 1a
                        00000010 23 f3 88 83 73 1c 04 63 f9 ac 3d 61 6b 04 33 2b
                        -307\t00000000 25 07 db 62 44 33 6e 00 8e 93 bd 27 42 f8 25 2a
                        00000010 42 71 a3 7a 58 e5 78 b8 1c d6 fc 7a ac 4c 11 9e
                        -272\t00000000 71 ea 20 7e 43 97 27 1f 5c d9 ee 04 5b 9c 17 f2
                        00000010 8c bf 95 30 57 1d 91 72 30 04 b7 02 cb 03 23 61
                        -559\t00000000 6c 3e 51 d7 eb b1 07 71 32 1f af 40 4e 8c 47 84
                        00000010 e9 c0 55 12 44 dc 4b c0 d9 1c 71 cf 5a 8f 21 06
                        560\t00000000 15 3e 0c 7f 3f 8f e4 b5 ab 34 21 29 23 e8 17 ca
                        00000010 f4 c0 8e e1 15 9c aa 69 48 c3 a0 d6 14 8b 7f 03
                        687\t00000000 75 95 fa 1f 92 24 b1 b8 67 65 08 b7 f8 41 00 33
                        00000010 ac 30 77 91 b2 de 58 45 d0 1b 58 be 33 92 cd 5c
                        629\t00000000 49 10 e7 7c 3f d6 88 3a 93 ef 24 a5 e2 bc 86 f9
                        00000010 92 a3 f1 92 08 f1 96 7f a0 cf 00 74 7c 32 16 38
                        -592\t00000000 7d f4 03 ed c9 2a 4e 91 c5 e4 39 b2 dd 0d a7 bb
                        00000010 d5 71 72 ba 9c ac 89 76 dd e7 1f eb 30 58 15 38
                        -228\t00000000 1c dd fc d2 8e 79 ec 02 b2 31 9c 69 be 74 9a ad
                        00000010 cc cf b8 e4 d1 7a 4f fb 16 fa 19 a2 df 43 81 a2
                        625\t00000000 e4 85 f1 13 06 f2 27 0f 0c ae 8c 49 a1 ce bf 46
                        00000010 36 0d 5b 7f 48 92 ff 37 63 be 5f b7 70 a0 07 8f
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else c
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_bin() a," +
                        " rnd_bin() b," +
                        " rnd_bin() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testBindVar() throws Exception {
        assertException(
                """
                        select\s
                            a,
                            case
                                when a > 10 then $1
                                else $2
                            end k
                        from test""",
                "create table test as (select cast(x as long) a, timestamp_sequence(0, 1000000) ts from long_sequence(5))",
                49,
                "CASE values cannot be bind variables"
        );
    }

    @Test
    public void testBindVarInElse() throws Exception {
        assertException(
                """
                        select\s
                            a,
                            case
                                when a > 10 then '>10'
                                else $2
                            end k
                        from test""",
                "create table test as (select cast(x as long) a, timestamp_sequence(0, 1000000) ts from long_sequence(5))",
                68,
                "CASE values cannot be bind variables"
        );
    }

    @Test
    public void testBoolean() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\tfalse
                        701\tfalse
                        706\tfalse
                        -714\ttrue
                        116\tfalse
                        67\tfalse
                        207\tfalse
                        -55\tfalse
                        -104\ttrue
                        -127\tfalse
                        790\tfalse
                        881\tfalse
                        -535\tfalse
                        -973\tfalse
                        -463\tfalse
                        -667\ttrue
                        578\tfalse
                        940\tfalse
                        -54\tfalse
                        -393\tfalse
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_boolean() a," +
                        " rnd_boolean() b," +
                        " rnd_boolean() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testBooleanOrElse() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\tfalse
                        701\ttrue
                        706\tfalse
                        -714\ttrue
                        116\tfalse
                        67\tfalse
                        207\ttrue
                        -55\tfalse
                        -104\ttrue
                        -127\tfalse
                        790\ttrue
                        881\tfalse
                        -535\tfalse
                        -973\tfalse
                        -463\tfalse
                        -667\ttrue
                        578\ttrue
                        940\ttrue
                        -54\tfalse
                        -393\tfalse
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else c
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_boolean() a," +
                        " rnd_boolean() b," +
                        " rnd_boolean() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testByte() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t102
                        701\t0
                        706\t0
                        -714\t55
                        116\t91
                        67\t0
                        207\t0
                        -55\t84
                        -104\t35
                        -127\t56
                        790\t0
                        881\t0
                        -535\t26
                        -973\t34
                        -463\t103
                        -667\t44
                        578\t0
                        940\t0
                        -54\t112
                        -393\t55
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_byte() a," +
                        " rnd_byte() b," +
                        " rnd_byte() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testByteOrElse() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t102
                        701\t83
                        706\t84
                        -714\t55
                        116\t91
                        67\t45
                        207\t60
                        -55\t84
                        -104\t35
                        -127\t56
                        790\t32
                        881\t24
                        -535\t26
                        -973\t34
                        -463\t103
                        -667\t44
                        578\t28
                        940\t43
                        -54\t112
                        -393\t55
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else c
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_byte() a," +
                        " rnd_byte() b," +
                        " rnd_byte() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testByteToVarcharCast() throws Exception {
        execute(
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_varchar() a," +
                        " rnd_varchar() b," +
                        " rnd_varchar() c" +
                        " from long_sequence(5)" +
                        ")"
        );
        assertException(
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else 3::byte\
                            end\s
                        from tanc""",
                104,
                "inconvertible types: BYTE -> VARCHAR [from=BYTE, to=VARCHAR]"
        );
    }

    @Test
    public void testCaseErrors() throws Exception {
        assertException("select case from long_sequence(1)", 7, "unbalanced 'case'");
        assertException("select case end from long_sequence(1)", 12, "'when' expected");
        assertException("select case x end from long_sequence(1)", 14, "'when' expected");
        assertException("select case 1 end from long_sequence(1)", 14, "'when' expected");
        assertException("select case false end from long_sequence(1)", 18, "'when' expected");
        assertException("select case x/2 end from long_sequence(1)", 16, "'when' expected");
        assertException("select case x/2 z end from long_sequence(1)", 18, "'when' expected");
        assertException("select case 1+5 end from long_sequence(1)", 16, "'when' expected");
        assertException("select case rnd_double() end from long_sequence(1)", 25, "'when' expected");
        assertException("select case x else 2 end from long_sequence(1)", 14, "'when' expected");
        assertException("select case x when else 2 end from long_sequence(1)", 19, "missing arguments");
        assertException("select case x when 1 end from long_sequence(1)", 21, "'then' expected");
        assertException("select case x when 1 else 2 end from long_sequence(1)", 21, "'then' expected");
        assertException("select case x when 1 then else 2 end from long_sequence(1)", 26, "missing arguments");
        assertException("select case x when 1 then 1 else else end from long_sequence(1)", 33, "missing arguments");
        assertException("select case x when 1 then 1 when else 2 end from long_sequence(1)", 33, "missing arguments");
        assertException("select case x when 1 then 1 when 2 else 2 end from long_sequence(1)", 35, "'then' expected");
        assertException("select case when end from long_sequence(1)", 17, "missing arguments");
        assertException("select case when then else end from long_sequence(1)", 17, "missing arguments");
        assertException("select case when else end from long_sequence(1)", 17, "missing arguments");
        assertException("select case when x end from long_sequence(1)", 19, "'then' expected");
        assertException("select case when x else end from long_sequence(1)", 19, "'then' expected");
        assertException("select case when x else 2 end from long_sequence(1)", 19, "'then' expected");
        assertException("select case when x then end from long_sequence(1)", 24, "missing arguments");
        assertException("select case when x then else end from long_sequence(1)", 24, "missing arguments");
        assertException("select case when x then x else end from long_sequence(1)", 31, "missing arguments");
    }

    @Test
    public void testCaseWithNoElseInSelectClause() throws Exception {
        assertQuery("c\n0\nnull\nnull\n",
                "select case x when 1 then 0 end c from long_sequence(3)", null, true, true
        );

        assertQuery("c\nnull\nnull\nnull\n",
                "select case x when -1 then 0 end c from long_sequence(3)", null, true, true
        );

        assertQuery("c\n0\n0\n0\n",
                "select case when x<5 then 0 end c from long_sequence(3)", null, true, true
        );

        assertQuery("c\n0\nnull\nnull\n",
                "select case when x<2 then 0 end c from long_sequence(3)", null, true, true
        );

        assertQuery("c\n1\n",
                "select case when true then 1 end c", null, true, true
        );

        assertQuery("c\nnull\n",
                "select case when false then 2 end c", null, true, true
        );
    }

    @Test
    public void testCaseWithNoElseInWhereClause() throws Exception {
        assertException("select x from long_sequence(3) where case x when 1 then 0 end", 37, "boolean expression expected");

        assertQuery("x\n1\n",
                "select x from long_sequence(3) where case when x<2 then true end", null, true, false
        );

        assertQuery("x\n1\n2\n",
                "select x from long_sequence(3) where case when x<3 then true else false end", null, true, false
        );

        assertQuery("x\n1\n",
                "select x from long_sequence(3) where case when x<2 then true when x<3 then false end", null, true, false
        );

        assertQuery("x\n",
                "select x from long_sequence(3) where case when false then true end", null, false, false
        );
    }

    @Test
    public void testChar() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\tT
                        701\t
                        706\t
                        -714\tE
                        116\tG
                        67\t
                        207\t
                        -55\tP
                        -104\tF
                        -127\tE
                        790\t
                        881\t
                        -535\tP
                        -973\tS
                        -463\tU
                        -667\tH
                        578\t
                        940\t
                        -54\tJ
                        -393\tJ
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_char() a," +
                        " rnd_char() b," +
                        " rnd_char() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testCharOrElse() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\tT
                        701\tW
                        706\tX
                        -714\tE
                        116\tG
                        67\tX
                        207\tT
                        -55\tP
                        -104\tF
                        -127\tE
                        790\tB
                        881\tW
                        -535\tP
                        -973\tS
                        -463\tU
                        -667\tH
                        578\tQ
                        940\tO
                        -54\tJ
                        -393\tJ
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else c
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_char() a," +
                        " rnd_char() b," +
                        " rnd_char() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testCharToVarcharCast() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t&BT+
                        363\tf
                        367\tf
                        895\tf
                        -6\t1W씌䒙\uD8F2\uDE8E>
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else 'f'::char\
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_varchar() a," +
                        " rnd_varchar() b," +
                        " rnd_varchar() c" +
                        " from long_sequence(5)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDate() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t1970-01-01T02:29:52.366Z
                        701\t
                        706\t
                        -714\t1970-01-01T01:29:21.807Z
                        116\t1970-01-01T00:07:58.011Z
                        67\t
                        207\t
                        -55\t1970-01-01T01:19:45.212Z
                        -104\t1970-01-01T01:06:13.663Z
                        -127\t1970-01-01T01:59:29.860Z
                        790\t
                        881\t
                        -535\t1970-01-01T01:35:56.830Z
                        -973\t1970-01-01T00:16:27.550Z
                        -463\t1970-01-01T01:14:59.209Z
                        -667\t1970-01-01T00:32:33.360Z
                        578\t
                        940\t
                        -54\t1970-01-01T02:29:09.756Z
                        -393\t1970-01-01T02:34:54.347Z
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_date() a," +
                        " rnd_date() b," +
                        " rnd_date() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDateOrElse() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t1970-01-01T02:29:52.366Z
                        701\t1970-01-01T02:14:51.881Z
                        706\t1970-01-01T00:01:52.276Z
                        -714\t1970-01-01T01:29:21.807Z
                        116\t1970-01-01T00:07:58.011Z
                        67\t1970-01-01T02:04:33.051Z
                        207\t1970-01-01T00:04:23.904Z
                        -55\t1970-01-01T01:19:45.212Z
                        -104\t1970-01-01T01:06:13.663Z
                        -127\t1970-01-01T01:59:29.860Z
                        790\t1970-01-01T01:46:42.508Z
                        881\t1970-01-01T02:35:38.504Z
                        -535\t1970-01-01T01:35:56.830Z
                        -973\t1970-01-01T00:16:27.550Z
                        -463\t1970-01-01T01:14:59.209Z
                        -667\t1970-01-01T00:32:33.360Z
                        578\t1970-01-01T01:08:40.476Z
                        940\t1970-01-01T00:34:14.137Z
                        -54\t1970-01-01T02:29:09.756Z
                        -393\t1970-01-01T02:34:54.347Z
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else c
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_date() a," +
                        " rnd_date() b," +
                        " rnd_date() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDecimal128() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -10\t123456789012345678.123456789012345678
                        0\t
                        50\t
                        150\t987654321098765432.987654321098765432
                        250\t
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select x," +
                        " cast(123456789012345678.123456789012345678 as DECIMAL(36,18)) a," +
                        " cast(987654321098765432.987654321098765432 as DECIMAL(36,18)) b," +
                        " cast(111111111111111111.111111111111111111 as DECIMAL(36,18)) c" +
                        " from (select -10 x union all select 0 union all select 50 union all select 150 union all select 250)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDecimal128OrElse() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -10\t123456789012345678.123456789012345678
                        0\t111111111111111111.111111111111111111
                        50\t111111111111111111.111111111111111111
                        150\t987654321098765432.987654321098765432
                        250\t111111111111111111.111111111111111111
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else c
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select x," +
                        " cast(123456789012345678.123456789012345678 as DECIMAL(36,18)) a," +
                        " cast(987654321098765432.987654321098765432 as DECIMAL(36,18)) b," +
                        " cast(111111111111111111.111111111111111111 as DECIMAL(36,18)) c" +
                        " from (select -10 x union all select 0 union all select 50 union all select 150 union all select 250)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDecimal16() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -10\t12.34
                        0\t
                        50\t
                        150\t56.78
                        250\t
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select x," +
                        " cast(12.34 as DECIMAL(4,2)) a," +
                        " cast(56.78 as DECIMAL(4,2)) b," +
                        " cast(99.99 as DECIMAL(4,2)) c" +
                        " from (select -10 x union all select 0 union all select 50 union all select 150 union all select 250)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDecimal16OrElse() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -10\t12.34
                        0\t99.99
                        50\t99.99
                        150\t56.78
                        250\t99.99
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else c
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select x," +
                        " cast(12.34 as DECIMAL(4,2)) a," +
                        " cast(56.78 as DECIMAL(4,2)) b," +
                        " cast(99.99 as DECIMAL(4,2)) c" +
                        " from (select -10 x union all select 0 union all select 50 union all select 150 union all select 250)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDecimal256() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -10\t1234567890123456789012345678901234567890.12345678901234567890123456789012345
                        0\t
                        50\t
                        150\t9876543210987654321098765432109876543210.98765432109876543210987654321098765
                        250\t
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select x," +
                        " cast(1234567890123456789012345678901234567890.12345678901234567890123456789012345 as DECIMAL(76,35)) a," +
                        " cast(9876543210987654321098765432109876543210.98765432109876543210987654321098765 as DECIMAL(76,35)) b," +
                        " cast(1111111111111111111111111111111111111111.11111111111111111111111111111111111 as DECIMAL(76,35)) c" +
                        " from (select -10 x union all select 0 union all select 50 union all select 150 union all select 250)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDecimal256OrElse() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -10\t1234567890123456789012345678901234567890.12345678901234567890123456789012345
                        0\t1111111111111111111111111111111111111111.11111111111111111111111111111111111
                        50\t1111111111111111111111111111111111111111.11111111111111111111111111111111111
                        150\t9876543210987654321098765432109876543210.98765432109876543210987654321098765
                        250\t1111111111111111111111111111111111111111.11111111111111111111111111111111111
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else c
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select x," +
                        " cast(1234567890123456789012345678901234567890.12345678901234567890123456789012345 as DECIMAL(76,35)) a," +
                        " cast(9876543210987654321098765432109876543210.98765432109876543210987654321098765 as DECIMAL(76,35)) b," +
                        " cast(1111111111111111111111111111111111111111.11111111111111111111111111111111111 as DECIMAL(76,35)) c" +
                        " from (select -10 x union all select 0 union all select 50 union all select 150 union all select 250)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDecimal32() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -10\t12345.6789
                        0\t
                        50\t
                        150\t98765.4321
                        250\t
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select x," +
                        " cast(12345.6789 as DECIMAL(9,4)) a," +
                        " cast(98765.4321 as DECIMAL(9,4)) b," +
                        " cast(11111.1111 as DECIMAL(9,4)) c" +
                        " from (select -10 x union all select 0 union all select 50 union all select 150 union all select 250)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDecimal32OrElse() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -10\t12345.6789
                        0\t11111.1111
                        50\t11111.1111
                        150\t98765.4321
                        250\t11111.1111
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else c
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select x," +
                        " cast(12345.6789 as DECIMAL(9,4)) a," +
                        " cast(98765.4321 as DECIMAL(9,4)) b," +
                        " cast(11111.1111 as DECIMAL(9,4)) c" +
                        " from (select -10 x union all select 0 union all select 50 union all select 150 union all select 250)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDecimal64() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -10\t1234567890.12345678
                        0\t
                        50\t
                        150\t9876543210.98765432
                        250\t
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select x," +
                        " cast(1234567890.12345678 as DECIMAL(18,8)) a," +
                        " cast(9876543210.98765432 as DECIMAL(18,8)) b," +
                        " cast(1111111111.11111111 as DECIMAL(18,8)) c" +
                        " from (select -10 x union all select 0 union all select 50 union all select 150 union all select 250)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDecimal64OrElse() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -10\t1234567890.12345678
                        0\t1111111111.11111111
                        50\t1111111111.11111111
                        150\t9876543210.98765432
                        250\t1111111111.11111111
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else c
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select x," +
                        " cast(1234567890.12345678 as DECIMAL(18,8)) a," +
                        " cast(9876543210.98765432 as DECIMAL(18,8)) b," +
                        " cast(1111111111.11111111 as DECIMAL(18,8)) c" +
                        " from (select -10 x union all select 0 union all select 50 union all select 150 union all select 250)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDecimal8() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -10\t1.2
                        0\t
                        50\t
                        150\t5.6
                        250\t
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select x," +
                        " cast(1.2 as DECIMAL(2,1)) a," +
                        " cast(5.6 as DECIMAL(2,1)) b," +
                        " cast(9.9 as DECIMAL(2,1)) c" +
                        " from (select -10 x union all select 0 union all select 50 union all select 150 union all select 250)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDecimal8OrElse() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -10\t1.2
                        0\t9.9
                        50\t9.9
                        150\t5.6
                        250\t9.9
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else c
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select x," +
                        " cast(1.2 as DECIMAL(2,1)) a," +
                        " cast(5.6 as DECIMAL(2,1)) b," +
                        " cast(9.9 as DECIMAL(2,1)) c" +
                        " from (select -10 x union all select 0 union all select 50 union all select 150 union all select 250)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDouble() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t0.8043224099968393
                        671\tnull
                        481\tnull
                        147\t0.5243722859289777
                        -55\t0.7261136209823622
                        -769\t0.3100545983862456
                        -831\t0.5249321062686694
                        -914\t0.6217326707853098
                        -463\t0.12503042190293423
                        -194\t0.6761934857077543
                        -835\t0.7883065830055033
                        -933\t0.5522494170511608
                        416\tnull
                        380\tnull
                        -574\t0.7997733229967019
                        -722\t0.40455469747939254
                        -128\t0.8828228366697741
                        -842\t0.9566236549439661
                        -123\t0.9269068519549879
                        535\tnull
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_double() a," +
                        " rnd_double() b," +
                        " rnd_double() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDoubleOrElse() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t0.8043224099968393
                        671\t0.8423410920883345
                        481\t0.3491070363730514
                        147\t0.5243722859289777
                        -55\t0.7261136209823622
                        -769\t0.3100545983862456
                        -831\t0.5249321062686694
                        -914\t0.6217326707853098
                        -463\t0.12503042190293423
                        -194\t0.6761934857077543
                        -835\t0.7883065830055033
                        -933\t0.5522494170511608
                        416\t0.4900510449885239
                        380\t0.38642336707855873
                        -574\t0.7997733229967019
                        -722\t0.40455469747939254
                        -128\t0.8828228366697741
                        -842\t0.9566236549439661
                        -123\t0.9269068519549879
                        535\t0.49428905119584543
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else c
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_double() a," +
                        " rnd_double() b," +
                        " rnd_double() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDoubleToVarcharCast() throws Exception {
        execute(
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_varchar() a," +
                        " rnd_varchar() b," +
                        " rnd_varchar() c" +
                        " from long_sequence(5)" +
                        ")"
        );
        assertException(
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else 3.5::double\
                            end\s
                        from tanc""",
                106,
                "inconvertible types: DOUBLE -> VARCHAR [from=DOUBLE, to=VARCHAR]"
        );
    }

    @Test
    public void testEverythingIsNull() throws Exception {
        assertQuery(
                """
                        case
                        null
                        """,
                "select case when null is null then null else null end",
                true,
                true
        );
    }

    @Test
    public void testFloat() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t0.80432236
                        701\tnull
                        706\tnull
                        -714\t0.7905675
                        116\t0.50938267
                        67\tnull
                        207\tnull
                        -55\t0.7261136
                        -104\t0.6693837
                        -127\t0.87567717
                        790\tnull
                        881\tnull
                        -535\t0.21583223
                        -973\t0.81468076
                        -463\t0.1250304
                        -667\t0.9687423
                        578\tnull
                        940\tnull
                        -54\t0.81016123
                        -393\t0.37625015
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_float() a," +
                        " rnd_float() b," +
                        " rnd_float() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testFloatOrElse() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t0.80432236
                        701\t0.08438319
                        706\t0.13123357
                        -714\t0.7905675
                        116\t0.50938267
                        67\t0.46218354
                        207\t0.8072372
                        -55\t0.7261136
                        -104\t0.6693837
                        -127\t0.87567717
                        790\t0.5249321
                        881\t0.021651804
                        -535\t0.21583223
                        -973\t0.81468076
                        -463\t0.1250304
                        -667\t0.9687423
                        578\t0.48820508
                        940\t0.78830653
                        -54\t0.81016123
                        -393\t0.37625015
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else c
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_float() a," +
                        " rnd_float() b," +
                        " rnd_float() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testFloatToVarcharCast() throws Exception {
        execute(
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_varchar() a," +
                        " rnd_varchar() b," +
                        " rnd_varchar() c" +
                        " from long_sequence(5)" +
                        ")"
        );

        assertException(
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else 3.5::float\
                            end\s
                        from tanc""",
                106,
                "inconvertible types: DOUBLE -> VARCHAR [from=DOUBLE, to=VARCHAR]"
        );
    }

    @Test
    public void testIPv4ToVarcharCast() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table tanc as (" +
                            "select rnd_int() % 1000 x," +
                            " rnd_varchar() a," +
                            " rnd_varchar() b," +
                            " rnd_varchar() c" +
                            " from long_sequence(20)" +
                            ")"
            );

            assertSql(
                    """
                            x\tcase
                            -920\t
                            363\t127.0.0.1
                            367\t127.0.0.1
                            895\t127.0.0.1
                            -6\t
                            -440\t
                            905\t127.0.0.1
                            -212\t
                            569\t127.0.0.1
                            204\t127.0.0.1
                            -845\t
                            768\t127.0.0.1
                            343\t127.0.0.1
                            797\t127.0.0.1
                            34\t127.0.0.1
                            -365\t
                            895\t127.0.0.1
                            416\t127.0.0.1
                            -765\t
                            754\t127.0.0.1
                            """,
                    """
                            select\s
                                x,
                                case
                                    when x < 0 then a
                                    when x > 100 and x < 200 then b
                                    else '127.0.0.1'::ipv4\
                                end\s
                            from tanc"""
            );
        });
    }

    @Test
    public void testInt() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t315515118
                        701\tnull
                        706\tnull
                        -714\t-1575378703
                        116\t339631474
                        67\tnull
                        207\tnull
                        -55\t-1792928964
                        -104\t-1153445279
                        -127\t1631244228
                        790\tnull
                        881\tnull
                        -535\t-938514914
                        -973\t-342047842
                        -463\t-27395319
                        -667\t2137969456
                        578\tnull
                        940\tnull
                        -54\t-1162267908
                        -393\t-296610933
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then c
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntOrElse() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t315515118
                        701\t592859671
                        706\t-2041844972
                        -714\t-1575378703
                        116\t339631474
                        67\t-1458132197
                        207\t426455968
                        -55\t-1792928964
                        -104\t-1153445279
                        -127\t1631244228
                        790\t-212807500
                        881\t-113506296
                        -535\t-938514914
                        -973\t-342047842
                        -463\t-27395319
                        -667\t2137969456
                        578\t44173540
                        940\t1978144263
                        -54\t-1162267908
                        -393\t-296610933
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then c
                                else c
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntOrElseMalformedBinaryOperator() throws Exception {
        assertException(
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then c
                                else +125
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(20)" +
                        ")",
                103,
                "too few arguments for '+' [found=1,expected=2]"
        );
    }

    @Test
    public void testIntOrElseUnaryNeg() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t315515118
                        701\t-125
                        706\t-125
                        -714\t-1575378703
                        116\t339631474
                        67\t-125
                        207\t-125
                        -55\t-1792928964
                        -104\t-1153445279
                        -127\t1631244228
                        790\t-125
                        881\t-125
                        -535\t-938514914
                        -973\t-342047842
                        -463\t-27395319
                        -667\t2137969456
                        578\t-125
                        940\t-125
                        -54\t-1162267908
                        -393\t-296610933
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then c
                                else -125
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " rnd_int() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntToStringCast() throws Exception {
        execute(
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_str() a," +
                        " rnd_str() b," +
                        " rnd_str() c" +
                        " from long_sequence(20)" +
                        ")"
        );
        assertException(
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else 5\
                            end\s
                        from tanc""",
                103,
                "inconvertible types: INT -> STRING [from=INT, to=STRING]"
        );
    }

    @Test
    public void testIntToStringCastOnBranch() throws Exception {
        execute(
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_str() a," +
                        " rnd_str() b," +
                        " rnd_str() c" +
                        " from long_sequence(20)" +
                        ")"
        );
        assertException(
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 500 then 10
                            end\s
                        from tanc""",
                88,
                "inconvertible types: INT -> STRING [from=INT, to=STRING]"
        );
    }

    @Test
    public void testIntToVarcharCast() throws Exception {
        execute(
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_varchar() a," +
                        " rnd_varchar() b," +
                        " rnd_varchar() c" +
                        " from long_sequence(5)" +
                        ")"
        );

        assertException(
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else 3\
                            end\s
                        from tanc""",
                103,
                "inconvertible types: INT -> VARCHAR [from=INT, to=VARCHAR]"
        );
    }

    @Test
    public void testIntToVarcharCastOnBranch() throws Exception {
        execute(
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_varchar() a," +
                        " rnd_varchar() b," +
                        " rnd_varchar() c" +
                        " from long_sequence(20)" +
                        ")"
        );

        assertException(
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 500 then 10
                            end\s
                        from tanc""",
                88,
                "inconvertible types: INT -> VARCHAR [from=INT, to=VARCHAR]"
        );
    }

    @Test
    public void testKeyedFunctionVarArgumentNumeric() throws Exception {
        assertMemoryLeak(() -> {
            String[] types = {"INT", "LONG", "SHORT", "STRING", "TIMESTAMP", "BOOLEAN", "TIMESTAMP_NS"};

            for (String type : types) {
                execute("create table tt as (" +
                        "select cast(x as TIMESTAMP_NS) as ts, cast(x as " + type + ") as x from long_sequence(10)" +
                        ") timestamp(ts)");

                // this is a bit confusing. for booleans, every value x != 0 will evaluate to 1
                // however, for int etc, only the value 1 will evaluate to 1
                assertSql("sum\n" +
                        (type.equals("BOOLEAN") ? "10\n" : "1\n"), "select sum(case x when CAST(1 as " + type + ") then 1 else 0 end) " +
                        "from tt"
                );

                execute("drop table tt");
            }
        });
    }

    @Test
    public void testLong() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t4729996258992366
                        701\tnull
                        706\tnull
                        -714\t-7489826605295361807
                        116\t3394168647660478011
                        67\tnull
                        207\tnull
                        -55\t5539350449504785212
                        -104\t-4100339045953973663
                        -127\t2811900023577169860
                        790\tnull
                        881\tnull
                        -535\t7199909180655756830
                        -973\t6404066507400987550
                        -463\t8573481508564499209
                        -667\t-8480005421611953360
                        578\tnull
                        940\tnull
                        -54\t3152466304308949756
                        -393\t6179044593759294347
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_long() a," +
                        " rnd_long() b," +
                        " rnd_long() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testLong256() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t0x72a215ba0462ad159f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee
                        -703\t0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059
                        -50\t0x38b73d329210d2774cdfb9e29522133c87aa0968faec6879a0d8cea7196b33a0
                        -348\t0x8a538661f350d0b46f06560981acb5496adc00ebd29fdd5373dee145497c5436
                        -973\t0xacea66fbe47c5e39bccb30ed7795ebc85f20a35e80e154f458dfd08eeb9cc39e
                        2\t
                        841\t
                        380\t
                        401\t
                        -819\t0xd364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf637b4f6e41fbfd55f
                        -330\t0xaa7dc4eccb68146fb37f1ec82752c7d784646fead466b67f39d5534da00d272c
                        -446\t0xc6dfacdd3f3c52b88b4e4831499fc2a526567f4430b46b7f78c594c496995885
                        782\t
                        -67\t0x61a4be9e1b8dcc3c84572da78228e0f2af44c40a67ef5e1c5b3ef21223ee8849
                        613\t
                        988\t
                        -478\t0x7f24de22c77acf93e983e65f5551d0738678dc0e1718f0c950d5a76fa806bdc3
                        -499\t0x4fc01e2b9fd116236359c71782852d0489661af328d0e234d7eb56647bc4ff57
                        259\t
                        -532\t0x8d5c4bed8432de9862a2f11e8510a3e99cb8fc6467028eb0a07934b2a15de8e0
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_long256() a," +
                        " rnd_long256() b," +
                        " rnd_long256() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testLong256OrElse() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t0x72a215ba0462ad159f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee
                        -703\t0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059
                        -50\t0x38b73d329210d2774cdfb9e29522133c87aa0968faec6879a0d8cea7196b33a0
                        -348\t0x8a538661f350d0b46f06560981acb5496adc00ebd29fdd5373dee145497c5436
                        -973\t0xacea66fbe47c5e39bccb30ed7795ebc85f20a35e80e154f458dfd08eeb9cc39e
                        2\t0x55c06051ee52138b655f87a3a21d575f610f69efe063fe79336dc434790ed331
                        841\t0x5277ee62a5a6e9fb9ff97d73fc0c62d069440048957ae05360802a2ca499f211
                        380\t0xc736a8b67656c4f159d574d2ff5fb1e3687a84abb7bfac3ebedf29efb28cdcb1
                        401\t0x6cecb916a1ad092b997918f622d62989c009aea26fdde482ba37e200ad5b17cd
                        -819\t0xd364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf637b4f6e41fbfd55f
                        -330\t0xaa7dc4eccb68146fb37f1ec82752c7d784646fead466b67f39d5534da00d272c
                        -446\t0xc6dfacdd3f3c52b88b4e4831499fc2a526567f4430b46b7f78c594c496995885
                        782\t0xd25adf928386cdd2d992946a26184664ba453d761efcf9bb7ee6a03f4f930fa3
                        -67\t0x61a4be9e1b8dcc3c84572da78228e0f2af44c40a67ef5e1c5b3ef21223ee8849
                        613\t0xbb56ab77cffe0a894aed11c72256a80c7b5dd2b8513b31e7b20e1900caff819a
                        988\t0x867f8923b4422debb63b32ce71b869c64068fde7370b826954c6fb68b1d6235e
                        -478\t0x7f24de22c77acf93e983e65f5551d0738678dc0e1718f0c950d5a76fa806bdc3
                        -499\t0x4fc01e2b9fd116236359c71782852d0489661af328d0e234d7eb56647bc4ff57
                        259\t0x8692bc8c04e4bb71d24b84c08ea7606a70061ac6a4115ca72121bcf90e438244
                        -532\t0x8d5c4bed8432de9862a2f11e8510a3e99cb8fc6467028eb0a07934b2a15de8e0
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else c
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_long256() a," +
                        " rnd_long256() b," +
                        " rnd_long256() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testLongOrElse() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t4729996258992366
                        701\t-5354193255228091881
                        706\t3614738589890112276
                        -714\t-7489826605295361807
                        116\t3394168647660478011
                        67\t8336855953317473051
                        207\t-6856503215590263904
                        -55\t5539350449504785212
                        -104\t-4100339045953973663
                        -127\t2811900023577169860
                        790\t-8479285918156402508
                        881\t8942747579519338504
                        -535\t7199909180655756830
                        -973\t6404066507400987550
                        -463\t8573481508564499209
                        -667\t-8480005421611953360
                        578\t-6186964045554120476
                        940\t-6253307669002054137
                        -54\t3152466304308949756
                        -393\t6179044593759294347
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else c
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_long() a," +
                        " rnd_long() b," +
                        " rnd_long() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testLongToVarcharCast() throws Exception {
        execute(
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_varchar() a," +
                        " rnd_varchar() b," +
                        " rnd_varchar() c" +
                        " from long_sequence(5)" +
                        ")"
        );
        assertException(
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else 3::long\
                            end\s
                        from tanc""",
                104,
                "inconvertible types: LONG -> VARCHAR [from=LONG, to=VARCHAR]"
        );
    }

    @Test
    public void testNoArgs() throws Exception {
        assertException(
                "select " +
                        "    x " +
                        "    case end c " +
                        "from long_sequence(1);",
                17,
                "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"case\""
        );
    }

    @Test
    public void testNonBooleanWhen() throws Exception {
        assertException(
                """
                        select\s
                            x,
                            case
                                when x then a
                                when x > 100 and x < 200 then b
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_date() a," +
                        " rnd_date() b," +
                        " rnd_date() c" +
                        " from long_sequence(20)" +
                        ")",
                37,
                "BOOLEAN expected, found INT"
        );
    }

    @Test
    public void testShort() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t24814
                        701\t0
                        706\t0
                        -714\t-24335
                        116\t7739
                        67\t0
                        207\t0
                        -55\t4924
                        -104\t-11679
                        -127\t-12348
                        790\t0
                        881\t0
                        -535\t26142
                        -973\t-15458
                        -463\t-1271
                        -667\t-11472
                        578\t0
                        940\t0
                        -54\t13052
                        -393\t5003
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_short() a," +
                        " rnd_short() b," +
                        " rnd_short() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testShortOrElse() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t24814
                        701\t21015
                        706\t-5356
                        -714\t-24335
                        116\t7739
                        67\t-21733
                        207\t13216
                        -55\t4924
                        -104\t-11679
                        -127\t-12348
                        790\t-12108
                        881\t2056
                        -535\t26142
                        -973\t-15458
                        -463\t-1271
                        -667\t-11472
                        578\t2276
                        940\t5639
                        -54\t13052
                        -393\t5003
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else c
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_short() a," +
                        " rnd_short() b," +
                        " rnd_short() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testShortToVarcharCast() throws Exception {
        execute(
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_varchar() a," +
                        " rnd_varchar() b," +
                        " rnd_varchar() c" +
                        " from long_sequence(5)" +
                        ")"
        );
        assertException(
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else '42'::short\
                            end\s
                        from tanc""",
                107,
                "inconvertible types: SHORT -> VARCHAR [from=SHORT, to=VARCHAR]"
        );
    }

    @Test
    public void testSingleCharSymbol() throws Exception {
        assertQuery(
                """
                        category\tres
                        V\tfalse
                        T\tfalse
                        J\tfalse
                        W\ttrue
                        C\tfalse
                        P\tfalse
                        S\tfalse
                        W\ttrue
                        H\tfalse
                        Y\tfalse
                        """,
                """
                        SELECT category,\s
                          CASE
                            WHEN category = 'W' THEN true
                            ELSE false
                          END AS res
                        FROM tab""",
                "create table tab as (" +
                        "select rnd_char()::symbol as category" +
                        " from long_sequence(10)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testStr() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\tWCPS
                        474\t
                        454\t
                        -666\tULOFJGE
                        -574\tYICCXZOUIC
                        -303\tYCTGQO
                        355\t
                        692\t
                        -743\tLJU
                        36\t
                        0\t
                        799\t
                        650\t
                        -760\tGXHFVWSWSR
                        -605\tUKL
                        -554\tNPH
                        -201\tTNLE
                        623\t
                        -341\tXBHYSBQYMI
                        386\t
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_str() a," +
                        " rnd_str() b," +
                        " rnd_str() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testStrOrElse() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\tWCPS
                        474\tYQEHBH
                        454\tUED
                        -666\tULOFJGE
                        -574\tYICCXZOUIC
                        -303\tYCTGQO
                        355\tSMSSUQ
                        692\tIHVL
                        -743\tLJU
                        36\tRGIIHYH
                        0\tIFOUSZM
                        799\tWNWIFFLR
                        650\tFKWZ
                        -760\tGXHFVWSWSR
                        -605\tUKL
                        -554\tNPH
                        -201\tTNLE
                        623\tZSLQVFGPP
                        -341\tXBHYSBQYMI
                        386\tDVRVNGS
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else c
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_str() a," +
                        " rnd_str() b," +
                        " rnd_str() c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testStringToVarcharCast() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t&BT+
                        363\tfoo
                        367\tfoo
                        895\tfoo
                        -6\t1W씌䒙\uD8F2\uDE8E>
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else 'foo'::string\
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_varchar() a," +
                        " rnd_varchar() b," +
                        " rnd_varchar() c" +
                        " from long_sequence(5)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testTimestamp() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t1970-01-01T00:00:00.000000000Z
                        118\t1970-01-01T00:00:00.000103000Z
                        833\t
                        -771\t1970-01-01T00:00:00.000030000Z
                        701\t
                        -339\t1970-01-01T00:00:00.000050000Z
                        242\t
                        671\t
                        706\t
                        -48\t1970-01-01T00:00:00.000090000Z
                        -516\t1970-01-01T00:00:00.000100000Z
                        -972\t1970-01-01T00:00:00.000110000Z
                        -714\t1970-01-01T00:00:00.000120000Z
                        -703\t1970-01-01T00:00:00.000130000Z
                        481\t
                        512\t
                        116\t1970-01-01T00:00:00.001603000Z
                        97\t
                        -405\t1970-01-01T00:00:00.000180000Z
                        474\t
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " timestamp_sequence(0, 10) a," +
                        " timestamp_sequence_ns(3000, 100000) b," +
                        " timestamp_sequence(6, 100) c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testTimestampOrElse() throws Exception {
        assertQuery(
                """
                        x\tcase
                        -920\t1970-01-01T00:00:00.000000000Z
                        118\t1970-01-01T00:00:00.000103000Z
                        833\t1970-01-01T00:00:00.000206000Z
                        -771\t1970-01-01T00:00:00.000030000Z
                        701\t1970-01-01T00:00:00.000406000Z
                        -339\t1970-01-01T00:00:00.000050000Z
                        242\t1970-01-01T00:00:00.000606000Z
                        671\t1970-01-01T00:00:00.000706000Z
                        706\t1970-01-01T00:00:00.000806000Z
                        -48\t1970-01-01T00:00:00.000090000Z
                        -516\t1970-01-01T00:00:00.000100000Z
                        -972\t1970-01-01T00:00:00.000110000Z
                        -714\t1970-01-01T00:00:00.000120000Z
                        -703\t1970-01-01T00:00:00.000130000Z
                        481\t1970-01-01T00:00:00.001406000Z
                        512\t1970-01-01T00:00:00.001506000Z
                        116\t1970-01-01T00:00:00.001603000Z
                        97\t1970-01-01T00:00:00.001706000Z
                        -405\t1970-01-01T00:00:00.000180000Z
                        474\t1970-01-01T00:00:00.001906000Z
                        """,
                """
                        select\s
                            x,
                            case
                                when x < 0 then a
                                when x > 100 and x < 200 then b
                                else c
                            end\s
                        from tanc""",
                "create table tanc as (" +
                        "select rnd_int() % 1000 x," +
                        " timestamp_sequence_ns(0, 10000) a," +
                        " timestamp_sequence(3, 100) b," +
                        " timestamp_sequence_ns(6000, 100000) c" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testUuidToVarcharCast() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table tanc as (" +
                            "select rnd_int() % 1000 x," +
                            " rnd_varchar() a," +
                            " rnd_varchar() b," +
                            " rnd_varchar() c" +
                            " from long_sequence(5)" +
                            ")"
            );

            assertSql(
                    """
                            x\tcase
                            -920\t
                            363\ta0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                            367\ta0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                            895\ta0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                            -6\t
                            """,
                    """
                            select\s
                                x,
                                case
                                    when x < 0 then a
                                    when x > 100 and x < 200 then b
                                    else 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid\
                                end\s
                            from tanc"""
            );
        });
    }

    @Test
    public void testVarcharCast() throws Exception {
        assertQuery(
                """
                        x\tswitch
                        -920\t
                        706\t
                        -104\t
                        940\t
                        841\t
                        """,
                """
                        select\s
                            x,
                            case
                                when x = 97 then a
                                else ''\
                            end\s
                        from x""",
                "create table x as (" +
                        "select rnd_int() % 1000 x," +
                        " rnd_varchar() a" +
                        " from long_sequence(5)" +
                        ")",
                null,
                true,
                true
        );
    }
}
