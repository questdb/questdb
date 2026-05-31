/*+*****************************************************************************
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

package io.questdb.test.griffin.engine.functions.conditional;

import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CoalesceFunctionFactoryTest extends AbstractCairoTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testCoalesceDecimal128() throws Exception {
        assertQuery("select coalesce(null::decimal(24, 1), 12.34m) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n12.34\n");
        assertQuery("select coalesce(null::decimal(25, 0), null::decimal(18, 1), null::decimal(6, 0)) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n\n");
        assertQuery("select coalesce(12345678.901m, null::decimal(23, 0)) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n12345678.901\n");
        assertQuery("select coalesce(1.2m, 3m, null::decimal(22, 0)) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n1.2\n");
        assertQuery("select coalesce(null::decimal(21, 1), 1.2m, 3.1m) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n1.2\n");
    }

    @Test
    public void testCoalesceDecimal16() throws Exception {
        assertQuery("select coalesce(null::decimal(2, 1), 12.34m) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n12.34\n");
        assertQuery("select coalesce(null::decimal(3, 0), null::decimal(2, 1), null::decimal(1, 0)) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n\n");
        assertQuery("select coalesce(12.34m, null::decimal(1, 0)) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n12.34\n");
        assertQuery("select coalesce(1.2m, 3m, null::decimal(3, 0)) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n1.2\n");
        assertQuery("select coalesce(null::decimal(3, 1), 1.2m, 3.1m) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n1.2\n");
    }

    @Test
    public void testCoalesceDecimal256() throws Exception {
        assertQuery("select coalesce(null::decimal(73, 1), 12.34m) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n12.34\n");
        assertQuery("select coalesce(null::decimal(70, 0), null::decimal(65, 1), null::decimal(6, 0)) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n\n");
        assertQuery("select coalesce(12345678.901m, null::decimal(60, 0)) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n12345678.901\n");
        assertQuery("select coalesce(1.2m, 3m, null::decimal(55, 0)) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n1.2\n");
        assertQuery("select coalesce(null::decimal(50, 1), 1.2m, 3.1m) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n1.2\n");
    }

    @Test
    public void testCoalesceDecimal32() throws Exception {
        assertQuery("select coalesce(null::decimal(6, 1), 12.34m) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n12.34\n");
        assertQuery("select coalesce(null::decimal(6, 0), null::decimal(2, 1), null::decimal(1, 0)) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n\n");
        assertQuery("select coalesce(1234.5678m, null::decimal(1, 0)) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n1234.5678\n");
        assertQuery("select coalesce(1.2m, 3m, null::decimal(5, 0)) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n1.2\n");
        assertQuery("select coalesce(null::decimal(5, 1), 1.2m, 3.1m) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n1.2\n");
    }

    @Test
    public void testCoalesceDecimal64() throws Exception {
        assertQuery("select coalesce(null::decimal(9, 1), 12.34m) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n12.34\n");
        assertQuery("select coalesce(null::decimal(12, 0), null::decimal(8, 1), null::decimal(6, 0)) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n\n");
        assertQuery("select coalesce(12345678.901m, null::decimal(1, 0)) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n12345678.901\n");
        assertQuery("select coalesce(1.2m, 3m, null::decimal(12, 0)) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n1.2\n");
        assertQuery("select coalesce(null::decimal(10, 1), 1.2m, 3.1m) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n1.2\n");
    }

    @Test
    public void testCoalesceDecimal8() throws Exception {
        assertQuery("select coalesce(null::decimal(2, 1), 1.2m) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n1.2\n");
        assertQuery("select coalesce(null::decimal(2, 0), null::decimal(1, 0), null::decimal(1, 0)) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n\n");
        assertQuery("select coalesce(1.2m, null::decimal(1, 0)) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n1.2\n");
        assertQuery("select coalesce(1.2m, 3m, null::decimal(1, 0)) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n1.2\n");
        assertQuery("select coalesce(null::decimal(2, 1), 1.2m, 3.1m) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n1.2\n");
    }

    @Test
    public void testCoalesceDecimalImplicitCasting() throws Exception {
        // Implicit cast byte
        assertQuery("select coalesce(1::byte, 1.2m) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n1.0\n");
        // Implicit cast short
        assertQuery("select coalesce(1::short, 1.2m) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n1.0\n");
        // Implicit cast int
        assertQuery("select coalesce(1::int, 1.2m) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n1.0\n");
        // Implicit cast long
        assertQuery("select coalesce(1::long, 1.2m) c")
                .noLeakCheck()
                .expectSize()
                .returns("c\n1.0\n");
    }

    @Test
    public void testCoalesceDecimalInvalidType() throws Exception {
        assertException("select coalesce(1.2m, 'abc') c", 22, "inconvertible types");
    }

    @Test
    public void testCoalesceIPv4InvalidStringLiteral() throws Exception {
        assertException(
                "select coalesce('192.168.1.1'::ipv4, 'foobar')",
                37,
                "invalid IPv4 constant"
        );
    }

    @Test
    public void testCoalesceIPv4InvalidVarcharLiteral() throws Exception {
        assertException(
                "select coalesce('192.168.1.1'::ipv4, 'foobar'::varchar)",
                45,
                "invalid IPv4 constant"
        );
    }

    @Test
    public void testCoalesceIPv4StringLiteral() throws Exception {
        assertQuery("select coalesce(x, '127.0.0.1') as c1, coalesce('127.0.0.1', x) as c2, x \n" +
                        "from t")
                .ddl("create table t as (" +
                        "select CASE WHEN x % 2 = 0 THEN rnd_ipv4('1.1.1.1/16', 2) ELSE NULL END as x " +
                        " from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        c1\tc2\tx
                        127.0.0.1\t127.0.0.1\t
                        1.1.96.238\t127.0.0.1\t1.1.96.238
                        127.0.0.1\t127.0.0.1\t
                        127.0.0.1\t127.0.0.1\t
                        127.0.0.1\t127.0.0.1\t
                        """);
    }

    @Test
    public void testCoalesceIPv4VarcharLiteral() throws Exception {
        assertQuery("select coalesce(x, '127.0.0.1'::varchar) as c1, coalesce('127.0.0.1'::varchar, x) as c2, x \n" +
                        "from t")
                .ddl("create table t as (" +
                        "select CASE WHEN x % 2 = 0 THEN rnd_ipv4('1.1.1.1/16', 2) ELSE NULL END as x " +
                        " from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        c1\tc2\tx
                        127.0.0.1\t127.0.0.1\t
                        1.1.96.238\t127.0.0.1\t1.1.96.238
                        127.0.0.1\t127.0.0.1\t
                        127.0.0.1\t127.0.0.1\t
                        127.0.0.1\t127.0.0.1\t
                        """);
    }

    @Test
    public void testCoalesceLong256() throws Exception {
        assertQuery("select coalesce(a, b, x) as c1, coalesce(a, b) c2, a, b, x \n" +
                        "from alex")
                .ddl("create table alex as (" +
                        "select CASE WHEN x % 2 = 0 THEN rnd_long256(1000) ELSE CAST(NULL as LONG256) END as x," +
                        " CASE WHEN x % 4 = 0 THEN rnd_long256(10) ELSE CAST(NULL as LONG256) END as a," +
                        " CASE WHEN x % 4 = 1 THEN rnd_long256(30) ELSE CAST(NULL as LONG256) END as b" +
                        " from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        c1\tc2\ta\tb\tx
                        0x1408db63570045c42b9133f96d5ac9b49395464592076d7cc536ccc3235f0a72\t0x1408db63570045c42b9133f96d5ac9b49395464592076d7cc536ccc3235f0a72\t\t0x1408db63570045c42b9133f96d5ac9b49395464592076d7cc536ccc3235f0a72\t
                        0xa4cdac45d508383f9c0a1370d099b7237e25b91255572a8f86fd0ebdb6707e47\t\t\t\t0xa4cdac45d508383f9c0a1370d099b7237e25b91255572a8f86fd0ebdb6707e47
                        \t\t\t\t
                        0x2000c672c7af13b68f38b4e22684beea970e01b3e4aca8b29e144cd789d939f0\t0x2000c672c7af13b68f38b4e22684beea970e01b3e4aca8b29e144cd789d939f0\t0x2000c672c7af13b68f38b4e22684beea970e01b3e4aca8b29e144cd789d939f0\t\t0x9ec31d67e4bc804a761b47dbe5d724a075234fffc7e1e6917d2037c10d3c9d2e
                        0x2ab49a7d2ed9aec81233ce62b3c6cf03600e7d2ef68eeb777b8273a492471abc\t0x2ab49a7d2ed9aec81233ce62b3c6cf03600e7d2ef68eeb777b8273a492471abc\t\t0x2ab49a7d2ed9aec81233ce62b3c6cf03600e7d2ef68eeb777b8273a492471abc\t
                        """);
    }

    @Test
    public void testCoalesceUuid() throws Exception {
        assertQuery("select coalesce(a, b, x) as c1, coalesce(a, b) c2, a, b, x \n" +
                        "from t")
                .ddl("create table t as (" +
                        "select CASE WHEN x % 2 = 0 THEN rnd_uuid4() ELSE NULL END as x," +
                        " CASE WHEN x % 4 = 0 THEN rnd_uuid4() ELSE NULL END as a," +
                        " CASE WHEN x % 4 = 1 THEN rnd_uuid4() ELSE NULL END as b" +
                        " from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        c1\tc2\ta\tb\tx
                        0010cde8-12ce-40ee-8010-a928bb8b9650\t0010cde8-12ce-40ee-8010-a928bb8b9650\t\t0010cde8-12ce-40ee-8010-a928bb8b9650\t
                        9f9b2131-d49f-4d1d-ab81-39815c50d341\t\t\t\t9f9b2131-d49f-4d1d-ab81-39815c50d341
                        \t\t\t\t
                        b5b2159a-2356-4217-965d-4c984f0ffa8a\tb5b2159a-2356-4217-965d-4c984f0ffa8a\tb5b2159a-2356-4217-965d-4c984f0ffa8a\t\t7bcd48d8-c77a-4655-b2a2-15ba0462ad15
                        e8beef38-cd7b-43d8-9b2d-34586f6275fa\te8beef38-cd7b-43d8-9b2d-34586f6275fa\t\te8beef38-cd7b-43d8-9b2d-34586f6275fa\t
                        """);
    }

    @Test
    public void testCoalesceUuidInvalidStringLiteral() throws Exception {
        assertException(
                "select coalesce('00000000-0000-0000-0000-000000000000'::uuid, 'foobar')",
                62,
                "invalid UUID constant"
        );
    }

    @Test
    public void testCoalesceUuidInvalidVarcharLiteral() throws Exception {
        assertException(
                "select coalesce('00000000-0000-0000-0000-000000000000'::uuid, 'foobar'::varchar)",
                70,
                "invalid UUID constant"
        );
    }

    @Test
    public void testCoalesceUuidNull() throws Exception {
        assertQuery("select coalesce(x, null) as c1, coalesce(null, x) c2, x \n" +
                        "from t")
                .ddl("create table t as (" +
                        "select CASE WHEN x % 2 = 0 THEN rnd_uuid4() ELSE NULL END as x " +
                        " from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        c1\tc2\tx
                        \t\t
                        0010cde8-12ce-40ee-8010-a928bb8b9650\t0010cde8-12ce-40ee-8010-a928bb8b9650\t0010cde8-12ce-40ee-8010-a928bb8b9650
                        \t\t
                        9f9b2131-d49f-4d1d-ab81-39815c50d341\t9f9b2131-d49f-4d1d-ab81-39815c50d341\t9f9b2131-d49f-4d1d-ab81-39815c50d341
                        \t\t
                        """);
    }

    @Test
    public void testCoalesceUuidStringLiteral() throws Exception {
        assertQuery("select coalesce(x, '00000000-0000-0000-0000-000000000000') as c1, coalesce('00000000-0000-0000-0000-000000000000', x) as c2, x \n" +
                        "from t")
                .ddl("create table t as (" +
                        "select CASE WHEN x % 2 = 0 THEN rnd_uuid4() ELSE NULL END as x " +
                        " from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        c1\tc2\tx
                        00000000-0000-0000-0000-000000000000\t00000000-0000-0000-0000-000000000000\t
                        0010cde8-12ce-40ee-8010-a928bb8b9650\t00000000-0000-0000-0000-000000000000\t0010cde8-12ce-40ee-8010-a928bb8b9650
                        00000000-0000-0000-0000-000000000000\t00000000-0000-0000-0000-000000000000\t
                        9f9b2131-d49f-4d1d-ab81-39815c50d341\t00000000-0000-0000-0000-000000000000\t9f9b2131-d49f-4d1d-ab81-39815c50d341
                        00000000-0000-0000-0000-000000000000\t00000000-0000-0000-0000-000000000000\t
                        """);
    }

    @Test
    public void testCoalesceUuidVarcharLiteral() throws Exception {
        assertQuery("select coalesce(x, '00000000-0000-0000-0000-000000000000'::varchar) as c1, coalesce('00000000-0000-0000-0000-000000000000'::varchar, x) as c2, x \n" +
                        "from t")
                .ddl("create table t as (" +
                        "select CASE WHEN x % 2 = 0 THEN rnd_uuid4() ELSE NULL END as x " +
                        " from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        c1\tc2\tx
                        00000000-0000-0000-0000-000000000000\t00000000-0000-0000-0000-000000000000\t
                        0010cde8-12ce-40ee-8010-a928bb8b9650\t00000000-0000-0000-0000-000000000000\t0010cde8-12ce-40ee-8010-a928bb8b9650
                        00000000-0000-0000-0000-000000000000\t00000000-0000-0000-0000-000000000000\t
                        9f9b2131-d49f-4d1d-ab81-39815c50d341\t00000000-0000-0000-0000-000000000000\t9f9b2131-d49f-4d1d-ab81-39815c50d341
                        00000000-0000-0000-0000-000000000000\t00000000-0000-0000-0000-000000000000\t
                        """);
    }

    @Test
    public void testCoalesceVarchar() throws Exception {
        assertQuery("select coalesce(a, b, x) as c1, coalesce(a, b) c2, a, b, x \n" +
                        "from t")
                .ddl("create table t as (" +
                        "select CASE WHEN x % 2 = 0 THEN rnd_varchar() ELSE CAST(NULL as VARCHAR) END as x," +
                        " CASE WHEN x % 4 = 0 THEN rnd_varchar() ELSE CAST(NULL as VARCHAR) END as a," +
                        " CASE WHEN x % 4 = 1 THEN rnd_varchar() ELSE CAST(NULL as VARCHAR) END as b" +
                        " from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        c1\tc2\ta\tb\tx
                        &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t\t&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t
                        8#3TsZ\t\t\t\t8#3TsZ
                        \t\t\t\t
                        ṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\tṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\tṟ\u1AD3ڎBH뤻䰭\u008B}ѱ\t\tzV衞͛Ԉ龘и\uDA89\uDFA4~
                        \uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46OF\t\uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46OF\t\t\uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46OF\t
                        """);
    }

    @Test
    public void testDateCoalesce() throws Exception {
        assertQuery("select coalesce(a, b, x) as c1, coalesce(a, b) c2, a, x \n" +
                        "from alex")
                .ddl("""
                        create table alex as (\
                        WITH tx as (
                        select CAST(dateadd('d', CAST(x as INT), CAST(0 AS DATE)) AS DATE) as x,\s
                        CAST(dateadd('d', CAST(x as INT) * 10, CAST(0 AS DATE)) AS DATE) as xx,\s
                        CAST(x AS DATE) xxx,\
                        x as n from long_sequence(5))
                        select \
                        CASE WHEN n > 3 THEN xxx ELSE CAST(NULL as DATE) END as x,\s
                        CASE WHEN n % 3 = 0 THEN xx ELSE CAST(NULL as DATE) END as a,\s
                        CASE WHEN n % 3 = 1 THEN xx ELSE CAST(NULL as DATE) END as b\s
                        from tx \
                        )""")
                .expectSize()
                .returns("""
                        c1\tc2\ta\tx
                        1970-01-11T00:00:00.000Z\t1970-01-11T00:00:00.000Z\t\t
                        \t\t\t
                        1970-01-31T00:00:00.000Z\t1970-01-31T00:00:00.000Z\t1970-01-31T00:00:00.000Z\t
                        1970-02-10T00:00:00.000Z\t1970-02-10T00:00:00.000Z\t\t1970-01-01T00:00:00.004Z
                        1970-01-01T00:00:00.005Z\t\t\t1970-01-01T00:00:00.005Z
                        """);
    }

    @Test
    public void testDouble3Args() throws Exception {
        assertQuery("select coalesce(b, a, x) " +
                        "from alex")
                .ddl("create table alex as (" +
                        "select CASE WHEN x % 3 = 0 THEN x / 10.0 ELSE CAST(NULL as double) END as x," +
                        " CASE WHEN x % 3 = 0 THEN 0.5 ELSE CAST(NULL as double) END as a," +
                        " CASE WHEN x % 3 = 1 THEN 10.0 ELSE CAST(NULL as double) END as b" +
                        " from long_sequence(6)" +
                        ")")
                .expectSize()
                .returns("""
                        coalesce
                        10.0
                        null
                        0.5
                        10.0
                        null
                        0.5
                        """);
    }

    @Test
    public void testDoubleAndLongMixed3Args() throws Exception {
        assertQuery("select coalesce(b, a, x) c1, coalesce(b, a) c2, a, b, x\n" +
                        "from alex")
                .ddl("create table alex as (" +
                        "select x / 10.0 as x," +
                        " CASE WHEN x % 3 = 0 THEN 100L ELSE CAST(NULL as long) END as a," +
                        " CASE WHEN x % 3 = 1 THEN 10.0 ELSE CAST(NULL as double) END as b" +
                        " from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        c1\tc2\ta\tb\tx
                        10.0\t10.0\tnull\t10.0\t0.1
                        0.2\tnull\tnull\tnull\t0.2
                        100.0\t100.0\t100\tnull\t0.3
                        10.0\t10.0\tnull\t10.0\t0.4
                        0.5\tnull\tnull\tnull\t0.5
                        """);
    }

    @Test
    public void testFailsWithSingleArg() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table alex as (" +
                            "select CASE WHEN x % 2 = 0 THEN CAST(NULL as long) ELSE x END as x," +
                            " CASE WHEN x % 3 = 0 THEN x * 2 ELSE CAST(NULL as long) END as a," +
                            " CASE WHEN x % 3 = 1 THEN x * 3 ELSE CAST(NULL as long) END as b" +
                            " from long_sequence(6)" +
                            ")"
            );

            assertExceptionNoLeakCheck(
                    "select coalesce(b) from alex",
                    7,
                    "coalesce can be used with 2 or more arguments"
            );
        });
    }

    @Test
    public void testFailsWithUnsupportedType() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table alex as (" +
                    "select CAST(NULL as binary) x, CAST(NULL as binary) a" +
                    " from long_sequence(6)" +
                    ")");

            try {
                assertExceptionNoLeakCheck("select coalesce(x, a)\n" +
                        "from alex");
            } catch (SqlException ex) {
                Assert.assertTrue(ex.getMessage().contains("coalesce"));
            }
        });
    }

    @Test
    public void testFloat3Args() throws Exception {
        assertQuery("select coalesce(b, a, x) c1, coalesce(b, a) c2, a, b, x\n" +
                        "from alex")
                .ddl("create table alex as (" +
                        "select " +
                        " CASE WHEN x > 3 THEN CAST(x as float) ELSE CAST(NULL as float) END as x," +
                        " CASE WHEN x % 3 = 0 THEN 0.5f ELSE CAST(NULL as float) END as a," +
                        " CASE WHEN x % 3 = 1 THEN 10.0f ELSE CAST(NULL as float) END as b" +
                        " from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        c1\tc2\ta\tb\tx
                        10.0\t10.0\tnull\t10.0\tnull
                        null\tnull\tnull\tnull\tnull
                        0.5\t0.5\t0.5\tnull\tnull
                        10.0\t10.0\tnull\t10.0\t4.0
                        5.0\tnull\tnull\tnull\t5.0
                        """);
    }

    @Test
    public void testIPv4Args() throws Exception {
        assertQuery("select coalesce(a, b, x) c1, coalesce(a, b) c2, a, b, x\n" +
                        "from test")
                .ddl("create table test as (" +
                        "select " +
                        " rnd_ipv4('1.1.1.1/16', 2) x," +
                        " rnd_ipv4('3.7.4.2/24', 2) a," +
                        " rnd_ipv4('54.23.11.87/8', 2) b\n" +
                        " from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        c1\tc2\ta\tb\tx
                        54.98.173.21\t54.98.173.21\t\t54.98.173.21\t1.1.96.238
                        54.132.76.40\t54.132.76.40\t\t54.132.76.40\t1.1.250.138
                        3.7.4.15\t3.7.4.15\t3.7.4.15\t\t1.1.20.236
                        54.62.93.114\t54.62.93.114\t\t54.62.93.114\t1.1.132.196
                        3.7.4.252\t3.7.4.252\t3.7.4.252\t54.22.249.199\t
                        """);
    }

    @Test
    public void testIntArgs() throws Exception {
        assertQuery("select coalesce(a, b, x) c1, coalesce(a, b) c2, a, b, x\n" +
                        "from alex")
                .ddl("create table alex as (" +
                        "select " +
                        " CASE WHEN x > 3 THEN CAST(x as INT)ELSE CAST(NULL as INT) END x," +
                        " CASE WHEN x % 3 = 0 THEN CAST(x AS INT) * 2 ELSE CAST(NULL as INT) END as a," +
                        " CASE WHEN x % 3 = 1 THEN CAST(x AS INT) * 10 ELSE CAST(NULL as INT) END as b\n" +
                        " from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        c1\tc2\ta\tb\tx
                        10\t10\tnull\t10\tnull
                        null\tnull\tnull\tnull\tnull
                        6\t6\t6\tnull\tnull
                        40\t40\tnull\t40\t4
                        5\tnull\tnull\tnull\t5
                        """);
    }

    @Test
    public void testLong2Args() throws Exception {
        assertQuery("select coalesce(b, a, x) c1, coalesce(b, a) c2, a, b, x\n" +
                        "from alex")
                .ddl("create table alex as (" +
                        "select CASE WHEN x % 2 = 0 THEN CAST(NULL as long) ELSE x END as x," +
                        " CASE WHEN x % 3 = 0 THEN x * 2 ELSE CAST(NULL as long) END as a," +
                        " CASE WHEN x % 3 = 1 THEN x * 3 ELSE CAST(NULL as long) END as b" +
                        " from long_sequence(6)" +
                        ")")
                .expectSize()
                .returns("""
                        c1\tc2\ta\tb\tx
                        3\t3\tnull\t3\t1
                        null\tnull\tnull\tnull\tnull
                        6\t6\t6\tnull\t3
                        12\t12\tnull\t12\tnull
                        5\tnull\tnull\tnull\t5
                        12\t12\t12\tnull\tnull
                        """);
    }

    @Test
    public void testLong2ArgsWithNulls() throws Exception {
        assertQuery("select coalesce(b, a, x) c1, coalesce(b, a) c2, a, b, x\n" +
                        "from alex")
                .ddl("create table alex as (" +
                        "select CASE WHEN x % 2 = 0 THEN NULL ELSE x END as x," +
                        " CASE WHEN x % 3 = 0 THEN x * 2 ELSE NULL END as a," +
                        " CASE WHEN x % 3 = 1 THEN x * 3 ELSE NULL END as b" +
                        " from long_sequence(6)" +
                        ")")
                .expectSize()
                .returns("""
                        c1\tc2\ta\tb\tx
                        3\t3\tnull\t3\t1
                        null\tnull\tnull\tnull\tnull
                        6\t6\t6\tnull\t3
                        12\t12\tnull\t12\tnull
                        5\tnull\tnull\tnull\t5
                        12\t12\t12\tnull\tnull
                        """);
    }

    @Test
    public void testNoArgs() throws Exception {
        assertException(
                "select coalesce();",
                7,
                "coalesce can be used with 2 or more arguments"
        );
    }

    @Test
    public void testStr3Args() throws Exception {
        assertQuery("select coalesce(x, a, b) c1, coalesce(x, a) c2, x, a, b\n" +
                        "from alex")
                .ddl("""
                        create table alex as (\
                        SELECT rnd_str('X',NULL,NULL) as x
                        , rnd_str('A','AA',NULL,NULL) as a
                        , rnd_str('B',NULL) as b
                        from long_sequence(5)\
                        )""")
                .expectSize()
                .returns("""
                        c1\tc2\tx\ta\tb
                        X\tX\tX\t\t
                        AA\tAA\t\tAA\t
                        B\t\t\t\tB
                        A\tA\t\tA\tB
                        \t\t\t\t
                        """);
    }

    @Test
    public void testStrCoalesceSymbolNocacheSorted() throws Exception {
        assertQuery("""
                select coalesce(x, a) as coalesce, x, a
                from t
                order by 1""")
                .ddl("create table t (x string, a symbol nocache)")
                .mutateWith("""
                        insert into t select \
                         rnd_str(NULL, 'X', 'Y') as x,
                         rnd_symbol('A', 'B', NULL) as a
                        from long_sequence(5)""")
                .expectSize()
                .returns("coalesce\tx\ta\n", """
                        coalesce\tx\ta
                        A\t\tA
                        B\t\tB
                        X\tX\t
                        Y\tY\t
                        Y\tY\tB
                        """);
    }

    @Test
    public void testSymbol3Args() throws Exception {
        assertQuery("select coalesce(x, a, b) c1, coalesce(x, a) c2, x, a, b " +
                        "from alex")
                .ddl("""
                        create table alex as (\
                        SELECT rnd_symbol('X',NULL,NULL) as x
                        , rnd_symbol('A','AA', NULL) as a
                        , rnd_symbol('B','BB') as b
                        from long_sequence(5)\
                        )""")
                .expectSize()
                .returns("""
                        c1\tc2\tx\ta\tb
                        X\tX\tX\tA\tBB
                        BB\t\t\t\tBB
                        AA\tAA\t\tAA\tB
                        AA\tAA\t\tAA\tB
                        AA\tAA\t\tAA\tBB
                        """);
    }

    @Test
    public void testSymbolCoalesceCharAndString() throws Exception {
        assertQuery("""
                select coalesce(x, a) as coalesce, x, a
                from alex
                order by 1""")
                .ddl("""
                        create table alex as (\
                        SELECT rnd_str('X',NULL) as x
                        , rnd_char() as a
                        from long_sequence(5)\
                        )""")
                .expectSize()
                .returns("""
                        coalesce\tx\ta
                        P\t\tP
                        W\t\tW
                        X\tX\tT
                        X\tX\tW
                        X\tX\tY
                        """);
    }

    @Test
    public void testSymbolCoalesceCharAndVarchar() throws Exception {
        assertQuery("""
                select coalesce(x, a) as coalesce, x, a
                from alex
                order by 1""")
                .ddl("""
                        create table alex as (\
                        SELECT rnd_varchar('X',NULL) as x
                        , rnd_char() as a
                        from long_sequence(5)\
                        )""")
                .expectSize()
                .returns("""
                        coalesce\tx\ta
                        P\t\tP
                        W\t\tW
                        X\tX\tT
                        X\tX\tW
                        X\tX\tY
                        """);
    }

    @Test
    public void testSymbolCoalesceShortAndByte() throws Exception {
        assertQuery("""
                select coalesce(x, a) as coalesce1, coalesce(a, x) as coalesce2, x, a
                from alex
                order by 1""")
                .ddl("""
                        create table alex as (\
                        SELECT CAST(x as BYTE) as x
                        , CAST(x*2 as SHORT) as a
                        from long_sequence(5)\
                        )""")
                .expectSize()
                .returns("""
                        coalesce1\tcoalesce2\tx\ta
                        1\t2\t1\t2
                        2\t4\t2\t4
                        3\t6\t3\t6
                        4\t8\t4\t8
                        5\t10\t5\t10
                        """);
    }

    @Test
    public void testSymbolCoalesceStr2() throws Exception {
        assertQuery("select coalesce(x, a) as coalesce, x, a " +
                        "from alex")
                .ddl("""
                        create table alex as (\
                        SELECT rnd_str('X',NULL) as x
                        , rnd_symbol('A', 'AA') as a
                        from long_sequence(5)\
                        )""")
                .expectSize()
                .returns("""
                        coalesce\tx\ta
                        X\tX\tA
                        AA\t\tAA
                        AA\t\tAA
                        X\tX\tAA
                        X\tX\tA
                        """);
    }

    @Test
    public void testSymbolCoalesceStrSorted() throws Exception {
        assertQuery("""
                select coalesce(x, a) as coalesce, x, a
                from alex
                order by 1""")
                .ddl("""
                        create table alex as (\
                        SELECT rnd_str('X',NULL) as x
                        , rnd_symbol('A', 'AA') as a
                        from long_sequence(5)\
                        )""")
                .expectSize()
                .returns("""
                        coalesce\tx\ta
                        AA\t\tAA
                        AA\t\tAA
                        X\tX\tA
                        X\tX\tAA
                        X\tX\tA
                        """);
    }

    @Test
    public void testSymbolCoalesceVarchar2() throws Exception {
        assertQuery("select coalesce(x, a) as coalesce, x, a " +
                        "from alex")
                .ddl("""
                        create table alex as (\
                        SELECT rnd_varchar('X',NULL) as x
                        , rnd_symbol('A', 'AA') as a
                        from long_sequence(5)\
                        )""")
                .expectSize()
                .returns("""
                        coalesce\tx\ta
                        X\tX\tA
                        AA\t\tAA
                        AA\t\tAA
                        X\tX\tAA
                        X\tX\tA
                        """);
    }

    @Test
    public void testSymbolCoalesceVarcharSorted() throws Exception {
        assertQuery("""
                select coalesce(x, a) as coalesce, x, a
                from alex
                order by 1""")
                .ddl("""
                        create table alex as (\
                        SELECT rnd_varchar('X',NULL) as x
                        , rnd_symbol('A', 'AA') as a
                        from long_sequence(5)\
                        )""")
                .expectSize()
                .returns("""
                        coalesce\tx\ta
                        AA\t\tAA
                        AA\t\tAA
                        X\tX\tA
                        X\tX\tAA
                        X\tX\tA
                        """);
    }

    @Test
    public void testSymbolNocache3ArgsSorted() throws Exception {
        assertQuery("select coalesce(x, a, b) coalesce, x, a, b " +
                        "from t\n" +
                        "order by 1")
                .ddl("create table t (x symbol nocache, a symbol nocache, b symbol nocache)")
                .mutateWith("""
                        insert into t select \
                         rnd_symbol('X', 'Y', NULL) as x,
                         rnd_symbol('A', 'AA', NULL) as a,
                         rnd_symbol('B', 'BB', NULL) as b
                        from long_sequence(5)""")
                .expectSize()
                .returns("coalesce\tx\ta\tb\n", """
                        coalesce\tx\ta\tb
                        \t\t\t
                        AA\t\tAA\tB
                        X\tX\tA\tBB
                        Y\tY\tAA\t
                        Y\tY\tAA\tBB
                        """);
    }

    @Test
    public void testSymbolNocacheCoalesceSorted() throws Exception {
        assertQuery("""
                select coalesce(a, x) as coalesce, x, a
                from t
                order by 1""")
                .ddl("create table t (x symbol nocache, a symbol nocache)")
                .mutateWith("""
                        insert into t select \
                         rnd_symbol('X', 'Y', 'Z', NULL) as x,
                         rnd_symbol('A', 'B', 'C', NULL) as a
                        from long_sequence(10)""")
                .expectSize()
                .returns("coalesce\tx\ta\n", """
                        coalesce\tx\ta
                        A\tZ\tA
                        A\tX\tA
                        A\tY\tA
                        B\tX\tB
                        C\tX\tC
                        C\tY\tC
                        Y\tY\t
                        Y\tY\t
                        Z\tZ\t
                        Z\tZ\t
                        """);
    }

    @Test
    public void testSymbolNocacheCoalesceStrSorted() throws Exception {
        assertQuery("""
                select coalesce(a, x) as coalesce, x, a
                from t
                order by 1""")
                .ddl("create table t (x string, a symbol nocache)")
                .mutateWith("""
                        insert into t select \
                         rnd_str('X', NULL) as x,
                         rnd_symbol('A', 'AA') as a
                        from long_sequence(5)""")
                .expectSize()
                .returns("coalesce\tx\ta\n", """
                        coalesce\tx\ta
                        A\tX\tA
                        A\tX\tA
                        AA\t\tAA
                        AA\t\tAA
                        AA\tX\tAA
                        """);
    }

    @Test
    public void testSymbolNocacheCoalesceVarcharSorted() throws Exception {
        assertQuery("""
                select coalesce(a, x) as coalesce, x, a
                from t
                order by 1""")
                .ddl("create table t (x varchar, a symbol nocache)")
                .mutateWith("""
                        insert into t select \
                         rnd_varchar('X', NULL) as x,
                         rnd_symbol('A', 'AA') as a
                        from long_sequence(5)""")
                .expectSize()
                .returns("coalesce\tx\ta\n", """
                        coalesce\tx\ta
                        A\tX\tA
                        A\tX\tA
                        AA\t\tAA
                        AA\t\tAA
                        AA\tX\tAA
                        """);
    }

    @Test
    public void testTestCoalesceImplicitCasts() throws Exception {
        assertCoalesce("cast('0.0.1.1' as varchar)", "0.0.1.1");
        assertCoalesce("'2'", "2");
        assertCoalesce("'abc'::symbol", "abc");
    }

    @Test
    public void testTimestampCoalesce() throws Exception {
        assertQuery("select coalesce(b, a, x) c1, coalesce(b, a) c2, a, b, x\n" +
                        "from alex")
                .ddl("create table alex as (" +
                        "select " +
                        "  CASE WHEN x % 2 = 0 THEN CAST(x as Timestamp) ELSE CAST(NULL as Timestamp) END as x" +
                        ", CASE WHEN x % 4 = 0 THEN CAST(x * 10000000000 as Timestamp) ELSE CAST(NULL as Timestamp) END as a" +
                        ", CASE WHEN x % 4 = 1 THEN CAST(x * 50000000000 as Timestamp) ELSE CAST(NULL as Timestamp) END as b" +
                        " from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        c1\tc2\ta\tb\tx
                        1970-01-01T13:53:20.000000Z\t1970-01-01T13:53:20.000000Z\t\t1970-01-01T13:53:20.000000Z\t
                        1970-01-01T00:00:00.000002Z\t\t\t\t1970-01-01T00:00:00.000002Z
                        \t\t\t\t
                        1970-01-01T11:06:40.000000Z\t1970-01-01T11:06:40.000000Z\t1970-01-01T11:06:40.000000Z\t\t1970-01-01T00:00:00.000004Z
                        1970-01-03T21:26:40.000000Z\t1970-01-03T21:26:40.000000Z\t\t1970-01-03T21:26:40.000000Z\t
                        """);
    }

    @Test
    public void testTimestampMixedCoalesce() throws Exception {
        assertQuery("select coalesce(b, a, x) c1, coalesce(b, a) c2, a, b, x\n" +
                        "from alex")
                .ddl("create table alex as (" +
                        "select " +
                        "  CASE WHEN x % 2 = 0 THEN CAST(x as Timestamp_NS) ELSE CAST(NULL as Timestamp_NS) END as x" +
                        ", CASE WHEN x % 4 = 0 THEN CAST(x * 10000000000 as Timestamp) ELSE CAST(NULL as Timestamp) END as a" +
                        ", CASE WHEN x % 4 = 1 THEN CAST(x * 50000000000 as Timestamp_NS) ELSE CAST(NULL as Timestamp_NS) END as b" +
                        " from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        c1\tc2\ta\tb\tx
                        1970-01-01T00:00:50.000000000Z\t1970-01-01T00:00:50.000000000Z\t\t1970-01-01T00:00:50.000000000Z\t
                        1970-01-01T00:00:00.000000002Z\t\t\t\t1970-01-01T00:00:00.000000002Z
                        \t\t\t\t
                        1970-01-01T11:06:40.000000000Z\t1970-01-01T11:06:40.000000000Z\t1970-01-01T11:06:40.000000Z\t\t1970-01-01T00:00:00.000000004Z
                        1970-01-01T00:04:10.000000000Z\t1970-01-01T00:04:10.000000000Z\t\t1970-01-01T00:04:10.000000000Z\t
                        """);

        assertQuery("select coalesce(a, b) from tango")
                .ddl("create table tango as (" +
                        "select " +
                        " x::timestamp a," +
                        " x::timestamp_ns b" +
                        " from long_sequence(2)" +
                        ")")
                .expectSize()
                .returns("""
                        coalesce
                        1970-01-01T00:00:00.000001000Z
                        1970-01-01T00:00:00.000002000Z
                        """);
    }

    @Test
    public void testTimestampNsCoalesce() throws Exception {
        assertQuery("select coalesce(b, a, x) c1, coalesce(b, a) c2, a, b, x\n" +
                        "from alex")
                .ddl("create table alex as (" +
                        "select " +
                        "  CASE WHEN x % 2 = 0 THEN CAST(x as Timestamp_NS) ELSE CAST(NULL as Timestamp) END as x" +
                        ", CASE WHEN x % 4 = 0 THEN CAST(x * 10000000000 as Timestamp_NS) ELSE CAST(NULL as Timestamp_NS) END as a" +
                        ", CASE WHEN x % 4 = 1 THEN CAST(x * 50000000000 as Timestamp_NS) ELSE CAST(NULL as Timestamp_NS) END as b" +
                        " from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        c1\tc2\ta\tb\tx
                        1970-01-01T00:00:50.000000000Z\t1970-01-01T00:00:50.000000000Z\t\t1970-01-01T00:00:50.000000000Z\t
                        1970-01-01T00:00:00.000000002Z\t\t\t\t1970-01-01T00:00:00.000000002Z
                        \t\t\t\t
                        1970-01-01T00:00:40.000000000Z\t1970-01-01T00:00:40.000000000Z\t1970-01-01T00:00:40.000000000Z\t\t1970-01-01T00:00:00.000000004Z
                        1970-01-01T00:04:10.000000000Z\t1970-01-01T00:04:10.000000000Z\t\t1970-01-01T00:04:10.000000000Z\t
                        """);
    }

    @Test
    public void testUnsupportedBindVariables() throws Exception {
        execute("create table test as (select x, rnd_str(2,10,1) a from long_sequence(10))");
        assertException(
                "select coalesce(a, $1, $2) from test",
                19,
                "coalesce cannot be used with bind variables"
        );
    }

    @Test
    public void testVarchar3Args() throws Exception {
        assertQuery("select coalesce(x, a, b) c1, coalesce(x, a) c2, x, a, b\n" +
                        "from alex")
                .ddl("""
                        create table alex as (\
                        SELECT rnd_varchar('X',NULL,NULL) as x
                        , rnd_varchar('A','AA',NULL,NULL) as a
                        , rnd_varchar('B',NULL) as b
                        from long_sequence(5)\
                        )""")
                .expectSize()
                .returns("""
                        c1\tc2\tx\ta\tb
                        X\tX\tX\t\t
                        AA\tAA\t\tAA\t
                        B\t\t\t\tB
                        A\tA\t\tA\tB
                        \t\t\t\t
                        """);
    }

    @Test
    public void testVarcharCoalesceSymbolNocacheSorted() throws Exception {
        assertQuery("""
                select coalesce(x, a) as coalesce, x, a
                from t
                order by 1""")
                .ddl("create table t (x varchar, a symbol nocache)")
                .mutateWith("""
                        insert into t select \
                         rnd_varchar(NULL, 'X', 'Y') as x,
                         rnd_symbol('A', 'B', NULL) as a
                        from long_sequence(5)""")
                .expectSize()
                .returns("coalesce\tx\ta\n", """
                        coalesce\tx\ta
                        A\t\tA
                        B\t\tB
                        X\tX\t
                        Y\tY\t
                        Y\tY\tB
                        """);
    }

    private void assertCoalesce(String value, String expected) throws Exception {
        assertQuery("select coalesce(" + value + ", '')")
                .expectSize()
                .returns("coalesce\n" + expected + "\n");
        assertQuery("select coalesce(" + value + ", " + value + ", '')")
                .expectSize()
                .returns("coalesce\n" + expected + "\n");
    }
}
