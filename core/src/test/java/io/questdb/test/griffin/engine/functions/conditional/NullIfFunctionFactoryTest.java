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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class NullIfFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCharSimple() throws Exception {
        assertQuery("select ch1,ch2,nullif(ch1,ch2) from x")
                .ddl("""
                        create table x as (\
                        select rnd_char() as ch1
                        , rnd_char() as ch2
                        from long_sequence(20)\
                        )""")
                .expectSize()
                .returns("""
                        ch1\tch2\tnullif
                        V\tT\tV
                        J\tW\tJ
                        C\tP\tC
                        S\tW\tS
                        H\tY\tH
                        R\tX\tR
                        P\tE\tP
                        H\tN\tH
                        R\tX\tR
                        G\tZ\tG
                        S\tX\tS
                        U\tX\tU
                        I\tB\tI
                        B\tT\tB
                        G\tP\tG
                        G\tW\tG
                        F\tF\t
                        Y\tU\tY
                        D\tE\tD
                        Y\tY\t
                        """);
    }

    @Test
    public void testDecimalSimple() throws Exception {
        assertQuery("select dec, nullif(dec,0.3m) from x")
                .ddl("create table x as (" +
                        "select x / 10.0m as dec\n" +
                        "from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        dec\tnullif
                        0.1\t0.1
                        0.2\t0.2
                        0.3\t
                        0.4\t0.4
                        0.5\t0.5
                        """);
    }

    @Test
    public void testDoubleNonConstant() throws Exception {
        assertQuery("""
                select nullif(five, four) from x\s
                UNION\s
                select nullif(five, five) from x\s
                UNION\s
                select nullif(four, five) from x\s
                """)
                .ddl("create table x as (" +
                        "SELECT 5::double as five, 4::double as four" +
                        ")")
                .noRandomAccess()
                .returns("""
                        nullif
                        5.0
                        null
                        4.0
                        """);
    }

    @Test
    public void testDoubleSimple() throws Exception {
        assertQuery("select double,nullif(double,0.3) from x")
                .ddl("create table x as (" +
                        "select x / 10.0 as double\n" +
                        "from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        double\tnullif
                        0.1\t0.1
                        0.2\t0.2
                        0.3\tnull
                        0.4\t0.4
                        0.5\t0.5
                        """);
    }

    @Test
    public void testIntConstNull() throws Exception {
        assertQuery("select nullif(null,5) nullif1, nullif(5,null) nullif2")
                .expectSize()
                .returns("""
                        nullif1\tnullif2
                        null\t5
                        """);
    }

    @Test
    public void testIntNonConstant() throws Exception {
        assertQuery("""
                select nullif(five, four) from x\s
                UNION\s
                select nullif(five, five) from x\s
                UNION\s
                select nullif(four, five) from x\s
                """)
                .ddl("create table x as (" +
                        "SELECT 5 as five, 4 as four" +
                        ")")
                .noRandomAccess()
                .returns("""
                        nullif
                        5
                        null
                        4
                        """);
    }

    @Test
    public void testIntSimple() throws Exception {
        assertQuery("select int,nullif(int,5) from x")
                .ddl("create table x as (" +
                        "select rnd_int(1,5,0) as int\n" +
                        "from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        int\tnullif
                        4\t4
                        2\t2
                        5\tnull
                        2\t2
                        4\t4
                        """);
    }

    @Test
    public void testLongConstNull() throws Exception {
        assertQuery("select nullif(null,5::long) nullif1, nullif(5::long,null) nullif2")
                .expectSize()
                .returns("""
                        nullif1\tnullif2
                        null\t5
                        """);
    }

    @Test
    public void testLongNonConstant() throws Exception {
        assertQuery("""
                select nullif(five, four) from x\s
                UNION\s
                select nullif(five, five) from x\s
                UNION\s
                select nullif(four, five) from x\s
                """)
                .ddl("create table x as (" +
                        "SELECT 5::long as five, 4::long as four" +
                        ")")
                .noRandomAccess()
                .returns("""
                        nullif
                        5
                        null
                        4
                        """);
    }

    @Test
    public void testLongSimple() throws Exception {
        assertQuery("select long,nullif(long,3) from x")
                .ddl("create table x as (" +
                        "select x as long\n" +
                        "from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        long\tnullif
                        1\t1
                        2\t2
                        3\tnull
                        4\t4
                        5\t5
                        """);
    }

    @Test
    public void testStrSimple() throws Exception {
        assertQuery("select str1,str2,nullif(str1,str2) from x")
                .ddl("""
                        create table x as (\
                        select rnd_str('cat','dog',NULL) as str1
                        , rnd_str('cat','dog',NULL) as str2
                        from long_sequence(10)\
                        )""")
                .expectSize()
                .returns("""
                        str1\tstr2\tnullif
                        cat\tcat\t
                        dog\t\tdog
                        \t\t
                        \tdog\t
                        cat\tdog\tcat
                        dog\t\tdog
                        dog\tdog\t
                        dog\tcat\tdog
                        cat\tdog\tcat
                        cat\tdog\tcat
                        """);
    }

    @Test
    public void testVarcharSimple() throws Exception {
        assertQuery("select str1,str2,nullif(str1,str2) from x")
                .ddl("""
                        create table x as (\
                        select rnd_varchar('cat','dog',NULL) as str1
                        , rnd_varchar('cat','dog',NULL) as str2
                        from long_sequence(10)\
                        )""")
                .expectSize()
                .returns("""
                        str1\tstr2\tnullif
                        cat\tcat\t
                        dog\t\tdog
                        \t\t
                        \tdog\t
                        cat\tdog\tcat
                        dog\t\tdog
                        dog\tdog\t
                        dog\tcat\tdog
                        cat\tdog\tcat
                        cat\tdog\tcat
                        """);
    }
}
