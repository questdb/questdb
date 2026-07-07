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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class PositionFunctionFactoryTest extends AbstractCairoTest {
    @Test
    public void testCharVar() throws Exception {
        assertQuery("select substr,str,position(str,substr) from x")
                .ddl("""
                        create table x as (\
                        select rnd_str('TEST','A X X','CDE',NULL) as str
                        , rnd_char() as substr
                        from long_sequence(15)\
                        )""")
                .expectSize()
                .returns("""
                        substr\tstr\tposition
                        T\tTEST\t1
                        W\tA X X\t0
                        P\tA X X\t0
                        W\tCDE\t0
                        Y\tCDE\t0
                        X\tTEST\t0
                        E\tCDE\t3
                        N\tA X X\t0
                        X\tTEST\t0
                        Z\tA X X\t0
                        X\t\tnull
                        X\tTEST\t0
                        B\t\tnull
                        T\tCDE\t0
                        P\t\tnull
                        """);
    }

    @Test
    public void testCharVarConst() throws Exception {
        assertQuery("select str,position(str,'C') from x")
                .ddl("create table x as (" +
                        "select rnd_str('ABC XYZ XYZ','CBA','XYZ') as str\n" +
                        "from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        str\tposition
                        ABC XYZ XYZ\t3
                        ABC XYZ XYZ\t3
                        CBA\t1
                        XYZ\t0
                        XYZ\t0
                        """);
    }

    @Test
    public void testConstantEmptyString() throws Exception {
        assertQuery("select position('','a') pos1, position('a',cast('' as string)) pos2, position('','') pos3")
                .expectSize()
                .returns("""
                        pos1\tpos2\tpos3
                        0\t1\t1
                        """);
    }

    @Test
    public void testConstantNull() throws Exception {
        assertQuery("select position(null,'a') pos1, position(null,'abc') pos2, position('a',null) pos3, position(null,null) pos4")
                .expectSize()
                .returns("""
                        pos1\tpos2\tpos3\tpos4
                        null\tnull\tnull\tnull
                        """);
    }

    @Test
    public void testSplitColumn() throws Exception {
        assertQuery("""
                select str,
                left(str, position(str, ',') - 1) str1,
                right(str, length(str) - position(str, ',')) str2
                from x""")
                .ddl("create table x as (" +
                        "select rnd_str('dog,cat','apple,pear') as str\n" +
                        "from long_sequence(3)" +
                        ")")
                .expectSize()
                .returns("""
                        str\tstr1\tstr2
                        dog,cat\tdog\tcat
                        dog,cat\tdog\tcat
                        apple,pear\tapple\tpear
                        """);
    }

    @Test
    public void testStrVar() throws Exception {
        assertQuery("select substr,str,position(str,substr) from x")
                .ddl("""
                        create table x as (\
                        select rnd_str('ABC XYZ XYZ','XYZ','XYW',NULL) as str
                        , rnd_str('XYZ','C',NULL) as substr
                        from long_sequence(15)\
                        )""")
                .expectSize()
                .returns("""
                        substr\tstr\tposition
                        XYZ\tABC XYZ XYZ\t5
                        \tXYZ\tnull
                        \tXYZ\tnull
                        C\tXYW\t0
                        C\tXYW\t0
                        \tABC XYZ XYZ\tnull
                        C\tXYW\t0
                        XYZ\tXYZ\t1
                        C\tABC XYZ XYZ\t3
                        C\tXYZ\t0
                        XYZ\t\tnull
                        \tABC XYZ XYZ\tnull
                        C\t\tnull
                        C\tXYW\t0
                        XYZ\t\tnull
                        """);
    }

    @Test
    public void testStrVarConst() throws Exception {
        assertQuery("select str,position(str,'XYZ') from x")
                .ddl("create table x as (" +
                        "select rnd_str('ABC XYZ XYZ','XYZ','XYW') as str\n" +
                        "from long_sequence(5)" +
                        ")")
                .expectSize()
                .returns("""
                        str\tposition
                        ABC XYZ XYZ\t5
                        ABC XYZ XYZ\t5
                        XYZ\t1
                        XYW\t0
                        XYW\t0
                        """);
    }

}