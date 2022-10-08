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

package io.questdb.griffin.engine.functions.str;

import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Test;

public class PositionFunctionFactoryTest extends AbstractGriffinTest {
    @Test
    public void testStrVar() throws Exception {
        assertQuery(
                "substr\tstr\tposition\n" +
                        "XYZ\tABC XYZ XYZ\t5\n" +
                        "\tXYZ\tNaN\n" +
                        "\tXYZ\tNaN\n" +
                        "C\tXYW\t0\n" +
                        "C\tXYW\t0\n" +
                        "\tABC XYZ XYZ\tNaN\n" +
                        "C\tXYW\t0\n" +
                        "XYZ\tXYZ\t1\n" +
                        "C\tABC XYZ XYZ\t3\n" +
                        "C\tXYZ\t0\n" +
                        "XYZ\t\tNaN\n" +
                        "\tABC XYZ XYZ\tNaN\n" +
                        "C\t\tNaN\n" +
                        "C\tXYW\t0\n" +
                        "XYZ\t\tNaN\n",
                "select substr,str,position(str,substr) from x",
                "create table x as (" +
                        "select rnd_str('ABC XYZ XYZ','XYZ','XYW',NULL) as str\n" +
                        ", rnd_str('XYZ','C',NULL) as substr\n" +
                        "from long_sequence(15)" +
                        ")",
                null,
                true,
                false,
                true
        );
    }

    @Test
    public void testStrVarConst() throws Exception {
        assertQuery(
                "str\tposition\n" +
                        "ABC XYZ XYZ\t5\n" +
                        "ABC XYZ XYZ\t5\n" +
                        "XYZ\t1\n" +
                        "XYW\t0\n" +
                        "XYW\t0\n",
                "select str,position(str,'XYZ') from x",
                "create table x as (" +
                        "select rnd_str('ABC XYZ XYZ','XYZ','XYW') as str\n" +
                        "from long_sequence(5)" +
                        ")",
                null,
                true,
                false,
                true
        );
    }

    @Test
    public void testCharVar() throws Exception {
        assertQuery(
                "substr\tstr\tposition\n" +
                        "T\tTEST\t1\n" +
                        "W\tA X X\t0\n" +
                        "P\tA X X\t0\n" +
                        "W\tCDE\t0\n" +
                        "Y\tCDE\t0\n" +
                        "X\tTEST\t0\n" +
                        "E\tCDE\t3\n" +
                        "N\tA X X\t0\n" +
                        "X\tTEST\t0\n" +
                        "Z\tA X X\t0\n" +
                        "X\t\tNaN\n" +
                        "X\tTEST\t0\n" +
                        "B\t\tNaN\n" +
                        "T\tCDE\t0\n" +
                        "P\t\tNaN\n",
                "select substr,str,position(str,substr) from x",
                "create table x as (" +
                        "select rnd_str('TEST','A X X','CDE',NULL) as str\n" +
                        ", rnd_char() as substr\n" +
                        "from long_sequence(15)" +
                        ")",
                null,
                true,
                false,
                true
        );
    }

    @Test
    public void testCharVarConst() throws Exception {
        assertQuery(
                "str\tposition\n" +
                        "ABC XYZ XYZ\t3\n" +
                        "ABC XYZ XYZ\t3\n" +
                        "CBA\t1\n" +
                        "XYZ\t0\n" +
                        "XYZ\t0\n",
                "select str,position(str,'C') from x",
                "create table x as (" +
                        "select rnd_str('ABC XYZ XYZ','CBA','XYZ') as str\n" +
                        "from long_sequence(5)" +
                        ")",
                null,
                true,
                false,
                true
        );
    }

    @Test
    public void testConstantNull() throws Exception {
        assertQuery(
                "pos1\tpos2\tpos3\tpos4\n" +
                        "NaN\tNaN\tNaN\tNaN\n",
                "select position(null,'a') pos1, position(null,'abc') pos2, position('a',null) pos3, position(null,null) pos4",
                null,
                null,
                true,
                false,
                true
        );
    }

    @Test
    public void testConstantEmptyString() throws Exception {
        assertQuery(
                "pos1\tpos2\tpos3\n" +
                        "0\t1\t1\n",
                "select position('','a') pos1, position('a',cast('' as string)) pos2, position('','') pos3",
                null,
                null,
                true,
                false,
                true
        );
    }

    @Test
    public void testSplitColumn() throws Exception {
        assertQuery(
                "str\tstr1\tstr2\n" +
                        "dog,cat\tdog\tcat\n" +
                        "dog,cat\tdog\tcat\n" +
                        "apple,pear\tapple\tpear\n",
                "select str,\n" +
                        "left(str, position(str, ',') - 1) str1,\n" +
                        "right(str, length(str) - position(str, ',')) str2\n" +
                        "from x",
                "create table x as (" +
                        "select rnd_str('dog,cat','apple,pear') as str\n" +
                        "from long_sequence(3)" +
                        ")",
                null,
                true,
                false,
                true
        );
    }

}