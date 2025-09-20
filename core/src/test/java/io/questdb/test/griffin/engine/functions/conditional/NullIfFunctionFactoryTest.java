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

public class NullIfFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCharSimple() throws Exception {
        assertQuery(
                "ch1\tch2\tnullif\n" +
                        "V\tT\tV\n" +
                        "J\tW\tJ\n" +
                        "C\tP\tC\n" +
                        "S\tW\tS\n" +
                        "H\tY\tH\n" +
                        "R\tX\tR\n" +
                        "P\tE\tP\n" +
                        "H\tN\tH\n" +
                        "R\tX\tR\n" +
                        "G\tZ\tG\n" +
                        "S\tX\tS\n" +
                        "U\tX\tU\n" +
                        "I\tB\tI\n" +
                        "B\tT\tB\n" +
                        "G\tP\tG\n" +
                        "G\tW\tG\n" +
                        "F\tF\t\n" +
                        "Y\tU\tY\n" +
                        "D\tE\tD\n" +
                        "Y\tY\t\n",
                "select ch1,ch2,nullif(ch1,ch2) from x",
                "create table x as (" +
                        "select rnd_char() as ch1\n" +
                        ", rnd_char() as ch2\n" +
                        "from long_sequence(20)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDecimalSimple() throws Exception {
        assertQuery(
                "dec\tnullif\n" +
                        "0.1\t0.1\n" +
                        "0.2\t0.2\n" +
                        "0.3\t\n" +
                        "0.4\t0.4\n" +
                        "0.5\t0.5\n",
                "select dec, nullif(dec,0.3m) from x",
                "create table x as (" +
                        "select x / 10.0m as dec\n" +
                        "from long_sequence(5)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testDoubleNonConstant() throws Exception {
        assertQuery(
                "nullif\n" +
                        "5.0\n" +
                        "null\n" +
                        "4.0\n",

                "select nullif(five, four) from x \n" +
                        "UNION \n" +
                        "select nullif(five, five) from x \n" +
                        "UNION \n" +
                        "select nullif(four, five) from x \n",
                "create table x as (" +
                        "SELECT 5::double as five, 4::double as four" +
                        ")",
                null,
                false,
                false
        );
    }

    @Test
    public void testDoubleSimple() throws Exception {
        assertQuery(
                "double\tnullif\n" +
                        "0.1\t0.1\n" +
                        "0.2\t0.2\n" +
                        "0.3\tnull\n" +
                        "0.4\t0.4\n" +
                        "0.5\t0.5\n",
                "select double,nullif(double,0.3) from x",
                "create table x as (" +
                        "select x / 10.0 as double\n" +
                        "from long_sequence(5)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntConstNull() throws Exception {
        assertQuery(
                "nullif1\tnullif2\n" +
                        "null\t5\n",
                "select nullif(null,5) nullif1, nullif(5,null) nullif2",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testIntNonConstant() throws Exception {
        assertQuery(
                "nullif\n" +
                        "5\n" +
                        "null\n" +
                        "4\n",

                "select nullif(five, four) from x \n" +
                        "UNION \n" +
                        "select nullif(five, five) from x \n" +
                        "UNION \n" +
                        "select nullif(four, five) from x \n",
                "create table x as (" +
                        "SELECT 5 as five, 4 as four" +
                        ")",
                null,
                false,
                false
        );
    }

    @Test
    public void testIntSimple() throws Exception {
        assertQuery(
                "int\tnullif\n" +
                        "4\t4\n" +
                        "2\t2\n" +
                        "5\tnull\n" +
                        "2\t2\n" +
                        "4\t4\n",
                "select int,nullif(int,5) from x",
                "create table x as (" +
                        "select rnd_int(1,5,0) as int\n" +
                        "from long_sequence(5)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testLongConstNull() throws Exception {
        assertQuery(
                "nullif1\tnullif2\n" +
                        "null\t5\n",
                "select nullif(null,5::long) nullif1, nullif(5::long,null) nullif2",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testLongNonConstant() throws Exception {
        assertQuery(
                "nullif\n" +
                        "5\n" +
                        "null\n" +
                        "4\n",

                "select nullif(five, four) from x \n" +
                        "UNION \n" +
                        "select nullif(five, five) from x \n" +
                        "UNION \n" +
                        "select nullif(four, five) from x \n",
                "create table x as (" +
                        "SELECT 5::long as five, 4::long as four" +
                        ")",
                null,
                false,
                false
        );
    }

    @Test
    public void testLongSimple() throws Exception {
        assertQuery(
                "long\tnullif\n" +
                        "1\t1\n" +
                        "2\t2\n" +
                        "3\tnull\n" +
                        "4\t4\n" +
                        "5\t5\n",
                "select long,nullif(long,3) from x",
                "create table x as (" +
                        "select x as long\n" +
                        "from long_sequence(5)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testStrSimple() throws Exception {
        assertQuery(
                "str1\tstr2\tnullif\n" +
                        "cat\tcat\t\n" +
                        "dog\t\tdog\n" +
                        "\t\t\n" +
                        "\tdog\t\n" +
                        "cat\tdog\tcat\n" +
                        "dog\t\tdog\n" +
                        "dog\tdog\t\n" +
                        "dog\tcat\tdog\n" +
                        "cat\tdog\tcat\n" +
                        "cat\tdog\tcat\n",
                "select str1,str2,nullif(str1,str2) from x",
                "create table x as (" +
                        "select rnd_str('cat','dog',NULL) as str1\n" +
                        ", rnd_str('cat','dog',NULL) as str2\n" +
                        "from long_sequence(10)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testVarcharSimple() throws Exception {
        assertQuery(
                "str1\tstr2\tnullif\n" +
                        "cat\tcat\t\n" +
                        "dog\t\tdog\n" +
                        "\t\t\n" +
                        "\tdog\t\n" +
                        "cat\tdog\tcat\n" +
                        "dog\t\tdog\n" +
                        "dog\tdog\t\n" +
                        "dog\tcat\tdog\n" +
                        "cat\tdog\tcat\n" +
                        "cat\tdog\tcat\n",
                "select str1,str2,nullif(str1,str2) from x",
                "create table x as (" +
                        "select rnd_varchar('cat','dog',NULL) as str1\n" +
                        ", rnd_varchar('cat','dog',NULL) as str2\n" +
                        "from long_sequence(10)" +
                        ")",
                null,
                true,
                true
        );
    }
}
