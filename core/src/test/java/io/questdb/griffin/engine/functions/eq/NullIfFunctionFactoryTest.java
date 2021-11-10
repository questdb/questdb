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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Test;

public class NullIfFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testStrSimple() throws Exception {
        assertQuery(
                "str1\tstr2\tnullif\n" +
                        "cat\tcat\t\n" +
                        "dog\t\t\n" +
                        "\t\t\n" +
                        "\tdog\t\n" +
                        "cat\tdog\tcat\n" +
                        "dog\t\t\n" +
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
                false,
                true
        );
    }

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
                false,
                true
        );
    }

    @Test
    public void testIntSimple() throws Exception {
        assertQuery(
                "int\tnullif\n" +
                        "4\t4\n" +
                        "2\t2\n" +
                        "5\tNaN\n" +
                        "2\t2\n" +
                        "4\t4\n",
                "select int,nullif(int,5) from x",
                "create table x as (" +
                        "select rnd_int(1,5,0) as int\n" +
                        "from long_sequence(5)" +
                        ")",
                null,
                true,
                false,
                true
        );
    }

    @Test
    public void testIntConstNull() throws Exception {
        assertQuery(
                "nullif1\tnullif2\n" +
                        "NaN\t5\n",
                "select nullif(null,5) nullif1, nullif(5,null) nullif2",
                null,
                null,
                true,
                false,
                true
        );
    }

}
