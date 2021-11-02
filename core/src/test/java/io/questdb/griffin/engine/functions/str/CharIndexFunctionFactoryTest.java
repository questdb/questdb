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

public class CharIndexFunctionFactoryTest  extends AbstractGriffinTest {

    @Test
    public void testSimple() throws Exception {
        assertQuery(
                "substr\tstr\tcharindex\n" +
                        "XYZ\tXYZ\t1\n" +
                        "\tABC XYZ XYZ\t0\n" +
                        "\t\t0\n" +
                        "D\t\t0\n" +
                        "D\tXYZ\t0\n" +
                        "\tABC XYZ XYZ\t0\n" +
                        "D\tABC XYZ XYZ\t0\n" +
                        "XYZ\tABC XYZ XYZ\t5\n" +
                        "D\tXYZ\t0\n" +
                        "D\tXYZ\t0\n",
                "select substr,str,charindex(substr,str) from x",
                "create table x as (" +
                        "select rnd_str('XYZ','ABC XYZ XYZ',NULL) as str\n" +
                        ", rnd_str('XYZ','D',NULL) as substr\n" +
                        "from long_sequence(10)" +
                        ")",
                null,
                true,
                false,
                true
        );
    }

    @Test
    public void testStart() throws Exception {
        assertQuery(
                "substr\tstr\tstart\tcharindex\n" +
                        ",\tcat,dog\t10\t0\n" +
                        ",\t\t6\t0\n" +
                        ",\tcat,dog\t7\t0\n" +
                        ",\ta,b,c\t-1\t2\n" +
                        ",\tcat,dog\t9\t0\n" +
                        ",\ta,b,c\t4\t4\n" +
                        ",\tcat,dog\t3\t4\n" +
                        ",\ta,b,c\t6\t0\n" +
                        ",\t\t0\t0\n" +
                        ",\t\t3\t0\n",
                "select substr,str,start,charindex(substr,str,start) from x",
                "create table x as (" +
                        "select rnd_str('cat,dog','a,b,c',NULL) as str\n" +
                        ", rnd_str(',') as substr\n" +
                        ", rnd_int(-1,10,0) as start\n" +
                        "from long_sequence(10)" +
                        ")",
                null,
                true,
                false,
                true
        );
    }

}
