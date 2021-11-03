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

public class StrPosFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testSimple() throws Exception {
        assertQuery(
                "substr\tstr\tstrpos\n" +
                        "XYZ\tABC XYZ XYZ\t5\n" +
                        "C\tXYZ\t0\n" +
                        "C\tXYW\t0\n" +
                        "C\tXYW\t0\n" +
                        "XYZ\tABC XYZ XYZ\t5\n" +
                        "XYZ\tXYZ\t1\n" +
                        "C\tXYZ\t0\n" +
                        "XYZ\tXYZ\t1\n" +
                        "C\tABC XYZ XYZ\t3\n" +
                        "XYZ\tABC XYZ XYZ\t5\n" +
                        "C\tXYZ\t0\n" +
                        "C\tABC XYZ XYZ\t3\n" +
                        "XYZ\tABC XYZ XYZ\t5\n" +
                        "XYZ\tXYW\t0\n" +
                        "XYZ\tXYZ\t1\n",
                "select substr,str,strpos(str,substr) from x",
                "create table x as (" +
                        "select rnd_str('ABC XYZ XYZ','XYZ','XYW') as str\n" +
                        ", rnd_str('XYZ','C') as substr\n" +
                        "from long_sequence(15)" +
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
                "strpos\n" +
                        "NaN\n",
                "select strpos(null,'a')",
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
                        // TODO fix the function, so that the result is "0\t1\t1\n",
                        "NaN\tNaN\tNaN\n",
                "select strpos('','a') pos1, strpos('a',cast('' as string)) pos2, strpos('','') pos3",
                null,
                null,
                true,
                false,
                true
        );
    }

}
