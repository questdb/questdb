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

package io.questdb.griffin.engine.functions.analytic;

import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Test;

public class RowNumberFunctionTest extends AbstractGriffinTest {
    @Test
    public void testRowNumberSequence() throws Exception {
        assertSql("select row_number() from long_sequence(3)",
                "row_number\n" +
                        "1\n" +
                        "2\n" +
                        "3\n"
        );
    }

    @Test
    public void testRowNumberPartition() throws Exception {
        // a	b
        //false	315515118
        //false	-727724771
        //false	-948263339
        //true	592859671
        //true	-847531048
        //false	-2041844972
        //false	-1575378703
        //true	1545253512
        //true	1573662097
        //false	339631474

        // a	row_number
        //false	1
        //false	2
        //false	3
        //true	2
        //true	3
        //false	5
        //false	4
        //true	1
        //true	0
        //false	0
        assertQuery("a\trow_number\n" +
                        "false\t1\n" +
                        "false\t2\n" +
                        "false\t3\n" +
                        "true\t2\n" +
                        "true\t3\n" +
                        "false\t5\n" +
                        "false\t4\n" +
                        "true\t1\n" +
                        "true\t0\n" +
                        "false\t0\n",
               "select a, row_number() over (partition by a order by b desc) from tmp",
                "create table tmp as (select rnd_boolean() a, rnd_int() b from long_sequence(10))",
                null
        );
    }

    @Test
    public void testRowNumberPartitionbySingle() throws Exception {
        // even though this test works, testRowNumberSequence will fail!
        assertQuery("row_number\n" +
                        "1\n" +
                        "2\n" +
                        "3\n" +
                        "2\n" +
                        "3\n" +
                        "5\n" +
                        "4\n" +
                        "1\n" +
                        "0\n" +
                        "0\n",
                "select row_number() over (partition by a order by b desc) from tmp2",
                "create table tmp2 as (select rnd_boolean() a, rnd_int() b from long_sequence(10))",
                null
        );
    }
}
