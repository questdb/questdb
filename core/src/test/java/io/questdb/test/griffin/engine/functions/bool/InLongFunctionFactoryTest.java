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

package io.questdb.test.griffin.engine.functions.bool;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class InLongFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBindVariables() throws Exception {
        bindVariableService.clear();
        bindVariableService.setLong(0, 4);
        bindVariableService.setLong(1, 2);
        assertQuery(
                "x\n" +
                        "2\n" +
                        "4\n",
                "select * from x where x in ($1,$2)",
                "create table x as (" +
                        "select x from long_sequence(10)" +
                        ")",
                null,
                true,
                false
        );
    }

    @Test
    public void testManyConst() throws Exception {
        assertQuery(
                "x\n" +
                        "1\n" +
                        "3\n" +
                        "5\n" +
                        "7\n",
                "select * from x where x in (7,5,3,1)",
                "create table x as (" +
                        "select x from long_sequence(10)" +
                        ")",
                null,
                true,
                false
        );
    }

    @Test
    public void testSingleConst() throws Exception {
        assertQuery(
                "x\n" +
                        "1\n",
                "select * from x where x in (1)",
                "create table x as (" +
                        "select x from long_sequence(5)" +
                        ")",
                null,
                true,
                false
        );
    }

    @Test
    public void testTwoConst() throws Exception {
        assertQuery(
                "x\n" +
                        "1\n" +
                        "2\n",
                "select * from x where x in (2,1)",
                "create table x as (" +
                        "select x from long_sequence(5)" +
                        ")",
                null,
                true,
                false
        );
    }
}
