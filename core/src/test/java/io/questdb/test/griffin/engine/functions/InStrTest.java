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

package io.questdb.test.griffin.engine.functions;

import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVariableTestTuple;
import org.junit.Test;

public class InStrTest extends AbstractCairoTest {

    @Test
    public void testBindVarTypeChange() throws SqlException {
        ddl("create table test as (select x, rnd_str(3,6,1) a from long_sequence(100))");

        // when more than one argument supplied, the function will match exact values from the list
        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "simple",
                "x\ta\n" +
                        "26\tSUW\n" +
                        "69\tWZLUOG\n" +
                        "87\tGQZVK\n",
                bindVariableService -> {
                    bindVariableService.setStr(0, "WZLUOG");
                    bindVariableService.setStr(1, "GQZVK");
                    bindVariableService.setStr(2, "SUW");
                }
        ));

        tuples.add(new BindVariableTestTuple(
                "undefined bind variable",
                "undefined bind variable: 1",
                bindVariableService -> {
                    bindVariableService.setStr(0, "ELLKK");
                    bindVariableService.setStr(2, "GNJJILL");
                },
                20
        ));

        tuples.add(new BindVariableTestTuple(
                "bad type",
                "inconvertible types: INT -> TIMESTAMP [from=INT, to=TIMESTAMP]",
                bindVariableService -> {
                    bindVariableService.setStr(0, "ELLKK");
                    bindVariableService.setInt(1, 30);
                    bindVariableService.setStr(2, "GNJJILL");
                },
                20
        ));

        tuples.add(new BindVariableTestTuple(
                "with nulls",
                "x\ta\n" +
                        "2\t\n" +
                        "4\tNRXG\n" +
                        "19\t\n" +
                        "24\t\n" +
                        "29\t\n" +
                        "30\t\n" +
                        "32\t\n" +
                        "33\t\n" +
                        "35\t\n" +
                        "36\t\n" +
                        "38\t\n" +
                        "39\t\n" +
                        "40\t\n" +
                        "41\t\n" +
                        "43\t\n" +
                        "49\t\n" +
                        "55\t\n" +
                        "62\t\n" +
                        "66\t\n" +
                        "68\t\n" +
                        "70\t\n" +
                        "73\t\n" +
                        "84\t\n" +
                        "93\tNPIW\n",
                bindVariableService -> {
                    bindVariableService.setStr(0, "NPIW");
                    bindVariableService.setStr(1, null);
                    bindVariableService.setStr(2, "NRXG");
                }
        ));

        assertSql("test where a in ($1,$2,$3)", tuples);
    }
}
