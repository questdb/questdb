/*******************************************************************************
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

package io.questdb.test.griffin.engine.functions;

import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVariableTestTuple;
import org.junit.Test;

public class InSymbolTest extends AbstractCairoTest {

    @Test
    public void testBindVarTypeChange2() throws SqlException {
        execute("create table test as (select x, rnd_symbol(20, 2, 5, 1) a from long_sequence(100))");

        // when more than one argument supplied, the function will match exact values from the list
        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "simple",
                "x\ta\n" +
                        "1\tGZS\n" +
                        "29\tPDXYS\n" +
                        "30\tGZS\n" +
                        "32\tWFF\n" +
                        "38\tPDXYS\n" +
                        "57\tGZS\n" +
                        "73\tPDXYS\n" +
                        "84\tPDXYS\n" +
                        "89\tGZS\n",
                bindVariableService -> {
                    bindVariableService.setStr(0, "PDXYS");
                    bindVariableService.setStr(1, "WFF");
                    bindVariableService.setStr(2, "GZS");
                }
        ));

        tuples.add(new BindVariableTestTuple(
                "undefined bind variable",
                "undefined bind variable: 2",
                bindVariableService -> {
                    bindVariableService.setStr(0, "ELLKK");
                    bindVariableService.setStr(1, "RX");
                },
                23
        ));

        tuples.add(new BindVariableTestTuple(
                "bad type",
                "inconvertible types: INT -> SYMBOL [from=INT, to=SYMBOL]",
                bindVariableService -> {
                    bindVariableService.setStr(0, "RX");
                    bindVariableService.setInt(1, 30);
                    bindVariableService.setStr(2, "CPSWH");
                },
                20
        ));

        tuples.add(new BindVariableTestTuple(
                "with nulls",
                "x\ta\n" +
                        "4\t\n" +
                        "5\t\n" +
                        "7\t\n" +
                        "8\t\n" +
                        "12\t\n" +
                        "15\t\n" +
                        "16\t\n" +
                        "17\t\n" +
                        "19\tOJSHR\n" +
                        "20\t\n" +
                        "21\t\n" +
                        "23\t\n" +
                        "24\t\n" +
                        "26\tOJSHR\n" +
                        "27\t\n" +
                        "28\t\n" +
                        "31\t\n" +
                        "32\tWFF\n" +
                        "34\t\n" +
                        "35\t\n" +
                        "36\t\n" +
                        "40\t\n" +
                        "41\t\n" +
                        "42\t\n" +
                        "44\t\n" +
                        "45\t\n" +
                        "46\t\n" +
                        "49\t\n" +
                        "50\t\n" +
                        "52\t\n" +
                        "53\t\n" +
                        "56\t\n" +
                        "58\t\n" +
                        "60\t\n" +
                        "61\t\n" +
                        "62\t\n" +
                        "63\t\n" +
                        "65\t\n" +
                        "67\t\n" +
                        "71\t\n" +
                        "72\t\n" +
                        "74\t\n" +
                        "78\tOJSHR\n" +
                        "79\t\n" +
                        "80\t\n" +
                        "81\t\n" +
                        "85\t\n" +
                        "86\t\n" +
                        "87\t\n" +
                        "88\t\n" +
                        "90\t\n" +
                        "91\t\n" +
                        "93\t\n" +
                        "95\t\n" +
                        "97\t\n" +
                        "98\t\n" +
                        "99\t\n" +
                        "100\t\n",
                bindVariableService -> {
                    bindVariableService.setStr(0, "OJSHR");
                    bindVariableService.setStr(1, null);
                    bindVariableService.setStr(2, "WFF");
                }
        ));

        assertSql("test where a in ($1,$2,$3)", tuples);
    }
}
