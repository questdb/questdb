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

package io.questdb.test.griffin.engine.functions;

import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVariableTestTuple;
import org.junit.Test;

public class InSymbolTest extends AbstractCairoTest {

    @Test
    public void testBindVarRhsWithConstantLhs() throws Exception {
        // Regression: when the IN function's LHS was fully constant but
        // the RHS held a deferred bind variable, Func.isConstant() used
        // to delegate to UnaryFunction's default (which only inspects
        // arg) and falsely report true. FunctionParser then folded the
        // function via getBool(null) before init() set testFunc, hitting
        // a NullPointerException at parse time.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL)");
            execute("INSERT INTO t VALUES ('A'), ('B'), ('C')");
            bindVariableService.clear();
            bindVariableService.setStr("b0", "Z");
            assertSql(
                    "s\n",
                    "SELECT s FROM t WHERE 'A'::SYMBOL IN ((:b0)::VARCHAR)"
            );
            bindVariableService.clear();
            bindVariableService.setStr("b0", "A");
            assertSql(
                    "s\nA\nB\nC\n",
                    "SELECT s FROM t WHERE 'A'::SYMBOL IN ((:b0)::VARCHAR) ORDER BY 1"
            );
        });
    }

    @Test
    public void testBindVarTypeChange2() throws SqlException {
        execute("create table test as (select x, rnd_symbol(20, 2, 5, 1) a from long_sequence(100))");

        // when more than one argument supplied, the function will match exact values from the list
        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "simple",
                """
                        x\ta
                        1\tGZS
                        29\tPDXYS
                        30\tGZS
                        32\tWFF
                        38\tPDXYS
                        57\tGZS
                        73\tPDXYS
                        84\tPDXYS
                        89\tGZS
                        """,
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
                """
                        x\ta
                        4\t
                        5\t
                        7\t
                        8\t
                        12\t
                        15\t
                        16\t
                        17\t
                        19\tOJSHR
                        20\t
                        21\t
                        23\t
                        24\t
                        26\tOJSHR
                        27\t
                        28\t
                        31\t
                        32\tWFF
                        34\t
                        35\t
                        36\t
                        40\t
                        41\t
                        42\t
                        44\t
                        45\t
                        46\t
                        49\t
                        50\t
                        52\t
                        53\t
                        56\t
                        58\t
                        60\t
                        61\t
                        62\t
                        63\t
                        65\t
                        67\t
                        71\t
                        72\t
                        74\t
                        78\tOJSHR
                        79\t
                        80\t
                        81\t
                        85\t
                        86\t
                        87\t
                        88\t
                        90\t
                        91\t
                        93\t
                        95\t
                        97\t
                        98\t
                        99\t
                        100\t
                        """,
                bindVariableService -> {
                    bindVariableService.setStr(0, "OJSHR");
                    bindVariableService.setStr(1, null);
                    bindVariableService.setStr(2, "WFF");
                }
        ));

        assertSql("test where a in ($1,$2,$3)", tuples);
    }

    @Test
    public void testCharNulInListMatchesNullSymbolRow() throws Exception {
        // Defensive: a CHAR(0) list element must be added to the set as null, mirroring
        // CastCharToSymbolFunctionFactory's CHAR(0) -> NULL mapping. Otherwise a NULL
        // symbol row would fail to match a CHAR(0) IN entry while it correctly matches
        // an explicit NULL entry, leaving the factory inconsistent with its own cast.
        // Covers the eager and deferred branches.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL)");
            execute("INSERT INTO t VALUES ('A'), (NULL), ('B')");
            assertSql(
                    "s\n\nA\n",
                    "SELECT s FROM t WHERE s IN ('A', (0)::CHAR) ORDER BY 1"
            );
            bindVariableService.clear();
            bindVariableService.setStr("b0", "0");
            assertSql(
                    "s\n\nA\n",
                    "SELECT s FROM t WHERE s IN ('A', (:b0::INT)::CHAR) ORDER BY 1"
            );
        });
    }

    @Test
    public void testBindVarTypedCastInList() throws Exception {
        // Regression: a bind variable wrapped in a non-STRING/VARCHAR cast
        // (e.g. ::SYMBOL or ::CHAR) inside the IN list used to fall through
        // to the SYMBOL/NULL/CHAR branches that read the value at compile
        // time, tripping NamedParameterLinkFunction.getBase()'s assertion
        // before the variable was bound. The deferred path now covers all
        // accepted types.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL)");
            execute("INSERT INTO t VALUES ('A'), ('B'), ('C')");
            bindVariableService.clear();
            bindVariableService.setStr("b0", "B");
            bindVariableService.setStr("b1", "C");
            assertSql(
                    "s\nA\nB\n",
                    "SELECT s FROM t WHERE s IN ('A', :b0::SYMBOL) ORDER BY 1"
            );
            assertSql(
                    "s\nA\nC\n",
                    "SELECT s FROM t WHERE s IN ('A', :b1::CHAR) ORDER BY 1"
            );
        });
    }
}
