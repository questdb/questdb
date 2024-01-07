/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin;

import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class InsertCastTest extends AbstractCairoTest {

    @Test
    public void testCastByteToCharBind() throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a char);");
            // execute insert statement for each value of reference table
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).getInsertOperation()
            ) {
                bindVariableService.setByte(0, (byte) 9);
                insert.execute(sqlExecutionContext);

                try {
                    bindVariableService.setByte(0, (byte) 33);
                    insert.execute(sqlExecutionContext);
                    Assert.fail();
                } catch (ImplicitCastException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
                }
            }
            assertSql(
                    "a\n" +
                            "9\n", "y"
            );
        });
    }

    @Test
    public void testCastCharByteFunc() throws Exception {
        assertCharFunc(
                "byte",
                "a\n" +
                        "5\n" +
                        "3\n" +
                        "0\n"
        );
    }

    @Test
    public void testCastCharByteTab() throws Exception {
        assertCharTab(
                "byte",
                "a\n" +
                        "5\n" +
                        "3\n" +
                        "0\n" +
                        "7\n" +
                        "0\n"
        );
    }

    @Test
    public void testCastCharDateFunc() throws Exception {
        assertCharFunc(
                "date",
                "a\n" +
                        "1970-01-01T00:00:00.005Z\n" +
                        "1970-01-01T00:00:00.003Z\n" +
                        "1970-01-01T00:00:00.000Z\n"
        );
    }

    @Test
    public void testCastCharDateTab() throws Exception {
        assertCharTab(
                "date",
                "a\n" +
                        "1970-01-01T00:00:00.005Z\n" +
                        "1970-01-01T00:00:00.003Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.007Z\n" +
                        "1970-01-01T00:00:00.000Z\n"
        );
    }

    @Test
    public void testCastCharDoubleFunc() throws Exception {
        assertCharFunc(
                "double",
                "a\n" +
                        "5.0\n" +
                        "3.0\n" +
                        "0.0\n"
        );
    }

    @Test
    public void testCastCharDoubleTab() throws Exception {
        assertCharTab(
                "double",
                "a\n" +
                        "5.0\n" +
                        "3.0\n" +
                        "0.0\n" +
                        "7.0\n" +
                        "0.0\n"
        );
    }

    @Test
    public void testCastCharFloatFunc() throws Exception {
        assertCharFunc(
                "float",
                "a\n" +
                        "5.0000\n" +
                        "3.0000\n" +
                        "0.0000\n"
        );
    }

    @Test
    public void testCastCharFloatTab() throws Exception {
        assertCharTab(
                "float",
                "a\n" +
                        "5.0000\n" +
                        "3.0000\n" +
                        "0.0000\n" +
                        "7.0000\n" +
                        "0.0000\n"
        );
    }

    @Test
    public void testCastCharGeoByteTab() throws Exception {
        assertCharTab(
                "geohash(1c)",
                "a\n" +
                        "5\n" +
                        "3\n" +
                        "0\n" +
                        "7\n" +
                        "0\n"
        );
    }

    @Test
    public void testCastCharIntFunc() throws Exception {
        assertCharFunc(
                "int",
                "a\n" +
                        "5\n" +
                        "3\n" +
                        "0\n"
        );
    }

    @Test
    public void testCastCharIntTab() throws Exception {
        assertCharTab(
                "int",
                "a\n" +
                        "5\n" +
                        "3\n" +
                        "0\n" +
                        "7\n" +
                        "0\n"
        );
    }

    @Test
    public void testCastCharLongFunc() throws Exception {
        assertCharFunc(
                "long",
                "a\n" +
                        "5\n" +
                        "3\n" +
                        "0\n"
        );
    }

    @Test
    public void testCastCharLongTab() throws Exception {
        assertCharTab(
                "long",
                "a\n" +
                        "5\n" +
                        "3\n" +
                        "0\n" +
                        "7\n" +
                        "0\n"
        );
    }

    @Test
    public void testCastCharShortFunc() throws Exception {
        assertCharFunc(
                "short",
                "a\n" +
                        "5\n" +
                        "3\n" +
                        "0\n"
        );
    }

    @Test
    public void testCastCharShortTab() throws Exception {
        assertCharTab(
                "short",
                "a\n" +
                        "5\n" +
                        "3\n" +
                        "0\n" +
                        "7\n" +
                        "0\n"
        );
    }

    @Test
    public void testCastCharTimestampFunc() throws Exception {
        assertCharFunc(
                "timestamp",
                "a\n" +
                        "1970-01-01T00:00:00.000005Z\n" +
                        "1970-01-01T00:00:00.000003Z\n" +
                        "1970-01-01T00:00:00.000000Z\n"
        );
    }

    @Test
    public void testCastCharTimestampTab() throws Exception {
        assertCharTab(
                "timestamp",
                "a\n" +
                        "1970-01-01T00:00:00.000005Z\n" +
                        "1970-01-01T00:00:00.000003Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000007Z\n" +
                        "1970-01-01T00:00:00.000000Z\n"
        );
    }

    @Test
    public void testCastCharToByteBind() throws Exception {
        assertCharBind(
                "byte",
                "a\n" +
                        "0\n" +
                        "3\n"
        );
    }

    @Test
    public void testCastCharToByteLit() throws Exception {
        assertCharLit(
                "byte",
                "a\n" +
                        "4\n" +
                        "7\n" +
                        "1\n"
        );
    }

    @Test
    public void testCastCharToDateBind() throws Exception {
        // this is internal widening cast
        assertCharBind(
                "date",
                "a\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.003Z\n"
        );
    }

    @Test
    public void testCastCharToDateLit() throws Exception {
        assertCharLit(
                "date",
                "a\n" +
                        "1970-01-01T00:00:00.004Z\n" +
                        "1970-01-01T00:00:00.007Z\n" +
                        "1970-01-01T00:00:00.001Z\n"
        );
    }

    @Test
    public void testCastCharToDoubleBind() throws Exception {
        assertCharBind(
                "double",
                "a\n" +
                        "0.0\n" +
                        "3.0\n"
        );
    }

    @Test
    public void testCastCharToDoubleLit() throws Exception {
        assertCharLit(
                "double",
                "a\n" +
                        "4.0\n" +
                        "7.0\n" +
                        "1.0\n"
        );
    }

    @Test
    public void testCastCharToFloatBind() throws Exception {
        assertCharBind(
                "float",
                "a\n" +
                        "0.0000\n" +
                        "3.0000\n"
        );
    }

    @Test
    public void testCastCharToFloatLit() throws Exception {
        assertCharLit(
                "float",
                "a\n" +
                        "4.0000\n" +
                        "7.0000\n" +
                        "1.0000\n"
        );
    }

    @Test
    public void testCastCharToGeoByteBind() throws Exception {
        assertCharBind(
                "geohash(1c)",
                "a\n" +
                        "0\n" +
                        "3\n"
        );
    }

    @Test
    public void testCastCharToGeoByteLit() throws Exception {
        assertCharLit(
                "geohash(1c)",
                "a\n" +
                        "4\n" +
                        "7\n" +
                        "1\n"
        );
    }

    @Test
    public void testCastCharToIntBind() throws Exception {
        assertCharBind(
                "int",
                "a\n" +
                        "0\n" +
                        "3\n"
        );
    }

    @Test
    public void testCastCharToIntLit() throws Exception {
        assertCharLit(
                "int",
                "a\n" +
                        "4\n" +
                        "7\n" +
                        "1\n"
        );
    }

    @Test
    public void testCastCharToLong256Bind() throws Exception {
        try {
            assertCharBind(
                    "long256",
                    "a\n" +
                            "0\n" +
                            "3\n"
            );
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 0 is defined as LONG256 and cannot accept CHAR");
        }
    }

    @Test
    public void testCastCharToLongBind() throws Exception {
        assertCharBind(
                "long",
                "a\n" +
                        "0\n" +
                        "3\n"
        );
    }

    @Test
    public void testCastCharToLongLit() throws Exception {
        assertCharLit(
                "long",
                "a\n" +
                        "4\n" +
                        "7\n" +
                        "1\n"
        );
    }

    @Test
    public void testCastCharToShortBind() throws Exception {
        assertCharBind(
                "short",
                "a\n" +
                        "0\n" +
                        "3\n"
        );
    }

    @Test
    public void testCastCharToShortLit() throws Exception {
        assertCharLit(
                "short",
                "a\n" +
                        "4\n" +
                        "7\n" +
                        "1\n"
        );
    }

    @Test
    public void testCastCharToTimestampBind() throws Exception {
        // this is internal widening cast
        assertCharBind(
                "timestamp",
                "a\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000003Z\n"
        );
    }

    @Test
    public void testCastCharToTimestampLit() throws Exception {
        assertCharLit(
                "timestamp",
                "a\n" +
                        "1970-01-01T00:00:00.000004Z\n" +
                        "1970-01-01T00:00:00.000007Z\n" +
                        "1970-01-01T00:00:00.000001Z\n"
        );
    }

    @Test
    public void testCastDoubleToFloatBind() throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a float);");
            // execute insert statement for each value of reference table
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).getInsertOperation()
            ) {
                bindVariableService.setDouble(0, 1.7e25);
                insert.execute(sqlExecutionContext);

                bindVariableService.setDouble(0, Double.NaN);
                insert.execute(sqlExecutionContext);

                try {
                    bindVariableService.setDouble(0, 4.5E198); // overflow
                    Assert.fail();
                } catch (ImplicitCastException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
                }
            }
            assertSql(
                    "a\n" +
                            "1.7E25\n" +
                            "NaN\n", "y"
            );
        });
    }

    @Test
    public void testCastFloatByteTab() throws Exception {
        assertCastFloatTab("byte",
                "a\n" +
                        "28\n" +
                        "29\n" +
                        "8\n" +
                        "20\n" +
                        "93\n" +
                        "0\n" +
                        "66\n" +
                        "80\n" +
                        "22\n" +
                        "12\n" +
                        "8\n",
                -210f,
                220f
        );
    }

    @Test
    public void testCastFloatIntTab() throws Exception {
        assertCastFloatTab("int",
                "a\n" +
                        "28\n" +
                        "29\n" +
                        "8\n" +
                        "20\n" +
                        "93\n" +
                        "NaN\n" +
                        "66\n" +
                        "80\n" +
                        "22\n" +
                        "12\n" +
                        "8\n",
                -3.4e20f,
                3.4e20f
        );
    }

    @Test
    public void testCastFloatLongTab() throws Exception {
        assertCastFloatTab("long",
                "a\n" +
                        "28\n" +
                        "29\n" +
                        "8\n" +
                        "20\n" +
                        "93\n" +
                        "NaN\n" +
                        "66\n" +
                        "80\n" +
                        "22\n" +
                        "12\n" +
                        "8\n",
                -3.4e35f,
                3.4e35f
        );
    }

    @Test
    public void testCastFloatShortTab() throws Exception {
        assertCastFloatTab("short",
                "a\n" +
                        "28\n" +
                        "29\n" +
                        "8\n" +
                        "20\n" +
                        "93\n" +
                        "0\n" +
                        "66\n" +
                        "80\n" +
                        "22\n" +
                        "12\n" +
                        "8\n",
                -42230f,
                42230f
        );
    }

    @Test
    public void testCastIntToByteBind() throws Exception {
        assertIntBind(
                "byte",
                "a\n" +
                        "3\n" +
                        "0\n"
        );
    }

    @Test
    public void testCastIntToCharBind() throws Exception {
        assertIntBind(
                "char",
                "a\n" +
                        "3\n" +
                        "\n"
        );
    }

    @Test
    public void testCastIntToLong256Bind() throws Exception {
        try {
            assertIntBind(
                    "long256",
                    "a\n" +
                            "3\n" +
                            "0\n"
            );
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 0 is defined as LONG256 and cannot accept INT");
        }
    }

    @Test
    public void testCastIntToShortBind() throws Exception {
        assertIntBind(
                "short",
                "a\n" +
                        "3\n" +
                        "0\n"
        );
    }

    @Test
    public void testCastLongToByteBind() throws Exception {
        assertLongBind(
                "byte",
                "a\n" +
                        "8\n" +
                        "0\n"
        );
    }

    @Test
    public void testCastLongToCharBind() throws Exception {
        assertLongBind(
                "char",
                "a\n" +
                        "8\n" +
                        "\n"
        );
    }

    @Test
    public void testCastLongToLong256Bind() throws Exception {
        try {
            assertLongBind(
                    "long256",
                    "a\n" +
                            "3\n" +
                            "0\n"
            );
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 0 is defined as LONG256 and cannot accept LONG");
        }
    }

    @Test
    public void testCastLongToShortBind() throws Exception {
        assertLongBind(
                "short",
                "a\n" +
                        "8\n" +
                        "0\n"
        );
    }

    @Test
    public void testCastShortToByteBind() throws Exception {
        assertShortBind("byte");
    }

    @Test
    public void testCastShortToCharBind() throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a char);");
            // execute insert statement for each value of reference table
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).getInsertOperation()
            ) {
                bindVariableService.setShort(0, (short) 2);
                insert.execute(sqlExecutionContext);

                bindVariableService.setShort(0, (short) 8);
                insert.execute(sqlExecutionContext);

                try {
                    bindVariableService.setShort(0, (short) 210); // overflow
                    Assert.fail();
                } catch (ImplicitCastException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
                }
            }
            assertSql(
                    "a\n" +
                            "2\n" +
                            "8\n", "y"
            );
        });
    }

    @Test
    public void testCastShortToLong256Bind() throws Exception {
        try {
            assertShortBind("long256");
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 0 is defined as LONG256 and cannot accept SHORT");
        }
    }

    @Test
    public void testCastStrByteTab() throws Exception {
        assertStrTab(
                "byte",
                "a\tb\n" +
                        "76\t76\n" +
                        "102\t102\n" +
                        "27\t27\n" +
                        "87\t87\n" +
                        "79\t79\n"
        );
    }

    @Test
    public void testCastStrCharTab() throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a char, b string);");
            ddl("create table x as (select cast(rnd_byte()%10 as string) a from long_sequence(5));");
            // execute insert statement for each value of reference table
            insert("insert into y select a,a from x");
            assertSql(
                    "a\tb\n" +
                            "6\t6\n" +
                            "2\t2\n" +
                            "7\t7\n" +
                            "7\t7\n" +
                            "9\t9\n", "y"
            );
        });
    }

    @Test
    public void testCastStrDateTab() throws Exception {
        assertStrTab(
                "date",
                "a\tb\n" +
                        "1970-01-01T00:00:00.076Z\t76\n" +
                        "1970-01-01T00:00:00.102Z\t102\n" +
                        "1970-01-01T00:00:00.027Z\t27\n" +
                        "1970-01-01T00:00:00.087Z\t87\n" +
                        "1970-01-01T00:00:00.079Z\t79\n"
        );
    }

    @Test
    public void testCastStrDoubleTab() throws Exception {
        assertStrTab(
                "double",
                "a\tb\n" +
                        "76.0\t76\n" +
                        "102.0\t102\n" +
                        "27.0\t27\n" +
                        "87.0\t87\n" +
                        "79.0\t79\n"
        );
    }

    @Test
    public void testCastStrFloatTab() throws Exception {
        assertStrTab(
                "float",
                "a\tb\n" +
                        "76.0000\t76\n" +
                        "102.0000\t102\n" +
                        "27.0000\t27\n" +
                        "87.0000\t87\n" +
                        "79.0000\t79\n"
        );
    }

    @Test
    public void testCastStrIntTab() throws Exception {
        assertStrTab(
                "int",
                "a\tb\n" +
                        "76\t76\n" +
                        "102\t102\n" +
                        "27\t27\n" +
                        "87\t87\n" +
                        "79\t79\n"
        );
    }

    @Test
    public void testCastStrLongTab() throws Exception {
        assertStrTab(
                "long",
                "a\tb\n" +
                        "76\t76\n" +
                        "102\t102\n" +
                        "27\t27\n" +
                        "87\t87\n" +
                        "79\t79\n"
        );
    }

    @Test
    public void testCastStrShortTab() throws Exception {
        assertStrTab(
                "short",
                "a\tb\n" +
                        "76\t76\n" +
                        "102\t102\n" +
                        "27\t27\n" +
                        "87\t87\n" +
                        "79\t79\n"
        );
    }

    @Test
    public void testCastStrTimestampTab() throws Exception {
        assertStrTab(
                "timestamp",
                "a\tb\n" +
                        "1970-01-01T00:00:00.000076Z\t76\n" +
                        "1970-01-01T00:00:00.000102Z\t102\n" +
                        "1970-01-01T00:00:00.000027Z\t27\n" +
                        "1970-01-01T00:00:00.000087Z\t87\n" +
                        "1970-01-01T00:00:00.000079Z\t79\n"
        );
    }

    @Test
    public void testCastStrToByteBind() throws Exception {
        assertStrBind(
                "byte",
                "a\n" +
                        "12\n" +
                        "31\n" +
                        "0\n"
        );
    }

    @Test
    public void testCastStrToByteLit() throws Exception {
        assertStrLit(
                "byte",
                "a\n" +
                        "45\n" +
                        "76\n" +
                        "124\n"
        );
    }

    @Test
    public void testCastStrToCharLit() throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a char);");
            // execute insert statement for each value of reference table
            insert("insert into y values (cast('A' as string))");
            insert("insert into y values (cast('7' as string))");
            try {
                insert("insert into y values ('cc')");
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
            insert("insert into y values (cast('K' as string))");
            assertSql(
                    "a\n" +
                            "A\n" +
                            "7\n" +
                            "K\n", "y"
            );
        });
    }

    @Test
    public void testCastStrToDateBind() throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a date);");
            // execute insert statement for each value of reference table
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).getInsertOperation()
            ) {
                bindVariableService.setStr(0, "2012-04-11 10:45:11Z");
                insert.execute(sqlExecutionContext);

                bindVariableService.setStr(0, "2012-04-11 10:45:11.344Z");
                insert.execute(sqlExecutionContext);

                bindVariableService.setStr(0, "2012-04-11 Z");
                insert.execute(sqlExecutionContext);

                bindVariableService.setStr(0, "2013-05-12");
                insert.execute(sqlExecutionContext);

                bindVariableService.setStr(0, null);
                insert.execute(sqlExecutionContext);

                try {
                    bindVariableService.setStr(0, "iabc");
                    insert.execute(sqlExecutionContext);
                    Assert.fail();
                } catch (ImplicitCastException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
                }

            }
            assertSql(
                    "a\n" +
                            "2012-04-11T10:45:11.000Z\n" +
                            "2012-04-11T10:45:11.344Z\n" +
                            "2012-04-11T00:00:00.000Z\n" +
                            "2013-05-12T00:00:00.000Z\n" +
                            "\n", "y"
            );
        });
    }

    @Test
    public void testCastStrToDateLit() throws Exception {
        assertStrLit(
                "date",
                "a\n" +
                        "1970-01-01T00:00:00.045Z\n" +
                        "1970-01-01T00:00:00.076Z\n" +
                        "1970-01-01T00:00:00.124Z\n"
        );
    }

    @Test
    public void testCastStrToDateLitAsDate() throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a date);");
            // execute insert statement for each value of reference table
            insert("insert into y values ('2022-01-01T00:00:00.045Z');");
            insert("insert into y values ('2022-01-01T00:00:00.076Z');");
            try {
                insert("insert into y values ('c')");
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
            insert("insert into y values ('2222-01-01T00:00:00.124Z');");
            assertSql(
                    "a\n" +
                            "2022-01-01T00:00:00.045Z\n" +
                            "2022-01-01T00:00:00.076Z\n" +
                            "2222-01-01T00:00:00.124Z\n", "y"
            );
        });
    }

    @Test
    public void testCastStrToDoubleBind() throws Exception {
        assertStrBind(
                "double",
                "a\n" +
                        "12.0\n" +
                        "31.0\n" +
                        "NaN\n"
        );
    }

    @Test
    public void testCastStrToDoubleLit() throws Exception {
        assertStrLit(
                "double",
                "a\n" +
                        "45.0\n" +
                        "76.0\n" +
                        "124.0\n"
        );
    }

    @Test
    public void testCastStrToFloatBind() throws Exception {
        assertStrBind(
                "float",
                "a\n" +
                        "12.0000\n" +
                        "31.0000\n" +
                        "NaN\n"
        );
    }

    @Test
    public void testCastStrToFloatLit() throws Exception {
        assertStrLit(
                "float",
                "a\n" +
                        "45.0000\n" +
                        "76.0000\n" +
                        "124.0000\n"
        );
    }

    @Test
    public void testCastStrToIntBind() throws Exception {
        assertStrBind(
                "int",
                "a\n" +
                        "12\n" +
                        "31\n" +
                        "NaN\n"
        );
    }

    @Test
    public void testCastStrToIntLit() throws Exception {
        assertStrLit(
                "int",
                "a\n" +
                        "45\n" +
                        "76\n" +
                        "124\n"
        );
    }

    @Test
    public void testCastStrToLong256Bind() throws Exception {
        assertStrBind(
                "long256",
                "a\n" +
                        "0x12\n" +
                        "0x31\n" +
                        "\n"
        );
    }

    @Test
    public void testCastStrToLongBind() throws Exception {
        assertStrBind(
                "long",
                "a\n" +
                        "12\n" +
                        "31\n" +
                        "NaN\n"
        );
    }

    @Test
    public void testCastStrToLongLit() throws Exception {
        assertStrLit(
                "long",
                "a\n" +
                        "45\n" +
                        "76\n" +
                        "124\n"
        );
    }

    @Test
    public void testCastStrToShortBind() throws Exception {
        assertStrBind(
                "short",
                "a\n" +
                        "12\n" +
                        "31\n" +
                        "0\n"
        );
    }

    @Test
    public void testCastStrToShortLit() throws Exception {
        assertStrLit(
                "short",
                "a\n" +
                        "45\n" +
                        "76\n" +
                        "124\n"
        );
    }

    @Test
    public void testCastStrToTimestampBind() throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a timestamp);");
            // execute insert statement for each value of reference table
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).getInsertOperation()
            ) {
                bindVariableService.setStr(0, "2012-04-11T10:45:11");
                insert.execute(sqlExecutionContext);

                bindVariableService.setStr(0, "2012-04-11T10:45:11.344999");
                insert.execute(sqlExecutionContext);

                bindVariableService.setStr(0, null);
                insert.execute(sqlExecutionContext);

                try {
                    bindVariableService.setStr(0, "iabc");
                    insert.execute(sqlExecutionContext);
                    Assert.fail();
                } catch (ImplicitCastException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
                }
            }
            assertSql(
                    "a\n" +
                            "2012-04-11T10:45:11.000000Z\n" +
                            "2012-04-11T10:45:11.344999Z\n" +
                            "\n", "y"
            );
        });
    }

    @Test
    public void testCastStrToTimestampLit() throws Exception {
        assertStrLit(
                "timestamp",
                "a\n" +
                        "1970-01-01T00:00:00.000045Z\n" +
                        "1970-01-01T00:00:00.000076Z\n" +
                        "1970-01-01T00:00:00.000124Z\n"
        );
    }

    @Test
    public void testCastStrToTimestampLitAsTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a " + "timestamp" + ");");
            // execute insert statement for each value of reference table
            insert("insert into y values ('2022-01-01T00:00:00.000045Z');");
            insert("insert into y values ('2222-01-01T00:00:00.000076Z');");
            try {
                insert("insert into y values ('c')");
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
            insert("insert into y values ('2222-01-01T00:00:00.000124Z');");
            assertSql(
                    "a\n" +
                            "2022-01-01T00:00:00.000045Z\n" +
                            "2222-01-01T00:00:00.000076Z\n" +
                            "2222-01-01T00:00:00.000124Z\n", "y"
            );
        });
    }

    @Test
    public void testCastTimestampToByteBind() throws Exception {
        assertTimestampBind(
                "byte",
                "a\n" +
                        "8\n" +
                        "0\n"
        );
    }

    @Test
    public void testCastTimestampToDoubleBind() throws Exception {
        assertTimestampBindNoOverflow(
                "double",
                "a\n" +
                        "8.0\n" +
                        "NaN\n" +
                        "8.8990229990007E13\n"
        );
    }

    @Test
    public void testCastTimestampToFloatBind() throws Exception {
        assertTimestampBindNoOverflow(
                "float",
                "a\n" +
                        "8.0000\n" +
                        "NaN\n" +
                        "8.8990229E13\n"
        );
    }

    @Test
    public void testCastTimestampToIntBind() throws Exception {
        assertTimestampBind(
                "int",
                "a\n" +
                        "8\n" +
                        "NaN\n"
        );
    }

    @Test
    public void testCastTimestampToLong256Bind() throws Exception {
        try {
            assertTimestampBindNoOverflow(
                    "long256",
                    "a\n" +
                            "1970-01-01T00:00:00.000008Z\n" +
                            "\n" +
                            "1972-10-26T23:30:29.990007Z\n"
            );
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 0 is defined as LONG256 and cannot accept TIMESTAMP");
        }
    }

    @Test
    public void testCastTimestampToLongBind() throws Exception {
        assertTimestampBindNoOverflow(
                "long",
                "a\n" +
                        "8\n" +
                        "NaN\n" +
                        "88990229990007\n"
        );
    }

    @Test
    public void testCastTimestampToShortBind() throws Exception {
        assertTimestampBind(
                "short",
                "a\n" +
                        "8\n" +
                        "0\n"
        );
    }

    @Test
    public void testCastTimestampToStringBind() throws Exception {
        assertTimestampBindNoOverflow(
                "string",
                "a\n" +
                        "1970-01-01T00:00:00.000008Z\n" +
                        "\n" +
                        "1972-10-26T23:30:29.990007Z\n"
        );
    }

    @Test
    public void testInsertNullDateIntoTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x(ts timestamp)");
            insert("insert into x values (cast(null as date))");
            assertSql(
                    "ts\n" +
                            "\n", "x"
            );
        });
    }

    private void assertCastFloatTab(String type, String expected, float outOfRangeLeft, float outOfRangeRight) throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a " + type + ");");
            ddl("create table x as (select rnd_float()*100 a from long_sequence(5));");
            ddl("insert into y select rnd_float()*100 a from long_sequence(5);");
            insert("insert into y values (cast ('NaN' as float));");
            // execute insert statement for each value of reference table
            ddl("insert into y select a from x");

            try {
                insert("insert into y values (cast ('" + outOfRangeLeft + "' as float));");
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }

            try {
                insert("insert into y values (cast ('" + outOfRangeRight + "' as float));");
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }

            assertSql(
                    expected, "y"
            );
        });
    }

    private void assertCharBind(String toType, String expected) throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a " + toType + ");");
            // execute insert statement for each value of reference table
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).getInsertOperation()
            ) {
                bindVariableService.setChar(0, '0');
                insert.execute(sqlExecutionContext);

                bindVariableService.setChar(0, '3');
                insert.execute(sqlExecutionContext);

                try {
                    bindVariableService.setChar(0, 'a');
                    insert.execute(sqlExecutionContext);
                    Assert.fail();
                } catch (ImplicitCastException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
                }
            }
            assertSql(expected, "y");
        });
    }

    private void assertCharFunc(String toType, String expected) throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a " + toType + ");");
            // execute insert statement for each value of reference table
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    InsertOperation insert = compiler.compile(
                            "insert into y values (cast(rnd_int(0, 10, 0) + 47 as char))",
                            sqlExecutionContext
                    ).getInsertOperation()
            ) {
                insert.execute(sqlExecutionContext);
                insert.execute(sqlExecutionContext);
                insert.execute(sqlExecutionContext);
            }
            assertSql(expected, "y");
        });
    }

    private void assertCharLit(String toType, String expected) throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a " + toType + ");");
            // execute insert statement for each value of reference table
            insert("insert into y values ('4')");
            insert("insert into y values ('7')");
            try {
                // 'a' is an invalid geohash and also invalid number
                insert("insert into y values ('a')");
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
            insert("insert into y values ('1')");
            assertSql(expected, "y");
        });
    }

    private void assertCharTab(String toType, String expected) throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a " + toType + ");");
            ddl("create table x as (select cast(rnd_int(0,10,0)+47 as char) a from long_sequence(5));");
            // execute insert statement for each value of reference table
            ddl("insert into y select a from x");
            assertSql(expected, "y");
        });
    }

    private void assertIntBind(String type, String expected) throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a " + type + ");");
            // execute insert statement for each value of reference table
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).getInsertOperation()
            ) {
                bindVariableService.setInt(0, 3); // compatible with everything
                insert.execute(sqlExecutionContext);

                bindVariableService.setInt(0, Numbers.INT_NaN);
                insert.execute(sqlExecutionContext);

                try {
                    bindVariableService.setInt(0, 88990227); // overflow
                    Assert.fail();
                } catch (ImplicitCastException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
                }
            }
            assertSql(expected, "y");
        });
    }

    private void assertLongBind(String type, String expected) throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a " + type + ");");
            // execute insert statement for each value of reference table
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).getInsertOperation()
            ) {
                bindVariableService.setLong(0, 8); // compatible with everything
                insert.execute(sqlExecutionContext);

                bindVariableService.setLong(0, Numbers.LONG_NaN);
                insert.execute(sqlExecutionContext);

                try {
                    bindVariableService.setLong(0, 88990229990007L); // overflow
                    Assert.fail();
                } catch (ImplicitCastException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
                }
            }
            assertSql(expected, "y");
        });
    }

    private void assertShortBind(String type) throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a " + type + ");");
            // execute insert statement for each value of reference table
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).getInsertOperation()
            ) {
                bindVariableService.setShort(0, (short) 12);
                insert.execute(sqlExecutionContext);

                bindVariableService.setShort(0, (short) 31);
                insert.execute(sqlExecutionContext);

                try {
                    bindVariableService.setShort(0, (short) 210); // overflow
                    Assert.fail();
                } catch (ImplicitCastException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
                }
            }
            assertSql(
                    "a\n" +
                            "12\n" +
                            "31\n", "y"
            );
        });
    }

    private void assertStrBind(String toType, String expected) throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a " + toType + ");");
            // execute insert statement for each value of reference table
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).getInsertOperation()
            ) {
                bindVariableService.setStr(0, "12");
                insert.execute(sqlExecutionContext);

                bindVariableService.setStr(0, "31");
                insert.execute(sqlExecutionContext);

                bindVariableService.setStr(0, null);
                insert.execute(sqlExecutionContext);

                try {
                    bindVariableService.setStr(0, "iabc");
                    insert.execute(sqlExecutionContext);
                    Assert.fail();
                } catch (ImplicitCastException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
                }
            }
            assertSql(expected, "y");
        });
    }

    private void assertStrLit(String toType, String expected) throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a " + toType + ");");
            // execute insert statement for each value of reference table
            insert("insert into y values ('45')");
            insert("insert into y values ('76')");
            try {
                insert("insert into y values ('cc')");
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
            insert("insert into y values ('124')");
            assertSql(expected, "y");
        });
    }

    private void assertStrTab(String toType, String expected) throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a " + toType + ", b string);");
            ddl("create table x as (select cast(rnd_byte() as string) a from long_sequence(5));");
            // execute insert statement for each value of reference table
            ddl("insert into y select a,a from x");
            assertSql(expected, "y");
        });
    }

    private void assertTimestampBind(String type, String expected) throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a " + type + ");");
            // execute insert statement for each value of reference table
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).getInsertOperation()
            ) {
                bindVariableService.setTimestamp(0, 8); // compatible with everything
                insert.execute(sqlExecutionContext);

                bindVariableService.setTimestamp(0, Numbers.LONG_NaN);
                insert.execute(sqlExecutionContext);

                try {
                    bindVariableService.setTimestamp(0, 88990229990007L); // overflow
                    Assert.fail();
                } catch (ImplicitCastException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
                }
            }
            assertSql(expected, "y");
        });
    }

    private void assertTimestampBindNoOverflow(String type, String expected) throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            ddl("create table y(a " + type + ");");
            // execute insert statement for each value of reference table
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).getInsertOperation()
            ) {
                bindVariableService.setTimestamp(0, 8); // compatible with everything
                insert.execute(sqlExecutionContext);

                bindVariableService.setTimestamp(0, Numbers.LONG_NaN);
                insert.execute(sqlExecutionContext);

                bindVariableService.setTimestamp(0, 88990229990007L);
                insert.execute(sqlExecutionContext);
            }
            assertSql(expected, "y");
        });
    }
}
