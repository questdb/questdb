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

package io.questdb.test.griffin;

import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class InsertCastTest extends AbstractCairoTest {

    @Test
    public void testCastByteToCharBind() throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            execute("create table y(a char);");
            // execute insert statement for each value of reference table
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).popInsertOperation()
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
                            "9\n",
                    "y"
            );
        });
    }

    @Test
    public void testCastCharByteFunc() throws Exception {
        assertMemoryLeak(() -> assertCharFunc(
                "byte",
                "a\n" +
                        "5\n" +
                        "3\n" +
                        "0\n"
        ));
    }

    @Test
    public void testCastCharByteTab() throws Exception {
        assertMemoryLeak(() -> assertCharTab(
                "byte",
                "a\n" +
                        "5\n" +
                        "3\n" +
                        "0\n" +
                        "7\n" +
                        "0\n"
        ));
    }

    @Test
    public void testCastCharDateFunc() throws Exception {
        assertMemoryLeak(() -> assertCharFunc(
                "date",
                "a\n" +
                        "1970-01-01T00:00:00.005Z\n" +
                        "1970-01-01T00:00:00.003Z\n" +
                        "1970-01-01T00:00:00.000Z\n"
        ));
    }

    @Test
    public void testCastCharDateTab() throws Exception {
        assertMemoryLeak(() -> assertCharTab(
                "date",
                "a\n" +
                        "1970-01-01T00:00:00.005Z\n" +
                        "1970-01-01T00:00:00.003Z\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.007Z\n" +
                        "1970-01-01T00:00:00.000Z\n"
        ));
    }

    @Test
    public void testCastCharDoubleFunc() throws Exception {
        assertMemoryLeak(() -> assertCharFunc(
                "double",
                "a\n" +
                        "5.0\n" +
                        "3.0\n" +
                        "0.0\n"
        ));
    }

    @Test
    public void testCastCharDoubleTab() throws Exception {
        assertMemoryLeak(() -> assertCharTab(
                "double",
                "a\n" +
                        "5.0\n" +
                        "3.0\n" +
                        "0.0\n" +
                        "7.0\n" +
                        "0.0\n"
        ));
    }

    @Test
    public void testCastCharFloatFunc() throws Exception {
        assertMemoryLeak(() -> assertCharFunc(
                "float",
                "a\n" +
                        "5.0\n" +
                        "3.0\n" +
                        "0.0\n"
        ));
    }

    @Test
    public void testCastCharFloatTab() throws Exception {
        assertMemoryLeak(() -> assertCharTab(
                "float",
                "a\n" +
                        "5.0\n" +
                        "3.0\n" +
                        "0.0\n" +
                        "7.0\n" +
                        "0.0\n"
        ));
    }

    @Test
    public void testCastCharGeoByteTab() throws Exception {
        assertMemoryLeak(() -> assertCharTab(
                "geohash(1c)",
                "a\n" +
                        "5\n" +
                        "3\n" +
                        "0\n" +
                        "7\n" +
                        "0\n"
        ));
    }

    @Test
    public void testCastCharIntFunc() throws Exception {
        assertMemoryLeak(() -> assertCharFunc(
                "int",
                "a\n" +
                        "5\n" +
                        "3\n" +
                        "0\n"
        ));
    }

    @Test
    public void testCastCharIntTab() throws Exception {
        assertMemoryLeak(() -> assertCharTab(
                "int",
                "a\n" +
                        "5\n" +
                        "3\n" +
                        "0\n" +
                        "7\n" +
                        "0\n"
        ));
    }

    @Test
    public void testCastCharLongFunc() throws Exception {
        assertMemoryLeak(() -> assertCharFunc(
                "long",
                "a\n" +
                        "5\n" +
                        "3\n" +
                        "0\n"
        ));
    }

    @Test
    public void testCastCharLongTab() throws Exception {
        assertMemoryLeak(() -> assertCharTab(
                "long",
                "a\n" +
                        "5\n" +
                        "3\n" +
                        "0\n" +
                        "7\n" +
                        "0\n"
        ));
    }

    @Test
    public void testCastCharShortFunc() throws Exception {
        assertMemoryLeak(() -> assertCharFunc(
                "short",
                "a\n" +
                        "5\n" +
                        "3\n" +
                        "0\n"
        ));
    }

    @Test
    public void testCastCharShortTab() throws Exception {
        assertMemoryLeak(() -> assertCharTab(
                "short",
                "a\n" +
                        "5\n" +
                        "3\n" +
                        "0\n" +
                        "7\n" +
                        "0\n"
        ));
    }

    @Test
    public void testCastCharTimestampFunc() throws Exception {
        assertMemoryLeak(() -> assertCharFunc(
                "timestamp",
                "a\n" +
                        "1970-01-01T00:00:00.000005Z\n" +
                        "1970-01-01T00:00:00.000003Z\n" +
                        "1970-01-01T00:00:00.000000Z\n"
        ));
    }

    @Test
    public void testCastCharTimestampNSFunc() throws Exception {
        assertMemoryLeak(() -> assertCharFunc(
                "timestamp_ns",
                "a\n" +
                        "1970-01-01T00:00:00.000000005Z\n" +
                        "1970-01-01T00:00:00.000000003Z\n" +
                        "1970-01-01T00:00:00.000000000Z\n"
        ));
    }

    @Test
    public void testCastCharTimestampNSTab() throws Exception {
        assertMemoryLeak(() -> assertCharTab(
                "timestamp_ns",
                "a\n" +
                        "1970-01-01T00:00:00.000000005Z\n" +
                        "1970-01-01T00:00:00.000000003Z\n" +
                        "1970-01-01T00:00:00.000000000Z\n" +
                        "1970-01-01T00:00:00.000000007Z\n" +
                        "1970-01-01T00:00:00.000000000Z\n"
        ));
    }

    @Test
    public void testCastCharTimestampTab() throws Exception {
        assertMemoryLeak(() -> assertCharTab(
                "timestamp",
                "a\n" +
                        "1970-01-01T00:00:00.000005Z\n" +
                        "1970-01-01T00:00:00.000003Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000007Z\n" +
                        "1970-01-01T00:00:00.000000Z\n"
        ));
    }

    @Test
    public void testCastCharToByteBind() throws Exception {
        assertMemoryLeak(() -> assertCharBind(
                "byte",
                "a\n" +
                        "0\n" +
                        "3\n"
        ));
    }

    @Test
    public void testCastCharToByteLit() throws Exception {
        assertMemoryLeak(() -> assertCharLit(
                "byte",
                "a\n" +
                        "4\n" +
                        "7\n" +
                        "1\n"
        ));
    }

    @Test
    public void testCastCharToDateBind() throws Exception {
        // this is internal widening cast
        assertMemoryLeak(() -> assertCharBind(
                "date",
                "a\n" +
                        "1970-01-01T00:00:00.000Z\n" +
                        "1970-01-01T00:00:00.003Z\n"
        ));
    }

    @Test
    public void testCastCharToDateLit() throws Exception {
        assertMemoryLeak(() -> assertCharLit(
                "date",
                "a\n" +
                        "1970-01-01T00:00:00.004Z\n" +
                        "1970-01-01T00:00:00.007Z\n" +
                        "1970-01-01T00:00:00.001Z\n"
        ));
    }

    @Test
    public void testCastCharToDoubleBind() throws Exception {
        assertMemoryLeak(() -> assertCharBind(
                "double",
                "a\n" +
                        "0.0\n" +
                        "3.0\n"
        ));
    }

    @Test
    public void testCastCharToDoubleLit() throws Exception {
        assertMemoryLeak(() -> assertCharLit(
                "double",
                "a\n" +
                        "4.0\n" +
                        "7.0\n" +
                        "1.0\n"
        ));
    }

    @Test
    public void testCastCharToFloatBind() throws Exception {
        assertMemoryLeak(() -> assertCharBind(
                "float",
                "a\n" +
                        "0.0\n" +
                        "3.0\n"
        ));
    }

    @Test
    public void testCastCharToFloatLit() throws Exception {
        assertMemoryLeak(() -> assertCharLit(
                "float",
                "a\n" +
                        "4.0\n" +
                        "7.0\n" +
                        "1.0\n"
        ));
    }

    @Test
    public void testCastCharToGeoByteBind() throws Exception {
        assertMemoryLeak(() -> assertCharBind(
                "geohash(1c)",
                "a\n" +
                        "0\n" +
                        "3\n"
        ));
    }

    @Test
    public void testCastCharToGeoByteLit() throws Exception {
        assertMemoryLeak(() -> assertCharLit(
                "geohash(1c)",
                "a\n" +
                        "4\n" +
                        "7\n" +
                        "1\n"
        ));
    }

    @Test
    public void testCastCharToIntBind() throws Exception {
        assertMemoryLeak(() -> assertCharBind(
                "int",
                "a\n" +
                        "0\n" +
                        "3\n"
        ));
    }

    @Test
    public void testCastCharToIntLit() throws Exception {
        assertMemoryLeak(() -> assertCharLit(
                "int",
                "a\n" +
                        "4\n" +
                        "7\n" +
                        "1\n"
        ));
    }

    @Test
    public void testCastCharToLong256Bind() throws Exception {
        assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testCastCharToLongBind() throws Exception {
        assertMemoryLeak(() -> assertCharBind(
                "long",
                "a\n" +
                        "0\n" +
                        "3\n"
        ));
    }

    @Test
    public void testCastCharToLongLit() throws Exception {
        assertMemoryLeak(() -> assertCharLit(
                "long",
                "a\n" +
                        "4\n" +
                        "7\n" +
                        "1\n"
        ));
    }

    @Test
    public void testCastCharToShortBind() throws Exception {
        assertMemoryLeak(() -> assertCharBind(
                "short",
                "a\n" +
                        "0\n" +
                        "3\n"
        ));
    }

    @Test
    public void testCastCharToShortLit() throws Exception {
        assertMemoryLeak(() -> assertCharLit(
                "short",
                "a\n" +
                        "4\n" +
                        "7\n" +
                        "1\n"
        ));
    }

    @Test
    public void testCastCharToTimestampBind() throws Exception {
        // this is internal widening cast
        assertMemoryLeak(() -> assertCharBind(
                "timestamp",
                "a\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000003Z\n"
        ));
    }

    @Test
    public void testCastCharToTimestampLit() throws Exception {
        assertMemoryLeak(() -> assertCharLit(
                "timestamp",
                "a\n" +
                        "1970-01-01T00:00:00.000004Z\n" +
                        "1970-01-01T00:00:00.000007Z\n" +
                        "1970-01-01T00:00:00.000001Z\n"
        ));
    }

    @Test
    public void testCastCharToTimestampNSBind() throws Exception {
        // this is internal widening cast
        assertMemoryLeak(() -> assertCharBind(
                "timestamp_ns",
                "a\n" +
                        "1970-01-01T00:00:00.000000000Z\n" +
                        "1970-01-01T00:00:00.000000003Z\n"
        ));
    }

    @Test
    public void testCastCharToTimestampNSLit() throws Exception {
        assertMemoryLeak(() -> assertCharLit(
                "timestamp_ns",
                "a\n" +
                        "1970-01-01T00:00:00.000000004Z\n" +
                        "1970-01-01T00:00:00.000000007Z\n" +
                        "1970-01-01T00:00:00.000000001Z\n"
        ));
    }

    @Test
    public void testCastDoubleToFloatBind() throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            execute("create table y(a float);");
            // execute insert statement for each value of reference table
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).popInsertOperation()
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
                            "null\n",
                    "y"
            );
        });
    }

    @Test
    public void testCastFloatByteTab() throws Exception {
        assertMemoryLeak(() -> assertCastFloatTab(
                "byte",
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
        ));
    }

    @Test
    public void testCastFloatIntTab() throws Exception {
        assertMemoryLeak(() -> assertCastFloatTab(
                "int",
                "a\n" +
                        "28\n" +
                        "29\n" +
                        "8\n" +
                        "20\n" +
                        "93\n" +
                        "null\n" +
                        "66\n" +
                        "80\n" +
                        "22\n" +
                        "12\n" +
                        "8\n",
                -3.4e20f,
                3.4e20f
        ));
    }

    @Test
    public void testCastFloatLongTab() throws Exception {
        assertMemoryLeak(() -> assertCastFloatTab(
                "long",
                "a\n" +
                        "28\n" +
                        "29\n" +
                        "8\n" +
                        "20\n" +
                        "93\n" +
                        "null\n" +
                        "66\n" +
                        "80\n" +
                        "22\n" +
                        "12\n" +
                        "8\n",
                -3.4e35f,
                3.4e35f
        ));
    }

    @Test
    public void testCastFloatShortTab() throws Exception {
        assertMemoryLeak(() -> assertCastFloatTab(
                "short",
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
        ));
    }

    @Test
    public void testCastIntToByteBind() throws Exception {
        assertMemoryLeak(() -> assertIntBind(
                "byte",
                "a\n" +
                        "3\n" +
                        "0\n"
        ));
    }

    @Test
    public void testCastIntToCharBind() throws Exception {
        assertMemoryLeak(() -> assertIntBind(
                "char",
                "a\n" +
                        "3\n" +
                        "\n"
        ));
    }

    @Test
    public void testCastIntToLong256Bind() throws Exception {
        assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testCastIntToShortBind() throws Exception {
        assertMemoryLeak(() -> assertIntBind(
                "short",
                "a\n" +
                        "3\n" +
                        "0\n"
        ));
    }

    @Test
    public void testCastLongToByteBind() throws Exception {
        assertMemoryLeak(() -> assertLongBind(
                "byte",
                "a\n" +
                        "8\n" +
                        "0\n"
        ));
    }

    @Test
    public void testCastLongToCharBind() throws Exception {
        assertMemoryLeak(() -> assertLongBind(
                "char",
                "a\n" +
                        "8\n" +
                        "\n"
        ));
    }

    @Test
    public void testCastLongToLong256Bind() throws Exception {
        assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testCastLongToShortBind() throws Exception {
        assertMemoryLeak(() -> assertLongBind(
                "short",
                "a\n" +
                        "8\n" +
                        "0\n"
        ));
    }

    @Test
    public void testCastShortToByteBind() throws Exception {
        assertMemoryLeak(() -> assertShortBind("byte"));
    }

    @Test
    public void testCastShortToCharBind() throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            execute("create table y(a char);");
            // execute insert statement for each value of reference table
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).popInsertOperation()
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
                            "8\n",
                    "y"
            );
        });
    }

    @Test
    public void testCastShortToLong256Bind() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertShortBind("long256");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "bind variable at 0 is defined as LONG256 and cannot accept SHORT");
            }
        });
    }

    @Test
    public void testCastStrByteTab() throws Exception {
        assertMemoryLeak(() -> assertStrTab(
                "byte",
                "a\tb\n" +
                        "76\t76\n" +
                        "102\t102\n" +
                        "27\t27\n" +
                        "87\t87\n" +
                        "79\t79\n"
        ));
    }

    @Test
    public void testCastStrCharTab() throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            execute("create table y(a char, b string);");
            execute("create table x as (select cast(rnd_byte()%10 as string) a from long_sequence(5));");
            // execute insert statement for each value of reference table
            execute("insert into y select a,a from x");
            assertSql(
                    "a\tb\n" +
                            "6\t6\n" +
                            "2\t2\n" +
                            "7\t7\n" +
                            "7\t7\n" +
                            "9\t9\n",
                    "y"
            );
        });
    }

    @Test
    public void testCastStrDateTab() throws Exception {
        assertMemoryLeak(() -> assertStrTab(
                "date",
                "a\tb\n" +
                        "1970-01-01T00:00:00.076Z\t76\n" +
                        "1970-01-01T00:00:00.102Z\t102\n" +
                        "1970-01-01T00:00:00.027Z\t27\n" +
                        "1970-01-01T00:00:00.087Z\t87\n" +
                        "1970-01-01T00:00:00.079Z\t79\n"
        ));
    }

    @Test
    public void testCastStrDoubleTab() throws Exception {
        assertMemoryLeak(() -> assertStrTab(
                "double",
                "a\tb\n" +
                        "76.0\t76\n" +
                        "102.0\t102\n" +
                        "27.0\t27\n" +
                        "87.0\t87\n" +
                        "79.0\t79\n"
        ));
    }

    @Test
    public void testCastStrFloatTab() throws Exception {
        assertMemoryLeak(() -> assertStrTab(
                "float",
                "a\tb\n" +
                        "76.0\t76\n" +
                        "102.0\t102\n" +
                        "27.0\t27\n" +
                        "87.0\t87\n" +
                        "79.0\t79\n"
        ));
    }

    @Test
    public void testCastStrIntTab() throws Exception {
        assertMemoryLeak(() -> assertStrTab(
                "int",
                "a\tb\n" +
                        "76\t76\n" +
                        "102\t102\n" +
                        "27\t27\n" +
                        "87\t87\n" +
                        "79\t79\n"
        ));
    }

    @Test
    public void testCastStrLongTab() throws Exception {
        assertMemoryLeak(() -> assertStrTab(
                "long",
                "a\tb\n" +
                        "76\t76\n" +
                        "102\t102\n" +
                        "27\t27\n" +
                        "87\t87\n" +
                        "79\t79\n"
        ));
    }

    @Test
    public void testCastStrShortTab() throws Exception {
        assertMemoryLeak(() -> assertStrTab(
                "short",
                "a\tb\n" +
                        "76\t76\n" +
                        "102\t102\n" +
                        "27\t27\n" +
                        "87\t87\n" +
                        "79\t79\n"
        ));
    }

    @Test
    public void testCastStrTimestampNSTab() throws Exception {
        assertMemoryLeak(() -> assertStrTab(
                "timestamp_ns",
                "a\tb\n" +
                        "1970-01-01T00:00:00.000000076Z\t76\n" +
                        "1970-01-01T00:00:00.000000102Z\t102\n" +
                        "1970-01-01T00:00:00.000000027Z\t27\n" +
                        "1970-01-01T00:00:00.000000087Z\t87\n" +
                        "1970-01-01T00:00:00.000000079Z\t79\n"
        ));
    }

    @Test
    public void testCastStrTimestampTab() throws Exception {
        assertMemoryLeak(() -> assertStrTab(
                "timestamp",
                "a\tb\n" +
                        "1970-01-01T00:00:00.000076Z\t76\n" +
                        "1970-01-01T00:00:00.000102Z\t102\n" +
                        "1970-01-01T00:00:00.000027Z\t27\n" +
                        "1970-01-01T00:00:00.000087Z\t87\n" +
                        "1970-01-01T00:00:00.000079Z\t79\n"
        ));
    }

    @Test
    public void testCastStrToByteBind() throws Exception {
        assertMemoryLeak(() -> assertStrBind(
                "byte",
                "a\n" +
                        "12\n" +
                        "31\n" +
                        "0\n"
        ));
    }

    @Test
    public void testCastStrToByteLit() throws Exception {
        assertMemoryLeak(() -> assertStrLit(
                "byte",
                "a\n" +
                        "45\n" +
                        "76\n" +
                        "124\n"
        ));
    }

    @Test
    public void testCastStrToCharLit() throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            execute("create table y(a char);");
            // execute insert statement for each value of reference table
            execute("insert into y values (cast('A' as string))");
            execute("insert into y values (cast('7' as string))");
            try {
                execute("insert into y values ('cc')");
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
            execute("insert into y values (cast('K' as string))");
            assertSql(
                    "a\n" +
                            "A\n" +
                            "7\n" +
                            "K\n",
                    "y"
            );
        });
    }

    @Test
    public void testCastStrToDateBind() throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            execute("create table y(a date);");
            // execute insert statement for each value of reference table
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).popInsertOperation()
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
                            "\n",
                    "y"
            );
        });
    }

    @Test
    public void testCastStrToDateLit() throws Exception {
        assertMemoryLeak(() -> assertStrLit(
                "date",
                "a\n" +
                        "1970-01-01T00:00:00.045Z\n" +
                        "1970-01-01T00:00:00.076Z\n" +
                        "1970-01-01T00:00:00.124Z\n"
        ));
    }

    @Test
    public void testCastStrToDateLitAsDate() throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            execute("create table y(a date);");
            // execute insert statement for each value of reference table
            execute("insert into y values ('2022-01-01T00:00:00.045Z');");
            execute("insert into y values ('2022-01-01T00:00:00.076Z');");
            try {
                execute("insert into y values ('c')");
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
            execute("insert into y values ('2222-01-01T00:00:00.124Z');");
            assertSql(
                    "a\n" +
                            "2022-01-01T00:00:00.045Z\n" +
                            "2022-01-01T00:00:00.076Z\n" +
                            "2222-01-01T00:00:00.124Z\n",
                    "y"
            );
        });
    }

    @Test
    public void testCastStrToDoubleBind() throws Exception {
        assertMemoryLeak(() -> assertStrBind(
                "double",
                "a\n" +
                        "12.0\n" +
                        "31.0\n" +
                        "null\n"
        ));
    }

    @Test
    public void testCastStrToDoubleLit() throws Exception {
        assertMemoryLeak(() -> assertStrLit(
                "double",
                "a\n" +
                        "45.0\n" +
                        "76.0\n" +
                        "124.0\n"
        ));
    }

    @Test
    public void testCastStrToFloatBind() throws Exception {
        assertMemoryLeak(() -> assertStrBind(
                "float",
                "a\n" +
                        "12.0\n" +
                        "31.0\n" +
                        "null\n"
        ));
    }

    @Test
    public void testCastStrToFloatLit() throws Exception {
        assertMemoryLeak(() -> assertStrLit(
                "float",
                "a\n" +
                        "45.0\n" +
                        "76.0\n" +
                        "124.0\n"
        ));
    }

    @Test
    public void testCastStrToIntBind() throws Exception {
        assertMemoryLeak(() -> assertStrBind(
                "int",
                "a\n" +
                        "12\n" +
                        "31\n" +
                        "null\n"
        ));
    }

    @Test
    public void testCastStrToIntLit() throws Exception {
        assertMemoryLeak(() -> assertStrLit(
                "int",
                "a\n" +
                        "45\n" +
                        "76\n" +
                        "124\n"
        ));
    }

    @Test
    public void testCastStrToLong256Bind() throws Exception {
        assertMemoryLeak(() -> assertStrBind(
                "long256",
                "a\n" +
                        "0x12\n" +
                        "0x31\n" +
                        "\n"
        ));
    }

    @Test
    public void testCastStrToLongBind() throws Exception {
        assertMemoryLeak(() -> assertStrBind(
                "long",
                "a\n" +
                        "12\n" +
                        "31\n" +
                        "null\n"
        ));
    }

    @Test
    public void testCastStrToLongLit() throws Exception {
        assertMemoryLeak(() -> assertStrLit(
                "long",
                "a\n" +
                        "45\n" +
                        "76\n" +
                        "124\n"
        ));
    }

    @Test
    public void testCastStrToShortBind() throws Exception {
        assertMemoryLeak(() -> assertStrBind(
                "short",
                "a\n" +
                        "12\n" +
                        "31\n" +
                        "0\n"
        ));
    }

    @Test
    public void testCastStrToShortLit() throws Exception {
        assertMemoryLeak(() -> assertStrLit(
                "short",
                "a\n" +
                        "45\n" +
                        "76\n" +
                        "124\n"
        ));
    }

    @Test
    public void testCastStrToTimestampBind() throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            execute("create table y(a timestamp);");
            // execute insert statement for each value of reference table
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).popInsertOperation()
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
                            "\n",
                    "y"
            );
        });
    }

    @Test
    public void testCastStrToTimestampLit() throws Exception {
        assertMemoryLeak(() -> assertStrLit(
                "timestamp",
                "a\n" +
                        "1970-01-01T00:00:00.000045Z\n" +
                        "1970-01-01T00:00:00.000076Z\n" +
                        "1970-01-01T00:00:00.000124Z\n"
        ));
    }

    @Test
    public void testCastStrToTimestampLitAsTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            execute("create table y(a " + "timestamp" + ");");
            // execute insert statement for each value of reference table
            execute("insert into y values ('2022-01-01T00:00:00.000045Z');");
            execute("insert into y values ('2222-01-01T00:00:00.000076Z');");
            try {
                execute("insert into y values ('c')");
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
            execute("insert into y values ('2222-01-01T00:00:00.000124Z');");
            assertSql(
                    "a\n" +
                            "2022-01-01T00:00:00.000045Z\n" +
                            "2222-01-01T00:00:00.000076Z\n" +
                            "2222-01-01T00:00:00.000124Z\n",
                    "y"
            );
        });
    }

    @Test
    public void testCastStrToTimestampLitAsTimestampNS() throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            execute("create table y(a " + "timestamp_ns" + ");");
            // execute insert statement for each value of reference table
            execute("insert into y values ('2022-01-01T00:00:00.000045678Z');");
            execute("insert into y values ('2222-01-01T00:00:00.000076543Z');");
            try {
                execute("insert into y values ('c')");
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
            execute("insert into y values ('2222-01-01T00:00:00.000124987Z');");
            assertSql(
                    "a\n" +
                            "2022-01-01T00:00:00.000045678Z\n" +
                            "2222-01-01T00:00:00.000076543Z\n" +
                            "2222-01-01T00:00:00.000124987Z\n",
                    "y"
            );
        });
    }

    @Test
    @Ignore("todo wait BindVariableService support timestamp_ns type")
    public void testCastStrToTimestampNSBind() throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            execute("create table y(a timestamp_ns);");
            // execute insert statement for each value of reference table
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).popInsertOperation()
            ) {
                bindVariableService.setStr(0, "2012-04-11T10:45:11");
                insert.execute(sqlExecutionContext);

                bindVariableService.setStr(0, "2012-04-11T10:45:11.344999123");
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
                            "2012-04-11T10:45:11.000000000Z\n" +
                            "2012-04-11T10:45:11.344999123Z\n" +
                            "\n",
                    "y"
            );
        });
    }

    @Test
    public void testCastStrToTimestampNSLit() throws Exception {
        assertMemoryLeak(() -> assertStrLit(
                "timestamp_ns",
                "a\n" +
                        "1970-01-01T00:00:00.000000045Z\n" +
                        "1970-01-01T00:00:00.000000076Z\n" +
                        "1970-01-01T00:00:00.000000124Z\n"
        ));
    }

    @Test
    public void testCastTimestampToByteBind() throws Exception {
        assertMemoryLeak(() -> assertTimestampBind(
                "byte",
                "a\n" +
                        "8\n" +
                        "0\n"
        ));
    }

    @Test
    public void testCastTimestampToDoubleBind() throws Exception {
        assertMemoryLeak(() -> assertTimestampBindNoOverflow(
                "double",
                "a\n" +
                        "8.0\n" +
                        "null\n" +
                        "8.8990229990007E13\n"
        ));
    }

    @Test
    public void testCastTimestampToFloatBind() throws Exception {
        assertMemoryLeak(() -> assertTimestampBindNoOverflow(
                "float",
                "a\n" +
                        "8.0\n" +
                        "null\n" +
                        "8.8990229E13\n"
        ));
    }

    @Test
    public void testCastTimestampToIntBind() throws Exception {
        assertMemoryLeak(() -> assertTimestampBind(
                "int",
                "a\n" +
                        "8\n" +
                        "null\n"
        ));
    }

    @Test
    public void testCastTimestampToLong256Bind() throws Exception {
        assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testCastTimestampToLongBind() throws Exception {
        assertMemoryLeak(() -> assertTimestampBindNoOverflow(
                "long",
                "a\n" +
                        "8\n" +
                        "null\n" +
                        "88990229990007\n"
        ));
    }

    @Test
    public void testCastTimestampToShortBind() throws Exception {
        assertMemoryLeak(() -> assertTimestampBind(
                "short",
                "a\n" +
                        "8\n" +
                        "0\n"
        ));
    }

    @Test
    public void testCastTimestampToStringBind() throws Exception {
        assertMemoryLeak(() -> assertTimestampBindNoOverflow(
                "string",
                "a\n" +
                        "1970-01-01T00:00:00.000008Z\n" +
                        "\n" +
                        "1972-10-26T23:30:29.990007Z\n"
        ));
    }

    @Test
    public void testCastVarcharToDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab(d string, ts timestamp) timestamp(ts) partition by day");
            execute("insert into tab values ('string', '2000'::string), ('varchar', '2000'::varchar);");
            assertSql(
                    "d\tts\n" +
                            "string\t2000-01-01T00:00:00.000000Z\n" +
                            "varchar\t2000-01-01T00:00:00.000000Z\n",
                    "select * from tab order by d"
            );
        });
    }

    @Test
    public void testCastVarcharToDesignatedTimestampNS() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab(d string, ts timestamp_ns) timestamp(ts) partition by day");
            execute("insert into tab values ('string', '2000'::string), ('varchar', '2000'::varchar);");
            assertSql(
                    "d\tts\n" +
                            "string\t2000-01-01T00:00:00.000000000Z\n" +
                            "varchar\t2000-01-01T00:00:00.000000000Z\n",
                    "select * from tab order by d"
            );
        });
    }

    @Test
    public void testInsertNullDateIntoTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x(ts timestamp)");
            execute("insert into x values (cast(null as date))");
            assertSql(
                    "ts\n" +
                            "\n",
                    "x"
            );
        });
    }

    @Test
    public void testInsertNullDateIntoTimestampNS() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x(ts timestamp_ns)");
            execute("insert into x values (cast(null as date))");
            assertSql(
                    "ts\n" +
                            "\n",
                    "x"
            );
        });
    }

    @Test
    public void testNullStringToTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab(ts timestamp) timestamp(ts)");
            try {
                execute("insert into tab values(null::string)");
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "designated timestamp column cannot be NULL");
            }
        });
    }

    @Test
    public void testNullStringToTimestampNS() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab(ts timestamp_ns) timestamp(ts)");
            try {
                execute("insert into tab values(null::string)");
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "designated timestamp column cannot be NULL");
            }
        });
    }

    @Test
    public void testNullVarcharToTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab(ts timestamp) timestamp(ts)");
            try {
                execute("insert into tab values(null::varchar)");
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "designated timestamp column cannot be NULL");
            }
        });
    }

    @Test
    public void testNullVarcharToTimestamp_ns() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab(ts timestamp_ns) timestamp(ts)");
            try {
                execute("insert into tab values(null::varchar)");
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "designated timestamp column cannot be NULL");
            }
        });
    }

    @Test
    public void testVarcharToArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab(arr double[][])");
            execute("insert into tab values ('{{1.0, 2.0}, {3.0, 4.0}}'::varchar), (null)");
            assertQuery("arr\n" +
                            "[[1.0,2.0],[3.0,4.0]]\n" +
                            "null\n",
                    "select * from tab",
                    true);

            // bad array
            assertException("insert into tab values ('not array'::varchar)",
                    0,
                    "inconvertible value: `not array` [VARCHAR -> DOUBLE[][]]");

            // bad non-ascii array (😀)
            assertException("insert into tab values ('{{1.0, 2.0}, {3.0, 4.0, \uD83D\uDE00}}'::varchar)",
                    0,
                    "inconvertible value: `{{1.0, 2.0}, {3.0, 4.0, \uD83D\uDE00}}` [VARCHAR -> DOUBLE[][]]");
        });
    }

    private void assertCastFloatTab(String type, String expected, float outOfRangeLeft, float outOfRangeRight) throws Exception {
        // insert table
        execute("create table y(a " + type + ");");
        execute("create table x as (select rnd_float()*100 a from long_sequence(5));");
        execute("insert into y select rnd_float()*100 a from long_sequence(5);");
        execute("insert into y values (cast ('null' as float));");
        // execute insert statement for each value of reference table
        execute("insert into y select a from x");

        try {
            execute("insert into y values (cast ('" + outOfRangeLeft + "' as float));");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
        }

        try {
            execute("insert into y values (cast ('" + outOfRangeRight + "' as float));");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
        }

        assertSql(expected, "y");
    }

    private void assertCharBind(String toType, String expected) throws Exception {
        // insert table
        execute("create table y(a " + toType + ");");
        // execute insert statement for each value of reference table
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).popInsertOperation()
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
    }

    private void assertCharFunc(String toType, String expected) throws Exception {
        // insert table
        execute("create table y(a " + toType + ");");
        // execute insert statement for each value of reference table
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                InsertOperation insert = compiler.compile(
                        "insert into y values (cast(rnd_int(0, 10, 0) + 47 as char))",
                        sqlExecutionContext
                ).popInsertOperation()
        ) {
            insert.execute(sqlExecutionContext);
            insert.execute(sqlExecutionContext);
            insert.execute(sqlExecutionContext);
        }
        assertSql(expected, "y");
    }

    private void assertCharLit(String toType, String expected) throws Exception {
        // insert table
        execute("create table y(a " + toType + ");");
        // execute insert statement for each value of reference table
        execute("insert into y values ('4')");
        execute("insert into y values ('7')");
        try {
            // 'a' is an invalid geohash and also invalid number
            execute("insert into y values ('a')");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
        }
        execute("insert into y values ('1')");
        assertSql(expected, "y");
    }

    private void assertCharTab(String toType, String expected) throws Exception {
        // insert table
        execute("create table y(a " + toType + ");");
        execute("create table x as (select cast(rnd_int(0,10,0)+47 as char) a from long_sequence(5));");
        // execute insert statement for each value of reference table
        execute("insert into y select a from x");
        assertSql(expected, "y");
    }

    private void assertIntBind(String type, String expected) throws Exception {
        // insert table
        execute("create table y(a " + type + ");");
        // execute insert statement for each value of reference table
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).popInsertOperation()
        ) {
            bindVariableService.setInt(0, 3); // compatible with everything
            insert.execute(sqlExecutionContext);

            bindVariableService.setInt(0, Numbers.INT_NULL);
            insert.execute(sqlExecutionContext);

            try {
                bindVariableService.setInt(0, 88990227); // overflow
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
        }
        assertSql(expected, "y");
    }

    private void assertLongBind(String type, String expected) throws Exception {
        // insert table
        execute("create table y(a " + type + ");");
        // execute insert statement for each value of reference table
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).popInsertOperation()
        ) {
            bindVariableService.setLong(0, 8); // compatible with everything
            insert.execute(sqlExecutionContext);

            bindVariableService.setLong(0, Numbers.LONG_NULL);
            insert.execute(sqlExecutionContext);

            try {
                bindVariableService.setLong(0, 88990229990007L); // overflow
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
        }
        assertSql(expected, "y");
    }

    private void assertShortBind(String type) throws Exception {
        // insert table
        execute("create table y(a " + type + ");");
        // execute insert statement for each value of reference table
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).popInsertOperation()
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
    }

    private void assertStrBind(String toType, String expected) throws Exception {
        // insert table
        execute("create table y(a " + toType + ");");
        // execute insert statement for each value of reference table
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).popInsertOperation()
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
    }

    private void assertStrLit(String toType, String expected) throws Exception {
        // insert table
        execute("create table y(a " + toType + ");");
        // execute insert statement for each value of reference table
        execute("insert into y values ('45')");
        execute("insert into y values ('76')");
        try {
            execute("insert into y values ('cc')");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
        }
        execute("insert into y values ('124')");
        assertSql(expected, "y");
    }

    private void assertStrTab(String toType, String expected) throws Exception {
        // insert table
        execute("create table y(a " + toType + ", b string);");
        execute("create table x as (select cast(rnd_byte() as string) a from long_sequence(5));");
        // execute insert statement for each value of reference table
        execute("insert into y select a,a from x");
        assertSql(expected, "y");
    }

    private void assertTimestampBind(String type, String expected) throws Exception {
        // insert table
        execute("create table y(a " + type + ");");
        // execute insert statement for each value of reference table
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).popInsertOperation()
        ) {
            bindVariableService.setTimestamp(0, 8L); // compatible with everything
            insert.execute(sqlExecutionContext);

            bindVariableService.setTimestamp(0, Numbers.LONG_NULL);
            insert.execute(sqlExecutionContext);

            try {
                bindVariableService.setTimestamp(0, 88990229990007L); // overflow
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
        }
        assertSql(expected, "y");
    }

    private void assertTimestampBindNoOverflow(String type, String expected) throws Exception {
        // insert table
        execute("create table y(a " + type + ");");
        // execute insert statement for each value of reference table
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).popInsertOperation()
        ) {
            bindVariableService.setTimestamp(0, 8L); // compatible with everything
            insert.execute(sqlExecutionContext);

            bindVariableService.setTimestamp(0, Numbers.LONG_NULL);
            insert.execute(sqlExecutionContext);

            bindVariableService.setTimestamp(0, 88990229990007L);
            insert.execute(sqlExecutionContext);
        }
        assertSql(expected, "y");
    }
}
