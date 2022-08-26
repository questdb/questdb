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

package io.questdb.griffin;

import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class InsertCastTest extends AbstractGriffinTest {

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
            compiler.compile("create table y(a date);", sqlExecutionContext);
            // execute insert statement for each value of reference table
            executeInsert("insert into y values ('2022-01-01T00:00:00.045Z');");
            executeInsert("insert into y values ('2022-01-01T00:00:00.076Z');");
            try {
                executeInsert("insert into y values ('c')");
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
            executeInsert("insert into y values ('2222-01-01T00:00:00.124Z');");
            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "y",
                    sink,
                    "a\n" +
                            "2022-01-01T00:00:00.045Z\n" +
                            "2022-01-01T00:00:00.076Z\n" +
                            "2222-01-01T00:00:00.124Z\n"
            );
        });
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
            compiler.compile("create table y(a " + "timestamp" + ");", sqlExecutionContext);
            // execute insert statement for each value of reference table
            executeInsert("insert into y values ('2022-01-01T00:00:00.000045Z');");
            executeInsert("insert into y values ('2222-01-01T00:00:00.000076Z');");
            try {
                executeInsert("insert into y values ('c')");
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
            executeInsert("insert into y values ('2222-01-01T00:00:00.000124Z');");
            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "y",
                    sink,
                    "a\n" +
                            "2022-01-01T00:00:00.000045Z\n" +
                            "2222-01-01T00:00:00.000076Z\n" +
                            "2222-01-01T00:00:00.000124Z\n"
            );
        });
    }

    @Test
    public void testInsertNullDateIntoTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table x(ts timestamp)", sqlExecutionContext);
            executeInsert("insert into x values (cast(null as date))");
            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "x",
                    sink,
                    "ts\n" +
                            "\n"
            );
        });
    }

    private void assertCharBind(String toType, String expected) throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            compiler.compile("create table y(a " + toType + ");", sqlExecutionContext);
            // execute insert statement for each value of reference table
            try (InsertOperation insert = compiler.compile("insert into y values ($1)", sqlExecutionContext).getInsertOperation()) {
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
            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "y",
                    sink,
                    expected
            );
        });
    }

    private void assertCharFunc(String toType, String expected) throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            compiler.compile("create table y(a " + toType + ");", sqlExecutionContext);
            // execute insert statement for each value of reference table
            try (
                    InsertOperation insert = compiler.compile(
                            "insert into y values (cast(rnd_int(0, 10, 0) + 47 as char))",
                            sqlExecutionContext
                    ).getInsertOperation()
            ) {
                insert.execute(sqlExecutionContext);
                insert.execute(sqlExecutionContext);
                insert.execute(sqlExecutionContext);
            }
            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "y",
                    sink,
                    expected
            );
        });
    }

    private void assertCharLit(String toType, String expected) throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            compiler.compile("create table y(a " + toType + ");", sqlExecutionContext);
            // execute insert statement for each value of reference table
            executeInsert("insert into y values ('4')");
            executeInsert("insert into y values ('7')");
            try {
                executeInsert("insert into y values ('c')");
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
            executeInsert("insert into y values ('1')");
            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "y",
                    sink,
                    expected
            );
        });
    }

    private void assertCharTab(String toType, String expected) throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            compiler.compile("create table y(a " + toType + ");", sqlExecutionContext);
            compiler.compile("create table x as (select cast(rnd_int(0,10,0)+47 as char) a from long_sequence(5));", sqlExecutionContext);
            // execute insert statement for each value of reference table
            compiler.compile("insert into y select a from x", sqlExecutionContext).getInsertOperation();
            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "y",
                    sink,
                    expected
            );
        });
    }

    private void assertStrLit(String toType, String expected) throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            compiler.compile("create table y(a " + toType + ");", sqlExecutionContext);
            // execute insert statement for each value of reference table
            executeInsert("insert into y values ('45')");
            executeInsert("insert into y values ('76')");
            try {
                executeInsert("insert into y values ('cc')");
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
            executeInsert("insert into y values ('124')");
            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "y",
                    sink,
                    expected
            );
        });
    }

    private void assertStrTab(String toType, String expected) throws Exception {
        assertMemoryLeak(() -> {
            // insert table
            compiler.compile("create table y(a " + toType + ", b string);", sqlExecutionContext);
            compiler.compile("create table x as (select cast(rnd_byte() as string) a from long_sequence(5));", sqlExecutionContext);
            // execute insert statement for each value of reference table
            compiler.compile("insert into y select a,a from x", sqlExecutionContext).getInsertOperation();
            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "y",
                    sink,
                    expected
            );
        });
    }
}
