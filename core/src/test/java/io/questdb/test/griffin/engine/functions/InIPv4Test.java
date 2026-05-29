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

import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVariableTestTuple;
import org.junit.Test;

public class InIPv4Test extends AbstractCairoTest {

    @Test
    public void testAllConst() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ip ipv4, ts timestamp) timestamp(ts)");
            execute(
                    "insert into test values " +
                            "('127.0.0.1', 0), " +
                            "('192.168.0.1', 1000000), " +
                            "('10.0.0.5', 2000000), " +
                            "('255.255.255.255', 3000000)"
            );
            assertQueryNoLeakCheck(
                    """
                            ip\tts
                            192.168.0.1\t1970-01-01T00:00:01.000000Z
                            """,
                    "test where ip in '192.168.0.1'",
                    "ts",
                    true,
                    false
            );
            assertQueryNoLeakCheck(
                    """
                            ip\tts
                            127.0.0.1\t1970-01-01T00:00:00.000000Z
                            10.0.0.5\t1970-01-01T00:00:02.000000Z
                            """,
                    "test where ip in ('127.0.0.1', '10.0.0.5')",
                    "ts",
                    true,
                    false
            );
            assertQueryNoLeakCheck(
                    """
                            ip\tts
                            127.0.0.1\t1970-01-01T00:00:00.000000Z
                            192.168.0.1\t1970-01-01T00:00:01.000000Z
                            255.255.255.255\t1970-01-01T00:00:03.000000Z
                            """,
                    "test where ip in ('127.0.0.1'::ipv4, '192.168.0.1'::ipv4, '255.255.255.255'::ipv4)",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testBadType() throws Exception {
        assertException(
                "test where ip in (12345)",
                "create table test (ip ipv4)",
                18,
                "cannot compare IPv4 with type INT"
        );
    }

    @Test
    public void testBindVarConstants() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ip ipv4, ts timestamp) timestamp(ts)");
            execute(
                    "insert into test values " +
                            "('127.0.0.1', 0), " +
                            "('192.168.0.1', 1000000), " +
                            "('10.0.0.5', 2000000)"
            );

            final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
            tuples.add(new BindVariableTestTuple(
                    "string bind vars",
                    """
                            ip\tts
                            127.0.0.1\t1970-01-01T00:00:00.000000Z
                            192.168.0.1\t1970-01-01T00:00:01.000000Z
                            """,
                    bindVariableService -> {
                        bindVariableService.setStr(0, "127.0.0.1");
                        bindVariableService.setStr(1, "192.168.0.1");
                    }
            ));
            tuples.add(new BindVariableTestTuple(
                    "varchar bind vars",
                    """
                            ip\tts
                            10.0.0.5\t1970-01-01T00:00:02.000000Z
                            """,
                    bindVariableService -> {
                        bindVariableService.setVarchar(0, new Utf8String("10.0.0.5"));
                        bindVariableService.setVarchar(1, new Utf8String("8.8.8.8"));
                    }
            ));
            tuples.add(new BindVariableTestTuple(
                    "ipv4 bind vars",
                    """
                            ip\tts
                            192.168.0.1\t1970-01-01T00:00:01.000000Z
                            """,
                    bindVariableService -> {
                        bindVariableService.setIPv4(0, "192.168.0.1");
                        bindVariableService.setIPv4(1, "8.8.8.8");
                    }
            ));

            assertSql("test where ip in ($1, $2) order by ts", tuples);
        });
    }

    @Test
    public void testBindVarInvalidIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ip ipv4)");
            execute("insert into test values ('127.0.0.1')");

            final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
            tuples.add(new BindVariableTestTuple(
                    "bad ip",
                    "invalid IPv4 format: not.an.ip",
                    bindVariableService -> bindVariableService.setStr(0, "not.an.ip"),
                    18
            ));

            assertSql("test where ip in ($1)", tuples);
        });
    }

    @Test
    public void testColumnInList() throws Exception {
        // Non-constant args in the variadic tail are rejected by FunctionParser
        // before reaching the factory, so the message comes from there.
        assertException(
                "test where ip in (other)",
                "create table test (ip ipv4, other ipv4)",
                18,
                "constant expected"
        );
    }

    @Test
    public void testConstFold() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ip ipv4, ts timestamp) timestamp(ts)");
            execute("insert into test values ('127.0.0.1', 0), ('192.168.0.1', 1000000)");
            // LHS constant IPv4 found in list - all rows pass.
            assertQueryNoLeakCheck(
                    """
                            ip\tts
                            127.0.0.1\t1970-01-01T00:00:00.000000Z
                            192.168.0.1\t1970-01-01T00:00:01.000000Z
                            """,
                    "test where '127.0.0.1'::ipv4 in ('127.0.0.1', '8.8.8.8')",
                    "ts",
                    true,
                    true
            );
            // LHS constant IPv4 not in list - no rows pass.
            assertQueryNoLeakCheck(
                    "ip\tts\n",
                    "test where '1.1.1.1'::ipv4 in ('127.0.0.1', '8.8.8.8')",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testInvalidIPv4StringConstant() throws Exception {
        assertException(
                "test where ip in ('not.an.ip')",
                "create table test (ip ipv4)",
                18,
                "invalid IPv4 format: not.an.ip"
        );
    }

    @Test
    public void testMixedStringAndIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ip ipv4, ts timestamp) timestamp(ts)");
            execute(
                    "insert into test values " +
                            "('127.0.0.1', 0), " +
                            "('192.168.0.1', 1000000), " +
                            "('10.0.0.5', 2000000)"
            );
            assertQueryNoLeakCheck(
                    """
                            ip\tts
                            127.0.0.1\t1970-01-01T00:00:00.000000Z
                            192.168.0.1\t1970-01-01T00:00:01.000000Z
                            """,
                    "test where ip in ('127.0.0.1', '192.168.0.1'::ipv4)",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testNotIn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ip ipv4, ts timestamp) timestamp(ts)");
            execute(
                    "insert into test values " +
                            "('127.0.0.1', 0), " +
                            "('192.168.0.1', 1000000), " +
                            "('10.0.0.5', 2000000)"
            );
            // Regression for the query that surfaced in the query fuzzer:
            // NOT IN on an IPv4 column with an IPv4-typed list element.
            assertQueryNoLeakCheck(
                    """
                            ip\tts
                            127.0.0.1\t1970-01-01T00:00:00.000000Z
                            10.0.0.5\t1970-01-01T00:00:02.000000Z
                            """,
                    "test where not (ip in ('192.168.0.1'::ipv4))",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testNullHandling() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ip ipv4, ts timestamp) timestamp(ts)");
            execute(
                    "insert into test values " +
                            "('127.0.0.1', 0), " +
                            "(null, 1000000), " +
                            "('192.168.0.1', 2000000)"
            );
            // Explicit NULL in the list matches the IPv4 NULL row.
            assertQueryNoLeakCheck(
                    """
                            ip\tts
                            \t1970-01-01T00:00:01.000000Z
                            """,
                    "test where ip in NULL",
                    "ts",
                    true,
                    false
            );
            // NULL mixed with concrete IPs matches both the NULL row and the IP rows.
            assertQueryNoLeakCheck(
                    """
                            ip\tts
                            127.0.0.1\t1970-01-01T00:00:00.000000Z
                            \t1970-01-01T00:00:01.000000Z
                            """,
                    "test where ip in ('127.0.0.1', NULL)",
                    "ts",
                    true,
                    false
            );
            // '0.0.0.0' is the IPv4 NULL marker (Numbers.IPv4_NULL = 0), so an IN
            // entry of '0.0.0.0'::ipv4 matches NULL rows the same way as explicit NULL.
            assertQueryNoLeakCheck(
                    """
                            ip\tts
                            \t1970-01-01T00:00:01.000000Z
                            """,
                    "test where ip in ('0.0.0.0'::ipv4)",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ip ipv4, ts timestamp) timestamp(ts)");
            assertPlanNoLeakCheck(
                    "test where ip in ('127.0.0.1', '192.168.0.1')",
                    """
                            Async Filter workers: 1
                              filter: ip in [127.0.0.1,192.168.0.1]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: test
                            """
            );
        });
    }

    @Test
    public void testSymbolListElementConst() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ip ipv4, ts timestamp) timestamp(ts)");
            execute(
                    "insert into test values " +
                            "('127.0.0.1', 0), " +
                            "('192.168.0.1', 1000000), " +
                            "('10.0.0.5', 2000000)"
            );
            // All-const list with SYMBOL elements: addIPv4ToSet must read them via
            // the type-agnostic getStrA, since SymbolFunction.getIPv4 is final and
            // throws UnsupportedOperationException.
            assertQueryNoLeakCheck(
                    """
                            ip\tts
                            192.168.0.1\t1970-01-01T00:00:01.000000Z
                            10.0.0.5\t1970-01-01T00:00:02.000000Z
                            """,
                    "test where ip in ('192.168.0.1'::symbol, '10.0.0.5'::symbol)",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testSymbolListElementRuntimeConst() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ip ipv4, ts timestamp) timestamp(ts)");
            execute(
                    "insert into test values " +
                            "('127.0.0.1', 0), " +
                            "('192.168.0.1', 1000000), " +
                            "('10.0.0.5', 2000000)"
            );

            final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
            // The runtime-constant bind var keeps the list un-folded, so the SYMBOL
            // element is resolved in InIPv4RuntimeConstFunction.init() via addIPv4ToSet.
            tuples.add(new BindVariableTestTuple(
                    "bind var plus symbol element",
                    """
                            ip\tts
                            127.0.0.1\t1970-01-01T00:00:00.000000Z
                            192.168.0.1\t1970-01-01T00:00:01.000000Z
                            """,
                    bindVariableService -> bindVariableService.setStr(0, "127.0.0.1")
            ));

            assertSql("test where ip in ($1, '192.168.0.1'::symbol) order by ts", tuples);
        });
    }
}
