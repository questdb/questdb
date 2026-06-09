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
import io.questdb.test.tools.BindVarTuple;
import org.junit.Test;

public class InStrTest extends AbstractCairoTest {

    @Test
    public void testAllConst() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as string) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))");
            assertQuery("test where a in NULL")
                    .noLeakCheck()
                    .returns("a\tts\n");
            assertQuery("test where a in ('16', NULL)")
                    .noLeakCheck()
                    .returns("""
                            a\tts
                            16\t1970-01-01T00:00:15.000000Z
                            """);
            assertQuery("test where a in '3'")
                    .noLeakCheck()
                    .returns("""
                            a\tts
                            3\t1970-01-01T00:00:02.000000Z
                            """);
            assertQuery("test where a in ('3', '22', '79')")
                    .noLeakCheck()
                    .returns("""
                            a\tts
                            3\t1970-01-01T00:00:02.000000Z
                            22\t1970-01-01T00:00:21.000000Z
                            79\t1970-01-01T00:01:18.000000Z
                            """);
        });
    }

    @Test
    public void testBindVarTypeChange() throws Exception {
        // when more than one argument supplied, the function will match exact values from the list
        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.ok(
                "simple",
                """
                        x\ta
                        26\tSUW
                        69\tWZLUOG
                        87\tGQZVK
                        """,
                bindVariableService -> {
                    bindVariableService.setStr(0, "WZLUOG");
                    bindVariableService.setStr(1, "GQZVK");
                    bindVariableService.setStr(2, "SUW");
                }
        ));

        cases.add(BindVarTuple.fails(
                "undefined bind variable",
                20,
                "undefined bind variable: 1",
                bindVariableService -> {
                    bindVariableService.setStr(0, "ELLKK");
                    bindVariableService.setStr(2, "GNJJILL");
                }
        ));

        cases.add(BindVarTuple.fails(
                "bad type",
                20,
                "cannot compare STRING with type INT",
                bindVariableService -> {
                    bindVariableService.setVarchar(0, new Utf8String("ELLKK"));
                    bindVariableService.setInt(1, 30);
                    bindVariableService.setStr(2, "GNJJILL");
                }
        ));

        cases.add(BindVarTuple.ok(
                "with nulls",
                """
                        x\ta
                        2\t
                        4\tNRXG
                        19\t
                        24\t
                        29\t
                        30\t
                        32\t
                        33\t
                        35\t
                        36\t
                        38\t
                        39\t
                        40\t
                        41\t
                        43\t
                        49\t
                        55\t
                        62\t
                        66\t
                        68\t
                        70\t
                        73\t
                        84\t
                        93\tNPIW
                        """,
                bindVariableService -> {
                    bindVariableService.setStr(0, "NPIW");
                    bindVariableService.setStr(1, null);
                    bindVariableService.setStr(2, "NRXG");
                }
        ));

        assertQuery("test where a in ($1,$2,$3)")
                .ddl("create table test as (select x, rnd_str(3,6,1) a from long_sequence(100))")
                .assertBinds(cases);
    }

    @Test
    public void testBindVarTypeNotConvertible() throws Exception {
        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.fails(
                "mixed",
                33,
                "cannot compare STRING with type FLOAT",
                bindVariableService -> {
                    bindVariableService.setVarchar(0, new Utf8String("52"));
                    bindVariableService.setFloat(1, (float) 4567.6);
                }
        ));

        assertQuery("test where a in ('81', $1, null, $2)")
                .ddl("create table test as (select cast(x as string) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))")
                .assertBinds(cases);
    }

    @Test
    public void testChar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as string) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))");
            assertQuery("test where a in '3'::char")
                    .noLeakCheck()
                    .returns("""
                            a\tts
                            3\t1970-01-01T00:00:02.000000Z
                            """);
            assertQuery("test where a in ('3'::char, '22', '79')")
                    .noLeakCheck()
                    .returns("""
                            a\tts
                            3\t1970-01-01T00:00:02.000000Z
                            22\t1970-01-01T00:00:21.000000Z
                            79\t1970-01-01T00:01:18.000000Z
                            """);

            final ObjList<BindVarTuple> cases = new ObjList<>();
            cases.add(BindVarTuple.ok(
                    "char test",
                    """
                            a\tts
                            4\t1970-01-01T00:00:03.000000Z
                            15\t1970-01-01T00:00:14.000000Z
                            77\t1970-01-01T00:01:16.000000Z
                            """,
                    bindVariableService -> {
                        bindVariableService.setChar(0, '4');
                        bindVariableService.setStr(1, "15");
                        bindVariableService.setVarchar(2, new Utf8String("77"));
                    }
            ));

            assertQuery("test where a in ($1, $2, $3)")
                    .noLeakCheck()
                    .assertBinds(cases);
        });
    }

    @Test
    public void testColumn() throws Exception {
        assertQuery("test where a in ('abcde', '81', b)")
                .ddl("create table test as (select cast(x as string) a, x b, timestamp_sequence(0, 1000000) ts from long_sequence(100))")
                .fails(32, "constant expected");
    }

    @Test
    public void testConstAndBindVarMixed() throws Exception {
        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.ok(
                "mixed",
                """
                        a\tts
                        6\t1970-01-01T00:00:05.000000Z
                        27\t1970-01-01T00:00:26.000000Z
                        52\t1970-01-01T00:00:51.000000Z
                        81\t1970-01-01T00:01:20.000000Z
                        """,
                bindVariableService -> {
                    bindVariableService.setStr(0, "27");
                    bindVariableService.setVarchar(1, new Utf8String("52"));
                }
        ));

        assertQuery("test where a in ('6', '81', $1, null, $2)")
                .ddl("create table test as (select cast(x as string) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))")
                .assertBinds(cases);
    }

    @Test
    public void testConstConst() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as string) a, timestamp_sequence(0, 1000000) ts from long_sequence(5))");
            assertQuery("test where 'a' in ('a', 'b')")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tts
                            1\t1970-01-01T00:00:00.000000Z
                            2\t1970-01-01T00:00:01.000000Z
                            3\t1970-01-01T00:00:02.000000Z
                            4\t1970-01-01T00:00:03.000000Z
                            5\t1970-01-01T00:00:04.000000Z
                            """);
            assertQuery("test where NULL::string in ('a', 'b')")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a\tts\n");
            assertQuery("test where NULL::string in ('a', NULL)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tts
                            1\t1970-01-01T00:00:00.000000Z
                            2\t1970-01-01T00:00:01.000000Z
                            3\t1970-01-01T00:00:02.000000Z
                            4\t1970-01-01T00:00:03.000000Z
                            5\t1970-01-01T00:00:04.000000Z
                            """);
        });
    }
}
