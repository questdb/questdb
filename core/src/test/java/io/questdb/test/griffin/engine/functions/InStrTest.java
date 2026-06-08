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
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVariableTestTuple;
import io.questdb.test.tools.TestUtils;
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
        assertMemoryLeak(() -> {
            execute("create table test as (select x, rnd_str(3,6,1) a from long_sequence(100))");

            // when more than one argument supplied, the function will match exact values from the list
            final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
            tuples.add(new BindVariableTestTuple(
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
                    "cannot compare STRING with type INT",
                    bindVariableService -> {
                        bindVariableService.setVarchar(0, new Utf8String("ELLKK"));
                        bindVariableService.setInt(1, 30);
                        bindVariableService.setStr(2, "GNJJILL");
                    },
                    20
            ));

            tuples.add(new BindVariableTestTuple(
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

            assertSql("test where a in ($1,$2,$3)", tuples);
        });
    }

    @Test
    public void testBindVarTypeNotConvertible() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as string) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))");

            final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
            tuples.add(new BindVariableTestTuple(
                    "mixed",
                    "a\tts\n",
                    bindVariableService -> {
                        bindVariableService.setVarchar(0, new Utf8String("52"));
                        bindVariableService.setFloat(1, (float) 4567.6);
                    }
            ));

            try {
                assertSql("test where a in ('81', $1, null, $2)", tuples);
            } catch (SqlException e) {
                TestUtils.assertContains(e.getMessage(), "[33] cannot compare STRING with type FLOAT");
            }
        });
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

            final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
            tuples.add(new BindVariableTestTuple(
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

            assertSql("test where a in ($1, $2, $3)", tuples);
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
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as string) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))");

            final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
            tuples.add(new BindVariableTestTuple(
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

            assertSql("test where a in ('6', '81', $1, null, $2)", tuples);
        });
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
