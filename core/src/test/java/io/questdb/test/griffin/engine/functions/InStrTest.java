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
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVariableTestTuple;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class InStrTest extends AbstractCairoTest {
    @Test
    public void testAllConst() throws Exception {
        execute("create table test as (select cast(x as string) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))");
        assertQuery("a\tts\n", "test where a in NULL", false);
        assertQuery("a\tts\n" +
                "16\t1970-01-01T00:00:15.000000Z\n", "test where a in ('16', NULL)", false);
        assertQuery("a\tts\n" +
                "3\t1970-01-01T00:00:02.000000Z\n", "test where a in '3'", false);
        assertQuery("a\tts\n" +
                "3\t1970-01-01T00:00:02.000000Z\n" +
                "22\t1970-01-01T00:00:21.000000Z\n" +
                "79\t1970-01-01T00:01:18.000000Z\n", "test where a in ('3', '22', '79')", false);
    }

    @Test
    public void testBindVarTypeChange() throws SqlException {
        execute("create table test as (select x, rnd_str(3,6,1) a from long_sequence(100))");

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

    @Test
    public void testBindVarTypeNotConvertible() throws Exception {
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
    }

    @Test
    public void testChar() throws Exception {
        execute("create table test as (select cast(x as string) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))");
        assertQuery("a\tts\n" +
                "3\t1970-01-01T00:00:02.000000Z\n", "test where a in '3'::char", false);
        assertQuery("a\tts\n" +
                "3\t1970-01-01T00:00:02.000000Z\n" +
                "22\t1970-01-01T00:00:21.000000Z\n" +
                "79\t1970-01-01T00:01:18.000000Z\n", "test where a in ('3'::char, '22', '79')", false);

        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "char test",
                "a\tts\n" +
                        "4\t1970-01-01T00:00:03.000000Z\n" +
                        "15\t1970-01-01T00:00:14.000000Z\n" +
                        "77\t1970-01-01T00:01:16.000000Z\n",
                bindVariableService -> {
                    bindVariableService.setChar(0, '4');
                    bindVariableService.setStr(1, "15");
                    bindVariableService.setVarchar(2, new Utf8String("77"));
                }
        ));

        assertSql("test where a in ($1, $2, $3)", tuples);
    }

    @Test
    public void testColumn() throws Exception {
        assertException(
                "test where a in ('abcde', '81', b)",
                "create table test as (select cast(x as string) a, x b, timestamp_sequence(0, 1000000) ts from long_sequence(100))",
                32,
                "constant expected"
        );
    }

    @Test
    public void testConstAndBindVarMixed() throws Exception {
        execute("create table test as (select cast(x as string) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))");

        final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
        tuples.add(new BindVariableTestTuple(
                "mixed",
                "a\tts\n" +
                        "6\t1970-01-01T00:00:05.000000Z\n" +
                        "27\t1970-01-01T00:00:26.000000Z\n" +
                        "52\t1970-01-01T00:00:51.000000Z\n" +
                        "81\t1970-01-01T00:01:20.000000Z\n",
                bindVariableService -> {
                    bindVariableService.setStr(0, "27");
                    bindVariableService.setVarchar(1, new Utf8String("52"));
                }
        ));

        assertSql("test where a in ('6', '81', $1, null, $2)", tuples);
    }

    @Test
    public void testConstConst() throws Exception {
        execute("create table test as (select cast(x as string) a, timestamp_sequence(0, 1000000) ts from long_sequence(5))");
        assertQuery("a\tts\n" +
                "1\t1970-01-01T00:00:00.000000Z\n" +
                "2\t1970-01-01T00:00:01.000000Z\n" +
                "3\t1970-01-01T00:00:02.000000Z\n" +
                "4\t1970-01-01T00:00:03.000000Z\n" +
                "5\t1970-01-01T00:00:04.000000Z\n", "test where 'a' in ('a', 'b')", true, true);
        assertQuery("a\tts\n", "test where NULL::string in ('a', 'b')", false, true);
        assertQuery("a\tts\n" +
                "1\t1970-01-01T00:00:00.000000Z\n" +
                "2\t1970-01-01T00:00:01.000000Z\n" +
                "3\t1970-01-01T00:00:02.000000Z\n" +
                "4\t1970-01-01T00:00:03.000000Z\n" +
                "5\t1970-01-01T00:00:04.000000Z\n", "test where NULL::string in ('a', NULL)", true, true);
    }
}
