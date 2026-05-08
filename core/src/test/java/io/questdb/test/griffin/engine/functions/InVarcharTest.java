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

public class InVarcharTest extends AbstractCairoTest {

    @Test
    public void testAllConst() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as varchar) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))");
            assertQueryNoLeakCheck("a\tts\n", "test where a in NULL", false);
            assertQueryNoLeakCheck(
                    """
                            a\tts
                            16\t1970-01-01T00:00:15.000000Z
                            """,
                    "test where a in ('16', NULL)",
                    false
            );
            assertQueryNoLeakCheck(
                    """
                            a\tts
                            3\t1970-01-01T00:00:02.000000Z
                            """,
                    "test where a in '3'",
                    false
            );
            assertQueryNoLeakCheck(
                    """
                            a\tts
                            3\t1970-01-01T00:00:02.000000Z
                            22\t1970-01-01T00:00:21.000000Z
                            79\t1970-01-01T00:01:18.000000Z
                            """,
                    "test where a in ('3', '22', '79')",
                    false
            );
        });
    }

    @Test
    public void testBindVarTypeChange() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as varchar) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))");

            final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
            tuples.add(new BindVariableTestTuple(
                    "all varchar",
                    """
                            a\tts
                            1\t1970-01-01T00:00:00.000000Z
                            2\t1970-01-01T00:00:01.000000Z
                            30\t1970-01-01T00:00:29.000000Z
                            """,
                    bindVariableService -> {
                        bindVariableService.setVarchar(0, new Utf8String("1"));
                        bindVariableService.setVarchar(1, new Utf8String("2"));
                        bindVariableService.setVarchar(2, new Utf8String("30"));
                    }
            ));

            tuples.add(new BindVariableTestTuple(
                    "type change",
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
    public void testBindVarTypeNotConvertible() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as varchar) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))");

            final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
            tuples.add(new BindVariableTestTuple(
                    "mixed",
                    "a\tts\n",
                    bindVariableService -> {
                        bindVariableService.setVarchar(0, new Utf8String("52"));
                        bindVariableService.setInt(1, 4567);
                    }
            ));

            try {
                assertSql("test where a in ('6', '81', $1, null, $2)", tuples);
            } catch (SqlException e) {
                TestUtils.assertContains(e.getMessage(), "[38] cannot compare VARCHAR with type INT");
            }
        });
    }

    @Test
    public void testChar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as varchar) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))");
            assertQueryNoLeakCheck(
                    """
                            a\tts
                            3\t1970-01-01T00:00:02.000000Z
                            """,
                    "test where a in '3'::char",
                    false
            );
            assertQueryNoLeakCheck(
                    """
                            a\tts
                            3\t1970-01-01T00:00:02.000000Z
                            22\t1970-01-01T00:00:21.000000Z
                            79\t1970-01-01T00:01:18.000000Z
                            """,
                    "test where a in ('3'::char, '22', '79')",
                    false
            );

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
    public void testCharNulInListMatchesNullVarcharRow() throws Exception {
        // A CHAR(0) list element must be added to the set as null, mirroring
        // CastCharToVarcharFunctionFactory's CHAR(0) -> NULL mapping. Otherwise a NULL
        // varchar row fails to match a CHAR(0) IN entry while it correctly matches an
        // explicit NULL entry. This shape is reachable in practice: with VARCHAR LHS
        // the dispatcher picks InVarcharFunctionFactory exactly, and a NULL row tested
        // against a CHAR(0) IN entry hits the all-const fast path here.
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as varchar) a, timestamp_sequence(0, 1000000) ts from long_sequence(3))");
            execute("insert into test values (NULL, '1970-01-01T00:03:00.000000Z')");
            // CHAR(0) alongside another literal: the NULL row matches via CHAR(0), '1' picks
            // up its own row.
            assertQueryNoLeakCheck(
                    """
                            a\tts
                            1\t1970-01-01T00:00:00.000000Z
                            \t1970-01-01T00:03:00.000000Z
                            """,
                    "test where a in ('1', (0)::char)",
                    false
            );
            // CHAR(0) alone in the list: only the NULL row matches.
            assertQueryNoLeakCheck(
                    """
                            a\tts
                            \t1970-01-01T00:03:00.000000Z
                            """,
                    "test where a in ((0)::char)",
                    false
            );
            // CHAR(0) alongside an explicit NULL: both map to set-null, no double-add or
            // false-mismatch; only the NULL row matches.
            assertQueryNoLeakCheck(
                    """
                            a\tts
                            \t1970-01-01T00:03:00.000000Z
                            """,
                    "test where a in (NULL, (0)::char)",
                    false
            );
            // A non-zero CHAR keeps its 1-byte encoding and only matches the corresponding
            // varchar row, never the NULL row.
            assertQueryNoLeakCheck(
                    """
                            a\tts
                            1\t1970-01-01T00:00:00.000000Z
                            """,
                    "test where a in (('1')::char)",
                    false
            );
            // Bind variant of CHAR(0): LHS is the varchar column, the IN list element is a
            // runtime-constant CHAR(0) routed through the RuntimeConstFunc path. The
            // factory's parseToVarchar runs in init(), so the same null-mirror behavior
            // must hold there too.
            bindVariableService.clear();
            bindVariableService.setStr("b0", "0");
            assertQueryNoLeakCheck(
                    """
                            a\tts
                            \t1970-01-01T00:03:00.000000Z
                            """,
                    "test where a in ((:b0::int)::char)",
                    false
            );
        });
    }

    @Test
    public void testColumn() throws Exception {
        assertException(
                "test where a in ('6', '81', b)",
                "create table test as (select cast(x as varchar) a, x b, timestamp_sequence(0, 1000000) ts from long_sequence(100))",
                28,
                "constant expected"
        );
    }

    @Test
    public void testConstAndBindVarMixed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as varchar) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))");

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
            execute("create table test as (select cast(x as varchar) a, timestamp_sequence(0, 1000000) ts from long_sequence(5))");
            assertQueryNoLeakCheck(
                    """
                            a\tts
                            1\t1970-01-01T00:00:00.000000Z
                            2\t1970-01-01T00:00:01.000000Z
                            3\t1970-01-01T00:00:02.000000Z
                            4\t1970-01-01T00:00:03.000000Z
                            5\t1970-01-01T00:00:04.000000Z
                            """,
                    "test where 'a'::varchar in ('a', 'b')",
                    null,
                    true,
                    true
            );
            assertQueryNoLeakCheck("a\tts\n", "test where NULL::varchar in ('a', 'b')", null, true, true);
            assertQueryNoLeakCheck(
                    """
                            a\tts
                            1\t1970-01-01T00:00:00.000000Z
                            2\t1970-01-01T00:00:01.000000Z
                            3\t1970-01-01T00:00:02.000000Z
                            4\t1970-01-01T00:00:03.000000Z
                            5\t1970-01-01T00:00:04.000000Z
                            """,
                    "test where NULL::varchar in ('a', NULL)",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testNonConstExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table in_values as (select cast(x as varchar) value, timestamp_sequence(0, 1000000) ts from long_sequence(5))");

            assertExceptionNoLeakCheck(
                    "test where a in (select value from in_values)",
                    "create table test as (select cast(x as varchar) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))",
                    17,
                    "constant expected"
            );
        });
    }

    @Test
    public void testNonSupportedType() throws Exception {
        assertException(
                "test where a in (12.34)",
                "create table test as (select cast(x as varchar) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))",
                17,
                "cannot compare VARCHAR with type DOUBLE"
        );
    }

    @Test
    public void testNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as varchar) a, timestamp_sequence(0, 1000000) ts from long_sequence(100))");
            execute("insert into test values (NULL, '1970-01-01T00:03:00.000000Z')");

            assertQueryNoLeakCheck(
                    """
                            a\tts
                            \t1970-01-01T00:03:00.000000Z
                            """,
                    "test where a in NULL",
                    false
            );
            assertQueryNoLeakCheck(
                    """
                            a\tts
                            16\t1970-01-01T00:00:15.000000Z
                            \t1970-01-01T00:03:00.000000Z
                            """,
                    "test where a in ('16', NULL)",
                    false
            );
            assertQueryNoLeakCheck(
                    """
                            a\tts
                            3\t1970-01-01T00:00:02.000000Z
                            """,
                    "test where a in '3'",
                    false
            );
            assertQueryNoLeakCheck(
                    """
                            a\tts
                            3\t1970-01-01T00:00:02.000000Z
                            22\t1970-01-01T00:00:21.000000Z
                            79\t1970-01-01T00:01:18.000000Z
                            """,
                    "test where a in ('3', '22', '79')",
                    false
            );

            final ObjList<BindVariableTestTuple> tuples = new ObjList<>();
            tuples.add(new BindVariableTestTuple(
                    "null test",
                    """
                            a\tts
                            15\t1970-01-01T00:00:14.000000Z
                            77\t1970-01-01T00:01:16.000000Z
                            \t1970-01-01T00:03:00.000000Z
                            """,
                    bindVariableService -> {
                        bindVariableService.setVarchar(0, null);
                        bindVariableService.setStr(1, "15");
                        bindVariableService.setVarchar(2, new Utf8String("77"));
                    }
            ));

            assertSql("test where a in ($1, $2, $3)", tuples);
        });
    }
}
