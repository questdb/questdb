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

package io.questdb.test.griffin.engine.functions.regex;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ILikeFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBindVariableConcatIndexed() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select rnd_str() name from long_sequence(2000))");

            bindVariableService.setStr(0, "H");
            try (RecordCursorFactory factory = select("select * from x where name ilike '%' || $1 || '%'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true, sink);
                    Assert.assertNotEquals(sink.toString().indexOf('H'), -1);
                }
            }
        });
    }

    @Test
    public void testBindVariableConcatNamed() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select rnd_str() name from long_sequence(2000))");

            bindVariableService.setStr("str", "H");
            try (RecordCursorFactory factory = select("select * from x where name ilike '%' || :str || '%'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true, sink);
                    Assert.assertNotEquals(sink.toString().indexOf('H'), -1);
                }
            }
        });
    }

    @Test
    public void testEmptyLikeString() throws Exception {
        assertMemoryLeak(() -> {
            String sql = "create table x as (\n" +
                    "select cast('ABCGE' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('SBDHDJ' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('BDGDGGG' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('AAAAVVV' as string) as name from long_sequence(1)\n" +
                    ")";
            ddl(sql);

            try (RecordCursorFactory factory = select("select * from x where name ilike ''")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), false, sink);
                    Assert.assertEquals(sink.toString(), "");
                }
            }
        });
    }

    @Test
    public void testInvalidRegex() throws Exception {
        assertMemoryLeak(() -> {
            String sql = "create table x as (\n" +
                    "select cast('ABCGE' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('SBDHDJ' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('BDGDGGG' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('AAAAVVV' as string) as name from long_sequence(1)\n" +
                    ")";
            ddl(sql);

            try (RecordCursorFactory factory = select("select * from x where name ilike '[][n'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), false, sink);
                    Assert.assertEquals(sink.toString(), "");
                }
            }
        });
    }

    @Test
    public void testLikeCharacterNoMatch() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select rnd_str() name from long_sequence(2000))");
            try (RecordCursorFactory factory = select("select * from x where  name ilike 'H'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true, sink);
                    Assert.assertEquals(Chars.indexOf(sink, 'H'), -1);
                    sink.clear();
                }
            }
        });
    }

    @Test
    public void testLikeStringCaseInsensitive() throws Exception {
        assertMemoryLeak(() -> {
            String sql = "create table x as (\n" +
                    "select cast('ABCGE' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('SBDHDJ' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('BDGDGGG' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('AAAAVVV' as string) as name from long_sequence(1)\n" +
                    ")";
            ddl(sql);

            try (RecordCursorFactory factory = select("select * from x where name ilike 'aBcGe'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), false, sink);
                    Assert.assertEquals(sink.toString().replace("\n", ""), "ABCGE");
                    sink.clear();
                }
            }
        });
    }

    @Test
    public void testLikeStringNoMatch() throws Exception {
        assertMemoryLeak(() -> {
                    ddl("create table x as (select rnd_str() name from long_sequence(2000))");
                    try (RecordCursorFactory factory = select("select * from x where name ilike 'XJ'")) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            sink.clear();
                            printer.print(cursor, factory.getMetadata(), true, sink);
                            Assert.assertEquals(sink.toString().indexOf("XJ"), -1);
                        }
                    }
                }
        );
    }

    @Test
    public void testLikeStringPercentageAtEnd() throws Exception {
        assertMemoryLeak(() -> {
            String sql = "create table x as (\n" +
                    "select cast('ABCGE' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('SBDHDJ' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('BDGDGGG' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('AAAAVVV' as string) as name from long_sequence(1)\n" +
                    ")";
            ddl(sql);

            try (RecordCursorFactory factory = select("select * from x where name ilike 'AbC%'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), false, sink);
                    Assert.assertEquals(sink.toString().replace("\n", ""), "ABCGE");
                    sink.clear();

                }
            }
        });
    }

    @Test
    public void testLikeStringPercentageAtStart() throws Exception {
        assertMemoryLeak(() -> {
            String sql = "create table x as (\n" +
                    "select cast('ABCGE' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('SBDHDJ' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('BDGDGGG' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('AAAAVVV' as string) as name from long_sequence(1)\n" +
                    ")";
            ddl(sql);

            try (RecordCursorFactory factory = select("select * from x where name ilike '%GgG'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), false, sink);
                    Assert.assertEquals(sink.toString().replace("\n", ""), "BDGDGGG");
                }
            }
        });
    }

    @Test
    public void testLikeStringPercentageAtStartAndEnd() throws Exception {
        assertMemoryLeak(() -> {
            String sql = "create table x as (\n" +
                    "select cast('ABCGE' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('SBDHDJ' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('BDGDGGG' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('AAAAVVV' as string) as name from long_sequence(1)\n" +
                    ")";
            ddl(sql);

            try (RecordCursorFactory factory = select("select * from x where name ilike '%BcG%'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), false, sink);
                    Assert.assertEquals(sink.toString().replace("\n", ""), "ABCGE");
                }
            }
        });
    }

    @Test
    public void testLikeStringUnderscoreAndPercentage() throws Exception {
        assertMemoryLeak(() -> {
            String sql = "create table x as (\n" +
                    "select cast('ABCGE' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('SBDHDJ' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('BDGDGGG' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('AAAAVVV' as string) as name from long_sequence(1)\n" +
                    ")";
            ddl(sql);

            try (RecordCursorFactory factory = select("select * from x where name ilike '_B%'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), false, sink);
                    Assert.assertEquals(sink.toString().split("\n").length, 2);
                }
            }
        });
    }

    @Test
    public void testLikeStringUnderscoreAtStartAndEnd() throws Exception {
        assertMemoryLeak(() -> {
            String sql = "create table x as (\n" +
                    "select cast('ABCGE' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('SBDHDJ' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('BDGDGGG' as string) as name from long_sequence(1)\n" +
                    "union\n" +
                    "select cast('AAAAVVV' as string) as name from long_sequence(1)\n" +
                    ")";
            ddl(sql);

            try (RecordCursorFactory factory = select("select * from x where name ilike '_BC__'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), false, sink);
                    Assert.assertEquals(sink.toString().replace("\n", ""), "ABCGE");
                }
            }
        });
    }

    @Test
    public void testNonConstantExpression() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select rnd_str() name from long_sequence(2000))");
            try {
                assertException("select * from x where name ilike rnd_str('foo','bar')");
            } catch (SqlException e) {
                Assert.assertEquals(33, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "use constant or bind variable");
            }
        });
    }

    @Test
    public void testNotLikeCharacterMatch() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select rnd_str() name from long_sequence(2000))");

            try (RecordCursorFactory factory = select("select * from x where not name ilike 'H'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true, sink);
                    Assert.assertNotEquals(sink.toString().indexOf('H'), -1);
                }
            }
        });
    }

    @Test
    public void testNotLikeStringMatch() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select rnd_str() name from long_sequence(2000))");

            try (RecordCursorFactory factory = select("select * from x where not name ilike 'XJ'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true, sink);
                    Assert.assertNotEquals(sink.toString().indexOf("XJ"), -1);
                }
            }
        });
    }

    @Test
    public void testSimplePatternLikeString() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table x ( s string ) ");
            compile("insert into x values ( 'foo' ), ( 'foobar' ), ( null ) ");

            assertLike("s\n", "select * from x where s ilike 'f'", false);
            assertLike("s\n", "select * from x where s ilike '_'", false);
            assertLike("s\nfoo\nfoobar\n\n", "select * from x where s ilike '%'", true);
            assertLike("s\nfoo\nfoobar\n", "select * from x where s ilike 'f%'", false);
            assertLike("s\nfoobar\n", "select * from x where s ilike '%r'", false);
            assertLike("s\nfoo\nfoobar\n", "select * from x where s ilike 'fOO%'", false);
            assertLike("s\nfoobar\n", "select * from x where s ilike '%baR'", false);
            assertLike("s\nfoo\nfoobar\n", "select * from x where s ilike '%OO%'", false);
        });
    }

    private void assertLike(String expected, String query, boolean expectSize) throws SqlException {
        assertQuery(expected, query, null, true, expectSize);
    }
}
