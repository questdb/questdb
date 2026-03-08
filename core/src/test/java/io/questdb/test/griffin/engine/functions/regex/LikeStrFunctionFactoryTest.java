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

package io.questdb.test.griffin.engine.functions.regex;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class LikeStrFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBindVariableConcatIndexed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_str() name from long_sequence(2000))");

            bindVariableService.setStr(0, "H");
            try (RecordCursorFactory factory = select("select * from x where name like '%' || $1 || '%'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                    Assert.assertNotEquals(-1, sink.toString().indexOf('H'));
                }
            }
        });
    }

    @Test
    public void testBindVariableConcatNamed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_str() name from long_sequence(2000))");

            bindVariableService.setStr("str", "H");
            try (RecordCursorFactory factory = select("select * from x where name like '%' || :str || '%'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                    Assert.assertNotEquals(-1, sink.toString().indexOf('H'));
                }
            }
        });
    }

    @Test
    public void testEmptyLike() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (\n" +
                            "select cast('ABCGE' as string) as name from long_sequence(1)\n" +
                            "union\n" +
                            "select cast('SBDHDJ' as string) as name from long_sequence(1)\n" +
                            "union\n" +
                            "select cast('BDGDGGG' as string) as name from long_sequence(1)\n" +
                            "union\n" +
                            "select cast('AAAAVVV' as string) as name from long_sequence(1)\n" +
                            ")"
            );

            assertSql(
                    "name\n",
                    "select * from x where name like ''"
            );
        });
    }

    @Test
    public void testInvalidRegex() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (\n" +
                            "select cast('ABCGE' as string) as name from long_sequence(1)\n" +
                            "union\n" +
                            "select cast('SBDHDJ' as string) as name from long_sequence(1)\n" +
                            "union\n" +
                            "select cast('BDGDGGG' as string) as name from long_sequence(1)\n" +
                            "union\n" +
                            "select cast('AAAAVVV' as string) as name from long_sequence(1)\n" +
                            ")"
            );

            assertSql(
                    "name\n",
                    "select * from x where name like '[][n'"
            );
        });
    }

    @Test
    public void testLikeCharacterNoMatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_str() name from long_sequence(2000))");
            try (RecordCursorFactory factory = select("select * from x where name like 'H'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                    Assert.assertEquals(-1, Chars.indexOf(sink, 'H'));
                }
            }
        });
    }

    @Test
    public void testLikeEscapeAtEndRegConstFunc() throws Exception {
        String createTable = "CREATE TABLE myTable (name string)";
        String insertRow = "INSERT INTO myTable (name) VALUES ('.\\docs\\');";

        String query = "SELECT * FROM myTable WHERE name LIKE '%docs\\';";
        String expected1 = "name\n";
        String expected2 = "";
        assertMemoryLeak(() -> {
            try {
                assertQueryNoLeakCheck(expected1, query, createTable, null, insertRow, expected2, true, true, true);
                Assert.fail();
            } catch (SqlException e) {
                String expectedMessage = "[5] found [tok='%docs\\', len=6] LIKE pattern must not end with escape character";
                String actualMessage = e.getMessage();
                assertTrue(actualMessage.contains(expectedMessage));
            }
        });
    }

    @Test
    public void testLikeEscapeAtEndRegExpFunc() throws Exception {
        String createTable = "CREATE TABLE myTable (name string)";
        String insertRow = "INSERT INTO myTable  (name) VALUES ('.\\docs\\');";

        String query = "SELECT * FROM myTable WHERE name LIKE '_%docs\\';";
        String expected1 = "name\n";
        String expected2 = "";
        assertMemoryLeak(() -> {
            try {
                assertQueryNoLeakCheck(expected1, query, createTable, null, insertRow, expected2, true, true, true);
                Assert.fail();
            } catch (SqlException e) {
                String expectedMessage = "[6] found [tok='_%docs\\', len=7] LIKE pattern must not end with escape character";
                String actualMessage = e.getMessage();
                assertTrue(actualMessage.contains(expectedMessage));
            }
        });
    }

    @Test
    public void testLikeEscapeOneSlashes() throws Exception {
        String createTable = "CREATE TABLE myTable (name string)";
        String insertRow = "INSERT INTO myTable  (name) VALUES ('The path is \\_ignore');";

        String query = "SELECT * FROM myTable WHERE name LIKE 'The path is \\_ignore';";
        String expected1 = "name\n";
        String expected2 = "name\n";

        assertQuery(expected1, query, createTable, null, insertRow, expected2, true, true, true);
    }

    @Test
    public void testLikeEscapeThreeSlashes() throws Exception {
        String createTable = "CREATE TABLE myTable (name string)";
        String insertRow = "INSERT INTO myTable  (name) VALUES ('The path is \\_ignore');";

        String query = "SELECT * FROM myTable WHERE name LIKE 'The path is \\\\\\_ignore';";
        String expected1 = "name\n";
        String expected2 = "name\nThe path is \\_ignore\n";

        assertQuery(expected1, query, createTable, null, insertRow, expected2, true, true, true);
    }

    @Test
    public void testLikeEscapeTwoSlashes() throws Exception {
        String createTable = "CREATE TABLE myTable (name string)";
        String insertRow = "INSERT INTO myTable  (name) VALUES ('The path is \\_ignore');";

        String query = "SELECT * FROM myTable WHERE name LIKE 'The path is \\\\_ignore';";
        String expected1 = "name\n";
        String expected2 = "name\nThe path is \\_ignore\n";

        assertQuery(expected1, query, createTable, null, insertRow, expected2, true, true, true);
    }

    @Test
    public void testLikeNoMatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_str() name from long_sequence(2000))");
            try (RecordCursorFactory factory = select("select * from x where name like 'XJ'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                    Assert.assertEquals(-1, sink.toString().indexOf("XJ"));
                }
            }
        });
    }

    @Test
    public void testLikeNotRealEscape() throws Exception {
        String createTable = "CREATE TABLE myTable (name string)";
        String insertRow = "INSERT INTO myTable  (name) VALUES ('\\\\?\\D:\\path');";

        String query = "SELECT * FROM myTable WHERE name LIKE '\\\\\\\\_\\\\%';";
        String expected1 = "name\n";
        String expected2 = "name\n\\\\?\\D:\\path\n";

        assertQuery(expected1, query, createTable, null, insertRow, expected2, true, true, true);
    }

    @Test
    public void testLikePercentageAtEnd() throws Exception {
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
            execute(sql);
            assertSql(
                    "name\n" +
                            "ABCGE\n",
                    "select * from x where name like 'ABC%'"
            );
        });
    }

    @Test
    public void testLikePercentageAtStart() throws Exception {
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
            execute(sql);
            assertSql(
                    "name\n" +
                            "BDGDGGG\n",
                    "select * from x where name like '%GGG'"
            );
        });
    }

    @Test
    public void testLikePercentageAtStartAndEnd() throws Exception {
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
            execute(sql);
            assertSql(
                    "name\n" +
                            "ABCGE\n",
                    "select * from x where name like '%BCG%'"
            );
        });
    }

    @Test
    public void testLikeUnderscoreAndPercentage() throws Exception {
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
            execute(sql);
            assertSql(
                    "name\n" +
                            "ABCGE\n" +
                            "SBDHDJ\n",
                    "select * from x where name like '_B%'"
            );
        });
    }

    @Test
    public void testLikeUnderscoreAtStartAndEnd() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (\n" +
                            "select cast('ABCGE' as string) as name from long_sequence(1)\n" +
                            "union\n" +
                            "select cast('SBDHDJ' as string) as name from long_sequence(1)\n" +
                            "union\n" +
                            "select cast('BDGDGGG' as string) as name from long_sequence(1)\n" +
                            "union\n" +
                            "select cast('AAAAVVV' as string) as name from long_sequence(1)\n" +
                            ")"
            );
            assertSql(
                    "name\n" +
                            "ABCGE\n",
                    "select * from x where name like '_BC__'"
            );
        });
    }

    @Test
    public void testNonConstantExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_str() name from long_sequence(10))");
            assertException("select * from x where name like rnd_str('foo','bar')", 32, "use constant or bind variable");
        });
    }

    @Test
    public void testNotLikeCharacterMatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_str('H', 'A', 'ZK') name from long_sequence(20))");
            assertSql(
                    "name\n" +
                            "A\n" +
                            "ZK\n" +
                            "ZK\n" +
                            "ZK\n" +
                            "ZK\n" +
                            "A\n" +
                            "A\n" +
                            "A\n" +
                            "ZK\n" +
                            "A\n" +
                            "A\n" +
                            "A\n" +
                            "A\n" +
                            "A\n",
                    "select * from x where not name like 'H'"
            );
        });
    }

    @Test
    public void testNotLikeMatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_str('KL', 'VK', 'XJ', 'TTT') name from long_sequence(30))");
            assertSql(
                    "name\n" +
                            "KL\n" +
                            "VK\n" +
                            "TTT\n" +
                            "VK\n" +
                            "TTT\n" +
                            "TTT\n" +
                            "KL\n" +
                            "KL\n" +
                            "KL\n" +
                            "TTT\n" +
                            "VK\n" +
                            "KL\n" +
                            "KL\n" +
                            "VK\n" +
                            "VK\n" +
                            "TTT\n" +
                            "TTT\n" +
                            "KL\n" +
                            "VK\n" +
                            "TTT\n" +
                            "KL\n" +
                            "KL\n" +
                            "TTT\n" +
                            "KL\n",
                    "select * from x where not name like 'XJ'"
            );
        });
    }

    @Test
    public void testSimplePatternLike() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x ( s string ) ");
            execute("insert into x values ( 'v' ), ( 'vv' ), ( null ) ");

            assertLike("s\nv\n", "select * from x where s like 'v'");
            assertLike("s\nv\n", "select * from x where s like '_'");
            assertLike("s\nv\nvv\n", "select * from x where s like '%'");
            assertLike("s\nv\nvv\n", "select * from x where s like 'v%'");
            assertLike("s\nv\nvv\n", "select * from x where s like '%v'");
            assertLike("s\nv\nvv\n", "select * from x where s like '%v%'");
            assertLike("s\n", "select * from x where s like 'w%'");
            assertLike("s\n", "select * from x where s like '%w'");
            assertLike("s\nv\nvv\n", "select * from x where s like '%%'");
            assertLike("s\n", "select * from x where s like '%\\%'");
            assertLike("s\n", "select * from x where s like '\\_'");
        });
    }

    private void assertLike(String expected, String query) throws Exception {
        assertQueryNoLeakCheck(expected, query, null, true, false);
        assertQueryNoLeakCheck(expected, query.replace("like", "ilike"), null, true, false);
    }
}
