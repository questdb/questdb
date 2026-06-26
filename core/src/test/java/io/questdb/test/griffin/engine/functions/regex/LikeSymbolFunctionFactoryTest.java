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

package io.questdb.test.griffin.engine.functions.regex;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LikeSymbolFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBindVariableConcatIndexed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('aha', 'hhh') name from long_sequence(10))");

            bindVariableService.setStr(0, "h");
            try (RecordCursorFactory factory = select("select * from x where name like '%' || $1 || '%'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                    Assert.assertNotEquals(-1, sink.toString().indexOf('h'));
                }
            }
        });
    }

    @Test
    public void testBindVariableConcatNamed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('aha', 'hhh') name from long_sequence(10))");

            bindVariableService.setStr("sym", "h");
            try (RecordCursorFactory factory = select("select * from x where name like '%' || :sym || '%'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                    Assert.assertNotEquals(-1, sink.toString().indexOf('h'));
                }
            }

            assertQuery("select * from x where name like '%' || :sym || '%'")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: name ~ concat(['%',:sym::string,'%']) [case-sensitive] [state-shared]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);
        });
    }

    @Test
    public void testEmptyLike() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table x as (
                            select cast('ABCGE' as symbol) as name from long_sequence(1)
                            union
                            select cast('SBDHDJ' as symbol) as name from long_sequence(1)
                            union
                            select cast('BDGDGGG' as symbol) as name from long_sequence(1)
                            union
                            select cast('AAAAVVV' as symbol) as name from long_sequence(1)
                            )"""
            );

            assertQuery("select * from x where name like ''")
                    .noLeakCheck()
                    .returns("name\n");
        });
    }

    @Test
    public void testInvalidRegex() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table x as (
                            select cast('ABCGE' as symbol) as name from long_sequence(1)
                            union
                            select cast('SBDHDJ' as symbol) as name from long_sequence(1)
                            union
                            select cast('BDGDGGG' as symbol) as name from long_sequence(1)
                            union
                            select cast('AAAAVVV' as symbol) as name from long_sequence(1)
                            )"""
            );

            assertQuery("select * from x where name like '[][n'")
                    .noLeakCheck()
                    .returns("name\n");
        });
    }

    @Test
    public void testLikeCharacterNoMatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('a','b','c') name from long_sequence(2000))");
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
        String createTable = "CREATE TABLE myTable (name symbol)";
        String query = "SELECT * FROM myTable WHERE name LIKE '%docs\\';";
        assertQuery(query)
                .ddl(createTable)
                .fails(5, "found [tok='%docs\\', len=6] LIKE pattern must not end with escape character");
    }

    @Test
    public void testLikeEscapeAtEndRegExpFunc() throws Exception {
        String createTable = "CREATE TABLE myTable (name symbol)";
        String query = "SELECT * FROM myTable WHERE name LIKE '_%docs\\';";
        assertQuery(query)
                .ddl(createTable)
                .fails(6, "found [tok='_%docs\\', len=7] LIKE pattern must not end with escape character");
    }

    @Test
    public void testLikeEscapeOneSlashes() throws Exception {
        String createTable = "CREATE TABLE myTable (name symbol)";
        String insertRow = "INSERT INTO myTable (name) VALUES ('The path is \\_ignore');";

        String query = "SELECT * FROM myTable WHERE name LIKE 'The path is \\_ignore';";
        String expected1 = "name\n";
        String expected2 = "name\n";

        assertQuery(query)
                .ddl(createTable)
                .mutateWith(insertRow)
                .expectSize()
                .sizeMayVary()
                .returns(expected1, expected2);
    }

    @Test
    public void testLikeEscapeThreeSlashes() throws Exception {
        String createTable = "CREATE TABLE myTable (name symbol)";
        String insertRow = "INSERT INTO myTable  (name) VALUES ('The path is \\_ignore');";

        String query = "SELECT * FROM myTable WHERE name LIKE 'The path is \\\\\\_ignore';";
        String expected1 = "name\n";
        String expected2 = "name\nThe path is \\_ignore\n";

        assertQuery(query)
                .ddl(createTable)
                .mutateWith(insertRow)
                .expectSize()
                .sizeMayVary()
                .returns(expected1, expected2);
    }

    @Test
    public void testLikeEscapeTwoSlashes() throws Exception {
        String createTable = "CREATE TABLE myTable (name symbol)";
        String insertRow = "INSERT INTO myTable (name) VALUES ('The path is \\_ignore');";

        String query = "SELECT * FROM myTable WHERE name LIKE 'The path is \\\\_ignore';";
        String expected1 = "name\n";
        String expected2 = "name\nThe path is \\_ignore\n";

        assertQuery(query)
                .ddl(createTable)
                .mutateWith(insertRow)
                .expectSize()
                .sizeMayVary()
                .returns(expected1, expected2);
    }

    @Test
    public void testLikeNoMatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('a','b','c') name from long_sequence(2000))");
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
        String createTable = "CREATE TABLE myTable (name symbol)";
        String insertRow = "INSERT INTO myTable  (name) VALUES ('\\\\?\\D:\\path');";

        String query = "SELECT * FROM myTable WHERE name LIKE '\\\\\\\\_\\\\%';";
        String expected1 = "name\n";
        String expected2 = "name\n\\\\?\\D:\\path\n";

        assertQuery(query)
                .ddl(createTable)
                .mutateWith(insertRow)
                .expectSize()
                .sizeMayVary()
                .returns(expected1, expected2);
    }

    @Test
    public void testLikePercentageAtEnd() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('ABCGE' as symbol) as name from long_sequence(1)
                    union
                    select cast('SBDHDJ' as symbol) as name from long_sequence(1)
                    union
                    select cast('BDGDGGG' as symbol) as name from long_sequence(1)
                    union
                    select cast('AAAAVVV' as symbol) as name from long_sequence(1)
                    )""";
            execute(sql);
            assertQuery("select * from x where name like 'ABC%'")
                    .noLeakCheck()
                    .returns("""
                            name
                            ABCGE
                            """);
        });
    }

    @Test
    public void testLikePercentageAtStart() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('ABCGE' as symbol) as name from long_sequence(1)
                    union
                    select cast('SBDHDJ' as symbol) as name from long_sequence(1)
                    union
                    select cast('BDGDGGG' as symbol) as name from long_sequence(1)
                    union
                    select cast('AAAAVVV' as symbol) as name from long_sequence(1)
                    )""";
            execute(sql);
            assertQuery("select * from x where name like '%GGG'")
                    .noLeakCheck()
                    .returns("""
                            name
                            BDGDGGG
                            """);
        });
    }

    @Test
    public void testLikePercentageAtStartAndEnd() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('ABCGE' as symbol) as name from long_sequence(1)
                    union
                    select cast('SBDHDJ' as symbol) as name from long_sequence(1)
                    union
                    select cast('BDGDGGG' as symbol) as name from long_sequence(1)
                    union
                    select cast('AAAAVVV' as symbol) as name from long_sequence(1)
                    )""";
            execute(sql);
            assertQuery("select * from x where name like '%BCG%'")
                    .noLeakCheck()
                    .returns("""
                            name
                            ABCGE
                            """);
        });
    }

    @Test
    public void testLikeUnderscoreAndPercentage() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('ABCGE' as symbol) as name from long_sequence(1)
                    union
                    select cast('SBDHDJ' as symbol) as name from long_sequence(1)
                    union
                    select cast('BDGDGGG' as symbol) as name from long_sequence(1)
                    union
                    select cast('AAAAVVV' as symbol) as name from long_sequence(1)
                    )""";
            execute(sql);
            assertQuery("select * from x where name like '_B%'")
                    .noLeakCheck()
                    .returns("""
                            name
                            ABCGE
                            SBDHDJ
                            """);
        });
    }

    @Test
    public void testLikeUnderscoreAtStartAndEnd() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table x as (
                            select cast('ABCGE' as symbol) as name from long_sequence(1)
                            union
                            select cast('SBDHDJ' as symbol) as name from long_sequence(1)
                            union
                            select cast('BDGDGGG' as symbol) as name from long_sequence(1)
                            union
                            select cast('AAAAVVV' as symbol) as name from long_sequence(1)
                            )"""
            );
            assertQuery("select * from x where name like '_BC__'")
                    .noLeakCheck()
                    .returns("""
                            name
                            ABCGE
                            """);
        });
    }

    @Test
    public void testNonConstantExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('a','b','c') name from long_sequence(10))");
            assertQuery("select * from x where name like rnd_str('foo','bar')")
                    .fails(32, "use constant or bind variable");
        });
    }

    @Test
    public void testNonStaticSymbolTable() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    name
                    ope
                    ope
                    ope
                    ope
                    ope
                    ope
                    ope
                    ope
                    ope
                    """;
            execute("create table x as (select rnd_str('jjke', 'jio2', 'ope', 'nbbe', null) name from long_sequence(50))");

            try (RecordCursorFactory factory = select("(select name::symbol name from x) where name like '%op%'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                    TestUtils.assertEquals(expected, sink);
                }
            }
        });
    }

    @Test
    public void testNotLikeCharacterMatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('H', 'A', 'ZK') name from long_sequence(20))");
            assertQuery("select * from x where not name like 'H'")
                    .noLeakCheck()
                    .returns("""
                            name
                            A
                            ZK
                            ZK
                            ZK
                            ZK
                            A
                            A
                            A
                            ZK
                            A
                            A
                            A
                            A
                            A
                            """);
        });
    }

    @Test
    public void testNotLikeMatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('KL', 'VK', 'XJ', 'TTT') name from long_sequence(30))");
            assertQuery("select * from x where not name like 'XJ'")
                    .noLeakCheck()
                    .returns("""
                            name
                            KL
                            VK
                            TTT
                            VK
                            TTT
                            TTT
                            KL
                            KL
                            KL
                            TTT
                            VK
                            KL
                            KL
                            VK
                            VK
                            TTT
                            TTT
                            KL
                            VK
                            TTT
                            KL
                            KL
                            TTT
                            KL
                            """);
        });
    }

    @Test
    public void testNullRegex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('jjke', 'jio2', 'ope', 'nbbe', null) name from long_sequence(2000))");
            assertQuery("select * from x where name like null")
                    .noLeakCheck()
                    .expectSize()
                    .returns("name\n");
        });
    }

    @Test
    public void testSimple() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = """
                    name
                    ope
                    ope
                    ope
                    ope
                    ope
                    ope
                    ope
                    ope
                    ope
                    """;
            execute("create table x as (select rnd_symbol('jjke', 'jio2', 'ope', 'nbbe', null) name from long_sequence(50))");

            try (RecordCursorFactory factory = select("select * from x where name like '%op%'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                    TestUtils.assertEquals(expected, sink);
                }
            }
        });
    }

    @Test
    public void testSimplePatternLike() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x ( s symbol ) ");
            execute("insert into x values ( 'v' ), ( 'vv' ), ( null ) ");

            assertLike("s\nv\n", "select * from x where s like 'v'");
            assertLike("s\nv\n", "select * from x where s like '_'");
            assertLike("s\nv\nvv\n", "select * from x where s like '%'");
            assertLike("s\nv\nvv\n", "select * from x where s like 'v%'");

            assertQuery("select * from x where s ilike 'v%'")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: s ilike v% [state-shared]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select * from x where s like 'v%'")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: s like v% [state-shared]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertLike("s\nv\nvv\n", "select * from x where s like '%v'");

            assertQuery("select * from x where s like '%v'")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: s like %v [state-shared]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select * from x where s ilike '%v'")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: s ilike %v [state-shared]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertLike("s\nv\nvv\n", "select * from x where s like '%v%'");
            assertQuery("select * from x where s like '%v%'")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: s like %v% [state-shared]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select * from x where s ilike '%v%'")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: s ilike %v% [state-shared]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);
            assertLike("s\n", "select * from x where s like 'w%'");
            assertLike("s\n", "select * from x where s like '%w'");
            assertLike("s\nv\nvv\n", "select * from x where s like '%%'");
            assertLike("s\n", "select * from x where s like '%\\%'");
            assertLike("s\n", "select * from x where s like '\\_'");
        });
    }

    @Test
    public void testExactLikeOnIndexedSymbolUsesIndexScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO x VALUES ('v', 0::TIMESTAMP), ('vv', 1::TIMESTAMP), (NULL, 2::TIMESTAMP)");

            // exact LIKE without wildcards should return same result as =
            assertSql("s\tts\n" +
                    "v\t1970-01-01T00:00:00.000000Z\n", "SELECT * FROM x WHERE s LIKE 'v'");

            // verify the plan uses index scan, not table scan
            assertSql("QUERY PLAN\n" +
                            "DeferredSingleSymbolFilterPageFrame\n" +
                            "    Index forward scan on: s\n" +
                            "      filter: s=1\n" +
                            "    Frame forward scan on: x\n",
                    "EXPLAIN SELECT * FROM x WHERE s LIKE 'v'");

            // wildcard LIKE should still use table scan
            assertSql("QUERY PLAN\n" +
                            "Async Filter workers: 1\n" +
                            "  filter: s like v% [state-shared]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: x\n",
                    "EXPLAIN SELECT * FROM x WHERE s LIKE 'v%'");
        });
    }

    private void assertLike(String expected, String query) throws Exception {
        assertQuery(query)
                .noLeakCheck()
                .returns(expected);
        assertQuery(query.replace("like", "ilike"))
                .noLeakCheck()
                .returns(expected);
    }
}
