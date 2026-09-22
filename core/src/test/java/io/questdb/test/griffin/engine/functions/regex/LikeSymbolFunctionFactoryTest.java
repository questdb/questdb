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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.regex.AbstractLikeSymbolFunctionFactory;
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
    public void testBindVariableRetainedFactoryScansOnlyWhenStateChanges() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL)");
            execute("INSERT INTO x VALUES ('alpha'), ('beta')");

            try (RecordCursorFactory factory = select("SELECT sym FROM x WHERE sym LIKE $1")) {
                AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.set(0);
                AbstractLikeSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = true;
                try {
                    bindVariableService.setStr(0, "al%");
                    assertBoundLike(factory, "sym\nalpha\n");
                    Assert.assertEquals(1, AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.get());

                    assertBoundLike(factory, "sym\nalpha\n");
                    Assert.assertEquals(
                            "an unchanged bind and dictionary must reuse the matched keys",
                            1,
                            AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.get()
                    );

                    bindVariableService.setStr(0, "b%");
                    assertBoundLike(factory, "sym\nbeta\n");
                    Assert.assertEquals(2, AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.get());

                    execute("INSERT INTO x VALUES ('alto'), ('bravo')");
                    assertBoundLike(factory, "sym\nbeta\nbravo\n");
                    Assert.assertEquals(3, AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.get());

                    assertBoundLike(factory, "sym\nbeta\nbravo\n");
                    Assert.assertEquals(3, AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.get());

                    bindVariableService.setStr(0, null);
                    assertBoundLike(factory, "sym\n");
                    Assert.assertEquals(3, AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.get());

                    bindVariableService.setStr(0, "");
                    assertBoundLike(factory, "sym\n");
                    Assert.assertEquals(3, AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.get());

                    bindVariableService.setStr(0, "al%");
                    assertBoundLike(factory, "sym\nalpha\nalto\n");
                    Assert.assertEquals(4, AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.get());
                } finally {
                    AbstractLikeSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = false;
                }
            }
        });
    }

    @Test
    public void testBindVariableRetainedAsyncFactoryRebuildsAfterSameCountTruncate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL)");
            execute("INSERT INTO x VALUES ('alpha'), ('alto'), ('beta'), ('bravo')");

            try (RecordCursorFactory factory = select("SELECT sym FROM x WHERE sym LIKE $1")) {
                AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.set(0);
                AbstractLikeSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = true;
                try {
                    bindVariableService.setStr(0, "al%");
                    assertBoundLike(factory, "sym\nalpha\nalto\n");
                    planSink.clear();
                    factory.toPlan(planSink);
                    TestUtils.assertContains(planSink.getSink(), "Async Filter workers: 1");
                    TestUtils.assertContains(planSink.getSink(), "[state-shared]");

                    execute("TRUNCATE TABLE x");
                    execute("INSERT INTO x VALUES ('amber'), ('delta'), ('alder'), ('foxtrot')");
                    assertBoundLike(factory, "sym\nalder\n");
                    Assert.assertEquals(
                            "replacing a same-size dictionary behind an async symbol-table view must rebuild the retained keys",
                            2,
                            AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.get()
                    );

                    assertBoundLike(factory, "sym\nalder\n");
                    Assert.assertEquals(2, AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.get());
                } finally {
                    AbstractLikeSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = false;
                }
            }
        });
    }

    @Test
    public void testBindVariableRetainedFactoryRebuildsAfterSameCountTruncate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL)");
            execute("INSERT INTO x VALUES ('alpha'), ('alto'), ('beta'), ('bravo')");

            sqlExecutionContext.setParallelFilterEnabled(false);
            try (RecordCursorFactory factory = select("SELECT sym FROM x WHERE sym LIKE $1")) {
                AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.set(0);
                AbstractLikeSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = true;
                try {
                    bindVariableService.setStr(0, "al%");
                    assertBoundLike(factory, "sym\nalpha\nalto\n");

                    execute("TRUNCATE TABLE x");
                    execute("INSERT INTO x VALUES ('amber'), ('delta'), ('alder'), ('foxtrot')");
                    assertBoundLike(factory, "sym\nalder\n");
                    Assert.assertEquals(
                            "replacing a same-size dictionary must rebuild the retained keys",
                            2,
                            AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.get()
                    );

                    assertBoundLike(factory, "sym\nalder\n");
                    Assert.assertEquals(2, AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.get());
                } finally {
                    AbstractLikeSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = false;
                }
            } finally {
                sqlExecutionContext.setParallelFilterEnabled(true);
            }
        });
    }

    @Test
    public void testBindVariableRebindRebuildsSymbolKeys() throws Exception {
        // A compiled statement outlives the values bound to it. The per-worker clones of the LIKE
        // predicate inherit the owner's matched symbol keys instead of re-deriving them, so a clone
        // that kept the PREVIOUS pattern's keys would answer the next execution with the previous
        // pattern's rows - silently, and only on the frames that worker happened to take. Re-bind $1
        // across opens of the same factory and cross-check every answer against the constant-pattern
        // oracle, which compiles to a different function class and shares no state with this one.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL)");
            execute("INSERT INTO x VALUES ('alpha'), ('alpine'), ('beta'), ('bravo'), ('gamma')");

            final String alRows = "sym\nalpha\nalpine\n";
            final String bRows = "sym\nbeta\nbravo\n";
            final String noRows = "sym\n";

            try (RecordCursorFactory factory = select("SELECT sym FROM x WHERE sym LIKE $1")) {
                bindVariableService.setStr(0, "al%");
                assertBoundLike(factory, alRows);
                bindVariableService.setStr(0, "b%");
                assertBoundLike(factory, bRows);
                // back to the first pattern: a clone caching the second key set answers bRows here
                bindVariableService.setStr(0, "al%");
                assertBoundLike(factory, alRows);
                // a pattern matching nothing must clear the key set, not fall back to the inherited one
                bindVariableService.setStr(0, "z%");
                assertBoundLike(factory, noRows);
            }

            // oracle: the same three predicates as constants, compiled to the Const* siblings
            assertQuery("SELECT sym FROM x WHERE sym LIKE 'al%'").noLeakCheck().returns(alRows);
            assertQuery("SELECT sym FROM x WHERE sym LIKE 'b%'").noLeakCheck().returns(bRows);
            assertQuery("SELECT sym FROM x WHERE sym LIKE 'z%'").noLeakCheck().returns(noRows);
        });
    }

    @Test
    public void testBindVariableUnchangedSeesNewSymbols() throws Exception {
        // The bind value is identical across the two opens, so escapeSpecialChars() returns null out
        // of its lastPattern memo and no regex is recompiled. The symbol dictionary grew in between,
        // so the key set must still be rebuilt: donating the owner's keys to the worker clones is
        // only safe because the OWNER re-derives them on every open, memo hit or not.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL)");
            execute("INSERT INTO x VALUES ('alpha'), ('beta')");

            try (RecordCursorFactory factory = select("SELECT sym FROM x WHERE sym LIKE $1")) {
                bindVariableService.setStr(0, "al%");
                assertBoundLike(factory, "sym\nalpha\n");

                execute("INSERT INTO x VALUES ('alto'), ('bravo')");
                assertBoundLike(factory, "sym\nalpha\nalto\n");
            }
        });
    }

    @Test
    public void testBindVariableWorkerClonesInheritSymbolKeys() throws Exception {
        // BindLikeStaticSymbolTableFunction reports isThreadSafe() == value.isThreadSafe(), which is
        // false for a plain SymbolColumn, so the async filter compiles one clone per shared worker and
        // donates the owner's matched key set to each of them. Opening the cursor must therefore scan
        // the 10_000-entry dictionary exactly ONCE, not once more per clone. The [state-shared] plan
        // marker keeps the count from being vacuous: it is written by offerStateTo(), so without it no
        // clone exists and a single scan proves nothing.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT rnd_symbol(10_000, 8, 12, 0) sym FROM long_sequence(10_000))");

            bindVariableService.setStr(0, "a%");
            assertQuery("SELECT sym FROM x WHERE sym LIKE $1")
                    .noLeakCheck()
                    .assertsPlanContaining("[state-shared]");

            Assert.assertEquals(
                    "opening the cursor must scan the symbol dictionary once, not once per worker clone",
                    1,
                    countSymbolKeyScans("SELECT sym FROM x WHERE sym LIKE $1")
            );
        });
    }

    @Test
    public void testBindVariableWorkerClonesInheritSymbolKeysParallelFilterDisabled() throws Exception {
        // Control for testBindVariableWorkerClonesInheritSymbolKeys: with the parallel filter off there
        // is no clone and no donation, so the owner's single scan is the whole cost. Both modes must
        // land on the same count - that is what makes the donated key set free rather than merely
        // cheap.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT rnd_symbol(10_000, 8, 12, 0) sym FROM long_sequence(10_000))");

            sqlExecutionContext.setParallelFilterEnabled(false);
            try {
                bindVariableService.setStr(0, "a%");
                assertQuery("SELECT sym FROM x WHERE sym LIKE $1")
                        .noLeakCheck()
                        .assertsPlanNotContaining("[state-shared]");

                Assert.assertEquals(
                        "the serial filter owns the only key set and must scan the dictionary once",
                        1,
                        countSymbolKeyScans("SELECT sym FROM x WHERE sym LIKE $1")
                );
            } finally {
                sqlExecutionContext.setParallelFilterEnabled(true);
            }
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

    private void assertBoundLike(RecordCursorFactory factory, CharSequence expected) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            assertCursor(expected, cursor, factory.getMetadata(), true);
        }
    }

    private void assertLike(String expected, String query) throws Exception {
        assertQuery(query)
                .noLeakCheck()
                .returns(expected);
        assertQuery(query.replace("like", "ilike"))
                .noLeakCheck()
                .returns(expected);
    }

    private long countSymbolKeyScans(String query) throws SqlException {
        try (RecordCursorFactory factory = select(query)) {
            AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.set(0);
            AbstractLikeSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = true;
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                }
            } finally {
                AbstractLikeSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = false;
            }
            return AbstractLikeSymbolFunctionFactory.testSymbolKeyScans.get();
        }
    }
}
