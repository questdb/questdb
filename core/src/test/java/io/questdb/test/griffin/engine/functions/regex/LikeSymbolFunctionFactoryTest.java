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

import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.regex.AbstractLikeSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.regex.SymbolKeySet;
import io.questdb.std.Chars;
import io.questdb.std.NativeMemoryBudget;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectString;
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
    public void testDictionaryScansFillAndReuseSharedCache() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE scan_symbols (s SYMBOL CAPACITY 8192 CACHE)");
            execute("INSERT INTO scan_symbols SELECT 'symbol' || x FROM long_sequence(5000)");
            ObjList<String> predicates = new ObjList<>(
                    "s LIKE 'symbol%'",
                    "s LIKE '%4999'",
                    "s LIKE '%499%'",
                    "s LIKE 'sym_ol%'",
                    "s ILIKE 'SYMBOL%'",
                    "s ILIKE '%4999'",
                    "s ILIKE '%499%'",
                    "s ~ '^symbol'"
            );
            int[] counts = {5000, 1, 15, 5000, 5000, 1, 15, 5000};
            NativeMemoryBudget budget = engine.getSymbolMapCachePool().getBudget();
            long originalLimit = budget.getLimit();
            try {
                for (long limit : new long[]{1024, 1024 * 1024}) {
                    budget.setLimit(limit);
                    for (int i = 0; i < predicates.size(); i++) {
                        engine.releaseAllReaders();
                        Assert.assertEquals(0, budget.getUsedBytes());
                        try (TableReader reader = engine.getReader("scan_symbols")) {
                            SymbolMapReaderImpl symbols = (SymbolMapReaderImpl) reader.getSymbolMapReader(0);
                            Assert.assertEquals(0, symbols.getLowCacheSize());
                            Assert.assertEquals(0, symbols.getCacheSize());
                            String query = "SELECT count() FROM scan_symbols WHERE " + predicates.getQuick(i);
                            String expected = "count\n" + counts[i] + "\n";
                            assertQuery(query).noLeakCheck().noRandomAccess().expectSize().returns(expected);

                            // a slot array does not fit 1KiB, so the tiny budget caches only map keys
                            boolean isTiny = limit == 1024;
                            int lowCount = symbols.getLowCacheSize();
                            int cachedCount = symbols.getCacheSize();
                            if (isTiny) {
                                Assert.assertEquals(predicates.getQuick(i), 0, lowCount);
                                Assert.assertTrue(predicates.getQuick(i), cachedCount > 0 && cachedCount < 904);
                            } else {
                                Assert.assertEquals(predicates.getQuick(i), 4096, lowCount);
                                Assert.assertEquals(predicates.getQuick(i), 904, cachedCount);
                            }
                            long cachedBytes = budget.getUsedBytes();
                            Assert.assertEquals(symbols.getCacheAllocatedBytes() + symbols.getLowCacheAllocatedBytes(), cachedBytes);
                            Assert.assertTrue(cachedBytes > 0 && cachedBytes <= limit);
                            // the map takes scanned keys in order, so the tiny budget runs out before key 4096
                            long lowAddress = ((DirectString) symbols.valueOf(0)).lo();
                            long highAddress = ((DirectString) symbols.valueOf(4096)).lo();
                            DirectString mapped = new DirectString();
                            if (isTiny) {
                                Assert.assertEquals(((DirectString) symbols.valueOf(0, mapped)).lo(), lowAddress);
                                Assert.assertEquals(((DirectString) symbols.valueOf(4096, mapped)).lo(), highAddress);
                            } else {
                                Assert.assertNotEquals(((DirectString) symbols.valueOf(0, mapped)).lo(), lowAddress);
                                Assert.assertNotEquals(((DirectString) symbols.valueOf(4096, mapped)).lo(), highAddress);
                            }

                            assertQuery(query).noLeakCheck().noRandomAccess().expectSize().returns(expected);
                            Assert.assertEquals(lowCount, symbols.getLowCacheSize());
                            Assert.assertEquals(cachedCount, symbols.getCacheSize());
                            Assert.assertEquals(cachedBytes, budget.getUsedBytes());
                            Assert.assertEquals(lowAddress, ((DirectString) symbols.valueOf(0)).lo());
                            Assert.assertEquals(highAddress, ((DirectString) symbols.valueOf(4096)).lo());
                            TestUtils.assertEquals("symbol1", symbols.valueOf(0));
                            TestUtils.assertEquals("symbol4097", symbols.valueOf(4096));
                            TestUtils.assertEquals("symbol5000", symbols.valueOf(4999));
                        }
                    }
                }
            } finally {
                engine.releaseAllReaders();
                budget.setLimit(originalLimit);
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

    @Test
    public void testSymbolKeySetAddRequiresNonnegativeSortedUniqueKeys() {
        SymbolKeySet keys = new SymbolKeySet();
        Assert.assertThrows(AssertionError.class, () -> keys.add(-1));
        keys.add(1);
        Assert.assertThrows(AssertionError.class, () -> keys.add(1));
        Assert.assertThrows(AssertionError.class, () -> keys.add(0));
        Assert.assertEquals(1, keys.getKeys().size());
    }

    @Test
    public void testSymbolKeySetBuildAndInvalidation() {
        SymbolKeySet keys = new SymbolKeySet();
        keys.build();
        Assert.assertFalse(keys.contains(0));
        int[] values = {0, 63, 64, 127, 128};
        for (int key : values) {
            keys.add(key);
        }
        for (int key : values) {
            Assert.assertTrue(keys.contains(key));
        }
        keys.build();
        for (int key = -2; key <= 129; key++) {
            Assert.assertEquals(key == 0 || key == 63 || key == 64 || key == 127 || key == 128, keys.contains(key));
        }
        keys.add(256);
        Assert.assertTrue(keys.contains(256));
        keys.build();
        Assert.assertTrue(keys.contains(256));
        keys.clear();
        Assert.assertFalse(keys.contains(0));
        Assert.assertFalse(keys.contains(256));
        keys.add(63);
        keys.add(64);
        keys.build();
        Assert.assertFalse(keys.contains(0));
        Assert.assertFalse(keys.contains(62));
        Assert.assertTrue(keys.contains(63));
        Assert.assertTrue(keys.contains(64));
        Assert.assertFalse(keys.contains(65));
        Assert.assertFalse(keys.contains(126));
        Assert.assertFalse(keys.contains(127));
        Assert.assertFalse(keys.contains(191));
    }

    @Test
    public void testSymbolKeySetCopiesRemainIndependent() {
        SymbolKeySet source = new SymbolKeySet();
        source.add(0);
        source.add(63);
        source.build();
        SymbolKeySet target = new SymbolKeySet();
        target.add(100);
        target.build();
        target.copyFrom(source);
        Assert.assertTrue(target.contains(0));
        Assert.assertTrue(target.contains(63));
        Assert.assertFalse(target.contains(100));
        source.add(64);
        source.build();
        Assert.assertTrue(source.contains(64));
        Assert.assertFalse(target.contains(64));
        target.clear();
        target.add(128);
        target.build();
        Assert.assertTrue(source.contains(0));
        Assert.assertFalse(source.contains(128));
        target.copyFrom(target);
        Assert.assertEquals(1, target.getKeys().size());
        Assert.assertTrue(target.contains(128));
        source.clear();
        target.copyFrom(source);
        Assert.assertFalse(target.contains(128));
        Assert.assertEquals(0, target.getKeys().size());
        for (int key = 0; key < 1000; key++) {
            target.add(key);
        }
        target.build();
        source.add(70);
        source.add(100);
        source.build();
        target.copyFrom(source);
        for (int key = -1; key <= 1000; key++) {
            Assert.assertEquals(key == 70 || key == 100, target.contains(key));
        }
        source.clear();
        source.add(10);
        source.add(5_000);
        source.build();
        target.copyFrom(source);
        for (int key = -1; key <= 5_001; key++) {
            Assert.assertEquals(key == 10 || key == 5_000, target.contains(key));
        }
    }

    @Test
    public void testSymbolKeySetRebuildClearsEarlierSpan() {
        SymbolKeySet keys = new SymbolKeySet();
        for (int key = 0; key < 1000; key++) {
            keys.add(key);
        }
        keys.build();
        keys.clear();
        keys.add(0);
        keys.add(1_000_000);
        keys.build();
        keys.clear();
        keys.add(70);
        keys.add(100);
        keys.build();
        for (int key = 0; key < 1000; key++) {
            Assert.assertEquals(key == 70 || key == 100, keys.contains(key));
        }
        keys.clear();
        keys.add(5);
        keys.build();
        for (int key = 0; key < 200; key++) {
            Assert.assertEquals(key == 5, keys.contains(key));
        }
    }

    @Test
    public void testSymbolKeySetSparseAndHighKeys() {
        SymbolKeySet keys = new SymbolKeySet();
        keys.add(0);
        keys.add(Integer.MAX_VALUE);
        keys.build();
        Assert.assertTrue(keys.contains(0));
        Assert.assertTrue(keys.contains(Integer.MAX_VALUE));
        Assert.assertFalse(keys.contains(-1));
        Assert.assertFalse(keys.contains(Integer.MIN_VALUE));
        Assert.assertFalse(keys.contains(1));
        Assert.assertFalse(keys.contains(Integer.MAX_VALUE - 1));
        keys.clear();
        int base = Integer.MAX_VALUE - 65;
        int[] offsets = {0, 2, 63, 64, 65};
        for (int offset : offsets) {
            keys.add(base + offset);
        }
        keys.build();
        for (int offset = -1; offset <= 65; offset++) {
            Assert.assertEquals(offset == 0 || offset == 2 || offset >= 63, keys.contains(base + offset));
        }
        Assert.assertFalse(keys.contains(-1));
        Assert.assertFalse(keys.contains(0));
        Assert.assertEquals(base, keys.getKeys().getQuick(0));
        Assert.assertEquals(Integer.MAX_VALUE, keys.getKeys().getQuick(4));
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
