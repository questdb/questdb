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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.regex.MatchSymbolFunctionFactory;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MatchSymbolFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstantPatternAsyncRebuildsAfterSameCountTruncate() throws Exception {
        // Truncate + reinsert replaces the symbol dictionary with one of the SAME size, so a retained
        // key set invalidated by symbol count alone would keep answering with the previous
        // dictionary's keys. Only the symbol table generation can tell the two dictionaries apart.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL)");
            execute("INSERT INTO x VALUES ('alpha'), ('alto'), ('beta'), ('bravo')");

            try (RecordCursorFactory factory = select("SELECT sym FROM x WHERE sym ~ '^al'")) {
                MatchSymbolFunctionFactory.testSymbolKeyScans.set(0);
                MatchSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = true;
                try {
                    assertMatch(factory, "sym\nalpha\nalto\n");

                    execute("TRUNCATE TABLE x");
                    execute("INSERT INTO x VALUES ('amber'), ('delta'), ('alder'), ('foxtrot')");
                    assertMatch(factory, "sym\nalder\n");
                    Assert.assertEquals(
                            "replacing a same-size dictionary behind an async symbol-table view must rebuild the retained keys",
                            2,
                            MatchSymbolFunctionFactory.testSymbolKeyScans.get()
                    );

                    assertMatch(factory, "sym\nalder\n");
                    Assert.assertEquals(2, MatchSymbolFunctionFactory.testSymbolKeyScans.get());
                } finally {
                    MatchSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = false;
                }
            }
        });
    }

    @Test
    public void testConstantPatternLimitZeroRetainedFactoryScansOnce() throws Exception {
        // LIMIT 0 opens the base cursor before consulting the limit, and the filter is initialized
        // eagerly on every open because PreparedSymbolPatternFilter.isThreadSafe() rests on that. The
        // retained-dictionary shortcut is what keeps a repeat zero-row execution from paying the full
        // O(symbolCount) regex pass all over again.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL)");
            execute("INSERT INTO x VALUES ('alpha'), ('beta')");

            try (RecordCursorFactory factory = select("SELECT sym FROM x WHERE sym ~ '^al' LIMIT 0")) {
                MatchSymbolFunctionFactory.testSymbolKeyScans.set(0);
                MatchSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = true;
                try {
                    assertMatch(factory, "sym\n");
                    Assert.assertEquals(1, MatchSymbolFunctionFactory.testSymbolKeyScans.get());

                    assertMatch(factory, "sym\n");
                    Assert.assertEquals(
                            "a repeat zero-row open of an unchanged dictionary must not rescan it",
                            1,
                            MatchSymbolFunctionFactory.testSymbolKeyScans.get()
                    );
                } finally {
                    MatchSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = false;
                }
            }
        });
    }

    @Test
    public void testConstantPatternRebuildsAfterSameCountTruncate() throws Exception {
        // Serial control for testConstantPatternAsyncRebuildsAfterSameCountTruncate: with the parallel
        // filter off the owner function is the only key set holder.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL)");
            execute("INSERT INTO x VALUES ('alpha'), ('alto'), ('beta'), ('bravo')");

            sqlExecutionContext.setParallelFilterEnabled(false);
            try (RecordCursorFactory factory = select("SELECT sym FROM x WHERE sym ~ '^al'")) {
                MatchSymbolFunctionFactory.testSymbolKeyScans.set(0);
                MatchSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = true;
                try {
                    assertMatch(factory, "sym\nalpha\nalto\n");

                    execute("TRUNCATE TABLE x");
                    execute("INSERT INTO x VALUES ('amber'), ('delta'), ('alder'), ('foxtrot')");
                    assertMatch(factory, "sym\nalder\n");
                    Assert.assertEquals(
                            "replacing a same-size dictionary must rebuild the retained keys",
                            2,
                            MatchSymbolFunctionFactory.testSymbolKeyScans.get()
                    );

                    assertMatch(factory, "sym\nalder\n");
                    Assert.assertEquals(2, MatchSymbolFunctionFactory.testSymbolKeyScans.get());
                } finally {
                    MatchSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = false;
                }
            } finally {
                sqlExecutionContext.setParallelFilterEnabled(true);
            }
        });
    }

    @Test
    public void testConstantPatternRetainedFactoryScansOnlyWhenStateChanges() throws Exception {
        // The memo half of testConstantPatternUnchangedSeesNewSymbols: the rows alone cannot tell a
        // retained key set from a re-derived one, so count the scans. Repeat opens of an unchanged
        // dictionary must reuse the keys; any dictionary change must rebuild them.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL)");
            execute("INSERT INTO x VALUES ('alpha'), ('beta')");

            try (RecordCursorFactory factory = select("SELECT sym FROM x WHERE sym ~ '^al'")) {
                MatchSymbolFunctionFactory.testSymbolKeyScans.set(0);
                MatchSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = true;
                try {
                    assertMatch(factory, "sym\nalpha\n");
                    Assert.assertEquals(1, MatchSymbolFunctionFactory.testSymbolKeyScans.get());

                    assertMatch(factory, "sym\nalpha\n");
                    Assert.assertEquals(
                            "an unchanged dictionary must reuse the matched keys",
                            1,
                            MatchSymbolFunctionFactory.testSymbolKeyScans.get()
                    );

                    execute("INSERT INTO x VALUES ('alto'), ('bravo')");
                    assertMatch(factory, "sym\nalpha\nalto\n");
                    Assert.assertEquals(2, MatchSymbolFunctionFactory.testSymbolKeyScans.get());

                    assertMatch(factory, "sym\nalpha\nalto\n");
                    Assert.assertEquals(2, MatchSymbolFunctionFactory.testSymbolKeyScans.get());
                } finally {
                    MatchSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = false;
                }
            }
        });
    }

    @Test
    public void testConstantPatternUnchangedSeesNewSymbols() throws Exception {
        // The pattern is a compile-time constant, so nothing about the function changes between opens
        // of a retained factory. The symbol dictionary grew in between, so the key set must still be
        // rebuilt on the later open: a retention shortcut that failed to notice the append would
        // silently drop every row carrying a new symbol.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL)");
            execute("INSERT INTO x VALUES ('alpha'), ('beta')");

            try (RecordCursorFactory factory = select("SELECT sym FROM x WHERE sym ~ '^al'")) {
                assertMatch(factory, "sym\nalpha\n");

                execute("INSERT INTO x VALUES ('alto'), ('bravo')");
                assertMatch(factory, "sym\nalpha\nalto\n");
            }

            // serial control: same growth visibility without worker clones and key donation
            sqlExecutionContext.setParallelFilterEnabled(false);
            try (RecordCursorFactory factory = select("SELECT sym FROM x WHERE sym ~ '^al'")) {
                assertMatch(factory, "sym\nalpha\nalto\n");

                execute("INSERT INTO x VALUES ('alder')");
                assertMatch(factory, "sym\nalpha\nalto\nalder\n");
            } finally {
                sqlExecutionContext.setParallelFilterEnabled(true);
            }
        });
    }

    @Test
    public void testConstantPatternWorkerClonesReuseDonatedKeys() throws Exception {
        assertWorkerClonesReuseDonatedKeys(false);
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

            try (RecordCursorFactory factory = select("(select name::symbol name from x) where name ~ '^op.*'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                    TestUtils.assertEquals(expected, sink);
                }
            }
        });
    }

    @Test
    public void testNullRegex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('jjke', 'jio2', 'ope', 'nbbe', null) name from long_sequence(2000))");
            assertQuery("select * from x where name ~ null")
                    .noLeakCheck()
                    .expectSize()
                    .returns("name\n");
        });
    }

    @Test
    public void testRegexSyntaxError() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('jjke', 'jio2', 'ope', 'nbbe', null) name from long_sequence(2000))");
            try {
                assertExceptionNoLeakCheck("select * from x where name ~ 'XJ**'");
            } catch (SqlException e) {
                Assert.assertEquals(33, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "Dangling meta");
            }
        });
    }

    @Test
    public void testRuntimeConstantPatternAsyncRebuildsAfterSameCountTruncate() throws Exception {
        // Same-count truncate lock for the runtime-constant pattern: the bind value never changes, so
        // pattern comparison cannot trigger the rebuild - dictionary identity/generation must.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL)");
            execute("INSERT INTO x VALUES ('alpha'), ('alto'), ('beta'), ('bravo')");

            bindVariableService.setStr(0, "^al");
            try (RecordCursorFactory factory = select("SELECT sym FROM x WHERE sym ~ $1")) {
                MatchSymbolFunctionFactory.testSymbolKeyScans.set(0);
                MatchSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = true;
                try {
                    assertMatch(factory, "sym\nalpha\nalto\n");

                    execute("TRUNCATE TABLE x");
                    execute("INSERT INTO x VALUES ('amber'), ('delta'), ('alder'), ('foxtrot')");
                    assertMatch(factory, "sym\nalder\n");
                    Assert.assertEquals(
                            "replacing a same-size dictionary behind an async symbol-table view must rebuild the retained keys",
                            2,
                            MatchSymbolFunctionFactory.testSymbolKeyScans.get()
                    );

                    assertMatch(factory, "sym\nalder\n");
                    Assert.assertEquals(2, MatchSymbolFunctionFactory.testSymbolKeyScans.get());
                } finally {
                    MatchSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = false;
                }
            }
        });
    }

    @Test
    public void testRuntimeConstantPatternRebindRebuildsSymbolKeys() throws Exception {
        // A compiled statement outlives the values bound to it. The per-worker clones of the regex
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

            try (RecordCursorFactory factory = select("SELECT sym FROM x WHERE sym ~ $1")) {
                bindVariableService.setStr(0, "^al");
                assertMatch(factory, alRows);
                bindVariableService.setStr(0, "^b");
                assertMatch(factory, bRows);
                // back to the first pattern: a clone caching the second key set answers bRows here
                bindVariableService.setStr(0, "^al");
                assertMatch(factory, alRows);
                // a pattern matching nothing must clear the key set, not fall back to the inherited one
                bindVariableService.setStr(0, "^z");
                assertMatch(factory, noRows);
                // a null pattern clears the matcher outright
                bindVariableService.setStr(0, null);
                assertMatch(factory, noRows);
                // and a later rebind must rebuild from the null state
                bindVariableService.setStr(0, "^b");
                assertMatch(factory, bRows);
            }

            // oracle: the same predicates as constants, compiled to the const-pattern sibling
            assertQuery("SELECT sym FROM x WHERE sym ~ '^al'").noLeakCheck().returns(alRows);
            assertQuery("SELECT sym FROM x WHERE sym ~ '^b'").noLeakCheck().returns(bRows);
            assertQuery("SELECT sym FROM x WHERE sym ~ '^z'").noLeakCheck().returns(noRows);
        });
    }

    @Test
    public void testRuntimeConstantPatternRebuildsAfterSameCountTruncate() throws Exception {
        // Serial control for testRuntimeConstantPatternAsyncRebuildsAfterSameCountTruncate.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL)");
            execute("INSERT INTO x VALUES ('alpha'), ('alto'), ('beta'), ('bravo')");

            bindVariableService.setStr(0, "^al");
            sqlExecutionContext.setParallelFilterEnabled(false);
            try (RecordCursorFactory factory = select("SELECT sym FROM x WHERE sym ~ $1")) {
                MatchSymbolFunctionFactory.testSymbolKeyScans.set(0);
                MatchSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = true;
                try {
                    assertMatch(factory, "sym\nalpha\nalto\n");

                    execute("TRUNCATE TABLE x");
                    execute("INSERT INTO x VALUES ('amber'), ('delta'), ('alder'), ('foxtrot')");
                    assertMatch(factory, "sym\nalder\n");
                    Assert.assertEquals(
                            "replacing a same-size dictionary must rebuild the retained keys",
                            2,
                            MatchSymbolFunctionFactory.testSymbolKeyScans.get()
                    );

                    assertMatch(factory, "sym\nalder\n");
                    Assert.assertEquals(2, MatchSymbolFunctionFactory.testSymbolKeyScans.get());
                } finally {
                    MatchSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = false;
                }
            } finally {
                sqlExecutionContext.setParallelFilterEnabled(true);
            }
        });
    }

    @Test
    public void testRuntimeConstantPatternRetainedFactoryScansOnlyWhenStateChanges() throws Exception {
        // Full mirror of testBindVariableRetainedFactoryScansOnlyWhenStateChanges for the regex
        // operator: only a rebind or a dictionary change may cost a scan; a null pattern costs none.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL)");
            execute("INSERT INTO x VALUES ('alpha'), ('beta')");

            try (RecordCursorFactory factory = select("SELECT sym FROM x WHERE sym ~ $1")) {
                MatchSymbolFunctionFactory.testSymbolKeyScans.set(0);
                MatchSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = true;
                try {
                    bindVariableService.setStr(0, "^al");
                    assertMatch(factory, "sym\nalpha\n");
                    Assert.assertEquals(1, MatchSymbolFunctionFactory.testSymbolKeyScans.get());

                    assertMatch(factory, "sym\nalpha\n");
                    Assert.assertEquals(
                            "an unchanged bind and dictionary must reuse the matched keys",
                            1,
                            MatchSymbolFunctionFactory.testSymbolKeyScans.get()
                    );

                    bindVariableService.setStr(0, "^b");
                    assertMatch(factory, "sym\nbeta\n");
                    Assert.assertEquals(2, MatchSymbolFunctionFactory.testSymbolKeyScans.get());

                    execute("INSERT INTO x VALUES ('alto'), ('bravo')");
                    assertMatch(factory, "sym\nbeta\nbravo\n");
                    Assert.assertEquals(3, MatchSymbolFunctionFactory.testSymbolKeyScans.get());

                    assertMatch(factory, "sym\nbeta\nbravo\n");
                    Assert.assertEquals(3, MatchSymbolFunctionFactory.testSymbolKeyScans.get());

                    bindVariableService.setStr(0, null);
                    assertMatch(factory, "sym\n");
                    Assert.assertEquals(3, MatchSymbolFunctionFactory.testSymbolKeyScans.get());

                    bindVariableService.setStr(0, "^al");
                    assertMatch(factory, "sym\nalpha\nalto\n");
                    Assert.assertEquals(4, MatchSymbolFunctionFactory.testSymbolKeyScans.get());
                } finally {
                    MatchSymbolFunctionFactory.isSymbolKeyScanCounterEnabled = false;
                }
            }
        });
    }

    @Test
    public void testRuntimeConstantPatternUnchangedSeesNewSymbols() throws Exception {
        // The bind value is identical across the opens, so the pattern cannot force a key set rebuild.
        // The symbol dictionary grew in between, so the key set must still be rebuilt.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL)");
            execute("INSERT INTO x VALUES ('alpha'), ('beta')");

            bindVariableService.setStr(0, "^al");
            try (RecordCursorFactory factory = select("SELECT sym FROM x WHERE sym ~ $1")) {
                assertMatch(factory, "sym\nalpha\n");

                execute("INSERT INTO x VALUES ('alto'), ('bravo')");
                assertMatch(factory, "sym\nalpha\nalto\n");
            }

            // serial control
            sqlExecutionContext.setParallelFilterEnabled(false);
            try (RecordCursorFactory factory = select("SELECT sym FROM x WHERE sym ~ $1")) {
                assertMatch(factory, "sym\nalpha\nalto\n");

                execute("INSERT INTO x VALUES ('alder')");
                assertMatch(factory, "sym\nalpha\nalto\nalder\n");
            } finally {
                sqlExecutionContext.setParallelFilterEnabled(true);
            }
        });
    }

    @Test
    public void testRuntimeConstantPatternWorkerClonesReuseDonatedKeys() throws Exception {
        assertWorkerClonesReuseDonatedKeys(true);
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

            try (RecordCursorFactory factory = select("select * from x where name ~ '^op.*'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                    TestUtils.assertEquals(expected, sink);
                }
            }
        });
    }

    private void assertMatch(RecordCursorFactory factory, CharSequence expected) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            assertCursor(expected, cursor, factory.getMetadata(), true);
        }
    }

    private Function newMatchFunction(CountingStaticSymbolTable symbolTable, boolean isRuntimeConstant) throws SqlException {
        final ObjList<Function> args = new ObjList<>();
        args.add(new CountingStaticSymbolFunction(symbolTable));
        if (isRuntimeConstant) {
            args.add(new StrFunction() {
                @Override
                public CharSequence getStrA(Record rec) {
                    return "^a.*";
                }

                @Override
                public CharSequence getStrB(Record rec) {
                    return "^a.*";
                }

                @Override
                public boolean isRuntimeConstant() {
                    return true;
                }
            });
        } else {
            args.add(new StrConstant("^a.*"));
        }
        final IntList argPositions = new IntList();
        argPositions.add(0);
        argPositions.add(0);
        return new MatchSymbolFunctionFactory().newInstance(
                0,
                args,
                argPositions,
                configuration,
                sqlExecutionContext
        );
    }

    private void assertWorkerClonesReuseDonatedKeys(boolean isRuntimeConstant) throws Exception {
        final CountingStaticSymbolTable symbolTable = new CountingStaticSymbolTable();
        final Function prototype = newMatchFunction(symbolTable, isRuntimeConstant);
        final ObjList<Function> clones = new ObjList<>();
        try {
            prototype.init(null, sqlExecutionContext);
            for (int i = 0; i < 4; i++) {
                clones.add(newMatchFunction(symbolTable, isRuntimeConstant));
            }
            Function.init(clones, null, sqlExecutionContext, prototype);
            Assert.assertEquals(
                    (isRuntimeConstant ? "runtime-constant" : "constant")
                            + " regex clones must reuse the owner's one dictionary scan",
                    symbolTable.getSymbolCount(),
                    symbolTable.getValueOfCallCount()
            );
        } finally {
            prototype.close();
            for (int i = 0, n = clones.size(); i < n; i++) {
                clones.getQuick(i).close();
            }
        }
    }

    private static class CountingStaticSymbolFunction extends SymbolFunction {
        private final CountingStaticSymbolTable symbolTable;

        private CountingStaticSymbolFunction(CountingStaticSymbolTable symbolTable) {
            this.symbolTable = symbolTable;
        }

        @Override
        public int getInt(Record rec) {
            return 0;
        }

        @Override
        public StaticSymbolTable getStaticSymbolTable() {
            return symbolTable;
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            return symbolTable.valueOf(0);
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            return symbolTable.valueBOf(0);
        }

        @Override
        public boolean isSymbolTableStatic() {
            return true;
        }

        @Override
        public CharSequence valueBOf(int key) {
            return symbolTable.valueBOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            return symbolTable.valueOf(key);
        }
    }

    private static class CountingStaticSymbolTable implements StaticSymbolTable {
        private final ObjList<String> symbols = new ObjList<>();
        private int valueOfCallCount;

        private CountingStaticSymbolTable() {
            symbols.add("alpha");
            symbols.add("beta");
            symbols.add("alpine");
        }

        @Override
        public boolean containsNullValue() {
            return false;
        }

        @Override
        public int getSymbolCount() {
            return symbols.size();
        }

        private int getValueOfCallCount() {
            return valueOfCallCount;
        }

        @Override
        public int keyOf(CharSequence value) {
            for (int i = 0, n = symbols.size(); i < n; i++) {
                if (symbols.getQuick(i).contentEquals(value)) {
                    return i;
                }
            }
            return VALUE_NOT_FOUND;
        }

        @Override
        public CharSequence valueBOf(int key) {
            return symbols.getQuick(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            valueOfCallCount++;
            return symbols.getQuick(key);
        }
    }
}
