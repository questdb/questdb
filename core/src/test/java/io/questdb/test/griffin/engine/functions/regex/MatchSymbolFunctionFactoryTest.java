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
