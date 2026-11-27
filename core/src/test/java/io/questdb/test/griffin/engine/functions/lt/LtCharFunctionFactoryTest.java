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

package io.questdb.test.griffin.engine.functions.lt;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.constants.CharConstant;
import io.questdb.griffin.engine.functions.lt.LtCharFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class LtCharFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testAll() throws SqlException {
        call('a', 'z').andAssert(true);
        call('B', 'B').andAssert(false);
        final char nullChar = CharConstant.ZERO.getChar(null);
        call(nullChar, '7').andAssert(false);
        call('0', nullChar).andAssert(false);
        call(nullChar, nullChar).andAssert(false);
    }

    @Test
    public void testNullCharConstantMatchesBindVariable() throws Exception {
        final String expected = """
                a\tb\tc
                false\tfalse\tfalse
                """;
        assertQuery("SELECT 0::CHAR <= null a, null >= 0::CHAR b, 0::CHAR >= 0::CHAR c")
                .expectSize()
                .returns(expected);
        bindVariableService.setChar(0, (char) 0);
        assertQuery("SELECT $1 <= null a, null >= $1 b, $1 >= $1 c")
                .expectSize()
                .returns(expected);
    }

    @Test
    public void testNullCharConstantVersusSymbol() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("SELECT ''::SYMBOL < null::CHAR a, null::CHAR > ''::SYMBOL b")
                    .noLeakCheck()
                    .expectSize()
                    .returns("a\tb\nfalse\tfalse\n");
            execute("CREATE TABLE x (s SYMBOL)");
            execute("INSERT INTO x VALUES (''), ('a'), (null)");
            assertQuery("x WHERE s < null::CHAR OR null::CHAR > s")
                    .noLeakCheck()
                    .returns("s\n");
        });
    }

    @Test
    public void testNullOrderedAgainstCharColumn() throws Exception {
        // CHAR ordering never holds for NULL, whichever side the NULL literal stands on
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (c CHAR)");
            execute("INSERT INTO x VALUES ('a'), (null)");
            assertQuery("SELECT c, null >= c a, c <= null b, null <= c d, c >= null e, null < c f, c > null g FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            c\ta\tb\td\te\tf\tg
                            a\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse
                            \tfalse\tfalse\tfalse\tfalse\tfalse\tfalse
                            """);
        });
    }

    @Test
    public void testNullOrderedAgainstCharEverySpelling() throws Exception {
        // An untyped NULL takes the CHAR type of the other operand, as PostgreSQL types an unknown literal, and
        // CHAR ordering never holds for NULL: no operator, side or spelling of the CHAR operand yields true.
        assertMemoryLeak(() -> {
            final String ops = "null >= %1$s a, %1$s <= null b, null <= %1$s c, %1$s >= null d,"
                    + " null > %1$s e, %1$s < null f, null < %1$s g, %1$s > null h, typeOf(null > %1$s) t";
            final String header = "a\tb\tc\td\te\tf\tg\th\tt\n";
            final String allFalse = "false\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\tBOOLEAN\n";
            execute("CREATE TABLE x (c CHAR)");
            execute("INSERT INTO x VALUES ('a'), (null)");
            assertQuery("SELECT " + String.format(ops, "c") + " FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .returns(header + allFalse + allFalse);
            assertQuery("x WHERE null >= c OR null <= c OR null > c OR null < c")
                    .noLeakCheck()
                    .returns("c\n");
            assertQuery("SELECT " + String.format(ops, "null::CHAR"))
                    .noLeakCheck()
                    .expectSize()
                    .returns(header + allFalse);
            bindVariableService.clear();
            bindVariableService.setChar(0, 'a');
            assertQuery("SELECT " + String.format(ops, "$1"))
                    .noLeakCheck()
                    .expectSize()
                    .returns(header + allFalse);
            bindVariableService.clear();
            bindVariableService.setChar(0);
            assertQuery("SELECT " + String.format(ops, "$1"))
                    .noLeakCheck()
                    .expectSize()
                    .returns(header + allFalse);
        });
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new LtCharFunctionFactory();
    }
}
