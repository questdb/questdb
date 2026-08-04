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

import io.questdb.PropertyKey;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Exercises the experimental native (JDK FFM + Rust {@code regex}) backend for the {@code ~}
 * operator over VARCHAR, enabled via {@code cairo.sql.varchar.regex.native.enabled}.
 * <p>
 * Parity is asserted by comparing the VARCHAR operator ({@code name ~ p}, signature {@code ~(ØS)},
 * native when available) against the STRING operator over the same data
 * ({@code cast(name as string) ~ p}, signature {@code ~(SS)}, always {@code java.util.regex}).
 * Both must return identical rows.
 * <p>
 * These assertions hold whether or not {@code libquestdbr} actually exposes the native symbols:
 * when it does not, {@code MatchVarcharFunctionFactory} transparently falls back to
 * {@code java.util.regex}, so parity is trivially preserved. When it does, the test pins the
 * native engine to JDK semantics.
 */
public class MatchVarcharNativeRegexTest extends AbstractCairoTest {

    @Override
    public void setUp() {
        node1.setProperty(PropertyKey.CAIRO_SQL_VARCHAR_REGEX_NATIVE_ENABLED, true);
        super.setUp();
    }

    @Test
    public void testNullRegex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_varchar() name from long_sequence(2000))");
            assertQuery("select * from x where name ~ null")
                    .noLeakCheck()
                    .expectSize()
                    .returns("name\n");
        });
    }

    @Test
    public void testParityWithJdkOnRandomData() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_varchar() name from long_sequence(5000))");
            // ASCII-only patterns with engine-independent semantics (no anchors / dot, whose
            // newline handling differs subtly between java.util.regex and the Rust engine).
            assertParity("[a-f][0-9]");
            assertParity("[0-9][0-9][0-9]");
            assertParity("[A-Za-z]");
            assertParity("[^0-9]");
            assertParity("abc");
        });
    }

    @Test
    public void testSimple() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (name varchar)");
            execute("insert into x values ('apple123x'),('banana'),('h3ll0'),(null),('A12'),('zf9')");
            // [a-f][0-9]: 'apple123x' -> "e1", 'zf9' -> "f9"; 'h3ll0' has no a-f char before a digit
            assertQuery("select name from x where name ~ '[a-f][0-9]'")
                    .noLeakCheck()
                    .returns("name\napple123x\nzf9\n");
        });
    }

    @Test
    public void testSyntaxError() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_varchar() name from long_sequence(10))");
            try {
                // invalid for both engines; native compile returns no handle, so we fall back to
                // java.util.regex which reports the precise syntax error and SQL position.
                assertExceptionNoLeakCheck("select * from x where name ~ 'XJ**'");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Dangling meta");
            }
        });
    }

    @Test
    public void testUnsupportedPatternFallsBackToJdk() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (name varchar)");
            execute("insert into x values ('abab'),('abc'),('xyababy'),(null),('aba')");
            // Backreferences are unsupported by the Rust engine: MatchVarcharFunctionFactory must
            // fall back to java.util.regex rather than failing or silently mismatching.
            final String jdkPredicate = "select name from x where cast(name as string) ~ '(ab)\\1'";
            final String varcharPredicate = "select name from x where name ~ '(ab)\\1'";
            assertSqlCursors(jdkPredicate, varcharPredicate);
            assertQuery("select name from x where name ~ '(ab)\\1'")
                    .noLeakCheck()
                    .returns("name\nabab\nxyababy\n");
        });
    }

    private static void assertParity(String regex) throws Exception {
        assertSqlCursors(
                "select name from x where cast(name as string) ~ '" + regex + "'",
                "select name from x where name ~ '" + regex + "'"
        );
    }
}
