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
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class LikeEscapeClauseTest extends AbstractCairoTest {

    @Test
    public void testIssue2623Reproducer() throws Exception {
        assertMemoryLeak(() -> assertQuery("select 'quest' like 'quest' escape 'Z'")
                .expectSize()
                                        .returns("column\ntrue\n"));
    }

    @Test
    public void testLikeCustomEscapeUnderscore() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select 'a_b' like 'a!_b' escape '!'")
                    .expectSize()
                                        .returns("column\ntrue\n");
            assertQuery("select 'axb' like 'a!_b' escape '!'")
                    .expectSize()
                                        .returns("column\nfalse\n");
        });
    }

    @Test
    public void testLikeCustomEscapePercent() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select 'a%b' like 'a!%b' escape '!'")
                    .expectSize()
                                        .returns("column\ntrue\n");
            assertQuery("select 'axyb' like 'a!%b' escape '!'")
                    .expectSize()
                                        .returns("column\nfalse\n");
        });
    }

    @Test
    public void testLikeEscapedEscapeCharItself() throws Exception {
        assertMemoryLeak(() -> assertQuery("select 'a!b' like 'a!!b' escape '!'")
                .expectSize()
                                        .returns("column\ntrue\n"));
    }

    @Test
    public void testLikeDefaultEscapeStillBackslash() throws Exception {
        assertMemoryLeak(() -> {
            // without an ESCAPE clause '!' is an ordinary literal, '_' stays a wildcard
            // ('a!xb' matches 'a!_b': '!' literal, '_' wildcard matching 'x')
            assertQuery("select 'a!xb' like 'a!_b'")
                                        .expectSize()
                                        .returns("column\ntrue\n");
            // '!_' does NOT act as an escaped underscore without ESCAPE: '!' is literal,
            // '_' is wildcard, so 'a_b' does not match pattern 'a!_b' (needs literal '!')
            assertQuery("select 'a_b' like 'a!_b'")
                                        .expectSize()
                                        .returns("column\nfalse\n");
        });
    }

    @Test
    public void testILikeCustomEscape() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select 'A_B' ilike 'a!_b' escape '!'")
                    .expectSize()
                                        .returns("column\ntrue\n");
            assertQuery("select 'AXB' ilike 'a!_b' escape '!'")
                    .expectSize()
                                        .returns("column\nfalse\n");
        });
    }

    @Test
    public void testEscapeKeywordIsCaseInsensitive() throws Exception {
        assertMemoryLeak(() -> assertQuery("select 'a_b' like 'a!_b' ESCAPE '!'")
                .expectSize()
                                        .returns("column\ntrue\n"));
    }

    @Test
    public void testEscapeCharCanBeRegexMetachar() throws Exception {
        assertMemoryLeak(() -> {
            // '.' is the escape character here, so '..' is a literal dot, not a wildcard
            assertQuery("select 'a.b' like 'a..b' escape '.'")
                    .expectSize()
                                        .returns("column\ntrue\n");
            assertQuery("select 'axb' like 'a..b' escape '.'")
                    .expectSize()
                                        .returns("column\nfalse\n");
        });
    }

    @Test
    public void testEscapeInWhereClauseOnStringColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select cast('a_b' as string) as name from long_sequence(1) " +
                    "union all select cast('axb' as string) from long_sequence(1) " +
                    "union all select cast('a%b' as string) from long_sequence(1))");
            assertQuery("select * from x where name like 'a!_b' escape '!'")
                                        .returns("name\na_b\n");
            assertQuery("select * from x where name like 'a!%b' escape '!'")
                                        .returns("name\na%b\n");
        });
    }

    @Test
    public void testEscapeInWhereClauseOnSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select cast('a_b' as symbol) as name from long_sequence(1) " +
                    "union all select cast('axb' as symbol) from long_sequence(1) " +
                    "union all select cast('a%b' as symbol) from long_sequence(1))");
            assertQuery("select * from x where name like 'a!_b' escape '!'")
                                        .returns("name\na_b\n");
            assertQuery("select * from x where name like 'a!%b' escape '!'")
                                        .returns("name\na%b\n");
            assertQuery("select * from x where name ilike 'A!_B' escape '!'")
                                        .returns("name\na_b\n");
        });
    }

    @Test
    public void testEscapeInWhereClauseOnVarcharColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select cast('a_b' as varchar) as name from long_sequence(1) " +
                    "union all select cast('axb' as varchar) from long_sequence(1) " +
                    "union all select cast('a%b' as varchar) from long_sequence(1))");
            assertQuery("select * from x where name like 'a!_b' escape '!'")
                                        .returns("name\na_b\n");
            assertQuery("select * from x where name like 'a!%b' escape '!'")
                                        .returns("name\na%b\n");
        });
    }

    @Test
    public void testEscapeWithBindVariablePattern() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.setStr(0, "a!_b");
            try (RecordCursorFactory factory = select("select 'a_b' like $1 escape '!'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                    Assert.assertTrue(sink.toString().contains("true"));
                }
            }

            // wildcards keep working alongside a custom escape character
            bindVariableService.setStr(0, "a_b");
            try (RecordCursorFactory factory = select("select 'axb' like $1 escape '!'")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                    Assert.assertTrue(sink.toString().contains("true"));
                }
            }
        });
    }

    @Test
    public void testEscapeComposedWithOtherPredicates() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select 'a_b' like 'a!_b' escape '!' and 'x' = 'x'")
                                        .expectSize()
                                        .returns("column\ntrue\n");
            assertQuery("select 'a_b' like 'a!_b' escape '!' or false")
                                        .expectSize()
                                        .returns("column\ntrue\n");
            assertQuery("select not ('a_b' like 'a!_b' escape '!')")
                    .expectSize()
                                        .returns("column\nfalse\n");
        });
    }

    @Test
    public void testEscapeCharFromVarcharCast() throws Exception {
        assertMemoryLeak(() -> assertQuery("select 'a_b' like 'a!_b' escape cast('!' as varchar)")
                .expectSize()
                                        .returns("column\ntrue\n"));
    }

    @Test
    public void testTrailingEscapeCharIsAnError() throws Exception {
        assertMemoryLeak(() -> assertException(
                "select 'abc' like 'abc!' escape '!'",
                3,
                "LIKE pattern must not end with escape character"
        ));
    }

    @Test
    public void testEmptyEscapeIsAnError() throws Exception {
        assertMemoryLeak(() -> assertException(
                "select 'a' like 'a' escape ''",
                27,
                "ESCAPE expression must be a single character"
        ));
    }

    @Test
    public void testMultiCharEscapeIsAnError() throws Exception {
        assertMemoryLeak(() -> assertException(
                "select 'a' like 'a' escape '!!'",
                27,
                "ESCAPE expression must be a single character"
        ));
    }

    @Test
    public void testNullEscapeIsAnError() throws Exception {
        assertMemoryLeak(() -> assertException(
                "select 'a' like 'a' escape cast(null as string)",
                27,
                "ESCAPE expression must be a single character"
        ));
    }

    @Test
    public void testNonConstantEscapeIsAnError() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast('!' as string) e from long_sequence(1))");
            assertException(
                    "select 'a_b' like 'a!_b' escape e from x",
                    32,
                    "ESCAPE expression must be a constant"
            );
        });
    }

    @Test
    public void testUnattachedEscapeIsAParseError() throws Exception {
        assertMemoryLeak(() -> assertException("select 'a' escape '!'", 18, "',', 'from' or 'over' expected"));
    }
}
