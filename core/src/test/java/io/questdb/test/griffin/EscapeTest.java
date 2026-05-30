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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class EscapeTest extends AbstractCairoTest {

    @Test
    public void testCreateTableAsSelectWithMultipleEscapedQuote() throws Exception {
        assertQuery("select * from t")
                .ddl("create table t as (select 'a ''quoted'' text' as s from long_sequence(1))")
                .expectSize()
                .returns("s\na 'quoted' text\n");
    }

    @Test
    public void testCreateTableAsSelectWithSingleEscapedQuote() throws Exception {
        assertQuery("select * from t")
                .ddl("create table t as (select x || '''b' as s from long_sequence(3))")
                .expectSize()
                .returns("s\n1'b\n2'b\n3'b\n");
    }

    @Test
    public void testInsertWithMultipleEscapedQuotes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t ( s1 string, s2 string, sym symbol );");
            execute("insert into t values ( '1st ''', '2nd ''''', '3rd ''''''' );");
            assertSql("s1\ts2\tsym\n1st '\t2nd ''\t3rd '''\n", "select * from t");
        });
    }

    @Test
    public void testSelectWithEscapedQuoteInFunctionArg() throws Exception {
        assertQuery("select strpos('abc''def', '''' ) ")
                .ddl(null)
                .expectSize()
                .returns("strpos\n4\n");
    }

    @Test
    public void testSelectWithMultipleEscapedQuote2() throws Exception {
        assertQuery("select ' a ''quot' || 'ed'' text2', 2, 'another txt'''")
                .ddl(null)
                .expectSize()
                .returns("concat\t2\tanother txt''\n a 'quoted' text2\t2\tanother txt'\n");
    }

    @Test
    public void testSelectWithMultipleEscapedQuotedIdentifiers() throws Exception {
        assertQuery("select 'a' \"a\"\"a\", 2 \"b\"\"2\", 3.0 \"c\"\"3\"\"\"")
                .ddl(null)
                .expectSize()
                .returns("a\"\"a\tb\"\"2\tc\"\"3\"\"\na\t2\t3.0\n");
    }

    @Test
    public void testSelectWithMultipleEscapedQuotes() throws Exception {
        assertQuery("select ' a ''quot' a,  'ed'' text2' b")
                .ddl(null)
                .expectSize()
                .returns("a\tb\n a 'quot\ted' text2\n");
    }

    @Test
    public void testSelectWithSingleEscapedQuotedIdentifiers() throws Exception {
        assertQuery("select 'single' as \"quoted\"\"identifier\"")
                .ddl(null)
                .expectSize()
                .returns("quoted\"\"identifier\nsingle\n");
    }
}
