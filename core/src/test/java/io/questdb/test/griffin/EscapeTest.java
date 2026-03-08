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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class EscapeTest extends AbstractCairoTest {

    @Test
    public void testCreateTableAsSelectWithMultipleEscapedQuote() throws Exception {
        assertQuery("s\na 'quoted' text\n",
                "select * from t",
                "create table t as (select 'a ''quoted'' text' as s from long_sequence(1))", null, true, true);
    }

    @Test
    public void testCreateTableAsSelectWithSingleEscapedQuote() throws Exception {
        assertQuery("s\n1'b\n2'b\n3'b\n",
                "select * from t",
                "create table t as (select x || '''b' as s from long_sequence(3))", null, true, true);
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
        assertQuery("strpos\n4\n",
                "select strpos('abc''def', '''' ) ",
                null, null, true, true);
    }

    @Test
    public void testSelectWithMultipleEscapedQuote2() throws Exception {
        assertQuery("concat\t2\tanother txt''\n a 'quoted' text2\t2\tanother txt'\n",
                "select ' a ''quot' || 'ed'' text2', 2, 'another txt'''",
                null, null, true, true);
    }

    @Test
    public void testSelectWithMultipleEscapedQuotedIdentifiers() throws Exception {
        assertQuery("a\"\"a\tb\"\"2\tc\"\"3\"\"\na\t2\t3.0\n",
                "select 'a' \"a\"\"a\", 2 \"b\"\"2\", 3.0 \"c\"\"3\"\"\"",
                null, null, true, true);
    }

    @Test
    public void testSelectWithMultipleEscapedQuotes() throws Exception {
        assertQuery("a\tb\n a 'quot\ted' text2\n",
                "select ' a ''quot' a,  'ed'' text2' b",
                null, null, true, true);
    }

    @Test
    public void testSelectWithSingleEscapedQuotedIdentifiers() throws Exception {
        assertQuery("quoted\"\"identifier\nsingle\n",
                "select 'single' as \"quoted\"\"identifier\"",
                null, null, true, true);
    }
}
