/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class VarcharFunctionsTest extends AbstractCairoTest {

    @Test
    public void testLtrim() throws Exception {
        assertQuery(
                "k\tltrim\n" +
                        "  abc\tabc\n" +
                        "  abc\tabc\n" +
                        "abc  \tabc  \n" +
                        "   \t\n" +
                        "   \t\n",
                "select k, ltrim(k) from x",
                "create table x as (select rnd_varchar('  abc', 'abc  ', '   ') k from long_sequence(5))",
                null, true, true
        );
    }

    @Test
    public void testPosition() throws Exception {
        assertQuery(
                "k\tposition\n" +
                        "xa\t2\n" +
                        "aax\t1\n" +
                        "xx\t0\n" +
                        "\t0\n" +
                        "xx\t0\n",
                "select k, position(k, 'a') from x",
                "create table x as (select rnd_varchar('xa', 'xx', 'aax', '') k from long_sequence(5))",
                null, true, true);
    }

    @Test
    public void testReplace() throws Exception {
        assertQuery(
                "k\treplace\n" +
                        "xa\txb\n" +
                        "aax\tbbx\n" +
                        "xx\txx\n" +
                        "\t\n" +
                        "xx\txx\n",
                "select k, replace(k, 'a', 'b') from x",
                "create table x as (select rnd_varchar('xa', 'xx', 'aax', '') k from long_sequence(5))",
                null, true, true);
    }

    @Test
    public void testRtrim() throws Exception {
        assertQuery(
                "k\trtrim\n" +
                        "  abc\t  abc\n" +
                        "  abc\t  abc\n" +
                        "abc  \tabc\n" +
                        "   \t\n" +
                        "   \t\n",
                "select k, rtrim(k) from x",
                "create table x as (select rnd_varchar('  abc', 'abc  ', '   ') k from long_sequence(5))",
                null, true, true
        );
    }

    @Test
    public void testStrpos() throws Exception {
        assertQuery(
                "k\tstrpos\n" +
                        "xa\t2\n" +
                        "aax\t1\n" +
                        "xx\t0\n" +
                        "\t0\n" +
                        "xx\t0\n",
                "select k, strpos(k, 'a') from x",
                "create table x as (select rnd_varchar('xa', 'xx', 'aax', '') k from long_sequence(5))",
                null, true, true);
    }

    @Test
    public void testTrim() throws Exception {
        assertQuery(
                "k\ttrim\n" +
                        "  abc\tabc\n" +
                        "  abc  \tabc\n" +
                        "abc  \tabc\n" +
                        "   \t\n" +
                        "abc  \tabc\n",
                "select k, trim(k) from x",
                "create table x as (select rnd_varchar('  abc', 'abc  ', '  abc  ', '   ') k from long_sequence(5))",
                null, true, true
        );
    }
}
