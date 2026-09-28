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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class NonCachedSymbolSharedViewTest extends AbstractCairoTest {

    @Test
    public void testFirstAndLastOverUnionKeepDistinctTexts() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k SYMBOL, a SYMBOL)");
            execute("CREATE TABLE y (k SYMBOL, a SYMBOL)");
            execute("INSERT INTO x VALUES ('k1', 'a1'), ('k2', 'a2'), ('k3', 'a3')");
            execute("INSERT INTO y VALUES ('k1', 'a1'), ('k2', 'a4'), ('k3', NULL)");

            assertQuery("""
                    SELECT k, first(a) f, last(a) l
                    FROM (SELECT k, a FROM x UNION ALL SELECT k, a FROM y)
                    ORDER BY k
                    """)
                    .expectSize()
                    .returns("""
                            k\tf\tl
                            k1\ta1\ta1
                            k2\ta2\ta4
                            k3\ta3\t
                            """);

            assertQuery("""
                    SELECT k, f, l FROM (
                      SELECT k, first(a) f, last(a) l
                      FROM (SELECT k, a FROM x UNION ALL SELECT k, a FROM y)
                    ) WHERE f = l
                    ORDER BY k
                    """)
                    .returns("""
                            k\tf\tl
                            k1\ta1\ta1
                            """);

            execute("CREATE TABLE xn (k SYMBOL, a SYMBOL NOCACHE)");
            execute("CREATE TABLE yn (k SYMBOL, a SYMBOL NOCACHE)");
            execute("INSERT INTO xn VALUES ('k1', 'a1'), ('k2', 'a2'), ('k3', 'a3')");
            execute("INSERT INTO yn VALUES ('k1', 'a1'), ('k2', 'a4'), ('k3', NULL)");

            assertQuery("""
                    SELECT k FROM (
                      SELECT k, first(a) f, last(a) l
                      FROM (SELECT k, a FROM xn UNION ALL SELECT k, a FROM yn)
                    ) WHERE f = trim(l)
                    ORDER BY k
                    """)
                    .returns("""
                            k
                            k1
                            """);

            assertQuery("""
                    SELECT k FROM (
                      SELECT k, first(a) f, last(a) l
                      FROM (SELECT k, a FROM xn UNION ALL SELECT k, a FROM yn)
                    ) WHERE f = l::string
                    ORDER BY k
                    """)
                    .returns("""
                            k
                            k1
                            """);

            assertQuery("""
                    SELECT k FROM (
                      SELECT k, first(a) f, last(a) l
                      FROM (SELECT k, a FROM xn UNION ALL SELECT k, a FROM yn)
                    ) WHERE f = lower(l)
                    ORDER BY k
                    """)
                    .returns("""
                            k
                            k1
                            """);

            assertQuery("""
                    SELECT k FROM (
                      SELECT k, first(a) f, last(a) l
                      FROM (SELECT k, a FROM xn UNION ALL SELECT k, a FROM yn)
                    ) WHERE f = upper(l)
                    ORDER BY k
                    """)
                    .returns("""
                            k
                            """);

            assertQuery("""
                    SELECT k FROM (
                      SELECT k, first(a) f, last(a) l
                      FROM (SELECT k, a FROM xn UNION ALL SELECT k, a FROM yn)
                    ) WHERE starts_with(f, l)
                    ORDER BY k
                    """)
                    .returns("""
                            k
                            k1
                            """);
        });
    }
}
