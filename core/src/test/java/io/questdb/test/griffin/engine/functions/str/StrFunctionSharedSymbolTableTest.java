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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

/**
 * Two string arguments of one function may resolve through the same symbol table, e.g.
 * lag(s) and s::string over a NOCACHE column, or lag(s) and lag(s, 2) over a UNION. A
 * non-static table hands out one mapped view per A/B slot, so a function that reads both
 * arguments through the same slot sees the second value twice. Each test compares a window
 * output with the column it was derived from, on a NOCACHE table, with the function both
 * in the projection (getStrA) and on the right of = (getStrB).
 */
public class StrFunctionSharedSymbolTableTest extends AbstractCairoTest {

    @Before
    public void setUpTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE n (ts TIMESTAMP, a SYMBOL NOCACHE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO n VALUES
                    ('2024-01-01T00:00:00', 'a1'),
                    ('2024-01-01T01:00:00', 'a2'),
                    ('2024-01-01T02:00:00', 'a3'),
                    ('2024-01-01T03:00:00', 'a3'),
                    ('2024-01-01T04:00:00', 'b1')
                    """);
        });
    }

    @Test
    public void testLPad() throws Exception {
        assertMemoryLeak(() -> {
            // the left side holds its A value while the right side runs through its B path
            assertQuery("SELECT ts FROM (SELECT ts, a, lag(a) OVER () x FROM n) WHERE a::string = lpad(a::string, 2, x)")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts
                            2024-01-01T01:00:00.000000Z
                            2024-01-01T02:00:00.000000Z
                            2024-01-01T03:00:00.000000Z
                            2024-01-01T04:00:00.000000Z
                            """);
            assertQuery("SELECT ts, lpad(a::string, 4, x) r FROM (SELECT ts, a, lag(a) OVER () x FROM n)")
                    .timestamp("ts")
                    .expectSize()
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts\tr
                            2024-01-01T00:00:00.000000Z\t
                            2024-01-01T01:00:00.000000Z\ta1a2
                            2024-01-01T02:00:00.000000Z\ta2a3
                            2024-01-01T03:00:00.000000Z\ta3a3
                            2024-01-01T04:00:00.000000Z\ta3b1
                            """);
            assertQuery("SELECT ts FROM (SELECT ts, a, lag(a) OVER () x FROM n) WHERE 'a3b1' = lpad(a::string, 4, x)")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts
                            2024-01-01T04:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testNullIf() throws Exception {
        assertMemoryLeak(() -> {
            // the left side holds its A value while the right side runs through its B path
            assertQuery("SELECT ts FROM (SELECT ts, a, lag(a) OVER () x FROM n) WHERE a::string = nullif(a::string, x)")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts
                            2024-01-01T00:00:00.000000Z
                            2024-01-01T01:00:00.000000Z
                            2024-01-01T02:00:00.000000Z
                            2024-01-01T04:00:00.000000Z
                            """);
            assertQuery("SELECT ts, nullif(x, a::string) r FROM (SELECT ts, a, lag(a) OVER () x FROM n)")
                    .timestamp("ts")
                    .expectSize()
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts\tr
                            2024-01-01T00:00:00.000000Z\t
                            2024-01-01T01:00:00.000000Z\ta1
                            2024-01-01T02:00:00.000000Z\ta2
                            2024-01-01T03:00:00.000000Z\t
                            2024-01-01T04:00:00.000000Z\ta3
                            """);
            assertQuery("SELECT ts FROM (SELECT ts, a, lag(a) OVER () x FROM n) WHERE 'a3' = nullif(x, a::string)")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts
                            2024-01-01T04:00:00.000000Z
                            """);
            // a UNION resolves both window columns through the dictionary of its cast function
            assertQuery("""
                    SELECT ts, nullif(x, y) r FROM (
                        SELECT ts, lag(a) OVER () x, lag(a, 2) OVER () y
                        FROM (SELECT ts, a FROM n UNION ALL SELECT ts, a FROM n)
                    ) LIMIT 5
                    """)
                    .expectSize()
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts\tr
                            2024-01-01T00:00:00.000000Z\t
                            2024-01-01T01:00:00.000000Z\ta1
                            2024-01-01T02:00:00.000000Z\ta2
                            2024-01-01T03:00:00.000000Z\ta3
                            2024-01-01T04:00:00.000000Z\t
                            """);
        });
    }

    @Test
    public void testRPad() throws Exception {
        assertMemoryLeak(() -> {
            // the left side holds its A value while the right side runs through its B path
            assertQuery("SELECT ts FROM (SELECT ts, a, lag(a) OVER () x FROM n) WHERE a::string = rpad(a::string, 2, x)")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts
                            2024-01-01T01:00:00.000000Z
                            2024-01-01T02:00:00.000000Z
                            2024-01-01T03:00:00.000000Z
                            2024-01-01T04:00:00.000000Z
                            """);
            assertQuery("SELECT ts, rpad(a::string, 4, x) r FROM (SELECT ts, a, lag(a) OVER () x FROM n)")
                    .timestamp("ts")
                    .expectSize()
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts\tr
                            2024-01-01T00:00:00.000000Z\t
                            2024-01-01T01:00:00.000000Z\ta2a1
                            2024-01-01T02:00:00.000000Z\ta3a2
                            2024-01-01T03:00:00.000000Z\ta3a3
                            2024-01-01T04:00:00.000000Z\tb1a3
                            """);
            assertQuery("SELECT ts FROM (SELECT ts, a, lag(a) OVER () x FROM n) WHERE 'b1a3' = rpad(a::string, 4, x)")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts
                            2024-01-01T04:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testReplace() throws Exception {
        assertMemoryLeak(() -> {
            // the left side holds its A value while the right side runs through its B path
            assertQuery("SELECT ts FROM (SELECT ts, a, lag(a) OVER () x FROM n) WHERE a::string = replace(a::string, x, 'Z')")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts
                            2024-01-01T01:00:00.000000Z
                            2024-01-01T02:00:00.000000Z
                            2024-01-01T04:00:00.000000Z
                            """);
            // constant replacement
            assertQuery("SELECT ts, replace(a::string, x, 'Z') r FROM (SELECT ts, a, lag(a) OVER () x FROM n)")
                    .timestamp("ts")
                    .expectSize()
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts\tr
                            2024-01-01T00:00:00.000000Z\t
                            2024-01-01T01:00:00.000000Z\ta2
                            2024-01-01T02:00:00.000000Z\ta3
                            2024-01-01T03:00:00.000000Z\tZ
                            2024-01-01T04:00:00.000000Z\tb1
                            """);
            assertQuery("SELECT ts FROM (SELECT ts, a, lag(a) OVER () x FROM n) WHERE 'Z' = replace(a::string, x, 'Z')")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts
                            2024-01-01T03:00:00.000000Z
                            """);
            // all three arguments resolve through the same table
            assertQuery("""
                    SELECT ts, a, x, y, replace(a::string, x, y) r FROM (
                        SELECT ts, a, lag(a) OVER () x, lag(a, 2) OVER () y FROM n
                    ) WHERE y IS NOT NULL
                    """)
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts\ta\tx\ty\tr
                            2024-01-01T02:00:00.000000Z\ta3\ta2\ta1\ta3
                            2024-01-01T03:00:00.000000Z\ta3\ta3\ta2\ta2
                            2024-01-01T04:00:00.000000Z\tb1\ta3\ta3\tb1
                            """);
            assertQuery("""
                    SELECT ts FROM (
                        SELECT ts, a, lag(a) OVER () x, lag(a, 2) OVER () y FROM n
                    ) WHERE 'a2' = replace(a::string, x, y)
                    """)
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts
                            2024-01-01T03:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testSplitPart() throws Exception {
        assertMemoryLeak(() -> {
            // the left side holds its A value while the right side runs through its B path
            assertQuery("SELECT ts FROM (SELECT ts, a, lag(a) OVER () x FROM n) WHERE a::string = split_part(a::string, x, 1)")
                    .timestamp("ts")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts
                            2024-01-01T01:00:00.000000Z
                            2024-01-01T02:00:00.000000Z
                            2024-01-01T04:00:00.000000Z
                            """);
            assertQuery("SELECT ts, split_part(a::string, x, 1) r FROM (SELECT ts, a, lag(a) OVER () x FROM n)")
                    .timestamp("ts")
                    .expectSize()
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts\tr
                            2024-01-01T00:00:00.000000Z\t
                            2024-01-01T01:00:00.000000Z\ta2
                            2024-01-01T02:00:00.000000Z\ta3
                            2024-01-01T03:00:00.000000Z\t
                            2024-01-01T04:00:00.000000Z\tb1
                            """);
        });
    }

    @Test
    public void testStartsWith() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("SELECT ts, starts_with(a::string, x) r FROM (SELECT ts, a, lag(a) OVER () x FROM n)")
                    .timestamp("ts")
                    .expectSize()
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts\tr
                            2024-01-01T00:00:00.000000Z\tfalse
                            2024-01-01T01:00:00.000000Z\tfalse
                            2024-01-01T02:00:00.000000Z\tfalse
                            2024-01-01T03:00:00.000000Z\ttrue
                            2024-01-01T04:00:00.000000Z\tfalse
                            """);
        });
    }

    @Test
    public void testStrPos() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("SELECT ts, strpos(a::string, x) r FROM (SELECT ts, a, lag(a) OVER () x FROM n)")
                    .timestamp("ts")
                    .expectSize()
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            ts\tr
                            2024-01-01T00:00:00.000000Z\tnull
                            2024-01-01T01:00:00.000000Z\t0
                            2024-01-01T02:00:00.000000Z\t0
                            2024-01-01T03:00:00.000000Z\t1
                            2024-01-01T04:00:00.000000Z\t0
                            """);
        });
    }
}
