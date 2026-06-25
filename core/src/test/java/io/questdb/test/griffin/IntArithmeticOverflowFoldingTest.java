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

/**
 * Pins the agreement between the constant-folded path and the column/bind path for
 * INT arithmetic that overflows. The query fuzzer's literal-vs-bind oracle surfaced a
 * divergence: a constant {@code INT * INT} product that overflowed was folded to a
 * wider LONG (2764486628), while the same expression over a column wrapped to INT
 * (-1530480668). That flipped a {@code > 2} comparison and changed a row count.
 * <p>
 * The fix keeps the overflowing arithmetic at INT static type (so both paths wrap),
 * while wider numeric/temporal casts read the widened value, preserving the overflow
 * widening introduced by PR #4824 (e.g. {@code to_utc(<seconds> * 1000000)}).
 */
public class IntArithmeticOverflowFoldingTest extends AbstractCairoTest {

    @Test
    public void testComparisonAgreesBetweenConstantAndColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT)");
            execute("INSERT INTO t VALUES (839759)");

            // plain INT projection wraps mod 2^32 on both paths
            assertQuery("SELECT 839759::INT * 330972L::SHORT AS v").expectSize().returns("v\n-1530480668\n");
            assertQuery("SELECT x::INT * 330972L::SHORT AS v FROM t").expectSize().returns("v\n-1530480668\n");

            // the offending comparison: false on both paths (was true for the folded LONG)
            assertQuery("SELECT (839759::INT * 330972L::SHORT) > 2 AS v").expectSize().returns("v\nfalse\n");
            assertQuery("SELECT (x::INT * 330972L::SHORT) > 2 AS v FROM t").expectSize().returns("v\nfalse\n");
        });
    }

    @Test
    public void testWiderCastsWidenOnBothPaths() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (y INT)");
            execute("INSERT INTO u VALUES (2147483647)");

            // LONG / DOUBLE / DATE targets hold the un-wrapped value, constant and column alike
            assertQuery("SELECT (2147483647 + 3)::LONG AS v").expectSize().returns("v\n2147483650\n");
            assertQuery("SELECT (y + 3)::LONG AS v FROM u").expectSize().returns("v\n2147483650\n");

            assertQuery("SELECT (2147483647 + 3)::DOUBLE AS v").expectSize().returns("v\n2.14748365E9\n");
            assertQuery("SELECT (y + 3)::DOUBLE AS v FROM u").expectSize().returns("v\n2.14748365E9\n");

            assertQuery("SELECT (2147483647 + 3)::DATE AS v").expectSize().returns("v\n1970-01-25T20:31:23.650Z\n");
            assertQuery("SELECT (y + 3)::DATE AS v FROM u").expectSize().returns("v\n1970-01-25T20:31:23.650Z\n");
        });
    }

    @Test
    public void testTimezoneConversionWidensOnBothPaths() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s (sec INT)");
            execute("INSERT INTO s VALUES (1720468802)");

            // PR #4824 use case: seconds * 1_000_000 must not overflow into the timestamp
            assertQuery("SELECT to_utc(1720468802 * 1000000, 'Europe/Berlin') AS v")
                    .expectSize().returns("v\n2024-07-08T18:00:02.000000Z\n");
            assertQuery("SELECT to_utc(sec * 1000000, 'Europe/Berlin') AS v FROM s")
                    .expectSize().returns("v\n2024-07-08T18:00:02.000000Z\n");
        });
    }
}
