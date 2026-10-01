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

public class ScalarSubqueryWhereClauseReentryTest extends AbstractCairoTest {

    private static final String EXPECTED = """
            ts\tsym\tvalue
            2024-01-02T00:00:00.000000Z\ta\t2
            2024-01-03T00:00:00.000000Z\ta\t3
            """;

    @Test
    public void testDepthTwoScalarSubqueryPreservesIndexedKey() throws Exception {
        assertMemoryLeak(() -> {
            createData();
            assertQuery("""
                    SELECT ts, sym, value
                    FROM events
                    WHERE ts >= (
                        SELECT lo
                        FROM bounds
                        WHERE selector = 1
                          AND lo >= (SELECT min(lo) FROM bounds WHERE selector = 1)
                    )
                      AND sym = 'a'
                    ORDER BY ts
                    """)
                    .withPlanContaining("Index forward scan on: sym")
                    .withPlanNotContaining("Async Filter", "Async JIT Filter")
                    .timestamp("ts")
                    .returns(EXPECTED);
        });
    }

    @Test
    public void testScalarSubqueryPreservesIndexedKeyInBothPredicateOrders() throws Exception {
        assertMemoryLeak(() -> {
            createData();
            assertQuery("""
                    SELECT ts, sym, value
                    FROM events
                    WHERE sym = 'a'
                      AND ts >= (SELECT lo FROM bounds WHERE selector = 1)
                    ORDER BY ts
                    """)
                    .withPlanContaining("Index forward scan on: sym")
                    .withPlanNotContaining("Async Filter", "Async JIT Filter")
                    .timestamp("ts")
                    .returns(EXPECTED);

            assertQuery("""
                    SELECT ts, sym, value
                    FROM events
                    WHERE ts >= (SELECT lo FROM bounds WHERE selector = 1)
                      AND sym = 'a'
                    ORDER BY ts
                    """)
                    .withPlanContaining("Index forward scan on: sym")
                    .withPlanNotContaining("Async Filter", "Async JIT Filter")
                    .timestamp("ts")
                    .returns(EXPECTED);
        });
    }

    private void createData() throws Exception {
        execute("CREATE TABLE events (ts TIMESTAMP, sym SYMBOL INDEX, value INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO events VALUES
                    ('2024-01-01', 'a', 1),
                    ('2024-01-02', 'a', 2),
                    ('2024-01-02', 'b', 20),
                    ('2024-01-03', 'a', 3)
                """);
        execute("CREATE TABLE bounds (lo TIMESTAMP, selector INT) TIMESTAMP(lo) PARTITION BY DAY");
        execute("INSERT INTO bounds VALUES ('2024-01-02', 1)");
    }
}
