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

package io.questdb.test.griffin.engine.window;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class WindowPartitionByCursorClosedTest extends AbstractCairoTest {

    // Every window function class that owns its PARTITION BY expressions, in the streaming and
    // the cached window. s::SYMBOL builds a native dictionary in init() and releases it in
    // cursorClosed(), so it exposes an owner that initializes the expression but never notifies it.
    // cume_dist(), percent_rank() and rank() without ORDER BY answer a constant and never evaluate
    // the key, so the dictionary stays empty for them; they are here for completeness.
    private static final String[] WINDOW_EXPRESSIONS = {
            "avg(n) OVER (PARTITION BY s::SYMBOL)",
            "avg(n) OVER (PARTITION BY s::SYMBOL ORDER BY n)",
            "corr(n, m) OVER (PARTITION BY s::SYMBOL)",
            "corr(n, m) OVER (PARTITION BY s::SYMBOL ORDER BY n)",
            "cume_dist() OVER (PARTITION BY s::SYMBOL)",
            "cume_dist() OVER (PARTITION BY s::SYMBOL ORDER BY n)",
            "lag(n) OVER (PARTITION BY s::SYMBOL)",
            "lag(n) OVER (PARTITION BY s::SYMBOL ORDER BY n)",
            "lag(sym) OVER (PARTITION BY s::SYMBOL)",
            "lag(sym) OVER (PARTITION BY s::SYMBOL ORDER BY n)",
            "lead(n) OVER (PARTITION BY s::SYMBOL)",
            "lead(n) OVER (PARTITION BY s::SYMBOL ORDER BY n)",
            "lead(sym) OVER (PARTITION BY s::SYMBOL)",
            "lead(sym) OVER (PARTITION BY s::SYMBOL ORDER BY n)",
            "ntile(2) OVER (PARTITION BY s::SYMBOL ORDER BY n)",
            "percent_rank() OVER (PARTITION BY s::SYMBOL)",
            "percent_rank() OVER (PARTITION BY s::SYMBOL ORDER BY n)",
            "rank() OVER (PARTITION BY s::SYMBOL)",
            "rank() OVER (PARTITION BY s::SYMBOL ORDER BY n)",
            "row_number() OVER (PARTITION BY s::SYMBOL)",
            "row_number() OVER (PARTITION BY s::SYMBOL ORDER BY n)",
    };

    @Test
    public void testClosedCursorReleasesPartitionByStateWhileFactoryIsRetained() throws Exception {
        // A retained factory is the normal case - the query cache holds one after every cursor
        // close. The per-query memory tracker has to be back at zero by then, or the next query
        // on the same context fails to acquire it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE part_fixture (id LONG, s STRING, sym SYMBOL, n DOUBLE, m DOUBLE)");
            execute("""
                    INSERT INTO part_fixture VALUES
                    (1, 'a', 'x', 1.0, 2.0),
                    (2, 'b', 'y', 2.0, 1.0),
                    (3, 'a', 'z', 3.0, 5.0),
                    (4, 'b', 'x', 4.0, 3.0),
                    (5, 'c', 'y', 5.0, 4.0)
                    """);

            for (String expression : WINDOW_EXPRESSIONS) {
                final String sql = "SELECT id, " + expression + " w FROM part_fixture";
                try (RecordCursorFactory factory = select(sql)) {
                    for (int run = 0; run < 2; run++) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            long rows = 0;
                            while (cursor.hasNext()) {
                                rows++;
                            }
                            Assert.assertEquals(sql, 5, rows);
                        }
                        try {
                            assertQuery("SELECT count() FROM part_fixture")
                                    .noLeakCheck()
                                    .noRandomAccess()
                                    .expectSize()
                                    .returns("""
                                            count
                                            5
                                            """);
                        } catch (AssertionError e) {
                            throw new AssertionError(sql + " [run=" + run + "]: " + e.getMessage(), e);
                        }
                    }
                }
            }
        });
    }
}
