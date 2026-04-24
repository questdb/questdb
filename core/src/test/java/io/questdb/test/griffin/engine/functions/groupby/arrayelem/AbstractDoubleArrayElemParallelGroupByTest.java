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

package io.questdb.test.griffin.engine.functions.groupby.arrayelem;

import io.questdb.PropertyKey;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public abstract class AbstractDoubleArrayElemParallelGroupByTest extends AbstractCairoTest {

    protected abstract String funcName();

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 64);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 2);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 2);
        super.setUp();
    }

    @Test
    public void testParallelBothNull() throws Exception {
        assertParallelGroupBy("DOUBLE[][]", "null",
                new String[][]{
                        {"2024-01-01T00:00:00", "null"},
                        {"2024-01-02T00:00:00", "null"},
                        {"2024-01-03T00:00:00", "null"},
                        {"2024-01-04T00:00:00", "null"}
                }
        );
    }

    protected void assertParallelGroupBy(String columnType, String expected, String[][] timestampedRows) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP NOT NULL, arr " + columnType + ") TIMESTAMP(ts) PARTITION BY DAY");
            for (String[] row : timestampedRows) {
                execute("INSERT INTO tab VALUES ('" + row[0] + "', " + row[1] + ")");
            }
            assertQueryNoLeakCheck(
                    "arr\n" + expected + "\n",
                    "SELECT " + funcName() + "(arr) arr FROM tab",
                    null, false, true
            );
        });
    }

    /**
     * Keyed GROUP BY executed via an explicit 4-worker pool.
     * Multiple workers processing different page frames builds separate per-worker
     * maps, forcing {@code merge()} to combine partial results.
     * Each row is {timestamp, groupKey, arrayExpression}.
     */
    protected void assertKeyedParallelGroupBy(String expected, String[][] rows) throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    execute(
                            compiler,
                            "CREATE TABLE tab (ts TIMESTAMP NOT NULL, grp INT, arr DOUBLE[][]) TIMESTAMP(ts) PARTITION BY DAY",
                            sqlExecutionContext
                    );
                    for (String[] row : rows) {
                        execute(
                                compiler,
                                "INSERT INTO tab VALUES ('" + row[0] + "', " + row[1] + ", " + row[2] + ")",
                                sqlExecutionContext
                        );
                    }
                    assertQueryNoLeakCheck(
                            compiler,
                            expected,
                            "SELECT grp, " + funcName() + "(arr) arr FROM tab ORDER BY grp",
                            null,
                            sqlExecutionContext,
                            true,
                            true
                    );
                }, configuration, LOG);
            }
        });
    }
}
