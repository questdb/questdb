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

package io.questdb.test.fuzz;

import io.questdb.cairo.TableWriterAPI;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class FuzzInsertOperationTest extends AbstractCairoTest {
    private static final int ROW_COUNT = 128;

    @Test
    public void testDecimalReplay() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE expected (
                        d8 DECIMAL(2, 1), d16 DECIMAL(4, 2), d32 DECIMAL(8, 3),
                        d64 DECIMAL(15, 5), d128 DECIMAL(30, 10), d256 DECIMAL(50, 15), ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("CREATE TABLE actual AS (SELECT * FROM expected) TIMESTAMP(ts) PARTITION BY DAY WAL");
            insert("expected", 0);
            insert("actual", 0);
            insert("expected", 1);
            insert("actual", 1);
            drainWalQueue();
            try (var compiler = engine.getSqlCompiler()) {
                TestUtils.assertEqualsExactOrder(compiler, sqlExecutionContext, "expected", "actual");
            }
            for (String column : new String[]{"d8", "d16", "d32", "d64", "d128", "d256"}) {
                assertQuery("SELECT count(" + column + "), min(" + column + ") < 0 lo, max(" + column + ") > 0 hi FROM actual")
                        .noRandomAccess().expectSize()
                        .returns("count\tlo\thi\n" + ROW_COUNT + "\ttrue\ttrue\n");
            }
        });
    }

    @Test
    public void testLong256NonNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE values256 (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            insert("values256", 0);
            assertQuery("SELECT count(v) FROM values256").noRandomAccess().expectSize()
                    .returns("count\n" + ROW_COUNT + "\n");
        });
    }

    @Test
    public void testLong256Null() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE values256 (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            insert("values256", 1);
            assertQuery("SELECT count(v) FROM values256").noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    private static void insert(String table, double nullRate) {
        try (TableWriterAPI writer = engine.getTableWriterAPI(table, "fuzz value test")) {
            Rnd rnd = new Rnd(12345, 67890);
            for (int i = 0; i < ROW_COUNT; i++) {
                new FuzzInsertOperation(12345 + i, 67890 + i, i, 0, nullRate, 0, 32, new String[0])
                        .apply(rnd, engine, writer, -1, null);
            }
            writer.commit();
        }
    }
}
