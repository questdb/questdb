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

package io.questdb.test.cairo.covering;

import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A covering-index scan that returns many rows reads them through its parallel plan, the covered
 * columns decoded on the workers (CoveringIndexRecordCursorFactory.setParallelRows()); a small one
 * keeps the serial cursor. Each query is compared with the scan plan.
 */
public class CoveringIndexParallelRowsTest extends AbstractCoveringIndexQueryTest {

    @Test
    public void testLargeScanReadsRowsInParallel() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table q (ts timestamp, sym symbol, v double, l long, vc varchar) timestamp(ts) partition by day bypass wal");
            // R selects 1 row in 8: 300K rows, past the parallel threshold and under the scan choice's share
            execute("insert into q select (x * 100_000L)::timestamp, case when x % 8 = 0 then 'R' when x % 1000 = 1 then 'S' else 'k' || (x % 50) end, "
                    + "x / 7.0, x, case when x % 13 = 0 then null else 'v' || (x % 17) end from long_sequence(2_400_000)");
            execute("alter table q alter column sym add index type posting include (v, l, vc)");
            execute("insert into q select (240_000_000_000L + x * 100_000L)::timestamp, case when x % 8 = 0 then 'R' else 'k' || (x % 50) end, "
                    + "-x / 3.0, -x, 'w' || x from long_sequence(50_000)");
            final String[][] queries = {
                    {"select ts, v, l, vc from q where sym = 'R'", "true"},
                    {"select ts, v * 2 + l d, vc from q where sym = 'R'", "true"},
                    // a residual filter runs as an Async Filter over the scan's frames: parallel already
                    {"select * from (select ts, v, l, vc from q where sym = 'R') where l % 3 = 0", "false"},
                    // a LIMIT reads few rows: the serial cursor
                    {"select ts, v, l, vc from q where sym = 'R' limit 5", "false"},
                    {"select ts, v, l, vc from q where sym = 'S'", "false"},
            };
            for (String[] q : queries) {
                final String query = q[0];
                final boolean isParallel = Boolean.parseBoolean(q[1]);
                final StringSink plan = new StringSink();
                printSql("explain " + query, plan);
                TestUtils.assertContains(query, plan, "CoveringIndex");
                if (query.contains("l % 3")) {
                    TestUtils.assertContains(query, plan, "Async");
                }
                try (RecordCursorFactory factory = select(query)) {
                    final CoveringIndexRecordCursorFactory covering = covering(factory);
                    Assert.assertNotNull(query, covering);
                    final StringSink actual = new StringSink();
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        CursorPrinter.println(cursor, factory.getMetadata(), actual);
                    }
                    Assert.assertEquals(query, isParallel ? 1 : 0, covering.getParallelRowsOpens());
                    final StringSink expected = new StringSink();
                    final String scan = query.replace("select ts", "select /*+ no_index */ ts");
                    try (RecordCursorFactory scanFactory = select(scan); RecordCursor cursor = scanFactory.getCursor(sqlExecutionContext)) {
                        Assert.assertFalse(scan, covering(scanFactory) != null);
                        CursorPrinter.println(cursor, scanFactory.getMetadata(), expected);
                    }
                    TestUtils.assertEquals(query, expected, actual);
                }
            }
        });
    }

    private static CoveringIndexRecordCursorFactory covering(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof CoveringIndexRecordCursorFactory covering) {
                return covering;
            }
        }
        return null;
    }
}
