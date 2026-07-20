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

/**
 * A {@code LATEST ON} result keeps the VALUES of its timestamp column, but
 * {@code LatestByLightRecordCursorFactory} (the "latest by over a sub-query" factory, as opposed
 * to the specialized cursors used for a plain {@code LATEST ON} directly over a table) did not
 * DESIGNATE any column as the output's timestamp. That made such a {@code LATEST ON} result
 * unusable as the direct input to a nested time-series operator ({@code SAMPLE BY}, {@code ASOF}
 * JOIN, {@code ORDER BY <ts>}, ...) -- those require {@code getTimestampIndex() != -1} on their
 * base factory and throw loudly otherwise.
 * <p>
 * {@code x} is wrapped in an inner pass-through sub-query ({@code (select ts, k, v from x)})
 * so that {@code LATEST ON} is applied over a sub-query rather than directly over the table --
 * only that shape routes through {@code LatestByLightRecordCursorFactory}; a bare
 * {@code x latest on ts partition by k} is handled by unrelated specialized factories
 * (e.g. {@code LatestByDeferredListValuesFilteredRecordCursorFactory}) that already carry a
 * designated timestamp and are out of scope here.
 * <p>
 * {@code SAMPLE BY} and {@code ASOF} JOIN require their input to ALREADY be in ascending ts
 * order and throw rather than insert a sort of their own -- that is a general, pre-existing
 * QuestDB rule for ANY unordered derived table (empirically confirmed against an unrelated
 * {@code UNION ALL} subquery feeding {@code SAMPLE BY}: same refusal, unrelated to LATEST ON),
 * not a gap this fix needs to close. So the realistic, correct shape adds an explicit
 * {@code ORDER BY ts} inside the LATEST-ON-bearing sub-query; plain {@code ORDER BY} is the one
 * consumer that DOES self-heal by inserting its own sort, which both makes
 * {@code testNestedLatestOnOrderByTs} below pass directly and is what makes the explicit
 * {@code ORDER BY ts} inside the SAMPLE BY/ASOF sub-queries actually take effect.
 * <p>
 * The data set is built so {@code LATEST ON}'s row-emission order (partition-key insertion
 * order into its internal map) provably diverges from ascending timestamp order: key
 * {@code 'a'} is inserted into the map first (so it is emitted first) but is later updated to
 * carry the LARGEST timestamp of the three keys. This makes "materialize LATEST ON into a real
 * table first, then run the time-series op on that table" a genuine correctness oracle -- a real
 * table is always physically stored in ascending designated-timestamp order (a QuestDB commit
 * invariant), so {@code tmp}'s scan is legitimately ts-ordered, whereas the un-materialized
 * {@code LATEST ON} cursor is not. If a fix designates the timestamp but fails to also advertise
 * "not ordered" (see {@code getScanDirection()}), a downstream {@code ORDER BY}/{@code SAMPLE BY}
 * could wrongly take its sort-skip fast path and silently return rows in map order instead of ts
 * order -- these tests would then fail on a cursor mismatch rather than a loud exception.
 * (Empirically confirmed: designating the timestamp without overriding {@code getScanDirection()}
 * makes {@code generateOrderBy} skip the sort entirely, and all three tests below fail on a
 * genuine wrong-order data mismatch against the materialized oracle.)
 */
public class LatestByTimestampDesignationTest extends AbstractCairoTest {

    private static final String LATEST_ON_SUBQ =
            "(select ts, k, v from (select ts, k, v from x) latest on ts partition by k)";
    private static final String LATEST_ON_SUBQ_ORDERED = LATEST_ON_SUBQ.substring(0, LATEST_ON_SUBQ.length() - 1) + " order by ts)";

    @Test
    public void testNestedLatestOnAsofJoin() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            createSlaveTable();
            execute("create table tmp as " + LATEST_ON_SUBQ + " timestamp(ts) partition by day");
            assertSqlCursors(
                    "select * from tmp asof join y",
                    "select * from " + LATEST_ON_SUBQ_ORDERED + " asof join y"
            );
        });
    }

    @Test
    public void testNestedLatestOnOrderByTs() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            execute("create table tmp as " + LATEST_ON_SUBQ + " timestamp(ts) partition by day");
            assertSqlCursors(
                    "select * from tmp order by ts",
                    "select * from " + LATEST_ON_SUBQ + " order by ts"
            );
        });
    }

    @Test
    public void testNestedLatestOnSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            execute("create table tmp as " + LATEST_ON_SUBQ + " timestamp(ts) partition by day");
            assertSqlCursors(
                    "select ts, count() from tmp sample by 1h",
                    "select ts, count() from " + LATEST_ON_SUBQ_ORDERED + " sample by 1h"
            );
        });
    }

    private void createBaseTable() throws Exception {
        execute("create table x (ts timestamp, k symbol, v double) timestamp(ts) partition by day wal");
        // 'a' is inserted into the LATEST ON map first (map/emission position 0), but is
        // later updated to the LARGEST timestamp of the three keys -- its map position does
        // not move on update. So LATEST ON's emission order is [a, b, c] == ts [02:00, 00:05,
        // 00:10], which is provably NOT ascending.
        execute("insert into x values ('2024-01-01T00:00:00.000000Z', 'a', 1.0)");
        execute("insert into x values ('2024-01-01T00:05:00.000000Z', 'b', 2.0)");
        execute("insert into x values ('2024-01-01T00:10:00.000000Z', 'c', 3.0)");
        execute("insert into x values ('2024-01-01T02:00:00.000000Z', 'a', 4.0)");
        drainWalQueue();
    }

    private void createSlaveTable() throws Exception {
        execute("create table y (ts timestamp, p double) timestamp(ts) partition by day wal");
        execute("insert into y values ('2024-01-01T00:06:00.000000Z', 100.0)");
        execute("insert into y values ('2024-01-01T00:12:00.000000Z', 200.0)");
        execute("insert into y values ('2024-01-01T01:30:00.000000Z', 300.0)");
        execute("insert into y values ('2024-01-01T03:00:00.000000Z', 400.0)");
        drainWalQueue();
    }
}
