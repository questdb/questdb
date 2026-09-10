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
 * Regression for community bug:
 * https://community.questdb.com/t/sorting-by-timestamp-asc-doesnt-work-on-qdb-9-4-2/1005
 * <p>
 * An explicit {@code timestamp(timestamp)} re-designation over a {@code GROUP BY timestamp}
 * sub-result makes the sub-result carry a designated timestamp. A keyed GROUP BY factory used
 * to report {@code SCAN_DIRECTION_FORWARD} (inherited from its forward base scan) even though
 * hash aggregation reorders rows. {@code generateOrderBy} then treated "forward scan + matching
 * designated timestamp" as "already ascending" and silently elided {@code ORDER BY timestamp ASC},
 * returning rows in hash order. DESC always kept its sort, which is why only ASC broke.
 */
public class SortTsAscJoinReproTest extends AbstractCairoTest {

    // Minimal trigger: single GROUP BY with an explicit timestamp() re-designation, no join.
    @Test
    public void testOrderByTsAscOverSingleGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (timestamp TIMESTAMP, number LONG) TIMESTAMP(timestamp) PARTITION BY DAY");
            execute("INSERT INTO t SELECT dateadd('h', x::int, '2026-01-01T00:00:00.000000Z'), x FROM long_sequence(48)");

            // suspect: timestamp() re-designation on the GROUP BY output; ORDER BY was wrongly elided
            final String suspect = "SELECT * FROM ((SELECT timestamp, sum(number) n FROM t WHERE timestamp > '2026-01' group by timestamp) timestamp(timestamp)) order by timestamp asc";
            // trusted reference: same query WITHOUT the timestamp() re-designation, so the sort is never elided
            final String trusted = "SELECT * FROM (SELECT timestamp, sum(number) n FROM t WHERE timestamp > '2026-01' group by timestamp) order by timestamp asc";

            // Rows must come back ascending by timestamp, identical (row-for-row) to the un-elided reference.
            assertSqlCursors(trusted, suspect);
        });
    }

    // The original report shape: GROUP BY -> hash JOIN -> GROUP BY, ORDER BY timestamp ASC.
    @Test
    public void testOrderByTsAscOverGroupByJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE summary_object_stat (timestamp TIMESTAMP, class_name SYMBOL, number LONG, surface DOUBLE) TIMESTAMP(timestamp) PARTITION BY DAY");
            execute("CREATE TABLE aisee_summary_object_stat (timestamp TIMESTAMP, number LONG, surface DOUBLE) TIMESTAMP(timestamp) PARTITION BY DAY");
            execute("INSERT INTO summary_object_stat SELECT dateadd('h', x::int, '2026-01-01T00:00:00.000000Z'), 'CEL_A', x, x*1.0 FROM long_sequence(48)");
            execute("INSERT INTO aisee_summary_object_stat SELECT dateadd('h', x::int, '2026-01-01T00:00:00.000000Z'), x, x*1.0 FROM long_sequence(48)");

            final String suspect =
                    "SELECT * FROM (" +
                            "SELECT a03.timestamp as timestamp, a03.n_pap_03, a03.s_pap_03, t03.n_03, t03.s_03 FROM " +
                            "((SELECT timestamp, sum(number) as n_pap_03, sum(surface) as s_pap_03 FROM summary_object_stat WHERE class_name like 'CEL%' and timestamp > '2026-01' group by timestamp) timestamp(timestamp)) as a03 " +
                            "JOIN " +
                            "((SELECT timestamp, sum(number) as n_03, sum(surface) as s_03 FROM aisee_summary_object_stat WHERE timestamp > '2026-01' group by timestamp) timestamp(timestamp)) as t03 on a03.timestamp = t03.timestamp" +
                            ") order by timestamp asc";
            // trusted: identical shape but no inner timestamp() re-designations -> sort is applied
            final String trusted =
                    "SELECT * FROM (" +
                            "SELECT a03.timestamp as timestamp, a03.n_pap_03, a03.s_pap_03, t03.n_03, t03.s_03 FROM " +
                            "(SELECT timestamp, sum(number) as n_pap_03, sum(surface) as s_pap_03 FROM summary_object_stat WHERE class_name like 'CEL%' and timestamp > '2026-01' group by timestamp) as a03 " +
                            "JOIN " +
                            "(SELECT timestamp, sum(number) as n_03, sum(surface) as s_03 FROM aisee_summary_object_stat WHERE timestamp > '2026-01' group by timestamp) as t03 on a03.timestamp = t03.timestamp" +
                            ") order by timestamp asc";

            assertSqlCursors(trusted, suspect);
        });
    }

    /**
     * The same dishonest signal also defeated an ordering check that already existed.
     * <p>
     * A time-series join walks its right-hand operand as a monotonically ascending
     * designated-timestamp stream, and {@code generateSelect} already rejects an operand whose
     * {@code getScanDirection()} is not {@code SCAN_DIRECTION_FORWARD}. That check passed
     * vacuously for every reordering factory, because {@code RecordCursorFactory.getScanDirection()}
     * defaults to {@code SCAN_DIRECTION_FORWARD} and none of them overrode it. The join then
     * searched a non-monotonic cursor and silently returned no match for keys outside the leading
     * run - a wrong answer rather than an error.
     * <p>
     * Reported from the field as missing price coverage: a {@code UNION ALL} of two price sources,
     * re-designated with {@code timestamp(ts)} and joined with {@code LT JOIN}, resolved only the
     * tokens carried by the first branch. Reproduced below with disjoint keys per branch.
     */
    @Test
    public void testUnorderedTimeSeriesJoinOperandIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE px_bridge (ts TIMESTAMP, token SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE px_tail (ts TIMESTAMP, token SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (ts TIMESTAMP, token SYMBOL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO px_bridge SELECT timestamp_sequence(0, 1000000), 'BRIDGE', 600.0 FROM long_sequence(10)");
            execute("INSERT INTO px_tail SELECT timestamp_sequence(0, 1000000), 'TAIL', 1.5 FROM long_sequence(10)");
            execute("INSERT INTO trades SELECT timestamp_sequence(5000000, 1000000), " +
                    "CASE WHEN x % 2 = 0 THEN 'BRIDGE' ELSE 'TAIL' END FROM long_sequence(4)");

            final String prefix = "SELECT sum(CASE WHEN p.price IS NOT NULL THEN 1 ELSE 0 END) r FROM trades t LT JOIN ";
            final String suffix = " p ON (t.token = p.token)";
            final String msg = "ASC order over TIMESTAMP column is required but not provided";

            // Control: a genuinely ascending operand still joins. px_bridge prices only BRIDGE,
            // which is 2 of the 4 trades.
            assertQuery(prefix + "px_bridge" + suffix)
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            r
                            2
                            """);

            // Concatenating UNION ALL - the reported shape. Rejected with and without an explicit
            // ORDER BY, since the sort is not what makes the concatenation ordered.
            assertException(prefix + "(SELECT * FROM (SELECT ts, token, price FROM px_bridge " +
                    "UNION ALL SELECT ts, token, price FROM px_tail) TIMESTAMP(ts))" + suffix, 85, msg);
            assertException(prefix + "(SELECT * FROM (SELECT ts, token, price FROM px_bridge " +
                    "UNION ALL SELECT ts, token, price FROM px_tail ORDER BY ts) TIMESTAMP(ts))" + suffix, 85, msg);

            // Hash-deduplicating UNION.
            assertException(prefix + "(SELECT * FROM (SELECT ts, token, price FROM px_bridge " +
                    "UNION SELECT ts, token, price FROM px_tail) TIMESTAMP(ts))" + suffix, 85, msg);

            // Sort whose leading key is not the designated timestamp.
            assertException(prefix + "(SELECT * FROM (SELECT ts, token, price FROM px_bridge " +
                    "ORDER BY price, ts) TIMESTAMP(ts))" + suffix, 85, msg);

            // Keyed GROUP BY - rows come back in hash order.
            assertException("SELECT sum(CASE WHEN p.px IS NOT NULL THEN 1 ELSE 0 END) r FROM trades t LT JOIN " +
                    "(SELECT * FROM (SELECT ts, token, max(price) px FROM px_bridge GROUP BY ts, token) TIMESTAMP(ts))" +
                    " p ON (t.token = p.token)", 82, msg);
        });
    }
}
