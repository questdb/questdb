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
}
