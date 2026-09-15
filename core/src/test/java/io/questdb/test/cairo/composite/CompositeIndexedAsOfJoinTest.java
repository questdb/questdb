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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

/**
 * The indexed ASOF JOIN ({@code AsOfJoinIndexedRecordCursorFactory}) walks the slave table's time frames
 * backwards and, per frame, asks the symbol index for the key's newest row. The index stores FILE rows,
 * while the time frame numbers rows from the frame's first row; the bound handed to the index has to be
 * the frame's index row lo ({@link io.questdb.cairo.sql.TimeFrameCursor#getIndexRowLoForCurrentFrame()}),
 * which on a composite partition is offset from the partition row by the piece's shift. Bounded with the
 * record's own row id instead, the join reads a row the index never named and matches the wrong quote.
 * <p>
 * Same oracle scheme as {@link CompositeIndexedSampleByTest}: the slave table is built twice, once with the
 * symbol INDEXed and once without, and the unindexed table's answer is the partition-row answer by
 * construction.
 */
public class CompositeIndexedAsOfJoinTest extends AbstractCairoTest {

    @Before
    public void setUpMergeAppend() {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
    }

    @Test
    public void testIndexedAsOfJoinOverACompositeSlavePartition() throws Exception {
        assertMemoryLeak(() -> {
            createCompositeQuotes("indexed", ", INDEX(s CAPACITY 8)");
            createCompositeQuotes("plain", "");
            // The join key is the one only the strides carry, so every match is a relocated row - a row
            // whose file row differs from its partition row. A key every other row carries would hide the
            // defect whenever a piece's shift happens to be even: the wrong range then holds a row of the
            // same key at the same frame offset, and the sum comes out right by accident.
            execute("""
                    CREATE TABLE m AS (
                      SELECT 'kz'::SYMBOL s, timestamp_sequence('2024-01-01T01:00:45', 60_000_000L) ts
                      FROM long_sequence(150)
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL""");
            drainWalQueue();

            final String query = "SELECT /*+ asof_index(m q) */ sum(q.v), count() FROM m ASOF JOIN %s q ON (s)";
            // Ten stride-B rows (810_001..810_010), fifty masters that still see the last of them, ten
            // stride-A rows, and eighty that see the last of those.
            final String expected = "sum\tcount\n120601410\t150\n";
            // The oracle: the unindexed slave takes the non-indexed ASOF JOIN path, whatever the hint says.
            assertQuery(String.format(query, "plain")).expectSize().noRandomAccess().returns(expected);
            assertQuery(String.format(query, "indexed"))
                    .expectSize()
                    .noRandomAccess()
                    .withPlanContaining("AsOf Join Indexed")
                    .returns(expected);
        });
    }

    /**
     * {@link CompositeIndexedSampleByTest#createCompositeDay}'s shape - a day of one row a minute, two
     * backdated strides rewritten at the shared files' tail - except that the strides carry a key of their
     * own, 'kz', and sit at :30 seconds, so no two 'kz' rows share a timestamp.
     */
    private static void createCompositeQuotes(String table, String index) throws Exception {
        execute("CREATE TABLE " + table + " AS (" +
                " SELECT x::INT v, ('k' || ((x % 2) + 1))::SYMBOL s," +
                " timestamp_sequence('2024-01-01', 60_000_000L) ts" +
                " FROM long_sequence(240))" + index + " TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + table + " VALUES (90_000, 'k1', '2024-01-03T00:00:00.000000Z')");
        drainWalQueue();
        execute("INSERT INTO " + table + " SELECT x::INT + 800_000, 'kz'," +
                " timestamp_sequence('2024-01-01T02:00:30', 60_000_000L) FROM long_sequence(10)");
        drainWalQueue();
        execute("INSERT INTO " + table + " SELECT x::INT + 810_000, 'kz'," +
                " timestamp_sequence('2024-01-01T01:00:30', 60_000_000L) FROM long_sequence(10)");
        drainWalQueue();
    }
}
