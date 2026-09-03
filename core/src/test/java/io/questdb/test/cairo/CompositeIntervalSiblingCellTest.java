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

package io.questdb.test.cairo;

import io.questdb.griffin.SqlException;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * An interval scan over a composite multi-cell day must check EVERY cell of that day against the
 * interval before retiring it.
 * <p>
 * {@code IntervalFwdPartitionFrameCursor} walks partitions and intervals as one merge of two sorted
 * sequences. That is sound for a plain table, where {@code partitionLo + 1} is always the NEXT DAY, so a
 * cell that fails to match an interval means the interval is finished. On a composite table
 * {@code partitionLo + 1} can be a SIBLING CELL of the same day — an independent cell, with its own
 * rows, which may fall squarely inside the interval the previous cell just failed to match. Retiring the
 * interval there drops those rows SILENTLY: a wrong count and a wrong row set on an ordinary
 * {@code WHERE ts = ...} or {@code WHERE ts BETWEEN ...} filter.
 * <p>
 * The fragment exit was fixed earlier (Task 6c). Two exits retired the interval the same way and were
 * missed, each reproduced below against a plain twin:
 * <ul>
 *     <li><b>empty frame</b> — the cell straddles the interval but has no row inside it;</li>
 *     <li><b>wholly above</b> — the cell's rows all start after the interval's high bound.</li>
 * </ul>
 * Both need only ONE dimension, ONE day and three rows. Recorded earlier as needing two or more
 * dimensions and an interval crossing a day boundary; neither is true, which is why the earlier
 * investigation looked in the wrong place.
 * <p>
 * Each test asserts against a plain twin fed identical rows, so it states the contract (composite
 * matches plain) rather than a hand-computed number.
 */
public class CompositeIntervalSiblingCellTest extends AbstractCompositeTwinTest {

    /**
     * Empty-frame exit. Cell E0 (lower cellKey) holds 01:00 and 03:00 and so straddles the interval
     * {@code [02:00, 02:00]} without matching it; sibling cell E1 holds 02:00. Before the fix E0 retired
     * the interval and E1 was never visited — {@code count()} returned 0 where plain returns 1.
     */
    @Test
    public void testStraddlingCellWithNoMatchMustNotRetireInterval() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-02T03:00:00.000000Z','E0',3.0),"
                    + "('2023-01-02T02:00:00.000000Z','E1',2.0)");
            drainWalQueue();

            assertTwinEqual(" WHERE ts = '2023-01-02T02:00:00.000000Z'");
            assertTwinEqual(" WHERE ts >= '2023-01-02T01:30:00.000000Z' AND ts < '2023-01-02T02:30:00.000000Z'");
            assertTwinEqual("");
        });
    }

    /**
     * Wholly-above exit. Cell E0 is created FIRST (so it takes the lower cellKey) holding only 03:00;
     * sibling cell E1 is created by a second commit holding 02:00. Scanning {@code [02:00, 02:00]} meets
     * E0 first, whose rows all start above the interval. Before the fix E0 retired the interval and E1
     * was never visited.
     * <p>
     * The two commits are load-bearing: within a single commit WAL apply sorts rows by timestamp, so the
     * cell holding the earliest row always interns first and takes the lower cellKey — the above-interval
     * cell could never be reached first.
     */
    @Test
    public void testCellStartingAboveIntervalMustNotRetireIt() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-02T03:00:00.000000Z','E0',3.0)");
            drainWalQueue();
            insertIntoBoth("('2023-01-02T02:00:00.000000Z','E1',2.0)");
            drainWalQueue();

            assertTwinEqual(" WHERE ts = '2023-01-02T02:00:00.000000Z'");
            assertTwinEqual(" WHERE ts >= '2023-01-02T01:30:00.000000Z' AND ts < '2023-01-02T02:30:00.000000Z'");
            assertTwinEqual("");
        });
    }

    /**
     * The shape the differential fuzz found (seed 1481/1683): many cells in one day, most holding no row
     * at the queried timestamp. Kept as a direct SQL case so the regression does not depend on the fuzz
     * harness reproducing that seed.
     */
    @Test
    public void testManyCellsWhereOnlyLaterOnesMatch() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            final StringBuilder rows = new StringBuilder();
            // Ten cells, each with rows at 01:00 and 05:00 -- all straddle 03:00 without matching it.
            for (int i = 0; i < 10; i++) {
                rows.append("('2023-01-02T01:00:00.000000Z','E").append(i).append("',1.0),")
                        .append("('2023-01-02T05:00:00.000000Z','E").append(i).append("',5.0),");
            }
            // Two later cells DO hold a row at 03:00.
            rows.append("('2023-01-02T03:00:00.000000Z','X1',31.0),")
                    .append("('2023-01-02T03:00:00.000000Z','X2',32.0)");
            insertIntoBoth(rows.toString());
            drainWalQueue();

            assertTwinEqual(" WHERE ts = '2023-01-02T03:00:00.000000Z'");
            assertTwinEqual(" WHERE ts >= '2023-01-02T02:00:00.000000Z' AND ts < '2023-01-02T04:00:00.000000Z'");
            assertTwinEqual(" WHERE ts >= '2023-01-02T00:00:00.000000Z' AND ts < '2023-01-03T00:00:00.000000Z'");
            assertTwinEqual("");
        });
    }


    /**
     * The BACKWARD cursor has the same defect mirrored, and it is worse: it returned NO rows at all.
     * <p>
     * {@code IntervalBwdPartitionFrameCursor} walks partitions downward, so its dangerous exit is "cell
     * wholly BELOW the interval" rather than above. Cell E1 (higher cellKey, so visited FIRST going
     * backwards) holds only 01:00 and sits entirely below {@code [04:00, 06:00]}; cell E0 holds the
     * 05:00 row that matches. Before the fix E1 retired the interval and E0 was never visited.
     * <p>
     * Two commits are load-bearing: within a single commit WAL apply sorts by timestamp, so the cell
     * holding the earliest row interns first and takes the LOWER cellKey — the below-interval cell could
     * never be the one a backward walk meets first.
     */
    @Test
    public void testBackwardScanCellBelowIntervalMustNotRetireIt() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-02T05:00:00.000000Z','E0',5.0)");
            drainWalQueue();
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','E1',1.0)");
            drainWalQueue();

            assertTwinEqualDesc(" WHERE ts >= '2023-01-02T04:00:00.000000Z' AND ts <= '2023-01-02T06:00:00.000000Z'");
            assertTwinEqualDesc(" WHERE ts = '2023-01-02T05:00:00.000000Z'");
            assertTwinEqualDesc(" WHERE ts >= '2023-01-02T00:00:00.000000Z' AND ts < '2023-01-03T00:00:00.000000Z'");
            assertTwinEqualDesc("");
        });
    }

    /**
     * Backward scan over a day with many cells, only some of which hold a matching row.
     */
    @Test
    public void testBackwardScanManyCells() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            // First commit fixes the cellKey order; later commits add cells that sort EARLIER, so a
            // backward walk meets below-interval cells before matching ones.
            insertIntoBoth("('2023-01-02T08:00:00.000000Z','M1',8.0),('2023-01-02T09:00:00.000000Z','M2',9.0)");
            drainWalQueue();
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','L1',1.0),('2023-01-02T02:00:00.000000Z','L2',2.0)");
            drainWalQueue();
            insertIntoBoth("('2023-01-02T03:00:00.000000Z','L3',3.0)");
            drainWalQueue();

            assertTwinEqualDesc(" WHERE ts >= '2023-01-02T07:00:00.000000Z' AND ts <= '2023-01-02T10:00:00.000000Z'");
            assertTwinEqualDesc(" WHERE ts = '2023-01-02T08:00:00.000000Z'");
            assertTwinEqualDesc(" WHERE ts >= '2023-01-02T02:30:00.000000Z' AND ts <= '2023-01-02T08:30:00.000000Z'");
            assertTwinEqualDesc("");
        });
    }

    /**
     * Backward-only comparison, delegating to the shared harness (which asserts the plan and projects
     * only ts -- see AbstractCompositeTwinTest for why those two things are not optional).
     */
    private void assertTwinEqualDesc(String where) throws SqlException {
        assertBackwardTwinEqual(where);
    }

    /**
     * A plain table must be completely unaffected: every branch changed here is gated on a same-day
     * sibling, which a one-cell-per-day table can never have. This drives the same interval shapes over
     * a plain table alone, so a regression in the shared code path surfaces here even if composite
     * routing is switched off entirely.
     */
    @Test
    public void testPlainTableIntervalScanUnaffected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO p VALUES ('2023-01-01T05:00:00.000000Z','E0',0.5),"
                    + "('2023-01-02T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-02T03:00:00.000000Z','E0',3.0),"
                    + "('2023-01-02T05:00:00.000000Z','E1',5.0),"
                    + "('2023-01-03T02:00:00.000000Z','E1',7.0)");
            drainWalQueue();

            assertQuery("SELECT count() FROM p WHERE ts = '2023-01-02T02:00:00.000000Z'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
            assertQuery("SELECT count() FROM p WHERE ts = '2023-01-02T03:00:00.000000Z'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
            assertQuery("SELECT count() FROM p WHERE ts >= '2023-01-02T00:00:00.000000Z' AND ts < '2023-01-03T00:00:00.000000Z'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
            assertQuery("SELECT count() FROM p WHERE ts >= '2023-01-01T18:00:00.000000Z' AND ts < '2023-01-02T06:00:00.000000Z'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
        });
    }


}
