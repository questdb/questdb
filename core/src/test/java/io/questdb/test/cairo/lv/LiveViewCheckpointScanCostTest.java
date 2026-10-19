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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.lv.LiveViewCheckpointScanCost;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * What one out-of-order repair's two candidate scans are priced at, and therefore
 * which disposition the plan picks.
 * <p>
 * The estimate reads partition metadata only, so its accuracy is exactly as good as
 * an even spread inside each partition. That is fine for a comparison and would not
 * be for a budget, which is why nothing charges a budget against it. What these
 * cases pin is the part that must be right regardless: a partition the interval
 * covers whole contributes its exact count, the outer partitions are bounded by the
 * table's own extremes rather than by the calendar, and an interval outside the
 * table costs nothing.
 */
public class LiveViewCheckpointScanCostTest extends AbstractCairoTest {
    // The first partition starts at noon and the last ends at 06:00, so both differ
    // from the calendar bounds a DAY partition would otherwise be measured over.
    private static final String DAY1_MAX = "2026-01-01T18:00:00.000000Z";
    private static final String DAY1_MIN = "2026-01-01T12:00:00.000000Z";
    private static final String DAY2_MAX = "2026-01-02T23:59:59.999999Z";
    private static final String DAY2_MID = "2026-01-02T11:59:59.999999Z";
    private static final String DAY2_MIN = "2026-01-02T00:00:00.000000Z";
    private static final String DAY3_MAX = "2026-01-03T06:00:00.000000Z";
    private static final String DAY3_MIN = "2026-01-03T00:00:00.000000Z";

    @Test
    public void testEmptyTableCostsNothing() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY");
            final LiveViewCheckpointScanCost cost = new LiveViewCheckpointScanCost();
            try (TableReader reader = engine.getReader("base")) {
                cost.of(reader);
                Assert.assertEquals(0, cost.estimateScanRows(Long.MIN_VALUE, Long.MAX_VALUE));
            }
        });
    }

    @Test
    public void testIntervalOutsideTheTableCostsNothing() throws Exception {
        assertMemoryLeak(() -> {
            createHistory();
            final LiveViewCheckpointScanCost cost = new LiveViewCheckpointScanCost();
            try (TableReader reader = engine.getReader("base")) {
                cost.of(reader);
                // Below every partition. The search for the low bound lands before the
                // first partition and the walk stops on it without counting anything.
                Assert.assertEquals(0, cost.estimateScanRows(Long.MIN_VALUE, ts("2025-12-31T23:59:59.999999Z")));
                // Above the last row, but inside the last partition's calendar day: the
                // clamp to the table maximum is what keeps this at zero.
                Assert.assertEquals(0, cost.estimateScanRows(ts(DAY3_MAX) + 1, Long.MAX_VALUE));
                // An inverted interval is not an interval.
                Assert.assertEquals(0, cost.estimateScanRows(ts(DAY2_MAX), ts(DAY2_MIN)));
            }
        });
    }

    @Test
    public void testOuterPartitionsAreBoundedByTheTableExtremes() throws Exception {
        assertMemoryLeak(() -> {
            createHistory();
            final LiveViewCheckpointScanCost cost = new LiveViewCheckpointScanCost();
            try (TableReader reader = engine.getReader("base")) {
                cost.of(reader);
                // Starting exactly at the table's minimum covers the first partition
                // whole. Measured from that partition's calendar floor instead, the
                // interval would look like half a day and cost half the rows - a repair
                // reading from the very first row would then be priced as if it skipped
                // some.
                Assert.assertEquals(8, cost.estimateScanRows(ts(DAY1_MIN), Long.MAX_VALUE));
                // The same at the top. The last partition's calendar ceiling is a day
                // away from its last row; for a PARTITION BY NONE table it is positive
                // infinity, and every interval over it would round to nothing.
                Assert.assertEquals(8, cost.estimateScanRows(Long.MIN_VALUE, ts(DAY3_MAX)));
                Assert.assertEquals(2, cost.estimateScanRows(ts(DAY3_MIN), ts(DAY3_MAX)));
            }
        });
    }

    @Test
    public void testPartialPartitionInterpolatesOverItsSpan() throws Exception {
        assertMemoryLeak(() -> {
            createHistory();
            final LiveViewCheckpointScanCost cost = new LiveViewCheckpointScanCost();
            try (TableReader reader = engine.getReader("base")) {
                cost.of(reader);
                // Half of the middle partition, which really does hold half its rows.
                // Counting the partition whole here is what would price a narrow repair
                // interval as the day that contains it.
                Assert.assertEquals(2, cost.estimateScanRows(ts(DAY2_MIN), ts(DAY2_MID)));
                // An interval ending inside two partitions at once interpolates both and
                // counts the one between them exactly.
                Assert.assertEquals(1 + 4 + 1, cost.estimateScanRows(ts(DAY1_MAX), ts(DAY3_MIN) + 10_800_000_000L));
            }
        });
    }

    @Test
    public void testUnpartitionedTableIsMeasuredOverItsOwnSpan() throws Exception {
        assertMemoryLeak(() -> {
            // PARTITION BY NONE has one partition whose calendar ceiling is
            // Long.MAX_VALUE and which maintains no table minimum - that reads back
            // ABOVE the maximum. Measured against either, every interval would round to
            // nothing, so the estimate falls back to the maximum and the partition's own
            // floor. The span never overflows a long subtraction because the arithmetic
            // is in double.
            execute("CREATE TABLE flat (ts TIMESTAMP, x LONG) TIMESTAMP(ts)");
            execute("INSERT INTO flat SELECT (x - 1)::timestamp, x FROM long_sequence(100)");
            final LiveViewCheckpointScanCost cost = new LiveViewCheckpointScanCost();
            try (TableReader reader = engine.getReader("flat")) {
                cost.of(reader);
                Assert.assertEquals(100, cost.estimateScanRows(Long.MIN_VALUE, Long.MAX_VALUE));
                Assert.assertEquals(50, cost.estimateScanRows(0, 49));
                Assert.assertEquals(50, cost.estimateScanRows(50, Long.MAX_VALUE));
            }
        });
    }

    @Test
    public void testWholePartitionsCountExactly() throws Exception {
        assertMemoryLeak(() -> {
            createHistory();
            final LiveViewCheckpointScanCost cost = new LiveViewCheckpointScanCost();
            try (TableReader reader = engine.getReader("base")) {
                cost.of(reader);
                Assert.assertEquals(8, cost.estimateScanRows(Long.MIN_VALUE, Long.MAX_VALUE));
                Assert.assertEquals(4, cost.estimateScanRows(ts(DAY2_MIN), ts(DAY2_MAX)));
                Assert.assertEquals(4 + 2, cost.estimateScanRows(ts(DAY2_MIN), Long.MAX_VALUE));
            }
        });
    }

    private static long ts(String timestamp) {
        return parseFloorPartialTimestamp(timestamp);
    }

    /**
     * Three DAY partitions holding 2, 4 and 2 rows, six hours apart. The first row sits
     * at noon and the last at 06:00, so neither outer partition's rows reach its
     * calendar bound.
     */
    private void createHistory() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY");
        execute(
                "INSERT INTO base VALUES" +
                        " ('" + DAY1_MIN + "', 1)," +
                        " ('" + DAY1_MAX + "', 2)," +
                        " ('" + DAY2_MIN + "', 3)," +
                        " ('2026-01-02T06:00:00.000000Z', 4)," +
                        " ('2026-01-02T12:00:00.000000Z', 5)," +
                        " ('2026-01-02T18:00:00.000000Z', 6)," +
                        " ('" + DAY3_MIN + "', 7)," +
                        " ('" + DAY3_MAX + "', 8)"
        );
    }
}
