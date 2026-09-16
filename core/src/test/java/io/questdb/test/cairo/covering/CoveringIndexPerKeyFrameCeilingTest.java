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

import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.std.Rows;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Pins the CORRECTNESS ceiling on the per-key (unordered) covering scan: the number of page
 * frames one execution emits must stay strictly below {@link Rows#MAX_SAFE_PARTITION_INDEX}.
 * <p>
 * {@code PageFrameMemoryRecord.getRowId()} returns
 * {@code Rows.toRowID(frameIndex, rowIndex) == (frameIndex << 44) + rowIndex}, where
 * {@code frameIndex} is the frame's position in the whole query's {@code PageFrameSequence}
 * -- one sequence-wide counter that {@code PageFrameSequence.buildAddressCache()} increments
 * per emitted frame and never caps. {@code first()} / {@code last()} and their siblings decide
 * purely by comparing that row id with a SIGNED {@code <}, so past frame index 2^19 the shifted
 * value carries the sign bit and the comparison inverts. (At EXACTLY 2^19 there is a second,
 * independent mechanism: {@code 524288 << 44} is {@code Long.MIN_VALUE}, which is
 * {@code Numbers.LONG_NULL} -- the "accumulator is empty" sentinel those same functions test
 * for.) Neither failure raises anything; both return a wrong number.
 * <p>
 * Merged mode reaches that ceiling only through the partition count. Per-key mode MULTIPLIES
 * the partition count by the key count, which is what brings ordinary shapes into range -- 30
 * symbols over two years of hourly partitions is 525,600 (key, partition) pairs.
 * <p>
 * <b>Why the shape is this big.</b> Genuine corruption needs the scan to actually EMIT more
 * than 524,287 frames. Per-key emits one frame per non-empty (key, partition) pair, so the
 * product {@code K * P} must clear the ceiling and the table must hold at least one row per
 * pair -- there is no cheaper shape, and {@code setMaxRowsPerFrameForTesting} cannot supply
 * one, because shrinking the row cap inflates the MERGED frame count by exactly the same
 * factor and would corrupt the fallback too. The free choice is only how to split the product:
 * {@link #KEYS} x {@link #PARTITIONS} keeps the partition count (the expensive axis to create)
 * low while the IN-list stays a few kilobytes of SQL.
 * <p>
 * The two arms straddle the ceiling on ONE fixture, so the only thing that differs between
 * them is the estimated frame count:
 * <ul>
 *     <li>{@link #KEYS} x {@link #PARTITIONS} = 532,480 frames, over the ceiling -&gt; the gate
 *     must fall back to the merge, and the answers must be right.</li>
 *     <li>{@link #KEYS_UNDER_CEILING} x {@link #PARTITIONS} = 520,000 frames, under it -&gt; the
 *     gate must still admit per-key, and the answers must be right THERE too. Without this arm
 *     a gate that simply never admitted per-key would pass the first arm.</li>
 * </ul>
 * The performance crossover is switched off for both arms (see {@link #setUp()}). Note what
 * that concedes: a correctly tuned crossover would have rejected BOTH of these shapes on
 * density alone, because clearing ~30 rows per pair while also reaching 524,288 frames needs
 * 15.7M selected rows. That is a reason to keep the two gates independent, not a reason to drop
 * the ceiling -- the ceiling is a correctness invariant and must not rest on a tuned number
 * that a later benchmark could move.
 */
public class CoveringIndexPerKeyFrameCeilingTest extends AbstractCoveringIndexQueryTest {

    // 1024 x 520 = 532,480 > 524,287: over the ceiling.
    private static final int KEYS = 1024;
    // 1000 x 520 = 520,000 < 524,287: under it, by 0.8%.
    private static final int KEYS_UNDER_CEILING = 1000;
    private static final int PARTITIONS = 520;
    private static final long ROWS = (long) KEYS * PARTITIONS;

    @After
    public void clearCrossoverOverride() {
        // Static override: MUST be cleared or it leaks into every later test class in the same
        // JVM fork.
        CoveringIndexRecordCursorFactory.setMinRowsPerKeyPartitionForTesting(-1);
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // Isolate the ceiling. This fixture holds exactly one row per (key, partition), so the
        // performance crossover (~30 rows per pair) would reject BOTH arms and neither would say
        // anything about the frame-count bound. 0 admits any density.
        CoveringIndexRecordCursorFactory.setMinRowsPerKeyPartitionForTesting(0);
    }

    @Test(timeout = 900_000)
    public void testFrameCountCeilingFallsBackToMerge() throws Exception {
        assertMemoryLeak(() -> {
            createOneRowPerKeyPerPartition();

            // Arm 1: over the ceiling. first()/last() are the aggregates that read the packed
            // row id, and grouping by the indexed key alone is what makes the unordered offer
            // acceptable to them in the first place -- so this is the exact pair of functions
            // the ceiling protects.
            final String overCeiling = inListQuery(KEYS);
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            assertSameResult(overCeiling, referenceOf(overCeiling));
            Assert.assertEquals(
                    "estimated " + (long) KEYS * PARTITIONS + " page frames, at or above the "
                            + Rows.MAX_SAFE_PARTITION_INDEX + " ceiling, yet per-key mode was chosen:"
                            + " row ids past frame 524,287 overflow into the sign bit and first()/last()"
                            + " silently return the wrong row.",
                    0,
                    CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting()
            );
            Assert.assertTrue(
                    "the gate never ran: no open was permitted to choose a mode, so this assertion"
                            + " pair proved nothing. Check the query still routes through the covering"
                            + " index and still receives the unordered offer.",
                    CoveringIndexRecordCursorFactory.getMergedModeOpensForTesting() > 0
            );

            // Arm 2: under the ceiling on the SAME data. Proves the fallback above is the
            // frame-count bound talking and not a gate that refuses per-key outright.
            final String underCeiling = inListQuery(KEYS_UNDER_CEILING);
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            assertSameResult(underCeiling, referenceOf(underCeiling));
            Assert.assertTrue(
                    "estimated " + (long) KEYS_UNDER_CEILING * PARTITIONS + " page frames, below the "
                            + Rows.MAX_SAFE_PARTITION_INDEX + " ceiling, yet the merge was chosen:"
                            + " the ceiling is rejecting shapes it should admit and the 12x per-key win"
                            + " is gone.",
                    CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting() > 0
            );
        });
    }

    private static String inListQuery(int keyCount) {
        final StringBuilder sb = new StringBuilder(
                "SELECT param_id, first(value) f, last(value) l FROM telemetry WHERE param_id IN (");
        for (int i = 0; i < keyCount; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append("'k").append(i).append('\'');
        }
        return sb.append(") ORDER BY param_id").toString();
    }

    private static String referenceOf(String indexedSql) {
        return indexedSql.replaceFirst("SELECT ", "SELECT /*+ no_index */ ");
    }

    private void assertScalar(String sql, String expected) throws Exception {
        final StringSink sink = new StringSink();
        printSql(sql, sink);
        TestUtils.assertEquals(expected, sink);
    }

    /**
     * One row per (key, partition) pair: the minimum data that makes per-key emit
     * {@code KEYS * PARTITIONS} frames. Row x maps to partition {@code (x - 1) / KEYS} and key
     * {@code (x - 1) % KEYS}, so the timestamps ascend and the loader stays on the append path.
     */
    private void createOneRowPerKeyPerPartition() throws Exception {
        execute("CREATE TABLE telemetry (" +
                "  ts TIMESTAMP," +
                "  param_id SYMBOL INDEX TYPE POSTING INCLUDE (ts, value)," +
                "  value DOUBLE" +
                ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO telemetry SELECT" +
                " ((((x - 1) / " + KEYS + ") * 86400000000L) + (((x - 1) % " + KEYS + ") * 1000L))::timestamp," +
                " concat('k', (x - 1) % " + KEYS + ")," +
                " x::double" +
                " FROM long_sequence(" + ROWS + ")");
        // Non-vacuity guard: both arms are calibrated against EXACTLY these two counts. A
        // spacing or row-count edit that moved either would slide one arm to the wrong side of
        // the ceiling and the test would keep passing while asserting the opposite thing.
        assertScalar("SELECT count() c FROM table_partitions('telemetry')", "c\n" + PARTITIONS + "\n");
        assertScalar("SELECT count_distinct(param_id) c FROM telemetry", "c\n" + KEYS + "\n");
    }
}
