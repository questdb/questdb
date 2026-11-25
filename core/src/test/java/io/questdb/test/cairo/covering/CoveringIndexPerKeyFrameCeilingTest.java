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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.std.ObjList;
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
 * Merged mode reaches that ceiling only through the partition frames: one per partition, per
 * {@code ts IN} window intersecting a partition, and per composite-partition piece. Per-key
 * mode MULTIPLIES those frames by the key count, which is what brings ordinary shapes into
 * range -- 30 symbols over two years of hourly partitions is 525,600 (key, partition) pairs,
 * and 1,024 symbols over 520 daily {@code ts IN} windows is 532,480 (key, window) pairs even
 * in two YEAR partitions.
 * <p>
 * <b>Why the shape is this big.</b> Genuine corruption needs the scan to actually EMIT more
 * than 524,287 frames. Per-key emits one frame per non-empty (key, partition frame) pair, so
 * the product of the key count and the partition frames must clear the ceiling and the table
 * must hold at least one row per pair. {@code setMaxRowsPerFrameForTesting} cannot supply a
 * cheaper shape, because shrinking the row cap raises the bound's split term, which then decides
 * the mode in place of the pairs this class is about. The free choice is only how to split the
 * product: {@link #KEYS} x {@link #PARTITIONS} keeps the partition count (the expensive axis
 * to create) low while the IN-list stays a few kilobytes of SQL. The mode choice itself needs
 * no such fixture: the frame bound counts (window, partition) pairs whether or not they hold
 * rows, so the small-fixture tests below cross the ceiling with 1,025 rows.
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
 * density alone. Clearing even the LOWEST crossover any consumer faces -- 64 rows per pair, the
 * base with a one-pass consumer -- while also reaching 524,288 frames needs 524,288 x 64 = ~34M
 * selected rows, and a four-aggregate vectorized consumer at 256 needs ~134M. This fixture holds
 * 532,480. That is a reason to keep the two gates independent, not a reason to drop the ceiling
 * -- the ceiling is a correctness invariant and must not rest on a tuned number that a later
 * benchmark could move, in either direction.
 */
public class CoveringIndexPerKeyFrameCeilingTest extends AbstractCoveringIndexQueryTest {

    // 1024 x 520 = 532,480 > 524,287: over the ceiling.
    private static final int KEYS = 1024;
    // 1000 x 520 = 520,000 < 524,287: under it, by 0.8%.
    private static final int KEYS_UNDER_CEILING = 1000;
    private static final int PARTITIONS = 520;
    private static final long ROWS = (long) KEYS * PARTITIONS;
    // The small fixture: KEYS rows on its first day, one row on its last, SPAN_DAYS days apart.
    private static final int SMALL_FIXTURE_ROWS = KEYS + 1;
    private static final int SPAN_DAYS = 520;

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
        // performance crossover (256 rows per pair) would reject BOTH arms and neither would say
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

    @Test
    public void testIntervalFrameBoundFollowsBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            createSmallFixture("t_bind", false);
            final String sql = "SELECT sym, first(value) f, last(value) l, count() c FROM t_bind"
                    + " WHERE sym IN (" + keyList(KEYS) + ") AND ts IN $1 ORDER BY sym";
            try (RecordCursorFactory factory = select(sql)) {
                // 520 windows: KEYS x 520 (window, partition) pairs, over the ceiling.
                bindVariableService.setStr(0, windows(SPAN_DAYS));
                assertMode(false, KEYS, SPAN_DAYS);
                CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
                assertFactory(factory).withContext(sqlExecutionContext).expectSize().returns(expectedSmall(KEYS, true, false));
                assertModeCounters(false);

                // 500 windows on the same factory: KEYS x 500 pairs, under it. The last row,
                // on day 519, falls outside the windows.
                bindVariableService.setStr(0, windows(500));
                assertMode(true, KEYS, 500);
                CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
                assertFactory(factory).withContext(sqlExecutionContext).expectSize().returns(expectedSmall(KEYS, false, false));
                assertModeCounters(true);
            }
        });
    }

    @Test
    public void testIntervalFrameBoundFollowsNewPartition() throws Exception {
        assertMemoryLeak(() -> {
            createSmallFixture("t_grow", false);
            // 560 windows, but the data ends on day 519: the frame bound clamps the windows to
            // the data, so only 520 of them count -- KEYS_UNDER_CEILING x 520, under the ceiling.
            final String sql = groupByQuery("t_grow", KEYS_UNDER_CEILING, windows(560));
            try (RecordCursorFactory factory = select(sql)) {
                assertMode(true, KEYS_UNDER_CEILING, SPAN_DAYS);
                CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
                assertFactory(factory).withContext(sqlExecutionContext).expectSize().returns(expectedSmall(KEYS_UNDER_CEILING, true, false));
                assertModeCounters(true);

                // A row on day 559 opens a new MONTH partition and brings all 560 windows into
                // the data range: KEYS_UNDER_CEILING x 560, over the ceiling, on the same factory.
                execute("INSERT INTO t_grow VALUES (dateadd('d', 559, '2024-01-01'::timestamp), 'k1', 2000.0)");
                assertScalar("SELECT count() c FROM table_partitions('t_grow')", "c\n3\n");
                assertMode(false, KEYS_UNDER_CEILING, 560);
                CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
                assertFactory(factory).withContext(sqlExecutionContext).expectSize().returns(expectedSmall(KEYS_UNDER_CEILING, true, true));
                assertModeCounters(false);
            }
        });
    }

    @Test
    public void testIntervalFrameBoundSelectsMode() throws Exception {
        assertMemoryLeak(() -> assertIntervalFrameBoundSelectsMode("t_small", false));
    }

    @Test
    public void testIntervalFrameBoundSelectsModeWal() throws Exception {
        assertMemoryLeak(() -> assertIntervalFrameBoundSelectsMode("t_small_wal", true));
    }

    @Test(timeout = 900_000)
    public void testIntervalSplitFramesCountTowardCeiling() throws Exception {
        assertMemoryLeak(() -> {
            // KEYS rows on each of SPAN_DAYS days, all in two YEAR partitions: the partition
            // count says 2 x KEYS frames, but each of the 520 daily windows cuts its own frame,
            // so a per-key scan emits KEYS x 520 = 532,480 frames.
            execute("CREATE TABLE t_windows (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY YEAR BYPASS WAL");
            execute("INSERT INTO t_windows SELECT ('2024-01-01'::timestamp + ((x - 1) / " + KEYS + ") * 86_400_000_000L"
                    + " + ((x - 1) % " + KEYS + ") * 1000L)::timestamp, concat('k', (x - 1) % " + KEYS + "), x::double"
                    + " FROM long_sequence(" + (long) KEYS * SPAN_DAYS + ")");
            assertScalar("SELECT count() c FROM table_partitions('t_windows')", "c\n2\n");
            final String sql = groupByQuery("t_windows", KEYS, windows(SPAN_DAYS));
            CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
            assertSameResult(sql, referenceOf(sql));
            assertModeCounters(false);
        });
    }

    /**
     * Asserts the frame-ceiling side of a (keys, windows) shape on the small fixture, computed
     * here rather than read off the factory: one frame per (key, window) pair, each window inside
     * one partition, plus at most one split frame per SMALL_FIXTURE_ROWS rows. Both terms keep
     * the shape on the asserted side of the ceiling with room to spare.
     */
    private static void assertMode(boolean isUnderCeiling, int keyCount, int windowsInData) {
        final long pairs = (long) keyCount * windowsInData;
        if (isUnderCeiling) {
            Assert.assertTrue(pairs + SMALL_FIXTURE_ROWS < Rows.MAX_SAFE_PARTITION_INDEX);
        } else {
            Assert.assertTrue(pairs >= Rows.MAX_SAFE_PARTITION_INDEX);
        }
    }

    private static void assertModeCounters(boolean isPerKey) {
        final long perKeyOpens = CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting();
        final long mergedOpens = CoveringIndexRecordCursorFactory.getMergedModeOpensForTesting();
        if (isPerKey) {
            Assert.assertTrue("perKeyOpens=" + perKeyOpens, perKeyOpens > 0);
            Assert.assertEquals("mergedOpens", 0, mergedOpens);
        } else {
            Assert.assertEquals("perKeyOpens", 0, perKeyOpens);
            Assert.assertTrue("mergedOpens=" + mergedOpens, mergedOpens > 0);
        }
    }

    /**
     * The small fixture's GROUP BY result, from plain Java: key i has value i on day 0, k0 also
     * has 1024.0 on day 519 when {@code hasLastDay}, and k1 also has 2000.0 on day 559 when
     * {@code hasDay559}. Rows sort by the symbol's text.
     */
    private static String expectedSmall(int keyCount, boolean hasLastDay, boolean hasDay559) {
        final ObjList<String> names = new ObjList<>();
        for (int i = 0; i < keyCount; i++) {
            names.add("k" + i);
        }
        names.sort(String::compareTo);
        final StringBuilder sb = new StringBuilder("sym\tf\tl\tc\n");
        for (int i = 0, n = names.size(); i < n; i++) {
            final String name = names.getQuick(i);
            final int key = Integer.parseInt(name.substring(1));
            double last = key;
            int count = 1;
            if (key == 0 && hasLastDay) {
                last = 1024.0;
                count++;
            }
            if (key == 1 && hasDay559) {
                last = 2000.0;
                count++;
            }
            sb.append(name).append('\t').append((double) key).append('\t').append(last).append('\t').append(count).append('\n');
        }
        return sb.toString();
    }

    private static String groupByQuery(String table, int keyCount, String windows) {
        return "SELECT sym, first(value) f, last(value) l, count() c FROM " + table
                + " WHERE sym IN (" + keyList(keyCount) + ") AND ts IN '" + windows + "' ORDER BY sym";
    }

    private static String keyList(int keyCount) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keyCount; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append("'k").append(i).append('\'');
        }
        return sb.toString();
    }

    // One-hour windows at midnight of each of `count` consecutive days from 2024-01-01.
    private static String windows(int count) {
        return "2024-01-01T00:00:00;1h;1d;" + count;
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

    private void assertIntervalFrameBoundSelectsMode(String table, boolean isWal) throws Exception {
        createSmallFixture(table, isWal);

        // Over: KEYS x 520 (window, partition) pairs reach the ceiling, although the table has
        // only two partitions and the scan would emit 1,025 frames.
        assertMode(false, KEYS, SPAN_DAYS);
        assertGroupByMode(groupByQuery(table, KEYS, windows(SPAN_DAYS)), expectedSmall(KEYS, true, false), false);

        // Under: KEYS_UNDER_CEILING x 520 pairs stay below it.
        assertMode(true, KEYS_UNDER_CEILING, SPAN_DAYS);
        assertGroupByMode(
                groupByQuery(table, KEYS_UNDER_CEILING, windows(SPAN_DAYS)),
                expectedSmall(KEYS_UNDER_CEILING, true, false),
                true
        );

        // No window: a full scan's frames are its two partitions, KEYS x 2 pairs.
        assertGroupByMode(
                "SELECT sym, first(value) f, last(value) l, count() c FROM " + table
                        + " WHERE sym IN (" + keyList(KEYS) + ") ORDER BY sym",
                expectedSmall(KEYS, true, false),
                true
        );

        // A window past the data: no (window, partition) pair, no frame, no row.
        assertGroupByMode(groupByQuery(table, KEYS, "2030-01-01;1d"), "sym\tf\tl\tc\n", true);
    }

    private void assertGroupByMode(String sql, String expected, boolean isPerKey) throws Exception {
        CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
        assertQuery(sql).noLeakCheck().expectSize().returns(expected);
        assertModeCounters(isPerKey);
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

    /**
     * Every key once, on the first day, plus k0 again on day 519: 1,025 rows in two MONTH
     * partitions whose data spans all 520 daily windows.
     */
    private void createSmallFixture(String table, boolean isWal) throws Exception {
        execute("CREATE TABLE " + table + " (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY MONTH " + (isWal ? "WAL" : "BYPASS WAL"));
        execute("INSERT INTO " + table + " SELECT ('2024-01-01'::timestamp + (x - 1) * 1000L)::timestamp,"
                + " concat('k', x - 1), (x - 1)::double FROM long_sequence(" + KEYS + ")");
        execute("INSERT INTO " + table + " VALUES (dateadd('d', " + (SPAN_DAYS - 1) + ", '2024-01-01'::timestamp), 'k0', 1024.0)");
        if (isWal) {
            drainWalQueue();
        }
        assertScalar("SELECT count() c FROM table_partitions('" + table + "')", "c\n2\n");
        assertScalar("SELECT count() c FROM " + table, "c\n" + SMALL_FIXTURE_ROWS + "\n");
    }
}
