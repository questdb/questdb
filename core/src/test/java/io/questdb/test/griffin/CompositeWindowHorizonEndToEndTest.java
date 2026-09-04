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

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Task 4 of the composite window/horizon-join-slave work (the DIFFERENTIAL CAPSTONE): an end-to-end
 * proof that {@code WINDOW JOIN} / {@code HORIZON JOIN} results with a composite table on the SLAVE
 * side -- the capability Task 3 wired via {@link io.questdb.griffin.engine.table.CompositeTimeFrameRecordCursor}
 * / {@link io.questdb.griffin.engine.table.CompositePageFrameRecordCursorFactory} -- EQUAL a plain twin,
 * across representative RANGE/LIST offsets, keyed and non-keyed, and {@code EXCLUDE PREVAILING}. Composite
 * on the MASTER side and on BOTH sides is re-confirmed too, and a backward (DESC) slave -- the merged
 * time-frame permutation is forward-only by construction -- is proven to be rejected LOUDLY rather than
 * silently wrong.
 * <p>
 * <b>Dataset shape</b> ({@link #createIdentitySlaveTwins}): composite master {@code cm} (twin {@code pm},
 * {@code partition by day, mexch}) and composite slave {@code cs} (twin {@code ps},
 * {@code partition by day, exch}), each an interleaved 2-cell-per-day composite spanning 3 days, built via
 * MULTIPLE commits -- a scrambled (OOO) bulk load per day, THEN two separate out-of-order backfill commits
 * into day 1 that EXTEND the already-populated X and Y cells (kept as two single-cell commits, not one
 * combined multi-cell commit, per {@code CompositeMultiCellMergeGateTest}'s documented write-path gate). A
 * second family, {@link #createExpressionSlaveTwins}, repeats a leaner version of this shape with an
 * EXPRESSION dimension ({@code partition by day, (upper(region)) AS r}) on the slave.
 * <p>
 * <b>Global timestamp uniqueness</b> (the 6a cross-cell tie-break caveat: equal-designated-ts rows from
 * different cells break ties in heap/cellKey order, which is SQL-legal for {@code ORDER BY ts} but would
 * make an ASOF-flavored differential ambiguous): every row-generating group in this class is pinned to its
 * OWN disjoint seconds-of-minute set --
 * master bulk {0,15,30,45}, slave bulk {3,18,33,48} (a flat +3s offset from the master grid), slave
 * cell-X-extend {2,17,32} (+2s from three of day 1's own master timestamps), slave cell-Y-extend
 * {58,13,28} (-2s from the same three anchors). These four sets are pairwise disjoint by construction, so
 * no two rows across {@code cm}/{@code pm}/{@code cs}/{@code ps} can ever collide, regardless of day, hour
 * or minute -- an easy, inspectable proof rather than a coincidence of the generated values. The same +-2s
 * offsets are also deliberately INSIDE the 5-second RANGE windows the tests below use, so a master row's
 * window/horizon lookup genuinely gathers rows from the extended cell alongside its sibling -- real
 * cross-cell interleaving exercised during the join walk itself, not merely data that sits unread in the
 * table. Verified empirically (not just by the green {@code assertSqlCursors}): the default INCLUDE
 * PREVAILING mode additively carries forward the nearest prior same-key slave value alongside any in-range
 * match ({@code WindowJoinPrevailingCache}), so several keyed-window rows in
 * {@link #testWindowJoinRangeKeyedSlaveMasterAndBothMatchPlainTwin} resolve their "prevailing" value to
 * one of these very cell-extend rows -- a diagnostic {@code EXCLUDE PREVAILING} probe confirmed every
 * plain in-range sum matches this class's hand-derived expectation exactly, and the default-mode "extra"
 * amount traces byte-for-byte to a specific extend row's price, proving the prevailing backward-scan
 * itself resolves correctly against the composite's merged, cross-cell permutation.
 */
public class CompositeWindowHorizonEndToEndTest extends AbstractCairoTest {

    // ==========================================================================================
    // IDENTITY dimension: WINDOW JOIN -- RANGE, keyed / non-keyed / EXCLUDE PREVAILING
    // ==========================================================================================

    /**
     * WINDOW JOIN, RANGE BETWEEN 5 SECONDS PRECEDING AND 5 SECONDS FOLLOWING, KEYED (on {@code sym}), the
     * grammar's default (INCLUDE) PREVAILING mode. Composite SLAVE, composite MASTER, and BOTH composite
     * must all equal the all-plain oracle.
     */
    @Test
    public void testWindowJoinRangeKeyedSlaveMasterAndBothMatchPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createIdentitySlaveTwins();
            sqlExecutionContext.setParallelWindowJoinEnabled(true);
            final String q = "select m.ts, m.sym, m.qty, sum(s.price) as window_price from %s m " +
                    "window join %s s on (m.sym = s.sym) " +
                    "range between 5 seconds preceding and 5 seconds following " +
                    "order by m.ts, m.sym";
            final String oracle = String.format(q, "pm", "ps");
            assertSqlCursors(oracle, String.format(q, "pm", "cs")); // composite SLAVE
            assertSqlCursors(oracle, String.format(q, "cm", "ps")); // composite MASTER
            assertSqlCursors(oracle, String.format(q, "cm", "cs")); // BOTH composite

            // Confirmation: routed to the SERIAL window join, never the async path (which would NPE on
            // the null per-worker concurrent cursor for a composite slave).
            assertQuery(String.format(q, "cm", "cs")).noLeakCheck().assertsPlanNotContaining("Async");
        });
    }

    /**
     * WINDOW JOIN, same RANGE, NON-KEYED -- no {@code ON} clause at all, so every master row aggregates
     * over every slave row (any {@code sym}) within its 5-second range.
     */
    @Test
    public void testWindowJoinRangeNonKeyedSlaveAndBothMatchPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createIdentitySlaveTwins();
            final String q = "select m.ts, m.qty, sum(s.price) as window_price from %s m " +
                    "window join %s s " +
                    "range between 5 seconds preceding and 5 seconds following " +
                    "order by m.ts";
            final String oracle = String.format(q, "pm", "ps");
            assertSqlCursors(oracle, String.format(q, "pm", "cs")); // composite SLAVE
            assertSqlCursors(oracle, String.format(q, "cm", "cs")); // BOTH composite
        });
    }

    /**
     * WINDOW JOIN, same RANGE and key, explicit {@code EXCLUDE PREVAILING}.
     */
    @Test
    public void testWindowJoinRangeExcludePrevailingSlaveAndBothMatchPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createIdentitySlaveTwins();
            final String q = "select m.ts, m.sym, m.qty, sum(s.price) as window_price from %s m " +
                    "window join %s s on (m.sym = s.sym) " +
                    "range between 5 seconds preceding and 5 seconds following exclude prevailing " +
                    "order by m.ts, m.sym";
            final String oracle = String.format(q, "pm", "ps");
            assertSqlCursors(oracle, String.format(q, "pm", "cs")); // composite SLAVE
            assertSqlCursors(oracle, String.format(q, "cm", "cs")); // BOTH composite
        });
    }

    // ==========================================================================================
    // IDENTITY dimension: HORIZON JOIN -- RANGE, keyed / non-keyed, and LIST offsets
    // ==========================================================================================

    /**
     * HORIZON JOIN, RANGE FROM -3s TO 3s STEP 1s, KEYED (on {@code sym}). The offset span deliberately
     * covers the +-2s/+3s anchors from {@link #createIdentitySlaveTwins}, so several buckets resolve their
     * per-offset ASOF lookup against the day-1 cell-extend rows specifically. Composite SLAVE, composite
     * MASTER, and BOTH composite must all equal the all-plain oracle.
     */
    @Test
    public void testHorizonJoinRangeKeyedSlaveMasterAndBothMatchPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createIdentitySlaveTwins();
            sqlExecutionContext.setParallelHorizonJoinEnabled(true);
            final String q = "select h.offset, avg(s.price), sum(m.qty), count() from %s m " +
                    "horizon join %s s on (m.sym = s.sym) " +
                    "range from -3s to 3s step 1s as h " +
                    "order by h.offset";
            final String oracle = String.format(q, "pm", "ps");
            assertSqlCursors(oracle, String.format(q, "pm", "cs")); // composite SLAVE
            assertSqlCursors(oracle, String.format(q, "cm", "ps")); // composite MASTER
            assertSqlCursors(oracle, String.format(q, "cm", "cs")); // BOTH composite

            assertQuery(String.format(q, "cm", "cs")).noLeakCheck().assertsPlanNotContaining("Async");
        });
    }

    /**
     * HORIZON JOIN, same RANGE, NON-KEYED -- no {@code ON} clause, pure ASOF-by-timestamp.
     */
    @Test
    public void testHorizonJoinRangeNonKeyedSlaveAndBothMatchPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createIdentitySlaveTwins();
            final String q = "select h.offset, avg(s.price), count() from %s m " +
                    "horizon join %s s " +
                    "range from -3s to 3s step 1s as h " +
                    "order by h.offset";
            final String oracle = String.format(q, "pm", "ps");
            assertSqlCursors(oracle, String.format(q, "pm", "cs")); // composite SLAVE
            assertSqlCursors(oracle, String.format(q, "cm", "cs")); // BOTH composite
        });
    }

    /**
     * HORIZON JOIN, explicit {@code LIST} offsets -- the same four seconds ({@code -2s, 0s, 2s, 3s}) that
     * exercise the cell-extend/bulk anchors exactly, KEYED.
     */
    @Test
    public void testHorizonJoinListOffsetsKeyedSlaveAndBothMatchPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createIdentitySlaveTwins();
            final String q = "select h.offset, avg(s.price), sum(m.qty), count() from %s m " +
                    "horizon join %s s on (m.sym = s.sym) " +
                    "list (-2s, 0s, 2s, 3s) as h " +
                    "order by h.offset";
            final String oracle = String.format(q, "pm", "ps");
            assertSqlCursors(oracle, String.format(q, "pm", "cs")); // composite SLAVE
            assertSqlCursors(oracle, String.format(q, "cm", "cs")); // BOTH composite
        });
    }

    // ==========================================================================================
    // EXPRESSION dimension variant (partition by day, (upper(region)) AS r)
    // ==========================================================================================

    /**
     * WINDOW JOIN over the EXPRESSION-dimensioned slave {@link #createExpressionSlaveTwins}. Same RANGE
     * and keying as {@link #testWindowJoinRangeKeyedSlaveMasterAndBothMatchPlainTwin}; the composite MASTER
     * here is the (IDENTITY-dimension) {@code cm} -- the point of this test is the EXPRESSION slave, not a
     * second master-side dimension kind.
     */
    @Test
    public void testExpressionWindowJoinRangeKeyedSlaveMasterAndBothMatchPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createExpressionSlaveTwins();
            final String q = "select m.ts, m.sym, m.qty, sum(s.price) as window_price from %s m " +
                    "window join %s s on (m.sym = s.sym) " +
                    "range between 5 seconds preceding and 5 seconds following " +
                    "order by m.ts, m.sym";
            final String oracle = String.format(q, "pm", "ps");
            assertSqlCursors(oracle, String.format(q, "pm", "cs")); // composite EXPRESSION slave
            assertSqlCursors(oracle, String.format(q, "cm", "ps")); // composite IDENTITY master
            assertSqlCursors(oracle, String.format(q, "cm", "cs")); // both composite
        });
    }

    /**
     * HORIZON JOIN over the same EXPRESSION-dimensioned slave.
     */
    @Test
    public void testExpressionHorizonJoinRangeKeyedSlaveAndBothMatchPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createExpressionSlaveTwins();
            final String q = "select h.offset, avg(s.price), sum(m.qty), count() from %s m " +
                    "horizon join %s s on (m.sym = s.sym) " +
                    "range from -3s to 3s step 1s as h " +
                    "order by h.offset";
            final String oracle = String.format(q, "pm", "ps");
            assertSqlCursors(oracle, String.format(q, "pm", "cs")); // composite EXPRESSION slave
            assertSqlCursors(oracle, String.format(q, "cm", "cs")); // both composite
        });
    }

    // ==========================================================================================
    // DESC / backward slave: must be rejected LOUDLY, never silently wrong
    // ==========================================================================================

    /**
     * The merged time-frame permutation ({@code CompositeTimeFrameRecordCursor}) is forward-only by
     * construction (built once, ascending, over the per-day cross-cell heap). Wrapping the slave in a
     * subquery with {@code order by ts desc} is the only way to make the compiler choose a genuinely
     * backward-scanned factory for a table used as a WINDOW/HORIZON slave (a bare table reference is
     * always compiled forward here). Both WINDOW and HORIZON JOIN must reject this loudly -- a clear
     * {@code SqlException}, never a silent fall-through to storage order or otherwise wrong rows.
     * <p>
     * The PLAIN slave, backward-ordered the exact same way, is asserted to be rejected too (same
     * mechanism): this is the generic ASC-only time-series-join constraint
     * ({@code SqlCodeGenerator#validateBothTimestampOrders}, checked before the composite-specific
     * {@code supportsTimeFrameCursor()} gate is even consulted) -- not a composite-only brittleness that
     * might be masking some other, distinct bug.
     */
    @Test
    public void testDescBackwardSlaveIsLoudNotSilentlyWrong() throws Exception {
        assertMemoryLeak(() -> {
            createIdentitySlaveTwins();

            final String windowDesc = "select m.ts, m.sym, m.qty, sum(s.price) as window_price from cm m " +
                    "window join (select * from %s order by ts desc) s on (m.sym = s.sym) " +
                    "range between 5 seconds preceding and 5 seconds following " +
                    "order by m.ts, m.sym";
            assertQuery(String.format(windowDesc, "cs")).noLeakCheck().failsWith("ASC timestamp order");
            assertQuery(String.format(windowDesc, "ps")).noLeakCheck().failsWith("ASC timestamp order");

            final String horizonDesc = "select h.offset, avg(s.price), sum(m.qty), count() from cm m " +
                    "horizon join (select * from %s order by ts desc) s on (m.sym = s.sym) " +
                    "range from -3s to 3s step 1s as h " +
                    "order by h.offset";
            assertQuery(String.format(horizonDesc, "cs")).noLeakCheck().failsWith("ASC timestamp order");
            assertQuery(String.format(horizonDesc, "ps")).noLeakCheck().failsWith("ASC timestamp order");
        });
    }

    // ==========================================================================================
    // Dataset builders
    // ==========================================================================================

    /**
     * Builds the EXPRESSION-dimension family: composite MASTER {@code cm}/{@code pm} (IDENTITY
     * {@code mexch} dimension -- unrelated to the EXPRESSION under test, only the SLAVE needs to carry
     * it) and the EXPRESSION composite SLAVE {@code cs} (twin {@code ps} carrying a real, precomputed
     * {@code r varchar} column -- the established idiom, see
     * {@code CompositeReadEndToEndTest#createExpressionLifecycleTwins}).
     * <p>
     * Lighter than {@link #createIdentitySlaveTwins} (2 days, not 3) since EXPRESSION's own value-add here
     * is specifically the out-of-order cell-extend, already proven for IDENTITY; the two extend commits
     * additionally mix casing ({@code Us}/{@code US}, {@code Eu}/{@code EU}) as a bonus, exercising the
     * multi-spelling collapse alongside the extend. Same +-2/+3-second disjoint-offset scheme as
     * {@link #createIdentitySlaveTwins} keeps every timestamp globally unique.
     */
    private void createExpressionSlaveTwins() throws SqlException {
        execute("create table cm (ts timestamp, mexch symbol, sym symbol, qty double) timestamp(ts) partition by day, mexch wal");
        execute("create table pm (ts timestamp, mexch symbol, sym symbol, qty double) timestamp(ts) partition by day wal");
        execute("create table cs (ts timestamp, region symbol, sym symbol, price double) timestamp(ts) partition by day, (upper(region)) AS r wal");
        execute("create table ps (ts timestamp, region symbol, sym symbol, price double, r varchar) timestamp(ts) partition by day wal");

        final String[] days = {"2021-10-01T00:00:00.000000Z", "2021-10-02T00:00:00.000000Z"};
        for (int i = 0; i < days.length; i++) {
            final long qtyOffset = i * 1000L;
            final String masterSelect =
                    "select ('" + days[i] + "'::timestamp + (x - 1) * 15000000L)::timestamp ts, " +
                            "case when x % 2 = 0 then 'X' else 'Y' end mexch, " +
                            "case when x % 3 = 0 then 'A' when x % 3 = 1 then 'B' else 'C' end sym, " +
                            "(x + " + qtyOffset + ")::double qty " +
                            "from long_sequence(40) order by x desc";
            execute("insert into cm " + masterSelect);
            execute("insert into pm " + masterSelect);
            drainWalQueue();

            final String slaveSelectC =
                    "select ('" + days[i] + "'::timestamp + (x - 1) * 15000000L + 3000000L)::timestamp ts, " +
                            "case when x % 2 = 0 then 'us' else 'eu' end region, " +
                            "case when x % 3 = 0 then 'A' when x % 3 = 1 then 'B' else 'C' end sym, " +
                            "(x + " + qtyOffset + ")::double price " +
                            "from long_sequence(40) order by x desc";
            final String slaveSelectP =
                    "select ('" + days[i] + "'::timestamp + (x - 1) * 15000000L + 3000000L)::timestamp ts, " +
                            "case when x % 2 = 0 then 'us' else 'eu' end region, " +
                            "case when x % 3 = 0 then 'A' when x % 3 = 1 then 'B' else 'C' end sym, " +
                            "(x + " + qtyOffset + ")::double price, " +
                            "case when x % 2 = 0 then 'US' else 'EU' end r " +
                            "from long_sequence(40) order by x desc";
            execute("insert into cs " + slaveSelectC);
            execute("insert into ps " + slaveSelectP);
            drainWalQueue();
        }

        // OOO backfill EXTENDING day-1's US cell (+2s from three of day 1's own timestamps), mixed
        // casing Us/US -- also exercises the EXPRESSION multi-spelling collapse alongside the extend.
        execute("insert into cs values " +
                "('2021-10-01T00:01:02.000000Z','Us','A',900.0), " +
                "('2021-10-01T00:02:17.000000Z','US','B',901.0), " +
                "('2021-10-01T00:03:32.000000Z','Us','C',902.0)");
        execute("insert into ps values " +
                "('2021-10-01T00:01:02.000000Z','Us','A',900.0,'US'), " +
                "('2021-10-01T00:02:17.000000Z','US','B',901.0,'US'), " +
                "('2021-10-01T00:03:32.000000Z','Us','C',902.0,'US')");
        drainWalQueue();

        // A SEPARATE commit extending day-1's EU cell (-2s from the same anchors), mixed casing Eu/EU --
        // kept as its own commit, not combined with the US extend above, per
        // CompositeMultiCellMergeGateTest's documented write-path gate (see createIdentitySlaveTwins).
        execute("insert into cs values " +
                "('2021-10-01T00:00:58.000000Z','Eu','B',910.0), " +
                "('2021-10-01T00:02:13.000000Z','EU','C',911.0), " +
                "('2021-10-01T00:03:28.000000Z','Eu','A',912.0)");
        execute("insert into ps values " +
                "('2021-10-01T00:00:58.000000Z','Eu','B',910.0,'EU'), " +
                "('2021-10-01T00:02:13.000000Z','EU','C',911.0,'EU'), " +
                "('2021-10-01T00:03:28.000000Z','Eu','A',912.0,'EU')");
        drainWalQueue();
    }

    /**
     * Builds the IDENTITY-dimension family: composite MASTER {@code cm} (twin {@code pm},
     * {@code partition by day, mexch}) and composite SLAVE {@code cs} (twin {@code ps},
     * {@code partition by day, exch}) -- both interleaved 2-cell-per-day composites, multi-day, built
     * out-of-order.
     * <p>
     * Master: 3 separate day commits (2021-09-01/02/03), 40 rows/day at a 15-second cadence, {@code mexch}
     * alternating X/Y by row parity (2 interleaved cells/day), each day's insertion order SCRAMBLED
     * ({@code order by x desc}) so the WAL write path O3-sorts every cell. Seconds-of-minute always in
     * {0,15,30,45}.
     * <p>
     * Slave: the same 3-day/40-row/15-second-cadence shape, offset a flat +3 SECONDS from the master grid
     * (seconds-of-minute always in {3,18,33,48} -- disjoint from the master set regardless of day/hour/
     * minute, so no slave-bulk row can ever collide with a master row) -- PLUS two separate out-of-order
     * backfill commits into day 1 that EXTEND the already-populated X and Y cells: three rows each at +2s
     * / -2s from three of day 1's own master timestamps (x=5,10,15, i.e. 00:01:00/00:02:15/00:03:30),
     * landing well inside each cell's already-written range (a genuine interior O3 merge, not a tail
     * append). Extend-X seconds-of-minute: {2,17,32}. Extend-Y: {58,13,28}. All four sets -- master
     * {0,15,30,45}, slave-bulk {3,18,33,48}, extend-X {2,17,32}, extend-Y {58,13,28} -- are pairwise
     * disjoint, so every timestamp across {@code cm}/{@code pm}/{@code cs}/{@code ps} is globally unique
     * (the 6a tie-break caveat never arises). The two extend commits are kept SEPARATE (one per cell), not
     * combined into a single multi-cell commit, per {@code CompositeMultiCellMergeGateTest}: a single
     * commit whose out-of-order rows genuinely interleave across 2+ already-populated cells hits a real,
     * separately-gated write-path issue unrelated to this task's read-side subject matter.
     */
    private void createIdentitySlaveTwins() throws SqlException {
        execute("create table cm (ts timestamp, mexch symbol, sym symbol, qty double) timestamp(ts) partition by day, mexch wal");
        execute("create table pm (ts timestamp, mexch symbol, sym symbol, qty double) timestamp(ts) partition by day wal");
        execute("create table cs (ts timestamp, exch symbol, sym symbol, price double) timestamp(ts) partition by day, exch wal");
        execute("create table ps (ts timestamp, exch symbol, sym symbol, price double) timestamp(ts) partition by day wal");

        final String[] days = {"2021-09-01T00:00:00.000000Z", "2021-09-02T00:00:00.000000Z", "2021-09-03T00:00:00.000000Z"};
        for (int i = 0; i < days.length; i++) {
            final long qtyOffset = i * 1000L;
            final String masterSelect =
                    "select ('" + days[i] + "'::timestamp + (x - 1) * 15000000L)::timestamp ts, " +
                            "case when x % 2 = 0 then 'X' else 'Y' end mexch, " +
                            "case when x % 3 = 0 then 'A' when x % 3 = 1 then 'B' else 'C' end sym, " +
                            "(x + " + qtyOffset + ")::double qty " +
                            "from long_sequence(40) order by x desc";
            execute("insert into cm " + masterSelect);
            execute("insert into pm " + masterSelect);
            drainWalQueue();

            final String slaveSelect =
                    "select ('" + days[i] + "'::timestamp + (x - 1) * 15000000L + 3000000L)::timestamp ts, " +
                            "case when x % 2 = 0 then 'X' else 'Y' end exch, " +
                            "case when x % 3 = 0 then 'A' when x % 3 = 1 then 'B' else 'C' end sym, " +
                            "(x + " + qtyOffset + ")::double price " +
                            "from long_sequence(40) order by x desc";
            execute("insert into cs " + slaveSelect);
            execute("insert into ps " + slaveSelect);
            drainWalQueue();
        }

        // OOO backfill EXTENDING the already-populated day-1 X cell: +2s from x=5,10,15's own timestamps
        // (00:01:00/00:02:15/00:03:30), interior to the cell's existing range.
        final String extendX = " values " +
                "('2021-09-01T00:01:02.000000Z','X','A',900.0), " +
                "('2021-09-01T00:02:17.000000Z','X','B',901.0), " +
                "('2021-09-01T00:03:32.000000Z','X','C',902.0)";
        execute("insert into cs" + extendX);
        execute("insert into ps" + extendX);
        drainWalQueue();

        // A SEPARATE commit extending day-1's Y cell (-2s from the same three anchors).
        final String extendY = " values " +
                "('2021-09-01T00:00:58.000000Z','Y','B',910.0), " +
                "('2021-09-01T00:02:13.000000Z','Y','C',911.0), " +
                "('2021-09-01T00:03:28.000000Z','Y','A',912.0)";
        execute("insert into cs" + extendY);
        execute("insert into ps" + extendY);
        drainWalQueue();
    }
}
