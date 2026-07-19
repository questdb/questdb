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

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Task 6b of composite partitioning (the read-side AUDIT + VERIFY capstone): with Task 6a's per-day
 * k-way cross-cell merge cursor in place ({@code CompositePageFrameRecordCursorFactory} /
 * {@code CompositeMergePartitionRecordCursor}), this suite is the differential proof that EVERY
 * order-dependent query shape over a composite table -- SAMPLE BY (+FILL), LATEST ON, the five
 * time-series join kinds, GROUP BY, and a tail LIMIT -- equals a plain twin holding the same rows, and
 * that the two index-based scan sites 6a deferred (indexed WHERE predicates, indexed LATEST BY) are
 * either routed correctly or LOUD-GATED (never silently wrong).
 * <p>
 * All datasets are interleaved multi-cell (2 {@code exch} cells/day, 3 days), inserted SCRAMBLED so
 * the write path O3-sorts each cell, with globally UNIQUE timestamps -- so equal-designated-ts
 * cross-cell tie-breaks (heap/cellKey order, SQL-legal for plain {@code ORDER BY ts} but OBSERVABLE to
 * ASOF/LT join semantics, per the 6a review) never arise and every comparison against the plain twin
 * is unambiguous.
 */
public class CompositeReadShapesTest extends AbstractCairoTest {

    // ==========================================================================================
    // Step 1: SAMPLE BY (1h & 1d, keyed & non-keyed, FILL(PREV|LINEAR|NULL|VALUE))
    // ==========================================================================================

    @Test
    public void testSampleBy1hNonKeyedEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins(false);
            assertSqlCursors(
                    "select ts, sum(px), count() from p sample by 1h",
                    "select ts, sum(px), count() from c sample by 1h"
            );
        });
    }

    @Test
    public void testSampleBy1dNonKeyedEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins(false);
            assertSqlCursors(
                    "select ts, sum(px), count(), min(px), max(px) from p sample by 1d",
                    "select ts, sum(px), count(), min(px), max(px) from c sample by 1d"
            );
        });
    }

    /**
     * Keyed SAMPLE BY groups by (bucket, key) through an internal hash map; the ORDER distinct keys
     * are first seen within one bucket is an implementation detail that can legitimately differ
     * between the composite merge and the plain single-stream scan even when every (bucket,key)
     * aggregate value is identical. The trailing {@code order by ts, sym} makes the row order
     * canonical on BOTH sides so the lock-step {@code assertSqlCursors} compares values, not
     * incidental hash-map emission order.
     */
    @Test
    public void testSampleBy1hKeyedEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins(false);
            assertSqlCursors(
                    "select ts, sym, sum(px), count() from p sample by 1h order by ts, sym",
                    "select ts, sym, sum(px), count() from c sample by 1h order by ts, sym"
            );
        });
    }

    @Test
    public void testSampleByKeyedByDimensionColumnEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins(false);
            // key by the composite table's own partitioning dimension (exch) as well as the ordinary
            // sym column -- both must merge correctly.
            assertSqlCursors(
                    "select ts, exch, sum(px), count() from p sample by 1h order by ts, exch",
                    "select ts, exch, sum(px), count() from c sample by 1h order by ts, exch"
            );
        });
    }

    /**
     * Every one of the four FILL modes over a keyed SAMPLE BY, where per-key gaps are real (each sym
     * value is present in roughly 1 of every 3 buckets by construction -- see
     * {@link #createSingleTableTwins}), so FILL genuinely interpolates/carries/nulls/defaults missing
     * buckets rather than being a no-op.
     */
    @Test
    public void testSampleByFillModesEqualPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins(false);

            assertSqlCursors(
                    "select ts, sym, sum(px) from p sample by 1h fill(prev) order by ts, sym",
                    "select ts, sym, sum(px) from c sample by 1h fill(prev) order by ts, sym"
            );
            assertSqlCursors(
                    "select ts, sym, sum(px) from p sample by 1h fill(linear) order by ts, sym",
                    "select ts, sym, sum(px) from c sample by 1h fill(linear) order by ts, sym"
            );
            assertSqlCursors(
                    "select ts, sym, sum(px) from p sample by 1h fill(null) order by ts, sym",
                    "select ts, sym, sum(px) from c sample by 1h fill(null) order by ts, sym"
            );
            assertSqlCursors(
                    "select ts, sym, sum(px) from p sample by 1h fill(0) order by ts, sym",
                    "select ts, sym, sum(px) from c sample by 1h fill(0) order by ts, sym"
            );
        });
    }

    // ==========================================================================================
    // Step 1: LATEST ON ts PARTITION BY <non-dim symbol> (indexed AND non-indexed)
    // ==========================================================================================

    @Test
    public void testLatestOnNonIndexedEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins(false);
            assertSqlCursors(
                    "select * from p latest on ts partition by sym order by sym",
                    "select * from c latest on ts partition by sym order by sym"
            );
        });
    }

    /**
     * sym carries a bitmap index here (unlike {@link #testLatestOnNonIndexedEqualsPlainTwin}), which
     * pre-6b would route through generateLatestByTableQuery's indexed backward-scan cursor family --
     * cell-blind, same bug class as the non-indexed family (see the Task 6b guard in
     * SqlCodeGenerator). Task 6b declines that whole method for composite and lets LATEST BY apply
     * generically over the merged getCursor() instead, so this must equal the plain twin exactly like
     * the non-indexed case.
     */
    @Test
    public void testLatestOnIndexedEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins(true);
            assertSqlCursors(
                    "select * from p latest on ts partition by sym order by sym",
                    "select * from c latest on ts partition by sym order by sym"
            );
        });
    }

    // ==========================================================================================
    // Step 1: time-series joins, composite on MASTER, SLAVE, and BOTH sides
    // ==========================================================================================

    @Test
    public void testAsofJoinEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createJoinTwins();
            final String q = "select m.ts, m.sym, m.qty, s.price from %s m asof join %s s on (m.sym = s.sym)";
            final String oracle = String.format(q, "pm", "ps");
            assertSqlCursors(oracle, String.format(q, "cm", "ps")); // master composite
            assertSqlCursors(oracle, String.format(q, "pm", "cs")); // slave composite
            assertSqlCursors(oracle, String.format(q, "cm", "cs")); // both composite

            // EXPLAIN confirmation: a composite slave must fall back to the light join (the fast
            // TimeFrameCursor-based factory is unavailable -- CompositePageFrameRecordCursorFactory
            // .supportsTimeFrameCursor() is false), never silently keep the (wrong-for-composite) fast
            // factory.
            assertQuery(String.format(q, "pm", "cs")).noLeakCheck().assertsPlanNotContaining("Fast");
            assertQuery(String.format(q, "cm", "cs")).noLeakCheck().assertsPlanNotContaining("Fast");
        });
    }

    @Test
    public void testLtJoinEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createJoinTwins();
            final String q = "select m.ts, m.sym, m.qty, s.price from %s m lt join %s s on (m.sym = s.sym)";
            final String oracle = String.format(q, "pm", "ps");
            assertSqlCursors(oracle, String.format(q, "cm", "ps"));
            assertSqlCursors(oracle, String.format(q, "pm", "cs"));
            assertSqlCursors(oracle, String.format(q, "cm", "cs"));

            assertQuery(String.format(q, "pm", "cs")).noLeakCheck().assertsPlanNotContaining("Fast");
            assertQuery(String.format(q, "cm", "cs")).noLeakCheck().assertsPlanNotContaining("Fast");
        });
    }

    @Test
    public void testSpliceJoinEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createJoinTwins();
            final String q = "select m.ts mts, m.sym msym, m.qty, s.ts sts, s.sym ssym, s.price " +
                    "from %s m splice join %s s on (m.sym = s.sym)";
            final String oracle = String.format(q, "pm", "ps");
            assertSqlCursors(oracle, String.format(q, "cm", "ps")); // master composite
            assertSqlCursors(oracle, String.format(q, "pm", "cs")); // slave composite
            assertSqlCursors(oracle, String.format(q, "cm", "cs")); // both composite
        });
    }

    /**
     * WINDOW JOIN is a PRE-EXISTING (not composite-related) hard requirement: every one of its three
     * implementations -- async/parallel, fast-sync, and even the "light"-looking sync
     * {@code WindowJoinRecordCursorFactory} (label "Window Join") -- is gated on
     * {@code slave.supportsTimeFrameCursor()} (SqlCodeGenerator's window-join generator, the
     * {@code parallelWindowJoinEnabled && ... && slave.supportsTimeFrameCursor()} / {@code else if
     * (slave.supportsTimeFrameCursor())} / final {@code else} three-way split). There is no
     * getCursor()-only fallback at all, so a composite slave (whose
     * {@code supportsTimeFrameCursor()} is false by 6a design) hits the final {@code else} and throws
     * "right side of window join must be a table, not sub-query" -- a clear, pre-existing,
     * NON-composite-specific SqlException, not a silent-wrong result and not a new gate Task 6b needed
     * to add. A composite MASTER has no such restriction (the join only ever calls
     * {@code master.getCursor()}) and must equal the plain twin.
     */
    @Test
    public void testWindowJoinEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createJoinTwins();
            final String q = "select m.ts, m.sym, m.qty, sum(s.price) as window_price from %s m " +
                    "window join %s s on (m.sym = s.sym) " +
                    "range between 20 minutes preceding and 20 minutes following " +
                    "order by m.ts, m.sym";
            final String oracle = String.format(q, "pm", "ps");
            assertSqlCursors(oracle, String.format(q, "cm", "ps")); // master composite: correct

            assertQuery(String.format(q, "pm", "cs")).noLeakCheck()
                    .failsWith("right side of window join must be a table, not sub-query");
            assertQuery(String.format(q, "cm", "cs")).noLeakCheck()
                    .failsWith("right side of window join must be a table, not sub-query");
        });
    }

    /**
     * HORIZON JOIN has the identical PRE-EXISTING hard requirement: {@code generateHorizonJoinFactory}
     * throws "right-hand side of HORIZON JOIN can only be a table with an optional filter" whenever
     * {@code !slaveFactory.supportsTimeFrameCursor()}, unconditionally (not just on the parallel path).
     * Same conclusion as WINDOW JOIN above: composite-as-slave is already loud, not a Task 6b gap.
     */
    @Test
    public void testHorizonJoinEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createJoinTwins();
            // Aggregate away sym (HORIZON JOIN's "keyed" refers to the ON-clause match, not a GROUP BY
            // dimension in the outer select) so each h.offset bucket is a single, order-stable row.
            final String q = "select h.offset, avg(s.price), sum(m.qty), count() from %s m " +
                    "horizon join %s s on (m.sym = s.sym) " +
                    "range from -10m to 10m step 5m as h " +
                    "order by h.offset";
            final String oracle = String.format(q, "pm", "ps");
            assertSqlCursors(oracle, String.format(q, "cm", "ps")); // master composite: correct

            assertQuery(String.format(q, "pm", "cs")).noLeakCheck()
                    .failsWith("right-hand side of HORIZON JOIN can only be a table with an optional filter");
            assertQuery(String.format(q, "cm", "cs")).noLeakCheck()
                    .failsWith("right-hand side of HORIZON JOIN can only be a table with an optional filter");
        });
    }

    // ==========================================================================================
    // Step 1: GROUP BY (order-independent sanity) and tail LIMIT -N
    // ==========================================================================================

    @Test
    public void testGroupByEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins(false);
            assertSqlCursors(
                    "select sym, exch, count(), sum(px), min(px), max(px) from p group by sym, exch order by sym, exch",
                    "select sym, exch, count(), sum(px), min(px), max(px) from c group by sym, exch order by sym, exch"
            );
        });
    }

    /**
     * {@code LIMIT -N} (tail limit) is the async order-sensitive consumer flagged in plan56-research
     * (Q3, {@code AsyncFilteredRecordCursorFactory}/{@code ...NegativeLimit}): it trusts the base
     * scan's advertised order to take the LAST N rows. A composite table must still equal the plain
     * twin's tail.
     */
    @Test
    public void testTailLimitEqualsPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins(false);
            assertSqlCursors(
                    "select * from p limit -5",
                    "select * from c limit -5"
            );
            // combined with a residual filter, still async-order-sensitive
            assertSqlCursors(
                    "select * from p where px > 10 limit -5",
                    "select * from c where px > 10 limit -5"
            );
        });
    }

    // ==========================================================================================
    // Step 3-4: the two index-based scan sites -- LOUD-GATED for composite (see SqlCodeGenerator's
    // Task 6b comments at the intrinsicModel.keyColumn != null guard and the sorted-symbol-index
    // guard for why a predicate-preserving fall-through was not implemented).
    // ==========================================================================================

    @Test
    public void testWhereIndexedSymInListCompositeIsLoudGated() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins(true);
            // sanity: the plain twin (same shape query) must NOT be affected by the composite gate.
            assertSqlCursors(
                    "select * from p where sym in ('A','B') order by ts",
                    "select * from p where sym in ('A','B') order by ts"
            );
            assertQuery("select * from c where sym in ('A','B') order by ts")
                    .noLeakCheck()
                    .failsWith("composite partitioning does not yet support an indexed WHERE predicate");
            // single-value equality takes the same guarded branch
            assertQuery("select * from c where sym = 'A' order by ts")
                    .noLeakCheck()
                    .failsWith("composite partitioning does not yet support an indexed WHERE predicate");
        });
    }

    /**
     * The NO_INDEX hint stops WhereClauseParser from ever setting intrinsicModel.keyColumn for this
     * query, so the predicate never enters the gated branch at all -- it stays a normal residual
     * filter over the already-correct (6a) merged scan. This is the workaround documented in the
     * gate's exception text and in task-6b-report.md; it must produce the SAME rows as the plain twin.
     */
    @Test
    public void testWhereIndexedSymInListNoIndexHintFallsThroughCorrectly() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins(true);
            assertSqlCursors(
                    "select * from p where sym in ('A','B') order by ts",
                    "select /*+ NO_INDEX(sym) */ * from c where sym in ('A','B') order by ts"
            );
        });
    }

    @Test
    public void testOrderByIndexedSymColumnCompositeIsLoudGated() throws Exception {
        assertMemoryLeak(() -> {
            createSingleTableTwins(true);
            // preconditions for the sorted-symbol-index optimisation: interval hits exactly one
            // partition (a single day) and no other filter. `order by sym, ts` (not `order by sym`
            // alone) so the comparison is unambiguous: many rows share one sym value, and "order by
            // sym" alone leaves their relative order unspecified -- a real (harmless) source of
            // divergence unrelated to any bug, since SortedSymbolIndexRecordCursorFactory's bitmap-index
            // walk order for same-key rows need not match a from-scratch scan's order.
            final String predicate = " where ts >= '2020-02-01' and ts < '2020-02-02'";
            // sanity: the plain twin still takes the SortedSymbolIndex optimisation this gate declines
            // for composite -- confirms the gate is scoped to composite only.
            assertQuery("select * from p" + predicate + " order by sym")
                    .noLeakCheck()
                    .assertsPlanContaining("SortedSymbolIndex");
            assertQuery("select * from c" + predicate + " order by sym")
                    .noLeakCheck()
                    .failsWith("composite partitioning does not yet support ORDER BY on an indexed symbol column");
            // the NO_INDEX hint falls back to a plain sorted scan over the merged getCursor() instead.
            assertSqlCursors(
                    "select * from p" + predicate + " order by sym, ts",
                    "select /*+ NO_INDEX(sym) */ * from c" + predicate + " order by sym, ts"
            );
        });
    }

    /**
     * Hard constraint: a PLAIN table (dimCount == 0) must be byte-identical to before this task --
     * none of the three new composite guards may ever fire for it, indexed column or not.
     */
    @Test
    public void testPlainTableIndexedShapesUnaffectedByGates() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, exch symbol, sym symbol index, px double) timestamp(ts) partition by day wal");
            execute("insert into p values " +
                    "('2020-02-01T00:00:00.000000Z','X','A',1), " +
                    "('2020-02-01T01:00:00.000000Z','Y','B',2), " +
                    "('2020-02-01T02:00:00.000000Z','X','A',3), " +
                    "('2020-02-02T00:00:00.000000Z','Y','C',4)");
            drainWalQueue();

            assertQuery("select * from p where sym in ('A','B') order by ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\texch\tsym\tpx\n" +
                            "2020-02-01T00:00:00.000000Z\tX\tA\t1.0\n" +
                            "2020-02-01T01:00:00.000000Z\tY\tB\t2.0\n" +
                            "2020-02-01T02:00:00.000000Z\tX\tA\t3.0\n");
            assertQuery("select * from p where sym = 'A' order by ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\texch\tsym\tpx\n" +
                            "2020-02-01T00:00:00.000000Z\tX\tA\t1.0\n" +
                            "2020-02-01T02:00:00.000000Z\tX\tA\t3.0\n");
            // SortedSymbolIndexRecordCursorFactory deliberately erases the timestamp index
            // (queryMeta.setTimestampIndex(-1)): the result is symbol-ordered, not ts-ordered, so no
            // .timestamp() claim here.
            assertQuery("select * from p where ts >= '2020-02-01' and ts < '2020-02-02' order by sym")
                    .noLeakCheck()
                    .returns("ts\texch\tsym\tpx\n" +
                            "2020-02-01T00:00:00.000000Z\tX\tA\t1.0\n" +
                            "2020-02-01T02:00:00.000000Z\tX\tA\t3.0\n" +
                            "2020-02-01T01:00:00.000000Z\tY\tB\t2.0\n");
            // LatestByLightRecordCursorFactory also strips the timestamp index from its own metadata
            // (its rows are in map key-insertion order, not ts order) -- no .timestamp() claim here
            // either, though (unlike the sorted-symbol-index query above) its size IS known upfront.
            assertQuery("select * from p latest on ts partition by sym order by sym")
                    .noLeakCheck()
                    .expectSize()
                    .returns("ts\texch\tsym\tpx\n" +
                            "2020-02-01T02:00:00.000000Z\tX\tA\t3.0\n" +
                            "2020-02-01T01:00:00.000000Z\tY\tB\t2.0\n" +
                            "2020-02-02T00:00:00.000000Z\tY\tC\t4.0\n");
        });
    }

    /**
     * Builds composite table {@code c} ({@code partition by day, exch}) and its plain twin {@code p}
     * ({@code partition by day}, {@code exch} an ordinary column). Both hold byte-identical rows: 288
     * rows spanning exactly 3 days at a 15-minute cadence (4 rows/hour), {@code exch} alternating by
     * row parity ('X'/'Y') and {@code sym} -- an ORDINARY column, NOT the partitioning dimension --
     * cycling 'A'/'B'/'C' by row index, so consecutive occurrences of one sym value land in
     * alternating exch cells (the shape that exposes a cell-concatenation bug: the row-wise "last
     * seen" for a key is not always in the most-recently-scanned cell). Rows are generated with a
     * monotonic ts and inserted in DESCENDING row order so the WAL write path O3-sorts each cell. All
     * 288 timestamps are globally unique (1-second granularity would not be, so 15-minute spacing is
     * used).
     *
     * @param symIndexed whether {@code sym} carries a bitmap index (exercises the indexed-symbol scan
     *                    sites Task 6b gates/audits; non-indexed exercises the plain residual-filter /
     *                    generic-LATEST-BY paths).
     */
    private void createSingleTableTwins(boolean symIndexed) throws SqlException {
        final String symCol = symIndexed ? "sym symbol index" : "sym symbol";
        execute("create table c (ts timestamp, exch symbol, " + symCol + ", px double) timestamp(ts) partition by day, exch wal");
        execute("create table p (ts timestamp, exch symbol, " + symCol + ", px double) timestamp(ts) partition by day wal");

        final String select =
                "select ('2020-02-01T00:00:00.000000Z'::timestamp + (x - 1) * 900000000L)::timestamp ts, " +
                        "case when x % 2 = 0 then 'X' else 'Y' end exch, " +
                        "case when x % 3 = 0 then 'A' when x % 3 = 1 then 'B' else 'C' end sym, " +
                        "x::double px " +
                        "from long_sequence(288) order by x desc";
        execute("insert into c " + select);
        execute("insert into p " + select);
        drainWalQueue();
    }

    /**
     * Builds four tables for the join differentials: composite master {@code cm} / plain master twin
     * {@code pm} (144 rows, 3 days, 30-minute cadence), and composite slave {@code cs} / plain slave
     * twin {@code ps} (216 rows, 3 days, 20-minute cadence, offset 7 minutes from any master
     * boundary). {@code exch} alternates by row parity in both master and slave (independently); the
     * join/group key {@code sym} (an ORDINARY column, not the partitioning dimension) cycles
     * 'A'/'B'/'C' in both. The master/slave cadences (30min vs 20min, offset 7min) are chosen so
     * {@code gcd(30,20)=10} never divides the 7-minute offset -- master and slave timestamps can never
     * coincide -- and each table's own timestamps are individually strictly increasing, so every
     * timestamp used in these join differentials (across all four tables) is globally unique, per the
     * 6a-review caveat about duplicate-ts tie-breaks being observable to ASOF/LT semantics.
     */
    private void createJoinTwins() throws SqlException {
        execute("create table cm (ts timestamp, exch symbol, sym symbol, qty double) timestamp(ts) partition by day, exch wal");
        execute("create table pm (ts timestamp, exch symbol, sym symbol, qty double) timestamp(ts) partition by day wal");
        execute("create table cs (ts timestamp, exch symbol, sym symbol, price double) timestamp(ts) partition by day, exch wal");
        execute("create table ps (ts timestamp, exch symbol, sym symbol, price double) timestamp(ts) partition by day wal");

        final String masterSelect =
                "select ('2020-03-01T00:00:00.000000Z'::timestamp + (x - 1) * 1800000000L)::timestamp ts, " +
                        "case when x % 2 = 0 then 'X' else 'Y' end exch, " +
                        "case when x % 3 = 0 then 'A' when x % 3 = 1 then 'B' else 'C' end sym, " +
                        "x::double qty " +
                        "from long_sequence(144) order by x desc";
        final String slaveSelect =
                "select ('2020-03-01T00:07:00.000000Z'::timestamp + (x - 1) * 1200000000L)::timestamp ts, " +
                        "case when x % 2 = 0 then 'X' else 'Y' end exch, " +
                        "case when x % 3 = 0 then 'A' when x % 3 = 1 then 'B' else 'C' end sym, " +
                        "(x * 10)::double price " +
                        "from long_sequence(216) order by x desc";

        execute("insert into cm " + masterSelect);
        execute("insert into pm " + masterSelect);
        execute("insert into cs " + slaveSelect);
        execute("insert into ps " + slaveSelect);
        drainWalQueue();
    }
}
