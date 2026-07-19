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

import io.questdb.PropertyKey;
import io.questdb.cairo.CompositeDimensionTransform;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Task 6c of composite partitioning (the READ-SIDE DIFFERENTIAL CAPSTONE): an end-to-end proof that a
 * composite table behaves IDENTICALLY to an equivalent plain twin across the full read surface AND the
 * full write lifecycle -- multiple commits, an out-of-order backfill that EXTENDS an already-populated
 * cell (Plan 4b), and a checkpoint/snapshot restore round-trip (Plan 4d) -- for at least an IDENTITY
 * dimension ({@code partition by day, exch}) and an EXPRESSION dimension ({@code partition by day,
 * (upper(region)) AS r}, Plan 4e), plus a bonus HASH+TRUNCATE multi-dimension combination.
 * <p>
 * <b>The headline confirmation</b> ({@link #testOrderByTsAscAndDescMatchesPlainTwin} and {@link
 * #testExpressionOrderByTsAscAndDescMatchesPlainTwin}): a BARE {@code order by ts} (no secondary sort
 * key) over a composite table with genuinely interleaved same-day cells was, before Task 6a's per-day
 * k-way cross-cell merge cursor ({@code CompositeMergePartitionRecordCursor} /
 * {@code CompositePageFrameRecordCursorFactory}), SILENTLY WRONG -- no error, no suspended table, just a
 * scan order indistinguishable from the raw per-cell-concatenated storage order. This was found,
 * undocumented until then, during Plan 4e Task 4 ({@code CompositeExpressionEndToEndTest}'s class
 * javadoc, "Cross-cell ORDER BY" note) and confirmed there to be kind-agnostic (reproduces identically
 * for a plain IDENTITY dimension), not EXPRESSION-specific. Both this class's IDENTITY and EXPRESSION
 * datasets deliberately build the exact shape that exposed it (an out-of-order-extend commit that makes
 * two sibling cells' timestamp ranges genuinely interleave within one day), and both prove a bare
 * {@code order by ts} now equals the plain twin exactly, ASC and DESC.
 * <p>
 * <b>Task 6a's equal-designated-ts tie-break caveat</b>: every dataset in this class uses globally
 * UNIQUE timestamps throughout (including across both sides of the ASOF self-join), so the
 * heap/cellKey-order tie-break among equal-ts rows from different cells (SQL-legal for {@code ORDER BY
 * ts}, since ts is not a total order, but OBSERVABLE to ASOF/LT join semantics per the 6a review) never
 * arises here -- every comparison against the plain twin is unambiguous.
 * <p>
 * <b>Task 6b's loud gates</b>: an indexed WHERE predicate against a composite table (on the DIMENSION
 * column itself here, not just an ordinary indexed symbol column as 6b's own tests used -- see {@link
 * #testIndexedDimensionWhereIsLoudGatedThenNoIndexFallsThrough}) and WINDOW/HORIZON JOIN with a
 * composite table on the SLAVE side (a pre-existing, non-composite-specific hard requirement -- see
 * {@link #testWindowJoinCompositeSlaveThrowsClearError}/{@link #testHorizonJoinCompositeSlaveThrowsClearError})
 * both still throw a CLEAR, documented exception -- never silently wrong, never silently dropped -- so
 * this capstone documents the current boundary rather than papering over it. The dimension-equality
 * filter used throughout the rest of this class ({@code where exch = 'X'} / {@code where upper(region) =
 * 'US'}) deliberately does NOT hit this gate (the dimension source column is a plain, non-indexed
 * {@code symbol}), matching the brief's own note that Plan 5b will eventually lift the gate.
 * <p>
 * Every table pair is built via the PRE-EXISTING write-side capabilities only (multi-commit WAL inserts,
 * generated/scrambled SELECTs forcing O3 sort, explicit VALUES lists for out-of-order backfills).
 * <p>
 * <b>One real, new write-path gap WAS found and fixed (loud-gated) while developing this suite</b> --
 * see {@link io.questdb.test.cairo.CompositeMultiCellMergeGateTest}: a single commit whose out-of-order
 * rows genuinely interleave across 2+ already-populated cells, where 2+ of those new rows land in the
 * SAME cell, used to silently corrupt that cell's non-timestamp column data (root cause in
 * {@code TableWriter#processO3BlockComposite}'s multi-cellKey regrouping path, not this task's read-side
 * subject matter, and not safely fixable within this task's scope) -- now a clear, diagnosable
 * {@code CairoException} (table suspended), never silent. Every lifecycle builder in this class avoids
 * that exact shape (splitting a would-be combined multi-cell extend into one single-cell commit per
 * already-populated cell, itself proven safe and exercised throughout this class); the gate itself is
 * proven by {@code CompositeMultiCellMergeGateTest}.
 */
public class CompositeReadEndToEndTest extends AbstractCairoTest {

    // ==========================================================================================
    // IDENTITY dimension (partition by day, exch): full lifecycle + full read battery
    // ==========================================================================================

    /**
     * THE HEADLINE CONFIRMATION for this capstone (see class javadoc). Commit 2 (built by {@link
     * #createIdentityLifecycleTwins}) is deliberately the shape that exposed the historical bug: it
     * extends day1's X and Y cells with half-hour timestamps genuinely interleaved between the two
     * cells' existing hourly grid, so day1/X now spans 00:00..21:30 and day1/Y spans 00:00..21:30 too,
     * interleaved. A bare {@code order by ts} must equal the plain twin's order exactly, ASC and DESC.
     */
    @Test
    public void testOrderByTsAscAndDescMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createIdentityLifecycleTwins();
            assertSqlCursors("select * from p order by ts", "select * from c order by ts");
            assertSqlCursors("select * from p order by ts desc", "select * from c order by ts desc");
        });
    }

    /**
     * A ts-range filter spanning day1 (from 06:00, i.e. mid-cell), the whole of day2, and day3 (up to
     * 06:00) -- exercises the interval-scan machinery (fixed pre-6a by {@code 233532984f}/{@code
     * 070581bef6}, "interval scan includes all sibling cells of the high day") together with 6a's own
     * cross-cell merge, not just the merge in isolation.
     */
    @Test
    public void testTsRangeFilterWithOrderByMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createIdentityLifecycleTwins();
            final String range = " where ts between '2020-04-01T06:00:00.000000Z' and '2020-04-03T06:00:00.000000Z'";
            assertSqlCursors(
                    "select * from p" + range + " order by ts",
                    "select * from c" + range + " order by ts");
            assertSqlCursors(
                    "select * from p" + range + " order by ts desc",
                    "select * from c" + range + " order by ts desc");
        });
    }

    @Test
    public void testSampleByWithFillMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createIdentityLifecycleTwins();
            assertSqlCursors(
                    "select ts, sym, sum(px) from p sample by 1h fill(linear) order by ts, sym",
                    "select ts, sym, sum(px) from c sample by 1h fill(linear) order by ts, sym");
            assertSqlCursors(
                    "select ts, sum(px), count() from p sample by 1d",
                    "select ts, sum(px), count() from c sample by 1d");
        });
    }

    @Test
    public void testLatestOnMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createIdentityLifecycleTwins();
            assertSqlCursors(
                    "select * from p latest on ts partition by sym order by sym",
                    "select * from c latest on ts partition by sym order by sym");
        });
    }

    /**
     * Reprises Task 6b's Critical fix ({@code latestBy(filter(scan))}, not {@code
     * filter(latestBy(scan))}) against this capstone's fuller, lifecycle-built dataset rather than 6b's
     * own minimal 4-row repro. {@code px < 1011} excludes sym A's and sym C's true table-wide latest
     * rows (day3's last two rows, px 1012 and 1011) while sym B's true latest (px 1010) survives
     * unfiltered -- a genuine, per-key-divergent fallback scenario, not a vacuous pass-through.
     */
    @Test
    public void testLatestOnWithResidualFilterMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createIdentityLifecycleTwins();
            assertSqlCursors(
                    "select * from p where px < 1011 latest on ts partition by sym order by sym",
                    "select * from c where px < 1011 latest on ts partition by sym order by sym");
        });
    }

    /**
     * ASOF self-join: the SAME composite table serves as BOTH master and slave in one query -- a shape
     * Task 6b's own join differentials (separate cm/cs master/slave tables) never exercised. The join
     * key ({@code a.peerSym = b.sym}) is deliberately NOT the row's own sym value (peerSym is the "next"
     * symbol in an A-&gt;B-&gt;C-&gt;A cycle, always different from sym) so a row never trivially
     * self-matches its own record: an ASOF join's matched slave row for master row M is always M itself
     * when master and slave are the identical unfiltered relation and the join key is reflexive (M's own
     * key trivially equals M's own key), which would make the differential degenerate. All timestamps in
     * this dataset are globally unique (6a review caveat), so the ASOF tie-break is never ambiguous.
     */
    @Test
    public void testAsofSelfJoinMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createIdentityLifecycleTwins();
            final String q = "select a.ts, a.exch, a.sym, a.peerSym, a.px, b.ts bts, b.sym bsym, b.px bpx " +
                    "from %s a asof join %s b on (a.peerSym = b.sym)";
            assertSqlCursors(String.format(q, "p", "p"), String.format(q, "c", "c"));
        });
    }

    @Test
    public void testDimensionEqualityFilterMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createIdentityLifecycleTwins();
            // exch is a plain (non-indexed) symbol column here, so this does NOT hit the Task 6b
            // indexed-WHERE gate at all -- see testIndexedDimensionWhereIsLoudGatedThenNoIndexFallsThrough
            // below for the indexed variant of this same shape.
            assertSqlCursors(
                    "select * from p where exch = 'X' order by ts",
                    "select * from c where exch = 'X' order by ts");
        });
    }

    @Test
    public void testTablePartitionsCellNamesAndAggregatesMatchPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createIdentityLifecycleTwins();

            // table_partitions() cell names: Commit 2 EXTENDED day1's existing X/Y cells rather than
            // creating new ones, so the cell count stays exactly 2/day x 3 days = 6, not more.
            assertQuery("select count() from table_partitions('c')")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n6\n");
            assertQuery("select name from table_partitions('c') order by name")
                    .noLeakCheck().expectSize().returns(
                            "name\n" +
                                    "2020-04-01/exch=X\n" +
                                    "2020-04-01/exch=Y\n" +
                                    "2020-04-02/exch=X\n" +
                                    "2020-04-02/exch=Y\n" +
                                    "2020-04-03/exch=X\n" +
                                    "2020-04-03/exch=Y\n");

            assertSqlCursors(
                    "select exch, sym, count(), sum(px), min(px), max(px) from p group by exch, sym order by exch, sym",
                    "select exch, sym, count(), sum(px), min(px), max(px) from c group by exch, sym order by exch, sym");
            assertSqlCursors(
                    "select count(), sum(px), avg(px), min(px), max(px) from p",
                    "select count(), sum(px), avg(px), min(px), max(px) from c");
        });
    }

    /**
     * Write lifecycle + checkpoint/restore (Plan 4d): rebuilds the SAME 3-commit lifecycle, checkpoints,
     * inserts more data into {@code c} ONLY (must not survive restore -- {@code p} never receives this
     * insert, so it stays the correct live oracle throughout), restores onto a "different install" (the
     * swapped snapshot instance id, mirroring {@code CompositeEndToEndTest}'s own established idiom),
     * then re-runs a representative subset of the read battery -- LIVE against {@code p}, not a
     * before/after string capture -- proving the composite==plain-twin property survives a real restore,
     * not just the interner/column-version rebuild mechanics {@code CompositeEndToEndTest}/{@code
     * CompositeExpressionEndToEndTest} already white-box-verified for Plan 4d.
     */
    @Test
    public void testCheckpointRestoreReadBatteryMatchesPlainTwin() throws Exception {
        final String snapshotId = "00000000-0000-0000-0000-000000000000";
        final String restartedId = "123e4567-e89b-12d3-a456-426614174000";

        engine.clear();
        setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

        createIdentityLifecycleTwins();

        execute("checkpoint create");

        // Post-checkpoint data into c ONLY -- must NOT survive restore; p stays untouched throughout, so
        // it remains a valid live oracle for the post-restore assertions below.
        execute("insert into c values ('2020-04-04T00:00:00.000000Z','X','A','B',9999.0)");
        drainWalQueue();

        engine.clear();
        setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, restartedId);
        try {
            engine.checkpointRecover();

            assertSqlCursors("select * from p order by ts", "select * from c order by ts");
            assertSqlCursors("select * from p order by ts desc", "select * from c order by ts desc");
            assertSqlCursors(
                    "select * from p where px < 1011 latest on ts partition by sym order by sym",
                    "select * from c where px < 1011 latest on ts partition by sym order by sym");
            final String selfJoinQ = "select a.ts, a.sym, a.px, b.sym bsym, b.px bpx " +
                    "from %s a asof join %s b on (a.peerSym = b.sym)";
            assertSqlCursors(String.format(selfJoinQ, "p", "p"), String.format(selfJoinQ, "c", "c"));
            assertQuery("select count() from table_partitions('c')")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n6\n");
            assertSqlCursors("select count() from p", "select count() from c");

            // The post-checkpoint row must NOT have survived restore.
            assertQuery("select count() from c where px = 9999.0")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");

            // c must still be fully writable/queryable post-restore.
            execute("insert into c values ('2020-04-05T00:00:00.000000Z','Y','B','C',1.0)");
            drainWalQueue();
            Assert.assertFalse("c must not be suspended after the post-restore insert",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));
        } finally {
            // checkpointRecover() does NOT itself clear DatabaseCheckpointAgent's in-progress flag --
            // even when it throws -- only checkpointRelease() does (mirrors CheckpointTest's own
            // class-wide @After net and CompositeEndToEndTest's identical checkpoint tests).
            engine.checkpointRelease();
            engine.releaseInactive();
            engine.clear();
        }
    }

    // ------------------------------------------------------------------------------------------
    // Loud-gate confirmation (documents the current boundary; see class javadoc)
    // ------------------------------------------------------------------------------------------

    /**
     * Task 6b's indexed-WHERE gate, confirmed here for the DIMENSION column itself ({@code exch}), not
     * just an ordinary indexed symbol column (which is what 6b's own {@code CompositeReadShapesTest}
     * tests used). The gate condition ({@code reader.getMetadata().getPartitionSpec().isComposite()}) is
     * table-level, not column-specific, so it must fire here too -- confirmed empirically, not assumed
     * from reading the guard.
     */
    @Test
    public void testIndexedDimensionWhereIsLoudGatedThenNoIndexFallsThrough() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ci (ts timestamp, exch symbol index, sym symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table pi (ts timestamp, exch symbol index, sym symbol, px double) timestamp(ts) partition by day wal");
            final String rows = " values " +
                    "('2020-04-01T00:00:00.000000Z','X','A',1.0), ('2020-04-01T01:00:00.000000Z','Y','B',2.0), " +
                    "('2020-04-01T02:00:00.000000Z','X','A',3.0), ('2020-04-02T00:00:00.000000Z','Y','C',4.0)";
            execute("insert into ci" + rows);
            execute("insert into pi" + rows);
            drainWalQueue();

            // Sanity: the plain twin is unaffected by the composite gate.
            assertSqlCursors(
                    "select * from pi where exch = 'X' order by ts",
                    "select * from pi where exch = 'X' order by ts");

            assertQuery("select * from ci where exch = 'X' order by ts")
                    .noLeakCheck()
                    .failsWith("composite partitioning does not yet support an indexed WHERE predicate");

            // NO_INDEX escape hatch falls through correctly, matching the plain twin.
            assertSqlCursors(
                    "select * from pi where exch = 'X' order by ts",
                    "select /*+ NO_INDEX(exch) */ * from ci where exch = 'X' order by ts");
        });
    }

    /**
     * WINDOW JOIN with a composite table on the SLAVE side is a PRE-EXISTING, non-composite-specific
     * hard requirement (every implementation is gated on {@code slave.supportsTimeFrameCursor()}, which
     * a composite factory's {@code CompositePageFrameRecordCursorFactory} always answers false by 6a
     * design) -- not a new Task 6c gate, but re-confirmed here as this capstone's own self-contained
     * proof of the boundary. A composite MASTER has no such restriction and must equal the plain twin.
     */
    @Test
    public void testWindowJoinCompositeSlaveThrowsClearError() throws Exception {
        assertMemoryLeak(() -> {
            createIdentityLifecycleTwins();
            createSlaveTwinsForJoinGates();
            final String q = "select m.ts, m.sym, sum(s.price) wp from %s m window join %s s on (m.sym = s.sym) " +
                    "range between 1 hour preceding and 1 hour following order by m.ts, m.sym";
            assertSqlCursors(String.format(q, "p", "ps2"), String.format(q, "c", "ps2")); // composite master: correct

            assertQuery(String.format(q, "p", "cs2")).noLeakCheck()
                    .failsWith("right side of window join must be a table, not sub-query");
            assertQuery(String.format(q, "c", "cs2")).noLeakCheck()
                    .failsWith("right side of window join must be a table, not sub-query");
        });
    }

    /**
     * HORIZON JOIN has the identical pre-existing hard requirement as WINDOW JOIN above -- re-confirmed
     * here for this capstone's own self-contained proof of the boundary.
     */
    @Test
    public void testHorizonJoinCompositeSlaveThrowsClearError() throws Exception {
        assertMemoryLeak(() -> {
            createIdentityLifecycleTwins();
            createSlaveTwinsForJoinGates();
            final String q = "select h.offset, avg(s.price), sum(m.px), count() from %s m " +
                    "horizon join %s s on (m.sym = s.sym) " +
                    "range from -10m to 10m step 5m as h " +
                    "order by h.offset";
            assertSqlCursors(String.format(q, "p", "ps2"), String.format(q, "c", "ps2")); // composite master: correct

            assertQuery(String.format(q, "p", "cs2")).noLeakCheck()
                    .failsWith("right-hand side of HORIZON JOIN can only be a table with an optional filter");
            assertQuery(String.format(q, "c", "cs2")).noLeakCheck()
                    .failsWith("right-hand side of HORIZON JOIN can only be a table with an optional filter");
        });
    }

    /**
     * Builds composite {@code c} ({@code partition by day, exch}) and plain twin {@code p} ({@code
     * partition by day}), columns {@code (ts, exch symbol, sym symbol, peerSym symbol, px double)} --
     * {@code sym} cycles A/B/C and is the LATEST BY / self-join key (an ORDINARY column, not the
     * partitioning dimension); {@code peerSym} is the "next" symbol in the A-&gt;B-&gt;C-&gt;A cycle,
     * used only by the ASOF self-join (see {@link #testAsofSelfJoinMatchesPlainTwin}). FOUR separate
     * commits, each {@code drainWalQueue()}'d independently:
     * <ol>
     *   <li>Bulk: two brand-new days (2020-04-01, 2020-04-02), hourly cadence, {@code exch} alternating
     *       X/Y by row parity (2 interleaved cells/day), inserted SCRAMBLED ({@code order by x desc}) so
     *       the WAL write path O3-sorts each cell.</li>
     *   <li>OUT-OF-ORDER backfill EXTENDING the already-populated day1/X cell (Plan 4b): three half-hour-
     *       mark rows interleaved within day1's existing hourly grid -- a real O3 merge into an EXISTING
     *       partition, not a tail append.</li>
     *   <li>A SEPARATE out-of-order backfill EXTENDING the already-populated day1/Y cell, same shape --
     *       kept as its OWN commit rather than combined with the X extend above (see
     *       {@link io.questdb.test.cairo.CompositeMultiCellMergeGateTest}: a single commit genuinely
     *       interleaving 2+ new rows across 2+ already-populated cells hits a real, newly-discovered
     *       write-path gap this capstone found and loud-gated). Together, commits 2+3 are exactly the
     *       shape that exposes the historical bare-{@code ORDER BY ts} bug (see class javadoc): day1/X
     *       and day1/Y end up with genuinely interleaved timestamp ranges either way.</li>
     *   <li>A brand-new day (2020-04-03), in order, same 2-cell shape, smaller.</li>
     * </ol>
     * All 66 rows carry globally UNIQUE timestamps (1-second granularity would not be; hourly/half-hourly
     * spacing is used throughout, across all four commits, with no overlap).
     */
    private void createIdentityLifecycleTwins() throws SqlException {
        execute("create table c (ts timestamp, exch symbol, sym symbol, peerSym symbol, px double) timestamp(ts) partition by day, exch wal");
        execute("create table p (ts timestamp, exch symbol, sym symbol, peerSym symbol, px double) timestamp(ts) partition by day wal");

        final String bulk =
                "select ('2020-04-01T00:00:00.000000Z'::timestamp + (x - 1) * 3600000000L)::timestamp ts, " +
                        "case when x % 2 = 0 then 'X' else 'Y' end exch, " +
                        "case when x % 3 = 0 then 'A' when x % 3 = 1 then 'B' else 'C' end sym, " +
                        "case when x % 3 = 0 then 'B' when x % 3 = 1 then 'C' else 'A' end peerSym, " +
                        "x::double px " +
                        "from long_sequence(48) order by x desc";
        execute("insert into c " + bulk);
        execute("insert into p " + bulk);
        drainWalQueue();

        // Two SEPARATE commits, one per cell (not one combined commit touching both X and Y) -- see
        // CompositeMultiCellMergeGateTest for why: a single commit whose out-of-order rows are genuinely
        // interleaved across 2+ ALREADY-populated cells, with 2+ new rows landing in at least one of
        // them, hits a real, newly-discovered write-path gap this capstone found and loud-gated (Task 6c
        // finding, TableWriter#processO3BlockComposite). Splitting into one single-cell commit per cell
        // is the documented, proven-safe workaround and is exactly as out-of-order/interleaved-within-
        // its-own-cell as the combined form would have been.
        final String extendX = " values " +
                "('2020-04-01T02:30:00.000000Z','X','A','B',100.5), " +
                "('2020-04-01T08:30:00.000000Z','X','B','C',101.5), " +
                "('2020-04-01T14:30:00.000000Z','X','C','A',102.5)";
        execute("insert into c" + extendX);
        execute("insert into p" + extendX);
        drainWalQueue();

        final String extendY = " values " +
                "('2020-04-01T03:30:00.000000Z','Y','A','B',103.5), " +
                "('2020-04-01T09:30:00.000000Z','Y','B','C',104.5), " +
                "('2020-04-01T21:30:00.000000Z','Y','C','A',105.5)";
        execute("insert into c" + extendY);
        execute("insert into p" + extendY);
        drainWalQueue();

        final String day3 =
                "select ('2020-04-03T00:00:00.000000Z'::timestamp + (x - 1) * 3600000000L)::timestamp ts, " +
                        "case when x % 2 = 0 then 'X' else 'Y' end exch, " +
                        "case when x % 3 = 0 then 'A' when x % 3 = 1 then 'B' else 'C' end sym, " +
                        "case when x % 3 = 0 then 'B' when x % 3 = 1 then 'C' else 'A' end peerSym, " +
                        "(x + 1000)::double px " +
                        "from long_sequence(12) order by x desc";
        execute("insert into c " + day3);
        execute("insert into p " + day3);
        drainWalQueue();

        Assert.assertFalse("c must not be suspended after the lifecycle commits",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));
        engine.releaseInactive(); // cold reopen -- no pooled reader may mask a fresh self-detect
    }

    /**
     * Small dedicated composite/plain SLAVE twins for the WINDOW/HORIZON gate tests -- named {@code
     * cs2}/{@code ps2} (not {@code cs}/{@code ps}) to avoid any confusion with {@link
     * #createIdentityLifecycleTwins}'s {@code c}/{@code p}, which serve as the MASTER side in those two
     * tests. 20-minute cadence offset 7 minutes from any hour/half-hour mark used by the master dataset,
     * spanning day1 only (deliberately -- these tests are about confirming the GATE fires/doesn't fire,
     * not re-proving WINDOW/HORIZON JOIN's own value correctness, which 6b's {@code
     * CompositeReadShapesTest} already covers with a composite MASTER).
     */
    private void createSlaveTwinsForJoinGates() throws SqlException {
        execute("create table cs2 (ts timestamp, exch symbol, sym symbol, price double) timestamp(ts) partition by day, exch wal");
        execute("create table ps2 (ts timestamp, exch symbol, sym symbol, price double) timestamp(ts) partition by day wal");
        final String rows =
                "select ('2020-04-01T00:07:00.000000Z'::timestamp + (x - 1) * 1200000000L)::timestamp ts, " +
                        "case when x % 2 = 0 then 'X' else 'Y' end exch, " +
                        "case when x % 3 = 0 then 'A' when x % 3 = 1 then 'B' else 'C' end sym, " +
                        "(x * 10)::double price " +
                        "from long_sequence(72) order by x desc";
        execute("insert into cs2 " + rows);
        execute("insert into ps2 " + rows);
        drainWalQueue();
    }

    // ==========================================================================================
    // EXPRESSION dimension (partition by day, (upper(region)) AS r, Plan 4e)
    // ==========================================================================================

    /**
     * See class javadoc's headline confirmation -- this is where the bug was ORIGINALLY found (Plan 4e
     * Task 4), so re-confirming it here specifically (not just for IDENTITY) closes the loop.
     */
    @Test
    public void testExpressionOrderByTsAscAndDescMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createExpressionLifecycleTwins();
            assertSqlCursors("select ts, region, px from p order by ts", "select ts, region, px from c order by ts");
            assertSqlCursors("select ts, region, px from p order by ts desc", "select ts, region, px from c order by ts desc");
        });
    }

    @Test
    public void testExpressionTsRangeAndSampleByMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createExpressionLifecycleTwins();
            final String range = " where ts between '2020-05-01T06:00:00.000000Z' and '2020-05-02T06:00:00.000000Z'";
            assertSqlCursors(
                    "select ts, region, px from p" + range + " order by ts",
                    "select ts, region, px from c" + range + " order by ts");
            assertSqlCursors(
                    "select ts, sum(px), count() from p sample by 1h",
                    "select ts, sum(px), count() from c sample by 1h");
        });
    }

    /**
     * {@code LATEST ON}'s grammar only accepts a literal column reference in {@code PARTITION BY}
     * ({@code SqlParser#parseLatestByNew} calls {@code expectLiteral}), never a bare expression -- so
     * {@code c} cannot write {@code LATEST ON ts PARTITION BY upper(region)} directly. Mirrors {@code
     * CompositeExpressionEndToEndTest}'s own established proxy: wrap {@code c} in a subquery that
     * projects {@code upper(region)} as a real virtual column {@code r} first, then apply {@code LATEST
     * ON ts PARTITION BY r} to that subquery's result.
     */
    @Test
    public void testExpressionLatestOnViaProxyMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createExpressionLifecycleTwins();
            assertSqlCursors(
                    "select ts, region, px from p latest on ts partition by r order by r",
                    "select ts, region, px from (select ts, region, px, upper(region) r from c) latest on ts partition by r order by r");
        });
    }

    @Test
    public void testExpressionDimensionFilterTablePartitionsAndCountMatchPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createExpressionLifecycleTwins();

            assertSqlCursors(
                    "select ts, region, px from p where r = 'US' order by ts",
                    "select ts, region, px from c where upper(region) = 'US' order by ts");
            assertSqlCursors("select count() from p where r = 'US'", "select count() from c where upper(region) = 'US'");
            assertSqlCursors("select count() from p", "select count() from c");
            // upper(region)'s return type (SYMBOL, since region is SYMBOL) differs from p's precomputed
            // VARCHAR "r" column even though the string VALUES are identical -- assertSqlCursors checks
            // column type equality too, so cast explicitly to make this a values-only comparison.
            assertSqlCursors(
                    "select r, count(), sum(px) from p group by r order by r",
                    "select upper(region)::varchar r, count(), sum(px) from c group by upper(region) order by r");

            // table_partitions() cell names: 2 cells (US, EU) x 2 days (Commit 2 extends day1's existing
            // cells, no new cell created).
            assertQuery("select count() from table_partitions('c')")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n4\n");
            assertQuery("select name from table_partitions('c') order by name")
                    .noLeakCheck().expectSize().returns(
                            "name\n" +
                                    "2020-05-01/r=EU\n" +
                                    "2020-05-01/r=US\n" +
                                    "2020-05-02/r=EU\n" +
                                    "2020-05-02/r=US\n");
        });
    }

    /**
     * Write lifecycle + checkpoint/restore (Plan 4d) for EXPRESSION, mirroring {@link
     * #testCheckpointRestoreReadBatteryMatchesPlainTwin}'s IDENTITY version: live differential against
     * {@code p} (untouched post-checkpoint) rather than a before/after string capture.
     */
    @Test
    public void testExpressionCheckpointRestoreReadBatteryMatchesPlainTwin() throws Exception {
        final String snapshotId = "00000000-0000-0000-0000-000000000000";
        final String restartedId = "123e4567-e89b-12d3-a456-426614174000";

        engine.clear();
        setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, snapshotId);

        createExpressionLifecycleTwins();

        execute("checkpoint create");

        execute("insert into c values ('2020-05-04T00:00:00.000000Z','de',9999.0)");
        drainWalQueue();

        engine.clear();
        setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, restartedId);
        try {
            engine.checkpointRecover();

            assertSqlCursors("select ts, region, px from p order by ts", "select ts, region, px from c order by ts");
            assertSqlCursors("select ts, region, px from p order by ts desc", "select ts, region, px from c order by ts desc");
            assertSqlCursors(
                    "select ts, region, px from p where r = 'US' order by ts",
                    "select ts, region, px from c where upper(region) = 'US' order by ts");
            assertSqlCursors(
                    "select ts, region, px from p latest on ts partition by r order by r",
                    "select ts, region, px from (select ts, region, px, upper(region) r from c) latest on ts partition by r order by r");
            assertQuery("select count() from table_partitions('c')")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n4\n");
            assertSqlCursors("select count() from p", "select count() from c");

            assertQuery("select count() from c where px = 9999.0")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");

            execute("insert into c values ('2020-05-05T00:00:00.000000Z','fr',1.0)");
            drainWalQueue();
            Assert.assertFalse("c must not be suspended after the post-restore insert",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));
        } finally {
            engine.checkpointRelease();
            engine.releaseInactive();
            engine.clear();
        }
    }

    /**
     * Builds composite {@code c} ({@code partition by day, (upper(region)) AS r}) and plain twin {@code
     * p} (a real, precomputed {@code r varchar} column populated client-side -- mirrors {@code
     * CompositeExpressionDimTest}/{@code CompositeExpressionEndToEndTest}'s established precomputed-twin
     * idiom). TWO commits (a lighter lifecycle than {@link #createIdentityLifecycleTwins}'s three,
     * deliberately -- see the HASH+TRUNCATE section's own javadoc for the general "why not repeat
     * everything for every dimension kind" reasoning; EXPRESSION's own value-add here is specifically the
     * OUT-OF-ORDER extend, which no prior EXPRESSION test exercised -- {@code
     * CompositeExpressionEndToEndTest}'s own extend test was IN-ORDER only):
     * <ol>
     *   <li>Bulk: two brand-new days (2020-05-01, 2020-05-02), hourly cadence, {@code region} alternating
     *       lowercase {@code us}/{@code eu} by row parity (2 interleaved cells/day), scrambled insertion
     *       order.</li>
     *   <li>OUT-OF-ORDER backfill EXTENDING the already-populated day1/US and day1/EU cells, via a
     *       DIFFERENT raw casing than commit 1 ({@code Us}/{@code US}/{@code Eu}/{@code EU}) at half-hour
     *       marks genuinely interleaved within day1's existing hourly grid -- proves both the O3-merge-
     *       into-an-existing-cell mechanic AND the multi-spelling collapse together, a combination no
     *       prior EXPRESSION test covers.</li>
     * </ol>
     * All timestamps globally unique throughout.
     */
    private void createExpressionLifecycleTwins() throws SqlException {
        execute("create table c (ts timestamp, region symbol, px double) timestamp(ts) partition by day, (upper(region)) AS r wal");
        execute("create table p (ts timestamp, region symbol, px double, r varchar) timestamp(ts) partition by day wal");

        final String bulkC =
                "select ('2020-05-01T00:00:00.000000Z'::timestamp + (x - 1) * 3600000000L)::timestamp ts, " +
                        "case when x % 2 = 0 then 'us' else 'eu' end region, " +
                        "x::double px " +
                        "from long_sequence(48) order by x desc";
        final String bulkP =
                "select ('2020-05-01T00:00:00.000000Z'::timestamp + (x - 1) * 3600000000L)::timestamp ts, " +
                        "case when x % 2 = 0 then 'us' else 'eu' end region, " +
                        "x::double px, " +
                        "case when x % 2 = 0 then 'US' else 'EU' end r " +
                        "from long_sequence(48) order by x desc";
        execute("insert into c " + bulkC);
        execute("insert into p " + bulkP);
        drainWalQueue();

        // Two SEPARATE commits, one per cell -- see this class's IDENTITY-section sibling comment
        // (createIdentityLifecycleTwins) and CompositeMultiCellMergeGateTest: a single commit with 2+
        // out-of-order rows landing in EACH of 2+ already-populated cells hits a real, newly-discovered
        // write-path gap this capstone found and loud-gated.
        execute("insert into c values " +
                "('2020-05-01T02:30:00.000000Z','Us',200.5), ('2020-05-01T08:30:00.000000Z','US',201.5)");
        execute("insert into p values " +
                "('2020-05-01T02:30:00.000000Z','Us',200.5,'US'), ('2020-05-01T08:30:00.000000Z','US',201.5,'US')");
        drainWalQueue();

        execute("insert into c values " +
                "('2020-05-01T03:30:00.000000Z','Eu',202.5), ('2020-05-01T09:30:00.000000Z','EU',203.5)");
        execute("insert into p values " +
                "('2020-05-01T03:30:00.000000Z','Eu',202.5,'EU'), ('2020-05-01T09:30:00.000000Z','EU',203.5,'EU')");
        drainWalQueue();

        Assert.assertFalse("c must not be suspended after the lifecycle commits",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));
        engine.releaseInactive();
    }

    // ==========================================================================================
    // HASH + TRUNCATE dimensions (bonus coverage -- brief: "if easy to add, include them")
    // ==========================================================================================

    /**
     * A THIRD, non-IDENTITY, non-EXPRESSION, multi-dimension composite ({@code hash(exch, 4)} +
     * {@code truncate(sku, 3)}, no IDENTITY dimension at all) -- proves the 6a cross-cell merge and the
     * read battery's core correctness signal (a bare {@code order by ts}, count()/aggregates, and
     * {@code table_partitions()}) is dimension-KIND-agnostic, not just proven for IDENTITY/EXPRESSION.
     * <p>
     * A full lifecycle (checkpoint/restore, the complete read battery -- SAMPLE BY, LATEST ON, ASOF
     * self-join) is deliberately NOT repeated here, scoped down per the brief's own "if easy to add"
     * framing, because: (a) {@code CompositeEndToEndTest#testCheckpointRestoreRoutedCompositeTableRoundTrips}
     * already proves a 2-dimension IDENTITY+TRUNCATE table's checkpoint/restore round-trips correctly,
     * including the dedicated dict's own rebuild; (b) {@code
     * CompositePartitionPathTest#testHashBucketRendersIntegerBothModes} already proves HASH's
     * ordinal/cell-segment rendering mechanics directly; and (c) the cross-cell MERGE cursor itself
     * (this capstone's primary subject) operates purely on {@code cellKey}/frame partitioning -- it has
     * no dependency on how a dimension's ordinal was computed (identity lookup, hash, truncate prefix, or
     * expression eval) -- so the IDENTITY+EXPRESSION sections' exhaustive proof already covers the merge
     * logic's own correctness across two independently-different ordinal-resolution mechanisms; this
     * section's job is narrowly to confirm that claim empirically for a THIRD, not-yet-exercised
     * combination.
     */
    @Test
    public void testHashTruncateOrderByCountAndTablePartitionsMatchPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createHashTruncateLifecycleTwins();

            assertSqlCursors("select * from p order by ts", "select * from c order by ts");
            assertSqlCursors("select * from p order by ts desc", "select * from c order by ts desc");
            assertSqlCursors("select count() from p", "select count() from c");
            assertSqlCursors(
                    "select exch, sku, count(), sum(px) from p group by exch, sku order by exch, sku",
                    "select exch, sku, count(), sum(px) from c group by exch, sku order by exch, sku");

            // Expected table_partitions() count is computed independently via
            // CompositeDimensionTransform.hashBucket, not hardcoded/guessed, since exch's 2 raw values
            // may or may not collide into the same hash bucket -- mirrors CompositePartitionPathTest's
            // own established technique for deriving an expected HASH value independently.
            int bucket0 = CompositeDimensionTransform.hashBucket("EX0", 4);
            int bucket1 = CompositeDimensionTransform.hashBucket("EX1", 4);
            int distinctBuckets = bucket0 == bucket1 ? 1 : 2;
            int expectedCells = distinctBuckets * 2 /* sku truncate(,3) prefixes: BTC, ETH */ * 2 /* days */;
            assertQuery("select count() from table_partitions('c')")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n" + expectedCells + "\n");
        });
    }

    /**
     * Builds composite {@code c} ({@code partition by day, hash(exch, 4), truncate(sku, 3)}) and plain
     * twin {@code p}. {@code exch} cycles 2 raw values ({@code EX0}/{@code EX1}) on a period-2 pattern
     * and {@code sku} cycles 2 raw values ({@code BTCUSDT}/{@code ETHUSDT}, truncating to {@code
     * BTC}/{@code ETH}) on a period-4 pattern -- the two periods are DIFFERENT so all 4 (exch, sku)
     * combinations occur, not just a 1:1 paired subset. Two commits (bulk day1, then a brand-new day2 in
     * order); all timestamps globally unique.
     */
    private void createHashTruncateLifecycleTwins() throws SqlException {
        execute("create table c (ts timestamp, exch symbol, sku symbol, px double) timestamp(ts) " +
                "partition by day, hash(exch, 4), truncate(sku, 3) wal");
        execute("create table p (ts timestamp, exch symbol, sku symbol, px double) timestamp(ts) partition by day wal");

        final String day1 =
                "select ('2020-06-01T00:00:00.000000Z'::timestamp + (x - 1) * 1800000000L)::timestamp ts, " +
                        "case when x % 2 = 0 then 'EX0' else 'EX1' end exch, " +
                        "case when x % 4 < 2 then 'BTCUSDT' else 'ETHUSDT' end sku, " +
                        "x::double px " +
                        "from long_sequence(16) order by x desc";
        execute("insert into c " + day1);
        execute("insert into p " + day1);
        drainWalQueue();

        final String day2 =
                "select ('2020-06-02T00:00:00.000000Z'::timestamp + (x - 1) * 1800000000L)::timestamp ts, " +
                        "case when x % 2 = 0 then 'EX0' else 'EX1' end exch, " +
                        "case when x % 4 < 2 then 'BTCUSDT' else 'ETHUSDT' end sku, " +
                        "(x + 100)::double px " +
                        "from long_sequence(16) order by x desc";
        execute("insert into c " + day2);
        execute("insert into p " + day2);
        drainWalQueue();

        Assert.assertFalse("c must not be suspended after the lifecycle commits",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));
        engine.releaseInactive();
    }
}
