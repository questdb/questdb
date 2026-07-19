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

import io.questdb.cairo.TableReader;
import io.questdb.griffin.SqlException;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Task 5a of composite partitioning (the FIRST read-side correctness task): a ts-range interval scan
 * ({@code WHERE ts BETWEEN/>=/<=/IN ...}) whose HIGH boundary falls INSIDE a day that has more than one
 * cell must include every sibling cell of that day, not just cellKey 0.
 * <p>
 * <b>Root cause:</b> {@code AbstractIntervalPartitionFrameCursor#cullPartitions} computes the exclusive
 * high partition-frame bound as {@code reader.getPartitionIndexByTimestamp(intervalHi) + 1}.
 * {@code getPartitionIndexByTimestamp} is a find-floor search over {@code TableReader.openPartitionInfo}
 * (one slot per (day, cellKey), sorted (ts ASC, cellKey ASC) -- all cells of one day share the SAME
 * timestamp) using {@code BIN_SEARCH_SCAN_UP}, which resolves an exact-match run to its LOWEST index --
 * cellKey 0. {@code + 1} then makes {@code [partitionLo, partitionHi)} include only cellKey 0 of the
 * high day, silently excluding cellKey &gt;= 1.
 * <p>
 * The fix (see {@code TableReader#getPartitionIndexByTimestampScanDown} and
 * {@code AbstractIntervalPartitionFrameCursor#cullPartitions}'s high-boundary line) swaps the high
 * boundary's search direction to {@code BIN_SEARCH_SCAN_DOWN}, which resolves an exact-match run to its
 * HIGHEST index (the day's last cell) instead. For a NOT-FOUND boundary (a timestamp strictly between
 * two days -- e.g. a gap day) both directions provably normalize to the identical index (grounded
 * directly against {@link io.questdb.std.LongList#binarySearchBlock}'s scan-up/scan-down source), and
 * for a PLAIN table every day has exactly one cell, so an exact match is never a multi-entry run either
 * -- both directions again return the identical index. The low boundary is untouched: cellKey 0 is
 * already the lowest index of the low day, so the existing {@code getPartitionIndexByTimestamp} call
 * there already includes that day's every sibling cell.
 * <p>
 * <b>A SEPARATE, pre-existing, out-of-scope bug (found while grounding this task, NOT fixed here):</b>
 * {@code TableReader#getPartitionMaxTimestampFromMetadata} (used by {@code NativeTimestampFinder} to
 * cheaply bound each partition inside {@code IntervalFwdPartitionFrameCursor#next()}/{@code
 * calculateSize()}, independent of the high-boundary fix above) approximates a partition's own max
 * timestamp as {@code (partitionIndex + 1)'s own min timestamp - 1}, i.e. it assumes the NEXT physical
 * partition slot always starts a genuinely LATER day. For a composite table's NON-LAST cell of a
 * multi-cell day, the next slot is that SAME day's next cellKey, sharing the identical timestamp -- so
 * this "approximate max" comes out as {@code (this cell's own day start) - 1}, ONE MICROSECOND BEFORE
 * the cell's own data even begins. Whenever a query's resolved low interval bound lands ON (or after)
 * that cell's own day -- which is normal for almost any {@code ts >=}/{@code IN}/{@code BETWEEN} query
 * touching that day at all -- {@code next()}'s wholly-below-partition pre-check (comparing this wrong
 * value against the interval's low bound) spuriously skips that cell's partition entirely, even though
 * it is genuinely inside {@code [partitionLo, partitionHi)}. Confirmed directly (temporary instrumentation
 * of {@code cullPartitions}/{@code IntervalFwdPartitionFrameCursor#next()}, since reverted) against this
 * exact test's original dataset: e.g. {@code ts IN '<a multi-cell day>'} resolves BOTH cellKey 0 and
 * cellKey 1's own day-start as the query's low bound, so cellKey 0 (the non-last cell) is always
 * spuriously skipped this way -- returning EMPTY for that whole day, not just missing its cellKey >= 1
 * sibling. This independently explains (and predates) the "zero rows" symptom {@code
 * io.questdb.test.cairo.CompositeRoutingTest} and {@code io.questdb.test.cairo.CompositeRoutingEndToEndTest}'s
 * own {@code assertPerDayExchCountsMatch} javadoc already diagnosed and routed around (via {@code
 * to_str(ts, ...)}, which the optimiser cannot fold into a prunable interval at all) rather than fixed.
 * It is NOT introduced or worsened by this task's fix -- it reproduces identically whether the high
 * boundary is fixed or not, in a completely different method ({@code getPartitionMaxTimestampFromMetadata},
 * not {@code getPartitionIndexByTimestamp}/{@code cullPartitions}) -- and is out of this task's assigned
 * scope (`TableReader.java`'s new method + `cullPartitions`'s high-boundary line only). Every query below
 * therefore anchors its LOW bound at or before the sentinel day {@code d0} (2019-12-31, deliberately
 * SINGLE-cell, so it can never trigger this separate bug) rather than directly at a multi-cell day, so
 * this test suite cleanly isolates and proves ONLY the high-boundary fix this task is scoped to. A bare
 * {@code ts IN '<multi-cell day>'} query -- whose low AND high bound are BOTH that same day, so the
 * separate bug is unavoidable regardless of dataset shape -- is deliberately NOT asserted as passing
 * here; see {@link #testRangeHighBoundaryOnMiddleDayIncludesAllCells()}'s javadoc for the safely-anchored
 * range this test suite uses instead to cover the same "whole day, both cells" concern.
 * <p>
 * Two twin tables, byte-for-byte identical rows: {@code c} ({@code partition by day, exch} -- composite,
 * 2 cells/day for d1..d3) and {@code p} ({@code partition by day} -- plain, {@code exch} an ordinary
 * column). 4 day partitions: sentinel {@code d0} (2019-12-31, single cell, 2 rows) + {@code d1, d2, d3}
 * (2020-01-01..03, 2 cells/day, 'A' mornings/'B' afternoons, 2 rows per (day, cell)) -- 14 rows total. A
 * single bulk insert per table (one WAL commit) is already sufficient to reproduce the high-boundary bug,
 * matching {@code CompositeRoutingTest}'s own diagnostic note that this predates, and is unrelated to,
 * multi-commit per-cell routing.
 * <p>
 * <b>UPDATE (Task 5a-2):</b> the separate bug documented above is now FIXED --
 * {@link io.questdb.cairo.TableReader#getPartitionMaxTimestampFromMetadata(int)} advances past sibling
 * cells (same raw timestamp) to the next DISTINCT day before reading its min timestamp for the ceiling,
 * falling back to {@code ceil(own min)} exactly as before when no later distinct day exists (the
 * last-partition-in-table edge, preserved for a composite table's non-last cell too). See
 * {@code testIntervalInMultiCellMiddleDayIncludesAllCells} onward below for direct coverage of the
 * previously-avoided {@code ts IN '<multi-cell day>'} shape (now green), including the table's own last
 * day, plus a direct-method proof ({@code testApproxMaxTimestampAgreesAcrossSiblingCells}) that every
 * sibling cell of the same day now reports an identical approx-max timestamp.
 */
public class CompositeIntervalScanTest extends AbstractCairoTest {

    /**
     * PLAIN-only regression (guards byte-identity): the SAME interval-scan predicate shapes exercised
     * against the composite table below, run ONLY against the plain twin {@code p}, with explicit
     * hardcoded expected counts. A plain table has exactly one cell per day, so this must hold (and must
     * keep holding, unchanged, after the fix) regardless of whether the high-boundary search uses
     * BIN_SEARCH_SCAN_UP or BIN_SEARCH_SCAN_DOWN -- this test is expected to be GREEN both BEFORE and
     * AFTER the fix, not RED-then-GREEN like the composite-table tests below.
     */
    @Test
    public void testPlainTwinIntervalScanUnaffected() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            assertQuery("select count() from p where ts >= '2019-12-31' and ts <= '2020-01-03T12:00:00.000000Z'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n13\n");
            assertQuery("select count() from p where ts >= '2019-12-31' and ts <= '2020-01-02T23:59:59.999999Z'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n10\n");
            assertQuery("select count() from p where ts between '2019-12-31' and '2020-01-03T18:00:00.000000Z'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n14\n");

            // Direct-method byte-identity proof: for a plain reader, getPartitionIndexByTimestamp and
            // getPartitionIndexByTimestampScanDown must return the EXACT SAME index for every one of its
            // partitions' own timestamps -- not merely "the same query result", but the same underlying
            // partition-frame bound the fix computes from.
            try (TableReader r = getReader("p")) {
                int partitionCount = r.getPartitionCount();
                Assert.assertEquals("plain twin must have exactly one partition per day", 4, partitionCount);
                for (int i = 0; i < partitionCount; i++) {
                    long ts = r.getPartitionTimestampByIndex(i);
                    Assert.assertEquals(
                            "plain table partition " + i + " (ts=" + ts + "): scan-up and scan-down must agree",
                            r.getPartitionIndexByTimestamp(ts),
                            r.getPartitionIndexByTimestampScanDown(ts));
                }
            }
        });
    }

    /**
     * High boundary (2020-01-03T12:00, a time strictly inside d3) lands on d3 -- the table's tail/most-
     * recently-appended day -- which has 2 cells (A, B). Low bound is anchored at the sentinel d0
     * (single-cell, immune to the separate bug documented on the class javadoc) rather than directly at
     * d1, so this cleanly isolates the high-boundary fix. Qualifying rows: all of d0 (2) + d1 (4) + d2 (4)
     * + d3's A@00:00, A@06:00, B@12:00 (3; B@18:00 excluded by the upper bound itself, not by the bug) =
     * 13. RED (pre-fix): d3's non-cellKey-0 cell's entire partition-frame is never scanned, so its
     * qualifying row(s) are silently missing regardless of the row-level bound.
     */
    @Test
    public void testRangeHighBoundaryOnTailDayIncludesAllCells() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive(); // cold reopen -- no pooled reader may mask a fresh self-detect

            String predicate = " where ts >= '2019-12-31' and ts <= '2020-01-03T12:00:00.000000Z'";
            assertCompositeMatchesPlain(predicate);
            assertQuery("select count() from c" + predicate)
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n13\n");
        });
    }

    /**
     * High boundary at d2's VERY LAST microsecond (2020-01-02T23:59:59.999999) -- a purely MIDDLE day
     * (earlier d1 and later d3 both exist) -- covers d2's ENTIRE span inclusive, both cells, while d3 is
     * excluded entirely. This is the safely-anchored equivalent of a bare {@code ts IN 'd2'} query (which
     * would assert the exact same "whole day, both cells" property) -- {@code IN} forces its OWN low
     * bound to land exactly ON d2's start, which is unavoidably the separate, out-of-scope bug documented
     * on the class javadoc (d2's low-boundary cellKey would be spuriously skipped regardless of this
     * task's fix); anchoring the low bound at the sentinel d0 instead avoids that bug while still proving
     * the SAME thing this task is scoped to fix: a high boundary landing at/inside a multi-cell day
     * includes ALL its cells, not just cellKey 0. Qualifying rows: d0 (2) + d1 (4) + d2 (4, its whole
     * span) = 10; d3 (0).
     */
    @Test
    public void testRangeHighBoundaryOnMiddleDayIncludesAllCells() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            String predicate = " where ts >= '2019-12-31' and ts <= '2020-01-02T23:59:59.999999Z'";
            assertCompositeMatchesPlain(predicate);
            assertQuery("select count() from c" + predicate)
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n10\n");
        });
    }

    /**
     * {@code ts BETWEEN .. AND ..} (inclusive both ends), high boundary at d3's VERY LAST row
     * (2020-01-03T18:00) -- every row in the dataset qualifies at the row level, so this isolates the
     * PARTITION-frame bug from row-level bounds entirely: even though BOTH of d3's cellKey&gt;=1 rows
     * (12:00 and 18:00) individually satisfy the predicate, the bug drops that whole cell's
     * partition-frame regardless, never even examining those rows. Qualifying rows: all 14. RED
     * (pre-fix): loses that whole cell's d3 rows (2 of the 14).
     */
    @Test
    public void testBetweenHighBoundaryCoversFullMultiCellDay() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            String predicate = " where ts between '2019-12-31' and '2020-01-03T18:00:00.000000Z'";
            assertCompositeMatchesPlain(predicate);
            assertQuery("select count() from c" + predicate)
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n14\n");
        });
    }

    /**
     * Guards that the predicate shape above genuinely triggers the interval partition-frame cursor
     * (rather than, say, falling back to a plain filtered full scan, which would pass the row-count
     * assertions above for the wrong reason and never touch the buggy code path at all). d0 (2 rows) +
     * d1 (4 rows, 2 cells), high boundary at d1's own end -- already in ts order via a forward interval
     * scan.
     */
    @Test
    public void testIntervalScanPlanGuardUsesIntervalCursor() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            assertQuery("select ts, exch, px from c where ts >= '2019-12-31' and ts <= '2020-01-01T23:59:59.999999Z' order by ts, exch")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("Interval forward scan")
                    .returns("""
                            ts\texch\tpx
                            2019-12-31T00:00:00.000000Z\tA\t0.1
                            2019-12-31T12:00:00.000000Z\tA\t0.2
                            2020-01-01T00:00:00.000000Z\tA\t1.0
                            2020-01-01T06:00:00.000000Z\tA\t1.1
                            2020-01-01T12:00:00.000000Z\tB\t1.2
                            2020-01-01T18:00:00.000000Z\tB\t1.3
                            """);
        });
    }

    /**
     * Direct-method proof on the composite reader, independent of SQL: for a genuine 2-cell day,
     * {@code getPartitionIndexByTimestamp} (scan-up) must resolve to the LOWEST-index partition sharing
     * that timestamp (cellKey 0) while {@code getPartitionIndexByTimestampScanDown} must resolve to the
     * HIGHEST-index partition sharing it (cellKey 1, that day's last cell) -- the two must differ by
     * exactly one, matching the day's 2-cell width, and the scan-down result plus one must land exactly
     * on the NEXT day's own first (cellKey 0) partition index.
     */
    @Test
    public void testScanDownFindsHighestCellForMultiCellDay() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            try (TableReader r = getReader("c")) {
                int partitionCount = r.getPartitionCount();
                Assert.assertEquals("d0 (1 cell) + d1/d2/d3 (2 cells each)", 7, partitionCount);

                // Locate a multi-cell day that has a distinct successor day (so the "+1 lands on the next
                // day's cellKey0" assertion below is meaningful) -- d1 fits: cellKey0, not index 0 (d0
                // precedes it), has a same-timestamp sibling (cellKey1), and d2 follows it.
                int midLo = -1;
                for (int i = 0; i < partitionCount - 2; i++) {
                    boolean hasSibling = r.getPartitionCellKey(i) == 0
                            && r.getPartitionTimestampByIndex(i + 1) == r.getPartitionTimestampByIndex(i);
                    boolean hasLaterDay = r.getPartitionTimestampByIndex(i + 2) != r.getPartitionTimestampByIndex(i);
                    if (hasSibling && hasLaterDay) {
                        midLo = i;
                        break;
                    }
                }
                Assert.assertTrue("expected to find a multi-cell day with a distinct successor day", midLo >= 0);
                long midTs = r.getPartitionTimestampByIndex(midLo);

                int scanUpIdx = r.getPartitionIndexByTimestamp(midTs);
                int scanDownIdx = r.getPartitionIndexByTimestampScanDown(midTs);

                Assert.assertEquals("scan-up must resolve this day's timestamp to its cellKey-0 (lowest) partition", midLo, scanUpIdx);
                Assert.assertEquals("scan-up partition must carry cellKey 0", 0, r.getPartitionCellKey(scanUpIdx));
                Assert.assertEquals("this day has exactly 2 cells, so scan-down must land one index above scan-up", scanUpIdx + 1, scanDownIdx);
                Assert.assertEquals("scan-down partition must carry the day's highest cellKey (1)", 1, r.getPartitionCellKey(scanDownIdx));
                // Both partitions genuinely share this day's timestamp (the premise of the whole bug).
                Assert.assertEquals(midTs, r.getPartitionTimestampByIndex(scanDownIdx));
                // scan-down + 1 lands exactly on the next day's own first (cellKey 0) partition.
                Assert.assertEquals(0, r.getPartitionCellKey(scanDownIdx + 1));
                Assert.assertTrue(r.getPartitionTimestampByIndex(scanDownIdx + 1) > midTs);
            }
        });
    }

    /**
     * Task 5a-2. The SEPARATE, pre-existing bug documented in the class javadoc above: {@code ts IN
     * '<day>'}'s low bound unavoidably lands exactly on that day's own start. d2 (2020-01-02) is a
     * genuine MIDDLE day (d1 precedes it, d3 follows it) with 2 cells -- its cellKey-0 ('A') partition is
     * the one whose {@code partitionIndex + 1} (cellKey-1, 'B', the SAME day) was wrongly used as the
     * "next day" for the approx-max ceiling, computing a ceiling ONE MICROSECOND BEFORE its own data.
     * RED (pre-fix): d2's 2 'A' rows (cellKey-0) are silently dropped; only the 2 'B' rows (cellKey-1,
     * never buggy since ITS next slot is genuinely d3) survive -- 2 rows instead of 4.
     */
    @Test
    public void testIntervalInMultiCellMiddleDayIncludesAllCells() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            String predicate = " where ts in '2020-01-02'";
            assertCompositeMatchesPlain(predicate);
            assertQuery("select ts, exch, px from c" + predicate + " order by ts, exch")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("Interval")
                    .returns("""
                            ts\texch\tpx
                            2020-01-02T00:00:00.000000Z\tA\t2.0
                            2020-01-02T06:00:00.000000Z\tA\t2.1
                            2020-01-02T12:00:00.000000Z\tB\t2.2
                            2020-01-02T18:00:00.000000Z\tB\t2.3
                            """);
        });
    }

    /**
     * Task 5a-2. Same underlying bug and day as {@link #testIntervalInMultiCellMiddleDayIncludesAllCells()},
     * but the explicit {@code >=}/{@code <} range shape instead of {@code IN} -- both forms resolve to the
     * identical low-bound-lands-on-the-day intrinsic interval, so both must independently be fixed by (and
     * regression-guarded against) the same change.
     */
    @Test
    public void testRangeLowBoundOnMultiCellMiddleDayIncludesAllCells() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            String predicate = " where ts >= '2020-01-02' and ts < '2020-01-03'";
            assertCompositeMatchesPlain(predicate);
            assertQuery("select ts, exch, px from c" + predicate + " order by ts, exch")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("Interval")
                    .returns("""
                            ts\texch\tpx
                            2020-01-02T00:00:00.000000Z\tA\t2.0
                            2020-01-02T06:00:00.000000Z\tA\t2.1
                            2020-01-02T12:00:00.000000Z\tB\t2.2
                            2020-01-02T18:00:00.000000Z\tB\t2.3
                            """);
        });
    }

    /**
     * Task 5a-2. The last-partition-in-table EDGE case for a NON-LAST cell: d3 (2020-01-03) is both a
     * 2-cell day AND the table's own last day -- there is no later distinct day at all. The fix must still
     * advance d3's cellKey-0 past its cellKey-1 sibling (same day), find no further distinct day, and fall
     * back to {@code ceil(own min)} exactly like the pre-existing (and correct) handling of a genuine last
     * partition -- NOT treat "no more distinct days" as "no more partitions at all" and either loop off
     * the end of the partition array or silently return the wrong (sibling-derived) value. RED (pre-fix):
     * same failure mode as the middle-day case -- d3's 2 'A' rows vanish, leaving only the 2 'B' rows.
     */
    @Test
    public void testIntervalInMultiCellLastDayIncludesAllCells() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            String predicate = " where ts in '2020-01-03'";
            assertCompositeMatchesPlain(predicate);
            assertQuery("select ts, exch, px from c" + predicate + " order by ts, exch")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("Interval")
                    .returns("""
                            ts\texch\tpx
                            2020-01-03T00:00:00.000000Z\tA\t3.0
                            2020-01-03T06:00:00.000000Z\tA\t3.1
                            2020-01-03T12:00:00.000000Z\tB\t3.2
                            2020-01-03T18:00:00.000000Z\tB\t3.3
                            """);
        });
    }

    /**
     * Task 5a-2. Same last-partition-in-table edge as {@link #testIntervalInMultiCellLastDayIncludesAllCells()},
     * but the explicit {@code >=}/{@code <} range shape; the upper bound ({@code 2020-01-04}) is a day that
     * does not exist in the dataset at all, purely a syntactic bound -- proving the fix's fallback does not
     * depend on there being a real row anywhere near the searched-for "next day".
     */
    @Test
    public void testRangeLowBoundOnMultiCellLastDayIncludesAllCells() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            String predicate = " where ts >= '2020-01-03' and ts < '2020-01-04'";
            assertCompositeMatchesPlain(predicate);
            assertQuery("select ts, exch, px from c" + predicate + " order by ts, exch")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("Interval")
                    .returns("""
                            ts\texch\tpx
                            2020-01-03T00:00:00.000000Z\tA\t3.0
                            2020-01-03T06:00:00.000000Z\tA\t3.1
                            2020-01-03T12:00:00.000000Z\tB\t3.2
                            2020-01-03T18:00:00.000000Z\tB\t3.3
                            """);
        });
    }

    /**
     * Task 5a-2. Direct-method proof, independent of SQL/the interval cursor: for every day (single- or
     * multi-cell), every sibling cell of that day must report the IDENTICAL
     * {@code getPartitionMaxTimestampFromMetadata} value -- the ceiling describes the whole DAY, not any
     * one cell of it. Ground truth for each day is taken from that day's LAST cell (highest cellKey),
     * which was never affected by this bug (its own "next slot" is always either a genuinely later day or
     * physically past the end of the table), and is independently cross-checked: against
     * {@code next distinct day's own min - 1} when a later day exists (d0, d1, d2), and against
     * {@code ceil(own min)} (day start + 24h, since DAY partitioning's ceiling is a fixed calendar step)
     * when it does not (d3, the table's own last day). RED (pre-fix): d1/d2/d3's cellKey-0 (non-last cell
     * of a multi-cell day) each disagree with their own day's last cell.
     */
    @Test
    public void testApproxMaxTimestampAgreesAcrossSiblingCells() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            try (TableReader r = getReader("c")) {
                int partitionCount = r.getPartitionCount();
                Assert.assertEquals("d0 (1 cell) + d1/d2/d3 (2 cells each)", 7, partitionCount);

                int i = 0;
                while (i < partitionCount) {
                    long dayTs = r.getPartitionTimestampByIndex(i);
                    int lastCellIndex = i;
                    while (lastCellIndex + 1 < partitionCount && r.getPartitionTimestampByIndex(lastCellIndex + 1) == dayTs) {
                        lastCellIndex++;
                    }
                    // The day's LAST cell was never affected by this bug -- ground truth for the whole day.
                    long groundTruth = r.getPartitionMaxTimestampFromMetadata(lastCellIndex);
                    boolean hasLaterDay = lastCellIndex + 1 < partitionCount;
                    if (hasLaterDay) {
                        Assert.assertEquals(
                                "day " + dayTs + "'s last cell must use the next DISTINCT day's own min - 1 as its ceiling",
                                r.getPartitionMinTimestampFromMetadata(lastCellIndex + 1) - 1,
                                groundTruth);
                    } else {
                        Assert.assertEquals(
                                "table's last day (" + dayTs + ") falls back to ceil(own min) = day start + 24h",
                                dayTs + Micros.DAY_MICROS,
                                groundTruth);
                    }
                    for (int j = i; j <= lastCellIndex; j++) {
                        Assert.assertEquals(
                                "partition " + j + " (cellKey=" + r.getPartitionCellKey(j) + ", day=" + dayTs
                                        + ") must agree with its day's last cell (index " + lastCellIndex
                                        + ") on the approx-max timestamp",
                                groundTruth,
                                r.getPartitionMaxTimestampFromMetadata(j));
                    }
                    i = lastCellIndex + 1;
                }
            }
        });
    }

    /**
     * Task 6c review Part A -- the MULTI-INTERVAL sibling-drop shape, now LOUD-GATED. Commit
     * {@code d31aa88716} fixed SINGLE-interval sibling visiting, but a query with 2+ intervals hitting the
     * SAME multi-cell day still SILENTLY dropped rows: {@code partitionLo}/{@code partitionHi} advance
     * monotonically, so once the first interval consumed a day's cells the later interval could not
     * revisit them. Here day1 has two genuinely interleaved cells X and Y, each with a row in BOTH sub-day
     * intervals ({@code [01:00,02:00)} and {@code [03:00,04:00)}); pre-gate the forward scan dropped X's
     * 03:00 row (it advanced past X to Y for the first interval and never returned), and the backward scan
     * + {@code calculateSize} (count) dropped the symmetric row.
     * <p>
     * A correct real fix would require the interval cursor to iterate cells and intervals as a 2D grid
     * (per-cell interval reset) while STILL emitting frames in the day-contiguous, per-cell-contiguous
     * order the downstream {@code CompositeMergePartitionRecordCursor} requires -- too invasive to do
     * safely here without risking a subtly-wrong scan in the hottest query path. So this shape is
     * LOUD-GATED (a clear {@code CairoException}) at the exact point the drop becomes imminent, in all
     * four cursor paths (forward/backward {@code next()} and {@code calculateSize()}); the plain twin,
     * which never has a same-day sibling, still answers the identical query correctly.
     */
    @Test
    public void testTwoSubDayIntervalsOverOneMultiCellDayIsLoudGated() throws Exception {
        assertMemoryLeak(() -> {
            createInterleavedTwins();
            engine.releaseInactive();

            // Sanity: a SINGLE sub-day interval over the multi-cell day IS correct (commit d31aa88716) --
            // not gated, matches the plain twin.
            final String single = " where ts in '2020-06-01T01:00:00.000000Z;1h'";
            assertSqlCursors("select ts, exch, px from pi" + single + " order by ts, exch",
                    "select ts, exch, px from ci" + single + " order by ts, exch");

            // The gated shape: TWO sub-day intervals over the SAME multi-cell day -- forward scan,
            // backward scan, and count() (calculateSize) all throw the same clear error.
            final String msg = "composite partitioning does not yet support multiple sub-day time intervals over a single multi-cell day";
            final String twoIntervals =
                    " where ts in '2020-06-01T01:00:00.000000Z;1h' or ts in '2020-06-01T03:00:00.000000Z;1h'";
            assertQuery("select ts, exch, px from ci" + twoIntervals + " order by ts").noLeakCheck().failsWith(msg);
            assertQuery("select ts, exch, px from ci" + twoIntervals + " order by ts desc").noLeakCheck().failsWith(msg);
            assertQuery("select count() from ci" + twoIntervals).noLeakCheck().failsWith(msg);

            // The plain twin is composite-agnostic: the identical query still returns all 4 matching rows.
            assertQuery("select count() from pi" + twoIntervals).noLeakCheck().noRandomAccess().expectSize().returns("count\n4\n");
        });
    }

    /**
     * Task 6c review Part A -- the MUST-NOT-BREAK case. Multiple WHOLE-day intervals on DIFFERENT
     * multi-cell days ({@code ts in 'day1' or ts in 'day3'}) were already correct and must stay correct:
     * each interval maps to a distinct day, so the monotonic {@code partitionLo} advance never needs to
     * revisit a day. Guards the Part A fix against regressing the common multi-day date-list shape.
     */
    @Test
    public void testMultipleWholeDayIntervalsAcrossDifferentDaysMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createInterleavedTwins();
            engine.releaseInactive();

            final String predicate = " where ts in '2020-06-01' or ts in '2020-06-03'";
            assertSqlCursors("select ts, exch, px from pi" + predicate + " order by ts, exch",
                    "select ts, exch, px from ci" + predicate + " order by ts, exch");
            assertSqlCursors("select ts, exch, px from pi" + predicate + " order by ts desc, exch",
                    "select ts, exch, px from ci" + predicate + " order by ts desc, exch");
            assertSqlCursors("select count() from pi" + predicate, "select count() from ci" + predicate);
        });
    }

    /**
     * Builds composite {@code ci} ({@code partition by day, exch}) and plain twin {@code pi} with two
     * genuinely INTERLEAVED cells X and Y per day over 3 days (2020-06-01..03): each day, cell X has rows
     * at 01:00 and 03:00, cell Y at 01:15 and 03:15 -- so both cells have a row inside each of the two
     * sub-day windows {@code [01:00,02:00)} and {@code [03:00,04:00)}. X is listed first so it interns as
     * cellKey 0. One bulk insert per table.
     */
    private void createInterleavedTwins() throws SqlException {
        execute("create table ci (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
        execute("create table pi (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");
        final StringBuilder rows = new StringBuilder(" values ");
        final String[] days = {"2020-06-01", "2020-06-02", "2020-06-03"};
        boolean first = true;
        for (int d = 0; d < days.length; d++) {
            // X first (interns as cellKey 0), then Y; two rows each, interleaved across the two windows.
            final String[][] cells = {
                    {"X", "01:00", "03:00"},
                    {"Y", "01:15", "03:15"},
            };
            for (int cIdx = 0; cIdx < cells.length; cIdx++) {
                final String exch = cells[cIdx][0];
                for (int t = 1; t < cells[cIdx].length; t++) {
                    if (!first) {
                        rows.append(", ");
                    }
                    first = false;
                    rows.append("('").append(days[d]).append('T').append(cells[cIdx][t])
                            .append(":00.000000Z','").append(exch).append("',")
                            .append(d + 1).append('.').append(cIdx).append(t).append(')');
                }
            }
        }
        execute("insert into ci" + rows);
        execute("insert into pi" + rows);
        drainWalQueue();
    }

    /**
     * Builds composite table {@code c} ({@code partition by day, exch} -- 2 cells/day for d1..d3) and its
     * plain twin {@code p} ({@code partition by day}, {@code exch} an ordinary column), then inserts
     * byte-for-byte identical rows into both via one bulk insert per table: a sentinel single-cell day d0
     * (2019-12-31, 'A' only, 2 rows) followed by 3 two-cell days d1..d3 (2020-01-01..03, 'A' mornings at
     * 00:00/06:00, 'B' afternoons at 12:00/18:00 -- deliberately ordered so 'A' is always the
     * first-seen, and hence cellKey-0, value for every day), 2 rows per (day, cell) -- 14 rows total. d0
     * is deliberately single-cell so every test below can anchor its LOW bound there (or earlier) without
     * tripping the separate, out-of-scope bug documented on the class javadoc.
     */
    private void createAndPopulateTwins() throws SqlException {
        execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
        execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

        final String rows = " values " +
                "('2019-12-31T00:00:00.000000Z','A',0.1), ('2019-12-31T12:00:00.000000Z','A',0.2), " +
                "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T06:00:00.000000Z','A',1.1), " +
                "('2020-01-01T12:00:00.000000Z','B',1.2), ('2020-01-01T18:00:00.000000Z','B',1.3), " +
                "('2020-01-02T00:00:00.000000Z','A',2.0), ('2020-01-02T06:00:00.000000Z','A',2.1), " +
                "('2020-01-02T12:00:00.000000Z','B',2.2), ('2020-01-02T18:00:00.000000Z','B',2.3), " +
                "('2020-01-03T00:00:00.000000Z','A',3.0), ('2020-01-03T06:00:00.000000Z','A',3.1), " +
                "('2020-01-03T12:00:00.000000Z','B',3.2), ('2020-01-03T18:00:00.000000Z','B',3.3)";
        execute("insert into c" + rows);
        execute("insert into p" + rows);
        drainWalQueue();
    }

    /**
     * Full row-content AND count parity between {@code c} and {@code p} for the given WHERE predicate
     * (including its leading space), ordered {@code ts, exch} for determinism, mirroring
     * {@code CompositeRoutingTest}/{@code CompositeRoutingEndToEndTest}'s own established
     * {@code assertTablesMatch}-style idiom.
     */
    private void assertCompositeMatchesPlain(String predicate) throws SqlException {
        assertSqlCursors(
                "select ts, exch, px from p" + predicate + " order by ts, exch",
                "select ts, exch, px from c" + predicate + " order by ts, exch");
        assertSqlCursors("select count() from p" + predicate, "select count() from c" + predicate);
    }
}
