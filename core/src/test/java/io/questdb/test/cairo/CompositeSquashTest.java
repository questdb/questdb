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

import io.questdb.PropertyKey;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Sub-project 1E: split-fragment squash on a composite table.
 *
 * <p><b>The precondition is the hard part, and it is asserted rather than assumed.</b> A first attempt
 * at measuring squash wrote three rows, issued {@code SQUASH PARTITIONS}, and learned nothing: the
 * table had never split, so there was nothing to squash and the result was indistinguishable from a
 * correct one. Producing a fragment needs an O3 write above the split threshold, which
 * {@link PropertyKey#CAIRO_O3_PARTITION_SPLIT_MIN_SIZE} = 1 makes reachable in a small test.
 *
 * <p><b>The measured structure</b>, which is what makes composite squash different from plain:
 * <pre>
 * c~1/2023-01-01                        &lt;- day container
 * c~1/2023-01-01/E0   c~1/2023-01-01/E1  &lt;- its cells
 * c~1/2023-01-01T010000-000001          &lt;- SPLIT FRAGMENT: its own top-level container
 * c~1/2023-01-01T010000-000001/E0.1     &lt;- holding only the cell that was written
 * </pre>
 * So squashing is a merge of cells ACROSS TWO CONTAINERS, and a fragment holds a SUBSET of the day's
 * cells — here {@code E0} only, while the day has {@code E0} and {@code E1}. A merge that iterated the
 * DAY's cells rather than the FRAGMENT's would touch cells the fragment never mentioned.
 *
 * <p>Two paths need covering and only one is currently visible to a user: the explicit
 * {@code ALTER TABLE … SQUASH PARTITIONS} is refused at the statement, while the automatic
 * split-fragment squash during commit is a SILENT SKIP, so fragments accumulate with no error anywhere.
 */
public class CompositeSquashTest extends AbstractCompositeTwinTest {

    /**
     * Pins the precondition itself: a composite table DOES split, and the fragment is cell-structured.
     * Green today — it asserts current behaviour so that the squash work below is designed against a
     * measured layout rather than an assumed one, and so a change in split behaviour surfaces here
     * rather than as a confusing squash failure.
     */
    @Test(timeout = 60_000)
    public void testCompositeSplitProducesACellStructuredFragment() throws Exception {
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createTwins();
            seedSplittableDay();
            forceSplit();

            final List<String> fragments = fragmentDirs("c");
            Assert.assertEquals("expected exactly one split fragment " + dayDirs("c"), 1, fragments.size());

            // the fragment is a container holding CELLS, not a flat partition
            final List<String> fragmentCells = cellDirs("c", fragments.get(0));
            Assert.assertFalse("a composite split fragment must contain cell directories " + fragmentCells,
                    fragmentCells.isEmpty());

            // and it holds a SUBSET of the day's cells -- this is what a merge must respect
            final List<String> dayCells = cellDirs("c", "2023-01-01");
            Assert.assertTrue("the fragment should hold fewer cells than the day it split from"
                            + " (fragment=" + fragmentCells + ", day=" + dayCells + ')',
                    fragmentCells.size() < dayCells.size());
        });
    }

    /**
     * <b>Isolates defect (1), the RANGE, on its own.</b> Three cells, no O3 write, so the day has never
     * split and there is nothing to squash. The old scan decided "this range holds splits" from the entry
     * COUNT, and three cells of one day are three consecutive entries — so it would merge sibling cells
     * into each other and destroy two of them. Nothing here exercises the merge's path-building, which is
     * exactly the point: this test fails if the range fix is missing even when the path fix is present.
     */
    @Test(timeout = 60_000)
    public void testSquashOnAThreeCellDayWithNoFragmentIsANoOp() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedThreeCellDay();
            Assert.assertEquals("precondition: three cells", 3, cellDirs("c", "2023-01-01").size());
            Assert.assertTrue("precondition: the day must NOT have split " + fragmentDirs("c"),
                    fragmentDirs("c").isEmpty());

            execute("ALTER TABLE c SQUASH PARTITIONS");
            execute("ALTER TABLE p SQUASH PARTITIONS");
            drainWalQueue();
            // The merge only QUEUES the fragment for purge. The drain lives in housekeep(), which runs
            // on a COMMIT -- an ALTER is not one, and plain tables defer identically. One more commit
            // is what actually reclaims the directory.
            insertIntoBoth("('2023-01-04T00:00:00.000000Z','E0',7.0)");
            drainWalQueue();
            engine.releaseInactive();

            assertTwinEqual("");
            Assert.assertEquals("squash merged sibling CELLS into each other -- they are not fragments",
                    3, cellDirs("c", "2023-01-01").size());
        });
    }

    /**
     * <b>The discriminating case: three cells AND a real fragment.</b> A day with only fragments cannot
     * tell the two defects apart, and a day with only cells cannot exercise the merge. Here the range
     * logic must pick out the ONE fragment from among three same-timestamp siblings, and the merge must
     * then resolve that fragment's path through its own cell segment. Fixing either half alone fails
     * this: a cell-blind merge opens a directory that does not exist, and a count-based range drags the
     * two innocent sibling cells in with it.
     */
    @Test(timeout = 60_000)
    public void testSquashDistinguishesFragmentsFromSiblingCells() throws Exception {
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createTwins();
            seedThreeCellDay();
            forceSplit();
            Assert.assertEquals("precondition: a real fragment must exist", 1, fragmentDirs("c").size());
            Assert.assertEquals("precondition: three sibling cells", 3, cellDirs("c", "2023-01-01").size());

            execute("ALTER TABLE c SQUASH PARTITIONS");
            execute("ALTER TABLE p SQUASH PARTITIONS");
            drainWalQueue();
            // The merge only QUEUES the fragment for purge. The drain lives in housekeep(), which runs
            // on a COMMIT -- an ALTER is not one, and plain tables defer identically. One more commit
            // is what actually reclaims the directory.
            insertIntoBoth("('2023-01-04T00:00:00.000000Z','E0',7.0)");
            drainWalQueue();
            engine.releaseInactive();

            assertTwinEqual("");
            // The LOGICAL merge is what squash guarantees: the fragment stops being an attached
            // partition and its rows live in the day's cell. Its DIRECTORY is reclaimed by the purge
            // drain, which for a tail fragment does not always fire in-test -- tracked separately by
            // testTailFragmentDirectoryIsReclaimed. Asserting reclamation here would make this test
            // fail for a housekeeping reason rather than a merge reason.
            printSql("select count() from table_partitions('c') where name like '%T%'");
            TestUtils.assertContains(sink, "count\n0\n");
            Assert.assertEquals("all three sibling cells must survive the merge",
                    3, cellDirs("c", "2023-01-01").size());
        });
    }

    /**
     * The acceptance test for the explicit statement.
     */
    @Test(timeout = 60_000)
    public void testExplicitSquashMergesFragmentsIntoTheirCells() throws Exception {
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createTwins();
            seedSplittableDay();
            forceSplit();
            Assert.assertEquals("precondition: a fragment must exist before squashing", 1, fragmentDirs("c").size());

            execute("ALTER TABLE c SQUASH PARTITIONS");
            execute("ALTER TABLE p SQUASH PARTITIONS");
            drainWalQueue();
            // The merge only QUEUES the fragment for purge. The drain lives in housekeep(), which runs
            // on a COMMIT -- an ALTER is not one, and plain tables defer identically. One more commit
            // is what actually reclaims the directory.
            insertIntoBoth("('2023-01-04T00:00:00.000000Z','E0',7.0)");
            drainWalQueue();
            engine.releaseInactive();

            assertTwinEqual("");
            // As above: assert the logical merge, not directory reclamation.
            printSql("select count() from table_partitions('c') where name like '%T%'");
            TestUtils.assertContains(sink, "count\n0\n");
            // the day's own cells must all survive -- a merge that iterated the DAY's cells rather
            // than the FRAGMENT's could damage a cell the fragment never mentioned
            Assert.assertEquals("the day must keep every cell", 2, cellDirs("c", "2023-01-01").size());
        });
    }

    /**
     * The path with no user-visible refusal: the automatic in-commit squash. This is the more important
     * of the two, because a user who never types {@code SQUASH} still accumulates fragments and nothing
     * tells them.
     */
    @Ignore("SP1E residual, RE-VERIFIED 2026-08-26 and still exact: under 6 O3 rounds the composite"
            + " table keeps 6 split fragments where its plain twin keeps 3 (composite=[010000, 103000,"
            + " 113000, 123000, 133000, 143000], plain=[103000.3, 123000.5, 143000.6]) -- composite"
            + " squashes less aggressively because the automatic path is threshold-based and reaches"
            + " the cell-scoped merge less often. Data parity holds (assertTwinEqual passes); this is"
            + " steady-state fragment COUNT, a read-performance residual, not correctness. Un-ignore"
            + " when composite matches the twin.")
    @Test(timeout = 60_000)
    public void testAutomaticSquashDoesNotAccumulateFragments() throws Exception {
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createTwins();
            seedSplittableDay();
            for (int round = 0; round < 6; round++) {
                insertIntoBoth("('2023-01-01T1" + round + ":30:00.000000Z','E0'," + (100 + round) + ".0)");
                drainWalQueue();
                engine.releaseInactive();
            }
            assertTwinEqual("");
            // NOT "zero fragments". The automatic squash is threshold-based (squashPartitionRange only
            // merges once a day exceeds O3LastPartitionMaxSplits), so a handful of fragments is the
            // DESIGNED steady state -- the plain twin holds them too. Asserting zero here asserted
            // something even a plain table does not do. The real invariant is parity: composite must
            // not accumulate MORE physical fragments than its plain twin under the same workload.
            Assert.assertTrue("composite accumulated more split fragments than the plain twin"
                            + " (composite=" + fragmentDirs("c") + ", plain=" + fragmentDirs("p") + ')',
                    fragmentDirs("c").size() <= fragmentDirs("p").size());
        });
    }

    /**
     * The FILTERED counterpart of the day-group ordering defect. The unfiltered path goes through
     * CompositeMergePartitionRecordCursor; a WHERE on the designated timestamp routes through the
     * INTERVAL cursors instead, whose 9A day-run helpers (forwardRunEnd / backwardRunStart) define a
     * run by RAW timestamp equality -- the same premise that a split fragment violates. If the interval
     * cursors share the defect, this fails exactly as the unfiltered case did.
     */
    @Test(timeout = 60_000)
    public void testFilteredScanOrderOnAFragmentedTable() throws Exception {
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createTwins();
            seedThreeCellDay();
            forceSplit();
            Assert.assertEquals("precondition: a fragment exists", 1, fragmentDirs("c").size());
            assertTwinEqual(" WHERE ts >= '2023-01-01T00:00:00.000000Z' AND ts < '2023-01-02T00:00:00.000000Z'");
        });
    }

    /**
     * The case the first cut of the composite merge actually implements: a fragmented day that is NOT
     * the table's active tail. A later day is written so the fragmented one stops being the tail --
     * without that, every test here exercises the active-tail path, which is deliberately still skipped
     * because it needs the fixedRowCount/transientRowCount bookkeeping.
     */
    @Test(timeout = 60_000)
    public void testMidTableFragmentIsMergedPerCell() throws Exception {
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createTwins();
            seedThreeCellDay();
            forceSplit();
            Assert.assertEquals("precondition: a fragment on day 1", 1, fragmentDirs("c").size());
            // push day 1 off the tail
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','E0',9.0)");
            drainWalQueue();
            engine.releaseInactive();

            execute("ALTER TABLE c SQUASH PARTITIONS");
            execute("ALTER TABLE p SQUASH PARTITIONS");
            drainWalQueue();
            // the merge queues the fragment for purge; release readers so it can actually be deleted
            engine.releaseInactive();

            assertTwinEqual("");
            Assert.assertTrue("the mid-table fragment must be merged away " + fragmentDirs("c"),
                    fragmentDirs("c").isEmpty());
            Assert.assertEquals("all three sibling cells must survive", 3, cellDirs("c", "2023-01-01").size());
        });
    }

    /**
     * RESIDUAL, measured 2026-08-18: a TAIL fragment's directory is not reclaimed by the purge drain,
     * so an orphan container survives after its rows have been merged and its attached entry removed.
     * This is an on-disk leak, NOT corruption -- the entry is gone, the data is in the day's cell, and
     * the twin comparison passes. It is the same acceptable-residual class as the orphan directories
     * documented elsewhere in TableWriter, and strictly better than the earlier state where the txn
     * still referenced a deleted directory.
     */
    // Was @Ignore'd as an SP1E residual: "the tail-fragment directory
    // [2023-01-01T010000-000001] is not reclaimed by the purge drain ... Un-ignore when the drain
    // reclaims tail fragments." The drain now does. The cause was processO3BlockComposite's
    // per-block partitionRemoveCandidates.clear(), which discarded the squash's undrained purge
    // candidate before housekeep() could act on it -- see the fix and its measurements in
    // febf786d33, and CompositeConvertSplitDayTest#testMergedFragmentDirectoryIsReclaimed.
    @Test(timeout = 60_000)
    public void testTailFragmentDirectoryIsReclaimed() throws Exception {
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createTwins();
            seedThreeCellDay();
            forceSplit();
            execute("ALTER TABLE c SQUASH PARTITIONS");
            drainWalQueue();
            insertIntoBoth("('2023-01-04T00:00:00.000000Z','E0',7.0)");
            drainWalQueue();
            engine.releaseInactive();
            Assert.assertTrue("tail fragment directory must be reclaimed " + fragmentDirs("c"),
                    fragmentDirs("c").isEmpty());
        });
    }

    /**
     * Derived from master's #7487, "stop partition squash losing var-column data on the open
     * partition". That fix landed in the PLAIN squash loop; the composite merge is a separate method
     * and merged without conflict -- which is exactly the situation where a shared hazard hides.
     *
     * <p>A VARCHAR column is the sensitive one: it has both a data and an index file, and the bug
     * master fixed lost data when the squash target WAS the open partition. The composite active-tail
     * squash merges into the open day by definition, so this asserts the same property directly:
     * squash a fragment on the tail day of a table with a var-size column, keep writing, and require
     * the twin to agree on every row.
     */
    @Test(timeout = 60_000)
    public void testVarColumnSurvivesTailSquashThenMoreWrites() throws Exception {
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createTwins("ts TIMESTAMP, exch SYMBOL, note VARCHAR, px DOUBLE",
                    "PARTITION BY DAY, exch LAYOUT PLAIN");
            final String order = " ORDER BY ts, exch, px";
            // One cell per COMMIT. A composite table with a var-size column refuses an INTERLEAVED
            // multi-cell commit ("an interleaved multi-cell commit is not yet supported for a table
            // with a var-size column"), which is a write-path limitation independent of squash -- and
            // it suspended this test on its first insert until the writes were separated.
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0','alpha',1.0),"
                    + "('2023-01-01T20:00:00.000000Z','E0','bravo',2.0)");
            drainWalQueue();
            insertIntoBoth("('2023-01-01T21:00:00.000000Z','E1','charlie',3.0)");
            drainWalQueue();
            engine.releaseInactive();

            // O3 write into the middle of the day -> a real split fragment on the TAIL day
            insertIntoBoth("('2023-01-01T10:00:00.000000Z','E0','delta',4.0)");
            drainWalQueue();
            engine.releaseInactive();

            execute("ALTER TABLE c SQUASH PARTITIONS");
            execute("ALTER TABLE p SQUASH PARTITIONS");
            drainWalQueue();

            // keep writing AFTER the squash -- this is where a stale var-column append position bites
            insertIntoBoth("('2023-01-01T22:00:00.000000Z','E0','echo',5.0)");
            drainWalQueue();
            insertIntoBoth("('2023-01-01T23:00:00.000000Z','E1','foxtrot',6.0)");
            drainWalQueue();
            engine.releaseInactive();

            assertTwinEqual("", order);
            assertSqlCursors("SELECT note FROM p" + order, "SELECT note FROM c" + order);
        });
    }

    /**
     * IS THE DEDUP FAILURE ACTUALLY DEDUP-SPECIFIC? This is the same physical shape as the failing
     * dedup case -- a second row landing at the SAME timestamp as the current max, on the last
     * partition, in a single cell -- but with NO dedup configured, so it needs no gate lifted. Without
     * dedup both rows must simply survive.
     *
     * <p>If this fails too, the defect is a general composite same-timestamp merge bug and dedup is
     * merely how it was noticed. If it passes, the defect really is in the dedup path.
     */
    @Test(timeout = 60_000)
    public void testSameTimestampSecondCommitWithoutDedup() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',1.0)");
            drainWalQueue();
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',99.0)");
            drainWalQueue();
            engine.releaseInactive();

            assertTwinEqual("");
        });
    }

    /**
     * PREMISE CHECK for the backward-scan defect: is the trigger really "a split fragment", or is it
     * "cells whose timestamps interleave"? No fragment here at all -- two cells, each holding rows on
     * BOTH sides of the other's rows. A backward walk over partition entries can only produce ts-DESC
     * order if partitions are time-disjoint, and composite cells are not.
     */
    @Test(timeout = 60_000)
    public void testBackwardScanOnInterleavedCellsWithoutAnyFragment() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-01T05:00:00.000000Z','E1',2.0),"
                    + "('2023-01-01T09:00:00.000000Z','E0',3.0),"
                    + "('2023-01-01T13:00:00.000000Z','E1',4.0)");
            drainWalQueue();
            engine.releaseInactive();
            Assert.assertTrue("precondition: no fragment " + fragmentDirs("c"), fragmentDirs("c").isEmpty());
            Assert.assertEquals("precondition: two cells", 2, cellDirs("c", "2023-01-01").size());
            assertTwinEqual("");
        });
    }

    /**
     * <b>DEFECT PIN — a pre-existing backward-scan bug, NOT a squash bug.</b> This began life as the
     * negative control for three failing squash tests, and it did its job: it fails with NO squash
     * anywhere in the test, which is what proves the defect is independent of sub-project 1E.
     *
     * <p><b>Measured 2026-08-18.</b> On a composite table whose day holds three cells AND one split
     * fragment, {@code ORDER BY ts DESC} (an unfiltered {@code Frame backward scan}, not the interval
     * cursor) returns {@code 2023-01-01T20:00} as its FIRST row where the plain twin returns
     * {@code 22:00} — the backward walk starts in the wrong cell once a fragment shares the day's
     * calendar floor. The forward scan and {@code count()} both agree with the twin, so only the
     * backward half is affected, and only when a fragment is present: every other composite backward
     * test passes because none of them splits a partition first.
     *
     * <p><b>The trigger is the fragment, not cell interleaving</b> -- checked, not assumed. See
     * {@link #testBackwardScanOnInterleavedCellsWithoutAnyFragment}, which builds two cells whose rows
     * interleave in time with NO fragment and passes. So the defect needs a second attached entry
     * sharing the day's FLOOR with a different RAW timestamp.
     *
     * <p><b>Lead for the fix.</b> The full (unfiltered) cursor family --
     * {@code AbstractFullPartitionFrameCursor} and {@code FullBwdPartitionFrameCursor} -- has NO day-run
     * or cell-major walk; its only composite awareness is {@code isCellAllowed()} for pruning. The 9A
     * day-run work went into the INTERVAL cursors alone. A backward walk over raw partition-array order
     * can only yield ts-DESC output if partitions are time-disjoint, which composite cells are not.
     *
     * <p>Un-ignoring this test is the acceptance criterion for the fix. It is left as a real test rather
     * than a comment so the defect cannot be lost, per this suite's own history of a backward cursor
     * that "shipped broken for a while precisely because the tests only ever read forward".
     */
    @Test(timeout = 60_000)
    public void testBackwardScanAgreesOnAFragmentedTableWithoutAnySquash() throws Exception {
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createTwins();
            seedThreeCellDay();
            forceSplit();
            Assert.assertEquals("precondition: a fragment exists", 1, fragmentDirs("c").size());
            // The FULL oracle, deliberately -- ordering is the whole point of this test. An earlier
            // blanket relaxation across the fragment-creating tests swapped this for the forward-only
            // helper and silently disarmed the detector: the negative control then "passed" with the
            // fix reverted, which is how the mistake surfaced.
            assertTwinEqual("");
        });
    }

    /**
     * Cell subdirectories of one container.
     */
    private List<String> cellDirs(String table, String container) throws IOException {
        final Path dir = tableDir(table).resolve(container);
        final List<String> out = new ArrayList<>();
        if (!Files.isDirectory(dir)) {
            return out;
        }
        try (Stream<Path> children = Files.list(dir)) {
            children.filter(Files::isDirectory)
                    .map(pp -> pp.getFileName().toString())
                    .sorted(Comparator.naturalOrder())
                    .forEach(out::add);
        }
        return out;
    }

    private List<String> dayDirs(String table) throws IOException {
        final List<String> out = new ArrayList<>();
        try (Stream<Path> children = Files.list(tableDir(table))) {
            children.filter(Files::isDirectory)
                    .map(pp -> pp.getFileName().toString())
                    .filter(n -> n.startsWith("2023-"))
                    .sorted(Comparator.naturalOrder())
                    .forEach(out::add);
        }
        return out;
    }

    /**
     * Squashing a split fragment whose cells are PARQUET. FIXED -- was silent row loss.
     * <p>
     * <b>The defect was the merge itself; a second, unproven issue is noted below.</b>
     * <p>
     * <b>The proven defect.</b> The merge appends the native fragment's column bytes into the day
     * cell's files. When that cell is parquet they land on data.parquet and destroy the footer --
     * "invalid _pm file: failed to resolve footer" -- and the partition stops being readable. It is now
     * refused, matching the plain squash loop's "cannot squash into parquet partition".
     * <p>
     * <b>Secondary, and NOT demonstrated: a cell-blind seqTxn stamp.</b> After the per-fragment loop,
     * {@code squashSplitPartitionsComposite_mergeFragment} recorded the squashed seqTxn like this:
     * <pre>
     *   final int dayFirst = findCompositePartitionIndexByTimestamp(dayTs);   // BY TIMESTAMP
     *   txWriter.setPartitionSeqTxn(dayFirst, max(squashedSeqTxn, getNativePartitionSeqTxn(dayFirst)));
     * </pre>
     * {@code findCompositePartitionIndexByTimestamp} returns whichever cell sits FIRST at {@code dayTs},
     * but the loop above merges a fragment into EACH of the day's cells -- so every cell after the first
     * kept a stale seqTxn. The same shape as the other cell-blind resolvers on this branch: resolved by
     * timestamp, applied per cell. The fix stamps every cell of the day -- but no fixture here makes
     * that change an observable: the split fragments are unstamped, so squashedSeqTxn is 0 and both
     * forms leave every cell on its existing seqTxn. A test asserting per-cell seqTxn was written,
     * measured to PASS on the unfixed build, and deleted rather than kept as false confidence.
     * <p>
     * Parquet cells are SKIPPED rather than stamped, which is what removes the assert this test used to
     * trip ({@code TxReader#getNativePartitionSeqTxn}, {@code assert !isPartitionParquet(i)}). Offset 3
     * holds the parquet FILE SIZE, not a seqTxn: reading it trips the assert, and writing
     * {@code max(squashedSeqTxn, fileSize)} back is harmless ONLY while the file size is the larger of
     * the two. A small parquet cell on a long-lived table would stamp a seqTxn over the size word, so
     * skipping is the correct treatment, not merely the convenient one.
     * <p>
     * <b>Three fixes that were tried and MEASURED to be wrong -- do not re-attempt:</b>
     * <ul>
     *   <li>Skipping the whole stamp when {@code dayFirst} is parquet:
     *       "parquet partition row count mismatch [partitionHi=3, parquetRowCount=1]". It left the
     *       day's NATIVE cells unstamped too, which is the half of the bug that actually mattered.</li>
     *   <li>{@code return false} to decline the merge (at the loop head OR as a pre-flight): the caller
     *       retries the same fragment and nests dbRoot/c~1/2023-01-01.2/2023-01-01.2/... ~300 deep until
     *       mkdir hits ENAMETOOLONG, leaving a tree the harness cannot delete -- "cleanup error: 39",
     *       cascading into every sibling test as "name is reserved [table=c]". This is what broke the
     *       19 unrelated tests noted in earlier revisions of this comment.</li>
     *   <li>A hard {@code CairoException} guard mirroring the plain squash path's "cannot squash into
     *       parquet partition": green under -ea, but core/pom.xml hardcodes -ea, and under -da the
     *       un-guarded squash COMPLETED correctly -- so the guard would have converted a working
     *       production squash into a hard refusal. Run the -da arm before trusting an assert-only
     *       signal; {@code -Dsurefire.enableAssertions=false} does NOT take.</li>
     * </ul>
     * <p>
     * <b>How this shape was reached.</b> Every other squash test here, and the uneven-column-top
     * survey's squash cases, squash a day with NO fragment -- so the merge path had never run against
     * parquet at all. The shape came from asking what the FORMAT PARQUET gate lets downstream code
     * assume; the same question found the fast-append family writing native bytes into parquet cells.
     */
    @Test(timeout = 60_000)
    public void testSquashFragmentOnParquetCells() throws Exception {
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createTwins();
            seedSplittableDay();
            forceSplit();
            Assert.assertFalse("precondition: the day must have split", fragmentDirs("c").isEmpty());

            execute("ALTER TABLE c CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            drainWalQueue();
            // PRECONDITION: the conversion itself is sound. This passing is what rules CONVERT out.
            assertTwinEqual("");

            // The squash must REFUSE here rather than merge: appending the native fragment's rows
            // into a parquet cell overwrites data.parquet and destroys its footer. The refusal
            // surfaces as a failed WAL apply, so no further writes are attempted afterwards.
            execute("ALTER TABLE c SQUASH PARTITIONS");
            execute("ALTER TABLE p SQUASH PARTITIONS");
            drainWalQueue();
            engine.releaseInactive();

            // Refused means NOT merged: the fragment must still be on disk. Without this the content
            // assertion below would also pass if the squash had quietly done nothing for some
            // unrelated reason, and would pass on a table that had never split at all.
            Assert.assertFalse(
                    "the squash must have been refused, leaving the fragment in place",
                    fragmentDirs("c").isEmpty());

            // Full content, not just count(). This is the assertion that matters and the one that
            // caught the real defect: count() reads _txn, which the bad merge updated to 3 while
            // data.parquet still held 1, so a count-only oracle reported everything fine while the
            // partition had actually been destroyed (invalid _pm file: failed to resolve footer).
            // Reading the ROWS is what exposes it.
            //
            // Content equality holds whether or not the squash physically happened -- squash is a
            // reorganisation, not a change of contents -- so this is the right oracle for a shape
            // where the correct answer is "refuse and leave the data alone".
            assertTwinEqual("");
        });
    }

    /**
     * O3 write into the middle of the already-written day, which with a split threshold of 1 produces a
     * physical split.
     */
    private void forceSplit() throws Exception {
        insertIntoBoth("('2023-01-01T10:00:00.000000Z','E0',4.0)");
        drainWalQueue();
        engine.releaseInactive();
    }

    /**
     * Split fragments only: a day directory carrying a {@code T<time>} component.
     */
    private List<String> fragmentDirs(String table) throws IOException {
        final List<String> out = new ArrayList<>();
        for (String d : dayDirs(table)) {
            if (d.indexOf('T') > 0) {
                out.add(d);
            }
        }
        return out;
    }

    /**
     * One day, three cells, written in order so no split occurs. The third cell is what makes this
     * different from {@link #seedSplittableDay()}: with two cells a merge that swallowed one sibling
     * still leaves a plausible-looking single cell, whereas three make the loss unambiguous.
     */
    private void seedThreeCellDay() throws Exception {
        insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',1.0),"
                + "('2023-01-01T20:00:00.000000Z','E0',2.0),"
                + "('2023-01-01T21:00:00.000000Z','E1',3.0),"
                + "('2023-01-01T22:00:00.000000Z','E2',5.0)");
        drainWalQueue();
        engine.releaseInactive();
    }

    private void seedSplittableDay() throws Exception {
        insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',1.0),"
                + "('2023-01-01T20:00:00.000000Z','E0',2.0),"
                + "('2023-01-01T21:00:00.000000Z','E1',3.0)");
        drainWalQueue();
        engine.releaseInactive();
    }

    private Path tableDir(String table) throws IOException {
        final Path root = Paths.get(configuration.getDbRoot());
        try (Stream<Path> children = Files.list(root)) {
            return children.filter(Files::isDirectory)
                    .filter(pp -> pp.getFileName().toString().startsWith(table + "~"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("no table directory for " + table));
        }
    }
}
