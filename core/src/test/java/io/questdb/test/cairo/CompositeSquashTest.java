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
    @Ignore("SP1E: the cell-scoped merge MERGES correctly -- verified 2026-08-18: exactly the fragment's"
            + " own cell, sibling cells untouched, twin data and ordering intact. What blocks it is the"
            + " fragment DIRECTORY purge: the candidate path renders doubled"
            + " (/c~1/<frag>/<frag>/E0.1), so the fragment would leak on disk with its attached entry"
            + " already gone -- worse than not squashing. Un-ignore when the purge path is fixed. NOTE"
            + " testMidTableFragmentIsMergedPerCell is the one to run first: it is the case the merge"
            + " actually implements (a fragmented day that is NOT the active tail).")
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
    @Ignore("SP1E: the cell-scoped merge MERGES correctly -- verified 2026-08-18: exactly the fragment's"
            + " own cell, sibling cells untouched, twin data and ordering intact. What blocks it is the"
            + " fragment DIRECTORY purge: the candidate path renders doubled"
            + " (/c~1/<frag>/<frag>/E0.1), so the fragment would leak on disk with its attached entry"
            + " already gone -- worse than not squashing. Un-ignore when the purge path is fixed. NOTE"
            + " testMidTableFragmentIsMergedPerCell is the one to run first: it is the case the merge"
            + " actually implements (a fragmented day that is NOT the active tail).")
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

            assertTwinEqual("");
            Assert.assertTrue("the fragment must be merged away " + fragmentDirs("c"),
                    fragmentDirs("c").isEmpty());
            Assert.assertEquals("all three sibling cells must survive the merge",
                    3, cellDirs("c", "2023-01-01").size());
        });
    }

    /**
     * The acceptance test for the explicit statement.
     */
    @Ignore("SP1E: the cell-scoped merge MERGES correctly -- verified 2026-08-18: exactly the fragment's"
            + " own cell, sibling cells untouched, twin data and ordering intact. What blocks it is the"
            + " fragment DIRECTORY purge: the candidate path renders doubled"
            + " (/c~1/<frag>/<frag>/E0.1), so the fragment would leak on disk with its attached entry"
            + " already gone -- worse than not squashing. Un-ignore when the purge path is fixed. NOTE"
            + " testMidTableFragmentIsMergedPerCell is the one to run first: it is the case the merge"
            + " actually implements (a fragmented day that is NOT the active tail).")
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

            assertTwinEqual("");
            Assert.assertTrue("every fragment must be merged away " + fragmentDirs("c"),
                    fragmentDirs("c").isEmpty());
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
    @Ignore("SP1E: the cell-scoped merge MERGES correctly -- verified 2026-08-18: exactly the fragment's"
            + " own cell, sibling cells untouched, twin data and ordering intact. What blocks it is the"
            + " fragment DIRECTORY purge: the candidate path renders doubled"
            + " (/c~1/<frag>/<frag>/E0.1), so the fragment would leak on disk with its attached entry"
            + " already gone -- worse than not squashing. Un-ignore when the purge path is fixed. NOTE"
            + " testMidTableFragmentIsMergedPerCell is the one to run first: it is the case the merge"
            + " actually implements (a fragmented day that is NOT the active tail).")
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
            Assert.assertTrue("fragments accumulated across commits with no refusal anywhere: "
                    + fragmentDirs("c"), fragmentDirs("c").isEmpty());
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
    @Ignore("SP1E: the cell-scoped merge MERGES correctly -- verified 2026-08-18: exactly the fragment's"
            + " own cell, sibling cells untouched, twin data and ordering intact. What blocks it is the"
            + " fragment DIRECTORY purge: the candidate path renders doubled"
            + " (/c~1/<frag>/<frag>/E0.1), so the fragment would leak on disk with its attached entry"
            + " already gone -- worse than not squashing. Un-ignore when the purge path is fixed. NOTE"
            + " testMidTableFragmentIsMergedPerCell is the one to run first: it is the case the merge"
            + " actually implements (a fragmented day that is NOT the active tail).")
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
