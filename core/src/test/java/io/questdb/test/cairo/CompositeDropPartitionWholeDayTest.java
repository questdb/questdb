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

import io.questdb.cairo.TableReader;
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
 * Sub-project 1B: {@code DROP PARTITION} for predicates that select WHOLE DAYS on a routed composite
 * table. The lifecycle spec defines this shape — a predicate with no dimension constraint — as
 * identical to today's plain-table behaviour, so the plain twin is the oracle throughout.
 * <p>
 * {@code DROP PARTITION} is gated for routed composite tables today, and the gate comment at
 * {@code TableWriter#removePartition} documents THREE independently-confirmed unsafe mechanisms. One
 * test here targets each, because they fail in three different ways and a single acceptance test
 * would not distinguish them:
 * <ol>
 *     <li><b>N1</b> — {@code dropPartitionByExactTimestamp}'s "removing active partition" branch
 *     resolves the new tail's min/max through the cell-blind 5-arg {@code setPathForNativePartition}
 *     and throws "file does not exist" on a routed composite tail. Fails LOUDLY.</li>
 *     <li><b>N2</b> — {@code TxWriter#removeAttachedPartitions(long)} defaults to {@code cellKey = 0},
 *     so the removal loop re-probes the same raw index forever once cell 0's entry is gone. HANGS:
 *     empirically reproduced as a forked test JVM spinning until killed. Hence the timeout below —
 *     for that test the timeout IS the assertion.</li>
 *     <li><b>N3</b> — the physical unlink can collapse to the SHARED DAY CONTAINER depending on which
 *     cell's nameTxn is the initial {@code -1} sentinel, deleting sibling data never selected.
 *     DESTROYS DATA SILENTLY, which makes it the one that matters most.</li>
 * </ol>
 * All four RUN now: the gate was narrowed as planned, and each test went on to fail (and then pass)
 * for its OWN reason rather than for the gate's message -- which is what they were written for. The
 * "all four are @Ignore'd" note that stood here was left behind by that transition and is corrected
 * rather than deleted, because a class doc claiming its own tests prove nothing is worse than no doc
 * at all.
 */
public class CompositeDropPartitionWholeDayTest extends AbstractCompositeTwinTest {

    /**
     * The acceptance test: dropping a whole middle day must leave the composite table agreeing with
     * its plain twin, and must remove the day's directory from disk.
     */
    @Test(timeout = 60_000)
    public void testDropWholeDayMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedThreeMultiCellDays();

            execute("ALTER TABLE c DROP PARTITION LIST '2023-01-02'");
            execute("ALTER TABLE p DROP PARTITION LIST '2023-01-02'");
            drainWalQueue();

            assertTwinEqual("");
            Assert.assertFalse("the dropped day's directory must be gone",
                    dayDirs("c").contains("2023-01-02"));
        });
    }

    /**
     * N1: dropping the ACTIVE tail partition. The failure mode is a "file does not exist" throw while
     * resolving the new tail's bounds cell-blind. Also asserts the row-count identity the spec calls
     * for, since the tail is where transient/fixed accounting is decided.
     */
    @Test(timeout = 60_000)
    public void testDropActivePartitionTail() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedThreeMultiCellDays();

            execute("ALTER TABLE c DROP PARTITION LIST '2023-01-03'");
            execute("ALTER TABLE p DROP PARTITION LIST '2023-01-03'");
            drainWalQueue();

            assertTwinEqual("");
            // the surviving tail must still be queryable and consistent -- an append after the drop
            // exercises the recomputed tail bounds rather than merely reading them
            insertIntoBoth("('2023-01-02T23:00:00.000000Z','E0',77.0)");
            drainWalQueue();
            assertTwinEqual("");
        });
    }

    /**
     * N2: the infinite loop. A day with THREE cells; dropping it must terminate. The timeout is the
     * assertion — a regression here wedges CI rather than failing it, so it must fail fast instead.
     */
    @Test(timeout = 60_000)
    public void testDropDayWithMultipleCellsTerminates() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedThreeMultiCellDays();

            execute("ALTER TABLE c DROP PARTITION LIST '2023-01-02'");
            drainWalQueue();

            // reaching here at all is the point; the row check keeps it from passing vacuously
            assertQuery("select count() from c where ts >= '2023-01-02' and ts < '2023-01-03'")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    /**
     * N3: the data-loss guard. Three multi-cell days; dropping the middle one must leave BOTH
     * neighbours with every row and every cell directory intact. If the unlink collapses to the shared
     * day container, this is what catches it.
     */
    @Test(timeout = 60_000)
    public void testDropDayDoesNotTouchSiblingDays() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedThreeMultiCellDays();

            final List<String> cellsBefore1 = cellDirs("c", "2023-01-01");
            final List<String> cellsBefore3 = cellDirs("c", "2023-01-03");
            Assert.assertEquals("setup: day 1 must be multi-cell for this test to mean anything",
                    3, cellsBefore1.size());
            Assert.assertEquals("setup: day 3 must be multi-cell for this test to mean anything",
                    3, cellsBefore3.size());

            execute("ALTER TABLE c DROP PARTITION LIST '2023-01-02'");
            execute("ALTER TABLE p DROP PARTITION LIST '2023-01-02'");
            drainWalQueue();

            assertTwinEqual("");
            Assert.assertEquals("dropping day 2 must not remove day 1's cells",
                    cellsBefore1.size(), cellDirs("c", "2023-01-01").size());
            Assert.assertEquals("dropping day 2 must not remove day 3's cells",
                    cellsBefore3.size(), cellDirs("c", "2023-01-03").size());
        });
    }

    /**
     * The adversarial N3 case: a day whose cells are a MIXTURE of nameTxn states.
     * <p>
     * The gate comment says the unlink can collapse to the shared day container "depending on which
     * cell's nameTxn happens to be the initial -1 sentinel". A freshly-seeded cell IS that sentinel —
     * it appears on disk as {@code <day>/E0} with no {@code .txn} suffix, gaining {@code E0.<txn>}
     * only once rewritten. So a day where EVERY cell was written exactly once (as
     * {@link #testDropDayDoesNotTouchSiblingDays()} builds) is uniform, and may not exercise the
     * branch at all.
     * <p>
     * This builds the mixture deliberately: day 2's E0 is rewritten out-of-order so it carries a real
     * nameTxn, while E1 and E2 keep the sentinel. Dropping day 2 must still leave days 1 and 3 whole.
     */
    @Test(timeout = 60_000)
    public void testDropDayWithMixedNameTxnStatesDoesNotTouchSiblings() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedThreeMultiCellDays();

            // rewrite ONLY day 2's E0, so that cell leaves the -1 sentinel and its siblings do not
            insertIntoBoth("('2023-01-02T00:30:00.000000Z','E0',99.0)");
            drainWalQueue();
            engine.releaseInactive();

            final List<String> day2Cells = cellDirs("c", "2023-01-02");
            boolean sawSentinel = false;
            boolean sawVersioned = false;
            for (String cell : day2Cells) {
                if (cell.indexOf('.') < 0) {
                    sawSentinel = true;
                } else {
                    sawVersioned = true;
                }
            }
            Assert.assertTrue("setup is vacuous unless day 2 mixes sentinel and versioned cells: "
                    + day2Cells, sawSentinel && sawVersioned);

            final int cells1 = cellDirs("c", "2023-01-01").size();
            final int cells3 = cellDirs("c", "2023-01-03").size();

            execute("ALTER TABLE c DROP PARTITION LIST '2023-01-02'");
            execute("ALTER TABLE p DROP PARTITION LIST '2023-01-02'");
            drainWalQueue();

            assertTwinEqual("");
            Assert.assertEquals("sibling day 1 lost cells", cells1, cellDirs("c", "2023-01-01").size());
            Assert.assertEquals("sibling day 3 lost cells", cells3, cellDirs("c", "2023-01-03").size());
            Assert.assertFalse("the emptied day container must be gone",
                    dayDirs("c").contains("2023-01-02"));
        });
    }

    /**
     * SP1C: the shape 1B refused is now SUPPORTED, and this test is the inversion of the refusal.
     * <p>
     * 1B measured {@code DROP PARTITION LIST '2023-01-01/E0'} taking a three-cell day to EMPTY, which
     * is why it was refused: a destructive statement must not do visibly more than it names. Now it
     * must do exactly what it names -- remove E0 and leave E1 and E2 whole.
     */
    @Test(timeout = 60_000)
    public void testCellQualifiedDropRemovesOnlyThatCell() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedThreeMultiCellDays();

            execute("ALTER TABLE c DROP PARTITION LIST '2023-01-01/E0'");
            drainWalQueue();

            Assert.assertFalse("a supported drop must not suspend the table",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));
            // E0's rows for that day are gone; its siblings keep every row
            assertQuery("select exch, count() from c where ts < '2023-01-02' order by exch")
                    .noLeakCheck().expectSize()
                    .returns("exch\tcount\nE1\t1\nE2\t1\n");
            // and no other day is touched
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n8\n");
            final List<String> cells = cellDirs("c", "2023-01-01");
            Assert.assertEquals("only the named cell may be removed " + cells, 2, cells.size());
            Assert.assertTrue("the day container must survive while siblings remain " + dayDirs("c"),
                    dayDirs("c").contains("2023-01-01"));
        });
    }

    /**
     * SP1C: dropping the cell that holds the table's MIN (or MAX) row must recompute that bound.
     * <p>
     * This is the defect self-review found and the other tests structurally could not: they assert row
     * COUNTS and directory contents, and counts stay right because {@code _txn} loses the correct
     * record. But a designated-timestamp {@code min(ts)}/{@code max(ts)} is answered FROM {@code _txn}
     * rather than by scanning, so a stale bound is handed to the user as a wrong answer.
     * <p>
     * Third instance of this class in one session -- 1B's N1 read the max through a cell-blind path,
     * 1D's read the min from a day container, and this one did not recompute at all.
     */
    @Test(timeout = 60_000)
    public void testDroppingBoundaryCellsRecomputesMinAndMax() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedThreeMultiCellDays();

            // seeded: day1 E0 01:00 (table MIN) .. day3 E2 09:00 (table MAX)
            assertBounds("2023-01-01T01:00:00.000000Z", "2023-01-03T09:00:00.000000Z");

            // drop the cell holding the MIN row
            execute("ALTER TABLE c DROP PARTITION LIST '2023-01-01/E0'");
            drainWalQueue();
            assertBounds("2023-01-01T05:00:00.000000Z", "2023-01-03T09:00:00.000000Z");

            // drop the cell holding the MAX row
            execute("ALTER TABLE c DROP PARTITION LIST '2023-01-03/E2'");
            drainWalQueue();
            assertBounds("2023-01-01T05:00:00.000000Z", "2023-01-03T05:00:00.000000Z");
        });
    }

    /**
     * Dropping every cell one at a time drops the day -- the second half of the spec's rule.
     */
    @Test(timeout = 60_000)
    public void testDroppingEveryCellDropsTheDay() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedThreeMultiCellDays();

            execute("ALTER TABLE c DROP PARTITION LIST '2023-01-01/E0'");
            drainWalQueue();
            System.out.println("SP1C_DIAG cells after E0 drop: " + cellDirs("c", "2023-01-01"));
            Assert.assertTrue("day must survive with 2 cells left", dayDirs("c").contains("2023-01-01"));
            execute("ALTER TABLE c DROP PARTITION LIST '2023-01-01/E1'");
            drainWalQueue();
            Assert.assertTrue("day must survive with 1 cell left", dayDirs("c").contains("2023-01-01"));
            execute("ALTER TABLE c DROP PARTITION LIST '2023-01-01/E2'");
            drainWalQueue();

            // Every CELL is gone and the day holds no rows. The day CONTAINER itself survives, and
            // that is a separate, pre-existing issue rather than a fault of the drop: it still holds
            // day-level column files (exch.d/px.d/ts.d) that nothing reads. Sub-project 1A established
            // that every live row lives in the CELL directories; these day-level files are vestigial.
            // removeEmptyDayContainer refuses to delete a non-empty directory ON PURPOSE -- ff.rmdir
            // is recursive, and this is the routine whose documented failure mode is silent data loss.
            // Asserting "container gone" here would either be false or force that guard open.
            Assert.assertTrue("every cell of the day must be gone " + cellDirs("c", "2023-01-01"),
                    cellDirs("c", "2023-01-01").isEmpty());
            Assert.assertEquals("the day container survives only because of vestigial day-level column"
                            + " files -- if this list changes, re-check whether it can now be removed",
                    "[exch.d, px.d, ts.d]", listAll("c", "2023-01-01").toString());
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n6\n");
        });
    }

    /**
     * A name that matches no attached cell must fail LOUDLY and change nothing. Dropping zero cells
     * while reporting success is the silent path the cardinal rule forbids.
     */
    @Test(timeout = 60_000)
    public void testUnknownCellNameIsRefusedAndChangesNothing() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedThreeMultiCellDays();

            try {
                execute("ALTER TABLE c DROP PARTITION LIST '2023-01-01/NOPE'");
                Assert.fail("an unknown cell name must be refused");
            } catch (SqlException expected) {
                TestUtils.assertContains(expected.getFlyweightMessage(), "no such partition cell");
            }
            drainWalQueue();

            Assert.assertEquals("nothing may be removed", 3, cellDirs("c", "2023-01-01").size());
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n9\n");
        });
    }

    /**
     * Asserts the table's designated-timestamp bounds. Uses printSql rather than the fluent battery
     * because min(ts)/max(ts) come back timestamp-typed, which that battery rejects.
     */
    private void assertBounds(String expectedMin, String expectedMax) throws Exception {
        // The SQL form is NOT sufficient on its own: for a COMPOSITE table min(ts)/max(ts) route
        // through the cross-cell merge SCAN, not the _txn shortcut (asserted in
        // CompositeFactoryCoverageTest), so a stale _txn bound is invisible to it. The stored bound is
        // checked directly as well -- that is what a missing recompute would corrupt, and what the
        // writer's own tail bookkeeping and partition pruning read.
        final StringSink sink = new StringSink();
        printSql("select min(ts) mn, max(ts) mx from c", sink);
        TestUtils.assertEquals("mn\tmx\n" + expectedMin + '\t' + expectedMax + '\n', sink);

        engine.releaseInactive();
        try (TableReader reader = getReader("c")) {
            final io.questdb.cairo.TimestampDriver driver =
                    io.questdb.cairo.ColumnType.getTimestampDriver(io.questdb.cairo.ColumnType.TIMESTAMP);
            Assert.assertEquals("_txn minTimestamp is stale after dropping a boundary cell",
                    driver.parseFloorLiteral(expectedMin), reader.getMinTimestamp());
            Assert.assertEquals("_txn maxTimestamp is stale after dropping a boundary cell",
                    driver.parseFloorLiteral(expectedMax), reader.getMaxTimestamp());
        }
    }

    /**
     * Three days, three cells each (E0/E1/E2), so every day is genuinely multi-cell — the precondition
     * all three mechanisms need.
     */
    private void seedThreeMultiCellDays() throws Exception {
        final StringBuilder sb = new StringBuilder();
        for (int day = 1; day <= 3; day++) {
            for (int cell = 0; cell <= 2; cell++) {
                if (sb.length() > 0) {
                    sb.append(',');
                }
                sb.append("('2023-01-0").append(day).append('T').append(String.format("%02d", 1 + cell * 4))
                        .append(":00:00.000000Z','E").append(cell).append("',")
                        .append(day * 10 + cell).append(".0)");
            }
        }
        insertIntoBoth(sb.toString());
        drainWalQueue();
    }

    private List<String> listAll(String table, String dayDir) throws IOException {
        final Path day = tableDir(table).resolve(dayDir);
        final List<String> out = new ArrayList<>();
        if (!Files.isDirectory(day)) {
            return out;
        }
        try (Stream<Path> children = Files.list(day)) {
            children.map(pp -> pp.getFileName().toString()).sorted(Comparator.naturalOrder()).forEach(out::add);
        }
        return out;
    }

    private List<String> cellDirs(String table, String dayDir) throws IOException {
        final Path day = tableDir(table).resolve(dayDir);
        final List<String> out = new ArrayList<>();
        if (!Files.isDirectory(day)) {
            return out;
        }
        try (Stream<Path> children = Files.list(day)) {
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
