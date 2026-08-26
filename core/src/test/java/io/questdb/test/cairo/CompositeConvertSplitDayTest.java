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
import org.junit.Assert;
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
 * What {@code CONVERT PARTITION TO PARQUET} does to a day that has already SPLIT.
 * <p>
 * This is the question upstream of the squash refusal added in
 * {@code squashSplitPartitionsComposite_mergeFragment}. That refusal stops a native fragment being
 * merged into a parquet cell -- which destroys the parquet footer and makes the partition unreadable --
 * but it is a late catch: it fires only once someone squashes. What CREATES the hazardous shape is
 * converting a split day, leaving a parquet parent beside a native fragment.
 * <p>
 * Before changing CONVERT, establish what it currently does, and in particular what the PLAIN twin
 * does. The plain table is the oracle for the whole branch: if plain also leaves a mixed
 * parquet-parent/native-fragment day, then the mixed shape is ordinary QuestDB behaviour and refusing
 * at squash (which the plain squash loop also does) is the consistent answer, and CONVERT needs no
 * change. If plain instead squashes first or refuses, composite should match it.
 * <p>
 * These tests ASSERT the current behaviour rather than a desired one, so they are a characterisation
 * of the status quo. Each states in its own message what a failure would mean.
 */
public class CompositeConvertSplitDayTest extends AbstractCompositeTwinTest {

    /**
     * The composite table's behaviour, stated against the plain twin's.
     * <p>
     * Whatever the twins do, the ROWS must agree afterwards -- CONVERT is a change of storage format,
     * not of contents, so any divergence here is a straight data bug.
     */
    @Test(timeout = 60_000)
    public void testConvertOnASplitDayAgreesWithThePlainTwin() throws Exception {
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createTwins();
            seedSplittableDay();
            forceSplit();
            Assert.assertFalse("precondition: c must have split", fragmentDirs("c").isEmpty());
            // NOTE: p does NOT split under this fixture -- measured, it rewrites the day instead
            // (its directory becomes 2023-01-01.1, a fresh name-txn, with no fragment). So the twins
            // are compared on ROWS only; their physical shapes legitimately differ. See the class
            // javadoc.

            execute("ALTER TABLE c CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            drainWalQueue();
            engine.releaseInactive();

            // The contents must survive the conversion on both sides.
            assertTwinEqual("");
        });
    }

    /**
     * CONVERT must consolidate a split day BEFORE converting it, as the plain path already does.
     * <p>
     * {@code convertPartitionNativeToParquet} calls {@code squashPartitionForce(partitionIndex)} as its
     * first mutating step, so on a plain table a split day is merged and then converted as one
     * partition. The composite branch returned before reaching that, and its cell collection matches on
     * the EXACT partition timestamp -- so the fragment, which shares the day's floor but has its own raw
     * timestamp, was simply not collected. It stayed native next to the freshly written parquet cells.
     * <p>
     * That mixed day is the shape that later destroys the parquet footer when someone squashes, which
     * squashSplitPartitionsComposite_mergeFragment now has to refuse. Consolidating here removes the
     * shape at its source instead of catching it late.
     * <p>
     * <b>The cell-count assertion is the one that matters.</b> A squash change can relocate every row
     * of a day into a single cell and still pass a row-level twin comparison -- all the rows survive,
     * count() agrees, and only the cell structure is gone. A green data-level assertion is NOT evidence
     * that a squash change is safe here.
     */
    @Test(timeout = 60_000)
    public void testConvertSquashesTheDayFirstLikeThePlainPath() throws Exception {
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createTwins();
            seedSplittableDay();
            forceSplit();
            Assert.assertFalse("precondition: the day must have split", fragmentDirs("c").isEmpty());

            // Two cells before conversion: E0 and E1. The '2023-01-01/%' pattern deliberately excludes
            // the FRAGMENT's cells, whose names are '2023-01-01T010000-000001/E0' -- counting those too
            // compares three-before against two-after and fails on a correct consolidation.
            printSql("SELECT count() FROM table_partitions('c') WHERE name LIKE '2023-01-01/%'");
            final String cellsBefore = sink.toString();

            execute("ALTER TABLE c CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            drainWalQueue();
            engine.releaseInactive();

            // The ATTACHED PARTITION LIST is the oracle here, not the directory listing. A merge only
            // queues the fragment's directory for purge; the directory survives until a later commit
            // reclaims it, so an on-disk check reports a correctly consolidated day as still split.
            printSql("SELECT name FROM table_partitions('c') WHERE name LIKE '%T%'");
            Assert.assertEquals(
                    "no split fragment may remain ATTACHED after the conversion: left attached it is a "
                            + "native fragment beside parquet cells, the shape that destroys the parquet "
                            + "footer on a later squash",
                    "name\n", sink.toString());

            // STRUCTURAL: the day must still have its cells. A merge that swallowed sibling cells into
            // one target keeps every row -- so this, not the row comparison, is what catches it.
            printSql("SELECT count() FROM table_partitions('c') WHERE name LIKE '2023-01-01/%'");
            Assert.assertEquals(
                    "the day's CELL COUNT must survive the squash-then-convert; a drop here means the "
                            + "merge swallowed sibling cells, which no row-level assertion can see",
                    cellsBefore, sink.toString());

            // And the rows themselves.
            assertTwinEqual("");

            // The table must keep working after the conversion.
            insertIntoBoth("('2023-01-04T00:00:00.000000Z','E0',7.0)");
            drainWalQueue();
            engine.releaseInactive();
            assertTwinEqual("");

            // KNOWN RESIDUAL, deliberately not asserted: the merged fragment's DIRECTORY is not
            // reclaimed by the following commit -- measured, 2023-01-01T010000-000001 survives it.
            // Squashing before the conversion detaches the fragment, and the subsequent purge does not
            // remove its directory once the day has become parquet; an explicit squash on a NATIVE day
            // does get reclaimed (see CompositeSquashTest). This is the orphan-directory class the
            // branch already documents as non-corrupting: the entry is gone from the attached list, the
            // rows are correct, and nothing reads the stale directory. Asserting it here would make
            // this test red for a storage-reclaim gap rather than for the consolidation behaviour it
            // exists to check. Worth fixing separately -- it wastes disk until the table is rebuilt.
            Assert.assertFalse(
                    "sanity: the leaked directory should still be OFF the attached list -- if it is "
                            + "attached again, this is no longer a mere reclaim gap",
                    tableHasAttachedFragment());
        });
    }

    /**
     * The composite table SPLITS where the plain table REWRITES -- measured, and the reason the twins'
     * physical shapes cannot be compared directly.
     * <p>
     * After the same O3 write into an already-written day, with the same split threshold:
     * <pre>
     *   c dayDirs = [2023-01-01, 2023-01-01T010000-000001]   parent + fragment
     *   p dayDirs = [2023-01-01.1]                            rewritten, no fragment
     * </pre>
     * So the mixed parquet-parent/native-fragment day is not a shape a plain table reaches under these
     * statements, and "match the plain twin's layout" is not an available oracle for it. What IS
     * available, and what the fix follows, is the plain CONVERT's own behaviour: it force-squashes the
     * day before converting.
     * <p>
     * Pinned so that if composite ever stops splitting here -- or plain starts -- the change is noticed
     * rather than silently making the other tests in this class vacuous.
     */
    @Test(timeout = 60_000)
    public void testCompositeSplitsWhereThePlainTwinRewrites() throws Exception {
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createTwins();
            seedSplittableDay();
            forceSplit();

            Assert.assertFalse(
                    "composite is expected to split this day; if it no longer does, the split-day tests "
                            + "in this class are vacuous. c dirs=" + dayDirs("c"),
                    fragmentDirs("c").isEmpty());
            Assert.assertTrue(
                    "the plain twin is expected NOT to split this day -- it rewrites it. If it now does "
                            + "split, the twins' shapes became comparable and these tests should be "
                            + "restated against the plain layout. p dirs=" + dayDirs("p"),
                    fragmentDirs("p").isEmpty());
        });
    }

    /**
     * The end-to-end consequence: convert a split day, then squash it.
     * <p>
     * On the composite table the squash is now REFUSED (merging a native fragment into a parquet cell
     * destroys the footer). This test states what the user is left with -- the data must remain intact
     * and readable either way. It is the invariant that matters regardless of which fix CONVERT
     * eventually grows.
     */
    @Test(timeout = 60_000)
    public void testConvertThenSquashLeavesTheDataIntactOnBothTwins() throws Exception {
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createTwins();
            seedSplittableDay();
            forceSplit();
            Assert.assertFalse("precondition: the day must have split", fragmentDirs("c").isEmpty());

            execute("ALTER TABLE c CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            drainWalQueue();
            assertTwinEqual("");

            execute("ALTER TABLE c SQUASH PARTITIONS");
            execute("ALTER TABLE p SQUASH PARTITIONS");
            drainWalQueue();
            engine.releaseInactive();

            // Reading the ROWS, not count(): a bad merge updates _txn while leaving the parquet file
            // behind, so a count-only oracle reports a destroyed partition as correct.
            assertTwinEqual("");
        });
    }

    /**
     * Is any split fragment still on the ATTACHED partition list? Distinct from a fragment DIRECTORY
     * surviving on disk, which can be a mere reclaim gap.
     */
    private boolean tableHasAttachedFragment() throws SqlException {
        printSql("SELECT count() FROM table_partitions('c') WHERE name LIKE '%T%'");
        return !sink.toString().contains("\n0\n");
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

    private void seedSplittableDay() throws SqlException {
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
