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

            // KNOWN LEAK, pinned separately by testMergedFragmentDirectoryIsLeaked below rather than
            // asserted here, so this test stays about consolidation. The merged fragment's directory
            // survives; see that test for the measurements.
            Assert.assertFalse(
                    "sanity: the leaked directory should still be OFF the attached list -- if it is "
                            + "attached again, this is no longer a mere reclaim gap",
                    tableHasAttachedFragment());
        });
    }

    /**
     * Both twins now SPLIT this day the same way -- which is the parity the split-heuristic fix was
     * after.
     * <p>
     * This test used to assert the OPPOSITE, and the reason it flipped is worth keeping. With the old
     * one-row prefix in the seed, composite split and the plain twin merely rewrote:
     * <pre>
     *   c dayDirs = [2023-01-01, 2023-01-01T010000-000001]   parent + fragment
     *   p dayDirs = [2023-01-01.1]                            rewritten, no fragment
     * </pre>
     * That asymmetry was an ARTIFACT, not a property of composite tables: the merged-row count
     * underflowed negative, so composite's "prefixHi > 2 * merged" was vacuously true and it split on a
     * prefix the plain table correctly declined to split. With the clamp in place
     * ({@code O3PartitionJob#o3MergedRowCount}) and a seed that provides a genuine two-row prefix, both
     * twins take the same decision.
     * <p>
     * Kept as a pin on that parity: if the two ever diverge again, the other tests in this class -- all
     * of which compare composite against the plain twin -- start measuring different physical shapes
     * without saying so.
     */
    @Test(timeout = 60_000)
    public void testBothTwinsSplitTheDayAlike() throws Exception {
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createTwins();
            seedSplittableDay();
            forceSplit();

            Assert.assertFalse(
                    "composite is expected to split this day; if it no longer does, the split-day tests "
                            + "in this class are vacuous. c dirs=" + dayDirs("c"),
                    fragmentDirs("c").isEmpty());
            Assert.assertFalse(
                    "the plain twin must split it too -- the two took the same decision once the split "
                            + "heuristic stopped comparing against a negative merged-row count. p dirs="
                            + dayDirs("p"),
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

    /**
     * The merged fragment's directory must be reclaimed. FIXED -- it used to leak forever.
     * <p>
     * After a squash merges a fragment into the day's cells the fragment is correctly detached from the
     * attached-partition list, but {@code 2023-01-01T010000-000001/E0.1} used to stay on disk
     * permanently, growing with every split that got squashed.
     * <p>
     * <b>The cause was a per-block reset discarding an undrained purge candidate.</b>
     * {@code processO3BlockComposite} opened with {@code partitionRemoveCandidates.clear()}, copied
     * from {@code processO3BlockPlain} where it is harmless: on a plain table the preceding statement's
     * commit has already drained the list, so the reset only ever clears an empty list. On a composite
     * table the squash queues the fragment's directory and does NOT commit before the next O3 block, so
     * that reset destroyed the candidate. The fix simply stops discarding them; {@code housekeep()}
     * then drains after {@code commit00()}, which is the post-commit point the merge's own
     * "NO eager removeEmptyDayContainer here" comment requires -- the detachment must be durable before
     * the directory goes. An aborted commit still drops them, because {@code rollback()} clears.
     * <p>
     * Four explanations were ruled out by measurement before the real one was found, and they are kept
     * because each is a plausible place to look again:
     * <ul>
     *   <li>NOT specific to parquet or to CONVERT. An explicit {@code SQUASH PARTITIONS} on a purely
     *       NATIVE day leaks identically. An earlier revision of this file claimed the native case was
     *       reclaimed; that was wrong, and this pin replaces it.</li>
     *   <li>NOT merely deferred to the async purge. Running {@code O3PartitionPurgeJob} to exhaustion
     *       after releasing all readers and writers leaves the directory in place.</li>
     *   <li>NOT an empty-container remnant: the directory still holds the cell {@code E0.1}.</li>
     *   <li>NOT a wrong enqueue: the merge queues {@code (fragTs, srcNameTxn, cellKey)}, the fragment's
     *       own timestamp, unconditionally right after {@code removeAttachedPartitions}. All 18
     *       {@code partitionRemoveCandidates.add} sites in TableWriter are 3-arg, so the triples the
     *       drain reads with {@code i += 3} are not desynchronised either.</li>
     *   <li>NOT a path-rendering difference: the 5-arg and 6-arg {@code setSinkForNativePartition}
     *       overloads share one {@code PartitionBy.setSinkForPartition} call, so the cell-aware form
     *       does not drop the split timestamp's {@code T010000-000001} component.</li>
     * </ul>
     * <p>
     * <b>LOCALISED by instrumenting the drain.</b> Logging every candidate at the top of
     * {@code processPartitionRemoveCandidates0} shows only TWO non-empty drains in the whole test, and
     * the only one for {@code c} carries a different candidate entirely:
     * <pre>
     *   INSTR-DRAIN     n=3, anyReaders=false, ckpt=false
     *   INSTR-CANDIDATE path=.../c~1/2023-01-01/E0, ts=2023-01-01T00:00:00, txn=-1, cellKey=0
     * </pre>
     * The fragment's candidate NEVER REACHES A DRAIN. It is queued by the squash, no drain follows the
     * squash's commit, and the next insert's {@code processO3BlockComposite} calls
     * {@code partitionRemoveCandidates.clear()} (TableWriter ~13639) as part of its per-block reset --
     * discarding it. So the leak is a missing drain, not a bad path or a bad candidate.
     * <p>
     * <b>The obvious fix is unsafe -- do not take it.</b> Draining at the end of
     * {@code squashPartitions()} would delete the fragment directory BEFORE the transaction recording
     * its detachment is durable, which is exactly the hazard the merge documents at its own
     * "NO eager removeEmptyDayContainer here" comment: a reload then sees the txn still listing
     * {@code <fragment>/<cell>} while the directory is gone. The fix needs a drain at a point that is
     * already past the commit -- i.e. reaching {@code housekeep()}'s
     * {@code processPartitionRemoveCandidates()} on the composite squash's commit path -- or a
     * per-block reset in {@code processO3BlockComposite} that does not discard undrained candidates.
     * <p>
     * The test also asserts the fragment is not ATTACHED, so a regression from "reclaimed" to "still
     * attached" -- a far worse bug than the leak -- fails distinctly rather than looking the same.
     */
    @Test(timeout = 60_000)
    public void testMergedFragmentDirectoryIsReclaimed() throws Exception {
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        assertMemoryLeak(() -> {
            createTwins();
            seedSplittableDay();
            forceSplit();
            Assert.assertFalse("precondition: the day must have split", fragmentDirs("c").isEmpty());

            execute("ALTER TABLE c SQUASH PARTITIONS");
            drainWalQueue();
            insertIntoBoth("('2023-01-04T00:00:00.000000Z','E0',7.0)");
            drainWalQueue();
            engine.releaseInactive();

            // The squash itself is correct: nothing fragmented remains ATTACHED, and the rows agree.
            Assert.assertFalse(
                    "the fragment must not remain attached -- that would be a different, worse bug",
                    tableHasAttachedFragment());
            assertTwinEqual("");

            // ... and its directory is reclaimed rather than leaked.
            Assert.assertTrue(
                    "the merged fragment's directory must be purged once the commit makes its detachment "
                            + "durable. Remaining: " + fragmentDirs("c"),
                    fragmentDirs("c").isEmpty());
        });
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
     * The 02:00 row gives cell E0 a TWO-row prefix before {@link #forceSplit()}'s 10:00 write. The split
     * heuristic requires the prefix to exceed twice the merged-row count, so with an empty merge that
     * means 2+ rows. A one-row prefix used to split anyway, but only because the merged-row count
     * underflowed NEGATIVE and made the comparison vacuous -- see O3PartitionJob#o3MergedRowCount.
     */
    private void seedSplittableDay() throws SqlException {
        insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',1.0),"
                + "('2023-01-01T02:00:00.000000Z','E0',1.5),"
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
