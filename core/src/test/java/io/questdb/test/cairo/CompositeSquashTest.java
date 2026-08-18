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
     * The acceptance test for the explicit statement. {@code @Ignore}d until sub-project 1E makes the
     * merge cell-aware; today {@code SQUASH PARTITIONS} is refused at the statement.
     */
    @Ignore("SP1E: SQUASH PARTITIONS is refused for composite tables. Un-ignore when the merge is"
            + " cell-aware. NOTE this test also unblocks DETACH, which suspends on the SQUASH gate.")
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
    @Ignore("SP1E: the automatic split-fragment squash is a SILENT SKIP for composite tables.")
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
