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
 * Does the O3 partition-split heuristic need a COMPOSITE-SPECIFIC threshold?
 * <p>
 * The heuristic weighs {@code prefixHi} against {@code getPartitionO3SplitThreshold()} (an absolute
 * row count derived from {@code cairo.o3.partition.split.min.size}) and against twice the merged-row
 * count. On a composite table the partition it is judging is a CELL, holding a fraction of the day's
 * rows, so the same absolute threshold is being applied to a smaller unit.
 * <p>
 * That asymmetry is REAL but not obviously wrong: the cell is also the unit that would be rewritten,
 * so judging the split on the cell is arguably the correct accounting. This survey exists to answer it
 * with measurements rather than argument, across several workload shapes, now that the merged-row
 * count underflow is fixed (see {@code O3PartitionJob#o3MergedRowCount} -- before that fix composite
 * split on EVERY O3 write and no threshold discussion was meaningful).
 * <p>
 * The invariant asserted throughout is PARITY: for the same statements, a composite table must not
 * accumulate more physical split fragments than its plain twin, and must hold the same rows. If a
 * shape ever violates that, this suite names it and a composite-specific threshold is justified.
 */
public class CompositeSplitHeuristicSurveyTest extends AbstractCompositeTwinTest {

    /**
     * At the DEFAULT threshold (cairo.o3.partition.split.min.size = 50MB) a table of this size never
     * splits -- neither twin. Asserted rather than assumed, because the first version of this survey
     * ran its parity check at the default and passed VACUOUSLY: composite=[] plain=[], no splits on
     * either side, so "composite is not worse" was true of nothing.
     * <p>
     * This is also the answer for ordinary users: at stock settings the composite/plain split
     * difference does not arise at all until a partition reaches 50MB.
     */
    @Test(timeout = 120_000)
    public void testDefaultThresholdSplitsNothingAtThisScale() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedAndO3("default-scale", 3);
            Assert.assertTrue("at the 50MB default neither twin should split at this data size."
                    + " composite=" + fragmentDirs("c"), fragmentDirs("c").isEmpty());
            Assert.assertTrue("at the 50MB default neither twin should split at this data size."
                    + " plain=" + fragmentDirs("p"), fragmentDirs("p").isEmpty());
            assertTwinEqual("");
        });
    }

    /**
     * A MODERATE threshold: small enough to be reachable here, large enough that the absolute check
     * still participates -- unlike split.min.size = 1, which removes it entirely and leaves only the
     * relative test. This is the setting where a composite-specific threshold would show up if one
     * were needed, because a 3-cell day gives each cell a third of the rows to clear the same bar.
     */
    @Test(timeout = 120_000)
    public void testModerateThresholdMultiCellO3() throws Exception {
        assertMemoryLeak(() -> runO3Rounds("multi-cell/moderate", 256, 3));
    }

    /**
     * The same moderate threshold with every O3 write landing in ONE cell, which isolates "a cell is
     * judged differently from a day" from the cell-count effect above.
     */
    @Test(timeout = 120_000)
    public void testModerateThresholdSingleCellO3() throws Exception {
        assertMemoryLeak(() -> runO3Rounds("single-cell/moderate", 256, 1));
    }

    /**
     * The aggressive setting the rest of the composite suite uses (split.min.size = 1), which removes
     * the absolute threshold and leaves only the relative one. Non-vacuous: splits DO occur here.
     */
    @Test(timeout = 120_000)
    public void testAggressiveThresholdMultiCellO3() throws Exception {
        assertMemoryLeak(() -> runO3Rounds("multi-cell/aggressive", 1, 3));
    }

    /**
     * Runs O3 rounds into {@code cellCount} cells of one day and asserts fragment PARITY plus row
     * equality against the plain twin.
     */
    private void runO3Rounds(String label, int splitMinSize, int cellCount) throws Exception {
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, splitMinSize);
        createTwins();

        seedAndO3(label, cellCount);
        assertParity(label);
    }

    /**
     * Seeds a 12-row day spread over {@code cellCount} cells, then issues six O3 writes into the
     * middle of it, cycling cells.
     */
    private void seedAndO3(String label, int cellCount) throws Exception {
        // A day with enough rows per cell that a split is a real decision rather than a degenerate one.
        final StringBuilder seed = new StringBuilder();
        for (int i = 0; i < 12; i++) {
            if (seed.length() > 0) {
                seed.append(',');
            }
            seed.append("('2023-01-01T")
                    .append(String.format("%02d", i))
                    .append(":00:00.000000Z','E").append(i % cellCount).append("',").append(i + 1.0).append(')');
        }
        insertIntoBoth(seed.toString());
        drainWalQueue();
        engine.releaseInactive();

        // O3 writes into the middle of that day, one per round, cycling cells.
        for (int round = 0; round < 6; round++) {
            insertIntoBoth("('2023-01-01T" + String.format("%02d", 3 + round) + ":30:00.000000Z','E"
                    + (round % cellCount) + "'," + (100 + round) + ".0)");
            drainWalQueue();
            engine.releaseInactive();
        }

    }

    private void assertParity(String label) throws Exception {
        assertTwinEqual("");

        final List<String> compositeFragments = fragmentDirs("c");
        final List<String> plainFragments = fragmentDirs("p");
        // NON-VACUITY: parity between two tables that both split nothing proves nothing. The first
        // version of this survey ran at the 50MB default and did exactly that.
        Assert.assertFalse(
                label + ": neither twin split, so the parity assertion below would be vacuous."
                        + " Lower the threshold or add rows.",
                compositeFragments.isEmpty() && plainFragments.isEmpty());
        Assert.assertTrue(
                label + ": composite accumulated MORE split fragments than the plain twin, which is what"
                        + " a composite-specific split threshold would exist to fix. composite="
                        + compositeFragments + ", plain=" + plainFragments,
                compositeFragments.size() <= plainFragments.size());
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

    private List<String> fragmentDirs(String table) throws IOException {
        final List<String> out = new ArrayList<>();
        for (String d : dayDirs(table)) {
            if (d.indexOf('T') > 0) {
                out.add(d);
            }
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
