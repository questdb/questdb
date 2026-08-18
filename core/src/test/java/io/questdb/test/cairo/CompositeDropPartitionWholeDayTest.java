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
 * All four are {@code @Ignore}d until the gate is narrowed in Task 5 — the same pattern
 * {@link CompositeO3PurgeSkipTest} followed from wave 0 until sub-project 1A fixed it. While ignored
 * they fail with the gate's own message, which proves the gate is present but says nothing about the
 * mechanisms; each is written so that it keeps failing for its OWN reason once the gate is lifted.
 */
public class CompositeDropPartitionWholeDayTest extends AbstractCompositeTwinTest {

    /**
     * The acceptance test: dropping a whole middle day must leave the composite table agreeing with
     * its plain twin, and must remove the day's directory from disk.
     */
    @Ignore("Sub-project 1B: DROP PARTITION is gated for routed composite tables. Un-ignore in Task 5,"
            + " once N1/N2/N3 are fixed and the gate is narrowed to dimension-constrained predicates.")
    @Test
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
    @Ignore("Sub-project 1B: see the class javadoc. Targets N1.")
    @Test
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
    @Ignore("Sub-project 1B: see the class javadoc. Targets N2.")
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
    @Ignore("Sub-project 1B: see the class javadoc. Targets N3 -- the silent data-loss mechanism.")
    @Test
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
