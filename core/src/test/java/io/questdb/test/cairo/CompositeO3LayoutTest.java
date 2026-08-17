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

import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.test.AbstractCairoTest;
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
 * CHARACTERISATION of the composite on-disk layout under out-of-order writes. Green from the start by
 * design — it asserts what today's build already does, so that sub-project 1A's fix is designed
 * against a measured layout rather than an inferred one.
 * <p>
 * It exists because inference got this wrong twice. {@code nameTxn} is stored per {@code (ts, cellKey)}
 * in {@code _txn}, which suggests {@code <day>.<txn>} directories should hold per-cell subsets; they do
 * not. And a short probe without the purge job running suggested day-level version directories never
 * accumulate at all; they do. Both readings would have produced a wrong fix.
 * <p>
 * The four facts pinned here, each of which sub-project 1A depends on:
 * <ol>
 *     <li>a composite table accumulates several {@code <day>[.<txn>]} directories under its root while
 *     its plain twin keeps exactly one — the leak itself;</li>
 *     <li>the composite table's LIVE cells sit under the <b>unversioned</b> day directory. This is the
 *     data-loss trap: a fix that keeps "the newest {@code <day>.<txn>}" and deletes the older entries
 *     would delete the unversioned directory, i.e. every live cell;</li>
 *     <li>the {@code <day>.<txn>} directories hold day-level column files and NO cell subdirectories;</li>
 *     <li>every live row is accounted for in the cell directories, so the day-level directories hold
 *     nothing live. This is the assertion that licenses deleting them.</li>
 * </ol>
 * If this test fails, sub-project 1A's premises are void — fix the plan, not the test.
 */
public class CompositeO3LayoutTest extends AbstractCairoTest {

    @Test
    public void testCompositeO3LayoutIsCellVersionedUnderAnUnversionedDay() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            for (String t : new String[]{"c", "p"}) {
                execute("INSERT INTO " + t + " VALUES ('2023-01-02T01:00:00.000000Z','E0',1.0),"
                        + "('2023-01-02T05:00:00.000000Z','E1',5.0)");
            }
            drainWalQueue();

            // Same churn as CompositeO3PurgeSkipTest, so both describe one workload: 20 rounds landing
            // inside the already-written day, most of them genuinely out-of-order.
            final O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine, 1);
            try {
                for (int round = 1; round <= 20; round++) {
                    for (String t : new String[]{"c", "p"}) {
                        execute("INSERT INTO " + t + " VALUES ('2023-01-02T0" + (round % 6) + ":30:00.000000Z','E"
                                + (round % 3) + "'," + round + ".0)");
                    }
                    drainWalQueue();
                    engine.releaseInactive();
                    purgeJob.drain(0);
                }
                drainWalQueue();
                engine.releaseInactive();
                purgeJob.drain(0);
            } finally {
                purgeJob.close();
            }

            // (1) the leak: composite accumulates day-level version dirs, the plain twin does not
            final List<String> compositeDayDirs = dayDirs("c");
            final List<String> plainDayDirs = dayDirs("p");
            Assert.assertEquals("control: the plain twin must reclaim down to a single day directory,"
                            + " otherwise this workload does not exercise purge at all " + plainDayDirs,
                    1, plainDayDirs.size());
            Assert.assertTrue("composite is expected to LEAK day-level version directories on today's"
                            + " build; if this now passes, 1A may already be fixed " + compositeDayDirs,
                    compositeDayDirs.size() > 1);

            // (2) THE TRAP: live cells sit under the UNVERSIONED day directory
            Assert.assertTrue("the unversioned day directory must exist " + compositeDayDirs,
                    compositeDayDirs.contains("2023-01-02"));
            final List<String> cells = cellDirs("c", "2023-01-02");
            Assert.assertEquals("expected one live cell per distinct exch value " + cells, 3, cells.size());
            for (String cell : cells) {
                Assert.assertTrue("a live cell directory is <value>.<txn>, got " + cell,
                        cell.matches("E[0-2]\\.\\d+"));
            }

            // (3) the versioned day dirs are day-level: column files, no cells
            for (String dayDir : compositeDayDirs) {
                if ("2023-01-02".equals(dayDir)) {
                    continue;
                }
                Assert.assertTrue(dayDir + " should be a day-level version directory holding column files",
                        Files.exists(tableDir("c").resolve(dayDir).resolve("ts.d")));
                Assert.assertTrue(dayDir + " must not contain cell subdirectories",
                        cellDirs("c", dayDir).isEmpty());
            }

            // (4) every live row is in the cells, so the day-level dirs hold nothing live
            long rowsInCells = 0;
            for (String cell : cells) {
                rowsInCells += Files.size(tableDir("c").resolve("2023-01-02").resolve(cell).resolve("ts.d")) / Long.BYTES;
            }
            Assert.assertEquals("rows reachable by SQL must all live in the CELL directories -- if this"
                            + " fails, the day-level directories hold live data and must NOT be deleted",
                    22L, rowsInCells);
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n22\n");
        });
    }

    /**
     * Cell subdirectories of one day directory, e.g. {@code E0.18}.
     */
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

    /**
     * Directories directly under the table root that look like {@code <day>} or {@code <day>.<txn>}.
     */
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
