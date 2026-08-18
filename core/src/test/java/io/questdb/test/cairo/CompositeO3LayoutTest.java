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
 * The composite on-disk layout under out-of-order writes. Written first as a CHARACTERISATION test —
 * green against the pre-fix build, asserting that composite leaked day-level version directories —
 * so that sub-project 1A's fix was designed against a measured layout rather than an inferred one.
 * Flipped to the fixed contract once the fix landed: assertion (1) went from "composite accumulates
 * more than one" to "composite keeps exactly one, like plain".
 * <p>
 * It exists because inference got this wrong repeatedly. {@code nameTxn} is stored per {@code (ts, cellKey)}
 * in {@code _txn}, which suggests {@code <day>.<txn>} directories should hold per-cell subsets; they do
 * not. And a short probe without the purge job running suggested day-level version directories never
 * accumulate at all; they do. Both readings would have produced a wrong fix.
 * <p>
 * The four facts pinned here, each of which sub-project 1A depends on:
 * <ol>
 *     <li>a composite table keeps exactly ONE day directory, like its plain twin. Before sub-project
 *     1A it accumulated one extra {@code <day>.<txn>} per commit that bumped cellKey 0's nameTxn;</li>
 *     <li>the composite table's LIVE cells sit under the <b>unversioned</b> day directory. This is the
 *     data-loss trap: a fix that keeps "the newest {@code <day>.<txn>}" and deletes the older entries
 *     would delete the unversioned directory, i.e. every live cell;</li>
 *     <li>no VERSIONED day directory survives — that was the leaked artifact;</li>
 *     <li>every live row is accounted for in the cell directories, so the day-level directories hold
 *     nothing live. This is the assertion that licenses deleting them.</li>
 * </ol>
 * Fixed by sub-project 1A: {@code openLastPartitionAndSetAppendPosition} no longer opens a day-level
 * "last partition" for a routed composite table. {@code openPartition} resolved that path with the
 * cell-blind {@code setStateForTimestamp} and then called {@code ff.mkdirs}, creating a directory
 * nothing ever read.
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

            // (1) no leak: composite keeps a single day directory, exactly like the plain twin
            final List<String> compositeDayDirs = dayDirs("c");
            final List<String> plainDayDirs = dayDirs("p");
            Assert.assertEquals("control: the plain twin must reclaim down to a single day directory,"
                            + " otherwise this workload does not exercise the leak at all " + plainDayDirs,
                    1, plainDayDirs.size());
            Assert.assertEquals("composite must not accumulate day-level version directories."
                            + " Before 1A this was 4 after the same churn " + compositeDayDirs,
                    1, compositeDayDirs.size());

            // (2) THE TRAP: live cells sit under the UNVERSIONED day directory
            Assert.assertTrue("the unversioned day directory must exist " + compositeDayDirs,
                    compositeDayDirs.contains("2023-01-02"));
            final List<String> cells = cellDirs("c", "2023-01-02");
            Assert.assertEquals("expected one live cell per distinct exch value " + cells, 3, cells.size());
            for (String cell : cells) {
                Assert.assertTrue("a live cell directory is <value>.<txn>, got " + cell,
                        cell.matches("E[0-2]\\.\\d+"));
            }

            // (3) no VERSIONED day directory exists at all -- that was the leaked artifact
            for (String dayDir : compositeDayDirs) {
                Assert.assertEquals("a composite day directory must be unversioned; " + dayDir
                        + " is a leaked day-level version", "2023-01-02", dayDir);
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
