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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CompositeDetachedArtifact;
import io.questdb.cairo.PartitionBy;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Sub-project 1, ATTACH task 1: enumerating a detached composite artifact's cells from its own copied
 * {@code _txn}, never from its directory names.
 */
public class CompositeAttachArtifactTest extends AbstractCompositeTwinTest {

    private static final String DAY = "2023-01-01";

    /**
     * The discriminating case: a day routed to TWO cells. Reading the artifact's copied {@code _txn}
     * must yield both, because everything downstream -- the size sum, the min/max fold, the per-cell
     * registration -- is driven off this enumeration. Reading only cellKey 0 is exactly the cell-blind
     * shape that makes ATTACH report {@code partitionSizeRows=1} for a two-row day today.
     */
    @Test(timeout = 60_000)
    public void testEnumeratesEveryCellOfADetachedDay() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('" + DAY + "T01:00:00.000000Z','E0',1.0),"
                    + "('" + DAY + "T20:00:00.000000Z','E1',2.0),"
                    + "('2023-01-02T01:00:00.000000Z','E0',3.0)");
            drainWalQueue();
            engine.releaseInactive();

            execute("ALTER TABLE c DETACH PARTITION LIST '" + DAY + "'");
            drainWalQueue();

            final IntList cellKeys = readArtifactCellKeys();
            Assert.assertEquals("the detached day was routed to two cells, got " + cellKeys, 2, cellKeys.size());
        });
    }

    /**
     * The artifact's {@code _txn} is the whole table's {@code _txn} as of the detach, so it also lists
     * 2023-01-02. Filtering by the requested timestamp is load-bearing, not incidental: without it the
     * enumeration returns the other day's cells too and ATTACH would register partitions that are not
     * in the artifact at all.
     */
    @Test(timeout = 60_000)
    public void testIgnoresCellsBelongingToOtherDays() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            // 2023-01-02 is deliberately routed to a cell (E2) that 2023-01-01 does NOT use, so a
            // missing timestamp filter shows up as an extra cellKey rather than a duplicate.
            insertIntoBoth("('" + DAY + "T01:00:00.000000Z','E0',1.0),"
                    + "('" + DAY + "T20:00:00.000000Z','E1',2.0),"
                    + "('2023-01-02T01:00:00.000000Z','E2',3.0)");
            drainWalQueue();
            engine.releaseInactive();

            execute("ALTER TABLE c DETACH PARTITION LIST '" + DAY + "'");
            drainWalQueue();

            final IntList cellKeys = readArtifactCellKeys();
            Assert.assertEquals("only " + DAY + "'s cells belong here, got " + cellKeys, 2, cellKeys.size());
        });
    }

    /**
     * The measured defect this fixes: attaching a two-row, two-cell day reported
     * {@code partitionSizeRows=1}. {@code getPartitionRowCountByTimestamp} resolves through
     * {@code findAttachedPartitionRawIndexByLoTimestamp}, which hardcodes {@code cellKey = 0}, so it
     * returns the FIRST cell's row count and calls it the day's. The size must be the sum across cells.
     * <p>
     * Asserting the number, not merely that the read stopped failing: a cell-blind read still returns a
     * plausible-looking 1 here.
     */
    @Test(timeout = 60_000)
    public void testSizeIsTheSumAcrossCellsNotTheFirstCell() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            // Three rows on the detached day, split 2/1 across two cells, so neither a first-cell-only
            // read (2) nor a cell count (2) can masquerade as the correct total of 3.
            insertIntoBoth("('" + DAY + "T01:00:00.000000Z','E0',1.0),"
                    + "('" + DAY + "T02:00:00.000000Z','E0',2.0),"
                    + "('" + DAY + "T20:00:00.000000Z','E1',3.0),"
                    + "('2023-01-02T01:00:00.000000Z','E0',4.0)");
            drainWalQueue();
            engine.releaseInactive();

            execute("ALTER TABLE c DETACH PARTITION LIST '" + DAY + "'");
            drainWalQueue();

            Assert.assertEquals("the detached day holds three rows across two cells",
                    3L, readArtifactSize());
        });
    }

    /**
     * The other half of the measured attach failure. {@code readNativeMinMaxTimestamps} opens
     * {@code <container>/ts.d} -- and on a composite artifact that file EXISTS and is ZERO BYTES (the
     * phantom bare-day files detach carries along), so both reads come back negative and attach throws
     * "cannot read min, max timestamp ... errno=2". The errno is incidental: the throw is on the
     * negative check, not on a failed open.
     * <p>
     * Worth stating because it is the dangerous shape: had that phantom file held bytes, attach would
     * have read WRONG timestamps silently instead of failing. Min and max must come from the CELLS.
     */
    @Test(timeout = 60_000)
    public void testMinMaxAreFoldedAcrossCellsNotReadAtTheContainerRoot() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            // The day's min lives in E0 and its max in E1, so a fold that looks at only one cell -- or at
            // the container root -- cannot produce both correct values by accident.
            insertIntoBoth("('" + DAY + "T01:00:00.000000Z','E0',1.0),"
                    + "('" + DAY + "T02:00:00.000000Z','E0',2.0),"
                    + "('" + DAY + "T20:00:00.000000Z','E1',3.0),"
                    + "('2023-01-02T01:00:00.000000Z','E0',4.0)");
            drainWalQueue();
            engine.releaseInactive();

            execute("ALTER TABLE c DETACH PARTITION LIST '" + DAY + "'");
            drainWalQueue();

            final long[] minMax = new long[]{-1, -1};
            final ObjList<CharSequence> segments = new ObjList<>();
            segments.add("E0");
            segments.add("E1");
            final LongList sizes = new LongList();
            sizes.add(2);
            sizes.add(1);

            final String artifactDir = tableDir().resolve(DAY + ".detached").toString();
            try (Path path = new Path()) {
                path.of(artifactDir);
                CompositeDetachedArtifact.readMinMaxTimestamps(
                        configuration.getFilesFacade(), path, "ts", ColumnType.TIMESTAMP,
                        segments, sizes, minMax);
            }

            Assert.assertEquals("min must come from E0's first row",
                    parseFloorPartialTimestamp(DAY + "T01:00:00.000000Z"), minMax[0]);
            Assert.assertEquals("max must come from E1's last row",
                    parseFloorPartialTimestamp(DAY + "T20:00:00.000000Z"), minMax[1]);
        });
    }

    private long readArtifactSize() throws IOException {
        final String artifact = tableDir().resolve(DAY + ".detached").toString();
        try (Path path = new Path()) {
            path.of(artifact);
            return CompositeDetachedArtifact.readSize(
                    configuration.getFilesFacade(),
                    path,
                    ColumnType.TIMESTAMP,
                    PartitionBy.DAY,
                    parseFloorPartialTimestamp(DAY)
            );
        }
    }

    private IntList readArtifactCellKeys() throws IOException {
        final IntList out = new IntList();
        final String artifact = tableDir().resolve(DAY + ".detached").toString();
        try (Path path = new Path()) {
            path.of(artifact);
            CompositeDetachedArtifact.readCellKeys(
                    configuration.getFilesFacade(),
                    path,
                    ColumnType.TIMESTAMP,
                    PartitionBy.DAY,
                    parseFloorPartialTimestamp(DAY),
                    out
            );
        }
        return out;
    }

    private java.nio.file.Path tableDir() throws IOException {
        try (Stream<java.nio.file.Path> children = Files.list(Paths.get(configuration.getDbRoot()))) {
            return children.filter(Files::isDirectory)
                    .filter(pp -> pp.getFileName().toString().startsWith("c~"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("no table directory for c"));
        }
    }
}
