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
