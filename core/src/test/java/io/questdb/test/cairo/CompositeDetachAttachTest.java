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
 * Sub-project 1: {@code DETACH PARTITION} / {@code ATTACH PARTITION} on a composite table.
 *
 * <p>These were blocked behind {@code SQUASH PARTITIONS} until 1E: {@code TableWriter#detachPartition}
 * calls squash internally, so DETACH suspended on the squash gate rather than on anything of its own.
 * With squash cell-aware that dependency is gone, and DETACH's own behaviour can be measured.
 *
 * <p>The measurement matters more than the guess. A plain DETACH renames the partition directory to
 * {@code <partition>.detached}; a composite day is a CONTAINER of cell directories, so the shape of a
 * detached composite partition is exactly what these tests pin rather than assume.
 */
public class CompositeDetachAttachTest extends AbstractCompositeTwinTest {

    /**
     * Whole-day DETACH, compared against the plain twin. Whether this passes today is unknown at the
     * time of writing -- that is the point: it establishes what DETACH does on a composite table before
     * any work is done to change it.
     */
    @Test(timeout = 60_000)
    public void testDetachWholeDay() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedTwoDays();

            execute("ALTER TABLE c DETACH PARTITION LIST '2023-01-01'");
            execute("ALTER TABLE p DETACH PARTITION LIST '2023-01-01'");
            drainWalQueue();

            assertTwinEqual("");

            // the detached artifact must exist and, for composite, must hold the day's CELLS
            final List<String> detached = detachedDirs("c");
            Assert.assertEquals("expected exactly one detached day " + dayDirs("c"), 1, detached.size());
            final List<String> cells = childDirs("c", detached.get(0));
            Assert.assertFalse("a detached composite day must contain its cell directories " + cells,
                    cells.isEmpty());
        });
    }

    /**
     * DETACH then ATTACH must round-trip: the table returns to its pre-detach contents and agrees with
     * the twin, which never lost the day at all.
     */
    @Ignore("SP1: ATTACH is not yet supported for composite. RE-MEASURED 2026-08-25 with the gates"
            + " lifted, and it now fails LATER than it used to. The size sum, the min/max fold, the bare"
            + " day container and the per-cell _txn registration are all in place, so attach returns OK --"
            + " and the day's rows are then MISSING from the table. That is the exact silent shape"
            + " attachPartition's own gate comment predicts: the per-cell column-version pinning"
            + " (attachPartitionPinColumnVersions iterates the CONTAINER) and the three"
            + " attachPartitionCheckFilesMatch* validators still concatenate flat, with no cell concept,"
            + " and a missing column is recorded as a full-partition column top rather than an error."
            + " Un-ignore when those walk cells.")
    @Test(timeout = 60_000)
    public void testDetachThenAttachRoundTrips() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedTwoDays();

            execute("ALTER TABLE c DETACH PARTITION LIST '2023-01-01'");
            drainWalQueue();

            // The documented operator step, not a defect: ATTACH consumes <partition>.attachable, so a
            // detached artifact is renamed before it can be re-attached. Doing this by hand is exactly
            // what a user does.
            Files.move(tableDir("c").resolve("2023-01-01.detached"),
                    tableDir("c").resolve("2023-01-01.attachable"));

            execute("ALTER TABLE c ATTACH PARTITION LIST '2023-01-01'");
            drainWalQueue();

            assertTwinEqual("");
            Assert.assertTrue("no detached artifact should remain " + detachedDirs("c"),
                    detachedDirs("c").isEmpty());
        });
    }

    private List<String> childDirs(String table, String container) throws IOException {
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

    private List<String> detachedDirs(String table) throws IOException {
        final List<String> out = new ArrayList<>();
        for (String d : dayDirs(table)) {
            if (d.endsWith(".detached")) {
                out.add(d);
            }
        }
        return out;
    }

    private void seedTwoDays() throws Exception {
        insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',1.0),"
                + "('2023-01-01T20:00:00.000000Z','E1',2.0),"
                + "('2023-01-02T01:00:00.000000Z','E0',3.0)");
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
