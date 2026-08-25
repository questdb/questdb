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
     * DETACH must remove EVERY cell directory of the day, not just cellKey 0's.
     * <p>
     * Measured 2026-08-25: detaching a two-cell day produced a correct artifact holding both cells, and
     * left a residual live {@code <day>/} still containing E1. The _txn entries are all removed, so the
     * orphan is invisible to queries and the twin comparison passes -- which is exactly why it went
     * unnoticed. It is not harmless: the residual directory makes the day's container already exist, so
     * re-attaching it fails ATTACH_ERR_DIR_EXISTS, and that status is TOLERATED as a WAL command failure,
     * so the ALTER silently does nothing.
     */
    @Test(timeout = 60_000)
    public void testDetachRemovesEveryCellDirectory() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedTwoDays();

            execute("ALTER TABLE c DETACH PARTITION LIST '2023-01-01'");
            execute("ALTER TABLE p DETACH PARTITION LIST '2023-01-01'");
            drainWalQueue();

            assertTwinEqual("");
            Assert.assertFalse("detach left a residual live day directory " + dayDirs("c"),
                    dayDirs("c").contains("2023-01-01"));
            // and the artifact must still hold both cells
            Assert.assertEquals("the artifact must hold both cells",
                    2, childDirs("c", "2023-01-01.detached").size());
        });
    }

    /**
     * DETACH then ATTACH must round-trip: the table returns to its pre-detach contents and agrees with
     * the twin, which never lost the day at all.
     */
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
            // the day must come back as a CONTAINER holding both its cells, not as a flat directory
            Assert.assertEquals("the re-attached day must hold both cells",
                    2, childDirs("c", "2023-01-01").size());
        });
    }

    /**
     * End-to-end counterpart of CompositeAttachArtifactTest's unit-level refusal: a real ATTACH of a
     * FOREIGN artifact must fail loudly and leave the table healthy.
     * <p>
     * A cellKey is table-local, and the artifact carries no dimension dictionaries or _cell registry --
     * those are table-root. Accepting one would bind its cells to whatever local cells share those
     * ordinals: correct-looking rows under the wrong dimension value.
     */
    @Test(timeout = 60_000)
    public void testAttachFromAnotherTableIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            execute("CREATE TABLE other (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            // c must NOT already hold 2023-01-01, or ATTACH stops at "partition is already attached"
            // before it ever looks at whose artifact this is.
            insertIntoBoth("('2023-01-02T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-03T01:00:00.000000Z','E0',2.0)");
            execute("INSERT INTO other VALUES ('2023-01-01T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-01T20:00:00.000000Z','E1',2.0),"
                    + "('2023-01-02T01:00:00.000000Z','E0',3.0)");
            drainWalQueue();
            engine.releaseInactive();

            execute("ALTER TABLE other DETACH PARTITION LIST '2023-01-01'");
            drainWalQueue();

            Files.move(tableDir("other").resolve("2023-01-01.detached"),
                    tableDir("c").resolve("2023-01-01.attachable"));

            // MEASURED, and it matches the PLAIN twin exactly: an ATTACH validation failure on a WAL
            // table does not throw from execute() -- the ALTER is applied asynchronously, so the failure
            // surfaces on the apply thread and suspends the table. A plain table given a foreign artifact
            // does precisely the same ("no throw from execute() | suspended=true"), via attachPrepare's
            // own table_id check. So composite is behaving as its twin, not inventing a worse outcome.
            execute("ALTER TABLE c ATTACH PARTITION LIST '2023-01-01'");
            drainWalQueue();

            assertQuery("SELECT suspended, errorMessage FROM wal_tables() WHERE name = 'c'")
                    .noLeakCheck().noRandomAccess()
                    .returns("suspended\terrorMessage\n"
                            + "true\tcomposite partitioning does not yet support attaching a partition "
                            + "from another table [table=c, tableId=1, artifactTableId=3]\n");
        });
    }

    private void assertWalTableNotSuspended(String tableName) {
        Assert.assertFalse(
                tableName + " must not be suspended",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
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
