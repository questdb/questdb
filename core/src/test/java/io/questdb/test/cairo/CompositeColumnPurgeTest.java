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

import io.questdb.cairo.ColumnPurgeOperator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Column-version purge on a composite table.
 * <p>
 * {@code ColumnPurgeOperator#setUpPartitionPath} builds the target table's partition path with the
 * bare 5-arg {@code setPathForNativePartition} -- no cell segment -- and neither
 * {@code ColumnPurgeJob} nor {@code ColumnPurgeTask} carries a composite gate. It reaches files it
 * DELETES, via both {@code VACUUM} and the background purge job, so on paper it is the same
 * cell-blind-path family as the rest of this branch.
 * <p>
 * <b>MEASURED: there is no leak here, and an earlier revision of this file claiming one was wrong.</b>
 * After ALTER COLUMN TYPE and VACUUM, every cell holds ONLY the new version:
 * <pre>
 *   2023-01-01/E0/px.d.1   2023-01-01/E1/px.d.1   2023-01-02/E0/px.d.1
 *   2023-01-01/exch.d   2023-01-01/ts.d        (day phantoms; px.d is what VACUUM removed)
 * </pre>
 * No stale {@code px.d} (version 0) survives in any cell -- the column conversion cleans up per cell as
 * it rewrites, so by the time the purge runs there is nothing per-cell left to reclaim. The single file
 * VACUUM removes is the DAY-LEVEL phantom, and that removal is benign: measured, the day still takes
 * further inserts across cells, still converts to parquet, and the rows still match the plain twin.
 * <p>
 * So the cell-blind path is real but currently harmless on this route. What is NOT established is
 * whether some other route strands a per-cell column version -- if one does, this purge could not
 * reclaim it. That would need a scenario that leaves a superseded per-cell file behind, which nothing
 * here produces. Compare {@code CompositePostingSealPurgeTest}, where exactly such a scenario DOES
 * exist (O3 into an already-sealed day) and the equivalent cell-blindness was a genuine leak, now
 * fixed by enumerating cell directories.
 * <p>
 * The oracle is the plain twin. Both tables get identical statements, and the ROWS must agree at every
 * stage regardless of what the purge does.
 */
public class CompositeColumnPurgeTest extends AbstractCompositeTwinTest {

    /**
     * ALTER COLUMN TYPE bumps a column version, leaving the previous version's files as purge
     * candidates. VACUUM then drives {@link io.questdb.cairo.ColumnPurgeOperator} over them. UPDATE
     * would be the more usual trigger, but composite refuses it outright.
     * <p>
     * The data assertion is the one that must never fail. The file-count comparison characterises the
     * reclaim.
     */
    @Test(timeout = 120_000)
    public void testAlterColumnTypeThenVacuumAgreesWithThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','E1',2.0),"
                    + "('2023-01-02T01:00:00.000000Z','E0',3.0)");
            drainWalQueue();

            // ALTER COLUMN TYPE rewrites the column, leaving the previous version's files behind as
            // purge candidates. UPDATE would be the more usual trigger but composite refuses it
            // ("composite partitioning does not support UPDATE"), so this is the reachable route.
            execute("ALTER TABLE c ALTER COLUMN px TYPE FLOAT");
            execute("ALTER TABLE p ALTER COLUMN px TYPE FLOAT");
            drainWalQueue();
            engine.releaseInactive();

            // The conversion itself must be correct before any purge question is meaningful.
            assertTwinEqual("");

            final java.util.List<String> beforeC = relFiles("c");

            execute("VACUUM TABLE c");
            execute("VACUUM TABLE p");
            drainWalQueue();
            engine.releaseInactive();

            // The rows must survive the purge on both sides. A cell-blind purge that deleted a live
            // cell's column files would show up here.
            assertTwinEqual("");

            // PIN: everything VACUUM removes on the composite table is a DAY-LEVEL file, i.e. via the
            // cell-blind path. Note this is NOT evidence of a leak -- measured, the cells hold only the
            // NEW version (px.d.1) by this point, so there is nothing per-cell left to reclaim.
            final java.util.List<String> removed = new ArrayList<>(beforeC);
            removed.removeAll(relFiles("c"));
            Assert.assertFalse(
                    "VACUUM must remove SOMETHING on the composite table, else this pin is vacuous",
                    removed.isEmpty());
            for (String r : removed) {
                // "2023-01-01/px.d" has one separator; a per-cell file "2023-01-01/E0.1/px.d" has two.
                Assert.assertEquals(
                        "ColumnPurgeOperator is now reaching per-cell files -- the cell-blind purge is "
                                + "fixed, update this pin. Removed: " + removed,
                        1, r.chars().filter(ch -> ch == '/' || ch == '\\').count());
            }

            // The deletion is nevertheless BENIGN today, which is why this is a leak and not a defect:
            // the day keeps taking writes and still converts, with the phantom gone.
            insertIntoBoth("('2023-01-01T03:00:00.000000Z','E0',9.0),"
                    + "('2023-01-01T04:00:00.000000Z','E2',10.0)");
            drainWalQueue();
            engine.releaseInactive();
            assertTwinEqual("");

            // And the day must still convert -- the parquet encoder is handed the day container, which
            // is where the deleted phantom lived.
            execute("ALTER TABLE c CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            drainWalQueue();
            engine.releaseInactive();
            assertTwinEqual("");
        });
    }

    /**
     * The other half of "harmless": what the purge DOES delete is the day-level phantom, and the table
     * must keep working afterwards. One or two follow-up operations proved little, so this exercises a
     * spread of them AFTER the phantom is gone -- writes into the purged day and a new day, a cell
     * added by a new dimension value, squash, convert to parquet, and reads throughout.
     * <p>
     * Every step compares against the plain twin, so a divergence introduced by the missing phantom
     * shows up as wrong DATA rather than as a size difference nobody would notice.
     */
    @Test(timeout = 120_000)
    public void testTableKeepsWorkingAfterThePhantomIsPurged() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','E1',2.0),"
                    + "('2023-01-02T01:00:00.000000Z','E0',3.0)");
            drainWalQueue();

            execute("ALTER TABLE c ALTER COLUMN px TYPE FLOAT");
            execute("ALTER TABLE p ALTER COLUMN px TYPE FLOAT");
            drainWalQueue();
            execute("VACUUM TABLE c");
            execute("VACUUM TABLE p");
            drainWalQueue();
            engine.releaseInactive();
            assertTwinEqual("");

            // 1. append into the purged day, and into a brand-new day
            insertIntoBoth("('2023-01-01T09:00:00.000000Z','E0',9.0),"
                    + "('2023-01-03T01:00:00.000000Z','E1',10.0)");
            drainWalQueue();
            engine.releaseInactive();
            assertTwinEqual("");

            // 2. an O3 write into the purged day
            insertIntoBoth("('2023-01-01T01:30:00.000000Z','E1',11.0)");
            drainWalQueue();
            engine.releaseInactive();
            assertTwinEqual("");

            // 3. a NEW dimension value, i.e. a cell the purged day never had
            insertIntoBoth("('2023-01-01T10:00:00.000000Z','E2',12.0)");
            drainWalQueue();
            engine.releaseInactive();
            assertTwinEqual("");
            assertTwinEqual(" WHERE exch = 'E2'");

            // 4. squash, then 5. convert the purged day to parquet
            execute("ALTER TABLE c SQUASH PARTITIONS");
            execute("ALTER TABLE p SQUASH PARTITIONS");
            drainWalQueue();
            engine.releaseInactive();
            assertTwinEqual("");

            execute("ALTER TABLE c CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            drainWalQueue();
            engine.releaseInactive();
            assertTwinEqual("");
            assertTwinEqual(" WHERE exch = 'E0'");

            // 6. and it still takes writes once parquet
            insertIntoBoth("('2023-01-04T01:00:00.000000Z','E0',13.0)");
            drainWalQueue();
            engine.releaseInactive();
            assertTwinEqual("");
        });
    }

    /**
     * The DEPTH RULE the deletion guard enforces, tested directly. The guard itself cannot be observed
     * firing through VACUUM -- ColumnPurgeOperator structurally cannot build a cell path -- so without
     * this the rule would be untested machinery, and "it never fires" would be indistinguishable from
     * "it is wrong".
     * <p>
     * Past the table directory: {@code /<day>/<file>} is two separators (allowed, this is every path
     * the operator builds); {@code /<day>/<cell>/<file>} is three (refused, that is live cell data).
     */
    @Test
    public void testDeletionGuardDepthRule() {
        Assert.assertFalse("a table-root file must be deletable", ColumnPurgeOperator.exceedsDayDepth(1));
        Assert.assertFalse("<day>/<file> is what this operator builds and must stay deletable",
                ColumnPurgeOperator.exceedsDayDepth(2));
        Assert.assertTrue("<day>/<cell>/<file> is live composite cell data and must be refused",
                ColumnPurgeOperator.exceedsDayDepth(3));
        Assert.assertTrue("anything deeper is refused too", ColumnPurgeOperator.exceedsDayDepth(4));
    }

    private java.util.List<String> relFiles(String table) throws IOException {
        final java.util.List<String> out = new ArrayList<>();
        final Path base = tableDir(table);
        try (Stream<Path> all = Files.walk(base)) {
            all.filter(Files::isRegularFile).map(f -> base.relativize(f).toString()).sorted().forEach(out::add);
        }
        return out;
    }

    private Path tableDir(String table) throws IOException {
        final Path root = Paths.get(configuration.getDbRoot());
        try (Stream<Path> children = Files.list(root)) {
            final List<Path> matches = new ArrayList<>();
            children.filter(Files::isDirectory)
                    .filter(pp -> pp.getFileName().toString().startsWith(table + "~"))
                    .forEach(matches::add);
            if (matches.isEmpty()) {
                throw new AssertionError("no table directory for " + table);
            }
            return matches.get(0);
        }
    }
}
