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
 * {@code ColumnPurgeJob} nor {@code ColumnPurgeTask} carries a composite gate. On a composite table the
 * real column files live at {@code <day>/<cell>.<nameTxn>/}, while the day container holds only
 * zero-byte phantoms, so a cell-blind purge cannot be addressing the files it means to.
 * <p>
 * That is the same shape as every other defect on this branch -- a path built without the cellKey --
 * and it reaches files it DELETES, via both {@code VACUUM} and the background purge job. This class
 * establishes what actually happens: whether stale column versions leak, or something worse.
 * <p>
 * The oracle is the plain twin. Both tables get identical statements; whatever reclaim the plain table
 * performs, the composite one should match, and the ROWS must agree regardless.
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

            // PIN: everything VACUUM removes on the composite table is a DAY-LEVEL file, i.e. the
            // cell-blind path. The real stale column versions live under <day>/<cell>.<nameTxn>/ and are
            // never reached, so they are never reclaimed.
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
