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

import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * COVERING posting index on a composite table.
 * <p>
 * The plain-POSTING path is covered by {@code CompositePostingIndexSealTest}; this exercises the
 * {@code hasCovering} branch of the seal, which is materially different code: it maps every covered
 * column's data through {@code mapCoveringColumnsForSeal} and rebuilds sidecar files
 * ({@code .pci}/{@code .pc}) rather than just rotating the chain. Those covered-column lookups are
 * {@code _cv} reads, and {@code _cv} carries the cellKey in the COLUMN INDEX's high bits -- so a
 * cell-blind version reads another cell's column top and name-txn, and rebuilds the sidecars from the
 * wrong column data.
 * <p>
 * This class previously asserted that a POSTING index was REFUSED on a composite table, because the
 * seal was cell-blind and suspended the table on the first merge commit. The seal is now cell-aware,
 * so the same scenario is asserted to work.
 */
public class CompositeAddColumnPostingGateTest extends AbstractCairoTest {

    /**
     * POSITIVE CONTROL. The identical statements against a PLAIN table must behave the same, so a
     * passing composite assertion cannot be explained by covering indexes being broken generally.
     */
    @Test
    public void testPlainTableCoveringIndexStillWorks() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO p VALUES ('2023-01-01T01:00:00.000000Z','BTC',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','ETH',2.0)");
            drainWalQueue();
            execute("ALTER TABLE p ALTER COLUMN exch ADD INDEX TYPE POSTING INCLUDE (px)");
            drainWalQueue();
            execute("INSERT INTO p VALUES ('2023-01-01T01:30:00.000000Z','BTC',3.0)");
            drainWalQueue();
            assertLive("p", "3");
        });
    }

    @Test
    public void testCompositeCoveringIndexSurvivesO3Seal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            // Three cells on the day, so "the last partition" is one cell among several.
            execute("INSERT INTO c VALUES ('2023-01-01T02:00:00.000000Z','BTC','A',1.0),"
                    + "('2023-01-01T03:00:00.000000Z','ETH','B',2.0),"
                    + "('2023-01-01T04:00:00.000000Z','SOL','A',3.0),"
                    + "('2023-01-02T02:00:00.000000Z','BTC','C',4.0)");
            drainWalQueue();

            // COVERING on a NON-DIMENSION symbol, covering an ordinary column.
            execute("ALTER TABLE c ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (px)");
            drainWalQueue();
            assertLive("c", "4");

            // O3 into two of the three cells: the seal must rebuild those cells' sidecars and leave
            // the third alone.
            execute("INSERT INTO c VALUES ('2023-01-01T01:00:00.000000Z','BTC','A',5.0),"
                    + "('2023-01-01T01:30:00.000000Z','ETH','A',6.0)");
            drainWalQueue();
            assertLive("c", "6");

            final List<String> sidecars = coveringSidecarsInCells();
            Assert.assertFalse("covering sidecars must be written INSIDE cell directories; saw none",
                    sidecars.isEmpty());
        });
    }

    /** {@code <day>/<cell>/*.pci|.pc} -- covering sidecars living inside a cell. */
    private List<String> coveringSidecarsInCells() throws Exception {
        final List<String> out = new ArrayList<>();
        final Path root = Paths.get(configuration.getDbRoot());
        try (Stream<Path> walk = Files.walk(root, 5)) {
            for (Path f : walk.filter(p -> !Files.isDirectory(p)).toList()) {
                final String n = f.getFileName().toString();
                if (!n.contains(".pci") && !n.contains(".pc")) {
                    continue;
                }
                if (!f.toString().contains("c~")) {
                    continue;
                }
                final Path parent = f.getParent();
                if (parent != null && parent.getParent() != null
                        && parent.getParent().getFileName().toString().startsWith("2023-")) {
                    out.add(root.relativize(f).toString());
                }
            }
        }
        out.sort(String::compareTo);
        return out;
    }

    private void assertLive(String table, String expectedCount) throws Exception {
        Assert.assertFalse(table + " suspended",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(table)));
        final StringSink sink = new StringSink();
        TestUtils.printSql(engine, sqlExecutionContext, "SELECT count() FROM " + table, sink);
        TestUtils.assertContains(sink, expectedCount);
    }
}
