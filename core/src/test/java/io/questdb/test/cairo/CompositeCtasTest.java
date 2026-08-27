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
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
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
 * CREATE TABLE AS SELECT producing a real COMPOSITE table.
 * <p>
 * This was refused ("composite partitioning is not yet supported with CREATE TABLE AS SELECT") because
 * composite dimensions are resolved against known column definitions, and a CTAS's columns are not
 * known until the select has been compiled. Refusing was the right call at the time -- with the gate
 * lifted the dimension was silently DROPPED and the user got a PLAIN table with no error anywhere.
 * <p>
 * The resolution is simply deferred to the point where the select's metadata exists, which is where
 * covering-index INCLUDE columns are already resolved for CTAS
 * ({@code CreateTableOperationImpl#validateAndUpdateMetadataFromSelect}).
 * <p>
 * <b>The structural assertion is the load-bearing one.</b> A CTAS that silently produced a plain table
 * still copies every row, so a row comparison passes through exactly the defect this feature exists to
 * prevent. Only the on-disk CELL layout distinguishes them.
 */
public class CompositeCtasTest extends AbstractCairoTest {

    /**
     * A dimension given as a bare SYMBOL column reference -- the common case.
     */
    @Test(timeout = 120_000)
    public void testCtasCreatesACompositeTable() throws Exception {
        assertMemoryLeak(() -> {
            seedSource();

            execute("CREATE TABLE c AS (SELECT * FROM src) TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            drainWalQueue();
            engine.releaseInactive();

            // STRUCTURAL: the day must hold CELL directories. A silently-plain table would copy every
            // row and pass the data assertion below while failing this.
            final List<String> cells = cellDirs("c", "2023-01-01");
            Assert.assertEquals("the CTAS table must be composite, with one cell per dimension value."
                    + " Found: " + cells, 2, cells.size());

            // And it must carry the dimension in its DDL, not just on disk.
            printSql("SHOW CREATE TABLE c");
            TestUtils.assertContains(sink, "exch");

            // Every row copied.
            printSql("SELECT count() FROM c");
            TestUtils.assertContains(sink, "count\n5\n");
            assertSameRowsAsSource();
        });
    }

    /**
     * The select's columns, not the source table's, are what the dimension resolves against. A
     * projection that RENAMES the dimension column must resolve to the new name.
     */
    @Test(timeout = 120_000)
    public void testCtasResolvesTheDimensionAgainstTheSelectColumns() throws Exception {
        assertMemoryLeak(() -> {
            seedSource();

            execute("CREATE TABLE c AS (SELECT ts, exch AS venue, px FROM src)"
                    + " TIMESTAMP(ts) PARTITION BY DAY, venue LAYOUT PLAIN WAL");
            drainWalQueue();
            engine.releaseInactive();

            Assert.assertEquals("the dimension must resolve against the SELECT's column names",
                    2, cellDirs("c", "2023-01-01").size());
            printSql("SELECT count() FROM c");
            TestUtils.assertContains(sink, "count\n5\n");
        });
    }

    /**
     * A dimension naming a column the select does not produce must be refused, and must not leave a
     * half-created table behind.
     */
    @Test(timeout = 120_000)
    public void testCtasWithUnknownDimensionColumnIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            seedSource();
            try {
                execute("CREATE TABLE c AS (SELECT ts, px FROM src) TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");
                Assert.fail("a dimension naming a column the select does not produce must be refused");
            } catch (Exception e) {
                // The resolver maps MISSING and non-SYMBOL to the same -1 sentinel by design, so both
                // surface as this one message -- identical to the plain CREATE TABLE path. Asserting
                // the column name here would be asserting a message this branch has never produced.
                TestUtils.assertContains(e.getMessage(), "partition dimension must be a SYMBOL column");
            }
            // The load-bearing half: the refusal must happen BEFORE anything is created. Deferring
            // resolution to the select's metadata put it later in the sequence, so this is exactly the
            // "created first, threw afterwards" shape the original gate test warned about.
            printSql("SELECT count() FROM tables() WHERE table_name = 'c'");
            Assert.assertEquals("a refused CTAS must leave no table behind", "count\n0\n", sink.toString());
        });
    }

    /**
     * A non-SYMBOL dimension must be refused for CTAS exactly as it is for a plain CREATE TABLE.
     */
    @Test(timeout = 120_000)
    public void testCtasWithNonSymbolDimensionIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            seedSource();
            try {
                execute("CREATE TABLE c AS (SELECT * FROM src) TIMESTAMP(ts) PARTITION BY DAY, px LAYOUT PLAIN WAL");
                Assert.fail("a DOUBLE dimension must be refused");
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), "partition dimension must be a SYMBOL column");
            }
            // The load-bearing half: the refusal must happen BEFORE anything is created. Deferring
            // resolution to the select's metadata put it later in the sequence, so this is exactly the
            // "created first, threw afterwards" shape the original gate test warned about.
            printSql("SELECT count() FROM tables() WHERE table_name = 'c'");
            Assert.assertEquals("a refused CTAS must leave no table behind", "count\n0\n", sink.toString());
        });
    }

    /**
     * POSITIVE CONTROL: a CTAS with no dimension is untouched, so none of the above can be passing by
     * refusing or rerouting every CTAS.
     */
    @Test(timeout = 120_000)
    public void testPlainCtasIsUnaffected() throws Exception {
        assertMemoryLeak(() -> {
            seedSource();
            execute("CREATE TABLE c AS (SELECT * FROM src) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            engine.releaseInactive();
            Assert.assertTrue("a plain CTAS must NOT create cell directories. Found: "
                    + cellDirs("c", "2023-01-01"), cellDirs("c", "2023-01-01").isEmpty());
            printSql("SELECT count() FROM c");
            TestUtils.assertContains(sink, "count\n5\n");
        });
    }

    private void assertSameRowsAsSource() throws SqlException {
        printSql("SELECT ts, exch, px FROM c ORDER BY ts");
        final String composite = sink.toString();
        printSql("SELECT ts, exch, px FROM src ORDER BY ts");
        Assert.assertEquals("the CTAS table must hold exactly the source rows", sink.toString(), composite);
    }

    /**
     * Cell directories of one day: the subdirectories of {@code <table>/<day>}. A plain table has none.
     */
    private List<String> cellDirs(String table, String day) throws IOException {
        final List<String> out = new ArrayList<>();
        final Path dayDir = tableDir(table).resolve(day);
        if (!Files.isDirectory(dayDir)) {
            return out;
        }
        try (Stream<Path> children = Files.list(dayDir)) {
            children.filter(Files::isDirectory)
                    .map(p -> p.getFileName().toString())
                    .sorted(Comparator.naturalOrder())
                    .forEach(out::add);
        }
        return out;
    }

    private void seedSource() throws SqlException {
        execute("CREATE TABLE src (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO src VALUES ('2023-01-01T01:00:00.000000Z','E0',1.0),"
                + "('2023-01-01T02:00:00.000000Z','E1',2.0),"
                + "('2023-01-01T03:00:00.000000Z','E0',3.0),"
                + "('2023-01-02T01:00:00.000000Z','E1',4.0),"
                + "('2023-01-02T02:00:00.000000Z','E0',5.0)");
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
