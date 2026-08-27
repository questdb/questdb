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
 * EXPRESSION dimensions: {@code PARTITION BY DAY, (expr) AS alias}.
 * <p>
 * Unlike IDENTITY/HASH/TRUNCATE, an expression dimension has no source SYMBOL column -- it is
 * evaluated per row and its result interned into a dedicated dictionary
 * ({@code PartitionDimension.KIND_EXPRESSION}). The pieces exist across the parser
 * ({@code resolveExpressionDimension} plus a DDL-time safe-subset walk), the writer's interner
 * ({@code TableWriter#internDimensionValue}) and the reader.
 * <p>
 * This class establishes what actually works end to end today: create, ingest, read back, and the
 * refusals that protect the shape (nondeterministic functions, subqueries, bind variables).
 */
public class CompositeExpressionDimensionTest extends AbstractCairoTest {

    /**
     * The whole point of an expression dimension: rows are grouped by the VALUE OF THE EXPRESSION, so
     * two different source values that the expression maps together share one cell.
     */
    @Test(timeout = 120_000)
    public void testExpressionDimensionGroupsByTheExpressionResult() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, (upper(exch)) AS venue LAYOUT PLAIN WAL");
            execute("INSERT INTO c VALUES ('2023-01-01T01:00:00.000000Z','e0',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','E0',2.0),"
                    + "('2023-01-01T03:00:00.000000Z','e1',3.0)");
            drainWalQueue();
            engine.releaseInactive();

            // 'e0' and 'E0' both map to 'E0', so they belong in ONE cell; 'e1' in another.
            final List<String> cells = cellDirs("c", "2023-01-01");
            Assert.assertEquals("rows must be grouped by the EXPRESSION's value, not the raw column. "
                    + "Found: " + cells, 2, cells.size());

            printSql("SELECT count() FROM c");
            TestUtils.assertContains(sink, "count\n3\n");
            printSql("SELECT ts, exch, px FROM c ORDER BY ts");
            TestUtils.assertContains(sink, "e0");
            TestUtils.assertContains(sink, "E0");
            TestUtils.assertContains(sink, "e1");
        });
    }

    /**
     * A bare column reference WITH an alias is a legal expression dimension too -- it evaluates to the
     * same value an IDENTITY dimension would, just without IDENTITY's ordinal fast path.
     */
    @Test(timeout = 120_000)
    public void testAliasedBareColumnIsAnExpressionDimension() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, (exch) AS venue LAYOUT PLAIN WAL");
            execute("INSERT INTO c VALUES ('2023-01-01T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','E1',2.0)");
            drainWalQueue();
            engine.releaseInactive();

            Assert.assertEquals("one cell per distinct expression value", 2, cellDirs("c", "2023-01-01").size());
            printSql("SELECT count() FROM c");
            TestUtils.assertContains(sink, "count\n2\n");
        });
    }

    /**
     * A NONDETERMINISTIC expression must be refused at CREATE: the cell a row belongs to has to be
     * reproducible, or a reader cannot find its rows again.
     */
    @Test(timeout = 120_000)
    public void testNonDeterministicExpressionIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                        + "PARTITION BY DAY, (rnd_symbol('a','b')) AS venue LAYOUT PLAIN WAL");
                Assert.fail("a nondeterministic expression dimension must be refused");
            } catch (Exception e) {
                Assert.assertNotNull(e.getMessage());
            }
            printSql("SELECT count() FROM tables() WHERE table_name = 'c'");
            Assert.assertEquals("a refused CREATE must leave no table behind", "count\n0\n", sink.toString());
        });
    }

    /**
     * An expression over a non-existent column must be refused, naming the column.
     */
    @Test(timeout = 120_000)
    public void testExpressionOverUnknownColumnIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                        + "PARTITION BY DAY, (upper(nope)) AS venue LAYOUT PLAIN WAL");
                Assert.fail("an expression over an unknown column must be refused");
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), "nope");
            }
            printSql("SELECT count() FROM tables() WHERE table_name = 'c'");
            Assert.assertEquals("a refused CREATE must leave no table behind", "count\n0\n", sink.toString());
        });
    }

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
