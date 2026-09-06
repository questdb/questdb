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
 * COMPOSITE + PARQUET, established by measurement rather than by reading the notes.
 * <p>
 * This class exists because the recorded picture was wrong in both directions and the existing tests
 * could not tell. {@code CompositeUnsupportedOpsTest#testConvertPartitionToParquetIsNoLongerGated}
 * asserts only that the table is not SUSPENDED after a CONVERT -- which a silent no-op satisfies
 * exactly as well as a real conversion -- so "CONVERT is supported" rested on an assertion that could
 * not distinguish the two. In the other direction, the project's own notes had the whole PARQUET
 * family recorded as unimplemented and blocking cold storage.
 * <p>
 * MEASURED 2026-08-26, before the fix:
 * <ul>
 *   <li>{@code CONVERT PARTITION TO PARQUET} genuinely worked, per CELL -- a two-cell day yields TWO
 *       parquet files and two partitions reporting {@code isParquet}.</li>
 *   <li>An O3 INSERT into a converted cell SUSPENDED the table and LOST the row:
 *       <pre>
 *         plain      suspended=false  count=4   (row landed)
 *         composite  suspended=true   count=3   (row lost)
 *         wal errorMessage: composite partitioning does not yet support FORMAT PARQUET
 *       </pre>
 *       Worse than a refusal: the INSERT returned SUCCESS to the client and the failure surfaced later
 *       inside the WAL apply job, breaking the invariant that a refusal fires at the statement which
 *       caused it -- reached by exactly the sequence cold storage performs.</li>
 * </ul>
 * <p>
 * Both are now covered as WORKING. The fix needed two changes together:
 * {@code O3PartitionJob#processParquetPartition} was cell-blind (nine bare path builds and a cellKey-0
 * index lookup), and the composite dispatch hardcoded {@code isParquet=false}, which was only safe
 * while the gate refused every parquet cell. Fixing the paths alone sent the write down the NATIVE
 * path into a parquet cell and suspended the table with an EMPTY error message -- strictly worse than
 * the refusal it replaced.
 * <p>
 * The assertions that matter are the TWIN comparison against a plain table (a lost row and a wrong
 * order both survive a hand-written expectation) and the check that the cell is STILL parquet
 * afterwards -- a fix that quietly converted the cell back to native would also produce correct rows
 * while undoing the tiering cold storage had just performed.
 */
public class CompositeParquetStateTest extends AbstractCairoTest {

    @Test
    public void testPlainTableConvertProducesAParquetFile() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY WAL");
            execute("INSERT INTO p VALUES "
                    + "('2023-09-01T01:00:00.000000Z','BTC',1.0),"
                    + "('2023-09-01T02:00:00.000000Z','ETH',2.0),"
                    + "('2023-09-02T01:00:00.000000Z','BTC',3.0)");
            drainWalQueue();

            execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '2023-09-01'");
            drainWalQueue();
            assertNotSuspended("p");

            Assert.assertFalse("plain CONVERT must leave a data.parquet on disk; saw " + allFiles("p~"),
                    parquetFiles("p~").isEmpty());
            assertPartitionsReportParquet("p", 1);
        });
    }

    /**
     * A two-cell day must yield TWO parquet files. That is what distinguishes a per-cell conversion
     * from a day-level one, and both from a no-op.
     */
    @Test
    public void testCompositeConvertProducesAParquetFilePerCell() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            execute("INSERT INTO c VALUES "
                    + "('2023-09-01T01:00:00.000000Z','BTC',1.0),"
                    + "('2023-09-01T02:00:00.000000Z','ETH',2.0),"
                    + "('2023-09-02T01:00:00.000000Z','BTC',3.0)");
            drainWalQueue();

            execute("ALTER TABLE c CONVERT PARTITION TO PARQUET LIST '2023-09-01'");
            drainWalQueue();
            assertNotSuspended("c");

            Assert.assertFalse("CONVERT reported success but produced NO parquet file. Files: "
                            + allFiles("c~"),
                    parquetFiles("c~").isEmpty());
            assertPartitionsReportParquet("c", 2);

            final StringSink sink = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext, "SELECT ts, exch, px FROM c ORDER BY ts", sink);
            TestUtils.assertEquals("ts\texch\tpx\n"
                    + "2023-09-01T01:00:00.000000Z\tBTC\t1.0\n"
                    + "2023-09-01T02:00:00.000000Z\tETH\t2.0\n"
                    + "2023-09-02T01:00:00.000000Z\tBTC\t3.0\n", sink);
        });
    }

    /**
     * THE COLD-STORAGE SEQUENCE: tier a day to parquet, then keep ingesting into it out of order.
     * <p>
     * This test previously PINNED the defect -- the INSERT returned success, the WAL apply job then
     * suspended the table and the row was lost (composite count=3/suspended vs plain
     * count=4/not-suspended). Two things had to change together: {@code processParquetPartition} was
     * cell-blind, and the composite dispatch hardcoded {@code isParquet=false}, which was only safe
     * while the gate refused every parquet cell. Fixing the paths alone sent the write down the NATIVE
     * path into a parquet cell and suspended the table with an EMPTY error message.
     * <p>
     * Asserted against the PLAIN twin rather than a hand-written expectation, because the failure modes
     * here are a lost row and a wrong ORDER, and a string written from belief can encode either.
     */
    @Test
    public void testO3IntoConvertedCellMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwoDayTable("c2", ", exch");
            createTwoDayTable("p2", "");

            execute("ALTER TABLE c2 CONVERT PARTITION TO PARQUET LIST '2023-09-01'");
            execute("ALTER TABLE p2 CONVERT PARTITION TO PARQUET LIST '2023-09-01'");
            drainWalQueue();
            assertNotSuspended("c2");
            assertPartitionsReportParquet("c2", 2);

            // O3: BEFORE the existing rows of a now-parquet cell.
            execute("INSERT INTO c2 VALUES ('2023-09-01T01:00:00.000000Z','BTC',4.0)");
            execute("INSERT INTO p2 VALUES ('2023-09-01T01:00:00.000000Z','BTC',4.0)");
            drainWalQueue();

            assertNotSuspended("c2");
            // The cell must still BE parquet -- a fix that silently converted it back to native would
            // also make the rows correct, and would quietly undo the tiering cold storage just did.
            assertPartitionsReportParquet("c2", 2);

            final StringSink composite = new StringSink();
            final StringSink plain = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext, "SELECT ts, exch, px FROM c2 ORDER BY ts", composite);
            TestUtils.printSql(engine, sqlExecutionContext, "SELECT ts, exch, px FROM p2 ORDER BY ts", plain);
            TestUtils.assertEquals("composite parquet cell differs from the plain twin", plain, composite);

            // and explicitly: the row LANDED
            final StringSink count = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext, "SELECT count() FROM c2", count);
            TestUtils.assertContains(count, "4");
        });
    }

    private void createTwoDayTable(String name, String dimension) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                + "PARTITION BY DAY" + dimension + " WAL");
        execute("INSERT INTO " + name + " VALUES "
                + "('2023-09-01T02:00:00.000000Z','BTC',1.0),"
                + "('2023-09-01T03:00:00.000000Z','ETH',2.0),"
                + "('2023-09-02T01:00:00.000000Z','BTC',3.0)");
        drainWalQueue();
    }

    /**
     * CONTROL: the identical sequence on a PLAIN table keeps running and keeps the row. Without this,
     * the composite result above could be a general QuestDB limitation rather than a composite gap.
     */
    @Test
    public void testPlainTableO3IntoConvertedPartitionSucceeds() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p2 (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY WAL");
            execute("INSERT INTO p2 VALUES "
                    + "('2023-09-01T02:00:00.000000Z','BTC',1.0),"
                    + "('2023-09-01T03:00:00.000000Z','ETH',2.0),"
                    + "('2023-09-02T01:00:00.000000Z','BTC',3.0)");
            drainWalQueue();
            execute("ALTER TABLE p2 CONVERT PARTITION TO PARQUET LIST '2023-09-01'");
            drainWalQueue();
            execute("INSERT INTO p2 VALUES ('2023-09-01T01:00:00.000000Z','BTC',4.0)");
            drainWalQueue();

            assertNotSuspended("p2");
            final StringSink count = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext, "SELECT count() FROM p2", count);
            TestUtils.assertContains(count, "4");
        });
    }

    private void assertNotSuspended(String table) {
        Assert.assertFalse(table + " suspended",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(table)));
    }

    private void assertPartitionsReportParquet(String table, int expected) throws Exception {
        final StringSink sink = new StringSink();
        TestUtils.printSql(engine, sqlExecutionContext,
                "SELECT count() FROM table_partitions('" + table + "') WHERE isParquet = true", sink);
        TestUtils.assertEquals("count\n" + expected + "\n", sink);
    }

    private List<String> allFiles(String tablePrefix) throws Exception {
        return filesMatching(tablePrefix, null);
    }

    private List<String> parquetFiles(String tablePrefix) throws Exception {
        return filesMatching(tablePrefix, ".parquet");
    }

    private List<String> filesMatching(String tablePrefix, String suffix) throws Exception {
        final List<String> out = new ArrayList<>();
        final Path root = Paths.get(configuration.getDbRoot());
        try (Stream<Path> walk = Files.walk(root, 5)) {
            for (Path f : walk.filter(p -> !Files.isDirectory(p)).toList()) {
                if (!f.toString().contains(tablePrefix)) {
                    continue;
                }
                if (suffix != null && !f.getFileName().toString().endsWith(suffix)) {
                    continue;
                }
                out.add(root.relativize(f).toString());
            }
        }
        out.sort(String::compareTo);
        return out;
    }
}
