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
 * What COMPOSITE + PARQUET actually does today, MEASURED rather than assumed.
 * <p>
 * This class exists because the recorded picture was wrong in both directions and the existing tests
 * could not tell. {@code CompositeUnsupportedOpsTest#testConvertPartitionToParquetIsNoLongerGated}
 * asserts only that the table is not SUSPENDED afterwards -- which a silent no-op satisfies exactly as
 * well as a real conversion -- so "CONVERT is supported" rested on an assertion that could not
 * distinguish the two. In the other direction, the project's own notes had the whole PARQUET family
 * recorded as unimplemented and blocking cold storage.
 * <p>
 * MEASURED 2026-08-26. Both halves were wrong:
 * <ul>
 *   <li>{@code CONVERT PARTITION TO PARQUET} genuinely works, per CELL -- a two-cell day yields TWO
 *       parquet files and two partitions reporting {@code isParquet}.</li>
 *   <li>An O3 INSERT into a converted cell <b>SUSPENDS the table and loses the row</b>:
 *       <pre>
 *         plain      suspended=false  count=4   (row landed)
 *         composite  suspended=true   count=3   (row lost)
 *         wal errorMessage: composite partitioning does not yet support FORMAT PARQUET
 *       </pre></li>
 * </ul>
 * <p>
 * <b>Why that is worse than a refusal.</b> The INSERT returns SUCCESS to the client; the failure
 * surfaces later, inside the WAL apply job, as a suspension. That breaks the invariant that a refusal
 * fires at the statement which caused it, and it is reached by exactly the sequence cold storage
 * performs -- tier old partitions to parquet, keep ingesting. A user following that workflow bricks
 * the table on the first late-arriving row.
 * <p>
 * These tests PIN current behaviour rather than assert desired behaviour, so the gap is recorded and
 * any change to it is noticed. {@link #testO3IntoConvertedCellSuspendsAndLosesTheRow} is the one to
 * invert when the composite O3 path learns to write into a parquet cell.
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

    /** PINS THE GAP. Invert when the composite O3 path can write into a parquet cell. */
    @Test
    public void testO3IntoConvertedCellSuspendsAndLosesTheRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c2 (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            execute("INSERT INTO c2 VALUES "
                    + "('2023-09-01T02:00:00.000000Z','BTC',1.0),"
                    + "('2023-09-01T03:00:00.000000Z','ETH',2.0),"
                    + "('2023-09-02T01:00:00.000000Z','BTC',3.0)");
            drainWalQueue();
            execute("ALTER TABLE c2 CONVERT PARTITION TO PARQUET LIST '2023-09-01'");
            drainWalQueue();
            assertNotSuspended("c2");

            // O3: BEFORE the existing rows of a now-parquet cell. The INSERT itself SUCCEEDS.
            execute("INSERT INTO c2 VALUES ('2023-09-01T01:00:00.000000Z','BTC',4.0)");
            drainWalQueue();

            final StringSink sink = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext,
                    "SELECT suspended, errorMessage FROM wal_tables() WHERE name = 'c2'", sink);
            TestUtils.assertContains(sink, "true");
            TestUtils.assertContains(sink,
                    "composite partitioning does not yet support FORMAT PARQUET");

            // and the row is GONE -- 3, not 4
            final StringSink count = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext, "SELECT count() FROM c2", count);
            TestUtils.assertContains(count, "3");
        });
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
