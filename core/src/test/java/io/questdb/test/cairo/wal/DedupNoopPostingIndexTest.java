/*+*****************************************************************************
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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.std.NumericException;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

/**
 * A commit whose O3 rows the dedup drops entirely leaves the partition as it is: the O3 partition task
 * reports the partition unchanged ({@code newPartitionSize = -1}, {@code partitionMutates = 0}), and the
 * TableWriter applies nothing for it. The POSTING index of such a partition has to stay as it is, too.
 * Rebuilding it from the column data reads and re-indexes the whole partition column on every commit that
 * re-sends rows the table already has.
 */
public class DedupNoopPostingIndexTest extends AbstractCairoTest {

    @Test
    public void testDuplicateLagRowsLeaveNoStaleIndexEntries() throws Exception {
        assertMemoryLeak(() -> {
            // maxUncommittedRows caps both the WAL lag (20 rows) and the transaction block size, so the
            // transactions are applied one by one, and a transaction that the next one overlaps stays in the
            // WAL lag
            execute("""
                    CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL
                    WITH maxUncommittedRows=1
                    DEDUP SKIP KEYS(ts, sym)
                    """);
            execute("""
                    INSERT INTO t
                    SELECT timestamp_sequence('2024-01-02T10:00:00', 1_000_000), CASE WHEN x % 2 = 0 THEN 'a' ELSE 'b' END, x
                    FROM long_sequence(30)
                    """);
            drainWalQueue();

            // The first transaction stays in the WAL lag, as the second one starts before it. Both carry only
            // duplicates of the partition rows, so the commit of the second one drops the lag rows and its own
            // rows, and leaves the partition as it is.
            execute("""
                    INSERT INTO t
                    SELECT timestamp_sequence('2024-01-02T10:00:10', 1_000_000), CASE WHEN (x + 10) % 2 = 0 THEN 'a' ELSE 'b' END, 100 + x
                    FROM long_sequence(15)
                    """);
            execute("""
                    INSERT INTO t
                    SELECT timestamp_sequence('2024-01-02T10:00:05', 1_000_000), CASE WHEN (x + 5) % 2 = 0 THEN 'a' ELSE 'b' END, 200 + x
                    FROM long_sequence(10)
                    """);
            drainWalQueue();

            // the new rows take the row ids of the dropped lag rows, with another symbol
            execute("""
                    INSERT INTO t
                    SELECT timestamp_sequence('2024-01-02T11:00:00', 1_000_000), 'c', 300 + x
                    FROM long_sequence(15)
                    """);
            drainWalQueue();

            assertQuery("SELECT sym, count(), sum(v) FROM t WHERE sym = 'a'")
                    .noLeakCheck()
                    .withPlanContaining("Index forward scan on: sym")
                    .expectSize()
                    .returns("""
                            sym\tcount\tsum
                            a\t15\t240
                            """);
            assertQuery("SELECT sym, count(), sum(v) FROM t WHERE sym = 'b'")
                    .noLeakCheck()
                    .withPlanContaining("Index forward scan on: sym")
                    .expectSize()
                    .returns("""
                            sym\tcount\tsum
                            b\t15\t225
                            """);
            assertQuery("SELECT sym, count(), sum(v) FROM t WHERE sym = 'c'")
                    .noLeakCheck()
                    .withPlanContaining("Index forward scan on: sym")
                    .expectSize()
                    .returns("""
                            sym\tcount\tsum
                            c\t15\t4620
                            """);
        });
    }

    @Test
    public void testDuplicatesUndoPartitionSplit() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 << 10);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, sym)");
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01', 60_000_000L), 'k' || (x % 10), x FROM long_sequence(1_320)");
            drainWalQueue();

            // Duplicates of the rows from 20:01 on: the O3 job splits the partition after the large prefix of rows
            // up to 20:00, then the dedup drops every row and the job undoes the split.
            assertDuplicatesKeepIndex(
                    "2024-01-01",
                    "INSERT INTO t SELECT timestamp_sequence('2024-01-01T20:01', 60_000_000L), 'k' || ((1_201 + x) % 10), -x FROM long_sequence(119)"
            );

            assertQuery("SELECT count(), min(v), max(v) FROM t WHERE sym = 'k2'")
                    .noLeakCheck()
                    .withPlanContaining("Index forward scan on: sym")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tmin\tmax
                            132\t2\t1312
                            """);
        });
    }

    @Test
    public void testMergedPartitionResealedNextToUnchangedPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (v), v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, sym)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T01:00:00.000000Z', 'a', 1),
                    ('2024-01-01T02:00:00.000000Z', 'b', 2),
                    ('2024-01-02T01:00:00.000000Z', 'a', 3),
                    ('2024-01-02T03:00:00.000000Z', 'a', 4),
                    ('2024-01-03T01:00:00.000000Z', 'a', 5)
                    """);
            drainWalQueue();
            final Set<String> indexFiles = listIndexFiles("2024-01-01");

            // one commit with a duplicate for the first partition and a new row inside the second one, which the
            // O3 job merges into a new version of the partition
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T01:00:00.000000Z', 'a', 10),
                    ('2024-01-02T02:00:00.000000Z', 'x', 30)
                    """);
            drainWalQueue();
            final Set<String> indexFilesAfterCommit = listIndexFiles("2024-01-01");

            // the covering index of the merged partition carries the value of the new row
            assertQuery("SELECT ts, v FROM t WHERE sym = 'x'")
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex on: sym")
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-01-02T02:00:00.000000Z\t30
                            """);

            // a row appended to the last partition after the merge is indexed in the last partition
            execute("INSERT INTO t VALUES ('2024-01-03T05:00:00.000000Z', 'x', 50)");
            drainWalQueue();
            assertQuery("SELECT ts, v FROM t WHERE sym = 'x'")
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex on: sym")
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-01-02T02:00:00.000000Z\t30
                            2024-01-03T05:00:00.000000Z\t50
                            """);
            assertQuery("SELECT ts, v FROM t WHERE sym = 'a'")
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex on: sym")
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-01-01T01:00:00.000000Z\t1
                            2024-01-02T01:00:00.000000Z\t3
                            2024-01-02T03:00:00.000000Z\t4
                            2024-01-03T01:00:00.000000Z\t5
                            """);

            // the commit left the index of the first partition as it was
            Assert.assertEquals(indexFiles, indexFilesAfterCommit);
        });
    }

    @Test
    public void testSkipDuplicatesKeepCoveringIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (v), v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, sym)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T01:00:00.000000Z', 'a', 1),
                    ('2024-01-01T02:00:00.000000Z', 'b', 2),
                    ('2024-01-01T03:00:00.000000Z', 'a', 3),
                    ('2024-01-02T01:00:00.000000Z', 'a', 4)
                    """);
            drainWalQueue();

            for (int i = 0; i < 2; i++) {
                assertDuplicatesKeepIndex(
                        "2024-01-01",
                        "INSERT INTO t VALUES ('2024-01-01T01:00:00.000000Z', 'a', " + (10 + i) + "), ('2024-01-01T03:00:00.000000Z', 'a', " + (20 + i) + ")"
                );
            }

            assertQuery("SELECT ts, v FROM t WHERE sym = 'a'")
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex on: sym")
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-01-01T01:00:00.000000Z\t1
                            2024-01-01T03:00:00.000000Z\t3
                            2024-01-02T01:00:00.000000Z\t4
                            """);
        });
    }

    @Test
    public void testSkipDuplicatesKeepCoveringIndexOfLastPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (v), tag SYMBOL INDEX TYPE POSTING, v LONG)
                    TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, sym)
                    """);
            // separate commits, so that the indexes of the last partition carry more than one generation
            execute("INSERT INTO t VALUES ('2024-01-01T01:00:00.000000Z', 'a', 'x', 1), ('2024-01-02T01:00:00.000000Z', 'a', 'x', 2)");
            drainWalQueue();
            execute("INSERT INTO t VALUES ('2024-01-02T02:00:00.000000Z', 'b', 'y', 3)");
            drainWalQueue();
            execute("INSERT INTO t VALUES ('2024-01-02T03:00:00.000000Z', 'a', 'y', 4)");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("t");
            final String partitions = readTxnToString(tableToken, false, true, true, true);
            final Set<String> indexFiles = listIndexFiles("2024-01-02");

            // duplicates of rows of the last partition, with other values in the columns that are not keys
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-02T01:00:00.000000Z', 'a', 'z', 20),
                    ('2024-01-02T03:00:00.000000Z', 'a', 'z', 40)
                    """);
            drainWalQueue();
            final String partitionsAfterCommit = readTxnToString(tableToken, false, true, true, true);
            final Set<String> indexFilesAfterCommit = listIndexFiles("2024-01-02");

            // both indexes of the last partition keep indexing appended rows, before and after the writer
            // reopens the partition
            execute("INSERT INTO t VALUES ('2024-01-02T04:00:00.000000Z', 'a', 'z', 5)");
            drainWalQueue();
            engine.releaseAllWriters();
            execute("INSERT INTO t VALUES ('2024-01-02T05:00:00.000000Z', 'a', 'z', 6)");
            drainWalQueue();

            assertQuery("SELECT ts, v FROM t WHERE sym = 'a'")
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex on: sym")
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-01-01T01:00:00.000000Z\t1
                            2024-01-02T01:00:00.000000Z\t2
                            2024-01-02T03:00:00.000000Z\t4
                            2024-01-02T04:00:00.000000Z\t5
                            2024-01-02T05:00:00.000000Z\t6
                            """);
            assertQuery("SELECT * FROM t WHERE tag = 'z'")
                    .noLeakCheck()
                    .withPlanContaining("Index forward scan on: tag")
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\ttag\tv
                            2024-01-02T04:00:00.000000Z\ta\tz\t5
                            2024-01-02T05:00:00.000000Z\ta\tz\t6
                            """);

            // the commit of duplicates left the partition and both of its indexes as they were
            Assert.assertEquals(partitions, partitionsAfterCommit);
            Assert.assertEquals(indexFiles, indexFilesAfterCommit);
        });
    }

    @Test
    public void testSkipDuplicatesKeepParquetCoveringIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (v), v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, sym)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T01:00:00.000000Z', 'a', 1),
                    ('2024-01-01T02:00:00.000000Z', 'b', 2),
                    ('2024-01-01T03:00:00.000000Z', 'a', 3),
                    ('2024-01-02T01:00:00.000000Z', 'a', 4)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            for (int i = 0; i < 2; i++) {
                assertDuplicatesKeepIndex(
                        "2024-01-01",
                        "INSERT INTO t VALUES ('2024-01-01T01:00:00.000000Z', 'a', " + (10 + i) + "), ('2024-01-01T03:00:00.000000Z', 'a', " + (20 + i) + ")"
                );
            }

            assertQuery("SELECT ts, v FROM t WHERE sym = 'a'")
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex on: sym")
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-01-01T01:00:00.000000Z\t1
                            2024-01-01T03:00:00.000000Z\t3
                            2024-01-02T01:00:00.000000Z\t4
                            """);
        });
    }

    @Test
    public void testSkipDuplicatesKeepParquetPostingIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, sym)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T01:00:00.000000Z', 'a', 1),
                    ('2024-01-01T02:00:00.000000Z', 'b', 2),
                    ('2024-01-01T03:00:00.000000Z', 'a', 3),
                    ('2024-01-02T01:00:00.000000Z', 'a', 4)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            for (int i = 0; i < 2; i++) {
                assertDuplicatesKeepIndex(
                        "2024-01-01",
                        "INSERT INTO t VALUES ('2024-01-01T01:00:00.000000Z', 'a', " + (10 + i) + "), ('2024-01-01T03:00:00.000000Z', 'a', " + (20 + i) + ")"
                );
            }

            assertQuery("SELECT * FROM t WHERE sym = 'a'")
                    .noLeakCheck()
                    .withPlanContaining("Index forward scan on: sym")
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv
                            2024-01-01T01:00:00.000000Z\ta\t1
                            2024-01-01T03:00:00.000000Z\ta\t3
                            2024-01-02T01:00:00.000000Z\ta\t4
                            """);
        });
    }

    @Test
    public void testSkipDuplicatesKeepPostingIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, sym)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T01:00:00.000000Z', 'a', 1),
                    ('2024-01-01T02:00:00.000000Z', 'b', 2),
                    ('2024-01-01T03:00:00.000000Z', 'a', 3),
                    ('2024-01-02T01:00:00.000000Z', 'a', 4)
                    """);
            drainWalQueue();

            // every commit re-sends rows of the older partition, with other values
            for (int i = 0; i < 3; i++) {
                assertDuplicatesKeepIndex(
                        "2024-01-01",
                        "INSERT INTO t VALUES ('2024-01-01T01:00:00.000000Z', 'a', " + (10 + i) + "), ('2024-01-01T03:00:00.000000Z', 'a', " + (20 + i) + ")"
                );
            }

            assertQuery("SELECT * FROM t WHERE sym = 'a'")
                    .noLeakCheck()
                    .withPlanContaining("Index forward scan on: sym")
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv
                            2024-01-01T01:00:00.000000Z\ta\t1
                            2024-01-01T03:00:00.000000Z\ta\t3
                            2024-01-02T01:00:00.000000Z\ta\t4
                            """);
        });
    }

    @Test
    public void testSkipDuplicatesKeepPostingIndexOfLastPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, sym)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T01:00:00.000000Z', 'a', 1),
                    ('2024-01-02T01:00:00.000000Z', 'a', 2),
                    ('2024-01-02T02:00:00.000000Z', 'b', 3),
                    ('2024-01-02T03:00:00.000000Z', 'a', 4)
                    """);
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("t");
            final String partitions = readTxnToString(tableToken, false, true, true, true);
            final Set<String> indexFiles = listIndexFiles("2024-01-02");

            execute("INSERT INTO t VALUES ('2024-01-02T01:00:00.000000Z', 'a', 20), ('2024-01-02T03:00:00.000000Z', 'a', 40)");
            drainWalQueue();
            final String partitionsAfterCommit = readTxnToString(tableToken, false, true, true, true);
            final Set<String> indexFilesAfterCommit = listIndexFiles("2024-01-02");

            // the last partition keeps indexing the rows appended after the commit
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-02T04:00:00.000000Z', 'a', 5),
                    ('2024-01-02T05:00:00.000000Z', 'b', 6)
                    """);
            drainWalQueue();

            assertQuery("SELECT * FROM t WHERE sym = 'a'")
                    .noLeakCheck()
                    .withPlanContaining("Index forward scan on: sym")
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv
                            2024-01-01T01:00:00.000000Z\ta\t1
                            2024-01-02T01:00:00.000000Z\ta\t2
                            2024-01-02T03:00:00.000000Z\ta\t4
                            2024-01-02T04:00:00.000000Z\ta\t5
                            """);
            assertQuery("SELECT * FROM t WHERE sym = 'b'")
                    .noLeakCheck()
                    .withPlanContaining("Index forward scan on: sym")
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv
                            2024-01-02T02:00:00.000000Z\tb\t3
                            2024-01-02T05:00:00.000000Z\tb\t6
                            """);

            // the commit of duplicates left the partition and its index as they were
            Assert.assertEquals(partitions, partitionsAfterCommit);
            Assert.assertEquals(indexFiles, indexFilesAfterCommit);
        });
    }

    @Test
    public void testUpsertIdenticalRowsKeepPostingIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T01:00:00.000000Z', 'a', 1),
                    ('2024-01-01T02:00:00.000000Z', 'b', 2),
                    ('2024-01-01T03:00:00.000000Z', 'a', 3),
                    ('2024-01-02T01:00:00.000000Z', 'a', 4)
                    """);
            drainWalQueue();

            // UPSERT leaves the partition as it is when the duplicates carry the values the partition has
            assertDuplicatesKeepIndex(
                    "2024-01-01",
                    "INSERT INTO t VALUES ('2024-01-01T01:00:00.000000Z', 'a', 1), ('2024-01-01T03:00:00.000000Z', 'a', 3)"
            );

            assertQuery("SELECT * FROM t WHERE sym = 'a'")
                    .noLeakCheck()
                    .withPlanContaining("Index forward scan on: sym")
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv
                            2024-01-01T01:00:00.000000Z\ta\t1
                            2024-01-01T03:00:00.000000Z\ta\t3
                            2024-01-02T01:00:00.000000Z\ta\t4
                            """);
        });
    }

    // Runs the insert of rows that duplicate rows of the partition and asserts that the commit kept both the
    // partition and its POSTING index files as they were
    private static void assertDuplicatesKeepIndex(String partition, String insertSql) throws Exception {
        final TableToken tableToken = engine.verifyTableName("t");
        final String partitions = readTxnToString(tableToken, false, true, true, true);
        final Set<String> indexFiles = listIndexFiles(partition);
        Assert.assertFalse(indexFiles.isEmpty());
        execute(insertSql);
        drainWalQueue();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
        Assert.assertEquals(partitions, readTxnToString(tableToken, false, true, true, true));
        Assert.assertEquals(indexFiles, listIndexFiles(partition));
    }

    // Names of the POSTING index files of every indexed column, e.g. sym.pk, sym.pv.0, sym.pci, sym.pc0.0.1, in
    // the directory of the partition version that the table "t" has
    private static Set<String> listIndexFiles(String partition) throws NumericException {
        final TableToken tableToken = engine.verifyTableName("t");
        final long partitionTimestamp = MicrosTimestampDriver.floor(partition);
        try (TxReader txReader = new TxReader(configuration.getFilesFacade())) {
            final Path path = Path.getThreadLocal(root).concat(tableToken);
            final int tableDirLen = path.size();
            txReader.ofRO(path.concat(TableUtils.TXN_FILE_NAME).$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
            txReader.unsafeLoadAll();
            TableUtils.setPathForNativePartition(
                    path.trimTo(tableDirLen),
                    ColumnType.TIMESTAMP,
                    PartitionBy.DAY,
                    partitionTimestamp,
                    txReader.getPartitionNameTxnByPartitionTimestamp(partitionTimestamp)
            );
            final String[] files = new File(path.toString()).list(
                    (dir, name) -> name.contains(".pk") || name.contains(".pv") || name.contains(".pc")
            );
            Assert.assertNotNull(files);
            return new TreeSet<>(Arrays.asList(files));
        }
    }
}
