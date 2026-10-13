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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Data semantics of DEDUP SKIP: a new row that matches an existing row, or an earlier row of the
 * same commit, on all dedup keys is discarded. The earliest write wins, mirroring DEDUP UPSERT where
 * the latest write wins.
 */
public class DedupSkipTest extends AbstractCairoTest {

    @Test
    public void testSkipAcrossCommitsAppliedOneByOne() throws Exception {
        assertMemoryLeak(() -> assertSkipAcrossCommits(true));
    }

    @Test
    public void testSkipAcrossCommitsAppliedTogether() throws Exception {
        assertMemoryLeak(() -> assertSkipAcrossCommits(false));
    }

    @Test
    public void testSkipAcrossWalWriters() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, sym)");
            final TableToken tableToken = engine.verifyTableName("t");
            final long ts = MicrosTimestampDriver.floor("2024-01-01T00:00:00");
            try (
                    WalWriter w1 = engine.getWalWriter(tableToken);
                    WalWriter w2 = engine.getWalWriter(tableToken)
            ) {
                // The transactions of the two writers interleave, the sequencer orders them as committed.
                appendRow(w1, ts, "a", 1);
                w1.commit();
                appendRow(w2, ts, "a", 2);
                appendRow(w2, ts, "b", 3);
                w2.commit();
                appendRow(w1, ts, "b", 4);
                appendRow(w1, ts + 1_000_000, "a", 5);
                w1.commit();
                appendRow(w2, ts + 1_000_000, "a", 6);
                appendRow(w2, ts, "a", 7);
                appendRow(w2, ts + 2_000_000, "b", 8);
                w2.commit();
            }
            drainWalQueue();

            assertQuery("SELECT * FROM t ORDER BY v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts\tsym\tv
                            2024-01-01T00:00:00.000000Z\ta\t1
                            2024-01-01T00:00:00.000000Z\tb\t3
                            2024-01-01T00:00:01.000000Z\ta\t5
                            2024-01-01T00:00:02.000000Z\tb\t8
                            """);
        });
    }

    @Test
    public void testSkipAgainstWalLag() throws Exception {
        assertMemoryLeak(() -> {
            // maxUncommittedRows caps both the WAL lag (20 rows) and the transaction block size, so the two
            // transactions are applied one by one. The second transaction overlaps the first one, which keeps
            // the first transaction in the WAL lag, then the second transaction merges with the lag rows.
            execute("""
                    CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL
                    WITH maxUncommittedRows=1
                    DEDUP SKIP KEYS(ts, sym)
                    """);
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01T10:00:10', 1_000_000), 'a', x FROM long_sequence(15)");
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01T10:00:05', 1_000_000), 'a', 100 + x FROM long_sequence(10)");
            drainWalQueue();

            assertQuery("SELECT count(), sum(v), min(v), max(v) FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum\tmin\tmax
                            20\t635\t1\t105
                            """);
            assertQuery("SELECT * FROM t WHERE ts BETWEEN '2024-01-01T10:00:08' AND '2024-01-01T10:00:11'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv
                            2024-01-01T10:00:08.000000Z\ta\t104
                            2024-01-01T10:00:09.000000Z\ta\t105
                            2024-01-01T10:00:10.000000Z\ta\t1
                            2024-01-01T10:00:11.000000Z\ta\t2
                            """);
        });
    }

    @Test
    public void testSkipAllDuplicatesThenAppend() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, sym)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 'a', 1),
                    ('2024-01-01T00:00:01.000000Z', 'a', 2)
                    """);
            drainWalQueue();

            // the row overlapping the partition data is a duplicate, the row after the partition data is appended
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:01.000000Z', 'a', 3),
                    ('2024-01-01T00:00:05.000000Z', 'a', 4)
                    """);
            drainWalQueue();

            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv
                            2024-01-01T00:00:00.000000Z\ta\t1
                            2024-01-01T00:00:01.000000Z\ta\t2
                            2024-01-01T00:00:05.000000Z\ta\t4
                            """);
        });
    }

    @Test
    public void testSkipColumnTopKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1),
                    ('2024-01-01T00:00:01.000000Z', 2)
                    """);
            drainWalQueue();

            // the existing rows have the column top for k, i.e. NULL
            execute("ALTER TABLE t ADD COLUMN k INT");
            execute("ALTER TABLE t DEDUP ENABLE SKIP KEYS(ts, k)");
            execute("""
                    INSERT INTO t(ts, v, k) VALUES
                    ('2024-01-01T00:00:00.000000Z', 10, null),
                    ('2024-01-01T00:00:00.000000Z', 11, 5),
                    ('2024-01-01T00:00:01.000000Z', 12, null)
                    """);
            drainWalQueue();

            assertQuery("SELECT * FROM t ORDER BY v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts\tv\tk
                            2024-01-01T00:00:00.000000Z\t1\tnull
                            2024-01-01T00:00:01.000000Z\t2\tnull
                            2024-01-01T00:00:00.000000Z\t11\t5
                            """);
        });
    }

    @Test
    public void testSkipDuplicatesInOlderPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, sym)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T10:00:00.000000Z', 'a', 1),
                    ('2024-01-01T11:00:00.000000Z', 'a', 2),
                    ('2024-01-02T10:00:00.000000Z', 'a', 3),
                    ('2024-01-03T10:00:00.000000Z', 'a', 4)
                    """);
            drainWalQueue();

            // O3 commit into the middle partition, a partition before the data and the last partition
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-02T10:00:00.000000Z', 'a', 10),
                    ('2024-01-02T10:00:00.000000Z', 'b', 11),
                    ('2023-12-31T10:00:00.000000Z', 'a', 12),
                    ('2024-01-01T10:00:00.000000Z', 'a', 13),
                    ('2024-01-03T10:00:00.000000Z', 'a', 14),
                    ('2024-01-03T11:00:00.000000Z', 'a', 15)
                    """);
            drainWalQueue();

            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv
                            2023-12-31T10:00:00.000000Z\ta\t12
                            2024-01-01T10:00:00.000000Z\ta\t1
                            2024-01-01T11:00:00.000000Z\ta\t2
                            2024-01-02T10:00:00.000000Z\ta\t3
                            2024-01-02T10:00:00.000000Z\tb\t11
                            2024-01-03T10:00:00.000000Z\ta\t4
                            2024-01-03T11:00:00.000000Z\ta\t15
                            """);
        });
    }

    @Test
    public void testSkipFormatParquetTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL DEDUP SKIP KEYS(ts, sym)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 'a', 1),
                    ('2024-01-01T01:00:00.000000Z', 'a', 2),
                    ('2024-01-01T02:00:00.000000Z', 'a', 3)
                    """);
            drainWalQueue();
            assertQuery("SELECT name, isParquet FROM table_partitions('t')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            name\tisParquet
                            2024-01-01\ttrue
                            """);

            // a client re-sends rows of the last partition, which is a Parquet one
            assertPartitionsUnchanged("""
                    INSERT INTO t VALUES
                    ('2024-01-01T01:00:00.000000Z', 'a', 20),
                    ('2024-01-01T02:00:00.000000Z', 'a', 30)
                    """);

            // the partition takes new rows, after its last row too
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T01:00:00.000000Z', 'b', 40),
                    ('2024-01-01T02:00:00.000000Z', 'a', 50),
                    ('2024-01-01T03:00:00.000000Z', 'a', 60)
                    """);
            drainWalQueue();

            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv
                            2024-01-01T00:00:00.000000Z\ta\t1
                            2024-01-01T01:00:00.000000Z\ta\t2
                            2024-01-01T01:00:00.000000Z\tb\t40
                            2024-01-01T02:00:00.000000Z\ta\t3
                            2024-01-01T03:00:00.000000Z\ta\t60
                            """);
        });
    }

    @Test
    public void testSkipParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, sym)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 'a', 1),
                    ('2024-01-01T01:00:00.000000Z', 'a', 2),
                    ('2024-01-01T01:00:00.000000Z', 'b', 3),
                    ('2024-01-02T00:00:00.000000Z', 'a', 4)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            assertQuery("SELECT name, isParquet FROM table_partitions('t')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            name\tisParquet
                            2024-01-01\ttrue
                            2024-01-02\tfalse
                            """);

            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T01:00:00.000000Z', 'a', 10),
                    ('2024-01-01T01:00:00.000000Z', 'c', 11),
                    ('2024-01-01T00:00:00.000000Z', 'a', 12),
                    ('2024-01-01T00:00:00.000000Z', 'a', 13)
                    """);
            drainWalQueue();

            // duplicates only, the parquet partition stays as is, though a merge into its only row group rewrites the file
            assertPartitionsUnchanged("""
                    INSERT INTO t VALUES
                    ('2024-01-01T01:00:00.000000Z', 'b', 14),
                    ('2024-01-01T01:00:00.000000Z', 'c', 15)
                    """);

            assertQuery("SELECT * FROM t ORDER BY v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts\tsym\tv
                            2024-01-01T00:00:00.000000Z\ta\t1
                            2024-01-01T01:00:00.000000Z\ta\t2
                            2024-01-01T01:00:00.000000Z\tb\t3
                            2024-01-02T00:00:00.000000Z\ta\t4
                            2024-01-01T01:00:00.000000Z\tc\t11
                            """);
        });
    }

    @Test
    public void testSkipParquetPartitionAllColumnsAreKeys() throws Exception {
        // Every column is a dedup key: the first stage of a merge decodes them all, the second stage of a merge
        // that writes rows has no column left to decode
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, sym)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 'a'),
                    ('2024-01-01T01:00:00.000000Z', 'a'),
                    ('2024-01-01T02:00:00.000000Z', 'a'),
                    ('2024-01-01T03:00:00.000000Z', 'a'),
                    ('2024-01-01T04:00:00.000000Z', 'b'),
                    ('2024-01-01T05:00:00.000000Z', 'b'),
                    ('2024-01-01T06:00:00.000000Z', 'b'),
                    ('2024-01-01T07:00:00.000000Z', 'b'),
                    ('2024-01-02T00:00:00.000000Z', 'a')
                    """);
            drainWalQueue();
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            assertPartitionsUnchanged("INSERT INTO t VALUES ('2024-01-01T05:00:00.000000Z', 'b')");

            // a duplicate in the first row group and a new row in the second one
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T01:00:00.000000Z', 'a'),
                    ('2024-01-01T05:00:00.000000Z', 'x')
                    """);
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));

            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym
                            2024-01-01T00:00:00.000000Z\ta
                            2024-01-01T01:00:00.000000Z\ta
                            2024-01-01T02:00:00.000000Z\ta
                            2024-01-01T03:00:00.000000Z\ta
                            2024-01-01T04:00:00.000000Z\tb
                            2024-01-01T05:00:00.000000Z\tb
                            2024-01-01T05:00:00.000000Z\tx
                            2024-01-01T06:00:00.000000Z\tb
                            2024-01-01T07:00:00.000000Z\tb
                            2024-01-02T00:00:00.000000Z\ta
                            """);
        });
    }

    @Test
    public void testSkipParquetPartitionCoalescedRowGroups() throws Exception {
        // 4 rows per row group: 03:00 is both the last timestamp of the first row group and the first one of the
        // second row group, so a merge of 03:00 rows dedups both row groups as one run and rewrites the file
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, sym)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 'a', 1),
                    ('2024-01-01T01:00:00.000000Z', 'a', 2),
                    ('2024-01-01T02:00:00.000000Z', 'a', 3),
                    ('2024-01-01T03:00:00.000000Z', 'a', 4),
                    ('2024-01-01T03:00:00.000000Z', 'b', 5),
                    ('2024-01-01T04:00:00.000000Z', 'a', 6),
                    ('2024-01-01T05:00:00.000000Z', 'a', 7),
                    ('2024-01-01T06:00:00.000000Z', 'a', 8),
                    ('2024-01-02T00:00:00.000000Z', 'a', 100)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            // the second row group has the key 03:00 b
            assertPartitionsUnchanged("INSERT INTO t VALUES ('2024-01-01T03:00:00.000000Z', 'b', 50)");

            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T03:00:00.000000Z', 'a', 40),
                    ('2024-01-01T03:00:00.000000Z', 'c', 41)
                    """);
            drainWalQueue();

            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv
                            2024-01-01T00:00:00.000000Z\ta\t1
                            2024-01-01T01:00:00.000000Z\ta\t2
                            2024-01-01T02:00:00.000000Z\ta\t3
                            2024-01-01T03:00:00.000000Z\ta\t4
                            2024-01-01T03:00:00.000000Z\tb\t5
                            2024-01-01T03:00:00.000000Z\tc\t41
                            2024-01-01T04:00:00.000000Z\ta\t6
                            2024-01-01T05:00:00.000000Z\ta\t7
                            2024-01-01T06:00:00.000000Z\ta\t8
                            2024-01-02T00:00:00.000000Z\ta\t100
                            """);
        });
    }

    @Test
    public void testSkipParquetPartitionConvertedKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k INT, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, k)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:01.000000Z', 10, 1),
                    ('2024-01-01T00:00:03.000000Z', 30, 3),
                    ('2024-01-01T00:00:05.000000Z', 50, 5),
                    ('2024-01-02T00:00:00.000000Z', 70, 7)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            // The Parquet file keeps the INT keys, the merge converts them to compare them with the VARCHAR
            // keys of the new rows
            execute("ALTER TABLE t ALTER COLUMN k TYPE VARCHAR");
            drainWalQueue();

            assertPartitionsUnchanged("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:03.000000Z', '30', 33),
                    ('2024-01-01T00:00:05.000000Z', '50', 55)
                    """);

            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:03.000000Z', '30', 333),
                    ('2024-01-01T00:00:03.000000Z', '31', 31)
                    """);
            drainWalQueue();

            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tk\tv
                            2024-01-01T00:00:01.000000Z\t10\t1
                            2024-01-01T00:00:03.000000Z\t30\t3
                            2024-01-01T00:00:03.000000Z\t31\t31
                            2024-01-01T00:00:05.000000Z\t50\t5
                            2024-01-02T00:00:00.000000Z\t70\t7
                            """);
        });
    }

    @Test
    public void testSkipParquetPartitionInPlaceUpdate() throws Exception {
        // 4 rows per row group: the Parquet partition has 3 row groups, the merges update the file in place. The
        // dead bytes that the first two updates leave behind would make the third one rewrite the file, the unused
        // ratio of 1.0 keeps it in place.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        assertMemoryLeak(() -> {
            createParquetPartitionTable("t");
            createParquetPartitionTable("u");

            // duplicates in every row group, which the dedup counts as dropped rows
            final long dedupRowCount = getDedupRowCount();
            assertDuplicatesKeepPartitions("""
                    ('2024-01-01T01:00:00.000000Z', 'a', 1001),
                    ('2024-01-01T05:00:00.000000Z', 'b', 1005),
                    ('2024-01-01T10:00:00.000000Z', 'c', 1010)
                    """);
            Assert.assertEquals(dedupRowCount + 3, getDedupRowCount());

            // duplicates in the first two row groups and a new row in the third one
            assertSameAsTwinInPlace(
                    """
                            ('2024-01-01T01:00:00.000000Z', 'a', 2001),
                            ('2024-01-01T05:00:00.000000Z', 'b', 2005)
                            """,
                    "('2024-01-01T09:00:00.000000Z', 'x', 2009)"
            );

            // a new row in the first row group and duplicates in the other two, which the merge meets
            // after it opened the file
            assertSameAsTwinInPlace(
                    """
                            ('2024-01-01T06:00:00.000000Z', 'b', 2506),
                            ('2024-01-01T11:00:00.000000Z', 'c', 2511)
                            """,
                    "('2024-01-01T00:00:00.000000Z', 'x', 2500)"
            );

            // A duplicate in the first row group and a new row after the last one, which adds a row group. The new
            // row group opens the file and goes after the 3 row groups, including the one of the duplicate, which
            // the merge left as it is.
            assertSameAsTwinInPlace(
                    "('2024-01-01T02:00:00.000000Z', 'a', 3002)",
                    "('2024-01-01T23:00:00.000000Z', 'x', 3023)"
            );

            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv
                            2024-01-01T00:00:00.000000Z\ta\t1
                            2024-01-01T00:00:00.000000Z\tx\t2500
                            2024-01-01T01:00:00.000000Z\ta\t2
                            2024-01-01T02:00:00.000000Z\ta\t3
                            2024-01-01T03:00:00.000000Z\ta\t4
                            2024-01-01T04:00:00.000000Z\tb\t5
                            2024-01-01T05:00:00.000000Z\tb\t6
                            2024-01-01T06:00:00.000000Z\tb\t7
                            2024-01-01T07:00:00.000000Z\tb\t8
                            2024-01-01T08:00:00.000000Z\tc\t9
                            2024-01-01T09:00:00.000000Z\tc\t10
                            2024-01-01T09:00:00.000000Z\tx\t2009
                            2024-01-01T10:00:00.000000Z\tc\t11
                            2024-01-01T11:00:00.000000Z\tc\t12
                            2024-01-01T23:00:00.000000Z\tx\t3023
                            2024-01-02T00:00:00.000000Z\ta\t100
                            """);
        });
    }

    @Test
    public void testSkipParquetPartitionInPlaceUpdateSplitsRowGroup() throws Exception {
        // 4 rows per row group: a merge of 7 rows writes 2 row groups, the second one goes after the row group that
        // the merge replaces
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            createParquetPartitionTable("t");
            createParquetPartitionTable("u");

            // a duplicate in the first row group, which stays as it is, and 3 new rows in the second one
            assertSameAsTwinInPlace(
                    "('2024-01-01T01:00:00.000000Z', 'a', 1001)",
                    """
                            ('2024-01-01T04:30:00.000000Z', 'x', 45),
                            ('2024-01-01T05:30:00.000000Z', 'x', 55),
                            ('2024-01-01T06:30:00.000000Z', 'x', 65)
                            """
            );

            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv
                            2024-01-01T00:00:00.000000Z\ta\t1
                            2024-01-01T01:00:00.000000Z\ta\t2
                            2024-01-01T02:00:00.000000Z\ta\t3
                            2024-01-01T03:00:00.000000Z\ta\t4
                            2024-01-01T04:00:00.000000Z\tb\t5
                            2024-01-01T04:30:00.000000Z\tx\t45
                            2024-01-01T05:00:00.000000Z\tb\t6
                            2024-01-01T05:30:00.000000Z\tx\t55
                            2024-01-01T06:00:00.000000Z\tb\t7
                            2024-01-01T06:30:00.000000Z\tx\t65
                            2024-01-01T07:00:00.000000Z\tb\t8
                            2024-01-01T08:00:00.000000Z\tc\t9
                            2024-01-01T09:00:00.000000Z\tc\t10
                            2024-01-01T10:00:00.000000Z\tc\t11
                            2024-01-01T11:00:00.000000Z\tc\t12
                            2024-01-02T00:00:00.000000Z\ta\t100
                            """);
        });
    }

    @Test
    public void testSkipParquetPartitionNextToChangedPartition() throws Exception {
        assertMemoryLeak(() -> {
            // the first two of the three partitions are Parquet ones
            for (String tableName : new String[]{"t", "u"}) {
                execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, sym)");
                execute("""
                        INSERT INTO %s VALUES
                        ('2024-01-01T00:00:00.000000Z', 'a', 1),
                        ('2024-01-01T01:00:00.000000Z', 'a', 2),
                        ('2024-01-02T00:00:00.000000Z', 'a', 3),
                        ('2024-01-02T01:00:00.000000Z', 'a', 4),
                        ('2024-01-03T00:00:00.000000Z', 'a', 5)
                        """.formatted(tableName));
                drainWalQueue();
                execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02'");
                drainWalQueue();
            }

            // one commit with a duplicate for the first partition and a new row for the second one
            assertSameAsTwin(
                    "('2024-01-01T01:00:00.000000Z', 'a', 20)",
                    "('2024-01-02T00:30:00.000000Z', 'b', 40)"
            );

            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv
                            2024-01-01T00:00:00.000000Z\ta\t1
                            2024-01-01T01:00:00.000000Z\ta\t2
                            2024-01-02T00:00:00.000000Z\ta\t3
                            2024-01-02T00:30:00.000000Z\tb\t40
                            2024-01-02T01:00:00.000000Z\ta\t4
                            2024-01-03T00:00:00.000000Z\ta\t5
                            """);
        });
    }

    @Test
    public void testSkipParquetPartitionRewriteAfterColumnTypeChange() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            createParquetPartitionTable("t");
            createParquetPartitionTable("u");
            // The Parquet file keeps v as LONG. A merge that writes rows rewrites the file, and it re-encodes the
            // row groups that it copies, through the decode buffers that the merge of a row group uses too.
            execute("ALTER TABLE t ALTER COLUMN v TYPE DOUBLE");
            execute("ALTER TABLE u ALTER COLUMN v TYPE DOUBLE");
            drainWalQueue();

            assertDuplicatesKeepPartitions("""
                    ('2024-01-01T01:00:00.000000Z', 'a', 1001.0),
                    ('2024-01-01T10:00:00.000000Z', 'c', 1010.0)
                    """);

            // The merge of the third row group decodes its keys before the rewrite opens its file. Then the
            // rewrite re-encodes the first two row groups, and the merge decodes the other columns and writes
            // the third row group.
            assertSameAsTwin(
                    "('2024-01-01T01:00:00.000000Z', 'a', 2001.0)",
                    "('2024-01-01T09:00:00.000000Z', 'x', 2009.0)"
            );

            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv
                            2024-01-01T00:00:00.000000Z\ta\t1.0
                            2024-01-01T01:00:00.000000Z\ta\t2.0
                            2024-01-01T02:00:00.000000Z\ta\t3.0
                            2024-01-01T03:00:00.000000Z\ta\t4.0
                            2024-01-01T04:00:00.000000Z\tb\t5.0
                            2024-01-01T05:00:00.000000Z\tb\t6.0
                            2024-01-01T06:00:00.000000Z\tb\t7.0
                            2024-01-01T07:00:00.000000Z\tb\t8.0
                            2024-01-01T08:00:00.000000Z\tc\t9.0
                            2024-01-01T09:00:00.000000Z\tc\t10.0
                            2024-01-01T09:00:00.000000Z\tx\t2009.0
                            2024-01-01T10:00:00.000000Z\tc\t11.0
                            2024-01-01T11:00:00.000000Z\tc\t12.0
                            2024-01-02T00:00:00.000000Z\ta\t100.0
                            """);
        });
    }

    @Test
    public void testSkipParquetPartitionRewriteCopiesCoalescedRowGroups() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            createParquetPartitionTableWithBoundaryTie(7);
            final TableToken tableToken = engine.verifyTableName("t");
            final Set<String> partitionDirs = listPartitionDirs(tableToken);

            // A new row in the first row group and a duplicate at 07:00, where the second and the third row
            // group meet. The rewrite opens its file at the first row group, then copies both row groups of the
            // duplicate as they are.
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:30:00.000000Z', 'x', 30),
                    ('2024-01-01T07:00:00.000000Z', 'b', 70)
                    """);
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            // the rewrite wrote a new partition directory
            Assert.assertFalse(partitionDirs.containsAll(listPartitionDirs(tableToken)));

            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv
                            2024-01-01T00:00:00.000000Z\ta\t1
                            2024-01-01T00:30:00.000000Z\tx\t30
                            2024-01-01T01:00:00.000000Z\ta\t2
                            2024-01-01T02:00:00.000000Z\ta\t3
                            2024-01-01T03:00:00.000000Z\ta\t4
                            2024-01-01T04:00:00.000000Z\ta\t5
                            2024-01-01T05:00:00.000000Z\ta\t6
                            2024-01-01T06:00:00.000000Z\ta\t7
                            2024-01-01T07:00:00.000000Z\ta\t8
                            2024-01-01T07:00:00.000000Z\tb\t20
                            2024-01-01T08:00:00.000000Z\ta\t9
                            2024-01-01T09:00:00.000000Z\ta\t10
                            2024-01-01T10:00:00.000000Z\ta\t11
                            2024-01-02T00:00:00.000000Z\ta\t100
                            """);
        });
    }

    @Test
    public void testSkipParquetPartitionRewriteCopiesUnchangedMerge() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            createRewrittenParquetPartitionTables();

            // A new row in the first row group, duplicates in the other two. The rewrite opens its file at the
            // first row group and copies the other two as they are.
            assertSameAsTwin(
                    """
                            ('2024-01-01T05:00:00.000000Z', 'b', 2005, 5),
                            ('2024-01-01T10:00:00.000000Z', 'c', 2010, 10)
                            """,
                    "('2024-01-01T02:00:00.000000Z', 'x', 2002, 2)"
            );

            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv\tw
                            2024-01-01T00:00:00.000000Z\ta\t1\tnull
                            2024-01-01T01:00:00.000000Z\ta\t2\tnull
                            2024-01-01T02:00:00.000000Z\ta\t3\tnull
                            2024-01-01T02:00:00.000000Z\tx\t2002\t2
                            2024-01-01T03:00:00.000000Z\ta\t4\tnull
                            2024-01-01T04:00:00.000000Z\tb\t5\tnull
                            2024-01-01T05:00:00.000000Z\tb\t6\tnull
                            2024-01-01T06:00:00.000000Z\tb\t7\tnull
                            2024-01-01T07:00:00.000000Z\tb\t8\tnull
                            2024-01-01T08:00:00.000000Z\tc\t9\tnull
                            2024-01-01T09:00:00.000000Z\tc\t10\tnull
                            2024-01-01T10:00:00.000000Z\tc\t11\tnull
                            2024-01-01T11:00:00.000000Z\tc\t12\tnull
                            2024-01-02T00:00:00.000000Z\ta\t100\tnull
                            """);
        });
    }

    @Test
    public void testSkipParquetPartitionRewriteFailsToCopy() throws Exception {
        // the rewrite opens its file, then fails at its first write: the copy of the first row group, which the
        // merge left as it is before it decided to write the third row group
        assertRewriteFailureRecovers(true);
    }

    @Test
    public void testSkipParquetPartitionRewriteFailsToOpen() throws Exception {
        // the rewrite creates its directory, then fails to open its file
        assertRewriteFailureRecovers(false);
    }

    @Test
    public void testSkipParquetPartitionRewriteOpensAfterCoalescedRowGroups() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            createParquetPartitionTableWithBoundaryTie(3);
            final TableToken tableToken = engine.verifyTableName("t");
            final Set<String> partitionDirs = listPartitionDirs(tableToken);

            // A duplicate at 03:00, where the first and the second row group meet, and a new row in the third
            // row group. The rewrite opens its file at the third row group, after it copies both row groups of
            // the duplicate as they are.
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T03:00:00.000000Z', 'b', 30),
                    ('2024-01-01T08:30:00.000000Z', 'x', 85)
                    """);
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            // the rewrite wrote a new partition directory
            Assert.assertFalse(partitionDirs.containsAll(listPartitionDirs(tableToken)));

            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv
                            2024-01-01T00:00:00.000000Z\ta\t1
                            2024-01-01T01:00:00.000000Z\ta\t2
                            2024-01-01T02:00:00.000000Z\ta\t3
                            2024-01-01T03:00:00.000000Z\ta\t4
                            2024-01-01T03:00:00.000000Z\tb\t20
                            2024-01-01T04:00:00.000000Z\ta\t5
                            2024-01-01T05:00:00.000000Z\ta\t6
                            2024-01-01T06:00:00.000000Z\ta\t7
                            2024-01-01T07:00:00.000000Z\ta\t8
                            2024-01-01T08:00:00.000000Z\ta\t9
                            2024-01-01T08:30:00.000000Z\tx\t85
                            2024-01-01T09:00:00.000000Z\ta\t10
                            2024-01-01T10:00:00.000000Z\ta\t11
                            2024-01-02T00:00:00.000000Z\ta\t100
                            """);
        });
    }

    @Test
    public void testSkipParquetPartitionRewriteOpensAtMerge() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            createRewrittenParquetPartitionTables();

            // A duplicate in the first row group, none in the second one and a new row in the third one. The
            // rewrite opens its file at the third row group, after it copies the first two as they are.
            assertSameAsTwin(
                    "('2024-01-01T01:00:00.000000Z', 'a', 2001, 1)",
                    "('2024-01-01T09:00:00.000000Z', 'x', 2009, 9)"
            );

            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv\tw
                            2024-01-01T00:00:00.000000Z\ta\t1\tnull
                            2024-01-01T01:00:00.000000Z\ta\t2\tnull
                            2024-01-01T02:00:00.000000Z\ta\t3\tnull
                            2024-01-01T03:00:00.000000Z\ta\t4\tnull
                            2024-01-01T04:00:00.000000Z\tb\t5\tnull
                            2024-01-01T05:00:00.000000Z\tb\t6\tnull
                            2024-01-01T06:00:00.000000Z\tb\t7\tnull
                            2024-01-01T07:00:00.000000Z\tb\t8\tnull
                            2024-01-01T08:00:00.000000Z\tc\t9\tnull
                            2024-01-01T09:00:00.000000Z\tc\t10\tnull
                            2024-01-01T09:00:00.000000Z\tx\t2009\t9
                            2024-01-01T10:00:00.000000Z\tc\t11\tnull
                            2024-01-01T11:00:00.000000Z\tc\t12\tnull
                            2024-01-02T00:00:00.000000Z\ta\t100\tnull
                            """);
        });
    }

    @Test
    public void testSkipParquetPartitionRewriteOpensAtNewRowGroup() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            createRewrittenParquetPartitionTables();

            // A duplicate in the second row group and a new row after the last one. The rewrite opens its file
            // at the new row group, after it copies the three row groups as they are.
            assertSameAsTwin(
                    "('2024-01-01T05:00:00.000000Z', 'b', 2005, 5)",
                    "('2024-01-01T23:00:00.000000Z', 'x', 2023, 23)"
            );

            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv\tw
                            2024-01-01T00:00:00.000000Z\ta\t1\tnull
                            2024-01-01T01:00:00.000000Z\ta\t2\tnull
                            2024-01-01T02:00:00.000000Z\ta\t3\tnull
                            2024-01-01T03:00:00.000000Z\ta\t4\tnull
                            2024-01-01T04:00:00.000000Z\tb\t5\tnull
                            2024-01-01T05:00:00.000000Z\tb\t6\tnull
                            2024-01-01T06:00:00.000000Z\tb\t7\tnull
                            2024-01-01T07:00:00.000000Z\tb\t8\tnull
                            2024-01-01T08:00:00.000000Z\tc\t9\tnull
                            2024-01-01T09:00:00.000000Z\tc\t10\tnull
                            2024-01-01T10:00:00.000000Z\tc\t11\tnull
                            2024-01-01T11:00:00.000000Z\tc\t12\tnull
                            2024-01-01T23:00:00.000000Z\tx\t2023\t23
                            2024-01-02T00:00:00.000000Z\ta\t100\tnull
                            """);
        });
    }

    @Test
    public void testSkipParquetPartitionTimestampOnlyKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1),
                    ('2024-01-01T01:00:00.000000Z', 2),
                    ('2024-01-02T00:00:00.000000Z', 3)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T01:00:00.000000Z', 10),
                    ('2024-01-01T00:30:00.000000Z', 11),
                    ('2024-01-01T00:00:00.000000Z', 12)
                    """);
            drainWalQueue();

            // duplicates only, the parquet partition stays as is
            assertPartitionsUnchanged("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:30:00.000000Z', 21),
                    ('2024-01-01T01:00:00.000000Z', 20)
                    """);

            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-01-01T00:00:00.000000Z\t1
                            2024-01-01T00:30:00.000000Z\t11
                            2024-01-01T01:00:00.000000Z\t2
                            2024-01-02T00:00:00.000000Z\t3
                            """);
        });
    }

    @Test
    public void testSkipParquetPartitionUpdateFailsToWrite() throws Exception {
        // 4 rows per row group: the Parquet partition has 3 row groups, the merge updates the file in place
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        final AtomicBoolean isArmed = new AtomicBoolean();
        assertMemoryLeak(newParquetWriteFailureFacade(isArmed, false), () -> {
            createParquetPartitionTable("t");
            final TableToken tableToken = engine.verifyTableName("t");
            final String partitions = readTxnToString(tableToken, false, true, true, true);
            final Set<String> partitionDirs = listPartitionDirs(tableToken);

            // A duplicate in the first row group and a new row in the third one. The update opens the Parquet file
            // at the third row group, then fails to write to it, which suspends the table and leaves the partition
            // as it was.
            isArmed.set(true);
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T01:00:00.000000Z', 'a', 2001),
                    ('2024-01-01T09:00:00.000000Z', 'x', 2009)
                    """);
            drainWalQueue();
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertEquals(partitions, readTxnToString(tableToken, false, true, true, true));

            // once the table resumes, the merge updates the file in place
            isArmed.set(false);
            execute("ALTER TABLE t RESUME WAL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertTrue(partitionDirs.containsAll(listPartitionDirs(tableToken)));

            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv
                            2024-01-01T00:00:00.000000Z\ta\t1
                            2024-01-01T01:00:00.000000Z\ta\t2
                            2024-01-01T02:00:00.000000Z\ta\t3
                            2024-01-01T03:00:00.000000Z\ta\t4
                            2024-01-01T04:00:00.000000Z\tb\t5
                            2024-01-01T05:00:00.000000Z\tb\t6
                            2024-01-01T06:00:00.000000Z\tb\t7
                            2024-01-01T07:00:00.000000Z\tb\t8
                            2024-01-01T08:00:00.000000Z\tc\t9
                            2024-01-01T09:00:00.000000Z\tc\t10
                            2024-01-01T09:00:00.000000Z\tx\t2009
                            2024-01-01T10:00:00.000000Z\tc\t11
                            2024-01-01T11:00:00.000000Z\tc\t12
                            2024-01-02T00:00:00.000000Z\ta\t100
                            """);
        });
    }

    @Test
    public void testSkipPreExistingDuplicates() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 'a', 1),
                    ('2024-01-01T00:00:00.000000Z', 'a', 2)
                    """);
            drainWalQueue();

            // dedup enabled after the duplicates were written, SKIP leaves them in place
            execute("ALTER TABLE t DEDUP ENABLE SKIP KEYS(ts, sym)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 'a', 3),
                    ('2024-01-01T00:00:00.000000Z', 'b', 4)
                    """);
            drainWalQueue();

            assertQuery("SELECT * FROM t ORDER BY v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts\tsym\tv
                            2024-01-01T00:00:00.000000Z\ta\t1
                            2024-01-01T00:00:00.000000Z\ta\t2
                            2024-01-01T00:00:00.000000Z\tb\t4
                            """);
        });
    }

    @Test
    public void testSkipTimestampOnlyKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts)");
            // a duplicate within the commit
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 1),
                    ('2024-01-01T00:00:01.000000Z', 2),
                    ('2024-01-01T00:00:00.000000Z', 3)
                    """);
            drainWalQueue();

            // duplicates of the partition data, only
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:01.000000Z', 4),
                    ('2024-01-01T00:00:02.000000Z', 5),
                    ('2024-01-01T00:00:00.000000Z', 6)
                    """);
            drainWalQueue();

            // duplicates of the partition data mixed with a new row in between them
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:01.000000Z', 12),
                    ('2024-01-01T00:00:00.500000Z', 13),
                    ('2024-01-01T00:00:00.000000Z', 14)
                    """);
            drainWalQueue();

            // duplicates across transactions applied together
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:02.000000Z', 7),
                    ('2024-01-01T00:00:03.000000Z', 8)
                    """);
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:03.000000Z', 9)");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:04.000000Z', 10), ('2024-01-01T00:00:04.000000Z', 11)");
            drainWalQueue();

            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-01-01T00:00:00.000000Z\t1
                            2024-01-01T00:00:00.500000Z\t13
                            2024-01-01T00:00:01.000000Z\t2
                            2024-01-01T00:00:02.000000Z\t5
                            2024-01-01T00:00:03.000000Z\t8
                            2024-01-01T00:00:04.000000Z\t10
                            """);
        });
    }

    @Test
    public void testSkipVarSizeKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k1 VARCHAR, k2 STRING, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, k1, k2)");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 'a', 'x', 1),
                    ('2024-01-01T00:00:00.000000Z', 'a', 'y', 2),
                    ('2024-01-01T00:00:00.000000Z', null, 'x', 3),
                    ('2024-01-01T00:00:00.000000Z', 'a', 'x', 4)
                    """);
            drainWalQueue();

            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 'a', 'y', 5),
                    ('2024-01-01T00:00:00.000000Z', null, 'x', 6),
                    ('2024-01-01T00:00:00.000000Z', null, null, 7),
                    ('2024-01-01T00:00:00.000000Z', 'a long varchar value that is not inlined', null, 8),
                    ('2024-01-01T00:00:00.000000Z', 'a long varchar value that is not inlined', null, 9)
                    """);
            drainWalQueue();

            assertQuery("SELECT * FROM t ORDER BY v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts\tk1\tk2\tv
                            2024-01-01T00:00:00.000000Z\ta\tx\t1
                            2024-01-01T00:00:00.000000Z\ta\ty\t2
                            2024-01-01T00:00:00.000000Z\t\tx\t3
                            2024-01-01T00:00:00.000000Z\t\t\t7
                            2024-01-01T00:00:00.000000Z\ta long varchar value that is not inlined\t\t8
                            """);
        });
    }

    @Test
    public void testSkipWithinCommit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, sym)");
            // in-order commit
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 'a', 1),
                    ('2024-01-01T00:00:00.000000Z', 'b', 2),
                    ('2024-01-01T00:00:00.000000Z', 'a', 3),
                    ('2024-01-01T00:00:01.000000Z', 'b', 4),
                    ('2024-01-01T00:00:01.000000Z', 'b', 5)
                    """);
            drainWalQueue();
            // out-of-order commit, no overlap with the partition data
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:03.000000Z', 'a', 6),
                    ('2024-01-01T00:00:02.000000Z', 'a', 7),
                    ('2024-01-01T00:00:03.000000Z', 'a', 8),
                    ('2024-01-01T00:00:02.000000Z', 'a', 9)
                    """);
            drainWalQueue();

            assertQuery("SELECT * FROM t ORDER BY v")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts\tsym\tv
                            2024-01-01T00:00:00.000000Z\ta\t1
                            2024-01-01T00:00:00.000000Z\tb\t2
                            2024-01-01T00:00:01.000000Z\tb\t4
                            2024-01-01T00:00:03.000000Z\ta\t6
                            2024-01-01T00:00:02.000000Z\ta\t7
                            """);
        });
    }

    @Test
    public void testSwitchDedupMode() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 'a', 1)");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 'a', 2)");
            drainWalQueue();
            assertValue(2);

            execute("ALTER TABLE t DEDUP ENABLE SKIP KEYS(ts, sym)");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 'a', 3)");
            drainWalQueue();
            assertValue(2);

            // ENABLE is optional
            execute("ALTER TABLE t DEDUP UPSERT KEYS(ts, sym)");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 'a', 4)");
            drainWalQueue();
            assertValue(4);

            // every transaction is applied with the dedup mode in effect when it was committed
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 'a', 5)");
            execute("ALTER TABLE t DEDUP SKIP KEYS(ts, sym)");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 'a', 6)");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 'a', 7)");
            drainWalQueue();
            assertValue(5);

            execute("ALTER TABLE t DEDUP UPSERT KEYS(ts, sym)");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 'a', 8)");
            execute("ALTER TABLE t DEDUP DISABLE");
            execute("ALTER TABLE t DEDUP ENABLE SKIP KEYS(ts, sym)");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 'a', 9)");
            drainWalQueue();
            assertValue(8);
        });
    }

    private static void appendRow(WalWriter writer, long timestamp, String sym, long value) {
        TableWriter.Row row = writer.newRow(timestamp);
        row.putSym(1, sym);
        row.putLong(2, value);
        row.append();
    }

    // The number of rows that the dedup of the table "t" dropped since the start
    private static long getDedupRowCount() throws SqlException {
        try (
                RecordCursorFactory factory = select("SELECT wal_dedup_row_count_since_start FROM tables() WHERE table_name = 't'");
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(cursor.hasNext());
            return cursor.getRecord().getLong(0);
        }
    }

    // Names of the directories of the 2024-01-01 partition, of every version that is on disk
    private static Set<String> listPartitionDirs(TableToken tableToken) {
        final String[] dirs = new File(root, tableToken.getDirName()).list((dir, name) -> name.startsWith("2024-01-01"));
        Assert.assertNotNull(dirs);
        return new HashSet<>(Arrays.asList(dirs));
    }

    // A FilesFacade that, once armed, fails to open a Parquet file for writing, or opens it read-only so that the
    // first write to it fails
    private static FilesFacade newParquetWriteFailureFacade(AtomicBoolean isArmed, boolean isOpenFailure) {
        return new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isArmed.get() && Utf8s.endsWithAscii(name, TableUtils.PARQUET_PARTITION_NAME)) {
                    if (isOpenFailure) {
                        return -1;
                    }
                    // a read-only descriptor: the updater takes it, its first write fails
                    super.close(super.openRW(name, opts));
                    return super.openRONoCache(name);
                }
                return super.openRW(name, opts);
            }
        };
    }

    // Inserts the rows, duplicates of rows that the table "t" and its twin "u" have, into both tables, then
    // asserts that both tables kept every partition as it was
    private void assertDuplicatesKeepPartitions(String duplicateRows) throws SqlException {
        final TableToken tableToken = engine.verifyTableName("t");
        final TableToken twinToken = engine.verifyTableName("u");
        final String partitions = readTxnToString(tableToken, false, true, true, true);
        final String twinPartitions = readTxnToString(twinToken, false, true, true, true);
        execute("INSERT INTO t VALUES " + duplicateRows);
        execute("INSERT INTO u VALUES " + duplicateRows);
        drainWalQueue();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(twinToken));
        Assert.assertEquals(partitions, readTxnToString(tableToken, false, true, true, true));
        Assert.assertEquals(twinPartitions, readTxnToString(twinToken, false, true, true, true));
    }

    // Runs the statement on the table "t" and asserts that it changed no partition: neither its row count, nor
    // its name txn, nor the size of its Parquet file, nor the column versions.
    private void assertPartitionsUnchanged(String sql) throws SqlException {
        final TableToken tableToken = engine.verifyTableName("t");
        final String partitions = readTxnToString(tableToken, false, true, true, true);
        execute(sql);
        drainWalQueue();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
        Assert.assertEquals(partitions, readTxnToString(tableToken, false, true, true, true));
    }

    // A SKIP merge with a duplicate in the first row group and a new row in the third one opens the output of a
    // rewrite at the third row group. The rewrite fails, which suspends the table and removes the directory that
    // the rewrite created. Once the table resumes, the merge succeeds.
    private void assertRewriteFailureRecovers(boolean isCopyFailure) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        final AtomicBoolean isArmed = new AtomicBoolean();
        assertMemoryLeak(newParquetWriteFailureFacade(isArmed, !isCopyFailure), () -> {
            createParquetPartitionTable("t");
            // The Parquet file lacks the new column, so a merge that writes rows rewrites the file
            execute("ALTER TABLE t ADD COLUMN w INT");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("t");
            final Set<String> partitionDirs = listPartitionDirs(tableToken);

            isArmed.set(true);
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T01:00:00.000000Z', 'a', 2001, 1),
                    ('2024-01-01T09:00:00.000000Z', 'x', 2009, 9)
                    """);
            drainWalQueue();
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            // the failed rewrite removed the directory it created
            Assert.assertTrue(partitionDirs.containsAll(listPartitionDirs(tableToken)));

            isArmed.set(false);
            execute("ALTER TABLE t RESUME WAL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));

            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv\tw
                            2024-01-01T00:00:00.000000Z\ta\t1\tnull
                            2024-01-01T01:00:00.000000Z\ta\t2\tnull
                            2024-01-01T02:00:00.000000Z\ta\t3\tnull
                            2024-01-01T03:00:00.000000Z\ta\t4\tnull
                            2024-01-01T04:00:00.000000Z\tb\t5\tnull
                            2024-01-01T05:00:00.000000Z\tb\t6\tnull
                            2024-01-01T06:00:00.000000Z\tb\t7\tnull
                            2024-01-01T07:00:00.000000Z\tb\t8\tnull
                            2024-01-01T08:00:00.000000Z\tc\t9\tnull
                            2024-01-01T09:00:00.000000Z\tc\t10\tnull
                            2024-01-01T09:00:00.000000Z\tx\t2009\t9
                            2024-01-01T10:00:00.000000Z\tc\t11\tnull
                            2024-01-01T11:00:00.000000Z\tc\t12\tnull
                            2024-01-02T00:00:00.000000Z\ta\t100\tnull
                            """);
        });
    }

    // Inserts the new rows with duplicates of stored rows into the table "t", and the new rows alone into its
    // twin "u", which has the same rows. SKIP drops the duplicates and writes none of the row groups that hold
    // their keys, so both tables must end up with the same partitions, down to the name txn and the size of
    // the Parquet file of each. A Parquet file holds the table name, which has the same length in both tables.
    private void assertSameAsTwin(String duplicateRows, String newRows) throws SqlException {
        final TableToken tableToken = engine.verifyTableName("t");
        final TableToken twinToken = engine.verifyTableName("u");
        execute("INSERT INTO t VALUES " + duplicateRows + ", " + newRows);
        execute("INSERT INTO u VALUES " + newRows);
        drainWalQueue();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(twinToken));
        Assert.assertEquals(readTxnToString(twinToken, false, true, true, true), readTxnToString(tableToken, false, true, true, true));
    }

    // assertSameAsTwin() for a merge that updates the Parquet file of the table "t" in place: a rewrite would
    // write a new partition directory
    private void assertSameAsTwinInPlace(String duplicateRows, String newRows) throws SqlException {
        final TableToken tableToken = engine.verifyTableName("t");
        final Set<String> partitionDirs = listPartitionDirs(tableToken);
        assertSameAsTwin(duplicateRows, newRows);
        Assert.assertTrue(partitionDirs.containsAll(listPartitionDirs(tableToken)));
    }

    private void assertSkipAcrossCommits(boolean drainEachCommit) throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, sym)");
        execute("""
                INSERT INTO t VALUES
                ('2024-01-01T00:00:00.000000Z', 'a', 1),
                ('2024-01-01T00:00:00.000000Z', 'b', 2),
                ('2024-01-01T00:00:01.000000Z', 'a', 3)
                """);
        if (drainEachCommit) {
            drainWalQueue();
        }
        execute("""
                INSERT INTO t VALUES
                ('2024-01-01T00:00:00.000000Z', 'a', 10),
                ('2024-01-01T00:00:00.000000Z', 'c', 11),
                ('2024-01-01T00:00:01.000000Z', 'a', 12),
                ('2024-01-01T00:00:02.000000Z', 'a', 13)
                """);
        if (drainEachCommit) {
            drainWalQueue();
        }
        execute("""
                INSERT INTO t VALUES
                ('2024-01-01T00:00:02.000000Z', 'a', 20),
                ('2024-01-01T00:00:00.000000Z', 'b', 21),
                ('2023-12-31T00:00:00.000000Z', 'z', 22)
                """);
        drainWalQueue();

        assertQuery("SELECT * FROM t ORDER BY v")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        ts\tsym\tv
                        2024-01-01T00:00:00.000000Z\ta\t1
                        2024-01-01T00:00:00.000000Z\tb\t2
                        2024-01-01T00:00:01.000000Z\ta\t3
                        2024-01-01T00:00:00.000000Z\tc\t11
                        2024-01-01T00:00:02.000000Z\ta\t13
                        2023-12-31T00:00:00.000000Z\tz\t22
                        """);
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
    }

    private void assertValue(long expected) throws Exception {
        assertQuery("SELECT * FROM t")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns("ts\tsym\tv\n2024-01-01T00:00:00.000000Z\ta\t" + expected + "\n");
    }

    // Creates the table with 12 rows on 2024-01-01, one per hour, which it converts to a Parquet partition, and
    // one row on 2024-01-02, in a native partition. With 4 rows per row group, each row group of the Parquet
    // partition has its own symbol: a, b and c.
    private void createParquetPartitionTable(String tableName) throws SqlException {
        execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, sym)");
        execute("""
                INSERT INTO %s
                SELECT '2024-01-01'::TIMESTAMP + (x - 1) * 3_600_000_000, CASE WHEN x <= 4 THEN 'a' WHEN x <= 8 THEN 'b' ELSE 'c' END, x
                FROM long_sequence(12)
                """.formatted(tableName));
        execute("INSERT INTO " + tableName + " VALUES ('2024-01-02T00:00:00.000000Z', 'a', 100)");
        drainWalQueue();
        execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
    }

    // Creates the table "t" with a row per hour from 00:00 to 10:00 on 2024-01-01, all with the symbol a, and one
    // more with the symbol b at the given hour, which it converts to a Parquet partition, and one row on
    // 2024-01-02, in a native partition. With 4 rows per row group, the two rows of the hour 3 or 7 straddle the
    // boundary of two row groups: a commit with a row at that hour dedups both row groups as one run and, when it
    // writes rows into the partition, rewrites the file.
    private void createParquetPartitionTableWithBoundaryTie(int tieHour) throws SqlException {
        execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, sym)");
        execute("""
                INSERT INTO t
                SELECT '2024-01-01'::TIMESTAMP + (x - 1) * 3_600_000_000, 'a', x
                FROM long_sequence(11)
                """);
        execute("INSERT INTO t VALUES ('2024-01-01T%02d:00:00.000000Z', 'b', 20), ('2024-01-02T00:00:00.000000Z', 'a', 100)".formatted(tieHour));
        drainWalQueue();
        execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
    }

    // Creates the table "t" and its twin "u" with a Parquet partition that lacks the column w, which the tables
    // add after the conversion: a merge that writes rows into the partition rewrites its file
    private void createRewrittenParquetPartitionTables() throws SqlException {
        createParquetPartitionTable("t");
        createParquetPartitionTable("u");
        execute("ALTER TABLE t ADD COLUMN w INT");
        execute("ALTER TABLE u ADD COLUMN w INT");
        drainWalQueue();
        // duplicates in the first and the third row group, the merge leaves the partition as it is
        assertDuplicatesKeepPartitions("""
                ('2024-01-01T01:00:00.000000Z', 'a', 1001, 1),
                ('2024-01-01T10:00:00.000000Z', 'c', 1010, 10)
                """);
    }
}
