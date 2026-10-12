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

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Data semantics of DEDUP REJECT: a new row that matches an existing row, or an earlier row of the
 * same commit, on all dedup keys is discarded. The earliest write wins, mirroring DEDUP UPSERT where
 * the latest write wins.
 */
public class DedupRejectTest extends AbstractCairoTest {

    @Test
    public void testRejectAcrossCommitsAppliedOneByOne() throws Exception {
        assertMemoryLeak(() -> assertRejectAcrossCommits(true));
    }

    @Test
    public void testRejectAcrossCommitsAppliedTogether() throws Exception {
        assertMemoryLeak(() -> assertRejectAcrossCommits(false));
    }

    @Test
    public void testRejectAcrossWalWriters() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP REJECT KEYS(ts, sym)");
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
    public void testRejectAgainstWalLag() throws Exception {
        assertMemoryLeak(() -> {
            // maxUncommittedRows caps both the WAL lag (20 rows) and the transaction block size, so the two
            // transactions are applied one by one. The second transaction overlaps the first one, which keeps
            // the first transaction in the WAL lag, then the second transaction merges with the lag rows.
            execute("""
                    CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL
                    WITH maxUncommittedRows=1
                    DEDUP REJECT KEYS(ts, sym)
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
    public void testRejectAllDuplicatesThenAppend() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP REJECT KEYS(ts, sym)");
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
    public void testRejectColumnTopKey() throws Exception {
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
            execute("ALTER TABLE t DEDUP ENABLE REJECT KEYS(ts, k)");
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
    public void testRejectDuplicatesInOlderPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP REJECT KEYS(ts, sym)");
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
    public void testRejectParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP REJECT KEYS(ts, sym)");
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

            // duplicates only, the parquet partition stays as is
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T01:00:00.000000Z', 'b', 14),
                    ('2024-01-01T01:00:00.000000Z', 'c', 15)
                    """);
            drainWalQueue();

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
    public void testRejectParquetPartitionTimestampOnlyKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP REJECT KEYS(ts)");
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
    public void testRejectPreExistingDuplicates() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 'a', 1),
                    ('2024-01-01T00:00:00.000000Z', 'a', 2)
                    """);
            drainWalQueue();

            // dedup enabled after the duplicates were written, REJECT leaves them in place
            execute("ALTER TABLE t DEDUP ENABLE REJECT KEYS(ts, sym)");
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
    public void testRejectTimestampOnlyKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP REJECT KEYS(ts)");
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
    public void testRejectVarSizeKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k1 VARCHAR, k2 STRING, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP REJECT KEYS(ts, k1, k2)");
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
    public void testRejectWithinCommit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP REJECT KEYS(ts, sym)");
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

            execute("ALTER TABLE t DEDUP ENABLE REJECT KEYS(ts, sym)");
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
            execute("ALTER TABLE t DEDUP REJECT KEYS(ts, sym)");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 'a', 6)");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 'a', 7)");
            drainWalQueue();
            assertValue(5);

            execute("ALTER TABLE t DEDUP UPSERT KEYS(ts, sym)");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 'a', 8)");
            execute("ALTER TABLE t DEDUP DISABLE");
            execute("ALTER TABLE t DEDUP ENABLE REJECT KEYS(ts, sym)");
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

    private void assertRejectAcrossCommits(boolean drainEachCommit) throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP REJECT KEYS(ts, sym)");
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
}
