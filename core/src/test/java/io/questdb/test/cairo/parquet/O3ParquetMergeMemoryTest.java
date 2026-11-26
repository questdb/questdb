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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.std.Files.SEPARATOR;

/**
 * An O3 merge into a parquet partition decodes row groups into buffers that the worker reuses. A
 * deduplicating commit at a timestamp that a run of row groups shares decodes the whole run at once,
 * so the worker must not keep the buffers after the merge: it would hold the memory of its largest
 * decode for good. The same holds for the rebase arena, which grows with the O3 rows of var-size
 * columns that the worker writes to a parquet file, and for the write of a new parquet partition.
 * <p>
 * The tests that read NATIVE_O3 release the writers before each reading, because a writer holds O3
 * buffers of its own, and warm up the merge context of the thread first, because the context holds
 * native lists of its own until the thread ends.
 */
public class O3ParquetMergeMemoryTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1000);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
    }

    @Test
    public void testCopyO3IntoParquetPartitionFreesRebaseArena() throws Exception {
        assertMemoryLeak(() -> {
            warmUpMergeContext();
            execute("CREATE TABLE t (ts TIMESTAMP, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01T06:00:00.000000Z', 1_000_000L), 'existing_' || x FROM long_sequence(2_000)");
            execute("INSERT INTO t VALUES ('2024-01-02T00:00:00.000000Z', 'next day')");
            drainWalQueue();
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            Assert.assertEquals(2, getParquetRowGroupCount("2024-01-01"));

            // The commit copies the 500 in-order rows before 06:00 to a new row group ahead of the stored ones and
            // rebases their VARCHAR aux in the arena. A merge of the rows into the first row group would leave two
            // row groups: 1,500 rows make one.
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 1_000_000L), 'trigger_' || x FROM long_sequence(500)");
            engine.releaseAllWriters();
            final long o3MemBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_O3);
            drainWalQueue();
            engine.releaseAllWriters();
            Assert.assertEquals("rebase arena", o3MemBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_O3));

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            Assert.assertEquals(3, getParquetRowGroupCount("2024-01-01"));
            assertQuery("SELECT count() FROM t").noLeakCheck().noRandomAccess().expectSize().returns("count\n2501\n");
            assertQuery("""
                    SELECT ts, v FROM t
                    WHERE ts IN ('2024-01-01T00:00:00.000000Z', '2024-01-01T00:08:19.000000Z', '2024-01-01T06:00:00.000000Z',
                        '2024-01-01T06:33:19.000000Z', '2024-01-02T00:00:00.000000Z')
                    """)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-01-01T00:00:00.000000Z\ttrigger_1
                            2024-01-01T00:08:19.000000Z\ttrigger_500
                            2024-01-01T06:00:00.000000Z\texisting_1
                            2024-01-01T06:33:19.000000Z\texisting_2000
                            2024-01-02T00:00:00.000000Z\tnext day
                            """);
        });
    }

    @Test
    public void testDedupTieRunMergeFreesDecodeBuffers() throws Exception {
        // 10,000 rows at 12:00 make a run of 10 row groups that share the timestamp, the commit at 12:00
        // merges the whole run
        assertMemoryLeak(() -> {
            createParquetTable("DEDUP UPSERT KEYS(ts, id)");
            assertCommitFreesDecodeBuffers("INSERT INTO t VALUES ('2024-01-01T12:00:00.000000Z', 1_000_000, 'new', 1.0)", 10_004);
            assertCommitFreesDecodeBuffers("INSERT INTO t VALUES ('2024-01-01T12:00:00.000000Z', 7, 'upd', 2.0)", 10_004);
            assertQuery("SELECT s, d FROM t WHERE id = 7").noLeakCheck().returns("s\td\nupd\t2.0\n");
        });
    }

    @Test
    public void testFreshPartitionWriteFailureFreesRebaseArena() throws Exception {
        final AtomicBoolean isPmOpenFailing = new AtomicBoolean();
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, TableUtils.PARQUET_METADATA_FILE_NAME) && isPmOpenFailing.compareAndSet(true, false)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            warmUpMergeContext();
            execute("CREATE TABLE t (ts TIMESTAMP, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 1_000_000L), 'value_' || x FROM long_sequence(10_000)");
            engine.releaseAllWriters();
            final long o3MemBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_O3);
            // the write of the new partition rebases the VARCHAR aux in the arena, then fails to open _pm
            isPmOpenFailing.set(true);
            drainWalQueue();
            engine.releaseAllWriters();
            Assert.assertEquals("rebase arena", o3MemBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_O3));

            Assert.assertFalse("the _pm open did not fail", isPmOpenFailing.get());
            final TableToken tableToken = engine.verifyTableName("t");
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertQuery("SELECT suspended, errorMessage FROM wal_tables() WHERE name = 't'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("suspended\terrorMessage\ntrue\tcould not open read-write [file=" + root + SEPARATOR + tableToken.getDirName()
                            + SEPARATOR + "2024-01-01" + SEPARATOR + TableUtils.PARQUET_METADATA_FILE_NAME + "]\n");
            assertQuery("SELECT name, isParquet FROM table_partitions('t')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("name\tisParquet\n");

            execute("ALTER TABLE t RESUME WAL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertQuery("SELECT name, isParquet FROM table_partitions('t')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("name\tisParquet\n2024-01-01\ttrue\n");
            assertQuery("SELECT count() FROM t").noLeakCheck().noRandomAccess().expectSize().returns("count\n10000\n");
            assertQuery("SELECT ts, v FROM t WHERE ts IN ('2024-01-01T00:00:00.000000Z', '2024-01-01T02:46:39.000000Z')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-01-01T00:00:00.000000Z\tvalue_1
                            2024-01-01T02:46:39.000000Z\tvalue_10000
                            """);
        });
    }

    @Test
    public void testFreshPartitionWriteFreesDecodeBuffersAndRebaseArena() throws Exception {
        assertMemoryLeak(() -> {
            warmUpMergeContext();
            execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL INDEX, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            // The commit of 20,000 in-order rows, 10,000 a day, writes two new partitions of 10 row groups. Each write
            // rebases the VARCHAR aux in the arena and decodes the new file to build the index of s.
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 8_640_000L), 'k' || (x % 7), 'value_' || x FROM long_sequence(20_000)");
            engine.releaseAllWriters();
            final long o3MemBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_O3);
            final long updaterMemBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER);
            drainWalQueue();
            engine.releaseAllWriters();
            Assert.assertEquals("decode buffers", updaterMemBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER));
            Assert.assertEquals("rebase arena", o3MemBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_O3));

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            assertQuery("SELECT name, isParquet FROM table_partitions('t')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tisParquet
                            2024-01-01\ttrue
                            2024-01-02\ttrue
                            """);
            assertQuery("""
                    SELECT ts, s, v FROM t
                    WHERE ts IN ('2024-01-01T00:00:00.000000Z', '2024-01-01T23:59:51.360000Z', '2024-01-02T00:00:00.000000Z',
                        '2024-01-02T23:59:51.360000Z')
                    """)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\ts\tv
                            2024-01-01T00:00:00.000000Z\tk1\tvalue_1
                            2024-01-01T23:59:51.360000Z\tk4\tvalue_10000
                            2024-01-02T00:00:00.000000Z\tk5\tvalue_10001
                            2024-01-02T23:59:51.360000Z\tk1\tvalue_20000
                            """);
            // each write built the index of its partition from the decode buffers, the second after the first freed them
            assertQuery("SELECT count() FROM t WHERE s = 'k3' AND ts IN '2024-01-01'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Index forward scan on: s")
                    .returns("count\n1429\n");
            assertQuery("SELECT count() FROM t WHERE s = 'k3' AND ts IN '2024-01-02'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Index forward scan on: s")
                    .returns("count\n1428\n");
        });
    }

    @Test
    public void testInPlaceMergeFreesDecodeBuffers() throws Exception {
        assertMemoryLeak(() -> {
            createParquetTable("");
            assertCommitFreesDecodeBuffers("INSERT INTO t VALUES ('2024-01-01T03:00:00.000000Z', 1_000_000, 'new', 1.0)", 10_004);
        });
    }

    private void assertCommitFreesDecodeBuffers(String insert, long expectedRowCount) throws Exception {
        final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER);
        execute(insert);
        drainWalQueue();
        assertQuery("SELECT count() FROM t").noLeakCheck().noRandomAccess().expectSize().returns("count\n" + expectedRowCount + "\n");
        Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER));
    }

    private static void createParquetTable(String dedup) throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, id LONG, s VARCHAR, d DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL " + dedup);
        // 10,000 rows at 12:00 between 1 row at 00:00 and 1 row at 23:00
        execute("""
                INSERT INTO t
                SELECT '2024-01-01T12:00:00.000000Z'::TIMESTAMP, x, 'v' || x, x::DOUBLE FROM long_sequence(10_000)
                UNION ALL SELECT '2024-01-01T00:00:00.000000Z'::TIMESTAMP, -1, 'first', 0.0 FROM long_sequence(1)
                UNION ALL SELECT '2024-01-01T23:00:00.000000Z'::TIMESTAMP, -2, 'last', 0.0 FROM long_sequence(1)
                """);
        execute("INSERT INTO t VALUES ('2024-01-02T00:00:00.000000Z', -3, 'next day', 0.0)");
        drainWalQueue();
        execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
    }

    private static int getParquetRowGroupCount(String partitionName) {
        final long partitionTimestamp = parseFloorPartialTimestamp(partitionName);
        try (TableReader reader = getReader("t")) {
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getPartitionTimestampByIndex(i) == partitionTimestamp) {
                    Assert.assertEquals(PartitionFormat.PARQUET, reader.getPartitionFormat(i));
                    reader.openPartition(i);
                    return reader.getAndInitParquetPartitionDecoder(i).metadata().getRowGroupCount();
                }
            }
        }
        throw new AssertionError("no partition " + partitionName);
    }

    // Creates the merge context of this thread with the write of a new parquet partition that sizes no rebase arena,
    // having no var-size column, and decodes nothing, having no indexed SYMBOL. A warm-up that sized the arena would
    // leave it in the reading before the measured commit when the worker keeps it, and a measured commit of the same
    // size would not grow it.
    private static void warmUpMergeContext() throws Exception {
        execute("CREATE TABLE warm_up (ts TIMESTAMP, n LONG) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
        execute("INSERT INTO warm_up VALUES ('2024-01-01T00:00:00.000000Z', 1)");
        drainWalQueue();
    }
}
