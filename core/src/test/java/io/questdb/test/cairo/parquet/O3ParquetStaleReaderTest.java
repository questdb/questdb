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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Regression test for stale-reader safety during append-only `_pm` updates.
 *
 * <p>QuestDB-managed parquet normalizes {@code column_top} to 0 when a
 * partition is converted or rewritten. The stale-reader guarantee therefore no
 * longer depends on mutating top metadata. It still depends on append-only
 * footer snapshots: a reader pinned to an older {@code parquetFileSize}
 * must keep resolving a consistent older footer via the {@code prev_footer_offset}
 * chain while a fresh reader sees the new footer after the O3 merge commits.
 */
public class O3ParquetStaleReaderTest extends AbstractCairoTest {

    @Test
    public void testCanSkipRowGroupSurvivesConcurrentO3Merge() throws Exception {
        // Pin a reader on a background thread and loop canSkipRowGroup
        // against its held ParquetMetaFileReader while the main thread
        // triggers an O3 merge that appends a new _pm footer. The pinned
        // reader's mmap still points at the pre-merge bytes, so the loop
        // must keep returning consistent results with no crash.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);

        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (a INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(a, ts) VALUES
                            (1,  '2020-01-01T00:00:00.000Z'),
                            (2,  '2020-01-01T01:00:00.000Z'),
                            (3,  '2020-01-01T02:00:00.000Z'),
                            (4,  '2020-01-01T03:00:00.000Z'),
                            (5,  '2020-01-01T04:00:00.000Z'),
                            (6,  '2020-01-01T05:00:00.000Z'),
                            (7,  '2020-01-01T06:00:00.000Z'),
                            (8,  '2020-01-01T07:00:00.000Z'),
                            (9,  '2020-01-01T08:00:00.000Z'),
                            (10, '2020-01-01T09:00:00.000Z'),
                            (11, '2020-01-01T10:00:00.000Z'),
                            (12, '2020-01-01T11:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(a, ts) VALUES (99, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            final AtomicReference<Throwable> bgError = new AtomicReference<>();
            final AtomicLong iterations = new AtomicLong();
            final AtomicBoolean stop = new AtomicBoolean();
            final CountDownLatch started = new CountDownLatch(1);

            Thread bg = new Thread(() -> {
                try (
                        TableReader reader = engine.getReader("x");
                        DirectLongList emptyFilters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)
                ) {
                    int parquetIdx = findParquetPartitionIndex(reader);
                    if (parquetIdx < 0) {
                        throw new IllegalStateException("no parquet partition");
                    }
                    reader.openPartition(parquetIdx);
                    ParquetMetaFileReader meta = reader
                            .getAndInitParquetPartitionDecoder(parquetIdx)
                            .metadata();
                    int rgCount = meta.getRowGroupCount();
                    if (rgCount < 2) {
                        throw new IllegalStateException("expected >= 2 row groups, got " + rgCount);
                    }
                    started.countDown();
                    while (!stop.get()) {
                        for (int i = 0; i < rgCount; i++) {
                            boolean canSkip = meta.canSkipRowGroup(i, emptyFilters, 0);
                            if (canSkip) {
                                throw new IllegalStateException("empty filter list must never skip");
                            }
                        }
                        iterations.incrementAndGet();
                    }
                } catch (Throwable e) {
                    bgError.set(e);
                } finally {
                    started.countDown();
                }
            }, "parquet-meta-canskip-loop");
            bg.setDaemon(true);
            bg.start();

            Assert.assertTrue("background thread failed to start", started.await(10, TimeUnit.SECONDS));
            Assert.assertNull("background thread threw before main work", bgError.get());

            // O3 insert + drain triggers the merge that appends a new _pm
            // footer on disk. The pinned reader above keeps resolving the
            // pre-merge snapshot.
            execute(
                    """
                            INSERT INTO x(a, ts) VALUES
                            (50, '2020-01-01T00:30:00.000Z'),
                            (51, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            // Give the loop time to iterate across the merge boundary.
            long target = iterations.get() + 200;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (iterations.get() < target && bgError.get() == null && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }

            stop.set(true);
            bg.join(30_000);
            Assert.assertFalse("background thread did not stop", bg.isAlive());
            Assert.assertNull("background thread threw: " + bgError.get(), bgError.get());
            Assert.assertTrue("background should have iterated", iterations.get() > 0);
        });
    }

    @Test
    public void testO3MergePreservesAddColumnSchemaInPm() throws Exception {
        // ADD COLUMN fires AFTER the partition is already parquet. On the
        // subsequent O3 merge, the target-schema loop in O3PartitionJob
        // must emit a null column chunk for the added column so the
        // merged _pm carries the evolved column count.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);

        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (a INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(a, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(a, ts) VALUES (99, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // Convert BEFORE adding the column so the parquet file is
            // written with only (a, ts).
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertParquetColumnCount("x", 2);

            execute("ALTER TABLE x ADD COLUMN newcol DOUBLE");
            drainWalQueue();

            // O3 insert with the new column present triggers a merge that
            // must emit a null column chunk into the existing row groups.
            execute(
                    """
                            INSERT INTO x(a, newcol, ts) VALUES
                            (50, 7.5, '2020-01-01T00:30:00.000Z'),
                            (51, 8.5, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            assertParquetColumnCount("x", 3);
        });
    }

    @Test
    public void testO3MergePreservesDropColumnSchemaInPm() throws Exception {
        // DROP COLUMN fires AFTER the partition is already parquet. On the
        // subsequent O3 merge, the target-schema loop in O3PartitionJob
        // skips the dropped column (colType < 0), so the merged _pm
        // carries the reduced column count.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);

        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (a INT, b INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(a, b, ts) VALUES
                            (1, 100, '2020-01-01T00:00:00.000Z'),
                            (2, 101, '2020-01-01T01:00:00.000Z'),
                            (3, 102, '2020-01-01T02:00:00.000Z'),
                            (4, 103, '2020-01-01T03:00:00.000Z'),
                            (5, 104, '2020-01-01T04:00:00.000Z'),
                            (6, 105, '2020-01-01T05:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(a, b, ts) VALUES (99, 999, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertParquetColumnCount("x", 3);

            execute("ALTER TABLE x DROP COLUMN b");
            drainWalQueue();

            execute(
                    """
                            INSERT INTO x(a, ts) VALUES
                            (50, '2020-01-01T00:30:00.000Z'),
                            (51, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            assertParquetColumnCount("x", 2);
        });
    }

    @Test
    public void testStaleReaderSurvivesO3MergeWithColumnTops() throws Exception {
        // Force the in-place update path to be the default by making rewrite
        // thresholds permissive. The merge must stay append-only even though
        // the parquet snapshot itself changes.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);

        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (a INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            // 12 rows over day 2020-01-01 → 3 row groups of 4 (rowGroupSize=4)
            // ensures the partition has rowGroupCount > 1, so the merge does
            // not fall through to the single-row-group rewrite branch.
            execute(
                    """
                            INSERT INTO x(a, ts) VALUES
                            (1,  '2020-01-01T00:00:00.000Z'),
                            (2,  '2020-01-01T01:00:00.000Z'),
                            (3,  '2020-01-01T02:00:00.000Z'),
                            (4,  '2020-01-01T03:00:00.000Z'),
                            (5,  '2020-01-01T04:00:00.000Z'),
                            (6,  '2020-01-01T05:00:00.000Z'),
                            (7,  '2020-01-01T06:00:00.000Z'),
                            (8,  '2020-01-01T07:00:00.000Z'),
                            (9,  '2020-01-01T08:00:00.000Z'),
                            (10, '2020-01-01T09:00:00.000Z'),
                            (11, '2020-01-01T10:00:00.000Z'),
                            (12, '2020-01-01T11:00:00.000Z')
                            """
            );
            // Insert into next day so 2020-01-01 is no longer the active
            // (last) partition, which is required for CONVERT TO PARQUET.
            execute("INSERT INTO x(a, ts) VALUES (99, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // ALTER TABLE ADD COLUMN gives the new column a non-zero native
            // column top on the existing partition. Converting to parquet must
            // materialize that null prefix into the parquet chunks and publish
            // an `_pm` snapshot with normalized top metadata.
            execute("ALTER TABLE x ADD COLUMN newcol DOUBLE");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Acquire a reader and pin it across the corrupting merge. The
            // reader holds a scoreboard reference at the pre-merge txn, so
            // even if the writer queues the previous partition directory for
            // purge, the on-disk bytes survive long enough for us to re-open
            // the partition through this reader's stale snapshot.
            try (TableReader staleReader = getReader("x")) {
                int parquetIdx = findParquetPartitionIndex(staleReader);
                Assert.assertTrue("expected a parquet partition", parquetIdx >= 0);

                final long sizeBefore = staleReader.openPartition(parquetIdx);
                Assert.assertTrue("partition should have rows", sizeBefore > 0);

                // O3 insert into the parquet partition with values for the
                // newly-added column. The merge publishes a new footer snapshot
                // while leaving the old bytes intact for the stale reader.
                execute(
                        """
                                INSERT INTO x(a, newcol, ts) VALUES
                                (50, 7.5, '2020-01-01T00:30:00.000Z'),
                                (51, 8.5, '2020-01-01T01:30:00.000Z')
                                """
                );
                drainWalQueue();

                // Re-open the parquet partition through the held reader
                // without refreshing its txn snapshot. The stale reader must
                // keep reading the pre-merge `_pm` snapshot even though a fresh
                // footer has been appended for the committed state.
                staleReader.closePartitionByIndex(parquetIdx);
                final long sizeAfter = staleReader.openPartition(parquetIdx);
                Assert.assertEquals(
                        "stale snapshot should resolve to the same partition row count",
                        sizeBefore,
                        sizeAfter
                );
            }

            // A fresh reader sees the post-merge state with the new rows
            // merged in.
            assertSql(
                    "count\n14\n",
                    "SELECT count() FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02'"
            );
            assertSql(
                    """
                            a\tnewcol\tts
                            1\tnull\t2020-01-01T00:00:00.000000Z
                            50\t7.5\t2020-01-01T00:30:00.000000Z
                            2\tnull\t2020-01-01T01:00:00.000000Z
                            51\t8.5\t2020-01-01T01:30:00.000000Z
                            3\tnull\t2020-01-01T02:00:00.000000Z
                            4\tnull\t2020-01-01T03:00:00.000000Z
                            5\tnull\t2020-01-01T04:00:00.000000Z
                            6\tnull\t2020-01-01T05:00:00.000000Z
                            7\tnull\t2020-01-01T06:00:00.000000Z
                            8\tnull\t2020-01-01T07:00:00.000000Z
                            9\tnull\t2020-01-01T08:00:00.000000Z
                            10\tnull\t2020-01-01T09:00:00.000000Z
                            11\tnull\t2020-01-01T10:00:00.000000Z
                            12\tnull\t2020-01-01T11:00:00.000000Z
                            99\tnull\t2020-01-02T00:00:00.000000Z
                            """,
                    "SELECT a, newcol, ts FROM x ORDER BY ts, a"
            );
        });
    }

    private static int findParquetPartitionIndex(TableReader reader) {
        for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
            if (reader.getPartitionFormat(i) == PartitionFormat.PARQUET) {
                return i;
            }
        }
        return -1;
    }

    private void assertParquetColumnCount(String tableName, int expectedColumnCount) {
        try (TableReader reader = getReader(tableName)) {
            int parquetCount = 0;
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                    continue;
                }
                reader.openPartition(i);
                ParquetMetaFileReader meta = reader
                        .getAndInitParquetPartitionDecoder(i)
                        .metadata();
                Assert.assertEquals("column count on parquet partition " + i,
                        expectedColumnCount, meta.getColumnCount());
                Assert.assertTrue("row group count > 0", meta.getRowGroupCount() > 0);
                parquetCount++;
            }
            Assert.assertTrue("expected at least one parquet partition", parquetCount > 0);
        }
    }
}
