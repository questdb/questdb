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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.IndexFactory;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.DirectLongList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

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
    private static final Comparator<SymRow> SYM_ROW_BY_TS = Comparator.comparing((SymRow row) -> row.day).thenComparing(row -> row.time);
    // Partition 2024-01-01 holds 12 rows, 3 row groups of 4 rows with a row group size of 4.
    // Partition 2024-01-02 keeps the first one from being the last partition, which CONVERT requires.
    private static final SymRow[] SYM_BASE_ROWS = {
            new SymRow("a", 1, "2024-01-01", "00:00"),
            new SymRow("b", 2, "2024-01-01", "01:00"),
            new SymRow(null, 3, "2024-01-01", "02:00"),
            new SymRow("a", 4, "2024-01-01", "03:00"),
            new SymRow("", 5, "2024-01-01", "04:00"),
            new SymRow("c", 6, "2024-01-01", "05:00"),
            new SymRow("a", 7, "2024-01-01", "06:00"),
            new SymRow("b", 8, "2024-01-01", "07:00"),
            new SymRow(null, 9, "2024-01-01", "08:00"),
            new SymRow("a", 10, "2024-01-01", "09:00"),
            new SymRow("", 11, "2024-01-01", "10:00"),
            new SymRow("c", 12, "2024-01-01", "11:00"),
            new SymRow("a", 99, "2024-01-02", "00:00")
    };
    // after the last row of partition 2024-01-01 (11:00): the existing rows keep their row ids
    private static final SymRow[] SYM_APPEND_ROWS = {
            new SymRow("a", 101, "2024-01-01", "11:10"),
            new SymRow(null, 102, "2024-01-01", "11:20"),
            new SymRow("d", 103, "2024-01-01", "11:30"),
            new SymRow("", 104, "2024-01-01", "11:40")
    };
    // into the first row group and later ones: every later row of partition 2024-01-01 moves to a new row id
    private static final SymRow[] SYM_MERGE_ROWS = {
            new SymRow("a", 101, "2024-01-01", "00:30"),
            new SymRow(null, 102, "2024-01-01", "04:30"),
            new SymRow("d", 103, "2024-01-01", "06:30"),
            new SymRow("", 104, "2024-01-01", "09:30")
    };
    private static final String[] STALE_INDEX_QUERIES = {
            "SELECT s, v, ts FROM pt WHERE s = 'a' AND ts IN '2024-01-01'",
            "SELECT s, v, ts FROM pt WHERE s IS NULL AND ts IN '2024-01-01'",
            "SELECT s, v, ts FROM pt WHERE s = '' AND ts IN '2024-01-01'",
            "SELECT s, v, ts FROM pt WHERE s = 'd' AND ts IN '2024-01-01'",
            "SELECT s, v, ts FROM pt WHERE s IN ('a', 'b') AND ts IN '2024-01-01' ORDER BY ts",
            "SELECT s, v, ts FROM pt WHERE s = 'a' AND ts IN '2024-01-01' ORDER BY ts DESC",
            "SELECT s, v, ts FROM pt LATEST ON ts PARTITION BY s",
            "SELECT s, v, ts FROM pt WHERE s = 'b' AND ts IN '2024-01-01' LATEST ON ts PARTITION BY s"
    };

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
    public void testConcurrentReaderSurvivesFailedInPlaceUpdate() throws Exception {
        // Failed-update companion to testCanSkipRowGroupSurvivesConcurrentO3Merge:
        // a pinned reader must keep working across a failed in-place update. The
        // failed update writes a footer past the committed head but never patches
        // the header (commitParquetMeta runs only after the index build) and never
        // truncates, so the reader's mapped [0, header) is untouched. A guard
        // against disturbing a live reader, not a SIGBUS repro -- the no-truncate
        // design makes that structurally unreachable.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);

        final AtomicBoolean armed = new AtomicBoolean(false);
        final AtomicInteger openROCount = new AtomicInteger(0);
        final FilesFacade dodgyFacade = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                // Fail the 2nd data.parquet openRO (the mapRO inside updateParquetIndexes).
                if (armed.get() && Utf8s.endsWithAscii(name, "data.parquet")) {
                    if (openROCount.incrementAndGet() == 2) {
                        return -1;
                    }
                }
                return super.openRO(name);
            }
        };

        assertMemoryLeak(dodgyFacade, () -> {
            execute(
                    """
                            CREATE TABLE x (a INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            // 3 row groups -> in-place update mode.
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

            final TableToken tableToken = engine.verifyTableName("x");
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
                            if (meta.canSkipRowGroup(i, emptyFilters, 0)) {
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
            }, "parquet-meta-canskip-loop-failed-update");
            bg.setDaemon(true);
            bg.start();

            Assert.assertTrue("background thread failed to start", started.await(10, TimeUnit.SECONDS));
            Assert.assertNull("background thread threw before main work", bgError.get());

            // Failed in-place update + rollback, concurrent with the pinned reader.
            armed.set(true);
            execute(
                    """
                            INSERT INTO x(a, ts) VALUES
                            (50, '2020-01-01T00:30:00.000Z'),
                            (51, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();
            armed.set(false);
            Assert.assertTrue("failed update must suspend the table", engine.getTableSequencerAPI().isSuspended(tableToken));

            // Give the loop time to iterate across the failed-update boundary.
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

            // Recover and verify the data.
            execute("ALTER TABLE x RESUME WAL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertQuery("SELECT count() FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n14\n");
        });
    }

    @Test
    public void testO3MergeAfterDropColumnStampsDenseSortColumn() throws Exception {
        // A column sits BEFORE the designated timestamp, so in the converted
        // parquet the timestamp is at dense column 1. Dropping that leading
        // column and then running an O3 merge rewrites the partition with the
        // dense target schema [ts], shifting the timestamp to dense column 0.
        // The _pm sort column must follow to 0. The table's raw timestamp slot
        // stays 1 (DROP COLUMN leaves a tombstone), so reusing that slot would
        // declare an out-of-range sort column.
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

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            // Converted with the leading column present: ts is the dense column 1.
            assertParquetSortColumn("x", 1);

            execute("ALTER TABLE x DROP COLUMN a");
            drainWalQueue();

            execute(
                    """
                            INSERT INTO x(ts) VALUES
                            ('2020-01-01T00:30:00.000Z'),
                            ('2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            // After the rewrite the dense schema is [ts]: the sort column is 0.
            assertParquetSortColumn("x", 0);
        });
    }

    @Test
    public void testO3MergeNoSchemaChangePreservesDenseSortColumn() throws Exception {
        // The leading column is dropped BEFORE conversion, so the partition is
        // already dense [ts] when it becomes parquet and the encoder writes the
        // sort column at 0. The table's raw timestamp slot is nonetheless 1 (the
        // dropped column leaves a tombstone). A later O3 merge rewrites the
        // partition with NO schema change; the rewrite must keep the dense sort
        // column 0 rather than re-stamping the stale slot 1.
        //
        // The single-row-group partition makes O3PartitionJob choose the rewrite
        // path (rowGroupCount == 1) even without a schema change, so the merge
        // runs through ParquetUpdater rather than the append path.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);

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
                            (4, '2020-01-01T03:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(a, ts) VALUES (99, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // Drop the leading column BEFORE conversion: the partition becomes
            // dense [ts] when encoded.
            execute("ALTER TABLE x DROP COLUMN a");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            // The encoder already writes the dense sort column 0.
            assertParquetSortColumn("x", 0);

            // O3 merge with no schema change rewrites the single-row-group partition.
            execute(
                    """
                            INSERT INTO x(ts) VALUES
                            ('2020-01-01T00:30:00.000Z'),
                            ('2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            // Still the dense sort column 0, not the stale raw slot 1.
            assertParquetSortColumn("x", 0);
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
            assertParquetColumnCount(2);

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

            assertParquetColumnCount(3);
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
            assertParquetColumnCount(3);

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

            assertParquetColumnCount(2);
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
            assertQuery("SELECT count() FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n14\n");
            assertQuery("SELECT a, newcol, ts FROM x ORDER BY ts, a")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testAppendBoundaryTimestamp() throws Exception {
        // An O3 row at exactly the partition max timestamp must take the merge path (new index version),
        // one microsecond later it takes the append path (same index version).
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            createIndexedParquetTable("INDEX", "TIMESTAMP");
            final long columnNameTxnBefore = getColumnNameTxn("pt", 0, "s");
            final long txnAtMax = getTableTxn("pt");
            final String sql = "SELECT s, v, ts FROM pt WHERE s = 'c' AND ts IN '2024-01-01'";
            try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                execute("INSERT INTO pt (s, v, ts) VALUES ('c', 201, '2024-01-01T11:00:00.000000Z')");
                drainWalQueue();
                assertStaleCursor(
                        "at max",
                        """
                                s\tv\tts
                                c\t6\t2024-01-01T05:00:00.000000Z
                                c\t12\t2024-01-01T11:00:00.000000Z
                                """,
                        factory,
                        cursor
                );
            }
            Assert.assertEquals(txnAtMax, getColumnNameTxn("pt", 0, "s"));
            Assert.assertNotEquals(columnNameTxnBefore, txnAtMax);

            try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                execute("INSERT INTO pt (s, v, ts) VALUES ('c', 202, '2024-01-01T11:00:00.000001Z')");
                drainWalQueue();
                // the merge at the max timestamp keeps the existing row first
                assertStaleCursor(
                        "max + 1us",
                        """
                                s\tv\tts
                                c\t6\t2024-01-01T05:00:00.000000Z
                                c\t12\t2024-01-01T11:00:00.000000Z
                                c\t201\t2024-01-01T11:00:00.000000Z
                                """,
                        factory,
                        cursor
                );
            }
            Assert.assertEquals(txnAtMax, getColumnNameTxn("pt", 0, "s"));
            assertQuery(sql)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            s\tv\tts
                            c\t6\t2024-01-01T05:00:00.000000Z
                            c\t12\t2024-01-01T11:00:00.000000Z
                            c\t201\t2024-01-01T11:00:00.000000Z
                            c\t202\t2024-01-01T11:00:00.000001Z
                            """);
        });
    }

    @Test
    public void testAppendBoundaryTimestampDedupUpsert() throws Exception {
        // Under DEDUP an O3 row at the max timestamp replaces the values of the existing row there,
        // so it takes the merge path even though it adds no row.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pt (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");
            execute(symRowsInsert(SYM_BASE_ROWS));
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            final long partitionNameTxn = getParquetPartitionNameTxn("pt", 0);
            final long txnBefore = getTableTxn("pt");
            final String sql = "SELECT s, v, ts FROM pt WHERE s = 'c' AND ts IN '2024-01-01'";
            try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                execute("INSERT INTO pt (s, v, ts) VALUES ('x', 12, '2024-01-01T11:00:00.000000Z')");
                drainWalQueue();
                assertStaleCursor(
                        "dedup upsert",
                        """
                                s\tv\tts
                                c\t6\t2024-01-01T05:00:00.000000Z
                                c\t12\t2024-01-01T11:00:00.000000Z
                                """,
                        factory,
                        cursor
                );
            }
            Assert.assertEquals(partitionNameTxn, getParquetPartitionNameTxn("pt", 0));
            Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
            assertQuery(sql)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            s\tv\tts
                            c\t6\t2024-01-01T05:00:00.000000Z
                            """);
            assertQuery("SELECT s, v, ts FROM pt WHERE s = 'x'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            s\tv\tts
                            x\t12\t2024-01-01T11:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testColumnTopAbsentColumnKeepsLiveIndexName() throws Exception {
        // s is added after partition 2024-01-01 exists, so _cv says the column is absent there (top -1).
        // No reader reads its index files, the merge rebuilds them in place and creates no _cv record.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pt (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO pt VALUES
                    (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z'), (4, '2024-01-01T03:00:00.000000Z'),
                    (5, '2024-01-01T04:00:00.000000Z'), (6, '2024-01-01T05:00:00.000000Z'),
                    (7, '2024-01-01T06:00:00.000000Z'), (8, '2024-01-01T07:00:00.000000Z'),
                    (99, '2024-01-02T00:00:00.000000Z')""");
            drainWalQueue();
            execute("ALTER TABLE pt ADD COLUMN s SYMBOL INDEX");
            execute("INSERT INTO pt VALUES (100, '2024-01-02T01:00:00.000000Z', 'a')");
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            Assert.assertEquals(-1, getColumnTop("pt", 0, "s"));
            Assert.assertEquals(-1, getColumnVersionRecordIndex("pt", 0, "s"));
            assertColumnTopShapeKeepsLiveIndexName(
                    """
                            v\tts
                            1\t2024-01-01T00:00:00.000000Z
                            2\t2024-01-01T01:00:00.000000Z
                            3\t2024-01-01T02:00:00.000000Z
                            4\t2024-01-01T03:00:00.000000Z
                            5\t2024-01-01T04:00:00.000000Z
                            6\t2024-01-01T05:00:00.000000Z
                            7\t2024-01-01T06:00:00.000000Z
                            8\t2024-01-01T07:00:00.000000Z
                            """
            );
            Assert.assertEquals(-1, getColumnVersionRecordIndex("pt", 0, "s"));
        });
    }

    @Test
    public void testColumnTopAtRowCountKeepsLiveIndexName() throws Exception {
        // s is added while partition 2024-01-01 is the last one, so its top there equals the row count.
        // No reader reads its index files, the merge rebuilds them in place.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pt (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO pt VALUES
                    (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z'), (4, '2024-01-01T03:00:00.000000Z'),
                    (5, '2024-01-01T04:00:00.000000Z'), (6, '2024-01-01T05:00:00.000000Z'),
                    (7, '2024-01-01T06:00:00.000000Z'), (8, '2024-01-01T07:00:00.000000Z')""");
            drainWalQueue();
            execute("ALTER TABLE pt ADD COLUMN s SYMBOL INDEX");
            execute("INSERT INTO pt VALUES (99, '2024-01-02T00:00:00.000000Z', 'a')");
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            Assert.assertEquals(8, getColumnTop("pt", 0, "s"));
            assertColumnTopShapeKeepsLiveIndexName(
                    """
                            v\tts
                            1\t2024-01-01T00:00:00.000000Z
                            2\t2024-01-01T01:00:00.000000Z
                            3\t2024-01-01T02:00:00.000000Z
                            4\t2024-01-01T03:00:00.000000Z
                            5\t2024-01-01T04:00:00.000000Z
                            6\t2024-01-01T05:00:00.000000Z
                            7\t2024-01-01T06:00:00.000000Z
                            8\t2024-01-01T07:00:00.000000Z
                            """
            );
            Assert.assertEquals(8, getColumnTop("pt", 0, "s"));
        });
    }

    @Test
    public void testColumnTopInsidePartitionAppend() throws Exception {
        // top = old row count - 1: the last committed row has a value, so readers use the index;
        // an append keeps the index version and indexes only the new rows
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            for (String indexDefinition : new String[]{"INDEX", "INDEX TYPE POSTING"}) {
                assertColumnTopInsidePartitionAppend(indexDefinition);
            }
        });
    }

    @Test
    public void testColumnTopInsidePartitionMerge() throws Exception {
        // top = old row count - 1: the last committed row has a value, so readers use the index;
        // a merge moves the index to a new version and keeps the top
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            for (String indexDefinition : new String[]{"INDEX", "INDEX TYPE POSTING"}) {
                final byte indexType = indexDefinition.contains("POSTING") ? IndexType.POSTING : IndexType.BITMAP;
                execute("DROP TABLE IF EXISTS pt");
                createColumnTopInsidePartitionTable(indexDefinition);
                final long columnNameTxnBefore = getColumnNameTxn("pt", 0, "s");
                final long txnBefore = getTableTxn("pt");
                assertColumnTopInsidePartitionStaleCursors("INSERT INTO pt VALUES (101, '2024-01-01T04:30:00.000000Z', 'x')");
                Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
                Assert.assertEquals(8, getColumnTop("pt", 0, "s"));
                Assert.assertTrue(isIndexKeyFilePresent("pt", 0, indexType, txnBefore));
                Assert.assertTrue(isIndexKeyFilePresent("pt", 0, indexType, columnNameTxnBefore));
            }
        });
    }

    @Test
    public void testConcurrentReaderDuringInPlaceIndexUpdate() throws Exception {
        // a reader thread opens its index cursor, the main thread commits, then the reader iterates
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            for (boolean isAppend : new boolean[]{true, false}) {
                execute("DROP TABLE IF EXISTS pt");
                createIndexedParquetTable("INDEX", "TIMESTAMP");
                final long columnNameTxnBefore = getColumnNameTxn("pt", 0, "s");
                final long txnBefore = getTableTxn("pt");
                final SymRow[] o3Rows = isAppend ? SYM_APPEND_ROWS : SYM_MERGE_ROWS;
                final CyclicBarrier cursorOpen = new CyclicBarrier(2);
                final CyclicBarrier committed = new CyclicBarrier(2);
                final CyclicBarrier purged = new CyclicBarrier(2);
                final AtomicReference<Throwable> readerError = new AtomicReference<>();
                final Thread reader = new Thread(() -> {
                    try (
                            SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1);
                            RecordCursorFactory factory = select(STALE_INDEX_QUERIES[0], context);
                            RecordCursor cursor = factory.getCursor(context)
                    ) {
                        cursorOpen.await();
                        committed.await();
                        assertStaleCursor("reader thread", expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS), factory, cursor);
                        purged.await();
                    } catch (Throwable e) {
                        readerError.set(e);
                        cursorOpen.reset();
                        committed.reset();
                        purged.reset();
                    } finally {
                        Path.clearThreadLocals();
                    }
                }, "stale-index-reader");
                reader.start();
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    cursorOpen.await();
                    execute(symRowsInsert(o3Rows));
                    drainWalQueue();
                    committed.await();
                    // the reader still holds its txn: the purge keeps the version it reads
                    purgeJob.run();
                    Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, columnNameTxnBefore));
                    purged.await();
                    reader.join();
                    Assert.assertNull(readerError.get());

                    engine.releaseInactive();
                    setCurrentMicros(currentMicros + 1_000_000);
                    purgeJob.run();
                }
                Assert.assertEquals(isAppend, isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, columnNameTxnBefore));
                Assert.assertEquals(!isAppend, isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
                assertSqlCursors(
                        "SELECT s, v, ts FROM pt WHERE s::VARCHAR = 'a'",
                        "SELECT s, v, ts FROM pt WHERE s = 'a'"
                );
            }
        });
    }

    @Test
    public void testFormatParquetInOrderAppendKeepsCursorSnapshotBitmap() throws Exception {
        assertFormatParquetInOrderAppendKeepsCursorSnapshot("INDEX");
    }

    @Test
    public void testFormatParquetInOrderAppendKeepsCursorSnapshotPosting() throws Exception {
        assertFormatParquetInOrderAppendKeepsCursorSnapshot("INDEX TYPE POSTING");
    }

    @Test
    public void testFormatParquetLateRowVersionsOnce() throws Exception {
        // A late row into the last partition of a FORMAT PARQUET table moves each indexed column to one
        // new index version. With a reader pinned the old version goes through one purge task per column,
        // without one the commit removes it.
        setInPlaceParquetO3Properties();
        final IndexFileOpenCounter ff = new IndexFileOpenCounter();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE pt (s SYMBOL INDEX, k SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            execute(symRowsInsert(Arrays.copyOf(SYM_BASE_ROWS, 12)));
            drainWalQueue();
            final long sNameTxn = getColumnNameTxn("pt", 0, "s");
            final long kNameTxn = getColumnNameTxn("pt", 0, "k");

            // a reader pinned before the commit
            long txn = getTableTxn("pt");
            long purgeTasksBefore = engine.getMessageBus().getColumnPurgePubSeq().current();
            try (TableReader ignore = getReader("pt")) {
                ff.clear();
                execute(symRowsInsert(SYM_MERGE_ROWS[0]));
                drainWalQueue();
            }
            Assert.assertEquals(txn, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals(txn, getColumnNameTxn("pt", 0, "k"));
            Assert.assertEquals(1, ff.getOpenCount("s.k." + txn));
            Assert.assertEquals(1, ff.getOpenCount("k.k." + txn));
            Assert.assertEquals(purgeTasksBefore + 2, engine.getMessageBus().getColumnPurgePubSeq().current());
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, sNameTxn));
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, kNameTxn));

            // no reader: the commit removes the old version itself
            engine.releaseInactive();
            final long sNameTxn2 = txn;
            txn = getTableTxn("pt");
            engine.releaseInactive();
            purgeTasksBefore = engine.getMessageBus().getColumnPurgePubSeq().current();
            ff.clear();
            execute(symRowsInsert(SYM_MERGE_ROWS[1]));
            drainWalQueue();
            Assert.assertEquals(txn, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals(txn, getColumnNameTxn("pt", 0, "k"));
            Assert.assertEquals(1, ff.getOpenCount("s.k." + txn));
            Assert.assertEquals(1, ff.getOpenCount("k.k." + txn));
            Assert.assertEquals(purgeTasksBefore, engine.getMessageBus().getColumnPurgePubSeq().current());
            Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, sNameTxn2));
            Assert.assertFalse(isIndexValueFilePresent("pt", 0, IndexType.BITMAP, sNameTxn2));
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, txn));

            assertQuery("SELECT s, v, ts FROM pt WHERE s IN ('a', 'b', 'd') ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(symRowsText(
                            false,
                            row -> "a".equals(row.s) || "b".equals(row.s) || "d".equals(row.s),
                            SYM_ROW_BY_TS,
                            Arrays.copyOf(SYM_BASE_ROWS, 12),
                            Arrays.copyOf(SYM_MERGE_ROWS, 2)
                    ));
        });
    }

    @Test
    public void testInPlaceIndexVersionJobFailure() throws Exception {
        // The index rebuild fails after it created the key file of the new version: the table suspends,
        // the new version's files go, the live index is untouched, and RESUME WAL applies the commit.
        setInPlaceParquetO3Properties();
        final AtomicReference<String> failingFileSuffix = new AtomicReference<>();
        final IndexFileOpenCounter ff = new IndexFileOpenCounter() {
            @Override
            public long openRW(LPSZ name, int opts) {
                final String suffix = failingFileSuffix.get();
                if (suffix != null && Utf8s.endsWithAscii(name, suffix)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            createIndexedParquetTable("INDEX", "TIMESTAMP");
            final TableToken tableToken = engine.verifyTableName("pt");
            final long partitionNameTxn = getParquetPartitionNameTxn("pt", 0);
            final long columnNameTxn = getColumnNameTxn("pt", 0, "s");
            final long txnBefore = getTableTxn("pt");
            final byte[] keyFileBefore = readIndexFile("pt", 0, IndexType.BITMAP, columnNameTxn, true);
            final byte[] valueFileBefore = readIndexFile("pt", 0, IndexType.BITMAP, columnNameTxn, false);
            try (
                    RecordCursorFactory factory = select(STALE_INDEX_QUERIES[0]);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                ff.clear();
                failingFileSuffix.set(Files.SEPARATOR + "s.v." + txnBefore);
                execute(symRowsInsert(SYM_MERGE_ROWS));
                drainWalQueue();
                failingFileSuffix.set(null);

                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
                // the rebuild got as far as creating the key file of the new version
                Assert.assertEquals(1, ff.getOpenCount("s.k." + txnBefore));
                Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
                Assert.assertFalse(isIndexValueFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
                Assert.assertEquals(columnNameTxn, getColumnNameTxn("pt", 0, "s"));
                Assert.assertEquals(partitionNameTxn, getParquetPartitionNameTxn("pt", 0));
                Assert.assertArrayEquals(keyFileBefore, readIndexFile("pt", 0, IndexType.BITMAP, columnNameTxn, true));
                Assert.assertArrayEquals(valueFileBefore, readIndexFile("pt", 0, IndexType.BITMAP, columnNameTxn, false));
                assertStaleCursor("after failed commit", expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS), factory, cursor);
            }
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS));

            execute("ALTER TABLE pt RESUME WAL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS, SYM_MERGE_ROWS));
        });
    }

    @Test
    public void testInPlaceIndexVersionOtherPartitionFailure() throws Exception {
        // Non-WAL commit into two parquet partitions: the first one builds its new index version, the
        // second one fails. The commit rolls back and the first partition's new version goes.
        setInPlaceParquetO3Properties();
        final AtomicBoolean isArmed = new AtomicBoolean();
        final IndexFileOpenCounter ff = new IndexFileOpenCounter() {
            @Override
            public long openRO(LPSZ name) {
                if (isArmed.get() && Utf8s.containsAscii(name, "2024-01-02") && Utf8s.endsWithAscii(name, TableUtils.PARQUET_METADATA_FILE_NAME)) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        final SymRow[] firstDayRows = Arrays.copyOf(SYM_BASE_ROWS, 12);
        final SymRow[] secondDayRows = new SymRow[12];
        for (int i = 0; i < secondDayRows.length; i++) {
            secondDayRows[i] = new SymRow(SYM_BASE_ROWS[i].s, SYM_BASE_ROWS[i].v + 20, "2024-01-02", SYM_BASE_ROWS[i].time);
        }
        final SymRow[] o3Rows = {
                new SymRow("a", 101, "2024-01-01", "04:30"),
                new SymRow("a", 121, "2024-01-02", "04:30")
        };
        final SymRow[] lastDayRow = {new SymRow("a", 99, "2024-01-03", "00:00")};
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE pt (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(symRowsInsert(firstDayRows));
            execute(symRowsInsert(secondDayRows));
            execute(symRowsInsert(lastDayRow));
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02'");
            final long columnNameTxn = getColumnNameTxn("pt", 0, "s");
            final long txnBefore = getTableTxn("pt");
            final String sql = "SELECT s, v, ts FROM pt WHERE s = 'a'";
            final String rowsBefore = symRowsText(false, row -> "a".equals(row.s), SYM_ROW_BY_TS, firstDayRows, secondDayRows, lastDayRow);

            ff.clear();
            isArmed.set(true);
            try {
                execute(symRowsInsert(o3Rows));
                Assert.fail("the commit must fail");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "bulk update failed and will be rolled back");
            } finally {
                isArmed.set(false);
            }
            // the first partition built its new index version before the commit failed
            Assert.assertEquals(1, ff.getOpenCount("s.k." + txnBefore));
            Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
            Assert.assertFalse(isIndexValueFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
            Assert.assertEquals(columnNameTxn, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals(txnBefore, getTableTxn("pt"));
            assertQuery(sql).noLeakCheck().timestamp("ts").returns(rowsBefore);

            execute(symRowsInsert(o3Rows));
            Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 1, "s"));
            assertQuery(sql)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(symRowsText(false, row -> "a".equals(row.s), SYM_ROW_BY_TS, firstDayRows, secondDayRows, lastDayRow, o3Rows));
        });
    }

    @Test
    public void testInPlaceIndexVersionTxnCommitFailureRollback() throws Exception {
        // Non-WAL: the _cv commit lands, the _txn commit fails. rollback() restores _cv to the committed
        // _txn and removes the new index version, and the next commit builds it again.
        setInPlaceParquetO3Properties();
        final TxnExtendFailingFacade ff = new TxnExtendFailingFacade("pt");
        final SymRow[] o3Rows = txnExtendingMergeRows();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE pt (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(symRowsInsert(SYM_BASE_ROWS));
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            final long columnNameTxn = getColumnNameTxn("pt", 0, "s");
            final long txnBefore = getTableTxn("pt");
            final long columnVersionBefore;
            try (TableReader reader = getReader("pt")) {
                columnVersionBefore = reader.getTxFile().getColumnVersion();
            }
            try (
                    RecordCursorFactory factory = select(STALE_INDEX_QUERIES[0]);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                ff.isArmed.set(true);
                try {
                    execute(symRowsInsert(o3Rows));
                    Assert.fail("the _txn commit must fail");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
                }
                Assert.assertFalse(ff.isArmed.get());
                // checked before a new reader opens: a reader waits for _cv to match _txn
                Assert.assertEquals(columnVersionBefore, readColumnVersionFileVersion("pt"));
                engine.releaseInactive();
                Assert.assertEquals(txnBefore, getTableTxn("pt"));
                Assert.assertEquals(columnNameTxn, getColumnNameTxn("pt", 0, "s"));
                Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
                Assert.assertFalse(isIndexValueFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
                Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, columnNameTxn));
                assertQuery("SELECT count() FROM pt")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("count\n" + SYM_BASE_ROWS.length + "\n");
                assertQuery(STALE_INDEX_QUERIES[0])
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns(expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS));

                execute(symRowsInsert(o3Rows));
                Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
                assertStaleCursor("after the second commit", expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS), factory, cursor);
            }
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS, o3Rows));
            assertSqlCursors(
                    "SELECT s, v, ts FROM pt WHERE s::VARCHAR = 'a'",
                    "SELECT s, v, ts FROM pt WHERE s = 'a'"
            );
        });
    }

    @Test
    public void testInPlaceIndexVersionTxnCommitFailureWal() throws Exception {
        // WAL: the _cv commit lands, the _txn commit fails and suspends the table. The writer reopen
        // restores _cv to the committed _txn, and the retry at the same txn builds the new index
        // version again over the files the failed attempt left.
        setInPlaceParquetO3Properties();
        final TxnExtendFailingFacade ff = new TxnExtendFailingFacade("pt");
        final SymRow[] o3Rows = txnExtendingMergeRows();
        assertMemoryLeak(ff, () -> {
            createIndexedParquetTable("INDEX", "TIMESTAMP");
            final TableToken tableToken = engine.verifyTableName("pt");
            final long columnNameTxn = getColumnNameTxn("pt", 0, "s");
            final long txnBefore = getTableTxn("pt");
            try (
                    RecordCursorFactory factory = select(STALE_INDEX_QUERIES[0]);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                ff.isArmed.set(true);
                execute(symRowsInsert(o3Rows));
                drainWalQueue();
                Assert.assertFalse(ff.isArmed.get());
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));

                execute("ALTER TABLE pt RESUME WAL");
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
                Assert.assertEquals(txnBefore + 1, getTableTxn("pt"));
                Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
                assertStaleCursor("after retry", expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS), factory, cursor);
            }
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS, o3Rows));
            assertSqlCursors(
                    "SELECT s, v, ts FROM pt WHERE s::VARCHAR = 'a'",
                    "SELECT s, v, ts FROM pt WHERE s = 'a'"
            );
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                purgeJob.run();
            }
            Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, columnNameTxn));
            Assert.assertFalse(isIndexValueFilePresent("pt", 0, IndexType.BITMAP, columnNameTxn));
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
        });
    }

    @Test
    public void testRepeatedInPlaceMergesKeepEachCursorSnapshot() throws Exception {
        // Two merges while one cursor stays open and a second one opens between them. Each cursor
        // returns its snapshot, and the purges remove exactly the replaced versions of both columns.
        setInPlaceParquetO3Properties();
        final SymRow[] secondMergeRows = {
                new SymRow("a", 201, "2024-01-01", "02:30"),
                new SymRow("b", 202, "2024-01-01", "07:30")
        };
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pt (s SYMBOL INDEX, k SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(symRowsInsert(SYM_BASE_ROWS));
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            final long version0 = getColumnNameTxn("pt", 0, "s");
            Assert.assertEquals(version0, getColumnNameTxn("pt", 0, "k"));
            final long version1 = getTableTxn("pt");
            final String sql = "SELECT s, v, ts FROM pt WHERE s IN ('a', 'b') AND ts IN '2024-01-01' ORDER BY ts";
            final String kSql = "SELECT count() FROM pt WHERE k IS NULL AND ts IN '2024-01-01'";
            try (
                    RecordCursorFactory factory1 = select(sql);
                    RecordCursor cursor1 = factory1.getCursor(sqlExecutionContext)
            ) {
                execute(symRowsInsert(SYM_MERGE_ROWS));
                drainWalQueue();
                final long version2 = getTableTxn("pt");
                try (
                        RecordCursorFactory factory2 = select(sql);
                        RecordCursor cursor2 = factory2.getCursor(sqlExecutionContext)
                ) {
                    execute(symRowsInsert(secondMergeRows));
                    drainWalQueue();
                    Assert.assertEquals(version2, getColumnNameTxn("pt", 0, "s"));
                    Assert.assertEquals(version2, getColumnNameTxn("pt", 0, "k"));
                    final Predicate<SymRow> isAOrB = row -> ("a".equals(row.s) || "b".equals(row.s)) && row.day.equals("2024-01-01");
                    assertStaleCursor("first cursor", symRowsText(false, isAOrB, SYM_ROW_BY_TS, SYM_BASE_ROWS), factory1, cursor1);
                    assertStaleCursor("second cursor", symRowsText(false, isAOrB, SYM_ROW_BY_TS, SYM_BASE_ROWS, SYM_MERGE_ROWS), factory2, cursor2);
                    for (long version : new long[]{version0, version1, version2}) {
                        Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, version));
                        Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, "k", version));
                    }
                }
            }
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                purgeJob.run();
            }
            final long version2 = getColumnNameTxn("pt", 0, "s");
            for (long version : new long[]{version0, version1}) {
                Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, version));
                Assert.assertFalse(isIndexValueFilePresent("pt", 0, IndexType.BITMAP, version));
                Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, "k", version));
            }
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, version2));
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, "k", version2));
            assertQuery(sql)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(symRowsText(
                            false,
                            row -> ("a".equals(row.s) || "b".equals(row.s)) && row.day.equals("2024-01-01"),
                            SYM_ROW_BY_TS,
                            SYM_BASE_ROWS,
                            SYM_MERGE_ROWS,
                            secondMergeRows
                    ));
            // every row of the partition has a NULL k
            assertQuery(kSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n18\n");
        });
    }

    @Test
    public void testInPlaceIndexVersionAcrossRestartAndSchemaChanges() throws Exception {
        // The new index version survives a writer and reader restart, a purge job restart, and the
        // operations that follow the column version: RENAME COLUMN, CONVERT TO NATIVE, DROP INDEX.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            createIndexedParquetTable("INDEX", "TIMESTAMP");
            final long columnNameTxn = getColumnNameTxn("pt", 0, "s");
            final long txnBefore = getTableTxn("pt");
            try (TableReader ignore = getReader("pt")) {
                execute(symRowsInsert(SYM_MERGE_ROWS));
                drainWalQueue();
                // the reader holds the old version: the first purge job records the task in its log
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    purgeJob.run();
                }
                Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, columnNameTxn));
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
            // a new purge job picks the task up from its log
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                setCurrentMicros(currentMicros + 1_000_000);
                purgeJob.run();
            }
            Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, columnNameTxn));
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS, SYM_MERGE_ROWS));

            execute("ALTER TABLE pt RENAME COLUMN s TO s2");
            drainWalQueue();
            assertSqlCursors(
                    "SELECT s2, v, ts FROM pt WHERE s2::VARCHAR = 'a'",
                    "SELECT s2, v, ts FROM pt WHERE s2 = 'a'"
            );
            execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();
            assertSqlCursors(
                    "SELECT s2, v, ts FROM pt WHERE s2::VARCHAR = 'a'",
                    "SELECT s2, v, ts FROM pt WHERE s2 = 'a'"
            );
            assertQuery("SELECT s2, v, ts FROM pt WHERE s2 = 'a' AND ts IN '2024-01-01'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS, SYM_MERGE_ROWS).replace("s\tv", "s2\tv"));
        });
    }

    @Test
    public void testInPlaceIndexVersionPurgeDuringCheckpoint() throws Exception {
        // With a checkpoint in progress the merge leaves the old index version to the async purge, even
        // with no reader, and the purge removes it once the checkpoint is released. The table is created
        // after the checkpoint starts, so the checkpoint pins no txn of it.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            execute("CHECKPOINT CREATE");
            final long columnNameTxn;
            final long txnBefore;
            try {
                createIndexedParquetTable("INDEX", "TIMESTAMP");
                columnNameTxn = getColumnNameTxn("pt", 0, "s");
                txnBefore = getTableTxn("pt");
                engine.releaseInactive();
                final long purgeTasksBefore = engine.getMessageBus().getColumnPurgePubSeq().current();
                execute(symRowsInsert(SYM_MERGE_ROWS));
                drainWalQueue();
                Assert.assertEquals(purgeTasksBefore + 1, engine.getMessageBus().getColumnPurgePubSeq().current());
                Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
                Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, columnNameTxn));
            } finally {
                execute("CHECKPOINT RELEASE");
            }
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                purgeJob.run();
            }
            Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, columnNameTxn));
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
        });
    }

    @Test
    public void testInPlaceMergeVersionsOnlyIndexesReadersUse() throws Exception {
        // One merge, three SYMBOL columns: s is indexed with data, t is indexed but absent from the
        // partition (added after it), u is not indexed. Only s moves to a new index version.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pt (s SYMBOL INDEX, u SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(symRowsInsert(SYM_BASE_ROWS));
            drainWalQueue();
            execute("ALTER TABLE pt ADD COLUMN t SYMBOL INDEX");
            execute("INSERT INTO pt (s, u, v, ts, t) VALUES ('a', 'u1', 100, '2024-01-02T01:00:00.000000Z', 't1')");
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            final long sNameTxn = getColumnNameTxn("pt", 0, "s");
            final long tNameTxn = getColumnNameTxn("pt", 0, "t");
            Assert.assertEquals(-1, getColumnTop("pt", 0, "t"));
            final long txnBefore = getTableTxn("pt");
            try (
                    RecordCursorFactory factory = select(STALE_INDEX_QUERIES[0]);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                execute(symRowsInsert(SYM_MERGE_ROWS));
                drainWalQueue();
                assertStaleCursor("mixed columns", expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS), factory, cursor);
            }
            Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, "s", txnBefore));
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, "s", sNameTxn));
            Assert.assertEquals(tNameTxn, getColumnNameTxn("pt", 0, "t"));
            Assert.assertEquals(-1, getColumnVersionRecordIndex("pt", 0, "t"));
            Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, "t", txnBefore));
            Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, "u", txnBefore));
            Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, "u", getColumnNameTxn("pt", 0, "u")));
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS, SYM_MERGE_ROWS));
        });
    }

    @Test
    public void testInPlaceIndexUpdateFuzz() throws Exception {
        // Random appends and merges into a parquet partition, each with an index cursor opened before
        // it. The held cursor must return the full-scan rows captured before the commit, and a fresh
        // index query the full-scan rows after it. The number of batches is a budget property.
        setInPlaceParquetO3Properties();
        final int batchCount = Integer.getInteger("questdb.fuzz.parquet.index.batches", 20);
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final String[] symbols = {"a", "b", "c", "d", null};
        final long dayMicros = 86_400_000_000L;
        final long dayStart = 1_704_067_200_000_000L; // 2024-01-01T00:00:00Z
        assertMemoryLeak(() -> {
            for (String indexDefinition : new String[]{"INDEX", "INDEX TYPE POSTING"}) {
                execute("DROP TABLE IF EXISTS pt");
                execute("CREATE TABLE pt (s SYMBOL " + indexDefinition + ", v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                // 40 rows half an hour apart, then the next day's row that lets CONVERT take the first day
                execute("INSERT INTO pt SELECT rnd_symbol('a', 'b', 'c', NULL), x, '2024-01-01'::TIMESTAMP + (x - 1) * 1_800_000_000L FROM long_sequence(40)");
                execute("INSERT INTO pt VALUES ('a', 0, '2024-01-02T00:00:00.000000Z')");
                drainWalQueue();
                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();
                final long partitionNameTxn = getParquetPartitionNameTxn("pt", 0);
                int appendBatches = 0;
                int mergeBatches = 0;
                long nextV = 1_000;
                long maxTimestamp = dayStart + 39 * 1_800_000_000L;
                for (int batch = 0; batch < batchCount; batch++) {
                    final String key = symbols[rnd.nextInt(symbols.length - 1)];
                    final String indexSql = "SELECT s, v, ts FROM pt WHERE s = '" + key + "' AND ts IN '2024-01-01'";
                    final String fullScanSql = "SELECT s, v, ts FROM pt WHERE s::VARCHAR = '" + key + "' AND ts IN '2024-01-01'";
                    printSql(fullScanSql);
                    final String rowsBefore = sink.toString();
                    final long columnNameTxnBefore = getColumnNameTxn("pt", 0, "s");
                    final long txnBefore = getTableTxn("pt");
                    // the first two batches pin one shape each, the rest are random
                    final boolean isAppend = batch == 0 || (batch > 1 && rnd.nextBoolean());
                    final int rowCount = 1 + rnd.nextInt(6);
                    final StringSink insert = new StringSink();
                    insert.put("INSERT INTO pt VALUES ");
                    for (int i = 0; i < rowCount; i++) {
                        final long timestamp = isAppend ? maxTimestamp + (i + 1) * 1_000_000L : dayStart + rnd.nextLong(maxTimestamp - dayStart);
                        final String symbol = symbols[rnd.nextInt(symbols.length)];
                        insert.put(i > 0 ? ", (" : "(").put(symbol == null ? "NULL" : "'" + symbol + "'").put(", ").put(nextV++).put(", ").put(timestamp).put("::TIMESTAMP)");
                    }
                    if (isAppend) {
                        maxTimestamp += rowCount * 1_000_000L;
                    }
                    Assert.assertTrue(maxTimestamp < dayStart + dayMicros);
                    final String context = indexDefinition + ", batch " + batch + (isAppend ? ", append " : ", merge ") + insert;
                    try (RecordCursorFactory factory = select(indexSql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        execute(insert);
                        drainWalQueue();
                        assertStaleCursor(context, rowsBefore, factory, cursor);
                    }
                    Assert.assertEquals(context, partitionNameTxn, getParquetPartitionNameTxn("pt", 0));
                    Assert.assertEquals(context, isAppend ? columnNameTxnBefore : txnBefore, getColumnNameTxn("pt", 0, "s"));
                    if (isAppend) {
                        appendBatches++;
                    } else {
                        mergeBatches++;
                    }
                    assertSqlCursors(fullScanSql, indexSql);
                    if (rnd.nextBoolean()) {
                        engine.releaseInactive();
                        try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                            purgeJob.run();
                        }
                    }
                }
                Assert.assertTrue(appendBatches > 0);
                Assert.assertTrue(mergeBatches > 0);
            }
        });
    }

    @Test
    public void testInPlaceIndexVersionHousekeepingFailure() throws Exception {
        // Removing the replaced index version fails after the commit landed: the commit stays visible,
        // the writer reports the housekeeping failure, the next commit does not retry the removal, and
        // VACUUM TABLE removes the leftover version.
        setInPlaceParquetO3Properties();
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicInteger oldKeyFileRemovals = new AtomicInteger();
        final AtomicReference<String> oldKeyFileSuffix = new AtomicReference<>();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                final String suffix = oldKeyFileSuffix.get();
                if (suffix != null && Utf8s.endsWithAscii(name, suffix)) {
                    oldKeyFileRemovals.incrementAndGet();
                    if (isArmed.compareAndSet(true, false)) {
                        throw new RuntimeException("test fault on remove");
                    }
                }
                return super.removeQuiet(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE pt (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(symRowsInsert(SYM_BASE_ROWS));
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            final long columnNameTxn = getColumnNameTxn("pt", 0, "s");
            final long txnBefore = getTableTxn("pt");
            oldKeyFileSuffix.set(Files.SEPARATOR + (columnNameTxn > -1 ? "s.k." + columnNameTxn : "s.k"));
            engine.releaseInactive();
            isArmed.set(true);
            try {
                execute(symRowsInsert(SYM_MERGE_ROWS[0]));
                Assert.fail("the housekeeping must fail");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Data has been persisted, but we could not perform housekeeping");
            }
            Assert.assertEquals(1, oldKeyFileRemovals.get());
            Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, columnNameTxn));
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS, Arrays.copyOf(SYM_MERGE_ROWS, 1)));

            // the next merge removes the version it replaces, not the leftover one
            engine.releaseInactive();
            final long secondVersion = getTableTxn("pt");
            engine.releaseInactive();
            execute(symRowsInsert(SYM_MERGE_ROWS[1]));
            Assert.assertEquals(1, oldKeyFileRemovals.get());
            Assert.assertEquals(secondVersion, getColumnNameTxn("pt", 0, "s"));
            Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, columnNameTxn));

            execute("VACUUM TABLE pt");
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                purgeJob.run();
            }
            Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, columnNameTxn));
            Assert.assertFalse(isIndexValueFilePresent("pt", 0, IndexType.BITMAP, columnNameTxn));
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, secondVersion));
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS, Arrays.copyOf(SYM_MERGE_ROWS, 2)));
        });
    }

    @Test
    public void testInPlaceIndexAppendFailureLeavesNoDuplicates() throws Exception {
        // An append fails after it appended to the live index of s: opening the live index of k, the
        // next indexed column, fails. RESUME WAL retries the append, which drops the entries the failed
        // attempt left past the committed rows, so the index of s holds each row once.
        setInPlaceParquetO3Properties();
        final AtomicBoolean isArmed = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, Files.SEPARATOR + "k.k") && isArmed.compareAndSet(true, false)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE pt (s SYMBOL INDEX CAPACITY 2, k SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(symRowsInsert(SYM_BASE_ROWS));
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("pt");
            Assert.assertEquals(-1, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals(-1, getColumnNameTxn("pt", 0, "k"));
            // 2_000 'a' rows after the last row of the partition, a second apart
            final String append = "INSERT INTO pt (s, v, ts) SELECT 'a', 1_000 + x, '2024-01-01T11:00:00.000000Z'::TIMESTAMP + x * 1_000_000L FROM long_sequence(2_000)";
            isArmed.set(true);
            execute(append);
            drainWalQueue();
            Assert.assertFalse(isArmed.get());
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));

            execute("ALTER TABLE pt RESUME WAL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertEquals(-1, getColumnNameTxn("pt", 0, "s"));
            // 4 'a' rows of the partition before the append, plus the 2_000 appended ones
            assertQuery("SELECT count(), min(v), max(v), sum(v) FROM pt WHERE s = 'a' AND ts IN '2024-01-01'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count\tmin\tmax\tsum
                            2004\t1\t3000\t4001022
                            """);
            assertSqlCursors(
                    "SELECT s, v, ts FROM pt WHERE s::VARCHAR = 'a'",
                    "SELECT s, v, ts FROM pt WHERE s = 'a'"
            );
        });
    }

    @Test
    public void testInPlaceIndexAppendExtendsLiveIndex() throws Exception {
        // An in-order commit into the parquet partition appends to the live index of s. The job opens
        // the live s.k and s.v once each and truncates each only when it closes them, to trim the
        // mapped tail. A rebuild would also truncate both when it re-initializes them.
        setInPlaceParquetO3Properties();
        final LiveIndexTruncateCounter ff = new LiveIndexTruncateCounter();
        assertMemoryLeak(ff, () -> {
            createIndexedParquetTable("INDEX", "TIMESTAMP");
            final long partitionNameTxn = getParquetPartitionNameTxn("pt", 0);
            Assert.assertEquals(-1, getColumnNameTxn("pt", 0, "s"));
            try (Path path = new Path()) {
                setPartitionPath("pt", 0, path);
                ff.track(path.toString());
            }

            execute(symRowsInsert(SYM_APPEND_ROWS));
            drainWalQueue();

            Assert.assertEquals(partitionNameTxn, getParquetPartitionNameTxn("pt", 0));
            Assert.assertEquals(-1, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals(1, ff.getOpenCount("s.k"));
            Assert.assertEquals(1, ff.getOpenCount("s.v"));
            Assert.assertEquals(1, ff.getTruncateCount("s.k"));
            Assert.assertEquals(1, ff.getTruncateCount("s.v"));
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS, SYM_APPEND_ROWS));
        });
    }

    @Test
    public void testInPlaceIndexAtServerDefaults() throws Exception {
        // Server default row group size (100_000) and rewrite thresholds: a 200_001-row parquet partition
        // has 3 row groups and takes both the append and the merge in place
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pt (s SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // s cycles through k0..k6, one row every 100 ms from the start of the day
            execute("INSERT INTO pt SELECT 'k' || (x % 7), x, '2024-01-01'::TIMESTAMP + (x - 1) * 100_000L FROM long_sequence(200_001)");
            execute("INSERT INTO pt VALUES ('k0', 0, '2024-01-02T00:00:00.000000Z')");
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            final long partitionNameTxn = getParquetPartitionNameTxn("pt", 0);
            final String indexSql = "SELECT count(), min(v), max(v), sum(v) FROM pt WHERE s = 'k3' AND ts IN '2024-01-01'";
            // k3 rows: x = 3, 10, ..., 200_000, i.e. 28_572 rows summing to 3 * 28_572 + 7 * (28_571 * 28_572 / 2)
            final String k3Before = """
                    count\tmin\tmax\tsum
                    28572\t3\t200000\t2857242858
                    """;
            for (boolean isAppend : new boolean[]{true, false}) {
                final long columnNameTxnBefore = getColumnNameTxn("pt", 0, "s");
                final long txnBefore = getTableTxn("pt");
                final String timestamp = isAppend ? "2024-01-01T06:00:00.000000Z" : "2024-01-01T01:00:00.050000Z";
                final long v = isAppend ? 300_000 : 300_001;
                try (RecordCursorFactory factory = select(indexSql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    execute("INSERT INTO pt VALUES ('k3', " + v + ", '" + timestamp + "')");
                    drainWalQueue();
                    assertStaleCursor(
                            isAppend ? "append" : "merge",
                            isAppend ? k3Before : "count\tmin\tmax\tsum\n28573\t3\t300000\t2857542858\n",
                            factory,
                            cursor
                    );
                }
                Assert.assertEquals(partitionNameTxn, getParquetPartitionNameTxn("pt", 0));
                Assert.assertEquals(isAppend ? columnNameTxnBefore : txnBefore, getColumnNameTxn("pt", 0, "s"));
                assertSqlCursors(
                        "SELECT s, v, ts FROM pt WHERE s::VARCHAR = 'k3'",
                        "SELECT s, v, ts FROM pt WHERE s = 'k3'"
                );
            }
            assertQuery(indexSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tmin\tmax\tsum\n28574\t3\t300001\t2857842859\n");
        });
    }

    @Test
    public void testInPlaceIndexVersionOrphanLeftoverIsReusedByRetry() throws Exception {
        // The failed commit cannot remove its new index version, the key file stays on disk. The retry at
        // the same txn commits that version under the same file names, and no purge may delete them.
        setInPlaceParquetO3Properties();
        final AtomicReference<String> failingOpenName = new AtomicReference<>();
        final AtomicReference<String> failingRemoveSuffix = new AtomicReference<>();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                final String failing = failingOpenName.get();
                if (failing != null && Utf8s.containsAscii(name, failing)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }

            @Override
            public boolean removeQuiet(LPSZ name) {
                final String suffix = failingRemoveSuffix.get();
                if (suffix != null && Utf8s.endsWithAscii(name, suffix)) {
                    return false;
                }
                return super.removeQuiet(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            for (String indexDefinition : new String[]{"INDEX", "INDEX TYPE POSTING"}) {
                final boolean isPosting = indexDefinition.contains("POSTING");
                final byte indexType = isPosting ? IndexType.POSTING : IndexType.BITMAP;
                execute("DROP TABLE IF EXISTS pt");
                createIndexedParquetTable(indexDefinition, "TIMESTAMP");
                final TableToken tableToken = engine.verifyTableName("pt");
                final long txnBefore = getTableTxn("pt");
                try (
                        RecordCursorFactory factory = select(STALE_INDEX_QUERIES[0]);
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    // the rebuild fails opening the value file of the new version, after it created the key file
                    failingOpenName.set(Files.SEPARATOR + (isPosting ? "s.pv." + txnBefore + '.' : "s.v." + txnBefore));
                    failingRemoveSuffix.set(Files.SEPARATOR + (isPosting ? "s.pk." : "s.k.") + txnBefore);
                    execute(symRowsInsert(SYM_MERGE_ROWS));
                    drainWalQueue();
                    failingOpenName.set(null);
                    failingRemoveSuffix.set(null);
                    Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
                    Assert.assertTrue(isIndexKeyFilePresent("pt", 0, indexType, txnBefore));

                    execute("ALTER TABLE pt RESUME WAL");
                    drainWalQueue();
                    Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
                    Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
                    assertStaleCursor(indexDefinition, expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS), factory, cursor);
                }
                engine.releaseInactive();
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    purgeJob.run();
                    setCurrentMicros(currentMicros + 1_000_000);
                    purgeJob.run();
                }
                Assert.assertTrue(isIndexKeyFilePresent("pt", 0, indexType, txnBefore));
                assertQuery(STALE_INDEX_QUERIES[0])
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns(expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS, SYM_MERGE_ROWS));
                assertSqlCursors(
                        "SELECT s, v, ts FROM pt WHERE s::VARCHAR = 'a'",
                        "SELECT s, v, ts FROM pt WHERE s = 'a'"
                );
            }
        });
    }

    @Test
    public void testInPlaceIndexVersionOrphanLeftoverRemovedByVacuum() throws Exception {
        // Non-WAL: the failed commit cannot remove its new index version, and no later commit reuses the
        // version name. VACUUM TABLE removes the leftover.
        setInPlaceParquetO3Properties();
        final AtomicReference<String> failingOpenSuffix = new AtomicReference<>();
        final AtomicReference<String> failingRemoveSuffix = new AtomicReference<>();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                final String suffix = failingOpenSuffix.get();
                if (suffix != null && Utf8s.endsWithAscii(name, suffix)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }

            @Override
            public boolean removeQuiet(LPSZ name) {
                final String suffix = failingRemoveSuffix.get();
                if (suffix != null && Utf8s.endsWithAscii(name, suffix)) {
                    return false;
                }
                return super.removeQuiet(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE pt (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(symRowsInsert(SYM_BASE_ROWS));
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            final long columnNameTxn = getColumnNameTxn("pt", 0, "s");
            final long txnBefore = getTableTxn("pt");
            failingOpenSuffix.set(Files.SEPARATOR + "s.v." + txnBefore);
            failingRemoveSuffix.set(Files.SEPARATOR + "s.k." + txnBefore);
            try {
                execute(symRowsInsert(SYM_MERGE_ROWS));
                Assert.fail("the commit must fail");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "bulk update failed and will be rolled back");
            } finally {
                failingOpenSuffix.set(null);
                failingRemoveSuffix.set(null);
            }
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
            Assert.assertEquals(columnNameTxn, getColumnNameTxn("pt", 0, "s"));

            // an in-order row in the next day moves the table txn past the leftover's name
            execute("INSERT INTO pt VALUES ('a', 100, '2024-01-02T01:00:00.000000Z')");
            Assert.assertEquals(txnBefore + 1, getTableTxn("pt"));
            Assert.assertEquals(columnNameTxn, getColumnNameTxn("pt", 0, "s"));

            engine.releaseInactive();
            execute("VACUUM TABLE pt");
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                purgeJob.run();
            }
            Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, columnNameTxn));
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS));
        });
    }

    @Test
    public void testInPlaceIndexVersionDropIndex() throws Exception {
        // DROP INDEX after a merge removes both index versions once the purge runs
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            createIndexedParquetTable("INDEX", "TIMESTAMP");
            final long columnNameTxn = getColumnNameTxn("pt", 0, "s");
            final long txnBefore = getTableTxn("pt");
            execute(symRowsInsert(SYM_MERGE_ROWS));
            drainWalQueue();
            final long indexedVersion = getColumnNameTxn("pt", 0, "s");
            Assert.assertEquals(txnBefore, indexedVersion);
            execute("ALTER TABLE pt ALTER COLUMN s DROP INDEX");
            drainWalQueue();
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                purgeJob.run();
                setCurrentMicros(currentMicros + 1_000_000);
                purgeJob.run();
            }
            Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, columnNameTxn));
            Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, indexedVersion));
            Assert.assertFalse(isIndexValueFilePresent("pt", 0, IndexType.BITMAP, indexedVersion));
            assertQuery("SELECT s, v, ts FROM pt WHERE s = 'a' AND ts IN '2024-01-01'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS, SYM_MERGE_ROWS));
        });
    }

    @Test
    public void testInPlaceIndexCompiledFactoryReusedAcrossCommits() throws Exception {
        // A factory compiled before an append and a merge, executed after each of them, as a PostgreSQL
        // wire prepared statement is: every execution returns the rows committed before it.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            createIndexedParquetTable("INDEX", "TIMESTAMP");
            final long columnNameTxnBefore = getColumnNameTxn("pt", 0, "s");
            try (RecordCursorFactory factory = select(STALE_INDEX_QUERIES[0])) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertStaleCursor("before", expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS), factory, cursor);
                }
                execute(symRowsInsert(SYM_APPEND_ROWS));
                drainWalQueue();
                Assert.assertEquals(columnNameTxnBefore, getColumnNameTxn("pt", 0, "s"));
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertStaleCursor("after append", expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS, SYM_APPEND_ROWS), factory, cursor);
                }
                final long txnBeforeMerge = getTableTxn("pt");
                execute(symRowsInsert(SYM_MERGE_ROWS));
                drainWalQueue();
                Assert.assertEquals(txnBeforeMerge, getColumnNameTxn("pt", 0, "s"));
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertStaleCursor(
                            "after merge",
                            expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS, SYM_APPEND_ROWS, SYM_MERGE_ROWS),
                            factory,
                            cursor
                    );
                }
            }
        });
    }

    @Test
    public void testInPlaceIndexDedupSkipNoopKeepsColumnVersion() throws Exception {
        // A DEDUP SKIP commit whose rows all duplicate existing rows changes nothing in the parquet
        // partition: no new index version, no purge task.
        setInPlaceParquetO3Properties();
        final IndexFileOpenCounter ff = new IndexFileOpenCounter();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE pt (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, s)");
            execute(symRowsInsert(SYM_BASE_ROWS));
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            final long partitionNameTxn = getParquetPartitionNameTxn("pt", 0);
            final long columnNameTxn = getColumnNameTxn("pt", 0, "s");
            final long txnBefore = getTableTxn("pt");
            final long purgeTasksBefore = engine.getMessageBus().getColumnPurgePubSeq().current();
            try (
                    RecordCursorFactory factory = select(STALE_INDEX_QUERIES[0]);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                ff.clear();
                // duplicates of rows in the first and the second row group
                execute(symRowsInsert(SYM_BASE_ROWS[0], SYM_BASE_ROWS[3], SYM_BASE_ROWS[6]));
                drainWalQueue();
                assertStaleCursor("dedup skip noop", expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS), factory, cursor);
            }
            Assert.assertEquals(partitionNameTxn, getParquetPartitionNameTxn("pt", 0));
            Assert.assertEquals(columnNameTxn, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals(0, ff.getOpenCount("s.k." + txnBefore));
            Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
            Assert.assertEquals(purgeTasksBefore, engine.getMessageBus().getColumnPurgePubSeq().current());
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS));
        });
    }

    @Test
    public void testInPlaceIndexRewriteKeepsColumnVersion() throws Exception {
        // A partition with a single row group takes the rewrite: the index goes into the new partition
        // directory under the same column version, and the stale cursor keeps reading the old directory.
        setInPlaceParquetO3Properties();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createIndexedParquetTable("INDEX", "TIMESTAMP");
            final long partitionNameTxn = getParquetPartitionNameTxn("pt", 0);
            final long columnNameTxn = getColumnNameTxn("pt", 0, "s");
            final long txnBefore = getTableTxn("pt");
            try (
                    RecordCursorFactory factory = select(STALE_INDEX_QUERIES[0]);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                execute(symRowsInsert(SYM_MERGE_ROWS));
                drainWalQueue();
                assertStaleCursor("rewrite", expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS), factory, cursor);
            }
            Assert.assertNotEquals(partitionNameTxn, getParquetPartitionNameTxn("pt", 0));
            Assert.assertEquals(columnNameTxn, getColumnNameTxn("pt", 0, "s"));
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, columnNameTxn));
            Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS, SYM_MERGE_ROWS));
        });
    }

    @Test
    public void testInPlaceIndexTwoPartitionsOneCommitWorkerPool() throws Exception {
        // One non-WAL commit, run by a worker pool, merges a row into parquet partition 2024-01-01 and
        // appends a row to parquet partition 2024-01-02. Each partition takes its own case: the first
        // moves to a new index version, the second keeps its version. A stale cursor over both and the
        // native partition 2024-01-03 returns its snapshot.
        setInPlaceParquetO3Properties();
        final SymRow[] firstDayRows = Arrays.copyOf(SYM_BASE_ROWS, 12);
        final SymRow[] secondDayRows = new SymRow[12];
        for (int i = 0; i < secondDayRows.length; i++) {
            secondDayRows[i] = new SymRow(SYM_BASE_ROWS[i].s, SYM_BASE_ROWS[i].v + 20, "2024-01-02", SYM_BASE_ROWS[i].time);
        }
        final SymRow[] lastDayRow = {new SymRow("a", 99, "2024-01-03", "00:00")};
        final SymRow[] o3Rows = {
                new SymRow("a", 101, "2024-01-01", "04:30"),
                new SymRow("a", 121, "2024-01-02", "11:30")
        };
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pt (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(symRowsInsert(firstDayRows));
            execute(symRowsInsert(secondDayRows));
            execute(symRowsInsert(lastDayRow));
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02'");
            final long firstDayNameTxn = getParquetPartitionNameTxn("pt", 0);
            final long secondDayNameTxn = getParquetPartitionNameTxn("pt", 1);
            final long columnNameTxn = getColumnNameTxn("pt", 0, "s");
            Assert.assertEquals(columnNameTxn, getColumnNameTxn("pt", 1, "s"));
            final long txnBefore = getTableTxn("pt");
            final String sql = "SELECT s, v, ts FROM pt WHERE s = 'a'";
            final WorkerPool pool = new TestWorkerPool(2);
            try {
                WorkerPoolUtils.setupWriterJobs(pool, engine);
                pool.start(LOG);
                try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    execute(symRowsInsert(o3Rows));
                    assertStaleCursor(
                            "two partitions",
                            symRowsText(false, row -> "a".equals(row.s), SYM_ROW_BY_TS, firstDayRows, secondDayRows, lastDayRow),
                            factory,
                            cursor
                    );
                }
            } finally {
                pool.halt();
            }
            Assert.assertEquals(firstDayNameTxn, getParquetPartitionNameTxn("pt", 0));
            Assert.assertEquals(secondDayNameTxn, getParquetPartitionNameTxn("pt", 1));
            Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals(columnNameTxn, getColumnNameTxn("pt", 1, "s"));
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
            Assert.assertFalse(isIndexKeyFilePresent("pt", 1, IndexType.BITMAP, txnBefore));
            Assert.assertTrue(isIndexKeyFilePresent("pt", 1, IndexType.BITMAP, columnNameTxn));
            assertQuery(sql)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(symRowsText(false, row -> "a".equals(row.s), SYM_ROW_BY_TS, firstDayRows, secondDayRows, lastDayRow, o3Rows));
            assertSqlCursors(
                    "SELECT s, v, ts FROM pt WHERE s::VARCHAR = 'a'",
                    sql
            );
        });
    }

    @Test
    public void testStaleIndexCursorKeysAcrossInPlaceO3Bitmap() throws Exception {
        assertStaleIndexCursorsAcrossInPlaceO3("INDEX");
    }

    @Test
    public void testStaleIndexCursorKeysAcrossInPlaceO3BitmapCapacity2() throws Exception {
        // two values per block: every key spans several value blocks, and case B appends across a block boundary
        assertStaleIndexCursorsAcrossInPlaceO3("INDEX CAPACITY 2");
    }

    @Test
    public void testStaleIndexCursorKeysAcrossInPlaceO3Posting() throws Exception {
        assertStaleIndexCursorsAcrossInPlaceO3("INDEX TYPE POSTING");
    }

    @Test
    public void testStaleIndexCursorKeysAcrossInPlaceO3PostingCovering() throws Exception {
        assertStaleIndexCursorsAcrossInPlaceO3("INDEX TYPE POSTING INCLUDE (v)");
    }

    private static int findParquetPartitionIndex(TableReader reader) {
        for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
            if (reader.getPartitionFormat(i) == PartitionFormat.PARQUET) {
                return i;
            }
        }
        return -1;
    }

    private static long getColumnNameTxn(String tableName, int partitionIndex, String columnName) {
        try (TableReader reader = getReader(tableName)) {
            final int columnIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(columnName));
            return reader.getColumnVersionReader().getColumnNameTxn(reader.getPartitionTimestampByIndex(partitionIndex), columnIndex);
        }
    }

    private static int getColumnVersionRecordIndex(String tableName, int partitionIndex, String columnName) {
        try (TableReader reader = getReader(tableName)) {
            final int columnIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(columnName));
            return reader.getColumnVersionReader().getRecordIndex(reader.getPartitionTimestampByIndex(partitionIndex), columnIndex);
        }
    }

    private static long getColumnTop(String tableName, int partitionIndex, String columnName) {
        try (TableReader reader = getReader(tableName)) {
            final int columnIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(columnName));
            return reader.getColumnVersionReader().getColumnTop(reader.getPartitionTimestampByIndex(partitionIndex), columnIndex);
        }
    }

    private static long getParquetPartitionNameTxn(String tableName, int partitionIndex) {
        try (TableReader reader = getReader(tableName)) {
            Assert.assertTrue("partition " + partitionIndex + " must be parquet", reader.getTxFile().isPartitionParquet(partitionIndex));
            return reader.getTxFile().getPartitionNameTxn(partitionIndex);
        }
    }

    private static long readColumnVersionFileVersion(String tableName) {
        try (ColumnVersionReader columnVersionReader = new ColumnVersionReader(); Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(engine.verifyTableName(tableName)).concat(TableUtils.COLUMN_VERSION_FILE_NAME);
            columnVersionReader.ofRO(configuration.getFilesFacade(), path.$());
            return columnVersionReader.readUnsafe();
        }
    }

    private static long getTableTxn(String tableName) {
        try (TableReader reader = getReader(tableName)) {
            return reader.getTxn();
        }
    }

    /**
     * Whether the key file of the index on column {@code s} exists for the given column version,
     * in the directory of partition {@code partitionIndex}.
     */
    private static boolean isIndexKeyFilePresent(String tableName, int partitionIndex, byte indexType, long columnNameTxn) {
        return isIndexKeyFilePresent(tableName, partitionIndex, indexType, "s", columnNameTxn);
    }

    private static boolean isIndexKeyFilePresent(String tableName, int partitionIndex, byte indexType, String columnName, long columnNameTxn) {
        try (Path path = new Path()) {
            setPartitionPath(tableName, partitionIndex, path);
            return configuration.getFilesFacade().exists(IndexFactory.keyFileName(indexType, path, columnName, columnNameTxn));
        }
    }

    private static boolean isIndexValueFilePresent(String tableName, int partitionIndex, byte indexType, long columnNameTxn) {
        assert indexType == IndexType.BITMAP : "the value file name of a POSTING index carries a seal txn";
        try (Path path = new Path()) {
            setPartitionPath(tableName, partitionIndex, path);
            return configuration.getFilesFacade().exists(IndexFactory.valueFileName(indexType, path, "s", columnNameTxn, -1));
        }
    }

    private static byte[] readIndexFile(String tableName, int partitionIndex, byte indexType, long columnNameTxn, boolean isKeyFile) throws IOException {
        try (Path path = new Path()) {
            setPartitionPath(tableName, partitionIndex, path);
            if (isKeyFile) {
                IndexFactory.keyFileName(indexType, path, "s", columnNameTxn);
            } else {
                IndexFactory.valueFileName(indexType, path, "s", columnNameTxn, -1);
            }
            return java.nio.file.Files.readAllBytes(Paths.get(path.toString()));
        }
    }

    private static void setPartitionPath(String tableName, int partitionIndex, Path path) {
        final TableToken tableToken = engine.verifyTableName(tableName);
        try (TableReader reader = getReader(tableName)) {
            path.of(configuration.getDbRoot()).concat(tableToken);
            TableUtils.setPathForNativePartition(
                    path,
                    reader.getMetadata().getTimestampType(),
                    reader.getPartitionedBy(),
                    reader.getPartitionTimestampByIndex(partitionIndex),
                    reader.getTxFile().getPartitionNameTxn(partitionIndex)
            );
        }
    }

    private static String symRowsText(boolean isNanos, Predicate<SymRow> filter, Comparator<SymRow> order, SymRow[]... rowSets) {
        final ObjList<SymRow> rows = new ObjList<>();
        for (SymRow[] rowSet : rowSets) {
            for (SymRow row : rowSet) {
                if (filter.test(row)) {
                    rows.add(row);
                }
            }
        }
        rows.sort(order);
        final StringSink text = new StringSink();
        text.put("s\tv\tts\n");
        for (int i = 0, n = rows.size(); i < n; i++) {
            final SymRow row = rows.getQuick(i);
            text.put(row.s == null ? "" : row.s).put('\t').put(row.v).put('\t').put(row.timestampText(isNanos)).put('\n');
        }
        return text.toString();
    }

    private static String symRowsInsert(SymRow... rows) {
        final StringSink sql = new StringSink();
        sql.put("INSERT INTO pt (s, v, ts) VALUES ");
        for (int i = 0; i < rows.length; i++) {
            if (i > 0) {
                sql.put(", ");
            }
            final SymRow row = rows[i];
            sql.put('(').put(row.s == null ? "NULL" : "'" + row.s + "'").put(", ").put(row.v).put(", '").put(row.timestampText(false)).put("')");
        }
        return sql.toString();
    }

    /**
     * Latest row of every distinct symbol among the rows, ordered by timestamp, the model of
     * LATEST ON ts PARTITION BY s.
     */
    private static String symRowsLatestText(boolean isNanos, Predicate<SymRow> filter, SymRow[]... rowSets) {
        final ObjList<SymRow> latest = new ObjList<>();
        for (SymRow[] rowSet : rowSets) {
            for (SymRow row : rowSet) {
                if (!filter.test(row)) {
                    continue;
                }
                int found = -1;
                for (int i = 0, n = latest.size(); i < n; i++) {
                    if (Objects.equals(latest.getQuick(i).s, row.s)) {
                        found = i;
                        break;
                    }
                }
                if (found < 0) {
                    latest.add(row);
                } else if (SYM_ROW_BY_TS.compare(row, latest.getQuick(found)) > 0) {
                    latest.setQuick(found, row);
                }
            }
        }
        final SymRow[] latestRows = new SymRow[latest.size()];
        for (int i = 0; i < latestRows.length; i++) {
            latestRows[i] = latest.getQuick(i);
        }
        return symRowsText(isNanos, row -> true, SYM_ROW_BY_TS, latestRows);
    }

    private void assertFormatParquetInOrderAppendKeepsCursorSnapshot(String indexDefinition) throws Exception {
        // On a FORMAT PARQUET table every commit into the last partition updates its parquet file in
        // place. In-order commits append to the live index: no new index version, no purge task.
        setInPlaceParquetO3Properties();
        final String keyFileSuffix = indexDefinition.contains("POSTING") ? ".pk." : ".k.";
        final IndexFileOpenCounter ff = new IndexFileOpenCounter();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE pt (s SYMBOL " + indexDefinition + ", k SYMBOL " + indexDefinition + ", v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            execute(symRowsInsert(Arrays.copyOf(SYM_BASE_ROWS, 12)));
            drainWalQueue();
            final long partitionNameTxn = getParquetPartitionNameTxn("pt", 0);
            final long sNameTxn = getColumnNameTxn("pt", 0, "s");
            final long kNameTxn = getColumnNameTxn("pt", 0, "k");
            final long purgeTasksBefore = engine.getMessageBus().getColumnPurgePubSeq().current();
            try (
                    RecordCursorFactory factory = select(STALE_INDEX_QUERIES[0]);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                for (int i = 0; i < SYM_APPEND_ROWS.length; i++) {
                    final long txn = getTableTxn("pt");
                    ff.clear();
                    execute(symRowsInsert(SYM_APPEND_ROWS[i]));
                    drainWalQueue();
                    Assert.assertEquals(txn + 1, getTableTxn("pt"));
                    Assert.assertEquals(partitionNameTxn, getParquetPartitionNameTxn("pt", 0));
                    Assert.assertEquals(sNameTxn, getColumnNameTxn("pt", 0, "s"));
                    Assert.assertEquals(kNameTxn, getColumnNameTxn("pt", 0, "k"));
                    // the append creates no index file named after the commit's txn
                    Assert.assertEquals(0, ff.getOpenCount("s" + keyFileSuffix + txn));
                    Assert.assertEquals(0, ff.getOpenCount("k" + keyFileSuffix + txn));
                }
                assertStaleCursor("in-order appends", expectedStaleIndexQueryResult(0, false, Arrays.copyOf(SYM_BASE_ROWS, 12)), factory, cursor);
            }
            Assert.assertEquals(purgeTasksBefore, engine.getMessageBus().getColumnPurgePubSeq().current());
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedStaleIndexQueryResult(0, false, Arrays.copyOf(SYM_BASE_ROWS, 12), SYM_APPEND_ROWS));
        });
    }

    private void assertStaleIndexCursorsAcrossInPlaceO3(String indexDefinition) throws Exception {
        setInPlaceParquetO3Properties();
        final byte indexType = indexDefinition.contains("POSTING") ? IndexType.POSTING : IndexType.BITMAP;
        assertMemoryLeak(() -> {
            for (String timestampType : new String[]{"TIMESTAMP", "TIMESTAMP_NS"}) {
                for (boolean isAppend : new boolean[]{true, false}) {
                    final String context = indexDefinition + ", " + timestampType + (isAppend ? ", append" : ", merge");
                    final boolean isNanos = "TIMESTAMP_NS".equals(timestampType);
                    final SymRow[] o3Rows = isAppend ? SYM_APPEND_ROWS : SYM_MERGE_ROWS;
                    execute("DROP TABLE IF EXISTS pt");
                    createIndexedParquetTable(indexDefinition, timestampType);

                    final long partitionNameTxn = getParquetPartitionNameTxn("pt", 0);
                    final long columnNameTxnBefore = getColumnNameTxn("pt", 0, "s");
                    final long txnBefore = getTableTxn("pt");
                    final ObjList<RecordCursorFactory> factories = new ObjList<>();
                    final ObjList<RecordCursor> cursors = new ObjList<>();
                    try {
                        for (int q = 0; q < STALE_INDEX_QUERIES.length; q++) {
                            final RecordCursorFactory factory = select(STALE_INDEX_QUERIES[q]);
                            factories.add(factory);
                            cursors.add(factory.getCursor(sqlExecutionContext));
                        }

                        execute(symRowsInsert(o3Rows));
                        drainWalQueue();

                        // the cursors opened before the commit return the rows of their snapshot
                        for (int q = 0; q < STALE_INDEX_QUERIES.length; q++) {
                            assertStaleCursor(
                                    context + ", stale " + STALE_INDEX_QUERIES[q],
                                    expectedStaleIndexQueryResult(q, isNanos, SYM_BASE_ROWS),
                                    factories.getQuick(q),
                                    cursors.getQuick(q)
                            );
                        }

                        Assert.assertEquals(context, partitionNameTxn, getParquetPartitionNameTxn("pt", 0));
                        Assert.assertEquals(context, txnBefore + 1, getTableTxn("pt"));
                        // a merge moves the index to the version named after the commit's txn, an append keeps it
                        Assert.assertEquals(context, isAppend ? columnNameTxnBefore : txnBefore, getColumnNameTxn("pt", 0, "s"));
                        Assert.assertTrue(context, isIndexKeyFilePresent("pt", 0, indexType, columnNameTxnBefore));
                        Assert.assertEquals(context, !isAppend, isIndexKeyFilePresent("pt", 0, indexType, txnBefore));
                    } finally {
                        Misc.freeObjList(cursors);
                        Misc.freeObjList(factories);
                    }

                    // queries that start after the commit see the merged rows
                    for (int q = 0; q < STALE_INDEX_QUERIES.length; q++) {
                        assertQuery(STALE_INDEX_QUERIES[q])
                                .noLeakCheck()
                                .inferTimestamp()
                                .inferRandomAccess()
                                .sizeMayVary()
                                .returns(expectedStaleIndexQueryResult(q, isNanos, SYM_BASE_ROWS, o3Rows));
                    }
                    for (String key : new String[]{"'a'", "''", "'d'"}) {
                        assertSqlCursors(
                                "SELECT s, v, ts FROM pt WHERE s::VARCHAR = " + key,
                                "SELECT s, v, ts FROM pt WHERE s = " + key
                        );
                    }

                    // with the cursors closed, the purge removes the version the merge replaced
                    engine.releaseInactive();
                    try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                        purgeJob.run();
                    }
                    Assert.assertEquals(context, isAppend, isIndexKeyFilePresent("pt", 0, indexType, columnNameTxnBefore));
                    Assert.assertEquals(context, !isAppend, isIndexKeyFilePresent("pt", 0, indexType, txnBefore));
                    assertQuery(STALE_INDEX_QUERIES[0])
                            .noLeakCheck()
                            .inferTimestamp()
                            .inferRandomAccess()
                            .sizeMayVary()
                            .returns(expectedStaleIndexQueryResult(0, isNanos, SYM_BASE_ROWS, o3Rows));
                }
            }
        });
    }

    private static void assertStaleCursor(String context, String expected, RecordCursorFactory factory, RecordCursor cursor) {
        try {
            TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, new StringSink());
        } catch (AssertionError e) {
            throw new AssertionError(context, e);
        }
    }

    /**
     * The merge rows plus one row in each of 300 new partitions: the partition table grows past the
     * size of the _txn file, so the _txn commit extends the file.
     */
    private static SymRow[] txnExtendingMergeRows() {
        final SymRow[] rows = new SymRow[SYM_MERGE_ROWS.length + 300];
        System.arraycopy(SYM_MERGE_ROWS, 0, rows, 0, SYM_MERGE_ROWS.length);
        for (int i = 0; i < 300; i++) {
            rows[SYM_MERGE_ROWS.length + i] = new SymRow("z", 1000 + i, LocalDate.of(2024, 1, 3).plusDays(i).toString(), "00:00");
        }
        return rows;
    }

    private static void setInPlaceParquetO3Properties() {
        // small row groups and permissive rewrite thresholds keep the O3 merge in place
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
    }

    /**
     * A stale cursor over the rows the column has no index entry for keeps returning them across an
     * O3 merge, and the merge leaves the index under its live name.
     */
    private void assertColumnTopShapeKeepsLiveIndexName(String expectedNullRows) throws Exception {
        final long partitionNameTxn = getParquetPartitionNameTxn("pt", 0);
        final long columnNameTxnBefore = getColumnNameTxn("pt", 0, "s");
        final long txnBefore = getTableTxn("pt");
        final String sql = "SELECT v, ts FROM pt WHERE s IS NULL AND ts IN '2024-01-01'";
        try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            execute("INSERT INTO pt VALUES (101, '2024-01-01T04:30:00.000000Z', 'x')");
            drainWalQueue();
            assertStaleCursor("column top shape", expectedNullRows, factory, cursor);
        }
        Assert.assertEquals(partitionNameTxn, getParquetPartitionNameTxn("pt", 0));
        Assert.assertEquals(columnNameTxnBefore, getColumnNameTxn("pt", 0, "s"));
        Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
    }

    private void assertColumnTopInsidePartitionAppend(String indexDefinition) throws Exception {
        final byte indexType = indexDefinition.contains("POSTING") ? IndexType.POSTING : IndexType.BITMAP;
        execute("DROP TABLE IF EXISTS pt");
        createColumnTopInsidePartitionTable(indexDefinition);
        final long columnNameTxnBefore = getColumnNameTxn("pt", 0, "s");
        final long txnBefore = getTableTxn("pt");
        assertColumnTopInsidePartitionStaleCursors("INSERT INTO pt VALUES (101, '2024-01-01T08:30:00.000000Z', 'x')");
        Assert.assertEquals(columnNameTxnBefore, getColumnNameTxn("pt", 0, "s"));
        Assert.assertEquals(8, getColumnTop("pt", 0, "s"));
        Assert.assertFalse(isIndexKeyFilePresent("pt", 0, indexType, txnBefore));
        assertQuery("SELECT v, ts, s FROM pt WHERE s IN ('x', 'y')")
                .noLeakCheck()
                .timestamp("ts")
                .returns("""
                        v\tts\ts
                        9\t2024-01-01T08:00:00.000000Z\ty
                        101\t2024-01-01T08:30:00.000000Z\tx
                        """);
    }

    private void assertColumnTopInsidePartitionStaleCursors(String o3Insert) throws Exception {
        final long partitionNameTxn = getParquetPartitionNameTxn("pt", 0);
        try (
                RecordCursorFactory valueFactory = select("SELECT v, ts, s FROM pt WHERE s = 'y' AND ts IN '2024-01-01'");
                RecordCursor valueCursor = valueFactory.getCursor(sqlExecutionContext);
                RecordCursorFactory nullFactory = select("SELECT v, ts, s FROM pt WHERE s IS NULL AND ts IN '2024-01-01'");
                RecordCursor nullCursor = nullFactory.getCursor(sqlExecutionContext)
        ) {
            execute(o3Insert);
            drainWalQueue();
            assertStaleCursor(
                    "value key",
                    """
                            v\tts\ts
                            9\t2024-01-01T08:00:00.000000Z\ty
                            """,
                    valueFactory,
                    valueCursor
            );
            assertStaleCursor(
                    "null key",
                    """
                            v\tts\ts
                            1\t2024-01-01T00:00:00.000000Z\t
                            2\t2024-01-01T01:00:00.000000Z\t
                            3\t2024-01-01T02:00:00.000000Z\t
                            4\t2024-01-01T03:00:00.000000Z\t
                            5\t2024-01-01T04:00:00.000000Z\t
                            6\t2024-01-01T05:00:00.000000Z\t
                            7\t2024-01-01T06:00:00.000000Z\t
                            8\t2024-01-01T07:00:00.000000Z\t
                            """,
                    nullFactory,
                    nullCursor
            );
        }
        Assert.assertEquals(partitionNameTxn, getParquetPartitionNameTxn("pt", 0));
    }

    private void createColumnTopInsidePartitionTable(String indexDefinition) throws Exception {
        // s is added while partition 2024-01-01 is the last one with 8 rows, CONVERT keeps that top
        // (it equals the row count), and an in-order row with a value makes it top 8 of 9 rows
        execute("CREATE TABLE pt (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO pt VALUES
                (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z'),
                (3, '2024-01-01T02:00:00.000000Z'), (4, '2024-01-01T03:00:00.000000Z'),
                (5, '2024-01-01T04:00:00.000000Z'), (6, '2024-01-01T05:00:00.000000Z'),
                (7, '2024-01-01T06:00:00.000000Z'), (8, '2024-01-01T07:00:00.000000Z')""");
        drainWalQueue();
        execute("ALTER TABLE pt ADD COLUMN s SYMBOL " + indexDefinition);
        execute("INSERT INTO pt VALUES (99, '2024-01-02T00:00:00.000000Z', 'a')");
        drainWalQueue();
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
        execute("INSERT INTO pt VALUES (9, '2024-01-01T08:00:00.000000Z', 'y')");
        drainWalQueue();
        Assert.assertEquals(8, getColumnTop("pt", 0, "s"));
    }

    private void createIndexedParquetTable(String indexDefinition, String timestampType) throws Exception {
        execute("CREATE TABLE pt (s SYMBOL " + indexDefinition + ", v INT, ts " + timestampType + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute(symRowsInsert(SYM_BASE_ROWS));
        drainWalQueue();
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
    }

    private String expectedStaleIndexQueryResult(int query, boolean isNanos, SymRow[]... rowSets) {
        final Predicate<SymRow> isFirstDay = row -> row.day.equals("2024-01-01");
        return switch (query) {
            case 0 -> symRowsText(isNanos, row -> "a".equals(row.s) && isFirstDay.test(row), SYM_ROW_BY_TS, rowSets);
            case 1 -> symRowsText(isNanos, row -> row.s == null && isFirstDay.test(row), SYM_ROW_BY_TS, rowSets);
            case 2 -> symRowsText(isNanos, row -> "".equals(row.s) && isFirstDay.test(row), SYM_ROW_BY_TS, rowSets);
            case 3 -> symRowsText(isNanos, row -> "d".equals(row.s) && isFirstDay.test(row), SYM_ROW_BY_TS, rowSets);
            case 4 -> symRowsText(isNanos, row -> ("a".equals(row.s) || "b".equals(row.s)) && isFirstDay.test(row), SYM_ROW_BY_TS, rowSets);
            case 5 -> symRowsText(isNanos, row -> "a".equals(row.s) && isFirstDay.test(row), SYM_ROW_BY_TS.reversed(), rowSets);
            case 6 -> symRowsLatestText(isNanos, row -> true, rowSets);
            case 7 -> symRowsLatestText(isNanos, row -> "b".equals(row.s) && isFirstDay.test(row), rowSets);
            default -> throw new AssertionError("unknown query " + query);
        };
    }

    private void assertParquetColumnCount(int expectedColumnCount) {
        try (TableReader reader = getReader("x")) {
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

    private void assertParquetSortColumn(String tableName, int expectedSortColumnIndex) {
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
                Assert.assertEquals("sort column count on parquet partition " + i,
                        1, meta.getSortingColumnCount());
                Assert.assertEquals("sort column index on parquet partition " + i,
                        expectedSortColumnIndex, meta.getSortingColumnIndex(0));
                // The sole sort key is the designated timestamp, so the
                // designated-timestamp index must independently equal the
                // expected dense position. Asserting it against
                // getSortingColumnIndex(0) would be tautological: that getter
                // returns getDesignatedTimestampColumnIndex() whenever the
                // SORTING_IS_DTS_ASC flag is set (which it always is here).
                Assert.assertEquals("designated timestamp index on parquet partition " + i,
                        expectedSortColumnIndex, meta.getDesignatedTimestampColumnIndex());
                parquetCount++;
            }
            Assert.assertTrue("expected at least one parquet partition", parquetCount > 0);
        }
    }

    /**
     * Counts the files opened read-write, by file name.
     */
    private static class IndexFileOpenCounter extends TestFilesFacadeImpl {
        private final ConcurrentHashMap<String, AtomicInteger> openCounts = new ConcurrentHashMap<>();

        @Override
        public long openRW(LPSZ name, int opts) {
            final String path = Utf8s.stringFromUtf8Bytes(name);
            openCounts.computeIfAbsent(path.substring(path.lastIndexOf(Files.SEPARATOR) + 1), k -> new AtomicInteger()).incrementAndGet();
            return super.openRW(name, opts);
        }

        void clear() {
            openCounts.clear();
        }

        int getOpenCount(String fileName) {
            final AtomicInteger count = openCounts.get(fileName);
            return count == null ? 0 : count.get();
        }
    }

    /**
     * Counts the read-write opens of the BITMAP index files of column {@code s} in one partition
     * directory, and the truncations of the descriptors those opens return.
     */
    private static class LiveIndexTruncateCounter extends TestFilesFacadeImpl {
        private final ConcurrentHashMap<Long, String> fileNamesByFd = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, AtomicInteger> openCounts = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, AtomicInteger> truncateCounts = new ConcurrentHashMap<>();
        private volatile String partitionDir;

        @Override
        public boolean close(long fd) {
            fileNamesByFd.remove(fd);
            return super.close(fd);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            final String dir = partitionDir;
            if (dir != null && fd > -1) {
                final String path = Utf8s.stringFromUtf8Bytes(name);
                for (String fileName : new String[]{"s.k", "s.v"}) {
                    if (path.equals(dir + Files.SEPARATOR + fileName)) {
                        fileNamesByFd.put(fd, fileName);
                        openCounts.computeIfAbsent(fileName, k -> new AtomicInteger()).incrementAndGet();
                    }
                }
            }
            return fd;
        }

        @Override
        public boolean truncate(long fd, long size) {
            final String fileName = fileNamesByFd.get(fd);
            if (fileName != null) {
                truncateCounts.computeIfAbsent(fileName, k -> new AtomicInteger()).incrementAndGet();
            }
            return super.truncate(fd, size);
        }

        int getOpenCount(String fileName) {
            final AtomicInteger count = openCounts.get(fileName);
            return count == null ? 0 : count.get();
        }

        int getTruncateCount(String fileName) {
            final AtomicInteger count = truncateCounts.get(fileName);
            return count == null ? 0 : count.get();
        }

        void track(String partitionDir) {
            this.partitionDir = partitionDir;
        }
    }

    /**
     * Fails, once when armed, the extension of the _txn file of one table by its writer.
     */
    private static class TxnExtendFailingFacade extends TestFilesFacadeImpl {
        private final AtomicBoolean isArmed = new AtomicBoolean();
        private final String tableDirPrefix;
        private volatile long txnFd = -1;

        TxnExtendFailingFacade(String tableName) {
            this.tableDirPrefix = Files.SEPARATOR + tableName;
        }

        @Override
        public boolean allocate(long fd, long size) {
            if (fd == txnFd && isArmed.compareAndSet(true, false)) {
                return false;
            }
            return super.allocate(fd, size);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (
                    Utf8s.endsWithAscii(name, Files.SEPARATOR + TableUtils.TXN_FILE_NAME)
                            && (Utf8s.containsAscii(name, tableDirPrefix + '~') || Utf8s.containsAscii(name, tableDirPrefix + Files.SEPARATOR))
            ) {
                txnFd = fd;
            }
            return fd;
        }
    }

    private record SymRow(String s, int v, String day, String time) {
        String timestampText(boolean isNanos) {
            return day + 'T' + time + (isNanos ? ":00.000000000Z" : ":00.000000Z");
        }
    }
}
