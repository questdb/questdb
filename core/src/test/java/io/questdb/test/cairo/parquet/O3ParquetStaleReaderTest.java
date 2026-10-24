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

import io.questdb.MessageBus;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnIndexerJob;
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.ColumnTaskJob;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.O3CopyJob;
import io.questdb.cairo.O3OpenColumnJob;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.idx.IndexFactory;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.SCSequence;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.DirectLongList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

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
    // the columns added together with s in the column top shape tests: one of each type CONVERT TO NATIVE decodes from parquet
    private static final String[] COLUMN_TOP_SHAPE_COLUMNS = {
            "s SYMBOL", "d DOUBLE", "i INT", "l LONG", "vc VARCHAR", "st STRING", "ch CHAR", "bo BOOLEAN", "bt BYTE", "sh SHORT", "u UUID", "ip IPv4",
            "l256 LONG256", "g GEOHASH(4c)", "d18 DECIMAL(18, 2)", "d38 DECIMAL(38, 4)", "arr DOUBLE[]", "sy SYMBOL", "tn TIMESTAMP_NS"
    };
    private static final String[] COLUMN_TOP_SHAPE_COLUMN_NAMES = {"s", "d", "i", "l", "vc", "st", "ch", "bo", "bt", "sh", "u", "ip", "l256", "g", "d18", "d38", "arr", "sy", "tn"};
    private static final String COLUMN_TOP_SHAPE_NULL_SQL = "SELECT v, ts, s FROM pt WHERE s IS NULL AND ts IN '2024-01-01'";
    // partition 2024-01-01: 2 row groups of 4 rows, every column added later NULL
    private static final String COLUMN_TOP_SHAPE_P1_ROWS = """
            (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z'),
            (3, '2024-01-01T02:00:00.000000Z'), (4, '2024-01-01T03:00:00.000000Z'),
            (5, '2024-01-01T04:00:00.000000Z'), (6, '2024-01-01T05:00:00.000000Z'),
            (7, '2024-01-01T06:00:00.000000Z'), (8, '2024-01-01T07:00:00.000000Z')""";
    private static final String COLUMN_TOP_SHAPE_P1_NULL_ROWS = """
            v\tts\ts
            1\t2024-01-01T00:00:00.000000Z\t
            2\t2024-01-01T01:00:00.000000Z\t
            3\t2024-01-01T02:00:00.000000Z\t
            4\t2024-01-01T03:00:00.000000Z\t
            5\t2024-01-01T04:00:00.000000Z\t
            6\t2024-01-01T05:00:00.000000Z\t
            7\t2024-01-01T06:00:00.000000Z\t
            8\t2024-01-01T07:00:00.000000Z\t
            """;
    // after the last row of partition 2024-01-01, more than one row group
    private static final String[] COLUMN_TOP_SHAPE_APPEND_ROWS = {
            columnTopShapeRow(101, "2024-01-01T08:10:00.000000Z", "x"),
            columnTopShapeRow(102, "2024-01-01T08:20:00.000000Z", "a"),
            columnTopShapeRow(103, "2024-01-01T08:30:00.000000Z", null),
            columnTopShapeRow(104, "2024-01-01T08:40:00.000000Z", ""),
            columnTopShapeRow(105, "2024-01-01T08:50:00.000000Z", "x")
    };
    // into the first row group, the second one, and past the last row
    private static final String[] COLUMN_TOP_SHAPE_MERGE_ROWS = {
            columnTopShapeRow(101, "2024-01-01T00:30:00.000000Z", "x"),
            columnTopShapeRow(102, "2024-01-01T04:30:00.000000Z", "a"),
            columnTopShapeRow(103, "2024-01-01T05:30:00.000000Z", null),
            columnTopShapeRow(104, "2024-01-01T06:30:00.000000Z", ""),
            columnTopShapeRow(105, "2024-01-01T09:30:00.000000Z", "x")
    };
    private static final String[] COLUMN_TOP_SHAPE_QUERIES = {
            "SELECT * FROM {t}",
            "SELECT * FROM {t} WHERE ts IN '2024-01-01'",
            "SELECT v, ts, s FROM {t} WHERE s = 'x'",
            "SELECT v, ts, s FROM {t} WHERE s = 'a'",
            "SELECT v, ts, s FROM {t} WHERE s = ''",
            "SELECT v, ts, s FROM {t} WHERE s IS NULL",
            "SELECT v, ts, s FROM {t} WHERE s IN ('x', 'a')",
            "SELECT v, ts, s FROM {t} WHERE s != 'x'",
            "SELECT v, ts, s FROM {t} WHERE s = 'x' ORDER BY ts DESC",
            "SELECT v, ts, s FROM {t} LATEST ON ts PARTITION BY s",
            "SELECT v, ts, s FROM {t} WHERE s = 'x' LATEST ON ts PARTITION BY s",
            "SELECT count() FROM {t} WHERE s = 'x'"
    };
    // reads d and vc through the index when it covers them
    private static final String[] COLUMN_TOP_SHAPE_COVERED_QUERIES = {"SELECT ts, s, d, vc FROM {t} WHERE s = 'x'"};
    private static final String[] LEGACY_ABSENT_QUERIES = {
            "SELECT * FROM {t}",
            "SELECT v, ts, s FROM {t} WHERE s = 'x'",
            "SELECT v, ts, s FROM {t} WHERE s = 'z'",
            "SELECT v, ts, s FROM {t} WHERE s IS NULL",
            "SELECT v, ts, s FROM {t} WHERE s IN ('x', 'a')",
            "SELECT v, ts, s FROM {t} LATEST ON ts PARTITION BY s"
    };
    private static final String[] LEGACY_LIVE_TOP_QUERIES = {
            "SELECT * FROM {t}",
            "SELECT v, ts, s FROM {t} WHERE s IS NULL",
            "SELECT v, ts, s FROM {t} WHERE s = 'y'",
            "SELECT v, ts, s FROM {t} WHERE s = 'x'",
            "SELECT v, ts, s FROM {t} WHERE s IN ('x', 'y', 'a')",
            "SELECT v, ts, s, d, vc FROM {t} WHERE s = 'y' ORDER BY ts DESC",
            "SELECT v, ts, k FROM {t} WHERE k = 'a'",
            "SELECT v, ts, k FROM {t} WHERE k = 'b' ORDER BY ts DESC",
            "SELECT v, ts, s FROM {t} LATEST ON ts PARTITION BY s",
            "SELECT count() FROM {t} WHERE s = 'x'"
    };
    private static final String LEGACY_LIVE_TOP_P1_ROWS = """
            ('a', 1, '2024-01-01T00:00:00.000000Z'), ('b', 2, '2024-01-01T01:00:00.000000Z'),
            ('a', 3, '2024-01-01T02:00:00.000000Z'), ('b', 4, '2024-01-01T03:00:00.000000Z'),
            ('a', 5, '2024-01-01T04:00:00.000000Z'), ('b', 6, '2024-01-01T05:00:00.000000Z'),
            ('a', 7, '2024-01-01T06:00:00.000000Z'), ('b', 8, '2024-01-01T07:00:00.000000Z')""";
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
    // the rows of partition 2024-01-01 in SYM_BASE_ROWS: the partition the attach tests copy from table src
    private static final SymRow[] SYM_ATTACHED_ROWS = Arrays.copyOf(SYM_BASE_ROWS, 12);
    // partition 2024-01-02 of table pt, which attaches 2024-01-01 from table src: its symbols, in the order
    // SYM_ATTACHED_ROWS first uses them, take the keys the copied s.d holds
    private static final SymRow[] SYM_ATTACH_TARGET_ROWS = {
            new SymRow("a", 91, "2024-01-02", "01:00"),
            new SymRow("b", 92, "2024-01-02", "02:00"),
            new SymRow("", 93, "2024-01-02", "03:00"),
            new SymRow("c", 94, "2024-01-02", "04:00")
    };
    private static final String[] ATTACH_INDEX_DEFINITIONS = {"INDEX", "INDEX CAPACITY 2", "INDEX TYPE POSTING", "INDEX TYPE POSTING INCLUDE (v)"};
    // the columns of the failure tests added after 5 of 8 rows of partition 2024-01-01
    private static final String[] CONVERT_FAILURE_COLUMNS = {"s", "bt", "st"};
    // one row in each of 300 new partitions: the next _txn record outgrows the _txn file
    private static final String CONVERT_FAILURE_PARTITIONS_INSERT = "INSERT INTO {t} (v, ts) SELECT (1_000 + x)::INT, dateadd('d', x::INT, '2024-01-02T12:00:00.000000Z'::TIMESTAMP) FROM long_sequence(300)";
    // the queries over partition 2024-01-01 that a failed CONVERT TO NATIVE must leave as they were
    private static final String[] CONVERT_FAILURE_QUERIES = {
            "SELECT * FROM {t} WHERE ts IN '2024-01-01'",
            "SELECT v, ts, s FROM {t} WHERE s = 'x' AND ts IN '2024-01-01'",
            "SELECT v, ts, s FROM {t} WHERE s IS NULL AND ts IN '2024-01-01'",
            "SELECT v, ts, s FROM {t} WHERE s IN ('x', 'a') AND ts IN '2024-01-01'",
            "SELECT v, ts, s FROM {t} WHERE ts IN '2024-01-01' LATEST ON ts PARTITION BY s"
    };
    private static final String[] LEGACY_ABSENT_NATIVE_QUERIES = {
            "SELECT * FROM {t}",
            "SELECT v, ts, s, d FROM {t} WHERE s = 'x'",
            "SELECT v, ts, s FROM {t} WHERE s = 'z'",
            "SELECT v, ts, s FROM {t} WHERE s IS NULL",
            "SELECT v, ts, s FROM {t} WHERE s IN ('x', 'a')",
            "SELECT v, ts, s FROM {t} WHERE s != 'x'",
            "SELECT v, ts, s FROM {t} WHERE s = 'x' ORDER BY ts DESC",
            "SELECT v, ts, s FROM {t} LATEST ON ts PARTITION BY s",
            "SELECT v, ts, s FROM {t} WHERE s = 'x' LATEST ON ts PARTITION BY s",
            "SELECT count() FROM {t} WHERE s = 'x'"
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
    private final SCSequence eventSubSequence = new SCSequence();

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
        // No reader reads its index files, the merge rebuilds them in place. The parquet file holds s
        // from row 0 after the update, so _cv gets a record with top 0 and the live column name txn.
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
            Assert.assertTrue(getColumnVersionRecordIndex("pt", 0, "s") > -1);
            Assert.assertEquals(0, getColumnTop("pt", 0, "s"));
        });
    }

    @Test
    public void testColumnTopAtRowCountKeepsLiveIndexName() throws Exception {
        // s is added while partition 2024-01-01 is the last one, so its top there equals the row count.
        // No reader reads its index files, the merge rebuilds them in place, and the top becomes 0.
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
            Assert.assertEquals(0, getColumnTop("pt", 0, "s"));
        });
    }

    @Test
    public void testInPlaceColumnTopShapesBitmap() throws Exception {
        assertInPlaceColumnTopShapes("");
    }

    @Test
    public void testInPlaceColumnTopShapesBitmapCapacity2() throws Exception {
        assertInPlaceColumnTopShapes("CAPACITY 2");
    }

    @Test
    public void testInPlaceColumnTopShapesPosting() throws Exception {
        assertInPlaceColumnTopShapes("TYPE POSTING");
    }

    @Test
    public void testInPlaceColumnTopShapesPostingCovering() throws Exception {
        assertInPlaceColumnTopShapes("TYPE POSTING INCLUDE (d, vc)");
    }

    @Test
    public void testInPlaceColumnTopFirstAndLastRowId() throws Exception {
        // Partition 2024-01-01 holds rows on the hour from 01:00. The O3 row lands at the first row id
        // (00:30, with s NULL: an all-NULL column) or after the last one (with s 'x'). With 8 rows the
        // update runs in place. With 1 row the partition has a single row group, which the update
        // rewrites into a new directory: that path gives top 0 as well.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            for (int p1RowCount : new int[]{8, 1}) {
                for (ColumnTopShape shape : new ColumnTopShape[]{ColumnTopShape.ABSENT, ColumnTopShape.AT_ROW_COUNT}) {
                    for (boolean isFirstRowId : new boolean[]{true, false}) {
                        final String context = p1RowCount + " rows, " + shape + (isFirstRowId ? ", first row id" : ", last row id");
                        execute("DROP TABLE IF EXISTS pt");
                        execute("DROP TABLE IF EXISTS tw");
                        final StringSink p1Rows = new StringSink();
                        final StringSink p1NullRows = new StringSink();
                        for (int v = 1; v <= p1RowCount; v++) {
                            final String timestamp = "2024-01-01T0" + v + ":00:00.000000Z";
                            p1Rows.put(v > 1 ? ", " : "").put('(').put(v).put(", '").put(timestamp).put("')");
                            p1NullRows.put(v).put('\t').put(timestamp).put("\t\n");
                        }
                        createColumnTopShapeTable("pt", shape, "", "WAL", p1Rows.toString(), true);
                        createColumnTopShapeTable("tw", shape, "", "WAL", p1Rows.toString(), false);
                        Assert.assertEquals(context, shape == ColumnTopShape.ABSENT ? -1 : p1RowCount, getColumnTop("pt", 0, "s"));
                        final long partitionNameTxn = getParquetPartitionNameTxn("pt", 0);
                        final String lastTimestamp = "2024-01-01T0" + (p1RowCount + 1) + ":00:00.000000Z";
                        final String o3Row = isFirstRowId
                                ? columnTopShapeRow(101, "2024-01-01T00:30:00.000000Z", null)
                                : columnTopShapeRow(101, lastTimestamp, "x");
                        execute(columnTopShapeInsert("pt", o3Row));
                        execute(columnTopShapeInsert("tw", o3Row));
                        drainWalQueue();
                        Assert.assertEquals(context, p1RowCount > 1, partitionNameTxn == getParquetPartitionNameTxn("pt", 0));
                        assertColumnTopsZero(context);
                        assertQuery("SELECT v, ts, s FROM pt WHERE s IS NULL AND ts IN '2024-01-01'")
                                .noLeakCheck()
                                .timestamp("ts")
                                .returns("v\tts\ts\n" + (isFirstRowId ? "101\t2024-01-01T00:30:00.000000Z\t\n" : "") + p1NullRows);
                        assertQuery("SELECT v, ts, s FROM pt WHERE s = 'x'")
                                .noLeakCheck()
                                .timestamp("ts")
                                .returns("v\tts\ts\n" + (isFirstRowId ? "" : "101\t" + lastTimestamp + "\tx\n"));
                        assertColumnTopShapeQueriesMatchTwin(context);
                        execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                        drainWalQueue();
                        assertColumnTopShapeQueriesMatchTwin(context + ", native");
                    }
                }
            }
        });
    }

    @Test
    public void testParquetRewriteKeepsCoveredValuesOfNameTxnZero() throws Exception {
        // In the ZERO shape s is the first column added to the empty table: its column name txn is 0.
        // With 1 row, partition 2024-01-01 is a single row group, so the O3 commit rewrites it into a
        // new directory and links the covering index of s there. The covered columns then read what
        // the native twin reads, before and after CONVERT TO NATIVE.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            for (String walMode : new String[]{"WAL", "BYPASS WAL"}) {
                final String p1Row = "(1, '2024-01-01T01:00:00.000000Z')";
                execute("DROP TABLE IF EXISTS pt");
                execute("DROP TABLE IF EXISTS tw");
                createColumnTopShapeTable("pt", ColumnTopShape.ZERO, "TYPE POSTING INCLUDE (d, vc)", walMode, p1Row, true);
                createColumnTopShapeTable("tw", ColumnTopShape.ZERO, "TYPE POSTING INCLUDE (d, vc)", walMode, p1Row, false);
                Assert.assertEquals(walMode, 0, getColumnNameTxn("pt", 0, "s"));
                final long partitionNameTxn = getParquetPartitionNameTxn("pt", 0);
                final String o3Rows = columnTopShapeInsert("{t}",
                        columnTopShapeRow(101, "2024-01-01T00:30:00.000000Z", "x"),
                        columnTopShapeRow(102, "2024-01-01T02:00:00.000000Z", "x")
                );
                execute(o3Rows.replace("{t}", "pt"));
                execute(o3Rows.replace("{t}", "tw"));
                drainWalQueue();
                Assert.assertNotEquals(walMode, partitionNameTxn, getParquetPartitionNameTxn("pt", 0));
                assertQuery("SELECT ts, s, d, vc FROM pt WHERE s = 'x'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .timestamp("ts")
                        .withPlanContaining("CoveringIndex on: s with: ts, d, vc")
                        .returns("""
                                ts\ts\td\tvc
                                2024-01-01T00:30:00.000000Z\tx\t101.5\tvc101
                                2024-01-01T02:00:00.000000Z\tx\t102.5\tvc102
                                """);
                assertQueriesMatchTwin("pt", walMode + ", rewritten", COLUMN_TOP_SHAPE_COVERED_QUERIES);
                execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                drainWalQueue();
                assertColumnTopShapeQueriesMatchTwin(walMode + ", native");
                assertQueriesMatchTwin("pt", walMode + ", native", COLUMN_TOP_SHAPE_COVERED_QUERIES);
            }
        });
    }

    @Test
    public void testInPlaceColumnTopTwoPartitionsOneCommitWorkerPool() throws Exception {
        // s and the other columns are added while 2024-01-02 is the last partition: their top is -1 on
        // 2024-01-01 and the row count on 2024-01-02. One non-WAL commit, run by a worker pool, merges
        // a row into the first parquet partition and appends a row to the second. Both end with top 0.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            for (String table : new String[]{"pt", "tw"}) {
                execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("INSERT INTO " + table + " (v, ts) VALUES " + COLUMN_TOP_SHAPE_P1_ROWS);
                execute("INSERT INTO " + table + " (v, ts) VALUES " + COLUMN_TOP_SHAPE_P1_ROWS.replace("2024-01-01", "2024-01-02"));
                addColumnTopShapeColumns(table, "");
                execute(columnTopShapeInsert(table, columnTopShapeRow(100, "2024-01-03T00:00:00.000000Z", "a")));
            }
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02'");
            Assert.assertEquals(-1, getColumnTop("pt", 0, "s"));
            Assert.assertEquals(8, getColumnTop("pt", 1, "s"));
            final long firstDayNameTxn = getParquetPartitionNameTxn("pt", 0);
            final long secondDayNameTxn = getParquetPartitionNameTxn("pt", 1);
            final String o3Insert = columnTopShapeInsert(
                    "pt",
                    columnTopShapeRow(101, "2024-01-01T04:30:00.000000Z", "x"),
                    columnTopShapeRow(102, "2024-01-02T08:30:00.000000Z", "x")
            );
            final WorkerPool pool = new TestWorkerPool(2);
            try {
                WorkerPoolUtils.setupWriterJobs(pool, engine);
                pool.start(LOG);
                execute(o3Insert);
                execute(o3Insert.replace("INSERT INTO pt ", "INSERT INTO tw "));
            } finally {
                pool.halt();
            }
            Assert.assertEquals(firstDayNameTxn, getParquetPartitionNameTxn("pt", 0));
            Assert.assertEquals(secondDayNameTxn, getParquetPartitionNameTxn("pt", 1));
            for (int partitionIndex = 0; partitionIndex < 2; partitionIndex++) {
                for (String column : COLUMN_TOP_SHAPE_COLUMN_NAMES) {
                    Assert.assertEquals("partition " + partitionIndex + ", column " + column, 0, getColumnTop("pt", partitionIndex, column));
                }
            }
            assertQuery("SELECT v, ts, s FROM pt WHERE s = 'x'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            v\tts\ts
                            101\t2024-01-01T04:30:00.000000Z\tx
                            102\t2024-01-02T08:30:00.000000Z\tx
                            """);
            assertColumnTopShapeQueriesMatchTwin("two partitions");
            execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01', '2024-01-02'");
            assertColumnTopShapeQueriesMatchTwin("two partitions, native");
        });
    }

    @Test
    public void testInPlaceColumnTopRepairSurvivesRestart() throws Exception {
        // The first in-place commit into a partition with top -1 rebuilds the index of s in place and
        // zeroes the tops. Both survive a writer and reader restart. The next in-order commit appends to
        // the live index: it opens and truncates the live s.k and s.v once each, and writes no _cv.
        setInPlaceParquetO3Properties();
        final LiveIndexTruncateCounter ff = new LiveIndexTruncateCounter();
        assertMemoryLeak(ff, () -> {
            createColumnTopShapeTable("pt", ColumnTopShape.ABSENT, "", "WAL", COLUMN_TOP_SHAPE_P1_ROWS, true);
            createColumnTopShapeTable("tw", ColumnTopShape.ABSENT, "", "WAL", COLUMN_TOP_SHAPE_P1_ROWS, false);
            final long columnNameTxn = getColumnNameTxn("pt", 0, "s");
            final long txnBefore = getTableTxn("pt");
            final String mergeRow = columnTopShapeRow(101, "2024-01-01T04:30:00.000000Z", "x");
            execute(columnTopShapeInsert("pt", mergeRow));
            execute(columnTopShapeInsert("tw", mergeRow));
            drainWalQueue();
            // case A: no reader used the index, so it stays under the live name
            Assert.assertEquals(columnNameTxn, getColumnNameTxn("pt", 0, "s"));
            Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, txnBefore));

            engine.releaseAllWriters();
            engine.releaseAllReaders();
            assertColumnTopsZero("after restart");
            Assert.assertEquals(columnNameTxn, getColumnNameTxn("pt", 0, "s"));
            assertColumnTopShapeQueriesMatchTwin("after restart");

            final long columnVersionBefore;
            try (TableReader reader = getReader("pt")) {
                columnVersionBefore = reader.getTxFile().getColumnVersion();
            }
            final String nameTxnSuffix = columnNameTxn > -1 ? "." + columnNameTxn : "";
            try (Path path = new Path()) {
                setPartitionPath("pt", 0, path);
                ff.track(path.toString(), "s.k" + nameTxnSuffix, "s.v" + nameTxnSuffix);
            }
            final String appendRow = columnTopShapeRow(102, "2024-01-01T08:30:00.000000Z", "x");
            execute(columnTopShapeInsert("pt", appendRow));
            execute(columnTopShapeInsert("tw", appendRow));
            drainWalQueue();
            try (TableReader reader = getReader("pt")) {
                Assert.assertEquals(columnVersionBefore, reader.getTxFile().getColumnVersion());
            }
            Assert.assertEquals(columnNameTxn, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals(1, ff.getOpenCount("s.k" + nameTxnSuffix));
            Assert.assertEquals(1, ff.getOpenCount("s.v" + nameTxnSuffix));
            Assert.assertEquals(1, ff.getTruncateCount("s.k" + nameTxnSuffix));
            Assert.assertEquals(1, ff.getTruncateCount("s.v" + nameTxnSuffix));
            assertQuery("SELECT v, ts, s FROM pt WHERE s = 'x'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            v\tts\ts
                            101\t2024-01-01T04:30:00.000000Z\tx
                            102\t2024-01-01T08:30:00.000000Z\tx
                            """);
            assertColumnTopShapeQueriesMatchTwin("after append");
        });
    }

    @Test
    public void testInPlaceColumnTopTxnCommitFailureRollback() throws Exception {
        // Non-WAL, s absent from partition 2024-01-01 (top -1): the _cv commit with the zeroed tops
        // lands, the _txn commit fails. rollback() restores top -1, and the retry zeroes it again.
        setInPlaceParquetO3Properties();
        final TxnExtendFailingFacade ff = new TxnExtendFailingFacade("pt");
        final StringSink o3Rows = new StringSink();
        o3Rows.put(columnTopShapeRow(101, "2024-01-01T04:30:00.000000Z", "x"));
        // one row in each of 150 new partitions: the partition table, about 5 KB, outgrows the first 4 KB of the
        // _txn file, see TxnExtendFailingFacade. The last comparison with the twin reads every partition of both
        // tables at once, and 300 partitions of 21 columns would open more files than the JVM allows on macOS
        for (int i = 0; i < 150; i++) {
            o3Rows.put(", ").put(columnTopShapeRow(1_000 + i, LocalDate.of(2024, 1, 3).plusDays(i) + "T00:00:00.000000Z", "z"));
        }
        assertMemoryLeak(ff, () -> {
            createColumnTopShapeTable("pt", ColumnTopShape.ABSENT, "", "BYPASS WAL", COLUMN_TOP_SHAPE_P1_ROWS, true);
            createColumnTopShapeTable("tw", ColumnTopShape.ABSENT, "", "BYPASS WAL", COLUMN_TOP_SHAPE_P1_ROWS, false);
            final long columnVersionBefore;
            try (TableReader reader = getReader("pt")) {
                columnVersionBefore = reader.getTxFile().getColumnVersion();
            }
            try (
                    RecordCursorFactory factory = select(COLUMN_TOP_SHAPE_NULL_SQL);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                ff.isArmed.set(true);
                try {
                    execute(columnTopShapeInsert("pt", o3Rows.toString()));
                    Assert.fail("the _txn commit must fail");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
                }
                Assert.assertFalse(ff.isArmed.get());
                Assert.assertEquals(columnVersionBefore, readColumnVersionFileVersion("pt"));
                engine.releaseInactive();
                Assert.assertEquals(-1, getColumnTop("pt", 0, "s"));
                Assert.assertEquals(-1, getColumnTop("pt", 0, "d"));
                assertQuery(COLUMN_TOP_SHAPE_NULL_SQL)
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns(COLUMN_TOP_SHAPE_P1_NULL_ROWS);
                assertColumnTopShapeQueriesMatchTwin("after the failed commit");

                execute(columnTopShapeInsert("pt", o3Rows.toString()));
                execute(columnTopShapeInsert("tw", o3Rows.toString()));
                assertStaleCursor("after the retry", COLUMN_TOP_SHAPE_P1_NULL_ROWS, factory, cursor);
            }
            assertColumnTopsZero("after the retry");
            assertQuery("SELECT v, ts, s FROM pt WHERE s = 'x'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            v\tts\ts
                            101\t2024-01-01T04:30:00.000000Z\tx
                            """);
            assertColumnTopShapeQueriesMatchTwin("after the retry");
        });
    }

    @Test
    public void testInPlaceColumnTopIndexRebuildFailure() throws Exception {
        // Non-WAL, s absent from partition 2024-01-01 (top -1): opening its index key file for the
        // rebuild from row 0 fails. The commit rolls back with the top still -1, and the next commit
        // rebuilds the index and zeroes the top.
        setInPlaceParquetO3Properties();
        final AtomicReference<String> failingKeyFile = new AtomicReference<>();
        final AtomicInteger failures = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                final String keyFile = failingKeyFile.get();
                if (keyFile != null && keyFile.equals(Utf8s.stringFromUtf8Bytes(name)) && failingKeyFile.compareAndSet(keyFile, null)) {
                    failures.incrementAndGet();
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            createColumnTopShapeTable("pt", ColumnTopShape.ABSENT, "", "BYPASS WAL", COLUMN_TOP_SHAPE_P1_ROWS, true);
            createColumnTopShapeTable("tw", ColumnTopShape.ABSENT, "", "BYPASS WAL", COLUMN_TOP_SHAPE_P1_ROWS, false);
            final long columnNameTxn = getColumnNameTxn("pt", 0, "s");
            final long txnBefore = getTableTxn("pt");
            try (Path path = new Path()) {
                setPartitionPath("pt", 0, path);
                failingKeyFile.set(Utf8s.stringFromUtf8Bytes(IndexFactory.keyFileName(IndexType.BITMAP, path, "s", columnNameTxn)));
            }
            final String o3Insert = columnTopShapeInsert("pt", columnTopShapeRow(101, "2024-01-01T04:30:00.000000Z", "x"));
            try {
                execute(o3Insert);
                Assert.fail("the index rebuild must fail");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "bulk update failed and will be rolled back");
            }
            Assert.assertEquals(1, failures.get());
            Assert.assertEquals(txnBefore, getTableTxn("pt"));
            Assert.assertEquals(-1, getColumnTop("pt", 0, "s"));
            Assert.assertEquals(-1, getColumnTop("pt", 0, "d"));
            assertQuery(COLUMN_TOP_SHAPE_NULL_SQL)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(COLUMN_TOP_SHAPE_P1_NULL_ROWS);
            assertColumnTopShapeQueriesMatchTwin("after the failed commit");

            execute(o3Insert);
            execute(o3Insert.replace("INSERT INTO pt ", "INSERT INTO tw "));
            assertColumnTopsZero("after the retry");
            Assert.assertEquals(columnNameTxn, getColumnNameTxn("pt", 0, "s"));
            assertQuery("SELECT v, ts, s FROM pt WHERE s = 'x'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            v\tts\ts
                            101\t2024-01-01T04:30:00.000000Z\tx
                            """);
            assertColumnTopShapeQueriesMatchTwin("after the retry");
        });
    }

    @Test
    public void testInPlaceColumnTopDedupSkip() throws Exception {
        // DEDUP SKIP, s absent from partition 2024-01-01 (top -1). A commit whose rows all duplicate
        // existing rows leaves the parquet file as it is, so the top stays -1. A commit that also adds
        // a row updates the file in place, which then holds every column from row 0: the top becomes 0.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            for (String table : new String[]{"pt", "tw"}) {
                execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts)");
                execute("INSERT INTO " + table + " (v, ts) VALUES " + COLUMN_TOP_SHAPE_P1_ROWS);
                execute("INSERT INTO " + table + " (v, ts) VALUES (99, '2024-01-02T00:00:00.000000Z')");
                addColumnTopShapeColumns(table, "");
                execute(columnTopShapeInsert(table, columnTopShapeRow(100, "2024-01-02T01:00:00.000000Z", "a")));
            }
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            final long txnBefore = getTableTxn("pt");
            for (String table : new String[]{"pt", "tw"}) {
                execute("INSERT INTO " + table + " (v, ts) VALUES (1, '2024-01-01T00:00:00.000000Z'), (6, '2024-01-01T05:00:00.000000Z')");
            }
            drainWalQueue();
            Assert.assertEquals(txnBefore + 1, getTableTxn("pt"));
            Assert.assertEquals(-1, getColumnTop("pt", 0, "s"));
            Assert.assertEquals(-1, getColumnTop("pt", 0, "d"));
            assertQuery(COLUMN_TOP_SHAPE_NULL_SQL)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(COLUMN_TOP_SHAPE_P1_NULL_ROWS);
            assertColumnTopShapeQueriesMatchTwin("dedup skip noop");

            for (String table : new String[]{"pt", "tw"}) {
                execute(columnTopShapeInsert(
                        table,
                        columnTopShapeRow(2, "2024-01-01T01:00:00.000000Z", "y"),
                        columnTopShapeRow(101, "2024-01-01T04:30:00.000000Z", "x")
                ));
            }
            drainWalQueue();
            assertColumnTopsZero("dedup skip partial");
            // the duplicate of row 2 is skipped, its s stays NULL
            assertQuery("SELECT v, ts, s FROM pt WHERE s IN ('x', 'y')")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            v\tts\ts
                            101\t2024-01-01T04:30:00.000000Z\tx
                            """);
            assertColumnTopShapeQueriesMatchTwin("dedup skip partial");
            execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();
            assertColumnTopShapeQueriesMatchTwin("dedup skip, native");
        });
    }

    @Test
    public void testLegacyColumnTopInsidePartitionAppend() throws Exception {
        assertLegacyColumnTopInsidePartition("2024-01-01T08:30:00.000000Z");
    }

    @Test
    public void testLegacyColumnTopInsidePartitionMerge() throws Exception {
        assertLegacyColumnTopInsidePartition("2024-01-01T04:30:00.000000Z");
    }

    @Test
    public void testLegacyAbsentColumnRepairedByNextCommit() throws Exception {
        // Fixture legacy_absent_merged: an earlier version merged a row with s 'x' into the parquet
        // partition 2024-01-01 in place and kept top -1 for s and d there. The next in-place commit
        // rebuilds the index of s from row 0 under the live name and zeroes the tops.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            loadStaleColumnTopFixture("legacy_absent_merged");
            createLegacyAbsentMergedTwin();
            final String table = "legacy_absent_merged";
            Assert.assertEquals(-1, getColumnTop(table, 0, "s"));
            Assert.assertEquals(-1, getColumnTop(table, 0, "d"));
            final long columnNameTxn = getColumnNameTxn(table, 0, "s");
            for (String t : new String[]{table, "tw"}) {
                execute("INSERT INTO " + t + " VALUES (102, '2024-01-01T09:30:00.000000Z', 'z', 2.5)");
            }
            Assert.assertEquals(0, getColumnTop(table, 0, "s"));
            Assert.assertEquals(0, getColumnTop(table, 0, "d"));
            Assert.assertEquals(columnNameTxn, getColumnNameTxn(table, 0, "s"));
            assertQuery("SELECT v, ts, s, d FROM " + table + " WHERE s = 'x'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            v\tts\ts\td
                            101\t2024-01-01T04:30:00.000000Z\tx\t1.5
                            """);
            assertLegacyQueriesMatchTwin(table, "repaired");
            execute("ALTER TABLE " + table + " CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            assertLegacyQueriesMatchTwin(table, "repaired, native");
        });
    }

    @Test
    public void testLegacyAbsentMergedConvertToNative() throws Exception {
        // Fixture legacy_absent_merged: s and d keep top -1 in parquet partition 2024-01-01, while the
        // file holds them from row 0, with row 101 = ('x', 1.5). CONVERT TO NATIVE takes the top from the
        // decoded rows, 0, and builds the index of s from them.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            final String table = "legacy_absent_merged";
            loadStaleColumnTopFixture(table);
            createLegacyAbsentMergedTwin();
            Assert.assertEquals(-1, getColumnTop(table, 0, "s"));
            execute("ALTER TABLE " + table + " CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            assertLegacyAbsentMergedNative(table, "native");
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertLegacyAbsentMergedNative(table, "native, reopen");
        });
    }

    @Test
    public void testLegacyAbsentMergedAlterColumnType() throws Exception {
        // ALTER COLUMN TYPE SYMBOL converts the legacy parquet partition to native in its pre-pass
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            final String table = "legacy_absent_merged";
            loadStaleColumnTopFixture(table);
            createLegacyAbsentMergedTwin();
            for (String t : new String[]{table, "tw"}) {
                execute("ALTER TABLE " + t + " ALTER COLUMN v TYPE SYMBOL");
            }
            Assert.assertEquals(0, getColumnTop(table, 0, "s"));
            Assert.assertEquals(0, getColumnTop(table, 0, "d"));
            assertQuery("SELECT v, ts, s, d FROM " + table + " WHERE s = 'x'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            v\tts\ts\td
                            101\t2024-01-01T04:30:00.000000Z\tx\t1.5
                            """);
            assertQueriesMatchTwin(table, "altered", LEGACY_ABSENT_NATIVE_QUERIES);
        });
    }

    @Test
    public void testLegacyLiveTopConvertToNative() throws Exception {
        // Fixtures legacy_live_top and legacy_live_top_posting: s, d and vc keep top 8 of 9 rows in parquet
        // partition 2024-01-01, while the file holds them from row 0. CONVERT TO NATIVE gives them top 0
        // and builds the index of s from the decoded rows. k keeps top 0, so CONVERT links its index.
        setInPlaceParquetO3Properties();
        final HardLinkRecordingFacade ff = new HardLinkRecordingFacade();
        assertMemoryLeak(ff, () -> {
            for (String table : new String[]{"legacy_live_top", "legacy_live_top_posting"}) {
                final boolean isPosting = table.endsWith("_posting");
                execute("DROP TABLE IF EXISTS tw");
                loadStaleColumnTopFixture(table);
                createLegacyLiveTopTwin(isPosting);
                Assert.assertEquals(table, 8, getColumnTop(table, 0, "s"));
                ff.linkedFileNames.clear();
                execute("ALTER TABLE " + table + " CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                Assert.assertTrue(table + " " + ff.linkedFileNames, ff.hasLinked("k."));
                Assert.assertFalse(table + " " + ff.linkedFileNames, ff.hasLinked("s."));
                assertLegacyLiveTopNative(table, table + ", native");
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                assertLegacyLiveTopNative(table, table + ", native, reopen");
            }
        });
    }

    @Test
    public void testLegacyRoundTripRepairsIndex() throws Exception {
        // CONVERT TO NATIVE and back to PARQUET repairs the index of a legacy partition: the parquet
        // index then agrees with the file, and a second CONVERT TO NATIVE reads the same rows
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            for (String table : new String[]{"legacy_absent_merged", "legacy_live_top", "legacy_live_top_posting"}) {
                execute("DROP TABLE IF EXISTS tw");
                loadStaleColumnTopFixture(table);
                final String[] queries;
                final String[] columns;
                if (table.equals("legacy_absent_merged")) {
                    createLegacyAbsentMergedTwin();
                    queries = LEGACY_ABSENT_NATIVE_QUERIES;
                    columns = new String[]{"s", "d"};
                } else {
                    createLegacyLiveTopTwin(table.endsWith("_posting"));
                    queries = LEGACY_LIVE_TOP_QUERIES;
                    columns = new String[]{"s", "d", "vc", "k"};
                }
                execute("ALTER TABLE " + table + " CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                // asserts the partition is parquet
                getParquetPartitionNameTxn(table, 0);
                for (String column : columns) {
                    Assert.assertEquals(table + ", column " + column, 0, getColumnTop(table, 0, column));
                }
                assertQueriesMatchTwin(table, table + ", parquet", queries);
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                assertQueriesMatchTwin(table, table + ", parquet, reopen", queries);
                execute("ALTER TABLE " + table + " CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                for (String column : columns) {
                    Assert.assertEquals(table + ", column " + column, 0, getColumnTop(table, 0, column));
                }
                assertQueriesMatchTwin(table, table + ", native again", queries);
            }
        });
    }

    @Test
    public void testConvertToNativeFailureKeepsColumnTopsLegacy() throws Exception {
        assertConvertToNativeFailureKeepsColumnTops(true, false);
    }

    @Test
    public void testConvertToNativeFailureKeepsColumnTopsLegacyAsyncWriter() throws Exception {
        assertConvertToNativeFailureKeepsColumnTops(true, true);
    }

    @Test
    public void testConvertToNativeFailureKeepsColumnTopsRowGroups() throws Exception {
        assertConvertToNativeFailureKeepsColumnTops(false, false);
    }

    @Test
    public void testConvertToNativeFailureKeepsColumnTopsRowGroupsAsyncWriter() throws Exception {
        assertConvertToNativeFailureKeepsColumnTops(false, true);
    }

    @Test
    public void testConvertToNativeTxnFailureWalApply() throws Exception {
        // WAL table, s, bt and st added after 5 of 8 rows of parquet partition 2024-01-01. The _cv commit
        // of CONVERT TO NATIVE lands, the _txn commit fails: the table suspends with the committed state
        // of before the CONVERT, and RESUME WAL applies it.
        setInPlaceParquetO3Properties();
        final ConvertToNativeFailingFacade ff = new ConvertToNativeFailingFacade("pt");
        assertMemoryLeak(ff, () -> {
            createConvertFailureTable("pt", "WAL", true);
            createConvertFailureTable("tw", "BYPASS WAL", false);
            for (String table : new String[]{"pt", "tw"}) {
                execute(CONVERT_FAILURE_PARTITIONS_INSERT.replace("{t}", table));
            }
            drainWalQueue();
            final long columnVersionBefore = readColumnVersionFileVersion("pt");
            final long seqTxnBefore;
            try (TableReader reader = getReader("pt")) {
                seqTxnBefore = reader.getSeqTxn();
            }
            final String[] snapshot = snapshotQueries("pt", CONVERT_FAILURE_QUERIES);
            ff.isArmed.set(true);
            execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();
            Assert.assertFalse(ff.isArmed.get());
            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 'pt'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("suspended\ntrue\n");
            try (TableReader reader = getReader("pt")) {
                Assert.assertEquals(seqTxnBefore, reader.getSeqTxn());
            }
            assertConvertFailureKeptState("pt", "failed WAL apply", columnVersionBefore, CONVERT_FAILURE_COLUMNS, new long[]{0, 0, 0}, snapshot);

            execute("ALTER TABLE pt RESUME WAL");
            drainWalQueue();
            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 'pt'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("suspended\nfalse\n");
            assertConvertFailureTableConverted("pt", "resumed");
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
            // one open builds the new version, and finishO3Commit() opens it once more to bind the
            // indexer of the append-blocked last partition, without truncating it
            Assert.assertEquals(2, ff.getOpenCount("s.k." + txn));
            Assert.assertEquals(2, ff.getOpenCount("k.k." + txn));
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
            // one open builds the new version, and finishO3Commit() opens it once more to bind the
            // indexer of the append-blocked last partition, without truncating it
            Assert.assertEquals(2, ff.getOpenCount("s.k." + txn));
            Assert.assertEquals(2, ff.getOpenCount("k.k." + txn));
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
        // Non-WAL commit into two parquet partitions: the committing thread runs the partition of the last
        // O3 row itself, before the queued one, so the second partition builds its new index version and
        // the first one fails. The commit rolls back and the second partition's new version goes.
        setInPlaceParquetO3Properties();
        final AtomicBoolean isArmed = new AtomicBoolean();
        final IndexFileOpenCounter ff = new IndexFileOpenCounter() {
            @Override
            public long openRO(LPSZ name) {
                if (isArmed.get() && Utf8s.containsAscii(name, "2024-01-01") && Utf8s.endsWithAscii(name, TableUtils.PARQUET_METADATA_FILE_NAME)) {
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
            final long firstDayColumnNameTxn = getColumnNameTxn("pt", 0, "s");
            final long secondDayColumnNameTxn = getColumnNameTxn("pt", 1, "s");
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
            // the second partition built its new index version before the first one failed
            Assert.assertEquals(1, ff.getOpenCount("s.k." + txnBefore));
            Assert.assertFalse(isIndexKeyFilePresent("pt", 1, IndexType.BITMAP, txnBefore));
            Assert.assertFalse(isIndexValueFilePresent("pt", 1, IndexType.BITMAP, txnBefore));
            Assert.assertEquals(firstDayColumnNameTxn, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals(secondDayColumnNameTxn, getColumnNameTxn("pt", 1, "s"));
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
    public void testInPlaceIndexVersionTxnMsyncFailureLandsBitmap() throws Exception {
        assertInPlaceIndexVersionTxnMsyncFailureLands("INDEX", "TIMESTAMP");
    }

    @Test
    public void testInPlaceIndexVersionTxnMsyncFailureLandsBitmapNanos() throws Exception {
        assertInPlaceIndexVersionTxnMsyncFailureLands("INDEX", "TIMESTAMP_NS");
    }

    @Test
    public void testInPlaceIndexVersionTxnMsyncFailureLandsPosting() throws Exception {
        assertInPlaceIndexVersionTxnMsyncFailureLands("INDEX TYPE POSTING", "TIMESTAMP");
    }

    @Test
    public void testInPlaceIndexVersionTxnMsyncFailureLandsPostingCovering() throws Exception {
        assertInPlaceIndexVersionTxnMsyncFailureLands("INDEX TYPE POSTING INCLUDE (v)", "TIMESTAMP");
    }

    @Test
    public void testInPlaceIndexVersionTxnMsyncFailureLandsPurgeFailure() throws Exception {
        // The _txn msync fails after the commit landed, and the rollback purge of the replaced index
        // version cannot remove its key file. The purge hands the version to the async purge: the writer
        // stays in the pool, the committed index answers, and the purge job removes the old version.
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        setInPlaceParquetO3Properties();
        final AtomicBoolean isRemoveArmed = new AtomicBoolean();
        final AtomicInteger refusedRemovals = new AtomicInteger();
        final AtomicReference<String> oldKeyFileSuffix = new AtomicReference<>();
        final TxnMsyncFailingFacade ff = new TxnMsyncFailingFacade("pt") {
            @Override
            public boolean removeQuiet(LPSZ name) {
                final String suffix = oldKeyFileSuffix.get();
                if (suffix != null && Utf8s.endsWithAscii(name, suffix) && isRemoveArmed.compareAndSet(true, false)) {
                    refusedRemovals.incrementAndGet();
                    return false;
                }
                return super.removeQuiet(name);
            }
        };
        final SymRow[] o3Rows = txnExtendingMergeRows();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE pt (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(symRowsInsert(SYM_BASE_ROWS));
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            final long columnNameTxn = getColumnNameTxn("pt", 0, "s");
            final long txnBefore = getTableTxn("pt");
            oldKeyFileSuffix.set(Files.SEPARATOR + (columnNameTxn > -1 ? "s.k." + columnNameTxn : "s.k"));
            // no pooled reader pins the txn before the commit, so the rollback purge runs at once
            engine.releaseInactive();
            isRemoveArmed.set(true);
            ff.isArmed.set(true);
            try {
                execute(symRowsInsert(o3Rows));
                Assert.fail("the _txn msync must fail");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not msync");
            }
            Assert.assertFalse(ff.isArmed.get());
            Assert.assertEquals(1, refusedRemovals.get());
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, columnNameTxn));
            Assert.assertEquals(txnBefore + 1, getTableTxn("pt"));
            Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
            Assert.assertTrue(isIndexValueFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
            // the pool kept the writer: taking it opens no _txn file
            final int txnOpenCount = ff.getTxnOpenCount();
            try (TableWriter writer = getWriter("pt")) {
                Assert.assertFalse(writer.isDistressed());
            }
            Assert.assertEquals(txnOpenCount, ff.getTxnOpenCount());
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS, o3Rows));

            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                purgeJob.run();
            }
            assertOnlyIndexVersion("after the purge job", "INDEX", 0, "s", txnBefore);
            assertSqlCursors(
                    "SELECT s, v, ts FROM pt WHERE s::VARCHAR = 'a'",
                    "SELECT s, v, ts FROM pt WHERE s = 'a'"
            );
        });
    }

    @Test
    public void testInPlaceIndexVersionTxnMsyncFailureLandsStaleCursor() throws Exception {
        // The _txn msync fails after the commit landed, while a cursor of the earlier txn reads the
        // replaced index version. The rollback purge leaves that version to the async purge, the cursor
        // keeps its snapshot, and once it closes the purge job removes the version.
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        setInPlaceParquetO3Properties();
        final TxnMsyncFailingFacade ff = new TxnMsyncFailingFacade("pt");
        final SymRow[] o3Rows = txnExtendingMergeRows();
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE pt (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(symRowsInsert(SYM_BASE_ROWS));
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            final long columnNameTxn = getColumnNameTxn("pt", 0, "s");
            final long txnBefore = getTableTxn("pt");
            try (
                    RecordCursorFactory factory = select(STALE_INDEX_QUERIES[0]);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                ff.isArmed.set(true);
                try {
                    execute(symRowsInsert(o3Rows));
                    Assert.fail("the _txn msync must fail");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not msync");
                }
                Assert.assertFalse(ff.isArmed.get());
                Assert.assertEquals(txnBefore + 1, getTableTxn("pt"));
                Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
                Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, columnNameTxn));
                Assert.assertTrue(isIndexValueFilePresent("pt", 0, IndexType.BITMAP, columnNameTxn));
                assertStaleCursor("after the landed commit", expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS), factory, cursor);
                assertQuery(STALE_INDEX_QUERIES[0])
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns(expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS, o3Rows));
            }
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                purgeJob.run();
            }
            assertOnlyIndexVersion("after the cursor closed", "INDEX", 0, "s", txnBefore);
            assertSqlCursors(
                    "SELECT s, v, ts FROM pt WHERE s::VARCHAR = 'a'",
                    "SELECT s, v, ts FROM pt WHERE s = 'a'"
            );
        });
    }

    @Test
    public void testInPlaceIndexVersionTxnMsyncFailureLandsTwoPartitions() throws Exception {
        // One non-WAL commit, run by a worker pool, merges into two parquet partitions with two indexed
        // columns, and its _txn msync fails after the commit landed. rollback() keeps the four new index
        // versions and purges the four replaced ones.
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        setInPlaceParquetO3Properties();
        final TxnMsyncFailingFacade ff = new TxnMsyncFailingFacade("pt");
        final SymRow[] firstDayRows = Arrays.copyOf(SYM_BASE_ROWS, 12);
        final SymRow[] secondDayRows = new SymRow[12];
        for (int i = 0; i < secondDayRows.length; i++) {
            secondDayRows[i] = new SymRow(SYM_BASE_ROWS[i].s, SYM_BASE_ROWS[i].v + 20, "2024-01-02", SYM_BASE_ROWS[i].time);
        }
        final SymRow[] lastDayRow = {new SymRow("a", 99, "2024-01-03", "00:00")};
        final SymRow[] o3Rows = {
                new SymRow("a", 101, "2024-01-01", "04:30"),
                new SymRow("a", 121, "2024-01-02", "04:30")
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE pt (s SYMBOL INDEX, t SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(twoSymbolRowsInsert("pt", firstDayRows));
            execute(twoSymbolRowsInsert("pt", secondDayRows));
            execute(twoSymbolRowsInsert("pt", lastDayRow));
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02'");
            final long txnBefore = getTableTxn("pt");
            final WorkerPool pool = new TestWorkerPool(2);
            try {
                WorkerPoolUtils.setupWriterJobs(pool, engine);
                pool.start(LOG);
                ff.isArmed.set(true);
                try {
                    execute(twoSymbolRowsInsert("pt", o3Rows));
                    Assert.fail("the _txn msync must fail");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not msync");
                }
            } finally {
                pool.halt();
            }
            Assert.assertFalse(ff.isArmed.get());
            Assert.assertEquals(txnBefore + 1, getTableTxn("pt"));
            final String expectedRows = symRowsText(false, row -> "a".equals(row.s), SYM_ROW_BY_TS, firstDayRows, secondDayRows, lastDayRow, o3Rows);
            assertQuery("SELECT s, v, ts FROM pt WHERE s = 'a'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedRows);
            assertQuery("SELECT t, v, ts FROM pt WHERE t = 'a'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedRows.replace("s\tv\tts", "t\tv\tts"));
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                purgeJob.run();
            }
            for (int partitionIndex = 0; partitionIndex < 2; partitionIndex++) {
                for (String columnName : new String[]{"s", "t"}) {
                    final String context = "partition " + partitionIndex + ", column " + columnName;
                    Assert.assertEquals(context, txnBefore, getColumnNameTxn("pt", partitionIndex, columnName));
                    assertOnlyIndexVersion(context, "INDEX", partitionIndex, columnName, txnBefore);
                }
            }
            assertSqlCursors(
                    "SELECT s, v, ts FROM pt WHERE s::VARCHAR = 'a'",
                    "SELECT s, v, ts FROM pt WHERE s = 'a'"
            );
            assertSqlCursors(
                    "SELECT t, v, ts FROM pt WHERE t::VARCHAR = 'a'",
                    "SELECT t, v, ts FROM pt WHERE t = 'a'"
            );
        });
    }

    @Test
    public void testInPlaceIndexVersionTxnMsyncFailureWal() throws Exception {
        // WAL: the _txn msync fails after the commit landed. The apply job marks the writer distressed
        // and suspends the table; after RESUME WAL the committed index answers and the commit is not
        // applied twice.
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        setInPlaceParquetO3Properties();
        final TxnMsyncFailingFacade ff = new TxnMsyncFailingFacade("pt");
        assertMemoryLeak(ff, () -> {
            createIndexedParquetTable("INDEX", "TIMESTAMP");
            final TableToken tableToken = engine.verifyTableName("pt");
            final long txnBefore = getTableTxn("pt");
            ff.isArmed.set(true);
            execute(symRowsInsert(SYM_MERGE_ROWS));
            drainWalQueue();
            Assert.assertFalse(ff.isArmed.get());
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));

            execute("ALTER TABLE pt RESUME WAL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertEquals(txnBefore + 1, getTableTxn("pt"));
            Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
            Assert.assertTrue(isIndexValueFilePresent("pt", 0, IndexType.BITMAP, txnBefore));
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS, SYM_MERGE_ROWS));
            assertQuery("SELECT count() FROM pt")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n" + (SYM_BASE_ROWS.length + SYM_MERGE_ROWS.length) + "\n");
            assertSqlCursors(
                    "SELECT s, v, ts FROM pt WHERE s::VARCHAR = 'a'",
                    "SELECT s, v, ts FROM pt WHERE s = 'a'"
            );
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
        // partition (added after it), u is not indexed. Only s moves to a new index version, t gets
        // top 0 under its live name.
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
            // the parquet file holds t from row 0 after the update
            Assert.assertTrue(getColumnVersionRecordIndex("pt", 0, "t") > -1);
            Assert.assertEquals(0, getColumnTop("pt", 0, "t"));
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
        // index query the full-scan rows after it. s starts in each column top shape: created with the
        // table, added after the partition exists (top -1), added while it is the last one (top at the
        // row count). Partition 2024-01-02, which no batch touches, is parquet too, with column late
        // added after 7 of its 10 rows: a top that spans a row group. At the end CONVERT TO NATIVE must
        // give the rows of a native twin table that got the same rows. For BITMAP, s also starts in
        // partition 2024-01-01 attached from another table, with s at a foreign column version equal to
        // the first merge's txn, and at one above it: every merge must move the index to a name above
        // every earlier one. For both index types, s also starts unindexed, with the index added while
        // partition 2024-01-01 is detached: ATTACH leaves it without index files, and the first append
        // must build the index under a new version. Some merges fail to msync the _txn file after it
        // switched its record: the commit landed, and after RESUME WAL it holds as any other merge. The
        // number of batches is a budget property.
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        setInPlaceParquetO3Properties();
        final TxnMsyncFailingFacade ff = new TxnMsyncFailingFacade("pt");
        final int batchCount = Integer.getInteger("questdb.fuzz.parquet.index.batches", 20);
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final String[] symbols = {"a", "b", "c", "d", null};
        final long dayMicros = 86_400_000_000L;
        final long dayStart = 1_704_067_200_000_000L; // 2024-01-01T00:00:00Z
        assertMemoryLeak(ff, () -> {
            // batches per shape, append and merge
            final int[][] shapeBatches = new int[ColumnTopShape.values().length][2];
            // merges into an attached partition whose live version equals the merge's txn, and lies above it
            final int[] attachedMerges = new int[2];
            // appends that build the index missing from the partition re-attached after ADD INDEX
            int healingAppends = 0;
            // merges whose commit landed although the _txn msync failed, per index type
            final int[] landedMsyncFailureMerges = new int[2];
            final ColumnTopShape[] shapes = ColumnTopShape.values();
            final String[] indexDefinitions = {"INDEX", "INDEX TYPE POSTING"};
            for (int indexDefinitionIndex = 0; indexDefinitionIndex < indexDefinitions.length; indexDefinitionIndex++) {
                final String indexDefinition = indexDefinitions[indexDefinitionIndex];
                // POSTING skips the attached starts: a POSTING index copied from another table hides the
                // rows its chain stamped with a txn above the reader's, a separate bug
                final int startCount = shapes.length + (indexDefinition.equals("INDEX") ? 2 : 0) + 1;
                for (int start = 0; start < startCount; start++) {
                    // the last start: s added unindexed, the index added while 2024-01-01 is detached
                    final boolean isIndexAddedWhileDetached = start == startCount - 1;
                    // the attached partition holds s from row 0, as the ZERO shape does
                    final boolean isAttached = !isIndexAddedWhileDetached && start >= shapes.length;
                    final ColumnTopShape shape = isAttached || isIndexAddedWhileDetached ? ColumnTopShape.ZERO : shapes[start];
                    final String startName = isIndexAddedWhileDetached ? "ATTACHED_INDEX_ADDED_WHILE_DETACHED" : isAttached ? "ATTACHED" : shape.name();
                    execute("DROP TABLE IF EXISTS pt");
                    execute("DROP TABLE IF EXISTS tw");
                    execute("DROP TABLE IF EXISTS src");
                    execute("CREATE TABLE pt (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    if (isAttached) {
                        execute("ALTER TABLE pt ADD COLUMN s SYMBOL " + indexDefinition);
                        // the symbols in the order src uses them first, so that they take the keys of the copied s.d
                        execute("INSERT INTO pt (s, v, ts) VALUES ('a', 0, '2024-01-03T00:00:00.000000Z'), ('b', 0, '2024-01-03T01:00:00.000000Z'), " +
                                "('c', 0, '2024-01-03T02:00:00.000000Z'), ('d', 0, '2024-01-03T03:00:00.000000Z')");
                    } else if (shape == ColumnTopShape.ZERO) {
                        execute("ALTER TABLE pt ADD COLUMN s SYMBOL" + (isIndexAddedWhileDetached ? "" : " " + indexDefinition));
                        // 40 rows half an hour apart, then the next day's row that lets CONVERT take the first day
                        execute("INSERT INTO pt (s, v, ts) SELECT rnd_symbol('a', 'b', 'c', NULL), x, '2024-01-01'::TIMESTAMP + (x - 1) * 1_800_000_000L FROM long_sequence(40)");
                    } else {
                        execute("INSERT INTO pt (v, ts) SELECT x, '2024-01-01'::TIMESTAMP + (x - 1) * 1_800_000_000L FROM long_sequence(40)");
                    }
                    if (shape == ColumnTopShape.AT_ROW_COUNT) {
                        execute("ALTER TABLE pt ADD COLUMN s SYMBOL " + indexDefinition);
                    }
                    execute("INSERT INTO pt (v, ts) VALUES (0, '2024-01-02T00:00:00.000000Z')");
                    if (shape == ColumnTopShape.ABSENT) {
                        execute("ALTER TABLE pt ADD COLUMN s SYMBOL " + indexDefinition);
                    }
                    execute("INSERT INTO pt (s, v, ts) VALUES ('a', 0, '2024-01-02T01:00:00.000000Z')");
                    execute("INSERT INTO pt (v, ts) SELECT x, '2024-01-02T02:00:00.000000Z'::TIMESTAMP + x * 60_000_000L FROM long_sequence(5)");
                    execute("ALTER TABLE pt ADD COLUMN late DOUBLE");
                    execute("INSERT INTO pt (s, v, ts, late) SELECT 'b', x, '2024-01-02T03:00:00.000000Z'::TIMESTAMP + x * 60_000_000L, x * 1.5 FROM long_sequence(3)");
                    drainWalQueue();
                    // the txn of the first merge, batch 1: after ATTACH, a commit per partition CONVERT takes,
                    // and the append of batch 0
                    long firstMergeTxn = getTableTxn("pt") + (isAttached ? 4 : 3);
                    if (isAttached) {
                        final long foreignNameTxn = start == shapes.length ? firstMergeTxn : firstMergeTxn + 1 + rnd.nextInt(40);
                        execute("CREATE TABLE src (v LONG, ts TIMESTAMP, s SYMBOL " + indexDefinition + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                        execute("INSERT INTO src (s, v, ts) VALUES ('a', 0, '2024-01-09T00:00:00.000000Z'), ('b', 0, '2024-01-09T01:00:00.000000Z'), " +
                                "('c', 0, '2024-01-09T02:00:00.000000Z'), ('d', 0, '2024-01-09T03:00:00.000000Z')");
                        execute("INSERT INTO src (s, v, ts) SELECT rnd_symbol('a', 'b', 'c', NULL), x, '2024-01-01'::TIMESTAMP + (x - 1) * 1_800_000_000L FROM long_sequence(40)");
                        updateSourceColumnAtTxn(0, foreignNameTxn);
                        attachFromSource(0, "2024-01-01");
                        Assert.assertEquals(foreignNameTxn, getColumnNameTxn("pt", 0, "s"));
                    }
                    execute("CREATE TABLE tw AS (SELECT * FROM pt) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02'");
                    drainWalQueue();
                    if (isIndexAddedWhileDetached) {
                        reattachPartition("2024-01-01", "ALTER TABLE pt ALTER COLUMN s ADD " + indexDefinition);
                        // ADD COLUMN gave s a column version, ATTACH built no index file
                        Assert.assertEquals(0, getColumnTop("pt", 0, "s"));
                        Assert.assertEquals("[]", listColumnFiles("pt", 0, "s"));
                        // after the append of batch 0
                        firstMergeTxn = getTableTxn("pt") + 1;
                    }
                    final long partitionNameTxn = getParquetPartitionNameTxn("pt", 0);
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
                        insert.put("INSERT INTO pt (s, v, ts) VALUES ");
                        for (int i = 0; i < rowCount; i++) {
                            final long timestamp = isAppend ? maxTimestamp + (i + 1) * 1_000_000L : dayStart + rnd.nextLong(maxTimestamp - dayStart);
                            final String symbol = symbols[rnd.nextInt(symbols.length)];
                            insert.put(i > 0 ? ", (" : "(").put(symbol == null ? "NULL" : "'" + symbol + "'").put(", ").put(nextV++).put(", ").put(timestamp).put("::TIMESTAMP)");
                        }
                        if (isAppend) {
                            maxTimestamp += rowCount * 1_000_000L;
                        }
                        Assert.assertTrue(maxTimestamp < dayStart + dayMicros);
                        // the first merge of each index type, and a random quarter of the others
                        final boolean isMsyncFailing = !isAppend && ((start == 0 && batch == 1) || rnd.nextInt(4) == 0);
                        final String context = indexDefinition + ", " + startName + ", batch " + batch + (isAppend ? ", append " : ", merge ")
                                + (isMsyncFailing ? "with a failing _txn msync " : "") + insert;
                        final boolean isHealingAppend = isIndexAddedWhileDetached && batch == 0;
                        if (isHealingAppend) {
                            // no index cursor opens before the append that builds the missing index: it
                            // would read index files ATTACH did not build, a separate bug
                            execute(insert);
                            execute(insert.toString().replace("INSERT INTO pt ", "INSERT INTO tw "));
                            drainWalQueue();
                        } else {
                            try (RecordCursorFactory factory = select(indexSql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                ff.isArmed.set(isMsyncFailing);
                                execute(insert);
                                execute(insert.toString().replace("INSERT INTO pt ", "INSERT INTO tw "));
                                drainWalQueue();
                                if (isMsyncFailing) {
                                    Assert.assertFalse(context, ff.isArmed.get());
                                    Assert.assertTrue(context, engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
                                    execute("ALTER TABLE pt RESUME WAL");
                                    drainWalQueue();
                                    Assert.assertEquals(context, txnBefore + 1, getTableTxn("pt"));
                                    landedMsyncFailureMerges[indexDefinitionIndex]++;
                                }
                                assertStaleCursor(context, rowsBefore, factory, cursor);
                            }
                        }
                        Assert.assertFalse(context, engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
                        Assert.assertEquals(context, partitionNameTxn, getParquetPartitionNameTxn("pt", 0));
                        if (batch == 1) {
                            Assert.assertEquals(context, firstMergeTxn, txnBefore);
                        }
                        // batch 0 is an append, after it s has top 0 in every shape. A merge names the new
                        // version after its txn, or one above the live version when that is not below the txn.
                        final long columnNameTxnAfter = getColumnNameTxn("pt", 0, "s");
                        if (isHealingAppend) {
                            // the only append that moves the name: it builds the index the append found missing
                            Assert.assertEquals(context, Math.max(txnBefore, columnNameTxnBefore + 1), columnNameTxnAfter);
                            healingAppends++;
                        } else if (isAppend) {
                            Assert.assertEquals(context, columnNameTxnBefore, columnNameTxnAfter);
                        } else if (isAttached) {
                            Assert.assertEquals(context, Math.max(txnBefore, columnNameTxnBefore + 1), columnNameTxnAfter);
                            if (columnNameTxnBefore >= txnBefore) {
                                attachedMerges[columnNameTxnBefore == txnBefore ? 0 : 1]++;
                            }
                        } else {
                            Assert.assertEquals(context, txnBefore, columnNameTxnAfter);
                        }
                        // the version names of the partition only grow
                        Assert.assertTrue(context, (isAppend && !isHealingAppend) || columnNameTxnAfter > columnNameTxnBefore);
                        Assert.assertEquals(context, 0, getColumnTop("pt", 0, "s"));
                        if (!isAttached && !isIndexAddedWhileDetached) {
                            shapeBatches[shape.ordinal()][isAppend ? 0 : 1]++;
                        }
                        assertSqlCursors(fullScanSql, indexSql);
                        assertSqlCursors(
                                "SELECT s, v, ts FROM pt WHERE s::VARCHAR IS NULL AND ts IN '2024-01-01'",
                                "SELECT s, v, ts FROM pt WHERE s IS NULL AND ts IN '2024-01-01'"
                        );
                        if (rnd.nextBoolean()) {
                            engine.releaseInactive();
                            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                                purgeJob.run();
                            }
                        }
                    }
                    execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01', '2024-01-02'");
                    drainWalQueue();
                    // the first row group of 2024-01-02 holds no row of late
                    Assert.assertEquals(4, getColumnTop("pt", 1, "late"));
                    assertSqlCursors("SELECT * FROM tw", "SELECT * FROM pt");
                }
            }
            for (ColumnTopShape shape : ColumnTopShape.values()) {
                Assert.assertTrue(shape + " appends", shapeBatches[shape.ordinal()][0] > 0);
                Assert.assertTrue(shape + " merges", shapeBatches[shape.ordinal()][1] > 0);
            }
            Assert.assertTrue("merges at a live version equal to the txn", attachedMerges[0] > 0);
            Assert.assertTrue("merges at a live version above the txn", attachedMerges[1] > 0);
            // one per index type
            Assert.assertEquals("appends that build a missing index", 2, healingAppends);
            for (int i = 0; i < indexDefinitions.length; i++) {
                Assert.assertTrue(indexDefinitions[i] + " merges with a failing _txn msync", landedMsyncFailureMerges[i] > 0);
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

    @Test
    public void testAttachedForeignIndexVersionBelowTxnO3Middle() throws Exception {
        // foreign version 2 below the merge's txn 3: the merge names the new version 3, as for a partition the table built
        assertAttachedForeignIndexVersionO3Merge(2, 3);
    }

    @Test
    public void testAttachedForeignIndexVersionEqualTxnO3Middle() throws Exception {
        // foreign version 3 equal to the merge's txn 3: version 3 is live, the merge names the new version 4
        assertAttachedForeignIndexVersionO3Merge(3, 4);
    }

    @Test
    public void testAttachedForeignIndexVersionOneAboveTxnO3Middle() throws Exception {
        assertAttachedForeignIndexVersionO3Merge(4, 5);
    }

    @Test
    public void testAttachedForeignIndexVersionFarAboveTxnO3Middle() throws Exception {
        assertAttachedForeignIndexVersionO3Merge(31, 32);
    }

    @Test
    public void testAttachedForeignIndexVersionTwoIndexedColumns() throws Exception {
        // The merge versions every live index of the partition, each column on its own name: s carries
        // foreign version 31 and moves to 32, t keeps the default version and moves to the txn, 3.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (s SYMBOL INDEX, t SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(twoSymbolRowsInsert("src", SYM_ATTACHED_ROWS));
            updateSourceColumnAtTxn(0, 31);
            Assert.assertEquals(-1, getColumnNameTxn("src", 0, "t"));
            execute("CREATE TABLE pt (s SYMBOL INDEX, t SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(twoSymbolRowsInsert("pt", SYM_ATTACH_TARGET_ROWS));
            attachFromSource(0, "2024-01-01");
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            Assert.assertEquals(3, getTableTxn("pt"));
            Assert.assertEquals(31, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals(-1, getColumnNameTxn("pt", 0, "t"));

            execute(twoSymbolRowsInsert("pt", SYM_MERGE_ROWS));
            Assert.assertEquals(32, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals(3, getColumnNameTxn("pt", 0, "t"));
            assertIndexMatchesFullScan("s");
            assertIndexMatchesFullScan("t");
            assertQuery("SELECT t, v, ts FROM pt WHERE t = 'a' AND ts IN '2024-01-01'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedStaleIndexQueryResult(0, false, SYM_ATTACHED_ROWS, SYM_MERGE_ROWS).replace("s\tv", "t\tv"));

            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                purgeJob.run();
            }
            assertOnlyIndexVersion("s", "INDEX", 0, "s", 32);
            assertOnlyIndexVersion("t", "INDEX", 0, "t", 3);
            assertIndexMatchesFullScan("s");
            assertIndexMatchesFullScan("t");
        });
    }

    @Test
    public void testAttachedForeignIndexVersionJobFailure() throws Exception {
        // The index rebuild fails after it created the key file of the new version: a non-WAL commit
        // fails, a WAL table suspends. The new version's files go, the live foreign version stays
        // byte for byte, and the next attempt commits the new version.
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
            // foreign version equal to the merge's txn 3, and far above it
            for (long foreignNameTxn : new long[]{3, 31}) {
                for (boolean isWal : new boolean[]{false, true}) {
                    final String context = "foreign version " + foreignNameTxn + (isWal ? ", WAL" : ", non-WAL");
                    final long newNameTxn = foreignNameTxn + 1;
                    execute("DROP TABLE IF EXISTS pt");
                    execute("DROP TABLE IF EXISTS src");
                    createAttachedForeignParquetTable("INDEX", isWal, foreignNameTxn);
                    final TableToken tableToken = engine.verifyTableName("pt");
                    final byte[] keyFileBefore = readIndexFile("pt", 0, IndexType.BITMAP, foreignNameTxn, true);
                    final byte[] valueFileBefore = readIndexFile("pt", 0, IndexType.BITMAP, foreignNameTxn, false);
                    ff.clear();
                    failingFileSuffix.set(Files.SEPARATOR + "s.v." + newNameTxn);
                    try {
                        execute(symRowsInsert(SYM_MERGE_ROWS));
                        drainWalQueue();
                        Assert.assertTrue(context + ": the commit must fail", isWal);
                    } catch (CairoException e) {
                        Assert.assertFalse(context, isWal);
                        TestUtils.assertContains(e.getFlyweightMessage(), "bulk update failed and will be rolled back");
                    } finally {
                        failingFileSuffix.set(null);
                    }
                    Assert.assertEquals(context, isWal, engine.getTableSequencerAPI().isSuspended(tableToken));
                    // the rebuild got as far as creating the key file of the new version
                    Assert.assertEquals(context, 1, ff.getOpenCount("s.k." + newNameTxn));
                    Assert.assertFalse(context, isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, newNameTxn));
                    Assert.assertFalse(context, isIndexValueFilePresent("pt", 0, IndexType.BITMAP, newNameTxn));
                    Assert.assertEquals(context, foreignNameTxn, getColumnNameTxn("pt", 0, "s"));
                    Assert.assertEquals(context, 3, getTableTxn("pt"));
                    Assert.assertArrayEquals(context, keyFileBefore, readIndexFile("pt", 0, IndexType.BITMAP, foreignNameTxn, true));
                    Assert.assertArrayEquals(context, valueFileBefore, readIndexFile("pt", 0, IndexType.BITMAP, foreignNameTxn, false));
                    assertAttachedRows(context + ", after the failure");

                    if (isWal) {
                        execute("ALTER TABLE pt RESUME WAL");
                        drainWalQueue();
                        Assert.assertFalse(context, engine.getTableSequencerAPI().isSuspended(tableToken));
                    } else {
                        execute(symRowsInsert(SYM_MERGE_ROWS));
                    }
                    Assert.assertEquals(context, newNameTxn, getColumnNameTxn("pt", 0, "s"));
                    assertAttachedRows(context + ", after the retry", SYM_MERGE_ROWS);
                }
            }
        });
    }

    @Test
    public void testAttachedForeignIndexVersionTxnCommitFailureRollback() throws Exception {
        // Non-WAL: the _cv commit lands, the _txn commit fails. rollback() restores _cv and removes the
        // new version 32, the foreign version 31 stays live, and the next commit builds 32 again.
        setInPlaceParquetO3Properties();
        final TxnExtendFailingFacade ff = new TxnExtendFailingFacade("pt");
        final SymRow[] o3Rows = txnExtendingMergeRows();
        assertMemoryLeak(ff, () -> {
            createAttachedForeignParquetTable("INDEX", false, 31);
            ff.isArmed.set(true);
            try {
                execute(symRowsInsert(o3Rows));
                Assert.fail("the _txn commit must fail");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
            }
            Assert.assertFalse(ff.isArmed.get());
            engine.releaseInactive();
            Assert.assertEquals(3, getTableTxn("pt"));
            Assert.assertEquals(31, getColumnNameTxn("pt", 0, "s"));
            Assert.assertFalse(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, 32));
            Assert.assertFalse(isIndexValueFilePresent("pt", 0, IndexType.BITMAP, 32));
            Assert.assertTrue(isIndexKeyFilePresent("pt", 0, IndexType.BITMAP, 31));
            assertAttachedRows("after the rollback");

            execute(symRowsInsert(o3Rows));
            Assert.assertEquals(32, getColumnNameTxn("pt", 0, "s"));
            assertAttachedRows("after the retry", o3Rows);
        });
    }

    @Test
    public void testAttachedForeignIndexVersionTwoMergesAcrossRestart() throws Exception {
        // The new version 32 survives a writer and reader restart, and the next merge, at txn 4, moves
        // the index on to 33: names in the partition only grow. The purge leaves 33 only.
        setInPlaceParquetO3Properties();
        final SymRow[] secondMergeRows = {
                new SymRow("d", 111, "2024-01-01", "01:30"),
                new SymRow(null, 112, "2024-01-01", "07:30")
        };
        assertMemoryLeak(() -> {
            createAttachedForeignParquetTable("INDEX", true, 31);
            execute(symRowsInsert(SYM_MERGE_ROWS));
            drainWalQueue();
            Assert.assertEquals(32, getColumnNameTxn("pt", 0, "s"));
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            Assert.assertEquals(32, getColumnNameTxn("pt", 0, "s"));
            assertAttachedRows("after the restart", SYM_MERGE_ROWS);

            Assert.assertEquals(4, getTableTxn("pt"));
            execute(symRowsInsert(secondMergeRows));
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
            Assert.assertEquals(33, getColumnNameTxn("pt", 0, "s"));
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                purgeJob.run();
            }
            assertOnlyIndexVersion("after two merges", "INDEX", 0, "s", 33);
            assertAttachedRows("after two merges", SYM_MERGE_ROWS, secondMergeRows);
        });
    }

    @Test
    public void testAttachedForeignIndexVersionPendingPurgeNoReuse() throws Exception {
        // Foreign version 4, one above the first merge's txn 3. The first merge replaces 4 with 5, and
        // the purge of 4 waits for the pinned reader. The second merge, at txn 4, must not name its
        // version 4: the queued purge would remove the live index. It names 6.
        setInPlaceParquetO3Properties();
        final SymRow[] secondMergeRows = {
                new SymRow("d", 111, "2024-01-01", "01:30"),
                new SymRow(null, 112, "2024-01-01", "07:30")
        };
        assertMemoryLeak(() -> {
            createAttachedForeignParquetTable("INDEX", false, 4);
            try (TableReader ignore = getReader("pt")) {
                execute(symRowsInsert(SYM_MERGE_ROWS));
                Assert.assertEquals(5, getColumnNameTxn("pt", 0, "s"));
                Assert.assertEquals(4, getTableTxn("pt"));
                execute(symRowsInsert(secondMergeRows));
                Assert.assertEquals(6, getColumnNameTxn("pt", 0, "s"));
            }
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                purgeJob.run();
            }
            assertOnlyIndexVersion("after the purge", "INDEX", 0, "s", 6);
            assertAttachedRows("after the purge", SYM_MERGE_ROWS, secondMergeRows);
        });
    }

    @Test
    public void testAttachedForeignIndexVersionDropIndex() throws Exception {
        // DROP INDEX after a merge into the attached partition removes the files of both the foreign
        // version 31 and the new version 32 once the purge runs
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            createAttachedForeignParquetTable("INDEX", true, 31);
            execute(symRowsInsert(SYM_MERGE_ROWS));
            drainWalQueue();
            Assert.assertEquals(32, getColumnNameTxn("pt", 0, "s"));
            execute("ALTER TABLE pt ALTER COLUMN s DROP INDEX");
            drainWalQueue();
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                purgeJob.run();
                setCurrentMicros(currentMicros + 1_000_000);
                purgeJob.run();
            }
            Assert.assertEquals("[]", listColumnFiles("pt", 0, "s"));
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedStaleIndexQueryResult(0, false, SYM_ATTACHED_ROWS, SYM_ATTACH_TARGET_ROWS, SYM_MERGE_ROWS));
        });
    }

    @Test
    public void testAttachedForeignIndexVersionConvertToNative() throws Exception {
        // CONVERT TO NATIVE after a merge into the attached partition writes s and its index under the
        // _cv name 32 into the native directory and purges the parquet one. Native O3 commits into the
        // middle and after the end then maintain that index.
        setInPlaceParquetO3Properties();
        final SymRow[] nativeMergeRows = {
                new SymRow("d", 111, "2024-01-01", "01:30"),
                new SymRow(null, 112, "2024-01-01", "07:30")
        };
        assertMemoryLeak(() -> {
            for (String indexDefinition : new String[]{"INDEX", "INDEX TYPE POSTING"}) {
                final String context = indexDefinition;
                execute("DROP TABLE IF EXISTS pt");
                execute("DROP TABLE IF EXISTS src");
                createAttachedForeignParquetTable(indexDefinition, true, 31);
                execute(symRowsInsert(SYM_MERGE_ROWS));
                drainWalQueue();
                Assert.assertEquals(context, 32, getColumnNameTxn("pt", 0, "s"));
                final String parquetDir;
                try (Path path = new Path()) {
                    setPartitionPath("pt", 0, path);
                    parquetDir = path.toString();
                }
                execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                drainWalQueue();
                Assert.assertEquals(context, PartitionFormat.NATIVE, getPartitionFormat("pt"));
                Assert.assertEquals(context, 32, getColumnNameTxn("pt", 0, "s"));
                Assert.assertEquals(
                        context,
                        indexDefinition.contains("POSTING") ? "[s.d.32, s.pk.32, s.pv.32.0]" : "[s.d.32, s.k.32, s.v.32]",
                        listColumnFiles("pt", 0, "s")
                );
                assertAttachedRows(context + ", native", SYM_MERGE_ROWS);
                engine.releaseInactive();
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    purgeJob.run();
                }
                Assert.assertFalse(context, new File(parquetDir).exists());

                execute(symRowsInsert(nativeMergeRows));
                execute(symRowsInsert(SYM_APPEND_ROWS));
                drainWalQueue();
                assertAttachedRows(context + ", native O3", SYM_MERGE_ROWS, nativeMergeRows, SYM_APPEND_ROWS);
            }
        });
    }

    @Test
    public void testAttachedForeignIndexVersionAppendKeepsName() throws Exception {
        // An in-order commit into the attached partition extends the live foreign version 31 in place;
        // the merge after it moves the index to 32. BITMAP only: a POSTING index copied from another
        // table hides the rows its chain stamped with a txn above the reader's, a separate bug.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            for (String indexDefinition : new String[]{"INDEX", "INDEX CAPACITY 2"}) {
                execute("DROP TABLE IF EXISTS pt");
                execute("DROP TABLE IF EXISTS src");
                createAttachedForeignParquetTable(indexDefinition, true, 31);
                execute(symRowsInsert(SYM_APPEND_ROWS));
                drainWalQueue();
                Assert.assertEquals(indexDefinition, 31, getColumnNameTxn("pt", 0, "s"));
                Assert.assertEquals(indexDefinition, "[s.k.31, s.v.31]", listColumnFiles("pt", 0, "s"));
                assertAttachedRows(indexDefinition + ", append", SYM_APPEND_ROWS);

                execute(symRowsInsert(SYM_MERGE_ROWS));
                drainWalQueue();
                Assert.assertEquals(indexDefinition, 32, getColumnNameTxn("pt", 0, "s"));
                assertAttachedRows(indexDefinition + ", merge", SYM_APPEND_ROWS, SYM_MERGE_ROWS);
            }
        });
    }

    @Test
    public void testAttachedForeignIndexVersionRewriteKeepsName() throws Exception {
        // A single row group takes the rewrite: the index goes into the new partition directory under
        // the foreign version 31
        setInPlaceParquetO3Properties();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createAttachedForeignParquetTable("INDEX", true, 31);
            final long partitionNameTxn = getParquetPartitionNameTxn("pt", 0);
            execute(symRowsInsert(SYM_MERGE_ROWS));
            drainWalQueue();
            Assert.assertNotEquals(partitionNameTxn, getParquetPartitionNameTxn("pt", 0));
            Assert.assertEquals(31, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals("[s.k.31, s.v.31]", listColumnFiles("pt", 0, "s"));
            assertAttachedRows("rewrite", SYM_MERGE_ROWS);
        });
    }

    @Test
    public void testAttachedForeignIndexVersionTwoPartitionsOneCommitWorkerPool() throws Exception {
        // One non-WAL commit, run by a worker pool, merges a row into two attached parquet partitions
        // whose s carries foreign versions 10 and 20. Each moves to its own next name, 11 and 21.
        setInPlaceParquetO3Properties();
        final SymRow[] secondDayRows = new SymRow[12];
        for (int i = 0; i < secondDayRows.length; i++) {
            secondDayRows[i] = new SymRow(SYM_ATTACHED_ROWS[i].s, SYM_ATTACHED_ROWS[i].v + 20, "2024-01-02", SYM_ATTACHED_ROWS[i].time);
        }
        final SymRow[] thirdDayRows = {
                new SymRow("a", 91, "2024-01-03", "01:00"),
                new SymRow("b", 92, "2024-01-03", "02:00"),
                new SymRow("", 93, "2024-01-03", "03:00"),
                new SymRow("c", 94, "2024-01-03", "04:00")
        };
        final SymRow[] o3Rows = {
                new SymRow("a", 101, "2024-01-01", "04:30"),
                new SymRow("a", 121, "2024-01-02", "04:30")
        };
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(symRowsInsert(SYM_ATTACHED_ROWS).replace("INSERT INTO pt ", "INSERT INTO src "));
            execute(symRowsInsert(secondDayRows).replace("INSERT INTO pt ", "INSERT INTO src "));
            updateSourceColumnAtTxn(0, 10);
            updateSourceColumnAtTxn(1, 20);
            execute("CREATE TABLE pt (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(symRowsInsert(thirdDayRows));
            copyPartitionFromSource(0, "2024-01-01");
            copyPartitionFromSource(1, "2024-01-02");
            engine.releaseAllWriters();
            execute("ALTER TABLE pt ATTACH PARTITION LIST '2024-01-01', '2024-01-02'");
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02'");
            // a commit per attached and per converted partition
            Assert.assertEquals(5, getTableTxn("pt"));
            Assert.assertEquals(10, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals(20, getColumnNameTxn("pt", 1, "s"));
            final String sql = "SELECT s, v, ts FROM pt WHERE s = 'a'";
            final WorkerPool pool = new TestWorkerPool(2);
            try {
                WorkerPoolUtils.setupWriterJobs(pool, engine);
                pool.start(LOG);
                // No cursor stays open across the commit: the pool's purge job would remove versions 10 and
                // 20 under it, as the purge gate of a version named above the commit's txn is empty (R1-RG).
                execute(symRowsInsert(o3Rows));
            } finally {
                pool.halt();
            }
            Assert.assertEquals(11, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals(21, getColumnNameTxn("pt", 1, "s"));
            assertQuery(sql)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(symRowsText(false, row -> "a".equals(row.s), SYM_ROW_BY_TS, SYM_ATTACHED_ROWS, secondDayRows, thirdDayRows, o3Rows));
            assertIndexMatchesFullScan("s");
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                purgeJob.run();
            }
            assertOnlyIndexVersion("first day", "INDEX", 0, "s", 11);
            assertOnlyIndexVersion("second day", "INDEX", 1, "s", 21);
        });
    }

    @Test
    public void testAttachedForeignIndexVersionAllNullSymbol() throws Exception {
        // the attached partition holds no s value but NULL: the merge moves the index from 31 to 32
        setInPlaceParquetO3Properties();
        final SymRow[] nullRows = new SymRow[8];
        for (int i = 0; i < nullRows.length; i++) {
            nullRows[i] = new SymRow(null, i + 1, "2024-01-01", "0" + i + ":00");
        }
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(symRowsInsert(nullRows).replace("INSERT INTO pt ", "INSERT INTO src "));
            updateSourceColumnAtTxn(0, 31);
            execute("CREATE TABLE pt (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(symRowsInsert(SYM_ATTACH_TARGET_ROWS));
            attachFromSource(0, "2024-01-01");
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            Assert.assertEquals(31, getColumnNameTxn("pt", 0, "s"));
            execute(symRowsInsert(SYM_MERGE_ROWS));
            Assert.assertEquals(32, getColumnNameTxn("pt", 0, "s"));
            assertQuery(STALE_INDEX_QUERIES[1])
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(symRowsText(false, row -> row.s == null && row.day.equals("2024-01-01"), SYM_ROW_BY_TS, nullRows, SYM_MERGE_ROWS));
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("s\tv\tts\na\t101\t2024-01-01T00:30:00.000000Z\n");
            assertIndexMatchesFullScan("s");
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                purgeJob.run();
            }
            assertOnlyIndexVersion("all NULL", "INDEX", 0, "s", 32);
        });
    }

    @Test
    public void testAttachedForeignIndexVersionAtServerDefaults() throws Exception {
        // Server default row group size (100_000) and rewrite thresholds: a merge into a 200_001-row
        // partition attached with foreign version 31 stays in place and moves the index to 32
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (s SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // s cycles through k1..k6, k0, one row every 100 ms from the start of the day
            execute("INSERT INTO src SELECT 'k' || (x % 7), x, '2024-01-01'::TIMESTAMP + (x - 1) * 100_000L FROM long_sequence(200_001)");
            updateSourceColumnAtTxn(0, 31);
            execute("CREATE TABLE pt (s SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // the symbols in the order src first uses them, so that they take the keys the copied s.d holds
            execute("INSERT INTO pt SELECT 'k' || (x % 7), 0, '2024-01-02'::TIMESTAMP + x * 1_000_000L FROM long_sequence(7)");
            drainWalQueue();
            attachFromSource(0, "2024-01-01");
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            final long partitionNameTxn = getParquetPartitionNameTxn("pt", 0);
            Assert.assertEquals(3, getTableTxn("pt"));
            Assert.assertEquals(31, getColumnNameTxn("pt", 0, "s"));
            execute("INSERT INTO pt VALUES ('k3', 300_001, '2024-01-01T01:00:00.050000Z')");
            drainWalQueue();
            Assert.assertEquals(partitionNameTxn, getParquetPartitionNameTxn("pt", 0));
            Assert.assertEquals(32, getColumnNameTxn("pt", 0, "s"));
            // k3 rows: x = 3, 10, ..., 200_000, i.e. 28_572 rows summing to 3 * 28_572 + 7 * (28_571 * 28_572 / 2), plus 300_001
            assertQuery("SELECT count(), min(v), max(v), sum(v) FROM pt WHERE s = 'k3' AND ts IN '2024-01-01'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tmin\tmax\tsum\n28573\t3\t300001\t2857542859\n");
            assertSqlCursors(
                    "SELECT s, v, ts FROM pt WHERE s::VARCHAR = 'k3'",
                    "SELECT s, v, ts FROM pt WHERE s = 'k3'"
            );
        });
    }

    @Test
    public void testAttachedMissingIndexAppendBuildsIndex() throws Exception {
        // ATTACH does not build an index added while the partition was detached (a separate bug): s keeps
        // top 0, column version -1 and no index files. An append into the parquet partition must build the
        // index from row 0 under a new column version named after the commit's txn, not extend live files
        // that do not exist. A factory compiled before the append, as a PostgreSQL wire prepared statement
        // is, returns the new rows.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            for (String indexDefinition : ATTACH_INDEX_DEFINITIONS) {
                final byte indexType = indexDefinition.contains("POSTING") ? IndexType.POSTING : IndexType.BITMAP;
                for (boolean isWal : new boolean[]{false, true}) {
                    for (String timestampType : new String[]{"TIMESTAMP", "TIMESTAMP_NS"}) {
                        final boolean isNanos = timestampType.equals("TIMESTAMP_NS");
                        final String context = indexDefinition + (isWal ? ", WAL, " : ", non-WAL, ") + timestampType;
                        execute("DROP TABLE IF EXISTS pt");
                        createReattachedMissingIndexTable(indexDefinition, isWal, timestampType, SYM_BASE_ROWS);
                        final long partitionNameTxn = getParquetPartitionNameTxn("pt", 0);
                        final long txnBefore = getTableTxn("pt");
                        try (RecordCursorFactory factory = select(STALE_INDEX_QUERIES[0])) {
                            execute(symRowsInsert(SYM_APPEND_ROWS));
                            drainWalQueue();
                            Assert.assertFalse(context, engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                assertStaleCursor(
                                        context + ", compiled before the append",
                                        expectedStaleIndexQueryResult(0, isNanos, SYM_BASE_ROWS, SYM_APPEND_ROWS),
                                        factory,
                                        cursor
                                );
                            }
                        }
                        Assert.assertEquals(context, txnBefore + 1, getTableTxn("pt"));
                        // the append updates the partition in place
                        Assert.assertEquals(context, partitionNameTxn, getParquetPartitionNameTxn("pt", 0));
                        Assert.assertEquals(context, 0, getColumnTop("pt", 0, "s"));
                        Assert.assertEquals(context, txnBefore, getColumnNameTxn("pt", 0, "s"));
                        Assert.assertTrue(context, isIndexKeyFilePresent("pt", 0, indexType, txnBefore));
                        assertOnlyIndexVersion(context, indexDefinition, 0, "s", txnBefore);
                        assertReattachedRows(context, isNanos, SYM_APPEND_ROWS);
                        // the queries read the index the append built
                        assertQuery(STALE_INDEX_QUERIES[0])
                                .noLeakCheck()
                                .inferTimestamp()
                                .inferRandomAccess()
                                .sizeMayVary()
                                .withPlanContaining(indexDefinition.contains("INCLUDE") ? "CoveringIndex on: s" : "Index forward scan on: s")
                                .returns(expectedStaleIndexQueryResult(0, isNanos, SYM_BASE_ROWS, SYM_APPEND_ROWS));
                        assertQuery(STALE_INDEX_QUERIES[5])
                                .noLeakCheck()
                                .inferTimestamp()
                                .inferRandomAccess()
                                .sizeMayVary()
                                .withPlanContaining(indexDefinition.contains("INCLUDE") ? "CoveringIndex on: s" : "Index backward scan on: s")
                                .returns(expectedStaleIndexQueryResult(5, isNanos, SYM_BASE_ROWS, SYM_APPEND_ROWS));
                    }
                }
            }
        });
    }

    @Test
    public void testAttachedMissingIndexSecondAppendExtendsLiveIndex() throws Exception {
        // The append that builds the missing index opens s.k.<txn> and s.v.<txn> once each and creates no
        // live s.k or s.v. Its single row starts a new row group after the 3 full ones. The next append
        // merges into that small row group and finds the new version's key file: it extends the index in
        // place, opening each file once and truncating it only on close, and names no version after its txn.
        setInPlaceParquetO3Properties();
        final LiveIndexTruncateCounter ff = new LiveIndexTruncateCounter();
        final SymRow[] healingRows = {SYM_APPEND_ROWS[0]};
        final SymRow[] secondRows = Arrays.copyOfRange(SYM_APPEND_ROWS, 1, SYM_APPEND_ROWS.length);
        assertMemoryLeak(ff, () -> {
            createReattachedMissingIndexTable("INDEX", true, "TIMESTAMP", SYM_BASE_ROWS);
            final long partitionNameTxn = getParquetPartitionNameTxn("pt", 0);
            final long healingTxn = getTableTxn("pt");
            final String keyFileName = "s.k." + healingTxn;
            final String valueFileName = "s.v." + healingTxn;
            try (Path path = new Path()) {
                setPartitionPath("pt", 0, path);
                ff.track(path.toString(), keyFileName, valueFileName);
            }

            execute(symRowsInsert(healingRows));
            drainWalQueue();
            Assert.assertEquals(partitionNameTxn, getParquetPartitionNameTxn("pt", 0));
            Assert.assertEquals(healingTxn, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals(1, ff.getOpenCount(keyFileName));
            Assert.assertEquals(1, ff.getOpenCount(valueFileName));
            Assert.assertEquals("[" + keyFileName + ", " + valueFileName + "]", listColumnFiles("pt", 0, "s"));
            assertReattachedRows("healing append", false, healingRows);

            final int keyTruncatesBefore = ff.getTruncateCount(keyFileName);
            final int valueTruncatesBefore = ff.getTruncateCount(valueFileName);
            final long secondTxn = getTableTxn("pt");
            execute(symRowsInsert(secondRows));
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
            Assert.assertEquals(secondTxn + 1, getTableTxn("pt"));
            Assert.assertEquals(partitionNameTxn, getParquetPartitionNameTxn("pt", 0));
            Assert.assertEquals(healingTxn, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals(2, ff.getOpenCount(keyFileName));
            Assert.assertEquals(2, ff.getOpenCount(valueFileName));
            Assert.assertEquals(keyTruncatesBefore + 1, ff.getTruncateCount(keyFileName));
            Assert.assertEquals(valueTruncatesBefore + 1, ff.getTruncateCount(valueFileName));
            Assert.assertEquals("[" + keyFileName + ", " + valueFileName + "]", listColumnFiles("pt", 0, "s"));
            assertReattachedRows("second append", false, healingRows, secondRows);
        });
    }

    @Test
    public void testAttachedMissingIndexTypeChangedWhileDetached() throws Exception {
        // The index of s changes type while partition 2024-01-01 is detached: ATTACH leaves the files of the
        // old type and none of the new one. The append must look for the key file of the current type, and
        // build the index of that type under a new version.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            for (String[] change : new String[][]{{"INDEX", "INDEX TYPE POSTING"}, {"INDEX TYPE POSTING", "INDEX"}}) {
                final byte oldIndexType = change[0].contains("POSTING") ? IndexType.POSTING : IndexType.BITMAP;
                final byte newIndexType = change[1].contains("POSTING") ? IndexType.POSTING : IndexType.BITMAP;
                for (boolean isWal : new boolean[]{false, true}) {
                    final String context = change[0] + " to " + change[1] + (isWal ? ", WAL" : ", non-WAL");
                    execute("DROP TABLE IF EXISTS pt");
                    execute("CREATE TABLE pt (s SYMBOL " + change[0] + ", v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
                    execute(symRowsInsert(SYM_BASE_ROWS));
                    drainWalQueue();
                    execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                    drainWalQueue();
                    reattachPartition("2024-01-01", "ALTER TABLE pt ALTER COLUMN s DROP INDEX", "ALTER TABLE pt ALTER COLUMN s ADD " + change[1]);
                    final long liveNameTxn = getColumnNameTxn("pt", 0, "s");
                    Assert.assertEquals(context, 0, getColumnTop("pt", 0, "s"));
                    Assert.assertTrue(context, isIndexKeyFilePresent("pt", 0, oldIndexType, liveNameTxn));
                    Assert.assertFalse(context, isIndexKeyFilePresent("pt", 0, newIndexType, liveNameTxn));

                    final long txnBefore = getTableTxn("pt");
                    execute(symRowsInsert(SYM_APPEND_ROWS));
                    drainWalQueue();
                    Assert.assertFalse(context, engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
                    final long newNameTxn = Math.max(txnBefore, liveNameTxn + 1);
                    Assert.assertEquals(context, newNameTxn, getColumnNameTxn("pt", 0, "s"));
                    Assert.assertTrue(context, isIndexKeyFilePresent("pt", 0, newIndexType, newNameTxn));
                    assertReattachedRows(context, false, SYM_APPEND_ROWS);
                }
            }
        });
    }

    @Test
    public void testAttachedMissingIndexAppendBuildFailure() throws Exception {
        // The append that builds the missing index fails: in a WAL table opening the key file of the new
        // version fails, in a non-WAL table opening its value file, after the build created the key file.
        // The failure leaves no file of the new version and no live index file, and s keeps version -1.
        // The retry builds the whole index: it returns the rows a full scan does, no partial file is trusted.
        setInPlaceParquetO3Properties();
        final AtomicReference<String> failingOpenName = new AtomicReference<>();
        final IndexFileOpenCounter ff = new IndexFileOpenCounter() {
            @Override
            public long openRW(LPSZ name, int opts) {
                final String failing = failingOpenName.get();
                if (failing != null && Utf8s.containsAscii(name, failing)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            for (String indexDefinition : new String[]{"INDEX", "INDEX TYPE POSTING"}) {
                final boolean isPosting = indexDefinition.contains("POSTING");
                for (boolean isWal : new boolean[]{false, true}) {
                    final String context = indexDefinition + (isWal ? ", WAL" : ", non-WAL");
                    execute("DROP TABLE IF EXISTS pt");
                    createReattachedMissingIndexTable(indexDefinition, isWal, "TIMESTAMP", SYM_BASE_ROWS);
                    final TableToken tableToken = engine.verifyTableName("pt");
                    final long txnBefore = getTableTxn("pt");
                    final String keyFileName = (isPosting ? "s.pk." : "s.k.") + txnBefore;
                    final String valueFileName = isPosting ? "s.pv." + txnBefore + '.' : "s.v." + txnBefore;
                    ff.clear();
                    failingOpenName.set(Files.SEPARATOR + (isWal ? keyFileName : valueFileName));
                    try {
                        execute(symRowsInsert(SYM_APPEND_ROWS));
                        drainWalQueue();
                        Assert.assertTrue(context + ": the commit must fail", isWal);
                    } catch (CairoException e) {
                        Assert.assertFalse(context, isWal);
                        TestUtils.assertContains(e.getFlyweightMessage(), "bulk update failed and will be rolled back");
                    } finally {
                        failingOpenName.set(null);
                    }
                    Assert.assertEquals(context, isWal, engine.getTableSequencerAPI().isSuspended(tableToken));
                    // non-WAL: the build got as far as creating the key file of the new version
                    Assert.assertEquals(context, isWal ? 0 : 1, ff.getOpenCount(keyFileName));
                    Assert.assertEquals(context, txnBefore, getTableTxn("pt"));
                    Assert.assertEquals(context, -1, getColumnNameTxn("pt", 0, "s"));
                    Assert.assertEquals(context, "[]", listColumnFiles("pt", 0, "s"));

                    if (isWal) {
                        execute("ALTER TABLE pt RESUME WAL");
                        drainWalQueue();
                        Assert.assertFalse(context, engine.getTableSequencerAPI().isSuspended(tableToken));
                    } else {
                        execute(symRowsInsert(SYM_APPEND_ROWS));
                    }
                    Assert.assertEquals(context, txnBefore + 1, getTableTxn("pt"));
                    Assert.assertEquals(context, txnBefore, getColumnNameTxn("pt", 0, "s"));
                    assertOnlyIndexVersion(context, indexDefinition, 0, "s", txnBefore);
                    assertReattachedRows(context + ", after the retry", false, SYM_APPEND_ROWS);
                }
            }
        });
    }

    @Test
    public void testAttachedMissingIndexTwoPartitionsOneCommitWorkerPool() throws Exception {
        // One non-WAL commit, run by a worker pool, appends to two parquet partitions. 2024-01-01, re-attached
        // without the index added while it was detached, builds the index under a new version. 2024-01-02,
        // whose index ADD INDEX built, extends its live index. Opening that live index fails on the first
        // attempt: the failure leaves no file of the new version in 2024-01-01, and the retry builds it.
        setInPlaceParquetO3Properties();
        final SymRow[] firstDayRows = Arrays.copyOf(SYM_BASE_ROWS, 12);
        final SymRow[] secondDayRows = new SymRow[12];
        for (int i = 0; i < secondDayRows.length; i++) {
            secondDayRows[i] = new SymRow(SYM_BASE_ROWS[i].s, SYM_BASE_ROWS[i].v + 20, "2024-01-02", SYM_BASE_ROWS[i].time);
        }
        final SymRow[] lastDayRow = {new SymRow("a", 99, "2024-01-03", "00:00")};
        final SymRow[] o3Rows = {
                new SymRow("a", 101, "2024-01-01", "11:30"),
                new SymRow("a", 121, "2024-01-02", "11:30")
        };
        final AtomicBoolean isArmed = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (
                        Utf8s.containsAscii(name, Files.SEPARATOR + "2024-01-02.")
                                && Utf8s.endsWithAscii(name, Files.SEPARATOR + "s.k")
                                && isArmed.compareAndSet(true, false)
                ) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE pt (s SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(symRowsInsert(firstDayRows));
            execute(symRowsInsert(secondDayRows));
            execute(symRowsInsert(lastDayRow));
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02'");
            reattachPartition("2024-01-01", "ALTER TABLE pt ALTER COLUMN s ADD INDEX");
            assertReattachedIndexMissing(0);
            Assert.assertEquals(-1, getColumnNameTxn("pt", 1, "s"));
            Assert.assertEquals("[s.k, s.v]", listColumnFiles("pt", 1, "s"));
            final long firstDayNameTxn = getParquetPartitionNameTxn("pt", 0);
            final long secondDayNameTxn = getParquetPartitionNameTxn("pt", 1);
            final long txnBefore = getTableTxn("pt");
            final String sql = "SELECT s, v, ts FROM pt WHERE s = 'a'";
            final WorkerPool pool = new TestWorkerPool(2);
            try {
                WorkerPoolUtils.setupWriterJobs(pool, engine);
                pool.start(LOG);
                isArmed.set(true);
                try {
                    execute(symRowsInsert(o3Rows));
                    Assert.fail("the commit must fail");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "bulk update failed and will be rolled back");
                }
                Assert.assertFalse(isArmed.get());
                Assert.assertEquals(txnBefore, getTableTxn("pt"));
                Assert.assertEquals(-1, getColumnNameTxn("pt", 0, "s"));
                Assert.assertEquals("[]", listColumnFiles("pt", 0, "s"));
                Assert.assertEquals(-1, getColumnNameTxn("pt", 1, "s"));
                Assert.assertEquals("[s.k, s.v]", listColumnFiles("pt", 1, "s"));

                execute(symRowsInsert(o3Rows));
            } finally {
                pool.halt();
            }
            Assert.assertEquals(txnBefore + 1, getTableTxn("pt"));
            Assert.assertEquals(firstDayNameTxn, getParquetPartitionNameTxn("pt", 0));
            Assert.assertEquals(secondDayNameTxn, getParquetPartitionNameTxn("pt", 1));
            Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals("[s.k." + txnBefore + ", s.v." + txnBefore + "]", listColumnFiles("pt", 0, "s"));
            Assert.assertEquals(-1, getColumnNameTxn("pt", 1, "s"));
            Assert.assertEquals("[s.k, s.v]", listColumnFiles("pt", 1, "s"));
            assertQuery(sql)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(symRowsText(false, row -> "a".equals(row.s), SYM_ROW_BY_TS, firstDayRows, secondDayRows, lastDayRow, o3Rows));
            assertIndexMatchesFullScan("s");
        });
    }

    @Test
    public void testAttachedMissingIndexSequenceAfterHealing() throws Exception {
        // After the append that builds the missing index, a second DETACH and ATTACH, with the index files
        // present, brings the partition back with that index, and the next append extends it in place.
        // Then DROP INDEX, CONVERT TO NATIVE and ADD INDEX give the rows a full scan does.
        setInPlaceParquetO3Properties();
        final SymRow[] secondAppendRows = {
                new SymRow("d", 111, "2024-01-01", "11:50"),
                new SymRow(null, 112, "2024-01-01", "11:55")
        };
        assertMemoryLeak(() -> {
            createReattachedMissingIndexTable("INDEX", true, "TIMESTAMP", SYM_BASE_ROWS);
            final TableToken tableToken = engine.verifyTableName("pt");
            final long healingTxn = getTableTxn("pt");
            final String healedFiles = "[s.k." + healingTxn + ", s.v." + healingTxn + "]";
            execute(symRowsInsert(SYM_APPEND_ROWS));
            drainWalQueue();
            Assert.assertEquals(healingTxn, getColumnNameTxn("pt", 0, "s"));

            reattachPartition("2024-01-01");
            Assert.assertEquals(healingTxn, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals(healedFiles, listColumnFiles("pt", 0, "s"));
            assertReattachedRows("reattached with the index", false, SYM_APPEND_ROWS);
            execute(symRowsInsert(secondAppendRows));
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertEquals(healingTxn, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals(healedFiles, listColumnFiles("pt", 0, "s"));
            assertReattachedRows("append after the second ATTACH", false, SYM_APPEND_ROWS, secondAppendRows);

            execute("ALTER TABLE pt ALTER COLUMN s DROP INDEX");
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();
            Assert.assertEquals(PartitionFormat.NATIVE, getPartitionFormat("pt"));
            execute("ALTER TABLE pt ALTER COLUMN s ADD INDEX");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertReattachedRows("native, index added again", false, SYM_APPEND_ROWS, secondAppendRows);
        });
    }

    @Test
    public void testAttachedMissingIndexRewriteAtServerDefaults() throws Exception {
        // Server default row group size (100_000) and rewrite thresholds: the append rewrites the 12-row
        // partition into a new directory and builds the index there under the live name
        assertMemoryLeak(() -> {
            for (String indexDefinition : new String[]{"INDEX", "INDEX TYPE POSTING"}) {
                final byte indexType = indexDefinition.contains("POSTING") ? IndexType.POSTING : IndexType.BITMAP;
                execute("DROP TABLE IF EXISTS pt");
                createReattachedMissingIndexTable(indexDefinition, true, "TIMESTAMP", SYM_BASE_ROWS);
                final long partitionNameTxn = getParquetPartitionNameTxn("pt", 0);
                execute(symRowsInsert(SYM_APPEND_ROWS));
                drainWalQueue();
                Assert.assertFalse(indexDefinition, engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
                Assert.assertNotEquals(indexDefinition, partitionNameTxn, getParquetPartitionNameTxn("pt", 0));
                Assert.assertEquals(indexDefinition, -1, getColumnNameTxn("pt", 0, "s"));
                Assert.assertTrue(indexDefinition, isIndexKeyFilePresent("pt", 0, indexType, -1));
                assertReattachedRows(indexDefinition, false, SYM_APPEND_ROWS);
            }
        });
    }

    @Test
    public void testAttachedMissingIndexTwoIndexedColumns() throws Exception {
        // t, indexed before partition 2024-01-01 was detached, keeps its index files across ATTACH; s,
        // indexed while it was detached, has none. The append versions the index of both: each moves to the
        // commit's txn, built from row 0.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pt (s SYMBOL, t SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(twoSymbolRowsInsert("pt", SYM_BASE_ROWS));
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            reattachPartition("2024-01-01", "ALTER TABLE pt ALTER COLUMN s ADD INDEX");
            assertReattachedIndexMissing(0);
            Assert.assertEquals(-1, getColumnNameTxn("pt", 0, "t"));
            Assert.assertEquals("[t.k, t.v]", listColumnFiles("pt", 0, "t"));

            final long txnBefore = getTableTxn("pt");
            execute(twoSymbolRowsInsert("pt", SYM_APPEND_ROWS));
            Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
            Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "t"));
            assertOnlyIndexVersion("s", "INDEX", 0, "s", txnBefore);
            // the purge of the replaced version of t waits for no reader
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                purgeJob.run();
            }
            assertOnlyIndexVersion("t", "INDEX", 0, "t", txnBefore);
            assertReattachedRows("s", false, SYM_APPEND_ROWS);
            assertIndexMatchesFullScan("t");
            assertQuery("SELECT t, v, ts FROM pt WHERE t = 'a' AND ts IN '2024-01-01'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedStaleIndexQueryResult(0, false, SYM_BASE_ROWS, SYM_APPEND_ROWS).replace("s\tv", "t\tv"));
        });
    }

    @Test
    public void testAttachedMissingIndexAllNullSymbol() throws Exception {
        // the re-attached partition holds no s value but NULL: the append builds its index from row 0
        setInPlaceParquetO3Properties();
        final SymRow[] nullRows = new SymRow[9];
        for (int i = 0; i < 8; i++) {
            nullRows[i] = new SymRow(null, i + 1, "2024-01-01", "0" + i + ":00");
        }
        nullRows[8] = new SymRow("a", 99, "2024-01-02", "00:00");
        assertMemoryLeak(() -> {
            createReattachedMissingIndexTable("INDEX", false, "TIMESTAMP", nullRows);
            final long txnBefore = getTableTxn("pt");
            execute(symRowsInsert(SYM_APPEND_ROWS));
            Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
            assertOnlyIndexVersion("all NULL", "INDEX", 0, "s", txnBefore);
            for (int q = 0; q < 4; q++) {
                assertQuery(STALE_INDEX_QUERIES[q])
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns(expectedStaleIndexQueryResult(q, false, nullRows, SYM_APPEND_ROWS));
            }
            assertIndexMatchesFullScan("s");
        });
    }

    @Test
    public void testAttachedMissingIndexHealingWithPinnedReader() throws Exception {
        // A cursor over partition 2024-01-01 opened before the append that builds the missing index keeps
        // returning its rows while a worker pool runs the commit. The commit schedules the purge of the
        // replaced version -1, whose files never existed: the purge finds nothing to remove and completes
        // without waiting for the cursor, and leaves the new version alone.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            createReattachedMissingIndexTable("INDEX", true, "TIMESTAMP", SYM_BASE_ROWS);
            final long txnBefore = getTableTxn("pt");
            final String fullScanSql = "SELECT s, v, ts FROM pt WHERE ts IN '2024-01-01'";
            final String rowsBefore = symRowsText(false, row -> row.day.equals("2024-01-01"), SYM_ROW_BY_TS, SYM_BASE_ROWS);
            final MessageBus messageBus = engine.getMessageBus();
            final long purgeTasksBefore = messageBus.getColumnPurgePubSeq().current();
            // the frozen clock keeps a taken purge task waiting for its retry delay until the test advances it
            setCurrentMicros(MicrosTimestampDriver.floor("2024-01-10T00:00:00.000000Z"));
            // the constructor replays and truncates the purge log the setup left behind
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                try (RecordCursorFactory factory = select(fullScanSql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final WorkerPool pool = new TestWorkerPool(2);
                    try {
                        // the pool runs the O3 jobs of the commit and the test's purge job, which takes the
                        // task but cannot purge it before the test advances the clock
                        pool.assign(new O3PartitionPurgeJob(engine));
                        pool.assign(purgeJob);
                        pool.assign(new ColumnIndexerJob(messageBus));
                        pool.assign(new O3PartitionJob(messageBus));
                        pool.assign(new O3OpenColumnJob(messageBus));
                        pool.assign(new O3CopyJob(messageBus));
                        pool.assign(new ColumnTaskJob(messageBus));
                        pool.start(LOG);
                        execute(symRowsInsert(SYM_APPEND_ROWS));
                        drainWalQueue();
                        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                        while (messageBus.getColumnPurgeSubSeq().current() < purgeTasksBefore + 1) {
                            Assert.assertTrue("the purge job in the pool did not take the task", System.nanoTime() < deadline);
                            Os.pause();
                        }
                    } finally {
                        pool.halt();
                    }
                    Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
                    Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
                    Assert.assertEquals(purgeTasksBefore + 1, messageBus.getColumnPurgePubSeq().current());
                    Assert.assertEquals(purgeTasksBefore + 1, messageBus.getColumnPurgeSubSeq().current());
                    Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
                    assertStaleCursor("pinned full scan", rowsBefore, factory, cursor);
                    purgeJob.run();
                    Assert.assertEquals(1, purgeJob.getOutstandingPurgeTasks());
                    setCurrentMicros(currentMicros + configuration.getColumnPurgeRetryDelay());
                    purgeJob.run();
                    Assert.assertEquals(0, purgeJob.getOutstandingPurgeTasks());
                    assertQuery("SELECT column_name, column_version, partition_timestamp, completed FROM '" + purgeJob.getLogTableName() + "'")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    column_name\tcolumn_version\tpartition_timestamp\tcompleted
                                    s\t-1\t2024-01-01T00:00:00.000000Z\t2024-01-10T00:00:00.000010Z
                                    """);
                    assertOnlyIndexVersion("pinned", "INDEX", 0, "s", txnBefore);
                    cursor.toTop();
                    assertStaleCursor("pinned full scan after the purge", rowsBefore, factory, cursor);
                }
            }
            setCurrentMicros(-1);
            assertOnlyIndexVersion("after the purge", "INDEX", 0, "s", txnBefore);
            assertReattachedRows("after the purge", false, SYM_APPEND_ROWS);
        });
    }

    @Test
    public void testAttachedMissingIndexAcrossRestart() throws Exception {
        // A writer and reader restart between ATTACH and the append, and after it: the append builds the
        // missing index, and the new column version persists
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            for (String indexDefinition : new String[]{"INDEX", "INDEX TYPE POSTING"}) {
                execute("DROP TABLE IF EXISTS pt");
                createReattachedMissingIndexTable(indexDefinition, true, "TIMESTAMP", SYM_BASE_ROWS);
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                assertReattachedIndexMissing(0);
                final long txnBefore = getTableTxn("pt");
                execute(symRowsInsert(SYM_APPEND_ROWS));
                drainWalQueue();
                Assert.assertEquals(indexDefinition, txnBefore, getColumnNameTxn("pt", 0, "s"));
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                Assert.assertEquals(indexDefinition, txnBefore, getColumnNameTxn("pt", 0, "s"));
                assertOnlyIndexVersion(indexDefinition, indexDefinition, 0, "s", txnBefore);
                assertReattachedRows(indexDefinition + ", after the restart", false, SYM_APPEND_ROWS);
            }
        });
    }

    @Test
    public void testAttachedMissingIndexCrashLeftoverRebuilt() throws Exception {
        // A crash during the append that builds the missing index skips the cleanup and leaves files of the
        // new version that no _cv entry references, garbage here. After the restart, the WAL apply builds
        // the version over them from scratch, and nothing of the leftovers reaches the index.
        setInPlaceParquetO3Properties();
        final byte[] garbage = new byte[4096];
        Arrays.fill(garbage, (byte) 0x5A);
        assertMemoryLeak(() -> {
            for (String indexDefinition : new String[]{"INDEX", "INDEX TYPE POSTING"}) {
                final boolean isPosting = indexDefinition.contains("POSTING");
                execute("DROP TABLE IF EXISTS pt");
                createReattachedMissingIndexTable(indexDefinition, true, "TIMESTAMP", SYM_BASE_ROWS);
                final long txnBefore = getTableTxn("pt");
                final String[] leftoverFileNames = isPosting
                        ? new String[]{"s.pk." + txnBefore, "s.pv." + txnBefore + ".0", "s.pv." + txnBefore + ".1", "s.pv." + txnBefore + ".2"}
                        : new String[]{"s.k." + txnBefore, "s.v." + txnBefore};
                try (Path path = new Path()) {
                    setPartitionPath("pt", 0, path);
                    for (String fileName : leftoverFileNames) {
                        java.nio.file.Files.write(Paths.get(path.toString(), fileName), garbage);
                    }
                }
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                execute(symRowsInsert(SYM_APPEND_ROWS));
                drainWalQueue();
                Assert.assertFalse(indexDefinition, engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
                Assert.assertEquals(indexDefinition, txnBefore, getColumnNameTxn("pt", 0, "s"));
                assertReattachedRows(indexDefinition, false, SYM_APPEND_ROWS);
            }
        });
    }

    @Test
    public void testAttachedMissingIndexFormatParquetTable() throws Exception {
        // FORMAT PARQUET table: partition 2024-01-01 is parquet from the start, and the append into it
        // after the ATTACH builds the index added while it was detached
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pt (s SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL");
            execute(symRowsInsert(SYM_BASE_ROWS));
            drainWalQueue();
            reattachPartition("2024-01-01", "ALTER TABLE pt ALTER COLUMN s ADD INDEX");
            assertReattachedIndexMissing(0);
            final long txnBefore = getTableTxn("pt");
            execute(symRowsInsert(SYM_APPEND_ROWS));
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
            Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
            assertOnlyIndexVersion("FORMAT PARQUET", "INDEX", 0, "s", txnBefore);
            assertReattachedRows("FORMAT PARQUET", false, SYM_APPEND_ROWS);
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

    private static byte getPartitionFormat(String tableName) {
        try (TableReader reader = getReader(tableName)) {
            return reader.getPartitionFormat(0);
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

    /**
     * The names of the files of column {@code columnName} in the directory of partition
     * {@code partitionIndex}, sorted.
     */
    private static String listColumnFiles(String tableName, int partitionIndex, String columnName) {
        try (Path path = new Path()) {
            setPartitionPath(tableName, partitionIndex, path);
            final String[] names = new File(path.toString()).list((dir, name) -> name.startsWith(columnName + '.'));
            Assert.assertNotNull(path.toString(), names);
            Arrays.sort(names);
            return Arrays.toString(names);
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

    /**
     * The text each query, run on {@code table}, returns. The queries name the table {t}.
     */
    private static String[] snapshotQueries(String table, String[] queries) throws SqlException {
        final String[] results = new String[queries.length];
        for (int i = 0; i < queries.length; i++) {
            printSql(queries[i].replace("{t}", table));
            results[i] = queries[i] + '\n' + sink;
        }
        return results;
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

    // inserts the rows into a table with two SYMBOL columns, s and t, both taking the row's symbol
    private static String twoSymbolRowsInsert(String tableName, SymRow... rows) {
        final StringSink sql = new StringSink();
        sql.put("INSERT INTO ").put(tableName).put(" (s, t, v, ts) VALUES ");
        for (int i = 0; i < rows.length; i++) {
            final SymRow row = rows[i];
            final String symbol = row.s == null ? "NULL" : "'" + row.s + "'";
            sql.put(i > 0 ? ", (" : "(").put(symbol).put(", ").put(symbol).put(", ").put(row.v).put(", '").put(row.timestampText(false)).put("')");
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

    /**
     * An O3 merge into the middle of a parquet partition attached from another table, whose s carries
     * the foreign column version {@code foreignNameTxn}, moves the index to version
     * {@code expectedNameTxn}: max(txn, foreignNameTxn + 1) for the merge at txn 3. Cursors opened
     * before the merge keep their snapshot, and the purge leaves only the new version.
     */
    private void assertAttachedForeignIndexVersionO3Merge(long foreignNameTxn, long expectedNameTxn) throws Exception {
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            for (String indexDefinition : ATTACH_INDEX_DEFINITIONS) {
                for (boolean isWal : new boolean[]{false, true}) {
                    final String context = indexDefinition + (isWal ? ", WAL" : ", non-WAL");
                    execute("DROP TABLE IF EXISTS pt");
                    execute("DROP TABLE IF EXISTS src");
                    createAttachedForeignParquetTable(indexDefinition, isWal, foreignNameTxn);
                    // A POSTING index copied from another table hides the rows its chain stamped with a txn
                    // above the reader's: the cursors over the foreign version, opened at txn 3, would read
                    // nothing of the attached partition. A separate bug, not this naming.
                    final boolean isForeignVersionReadable = !indexDefinition.contains("POSTING") || foreignNameTxn < 3;
                    final ObjList<RecordCursorFactory> factories = new ObjList<>();
                    final ObjList<RecordCursor> cursors = new ObjList<>();
                    try {
                        for (int q = 0; isForeignVersionReadable && q < STALE_INDEX_QUERIES.length; q++) {
                            final RecordCursorFactory factory = select(STALE_INDEX_QUERIES[q]);
                            factories.add(factory);
                            cursors.add(factory.getCursor(sqlExecutionContext));
                        }
                        execute(symRowsInsert(SYM_MERGE_ROWS));
                        drainWalQueue();
                        Assert.assertFalse(context, engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
                        for (int q = 0, n = cursors.size(); q < n; q++) {
                            assertStaleCursor(
                                    context + ", stale " + STALE_INDEX_QUERIES[q],
                                    expectedStaleIndexQueryResult(q, false, SYM_ATTACHED_ROWS, SYM_ATTACH_TARGET_ROWS),
                                    factories.getQuick(q),
                                    cursors.getQuick(q)
                            );
                        }
                    } finally {
                        Misc.freeObjList(cursors);
                        Misc.freeObjList(factories);
                    }
                    Assert.assertEquals(context, 4, getTableTxn("pt"));
                    Assert.assertEquals(context, expectedNameTxn, getColumnNameTxn("pt", 0, "s"));
                    assertAttachedRows(context, SYM_MERGE_ROWS);

                    // with the cursors closed, the purge removes the foreign version the merge replaced
                    engine.releaseInactive();
                    try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                        purgeJob.run();
                    }
                    assertOnlyIndexVersion(context, indexDefinition, 0, "s", expectedNameTxn);
                    assertAttachedRows(context, SYM_MERGE_ROWS);
                }
            }
        });
    }

    /**
     * Every STALE_INDEX_QUERIES query over table pt, built by createAttachedForeignParquetTable(),
     * returns the model rows of the attached partition, partition 2024-01-02 and {@code rowSets}, and
     * the index of s returns the rows a full scan does.
     */
    private void assertAttachedRows(String context, SymRow[]... rowSets) throws Exception {
        final SymRow[][] allRowSets = new SymRow[rowSets.length + 2][];
        allRowSets[0] = SYM_ATTACHED_ROWS;
        allRowSets[1] = SYM_ATTACH_TARGET_ROWS;
        System.arraycopy(rowSets, 0, allRowSets, 2, rowSets.length);
        assertStaleIndexQueryRows(context, false, allRowSets);
        assertIndexMatchesFullScan("s");
    }

    /**
     * Every STALE_INDEX_QUERIES query over table pt, built by createReattachedMissingIndexTable() from
     * SYM_BASE_ROWS, returns the model rows of SYM_BASE_ROWS and {@code rowSets}, the index of s returns
     * the rows a full scan does, and a key no row holds returns no row.
     */
    private void assertReattachedRows(String context, boolean isNanos, SymRow[]... rowSets) throws Exception {
        final SymRow[][] allRowSets = new SymRow[rowSets.length + 1][];
        allRowSets[0] = SYM_BASE_ROWS;
        System.arraycopy(rowSets, 0, allRowSets, 1, rowSets.length);
        assertStaleIndexQueryRows(context, isNanos, allRowSets);
        assertIndexMatchesFullScan("s");
        assertQuery("SELECT s, v, ts FROM pt WHERE s = 'zz'")
                .noLeakCheck()
                .inferTimestamp()
                .inferRandomAccess()
                .sizeMayVary()
                .returns("s\tv\tts\n");
    }

    private void assertStaleIndexQueryRows(String context, boolean isNanos, SymRow[][] rowSets) throws Exception {
        for (int q = 0; q < STALE_INDEX_QUERIES.length; q++) {
            try {
                assertQuery(STALE_INDEX_QUERIES[q])
                        .noLeakCheck()
                        .inferTimestamp()
                        .inferRandomAccess()
                        .sizeMayVary()
                        .returns(expectedStaleIndexQueryResult(q, isNanos, rowSets));
            } catch (AssertionError e) {
                throw new AssertionError(context + ", " + STALE_INDEX_QUERIES[q], e);
            }
        }
    }

    /**
     * Table pt holding {@code rows}, with s SYMBOL unindexed while its partition 2024-01-01, converted to
     * parquet, is attached. ALTER COLUMN s ADD {@code indexDefinition} runs while that partition is
     * detached, and ATTACH brings it back with s at top 0 and column version -1, but no index files.
     */
    private static void createReattachedMissingIndexTable(String indexDefinition, boolean isWal, String timestampType, SymRow[] rows) throws Exception {
        execute("CREATE TABLE pt (s SYMBOL, v INT, ts " + timestampType + ") TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
        execute(symRowsInsert(rows));
        drainWalQueue();
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
        reattachPartition("2024-01-01", "ALTER TABLE pt ALTER COLUMN s ADD " + indexDefinition);
        assertReattachedIndexMissing(0);
    }

    // the partition of table pt is parquet, and s has top 0, column version -1 and no index file there
    private static void assertReattachedIndexMissing(int partitionIndex) {
        getParquetPartitionNameTxn("pt", partitionIndex);
        Assert.assertEquals(0, getColumnTop("pt", partitionIndex, "s"));
        Assert.assertEquals(-1, getColumnNameTxn("pt", partitionIndex, "s"));
        Assert.assertEquals("[]", listColumnFiles("pt", partitionIndex, "s"));
    }

    // detaches partition {@code day} of table pt, runs {@code alterSqls} while it is detached, and attaches it back
    private static void reattachPartition(String day, String... alterSqls) throws SqlException {
        execute("ALTER TABLE pt DETACH PARTITION LIST '" + day + "'");
        drainWalQueue();
        for (String alterSql : alterSqls) {
            execute(alterSql);
            drainWalQueue();
        }
        final TableToken tableToken = engine.verifyTableName("pt");
        try (Path from = new Path(); Path to = new Path()) {
            from.of(configuration.getDbRoot()).concat(tableToken).concat(day).put(TableUtils.DETACHED_DIR_MARKER).$();
            to.of(configuration.getDbRoot()).concat(tableToken).concat(day).put(configuration.getAttachPartitionSuffix()).$();
            Assert.assertEquals(0, Files.rename(from.$(), to.$()));
        }
        execute("ALTER TABLE pt ATTACH PARTITION LIST '" + day + "'");
        drainWalQueue();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
    }

    // the index of column {@code columnName} of table pt returns the rows a full scan does, for each key class
    private void assertIndexMatchesFullScan(String columnName) throws SqlException {
        for (String predicate : new String[]{" = 'a'", " = ''", " = 'd'", " IS NULL"}) {
            assertSqlCursors(
                    "SELECT " + columnName + ", v, ts FROM pt WHERE " + columnName + "::VARCHAR" + predicate,
                    "SELECT " + columnName + ", v, ts FROM pt WHERE " + columnName + predicate
            );
        }
    }

    /**
     * Table pt with parquet partition 2024-01-01 attached from table src: it holds SYM_ATTACHED_ROWS,
     * and its s carries the foreign column version {@code foreignNameTxn}. Partition 2024-01-02 holds
     * SYM_ATTACH_TARGET_ROWS, and pt is at txn 3.
     */
    private void createAttachedForeignParquetTable(String indexDefinition, boolean isWal, long foreignNameTxn) throws Exception {
        execute("CREATE TABLE src (s SYMBOL " + indexDefinition + ", v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute(symRowsInsert(SYM_ATTACHED_ROWS).replace("INSERT INTO pt ", "INSERT INTO src "));
        updateSourceColumnAtTxn(0, foreignNameTxn);
        execute("CREATE TABLE pt (s SYMBOL " + indexDefinition + ", v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
        execute(symRowsInsert(SYM_ATTACH_TARGET_ROWS));
        drainWalQueue();
        attachFromSource(0, "2024-01-01");
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
        Assert.assertEquals(3, getTableTxn("pt"));
        Assert.assertEquals(foreignNameTxn, getColumnNameTxn("pt", 0, "s"));
    }

    /**
     * Copies the directory of partition {@code srcPartitionIndex} of table src, with every column
     * version it holds, to partition {@code day} of table pt, and attaches it.
     */
    private static void attachFromSource(int srcPartitionIndex, String day) throws SqlException {
        copyPartitionFromSource(srcPartitionIndex, day);
        engine.releaseAllWriters();
        execute("ALTER TABLE pt ATTACH PARTITION LIST '" + day + "'");
        drainWalQueue();
    }

    // copies the directory of partition srcPartitionIndex of table src to the attachable directory of partition day of table pt
    private static void copyPartitionFromSource(int srcPartitionIndex, String day) {
        try (Path from = new Path(); Path to = new Path()) {
            setPartitionPath("src", srcPartitionIndex, from);
            from.slash$();
            to.of(configuration.getDbRoot()).concat(engine.verifyTableName("pt")).concat(day).put(configuration.getAttachPartitionSuffix()).slash$();
            TestUtils.copyDirectory(from, to, configuration.getMkDirMode());
        }
    }

    /**
     * The files of column {@code columnName} in partition {@code partitionIndex} of table pt are the
     * index files of version {@code columnNameTxn} only: s.k and s.v for BITMAP; the key file and the
     * sealed value and covering files of that version for POSTING.
     */
    private static void assertOnlyIndexVersion(String context, String indexDefinition, int partitionIndex, String columnName, long columnNameTxn) {
        final String files = listColumnFiles("pt", partitionIndex, columnName);
        if (!indexDefinition.contains("POSTING")) {
            Assert.assertEquals(context, "[" + columnName + ".k." + columnNameTxn + ", " + columnName + ".v." + columnNameTxn + "]", files);
            return;
        }
        final String[] names = files.substring(1, files.length() - 1).split(", ");
        Assert.assertTrue(context + ' ' + files, Arrays.asList(names).contains(columnName + ".pk." + columnNameTxn));
        for (String name : names) {
            Assert.assertTrue(context + ' ' + files, name.matches(columnName + "\\.p[a-z0-9]+\\." + columnNameTxn + "(\\.\\d+)?"));
        }
    }

    /**
     * Moves table src to txn {@code foreignNameTxn} with commits into partition 2024-01-09, then
     * updates column s of partition {@code partitionIndex} under a pinned reader: s takes column
     * version {@code foreignNameTxn} there, and the previous version stays on disk beside it.
     */
    private static void updateSourceColumnAtTxn(int partitionIndex, long foreignNameTxn) throws SqlException {
        while (getTableTxn("src") < foreignNameTxn) {
            execute("INSERT INTO src (ts) VALUES ('2024-01-09T00:00:00.000000Z')");
        }
        Assert.assertEquals(foreignNameTxn, getTableTxn("src"));
        final String day;
        try (TableReader reader = getReader("src")) {
            day = Micros.toString(reader.getPartitionTimestampByIndex(partitionIndex)).substring(0, 10);
            execute("UPDATE src SET s = s WHERE ts IN '" + day + "'");
        }
        Assert.assertEquals(foreignNameTxn, getColumnNameTxn("src", partitionIndex, "s"));
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

    /**
     * Non-WAL: the _txn commit switches its record and then fails to msync it, so the commit has
     * landed. rollback(), run when the writer returns to the pool, finds the new index version in the
     * restored _cv: it keeps it and purges the replaced one. The next merge moves the index on, and the
     * index answers after a restart and through CONVERT TO NATIVE.
     */
    private void assertInPlaceIndexVersionTxnMsyncFailureLands(String indexDefinition, String timestampType) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        setInPlaceParquetO3Properties();
        final boolean isNanos = "TIMESTAMP_NS".equals(timestampType);
        // a covering index answers the query from the index: no random access, known size
        final boolean isCovering = indexDefinition.contains("INCLUDE");
        final TxnMsyncFailingFacade ff = new TxnMsyncFailingFacade("pt");
        final SymRow[] o3Rows = txnExtendingMergeRows();
        final SymRow[] nextMergeRows = {new SymRow("a", 201, "2024-01-01", "02:30")};
        final String referenceSql = "SELECT s, v, ts FROM pt WHERE s::VARCHAR = 'a'";
        final String indexSql = "SELECT s, v, ts FROM pt WHERE s = 'a'";
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE pt (s SYMBOL " + indexDefinition + ", v INT, ts " + timestampType + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(symRowsInsert(SYM_BASE_ROWS));
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            final long txnBefore = getTableTxn("pt");
            ff.isArmed.set(true);
            try {
                execute(symRowsInsert(o3Rows));
                Assert.fail("the _txn msync must fail");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not msync");
            }
            Assert.assertFalse(ff.isArmed.get());
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .supportsRandomAccess(!isCovering)
                    .expectSize(isCovering)
                    .returns(expectedStaleIndexQueryResult(0, isNanos, SYM_BASE_ROWS, o3Rows));
            assertSqlCursors(referenceSql, indexSql);
            Assert.assertEquals(txnBefore + 1, getTableTxn("pt"));
            Assert.assertEquals(txnBefore, getColumnNameTxn("pt", 0, "s"));
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                purgeJob.run();
            }
            assertOnlyIndexVersion("after the landed commit", indexDefinition, 0, "s", txnBefore);

            // the next merge moves the index to the next version and purges the one the landed commit built
            final long nextTxn = getTableTxn("pt");
            execute(symRowsInsert(nextMergeRows));
            Assert.assertEquals(nextTxn, getColumnNameTxn("pt", 0, "s"));
            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                purgeJob.run();
            }
            assertOnlyIndexVersion("after the next merge", indexDefinition, 0, "s", nextTxn);
            final String expectedRows = expectedStaleIndexQueryResult(0, isNanos, SYM_BASE_ROWS, o3Rows, nextMergeRows);
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .supportsRandomAccess(!isCovering)
                    .expectSize(isCovering)
                    .returns(expectedRows);

            engine.releaseAllWriters();
            engine.releaseAllReaders();
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .supportsRandomAccess(!isCovering)
                    .expectSize(isCovering)
                    .returns(expectedRows);
            execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            assertQuery(STALE_INDEX_QUERIES[0])
                    .noLeakCheck()
                    .timestamp("ts")
                    .supportsRandomAccess(!isCovering)
                    .expectSize(isCovering)
                    .returns(expectedRows);
            assertSqlCursors(referenceSql, indexSql);
        });
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
        assertSqlCursors(
                "SELECT v, ts, s FROM pt WHERE s::VARCHAR = 'x'",
                "SELECT v, ts, s FROM pt WHERE s = 'x'"
        );
        assertSqlCursors(
                "SELECT v, ts, s FROM pt WHERE s::VARCHAR IS NULL",
                "SELECT v, ts, s FROM pt WHERE s IS NULL"
        );
    }

    private static void assertColumnTopsZero(String context) {
        for (String column : COLUMN_TOP_SHAPE_COLUMN_NAMES) {
            Assert.assertEquals(context + ", column " + column, 0, getColumnTop("pt", 0, column));
        }
    }

    private static void assertColumnTopShapeQueriesMatchTwin(String context) throws Exception {
        assertQueriesMatchTwin("pt", context, COLUMN_TOP_SHAPE_QUERIES);
    }

    /**
     * Every query, run on {@code table}, returns what it returns on the native twin table tw, which
     * received the same DDL and rows but never became parquet. The queries name the table {t}.
     */
    private static void assertQueriesMatchTwin(String table, String context, String[] queries) throws Exception {
        for (String sql : queries) {
            try {
                assertSqlCursors(sql.replace("{t}", "tw"), sql.replace("{t}", table));
            } catch (AssertionError e) {
                throw new AssertionError(context + ": " + sql, e);
            }
        }
    }

    private void assertInPlaceColumnTopShapes(String indexSuffix) throws Exception {
        // For each start shape of the column tops of s and of the columns added with it, an append and
        // a merge in place. Afterwards _cv says what the parquet file holds, every column from row 0,
        // and every index query and CONVERT TO NATIVE agree with the native twin table.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            for (String walMode : new String[]{"WAL", "BYPASS WAL"}) {
                for (ColumnTopShape shape : ColumnTopShape.values()) {
                    for (boolean isAppend : new boolean[]{true, false}) {
                        final String context = "index " + indexSuffix + ", " + walMode + ", " + shape + (isAppend ? ", append" : ", merge");
                        execute("DROP TABLE IF EXISTS pt");
                        execute("DROP TABLE IF EXISTS tw");
                        createColumnTopShapeTable("pt", shape, indexSuffix, walMode, COLUMN_TOP_SHAPE_P1_ROWS, true);
                        createColumnTopShapeTable("tw", shape, indexSuffix, walMode, COLUMN_TOP_SHAPE_P1_ROWS, false);
                        final long expectedTop = switch (shape) {
                            case ZERO -> 0;
                            case ABSENT -> -1;
                            case AT_ROW_COUNT -> 8;
                        };
                        for (String column : COLUMN_TOP_SHAPE_COLUMN_NAMES) {
                            Assert.assertEquals(context + ", column " + column, expectedTop, getColumnTop("pt", 0, column));
                        }
                        final long partitionNameTxn = getParquetPartitionNameTxn("pt", 0);
                        final String[] o3Rows = isAppend ? COLUMN_TOP_SHAPE_APPEND_ROWS : COLUMN_TOP_SHAPE_MERGE_ROWS;
                        try (
                                RecordCursorFactory factory = select(COLUMN_TOP_SHAPE_NULL_SQL);
                                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                        ) {
                            execute(columnTopShapeInsert("pt", o3Rows));
                            execute(columnTopShapeInsert("tw", o3Rows));
                            drainWalQueue();
                            assertStaleCursor(context, COLUMN_TOP_SHAPE_P1_NULL_ROWS, factory, cursor);
                        }
                        Assert.assertEquals(context, partitionNameTxn, getParquetPartitionNameTxn("pt", 0));
                        assertColumnTopsZero(context);
                        assertQuery("SELECT v, ts, s FROM pt WHERE s = 'x'")
                                .noLeakCheck()
                                .timestamp("ts")
                                .returns(isAppend
                                        ? """
                                        v\tts\ts
                                        101\t2024-01-01T08:10:00.000000Z\tx
                                        105\t2024-01-01T08:50:00.000000Z\tx
                                        """
                                        : """
                                        v\tts\ts
                                        101\t2024-01-01T00:30:00.000000Z\tx
                                        105\t2024-01-01T09:30:00.000000Z\tx
                                        """);
                        assertQuery("SELECT v, ts, s FROM pt WHERE s = 'x'")
                                .noLeakCheck()
                                .assertsPlanContaining("Index forward scan on: s");
                        assertColumnTopShapeQueriesMatchTwin(context);
                        assertQueriesMatchTwin("pt", context, COLUMN_TOP_SHAPE_COVERED_QUERIES);
                        execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                        drainWalQueue();
                        assertColumnTopShapeQueriesMatchTwin(context + ", native");
                        // in the ZERO shape s is the first column added to the empty table, so its column
                        // name txn is 0, which its covering .pc file names share with txn NONE
                        assertQueriesMatchTwin("pt", context + ", native", COLUMN_TOP_SHAPE_COVERED_QUERIES);
                    }
                }
            }
        });
    }

    private void assertLegacyColumnTopInsidePartition(String o3Timestamp) throws Exception {
        // Fixtures legacy_live_top and legacy_live_top_posting: an earlier version left top 8 of 9 rows
        // for s, d and vc in parquet partition 2024-01-01, so readers use the index of s with 8 implicit
        // NULL rows. k has top 0. The next in-place commit, an append or a merge, zeroes the tops and
        // moves the index of every live column of the partition, s and k, to the version named after the
        // commit's txn. Cursors opened before it keep their rows, and the purge removes the old versions.
        setInPlaceParquetO3Properties();
        assertMemoryLeak(() -> {
            for (String table : new String[]{"legacy_live_top", "legacy_live_top_posting"}) {
                final boolean isPosting = table.endsWith("_posting");
                final byte indexType = isPosting ? IndexType.POSTING : IndexType.BITMAP;
                final String context = table + ", O3 row at " + o3Timestamp;
                execute("DROP TABLE IF EXISTS tw");
                loadStaleColumnTopFixture(table);
                createLegacyLiveTopTwin(isPosting);
                for (String column : new String[]{"s", "d", "vc"}) {
                    Assert.assertEquals(context + ", column " + column, 8, getColumnTop(table, 0, column));
                }
                Assert.assertEquals(context, 0, getColumnTop(table, 0, "k"));
                final long partitionNameTxn = getParquetPartitionNameTxn(table, 0);
                final long sNameTxn = getColumnNameTxn(table, 0, "s");
                final long kNameTxn = getColumnNameTxn(table, 0, "k");
                final long txnBefore = getTableTxn(table);
                final String o3Insert = "INSERT INTO " + table + " (k, v, ts, d, vc, s) VALUES ('a', 101, '" + o3Timestamp + "', 1.5, 'x101', 'x')";
                try (
                        RecordCursorFactory nullFactory = select("SELECT v, ts, s FROM " + table + " WHERE s IS NULL AND ts IN '2024-01-01'");
                        RecordCursor nullCursor = nullFactory.getCursor(sqlExecutionContext);
                        RecordCursorFactory valueFactory = select("SELECT v, ts, s FROM " + table + " WHERE s = 'y'");
                        RecordCursor valueCursor = valueFactory.getCursor(sqlExecutionContext);
                        RecordCursorFactory kFactory = select("SELECT v, ts, k FROM " + table + " WHERE k = 'a' AND ts IN '2024-01-01'");
                        RecordCursor kCursor = kFactory.getCursor(sqlExecutionContext)
                ) {
                    execute(o3Insert);
                    execute(o3Insert.replace("INSERT INTO " + table + " ", "INSERT INTO tw "));
                    assertStaleCursor(context + ", s IS NULL", COLUMN_TOP_SHAPE_P1_NULL_ROWS, nullFactory, nullCursor);
                    assertStaleCursor(
                            context + ", s = 'y'",
                            """
                                    v\tts\ts
                                    9\t2024-01-01T08:00:00.000000Z\ty
                                    """,
                            valueFactory,
                            valueCursor
                    );
                    assertStaleCursor(
                            context + ", k = 'a'",
                            """
                                    v\tts\tk
                                    1\t2024-01-01T00:00:00.000000Z\ta
                                    3\t2024-01-01T02:00:00.000000Z\ta
                                    5\t2024-01-01T04:00:00.000000Z\ta
                                    7\t2024-01-01T06:00:00.000000Z\ta
                                    """,
                            kFactory,
                            kCursor
                    );
                }
                Assert.assertEquals(context, partitionNameTxn, getParquetPartitionNameTxn(table, 0));
                Assert.assertEquals(context, txnBefore, getColumnNameTxn(table, 0, "s"));
                Assert.assertEquals(context, txnBefore, getColumnNameTxn(table, 0, "k"));
                for (String column : new String[]{"s", "d", "vc", "k"}) {
                    Assert.assertEquals(context + ", column " + column, 0, getColumnTop(table, 0, column));
                }
                Assert.assertTrue(context, isIndexKeyFilePresent(table, 0, indexType, "s", txnBefore));
                Assert.assertTrue(context, isIndexKeyFilePresent(table, 0, IndexType.BITMAP, "k", txnBefore));
                assertQuery("SELECT v, ts, s FROM " + table + " WHERE s = 'x'")
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns("v\tts\ts\n101\t" + o3Timestamp + "\tx\n");
                assertQueriesMatchTwin(table, context, LEGACY_LIVE_TOP_QUERIES);

                engine.releaseInactive();
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    purgeJob.run();
                }
                Assert.assertFalse(context, isIndexKeyFilePresent(table, 0, indexType, "s", sNameTxn));
                Assert.assertFalse(context, isIndexKeyFilePresent(table, 0, IndexType.BITMAP, "k", kNameTxn));
                Assert.assertTrue(context, isIndexKeyFilePresent(table, 0, indexType, "s", txnBefore));
                Assert.assertTrue(context, isIndexKeyFilePresent(table, 0, IndexType.BITMAP, "k", txnBefore));
                assertQueriesMatchTwin(table, context + ", after purge", LEGACY_LIVE_TOP_QUERIES);

                execute("ALTER TABLE " + table + " CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                assertQueriesMatchTwin(table, context + ", native", LEGACY_LIVE_TOP_QUERIES);
            }
        });
    }

    /**
     * After a failed CONVERT TO NATIVE, the _cv file, the column tops and the partition are those of
     * before, the new partition directory is gone, and the queries return what they did before.
     */
    private static void assertConvertFailureKeptState(
            String table,
            String context,
            long columnVersion,
            String[] columns,
            long[] tops,
            String[] querySnapshot
    ) throws Exception {
        Assert.assertEquals(context, columnVersion, readColumnVersionFileVersion(table));
        // asserts the partition is parquet
        getParquetPartitionNameTxn(table, 0);
        for (int i = 0; i < columns.length; i++) {
            Assert.assertEquals(context + ", column " + columns[i], tops[i], getColumnTop(table, 0, columns[i]));
        }
        final File tableDir = new File(configuration.getDbRoot().toString(), engine.verifyTableName(table).getDirName());
        final String[] partitionDirs = tableDir.list((_, name) -> name.startsWith("2024-01-01"));
        Assert.assertNotNull(partitionDirs);
        Assert.assertEquals(context + ": " + Arrays.toString(partitionDirs), 1, partitionDirs.length);
        Assert.assertArrayEquals(context, querySnapshot, snapshotQueries(table, CONVERT_FAILURE_QUERIES));
    }

    /**
     * Table pt of the failure tests, converted: s and st start at row group 1, bt keeps its top of 5.
     */
    private void assertConvertFailureTableConverted(String table, String context) throws Exception {
        Assert.assertEquals(context, PartitionFormat.NATIVE, getPartitionFormat(table));
        Assert.assertEquals(context, 4, getColumnTop(table, 0, "s"));
        Assert.assertEquals(context, 5, getColumnTop(table, 0, "bt"));
        Assert.assertEquals(context, 4, getColumnTop(table, 0, "st"));
        assertQuery("SELECT v, ts, s, bt, st FROM " + table + " WHERE ts IN '2024-01-01'")
                .noLeakCheck()
                .timestamp("ts")
                .returns("""
                        v\tts\ts\tbt\tst
                        1\t2024-01-01T00:00:00.000000Z\t\t0\t
                        2\t2024-01-01T01:00:00.000000Z\t\t0\t
                        3\t2024-01-01T02:00:00.000000Z\t\t0\t
                        4\t2024-01-01T03:00:00.000000Z\t\t0\t
                        5\t2024-01-01T04:00:00.000000Z\t\t0\t
                        6\t2024-01-01T05:00:00.000000Z\tx\t6\t\u00e9\ud834\udd1e6
                        7\t2024-01-01T06:00:00.000000Z\t\t-7\t
                        8\t2024-01-01T07:00:00.000000Z\ta\t0\t
                        """);
        assertQueriesMatchTwin(table, context, CONVERT_FAILURE_QUERIES);
        assertQueriesMatchTwin(table, context, new String[]{"SELECT * FROM {t}", "SELECT v, ts, s FROM {t} WHERE s IS NULL", "SELECT v, ts, s FROM {t} WHERE s = 'a'"});
    }

    private void assertConvertToNativeFailureKeepsColumnTops(boolean isLegacy, boolean isAsync) throws Exception {
        // Three failures of CONVERT TO NATIVE in a row on one table, each through the writer pool or the
        // command queue of a writer the test holds: the _txn commit after the _cv commit, the index
        // rebuild of s, and the open of the last column file. Each one leaves the committed state of
        // before the CONVERT, in the files and in the writer, which then commits a row of another
        // partition. A last CONVERT succeeds.
        setInPlaceParquetO3Properties();
        final String table = isLegacy ? "legacy_absent_merged" : "pt";
        final ConvertToNativeFailingFacade ff = new ConvertToNativeFailingFacade(table);
        assertMemoryLeak(ff, () -> {
            final String[] columns;
            final long[] tops;
            if (isLegacy) {
                loadStaleColumnTopFixture(table);
                createLegacyAbsentMergedTwin();
                columns = new String[]{"s", "d"};
                tops = new long[]{-1, -1};
            } else {
                createConvertFailureTable(table, "BYPASS WAL", true);
                createConvertFailureTable("tw", "BYPASS WAL", false);
                columns = CONVERT_FAILURE_COLUMNS;
                tops = new long[]{0, 0, 0};
            }
            for (String t : new String[]{table, "tw"}) {
                execute(CONVERT_FAILURE_PARTITIONS_INSERT.replace("{t}", t));
            }
            final long columnVersion = readColumnVersionFileVersion(table);
            final String[] querySnapshot = snapshotQueries(table, CONVERT_FAILURE_QUERIES);
            final String convertSql = "ALTER TABLE " + table + " CONVERT PARTITION TO NATIVE LIST '2024-01-01'";
            final String[] failures = {"_txn commit", "index rebuild", "column file open"};
            final String[] failingFileNames = {null, "s.k", isLegacy ? "d.d" : "st.d"};
            final String[] errors = {"No space left", "could not open read-write", "could not open"};
            if (isAsync) {
                try (TableWriter writer = getWriter(table)) {
                    final int partitionColumnIndex = writer.getMetadata().getColumnIndex("s");
                    for (int i = 0; i < failures.length; i++) {
                        final String context = "async writer, failed " + failures[i];
                        ff.arm(i == 0, failingFileNames[i]);
                        try (SqlCompiler compiler = engine.getSqlCompiler()) {
                            final CompiledQuery cq = compiler.compile(convertSql, sqlExecutionContext);
                            try (OperationFuture fut = cq.execute(eventSubSequence)) {
                                writer.tick();
                                fut.await();
                            }
                            Assert.fail(context + ": the CONVERT must fail");
                        } catch (SqlException | CairoException e) {
                            TestUtils.assertContains(context, e.getMessage(), errors[i]);
                        }
                        Assert.assertTrue(context, ff.hasFired());
                        Assert.assertEquals(context, PartitionFormat.PARQUET, writer.getPartitionFormat(0));
                        Assert.assertEquals(context, tops[0], writer.columnVersionReader().getColumnTop(writer.getPartitionTimestamp(0), partitionColumnIndex));
                        assertConvertFailureKeptState(table, context, columnVersion, columns, tops, querySnapshot);
                        // the writer commits a row of partition 2024-01-02 on the committed state of before the CONVERT
                        final TableWriter.Row row = writer.newRow(1_704_153_600_000_000L + 5 * Micros.HOUR_MICROS + i * Micros.MINUTE_MICROS);
                        row.putInt(0, 500 + i);
                        row.append();
                        writer.commit();
                        execute("INSERT INTO tw (v, ts) VALUES (" + (500 + i) + ", '2024-01-02T05:0" + i + ":00.000000Z')");
                        assertConvertFailureKeptState(table, context + ", after commit", columnVersion, columns, tops, querySnapshot);
                    }
                    try (SqlCompiler compiler = engine.getSqlCompiler()) {
                        final CompiledQuery cq = compiler.compile(convertSql, sqlExecutionContext);
                        try (OperationFuture fut = cq.execute(eventSubSequence)) {
                            writer.tick();
                            fut.await();
                        }
                    }
                }
            } else {
                for (int i = 0; i < failures.length; i++) {
                    final String context = "pooled writer, failed " + failures[i];
                    ff.arm(i == 0, failingFileNames[i]);
                    try {
                        execute(convertSql);
                        Assert.fail(context + ": the CONVERT must fail");
                    } catch (CairoException e) {
                        TestUtils.assertContains(context, e.getFlyweightMessage(), errors[i]);
                    }
                    Assert.assertTrue(context, ff.hasFired());
                    assertConvertFailureKeptState(table, context, columnVersion, columns, tops, querySnapshot);
                    for (String t : new String[]{table, "tw"}) {
                        execute("INSERT INTO " + t + " (v, ts) VALUES (" + (500 + i) + ", '2024-01-02T05:0" + i + ":00.000000Z')");
                    }
                    assertConvertFailureKeptState(table, context + ", after commit", columnVersion, columns, tops, querySnapshot);
                }
                if (!isLegacy) {
                    // the parquet -> native pre-pass of ALTER COLUMN TYPE fails the same way
                    ff.arm(false, "st.d");
                    try {
                        execute("ALTER TABLE pt ALTER COLUMN v TYPE SYMBOL");
                        Assert.fail("ALTER COLUMN TYPE must fail");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not open");
                    }
                    Assert.assertTrue(ff.hasFired());
                    engine.releaseAllWriters();
                    assertConvertFailureKeptState(table, "failed pre-pass", columnVersion, columns, tops, querySnapshot);
                }
                execute(convertSql);
            }
            if (isLegacy) {
                assertLegacyAbsentMergedNative(table, "converted");
            } else {
                assertConvertFailureTableConverted(table, "converted");
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            if (isLegacy) {
                assertLegacyAbsentMergedNative(table, "converted, reopen");
            } else {
                assertConvertFailureTableConverted(table, "converted, reopen");
            }
        });
    }

    private void assertLegacyAbsentMergedNative(String table, String context) throws Exception {
        Assert.assertEquals(context, PartitionFormat.NATIVE, getPartitionFormat(table));
        Assert.assertEquals(context, 0, getColumnTop(table, 0, "s"));
        Assert.assertEquals(context, 0, getColumnTop(table, 0, "d"));
        assertQuery("SELECT v, ts, s, d FROM " + table + " WHERE s = 'x'")
                .noLeakCheck()
                .timestamp("ts")
                .returns("""
                        v\tts\ts\td
                        101\t2024-01-01T04:30:00.000000Z\tx\t1.5
                        """);
        assertQuery("SELECT v, ts, s FROM " + table + " WHERE s IS NULL AND ts IN '2024-01-01'")
                .noLeakCheck()
                .timestamp("ts")
                .returns(COLUMN_TOP_SHAPE_P1_NULL_ROWS);
        assertQuery("SELECT v, ts, s FROM " + table + " WHERE s = 'x'")
                .noLeakCheck()
                .assertsPlanContaining("Index forward scan on: s");
        assertQueriesMatchTwin(table, context, LEGACY_ABSENT_NATIVE_QUERIES);
    }

    private void assertLegacyLiveTopNative(String table, String context) throws Exception {
        Assert.assertEquals(context, PartitionFormat.NATIVE, getPartitionFormat(table));
        for (String column : new String[]{"s", "d", "vc", "k"}) {
            Assert.assertEquals(context + ", column " + column, 0, getColumnTop(table, 0, column));
        }
        assertQuery("SELECT v, ts, s, d, vc FROM " + table + " WHERE s = 'y'")
                .noLeakCheck()
                .timestamp("ts")
                .returns("""
                        v\tts\ts\td\tvc
                        9\t2024-01-01T08:00:00.000000Z\ty\t2.5\ty9
                        """);
        assertQuery("SELECT v, ts, s FROM " + table + " WHERE s IS NULL AND ts IN '2024-01-01'")
                .noLeakCheck()
                .timestamp("ts")
                .returns(COLUMN_TOP_SHAPE_P1_NULL_ROWS);
        assertQueriesMatchTwin(table, context, LEGACY_LIVE_TOP_QUERIES);
    }

    private static void assertLegacyQueriesMatchTwin(String table, String context) throws Exception {
        assertQueriesMatchTwin(table, context, LEGACY_ABSENT_QUERIES);
    }

    private static String columnTopShapeInsert(String table, String... rows) {
        return "INSERT INTO " + table + " (v, ts, s, d, i, l, vc, st, ch, bo, bt, sh, u, ip, l256, g, d18, d38, arr, sy, tn) VALUES " + String.join(", ", rows);
    }

    /**
     * A row with a value derived from {@code v} in every column added with s, or NULL in all of them
     * when {@code s} is null.
     */
    private static String columnTopShapeRow(int v, String timestamp, String s) {
        if (s == null) {
            return "(" + v + ", '" + timestamp + "'" + ", NULL".repeat(COLUMN_TOP_SHAPE_COLUMN_NAMES.length) + ")";
        }
        return "(" + v + ", '" + timestamp + "', '" + s + "', " + v + ".5, " + v + ", " + v + "000000000, 'vc" + v + "', 'st" + v + "', 'q', true, "
                + (v % 100) + "::BYTE, " + v + "::SHORT, '00000000-0000-0000-0000-" + String.format("%012d", v) + "', '10.0.0." + (v % 250) + "', '0x"
                + Integer.toHexString(v) + "'::LONG256, #9v1s, '" + v + ".25'::DECIMAL(18, 2), '" + v + ".1234'::DECIMAL(38, 4), ARRAY[" + v + ".0, 1.5], 'k"
                + (v % 3) + "', '2024-01-01T00:00:00." + String.format("%09d", v) + "Z'::TIMESTAMP_NS)";
    }

    /**
     * Partition 2024-01-01 gets {@code p1Rows} and 2024-01-02 two rows. s, with the index
     * {@code indexSuffix}, and the other columns of {@link #COLUMN_TOP_SHAPE_COLUMNS} are added at the
     * point of the {@code shape}.
     */
    private static void createColumnTopShapeTable(
            String table,
            ColumnTopShape shape,
            String indexSuffix,
            String walMode,
            String p1Rows,
            boolean isParquet
    ) throws Exception {
        execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
        if (shape == ColumnTopShape.ZERO) {
            addColumnTopShapeColumns(table, indexSuffix);
        }
        execute("INSERT INTO " + table + " (v, ts) VALUES " + p1Rows);
        if (shape == ColumnTopShape.AT_ROW_COUNT) {
            addColumnTopShapeColumns(table, indexSuffix);
        }
        execute("INSERT INTO " + table + " (v, ts) VALUES (99, '2024-01-02T00:00:00.000000Z')");
        if (shape == ColumnTopShape.ABSENT) {
            addColumnTopShapeColumns(table, indexSuffix);
        }
        execute(columnTopShapeInsert(table, columnTopShapeRow(100, "2024-01-02T01:00:00.000000Z", "a")));
        drainWalQueue();
        if (isParquet) {
            execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
        }
    }

    private static void addColumnTopShapeColumns(String table, String indexSuffix) throws Exception {
        // one column per statement: a WAL table takes one structure change per ALTER
        for (String column : COLUMN_TOP_SHAPE_COLUMNS) {
            execute("ALTER TABLE " + table + " ADD COLUMN " + column);
        }
        execute("ALTER TABLE " + table + " ALTER COLUMN s ADD INDEX " + indexSuffix);
    }

    /**
     * Table {@code table} of the failure tests: s SYMBOL INDEX, bt BYTE and st STRING added after 5 of
     * the 8 rows of partition 2024-01-01, with a row group size of 4, and one row on 2024-01-02.
     */
    private static void createConvertFailureTable(String table, String walMode, boolean isParquet) throws Exception {
        execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
        execute("""
                INSERT INTO {t} (v, ts) VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z'),
                (3, '2024-01-01T02:00:00.000000Z'), (4, '2024-01-01T03:00:00.000000Z'), (5, '2024-01-01T04:00:00.000000Z')
                """.replace("{t}", table));
        execute("ALTER TABLE " + table + " ADD COLUMN s SYMBOL INDEX");
        execute("ALTER TABLE " + table + " ADD COLUMN bt BYTE");
        execute("ALTER TABLE " + table + " ADD COLUMN st STRING");
        execute("""
                INSERT INTO {t} VALUES (6, '2024-01-01T05:00:00.000000Z', 'x', 6, '\u00e9\ud834\udd1e6'),
                (7, '2024-01-01T06:00:00.000000Z', NULL, -7, ''), (8, '2024-01-01T07:00:00.000000Z', 'a', 0, NULL),
                (99, '2024-01-02T00:00:00.000000Z', 'a', 9, 'p2')
                """.replace("{t}", table));
        drainWalQueue();
        if (isParquet) {
            execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
        }
    }

    /**
     * The native twin of the legacy_absent_merged fixture: the statements that wrote it, without the
     * CONVERT.
     */
    private static void createLegacyAbsentMergedTwin() throws Exception {
        execute("CREATE TABLE tw (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO tw VALUES " + COLUMN_TOP_SHAPE_P1_ROWS + ", (99, '2024-01-02T00:00:00.000000Z')");
        execute("ALTER TABLE tw ADD COLUMN s SYMBOL INDEX");
        execute("ALTER TABLE tw ADD COLUMN d DOUBLE");
        execute("INSERT INTO tw VALUES (100, '2024-01-02T01:00:00.000000Z', 'a', 0.5)");
        execute("INSERT INTO tw VALUES (101, '2024-01-01T04:30:00.000000Z', 'x', 1.5)");
    }

    /**
     * The native twin of the legacy_live_top fixtures: the statements that wrote them, without the
     * CONVERT.
     */
    private static void createLegacyLiveTopTwin(boolean isPosting) throws Exception {
        execute("CREATE TABLE tw (k SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO tw VALUES " + LEGACY_LIVE_TOP_P1_ROWS);
        execute("ALTER TABLE tw ADD COLUMN d DOUBLE");
        execute("ALTER TABLE tw ADD COLUMN vc VARCHAR");
        execute("ALTER TABLE tw ADD COLUMN s SYMBOL INDEX" + (isPosting ? " TYPE POSTING" : ""));
        execute("INSERT INTO tw (k, v, ts, d, vc, s) VALUES ('a', 99, '2024-01-02T00:00:00.000000Z', 0.5, 'p2', 'a')");
        execute("INSERT INTO tw (k, v, ts, d, vc, s) VALUES ('b', 9, '2024-01-01T08:00:00.000000Z', 2.5, 'y9', 'y')");
    }

    /**
     * Unpacks the table {@code tableName} from resources/parquet-stale-column-top into the db root.
     * The fixtures hold column tops that only versions before the in-place parquet update kept _cv in
     * step with the file could write. A throwaway AbstractCairoTest wrote them at commit 0ea9e95767,
     * with the settings of {@link #setInPlaceParquetO3Properties()}, as BYPASS WAL tables:
     * <ul>
     *     <li>legacy_live_top: CREATE TABLE (k SYMBOL INDEX, v INT, ts TIMESTAMP), INSERT
     *     {@link #LEGACY_LIVE_TOP_P1_ROWS}, ADD COLUMN d DOUBLE, ADD COLUMN vc VARCHAR, ADD COLUMN s
     *     SYMBOL INDEX, INSERT ('a', 99, '2024-01-02', 0.5, 'p2', 'a'), CONVERT '2024-01-01' TO PARQUET,
     *     INSERT ('b', 9, '2024-01-01T08:00', 2.5, 'y9', 'y'): s, d and vc have top 8 of 9 rows;</li>
     *     <li>legacy_live_top_posting: the same with s SYMBOL INDEX TYPE POSTING;</li>
     *     <li>legacy_absent_merged: CREATE TABLE (v INT, ts TIMESTAMP), INSERT
     *     {@link #COLUMN_TOP_SHAPE_P1_ROWS} and (99, '2024-01-02'), ADD COLUMN s SYMBOL INDEX, ADD
     *     COLUMN d DOUBLE, INSERT (100, '2024-01-02T01:00', 'a', 0.5), CONVERT '2024-01-01' TO PARQUET,
     *     INSERT (101, '2024-01-01T04:30', 'x', 1.5): s and d keep top -1.</li>
     * </ul>
     * A covering POSTING index cannot have the first shape: that code fails its covering reseal.
     */
    private static void loadStaleColumnTopFixture(String tableName) throws IOException {
        final byte[] buffer = new byte[64 * 1024];
        final File root = new File(configuration.getDbRoot().toString());
        try (InputStream is = O3ParquetStaleReaderTest.class.getResourceAsStream("/parquet-stale-column-top/" + tableName + ".zip")) {
            Assert.assertNotNull(tableName, is);
            try (ZipInputStream zip = new ZipInputStream(is)) {
                ZipEntry entry;
                while ((entry = zip.getNextEntry()) != null) {
                    final File dest = new File(root, entry.getName());
                    if (!entry.isDirectory()) {
                        final File dir = dest.getParentFile();
                        if (!dir.exists()) {
                            Assert.assertTrue(dir.mkdirs());
                        }
                        try (FileOutputStream out = new FileOutputStream(dest)) {
                            int n;
                            while ((n = zip.read(buffer, 0, buffer.length)) > 0) {
                                out.write(buffer, 0, n);
                            }
                        }
                    }
                    zip.closeEntry();
                }
            }
        }
        engine.reloadTableNames();
        engine.reconcileTableNameRegistryState();
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
            case 4 ->
                    symRowsText(isNanos, row -> ("a".equals(row.s) || "b".equals(row.s)) && isFirstDay.test(row), SYM_ROW_BY_TS, rowSets);
            case 5 ->
                    symRowsText(isNanos, row -> "a".equals(row.s) && isFirstDay.test(row), SYM_ROW_BY_TS.reversed(), rowSets);
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
     * directory, s.k and s.v unless {@code track()} names them, and the truncations of the
     * descriptors those opens return.
     */
    private static class LiveIndexTruncateCounter extends TestFilesFacadeImpl {
        private final ConcurrentHashMap<Long, String> fileNamesByFd = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, AtomicInteger> openCounts = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, AtomicInteger> truncateCounts = new ConcurrentHashMap<>();
        private volatile String keyFileName = "s.k";
        private volatile String partitionDir;
        private volatile String valueFileName = "s.v";

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
                for (String fileName : new String[]{keyFileName, valueFileName}) {
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

        void track(String partitionDir, String keyFileName, String valueFileName) {
            this.keyFileName = keyFileName;
            this.valueFileName = valueFileName;
            this.partitionDir = partitionDir;
        }
    }

    /**
     * Fails, once when armed, the extension of the _txn file of one table by its writer, or the open of a
     * file for append or read-write by its name, with or without the column name txn suffix.
     */
    private static class ConvertToNativeFailingFacade extends TxnExtendFailingFacade {
        private final AtomicInteger fileFailures = new AtomicInteger();
        private final AtomicReference<String> failingFileNameRef = new AtomicReference<>();
        private volatile boolean isTxnExtendFailing;

        ConvertToNativeFailingFacade(String tableName) {
            super(tableName);
        }

        @Override
        public long openAppend(LPSZ name) {
            return isFailingFile(name) ? -1 : super.openAppend(name);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            return isFailingFile(name) ? -1 : super.openRW(name, opts);
        }

        void arm(boolean isTxnExtendFailing, String failingFileName) {
            this.isTxnExtendFailing = isTxnExtendFailing;
            isArmed.set(isTxnExtendFailing);
            failingFileNameRef.set(failingFileName);
            fileFailures.set(0);
        }

        boolean hasFired() {
            return isTxnExtendFailing ? !isArmed.get() : fileFailures.get() == 1;
        }

        private boolean isFailingFile(LPSZ name) {
            final String fileName = failingFileNameRef.get();
            if (fileName == null) {
                return false;
            }
            final String path = Utf8s.stringFromUtf8Bytes(name);
            final String lastSegment = path.substring(path.lastIndexOf(Files.SEPARATOR) + 1);
            if ((lastSegment.equals(fileName) || lastSegment.startsWith(fileName + '.')) && failingFileNameRef.compareAndSet(fileName, null)) {
                fileFailures.incrementAndGet();
                return true;
            }
            return false;
        }
    }

    /**
     * Records the names of the files the writer links.
     */
    private static class HardLinkRecordingFacade extends TestFilesFacadeImpl {
        private final ConcurrentLinkedQueue<String> linkedFileNames = new ConcurrentLinkedQueue<>();

        @Override
        public int hardLink(LPSZ src, LPSZ hardLink) {
            final String path = Utf8s.stringFromUtf8Bytes(src);
            linkedFileNames.add(path.substring(path.lastIndexOf(Files.SEPARATOR) + 1));
            return super.hardLink(src, hardLink);
        }

        boolean hasLinked(String fileNamePrefix) {
            for (String fileName : linkedFileNames) {
                if (fileName.startsWith(fileNamePrefix)) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Fails, once when armed, the extension of the _txn file of one table by its writer.
     * <p>
     * The tests size their commits for Linux x86-64. Its 4 KB page holds the _txn file of a new table, and a
     * commit that adds 300 partitions writes a _txn record of about 10 KB: the writer extends its mapping of the
     * file, finds the file shorter than the new mapping and allocates disk space, which fails. The page size is
     * 16 KB on macOS arm64 and the allocation granularity 64 KB on Windows, so there the record fits in the
     * first page of the mapping, and the file of a new table is a page long. This facade makes every platform
     * behave as Linux: the _txn writers map their files in steps of 4 KB, and while armed the _txn file of the
     * table reads as empty, so that the next extension of the mapping allocates disk space.
     */
    private static class TxnExtendFailingFacade extends TestFilesFacadeImpl {
        private static final long TXN_WRITER_PAGE_SIZE = 4 * 1024;
        final AtomicBoolean isArmed = new AtomicBoolean();
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
        public long getPageSize() {
            final long pageSize = super.getPageSize();
            return pageSize > TXN_WRITER_PAGE_SIZE && isTxnWriterCalling() ? TXN_WRITER_PAGE_SIZE : pageSize;
        }

        @Override
        public long length(long fd) {
            return fd == txnFd && isArmed.get() ? 0 : super.length(fd);
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

        // Whether a _txn writer asks for the page size, which it does only to open its file. The other callers keep
        // the page size of the platform: the configuration hands it to memory that maps pages at offsets of it
        private static boolean isTxnWriterCalling() {
            return StackWalker.getInstance().walk(
                    frames -> frames.anyMatch(frame -> TxWriter.class.getName().equals(frame.getClassName()))
            );
        }
    }

    /**
     * Fails, once when armed, the msync of the _txn file of one table, and counts the read-write opens of
     * that file. TxWriter.finishABHeader() switches the record before it syncs it, so the commit that
     * sees the failure has landed. Only commit mode sync reaches the fault: MemoryCMARWImpl.sync() skips
     * the msync in async mode.
     */
    private static class TxnMsyncFailingFacade extends TestFilesFacadeImpl {
        final AtomicBoolean isArmed = new AtomicBoolean();
        private final String tableDirPrefix;
        private final Set<Long> txnAddresses = ConcurrentHashMap.newKeySet();
        private final AtomicInteger txnOpenCount = new AtomicInteger();
        private volatile long txnFd = -1;

        TxnMsyncFailingFacade(String tableName) {
            this.tableDirPrefix = Files.SEPARATOR + tableName;
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            final long address = super.mmap(fd, len, offset, flags, memoryTag);
            if (fd == txnFd && address != FilesFacade.MAP_FAILED) {
                txnAddresses.add(address);
            }
            return address;
        }

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            final long address = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
            if (fd == txnFd && address != FilesFacade.MAP_FAILED) {
                txnAddresses.remove(addr);
                txnAddresses.add(address);
            }
            return address;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            if (txnAddresses.contains(addr) && isArmed.compareAndSet(true, false)) {
                throw CairoException.critical(5).put("could not msync [test fault]");
            }
            super.msync(addr, len, async);
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            txnAddresses.remove(address);
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (
                    Utf8s.endsWithAscii(name, Files.SEPARATOR + TableUtils.TXN_FILE_NAME)
                            && (Utf8s.containsAscii(name, tableDirPrefix + '~') || Utf8s.containsAscii(name, tableDirPrefix + Files.SEPARATOR))
            ) {
                txnFd = fd;
                txnOpenCount.incrementAndGet();
            }
            return fd;
        }

        int getTxnOpenCount() {
            return txnOpenCount.get();
        }
    }

    private enum ColumnTopShape {
        // s and the columns added with it exist before partition 2024-01-01 has rows: top 0
        ZERO,
        // added after partition 2024-01-02 exists: absent from partition 2024-01-01, top -1
        ABSENT,
        // added while partition 2024-01-01 is the last one: top at its row count
        AT_ROW_COUNT
    }

    private record SymRow(String s, int v, String day, String time) {
        String timestampText(boolean isNanos) {
            return day + 'T' + time + (isNanos ? ":00.000000000Z" : ":00.000000Z");
        }
    }
}
