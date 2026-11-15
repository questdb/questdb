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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.O3ParquetCounters;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;
import static io.questdb.test.cairo.parquet.ParquetCommitTestUtils.parquetFileSize;
import static io.questdb.test.cairo.parquet.ParquetCommitTestUtils.partitionNameTxn;

/**
 * An in-place parquet O3 update rebuilds the partition's symbol indexes (bitmap, posting,
 * covering) under a new column name txn, published through _cv with _txn, and never
 * touches the committed files. These tests pin that:
 * <ul>
 * <li>a pinned reader keeps its snapshot through the index; a fresh one sees new rows;</li>
 * <li>a failure between the rebuild and the _txn commit leaves the committed index
 * answering until the retry converges;</li>
 * <li>superseded files stay while a reader is pinned and go once it closes;</li>
 * <li>FORCE REINDEX PARQUET and DETACH leave a superseded version waiting for its purge
 * out of the directory they copy the partition into;</li>
 * <li>a two-partition commit failing in the second leaves both as committed;</li>
 * <li>files of a failed rebuild that RESUME WAL skips stay until VACUUM TABLE.</li>
 * </ul>
 */
public class ParquetInPlaceIndexPinnedReaderTest extends AbstractCairoTest {
    // The day the in-place commits of the FORCE REINDEX and DETACH tests go to
    private static final String DAY = "2024-01-01";
    // The files of a DETACH copy of a parquet partition besides its index files: DETACH adds the table's _cv, _meta
    // and _txn
    private static final String[] DETACHED_PARQUET_PARTITION_FILES = {
            TableUtils.COLUMN_VERSION_FILE_NAME,
            TableUtils.META_FILE_NAME,
            TableUtils.PARQUET_METADATA_FILE_NAME,
            TableUtils.TXN_FILE_NAME,
            TableUtils.PARQUET_PARTITION_NAME
    };
    private static final String IDX_BITMAP = "INDEX";
    private static final String IDX_COVERING = "INDEX TYPE POSTING INCLUDE (v)";
    private static final String IDX_POSTING = "INDEX TYPE POSTING";
    private static final String[] KEYS = {"s0", "s1", "s2"};
    // The files of a parquet partition directory besides its index files
    private static final String[] PARQUET_PARTITION_FILES = {TableUtils.PARQUET_METADATA_FILE_NAME, TableUtils.PARQUET_PARTITION_NAME};
    // O3 rows into row groups 0 and 1 of both parquet days: the 2024-01-01 task updates its
    // partition in place before the 2024-01-02 task runs.
    private static final String CROSS_O3_INSERT = """
            INSERT INTO %s(id, ts, sym, sym2, v) VALUES
            (1100, '2024-01-01T01:00:00.000000Z', 's1', 't1', 1100),
            (1101, '2024-01-01T09:00:00.000000Z', 's2', 't0', 1101),
            (1102, '2024-01-02T01:00:00.000000Z', 's1', 't1', 1102),
            (1103, '2024-01-02T09:00:00.000000Z', 's0', 't0', 1103)
            """;
    // Every O3 row lands in 2024-01-01 row groups 0 and 1, so every later row
    // group of the partition shifts: its row ids and ordinals change.
    private static final String O3_INSERT = """
            INSERT INTO %s(id, ts, sym, sym2, v) VALUES
            (1000, '2024-01-01T00:30:00.000000Z', 's1', 't1', 1000),
            (1001, '2024-01-01T01:00:00.000000Z', 's2', 't0', 1001),
            (1002, '2024-01-01T01:30:00.000000Z', 's1', 't1', 1002),
            (1003, '2024-01-01T03:00:00.000000Z', 's0', 't0', 1003),
            (1004, '2024-01-01T09:00:00.000000Z', 's1', 't1', 1004)
            """;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // 12 rows per day -> 3 row groups of 4; never rewrite on dead bytes, so
        // the O3 below updates the parquet partition in place.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
    }

    @Test
    public void testCrashWindowBitmap() throws Exception {
        assertCrashWindow(IDX_BITMAP, "sym2.k", ParquetInPlaceIndexPinnedReaderTest::applyO3Insert);
    }

    @Test
    public void testCrashWindowCovering() throws Exception {
        assertCrashWindow(IDX_COVERING, "sym2.pk", ParquetInPlaceIndexPinnedReaderTest::applyO3Insert);
    }

    @Test
    public void testCrashWindowCoveringSidecarData() throws Exception {
        // The covering value sidecar of the first column: the failure lands in
        // finishO3Commit's covering reseal, after both columns' .pk/.pv were rebuilt.
        assertCrashWindow(IDX_COVERING, "sym.pc0", ParquetInPlaceIndexPinnedReaderTest::applyO3Insert);
    }

    @Test
    public void testCrashWindowCoveringSidecarInfo() throws Exception {
        assertCrashWindow(IDX_COVERING, "sym.pci", ParquetInPlaceIndexPinnedReaderTest::applyO3Insert);
    }

    @Test
    public void testCrashWindowDropBitmap() throws Exception {
        assertCrashWindow(IDX_BITMAP, "sym2.k", ParquetInPlaceIndexPinnedReaderTest::applyDrop);
    }

    @Test
    public void testCrashWindowDropCoveringSidecarData() throws Exception {
        assertCrashWindow(IDX_COVERING, "sym.pc0", ParquetInPlaceIndexPinnedReaderTest::applyDrop);
    }

    @Test
    public void testCrashWindowDropPosting() throws Exception {
        assertCrashWindow(IDX_POSTING, "sym2.pk", ParquetInPlaceIndexPinnedReaderTest::applyDrop);
    }

    @Test
    public void testCrashWindowFilterMergeBitmap() throws Exception {
        assertCrashWindow(IDX_BITMAP, "sym2.k", ParquetInPlaceIndexPinnedReaderTest::applyFilterMerge);
    }

    @Test
    public void testCrashWindowFilterMergeCovering() throws Exception {
        assertCrashWindow(IDX_COVERING, "sym2.pk", ParquetInPlaceIndexPinnedReaderTest::applyFilterMerge);
    }

    @Test
    public void testCrashWindowFilterMergePosting() throws Exception {
        assertCrashWindow(IDX_POSTING, "sym2.pk", ParquetInPlaceIndexPinnedReaderTest::applyFilterMerge);
    }

    @Test
    public void testCrashWindowPosting() throws Exception {
        assertCrashWindow(IDX_POSTING, "sym2.pk", ParquetInPlaceIndexPinnedReaderTest::applyO3Insert);
    }

    @Test
    public void testCrossPartitionO3DataFileFaultAfterOtherInPlaceBitmap() throws Exception {
        assertCrossPartitionFault(IDX_BITMAP, ParquetInPlaceIndexPinnedReaderTest::applyCrossO3Insert, TableUtils.PARQUET_PARTITION_NAME, 1, 0);
    }

    @Test
    public void testCrossPartitionO3DataFileFaultAfterOtherInPlaceCovering() throws Exception {
        assertCrossPartitionFault(IDX_COVERING, ParquetInPlaceIndexPinnedReaderTest::applyCrossO3Insert, TableUtils.PARQUET_PARTITION_NAME, 1, 0);
    }

    @Test
    public void testCrossPartitionO3DataFileFaultAfterOtherInPlacePosting() throws Exception {
        assertCrossPartitionFault(IDX_POSTING, ParquetInPlaceIndexPinnedReaderTest::applyCrossO3Insert, TableUtils.PARQUET_PARTITION_NAME, 1, 0);
    }

    @Test
    public void testCrossPartitionReplaceIndexFaultAfterOtherInPlaceBitmap() throws Exception {
        // the failing partition's parquet update is done too; its index rebuild fails
        assertCrossPartitionFault(IDX_BITMAP, ParquetInPlaceIndexPinnedReaderTest::applyCrossReplace, "sym2.k", 2, 0);
    }

    @Test
    public void testCrossPartitionReplaceIndexFaultAfterOtherInPlaceCovering() throws Exception {
        assertCrossPartitionFault(IDX_COVERING, ParquetInPlaceIndexPinnedReaderTest::applyCrossReplace, "sym2.pk", 2, 0);
    }

    @Test
    public void testCrossPartitionReplaceIndexFaultAfterOtherInPlacePosting() throws Exception {
        assertCrossPartitionFault(IDX_POSTING, ParquetInPlaceIndexPinnedReaderTest::applyCrossReplace, "sym2.pk", 2, 0);
    }

    @Test
    public void testCrossPartitionReplaceFaultAfterOtherRemovedBitmap() throws Exception {
        assertCrossPartitionFault(IDX_BITMAP, ParquetInPlaceIndexPinnedReaderTest::applyReplaceRemovingFirstDay, TableUtils.PARQUET_PARTITION_NAME, 0, 1);
    }

    @Test
    public void testCrossPartitionReplaceFaultAfterOtherRemovedCovering() throws Exception {
        assertCrossPartitionFault(IDX_COVERING, ParquetInPlaceIndexPinnedReaderTest::applyReplaceRemovingFirstDay, "sym2.pk", 1, 1);
    }

    @Test
    public void testDetachCarriesOnlyTheLiveIndexVersionBitmap() throws Exception {
        assertDetachCarriesOnlyTheLiveIndexVersion(IDX_BITMAP, false, true, null);
    }

    @Test
    public void testDetachCarriesOnlyTheLiveIndexVersionCovering() throws Exception {
        assertDetachCarriesOnlyTheLiveIndexVersion(IDX_COVERING, false, true, null);
    }

    @Test
    public void testDetachCarriesOnlyTheLiveIndexVersionPosting() throws Exception {
        assertDetachCarriesOnlyTheLiveIndexVersion(IDX_POSTING, false, true, null);
    }

    @Test
    public void testDetachCarriesOnlyTheLiveIndexVersionWalBitmap() throws Exception {
        // ADAPTIVE defers every in-place purge, no reader needed; under NOSYNC the purge would run at once
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        assertDetachCarriesOnlyTheLiveIndexVersion(IDX_BITMAP, true, false, null);
    }

    @Test
    public void testDetachCarriesOnlyTheLiveIndexVersionWalCovering() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        assertDetachCarriesOnlyTheLiveIndexVersion(IDX_COVERING, true, false, null);
    }

    @Test
    public void testDetachCrossDeviceCopyCarriesOnlyTheLiveIndexVersion() throws Exception {
        // DETACH copies the directory instead of linking it, and ATTACH copies it back
        node1.setProperty(PropertyKey.CAIRO_ATTACH_PARTITION_COPY, true);
        final PartitionCopyFacade ff = new PartitionCopyFacade();
        ff.isCrossDevice = true;
        assertDetachCarriesOnlyTheLiveIndexVersion(IDX_BITMAP, false, true, ff);
    }

    @Test
    public void testDetachFailureAfterSupersededRemovalKeepsPartition() throws Exception {
        // The copy of _meta that follows the pass fails: DETACH removes its whole temporary copy and the partition
        // stays attached, its directory as it was. The retry builds a fresh copy.
        final PartitionCopyFacade ff = new PartitionCopyFacade();
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
        try {
            assertMemoryLeak(ff, () -> {
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    createSimpleTwins(IDX_BITMAP, false, "nat", "pq");
                    final String exp0 = printed(plainQuery("nat", "s0"));
                    final long nameTxnBefore = partitionNameTxn(engine, "pq", 0);
                    final File oldDir = partitionDir("pq", DAY, nameTxnBefore);
                    final File attachedDir;
                    final String[] attachedListing;
                    try (RecordCursorFactory factory = select(indexQuery("pq", "s0")); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        applyO3Wave("pq", 1000, "00");
                        final long live = symColumnNameTxn();
                        assertSupersededVersionWaits("after O3", oldDir, "sym", live);
                        final Map<String, byte[]> oldFiles = dirFiles(oldDir);

                        ff.failCopyTo = TableUtils.DETACHING_DIR_MARKER + io.questdb.std.Files.SEPARATOR + TableUtils.META_FILE_NAME;
                        final String detach = "ALTER TABLE pq DETACH PARTITION LIST '2024-01-01'";
                        try {
                            execute(detach);
                            Assert.fail("the copy of _meta must fail");
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "could not detach partition [table=pq, detachStatus=DETACH_ERR_COPY_META");
                            Assert.assertEquals(detach.indexOf("'2024-01-01'"), e.getPosition());
                        }
                        Assert.assertEquals(1, ff.faultCount.get());
                        Assert.assertArrayEquals("no DETACH copy may stay", new String[0], detachCopies("pq", DAY));
                        Assert.assertEquals(nameTxnBefore, partitionNameTxn(engine, "pq", 0));
                        assertFilesUnchanged("old directory after the failed DETACH", oldFiles, dirFiles(oldDir));
                        assertQuery("SELECT count() FROM pq WHERE ts IN '2024-01-01'")
                                .noLeakCheck()
                                .noRandomAccess()
                                .expectSize()
                                .returns("count\n14\n");

                        execute(detach);
                        assertLiveOnly("after the retried DETACH", detachedCopy("pq", DAY), DETACHED_PARQUET_PARTITION_FILES, "sym", live);
                        attach("pq", DAY);
                        attachedDir = partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0));
                        attachedListing = assertLiveOnly("after ATTACH", attachedDir, DETACHED_PARQUET_PARTITION_FILES, "sym", live);
                        TestUtils.assertEquals(exp0, drain(factory, cursor));
                    }
                    settle(purgeJob);
                    Assert.assertFalse("the detached directory must go: " + oldDir, oldDir.exists());
                    Assert.assertArrayEquals(attachedListing, listDir(attachedDir));
                    applyO3Wave("nat", 1000, "00");
                    assertTwins("after ATTACH");
                }
            });
        } finally {
            setCurrentMicros(-1);
        }
    }

    @Test
    public void testDetachKeepsIndexNamedByDefaultColumnVersion() throws Exception {
        // A parquet day with no _cv record of its own for an indexed column names the column's index files by the
        // column's default record: s2 was added before the day was written, and sym was renamed to sym9 with no purge
        // waiting. DETACH rebuilds nothing after the pass, so the pass must read the default record to keep them.
        assertMemoryLeak(() -> {
            for (String table : new String[]{"nat", "pq"}) {
                execute("CREATE TABLE " + table + " (id INT, ts TIMESTAMP, sym SYMBOL INDEX, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("ALTER TABLE " + table + " ADD COLUMN s2 SYMBOL INDEX");
                execute("INSERT INTO " + table + " SELECT x::INT, '2024-01-01'::TIMESTAMP + (x - 1) * 7_200_000_000L, 's' || (x % 3), x, 't' || (x % 2) FROM long_sequence(36)");
            }
            execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
            engine.releaseAllReaders();
            execute("ALTER TABLE nat RENAME COLUMN sym TO sym9");
            execute("ALTER TABLE pq RENAME COLUMN sym TO sym9");

            final String[] columns = {"s2", "sym9"};
            final long[] live = new long[columns.length];
            try (TableReader reader = engine.getReader("pq")) {
                final long partitionTimestamp = reader.getTxFile().getPartitionTimestampByIndex(1);
                final ColumnVersionReader columnVersionReader = reader.getColumnVersionReader();
                for (int i = 0; i < columns.length; i++) {
                    final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(columns[i]));
                    Assert.assertEquals("_cv record of " + columns[i] + " for 2024-01-02", -1, columnVersionReader.getRecordIndex(partitionTimestamp, writerIndex));
                    live[i] = columnVersionReader.getDefaultColumnNameTxn(writerIndex);
                    Assert.assertEquals(live[i], columnVersionReader.getColumnNameTxn(partitionTimestamp, writerIndex));
                }
            }
            Assert.assertTrue("s2 added at a txn: " + live[0], live[0] > -1);
            Assert.assertTrue("sym9 named after the RENAME: " + live[1], live[1] > live[0]);
            assertLiveOnly("before DETACH", partitionDir("pq", "2024-01-02", partitionNameTxn(engine, "pq", 1)), PARQUET_PARTITION_FILES, columns, live);

            execute("ALTER TABLE pq DETACH PARTITION LIST '2024-01-02'");
            assertLiveOnly("after DETACH", detachedCopy("pq", "2024-01-02"), DETACHED_PARQUET_PARTITION_FILES, columns, live);
            attach("pq", "2024-01-02");
            assertLiveOnly("after ATTACH", partitionDir("pq", "2024-01-02", partitionNameTxn(engine, "pq", 1)), DETACHED_PARQUET_PARTITION_FILES, columns, live);

            assertSqlCursors("SELECT * FROM nat", "SELECT * FROM pq");
            final String[][] keys = {{"t0", "t1"}, {"s0", "s1", "s2"}};
            for (int i = 0; i < columns.length; i++) {
                assertQuery("SELECT ts, " + columns[i] + " FROM pq WHERE " + columns[i] + " = '" + keys[i][0] + "'")
                        .noLeakCheck()
                        .assertsPlanContaining("Index forward scan on: " + columns[i]);
                for (String key : keys[i]) {
                    assertSqlCursors(
                            "SELECT ts, " + columns[i] + " FROM nat WHERE concat(" + columns[i] + ", '') = '" + key + "'",
                            "SELECT ts, " + columns[i] + " FROM pq WHERE " + columns[i] + " = '" + key + "'"
                    );
                }
            }
        });
    }

    @Test
    public void testDetachKeepsSupersededFileItCannotRemove() throws Exception {
        // A superseded index file the pass cannot remove stays in the DETACH copy, as before the pass existed, and the
        // error log names it; ATTACH carries it back, and VACUUM TABLE removes it.
        final PartitionCopyFacade ff = new PartitionCopyFacade();
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(ff, () -> {
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    createSimpleTwins(IDX_BITMAP, false, "nat", "pq");
                    final String exp0 = printed(plainQuery("nat", "s0"));
                    final File attachedDir;
                    final long live;
                    try (RecordCursorFactory factory = select(indexQuery("pq", "s0")); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        applyO3Wave("pq", 1000, "00");
                        live = symColumnNameTxn();
                        assertSupersededVersionWaits("after O3", partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0)), "sym", live);

                        final String keptFile = TableUtils.DETACHING_DIR_MARKER + io.questdb.std.Files.SEPARATOR + "sym.k";
                        ff.keptPathSuffix = keptFile;
                        execute("ALTER TABLE pq DETACH PARTITION LIST '2024-01-01'");
                        capture.drain();
                        capture.assertLoggedRE("could not remove superseded index file \\[path=[^\\]]*" + Pattern.quote(keptFile) + ", errno=");
                        ff.keptPathSuffix = null;
                        Assert.assertEquals(1, ff.faultCount.get());
                        assertLiveOnly("after DETACH", detachedCopy("pq", DAY), withFile(DETACHED_PARQUET_PARTITION_FILES, "sym.k"), "sym", live);
                        attach("pq", DAY);
                        attachedDir = partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0));
                        assertLiveOnly("after ATTACH", attachedDir, withFile(DETACHED_PARQUET_PARTITION_FILES, "sym.k"), "sym", live);
                        TestUtils.assertEquals(exp0, drain(factory, cursor));
                    }
                    settle(purgeJob);
                    assertLiveOnly("after the purges", attachedDir, withFile(DETACHED_PARQUET_PARTITION_FILES, "sym.k"), "sym", live);
                    applyO3Wave("nat", 1000, "00");
                    assertTwins("after ATTACH");

                    execute("VACUUM TABLE pq");
                    runPurgeJob(purgeJob);
                    assertLiveOnly("after VACUUM", attachedDir, DETACHED_PARQUET_PARTITION_FILES, "sym", live);
                    assertTwins("after VACUUM");
                }
            });
        } finally {
            capture.stop();
            setCurrentMicros(-1);
        }
    }

    @Test
    public void testDetachNativePartitionKeepsEveryFile() throws Exception {
        // DETACH copies a native partition whole: its superseded column versions drive the column version pin of
        // ATTACH, so the pass that leaves superseded index versions out of a parquet partition's copy must not touch
        // it. A stray index file under a column name txn _cv does not name stands for such a version.
        assertMemoryLeak(() -> {
            createSimpleTwins(IDX_BITMAP, false, "nat", "pq");
            // 2024-01-03 stays native and stops being the last partition
            final String insert = "INSERT INTO %s VALUES (2000, '2024-01-04T00:00:00.000000Z', 's1', 2000)";
            execute(String.format(insert, "nat"));
            execute(String.format(insert, "pq"));
            final String day = "2024-01-03";
            final File dayDir = partitionDir("pq", day, partitionNameTxn(engine, "pq", 2));
            Assert.assertTrue(new File(dayDir, "sym.d").exists());
            Assert.assertEquals(-1, columnNameTxn("sym", 2));
            Assert.assertTrue(new File(dayDir, "sym.k.999").createNewFile());
            final String[] dayListing = listDir(dayDir);

            execute("ALTER TABLE pq DETACH PARTITION LIST '" + day + "'");
            final String[] expected = withFile(withFile(withFile(dayListing, TableUtils.COLUMN_VERSION_FILE_NAME), TableUtils.META_FILE_NAME), TableUtils.TXN_FILE_NAME);
            Assert.assertArrayEquals(expected, listDir(detachedCopy("pq", day)));
            attach("pq", day);
            Assert.assertArrayEquals(expected, listDir(partitionDir("pq", day, partitionNameTxn(engine, "pq", 2))));
            assertTwins("after ATTACH");
        });
    }

    @Test
    public void testDetachRestartAttachCarriesOnlyTheLiveIndexVersion() throws Exception {
        // The DETACH copy outlives the writer and the readers: ATTACH after their restart takes back the live version
        // only, and the restarted purge job completes the superseded version's logged purge.
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
        try {
            assertMemoryLeak(() -> {
                createSimpleTwins(IDX_BITMAP, false, "nat", "pq");
                final String exp0 = printed(plainQuery("nat", "s0"));
                final File oldDir = partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0));
                final long live;
                try (RecordCursorFactory factory = select(indexQuery("pq", "s0")); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    applyO3Wave("pq", 1000, "00");
                    live = symColumnNameTxn();
                    assertSupersededVersionWaits("after O3", oldDir, "sym", live);
                    execute("ALTER TABLE pq DETACH PARTITION LIST '2024-01-01'");
                    TestUtils.assertEquals(exp0, drain(factory, cursor));
                }
                engine.releaseAllReaders();
                engine.releaseAllWriters();

                attach("pq", DAY);
                final File attachedDir = partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0));
                final String[] attachedListing = assertLiveOnly("after ATTACH", attachedDir, DETACHED_PARQUET_PARTITION_FILES, "sym", live);
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    settle(purgeJob);
                }
                Assert.assertFalse("the detached directory must go: " + oldDir, oldDir.exists());
                Assert.assertArrayEquals(attachedListing, listDir(attachedDir));
                applyO3Wave("nat", 1000, "00");
                assertTwins("after ATTACH");
            });
        } finally {
            setCurrentMicros(-1);
        }
    }

    @Test
    public void testDetachTwoPartitionsChecksEachPartitionColumnVersion() throws Exception {
        // Only 2024-01-01 took the in-place commit: its live sym version is the commit's, while 2024-01-02 has no _cv
        // record of sym and keeps the unsuffixed version CONVERT wrote. Each copy is checked against its own day.
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
        try {
            assertMemoryLeak(() -> {
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    createSimpleTwins(IDX_BITMAP, false, "nat", "pq");
                    final String exp0 = printed(plainQuery("nat", "s0"));
                    final long live1;
                    try (RecordCursorFactory factory = select(indexQuery("pq", "s0")); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        applyO3Wave("pq", 1000, "00");
                        live1 = symColumnNameTxn();
                        Assert.assertEquals(-1, columnNameTxn("sym", 1));
                        assertSupersededVersionWaits("after O3", partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0)), "sym", live1);

                        execute("ALTER TABLE pq DETACH PARTITION LIST '2024-01-01', '2024-01-02'");
                        Assert.assertArrayEquals(
                                new String[]{"_cv", "_meta", "_pm", "_txn", "data.parquet", "sym.k." + live1, "sym.v." + live1},
                                listDir(detachedCopy("pq", DAY))
                        );
                        Assert.assertArrayEquals(
                                new String[]{"_cv", "_meta", "_pm", "_txn", "data.parquet", "sym.k", "sym.v"},
                                listDir(detachedCopy("pq", "2024-01-02"))
                        );
                        TestUtils.assertEquals(exp0, drain(factory, cursor));
                    }
                    releaseSavePointHolds("pq");
                    final File tableDir = tableDir("pq");
                    for (String day : new String[]{DAY, "2024-01-02"}) {
                        Files.move(new File(tableDir, day + TableUtils.DETACHED_DIR_MARKER).toPath(), new File(tableDir, day + configuration.getAttachPartitionSuffix()).toPath());
                    }
                    execute("ALTER TABLE pq ATTACH PARTITION LIST '2024-01-01', '2024-01-02'");
                    Assert.assertEquals(live1, columnNameTxn("sym", 0));
                    Assert.assertEquals(-1, columnNameTxn("sym", 1));
                    settle(purgeJob);
                    Assert.assertArrayEquals(
                            new String[]{"_cv", "_meta", "_pm", "_txn", "data.parquet", "sym.k." + live1, "sym.v." + live1},
                            listDir(partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0)))
                    );
                    Assert.assertArrayEquals(
                            new String[]{"_cv", "_meta", "_pm", "_txn", "data.parquet", "sym.k", "sym.v"},
                            listDir(partitionDir("pq", "2024-01-02", partitionNameTxn(engine, "pq", 1)))
                    );
                    applyO3Wave("nat", 1000, "00");
                    assertTwins("after ATTACH");
                }
            });
        } finally {
            setCurrentMicros(-1);
        }
    }

    @Test
    public void testDropAddIndexThenDetachCarriesOnlyTheLiveIndexVersion() throws Exception {
        assertDropAddIndexCarriesOnlyTheLiveIndexVersion(true);
    }

    @Test
    public void testDropAddIndexThenForceReindexCarriesOnlyTheLiveIndexVersion() throws Exception {
        assertDropAddIndexCarriesOnlyTheLiveIndexVersion(false);
    }

    @Test
    public void testForceReindexAfterIndexAddedCarriesOnlyTheLiveIndexVersion() throws Exception {
        // The index added after CONVERT is named without a txn; the in-place commit supersedes it
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
        try {
            assertMemoryLeak(() -> {
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    createSimpleTwins("", false, "nat", "pq");
                    execute("ALTER TABLE nat ALTER COLUMN sym ADD INDEX");
                    execute("ALTER TABLE pq ALTER COLUMN sym ADD INDEX");
                    final File addedDir = partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0));
                    Assert.assertArrayEquals(new String[]{"_pm", "data.parquet", "sym.k", "sym.v"}, listDir(addedDir));
                    final File newDir;
                    final long live;
                    try (RecordCursorFactory factory = select(indexQuery("pq", "s0")); RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                        applyO3Wave("pq", 1000, "00");
                        live = symColumnNameTxn();
                        assertSupersededVersionWaits("after O3", addedDir, "sym", live);
                        execute("ALTER TABLE pq FORCE REINDEX PARQUET PARTITION LIST '2024-01-01'");
                        newDir = partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0));
                        Assert.assertArrayEquals(new String[]{"_pm", "data.parquet", "sym.k." + live, "sym.v." + live}, listDir(newDir));
                    }
                    settle(purgeJob);
                    Assert.assertFalse(addedDir.exists());
                    Assert.assertArrayEquals(new String[]{"_pm", "data.parquet", "sym.k." + live, "sym.v." + live}, listDir(newDir));
                    applyO3Wave("nat", 1000, "00");
                    assertTwins("after FORCE REINDEX");
                }
            });
        } finally {
            setCurrentMicros(-1);
        }
    }

    @Test
    public void testForceReindexAfterRestartCarriesOnlyTheLiveIndexVersion() throws Exception {
        // The superseded version's purge is still to run when the writer, the readers and the purge job restart: the
        // reindexed directory must not carry it, and the restarted purge job completes the purge.
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
        try {
            assertMemoryLeak(() -> {
                createSimpleTwins(IDX_BITMAP, false, "nat", "pq");
                final File oldDir = partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0));
                try (RecordCursorFactory factory = select(indexQuery("pq", "s0")); RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                    applyO3Wave("pq", 1000, "00");
                }
                final long live = symColumnNameTxn();
                assertSupersededVersionWaits("after the reader closed", oldDir, "sym", live);
                engine.releaseAllReaders();
                engine.releaseAllWriters();

                execute("ALTER TABLE pq FORCE REINDEX PARQUET PARTITION LIST '2024-01-01'");
                final File newDir = partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0));
                final String[] expected = {"_pm", "data.parquet", "sym.k." + live, "sym.v." + live};
                Assert.assertArrayEquals(expected, listDir(newDir));
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    settle(purgeJob);
                }
                Assert.assertFalse(oldDir.exists());
                Assert.assertArrayEquals(expected, listDir(newDir));
                applyO3Wave("nat", 1000, "00");
                assertTwins("after FORCE REINDEX");
            });
        } finally {
            setCurrentMicros(-1);
        }
    }

    @Test
    public void testForceReindexAllPartitionsChecksEachPartitionColumnVersion() throws Exception {
        // Only 2024-01-01 took the in-place commit: its live sym version is the commit's, while 2024-01-02 has no _cv
        // record of sym and keeps the unsuffixed version CONVERT wrote. Each new directory is checked against its own
        // day.
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
        try {
            assertMemoryLeak(() -> {
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    createSimpleTwins(IDX_BITMAP, false, "nat", "pq");
                    final String exp0 = printed(plainQuery("nat", "s0"));
                    final long live1;
                    final long day2NameTxn = partitionNameTxn(engine, "pq", 1);
                    try (RecordCursorFactory factory = select(indexQuery("pq", "s0")); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        applyO3Wave("pq", 1000, "00");
                        live1 = symColumnNameTxn();
                        Assert.assertEquals(-1, columnNameTxn("sym", 1));
                        assertSupersededVersionWaits("after O3", partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0)), "sym", live1);
                        execute("ALTER TABLE pq FORCE REINDEX PARQUET");
                        Assert.assertNotEquals(day2NameTxn, partitionNameTxn(engine, "pq", 1));
                        Assert.assertArrayEquals(
                                new String[]{"_pm", "data.parquet", "sym.k." + live1, "sym.v." + live1},
                                listDir(partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0)))
                        );
                        Assert.assertArrayEquals(
                                new String[]{"_pm", "data.parquet", "sym.k", "sym.v"},
                                listDir(partitionDir("pq", "2024-01-02", partitionNameTxn(engine, "pq", 1)))
                        );
                        TestUtils.assertEquals(exp0, drain(factory, cursor));
                    }
                    settle(purgeJob);
                    applyO3Wave("nat", 1000, "00");
                    assertTwins("after FORCE REINDEX");
                }
            });
        } finally {
            setCurrentMicros(-1);
        }
    }

    @Test
    public void testForceReindexCarriesOnlyTheLiveIndexVersionBitmap() throws Exception {
        assertForceReindexCarriesOnlyTheLiveIndexVersion(IDX_BITMAP, "TIMESTAMP", false, true, null);
    }

    @Test
    public void testForceReindexCarriesOnlyTheLiveIndexVersionCovering() throws Exception {
        assertForceReindexCarriesOnlyTheLiveIndexVersion(IDX_COVERING, "TIMESTAMP", false, true, null);
    }

    @Test
    public void testForceReindexCarriesOnlyTheLiveIndexVersionNonAsciiColumn() throws Exception {
        // An index file name starts with its column name: a 2-byte (Cyrillic) and a 4-byte (emoji) UTF-8 column name
        // must resolve to their indexed columns, so that their superseded versions stay out of the new directory.
        final String[] columns = {"символ", "s😀"};
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
        try {
            assertMemoryLeak(() -> {
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    for (String table : new String[]{"nat", "pq"}) {
                        execute("CREATE TABLE " + table + " (id INT, ts TIMESTAMP, \"символ\" SYMBOL INDEX, \"s😀\" SYMBOL INDEX TYPE POSTING, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                        execute("INSERT INTO " + table + " SELECT x::INT, '2024-01-01'::TIMESTAMP + (x - 1) * 7_200_000_000L, 's' || (x % 3), 't' || (x % 2), x FROM long_sequence(36)");
                    }
                    execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
                    final String query = "SELECT ts, \"символ\", \"s😀\", v FROM %s WHERE \"символ\" = 's0'";
                    final String exp0 = printed(String.format(query, "nat"));
                    final long nameTxnBefore = partitionNameTxn(engine, "pq", 0);
                    final String o3Insert = "INSERT INTO %s VALUES (1000, '2024-01-01T00:30:00.000000Z', 's0', 't1', 1000), (1001, '2024-01-01T01:30:00.000000Z', 's1', 't0', 1001)";
                    final File newDir;
                    final String[] newListing;
                    try (RecordCursorFactory factory = select(String.format(query, "pq")); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        execute(String.format(o3Insert, "pq"));
                        Assert.assertEquals(nameTxnBefore, partitionNameTxn(engine, "pq", 0));
                        final long[] live = {columnNameTxn(columns[0], 0), columnNameTxn(columns[1], 0)};
                        for (int i = 0; i < columns.length; i++) {
                            assertSupersededVersionWaits("after O3", partitionDir("pq", DAY, nameTxnBefore), columns[i], live[i]);
                        }
                        execute("ALTER TABLE pq FORCE REINDEX PARQUET PARTITION LIST '2024-01-01'");
                        newDir = partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0));
                        newListing = assertLiveOnly("after FORCE REINDEX", newDir, PARQUET_PARTITION_FILES, columns, live);
                        TestUtils.assertEquals(exp0, drain(factory, cursor));
                    }
                    settle(purgeJob);
                    Assert.assertArrayEquals(newListing, listDir(newDir));
                    execute(String.format(o3Insert, "nat"));
                    assertSqlCursors("SELECT * FROM nat", "SELECT * FROM pq");
                    final String[][] keys = {{"s0", "s1", "s2"}, {"t0", "t1"}};
                    for (int i = 0; i < columns.length; i++) {
                        final String column = '"' + columns[i] + '"';
                        assertQuery("SELECT ts, v FROM pq WHERE " + column + " = '" + keys[i][0] + "'")
                                .noLeakCheck()
                                .assertsPlanContaining("Index forward scan on: " + columns[i]);
                        for (String key : keys[i]) {
                            assertSqlCursors(
                                    "SELECT ts, v FROM nat WHERE concat(" + column + ", '') = '" + key + "'",
                                    "SELECT ts, v FROM pq WHERE " + column + " = '" + key + "'"
                            );
                        }
                    }
                }
            });
        } finally {
            setCurrentMicros(-1);
        }
    }

    @Test
    public void testForceReindexCarriesOnlyTheLiveIndexVersionPosting() throws Exception {
        assertForceReindexCarriesOnlyTheLiveIndexVersion(IDX_POSTING, "TIMESTAMP", false, true, null);
    }

    @Test
    public void testForceReindexCarriesOnlyTheLiveIndexVersionPostingDelta() throws Exception {
        assertForceReindexCarriesOnlyTheLiveIndexVersion("INDEX TYPE POSTING DELTA", "TIMESTAMP", false, true, null);
    }

    @Test
    public void testForceReindexCarriesOnlyTheLiveIndexVersionPostingEf() throws Exception {
        assertForceReindexCarriesOnlyTheLiveIndexVersion("INDEX TYPE POSTING EF", "TIMESTAMP", false, true, null);
    }

    @Test
    public void testForceReindexCarriesOnlyTheLiveIndexVersionTimestampNs() throws Exception {
        assertForceReindexCarriesOnlyTheLiveIndexVersion(IDX_BITMAP, "TIMESTAMP_NS", false, true, null);
    }

    @Test
    public void testForceReindexCarriesOnlyTheLiveIndexVersionWalBitmap() throws Exception {
        // ADAPTIVE defers every in-place purge, no reader needed; under NOSYNC the purge would run at once
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        assertForceReindexCarriesOnlyTheLiveIndexVersion(IDX_BITMAP, "TIMESTAMP", true, false, null);
    }

    @Test
    public void testForceReindexCarriesOnlyTheLiveIndexVersionWalCovering() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        assertForceReindexCarriesOnlyTheLiveIndexVersion(IDX_COVERING, "TIMESTAMP", true, false, null);
    }

    @Test
    public void testForceReindexCarriesOnlyTheLiveIndexVersionWalNosyncPinned() throws Exception {
        // The default commit mode defers the in-place purge while a reader holds the txn before the commit
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertForceReindexCarriesOnlyTheLiveIndexVersion(IDX_BITMAP, "TIMESTAMP", true, true, null);
    }

    @Test
    public void testForceReindexColumnKeepsOnlyTheLiveVersionOfEveryIndex() throws Exception {
        // FORCE REINDEX of sym alone links sym2's live files from the old directory: neither column may keep a
        // superseded version in the new one, and sym2's live files stay byte for byte.
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
        try {
            assertMemoryLeak(() -> {
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    createTwins(IDX_BITMAP);
                    final String exp0 = printed(plainQuery("nat", "s0"));
                    final File oldDir = partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0));
                    final String[] columns = {"sym", "sym2"};
                    final File newDir;
                    final String[] newListing;
                    try (RecordCursorFactory factory = select(indexQuery("pq", "s0")); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        applyO3Insert("pq");
                        final long[] live = {columnNameTxn("sym", 0), columnNameTxn("sym2", 0)};
                        for (int i = 0; i < columns.length; i++) {
                            assertSupersededVersionWaits("after O3", oldDir, columns[i], live[i]);
                        }
                        final Map<String, byte[]> oldFiles = dirFiles(oldDir);

                        execute("ALTER TABLE pq FORCE REINDEX PARQUET COLUMN sym PARTITION LIST '2024-01-01'");
                        newDir = partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0));
                        newListing = assertLiveOnly("after FORCE REINDEX", newDir, PARQUET_PARTITION_FILES, columns, live);
                        for (String name : newListing) {
                            if (isLiveIndexFile(name, "sym2", live[1])) {
                                Assert.assertArrayEquals(name, oldFiles.get(name), Files.readAllBytes(new File(newDir, name).toPath()));
                            }
                        }
                        assertFilesUnchanged("old directory after FORCE REINDEX", oldFiles, dirFiles(oldDir));
                        TestUtils.assertEquals(exp0, drain(factory, cursor));
                    }
                    settle(purgeJob);
                    Assert.assertFalse(oldDir.exists());
                    Assert.assertArrayEquals(newListing, listDir(newDir));
                    applyO3Insert("nat");
                    final StringBuilder mismatches = new StringBuilder();
                    checkTwins("after FORCE REINDEX", KEYS, mismatches);
                    if (mismatches.length() > 0) {
                        Assert.fail(mismatches.toString());
                    }
                }
            });
        } finally {
            setCurrentMicros(-1);
        }
    }

    @Test
    public void testForceReindexCrossDeviceCopyCarriesOnlyTheLiveIndexVersion() throws Exception {
        // FORCE REINDEX copies the directory instead of linking it
        final PartitionCopyFacade ff = new PartitionCopyFacade();
        ff.isCrossDevice = true;
        assertForceReindexCarriesOnlyTheLiveIndexVersion(IDX_BITMAP, "TIMESTAMP", false, true, ff);
    }

    @Test
    public void testForceReindexFailureAfterSupersededRemovalKeepsOldDirectory() throws Exception {
        // The index rebuild that follows the pass fails: FORCE REINDEX removes the whole new directory, and the old
        // one, which the pass never touches, still holds every file of the pinned snapshot. The retry builds a fresh
        // copy.
        final PartitionCopyFacade ff = new PartitionCopyFacade();
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
        try {
            assertMemoryLeak(ff, () -> {
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    createSimpleTwins(IDX_BITMAP, false, "nat", "pq");
                    final String exp0 = printed(plainQuery("nat", "s0"));
                    final long nameTxnBefore = partitionNameTxn(engine, "pq", 0);
                    final File oldDir = partitionDir("pq", DAY, nameTxnBefore);
                    final File newDir;
                    final long live;
                    try (RecordCursorFactory factory = select(indexQuery("pq", "s0")); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        applyO3Wave("pq", 1000, "00");
                        live = symColumnNameTxn();
                        assertSupersededVersionWaits("after O3", oldDir, "sym", live);
                        final Map<String, byte[]> oldFiles = dirFiles(oldDir);
                        try (TableWriter writer = getWriter("pq")) {
                            newDir = partitionDir("pq", DAY, writer.getTxn());
                        }

                        ff.failOpenRw = io.questdb.std.Files.SEPARATOR + newDir.getName() + io.questdb.std.Files.SEPARATOR + "sym.";
                        try {
                            execute("ALTER TABLE pq FORCE REINDEX PARQUET PARTITION LIST '2024-01-01'");
                            Assert.fail("the index rebuild must fail");
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "could not open read-write");
                        }
                        Assert.assertEquals(1, ff.faultCount.get());
                        Assert.assertFalse("the new directory must go: " + newDir, newDir.exists());
                        Assert.assertEquals(nameTxnBefore, partitionNameTxn(engine, "pq", 0));
                        assertFilesUnchanged("old directory after the failed FORCE REINDEX", oldFiles, dirFiles(oldDir));

                        execute("ALTER TABLE pq FORCE REINDEX PARQUET PARTITION LIST '2024-01-01'");
                        Assert.assertEquals(newDir, partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0)));
                        Assert.assertArrayEquals(new String[]{"_pm", "data.parquet", "sym.k." + live, "sym.v." + live}, listDir(newDir));
                        assertFilesUnchanged("old directory after the retried FORCE REINDEX", oldFiles, dirFiles(oldDir));
                        TestUtils.assertEquals(exp0, drain(factory, cursor));
                    }
                    settle(purgeJob);
                    Assert.assertFalse(oldDir.exists());
                    Assert.assertArrayEquals(new String[]{"_pm", "data.parquet", "sym.k." + live, "sym.v." + live}, listDir(newDir));
                    applyO3Wave("nat", 1000, "00");
                    assertTwins("after FORCE REINDEX");
                }
            });
        } finally {
            setCurrentMicros(-1);
        }
    }

    @Test
    public void testForceReindexKeepsFilesItCannotClassify() throws Exception {
        // Only an index file of an indexed column under another column name txn than the live one is superseded. The
        // index-like file of a column that is not indexed or does not exist, an index kind with a malformed txn, a
        // column data file and a name without a dot stay, next to the superseded sym.k and sym.v that go.
        final String[] strays = {"notes", "sym.d.1", "sym.k.x", "u.k.1", "zz.k.1"};
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
        try {
            assertMemoryLeak(() -> {
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    // u, a SYMBOL that is not indexed, is in the parquet files: a later column would make the O3
                    // commit rewrite the partition instead of updating it in place
                    for (String table : new String[]{"nat", "pq"}) {
                        execute("CREATE TABLE " + table + " (id INT, ts TIMESTAMP, sym SYMBOL INDEX, v LONG, u SYMBOL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                        execute("INSERT INTO " + table + " SELECT x::INT, '2024-01-01'::TIMESTAMP + (x - 1) * 7_200_000_000L, 's' || (x % 3), x, 'u' || (x % 2) FROM long_sequence(36)");
                    }
                    execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
                    final File oldDir = partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0));
                    final File newDir;
                    final long live;
                    try (RecordCursorFactory factory = select(indexQuery("pq", "s0")); RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                        applyO3Wave("pq", 1000, "00");
                        live = symColumnNameTxn();
                        Assert.assertArrayEquals(new String[]{"_pm", "data.parquet", "sym.k", "sym.k." + live, "sym.v", "sym.v." + live}, listDir(oldDir));
                        for (String stray : strays) {
                            Assert.assertTrue(stray, new File(oldDir, stray).createNewFile());
                        }
                        execute("ALTER TABLE pq FORCE REINDEX PARQUET PARTITION LIST '2024-01-01'");
                        newDir = partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0));
                    }
                    final String[] expected = {"_pm", "data.parquet", "notes", "sym.d.1", "sym.k." + live, "sym.k.x", "sym.v." + live, "u.k.1", "zz.k.1"};
                    Assert.assertArrayEquals(expected, listDir(newDir));
                    settle(purgeJob);
                    Assert.assertArrayEquals(expected, listDir(newDir));
                    applyO3Wave("nat", 1000, "00");
                    assertTwins("after FORCE REINDEX");
                }
            });
        } finally {
            setCurrentMicros(-1);
        }
    }

    @Test
    public void testForceReindexKeepsSupersededFileItCannotRemove() throws Exception {
        // A superseded index file the pass cannot remove stays in the new directory, as before the pass existed, and
        // the error log names it; VACUUM TABLE removes it later. A failed removal that still leaves no file, as when
        // another unlink got there first, has nothing to report.
        final PartitionCopyFacade ff = new PartitionCopyFacade();
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            assertMemoryLeak(ff, () -> {
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    createSimpleTwins(IDX_BITMAP, false, "nat", "pq");
                    final String exp0 = printed(plainQuery("nat", "s0"));
                    final File newDir;
                    final long live;
                    try (RecordCursorFactory factory = select(indexQuery("pq", "s0")); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        applyO3Wave("pq", 1000, "00");
                        live = symColumnNameTxn();
                        assertSupersededVersionWaits("after O3", partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0)), "sym", live);
                        try (TableWriter writer = getWriter("pq")) {
                            newDir = partitionDir("pq", DAY, writer.getTxn());
                        }

                        final String keptFile = io.questdb.std.Files.SEPARATOR + newDir.getName() + io.questdb.std.Files.SEPARATOR + "sym.k";
                        final String goneFile = io.questdb.std.Files.SEPARATOR + newDir.getName() + io.questdb.std.Files.SEPARATOR + "sym.v";
                        ff.keptPathSuffix = keptFile;
                        ff.goneDespiteFailurePathSuffix = goneFile;
                        execute("ALTER TABLE pq FORCE REINDEX PARQUET PARTITION LIST '2024-01-01'");
                        capture.drain();
                        capture.assertLoggedRE("could not remove superseded index file \\[path=[^\\]]*" + Pattern.quote(keptFile) + ", errno=");
                        capture.assertNotLogged(goneFile + ", errno=");
                        ff.keptPathSuffix = null;
                        ff.goneDespiteFailurePathSuffix = null;
                        Assert.assertEquals(2, ff.faultCount.get());
                        Assert.assertEquals(newDir, partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0)));
                        Assert.assertArrayEquals(new String[]{"_pm", "data.parquet", "sym.k", "sym.k." + live, "sym.v." + live}, listDir(newDir));
                        TestUtils.assertEquals(exp0, drain(factory, cursor));
                    }
                    settle(purgeJob);
                    Assert.assertArrayEquals(new String[]{"_pm", "data.parquet", "sym.k", "sym.k." + live, "sym.v." + live}, listDir(newDir));
                    applyO3Wave("nat", 1000, "00");
                    assertTwins("after FORCE REINDEX");

                    execute("VACUUM TABLE pq");
                    runPurgeJob(purgeJob);
                    Assert.assertArrayEquals(new String[]{"_pm", "data.parquet", "sym.k." + live, "sym.v." + live}, listDir(newDir));
                    assertTwins("after VACUUM");
                }
            });
        } finally {
            capture.stop();
            setCurrentMicros(-1);
        }
    }

    @Test
    public void testForceReindexKeepsUndecodableName() throws Exception {
        // A file name that is not valid UTF-8 decodes only up to its first bad byte. Here that prefix, sym.k, names the
        // superseded index version, so classifying the prefix would remove a file the pass cannot even name. FORCE
        // REINDEX carries it into the new directory, and DETACH of that directory into its copy. Only Linux file
        // systems take such a name: APFS and NTFS reject it.
        Assume.assumeTrue(Os.isLinux());
        final String undecodable = "sym.k\uFFFD";
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
        try {
            assertMemoryLeak(() -> {
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    createSimpleTwins(IDX_BITMAP, false, "nat", "pq");
                    final File oldDir = partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0));
                    final long live;
                    try (RecordCursorFactory factory = select(indexQuery("pq", "s0")); RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                        applyO3Wave("pq", 1000, "00");
                        live = symColumnNameTxn();
                        assertSupersededVersionWaits("after O3", oldDir, "sym", live);
                        try (Path path = new Path()) {
                            path.of(oldDir.getAbsolutePath()).concat("sym.k").put((byte) 0xFF);
                            Assert.assertTrue(configuration.getFilesFacade().touch(path.$()));
                        }
                        execute("ALTER TABLE pq FORCE REINDEX PARQUET PARTITION LIST '2024-01-01'");
                    }
                    final File newDir = partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0));
                    Assert.assertArrayEquals(new String[]{"_pm", "data.parquet", "sym.k." + live, undecodable, "sym.v." + live}, listDir(newDir));
                    settle(purgeJob);
                    Assert.assertFalse(oldDir.exists());

                    execute("ALTER TABLE pq DETACH PARTITION LIST '2024-01-01'");
                    Assert.assertArrayEquals(
                            new String[]{"_cv", "_meta", "_pm", "_txn", "data.parquet", "sym.k." + live, undecodable, "sym.v." + live},
                            listDir(detachedCopy("pq", DAY))
                    );
                    attach("pq", DAY);
                    applyO3Wave("nat", 1000, "00");
                    assertTwins("after ATTACH");
                }
            });
        } finally {
            setCurrentMicros(-1);
        }
    }

    @Test
    public void testForceReindexKeepsUnsuffixedLiveIndex() throws Exception {
        // ADD INDEX on a parquet day names the index files by the column's _cv record, none here: the unsuffixed sym.k
        // and sym.v are the live version, as in a day an earlier QuestDB version wrote, and FORCE REINDEX and DETACH
        // keep them.
        assertMemoryLeak(() -> {
            createSimpleTwins("", false, "nat", "pq");
            execute("ALTER TABLE nat ALTER COLUMN sym ADD INDEX");
            execute("ALTER TABLE pq ALTER COLUMN sym ADD INDEX");
            Assert.assertEquals(-1, columnNameTxn("sym", 0));
            Assert.assertEquals(-1, columnNameTxn("sym", 1));
            final String[] unsuffixed = {"_pm", "data.parquet", "sym.k", "sym.v"};
            Assert.assertArrayEquals(unsuffixed, listDir(partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0))));

            execute("ALTER TABLE pq FORCE REINDEX PARQUET PARTITION LIST '2024-01-01'");
            Assert.assertArrayEquals(unsuffixed, listDir(partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0))));

            execute("ALTER TABLE pq DETACH PARTITION LIST '2024-01-02'");
            final String[] detached = {"_cv", "_meta", "_pm", "_txn", "data.parquet", "sym.k", "sym.v"};
            Assert.assertArrayEquals(detached, listDir(detachedCopy("pq", "2024-01-02")));
            attach("pq", "2024-01-02");
            Assert.assertArrayEquals(detached, listDir(partitionDir("pq", "2024-01-02", partitionNameTxn(engine, "pq", 1))));
            assertTwins("after ATTACH");
        });
    }

    @Test
    public void testForceReindexRepeatedKeepsOnlyTheLiveIndexVersion() throws Exception {
        // Each round pins the live version, commits in place, reindexes and lets the purges run: a superseded version
        // a round carried would stay for good, and every round would add one more.
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
        try {
            assertMemoryLeak(() -> {
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    createSimpleTwins(IDX_BITMAP, false, "nat", "pq");
                    final String[] hours = {"00", "03", "05"};
                    long previousLive = symColumnNameTxn();
                    for (int round = 0; round < hours.length; round++) {
                        final String stage = "round " + round;
                        final File oldDir = partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0));
                        final long live;
                        try (RecordCursorFactory factory = select(indexQuery("pq", "s0")); RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                            applyO3Wave("pq", 1000 + 10 * round, hours[round]);
                            live = symColumnNameTxn();
                            Assert.assertNotEquals(stage, previousLive, live);
                            assertSupersededVersionWaits(stage, oldDir, "sym", live);
                            execute("ALTER TABLE pq FORCE REINDEX PARQUET PARTITION LIST '2024-01-01'");
                        }
                        settle(purgeJob);
                        Assert.assertFalse(stage + ": the replaced directory must go", oldDir.exists());
                        Assert.assertArrayEquals(
                                stage,
                                new String[]{"_pm", "data.parquet", "sym.k." + live, "sym.v." + live},
                                listDir(partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0)))
                        );
                        applyO3Wave("nat", 1000 + 10 * round, hours[round]);
                        assertTwins(stage);
                        previousLive = live;
                    }
                }
            });
        } finally {
            setCurrentMicros(-1);
        }
    }

    @Test
    public void testInPlaceCommitsReleasePurgeEntries() throws Exception {
        assertMemoryLeak(() -> {
            createSimpleTwins(IDX_BITMAP, false, "nat", "pq");
            final long nameTxnBefore = partitionNameTxn(engine, "pq", 0);
            final long symNameTxnBefore = symColumnNameTxn();
            final long ts = MicrosTimestampDriver.floor("2024-01-01T00:30:00.000000Z");
            // One writer for every commit, as under continuous ingestion: each in-place
            // O3 supersedes the sym index version, and housekeep() must release the
            // purge entry it queued instead of keeping it for the writer's lifetime.
            try (TableWriter writer = getWriter("pq")) {
                for (int i = 0; i < 5; i++) {
                    final TableWriter.Row row = writer.newRow(ts + i);
                    row.putInt(0, 1000 + i);
                    row.putSym(2, "s1");
                    row.putLong(3, 1000 + i);
                    row.append();
                    writer.commit();
                    Assert.assertEquals("commit " + i, 0, writer.getParquetIndexPurgeColumnNameCount());
                }
            }
            final StringBuilder mismatches = new StringBuilder();
            assertInPlace("after o3 commits", nameTxnBefore, symNameTxnBefore, mismatches);
            if (mismatches.length() > 0) {
                Assert.fail(mismatches.toString());
            }
        });
    }

    @Test
    public void testNonWalBitmap() throws Exception {
        assertNonWalInPlace(IDX_BITMAP);
    }

    @Test
    public void testNonWalCovering() throws Exception {
        assertNonWalInPlace(IDX_COVERING);
    }

    @Test
    public void testNonWalPosting() throws Exception {
        assertNonWalInPlace(IDX_POSTING);
    }

    @Test
    public void testPinnedCursorMidScanBitmap() throws Exception {
        assertPinned(IDX_BITMAP, true);
    }

    @Test
    public void testPinnedCursorMidScanCovering() throws Exception {
        assertPinned(IDX_COVERING, true);
    }

    @Test
    public void testPinnedCursorMidScanDropBitmap() throws Exception {
        assertPinned(IDX_BITMAP, true, ParquetInPlaceIndexPinnedReaderTest::applyDrop);
    }

    @Test
    public void testPinnedCursorMidScanDropCovering() throws Exception {
        assertPinned(IDX_COVERING, true, ParquetInPlaceIndexPinnedReaderTest::applyDrop);
    }

    @Test
    public void testPinnedCursorMidScanDropPosting() throws Exception {
        assertPinned(IDX_POSTING, true, ParquetInPlaceIndexPinnedReaderTest::applyDrop);
    }

    @Test
    public void testPinnedCursorMidScanPosting() throws Exception {
        assertPinned(IDX_POSTING, true);
    }

    @Test
    public void testPinnedCursorNotStartedBitmap() throws Exception {
        assertPinned(IDX_BITMAP, false);
    }

    @Test
    public void testPinnedCursorNotStartedCovering() throws Exception {
        assertPinned(IDX_COVERING, false);
    }

    @Test
    public void testPinnedCursorNotStartedPosting() throws Exception {
        assertPinned(IDX_POSTING, false);
    }

    @Test
    public void testSkippedFailedRebuildOrphansPurgedByVacuumBitmap() throws Exception {
        assertSkippedFailedRebuildOrphansPurgedByVacuum(IDX_BITMAP, "sym2.k");
    }

    @Test
    public void testSkippedFailedRebuildOrphansPurgedByVacuumCovering() throws Exception {
        assertSkippedFailedRebuildOrphansPurgedByVacuum(IDX_COVERING, "sym2.pk");
    }

    @Test
    public void testSkippedFailedRebuildOrphansPurgedByVacuumPosting() throws Exception {
        assertSkippedFailedRebuildOrphansPurgedByVacuum(IDX_POSTING, "sym2.pk");
    }

    @Test
    public void testSupersededFilesPurgedAfterPinnedReaderClosesBitmap() throws Exception {
        assertSupersededFilesPurged(IDX_BITMAP);
    }

    @Test
    public void testSupersededFilesPurgedAfterPinnedReaderClosesCovering() throws Exception {
        // Covers the .pci/.pc sidecars: every one of the old version's files must
        // survive the O3 byte for byte while the reader is pinned.
        assertSupersededFilesPurged(IDX_COVERING);
    }

    @Test
    public void testSupersededFilesPurgedAfterPinnedReaderClosesPosting() throws Exception {
        assertSupersededFilesPurged(IDX_POSTING);
    }

    @Test
    public void testTwoVersionsPinnedThenConvertNativeBitmap() throws Exception {
        assertTwoVersionsPinnedThenConvertNative(IDX_BITMAP);
    }

    @Test
    public void testTwoVersionsPinnedThenConvertNativeCovering() throws Exception {
        assertTwoVersionsPinnedThenConvertNative(IDX_COVERING);
    }

    @Test
    public void testTwoVersionsPinnedThenConvertNativePosting() throws Exception {
        assertTwoVersionsPinnedThenConvertNative(IDX_POSTING);
    }

    private static void applyCrossO3Insert(String table) throws Exception {
        execute(String.format(CROSS_O3_INSERT, table));
        drainWalQueue();
    }

    // A replace commit over [2024-01-01T19:00, 2024-01-02T05:00] that brings a row on each
    // day: row group 2 of 2024-01-01 (16:00..22:00) and row group 0 of 2024-01-02
    // (00:00..06:00) both merge in place.
    private static void applyCrossReplace(String table) throws Exception {
        applyReplace(table, "2024-01-01T19:00:00.000000Z", "2024-01-02T05:00:00.000000Z", "2024-01-01T21:00:00.000000Z", "2024-01-02T03:00:00.000000Z");
    }

    // A replace commit over [07:00, 15:00] of 2024-01-01 that brings one row at
    // 15:00: row group 1 (08:00..14:00) is dropped in place, so row group 2's row
    // ids and ordinal shift down.
    private static void applyDrop(String table) throws Exception {
        try (WalWriter ww = engine.getWalWriter(engine.verifyTableName(table))) {
            final TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2024-01-01T15:00:00.000000Z"));
            row.putInt(0, 2000);
            row.putSym(2, "s1");
            row.putSym(3, "t1");
            row.putLong(4, 2000);
            row.append();
            ww.commitWithParams(
                    MicrosTimestampDriver.floor("2024-01-01T07:00:00.000000Z"),
                    MicrosTimestampDriver.floor("2024-01-01T15:00:00.000000Z") + 1,
                    WAL_DEDUP_MODE_REPLACE_RANGE
            );
        }
        drainWalQueue();
    }

    // A replace commit over [08:30, 11:30] of 2024-01-01 that brings no rows: a filter-only
    // MERGE of row group 1 (08:00..14:00) removes the 10:00 row, so row group 2 shifts.
    private static void applyFilterMerge(String table) throws Exception {
        applyReplace(table, "2024-01-01T08:30:00.000000Z", "2024-01-01T11:30:00.000000Z");
    }

    private static void applyO3Insert(String table) throws Exception {
        execute(String.format(O3_INSERT, table));
        drainWalQueue();
    }

    // Commits a replace over the inclusive range [lo, hi] that brings one row per timestamp.
    private static void applyReplace(String table, String lo, String hi, String... rowTs) throws Exception {
        try (WalWriter ww = engine.getWalWriter(engine.verifyTableName(table))) {
            for (int i = 0; i < rowTs.length; i++) {
                final TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor(rowTs[i]));
                row.putInt(0, 3000 + i);
                row.putSym(2, i % 2 == 0 ? "s1" : "s2");
                row.putSym(3, "t1");
                row.putLong(4, 3000 + i);
                row.append();
            }
            ww.commitWithParams(
                    MicrosTimestampDriver.floor(lo),
                    MicrosTimestampDriver.floor(hi) + 1,
                    WAL_DEDUP_MODE_REPLACE_RANGE
            );
        }
        drainWalQueue();
    }

    // A replace commit over [2024-01-01T00:00, 2024-01-02T01:00] that brings a row at
    // 2024-01-02T00:30: it removes every row of 2024-01-01, so that partition is removed,
    // and row group 0 of 2024-01-02 merges in place.
    private static void applyReplaceRemovingFirstDay(String table) throws Exception {
        applyReplace(table, "2024-01-01T00:00:00.000000Z", "2024-01-02T01:00:00.000000Z", "2024-01-02T00:30:00.000000Z");
    }

    private static void assertFilesUnchanged(String stage, Map<String, byte[]> before, Map<String, byte[]> after) {
        Assert.assertEquals("[" + stage + "] files", before.keySet(), after.keySet());
        for (Map.Entry<String, byte[]> e : before.entrySet()) {
            Assert.assertArrayEquals("[" + stage + "] " + e.getKey(), e.getValue(), after.get(e.getKey()));
        }
    }

    private static void assertInPlace(String stage, long nameTxnBefore, long symNameTxnBefore, StringBuilder mismatches) {
        final long nameTxnAfter = partitionNameTxn(engine, "pq", 0);
        if (nameTxnAfter != nameTxnBefore) {
            mismatches.append("\n[").append(stage).append("] parquet O3 must update in place, name txn ")
                    .append(nameTxnBefore).append(" -> ").append(nameTxnAfter);
        }
        final long symNameTxnAfter = symColumnNameTxn();
        if (symNameTxnAfter == symNameTxnBefore) {
            mismatches.append("\n[").append(stage).append("] the in-place index rebuild must publish a new column name txn, stayed ")
                    .append(symNameTxnBefore);
        }
    }

    /**
     * Asserts that dir holds otherFiles and, of each of the indexed columns, only the index files of its live column
     * name txn, the key and value files among them, and nothing else. Returns the sorted listing.
     */
    private static String[] assertLiveOnly(String stage, File dir, String[] otherFiles, String[] columns, long[] liveTxns) {
        final String[] names = listDir(dir);
        final StringBuilder problems = new StringBuilder();
        final Set<String> missingOtherFiles = new TreeSet<>(Arrays.asList(otherFiles));
        final boolean[] hasKeyFile = new boolean[columns.length];
        final boolean[] hasValueFile = new boolean[columns.length];
        for (String name : names) {
            if (missingOtherFiles.remove(name)) {
                continue;
            }
            boolean isLive = false;
            for (int i = 0; i < columns.length; i++) {
                if (isLiveIndexFile(name, columns[i], liveTxns[i])) {
                    isLive = true;
                    final String kind = name.substring(columns[i].length() + 1).split("\\.")[0];
                    hasKeyFile[i] |= kind.equals("k") || kind.equals("pk");
                    hasValueFile[i] |= kind.equals("v") || kind.equals("pv");
                }
            }
            if (!isLive) {
                problems.append(" unexpected ").append(name).append(';');
            }
        }
        for (String name : missingOtherFiles) {
            problems.append(" missing ").append(name).append(';');
        }
        for (int i = 0; i < columns.length; i++) {
            if (!hasKeyFile[i] || !hasValueFile[i]) {
                problems.append(" no key or value file of ").append(columns[i]).append(" at txn ").append(liveTxns[i]).append(';');
            }
        }
        if (problems.length() > 0) {
            Assert.fail("[" + stage + "] " + dir.getName() + " " + Arrays.toString(names) + ":" + problems);
        }
        return names;
    }

    private static String[] assertLiveOnly(String stage, File dir, String[] otherFiles, String column, long live) {
        return assertLiveOnly(stage, dir, otherFiles, new String[]{column}, new long[]{live});
    }

    // Asserts that dir holds the live index version of column and another one, waiting for its purge
    private static void assertSupersededVersionWaits(String stage, File dir, String column, long live) {
        final String[] names = listDir(dir);
        boolean hasLive = false;
        boolean hasSuperseded = false;
        for (String name : names) {
            if (isLiveIndexFile(name, column, live)) {
                hasLive = true;
            } else if (isIndexFileOf(name, column)) {
                hasSuperseded = true;
            }
        }
        Assert.assertTrue(
                "[" + stage + "] " + dir.getName() + " must hold the live version " + live + " of " + column
                        + " and a superseded one: " + Arrays.toString(names),
                hasLive && hasSuperseded
        );
    }

    private static void assertTwins(String stage) throws Exception {
        final StringBuilder mismatches = new StringBuilder();
        checkTwins(stage, KEYS, mismatches);
        if (mismatches.length() > 0) {
            Assert.fail(mismatches.toString());
        }
    }

    // Renames the DETACH copy of day to its attachable name and attaches it; on an adaptive table the save points
    // first give the copy its final .detached name
    private static void attach(String table, String day) throws Exception {
        releaseSavePointHolds(table);
        final File tableDir = tableDir(table);
        Files.move(
                new File(tableDir, day + TableUtils.DETACHED_DIR_MARKER).toPath(),
                new File(tableDir, day + configuration.getAttachPartitionSuffix()).toPath()
        );
        execute("ALTER TABLE " + table + " ATTACH PARTITION LIST '" + day + "'");
        drainWalQueue();
    }

    // A column's name txn in a pq partition, from a fresh reader.
    private static long columnNameTxn(String column, int partitionIndex) {
        try (TableReader reader = engine.getReader("pq")) {
            final long partitionTimestamp = reader.getTxFile().getPartitionTimestampByIndex(partitionIndex);
            return reader.getColumnVersionReader().getColumnNameTxn(partitionTimestamp, reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(column)));
        }
    }

    // The parquet day a path belongs to, or null.
    private static String dayOf(LPSZ path) {
        if (Utf8s.containsAscii(path, "2024-01-01")) {
            return "2024-01-01";
        }
        if (Utf8s.containsAscii(path, "2024-01-02")) {
            return "2024-01-02";
        }
        return null;
    }

    // The names of the DETACH copies of a day: its .detached directory and the temporary .detaching ones
    private static String[] detachCopies(String table, String day) {
        final String[] names = tableDir(table).list((dir, name) -> name.startsWith(day + ".")
                && (name.endsWith(TableUtils.DETACHED_DIR_MARKER) || name.endsWith(TableUtils.DETACHING_DIR_MARKER)));
        Assert.assertNotNull(names);
        Arrays.sort(names);
        return names;
    }

    // The DETACH copy of a day: <day>.detached, or <day>.<txn>.detaching until the save points cover the DETACH
    private static File detachedCopy(String table, String day) {
        final String[] names = detachCopies(table, day);
        Assert.assertEquals("one DETACH copy of " + day + ": " + Arrays.toString(names), 1, names.length);
        return new File(tableDir(table), names[0]);
    }

    // The files of a directory with ASCII names, by name, with their content
    private static Map<String, byte[]> dirFiles(File dir) throws IOException {
        final Map<String, byte[]> files = new TreeMap<>();
        for (String name : listDir(dir)) {
            files.put(name, Files.readAllBytes(new File(dir, name).toPath()));
        }
        return files;
    }

    // Prints factory's header, then every remaining row of cursor: a cursor opened
    // before an O3 commit and drained after it reads the old snapshot through its
    // pinned txn.
    private static String drain(RecordCursorFactory factory, RecordCursor cursor) {
        final StringSink sink = new StringSink();
        CursorPrinter.println(factory.getMetadata(), sink);
        readRest(cursor, factory, sink);
        return sink.toString();
    }

    // While armed, the first openRW of a file whose path contains partition and fileName
    // fails once.
    private static FilesFacade failOpenOnce(String partition, String fileName, AtomicBoolean armed, AtomicInteger faults) {
        return new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (armed.get()
                        && Utf8s.containsAscii(name, partition)
                        && Utf8s.containsAscii(name, fileName)
                        && armed.compareAndSet(true, false)) {
                    faults.incrementAndGet();
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
    }

    private static String indexQuery(String table, String key) {
        return "SELECT ts, sym, v FROM " + table + " WHERE sym = '" + key + "'";
    }

    // Whether fileName has the form of an index file of column, see isLiveIndexFile()
    private static boolean isIndexFileOf(String fileName, String column) {
        if (!fileName.startsWith(column + ".")) {
            return false;
        }
        final String kind = fileName.substring(column.length() + 1).split("\\.", -1)[0];
        return switch (kind) {
            case "k", "v", "pk", "pci", "pv" -> true;
            default -> kind.matches("pc[0-9]+");
        };
    }

    /**
     * Whether fileName is an index file of column under the column name txn live, by the file names IndexFactory and
     * PostingIndexUtils give: {@code <column>.<k|v|pk|pci>[.<txn>]} and {@code <column>.pv[.<txn>].<sealTxn>}, which
     * leave the txn out for txn -1, and {@code <column>.pc<N>.<host>[.<covered column name txn>].<sealTxn>}, whose
     * host is the txn, or 0 for txn -1.
     */
    private static boolean isLiveIndexFile(String fileName, String column, long live) {
        if (!fileName.startsWith(column + ".")) {
            return false;
        }
        final String[] parts = fileName.substring(column.length() + 1).split("\\.", -1);
        final String txn = Long.toString(live);
        return switch (parts[0]) {
            case "k", "v", "pk", "pci" -> live == -1 ? parts.length == 1 : parts.length == 2 && parts[1].equals(txn);
            case "pv" -> live == -1
                    ? parts.length == 2 && parts[1].matches("[0-9]+")
                    : parts.length == 3 && parts[1].equals(txn) && parts[2].matches("[0-9]+");
            default -> parts[0].matches("pc[0-9]+")
                    && (parts.length == 3 || parts.length == 4)
                    && parts[1].equals(live == -1 ? "0" : txn);
        };
    }

    // The names in a directory, sorted; a name that is not valid UTF-8 shows its valid prefix and U+FFFD
    private static String[] listDir(File dir) {
        final ObjList<String> names = new ObjList<>();
        try (Path path = new Path()) {
            path.of(dir.getAbsolutePath());
            Assert.assertTrue("directory must exist: " + dir, TestFilesFacadeImpl.INSTANCE.exists(path.$()));
            TestFilesFacadeImpl.INSTANCE.iterateDir(path.$(), (pUtf8NameZ, type) -> {
                if (io.questdb.std.Files.notDots(pUtf8NameZ)) {
                    final StringSink name = new StringSink();
                    if (!Utf8s.utf8ToUtf16Z(pUtf8NameZ, name)) {
                        name.put('\uFFFD');
                    }
                    names.add(name.toString());
                }
            });
        }
        final String[] sorted = new String[names.size()];
        for (int i = 0; i < sorted.length; i++) {
            sorted[i] = names.getQuick(i);
        }
        Arrays.sort(sorted);
        return sorted;
    }

    // The directory of a day of table at a partition name txn
    private static File partitionDir(String table, String day, long nameTxn) {
        return new File(tableDir(table), nameTxn > -1 ? day + "." + nameTxn : day);
    }

    // sym.* and sym2.* files (the index files) of pq's first partition, by name, with
    // their content.
    private static Map<String, byte[]> indexFiles() throws IOException {
        return indexFiles(0);
    }

    // sym.* and sym2.* files (the index files) of a pq partition, by name, with their content.
    private static Map<String, byte[]> indexFiles(int partitionIndex) throws IOException {
        final TableToken token = engine.verifyTableName("pq");
        final Map<String, byte[]> files = new TreeMap<>();
        try (TableReader reader = engine.getReader(token); Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token);
            TableUtils.setPathForNativePartition(
                    path,
                    ColumnType.TIMESTAMP,
                    PartitionBy.DAY,
                    reader.getTxFile().getPartitionTimestampByIndex(partitionIndex),
                    reader.getTxFile().getPartitionNameTxn(partitionIndex)
            );
            final File dir = new File(path.toString());
            final String[] names = dir.list();
            Assert.assertNotNull("partition dir must exist: " + dir, names);
            for (String name : names) {
                if (name.startsWith("sym.") || name.startsWith("sym2.")) {
                    files.put(name, Files.readAllBytes(new File(dir, name).toPath()));
                }
            }
        }
        return files;
    }

    private static String plainQuery(String table, String key) {
        // The same rows without the index: a filter on a function of sym.
        return "SELECT ts, sym, v FROM " + table + " WHERE concat(sym, '') = '" + key + "'";
    }

    private static String printed(CharSequence sql) throws Exception {
        final StringSink out = new StringSink();
        printSql(sql, out);
        return out.toString();
    }

    private static void readRest(RecordCursor cursor, RecordCursorFactory factory, StringSink sink) {
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            CursorPrinter.println(record, factory.getMetadata(), sink);
        }
    }

    private static void runPurgeJob(ColumnPurgeJob purgeJob) {
        engine.releaseInactive();
        setCurrentMicros(currentMicros + 10);
        purgeJob.run();
        setCurrentMicros(currentMicros + 10);
        purgeJob.run();
    }

    // Runs the purges the closed readers and the save points held back: the column purges of the superseded index
    // versions and the removal of the replaced partition directories
    private static void settle(ColumnPurgeJob purgeJob) {
        engine.releaseAllReaders();
        for (int round = 0; round < 2; round++) {
            releaseSavePointHolds("pq");
            runPurgeJob(purgeJob);
            try (O3PartitionPurgeJob partitionPurgeJob = new O3PartitionPurgeJob(engine, 1)) {
                partitionPurgeJob.drain(0);
            }
        }
    }

    // sym's column name txn in pq's first partition, from a fresh reader.
    private static long symColumnNameTxn() {
        return columnNameTxn("sym", 0);
    }

    private static File tableDir(String table) {
        return new File(configuration.getDbRoot().toString(), engine.verifyTableName(table).getDirName());
    }

    private static String[] withFile(String[] files, String file) {
        final String[] result = Arrays.copyOf(files, files.length + 1);
        result[files.length] = file;
        Arrays.sort(result);
        return result;
    }

    // The last seqTxn the table writer applied.
    private static long writerSeqTxn(String table) throws Exception {
        final StringSink sink = new StringSink();
        printSql("SELECT writerTxn FROM wal_tables() WHERE name = '" + table + "'", sink);
        return Long.parseLong(sink.toString().split("\n")[1]);
    }

    // Two out-of-order rows into 2024-01-01: one at hour:30 (sym s1), one at the next
    // hour mod 10 :45 (sym s0). baseId seeds both rows' id and v.
    private void applyO3Wave(String table, int baseId, String hour) throws Exception {
        execute("INSERT INTO " + table + "(id, ts, sym, v) VALUES (" + baseId + ", '2024-01-01T" + hour + ":30:00.000000Z', 's1', " + baseId + "),"
                + "(" + (baseId + 1) + ", '2024-01-01T0" + (Integer.parseInt(hour) % 10) + ":45:00.000000Z', 's0', " + (baseId + 1) + ")");
        drainWalQueue();
    }

    private void assertCrashWindow(String indexClause, String faultFile, TableCommit commit) throws Exception {
        // sym is rebuilt first, then an open of faultFile fails (sym2's index, or a
        // covering sidecar of sym in the reseal that follows the O3 workers): the
        // failure lands after sym's index was rebuilt for the new row layout and
        // before _txn commits.
        final AtomicBoolean armed = new AtomicBoolean(false);
        final AtomicInteger faults = new AtomicInteger();
        assertMemoryLeak(failOpenOnce("2024-01-01", faultFile, armed, faults), () -> {
            createTwins(indexClause);
            final long nameTxnBefore = partitionNameTxn(engine, "pq", 0);
            final long symNameTxnBefore = symColumnNameTxn();
            final Map<String, byte[]> filesBefore = indexFiles();
            final String[] keys = {"s0", "s1", "s2"};
            final String[] oldExpected = new String[keys.length];
            for (int i = 0; i < keys.length; i++) {
                oldExpected[i] = printed(plainQuery("nat", keys[i]));
            }

            armed.set(true);
            commit.apply("pq");
            Assert.assertEquals("fault must fire exactly once", 1, faults.get());
            Assert.assertTrue("pq must suspend", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
            Assert.assertEquals(nameTxnBefore, partitionNameTxn(engine, "pq", 0));
            // A failure after finishO3Commit moved _cv (the covering reseal) has queued the
            // superseded version for the purge: the rollback must drop it with _cv. The writer
            // survives the failure (not distressed), so the pool hands back the same one.
            try (TableWriter writer = getWriter("pq")) {
                Assert.assertEquals("queued index purges after rollback", 0, writer.getParquetIndexPurgeColumnNameCount());
            }
            Assert.assertEquals("sym name txn after rollback", symNameTxnBefore, symColumnNameTxn());

            // Committed state is still the old one: the index must answer from it,
            // and every committed index file is untouched.
            final StringBuilder mismatches = new StringBuilder();
            checkIndexQueries("after fault", keys, oldExpected, mismatches);
            checkFilesKept("after fault", filesBefore, indexFiles(), mismatches);

            // Simulated restart before the retry: drop every cached reader and writer,
            // then reopen the writer (runs the posting chain's abandoned-entry recovery).
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            try (TableWriter ignore = getWriter("pq")) {
                Assert.assertEquals(nameTxnBefore, partitionNameTxn(engine, "pq", 0));
            }
            checkIndexQueries("after writer reopen", keys, oldExpected, mismatches);

            execute("ALTER TABLE pq RESUME WAL");
            drainWalQueue();
            commit.apply("nat");
            Assert.assertFalse("pq suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
            checkTwins("after resume", keys, mismatches);
            assertInPlace("after resume", nameTxnBefore, symNameTxnBefore, mismatches);
            // The retry runs at the failed attempt's txn, so it re-initialises the files the
            // failure left: every index file is the live version or the superseded one.
            checkOnlyLiveOrCommittedIndexFiles("after resume", filesBefore, mismatches);
            if (mismatches.length() > 0) {
                Assert.fail(mismatches.toString());
            }
        });
    }

    /**
     * A commit spans both parquet days. The first partition task updates its day in place or,
     * for a replace, removes it; then faultFile fails on the other day, either its parquet update
     * (read-only data.parquet) or its index rebuild. Neither partition may be published: name
     * txns, parquet sizes, column name txns and index files stay as committed, and the index
     * answers from the committed snapshot. After RESUME WAL both converge to nat.
     *
     * @param inPlaceBeforeFault in-place partition writes done when the fault fires
     * @param removalBeforeFault replace removals done when the fault fires
     */
    private void assertCrossPartitionFault(
            String indexClause,
            TableCommit commit,
            String faultFile,
            int inPlaceBeforeFault,
            int removalBeforeFault
    ) throws Exception {
        final AtomicBoolean armed = new AtomicBoolean(false);
        final AtomicInteger faults = new AtomicInteger();
        // in-place writes and replace removals when the fault fired
        final long[] countsAtFault = new long[2];
        // the day whose partition task opened its data.parquet first while armed
        final AtomicReference<String> firstDay = new AtomicReference<>();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                // every parquet partition task maps its data.parquet before it decides anything
                if (armed.get() && Utf8s.endsWithAscii(name, TableUtils.PARQUET_PARTITION_NAME)) {
                    firstDay.compareAndSet(null, dayOf(name));
                }
                return super.openRO(name);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                final String first = firstDay.get();
                if (armed.get()
                        && first != null
                        && dayOf(name) != null
                        && !first.equals(dayOf(name))
                        && Utf8s.containsAscii(name, faultFile)
                        && armed.compareAndSet(true, false)) {
                    faults.incrementAndGet();
                    countsAtFault[0] = O3ParquetCounters.getInPlaceCount();
                    countsAtFault[1] = O3ParquetCounters.getReplaceRemovalCount();
                    if (Utf8s.endsWithAscii(name, TableUtils.PARQUET_PARTITION_NAME)) {
                        // Opens, but read-only, so the parquet update's first write fails. The
                        // fd bypasses the fd cache: the job detaches it for the Rust writer.
                        final long fd = super.openRW(name, opts);
                        super.close(fd);
                        return super.openRONoCache(name);
                    }
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            createTwins(indexClause);
            final String[] keys = {"s0", "s1", "s2"};
            final String[] oldExpected = new String[keys.length];
            for (int i = 0; i < keys.length; i++) {
                oldExpected[i] = printed(plainQuery("nat", keys[i]));
            }
            final long[] nameTxnBefore = new long[2];
            final long[] sizeBefore = new long[2];
            final long[] symNameTxnBefore = new long[2];
            final long[] sym2NameTxnBefore = new long[2];
            final ObjList<Map<String, byte[]>> filesBefore = new ObjList<>();
            for (int p = 0; p < 2; p++) {
                nameTxnBefore[p] = partitionNameTxn(engine, "pq", p);
                sizeBefore[p] = parquetFileSize(engine, "pq", p);
                symNameTxnBefore[p] = columnNameTxn("sym", p);
                sym2NameTxnBefore[p] = columnNameTxn("sym2", p);
                filesBefore.add(indexFiles(p));
            }
            final long inPlaceBase = O3ParquetCounters.getInPlaceCount();
            final long removalBase = O3ParquetCounters.getReplaceRemovalCount();

            armed.set(true);
            commit.apply("pq");
            Assert.assertEquals("fault must fire exactly once", 1, faults.get());
            LOG.info().$("first partition task [day=").$(firstDay.get()).I$();
            // the first day's task was done before the other day's failed
            Assert.assertEquals("in-place writes before the fault", inPlaceBeforeFault, countsAtFault[0] - inPlaceBase);
            Assert.assertEquals("replace removals before the fault", removalBeforeFault, countsAtFault[1] - removalBase);
            Assert.assertTrue("pq must suspend", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
            try (TableWriter writer = getWriter("pq")) {
                Assert.assertEquals("queued index purges after rollback", 0, writer.getParquetIndexPurgeColumnNameCount());
            }

            final StringBuilder mismatches = new StringBuilder();
            for (int p = 0; p < 2; p++) {
                final String stage = "after fault, partition " + p;
                Assert.assertEquals(stage + ", name txn", nameTxnBefore[p], partitionNameTxn(engine, "pq", p));
                Assert.assertEquals(stage + ", parquet size", sizeBefore[p], parquetFileSize(engine, "pq", p));
                Assert.assertEquals(stage + ", sym name txn", symNameTxnBefore[p], columnNameTxn("sym", p));
                Assert.assertEquals(stage + ", sym2 name txn", sym2NameTxnBefore[p], columnNameTxn("sym2", p));
                checkFilesKept(stage, filesBefore.getQuick(p), indexFiles(p), mismatches);
            }
            // nat has not applied the commit: it is the committed snapshot
            assertSqlCursors("SELECT * FROM nat", "SELECT * FROM pq");
            checkIndexQueries("after fault", keys, oldExpected, mismatches);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            checkIndexQueries("after reader and writer release", keys, oldExpected, mismatches);

            execute("ALTER TABLE pq RESUME WAL");
            drainWalQueue();
            commit.apply("nat");
            Assert.assertFalse("pq suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
            checkTwins("after resume", keys, mismatches);
            if (removalBeforeFault == 0) {
                assertInPlace("after resume", nameTxnBefore[0], symNameTxnBefore[0], mismatches);
            }
            if (mismatches.length() > 0) {
                Assert.fail(mismatches.toString());
            }
        });
    }

    /**
     * An index cursor on pq opened before an in-place O3 commit into 2024-01-01 keeps the superseded index version in
     * the partition directory, its purge waiting there; without a cursor an adaptive WAL table defers the purge the
     * same way. DETACH links the directory into its copy and must leave every index file but the live version out of
     * it, while the old directory keeps the pinned snapshot whole. ATTACH takes the copy back as it is.
     */
    private void assertDetachCarriesOnlyTheLiveIndexVersion(String indexClause, boolean isWal, boolean isPinned, @Nullable FilesFacade ff) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
        try {
            assertMemoryLeak(ff, () -> {
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    createSimpleTwins(indexClause, isWal, "nat", "pq");
                    releaseSavePointHolds("pq");
                    final String exp0 = printed(plainQuery("nat", "s0"));
                    final long nameTxnBefore = partitionNameTxn(engine, "pq", 0);
                    final File oldDir = partitionDir("pq", DAY, nameTxnBefore);
                    final File attachedDir;
                    final String[] attachedListing;
                    try (
                            RecordCursorFactory factory = select(indexQuery("pq", "s0"));
                            RecordCursor cursor = isPinned ? factory.getCursor(sqlExecutionContext) : null
                    ) {
                        applyO3Wave("pq", 1000, "00");
                        Assert.assertEquals("the O3 commit must update the partition in place", nameTxnBefore, partitionNameTxn(engine, "pq", 0));
                        final long live = symColumnNameTxn();
                        assertSupersededVersionWaits("after O3", oldDir, "sym", live);
                        final Map<String, byte[]> oldFiles = dirFiles(oldDir);

                        execute("ALTER TABLE pq DETACH PARTITION LIST '2024-01-01'");
                        drainWalQueue();
                        assertLiveOnly("after DETACH", detachedCopy("pq", DAY), DETACHED_PARQUET_PARTITION_FILES, "sym", live);
                        if (isPinned) {
                            assertFilesUnchanged("old directory after DETACH", oldFiles, dirFiles(oldDir));
                        }

                        attach("pq", DAY);
                        Assert.assertEquals(live, symColumnNameTxn());
                        attachedDir = partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0));
                        attachedListing = assertLiveOnly("after ATTACH", attachedDir, DETACHED_PARQUET_PARTITION_FILES, "sym", live);
                        if (isPinned) {
                            assertFilesUnchanged("old directory after ATTACH", oldFiles, dirFiles(oldDir));
                            TestUtils.assertEquals(exp0, drain(factory, cursor));
                        }
                    }
                    settle(purgeJob);
                    Assert.assertFalse("the detached directory must go: " + oldDir, oldDir.exists());
                    Assert.assertArrayEquals(attachedListing, listDir(attachedDir));
                    applyO3Wave("nat", 1000, "00");
                    assertTwins("after ATTACH");
                }
            });
        } finally {
            setCurrentMicros(-1);
        }
    }

    /**
     * DROP INDEX and ADD INDEX with a reader pinned leave the dropped index version waiting for its purge in the
     * directory, next to the version ADD INDEX built. The base carried it into the copy of FORCE REINDEX and DETACH.
     */
    private void assertDropAddIndexCarriesOnlyTheLiveIndexVersion(boolean isDetach) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
        try {
            assertMemoryLeak(() -> {
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    createSimpleTwins(IDX_BITMAP, false, "nat", "pq");
                    final String exp0 = printed(plainQuery("nat", "s0"));
                    final long nameTxnBefore = partitionNameTxn(engine, "pq", 0);
                    final File oldDir = partitionDir("pq", DAY, nameTxnBefore);
                    final File copyDir;
                    final String[] copyListing;
                    try (RecordCursorFactory factory = select(indexQuery("pq", "s0")); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        for (String table : new String[]{"nat", "pq"}) {
                            execute("ALTER TABLE " + table + " ALTER COLUMN sym DROP INDEX");
                            execute("ALTER TABLE " + table + " ALTER COLUMN sym ADD INDEX");
                        }
                        Assert.assertEquals(nameTxnBefore, partitionNameTxn(engine, "pq", 0));
                        final long live = symColumnNameTxn();
                        assertSupersededVersionWaits("after DROP INDEX and ADD INDEX", oldDir, "sym", live);
                        final Map<String, byte[]> oldFiles = dirFiles(oldDir);
                        if (isDetach) {
                            execute("ALTER TABLE pq DETACH PARTITION LIST '2024-01-01'");
                            assertLiveOnly("after DETACH", detachedCopy("pq", DAY), DETACHED_PARQUET_PARTITION_FILES, "sym", live);
                            attach("pq", DAY);
                            copyDir = partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0));
                            copyListing = assertLiveOnly("after ATTACH", copyDir, DETACHED_PARQUET_PARTITION_FILES, "sym", live);
                        } else {
                            execute("ALTER TABLE pq FORCE REINDEX PARQUET PARTITION LIST '2024-01-01'");
                            copyDir = partitionDir("pq", DAY, partitionNameTxn(engine, "pq", 0));
                            copyListing = assertLiveOnly("after FORCE REINDEX", copyDir, PARQUET_PARTITION_FILES, "sym", live);
                        }
                        Assert.assertNotEquals(oldDir, copyDir);
                        assertFilesUnchanged("old directory", oldFiles, dirFiles(oldDir));
                        TestUtils.assertEquals(exp0, drain(factory, cursor));
                    }
                    settle(purgeJob);
                    Assert.assertFalse("the replaced directory must go: " + oldDir, oldDir.exists());
                    Assert.assertArrayEquals(copyListing, listDir(copyDir));
                    applyO3Wave("nat", 1000, "00");
                    applyO3Wave("pq", 1000, "00");
                    assertTwins("after the copy");
                }
            });
        } finally {
            setCurrentMicros(-1);
        }
    }

    /**
     * An index cursor on pq opened before an in-place O3 commit into 2024-01-01 keeps the superseded index version in
     * the partition directory, its purge waiting there; without a cursor an adaptive WAL table defers the purge the
     * same way. FORCE REINDEX PARQUET of the day links the directory into a new version and must leave every index
     * file but the live version out of it, while the old directory keeps the pinned snapshot whole.
     */
    private void assertForceReindexCarriesOnlyTheLiveIndexVersion(
            String indexClause,
            String timestampType,
            boolean isWal,
            boolean isPinned,
            @Nullable FilesFacade ff
    ) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
        try {
            assertMemoryLeak(ff, () -> {
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    createSimpleTwins(indexClause, isWal, timestampType, "nat", "pq");
                    releaseSavePointHolds("pq");
                    final String exp0 = printed(plainQuery("nat", "s0"));
                    final long nameTxnBefore = partitionNameTxn(engine, "pq", 0);
                    final File oldDir = partitionDir("pq", DAY, nameTxnBefore);
                    final File newDir;
                    final String[] newListing;
                    try (
                            RecordCursorFactory factory = select(indexQuery("pq", "s0"));
                            RecordCursor cursor = isPinned ? factory.getCursor(sqlExecutionContext) : null
                    ) {
                        applyO3Wave("pq", 1000, "00");
                        Assert.assertEquals("the O3 commit must update the partition in place", nameTxnBefore, partitionNameTxn(engine, "pq", 0));
                        final long live = symColumnNameTxn();
                        assertSupersededVersionWaits("after O3", oldDir, "sym", live);
                        final Map<String, byte[]> oldFiles = dirFiles(oldDir);

                        execute("ALTER TABLE pq FORCE REINDEX PARQUET PARTITION LIST '2024-01-01'");
                        final long newNameTxn = partitionNameTxn(engine, "pq", 0);
                        Assert.assertNotEquals(nameTxnBefore, newNameTxn);
                        Assert.assertEquals(live, symColumnNameTxn());
                        newDir = partitionDir("pq", DAY, newNameTxn);
                        newListing = assertLiveOnly("after FORCE REINDEX", newDir, PARQUET_PARTITION_FILES, "sym", live);
                        if (isPinned) {
                            assertFilesUnchanged("old directory after FORCE REINDEX", oldFiles, dirFiles(oldDir));
                            TestUtils.assertEquals(exp0, drain(factory, cursor));
                        }
                    }
                    settle(purgeJob);
                    Assert.assertFalse("the replaced directory must go: " + oldDir, oldDir.exists());
                    Assert.assertArrayEquals(newListing, listDir(newDir));
                    applyO3Wave("nat", 1000, "00");
                    assertTwins("after FORCE REINDEX");
                }
            });
        } finally {
            setCurrentMicros(-1);
        }
    }

    private void assertNonWalInPlace(String indexClause) throws Exception {
        assertMemoryLeak(() -> {
            createSimpleTwins(indexClause, false, "nat", "pq");
            final long nameTxnBefore = partitionNameTxn(engine, "pq", 0);
            final long symNameTxnBefore = symColumnNameTxn();
            final String exp0 = printed(plainQuery("nat", "s0"));
            final String[] keys = {"s0", "s1", "s2"};
            final StringBuilder mismatches = new StringBuilder();

            try (RecordCursorFactory factory = select(indexQuery("pq", "s0")); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                applyO3Wave("pq", 1000, "00");
                TestUtils.assertEquals(exp0, drain(factory, cursor));
            }
            applyO3Wave("nat", 1000, "00");
            assertInPlace("after pq and nat o3", nameTxnBefore, symNameTxnBefore, mismatches);
            checkTwins("after pq and nat o3", keys, mismatches);

            // Rollback of an uncommitted in-place O3 on a non-WAL writer must leave the
            // committed rows untouched.
            try (TableWriter writer = getWriter("pq")) {
                final TableWriter.Row row = writer.newRow(1_704_085_800_000_000L);
                row.putInt(0, 9999);
                row.putSym(2, "s2");
                row.putLong(3, 9999);
                row.append();
                writer.rollback();
            }
            checkTwins("after rollback", keys, mismatches);

            engine.releaseAllReaders();
            engine.releaseAllWriters();
            checkTwins("after reader and writer release", keys, mismatches);
            if (mismatches.length() > 0) {
                Assert.fail(mismatches.toString());
            }
        });
    }

    private void assertPinned(String indexClause, boolean isMidScan) throws Exception {
        assertPinned(indexClause, isMidScan, ParquetInPlaceIndexPinnedReaderTest::applyO3Insert);
    }

    private void assertPinned(String indexClause, boolean isMidScan, TableCommit commit) throws Exception {
        assertMemoryLeak(() -> {
            createTwins(indexClause);
            final long nameTxnBefore = partitionNameTxn(engine, "pq", 0);
            final long symNameTxnBefore = symColumnNameTxn();
            final String[] keys = {"s0", "s1", "s2"};
            final String[] oldExpected = new String[keys.length];
            for (int i = 0; i < keys.length; i++) {
                oldExpected[i] = printed(plainQuery("nat", keys[i]));
                Assert.assertTrue(oldExpected[i], oldExpected[i].split("\n").length > 4);
            }

            final StringBuilder mismatches = new StringBuilder();
            final RecordCursorFactory[] factories = new RecordCursorFactory[keys.length];
            final RecordCursor[] cursors = new RecordCursor[keys.length];
            final StringSink[] sinks = new StringSink[keys.length];
            try {
                for (int i = 0; i < keys.length; i++) {
                    factories[i] = select(indexQuery("pq", keys[i]));
                    cursors[i] = factories[i].getCursor(sqlExecutionContext);
                    sinks[i] = new StringSink();
                    CursorPrinter.println(factories[i].getMetadata(), sinks[i]);
                    if (isMidScan) {
                        // Read one row: the index cursor over the first (parquet)
                        // partition is open and positioned before the O3 lands.
                        Assert.assertTrue(cursors[i].hasNext());
                        CursorPrinter.println(cursors[i].getRecord(), factories[i].getMetadata(), sinks[i]);
                    }
                }

                // In-place O3 or DROP on the pinned readers' parquet partition,
                // committed while the cursors above stay open and are never reloaded.
                commit.apply("pq");
                Assert.assertFalse("pq suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));

                for (int i = 0; i < keys.length; i++) {
                    readRest(cursors[i], factories[i], sinks[i]);
                    compare("pinned cursor", keys[i], oldExpected[i], sinks[i], mismatches);
                }
                assertInPlace("path", nameTxnBefore, symNameTxnBefore, mismatches);
            } finally {
                for (int i = 0; i < keys.length; i++) {
                    cursors[i] = Misc.free(cursors[i]);
                    factories[i] = Misc.free(factories[i]);
                }
            }

            commit.apply("nat");
            checkTwins("fresh reader", keys, mismatches);
            if (mismatches.length() > 0) {
                Assert.fail(mismatches.toString());
            }
        });
    }

    /**
     * The in-place rebuild of 2024-01-01 fails after writing sym's index under the new column
     * name txn, and RESUME WAL FROM TXN skips the transaction, orphaning those files. Once the
     * table txn moves past it, VACUUM TABLE must purge them and keep every committed index file.
     */
    private void assertSkippedFailedRebuildOrphansPurgedByVacuum(String indexClause, String faultFile) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
        final AtomicBoolean armed = new AtomicBoolean(false);
        final AtomicInteger faults = new AtomicInteger();
        try {
            assertMemoryLeak(failOpenOnce("2024-01-01", faultFile, armed, faults), () -> {
                try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                    createTwins(indexClause);
                    final Map<String, byte[]> filesBefore = indexFiles();
                    final String[] keys = {"s0", "s1", "s2"};
                    final StringBuilder mismatches = new StringBuilder();

                    armed.set(true);
                    applyO3Insert("pq");
                    Assert.assertEquals("fault must fire exactly once", 1, faults.get());
                    Assert.assertTrue("pq must suspend", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
                    final Set<String> orphans = new TreeSet<>(indexFiles().keySet());
                    orphans.removeAll(filesBefore.keySet());
                    Assert.assertFalse("the failed rebuild must leave uncommitted index files", orphans.isEmpty());

                    // Skip the failed transaction. Two commits into the native 2024-01-03 move
                    // the table txn past the failed one without touching 2024-01-01.
                    final long failedSeqTxn = writerSeqTxn("pq") + 1;
                    final String insert = "INSERT INTO %s(id, ts, sym, sym2, v) VALUES (%d, '2024-01-03T23:%d0:00.000000Z', 's1', 't1', %d)";
                    for (int i = 0; i < 2; i++) {
                        execute(String.format(insert, "pq", 5000 + i, i + 1, 5000 + i));
                    }
                    execute("ALTER TABLE pq RESUME WAL FROM TXN " + (failedSeqTxn + 1));
                    drainWalQueue();
                    Assert.assertFalse("pq suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
                    for (int i = 0; i < 2; i++) {
                        execute(String.format(insert, "nat", 5000 + i, i + 1, 5000 + i));
                    }
                    drainWalQueue();
                    checkTwins("after skip", keys, mismatches);
                    final Map<String, byte[]> afterSkip = indexFiles();
                    checkFilesKept("after skip", filesBefore, afterSkip, mismatches);
                    for (String orphan : orphans) {
                        if (!afterSkip.containsKey(orphan)) {
                            mismatches.append("\n[after skip] nothing should have removed the orphan ").append(orphan);
                        }
                    }

                    engine.releaseAllReaders();
                    // Under ADAPTIVE the save points still name the orphans' column versions.
                    releaseSavePointHolds("pq");
                    execute("VACUUM TABLE pq");
                    runPurgeJob(purgeJob);
                    final Map<String, byte[]> afterVacuum = indexFiles();
                    for (String orphan : orphans) {
                        if (afterVacuum.containsKey(orphan)) {
                            mismatches.append("\n[after vacuum] orphan not purged: ").append(orphan)
                                    .append(", files=").append(afterVacuum.keySet());
                        }
                    }
                    checkFilesKept("after vacuum", filesBefore, afterVacuum, mismatches);
                    checkTwins("after vacuum", keys, mismatches);
                    if (mismatches.length() > 0) {
                        Assert.fail(mismatches.toString());
                    }
                }
            });
        } finally {
            setCurrentMicros(-1);
        }
    }

    private void assertSupersededFilesPurged(String indexClause) throws Exception {
        // Retry the column purge on every run; the test clock drives the retry schedule.
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
        try {
            assertMemoryLeak(() -> assertSupersededFilesPurged0(indexClause));
        } finally {
            setCurrentMicros(-1);
        }
    }

    private void assertSupersededFilesPurged0(String indexClause) throws Exception {
        try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
            createTwins(indexClause);
            final long nameTxnBefore = partitionNameTxn(engine, "pq", 0);
            final long symNameTxnBefore = symColumnNameTxn();
            final Map<String, byte[]> filesBefore = indexFiles();
            Assert.assertFalse("no index files found", filesBefore.isEmpty());
            if (indexClause.contains("INCLUDE")) {
                Assert.assertTrue("covering sidecars missing: " + filesBefore.keySet(),
                        filesBefore.keySet().stream().anyMatch(n -> n.startsWith("sym.pci"))
                                && filesBefore.keySet().stream().anyMatch(n -> n.startsWith("sym.pc0")));
            }
            final String key = "s0";
            final String oldExpected = printed(plainQuery("nat", key));
            final StringBuilder mismatches = new StringBuilder();

            final StringSink sink = new StringSink();
            try (
                    RecordCursorFactory factory = select(indexQuery("pq", key));
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                CursorPrinter.println(factory.getMetadata(), sink);

                execute(String.format(O3_INSERT, "pq"));
                drainWalQueue();
                Assert.assertFalse("pq suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
                assertInPlace("path", nameTxnBefore, symNameTxnBefore, mismatches);
                checkFilesKept("after O3, reader pinned", filesBefore, indexFiles(), mismatches);

                // The cursor still pins the old txn: the purge must keep the old files.
                runPurgeJob(purgeJob);
                checkFilesKept("after purge, reader pinned", filesBefore, indexFiles(), mismatches);

                readRest(cursor, factory, sink);
                compare("pinned cursor", key, oldExpected, sink, mismatches);
            }

            // The reader is released: the purge removes every superseded file and
            // keeps the new version. Under ADAPTIVE the save points must move past
            // the superseded versions first.
            releaseSavePointHolds("pq");
            runPurgeJob(purgeJob);
            final Map<String, byte[]> filesAfter = indexFiles();
            for (String name : filesBefore.keySet()) {
                if (filesAfter.containsKey(name)) {
                    mismatches.append("\n[after purge, reader closed] superseded file not purged: ").append(name)
                            .append(", files=").append(filesAfter.keySet());
                }
            }
            if (filesAfter.isEmpty()) {
                mismatches.append("\n[after purge, reader closed] the new index files are missing");
            }

            execute(String.format(O3_INSERT, "nat"));
            drainWalQueue();
            checkTwins("fresh reader", new String[]{"s0", "s1", "s2"}, mismatches);
            if (mismatches.length() > 0) {
                Assert.fail(mismatches.toString());
            }
        }
    }

    private void assertTwoVersionsPinnedThenConvertNative(String indexClause) throws Exception {
        // Retry the column purge on every run; the test clock drives the retry schedule.
        node1.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks());
        try {
            assertMemoryLeak(() -> assertTwoVersionsPinnedThenConvertNative0(indexClause));
        } finally {
            setCurrentMicros(-1);
        }
    }

    private void assertTwoVersionsPinnedThenConvertNative0(String indexClause) throws Exception {
        try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
            createSimpleTwins(indexClause, true, "nat", "pq");
            final long nameTxnBefore = partitionNameTxn(engine, "pq", 0);
            final String[] keys = {"s0", "s1", "s2"};
            final StringBuilder mismatches = new StringBuilder();

            final String exp0 = printed(plainQuery("nat", "s0"));
            final RecordCursorFactory factory0 = select(indexQuery("pq", "s0"));
            final RecordCursor cursor0 = factory0.getCursor(sqlExecutionContext);
            applyO3Wave("pq", 1000, "00");
            applyO3Wave("nat", 1000, "00");
            final long symNameTxnAfterFirst = symColumnNameTxn();

            final String exp1 = printed(plainQuery("nat", "s0"));
            final RecordCursorFactory factory1 = select(indexQuery("pq", "s0"));
            final RecordCursor cursor1 = factory1.getCursor(sqlExecutionContext);
            applyO3Wave("pq", 2000, "03");
            applyO3Wave("nat", 2000, "03");
            final long symNameTxnAfterSecond = symColumnNameTxn();
            Assert.assertNotEquals(symNameTxnAfterFirst, symNameTxnAfterSecond);
            Assert.assertEquals(nameTxnBefore, partitionNameTxn(engine, "pq", 0));

            // The purge runs while both older versions are pinned, one per open cursor.
            runPurgeJob(purgeJob);
            TestUtils.assertEquals(exp1, drain(factory1, cursor1));
            TestUtils.assertEquals(exp0, drain(factory0, cursor0));
            cursor1.close();
            factory1.close();
            cursor0.close();
            factory0.close();

            runPurgeJob(purgeJob);
            checkTwins("after purge, both readers closed", keys, mismatches);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            checkTwins("after reader and writer release", keys, mismatches);

            execute("ALTER TABLE pq CONVERT PARTITION TO NATIVE WHERE ts < '2024-01-02'");
            drainWalQueue();
            checkTwins("after convert to native", keys, mismatches);

            // A round trip back to parquet, then another in-place O3.
            execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            drainWalQueue();
            applyO3Wave("pq", 3000, "05");
            applyO3Wave("nat", 3000, "05");
            checkTwins("after the round trip and a further in-place O3", keys, mismatches);
            if (mismatches.length() > 0) {
                Assert.fail(mismatches.toString());
            }
        }
    }

    private static void checkFilesKept(String stage, Map<String, byte[]> before, Map<String, byte[]> after, StringBuilder mismatches) {
        for (Map.Entry<String, byte[]> e : before.entrySet()) {
            final byte[] now = after.get(e.getKey());
            if (now == null) {
                mismatches.append("\n[").append(stage).append("] committed index file removed: ").append(e.getKey());
            } else if (!Arrays.equals(e.getValue(), now)) {
                mismatches.append("\n[").append(stage).append("] committed index file changed: ").append(e.getKey());
            }
        }
    }

    private static void checkIndexQueries(String stage, String[] keys, String[] expected, StringBuilder mismatches) throws Exception {
        for (int i = 0; i < keys.length; i++) {
            final String sql = indexQuery("pq", keys[i]);
            try {
                compare(stage, keys[i], expected[i], printed(sql), mismatches);
            } catch (Throwable th) {
                mismatches.append("\n[").append(stage).append(", key=").append(keys[i]).append("] threw: ").append(th);
            }
        }
    }

    // Every index file of pq's first partition is either one of the committed files from
    // before the commit (the superseded version, until the purge) or a file of the column's
    // live version from _cv: no file of a failed attempt is left behind.
    private static void checkOnlyLiveOrCommittedIndexFiles(String stage, Map<String, byte[]> committed, StringBuilder mismatches) throws IOException {
        final long liveSym = columnNameTxn("sym", 0);
        final long liveSym2 = columnNameTxn("sym2", 0);
        for (String name : indexFiles().keySet()) {
            if (committed.containsKey(name)) {
                continue;
            }
            // <column>.<k|v|pk|pv|pci|pcN>.<column name txn>[...]
            final String[] parts = name.split("\\.");
            final long live = "sym".equals(parts[0]) ? liveSym : liveSym2;
            if (parts.length < 3 || !parts[2].equals(Long.toString(live))) {
                mismatches.append("\n[").append(stage).append("] index file is neither committed nor of the live version ")
                        .append(live).append(": ").append(name);
            }
        }
    }

    private static void checkTwins(String stage, String[] keys, StringBuilder mismatches) throws Exception {
        assertSqlCursors("SELECT * FROM nat", "SELECT * FROM pq");
        final String[] expected = new String[keys.length];
        for (int i = 0; i < keys.length; i++) {
            expected[i] = printed(plainQuery("nat", keys[i]));
            // The native twin's index must agree with its own full scan.
            TestUtils.assertEquals(expected[i], printed(indexQuery("nat", keys[i])));
        }
        checkIndexQueries(stage, keys, expected, mismatches);
    }

    private static void compare(String stage, String key, CharSequence expected, CharSequence actual, StringBuilder mismatches) {
        if (!Chars.equals(expected, actual)) {
            mismatches.append("\n[").append(stage).append(", key=").append(key).append("]\nexpected:\n").append(expected)
                    .append("actual:\n").append(actual);
        }
    }

    /**
     * nat and pq with a single indexed sym column (no sym2), 36 rows at 2h spacing
     * starting 2024-01-01, as WAL or non-WAL tables. pq's first two days are parquet.
     */
    private void createSimpleTwins(String indexClause, boolean isWal, String nat, String pq) throws Exception {
        createSimpleTwins(indexClause, isWal, "TIMESTAMP", nat, pq);
    }

    // createSimpleTwins() with the given designated timestamp type
    private void createSimpleTwins(String indexClause, boolean isWal, String timestampType, String nat, String pq) throws Exception {
        final String ddl = "CREATE TABLE %s (id INT, ts " + timestampType + ", sym SYMBOL %s, v LONG) TIMESTAMP(ts) PARTITION BY DAY" + (isWal ? " WAL" : " BYPASS WAL");
        execute(String.format(ddl, nat, indexClause));
        execute(String.format(ddl, pq, indexClause));
        final String ins = "INSERT INTO %s SELECT x::INT, '2024-01-01'::TIMESTAMP + (x - 1) * 7_200_000_000L, 's' || (x %% 3), x FROM long_sequence(36)";
        execute(String.format(ins, nat));
        execute(String.format(ins, pq));
        drainWalQueue();
        execute("ALTER TABLE " + pq + " CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
        drainWalQueue();
    }

    /**
     * nat (native) and pq with 3 daily partitions of 12 rows at 2h spacing, two
     * indexed symbols (sym before sym2). pq's first two days are parquet.
     */
    private void createTwins(String indexClause) throws Exception {
        final String ddl = "CREATE TABLE %s (id INT, ts TIMESTAMP, sym SYMBOL %s, sym2 SYMBOL %s, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL";
        final String idx2 = indexClause.replace("INCLUDE (v)", "INCLUDE (id)");
        execute(String.format(ddl, "nat", indexClause, idx2));
        execute(String.format(ddl, "pq", indexClause, idx2));
        execute("""
                INSERT INTO nat
                SELECT
                    x::INT,
                    '2024-01-01'::TIMESTAMP + (x - 1) * 7_200_000_000L,
                    's' || (x % 3),
                    't' || (x % 2),
                    x
                FROM long_sequence(36)
                """);
        drainWalQueue();
        execute("INSERT INTO pq SELECT * FROM nat");
        drainWalQueue();
        execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
        drainWalQueue();
        assertQuery("SELECT count() FROM table_partitions('pq') WHERE isParquet")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\n2\n");
        // The index, not a full scan, must serve the queries under test.
        final String planFragment = indexClause.contains("INCLUDE") ? "CoveringIndex on: sym" : "Index forward scan on: sym";
        assertQuery(indexQuery("pq", "s1")).noLeakCheck().assertsPlanContaining(planFragment);
        assertQuery(plainQuery("pq", "s1")).noLeakCheck().assertsPlanNotContaining("Index");
    }

    @FunctionalInterface
    private interface TableCommit {
        void apply(String table) throws Exception;
    }

    // Faults in the copies of a partition directory, each off while null or false
    private static class PartitionCopyFacade extends TestFilesFacadeImpl {
        private final AtomicInteger faultCount = new AtomicInteger();
        // copy() to a path ending with it fails once
        private volatile String failCopyTo;
        // openRW() of a path containing it fails once
        private volatile String failOpenRw;
        // removeQuiet() removes every file whose path ends with it, yet reports a failure
        private volatile String goneDespiteFailurePathSuffix;
        // hardLinkDirRecursive() fails with an error that a copy takes over from, as across devices
        private volatile boolean isCrossDevice;
        // removeQuiet() keeps every file whose path ends with it
        private volatile String keptPathSuffix;

        @Override
        public int copy(LPSZ from, LPSZ to) {
            final String term = failCopyTo;
            if (term != null && Utf8s.endsWithAscii(to, term)) {
                failCopyTo = null;
                faultCount.incrementAndGet();
                return -1;
            }
            return super.copy(from, to);
        }

        @Override
        public int hardLinkDirRecursive(Path src, Path dst, int dirMode) {
            return isCrossDevice ? -1 : super.hardLinkDirRecursive(src, dst, dirMode);
        }

        @Override
        public boolean isCrossDeviceCopyError(int errno) {
            return isCrossDevice || super.isCrossDeviceCopyError(errno);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final String term = failOpenRw;
            if (term != null && Utf8s.containsAscii(name, term)) {
                failOpenRw = null;
                faultCount.incrementAndGet();
                return -1;
            }
            return super.openRW(name, opts);
        }

        @Override
        public boolean removeQuiet(LPSZ name) {
            final String keptSuffix = keptPathSuffix;
            if (keptSuffix != null && Utf8s.endsWithAscii(name, keptSuffix)) {
                faultCount.incrementAndGet();
                return false;
            }
            final String goneSuffix = goneDespiteFailurePathSuffix;
            if (goneSuffix != null && Utf8s.endsWithAscii(name, goneSuffix)) {
                faultCount.incrementAndGet();
                super.removeQuiet(name);
                return false;
            }
            return super.removeQuiet(name);
        }
    }
}
