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
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.O3ParquetCounters;
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
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
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

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

/**
 * An in-place parquet O3 update rebuilds the partition's symbol indexes (bitmap, posting,
 * covering) under a new column name txn, published through _cv with _txn, and never
 * touches the committed files. These tests pin that:
 * <ul>
 * <li>a pinned reader keeps its snapshot through the index; a fresh one sees new rows;</li>
 * <li>a failure between the rebuild and the _txn commit leaves the committed index
 * answering until the retry converges;</li>
 * <li>superseded files stay while a reader is pinned and go once it closes;</li>
 * <li>a two-partition commit failing in the second leaves both as committed;</li>
 * <li>files of a failed rebuild that RESUME WAL skips stay until VACUUM TABLE.</li>
 * </ul>
 */
public class ParquetInPlaceIndexPinnedReaderTest extends AbstractCairoTest {
    private static final String IDX_BITMAP = "INDEX";
    private static final String IDX_COVERING = "INDEX TYPE POSTING INCLUDE (v)";
    private static final String IDX_POSTING = "INDEX TYPE POSTING";
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
    public void testInPlaceCommitsReleasePurgeEntries() throws Exception {
        assertMemoryLeak(() -> {
            createSimpleTwins(IDX_BITMAP, false, "nat", "pq");
            final long nameTxnBefore = partitionNameTxn("pq", 0);
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

    private static void assertInPlace(String stage, long nameTxnBefore, long symNameTxnBefore, StringBuilder mismatches) {
        final long nameTxnAfter = partitionNameTxn("pq", 0);
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

    // A pq partition's parquet file size as committed in _txn.
    private static long parquetFileSize(int partitionIndex) {
        try (TableReader reader = engine.getReader("pq")) {
            return reader.getTxFile().getPartitionParquetFileSize(partitionIndex);
        }
    }

    private static long partitionNameTxn(String table, int partitionIndex) {
        try (TableReader reader = engine.getReader(table)) {
            return reader.getTxFile().getPartitionNameTxn(partitionIndex);
        }
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

    // sym's column name txn in pq's first partition, from a fresh reader.
    private static long symColumnNameTxn() {
        return columnNameTxn("sym", 0);
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
            final long nameTxnBefore = partitionNameTxn("pq", 0);
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
            Assert.assertEquals(nameTxnBefore, partitionNameTxn("pq", 0));
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
                Assert.assertEquals(nameTxnBefore, partitionNameTxn("pq", 0));
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
     * A commit spans both parquet days. The partition task that runs first updates its
     * partition in place (parquet footer appended, _pm header patched, indexes rebuilt under
     * the new column name txn) or, for a replace, removes it. Which day runs first depends on
     * the commit: the writer runs a partition task inline when it cannot queue it. Then a file
     * of the other day fails (faultFile): data.parquet opens read-only, so its parquet update
     * fails, and an index file fails to open, so its index rebuild fails after its parquet
     * update. Nothing of either partition may be published: the committed name txns, parquet
     * sizes, column name txns and index files stay, and the index answers from the committed
     * snapshot. After RESUME WAL both converge to nat.
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
                nameTxnBefore[p] = partitionNameTxn("pq", p);
                sizeBefore[p] = parquetFileSize(p);
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
                Assert.assertEquals(stage + ", name txn", nameTxnBefore[p], partitionNameTxn("pq", p));
                Assert.assertEquals(stage + ", parquet size", sizeBefore[p], parquetFileSize(p));
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

    private void assertNonWalInPlace(String indexClause) throws Exception {
        assertMemoryLeak(() -> {
            createSimpleTwins(indexClause, false, "nat", "pq");
            final long nameTxnBefore = partitionNameTxn("pq", 0);
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
            final long nameTxnBefore = partitionNameTxn("pq", 0);
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
     * The in-place rebuild of 2024-01-01 fails after sym's index was written under the new
     * column name txn. RESUME WAL FROM TXN skips the failed transaction, so nothing ever
     * names those files: they stay in the live partition directory as orphans. Once the
     * table txn has moved past the failed one, VACUUM TABLE must purge them and keep every
     * committed index file.
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
            final long nameTxnBefore = partitionNameTxn("pq", 0);
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
            final long nameTxnBefore = partitionNameTxn("pq", 0);
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
            Assert.assertEquals(nameTxnBefore, partitionNameTxn("pq", 0));

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
        final String ddl = "CREATE TABLE %s (id INT, ts TIMESTAMP, sym SYMBOL %s, v LONG) TIMESTAMP(ts) PARTITION BY DAY" + (isWal ? " WAL" : " BYPASS WAL");
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
}
