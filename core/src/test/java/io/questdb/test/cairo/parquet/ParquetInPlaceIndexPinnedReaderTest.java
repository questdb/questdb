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
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

/**
 * An in-place parquet O3 update (partition name txn unchanged) rebuilds the
 * partition's symbol indexes (bitmap .k/.v, posting .pk/.pv, covering .pci/.pc)
 * for the new row layout. The rebuild must write new files under a new column
 * name txn, published through _cv with _txn, and never touch the committed
 * files. These tests pin what that gives:
 * <ul>
 * <li>a reader at the old txn keeps returning the old snapshot's rows through
 * the index, and a fresh reader sees the new rows;</li>
 * <li>a failure between the index rebuild and the _txn commit (the second
 * indexed column, or a covering sidecar of the first) leaves the committed
 * index answering from the committed layout until the retry converges;</li>
 * <li>the superseded files stay while a reader is pinned and are purged once it
 * closes.</li>
 * </ul>
 * Each test checks the rows before the path, so a failure names the wrong rows
 * first.
 */
public class ParquetInPlaceIndexPinnedReaderTest extends AbstractCairoTest {
    private static final String IDX_BITMAP = "INDEX";
    private static final String IDX_COVERING = "INDEX TYPE POSTING INCLUDE (v)";
    private static final String IDX_POSTING = "INDEX TYPE POSTING";
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
        assertCrashWindow(IDX_BITMAP, "sym2.k");
    }

    @Test
    public void testCrashWindowCovering() throws Exception {
        assertCrashWindow(IDX_COVERING, "sym2.pk");
    }

    @Test
    public void testCrashWindowCoveringSidecarData() throws Exception {
        // The covering value sidecar of the first column: the failure lands in
        // finishO3Commit's covering reseal, after both columns' .pk/.pv were rebuilt.
        assertCrashWindow(IDX_COVERING, "sym.pc0");
    }

    @Test
    public void testCrashWindowCoveringSidecarInfo() throws Exception {
        assertCrashWindow(IDX_COVERING, "sym.pci");
    }

    @Test
    public void testCrashWindowPosting() throws Exception {
        assertCrashWindow(IDX_POSTING, "sym2.pk");
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

    private static void applyO3Insert(String table) throws Exception {
        execute(String.format(O3_INSERT, table));
        drainWalQueue();
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

    // Prints factory's header, then every remaining row of cursor: a cursor opened
    // before an O3 commit and drained after it reads the old snapshot through its
    // pinned txn.
    private static String drain(RecordCursorFactory factory, RecordCursor cursor) {
        final StringSink sink = new StringSink();
        CursorPrinter.println(factory.getMetadata(), sink);
        readRest(cursor, factory, sink);
        return sink.toString();
    }

    private static String indexQuery(String table, String key) {
        return "SELECT ts, sym, v FROM " + table + " WHERE sym = '" + key + "'";
    }

    // sym.* and sym2.* files (the index files) of pq's first partition, by name, with
    // their content.
    private static Map<String, byte[]> indexFiles() throws IOException {
        final TableToken token = engine.verifyTableName("pq");
        final Map<String, byte[]> files = new TreeMap<>();
        try (TableReader reader = engine.getReader(token); Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token);
            TableUtils.setPathForNativePartition(
                    path,
                    ColumnType.TIMESTAMP,
                    PartitionBy.DAY,
                    reader.getTxFile().getPartitionTimestampByIndex(0),
                    reader.getTxFile().getPartitionNameTxn(0)
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
        try (TableReader reader = engine.getReader("pq")) {
            final long partitionTimestamp = reader.getTxFile().getPartitionTimestampByIndex(0);
            return reader.getColumnVersionReader().getColumnNameTxn(partitionTimestamp, reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("sym")));
        }
    }

    // Two out-of-order rows into 2024-01-01: one at hour:30 (sym s1), one at the next
    // hour mod 10 :45 (sym s0). baseId seeds both rows' id and v.
    private void applyO3Wave(String table, int baseId, String hour) throws Exception {
        execute("INSERT INTO " + table + "(id, ts, sym, v) VALUES (" + baseId + ", '2024-01-01T" + hour + ":30:00.000000Z', 's1', " + baseId + "),"
                + "(" + (baseId + 1) + ", '2024-01-01T0" + (Integer.parseInt(hour) % 10) + ":45:00.000000Z', 's0', " + (baseId + 1) + ")");
        drainWalQueue();
    }

    private void assertCrashWindow(String indexClause, String faultFile) throws Exception {
        // sym is rebuilt first, then an open of faultFile fails (sym2's index, or a
        // covering sidecar of sym in the reseal that follows the O3 workers): the
        // failure lands after sym's index was rebuilt for the new row layout and
        // before _txn commits.
        final AtomicBoolean armed = new AtomicBoolean(false);
        final AtomicInteger faults = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (armed.get()
                        && Utf8s.containsAscii(name, "2024-01-01")
                        && Utf8s.containsAscii(name, faultFile)
                        && armed.compareAndSet(true, false)) {
                    faults.incrementAndGet();
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
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
            execute(String.format(O3_INSERT, "pq"));
            drainWalQueue();
            Assert.assertEquals("fault must fire exactly once", 1, faults.get());
            Assert.assertTrue("pq must suspend", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
            Assert.assertEquals(nameTxnBefore, partitionNameTxn("pq", 0));

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
            execute(String.format(O3_INSERT, "nat"));
            drainWalQueue();
            Assert.assertFalse("pq suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
            checkTwins("after resume", keys, mismatches);
            assertInPlace("after resume", nameTxnBefore, symNameTxnBefore, mismatches);
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
                final TableWriter.Row row = writer.newRow(1704085800000000L);
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

    private void assertPinned(String indexClause, boolean midScan) throws Exception {
        assertPinned(indexClause, midScan, ParquetInPlaceIndexPinnedReaderTest::applyO3Insert);
    }

    private void assertPinned(String indexClause, boolean midScan, TableCommit commit) throws Exception {
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
                    if (midScan) {
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
            // keeps the new version.
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
    private void createSimpleTwins(String indexClause, boolean wal, String nat, String pq) throws Exception {
        final String ddl = "CREATE TABLE %s (id INT, ts TIMESTAMP, sym SYMBOL %s, v LONG) TIMESTAMP(ts) PARTITION BY DAY" + (wal ? " WAL" : " BYPASS WAL");
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
