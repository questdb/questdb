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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.WriterInvariantChecker;
import io.questdb.cairo.idx.BitmapIndexUtils;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * How the writer opens, positions and closes partition column mappings around merge-append and composite
 * partitions. Nothing may leave columns[] open on files the O3 executor grows through its own fds, rebind an index
 * writer to a stale partition, or truncate committed rows on close. The debug writer invariant check
 * (debug.cairo.writer.invariant.check.enabled, on in tests) fails a test the moment such a state arises.
 */
public class CompositeColumnMappingTest extends AbstractCairoTest {

    @Test
    public void testAlterColumnTypeToIndexedSymbolAfterLastPartitionReturnedToNative() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();
            execute("CREATE TABLE t (s STRING, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT 's' || (x % 10), x, timestamp_sequence('2024-01-01', 1_000_000L) FROM long_sequence(5000)");
            drainWalQueue();
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE t CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();
            assertNotSuspended("t");

            execute("ALTER TABLE t ALTER COLUMN s TYPE SYMBOL INDEX");
            drainWalQueue();
            assertNotSuspended("t");
            assertQuery("SELECT count() c FROM t WHERE s = 's1'").noRandomAccess().expectSize().returns("c\n500\n");
        });
    }

    @Test
    public void testAlterColumnTypeToIndexedSymbolOnParquetLastPartition() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();
            execute("CREATE TABLE t (s STRING, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT 's' || (x % 10), x, timestamp_sequence('2024-01-01', 1_000_000L) FROM long_sequence(5000)");
            drainWalQueue();
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            assertNotSuspended("t");

            execute("ALTER TABLE t ALTER COLUMN s TYPE SYMBOL INDEX");
            drainWalQueue();
            assertNotSuspended("t");
            assertQuery("SELECT count() c FROM t WHERE s = 's1'").noRandomAccess().expectSize().returns("c\n500\n");
        });
    }

    /**
     * Merge-append switched off under a pooled writer (test-only: the property is not reloadable), then block apply.
     */
    @Test
    public void testBlockApplyAfterMergeAppendDisabledUnderPooledWriter() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();
            execute("CREATE TABLE x (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x SELECT x, timestamp_sequence('2022-02-24', 1_000_000L) FROM long_sequence(100)");
            drainWalQueue();
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
            for (int i = 1; i < 7; i++) {
                execute("INSERT INTO x SELECT x, timestamp_sequence('2022-02-24T0" + i + "', 1_000_000L) FROM long_sequence(100)");
            }
            drainWalQueue();
            assertNotSuspended("x");
            assertQuery("SELECT count() c FROM x").noRandomAccess().expectSize().returns("c\n700\n");
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertColumnFilesCoverPhysicalRows("x", "");
            assertQuery("SELECT count() c FROM x").noRandomAccess().expectSize().returns("c\n700\n");
        });
    }

    @Test
    public void testConvertLastPartitionToNativeKeepsWriterColumnsClosed() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();
            execute("CREATE TABLE t (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT x, timestamp_sequence('2024-01-01', 1_000_000L) FROM long_sequence(10_000)");
            drainWalQueue();
            final long violationsBefore = WriterInvariantChecker.getViolationCount();

            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE t CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();
            assertNotSuspended("t");

            assertNoWriterInvariantViolations(violationsBefore);
        });
    }

    @Test
    public void testFailedMergeAppendFirstActionKeepsIndexFiles() throws Exception {
        checkFailedMergeAppendThenWriterClose(1, false);
    }

    /**
     * A merge-append commit that fails after its first action wrote rows and index entries: the writer's close must
     * not truncate the BITMAP .k/.v back to their pre-commit sizes.
     */
    @Test
    public void testFailedMergeAppendSecondActionKeepsIndexFiles() throws Exception {
        checkFailedMergeAppendThenWriterClose(2, true);
    }

    /**
     * Merge-append commits that add more BITMAP keys than the writer's own index writer cached when bound.
     */
    @Test
    public void testMergeAppendManyNewKeysKeepsBitmapIndexFiles() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();
            execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL INDEX, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01', 60_000_000L), 'k' || (x % 10), x FROM long_sequence(1000)");
            drainWalQueue();
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01T00:00:30', 60_000_000L), 'n' || x, x FROM long_sequence(1000)");
            drainWalQueue();
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01T00:00:40', 60_000_000L), 'm' || x, x FROM long_sequence(1000)");
            drainWalQueue();
            assertNotSuspended("t");
            Assert.assertTrue("fixture: the last partition must be composite", isComposite("t", "2024-01-01"));
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertColumnFilesCoverPhysicalRows("t", "");
            assertQuery("SELECT count() c FROM t WHERE s = 'n999'").noRandomAccess().expectSize().returns("c\n1\n");
            assertQuery("SELECT count() c FROM t WHERE s = 'm999'").noRandomAccess().expectSize().returns("c\n1\n");
            assertQuery("SELECT count() c FROM t WHERE s = 'k3'").noRandomAccess().expectSize().returns("c\n100\n");
        });
    }

    /**
     * In-order rows into a non-WAL table whose last partition was left composite by its WAL days.
     */
    @Test
    public void testNonWalInOrderRowAfterCompositeLastPartition() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();
            execute("CREATE TABLE t AS (SELECT x::INT i, timestamp_sequence('2020-01-01', 15 * 1_000_000L) ts" +
                    " FROM long_sequence(4800)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            execute("INSERT INTO t SELECT x::INT + 70_000 i, timestamp_sequence('2020-01-01T04:00:07', 5 * 1_000_000L) ts" +
                    " FROM long_sequence(200)");
            drainWalQueue();
            Assert.assertTrue("fixture: the only partition must be composite", isComposite("t", "2020-01-01"));

            execute("ALTER TABLE t SET TYPE BYPASS WAL");
            engine.releaseInactive();
            engine.load();
            Assert.assertFalse(engine.verifyTableName("t").isWal());

            execute("INSERT INTO t VALUES (1, '2020-01-01T23:59:50')");
            execute("INSERT INTO t SELECT x::INT + 90_000, timestamp_sequence('2020-01-01T23:00:00', 100_000L) FROM long_sequence(5000)");

            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertColumnFilesCoverPhysicalRows("t", "");
            assertQuery("SELECT i, ts FROM t LIMIT -1")
                    .timestamp("ts")
                    .expectSize()
                    .returns("i\tts\n1\t2020-01-01T23:59:50.000000Z\n");
            assertQuery("SELECT count() c FROM t")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n10001\n");
        });
    }

    @Test
    public void testPersistedLagKeepsWriterColumnsClosedOnMergeAppendTable() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "8K");
        node1.setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 0);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT x, timestamp_sequence('2024-01-01', 1_000_000L) FROM long_sequence(10_000)");
            execute("INSERT INTO t SELECT x, timestamp_sequence('2024-01-02', 1_000_000L) FROM long_sequence(10_000)");
            drainWalQueue();
            execute("INSERT INTO t VALUES (1, '2024-01-01T05:00:00.5')");
            execute("INSERT INTO t VALUES (2, '2024-01-01T06:00:00.5')");
            final TableToken token = engine.verifyTableName("t");
            engine.getTableSequencerAPI().getTxnTracker(token).getMemPressureControl().setMaxBlockRowCount(1);
            try (var job = createWalApplyJob(engine)) {
                job.run();
            }
            final long lag;
            try (TableReader r = engine.getReader(token)) {
                lag = r.getTxFile().getLagRowCount();
            }
            Assert.assertTrue("fixture: no LAG parked before enabling merge-append", lag > 0);

            engine.releaseAllWriters();
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            final long violationsBefore = WriterInvariantChecker.getViolationCount();
            drainWalQueue();
            assertNotSuspended("t");
            assertQuery("SELECT count() c FROM t").noRandomAccess().expectSize().returns("c\n20002\n");
            assertNoWriterInvariantViolations(violationsBefore);
        });
    }

    /**
     * A pooled reader agrees with a fresh one after MAKE-PLAIN clamps the top of a column it holds unmapped.
     */
    @Test
    public void testPooledReaderAfterMakePlainClampOfEmptyColumn() throws Exception {
        assertMemoryLeak(() -> checkPooledReaderAcrossMakePlainClamp(false));
    }

    /**
     * A pooled reader agrees with a fresh one after MAKE-PLAIN clamps the top of a column it holds mapped.
     */
    @Test
    public void testPooledReaderAfterMakePlainClampOfMappedColumn() throws Exception {
        assertMemoryLeak(() -> checkPooledReaderAcrossMakePlainClamp(true));
    }

    @Test
    public void testRenameBitmapIndexedColumnKeepsIndexAfterConvertRoundTrip() throws Exception {
        assertMemoryLeak(() -> checkRenameIndexedColumnThenMergeIntoFormerLastPartition(true, "SYMBOL INDEX"));
    }

    @Test
    public void testRenameBitmapIndexedColumnKeepsIndexWithoutConvertRoundTrip() throws Exception {
        assertMemoryLeak(() -> checkRenameIndexedColumnThenMergeIntoFormerLastPartition(false, "SYMBOL INDEX"));
    }

    @Test
    public void testRenamePostingIndexedColumnKeepsIndexAfterConvertRoundTrip() throws Exception {
        assertMemoryLeak(() -> checkRenameIndexedColumnThenMergeIntoFormerLastPartition(true, "SYMBOL INDEX TYPE POSTING"));
    }

    /**
     * Split removal lowers a plain parent's live size; a later merge-append must not overwrite the rows above it
     * that a reader pinned before the removal still maps.
     */
    @Test
    public void testSplitRemovalThenMergeKeepsPinnedReaderRows() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();
            enableMoveTail();
            setCurrentMicros(MicrosTimestampDriver.floor("2024-01-10T00:00:00.000000Z"));
            execute("CREATE TABLE x AS (SELECT x::INT i, timestamp_sequence('2024-01-01', 1_000_000L) ts" +
                    " FROM long_sequence(20_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            for (int k = 0; k < 3; k++) {
                execute("INSERT INTO x SELECT x::INT + 500_000 i, timestamp_sequence('2024-01-01T05:00:00', 1_000_000L) ts" +
                        " FROM long_sequence(200)");
                drainWalQueue();
            }
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, Long.MAX_VALUE / 8);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 2);
            for (int k = 0; k < 6; k++) {
                execute("INSERT INTO x SELECT x::INT + 800_000 i, timestamp_sequence('2024-03-0" + (k + 1) + "', 60_000_000L) ts" +
                        " FROM long_sequence(2)");
                drainWalQueue();
            }
            Assert.assertEquals("fixture: MOVE-TAIL must leave front + one sibling", 2,
                    scalar("SELECT count() FROM table_partitions('x') WHERE name LIKE '2024-01-01%'"));
            Assert.assertFalse("fixture: MAKE-PLAIN must have made the front plain", isComposite("x", "2024-01-01"));
            final long siblingMinTs = scalar("SELECT minTimestamp FROM table_partitions('x')" +
                    " WHERE name LIKE '2024-01-01%' ORDER BY minTimestamp DESC LIMIT 1");
            final long dayEnd = MicrosTimestampDriver.floor("2024-01-02T00:00:00.000000Z");
            final TableToken xt = engine.verifyTableName("x");
            engine.releaseAllReaders();

            try (TableReader pinned = engine.getReader(xt)) {
                final String before = fingerprintOfDay(pinned, dayEnd);
                final LongList rowsBefore = rowsOfDay(pinned, dayEnd);
                try (WalWriter ww = engine.getWalWriter(xt)) {
                    ww.commitWithParams(siblingMinTs, dayEnd, WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE);
                    ww.commit();
                }
                drainWalQueue();
                assertNotSuspended("x");
                Assert.assertEquals("fixture: the replace itself changed the pinned view", before, fingerprintOfDay(pinned, dayEnd));

                // Merge-append into the (now shrunk) plain parent, well inside its range.
                execute("INSERT INTO x SELECT x::INT + 3_000_000 i, timestamp_sequence('2024-01-01T01:00:00.5', 100_000L) ts" +
                        " FROM long_sequence(5000)");
                drainWalQueue();
                assertNotSuspended("x");
                Assert.assertEquals(
                        "rows a pinned reader still maps were rewritten in place " + firstDifference(rowsBefore, rowsOfDay(pinned, dayEnd)),
                        before,
                        fingerprintOfDay(pinned, dayEnd)
                );
            }
        });
    }

    @Test
    public void testSquashAfterConvertRoundTripKeepsMergedRows() throws Exception {
        assertMemoryLeak(() -> checkSquashAfterMoveTail(true));
    }

    @Test
    public void testSquashWithoutConvertRoundTripKeepsMergedRows() throws Exception {
        assertMemoryLeak(() -> checkSquashAfterMoveTail(false));
    }

    /**
     * Checks, without mapping anything, that every native partition's column files (fixed-size data, var-size
     * aux) and BITMAP index files are at least as long as committed state says. A truncation shows up here as
     * an assertion instead of a SIGBUS in a later query.
     */
    private static void assertColumnFilesCoverPhysicalRows(String table, String state) {
        final TableToken tt = engine.verifyTableName(table);
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (TableReader reader = engine.getReader(tt); Path path = new Path()) {
            final TableReaderMetadata md = reader.getMetadata();
            final TxReader tx = reader.getTxFile();
            final ColumnVersionReader cvr = reader.getColumnVersionReader();
            for (int p = 0, n = reader.getPartitionCount(); p < n; p++) {
                if (tx.isPartitionParquet(p)) {
                    continue;
                }
                final long physical = reader.getPartitionPhysicalRowCount(p);
                final long pts = tx.getPartitionTimestampByIndex(p);
                final long nameTxn = tx.getPartitionNameTxn(p);
                path.of(engine.getConfiguration().getDbRoot()).concat(tt);
                TableUtils.setPathForNativePartition(path, md.getTimestampType(), reader.getPartitionedBy(), pts, nameTxn);
                final int plen = path.size();
                for (int c = 0, cn = md.getColumnCount(); c < cn; c++) {
                    final int type = md.getColumnType(c);
                    if (type < 0) {
                        continue;
                    }
                    final int wi = md.getWriterIndex(c);
                    final long top = cvr.getColumnTop(pts, wi);
                    if (top < 0) {
                        continue;
                    }
                    final long rows = physical - top;
                    if (rows <= 0) {
                        continue;
                    }
                    final CharSequence name = md.getColumnName(c);
                    final long colTxn = cvr.getColumnNameTxn(pts, wi);
                    final String where = "[partitionIndex=" + p + ", partitionTs=" + pts + ", column=" + name +
                            ", physicalRows=" + physical + ", top=" + top + "] " + state;
                    if (ColumnType.isVarSize(type)) {
                        final long auxSize = ColumnType.getDriver(type).getAuxVectorSize(rows);
                        final long len = ff.length(TableUtils.iFile(path.trimTo(plen), name, colTxn));
                        Assert.assertTrue("aux file shorter than committed rows, len=" + len + ", need=" + auxSize + ' ' + where, len >= auxSize);
                    } else {
                        final long need = rows * ColumnType.sizeOf(type);
                        final long len = ff.length(TableUtils.dFile(path.trimTo(plen), name, colTxn));
                        Assert.assertTrue("data file shorter than committed rows, len=" + len + ", need=" + need + ' ' + where, len >= need);
                    }
                    if (md.isColumnIndexed(c) && IndexType.isBitmap(md.getColumnIndexType(c))) {
                        final long kLen = ff.length(BitmapIndexUtils.keyFileName(path.trimTo(plen), name, colTxn));
                        final long kFd = ff.openRO(BitmapIndexUtils.keyFileName(path.trimTo(plen), name, colTxn));
                        Assert.assertTrue("cannot open .k " + where, kFd > -1);
                        final long keyCount;
                        final long valueMemSize;
                        try {
                            keyCount = ff.readNonNegativeInt(kFd, BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);
                            valueMemSize = ff.readNonNegativeLong(kFd, BitmapIndexUtils.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
                        } finally {
                            ff.close(kFd);
                        }
                        final long vLen = ff.length(BitmapIndexUtils.valueFileName(path.trimTo(plen), name, colTxn));
                        final long kNeed = BitmapIndexUtils.KEY_FILE_RESERVED + keyCount * BitmapIndexUtils.KEY_ENTRY_SIZE;
                        Assert.assertTrue(".k shorter than its header's key count, len=" + kLen + ", need=" + kNeed + ' ' + where, kLen >= kNeed);
                        Assert.assertTrue(".v shorter than its header's value mem size, len=" + vLen + ", need=" + valueMemSize + ' ' + where, vLen >= valueMemSize);
                    }
                }
            }
        }
    }

    private static void assertNoWriterInvariantViolations(long violationsBefore) {
        Assert.assertEquals(
                "writer invariant violated, last: " + WriterInvariantChecker.getLastViolation(),
                0,
                WriterInvariantChecker.getViolationCount() - violationsBefore
        );
    }

    private static void assertNotSuspended(String table) {
        Assert.assertFalse("table " + table + " is suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(table)));
    }

    private static String columnTopState(String table, String day, String column) throws Exception {
        try (TableReader reader = engine.getReader(engine.verifyTableName(table))) {
            final long pts = MicrosTimestampDriver.floor(day + "T00:00:00.000000Z");
            final int p = reader.getTxFile().getPartitionIndex(pts);
            final int wi = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(column));
            return "top=" + reader.getColumnVersionReader().getColumnTop(pts, wi) + ", liveRows=" + reader.getTxFile().getPartitionSize(p)
                    + ", physicalRows=" + reader.getPartitionPhysicalRowCount(p) + ", composite=" + reader.getTxFile().isPartitionComposite(p)
                    + ", nameTxn=" + reader.getTxFile().getPartitionNameTxn(p);
        }
    }

    private static void enableMergeAppend() {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "8K");
    }

    /**
     * The MOVE-TAIL-eager settings of O3PartitionCompactionTest#testMoveTailCopiesTheTailNotTheWholePartition.
     */
    private static void enableMoveTail() {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_TIME, 0);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_MOVE_TAIL_MIN_GAIN, 1);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 16);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);
    }

    private static String fingerprintOfColumnC(String day) throws Exception {
        long count = 0;
        long sum = 0;
        try (RecordCursorFactory f = select("SELECT c FROM x WHERE ts IN '" + day + "'")) {
            try (RecordCursor cursor = f.getCursor(sqlExecutionContext)) {
                while (cursor.hasNext()) {
                    final long v = cursor.getRecord().getLong(0);
                    if (v != Numbers.LONG_NULL) {
                        count++;
                        sum += v;
                    }
                }
            }
        }
        return count + "/" + sum;
    }

    /**
     * What a reader's OWN (non-reloaded) snapshot holds below {@code dayHi}: row count, sum of i, and the last row.
     */
    private static String fingerprintOfDay(TableReader reader, long dayHi) {
        long count = 0;
        long sum = 0;
        long lastI = 0;
        long lastTs = 0;
        try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
            while (cursor.hasNext()) {
                final long ts = cursor.getRecord().getTimestamp(1);
                if (ts >= dayHi) {
                    continue;
                }
                count++;
                lastI = cursor.getRecord().getInt(0);
                lastTs = ts;
                sum += lastI;
            }
        }
        return count + "/" + sum + "/last=" + lastI + "@" + lastTs;
    }

    private static String firstDifference(LongList before, LongList after) {
        final StringBuilder sb = new StringBuilder("[rowsBefore=").append(before.size() / 2).append(", rowsAfter=").append(after.size() / 2);
        int diffs = 0;
        for (int r = 0, n = Math.min(before.size(), after.size()); r < n; r += 2) {
            if (before.getQuick(r) != after.getQuick(r) || before.getQuick(r + 1) != after.getQuick(r + 1)) {
                if (diffs++ < 5) {
                    sb.append(", row ").append(r / 2).append(": i=").append(before.getQuick(r)).append("->").append(after.getQuick(r))
                            .append(" ts=").append(before.getQuick(r + 1)).append("->").append(after.getQuick(r + 1));
                }
            }
        }
        return sb.append(", differingRows=").append(diffs).append(']').toString();
    }

    private static boolean isComposite(String table, String day) throws Exception {
        try (TableReader reader = engine.getReader(engine.verifyTableName(table))) {
            final int partitionIndex = reader.getTxFile().getPartitionIndex(MicrosTimestampDriver.floor(day + "T00:00:00.000000Z"));
            return partitionIndex > -1 && reader.getTxFile().isPartitionComposite(partitionIndex);
        }
    }

    private static LongList rowsOfDay(TableReader reader, long dayHi) {
        final LongList rows = new LongList();
        try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
            while (cursor.hasNext()) {
                final long ts = cursor.getRecord().getTimestamp(1);
                if (ts < dayHi) {
                    rows.add(cursor.getRecord().getInt(0));
                    rows.add(ts);
                }
            }
        }
        return rows;
    }

    private static long scalar(String sql) throws Exception {
        try (RecordCursorFactory f = select(sql)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                Assert.assertTrue("query returned no row: " + sql, c.hasNext());
                return c.getRecord().getLong(0);
            }
        }
    }

    /**
     * @param failOnWrite 1 fails the commit's first write to v.d by failing the file's open. 2 lets the first action
     *                    write and fails the second one's: a composite plan opens v.d once for all its actions, so
     *                    that failure is the second writable mapping of the one open file.
     */
    private void checkFailedMergeAppendThenWriterClose(int failOnWrite, boolean twoActions) throws Exception {
        final AtomicBoolean armed = new AtomicBoolean();
        final AtomicInteger opens = new AtomicInteger();
        final AtomicInteger writeMaps = new AtomicInteger();
        final AtomicLong vFd = new AtomicLong(-1);
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (armed.get() && fd == vFd.get() && flags == Files.MAP_RW && writeMaps.incrementAndGet() >= failOnWrite) {
                    return FilesFacade.MAP_FAILED;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                if (armed.get() && Utf8s.containsAscii(name, "2024-01-02") && Utf8s.containsAscii(name, Files.SEPARATOR + "v.d")) {
                    opens.incrementAndGet();
                    if (failOnWrite == 1) {
                        return -1;
                    }
                    final long fd = super.openRW(name, opts);
                    vFd.set(fd);
                    return fd;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            // Pooled frame columns capture the FilesFacade they were built with; start from a fresh pool.
            engine.resetFrameFactory();
            enableMergeAppend();
            execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL INDEX, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // 2024-01-01 full, 2024-01-02 up to 11:59 when twoActions, so a batch after that founds a NEW piece.
            final int baseRows = twoActions ? 2160 : 2880;
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01', 60_000_000L), 'k' || (x % 10), x FROM long_sequence(" + baseRows + ")");
            drainWalQueue();

            armed.set(true);
            if (twoActions) {
                execute("INSERT INTO t SELECT * FROM (" +
                        "SELECT timestamp_sequence('2024-01-02T00:00:10', 10_000_000L) ts, 'n' || x s, x v FROM long_sequence(3000)" +
                        " UNION ALL " +
                        "SELECT timestamp_sequence('2024-01-02T18:00:00', 10_000_000L) ts, 'm' || x s, x v FROM long_sequence(1000))");
            } else {
                execute("INSERT INTO t SELECT timestamp_sequence('2024-01-02T00:00:10', 20_000_000L), 'n' || x, x FROM long_sequence(3000)");
            }
            drainWalQueue();
            armed.set(false);
            final boolean suspended = engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t"));
            Assert.assertTrue("fixture: the injected failure did not fail the commit [opens=" + opens.get() + ", writeMaps=" + writeMaps.get() + ']', suspended);

            engine.releaseAllReaders();
            engine.releaseAllWriters();
            // The distressed writer's close must not truncate the index files. The result is asserted after the
            // resume, so a failure also reports whether the resumed apply copes.
            String afterFailure = "files intact";
            try {
                assertColumnFilesCoverPhysicalRows("t", "[after failed commit, opens=" + opens.get() + ']');
            } catch (AssertionError e) {
                afterFailure = e.getMessage();
            }

            execute("ALTER TABLE t RESUME WAL");
            drainWalQueue();
            Assert.assertFalse("resumed apply suspended the table again; after the failed commit: " + afterFailure,
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            Assert.assertEquals("index files after the failed commit", "files intact", afterFailure);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertColumnFilesCoverPhysicalRows("t", "[after resume]");
            assertQuery("SELECT count() c FROM t WHERE s = 'n2999'").noRandomAccess().expectSize().returns("c\n1\n");
            assertQuery("SELECT count() c FROM t WHERE s = 'k3' AND ts IN '2024-01-02'").noRandomAccess().expectSize()
                    .returns(twoActions ? "c\n72\n" : "c\n144\n");
            if (twoActions) {
                assertQuery("SELECT count() c FROM t WHERE s = 'm999'").noRandomAccess().expectSize().returns("c\n1\n");
            }
        });
    }

    private void checkPooledReaderAcrossMakePlainClamp(boolean columnHasRowsAtWarmUp) throws Exception {
        enableMergeAppend();
        enableMoveTail();
        setCurrentMicros(MicrosTimestampDriver.floor("2024-01-10T00:00:00.000000Z"));
        execute("CREATE TABLE x AS (SELECT x::INT i, timestamp_sequence('2024-01-01', 1_000_000L) ts" +
                " FROM long_sequence(20_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalQueue();
        for (int k = 0; k < 3; k++) {
            execute("INSERT INTO x SELECT x::INT + 500_000 i, timestamp_sequence('2024-01-01T05:00:00', 1_000_000L) ts" +
                    " FROM long_sequence(200)");
            drainWalQueue();
        }
        Assert.assertTrue("fixture: the day must be composite", isComposite("x", "2024-01-01"));
        // Added while the LAST partition is composite: top = E, above what MOVE-TAIL will leave live.
        execute("ALTER TABLE x ADD COLUMN c LONG");
        drainWalQueue();
        long expectedCount = 3000;
        long expectedSum = 126_000;
        if (columnHasRowsAtWarmUp) {
            // Into the tail stride MOVE-TAIL is going to move out: c gets file rows above its top.
            execute("INSERT INTO x (i, ts, c) SELECT x::INT + 600_000 i, timestamp_sequence('2024-01-01T05:10:00.5', 1_000_000L) ts, x" +
                    " FROM long_sequence(100)");
            drainWalQueue();
            expectedCount += 100;
            expectedSum += 5050;
        }
        engine.releaseAllReaders();
        // Warm the pooled reader with the day open and c's top cached.
        Assert.assertEquals(columnHasRowsAtWarmUp ? "100/5050" : "0/0", fingerprintOfColumnC("2024-01-01"));
        final String topAfterAdd = columnTopState("x", "2024-01-01", "c");

        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, Long.MAX_VALUE / 8);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 2);
        for (int k = 0; k < 6; k++) {
            execute("INSERT INTO x (i, ts) SELECT x::INT + 800_000 i, timestamp_sequence('2024-03-0" + (k + 1) + "', 60_000_000L) ts" +
                    " FROM long_sequence(2)");
            drainWalQueue();
        }
        Assert.assertEquals("fixture: MOVE-TAIL must leave front + one sibling", 2,
                scalar("SELECT count() FROM table_partitions('x') WHERE name LIKE '2024-01-01%'"));
        // The pooled reader still maps the day, so on Windows TRIM-FILES fails (ERROR_USER_MAPPED_FILE) and the front
        // stays composite. MAKE-PLAIN's first commit, which clamps the tops this test is about, runs either way.
        if (!Os.isWindows()) {
            Assert.assertFalse("fixture: MAKE-PLAIN must have made the front plain", isComposite("x", "2024-01-01"));
        }
        Assert.assertEquals(columnHasRowsAtWarmUp ? "100/5050" : "0/0", fingerprintOfColumnC("2024-01-01"));
        final String topAfterMakePlain = columnTopState("x", "2024-01-01", "c");

        execute("INSERT INTO x (i, ts, c) SELECT x::INT + 3_000_000 i, timestamp_sequence('2024-01-01T01:00:00.5', 100_000L) ts, 42" +
                " FROM long_sequence(3000)");
        drainWalQueue();
        assertNotSuspended("x");

        final String pooled = fingerprintOfColumnC("2024-01-01");
        engine.releaseAllReaders();
        final String fresh = fingerprintOfColumnC("2024-01-01");
        Assert.assertEquals("fresh reader", expectedCount + "/" + expectedSum, fresh);
        Assert.assertEquals("pooled reader disagrees with a fresh one [afterAdd " + topAfterAdd + ", afterMakePlain "
                + topAfterMakePlain + ", afterMerge " + columnTopState("x", "2024-01-01", "c") + ']', fresh, pooled);
    }

    private void checkRenameIndexedColumnThenMergeIntoFormerLastPartition(boolean convertRoundTrip, String symbolType) throws Exception {
        enableMergeAppend();
        execute("CREATE TABLE t (ts TIMESTAMP, s " + symbolType + ", v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01', 60_000_000L), 'k' || (x % 10), x FROM long_sequence(2880)");
        drainWalQueue();
        if (convertRoundTrip) {
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-02'");
            drainWalQueue();
            execute("ALTER TABLE t CONVERT PARTITION TO NATIVE LIST '2024-01-02'");
            drainWalQueue();
        }
        execute("INSERT INTO t VALUES ('2024-01-03T00:00:00', 'k1', 1)");
        drainWalQueue();
        execute("ALTER TABLE t RENAME COLUMN s TO s2");
        drainWalQueue();

        // 3000 NEW keys merged into 2024-01-02: ~96KB of .k and >=3000 new value blocks in .v.
        execute("INSERT INTO t SELECT timestamp_sequence('2024-01-02T00:00:10', 20_000_000L), 'n' || x, x FROM long_sequence(3000)");
        drainWalQueue();
        assertNotSuspended("t");
        final String state = "[convertRoundTrip=" + convertRoundTrip + ']';

        engine.releaseAllReaders();
        engine.releaseAllWriters();
        assertColumnFilesCoverPhysicalRows("t", state);
        assertQuery("SELECT count() c FROM t WHERE s2 = 'n2999'").noRandomAccess().expectSize().returns("c\n1\n");
        assertQuery("SELECT count() c FROM t WHERE s2 = 'k3' AND ts IN '2024-01-02'").noRandomAccess().expectSize().returns("c\n144\n");

        // The table must keep taking writes.
        execute("INSERT INTO t VALUES ('2024-01-02T23:59:59', 'k3', 7)");
        drainWalQueue();
        assertNotSuspended("t");
        assertQuery("SELECT count() c FROM t WHERE s2 = 'k3' AND ts IN '2024-01-02'").noRandomAccess().expectSize().returns("c\n145\n");
    }

    private void checkSquashAfterMoveTail(boolean convertRoundTrip) throws Exception {
        enableMergeAppend();
        enableMoveTail();
        setCurrentMicros(MicrosTimestampDriver.floor("2024-01-10T00:00:00.000000Z"));

        execute("CREATE TABLE x AS (SELECT x::INT i, timestamp_sequence('2024-01-01', 1_000_000L) ts" +
                " FROM long_sequence(20_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalQueue();

        if (convertRoundTrip) {
            // Takes the last partition through parquet and back to native before the squash.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();
        }
        final long d1 = MicrosTimestampDriver.floor("2024-01-01T00:00:00.000000Z");

        // Make the day composite, then let MOVE-TAIL give it a split sibling while later days are created.
        for (int k = 0; k < 3; k++) {
            execute("INSERT INTO x SELECT x::INT + 500_000 i, timestamp_sequence('2024-01-01T05:00:00', 1_000_000L) ts" +
                    " FROM long_sequence(200)");
            drainWalQueue();
        }
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, Long.MAX_VALUE / 8);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 2);
        for (int k = 0; k < 6; k++) {
            execute("INSERT INTO x SELECT x::INT + 800_000 i, timestamp_sequence('2024-03-0" + (k + 1) + "', 60_000_000L) ts" +
                    " FROM long_sequence(2)");
            drainWalQueue();
        }
        assertNotSuspended("x");
        final long siblings = scalar("SELECT count() FROM table_partitions('x') WHERE name LIKE '2024-01-01%'");
        Assert.assertEquals("fixture: MOVE-TAIL must leave the day as front + one sibling", 2, siblings);

        engine.releaseAllReaders();
        execute("ALTER TABLE x SQUASH PARTITIONS");
        drainWalQueue();
        assertNotSuspended("x");

        // APPEND past the day's max through the executor's own fds: 20k INT rows = 80KB, far past a page.
        execute("INSERT INTO x SELECT x::INT + 2_000_000 i, timestamp_sequence('2024-01-01T12:00:00', 1_000_000L) ts" +
                " FROM long_sequence(20_000)");
        drainWalQueue();
        assertNotSuspended("x");

        final String state = "[convertRoundTrip=" + convertRoundTrip + ", siblingsBeforeSquash=" + siblings +
                ", day=" + d1 + ']';

        engine.releaseAllReaders();
        engine.releaseAllWriters();

        assertColumnFilesCoverPhysicalRows("x", state);
        assertQuery("SELECT i, ts FROM x WHERE ts IN '2024-01-01' LIMIT -1")
                .timestamp("ts")
                .expectSize()
                .returns("i\tts\n2020000\t2024-01-01T17:33:19.000000Z\n");
        assertQuery("SELECT count() c FROM x WHERE ts IN '2024-01-01'")
                .noRandomAccess()
                .expectSize()
                .returns("c\n40600\n");
    }
}
