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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.function.LongUnaryOperator;

import static io.questdb.cairo.TableUtils.*;

/**
 * A power loss can make the first page of {@code _txn} durable after commit N -- the version word, the A/B
 * geometry and the head of N's area -- while a later page keeps what it held before N. When N's area sits
 * where commit N-2's did, that page holds N-2's tail: the live area fails its body checksum, and the other
 * area still holds commit N-1 intact.
 * <p>
 * Each test builds that image from page-cache snapshots of {@code _txn}, restarts on it and checks that the
 * table continues from commit N-1. WAL apply replays commit N, a non-WAL table loses it, and a table whose
 * other files already moved past commit N-1 refuses to open without changing a byte of {@code _txn}.
 * Tables enrolled in adaptive commit mode never reach that code: startup recovery restores their
 * {@code _txn} from the durable epoch, and the adaptive runs check that the end state is the same.
 */
@RunWith(Parameterized.class)
public class TxnTornLiveAreaRollbackTest extends AbstractCairoTest {
    private static final long DAY = 86_400_000_000L;
    private static final long HOUR = 3_600_000_000L;
    private static final long T0 = 1_704_067_200_000_000L; // 2024-01-01T00:00:00Z
    // The image keeps the bytes below this boundary from commit N and the rest from commit N-1: 4 KiB is the
    // smallest unit the kernel writes back on its own. The rollback does not depend on the OS page size.
    private static final int TEAR_BOUNDARY = 4096;
    private final String commitMode;

    public TxnTornLiveAreaRollbackTest(String commitMode) {
        this.commitMode = commitMode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{"nosync"}, {"async"}, {"sync"}, {"adaptive"}});
    }

    @Test
    public void testNonWalLiveAreaPastEndOfFile() throws Exception {
        // The header page names an area the file never grew to hold: a crash made the header durable before
        // the file length. A read-only mapping past the end of the file faults on first touch.
        assertMemoryLeak(() -> {
            configureCommitMode();
            final String txnPath = createTable(false, DAY, 20);
            final ObjList<String> scans = new ObjList<>();
            // In-place appends to older partitions: each commit rewrites the full record, stamped.
            final ObjList<byte[]> snapshots = commitRows(txnPath, false, 2, k -> T0 + (5 + k) * DAY + HOUR, scans);
            engine.clear();

            final byte[] image = snapshots.getLast().clone();
            final ByteBuffer bb = ByteBuffer.wrap(image).order(ByteOrder.LITTLE_ENDIAN);
            final long version = bb.getLong((int) TX_BASE_OFFSET_VERSION_64);
            Assert.assertTrue("the previous area must be intact", isAreaVerified(image, version - 1));
            bb.putInt((int) ((version & 1) == 0 ? TX_BASE_OFFSET_A_32 : TX_BASE_OFFSET_B_32), image.length + TEAR_BOUNDARY);
            writeFile(txnPath, image);

            assertReaderLoadsPreviousRecord(txnPath, version - 1);
            try (CairoEngine restarted = new CairoEngine(configuration)) {
                final SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(restarted);
                assertQuery("x").withEngine(restarted).withContext(ctx).noLeakCheck().timestamp("ts").expectSize()
                        .returns(scans.getQuick(scans.size() - 2));
                restarted.clear();
            }
        });
    }

    @Test
    public void testNonWalOutOfOrderCommitRollsBack() throws Exception {
        final TxnFsyncCountingFacade ff = new TxnFsyncCountingFacade();
        assertMemoryLeak(ff, () -> {
            configureCommitMode();
            final String txnPath = createTable(false, DAY, 200);
            final ObjList<String> scans = new ObjList<>();
            // Rows at 01:00 of days that hold a 00:00 row: each commit appends in place to an older partition
            // whose entry lies in the second page, and takes the full-record path through _txn.
            final ObjList<byte[]> snapshots = commitRows(txnPath, false, 4, k -> T0 + (150 + k) * DAY + HOUR, scans);
            engine.clear();
            final int n = installTornImage(txnPath, snapshots);
            final byte[] image = readFile(txnPath);
            final long tornVersion = ByteBuffer.wrap(image).order(ByteOrder.LITTLE_ENDIAN).getLong((int) TX_BASE_OFFSET_VERSION_64);
            assertReaderLoadsPreviousRecord(txnPath, tornVersion - 1);

            ff.txnFsyncCount = 0;
            try (CairoEngine restarted = new CairoEngine(configuration)) {
                final SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(restarted);
                // The first query cannot read the torn _txn, so the engine opens the table's writer to repair
                // it, which rolls back to commit N-1. A non-WAL table has nothing to replay commit N from.
                assertQuery("x").withEngine(restarted).withContext(ctx).noLeakCheck().timestamp("ts").expectSize()
                        .returns(scans.getQuick(n - 1));
                Assert.assertEquals("the rollback must be durable whatever the commit mode", 1, ff.txnFsyncCount);

                // Only the version word moved, back to commit N-1.
                final byte[] rolledBack = readFile(txnPath);
                ByteBuffer.wrap(image).order(ByteOrder.LITTLE_ENDIAN).putLong((int) TX_BASE_OFFSET_VERSION_64, tornVersion - 1);
                Assert.assertTrue(Arrays.equals(image, 0, image.length, rolledBack, 0, image.length));

                restarted.execute("insert into x values (" + (T0 + 220 * DAY) + "::timestamp, 777)", ctx);
                assertQuery("select count() c, sum(v) s from x").withEngine(restarted).withContext(ctx).noLeakCheck().noRandomAccess().expectSize()
                        .returns("c\ts\n" + (200 + n) + "\t" + (sumOfFirst(200) + sumOfCommits(n - 1) + 777) + "\n");
                restarted.clear();
            }
        });
    }

    @Test
    public void testWalAppendWithAreaAfterPrevious() throws Exception {
        // 100 partitions: the area at offset 64 fits the first page, and the one after it straddles the page
        // boundary. The previous record loads, but a writer used to refuse to continue from it.
        final long lastDay = T0 + 99 * DAY;
        assertWalReplaysTornCommit(DAY, 100, 6, k -> lastDay + k * HOUR, false, true);
    }

    @Test
    public void testWalAppendWithAreaAtHeader() throws Exception {
        // 200 partitions: the area at offset 64 crosses the first page, and the previous area lies wholly
        // past the part of the file a reader maps for the live one.
        final long lastDay = T0 + 199 * DAY;
        assertWalReplaysTornCommit(DAY, 200, 4, k -> lastDay + k * HOUR, false, true);
    }

    @Test
    public void testWalNewPartitionEachCommit() throws Exception {
        // A new partition per commit grows the record, so the areas move: commit N's area may start past the
        // first page, or overlap where an older record was. Base read such images as partitions that did not
        // exist and wiped the table.
        final long lastDay = T0 + 199 * DAY;
        assertWalReplaysTornCommit(DAY, 200, 4, k -> lastDay + k * DAY, false, false);
    }

    @Test
    public void testWalOutOfOrderAppendIntoOlderPartition() throws Exception {
        // Each commit appends in place to an older partition whose entry lies in the second page. Reading the
        // torn bytes as they are used to lose committed rows for good.
        assertWalReplaysTornCommit(DAY, 200, 4, k -> T0 + (150 + k) * DAY + HOUR, false, true);
    }

    @Test
    public void testWalOutOfOrderMergeReplaysOverOlderPartitionVersion() throws Exception {
        // Each commit merges a row into the middle of an older partition, which writes a new partition
        // version. A reader holds the versions commit N-1 reads, so they are still on disk; the lost commit's
        // version is not attached to commit N-1, so the writer drops it, and the replay merges again.
        assertWalReplaysTornCommit(12 * HOUR, 400, 4, k -> T0 + (150 + k) * DAY + 6 * HOUR, true, true);
    }

    @Test
    public void testWalRefusesWhenMetadataMovedPastPreviousTransaction() throws Exception {
        Assume.assumeFalse("adaptive recovery restores enrolled tables", "adaptive".equals(commitMode));
        assertMemoryLeak(() -> {
            configureCommitMode();
            final String txnPath = createTable(true, DAY, 200);
            final long lastDay = T0 + 199 * DAY;
            final ObjList<byte[]> snapshots = commitRows(txnPath, true, 3, k -> lastDay + k * HOUR, null);
            // _meta moves to the next version before _txn records the structural commit.
            execute("alter table x add column c int");
            drainWalQueue();
            snapshots.add(readFile(txnPath));
            engine.clear();
            Assert.assertEquals(4, installTornImage(txnPath, snapshots));
            assertRestartRefuses(txnPath, "_meta does not match it");
        });
    }

    @Test
    public void testWalRefusesWhenPreviousPartitionVersionIsGone() throws Exception {
        Assume.assumeFalse("adaptive recovery restores enrolled tables", "adaptive".equals(commitMode));
        assertMemoryLeak(() -> {
            configureCommitMode();
            final String txnPath = createTable(true, 12 * HOUR, 400);
            final ObjList<byte[]> snapshots = commitRows(txnPath, true, 4, k -> T0 + (150 + k) * DAY + 6 * HOUR, null);
            engine.clear();
            final int n = installTornImage(txnPath, snapshots);

            // Commit N merged into partition 150+N and wrote a new version of it. With no reader holding the
            // version commit N-1 reads, the writer removed it right after the commit, and that removal can
            // reach the disk before the second page of _txn does.
            final long partitionTimestamp = T0 + (150 + n) * DAY;
            final long previousNameTxn = readPartitionNameTxn(snapshots.getQuick(n - 1), partitionTimestamp);
            Assert.assertNotEquals(
                    "commit N must have written a new version of the partition",
                    previousNameTxn,
                    readPartitionNameTxn(snapshots.getQuick(n), partitionTimestamp)
            );
            try (Path path = new Path()) {
                path.of(txnPath).parent();
                TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTimestamp, previousNameTxn);
                if (configuration.getFilesFacade().exists(path.$())) {
                    Assert.assertTrue(configuration.getFilesFacade().rmdir(path.slash()));
                }
                Assert.assertFalse(configuration.getFilesFacade().exists(path.$()));
            }
            assertRestartRefuses(txnPath, "a partition it reads is missing");
        });
    }

    @Test
    public void testWalRefusesWhenPreviousRecordIsUnverified() throws Exception {
        Assume.assumeFalse("adaptive recovery restores enrolled tables", "adaptive".equals(commitMode));
        assertMemoryLeak(() -> {
            configureCommitMode();
            final String txnPath = createTable(true, DAY, 100);
            final long lastDay = T0 + 99 * DAY;
            final ObjList<byte[]> snapshots = commitRows(txnPath, true, 6, k -> lastDay + k * HOUR, null);
            engine.clear();
            installTornImage(txnPath, snapshots);

            // Erase the previous record's checksum stamp: it now loads unverified, like a record an older
            // binary wrote, and nothing vouches for it.
            final byte[] image = readFile(txnPath);
            final ByteBuffer bb = ByteBuffer.wrap(image).order(ByteOrder.LITTLE_ENDIAN);
            final long previousVersion = bb.getLong((int) TX_BASE_OFFSET_VERSION_64) - 1;
            final int previousOffset = bb.getInt((int) ((previousVersion & 1) == 0 ? TX_BASE_OFFSET_A_32 : TX_BASE_OFFSET_B_32));
            bb.putInt(previousOffset + (int) TX_OFFSET_BODY_CHECKSUM_STAMP_32, 0);
            writeFile(txnPath, image);
            assertReaderLoadsPreviousRecord(txnPath, previousVersion);
            assertRestartRefuses(txnPath, "_txn live area is torn; refusing to write from the previous transaction [txn=");
        });
    }

    private static int installTornImage(String txnPath, ObjList<byte[]> snapshots) throws IOException {
        return installTornImage(txnPath, snapshots, true);
    }

    // Builds the image a power loss leaves when the kernel wrote back the first page of _txn after commit n but
    // not the rest: bytes below TEAR_BOUNDARY from snapshot n, the rest from snapshot n-1, where the file was
    // as long. Takes the latest commit whose live area that tears. With isSameArea, only an area placed and
    // sized like the one two commits back qualifies, so the stale bytes it keeps belong to that older record;
    // otherwise the area may have moved, and the stale bytes are whatever that part of the file held.
    private static int installTornImage(String txnPath, ObjList<byte[]> snapshots, boolean isSameArea) throws IOException {
        for (int n = snapshots.size() - 1; n > 1; n--) {
            final byte[] torn = snapshots.getQuick(n);
            final byte[] previous = snapshots.getQuick(n - 1);
            final byte[] older = snapshots.getQuick(n - 2);
            if (torn.length <= TEAR_BOUNDARY
                    || (isSameArea && (torn.length != previous.length
                    || liveAreaOffset(torn) != liveAreaOffset(older)
                    || liveAreaPartitionsSize(torn) != liveAreaPartitionsSize(older)))) {
                continue;
            }
            final byte[] image = new byte[torn.length];
            System.arraycopy(torn, 0, image, 0, TEAR_BOUNDARY);
            System.arraycopy(previous, TEAR_BOUNDARY, image, TEAR_BOUNDARY, Math.max(0, Math.min(previous.length, torn.length) - TEAR_BOUNDARY));
            final long version = ByteBuffer.wrap(image).order(ByteOrder.LITTLE_ENDIAN).getLong((int) TX_BASE_OFFSET_VERSION_64);
            if (isAreaVerified(image, version) || !isAreaVerified(image, version - 1)) {
                continue;
            }
            writeFile(txnPath, image);
            return n;
        }
        Assert.fail("no commit tore the live area over an intact previous area");
        return -1;
    }

    private static boolean isAreaVerified(byte[] file, long areaVersion) {
        final ByteBuffer bb = ByteBuffer.wrap(file).order(ByteOrder.LITTLE_ENDIAN);
        final boolean isA = (areaVersion & 1) == 0;
        final int offset = bb.getInt((int) (isA ? TX_BASE_OFFSET_A_32 : TX_BASE_OFFSET_B_32));
        final int symbolsSize = bb.getInt((int) (isA ? TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 : TX_BASE_OFFSET_SYMBOLS_SIZE_B_32));
        final int partitionsSize = bb.getInt((int) (isA ? TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 : TX_BASE_OFFSET_PARTITIONS_SIZE_B_32));
        final int size = TableUtils.calculateTxRecordSize(symbolsSize, partitionsSize);
        if (offset < TX_BASE_HEADER_SIZE
                || offset + size > file.length
                || bb.getLong(offset + (int) TX_OFFSET_TXN_64) != areaVersion
                || bb.getInt(offset + (int) TX_OFFSET_BODY_CHECKSUM_STAMP_32) != ((int) areaVersion ^ TX_BODY_CHECKSUM_STAMP_XOR)) {
            return false;
        }
        // Computed apart from TxReader, so the test checks the image it builds rather than the code under test.
        final long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < size; i++) {
                Unsafe.getUnsafe().putByte(address + i, file[offset + i]);
            }
            final long checksum = TableUtils.calculateTxnBodyChecksum(address, size, TX_RECORD_HEADER_SIZE + symbolsSize);
            return bb.getLong(offset + (int) TX_OFFSET_BODY_CHECKSUM_64) == checksum;
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static int liveAreaOffset(byte[] file) {
        final ByteBuffer bb = ByteBuffer.wrap(file).order(ByteOrder.LITTLE_ENDIAN);
        return bb.getInt((int) ((bb.getLong((int) TX_BASE_OFFSET_VERSION_64) & 1) == 0 ? TX_BASE_OFFSET_A_32 : TX_BASE_OFFSET_B_32));
    }

    private static int liveAreaPartitionsSize(byte[] file) {
        final ByteBuffer bb = ByteBuffer.wrap(file).order(ByteOrder.LITTLE_ENDIAN);
        final boolean isA = (bb.getLong((int) TX_BASE_OFFSET_VERSION_64) & 1) == 0;
        return bb.getInt((int) (isA ? TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 : TX_BASE_OFFSET_PARTITIONS_SIZE_B_32));
    }

    private static byte[] readFile(String path) throws IOException {
        return java.nio.file.Files.readAllBytes(Paths.get(path));
    }

    private static long readPartitionNameTxn(byte[] file, long partitionTimestamp) {
        final ByteBuffer bb = ByteBuffer.wrap(file).order(ByteOrder.LITTLE_ENDIAN);
        final boolean isA = (bb.getLong((int) TX_BASE_OFFSET_VERSION_64) & 1) == 0;
        final int offset = bb.getInt((int) (isA ? TX_BASE_OFFSET_A_32 : TX_BASE_OFFSET_B_32));
        final int symbolsSize = bb.getInt((int) (isA ? TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 : TX_BASE_OFFSET_SYMBOLS_SIZE_B_32));
        final int partitionsSize = bb.getInt((int) (isA ? TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 : TX_BASE_OFFSET_PARTITIONS_SIZE_B_32));
        final int table = offset + TX_RECORD_HEADER_SIZE + symbolsSize + Integer.BYTES;
        for (int p = table, hi = table + partitionsSize; p < hi; p += LONGS_PER_TX_ATTACHED_PARTITION * Long.BYTES) {
            if (bb.getLong(p) == partitionTimestamp) {
                return bb.getLong(p + 2 * Long.BYTES);
            }
        }
        Assert.fail("partition not found: " + partitionTimestamp);
        return -1;
    }

    private static long sumOfCommits(int commits) {
        return 100_000L * commits + (long) commits * (commits + 1) / 2;
    }

    private static long sumOfFirst(int rows) {
        return (long) rows * (rows + 1) / 2;
    }

    private static void writeFile(String path, byte[] image) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(path, "rw")) {
            file.setLength(image.length);
            file.seek(0);
            file.write(image);
        }
    }

    private void assertReaderLoadsPreviousRecord(String txnPath, long previousTxn) {
        // A reader maps _txn only as far as the live area; it must still reach the previous record.
        try (Path path = new Path(); TxReader reader = new TxReader(configuration.getFilesFacade())) {
            reader.ofRO(path.of(txnPath).$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
            Assert.assertTrue(reader.unsafeLoadAll());
            Assert.assertEquals(previousTxn, reader.getTxn());
            Assert.assertEquals(previousTxn + 1, reader.unsafeReadVersion());
        }
    }

    private void assertRestartRefuses(String txnPath, String reason) throws Exception {
        final byte[] image = readFile(txnPath);
        try (CairoEngine restarted = new CairoEngine(configuration)) {
            final SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(restarted);
            TestUtils.drainWalQueue(restarted);
            final TableToken token = restarted.verifyTableName("x");
            try (TableWriter ignore = restarted.getWriter(token, "test")) {
                Assert.fail("the writer must not continue from the previous transaction");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), reason);
            }
            assertQuery("select suspended from wal_tables()").withEngine(restarted).withContext(ctx).noLeakCheck()
                    .noRandomAccess()
                    .returns("suspended\ntrue\n");
            restarted.clear();
        }
        // Refusing leaves _txn as the crash left it.
        final byte[] after = readFile(txnPath);
        Assert.assertTrue(Arrays.equals(image, 0, image.length, after, 0, image.length));
    }

    private void assertWalReplaysTornCommit(
            long rowStep,
            int rows,
            int commits,
            LongUnaryOperator commitTimestamp,
            boolean holdReader,
            boolean isSameArea
    ) throws Exception {
        assertMemoryLeak(() -> {
            configureCommitMode();
            final String txnPath = createTable(true, rowStep, rows);
            final ObjList<byte[]> snapshots;
            if (holdReader) {
                // A reader on the first transaction stops the writer from removing the partition versions it
                // replaces, as a live query does.
                try (TableReader ignore = engine.getReader("x")) {
                    snapshots = commitRows(txnPath, true, commits, commitTimestamp, null);
                }
            } else {
                snapshots = commitRows(txnPath, true, commits, commitTimestamp, null);
            }
            final String expected = selectAll();
            engine.clear();
            installTornImage(txnPath, snapshots, isSameArea);
            final long tornVersion = ByteBuffer.wrap(readFile(txnPath)).order(ByteOrder.LITTLE_ENDIAN).getLong((int) TX_BASE_OFFSET_VERSION_64);
            assertReaderLoadsPreviousRecord(txnPath, tornVersion - 1);

            try (CairoEngine restarted = new CairoEngine(configuration)) {
                final SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(restarted);
                TestUtils.drainWalQueue(restarted);
                // WAL apply replays the commit the rollback dropped: nothing committed is lost.
                assertQuery("x").withEngine(restarted).withContext(ctx).noLeakCheck().timestamp("ts").expectSize()
                        .returns(expected);
                assertQuery("select suspended, writerTxn = sequencerTxn caught_up from wal_tables()")
                        .withEngine(restarted).withContext(ctx).noLeakCheck().noRandomAccess()
                        .returns("suspended\tcaught_up\nfalse\ttrue\n");
                restarted.execute("insert into x values (" + (T0 + 220 * DAY) + "::timestamp, 777)", ctx);
                TestUtils.drainWalQueue(restarted);
                assertQuery("select count() c, sum(v) s from x").withEngine(restarted).withContext(ctx).noLeakCheck().noRandomAccess().expectSize()
                        .returns("c\ts\n" + (rows + commits + 1) + "\t" + (sumOfFirst(rows) + sumOfCommits(commits) + 777) + "\n");
                restarted.clear();
            }
        });
    }

    private ObjList<byte[]> commitRows(
            String txnPath,
            boolean isWal,
            int commits,
            LongUnaryOperator commitTimestamp,
            ObjList<String> scans
    ) throws Exception {
        final ObjList<byte[]> snapshots = new ObjList<>();
        snapshots.add(readFile(txnPath));
        if (scans != null) {
            scans.add(selectAll());
        }
        for (int k = 1; k <= commits; k++) {
            execute("insert into x values (" + commitTimestamp.applyAsLong(k) + "::timestamp, " + (100_000 + k) + ")");
            if (isWal) {
                drainWalQueue();
            }
            snapshots.add(readFile(txnPath));
            if (scans != null) {
                scans.add(selectAll());
            }
        }
        return snapshots;
    }

    private void configureCommitMode() {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        // No epoch after the enrolment baseline: an image torn at or below an fsync'd epoch cannot happen.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 1000);
        spinLockTimeout = 1000;
        // Write the upgrade marker first, or the restarted engine would re-run every migration on the table.
        try (CairoEngine ignore = new CairoEngine(configuration)) {
            Assert.assertNotNull(ignore.getConfiguration());
        }
    }

    private String createTable(boolean isWal, long rowStep, int rows) throws SqlException {
        execute("create table x (ts timestamp, v long) timestamp(ts) partition by DAY" + (isWal ? " WAL" : " BYPASS WAL"));
        execute("insert into x select timestamp_sequence(" + T0 + ", " + rowStep + ") ts, x v from long_sequence(" + rows + ")");
        if (isWal) {
            drainWalQueue();
        }
        final TableToken token = engine.verifyTableName("x");
        return configuration.getDbRoot() + Files.SEPARATOR + token.getDirName() + Files.SEPARATOR + TXN_FILE_NAME;
    }

    private String selectAll() throws SqlException {
        final StringSink sink = new StringSink();
        TestUtils.printSql(engine, sqlExecutionContext, "x", sink);
        return sink.toString();
    }

    private static final class TxnFsyncCountingFacade extends TestFilesFacadeImpl {
        private final HashSet<Long> txnFds = new HashSet<>();
        private int txnFsyncCount;

        @Override
        public boolean close(long fd) {
            synchronized (txnFds) {
                txnFds.remove(fd);
            }
            return super.close(fd);
        }

        @Override
        public void fsync(long fd) {
            synchronized (txnFds) {
                if (txnFds.contains(fd)) {
                    txnFsyncCount++;
                }
            }
            super.fsync(fd);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (fd > -1 && Utf8s.endsWithAscii(name, Files.SEPARATOR + TXN_FILE_NAME)) {
                synchronized (txnFds) {
                    txnFds.add(fd);
                }
            }
            return fd;
        }
    }
}
