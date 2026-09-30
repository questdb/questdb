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
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Files;
import io.questdb.std.FlyweightMessageContainer;
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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import static io.questdb.cairo.TableUtils.*;

/**
 * A power loss can leave {@code _cv} unable to serve the column version that an intact {@code _txn} record
 * names. Part of the live area keeps what its page held before the commit that wrote it, or the whole file
 * stays one commit behind {@code _txn}. Commit N is then not durable as a whole, even though its {@code _txn}
 * record is.
 * <p>
 * Each test builds such an image from page-cache snapshots and restarts on it. The table continues from commit
 * N-1 when that commit's column versions and every file they name are still on disk: WAL apply replays commit
 * N, a non-WAL table loses it. Otherwise the writer refuses to open without changing a byte, and a WAL table is
 * suspended with an actionable error. Either way the WAL writer keeps accepting rows. Tables enrolled in
 * adaptive commit mode never reach that code: startup recovery restores their {@code _txn} and {@code _cv}
 * from the durable epoch, and the adaptive runs check that the end state is the same.
 */
@RunWith(Parameterized.class)
public class ColumnVersionTornRecoveryTest extends AbstractCairoTest {
    private static final long DAY = 86_400_000_000L;
    private static final long HOUR = 3_600_000_000L;
    private static final int PAGE = 4096;
    private static final int ROWS = 300;
    private static final long T0 = 1_704_067_200_000_000L; // 2024-01-01T00:00:00Z
    private static final long NEW_ROW_TS = T0 + (ROWS - 1) * DAY + 2 * HOUR;
    private final String commitMode;

    public ColumnVersionTornRecoveryTest(String commitMode) {
        this.commitMode = commitMode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{"nosync"}, {"async"}, {"sync"}, {"adaptive"}});
    }

    @Test
    public void testNonWalColumnVersionBehindTxnRollsBackOnFirstRead() throws Exception {
        // _txn made commit N durable, _cv none of it: the whole file still reads as commit N-1 left it.
        final TxnFsyncCountingFacade ff = new TxnFsyncCountingFacade();
        assertMemoryLeak(ff, () -> {
            configureCommitMode();
            final String dir = createTable(false);
            final Commits commits = new Commits(dir);
            try (TableReader ignore = engine.getReader("x")) {
                commits.updateColumnC(false, 3);
            }
            engine.clear();
            final int n = commits.size() - 1;
            writeFile(dir + COLUMN_VERSION_FILE_NAME, commits.cv(n - 1));
            assertNonWalRollsBackOnFirstRead(ff, dir, commits, n);
        });
    }

    @Test
    public void testNonWalTornColumnVersionRefusedWhenPreviousColumnFilesAreGone() throws Exception {
        assertMemoryLeak(() -> {
            configureCommitMode();
            final String dir = createTable(false);
            final Commits commits = new Commits(dir);
            // No reader holds the previous column versions, so each UPDATE removes them right after it commits.
            commits.updateColumnC(false, 3);
            engine.clear();
            final int n = commits.size() - 1;
            installTornColumnVersion(dir, commits.cv(n), commits.cv(n - 1));
            final byte[] cvImage = readFile(dir + COLUMN_VERSION_FILE_NAME);
            final byte[] txnImage = readFile(dir + TXN_FILE_NAME);
            final String[] lastPartitionFiles = listPartitionFiles(dir, ROWS - 1);

            try (CairoEngine restarted = new CairoEngine(configuration)) {
                final SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(restarted);
                try {
                    restarted.execute("insert into x values (" + NEW_ROW_TS + "::timestamp, 's9', 777, 777)", ctx);
                    Assert.fail("the writer must not act on a torn _cv");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "a column file it reads is missing");
                    TestUtils.assertContains(e.getFlyweightMessage(), "Restore the table from a backup or checkpoint");
                }
                assertReadFails(restarted, ctx, "_cv live area is torn");
                restarted.clear();
            }
            // Refusing leaves every file as the crash left it.
            assertFileUnchanged(dir + COLUMN_VERSION_FILE_NAME, cvImage);
            assertFileUnchanged(dir + TXN_FILE_NAME, txnImage);
            Assert.assertArrayEquals(lastPartitionFiles, listPartitionFiles(dir, ROWS - 1));
        });
    }

    @Test
    public void testNonWalTornColumnVersionRollsBackOnFirstRead() throws Exception {
        final TxnFsyncCountingFacade ff = new TxnFsyncCountingFacade();
        assertMemoryLeak(ff, () -> {
            configureCommitMode();
            final String dir = createTable(false);
            final Commits commits = new Commits(dir);
            // A reader on an older transaction keeps the column files each UPDATE replaces, as a live query does.
            try (TableReader ignore = engine.getReader("x")) {
                commits.updateColumnC(false, 3);
            }
            engine.clear();
            final int n = commits.size() - 1;
            installTornColumnVersion(dir, commits.cv(n), commits.cv(n - 1));
            assertNonWalRollsBackOnFirstRead(ff, dir, commits, n);
        });
    }

    @Test
    public void testTornTxnRefusedWhenUpdateRemovedPreviousColumnFiles() throws Exception {
        Assume.assumeFalse("adaptive recovery restores enrolled tables", "adaptive".equals(commitMode));
        assertMemoryLeak(() -> {
            configureCommitMode();
            final String dir = createTable(true);
            final Commits commits = new Commits(dir);
            // Each UPDATE rewrites column c in the last three partitions under a new column name txn and, with
            // no reader holding the versions it replaced, removes them right after its commit.
            commits.updateLastPartitions(true, 6);
            engine.clear();
            installTornTxn(dir, commits);
            final byte[] txnImage = readFile(dir + TXN_FILE_NAME);
            final byte[] cvImage = readFile(dir + COLUMN_VERSION_FILE_NAME);
            final String[] lastPartitionFiles = listPartitionFiles(dir, ROWS - 1);

            try (CairoEngine restarted = new CairoEngine(configuration)) {
                final SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(restarted);
                TestUtils.drainWalQueue(restarted);
                final TableToken token = restarted.verifyTableName("x");
                try (TableWriter ignore = restarted.getWriter(token, "test")) {
                    Assert.fail("the writer must not continue from a transaction whose column files are gone");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "_txn live area is torn; refusing to write from the previous transaction: a column file it reads is missing");
                }
                assertQuery("select suspended from wal_tables()").withEngine(restarted).withContext(ctx).noLeakCheck()
                        .noRandomAccess()
                        .returns("suspended\ntrue\n");
                restarted.clear();
            }
            assertFileUnchanged(dir + TXN_FILE_NAME, txnImage);
            assertFileUnchanged(dir + COLUMN_VERSION_FILE_NAME, cvImage);
            // The writer used to roll back and then create an empty file for the column version it could not find.
            Assert.assertArrayEquals(lastPartitionFiles, listPartitionFiles(dir, ROWS - 1));
        });
    }

    @Test
    public void testTornTxnReplaysUpdateWhenPreviousColumnFilesRemain() throws Exception {
        assertMemoryLeak(() -> {
            configureCommitMode();
            final String dir = createTable(true);
            final Commits commits = new Commits(dir);
            try (TableReader ignore = engine.getReader("x")) {
                commits.updateLastPartitions(true, 6);
            }
            final String expected = selectAll(engine, sqlExecutionContext, "x");
            engine.clear();
            installTornTxn(dir, commits);

            try (CairoEngine restarted = new CairoEngine(configuration)) {
                final SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(restarted);
                TestUtils.drainWalQueue(restarted);
                assertQuery("x").withEngine(restarted).withContext(ctx).noLeakCheck().timestamp("ts").expectSize()
                        .returns(expected);
                assertWalTableCaughtUp(restarted, ctx);
                restarted.clear();
            }
        });
    }

    @Test
    public void testWalTableIngestsAndReplaysOverTornColumnVersion() throws Exception {
        assertMemoryLeak(() -> {
            configureCommitMode();
            final String dir = createTable(true);
            final Commits commits = new Commits(dir);
            try (TableReader ignore = engine.getReader("x")) {
                commits.updateColumnC(true, 3);
            }
            final String expected = selectAll(engine, sqlExecutionContext, "x");
            engine.clear();
            final int n = commits.size() - 1;
            installTornColumnVersion(dir, commits.cv(n), commits.cv(n - 1));

            try (CairoEngine restarted = new CairoEngine(configuration)) {
                final SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(restarted);
                // Bootstrapping a WAL writer reads the table's symbol counts through _cv. A _cv that cannot serve
                // _txn used to fail every INSERT and DDL for good, because nothing then opened the table writer.
                restarted.execute("insert into x values (" + NEW_ROW_TS + "::timestamp, 's9', 777, 777)", ctx);
                TestUtils.drainWalQueue(restarted);
                // The writer continued from commit N-1 and WAL apply replayed commit N: nothing is lost.
                assertQuery("x where ts <> " + NEW_ROW_TS).withEngine(restarted).withContext(ctx).noLeakCheck()
                        .timestamp("ts")
                        .returns(expected);
                assertQuery("select s, v, c from x where ts = " + NEW_ROW_TS).withEngine(restarted).withContext(ctx)
                        .noLeakCheck()
                        .returns("s\tv\tc\ns9\t777\t777\n");
                assertWalTableCaughtUp(restarted, ctx);
                restarted.clear();
            }
        });
    }

    @Test
    public void testWalTableReadRepairsTornColumnVersion() throws Exception {
        assertMemoryLeak(() -> {
            configureCommitMode();
            final String dir = createTable(true);
            final Commits commits = new Commits(dir);
            try (TableReader ignore = engine.getReader("x")) {
                commits.updateColumnC(true, 3);
            }
            final String expected = selectAll(engine, sqlExecutionContext, "x");
            engine.clear();
            final int n = commits.size() - 1;
            installTornColumnVersion(dir, commits.cv(n), commits.cv(n - 1));

            try (CairoEngine restarted = new CairoEngine(configuration)) {
                final SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(restarted);
                // Nothing is pending in the WAL, so no apply job opens the writer. The read has to. Under adaptive,
                // recovery has already restored the table to its enrolment baseline, which WAL apply rolls forward.
                final StringSink sink = new StringSink();
                TestUtils.printSql(restarted, ctx, "select count() from x", sink);
                if (!"adaptive".equals(commitMode)) {
                    TestUtils.assertEquals("count\n" + ROWS + "\n", sink);
                }
                TestUtils.drainWalQueue(restarted);
                assertQuery("x").withEngine(restarted).withContext(ctx).noLeakCheck().timestamp("ts").expectSize()
                        .returns(expected);
                assertWalTableCaughtUp(restarted, ctx);
                restarted.clear();
            }
        });
    }

    @Test
    public void testWalTableSuspendsOnTornColumnVersionItCannotRollBack() throws Exception {
        Assume.assumeFalse("adaptive recovery restores enrolled tables", "adaptive".equals(commitMode));
        assertMemoryLeak(() -> {
            configureCommitMode();
            final String dir = createTable(true);
            final Commits commits = new Commits(dir);
            commits.updateColumnC(true, 3);
            // An append after the UPDATE: both _txn records now name the torn column version.
            execute("insert into x values (" + (T0 + (ROWS - 1) * DAY + HOUR) + "::timestamp, 's1', 0, 0)");
            drainWalQueue();
            engine.clear();
            final int n = commits.size() - 1;
            Assert.assertArrayEquals("the append must not change _cv", commits.cv(n), readFile(dir + COLUMN_VERSION_FILE_NAME));
            installTornColumnVersion(dir, commits.cv(n), commits.cv(n - 1));
            final byte[] cvImage = readFile(dir + COLUMN_VERSION_FILE_NAME);
            final byte[] txnImage = readFile(dir + TXN_FILE_NAME);

            try (CairoEngine restarted = new CairoEngine(configuration)) {
                final SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(restarted);
                // The WAL keeps taking rows while the table cannot apply them, as for any suspended table.
                restarted.execute("insert into x values (" + NEW_ROW_TS + "::timestamp, 's9', 777, 777)", ctx);
                TestUtils.drainWalQueue(restarted);
                final StringSink sink = new StringSink();
                TestUtils.printSql(restarted, ctx, "select suspended, errorMessage from wal_tables()", sink);
                TestUtils.assertContains(sink, "true\t");
                TestUtils.assertContains(sink, "Restore the table from a backup or checkpoint");
                assertReadFails(restarted, ctx, "_cv live area is torn");
                restarted.clear();
            }
            assertFileUnchanged(dir + COLUMN_VERSION_FILE_NAME, cvImage);
            assertFileUnchanged(dir + TXN_FILE_NAME, txnImage);
        });
    }

    private static void assertFileUnchanged(String path, byte[] image) throws IOException {
        final byte[] after = readFile(path);
        Assert.assertTrue(path, after.length >= image.length && Arrays.equals(image, 0, image.length, after, 0, image.length));
    }

    private static void assertReadFails(CairoEngine engine, SqlExecutionContext ctx, String message) {
        try {
            TestUtils.printSql(engine, ctx, "select count() from x", new StringSink());
            Assert.fail("a reader must not serve a _cv that cannot serve _txn");
        } catch (CairoException | SqlException e) {
            TestUtils.assertContains(((FlyweightMessageContainer) e).getFlyweightMessage(), message);
        }
    }

    // The area geometry the header gives column version areaVersion: {offset, size}.
    private static long[] cvArea(byte[] file, long areaVersion) {
        final ByteBuffer bb = ByteBuffer.wrap(file).order(ByteOrder.LITTLE_ENDIAN);
        final boolean isA = (areaVersion & 1) == 0;
        return new long[]{
                bb.getLong(isA ? ColumnVersionReader.OFFSET_OFFSET_A_64 : ColumnVersionReader.OFFSET_OFFSET_B_64),
                bb.getLong(isA ? ColumnVersionReader.OFFSET_SIZE_A_64 : ColumnVersionReader.OFFSET_SIZE_B_64)
        };
    }

    private static long cvVersion(byte[] file) {
        return ByteBuffer.wrap(file).order(ByteOrder.LITTLE_ENDIAN).getLong(ColumnVersionReader.OFFSET_VERSION_64);
    }

    // Builds the image a power loss leaves when every page of _cv reached the disk after commit V except one page
    // that lies wholly inside V's area: it keeps what it held before V. Computed apart from ColumnVersionReader,
    // so the test checks the image it builds rather than the code under test.
    private static void installTornColumnVersion(String dir, byte[] fresh, byte[] stale) throws IOException {
        final long version = cvVersion(fresh);
        final long[] area = cvArea(fresh, version);
        final long lo = (area[0] + PAGE - 1) / PAGE;
        final long hi = (area[0] + area[1]) / PAGE;
        for (long page = Math.max(lo, 1); page < hi; page++) {
            final int from = (int) (page * PAGE);
            if (from + PAGE <= stale.length && !Arrays.equals(fresh, from, from + PAGE, stale, from, from + PAGE)) {
                final byte[] image = fresh.clone();
                System.arraycopy(stale, from, image, from, PAGE);
                Assert.assertTrue("the live area must carry its own checksum stamp", isCvAreaStamped(image, version));
                Assert.assertFalse("the live area must be torn", isCvAreaVerified(image, version));
                Assert.assertTrue("the previous area must be intact", isCvAreaVerified(image, version - 1));
                writeFile(dir + COLUMN_VERSION_FILE_NAME, image);
                return;
            }
        }
        Assert.fail("no page of the live _cv area differs from what the file held before the commit");
    }

    // Builds the image a power loss leaves when the first page of _txn reached the disk after commit N, and the
    // rest kept what it held after commit N-1. Takes the latest commit whose live area that tears, placed and
    // sized like the one two commits back, so the stale bytes belong to that older record.
    private static void installTornTxn(String dir, Commits commits) throws IOException {
        for (int n = commits.size() - 1; n > 1; n--) {
            final byte[] torn = commits.txn(n);
            final byte[] previous = commits.txn(n - 1);
            final byte[] older = commits.txn(n - 2);
            if (torn.length <= PAGE || torn.length != previous.length
                    || txnLiveAreaOffset(torn) != txnLiveAreaOffset(older)
                    || txnLiveAreaPartitionsSize(torn) != txnLiveAreaPartitionsSize(older)) {
                continue;
            }
            final byte[] image = torn.clone();
            System.arraycopy(previous, PAGE, image, PAGE, torn.length - PAGE);
            final long version = ByteBuffer.wrap(image).order(ByteOrder.LITTLE_ENDIAN).getLong((int) TX_BASE_OFFSET_VERSION_64);
            if (isTxnAreaVerified(image, version) || !isTxnAreaVerified(image, version - 1)) {
                continue;
            }
            // The torn commit is an UPDATE: _cv holds its column version intact.
            Assert.assertTrue(isCvAreaVerified(commits.cv(n), cvVersion(commits.cv(n))));
            writeFile(dir + TXN_FILE_NAME, image);
            writeFile(dir + COLUMN_VERSION_FILE_NAME, commits.cv(n));
            return;
        }
        Assert.fail("no commit tore the live _txn area over an intact previous area");
    }

    private static boolean isCvAreaStamped(byte[] file, long areaVersion) {
        final long[] area = cvArea(file, areaVersion);
        return area[0] + area[1] + TableUtils.CV_CHECKSUM_TRAILER_SIZE <= file.length
                && ByteBuffer.wrap(file).order(ByteOrder.LITTLE_ENDIAN).getLong((int) (area[0] + area[1])) == (TableUtils.CV_CHECKSUM_MAGIC ^ areaVersion);
    }

    private static boolean isCvAreaVerified(byte[] file, long areaVersion) {
        if (!isCvAreaStamped(file, areaVersion)) {
            return false;
        }
        final long[] area = cvArea(file, areaVersion);
        final int offset = (int) area[0];
        final int size = (int) area[1];
        final long address = Unsafe.malloc(Math.max(size, 1), MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < size; i++) {
                Unsafe.getUnsafe().putByte(address + i, file[offset + i]);
            }
            return TableUtils.calculateCvAreaChecksum(address, size)
                    == ByteBuffer.wrap(file).order(ByteOrder.LITTLE_ENDIAN).getLong(offset + size + Long.BYTES);
        } finally {
            Unsafe.free(address, Math.max(size, 1), MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static boolean isTxnAreaVerified(byte[] file, long areaVersion) {
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
        final long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < size; i++) {
                Unsafe.getUnsafe().putByte(address + i, file[offset + i]);
            }
            return bb.getLong(offset + (int) TX_OFFSET_BODY_CHECKSUM_64) == TableUtils.calculateTxnBodyChecksum(address, size, TX_RECORD_HEADER_SIZE + symbolsSize);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static String[] listPartitionFiles(String dir, int partition) {
        final String prefix = java.time.LocalDate.of(2024, 1, 1).plusDays(partition).toString();
        final File[] partitions = new File(dir).listFiles((d, name) -> name.startsWith(prefix));
        Assert.assertNotNull(partitions);
        final ObjList<String> names = new ObjList<>();
        for (File partitionDir : partitions) {
            final String[] files = partitionDir.list();
            if (files != null) {
                for (String file : files) {
                    names.add(partitionDir.getName() + '/' + file);
                }
            }
        }
        final String[] sorted = new String[names.size()];
        for (int i = 0; i < sorted.length; i++) {
            sorted[i] = names.getQuick(i);
        }
        Arrays.sort(sorted);
        return sorted;
    }

    private static byte[] readFile(String path) throws IOException {
        return java.nio.file.Files.readAllBytes(Paths.get(path));
    }

    private static String selectAll(CairoEngine engine, SqlExecutionContext ctx, String sql) throws SqlException {
        final StringSink sink = new StringSink();
        TestUtils.printSql(engine, ctx, sql, sink);
        return sink.toString();
    }

    private static int txnLiveAreaOffset(byte[] file) {
        final ByteBuffer bb = ByteBuffer.wrap(file).order(ByteOrder.LITTLE_ENDIAN);
        return bb.getInt((int) ((bb.getLong((int) TX_BASE_OFFSET_VERSION_64) & 1) == 0 ? TX_BASE_OFFSET_A_32 : TX_BASE_OFFSET_B_32));
    }

    private static int txnLiveAreaPartitionsSize(byte[] file) {
        final ByteBuffer bb = ByteBuffer.wrap(file).order(ByteOrder.LITTLE_ENDIAN);
        final boolean isA = (bb.getLong((int) TX_BASE_OFFSET_VERSION_64) & 1) == 0;
        return bb.getInt((int) (isA ? TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 : TX_BASE_OFFSET_PARTITIONS_SIZE_B_32));
    }

    private static void writeFile(String path, byte[] image) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(path, "rw")) {
            file.setLength(image.length);
            file.seek(0);
            file.write(image);
        }
    }

    private void assertNonWalRollsBackOnFirstRead(TxnFsyncCountingFacade ff, String dir, Commits commits, int n) throws Exception {
        final long txnVersion = ByteBuffer.wrap(readFile(dir + TXN_FILE_NAME)).order(ByteOrder.LITTLE_ENDIAN).getLong((int) TX_BASE_OFFSET_VERSION_64);
        ff.txnFsyncCount = 0;
        try (CairoEngine restarted = new CairoEngine(configuration)) {
            final SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(restarted);
            // The first query cannot read commit N, so the engine opens the table's writer to repair the table,
            // which continues from commit N-1. A non-WAL table has nothing to replay commit N from.
            assertQuery("x").withEngine(restarted).withContext(ctx).noLeakCheck().timestamp("ts").expectSize()
                    .returns(commits.scan(n - 1));
            Assert.assertEquals("the rollback must be durable whatever the commit mode", 1, ff.txnFsyncCount);

            // The reader registered commit N with the scoreboard before it failed, so the scoreboard refuses commit
            // N-1. The writer published commit N-1's state under txn N.
            try (Path path = new Path(); TxReader reader = new TxReader(configuration.getFilesFacade())) {
                reader.ofRO(path.of(dir).concat(TXN_FILE_NAME).$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
                Assert.assertTrue(reader.unsafeLoadAll());
                Assert.assertEquals(txnVersion, reader.getTxn());
                Assert.assertEquals(cvVersion(commits.cv(n - 1)), reader.getColumnVersion());
            }

            restarted.execute("insert into x values (" + NEW_ROW_TS + "::timestamp, 's9', 777, 777)", ctx);
            restarted.execute("alter table x add column f int", ctx);
            assertQuery("select s, v, c, f from x where ts = " + NEW_ROW_TS).withEngine(restarted).withContext(ctx)
                    .noLeakCheck()
                    .returns("s\tv\tc\tf\ns9\t777\t777\tnull\n");
            assertQuery("select count() from x").withEngine(restarted).withContext(ctx).noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n" + (ROWS + 1) + "\n");
            restarted.clear();
        }
    }

    private void assertWalTableCaughtUp(CairoEngine restarted, SqlExecutionContext ctx) throws Exception {
        assertQuery("select suspended, writerTxn = sequencerTxn caught_up from wal_tables()")
                .withEngine(restarted).withContext(ctx).noLeakCheck().noRandomAccess()
                .returns("suspended\tcaught_up\nfalse\ttrue\n");
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

    // One row per day for ROWS days, then an UPDATE of v and one of c: every partition gets a _cv entry for both
    // columns, so each _cv area spans five pages.
    private String createTable(boolean isWal) throws SqlException {
        execute("create table x (ts timestamp, s symbol, v long, c int) timestamp(ts) partition by DAY" + (isWal ? " WAL" : " BYPASS WAL"));
        execute("insert into x select timestamp_sequence(" + T0 + ", " + DAY + ") ts, ('s' || (x % 3))::symbol s, x v, x::int c from long_sequence(" + ROWS + ")");
        drain(isWal);
        execute("update x set v = v + 1");
        drain(isWal);
        execute("update x set c = c + 1");
        drain(isWal);
        final TableToken token = engine.verifyTableName("x");
        return configuration.getDbRoot() + Files.SEPARATOR + token.getDirName() + Files.SEPARATOR;
    }

    private void drain(boolean isWal) {
        if (isWal) {
            drainWalQueue();
        }
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

    // Page-cache snapshots of _cv and _txn, and a scan of the table, after each commit.
    private class Commits {
        private final ObjList<byte[]> cvSnapshots = new ObjList<>();
        private final String dir;
        private final ObjList<String> scans = new ObjList<>();
        private final ObjList<byte[]> txnSnapshots = new ObjList<>();

        private Commits(String dir) throws Exception {
            this.dir = dir;
            snapshot();
        }

        private byte[] cv(int commit) {
            return cvSnapshots.getQuick(commit);
        }

        private String scan(int commit) {
            return scans.getQuick(commit);
        }

        private int size() {
            return cvSnapshots.size();
        }

        private void snapshot() throws Exception {
            cvSnapshots.add(readFile(dir + COLUMN_VERSION_FILE_NAME));
            txnSnapshots.add(readFile(dir + TXN_FILE_NAME));
            scans.add(selectAll(engine, sqlExecutionContext, "x"));
        }

        private byte[] txn(int commit) {
            return txnSnapshots.getQuick(commit);
        }

        // Rewrites column c in every partition: each commit changes every c entry of _cv, so every page of the
        // new area differs from what the same slot held two commits back.
        private void updateColumnC(boolean isWal, int count) throws Exception {
            for (int k = 0; k < count; k++) {
                execute("update x set c = c + 1");
                drain(isWal);
                snapshot();
            }
        }

        private void updateLastPartitions(boolean isWal, int count) throws Exception {
            for (int k = 0; k < count; k++) {
                execute("update x set c = c + 1 where ts > " + (T0 + (ROWS - 4) * DAY));
                drain(isWal);
                snapshot();
            }
        }
    }
}
