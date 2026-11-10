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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.ColumnVersionWriter;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.TableUtils.COLUMN_VERSION_FILE_NAME;
import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

/**
 * A commit publishes {@code _cv} before the {@code _txn} record that names its version, and readers wait for
 * {@code _txn} to name it. A commit that stops between the two leaves {@code _cv} a version ahead: WAL apply gives
 * the writer of a failed commit up distressed and suspends the table, which opens no other writer until RESUME WAL,
 * and a process that dies there leaves no writer at all.
 */
@RunWith(Parameterized.class)
public class ColumnVersionAheadOfTxnTest extends AbstractCairoTest {
    // the table after its pending commit applies: 200 rows of 'a' from before column c, 600 of 'b' with c = v
    private static final String ALL_ROWS = """
            s\tn\tnc\tsc\tlo\thi
            a\t200\t0\tnull\t2024-01-01T00:00:00.000000Z\t2024-07-18T00:00:00.000000Z
            b\t600\t600\t180300\t2024-01-01T12:00:00.000000Z\t2025-08-22T00:00:00.000000Z
            """;
    // the table at its last commit, when the pending commit failed
    private static final String COMMITTED_ROWS = """
            s\tn\tnc\tsc\tlo\thi
            a\t200\t0\tnull\t2024-01-01T00:00:00.000000Z\t2024-07-18T00:00:00.000000Z
            """;
    private static final String INSERT_SENT_ROW = "INSERT INTO x VALUES ('n', 0, '2024-06-01T06:00:00.000000Z'::TIMESTAMP, 0)";
    private static final String ROWS_QUERY = "SELECT s, count() n, count(c) nc, sum(c) sc, min(ts) lo, max(ts) hi FROM x ORDER BY s";
    private static final String SENT_ROW = "n\t1\t1\t0\t2024-06-01T06:00:00.000000Z\t2024-06-01T06:00:00.000000Z\n";
    private static final LogCapture capture = new LogCapture();
    private final String commitMode;

    public ColumnVersionAheadOfTxnTest(String commitMode) {
        this.commitMode = commitMode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{"nosync"}, {"async"}, {"sync"}, {"adaptive"}});
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        capture.start();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        capture.stop();
        super.tearDown();
    }

    @Test
    public void testDropTableAfterFailedApply() throws Exception {
        // the purge of a dropped WAL table opens the table's writer, and destroys it rather than close it
        final ApplyFaultFacade ff = new ApplyFaultFacade();
        assertMemoryLeak(ff, () -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
            final TableToken token = createTableWithPendingCommit();
            failPendingCommit(ff, TXN_FILE_NAME, Fault.ALLOCATE, "No space left");
            assertColumnVersionMatchesTxn(token);
            execute("DROP TABLE x");
            drainWalQueue();
            Assert.assertNull(engine.getTableTokenIfExists("x"));
            capture.drain();
            capture.assertLogged("closing table files [table=" + token.getDirName());
            // the writer of the failed commit rolled _cv back, the one the purge opened found nothing to roll back
            capture.assertOnlyOnce("rolling back column version of failed commit");
            capture.assertNotLogged("rolling back column version change to match _txn file");
            capture.assertNotLogged("could not roll back column version of failed commit");
        });
    }

    @Test
    public void testFailedApplyKeepsColumnVersionThatNeverPublished() throws Exception {
        // disk full while the commit grows _cv: _cv never published
        assertFailedApplyKeepsUnpublishedColumnVersion(Fault.ALLOCATE, "No space left");
    }

    @Test
    public void testFailedApplyKeepsColumnVersionThatNeverPublishedWhenCvUnmapped() throws Exception {
        // mremap fails while the commit grows _cv: _cv never published, and the writer gives the table up without a
        // _cv mapping
        assertFailedApplyKeepsUnpublishedColumnVersion(Fault.MREMAP, "could not remap file");
    }

    @Test
    public void testFailedApplyReleasesWriterWhenColumnVersionRollbackFails() throws Exception {
        // the rollback of _cv fails in turn: the writer still closes, frees what it holds and releases the table
        final ApplyFaultFacade ff = new ApplyFaultFacade();
        assertMemoryLeak(ff, () -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
            final TableToken token = createTableWithPendingCommit();
            ff.isCvLengthFailureAfterFault = true;
            failPendingCommit(ff, TXN_FILE_NAME, Fault.ALLOCATE, "No space left");
            capture.drain();
            capture.assertLogged("rolling back column version of failed commit");
            capture.assertLogged("could not roll back column version of failed commit");
            capture.assertLogged("injected fstat failure");
            assertTableResumes(token, "");
        });
    }

    @Test
    public void testFailedApplyRollsColumnVersionBack() throws Exception {
        // disk full while the commit grows _txn
        assertFailedApplyRollsColumnVersionBack(Fault.ALLOCATE, "No space left");
    }

    @Test
    public void testFailedApplyRollsColumnVersionBackWhenTxnRemapFails() throws Exception {
        // mremap fails while the commit grows _txn: the writer keeps its _txn mapping, and rolls _cv back as for disk
        // full
        assertFailedApplyRollsColumnVersionBack(Fault.MREMAP, "could not remap file");
    }

    @Test
    public void testWalWriterRollsSegmentAfterCommitInFlight() throws Exception {
        // A WAL writer that rolls its segment while a commit has published _cv and not _txn waits for the commit,
        // then takes the symbol counts of that commit, short of any deadline.
        assertWalWriterWaitsForCommitInFlight(true);
    }

    @Test
    public void testWalWriterStopsWaitingForColumnVersionAheadOfTxn() throws Exception {
        // A process that died between the _cv and _txn commits leaves _cv ahead and no writer to roll it back.
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
            execute("CREATE TABLE x (s SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES ('a', 1, '2024-01-01T00:00:00.000000Z'::TIMESTAMP), ('b', 2, '2024-01-02T00:00:00.000000Z'::TIMESTAMP)");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("x");
            engine.releaseAllWalWriters();
            try (WalWriter openWalWriter = engine.getWalWriter(token)) {
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                publishColumnVersionAheadOfTxn(token);
                skipColumnVersionWaits();
                // the open WAL writer rolls its segment with the symbol counts it has
                assertReturns(token, openWalWriter::rollSegment);
                Assert.assertEquals(2, openWalWriter.getSymbolCountWatermark(0));
                assertReturns(token, () -> {
                    try (WalWriter walWriter = engine.getWalWriter(token)) {
                        Assert.assertEquals(0, walWriter.getSymbolCountWatermark(0));
                    }
                });
            }
            capture.drain();
            capture.assertLogged("_cv stays ahead of _txn, keeping symbol watermarks");
            capture.assertLogged("_cv stays ahead of _txn, writing symbols in full");

            // the commit wakes WAL apply, and its writer rolls _cv back as it opens the table
            assertReturns(token, () -> execute("INSERT INTO x VALUES ('c', 3, '2024-01-03T00:00:00.000000Z'::TIMESTAMP)"));
            drainWalQueue();
            assertColumnVersionMatchesTxn(token);
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT s, v, ts FROM x").noLeakCheck().timestamp("ts").expectSize().returns("""
                    s\tv\tts
                    a\t1\t2024-01-01T00:00:00.000000Z
                    b\t2\t2024-01-02T00:00:00.000000Z
                    c\t3\t2024-01-03T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testWalWriterWaitsForCommitInFlight() throws Exception {
        // A WAL writer that opens while a commit has published _cv and not _txn waits for the commit, then reads
        // the table's symbol table at that commit, short of any deadline.
        assertWalWriterWaitsForCommitInFlight(false);
    }

    private static void assertColumnVersionMatchesTxn(TableToken token) {
        Assert.assertEquals(readTxnColumnVersion(token), readColumnVersionFileVersion(token));
    }

    // Fails, rather than hangs, when the code waits for good for _txn to name _cv
    private static void assertReturns(TableToken token, TestUtils.LeakProneCode code) throws Exception {
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final Thread thread = new Thread(() -> {
            try {
                code.run();
            } catch (Throwable th) {
                error.set(th);
            } finally {
                Path.clearThreadLocals();
            }
        });
        thread.start();
        thread.join(TimeUnit.SECONDS.toMillis(60));
        if (thread.isAlive()) {
            // release it as the next table writer would, so that it does not outlive the test
            try (
                    Path path = new Path().of(configuration.getDbRoot()).concat(token).concat(COLUMN_VERSION_FILE_NAME);
                    ColumnVersionWriter columnVersionWriter = new ColumnVersionWriter(configuration, path.$(), true)
            ) {
                columnVersionWriter.rollback();
            }
            thread.join();
            Assert.fail("still waits for _txn to name the _cv version");
        }
        rethrow(error);
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(60, TimeUnit.SECONDS)) {
                throw new AssertionError("latch timeout");
            }
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    private static AtomicInteger countColumnVersionWaits() {
        final AtomicInteger count = new AtomicInteger();
        final MicrosecondClock clock = testMicrosClock;
        testMicrosClock = () -> {
            if (isCalledFromColumnVersionWait()) {
                count.incrementAndGet();
            }
            return clock.getTicks();
        };
        return count;
    }

    private static boolean isCalledFrom(Class<?> clazz, String methodName) {
        return StackWalker.getInstance().walk(frames -> frames
                .filter(f -> !f.getClassName().startsWith(ColumnVersionAheadOfTxnTest.class.getName())
                        && !f.getClassName().startsWith(CairoTestConfiguration.class.getName()))
                .findFirst()
                .map(f -> clazz.getName().equals(f.getClassName()) && methodName.equals(f.getMethodName()))
                .orElse(false));
    }

    // Whether a WAL writer reads the clock to check the deadline of its wait for _txn to name the _cv version
    private static boolean isCalledFromColumnVersionWait() {
        return isCalledFrom(WalWriter.class, "isColumnVersionWaitOver");
    }

    // a commit that published _cv and stopped before its _txn commit, as a process that dies there leaves it
    private static void publishColumnVersionAheadOfTxn(TableToken token) {
        final long partitionTimestamp;
        try (TableReader reader = engine.getReader(token)) {
            partitionTimestamp = reader.getPartitionTimestampByIndex(0);
        }
        engine.releaseAllReaders();
        try (
                Path path = new Path().of(configuration.getDbRoot()).concat(token).concat(COLUMN_VERSION_FILE_NAME);
                ColumnVersionWriter columnVersionWriter = new ColumnVersionWriter(configuration, path.$(), true)
        ) {
            columnVersionWriter.upsert(partitionTimestamp, 1, -1, 0);
            columnVersionWriter.commit();
        }
        Assert.assertEquals(readTxnColumnVersion(token) + 1, readColumnVersionFileVersion(token));
    }

    private static long readColumnVersionFileVersion(TableToken token) {
        try (
                Path path = new Path().of(configuration.getDbRoot()).concat(token).concat(COLUMN_VERSION_FILE_NAME);
                ColumnVersionReader columnVersionReader = new ColumnVersionReader().ofRO(configuration.getFilesFacade(), path.$())
        ) {
            return columnVersionReader.readUnsafe();
        }
    }

    private static long readTxnColumnVersion(TableToken token) {
        try (
                Path path = new Path().of(configuration.getDbRoot()).concat(token).concat(TXN_FILE_NAME);
                TxReader txReader = new TxReader(configuration.getFilesFacade())
        ) {
            txReader.ofRO(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
            Assert.assertTrue(txReader.unsafeLoadAll());
            return txReader.getColumnVersion();
        }
    }

    private static void rethrow(AtomicReference<Throwable> error) {
        if (error.get() != null) {
            throw new AssertionError(error.get());
        }
    }

    // Moves time past the deadline of every wait for _txn to name the _cv version, as when it is not going to
    private static void skipColumnVersionWaits() {
        final MicrosecondClock clock = testMicrosClock;
        testMicrosClock = () -> isCalledFromColumnVersionWait() || isCalledFrom(TableReader.class, "reloadColumnVersion")
                ? Long.MAX_VALUE
                : clock.getTicks();
    }

    private void assertFailedApplyKeepsUnpublishedColumnVersion(Fault fault, String error) throws Exception {
        final ApplyFaultFacade ff = new ApplyFaultFacade();
        assertMemoryLeak(ff, () -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
            final TableToken token = createTableWithPendingCommit();
            final long columnVersion = readColumnVersionFileVersion(token);
            failPendingCommit(ff, COLUMN_VERSION_FILE_NAME, fault, error);
            Assert.assertEquals(columnVersion, readColumnVersionFileVersion(token));
            assertColumnVersionMatchesTxn(token);
            capture.drain();
            capture.assertNotLogged("rolling back column version");
            capture.assertNotLogged("could not roll back column version of failed commit");

            final AtomicInteger waitCount = countColumnVersionWaits();
            engine.releaseAllReaders();
            assertRows(COMMITTED_ROWS);
            engine.releaseAllWalWriters();
            try (WalWriter walWriter = engine.getWalWriter(token)) {
                Assert.assertEquals(1, walWriter.getSymbolCountWatermark(0));
            }
            Assert.assertEquals(0, waitCount.get());
            assertTableResumes(token, "");
        });
    }

    private void assertFailedApplyRollsColumnVersionBack(Fault fault, String error) throws Exception {
        final ApplyFaultFacade ff = new ApplyFaultFacade();
        assertMemoryLeak(ff, () -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
            final TableToken token = createTableWithPendingCommit();
            failPendingCommit(ff, TXN_FILE_NAME, fault, error);
            // the writer rolled _cv back as it gave the table up, before the table got suspended
            assertColumnVersionMatchesTxn(token);
            capture.drain();
            capture.assertOnlyOnce("rolling back column version of failed commit");
            capture.assertNotLogged("could not roll back column version of failed commit");

            // no reader or WAL writer waits for the failed commit's _txn
            final AtomicInteger waitCount = countColumnVersionWaits();
            engine.releaseAllReaders();
            assertRows(COMMITTED_ROWS);
            engine.releaseAllWalWriters();
            try (WalWriter walWriter = engine.getWalWriter(token)) {
                // the table's symbol table at its last commit: 'a'
                Assert.assertEquals(1, walWriter.getSymbolCountWatermark(0));
                walWriter.rollSegment();
                Assert.assertEquals(1, walWriter.getSymbolCountWatermark(0));
            }
            execute(INSERT_SENT_ROW);
            Assert.assertEquals(0, waitCount.get());
            capture.drain();
            capture.assertNotLogged("_cv stays ahead of _txn");
            assertTableResumes(token, SENT_ROW);
            // the writer RESUME WAL opened found nothing to roll back
            capture.drain();
            capture.assertNotLogged("rolling back column version change to match _txn file");
        });
    }

    private void assertRows(String expected) throws Exception {
        assertQuery(ROWS_QUERY).noLeakCheck().expectSize().returns(expected);
    }

    private void assertTableResumes(TableToken token, String sentRows) throws Exception {
        execute("ALTER TABLE x RESUME WAL");
        drainWalQueue();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
        assertColumnVersionMatchesTxn(token);
        assertRows(ALL_ROWS + sentRows);
    }

    private void assertWalWriterWaitsForCommitInFlight(boolean isRollingSegment) throws Exception {
        final ApplyFaultFacade ff = new ApplyFaultFacade();
        assertMemoryLeak(ff, () -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
            final TableToken token = createTableWithPendingCommit();
            engine.releaseAllWalWriters();
            // a WAL writer opened at the last commit, which knows 'a' only
            final WalWriter openWalWriter = isRollingSegment ? engine.getWalWriter(token) : null;
            try {
                if (openWalWriter != null) {
                    Assert.assertEquals(1, openWalWriter.getSymbolCountWatermark(0));
                }
                final AtomicInteger watermark = new AtomicInteger(-1);
                final AtomicReference<Throwable> walWriterError = new AtomicReference<>();
                final Thread walWriterThread = new Thread(() -> {
                    try {
                        if (openWalWriter != null) {
                            openWalWriter.rollSegment();
                            watermark.set(openWalWriter.getSymbolCountWatermark(0));
                        } else {
                            try (WalWriter walWriter = engine.getWalWriter(token)) {
                                watermark.set(walWriter.getSymbolCountWatermark(0));
                            }
                        }
                    } catch (Throwable th) {
                        walWriterError.set(th);
                    } finally {
                        Path.clearThreadLocals();
                    }
                });
                final CountDownLatch walWriterWaits = new CountDownLatch(1);
                final CountDownLatch walWriterRelease = new CountDownLatch(1);
                final AtomicBoolean isStallArmed = new AtomicBoolean(true);
                final MicrosecondClock clock = testMicrosClock;
                // Stalls the WAL writer on its first check of the wait deadline, which it reaches only after it found
                // _cv ahead of _txn. Time does not pass for the check while it stalls.
                testMicrosClock = () -> {
                    final long ticks = clock.getTicks();
                    if (Thread.currentThread() == walWriterThread && isCalledFromColumnVersionWait() && isStallArmed.compareAndSet(true, false)) {
                        walWriterWaits.countDown();
                        awaitLatch(walWriterRelease);
                    }
                    return ticks;
                };
                // The commit has published _cv and is about to grow _txn: the WAL writer reads the table then
                final AtomicReference<String> versionsInFlight = new AtomicReference<>();
                ff.pause = () -> {
                    versionsInFlight.set("txn=" + readTxnColumnVersion(token) + ", cv=" + readColumnVersionFileVersion(token));
                    walWriterThread.start();
                    awaitLatch(walWriterWaits);
                };
                ff.arm(TXN_FILE_NAME, Fault.PAUSE);
                try {
                    drainWalQueue();
                } finally {
                    walWriterRelease.countDown();
                    if (walWriterThread.getState() != Thread.State.NEW) {
                        walWriterThread.join();
                    }
                    testMicrosClock = clock;
                }
                rethrow(walWriterError);
                Assert.assertEquals(1, ff.faultCount.get());
                final long columnVersion = readColumnVersionFileVersion(token);
                Assert.assertEquals("txn=" + (columnVersion - 1) + ", cv=" + columnVersion, versionsInFlight.get());
                // 'a' and the 'b' of the commit it waited for
                Assert.assertEquals(2, watermark.get());
            } finally {
                Misc.free(openWalWriter);
            }
            capture.drain();
            capture.assertNotLogged("_cv stays ahead of _txn");
            assertColumnVersionMatchesTxn(token);
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertRows(ALL_ROWS);
        });
    }

    // One WAL transaction that merges a row into each of 200 day partitions written before column c, which gives
    // them column tops in _cv, and adds 400 partitions to the table: its commit grows both _cv and _txn.
    private TableToken createTableWithPendingCommit() throws SqlException {
        execute("CREATE TABLE x (s SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO x (s, v, ts) SELECT 'a', x::INT, dateadd('d', (x - 1)::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP) FROM long_sequence(200)");
        drainWalQueue();
        execute("ALTER TABLE x ADD COLUMN c INT");
        drainWalQueue();
        execute("""
                INSERT INTO x (s, v, ts, c)
                SELECT 'b', x::INT,
                dateadd('h', CASE WHEN x <= 200 THEN 12 ELSE 0 END, dateadd('d', (x - 1)::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP)),
                x::INT
                FROM long_sequence(600)""");
        return engine.verifyTableName("x");
    }

    private void failPendingCommit(ApplyFaultFacade ff, String fileName, Fault fault, String error) throws Exception {
        ff.arm(fileName, fault);
        drainWalQueue();
        Assert.assertEquals(1, ff.faultCount.get());
        final StringSink sink = new StringSink();
        TestUtils.printSql(engine, sqlExecutionContext, "SELECT suspended, errorMessage FROM wal_tables()", sink);
        TestUtils.assertContains(sink, "true\t" + error);
    }

    private enum Fault {
        NONE, ALLOCATE, MREMAP, PAUSE
    }

    // Faults the first growth of _txn or _cv of table x while armed: allocate() or mremap() fails, or, for PAUSE,
    // allocate() runs the test's code first, inside the commit.
    private static final class ApplyFaultFacade extends TestFilesFacadeImpl {
        private final Set<Long> cvFds = ConcurrentHashMap.newKeySet();
        private final AtomicInteger faultCount = new AtomicInteger();
        private final Set<Long> txnFds = ConcurrentHashMap.newKeySet();
        private volatile Fault fault = Fault.NONE;
        private volatile String faultFileName;
        // fails the next length() of the _cv the faulted commit published, which the rollback of _cv reads
        private volatile boolean isCvLengthFailureAfterFault;
        private volatile boolean isCvLengthFailureArmed;
        private volatile Runnable pause;

        @Override
        public boolean allocate(long fd, long size) {
            if (isFaulted(fd, Fault.ALLOCATE)) {
                return false;
            }
            if (isFaulted(fd, Fault.PAUSE)) {
                pause.run();
            }
            return super.allocate(fd, size);
        }

        @Override
        public boolean close(long fd) {
            cvFds.remove(fd);
            txnFds.remove(fd);
            return super.close(fd);
        }

        @Override
        public long length(long fd) {
            if (isCvLengthFailureArmed && cvFds.contains(fd)) {
                isCvLengthFailureArmed = false;
                throw CairoException.critical(5).put("injected fstat failure [fd=").put(fd).put(']');
            }
            return super.length(fd);
        }

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            if (isFaulted(fd, Fault.MREMAP)) {
                return FilesFacade.MAP_FAILED;
            }
            return super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (fd > -1 && Utf8s.containsAscii(name, Files.SEPARATOR + "x~")) {
                if (Utf8s.endsWithAscii(name, Files.SEPARATOR + TXN_FILE_NAME)) {
                    txnFds.add(fd);
                } else if (Utf8s.endsWithAscii(name, Files.SEPARATOR + COLUMN_VERSION_FILE_NAME)) {
                    cvFds.add(fd);
                }
            }
            return fd;
        }

        void arm(String fileName, Fault fault) {
            this.faultFileName = fileName;
            this.fault = fault;
        }

        private boolean isFaulted(long fd, Fault kind) {
            if (fault != kind || !(TXN_FILE_NAME.equals(faultFileName) ? txnFds : cvFds).contains(fd)) {
                return false;
            }
            fault = Fault.NONE;
            faultCount.incrementAndGet();
            isCvLengthFailureArmed = isCvLengthFailureAfterFault;
            return true;
        }
    }
}
