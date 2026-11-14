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

package io.questdb.test.cutlass.websocket;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Measurements for the QWP commit batch's role-switch lock hold, not run in CI: run with
 * {@code -Dquestdb.measure.e1=true -Djava.io.tmpdir=<a directory on a real disk>}. The test root lives under
 * {@code java.io.tmpdir}, which is tmpfs on many hosts, and tmpfs voids SYNC and ADAPTIVE numbers.
 * <p>
 * It uses only what the base of the change has, so the same file measures the base: the engine wraps the
 * role-switch read lock to time each hold on the ingress thread, and a receive pass of one-row frames, one per
 * table, makes one batch. Each line it prints starts with {@code E1-MEASURE}.
 */
public class QwpCommitBatchLockHoldMeasurementTest extends AbstractCairoTest {
    private static final int REPEATS = Integer.getInteger("questdb.measure.e1.repeats", 3);
    private static final long T0 = 1_000_000_000L;
    private static final int TABLES = Integer.getInteger("questdb.measure.e1.tables", 1_000);
    private static final long WINDOW_US = 50_000;

    @Before
    public void assumeMeasurement() {
        Assume.assumeTrue("a measurement, run with -Dquestdb.measure.e1=true", Boolean.getBoolean("questdb.measure.e1"));
    }

    @Test
    public void testDiscardCost() throws Exception {
        assertMemoryLeak(() -> {
            for (int r = 0; r < REPEATS; r++) {
                measureDiscard("dp" + r, false);
                measureDiscard("dr" + r, true);
            }
        });
    }

    @Test
    public void testLockHoldAdaptiveNoWindow() throws Exception {
        assertHold("a0", CommitMode.ADAPTIVE, 0, TABLES, 1, false);
    }

    @Test
    public void testLockHoldAdaptiveWindow() throws Exception {
        assertHold("aw", CommitMode.ADAPTIVE, WINDOW_US, TABLES, 1, true);
    }

    @Test
    public void testLockHoldNosync() throws Exception {
        assertHold("ns", CommitMode.NOSYNC, 0, TABLES, 1, false);
    }

    @Test
    public void testLockHoldSync() throws Exception {
        assertHold("sy", CommitMode.SYNC, 0, TABLES, 1, false);
    }

    @Test
    public void testLockHoldSyncWideTables() throws Exception {
        assertHold("sw", CommitMode.SYNC, 0, 150, 64, false);
    }

    @Test
    public void testRefusalPath() throws Exception {
        assertMemoryLeak(() -> {
            for (int r = 0; r < REPEATS; r++) {
                measureRefusal("rf" + r, TABLES / 2);
            }
        });
    }

    private static byte[][] frames(String prefix, int tableCount, long value, long tsMicros) {
        final byte[][] frames = new byte[tableCount][];
        for (int t = 0; t < tableCount; t++) {
            frames[t] = createMaskedFrameOf(prefix + t, value + t, tsMicros + t);
        }
        return frames;
    }

    private static byte[] createMaskedFrameOf(String table, long value, long tsMicros) {
        return QwpIngressWireHarness.createMaskedFrame(WebSocketOpcode.BINARY, QwpIngressWireHarness.oneRowMessage(table, value, tsMicros));
    }

    private static int recvBufferSize(byte[][] frames) {
        int size = 0;
        for (byte[] f : frames) {
            size += f.length;
        }
        return size + 1024;
    }

    private static void createTables(String prefix, int tableCount, int columnCount) throws Exception {
        final StringBuilder ddl = new StringBuilder();
        for (int t = 0; t < tableCount; t++) {
            ddl.setLength(0);
            ddl.append("CREATE TABLE ").append(prefix).append(t).append(" (v LONG");
            for (int c = 1; c < columnCount; c++) {
                ddl.append(", c").append(c).append(" LONG");
            }
            ddl.append(", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(ddl);
        }
    }

    private static String ms(long nanos) {
        return String.format("%.3f", nanos / 1e6);
    }

    private static void print(String line) {
        System.out.println("E1-MEASURE " + line);
        LOG.info().$("E1-MEASURE ").$(line).$();
    }

    private static void warmWriters(CairoEngine engine, String prefix, int tableCount) {
        for (int t = 0; t < tableCount; t++) {
            try (WalWriter ignore = engine.getWalWriter(engine.verifyTableName(prefix + t))) {
                Assert.assertFalse(ignore.isDistressed());
            }
        }
    }

    // One measureHold() per repeat, each on tables of its own.
    private void assertHold(String prefix, int commitMode, long windowUs, int tableCount, int columnCount, boolean isDueFlush) throws Exception {
        assertMemoryLeak(() -> {
            for (int r = 0; r < REPEATS; r++) {
                measureHold(prefix + r + "_", commitMode, windowUs, tableCount, columnCount, isDueFlush);
            }
        });
    }

    // A prepare (E1) or commit (base) failure at the last table, or a REVOKE at the first, under SYNC: the time the
    // failed pass takes after the fault, the replay's time and the WAL directories the replay creates.
    private void measureDiscard(String prefix, boolean isRevoke) throws Exception {
        createTables(prefix, TABLES, 1);
        final MeasureFacade ff = new MeasureFacade();
        try (MeasureEngine measureEngine = new MeasureEngine(configuration(CommitMode.SYNC, 0, ff))) {
            warmWriters(measureEngine, prefix, TABLES);
            final byte[][] frames = frames(prefix, TABLES, 100, 1_000_000L);
            final SecurityContext securityContext;
            if (isRevoke) {
                final String revoked = prefix + 0;
                securityContext = new AllowAllSecurityContext() {
                    @Override
                    public void authorizeInsert(TableToken tableToken) {
                        if (tableToken.getTableName().equals(revoked) && measureEngine.getRoleSwitchReadLockCount() > 0) {
                            ff.faultNanos.set(System.nanoTime());
                            throw CairoException.nonCritical().put("measure: insert revoked");
                        }
                    }
                };
            } else {
                ff.failEventFlushOf(prefix + (TABLES - 1) + "~");
                securityContext = AllowAllSecurityContext.INSTANCE;
            }
            final long passEnd;
            try (QwpIngressTestConnection connection = new QwpIngressTestConnection(measureEngine, configuration, recvBufferSize(frames), securityContext, frames)) {
                connection.deliverFrames(frames.length);
                passEnd = System.nanoTime();
            }
            Assert.assertTrue("the fault fired", ff.faultNanos.get() > 0);
            final long afterFault = passEnd - ff.faultNanos.get();
            ff.walDirCreations.set(0);
            final long replayStart = System.nanoTime();
            try (QwpIngressTestConnection replay = new QwpIngressTestConnection(measureEngine, configuration, recvBufferSize(frames), AllowAllSecurityContext.INSTANCE, frames)) {
                replay.deliverFrames(frames.length);
                Assert.assertEquals(frames.length - 1, QwpIngressTestConnection.maxCumulativeOkAck(replay.sentFrames()));
            }
            final long replay = System.nanoTime() - replayStart;
            print((isRevoke ? "discard revoke-first" : "discard prepare-fail-last") + " tables=" + TABLES
                    + " afterFaultMs=" + ms(afterFault) + " replayMs=" + ms(replay) + " replayWalDirs=" + ff.walDirCreations.get());
        }
    }

    private void measureHold(String prefix, int commitMode, long windowUs, int tableCount, int columnCount, boolean isDueFlush) throws Exception {
        createTables(prefix, tableCount, columnCount);
        final MeasureFacade ff = new MeasureFacade();
        try (MeasureEngine measureEngine = new MeasureEngine(configuration(commitMode, windowUs, ff))) {
            Assert.assertEquals(commitMode, measureEngine.getConfiguration().getCommitMode());
            Assert.assertEquals(windowUs, measureEngine.getConfiguration().getAdaptiveCommitGroupWindowUs());
            warmWriters(measureEngine, prefix, tableCount);
            final byte[][] first = frames(prefix, tableCount, 100, 1_000_000L);
            final byte[][] second = frames(prefix, tableCount, 10_000, 2_000_000L);
            final byte[][] all = Arrays.copyOf(first, first.length * 2);
            System.arraycopy(second, 0, all, first.length, second.length);
            setCurrentMicros(T0);
            try (QwpIngressTestConnection connection = new QwpIngressTestConnection(measureEngine, configuration, recvBufferSize(first), AllowAllSecurityContext.INSTANCE, all)) {
                // the first pass opens the writers' files and, under a window, starts each writer's batch
                connection.deliverFrames(first.length);
                setCurrentMicros(isDueFlush ? T0 + windowUs : T0);
                measureEngine.resetHolds();
                final long start = System.nanoTime();
                connection.deliverFrames(second.length);
                final long pass = System.nanoTime() - start;
                Assert.assertEquals(all.length - 1, QwpIngressTestConnection.maxCumulativeOkAck(connection.sentFrames()));
                print("hold mode=" + CommitMode.toString(commitMode) + " windowUs=" + windowUs + " tables=" + tableCount
                        + " columns=" + columnCount + " dueFlush=" + isDueFlush
                        + " holdMs=" + ms(measureEngine.maxHoldNanos.get()) + " holds=" + measureEngine.holdCount.get()
                        + " passMs=" + ms(pass));
            }
        }
        setCurrentMicros(-1);
    }

    // A demote at the k-th prepare (E1) or commit (base) of the batch, under SYNC: how long the demote waits for
    // the write lock, and how long from the moment it has it to the connection's role-change close.
    private void measureRefusal(String prefix, int k) throws Exception {
        createTables(prefix, TABLES, 1);
        final MeasureFacade ff = new MeasureFacade();
        try (MeasureEngine measureEngine = new MeasureEngine(configuration(CommitMode.SYNC, 0, ff))) {
            warmWriters(measureEngine, prefix, TABLES);
            final byte[][] frames = frames(prefix, TABLES, 100, 1_000_000L);
            final AtomicLong writeLockWait = new AtomicLong(-1);
            final AtomicLong acquiredAt = new AtomicLong(-1);
            final AtomicInteger demoteErrors = new AtomicInteger();
            final Thread[] demote = new Thread[1];
            ff.onEventFlushOf(prefix + k + "~", () -> {
                demote[0] = new Thread(() -> {
                    try {
                        final Lock writeLock = measureEngine.getRoleSwitchWriteLock();
                        final long requested = System.nanoTime();
                        writeLock.lock();
                        try {
                            acquiredAt.set(System.nanoTime());
                            writeLockWait.set(acquiredAt.get() - requested);
                            measureEngine.isReadOnly.set(true);
                        } finally {
                            writeLock.unlock();
                        }
                    } catch (Throwable th) {
                        demoteErrors.incrementAndGet();
                    }
                });
                demote[0].start();
            });
            final long closedAt;
            try (QwpIngressTestConnection connection = new QwpIngressTestConnection(measureEngine, configuration, recvBufferSize(frames), AllowAllSecurityContext.INSTANCE, frames)) {
                connection.deliverFrames(frames.length);
                closedAt = System.nanoTime();
                demote[0].join();
                Assert.assertEquals(0, demoteErrors.get());
                final ObjList<byte[]> sent = connection.sentFrames();
                Assert.assertTrue("the role-change close went out", sent.size() > 0 && (sent.getLast()[0] & 0x0F) == WebSocketOpcode.CLOSE);
            }
            print("refusal mode=sync tables=" + TABLES + " demoteAtTable=" + k + " writeLockWaitMs=" + ms(writeLockWait.get())
                    + " acquiredToCloseMs=" + ms(closedAt - acquiredAt.get()));
        }
    }

    private DefaultTestCairoConfiguration configuration(int commitMode, long windowUs, FilesFacade ff) {
        return new DefaultTestCairoConfiguration(root) {
            @Override
            public long getAdaptiveCommitGroupWindowUs() {
                return windowUs;
            }

            @Override
            public int getCommitMode() {
                return commitMode;
            }

            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return ff;
            }

            @Override
            public @NotNull MicrosecondClock getMicrosecondClock() {
                return testMicrosClock;
            }
        };
    }

    /**
     * Acts at the first flush of a table's WAL event file: runs an action, or fails the flush once, and counts the
     * WAL directories created.
     */
    private static final class MeasureFacade extends TestFilesFacadeImpl {
        final AtomicLong faultNanos = new AtomicLong();
        final AtomicInteger walDirCreations = new AtomicInteger();
        private final Map<Long, String> eventFds = new ConcurrentHashMap<>();
        // mapping address -> {length, fd} of the event files
        private final ConcurrentSkipListMap<Long, long[]> eventMappings = new ConcurrentSkipListMap<>();
        private volatile Runnable action;
        private volatile String actionTable;
        private volatile String failedTable;

        @Override
        public void barrierFsync(long fd) {
            onFlush(fd);
            super.barrierFsync(fd);
        }

        @Override
        public boolean close(long fd) {
            eventFds.remove(fd);
            return super.close(fd);
        }

        @Override
        public void fdatasync(long fd) {
            onFlush(fd);
            super.fdatasync(fd);
        }

        @Override
        public int mkdirs(Path path, int mode) {
            final String p = path.toString();
            if (p.matches(".*[/\\\\]wal[0-9]+[/\\\\]?$")) {
                walDirCreations.incrementAndGet();
            }
            return super.mkdirs(path, mode);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            final long addr = super.mmap(fd, len, offset, flags, memoryTag);
            if (eventFds.containsKey(fd)) {
                eventMappings.put(addr, new long[]{len, fd});
            }
            return addr;
        }

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            final long newAddr = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
            if (eventFds.containsKey(fd)) {
                eventMappings.remove(addr);
                eventMappings.put(newAddr, new long[]{newSize, fd});
            }
            return newAddr;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            if (!async) {
                final Map.Entry<Long, long[]> mapping = eventMappings.floorEntry(addr);
                if (mapping != null && addr < mapping.getKey() + mapping.getValue()[0]) {
                    onFlush(mapping.getValue()[1]);
                }
            }
            super.msync(addr, len, async);
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            eventMappings.remove(address);
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (fd > -1 && Utf8s.endsWithAscii(name, WalUtils.EVENT_FILE_NAME)) {
                eventFds.put(fd, Utf8s.stringFromUtf8BytesSafe(name));
            }
            return fd;
        }

        void failEventFlushOf(String table) {
            failedTable = table;
        }

        void onEventFlushOf(String table, Runnable action) {
            this.action = action;
            actionTable = table;
        }

        private void onFlush(long fd) {
            final String path = eventFds.get(fd);
            if (path == null) {
                return;
            }
            final String actionTable = this.actionTable;
            if (actionTable != null && path.contains(File.separator + actionTable)) {
                this.actionTable = null;
                action.run();
            }
            final String failedTable = this.failedTable;
            if (failedTable != null && path.contains(File.separator + failedTable)) {
                this.failedTable = null;
                faultNanos.set(System.nanoTime());
                throw CairoException.critical(0).put("measure: event flush failed");
            }
        }
    }

    /**
     * Times each hold of the role-switch read lock by the ingress thread, and reads as read-only once a demote set
     * it.
     */
    private static final class MeasureEngine extends CairoEngine {
        final AtomicInteger holdCount = new AtomicInteger();
        final AtomicBoolean isReadOnly = new AtomicBoolean();
        final AtomicLong maxHoldNanos = new AtomicLong();
        private Lock readLock;

        MeasureEngine(CairoConfiguration configuration) {
            super(configuration);
        }

        @Override
        public synchronized Lock getRoleSwitchReadLock() {
            if (readLock == null) {
                final Lock delegate = super.getRoleSwitchReadLock();
                readLock = new Lock() {
                    private long lockedAt;

                    @Override
                    public void lock() {
                        delegate.lock();
                        lockedAt = System.nanoTime();
                    }

                    @Override
                    public void lockInterruptibly() throws InterruptedException {
                        delegate.lockInterruptibly();
                        lockedAt = System.nanoTime();
                    }

                    @Override
                    public @NotNull Condition newCondition() {
                        return delegate.newCondition();
                    }

                    @Override
                    public boolean tryLock() {
                        return delegate.tryLock();
                    }

                    @Override
                    public boolean tryLock(long time, @NotNull TimeUnit unit) throws InterruptedException {
                        return delegate.tryLock(time, unit);
                    }

                    @Override
                    public void unlock() {
                        final long held = System.nanoTime() - lockedAt;
                        holdCount.incrementAndGet();
                        maxHoldNanos.accumulateAndGet(held, Math::max);
                        delegate.unlock();
                    }
                };
            }
            return readLock;
        }

        @Override
        public boolean isReadOnlyMode() {
            return isReadOnly.get();
        }

        void resetHolds() {
            holdCount.set(0);
            maxHoldNanos.set(0);
        }
    }
}
