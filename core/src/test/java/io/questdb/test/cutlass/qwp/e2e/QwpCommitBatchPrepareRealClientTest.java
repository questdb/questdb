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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.client.Sender;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * The real QWP client against a QWP commit batch that prepares its tables outside the role-switch lock, under
 * ADAPTIVE with a 50 ms group window: a demote after the prepares and before the sequencing, and a prepare that
 * fails without stopping the engine. The client replays, and every row is stored once.
 */
public class QwpCommitBatchPrepareRealClientTest extends AbstractCairoTest {
    private static final long GROUP_WINDOW_US = 50_000;
    private static final String[] TABLES = {"pa", "pb", "pc"};

    @Test
    public void testDemoteAfterPreparePhaseReplaysEveryRowOnce() throws Exception {
        assertDemoteAfterPreparePhaseReplaysEveryRowOnce(null);
    }

    @Test
    public void testDemoteAfterPreparePhaseReplaysEveryRowOnceWithDurableAck() throws Exception {
        assertDemoteAfterPreparePhaseReplaysEveryRowOnce("local");
    }

    @Test
    public void testPrepareFailureReplaysEveryRowOnce() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final EventFlushFaultFacade ff = new EventFlushFaultFacade();
            try (DemoteAtLockEngine pinnedEngine = new DemoteAtLockEngine(pinnedConfiguration(ff))) {
                assertPinnedMode(pinnedEngine);
                final int port = RestartableQwpServer.pickFreePort();
                try (
                        RestartableQwpServer server = new RestartableQwpServer(pinnedEngine, pinnedEngine.getConfiguration(), port);
                        GroupCommitFlusher ignore = new GroupCommitFlusher(pinnedEngine)
                ) {
                    server.start();
                    try (Sender sender = newSender(port, null)) {
                        sendRows(sender, 0);
                        awaitRows(pinnedEngine, 1);
                        // The next batch's prepare of pb fails once, with an error that does not stop the engine.
                        ff.failNextEventFlush("pb~");
                        sendRows(sender, 1);
                        TestUtils.assertEventually(() -> Assert.assertTrue("the prepare fault fired", ff.hasFired));
                    }
                    awaitRows(pinnedEngine, 2);
                    Assert.assertFalse(pinnedEngine.isDurabilityFailed());
                }
            }
        });
    }

    private static void assertPinnedMode(CairoEngine engine) {
        Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
        Assert.assertEquals(GROUP_WINDOW_US, engine.getConfiguration().getAdaptiveCommitGroupWindowUs());
    }

    // Waits for each table to hold rowsPerTable rows, each value once.
    private void awaitRows(CairoEngine engine, int rowsPerTable) throws Exception {
        TestUtils.assertEventually(() -> {
            drainWalQueue(engine);
            for (String table : TABLES) {
                final StringBuilder expected = new StringBuilder("count\tcount_distinct\n");
                expected.append(rowsPerTable).append('\t').append(rowsPerTable).append('\n');
                assertQuery("SELECT count(), count_distinct(v) FROM " + table)
                        .withEngine(engine)
                        .withContext(TestUtils.createSqlExecutionCtx(engine))
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns(expected.toString());
            }
        });
    }

    private static void createTables() throws Exception {
        for (String table : TABLES) {
            execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        }
    }

    private static Sender newSender(int port, String durableAckTiers) {
        // One frame per flush, carrying a row of every table: one receive pass makes a batch of three tables.
        final Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address("localhost:" + port)
                .autoFlushRows(0)
                .autoFlushBytes(0)
                .autoFlushIntervalMillis(3_600_000)
                .closeFlushTimeoutMillis(30_000);
        if (durableAckTiers != null) {
            builder.requestDurableAck(durableAckTiers);
        }
        return builder.build();
    }

    private static DefaultTestCairoConfiguration pinnedConfiguration(FilesFacade ff) {
        return new DefaultTestCairoConfiguration(root) {
            @Override
            public long getAdaptiveCommitGroupWindowUs() {
                return GROUP_WINDOW_US;
            }

            @Override
            public int getCommitMode() {
                return CommitMode.ADAPTIVE;
            }

            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return ff;
            }
        };
    }

    private static void sendRows(Sender sender, int round) {
        for (int t = 0; t < TABLES.length; t++) {
            final long v = round * 10L + t;
            sender.table(TABLES[t]).longColumn("v", v).at(1_000_000L + v, ChronoUnit.MICROS);
        }
        sender.flush();
    }

    private void assertDemoteAfterPreparePhaseReplaysEveryRowOnce(String durableAckTiers) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (DemoteAtLockEngine demotableEngine = new DemoteAtLockEngine(pinnedConfiguration(TestFilesFacadeImpl.INSTANCE))) {
                assertPinnedMode(demotableEngine);
                final int port = RestartableQwpServer.pickFreePort();
                try (
                        RestartableQwpServer server = new RestartableQwpServer(demotableEngine, demotableEngine.getConfiguration(), port);
                        GroupCommitFlusher ignore = new GroupCommitFlusher(demotableEngine)
                ) {
                    server.start();
                    try (Sender sender = newSender(port, durableAckTiers)) {
                        sendRows(sender, 0);
                        awaitRows(demotableEngine, 1);
                        // The next batch's lock() finds the node demoted, after its tables prepared.
                        demotableEngine.isDemoteArmed.set(true);
                        sendRows(sender, 1);
                        TestUtils.assertEventually(() -> Assert.assertTrue("the demote landed", demotableEngine.demotes.get() > 0));
                        // After the role-change close the node is writable again: the client's reconnect gets through.
                        TestUtils.assertEventually(() -> Assert.assertTrue("the batch was refused", demotableEngine.refusals.get() > 0));
                        demotableEngine.isReadOnly.set(false);
                        sendRows(sender, 2);
                    }
                    awaitRows(demotableEngine, 3);
                }
            }
        });
    }

    /**
     * Demotes at the first role-switch lock acquisition after {@link #isDemoteArmed} is set: a commit batch takes
     * the lock after it prepared its tables. Stays read-only until the test reverts it.
     */
    private static final class DemoteAtLockEngine extends CairoEngine {
        private final AtomicInteger demotes = new AtomicInteger();
        private final AtomicBoolean isDemoteArmed = new AtomicBoolean();
        private final AtomicBoolean isReadOnly = new AtomicBoolean();
        private final AtomicInteger refusals = new AtomicInteger();
        private Lock readLock;

        DemoteAtLockEngine(CairoConfiguration configuration) {
            super(configuration);
        }

        @Override
        public synchronized Lock getRoleSwitchReadLock() {
            if (readLock == null) {
                final Lock delegate = super.getRoleSwitchReadLock();
                readLock = new Lock() {
                    @Override
                    public void lock() {
                        delegate.lock();
                        if (isDemoteArmed.compareAndSet(true, false)) {
                            isReadOnly.set(true);
                            demotes.incrementAndGet();
                        }
                    }

                    @Override
                    public void lockInterruptibly() throws InterruptedException {
                        delegate.lockInterruptibly();
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
                        delegate.unlock();
                    }
                };
            }
            return readLock;
        }

        @Override
        public boolean isReadOnlyMode() {
            final boolean isReadOnlyNow = isReadOnly.get();
            if (isReadOnlyNow) {
                refusals.incrementAndGet();
            }
            return isReadOnlyNow;
        }
    }

    /**
     * The server's background group-commit flusher, which WalPurgeJob drives in production: flushes every batch a
     * group window old, so the local durable frontier, and with it the durable ack, follows an idle writer's
     * last commit.
     */
    private static final class GroupCommitFlusher implements AutoCloseable {
        private final AtomicBoolean isRunning = new AtomicBoolean(true);
        private final Thread thread;

        GroupCommitFlusher(CairoEngine engine) {
            thread = new Thread(() -> {
                while (isRunning.get()) {
                    engine.getWalGroupCommitFlushQueue().sweepIfDue(GROUP_WINDOW_US, engine.getConfiguration().getMicrosecondClock());
                    Os.pause();
                }
            }, "test-group-commit-flusher");
            thread.start();
        }

        @Override
        public void close() throws InterruptedException {
            isRunning.set(false);
            thread.join();
        }
    }

    /**
     * Fails, once, the next flush of a table's WAL event file, with an error that is not a data-sync failure. A
     * table is named by its directory prefix, such as {@code "pb~"}.
     */
    private static final class EventFlushFaultFacade extends TestFilesFacadeImpl {
        private final Map<Long, String> eventFds = new ConcurrentHashMap<>();
        // mapping address -> {length, fd} of the event files
        private final ConcurrentSkipListMap<Long, long[]> eventMappings = new ConcurrentSkipListMap<>();
        private volatile String failedTable;
        volatile boolean hasFired;

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

        void failNextEventFlush(String table) {
            failedTable = table;
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

        private void onFlush(long fd) {
            final String path = eventFds.get(fd);
            final String failedTable = this.failedTable;
            if (path != null && failedTable != null && path.contains(File.separator + failedTable)) {
                this.failedTable = null;
                hasFired = true;
                throw CairoException.critical(0).put("test: prepare failed");
            }
        }
    }
}
