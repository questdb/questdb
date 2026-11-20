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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.LongList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.cutlass.websocket.QwpIngressTestConnection;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A power loss at each step of a QWP commit batch that prepares its tables outside the role-switch lock: at the
 * k-th table's event flush in the prepare phase, between the prepare phase and the lock, after the first table
 * sequenced, and inside the deferred group flush. Under SYNC, ADAPTIVE with a 50 ms group window and ADAPTIVE with
 * none.
 * <p>
 * The oracle, for each table: its transaction is all or nothing; no row survives that the sequencer did not
 * record; and after the client's replay of the batch, which no ack covered at the power loss, every row is
 * stored, exactly once for a power loss before the first sequencing. A power loss inside the sequencing can keep
 * the tables that sequenced first, which the replay then stores twice, as at head.
 */
@RunWith(Parameterized.class)
public class QwpCommitBatchPrepareCrashTest extends AbstractCrashConsistencyTest {
    private static final String[] TABLES = {"ca", "cb", "cc"};
    private static final long T0 = 1_000_000_000L;
    private static final long WINDOW_US = 50_000L;
    private final int commitMode;
    private final String commitModeName;
    private final long groupWindowUs;
    private PowerLossTriggerFacade triggerFf;

    public QwpCommitBatchPrepareCrashTest(String commitModeName, long groupWindowUs) {
        this.commitModeName = commitModeName;
        this.commitMode = CommitMode.fromString(commitModeName);
        this.groupWindowUs = groupWindowUs;
    }

    @Parameterized.Parameters(name = "{0}, W={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"sync", 0L},
                {"adaptive", WINDOW_US},
                {"adaptive", 0L},
        });
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitModeName);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, groupWindowUs);
        // No periodic save point: the test decides what is durable.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
    }

    @Test
    public void testPowerLossAfterFirstTableSequenced() throws Exception {
        runCrash(true, () -> {
            if (groupWindowUs > 0) {
                // No sequencer flush under the lock: the hook after the first table's txn is sequenced.
                final WalWriter.DeferredCommitInterceptor previous = WalWriter.deferredCommitInterceptor;
                WalWriter.deferredCommitInterceptor = (walId, seqTxn) -> {
                    if (SuspensionScope.hasAnyRoleSwitchLock()) {
                        triggerFf.powerLossNow();
                    }
                };
                return () -> WalWriter.deferredCommitInterceptor = previous;
            }
            // The first table's sequencer-log flush, made under the lock as it sequences.
            triggerFf.powerLossAfterSequencerFlush(TABLES[0], true);
            return null;
        });
    }

    @Test
    public void testPowerLossAtSecondTablePrepare() throws Exception {
        runCrash(false, () -> {
            triggerFf.powerLossAtEventFlush(TABLES[1]);
            return null;
        });
    }

    @Test
    public void testPowerLossBetweenPreparePhaseAndLock() throws Exception {
        runCrash(false, () -> {
            triggerFf.isPowerLossAfterPreparePhase = true;
            return null;
        });
    }

    @Test
    public void testPowerLossInsideDeferredGroupFlush() throws Exception {
        Assume.assumeTrue("only a group window defers the flush", groupWindowUs > 0);
        runCrash(true, () -> {
            // The first table's batch flush, which the batch runs after the unlock.
            triggerFf.powerLossAfterSequencerFlush(TABLES[0], false);
            return null;
        });
    }

    // Two one-row frames per table, interleaved: table t gets v = firstValue + t and firstValue + t + 3.
    private static byte[][] frames(int firstValue) {
        final byte[][] frames = new byte[2 * TABLES.length][];
        for (int i = 0; i < frames.length; i++) {
            final int t = i % TABLES.length;
            frames[i] = QwpIngressTestConnection.oneRowFrame(TABLES[t], firstValue + i, (firstValue + i) * 1_000_000L);
        }
        return frames;
    }

    private static byte[][] concat(byte[][] a, byte[][] b) {
        final byte[][] all = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, all, a.length, b.length);
        return all;
    }

    private void assertModeAndWindow() {
        Assert.assertEquals(commitMode, engine.getConfiguration().getCommitMode());
        Assert.assertEquals(groupWindowUs, engine.getConfiguration().getAdaptiveCommitGroupWindowUs());
    }

    // Each table's rows: its transaction's two rows, or none; any other count is a torn transaction.
    private LongList readValues(String table) {
        final LongList values = new LongList();
        try (RecordCursorFactory f = select("SELECT v FROM " + table)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                while (c.hasNext()) {
                    values.add(c.getRecord().getLong(0));
                }
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        return values;
    }

    private void rebootAfterPowerLoss() {
        crashAndReopen();
        for (String table : TABLES) {
            final TableToken tt = engine.verifyTableName(table);
            if (engine.getTableSequencerAPI().isSuspended(tt)) {
                engine.getTableSequencerAPI().getTxnTracker(tt).setUnsuspended();
            }
            engine.getTxnScoreboardPool().remove(tt);
            engine.getTableSequencerAPI().resetForReboot(tt);
        }
        new RecoveryCoordinator(engine).recover();
        for (String table : TABLES) {
            engine.notifyWalTxnRepublisher(engine.verifyTableName(table));
        }
        drainWalQueue();
    }

    /**
     * Runs the batch with the power loss {@code arm} sets up, reboots, checks the oracle, and replays the batch. With
     * {@code isInsideSequencing} the power loss may keep tables that sequenced before it.
     */
    private void runCrash(boolean isInsideSequencing, Arm arm) throws Exception {
        assumeCrashHarnessSupported();
        triggerFf = new PowerLossTriggerFacade();
        crashFf = triggerFf;
        crashFf.setDbRoot(root);
        try {
            assertMemoryLeak(crashFf, () -> {
                assertModeAndWindow();
                for (String table : TABLES) {
                    execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                }
                final boolean isGroupFlushCrash = isInsideSequencing && groupWindowUs > 0;
                final byte[][] firstBatch = frames(1);
                final byte[][] batch = frames(11);
                final int baseRows = isGroupFlushCrash ? 2 : 0;
                setCurrentMicros(T0);
                // Open each table's writer before the power loss is armed, so that a segment open's event flush cannot
                // fire it: the pool hands the writers to the connection.
                for (String table : TABLES) {
                    try (WalWriter ignore = engine.getWalWriter(engine.verifyTableName(table))) {
                        Assert.assertFalse(ignore.isDistressed());
                    }
                }
                try (QwpIngressTestConnection connection = new QwpIngressTestConnection(
                        engine, configuration, 4096, isGroupFlushCrash ? concat(firstBatch, batch) : batch)) {
                    if (isGroupFlushCrash) {
                        // A first batch starts each writer's group-commit batch; the batch under test, a window later on
                        // the same writers, makes the flush due.
                        connection.deliverFrames(firstBatch.length);
                        Assert.assertEquals(firstBatch.length - 1, QwpIngressTestConnection.maxCumulativeOkAck(connection.sentFrames()));
                        setCurrentMicros(T0 + WINDOW_US);
                    }
                    // What is on disk now counts as durable; the batch under test does not.
                    markDurableBaseline();
                    final Runnable disarm = arm.arm();
                    try {
                        connection.getState().setAfterPreparePhaseHook(triggerFf::onAfterPreparePhase);
                        connection.deliverFrames(batch.length);
                    } finally {
                        if (disarm != null) {
                            disarm.run();
                        }
                    }
                }
                Assert.assertTrue("the power loss must fire in the batch", crashFf.isPowerLossMarked());
                rebootAfterPowerLoss();

                for (int t = 0; t < TABLES.length; t++) {
                    final LongList values = readValues(TABLES[t]);
                    final long lastTxn = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName(TABLES[t]));
                    Assert.assertEquals(TABLES[t] + ": a transaction is all or nothing " + values, 0, values.size() % 2);
                    Assert.assertTrue(TABLES[t] + ": no row the sequencer did not record " + values + ", lastTxn=" + lastTxn,
                            values.size() / 2 <= lastTxn);
                    Assert.assertTrue(TABLES[t] + ": at most the batches sent " + values, values.size() <= baseRows + 2);
                    if (!isInsideSequencing) {
                        Assert.assertEquals(TABLES[t] + ": nothing sequenced before the power loss " + values, baseRows, values.size());
                    }
                }

                // The client's replay: no ack of the batch reached it before the power loss.
                try (QwpIngressTestConnection replay = new QwpIngressTestConnection(engine, configuration, 4096, batch)) {
                    replay.deliverFrames(batch.length);
                    Assert.assertEquals(batch.length - 1, QwpIngressTestConnection.maxCumulativeOkAck(replay.sentFrames()));
                }
                drainWalQueue();
                for (int t = 0; t < TABLES.length; t++) {
                    final LongList values = readValues(TABLES[t]);
                    if (!isInsideSequencing) {
                        Assert.assertEquals(TABLES[t] + ": the replay stores every row once " + values,
                                baseRows + 2, values.size());
                    } else {
                        Assert.assertTrue(TABLES[t] + ": the replay stores every row " + values, values.size() >= baseRows + 2);
                    }
                    values.sort();
                    final StringBuilder distinct = new StringBuilder("v\n");
                    for (int i = 0, n = values.size(); i < n; i++) {
                        if (i == 0 || values.getQuick(i) != values.getQuick(i - 1)) {
                            distinct.append(values.getQuick(i)).append('\n');
                        }
                    }
                    final String expected = "v\n" + (isGroupFlushCrash ? (1 + t) + "\n" + (4 + t) + "\n" : "") + (11 + t) + "\n" + (14 + t) + "\n";
                    Assert.assertEquals(TABLES[t], expected, distinct.toString());
                }
            });
        } finally {
            crashFf.reset();
            engine.releaseCrashOrphanedWalWriters();
            engine.releaseCrashOrphanedWriters();
        }
    }

    @FunctionalInterface
    private interface Arm {
        // Arms the power loss; returns what disarms it after the batch, or null.
        Runnable arm();
    }

    /**
     * Marks the power loss at a chosen step of the batch: the first flush of a table's event file, after the
     * prepare phase, or around the first flush of a table's sequencer log made under or outside the role-switch
     * lock. What the engine does after the mark adds no durability.
     */
    private static final class PowerLossTriggerFacade extends CrashFaultFilesFacade {
        // mapping address -> {length, fd} of the files this facade opened
        private final ConcurrentSkipListMap<Long, long[]> mappings = new ConcurrentSkipListMap<>();
        private final Map<Long, String> paths = new ConcurrentHashMap<>();
        volatile boolean isPowerLossAfterPreparePhase;
        private volatile String eventTable;
        private volatile boolean isSequencerFlushUnderLock;
        private volatile String sequencerTable;

        @Override
        public void barrierFsync(long fd) {
            onEventFlush(fd);
            super.barrierFsync(fd);
        }

        @Override
        public boolean close(long fd) {
            paths.remove(fd);
            return super.close(fd);
        }

        @Override
        public void fdatasync(long fd) {
            onEventFlush(fd);
            super.fdatasync(fd);
            onSequencerFlush(fd);
        }

        @Override
        public void fsync(long fd) {
            onEventFlush(fd);
            super.fsync(fd);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            final long addr = super.mmap(fd, len, offset, flags, memoryTag);
            if (paths.containsKey(fd)) {
                mappings.put(addr, new long[]{len, fd});
            }
            return addr;
        }

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            final long newAddr = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
            if (paths.containsKey(fd)) {
                mappings.remove(addr);
                mappings.put(newAddr, new long[]{newSize, fd});
            }
            return newAddr;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            long fd = -1;
            if (!async) {
                final Map.Entry<Long, long[]> mapping = mappings.floorEntry(addr);
                if (mapping != null && addr < mapping.getKey() + mapping.getValue()[0]) {
                    fd = mapping.getValue()[1];
                    onEventFlush(fd);
                }
            }
            super.msync(addr, len, async);
            if (fd != -1) {
                onSequencerFlush(fd);
            }
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            mappings.remove(address);
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (fd > -1) {
                paths.put(fd, Utf8s.stringFromUtf8BytesSafe(name));
            }
            return fd;
        }

        void onAfterPreparePhase() {
            if (isPowerLossAfterPreparePhase) {
                isPowerLossAfterPreparePhase = false;
                powerLossNow();
            }
        }

        void powerLossAfterSequencerFlush(String table, boolean isUnderLock) {
            isSequencerFlushUnderLock = isUnderLock;
            sequencerTable = table;
        }

        void powerLossAtEventFlush(String table) {
            eventTable = table;
        }

        void powerLossNow() {
            powerLoss();
        }

        private boolean isEventFile(long fd) {
            final String path = paths.get(fd);
            return path != null && path.endsWith(File.separator + WalUtils.EVENT_FILE_NAME);
        }

        private void onEventFlush(long fd) {
            final String table = eventTable;
            if (table != null && isEventFile(fd) && paths.get(fd).contains(File.separator + table + "~")) {
                eventTable = null;
                // the flush itself comes after the power loss
                powerLossNow();
            }
        }

        private void onSequencerFlush(long fd) {
            final String table = sequencerTable;
            final String path = paths.get(fd);
            if (table != null && path != null && path.contains(File.separator + table + "~")
                    && path.contains(File.separator + WalUtils.SEQ_DIR) && SuspensionScope.hasAnyRoleSwitchLock() == isSequencerFlushUnderLock) {
                sequencerTable = null;
                powerLossNow();
            }
        }
    }
}
