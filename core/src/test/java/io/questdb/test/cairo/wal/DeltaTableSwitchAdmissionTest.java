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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import static io.questdb.cairo.wal.WalUtils.CONVERT_FILE_NAME;
import static io.questdb.cairo.wal.WalUtils.DELTA_TABLE_SWITCH_WAL_ID;

public class DeltaTableSwitchAdmissionTest extends AbstractCairoTest {
    private static volatile boolean isDeltaSupported;
    private static volatile boolean isReadOnly;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public boolean isDeltaTableSupported() {
                return isDeltaSupported;
            }

            @Override
            public boolean isReadOnlyMode() {
                return isReadOnly || super.isReadOnlyMode();
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testAcceptedPrefixAndGateSurviveReopenV1() throws Exception {
        testAcceptedPrefixAndGateSurviveReopen(0);
    }

    @Test
    public void testAcceptedPrefixAndGateSurviveReopenV2() throws Exception {
        testAcceptedPrefixAndGateSurviveReopen(2);
    }

    @Test
    public void testBorrowedWriterCannotCommitAfterSwitch() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createWalTable();
            TableSequencerAPI api = engine.getTableSequencerAPI();
            try (WalWriter writer = engine.getWalWriter(token)) {
                TableWriter.Row row = writer.newRow(100);
                row.putSym(0, "unaccepted");
                row.append();
                Assert.assertEquals(1, api.beginDeltaTableSwitch(token, TestUtils.getSequencerMetadataVersion(engine, token)));
                assertPending(writer::commit);
                Assert.assertEquals(1, api.lastTxn(token));
            }
            assertQuery("SELECT sym FROM " + token.getTableName())
                    .noLeakCheck().expectSize()
                    .returns("sym\n");
        });
    }

    @Test
    public void testConcurrentCommitsAreAcceptedBeforeSwitchOrRefused() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createWalTable();
            TableSequencerAPI api = engine.getTableSequencerAPI();
            final int writerCount = 4;
            boolean[] accepted = new boolean[writerCount];
            CountDownLatch ready = new CountDownLatch(writerCount + 1);
            CountDownLatch start = new CountDownLatch(1);
            AtomicLong switchTxn = new AtomicLong();
            AtomicReference<Throwable> error = new AtomicReference<>();
            ObjList<Thread> threads = new ObjList<>();
            for (int i = 0; i < writerCount; i++) {
                final int index = i;
                threads.add(new Thread(() -> {
                    try (WalWriter writer = engine.getWalWriter(token)) {
                        TableWriter.Row row = writer.newRow(100 + index);
                        row.putSym(0, "writer_" + index);
                        row.append();
                        ready.countDown();
                        Assert.assertTrue(start.await(10, TimeUnit.SECONDS));
                        try {
                            writer.commit();
                            accepted[index] = true;
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "Delta table switch is pending");
                        }
                    } catch (Throwable th) {
                        error.compareAndSet(null, th);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }, "delta-racing-writer-" + index));
            }
            threads.add(new Thread(() -> {
                try {
                    ready.countDown();
                    Assert.assertTrue(start.await(10, TimeUnit.SECONDS));
                    switchTxn.set(api.beginDeltaTableSwitch(token, TestUtils.getSequencerMetadataVersion(engine, token)));
                } catch (Throwable th) {
                    error.compareAndSet(null, th);
                } finally {
                    Path.clearThreadLocals();
                }
            }, "delta-racing-switch"));
            for (int i = 0; i < threads.size(); i++) {
                threads.getQuick(i).start();
            }
            try {
                Assert.assertTrue(ready.await(10, TimeUnit.SECONDS));
            } finally {
                start.countDown();
                for (int i = 0; i < threads.size(); i++) {
                    Thread thread = threads.getQuick(i);
                    thread.join(30_000);
                    Assert.assertFalse(thread.isAlive());
                }
            }
            if (error.get() != null) {
                throw new AssertionError(error.get());
            }
            int acceptedCount = 0;
            StringBuilder expected = new StringBuilder("sym\n");
            for (int i = 0; i < writerCount; i++) {
                if (accepted[i]) {
                    acceptedCount++;
                    expected.append("writer_").append(i).append('\n');
                }
            }
            Assert.assertEquals(acceptedCount + 1, switchTxn.get());
            Assert.assertEquals(switchTxn.get(), api.beginDeltaTableSwitch(token, TestUtils.getSequencerMetadataVersion(engine, token)));
            Assert.assertEquals(switchTxn.get(), api.lastTxn(token));
            try (TransactionLogCursor cursor = api.getCursor(token, 0)) {
                for (long txn = 1; txn <= switchTxn.get(); txn++) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(txn, cursor.getTxn());
                    if (txn < switchTxn.get()) {
                        Assert.assertTrue(cursor.getWalId() > 0);
                    } else {
                        Assert.assertEquals(DELTA_TABLE_SWITCH_WAL_ID, cursor.getWalId());
                    }
                }
                Assert.assertFalse(cursor.hasNext());
            }
            drainWalQueue();
            Assert.assertTrue(api.isSuspended(token));
            Assert.assertEquals(switchTxn.get(), api.getPendingDeltaTableSwitch(token));
            assertQuery("SELECT sym FROM " + token.getTableName() + " ORDER BY ts")
                    .noLeakCheck().sizeMayVary().returns(expected);
        });
    }

    @Test
    public void testDataMetadataAndWriterAdmissionAreClosed() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createWalTable();
            TableSequencerAPI api = engine.getTableSequencerAPI();
            AlterOperationBuilder builder = new AlterOperationBuilder();
            builder.ofAddColumn(0, token, token.getTableId());
            builder.ofAddColumn("extra", 0, ColumnType.LONG, 0, false, (byte) 0, 0);
            try (AlterOperation alter = builder.build()) {
                Assert.assertEquals(1, api.beginDeltaTableSwitch(token, TestUtils.getSequencerMetadataVersion(engine, token)));
                assertPending(() -> api.nextTxn(token, 1, 0, 0, 0, 100, 100, 1));
                assertPending(() -> api.nextStructureTxn(token, 0, alter));
                assertPending(() -> api.getNextWalId(token));
                Assert.assertEquals(1, api.lastTxn(token));
                Assert.assertFalse(engine.isTableDropped(token));
                Assert.assertEquals(token, engine.verifyTableName(token.getTableName()));
                try (TableReader reader = engine.getReader(token)) {
                    Assert.assertEquals(2, reader.getMetadata().getColumnCount());
                    Assert.assertEquals(0, reader.size());
                }
            }
        });
    }

    @Test
    public void testReadOnlyRefusalDoesNotPublishSwitch() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createWalTable();
            TableSequencerAPI api = engine.getTableSequencerAPI();
            isReadOnly = true;
            try {
                CairoException error = Assert.assertThrows(CairoException.class, () -> api.beginDeltaTableSwitch(token, TestUtils.getSequencerMetadataVersion(engine, token)));
                Assert.assertTrue(error.isReadOnlyAccessRefusal());
            } finally {
                isReadOnly = false;
            }
            Assert.assertEquals(0, api.lastTxn(token));
            api.checkNoTableModeTransition(token);
            assertNoConvertMarker(token);
            execute("INSERT INTO " + token.getTableName() + " VALUES ('after_refusal', 100)");
            drainWalQueue();
            assertQuery("SELECT sym FROM " + token.getTableName())
                    .noLeakCheck().expectSize()
                    .returns("sym\nafter_refusal\n");
        });
    }

    @Test
    public void testResumeCannotSkipSwitch() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createWalTable();
            execute("INSERT INTO " + token.getTableName() + " VALUES ('prefix', 100)");
            TableSequencerAPI api = engine.getTableSequencerAPI();
            long switchTxn = api.beginDeltaTableSwitch(token, TestUtils.getSequencerMetadataVersion(engine, token));
            drainWalQueue();
            Assert.assertTrue(api.isSuspended(token));
            CairoException error = Assert.assertThrows(CairoException.class, () -> api.resumeTable(token, switchTxn + 1));
            TestUtils.assertContains(error.getFlyweightMessage(), "cannot skip a Delta table switch");
            Assert.assertEquals(switchTxn, api.getPendingDeltaTableSwitch(token));
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals(switchTxn - 1, reader.getTxFile().getSeqTxn());
                Assert.assertEquals(1, reader.size());
            }
        });
    }

    @Test
    public void testSqlSwitchMintPrecedesConcurrentReadOnlyTransition() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createWalTable();
            CountDownLatch atMint = new CountDownLatch(1);
            CountDownLatch releaseMint = new CountDownLatch(1);
            CountDownLatch demoteAttempted = new CountDownLatch(1);
            AtomicBoolean wasDemoteBlocked = new AtomicBoolean();
            AtomicBoolean wasReadOnlyAtMint = new AtomicBoolean();
            AtomicLong seqTxnAtDemote = new AtomicLong(-1);
            AtomicReference<Throwable> error = new AtomicReference<>();
            isDeltaSupported = true;
            CairoEngine.setRoleSwitchMintObserver(() -> {
                atMint.countDown();
                TestUtils.await(releaseMint);
                wasReadOnlyAtMint.set(engine.isReadOnlyMode());
            });
            Thread switcher = new Thread(() -> {
                try (
                        SqlCompiler compiler = engine.getSqlCompiler();
                        SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1)
                ) {
                    context.with(sqlExecutionContext.getSecurityContext());
                    try (OperationFuture future = compiler.compile("ALTER TABLE " + token.getTableName() + " SET TYPE DELTA", context).execute(null)) {
                        Assert.assertEquals(OperationFuture.QUERY_STARTED, future.getStatus());
                    }
                } catch (Throwable th) {
                    error.compareAndSet(null, th);
                } finally {
                    Path.clearThreadLocals();
                }
            }, "delta-sql-switch");
            Thread demoter = new Thread(() -> {
                Lock lock = engine.getRoleSwitchWriteLock();
                boolean isLocked = lock.tryLock();
                wasDemoteBlocked.set(!isLocked);
                demoteAttempted.countDown();
                if (!isLocked) {
                    lock.lock();
                }
                try {
                    // The same write lock protects the enterprise read-only role publication.
                    isReadOnly = true;
                    seqTxnAtDemote.set(engine.getTableSequencerAPI().lastTxn(token));
                } catch (Throwable th) {
                    error.compareAndSet(null, th);
                } finally {
                    lock.unlock();
                    Path.clearThreadLocals();
                }
            }, "delta-read-only-transition");
            try {
                switcher.start();
                Assert.assertTrue(atMint.await(10, TimeUnit.SECONDS));
                demoter.start();
                Assert.assertTrue(demoteAttempted.await(10, TimeUnit.SECONDS));
                releaseMint.countDown();
                switcher.join(30_000);
                demoter.join(30_000);
                Assert.assertFalse(switcher.isAlive());
                Assert.assertFalse(demoter.isAlive());
                if (error.get() != null) {
                    throw new AssertionError(error.get());
                }
                Assert.assertTrue(wasDemoteBlocked.get());
                Assert.assertFalse(wasReadOnlyAtMint.get());
                Assert.assertEquals(1, seqTxnAtDemote.get());
                Assert.assertEquals(1, engine.getTableSequencerAPI().getPendingDeltaTableSwitch(token));
                CairoException refusal = Assert.assertThrows(CairoException.class,
                        () -> engine.getTableSequencerAPI().beginDeltaTableSwitch(token, TestUtils.getSequencerMetadataVersion(engine, token)));
                Assert.assertTrue(refusal.isReadOnlyAccessRefusal());
                Assert.assertEquals(1, engine.getTableSequencerAPI().lastTxn(token));
                assertNoConvertMarker(token);
                Assert.assertFalse(engine.verifyTableName(token.getTableName()).isDelta());
            } finally {
                releaseMint.countDown();
                switcher.join(30_000);
                demoter.join(30_000);
                CairoEngine.setRoleSwitchMintObserver(null);
                isDeltaSupported = false;
                isReadOnly = false;
            }
        });
    }

    @Test
    public void testSyncFailureKeepsAdmissionConsistentWithJournal() throws Exception {
        for (int failAt : new int[]{1, 2}) {
            int[] syncCount = {-1};
            TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
                @Override
                public void msync(long addr, long len, boolean async) {
                    if (syncCount[0] >= 0 && ++syncCount[0] == failAt) {
                        throw CairoException.critical(0).put("injected switch sync failure");
                    }
                    super.msync(addr, len, async);
                }
            };
            assertMemoryLeak(ff, () -> {
                String tableName = "switch_sync_" + failAt;
                execute("CREATE TABLE " + tableName + " (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                TableToken token = engine.verifyTableName(tableName);
                TableSequencerAPI api = engine.getTableSequencerAPI();
                syncCount[0] = 0;
                try {
                    CairoException error = Assert.assertThrows(CairoException.class, () -> api.beginDeltaTableSwitch(token, TestUtils.getSequencerMetadataVersion(engine, token)));
                    TestUtils.assertContains(error.getFlyweightMessage(), "injected switch sync failure");
                } finally {
                    syncCount[0] = -1;
                }
                Assert.assertEquals(failAt == 1 ? 0 : 1, api.lastTxn(token));
                if (failAt == 2) {
                    assertPending(() -> api.checkNoTableModeTransition(token));
                    Assert.assertTrue(api.releaseAll());
                    assertPending(() -> api.checkNoTableModeTransition(token));
                } else {
                    api.checkNoTableModeTransition(token);
                    execute("INSERT INTO " + token.getTableName() + " VALUES ('after_refusal', 100)");
                    drainWalQueue();
                    assertQuery("SELECT sym FROM " + token.getTableName()).noLeakCheck().expectSize().returns("sym\nafter_refusal\n");
                }
            });
        }
    }

    @Test
    public void testUnsupportedSqlLeavesWalUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createWalTable();
            TableSequencerAPI api = engine.getTableSequencerAPI();
            Assert.assertThrows(CairoException.class, () -> execute("ALTER TABLE " + token.getTableName() + " SET TYPE DELTA"));
            Assert.assertEquals(0, api.lastTxn(token));
            api.checkNoTableModeTransition(token);
            Assert.assertFalse(engine.verifyTableName(token.getTableName()).isDelta());
            assertNoConvertMarker(token);
            execute("INSERT INTO " + token.getTableName() + " VALUES ('after_refusal', 100)");
            drainWalQueue();
            assertQuery("SELECT sym FROM " + token.getTableName())
                    .noLeakCheck().expectSize()
                    .returns("sym\nafter_refusal\n");
        });
    }

    private static void assertPending(Runnable action) {
        CairoException error = Assert.assertThrows(CairoException.class, action::run);
        TestUtils.assertContains(error.getFlyweightMessage(), "Delta table switch is pending");
    }

    private void assertNoConvertMarker(TableToken token) {
        try (Path path = new Path()) {
            Assert.assertFalse(configuration.getFilesFacade().exists(path.of(root).concat(token).concat(CONVERT_FILE_NAME).$()));
        }
    }

    private TableToken createWalTable() throws SqlException {
        String tableName = testName.getMethodName();
        execute("CREATE TABLE " + tableName + " (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        return engine.verifyTableName(tableName);
    }

    private void testAcceptedPrefixAndGateSurviveReopen(int partSize) throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, partSize);
            TableToken token = createWalTable();
            execute("INSERT INTO " + token.getTableName() + " VALUES ('visible', 100)");
            drainWalQueue();
            TableSequencerAPI api = engine.getTableSequencerAPI();
            try (TableReader reader = engine.getReader(token)) {
                execute("INSERT INTO " + token.getTableName() + " VALUES ('accepted', 200)");
                Assert.assertEquals(2, api.lastTxn(token));
                Assert.assertEquals(3, api.beginDeltaTableSwitch(token, TestUtils.getSequencerMetadataVersion(engine, token)));
                Assert.assertEquals(3, api.beginDeltaTableSwitch(token, TestUtils.getSequencerMetadataVersion(engine, token)));
                Assert.assertEquals(3, api.lastTxn(token));
                Assert.assertEquals(1, reader.size());
                Assert.assertFalse(reader.getMetadata().getTableToken().isDelta());
                assertQuery("SELECT sym FROM " + token.getTableName())
                        .noLeakCheck().expectSize()
                        .returns("sym\nvisible\n");
            }
            Assert.assertTrue(api.releaseAll());
            Assert.assertEquals(3, api.beginDeltaTableSwitch(token, TestUtils.getSequencerMetadataVersion(engine, token)));
            assertPending(() -> api.checkNoTableModeTransition(token));
            try (TransactionLogCursor cursor = api.getCursor(token, 0)) {
                for (long txn = 1; txn <= 3; txn++) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(txn, cursor.getTxn());
                    if (txn < 3) {
                        Assert.assertTrue(cursor.getWalId() > 0);
                    } else {
                        Assert.assertEquals(DELTA_TABLE_SWITCH_WAL_ID, cursor.getWalId());
                    }
                }
                Assert.assertFalse(cursor.hasNext());
            }
            Assert.assertTrue(api.releaseAll());
            try (TableSequencerAPI reopened = new TableSequencerAPI(engine, configuration)) {
                assertPending(() -> reopened.checkNoTableModeTransition(token));
                Assert.assertEquals(3, reopened.beginDeltaTableSwitch(token, 0));
                Assert.assertEquals(3, reopened.lastTxn(token));
            }
        });
    }
}
