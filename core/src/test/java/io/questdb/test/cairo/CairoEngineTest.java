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
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableFlagResolver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.mp.Job;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Files;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.TelemetryTask;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.questdb.cairo.TableUtils.TABLE_EXISTS;
import static io.questdb.cairo.TableUtils.TABLE_RESERVED;
import static org.junit.Assert.*;

public class CairoEngineTest extends AbstractCairoTest {

    private static Path otherPath = new Path();
    private static Path path = new Path();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.setUpStatic();
        otherPath = new Path();
        path = new Path();
    }

    @AfterClass
    public static void tearDownStatic() {
        otherPath = Misc.free(otherPath);
        path = Misc.free(path);
        AbstractCairoTest.tearDownStatic();
    }

    @Test
    public void testAncillaries() throws Exception {
        assertMemoryLeak(() -> {

            class MyListener implements PoolListener {
                int count = 0;

                @Override
                public void onEvent(byte factoryType, long thread, TableToken tableToken, short event, short segment, short position) {
                    count++;
                }
            }

            MyListener listener = new MyListener();

            try (CairoEngine engine = new CairoEngine(configuration)) {
                TableToken x = createX(engine);

                engine.setPoolListener(listener);
                Assert.assertEquals(listener, engine.getPoolListener());

                TableReader reader = engine.getReader(x, -1, null);
                TableWriter writer = engine.getWriter(x, "test");
                Assert.assertEquals(1, engine.getBusyReaderCount());
                Assert.assertEquals(1, engine.getBusyWriterCount());

                reader.close();
                writer.close();

                Assert.assertEquals(4, listener.count);
                Assert.assertEquals(configuration, engine.getConfiguration());
            }
        });
    }

    @Test
    public void testCannotMapTableId() throws Exception {
        TestUtils.assertMemoryLeak(new TestUtils.LeakProneCode() {
            @Override
            public void run() {
                ff = new TestFilesFacadeImpl() {
                    private boolean failNextAlloc = false;

                    @Override
                    public boolean allocate(long fd, long size) {
                        if (failNextAlloc) {
                            failNextAlloc = false;
                            return false;
                        }
                        return super.allocate(fd, size);
                    }

                    @Override
                    public long length(long fd) {
                        if (this.fd == fd) {
                            failNextAlloc = true;
                            this.fd = -1;
                            return 0;
                        }
                        return super.length(fd);
                    }

                    @Override
                    public long openRW(LPSZ name, int opts) {
                        long fd = super.openRW(name, opts);
                        if (Utf8s.endsWithAscii(name, TableUtils.TAB_INDEX_FILE_NAME)) {
                            this.fd = fd;
                        }
                        return fd;
                    }
                };

                try {
                    //noinspection resource
                    new CairoEngine(configuration);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
                } finally {
                    ff = null;
                }
                Path.clearThreadLocals();
            }
        });
    }

    @Test
    public void testDuplicateTableCreation() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("a", ColumnType.INT);
            AbstractCairoTest.create(model);
            try (
                    Path path = new Path()
            ) {
                try (MemoryMARW mem = Vm.getCMARWInstance()) {
                    engine.createTable(securityContext, mem, path, false, model, false);
                    fail("duplicated tables should not be permitted!");
                }
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "table exists");
            }
        });
    }

    @Test
    public void testExpiry() throws Exception {
        assertMemoryLeak(() -> {
            class MyListener implements PoolListener {
                int count = 0;

                @Override
                public void onEvent(byte factoryType, long thread, TableToken tableToken, short event, short segment, short position) {
                    if (event == PoolListener.EV_EXPIRE) {
                        count++;
                    }
                }
            }

            MyListener listener = new MyListener();

            try (CairoEngine engine = new CairoEngine(
                    new DefaultCairoConfiguration(configuration.getDbRoot()) {
                        @Override
                        public boolean getAllowTableRegistrySharedWrite() {
                            return true;
                        }

                        @Override
                        public long getIdleCheckInterval() {
                            // Make it big to prevent second run even on slow machines
                            return Micros.DAY_MICROS;
                        }
                    })
            ) {
                TableToken x = createX(engine);

                engine.setPoolListener(listener);

                assertWriter(engine, x);
                assertReader(engine, x);

                Job job = new ServerMain.EngineMaintenanceJob(engine);
                Assert.assertNotNull(job);

                Assert.assertTrue(job.run());
                Assert.assertFalse(job.run());

                Assert.assertEquals(2, listener.count);
            }
        });
    }

    @Test
    public void testGetTableFlagResolver() throws Exception {
        assertMemoryLeak(() -> {
            final TableFlagResolver tableFlagResolver = engine.getTableFlagResolver();
            assertTrue(tableFlagResolver.isSystem(engine.getConfiguration().getSystemTableNamePrefix() + ".tableName"));
            assertTrue(tableFlagResolver.isPublic(TelemetryTask.TABLE_NAME));
        });
    }

    @Test
    public void testGetTableTokenIfExists() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "x";
            final SOCountDownLatch latch = new SOCountDownLatch(1);
            final AtomicReference<Throwable> ref = new AtomicReference<>();
            TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE) {
                @Override
                public int getColumnCount() {
                    latch.await();
                    return super.getColumnCount();
                }
            };
            final Thread createTableThread = new Thread(() -> {
                model.col("a", ColumnType.INT);
                try {
                    createTable(model);
                } catch (Throwable th) {
                    LOG.error().$("Error in thread").$(th).$();
                    ref.set(th);
                } finally {
                    Path.clearThreadLocals();
                }
            });
            createTableThread.start();

            waitForTableStatus(TABLE_RESERVED);
            final TableToken token1 = engine.getTableTokenIfExists(tableName);
            assertNull(token1);

            latch.countDown();

            waitForTableStatus(TABLE_EXISTS);
            final TableToken token2 = engine.getTableTokenIfExists(tableName);
            assertEquals(tableName, token2.getTableName());

            createTableThread.join();
            if (ref.get() != null) {
                fail("Error " + ref.get().getMessage());
            }
        });
    }

    @Test
    public void testLockBusyReader() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                TableToken x = createX(engine);
                try (TableReader reader = engine.getReader(x)) {
                    Assert.assertNotNull(reader);
                    Assert.assertEquals(CairoEngine.REASON_BUSY_READER, engine.lockAll(x, "testing", true));
                    assertReader(engine, x);
                    assertWriter(engine, x);
                }
            }
        });
    }

    @Test
    public void testNewTableRename() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                createX(engine);
                try (MemoryMARW mem = Vm.getCMARWInstance()) {
                    TableToken y = engine.rename(securityContext, path, mem, "x", otherPath, "y");
                    assertWriter(engine, y);
                    assertReader(engine, y);
                }
            }
        });
    }

    @Test
    public void testReconcileReadLockBlocksReadersMetadataAndQueries() throws Exception {
        // The enterprise RECONCILE TABLE apply calls lockReconcileReads(token) while it swaps the
        // table's files. From that point every getReader overload and getTableMetadata must refuse
        // the dir with EntryLockedException -- which extends CairoException, so a racing SELECT
        // surfaces it as an ordinary query error instead of opening a reader over half-swapped
        // files. unlockReconcileReads restores full access.
        assertMemoryLeak(() -> {
            execute("create table x (a int, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into x values (1, '2024-01-01T00:00:00Z')");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("x");

            engine.lockReconcileReads(token);
            try {
                assertReconcileReadLocked(() -> engine.getReader("x"));
                assertReconcileReadLocked(() -> engine.getReader(token));
                assertReconcileReadLocked(() -> engine.getReader(token, null));
                assertReconcileReadLocked(() -> engine.getReader(token, -1, null));
                assertReconcileReadLocked(() -> engine.getTableMetadata(token));
                assertReconcileReadLocked(() -> engine.getTableMetadata(token, -1));
                // Sequencer metadata is the write-compile path (getMetadataForWrite -> getLegacyMetadata
                // -> getSequencerMetadata for WAL tables); it must refuse the dir too, or an UPDATE /
                // INSERT-SELECT / ALTER could open metadata over half-swapped sequencer files.
                assertReconcileReadLocked(() -> engine.getSequencerMetadata(token));
                assertReconcileReadLocked(() -> engine.getSequencerMetadata(token, -1));
                assertReconcileReadLocked(() -> engine.getLegacyMetadata(token));
                assertReconcileReadLocked(() -> engine.getLegacyMetadata(token, -1));

                // A SELECT resolves through the same guarded entry points; the compiler catches the
                // EntryLockedException during table resolution and surfaces it as a sensible
                // "table is locked" error rather than opening a reader over half-swapped files.
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    try (RecordCursorFactory factory = compiler.compile("select * from x", sqlExecutionContext).getRecordCursorFactory();
                         RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                        }
                        Assert.fail("expected the SELECT to fail while the reconcile read lock is held");
                    }
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table is locked");
                }
            } finally {
                engine.unlockReconcileReads(token);
            }

            // Unlock restores full access: readers, metadata and the SELECT all work again.
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals(1, reader.size());
            }
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                Assert.assertEquals(2, metadata.getColumnCount());
            }
            Misc.freeIfCloseable(engine.getSequencerMetadata(token)); // sequencer metadata opens again too
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try (RecordCursorFactory factory = compiler.compile("select count() from x", sqlExecutionContext).getRecordCursorFactory();
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(1, cursor.getRecord().getLong(0));
                }
            }
        });
    }

    @Test
    public void testGetReaderWithRepairSkipsRepairUnderReconcileLock() throws Exception {
        // M3: getReaderWithRepair caught EntryLockedException in its generic `catch (CairoException)`
        // and ran tryRepairTable -- which opens a TableWriter (bypassing the write-suspend gate) and
        // logs a misleading "starting table repair" -- even though the table is only locked by an
        // in-progress RECONCILE, not corrupt. It must now rethrow EntryLockedException directly so the
        // sole caller (DatabaseCheckpointAgent) retries once the reconcile releases the lock.
        assertMemoryLeak(() -> {
            execute("create table x (a int, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into x values (1, '2024-01-01T00:00:00Z')");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("x");

            final LogCapture capture = new LogCapture();
            capture.start();
            engine.lockReconcileReads(token);
            try {
                try {
                    Misc.freeIfCloseable(engine.getReaderWithRepair(token));
                    Assert.fail("expected EntryLockedException while the reconcile read lock is held");
                } catch (EntryLockedException expected) {
                    TestUtils.assertContains(expected.getFlyweightMessage(), "reconcile in progress");
                }
                // The console log writer is async, so drain the FIFO past any repair log by emitting
                // and waiting for a unique sentinel before asserting the repair message is absent.
                execute("create table m3_repair_log_drain_sentinel (a int)");
                capture.waitFor("m3_repair_log_drain_sentinel");
                Assert.assertFalse(
                        "getReaderWithRepair must not run tryRepairTable for a transient reconcile lock",
                        capture.captured().contains("starting table repair"));
            } finally {
                engine.unlockReconcileReads(token);
                capture.stop();
            }
        });
    }

    @Test
    public void testRemoveExisting() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 1);
            spinLockTimeout = 1;
            try (CairoEngine engine = new CairoEngine(configuration)) {
                TableToken x = createX(engine);
                assertReader(engine, x);
                assertWriter(engine, x);
                engine.dropTableOrViewOrMatView(path, x);
                Assert.assertEquals(TableUtils.TABLE_DOES_NOT_EXIST, engine.getTableStatus(path, x));

                try {
                    engine.getReader(x);
                    Assert.fail();
                } catch (CairoException ignored) {
                }

                try {
                    getWriter(x);
                    Assert.fail();
                } catch (CairoException ignored) {
                }
            }
        });
    }

    @Test
    public void testRemoveNewTable() {
        try (CairoEngine engine = new CairoEngine(configuration)) {
            TableToken x = createX(engine);
            engine.dropTableOrViewOrMatView(path, x);
            Assert.assertEquals(TableUtils.TABLE_DOES_NOT_EXIST, engine.getTableStatus(path, x));
        }
    }

    @Test
    public void testRemoveNonExisting() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                createY(engine);
                try {
                    engine.dropTableOrViewOrMatView(path, engine.verifyTableName("x"));
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist");
                }
            }
        });
    }

    @Test
    public void testRemoveWhenReaderBusy() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                TableToken x = createX(engine);
                try (TableReader reader = engine.getReader(x)) {
                    Assert.assertNotNull(reader);
                    try {
                        engine.dropTableOrViewOrMatView(path, x);
                        Assert.fail();
                    } catch (CairoException ignored) {
                    }
                }
            }
        });
    }

    @Test
    public void testRemoveWhenWriterBusy() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                TableToken x = createX(engine);
                try (TableWriter writer = getWriter(engine, x.getTableName())) {
                    Assert.assertNotNull(writer);
                    try {
                        engine.dropTableOrViewOrMatView(path, x);
                        Assert.fail();
                    } catch (CairoException ignored) {
                    }
                }
            }
        });
    }

    @Test
    public void testRenameExisting() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                TableToken x = createX(engine);
                assertWriter(engine, x);
                assertReader(engine, x);


                try (MemoryMARW mem = Vm.getCMARWInstance()) {
                    TableToken y = engine.rename(securityContext, path, mem, "x", otherPath, "y");

                    assertWriter(engine, y);
                    assertReader(engine, y);

                    Assert.assertTrue(engine.clear());
                }
            }
        });
    }

    @Test
    public void testRenameExternallyLockedTable() throws Exception {
        assertMemoryLeak(() -> {
            TableToken x = createX(engine);
            try (TableWriter ignored1 = newOffPoolWriter(configuration, "x")) {

                try (CairoEngine engine = new CairoEngine(configuration)) {
                    try {
                        getWriter(x);
                        Assert.fail();
                    } catch (CairoException ignored) {
                    }

                    try (MemoryMARW mem = Vm.getCMARWInstance()) {
                        engine.rename(securityContext, path, mem, "x", otherPath, "y");
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "table busy [reason=missing or owned by other process]");
                    }
                }
            }
        });
    }

    @Test
    public void testRenameFail() throws Exception {
        assertMemoryLeak(() -> {

            TestFilesFacade ff = new TestFilesFacade() {
                int counter = 1;

                @Override
                public int rename(LPSZ from, LPSZ to) {
                    return counter-- <= 0 && super.rename(from, to) == Files.FILES_RENAME_OK ? Files.FILES_RENAME_OK : Files.FILES_RENAME_ERR_OTHER;
                }

                @Override
                public boolean wasCalled() {
                    return counter < 1;
                }
            };
            AbstractCairoTest.ff = ff;

            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    MemoryMARW mem = Vm.getCMARWInstance()
            ) {
                TableToken x = createX(engine);

                assertReader(engine, x);
                assertWriter(engine, x);

                try {
                    engine.rename(securityContext, path, mem, "x", otherPath, "y");
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not rename");
                }

                assertReader(engine, x);
                assertWriter(engine, x);
                TableToken y = engine.rename(securityContext, path, mem, "x", otherPath, "y");
                assertReader(engine, y);
                assertWriter(engine, y);
            }

            Assert.assertTrue(ff.wasCalled());
        });
    }

    @Test
    public void testRenameNonExisting() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "z", PartitionBy.NONE).col("a", ColumnType.INT);
            AbstractCairoTest.create(model);

            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    MemoryMARW mem = Vm.getCMARWInstance()
            ) {
                engine.rename(securityContext, path, mem, "x", otherPath, "y");
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "does not exist");
            }
        });
    }

    @Test
    public void testRenameToExistingTarget() throws Exception {
        assertMemoryLeak(() -> {

            try (CairoEngine engine = new CairoEngine(configuration)) {
                TableToken x = createX(engine);
                TableToken y = createY(engine);

                assertWriter(engine, x);
                assertReader(engine, x);
                try (MemoryMARW mem = Vm.getCMARWInstance()) {
                    engine.rename(securityContext, path, mem, "x", otherPath, "y");
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "exists");
                }
                assertWriter(engine, x);
                assertReader(engine, x);

                assertReader(engine, y);
                assertWriter(engine, y);
            }
        });
    }

    @Test
    public void testTheMaintenanceJobDoesNotObstructTableLocking() throws Exception {
        final String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            // the test relies on negative inactive writer TTL - we want the maintenance job to always close idle writers
            assert engine.getConfiguration().getInactiveWriterTTL() < 0;

            try (WorkerPool workerPool = new TestWorkerPool(1, configuration.getMetrics())) {
                TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                        .col("a", ColumnType.BYTE)
                        .col("b", ColumnType.STRING)
                        .timestamp("ts");
                Job job = new ServerMain.EngineMaintenanceJob(engine);
                workerPool.assign(job);
                workerPool.start();

                Rnd rnd = new Rnd();
                for (int i = 0; i < 50; i++) {
                    createTable(model);// create a table eligible for maintenance
                    Os.sleep(rnd.nextInt(10)); // give the maintenance job a chance to run
                    execute("drop table " + tableName); // drop the table. this should always pass. regardless of the maintenance job.
                }
            }
        });
    }

    @Test
    public void testWrongReaderVersion() throws Exception {

        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                TableToken x = createX(engine);

                assertWriter(engine, x);
                try {
                    engine.getReader(x, 2, null);
                    Assert.fail();
                } catch (TableReferenceOutOfDateException ignored) {
                }
                Assert.assertTrue(engine.clear());
            }
        });
    }

    private static void assertReconcileReadLocked(Supplier<?> readerOrMetadata) {
        try {
            // On the bug path the call would hand back a reader/metadata over the locked dir; free
            // it so the failure is a clean assertion rather than a masked resource leak.
            Misc.freeIfCloseable(readerOrMetadata.get());
            Assert.fail("expected EntryLockedException while the reconcile read lock is held");
        } catch (EntryLockedException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "reconcile in progress");
        }
    }

    private static void waitForTableStatus(int status) {
        while (engine.getTableStatus("x") != status) {
            Os.sleep(50);
        }
    }

    private void assertReader(CairoEngine engine, TableToken name) {
        try (TableReader reader = engine.getReader(name)) {
            Assert.assertNotNull(reader);
        }
    }

    private void assertWriter(CairoEngine engine, TableToken name) {
        try (TableWriter w = engine.getWriter(name, "testing")) {
            Assert.assertNotNull(w);
        }
    }

    private TableToken createX(CairoEngine engine) {
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("a", ColumnType.INT);
        return TestUtils.createTable(engine, model);
    }

    private TableToken createY(CairoEngine engine) {
        TableModel model = new TableModel(configuration, "y", PartitionBy.NONE).col("b", ColumnType.INT);
        return TestUtils.createTable(engine, model);
    }
}
