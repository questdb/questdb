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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.wal.UnsupportedWalTxnTypeHandler;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalTxnTypeHandler;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.mp.SCSequence;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.tasks.TableWriterTask;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.sql.AsyncWriterCommand.Error.CAIRO_ERROR;
import static io.questdb.cairo.wal.WalUtils.CONVERT_FILE_NAME;
import static io.questdb.tasks.TableWriterTask.TSK_COMPLETE;

public class DeltaTableAlterTest extends AbstractCairoTest {
    private static boolean isDeltaSupported;
    private static boolean isEmptySwitchHandlerEnabled;
    private static boolean isReadOnly;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public WalTxnTypeHandler getWalTxnTypeHandler() {
                if (!isEmptySwitchHandlerEnabled) {
                    return super.getWalTxnTypeHandler();
                }
                return new WalTxnTypeHandler() {
                    @Override
                    public void applyDeltaTableSwitch(TableWriter writer, long seqTxn) {
                        // Exercise F1 publication on an empty table; there is no source to adopt.
                        Assert.assertEquals(0, writer.getTxWriter().getPartitionCount());
                        writer.completeDeltaTableSwitch(seqTxn);
                    }

                    @Override
                    public int applyUnknownWalTxn(byte type, TableWriter writer, WalEventCursor cursor, long seqTxn) {
                        return UnsupportedWalTxnTypeHandler.INSTANCE.applyUnknownWalTxn(type, writer, cursor, seqTxn);
                    }
                };
            }

            @Override
            public boolean isDeltaTableSupported() {
                return isDeltaSupported;
            }

            @Override
            public boolean isReadOnlyMode() {
                return isReadOnly || super.isReadOnlyMode();
            }

            @Override
            protected TableWriterAPI createDeltaTableWriter(TableToken token, String reason) {
                // A close-only fixture verifies handoff ordering; it cannot accept rows or commits.
                return (TableWriterAPI) Proxy.newProxyInstance(
                        TableWriterAPI.class.getClassLoader(), new Class<?>[]{TableWriterAPI.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("close")) {
                                return null;
                            }
                            throw new UnsupportedOperationException("F1 empty-table fixture cannot write");
                        }
                );
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void setUp() {
        isDeltaSupported = true;
        isEmptySwitchHandlerEnabled = false;
        isReadOnly = false;
        super.setUp();
    }

    @Test
    public void testAlreadyDeltaAlterDoesNotMintSwitch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY DELTA");
            TableToken token = engine.verifyTableName("x");
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    OperationFuture future = compiler.compile("ALTER TABLE x SET TYPE DELTA", sqlExecutionContext).execute(null)
            ) {
                Assert.assertEquals(OperationFuture.QUERY_COMPLETE, future.getStatus());
            }
            Assert.assertEquals(0, engine.getTableSequencerAPI().lastTxn(token));
            Assert.assertTrue(engine.getUpdatedTableToken(token).isDelta());
        });
    }

    @Test
    public void testBorrowedWalWriterCannotChangeSchemaAfterHandoff() throws Exception {
        assertMemoryLeak(() -> {
            isEmptySwitchHandlerEnabled = true;
            TableToken token = createWalTable();
            try (
                    WalWriter borrowed = engine.getWalWriter(token);
                    SqlCompiler compiler = engine.getSqlCompiler();
                    OperationFuture future = compiler.compile("ALTER TABLE x SET TYPE DELTA", sqlExecutionContext).execute(null)
            ) {
                drainWalQueue();
                Assert.assertEquals(OperationFuture.QUERY_COMPLETE, future.await(1));
                Assert.assertTrue(engine.getUpdatedTableToken(token).isDelta());
                Assert.assertEquals(-1, engine.getTableSequencerAPI().getPendingDeltaTableSwitch(token));
                CairoException error = Assert.assertThrows(CairoException.class,
                        () -> borrowed.addColumn("late_column", ColumnType.LONG, sqlExecutionContext.getSecurityContext()));
                TestUtils.assertContains(error.getFlyweightMessage(), "WAL schema changes are not supported for Delta tables");
                Assert.assertEquals(1, engine.getTableSequencerAPI().lastTxn(token));
                try (TableReader reader = engine.getReader(token); var metadata = engine.getSequencerMetadata(token)) {
                    Assert.assertEquals(2, reader.getMetadata().getColumnCount());
                    Assert.assertEquals(0, reader.getMetadataVersion());
                    Assert.assertEquals(-1, reader.getMetadata().getColumnIndexQuiet("late_column"));
                    Assert.assertEquals(2, metadata.getColumnCount());
                    Assert.assertEquals(0, metadata.getMetadataVersion());
                    Assert.assertEquals(-1, metadata.getColumnIndexQuiet("late_column"));
                }
            }
        });
    }

    @Test
    public void testCompileDoesNotSubmitSwitch() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createWalTable();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery query = compiler.compile("ALTER TABLE x SET TYPE DELTA", sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.ALTER, query.getType());
                Assert.assertEquals(AlterOperation.SET_TABLE_TYPE_DELTA, query.getAlterOperation().getCommand());
                query.closeAllButSelect();
            }
            assertWalUnchanged(token);
        });
    }

    @Test
    public void testDropTableFailsAwaitingSwitchFuture() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createWalTable();
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    OperationFuture future = compiler.compile("ALTER TABLE x SET TYPE DELTA", sqlExecutionContext).execute(null)
            ) {
                Assert.assertEquals(1, engine.getTableSequencerAPI().getPendingDeltaTableSwitch(token));
                execute("DROP TABLE x");
                CairoException error = Assert.assertThrows(CairoException.class, () -> future.await(10_000));
                TestUtils.assertContains(error.getFlyweightMessage(), "table does not exist");
            }
            drainWalQueue();
            assertTableDirectoryRemoved(token);
        });
    }

    @Test
    public void testDropTableWithSuspendedPendingSwitch() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createWalTable();
            execute("INSERT INTO x VALUES ('2020-01-01', 42)");
            drainWalQueue();
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    OperationFuture future = compiler.compile("ALTER TABLE x SET TYPE DELTA", sqlExecutionContext).execute(null)
            ) {
                drainWalQueue();
                Assert.assertThrows(SqlException.class, () -> future.await(1));
            }
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(token));
            Assert.assertEquals(2, engine.getTableSequencerAPI().getPendingDeltaTableSwitch(token));
            execute("DROP TABLE x");
            Assert.assertTrue(engine.isTableDropped(token));
            drainWalQueue();
            assertTableDirectoryRemoved(token);
            TableToken recreated = createWalTable();
            Assert.assertNotEquals(token.getTableId(), recreated.getTableId());
            Assert.assertFalse(recreated.isDelta());
            Assert.assertEquals(-1, engine.getTableSequencerAPI().getPendingDeltaTableSwitch(recreated));
            execute("INSERT INTO x VALUES ('2020-01-02', 7)");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(recreated));
            assertQuery("SELECT v FROM x").noLeakCheck().expectSize().returns("v\n7\n");
        });
    }

    @Test
    public void testExecutionReauthorizesCachedAlter() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createWalTable();
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1)
            ) {
                CompiledQuery query = compiler.compile("ALTER TABLE x SET TYPE DELTA", sqlExecutionContext);
                context.with(ReadOnlySecurityContext.INSTANCE);
                Assert.assertThrows(CairoException.class, () -> query.execute(context, null, true));
            }
            assertWalUnchanged(token);
        });
    }

    @Test
    public void testForceDropRefusesFinalizedDeltaTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY DELTA");
            TableToken token = engine.verifyTableName("x");
            assertForceDropRefused();
            Assert.assertEquals(0, engine.getTableSequencerAPI().lastTxn(token));
            Assert.assertTrue(engine.getUpdatedTableToken(token).isDelta());
        });
    }

    @Test
    public void testForceDropRefusesSuspendedPendingSwitch() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createWalTable();
            execute("INSERT INTO x VALUES ('2020-01-01', 42)");
            drainWalQueue();
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    OperationFuture future = compiler.compile("ALTER TABLE x SET TYPE DELTA", sqlExecutionContext).execute(null)
            ) {
                drainWalQueue();
                Assert.assertThrows(SqlException.class, () -> future.await(1));
                assertForceDropRefused();
                Assert.assertEquals(2, engine.getTableSequencerAPI().getPendingDeltaTableSwitch(token));
                Assert.assertEquals(2, engine.getTableSequencerAPI().lastTxn(token));
                assertQuery("SELECT count() FROM x").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
            }
        });
    }

    @Test
    public void testInlineCommandRefusesDeltaWriterAfterStaleAdmissionCheck() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createWalTable();
            try (TableWriter writer = engine.getWriter(token, "test")) {
                writer.completeDeltaTableSwitch(0);
            }
            // Model an admission check that observed WAL just before conversion completed.
            // The registry still passes that check; the owned writer has the finalized mode.
            Assert.assertFalse(engine.getUpdatedTableToken(token).isDelta());
            for (boolean hasWriterIdSink : new boolean[]{false, true}) {
                try (AlterOperation operation = new AlterOperationBuilder()
                        .ofSetParamUncommittedRows(0, token, token.getTableId(), 99)
                        .build()) {
                    CairoException error = Assert.assertThrows(CairoException.class, () -> {
                        try (TableWriter ignored = hasWriterIdSink
                                ? engine.getWriterOrPublishCommand(token, operation, writerId -> Assert.fail("command was queued"))
                                : engine.getWriterOrPublishCommand(token, operation)) {
                            Assert.fail("acquired native writer for Delta command");
                        }
                    });
                    TestUtils.assertContains(error.getFlyweightMessage(), "legacy writer commands are not supported for Delta tables");
                }
                // Refusal must return the writer to its pool.
                try (TableWriter writer = engine.getWriter(token, "test")) {
                    Assert.assertNotEquals(99, writer.getMetadata().getMaxUncommittedRows());
                }
            }
        });
    }

    @Test
    public void testMissingProviderRefusesBeforeSwitch() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createWalTable();
            isDeltaSupported = false;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery query = compiler.compile("ALTER TABLE x SET TYPE DELTA", sqlExecutionContext);
                Assert.assertThrows(CairoException.class, () -> query.execute(null));
            }
            assertWalUnchanged(token);
        });
    }

    @Test
    public void testPendingFutureAndCloseDoNotAcknowledgeOrCancelSwitch() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createWalTable();
            TableSequencerAPI api = engine.getTableSequencerAPI();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try (OperationFuture future = compiler.compile("ALTER TABLE x SET TYPE DELTA", sqlExecutionContext).execute(null)) {
                    Assert.assertEquals(1, api.lastTxn(token));
                    Assert.assertEquals(OperationFuture.QUERY_STARTED, future.await(1));
                }
                // The caller's timeout/disconnect closes only the future, not the accepted switch.
                try (OperationFuture retry = compiler.compile("ALTER TABLE x SET TYPE DELTA", sqlExecutionContext).execute(null)) {
                    Assert.assertEquals(1, api.lastTxn(token));
                    Assert.assertEquals(OperationFuture.QUERY_STARTED, retry.await(1));
                }
            }
            Assert.assertThrows(CairoException.class, () -> api.nextTxn(token, 1, 0, 0, 0, 0, 0, 1));
            Assert.assertFalse(engine.getUpdatedTableToken(token).isDelta());
        });
    }

    @Test
    public void testQueuedCommandDefersUntilSwitchAndThenFailsWithoutMutation() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createWalTable();
            SCSequence events = new SCSequence();
            engine.getMessageBus().getTableWriterEventFanOut().and(events);
            try (AlterOperation operation = new AlterOperationBuilder()
                    .ofSetParamUncommittedRows(0, token, token.getTableId(), 99)
                    .build()) {
                final long correlation = engine.getCommandCorrelationId();
                operation.setCommandCorrelationId(correlation);
                final long seqTxn;
                final int originalMaxUncommittedRows;
                try (TableWriter writer = engine.getWriter(token, "hold for queued command")) {
                    originalMaxUncommittedRows = writer.getMetadata().getMaxUncommittedRows();
                    Assert.assertNotEquals(99, originalMaxUncommittedRows);
                    AtomicReference<Throwable> error = new AtomicReference<>();
                    Thread publisher = new Thread(() -> {
                        try (TableWriter acquired = engine.getWriterOrPublishCommand(token, operation)) {
                            Assert.assertNull("another thread owns the writer, so command must queue", acquired);
                        } catch (Throwable th) {
                            error.set(th);
                        } finally {
                            Path.clearThreadLocals();
                        }
                    }, "delta-queued-command");
                    publisher.start();
                    publisher.join(30_000);
                    Assert.assertFalse(publisher.isAlive());
                    if (error.get() != null) {
                        throw new AssertionError(error.get());
                    }
                    seqTxn = engine.getTableSequencerAPI().beginDeltaTableSwitch(token, TestUtils.getSequencerMetadataVersion(engine, token));
                    writer.tick();
                    Assert.assertEquals(-1, events.next());
                    Assert.assertEquals(originalMaxUncommittedRows, writer.getMetadata().getMaxUncommittedRows());
                    writer.completeDeltaTableSwitch(seqTxn);
                }
                // Complete only the empty-table handoff fixture; no direct ingestion is exercised.
                engine.finishDeltaTableSwitch(token);
                try (TableWriter writer = engine.getWriter(token, "drain legacy command")) {
                    writer.tick();
                    Assert.assertEquals(originalMaxUncommittedRows, writer.getMetadata().getMaxUncommittedRows());
                    Assert.assertEquals(seqTxn, writer.getSeqTxn());
                }
                boolean isRejected = false;
                long cursor;
                while ((cursor = events.next()) >= 0) {
                    try {
                        TableWriterTask event = engine.getMessageBus().getTableWriterEventQueue().get(cursor);
                        if (event.getInstance() == correlation && event.getType() == TSK_COMPLETE) {
                            Assert.assertEquals(CAIRO_ERROR, Unsafe.getInt(event.getData()));
                            isRejected = true;
                        }
                    } finally {
                        events.done(cursor);
                    }
                }
                Assert.assertTrue("the queued command must receive an explicit rejection", isRejected);
                Assert.assertEquals(seqTxn, engine.getTableSequencerAPI().lastTxn(token));
            } finally {
                engine.getMessageBus().getTableWriterEventFanOut().remove(events);
            }
        });
    }

    @Test
    public void testReadOnlyExecutionRefusesBeforeSwitch() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createWalTable();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery query = compiler.compile("ALTER TABLE x SET TYPE DELTA", sqlExecutionContext);
                isReadOnly = true;
                try {
                    CairoException error = Assert.assertThrows(CairoException.class, () -> query.execute(null));
                    Assert.assertTrue(error.isReadOnlyAccessRefusal());
                } finally {
                    isReadOnly = false;
                }
            }
            assertWalUnchanged(token);
        });
    }

    @Test
    public void testSuspendedApplyFailsFutureWithoutCompletingSwitch() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createWalTable();
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    OperationFuture future = compiler.compile("ALTER TABLE x SET TYPE DELTA", sqlExecutionContext).execute(null)
            ) {
                // OSS has no adoption handler; the admitted internal fixture must suspend, not succeed.
                drainWalQueue();
                SqlException error = Assert.assertThrows(SqlException.class, () -> future.await(1));
                TestUtils.assertContains(error.getFlyweightMessage(), "Delta table switch is suspended");
                Assert.assertEquals(OperationFuture.QUERY_STARTED, future.getStatus());
                Assert.assertEquals(1, engine.getTableSequencerAPI().lastTxn(token));
                Assert.assertFalse(engine.getUpdatedTableToken(token).isDelta());
            }
        });
    }

    @Test
    public void testSuspendedFutureCompletesAfterResume() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createWalTable();
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    OperationFuture future = compiler.compile("ALTER TABLE x SET TYPE DELTA", sqlExecutionContext).execute(null)
            ) {
                drainWalQueue();
                Assert.assertThrows(SqlException.class, () -> future.await(1));
                isEmptySwitchHandlerEnabled = true;
                engine.getTableSequencerAPI().resumeTable(token, 0);
                drainWalQueue();
                Assert.assertEquals(OperationFuture.QUERY_COMPLETE, future.await(1));
                Assert.assertEquals(1, engine.getTableSequencerAPI().lastTxn(token));
                Assert.assertTrue(engine.getUpdatedTableToken(token).isDelta());
            }
        });
    }

    @Test
    public void testTimedOutFutureCompletesAndRepeatDoesNotMintSwitch() throws Exception {
        assertMemoryLeak(() -> {
            isEmptySwitchHandlerEnabled = true;
            TableToken token = createWalTable();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try (OperationFuture future = compiler.compile("ALTER TABLE x SET TYPE DELTA", sqlExecutionContext).execute(null)) {
                    Assert.assertEquals(OperationFuture.QUERY_STARTED, future.await(1));
                    drainWalQueue();
                    Assert.assertEquals(OperationFuture.QUERY_COMPLETE, future.await(1));
                }
                try (OperationFuture repeat = compiler.compile("ALTER TABLE x SET TYPE DELTA", sqlExecutionContext).execute(null)) {
                    Assert.assertEquals(OperationFuture.QUERY_COMPLETE, repeat.getStatus());
                }
            }
            Assert.assertEquals(1, engine.getTableSequencerAPI().lastTxn(token));
            Assert.assertTrue(engine.getUpdatedTableToken(token).isDelta());
        });
    }

    @Test
    public void testValidationOnlyExecutionDoesNotSubmitSwitch() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createWalTable();
            SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
            context.setValidationOnly(true);
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    OperationFuture future = compiler.compile("ALTER TABLE x SET TYPE DELTA", context).execute(null)
            ) {
                Assert.assertEquals(OperationFuture.QUERY_COMPLETE, future.getStatus());
            } finally {
                context.setValidationOnly(false);
            }
            assertWalUnchanged(token);
        });
    }

    private static void assertForceDropRefused() throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            CompiledQuery query = compiler.compile("ALTER TABLE x FORCE DROP PARTITION LIST '2020-01-01'", sqlExecutionContext);
            CairoException error = Assert.assertThrows(CairoException.class, () -> query.execute(null));
            TestUtils.assertContains(error.getFlyweightMessage(), "force WAL bypass is not supported during or after Delta conversion");
        }
    }

    private static void assertTableDirectoryRemoved(TableToken token) {
        try (Path path = new Path()) {
            // The apply job purges table files; WalPurgeJob then removes the WAL and sequencer directories.
            Assert.assertFalse(configuration.getFilesFacade().exists(path.of(root).concat(token).concat(TableUtils.TXN_FILE_NAME).$()));
            engine.releaseInactiveTableSequencers();
            try (WalPurgeJob job = new WalPurgeJob(engine, configuration.getFilesFacade(), configuration.getMicrosecondClock())) {
                job.run();
            }
            Assert.assertFalse(configuration.getFilesFacade().exists(path.of(root).concat(token).$()));
        }
    }

    private static void assertWalUnchanged(TableToken token) {
        Assert.assertEquals(0, engine.getTableSequencerAPI().lastTxn(token));
        Assert.assertFalse(engine.getUpdatedTableToken(token).isDelta());
        try (Path path = new Path()) {
            Assert.assertFalse(configuration.getFilesFacade().exists(path.of(root).concat(token).concat(CONVERT_FILE_NAME).$()));
        }
    }

    private static TableToken createWalTable() throws SqlException {
        execute("CREATE TABLE x (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        return engine.verifyTableName("x");
    }
}
