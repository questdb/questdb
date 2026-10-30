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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableFlagResolverImpl;
import io.questdb.cairo.TableNameRegistryRO;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriteMode;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.wal.UnsupportedWalTxnTypeHandler;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalTxnTypeHandler;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.cairo.CairoTestConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Proxy;

import static io.questdb.cairo.wal.WalUtils.DELTA_TABLE_SWITCH_WAL_ID;
import static io.questdb.cairo.wal.WalUtils.SEQ_DIR;
import static io.questdb.cairo.wal.WalUtils.TXNLOG_PARTS_DIR;

/** Tests publication/recovery on empty tables, without pretending to implement F2's writer. */
public class DeltaTableSwitchRecoveryTest extends AbstractCairoTest {
    private static final FilesFacade FAULT_FF = new TestFilesFacadeImpl() {
        @Override
        public void msync(long addr, long len, boolean async) {
            if (snapshotSyncArmed) {
                snapshotSyncArmed = false;
                failSnapshotSync = false;
                throw CairoException.critical(0).put("injected snapshot sync failure");
            }
            super.msync(addr, len, async);
        }

        @Override
        public int rename(LPSZ from, LPSZ to) {
            if (failurePath != null && Utf8s.containsAscii(from, failurePath)) {
                return Files.FILES_RENAME_ERR_OTHER;
            }
            return super.rename(from, to);
        }
    };
    private static boolean failProvider;
    private static boolean failSnapshotSync;
    private static String failurePath;
    private static boolean snapshotSyncArmed;
    private static int hookCalls;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        configurationFactory = (root, telemetry, overrides) -> new CairoTestConfiguration(root, telemetry, overrides) {
            @Override
            public FilesFacade getFilesFacade() {
                return FAULT_FF;
            }
        };
        engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public WalTxnTypeHandler getWalTxnTypeHandler() {
                return new WalTxnTypeHandler() {
                    @Override
                    public void applyDeltaTableSwitch(TableWriter writer, long seqTxn) {
                        Assert.assertEquals(0, writer.getTxWriter().getPartitionCount());
                        hookCalls++;
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
                return true;
            }

            @Override
            protected TableWriterAPI createDeltaTableWriter(TableToken token, String reason) {
                if (failProvider) {
                    throw CairoException.critical(0).put("injected provider initialization failure");
                }
                snapshotSyncArmed = failSnapshotSync;
                return (TableWriterAPI) Proxy.newProxyInstance(
                        TableWriterAPI.class.getClassLoader(), new Class<?>[]{TableWriterAPI.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("close")) {
                                return null;
                            }
                            throw new UnsupportedOperationException("F1 recovery fixture cannot write");
                        }
                );
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void setUp() {
        failProvider = false;
        failSnapshotSync = false;
        failurePath = null;
        snapshotSyncArmed = false;
        hookCalls = 0;
        super.setUp();
    }

    @Test
    public void testAppliedSwitchWithWalModeIsFoundAfterTrackerReopenV1() throws Exception {
        testAppliedSwitch(0);
    }

    @Test
    public void testAppliedSwitchWithWalModeIsFoundAfterTrackerReopenV2() throws Exception {
        testAppliedSwitch(2);
    }

    @Test
    public void testInterruptedMetadataSwap() throws Exception {
        testPublicationFailure("_meta.swp");
    }

    @Test
    public void testInterruptedRegistrySnapshot() throws Exception {
        testPublicationFailure("tables.d.tmp");
    }

    @Test
    public void testProviderFailureRetainsGateUntilResume() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createTable();
            long seqTxn = engine.getTableSequencerAPI().beginDeltaTableSwitch(token, TestUtils.getSequencerMetadataVersion(engine, token));
            failProvider = true;
            drainWalQueue();
            assertPending(token, seqTxn);
            try (TableReader reader = engine.getReader(token); TableReaderMetadata metadata = new TableReaderMetadata(configuration, token)) {
                Assert.assertEquals(seqTxn, reader.getTxFile().getSeqTxn());
                metadata.loadMetadata();
                Assert.assertEquals(TableWriteMode.DELTA_RUNS, metadata.getWriteMode());
            }
            failProvider = false;
            engine.getTableSequencerAPI().releaseAll();
            engine.getTableSequencerAPI().resumeTable(token, -1);
            drainWalQueue();
            assertCompleted(token, seqTxn);
            Assert.assertEquals(2, hookCalls);
        });
    }

    @Test
    public void testPurgeRetainsAppliedSwitchWhileModeIsPendingV2() throws Exception {
        testPurgeRetainsAppliedSwitch(false);
    }

    @Test
    public void testPurgeRetainsAppliedSwitchWhileProviderIsPendingV2() throws Exception {
        testPurgeRetainsAppliedSwitch(true);
    }

    @Test
    public void testSnapshotSyncFailureKeepsUnrelatedRegistryWrites() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createTable();
            long seqTxn = engine.getTableSequencerAPI().beginDeltaTableSwitch(token, TestUtils.getSequencerMetadataVersion(engine, token));
            failSnapshotSync = true;
            drainWalQueue();
            assertPending(token, seqTxn);
            execute("CREATE TABLE unrelated (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (TableNameRegistryRO registry = new TableNameRegistryRO(
                    engine, new TableFlagResolverImpl(configuration.getSystemTableNamePrefix().toString())
            )) {
                registry.reload();
                Assert.assertNotNull(registry.getTableToken("unrelated"));
                Assert.assertFalse(registry.getTableToken("x").isDelta());
            }
            engine.getTableSequencerAPI().resumeTable(token, -1);
            drainWalQueue();
            assertCompleted(token, seqTxn);
        });
    }

    private static void assertCompleted(TableToken token, long seqTxn) {
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
        Assert.assertTrue(engine.isDeltaTableSwitchComplete(token, seqTxn));
        Assert.assertEquals(seqTxn, engine.getTableSequencerAPI().lastTxn(token));
        TableToken current = engine.verifyTableName(token.getTableName());
        Assert.assertEquals(token, current);
        Assert.assertTrue(current.isDelta());
        Assert.assertThrows(CairoException.class, () -> engine.getWalWriter(token));
        try (TableReader reader = engine.getReader(current)) {
            Assert.assertEquals(seqTxn, reader.getTxFile().getSeqTxn());
            Assert.assertEquals(0, reader.getMetadata().getMetadataVersion());
            Assert.assertEquals(TableWriteMode.DELTA_RUNS, reader.getMetadata().getWriteMode());
            Assert.assertEquals(0, reader.size());
        }
        try (TableNameRegistryRO registry = new TableNameRegistryRO(
                engine, new TableFlagResolverImpl(configuration.getSystemTableNamePrefix().toString())
        )) {
            registry.reload();
            Assert.assertEquals(token, registry.getTableToken(token.getTableName()));
            Assert.assertTrue(registry.getTableToken(token.getTableName()).isDelta());
        }
        Assert.assertTrue(engine.getTableSequencerAPI().releaseAll());
        Assert.assertEquals(-1, engine.getTableSequencerAPI().getPendingDeltaTableSwitch(token));
        Assert.assertEquals(0, engine.getTableSequencerAPI().beginDeltaTableSwitch(token, TestUtils.getSequencerMetadataVersion(engine, token)));
        Assert.assertEquals(seqTxn, engine.getTableSequencerAPI().lastTxn(token));
    }

    private static void assertPending(TableToken token, long seqTxn) {
        Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(token));
        Assert.assertFalse(engine.isDeltaTableSwitchComplete(token, seqTxn));
        Assert.assertFalse(engine.verifyTableName(token.getTableName()).isDelta());
        Assert.assertEquals(seqTxn, engine.getTableSequencerAPI().getPendingDeltaTableSwitch(token));
        Assert.assertThrows(CairoException.class, () -> engine.getTableWriterAPI(token, "refused"));
    }

    private TableToken createTable() throws Exception {
        execute("CREATE TABLE x (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        TableToken token = engine.verifyTableName("x");
        try (TableReader reader = engine.getReader(token)) {
            Assert.assertEquals(TableWriteMode.WAL_SEGMENTS, reader.getMetadata().getWriteMode());
        }
        return token;
    }

    private void testAppliedSwitch(int journalPartSize) throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, journalPartSize);
            node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
            TableToken token = createTable();
            long seqTxn = engine.getTableSequencerAPI().beginDeltaTableSwitch(token, TestUtils.getSequencerMetadataVersion(engine, token));
            // Reproduce a crash after durable table activation but before the mode-only swap.
            try (TableWriter writer = engine.getWriter(token, "crash fixture")) {
                writer.commitSeqTxn(seqTxn);
            }
            engine.releaseAllWriters();
            Assert.assertTrue(engine.getTableSequencerAPI().releaseAll());
            Assert.assertTrue(engine.getTableSequencerAPI().initTxnTracker(token, seqTxn, seqTxn));
            drainWalQueue();
            assertCompleted(token, seqTxn);
            Assert.assertEquals(1, hookCalls);
        });
    }

    private void testPublicationFailure(String sourceName) throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createTable();
            long seqTxn = engine.getTableSequencerAPI().beginDeltaTableSwitch(token, TestUtils.getSequencerMetadataVersion(engine, token));
            failurePath = sourceName;
            drainWalQueue();
            assertPending(token, seqTxn);
            failurePath = null;
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            Assert.assertTrue(engine.reloadTableNames());
            Assert.assertFalse(engine.verifyTableName("x").isDelta());
            Assert.assertTrue(engine.getTableSequencerAPI().releaseAll());
            engine.getTableSequencerAPI().resumeTable(token, -1);
            drainWalQueue();
            assertCompleted(token, seqTxn);
            Assert.assertEquals(2, hookCalls);
        });
    }

    private void testPurgeRetainsAppliedSwitch(boolean isProviderPending) throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 2);
            TableToken token = createTable();
            for (int i = 0; i < 4; i++) {
                execute("ALTER TABLE x SET PARAM maxUncommittedRows = " + (100 + i));
            }
            drainWalQueue();
            Assert.assertEquals(4, engine.getTableSequencerAPI().lastTxn(token));
            long seqTxn = engine.getTableSequencerAPI().beginDeltaTableSwitch(token, TestUtils.getSequencerMetadataVersion(engine, token));
            Assert.assertEquals(5, seqTxn);
            if (isProviderPending) {
                failProvider = true;
                drainWalQueue();
                assertPending(token, seqTxn);
            } else {
                // Crash boundary: table activation persisted, but mode still WAL.
                try (TableWriter writer = engine.getWriter(token, "crash before mode")) {
                    writer.commitSeqTxn(seqTxn);
                }
            }
            engine.releaseAllWriters();
            engine.releaseAllWalWriters();
            try (TableReader reader = engine.getReader(token); Path path = new Path()) {
                Assert.assertEquals(seqTxn, reader.getTxFile().getSeqTxn());
                path.of(root).concat(token).concat(SEQ_DIR).concat(TXNLOG_PARTS_DIR);
                int rootLen = path.size();
                for (int part = 0; part < 3; part++) {
                    Assert.assertTrue(configuration.getFilesFacade().exists(path.trimTo(rootLen).concat(Integer.toString(part)).$()));
                }
                drainPurgeJob();
                Assert.assertFalse("an old part below the adaptive recovery floor must be purged",
                        configuration.getFilesFacade().exists(path.trimTo(rootLen).concat("0").$()));
                // Adaptive commit may also retain part 1 for its older save point. The switch part itself must
                // survive regardless: mode/provider publication still needs the sequencer record after restart.
                Assert.assertTrue("the SWITCH part must survive",
                        configuration.getFilesFacade().exists(path.trimTo(rootLen).concat("2").$()));
            }
            Assert.assertTrue(engine.getTableSequencerAPI().releaseAll());
            Assert.assertEquals(seqTxn, engine.getTableSequencerAPI().getPendingDeltaTableSwitch(token));
            try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(token, seqTxn - 1)) {
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(seqTxn, cursor.getTxn());
                Assert.assertEquals(DELTA_TABLE_SWITCH_WAL_ID, cursor.getWalId());
                Assert.assertFalse(cursor.hasNext());
            }
            failProvider = false;
            engine.getTableSequencerAPI().resumeTable(token, -1);
            drainWalQueue();
            Assert.assertTrue(engine.isDeltaTableSwitchComplete(token, seqTxn));
            Assert.assertEquals(seqTxn, engine.getTableSequencerAPI().lastTxn(token));
            Assert.assertEquals(0, engine.getTableSequencerAPI().beginDeltaTableSwitch(token, TestUtils.getSequencerMetadataVersion(engine, token)));
        });
    }
}
