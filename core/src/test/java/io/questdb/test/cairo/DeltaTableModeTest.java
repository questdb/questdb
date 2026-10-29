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
import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableFlagResolverImpl;
import io.questdb.cairo.TableNameRegistryRO;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriteMode;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.mig.EngineMigration;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class DeltaTableModeTest extends AbstractCairoTest {
    private static int providerCalls;
    private static boolean rejectMetadataReads;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        engineFactory = configuration -> new CairoEngine(configuration) {
            @Override
            public TableMetadata getTableMetadata(TableToken token, long desiredVersion) {
                Assert.assertFalse("writer routing opened a metadata reader", rejectMetadataReads);
                return super.getTableMetadata(token, desiredVersion);
            }

            @Override
            public boolean isDeltaTableSupported() {
                return true;
            }

            @Override
            protected TableWriterAPI createDeltaTableWriter(TableToken token, String reason) {
                Assert.assertTrue("provider must receive the current Delta token", token.isDelta());
                providerCalls++;
                return super.createDeltaTableWriter(token, reason);
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testCheckpointRefusesDeltaAndMigrationPreservesVersion() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createDelta();
            EngineMigration.migrateEngineTo(engine, ColumnType.VERSION, ColumnType.MIGRATION_VERSION, true);
            assertMeta(token, TableUtils.META_FORMAT_VERSION_DELTA, TableWriteMode.DELTA_RUNS);
            final CairoException error = Assert.assertThrows(CairoException.class, () -> execute("CHECKPOINT CREATE"));
            TestUtils.assertContains(error.getFlyweightMessage(), "checkpoint and backup are not supported for Delta tables");
            Assert.assertFalse(engine.getCheckpointStatus().isInProgress());
            assertMeta(token, TableUtils.META_FORMAT_VERSION_DELTA, TableWriteMode.DELTA_RUNS);
        });
    }

    @Test
    public void testNativeTtlRefusesDeltaCreationAndConversion() throws Exception {
        assertMemoryLeak(() -> {
            final SqlException createError = Assert.assertThrows(SqlException.class, () -> execute(
                    "CREATE TABLE rejected (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY TTL 1 DAY DELTA"));
            TestUtils.assertContains(createError.getFlyweightMessage(), "TTL is not supported for Delta tables");
            Assert.assertNull(engine.getTableTokenIfExists("rejected"));

            execute("CREATE TABLE original (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY TTL 1 DAY WAL");
            final TableToken token = engine.verifyTableName("original");
            assertRefused(() -> engine.validateDeltaTableSwitch(token), "TTL is not supported for Delta tables");
            Assert.assertEquals(0, engine.getTableSequencerAPI().lastTxn(token));
            Assert.assertFalse(engine.verifyTableName("original").isDelta());
        });
    }

    @Test
    public void testCreateReopenAndMetadataRewrite() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createDelta();
            Assert.assertTrue(token.isWal());
            Assert.assertTrue(token.isDelta());
            Assert.assertEquals(TableToken.Type.TABLE, token.getType());
            assertEmptyDelta(token);
            assertMeta(token, TableUtils.META_FORMAT_VERSION_DELTA, TableWriteMode.DELTA_RUNS);
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals(0, reader.getTxn());
                Assert.assertEquals(0, reader.getTxFile().getSeqTxn());
                Assert.assertEquals(0, reader.getPartitionCount());
            }
            try (TableReader pinned = engine.getReader(token); TableWriter writer = engine.getWriter(token, "test metadata preservation")) {
                writer.setMetaMaxUncommittedRows(42);
                writer.addColumn("extra", ColumnType.LONG, sqlExecutionContext.getSecurityContext());
                Assert.assertTrue(pinned.reload());
                Assert.assertEquals(TableWriteMode.DELTA_RUNS, pinned.getMetadata().getWriteMode());
                Assert.assertEquals(TableWriteMode.DELTA_RUNS, writer.getMetadata().getWriteMode());
                assertRefused(() -> writer.newRow(0), "native row writes are not supported");
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertEmptyDelta(token);
            assertMeta(token, TableUtils.META_FORMAT_VERSION_DELTA, TableWriteMode.DELTA_RUNS);
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals(TableWriteMode.DELTA_RUNS, reader.getMetadata().getWriteMode());
                Assert.assertEquals(42, reader.getMetadata().getMaxUncommittedRows());
                Assert.assertEquals(3, reader.getMetadata().getColumnCount());
            }
            try (CairoEngine reopened = new CairoEngine(configuration)) {
                TableToken reopenedToken = reopened.verifyTableName("d");
                Assert.assertTrue(reopened.isDeltaTable(reopenedToken));
                try (TableReader reader = reopened.getReader(reopenedToken)) {
                    Assert.assertEquals(TableWriteMode.DELTA_RUNS, reader.getMetadata().getWriteMode());
                    Assert.assertEquals(0, reader.size());
                }
                assertRefused(() -> reopened.getTableWriterAPI(reopenedToken, "test"), "Delta table writer is not available");
            }
        });
    }

    @Test
    public void testDropRecreateCannotRouteUsingPreviousIdentity() throws Exception {
        assertMemoryLeak(() -> {
            TableToken previous = createDelta();
            Assert.assertTrue(engine.isDeltaTable(previous));
            execute("DROP TABLE d");
            execute("CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            TableToken current = engine.verifyTableName("d");
            Assert.assertNotEquals(previous, current);
            Assert.assertFalse(engine.isDeltaTable(current));
            Assert.assertThrows(TableReferenceOutOfDateException.class, () -> engine.isDeltaTable(previous));
            try (TableWriterAPI writer = engine.getTableWriterAPI(current, "test")) {
                Assert.assertTrue(writer instanceof WalWriter);
            }
        });
    }

    @Test
    public void testInvalidMetadataAndLegacyReservedBytes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE w (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            TableToken token = engine.verifyTableName("w");
            engine.releaseAllReaders();
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.META_FILE_NAME);
                mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
                mem.putInt(TableUtils.META_OFFSET_WRITE_MODE, 987654);
                TableUtils.validateMeta(path, mem, new LowerCaseCharSequenceIntHashMap(), ColumnType.VERSION);
                Assert.assertEquals(TableWriteMode.WAL_SEGMENTS, TableUtils.getWriteMode(mem));
                mem.putInt(TableUtils.META_OFFSET_VERSION, TableUtils.META_FORMAT_VERSION_DELTA);
                assertRefused(() -> TableUtils.validateMeta(path, mem, new LowerCaseCharSequenceIntHashMap(), ColumnType.VERSION), "invalid Delta table metadata");
                mem.putInt(TableUtils.META_OFFSET_WRITE_MODE, TableWriteMode.DELTA_RUNS);
                TableUtils.validateMeta(path, mem, new LowerCaseCharSequenceIntHashMap(), ColumnType.VERSION);
                assertRefused(() -> TableUtils.validateMetaVersion(path, mem, TableUtils.META_OFFSET_VERSION, ColumnType.VERSION, ColumnType.MAX_STORAGE_VERSION), "metadata version does not match");
                mem.putBool(TableUtils.META_OFFSET_WAL_ENABLED, false);
                assertRefused(() -> TableUtils.validateMeta(path, mem, new LowerCaseCharSequenceIntHashMap(), ColumnType.VERSION), "invalid Delta table metadata");
                mem.putBool(TableUtils.META_OFFSET_WAL_ENABLED, true);
                mem.putInt(TableUtils.META_OFFSET_PARTITION_BY, PartitionBy.NONE);
                assertRefused(() -> TableUtils.validateMeta(path, mem, new LowerCaseCharSequenceIntHashMap(), ColumnType.VERSION), "invalid Delta table metadata");
                mem.putInt(TableUtils.META_OFFSET_PARTITION_BY, PartitionBy.DAY);
                mem.putInt(TableUtils.META_OFFSET_VERSION, TableUtils.META_FORMAT_VERSION_DELTA + 1);
                assertRefused(() -> TableUtils.validateMeta(path, mem, new LowerCaseCharSequenceIntHashMap(), ColumnType.VERSION), "metadata version does not match");
                mem.putInt(TableUtils.META_OFFSET_VERSION, ColumnType.VERSION);
            }
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals(TableWriteMode.WAL_SEGMENTS, reader.getMetadata().getWriteMode());
            }
        });
    }

    @Test
    public void testInvalidStructureRejectedBeforeFiles() throws Exception {
        assertMemoryLeak(() -> {
            TableModel unknown = new TableModel(configuration, "invalid", PartitionBy.DAY) {
                @Override
                public int getWriteMode() {
                    return 2;
                }
            };
            unknown.timestamp().wal();
            assertRefused(() -> createThroughEngine(unknown), "unsupported table write mode");
            Assert.assertNull(engine.getTableTokenIfExists("invalid"));
            TableModel nonWal = new TableModel(configuration, "invalid", PartitionBy.DAY) {
                @Override
                public int getWriteMode() {
                    return TableWriteMode.DELTA_RUNS;
                }
            };
            nonWal.timestamp().noWal();
            assertRefused(() -> createThroughEngine(nonWal), "Delta tables require WAL sequencing");
            Assert.assertNull(engine.getTableTokenIfExists("invalid"));
        });
    }

    @Test
    public void testMissingOrMismatchedCacheUsesTokenWithoutMetadata() throws Exception {
        assertMemoryLeak(() -> {
            TableToken delta = createDelta();
            execute("CREATE TABLE w (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            TableToken wal = engine.verifyTableName("w");
            try (MetadataCacheWriter cache = engine.getMetadataCache().writeLock()) {
                cache.clearCache();
            }
            Assert.assertTrue(engine.isDeltaTable(delta));
            Assert.assertFalse(engine.isDeltaTable(wal));
            // Simulate the cache being behind the registry while two names are being exchanged.
            try (MetadataCacheWriter cache = engine.getMetadataCache().writeLock()) {
                cache.hydrateTable(wal);
                cache.renameTable(wal, wal.renamed("d"));
            }
            rejectMetadataReads = true;
            try {
                Assert.assertTrue(engine.isDeltaTable(delta));
                Assert.assertFalse(engine.isDeltaTable(wal));
                assertRefused(() -> engine.getTableWriterAPI(delta, "test"), "Delta table writer is not available");
                assertRefused(() -> engine.getTableWriterAPI("d", "test"), "Delta table writer is not available");
                assertRefused(() -> engine.getWalWriter(delta), "WAL writers are not supported");
            } finally {
                rejectMetadataReads = false;
            }
        });
    }

    @Test
    public void testRegistryDiscoveryPreservesDeltaMode() throws Exception {
        assertMemoryLeak(() -> {
            TableToken delta = createDelta();
            engine.closeNameRegistry();
            Files.delete(java.nio.file.Path.of(root, "tables.d.0"));
            Assert.assertTrue(engine.reloadTableNames());
            assertTokenAndModeEquals(delta, engine.verifyTableName("d"));
            assertRefused(() -> engine.getWalWriter(delta), "WAL writers are not supported");
        });
    }

    @Test
    public void testRegistryDiscoveryRejectsInvalidDeltaMode() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createDelta();
            engine.closeNameRegistry();
            Files.delete(java.nio.file.Path.of(root, "tables.d.0"));
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                path.of(root).concat(token).concat(TableUtils.META_FILE_NAME);
                mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
                mem.putInt(TableUtils.META_OFFSET_WRITE_MODE, 2);
                try {
                    assertRefused(() -> engine.reloadTableNames(), "invalid Delta table metadata");
                } finally {
                    mem.putInt(TableUtils.META_OFFSET_WRITE_MODE, TableWriteMode.DELTA_RUNS);
                }
            }
            Assert.assertTrue(engine.reloadTableNames());
            assertTokenAndModeEquals(token, engine.verifyTableName("d"));
        });
    }

    @Test
    public void testRegistryLegacyProjectionRecoversDeltaAndCompacts() throws Exception {
        assertLegacyRegistryProjection(true);
    }

    @Test
    public void testRegistryLegacyProjectionAllowsPendingBypassConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            TableToken token = engine.verifyTableName("d");
            execute("ALTER TABLE d SET TYPE BYPASS WAL");
            replaceRegistryFlags(token, 0);
            // Model an old binary stopping after the metadata rewrite but before registry conversion.
            engine.releaseAllReaders();
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                path.of(root).concat(token).concat(TableUtils.META_FILE_NAME);
                mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
                mem.putBool(TableUtils.META_OFFSET_WAL_ENABLED, false);
            }
            Assert.assertTrue(engine.reloadTableNames());
            engine.releaseInactive();
            engine.load();
            Assert.assertFalse(engine.verifyTableName("d").isWal());
            Assert.assertFalse(engine.verifyTableName("d").isDelta());
        });
    }

    @Test
    public void testRegistryLegacyProjectionRecoversWalAndCompacts() throws Exception {
        assertLegacyRegistryProjection(false);
    }

    @Test
    public void testRegistryLegacyProjectionSkipsDroppedMetadata() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createDelta();
            replaceRegistryFlags(token, 0);
            execute("DROP TABLE d");
            Files.delete(java.nio.file.Path.of(root, token.getDirName(), TableUtils.META_FILE_NAME));
            Assert.assertTrue(engine.reloadTableNames());
            Assert.assertNull(engine.getTableTokenIfExists("d"));
            Assert.assertTrue(engine.isTableDropped(token));
        });
    }

    @Test
    public void testRegistryModernReloadAndWriterAcquisitionDoNotOpenMetadata() throws Exception {
        AtomicBoolean reject = new AtomicBoolean();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                Assert.assertFalse("unexpected metadata open: " + name, reject.get() && Utf8s.endsWithAscii(name, "/_meta"));
                return super.openRO(name);
            }
        }, () -> {
            TableToken token = createDelta();
            try (MetadataCacheWriter cache = engine.getMetadataCache().writeLock()) {
                cache.clearCache();
            }
            reject.set(true);
            rejectMetadataReads = true;
            try {
                Assert.assertTrue(engine.reloadTableNames());
                assertTokenAndModeEquals(token, engine.verifyTableName("d"));
                assertRefused(() -> engine.getTableWriterAPI(token, "test"), "Delta table writer is not available");
                assertRefused(() -> engine.getTableWriterAPI("d", "test"), "Delta table writer is not available");
                assertRefused(() -> engine.getWalWriterUnsafe(token), "WAL writers are not supported");
                assertRefused(() -> new WalWriter(configuration, token, engine.getTableSequencerAPI(), engine.getDdlListener(token), engine.getWalDirectoryPolicy(), engine.getWalLocker(), engine.getRecentWriteTracker(), engine.getTelemetryWal()), "WAL writers are not supported");
            } finally {
                reject.set(false);
                rejectMetadataReads = false;
            }
            assertEmptyDelta(token);
        });
    }

    @Test
    public void testRegistryReadOnlyReloadPreservesDeltaBeforeAndAfterCompaction() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createDelta();
            try (TableNameRegistryRO registry = new TableNameRegistryRO(engine, new TableFlagResolverImpl(configuration.getSystemTableNamePrefix().toString()))) {
                registry.reload();
                assertTokenAndModeEquals(token, registry.getTableToken("d"));
                replaceRegistryFlags(token, 0);
                registry.reload();
                assertTokenAndModeEquals(token, registry.getTableToken("d"));
                Assert.assertFalse(Files.exists(java.nio.file.Path.of(root, "tables.d.1")));
                Assert.assertTrue(engine.reloadTableNames());
                Assert.assertTrue(Files.exists(java.nio.file.Path.of(root, "tables.d.1")));
                registry.reload();
                assertTokenAndModeEquals(token, registry.getTableToken("d"));
            }
        });
    }

    @Test
    public void testRegistryUnknownProjectionFailsClosed() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createDelta();
            long flags = replaceRegistryFlags(token, 17);
            try {
                assertRefused(() -> engine.reloadTableNames(), "invalid table registry flags");
            } finally {
                replaceRegistryFlags(token, flags);
                Assert.assertTrue(engine.reloadTableNames());
            }
            assertTokenAndModeEquals(token, engine.verifyTableName("d"));
        });
    }

    @Test
    public void testRenamedTokenPreservesDeltaAndRejectsOldName() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createDelta();
            TableToken renamed = engine.getTableNameRegistry().rename("d", "renamed", token);
            Assert.assertTrue(renamed.isDelta());
            Assert.assertThrows(CairoException.class, () -> engine.getTableWriterAPI(token, "test"));
            assertRefused(() -> engine.getTableWriterAPI(renamed, "test"), "Delta table writer is not available");
            Assert.assertTrue(engine.reloadTableNames());
            assertTokenAndModeEquals(renamed, engine.verifyTableName("renamed"));
        });
    }

    @Test
    public void testWriterRoutingRejectsMismatchedIdAndDirectory() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createDelta();
            TableToken wrongId = new TableToken("d", token.getDirName(), null, token.getTableId() + 1, true, false, false).withDelta();
            TableToken wrongDirectory = new TableToken("d", "anotherDirectory", null, token.getTableId(), true, false, false).withDelta();
            assertWriterIdentityRejected(wrongId);
            assertWriterIdentityRejected(wrongDirectory);
            assertEmptyDelta(token);
        });
    }

    @Test
    public void testWriterRoutingUsesCurrentModeForStaleTokens() throws Exception {
        assertMemoryLeak(() -> {
            TableToken delta = createDelta();
            TableToken staleWal = new TableToken("d", delta.getDirName(), null, delta.getTableId(), true, false, false);
            execute("CREATE TABLE w (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            TableToken wal = engine.verifyTableName("w");
            TableToken staleDelta = wal.withDelta();
            Assert.assertEquals(delta, staleWal);
            Assert.assertEquals(wal, staleDelta);
            try (MetadataCacheWriter cache = engine.getMetadataCache().writeLock()) {
                cache.clearCache();
            }
            rejectMetadataReads = true;
            try {
                Assert.assertTrue(engine.isDeltaTable(staleWal));
                Assert.assertFalse(engine.isDeltaTable(staleDelta));
                assertRefused(() -> engine.getTableWriterAPI(staleWal, "test"), "Delta table writer is not available");
                assertRefused(() -> engine.getWalWriter(staleWal), "WAL writers are not supported");
                assertRefused(() -> engine.getWalWriterUnsafe(staleWal), "WAL writers are not supported");
                assertRefused(() -> new WalWriter(configuration, staleWal, engine.getTableSequencerAPI(), engine.getDdlListener(staleWal), engine.getWalDirectoryPolicy(), engine.getWalLocker(), engine.getRecentWriteTracker(), engine.getTelemetryWal()), "WAL writers are not supported");
                try (TableWriterAPI writer = engine.getTableWriterAPI(staleDelta, "test")) {
                    Assert.assertTrue(writer instanceof WalWriter);
                    Assert.assertSame(wal, writer.getTableToken());
                }
                try (WalWriter writer = engine.getWalWriterUnsafe(staleDelta)) {
                    Assert.assertSame(wal, writer.getTableToken());
                }
                try (WalWriter writer = engine.getWalWriter(staleDelta)) {
                    Assert.assertSame(wal, writer.getTableToken());
                }
            } finally {
                rejectMetadataReads = false;
            }
            assertEmptyDelta(delta);
        });
    }

    @Test
    public void testPendingLegacyConversionCannotDowngradeEmptyDelta() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createDelta();
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.createConvertFile(configuration.getFilesFacade(), path, (byte) TableUtils.TABLE_TYPE_NON_WAL);
            }
            engine.releaseInactive();
            engine.load();
            Assert.assertTrue(engine.verifyTableName("d").isWal());
            Assert.assertTrue(engine.isDeltaTable(token));
            assertMeta(token, TableUtils.META_FORMAT_VERSION_DELTA, TableWriteMode.DELTA_RUNS);
            assertEmptyDelta(token);
        });
    }

    @Test
    public void testWriterRoutingAndDirectWalConstructorRefuseWithoutPayload() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createDelta();
            providerCalls = 0;
            assertRefused(() -> engine.getTableWriterAPI(token, "test"), "Delta table writer is not available");
            assertRefused(() -> engine.getTableWriterAPI("d", "test"), "Delta table writer is not available");
            Assert.assertEquals(2, providerCalls);
            assertRefused(() -> engine.getWalWriter(token), "WAL writers are not supported");
            assertRefused(() -> engine.getWalWriterUnsafe(token), "WAL writers are not supported");
            assertRefused(() -> new WalWriter(configuration, token, engine.getTableSequencerAPI(), engine.getDdlListener(token), engine.getWalDirectoryPolicy(), engine.getWalLocker(), engine.getRecentWriteTracker(), engine.getTelemetryWal()), "WAL writers are not supported");
            assertEmptyDelta(token);
            execute("CREATE TABLE w (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            try (TableWriterAPI writer = engine.getTableWriterAPI("w", "test")) {
                Assert.assertTrue(writer instanceof WalWriter);
            }
            try (TableWriterAPI writer = engine.getTableWriterAPI("b", "test")) {
                Assert.assertTrue(writer instanceof TableWriter);
            }
            Assert.assertEquals(2, providerCalls);
            assertMeta(engine.verifyTableName("w"), ColumnType.VERSION, TableWriteMode.WAL_SEGMENTS);
            assertMeta(engine.verifyTableName("b"), ColumnType.VERSION, TableWriteMode.WAL_SEGMENTS);
        });
    }

    @Test
    public void testRebaseRefusesEmptyDeltaBeforeReplacingIdentity() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
            TableToken token = createDelta();
            execute("ALTER TABLE d SUSPEND WAL");
            assertRefused(() -> engine.rebaseWalTable(token), "cannot rebase WAL for a Delta table");
            assertTokenAndModeEquals(token, engine.verifyTableName("d"));
            assertMeta(token, TableUtils.META_FORMAT_VERSION_DELTA, TableWriteMode.DELTA_RUNS);
            assertEmptyDelta(token);
        });
    }

    @Test
    public void testReverseTypeChangeRejectedForEmptyDelta() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createDelta();
            assertExceptionNoLeakCheck("ALTER TABLE d SET TYPE WAL", -1, "cannot change table type");
            assertExceptionNoLeakCheck("ALTER TABLE d SET TYPE BYPASS WAL", -1, "cannot change table type");
            Assert.assertFalse(Files.exists(java.nio.file.Path.of(configuration.getDbRoot().toString(), token.getDirName(), "_convert")));
            assertMeta(token, TableUtils.META_FORMAT_VERSION_DELTA, TableWriteMode.DELTA_RUNS);
        });
    }

    private static void assertEmptyDelta(TableToken token) throws Exception {
        try (Stream<java.nio.file.Path> files = Files.list(java.nio.file.Path.of(configuration.getDbRoot().toString(), token.getDirName()))) {
            files.filter(Files::isDirectory).forEach(path -> Assert.assertEquals("unexpected partition or WAL directory: " + path, "txn_seq", path.getFileName().toString()));
        }
    }

    private static void assertLegacyRegistryProjection(boolean delta) throws Exception {
        AtomicBoolean reject = new AtomicBoolean();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                Assert.assertFalse("modern registry reopened metadata: " + name, reject.get() && Utf8s.endsWithAscii(name, "/_meta"));
                return super.openRO(name);
            }
        }, () -> {
            execute("CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + (delta ? "DELTA" : "WAL"));
            TableToken token = engine.verifyTableName("d");
            if (!delta) {
                // Ordinary metadata versions must ignore old reserved bytes at the mode offset.
                try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                    path.of(root).concat(token).concat(TableUtils.META_FILE_NAME);
                    mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
                    mem.putInt(TableUtils.META_OFFSET_WRITE_MODE, 987654);
                }
            }
            replaceRegistryFlags(token, 0);
            Assert.assertTrue(engine.reloadTableNames());
            assertTokenAndModeEquals(token, engine.verifyTableName("d"));
            Assert.assertTrue("legacy projection must be compacted", Files.exists(java.nio.file.Path.of(root, "tables.d.1")));
            reject.set(true);
            try {
                Assert.assertTrue(engine.reloadTableNames());
                assertTokenAndModeEquals(token, engine.verifyTableName("d"));
            } finally {
                reject.set(false);
            }
        });
    }

    private static void assertMeta(TableToken token, int version, int mode) {
        try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
            path.of(configuration.getDbRoot()).concat(token).concat(TableUtils.META_FILE_NAME);
            mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
            Assert.assertEquals(version, mem.getInt(TableUtils.META_OFFSET_VERSION));
            Assert.assertEquals(mode, TableUtils.getWriteMode(mem));
        }
    }

    private static void assertRefused(Runnable code, String message) {
        try {
            code.run();
            Assert.fail("expected refusal: " + message);
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), message);
        }
    }

    private static void assertTokenAndModeEquals(TableToken expected, TableToken actual) {
        Assert.assertEquals(expected, actual);
        Assert.assertEquals(expected.isDelta(), actual.isDelta());
    }

    private static void assertWriterIdentityRejected(TableToken token) {
        Assert.assertThrows(TableReferenceOutOfDateException.class, () -> engine.isDeltaTable(token));
        Assert.assertThrows(TableReferenceOutOfDateException.class, () -> engine.getTableWriterAPI(token, "test"));
        Assert.assertThrows(TableReferenceOutOfDateException.class, () -> engine.getWalWriter(token));
        Assert.assertThrows(TableReferenceOutOfDateException.class, () -> engine.getWalWriterUnsafe(token));
        Assert.assertThrows(TableReferenceOutOfDateException.class, () -> new WalWriter(configuration, token, engine.getTableSequencerAPI(), engine.getDdlListener(token), engine.getWalDirectoryPolicy(), engine.getWalLocker(), engine.getRecentWriteTracker(), engine.getTelemetryWal()));
    }

    private static void createThroughEngine(TableModel model) {
        try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
            engine.createTable(sqlExecutionContext.getSecurityContext(), mem, path, false, model, false);
        }
    }

    private static TableToken createDelta() throws Exception {
        execute("CREATE TABLE d (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY DELTA");
        return engine.verifyTableName("d");
    }

    private static long replaceRegistryFlags(TableToken token, long flags) {
        try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
            path.of(root).concat("tables.d.0");
            mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
            long offset = Long.BYTES + Integer.BYTES + Vm.getStorageLength(token.getTableName()) + Vm.getStorageLength(token.getDirName()) + 2L * Integer.BYTES;
            long previous = mem.getLong(offset);
            mem.putLong(offset, flags);
            return previous;
        }
    }
}
