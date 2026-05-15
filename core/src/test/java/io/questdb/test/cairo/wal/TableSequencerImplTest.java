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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TableTransactionLog;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TableSequencerImplTest extends AbstractCairoTest {
    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_RECREATE_DISTRESSED_SEQUENCER_ATTEMPTS, Integer.MAX_VALUE);
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testCanReadStructureVersionV1() {
        testTableTransactionLogCanReadStructureVersion();
    }

    @Test
    public void testCanReadStructureVersionV2() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, rnd.nextInt(20) + 10);
        testTableTransactionLogCanReadStructureVersion();
    }

    @Test
    public void testCopyMetadataRace() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        final String tableName = testName.getMethodName();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        int iterations = 100;
        int initialColumnCount = 2;

        runAddColumnRace(
                barrier, tableName, iterations, 1, exception,
                () -> {
                    try {
                        TestUtils.await(barrier);

                        TableToken tableToken = engine.verifyTableName(tableName);
                        int metadataColumnCount;
                        do {
                            try (TableRecordMetadata metadata = engine.getSequencerMetadata(tableToken)) {
                                Assert.assertEquals(metadata.getColumnCount() - initialColumnCount, metadata.getMetadataVersion());
                                metadataColumnCount = metadata.getColumnCount();
                            }
                        } while (metadataColumnCount < initialColumnCount + iterations && exception.get() == null);
                    } catch (Throwable e) {
                        exception.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }
        );
    }

    @Test
    public void testGetCurrentWalId() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("int", ColumnType.INT)
                    .timestamp("ts")
                    .wal();
            createTable(model);

            TableToken tableToken = engine.verifyTableName(tableName);

            // Get initial current WAL ID (should be 0 - no WALs allocated yet)
            int currentWalId = engine.getTableSequencerAPI().getCurrentWalId(tableToken);
            Assert.assertEquals("initial WAL ID should be 0", 0, currentWalId);

            // Calling getCurrentWalId() multiple times should not increment
            int currentWalId2 = engine.getTableSequencerAPI().getCurrentWalId(tableToken);
            Assert.assertEquals("getCurrentWalId should not increment", currentWalId, currentWalId2);

            // Now allocate a new WAL ID
            int nextWalId1 = engine.getTableSequencerAPI().getNextWalId(tableToken);
            Assert.assertEquals("first getNextWalId should return 1", 1, nextWalId1);

            // getCurrentWalId should now return the allocated ID
            currentWalId = engine.getTableSequencerAPI().getCurrentWalId(tableToken);
            Assert.assertEquals("getCurrentWalId should return last allocated ID", nextWalId1, currentWalId);

            // Allocate another WAL ID
            int nextWalId2 = engine.getTableSequencerAPI().getNextWalId(tableToken);
            Assert.assertEquals("second getNextWalId should return 2", 2, nextWalId2);

            // getCurrentWalId should now return the new allocated ID
            currentWalId = engine.getTableSequencerAPI().getCurrentWalId(tableToken);
            Assert.assertEquals("getCurrentWalId should return last allocated ID", nextWalId2, currentWalId);

            // Verify getCurrentWalId doesn't increment on multiple calls
            int currentWalId3 = engine.getTableSequencerAPI().getCurrentWalId(tableToken);
            int currentWalId4 = engine.getTableSequencerAPI().getCurrentWalId(tableToken);
            Assert.assertEquals("getCurrentWalId should be stable", currentWalId3, currentWalId4);
            Assert.assertEquals("getCurrentWalId should still be 2", 2, currentWalId4);
        });
    }

    @Test
    public void testGetCursorDistressedSequencerRace() throws Exception {
        int readerCount = 2;
        CyclicBarrier barrier = new CyclicBarrier(readerCount + 1);
        final String tableName = testName.getMethodName();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        int iterations = 50;

        AtomicInteger threadId = new AtomicInteger();

        runAddColumnRace(
                barrier, tableName, iterations, readerCount, exception,
                () -> {
                    try {
                        TestUtils.await(barrier);
                        int threadIdValue = threadId.getAndIncrement();
                        long sv = 0;
                        TableToken tableToken = engine.verifyTableName(tableName);
                        do {
                            long sv2 = engine.getTableSequencerAPI().lastTxn(tableToken);
                            if (threadIdValue != 0) {
                                engine.getTableSequencerAPI().setDistressed(tableToken);
                                if (sv != sv2) {
                                    sv = sv2;
                                    LOG.info().$("destroyed sv ").$(sv).$();
                                }
                            } else {
                                try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(tableToken, 0)) {
                                    long transactions = 0;
                                    while (cursor.hasNext()) {
                                        transactions++;
                                    }
                                    Assert.assertTrue(transactions >= sv2);
                                }
                            }
                        } while (engine.getTableSequencerAPI().lastTxn(tableToken) < iterations && exception.get() == null);
                    } catch (Throwable e) {
                        exception.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }
        );
    }

    @Test
    public void testGetTxnRace() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        final String tableName = testName.getMethodName();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        int iterations = 100;

        runAddColumnRace(
                barrier, tableName, iterations, 1, exception,
                () -> {
                    try {
                        TestUtils.await(barrier);
                        TableToken tableToken = engine.verifyTableName(tableName);
                        do {
                            engine.getTableSequencerAPI().lastTxn(tableToken);
                        } while (engine.getTableSequencerAPI().lastTxn(tableToken) < iterations && exception.get() == null);
                    } catch (Throwable e) {
                        exception.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }
        );
    }

    @Test
    public void testOpenFailsClosedWhenSequencerMetaVersionIsUnsupportedV1() throws Exception {
        testOpenFailsClosedWhenSequencerMetaVersionIsUnsupported();
    }

    @Test
    public void testOpenFailsClosedWhenSequencerMetaVersionIsUnsupportedV2() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
        testOpenFailsClosedWhenSequencerMetaVersionIsUnsupported();
    }

    @Test
    public void testOpenIgnoresUncommittedMetadataSidecarTailV1() throws Exception {
        testOpenIgnoresUncommittedMetadataSidecarTail();
    }

    @Test
    public void testOpenIgnoresUncommittedMetadataSidecarTailV2() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
        testOpenIgnoresUncommittedMetadataSidecarTail();
    }

    @Test
    public void testOpenRebuildsSequencerMetaAheadOfTxnLogV1() throws Exception {
        testOpenRebuildsSequencerMetaAheadOfTxnLog();
    }

    @Test
    public void testOpenRebuildsSequencerMetaAheadOfTxnLogV2() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
        testOpenRebuildsSequencerMetaAheadOfTxnLog();
    }

    @Test
    public void testOpenRebuildsUnreadableSequencerMetaV1() throws Exception {
        testOpenRebuildsUnreadableSequencerMeta();
    }

    @Test
    public void testOpenRebuildsUnreadableSequencerMetaV2() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
        testOpenRebuildsUnreadableSequencerMeta();
    }

    @Test
    public void testOpenRebuildsCreateTimeCoveringIndexV1() throws Exception {
        testOpenRebuildsCreateTimeCoveringIndex();
    }

    @Test
    public void testOpenRebuildsCreateTimeCoveringIndexV2() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
        testOpenRebuildsCreateTimeCoveringIndex();
    }

    @Test
    public void testOpenRecoversCommittedRenameWhenRegistryAlreadyRenamedV1() throws Exception {
        testOpenRecoversCommittedRenameWhenRegistryAlreadyRenamed();
    }

    @Test
    public void testOpenRecoversCommittedRenameWhenRegistryAlreadyRenamedV2() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
        testOpenRecoversCommittedRenameWhenRegistryAlreadyRenamed();
    }

    @Test
    public void testOpenRepairsSequencerMetaBehindTxnLogV1() throws Exception {
        testOpenRepairsSequencerMetaBehindTxnLog();
    }

    @Test
    public void testOpenRepairsSequencerMetaBehindTxnLogV2() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
        testOpenRepairsSequencerMetaBehindTxnLog();
    }

    @Test
    public void testOpenSequencerAfterFullDropTranslatesEnoentToTableDropped() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " (i int, ts timestamp) timestamp(ts) partition by day WAL");
            TableToken staleToken = engine.verifyTableName(tableName);

            execute("drop table " + tableName);
            drainWalQueue();

            try (WalPurgeJob job = new WalPurgeJob(
                    engine,
                    engine.getConfiguration().getFilesFacade(),
                    engine.getConfiguration().getMicrosecondClock())
            ) {
                //noinspection StatementWithEmptyBody
                while (job.run(0)) {
                }
            }

            Assert.assertNull(
                    "test setup: WalPurgeJob did not sweep the reverse-map entry",
                    engine.getTableTokenByDirName(staleToken.getDirName())
            );

            try {
                engine.getTableSequencerAPI().getNextWalId(staleToken);
                Assert.fail("Expected CairoException with isTableDropped()");
            } catch (CairoException ex) {
                Assert.assertTrue(
                        "Expected isTableDropped() but got errno=" + ex.getErrno()
                                + ", msg=" + ex.getFlyweightMessage(),
                        ex.isTableDropped()
                );
            }
        });
    }

    @Test
    public void testOpenSkipsCommittedRenameWhenRegistryStillHasOldNameV1() throws Exception {
        testOpenSkipsCommittedRenameWhenRegistryStillHasOldName();
    }

    @Test
    public void testOpenSkipsCommittedRenameWhenRegistryStillHasOldNameV2() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
        testOpenSkipsCommittedRenameWhenRegistryStillHasOldName();
    }

    @Test
    public void testOpenSkipsRenameChainWhenRegistryHasFinalNameV1() throws Exception {
        testOpenSkipsRenameChainWhenRegistryHasFinalName();
    }

    @Test
    public void testOpenSkipsRenameChainWhenRegistryHasFinalNameV2() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
        testOpenSkipsRenameChainWhenRegistryHasFinalName();
    }

    @Test
    public void testOpenSkipsStaleRenameBeforeLaterDurableRenameV1() throws Exception {
        testOpenSkipsStaleRenameBeforeLaterDurableRename();
    }

    @Test
    public void testOpenSkipsStaleRenameBeforeLaterDurableRenameV2() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
        testOpenSkipsStaleRenameBeforeLaterDurableRename();
    }

    @Test
    public void testReloadRebuildsSequencerMetaAheadOfTxnLogV1() throws Exception {
        testReloadRebuildsSequencerMetaAheadOfTxnLog();
    }

    @Test
    public void testReloadRebuildsSequencerMetaAheadOfTxnLogV2() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
        testReloadRebuildsSequencerMetaAheadOfTxnLog();
    }

    @Test
    public void testTxnDistressedCursorRace() throws Exception {
        int readers = 3;
        CyclicBarrier barrier = new CyclicBarrier(readers + 1);
        final String tableName = testName.getMethodName();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        int iterations = 100;

        runAddColumnRace(
                barrier, tableName, iterations, readers, exception,
                () -> {
                    try {
                        TestUtils.await(barrier);
                        TableToken tableToken = engine.verifyTableName(tableName);
                        long lastTxn = 0;
                        do {
                            try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(tableToken, lastTxn)) {
                                while (cursor.hasNext()) {
                                    lastTxn = cursor.getTxn();
                                }
                            }
                        } while (engine.getTableSequencerAPI().lastTxn(tableToken) < iterations && exception.get() == null);
                    } catch (Throwable e) {
                        exception.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }
        );
    }

    private void addIntColumn(TableToken tableToken, String columnName) {
        try (WalWriter ww = engine.getWalWriter(tableToken)) {
            addColumn(ww, columnName, ColumnType.INT);
        }
    }

    private void assertColumn(TableRecordMetadata metadata, String columnName, boolean exists) {
        int columnIndex = metadata.getColumnIndexQuiet(columnName);
        if (exists) {
            Assert.assertTrue("expected column to exist: " + columnName, columnIndex > -1);
        } else {
            Assert.assertEquals("expected column to be absent: " + columnName, -1, columnIndex);
        }
    }

    private void assertMaxStructureVersion(TableToken tableToken, long expectedStructureVersion) {
        try (Path path = new Path()) {
            pathToSequencerPath(path, tableToken);
            Assert.assertEquals(
                    expectedStructureVersion,
                    TableTransactionLog.readMaxStructureVersion(engine.getConfiguration().getFilesFacade(), path)
            );
        }
    }

    private void assertSequencerMetadata(TableToken tableToken, long expectedVersion, String[] presentColumns, String[] absentColumns) {
        try (TableRecordMetadata metadata = engine.getSequencerMetadata(tableToken)) {
            Assert.assertEquals(expectedVersion, metadata.getMetadataVersion());
            for (int i = 0, n = presentColumns.length; i < n; i++) {
                assertColumn(metadata, presentColumns[i], true);
            }
            for (int i = 0, n = absentColumns.length; i < n; i++) {
                assertColumn(metadata, absentColumns[i], false);
            }
        }
    }

    private void assertSequencerCoveringIndexIncludes(TableToken tableToken, String indexedColumn, String coveringColumn) {
        try (TableRecordMetadata metadata = engine.getSequencerMetadata(tableToken)) {
            int indexedColumnIndex = metadata.getColumnIndexQuiet(indexedColumn);
            int coveringColumnIndex = metadata.getColumnIndexQuiet(coveringColumn);
            Assert.assertTrue("expected indexed column to exist: " + indexedColumn, indexedColumnIndex > -1);
            Assert.assertTrue("expected covering column to exist: " + coveringColumn, coveringColumnIndex > -1);

            IntList coveringIndices = metadata.getColumnMetadata(indexedColumnIndex).getCoveringColumnIndices();
            Assert.assertNotNull("expected INCLUDE list to exist for column: " + indexedColumn, coveringIndices);
            Assert.assertTrue(
                    "expected INCLUDE list for " + indexedColumn + " to contain " + coveringColumn,
                    coveringIndices.contains(metadata.getWriterIndex(coveringColumnIndex))
            );
        }
    }

    private void assertTableCoveringIndexIncludes(String tableName, String indexedColumn, String coveringColumn) {
        try (TableReader reader = engine.getReader(tableName)) {
            TableRecordMetadata metadata = reader.getMetadata();
            int indexedColumnIndex = metadata.getColumnIndexQuiet(indexedColumn);
            int coveringColumnIndex = metadata.getColumnIndexQuiet(coveringColumn);
            Assert.assertTrue("expected indexed column to exist: " + indexedColumn, indexedColumnIndex > -1);
            Assert.assertTrue("expected covering column to exist: " + coveringColumn, coveringColumnIndex > -1);

            IntList coveringIndices = metadata.getColumnMetadata(indexedColumnIndex).getCoveringColumnIndices();
            Assert.assertNotNull("expected table INCLUDE list to exist for column: " + indexedColumn, coveringIndices);
            Assert.assertTrue(
                    "expected table INCLUDE list for " + indexedColumn + " to contain " + coveringColumn,
                    coveringIndices.contains(metadata.getWriterIndex(coveringColumnIndex))
            );
        }
    }

    private void assertInitialSequencerMetadataCoveringIndexIncludes(TableToken tableToken, String indexedColumn, String coveringColumn) {
        try (Path path = new Path(); TableReaderMetadata metadata = new TableReaderMetadata(engine.getConfiguration())) {
            pathToSequencerFile(path, tableToken, WalUtils.INITIAL_META_FILE_NAME);
            metadata.loadMetadata(path.$());
            int indexedColumnIndex = metadata.getColumnIndexQuiet(indexedColumn);
            int coveringColumnIndex = metadata.getColumnIndexQuiet(coveringColumn);
            Assert.assertTrue("expected initial indexed column to exist: " + indexedColumn, indexedColumnIndex > -1);
            Assert.assertTrue("expected initial covering column to exist: " + coveringColumn, coveringColumnIndex > -1);

            IntList coveringIndices = metadata.getColumnMetadata(indexedColumnIndex).getCoveringColumnIndices();
            Assert.assertNotNull("expected initial INCLUDE list to exist for column: " + indexedColumn, coveringIndices);
            Assert.assertTrue(
                    "expected initial INCLUDE list for " + indexedColumn + " to contain " + coveringColumn,
                    coveringIndices.contains(metadata.getWriterIndex(coveringColumnIndex))
            );
        }
    }

    private void assertSequencerTableName(TableToken tableToken, String expectedTableName) {
        try (TableRecordMetadata metadata = engine.getSequencerMetadata(tableToken)) {
            Assert.assertEquals(expectedTableName, metadata.getTableToken().getTableName());
        }
    }

    private void copySequencerFile(TableToken tableToken, CharSequence fromFileName, CharSequence toFileName) {
        FilesFacade ff = configuration.getFilesFacade();
        try (Path fromPath = new Path(); Path toPath = new Path()) {
            pathToSequencerFile(fromPath, tableToken, fromFileName);
            pathToSequencerFile(toPath, tableToken, toFileName);
            ff.removeQuiet(toPath.$());
            int result = ff.copy(fromPath.$(), toPath.$());
            Assert.assertTrue(
                    "could not copy sequencer file [from=" + fromPath + ", to=" + toPath + ", errno=" + Os.errno() + "]",
                    result >= 0
            );
        }
    }

    private TableToken createWalTable(String tableName) {
        TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                .col("int", ColumnType.INT)
                .timestamp("ts")
                .wal();
        return createTable(model);
    }

    private TableToken createWalTableWithCoveringIndex(String tableName) throws Exception {
        execute("create table " + tableName + " (" +
                "sym symbol index type posting include (price), " +
                "price double, " +
                "ts timestamp" +
                ") timestamp(ts) partition by hour WAL");
        return engine.verifyTableName(tableName);
    }

    private void pathToSequencerFile(Path path, TableToken tableToken, CharSequence fileName) {
        pathToSequencerPath(path, tableToken).concat(fileName);
    }

    private Path pathToSequencerPath(Path path, TableToken tableToken) {
        return path.of(configuration.getDbRoot()).concat(tableToken.getDirName()).concat(WalUtils.SEQ_DIR);
    }

    private TableToken renameTable(String fromTableName, String toTableName) {
        try (MemoryCMARW mem = Vm.getCMARWInstance()) {
            return engine.rename(
                    AllowAllSecurityContext.INSTANCE,
                    Path.getThreadLocal(""),
                    mem,
                    fromTableName,
                    Path.getThreadLocal2(""),
                    toTableName
            );
        }
    }

    private void rewriteSequencerMaxTxn(TableToken tableToken, long maxTxn) {
        FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path(); MemoryCMARW txnLogMem = Vm.getCMARWInstance()) {
            pathToSequencerFile(path, tableToken, WalUtils.TXNLOG_FILE_NAME);
            txnLogMem.smallFile(ff, path.$(), MemoryTag.MMAP_TX_LOG);
            txnLogMem.putLong(TableTransactionLogFile.MAX_TXN_OFFSET_64, maxTxn);
            txnLogMem.sync(false);
        }
    }

    private void rewriteSequencerMetaSize(TableToken tableToken, int size) {
        FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path(); MemoryCMARW metaMem = Vm.getCMARWInstance()) {
            pathToSequencerFile(path, tableToken, TableUtils.META_FILE_NAME);
            metaMem.smallFile(ff, path.$(), MemoryTag.MMAP_SEQUENCER_METADATA);
            metaMem.putInt(0, size);
            metaMem.sync(false);
        }
    }

    private void rewriteSequencerMetaWalVersion(TableToken tableToken, int walVersion) {
        FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path(); MemoryCMARW metaMem = Vm.getCMARWInstance()) {
            pathToSequencerFile(path, tableToken, TableUtils.META_FILE_NAME);
            metaMem.smallFile(ff, path.$(), MemoryTag.MMAP_SEQUENCER_METADATA);
            metaMem.putInt(WalUtils.SEQ_META_OFFSET_WAL_VERSION, walVersion);
            metaMem.sync(false);
        }
    }

    private void rewriteSequencerMetadataIndexOffset(TableToken tableToken, long structureVersion, long offset) {
        FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path(); MemoryCMARW metaIndexMem = Vm.getCMARWInstance()) {
            pathToSequencerFile(path, tableToken, WalUtils.TXNLOG_FILE_NAME_META_INX);
            metaIndexMem.smallFile(ff, path.$(), MemoryTag.MMAP_TX_LOG);
            metaIndexMem.putLong(structureVersion * Long.BYTES, offset);
            metaIndexMem.sync(false);
        }
    }

    private void runAddColumnRace(CyclicBarrier barrier, String tableName, int iterations, int readerThreads, AtomicReference<Throwable> exception, Runnable runnable) throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("int", ColumnType.INT)
                    .timestamp("ts")
                    .wal();
            createTable(model);
            ObjList<Thread> readerThreadList = new ObjList<>();
            for (int i = 0; i < readerThreads; i++) {
                Thread t = new Thread(runnable);
                readerThreadList.add(t);
                t.start();
            }

            runColumnAdd(barrier, tableName, exception, iterations);

            for (int i = 0; i < readerThreads; i++) {
                readerThreadList.get(i).join();
            }

            if (exception.get() != null) {
                throw new AssertionError(exception.get());
            }
        });
    }

    private void runColumnAdd(CyclicBarrier barrier, String tableName, AtomicReference<Throwable> exception, int iterations) {
        try (WalWriter ww = engine.getWalWriter(engine.verifyTableName(tableName))) {
            TestUtils.await(barrier);

            for (int i = 0; i < iterations; i++) {
                addColumn(ww, "newCol" + i, ColumnType.INT);
                if (exception.get() != null) {
                    break;
                }
            }
        } catch (Throwable e) {
            exception.set(e);
        }
    }

    private void testOpenFailsClosedWhenSequencerMetaVersionIsUnsupported() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            TableToken tableToken = createWalTable(tableName);

            addIntColumn(tableToken, "c1");
            engine.clear();
            rewriteSequencerMetaWalVersion(tableToken, WalUtils.WAL_FORMAT_VERSION + 1);

            try {
                engine.getTableSequencerAPI().openSequencer(tableToken);
                Assert.fail("Expected sequencer open to fail");
            } catch (CairoException ex) {
                TestUtils.assertContains(
                        ex.getFlyweightMessage(),
                        "metadata version does not match runtime version"
                );
            }
        });
    }

    private void testOpenIgnoresUncommittedMetadataSidecarTail() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            TableToken tableToken = createWalTable(tableName);
            String snapshotFileName = TableUtils.META_FILE_NAME + ".v1";

            addIntColumn(tableToken, "c1");
            engine.clear();
            copySequencerFile(tableToken, TableUtils.META_FILE_NAME, snapshotFileName);

            addIntColumn(tableToken, "c2");
            engine.clear();
            copySequencerFile(tableToken, snapshotFileName, TableUtils.META_FILE_NAME);
            rewriteSequencerMaxTxn(tableToken, 1);

            engine.getTableSequencerAPI().openSequencer(tableToken);
            assertSequencerMetadata(tableToken, 1, new String[]{"c1"}, new String[]{"c2"});

            addIntColumn(tableToken, "c3");
            assertSequencerMetadata(tableToken, 2, new String[]{"c1", "c3"}, new String[]{"c2"});
            assertMaxStructureVersion(tableToken, 2);
        });
    }

    private void testOpenRebuildsSequencerMetaAheadOfTxnLog() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            TableToken tableToken = createWalTable(tableName);

            addIntColumn(tableToken, "c1");
            addIntColumn(tableToken, "c2");
            engine.clear();
            rewriteSequencerMaxTxn(tableToken, 1);

            engine.getTableSequencerAPI().openSequencer(tableToken);
            engine.clear();
            assertSequencerMetadata(tableToken, 1, new String[]{"c1"}, new String[]{"c2"});

            addIntColumn(tableToken, "c3");
            assertSequencerMetadata(tableToken, 2, new String[]{"c1", "c3"}, new String[]{"c2"});
            assertMaxStructureVersion(tableToken, 2);
        });
    }

    private void testOpenRebuildsUnreadableSequencerMeta() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            TableToken tableToken = createWalTable(tableName);

            addIntColumn(tableToken, "c1");
            addIntColumn(tableToken, "c2");
            engine.clear();
            rewriteSequencerMetaSize(tableToken, 0);

            engine.getTableSequencerAPI().openSequencer(tableToken);
            engine.clear();
            assertSequencerMetadata(tableToken, 2, new String[]{"c1", "c2"}, new String[0]);

            addIntColumn(tableToken, "c3");
            assertSequencerMetadata(tableToken, 3, new String[]{"c1", "c2", "c3"}, new String[0]);
            assertMaxStructureVersion(tableToken, 3);
        });
    }

    private void testOpenRebuildsCreateTimeCoveringIndex() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            TableToken tableToken = createWalTableWithCoveringIndex(tableName);
            assertTableCoveringIndexIncludes(tableName, "sym", "price");
            assertInitialSequencerMetadataCoveringIndexIncludes(tableToken, "sym", "price");

            engine.clear();
            rewriteSequencerMetaSize(tableToken, 0);

            engine.getTableSequencerAPI().openSequencer(tableToken);
            engine.clear();
            assertSequencerMetadata(tableToken, 0, new String[]{"sym", "price"}, new String[0]);
            assertSequencerCoveringIndexIncludes(tableToken, "sym", "price");

            addIntColumn(tableToken, "c1");
            assertSequencerMetadata(tableToken, 1, new String[]{"sym", "price", "c1"}, new String[0]);
            assertSequencerCoveringIndexIncludes(tableToken, "sym", "price");
            assertMaxStructureVersion(tableToken, 1);
        });
    }

    private void testOpenRecoversCommittedRenameWhenRegistryAlreadyRenamed() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String newTableName = tableName + "_new";
            TableToken tableToken = createWalTable(tableName);

            addIntColumn(tableToken, "c1");

            TableToken renamedTableToken = renameTable(tableName, newTableName);
            engine.clear();
            rewriteSequencerMetaSize(renamedTableToken, 0);

            engine.getTableSequencerAPI().openSequencer(renamedTableToken);
            engine.clear();
            Assert.assertNull(engine.getTableTokenIfExists(tableName));
            Assert.assertNotNull(engine.getTableTokenIfExists(newTableName));
            assertSequencerTableName(renamedTableToken, newTableName);
            assertSequencerMetadata(renamedTableToken, 2, new String[]{"c1"}, new String[0]);

            addIntColumn(renamedTableToken, "c2");
            assertSequencerMetadata(renamedTableToken, 3, new String[]{"c1", "c2"}, new String[0]);
            assertMaxStructureVersion(renamedTableToken, 3);
        });
    }

    private void testOpenRepairsSequencerMetaBehindTxnLog() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            TableToken tableToken = createWalTable(tableName);
            String snapshotFileName = TableUtils.META_FILE_NAME + ".v1";

            addIntColumn(tableToken, "c1");
            engine.clear();
            copySequencerFile(tableToken, TableUtils.META_FILE_NAME, snapshotFileName);

            addIntColumn(tableToken, "c2");
            engine.clear();
            copySequencerFile(tableToken, snapshotFileName, TableUtils.META_FILE_NAME);
            // If recovery rebuilt from version 0, this missing historical sidecar would fail the open.
            rewriteSequencerMetadataIndexOffset(tableToken, 0, -1);

            engine.getTableSequencerAPI().openSequencer(tableToken);
            engine.clear();
            assertSequencerMetadata(tableToken, 2, new String[]{"c1", "c2"}, new String[0]);

            addIntColumn(tableToken, "c3");
            assertSequencerMetadata(tableToken, 3, new String[]{"c1", "c2", "c3"}, new String[0]);
            assertMaxStructureVersion(tableToken, 3);
        });
    }

    private void testOpenSkipsCommittedRenameWhenRegistryStillHasOldName() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String newTableName = tableName + "_new";
            TableToken tableToken = createWalTable(tableName);

            addIntColumn(tableToken, "c1");

            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                ww.renameTable(tableName, newTableName, AllowAllSecurityContext.INSTANCE);
            }
            engine.clear();
            rewriteSequencerMetaSize(tableToken, 0);

            engine.getTableSequencerAPI().openSequencer(tableToken);
            engine.clear();
            Assert.assertNotNull(engine.getTableTokenIfExists(tableName));
            Assert.assertNull(engine.getTableTokenIfExists(newTableName));
            assertSequencerTableName(tableToken, tableName);
            assertSequencerMetadata(tableToken, 2, new String[]{"c1"}, new String[0]);

            addIntColumn(tableToken, "c2");
            assertSequencerMetadata(tableToken, 3, new String[]{"c1", "c2"}, new String[0]);
            assertMaxStructureVersion(tableToken, 3);
        });
    }

    private void testOpenSkipsRenameChainWhenRegistryHasFinalName() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String intermediateTableName = tableName + "_b";
            String newTableName = tableName + "_c";
            TableToken tableToken = createWalTable(tableName);

            addIntColumn(tableToken, "c1");

            renameTable(tableName, intermediateTableName);
            TableToken renamedTableToken = renameTable(intermediateTableName, newTableName);
            engine.clear();
            rewriteSequencerMetaSize(renamedTableToken, 0);

            engine.getTableSequencerAPI().openSequencer(renamedTableToken);
            engine.clear();
            Assert.assertNull(engine.getTableTokenIfExists(tableName));
            Assert.assertNull(engine.getTableTokenIfExists(intermediateTableName));
            Assert.assertNotNull(engine.getTableTokenIfExists(newTableName));
            assertSequencerTableName(renamedTableToken, newTableName);
            assertSequencerMetadata(renamedTableToken, 3, new String[]{"c1"}, new String[0]);

            addIntColumn(renamedTableToken, "c2");
            assertSequencerMetadata(renamedTableToken, 4, new String[]{"c1", "c2"}, new String[0]);
            assertMaxStructureVersion(renamedTableToken, 4);
        });
    }

    private void testOpenSkipsStaleRenameBeforeLaterDurableRename() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String abandonedTableName = tableName + "_b";
            String newTableName = tableName + "_c";
            TableToken tableToken = createWalTable(tableName);

            addIntColumn(tableToken, "c1");

            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                ww.renameTable(tableName, abandonedTableName, AllowAllSecurityContext.INSTANCE);
            }
            engine.clear();
            TableToken renamedTableToken = renameTable(tableName, newTableName);
            engine.clear();
            rewriteSequencerMetaSize(renamedTableToken, 0);

            engine.getTableSequencerAPI().openSequencer(renamedTableToken);
            engine.clear();
            Assert.assertNull(engine.getTableTokenIfExists(tableName));
            Assert.assertNull(engine.getTableTokenIfExists(abandonedTableName));
            Assert.assertNotNull(engine.getTableTokenIfExists(newTableName));
            assertSequencerTableName(renamedTableToken, newTableName);
            assertSequencerMetadata(renamedTableToken, 3, new String[]{"c1"}, new String[0]);

            addIntColumn(renamedTableToken, "c2");
            assertSequencerMetadata(renamedTableToken, 4, new String[]{"c1", "c2"}, new String[0]);
            assertMaxStructureVersion(renamedTableToken, 4);
        });
    }

    private void testReloadRebuildsSequencerMetaAheadOfTxnLog() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            TableToken tableToken = createWalTable(tableName);

            addIntColumn(tableToken, "c1");
            addIntColumn(tableToken, "c2");
            rewriteSequencerMaxTxn(tableToken, 1);

            engine.getTableSequencerAPI().reload(tableToken);
            engine.clear();
            assertSequencerMetadata(tableToken, 1, new String[]{"c1"}, new String[]{"c2"});

            addIntColumn(tableToken, "c3");
            assertSequencerMetadata(tableToken, 2, new String[]{"c1", "c3"}, new String[]{"c2"});
            assertMaxStructureVersion(tableToken, 2);
        });
    }

    private void testTableTransactionLogCanReadStructureVersion() {
        final String tableName = testName.getMethodName();
        int iterations = 100;

        TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                .col("int", ColumnType.INT)
                .timestamp("ts")
                .wal();
        createTable(model);

        TableToken tableToken = engine.verifyTableName(tableName);
        try (
                Path path = new Path();
                WalWriter ww = engine.getWalWriter(tableToken)
        ) {

            path.concat(engine.getConfiguration().getDbRoot()).concat(ww.getTableToken()).concat(WalUtils.SEQ_DIR);
            for (int i = 0; i < iterations; i++) {
                addColumn(ww, "newCol" + i, ColumnType.INT);
                try (TableRecordMetadata metadata = engine.getSequencerMetadata(tableToken)) {
                    Assert.assertEquals(i + 1, metadata.getMetadataVersion());
                    long seqMeta = TableTransactionLog.readMaxStructureVersion(engine.getConfiguration().getFilesFacade(), path);
                    Assert.assertEquals(metadata.getMetadataVersion(), seqMeta);
                }
            }
        }
    }
}
