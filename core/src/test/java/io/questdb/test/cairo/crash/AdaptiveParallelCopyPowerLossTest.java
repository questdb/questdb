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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.text.CopyImportTask;
import io.questdb.cutlass.text.ParallelCsvFileImporter;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

/**
 * A parallel COPY that reported success must survive a power loss in SYNC, and in ADAPTIVE into a WAL table, into a
 * new table as well as into an existing empty one. The importer writes every partition outside the WAL, so nothing
 * can redo it after the crash: the import must be durable before COPY reports it done, and a power loss before that
 * must not leave a table whose _txn names files that are not complete. Unless a test says otherwise, nothing is
 * written back by the kernel here: only the product's own barriers make bytes durable. Where syncfs is
 * filesystem-wide one syncfs before the attach and one after it make the import durable; elsewhere the importer
 * flushes file by file. NOSYNC, ASYNC and a non-WAL table in ADAPTIVE promise nothing on a power loss, and the
 * importer makes no flush there.
 */
@RunWith(Parameterized.class)
public class AdaptiveParallelCopyPowerLossTest extends AbstractAdaptiveCrashTest {
    private static final String CSV = """
            ts,s,v
            2024-02-01T00:00:00.000000Z,c1,101
            2024-02-01T01:00:00.000000Z,c2,102
            2024-02-02T00:00:00.000000Z,c3,103
            2024-02-03T00:00:00.000000Z,c4,104
            """;
    private static final String IMPORTED = """
            ts\ts\tv
            2024-02-01T00:00:00.000000Z\tc1\t101
            2024-02-01T01:00:00.000000Z\tc2\t102
            2024-02-02T00:00:00.000000Z\tc3\t103
            2024-02-03T00:00:00.000000Z\tc4\t104
            """;
    private final String commitMode;
    private final boolean newTable;
    private final boolean syncfsFileSystemWide;
    private final boolean walTable;

    public AdaptiveParallelCopyPowerLossTest(String commitMode, boolean newTable, boolean walTable, boolean syncfsFileSystemWide) {
        this.commitMode = commitMode;
        this.newTable = newTable;
        this.walTable = walTable;
        this.syncfsFileSystemWide = syncfsFileSystemWide;
    }

    @Parameterized.Parameters(name = "mode={0}, newTable={1}, walTable={2}, syncfsFileSystemWide={3}")
    public static Collection<Object[]> data() {
        final List<Object[]> params = new ArrayList<>();
        for (String mode : new String[]{"adaptive", "sync", "nosync", "async"}) {
            for (boolean walTable : new boolean[]{true, false}) {
                for (boolean syncfsFileSystemWide : new boolean[]{true, false}) {
                    params.add(new Object[]{mode, true, walTable, syncfsFileSystemWide});
                    params.add(new Object[]{mode, false, walTable, syncfsFileSystemWide});
                }
            }
        }
        return params;
    }

    @Test
    public void testFailedDeviceFlushStopsTheEngine() throws Exception {
        assertFailedFlushStopsTheEngine();
    }

    @Test
    public void testImportSurvivesPowerLoss() throws Exception {
        assumeCrashHarnessSupported();
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, Boolean.toString(walTable));
        // A power loss takes no close-time save point.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        inputRoot = temp.newFolder().getAbsolutePath();
        inputWorkRoot = temp.newFolder().getAbsolutePath();
        Files.writeString(java.nio.file.Path.of(inputRoot, "t.csv"), CSV, StandardCharsets.UTF_8);
        final AtomicInteger syncfsCalls = new AtomicInteger();
        final AtomicInteger columnFileFsyncs = new AtomicInteger();
        crashFf = new CrashFaultFilesFacade() {
            @Override
            public void fsync(long fd) {
                countColumnFile(fd);
                super.fsync(fd);
            }

            @Override
            public void fsyncAndClose(long fd) {
                countColumnFile(fd);
                super.fsyncAndClose(fd);
            }

            @Override
            public boolean isSyncfsFileSystemWide() {
                return syncfsFileSystemWide;
            }

            @Override
            public void syncfs(long fd) {
                syncfsCalls.incrementAndGet();
                super.syncfs(fd);
            }

            private void countColumnFile(long fd) {
                final String path = pathOf(fd);
                if (path != null && path.endsWith(".d")) {
                    columnFileFsyncs.incrementAndGet();
                }
            }
        };
        crashFf.setDbRoot(root);
        assertMemoryLeak(crashFf, () -> {
            if (!newTable) {
                createTable();
            }
            markDurableBaseline();
            syncfsCalls.set(0);
            columnFileFsyncs.set(0);
            try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 1)) {
                importer.of("t", "t.csv", 1, PartitionBy.DAY, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                importer.process(AllowAllSecurityContext.INSTANCE);
            }
            final int importSyncfsCalls = syncfsCalls.get();
            final int importColumnFileFsyncs = columnFileFsyncs.get();
            final String imported = rows(engine);
            Assert.assertEquals(IMPORTED, imported);

            crashFf.powerLoss();
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            engine.releaseAllWalWriters();
            engine.releaseInactive();
            crashFf.crash(root);
            final boolean promised = "sync".equals(commitMode) || ("adaptive".equals(commitMode) && walTable);
            try (CairoEngine restarted = new CairoEngine(configuration)) {
                TestUtils.drainWalQueue(restarted);
                final String recovered = rows(restarted);
                if (promised) {
                    Assert.assertEquals("a finished COPY must survive a power loss", imported, recovered);
                } else {
                    // No promise: the rows that come back are some of the imported ones.
                    for (String line : recovered.split("\n")) {
                        Assert.assertTrue("unexpected row after the crash: " + line, imported.contains(line));
                    }
                    Assert.assertEquals("no flush where nothing is promised", 0, importSyncfsCalls);
                }
            }
            // An existing WAL table in ADAPTIVE is flushed by the writer's save point instead.
            if ("sync".equals(commitMode) || ("adaptive".equals(commitMode) && walTable && newTable)) {
                if (syncfsFileSystemWide) {
                    Assert.assertEquals("one syncfs before the attach and one after it make the import durable", 2, importSyncfsCalls);
                    Assert.assertEquals("no column file fsync where syncfs is filesystem-wide", 0, importColumnFileFsyncs);
                } else {
                    Assert.assertTrue("the import is flushed file by file", importColumnFileFsyncs > 0);
                }
            }
        });
    }

    @Test
    public void testPowerLossBeforeTheImportIsDurableLeavesNoCorruptTable() throws Exception {
        // A power loss before any device flush from the partition move on, before COPY reports done, with or without
        // the kernel having written _txn and the database root back on its own: after the restart the table is gone
        // (a created one), or readable with whole imported partitions, never a table whose _txn names partitions with
        // incomplete column or symbol files.
        assumeCrashHarnessSupported();
        Assume.assumeTrue(
                "nothing is promised on a power loss",
                "sync".equals(commitMode) || ("adaptive".equals(commitMode) && walTable)
        );
        // The file-by-file flush makes about fifty power loss points per row, which takes minutes.
        Assume.assumeTrue(
                "the file-by-file sweep is nightly-only; run with -Dquestdb.fuzz.nightly=true",
                syncfsFileSystemWide || Boolean.getBoolean("questdb.fuzz.nightly")
        );
        final AtomicInteger windowStart = new AtomicInteger();
        final AtomicInteger freezeAt = new AtomicInteger(-1);
        final AtomicBoolean frozen = new AtomicBoolean();
        final AtomicBoolean txnWrittenBack = new AtomicBoolean();
        final AtomicReference<String> txnPath = new AtomicReference<>();
        final AtomicBoolean writingBack = new AtomicBoolean();
        final BooleanSupplier poweredOff = () -> {
            if (!frozen.get() && !writingBack.get() && freezeAt.get() > -1 && crashFf.durabilityOpCount() >= freezeAt.get()) {
                if (txnWrittenBack.get()) {
                    // The kernel may have written back the table's _txn, and the database root's files and entries,
                    // a created table's among them, on its own.
                    writingBack.set(true);
                    try {
                        writeBackMetadata(txnPath.get());
                    } finally {
                        writingBack.set(false);
                    }
                }
                // The power fails here: no later flush reaches the disk.
                crashFf.powerLoss();
                frozen.set(true);
            }
            return frozen.get();
        };
        crashFf = new CrashFaultFilesFacade() {
            @Override
            public void fdatasync(long fd) {
                if (!poweredOff.getAsBoolean()) {
                    super.fdatasync(fd);
                }
            }

            @Override
            public void fsync(long fd) {
                if (!poweredOff.getAsBoolean()) {
                    super.fsync(fd);
                }
            }

            @Override
            public void fsyncAndClose(long fd) {
                if (poweredOff.getAsBoolean()) {
                    close(fd);
                } else {
                    super.fsyncAndClose(fd);
                }
            }

            @Override
            public boolean isSyncfsFileSystemWide() {
                return syncfsFileSystemWide;
            }

            @Override
            public void msync(long addr, long len, boolean async) {
                if (!poweredOff.getAsBoolean()) {
                    super.msync(addr, len, async);
                }
            }

            @Override
            public int syncFileRange(long fd, long offset, long nbytes, int flags) {
                return poweredOff.getAsBoolean() ? 0 : super.syncFileRange(fd, offset, nbytes, flags);
            }

            @Override
            public void syncfs(long fd) {
                if (!poweredOff.getAsBoolean()) {
                    super.syncfs(fd);
                }
            }
        };
        crashFf.setDbRoot(root);
        assertMemoryLeak(crashFf, () -> {
            // The first round records the device flushes from the partition move on; round k loses power after k of
            // them, before the next one.
            final List<String> flushes = new ArrayList<>();
            for (int k = -1; k <= flushes.size(); k++) {
                for (int variant = k < 0 ? 1 : 0; variant < 2; variant++) {
                    frozen.set(false);
                    freezeAt.set(-1);
                    txnWrittenBack.set(variant == 1);
                    startRound(k > -1, true);
                    final int powerLossAfter = k;
                    try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 1)) {
                        importer.of("t", "t.csv", 1, PartitionBy.DAY, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                        importer.setStatusReporter((phase, status, msg, rowsHandled, rowsImported, errors) -> {
                            if (phase == CopyImportTask.PHASE_MOVE_PARTITIONS && status == CopyImportTask.STATUS_STARTED) {
                                windowStart.set(crashFf.durabilityOpCount());
                                txnPath.set(java.nio.file.Path.of(root, engine.verifyTableName("t").getDirName(), "_txn").toString());
                                freezeAt.set(powerLossAfter > -1 ? windowStart.get() + powerLossAfter : -1);
                            }
                        });
                        importer.process(AllowAllSecurityContext.INSTANCE);
                    }
                    if (k < 0) {
                        Assert.assertEquals(IMPORTED, rows(engine));
                        final List<String> log = crashFf.durabilityOpLog();
                        flushes.addAll(log.subList(windowStart.get(), log.size()));
                        Assert.assertFalse("the import flushes", flushes.isEmpty());
                        continue;
                    }
                    // Every round makes the same flushes: the power failed during the import, or fails now, after the
                    // last flush.
                    Assert.assertEquals(k < flushes.size(), frozen.get());
                    Assert.assertTrue(poweredOff.getAsBoolean());
                    final String context = "power loss after flush " + k + " of " + flushes.size()
                            + (k < flushes.size() ? " [before " + flushes.get(k) + ']' : "")
                            + (variant == 1 ? ", metadata written back" : "");
                    engine.releaseAllReaders();
                    engine.releaseAllWriters();
                    engine.releaseAllWalWriters();
                    engine.releaseInactive();
                    crashFf.crash(root);
                    // An existing WAL table in ADAPTIVE comes back to the writer's save point instead.
                    final boolean importerFlushes = "sync".equals(commitMode) || newTable;
                    final String outcome = context + (importerFlushes && redoLostRenames(txnPath.get()) > 0 ? ", rename lost" : "");
                    try (CairoEngine restarted = new CairoEngine(configuration)) {
                        assertNoCorruptImport(restarted, outcome);
                    } catch (Throwable th) {
                        throw new AssertionError(outcome + ": " + th, th);
                    }
                }
            }
        });
    }

    private void assertFailedFlushStopsTheEngine() throws Exception {
        // syncfs reports a write-back error of any file on the filesystem, so a failed device flush may belong to
        // another table: in SYNC, and in ADAPTIVE for a WAL table, the engine must stop, whichever flush that makes
        // the imported partitions live and durable fails. It stops at the flush, before the importer can clean up
        // (a server halts there), so the table keeps what the failed COPY left until the guide's manual step clears it.
        if ("adaptive".equals(commitMode)) {
            Assume.assumeTrue("a non-WAL table in adaptive commit mode is flushed nowhere", walTable);
            Assume.assumeTrue("an existing table is flushed by the writer's save point, which stops the engine itself", newTable);
        } else {
            Assume.assumeTrue("the import flushes nothing in nosync and async", "sync".equals(commitMode));
        }
        assumeCrashHarnessSupported();
        final AtomicBoolean attaching = new AtomicBoolean();
        final AtomicInteger failAt = new AtomicInteger();
        final AtomicInteger flushes = new AtomicInteger();
        final StringSink failedFlush = new StringSink();
        crashFf = new CrashFaultFilesFacade() {
            @Override
            public void barrierFsync(long fd) {
                failIfDue("barrierFsync", fd);
                super.barrierFsync(fd);
            }

            @Override
            public void fdatasync(long fd) {
                failIfDue("fdatasync", fd);
                super.fdatasync(fd);
            }

            @Override
            public void fsync(long fd) {
                failIfDue("fsync", fd);
                super.fsync(fd);
            }

            @Override
            public void fsyncAndClose(long fd) {
                try {
                    failIfDue("fsyncAndClose", fd);
                } catch (CairoException e) {
                    close(fd);
                    throw e;
                }
                super.fsyncAndClose(fd);
            }

            @Override
            public void fsyncDurable(long fd) {
                failIfDue("fsyncDurable", fd);
                super.fsyncDurable(fd);
            }

            @Override
            public boolean isSyncfsFileSystemWide() {
                return syncfsFileSystemWide;
            }

            @Override
            public void msync(long addr, long len, boolean async) {
                failIfDue("msync", -1);
                super.msync(addr, len, async);
            }

            @Override
            public void syncfs(long fd) {
                failIfDue("syncfs", fd);
                super.syncfs(fd);
            }

            private void failIfDue(String operation, long fd) {
                if (attaching.get() && failAt.get() > 0 && flushes.incrementAndGet() == failAt.get()) {
                    failedFlush.clear();
                    failedFlush.put(operation).put(' ').put(fd > -1 ? pathOf(fd) : "");
                    throw CairoException.dataSyncFailure(this, 5, operation, "injected [operation=" + operation + ']');
                }
            }
        };
        crashFf.setDbRoot(root);
        assertMemoryLeak(crashFf, () -> {
            startRound(false, false);
            // Fail the first device flush from the attach on, then the second, and so on, until an import completes
            // without reaching the failing one.
            for (int i = 1; ; i++) {
                attaching.set(false);
                flushes.set(0);
                failAt.set(i);
                Throwable failure = null;
                try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(engine, 1)) {
                    importer.of("t", "t.csv", 1, PartitionBy.DAY, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                    importer.setStatusReporter((phase, status, msg, rowsHandled, rowsImported, errors) -> {
                        if (phase == CopyImportTask.PHASE_ATTACH_PARTITIONS && status == CopyImportTask.STATUS_STARTED) {
                            attaching.set(true);
                        }
                    });
                    importer.process(AllowAllSecurityContext.INSTANCE);
                } catch (Throwable th) {
                    failure = th;
                }
                failAt.set(0);
                if (flushes.get() < i) {
                    Assert.assertNull("round " + i, failure);
                    Assert.assertTrue("no device flush from the attach on", i > 1);
                    break;
                }
                final String context = "failed flush " + i + " [" + failedFlush + ']';
                Assert.assertNotNull(context, failure);
                Assert.assertTrue(context, engine.isDurabilityFailed());
                try {
                    execute("INSERT INTO other VALUES ('2024-01-01', 1)");
                    Assert.fail(context + ": a write was accepted after the engine stopped");
                } catch (CairoException | CairoError expected) {
                    // The engine refuses writes.
                } finally {
                    engine.resetDurabilityFailure();
                }
                if (newTable) {
                    clearCreatedTableAfterRestart(context);
                    startRound(true, false);
                } else {
                    // The table can keep rows of the failed COPY until it is truncated.
                    assertOnlyImportedRows(context, rows(engine));
                    execute("TRUNCATE TABLE t");
                    drainWalQueue();
                    // A failed attach leaves imported partitions next to the table's own as .attachable directories;
                    // a retry fails to move its partitions onto them until they are removed.
                    removeLeftoverAttachables();
                }
            }
        });
    }

    private void assertNoCorruptImport(CairoEngine restarted, String context) throws Exception {
        TestUtils.drainWalQueue(restarted);
        if (restarted.getTableTokenIfExists("t") == null) {
            Assert.assertTrue(context + ": the existing table is gone", newTable);
            return;
        }
        final StringSink sink = new StringSink();
        restarted.print("SELECT * FROM t", sink);
        // Each attach is a commit of its own, so the table holds whole imported partitions, with their own values.
        final String[] imported = IMPORTED.split("\n");
        final String[] recovered = sink.toString().split("\n");
        Assert.assertEquals(context, imported[0], recovered[0]);
        for (int i = 1; i < imported.length; i++) {
            final String day = imported[i].substring(0, 10);
            final boolean dayRecovered = sink.toString().contains("\n" + day);
            Assert.assertEquals(context + ": a partition is attached whole [row=" + imported[i] + ']', dayRecovered, sink.toString().contains(imported[i]));
        }
        for (int i = 1; i < recovered.length; i++) {
            Assert.assertTrue(context + ": unexpected row " + recovered[i], IMPORTED.contains(recovered[i]));
        }
    }

    private void assertOnlyImportedRows(String context, String rows) {
        for (String line : rows.split("\n")) {
            Assert.assertTrue(context + ": unexpected row " + line, IMPORTED.contains(line));
        }
    }

    private void clearCreatedTableAfterRestart(String context) throws Exception {
        // A server halts inside the failed flush, so the created table keeps what the failed COPY left. A test engine
        // runs on after the stop, and where the failure reaches the importer without its flush flag, its clean-up
        // drops the table; either way the restart must find nothing but imported rows. The guide's step for a non-WAL
        // table is DROP TABLE. A WAL table the COPY created has no sequencer after a restart, so no SQL step works on
        // it: its directory is deleted while the server is stopped, and the table is created before the COPY runs
        // again.
        releaseEngineHandles();
        // The registry lock is the server's: the next round's setUp() takes it again.
        engine.closeNameRegistry();
        String walTableDir = null;
        try (CairoEngine restarted = new CairoEngine(configuration)) {
            final TableToken token = restarted.getTableTokenIfExists("t");
            if (token != null) {
                assertOnlyImportedRows(context, rows(restarted));
                if (walTable) {
                    walTableDir = token.getDirName();
                } else {
                    restarted.execute("DROP TABLE t");
                }
            }
        }
        if (walTableDir != null) {
            deleteRecursively(java.nio.file.Path.of(root, walTableDir));
        }
        try (CairoEngine restarted = new CairoEngine(configuration)) {
            Assert.assertNull(context + ": the manual step removes the table", restarted.getTableTokenIfExists("t"));
            if (walTable) {
                restarted.execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            }
            try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(restarted, 1)) {
                importer.of("t", "t.csv", 1, PartitionBy.DAY, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
                importer.process(AllowAllSecurityContext.INSTANCE);
            }
            TestUtils.drainWalQueue(restarted);
            Assert.assertEquals(context + ": the COPY runs again", IMPORTED, rows(restarted));
        }
    }

    private void createTable() throws SqlException {
        execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY" + (walTable ? " WAL" : " BYPASS WAL"));
        drainWalQueue();
    }

    private void removeLeftoverAttachables() throws IOException {
        final java.nio.file.Path tableDir = java.nio.file.Path.of(root, engine.verifyTableName("t").getDirName());
        try (Stream<java.nio.file.Path> dirs = Files.list(tableDir)) {
            for (java.nio.file.Path dir : dirs.filter(d -> d.getFileName().toString().endsWith(".attachable")).toList()) {
                deleteRecursively(dir);
            }
        }
    }

    private int redoLostRenames(String txnPath) throws IOException {
        // An attach renames the partition's .attachable directory and names the partition in _txn; until the table
        // directory is flushed after it, a power loss can keep _txn but lose the rename. The partition's files then
        // stay whole in the .attachable directory, as the flush before the attach made them: the rename is redone
        // here, so the checks after the restart see whether _txn names complete files.
        final java.nio.file.Path txn = java.nio.file.Path.of(txnPath);
        if (!Files.exists(txn) || Files.size(txn) == 0) {
            return 0;
        }
        int redone = 0;
        try (TxReader txReader = new TxReader(crashFf); Path path = new Path()) {
            txReader.ofRO(path.of(txnPath).$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
            if (!txReader.unsafeLoadAll()) {
                return 0;
            }
            for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
                path.of(txn.getParent().toString());
                TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, txReader.getPartitionTimestampByIndex(i), txReader.getPartitionNameTxn(i));
                final java.nio.file.Path partitionDir = java.nio.file.Path.of(path.toString());
                if (!Files.exists(partitionDir)) {
                    final String name = partitionDir.getFileName().toString();
                    final int dot = name.indexOf('.');
                    final java.nio.file.Path attachable = txn.resolveSibling((dot > 0 ? name.substring(0, dot) : name) + ".attachable");
                    Assert.assertTrue("_txn names a partition that is nowhere in the table directory: " + name, Files.isDirectory(attachable));
                    Files.move(attachable, partitionDir);
                    redone++;
                }
            }
        }
        return redone;
    }

    private void startRound(boolean restart, boolean powerLoss) throws Exception {
        if (restart) {
            tearDown(true);
            TestUtils.removeTestPath(root);
            crashFf.reset();
            // The teardown resets the overrides, the files facade among them.
            ff = crashFf;
            setUp();
        }
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, Boolean.toString(walTable));
        if (powerLoss) {
            // A power loss takes no close-time save point.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        } else {
            // A stopped engine refuses a write to this table.
            execute("CREATE TABLE other (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
        }
        inputRoot = temp.newFolder().getAbsolutePath();
        inputWorkRoot = temp.newFolder().getAbsolutePath();
        Files.writeString(java.nio.file.Path.of(inputRoot, "t.csv"), CSV, StandardCharsets.UTF_8);
        if (!newTable) {
            createTable();
        }
        if (powerLoss) {
            markDurableBaseline();
        }
    }

    private void writeBackMetadata(String txnPath) {
        crashFf.markFileDurable(txnPath);
        if (walTable && !Files.isDirectory(java.nio.file.Path.of(txnPath).resolveSibling(WalUtils.SEQ_DIR))) {
            // Until a WAL table the import creates has its sequencer on disk, its registry entry is left alone.
            return;
        }
        final java.io.File[] rootFiles = new java.io.File(root).listFiles(java.io.File::isFile);
        if (rootFiles != null) {
            for (java.io.File file : rootFiles) {
                crashFf.markFileDurable(file.getAbsolutePath());
            }
        }
        try (Path path = new Path()) {
            final long fd = crashFf.openRONoCache(path.of(root).$());
            Assert.assertTrue(fd > -1);
            crashFf.fsyncAndClose(fd);
        }
    }

    private static void deleteRecursively(java.nio.file.Path dir) throws IOException {
        try (Stream<java.nio.file.Path> files = Files.walk(dir)) {
            for (java.nio.file.Path file : files.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(file);
            }
        }
    }

    private static String rows(CairoEngine e) {
        final StringSink sink = new StringSink();
        try {
            e.print("SELECT * FROM t", sink);
        } catch (SqlException | CairoException ex) {
            // The table did not survive.
            return "";
        }
        return sink.toString();
    }
}
