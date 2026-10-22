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
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * What a failed flush does, by commit mode. In {@code sync} and {@code adaptive} it stops the engine; in
 * {@code nosync} and {@code async} it fails only the operation, as before adaptive commit, and the table stays
 * usable. The engine's handler owns the rule, so every caller follows it; only the stops decided for every mode
 * call {@code handleDataSyncFailureInEveryMode}.
 * <p>
 * Each path test injects a failure into a flush that the base release already ran in that mode, so the flush is
 * reached outside the durable modes too.
 */
@RunWith(Parameterized.class)
public class DataSyncFailureModePolicyTest extends AbstractCairoTest {
    private static final int EIO = 5;
    private static final int FSYNC = 1;
    private static final int MSYNC = 0;
    private final String commitMode;
    private FailingFlushFacade ff;

    public DataSyncFailureModePolicyTest(String commitMode) {
        this.commitMode = commitMode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{"nosync"}, {"async"}, {"sync"}, {"adaptive"}});
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        ff = new FailingFlushFacade();
    }

    @Test
    public void testAlterWalTableAddedColumnFsync() throws Exception {
        // A column added to a WAL writer that holds uncommitted rows (ILP adds columns mid-batch) is null-filled and
        // fsynced, in every mode.
        assertFlushFailure(
                "/wal1/0/c.d", FSYNC, 0, true,
                () -> execute("CREATE TABLE w (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL"),
                () -> {
                    try (WalWriter walWriter = getWalWriter("w")) {
                        final TableWriter.Row row = walWriter.newRow(0);
                        row.putLong(1, 1);
                        row.append();
                        addColumn(walWriter, "c", ColumnType.INT);
                        walWriter.commit();
                    }
                },
                () -> {
                    execute("INSERT INTO w VALUES ('2024-01-01T00:00:01.000000Z', 2, 3)");
                    drainWalQueue();
                    assertQuery("SELECT count() FROM w").noRandomAccess().expectSize().returns("count\n1\n");
                }
        );
    }

    @Test
    public void testAlterWalTableSequencerMetaMsync() throws Exception {
        // The sequencer's _meta is msynced (MS_SYNC) on every structural change, in every mode.
        assertFlushFailure(
                "txn_seq/_meta", MSYNC, 0, true,
                () -> execute("CREATE TABLE w (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL"),
                () -> execute("ALTER TABLE w ADD COLUMN c INT"),
                () -> {
                    execute("INSERT INTO w (ts, v) VALUES ('2024-01-01T00:00:00.000000Z', 1)");
                    drainWalQueue();
                    assertQuery("SELECT count() FROM w").noRandomAccess().expectSize().returns("count\n1\n");
                }
        );
    }

    @Test
    public void testDdlMetaSwapMsync() throws Exception {
        // _meta.swp is msynced (MS_SYNC) on every schema change, in every mode.
        assertFlushFailure(
                "_meta.swp", MSYNC, 0, true,
                () -> {
                    execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                    execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 1)");
                },
                () -> execute("ALTER TABLE t ADD COLUMN c INT"),
                () -> {
                    execute("ALTER TABLE t ADD COLUMN c INT");
                    execute("INSERT INTO t VALUES ('2024-01-01T00:00:01.000000Z', 2, 3)");
                    assertQuery("SELECT count() FROM t").noRandomAccess().expectSize().returns("count\n2\n");
                }
        );
    }

    @Test
    public void testDdlRestoreMetaTodoMsync() throws Exception {
        // The first _todo_ msync of a schema change on an open writer is the restore-meta marker, in every mode.
        assertFlushFailure(
                "_todo_", MSYNC, 0, true,
                () -> {
                    execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                    execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 1)");
                },
                () -> execute("ALTER TABLE t ADD COLUMN c INT"),
                () -> {
                    execute("INSERT INTO t (ts, v) VALUES ('2024-01-01T00:00:01.000000Z', 2)");
                    assertQuery("SELECT count() FROM t").noRandomAccess().expectSize().returns("count\n2\n");
                }
        );
    }

    @Test
    public void testDdlTodoClearMsync() throws Exception {
        // The second _todo_ msync of a schema change clears the marker, in every mode.
        assertFlushFailure(
                "_todo_", MSYNC, 1, true,
                () -> {
                    execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                    execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 1)");
                },
                () -> execute("ALTER TABLE t ADD COLUMN c INT"),
                () -> {
                    execute("INSERT INTO t (ts, v) VALUES ('2024-01-01T00:00:01.000000Z', 2)");
                    assertQuery("SELECT count() FROM t").noRandomAccess().expectSize().returns("count\n2\n");
                }
        );
    }

    @Test
    public void testEngineHandlerFollowsCommitMode() {
        final boolean durable = CommitMode.isPowerLossDurable(CommitMode.fromString(commitMode));
        try {
            try {
                engine.handleDataSyncFailure(CairoException.dataSyncFailureCarry(EIO, "fsync").put("injected"));
                Assert.assertFalse("a durable mode must stop the engine", durable);
            } catch (CairoError e) {
                Assert.assertTrue("only a durable mode may stop the engine", durable);
            }
            Assert.assertEquals(durable, engine.isDurabilityFailed());
            engine.resetDurabilityFailure();

            final CairoError poisoned = engine.poisonOnDataSyncFailure(CairoException.dataSyncFailureCarry(EIO, "msync").put("injected"));
            Assert.assertEquals(durable, poisoned != null);
            Assert.assertEquals(durable, engine.isDurabilityFailed());
            engine.resetDurabilityFailure();

            // A failure without the flag never stops the engine, and the handler leaves it to the caller.
            engine.handleDataSyncFailure(CairoException.critical(EIO).put("not a flush"));
            Assert.assertFalse(engine.isDurabilityFailed());

            // The stops decided for every mode.
            try {
                engine.handleDataSyncFailureInEveryMode(CairoException.dataSyncFailureCarry(EIO, "fsync").put("injected"));
                Assert.fail("the every-mode stop must stop the engine");
            } catch (CairoError expected) {
                Assert.assertTrue(engine.isDurabilityFailed());
            }
        } finally {
            engine.resetDurabilityFailure();
        }
    }

    @Test
    public void testEveryModeStopsAreTheDecidedOnes() throws IOException {
        // A stop that does not follow the resolved commit mode needs a decision: the WAL replay before a conversion
        // to non-WAL, in every mode (guide sections 8.1 and 11.2), and the Enterprise promote flush, which runs only
        // in adaptive and must stop while the replica's policy may still resolve adaptive to nosync (D60).
        Assume.assumeTrue("one source scan is enough", "nosync".equals(commitMode));
        Assert.assertEquals(
                "[TableConverter.java]" + (hasEnterpriseSources() ? "+[EntCairoEngine.java]" : ""),
                filesUsing(Pattern.compile("handleDataSyncFailureInEveryMode\\("), "void handleDataSyncFailureInEveryMode(")
        );
    }

    @Test
    public void testFlushFailureRuleLivesInTheEngine() throws IOException {
        // CairoEngine.isDataSyncFailureFatal is the one rule for a failed flush (D60). The raw flag is read only by
        // the engine, the exception itself and the code that carries a failure across a task boundary or decides a
        // stop named above. The pool preserves an already-decided CairoError; restore distinguishes flush failure
        // from corruption for quarantine, not fatality. Every other site asks the engine for the mode decision.
        Assume.assumeTrue("one source scan is enough", "nosync".equals(commitMode));
        Assert.assertEquals(
                "[AbstractMultiTenantPool.java, CairoEngine.java, CairoException.java, ConvertOperatorImpl.java, SeqTxnTracker.java, TableConverter.java, "
                        + "TableWriter.java, WalGroupCommitFlushQueue.java]" + (hasEnterpriseSources() ? "+[BackupRestoreAgent.java, EntCairoEngine.java]" : ""),
                filesUsing(Pattern.compile("isDataSyncFailure\\("), null)
        );
        // Every Java flush failure is created by the recording factory (D61); only the code that carries one already
        // recorded raises it again without telling the engines.
        Assert.assertEquals(
                "[CairoException.java, ConvertOperatorImpl.java, SeqTxnTracker.java, TableWriter.java]",
                filesUsing(Pattern.compile("dataSyncFailureCarry\\("), "CairoException dataSyncFailureCarry(")
        );
        // Configured reads are classified exceptions, not new flush gates. K: boot (RecoveryCoordinator,
        // TableConverter), promotion (EntCairoEngine), local WAL/view/REBASE truth, lifecycle anchors
        // (TableWriter, WalPurgeJob, WalEvents) and primary-only upload. U: configuration definitions,
        // one-shot structural/checkpoint writers, lazy column/index seeds, cold storage's existing grade,
        // COPY (no replica ingress, including the import task), CREATE TABLE's file creation (TableUtils skips the
        // per-symbol msync in nosync). CairoEngine and V1/V2 own the live resolution; the WAL table scan
        // (TableSequencerAPI) applies the V1/V2 open's own rule through the same resolution. Composite-partition
        // writes (O3PartitionJob's geometry and piece flushes) take the writer's flush grade, and the compaction
        // job (PartitionCompactionScanJob) the engine's resolved mode, so neither reads the configured mode.
        // A new file reading the configured mode needs classification here before it can bypass the policy.
        Assert.assertEquals(
                "[BitmapIndexWriter.java, CairoConfiguration.java, CairoConfigurationWrapper.java, CairoEngine.java, "
                        + "ColumnVersionWriter.java, CopyImportTask.java, DatabaseCheckpointAgent.java, DefaultCairoConfiguration.java, "
                        + "DefaultLineUdpReceiverConfiguration.java, IDGeneratorFactory.java, "
                        + "LineUdpReceiverConfiguration.java, LiveViewCheckpointDataSegmentWriter.java, "
                        + "LiveViewCheckpointMetaSegmentWriter.java, LiveViewCheckpointRepairMarker.java, "
                        + "LiveViewCheckpointRepairState.java, LiveViewCheckpointSuperblock.java, LiveViewRefreshJob.java, "
                        + "MemoryPMARImpl.java, ParallelCsvFileImporter.java, PostingIndexWriter.java, PropServerConfiguration.java, "
                        + "RecoveryCoordinator.java, SequencerMetadata.java, SettingsStore.java, SqlCompilerImpl.java, "
                        + "TableConverter.java, TableSequencerAPI.java, TableSequencerImpl.java, TableSnapshotRestore.java, TableTransactionLog.java, "
                        + "TableTransactionLogV1.java, TableTransactionLogV2.java, TableUtils.java, TableWriter.java, TxWriter.java, ViewWalWriter.java, "
                        + "WalEventWriter.java, WalPurgeJob.java, WalUtils.java, WalWriter.java, WalWriterBase.java]"
                        + (hasEnterpriseSources() ?
                        "+[ColdStorageTaskJob.java, EntCairoEngine.java, EntPropServerConfiguration.java, PrimaryRoleState.java, "
                        + "StoragePolicyJob.java, StoragePolicyWriterCommand.java, StoragePolicyWriterImpl.java, WalEvents.java]"
                        : ""),
                filesUsing(Pattern.compile("getCommitMode\\(\\)"), null)
        );
        // poisonOnDataSyncFailure returns null when the failure is not fatal: each caller is inside a fatal branch.
        final List<String> unguarded = new ArrayList<>();
        for (Path root : sourceRoots()) {
            try (Stream<Path> files = Files.walk(root)) {
                for (Path file : (Iterable<Path>) files.filter(p -> p.toString().endsWith(".java"))::iterator) {
                    final List<String> lines = Files.readAllLines(file);
                    for (int i = 0, n = lines.size(); i < n; i++) {
                        final String line = lines.get(i);
                        if (!line.contains("poisonOnDataSyncFailure(") || line.contains("CairoError poisonOnDataSyncFailure(")
                                || line.trim().startsWith("*") || line.contains("return engine.poisonOnDataSyncFailure(")) {
                            continue;
                        }
                        boolean guarded = false;
                        for (int j = Math.max(0, i - 8); j < i && !guarded; j++) {
                            guarded = lines.get(j).contains("isDataSyncFailureFatal(");
                        }
                        if (!guarded) {
                            unguarded.add(file.getFileName() + ":" + (i + 1));
                        }
                    }
                }
            }
        }
        Assert.assertEquals("[]", unguarded.toString());
    }

    @Test
    public void testReplicaAppliedAlterMetaSwapMsync() throws Exception {
        // A replica's applied ALTER msyncs _meta.swp (MS_SYNC), in every mode.
        assertReplicaFlushFailure(
                "_meta.swp",
                () -> {
                    execute("CREATE TABLE w (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    execute("INSERT INTO w VALUES ('2024-01-01T00:00:00.000000Z', 1)");
                    drainWalQueue();
                },
                () -> {
                    execute("ALTER TABLE w ADD COLUMN c INT");
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testReplicaApplyWriterOpenTodoClearMsync() throws Exception {
        // The apply job's writer open clears _todo_ and msyncs it (MS_SYNC), in every mode.
        assertReplicaFlushFailure(
                "_todo_",
                () -> {
                    execute("CREATE TABLE w (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    execute("INSERT INTO w VALUES ('2024-01-01T00:00:00.000000Z', 1)");
                    drainWalQueue();
                    engine.releaseAllWriters();
                },
                () -> {
                    execute("INSERT INTO w VALUES ('2024-01-01T00:00:01.000000Z', 2)");
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalSegmentDirectoryFsync() throws Exception {
        Assume.assumeFalse("a WAL segment directory takes no fsync on Windows", Os.isWindows());
        // async fsyncs a new segment's directory, as the base release does; nosync takes none.
        assertFlushFailure(
                "/wal1/0", FSYNC, 0, !"nosync".equals(commitMode),
                () -> execute("CREATE TABLE w (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL"),
                () -> execute("INSERT INTO w VALUES ('2024-01-01T00:00:00.000000Z', 1)"),
                () -> {
                    execute("INSERT INTO w VALUES ('2024-01-01T00:00:01.000000Z', 2)");
                    drainWalQueue();
                    // nosync takes no segment directory fsync, so its first insert went through
                    assertQuery("SELECT count() FROM w").noRandomAccess().expectSize().returns("count\n" + (ff.failures > 0 ? 1 : 2) + "\n");
                }
        );
    }

    @Test
    public void testWriterOpenTodoClearMsync() throws Exception {
        // Every writer open clears _todo_ and msyncs it (MS_SYNC), in every mode. The base release ignored a failure.
        assertFlushFailure(
                "_todo_", MSYNC, 0, true,
                () -> {
                    execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                    execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 1)");
                    engine.releaseAllWriters();
                },
                () -> execute("INSERT INTO t VALUES ('2024-01-01T00:00:01.000000Z', 2)"),
                () -> {
                    execute("INSERT INTO t VALUES ('2024-01-01T00:00:02.000000Z', 3)");
                    assertQuery("SELECT count() FROM t").noRandomAccess().expectSize().returns("count\n" + (ff.failures > 0 ? 2 : 3) + "\n");
                }
        );
    }

    /**
     * Runs {@code operation} with the {@code skip}+1-th flush of a file whose path contains {@code needle} failing.
     * In {@code sync} and {@code adaptive} the engine must stop. In {@code nosync} and {@code async} it must not,
     * and {@code followUp} must succeed on the same table.
     */
    private void assertFlushFailure(
            String needle,
            int kind,
            int skip,
            boolean reachedInThisMode,
            TestUtils.LeakProneCode setup,
            TestUtils.LeakProneCode operation,
            TestUtils.LeakProneCode followUp
    ) throws Exception {
        final boolean durable = CommitMode.isPowerLossDurable(CommitMode.fromString(commitMode));
        assertMemoryLeak(ff, () -> {
            try {
                setup.run();
                ff.arm(needle, kind, skip);
                Throwable failure = null;
                try {
                    operation.run();
                } catch (Throwable th) {
                    failure = th;
                } finally {
                    ff.disarm();
                }
                if (reachedInThisMode) {
                    Assert.assertEquals("the injected flush failure must be reached", 1, ff.failures);
                }
                if (durable && ff.failures > 0) {
                    Assert.assertTrue("a failed flush must stop the engine in " + commitMode, engine.isDurabilityFailed());
                    Assert.assertTrue("the operation must fail with the engine stop", failure instanceof CairoError);
                    return;
                }
                Assert.assertFalse(
                        "a failed flush must not stop the engine in " + commitMode + ", failure: " + failure,
                        engine.isDurabilityFailed()
                );
                Assert.assertFalse("the operation must not fail with an engine stop: " + failure, failure instanceof CairoError && CairoException.isDataSyncFailure(failure));
                engine.releaseAllWriters();
                followUp.run();
                Assert.assertFalse(engine.isDurabilityFailed());
            } finally {
                ff.disarm();
                engine.releaseAllWriters();
                engine.resetDurabilityFailure();
            }
        });
    }

    /**
     * Runs {@code apply} on an engine whose save point policy is the replica's (D60), with the first msync of a file
     * whose path ends with {@code needle} failing. The engine stops exactly where the mode the policy resolves is
     * durable: on a replica adaptive resolves to nosync, so an adaptive replica reports the error and keeps applying.
     */
    private void assertReplicaFlushFailure(
            String needle,
            TestUtils.LeakProneCode setup,
            TestUtils.LeakProneCode apply
    ) throws Exception {
        final boolean durable = CommitMode.isPowerLossDurable(
                LocalDurabilityPolicy.resolveCommitMode(CommitMode.fromString(commitMode), LocalDurabilityPolicy.REPLICA_SKIP)
        );
        assertMemoryLeak(ff, () -> {
            try {
                setup.run();
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
                ff.arm(needle, MSYNC, 0);
                try {
                    apply.run();
                } catch (Throwable ignore) {
                    // the statement fails or the apply job suspends the table; the check is on the engine
                } finally {
                    ff.disarm();
                }
                Assert.assertEquals("the injected flush failure must be reached", 1, ff.failures);
                Assert.assertEquals(
                        "a replica stops on a failed flush exactly where its resolved mode is durable [mode=" + commitMode + ']',
                        durable,
                        engine.isDurabilityFailed()
                );
                if (!durable) {
                    // The replica keeps applying.
                    execute("ALTER TABLE w RESUME WAL");
                    execute("INSERT INTO w (ts, v) VALUES ('2024-01-01T00:00:02.000000Z', 3)");
                    drainWalQueue();
                    assertQuery("SELECT count() FROM w WHERE v = 3").noRandomAccess().expectSize().returns("count\n1\n");
                }
            } finally {
                ff.disarm();
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
                engine.releaseAllWriters();
                engine.resetDurabilityFailure();
            }
        });
    }

    /**
     * The source files, OSS first and then Enterprise ({@code +[...]}), with a line matching {@code token} that is
     * not a comment and does not contain {@code definition}.
     */
    private static String filesUsing(Pattern token, String definition) throws IOException {
        final StringBuilder result = new StringBuilder();
        for (Path root : sourceRoots()) {
            final TreeSet<String> names = new TreeSet<>();
            try (Stream<Path> files = Files.walk(root)) {
                for (Path file : (Iterable<Path>) files.filter(p -> p.toString().endsWith(".java"))::iterator) {
                    for (String line : Files.readAllLines(file)) {
                        final String trimmed = line.trim();
                        if (trimmed.startsWith("//") || trimmed.startsWith("*") || (definition != null && line.contains(definition))) {
                            continue;
                        }
                        if (token.matcher(line).find()) {
                            names.add(file.getFileName().toString());
                            break;
                        }
                    }
                }
            }
            if (!names.isEmpty()) {
                result.append(result.isEmpty() ? "" : "+").append(names);
            }
        }
        return result.toString();
    }

    private static boolean hasEnterpriseSources() {
        return sourceRoots().size() > 1;
    }

    private static String pathToString(LPSZ name) {
        final int n = name.size();
        final StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            final char c = (char) (name.byteAt(i) & 0xFF);
            sb.append(c == '\\' ? '/' : c);
        }
        return sb.toString();
    }

    private static List<Path> sourceRoots() {
        final Path core = Paths.get(System.getProperty("user.dir"));
        final Path oss = core.resolve("src/main/java");
        Assume.assumeTrue("needs the core module's sources", Files.isDirectory(oss));
        final List<Path> roots = new ArrayList<>();
        roots.add(oss);
        // The Enterprise sources, when this checkout is the Enterprise submodule
        final Path enterprise = core.resolve("../../questdb-ent/src/main/java").normalize();
        if (Files.isDirectory(enterprise)) {
            roots.add(enterprise);
        }
        return roots;
    }

    /**
     * Fails one flush of a file whose path contains the armed needle, with the error {@code FilesFacadeImpl}
     * raises for a real failure: a data-sync failure.
     */
    private static class FailingFlushFacade extends TestFilesFacadeImpl {
        private final Map<Long, String> fdToPath = new HashMap<>();
        private final List<long[]> mappings = new ArrayList<>(); // {addrLo, addrHi, fd}
        volatile int failures;
        private volatile int kind;
        private volatile String needle;
        private volatile int skip;

        @Override
        public void fdatasync(long fd) {
            if (shouldFail(FSYNC, pathOfFd(fd))) {
                throw CairoException.dataSyncFailure(this, EIO, "fdatasync", "injected fdatasync failure [fd=" + fd + ']');
            }
            super.fdatasync(fd);
        }

        @Override
        public void fsync(long fd) {
            if (shouldFail(FSYNC, pathOfFd(fd))) {
                throw CairoException.dataSyncFailure(this, EIO, "fsync", "injected fsync failure [fd=" + fd + ']');
            }
            super.fsync(fd);
        }

        @Override
        public void fsyncAndClose(long fd) {
            if (shouldFail(FSYNC, pathOfFd(fd))) {
                close(fd);
                throw CairoException.dataSyncFailure(this, EIO, "fsyncAndClose", "injected fsync failure [fd=" + fd + ']');
            }
            super.fsyncAndClose(fd);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            final long addr = super.mmap(fd, len, offset, flags, memoryTag);
            if (addr > 0) {
                synchronized (this) {
                    mappings.add(new long[]{addr, addr + len, fd});
                }
            }
            return addr;
        }

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            final long newAddr = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
            synchronized (this) {
                removeMapping(addr);
                if (newAddr > 0) {
                    mappings.add(new long[]{newAddr, newAddr + newSize, fd});
                }
            }
            return newAddr;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            if (!async && shouldFail(MSYNC, pathOfAddress(addr))) {
                throw CairoException.dataSyncFailure(this, EIO, "msync", "could not msync");
            }
            super.msync(addr, len, async);
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            synchronized (this) {
                removeMapping(address);
            }
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openCleanRW(LPSZ name, long size) {
            return remember(super.openCleanRW(name, size), name);
        }

        @Override
        public long openRO(LPSZ name) {
            return remember(super.openRO(name), name);
        }

        @Override
        public long openRONoCache(LPSZ name) {
            return remember(super.openRONoCache(name), name);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            return remember(super.openRW(name, opts), name);
        }

        void arm(String needle, int kind, int skip) {
            this.failures = 0;
            this.kind = kind;
            this.skip = skip;
            this.needle = needle;
        }

        void disarm() {
            this.needle = null;
        }

        private synchronized String pathOfAddress(long addr) {
            for (int i = mappings.size() - 1; i >= 0; i--) {
                final long[] m = mappings.get(i);
                if (addr >= m[0] && addr < m[1]) {
                    return pathOfFd(m[2]);
                }
            }
            return "<unmapped>";
        }

        private synchronized String pathOfFd(long fd) {
            final String path = fdToPath.get(fd);
            return path == null ? "<unknown>" : path;
        }

        private synchronized long remember(long fd, LPSZ name) {
            if (fd > -1) {
                fdToPath.put(fd, pathToString(name));
            }
            return fd;
        }

        private void removeMapping(long addr) {
            for (int i = mappings.size() - 1; i >= 0; i--) {
                if (mappings.get(i)[0] == addr) {
                    mappings.remove(i);
                    return;
                }
            }
        }

        private synchronized boolean shouldFail(int kind, String path) {
            final String needle = this.needle;
            if (needle == null || this.kind != kind || !path.endsWith(needle)) {
                return false;
            }
            if (skip > 0) {
                skip--;
                return false;
            }
            this.needle = null;
            failures++;
            return true;
        }
    }
}
