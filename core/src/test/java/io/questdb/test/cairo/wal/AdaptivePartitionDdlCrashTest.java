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
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cutlass.text.ParallelCsvFileImporter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.ops.OperationDispatcher;
import io.questdb.mp.SCSequence;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * A process crash right after a partition operation must not change what the table holds. After a crash,
 * startup recovery rewinds an adaptive WAL table to its last save point and re-applies the WAL after it, so
 * an operation that consumes or destroys files the WAL cannot recreate is lost unless a save point follows
 * it. Each case runs a partition operation on a table with a given save-point history, crashes before any
 * later save point, restarts, and requires the rows, the partition list and the WAL apply position to equal
 * what the table showed before the crash, which is the result of the same statements without a crash.
 * Each operation runs through the WAL and directly in the writer of a hard-suspended table, the way
 * {@code FORCE DROP PARTITION} always runs: with the writer free, queued behind a busy writer, and with the
 * writer busy at dispatch but free by the time the statement is queued; a direct change is not in the WAL at
 * all. COPY and {@code RESUME WAL FROM TXN} change the table outside the WAL by themselves.
 * <p>
 * The crash is a process kill: everything written reached the page cache, nothing is synced on the way
 * out. A clean close would take a save point, so the close-time save point is switched off, except in the
 * histories that restart cleanly, where the close must take the save point the operation still owes. The
 * restart is a fresh {@link CairoEngine}, which runs startup recovery before the WAL apply resumes. The
 * NOSYNC history is the control: nothing is rewound there.
 */
@RunWith(Parameterized.class)
public class AdaptivePartitionDdlCrashTest extends AbstractCairoTest {
    private static final String CSV = """
            ts,s,v
            2024-02-01T00:00:00.000000Z,c1,101
            2024-02-01T01:00:00.000000Z,c2,102
            2024-02-02T00:00:00.000000Z,c3,103
            2024-02-03T00:00:00.000000Z,c4,104
            """;
    // Part of the .detached name and of the temporary name a DETACH uses before it.
    private static final String DETACHED_NAME = ".detach";
    private static final FilesFacade FF = new TestFilesFacadeImpl() {
        @Override
        public int copyRecursive(Path src, Path dst, int dirMode) {
            return afterMove(super.copyRecursive(src, dst, dirMode), src.$(), dst.$());
        }

        @Override
        public int hardLinkDirRecursive(Path src, Path dst, int dirMode) {
            // A link out of a partition directory or an .attachable one crosses to another device; a link between
            // two detached names stays on the device that holds them.
            if (crossDevice && !Utf8s.containsAscii(src.$(), DETACHED_NAME)) {
                return -1;
            }
            return afterMove(super.hardLinkDirRecursive(src, dst, dirMode), src.$(), dst.$());
        }

        @Override
        public boolean isCrossDeviceCopyError(int errno) {
            return crossDevice || super.isCrossDeviceCopyError(errno);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            if (Utf8s.containsAscii(name, DurableEpochManifest.FILE_NAME)) {
                final int failAfter = failSavePointsAfter;
                if (savePoints.incrementAndGet() > failAfter && failAfter > -1) {
                    return -1;
                }
            }
            return super.openRW(name, opts);
        }

        @Override
        public int rename(LPSZ from, LPSZ to) {
            return afterMove(super.rename(from, to), from, to);
        }

        @Override
        public int softLink(LPSZ src, LPSZ softLink) {
            final int result = super.softLink(src, softLink);
            if (result == 0 && killAfterMove) {
                takeKillImage(tableDirOf(softLink));
            }
            return result;
        }
    };
    private static final long NO_PERIODIC_SAVE_POINT_MS = 3_600_000L;
    private static final String PARTITION = "2024-01-02";
    private static final String THREE_PARTITIONS = "'2024-01-01', '2024-01-02', '2024-01-03'";
    // Every save point writes a manifest; counting its opens counts the save points.
    private static final AtomicInteger savePoints = new AtomicInteger();
    // Links out of a partition directory fail as across devices, so the operation copies instead.
    private static volatile boolean crossDevice;
    // The number of save points after which a save point fails with an I/O error; -1 never fails.
    private static volatile int failSavePointsAfter = -1;
    // The process is killed right after the operation moves a partition directory.
    private static volatile boolean killAfterMove;
    // What a process kill leaves of the table directory; the next restart puts it back.
    private static java.nio.file.Path killImageRoot;
    private static Map<java.nio.file.Path, byte[]> killImageFiles;
    private static Map<java.nio.file.Path, java.nio.file.Path> killImageLinks;
    private static List<java.nio.file.Path> killImageDirs;
    private final History history;
    private final Operation operation;
    private final OperationPath path;
    // The table as the operation found it.
    private String beforeOperation;

    public AdaptivePartitionDdlCrashTest(History history, Operation operation, OperationPath path) {
        this.history = history;
        this.operation = operation;
        this.path = path;
    }

    @Parameterized.Parameters(name = "{0}, {1}, {2}")
    public static Collection<Object[]> data() {
        final List<Object[]> params = new ArrayList<>();
        for (History history : History.values()) {
            for (Operation operation : Operation.values()) {
                if (history.killed()) {
                    if (operation.movesDirectory()) {
                        params.add(new Object[]{history, operation, OperationPath.WAL});
                        params.add(new Object[]{history, operation, OperationPath.SUSPENDED});
                    }
                    continue;
                }
                switch (operation) {
                    // TRUNCATE is not forced through on a suspended table.
                    case TRUNCATE -> params.add(new Object[]{history, operation, OperationPath.WAL});
                    // These change the table outside the WAL by themselves.
                    case COPY_INTO_EXISTING_TABLE, RESUME_WAL_FROM_TXN ->
                            params.add(new Object[]{history, operation, OperationPath.OWN});
                    // The statement-level save point after a part-way failure, through the WAL and directly.
                    case CONVERT_LIST_SECOND_FAILS, DETACH_LIST_SECOND_FAILS -> {
                        params.add(new Object[]{history, operation, OperationPath.WAL});
                        params.add(new Object[]{history, operation, OperationPath.SUSPENDED});
                    }
                    case ATTACH_CROSS_DEVICE, ATTACH_SOFT_LINK, DETACH_AFTER_O3, DETACH_CROSS_DEVICE -> {
                        params.add(new Object[]{history, operation, OperationPath.WAL});
                        params.add(new Object[]{history, operation, OperationPath.SUSPENDED});
                    }
                    // The second engine of ENROLLED_ON_WRITER_OPEN cannot create or drop tables.
                    case COPY_INTO_NEW_TABLE -> {
                        if (history != History.ENROLLED_ON_WRITER_OPEN) {
                            params.add(new Object[]{history, operation, OperationPath.OWN});
                        }
                    }
                    default -> {
                        params.add(new Object[]{history, operation, OperationPath.WAL});
                        params.add(new Object[]{history, operation, OperationPath.SUSPENDED});
                        params.add(new Object[]{history, operation, OperationPath.SUSPENDED_BUSY_WRITER});
                        params.add(new Object[]{history, operation, OperationPath.SUSPENDED_WRITER_FREES_UP});
                    }
                }
            }
        }
        return params;
    }

    @Test
    public void testCrashAfterOperation() throws Exception {
        final boolean startsNosync = history == History.NOSYNC || history == History.ENROLLED_ON_WRITER_OPEN;
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, startsNosync ? "nosync" : "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, history.periodicOff() ? -1 : NO_PERIODIC_SAVE_POINT_MS);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, history.cleanRestart() ? "true" : "false");
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, "true");
        killImageRoot = null;
        inputRoot = temp.newFolder().getAbsolutePath();
        inputWorkRoot = temp.newFolder().getAbsolutePath();
        java.nio.file.Files.writeString(java.nio.file.Path.of(inputRoot, "t.csv"), CSV, StandardCharsets.UTF_8);
        assertMemoryLeak(FF, () -> {
            CairoEngine e = engine;
            try {
                e.execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                insertDay(e, "2024-01-01", 0);
                insertDay(e, PARTITION, 10);
                insertDay(e, "2024-01-03", 20);
                prepare(e);
                switch (history) {
                    case ENROLLED_ON_WRITER_OPEN -> {
                        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
                        e = restart(e);
                    }
                    case PERIODIC_SAVE_POINT -> takePeriodicSavePoint(e);
                    default -> {
                    }
                }
                insertDay(e, "2024-01-04", 30);
                if (history.cleanRestart()) {
                    // A direct operation does not move the seqTxn, so only the save point it owes makes the
                    // close take one.
                    takeSavePoint(e);
                }
                e = apply(e);
                if (operation == Operation.RESUME_WAL_FROM_TXN) {
                    Assert.assertFalse("the transaction must be skipped", snapshot(e).contains("skipped"));
                }
                if (!history.cleanRestart() && !history.killed()) {
                    insertDay(e, "2024-01-05", 40);
                }
                if (history.killed()) {
                    Assert.assertNotNull("the kill must happen", killImageRoot);
                }
                // A kill undoes a change made outside the WAL, which nothing can redo; it must not lose the partition.
                final String expected = history.killed() && path != OperationPath.WAL ? beforeOperation : snapshot(e);

                if (history == History.SAVE_POINT_FAILS_CLEAN_RESTART_INTO_NOSYNC) {
                    setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
                }
                e = restart(e);
                if (e.getTableSequencerAPI().isSuspended(e.verifyTableName("t"))) {
                    e.execute("ALTER TABLE t RESUME WAL");
                }
                TestUtils.drainWalQueue(e);
                Assert.assertEquals(expected, snapshot(e));
            } finally {
                if (e != engine) {
                    e.close();
                }
            }
        });
    }

    private static int afterMove(int result, LPSZ src, LPSZ dst) {
        if (result == 0 && killAfterMove && (Utf8s.endsWithAscii(src, TableUtils.ATTACHABLE_DIR_MARKER)
                || (Utf8s.containsAscii(dst, DETACHED_NAME) && !Utf8s.containsAscii(src, DETACHED_NAME)))) {
            takeKillImage(tableDirOf(dst));
        }
        return result;
    }

    private static void alter(CairoEngine e, String sql) throws SqlException {
        e.execute(sql);
        TestUtils.drainWalQueue(e);
        Assert.assertFalse(sql, e.getTableSequencerAPI().isSuspended(e.verifyTableName("t")));
    }

    private static void assertResumeFails(CairoEngine e, String resume) throws SqlException {
        try {
            e.execute(resume);
            Assert.fail("the statement must fail");
        } catch (CairoException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "the table stays suspended");
        }
        TestUtils.drainWalQueue(e);
        Assert.assertTrue("the table must stay suspended", e.getTableSequencerAPI().isSuspended(e.verifyTableName("t")));
        Assert.assertFalse("the transaction must be skipped", snapshot(e).contains("skipped"));
    }

    private static void insertDay(CairoEngine e, String day, int base) throws SqlException {
        final StringBuilder sql = new StringBuilder("INSERT INTO t VALUES ");
        for (int i = 1; i <= 3; i++) {
            sql.append(i > 1 ? ", " : "")
                    .append("('").append(day).append("T0").append(i - 1).append(":00:00.000000Z', 's")
                    .append(base + i).append("', ").append(base + i).append(')');
        }
        alter(e, sql.toString());
    }

    // An .attachable next to an attached partition of the same name is a leftover the next save point deletes.
    private static String partitions(CairoEngine e) throws SqlException {
        final StringSink sink = new StringSink();
        e.print(
                "SELECT name, minTimestamp, maxTimestamp, numRows, attached, detached, attachable, isParquet FROM table_partitions('t')",
                sink
        );
        final String suffix = e.getConfiguration().getAttachPartitionSuffix();
        final String[] rows = sink.toString().split("\n");
        final StringBuilder partitions = new StringBuilder();
        for (String row : rows) {
            final String[] fields = row.split("\t");
            final boolean leftover = fields.length > 6 && fields[6].equals("true") && fields[0].endsWith(suffix)
                    && Stream.of(rows).anyMatch(r -> r.startsWith(fields[0].substring(0, fields[0].length() - suffix.length()) + '\t')
                    && r.split("\t")[4].equals("true"));
            if (!leftover) {
                partitions.append(row).append('\n');
            }
        }
        return partitions.toString();
    }

    // Startup in a fresh engine. Pooled writers close without a save point unless the history restarts cleanly. After
    // a kill the table directory is put back as the kill left it.
    private static CairoEngine restart(CairoEngine e) {
        e.releaseAllReaders();
        e.releaseAllWriters();
        e.releaseInactive();
        if (e != engine) {
            e.close();
        }
        if (killImageRoot != null) {
            restoreKillImage();
        }
        return new CairoEngine(configuration);
    }

    private static void restoreKillImage() {
        try {
            try (Stream<java.nio.file.Path> paths = java.nio.file.Files.walk(killImageRoot)) {
                for (java.nio.file.Path p : paths.sorted(Comparator.reverseOrder()).toList()) {
                    if (!p.equals(killImageRoot)) {
                        java.nio.file.Files.delete(p);
                    }
                }
            }
            for (java.nio.file.Path dir : killImageDirs) {
                java.nio.file.Files.createDirectories(dir);
            }
            for (Map.Entry<java.nio.file.Path, java.nio.file.Path> link : killImageLinks.entrySet()) {
                java.nio.file.Files.createSymbolicLink(link.getKey(), link.getValue());
            }
            for (Map.Entry<java.nio.file.Path, byte[]> file : killImageFiles.entrySet()) {
                java.nio.file.Files.write(file.getKey(), file.getValue());
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
        killImageRoot = null;
    }

    private static String snapshot(CairoEngine e) throws SqlException {
        final StringSink sink = new StringSink();
        final StringBuilder snapshot = new StringBuilder();
        e.print("SELECT * FROM t", sink);
        snapshot.append(sink);
        snapshot.append(partitions(e));
        e.print("SELECT suspended, writerTxn, sequencerTxn FROM wal_tables() WHERE name = 't'", sink);
        snapshot.append(sink);
        return snapshot.toString();
    }

    // The names of the directories of the test partition.
    private static String supersededDirNames(CairoEngine e) throws IOException {
        final java.nio.file.Path tableDir = java.nio.file.Path.of(e.getConfiguration().getDbRoot(), e.verifyTableName("t").getDirName());
        try (Stream<java.nio.file.Path> dirs = java.nio.file.Files.list(tableDir)) {
            return dirs.map(d -> d.getFileName().toString()).filter(n -> n.matches(PARTITION + "(\\.\\d+)?")).sorted().toList().toString();
        }
    }

    // Partition directories on disk beyond one per attached partition, not counting detached and attachable ones.
    private static long supersededPartitionDirs(CairoEngine e) throws Exception {
        final StringSink sink = new StringSink();
        e.print("SELECT count() FROM table_partitions('t') WHERE attached", sink);
        final java.nio.file.Path tableDir = java.nio.file.Path.of(e.getConfiguration().getDbRoot(), e.verifyTableName("t").getDirName());
        try (Stream<java.nio.file.Path> dirs = java.nio.file.Files.list(tableDir)) {
            return dirs.filter(d -> d.getFileName().toString().matches("\\d{4}-\\d{2}-\\d{2}(\\.\\d+)?")).count()
                    - Long.parseLong(sink.toString().split("\n")[1]);
        }
    }

    private static java.nio.file.Path tableDirOf(LPSZ inTableDir) {
        final StringBuilder name = new StringBuilder();
        for (int i = 0, n = inTableDir.size(); i < n; i++) {
            name.append((char) inTableDir.byteAt(i));
        }
        return java.nio.file.Path.of(name.toString()).getParent();
    }

    // The bytes of every file and the tree of the table directory, as a process kill at this moment leaves them.
    private static void takeKillImage(java.nio.file.Path tableDir) {
        killAfterMove = false;
        killImageRoot = tableDir;
        killImageFiles = new HashMap<>();
        killImageLinks = new HashMap<>();
        killImageDirs = new ArrayList<>();
        try (Stream<java.nio.file.Path> paths = java.nio.file.Files.walk(killImageRoot)) {
            for (java.nio.file.Path p : paths.toList()) {
                if (java.nio.file.Files.isSymbolicLink(p)) {
                    killImageLinks.put(p, java.nio.file.Files.readSymbolicLink(p));
                } else if (java.nio.file.Files.isDirectory(p)) {
                    killImageDirs.add(p);
                } else {
                    killImageFiles.put(p, java.nio.file.Files.readAllBytes(p));
                }
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static void takePeriodicSavePoint(CairoEngine e) throws SqlException {
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        alter(e, "INSERT INTO t VALUES ('2024-01-03T12:00:00.000000Z', 's99', 99)");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, NO_PERIODIC_SAVE_POINT_MS);
        final SeqTxnTracker tracker = e.getTableSequencerAPI().getTxnTracker(e.verifyTableName("t"));
        Assert.assertEquals("the apply job must take a save point", tracker.getSeqTxn(), tracker.getDurableEpochSeqTxn());
    }

    // A save point at the current seqTxn, as the apply job takes after a batch.
    private static void takeSavePoint(CairoEngine e) {
        final TableToken token = e.verifyTableName("t");
        try (TableWriter writer = e.getWriter(token, "test")) {
            writer.advanceDurableEpoch(e.getConfiguration().getMicrosecondClock().getTicks() / 1000L);
        }
        final SeqTxnTracker tracker = e.getTableSequencerAPI().getTxnTracker(token);
        Assert.assertEquals(tracker.getSeqTxn(), tracker.getDurableEpochSeqTxn());
    }

    private CairoEngine apply(CairoEngine e) throws Exception {
        final String before = partitions(e);
        switch (operation) {
            case COPY_INTO_EXISTING_TABLE -> {
                alter(e, "TRUNCATE TABLE t");
                importCsv(e);
            }
            case COPY_INTO_NEW_TABLE -> {
                e.execute("DROP TABLE t");
                TestUtils.drainWalQueue(e);
                importCsv(e);
            }
            case RESUME_WAL_FROM_TXN -> {
                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "false");
                e.execute("ALTER TABLE t SUSPEND WAL");
                e.execute("INSERT INTO t VALUES ('2024-01-06T00:00:00.000000Z', 'skipped', -1)");
                TestUtils.drainWalQueue(e);
                final String resume = "ALTER TABLE t RESUME WAL FROM TXN " + (e.getTableSequencerAPI().lastTxn(e.verifyTableName("t")) + 1);
                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
                startOperation();
                try {
                    if (history.failsSavePoint()) {
                        // The skip is not durable, so the statement fails and the table stays suspended, and so does
                        // a plain RESUME WAL while the save point still fails.
                        assertResumeFails(e, resume);
                        assertResumeFails(e, "ALTER TABLE t RESUME WAL");
                        // Nothing is left to skip, but a repeat, or a plain RESUME WAL, takes the save point the skip
                        // owes, so a crash right after it keeps the skip.
                        failSavePointsAfter = -1;
                        alter(e, history.periodicOff() ? "ALTER TABLE t RESUME WAL" : resume);
                        return crash(e);
                    }
                    alter(e, resume);
                } finally {
                    endOperation();
                }
                return e;
            }
            default -> applySql(e, sql(e));
        }
        Assert.assertNotEquals("the operation must change the partitions", before, partitions(e));
        if (operation == Operation.ATTACH_LIST_SECOND_FAILS) {
            final String rows = snapshot(e);
            Assert.assertTrue("the first partition must attach", rows.contains("\ts11\t"));
            Assert.assertFalse("the second partition must not attach", rows.contains("\ts1\t"));
        }
        return e;
    }

    private void applySql(CairoEngine e, String sql) throws Exception {
        final long supersededBefore = supersededPartitionDirs(e);
        beforeOperation = snapshot(e);
        if (path == OperationPath.WAL) {
            startOperation();
            try {
                if (operation == Operation.CONVERT_LIST_SECOND_FAILS) {
                    // A convert that fails in the apply job suspends the table.
                    e.execute(sql);
                    TestUtils.drainWalQueue(e);
                    Assert.assertTrue("the table must be suspended", e.getTableSequencerAPI().isSuspended(e.verifyTableName("t")));
                } else {
                    // A failing attach or detach fails in the apply job, which skips it.
                    alter(e, sql);
                }
                killAfterFailedSavePoint(e);
                assertSavePointsOfOneStatement();
                assertSupersededDirsRemoved(e, supersededBefore);
            } finally {
                endOperation();
            }
            if (history == History.SAVE_POINT_FAILS_THEN_KILLED) {
                // Without the kill, the next save point would finish what the failed one left.
                takeSavePoint(e);
            }
            if (operation == Operation.CONVERT_LIST_SECOND_FAILS) {
                alter(e, "ALTER TABLE t RESUME WAL");
            }
            return;
        }
        // A hard-suspended table that denies WAL writes takes the change directly in its writer, outside the WAL.
        e.execute("ALTER TABLE t SUSPEND WAL");
        startOperation();
        try {
            switch (path) {
                case SUSPENDED -> e.execute(sql);
                case SUSPENDED_BUSY_WRITER -> {
                    // The writer is busy, so the change is queued and runs when the holder ticks the writer.
                    try (
                            TableWriter writer = e.getWriter(e.verifyTableName("t"), "test");
                            SqlCompiler compiler = e.getSqlCompiler();
                            SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(e);
                            OperationFuture future = compiler.compile(sql, ctx).execute(new SCSequence())
                    ) {
                        writer.tick(true);
                        future.await();
                    }
                }
                case SUSPENDED_WRITER_FREES_UP -> {
                    // The writer is busy at dispatch and free before the change is queued, so the statement
                    // applies it in the writer itself.
                    final TableWriter[] holder = {e.getWriter(e.verifyTableName("t"), "test")};
                    OperationDispatcher.setAsyncEnqueueObserver(() -> holder[0] = Misc.free(holder[0]));
                    try (
                            SqlCompiler compiler = e.getSqlCompiler();
                            SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(e);
                            OperationFuture future = compiler.compile(sql, ctx).execute(new SCSequence())
                    ) {
                        future.await();
                    } finally {
                        OperationDispatcher.setAsyncEnqueueObserver(null);
                        Misc.free(holder[0]);
                    }
                }
                default -> throw new AssertionError(path);
            }
            killAfterFailedSavePoint(e);
            Assert.assertFalse("the statement must fail", operation.failsPartWay());
        } catch (CairoException | SqlException ex) {
            if (!operation.failsPartWay()) {
                throw ex;
            }
        } finally {
            endOperation();
        }
        assertSavePointsOfOneStatement();
        assertSupersededDirsRemoved(e, supersededBefore);
        alter(e, "ALTER TABLE t RESUME WAL");
    }

    // One statement takes one save point at most, however many partitions it changes, and none outside ADAPTIVE.
    private void assertSavePointsOfOneStatement() {
        if (!history.failsSavePoint()) {
            Assert.assertTrue("save points of one statement: " + savePoints.get(), savePoints.get() <= (history == History.NOSYNC ? 0 : 1));
        }
    }

    // DETACH and CONVERT remove the partition directories they supersede before the statement returns, after
    // its save point.
    private void assertSupersededDirsRemoved(CairoEngine e, long supersededBefore) throws Exception {
        switch (operation) {
            case CONVERT_LIST_SECOND_FAILS, CONVERT_TO_NATIVE, CONVERT_TO_PARQUET, DETACH, DETACH_LIST_SECOND_FAILS -> {
                if (!history.failsSavePoint()) {
                    Assert.assertTrue("superseded partition directories left behind", supersededPartitionDirs(e) <= supersededBefore);
                }
            }
            default -> {
            }
        }
    }

    // A process kill in the middle of an operation: the writers close without a save point.
    private CairoEngine crash(CairoEngine e) {
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        final CairoEngine restarted = restart(e);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, history.cleanRestart() ? "true" : "false");
        TestUtils.drainWalQueue(restarted);
        return restarted;
    }

    private void endOperation() {
        failSavePointsAfter = -1;
        crossDevice = false;
        killAfterMove = false;
    }

    private void importCsv(CairoEngine e) throws Exception {
        startOperation();
        try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(e, 1)) {
            importer.of("t", "t.csv", 1, PartitionBy.DAY, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
            importer.process(AllowAllSecurityContext.INSTANCE);
        } finally {
            endOperation();
        }
        // One save point per COPY statement, not one per imported partition. A table COPY creates has no
        // save point until the next restart: the importer's writer holds a non-WAL token, so it does not enrol it.
        final int expected = history == History.NOSYNC || operation == Operation.COPY_INTO_NEW_TABLE ? 0 : 1;
        Assert.assertEquals("save points during the COPY", expected, savePoints.get());
    }

    private void killAfterFailedSavePoint(CairoEngine e) {
        if (history == History.SAVE_POINT_FAILS_THEN_KILLED) {
            takeKillImage(java.nio.file.Path.of(e.getConfiguration().getDbRoot(), e.verifyTableName("t").getDirName()));
        }
    }

    private void prepare(CairoEngine e) throws SqlException {
        switch (operation) {
            case ATTACH_COPIED_DIR, ATTACH_CROSS_DEVICE, ATTACH_LIST_SECOND_FAILS, ATTACH_MOVED_DIR, ATTACH_SOFT_LINK,
                 ATTACH_WITH_COPY_SETTING -> {
                alter(e, "ALTER TABLE t DETACH PARTITION LIST '" + PARTITION + "'");
                final FilesFacade ff = e.getConfiguration().getFilesFacade();
                final TableToken token = e.verifyTableName("t");
                try (Path detached = new Path(); Path attachable = new Path()) {
                    detached.of(e.getConfiguration().getDbRoot()).concat(token).concat(PARTITION + TableUtils.DETACHED_DIR_MARKER);
                    attachable.of(e.getConfiguration().getDbRoot()).concat(token)
                            .concat(PARTITION + e.getConfiguration().getAttachPartitionSuffix());
                    if (operation == Operation.ATTACH_COPIED_DIR || operation == Operation.ATTACH_CROSS_DEVICE) {
                        Assert.assertEquals(0, ff.copyRecursive(detached, attachable, e.getConfiguration().getMkDirMode()));
                    } else if (operation == Operation.ATTACH_SOFT_LINK) {
                        try (Path target = new Path()) {
                            target.of(inputWorkRoot).concat(PARTITION);
                            Assert.assertEquals(0, ff.copyRecursive(detached, target, e.getConfiguration().getMkDirMode()));
                            Assert.assertEquals(0, ff.softLink(target.$(), attachable.$()));
                        }
                    } else {
                        Assert.assertEquals(Files.FILES_RENAME_OK, ff.rename(detached.$(), attachable.$()));
                    }
                }
                if (operation == Operation.ATTACH_WITH_COPY_SETTING) {
                    setProperty(PropertyKey.CAIRO_ATTACH_PARTITION_COPY, "true");
                }
                if (operation == Operation.ATTACH_LIST_SECOND_FAILS) {
                    // Detached, not attachable.
                    alter(e, "ALTER TABLE t DETACH PARTITION LIST '2024-01-01'");
                }
            }
            case CONVERT_TO_NATIVE -> alter(e, "ALTER TABLE t CONVERT PARTITION TO PARQUET LIST " + THREE_PARTITIONS);
            default -> {
            }
        }
    }

    private String sql(CairoEngine e) throws Exception {
        return switch (operation) {
            case ATTACH_COPIED_DIR, ATTACH_CROSS_DEVICE, ATTACH_MOVED_DIR, ATTACH_SOFT_LINK, ATTACH_WITH_COPY_SETTING ->
                    "ALTER TABLE t ATTACH PARTITION LIST '" + PARTITION + "'";
            case ATTACH_LIST_SECOND_FAILS -> "ALTER TABLE t ATTACH PARTITION LIST '" + PARTITION + "', '2024-01-01'";
            case CONVERT_LIST_SECOND_FAILS ->
                    "ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-09'";
            // The WHERE form picks its partitions when the WAL applies it; applied directly to a WAL table it
            // matches none.
            case CONVERT_TO_NATIVE -> path == OperationPath.WAL
                    ? "ALTER TABLE t CONVERT PARTITION TO NATIVE WHERE ts < '2024-01-04'"
                    : "ALTER TABLE t CONVERT PARTITION TO NATIVE LIST " + THREE_PARTITIONS;
            case CONVERT_TO_PARQUET -> "ALTER TABLE t CONVERT PARTITION TO PARQUET LIST " + THREE_PARTITIONS;
            case DETACH, DETACH_CROSS_DEVICE -> "ALTER TABLE t DETACH PARTITION LIST " + THREE_PARTITIONS;
            case DETACH_AFTER_O3 -> {
                final String before = supersededDirNames(e);
                alter(e, "INSERT INTO t VALUES ('" + PARTITION + "T00:30:00.000000Z', 's97', 97)");
                Assert.assertNotEquals("the partition must be rewritten", before, supersededDirNames(e));
                yield "ALTER TABLE t DETACH PARTITION LIST '" + PARTITION + "'";
            }
            case DETACH_LIST_SECOND_FAILS -> "ALTER TABLE t DETACH PARTITION LIST '2024-01-01', '2024-01-09'";
            case DROP -> "ALTER TABLE t DROP PARTITION LIST '" + PARTITION + "'";
            case FORCE_DROP -> "ALTER TABLE t FORCE DROP PARTITION LIST '" + PARTITION + "'";
            case SQUASH -> {
                // An out-of-order row splits the last partition, so there is something to squash.
                setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
                setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 4);
                final String unsplit = partitions(e);
                alter(e, "INSERT INTO t VALUES ('2024-01-04T01:30:00.000000Z', 's98', 98)");
                Assert.assertNotEquals("the last partition must be split", unsplit.split("\n").length, partitions(e).split("\n").length);
                yield "ALTER TABLE t SQUASH PARTITIONS";
            }
            case TRUNCATE -> "TRUNCATE TABLE t";
            case TTL -> "ALTER TABLE t SET TTL 2 DAYS";
            default -> throw new AssertionError(operation);
        };
    }

    // The save points of the operation are counted from here; in the histories where they fail, they fail.
    private void startOperation() {
        savePoints.set(0);
        if (history.failsSavePoint()) {
            failSavePointsAfter = 0;
        }
        crossDevice = operation == Operation.ATTACH_CROSS_DEVICE || operation == Operation.DETACH_CROSS_DEVICE;
        killAfterMove = history == History.KILLED_BEFORE_TXN;
    }

    public enum History {
        // Created on an adaptive instance: the creation save point is the last one.
        ENROLLED_AT_CREATE,
        // Written under NOSYNC, then restarted in ADAPTIVE: the writer enrols the table when it opens.
        ENROLLED_ON_WRITER_OPEN,
        // A periodic save point, then more transactions.
        PERIODIC_SAVE_POINT,
        // The periodic save point is off, so only the inline save points of DDL happen.
        INLINE_SAVE_POINTS_ONLY,
        // The save point after the operation fails with an I/O error; the next apply batch must take one.
        SAVE_POINT_FAILS,
        // As SAVE_POINT_FAILS with the periodic save point off: the next apply batch must still take one.
        SAVE_POINT_FAILS_NO_PERIODIC,
        // The save point after the operation fails, nothing is written after it, and the restart is clean: the
        // writer's close must take the save point, also when the operation did not move the seqTxn.
        SAVE_POINT_FAILS_CLEAN_RESTART,
        // As SAVE_POINT_FAILS_CLEAN_RESTART with the periodic save point off.
        SAVE_POINT_FAILS_CLEAN_RESTART_NO_PERIODIC,
        // As SAVE_POINT_FAILS_CLEAN_RESTART_NO_PERIODIC, restarting in NOSYNC.
        SAVE_POINT_FAILS_CLEAN_RESTART_INTO_NOSYNC,
        // The control: nothing is rewound after a crash.
        NOSYNC,
        // The process is killed right after the operation moves a partition directory, before its _txn commit.
        KILLED_BEFORE_TXN,
        // The save point after the operation fails, and the process is killed right after the statement.
        SAVE_POINT_FAILS_THEN_KILLED;

        boolean cleanRestart() {
            return switch (this) {
                case SAVE_POINT_FAILS_CLEAN_RESTART, SAVE_POINT_FAILS_CLEAN_RESTART_NO_PERIODIC,
                     SAVE_POINT_FAILS_CLEAN_RESTART_INTO_NOSYNC -> true;
                default -> false;
            };
        }

        boolean failsSavePoint() {
            return this == SAVE_POINT_FAILS || this == SAVE_POINT_FAILS_NO_PERIODIC || this == SAVE_POINT_FAILS_THEN_KILLED || cleanRestart();
        }

        boolean killed() {
            return this == KILLED_BEFORE_TXN || this == SAVE_POINT_FAILS_THEN_KILLED;
        }

        boolean periodicOff() {
            return switch (this) {
                case INLINE_SAVE_POINTS_ONLY, SAVE_POINT_FAILS_NO_PERIODIC, SAVE_POINT_FAILS_CLEAN_RESTART_NO_PERIODIC,
                     SAVE_POINT_FAILS_CLEAN_RESTART_INTO_NOSYNC -> true;
                default -> false;
            };
        }
    }

    public enum Operation {
        // The .attachable directory is a copy of .detached, renamed into place by the attach.
        ATTACH_COPIED_DIR,
        // As ATTACH_COPIED_DIR, with the partition directory on another device, so the attach cannot link.
        ATTACH_CROSS_DEVICE,
        // ATTACH of two partitions whose second is not attachable: the statement fails after attaching the first.
        ATTACH_LIST_SECOND_FAILS,
        // The .attachable directory is the moved .detached, so the attach consumes the only copy.
        ATTACH_MOVED_DIR,
        // The .attachable is a soft link to a copy of .detached outside the table directory.
        ATTACH_SOFT_LINK,
        // cairo.attach.partition.copy=true: the attach copies and leaves .attachable in place.
        ATTACH_WITH_COPY_SETTING,
        // CONVERT of two partitions whose second does not exist: the statement fails after converting the first.
        CONVERT_LIST_SECOND_FAILS,
        CONVERT_TO_NATIVE,
        CONVERT_TO_PARQUET,
        // COPY into an empty WAL table, which attaches each imported partition outside the WAL.
        COPY_INTO_EXISTING_TABLE,
        // COPY into a table the importer creates.
        COPY_INTO_NEW_TABLE,
        DETACH,
        // An out-of-order row rewrites the partition into a new version after the last save point, then DETACH.
        DETACH_AFTER_O3,
        // As DETACH, with the detached directory on another device, so the detach copies.
        DETACH_CROSS_DEVICE,
        // DETACH of two partitions whose second does not exist: the statement fails after detaching the first.
        DETACH_LIST_SECOND_FAILS,
        DROP,
        FORCE_DROP,
        // Skips a transaction by moving the writer's seqTxn past it, outside the WAL.
        RESUME_WAL_FROM_TXN,
        SQUASH,
        TRUNCATE,
        TTL;

        // The statement fails after it changed the table.
        boolean failsPartWay() {
            return this == ATTACH_LIST_SECOND_FAILS || this == CONVERT_LIST_SECOND_FAILS || this == DETACH_LIST_SECOND_FAILS;
        }

        // The operation moves a partition directory to or from a name the WAL replay needs to find as it was.
        boolean movesDirectory() {
            return switch (this) {
                case ATTACH_COPIED_DIR, ATTACH_CROSS_DEVICE, ATTACH_MOVED_DIR, ATTACH_SOFT_LINK -> true;
                default -> false;
            };
        }
    }

    public enum OperationPath {
        // The operation changes the table outside the WAL by itself.
        OWN,
        // Directly in the writer of a hard-suspended table.
        SUSPENDED,
        // As SUSPENDED, queued behind a writer another thread holds.
        SUSPENDED_BUSY_WRITER,
        // As SUSPENDED, with the writer busy at dispatch and free by the time the change would be queued.
        SUSPENDED_WRITER_FREES_UP,
        WAL
    }
}
