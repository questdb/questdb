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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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
    private static final FilesFacade FF = new TestFilesFacadeImpl() {
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
    };
    private static final long NO_PERIODIC_SAVE_POINT_MS = 3_600_000L;
    private static final String PARTITION = "2024-01-02";
    // Every save point writes a manifest; counting its opens counts the save points.
    private static final AtomicInteger savePoints = new AtomicInteger();
    // The number of save points after which a save point fails with an I/O error; -1 never fails.
    private static volatile int failSavePointsAfter = -1;
    private final History history;
    private final Operation operation;
    private final OperationPath path;

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
                switch (operation) {
                    // TRUNCATE is not forced through on a suspended table.
                    case TRUNCATE -> params.add(new Object[]{history, operation, OperationPath.WAL});
                    // These change the table outside the WAL by themselves.
                    case COPY_INTO_EXISTING_TABLE, RESUME_WAL_FROM_TXN ->
                            params.add(new Object[]{history, operation, OperationPath.OWN});
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
                apply(e);
                if (!history.cleanRestart()) {
                    insertDay(e, "2024-01-05", 40);
                }
                final String expected = snapshot(e);

                e = restart(e);
                TestUtils.drainWalQueue(e);
                Assert.assertEquals(expected, snapshot(e));
            } finally {
                if (e != engine) {
                    e.close();
                }
            }
        });
    }

    private static void alter(CairoEngine e, String sql) throws SqlException {
        e.execute(sql);
        TestUtils.drainWalQueue(e);
        Assert.assertFalse(sql, e.getTableSequencerAPI().isSuspended(e.verifyTableName("t")));
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

    private static String partitions(CairoEngine e) throws SqlException {
        final StringSink sink = new StringSink();
        e.print(
                "SELECT name, minTimestamp, maxTimestamp, numRows, attached, detached, attachable, isParquet FROM table_partitions('t')",
                sink
        );
        return sink.toString();
    }

    // Startup in a fresh engine. Pooled writers close without a save point unless the history restarts cleanly.
    private static CairoEngine restart(CairoEngine e) {
        e.releaseAllReaders();
        e.releaseAllWriters();
        e.releaseInactive();
        if (e != engine) {
            e.close();
        }
        return new CairoEngine(configuration);
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

    private void apply(CairoEngine e) throws Exception {
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
                final long skippedTxn = e.getTableSequencerAPI().lastTxn(e.verifyTableName("t"));
                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
                startOperation();
                try {
                    alter(e, "ALTER TABLE t RESUME WAL FROM TXN " + (skippedTxn + 1));
                } finally {
                    failSavePointsAfter = -1;
                }
                Assert.assertFalse("the transaction must be skipped", snapshot(e).contains("skipped"));
                return;
            }
            default -> applySql(e, sql(e));
        }
        Assert.assertNotEquals("the operation must change the partitions", before, partitions(e));
        if (operation == Operation.ATTACH_LIST_SECOND_FAILS) {
            final String rows = snapshot(e);
            Assert.assertTrue("the first partition must attach", rows.contains("\ts11\t"));
            Assert.assertFalse("the second partition must not attach", rows.contains("\ts1\t"));
        }
    }

    private void applySql(CairoEngine e, String sql) throws Exception {
        if (path == OperationPath.WAL) {
            startOperation();
            try {
                // A failing statement fails in the apply job, which skips it.
                alter(e, sql);
            } finally {
                failSavePointsAfter = -1;
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
            Assert.assertNotEquals("the statement must fail", Operation.ATTACH_LIST_SECOND_FAILS, operation);
        } catch (CairoException | SqlException ex) {
            if (operation != Operation.ATTACH_LIST_SECOND_FAILS) {
                throw ex;
            }
        } finally {
            failSavePointsAfter = -1;
        }
        alter(e, "ALTER TABLE t RESUME WAL");
    }

    private void importCsv(CairoEngine e) throws Exception {
        startOperation();
        try (ParallelCsvFileImporter importer = new ParallelCsvFileImporter(e, 1)) {
            importer.of("t", "t.csv", 1, PartitionBy.DAY, (byte) ',', "ts", "yyyy-MM-ddTHH:mm:ss.SSSSSSZ", true);
            importer.process(AllowAllSecurityContext.INSTANCE);
        } finally {
            failSavePointsAfter = -1;
        }
        // One save point per COPY statement, not one per imported partition. A table COPY creates has no
        // save point until the next restart: the importer's writer holds a non-WAL token, so it does not enrol it.
        final int expected = history == History.NOSYNC || operation == Operation.COPY_INTO_NEW_TABLE ? 0 : 1;
        Assert.assertEquals("save points during the COPY", expected, savePoints.get());
    }

    private void prepare(CairoEngine e) throws SqlException {
        switch (operation) {
            case ATTACH_COPIED_DIR, ATTACH_LIST_SECOND_FAILS, ATTACH_MOVED_DIR, ATTACH_WITH_COPY_SETTING -> {
                alter(e, "ALTER TABLE t DETACH PARTITION LIST '" + PARTITION + "'");
                final FilesFacade ff = e.getConfiguration().getFilesFacade();
                final TableToken token = e.verifyTableName("t");
                try (Path detached = new Path(); Path attachable = new Path()) {
                    detached.of(e.getConfiguration().getDbRoot()).concat(token).concat(PARTITION + TableUtils.DETACHED_DIR_MARKER);
                    attachable.of(e.getConfiguration().getDbRoot()).concat(token)
                            .concat(PARTITION + e.getConfiguration().getAttachPartitionSuffix());
                    if (operation == Operation.ATTACH_COPIED_DIR) {
                        Assert.assertEquals(0, ff.copyRecursive(detached, attachable, e.getConfiguration().getMkDirMode()));
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
            case CONVERT_TO_NATIVE -> alter(e, "ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '" + PARTITION + "'");
            default -> {
            }
        }
    }

    private String sql(CairoEngine e) throws SqlException {
        return switch (operation) {
            case ATTACH_COPIED_DIR, ATTACH_MOVED_DIR, ATTACH_WITH_COPY_SETTING ->
                    "ALTER TABLE t ATTACH PARTITION LIST '" + PARTITION + "'";
            case ATTACH_LIST_SECOND_FAILS -> "ALTER TABLE t ATTACH PARTITION LIST '" + PARTITION + "', '2024-01-01'";
            case CONVERT_TO_NATIVE -> "ALTER TABLE t CONVERT PARTITION TO NATIVE LIST '" + PARTITION + "'";
            case CONVERT_TO_PARQUET -> "ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '" + PARTITION + "'";
            case DETACH -> "ALTER TABLE t DETACH PARTITION LIST '" + PARTITION + "'";
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
        // The control: nothing is rewound after a crash.
        NOSYNC;

        boolean cleanRestart() {
            return this == SAVE_POINT_FAILS_CLEAN_RESTART;
        }

        boolean failsSavePoint() {
            return this == SAVE_POINT_FAILS || this == SAVE_POINT_FAILS_NO_PERIODIC || this == SAVE_POINT_FAILS_CLEAN_RESTART;
        }

        boolean periodicOff() {
            return this == INLINE_SAVE_POINTS_ONLY || this == SAVE_POINT_FAILS_NO_PERIODIC;
        }
    }

    public enum Operation {
        // The .attachable directory is a copy of .detached, renamed into place by the attach.
        ATTACH_COPIED_DIR,
        // ATTACH of two partitions whose second is not attachable: the statement fails after attaching the first.
        ATTACH_LIST_SECOND_FAILS,
        // The .attachable directory is the moved .detached, so the attach consumes the only copy.
        ATTACH_MOVED_DIR,
        // cairo.attach.partition.copy=true: the attach copies and leaves .attachable in place.
        ATTACH_WITH_COPY_SETTING,
        CONVERT_TO_NATIVE,
        CONVERT_TO_PARQUET,
        // COPY into an empty WAL table, which attaches each imported partition outside the WAL.
        COPY_INTO_EXISTING_TABLE,
        // COPY into a table the importer creates.
        COPY_INTO_NEW_TABLE,
        DETACH,
        DROP,
        FORCE_DROP,
        // Skips a transaction by moving the writer's seqTxn past it, outside the WAL.
        RESUME_WAL_FROM_TXN,
        SQUASH,
        TRUNCATE,
        TTL
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
