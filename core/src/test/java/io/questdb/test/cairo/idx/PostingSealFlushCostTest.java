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

package io.questdb.test.cairo.idx;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.std.Files;
import io.questdb.std.str.LPSZ;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Counts the flushes a POSTING index seal makes, per commit mode, for every kind of seal: the ADD INDEX build, the
 * build of a column type change to an indexed SYMBOL, the reseal of older partitions by a WAL commit, the seal at a
 * partition switch, the seal a fast WAL commit makes at the generation cap of the last partition, and REINDEX.
 * <p>
 * The expected counts come from what each mode promises, not from the code. ADAPTIVE recovers a WAL table to its last
 * save point and drops every chain entry stamped above it, so a seal of a commit in progress needs no flush; a seal
 * stamped with the committed txn (the generation cap, REINDEX), every seal of an index build, whose files no recovery
 * drops, and every seal of a table that takes no save point (a non-WAL table, a replica), takes the ordered flush.
 * Every other mode takes the ordered flush for every seal: NOSYNC and ASYNC may lose commits on a power loss, but must
 * not leave an index that names files the purge of the superseded generation deleted, or files that never reached the
 * disk. The ordered flush takes one flush of every new value and cover file, then the {@code .pk} flush: no more than
 * the flush of each file a seal took before it followed the commit mode, and no wait for other files' dirty data.
 * <p>
 * A rebuild of the index from the column data into a new value file, as a WAL commit into older partitions makes, is
 * not a seal: it flushes as before the seals took the ordered flush. Under SYNC the new value file reaches the disk
 * before the {@code .pk} entry that names it, then the {@code .pk} does, one file at a time; the other modes make no
 * synchronous flush for it.
 * <p>
 * The facade takes the Linux flush path on every platform, so the counts are the same everywhere.
 */
@RunWith(Parameterized.class)
public class PostingSealFlushCostTest extends AbstractCairoTest {
    private static final long DAY = 86_400_000_000L;
    private static final int INCLUDE_COLUMNS = 3;
    // k.pv.<sealTxn>, or k.pv.<columnNameTxn>.<sealTxn> for a column that has a name txn
    private static final Pattern NEW_VALUE_FILE = Pattern.compile(".*[/\\\\]k\\.pv\\.([0-9]+\\.)?[0-9]+$");
    // The kinds of the files a rebuild flushed, in order.
    private static final StringBuilder REBUILD_FLUSHES = new StringBuilder();
    // The PostingIndexWriter methods that rebuild the index into a new value file, discardForRebuild() then
    // commitDense(), when no seal method runs inside them.
    private static final Set<String> REBUILD_METHODS = Set.of("discardForRebuild", "commitDense");
    // Flushes by the kind of file they flush, made inside a PostingIndexWriter seal, rollback re-encode or truncate.
    private static final Map<String, Integer> SEAL_FLUSHES = new TreeMap<>();
    private static final Set<String> SEAL_METHODS = Set.of("seal", "rebuildSidecarsByCopy", "rollbackToMaxValue", "truncate");
    private static final String WRITER_CLASS = PostingIndexWriter.class.getName();
    // Set by the test: the first file open inside a WAL apply promotes the replica, see CountingFacade.
    private static volatile boolean isPromotionArmed;
    private static volatile boolean isPromoted;
    private static int rebuilds;
    private static int seals;
    private final String commitMode;
    private final boolean isCovering;

    public PostingSealFlushCostTest(String commitMode, boolean isCovering) {
        this.commitMode = commitMode;
        this.isCovering = isCovering;
    }

    @Parameterized.Parameters(name = "{0} INCLUDE={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"nosync", false}, {"nosync", true},
                {"async", false}, {"async", true},
                {"adaptive", false}, {"adaptive", true},
                {"sync", false}, {"sync", true}
        });
    }

    @Override
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        // No save point inside a measured operation: its flushes are not the seal's.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1_000_000_000L);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, Long.MAX_VALUE);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        // The production default: an in-place commit rewrites the partition and reseals its index.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
    }

    // A fast WAL commit into the last partition seals it inline once it reaches the generation cap. The seal is stamped
    // with the committed txn.
    @Test
    public void testAppendSealAtGenerationCap() throws Exception {
        assertMemoryLeak(new CountingFacade(), () -> {
            createTable(true, false);
            insertAndApply("(0::TIMESTAMP, 'k0', 0, 0, 0)");
            resetCounts();
            for (int i = 1; i <= 140; i++) {
                insertAndApply("(" + i + "_000_000::TIMESTAMP, 'k" + (i % 3) + "', " + i + ", " + i + ", " + i + ")");
            }
            assertSealFlushes(false, "WAL commits up to the generation cap of the last partition");
            assertIndexMatchesScan();
        });
    }

    // ALTER TABLE ADD INDEX builds and seals the index of every partition. The seals take the ordered flush in every
    // mode: after a power loss the restored _meta does not index the column, so recovery cannot drop a build seal, and
    // the replayed ADD INDEX accepts the files it finds.
    @Test
    public void testIndexBuild() throws Exception {
        assertMemoryLeak(new CountingFacade(), () -> {
            createTable(false, false);
            insertAndApply(rowsOverDays(0, 20, 4));
            resetCounts();
            execute("ALTER TABLE t ALTER COLUMN k ADD INDEX TYPE POSTING" + includeClause());
            drainWalQueue();
            assertSealFlushes(false, "ADD INDEX");
            assertIndexMatchesScan();
        });
    }

    // A column type change to an indexed SYMBOL builds and seals the index of the new column in every partition. As with
    // ADD INDEX, the seals take the ordered flush in every mode: after a power loss the restored _meta does not index
    // the column, recovery leaves its files alone, and the replayed change reuses the column name txn and accepts the
    // files it finds.
    @Test
    public void testIndexBuildOfTypeChange() throws Exception {
        Assume.assumeFalse("a column type change takes no INCLUDE columns", isCovering);
        assertMemoryLeak(new CountingFacade(), () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k VARCHAR, c0 LONG, c1 LONG, c2 LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            insertAndApply(rowsOverDays(0, 20, 4));
            resetCounts();
            execute("ALTER TABLE t ALTER COLUMN k TYPE SYMBOL INDEX TYPE POSTING");
            drainWalQueue();
            assertSealFlushes(false, "ALTER COLUMN TYPE");
            assertIndexMatchesScan();
        });
    }

    // A WAL commit with rows in older partitions rewrites them and reseals their index at the commit's txn.
    @Test
    public void testOlderPartitionReseal() throws Exception {
        assertMemoryLeak(new CountingFacade(), () -> {
            createTable(true, true);
            insertAndApply(rowsOverDays(0, 20, 4));
            resetCounts();
            // Between the committed rows, so the commit rewrites each partition and rebuilds its index.
            insertAndApply(rowsOverDays(20, 20, 4, -500_000L));
            assertRebuildAndSealFlushes(true, "WAL commit into older partitions");
            assertIndexMatchesScan();
        });
    }

    // A replica promoted in the middle of an apply batch commits under ADAPTIVE at once, but its table has no save point
    // and no pin until the batch-end reconcile: nothing would keep the files a seal superseded, so the seals of the
    // batch take the ordered flush.
    @Test
    public void testOlderPartitionResealAfterPromotionMidBatch() throws Exception {
        assertMemoryLeak(new CountingFacade(), () -> {
            createTable(true, true);
            // one generation per commit, up to the seal threshold, in each of 3 days
            for (int d = 0; d < 3; d++) {
                for (int i = 1; i <= 16; i++) {
                    final int v = d * 16 + i;
                    insertAndApply("(" + (d * DAY + i * 1_000_000L) + "::TIMESTAMP, 'k" + (v % 4) + "', " + v + ", " + v + ", " + v + ")");
                }
            }
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            try {
                // the replica batch's reconcile removes the save point and its pin
                insertAndApply("(" + (2 * DAY + 100_000_000L) + "::TIMESTAMP, 'k1', 49, 49, 49)");
                resetCounts();
                isPromotionArmed = true;
                // one WAL transaction appending after the last row of each day: the older days take it out of order
                final StringBuilder rows = new StringBuilder();
                for (int d = 0; d < 3; d++) {
                    for (int i = 0; i < 4; i++) {
                        final int v = 50 + d * 4 + i;
                        rows.append(rows.length() == 0 ? "" : ", ").append('(').append(d * DAY + 3_600_000_000L + i * 1_000_000L)
                                .append("::TIMESTAMP, 'k").append(v % 4).append("', ").append(v).append(", ").append(v)
                                .append(", ").append(v).append(')');
                    }
                }
                insertAndApply(rows.toString());
                Assert.assertTrue("the apply opened no file", isPromoted);
                Assert.assertTrue(engine.getLocalDurabilityPolicy().isLocalDurabilityEnabled());
                assertSealFlushes(false, "WAL commit into older partitions after a promotion in the batch");
                assertIndexMatchesScan();
            } finally {
                isPromotionArmed = false;
                isPromoted = false;
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            }
        });
    }

    // A replica takes no save point, so nothing would make the files of a seal of a commit in progress durable, and no
    // pin would keep the files it superseded: the seal takes the ordered flush, as one that recovery keeps.
    @Test
    public void testOlderPartitionResealOnReplica() throws Exception {
        assertMemoryLeak(new CountingFacade(), () -> {
            createTable(true, true);
            insertAndApply(rowsOverDays(0, 20, 4));
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            try {
                resetCounts();
                insertAndApply(rowsOverDays(20, 20, 4, -500_000L));
                assertRebuildAndSealFlushes(false, "WAL commit into older partitions on a replica");
                assertIndexMatchesScan();
            } finally {
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            }
        });
    }

    // The last partition seals its generations when the writer moves to the next one, as part of the commit that does.
    // A non-WAL table takes no save point, so the seal flushes in every mode.
    @Test
    public void testPartitionSwitchSeal() throws Exception {
        assertMemoryLeak(new CountingFacade(), () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k SYMBOL INDEX TYPE POSTING" + includeClause()
                    + ", c0 LONG, c1 LONG, c2 LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            for (int i = 0; i <= 40; i++) {
                execute("INSERT INTO t VALUES (" + i + "_000_000::TIMESTAMP, 'k" + (i % 3) + "', " + i + ", " + i + ", " + i + ")");
            }
            resetCounts();
            execute("INSERT INTO t VALUES (" + DAY + "::TIMESTAMP, 'k1', 41, 41, 41)");
            // a non-WAL table takes no save point
            assertSealFlushes(false, "commit into the next partition of a non-WAL table");
            assertIndexMatchesScan();
        });
    }

    // A WAL commit into the next partition seals the covering index of the last one.
    @Test
    public void testPartitionSwitchSealWal() throws Exception {
        Assume.assumeTrue("only a covering index seals at a WAL partition switch", isCovering);
        assertMemoryLeak(new CountingFacade(), () -> {
            createTable(true, false);
            for (int i = 0; i <= 40; i++) {
                insertAndApply("(" + i + "_000_000::TIMESTAMP, 'k" + (i % 3) + "', " + i + ", " + i + ", " + i + ")");
            }
            resetCounts();
            insertAndApply("(" + DAY + "::TIMESTAMP, 'k1', 41, 41, 41)");
            assertSealFlushes(true, "WAL commit into the next partition");
            assertIndexMatchesScan();
        });
    }

    // REINDEX rebuilds the index of the committed rows outside any commit.
    @Test
    public void testReindex() throws Exception {
        assertMemoryLeak(new CountingFacade(), () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k SYMBOL INDEX TYPE POSTING" + includeClause()
                    + ", c0 LONG, c1 LONG, c2 LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES " + rowsOverDays(0, 20, 4));
            engine.releaseAllWriters();
            resetCounts();
            execute("REINDEX TABLE t COLUMN k LOCK EXCLUSIVE");
            assertSealFlushes(false, "REINDEX");
            assertIndexMatchesScan();
        });
    }

    private static String kindOf(String path) {
        if (path == null) {
            return "unknown";
        }
        // k.pv.1 -> pv, k.pc3.0.0.1 -> pcN, k.pci.0 -> pci, k.pk -> pk
        final String name = path.substring(Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\')) + 1);
        final String[] parts = name.split("\\.");
        return parts.length > 1 ? parts[1].replaceAll("^pc[0-9]+$", "pcN") : name;
    }

    // The operation of the innermost PostingIndexWriter seal or rebuild method on the stack: a seal() that a rebuild
    // runs is a seal.
    private static Operation operationOnStack() {
        return StackWalker.getInstance().walk(s -> s
                .filter(frame -> WRITER_CLASS.equals(frame.getClassName()))
                .map(frame -> SEAL_METHODS.contains(frame.getMethodName()) ? Operation.SEAL
                        : REBUILD_METHODS.contains(frame.getMethodName()) ? Operation.REBUILD : Operation.NONE)
                .filter(operation -> operation != Operation.NONE)
                .findFirst()
                .orElse(Operation.NONE));
    }

    // Promotes the replica at the first file open inside a WAL apply, after the batch-start reconcile, and counts from
    // there.
    private static void maybePromote() {
        if (isPromotionArmed && !isPromoted && StackWalker.getInstance().walk(s -> s.anyMatch(frame ->
                ApplyWal2TableJob.class.getName().equals(frame.getClassName()) && "processWalCommit".equals(frame.getMethodName())))) {
            isPromoted = true;
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            resetCounts();
        }
    }

    private static void recordFlush(String kind) {
        final Operation operation = operationOnStack();
        synchronized (SEAL_FLUSHES) {
            if (operation == Operation.SEAL) {
                SEAL_FLUSHES.merge(kind, 1, Integer::sum);
            } else if (operation == Operation.REBUILD) {
                REBUILD_FLUSHES.append(kind).append(' ');
            }
        }
    }

    private static void recordNewValueFile() {
        final Operation operation = operationOnStack();
        synchronized (SEAL_FLUSHES) {
            if (operation == Operation.SEAL) {
                seals++;
            } else if (operation == Operation.REBUILD) {
                rebuilds++;
            }
        }
    }

    private static void resetCounts() {
        synchronized (SEAL_FLUSHES) {
            SEAL_FLUSHES.clear();
            REBUILD_FLUSHES.setLength(0);
            rebuilds = 0;
            seals = 0;
        }
    }

    // count rows of 4 symbols over `days` day partitions, from value `from`, in time order within each day
    private static String rowsOverDays(int from, int count, int days) {
        return rowsOverDays(from, count, days, 0);
    }

    // As above, the i-th row at i + 1 seconds into its day plus shiftMicros.
    private static String rowsOverDays(int from, int count, int days, long shiftMicros) {
        final StringBuilder sql = new StringBuilder();
        for (int i = 0; i < count; i++) {
            final int v = from + i;
            final long ts = (v % days) * DAY + (i + 1) * 1_000_000L + shiftMicros;
            sql.append(i == 0 ? "" : ", ").append('(').append(ts).append("::TIMESTAMP, 'k").append(v % 4).append("', ")
                    .append(v).append(", ").append(v).append(", ").append(v).append(')');
        }
        return sql.toString();
    }

    // isSavePointCovered: the seal is of a commit in progress on a table that takes save points, so under ADAPTIVE the
    // next save point makes it durable and it takes no flush
    private void assertFlushes(boolean isSavePointCovered, String operation) {
        Assert.assertEquals(CommitMode.fromString(commitMode), engine.getConfiguration().getCommitMode());
        final int rebuildCount;
        final String rebuildFlushes;
        final int sealCount;
        final Map<String, Integer> flushes;
        synchronized (SEAL_FLUSHES) {
            rebuildCount = rebuilds;
            rebuildFlushes = REBUILD_FLUSHES.toString();
            sealCount = seals;
            flushes = new TreeMap<>(SEAL_FLUSHES);
        }
        // Under SYNC a rebuild flushes its value file, then the .pk; no other mode flushes a rebuild synchronously.
        Assert.assertEquals(
                "flushes of " + rebuildCount + " rebuilds under " + commitMode + ", " + operation,
                "sync".equals(commitMode) ? "pv pk ".repeat(rebuildCount) : "",
                rebuildFlushes
        );
        final boolean isFlushed = !"adaptive".equals(commitMode) || !isSavePointCovered;
        // The ordered flush: each new file once, then the .pk.
        final Map<String, Integer> expected = new TreeMap<>();
        if (isFlushed && sealCount > 0) {
            expected.put("pk", sealCount);
            expected.put("pv", sealCount);
            if (isCovering) {
                // the INCLUDE columns and the designated timestamp the index includes by default
                expected.put("pcN", (INCLUDE_COLUMNS + 1) * sealCount);
                expected.put("pci", sealCount);
            }
        }
        Assert.assertEquals(
                "flushes of " + sealCount + " seals under " + commitMode + ", " + operation,
                expected,
                flushes
        );
    }

    private void assertIndexMatchesScan() throws Exception {
        for (int k = 0; k < 4; k++) {
            assertSqlCursors(
                    "SELECT ts, k, c0, c1, c2 FROM t WHERE k::STRING = 'k" + k + "'",
                    "SELECT ts, k, c0, c1, c2 FROM t WHERE k = 'k" + k + "'"
            );
        }
    }

    // The operation rebuilds the index of every partition it rewrites; a covering index then seals the rebuilt
    // generation with its INCLUDE values.
    private void assertRebuildAndSealFlushes(boolean isSavePointCovered, String operation) {
        final int rebuildCount;
        final int sealCount;
        synchronized (SEAL_FLUSHES) {
            rebuildCount = rebuilds;
            sealCount = seals;
        }
        Assert.assertTrue(operation + " made no rebuild", rebuildCount > 0);
        Assert.assertEquals(operation + " seals after the rebuild", isCovering, sealCount > 0);
        assertFlushes(isSavePointCovered, operation);
    }

    private void assertSealFlushes(boolean isSavePointCovered, String operation) {
        synchronized (SEAL_FLUSHES) {
            Assert.assertTrue(operation + " made no seal", seals > 0);
        }
        assertFlushes(isSavePointCovered, operation);
    }

    private void createTable(boolean isIndexed, boolean isDedup) throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, k SYMBOL" + (isIndexed ? " INDEX TYPE POSTING" + includeClause() : "")
                + ", c0 LONG, c1 LONG, c2 LONG) TIMESTAMP(ts) PARTITION BY DAY WAL"
                + (isDedup ? " DEDUP UPSERT KEYS(ts, k)" : ""));
    }

    private String includeClause() {
        if (!isCovering) {
            return "";
        }
        final StringBuilder sql = new StringBuilder(" INCLUDE (");
        for (int i = 0; i < INCLUDE_COLUMNS; i++) {
            sql.append(i == 0 ? "" : ", ").append('c').append(i);
        }
        return sql.append(')').toString();
    }

    private void insertAndApply(String values) throws Exception {
        execute("INSERT INTO t VALUES " + values);
        drainWalQueue();
    }

    private enum Operation {
        NONE, REBUILD, SEAL
    }

    private static class CountingFacade extends TestFilesFacadeImpl {
        @Override
        public void barrierFsync(long fd) {
            super.barrierFsync(fd);
            recordFlush(kindOf(Files.findPathOfFd(fd)));
        }

        @Override
        public void fdatasync(long fd) {
            super.fdatasync(fd);
            recordFlush(kindOf(Files.findPathOfFd(fd)));
        }

        @Override
        public void fsync(long fd) {
            super.fsync(fd);
            recordFlush(kindOf(Files.findPathOfFd(fd)));
        }

        @Override
        public void fsyncDurable(long fd) {
            super.fsyncDurable(fd);
            recordFlush(kindOf(Files.findPathOfFd(fd)));
        }

        @Override
        public boolean isSyncfsFileSystemWide() {
            return true;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            super.msync(addr, len, async);
            if (!async) {
                recordFlush(kindOf(Files.findPathOfMapping(addr)));
            }
        }

        @Override
        public long openRO(LPSZ name) {
            maybePromote();
            return super.openRO(name);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            maybePromote();
            if (NEW_VALUE_FILE.matcher(name.asAsciiCharSequence()).matches() && !exists(name)) {
                recordNewValueFile();
            }
            return super.openRW(name, opts);
        }

        @Override
        public void syncfs(long fd) {
            super.syncfs(fd);
            recordFlush("syncfs");
        }
    }
}
