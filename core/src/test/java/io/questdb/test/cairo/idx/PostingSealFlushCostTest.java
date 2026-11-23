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
 * reseal of older partitions by a WAL commit, the seal at a partition switch, the seal a fast WAL commit makes at the
 * generation cap of the last partition, and REINDEX.
 * <p>
 * The expected counts come from what each mode promises, not from the code: NOSYNC and ASYNC promise nothing on a
 * power loss, so a seal waits for no disk. ADAPTIVE recovers to its last save point and drops every chain entry
 * stamped above it, so a seal of a commit in progress needs no flush either; a seal stamped with the committed txn
 * (the generation cap, REINDEX) is kept by recovery and takes the ordered flush. SYNC takes the ordered flush for
 * every seal. The ordered flush is two disk waits: one device flush for every new value and cover file, then the
 * {@code .pk} flush.
 * <p>
 * The facade reports a filesystem-wide {@code syncfs} on every platform, so the counts are the same everywhere.
 */
@RunWith(Parameterized.class)
public class PostingSealFlushCostTest extends AbstractCairoTest {
    private static final long DAY = 86_400_000_000L;
    private static final int INCLUDE_COLUMNS = 3;
    private static final Pattern NEW_VALUE_FILE = Pattern.compile(".*/k\\.pv\\.[0-9]+$");
    // Flushes by the kind of file they flush, made inside a PostingIndexWriter seal, rollback re-encode, truncate or
    // dense rebuild into a new value file.
    private static final Map<String, Integer> SEAL_FLUSHES = new TreeMap<>();
    private static final Set<String> SEAL_METHODS = Set.of(
            "seal", "rebuildSidecarsByCopy", "rollbackToMaxValue", "truncate", "discardForRebuild", "commitDense"
    );
    private static final String WRITER_CLASS = PostingIndexWriter.class.getName();
    private static int newValueFiles;
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

    // ALTER TABLE ADD INDEX builds and seals the index of every partition; the build commits at the next txn.
    @Test
    public void testIndexBuild() throws Exception {
        assertMemoryLeak(new CountingFacade(), () -> {
            createTable(false, false);
            insertAndApply(rowsOverDays(0, 20, 4));
            resetCounts();
            execute("ALTER TABLE t ALTER COLUMN k ADD INDEX TYPE POSTING" + includeClause());
            drainWalQueue();
            assertSealFlushes(true, "ADD INDEX");
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
            assertSealFlushes(true, "WAL commit into older partitions");
            assertIndexMatchesScan();
        });
    }

    // The last partition seals its generations when the writer moves to the next one, as part of the commit that does.
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
            assertSealFlushes(true, "commit into the next partition");
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

    private static boolean isSealFrame(StackWalker.StackFrame frame) {
        return WRITER_CLASS.equals(frame.getClassName()) && SEAL_METHODS.contains(frame.getMethodName());
    }

    private static String kindOf(String path) {
        if (path == null) {
            return "unknown";
        }
        // k.pv.1 -> pv, k.pc3.0.0.1 -> pcN, k.pci.0 -> pci, k.pk -> pk
        final String name = path.substring(path.lastIndexOf('/') + 1);
        final String[] parts = name.split("\\.");
        return parts.length > 1 ? parts[1].replaceAll("^pc[0-9]+$", "pcN") : name;
    }

    private static boolean isInSeal() {
        return StackWalker.getInstance().walk(s -> s.anyMatch(PostingSealFlushCostTest::isSealFrame));
    }

    private static void recordSealFlush(String kind) {
        if (isInSeal()) {
            synchronized (SEAL_FLUSHES) {
                SEAL_FLUSHES.merge(kind, 1, Integer::sum);
            }
        }
    }

    private static void resetCounts() {
        synchronized (SEAL_FLUSHES) {
            SEAL_FLUSHES.clear();
            newValueFiles = 0;
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

    private void assertIndexMatchesScan() throws Exception {
        for (int k = 0; k < 4; k++) {
            assertSqlCursors(
                    "SELECT ts, k, c0, c1, c2 FROM t WHERE k::STRING = 'k" + k + "'",
                    "SELECT ts, k, c0, c1, c2 FROM t WHERE k = 'k" + k + "'"
            );
        }
    }

    private void assertSealFlushes(boolean isInProgress, String operation) {
        Assert.assertEquals(CommitMode.fromString(commitMode), engine.getConfiguration().getCommitMode());
        final int seals;
        final Map<String, Integer> flushes;
        synchronized (SEAL_FLUSHES) {
            seals = newValueFiles;
            flushes = new TreeMap<>(SEAL_FLUSHES);
        }
        Assert.assertTrue(operation + " made no seal", seals > 0);
        final boolean isFlushed = switch (commitMode) {
            case "sync" -> true;
            case "adaptive" -> !isInProgress;
            default -> false;
        };
        // The ordered flush is two disk waits: the new files, then the .pk.
        int total = 0;
        for (int n : flushes.values()) {
            total += n;
        }
        Assert.assertEquals(
                "flushes of " + seals + " seals under " + commitMode + ", " + operation + ": " + flushes,
                isFlushed ? 2 * seals : 0,
                total
        );
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

    private static class CountingFacade extends TestFilesFacadeImpl {
        @Override
        public void barrierFsync(long fd) {
            super.barrierFsync(fd);
            recordSealFlush(kindOf(Files.findPathOfFd(fd)));
        }

        @Override
        public void fdatasync(long fd) {
            super.fdatasync(fd);
            recordSealFlush(kindOf(Files.findPathOfFd(fd)));
        }

        @Override
        public void fsync(long fd) {
            super.fsync(fd);
            recordSealFlush(kindOf(Files.findPathOfFd(fd)));
        }

        @Override
        public void fsyncDurable(long fd) {
            super.fsyncDurable(fd);
            recordSealFlush(kindOf(Files.findPathOfFd(fd)));
        }

        @Override
        public boolean isSyncfsFileSystemWide() {
            return true;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            super.msync(addr, len, async);
            if (!async) {
                recordSealFlush(kindOf(Files.findPathOfMapping(addr)));
            }
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            if (NEW_VALUE_FILE.matcher(name.asAsciiCharSequence()).matches() && !exists(name) && isInSeal()) {
                synchronized (SEAL_FLUSHES) {
                    newValueFiles++;
                }
            }
            return super.openRW(name, opts);
        }

        @Override
        public void syncfs(long fd) {
            super.syncfs(fd);
            recordSealFlush("syncfs");
        }
    }
}
