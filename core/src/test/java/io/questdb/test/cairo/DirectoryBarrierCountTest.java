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
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Pins how many directory fsyncs each commit mode pays on the ingest and apply paths, per partition switch,
 * O3 commit into the last partition, WAL writer open, WAL segment roll and WAL column DDL that rolls pending
 * rows.
 * <p>
 * A directory fsync makes names durable, and a mode takes one only where it promises that what the name
 * points at survives a power loss:
 * <ul>
 *   <li>{@code TableWriter.openPartition} fsyncs the partition directory and the table directory under SYNC
 *   only. ADAPTIVE relies on its durable epoch and WAL replay, ASYNC and NOSYNC promise nothing.</li>
 *   <li>The WAL writer fsyncs a new segment's entry in {@code wal<N>} under SYNC and ADAPTIVE, and the table
 *   directory once per writer, when it has created {@code wal<N>}. ASYNC keeps only the segment directory
 *   fsync of a newly opened segment, as it always had.</li>
 * </ul>
 * The crash tests that prove these are enough are in {@code DirectoryBarrierCrashTest}.
 */
@RunWith(Parameterized.class)
public class DirectoryBarrierCountTest extends AbstractCairoTest {
    private final String commitMode;
    private DirFsyncCountingFacade ff;

    public DirectoryBarrierCountTest(String commitMode) {
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
        // Windows cannot open a directory for fsync, so it takes none of these barriers in any mode.
        Assume.assumeFalse(Os.isWindows());
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        // No durable epoch inside a measured window: an epoch fsyncs the table directory itself.
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000);
        ff = new DirFsyncCountingFacade();
    }

    @Test
    public void testNonWalO3IntoLastPartition() throws Exception {
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO x VALUES
                        ('2024-01-01T00:00:00.000000Z', 0),
                        ('2024-01-02T00:00:00.000000Z', 0),
                        ('2024-01-02T12:00:00.000000Z', 0)
                    """);
            final String tableDir = tableDir("x");
            ff.clear();
            for (int i = 1; i <= 5; i++) {
                // Each commit writes a new version of the last partition, which the writer then reopens.
                execute("INSERT INTO x VALUES ('2024-01-02T0" + i + ":00:00.000000Z', " + i + ")");
            }
            assertTableSide(5, tableDir);
        });
    }

    @Test
    public void testNonWalOneCommitAcrossPartitions() throws Exception {
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL");
            final String tableDir = tableDir("x");
            ff.clear();
            execute("""
                    INSERT INTO x
                    SELECT timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L), x
                    FROM long_sequence(48)
                    """);
            assertTableSide(48, tableDir);
        });
    }

    @Test
    public void testNonWalPartitionSwitch() throws Exception {
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 0)");
            final String tableDir = tableDir("x");
            ff.clear();
            for (int i = 1; i <= 5; i++) {
                execute("INSERT INTO x VALUES ('2024-01-0" + (i + 1) + "T00:00:00.000000Z', " + i + ")");
            }
            assertTableSide(5, tableDir);

            // Appending to the open partition creates nothing, so no mode fsyncs a directory for it.
            ff.clear();
            for (int i = 1; i <= 5; i++) {
                execute("INSERT INTO x VALUES ('2024-01-06T0" + i + ":00:00.000000Z', " + i + ")");
            }
            assertTableSide(0, tableDir);
        });
    }

    @Test
    public void testWalApplyPartitionSwitch() throws Exception {
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 0)");
            drainWalQueue();
            final String tableDir = tableDir("x");
            ff.clear();
            for (int i = 1; i <= 5; i++) {
                execute("INSERT INTO x VALUES ('2024-01-0" + (i + 1) + "T00:00:00.000000Z', " + i + ")");
                drainWalQueue();
            }
            // Apply creates each new partition through O3 and then reopens it as the last partition.
            assertTableSide(5, tableDir);
            assertQuery("SELECT count() FROM x").noRandomAccess().expectSize().returns("count\n6\n");
        });
    }

    @Test
    public void testWalColumnDdlRollsPendingRows() throws Exception {
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (WalWriter writer = getWalWriter("x")) {
                final String walDir = tableDir("x") + "/" + writer.getWalName();
                appendRow(writer, 0, 1);
                writer.commit();
                appendRow(writer, 1, 2);
                execute("ALTER TABLE x ADD COLUMN c INT");
                ff.clear();
                // NO_TXN: the writer replays the ALTER, rolls the pending row into a new segment, then adds
                // the column's files to that segment.
                writer.commit();
                Assert.assertEquals("the pending row must have rolled to a new segment", 1, writer.getSegmentId());
                final boolean isDurable = isSyncOrAdaptive();
                Assert.assertEquals("wal directory fsyncs", isDurable ? 1 : 0, ff.count(walDir));
                Assert.assertEquals("segment directory fsyncs", isDurable ? 2 : 0, ff.count(walDir + "/1"));
                Assert.assertEquals("fsyncs of an empty directory", 0, ff.countEmpty());
            }
        });
    }

    @Test
    public void testWalWriterOpenAndSegmentRoll() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 3);
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String tableDir = tableDir("x");
            ff.clear();
            try (WalWriter writer = getWalWriter("x")) {
                final String walDir = tableDir + "/" + writer.getWalName();
                final boolean isDurable = isSyncOrAdaptive();
                final int segmentDirFsyncs = "nosync".equals(commitMode) ? 0 : 1;
                Assert.assertEquals("open: table directory fsyncs", isDurable ? 1 : 0, ff.count(tableDir));
                Assert.assertEquals("open: wal directory fsyncs", isDurable ? 1 : 0, ff.count(walDir));
                Assert.assertEquals("open: segment directory fsyncs", segmentDirFsyncs, ff.count(walDir + "/0"));

                ff.clear();
                long v = 0;
                for (int commit = 0; commit < 4; commit++) {
                    for (int row = 0; row < 3; row++) {
                        appendRow(writer, v * 1_000_000L, v++);
                    }
                    writer.commit();
                }
                Assert.assertEquals("four commits of three rows must roll three times", 3, writer.getSegmentId());
                Assert.assertEquals("rolls: table directory fsyncs", 0, ff.count(tableDir));
                Assert.assertEquals("rolls: wal directory fsyncs", isDurable ? 3 : 0, ff.count(walDir));
                for (int segment = 1; segment <= 3; segment++) {
                    Assert.assertEquals("roll: segment directory fsyncs", segmentDirFsyncs, ff.count(walDir + "/" + segment));
                }
                Assert.assertEquals("rolls: all directory fsyncs", (isDurable ? 3 : 0) + 3 * segmentDirFsyncs, ff.total());
                Assert.assertEquals("fsyncs of an empty directory", 0, ff.countEmpty());
            }
        });
    }

    private static void appendRow(WalWriter writer, long ts, long v) {
        final TableWriter.Row row = writer.newRow(ts);
        row.putLong(1, v);
        row.append();
    }

    /**
     * Asserts the partition-directory and table-directory fsyncs of the measured window: one of each per
     * partition that {@code openPartition} opened under SYNC, none in any other mode.
     */
    private void assertTableSide(int syncCount, String tableDir) {
        final int expected = "sync".equals(commitMode) ? syncCount : 0;
        Assert.assertEquals("partition directory fsyncs" + ff.dump(), expected, ff.countChildren(tableDir));
        Assert.assertEquals("table directory fsyncs" + ff.dump(), expected, ff.count(tableDir));
        Assert.assertEquals("all directory fsyncs" + ff.dump(), 2 * expected, ff.total());
        // A partition directory fsynced before its column files exist persists an empty directory.
        Assert.assertEquals("fsyncs of an empty directory", 0, ff.countEmpty());
    }

    private boolean isSyncOrAdaptive() {
        return "sync".equals(commitMode) || "adaptive".equals(commitMode);
    }

    private String tableDir(String tableName) {
        return Paths.get(root, engine.verifyTableName(tableName).getDirName()).toString();
    }

    /**
     * Counts fsyncs of directories by path. A directory fsync opens the directory with
     * {@code openRONoCache}, so that is where the facade learns which descriptors are directories.
     */
    private static class DirFsyncCountingFacade extends TestFilesFacadeImpl {
        private final Map<String, Integer> counts = new HashMap<>();
        private final Map<Long, String> dirFds = new HashMap<>();
        private int emptyCount;

        @Override
        public synchronized boolean close(long fd) {
            dirFds.remove(fd);
            return super.close(fd);
        }

        @Override
        public void fsync(long fd) {
            record(fd, false);
            super.fsync(fd);
        }

        @Override
        public void fsyncAndClose(long fd) {
            record(fd, true);
            super.fsyncAndClose(fd);
        }

        @Override
        public long openRONoCache(LPSZ name) {
            final long fd = super.openRONoCache(name);
            if (fd > -1) {
                final String path = toPathString(name);
                if (Files.isDirectory(Paths.get(path))) {
                    synchronized (this) {
                        dirFds.put(fd, path);
                    }
                }
            }
            return fd;
        }

        private static String toPathString(LPSZ name) {
            final StringBuilder sb = new StringBuilder(name.size());
            for (int i = 0, n = name.size(); i < n; i++) {
                sb.append((char) (name.byteAt(i) & 0xFF));
            }
            int len = sb.length();
            while (len > 1 && sb.charAt(len - 1) == '/') {
                len--;
            }
            return Paths.get(sb.substring(0, len)).toString();
        }

        synchronized void clear() {
            counts.clear();
            emptyCount = 0;
        }

        synchronized int count(String dir) {
            return counts.getOrDefault(dir, 0);
        }

        /**
         * fsyncs of the directories directly inside {@code dir} whose name starts with a digit: partitions,
         * as opposed to {@code wal<N>}, {@code txn_seq} and the like.
         */
        synchronized int countChildren(String dir) {
            int total = 0;
            for (Map.Entry<String, Integer> e : counts.entrySet()) {
                final String path = e.getKey();
                if (path.startsWith(dir + "/")) {
                    final String name = path.substring(dir.length() + 1);
                    if (name.indexOf('/') < 0 && Character.isDigit(name.charAt(0))) {
                        total += e.getValue();
                    }
                }
            }
            return total;
        }

        synchronized int countEmpty() {
            return emptyCount;
        }

        synchronized String dump() {
            return " " + counts;
        }

        synchronized int total() {
            int total = 0;
            for (int c : counts.values()) {
                total += c;
            }
            return total;
        }

        private synchronized void record(long fd, boolean isClosing) {
            final String dir = isClosing ? dirFds.remove(fd) : dirFds.get(fd);
            if (dir != null) {
                counts.merge(dir, 1, Integer::sum);
                try (Stream<Path> entries = Files.list(Paths.get(dir))) {
                    if (entries.findAny().isEmpty()) {
                        emptyCount++;
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }
}
