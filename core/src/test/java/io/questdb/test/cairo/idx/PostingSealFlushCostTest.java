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

package io.questdb.test.cairo.idx;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.std.Files;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * Counts the msync calls on POSTING index files ({@code .pv}, {@code .pcN}, {@code .pci}, {@code .pk}) while
 * seals run, per commit mode. A seal flushes the files it wrote only under a mode that promises to survive a power
 * loss: SYNC and ADAPTIVE wait for each file, NOSYNC and ASYNC leave them to the OS, as their commits do.
 */
@RunWith(Parameterized.class)
public class PostingSealFlushCostTest extends AbstractCairoTest {
    private static final long DAY = 86_400_000_000L;
    private static final int DAYS = 5;
    // msync calls on index files by kind: pv, pcN, pci, pk, with " async" for an asynchronous one
    private static final Map<String, Integer> MSYNCS = new TreeMap<>();
    private final String commitMode;

    public PostingSealFlushCostTest(String commitMode) {
        this.commitMode = commitMode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{"nosync"}, {"async"}, {"sync"}, {"adaptive"}});
    }

    @Override
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        // No save point inside a measured operation: it flushes the open indexers itself.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1_000_000_000L);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, Long.MAX_VALUE);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
    }

    // ALTER TABLE ADD INDEX builds and seals the index of every partition.
    @Test
    public void testAddIndex() throws Exception {
        assertMemoryLeak(new CountingFacade(), () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k SYMBOL, c0 LONG, c1 LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            insertAndApply(rows(0));
            clearMsyncs();
            execute("ALTER TABLE t ALTER COLUMN k ADD INDEX TYPE POSTING INCLUDE (c0, c1)");
            drainWalQueue();
            assertSealFlushes(DAYS);
            assertIndexMatchesScan();
        });
    }

    // The commit that leaves a partition for the next one seals the generations the earlier commits appended to it.
    @Test
    public void testPartitionSwitchAfterAppends() throws Exception {
        assertMemoryLeak(new CountingFacade(), () -> {
            createIndexedTable();
            for (int i = 0; i < 40; i++) {
                insertAndApply("(" + i + "::TIMESTAMP, 'k" + (i % 3) + "', " + i + ", " + -i + ")");
            }
            clearMsyncs();
            insertAndApply("(" + DAY + "::TIMESTAMP, 'k1', 40, -40)");
            assertSealFlushes(1);
            assertIndexMatchesScan();
        });
    }

    // A WAL commit into several partitions seals the covering index of each.
    @Test
    public void testWalCommitAcrossPartitions() throws Exception {
        assertMemoryLeak(new CountingFacade(), () -> {
            createIndexedTable();
            clearMsyncs();
            insertAndApply(rows(0));
            assertSealFlushes(DAYS);
            assertIndexMatchesScan();
        });
    }

    // A WAL commit with rows between the committed rows of older partitions rewrites them and reseals their index.
    @Test
    public void testWalCommitIntoOlderPartitions() throws Exception {
        // the production default: such a commit rewrites the partition rather than appending to it in place
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(new CountingFacade(), () -> {
            createIndexedTable();
            insertAndApply(rows(0));
            clearMsyncs();
            insertAndApply(rows(500_000L));
            assertSealFlushes(DAYS);
            assertIndexMatchesScan();
        });
    }

    private static void clearMsyncs() {
        synchronized (MSYNCS) {
            MSYNCS.clear();
        }
    }

    private static void createIndexedTable() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, k SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1), c0 LONG, c1 LONG)" +
                " TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    // k.pv.1 -> pv, k.pc1.0.0.1 -> pcN, k.pci.0 -> pci, k.pk -> pk; null for a file that is not an index file
    private static String indexFileKind(String path) {
        if (path == null) {
            return null;
        }
        final String[] parts = path.substring(Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\')) + 1).split("\\.");
        final String kind = parts.length > 1 && parts[0].equals("k") ? parts[1].replaceAll("^pc[0-9]+$", "pcN") : "";
        return switch (kind) {
            case "pv", "pcN", "pci", "pk" -> kind;
            default -> null;
        };
    }

    private static void insertAndApply(String values) throws Exception {
        execute("INSERT INTO t VALUES " + values);
        drainWalQueue();
    }

    // four rows a day in each of DAYS day partitions, each row shiftMicros past a whole hour
    private static String rows(long shiftMicros) {
        final StringBuilder sql = new StringBuilder();
        for (int i = 0; i < 4 * DAYS; i++) {
            sql.append(i == 0 ? "" : ", ").append('(').append((i % DAYS) * DAY + (i / DAYS) * 3_600_000_000L + shiftMicros)
                    .append("::TIMESTAMP, 'k").append(i % 3).append("', ").append(i).append(", ").append(-i).append(')');
        }
        return sql.toString();
    }

    private void assertIndexMatchesScan() throws Exception {
        for (int k = 0; k < 3; k++) {
            assertSqlCursors(
                    "SELECT ts, k, c0, c1 FROM t WHERE k::STRING = 'k" + k + "'",
                    "SELECT ts, k, c0, c1 FROM t WHERE k = 'k" + k + "'"
            );
        }
    }

    private void assertSealFlushes(int sealedPartitions) {
        Assert.assertEquals(CommitMode.fromString(commitMode), engine.getConfiguration().getCommitMode());
        final Map<String, Integer> msyncs;
        synchronized (MSYNCS) {
            msyncs = new TreeMap<>(MSYNCS);
        }
        if (!CommitMode.isPowerLossDurable(engine.getConfiguration().getCommitMode())) {
            Assert.assertEquals("msync calls on index files under " + commitMode, "{}", msyncs.toString());
            return;
        }
        // The seal of each partition waits for its value file, the cover files of c0, c1 and the designated
        // timestamp, the cover info file and the key file.
        final int[] perPartition = {1, 3, 1, 1};
        final String[] kinds = {"pv", "pcN", "pci", "pk"};
        for (int i = 0; i < kinds.length; i++) {
            Assert.assertTrue(
                    "msync calls on " + kinds[i] + " files under " + commitMode + ' ' + msyncs,
                    msyncs.getOrDefault(kinds[i], 0) >= perPartition[i] * sealedPartitions
            );
        }
    }

    private static class CountingFacade extends TestFilesFacadeImpl {
        @Override
        public void msync(long addr, long len, boolean async) {
            super.msync(addr, len, async);
            final String kind = indexFileKind(Files.findPathOfMapping(addr));
            if (kind != null) {
                synchronized (MSYNCS) {
                    MSYNCS.merge(async ? kind + " async" : kind, 1, Integer::sum);
                }
            }
        }
    }
}
