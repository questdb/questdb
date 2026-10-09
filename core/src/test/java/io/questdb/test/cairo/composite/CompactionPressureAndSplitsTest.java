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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.PartitionCompactionScanJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class CompactionPressureAndSplitsTest extends AbstractCairoTest {
    @Test
    public void testClassicSplitOverflowsCapWhileEveryFolderIsHot() throws Exception {
        assertMemoryLeak(() -> {
            createFiveFolders();
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MAX_SPLITS, 5);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 1000);
            execute("INSERT INTO x SELECT x::INT + 100_000, timestamp_sequence('2024-01-01T04:00:00.001', 1_000L) FROM long_sequence(20)");
            drainWalQueue();
            try (TableReader reader = getReader("x")) {
                Assert.assertEquals("the cap is a squash target, not a split gate: the day overflows while its folders are hot",
                        6, reader.getPartitionCount());
            }
            assertQuery("SELECT count() c, sum(i) s FROM x").noRandomAccess().expectSize().returns("c\ts\n40100\t810021050\n");
        });
    }

    @Test
    public void testCommitCapChoosesSmallestColdAdjacentPair() throws Exception {
        assertMemoryLeak(() -> {
            createFiveFolders();
            final long sourceTimestamp;
            final long sourceRows;
            final long prefixRows;
            try (TableReader reader = getReader("x")) {
                final TxReader tx = reader.getTxFile();
                long smallest = Long.MAX_VALUE;
                int selected = -1;
                for (int i = 0; i < tx.getPartitionCount() - 1; i++) {
                    final long rows = tx.getPartitionSize(i) + tx.getPartitionSize(i + 1);
                    if (rows < smallest) {
                        smallest = rows;
                        selected = i;
                    }
                }
                Assert.assertTrue("the smallest pair must not include the large prefix", selected > 0);
                sourceTimestamp = tx.getPartitionTimestampByIndex(selected + 1);
                sourceRows = tx.getPartitionSize(selected + 1);
                prefixRows = tx.getPartitionSize(0);
            }
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MAX_SPLITS, 4);
            final long writtenBefore = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
            appendNextDay();
            Assert.assertEquals(sourceRows + 1,
                    node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - writtenBefore);
            try (TableReader reader = getReader("x")) {
                Assert.assertEquals(5, reader.getPartitionCount());
                Assert.assertTrue(reader.getTxFile().findAttachedPartitionIndexByLoTimestamp(sourceTimestamp) < 0);
                Assert.assertEquals(prefixRows, reader.getTxFile().getPartitionSize(0));
            }
            assertRows();
        });
    }

    @Test
    public void testIdleSquashIgnoresHotCommitWindow() throws Exception {
        assertMemoryLeak(() -> {
            createFiveFolders();
            appendNextDay();
            // The default window: the day's splits were written by the last 6 commits, so by commit count they are
            // all still hot - and the table makes no more commits to cool them.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 10);
            final long writtenBefore = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, configuration.getFilesFacade(), MicrosecondClockImpl.INSTANCE)) {
                job.run();
            }
            try (TableReader reader = getReader("x")) {
                Assert.assertEquals("a split written within the squash idle timeout must block the whole-day squash", 6, reader.getPartitionCount());
            }
            Assert.assertEquals(writtenBefore, node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows());
            // Idle past the timeout, the day folds whatever the commit window says.
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, configuration.getFilesFacade(), idleClock())) {
                job.run();
            }
            try (TableReader reader = getReader("x")) {
                Assert.assertEquals(2, reader.getPartitionCount());
            }
            assertRows();
        });
    }

    @Test
    public void testNextDayRetainsFiveFoldersAndSweepMergesTheDay() throws Exception {
        assertMemoryLeak(() -> {
            createFiveFolders();
            // The deprecated mid-partition limit must not collapse this day when the next day appears.
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 1);
            appendNextDay();
            try (TableReader reader = getReader("x")) {
                Assert.assertEquals(6, reader.getPartitionCount());
            }
            // The sweep folds the idle day whole, in one staged copy, however small the time budget.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TIME_BUDGET, "0ms");
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, configuration.getFilesFacade(), idleClock())) {
                job.run();
            }
            try (TableReader reader = getReader("x")) {
                Assert.assertEquals("the day's five folders must merge into one", 2, reader.getPartitionCount());
                Assert.assertFalse(reader.getTxFile().isPartitionComposite(0));
            }
            assertRows();
        });
    }

    @Test
    public void testSweepMergeDuringCheckpointKeepsRetiredFolders() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createFiveFolders();
            appendNextDay();
            final ObjList<String> dayFolders = new ObjList<>();
            try (TableReader reader = getReader("x"); Path path = new Path()) {
                final TxReader tx = reader.getTxFile();
                for (int i = 0; i < tx.getPartitionCount() - 1; i++) {
                    path.of(configuration.getDbRoot()).concat(reader.getTableToken().getDirName());
                    TableUtils.setPathForNativePartition(path, reader.getMetadata().getTimestampType(), reader.getPartitionedBy(),
                            tx.getPartitionTimestampByIndex(i), tx.getPartitionNameTxn(i));
                    dayFolders.add(path.toString());
                }
            }
            Assert.assertEquals(5, dayFolders.size());
            execute("CHECKPOINT CREATE");
            try {
                // The merge writes a new directory and never touches the day's folders, so a checkpoint does not
                // have to hold it back - only the retired folders have to outlive the checkpoint.
                try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, configuration.getFilesFacade(), idleClock())) {
                    job.run();
                }
                try (TableReader reader = getReader("x")) {
                    Assert.assertEquals(2, reader.getPartitionCount());
                }
                try (Path path = new Path()) {
                    for (int i = 0, n = dayFolders.size(); i < n; i++) {
                        Assert.assertTrue("a retired folder must outlive the checkpoint: " + dayFolders.getQuick(i),
                                configuration.getFilesFacade().exists(path.of(dayFolders.getQuick(i)).$()));
                    }
                }
                assertRows();
            } finally {
                execute("CHECKPOINT RELEASE");
            }
            assertRows();
        });
    }

    private static void appendNextDay() throws Exception {
        execute("INSERT INTO x VALUES (999, '2024-01-02')");
        drainWalQueue();
    }

    private void assertRows() throws Exception {
        assertQuery("SELECT count() c, sum(i) s FROM x WHERE ts IN '2024-01-01'")
                .noRandomAccess().expectSize().returns("c\ts\n40080\t808020840\n");
        assertQuery("SELECT count() c, sum(i) s FROM x").noRandomAccess().expectSize().returns("c\ts\n40081\t808021839\n");
        assertQuery("SELECT count() c FROM (SELECT ts FROM x ORDER BY ts)")
                .noRandomAccess().expectSize().returns("c\n40081\n");
    }

    private static void createFiveFolders() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MAX_SPLITS, 20);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, "0ms");
        setCurrentMicros(1);
        execute("CREATE TABLE x AS (SELECT x::INT i, timestamp_sequence('2024-01-01', 1_000_000L) ts FROM long_sequence(40_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalQueue();
        for (int minute : new int[]{540, 420, 345, 270}) {
            execute("INSERT INTO x SELECT x::INT + 100_000, timestamp_sequence('2024-01-01'::TIMESTAMP + "
                    + minute + " * 60_000_000L, 1_000L) FROM long_sequence(20)");
            drainWalQueue();
        }
        try (TableReader reader = getReader("x")) {
            Assert.assertEquals("fixture must create five native folders", 5, reader.getPartitionCount());
        }
    }

    private static Clock idleClock() {
        final AtomicLong ticks = new AtomicLong(MicrosecondClockImpl.INSTANCE.getTicks() + 2 * Micros.HOUR_MICROS);
        return ticks::incrementAndGet;
    }
}
