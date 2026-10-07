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
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The compaction sweep builds every POSTING index of the directory it stages - covered values included - on its own
 * thread, so the writer's swap re-indexes nothing. Both staged paths: the whole logical partition MERGE and the
 * single-folder REWRITE, on a table with a COVERING posting index and a plain one.
 */
public class CompactionStagedPostingIndexTest extends AbstractCairoTest {
    private static final String DAY_ONE = "SELECT x::INT i, ('sym' || (x % 7))::SYMBOL sym, ('p' || (x % 5))::SYMBOL p, x * 3 v," +
            " ('s' || x)::VARCHAR s, timestamp_sequence('2020-01-01', 15 * 1_000_000L) ts FROM long_sequence(5760)";
    private static final String DAY_THREE = "SELECT x::INT + 90_000 i, ('sym' || (x % 7))::SYMBOL sym, ('p' || (x % 5))::SYMBOL p, x * 3 v," +
            " ('s' || x)::VARCHAR s, timestamp_sequence('2020-01-03', 60 * 1_000_000L) ts FROM long_sequence(50)";

    @Test
    public void testMergeBuildsPostingIndexesOffTheWriter() throws Exception {
        // Merge-append off: the O3 insert splits the day into two plain folders, which the sweep merges.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        final PostingFileCounter ff = new PostingFileCounter();
        assertMemoryLeak(ff, () -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createTable("2020-01-01T22:00:07");
            Assert.assertEquals("the O3 insert must split 2020-01-01", 3, partitionCount());

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_SQUASH_IDLE_TIMEOUT, "30m");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:00:00.000000Z"));
            // Plain folders use filesystem modification times, not the simulated clock.
            final long idleTicks = MicrosecondClockImpl.INSTANCE.getTicks() + 2 * Micros.HOUR_MICROS;
            sweep(ff, () -> idleTicks);
            Assert.assertEquals("the sweep must merge the split day", 2, partitionCount());

            assertIndexes();
            assertIndexesAfterFurtherWrites();
        });
    }

    @Test
    public void testRewriteBuildsPostingIndexesOffTheWriter() throws Exception {
        // Merge-append on: the O3 insert lands inside 2020-01-01 and cuts it into pieces, which the sweep rewrites.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        final PostingFileCounter ff = new PostingFileCounter();
        assertMemoryLeak(ff, () -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createTable("2020-01-01T04:00:07");
            try (TableReader reader = engine.getReader("cx")) {
                Assert.assertTrue("2020-01-01 must be composite", reader.getTxFile().isPartitionComposite(0));
            }

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));
            sweep(ff, configuration.getMicrosecondClock());
            try (TableReader reader = engine.getReader("cx")) {
                Assert.assertFalse("the sweep must rewrite 2020-01-01", reader.getTxFile().isPartitionComposite(0));
            }

            assertIndexes();
            assertIndexesAfterFurtherWrites();
        });
    }

    private static void createTable(String backfillStart) throws Exception {
        execute("CREATE TABLE cx (i INT, sym SYMBOL INDEX TYPE POSTING INCLUDE (v, s), p SYMBOL INDEX TYPE POSTING," +
                " v LONG, s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE TABLE cx_oracle (i INT, sym SYMBOL, p SYMBOL, v LONG, s VARCHAR, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        insert(DAY_ONE);
        insert(DAY_THREE);
        insert("SELECT x::INT + 70_000 i, ('sym' || (x % 7))::SYMBOL sym, ('p' || (x % 5))::SYMBOL p, x * 7 v," +
                " ('b' || x)::VARCHAR s, timestamp_sequence('" + backfillStart + "', 5 * 1_000_000L) ts FROM long_sequence(200)");
    }

    private static void insert(String select) throws Exception {
        execute("INSERT INTO cx " + select);
        execute("INSERT INTO cx_oracle " + select);
        drainWalQueue();
    }

    private static int partitionCount() {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        try (TableReader reader = engine.getReader("cx")) {
            final TxReader tx = reader.getTxFile();
            return tx.getPartitionCount();
        }
    }

    private static void sweep(PostingFileCounter ff, Clock clock) {
        ff.isCounting = true;
        try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, ff, clock)) {
            job.run();
        } finally {
            ff.isCounting = false;
        }
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        Assert.assertTrue("the sweep must have built posting files in its staging directory", ff.stagedOpens > 0);
        Assert.assertEquals("the writer must not write posting index files during the swap: " + ff.publishedFiles,
                0, ff.publishedOpens);
    }

    private void assertIndexes() throws Exception {
        // The COVERING index serves the covered columns out of its sidecars.
        assertQuery("SELECT v, s, ts FROM cx WHERE sym = 'sym3'").assertsPlanContaining("CoveringIndex on: sym");
        for (int k = 0; k < 7; k++) {
            TestUtils.assertSqlCursors(
                    engine, sqlExecutionContext,
                    "SELECT v, s, ts FROM cx_oracle WHERE sym = 'sym" + k + "'",
                    "SELECT v, s, ts FROM cx WHERE sym = 'sym" + k + "'",
                    LOG
            );
        }
        for (int k = 0; k < 5; k++) {
            TestUtils.assertSqlCursors(
                    engine, sqlExecutionContext,
                    "SELECT i, sym, p, v, s, ts FROM cx_oracle WHERE p = 'p" + k + "'",
                    "SELECT i, sym, p, v, s, ts FROM cx WHERE p = 'p" + k + "'",
                    LOG
            );
        }
    }

    /**
     * The writer carries on from the chain the sweep built: more rows into the compacted day, in order and out of it.
     */
    private void assertIndexesAfterFurtherWrites() throws Exception {
        insert("SELECT x::INT + 80_000 i, ('sym' || (x % 7))::SYMBOL sym, ('p' || (x % 5))::SYMBOL p, x * 11 v," +
                " ('o' || x)::VARCHAR s, timestamp_sequence('2020-01-01T10:00:03', 7 * 1_000_000L) ts FROM long_sequence(100)");
        insert("SELECT x::INT + 85_000 i, ('sym' || (x % 7))::SYMBOL sym, ('p' || (x % 5))::SYMBOL p, x * 13 v," +
                " ('n' || x)::VARCHAR s, timestamp_sequence('2020-01-03T12:00:00', 1_000_000L) ts FROM long_sequence(100)");
        assertIndexes();
    }

    private static class PostingFileCounter extends TestFilesFacadeImpl {
        private final StringBuilder publishedFiles = new StringBuilder();
        private boolean isCounting;
        private int publishedOpens;
        private int stagedOpens;

        @Override
        public long openRW(LPSZ name, int opts) {
            // Only the compacted day: taking the writer out of the pool reopens the ACTIVE partition's indexers.
            if (isCounting && Utf8s.containsAscii(name, "2020-01-01")
                    && (Utf8s.containsAscii(name, ".pv") || Utf8s.containsAscii(name, ".pc"))) {
                if (Utf8s.containsAscii(name, TableUtils.MERGING_DIR_MARKER) || Utf8s.containsAscii(name, TableUtils.COMPACTING_DIR_MARKER)) {
                    stagedOpens++;
                } else {
                    publishedOpens++;
                    publishedFiles.append(Utf8s.stringFromUtf8Bytes(name)).append(' ');
                }
            }
            return super.openRW(name, opts);
        }
    }
}
