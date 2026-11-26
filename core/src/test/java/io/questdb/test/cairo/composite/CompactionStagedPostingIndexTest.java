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
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.std.Files;
import io.questdb.std.LongHashSet;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
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
    public void testMergeBuildsPostingIndexesOfAddedColumns() throws Exception {
        assertMergeBuildsPostingIndexes(true);
    }

    @Test
    public void testMergeBuildsPostingIndexesOffTheWriter() throws Exception {
        assertMergeBuildsPostingIndexes(false);
    }

    @Test
    public void testRewriteBuildsPostingIndexesOfAddedColumns() throws Exception {
        assertRewriteBuildsPostingIndexes(true);
    }

    @Test
    public void testRewriteBuildsPostingIndexesOffTheWriter() throws Exception {
        assertRewriteBuildsPostingIndexes(false);
    }

    /**
     * The compacted 2020-01-01 holds a POSTING key file for every one of {@code columnNames}, whether or not the
     * column has rows in it.
     */
    private static void assertPostingKeyFilesExist(String... columnNames) {
        try (TableReader reader = engine.getReader("cx"); Path path = new Path()) {
            final TxReader txFile = reader.getTxFile();
            final TableRecordMetadata metadata = reader.getMetadata();
            final long partitionTimestamp = txFile.getPartitionTimestampByIndex(0);
            path.of(configuration.getDbRoot()).concat(reader.getTableToken().getDirName());
            TableUtils.setPathForNativePartition(
                    path,
                    metadata.getTimestampType(),
                    reader.getPartitionedBy(),
                    partitionTimestamp,
                    txFile.getPartitionNameTxn(0)
            );
            final int partitionLen = path.size();
            for (String columnName : columnNames) {
                final int writerIndex = metadata.getWriterIndex(metadata.getColumnIndex(columnName));
                final long columnNameTxn = reader.getColumnVersionReader().getColumnNameTxn(partitionTimestamp, writerIndex);
                final LPSZ keyFile = PostingIndexUtils.keyFileName(path.trimTo(partitionLen), columnName, columnNameTxn);
                Assert.assertTrue(
                        "missing posting key file: " + Utf8s.stringFromUtf8Bytes(keyFile),
                        configuration.getFilesFacade().exists(keyFile)
                );
            }
        }
    }

    /**
     * @param withAddedColumns adds two POSTING-indexed columns: {@code q} after the first day, which the later inserts
     *                         fill, so the staged day holds it under a column top, and {@code r} after the last
     *                         insert, so the staged day holds no rows of it at all
     */
    private static void createTable(String backfillStart, boolean withAddedColumns) throws Exception {
        execute("CREATE TABLE cx (i INT, sym SYMBOL INDEX TYPE POSTING INCLUDE (v, s), p SYMBOL INDEX TYPE POSTING," +
                " v LONG, s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE TABLE cx_oracle (i INT, sym SYMBOL, p SYMBOL, v LONG, s VARCHAR, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        insert(DAY_ONE);
        final String backfill = "SELECT x::INT + 70_000 i, ('sym' || (x % 7))::SYMBOL sym, ('p' || (x % 5))::SYMBOL p, x * 7 v," +
                " ('b' || x)::VARCHAR s, timestamp_sequence('" + backfillStart + "', 5 * 1_000_000L) ts";
        if (withAddedColumns) {
            execute("ALTER TABLE cx ADD COLUMN q SYMBOL INDEX TYPE POSTING");
            execute("ALTER TABLE cx_oracle ADD COLUMN q SYMBOL");
            drainWalQueue();
            insert(DAY_THREE.replace(" FROM ", ", ('q' || (x % 3))::SYMBOL q FROM "));
            insert(backfill + ", ('q' || (x % 3))::SYMBOL q FROM long_sequence(200)");
            execute("ALTER TABLE cx ADD COLUMN r SYMBOL INDEX TYPE POSTING");
            execute("ALTER TABLE cx_oracle ADD COLUMN r SYMBOL");
            drainWalQueue();
        } else {
            insert(DAY_THREE);
            insert(backfill + " FROM long_sequence(200)");
        }
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
        Assert.assertEquals("the copy must leave POSTING indexes to the index build, which rebuilds them from the data files: "
                + ff.copyPostingFiles, 0, ff.copyPostingOpens);
        Assert.assertEquals("the writer must not write posting index files during the swap: " + ff.publishedFiles,
                0, ff.publishedOpens);
    }

    private void assertIndexes(boolean withAddedColumns) throws Exception {
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
        if (withAddedColumns) {
            for (int k = 0; k < 3; k++) {
                TestUtils.assertSqlCursors(
                        engine, sqlExecutionContext,
                        "SELECT i, q, r, ts FROM cx_oracle WHERE q = 'q" + k + "'",
                        "SELECT i, q, r, ts FROM cx WHERE q = 'q" + k + "'",
                        LOG
                );
            }
            for (int k = 0; k < 4; k++) {
                TestUtils.assertSqlCursors(
                        engine, sqlExecutionContext,
                        "SELECT i, q, r, ts FROM cx_oracle WHERE r = 'r" + k + "'",
                        "SELECT i, q, r, ts FROM cx WHERE r = 'r" + k + "'",
                        LOG
                );
            }
        }
    }

    /**
     * The writer carries on from the chain the sweep built: more rows into the compacted day, in order and out of it.
     */
    private void assertIndexesAfterFurtherWrites(boolean withAddedColumns) throws Exception {
        final String addedColumns = withAddedColumns ? ", ('q' || (x % 3))::SYMBOL q, ('r' || (x % 4))::SYMBOL r" : "";
        insert("SELECT x::INT + 80_000 i, ('sym' || (x % 7))::SYMBOL sym, ('p' || (x % 5))::SYMBOL p, x * 11 v," +
                " ('o' || x)::VARCHAR s, timestamp_sequence('2020-01-01T10:00:03', 7 * 1_000_000L) ts" + addedColumns +
                " FROM long_sequence(100)");
        insert("SELECT x::INT + 85_000 i, ('sym' || (x % 7))::SYMBOL sym, ('p' || (x % 5))::SYMBOL p, x * 13 v," +
                " ('n' || x)::VARCHAR s, timestamp_sequence('2020-01-03T12:00:00', 1_000_000L) ts" + addedColumns +
                " FROM long_sequence(100)");
        assertIndexes(withAddedColumns);
    }

    private void assertMergeBuildsPostingIndexes(boolean withAddedColumns) throws Exception {
        // Merge-append off: the O3 insert splits the day into two plain folders, which the sweep merges.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        final PostingFileCounter ff = new PostingFileCounter();
        assertMemoryLeak(ff, () -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createTable("2020-01-01T22:00:07", withAddedColumns);
            Assert.assertEquals("the O3 insert must split 2020-01-01", 3, partitionCount());

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_SQUASH_IDLE_TIMEOUT, "30m");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:00:00.000000Z"));
            // Plain folders use filesystem modification times, not the simulated clock.
            final long idleTicks = MicrosecondClockImpl.INSTANCE.getTicks() + 2 * Micros.HOUR_MICROS;
            sweep(ff, () -> idleTicks);
            Assert.assertEquals("the sweep must merge the split day", 2, partitionCount());

            if (withAddedColumns) {
                assertPostingKeyFilesExist("sym", "p", "q", "r");
            }
            assertIndexes(withAddedColumns);
            assertIndexesAfterFurtherWrites(withAddedColumns);
        });
    }

    private void assertRewriteBuildsPostingIndexes(boolean withAddedColumns) throws Exception {
        // Merge-append on: the O3 insert lands inside 2020-01-01 and cuts it into pieces, which the sweep rewrites.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        final PostingFileCounter ff = new PostingFileCounter();
        assertMemoryLeak(ff, () -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-01T00:00:00.000000Z"));
            createTable("2020-01-01T04:00:07", withAddedColumns);
            try (TableReader reader = engine.getReader("cx")) {
                Assert.assertTrue("2020-01-01 must be composite", reader.getTxFile().isPartitionComposite(0));
            }

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2020-01-10T00:10:00.000000Z"));
            sweep(ff, configuration.getMicrosecondClock());
            try (TableReader reader = engine.getReader("cx")) {
                Assert.assertFalse("the sweep must rewrite 2020-01-01", reader.getTxFile().isPartitionComposite(0));
            }

            if (withAddedColumns) {
                assertPostingKeyFilesExist("sym", "p", "q", "r");
            }
            assertIndexes(withAddedColumns);
            assertIndexesAfterFurtherWrites(withAddedColumns);
        });
    }

    private static class PostingFileCounter extends TestFilesFacadeImpl {
        private final StringBuilder copyPostingFiles = new StringBuilder();
        private final StringBuilder publishedFiles = new StringBuilder();
        // The staged column data files open for writing: only the copy writes them, the index build reads them.
        private final LongHashSet writableStagedDataFds = new LongHashSet();
        private int copyPostingOpens;
        private boolean isCounting;
        private int publishedOpens;
        private int stagedOpens;

        @Override
        public boolean close(long fd) {
            writableStagedDataFds.remove(fd);
            return super.close(fd);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            // Only the compacted day: taking the writer out of the pool reopens the ACTIVE partition's indexers.
            if (isCounting && Utf8s.containsAscii(name, "2020-01-01")) {
                final boolean isStaged = Utf8s.containsAscii(name, TableUtils.MERGING_DIR_MARKER)
                        || Utf8s.containsAscii(name, TableUtils.COMPACTING_DIR_MARKER);
                if (Utf8s.containsAscii(name, ".pk") || Utf8s.containsAscii(name, ".pv") || Utf8s.containsAscii(name, ".pc")) {
                    if (isStaged) {
                        if (writableStagedDataFds.size() > 0) {
                            copyPostingOpens++;
                            copyPostingFiles.append(Utf8s.stringFromUtf8Bytes(name)).append(' ');
                        }
                        if (!Utf8s.containsAscii(name, ".pk")) {
                            stagedOpens++;
                        }
                    } else if (!Utf8s.containsAscii(name, ".pk")) {
                        publishedOpens++;
                        publishedFiles.append(Utf8s.stringFromUtf8Bytes(name)).append(' ');
                    }
                } else if (isStaged && fd > -1 && isColumnDataFile(name)) {
                    writableStagedDataFds.add(fd);
                }
            }
            return fd;
        }

        private static boolean isColumnDataFile(LPSZ name) {
            final String path = Utf8s.stringFromUtf8Bytes(name);
            final String fileName = path.substring(path.lastIndexOf(Files.SEPARATOR) + 1);
            return fileName.endsWith(".d") || fileName.contains(".d.");
        }
    }
}
