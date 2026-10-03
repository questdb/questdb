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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.PartitionCompactionScanJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.std.Files;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PartitionCompactionSplitTest extends AbstractCairoTest {
    private static final int ALL_FOLDERS = 7;
    private static final int FOLDER_COUNT = 3;
    private static final int SNAPSHOT_STRIDE = 3;
    private final Rnd rnd = TestUtils.generateRandom(LOG);

    @Test
    public void testAllHotFoldersPreventCompaction() throws Exception {
        final int compositeMask = rnd.nextInt(ALL_FOLDERS + 1);
        LOG.info().$("compositeMask=").$(compositeMask).$();
        assertSelectiveCompaction(compositeMask, ALL_FOLDERS);
    }

    @Test
    public void testHotFoldersAllowOnlyColdCompositeCompaction() throws Exception {
        final int compositeMask = rnd.nextInt(ALL_FOLDERS + 1);
        LOG.info().$("compositeMask=").$(compositeMask).$();
        assertSelectiveCompaction(compositeMask, 1 + rnd.nextInt(ALL_FOLDERS - 1));
    }

    @Test
    public void testSquashAtIdleBoundary() throws Exception {
        final int compositeMask = rnd.nextInt(ALL_FOLDERS + 1);
        LOG.info().$("compositeMask=").$(compositeMask).$();
        assertSquash(compositeMask, 30 * Micros.MINUTE_MICROS);
    }

    @Test
    public void testSquashBetweenIdleTimeouts() throws Exception {
        final int compositeMask = rnd.nextInt(ALL_FOLDERS + 1);
        LOG.info().$("compositeMask=").$(compositeMask).$();
        assertSquash(compositeMask, 45 * Micros.MINUTE_MICROS);
    }

    private static void assertData() throws Exception {
        TestUtils.assertSqlCursors(
                engine, sqlExecutionContext,
                "SELECT * FROM oracle ORDER BY ts, i", "SELECT * FROM x ORDER BY ts, i", LOG
        );
        TestUtils.assertSqlCursors(
                engine, sqlExecutionContext,
                "SELECT * FROM oracle WHERE sym_bitmap = 's1' ORDER BY ts, i",
                "SELECT * FROM x WHERE sym_bitmap = 's1' ORDER BY ts, i", LOG
        );
        TestUtils.assertSqlCursors(
                engine, sqlExecutionContext,
                "SELECT * FROM oracle WHERE sym_posting = 's1' ORDER BY ts, i",
                "SELECT * FROM x WHERE sym_posting = 's1' ORDER BY ts, i", LOG
        );
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
    }

    private static void assertMerged(LongList snapshot) throws Exception {
        try (TableReader reader = engine.getReader("x")) {
            final TxReader tx = reader.getTxFile();
            Assert.assertEquals("one merged day plus the active day", 2, tx.getPartitionCount());
            Assert.assertEquals(snapshot.getQuick(0), tx.getPartitionTimestampByIndex(0));
            Assert.assertFalse(tx.isPartitionComposite(0));
            Assert.assertEquals(1, reader.getGeometry().getPieceCount(0));
            long rows = 0;
            for (int i = 0; i < FOLDER_COUNT; i++) {
                rows += snapshot.getQuick(i * SNAPSHOT_STRIDE + 1);
            }
            Assert.assertEquals(rows, tx.getPartitionSize(0));
            Assert.assertNotEquals(snapshot.getQuick(2), tx.getPartitionNameTxn(0));
            assertSnapshotFolder(tx, 1, snapshot, FOLDER_COUNT, false);
        }
        assertData();
    }

    private static void assertSnapshotFolder(TxReader tx, int index, LongList snapshot, int sourceIndex, boolean isRewritten) {
        final int offset = sourceIndex * SNAPSHOT_STRIDE;
        Assert.assertEquals("folder timestamp " + sourceIndex, snapshot.getQuick(offset), tx.getPartitionTimestampByIndex(index));
        Assert.assertEquals("folder rows " + sourceIndex, snapshot.getQuick(offset + 1), tx.getPartitionSize(index));
        if (isRewritten) {
            Assert.assertNotEquals("folder must be rewritten " + sourceIndex, snapshot.getQuick(offset + 2), tx.getPartitionNameTxn(index));
        } else {
            Assert.assertEquals("folder must stay untouched " + sourceIndex, snapshot.getQuick(offset + 2), tx.getPartitionNameTxn(index));
        }
    }

    private static void assertSplit(LongList snapshot, int expectedCompositeMask, int rewrittenMask) throws Exception {
        try (TableReader reader = engine.getReader("x")) {
            final TxReader tx = reader.getTxFile();
            Assert.assertEquals("the logical day must retain its three folders", FOLDER_COUNT + 1, tx.getPartitionCount());
            for (int i = 0; i <= FOLDER_COUNT; i++) {
                assertSnapshotFolder(tx, i, snapshot, i, (rewrittenMask & (1 << i)) != 0);
                final boolean isComposite = (expectedCompositeMask & (1 << i)) != 0;
                Assert.assertEquals("composite folder " + i, isComposite, tx.isPartitionComposite(i));
                if (isComposite) {
                    Assert.assertTrue("fixture must require REWRITE, not MAKE-PLAIN", reader.getGeometry().getPieceCount(i) > 1);
                }
            }
        }
    }

    private static void insertRows(int idOffset, long timestamp, int rows, long step) throws Exception {
        execute("""
                INSERT INTO x
                SELECT (x + %d)::INT i,
                       CASE WHEN x %% 7 = 0 THEN NULL ELSE ('value-' || x)::VARCHAR END v,
                       ('s' || (x %% 4))::SYMBOL sym_bitmap,
                       ('s' || (x %% 4))::SYMBOL sym_posting,
                       timestamp_sequence(%d, %d) ts
                FROM long_sequence(%d)
                """.formatted(idOffset, timestamp, step, rows));
        drainWalQueue();
    }

    private void assertSelectiveCompaction(int compositeMask, int hotMask) throws Exception {
        final CompactionFilesFacade ff = new CompactionFilesFacade();
        assertMemoryLeak(ff, () -> {
            final long writtenAt = MicrosFormatUtils.parseTimestamp("2020-01-10T00:00:00.000000Z");
            final LongList snapshot = createSplitTable(ff, writtenAt, compositeMask, hotMask);
            int remainingColdMask = compositeMask & ~hotMask;
            int rewrittenMask = 0;
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, ff, configuration.getMicrosecondClock())) {
                setCurrentMicros(writtenAt + Micros.HOUR_MICROS - 1);
                job.run();
                assertSplit(snapshot, compositeMask, 0);
                Assert.assertEquals(0, ff.rewriteBuildCount);
                Assert.assertEquals(0, ff.mergeBuildCount);

                // Each sweep can compact only one cold folder; hot composites and plain folders stay untouched.
                for (int sweep = 0; sweep < FOLDER_COUNT; sweep++) {
                    setCurrentMicros(writtenAt + Micros.HOUR_MICROS + sweep);
                    job.run();
                    final int nextFolder = Integer.lowestOneBit(remainingColdMask);
                    rewrittenMask |= nextFolder;
                    remainingColdMask &= ~nextFolder;
                    assertSplit(snapshot, compositeMask & ~rewrittenMask, rewrittenMask);
                    Assert.assertEquals(Integer.bitCount(rewrittenMask), ff.rewriteBuildCount);
                    Assert.assertEquals(0, ff.mergeBuildCount);
                    assertData();
                }
                Assert.assertEquals("all eligible composites must have been compacted", 0, remainingColdMask);

                // The original hot folders are now squashable, but a fresh REWRITE must restart its idle clock.
                if (rewrittenMask != 0) {
                    setCurrentMicros(writtenAt + 70 * Micros.MINUTE_MICROS);
                    job.run();
                    assertSplit(snapshot, compositeMask & ~rewrittenMask, rewrittenMask);
                    Assert.assertEquals(0, ff.mergeBuildCount);
                    Assert.assertEquals(Integer.bitCount(rewrittenMask), ff.rewriteBuildCount);
                }
                setCurrentMicros(writtenAt + 90 * Micros.MINUTE_MICROS + Micros.SECOND_MICROS);
                job.run();
                Assert.assertEquals(1, ff.mergeBuildCount);
                Assert.assertEquals(Integer.bitCount(rewrittenMask), ff.rewriteBuildCount);
                assertMerged(snapshot);
            }
        });
    }

    private void assertSquash(int compositeMask, long idleMicros) throws Exception {
        final CompactionFilesFacade ff = new CompactionFilesFacade();
        assertMemoryLeak(ff, () -> {
            final long writtenAt = MicrosFormatUtils.parseTimestamp("2020-01-10T00:00:00.000000Z");
            final LongList snapshot = createSplitTable(ff, writtenAt, compositeMask, 0);
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, ff, configuration.getMicrosecondClock())) {
                setCurrentMicros(writtenAt + 30 * Micros.MINUTE_MICROS - 1);
                job.run();
                assertSplit(snapshot, compositeMask, 0);
                Assert.assertEquals(0, ff.mergeBuildCount);
                Assert.assertEquals(0, ff.rewriteBuildCount);

                setCurrentMicros(writtenAt + idleMicros);
                job.run();
                Assert.assertEquals("squash must not wait for the 60-minute single-folder timeout", 1, ff.mergeBuildCount);
                Assert.assertEquals("merge the whole run instead of rewriting individual folders", 0, ff.rewriteBuildCount);
                assertMerged(snapshot);

                setCurrentMicros(currentMicros + Micros.HOUR_MICROS);
                job.run();
                Assert.assertEquals("a merged plain folder has nothing left to compact", 1, ff.mergeBuildCount);
                Assert.assertEquals(0, ff.rewriteBuildCount);
            }
        });
    }

    private LongList createSplitTable(CompactionFilesFacade ff, long writtenAt, int compositeMask, int hotMask) throws Exception {
        // Bits 0..2 select first/middle/last folders; zero exercises an entirely plain split day.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 1000);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_THRESHOLD, "1T");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "100000h");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, 0);
        setCurrentMicros(writtenAt);
        execute("""
                CREATE TABLE x AS (
                    SELECT x::INT i,
                           CASE WHEN x % 7 = 0 THEN NULL ELSE ('value-' || x)::VARCHAR END v,
                           ('s' || (x % 4))::SYMBOL sym_bitmap,
                           ('s' || (x % 4))::SYMBOL sym_posting,
                           timestamp_sequence('2020-01-01', 15_000_000L) ts
                    FROM long_sequence(5760)
                ), INDEX(sym_bitmap), INDEX(sym_posting TYPE POSTING)
                TIMESTAMP(ts) PARTITION BY DAY WAL
                """);
        insertRows(90_000, MicrosFormatUtils.parseTimestamp("2020-01-03T00:00:00.000000Z"), 50, Micros.MINUTE_MICROS);
        // Cut the large plain prefix twice, leaving first/middle/last folders in the same day.
        insertRows(70_000, MicrosFormatUtils.parseTimestamp("2020-01-01T22:00:07.000000Z"), 200, 5 * Micros.SECOND_MICROS);
        insertRows(80_000, MicrosFormatUtils.parseTimestamp("2020-01-01T18:00:07.000000Z"), 200, 5 * Micros.SECOND_MICROS);

        final LongList timestamps = new LongList();
        try (TableReader reader = engine.getReader("x")) {
            final TxReader tx = reader.getTxFile();
            Assert.assertEquals(FOLDER_COUNT + 1, tx.getPartitionCount());
            for (int i = 0; i < FOLDER_COUNT; i++) {
                Assert.assertFalse(tx.isPartitionComposite(i));
                Assert.assertEquals(tx.getPartitionTimestampByIndex(0), tx.getLogicalPartitionTimestamp(tx.getPartitionTimestampByIndex(i)));
                timestamps.add(tx.getPartitionTimestampByIndex(i));
            }
        }
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, true);
        // Write the cold folders first so the simulated clock never moves backwards.
        for (int pass = 0; pass < 2; pass++) {
            final int mask = compositeMask & (pass == 0 ? ALL_FOLDERS & ~hotMask : hotMask);
            if (mask == 0) {
                continue;
            }
            setCurrentMicros(writtenAt + (pass == 0 ? 0 : 40 * Micros.MINUTE_MICROS));
            for (int i = 0; i < FOLDER_COUNT; i++) {
                if ((mask & (1 << i)) != 0) {
                    final long lo = timestamps.getQuick(i);
                    final long hi = i + 1 < FOLDER_COUNT ? timestamps.getQuick(i + 1) : lo - lo % Micros.DAY_MICROS + Micros.DAY_MICROS;
                    insertRows(100_000 + i * 100, lo + (hi - lo) / 4 + 1, 20, Micros.SECOND_MICROS);
                }
            }
        }

        final LongList snapshot = new LongList();
        try (TableReader reader = engine.getReader("x"); Path path = new Path()) {
            final TxReader tx = reader.getTxFile();
            final TableToken token = reader.getTableToken();
            Assert.assertEquals(FOLDER_COUNT + 1, tx.getPartitionCount());
            for (int i = 0; i <= FOLDER_COUNT; i++) {
                snapshot.add(tx.getPartitionTimestampByIndex(i));
                snapshot.add(tx.getPartitionSize(i));
                snapshot.add(tx.getPartitionNameTxn(i));
                final long lastWrite = writtenAt + ((hotMask & (1 << i)) == 0 ? 0 : 40 * Micros.MINUTE_MICROS);
                if (tx.isPartitionComposite(i)) {
                    Assert.assertEquals("composite age must come from its actual write", lastWrite, reader.getGeometry().getLastWriteMicros(i));
                }
                path.of(configuration.getDbRoot()).concat(token.getDirName());
                TableUtils.setPathForNativePartition(path, reader.getMetadata().getTimestampType(), reader.getPartitionedBy(),
                        tx.getPartitionTimestampByIndex(i), tx.getPartitionNameTxn(i));
                ff.lastModifiedMillis.put(path.concat("ts.d").toString(), lastWrite / Micros.MILLI_MICROS);
            }
        }
        assertSplit(snapshot, compositeMask, 0);
        execute("CREATE TABLE oracle AS (SELECT * FROM x) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_SQUASH_IDLE_TIMEOUT, "30m");
        ff.mergeBuildCount = 0;
        ff.rewriteBuildCount = 0;
        return snapshot;
    }

    private static class CompactionFilesFacade extends TestFilesFacadeImpl {
        private final Map<String, Long> lastModifiedMillis = new HashMap<>();
        private int mergeBuildCount;
        private int rewriteBuildCount;

        @Override
        public long getLastModified(LPSZ path) {
            final String fileName = Utf8s.stringFromUtf8Bytes(path);
            final Long modified = lastModifiedMillis.get(fileName);
            if (fileName.endsWith("ts.d")) {
                Assert.assertNotNull("uncontrolled timestamp file: " + fileName + "; known: " + lastModifiedMillis, modified);
            }
            return modified != null ? modified : super.getLastModified(path);
        }

        @Override
        public int mkdirs(Path path, int mode) {
            if (Utf8s.containsAscii(path, TableUtils.MERGING_DIR_MARKER)) {
                mergeBuildCount++;
            } else if (Utf8s.containsAscii(path, TableUtils.COMPACTING_DIR_MARKER)) {
                rewriteBuildCount++;
            }
            return super.mkdirs(path, mode);
        }

        @Override
        public int rename(LPSZ from, LPSZ to) {
            final int result = super.rename(from, to);
            if (result == Files.FILES_RENAME_OK
                    && (Utf8s.containsAscii(from, TableUtils.MERGING_DIR_MARKER)
                    || Utf8s.containsAscii(from, TableUtils.COMPACTING_DIR_MARKER))) {
                lastModifiedMillis.put(Utf8s.stringFromUtf8Bytes(to) + Files.SEPARATOR + "ts.d", currentMicros / Micros.MILLI_MICROS);
            }
            return result;
        }
    }
}
