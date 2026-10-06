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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionCompactionPolicy;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The OPPORTUNISTIC squash - the one housekeeping runs after every commit to keep the split population
 * under {@code o3.*.partition.max.splits} - folding COMPOSITE partitions, as source, as target, and as
 * both. It cleans a small cold composite target before appending. Otherwise it appends piece by piece
 * with {@code FrameAlgebra}, starting at the target's physical extent rather than its live row count,
 * and republishes the target's geometry with one more piece.
 * <p>
 * MOVE-TAIL leaves a sibling behind on every fire, so on a real-time ingest this is the only thing
 * keeping the split count bounded - and by then every partition in reach is composite.
 * <p>
 * Each test snapshots the day's rows IN CURSOR ORDER before the fold and compares afterwards, so a piece
 * appended at the wrong offset, in the wrong order, or with dead space left in shows up as a difference
 * rather than only as a row-count change.
 */
public class CompositePartitionSquashTest extends AbstractCairoTest {
    private static final String DAY = "2024-01-01";

    @Before
    public void setUpSplits() {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 << 10);
        // Hold the siblings still while the fixture builds; each test lowers this to let the fold run.
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 1000);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 1000);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
    }

    @Test
    public void testForecastDoesNotRewriteASourceBeforeBlockCreatesALaterDayAndSquashes() throws Exception {
        assertMemoryLeak(() -> {
            createSingleDaySplit();
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1T");
            makeComposite("T11:00:00");
            final long sourceRows;
            final long lastSourceRows;
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                Assert.assertEquals(3, reader.getPartitionCount());
                Assert.assertTrue("fixture must have a composite source", reader.getTxFile().isPartitionComposite(2));
                lastSourceRows = reader.getTxFile().getPartitionSize(2);
                sourceRows = reader.getTxFile().getPartitionSize(1) + lastSourceRows;
            }
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, Long.MAX_VALUE / 8);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 1);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MAX_SPLITS, 1);
            final long countBefore = rowsOfDay();
            final long writtenBefore = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
            execute("INSERT INTO x (i, ts) VALUES (100, '2024-01-01T12:00:00')");
            execute("INSERT INTO x (i, ts) VALUES (101, '2024-01-02')");
            drainWalQueue();
            Assert.assertEquals("pair squashes must not also pay a preliminary forecast REWRITE",
                    // Pack the smallest pair first, then fold it into the prefix. The last source is copied twice.
                    // Two incoming writes and two copies of the last source's incoming row add four.
                    sourceRows + lastSourceRows + 4, node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - writtenBefore);
            Assert.assertEquals(1, partitionCountOfDay());
            Assert.assertEquals(countBefore + 1, rowsOfDay());
            assertDayReadsBack();
        });
    }

    @Test
    public void testMoveTailOverflowsSplitCapWithoutCopyingThePrefix() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, true);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1_024);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 2);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 16);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 100_000);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "100000h");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_THRESHOLD_PERCENT, 99);
            execute("CREATE TABLE x (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x SELECT x::INT, timestamp_sequence('2024-01-01', 1_000_000L) FROM long_sequence(10_000)");
            drainWalQueue();
            final long writtenBefore = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
            for (int i = 0; i < 5; i++) {
                execute("INSERT INTO x SELECT x::INT + 10_000, timestamp_sequence('2024-01-01T02:38:20', 1_000_000L) FROM long_sequence(200)");
                drainWalQueue();
            }
            Assert.assertEquals("first threshold breach must leave a tail sibling", 2, partitionCountOfDay());
            final long prefixRows;
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                prefixRows = reader.getTxFile().getPartitionSize(0);
                Assert.assertTrue("the move must preserve the large prefix", prefixRows > 9_000);
            }
            // The cap is a squash target, not a split gate: hot tails may overflow it up to the ceiling.
            final int ceiling = PartitionCompactionPolicy.getSplitCeiling(configuration);
            Assert.assertTrue("the ceiling must leave room past the cap", ceiling > 2);
            for (int i = 0; i < 8; i++) {
                execute("INSERT INTO x SELECT x::INT + 30_000, timestamp_sequence('2024-01-01T02:42:30'::TIMESTAMP + "
                        + (i * 25_000_000L) + ", 1_000_000L) FROM long_sequence(25)");
                drainWalQueue();
                Assert.assertTrue("ordinary squash must bound sibling growth", partitionCountOfDay() <= ceiling);
            }
            final long written = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - writtenBefore;
            Assert.assertTrue("tail moves and squash must not copy the 9,500-row prefix repeatedly: " + written, written < 20_000);
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                Assert.assertEquals("the accumulator must keep its original directory", -1, reader.getTxFile().getPartitionNameTxn(0));
                // A squash may append a cold sibling into the accumulator, but never rewrites it.
                Assert.assertTrue("MOVE-TAIL must not recopy the prefix", reader.getTxFile().getPartitionSize(0) >= prefixRows);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery("SELECT count() c, sum(i) s FROM x").noRandomAccess().expectSize()
                    .returns("c\ts\n11200\t66108100\n");
        });
    }

    @Test
    public void testAColumnAddedMidDaySurvivesTheFold() throws Exception {
        assertMemoryLeak(() -> {
            createSplitDay();
            // A column added now has a top on every existing partition, so both sides of the fold carry
            // one and the target's share of the source's leading NULLs has to be written out.
            execute("ALTER TABLE x ADD COLUMN v LONG");
            drainWalQueue();
            execute("INSERT INTO x (i, s, ts, v) SELECT cast(x AS int) + 400_000, rnd_str(5, 16, 2)," +
                    " timestamp_sequence('" + DAY + "T02:00:00', 1_000_000L) ts, x v FROM long_sequence(200)");
            drainWalQueue();
            makeComposite("T01:00:00");
            makeComposite("T11:00:00");

            assertFoldPreservesTheDay();
        });
    }

    /**
     * The column exists only in the SOURCES, so the composite target carries no data for it at all and the
     * fold has to write the target's share of it as NULLs.
     */
    @Test
    public void testAColumnOnlyTheSourcesHaveSurvivesTheFold() throws Exception {
        assertMemoryLeak(() -> {
            createSplitDay();
            execute("ALTER TABLE x ADD COLUMN v LONG");
            drainWalQueue();
            // Rows carrying v land in the LAST sibling only, well past anything the target holds.
            execute("INSERT INTO x (i, s, ts, v) SELECT cast(x AS int) + 400_000, rnd_str(5, 16, 2)," +
                    " timestamp_sequence('" + DAY + "T11:00:00', 1_000_000L) ts, x v FROM long_sequence(200)");
            drainWalQueue();
            makeComposite("T01:00:00");

            Assert.assertTrue("fixture left the target plain", isComposite(DAY));
            final long vRows = scalar("SELECT count() FROM x WHERE ts IN '" + DAY + "' AND v IS NOT NULL");
            final long vSum = scalar("SELECT sum(v) FROM x WHERE ts IN '" + DAY + "'");
            assertFoldPreservesTheDay();
            Assert.assertEquals("the fold lost rows of the added column", vRows,
                    scalar("SELECT count() FROM x WHERE ts IN '" + DAY + "' AND v IS NOT NULL"));
            Assert.assertEquals("the fold changed the added column's values", vSum,
                    scalar("SELECT sum(v) FROM x WHERE ts IN '" + DAY + "'"));
        });
    }

    @Test
    public void testColdSmallTargetCompactsWithIndexedVariableColumnsAndPinnedReader() throws Exception {
        assertMemoryLeak(() -> {
            createSplitDay();
            execute("ALTER TABLE x ADD COLUMN v VARCHAR");
            execute("ALTER TABLE x ADD COLUMN sym SYMBOL INDEX");
            drainWalQueue();
            execute("INSERT INTO x (i, ts, v, sym) SELECT x::INT, timestamp_sequence('2024-01-01T02:00:00', 1_000_000L), 'value-' || x, 'prefix' FROM long_sequence(200)");
            drainWalQueue();
            makeComposite("T01:00:00");
            makeComposite("T11:00:00");
            Assert.assertTrue(isComposite(DAY));
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1G");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MAX_SPLITS, 1);
            final String before = fingerprintOfDay();
            final long dayRows = rowsOfDay();
            final long lastSourceRows;
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                lastSourceRows = reader.getTxFile().getPartitionSize(2);
            }
            final long sumBefore = scalar("SELECT sum(i) FROM x");
            final long oldNameTxn;
            final TableToken token = engine.verifyTableName("x");
            final long writtenBefore = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
            try (TableReader pinned = engine.getReader(token)) {
                oldNameTxn = pinned.getTxFile().getPartitionNameTxn(0);
                final long oldRows = pinned.size();
                Assert.assertTrue(pinned.getGeometry().getWriterTxn(0) < pinned.getTxn());
                execute("INSERT INTO x (i, ts) VALUES (999, '2024-01-05')");
                drainWalQueue();
                final TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(pinned);
                long count = 0;
                long sum = 0;
                while (cursor.hasNext()) {
                    count++;
                    sum += cursor.getRecord().getInt(0);
                }
                Assert.assertEquals("the old snapshot must survive target rewrite and squash", oldRows, count);
                Assert.assertEquals(sumBefore, sum);
            }
            Assert.assertEquals("smallest-pair packing, target cleanup and trigger row", dayRows + lastSourceRows + 1,
                    node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - writtenBefore);
            Assert.assertEquals(1, partitionCountOfDay());
            Assert.assertFalse("the small cold accumulator must become plain", isComposite(DAY));
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertNotEquals(oldNameTxn, reader.getTxFile().getPartitionNameTxn(0));
            }
            Assert.assertEquals(before, fingerprintOfDay());
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery("SELECT count() c FROM x WHERE sym = 'prefix' AND ts IN '2024-01-01'")
                    .noRandomAccess().expectSize().returns("c\n200\n");
            assertQuery("SELECT count(v) c FROM x WHERE ts IN '2024-01-01'")
                    .noRandomAccess().expectSize().returns("c\n200\n");
            assertDayReadsBack();
        });
    }

    @Test
    public void testColdSmallTargetRewriteOpenFailureLeavesOriginalReadableAndRetries() throws Exception {
        final AtomicBoolean isArmed = new AtomicBoolean();
        final var ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.containsAscii(name, "2024-01-01.") && Utf8s.endsWithAscii(name, ".d")
                        && isArmed.compareAndSet(true, false)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            // Pooled frame columns otherwise retain the facade from preceding tests.
            engine.resetFrameFactory();
            createSingleDaySplit();
            makeComposite("T01:00:00");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1G");
            execute("INSERT INTO x (i, ts) VALUES (123, '2024-01-01T12:00:00')");
            drainWalQueue();
            final String before = fingerprintOfDay();
            isArmed.set(true);
            try (TableWriter writer = getWriter(engine.verifyTableName("x"))) {
                try {
                    writer.squashAllPartitionsIntoOne();
                    Assert.fail("the target rewrite must hit the injected open failure");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getFlyweightMessage().toString().contains("could not open read-write"));
                }
                Assert.assertFalse("the injected failure must have fired", isArmed.get());
                Assert.assertTrue(isComposite(DAY));
                Assert.assertEquals(3, partitionCountOfDay());
                Assert.assertEquals(before, fingerprintOfDay());
                writer.squashAllPartitionsIntoOne();
            }
            Assert.assertFalse(isComposite(DAY));
            Assert.assertEquals(1, partitionCountOfDay());
            Assert.assertEquals(before, fingerprintOfDay());
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            Assert.assertEquals(before, fingerprintOfDay());
            engine.releaseAllReaders();
            engine.resetFrameFactory();
        });
    }

    @Test
    public void testCompactingAnEarlierDayDoesNotMakeTheLastCommitsTargetCold() throws Exception {
        assertMemoryLeak(() -> {
            createSingleDaySplit();
            execute("INSERT INTO x (i, ts) SELECT x::INT, timestamp_sequence('2024-01-02', 1_000_000L) FROM long_sequence(20_000)");
            drainWalQueue();
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, false);
            execute("INSERT INTO x (i, ts) SELECT x::INT + 200_000, timestamp_sequence('2024-01-02T05:00:00', 1_000L) FROM long_sequence(200)");
            drainWalQueue();
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, true);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1G");
            makeComposite("T01:00:00");
            execute("INSERT INTO x (i, ts) SELECT x::INT + 300_000, timestamp_sequence('2024-01-02T01:00:00', 1_000_000L) FROM long_sequence(200)");
            drainWalQueue();
            Assert.assertTrue(isComposite(DAY));
            Assert.assertTrue(isComposite("2024-01-02"));
            final String firstDayBefore = fingerprintOfDay();
            final long secondDaySum = scalar("SELECT sum(i) FROM x WHERE ts IN '2024-01-02'");
            final long secondDayNameTxn;
            final long secondDayTs = MicrosTimestampDriver.floor("2024-01-02T00:00:00.000000Z");
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                Assert.assertEquals(5, reader.getPartitionCount());
                secondDayNameTxn = reader.getTxFile().getPartitionNameTxn(reader.getTxFile().getPartitionIndex(secondDayTs));
            }
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MAX_SPLITS, 1);
            execute("INSERT INTO x (i, ts) VALUES (777, '2024-01-02T01:00:00'), (888, '2024-01-03')");
            drainWalQueue();
            Assert.assertFalse("the earlier cold target must compact", isComposite(DAY));
            Assert.assertTrue("maintenance txns must not make the updated target cold", isComposite("2024-01-02"));
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                Assert.assertEquals(3, reader.getPartitionCount());
                Assert.assertEquals(secondDayNameTxn, reader.getTxFile().getPartitionNameTxn(reader.getTxFile().getPartitionIndex(secondDayTs)));
            }
            Assert.assertEquals(firstDayBefore, fingerprintOfDay());
            assertQuery("SELECT sum(i) s FROM x WHERE ts IN '2024-01-02'")
                    .noRandomAccess().expectSize().returns("s\n" + (secondDaySum + 777) + "\n");
            assertDayReadsBack();
        });
    }

    @Test
    public void testHotSmallSquashTargetStaysComposite() throws Exception {
        assertMemoryLeak(() -> {
            createSingleDaySplit();
            makeComposite("T01:00:00");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1G");
            final String before = fingerprintOfDay();
            final long sourceRows;
            final long oldNameTxn;
            final TableToken token = engine.verifyTableName("x");
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals("fixture must update the target in the last commit",
                        reader.getTxn(), reader.getGeometry().getWriterTxn(0));
                sourceRows = rowsOfDay() - reader.getTxFile().getPartitionSize(0);
                oldNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }
            final long writtenBefore = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
            try (TableWriter writer = getWriter(token)) {
                writer.squashAllPartitionsIntoOne();
            }
            Assert.assertEquals(sourceRows, node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - writtenBefore);
            Assert.assertTrue(isComposite(DAY));
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals(oldNameTxn, reader.getTxFile().getPartitionNameTxn(0));
            }
            Assert.assertEquals(before, fingerprintOfDay());
            assertDayReadsBack();
        });
    }

    @Test
    public void testSmallColdTargetIsNotCompactedWhenOnlySourceIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            createLastDaySplit();
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1G");
            final String before = fingerprintOfDay();
            final long writtenBefore = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
            try (TableWriter writer = getWriter(engine.verifyTableName("x"))) {
                writer.squashAllPartitionsIntoOne();
            }
            Assert.assertEquals(writtenBefore, node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows());
            Assert.assertEquals(2, partitionCountOfDay());
            Assert.assertTrue(isComposite(DAY));
            Assert.assertEquals(before, fingerprintOfDay());
        });
    }

    @Test
    public void testSquashTargetAtDiskSizeLimitStaysComposite() throws Exception {
        assertMemoryLeak(() -> checkSquashTargetDiskSize(false));
    }

    @Test
    public void testSquashTargetBelowDiskSizeLimitBecomesPlain() throws Exception {
        assertMemoryLeak(() -> checkSquashTargetDiskSize(true));
    }

    @Test
    public void testSquashTargetSizeLimitDoesNotOverflow() throws Exception {
        assertMemoryLeak(() -> {
            createSingleDaySplit();
            makeComposite("T01:00:00");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, Long.MAX_VALUE);
            execute("INSERT INTO x (i, ts) VALUES (123, '2024-01-01T12:00:00')");
            drainWalQueue();
            final String before = fingerprintOfDay();
            try (TableWriter writer = getWriter(engine.verifyTableName("x"))) {
                writer.squashAllPartitionsIntoOne();
            }
            Assert.assertFalse("four times the configured split size must not overflow", isComposite(DAY));
            Assert.assertEquals(1, partitionCountOfDay());
            Assert.assertEquals(before, fingerprintOfDay());
        });
    }

    @Test
    public void testCompositeSourceIntoCompositeTarget() throws Exception {
        assertMemoryLeak(() -> {
            createSplitDay();
            makeComposite("T01:00:00");   // the front sibling: the fold's target
            makeComposite("T11:00:00");   // the last sibling: one of its sources

            Assert.assertTrue("fixture left the target plain", isComposite(DAY));
            assertFoldPreservesTheDay();
        });
    }

    @Test
    public void testCompositeSourceIntoPlainTarget() throws Exception {
        assertMemoryLeak(() -> {
            createSplitDay();
            // Only a later sibling is made composite, so the front one - the fold's target - stays plain
            // and the result collapses back to the ordinary single-piece shape.
            makeComposite("T11:00:00");

            Assert.assertFalse("fixture made the target composite", isComposite(DAY));
            assertFoldPreservesTheDay();
            Assert.assertFalse("folding into a plain target left it composite", isComposite(DAY));
        });
    }

    /**
     * The merged partition's stamp. {@code squashSplitPartitions} folds max(sources' seqTxn) into the
     * target, and a composite source keeps its stamp in {@code _geometry} rather than in the offset-3
     * word. {@code Math.max(0, ...)} floors a -1 read away, so a source whose stamp does not come back
     * would leave the merged partition reporting a seqTxn LOWER than one that wrote rows now inside it,
     * and unchanged across a fold that did change its bytes - both of which
     * {@link io.questdb.cairo.TxReader#getNativePartitionSeqTxn} promises never happens.
     * <p>
     * This holds today, and held before {@code PartitionGeometry.getSeqTxn} started resolving, but only
     * by an accident of ordering: the {@code isComposite} call a few lines above each read has already
     * pulled the slot into the resolved cache, so the old cache-only lookup happened to find it. Moving
     * or dropping that call would have silently dropped the stamp. Pin the outcome, not the ordering.
     */
    @Test
    public void testFoldCarriesACompositeSourcesSeqTxn() throws Exception {
        assertMemoryLeak(() -> {
            createSplitDay();
            // Only the LAST sibling goes composite, so it is a source and the fold's target stays plain -
            // the one shape where the merged stamp is written into the offset-3 word and is observable.
            makeComposite("T11:00:00");

            Assert.assertFalse("fixture made the target composite", isComposite(DAY));
            final long targetSeqTxnBefore = seqTxnOfSibling(0);
            final long sourceSeqTxn = seqTxnOfSibling(2);
            Assert.assertTrue("fixture left the composite source unstamped", sourceSeqTxn > 0);
            Assert.assertTrue("the composite source must out-rank the target or the fold cannot show the drop",
                    sourceSeqTxn > targetSeqTxnBefore);

            assertFoldPreservesTheDay();

            Assert.assertFalse("folding into a plain target left it composite", isComposite(DAY));
            Assert.assertEquals("the fold dropped the composite source's stamp", sourceSeqTxn, seqTxnOfSibling(0));
        });
    }

    /**
     * The squash appends its folded run as one piece starting at the target's file extent E. When the
     * target's own extent piece is also its last by timestamp, that new piece is file-adjacent to it
     * ({@code rowOffset == prevRowOffset + prevRowCount}), hence carries the SAME shift. Every other
     * publish path folds such a pair (O3PartitionJob.foldAdjacentPieces, the carve republish,
     * moveTailToFreshPartition); the squash composite-target branch must too, or it commits geometry
     * no other path ever emits - one extra page frame until the next compaction folds it. The fold
     * must be result-preserving.
     * <p>
     * makeComposite rewrites everything from its insert point to the front sibling's end at the file
     * tail, so that rewritten piece is BOTH the last by timestamp and the one reaching E - exactly the
     * extent-piece-last shape. Folding the later siblings onto it then appends one run at E, adjacent to it.
     */
    @Test
    public void testSquashFoldsAdjacentSameShiftTailPiece() throws Exception {
        assertMemoryLeak(() -> {
            // A single-day fixture: squashAllPartitionsIntoOne folds ALL partitions into one and is not
            // day-aware, so a later day would be merged into this one. With only 2024-01-01 present the
            // squash folds its own siblings alone.
            createSingleDaySplit();
            // Rewrite the FRONT sibling's TAIL (200 rows at 1s ending at its max, 04:59:59) to the file
            // tail, so the piece reaching E is also its last by timestamp - the extent-piece-last shape.
            makeComposite("T04:56:40");
            Assert.assertTrue("fixture left the target plain", isComposite(DAY));

            final String before = fingerprintOfDay();
            final long rowsBefore = rowsOfDay();
            final long sumBefore = scalar("SELECT sum(i) FROM x WHERE ts IN '" + DAY + "'");

            // Drive the REAL non-force squash straight through the writer. It reaches the same
            // composite-target publish branch housekeeping's opportunistic squash does, but does NOT run
            // the compaction net (foldFoldableFolders / foldContiguousPieces) afterwards. That net stands
            // down only while lagRowCount > 0, which a full drainWalQueue never leaves behind, so it would
            // otherwise repair the gap within the same pass and hide it. Driving the squash alone is the
            // one deterministic window in which the squash's own committed geometry is observable.
            final TableToken tt = engine.verifyTableName("x");
            try (TableWriter writer = getWriter(tt)) {
                writer.squashAllPartitionsIntoOne();
            }

            Assert.assertEquals("squash did not fold the day to one directory", 1, partitionCountOfDay());
            Assert.assertTrue("the folded target dropped its composite geometry", isComposite(DAY));

            // RED before the squash fold fix: the tail piece landed at E adjacent to the extent piece.
            assertNoAdjacentSameShiftPair();

            // The fold must be result-preserving.
            Assert.assertEquals("the fold changed the day's row count", rowsBefore, rowsOfDay());
            Assert.assertEquals("the fold changed the day's rows or their order", before, fingerprintOfDay());
            assertQuery("SELECT count(), sum(i) FROM x WHERE ts IN '" + DAY + "'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\tsum\n" + rowsBefore + "\t" + sumBefore + "\n");
            assertDayReadsBack();
        });
    }

    @Test
    public void testPlainSourceIntoCompositeTarget() throws Exception {
        assertMemoryLeak(() -> {
            createSplitDay();
            makeComposite("T01:00:00");

            Assert.assertTrue("fixture left the target plain", isComposite(DAY));
            assertFoldPreservesTheDay();
            // The target keeps its dead space, so it stays composite: collapsing it to the plain shape
            // here would declare the dead rows live.
            Assert.assertTrue("folding into a composite target dropped its geometry", isComposite(DAY));
        });
    }

    /**
     * The fold refuses a composite LAST partition as a source - its file carries {@code lagRowCount} rows
     * past the live ones, belonging to no piece. When that refusal is the only thing on offer the pass
     * appends nothing, and it must then leave the target exactly as it found it. Publishing the plain
     * shape here would declare a composite target's dead rows live.
     */
    @Test
    public void testRefusedFoldLeavesTheTargetsGeometryAlone() throws Exception {
        assertMemoryLeak(() -> {
            createLastDaySplit();
            Assert.assertTrue("fixture left the target plain", isComposite(DAY));
            Assert.assertTrue("fixture lost the split", partitionCountOfDay() > 1);
            final String before = fingerprintOfDay();
            final long rows = rowsOfDay();

            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 1);
            execute("INSERT INTO x (i, s, ts) SELECT cast(x AS int) + 500_000, rnd_str(5, 16, 2)," +
                    " timestamp_sequence('" + DAY + "T23:30:00', 1_000_000L) ts FROM long_sequence(10)");
            drainWalQueue();

            Assert.assertTrue("the refused fold dropped the target's geometry", isComposite(DAY));
            Assert.assertEquals("the refused fold changed the day's row count", rows + 10, rowsOfDay());
            Assert.assertNotEquals("the trigger commit did not reach the day", before, fingerprintOfDay());
            assertDayReadsBack();
        });
    }

    /**
     * Reads the day back two ways that disagree the moment a piece lands at the wrong offset: the row
     * count against a fully ordered read, and the partition catalogue's timestamp bounds against the
     * data's own.
     */
    /**
     * The write-side fold invariant: no committed composite geometry may hold a file-adjacent piece pair.
     * Two list-consecutive pieces are file-adjacent - and so share one shift and one linear page frame -
     * exactly when {@code rowOffset_p == rowOffset_{p-1} + rowCount_{p-1}}. Every publish path folds such
     * pairs before committing; the squash branch must not be the one exception.
     */
    private static void assertNoAdjacentSameShiftPair() throws Exception {
        final long dayLo = MicrosTimestampDriver.floor(DAY + "T00:00:00.000000Z");
        final TableToken tt = engine.verifyTableName("x");
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final PartitionGeometry geometry = reader.getGeometry();
            for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
                if (txReader.getLogicalPartitionTimestamp(txReader.getPartitionTimestampByIndex(i)) != dayLo) {
                    continue;
                }
                final int pieceCount = geometry.getPieceCount(i);
                for (int p = 1; p < pieceCount; p++) {
                    final long prevEnd = geometry.getPieceRowOffset(i, p - 1) + geometry.getPieceRowCount(i, p - 1);
                    Assert.assertNotEquals(
                            "squash committed a file-adjacent same-shift piece pair (fold gap) at partition "
                                    + i + " piece " + p,
                            prevEnd,
                            geometry.getPieceRowOffset(i, p)
                    );
                }
            }
        }
    }

    private static void assertDayReadsBack() throws Exception {
        Assert.assertEquals(
                "the day's timestamps came back unordered",
                rowsOfDay(),
                scalar("SELECT count() FROM (SELECT ts FROM x WHERE ts IN '" + DAY + "' ORDER BY ts)")
        );
        Assert.assertEquals(
                "the partition catalogue disagrees with the data on the day's first timestamp",
                scalar("SELECT min(ts)::long FROM x WHERE ts IN '" + DAY + "'"),
                scalar("SELECT min(minTimestamp)::long FROM table_partitions('x')" +
                        " WHERE name LIKE '" + DAY + "%' AND NOT name LIKE '%.detached'")
        );
        Assert.assertEquals(
                "the partition catalogue disagrees with the data on the day's last timestamp",
                scalar("SELECT max(ts)::long FROM x WHERE ts IN '" + DAY + "'"),
                scalar("SELECT max(maxTimestamp)::long FROM table_partitions('x')" +
                        " WHERE name LIKE '" + DAY + "%' AND NOT name LIKE '%.detached'")
        );
        Assert.assertEquals(
                "the partition catalogue disagrees with the data on the day's row count",
                rowsOfDay(),
                scalar("SELECT sum(numRows) FROM table_partitions('x')" +
                        " WHERE name LIKE '" + DAY + "%' AND NOT name LIKE '%.detached'")
        );
    }

    /**
     * Lowers the split limit, drives one more commit so housekeeping folds, then asserts the day came
     * back with fewer directories and the same rows in the same order.
     */
    private static void assertFoldPreservesTheDay() throws Exception {
        final long siblingsBefore = partitionCountOfDay();
        Assert.assertTrue("fixture produced nothing to fold", siblingsBefore > 1);
        final String before = fingerprintOfDay();
        final long rowsBefore = rowsOfDay();

        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MAX_SPLITS, 1);
        // A row in a LATER day, so the commit touches nothing in the day under test and only its
        // housekeeping can account for what changes there.
        execute("INSERT INTO x (i, s, ts) VALUES (999, 'z', '2024-01-05T00:00:00.000000Z')");
        drainWalQueue();

        // All the way down to the limit, not merely fewer: stopping short would mean the pass refused a
        // sibling, and refusing a composite one is exactly the bug this suite is here for.
        Assert.assertEquals(
                "housekeeping did not fold the day to the split limit, from " + siblingsBefore + " siblings",
                1,
                partitionCountOfDay()
        );
        Assert.assertEquals("the fold changed the day's row count", rowsBefore, rowsOfDay());
        Assert.assertEquals("the fold changed the day's rows or their order", before, fingerprintOfDay());
        assertDayReadsBack();
    }

    private static void checkSquashTargetDiskSize(boolean isBelowLimit) throws Exception {
        createSingleDaySplit();
        makeComposite("T01:00:00");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1G");
        // Leave the target cold but the last source plain, so squash can consume it.
        execute("INSERT INTO x (i, ts) VALUES (123, '2024-01-01T12:00:00')");
        drainWalQueue();
        final String before = fingerprintOfDay();
        final long dayRows = rowsOfDay();
        final long targetRows;
        final TableToken token = engine.verifyTableName("x");
        final var ff = engine.getConfiguration().getFilesFacade();
        try (TableReader reader = engine.getReader(token);
             Path path = new Path().of(engine.getConfiguration().getDbRoot()).concat(token)) {
            targetRows = reader.getTxFile().getPartitionSize(0);
            Assert.assertTrue(reader.getGeometry().getWriterTxn(0) < reader.getTxn());
            TableUtils.setPathForNativePartition(path, reader.getMetadata().getTimestampType(), reader.getPartitionedBy(),
                    reader.getTxFile().getPartitionTimestampByIndex(0), reader.getTxFile().getPartitionNameTxn(0));
            final long diskSize = ff.getDirSize(path);
            final long splitSize = diskSize / 4 + 65_536;
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, splitSize);
            final long paddingSize = splitSize * 4 - diskSize - (isBelowLimit ? 1 : 0);
            final int partitionPathSize = path.size();
            final long fd = TableUtils.openRW(ff, path.concat("size.pad").$(), LOG, 0);
            try {
                Assert.assertTrue(ff.truncate(fd, paddingSize));
            } finally {
                ff.close(fd);
            }
            path.trimTo(partitionPathSize);
            Assert.assertEquals(splitSize * 4 - (isBelowLimit ? 1 : 0), ff.getDirSize(path));
        }
        final long writtenBefore = node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
        try (TableWriter writer = getWriter(token)) {
            writer.squashAllPartitionsIntoOne();
        }
        Assert.assertEquals("the limit must use actual directory size, including padding, not live rows",
                isBelowLimit ? dayRows : dayRows - targetRows,
                node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - writtenBefore);
        Assert.assertEquals(!isBelowLimit, isComposite(DAY));
        Assert.assertEquals(1, partitionCountOfDay());
        Assert.assertEquals(before, fingerprintOfDay());
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        Assert.assertEquals(before, fingerprintOfDay());
        assertDayReadsBack();
    }

    /**
     * A day whose split siblings are the LAST partitions of the table, with the final one composite -
     * the shape the fold has to refuse.
     */
    private static void createLastDaySplit() throws Exception {
        execute("CREATE TABLE x AS (" +
                "SELECT cast(x AS int) i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('" + DAY + "', 1_000_000L) ts" +
                " FROM long_sequence(20_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalQueue();
        splitTheDayAt("T05:00:00");
        Assert.assertEquals("fixture did not split the day in two", 2, partitionCountOfDay());
        makeComposite("T01:00:00");
        makeComposite("T05:10:00");
    }

    /**
     * The day cut into three siblings, with NO later day: the whole table is 2024-01-01. Used by the
     * squashAllPartitionsIntoOne path, which folds every partition into one and would otherwise pull a
     * later day into the day under test.
     */
    private static void createSingleDaySplit() throws Exception {
        execute("CREATE TABLE x AS (" +
                "SELECT cast(x AS int) i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('" + DAY + "', 1_000_000L) ts" +
                " FROM long_sequence(20_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalQueue();

        splitTheDayAt("T05:00:00");
        execute("INSERT INTO x SELECT cast(x AS int) + 20_000 i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('" + DAY + "T05:33:21', 1_000_000L) ts FROM long_sequence(20_000)");
        drainWalQueue();
        splitTheDayAt("T10:30:00");
        Assert.assertEquals("fixture did not split the day into three", 3, partitionCountOfDay());
    }

    /**
     * A day cut into three siblings, pushed off the end of the table so none of them is the last
     * partition and every one of them is foldable.
     */
    private static void createSplitDay() throws Exception {
        execute("CREATE TABLE x AS (" +
                "SELECT cast(x AS int) i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('" + DAY + "', 1_000_000L) ts" +
                " FROM long_sequence(20_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalQueue();

        // Only the LAST partition ever splits, and only when the rows ahead of the O3 batch outnumber
        // everything behind it two to one - so each cut goes near the end of what the day holds so far,
        // and the day grows by an in-order append before the next one.
        splitTheDayAt("T05:00:00");
        execute("INSERT INTO x SELECT cast(x AS int) + 20_000 i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('" + DAY + "T05:33:21', 1_000_000L) ts FROM long_sequence(20_000)");
        drainWalQueue();
        splitTheDayAt("T10:30:00");
        Assert.assertEquals("fixture did not split the day into three", 3, partitionCountOfDay());

        execute("INSERT INTO x SELECT cast(x AS int) + 100_000 i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('2024-01-03', 1_000_000L) ts FROM long_sequence(1_000)");
        drainWalQueue();
    }

    /**
     * Content fingerprint of one day IN CURSOR ORDER, so a piece appended at the wrong offset moves it
     * even when the row count holds.
     */
    private static String fingerprintOfDay() throws Exception {
        long count = 0;
        long hash = 0;
        try (RecordCursorFactory f = select("SELECT ts, i, s FROM x WHERE ts IN '" + DAY + "'")) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                while (c.hasNext()) {
                    count++;
                    hash = hash * 1_000_003L + c.getRecord().getLong(0);
                    hash = hash * 1_000_003L + c.getRecord().getInt(1);
                    final CharSequence s = c.getRecord().getStrA(2);
                    hash = hash * 1_000_003L + (s == null ? -1 : s.hashCode());
                }
            }
        }
        return count + "/" + hash;
    }

    /**
     * Whether the day's OWN (front) partition is flagged composite.
     */
    private static boolean isComposite(String day) throws Exception {
        final TableToken tt = engine.verifyTableName("x");
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(MicrosTimestampDriver.floor(day + "T00:00:00.000000Z"));
            return partitionIndex > -1 && txReader.isPartitionComposite(partitionIndex);
        }
    }

    /**
     * Two narrow backdated strides over the same rows: merge-append rewrites the owning piece at the
     * shared file tail and abandons the previous copy, which is what leaves dead space behind.
     */
    private static void makeComposite(String timeOfDay) throws Exception {
        for (int i = 0; i < 2; i++) {
            execute("INSERT INTO x (i, s, ts) SELECT cast(x AS int) + 300_000, rnd_str(5, 16, 2)," +
                    " timestamp_sequence('" + DAY + timeOfDay + "', 1_000_000L) ts FROM long_sequence(200)");
            drainWalQueue();
        }
    }

    /**
     * Attached partitions of the day under test; a detached directory still shows up here, so exclude it.
     */
    private static long partitionCountOfDay() throws Exception {
        return scalar("SELECT count() FROM table_partitions('x') WHERE name LIKE '" + DAY + "%'" +
                " AND NOT name LIKE '%.detached'");
    }

    /**
     * The resolved seqTxn of the day's sibling at {@code ordinal}: a composite one keeps its stamp in
     * {@code _geometry}, a plain one in the offset-3 word. Resolves the geometry first, so what the
     * fixture reads back does not itself depend on the accessor under test.
     */
    private static long seqTxnOfSibling(int ordinal) throws Exception {
        final long dayLo = MicrosTimestampDriver.floor(DAY + "T00:00:00.000000Z");
        final TableToken tt = engine.verifyTableName("x");
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            int seen = 0;
            for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
                if (txReader.getLogicalPartitionTimestamp(txReader.getPartitionTimestampByIndex(i)) != dayLo) {
                    continue;
                }
                if (seen++ != ordinal) {
                    continue;
                }
                if (!txReader.isPartitionComposite(i)) {
                    return txReader.getNativePartitionSeqTxn(i);
                }
                reader.getGeometry().getPieceCount(i);
                return reader.getGeometry().getSeqTxn(i);
            }
        }
        return -1;
    }

    private static long rowsOfDay() throws Exception {
        return scalar("SELECT count() FROM x WHERE ts IN '" + DAY + "'");
    }

    private static long scalar(String sql) throws Exception {
        try (RecordCursorFactory f = select(sql)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                Assert.assertTrue("query returned no row: " + sql, c.hasNext());
                return c.getRecord().getLong(0);
            }
        }
    }

    /**
     * Cuts a sibling off the tail of the day with an O3 write landing well inside it. Merge-append would
     * rewrite the piece in place instead, so it is off for the duration of the cut.
     */
    private static void splitTheDayAt(String timeOfDay) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        execute("INSERT INTO x (i, s, ts) SELECT cast(x AS int) + 200_000, rnd_str(5, 16, 2)," +
                " timestamp_sequence('" + DAY + timeOfDay + "', 1_000L) ts FROM long_sequence(200)");
        drainWalQueue();
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
    }
}
