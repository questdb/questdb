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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

/**
 * Reproduction for the split-partition-removal branch that lacks the composite guard its sibling has
 * (TableWriter.o3ConsumePartitionUpdateSink_processSplitPartitionRemoval, the else branch that carves one
 * line off the parent). The sibling at the trim-column-tops call site guards with
 * {@code if (!partitionMutates && !isComposite)}; this branch does not.
 * <p>
 * The shape: a MOVE-TAIL leaves the day's front folder COMPOSITE (one piece at file row 0, dead space
 * above it) and copies the messy tail into a fresh sibling split at a higher timestamp within the same
 * day. A later REPLACE RANGE that empties that sibling drives
 * {@code processSplitPartitionRemoval} with the composite front as the "parent". The branch reduces the
 * front's row-count word with {@code updatePartitionSizeByTimestamp} but leaves the front's geometry
 * record untouched, so the piece row-count sum and the committed size word diverge - a composite
 * partition whose {@code _geometry} record over-counts its live rows.
 * <p>
 * MAKE-PLAIN happens to repair the size word (it trims to {@code getPartitionSize}), but only once a
 * pinned reader releases; a forced REWRITE (SQUASH / DETACH / CONVERT PARTITION TO PARQUET) or a
 * subsequent merge-append that carries the over-counted piece forward would resurrect the carved-off
 * rows as duplicates. The invariant assertion here is the deterministic red signal, independent of which
 * downstream path later trusts the geometry.
 */
public class O3PartitionSplitRemovalCompositeParentTest extends AbstractCairoTest {

    private static final String DAY = "2024-01-01";

    @Test
    public void testReplaceEmptyingMoveTailSiblingKeepsCompositeFrontConsistent() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            // Housekeeping on, with the scaled piece-count cap out of the way and the pre-split allowed to
            // cut - mirrors O3PartitionCompactionTest's MOVE-TAIL fixture.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 16);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_TIME, 0);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_MOVE_TAIL_MIN_GAIN, 1);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

            execute("CREATE TABLE x AS (SELECT cast(x AS int) i," +
                    " timestamp_sequence('" + DAY + "', 1000000L) ts" +
                    " FROM long_sequence(20000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            // Three rewrites of one 200-row stride near the end of the clean front, each relocating that
            // stride to the file tail and leaving its old copy dead - the shape MOVE-TAIL is for.
            for (int i = 0; i < 3; i++) {
                execute("INSERT INTO x SELECT cast(x AS int) + 500000 i," +
                        " timestamp_sequence('" + DAY + "T05:00:00', 1000000L) ts FROM long_sequence(200)");
                drainWalQueue();
            }
            // Put the piece-count rule in play at exactly 2 pieces, so the observed pass trips it.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, Long.MAX_VALUE / 8);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 2);

            final TableToken xt = engine.verifyTableName("x");
            final long dayEnd = MicrosTimestampDriver.floor("2024-01-02T00:00:00.000000Z");

            // A reader pinned across the whole scenario keeps MAKE-PLAIN declining, so the front stays
            // composite - both while MOVE-TAIL runs and while the replace below drives the removal branch.
            try (TableReader pinned = engine.getReader(xt)) {
                Assert.assertNotNull(pinned);
                runCompactionPasses();

                Assert.assertTrue("fixture did not reach MOVE-TAIL: front not composite", isCompositeDay());
                Assert.assertEquals("MOVE-TAIL front must be one piece at row 0", 1, pieceCountOfDay());
                Assert.assertEquals("MOVE-TAIL must leave the day as front + one sibling split", 2, partitionCountOfDay());
                Assert.assertTrue("MOVE-TAIL left no dead space to protect", deadRowsOfDay() > 0);

                // The sibling split is the tail MOVE-TAIL copied out - strictly later in time than the front.
                final long siblingMinTs = scalar("SELECT minTimestamp FROM table_partitions('x')" +
                        " WHERE name LIKE '" + DAY + "%' ORDER BY minTimestamp DESC LIMIT 1");
                final long siblingRows = scalar("SELECT numRows FROM table_partitions('x')" +
                        " WHERE name LIKE '" + DAY + "%' ORDER BY minTimestamp DESC LIMIT 1");
                Assert.assertTrue("sibling split has no rows to remove", siblingRows > 0);

                final long totalBefore = scalar("SELECT count() FROM x");

                // REPLACE RANGE covering exactly the sibling's rows, inserting none: empties the sibling and
                // drives processSplitPartitionRemoval with the still-composite front as the parent.
                try (WalWriter ww = engine.getWalWriter(xt)) {
                    ww.commitWithParams(siblingMinTs, dayEnd, WAL_DEDUP_MODE_REPLACE_RANGE);
                    ww.commit();
                }
                drainWalQueue();

                Assert.assertFalse(
                        "table suspended after emptying the MOVE-TAIL sibling",
                        engine.getTableSequencerAPI().isSuspended(xt)
                );

                // The front is still composite (MAKE-PLAIN still declined under the pinned reader). Its
                // committed row-count word and its geometry piece row-count sum MUST agree - the composite
                // invariant. The removal branch reduced the size word without touching the geometry, so
                // they diverge by the rows it carved off.
                assertCompositeFrontSizeMatchesGeometry();

                // The reader clamps at the size word, so the query row count stays correct even while the
                // geometry over-counts - which is why this is latent until a geometry-trusting path runs.
                Assert.assertEquals(
                        "row count changed by more than the emptied sibling",
                        totalBefore - siblingRows,
                        scalar("SELECT count() FROM x")
                );

                // The user-visible consequence: a merge-append into the still-composite front carries its
                // single piece forward. If that piece over-counts its rows (geometry left stale), the MERGE
                // copies the carved-off row again and it resurfaces in both the front and the carved split -
                // a duplicate. With the geometry republished at the reduced count, exactly one new row lands.
                try (WalWriter ww = engine.getWalWriter(xt)) {
                    TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor(DAY + "T03:00:00.000000Z"));
                    row.putInt(0, 424242);
                    row.append();
                    ww.commit();
                }
                drainWalQueue();
                Assert.assertFalse(
                        "table suspended after merge-append into the composite front",
                        engine.getTableSequencerAPI().isSuspended(xt)
                );
                Assert.assertEquals(
                        "merge-append into the composite front resurrected the carved-off row as a duplicate",
                        totalBefore - siblingRows + 1,
                        scalar("SELECT count() FROM x")
                );
            }
        });
    }

    private static void assertCompositeFrontSizeMatchesGeometry() {
        final TableToken tt = engine.verifyTableName("x");
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(dayFloor());
            Assert.assertTrue("day partition vanished", partitionIndex > -1);
            if (!txReader.isPartitionComposite(partitionIndex)) {
                // Repaired to plain: nothing to reconcile.
                return;
            }
            final PartitionGeometry geometry = reader.getGeometry();
            long pieceSum = 0;
            for (int p = 0, n = geometry.getPieceCount(partitionIndex); p < n; p++) {
                pieceSum += geometry.getPieceRowCount(partitionIndex, p);
            }
            Assert.assertEquals(
                    "composite front's geometry piece-row-count sum diverged from its committed size word"
                            + " [size=" + txReader.getPartitionSize(partitionIndex) + ", pieceSum=" + pieceSum + ']',
                    txReader.getPartitionSize(partitionIndex),
                    pieceSum
            );
        }
    }

    private static long dayFloor() {
        try {
            return MicrosTimestampDriver.floor(DAY + "T00:00:00.000000Z");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static long deadRowsOfDay() throws Exception {
        return scalar("SELECT coalesce(sum(deadRows), 0) d FROM table_partitions('x') WHERE name LIKE '" + DAY + "%'");
    }

    private static boolean isCompositeDay() {
        final TableToken tt = engine.verifyTableName("x");
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(dayFloor());
            return partitionIndex > -1 && txReader.isPartitionComposite(partitionIndex);
        }
    }

    private static long partitionCountOfDay() throws Exception {
        return scalar("SELECT count() c FROM table_partitions('x') WHERE name LIKE '" + DAY + "%'");
    }

    private static long pieceCountOfDay() {
        final TableToken tt = engine.verifyTableName("x");
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(dayFloor());
            return partitionIndex > -1 ? reader.getGeometry().getPieceCount(partitionIndex) : 0;
        }
    }

    /**
     * Compaction runs inside TableWriter.housekeep, once per commit. A handful of small unrelated commits,
     * each into a day of its own, give it several chances to act without adding waste of their own.
     */
    private static void runCompactionPasses() throws Exception {
        for (int i = 0; i < 6; i++) {
            execute("INSERT INTO x SELECT cast(x AS int) + 800000 + " + (i * 10) + " i," +
                    " timestamp_sequence('2024-03-" + String.format("%02d", 1 + i) + "', 60000000L) ts" +
                    " FROM long_sequence(2)");
            drainWalQueue();
        }
        engine.releaseInactive();
    }

    private static long scalar(String sql) throws Exception {
        try (RecordCursorFactory f = select(sql)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                Assert.assertTrue("query returned no row: " + sql, c.hasNext());
                return c.getRecord().getLong(0);
            }
        }
    }
}
