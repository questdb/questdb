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

package io.questdb.test.cairo.o3;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CompositePartitionMerger;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * End-to-end coverage for the idle-triggered composite (multi-piece) partition compaction:
 * {@link CompositePartitionMerger#merge} (the build step, off a {@link TableReader} snapshot) and
 * {@link TableWriter#swapCompositePartition} (the swap, on the writer thread).
 * <p>
 * Uses the same table shape and oracle idiom as {@link O3CompositePartitionTest}: a WAL table under test,
 * a plain {@code BYPASS WAL} reference table assembled from the same batches via {@code UNION ALL}, and
 * direct geometry assertions off the reader's own {@code TxFile}/{@link PartitionGeometry}.
 */
public class CompositePartitionCompactionTest extends AbstractCairoTest {

    /**
     * End-to-end idle compaction: builds a composite partition with an indexed column, merges and swaps it
     * directly (no scheduler exists yet), and checks three independent things - the rows, the geometry, and
     * the indexed lookup.
     * <p>
     * {@code COMPOSITE_PARTITION_STATE.md} section 26 documents a gap where composite
     * {@code MERGE}/{@code NEW_PIECE} writes never maintain a BITMAP index. Verified empirically against
     * this branch's current HEAD before writing this test: that gap does not reproduce here.
     * {@code FrameAlgebra.append}/{@code merge} - what the composite executor actually calls for both action
     * types - dispatch to {@code ContiguousFileIndexedFrameColumn} for an indexed column, which re-derives
     * and posts fresh index entries at a row's new physical position as part of the same write. Section 26
     * is stale for this column type (it is consistent with the status table's own item 13, "Index
     * maintenance for merged rows - BUILT", which contradicts section 26's later, apparently out of date,
     * claim). This test's indexed lookup on {@code TARGET} - a value that exists ONLY in the rows the
     * backdated batch writes - therefore reads back correctly both BEFORE and AFTER compaction; the
     * assertion here is that the mandatory post-swap index rebuild does not regress an already-correct
     * index, not that it fixes a broken one.
     */
    @Test
    public void testEndToEndIdleCompactionFixesRowsGeometryAndIndex() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

            // One day at 15s, so the partition holds 5760 rows before anything backdated lands.
            final String base = "SELECT x::INT i, ('k-' || (x % 200))::SYMBOL symi," +
                    " timestamp_sequence('2020-02-03', 15*1000000L) ts FROM long_sequence(5760)";
            // A later day, so 2020-02-03 is never the active partition and the backfill below goes
            // through the O3 path rather than an append to the open one.
            final String nextDay = "SELECT x::INT + 90000 i, ('k-' || (x % 200))::SYMBOL symi," +
                    " timestamp_sequence('2020-02-06', 60*1000000L) ts FROM long_sequence(50)";
            execute("CREATE TABLE x AS (" + base + "), INDEX(symi CAPACITY 128) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x " + nextDay);
            drainWalQueue();

            // TARGET exists ONLY in these 20 rows, all written by the composite path.
            final String backfill = "SELECT x::INT + 70000 i," +
                    " (CASE WHEN x <= 20 THEN 'TARGET' ELSE 'k-' || (x % 200) END)::SYMBOL symi," +
                    " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts FROM long_sequence(200)";
            execute("INSERT INTO x " + backfill);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            Assert.assertFalse("the composite write suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            final long partitionTimestamp;
            final long liveRowsBefore;
            try (TableReader reader = engine.getReader(xt)) {
                partitionTimestamp = reader.getPartitionTimestampByIndex(0);
                liveRowsBefore = reader.getTxFile().getPartitionSize(0);
                Assert.assertTrue("the partition should have been cut into pieces",
                        reader.getGeometry().getPieceCount(0) > 1);
                Assert.assertTrue("the partition should be composite", reader.getTxFile().isPartitionComposite(0));
            }
            Assert.assertEquals(5960, liveRowsBefore);

            // The oracle: the same rows, assembled without ever touching the composite machinery.
            execute("CREATE TABLE o AS (SELECT i, symi::SYMBOL symi, ts FROM (" +
                    base + " UNION ALL " + nextDay + " UNION ALL " + backfill +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            // The indexed lookup, BEFORE compaction: already correct (see the class javadoc for why this
            // is not the documented gap), and re-asserted after the swap so the mandatory rebuild is proven
            // not to regress it.
            assertQuery("SELECT count() c FROM o WHERE symi = 'TARGET'").noRandomAccess().expectSize().returns("c\n20\n");
            assertQuery("SELECT count() c FROM x WHERE symi = 'TARGET'")
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Index")
                    .returns("c\n20\n");

            // Build and swap, directly - no scheduler exists yet.
            final CompositePartitionMerger.MergeResult mergeResult =
                    CompositePartitionMerger.merge(engine, xt, partitionTimestamp);
            Assert.assertNotNull("expected a composite partition to merge", mergeResult);
            Assert.assertTrue(mergeResult.getSnapshotPieceCount() > 1);

            try (TableWriter writer = engine.getWriter(xt, "compaction test")) {
                final int swapResult = writer.swapCompositePartition(mergeResult);
                Assert.assertEquals(TableWriter.SWAP_OK, swapResult);
            }

            // The staging directory is gone - either renamed into place (success) or removed - and the OLD
            // directory (the one the pre-compaction nameTxn named) no longer exists either.
            assertStagingDirGone(xt, mergeResult);

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(xt)) {
                Assert.assertEquals("row count must be unchanged by compaction",
                        liveRowsBefore, reader.getTxFile().getPartitionSize(0));
                Assert.assertEquals("a fully compacted partition should read back as one piece",
                        1, reader.getGeometry().getPieceCount(0));
                Assert.assertNotEquals("the partition directory must have a new name txn",
                        mergeResult.getOldNameTxn(), reader.getTxFile().getPartitionNameTxn(0));
                assertOldPartitionDirGone(xt, partitionTimestamp, mergeResult.getOldNameTxn());
            }

            // Rows: the same UNION ALL oracle as before compaction, now over the merged partition.
            TestUtils.assertSqlCursors(
                    engine, sqlExecutionContext, "SELECT * FROM o ORDER BY ts, i", "SELECT * FROM x ORDER BY ts, i", LOG
            );

            // The index: still correct after the swap's mandatory rebuild, using the index the same way
            // the pre-compaction assertion above did.
            assertQuery("SELECT count() c FROM x WHERE symi = 'TARGET'")
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Index")
                    .returns("c\n20\n");
        });
    }

    /**
     * A merge snapshot goes stale when a fresh WAL insert lands another piece on the same partition before
     * the swap runs. The swap must detect this, discard the staging directory, leave the table's real state
     * completely untouched, and let the caller retry with a fresh {@link CompositePartitionMerger#merge}.
     */
    @Test
    public void testStaleSwapIsRejectedAndRetrySucceeds() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

            final String base = "SELECT x::INT i, ('k-' || (x % 200))::SYMBOL symi," +
                    " timestamp_sequence('2020-02-03', 15*1000000L) ts FROM long_sequence(5760)";
            final String nextDay = "SELECT x::INT + 90000 i, ('k-' || (x % 200))::SYMBOL symi," +
                    " timestamp_sequence('2020-02-06', 60*1000000L) ts FROM long_sequence(50)";
            execute("CREATE TABLE x AS (" + base + "), INDEX(symi CAPACITY 128) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x " + nextDay);
            drainWalQueue();

            final String backfill1 = "SELECT x::INT + 70000 i, ('k-' || (x % 200))::SYMBOL symi," +
                    " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts FROM long_sequence(200)";
            execute("INSERT INTO x " + backfill1);
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");
            final long partitionTimestamp;
            try (TableReader reader = engine.getReader(xt)) {
                partitionTimestamp = reader.getPartitionTimestampByIndex(0);
                Assert.assertTrue("the partition should have been cut into pieces",
                        reader.getGeometry().getPieceCount(0) > 1);
            }

            final CompositePartitionMerger.MergeResult staleResult =
                    CompositePartitionMerger.merge(engine, xt, partitionTimestamp);
            Assert.assertNotNull(staleResult);

            // A fresh WAL insert lands another piece on the SAME partition after the snapshot was taken.
            final String backfill2 = "SELECT x::INT + 80000 i, ('k-' || (x % 200))::SYMBOL symi," +
                    " timestamp_sequence('2020-02-03T14:00:11', 5*1000000L) ts FROM long_sequence(120)";
            execute("INSERT INTO x " + backfill2);
            drainWalQueue();

            final long liveRowsAfterSecondBackfill;
            final int pieceCountAfterSecondBackfill;
            try (TableReader reader = engine.getReader(xt)) {
                liveRowsAfterSecondBackfill = reader.getTxFile().getPartitionSize(0);
                pieceCountAfterSecondBackfill = reader.getGeometry().getPieceCount(0);
                Assert.assertTrue("the second backfill should have advanced past the snapshot",
                        pieceCountAfterSecondBackfill != staleResult.getSnapshotPieceCount()
                                || reader.getGeometry().getWriterTxn(0) != staleResult.getSnapshotWriterTxn());
            }

            try (TableWriter writer = engine.getWriter(xt, "compaction test")) {
                final int staleSwapResult = writer.swapCompositePartition(staleResult);
                Assert.assertEquals(TableWriter.SWAP_STALE, staleSwapResult);
            }

            // The staging directory the stale attempt built is gone...
            assertStagingDirGone(xt, staleResult);
            // ...and the table's real state is completely unaffected: same row count, same piece count,
            // same old-name-txn directory still in place.
            try (TableReader reader = engine.getReader(xt)) {
                Assert.assertEquals(liveRowsAfterSecondBackfill, reader.getTxFile().getPartitionSize(0));
                Assert.assertEquals(pieceCountAfterSecondBackfill, reader.getGeometry().getPieceCount(0));
                Assert.assertEquals(staleResult.getOldNameTxn(), reader.getTxFile().getPartitionNameTxn(0));
            }

            // Retry: a fresh merge, over the table's now-current state, followed by a swap - must succeed.
            final CompositePartitionMerger.MergeResult freshResult =
                    CompositePartitionMerger.merge(engine, xt, partitionTimestamp);
            Assert.assertNotNull(freshResult);
            Assert.assertNotEquals(
                    "the retry must observe a different snapshot than the stale attempt",
                    staleResult.getSnapshotPieceCount(), freshResult.getSnapshotPieceCount()
            );

            try (TableWriter writer = engine.getWriter(xt, "compaction test")) {
                final int okResult = writer.swapCompositePartition(freshResult);
                Assert.assertEquals(TableWriter.SWAP_OK, okResult);
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader reader = engine.getReader(xt)) {
                Assert.assertEquals(liveRowsAfterSecondBackfill, reader.getTxFile().getPartitionSize(0));
                Assert.assertEquals(1, reader.getGeometry().getPieceCount(0));
            }

            execute("CREATE TABLE o AS (SELECT i, symi::SYMBOL symi, ts FROM (" +
                    base + " UNION ALL " + nextDay + " UNION ALL " + backfill1 + " UNION ALL " + backfill2 +
                    ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            TestUtils.assertSqlCursors(
                    engine, sqlExecutionContext, "SELECT * FROM o ORDER BY ts, i", "SELECT * FROM x ORDER BY ts, i", LOG
            );
        });
    }

    private static void assertOldPartitionDirGone(TableToken tableToken, long partitionTimestamp, long oldNameTxn) throws Exception {
        final CairoConfiguration configuration = engine.getConfiguration();
        final FilesFacade ff = configuration.getFilesFacade();
        try (TableReader reader = engine.getReader(tableToken); Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(tableToken.getDirName());
            TableUtils.setPathForNativePartition(
                    path, reader.getMetadata().getTimestampType(), reader.getPartitionedBy(), partitionTimestamp, oldNameTxn
            );
            Assert.assertFalse("old partition directory must be gone after swap [path=" + path + ']', ff.exists(path.$()));
        }
    }

    private static void assertStagingDirGone(TableToken tableToken, CompositePartitionMerger.MergeResult mergeResult) throws Exception {
        final CairoConfiguration configuration = engine.getConfiguration();
        final FilesFacade ff = configuration.getFilesFacade();
        try (TableReader reader = engine.getReader(tableToken); Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(tableToken.getDirName());
            TableUtils.setPathForCompactingPartition(
                    path, reader.getMetadata().getTimestampType(), reader.getPartitionedBy(),
                    mergeResult.getPartitionTimestamp(), mergeResult.getOldNameTxn(), mergeResult.getSnapshotWriterTxn()
            );
            Assert.assertFalse("staging directory must be gone after swap [path=" + path + ']', ff.exists(path.$()));
        }
    }
}
