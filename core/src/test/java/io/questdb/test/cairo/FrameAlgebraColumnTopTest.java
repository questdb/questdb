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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.NumericException;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.setPathForNativePartition;
import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

/**
 * {@code FrameAlgebra.appendColumn} absorbing a source's leading NULLs into a PLAIN target's own column
 * top instead of materialising them.
 * <p>
 * A column added after a partition already exists has no data in that partition at all, so the target's
 * top equals its row count. Folding a source that carries the column only from its own top onwards then
 * has two ways to account for the NULL run in between: push the target's top forward by that many rows -
 * free, nothing on disk - or write the run out. Only the first keeps a late-added column's files
 * proportional to the rows that actually carry a value; the second re-writes and retains the whole NULL
 * prefix on every split and every squash, for every such column, forever.
 * <p>
 * Query results are identical either way, which is why these tests observe the published column top and
 * the on-disk column file rather than the rows. A composite target is the one shape the shortcut does not
 * hold for - its live rows sit at piece offsets inside a larger extent - so the merge-append case asserts
 * the fold still leaves the day readable rather than asserting a top.
 */
public class FrameAlgebraColumnTopTest extends AbstractCairoTest {
    private static final String DAY = "2024-01-01";
    private static final int LATE_COLUMN_ROWS = 500;
    private static final int NULL_RUN_ROWS = 20_000;
    private static final int SEED_ROWS = 20_000;

    @Test
    public void testFoldIntoCompositeTargetDoesNotExtendColumnTop() throws Exception {
        // The one shape the shortcut does NOT hold for, and the whole correctness risk of restoring it:
        // a composite target is appended at E, past rows its pieces still point at, so its top is not the
        // flat run from row 0 an extended top would describe. If the flag ever leaked true onto such a
        // target the damage would be silent, so this pins the conservative branch with an assertion.
        // The fold has to be the OPPORTUNISTIC (housekeeping) one - a forced SQUASH PARTITIONS flattens
        // the target to plain first, so it never reaches the composite path at all.
        setUpSplitLimits();
        assertMemoryLeak(() -> {
            createDaySplitInTwo();
            growTheFoldSource();
            // Two narrow backdated strides over the same rows leave the FRONT sibling - the fold's target -
            // composite: merge-append rewrites the owning piece at the file tail and abandons the old copy.
            // BEFORE the column is added, so those writes record no v top of their own and the target's top
            // stays its whole extent - the shortcut's precondition.
            for (int i = 0; i < 2; i++) {
                execute("INSERT INTO x (i, ts) SELECT x::INT + 400_000," +
                        " timestamp_sequence('" + DAY + "T01:00:00', 1_000_000L) FROM long_sequence(200)");
                drainWalQueue();
            }
            execute("ALTER TABLE x ADD COLUMN v LONG");
            drainWalQueue();
            appendRowsCarryingV();
            // A later day, so neither sibling is the last partition: the fold leaves a composite LAST
            // partition alone, and would then never reach the target under test.
            execute("INSERT INTO x (i, ts) SELECT x::INT + 900_000," +
                    " timestamp_sequence('2024-01-05', 1_000_000L) FROM long_sequence(100)");
            drainWalQueue();

            final long targetExtent;
            final long sourceTop;
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                final TxReader txFile = reader.getTxFile();
                final int front = txFile.getPartitionIndex(dayFloor());
                Assert.assertTrue("fixture left the fold target plain", txFile.isPartitionComposite(front));
                // E, the target's physical extent: where the fold appends, and what its frame reports as
                // its row count - so this is the top appendNulls has to leave behind.
                targetExtent = reader.getPartitionPhysicalRowCount(front);
                Assert.assertTrue("fixture left the composite target with no dead space [E=" + targetExtent
                                + ", liveRows=" + txFile.getPartitionSize(front) + ']',
                        targetExtent > txFile.getPartitionSize(front));
                final ColumnVersionReader cvr = reader.getColumnVersionReader();
                final int vIndex = reader.getMetadata().getColumnIndex("v");
                // Resolved against E, the way the target's own frame does it: equal to E means the column
                // is absent from the file, which is exactly the shortcut's targetColTop == targetRowCount.
                Assert.assertEquals("test setup gap: the composite target already carries v, so the"
                                + " shortcut's targetColTop == targetRowCount precondition does not hold",
                        targetExtent, resolveTop(cvr, txFile.getPartitionTimestampByIndex(front), vIndex, targetExtent));
                final long sourceRows = txFile.getPartitionSize(front + 1);
                sourceTop = resolveTop(cvr, txFile.getPartitionTimestampByIndex(front + 1), vIndex, sourceRows);
                Assert.assertTrue("test setup gap: the fold source pads no NULLs [sourceTop=" + sourceTop
                        + ']', sourceTop > 0);
            }

            final long rowsBefore = scalar("SELECT count() FROM x WHERE ts IN '" + DAY + "'");
            final long vRowsBefore = scalar("SELECT count() FROM x WHERE ts IN '" + DAY + "' AND v IS NOT NULL");
            final long vSumBefore = scalar("SELECT sum(v) FROM x WHERE ts IN '" + DAY + "'");
            final String fingerprintBefore = fingerprintOfDay();

            foldByHousekeeping();

            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                final TxReader txFile = reader.getTxFile();
                final int front = txFile.getPartitionIndex(dayFloor());
                final ColumnVersionReader cvr = reader.getColumnVersionReader();
                final int vIndex = reader.getMetadata().getColumnIndex("v");
                final long topAfter = cvr.getColumnTop(txFile.getPartitionTimestampByIndex(front), vIndex);
                Assert.assertEquals("the fold EXTENDED a composite target's v column top instead of writing"
                                + " the NULLs out; its live rows sit at piece offsets inside a larger"
                                + " extent, so an extended top does not describe them",
                        targetExtent, topAfter);
                Assert.assertNotEquals("the fold absorbed the source's NULL run into a composite target's"
                        + " v column top", targetExtent + sourceTop, topAfter);
            }

            Assert.assertEquals("the fold changed the day's row count", rowsBefore,
                    scalar("SELECT count() FROM x WHERE ts IN '" + DAY + "'"));
            Assert.assertEquals("the fold lost rows of the late-added column", vRowsBefore,
                    scalar("SELECT count() FROM x WHERE ts IN '" + DAY + "' AND v IS NOT NULL"));
            Assert.assertEquals("the fold changed the late-added column's values", vSumBefore,
                    scalar("SELECT sum(v) FROM x WHERE ts IN '" + DAY + "'"));
            Assert.assertEquals("the fold changed the day's rows or their order", fingerprintBefore,
                    fingerprintOfDay());
        });
    }

    @Test
    public void testSplitRemovalRepairExtendsColumnTop() throws Exception {
        // The finding's own trigger: an O3 commit empties a split sibling whose parent a reader may still
        // be holding, so TableWriter cannot simply drop it - it splits the parent's last timestamp off
        // into a fresh directory first (TableWriter#o3ConsumePartitionUpdateSink_processSplitPartitionRemoval).
        // That copy is a FrameAlgebra.append into a brand-new, empty target, and the parent's whole NULL
        // prefix for a late-added column is what it has to account for.
        setUpSplitLimits();
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            createDaySplitInTwo();
            // The column exists in neither sibling's files, so the parent's top is its whole row count.
            execute("ALTER TABLE x ADD COLUMN v LONG");
            drainWalQueue();

            final long parentRows;
            final long siblingTs;
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                final TxReader txFile = reader.getTxFile();
                Assert.assertEquals("fixture did not split the day in two", 2, txFile.getPartitionCount());
                parentRows = txFile.getPartitionSize(0);
                siblingTs = txFile.getPartitionTimestampByIndex(1);
                final ColumnVersionReader cvr = reader.getColumnVersionReader();
                final int vIndex = reader.getMetadata().getColumnIndex("v");
                Assert.assertEquals("test setup gap: the parent already carries v, so the split copy pads"
                                + " no NULLs", parentRows,
                        resolveTop(cvr, txFile.getPartitionTimestampByIndex(0), vIndex, parentRows));
            }

            // Replace everything from the sibling's floor upwards, carrying one row in a much later day.
            // The sibling loses every row it had; the parent is below the range and is left alone.
            try (WalWriter ww = engine.getWalWriter(engine.verifyTableName("x"))) {
                final TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2024-01-09T00:00:00.000000Z"));
                row.putInt(0, 999);
                row.append();
                ww.commitWithParams(siblingTs, Long.MAX_VALUE, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalQueue();

            final long repairRows;
            final long repairTop;
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                final TxReader txFile = reader.getTxFile();
                final int front = txFile.getPartitionIndex(dayFloor());
                Assert.assertTrue("the removal was not repaired by splitting the parent: the day holds one"
                                + " partition, so no FrameAlgebra.append ran [partitionCount="
                                + txFile.getPartitionCount() + ']',
                        front + 1 < txFile.getPartitionCount()
                                && txFile.getPartitionFloor(txFile.getPartitionTimestampByIndex(front + 1)) == dayFloor());
                Assert.assertTrue("the repair split off no rows", txFile.getPartitionSize(front) < parentRows);
                repairRows = txFile.getPartitionSize(front + 1);
                Assert.assertTrue("the repair split is empty", repairRows > 0);
                final ColumnVersionReader cvr = reader.getColumnVersionReader();
                final int vIndex = reader.getMetadata().getColumnIndex("v");
                repairTop = resolveTop(cvr, txFile.getPartitionTimestampByIndex(front + 1), vIndex, repairRows);
            }

            // The parent holds no v at all, so every row the repair copies is NULL: the whole run belongs
            // in the new directory's column top, not in its column file.
            Assert.assertEquals("the split repair materialised the parent's NULL run instead of extending"
                            + " the new split's v top [repairRows=" + repairRows + ']',
                    repairRows, repairTop);
        });
    }

    @Test
    public void testSquashIntoPlainTargetExtendsColumnTopBypassWal() throws Exception {
        setUpSplitLimits();
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (" +
                    "SELECT x::INT i, timestamp_sequence('" + DAY + "', 1_000_000L) ts" +
                    " FROM long_sequence(" + SEED_ROWS + ")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            splitTheDay();
            growTheFoldSource();
            execute("ALTER TABLE x ADD COLUMN v LONG");
            appendRowsCarryingV();
            assertTheFoldExtendsTheTargetTop();
        });
    }

    @Test
    public void testSquashIntoPlainTargetExtendsColumnTopWal() throws Exception {
        setUpSplitLimits();
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            createDaySplitInTwo();
            growTheFoldSource();
            execute("ALTER TABLE x ADD COLUMN v LONG");
            drainWalQueue();
            appendRowsCarryingV();
            assertTheFoldExtendsTheTargetTop();
        });
    }

    private static void appendRowsCarryingV() throws Exception {
        // In order, past everything the day holds, so these land in the LAST sibling - the fold's source -
        // and leave the front sibling, its target, with no v data at all.
        execute("INSERT INTO x SELECT x::INT + 300_000," +
                " timestamp_sequence('" + DAY + "T12:00:00', 1_000_000L), x" +
                " FROM long_sequence(" + LATE_COLUMN_ROWS + ")");
        drainWalQueue();
    }

    /**
     * Grows the LAST sibling in order, before the column is added, so its own NULL prefix for that column
     * dwarfs the rows that go on to carry a value - the two are then far enough apart that page rounding
     * cannot blur which of them the merged column file holds.
     */
    private static void growTheFoldSource() throws Exception {
        execute("INSERT INTO x (i, ts) SELECT x::INT + 100_000," +
                " timestamp_sequence('" + DAY + "T06:00:00', 1_000_000L) FROM long_sequence(" + NULL_RUN_ROWS + ")");
        drainWalQueue();
    }

    /**
     * Captures the two siblings' v tops, folds them, then asserts the merged partition accounts for the
     * source's NULL run as a top rather than as bytes.
     */
    private void assertTheFoldExtendsTheTargetTop() throws Exception {
        final long targetRows;
        final long sourceTop;
        final long sourceRows;
        try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
            final TxReader txFile = reader.getTxFile();
            Assert.assertEquals("fixture did not split the day in two", 2, txFile.getPartitionCount());
            final ColumnVersionReader cvr = reader.getColumnVersionReader();
            final int vIndex = reader.getMetadata().getColumnIndex("v");
            targetRows = txFile.getPartitionSize(0);
            sourceRows = txFile.getPartitionSize(1);
            sourceTop = resolveTop(cvr, txFile.getPartitionTimestampByIndex(1), vIndex, sourceRows);
            final long targetTop = resolveTop(cvr, txFile.getPartitionTimestampByIndex(0), vIndex, targetRows);
            // The shortcut is reachable only from this shape.
            Assert.assertEquals("test setup gap: the fold target already carries v, so its top is not"
                            + " its row count [targetTop=" + targetTop + ", targetRows=" + targetRows + ']',
                    targetRows, targetTop);
            Assert.assertTrue("test setup gap: the fold source carries no v NULL run, so the append pads"
                    + " nothing [sourceTop=" + sourceTop + ']', sourceTop > 0);
        }

        squashTheDay();
        // The writer's append memories keep the merged .d mapped to a page boundary; only a truncating
        // close cuts it back to the rows it actually holds, which is what makes the waste permanent.
        engine.releaseAllWriters();

        final long mergedRows = targetRows + sourceRows;
        final long mergedTop;
        final long vFileLength;
        try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
            final TxReader txFile = reader.getTxFile();
            Assert.assertEquals("the fold did not merge the day into one partition", 1, txFile.getPartitionCount());
            Assert.assertEquals("the fold changed the merged partition's row count",
                    mergedRows, txFile.getPartitionSize(0));
            final ColumnVersionReader cvr = reader.getColumnVersionReader();
            final int vIndex = reader.getMetadata().getColumnIndex("v");
            final long partitionTs = txFile.getPartitionTimestampByIndex(0);
            mergedTop = resolveTop(cvr, partitionTs, vIndex, mergedRows);
            vFileLength = lengthOfV(reader.getTableToken(), partitionTs, txFile.getPartitionNameTxn(0),
                    cvr.getColumnNameTxn(partitionTs, vIndex));
        }

        Assert.assertEquals("the fold materialised the source's NULL run instead of extending the"
                        + " merged partition's v top",
                targetRows + sourceTop, mergedTop);
        // A truncating close rounds the file up to a page, so the two outcomes are told apart by which
        // side of the NULL run the length falls on rather than by an exact byte count.
        final long valueBytes = (mergedRows - targetRows - sourceTop) * Long.BYTES;
        final long nullRunBytes = sourceTop * Long.BYTES;
        Assert.assertTrue("the fold left v.d shorter than the rows carrying a value need [vFileLength="
                + vFileLength + ", valueBytes=" + valueBytes + ']', vFileLength >= valueBytes);
        Assert.assertTrue("the fold wrote the source's NULL run into v.d [vFileLength=" + vFileLength
                        + ", valueBytes=" + valueBytes + ", nullRunBytes=" + nullRunBytes + ']',
                vFileLength < nullRunBytes);

        assertQuery("SELECT count() c, count(v) cv, sum(v) sv FROM x WHERE ts IN '" + DAY + "'")
                .expectSize()
                .noRandomAccess()
                .returns("c\tcv\tsv\n"
                        + mergedRows + '\t' + LATE_COLUMN_ROWS + '\t'
                        + ((long) LATE_COLUMN_ROWS * (LATE_COLUMN_ROWS + 1) / 2) + '\n');
    }

    /**
     * A day cut into two siblings, pushed off the end of the table so both are foldable.
     */
    private static void createDaySplitInTwo() throws Exception {
        execute("CREATE TABLE x AS (" +
                "SELECT x::INT i, timestamp_sequence('" + DAY + "', 1_000_000L) ts" +
                " FROM long_sequence(" + SEED_ROWS + ")) TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalQueue();
        splitTheDay();
    }

    /**
     * Content fingerprint of the day IN CURSOR ORDER, so a run appended at the wrong offset moves it even
     * when the row count holds.
     */
    private static String fingerprintOfDay() throws Exception {
        long count = 0;
        long hash = 0;
        try (RecordCursorFactory f = select("SELECT ts, i, v FROM x WHERE ts IN '" + DAY + "'")) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                while (c.hasNext()) {
                    count++;
                    hash = hash * 1_000_003L + c.getRecord().getLong(0);
                    hash = hash * 1_000_003L + c.getRecord().getInt(1);
                    hash = hash * 1_000_003L + c.getRecord().getLong(2);
                }
            }
        }
        return count + "/" + hash;
    }

    private static long dayFloor() throws NumericException {
        return MicrosTimestampDriver.floor(DAY + "T00:00:00.000000Z");
    }

    /**
     * The OPPORTUNISTIC fold: lower the split limit and drive one more commit, in a LATER day so nothing
     * but the day's own housekeeping can account for what changes there. Unlike SQUASH PARTITIONS this
     * leaves a composite target composite, which is the whole point.
     */
    private static void foldByHousekeeping() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 1);
        execute("INSERT INTO x (i, ts) VALUES (999, '2024-01-06T00:00:00.000000Z')");
        drainWalQueue();
    }

    private static long lengthOfV(TableToken token, long partitionTs, long partitionNameTxn, long columnNameTxn) {
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token);
            setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
            return configuration.getFilesFacade().length(TableUtils.dFile(path, "v", columnNameTxn));
        }
    }

    /**
     * Resolves a top the way {@code FrameImpl.createColumn} does: an absent record means the column does
     * not exist in that partition, which is a top equal to its row count.
     */
    private static long resolveTop(ColumnVersionReader cvr, long partitionTs, int columnIndex, long rowCount) {
        final long top = cvr.getColumnTop(partitionTs, columnIndex);
        return Math.min(top < 0 ? rowCount : top, rowCount);
    }

    private static long scalar(String sql) throws Exception {
        try (RecordCursorFactory f = select(sql)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                Assert.assertTrue("query returned no row: " + sql, c.hasNext());
                return c.getRecord().getLong(0);
            }
        }
    }

    private static void setUpSplitLimits() {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 << 10);
        // Hold the siblings still: each test folds them with an explicit SQUASH PARTITIONS instead, so
        // nothing but that call can account for what the merged partition looks like.
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 1_000);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 1_000);
    }

    /**
     * Cuts a sibling off the tail of the day with an O3 write landing well inside it. Merge-append would
     * rewrite the piece in place instead, so it is off for the duration of the cut.
     */
    private static void splitTheDay() throws Exception {
        final String mergeAppend = configuration.isO3PartitionMergeAppendEnabled() ? "true" : "false";
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        execute("INSERT INTO x (i, ts) SELECT x::INT + 200_000," +
                " timestamp_sequence('" + DAY + "T05:00:00', 1_000L) FROM long_sequence(200)");
        drainWalQueue();
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, mergeAppend);
    }

    private static void squashTheDay() throws Exception {
        execute("ALTER TABLE x SQUASH PARTITIONS");
        drainWalQueue();
    }
}
