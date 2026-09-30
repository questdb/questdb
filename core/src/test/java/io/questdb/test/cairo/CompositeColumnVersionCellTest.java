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

import io.questdb.cairo.ColumnVersionWriter;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Plan 3 (composite partitioning), Task 5: the {@code _cv} column-versions record stays {@code
 * BLOCK_SIZE = 4} -- unlike {@code _txn} (which widened its stride, see {@link CompositeTxCellTest}),
 * {@code _cv} packs the cellKey into the spare high 32 bits of the {@code COLUMN_INDEX_OFFSET} slot:
 * {@code columnIndexPacked = ((long) cellKey << 32) | (columnIndex & 0xFFFF_FFFFL)}. For a plain table
 * {@code cellKey} is always 0, so {@code columnIndexPacked == columnIndex} exactly and the on-disk
 * bytes are byte-identical to the pre-composite-partitioning layout.
 * <p>
 * In this plan, production write-routing only ever produces cellKey 0 (multi-cell routing is Plan 4);
 * the multi-cell states exercised below are driven directly through the writer API as test-only
 * scaffolding for machinery that Plan 4 will activate.
 */
public class CompositeColumnVersionCellTest extends AbstractCairoTest {

    /**
     * Brief-mandated test 1: two cells at the same timestamp and column index must resolve to
     * distinct records -- a single-key (cellKey-oblivious) implementation would alias cell1's upsert
     * onto cell0's record, losing cell0's column top.
     */
    @Test
    public void testNoAliasingAcrossCells() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = createColumnVersionWriter(path)
            ) {
                final long day1 = 0L;
                final int col3 = 3;

                w.upsert(day1, 0, col3, -1, 100);
                w.upsert(day1, 1, col3, -1, 200);

                int cell0Index = w.getRecordIndex(day1, 0, col3);
                int cell1Index = w.getRecordIndex(day1, 1, col3);

                Assert.assertTrue("cell0 record must exist", cell0Index > -1);
                Assert.assertTrue("cell1 record must exist", cell1Index > -1);
                Assert.assertNotEquals(
                        "cell0 and cell1 records must not alias to the same slot",
                        cell0Index, cell1Index
                );
                Assert.assertEquals("cell0's column top must be unaffected by cell1's upsert", 100, w.getColumnTopByIndex(cell0Index));
                Assert.assertEquals("cell1's column top must be its own value", 200, w.getColumnTopByIndex(cell1Index));
            }
        });
    }

    /**
     * Brief-mandated test 2: for a plain-shaped table (cellKey 0), the raw stored long at the
     * columnIndex offset must equal the bare column index exactly -- high 32 bits zero. This is the
     * byte-identity proof: {@code packColIndex(0, columnIndex) == columnIndex}.
     */
    @Test
    public void testPlainShapeByteIdentity() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = createColumnVersionWriter(path)
            ) {
                final long day1 = 0L;
                final int col3 = 3;

                w.upsert(day1, 0, col3, -1, 100);

                int recordIndex = w.getRecordIndex(day1, 0, col3);
                Assert.assertTrue(recordIndex > -1);
                long rawStoredLong = w.getCachedColumnVersionList().getQuick(recordIndex + ColumnVersionWriter.COLUMN_INDEX_OFFSET);
                Assert.assertEquals(
                        "cellKey=0 must pack to the bare columnIndex -- high 32 bits zero (byte-identical to pre-feature layout)",
                        (long) col3, rawStoredLong
                );
            }
        });
    }

    // ---- additional coverage beyond the brief's mandated two tests ------------------------------

    /**
     * {@link ColumnVersionWriter#removePartition(long, int)} must remove exactly the matching cell's
     * contiguous sub-run within a timestamp block, not the whole block and not a neighbouring cell.
     * Builds three cells at day1 (cell0/col3, cell1/col3, cell1/col7, cell2/col3), removes cell1, and
     * asserts cell0 and cell2's records survive untouched.
     */
    @Test
    public void testRemovePartitionRemovesOnlyMatchingCellContiguousSubRun() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = createColumnVersionWriter(path)
            ) {
                final long day1 = 0L;

                w.upsert(day1, 0, 3, -1, 100);
                w.upsert(day1, 1, 3, -1, 200);
                w.upsert(day1, 1, 7, -1, 201);
                w.upsert(day1, 2, 3, -1, 300);

                w.removePartition(day1, 1);

                Assert.assertEquals("cell1/col3 must be gone", -1, w.getRecordIndex(day1, 1, 3));
                Assert.assertEquals("cell1/col7 must be gone", -1, w.getRecordIndex(day1, 1, 7));

                int cell0Index = w.getRecordIndex(day1, 0, 3);
                int cell2Index = w.getRecordIndex(day1, 2, 3);
                Assert.assertTrue("cell0's row must survive a cell1 removal", cell0Index > -1);
                Assert.assertTrue("cell2's row must survive a cell1 removal", cell2Index > -1);
                Assert.assertEquals(100, w.getColumnTopByIndex(cell0Index));
                Assert.assertEquals(300, w.getColumnTopByIndex(cell2Index));
            }
        });
    }

    /**
     * Plain-table equivalence for the remove path: {@link ColumnVersionWriter#removePartition(long)}
     * (the cellKey=0 wrapper) must remove the same complete row set at a timestamp as it did before
     * this feature, when every row at that timestamp is cellKey 0 (the plain/dormant shape). This is
     * the explicit removePartition byte-identity check called for by the ambiguity-resolution notes.
     */
    @Test
    public void testPlainRemovePartitionRemovesWholeTimestampWhenAllCellKeyZero() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = createColumnVersionWriter(path)
            ) {
                final long day1 = 0L;
                final long day2 = 1L;

                w.upsert(day1, 3, -1, 100);
                w.upsert(day1, 7, -1, 101);
                w.upsert(day2, 3, -1, 200);

                w.removePartition(day1);

                Assert.assertEquals(-1, w.getRecordIndex(day1, 3));
                Assert.assertEquals(-1, w.getRecordIndex(day1, 7));
                Assert.assertTrue("an untouched timestamp must be unaffected", w.getRecordIndex(day2, 3) > -1);
            }
        });
    }

    /**
     * {@link ColumnVersionWriter#getMaxPartitionVersion(long, int)} must scope its max-nameTxn scan to
     * the requested cell only, not scan across cells at the same timestamp.
     */
    @Test
    public void testGetMaxPartitionVersionPerCell() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = createColumnVersionWriter(path)
            ) {
                final long day1 = 0L;

                w.upsert(day1, 0, 3, 5, 100);
                w.upsert(day1, 1, 3, 9, 200);

                Assert.assertEquals("cell0's max version", 5, w.getMaxPartitionVersion(day1, 0));
                Assert.assertEquals("cell1's max version", 9, w.getMaxPartitionVersion(day1, 1));
            }
        });
    }

    private static ColumnVersionWriter createColumnVersionWriter(Path path) {
        return new ColumnVersionWriter(configuration, path.of(root).concat("_cv").$(), true);
    }
}
