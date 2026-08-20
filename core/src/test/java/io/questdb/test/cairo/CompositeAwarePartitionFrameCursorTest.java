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

import io.questdb.PropertyKey;
import io.questdb.cairo.CompositeAwarePartitionFrameCursor;
import io.questdb.cairo.FullBwdPartitionFrameCursor;
import io.questdb.cairo.FullFwdPartitionFrameCursor;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.std.LongList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Direct tests for {@link CompositeAwarePartitionFrameCursor}: non-composite passthrough, per-piece
 * splitting in both directions, and resume across {@code next()} calls. The end-to-end behavior these
 * primitives support is covered by {@code CoveringIndexTest} and {@code PostingIndexDistinctRecordCursorFactory}
 * composite-partition tests; this class isolates the wrapper's own contract.
 */
public class CompositeAwarePartitionFrameCursorTest extends AbstractCairoTest {

    @Test
    public void testBackwardPieceSplitVisitsPiecesInDescendingCumulativeOrder() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tt = createMultiPieceTable("t_bwd_split");
            // Only cursor is a try-with-resources: its close() cascades to delegate.close() and,
            // through that, to the reader -- declaring reader/delegate as resources too would double-close.
            final TableReader reader = engine.getReader(tt);
            final FullBwdPartitionFrameCursor delegate = new FullBwdPartitionFrameCursor();
            delegate.of(reader);
            try (CompositeAwarePartitionFrameCursor cursor = new CompositeAwarePartitionFrameCursor()) {
                cursor.of(delegate, true);

                final PartitionGeometry geometry = reader.getGeometry();
                final int pieceCount = geometry.getPieceCount(0);
                Assert.assertTrue("test precondition: partition 0 must be genuinely composite", pieceCount > 1);

                // Expected physical [rowLo, rowHi) per piece, highest ordinal first.
                final LongList expectedLo = new LongList();
                final LongList expectedHi = new LongList();
                for (int p = pieceCount - 1; p >= 0; p--) {
                    final long shift = geometry.getPieceShift(0, p);
                    final long cumLo = geometry.getPieceCumulativeLo(0, p);
                    final long cumHi = cumLo + geometry.getPieceRowCount(0, p);
                    expectedLo.add(cumLo + shift);
                    expectedHi.add(cumHi + shift);
                }

                int seen = 0;
                PartitionFrame frame;
                while ((frame = cursor.next()) != null && frame.getPartitionIndex() == 0) {
                    Assert.assertEquals("piece " + seen + " rowLo", expectedLo.getQuick(seen), frame.getRowLo());
                    Assert.assertEquals("piece " + seen + " rowHi", expectedHi.getQuick(seen), frame.getRowHi());
                    seen++;
                }
                Assert.assertEquals("expected one wrapper frame per piece, highest first", pieceCount, seen);
            }
        });
    }

    @Test
    public void testForwardPieceSplitCoversWholePartitionExactlyOnce() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tt = createMultiPieceTable("t_fwd_split");
            final TableReader reader = engine.getReader(tt);
            final FullFwdPartitionFrameCursor delegate = new FullFwdPartitionFrameCursor();
            delegate.of(reader);
            try (CompositeAwarePartitionFrameCursor cursor = new CompositeAwarePartitionFrameCursor()) {
                cursor.of(delegate, false);

                final PartitionGeometry geometry = reader.getGeometry();
                final int pieceCount = geometry.getPieceCount(0);
                Assert.assertTrue("test precondition: partition 0 must be genuinely composite", pieceCount > 1);

                // A single next() on the RAW (unwrapped) delegate returns the whole partition as one
                // logical frame; draining the SAME partition through the wrapper takes one next() call per
                // piece instead -- this is the resume behavior in question.
                int piecesSeen = 0;
                long cumulativeRowsSeen = 0;
                PartitionFrame frame;
                while ((frame = cursor.next()) != null && frame.getPartitionIndex() == 0) {
                    final long shift = geometry.getPieceShift(0, piecesSeen);
                    final long cumLo = geometry.getPieceCumulativeLo(0, piecesSeen);
                    final long expectedRowCount = geometry.getPieceRowCount(0, piecesSeen);
                    Assert.assertEquals("piece " + piecesSeen + " rowLo", cumLo + shift, frame.getRowLo());
                    Assert.assertEquals("piece " + piecesSeen + " rowHi", cumLo + shift + expectedRowCount, frame.getRowHi());
                    cumulativeRowsSeen += frame.getRowHi() - frame.getRowLo();
                    piecesSeen++;
                }
                Assert.assertEquals("expected one wrapper frame per piece, lowest first", pieceCount, piecesSeen);
                Assert.assertEquals("physical ranges must cover every live row exactly once",
                        geometry.getLiveRows(0), cumulativeRowsSeen);
            }
        });
    }

    @Test
    public void testNonCompositePartitionPassesThroughUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_plain (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_plain VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-02T00:00:00', 'B', 2.0),
                    ('2024-01-03T00:00:00', 'C', 3.0)
                    """);
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("t_plain");
            final TableReader reader = engine.getReader(tt);
            final FullFwdPartitionFrameCursor delegate = new FullFwdPartitionFrameCursor();
            delegate.of(reader);
            Assert.assertFalse(reader.getTxFile().isPartitionComposite(0));
            try (CompositeAwarePartitionFrameCursor cursor = new CompositeAwarePartitionFrameCursor()) {
                cursor.of(delegate, false);

                int frameCount = 0;
                PartitionFrame frame;
                while ((frame = cursor.next()) != null) {
                    // Each partition here holds exactly one row: a wrapper that (incorrectly) tried to
                    // piece-split a non-composite partition would still report the same [0, 1) range, since
                    // there is only one piece to find -- so this also exercises the composite check itself
                    // taking the cheap TxReader flag, not a resolved PartitionGeometry piece lookup.
                    Assert.assertEquals(0, frame.getRowLo());
                    Assert.assertEquals(1, frame.getRowHi());
                    frameCount++;
                }
                Assert.assertEquals(3, frameCount);
            }
        });
    }

    /**
     * Builds a WAL table with a genuinely multi-piece (not just single-piece-with-offset) composite
     * partition 0: a full day of filler rows, committed first, then two cold-gap-separated backdated
     * batches bundled into one WAL apply (no drain between them) so transaction clustering has two
     * separate hot strides to cut around instead of one contiguous one.
     */
    private TableToken createMultiPieceTable(String tableName) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        execute("CREATE TABLE " + tableName + " (" +
                "ts TIMESTAMP, sym SYMBOL, price DOUBLE" +
                ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + tableName +
                " SELECT timestamp_sequence('2020-02-03', 15 * 1000000L) ts, 'F'::SYMBOL sym, x * 1.0 price" +
                " FROM long_sequence(5760)");
        drainWalQueue();

        execute("INSERT INTO " + tableName +
                " SELECT timestamp_sequence('2020-02-03T02:00:07', 5 * 1000000L) ts, 'G'::SYMBOL sym, x * 1.0 price" +
                " FROM long_sequence(120)");
        execute("INSERT INTO " + tableName +
                " SELECT timestamp_sequence('2020-02-03T08:00:11', 5 * 1000000L) ts, 'H'::SYMBOL sym, x * 1.0 price" +
                " FROM long_sequence(120)");
        drainWalQueue();

        return engine.verifyTableName(tableName);
    }
}
