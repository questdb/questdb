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
import io.questdb.cairo.AbstractIntervalPartitionFrameCursor;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CompositeAwarePartitionFrameCursor;
import io.questdb.cairo.FullBwdPartitionFrameCursor;
import io.questdb.cairo.FullFwdPartitionFrameCursor;
import io.questdb.cairo.IntervalBwdPartitionFrameCursor;
import io.questdb.cairo.IntervalFwdPartitionFrameCursor;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.model.RuntimeIntervalModel;
import io.questdb.std.LongList;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

/**
 * Direct tests for {@link CompositeAwarePartitionFrameCursor}: non-composite passthrough, per-piece
 * splitting in both directions, resume across {@code next()} calls, the frame-count upper bound, and splitting a
 * parquet partition with an index row map into runs of row groups with contiguous index rows. The end-to-end behavior these
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
    public void testFrameCountUpperBoundCountsPiecesBackward() throws Exception {
        assertMemoryLeak(() -> assertFrameCountUpperBoundCountsPieces(true));
    }

    @Test
    public void testFrameCountUpperBoundCountsPiecesForward() throws Exception {
        assertMemoryLeak(() -> assertFrameCountUpperBoundCountsPieces(false));
    }

    @Test
    public void testFrameCountUpperBoundOfEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_empty (ts TIMESTAMP, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            final TableToken tt = engine.verifyTableName("t_empty");
            Assert.assertEquals(0, drainFull(tt, false, 0));
            Assert.assertEquals(0, drainInterval(tt, false, "2024-01-01", "2024-12-31", 0));
        });
    }

    @Test
    public void testFrameCountUpperBoundPassesParquetThrough() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_parquet (ts TIMESTAMP, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t_parquet VALUES
                    ('2024-01-01T00:00:00', 1.0),
                    ('2024-01-01T12:00:00', 2.0),
                    ('2024-01-02T00:00:00', 3.0),
                    ('2024-01-03T00:00:00', 4.0)
                    """);
            execute("ALTER TABLE t_parquet CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02'");
            final TableToken tt = engine.verifyTableName("t_parquet");
            try (TableReader reader = engine.getReader(tt)) {
                Assert.assertEquals(PartitionFormat.PARQUET, reader.getPartitionFormat(0));
                Assert.assertEquals(PartitionFormat.NATIVE, reader.getPartitionFormat(2));
            }
            // One frame per partition, parquet or native.
            Assert.assertEquals(3, drainFull(tt, false, 3));
            Assert.assertEquals(3, drainFull(tt, true, 3));
            // Two windows, each inside one partition: two (window, partition) pairs.
            Assert.assertEquals(2, drainIntervals(tt, false, 2, "2024-01-01T10:00:00", "2024-01-01T13:00:00", "2024-01-03", "2024-01-03T01:00:00"));
        });
    }

    @Test
    public void testFrameCountUpperBoundUnknownWhenDelegateUnknown() {
        try (CompositeAwarePartitionFrameCursor cursor = new CompositeAwarePartitionFrameCursor()) {
            cursor.of(new UnboundedFrameCursor(), false);
            Assert.assertEquals(-1, cursor.getFrameCountUpperBound());
        }
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

    @Test
    public void testParquetIndexRowMapClipsRunsToPartialRanges() throws Exception {
        // Every partial partition-row range [lo, hi), as an interval filter would produce, must map to the
        // index rows of exactly its own rows, with the run extension stopping at the range bounds. The oracle
        // maps each row on its own and merges consecutive index rows, so it shares no code with the run walk.
        assertMemoryLeak(() -> {
            final TableToken tt = createIndexRowMapTable("t_pq_clip");
            final LongList rowShifts = new LongList();
            try (TableReader reader = engine.getReader(tt)) {
                reader.openPartition(0);
                final ParquetMetaFileReader metadata = reader.getAndInitParquetPartitionDecoder(0).metadata();
                Assert.assertTrue("test precondition: the in-place update must write an index row map", metadata.hasIndexRowMap());
                long rowGroupStartRow = 0;
                for (int i = 0, n = metadata.getRowGroupCount(); i < n; i++) {
                    final long shift = metadata.getRowGroupIndexRowLo(i, rowGroupStartRow) - rowGroupStartRow;
                    for (long r = 0, size = metadata.getRowGroupSize(i); r < size; r++) {
                        rowShifts.add(shift);
                    }
                    rowGroupStartRow += metadata.getRowGroupSize(i);
                }

                final LongList ranges = new LongList();
                for (int lo = 0, rowCount = rowShifts.size(); lo < rowCount; lo++) {
                    for (int hi = lo + 1; hi <= rowCount; hi++) {
                        ranges.clear();
                        metadata.collectIndexRowRanges(lo, hi, ranges);
                        Assert.assertEquals("collectIndexRowRanges [" + lo + ", " + hi + ')', expectedIndexRowRanges(rowShifts, lo, hi), ranges);
                    }
                }
            }

            final ClippedFwdPartitionFrameCursor delegate = new ClippedFwdPartitionFrameCursor();
            delegate.of(engine.getReader(tt));
            try (CompositeAwarePartitionFrameCursor cursor = new CompositeAwarePartitionFrameCursor()) {
                final LongList actual = new LongList();
                final LongList ascending = new LongList();
                for (int lo = 0, rowCount = rowShifts.size(); lo < rowCount; lo++) {
                    for (int hi = lo + 1; hi <= rowCount; hi++) {
                        final LongList expected = expectedIndexRowRanges(rowShifts, lo, hi);
                        for (boolean descending : new boolean[]{false, true}) {
                            delegate.clipPartition0(lo, hi);
                            cursor.of(delegate, descending);
                            actual.clear();
                            PartitionFrame frame;
                            while ((frame = cursor.next()) != null) {
                                if (frame.getPartitionIndex() == 0) {
                                    actual.add(frame.getRowLo(), frame.getRowHi());
                                }
                            }
                            if (descending) {
                                // frames come highest first; flip the pairs back to ascending order
                                ascending.clear();
                                for (int i = actual.size() - 2; i >= 0; i -= 2) {
                                    ascending.add(actual.getQuick(i), actual.getQuick(i + 1));
                                }
                                Assert.assertEquals("descending [" + lo + ", " + hi + ')', expected, ascending);
                            } else {
                                Assert.assertEquals("ascending [" + lo + ", " + hi + ')', expected, actual);
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testParquetIndexRowMapMergesRowGroupsWithContiguousIndexRows() throws Exception {
        // One frame per run of row groups that share a shift, not one per row group: an in-place update of a
        // middle row group leaves three runs (before it, the rewritten row group, after it).
        assertMemoryLeak(() -> {
            final TableToken tt = createIndexRowMapTable("t_pq");
            final LongList expected = new LongList();
            try (TableReader reader = engine.getReader(tt)) {
                reader.openPartition(0);
                final ParquetMetaFileReader metadata = reader.getAndInitParquetPartitionDecoder(0).metadata();
                Assert.assertTrue("test precondition: the in-place update must write an index row map", metadata.hasIndexRowMap());
                Assert.assertEquals(10, metadata.getRowGroupCount());
                long rowGroupStartRow = 0;
                for (int i = 0, n = metadata.getRowGroupCount(); i < n; i++) {
                    final long indexRowLo = metadata.getRowGroupIndexRowLo(i, rowGroupStartRow);
                    final long indexRowHi = indexRowLo + metadata.getRowGroupSize(i);
                    if (expected.size() > 0 && expected.getLast() == indexRowLo) {
                        expected.setQuick(expected.size() - 1, indexRowHi);
                    } else {
                        expected.add(indexRowLo, indexRowHi);
                    }
                    rowGroupStartRow += metadata.getRowGroupSize(i);
                }
                Assert.assertEquals("expected three runs of row groups", 6, expected.size());

                final LongList ranges = new LongList();
                metadata.collectIndexRowRanges(0, rowGroupStartRow, ranges);
                Assert.assertEquals(expected, ranges);
            }

            for (boolean descending : new boolean[]{false, true}) {
                final TableReader reader = engine.getReader(tt);
                final LongList actual = new LongList();
                try (CompositeAwarePartitionFrameCursor cursor = new CompositeAwarePartitionFrameCursor()) {
                    if (descending) {
                        final FullBwdPartitionFrameCursor delegate = new FullBwdPartitionFrameCursor();
                        delegate.of(reader);
                        cursor.of(delegate, true);
                    } else {
                        final FullFwdPartitionFrameCursor delegate = new FullFwdPartitionFrameCursor();
                        delegate.of(reader);
                        cursor.of(delegate, false);
                    }
                    PartitionFrame frame;
                    while ((frame = cursor.next()) != null) {
                        if (frame.getPartitionIndex() == 0) {
                            actual.add(frame.getRowLo(), frame.getRowHi());
                        }
                    }
                }
                if (descending) {
                    // frames come highest first; flip the pairs back to ascending order
                    final LongList ascending = new LongList();
                    for (int i = actual.size() - 2; i >= 0; i -= 2) {
                        ascending.add(actual.getQuick(i), actual.getQuick(i + 1));
                    }
                    Assert.assertEquals("descending", expected, ascending);
                } else {
                    Assert.assertEquals("ascending", expected, actual);
                }
            }
        });
    }

    private static LongList expectedIndexRowRanges(LongList rowShifts, long lo, long hi) {
        final LongList expected = new LongList();
        for (long r = lo; r < hi; r++) {
            final long indexRow = r + rowShifts.getQuick((int) r);
            if (expected.size() > 0 && expected.getLast() == indexRow) {
                expected.setQuick(expected.size() - 1, indexRow + 1);
            } else {
                expected.add(indexRow, indexRow + 1);
            }
        }
        return expected;
    }

    /**
     * Builds a WAL table whose partition 0 is a parquet partition of 4-row row groups with an index row map: an
     * in-place O3 update rewrites row group 4, moving it to fresh index rows past the end.
     */
    private TableToken createIndexRowMapTable(String tableName) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
        execute("CREATE TABLE " + tableName + " (sym SYMBOL INDEX TYPE POSTING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + tableName +
                " SELECT rnd_symbol('a', 'b', 'c'), timestamp_sequence('2024-01-01', 60_000_000L) FROM long_sequence(40)");
        execute("INSERT INTO " + tableName + " VALUES ('a', '2024-01-02')");
        drainWalQueue();
        execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
        // lands in row group 4 (minutes 16-19), which the update rewrites in place
        execute("INSERT INTO " + tableName + " VALUES ('b', '2024-01-01T00:17:30')");
        drainWalQueue();
        return engine.verifyTableName(tableName);
    }

    /**
     * Drains the wrapper over a full cursor, asserting its frame-count upper bound is
     * {@code expectedBound} and that the walk stays within it; returns the frames drained.
     */
    private static long drainFull(TableToken tt, boolean descending, long expectedBound) {
        final TableReader reader = engine.getReader(tt);
        final PartitionFrameCursor delegate = descending
                ? new FullBwdPartitionFrameCursor().of(reader)
                : new FullFwdPartitionFrameCursor().of(reader);
        try (CompositeAwarePartitionFrameCursor cursor = new CompositeAwarePartitionFrameCursor()) {
            cursor.of(delegate, descending);
            return drain(cursor, expectedBound);
        }
    }

    private static long drain(CompositeAwarePartitionFrameCursor cursor, long expectedBound) {
        Assert.assertEquals("frame-count upper bound", expectedBound, cursor.getFrameCountUpperBound());
        long frames = 0;
        while (cursor.next() != null) {
            frames++;
        }
        Assert.assertTrue("drained " + frames + " frames past the upper bound " + expectedBound, frames <= expectedBound);
        return frames;
    }

    private static long drainInterval(TableToken tt, boolean descending, String lo, String hi, long expectedBound) throws Exception {
        return drainIntervals(tt, descending, expectedBound, lo, hi);
    }

    /**
     * Drains the wrapper over an interval cursor of the closed windows {@code bounds} (lo, hi
     * pairs), asserting the bound and the walk as {@link #drain} does; returns the frames drained.
     */
    private static long drainIntervals(TableToken tt, boolean descending, long expectedBound, String... bounds) throws Exception {
        final TableReader reader = engine.getReader(tt);
        final TimestampDriver driver = ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType());
        final LongList intervals = new LongList();
        for (int i = 0; i < bounds.length; i += 2) {
            intervals.add(driver.parseFloorLiteral(bounds[i]));
            intervals.add(driver.parseFloorLiteral(bounds[i + 1]));
        }
        final RuntimeIntervalModel model = new RuntimeIntervalModel(driver, reader.getPartitionedBy(), intervals);
        final int timestampIndex = reader.getMetadata().getTimestampIndex();
        final AbstractIntervalPartitionFrameCursor delegate = descending
                ? new IntervalBwdPartitionFrameCursor(configuration, model, timestampIndex)
                : new IntervalFwdPartitionFrameCursor(configuration, model, timestampIndex);
        try (CompositeAwarePartitionFrameCursor cursor = new CompositeAwarePartitionFrameCursor()) {
            cursor.of(delegate, descending);
            delegate.of(reader, null);
            return drain(cursor, expectedBound);
        }
    }

    /**
     * A composite partition, its O3 neighbours and a plain partition appended after them: the
     * bound must count every piece a full scan emits, and for windows it adds the piece splits
     * to the delegate's (window, partition) pairs.
     */
    private void assertFrameCountUpperBoundCountsPieces(boolean descending) throws Exception {
        final TableToken tt = createMultiPieceTable("t_bound");
        execute("INSERT INTO t_bound VALUES ('2020-02-05T00:00:00', 'P', 1.0)");
        drainWalQueue();
        final int partitionCount;
        int compositeCount = 0;
        try (TableReader reader = engine.getReader(tt)) {
            partitionCount = reader.getPartitionCount();
            for (int p = 0; p < partitionCount; p++) {
                if (reader.getTxFile().isPartitionComposite(p)) {
                    compositeCount++;
                }
            }
            Assert.assertFalse("test precondition: the appended partition is plain",
                    reader.getTxFile().isPartitionComposite(partitionCount - 1));
        }
        Assert.assertTrue("test precondition: a composite partition", compositeCount > 0);

        // A full scan emits a frame per piece of every partition, all non-empty, so the bound,
        // which must cover it, is exactly the frames it emits: the pieces beyond one per
        // partition are the splits.
        final long fullFrames = countFullFrames(tt, descending);
        final long pieceSplits = fullFrames - partitionCount;
        Assert.assertTrue("test precondition: the composite partition holds several pieces", pieceSplits > 0);
        Assert.assertEquals(fullFrames, drainFull(tt, descending, fullFrames));

        // One window covering every row: one (window, partition) pair per partition, so the
        // walk is the full scan again.
        Assert.assertEquals(fullFrames, drainInterval(tt, descending, "2020-02-01", "2020-02-06", partitionCount + pieceSplits));

        // Windows cutting inside and across pieces of the composite day, and one reaching the
        // plain partition: the delegate counts its pairs, the wrapper adds every split, whether
        // or not a window reaches the piece boundary.
        final long pairs = countIntervalPairs(tt, "2020-02-03T01:00:00", "2020-02-03T02:00:30", "2020-02-03T07:59:00", "2020-02-05T00:00:00");
        drainIntervals(tt, descending, pairs + pieceSplits,
                "2020-02-03T01:00:00", "2020-02-03T02:00:30", "2020-02-03T07:59:00", "2020-02-05T00:00:00");
    }

    // The frames a full scan of the wrapper emits, drained without reading its bound.
    private static long countFullFrames(TableToken tt, boolean descending) {
        final TableReader reader = engine.getReader(tt);
        final PartitionFrameCursor delegate = descending
                ? new FullBwdPartitionFrameCursor().of(reader)
                : new FullFwdPartitionFrameCursor().of(reader);
        try (CompositeAwarePartitionFrameCursor cursor = new CompositeAwarePartitionFrameCursor()) {
            cursor.of(delegate, descending);
            long frames = 0;
            while (cursor.next() != null) {
                frames++;
            }
            return frames;
        }
    }

    // The (window, partition) pairs whose timestamp ranges overlap, from the partition list.
    private static long countIntervalPairs(TableToken tt, String... bounds) throws Exception {
        try (TableReader reader = engine.getReader(tt)) {
            final TimestampDriver driver = ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType());
            long pairs = 0;
            for (int i = 0; i < bounds.length; i += 2) {
                final long lo = driver.parseFloorLiteral(bounds[i]);
                final long hi = driver.parseFloorLiteral(bounds[i + 1]);
                for (int p = 0, n = reader.getPartitionCount(); p < n; p++) {
                    final long partitionLo = reader.getPartitionTimestampByIndex(p);
                    final long partitionHi = p + 1 < n ? reader.getPartitionTimestampByIndex(p + 1) - 1 : reader.getMaxTimestamp();
                    if (lo <= partitionHi && hi >= partitionLo) {
                        pairs++;
                    }
                }
            }
            return pairs;
        }
    }

    /**
     * Builds a WAL table with a genuinely multi-piece (not just single-piece-with-offset) composite
     * partition 0: a full day of filler rows, committed first, then two cold-gap-separated backdated
     * batches bundled into one WAL apply (no drain between them) so transaction clustering has two
     * separate hot strides to cut around instead of one contiguous one.
     */
    private TableToken createMultiPieceTable(String tableName) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
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

    // A delegate that offers no frame-count bound.
    private static class UnboundedFrameCursor implements PartitionFrameCursor {
        @Override
        public void close() {
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return null;
        }

        @Override
        public TableReader getTableReader() {
            return null;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return null;
        }

        @Override
        public PartitionFrame next(long skipTarget) {
            return null;
        }

        @Override
        public boolean reload() {
            return false;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
        }
    }

    /**
     * Clips partition 0's frame to partition rows {@code [lo, hi)}, as an interval-filtered cursor would.
     */
    private static class ClippedFwdPartitionFrameCursor extends FullFwdPartitionFrameCursor implements PartitionFrame {
        private PartitionFrame base;
        private long rowHi;
        private long rowLo;

        @Override
        public ParquetPartitionDecoder getParquetMetaDecoder() {
            return base.getParquetMetaDecoder();
        }

        @Override
        public byte getPartitionFormat() {
            return base.getPartitionFormat();
        }

        @Override
        public int getPartitionIndex() {
            return base.getPartitionIndex();
        }

        @Override
        public long getRowHi() {
            return rowHi;
        }

        @Override
        public long getRowLo() {
            return rowLo;
        }

        @Override
        public @Nullable PartitionFrame next(long skipTarget) {
            final PartitionFrame frame = super.next(skipTarget);
            if (frame == null || frame.getPartitionIndex() != 0) {
                return frame;
            }
            base = frame;
            return this;
        }

        void clipPartition0(long lo, long hi) {
            rowLo = lo;
            rowHi = hi;
            toTop();
        }
    }
}
