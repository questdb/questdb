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

package io.questdb.cairo;

import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.Mutable;
import io.questdb.std.Vect;

import static io.questdb.std.Vect.BIN_SEARCH_SCAN_DOWN;

/**
 * Timestamp finder over a COMPOSITE partition - several pieces sharing one set of column files.
 * <p>
 * Every row index this class takes and returns is a DIRECTORY row, the {@code [0, liveRows)} space the
 * partition frame speaks. Pieces are ordered by timestamp and do not overlap, so that space is ascending
 * end to end and a binary search over it is sound. The FILE rows underneath are not: a merge-append parks
 * a rewritten piece at the tail, above pieces that sort before it. So the search runs inside one piece,
 * over the one range of file rows that is both contiguous and sorted, and shifts the answer back.
 * <p>
 * Two binary searches, never a walk. The first picks the piece out of the geometry's timestamp bounds and
 * touches no column data; the second runs inside that piece. A directory can hold thousands of pieces once
 * a fine cut floor has been applied for a while, so neither step may be linear in the piece count.
 * <p>
 * This is the native analogue of {@link ParquetTimestampFinder}, which searches a row group at a time for
 * the same reason.
 */
public class CompositeTimestampFinder implements TimestampFinder, Mutable {
    private MemoryR column;
    private PartitionGeometry geometry;
    private long maxTimestampApprox;
    private long minTimestampApprox;
    private int partitionIndex = -1;
    private TableReader reader;
    private long rowCount;
    private int timestampColumnOffset;

    @Override
    public void clear() {
        column = null;
        geometry = null;
        partitionIndex = -1;
        rowCount = 0;
    }

    @Override
    public long findTimestamp(long value, long rowLo, long rowHi) {
        // Timestamps do not decrease across the directory, so the qualifying rows are a prefix and the
        // window only has to clamp it. Resolving against the whole directory rather than the window costs
        // nothing here - both are one binary search - and keeps the piece arithmetic out of the clamp.
        final long row = findRow(value);
        return row < rowLo ? rowLo - 1 : Math.min(row, rowHi);
    }

    @Override
    public long maxTimestampApproxFromMetadata() {
        return maxTimestampApprox;
    }

    @Override
    public long maxTimestampExact() {
        return timestampAt(rowCount - 1);
    }

    @Override
    public long minTimestampApproxFromMetadata() {
        return minTimestampApprox;
    }

    @Override
    public long minTimestampExact() {
        return timestampAt(0);
    }

    public CompositeTimestampFinder of(TableReader reader, int partitionIndex, int timestampIndex, long rowCount) {
        this.timestampColumnOffset = TableReader.getPrimaryColumnIndex(reader.getColumnBase(partitionIndex), timestampIndex);
        this.reader = reader;
        this.geometry = reader.getGeometry();
        this.partitionIndex = partitionIndex;
        this.rowCount = rowCount;
        this.minTimestampApprox = reader.getPartitionMinTimestampFromMetadata(partitionIndex);
        this.maxTimestampApprox = reader.getPartitionMaxTimestampFromMetadata(partitionIndex);
        return this;
    }

    @Override
    public void prepare() {
        this.column = reader.getColumn(timestampColumnOffset);
    }

    @Override
    public long timestampAt(long rowIndex) {
        final int piece = geometry.findPieceByRow(partitionIndex, rowIndex);
        return column.getLong((rowIndex + geometry.getPieceShift(partitionIndex, piece)) * 8);
    }

    /**
     * The last directory row of the whole partition whose timestamp is at or below {@code value}, or
     * {@code -1} when the partition starts above it.
     */
    private long findRow(long value) {
        if (geometry.getPieceTimestampLo(partitionIndex, 0) > value) {
            return -1;
        }
        // The last piece starting at or below the value. Pieces ascend by tsLo, so this is a binary search,
        // and its own bounds answer it outright unless the value falls strictly inside it.
        final int piece = geometry.findPiece(partitionIndex, value);
        final long cumulativeLo = geometry.getPieceCumulativeLo(partitionIndex, piece);
        final long pieceRows = geometry.getPieceRowCount(partitionIndex, piece);
        if (geometry.getPieceTimestampHi(partitionIndex, piece) <= value) {
            return cumulativeLo + pieceRows - 1;
        }
        final long shift = geometry.getPieceShift(partitionIndex, piece);
        long idx = Vect.binarySearch64Bit(
                column.getPageAddress(0),
                value,
                cumulativeLo + shift,
                cumulativeLo + pieceRows - 1 + shift,
                BIN_SEARCH_SCAN_DOWN
        );
        if (idx < 0) {
            idx = -idx - 2;
        }
        // A miss at the piece's own floor lands on cumulativeLo - 1, which is the last row of the piece
        // below it - the right answer, and the reason this needs no fallback of its own.
        return idx - shift;
    }
}
