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
    public long countBefore(long timestamp) {
        return rowCount == 0 || timestamp == Long.MIN_VALUE ? 0 : findRow(timestamp - 1) + 1;
    }

    @Override
    public long countThrough(long timestamp) {
        return rowCount == 0 ? 0 : findRow(timestamp) + 1;
    }

    @Override
    public long maxTimestampUpperBound() {
        return maxTimestampApprox;
    }

    @Override
    public long minTimestampLowerBound() {
        return minTimestampApprox;
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

    /**
     * The last directory row of the whole partition whose timestamp is at or below {@code value}, or
     * {@code -1} when the partition starts above it.
     */
    private long findRow(long value) {
        if (geometry.getPieceTimestampLo(partitionIndex, 0) > value) {
            return -1;
        }
        // The last piece starting at or below the value.
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
