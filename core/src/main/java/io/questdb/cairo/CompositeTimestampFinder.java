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
 * a rewritten piece at the tail, above pieces that sort before it. So the search runs per piece, over the
 * one range of file rows that is both contiguous and sorted, and shifts the answer back.
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
    private int pieceCount;
    private TableReader reader;
    private long rowCount;
    private int timestampColumnOffset;

    @Override
    public void clear() {
        column = null;
        geometry = null;
        partitionIndex = -1;
        pieceCount = 0;
        rowCount = 0;
    }

    @Override
    public long findTimestamp(long value, long rowLo, long rowHi) {
        long result = rowLo - 1;
        long cumulativeLo = 0;
        for (int i = 0; i < pieceCount; i++) {
            final long pieceRows = geometry.getPieceRowCount(partitionIndex, i);
            final long lo = Math.max(rowLo, cumulativeLo);
            final long hi = Math.min(rowHi, cumulativeLo + pieceRows - 1);
            final long shift = geometry.getPieceRowOffset(partitionIndex, i) - cumulativeLo;
            cumulativeLo += pieceRows;
            if (lo > hi) {
                continue;
            }
            // The piece bounds answer most pieces without reading a row. They bracket the piece's own
            // timestamps, so a piece wholly at or below the value contributes its whole clipped range, and
            // the first piece wholly above it ends the walk - every later piece is higher still.
            if (geometry.getPieceTimestampLo(partitionIndex, i) > value) {
                break;
            }
            if (geometry.getPieceTimestampHi(partitionIndex, i) <= value) {
                result = hi;
                continue;
            }
            long idx = Vect.binarySearch64Bit(column.getPageAddress(0), value, lo + shift, hi + shift, BIN_SEARCH_SCAN_DOWN);
            if (idx < 0) {
                idx = -idx - 2;
            }
            return Math.max(result, idx - shift);
        }
        return result;
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
        this.pieceCount = geometry.getPieceCount(partitionIndex);
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
}
