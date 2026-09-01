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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import org.jetbrains.annotations.Nullable;

/**
 * Where a native partition's page frames begin and end.
 * <p>
 * A frame is cut at three places: the page frame row limit, a composite partition's piece
 * boundaries, and a column top. {@link FwdTableReaderPageFrameCursor#computeNativeFrame} applies
 * the same three, in the same order, while walking an open partition; {@link #of} applies them to
 * a partition that is NOT open, from column version metadata alone. The two must agree on the
 * frame count, because a cursor that pre-computes frames and later opens the partition to fill in
 * their addresses asserts exactly that - so the rule lives here rather than in either caller.
 */
final class NativeFrameBoundaries {
    private final LongList columnTops = new LongList();
    // Exclusive high row of each frame, in order. A frame's low row is the previous entry, or 0.
    private final LongList frameHis = new LongList();

    /**
     * Rows per page frame, before any cut at a piece boundary or a column top. Sized so the
     * partition splits across the shared query workers without leaving a tiny trailing frame.
     */
    static long calculatePageFrameRowLimit(
            long partitionLo,
            long partitionHi,
            long pageFrameMinRows,
            long pageFrameMaxRows,
            int sharedQueryWorkerCount
    ) {
        final int workerCount = Math.max(sharedQueryWorkerCount, 1);
        long rowsPerFrame = Math.min(pageFrameMaxRows, Math.max(pageFrameMinRows, (partitionHi - partitionLo) / workerCount));
        final long lastFrameSize = (partitionHi - partitionLo) % rowsPerFrame;
        if (lastFrameSize > 0 && lastFrameSize < pageFrameMinRows) {
            // Adjust the limit, so that we don't have tiny trailing frames.
            final long frameCount = Math.max((partitionHi - partitionLo) / rowsPerFrame, 1);
            rowsPerFrame += (lastFrameSize + frameCount - 1) / frameCount;
        }
        return rowsPerFrame;
    }

    public long getQuick(int index) {
        return frameHis.getQuick(index);
    }

    /**
     * Computes the boundaries for one partition, replacing whatever the previous call left. Read
     * them back with {@link #size()} and {@link #getQuick(int)}.
     */
    public void of(
            TableReader tableReader,
            ColumnVersionReader columnVersionReader,
            IntList columnIndexes,
            int columnCount,
            int partitionIndex,
            long partitionTimestamp,
            long partitionRowCount,
            int pageFrameMinRows,
            int pageFrameMaxRows,
            int workerCount
    ) {
        populateColumnTops(tableReader, columnVersionReader, columnIndexes, columnCount, partitionTimestamp, partitionRowCount);

        final long pageFrameRowLimit = calculatePageFrameRowLimit(
                0,
                partitionRowCount,
                pageFrameMinRows,
                pageFrameMaxRows,
                workerCount
        );
        // Resolving the geometry costs a read, so only a composite partition pays for it. Every
        // piece shift is 0 for a partition that is not composite, which is every partition of an
        // unsplit table.
        final PartitionGeometry geometry = tableReader.getTxFile().isPartitionComposite(partitionIndex)
                ? tableReader.getGeometry()
                : null;

        frameHis.clear();
        long lo = 0;
        while (lo < partitionRowCount) {
            final long hi = frameHi(
                    geometry,
                    columnCount,
                    partitionIndex,
                    lo,
                    Math.min(partitionRowCount, lo + pageFrameRowLimit)
            );
            frameHis.add(hi);
            lo = hi;
        }
    }

    public int size() {
        return frameHis.size();
    }

    /**
     * Exclusive high row of the frame starting at {@code lo}, cut first at a composite piece
     * boundary and then at a column top. {@code hiLimit} is the high row before either cut.
     */
    private long frameHi(
            @Nullable PartitionGeometry geometry,
            int columnCount,
            int partitionIndex,
            long lo,
            long hiLimit
    ) {
        long adjustedHi = hiLimit;
        long pieceShift = 0;
        // A COMPOSITE partition is several PIECES over one set of column files, and each piece sits at
        // its own place in those files. A frame spanning two pieces would address the dead space
        // between them, so it is cut at the piece boundary and carries that piece's SHIFT, which turns
        // a partition row into a file row.
        if (geometry != null) {
            final int piece = geometry.findPieceByRow(partitionIndex, lo);
            pieceShift = geometry.getPieceShift(partitionIndex, piece);
            final long pieceHi = geometry.getPieceCumulativeLo(partitionIndex, piece)
                    + geometry.getPieceRowCount(partitionIndex, piece);
            if (pieceHi > lo && pieceHi < adjustedHi) {
                adjustedHi = pieceHi;
            }
        }
        // The column top cut has to come AFTER the piece is known: a top is a FILE row while lo and
        // adjustedHi are PARTITION rows, and the two are only comparable once the shift is in hand.
        // They coincide for a partition with no geometry. A frame straddling a top would address the
        // column below its first stored row.
        for (int i = 0; i < columnCount; i++) {
            final long top = columnTops.getQuick(i) - pieceShift;
            if (top > lo && top < adjustedHi) {
                adjustedHi = top;
            }
        }
        return adjustedHi;
    }

    /**
     * Reads each column's top - the first row where it has data, with the rows before it NULL - from
     * column version metadata. A column absent from this partition gets a top of the partition's row
     * count, making it all-null.
     */
    private void populateColumnTops(
            TableReader tableReader,
            ColumnVersionReader columnVersionReader,
            IntList columnIndexes,
            int columnCount,
            long partitionTimestamp,
            long partitionRowCount
    ) {
        // Reader metadata, not factory metadata, for the writer index lookup: factory metadata (a
        // SelectedRecordCursorFactory's, say) may not implement getWriterIndex().
        final RecordMetadata readerMetadata = tableReader.getMetadata();
        columnTops.clear();
        for (int i = 0; i < columnCount; i++) {
            final int readerColumnIndex = columnIndexes.getQuick(i);
            final int writerIndex = readerMetadata.getWriterIndex(readerColumnIndex);
            final int recordIndex = columnVersionReader.getRecordIndex(partitionTimestamp, writerIndex);
            if (recordIndex > -1) {
                columnTops.add(columnVersionReader.getColumnTopByIndex(recordIndex));
            } else if (columnVersionReader.getColumnTopPartitionTimestamp(writerIndex) <= partitionTimestamp) {
                columnTops.add(0); // column exists from start, no top
            } else {
                columnTops.add(partitionRowCount); // column doesn't exist - all-null
            }
        }
    }
}
