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

package io.questdb.cairo.frm;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;

/**
 * Frame-level algebra: whole partitions and pieces in, bytes appended at a target's tail.
 * <p>
 * Used for partition squashing in {@link io.questdb.cairo.TableWriter}, and for writing COMPOSITE
 * partitions, where the two operations map onto the two actions that write anything: a NEW_PIECE is
 * {@link #append} of the incoming rows, and a MERGE is {@link #merge} of a piece with the rows landing
 * inside it. A KEEP writes nothing at all, which is why it has no operation here.
 */
public class FrameAlgebra {

    public static void append(Frame target, Frame source, long upcomingTableTxn, int commitMode) {
        append(target, source, 0, source.getRowCount(), upcomingTableTxn, commitMode);
    }

    /**
     * @param upcomingTableTxn the upcoming table {@code _txn} (typically
     *                         {@code txWriter.getTxn() + 1L}). Indexed
     *                         columns tag posting-index chain entries
     *                         published during this append with the value
     *                         so a partial publish (commit fails before
     *                         landing) is droppable by recovery.
     */
    public static void append(Frame target, Frame source, long sourceLo, long sourceHi, long upcomingTableTxn, int commitMode) {
        if (sourceLo < sourceHi) {
            for (int i = 0, n = source.columnCount(); i < n; i++) {
                try (
                        FrameColumn sourceColumn = source.createColumn(i);
                        FrameColumn targetColumn = target.createColumn(i)
                ) {
                    if (sourceColumn.getColumnType() >= 0) {
                        targetColumn.setUpcomingTableTxn(upcomingTableTxn);
                        append(targetColumn, target.getRowCount(), sourceColumn, sourceLo, sourceHi, commitMode);
                        target.saveChanges(targetColumn);
                    }
                }
            }
            target.setRowCount(target.getRowCount() + (sourceHi - sourceLo));
        }
    }

    /**
     * Appends the MERGE of two frames to {@code target}'s tail, interleaved by {@code mergeIndexAddr}.
     * <p>
     * {@link #append} carries ONE source through unchanged, and is what writes a brand-new piece: the
     * incoming rows go down at the tail as they are. This carries TWO, in the order the merge index
     * dictates, and is what rewrites a piece the incoming rows land inside - the piece and the batch go out
     * as one image at the tail, in timestamp order, and the piece's old bytes become dead space.
     * <p>
     * The index is the standard 16-bytes-per-row form {@code Vect.mergeTwoLongIndexesAsc} produces from the
     * piece's designated-timestamp slice and the sorted O3 index: a timestamp, then a row id whose top bit
     * says which side it came from. So the row count appended is the index's row count, and both sources
     * are read in a single pass.
     * <p>
     * Column TOPS are each column's own business, exactly as they are in {@link #append}: a source column
     * knows the row its data starts at and offsets its own reads, and the target knows where its data
     * starts and offsets its own writes. Nothing here has to reason about them.
     *
     * @param mergeIndexAddr native address of the merge index over {@code [source1Lo, source1Hi)} and
     *                       {@code [source2Lo, source2Hi)}
     */
    public static void merge(
            Frame target,
            Frame source1,
            long source1Lo,
            long source1Hi,
            Frame source2,
            long source2Lo,
            long source2Hi,
            long mergeIndexAddr,
            long mergeIndexRows,
            long upcomingTableTxn,
            int commitMode
    ) {
        // The caller passes the index's OWN length rather than letting this derive it from the two source
        // ranges. A deduplicating commit drops rows, so the merged image is shorter than both sides added
        // together, and only whoever built the index knows by how much.
        assert mergeIndexRows <= (source1Hi - source1Lo) + (source2Hi - source2Lo);
        if (mergeIndexRows > 0) {
            for (int i = 0, n = source1.columnCount(); i < n; i++) {
                try (
                        FrameColumn sourceColumn1 = source1.createColumn(i);
                        FrameColumn sourceColumn2 = source2.createColumn(i);
                        FrameColumn targetColumn = target.createColumn(i)
                ) {
                    if (sourceColumn1.getColumnType() >= 0) {
                        targetColumn.setUpcomingTableTxn(upcomingTableTxn);
                        targetColumn.merge(
                                target.getRowCount(),
                                sourceColumn1,
                                source1Lo,
                                source1Hi,
                                sourceColumn2,
                                source2Lo,
                                source2Hi,
                                mergeIndexAddr,
                                mergeIndexRows,
                                commitMode
                        );
                        target.saveChanges(targetColumn);
                    }
                }
            }
            target.setRowCount(target.getRowCount() + mergeIndexRows);
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean isColumnReplaceIdentical(
            int columnIndex,
            Frame partitionFrame,
            long partitionLo,
            long partitionHi,
            Frame commitFrame,
            long commitLo,
            long commitHi,
            long mergeIndexAddr,
            long mergeIndexRows
    ) {
        try (
                FrameColumn partitionColumn = partitionFrame.createColumn(columnIndex);
                FrameColumn commitColumn = commitFrame.createColumn(columnIndex)
        ) {
            if (partitionColumn.getColumnType() >= 0) {
                return isColumnReplaceIdentical(
                        partitionColumn,
                        partitionLo,
                        partitionHi,
                        commitColumn,
                        commitLo,
                        commitHi,
                        mergeIndexAddr,
                        mergeIndexRows
                );
            }
        }
        return true;
    }

    public static boolean isDesignatedTimestampColumnReplaceIdentical(
            int columnIndex,
            Frame partitionFrame,
            long partitionLo,
            long partitionHi,
            Frame commitFrame,
            long commitLo,
            long commitHi
    ) {
        try (
                FrameColumn partitionColumn = partitionFrame.createColumn(columnIndex);
                FrameColumn commitColumn = commitFrame.createColumn(columnIndex)
        ) {
            assert partitionColumn.getColumnTop() == 0;

            long partitionDataAddr = partitionColumn.getContiguousDataAddr(partitionHi);
            long commitDataAddr = commitColumn.getContiguousDataAddr(commitHi);

            return isDesignatedTimestampColumnReplaceIdentical0(
                    partitionDataAddr + partitionLo * Long.BYTES,
                    commitDataAddr + commitLo * Long.BYTES * 2,
                    partitionHi - partitionLo
            );

        }
    }

    private static void append(FrameColumn targetColumn, long targetRowCount, FrameColumn sourceColumn, long sourceLo, long sourceHi, int commitMode) {
        int columnType = sourceColumn.getColumnType();
        if (columnType != targetColumn.getColumnType()) {
            throw new UnsupportedOperationException();
        }

        final long sourceColumnTop = sourceColumn.getColumnTop();
        final long nullPaddingRowCount = Math.max(0, Math.min(sourceColumnTop, sourceHi) - sourceLo);
        if (nullPaddingRowCount > 0) {
            long targetColTop = targetColumn.getColumnTop();
            if (targetColTop == targetRowCount) {
                // Increase target column top
                targetColumn.addTop(nullPaddingRowCount);
            } else {
                // Pad target with NULLs
                targetColumn.appendNulls(targetRowCount, nullPaddingRowCount, commitMode);
            }
        }

        if (sourceColumnTop < sourceHi) {
            targetColumn.append(
                    targetRowCount + nullPaddingRowCount,
                    sourceColumn,
                    sourceLo + nullPaddingRowCount,
                    sourceHi,
                    commitMode
            );
        }
    }

    private static boolean isColumnReplaceIdentical(
            FrameColumn partitionColumn,
            long partitionLo,
            long partitionHi,
            FrameColumn commitColumn,
            long commitLo,
            long commitHi,
            long mergeIndexAddr,
            long mergeIndexRows
    ) {
        long partitionAddrAux = partitionColumn.getContiguousAuxAddr(partitionHi);
        long partitionDataAddr = partitionColumn.getContiguousDataAddr(partitionHi);

        long commitAuxAddr = commitColumn.getContiguousAuxAddr(commitHi);
        long commitDataAddr = commitColumn.getContiguousDataAddr(commitHi);

        int columnType = partitionColumn.getColumnType();
        short columnTypeTag = ColumnType.tagOf(columnType);

        return isColumnReplaceIdentical(
                columnTypeTag,
                ColumnType.isVarSize(columnType) ? -1 : ColumnType.sizeOf(columnType),
                partitionColumn.getColumnTop(),
                partitionLo,
                partitionHi,
                partitionAddrAux,
                partitionDataAddr,
                commitColumn.getColumnTop(),
                commitLo,
                commitHi,
                commitAuxAddr,
                commitDataAddr,
                mergeIndexAddr,
                mergeIndexRows,
                TableUtils.getNullLong(columnTypeTag, 0),
                TableUtils.getNullLong(columnTypeTag, 1),
                TableUtils.getNullLong(columnTypeTag, 2),
                TableUtils.getNullLong(columnTypeTag, 3)
        );
    }

    private static native boolean isColumnReplaceIdentical(
            int columnTypeTag,
            int columnSize,
            long columnTop1,
            long lo1,
            long hi1,
            long auxAddr1,
            long dataAddr1,
            long columnTop2,
            long lo2,
            long hi2,
            long auxAddr2,
            long dataAddr2,
            long mergeIndexAddr,
            long mergeIndexSize,
            long nullLong,
            long nullLong1,
            long nullLong2,
            long nullLong3
    );

    private static native boolean isDesignatedTimestampColumnReplaceIdentical0(
            long partitionTsAddr,
            long commitTsAddr,
            long rowCount
    );
}
