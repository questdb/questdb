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
 * Used for partition squashing in {@link io.questdb.cairo.TableWriter}.
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
     * Checks that a dedup merge leaves one column unchanged, on raw buffers with no
     * column tops: side 1 is the existing data ({@code rowCount} rows), side 2 the O3
     * commit. The merge index must hold exactly {@code rowCount} entries, entry
     * {@code i} standing for existing row {@code i}: either kept (from side 1) or
     * replaced by an O3 row (from side 2). Each replacement is compared value by
     * value, NULLs included, stopping at the first difference. This is the native
     * comparison {@code TableWriter.checkDedupCommitIdenticalToPartition} runs per
     * non-key column, minus the column-top handling.
     *
     * @param columnType     column type; the buffers use its native layout
     * @param rowCount       existing row count, also the merge index entry count
     * @param srcAuxAddr     existing aux vector (var-size columns only, else 0)
     * @param srcDataAddr    existing data vector
     * @param o3AuxAddr      O3 aux vector (var-size columns only, else 0)
     * @param o3DataAddr     O3 data vector, indexed by the merge index's O3 row ids
     * @param mergeIndexAddr merge index, {@code rowCount} (timestamp, row id) entries
     * @return true if every O3 row the merge index takes equals the existing row it replaces
     */
    public static boolean isColumnMergeIdentical(
            int columnType,
            long rowCount,
            long srcAuxAddr,
            long srcDataAddr,
            long o3AuxAddr,
            long o3DataAddr,
            long mergeIndexAddr
    ) {
        assert rowCount > 0;
        final short columnTypeTag = ColumnType.tagOf(columnType);
        return isColumnReplaceIdentical(
                columnTypeTag,
                ColumnType.isVarSize(columnType) ? -1 : ColumnType.sizeOf(columnType),
                0,
                0,
                rowCount,
                srcAuxAddr,
                srcDataAddr,
                0,
                // The O3 range is read only when every existing row is in the column
                // top, which a zero column top rules out; O3 row ids are absolute.
                0,
                0,
                o3AuxAddr,
                o3DataAddr,
                mergeIndexAddr,
                rowCount,
                TableUtils.getNullLong(columnTypeTag, 0),
                TableUtils.getNullLong(columnTypeTag, 1),
                TableUtils.getNullLong(columnTypeTag, 2),
                TableUtils.getNullLong(columnTypeTag, 3)
        );
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
