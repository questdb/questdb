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

package io.questdb.cairo;

import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

/**
 * Reads a DETACHED composite partition artifact (a {@code <day>.detached} / {@code <day>.attachable}
 * directory) as ATTACH sees it.
 * <p>
 * {@code detachPartition} copies {@code _meta}, {@code _cv} and {@code _txn} into the artifact, and a
 * composite day keeps its data one level down in per-cell directories. The copied {@code _txn} carries
 * one attached-partition entry per cell, each with its cellKey, so the artifact's cells can be
 * enumerated authoritatively from metadata.
 * <p>
 * <b>Never parse a cell directory name to recover this.</b> Segment rendering is deliberately one-way:
 * it is path-safety encoded, has its own NULL token, and several dimension kinds render through
 * {@code putCellSegmentPathSafe}. Cell-qualified DROP already resolves names by RENDERING each attached
 * cellKey and comparing, precisely to avoid a reverse parse. A parser here would be a second, lossier
 * source of truth for the same mapping.
 */
public final class CompositeDetachedArtifact {

    private CompositeDetachedArtifact() {
    }

    /**
     * Collects, into {@code out}, the cellKey of every entry in the artifact's {@code _txn} whose
     * partition timestamp is {@code partitionTimestamp}. A plain artifact yields exactly one entry,
     * with cellKey 0.
     * <p>
     * The artifact's {@code _txn} is the whole table's {@code _txn} as of the detach, so it lists other
     * partitions too -- filtering by timestamp is required, not incidental.
     *
     * @param artifactRoot path to the artifact directory itself; left trimmed back to its original
     *                     length on return
     */
    public static void readCellKeys(
            FilesFacade ff,
            Path artifactRoot,
            int timestampType,
            int partitionBy,
            long partitionTimestamp,
            IntList out
    ) {
        out.clear();
        final int rootLen = artifactRoot.size();
        try (TxReader txReader = new TxReader(ff)) {
            txReader.ofRO(artifactRoot.concat(TableUtils.TXN_FILE_NAME).$(), timestampType, partitionBy);
            txReader.unsafeLoadAll();
            // getPartitionCount()/getPartitionCellKey() are stride-aware: the artifact's _txn carries the
            // self-describing stride marker, so a composite artifact reads back stride 8 and a plain one
            // stride 4 (cellKey always 0) without the caller knowing which it holds.
            for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
                if (txReader.getPartitionTimestampByIndex(i) == partitionTimestamp) {
                    out.add(txReader.getPartitionCellKey(i));
                }
            }
        } finally {
            artifactRoot.trimTo(rootLen);
        }
    }

    /**
     * Refuses an artifact that did not come from this table.
     * <p>
     * A cellKey is table-local, and the artifact carries {@code _meta}, {@code _cv} and {@code _txn} but
     * NOT the dimension dictionaries or the {@code _cell} registry -- those live at the table root. So a
     * foreign artifact's cellKeys cannot be decoded to dimension values here, and attaching it would
     * bind its cells to whatever local cells happen to share those ordinals: silently wrong data filed
     * under a different dimension value.
     * <p>
     * Cross-table attach needs a self-describing artifact (the interners copied in, or a
     * cellKey-to-values manifest written at detach). Until then this refuses.
     */
    public static void checkSameTable(
            FilesFacade ff,
            CairoConfiguration configuration,
            Path artifactRoot,
            int expectedTableId,
            CharSequence tableName
    ) {
        final int rootLen = artifactRoot.size();
        try (TableReaderMetadata artifactMeta = new TableReaderMetadata(configuration)) {
            final LPSZ metaPath = artifactRoot.concat(TableUtils.META_FILE_NAME).$();
            if (!ff.exists(metaPath)) {
                throw CairoException.critical(0)
                        .put("composite partitioning does not yet support attaching a partition from another table")
                        .put(" [table=").put(tableName).put(", reason=artifact carries no _meta]");
            }
            artifactMeta.loadMetadata(metaPath);
            final int artifactTableId = artifactMeta.getTableId();
            if (artifactTableId != expectedTableId) {
                throw CairoException.critical(0)
                        .put("composite partitioning does not yet support attaching a partition from another table")
                        .put(" [table=").put(tableName)
                        .put(", tableId=").put(expectedTableId)
                        .put(", artifactTableId=").put(artifactTableId).put(']');
            }
        } finally {
            artifactRoot.trimTo(rootLen);
        }
    }

    /**
     * Folds the designated-timestamp min and max across the artifact's cells into
     * {@code minMaxOut[0]}/{@code minMaxOut[1]}.
     * <p>
     * Deliberately never reads the container root. A detached composite artifact carries ZERO-BYTE
     * phantom {@code <column>.d} files there -- measured, not assumed -- so a root read returns -1 for
     * both bounds. Worse, if such a file ever held bytes it would yield silently WRONG bounds rather
     * than an error.
     *
     * @param cellSegments rendered cell segment names, in the same order as {@code cellSizes}
     */
    public static void readMinMaxTimestamps(
            FilesFacade ff,
            Path artifactRoot,
            CharSequence tsColumnName,
            int timestampType,
            ObjList<CharSequence> cellSegments,
            LongList cellSizes,
            long[] minMaxOut
    ) {
        final int rootLen = artifactRoot.size();
        try {
            minMaxOut[0] = -1;
            minMaxOut[1] = -1;
            boolean first = true;
            for (int i = 0, n = cellSegments.size(); i < n; i++) {
                artifactRoot.trimTo(rootLen).concat(cellSegments.getQuick(i));
                readBounds(ff, artifactRoot, tsColumnName, timestampType, cellSizes.getQuick(i), minMaxOut, first);
                if (minMaxOut[0] < 0 || minMaxOut[1] < 0) {
                    // One unreadable cell fails the whole fold: a day whose bounds are computed from a
                    // subset of its cells is worse than one that refuses to attach.
                    return;
                }
                first = false;
            }
        } finally {
            artifactRoot.trimTo(rootLen);
        }
    }

    private static void readBounds(
            FilesFacade ff,
            Path dir,
            CharSequence tsColumnName,
            int timestampType,
            long rows,
            long[] minMaxOut,
            boolean reset
    ) {
        final int len = dir.size();
        try {
            final long fd = ff.openRO(TableUtils.dFile(dir, tsColumnName, TableUtils.COLUMN_NAME_TXN_NONE));
            if (fd < 0) {
                minMaxOut[0] = -1;
                minMaxOut[1] = -1;
                return;
            }
            try {
                final long lo = ff.readNonNegativeLong(fd, 0);
                final long hi = ff.readNonNegativeLong(fd, (rows - 1) * ColumnType.sizeOf(timestampType));
                if (reset || lo < 0 || hi < 0) {
                    minMaxOut[0] = lo;
                    minMaxOut[1] = hi;
                } else {
                    minMaxOut[0] = Math.min(minMaxOut[0], lo);
                    minMaxOut[1] = Math.max(minMaxOut[1], hi);
                }
            } finally {
                ff.close(fd);
            }
        } finally {
            dir.trimTo(len);
        }
    }

    /**
     * Returns the total row count the artifact holds for {@code partitionTimestamp}, summed across every
     * cell. A plain artifact has exactly one entry, so the sum degenerates to it.
     */
    public static long readSize(
            FilesFacade ff,
            Path artifactRoot,
            int timestampType,
            int partitionBy,
            long partitionTimestamp
    ) {
        final int rootLen = artifactRoot.size();
        try (TxReader txReader = new TxReader(ff)) {
            txReader.ofRO(artifactRoot.concat(TableUtils.TXN_FILE_NAME).$(), timestampType, partitionBy);
            txReader.unsafeLoadAll();
            // Deliberately NOT getPartitionRowCountByTimestamp: that resolves through
            // findAttachedPartitionRawIndexByLoTimestamp, which hardcodes cellKey = 0, so on a composite
            // artifact it returns the FIRST cell's row count and calls it the day's.
            long size = 0;
            for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
                if (txReader.getPartitionTimestampByIndex(i) == partitionTimestamp) {
                    size += txReader.getPartitionSize(i);
                }
            }
            return size;
        } finally {
            artifactRoot.trimTo(rootLen);
        }
    }
}
