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

import io.questdb.cairo.frm.Frame;
import io.questdb.cairo.frm.FrameAlgebra;
import io.questdb.cairo.frm.file.FrameFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;

/**
 * Idle-triggered build step for a composite (multi-piece) partition's compaction.
 * <p>
 * Reads every piece of a partition through the frame layer and appends them, in ascending order, onto one
 * fresh staging directory - the same shape {@link TableWriter#squashSplitPartitions} already uses for the
 * unrelated O3 partition-split feature, one piece at a time instead of one whole partition at a time.
 * <p>
 * Runs off a {@link TableReader} snapshot of the table, so it needs no writer lock: the reader's own
 * {@link PartitionGeometry} and {@link TxReader} describe the pieces to merge, and every piece is opened
 * read-only, bounded to its own rows - {@code frameFactory.openRO(..., pieceHi)}, never {@code pieceRowCount},
 * since {@link io.questdb.cairo.frm.file.FrameImpl} clamps a column's top against whatever is passed as the
 * frame's row count, and that has to be the piece's absolute file-row bound for the clamp to mean anything.
 * Nothing here mutates the live table: the result is a directory on disk nothing yet points at, plus a
 * snapshot of the staleness triple {@link TableWriter#swapCompositePartition} checks before publishing it.
 */
public class CompositePartitionMerger {
    private static final Log LOG = LogFactory.getLog(CompositePartitionMerger.class);

    private CompositePartitionMerger() {
    }

    /**
     * Merges every piece of {@code partitionTimestamp} into one fresh staging directory.
     * <p>
     * The staging directory's column versions - column tops in particular - are tracked in a throwaway
     * {@link ColumnVersionWriter} opened over the table's current, on-disk {@code _cv}, used ONLY to derive
     * the merged directory's per-column tops and never committed. A repack can shift a column's top even
     * though nothing about the column's own history changed: the merge preserves every row's LOGICAL
     * (cumulative) position exactly, but a piece's PHYSICAL file offset can differ from where it sat
     * pre-merge once its dead space is gone, and a top is recorded as a file offset. The derived tops - one
     * per column, in metadata order - travel back inside {@link MergeResult} as plain data, for
     * {@link TableWriter#swapCompositePartition} to transplant into the live {@code ColumnVersionWriter} at
     * swap time; the scratch instance itself is closed before this method returns.
     *
     * @return a snapshot of the merge result, or {@code null} when the partition is not, or is no longer,
     *         composite - there is nothing to merge
     */
    public static MergeResult merge(CairoEngine engine, TableToken tableToken, long partitionTimestamp) {
        try (TableReader reader = engine.getReader(tableToken)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(partitionTimestamp);
            if (partitionIndex < 0 || txReader.getPartitionTimestampByIndex(partitionIndex) != partitionTimestamp) {
                LOG.info().$("partition not found, nothing to compact [table=").$(tableToken)
                        .$(", partitionTimestamp=").$ts(partitionTimestamp).I$();
                return null;
            }
            if (txReader.isPartitionParquet(partitionIndex)) {
                return null;
            }

            final PartitionGeometry geometry = reader.getGeometry();
            if (!geometry.isComposite(partitionIndex)) {
                return null;
            }

            final long oldNameTxn = txReader.getPartitionNameTxn(partitionIndex);
            final long snapshotWriterTxn = geometry.getWriterTxn(partitionIndex);
            final int snapshotPieceCount = geometry.getPieceCount(partitionIndex);

            final CairoConfiguration configuration = engine.getConfiguration();
            final FilesFacade ff = configuration.getFilesFacade();
            final int timestampType = reader.getMetadata().getTimestampType();
            final int partitionBy = reader.getPartitionedBy();

            ColumnVersionWriter scratchColumnVersionWriter = null;
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken.getDirName());
                final int tableRootLen = path.size();

                path.concat(TableUtils.COLUMN_VERSION_FILE_NAME);
                scratchColumnVersionWriter = new ColumnVersionWriter(configuration, path.$(), true);

                path.trimTo(tableRootLen);
                TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, oldNameTxn);
                final Path sourcePartitionPath = path;

                try (Path stagingPath = new Path()) {
                    stagingPath.of(configuration.getDbRoot()).concat(tableToken.getDirName());
                    TableUtils.setPathForCompactingPartition(
                            stagingPath, timestampType, partitionBy, partitionTimestamp, oldNameTxn, snapshotWriterTxn
                    );
                    if (ff.exists(stagingPath.$())) {
                        // A prior attempt at the same writer txn left this behind; the merge below produces
                        // byte-for-byte the same output, so start clean rather than appending onto it twice.
                        ff.rmdir(stagingPath.slash());
                    }
                    TableUtils.createDirsOrFail(ff, stagingPath, configuration.getMkDirMode());

                    try {
                        final FrameFactory frameFactory = engine.getFrameFactory();
                        final int commitMode = configuration.getCommitMode();
                        final long upcomingTxn = txReader.getTxn() + 1L;
                        final long mergedRowCount;

                        try (Frame target = frameFactory.openRW(stagingPath, partitionTimestamp, reader.getMetadata(), scratchColumnVersionWriter, 0)) {
                            for (int p = 0; p < snapshotPieceCount; p++) {
                                final long pieceLo = geometry.getPieceRowOffset(partitionIndex, p);
                                final long pieceRowCount = geometry.getPieceRowCount(partitionIndex, p);
                                final long pieceHi = pieceLo + pieceRowCount;
                                try (
                                        Frame source = frameFactory.openRO(
                                                sourcePartitionPath, partitionTimestamp, reader.getMetadata(),
                                                reader.getColumnVersionReader(), pieceHi
                                        )
                                ) {
                                    FrameAlgebra.append(target, source, pieceLo, pieceHi, upcomingTxn, commitMode);
                                }
                            }
                            mergedRowCount = target.getRowCount();
                        }

                        // The per-column tops the merge just (re)established, read back out of the scratch
                        // writer's in-memory state - never off disk, and never committed through it. This is
                        // the whole reason the scratch writer exists: FrameColumn.saveChanges computed these
                        // as a byproduct of the append loop above, and a repack can genuinely change them
                        // (see the class javadoc), so they cannot simply be copied over from the old directory.
                        final LongList columnTops = new LongList(reader.getMetadata().getColumnCount());
                        for (int i = 0, n = reader.getMetadata().getColumnCount(); i < n; i++) {
                            final int recordIndex = scratchColumnVersionWriter.getRecordIndex(partitionTimestamp, i);
                            columnTops.add(scratchColumnVersionWriter.getColumnTopByIndexOrDefault(recordIndex, partitionTimestamp, i, mergedRowCount));
                        }

                        LOG.info().$("composite partition merged into staging [table=").$(tableToken)
                                .$(", partitionTimestamp=").$ts(partitionTimestamp)
                                .$(", oldNameTxn=").$(oldNameTxn)
                                .$(", snapshotWriterTxn=").$(snapshotWriterTxn)
                                .$(", pieceCount=").$(snapshotPieceCount)
                                .$(", mergedRowCount=").$(mergedRowCount)
                                .I$();

                        return new MergeResult(
                                partitionTimestamp, oldNameTxn, snapshotWriterTxn, snapshotPieceCount,
                                mergedRowCount, columnTops
                        );
                    } catch (Throwable th) {
                        // Best-effort: an incomplete staging directory is harmless clutter, never a table-state
                        // problem, but there is no reason to leave it behind when the build itself failed.
                        if (!ff.rmdir(stagingPath.slash())) {
                            LOG.error().$("could not remove incomplete compaction staging dir [path=").$(stagingPath).I$();
                        }
                        throw th;
                    }
                }
            } finally {
                // Read back out of, never written back into: the scratch instance is discarded here either
                // way, whether this attempt succeeded or failed.
                Misc.free(scratchColumnVersionWriter);
            }
        }
    }

    /**
     * The outcome of one {@link #merge} attempt: everything {@link TableWriter#swapCompositePartition} needs
     * to validate the attempt is still fresh, locate the staging directory it produced, and adopt the column
     * tops built for it. Plain data - nothing here holds a file handle or a native resource, so a stale
     * result can simply be dropped.
     */
    public static final class MergeResult {
        private final LongList columnTops;
        private final long mergedRowCount;
        private final long oldNameTxn;
        private final long partitionTimestamp;
        private final int snapshotPieceCount;
        private final long snapshotWriterTxn;

        MergeResult(
                long partitionTimestamp,
                long oldNameTxn,
                long snapshotWriterTxn,
                int snapshotPieceCount,
                long mergedRowCount,
                LongList columnTops
        ) {
            this.partitionTimestamp = partitionTimestamp;
            this.oldNameTxn = oldNameTxn;
            this.snapshotWriterTxn = snapshotWriterTxn;
            this.snapshotPieceCount = snapshotPieceCount;
            this.mergedRowCount = mergedRowCount;
            this.columnTops = columnTops;
        }

        /**
         * The merged directory's new top for each column, indexed positionally exactly as
         * {@link io.questdb.cairo.sql.RecordMetadata#getColumnCount()} enumerates them.
         */
        public LongList getColumnTops() {
            return columnTops;
        }

        public long getMergedRowCount() {
            return mergedRowCount;
        }

        public long getOldNameTxn() {
            return oldNameTxn;
        }

        public long getPartitionTimestamp() {
            return partitionTimestamp;
        }

        public int getSnapshotPieceCount() {
            return snapshotPieceCount;
        }

        public long getSnapshotWriterTxn() {
            return snapshotWriterTxn;
        }
    }
}
