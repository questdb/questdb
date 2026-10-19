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

import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Wraps a {@link PartitionFrameCursor}, translating each delegate frame's directory-cumulative {@code [rowLo, rowHi)}
 * into one or more physical, file-row-space sub-frames.
 * <p>
 * A parquet partition gets the same treatment once an in-place O3 update has moved some of its row groups to other
 * index rows (see {@link ParquetMetaFileReader#getRowGroupIndexRowLo}): one sub-frame per row group, in index rows.
 */
public class CompositeAwarePartitionFrameCursor implements PartitionFrameCursor {
    private final CompositePartitionFrame frame = new CompositePartitionFrame();
    private PartitionFrameCursor delegate;
    private boolean descending;
    private long pendingCumHi;
    private long pendingCumLo;
    // Set while the pending range is a parquet partition with an index row map, null otherwise.
    private ParquetPartitionDecoder pendingParquetDecoder;
    private int pendingPartitionIndex = -1;
    private int pendingPieceOrdinal;
    // Partition row of the first row of row group pendingPieceOrdinal, parquet only.
    private long pendingRowGroupStartRow;

    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        delegate.calculateSize(counter);
    }

    @Override
    public void close() {
        delegate = Misc.free(delegate);
        clearPending();
    }

    @Override
    public TableReader getTableReader() {
        return delegate.getTableReader();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return delegate.getSymbolTable(columnIndex);
    }

    @Override
    public @Nullable LongList getIntervals() {
        return delegate.getIntervals();
    }

    @Override
    public boolean hasIntervalFilter() {
        return delegate.hasIntervalFilter();
    }

    @Override
    public @Nullable PartitionFrame next(long skipTarget) {
        if (pendingPartitionIndex >= 0) {
            final PartitionFrame piece = pendingParquetDecoder != null ? nextRowGroup() : nextPiece();
            if (piece != null) {
                return piece;
            }
        }
        while (true) {
            final PartitionFrame delegateFrame = delegate.next(skipTarget);
            if (delegateFrame == null) {
                return null;
            }
            if (delegateFrame.getPartitionFormat() != PartitionFormat.NATIVE) {
                // A parquet partition is never composite, but its index rows differ from its partition rows once
                // an in-place O3 update moved row groups to fresh index rows.
                final ParquetPartitionDecoder decoder = delegateFrame.getParquetMetaDecoder();
                if (decoder == null || !decoder.metadata().hasIndexRowMap()) {
                    return delegateFrame;
                }
                pendingPartitionIndex = delegateFrame.getPartitionIndex();
                pendingCumLo = delegateFrame.getRowLo();
                pendingCumHi = delegateFrame.getRowHi();
                pendingParquetDecoder = decoder;
                findRowGroup(decoder.metadata(), descending ? pendingCumHi - 1 : pendingCumLo);
                final PartitionFrame piece = nextRowGroup();
                if (piece != null) {
                    return piece;
                }
                continue;
            }
            final int partitionIndex = delegateFrame.getPartitionIndex();
            if (!getTableReader().getTxFile().isPartitionComposite(partitionIndex)) {
                return delegateFrame;
            }
            pendingPartitionIndex = partitionIndex;
            pendingCumLo = delegateFrame.getRowLo();
            pendingCumHi = delegateFrame.getRowHi();
            final PartitionGeometry geometry = getTableReader().getGeometry();
            pendingPieceOrdinal = geometry.findPieceByRow(partitionIndex, descending ? pendingCumHi - 1 : pendingCumLo);
            final PartitionFrame piece = nextPiece();
            if (piece != null) {
                return piece;
            }
            // An empty delegate range yielded no piece; move on to the next delegate frame.
        }
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return delegate.newSymbolTable(columnIndex);
    }

    /**
     * Binds this cursor to {@code delegate}, whose {@link #next(long)} results this cursor will piece-split as needed.
     */
    public PartitionFrameCursor of(PartitionFrameCursor delegate, boolean descending) {
        this.delegate = delegate;
        this.descending = descending;
        clearPending();
        return this;
    }

    @TestOnly
    @Override
    public boolean reload() {
        clearPending();
        return delegate.reload();
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @Override
    public boolean supportsSizeCalculation() {
        return delegate.supportsSizeCalculation();
    }

    @Override
    public void toPartition(int partitionIndex) {
        clearPending();
        delegate.toPartition(partitionIndex);
    }

    @Override
    public void toTop() {
        clearPending();
        delegate.toTop();
    }

    private void clearPending() {
        pendingPartitionIndex = -1;
        pendingParquetDecoder = null;
    }

    /**
     * Positions {@link #pendingPieceOrdinal} and {@link #pendingRowGroupStartRow} at the row group holding partition
     * row {@code row}.
     */
    private void findRowGroup(ParquetMetaFileReader metadata, long row) {
        long rowGroupStartRow = 0;
        int rowGroup = 0;
        for (int n = metadata.getRowGroupCount() - 1; rowGroup < n; rowGroup++) {
            final long rowGroupEndRow = rowGroupStartRow + metadata.getRowGroupSize(rowGroup);
            if (row < rowGroupEndRow) {
                break;
            }
            rowGroupStartRow = rowGroupEndRow;
        }
        pendingPieceOrdinal = rowGroup;
        pendingRowGroupStartRow = rowGroupStartRow;
    }

    /**
     * Emits the next physical sub-frame from the pending composite range, advancing {@link #pendingCumLo}/{@link
     * #pendingCumHi} and {@link #pendingPieceOrdinal} past it.
     */
    private @Nullable PartitionFrame nextPiece() {
        final TableReader reader = getTableReader();
        final PartitionGeometry geometry = reader.getGeometry();
        while (pendingCumLo < pendingCumHi) {
            final long pieceCumLo = geometry.getPieceCumulativeLo(pendingPartitionIndex, pendingPieceOrdinal);
            final long pieceCumHi = pieceCumLo + geometry.getPieceRowCount(pendingPartitionIndex, pendingPieceOrdinal);
            final long shift = geometry.getPieceShift(pendingPartitionIndex, pendingPieceOrdinal);
            final long subCumLo = Math.max(pendingCumLo, pieceCumLo);
            final long subCumHi = Math.min(pendingCumHi, pieceCumHi);
            if (subCumLo >= subCumHi) {
                break;
            }
            frame.format = PartitionFormat.NATIVE;
            frame.parquetDecoder = null;
            frame.partitionIndex = pendingPartitionIndex;
            frame.rowLo = subCumLo + shift;
            frame.rowHi = subCumHi + shift;
            if (descending) {
                pendingCumHi = subCumLo;
                pendingPieceOrdinal--;
            } else {
                pendingCumLo = subCumHi;
                pendingPieceOrdinal++;
            }
            if (pendingCumLo >= pendingCumHi) {
                clearPending();
            }
            return frame;
        }
        clearPending();
        return null;
    }

    /**
     * Emits the next parquet sub-frame from the pending range: the part of one row group the range covers, in index
     * rows, advancing {@link #pendingCumLo}/{@link #pendingCumHi} and the pending row group past it.
     */
    private @Nullable PartitionFrame nextRowGroup() {
        final ParquetMetaFileReader metadata = pendingParquetDecoder.metadata();
        final int rowGroupCount = metadata.getRowGroupCount();
        while (pendingCumLo < pendingCumHi && pendingPieceOrdinal >= 0 && pendingPieceOrdinal < rowGroupCount) {
            final long rowGroupStartRow = pendingRowGroupStartRow;
            final long rowGroupSize = metadata.getRowGroupSize(pendingPieceOrdinal);
            final long shift = metadata.getRowGroupIndexRowLo(pendingPieceOrdinal, rowGroupStartRow) - rowGroupStartRow;
            final long subCumLo = Math.max(pendingCumLo, rowGroupStartRow);
            final long subCumHi = Math.min(pendingCumHi, rowGroupStartRow + rowGroupSize);
            if (subCumLo >= subCumHi) {
                break;
            }
            frame.format = PartitionFormat.PARQUET;
            frame.parquetDecoder = pendingParquetDecoder;
            frame.partitionIndex = pendingPartitionIndex;
            frame.rowLo = subCumLo + shift;
            frame.rowHi = subCumHi + shift;
            if (descending) {
                pendingCumHi = subCumLo;
                pendingPieceOrdinal--;
                if (pendingPieceOrdinal >= 0) {
                    pendingRowGroupStartRow -= metadata.getRowGroupSize(pendingPieceOrdinal);
                }
            } else {
                pendingCumLo = subCumHi;
                pendingPieceOrdinal++;
                pendingRowGroupStartRow += rowGroupSize;
            }
            if (pendingCumLo >= pendingCumHi) {
                clearPending();
            }
            return frame;
        }
        clearPending();
        return null;
    }

    private static class CompositePartitionFrame implements PartitionFrame {
        private byte format;
        private ParquetPartitionDecoder parquetDecoder;
        private int partitionIndex;
        private long rowHi;
        private long rowLo;

        @Override
        public ParquetPartitionDecoder getParquetMetaDecoder() {
            return parquetDecoder;
        }

        @Override
        public byte getPartitionFormat() {
            return format;
        }

        @Override
        public int getPartitionIndex() {
            return partitionIndex;
        }

        @Override
        public long getRowHi() {
            return rowHi;
        }

        @Override
        public long getRowLo() {
            return rowLo;
        }
    }
}
