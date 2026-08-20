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
import io.questdb.std.Misc;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Wraps a {@link PartitionFrameCursor}, translating each delegate frame's directory-cumulative
 * {@code [rowLo, rowHi)} into one or more physical, file-row-space sub-frames.
 * <p>
 * A consumer that reads an index reader directly (a posting/BITMAP index scan bypassing
 * {@link io.questdb.cairo.sql.PageFrame}) needs {@link PartitionFrame#getRowLo()}/{@link PartitionFrame#getRowHi()}
 * to already be physical, because the index stores and compares physical file row ids -- see
 * {@code FwdTableReaderPageFrameCursor.computeNativeFrame}, which applies the identical translation
 * for the {@code PageFrame} path via {@link PartitionGeometry#findPieceByRow}/{@link PartitionGeometry#getPieceShift}.
 * For an ordinary (non-composite) partition the two spaces coincide, so the delegate's frame is
 * returned untouched -- zero extra cost, the common case.
 * <p>
 * For a composite partition -- one whose column files hold several physical pieces, see
 * {@link PartitionGeometry} -- the delegate's logical range is split into one sub-frame per piece
 * it overlaps, each carrying that piece's own physical row range. {@code descending} controls the
 * order pieces are emitted within a partition: forward cursors (ascending row id) walk pieces low to
 * high, {@code LATEST BY} and negative-limit cursors need the highest (most recent) piece first, to
 * match the frame order their delegate already advertises.
 */
public class CompositeAwarePartitionFrameCursor implements PartitionFrameCursor {
    private final CompositePartitionFrame frame = new CompositePartitionFrame();
    private PartitionFrameCursor delegate;
    private boolean descending;
    private long pendingCumHi;
    private long pendingCumLo;
    private int pendingPartitionIndex = -1;
    private int pendingPieceOrdinal;

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
    public boolean hasIntervalFilter() {
        return delegate.hasIntervalFilter();
    }

    @Override
    public @Nullable PartitionFrame next(long skipTarget) {
        if (pendingPartitionIndex >= 0) {
            final PartitionFrame piece = nextPiece();
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
                // A parquet partition is materialized whole and is never composite.
                return delegateFrame;
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
     * Binds this cursor to {@code delegate}, whose {@link #next(long)} results this cursor will
     * piece-split as needed. {@code descending} must match the piece order the delegate's own frames
     * are consumed in -- {@code true} for a backward ({@code LATEST BY}, negative-limit) scan, {@code false}
     * for an ordinary forward scan.
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
    }

    /**
     * Emits the next physical sub-frame from the pending composite range, advancing
     * {@link #pendingCumLo}/{@link #pendingCumHi} and {@link #pendingPieceOrdinal} past it. Returns
     * {@code null}, and clears the pending state, once the range is fully covered.
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

    private static class CompositePartitionFrame implements PartitionFrame {
        private byte format;
        private int partitionIndex;
        private long rowHi;
        private long rowLo;

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
