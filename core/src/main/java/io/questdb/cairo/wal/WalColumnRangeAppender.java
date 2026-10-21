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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.IntList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

/**
 * Appends column ranges that are already in WAL layout, such as the rows of another table's WAL segment, as
 * blocks of a {@link WalColumnSink}. A client brackets each block with {@link #begin(int)} and
 * {@link #finish(long, long, boolean)} or {@link #cancel()}, and between the two hands over one range per column it
 * writes; the columns it does not write are NULL-filled by the sink when the block finishes. Every range covers
 * the whole block. This class keeps the sink's rules for its client: it advances the not-null watermark of every
 * written column, writes the designated timestamp as (timestamp, row) pairs whose row is the segment's, continues
 * a STRING or BINARY aux vector from the entry already present, resolves symbols through the sink's own reader in
 * first-row order, checks the timestamp bounds before the block finishes, and leaves a refused call to be
 * cancelled by the client. The ranges are not validated beyond their sizes: the client owns their layout.
 * <p>
 * Not thread-safe; one instance per sink. {@link #close()} frees the buffer the var-size aux shift uses.
 */
public final class WalColumnRangeAppender implements QuietCloseable {
    private final WalColumnSink sink;
    private final IntList writtenColumns = new IntList();
    private long firstRow;
    private boolean isInBlock;
    private int rowCount;

    public WalColumnRangeAppender(WalColumnSink sink) {
        this.sink = sink;
    }

    /**
     * Opens a block of {@code rowCount} rows. The block's first row is the segment's row count at this call.
     */
    public void begin(int rowCount) {
        sink.checkDistressed();
        if (isInBlock) {
            throw CairoException.nonCritical().put("block already open");
        }
        if (rowCount < 1) {
            throw CairoException.nonCritical().put("block needs at least one row [rowCount=").put(rowCount).put(']');
        }
        sink.beginColumnarWrite();
        this.firstRow = sink.getSegmentRowCount();
        this.rowCount = rowCount;
        writtenColumns.clear();
        isInBlock = true;
    }

    /**
     * Discards the open block. Does nothing without one.
     */
    public void cancel() {
        if (!isInBlock) {
            return;
        }
        sink.cancelColumnarWrite();
        isInBlock = false;
    }

    @Override
    public void close() {
    }

    /**
     * Checks the timestamp bounds and closes the block; the sink NULL-fills the columns that received no range.
     */
    public void finish(long minTimestamp, long maxTimestamp, boolean outOfOrder) {
        checkInBlock();
        sink.validateDesignatedTimestampBounds(minTimestamp);
        sink.validateDesignatedTimestampBounds(maxTimestamp);
        sink.finishColumnarWrite(rowCount, minTimestamp, maxTimestamp, outOfOrder);
        isInBlock = false;
    }

    /**
     * Appends the block's values of a fixed-size column (not SYMBOL, not the designated timestamp): {@code size}
     * bytes at {@code address}, one value per row in the column's WAL width.
     */
    public void putFixedRange(int columnIndex, long address, long size) {
        checkInBlock();
        final TableRecordMetadata metadata = sink.getMetadata();
        final int type = metadata.getColumnType(columnIndex);
        if (ColumnType.isVarSize(type) || ColumnType.isSymbol(type) || columnIndex == metadata.getTimestampIndex()) {
            throw CairoException.nonCritical().put("not a fixed-size column [column=").put(columnIndex).put(']');
        }
        final long expected = (long) rowCount << ColumnType.getWalDataColumnShl(type, false);
        if (size != expected) {
            throw CairoException.nonCritical().put("fixed-size range does not match the block [column=").put(columnIndex)
                    .put(", size=").put(size).put(", expected=").put(expected).put(']');
        }
        sink.getDataColumn(columnIndex).putBlockOfBytes(address, size);
        written(columnIndex);
    }

    /**
     * Appends the block's designated timestamps from 16-byte (timestamp, row) pairs at {@code address}, whose rows
     * count from {@code sourceFirstRow}. The pairs are copied when {@code sourceFirstRow} is the block's first
     * row, and rewritten with the block's rows otherwise.
     */
    public void putTimestampRange(int columnIndex, long address, long sourceFirstRow) {
        checkInBlock();
        if (columnIndex != sink.getMetadata().getTimestampIndex()) {
            throw CairoException.nonCritical().put("not the designated timestamp [column=").put(columnIndex).put(']');
        }
        final MemoryMA data = sink.getDataColumn(columnIndex);
        if (sourceFirstRow == firstRow) {
            data.putBlockOfBytes(address, (long) rowCount << 4);
        } else {
            for (int i = 0; i < rowCount; i++) {
                data.putLong128(Unsafe.getLong(address + ((long) i << 4)), firstRow + i);
            }
        }
        written(columnIndex);
    }

    private void checkInBlock() {
        if (!isInBlock) {
            throw CairoException.nonCritical().put("no open block");
        }
    }

    private void written(int columnIndex) {
        sink.setRowValueNotNullColumnar(columnIndex, firstRow + rowCount - 1);
        writtenColumns.add(columnIndex);
    }
}
