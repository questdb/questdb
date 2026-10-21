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
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

/**
 * Appends column ranges that are already in WAL layout, such as the rows of another table's WAL segment, as
 * blocks of a {@link WalColumnSink}. A client brackets each block with {@link #begin(int)} and
 * {@link #finish(long, long, boolean)} or {@link #cancel()}, and between the two hands over one range per column it
 * writes; the columns it does not write are NULL-filled by the sink when the block finishes. Every range covers
 * the whole block. A column takes one range per block; a second range for it is refused. This class keeps the
 * sink's rules for its client: it advances the not-null watermark of every written column, writes the designated
 * timestamp as (timestamp, row) pairs whose row is the segment's, continues a STRING or BINARY aux vector from
 * the entry already present, resolves symbols through the sink's own reader in first-row order, checks the
 * timestamp bounds before the block finishes, and leaves a refused call to be cancelled by the client. The
 * ranges are not validated beyond their sizes: the client owns their layout.
 * <p>
 * Not thread-safe; one instance per sink. {@link #close()} frees the buffer the var-size aux shift uses.
 */
public final class WalColumnRangeAppender implements QuietCloseable {
    private final WalColumnSink sink;
    private final IntIntHashMap symbolKeys = new IntIntHashMap();
    private final IntList writtenColumns = new IntList();
    private long firstRow;
    private boolean isInBlock;
    private int rowCount;
    private long scratch;
    private long scratchSize;

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
        scratch = Unsafe.free(scratch, scratchSize, MemoryTag.NATIVE_DEFAULT);
        scratchSize = 0;
    }

    /**
     * Writes one symbol key, as {@link #resolveSymbol(int, CharSequence)} returned it, for every row of the block.
     */
    public void fillSymbolKey(int columnIndex, int key) {
        checkInBlock();
        checkNotWritten(columnIndex);
        checkSymbol(columnIndex);
        final MemoryMA data = sink.getDataColumn(columnIndex);
        for (int i = 0; i < rowCount; i++) {
            data.putInt(key);
        }
        written(columnIndex);
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
        checkNotWritten(columnIndex);
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
     * Appends the block's values of a SYMBOL column from 4-byte keys of another dictionary at {@code keysAddress}:
     * each distinct key is looked up once, in the order of its first row, and resolved through the sink; a
     * negative key, or a key {@code values} does not know, is NULL.
     */
    public void putSymbolRange(int columnIndex, long keysAddress, SymbolValues values) {
        checkInBlock();
        checkNotWritten(columnIndex);
        checkSymbol(columnIndex);
        final SymbolMapReader reader = sink.getSymbolMapReader(columnIndex);
        final MemoryMA data = sink.getDataColumn(columnIndex);
        symbolKeys.clear();
        for (int i = 0; i < rowCount; i++) {
            final int key = Unsafe.getInt(keysAddress + ((long) i << 2));
            final int index = symbolKeys.keyIndex(key);
            final int targetKey;
            if (index > -1) {
                // first row of this key in the block: resolve once, in first-row order as the row API does
                targetKey = sink.resolveSymbol(columnIndex, key < 0 ? null : values.valueOf(key), reader);
                symbolKeys.putAt(index, key, targetKey);
            } else {
                targetKey = symbolKeys.valueAt(index);
            }
            data.putInt(targetKey);
        }
        written(columnIndex);
    }

    /**
     * Appends the block's designated timestamps from 16-byte (timestamp, row) pairs at {@code address}, whose rows
     * count from {@code sourceFirstRow}. The pairs are copied when {@code sourceFirstRow} is the block's first
     * row, and rewritten with the block's rows otherwise.
     */
    public void putTimestampRange(int columnIndex, long address, long sourceFirstRow) {
        checkInBlock();
        checkNotWritten(columnIndex);
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

    /**
     * Appends the block's values of a var-size column (STRING, BINARY, VARCHAR, ARRAY) from WAL layout:
     * {@code auxAddress} is the aux entry of the range's first row (STRING and BINARY: the entry holding its data
     * offset, of which the block's rows plus one are read; VARCHAR and ARRAY: its 16-byte entry),
     * {@code dataAddress} and {@code dataSize} the range's data bytes, and {@code dataOffset} the offset of
     * {@code dataAddress} in the source data vector, which the aux entries are relative to. The entries are
     * shifted to the sink's data append offset; a STRING or BINARY range continues from the entry the sink's aux
     * column already holds.
     */
    public void putVarSizeRange(int columnIndex, long auxAddress, long dataAddress, long dataOffset, long dataSize) {
        checkInBlock();
        checkNotWritten(columnIndex);
        final int type = sink.getMetadata().getColumnType(columnIndex);
        if (!ColumnType.isVarSize(type)) {
            throw CairoException.nonCritical().put("not a var-size column [column=").put(columnIndex).put(']');
        }
        final ColumnTypeDriver driver = ColumnType.getDriver(type);
        final MemoryMA aux = sink.getAuxColumn(columnIndex);
        final MemoryMA data = sink.getDataColumn(columnIndex);
        final long shift = dataOffset - data.getAppendOffset();
        final long auxSize = driver.getAuxVectorSize(rowCount);
        final long appendBytes = driver.auxRowsToBytes(rowCount);
        if (auxSize > scratchSize) {
            scratch = Unsafe.realloc(scratch, scratchSize, auxSize, MemoryTag.NATIVE_DEFAULT);
            scratchSize = auxSize;
        }
        driver.shiftCopyAuxVector(shift, auxAddress, 0, rowCount - 1, scratch, auxSize);
        // the leading entry of a STRING or BINARY vector is already present in the sink's aux column
        aux.putBlockOfBytes(scratch + auxSize - appendBytes, appendBytes);
        if (dataSize > 0) {
            data.putBlockOfBytes(dataAddress, dataSize);
        }
        written(columnIndex);
    }

    /**
     * Returns the key of a symbol value in the sink's dictionary, for {@link #fillSymbolKey(int, int)}.
     */
    public int resolveSymbol(int columnIndex, @Nullable CharSequence value) {
        checkInBlock();
        checkSymbol(columnIndex);
        return sink.resolveSymbol(columnIndex, value, sink.getSymbolMapReader(columnIndex));
    }

    /**
     * Syncs the memories of the columns written in the open block, data and aux; what the block finish and the
     * commit sync afterwards is unchanged.
     */
    public void syncWrittenColumns() {
        checkInBlock();
        final TableRecordMetadata metadata = sink.getMetadata();
        for (int i = 0, n = writtenColumns.size(); i < n; i++) {
            final int column = writtenColumns.getQuick(i);
            sink.getDataColumn(column).sync(false);
            if (ColumnType.isVarSize(metadata.getColumnType(column))) {
                sink.getAuxColumn(column).sync(false);
            }
        }
    }

    private void checkInBlock() {
        if (!isInBlock) {
            throw CairoException.nonCritical().put("no open block");
        }
    }

    private void checkNotWritten(int columnIndex) {
        if (writtenColumns.contains(columnIndex)) {
            throw CairoException.nonCritical().put("column already written in this block [column=").put(columnIndex).put(']');
        }
    }

    private void checkSymbol(int columnIndex) {
        if (!ColumnType.isSymbol(sink.getMetadata().getColumnType(columnIndex))) {
            throw CairoException.nonCritical().put("not a SYMBOL column [column=").put(columnIndex).put(']');
        }
    }

    private void written(int columnIndex) {
        sink.setRowValueNotNullColumnar(columnIndex, firstRow + rowCount - 1);
        writtenColumns.add(columnIndex);
    }

    /**
     * The values of the dictionary a symbol range's keys belong to.
     */
    public interface SymbolValues {
        /**
         * Returns the value of a non-negative key, or null for a key the dictionary does not hold.
         */
        @Nullable
        CharSequence valueOf(int key);
    }
}
