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

import io.questdb.cairo.AbstractColumnarRowAppender;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.Numbers;

/** WAL storage implementation of the shared columnar appender. */
public class WalColumnarRowAppender extends AbstractColumnarRowAppender {
    private final WalWriter walWriter;

    public WalColumnarRowAppender(WalWriter walWriter) {
        this.walWriter = walWriter;
    }

    @Override
    public void putNativeColumn(int columnIndex, long dataAddress, long dataSize, long auxAddress, long auxSize, int rowCount) {
        checkInColumnarWrite();
        if (rowCount == 0) {
            return;
        }
        final int type = getMetadata().getColumnType(columnIndex);
        if (ColumnType.isSymbol(type)) {
            throw CairoException.nonCritical().put("native symbol keys require a source dictionary [column=").put(columnIndex).put(']');
        }
        if (ColumnType.isVarSize(type)) {
            if (auxAddress == 0 || auxSize < ColumnType.getDriver(type).getAuxVectorSize(rowCount)) {
                throw CairoException.nonCritical().put("incomplete native auxiliary bounds [column=").put(columnIndex).put(']');
            }
        } else if (dataAddress == 0 || dataSize < (long) rowCount * ColumnType.sizeOf(type)) {
            throw CairoException.nonCritical().put("incomplete native data bounds [column=").put(columnIndex).put(']');
        }
        if (columnIndex == getMetadata().getTimestampIndex()) {
            putTimestampColumn(columnIndex, dataAddress, rowCount, 0, rowCount, Numbers.LONG_NULL);
        } else if (ColumnType.isVarSize(type)) {
            final ColumnTypeDriver driver = ColumnType.getDriver(type);
            final long sourceLo = driver.getDataVectorOffset(auxAddress, 0);
            final long sourceHi = driver.getDataVectorSizeAt(auxAddress, rowCount - 1);
            if (sourceLo < 0 || sourceHi < sourceLo || sourceHi > dataSize) {
                throw CairoException.nonCritical().put("invalid native column data bounds [column=").put(columnIndex).put(']');
            }
            final MemoryMA data = walWriter.getDataColumn(columnIndex);
            final MemoryMA aux = walWriter.getAuxColumn(columnIndex);
            final long destinationLo = data.getAppendOffset();
            if (sourceHi > sourceLo) {
                data.putBlockOfBytes(dataAddress + sourceLo, sourceHi - sourceLo);
            }
            // STRING/BINARY carry an N+1 terminal offset, replaced by the next slice.
            final long auxOffset = Math.max(0, aux.getAppendOffset() - driver.getAuxVectorSize(0));
            final long bytes = driver.getAuxVectorSize(rowCount);
            aux.jumpTo(auxOffset + bytes);
            driver.shiftCopyAuxVector(sourceLo - destinationLo, auxAddress, 0, rowCount - 1, aux.addressOf(auxOffset), bytes);
            walWriter.setRowValueNotNullColumnar(columnIndex, walWriter.getSegmentRowCount() + rowCount - 1);
        } else {
            putFixedColumn(columnIndex, dataAddress, rowCount, ColumnType.sizeOf(type), 0, rowCount);
        }
    }

    @Override
    protected long beginWrite(int rowCount) {
        walWriter.checkDistressed();
        if (walWriter.getTableToken().isDelta()) {
            throw CairoException.schemaMismatch().put("table switched to Delta; reacquire the columnar appender [table=")
                    .put(walWriter.getTableToken()).put(']');
        }
        // Column files for a deferred schema change open when the segment rolls.
        // The roll opens an empty segment without copying rows, so it must never
        // find uncommitted rows. QWP commit batching relies on this invariant.
        assert !walWriter.rollSegmentOnNextRow || walWriter.getUncommittedRowCount() == 0;
        try {
            if (walWriter.rollSegmentOnNextRow) {
                walWriter.rollSegment();
                walWriter.rollSegmentOnNextRow = false;
            }
        } catch (Throwable e) {
            walWriter.distressed = true;
            throw e;
        }
        return walWriter.getSegmentRowCount();
    }

    @Override
    protected void cancelWrite(long startRowId) {
        walWriter.cancelColumnarWrite(startRowId);
    }

    @Override
    protected void finishWrite(int rowCount, long minTimestamp, long maxTimestamp, boolean outOfOrder) {
        walWriter.finishColumnarWrite(rowCount, minTimestamp, maxTimestamp, outOfOrder);
    }

    @Override
    protected MemoryA getAuxColumn(int columnIndex) {
        return walWriter.getAuxColumn(columnIndex);
    }

    @Override
    protected MemoryA getDataColumn(int columnIndex) {
        return walWriter.getDataColumn(columnIndex);
    }

    @Override
    protected TableRecordMetadata getMetadata() {
        return walWriter.getMetadata();
    }

    @Override
    protected SymbolMapReader getSymbolMapReader(int columnIndex) {
        final SymbolMapReader reader = walWriter.getSymbolMapReader(columnIndex);
        if (reader == null) {
            throw CairoException.nonCritical().put("symbol map reader is not available [column=").put(columnIndex).put(']');
        }
        return reader;
    }

    @Override
    protected void markSymbolMapNull(int columnIndex) {
        walWriter.markSymbolMapNull(columnIndex);
    }

    @Override
    protected void putDesignatedTimestamp(MemoryA dataMem, long timestamp, long rowId) {
        dataMem.putLong128(timestamp, rowId);
    }

    @Override
    protected int resolveSymbol(int columnIndex, CharSequence value, SymbolMapReader reader) {
        return walWriter.resolveSymbol(columnIndex, value, reader);
    }

    @Override
    protected void setRowValueNotNullColumnar(int columnIndex, long rowId) {
        walWriter.setRowValueNotNullColumnar(columnIndex, rowId);
    }

    @Override
    protected void validateDesignatedTimestampBounds(long timestamp) {
        walWriter.validateDesignatedTimestampBounds(timestamp);
    }
}
