/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.BinaryTypeDriver;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.StringTypeDriver;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.wal.WalRowFirstWriter;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.LongList;
import io.questdb.std.Rows;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class WalRowFirstRecord implements Record, Sinkable {
    // Holds [column index, offset] pairs
    private final LongList columnOffsets = new LongList();
    private WalRowFirstReader reader;
    private long rowIndex = 0;
    private long rowOffset = 0;
    private long rowSize = 0;

    @Override
    public BinarySequence getBin(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getBin(offset);
    }

    @Override
    public long getBinLen(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getBinLen(offset);
    }

    @Override
    public boolean getBool(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getBool(offset);
    }

    @Override
    public byte getByte(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getByte(offset);
    }

    @Override
    public char getChar(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getChar(offset);
    }

    public long getDesignatedTimestampRowId(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getLong(offset + Long.BYTES);
    }

    @Override
    public double getDouble(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getDouble(offset);
    }

    @Override
    public float getFloat(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getFloat(offset);
    }

    @Override
    public byte getGeoByte(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getByte(offset);
    }

    @Override
    public int getGeoInt(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getInt(offset);
    }

    @Override
    public long getGeoLong(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getLong(offset);
    }

    @Override
    public short getGeoShort(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getShort(offset);
    }

    @Override
    public int getIPv4(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getIPv4(offset);
    }

    @Override
    public int getInt(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getInt(offset);
    }

    @Override
    public long getLong(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getLong(offset);
    }

    @Override
    public long getLong128Hi(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getLong(offset + Long.BYTES);
    }

    @Override
    public long getLong128Lo(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getLong(offset);
    }

    @Override
    public void getLong256(int col, CharSink<?> sink) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        reader.getRowMem().getLong256(offset, sink);
    }

    @Override
    public Long256 getLong256A(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getLong256A(offset);
    }

    @Override
    public Long256 getLong256B(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getLong256B(offset);
    }

    @Override
    public long getRowId() {
        return Rows.toRowID(0, rowIndex);
    }

    public long getRowIndex() {
        return rowIndex;
    }

    @Override
    public short getShort(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getShort(offset);
    }

    @Override
    public CharSequence getStrA(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getStrA(offset);
    }

    @Override
    public CharSequence getStrB(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getStrB(offset);
    }

    @Override
    public int getStrLen(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getStrLen(offset);
    }

    @Override
    public CharSequence getSymA(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getSymbolValue(col, reader.getRowMem().getInt(offset));
    }

    @Override
    public CharSequence getSymB(int col) {
        return getSymA(col);
    }

    @Override
    public long getTimestamp(int col) {
        return col == reader.getTimestampIndex() ? getDesignatedTimestamp(col) : getLong(col);
    }

    @Override
    public long getUpdateRowId() {
        throw new UnsupportedOperationException("UPDATE is not supported in WAL");
    }

    @Override
    public @Nullable Utf8Sequence getVarcharA(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return VarcharTypeDriver.getPlainValue(
                reader.getRowMem(),
                offset,
                1
        );
    }

    @Override
    public @Nullable Utf8Sequence getVarcharB(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return VarcharTypeDriver.getPlainValue(
                reader.getRowMem(),
                offset,
                2
        );
    }

    @Override
    public int getVarcharSize(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return VarcharTypeDriver.getPlainValueSize(reader.getRowMem(), offset);
    }

    public boolean hasColumnValue(int col) {
        return getColumnOffset(col) != -1;
    }

    public void nextRow() {
        rowIndex++;
        if (rowOffset == -1) {
            rowOffset = 0;
        } else {
            rowOffset += rowSize;
        }
        scanRow();
    }

    public void of(WalRowFirstReader reader) {
        this.reader = reader;
        reset();
    }

    public void reset() {
        this.rowIndex = -1;
        this.rowOffset = -1;
        this.rowSize = -1;
        this.columnOffsets.erase();
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("WalRowFirstRecord [rowIndex=").put(rowIndex).putAscii(']');
    }

    private long getColumnOffset(int col) {
        return columnOffsets.getQuick(col);
    }

    private long getDesignatedTimestamp(int col) {
        final long offset = getColumnOffset(col);
        assert offset != -1;
        return reader.getRowMem().getLong(offset);
    }

    private long readSize(long offset, int columnIndex) {
        int columnType = reader.getColumnType(columnIndex);
        if (columnIndex == reader.getTimestampIndex()) {
            // designated timestamp is stored as long128
            return ColumnType.sizeOf(ColumnType.LONG128);
        }
        long size = ColumnType.sizeOf(columnType);
        if (size > 0) {
            // fixed-size
            return size;
        }
        // var-size
        switch (columnType) {
            case ColumnType.VARCHAR:
                final Utf8Sequence us = VarcharTypeDriver.getPlainValue(reader.getRowMem(), offset, 1);
                return VarcharTypeDriver.getPlainValueByteCount(us);
            case ColumnType.STRING:
                final CharSequence cs = reader.getRowMem().getStrA(offset);
                return StringTypeDriver.getPlainValueByteCount(cs);
            case ColumnType.BINARY:
                final BinarySequence bs = reader.getRowMem().getBin(offset);
                return BinaryTypeDriver.getPlainValueByteCount(bs);
            default:
                throw new UnsupportedOperationException();
        }
    }

    private void scanRow() {
        columnOffsets.erase();
        long offset = rowOffset;
        for (; ; ) {
            int columnIndex = reader.getRowMem().getInt(offset);
            offset += Integer.BYTES;
            if (columnIndex == WalRowFirstWriter.NEW_ROW_SEPARATOR) {
                rowSize = offset - rowOffset;
                return;
            }
            columnOffsets.extendAndSet(columnIndex, offset);
            offset += readSize(offset, columnIndex);
        }
    }
}
