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

package io.questdb.test.cairo;

import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Long128;
import io.questdb.std.Long256;
import io.questdb.std.Rows;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Warning: this cursor only supports native partitions. It's only good to be used in tests.
 */
public class TestTableReaderRecord implements Record, Sinkable {
    private int columnBase;
    private TableReader reader;
    private long recordIndex = 0;

    @Override
    public BinarySequence getBin(int col) {
        final long recordIndex = getAdjustedRecordIndex(col) * Long.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                recordIndex,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getBin(
                reader.getColumn(absoluteColumnIndex + 1).getLong(recordIndex)
        );
    }

    @Override
    public long getBinLen(int col) {
        final long recordIndex = getAdjustedRecordIndex(col) * Long.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                recordIndex,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getBinLen(
                reader.getColumn(absoluteColumnIndex + 1).getLong(recordIndex)
        );
    }

    @Override
    public boolean getBool(int col) {
        final long offset = getAdjustedRecordIndex(col);
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getBool(offset);
    }

    @Override
    public byte getByte(int col) {
        final long offset = getAdjustedRecordIndex(col);
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getByte(offset);
    }

    @Override
    public char getChar(int col) {
        final long offset = getAdjustedRecordIndex(col) * Character.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getChar(offset);
    }


    @Override
    public void getDecimal128(int col, Decimal128 sink) {
        final long offset = getAdjustedRecordIndex(col) * (Long.BYTES << 1);
        final int absoluteColumnIndex = TableReader.getPrimaryColumnIndex(columnBase, col);
        if (offset < 0) {
            sink.ofRawNull();
        } else {
            reader.getColumn(absoluteColumnIndex).getDecimal128(offset, sink);
        }
    }

    @Override
    public short getDecimal16(int col) {
        final long offset = getAdjustedRecordIndex(col) * Short.BYTES;
        final int absoluteColumnIndex = TableReader.getPrimaryColumnIndex(columnBase, col);
        return offset < 0 ? Decimals.DECIMAL16_NULL : reader.getColumn(absoluteColumnIndex).getDecimal16(offset);
    }

    @Override
    public void getDecimal256(int col, Decimal256 sink) {
        final long offset = getAdjustedRecordIndex(col) * (Long.BYTES << 2);
        final int absoluteColumnIndex = TableReader.getPrimaryColumnIndex(columnBase, col);
        if (offset < 0) {
            sink.ofRawNull();
        } else {
            reader.getColumn(absoluteColumnIndex).getDecimal256(offset, sink);
        }
    }

    @Override
    public int getDecimal32(int col) {
        final long offset = getAdjustedRecordIndex(col) * Integer.BYTES;
        final int absoluteColumnIndex = TableReader.getPrimaryColumnIndex(columnBase, col);
        return offset < 0 ? Decimals.DECIMAL32_NULL : reader.getColumn(absoluteColumnIndex).getDecimal32(offset);
    }

    @Override
    public long getDecimal64(int col) {
        final long offset = getAdjustedRecordIndex(col) * Long.BYTES;
        final int absoluteColumnIndex = TableReader.getPrimaryColumnIndex(columnBase, col);
        return offset < 0 ? Decimals.DECIMAL64_NULL : reader.getColumn(absoluteColumnIndex).getDecimal64(offset);
    }

    @Override
    public byte getDecimal8(int col) {
        final long offset = getAdjustedRecordIndex(col) * Byte.BYTES;
        final int absoluteColumnIndex = TableReader.getPrimaryColumnIndex(columnBase, col);
        return offset < 0 ? Decimals.DECIMAL8_NULL : reader.getColumn(absoluteColumnIndex).getDecimal8(offset);
    }

    @Override
    public double getDouble(int col) {
        final long offset = getAdjustedRecordIndex(col) * Double.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getDouble(offset);
    }

    @Override
    public float getFloat(int col) {
        final long offset = getAdjustedRecordIndex(col) * Float.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getFloat(offset);
    }

    @Override
    public byte getGeoByte(int col) {
        final long offset = getAdjustedRecordIndex(col) * Byte.BYTES;
        final int absoluteColumnIndex = TableReader.getPrimaryColumnIndex(columnBase, col);
        return offset < 0 ? GeoHashes.BYTE_NULL : reader.getColumn(absoluteColumnIndex).getByte(offset);
    }

    @Override
    public int getGeoInt(int col) {
        final long offset = getAdjustedRecordIndex(col) * Integer.BYTES;
        final int absoluteColumnIndex = TableReader.getPrimaryColumnIndex(columnBase, col);
        return offset < 0 ? GeoHashes.INT_NULL : reader.getColumn(absoluteColumnIndex).getInt(offset);
    }

    @Override
    public long getGeoLong(int col) {
        final long offset = getAdjustedRecordIndex(col) * Long.BYTES;
        final int absoluteColumnIndex = TableReader.getPrimaryColumnIndex(columnBase, col);
        return offset < 0 ? GeoHashes.NULL : reader.getColumn(absoluteColumnIndex).getLong(offset);
    }

    @Override
    public short getGeoShort(int col) {
        final long offset = getAdjustedRecordIndex(col) * Short.BYTES;
        final int absoluteColumnIndex = TableReader.getPrimaryColumnIndex(columnBase, col);
        return offset < 0 ? GeoHashes.SHORT_NULL : reader.getColumn(absoluteColumnIndex).getShort(offset);
    }

    @Override
    public int getIPv4(int col) {
        final long offset = getAdjustedRecordIndex(col) * Integer.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getIPv4(offset);
    }

    @Override
    public int getInt(int col) {
        final long offset = getAdjustedRecordIndex(col) * Integer.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getInt(offset);
    }

    @Override
    public long getLong(int col) {
        final long offset = getAdjustedRecordIndex(col) * Long.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getLong(offset);
    }

    @Override
    public long getLong128Hi(int col) {
        final long offset = getAdjustedRecordIndex(col) * Long128.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getLong(offset + Long.BYTES);
    }

    @Override
    public long getLong128Lo(int col) {
        final long offset = getAdjustedRecordIndex(col) * Long128.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getLong(offset);
    }

    @Override
    public void getLong256(int col, CharSink<?> sink) {
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col) * Long256.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(offset, index);
        reader.getColumn(absoluteColumnIndex).getLong256(offset, sink);
    }

    @Override
    public Long256 getLong256A(int col) {
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col) * Long256.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(offset, index);
        return reader.getColumn(absoluteColumnIndex).getLong256A(offset);
    }

    @Override
    public Long256 getLong256B(int col) {
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col) * Long256.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(offset, index);
        return reader.getColumn(absoluteColumnIndex).getLong256B(offset);
    }

    public TableReader getReader() {
        return reader;
    }

    public long getRecordIndex() {
        return recordIndex;
    }

    @Override
    public long getRowId() {
        return Rows.toRowID(reader.getPartitionIndex(columnBase), recordIndex);
    }

    @Override
    public short getShort(int col) {
        final long offset = getAdjustedRecordIndex(col) * Short.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getShort(offset);
    }

    @Override
    public CharSequence getStrA(int col) {
        final long recordIndex = getAdjustedRecordIndex(col) * Long.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                recordIndex,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getStrA(
                reader.getColumn(absoluteColumnIndex + 1).getLong(recordIndex)
        );
    }

    @Override
    public CharSequence getStrB(int col) {
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long recordIndex = getAdjustedRecordIndex(col) * Long.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(recordIndex, index);
        return reader.getColumn(absoluteColumnIndex).getStrB(
                reader.getColumn(absoluteColumnIndex + 1).getLong(recordIndex)
        );
    }

    @Override
    public int getStrLen(int col) {
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long recordIndex = getAdjustedRecordIndex(col) * Long.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(recordIndex, index);
        return reader.getColumn(absoluteColumnIndex).getStrLen(
                reader.getColumn(absoluteColumnIndex + 1).getLong(recordIndex)
        );
    }

    @Override
    public CharSequence getSymA(int col) {
        final long offset = getAdjustedRecordIndex(col) * Integer.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getSymbolMapReader(col).valueOf(reader.getColumn(absoluteColumnIndex).getInt(offset));
    }

    @Override
    public CharSequence getSymB(int col) {
        final long offset = getAdjustedRecordIndex(col) * Integer.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getSymbolMapReader(col).valueBOf(reader.getColumn(absoluteColumnIndex).getInt(offset));
    }

    @Override
    public long getUpdateRowId() {
        return getRowId();
    }

    @Override
    @Nullable
    public Utf8Sequence getVarcharA(int col) {
        return getVarchar(col, 1);
    }

    @Override
    public @Nullable Utf8Sequence getVarcharB(int col) {
        return getVarchar(col, 2);
    }

    @Override
    public int getVarcharSize(int col) {
        final long rowNum = getAdjustedRecordIndex(col);
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                rowNum,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return VarcharTypeDriver.getValueSize(
                reader.getColumn(absoluteColumnIndex + 1),
                rowNum
        );
    }

    public void incrementRecordIndex() {
        recordIndex++;
    }

    public void jumpTo(int partitionIndex, long recordIndex) {
        this.columnBase = reader.getColumnBase(partitionIndex);
        this.recordIndex = recordIndex;
    }

    public void of(TableReader reader) {
        this.reader = reader;
    }

    public void setRecordIndex(long recordIndex) {
        this.recordIndex = recordIndex;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("TableReaderRecord [columnBase=").put(columnBase).putAscii(", recordIndex=").put(recordIndex).putAscii(']');
    }

    private static int ifOffsetNegThen0ElseValue(long offset, int value) {
        return offset < 0 ? 0 : value;
    }

    private long getAdjustedRecordIndex(int col) {
        assert col > -1 && col < reader.getColumnCount() : "Column index out of bounds: " + col + " >= " + reader.getColumnCount();
        return recordIndex - reader.getColumnTop(columnBase, col);
    }

    @Nullable
    private Utf8Sequence getVarchar(int col, int ab) {
        final long rowNum = getAdjustedRecordIndex(col);
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                rowNum,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return VarcharTypeDriver.getSplitValue(
                reader.getColumn(absoluteColumnIndex + 1),
                reader.getColumn(absoluteColumnIndex),
                rowNum,
                ab
        );
    }
}
