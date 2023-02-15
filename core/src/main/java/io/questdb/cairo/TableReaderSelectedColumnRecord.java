/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.sql.Record;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cairo.TableReaderRecord.ifOffsetNegThen0ElseValue;

public class TableReaderSelectedColumnRecord implements Record {

    private final IntList columnIndexes;
    private int columnBase;
    private TableReader reader;
    private long recordIndex = 0;

    public TableReaderSelectedColumnRecord(@NotNull IntList columnIndexes) {
        this.columnIndexes = columnIndexes;
    }

    public long getAdjustedRecordIndex() {
        return recordIndex;
    }

    @Override
    public BinarySequence getBin(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
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
    public long getBinLen(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
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
    public boolean getBool(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col);
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(offset, index);
        return reader.getColumn(absoluteColumnIndex).getBool(offset);
    }

    @Override
    public byte getByte(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col);
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(offset, index);
        return reader.getColumn(absoluteColumnIndex).getByte(offset);
    }

    @Override
    public char getChar(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col) * Character.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(offset, index);
        return reader.getColumn(absoluteColumnIndex).getChar(offset);
    }

    @Override
    public double getDouble(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col) * Double.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(offset, index);
        return reader.getColumn(absoluteColumnIndex).getDouble(offset);
    }

    @Override
    public float getFloat(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col) * Float.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(offset, index);
        return reader.getColumn(absoluteColumnIndex).getFloat(offset);
    }

    @Override
    public byte getGeoByte(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col);
        return offset < 0 ? GeoHashes.BYTE_NULL : reader.getColumn(index).getByte(offset);
    }

    @Override
    public int getGeoInt(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col) * Integer.BYTES;
        return offset < 0 ? GeoHashes.INT_NULL : reader.getColumn(index).getInt(offset);
    }

    @Override
    public long getGeoLong(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col) * Long.BYTES;
        return offset < 0 ? GeoHashes.NULL : reader.getColumn(index).getLong(offset);
    }

    @Override
    public short getGeoShort(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col) * Short.BYTES;
        return offset < 0 ? GeoHashes.SHORT_NULL : reader.getColumn(index).getShort(offset);
    }

    @Override
    public int getInt(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col) * Integer.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(offset, index);
        return reader.getColumn(absoluteColumnIndex).getInt(offset);
    }

    @Override
    public long getLong(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col) * Long.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(offset, index);
        return reader.getColumn(absoluteColumnIndex).getLong(offset);
    }

    @Override
    public long getLong128Hi(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col) * Long128.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(offset, index);
        return reader.getColumn(absoluteColumnIndex).getLong(offset + Long.BYTES);
    }

    @Override
    public long getLong128Lo(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col) * Long128.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(offset, index);
        return reader.getColumn(absoluteColumnIndex).getLong(offset);
    }

    @Override
    public void getLong256(int columnIndex, CharSink sink) {
        final int col = deferenceColumn(columnIndex);
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col) * Long256.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(offset, index);
        reader.getColumn(absoluteColumnIndex).getLong256(offset, sink);
    }

    @Override
    public Long256 getLong256A(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col) * Long256.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(offset, index);
        return reader.getColumn(absoluteColumnIndex).getLong256A(offset);
    }

    @Override
    public Long256 getLong256B(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col) * Long256.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(offset, index);
        return reader.getColumn(absoluteColumnIndex).getLong256B(offset);
    }

    @Override
    public long getRowId() {
        return Rows.toRowID(reader.getPartitionIndex(columnBase), recordIndex);
    }

    @Override
    public short getShort(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col) * Short.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(offset, index);
        return reader.getColumn(absoluteColumnIndex).getShort(offset);
    }

    @Override
    public CharSequence getStr(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final long recordIndex = getAdjustedRecordIndex(col) * Long.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                recordIndex,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        long offset = reader.getColumn(absoluteColumnIndex + 1).getLong(recordIndex);
        assert recordIndex != 0 || (offset == 0 || offset == Numbers.LONG_NaN);
        return reader.getColumn(absoluteColumnIndex).getStr(offset);
    }

    @Override
    public CharSequence getStrB(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final long recordIndex = getAdjustedRecordIndex(col) * Long.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                recordIndex,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getStr2(
                reader.getColumn(absoluteColumnIndex + 1).getLong(recordIndex)
        );
    }

    @Override
    public int getStrLen(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final long recordIndex = getAdjustedRecordIndex(col) * Long.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                recordIndex,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getStrLen(
                reader.getColumn(absoluteColumnIndex + 1).getLong(recordIndex)
        );
    }

    @Override
    public CharSequence getSym(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final long offset = getAdjustedRecordIndex(col) * Integer.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getSymbolMapReader(col).valueOf(reader.getColumn(absoluteColumnIndex).getInt(offset));
    }

    @Override
    public CharSequence getSymB(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
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

    private int deferenceColumn(int columnIndex) {
        return columnIndexes.getQuick(columnIndex);
    }

    private long getAdjustedRecordIndex(int col) {
        assert col > -1 && col < reader.getColumnCount() : "Column index out of bounds: " + col + " >= " + reader.getColumnCount();
        return recordIndex - reader.getColumnTop(columnBase, col);
    }
}
