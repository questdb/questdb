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

package io.questdb.cairo.wal;

import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.api.MemoryCR;
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

import static io.questdb.cairo.wal.WalReader.getPrimaryColumnIndex;

public class WalDataRecord implements Record, Sinkable {
    private final BorrowedArray array = new BorrowedArray();
    private WalReader reader;
    private long recordIndex = 0;

    @Override
    public ArrayView getArray(int col, int columnType) {
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        MemoryCR auxMem = reader.getColumn(absoluteColumnIndex + 1);
        MemoryCR dataMem = reader.getColumn(absoluteColumnIndex);

        array.of(columnType, auxMem.addressOf(0), auxMem.addressHi(), dataMem.addressOf(0), dataMem.addressHi(), recordIndex);
        return array;
    }

    @Override
    public BinarySequence getBin(int col) {
        final long offset = recordIndex * Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getBin(reader.getColumn(absoluteColumnIndex + 1).getLong(offset));
    }

    @Override
    public long getBinLen(int col) {
        final long offset = recordIndex * Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getBinLen(reader.getColumn(absoluteColumnIndex + 1).getLong(offset));
    }

    @Override
    public boolean getBool(int col) {
        final long offset = recordIndex;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getBool(offset);
    }

    @Override
    public byte getByte(int col) {
        final long offset = recordIndex;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getByte(offset);
    }

    @Override
    public char getChar(int col) {
        final long offset = recordIndex * Character.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getChar(offset);
    }

    @Override
    public void getDecimal128(int col, Decimal128 sink) {
        final long offset = recordIndex * 2 * Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        if (offset < 0) {
            sink.ofRawNull();
        } else {
            reader.getColumn(absoluteColumnIndex).getDecimal128(offset, sink);
        }
    }

    @Override
    public short getDecimal16(int col) {
        final long offset = recordIndex * Short.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return offset < 0 ? Decimals.DECIMAL16_NULL : reader.getColumn(absoluteColumnIndex).getShort(offset);
    }

    @Override
    public void getDecimal256(int col, Decimal256 sink) {
        final long offset = recordIndex * 4 * Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        if (offset < 0) {
            sink.ofRawNull();
        } else {
            reader.getColumn(absoluteColumnIndex).getDecimal256(offset, sink);
        }
    }

    @Override
    public int getDecimal32(int col) {
        final long offset = recordIndex * Integer.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return offset < 0 ? Decimals.DECIMAL32_NULL : reader.getColumn(absoluteColumnIndex).getInt(offset);
    }

    @Override
    public long getDecimal64(int col) {
        final long offset = recordIndex * Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return offset < 0 ? Decimals.DECIMAL64_NULL : reader.getColumn(absoluteColumnIndex).getLong(offset);
    }

    @Override
    public byte getDecimal8(int col) {
        final long offset = recordIndex;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return offset < 0 ? Decimals.DECIMAL8_NULL : reader.getColumn(absoluteColumnIndex).getByte(offset);
    }

    // only for tests
    @SuppressWarnings("SameParameterValue")
    public long getDesignatedTimestampRowId(int col) {
        final long offset = 2 * recordIndex * Long.BYTES + Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getLong(offset);
    }

    @Override
    public double getDouble(int col) {
        final long offset = recordIndex * Double.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getDouble(offset);
    }

    @Override
    public float getFloat(int col) {
        final long offset = recordIndex * Float.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getFloat(offset);
    }

    @Override
    public byte getGeoByte(int col) {
        final long offset = recordIndex * Byte.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return offset < 0 ? GeoHashes.BYTE_NULL : reader.getColumn(absoluteColumnIndex).getByte(offset);
    }

    @Override
    public int getGeoInt(int col) {
        final long offset = recordIndex * Integer.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return offset < 0 ? GeoHashes.INT_NULL : reader.getColumn(absoluteColumnIndex).getInt(offset);
    }

    @Override
    public long getGeoLong(int col) {
        final long offset = recordIndex * Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return offset < 0 ? GeoHashes.NULL : reader.getColumn(absoluteColumnIndex).getLong(offset);
    }

    @Override
    public short getGeoShort(int col) {
        final long offset = recordIndex * Short.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return offset < 0 ? GeoHashes.SHORT_NULL : reader.getColumn(absoluteColumnIndex).getShort(offset);
    }

    @Override
    public int getIPv4(int col) {
        final long offset = recordIndex * Integer.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getIPv4(offset);
    }

    @Override
    public int getInt(int col) {
        final long offset = recordIndex * Integer.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getInt(offset);
    }

    @Override
    public long getLong(int col) {
        final long offset = recordIndex * Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getLong(offset);
    }

    @Override
    public long getLong128Hi(int col) {
        final long offset = recordIndex * Long128.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getLong(offset + Long.BYTES);
    }

    @Override
    public long getLong128Lo(int col) {
        final long offset = recordIndex * Long128.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getLong(offset);
    }

    @Override
    public void getLong256(int col, CharSink<?> sink) {
        final long offset = recordIndex * Long256.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        reader.getColumn(absoluteColumnIndex).getLong256(offset, sink);
    }

    @Override
    public Long256 getLong256A(int col) {
        final long offset = recordIndex * Long256.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getLong256A(offset);
    }

    @Override
    public Long256 getLong256B(int col) {
        final long offset = recordIndex * Long256.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getLong256B(offset);
    }

    public long getRecordIndex() {
        return recordIndex;
    }

    @Override
    public long getRowId() {
        return Rows.toRowID(0, recordIndex);
    }

    @Override
    public short getShort(int col) {
        final long offset = recordIndex * Short.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getShort(offset);
    }

    @Override
    public CharSequence getStrA(int col) {
        final long offset = recordIndex * Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getStrA(reader.getColumn(absoluteColumnIndex + 1).getLong(offset));
    }

    @Override
    public CharSequence getStrB(int col) {
        final long offset = recordIndex * Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getStrB(reader.getColumn(absoluteColumnIndex + 1).getLong(offset));
    }

    @Override
    public int getStrLen(int col) {
        final long offset = recordIndex * Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getStrLen(reader.getColumn(absoluteColumnIndex + 1).getLong(offset));
    }

    @Override
    public CharSequence getSymA(int col) {
        return reader.getSymbolValue(col, getInt(col));
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
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return VarcharTypeDriver.getSplitValue(
                reader.getColumn(absoluteColumnIndex + 1),
                reader.getColumn(absoluteColumnIndex),
                recordIndex,
                1
        );
    }

    @Override
    public @Nullable Utf8Sequence getVarcharB(int col) {
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return VarcharTypeDriver.getSplitValue(
                reader.getColumn(absoluteColumnIndex + 1),
                reader.getColumn(absoluteColumnIndex),
                recordIndex,
                2
        );
    }

    @Override
    public int getVarcharSize(int col) {
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return VarcharTypeDriver.getValueSize(
                reader.getColumn(absoluteColumnIndex + 1),
                recordIndex
        );
    }

    public void incrementRecordIndex() {
        recordIndex++;
    }

    public void jumpTo(long recordIndex) {
        this.recordIndex = recordIndex;
    }

    public void of(WalReader reader) {
        this.reader = reader;
        jumpTo(-1);
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("WalReaderRecord [recordIndex=").put(recordIndex).putAscii(']');
    }

    private long getDesignatedTimestamp(int col) {
        final long offset = 2 * recordIndex * Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getLong(offset);
    }
}
