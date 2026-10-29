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

import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.wal.WriterRowUtils;
import io.questdb.griffin.SqlUtil;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal256;
import io.questdb.std.Long256;
import io.questdb.std.Uuid;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

/** Appends native column values; the owner supplies row, symbol and timestamp lifecycle. */
public abstract class NativeWriterRow implements TableWriter.Row {
    protected final StringSink tempSink = new StringSink();
    private final Decimal256 decimal256Sink = new Decimal256();
    private final Utf8StringSink tempUtf8Sink = new Utf8StringSink();
    private final Uuid uuid = new Uuid();

    @Override
    public void putArray(int columnIndex, @NotNull ArrayView arrayView) {
        ArrayTypeDriver.appendValue(
                getSecondaryColumn(columnIndex),
                getPrimaryColumn(columnIndex),
                arrayView
        );
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putBin(int columnIndex, long address, long len) {
        getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putBin(address, len));
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putBin(int columnIndex, BinarySequence sequence) {
        getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putBin(sequence));
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putBool(int columnIndex, boolean value) {
        getPrimaryColumn(columnIndex).putBool(value);
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putByte(int columnIndex, byte value) {
        getPrimaryColumn(columnIndex).putByte(value);
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putChar(int columnIndex, char value) {
        getPrimaryColumn(columnIndex).putChar(value);
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putDate(int columnIndex, long value) {
        putLong(columnIndex, value);
    }

    @Override
    public void putDecimal(int columnIndex, Decimal256 value) {
        int type = getColumnType(columnIndex);
        WriterRowUtils.putDecimal(columnIndex, value, type, this);
    }

    @Override
    public void putDecimal128(int columnIndex, long high, long low) {
        getPrimaryColumn(columnIndex).putDecimal128(high, low);
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putDecimal256(int columnIndex, long hh, long hl, long lh, long ll) {
        getPrimaryColumn(columnIndex).putDecimal256(hh, hl, lh, ll);
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putDecimalChar(int columnIndex, char decimalValue) {
        int columnType = getColumnType(columnIndex);
        WriterRowUtils.putDecimalChar(columnIndex, decimal256Sink, decimalValue, columnType, this);
    }

    @Override
    public void putDecimalStr(int columnIndex, CharSequence decimalValue) {
        int columnType = getColumnType(columnIndex);
        WriterRowUtils.putDecimalStr(columnIndex, decimal256Sink, decimalValue, columnType, this);
    }

    @Override
    public void putDecimalVarchar(int columnIndex, Utf8Sequence decimalValue) {
        int columnType = getColumnType(columnIndex);
        WriterRowUtils.putDecimalVarchar(columnIndex, decimal256Sink, decimalValue, columnType, this);
    }

    @Override
    public void putDouble(int columnIndex, double value) {
        getPrimaryColumn(columnIndex).putDouble(value);
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putFloat(int columnIndex, float value) {
        getPrimaryColumn(columnIndex).putFloat(value);
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putGeoHash(int columnIndex, long value) {
        int type = getColumnType(columnIndex);
        WriterRowUtils.putGeoHash(columnIndex, value, type, this);
    }

    @Override
    public void putGeoHashDeg(int columnIndex, double lat, double lon) {
        final int type = getColumnType(columnIndex);
        WriterRowUtils.putGeoHash(columnIndex, GeoHashes.fromCoordinatesDegUnsafe(lat, lon, ColumnType.getGeoHashBits(type)), type, this);
    }

    @Override
    public void putGeoStr(int columnIndex, CharSequence hash) {
        final int type = getColumnType(columnIndex);
        WriterRowUtils.putGeoStr(columnIndex, hash, type, this);
    }

    @Override
    public void putGeoVarchar(int columnIndex, Utf8Sequence hash) {
        final int type = getColumnType(columnIndex);
        WriterRowUtils.putGeoVarchar(columnIndex, hash, type, this);
    }

    @Override
    public void putIPv4(int columnIndex, int value) {
        putInt(columnIndex, value);
    }

    @Override
    public void putInt(int columnIndex, int value) {
        getPrimaryColumn(columnIndex).putInt(value);
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putLong(int columnIndex, long value) {
        getPrimaryColumn(columnIndex).putLong(value);
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putLong128(int columnIndex, long lo, long hi) {
        MemoryA primaryColumn = getPrimaryColumn(columnIndex);
        primaryColumn.putLong(lo);
        primaryColumn.putLong(hi);
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putLong256(int columnIndex, long l0, long l1, long l2, long l3) {
        getPrimaryColumn(columnIndex).putLong256(l0, l1, l2, l3);
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putLong256(int columnIndex, Long256 value) {
        getPrimaryColumn(columnIndex).putLong256(value.getLong0(), value.getLong1(), value.getLong2(), value.getLong3());
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putLong256(int columnIndex, CharSequence hexString) {
        getPrimaryColumn(columnIndex).putLong256(hexString);
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putLong256(int columnIndex, @NotNull CharSequence hexString, int start, int end) {
        getPrimaryColumn(columnIndex).putLong256(hexString, start, end);
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putLong256Utf8(int columnIndex, DirectUtf8Sequence hexString) {
        putLong256(columnIndex, hexString != null ? hexString.asAsciiCharSequence() : null);
    }

    @Override
    public void putLong256Utf8(int columnIndex, Utf8Sequence hexString) {
        if (hexString == null) {
            putLong256(columnIndex, (CharSequence) null);
            return;
        }
        if (hexString instanceof DirectUtf8Sequence) {
            putLong256Utf8(columnIndex, (DirectUtf8Sequence) hexString);
            return;
        }
        // Long256 hex strings are always ASCII
        putLong256(columnIndex, hexString.asAsciiCharSequence());
    }

    @Override
    public void putShort(int columnIndex, short value) {
        getPrimaryColumn(columnIndex).putShort(value);
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putStr(int columnIndex, CharSequence value) {
        getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStr(value));
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putStr(int columnIndex, char value) {
        getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStr(value));
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putStr(int columnIndex, CharSequence value, int pos, int len) {
        getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStr(value, pos, len));
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putStrUtf8(int columnIndex, DirectUtf8Sequence value) {
        getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStrUtf8(value));
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putStrUtf8(int columnIndex, Utf8Sequence value) {
        if (value instanceof DirectUtf8Sequence directValue) {
            putStrUtf8(columnIndex, directValue);
            return;
        }
        putStr(columnIndex, value != null ? Utf8s.utf8ToUtf16OrThrow(value, tempSink) : null);
    }

    @Override
    public void putUuid(int columnIndex, CharSequence uuidStr) {
        SqlUtil.implicitCastStrAsUuid(uuidStr, uuid);
        putLong128(columnIndex, uuid.getLo(), uuid.getHi());
    }

    @Override
    public void putUuidUtf8(int columnIndex, Utf8Sequence uuidStr) {
        SqlUtil.implicitCastStrAsUuid(uuidStr, uuid);
        putLong128(columnIndex, uuid.getLo(), uuid.getHi());
    }

    @Override
    public void putVarchar(int columnIndex, char value) {
        tempUtf8Sink.clear();
        tempUtf8Sink.put(value);
        VarcharTypeDriver.appendValue(
                getSecondaryColumn(columnIndex), getPrimaryColumn(columnIndex),
                tempUtf8Sink
        );
        setRowValueNotNull(columnIndex);
    }

    @Override
    public void putVarchar(int columnIndex, Utf8Sequence value) {
        VarcharTypeDriver.appendValue(
                getSecondaryColumn(columnIndex), getPrimaryColumn(columnIndex),
                value
        );
        setRowValueNotNull(columnIndex);
    }

    protected abstract int getColumnType(int columnIndex);

    protected abstract MemoryA getPrimaryColumn(int columnIndex);

    protected abstract MemoryA getSecondaryColumn(int columnIndex);

    protected abstract void setRowValueNotNull(int columnIndex);
}
