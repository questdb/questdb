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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.arr.BorrowedFlatArrayView;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.Chars;
import io.questdb.std.Decimal256;
import io.questdb.std.Long128;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.Uuid;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.FlyweightDirectUtf16Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

import static io.questdb.cutlass.line.tcp.LineTcpParser.ENTITY_TYPE_NULL;

public class LineTcpEventBuffer {
    private final BorrowedArray borrowedDirectArrayView = new BorrowedArray();
    private final long bufLo;
    private final long bufSize;
    private final FlyweightDirectUtf16Sink tempSink = new FlyweightDirectUtf16Sink();
    private final FlyweightDirectUtf16Sink tempSinkB = new FlyweightDirectUtf16Sink();
    private final DirectUtf8String utf8Sequence = new DirectUtf8String();

    public LineTcpEventBuffer(long bufLo, long bufSize) {
        this.bufLo = bufLo;
        this.bufSize = bufLo + bufSize;
    }

    public long addArray(long address, BorrowedArray arrayView) {
        if (arrayView == null) {
            return addNull(address);
        }

        int dims = arrayView.getDimCount();
        BorrowedFlatArrayView values = (BorrowedFlatArrayView) arrayView.flatView();

        // record totalLength to trade space for time.
        // +-------------+-------------+-------------+------------------------+--------------------+
        // |  type flag  | totalLength |  arrayType  |       shapes           |    flat values     |
        // +-------------+-------------+-------------+------------------------+--------------------+
        // |    1 byte   |   4 bytes   |  4 bytes    |     $dims * 4 bytes    |                    |
        // +-------------+-------------+-------------+------------------------+--------------------+
        int totalLength = Integer.BYTES + Integer.BYTES + dims * Integer.BYTES + values.size();
        // non-wal tables do not support large array ingestion.
        checkCapacity(address, totalLength + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_ARRAY);
        address += Byte.BYTES;
        Unsafe.getUnsafe().putInt(address, totalLength);
        address += Integer.BYTES;
        Unsafe.getUnsafe().putInt(address, arrayView.getType());
        address += Integer.BYTES;
        for (int i = 0; i < dims; i++) {
            Unsafe.getUnsafe().putInt(address, arrayView.getDimLen(i));
            address += Integer.BYTES;
        }
        Vect.memcpy(address, values.ptr(), values.size());
        address += values.size();
        return address;
    }

    public long addBoolean(long address, byte value) {
        checkCapacity(address, Byte.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_BOOLEAN);
        Unsafe.getUnsafe().putByte(address + Byte.BYTES, value);
        return address + Byte.BYTES + Byte.BYTES;
    }

    public long addByte(long address, byte value) {
        checkCapacity(address, Byte.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_BYTE);
        Unsafe.getUnsafe().putByte(address + Byte.BYTES, value);
        return address + Byte.BYTES + Byte.BYTES;
    }

    public long addChar(long address, char value) {
        checkCapacity(address, Character.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_CHAR);
        Unsafe.getUnsafe().putChar(address + Byte.BYTES, value);
        return address + Character.BYTES + Byte.BYTES;
    }

    public long addColumnIndex(long address, int colIndex) {
        checkCapacity(address, Integer.BYTES);
        Unsafe.getUnsafe().putInt(address, colIndex);
        return address + Integer.BYTES;
    }

    public long addColumnName(long address, CharSequence colName, CharSequence principal) {
        int colNameLen = colName.length();
        int principalLen = principal != null ? principal.length() : 0;
        int colNameCapacity = Integer.BYTES + 2 * colNameLen;
        int fullCapacity = colNameCapacity + Integer.BYTES + 2 * principalLen;
        checkCapacity(address, fullCapacity);

        // Negative length indicates to the writer thread that column is passed by
        // name rather than by index. When value is positive (on the else branch)
        // the value is treated as column index.
        Unsafe.getUnsafe().putInt(address, -colNameLen);
        Chars.copyStrChars(colName, 0, colNameLen, address + Integer.BYTES);

        // Now write principal name, so that we can call DdlListener#onColumnAdded()
        // when adding the column.
        Unsafe.getUnsafe().putInt(address + colNameCapacity, principalLen);
        if (principalLen > 0) {
            Chars.copyStrChars(principal, 0, principalLen, address + colNameCapacity + Integer.BYTES);
        }

        return address + fullCapacity;
    }

    public long addDate(long address, long value) {
        checkCapacity(address, Long.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_DATE);
        Unsafe.getUnsafe().putLong(address + Byte.BYTES, value);
        return address + Long.BYTES + Byte.BYTES;
    }

    public long addDecimal(long address, Decimal256 decimal256, int columnType) {
        // Layout:
        // +-------------+--------+----------+
        // | column type | scale  |  values  |
        // +-------------+--------+----------+
        // |   4 bytes   | 1 byte | 32 bytes |
        // +-------------+--------+----------+

        checkCapacity(address, Byte.BYTES * 2 + Integer.BYTES + Decimal256.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_DECIMAL);
        Unsafe.getUnsafe().putInt(address + Byte.BYTES, columnType);
        Unsafe.getUnsafe().putByte(address + Integer.BYTES + Byte.BYTES, (byte) decimal256.getScale());
        Unsafe.getUnsafe().putLong(address + Integer.BYTES + Byte.BYTES * 2, decimal256.getHh());
        Unsafe.getUnsafe().putLong(address + Integer.BYTES + Byte.BYTES * 2 + Long.BYTES, decimal256.getHl());
        Unsafe.getUnsafe().putLong(address + Integer.BYTES + Byte.BYTES * 2 + Long.BYTES * 2, decimal256.getLh());
        Unsafe.getUnsafe().putLong(address + Integer.BYTES + Byte.BYTES * 2 + Long.BYTES * 3, decimal256.getLl());
        return address + Byte.BYTES * 2 + Integer.BYTES + Decimal256.BYTES;
    }

    public void addDesignatedTimestamp(long address, long timestamp) {
        checkCapacity(address, Long.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putLong(address, timestamp);
    }

    public long addDouble(long address, double value) {
        checkCapacity(address, Double.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_DOUBLE);
        Unsafe.getUnsafe().putDouble(address + Byte.BYTES, value);
        return address + Double.BYTES + Byte.BYTES;
    }

    public long addFloat(long address, float value) {
        checkCapacity(address, Float.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_FLOAT);
        Unsafe.getUnsafe().putFloat(address + Byte.BYTES, value);
        return address + Float.BYTES + Byte.BYTES;
    }

    public long addGeoHash(long address, DirectUtf8Sequence value, int colTypeMeta) {
        long geohash;
        try {
            geohash = GeoHashes.fromAsciiTruncatingNl(value.lo(), value.hi(), Numbers.decodeLowShort(colTypeMeta));
        } catch (NumericException e) {
            geohash = GeoHashes.NULL;
        }
        switch (Numbers.decodeHighShort(colTypeMeta)) {
            case ColumnType.GEOINT:
                checkCapacity(address, Integer.BYTES + Byte.BYTES);
                Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_GEOINT);
                Unsafe.getUnsafe().putInt(address + Byte.BYTES, (int) geohash);
                return address + Integer.BYTES + Byte.BYTES;
            case ColumnType.GEOSHORT:
                checkCapacity(address, Short.BYTES + Byte.BYTES);
                Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_GEOSHORT);
                Unsafe.getUnsafe().putShort(address + Byte.BYTES, (short) geohash);
                return address + Short.BYTES + Byte.BYTES;
            case ColumnType.GEOBYTE:
                checkCapacity(address, Byte.BYTES + Byte.BYTES);
                Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_GEOBYTE);
                Unsafe.getUnsafe().putByte(address + Byte.BYTES, (byte) geohash);
                return address + Byte.BYTES + Byte.BYTES;
            default:
                checkCapacity(address, Long.BYTES + Byte.BYTES);
                Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_GEOLONG);
                Unsafe.getUnsafe().putLong(address + Byte.BYTES, geohash);
                return address + Long.BYTES + Byte.BYTES;
        }
    }

    public long addInt(long address, int value) {
        checkCapacity(address, Integer.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_INTEGER);
        Unsafe.getUnsafe().putInt(address + Byte.BYTES, value);
        return address + Integer.BYTES + Byte.BYTES;
    }

    public long addLong(long address, long value) {
        checkCapacity(address, Long.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_LONG);
        Unsafe.getUnsafe().putLong(address + Byte.BYTES, value);
        return address + Long.BYTES + Byte.BYTES;
    }

    public long addLong256(long address, DirectUtf8Sequence value) {
        return addString(address, value, LineTcpParser.ENTITY_TYPE_LONG256);
    }

    public long addNull(long address) {
        checkCapacity(address, Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_NULL);
        return address + Byte.BYTES;
    }

    public void addNumOfColumns(long address, int numOfColumns) {
        checkCapacity(address, Integer.BYTES);
        Unsafe.getUnsafe().putInt(address, numOfColumns);
    }

    public long addShort(long address, short value) {
        checkCapacity(address, Short.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_SHORT);
        Unsafe.getUnsafe().putShort(address + Byte.BYTES, value);
        return address + Short.BYTES + Byte.BYTES;
    }

    public long addString(long address, DirectUtf8Sequence value) {
        return addString(address, value, LineTcpParser.ENTITY_TYPE_STRING);
    }

    public void addStructureVersion(long address, long structureVersion) {
        checkCapacity(address, Long.BYTES);
        Unsafe.getUnsafe().putLong(address, structureVersion);
    }

    public long addSymbol(long address, DirectUtf8Sequence value, DirectUtf8SymbolLookup symbolLookup) {
        final int maxLen = 2 * value.size();
        checkCapacity(address, Byte.BYTES + Integer.BYTES + maxLen);
        final long strPos = address + Byte.BYTES + Integer.BYTES; // skip field type and string length

        // via temp string the utf8 decoder will be writing directly to our buffer
        tempSink.of(strPos, strPos + maxLen);

        final int symIndex = symbolLookup.keyOf(value);
        if (symIndex != SymbolTable.VALUE_NOT_FOUND) {
            // We know the symbol int value
            // Encode the int
            Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_CACHED_TAG);
            Unsafe.getUnsafe().putInt(address + Byte.BYTES, symIndex);
            return address + Integer.BYTES + Byte.BYTES;
        } else {
            // Symbol value cannot be resolved at this point
            // Encode whole string value into the message
            if (value.isAscii()) {
                tempSink.put(value);
            } else {
                Utf8s.utf8ToUtf16(value, tempSink);
            }
            final int length = tempSink.length();
            Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_TAG);
            Unsafe.getUnsafe().putInt(address + Byte.BYTES, length);
            return address + length * 2L + Integer.BYTES + Byte.BYTES;
        }
    }

    public long addTimestamp(long address, long value) {
        checkCapacity(address, Long.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_TIMESTAMP);
        Unsafe.getUnsafe().putLong(address + Byte.BYTES, value);
        return address + Long.BYTES + Byte.BYTES;
    }

    /**
     * Add UUID encoded as a string to the buffer.
     * <p>
     * Technically, DirectByteCharSequence is UTF-8 encoded, but any non-ASCII character will cause the UUID
     * to be rejected by the parser. Hence, we do not have to bother with UTF-8 decoding.
     *
     * @param offset offset in the buffer to write to
     * @param value  value to write
     * @return new offset
     * @throws NumericException if the value is not a valid UUID string
     */
    public long addUuid(long offset, DirectUtf8Sequence value) throws NumericException {
        checkCapacity(offset, Byte.BYTES + 2 * Long.BYTES);
        final CharSequence csView = value.asAsciiCharSequence();
        Uuid.checkDashesAndLength(csView);
        long hi = Uuid.parseHi(csView);
        long lo = Uuid.parseLo(csView);
        Unsafe.getUnsafe().putByte(offset, LineTcpParser.ENTITY_TYPE_UUID);
        offset += Byte.BYTES;
        Unsafe.getUnsafe().putLong(offset, lo);
        offset += Long.BYTES;
        Unsafe.getUnsafe().putLong(offset, hi);
        return offset + Long.BYTES;
    }

    public long addVarchar(long address, DirectUtf8Sequence value) {
        final int valueSize = value.size();
        final int totalSize = Byte.BYTES + Byte.BYTES + Integer.BYTES + valueSize;
        checkCapacity(address, totalSize);
        Unsafe.getUnsafe().putByte(address++, LineTcpParser.ENTITY_TYPE_VARCHAR);
        Unsafe.getUnsafe().putByte(address++, (byte) (value.isAscii() ? 0 : 1));
        Unsafe.getUnsafe().putInt(address, valueSize);
        address += Integer.BYTES;
        value.writeTo(address, 0, valueSize);
        return address + valueSize;
    }

    public long columnValueLength(byte entityType, long offset) {
        CharSequence cs;
        return switch (entityType) {
            case LineTcpParser.ENTITY_TYPE_TAG, LineTcpParser.ENTITY_TYPE_STRING, LineTcpParser.ENTITY_TYPE_LONG256 -> {
                cs = readUtf16Chars(offset);
                yield cs.length() * 2L + Integer.BYTES;
            }
            case LineTcpParser.ENTITY_TYPE_BYTE, LineTcpParser.ENTITY_TYPE_GEOBYTE, LineTcpParser.ENTITY_TYPE_BOOLEAN ->
                    Byte.BYTES;
            case LineTcpParser.ENTITY_TYPE_SHORT, LineTcpParser.ENTITY_TYPE_GEOSHORT -> Short.BYTES;
            case LineTcpParser.ENTITY_TYPE_CHAR -> Character.BYTES;
            case LineTcpParser.ENTITY_TYPE_CACHED_TAG, LineTcpParser.ENTITY_TYPE_INTEGER,
                 LineTcpParser.ENTITY_TYPE_GEOINT -> Integer.BYTES;
            case LineTcpParser.ENTITY_TYPE_LONG, LineTcpParser.ENTITY_TYPE_GEOLONG, LineTcpParser.ENTITY_TYPE_DATE,
                 LineTcpParser.ENTITY_TYPE_TIMESTAMP -> Long.BYTES;
            case LineTcpParser.ENTITY_TYPE_FLOAT -> Float.BYTES;
            case LineTcpParser.ENTITY_TYPE_DOUBLE -> Double.BYTES;
            case LineTcpParser.ENTITY_TYPE_UUID -> Long128.BYTES;
            case LineTcpParser.ENTITY_TYPE_ARRAY -> readInt(offset);
            case ENTITY_TYPE_NULL -> 0;
            default -> throw new UnsupportedOperationException("entityType " + entityType + " is not implemented!");
        };
    }

    public long getAddress() {
        return bufLo;
    }

    public long getAddressAfterHeader() {
        // The header contains structure version (long), timestamp (long) and number of columns (int).
        return bufLo + 2 * Long.BYTES + Integer.BYTES;
    }

    public ArrayView readArray(long address) {
        int totalSize = readInt(address);
        address += Integer.BYTES;
        int type = readInt(address);
        address += Integer.BYTES;
        int dims = ColumnType.decodeWeakArrayDimensionality(type);
        if (dims < 1 || dims > ColumnType.ARRAY_NDIMS_LIMIT) {
            throw CairoException.critical(0).put("unsupported array dimensionality [dims=").put(dims).put(']');
        }
        borrowedDirectArrayView.of(
                type,
                address,
                address + (long) dims * Integer.BYTES,
                totalSize - (dims + 2) * Integer.BYTES
        );
        return borrowedDirectArrayView;
    }

    public byte readByte(long address) {
        return Unsafe.getUnsafe().getByte(address);
    }

    public char readChar(long address) {
        return Unsafe.getUnsafe().getChar(address);
    }

    public int readDecimal(long address, Decimal256 result) {
        int scale = Unsafe.getUnsafe().getByte(address + Integer.BYTES);
        result.of(
                Unsafe.getUnsafe().getLong(address + Integer.BYTES + Byte.BYTES),
                Unsafe.getUnsafe().getLong(address + Integer.BYTES + Byte.BYTES + Long.BYTES),
                Unsafe.getUnsafe().getLong(address + Integer.BYTES + Byte.BYTES + Long.BYTES * 2),
                Unsafe.getUnsafe().getLong(address + Integer.BYTES + Byte.BYTES + Long.BYTES * 3),
                scale
        );
        return Unsafe.getUnsafe().getInt(address);
    }

    public double readDouble(long address) {
        return Unsafe.getUnsafe().getDouble(address);
    }

    public float readFloat(long address) {
        return Unsafe.getUnsafe().getFloat(address);
    }

    public int readInt(long address) {
        return Unsafe.getUnsafe().getInt(address);
    }

    public long readLong(long address) {
        return Unsafe.getUnsafe().getLong(address);
    }

    public short readShort(long address) {
        return Unsafe.getUnsafe().getShort(address);
    }

    public CharSequence readUtf16Chars(long address) {
        int len = readInt(address);
        return readUtf16Chars(address + Integer.BYTES, len);
    }

    public CharSequence readUtf16Chars(long address, int length) {
        return tempSink.asCharSequence(address, address + length * 2L);
    }

    public CharSequence readUtf16CharsB(long address, int length) {
        return tempSinkB.asCharSequence(address, address + length * 2L);
    }

    public Utf8Sequence readVarchar(long address, boolean ascii) {
        int size = readInt(address);
        return utf8Sequence.of(address + Integer.BYTES, address + Integer.BYTES + size, ascii);
    }

    private long addString(long address, DirectUtf8Sequence value, byte entityTypeString) {
        int maxLen = 2 * value.size();
        checkCapacity(address, Byte.BYTES + Integer.BYTES + maxLen);
        long strPos = address + Byte.BYTES + Integer.BYTES; // skip field type and string length
        tempSink.of(strPos, strPos + maxLen);
        if (value.isAscii()) {
            tempSink.put(value);
        } else {
            Utf8s.utf8ToUtf16Unchecked(value, tempSink);
        }
        final int length = tempSink.length();
        Unsafe.getUnsafe().putByte(address, entityTypeString);
        Unsafe.getUnsafe().putInt(address + Byte.BYTES, length);
        return address + length * 2L + Integer.BYTES + Byte.BYTES;
    }

    private void checkCapacity(long address, int length) {
        if (address + length > bufSize) {
            throw CairoException.critical(0).put("queue buffer overflow");
        }
    }
}
