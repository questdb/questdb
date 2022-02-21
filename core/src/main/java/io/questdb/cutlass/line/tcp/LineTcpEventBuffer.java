/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.sql.SymbolLookup;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.FloatingDirectCharSink;

import static io.questdb.cutlass.line.tcp.LineTcpUtils.utf8ToUtf16;
import static io.questdb.cutlass.line.tcp.LineTcpUtils.utf8ToUtf16Unchecked;

public class LineTcpEventBuffer {
    private final long bufLo;
    private final long bufMax;
    private final FloatingDirectCharSink tempSink = new FloatingDirectCharSink();

    public LineTcpEventBuffer(long bufLo, long bufSize) {
        this.bufLo = bufLo;
        this.bufMax = bufSize;
    }

    public long addBoolean(long offset, byte value) {
        checkCapacity(offset, Byte.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(bufLo + offset, LineTcpParser.ENTITY_TYPE_BOOLEAN);
        Unsafe.getUnsafe().putByte(bufLo + offset + Byte.BYTES, value);
        return offset + Byte.BYTES * 2;
    }

    public long addByte(long offset, byte value) {
        checkCapacity(offset, Byte.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(bufLo + offset, LineTcpParser.ENTITY_TYPE_BYTE);
        Unsafe.getUnsafe().putByte(bufLo + offset + 1, value);
        return offset + Byte.BYTES * 2;
    }

    public long addChar(long offset, char value) {
        checkCapacity(offset, Character.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(bufLo + offset, LineTcpParser.ENTITY_TYPE_CHAR);
        Unsafe.getUnsafe().putChar(bufLo + offset + 1, value);
        return offset + Character.BYTES + Byte.BYTES;
    }

    public long addColumnIndex(long offset, int colIndex) {
        checkCapacity(offset, Integer.BYTES);
        Unsafe.getUnsafe().putInt(bufLo + offset, colIndex);
        return offset + Integer.BYTES;
    }

    public long addColumnName(long offset, CharSequence colName) {
        int length = colName.length();
        int len = 2 * length;
        checkCapacity(offset, Integer.BYTES + len);

        // Negative length indicates to the writer thread that column is passed by
        // name rather than by index. When value is positive (on the else branch)
        // the value is treated as column index.
        Unsafe.getUnsafe().putInt(bufLo + offset, -1 * length);

        Chars.copyStrChars(colName, 0, length, bufLo + offset + Integer.BYTES);
        return offset + len + Integer.BYTES;
    }

    public long addDate(long offset, long value) {
        checkCapacity(offset, Long.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(bufLo + offset, LineTcpParser.ENTITY_TYPE_DATE);
        Unsafe.getUnsafe().putLong(bufLo + offset + 1, value);
        return offset + Long.BYTES + Byte.BYTES;
    }

    public void addDesignatedTimestamp(long offset, long timestamp) {
        checkCapacity(offset, Long.BYTES);
        Unsafe.getUnsafe().putLong(bufLo + offset, timestamp);
    }

    public long addDouble(long offset, double value) {
        checkCapacity(offset, Double.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(bufLo + offset, LineTcpParser.ENTITY_TYPE_DOUBLE);
        Unsafe.getUnsafe().putDouble(bufLo + offset + 1, value);
        return offset + Double.BYTES + Byte.BYTES;
    }

    public long addFloat(long offset, float value) {
        checkCapacity(offset, Float.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(bufLo + offset, LineTcpParser.ENTITY_TYPE_FLOAT);
        Unsafe.getUnsafe().putFloat(bufLo + offset + 1, value);
        return offset + Float.BYTES + Byte.BYTES;
    }

    public long addGeoHash(long offset, DirectByteCharSequence value, int colTypeMeta) {
        long geohash;
        try {
            geohash = GeoHashes.fromStringTruncatingNl(value.getLo(), value.getHi(), Numbers.decodeLowShort(colTypeMeta));
        } catch (NumericException e) {
            geohash = GeoHashes.NULL;
        }
        switch (Numbers.decodeHighShort(colTypeMeta)) {
            default:
                checkCapacity(offset, Long.BYTES + Byte.BYTES);
                Unsafe.getUnsafe().putByte(bufLo + offset, LineTcpParser.ENTITY_TYPE_GEOLONG);
                Unsafe.getUnsafe().putLong(bufLo + offset + 1, geohash);
                return offset + Long.BYTES + Byte.BYTES;
            case ColumnType.GEOINT:
                checkCapacity(offset, Integer.BYTES + Byte.BYTES);
                Unsafe.getUnsafe().putByte(bufLo + offset, LineTcpParser.ENTITY_TYPE_GEOINT);
                Unsafe.getUnsafe().putInt(bufLo + offset + 1, (int) geohash);
                return offset + Integer.BYTES + Byte.BYTES;
            case ColumnType.GEOSHORT:
                checkCapacity(offset, Short.BYTES + Byte.BYTES);
                Unsafe.getUnsafe().putByte(bufLo + offset, LineTcpParser.ENTITY_TYPE_GEOSHORT);
                Unsafe.getUnsafe().putShort(bufLo + offset + 1, (short) geohash);
                return offset + Short.BYTES + Byte.BYTES;
            case ColumnType.GEOBYTE:
                checkCapacity(offset, Byte.BYTES + Byte.BYTES);
                Unsafe.getUnsafe().putByte(bufLo + offset, LineTcpParser.ENTITY_TYPE_GEOBYTE);
                Unsafe.getUnsafe().putByte(bufLo + offset + 1, (byte) geohash);
                return offset + Byte.BYTES * 2;
        }
    }

    public long addInt(long offset, int value) {
        checkCapacity(offset, Integer.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(bufLo + offset, LineTcpParser.ENTITY_TYPE_INTEGER);
        Unsafe.getUnsafe().putInt(bufLo + offset + 1, value);
        return offset + Integer.BYTES + Byte.BYTES;
    }

    public long addLong(long offset, long value) {
        checkCapacity(offset, Long.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(bufLo + offset, LineTcpParser.ENTITY_TYPE_LONG);
        Unsafe.getUnsafe().putLong(bufLo + offset + Byte.BYTES, value);
        return offset + Long.BYTES + Byte.BYTES;
    }

    public long addLong256(long offset, DirectByteCharSequence value, boolean hasNonAsciiChars) {
        return addString(offset, value, hasNonAsciiChars, LineTcpParser.ENTITY_TYPE_LONG256);
    }

    public long addNull(long offset) {
        checkCapacity(offset, Byte.BYTES);
        Unsafe.getUnsafe().putByte(bufLo + offset, LineTcpParser.ENTITY_TYPE_NULL);
        return offset + Byte.BYTES;
    }

    public void addNumOfColumns(long offset, int numOfColumns) {
        checkCapacity(offset, Integer.BYTES);
        Unsafe.getUnsafe().putInt(bufLo + offset, numOfColumns);
    }

    public long addShort(long offset, short value) {
        checkCapacity(offset, Short.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(bufLo + offset, LineTcpParser.ENTITY_TYPE_SHORT);
        Unsafe.getUnsafe().putShort(bufLo + offset + 1, value);
        return offset + Short.BYTES + Byte.BYTES;
    }

    public long addString(long offset, DirectByteCharSequence value, boolean hasNonAsciiChars) {
        return addString(offset, value, hasNonAsciiChars, LineTcpParser.ENTITY_TYPE_STRING);
    }

    public long addSymbol(long offset, DirectByteCharSequence value, boolean hasNonAsciiChars, SymbolLookup symbolLookup) {
        final int maxLen = 2 * value.length();
        checkCapacity(offset, Byte.BYTES + Integer.BYTES + maxLen);
        final long strPos = bufLo + offset + Byte.BYTES + Integer.BYTES; // skip field type and string length

        // via temp string the utf8 decoder will be writing directly to our buffer
        tempSink.of(strPos, strPos + maxLen);

        // this method will write column name to the buffer if it has to be utf8 decoded
        // otherwise it will write nothing.
        CharSequence columnValue = utf8ToUtf16(value, tempSink, hasNonAsciiChars);
        final int symIndex = symbolLookup.keyOf(columnValue);
        if (symIndex != SymbolTable.VALUE_NOT_FOUND) {
            // We know the symbol int value
            // Encode the int
            Unsafe.getUnsafe().putByte(bufLo + offset, LineTcpParser.ENTITY_TYPE_CACHED_TAG);
            Unsafe.getUnsafe().putInt(bufLo + offset + 1, symIndex);
            return offset + Integer.BYTES + 1;
        } else {
            // Symbol value cannot be resolved at this point
            // Encode whole string value into the message
            if (!hasNonAsciiChars) {
                tempSink.put(columnValue);
            }
            final int length = tempSink.length();
            Unsafe.getUnsafe().putByte(bufLo + offset, LineTcpParser.ENTITY_TYPE_TAG);
            Unsafe.getUnsafe().putInt(bufLo + offset + 1, length);
            return offset + length * 2L + Integer.BYTES + 1;
        }
    }

    public long addTimestamp(long offset, long value) {
        checkCapacity(offset, Long.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(bufLo + offset, LineTcpParser.ENTITY_TYPE_TIMESTAMP);
        Unsafe.getUnsafe().putLong(bufLo + offset + 1, value);
        return offset + Long.BYTES + Byte.BYTES;
    }

    public byte readByte(long offset) {
        return Unsafe.getUnsafe().getByte(bufLo + offset);
    }

    public char readChar(long offset) {
        return Unsafe.getUnsafe().getChar(bufLo + offset);
    }

    public double readDouble(long offset) {
        return Unsafe.getUnsafe().getDouble(bufLo + offset);
    }

    public float readFloat(long offset) {
        return Unsafe.getUnsafe().getFloat(bufLo + offset);
    }

    public int readInt(long offset) {
        return Unsafe.getUnsafe().getInt(bufLo + offset);
    }

    public long readLong(long offset) {
        return Unsafe.getUnsafe().getLong(bufLo + offset);
    }

    public short readShort(long offset) {
        return Unsafe.getUnsafe().getShort(bufLo + offset);
    }

    public CharSequence readUtf16Chars(long offset) {
        int len = readInt(offset);
        return readUtf16Chars(offset + Integer.BYTES, len);
    }

    public CharSequence readUtf16Chars(long offset, int length) {
        tempSink.asCharSequence(bufLo + offset, bufLo + offset + length * 2L);
        return tempSink;
    }

    private long addString(long offset, DirectByteCharSequence value, boolean hasNonAsciiChars, byte entityTypeString) {
        int maxLen = 2 * value.length();
        checkCapacity(offset, Byte.BYTES + Integer.BYTES + maxLen);
        long strPos = bufLo + offset + Byte.BYTES + Integer.BYTES; // skip field type and string length
        tempSink.of(strPos, strPos + maxLen);
        if (hasNonAsciiChars) {
            utf8ToUtf16Unchecked(value, tempSink);
        } else {
            tempSink.put(value);
        }
        final int length = tempSink.length();
        Unsafe.getUnsafe().putByte(bufLo + offset, entityTypeString);
        Unsafe.getUnsafe().putInt(bufLo + offset + 1, length);
        return offset + length * 2L + Integer.BYTES + Byte.BYTES;
    }

    private void checkCapacity(long offset, int length) {
        if (offset + length > bufMax) {
            throw CairoException.instance(0).put("queue buffer overflow");
        }
    }
}
