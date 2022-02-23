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
    private final long bufSize;
    private final FloatingDirectCharSink tempSink = new FloatingDirectCharSink();

    public LineTcpEventBuffer(long bufLo, long bufSize) {
        this.bufLo = bufLo;
        this.bufSize = bufLo + bufSize;
    }

    public long getAddress() {
        return bufLo;
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

    public long addColumnName(long address, CharSequence colName) {
        int length = colName.length();
        int capacity = Integer.BYTES + 2 * length;
        checkCapacity(address, capacity);

        // Negative length indicates to the writer thread that column is passed by
        // name rather than by index. When value is positive (on the else branch)
        // the value is treated as column index.
        Unsafe.getUnsafe().putInt(address, -1 * length);

        Chars.copyStrChars(colName, 0, length, address + Integer.BYTES);
        return address + capacity;
    }

    public long addDate(long address, long value) {
        checkCapacity(address, Long.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_DATE);
        Unsafe.getUnsafe().putLong(address + Byte.BYTES, value);
        return address + Long.BYTES + Byte.BYTES;
    }

    public void addDesignatedTimestamp(long address, long timestamp) {
        checkCapacity(address, Long.BYTES);
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

    public long addGeoHash(long address, DirectByteCharSequence value, int colTypeMeta) {
        long geohash;
        try {
            geohash = GeoHashes.fromStringTruncatingNl(value.getLo(), value.getHi(), Numbers.decodeLowShort(colTypeMeta));
        } catch (NumericException e) {
            geohash = GeoHashes.NULL;
        }
        switch (Numbers.decodeHighShort(colTypeMeta)) {
            default:
                checkCapacity(address, Long.BYTES + Byte.BYTES);
                Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_GEOLONG);
                Unsafe.getUnsafe().putLong(address + Byte.BYTES, geohash);
                return address + Long.BYTES + Byte.BYTES;
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

    public long addLong256(long address, DirectByteCharSequence value, boolean hasNonAsciiChars) {
        return addString(address, value, hasNonAsciiChars, LineTcpParser.ENTITY_TYPE_LONG256);
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

    public long addString(long address, DirectByteCharSequence value, boolean hasNonAsciiChars) {
        return addString(address, value, hasNonAsciiChars, LineTcpParser.ENTITY_TYPE_STRING);
    }

    public long addSymbol(long address, DirectByteCharSequence value, boolean hasNonAsciiChars, SymbolLookup symbolLookup) {
        final int maxLen = 2 * value.length();
        checkCapacity(address, Byte.BYTES + Integer.BYTES + maxLen);
        final long strPos = address + Byte.BYTES + Integer.BYTES; // skip field type and string length

        // via temp string the utf8 decoder will be writing directly to our buffer
        tempSink.of(strPos, strPos + maxLen);

        // this method will write column name to the buffer if it has to be utf8 decoded
        // otherwise it will write nothing.
        CharSequence columnValue = utf8ToUtf16(value, tempSink, hasNonAsciiChars);
        final int symIndex = symbolLookup.keyOf(columnValue);
        if (symIndex != SymbolTable.VALUE_NOT_FOUND) {
            // We know the symbol int value
            // Encode the int
            Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_CACHED_TAG);
            Unsafe.getUnsafe().putInt(address + Byte.BYTES, symIndex);
            return address + Integer.BYTES + Byte.BYTES;
        } else {
            // Symbol value cannot be resolved at this point
            // Encode whole string value into the message
            if (!hasNonAsciiChars) {
                tempSink.put(columnValue);
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

    public byte readByte(long address) {
        return Unsafe.getUnsafe().getByte(address);
    }

    public char readChar(long address) {
        return Unsafe.getUnsafe().getChar(address);
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
        tempSink.asCharSequence(address,  address + length * 2L);
        return tempSink;
    }

    private long addString(long address, DirectByteCharSequence value, boolean hasNonAsciiChars, byte entityTypeString) {
        int maxLen = 2 * value.length();
        checkCapacity(address, Byte.BYTES + Integer.BYTES + maxLen);
        long strPos = address + Byte.BYTES + Integer.BYTES; // skip field type and string length
        tempSink.of(strPos, strPos + maxLen);
        if (hasNonAsciiChars) {
            utf8ToUtf16Unchecked(value, tempSink);
        } else {
            tempSink.put(value);
        }
        final int length = tempSink.length();
        Unsafe.getUnsafe().putByte(address, entityTypeString);
        Unsafe.getUnsafe().putInt(address + Byte.BYTES, length);
        return address + length * 2L + Integer.BYTES + Byte.BYTES;
    }

    private void checkCapacity(long address, int length) {
        if (address + length > bufSize) {
            throw CairoException.instance(0).put("queue buffer overflow");
        }
    }
}
