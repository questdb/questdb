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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.FloatingDirectCharSink;

import static io.questdb.cutlass.line.tcp.LineTcpUtils.utf8ToUtf16;

public class LineTcpEventBuffer {
    private final long bufLo;
    private final long bufMax;

    private long bufPos;

    private DirectByteCharSequence value;
    private FloatingDirectCharSink tempSink;
    private boolean hasNonAsciiChars;

    public LineTcpEventBuffer(long bufLo, long bufSize) {
        this.bufLo = bufLo;
        this.bufPos = bufLo;
        this.bufMax = bufLo + bufSize;
    }

    public void addBoolean(byte value) {
        checkCapacity(Byte.BYTES + Byte.BYTES);
        putByte(LineTcpParser.ENTITY_TYPE_BOOLEAN);
        putByte(value);
    }

    public void addByte(byte value) {
        checkCapacity(Byte.BYTES + Byte.BYTES);
        putByte(LineTcpParser.ENTITY_TYPE_BYTE);
        putByte(value);
    }

    public void addChar(char value) {
        checkCapacity(Character.BYTES + Byte.BYTES);
        putByte(LineTcpParser.ENTITY_TYPE_CHAR);
        putChar(value);
    }

    public void addColumnIndex(int colIndex) {
        checkCapacity(Integer.BYTES);
        putInt(colIndex);
    }

    public void addColumnName(CharSequence colName) {
        int length = colName.length();
        int len = 2 * length;
        checkCapacity(Integer.BYTES + len);

        // Negative length indicates to the writer thread that column is passed by
        // name rather than by index. When value is positive (on the else branch)
        // the value is treated as column index.
        putInt(-1 * length);

        Chars.copyStrChars(colName, 0, length, bufPos);
        bufPos += len;
    }

    public void addDate(long value) {
        checkCapacity(Long.BYTES + Byte.BYTES);
        putByte(LineTcpParser.ENTITY_TYPE_DATE);
        putLong(value);
    }

    public void addDesignatedTimestamp(long timestamp) {
        checkCapacity(Long.BYTES);
        putLong(timestamp);
    }

    public void addDouble(double value) {
        checkCapacity(Double.BYTES + Byte.BYTES);
        putByte(LineTcpParser.ENTITY_TYPE_DOUBLE);
        putDouble(value);
    }

    public void addFloat(float value) {
        checkCapacity(Float.BYTES + Byte.BYTES);
        putByte(LineTcpParser.ENTITY_TYPE_FLOAT);
        putFloat(value);
    }

    public void addGeoHash(DirectByteCharSequence value, int colTypeMeta) {
        long geohash;
        try {
            geohash = GeoHashes.fromStringTruncatingNl(value.getLo(), value.getHi(), Numbers.decodeLowShort(colTypeMeta));
        } catch (NumericException e) {
            geohash = GeoHashes.NULL;
        }
        switch (Numbers.decodeHighShort(colTypeMeta)) {
            default:
                checkCapacity(Long.BYTES + Byte.BYTES);
                putByte(LineTcpParser.ENTITY_TYPE_GEOLONG);
                putLong(geohash);
                break;
            case ColumnType.GEOINT:
                checkCapacity(Integer.BYTES + Byte.BYTES);
                putByte(LineTcpParser.ENTITY_TYPE_GEOINT);
                putInt((int) geohash);
                break;
            case ColumnType.GEOSHORT:
                checkCapacity(Short.BYTES + Byte.BYTES);
                putByte(LineTcpParser.ENTITY_TYPE_GEOSHORT);
                putShort((short) geohash);
                break;
            case ColumnType.GEOBYTE:
                checkCapacity(Byte.BYTES + Byte.BYTES);
                putByte(LineTcpParser.ENTITY_TYPE_GEOBYTE);
                putByte((byte) geohash);
                break;
        }
    }

    public void addInt(int value) {
        checkCapacity(Integer.BYTES + Byte.BYTES);
        putByte(LineTcpParser.ENTITY_TYPE_INTEGER);
        putInt(value);
    }

    public void addLong(long value) {
        checkCapacity(Long.BYTES + Byte.BYTES);
        putByte(LineTcpParser.ENTITY_TYPE_LONG);
        putLong(value);
    }

    public void addLong256(DirectByteCharSequence value, FloatingDirectCharSink tempSink, boolean hasNonAsciiChars) {
        prepareUtf8CharSequence(value, tempSink, hasNonAsciiChars);
        closePreparedUtf8CharSequence(LineTcpParser.ENTITY_TYPE_LONG256);
    }

    public void addNull() {
        checkCapacity(Byte.BYTES);
        putByte(LineTcpParser.ENTITY_TYPE_NULL);
    }

    public void addNumOfColumns(int numOfColumns) {
        checkCapacity(Integer.BYTES);
        putInt(numOfColumns);
    }

    public void addShort(short value) {
        checkCapacity(Short.BYTES + Byte.BYTES);
        putByte(LineTcpParser.ENTITY_TYPE_SHORT);
        putShort(value);
    }

    public void addString(DirectByteCharSequence value, FloatingDirectCharSink tempSink, boolean hasNonAsciiChars) {
        prepareUtf8CharSequence(value, tempSink, hasNonAsciiChars);
        closePreparedUtf8CharSequence(LineTcpParser.ENTITY_TYPE_STRING);
    }

    public void addSymbol(
            DirectByteCharSequence value,
            FloatingDirectCharSink tempSink,
            boolean hasNonAsciiChars,
            TableUpdateDetails.ThreadLocalDetails localDetails,
            int colIndex
    ) {
        CharSequence columnValue = prepareUtf8CharSequence(value, tempSink, hasNonAsciiChars);
        int symIndex = getSymbolIndex(localDetails, colIndex, columnValue);
        if (symIndex != SymbolTable.VALUE_NOT_FOUND) {
            // We know the symbol int value
            // Encode the int
            putByte(LineTcpParser.ENTITY_TYPE_CACHED_TAG);
            putInt(symIndex);
        } else {
            // Symbol value cannot be resolved at this point
            // Encode whole string value into the messagecrossjoinrecord
            closePreparedUtf8CharSequence(LineTcpParser.ENTITY_TYPE_TAG);
        }
    }

    public void addTimestamp(long value) {
        checkCapacity(Long.BYTES + Byte.BYTES);
        putByte(LineTcpParser.ENTITY_TYPE_TIMESTAMP);
        putLong(value);
    }

    public byte readByte() {
        byte value = Unsafe.getUnsafe().getByte(bufPos);
        bufPos += Byte.BYTES;
        return value;
    }

    public char readChar() {
        char value = Unsafe.getUnsafe().getChar(bufPos);
        bufPos += Character.BYTES;
        return value;
    }

    public double readDouble() {
        double value = Unsafe.getUnsafe().getDouble(bufPos);
        bufPos += Double.BYTES;
        return value;
    }

    public float readFloat() {
        float value = Unsafe.getUnsafe().getFloat(bufPos);
        bufPos += Float.BYTES;
        return value;
    }

    public int readInt() {
        int value = Unsafe.getUnsafe().getInt(bufPos);
        bufPos += Integer.BYTES;
        return value;
    }

    public long readLong() {
        long value = Unsafe.getUnsafe().getLong(bufPos);
        bufPos += Long.BYTES;
        return value;
    }

    public short readShort() {
        short value = Unsafe.getUnsafe().getShort(bufPos);
        bufPos += Short.BYTES;
        return value;
    }

    public void readUtf16Chars(FloatingDirectCharSink sink) {
        readUtf16Chars(sink, readInt());
    }

    public void readUtf16Chars(FloatingDirectCharSink sink, int length) {
        long nameLo = bufPos;
        bufPos += 2L * length;
        sink.asCharSequence(nameLo, bufPos);
    }

    public void reset() {
        bufPos = bufLo;
    }

    public void seekToEntities() {
        bufPos = bufLo + Long.BYTES + Integer.BYTES; // timestamp and number of columns
    }

    private static int getSymbolIndex(TableUpdateDetails.ThreadLocalDetails localDetails, int colIndex, CharSequence symValue) {
        if (colIndex >= 0) {
            return localDetails.getSymbolIndex(colIndex, symValue);
        }
        return SymbolTable.VALUE_NOT_FOUND;
    }

    private void checkCapacity(int length) {
        if (bufPos + length >= bufMax) {
            throw CairoException.instance(0).put("queue buffer overflow");
        }
    }

    private CharSequence prepareUtf8CharSequence(
            DirectByteCharSequence value,
            FloatingDirectCharSink tempSink,
            boolean hasNonAsciiChars
    ) {
        int len = 2 * value.length();
        checkCapacity(Byte.BYTES + Integer.BYTES + len);

        this.value = value;
        this.tempSink = tempSink;
        this.hasNonAsciiChars = hasNonAsciiChars;

        long strPos = bufPos + Byte.BYTES + Integer.BYTES; // skip field type and string length
        tempSink.of(strPos, strPos + len);
        return utf8ToUtf16(value, tempSink, hasNonAsciiChars);
    }

    private void closePreparedUtf8CharSequence(byte type) {
        if (!hasNonAsciiChars) {
            tempSink.put(value);
        }
        int length = tempSink.length();
        putByte(type);
        putInt(length);
        bufPos += length * 2L;
    }

    private void putByte(byte value) {
        Unsafe.getUnsafe().putByte(bufPos, value);
        bufPos += Byte.BYTES;
    }

    private void putChar(char value) {
        Unsafe.getUnsafe().putChar(bufPos, value);
        bufPos += Character.BYTES;
    }

    private void putDouble(double value) {
        Unsafe.getUnsafe().putDouble(bufPos, value);
        bufPos += Double.BYTES;
    }

    private void putFloat(float value) {
        Unsafe.getUnsafe().putFloat(bufPos, value);
        bufPos += Float.BYTES;
    }

    private void putInt(int value) {
        Unsafe.getUnsafe().putInt(bufPos, value);
        bufPos += Integer.BYTES;
    }

    private void putLong(long value) {
        Unsafe.getUnsafe().putLong(bufPos, value);
        bufPos += Long.BYTES;
    }

    private void putShort(short value) {
        Unsafe.getUnsafe().putShort(bufPos, value);
        bufPos += Short.BYTES;
    }
}
