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

package io.questdb.cutlass.line.tcp.v4;

import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8Sequence;

/**
 * Represents a decoded column from an ILP v4 table block.
 * <p>
 * This class provides type-safe access to column values by row index.
 * The column stores decoded values in arrays for efficient access.
 */
public class IlpV4DecodedColumn {

    private final String name;
    private final int type;
    private final boolean nullable;
    private final int rowCount;

    // Storage for different types
    private boolean[] nulls;
    private long[] longValues;      // BYTE, SHORT, INT, LONG, DATE, TIMESTAMP
    private double[] doubleValues;  // FLOAT, DOUBLE
    private boolean[] boolValues;   // BOOLEAN
    private long[] uuidHi;          // UUID high bits
    private long[] uuidLo;          // UUID low bits
    private long[][] long256Values; // LONG256 (4 longs per value)
    private String[] stringValues;  // STRING, VARCHAR, SYMBOL
    private int[] symbolIndices;    // SYMBOL dictionary indices
    private long[] geoHashValues;   // GEOHASH
    private int geoHashPrecision;   // GEOHASH precision in bits

    public IlpV4DecodedColumn(String name, int type, boolean nullable, int rowCount) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.rowCount = rowCount;
        initStorage();
    }

    private void initStorage() {
        if (nullable) {
            nulls = new boolean[rowCount];
        }

        switch (type) {
            case IlpV4Constants.TYPE_BOOLEAN:
                boolValues = new boolean[rowCount];
                break;
            case IlpV4Constants.TYPE_BYTE:
            case IlpV4Constants.TYPE_SHORT:
            case IlpV4Constants.TYPE_INT:
            case IlpV4Constants.TYPE_LONG:
            case IlpV4Constants.TYPE_DATE:
            case IlpV4Constants.TYPE_TIMESTAMP:
                longValues = new long[rowCount];
                break;
            case IlpV4Constants.TYPE_FLOAT:
            case IlpV4Constants.TYPE_DOUBLE:
                doubleValues = new double[rowCount];
                break;
            case IlpV4Constants.TYPE_UUID:
                uuidHi = new long[rowCount];
                uuidLo = new long[rowCount];
                break;
            case IlpV4Constants.TYPE_LONG256:
                long256Values = new long[rowCount][4];
                break;
            case IlpV4Constants.TYPE_STRING:
            case IlpV4Constants.TYPE_VARCHAR:
                stringValues = new String[rowCount];
                break;
            case IlpV4Constants.TYPE_SYMBOL:
                stringValues = new String[rowCount];
                symbolIndices = new int[rowCount];
                break;
            case IlpV4Constants.TYPE_GEOHASH:
                geoHashValues = new long[rowCount];
                break;
        }
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }

    public int getRowCount() {
        return rowCount;
    }

    public boolean isNull(int rowIndex) {
        return nullable && nulls[rowIndex];
    }

    public void setNull(int rowIndex) {
        if (nullable) {
            nulls[rowIndex] = true;
        }
    }

    // Boolean access
    public boolean getBoolean(int rowIndex) {
        return boolValues[rowIndex];
    }

    public void setBoolean(int rowIndex, boolean value) {
        boolValues[rowIndex] = value;
    }

    // Integer types access
    public byte getByte(int rowIndex) {
        return (byte) longValues[rowIndex];
    }

    public void setByte(int rowIndex, byte value) {
        longValues[rowIndex] = value;
    }

    public short getShort(int rowIndex) {
        return (short) longValues[rowIndex];
    }

    public void setShort(int rowIndex, short value) {
        longValues[rowIndex] = value;
    }

    public int getInt(int rowIndex) {
        return (int) longValues[rowIndex];
    }

    public void setInt(int rowIndex, int value) {
        longValues[rowIndex] = value;
    }

    public long getLong(int rowIndex) {
        return longValues[rowIndex];
    }

    public void setLong(int rowIndex, long value) {
        longValues[rowIndex] = value;
    }

    // Floating point access
    public float getFloat(int rowIndex) {
        return (float) doubleValues[rowIndex];
    }

    public void setFloat(int rowIndex, float value) {
        doubleValues[rowIndex] = value;
    }

    public double getDouble(int rowIndex) {
        return doubleValues[rowIndex];
    }

    public void setDouble(int rowIndex, double value) {
        doubleValues[rowIndex] = value;
    }

    // Date/Timestamp access (stored as long)
    public long getDate(int rowIndex) {
        return longValues[rowIndex];
    }

    public void setDate(int rowIndex, long value) {
        longValues[rowIndex] = value;
    }

    public long getTimestamp(int rowIndex) {
        return longValues[rowIndex];
    }

    public void setTimestamp(int rowIndex, long value) {
        longValues[rowIndex] = value;
    }

    // UUID access
    public long getUuidHi(int rowIndex) {
        return uuidHi[rowIndex];
    }

    public long getUuidLo(int rowIndex) {
        return uuidLo[rowIndex];
    }

    public void setUuid(int rowIndex, long hi, long lo) {
        uuidHi[rowIndex] = hi;
        uuidLo[rowIndex] = lo;
    }

    // Long256 access
    public long getLong256_0(int rowIndex) {
        return long256Values[rowIndex][0];
    }

    public long getLong256_1(int rowIndex) {
        return long256Values[rowIndex][1];
    }

    public long getLong256_2(int rowIndex) {
        return long256Values[rowIndex][2];
    }

    public long getLong256_3(int rowIndex) {
        return long256Values[rowIndex][3];
    }

    public void setLong256(int rowIndex, long l0, long l1, long l2, long l3) {
        long256Values[rowIndex][0] = l0;
        long256Values[rowIndex][1] = l1;
        long256Values[rowIndex][2] = l2;
        long256Values[rowIndex][3] = l3;
    }

    // String access
    public String getString(int rowIndex) {
        return stringValues[rowIndex];
    }

    public void setString(int rowIndex, String value) {
        stringValues[rowIndex] = value;
    }

    // Symbol access
    public String getSymbol(int rowIndex) {
        return stringValues[rowIndex];
    }

    public int getSymbolIndex(int rowIndex) {
        return symbolIndices[rowIndex];
    }

    public void setSymbol(int rowIndex, int dictIndex, String value) {
        symbolIndices[rowIndex] = dictIndex;
        stringValues[rowIndex] = value;
    }

    // GeoHash access
    public long getGeoHash(int rowIndex) {
        return geoHashValues[rowIndex];
    }

    public int getGeoHashPrecision() {
        return geoHashPrecision;
    }

    public void setGeoHash(int rowIndex, long value, int precision) {
        geoHashValues[rowIndex] = value;
        geoHashPrecision = precision;
    }

    /**
     * Creates a ColumnSink that writes to this decoded column.
     */
    public IlpV4ColumnDecoder.ColumnSink createSink() {
        return new DecodedColumnSink();
    }

    private class DecodedColumnSink implements IlpV4ColumnDecoder.ColumnSink,
            IlpV4StringDecoder.StringColumnSink,
            IlpV4SymbolDecoder.SymbolColumnSink,
            IlpV4GeoHashDecoder.GeoHashColumnSink {

        @Override
        public void putNull(int rowIndex) {
            setNull(rowIndex);
        }

        @Override
        public void putByte(int rowIndex, byte value) {
            setByte(rowIndex, value);
        }

        @Override
        public void putShort(int rowIndex, short value) {
            setShort(rowIndex, value);
        }

        @Override
        public void putInt(int rowIndex, int value) {
            setInt(rowIndex, value);
        }

        @Override
        public void putLong(int rowIndex, long value) {
            setLong(rowIndex, value);
        }

        @Override
        public void putFloat(int rowIndex, float value) {
            setFloat(rowIndex, value);
        }

        @Override
        public void putDouble(int rowIndex, double value) {
            setDouble(rowIndex, value);
        }

        @Override
        public void putBoolean(int rowIndex, boolean value) {
            setBoolean(rowIndex, value);
        }

        @Override
        public void putUuid(int rowIndex, long hi, long lo) {
            setUuid(rowIndex, hi, lo);
        }

        @Override
        public void putLong256(int rowIndex, long l0, long l1, long l2, long l3) {
            setLong256(rowIndex, l0, l1, l2, l3);
        }

        @Override
        public void putString(int rowIndex, long address, int length) {
            // Read UTF-8 string from memory
            if (length == 0) {
                setString(rowIndex, "");
            } else {
                byte[] bytes = new byte[length];
                for (int i = 0; i < length; i++) {
                    bytes[i] = io.questdb.std.Unsafe.getUnsafe().getByte(address + i);
                }
                setString(rowIndex, new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
            }
        }

        @Override
        public void putSymbol(int rowIndex, int dictIndex, String symbolValue) {
            setSymbol(rowIndex, dictIndex, symbolValue);
        }

        @Override
        public void putGeoHash(int rowIndex, long value, int precision) {
            setGeoHash(rowIndex, value, precision);
        }
    }
}
