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

import io.questdb.std.ObjList;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;

/**
 * Buffers rows for a single table in columnar format.
 * <p>
 * This buffer accumulates row data column by column, allowing efficient
 * encoding to the ILP v4 wire format.
 */
public class IlpV4TableBuffer {

    private final String tableName;
    private final ObjList<ColumnBuffer> columns;
    private final Map<String, Integer> columnNameToIndex;
    private int rowCount;
    private long schemaHash;
    private boolean schemaHashComputed;

    public IlpV4TableBuffer(String tableName) {
        this.tableName = tableName;
        this.columns = new ObjList<>();
        this.columnNameToIndex = new HashMap<>();
        this.rowCount = 0;
        this.schemaHash = 0;
        this.schemaHashComputed = false;
    }

    /**
     * Returns the table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns the number of rows buffered.
     */
    public int getRowCount() {
        return rowCount;
    }

    /**
     * Returns the number of columns.
     */
    public int getColumnCount() {
        return columns.size();
    }

    /**
     * Returns the column at the given index.
     */
    public ColumnBuffer getColumn(int index) {
        return columns.get(index);
    }

    /**
     * Returns the column definitions.
     */
    public IlpV4ColumnDef[] getColumnDefs() {
        IlpV4ColumnDef[] defs = new IlpV4ColumnDef[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            ColumnBuffer col = columns.get(i);
            defs[i] = new IlpV4ColumnDef(col.name, col.type, col.nullable);
        }
        return defs;
    }

    /**
     * Gets or creates a column with the given name and type.
     */
    public ColumnBuffer getOrCreateColumn(String name, byte type, boolean nullable) {
        Integer idx = columnNameToIndex.get(name);
        if (idx != null) {
            ColumnBuffer existing = columns.get(idx);
            if (existing.type != type) {
                throw new IllegalArgumentException(
                        "Column type mismatch for " + name + ": existing=" + existing.type + " new=" + type
                );
            }
            return existing;
        }

        // Create new column
        ColumnBuffer col = new ColumnBuffer(name, type, nullable);
        int index = columns.size();
        columns.add(col);
        columnNameToIndex.put(name, index);
        schemaHashComputed = false;
        return col;
    }

    /**
     * Advances to the next row.
     * <p>
     * This should be called after all column values for the current row have been set.
     */
    public void nextRow() {
        // Ensure all columns have the same row count
        for (int i = 0, n = columns.size(); i < n; i++) {
            ColumnBuffer col = columns.get(i);
            // If column wasn't set for this row, add a null
            while (col.size < rowCount + 1) {
                col.addNull();
            }
        }
        rowCount++;
    }

    /**
     * Returns the schema hash for this table.
     * <p>
     * The hash is computed to match what IlpV4Schema.computeSchemaHash() produces:
     * - Uses wire type codes (with nullable bit)
     * - Hash is over name bytes + type code for each column
     */
    public long getSchemaHash() {
        if (!schemaHashComputed) {
            IlpV4ColumnDef[] defs = getColumnDefs();
            String[] names = new String[defs.length];
            byte[] types = new byte[defs.length];
            for (int i = 0; i < defs.length; i++) {
                names[i] = defs[i].getName();
                // Use wire type code (includes nullable bit) to match IlpV4Schema
                types[i] = defs[i].getWireTypeCode();
            }
            schemaHash = IlpV4SchemaHash.computeSchemaHash(names, types);
            schemaHashComputed = true;
        }
        return schemaHash;
    }

    /**
     * Resets the buffer for reuse.
     */
    public void reset() {
        for (int i = 0, n = columns.size(); i < n; i++) {
            columns.get(i).reset();
        }
        rowCount = 0;
    }

    /**
     * Clears the buffer completely, including column definitions.
     */
    public void clear() {
        columns.clear();
        columnNameToIndex.clear();
        rowCount = 0;
        schemaHash = 0;
        schemaHashComputed = false;
    }

    /**
     * Encodes this table buffer to the given encoder.
     *
     * @param encoder      the encoder to write to
     * @param useSchemaRef whether to use schema reference mode
     * @param useGorilla   whether to use Gorilla encoding for timestamps
     */
    public void encode(IlpV4MessageEncoder encoder, boolean useSchemaRef, boolean useGorilla) {
        IlpV4ColumnDef[] columnDefs = getColumnDefs();

        if (useSchemaRef) {
            encoder.writeTableHeaderWithSchemaRef(tableName, rowCount, getSchemaHash(), columnDefs.length);
        } else {
            encoder.writeTableHeaderWithSchema(tableName, rowCount, columnDefs);
        }

        // Write each column's data
        for (int i = 0; i < columns.size(); i++) {
            ColumnBuffer col = columns.get(i);
            int valueCount = col.getValueCount();

            // Write null bitmap if column is nullable (ALWAYS write it, even if no nulls)
            if (col.nullable) {
                if (col.hasNulls()) {
                    encoder.writeNullBitmap(col.getNullBitmap(), rowCount);
                } else {
                    // Write empty null bitmap (all zeros)
                    encoder.writeNullBitmap(new boolean[rowCount], rowCount);
                }
            }

            // Write column data based on type
            // Note: we pass valueCount (not rowCount) because nulls don't take space in value buffer
            switch (col.type) {
                case TYPE_BOOLEAN:
                    encoder.writeBooleanColumn(col.getBooleanValues(), valueCount);
                    break;
                case TYPE_BYTE:
                    encoder.writeByteColumn(col.getByteValues(), valueCount);
                    break;
                case TYPE_SHORT:
                    encoder.writeShortColumn(col.getShortValues(), valueCount);
                    break;
                case TYPE_INT:
                    encoder.writeIntColumn(col.getIntValues(), valueCount);
                    break;
                case TYPE_LONG:
                    encoder.writeLongColumn(col.getLongValues(), valueCount);
                    break;
                case TYPE_FLOAT:
                    encoder.writeFloatColumn(col.getFloatValues(), valueCount);
                    break;
                case TYPE_DOUBLE:
                    encoder.writeDoubleColumn(col.getDoubleValues(), valueCount);
                    break;
                case TYPE_TIMESTAMP:
                    encoder.writeTimestampColumn(
                            col.getLongValues(),
                            col.nullable ? col.getNullBitmap() : null,
                            rowCount,
                            valueCount,
                            useGorilla
                    );
                    break;
                case TYPE_DATE:
                    encoder.writeLongColumn(col.getLongValues(), valueCount);
                    break;
                case TYPE_STRING:
                case TYPE_VARCHAR:
                    encoder.writeStringColumn(col.getStringValues(), valueCount);
                    break;
                case TYPE_SYMBOL:
                    col.encodeSymbol(encoder, valueCount);
                    break;
                case TYPE_UUID:
                    encoder.writeUuidColumn(col.getUuidHigh(), col.getUuidLow(), valueCount);
                    break;
                case TYPE_LONG256:
                    // Long256 is 4 longs (32 bytes)
                    encodeLong256Column(encoder, col, valueCount);
                    break;
                default:
                    throw new IllegalStateException("Unknown column type: " + col.type);
            }
        }
    }

    private void encodeLong256Column(IlpV4MessageEncoder encoder, ColumnBuffer col, int valueCount) {
        long[][] values = col.getLong256Values();
        for (int i = 0; i < valueCount; i++) {
            // Long256 is big-endian, 4 longs
            for (int j = 0; j < 4; j++) {
                encoder.writeLongBigEndian(values[i][j]);
            }
        }
    }

    /**
     * Column buffer for a single column.
     */
    public static class ColumnBuffer {
        final String name;
        final byte type;
        final boolean nullable;

        private int size;         // Total row count (including nulls)
        private int valueCount;   // Actual stored values (excludes nulls)
        private int capacity;

        // Storage for different types
        private boolean[] booleanValues;
        private byte[] byteValues;
        private short[] shortValues;
        private int[] intValues;
        private long[] longValues;
        private float[] floatValues;
        private double[] doubleValues;
        private String[] stringValues;
        private long[] uuidHigh;
        private long[] uuidLow;
        private long[][] long256Values;

        // Null tracking
        private boolean[] nullBitmap;
        private boolean hasNulls;

        // Symbol specific
        private Map<String, Integer> symbolDict;
        private ObjList<String> symbolList;
        private int[] symbolIndices;

        public ColumnBuffer(String name, byte type, boolean nullable) {
            this.name = name;
            this.type = type;
            this.nullable = nullable;
            this.size = 0;
            this.valueCount = 0;
            this.capacity = 16;
            this.hasNulls = false;

            allocateStorage(type, capacity);
            if (nullable) {
                nullBitmap = new boolean[capacity];
            }
        }

        public String getName() {
            return name;
        }

        public byte getType() {
            return type;
        }

        public int getSize() {
            return size;
        }

        /**
         * Returns the number of actual stored values (excludes nulls).
         */
        public int getValueCount() {
            return valueCount;
        }

        public boolean hasNulls() {
            return hasNulls;
        }

        public boolean[] getNullBitmap() {
            return nullBitmap;
        }

        public boolean[] getBooleanValues() {
            return booleanValues;
        }

        public byte[] getByteValues() {
            return byteValues;
        }

        public short[] getShortValues() {
            return shortValues;
        }

        public int[] getIntValues() {
            return intValues;
        }

        public long[] getLongValues() {
            return longValues;
        }

        public float[] getFloatValues() {
            return floatValues;
        }

        public double[] getDoubleValues() {
            return doubleValues;
        }

        public String[] getStringValues() {
            return stringValues;
        }

        public long[] getUuidHigh() {
            return uuidHigh;
        }

        public long[] getUuidLow() {
            return uuidLow;
        }

        public long[][] getLong256Values() {
            return long256Values;
        }

        public void addBoolean(boolean value) {
            ensureCapacity();
            booleanValues[valueCount++] = value;
            size++;
        }

        public void addByte(byte value) {
            ensureCapacity();
            byteValues[valueCount++] = value;
            size++;
        }

        public void addShort(short value) {
            ensureCapacity();
            shortValues[valueCount++] = value;
            size++;
        }

        public void addInt(int value) {
            ensureCapacity();
            intValues[valueCount++] = value;
            size++;
        }

        public void addLong(long value) {
            ensureCapacity();
            longValues[valueCount++] = value;
            size++;
        }

        public void addFloat(float value) {
            ensureCapacity();
            floatValues[valueCount++] = value;
            size++;
        }

        public void addDouble(double value) {
            ensureCapacity();
            doubleValues[valueCount++] = value;
            size++;
        }

        public void addString(String value) {
            ensureCapacity();
            if (value == null && nullable) {
                markNull(size);
                // Null strings don't take space in the value buffer
                size++;
            } else {
                stringValues[valueCount++] = value;
                size++;
            }
        }

        public void addSymbol(String value) {
            ensureCapacity();
            if (value == null) {
                if (nullable) {
                    markNull(size);
                }
                // Null symbols don't take space in the value buffer
                size++;
            } else {
                Integer idx = symbolDict.get(value);
                if (idx == null) {
                    idx = symbolList.size();
                    symbolDict.put(value, idx);
                    symbolList.add(value);
                }
                symbolIndices[valueCount++] = idx;
                size++;
            }
        }

        public void addUuid(long high, long low) {
            ensureCapacity();
            uuidHigh[valueCount] = high;
            uuidLow[valueCount] = low;
            valueCount++;
            size++;
        }

        public void addLong256(long l0, long l1, long l2, long l3) {
            ensureCapacity();
            if (long256Values[valueCount] == null) {
                long256Values[valueCount] = new long[4];
            }
            long256Values[valueCount][0] = l0;
            long256Values[valueCount][1] = l1;
            long256Values[valueCount][2] = l2;
            long256Values[valueCount][3] = l3;
            valueCount++;
            size++;
        }

        public void addNull() {
            ensureCapacity();
            if (nullable) {
                // For nullable columns, mark null in bitmap but don't store a value
                markNull(size);
                size++;
            } else {
                // For non-nullable columns, we must store a sentinel/default value
                // because no null bitmap will be written
                switch (type) {
                    case TYPE_BOOLEAN:
                        booleanValues[valueCount++] = false;
                        break;
                    case TYPE_BYTE:
                        byteValues[valueCount++] = 0;
                        break;
                    case TYPE_SHORT:
                        shortValues[valueCount++] = 0;
                        break;
                    case TYPE_INT:
                        intValues[valueCount++] = 0;
                        break;
                    case TYPE_LONG:
                    case TYPE_TIMESTAMP:
                    case TYPE_DATE:
                        longValues[valueCount++] = Long.MIN_VALUE;
                        break;
                    case TYPE_FLOAT:
                        floatValues[valueCount++] = Float.NaN;
                        break;
                    case TYPE_DOUBLE:
                        doubleValues[valueCount++] = Double.NaN;
                        break;
                    case TYPE_STRING:
                    case TYPE_VARCHAR:
                        stringValues[valueCount++] = null;
                        break;
                    case TYPE_SYMBOL:
                        symbolIndices[valueCount++] = -1;
                        break;
                    case TYPE_UUID:
                        uuidHigh[valueCount] = Long.MIN_VALUE;
                        uuidLow[valueCount] = Long.MIN_VALUE;
                        valueCount++;
                        break;
                    case TYPE_LONG256:
                        if (long256Values[valueCount] == null) {
                            long256Values[valueCount] = new long[4];
                        }
                        Arrays.fill(long256Values[valueCount], Long.MIN_VALUE);
                        valueCount++;
                        break;
                }
                size++;
            }
        }

        private void markNull(int index) {
            nullBitmap[index] = true;
            hasNulls = true;
        }

        public void reset() {
            size = 0;
            valueCount = 0;
            hasNulls = false;
            if (nullBitmap != null) {
                Arrays.fill(nullBitmap, 0, Math.min(capacity, nullBitmap.length), false);
            }
            if (symbolDict != null) {
                symbolDict.clear();
                symbolList.clear();
            }
        }

        void encodeSymbol(IlpV4MessageEncoder encoder, int count) {
            // Build dictionary array
            String[] dict = new String[symbolList.size()];
            for (int i = 0; i < symbolList.size(); i++) {
                dict[i] = symbolList.get(i);
            }
            encoder.writeSymbolColumn(symbolIndices, dict, count);
        }

        private void ensureCapacity() {
            if (size >= capacity) {
                int newCapacity = capacity * 2;
                growStorage(type, newCapacity);
                if (nullable && nullBitmap != null) {
                    nullBitmap = Arrays.copyOf(nullBitmap, newCapacity);
                }
                capacity = newCapacity;
            }
        }

        private void allocateStorage(byte type, int cap) {
            switch (type) {
                case TYPE_BOOLEAN:
                    booleanValues = new boolean[cap];
                    break;
                case TYPE_BYTE:
                    byteValues = new byte[cap];
                    break;
                case TYPE_SHORT:
                    shortValues = new short[cap];
                    break;
                case TYPE_INT:
                    intValues = new int[cap];
                    break;
                case TYPE_LONG:
                case TYPE_TIMESTAMP:
                case TYPE_DATE:
                    longValues = new long[cap];
                    break;
                case TYPE_FLOAT:
                    floatValues = new float[cap];
                    break;
                case TYPE_DOUBLE:
                    doubleValues = new double[cap];
                    break;
                case TYPE_STRING:
                case TYPE_VARCHAR:
                    stringValues = new String[cap];
                    break;
                case TYPE_SYMBOL:
                    symbolIndices = new int[cap];
                    symbolDict = new HashMap<>();
                    symbolList = new ObjList<>();
                    break;
                case TYPE_UUID:
                    uuidHigh = new long[cap];
                    uuidLow = new long[cap];
                    break;
                case TYPE_LONG256:
                    long256Values = new long[cap][];
                    break;
            }
        }

        private void growStorage(byte type, int newCap) {
            switch (type) {
                case TYPE_BOOLEAN:
                    booleanValues = Arrays.copyOf(booleanValues, newCap);
                    break;
                case TYPE_BYTE:
                    byteValues = Arrays.copyOf(byteValues, newCap);
                    break;
                case TYPE_SHORT:
                    shortValues = Arrays.copyOf(shortValues, newCap);
                    break;
                case TYPE_INT:
                    intValues = Arrays.copyOf(intValues, newCap);
                    break;
                case TYPE_LONG:
                case TYPE_TIMESTAMP:
                case TYPE_DATE:
                    longValues = Arrays.copyOf(longValues, newCap);
                    break;
                case TYPE_FLOAT:
                    floatValues = Arrays.copyOf(floatValues, newCap);
                    break;
                case TYPE_DOUBLE:
                    doubleValues = Arrays.copyOf(doubleValues, newCap);
                    break;
                case TYPE_STRING:
                case TYPE_VARCHAR:
                    stringValues = Arrays.copyOf(stringValues, newCap);
                    break;
                case TYPE_SYMBOL:
                    symbolIndices = Arrays.copyOf(symbolIndices, newCap);
                    break;
                case TYPE_UUID:
                    uuidHigh = Arrays.copyOf(uuidHigh, newCap);
                    uuidLow = Arrays.copyOf(uuidLow, newCap);
                    break;
                case TYPE_LONG256:
                    long256Values = Arrays.copyOf(long256Values, newCap);
                    break;
            }
        }
    }
}
