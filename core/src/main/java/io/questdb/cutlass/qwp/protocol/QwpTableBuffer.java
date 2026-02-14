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

package io.questdb.cutlass.qwp.protocol;

import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.ObjList;

import java.util.Arrays;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Buffers rows for a single table in columnar format.
 * <p>
 * This buffer accumulates row data column by column, allowing efficient
 * encoding to the ILP v4 wire format.
 */
public class QwpTableBuffer {

    private final String tableName;
    private final ObjList<ColumnBuffer> columns;
    private final CharSequenceIntHashMap columnNameToIndex;
    private ColumnBuffer[] fastColumns; // plain array for O(1) sequential access
    private int columnAccessCursor; // tracks expected next column index
    private int rowCount;
    private long schemaHash;
    private boolean schemaHashComputed;
    private QwpColumnDef[] cachedColumnDefs;
    private boolean columnDefsCacheValid;

    public QwpTableBuffer(String tableName) {
        this.tableName = tableName;
        this.columns = new ObjList<>();
        this.columnNameToIndex = new CharSequenceIntHashMap();
        this.rowCount = 0;
        this.schemaHash = 0;
        this.schemaHashComputed = false;
        this.columnDefsCacheValid = false;
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
     * Returns the column definitions (cached for efficiency).
     */
    public QwpColumnDef[] getColumnDefs() {
        if (!columnDefsCacheValid || cachedColumnDefs == null || cachedColumnDefs.length != columns.size()) {
            cachedColumnDefs = new QwpColumnDef[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                ColumnBuffer col = columns.get(i);
                cachedColumnDefs[i] = new QwpColumnDef(col.name, col.type, col.nullable);
            }
            columnDefsCacheValid = true;
        }
        return cachedColumnDefs;
    }

    /**
     * Gets or creates a column with the given name and type.
     * <p>
     * Optimized for the common case where columns are accessed in the same
     * order every row: a sequential cursor avoids hash map lookups entirely.
     */
    public ColumnBuffer getOrCreateColumn(String name, byte type, boolean nullable) {
        // Fast path: predict next column in sequence
        int n = columns.size();
        if (columnAccessCursor < n) {
            ColumnBuffer candidate = fastColumns[columnAccessCursor];
            if (candidate.name.equals(name)) {
                columnAccessCursor++;
                if (candidate.type != type) {
                    throw new IllegalArgumentException(
                            "Column type mismatch for " + name + ": existing=" + candidate.type + " new=" + type
                    );
                }
                return candidate;
            }
        }

        // Slow path: hash map lookup
        int idx = columnNameToIndex.get(name);
        if (idx != CharSequenceIntHashMap.NO_ENTRY_VALUE) {
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
        // Update fast access array
        if (fastColumns == null || index >= fastColumns.length) {
            int newLen = Math.max(8, index + 4);
            ColumnBuffer[] newArr = new ColumnBuffer[newLen];
            if (fastColumns != null) {
                System.arraycopy(fastColumns, 0, newArr, 0, index);
            }
            fastColumns = newArr;
        }
        fastColumns[index] = col;
        schemaHashComputed = false;
        columnDefsCacheValid = false;
        return col;
    }

    /**
     * Advances to the next row.
     * <p>
     * This should be called after all column values for the current row have been set.
     */
    public void nextRow() {
        // Reset sequential access cursor for the next row
        columnAccessCursor = 0;
        // Ensure all columns have the same row count
        for (int i = 0, n = columns.size(); i < n; i++) {
            ColumnBuffer col = fastColumns[i];
            // If column wasn't set for this row, add a null
            while (col.size < rowCount + 1) {
                col.addNull();
            }
        }
        rowCount++;
    }

    /**
     * Cancels the current in-progress row.
     * <p>
     * This removes any column values added since the last {@link #nextRow()} call.
     * If no values have been added for the current row, this is a no-op.
     */
    public void cancelCurrentRow() {
        // Reset sequential access cursor
        columnAccessCursor = 0;
        // Truncate each column back to the committed row count
        for (int i = 0, n = columns.size(); i < n; i++) {
            ColumnBuffer col = fastColumns[i];
            col.truncateTo(rowCount);
        }
    }

    /**
     * Returns the schema hash for this table.
     * <p>
     * The hash is computed to match what QwpSchema.computeSchemaHash() produces:
     * - Uses wire type codes (with nullable bit)
     * - Hash is over name bytes + type code for each column
     */
    public long getSchemaHash() {
        if (!schemaHashComputed) {
            // Compute hash directly from column buffers without intermediate arrays
            schemaHash = QwpSchemaHash.computeSchemaHashDirect(columns);
            schemaHashComputed = true;
        }
        return schemaHash;
    }

    /**
     * Resets the buffer for reuse.
     */
    public void reset() {
        for (int i = 0, n = columns.size(); i < n; i++) {
            fastColumns[i].reset();
        }
        columnAccessCursor = 0;
        rowCount = 0;
    }

    /**
     * Clears the buffer completely, including column definitions.
     */
    public void clear() {
        columns.clear();
        columnNameToIndex.clear();
        fastColumns = null;
        columnAccessCursor = 0;
        rowCount = 0;
        schemaHash = 0;
        schemaHashComputed = false;
        columnDefsCacheValid = false;
        cachedColumnDefs = null;
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
        // Long256 stored as flat array: 4 longs per value (avoids inner array allocation)
        private long[] long256Values;

        // Array storage (double/long arrays - variable length per row)
        // Each row stores: [nDims (1B)][dim1..dimN (4B each)][flattened data]
        // We track per-row metadata separately from the actual data
        private byte[] arrayDims;           // nDims per row
        private int[] arrayShapes;          // Flattened shape data (all dimensions concatenated)
        private int arrayShapeOffset;       // Current write offset in arrayShapes
        private double[] doubleArrayData;   // Flattened double values
        private long[] longArrayData;       // Flattened long values
        private int arrayDataOffset;        // Current write offset in data arrays
        private int arrayRowCapacity;       // Capacity for array row count

        // Null tracking - bit-packed for memory efficiency (1 bit per row vs 8 bits with boolean[])
        private long[] nullBitmapPacked;
        private boolean hasNulls;

        // Symbol specific
        private CharSequenceIntHashMap symbolDict;
        private ObjList<String> symbolList;
        private int[] symbolIndices;

        // Global symbol IDs for delta encoding (parallel to symbolIndices)
        private int[] globalSymbolIds;
        private int maxGlobalSymbolId = -1;

        // Decimal storage
        // All values in a decimal column must share the same scale
        // For Decimal64: single long per value (64-bit unscaled)
        // For Decimal128: two longs per value (128-bit unscaled: high, low)
        // For Decimal256: four longs per value (256-bit unscaled: hh, hl, lh, ll)
        private byte decimalScale = -1;   // Shared scale for column (-1 = not set)
        private long[] decimal64Values;   // Decimal64: one long per value
        private long[] decimal128High;    // Decimal128: high 64 bits
        private long[] decimal128Low;     // Decimal128: low 64 bits
        private long[] decimal256Hh;      // Decimal256: bits 255-192
        private long[] decimal256Hl;      // Decimal256: bits 191-128
        private long[] decimal256Lh;      // Decimal256: bits 127-64
        private long[] decimal256Ll;      // Decimal256: bits 63-0

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
                // Bit-packed: 64 bits per long, so we need (capacity + 63) / 64 longs
                nullBitmapPacked = new long[(capacity + 63) >>> 6];
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

        /**
         * Returns the bit-packed null bitmap.
         * Each long contains 64 bits, bit 0 of long 0 = row 0, bit 1 of long 0 = row 1, etc.
         */
        public long[] getNullBitmapPacked() {
            return nullBitmapPacked;
        }

        /**
         * Returns the null bitmap as boolean array (for backward compatibility).
         * This creates a new array, so prefer getNullBitmapPacked() for efficiency.
         */
        public boolean[] getNullBitmap() {
            if (nullBitmapPacked == null) {
                return null;
            }
            boolean[] result = new boolean[size];
            for (int i = 0; i < size; i++) {
                result[i] = isNull(i);
            }
            return result;
        }

        /**
         * Checks if the row at the given index is null.
         */
        public boolean isNull(int index) {
            if (nullBitmapPacked == null) {
                return false;
            }
            int longIndex = index >>> 6;
            int bitIndex = index & 63;
            return (nullBitmapPacked[longIndex] & (1L << bitIndex)) != 0;
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

        /**
         * Returns Long256 values as flat array (4 longs per value).
         * Use getLong256Value(index, component) for indexed access.
         */
        public long[] getLong256Values() {
            return long256Values;
        }

        /**
         * Returns a component of a Long256 value.
         * @param index value index
         * @param component component 0-3
         */
        public long getLong256Value(int index, int component) {
            return long256Values[index * 4 + component];
        }

        // ==================== Decimal getters ====================

        /**
         * Returns the shared scale for this decimal column.
         * Returns -1 if no values have been added yet.
         */
        public byte getDecimalScale() {
            return decimalScale;
        }

        /**
         * Returns the Decimal64 values (one long per value).
         */
        public long[] getDecimal64Values() {
            return decimal64Values;
        }

        /**
         * Returns the high 64 bits of Decimal128 values.
         */
        public long[] getDecimal128High() {
            return decimal128High;
        }

        /**
         * Returns the low 64 bits of Decimal128 values.
         */
        public long[] getDecimal128Low() {
            return decimal128Low;
        }

        /**
         * Returns bits 255-192 of Decimal256 values.
         */
        public long[] getDecimal256Hh() {
            return decimal256Hh;
        }

        /**
         * Returns bits 191-128 of Decimal256 values.
         */
        public long[] getDecimal256Hl() {
            return decimal256Hl;
        }

        /**
         * Returns bits 127-64 of Decimal256 values.
         */
        public long[] getDecimal256Lh() {
            return decimal256Lh;
        }

        /**
         * Returns bits 63-0 of Decimal256 values.
         */
        public long[] getDecimal256Ll() {
            return decimal256Ll;
        }

        /**
         * Returns the array dimensions per row (nDims for each row).
         */
        public byte[] getArrayDims() {
            return arrayDims;
        }

        /**
         * Returns the flattened array shapes (all dimension lengths concatenated).
         */
        public int[] getArrayShapes() {
            return arrayShapes;
        }

        /**
         * Returns the current write offset in arrayShapes.
         */
        public int getArrayShapeOffset() {
            return arrayShapeOffset;
        }

        /**
         * Returns the flattened double array data.
         */
        public double[] getDoubleArrayData() {
            return doubleArrayData;
        }

        /**
         * Returns the flattened long array data.
         */
        public long[] getLongArrayData() {
            return longArrayData;
        }

        /**
         * Returns the current write offset in the data arrays.
         */
        public int getArrayDataOffset() {
            return arrayDataOffset;
        }

        /**
         * Returns the symbol indices array (one index per value).
         * Each index refers to a position in the symbol dictionary.
         */
        public int[] getSymbolIndices() {
            return symbolIndices;
        }

        /**
         * Returns the symbol dictionary as a String array.
         * Index i in symbolIndices maps to symbolDictionary[i].
         */
        public String[] getSymbolDictionary() {
            if (symbolList == null) {
                return new String[0];
            }
            String[] dict = new String[symbolList.size()];
            for (int i = 0; i < symbolList.size(); i++) {
                dict[i] = symbolList.get(i);
            }
            return dict;
        }

        /**
         * Returns the size of the symbol dictionary.
         */
        public int getSymbolDictionarySize() {
            return symbolList == null ? 0 : symbolList.size();
        }

        /**
         * Returns the global symbol IDs array for delta encoding.
         * Returns null if no global IDs have been stored.
         */
        public int[] getGlobalSymbolIds() {
            return globalSymbolIds;
        }

        /**
         * Returns the maximum global symbol ID used in this column.
         * Returns -1 if no symbols have been added with global IDs.
         */
        public int getMaxGlobalSymbolId() {
            return maxGlobalSymbolId;
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
                int idx = symbolDict.get(value);
                if (idx == CharSequenceIntHashMap.NO_ENTRY_VALUE) {
                    idx = symbolList.size();
                    symbolDict.put(value, idx);
                    symbolList.add(value);
                }
                symbolIndices[valueCount++] = idx;
                size++;
            }
        }

        /**
         * Adds a symbol with both local dictionary and global ID tracking.
         * Used for delta dictionary encoding where global IDs are shared across all columns.
         *
         * @param value    the symbol string
         * @param globalId the global ID from GlobalSymbolDictionary
         */
        public void addSymbolWithGlobalId(String value, int globalId) {
            ensureCapacity();
            if (value == null) {
                if (nullable) {
                    markNull(size);
                }
                size++;
            } else {
                // Add to local dictionary (for backward compatibility with existing encoder)
                int localIdx = symbolDict.get(value);
                if (localIdx == CharSequenceIntHashMap.NO_ENTRY_VALUE) {
                    localIdx = symbolList.size();
                    symbolDict.put(value, localIdx);
                    symbolList.add(value);
                }
                symbolIndices[valueCount] = localIdx;

                // Also store global ID for delta encoding
                if (globalSymbolIds == null) {
                    globalSymbolIds = new int[capacity];
                }
                globalSymbolIds[valueCount] = globalId;

                // Track max global ID for this column
                if (globalId > maxGlobalSymbolId) {
                    maxGlobalSymbolId = globalId;
                }

                valueCount++;
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
            int offset = valueCount * 4;
            long256Values[offset] = l0;
            long256Values[offset + 1] = l1;
            long256Values[offset + 2] = l2;
            long256Values[offset + 3] = l3;
            valueCount++;
            size++;
        }

        // ==================== Decimal methods ====================

        /**
         * Adds a Decimal64 value.
         * All values in a decimal column must share the same scale.
         *
         * @param value the Decimal64 value to add
         * @throws IllegalArgumentException if the scale doesn't match previous values
         */
        public void addDecimal64(Decimal64 value) {
            if (value == null || value.isNull()) {
                addNull();
                return;
            }
            ensureCapacity();
            validateAndSetScale((byte) value.getScale());
            decimal64Values[valueCount++] = value.getValue();
            size++;
        }

        /**
         * Adds a Decimal128 value.
         * All values in a decimal column must share the same scale.
         *
         * @param value the Decimal128 value to add
         * @throws IllegalArgumentException if the scale doesn't match previous values
         */
        public void addDecimal128(Decimal128 value) {
            if (value == null || value.isNull()) {
                addNull();
                return;
            }
            ensureCapacity();
            validateAndSetScale((byte) value.getScale());
            decimal128High[valueCount] = value.getHigh();
            decimal128Low[valueCount] = value.getLow();
            valueCount++;
            size++;
        }

        /**
         * Adds a Decimal256 value.
         * All values in a decimal column must share the same scale.
         *
         * @param value the Decimal256 value to add
         * @throws IllegalArgumentException if the scale doesn't match previous values
         */
        public void addDecimal256(Decimal256 value) {
            if (value == null || value.isNull()) {
                addNull();
                return;
            }
            ensureCapacity();
            validateAndSetScale((byte) value.getScale());
            decimal256Hh[valueCount] = value.getHh();
            decimal256Hl[valueCount] = value.getHl();
            decimal256Lh[valueCount] = value.getLh();
            decimal256Ll[valueCount] = value.getLl();
            valueCount++;
            size++;
        }

        /**
         * Validates that the given scale matches the column's scale.
         * If this is the first value, sets the column scale.
         *
         * @param scale the scale of the value being added
         * @throws IllegalArgumentException if the scale doesn't match
         */
        private void validateAndSetScale(byte scale) {
            if (decimalScale == -1) {
                decimalScale = scale;
            } else if (decimalScale != scale) {
                throw new IllegalArgumentException(
                        "decimal scale mismatch in column '" + name + "': expected " +
                                decimalScale + " but got " + scale +
                                ". All values in a decimal column must have the same scale."
                );
            }
        }

        // ==================== Array methods ====================

        /**
         * Adds a 1D double array.
         */
        public void addDoubleArray(double[] values) {
            if (values == null) {
                addNull();
                return;
            }
            ensureArrayCapacity(1, values.length);
            arrayDims[valueCount] = 1;
            arrayShapes[arrayShapeOffset++] = values.length;
            for (double v : values) {
                doubleArrayData[arrayDataOffset++] = v;
            }
            valueCount++;
            size++;
        }

        /**
         * Adds a 2D double array.
         * @throws IllegalArgumentException if the array is jagged (irregular shape)
         */
        public void addDoubleArray(double[][] values) {
            if (values == null) {
                addNull();
                return;
            }
            int dim0 = values.length;
            int dim1 = dim0 > 0 ? values[0].length : 0;
            // Validate rectangular shape
            for (int i = 1; i < dim0; i++) {
                if (values[i].length != dim1) {
                    throw new IllegalArgumentException("irregular array shape");
                }
            }
            ensureArrayCapacity(2, dim0 * dim1);
            arrayDims[valueCount] = 2;
            arrayShapes[arrayShapeOffset++] = dim0;
            arrayShapes[arrayShapeOffset++] = dim1;
            for (double[] row : values) {
                for (double v : row) {
                    doubleArrayData[arrayDataOffset++] = v;
                }
            }
            valueCount++;
            size++;
        }

        /**
         * Adds a 3D double array.
         * @throws IllegalArgumentException if the array is jagged (irregular shape)
         */
        public void addDoubleArray(double[][][] values) {
            if (values == null) {
                addNull();
                return;
            }
            int dim0 = values.length;
            int dim1 = dim0 > 0 ? values[0].length : 0;
            int dim2 = dim0 > 0 && dim1 > 0 ? values[0][0].length : 0;
            // Validate rectangular shape
            for (int i = 0; i < dim0; i++) {
                if (values[i].length != dim1) {
                    throw new IllegalArgumentException("irregular array shape");
                }
                for (int j = 0; j < dim1; j++) {
                    if (values[i][j].length != dim2) {
                        throw new IllegalArgumentException("irregular array shape");
                    }
                }
            }
            ensureArrayCapacity(3, dim0 * dim1 * dim2);
            arrayDims[valueCount] = 3;
            arrayShapes[arrayShapeOffset++] = dim0;
            arrayShapes[arrayShapeOffset++] = dim1;
            arrayShapes[arrayShapeOffset++] = dim2;
            for (double[][] plane : values) {
                for (double[] row : plane) {
                    for (double v : row) {
                        doubleArrayData[arrayDataOffset++] = v;
                    }
                }
            }
            valueCount++;
            size++;
        }

        /**
         * Adds a 1D long array.
         */
        public void addLongArray(long[] values) {
            if (values == null) {
                addNull();
                return;
            }
            ensureArrayCapacity(1, values.length);
            arrayDims[valueCount] = 1;
            arrayShapes[arrayShapeOffset++] = values.length;
            for (long v : values) {
                longArrayData[arrayDataOffset++] = v;
            }
            valueCount++;
            size++;
        }

        /**
         * Adds a 2D long array.
         * @throws IllegalArgumentException if the array is jagged (irregular shape)
         */
        public void addLongArray(long[][] values) {
            if (values == null) {
                addNull();
                return;
            }
            int dim0 = values.length;
            int dim1 = dim0 > 0 ? values[0].length : 0;
            // Validate rectangular shape
            for (int i = 1; i < dim0; i++) {
                if (values[i].length != dim1) {
                    throw new IllegalArgumentException("irregular array shape");
                }
            }
            ensureArrayCapacity(2, dim0 * dim1);
            arrayDims[valueCount] = 2;
            arrayShapes[arrayShapeOffset++] = dim0;
            arrayShapes[arrayShapeOffset++] = dim1;
            for (long[] row : values) {
                for (long v : row) {
                    longArrayData[arrayDataOffset++] = v;
                }
            }
            valueCount++;
            size++;
        }

        /**
         * Adds a 3D long array.
         * @throws IllegalArgumentException if the array is jagged (irregular shape)
         */
        public void addLongArray(long[][][] values) {
            if (values == null) {
                addNull();
                return;
            }
            int dim0 = values.length;
            int dim1 = dim0 > 0 ? values[0].length : 0;
            int dim2 = dim0 > 0 && dim1 > 0 ? values[0][0].length : 0;
            // Validate rectangular shape
            for (int i = 0; i < dim0; i++) {
                if (values[i].length != dim1) {
                    throw new IllegalArgumentException("irregular array shape");
                }
                for (int j = 0; j < dim1; j++) {
                    if (values[i][j].length != dim2) {
                        throw new IllegalArgumentException("irregular array shape");
                    }
                }
            }
            ensureArrayCapacity(3, dim0 * dim1 * dim2);
            arrayDims[valueCount] = 3;
            arrayShapes[arrayShapeOffset++] = dim0;
            arrayShapes[arrayShapeOffset++] = dim1;
            arrayShapes[arrayShapeOffset++] = dim2;
            for (long[][] plane : values) {
                for (long[] row : plane) {
                    for (long v : row) {
                        longArrayData[arrayDataOffset++] = v;
                    }
                }
            }
            valueCount++;
            size++;
        }

        /**
         * Ensures capacity for array storage.
         * @param nDims number of dimensions for this array
         * @param dataElements number of data elements
         */
        private void ensureArrayCapacity(int nDims, int dataElements) {
            ensureCapacity(); // For row-level capacity (arrayDims uses valueCount)

            // Ensure shape array capacity
            int requiredShapeCapacity = arrayShapeOffset + nDims;
            if (arrayShapes == null) {
                arrayShapes = new int[Math.max(64, requiredShapeCapacity)];
            } else if (requiredShapeCapacity > arrayShapes.length) {
                arrayShapes = Arrays.copyOf(arrayShapes, Math.max(arrayShapes.length * 2, requiredShapeCapacity));
            }

            // Ensure data array capacity
            int requiredDataCapacity = arrayDataOffset + dataElements;
            if (type == TYPE_DOUBLE_ARRAY) {
                if (doubleArrayData == null) {
                    doubleArrayData = new double[Math.max(256, requiredDataCapacity)];
                } else if (requiredDataCapacity > doubleArrayData.length) {
                    doubleArrayData = Arrays.copyOf(doubleArrayData, Math.max(doubleArrayData.length * 2, requiredDataCapacity));
                }
            } else if (type == TYPE_LONG_ARRAY) {
                if (longArrayData == null) {
                    longArrayData = new long[Math.max(256, requiredDataCapacity)];
                } else if (requiredDataCapacity > longArrayData.length) {
                    longArrayData = Arrays.copyOf(longArrayData, Math.max(longArrayData.length * 2, requiredDataCapacity));
                }
            }
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
                    case TYPE_CHAR:
                        shortValues[valueCount++] = 0;
                        break;
                    case TYPE_INT:
                        intValues[valueCount++] = 0;
                        break;
                    case TYPE_LONG:
                    case TYPE_TIMESTAMP:
                    case TYPE_TIMESTAMP_NANOS:
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
                        int offset = valueCount * 4;
                        long256Values[offset] = Long.MIN_VALUE;
                        long256Values[offset + 1] = Long.MIN_VALUE;
                        long256Values[offset + 2] = Long.MIN_VALUE;
                        long256Values[offset + 3] = Long.MIN_VALUE;
                        valueCount++;
                        break;
                    case TYPE_DECIMAL64:
                        decimal64Values[valueCount++] = Decimals.DECIMAL64_NULL;
                        break;
                    case TYPE_DECIMAL128:
                        decimal128High[valueCount] = Decimals.DECIMAL128_HI_NULL;
                        decimal128Low[valueCount] = Decimals.DECIMAL128_LO_NULL;
                        valueCount++;
                        break;
                    case TYPE_DECIMAL256:
                        decimal256Hh[valueCount] = Decimals.DECIMAL256_HH_NULL;
                        decimal256Hl[valueCount] = Decimals.DECIMAL256_HL_NULL;
                        decimal256Lh[valueCount] = Decimals.DECIMAL256_LH_NULL;
                        decimal256Ll[valueCount] = Decimals.DECIMAL256_LL_NULL;
                        valueCount++;
                        break;
                }
                size++;
            }
        }

        private void markNull(int index) {
            int longIndex = index >>> 6;
            int bitIndex = index & 63;
            nullBitmapPacked[longIndex] |= (1L << bitIndex);
            hasNulls = true;
        }

        public void reset() {
            size = 0;
            valueCount = 0;
            hasNulls = false;
            if (nullBitmapPacked != null) {
                Arrays.fill(nullBitmapPacked, 0L);
            }
            if (symbolDict != null) {
                symbolDict.clear();
                symbolList.clear();
            }
            // Reset global symbol tracking
            maxGlobalSymbolId = -1;
            // Reset array tracking
            arrayShapeOffset = 0;
            arrayDataOffset = 0;
            // Reset decimal scale (will be set by first non-null value)
            decimalScale = -1;
        }

        /**
         * Truncates the column to the specified size.
         * This is used to cancel uncommitted row values.
         *
         * @param newSize the target size (number of rows)
         */
        public void truncateTo(int newSize) {
            if (newSize >= size) {
                return; // Nothing to truncate
            }

            // Count non-null values up to newSize
            int newValueCount = 0;
            if (nullable && nullBitmapPacked != null) {
                for (int i = 0; i < newSize; i++) {
                    int longIndex = i >>> 6;
                    int bitIndex = i & 63;
                    if ((nullBitmapPacked[longIndex] & (1L << bitIndex)) == 0) {
                        newValueCount++;
                    }
                }
                // Clear null bits for truncated rows
                for (int i = newSize; i < size; i++) {
                    int longIndex = i >>> 6;
                    int bitIndex = i & 63;
                    nullBitmapPacked[longIndex] &= ~(1L << bitIndex);
                }
                // Recompute hasNulls
                hasNulls = false;
                for (int i = 0; i < newSize && !hasNulls; i++) {
                    int longIndex = i >>> 6;
                    int bitIndex = i & 63;
                    if ((nullBitmapPacked[longIndex] & (1L << bitIndex)) != 0) {
                        hasNulls = true;
                    }
                }
            } else {
                newValueCount = newSize;
            }

            size = newSize;
            valueCount = newValueCount;
        }

        private void ensureCapacity() {
            if (size >= capacity) {
                int newCapacity = capacity * 2;
                growStorage(type, newCapacity);
                if (nullable && nullBitmapPacked != null) {
                    int newLongCount = (newCapacity + 63) >>> 6;
                    nullBitmapPacked = Arrays.copyOf(nullBitmapPacked, newLongCount);
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
                case TYPE_CHAR:
                    shortValues = new short[cap];
                    break;
                case TYPE_INT:
                    intValues = new int[cap];
                    break;
                case TYPE_LONG:
                case TYPE_TIMESTAMP:
                case TYPE_TIMESTAMP_NANOS:
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
                    symbolDict = new CharSequenceIntHashMap();
                    symbolList = new ObjList<>();
                    break;
                case TYPE_UUID:
                    uuidHigh = new long[cap];
                    uuidLow = new long[cap];
                    break;
                case TYPE_LONG256:
                    // Flat array: 4 longs per value
                    long256Values = new long[cap * 4];
                    break;
                case TYPE_DOUBLE_ARRAY:
                case TYPE_LONG_ARRAY:
                    // Array types: allocate per-row tracking
                    // Shape and data arrays are grown dynamically in ensureArrayCapacity()
                    arrayDims = new byte[cap];
                    arrayRowCapacity = cap;
                    break;
                case TYPE_DECIMAL64:
                    decimal64Values = new long[cap];
                    break;
                case TYPE_DECIMAL128:
                    decimal128High = new long[cap];
                    decimal128Low = new long[cap];
                    break;
                case TYPE_DECIMAL256:
                    decimal256Hh = new long[cap];
                    decimal256Hl = new long[cap];
                    decimal256Lh = new long[cap];
                    decimal256Ll = new long[cap];
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
                case TYPE_CHAR:
                    shortValues = Arrays.copyOf(shortValues, newCap);
                    break;
                case TYPE_INT:
                    intValues = Arrays.copyOf(intValues, newCap);
                    break;
                case TYPE_LONG:
                case TYPE_TIMESTAMP:
                case TYPE_TIMESTAMP_NANOS:
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
                    if (globalSymbolIds != null) {
                        globalSymbolIds = Arrays.copyOf(globalSymbolIds, newCap);
                    }
                    break;
                case TYPE_UUID:
                    uuidHigh = Arrays.copyOf(uuidHigh, newCap);
                    uuidLow = Arrays.copyOf(uuidLow, newCap);
                    break;
                case TYPE_LONG256:
                    // Flat array: 4 longs per value
                    long256Values = Arrays.copyOf(long256Values, newCap * 4);
                    break;
                case TYPE_DOUBLE_ARRAY:
                case TYPE_LONG_ARRAY:
                    // Array types: grow per-row tracking
                    arrayDims = Arrays.copyOf(arrayDims, newCap);
                    arrayRowCapacity = newCap;
                    // Note: shapes and data arrays are grown in ensureArrayCapacity()
                    break;
                case TYPE_DECIMAL64:
                    decimal64Values = Arrays.copyOf(decimal64Values, newCap);
                    break;
                case TYPE_DECIMAL128:
                    decimal128High = Arrays.copyOf(decimal128High, newCap);
                    decimal128Low = Arrays.copyOf(decimal128Low, newCap);
                    break;
                case TYPE_DECIMAL256:
                    decimal256Hh = Arrays.copyOf(decimal256Hh, newCap);
                    decimal256Hl = Arrays.copyOf(decimal256Hl, newCap);
                    decimal256Lh = Arrays.copyOf(decimal256Lh, newCap);
                    decimal256Ll = Arrays.copyOf(decimal256Ll, newCap);
                    break;
            }
        }
    }

}
