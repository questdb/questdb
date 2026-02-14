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

import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sequence;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Streaming cursor over a decoded ILP v4 table block.
 * <p>
 * Provides zero-allocation row-by-row iteration through the table data.
 * Column cursors are managed internally and reused across table blocks.
 * <p>
 * <b>Usage:</b>
 * <pre>
 * while (cursor.hasNextRow()) {
 *     cursor.nextRow();
 *     for (int col = 0; col < cursor.getColumnCount(); col++) {
 *         if (cursor.isColumnNull(col)) {
 *             continue;  // use isColumnNull() to avoid megamorphic calls
 *         }
 *         QwpColumnCursor colCursor = cursor.getColumn(col);
 *         // read value based on type
 *     }
 * }
 * </pre>
 */
public class QwpTableBlockCursor implements Mutable {

    private final QwpTableHeader tableHeader = new QwpTableHeader();
    private final QwpSchema.ParseResult parseResult = new QwpSchema.ParseResult();

    // Column cursors (reused across table blocks)
    private final ObjList<QwpColumnCursor> columnCursors = new ObjList<>();

    // Cached null flags per column for current row (avoids megamorphic isNull() calls)
    private boolean[] columnNullFlags = new boolean[16];

    // Type-bucketed column indices for monomorphic advanceRow() calls
    private int[] booleanColumnIndices = new int[16];
    private int[] fixedWidthColumnIndices = new int[16];
    private int[] timestampColumnIndices = new int[16];
    private int[] stringColumnIndices = new int[16];
    private int[] symbolColumnIndices = new int[16];
    private int[] geoHashColumnIndices = new int[16];
    private int[] arrayColumnIndices = new int[16];
    private int[] decimalColumnIndices = new int[16];
    private int booleanColumnCount;
    private int fixedWidthColumnCount;
    private int timestampColumnCount;
    private int stringColumnCount;
    private int symbolColumnCount;
    private int geoHashColumnCount;
    private int arrayColumnCount;
    private int decimalColumnCount;

    // Schema cache reference
    private QwpSchemaCache schemaCache;

    // Delta symbol dictionary support
    private ObjList<String> connectionSymbolDict;
    private boolean deltaSymbolDictEnabled;

    // Table state
    private int rowCount;
    private int columnCount;
    private int currentRow;
    private boolean gorillaEnabled;

    // Column definitions from schema
    private QwpColumnDef[] columnDefs;

    // Wire position tracking
    private int bytesConsumed;

    /**
     * Initializes this cursor for the given table block data with delta symbol dictionary support.
     *
     * @param dataAddress            address of table block data
     * @param dataLength             available bytes
     * @param gorillaEnabled         whether Gorilla encoding is enabled
     * @param schemaCache            schema cache for reference mode (may be null)
     * @param connectionSymbolDict   connection-level symbol dictionary (may be null)
     * @param deltaSymbolDictEnabled whether delta mode is enabled
     * @return bytes consumed from dataAddress
     * @throws QwpParseException if parsing fails
     */
    public int of(long dataAddress, int dataLength, boolean gorillaEnabled, QwpSchemaCache schemaCache,
                  ObjList<String> connectionSymbolDict, boolean deltaSymbolDictEnabled)
            throws QwpParseException {
        this.gorillaEnabled = gorillaEnabled;
        this.schemaCache = schemaCache;
        this.connectionSymbolDict = connectionSymbolDict;
        this.deltaSymbolDictEnabled = deltaSymbolDictEnabled;

        int offset = 0;
        long limit = dataAddress + dataLength;

        // Parse table header
        tableHeader.parse(dataAddress, dataLength);
        offset = tableHeader.getBytesConsumed();

        this.rowCount = (int) tableHeader.getRowCount();
        this.columnCount = tableHeader.getColumnCount();

        // Parse schema section (zero-alloc: reuses parseResult)
        QwpSchema.parse(dataAddress + offset, dataLength - offset, columnCount, parseResult);
        offset += parseResult.bytesConsumed;

        QwpSchema schema;
        if (!parseResult.isReference) {
            schema = parseResult.schema;
            // Cache the schema if caching is enabled
            if (schemaCache != null) {
                schemaCache.put(tableHeader.getTableName(), schema);
            }
        } else {
            // Schema reference mode - look up in cache
            if (schemaCache == null) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.SCHEMA_NOT_FOUND,
                        "schema reference mode requires schema cache"
                );
            }
            schema = schemaCache.get(tableHeader.getTableNameUtf8(), parseResult.schemaHash);
            if (schema == null) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.SCHEMA_NOT_FOUND,
                        "schema not found in cache for table: " + tableHeader.getTableName()
                );
            }
        }

        this.columnDefs = schema.getColumns();

        // Initialize column cursors and type buckets
        ensureColumnCursorCapacity(columnCount);
        booleanColumnCount = 0;
        fixedWidthColumnCount = 0;
        timestampColumnCount = 0;
        stringColumnCount = 0;
        symbolColumnCount = 0;
        geoHashColumnCount = 0;
        arrayColumnCount = 0;
        decimalColumnCount = 0;
        for (int i = 0; i < columnCount; i++) {
            QwpColumnDef colDef = columnDefs[i];
            byte typeCode = colDef.getTypeCode();
            boolean nullable = colDef.isNullable();

            int consumed = initializeColumnCursor(
                    i, dataAddress + offset, dataLength - offset, rowCount,
                    typeCode, nullable, 0, 0  // Column mapping uses index-based lookup
            );
            offset += consumed;
        }

        this.bytesConsumed = offset;
        this.currentRow = -1;

        return offset;
    }

    private void ensureColumnCursorCapacity(int capacity) {
        while (columnCursors.size() < capacity) {
            // Pre-allocate with null, will be replaced with correct type
            columnCursors.add(null);
        }
        if (columnNullFlags.length < capacity) {
            columnNullFlags = new boolean[capacity];
            booleanColumnIndices = new int[capacity];
            fixedWidthColumnIndices = new int[capacity];
            timestampColumnIndices = new int[capacity];
            stringColumnIndices = new int[capacity];
            symbolColumnIndices = new int[capacity];
            geoHashColumnIndices = new int[capacity];
            arrayColumnIndices = new int[capacity];
            decimalColumnIndices = new int[capacity];
        }
    }

    private int initializeColumnCursor(int colIndex, long dataAddress, int dataLength, int rowCount,
                                        byte typeCode, boolean nullable, long nameAddress, int nameLength)
            throws QwpParseException {
        int type = typeCode & TYPE_MASK;

        QwpColumnCursor cursor = columnCursors.getQuick(colIndex);

        switch (type) {
            case TYPE_BOOLEAN:
                QwpBooleanColumnCursor boolCursor;
                if (cursor instanceof QwpBooleanColumnCursor) {
                    boolCursor = (QwpBooleanColumnCursor) cursor;
                } else {
                    boolCursor = new QwpBooleanColumnCursor();
                    columnCursors.setQuick(colIndex, boolCursor);
                }
                booleanColumnIndices[booleanColumnCount++] = colIndex;
                return boolCursor.of(dataAddress, dataLength, rowCount, nullable, nameAddress, nameLength);

            case TYPE_BYTE:
            case TYPE_SHORT:
            case TYPE_CHAR:
            case TYPE_INT:
            case TYPE_LONG:
            case TYPE_FLOAT:
            case TYPE_DOUBLE:
            case TYPE_DATE:
            case TYPE_UUID:
            case TYPE_LONG256:
                QwpFixedWidthColumnCursor fixedCursor;
                if (cursor instanceof QwpFixedWidthColumnCursor) {
                    fixedCursor = (QwpFixedWidthColumnCursor) cursor;
                } else {
                    fixedCursor = new QwpFixedWidthColumnCursor();
                    columnCursors.setQuick(colIndex, fixedCursor);
                }
                fixedWidthColumnIndices[fixedWidthColumnCount++] = colIndex;
                return fixedCursor.of(dataAddress, dataLength, rowCount, typeCode, nullable, nameAddress, nameLength);

            case TYPE_TIMESTAMP:
            case TYPE_TIMESTAMP_NANOS:
                QwpTimestampColumnCursor tsCursor;
                if (cursor instanceof QwpTimestampColumnCursor) {
                    tsCursor = (QwpTimestampColumnCursor) cursor;
                } else {
                    tsCursor = new QwpTimestampColumnCursor();
                    columnCursors.setQuick(colIndex, tsCursor);
                }
                timestampColumnIndices[timestampColumnCount++] = colIndex;
                return tsCursor.of(dataAddress, dataLength, rowCount, typeCode, nullable, nameAddress, nameLength, gorillaEnabled);

            case TYPE_STRING:
            case TYPE_VARCHAR:
                QwpStringColumnCursor strCursor;
                if (cursor instanceof QwpStringColumnCursor) {
                    strCursor = (QwpStringColumnCursor) cursor;
                } else {
                    strCursor = new QwpStringColumnCursor();
                    columnCursors.setQuick(colIndex, strCursor);
                }
                stringColumnIndices[stringColumnCount++] = colIndex;
                return strCursor.of(dataAddress, dataLength, rowCount, typeCode, nullable, nameAddress, nameLength);

            case TYPE_SYMBOL:
                QwpSymbolColumnCursor symCursor;
                if (cursor instanceof QwpSymbolColumnCursor) {
                    symCursor = (QwpSymbolColumnCursor) cursor;
                } else {
                    symCursor = new QwpSymbolColumnCursor();
                    columnCursors.setQuick(colIndex, symCursor);
                }
                symbolColumnIndices[symbolColumnCount++] = colIndex;
                // In delta mode, pass connection dictionary; otherwise null (per-column dict)
                ObjList<String> dictForSymbol = deltaSymbolDictEnabled ? connectionSymbolDict : null;
                return symCursor.of(dataAddress, dataLength, rowCount, nullable, nameAddress, nameLength, dictForSymbol);

            case TYPE_GEOHASH:
                QwpGeoHashColumnCursor geoCursor;
                if (cursor instanceof QwpGeoHashColumnCursor) {
                    geoCursor = (QwpGeoHashColumnCursor) cursor;
                } else {
                    geoCursor = new QwpGeoHashColumnCursor();
                    columnCursors.setQuick(colIndex, geoCursor);
                }
                geoHashColumnIndices[geoHashColumnCount++] = colIndex;
                return geoCursor.of(dataAddress, dataLength, rowCount, nullable, nameAddress, nameLength);

            case TYPE_DOUBLE_ARRAY:
            case TYPE_LONG_ARRAY:
                QwpArrayColumnCursor arrCursor;
                if (cursor instanceof QwpArrayColumnCursor) {
                    arrCursor = (QwpArrayColumnCursor) cursor;
                } else {
                    arrCursor = new QwpArrayColumnCursor();
                    columnCursors.setQuick(colIndex, arrCursor);
                }
                arrayColumnIndices[arrayColumnCount++] = colIndex;
                return arrCursor.of(dataAddress, dataLength, rowCount, typeCode, nullable, nameAddress, nameLength);

            case TYPE_DECIMAL64:
            case TYPE_DECIMAL128:
            case TYPE_DECIMAL256:
                QwpDecimalColumnCursor decCursor;
                if (cursor instanceof QwpDecimalColumnCursor) {
                    decCursor = (QwpDecimalColumnCursor) cursor;
                } else {
                    decCursor = new QwpDecimalColumnCursor();
                    columnCursors.setQuick(colIndex, decCursor);
                }
                decimalColumnIndices[decimalColumnCount++] = colIndex;
                return decCursor.of(dataAddress, dataLength, rowCount, typeCode, nullable, nameAddress, nameLength);

            default:
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INVALID_COLUMN_TYPE,
                        "unknown column type: " + type
                );
        }
    }

    /**
     * Returns the table name as a UTF-8 sequence (zero allocation).
     * <p>
     * The returned sequence points directly to wire memory and is valid
     * until the cursor is reused for another table block.
     */
    public DirectUtf8Sequence getTableNameUtf8() {
        return tableHeader.getTableNameUtf8();
    }

    /**
     * Returns the table name as a String.
     * <p>
     * This allocates on first call per table block. Prefer {@link #getTableNameUtf8()}
     * for zero-allocation access on hot paths.
     */
    public String getTableName() {
        return tableHeader.getTableName();
    }

    /**
     * Returns the number of rows in this table block.
     */
    public int getRowCount() {
        return rowCount;
    }

    /**
     * Returns the number of columns.
     */
    public int getColumnCount() {
        return columnCount;
    }

    /**
     * Returns the column definition at the specified index.
     */
    public QwpColumnDef getColumnDef(int index) {
        return columnDefs[index];
    }

    /**
     * Returns the column definitions array for schema access.
     * Note: Returns internal array directly (no copy) for zero-allocation.
     */
    public QwpColumnDef[] getSchema() {
        return columnDefs;
    }

    /**
     * Returns whether there are more rows to iterate.
     */
    public boolean hasNextRow() {
        return currentRow + 1 < rowCount;
    }

    /**
     * Advances all column cursors to the next row.
     * <p>
     * Caches null flags for each column to avoid megamorphic {@code isNull()} calls
     * in consumer loops. Use {@link #isColumnNull(int)} to check null status.
     *
     * @throws QwpParseException if parsing fails during row advance
     */
    public void nextRow() throws QwpParseException {
        currentRow++;
        // Type-bucketed iteration for monomorphic advanceRow() calls
        for (int i = 0; i < booleanColumnCount; i++) {
            int col = booleanColumnIndices[i];
            columnNullFlags[col] = getBooleanColumn(col).advanceRow();
        }
        for (int i = 0; i < fixedWidthColumnCount; i++) {
            int col = fixedWidthColumnIndices[i];
            columnNullFlags[col] = getFixedWidthColumn(col).advanceRow();
        }
        for (int i = 0; i < timestampColumnCount; i++) {
            int col = timestampColumnIndices[i];
            columnNullFlags[col] = getTimestampColumn(col).advanceRow();
        }
        for (int i = 0; i < stringColumnCount; i++) {
            int col = stringColumnIndices[i];
            columnNullFlags[col] = getStringColumn(col).advanceRow();
        }
        for (int i = 0; i < symbolColumnCount; i++) {
            int col = symbolColumnIndices[i];
            columnNullFlags[col] = getSymbolColumn(col).advanceRow();
        }
        for (int i = 0; i < geoHashColumnCount; i++) {
            int col = geoHashColumnIndices[i];
            columnNullFlags[col] = getGeoHashColumn(col).advanceRow();
        }
        for (int i = 0; i < arrayColumnCount; i++) {
            int col = arrayColumnIndices[i];
            columnNullFlags[col] = getArrayColumn(col).advanceRow();
        }
        for (int i = 0; i < decimalColumnCount; i++) {
            int col = decimalColumnIndices[i];
            columnNullFlags[col] = getDecimalColumn(col).advanceRow();
        }
    }

    /**
     * Returns whether the column at the specified index is null for the current row.
     * <p>
     * This method provides monomorphic access to null status, avoiding the megamorphic
     * virtual call overhead of {@code getColumn(i).isNull()}.
     *
     * @param index column index
     * @return true if the column value is null for the current row
     */
    public boolean isColumnNull(int index) {
        return columnNullFlags[index];
    }

    /**
     * Returns the current row index (0-based).
     */
    public int getCurrentRow() {
        return currentRow;
    }

    /**
     * Returns the column cursor at the specified index.
     * <p>
     * The returned cursor is positioned at the current row after {@link #nextRow()}.
     */
    public final QwpColumnCursor getColumn(int index) {
        return columnCursors.getQuick(index);
    }

    /**
     * Returns the column cursor cast to fixed-width type.
     */
    public QwpFixedWidthColumnCursor getFixedWidthColumn(int index) {
        return (QwpFixedWidthColumnCursor) columnCursors.getQuick(index);
    }

    /**
     * Returns the column cursor cast to boolean type.
     */
    public QwpBooleanColumnCursor getBooleanColumn(int index) {
        return (QwpBooleanColumnCursor) columnCursors.getQuick(index);
    }

    /**
     * Returns the column cursor cast to string type.
     */
    public QwpStringColumnCursor getStringColumn(int index) {
        return (QwpStringColumnCursor) columnCursors.getQuick(index);
    }

    /**
     * Returns the column cursor cast to symbol type.
     */
    public QwpSymbolColumnCursor getSymbolColumn(int index) {
        return (QwpSymbolColumnCursor) columnCursors.getQuick(index);
    }

    /**
     * Returns the column cursor cast to timestamp type.
     */
    public QwpTimestampColumnCursor getTimestampColumn(int index) {
        return (QwpTimestampColumnCursor) columnCursors.getQuick(index);
    }

    /**
     * Returns the column cursor cast to geohash type.
     */
    public QwpGeoHashColumnCursor getGeoHashColumn(int index) {
        return (QwpGeoHashColumnCursor) columnCursors.getQuick(index);
    }

    /**
     * Returns the column cursor cast to array type.
     */
    public QwpArrayColumnCursor getArrayColumn(int index) {
        return (QwpArrayColumnCursor) columnCursors.getQuick(index);
    }

    /**
     * Returns the column cursor cast to decimal type.
     */
    public QwpDecimalColumnCursor getDecimalColumn(int index) {
        return (QwpDecimalColumnCursor) columnCursors.getQuick(index);
    }

    /**
     * Resets row iteration to the beginning.
     */
    public void resetRowIteration() {
        currentRow = -1;
        for (int i = 0; i < columnCount; i++) {
            columnCursors.getQuick(i).resetRowPosition();
        }
    }

    /**
     * Returns the number of bytes consumed during parsing.
     */
    public int getBytesConsumed() {
        return bytesConsumed;
    }

    @Override
    public void clear() {
        tableHeader.reset();
        parseResult.clear();
        rowCount = 0;
        columnCount = 0;
        currentRow = -1;
        gorillaEnabled = false;
        columnDefs = null;
        bytesConsumed = 0;
        schemaCache = null;
        connectionSymbolDict = null;
        deltaSymbolDictEnabled = false;

        // Reset type bucket counts
        booleanColumnCount = 0;
        fixedWidthColumnCount = 0;
        timestampColumnCount = 0;
        stringColumnCount = 0;
        symbolColumnCount = 0;
        geoHashColumnCount = 0;
        arrayColumnCount = 0;
        decimalColumnCount = 0;

        // Clear all column cursors
        for (int i = 0; i < columnCursors.size(); i++) {
            QwpColumnCursor cursor = columnCursors.getQuick(i);
            if (cursor != null) {
                cursor.clear();
            }
        }
    }
}
