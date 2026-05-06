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

package io.questdb.cutlass.qwp.protocol;

import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sequence;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Streaming cursor over a decoded QWP v1 table block.
 * <p>
 * Provides zero-allocation row-by-row iteration through the table data.
 * Column cursors are managed internally and reused across table blocks.
 * <p>
 * <b>Usage:</b>
 * <pre>{@code
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
 * }</pre>
 */
public class QwpTableBlockCursor implements Mutable {

    // Column cursors (reused across table blocks)
    private final ObjList<QwpColumnCursor> columnCursors = new ObjList<>();
    private final QwpSchema.ParseResult parseResult = new QwpSchema.ParseResult();
    private final QwpTableHeader tableHeader;
    private int arrayColumnCount;
    private int[] arrayColumnIndices = new int[16];
    private int booleanColumnCount;
    // Type-bucketed column indices for monomorphic advanceRow() calls
    private int[] booleanColumnIndices = new int[16];
    // Wire position tracking
    private int columnCount;
    // Column definitions from schema
    private ObjList<QwpColumnDef> columnDefs;
    // Cached null flags per column for current row (avoids megamorphic isNull() calls)
    private boolean[] columnNullFlags = new boolean[16];
    // Delta symbol dictionary support
    private ObjList<String> connectionSymbolDict;
    private int currentRow;
    private int decimalColumnCount;
    private int[] decimalColumnIndices = new int[16];
    private boolean deltaSymbolDictEnabled;
    private int fixedWidthColumnCount;
    private int[] fixedWidthColumnIndices = new int[16];
    private int geoHashColumnCount;
    private int[] geoHashColumnIndices = new int[16];
    private boolean gorillaEnabled;
    // Table state
    private int rowCount;
    private int stringColumnCount;
    private int[] stringColumnIndices = new int[16];
    private int symbolColumnCount;
    private int[] symbolColumnIndices = new int[16];
    private int timestampColumnCount;
    private int[] timestampColumnIndices = new int[16];

    public QwpTableBlockCursor() {
        this(DEFAULT_MAX_ROWS_PER_TABLE);
    }

    public QwpTableBlockCursor(int maxRowsPerTable) {
        this.tableHeader = new QwpTableHeader(maxRowsPerTable);
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

    /**
     * Returns the column cursor cast to array type.
     */
    public QwpArrayColumnCursor getArrayColumn(int index) {
        return (QwpArrayColumnCursor) columnCursors.getQuick(index);
    }

    /**
     * Returns the column cursor cast to boolean type.
     */
    public QwpBooleanColumnCursor getBooleanColumn(int index) {
        return (QwpBooleanColumnCursor) columnCursors.getQuick(index);
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
     * Returns the number of columns.
     */
    public int getColumnCount() {
        return columnCount;
    }

    /**
     * Returns the column definition at the specified index.
     */
    public QwpColumnDef getColumnDef(int index) {
        return columnDefs.getQuick(index);
    }

    /**
     * Returns the column cursor cast to decimal type.
     */
    public QwpDecimalColumnCursor getDecimalColumn(int index) {
        return (QwpDecimalColumnCursor) columnCursors.getQuick(index);
    }

    /**
     * Returns the column cursor cast to fixed-width type.
     */
    public QwpFixedWidthColumnCursor getFixedWidthColumn(int index) {
        return (QwpFixedWidthColumnCursor) columnCursors.getQuick(index);
    }

    /**
     * Returns the column cursor cast to geohash type.
     */
    public QwpGeoHashColumnCursor getGeoHashColumn(int index) {
        return (QwpGeoHashColumnCursor) columnCursors.getQuick(index);
    }

    /**
     * Returns the number of rows in this table block.
     */
    public int getRowCount() {
        return rowCount;
    }

    /**
     * Returns the column definitions array for schema access.
     * Note: Returns internal array directly (no copy) for zero-allocation.
     */
    public ObjList<QwpColumnDef> getSchema() {
        return columnDefs;
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
     * Returns the table name as a String.
     * <p>
     * This allocates on first call per table block. Prefer {@link #getTableNameUtf8()}
     * for zero-allocation access on hot paths.
     */
    public String getTableName() {
        return tableHeader.getTableName();
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
     * Returns the column cursor cast to timestamp type.
     */
    public QwpTimestampColumnCursor getTimestampColumn(int index) {
        return (QwpTimestampColumnCursor) columnCursors.getQuick(index);
    }

    /**
     * Returns whether there are more rows to iterate.
     */
    public boolean hasNextRow() {
        return currentRow + 1 < rowCount;
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
     * Initializes this cursor for the given table block data with delta symbol dictionary support.
     *
     * @param dataAddress            address of table block data
     * @param dataLength             available bytes
     * @param gorillaEnabled         whether Gorilla encoding is enabled
     * @param schemaRegistry         schema registry for reference mode (may be null)
     * @param connectionSymbolDict   connection-level symbol dictionary (may be null)
     * @param deltaSymbolDictEnabled whether delta mode is enabled
     * @return bytes consumed from dataAddress
     * @throws QwpParseException if parsing fails
     */
    public int of(
            long dataAddress,
            int dataLength,
            boolean gorillaEnabled,
            QwpSchemaRegistry schemaRegistry,
            ObjList<String> connectionSymbolDict,
            boolean deltaSymbolDictEnabled
    ) throws QwpParseException {
        this.gorillaEnabled = gorillaEnabled;
        this.connectionSymbolDict = connectionSymbolDict;
        this.deltaSymbolDictEnabled = deltaSymbolDictEnabled;

        // Parse table header
        tableHeader.parse(dataAddress, dataLength);
        int offset = tableHeader.getBytesConsumed();

        this.rowCount = (int) tableHeader.getRowCount();
        this.columnCount = tableHeader.getColumnCount();

        // Parse schema section (zero-alloc: reuses parseResult)
        QwpSchema.parse(dataAddress + offset, dataLength - offset, columnCount, parseResult);
        offset += parseResult.bytesConsumed;

        QwpSchema schema;
        if (!parseResult.isReference) {
            schema = parseResult.schema;
            // Register the schema if registry is enabled
            if (schemaRegistry != null) {
                schemaRegistry.put(parseResult.schemaId, schema);
            }
        } else {
            // Schema reference mode - look up in registry
            if (schemaRegistry == null) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.SCHEMA_NOT_FOUND,
                        "schema reference mode requires schema registry"
                );
            }
            schema = schemaRegistry.get(parseResult.schemaId);
            if (schema == null) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.SCHEMA_NOT_FOUND,
                        "schema not found in registry for table: " + tableHeader.getTableName()
                );
            }
            if (schema.getColumnCount() != columnCount) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.SCHEMA_MISMATCH,
                        "schema column count mismatch: header=" + columnCount
                                + ", schema=" + schema.getColumnCount()
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
            QwpColumnDef colDef = columnDefs.getQuick(i);
            byte typeCode = colDef.getTypeCode();

            if (offset < 0 || offset > dataLength) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "column data exceeds table block bounds before column " + i
                );
            }
            int consumed = initializeColumnCursor(
                    i,
                    dataAddress + offset,
                    dataLength - offset,
                    rowCount,
                    typeCode
            );
            offset += consumed;
        }

        this.currentRow = -1;

        return offset;
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

    private int initializeColumnCursor(
            int colIndex,
            long dataAddress,
            int dataLength,
            int rowCount,
            byte typeCode
    ) throws QwpParseException {
        QwpColumnCursor cursor = columnCursors.getQuick(colIndex);

        return switch (typeCode) {
            case TYPE_BOOLEAN -> {
                QwpBooleanColumnCursor boolCursor;
                if (cursor instanceof QwpBooleanColumnCursor c) {
                    boolCursor = c;
                } else {
                    boolCursor = new QwpBooleanColumnCursor();
                    columnCursors.setQuick(colIndex, boolCursor);
                }
                booleanColumnIndices[booleanColumnCount++] = colIndex;
                yield boolCursor.of(dataAddress, dataLength, rowCount);
            }
            case TYPE_BYTE, TYPE_SHORT, TYPE_CHAR, TYPE_INT, TYPE_LONG,
                 TYPE_FLOAT, TYPE_DOUBLE, TYPE_DATE, TYPE_UUID, TYPE_LONG256 -> {
                QwpFixedWidthColumnCursor fixedCursor;
                if (cursor instanceof QwpFixedWidthColumnCursor c) {
                    fixedCursor = c;
                } else {
                    fixedCursor = new QwpFixedWidthColumnCursor();
                    columnCursors.setQuick(colIndex, fixedCursor);
                }
                fixedWidthColumnIndices[fixedWidthColumnCount++] = colIndex;
                yield fixedCursor.of(dataAddress, dataLength, rowCount, typeCode);
            }
            case TYPE_TIMESTAMP, TYPE_TIMESTAMP_NANOS -> {
                QwpTimestampColumnCursor tsCursor;
                if (cursor instanceof QwpTimestampColumnCursor c) {
                    tsCursor = c;
                } else {
                    tsCursor = new QwpTimestampColumnCursor();
                    columnCursors.setQuick(colIndex, tsCursor);
                }
                timestampColumnIndices[timestampColumnCount++] = colIndex;
                yield tsCursor.of(dataAddress, dataLength, rowCount, typeCode, gorillaEnabled);
            }
            case TYPE_VARCHAR -> {
                QwpStringColumnCursor strCursor;
                if (cursor instanceof QwpStringColumnCursor c) {
                    strCursor = c;
                } else {
                    strCursor = new QwpStringColumnCursor();
                    columnCursors.setQuick(colIndex, strCursor);
                }
                stringColumnIndices[stringColumnCount++] = colIndex;
                yield strCursor.of(dataAddress, dataLength, rowCount, typeCode);
            }
            case TYPE_SYMBOL -> {
                QwpSymbolColumnCursor symCursor;
                if (cursor instanceof QwpSymbolColumnCursor c) {
                    symCursor = c;
                } else {
                    symCursor = new QwpSymbolColumnCursor();
                    columnCursors.setQuick(colIndex, symCursor);
                }
                symbolColumnIndices[symbolColumnCount++] = colIndex;
                // In delta mode, pass connection dictionary; otherwise null (per-column dict)
                ObjList<String> dictForSymbol = deltaSymbolDictEnabled ? connectionSymbolDict : null;
                yield symCursor.of(dataAddress, dataLength, rowCount, dictForSymbol);
            }
            case TYPE_GEOHASH -> {
                QwpGeoHashColumnCursor geoCursor;
                if (cursor instanceof QwpGeoHashColumnCursor c) {
                    geoCursor = c;
                } else {
                    geoCursor = new QwpGeoHashColumnCursor();
                    columnCursors.setQuick(colIndex, geoCursor);
                }
                geoHashColumnIndices[geoHashColumnCount++] = colIndex;
                yield geoCursor.of(dataAddress, dataLength, rowCount);
            }
            case TYPE_DOUBLE_ARRAY, TYPE_LONG_ARRAY -> {
                QwpArrayColumnCursor arrCursor;
                if (cursor instanceof QwpArrayColumnCursor c) {
                    arrCursor = c;
                } else {
                    arrCursor = new QwpArrayColumnCursor();
                    columnCursors.setQuick(colIndex, arrCursor);
                }
                arrayColumnIndices[arrayColumnCount++] = colIndex;
                yield arrCursor.of(dataAddress, dataLength, rowCount, typeCode);
            }
            case TYPE_DECIMAL64, TYPE_DECIMAL128, TYPE_DECIMAL256 -> {
                QwpDecimalColumnCursor decCursor;
                if (cursor instanceof QwpDecimalColumnCursor c) {
                    decCursor = c;
                } else {
                    decCursor = new QwpDecimalColumnCursor();
                    columnCursors.setQuick(colIndex, decCursor);
                }
                decimalColumnIndices[decimalColumnCount++] = colIndex;
                yield decCursor.of(dataAddress, dataLength, rowCount, typeCode);
            }
            default -> throw QwpParseException.create(
                    QwpParseException.ErrorCode.INVALID_COLUMN_TYPE,
                    "unknown column type: " + typeCode
            );
        };
    }
}
