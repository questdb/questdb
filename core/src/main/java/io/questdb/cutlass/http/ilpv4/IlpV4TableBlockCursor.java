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

package io.questdb.cutlass.http.ilpv4;

import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;

import static io.questdb.cutlass.http.ilpv4.IlpV4Constants.*;

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
 *         IlpV4ColumnCursor colCursor = cursor.getColumn(col);
 *         if (!colCursor.isNull()) {
 *             // read value based on type
 *         }
 *     }
 * }
 * </pre>
 */
public class IlpV4TableBlockCursor implements Mutable {

    private final DirectUtf8String tableNameUtf8 = new DirectUtf8String();
    private final IlpV4TableHeader tableHeader = new IlpV4TableHeader();
    private final IlpV4Varint.DecodeResult decodeResult = new IlpV4Varint.DecodeResult();

    // Column cursors (reused across table blocks)
    private final ObjList<IlpV4ColumnCursor> columnCursors = new ObjList<>();

    // Schema cache reference
    private IlpV4SchemaCache schemaCache;

    // Table state
    private String tableName;  // Cached for table lookup (one allocation per table block)
    private int rowCount;
    private int columnCount;
    private int currentRow;
    private boolean gorillaEnabled;

    // Column definitions from schema
    private IlpV4ColumnDef[] columnDefs;

    // Wire position tracking
    private int bytesConsumed;

    /**
     * Initializes this cursor for the given table block data.
     *
     * @param dataAddress    address of table block data
     * @param dataLength     available bytes
     * @param gorillaEnabled whether Gorilla encoding is enabled
     * @param schemaCache    schema cache for reference mode (may be null)
     * @return bytes consumed from dataAddress
     * @throws IlpV4ParseException if parsing fails
     */
    public int of(long dataAddress, int dataLength, boolean gorillaEnabled, IlpV4SchemaCache schemaCache)
            throws IlpV4ParseException {
        this.gorillaEnabled = gorillaEnabled;
        this.schemaCache = schemaCache;

        int offset = 0;
        long limit = dataAddress + dataLength;

        // Parse table header
        tableHeader.parse(dataAddress, dataLength);
        offset = tableHeader.getBytesConsumed();

        this.tableName = tableHeader.getTableName();
        this.rowCount = (int) tableHeader.getRowCount();
        this.columnCount = tableHeader.getColumnCount();

        // Set up table name flyweight (points to wire memory)
        // Note: We need to find the table name start address from the header parsing
        // The table header format is: [name_len varint][name bytes][row_count varint][col_count varint]
        // For now, we use the String from the header (one allocation per table)
        // TODO: Parse table name address directly for full zero-alloc

        // Parse schema section
        IlpV4Schema.ParseResult schemaResult = IlpV4Schema.parse(dataAddress + offset, dataLength - offset, columnCount);
        offset += schemaResult.bytesConsumed;

        IlpV4Schema schema;
        if (!schemaResult.isReference) {
            schema = schemaResult.schema;
            // Cache the schema if caching is enabled
            if (schemaCache != null) {
                schemaCache.put(tableName, schema);
            }
        } else {
            // Schema reference mode - look up in cache
            if (schemaCache == null) {
                throw IlpV4ParseException.create(
                        IlpV4ParseException.ErrorCode.SCHEMA_NOT_FOUND,
                        "schema reference mode requires schema cache"
                );
            }
            schema = schemaCache.get(tableName, schemaResult.schemaHash);
            if (schema == null) {
                throw IlpV4ParseException.create(
                        IlpV4ParseException.ErrorCode.SCHEMA_NOT_FOUND,
                        "schema not found in cache for table: " + tableName
                );
            }
        }

        this.columnDefs = schema.getColumns();

        // Initialize column cursors
        ensureColumnCursorCapacity(columnCount);
        for (int i = 0; i < columnCount; i++) {
            IlpV4ColumnDef colDef = columnDefs[i];
            byte typeCode = colDef.getTypeCode();
            boolean nullable = colDef.isNullable();
            String colName = colDef.getName();

            // Get column name address (we need to find it in schema bytes)
            // For simplicity, use name bytes from the ColumnDef
            byte[] nameBytes = colName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            // We can't easily get the wire address here, so we skip the name flyweight
            // The column cursor will have an empty/null name reference
            // This is acceptable since column mapping uses index-based lookup

            int consumed = initializeColumnCursor(
                    i, dataAddress + offset, dataLength - offset, rowCount,
                    typeCode, nullable, 0, 0  // No name address/length for now
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
    }

    private int initializeColumnCursor(int colIndex, long dataAddress, int dataLength, int rowCount,
                                        byte typeCode, boolean nullable, long nameAddress, int nameLength)
            throws IlpV4ParseException {
        int type = typeCode & TYPE_MASK;

        IlpV4ColumnCursor cursor = columnCursors.getQuick(colIndex);

        switch (type) {
            case TYPE_BOOLEAN:
                IlpV4BooleanColumnCursor boolCursor;
                if (cursor instanceof IlpV4BooleanColumnCursor) {
                    boolCursor = (IlpV4BooleanColumnCursor) cursor;
                } else {
                    boolCursor = new IlpV4BooleanColumnCursor();
                    columnCursors.setQuick(colIndex, boolCursor);
                }
                return boolCursor.of(dataAddress, dataLength, rowCount, nullable, nameAddress, nameLength);

            case TYPE_BYTE:
            case TYPE_SHORT:
            case TYPE_INT:
            case TYPE_LONG:
            case TYPE_FLOAT:
            case TYPE_DOUBLE:
            case TYPE_DATE:
            case TYPE_UUID:
            case TYPE_LONG256:
                IlpV4FixedWidthColumnCursor fixedCursor;
                if (cursor instanceof IlpV4FixedWidthColumnCursor) {
                    fixedCursor = (IlpV4FixedWidthColumnCursor) cursor;
                } else {
                    fixedCursor = new IlpV4FixedWidthColumnCursor();
                    columnCursors.setQuick(colIndex, fixedCursor);
                }
                return fixedCursor.of(dataAddress, dataLength, rowCount, typeCode, nullable, nameAddress, nameLength);

            case TYPE_TIMESTAMP:
            case TYPE_TIMESTAMP_NANOS:
                IlpV4TimestampColumnCursor tsCursor;
                if (cursor instanceof IlpV4TimestampColumnCursor) {
                    tsCursor = (IlpV4TimestampColumnCursor) cursor;
                } else {
                    tsCursor = new IlpV4TimestampColumnCursor();
                    columnCursors.setQuick(colIndex, tsCursor);
                }
                return tsCursor.of(dataAddress, dataLength, rowCount, typeCode, nullable, nameAddress, nameLength, gorillaEnabled);

            case TYPE_STRING:
            case TYPE_VARCHAR:
                IlpV4StringColumnCursor strCursor;
                if (cursor instanceof IlpV4StringColumnCursor) {
                    strCursor = (IlpV4StringColumnCursor) cursor;
                } else {
                    strCursor = new IlpV4StringColumnCursor();
                    columnCursors.setQuick(colIndex, strCursor);
                }
                return strCursor.of(dataAddress, dataLength, rowCount, typeCode, nullable, nameAddress, nameLength);

            case TYPE_SYMBOL:
                IlpV4SymbolColumnCursor symCursor;
                if (cursor instanceof IlpV4SymbolColumnCursor) {
                    symCursor = (IlpV4SymbolColumnCursor) cursor;
                } else {
                    symCursor = new IlpV4SymbolColumnCursor();
                    columnCursors.setQuick(colIndex, symCursor);
                }
                return symCursor.of(dataAddress, dataLength, rowCount, nullable, nameAddress, nameLength);

            case TYPE_GEOHASH:
                IlpV4GeoHashColumnCursor geoCursor;
                if (cursor instanceof IlpV4GeoHashColumnCursor) {
                    geoCursor = (IlpV4GeoHashColumnCursor) cursor;
                } else {
                    geoCursor = new IlpV4GeoHashColumnCursor();
                    columnCursors.setQuick(colIndex, geoCursor);
                }
                return geoCursor.of(dataAddress, dataLength, rowCount, nullable, nameAddress, nameLength);

            default:
                throw IlpV4ParseException.create(
                        IlpV4ParseException.ErrorCode.INVALID_COLUMN_TYPE,
                        "unknown column type: " + type
                );
        }
    }

    /**
     * Returns the table name.
     * <p>
     * Note: This allocates on first call per table block for cache lookup.
     */
    public String getTableName() {
        return tableName;
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
    public IlpV4ColumnDef getColumnDef(int index) {
        return columnDefs[index];
    }

    /**
     * Returns the column definitions array for schema access.
     * Note: Returns internal array directly (no copy) for zero-allocation.
     */
    public IlpV4ColumnDef[] getSchema() {
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
     *
     * @throws IlpV4ParseException if parsing fails during row advance
     */
    public void nextRow() throws IlpV4ParseException {
        currentRow++;
        for (int i = 0; i < columnCount; i++) {
            columnCursors.getQuick(i).advanceRow();
        }
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
    public IlpV4ColumnCursor getColumn(int index) {
        return columnCursors.getQuick(index);
    }

    /**
     * Returns the column cursor cast to fixed-width type.
     */
    public IlpV4FixedWidthColumnCursor getFixedWidthColumn(int index) {
        return (IlpV4FixedWidthColumnCursor) columnCursors.getQuick(index);
    }

    /**
     * Returns the column cursor cast to boolean type.
     */
    public IlpV4BooleanColumnCursor getBooleanColumn(int index) {
        return (IlpV4BooleanColumnCursor) columnCursors.getQuick(index);
    }

    /**
     * Returns the column cursor cast to string type.
     */
    public IlpV4StringColumnCursor getStringColumn(int index) {
        return (IlpV4StringColumnCursor) columnCursors.getQuick(index);
    }

    /**
     * Returns the column cursor cast to symbol type.
     */
    public IlpV4SymbolColumnCursor getSymbolColumn(int index) {
        return (IlpV4SymbolColumnCursor) columnCursors.getQuick(index);
    }

    /**
     * Returns the column cursor cast to timestamp type.
     */
    public IlpV4TimestampColumnCursor getTimestampColumn(int index) {
        return (IlpV4TimestampColumnCursor) columnCursors.getQuick(index);
    }

    /**
     * Returns the column cursor cast to geohash type.
     */
    public IlpV4GeoHashColumnCursor getGeoHashColumn(int index) {
        return (IlpV4GeoHashColumnCursor) columnCursors.getQuick(index);
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
        tableNameUtf8.clear();
        tableHeader.reset();
        tableName = null;
        rowCount = 0;
        columnCount = 0;
        currentRow = -1;
        gorillaEnabled = false;
        columnDefs = null;
        bytesConsumed = 0;
        schemaCache = null;

        // Clear all column cursors
        for (int i = 0; i < columnCursors.size(); i++) {
            IlpV4ColumnCursor cursor = columnCursors.getQuick(i);
            if (cursor != null) {
                cursor.clear();
            }
        }
    }
}
