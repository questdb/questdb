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

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;

/**
 * Decodes ILP v4 table blocks from binary format.
 * <p>
 * A table block consists of:
 * <pre>
 * TABLE HEADER:
 *   Table name length: varint
 *   Table name: UTF-8 bytes
 *   Row count: varint
 *   Column count: varint
 *
 * SCHEMA SECTION:
 *   Schema mode: 0x00 (full) or 0x01 (reference)
 *   If full: column definitions
 *   If reference: schema hash (8 bytes)
 *
 * COLUMN DATA SECTION:
 *   For each column: encoded column data
 * </pre>
 */
public class IlpV4TableBlockDecoder {

    private final IlpV4TableHeader tableHeader = new IlpV4TableHeader();
    private final IlpV4SchemaCache schemaCache;
    private int bytesConsumed;

    // Column decoders
    private final IlpV4BooleanDecoder booleanDecoder = IlpV4BooleanDecoder.INSTANCE;
    private final IlpV4StringDecoder stringDecoder = IlpV4StringDecoder.INSTANCE;
    private final IlpV4SymbolDecoder symbolDecoder = IlpV4SymbolDecoder.INSTANCE;
    private final IlpV4TimestampDecoder timestampDecoder = IlpV4TimestampDecoder.INSTANCE;
    private final IlpV4GeoHashDecoder geoHashDecoder = IlpV4GeoHashDecoder.INSTANCE;

    /**
     * Creates a table block decoder without schema caching.
     */
    public IlpV4TableBlockDecoder() {
        this(null);
    }

    /**
     * Creates a table block decoder with schema caching.
     *
     * @param schemaCache schema cache for reference mode, or null to disable caching
     */
    public IlpV4TableBlockDecoder(IlpV4SchemaCache schemaCache) {
        this.schemaCache = schemaCache;
    }

    /**
     * Decodes a table block from the given address.
     *
     * @param address        starting address of the table block
     * @param limit          end address (exclusive)
     * @param gorillaEnabled whether Gorilla timestamp encoding is enabled
     * @return decoded table block
     * @throws IlpV4ParseException if parsing fails
     */
    public IlpV4DecodedTableBlock decode(long address, long limit, boolean gorillaEnabled) throws IlpV4ParseException {
        int offset = 0;
        int length = (int) (limit - address);

        // Parse table header
        tableHeader.parse(address, length);
        offset = tableHeader.getBytesConsumed();

        String tableName = tableHeader.getTableName();
        int rowCount = (int) tableHeader.getRowCount();
        int columnCount = (int) tableHeader.getColumnCount();

        // Parse schema section
        IlpV4Schema.ParseResult schemaResult = IlpV4Schema.parse(address + offset, length - offset, columnCount);
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
                        "schema not found in cache for table: " + tableName + ", hash: " + schemaResult.schemaHash
                );
            }
        }

        // Verify column count matches schema
        if (schema.getColumnCount() != columnCount) {
            throw IlpV4ParseException.create(
                    IlpV4ParseException.ErrorCode.TABLE_COUNT_MISMATCH,
                    "column count mismatch: header says " + columnCount + ", schema has " + schema.getColumnCount()
            );
        }

        // Create the decoded table block
        IlpV4DecodedTableBlock tableBlock = new IlpV4DecodedTableBlock(tableName, rowCount, schema.getColumns());

        // Decode each column
        for (int i = 0; i < columnCount; i++) {
            IlpV4ColumnDef colDef = schema.getColumn(i);
            IlpV4DecodedColumn decodedCol = tableBlock.getColumn(i);

            int bytesConsumed = decodeColumn(
                    address + offset,
                    length - offset,
                    rowCount,
                    colDef.getTypeCode(),
                    colDef.isNullable(),
                    decodedCol,
                    gorillaEnabled
            );

            offset += bytesConsumed;
        }

        this.bytesConsumed = offset;
        return tableBlock;
    }

    /**
     * Gets the number of bytes consumed by the last decode operation.
     *
     * @return bytes consumed
     */
    public int getBytesConsumed() {
        return bytesConsumed;
    }

    /**
     * Decodes a single column.
     *
     * @param address        column data address
     * @param maxLength      maximum bytes available
     * @param rowCount       number of rows
     * @param typeCode       column type code
     * @param nullable       whether column is nullable
     * @param decodedCol     destination column
     * @param gorillaEnabled whether Gorilla encoding is enabled for timestamps
     * @return bytes consumed
     * @throws IlpV4ParseException if parsing fails
     */
    private int decodeColumn(long address, int maxLength, int rowCount, byte typeCode, boolean nullable,
                             IlpV4DecodedColumn decodedCol, boolean gorillaEnabled) throws IlpV4ParseException {
        IlpV4ColumnDecoder.ColumnSink sink = decodedCol.createSink();
        int type = typeCode & 0xFF;

        switch (type) {
            case TYPE_BOOLEAN:
                return booleanDecoder.decode(address, maxLength, rowCount, nullable, sink);

            case TYPE_BYTE:
            case TYPE_SHORT:
            case TYPE_INT:
            case TYPE_LONG:
            case TYPE_FLOAT:
            case TYPE_DOUBLE:
            case TYPE_DATE:
            case TYPE_UUID:
            case TYPE_LONG256:
                IlpV4FixedWidthDecoder fixedDecoder = new IlpV4FixedWidthDecoder((byte) type);
                return fixedDecoder.decode(address, maxLength, rowCount, nullable, sink);

            case TYPE_TIMESTAMP:
                // Use Gorilla decoder if enabled, otherwise fixed-width
                if (gorillaEnabled) {
                    return timestampDecoder.decode(address, maxLength, rowCount, nullable, sink);
                } else {
                    IlpV4FixedWidthDecoder tsDecoder = new IlpV4FixedWidthDecoder((byte) TYPE_LONG);
                    return tsDecoder.decode(address, maxLength, rowCount, nullable, sink);
                }

            case TYPE_STRING:
            case TYPE_VARCHAR:
                return stringDecoder.decode(address, maxLength, rowCount, nullable,
                        (IlpV4StringDecoder.StringColumnSink) sink);

            case TYPE_SYMBOL:
                return symbolDecoder.decode(address, maxLength, rowCount, nullable,
                        (IlpV4SymbolDecoder.SymbolColumnSink) sink);

            case TYPE_GEOHASH:
                return geoHashDecoder.decode(address, maxLength, rowCount, nullable,
                        (IlpV4GeoHashDecoder.GeoHashColumnSink) sink);

            default:
                throw IlpV4ParseException.create(
                        IlpV4ParseException.ErrorCode.INVALID_COLUMN_TYPE,
                        "unknown column type: " + type
                );
        }
    }

    /**
     * Gets the last parsed table header (for diagnostics).
     *
     * @return table header
     */
    public IlpV4TableHeader getTableHeader() {
        return tableHeader;
    }
}
