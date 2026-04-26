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
import io.questdb.std.Unsafe;

import java.nio.charset.StandardCharsets;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.MAX_COLUMN_NAME_LENGTH;

/**
 * Represents a QWP v1 table schema (immutable, safe for caching).
 * <p>
 * A schema consists of an ordered list of column definitions.
 * <p>
 * Schema modes:
 * <ul>
 *   <li>0x00 - Full schema (column definitions inline, preceded by varint schemaId)</li>
 *   <li>0x01 - Schema reference (only varint schemaId, lookup from registry)</li>
 * </ul>
 */
public final class QwpSchema {

    /**
     * Full schema mode - schema is defined inline.
     */
    public static final byte SCHEMA_MODE_FULL = 0x00;

    /**
     * Schema reference mode - only schemaId is provided.
     */
    public static final byte SCHEMA_MODE_REFERENCE = 0x01;

    private final ObjList<QwpColumnDef> columns;

    private QwpSchema(ObjList<QwpColumnDef> columns) {
        this.columns = columns;
    }

    /**
     * Creates a schema from column definitions.
     *
     * @param columns the column definitions
     * @return the schema
     */
    public static QwpSchema create(QwpColumnDef[] columns) {
        ObjList<QwpColumnDef> list = new ObjList<>(columns.length);
        for (QwpColumnDef col : columns) {
            list.add(col);
        }
        return new QwpSchema(list);
    }

    /**
     * Encodes a schema reference to direct memory.
     *
     * @param address  destination address
     * @param schemaId the schema ID
     * @return address after encoded reference
     */
    public static long encodeReference(long address, int schemaId) {
        Unsafe.putByte(address, SCHEMA_MODE_REFERENCE);
        return QwpVarint.encode(address + 1, schemaId);
    }

    /**
     * Encodes a schema reference to a byte array.
     *
     * @param buf      destination buffer
     * @param offset   starting offset
     * @param schemaId the schema ID
     */
    public static void encodeReference(byte[] buf, int offset, int schemaId) {
        buf[offset++] = SCHEMA_MODE_REFERENCE;
        QwpVarint.encode(buf, offset, schemaId);
    }

    /**
     * Parses a schema section from a byte array.
     *
     * @param buf         buffer
     * @param bufOffset   starting offset
     * @param length      available bytes
     * @param columnCount expected number of columns
     * @return parse result
     * @throws QwpParseException if parsing fails
     */
    public static ParseResult parse(byte[] buf, int bufOffset, int length, int columnCount) throws QwpParseException {
        if (length < 1) {
            throw QwpParseException.headerTooShort();
        }

        byte schemaMode = buf[bufOffset];
        int offset = 1;

        if (schemaMode == SCHEMA_MODE_REFERENCE) {
            QwpVarint.DecodeResult decodeResult = new QwpVarint.DecodeResult();
            QwpVarint.decode(buf, bufOffset + offset, bufOffset + length, decodeResult);
            int schemaId = validateSchemaId(decodeResult.value);
            offset += decodeResult.bytesRead;
            return ParseResult.reference(schemaId, offset);
        } else if (schemaMode == SCHEMA_MODE_FULL) {
            return parseFullSchemaFromArray(buf, bufOffset, length, columnCount, offset);
        } else {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INVALID_SCHEMA_MODE,
                    "unknown schema mode: 0x" + Integer.toHexString(schemaMode & 0xFF)
            );
        }
    }

    /**
     * Parses a schema section from direct memory (zero-allocation version).
     * <p>
     * This method populates the provided {@link ParseResult} instead of allocating a new one.
     * Use this on hot paths where allocation must be avoided.
     *
     * @param address     memory address
     * @param length      available bytes
     * @param columnCount expected number of columns (from table header)
     * @param result      reusable parse result to populate
     * @throws QwpParseException if parsing fails
     */
    public static void parse(long address, int length, int columnCount, ParseResult result) throws QwpParseException {
        if (length < 1) {
            throw QwpParseException.headerTooShort();
        }

        byte schemaMode = Unsafe.getByte(address);
        int offset = 1;

        if (schemaMode == SCHEMA_MODE_REFERENCE) {
            // Schema reference mode - just a varint schemaId (zero-alloc path)
            QwpVarint.decode(address + offset, address + length, result.decodeResult);
            int schemaId = validateSchemaId(result.decodeResult.value);
            offset += result.decodeResult.bytesRead;
            result.setReference(schemaId, offset);
        } else if (schemaMode == SCHEMA_MODE_FULL) {
            // Full schema mode - parse schemaId + column definitions (allocates for new schema)
            parseFullSchema(address, length, columnCount, offset, result);
        } else {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INVALID_SCHEMA_MODE,
                    "unknown schema mode: 0x" + Integer.toHexString(schemaMode & 0xFF)
            );
        }
    }

    /**
     * Parses a schema section from direct memory.
     * <p>
     * Convenience method that allocates a new {@link ParseResult}. For zero-allocation
     * parsing, use {@link #parse(long, int, int, ParseResult)} instead.
     *
     * @param address     memory address
     * @param length      available bytes
     * @param columnCount expected number of columns (from table header)
     * @return parse result
     * @throws QwpParseException if parsing fails
     */
    public static ParseResult parse(long address, int length, int columnCount) throws QwpParseException {
        ParseResult result = new ParseResult();
        parse(address, length, columnCount, result);
        return result;
    }

    /**
     * Encodes this schema in full schema mode to direct memory.
     *
     * @param address  destination address
     * @param schemaId the schema ID to encode
     * @return address after encoded schema
     */
    public long encode(long address, int schemaId) {
        Unsafe.putByte(address, SCHEMA_MODE_FULL);
        long pos = address + 1;
        pos = QwpVarint.encode(pos, schemaId);

        for (int i = 0, n = columns.size(); i < n; i++) {
            QwpColumnDef col = columns.getQuick(i);
            byte[] nameBytes = col.getNameUtf8();
            pos = QwpVarint.encode(pos, nameBytes.length);
            for (byte b : nameBytes) {
                Unsafe.putByte(pos++, b);
            }
            Unsafe.putByte(pos++, col.getWireTypeCode());
        }

        return pos;
    }

    /**
     * Encodes this schema in full schema mode to a byte array.
     *
     * @param buf      destination buffer
     * @param offset   starting offset
     * @param schemaId the schema ID to encode
     * @return offset after encoded schema
     */
    public int encode(byte[] buf, int offset, int schemaId) {
        buf[offset++] = SCHEMA_MODE_FULL;
        offset = QwpVarint.encode(buf, offset, schemaId);

        for (int i = 0, n = columns.size(); i < n; i++) {
            QwpColumnDef col = columns.getQuick(i);
            byte[] nameBytes = col.getNameUtf8();
            offset = QwpVarint.encode(buf, offset, nameBytes.length);
            System.arraycopy(nameBytes, 0, buf, offset, nameBytes.length);
            offset += nameBytes.length;
            buf[offset++] = col.getWireTypeCode();
        }

        return offset;
    }

    /**
     * Computes the encoded size in bytes for this schema in full mode.
     *
     * @param schemaId the schema ID (needed to compute varint size)
     * @return encoded size
     */
    public int encodedSize(int schemaId) {
        int size = 1; // schema mode byte
        size += QwpVarint.encodedLength(schemaId);
        for (int i = 0, n = columns.size(); i < n; i++) {
            QwpColumnDef col = columns.getQuick(i);
            byte[] nameBytes = col.getNameUtf8();
            size += QwpVarint.encodedLength(nameBytes.length);
            size += nameBytes.length;
            size += 1; // type code
        }
        return size;
    }

    /**
     * Gets the column definition at the specified index.
     *
     * @param index column index
     * @return column definition
     */
    public QwpColumnDef getColumn(int index) {
        return columns.getQuick(index);
    }

    /**
     * Gets the number of columns in this schema.
     */
    public int getColumnCount() {
        return columns.size();
    }

    /**
     * Gets all column definitions.
     * <p>
     * Returns the internal list directly (no copy) for zero-allocation access.
     * Callers must not modify the returned list.
     *
     * @return column definitions list (do not modify)
     */
    public ObjList<QwpColumnDef> getColumns() {
        return columns;
    }

    private static void parseFullSchema(long address, int length, int columnCount, int offset, ParseResult result) throws QwpParseException {
        ObjList<QwpColumnDef> columns = new ObjList<>(columnCount);
        QwpVarint.DecodeResult decodeResult = result.decodeResult;
        long limit = address + length; // Absolute end address

        // Read schema ID
        QwpVarint.decode(address + offset, limit, decodeResult);
        int schemaId = validateSchemaId(decodeResult.value);
        offset += decodeResult.bytesRead;

        for (int i = 0; i < columnCount; i++) {
            if (offset >= length) {
                throw QwpParseException.headerTooShort();
            }

            // Parse column name length (varint)
            QwpVarint.decode(address + offset, limit, decodeResult);
            offset += decodeResult.bytesRead;

            // Empty column names (length 0) are allowed for designated timestamp columns.
            if (decodeResult.value < 0 || decodeResult.value > MAX_COLUMN_NAME_LENGTH) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INVALID_COLUMN_NAME,
                        "invalid column name length at column " + i + ": " + decodeResult.value
                );
            }
            int nameLenInt = (int) decodeResult.value;
            if (offset + nameLenInt + 1 > length) {
                throw QwpParseException.headerTooShort();
            }

            // Read column name bytes
            byte[] nameBytes = new byte[nameLenInt];
            for (int j = 0; j < nameLenInt; j++) {
                nameBytes[j] = Unsafe.getByte(address + offset + j);
            }
            String columnName = new String(nameBytes, StandardCharsets.UTF_8);
            offset += nameLenInt;

            // Read column type
            byte typeCode = Unsafe.getByte(address + offset);
            offset++;

            QwpColumnDef colDef = new QwpColumnDef(columnName, typeCode);
            colDef.validate();
            columns.add(colDef);
        }

        result.setFullSchema(new QwpSchema(columns), schemaId, offset);
    }

    private static ParseResult parseFullSchemaFromArray(byte[] buf, int bufOffset, int length, int columnCount, int offset) throws QwpParseException {
        ObjList<QwpColumnDef> columns = new ObjList<>(columnCount);
        QwpVarint.DecodeResult decodeResult = new QwpVarint.DecodeResult();
        int limit = bufOffset + length; // Absolute end position

        // Read schema ID
        QwpVarint.decode(buf, bufOffset + offset, limit, decodeResult);
        int schemaId = validateSchemaId(decodeResult.value);
        offset += decodeResult.bytesRead;

        for (int i = 0; i < columnCount; i++) {
            if (offset >= length) {
                throw QwpParseException.headerTooShort();
            }

            // Parse column name length (varint)
            QwpVarint.decode(buf, bufOffset + offset, limit, decodeResult);
            offset += decodeResult.bytesRead;

            if (decodeResult.value < 0 || decodeResult.value > MAX_COLUMN_NAME_LENGTH) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INVALID_COLUMN_NAME,
                        "invalid column name length at column " + i + ": " + decodeResult.value
                );
            }
            int nameLenInt = (int) decodeResult.value;
            if (offset + nameLenInt + 1 > length) {
                throw QwpParseException.headerTooShort();
            }

            // Read column name
            String columnName = new String(buf, bufOffset + offset, nameLenInt, StandardCharsets.UTF_8);
            offset += nameLenInt;

            // Read column type
            byte typeCode = buf[bufOffset + offset];
            offset++;

            QwpColumnDef colDef = new QwpColumnDef(columnName, typeCode);
            colDef.validate();
            columns.add(colDef);
        }

        return ParseResult.fullSchema(new QwpSchema(columns), schemaId, offset);
    }

    private static int validateSchemaId(long value) throws QwpParseException {
        if (value > Integer.MAX_VALUE) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INVALID_SCHEMA_ID,
                    "schema ID exceeds int range: " + value
            );
        }
        return (int) value;
    }

    /**
     * Result of parsing a schema section.
     * <p>
     * This class is mutable and reusable to avoid allocations on hot paths.
     * Use {@link #setFullSchema} or {@link #setReference} to populate, and
     * {@link #clear} to reset for reuse.
     */
    public static final class ParseResult implements Mutable {
        public final QwpVarint.DecodeResult decodeResult = new QwpVarint.DecodeResult();
        public int bytesConsumed;
        public boolean isReference;
        public QwpSchema schema;
        public int schemaId;

        // Factory methods for convenience (allocate - use in tests)
        public static ParseResult fullSchema(QwpSchema schema, int schemaId, int bytesConsumed) {
            ParseResult result = new ParseResult();
            result.setFullSchema(schema, schemaId, bytesConsumed);
            return result;
        }

        public static ParseResult reference(int schemaId, int bytesConsumed) {
            ParseResult result = new ParseResult();
            result.setReference(schemaId, bytesConsumed);
            return result;
        }

        @Override
        public void clear() {
            this.schema = null;
            this.schemaId = 0;
            this.isReference = false;
            this.bytesConsumed = 0;
        }

        public void setFullSchema(QwpSchema schema, int schemaId, int bytesConsumed) {
            this.schema = schema;
            this.schemaId = schemaId;
            this.isReference = false;
            this.bytesConsumed = bytesConsumed;
        }

        public void setReference(int schemaId, int bytesConsumed) {
            this.schema = null;
            this.schemaId = schemaId;
            this.isReference = true;
            this.bytesConsumed = bytesConsumed;
        }
    }
}
