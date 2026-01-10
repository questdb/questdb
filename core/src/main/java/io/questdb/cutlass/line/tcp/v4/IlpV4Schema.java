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

import io.questdb.std.Unsafe;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;

/**
 * Represents an ILP v4 table schema (immutable, safe for caching).
 * <p>
 * A schema consists of an ordered list of column definitions.
 * The schema hash is computed as XXH64 over the full schema bytes.
 * <p>
 * Schema modes:
 * <ul>
 *   <li>0x00 - Full schema (column definitions inline)</li>
 *   <li>0x01 - Schema reference (only hash, lookup from cache)</li>
 * </ul>
 */
public final class IlpV4Schema {

    /**
     * Full schema mode - schema is defined inline.
     */
    public static final byte SCHEMA_MODE_FULL = 0x00;

    /**
     * Schema reference mode - only hash is provided.
     */
    public static final byte SCHEMA_MODE_REFERENCE = 0x01;

    private final IlpV4ColumnDef[] columns;
    private final long schemaHash;

    private IlpV4Schema(IlpV4ColumnDef[] columns, long schemaHash) {
        this.columns = columns;
        this.schemaHash = schemaHash;
    }

    /**
     * Creates a schema from column definitions and computes the hash.
     *
     * @param columns the column definitions
     * @return the schema
     */
    public static IlpV4Schema create(IlpV4ColumnDef[] columns) {
        long hash = computeSchemaHash(columns);
        return new IlpV4Schema(columns.clone(), hash);
    }

    /**
     * Creates a schema from column definitions with a pre-computed hash.
     *
     * @param columns    the column definitions
     * @param schemaHash the pre-computed schema hash
     * @return the schema
     */
    public static IlpV4Schema createWithHash(IlpV4ColumnDef[] columns, long schemaHash) {
        return new IlpV4Schema(columns.clone(), schemaHash);
    }

    /**
     * Result of parsing a schema section.
     */
    public static final class ParseResult {
        public final IlpV4Schema schema;
        public final long schemaHash;
        public final boolean isReference;
        public final int bytesConsumed;

        private ParseResult(IlpV4Schema schema, long schemaHash, boolean isReference, int bytesConsumed) {
            this.schema = schema;
            this.schemaHash = schemaHash;
            this.isReference = isReference;
            this.bytesConsumed = bytesConsumed;
        }

        public static ParseResult fullSchema(IlpV4Schema schema, int bytesConsumed) {
            return new ParseResult(schema, schema.getSchemaHash(), false, bytesConsumed);
        }

        public static ParseResult reference(long schemaHash, int bytesConsumed) {
            return new ParseResult(null, schemaHash, true, bytesConsumed);
        }
    }

    /**
     * Parses a schema section from direct memory.
     *
     * @param address     memory address
     * @param length      available bytes
     * @param columnCount expected number of columns (from table header)
     * @return parse result
     * @throws IlpV4ParseException if parsing fails
     */
    public static ParseResult parse(long address, int length, int columnCount) throws IlpV4ParseException {
        if (length < 1) {
            throw IlpV4ParseException.headerTooShort();
        }

        byte schemaMode = Unsafe.getUnsafe().getByte(address);
        int offset = 1;

        if (schemaMode == SCHEMA_MODE_REFERENCE) {
            // Schema reference mode - just a hash
            if (length < 1 + 8) {
                throw IlpV4ParseException.headerTooShort();
            }
            long schemaHash = Unsafe.getUnsafe().getLong(address + offset);
            return ParseResult.reference(schemaHash, 1 + 8);
        } else if (schemaMode == SCHEMA_MODE_FULL) {
            // Full schema mode - parse column definitions
            return parseFullSchema(address, length, columnCount, offset);
        } else {
            throw IlpV4ParseException.create(
                    IlpV4ParseException.ErrorCode.INVALID_SCHEMA_MODE,
                    "unknown schema mode: 0x" + Integer.toHexString(schemaMode & 0xFF)
            );
        }
    }

    /**
     * Parses a schema section from a byte array.
     *
     * @param buf         buffer
     * @param bufOffset   starting offset
     * @param length      available bytes
     * @param columnCount expected number of columns
     * @return parse result
     * @throws IlpV4ParseException if parsing fails
     */
    public static ParseResult parse(byte[] buf, int bufOffset, int length, int columnCount) throws IlpV4ParseException {
        if (length < 1) {
            throw IlpV4ParseException.headerTooShort();
        }

        byte schemaMode = buf[bufOffset];
        int offset = 1;

        if (schemaMode == SCHEMA_MODE_REFERENCE) {
            if (length < 1 + 8) {
                throw IlpV4ParseException.headerTooShort();
            }
            long schemaHash = (buf[bufOffset + offset] & 0xFFL) |
                    ((buf[bufOffset + offset + 1] & 0xFFL) << 8) |
                    ((buf[bufOffset + offset + 2] & 0xFFL) << 16) |
                    ((buf[bufOffset + offset + 3] & 0xFFL) << 24) |
                    ((buf[bufOffset + offset + 4] & 0xFFL) << 32) |
                    ((buf[bufOffset + offset + 5] & 0xFFL) << 40) |
                    ((buf[bufOffset + offset + 6] & 0xFFL) << 48) |
                    ((buf[bufOffset + offset + 7] & 0xFFL) << 56);
            return ParseResult.reference(schemaHash, 1 + 8);
        } else if (schemaMode == SCHEMA_MODE_FULL) {
            return parseFullSchemaFromArray(buf, bufOffset, length, columnCount, offset);
        } else {
            throw IlpV4ParseException.create(
                    IlpV4ParseException.ErrorCode.INVALID_SCHEMA_MODE,
                    "unknown schema mode: 0x" + Integer.toHexString(schemaMode & 0xFF)
            );
        }
    }

    private static ParseResult parseFullSchema(long address, int length, int columnCount, int offset) throws IlpV4ParseException {
        IlpV4ColumnDef[] columns = new IlpV4ColumnDef[columnCount];
        IlpV4Varint.DecodeResult decodeResult = new IlpV4Varint.DecodeResult();
        long limit = address + length; // Absolute end address

        for (int i = 0; i < columnCount; i++) {
            if (offset >= length) {
                throw IlpV4ParseException.headerTooShort();
            }

            // Parse column name length (varint)
            IlpV4Varint.decode(address + offset, limit, decodeResult);
            offset += decodeResult.bytesRead;

            int nameLenInt = (int) decodeResult.value;
            // Empty column names are allowed for designated timestamp (empty name + TIMESTAMP type)
            if (nameLenInt < 0) {
                throw IlpV4ParseException.create(
                        IlpV4ParseException.ErrorCode.INVALID_COLUMN_NAME,
                        "negative column name length at column " + i
                );
            }
            if (nameLenInt > MAX_COLUMN_NAME_LENGTH) {
                throw IlpV4ParseException.create(
                        IlpV4ParseException.ErrorCode.INVALID_COLUMN_NAME,
                        "column name too long: " + nameLenInt + " bytes"
                );
            }
            if (offset + nameLenInt + 1 > length) {
                throw IlpV4ParseException.headerTooShort();
            }

            // Read column name bytes
            byte[] nameBytes = new byte[nameLenInt];
            for (int j = 0; j < nameLenInt; j++) {
                nameBytes[j] = Unsafe.getUnsafe().getByte(address + offset + j);
            }
            String columnName = new String(nameBytes, StandardCharsets.UTF_8);
            offset += nameLenInt;

            // Read column type
            byte typeCode = Unsafe.getUnsafe().getByte(address + offset);
            offset++;

            columns[i] = new IlpV4ColumnDef(columnName, typeCode);
            columns[i].validate();
        }

        // Compute hash over column definitions (consistent with create())
        long schemaHash = computeSchemaHash(columns);

        return ParseResult.fullSchema(new IlpV4Schema(columns, schemaHash), offset);
    }

    private static ParseResult parseFullSchemaFromArray(byte[] buf, int bufOffset, int length, int columnCount, int offset) throws IlpV4ParseException {
        IlpV4ColumnDef[] columns = new IlpV4ColumnDef[columnCount];
        IlpV4Varint.DecodeResult decodeResult = new IlpV4Varint.DecodeResult();
        int limit = bufOffset + length; // Absolute end position

        for (int i = 0; i < columnCount; i++) {
            if (offset >= length) {
                throw IlpV4ParseException.headerTooShort();
            }

            // Parse column name length (varint)
            IlpV4Varint.decode(buf, bufOffset + offset, limit, decodeResult);
            offset += decodeResult.bytesRead;

            int nameLenInt = (int) decodeResult.value;
            // Empty column names are allowed for designated timestamp (empty name + TIMESTAMP type)
            if (nameLenInt < 0) {
                throw IlpV4ParseException.create(
                        IlpV4ParseException.ErrorCode.INVALID_COLUMN_NAME,
                        "negative column name length at column " + i
                );
            }
            if (nameLenInt > MAX_COLUMN_NAME_LENGTH) {
                throw IlpV4ParseException.create(
                        IlpV4ParseException.ErrorCode.INVALID_COLUMN_NAME,
                        "column name too long: " + nameLenInt + " bytes"
                );
            }
            if (offset + nameLenInt + 1 > length) {
                throw IlpV4ParseException.headerTooShort();
            }

            // Read column name
            String columnName = new String(buf, bufOffset + offset, nameLenInt, StandardCharsets.UTF_8);
            offset += nameLenInt;

            // Read column type
            byte typeCode = buf[bufOffset + offset];
            offset++;

            columns[i] = new IlpV4ColumnDef(columnName, typeCode);
            columns[i].validate();
        }

        // Compute hash over column definitions (consistent with create())
        long schemaHash = computeSchemaHash(columns);

        return ParseResult.fullSchema(new IlpV4Schema(columns, schemaHash), offset);
    }

    /**
     * Encodes this schema in full schema mode to direct memory.
     *
     * @param address destination address
     * @return address after encoded schema
     */
    public long encode(long address) {
        Unsafe.getUnsafe().putByte(address, SCHEMA_MODE_FULL);
        long pos = address + 1;

        for (IlpV4ColumnDef col : columns) {
            byte[] nameBytes = col.getName().getBytes(StandardCharsets.UTF_8);
            pos = IlpV4Varint.encode(pos, nameBytes.length);
            for (byte b : nameBytes) {
                Unsafe.getUnsafe().putByte(pos++, b);
            }
            Unsafe.getUnsafe().putByte(pos++, col.getWireTypeCode());
        }

        return pos;
    }

    /**
     * Encodes this schema in full schema mode to a byte array.
     *
     * @param buf    destination buffer
     * @param offset starting offset
     * @return offset after encoded schema
     */
    public int encode(byte[] buf, int offset) {
        buf[offset++] = SCHEMA_MODE_FULL;

        for (IlpV4ColumnDef col : columns) {
            byte[] nameBytes = col.getName().getBytes(StandardCharsets.UTF_8);
            offset = IlpV4Varint.encode(buf, offset, nameBytes.length);
            System.arraycopy(nameBytes, 0, buf, offset, nameBytes.length);
            offset += nameBytes.length;
            buf[offset++] = col.getWireTypeCode();
        }

        return offset;
    }

    /**
     * Encodes a schema reference to direct memory.
     *
     * @param address    destination address
     * @param schemaHash the schema hash
     * @return address after encoded reference
     */
    public static long encodeReference(long address, long schemaHash) {
        Unsafe.getUnsafe().putByte(address, SCHEMA_MODE_REFERENCE);
        Unsafe.getUnsafe().putLong(address + 1, schemaHash);
        return address + 1 + 8;
    }

    /**
     * Encodes a schema reference to a byte array.
     *
     * @param buf        destination buffer
     * @param offset     starting offset
     * @param schemaHash the schema hash
     * @return offset after encoded reference
     */
    public static int encodeReference(byte[] buf, int offset, long schemaHash) {
        buf[offset++] = SCHEMA_MODE_REFERENCE;
        buf[offset++] = (byte) schemaHash;
        buf[offset++] = (byte) (schemaHash >> 8);
        buf[offset++] = (byte) (schemaHash >> 16);
        buf[offset++] = (byte) (schemaHash >> 24);
        buf[offset++] = (byte) (schemaHash >> 32);
        buf[offset++] = (byte) (schemaHash >> 40);
        buf[offset++] = (byte) (schemaHash >> 48);
        buf[offset++] = (byte) (schemaHash >> 56);
        return offset;
    }

    /**
     * Computes the encoded size in bytes for this schema in full mode.
     *
     * @return encoded size
     */
    public int encodedSize() {
        int size = 1; // schema mode byte
        for (IlpV4ColumnDef col : columns) {
            byte[] nameBytes = col.getName().getBytes(StandardCharsets.UTF_8);
            size += IlpV4Varint.encodedLength(nameBytes.length);
            size += nameBytes.length;
            size += 1; // type code
        }
        return size;
    }

    /**
     * Computes the schema hash for an array of column definitions.
     * <p>
     * Hash is XXH64 over column name bytes + type code for each column.
     * This matches IlpV4SchemaHash.computeSchemaHash() for consistency.
     */
    private static long computeSchemaHash(IlpV4ColumnDef[] columns) {
        String[] names = new String[columns.length];
        byte[] types = new byte[columns.length];
        for (int i = 0; i < columns.length; i++) {
            names[i] = columns[i].getName();
            types[i] = columns[i].getWireTypeCode();
        }
        return IlpV4SchemaHash.computeSchemaHash(names, types);
    }

    // ==================== Getters ====================

    /**
     * Gets the number of columns in this schema.
     */
    public int getColumnCount() {
        return columns.length;
    }

    /**
     * Gets the column definition at the specified index.
     *
     * @param index column index
     * @return column definition
     */
    public IlpV4ColumnDef getColumn(int index) {
        return columns[index];
    }

    /**
     * Gets all column definitions.
     *
     * @return copy of column definitions array
     */
    public IlpV4ColumnDef[] getColumns() {
        return columns.clone();
    }

    /**
     * Gets the schema hash.
     */
    public long getSchemaHash() {
        return schemaHash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IlpV4Schema that = (IlpV4Schema) o;
        return schemaHash == that.schemaHash && Arrays.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return (int) (schemaHash ^ (schemaHash >>> 32));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IlpV4Schema{hash=0x").append(Long.toHexString(schemaHash));
        sb.append(", columns=[");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(columns[i]);
        }
        sb.append("]}");
        return sb.toString();
    }
}
